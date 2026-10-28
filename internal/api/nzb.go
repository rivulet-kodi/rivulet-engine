// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — NZB/Usenet streaming at /nzb/*.
//
// Routes:
//
//	POST /nzb/create
//	POST /nzb/create/{key}   body: {"servers":[{host,port,user,pass,ssl,connections}], "nzbUrl":"..."}
//	                         or ?lz=<lz-string-encoded-json>
//	                         → {"key":"<session-key>"}
//	GET  /nzb/create?lz=...  → creates the session like POST, then 307-redirects
//	                         to /nzb/stream/{key} (stremio-core's Stream::convert()
//	                         emits this GET form for NZB streams)
//	GET  /nzb/create         → 501 (no ?lz= payload)
//
//	GET  /nzb/stream?key={key}
//	GET  /nzb/stream/{key}/{file...}
//	                         → file served with Range/HEAD support; assembly is
//	                         progressive (a range is answered as soon as the
//	                         segments covering it are downloaded, no need to
//	                         wait for the whole file)
//
// Sessions expire after 1 hour of inactivity; extracted temp files are removed
// by a background janitor started lazily on first create. Players re-request
// the create URL on every open/seek, so re-creating an existing key with an
// equivalent payload (same NZB URL and first server, see nzbCreateSig) reuses
// the live session and the file it has assembled so far — as long as no
// assembly of it failed and it is younger than nzbSessionTTL (the NZB is not
// fetched again to compare content, so a changed NZB behind the same URL is
// picked up at most one TTL later). A different payload replaces the session;
// a replaced session that a response is still streaming from lives on until
// that response ends, so a playing stream is never cut off.
package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	lzstring "github.com/daku10/go-lz-string"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
	"github.com/rivulet-kodi/rivulet-engine/internal/nzb"
)

// lz-string size guards — mirrors the limits used in archive.go and ftp.go.
const (
	nzbLzMaxEncoded = 1 << 20 // 1 MiB — max encoded ?lz= parameter length
	nzbLzMaxDecoded = 8 << 20 // 8 MiB — max decompressed JSON length
	// nzbMaxDownloadBytes caps the NZB XML fetched from the caller-supplied
	// nzbUrl (mirrors the cap httpGet used before this route switched to
	// archiveFetchGet for the guarded client).
	nzbMaxDownloadBytes = 16 << 20 // 16 MiB
)

// ---- session store ---------------------------------------------------------

// nzbFileState tracks the background assembly of a single file within a
// session. The assembly is shared: every request for the file reads from the
// same temp file while one set of parallel NNTP workers fills it, so a client
// disconnecting never stops it. The mutex only guards (re)starting; nothing
// blocks while holding it. A failed assembly is discarded and restarted by the
// next request.
type nzbFileState struct {
	mu      sync.Mutex
	asm     *nzb.Assembly // nil until the first request
	path    string        // temp file backing asm
	started time.Time     // stable Last-Modified while the file fills up
	restart int           // how many times a failed assembly was restarted

	// closed is closed once the temp file of asm — and of every assembly this
	// state ran before it — has been closed. discard waits on it so the
	// directory is never deleted under an open file (Windows refuses that).
	closed chan struct{}
}

// nzbSession holds all state for one NZB streaming session.
type nzbSession struct {
	key        string
	cfg        nzb.ServerConfig
	files      []nzb.File
	tmpDir     string
	created    time.Time
	lastAccess time.Time
	refCount   int // in-flight requests; >0 blocks eviction (guarded by nzbSessionsMu)

	// sig is the normalized create payload this session was built from (see
	// nzbCreateSig); an equivalent re-create reuses the session. "" never
	// matches, so sessions built any other way are always replaced.
	sig string
	// noReuse marks a session an equivalent create must not reuse — its
	// assembly failed — so the retry rebuilds (guarded by nzbSessionsMu).
	noReuse bool

	// closeFile closes an assembly's temp file; nil means (*os.File).Close.
	// It is a test seam for asserting close-before-delete ordering; tests
	// set it under nzbSessionsMu before the session's first request.
	closeFile func(*os.File) error

	mu         sync.Mutex
	fileStates map[string]*nzbFileState // file Name → assembly state
}

var (
	nzbSessionsMu  sync.Mutex
	nzbSessions    = map[string]*nzbSession{}
	nzbJanitorOnce sync.Once

	// nzbOrphans holds sessions that were replaced under their key while a
	// request was still reading from them (guarded by nzbSessionsMu). They are
	// no longer reachable through nzbSessions, so the last request to finish
	// discards them (nzbRelease) instead of eviction or replacement.
	nzbOrphans = map[*nzbSession]struct{}{}

	// nzbBuilds holds the in-progress builds of sessions that carry a caller
	// key (guarded by nzbSessionsMu): identical creates arriving meanwhile wait
	// for the build instead of fetching the NZB and building again.
	nzbBuilds = map[nzbBuildKey]*nzbBuildFlight{}
)

func nzbStartJanitor() {
	nzbJanitorOnce.Do(func() {
		go func() {
			// Reclaim assembled-file dirs leaked by a previous process.
			nzbSweepStale(os.TempDir())
			tick := time.NewTicker(10 * time.Minute)
			defer tick.Stop()
			for range tick.C {
				nzbEvictIdle()
				nzbSweepStale(os.TempDir())
			}
		}()
	})
}

// nzbSessionTTL is how long an idle session lives before the janitor evicts
// it, and how long after its build it may still be reused by an equivalent
// create request (see nzbReuseOrLead).
const nzbSessionTTL = time.Hour

// nzbTmpDirPrefix is the temp-dir prefix of NZB session dirs; only dirs with
// exactly this prefix are ever swept.
const nzbTmpDirPrefix = "rivulet-engine-nzb-"

// nzbSweepStale removes NZB session dirs under root that no live session owns
// and that have not been modified within an hour (leaked by a prior process).
func nzbSweepStale(root string) {
	live := map[string]struct{}{}
	nzbSessionsMu.Lock()
	for _, sess := range nzbSessions {
		live[sess.tmpDir] = struct{}{}
	}
	for sess := range nzbOrphans {
		live[sess.tmpDir] = struct{}{}
	}
	nzbSessionsMu.Unlock()
	sweepStaleTemp(root, live, time.Hour, func(e os.DirEntry) bool {
		return e.IsDir() && strings.HasPrefix(e.Name(), nzbTmpDirPrefix)
	})
}

// nzbRetire is called, under nzbSessionsMu, for a session that has just been
// replaced under its key. A session no request is reading from is returned
// for immediate discard; one that is still streaming is parked in nzbOrphans
// (and nil is returned) so its assembly keeps feeding those responses until
// the last of them finishes — cancelling it now would truncate a playing
// stream, and players re-request the create URL on every open/seek.
func nzbRetire(old *nzbSession) *nzbSession {
	if old.refCount > 0 {
		nzbOrphans[old] = struct{}{}
		return nil
	}
	return old
}

// nzbRelease drops one in-flight request from sess. The last request to leave
// an orphaned session discards it, in the background so the (already
// complete) response is not held up by the teardown.
func nzbRelease(sess *nzbSession) {
	nzbSessionsMu.Lock()
	sess.refCount--
	_, orphan := nzbOrphans[sess]
	last := orphan && sess.refCount == 0
	if last {
		delete(nzbOrphans, sess)
	}
	nzbSessionsMu.Unlock()
	if last {
		go sess.discard()
	}
}

// nzbEvictIdle removes sessions that have not been accessed in the last hour
// and have no in-flight requests, and deletes their temporary directories.
func nzbEvictIdle() {
	nzbSessionsMu.Lock()
	var evict []*nzbSession
	for key, sess := range nzbSessions {
		if sess.refCount > 0 {
			continue
		}
		if time.Since(sess.lastAccess) > nzbSessionTTL {
			evict = append(evict, sess)
			delete(nzbSessions, key)
		}
	}
	nzbSessionsMu.Unlock()

	for _, sess := range evict {
		sess.discard()
	}
}

// discard stops the session's background assemblies, waits (bounded) for
// their workers to release the temp files and NNTP connections, and deletes
// the session's temp directory.
func (sess *nzbSession) discard() {
	sess.mu.Lock()
	states := make([]*nzbFileState, 0, len(sess.fileStates))
	for _, fs := range sess.fileStates {
		states = append(states, fs)
	}
	sess.mu.Unlock()

	for _, fs := range states {
		fs.mu.Lock()
		asm, closed := fs.asm, fs.closed
		fs.mu.Unlock()
		if asm == nil {
			continue
		}
		asm.Cancel()
		waitBounded(asm.Done(), nzbDiscardWait)
		// Done only says the workers are gone: the temp file is closed by a
		// separate goroutine, and an open file cannot be deleted on Windows.
		waitBounded(closed, nzbDiscardWait)
	}
	if sess.tmpDir != "" {
		_ = os.RemoveAll(sess.tmpDir)
	}
}

// nzbDiscardWait bounds how long discard waits for one assembly's workers
// (and then its temp file) to wind down before deleting the directory anyway.
const nzbDiscardWait = 5 * time.Second

// waitBounded waits for ch to close, for at most d.
func waitBounded(ch <-chan struct{}, d time.Duration) {
	if ch == nil {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
	case <-t.C:
	}
}

// ---- request body types ----------------------------------------------------

type nzbServerCfg struct {
	Host        string `json:"host"`
	Port        int    `json:"port"`
	User        string `json:"user"`
	Pass        string `json:"pass"`
	SSL         bool   `json:"ssl"`
	Connections int    `json:"connections"`
}

type nzbCreateReq struct {
	Servers json.RawMessage `json:"servers"`
	NZBUrl  string          `json:"nzbUrl"`
	NZBUrls []string        `json:"nzbUrls"`
}

// ---- handler ---------------------------------------------------------------

// handleNZB dispatches all /nzb/* routes.
func (s *server) handleNZB(w http.ResponseWriter, r *http.Request, seg []string) {
	nzbStartJanitor()

	// seg[0] == "nzb"
	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}

	switch seg[1] {
	case "create":
		if r.Method == http.MethodGet && r.URL.Query().Get("lz") == "" {
			writeJSON(w, http.StatusNotImplemented, map[string]any{
				"error": "GET /nzb/create requires ?lz=; use POST with {servers, nzbUrl} or GET with ?lz=",
			})
			return
		}
		key := ""
		if len(seg) >= 3 {
			key = seg[2]
		}
		s.nzbCreate(w, r, key)
	case "stream":
		s.nzbStream(w, r, seg)
	default:
		http.NotFound(w, r)
	}
}

// nzbCreate handles POST /nzb/create[/{key}] and GET /nzb/create[/{key}]?lz=...
//   - POST → resolve, store session, respond {"key":…}.
//   - GET  → same, then 307-redirect to /nzb/stream/{key}.
func (s *server) nzbCreate(w http.ResponseWriter, r *http.Request, key string) {
	var req nzbCreateReq

	// Accept either ?lz=<compressed> or a plain JSON body.
	if lzParam := r.URL.Query().Get("lz"); lzParam != "" {
		if len(lzParam) > nzbLzMaxEncoded {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "lz: encoded payload too large"})
			return
		}
		jsonStr, err := lzstring.DecompressFromEncodedURIComponent(lzParam)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "lz: " + err.Error()})
			return
		}
		if len(jsonStr) > nzbLzMaxDecoded {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "lz: decompressed payload too large"})
			return
		}
		if err := json.Unmarshal([]byte(jsonStr), &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "json: " + err.Error()})
			return
		}
	} else {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "json: " + err.Error()})
			return
		}
	}

	nzbURL := req.NZBUrl
	if nzbURL == "" && len(req.NZBUrls) > 0 {
		nzbURL = req.NZBUrls[0]
	}
	if nzbURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "nzbUrl required"})
		return
	}

	servers, err := parseNzbServers(req.Servers)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if len(servers) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "servers list required"})
		return
	}

	// Build server config from the first server entry. Control guards the
	// NNTP TCP/TLS dial itself: the client-supplied host/port in servers[]
	// previously bypassed netguard entirely (SEC-2), unlike nzbURL below.
	srv := servers[0]
	cfg := nzb.ServerConfig{
		Host:        srv.Host,
		Port:        srv.Port,
		User:        srv.User,
		Pass:        srv.Pass,
		SSL:         srv.SSL,
		Connections: srv.Connections,
		Control:     netguard.DialControl(!archiveAllowPrivateHosts()),
	}
	if cfg.Port == 0 {
		if cfg.SSL {
			cfg.Port = 563
		} else {
			cfg.Port = 119
		}
	}

	// A caller-supplied key is how players re-request the same stream (every
	// open/seek): an equivalent payload keeps the live session — and the file
	// it has assembled so far — instead of fetching the NZB and downloading
	// every segment again. Identical creates in flight share one build.
	var res nzbCreateResult
	if key == "" {
		res = nzbBuildAndRegister(key, "", nil, nzbURL, cfg)
	} else {
		var ok bool
		res, ok = nzbCreateOrReuse(r.Context(), key, nzbCreateSig(nzbURL, cfg), nzbURL, cfg)
		if !ok {
			return // the client went away while waiting for an identical build
		}
	}
	if res.status != 0 {
		writeJSON(w, res.status, map[string]any{"error": res.msg})
		return
	}

	if r.Method == http.MethodPost {
		writeJSON(w, http.StatusOK, map[string]any{"key": res.key})
		return
	}
	// GET → redirect straight to the stream URL; nzbResolveFile picks the file.
	http.Redirect(w, r, "/nzb/stream/"+url.PathEscape(res.key), http.StatusTemporaryRedirect)
}

// nzbCreateResult is the outcome of a create: the session's key, or the error
// response (status != 0) every request that shared the build answers with.
type nzbCreateResult struct {
	key    string
	status int
	msg    string
}

// nzbBuildKey identifies one in-progress build: a key and the normalized
// payload it builds for.
type nzbBuildKey struct{ key, sig string }

// nzbBuildFlight is one in-progress build of a keyed session. The request that
// started it is the leader; identical requests wait on done and answer with res.
type nzbBuildFlight struct {
	done chan struct{}
	res  nzbCreateResult // set before done is closed
}

// nzbCreateSig is the normalized create payload: what the built session
// depends on. Equivalent spellings of the same request (nzbUrl vs nzbUrls[0],
// URL vs object servers, an omitted port vs its default, …) share one
// signature, because it is computed from the parsed values, not the raw JSON.
// Only the first server is used by a session, so only it counts. The NZB is
// identified by its URL: it is not fetched again to compare content.
func nzbCreateSig(nzbURL string, cfg nzb.ServerConfig) string {
	b, err := json.Marshal([]any{nzbURL, cfg.Host, cfg.Port, cfg.User, cfg.Pass, cfg.SSL, cfg.Connections})
	if err != nil {
		return "" // never matches: always rebuilt
	}
	return string(b)
}

// nzbCreateOrReuse answers a keyed create. In order: an equivalent live
// session is reused; an identical build already in progress is waited for;
// otherwise this request builds (and replaces whatever the key held). It
// returns ok=false when ctx ends while waiting.
func nzbCreateOrReuse(ctx context.Context, key, sig, nzbURL string, cfg nzb.ServerConfig) (res nzbCreateResult, ok bool) {
	if sig == "" {
		return nzbBuildAndRegister(key, sig, nil, nzbURL, cfg), true
	}
	for {
		sess, fl, leader := nzbReuseOrLead(key, sig)
		switch {
		case sess != nil:
			healthy := sess.healthy()
			nzbSessionsMu.Lock()
			if !healthy {
				sess.noReuse = true // the retry — and every later create — rebuilds
			}
			nzbSessionsMu.Unlock()
			nzbRelease(sess)
			if healthy {
				return nzbCreateResult{key: key}, true
			}
		case leader:
			return nzbBuildAndRegister(key, sig, fl, nzbURL, cfg), true
		default:
			select {
			case <-fl.done:
				return fl.res, true
			case <-ctx.Done():
				return nzbCreateResult{}, false
			}
		}
	}
}

// nzbReuseOrLead atomically decides what a keyed create does. It returns the
// live session to reuse — pinned (refCount) and with lastAccess refreshed; the
// caller must nzbRelease it — or the build already in progress for the same
// payload to wait for, or (leader) a new flight this request must complete.
// Doing all of it under one lock means a create can never slip between a
// finishing build and its registration and build a duplicate.
func nzbReuseOrLead(key, sig string) (sess *nzbSession, fl *nzbBuildFlight, leader bool) {
	nzbSessionsMu.Lock()
	defer nzbSessionsMu.Unlock()
	now := time.Now()
	if cur, ok := nzbSessions[key]; ok && cur.sig == sig && !cur.noReuse &&
		now.Sub(cur.created) < nzbSessionTTL && now.Sub(cur.lastAccess) < nzbSessionTTL {
		cur.refCount++
		cur.lastAccess = now
		return cur, nil, false
	}
	bk := nzbBuildKey{key, sig}
	if cur, ok := nzbBuilds[bk]; ok {
		return nil, cur, false
	}
	fl = &nzbBuildFlight{done: make(chan struct{})}
	nzbBuilds[bk] = fl
	return nil, fl, true
}

// healthy reports whether no assembly of the session has failed. A session
// with a failed (or cancelled) assembly is not reused: a retry must rebuild.
func (sess *nzbSession) healthy() bool {
	sess.mu.Lock()
	states := make([]*nzbFileState, 0, len(sess.fileStates))
	for _, fs := range sess.fileStates {
		states = append(states, fs)
	}
	sess.mu.Unlock()
	for _, fs := range states {
		fs.mu.Lock()
		asm := fs.asm
		fs.mu.Unlock()
		if asm != nil && asm.Err() != nil {
			return false
		}
	}
	return true
}

// nzbBuildAndRegister builds a session and registers it under its key. When
// fl is non-nil the caller leads that flight, and it is always completed —
// also on failure or panic — so waiters are never stranded.
func nzbBuildAndRegister(key, sig string, fl *nzbBuildFlight, nzbURL string, cfg nzb.ServerConfig) (res nzbCreateResult) {
	bk := nzbBuildKey{key, sig}
	if fl != nil {
		res = nzbCreateResult{status: http.StatusInternalServerError, msg: "nzb session build aborted"}
		defer func() {
			nzbSessionsMu.Lock()
			if nzbBuilds[bk] == fl {
				delete(nzbBuilds, bk)
			}
			nzbSessionsMu.Unlock()
			fl.res = res
			close(fl.done)
		}()
	}

	sess, fail := nzbBuildSession(key, sig, nzbURL, cfg)
	if sess == nil {
		res = fail
		return res
	}

	nzbSessionsMu.Lock()
	// Retire any previous session under the same key. One that a response is
	// still streaming from is kept alive until that response finishes
	// (nzbRetire/nzbRelease): discarding it now would cancel the assembly
	// mid-stream and truncate the playing file.
	var discard *nzbSession
	if old, ok := nzbSessions[sess.key]; ok {
		discard = nzbRetire(old)
	}
	nzbSessions[sess.key] = sess
	if fl != nil && nzbBuilds[bk] == fl {
		delete(nzbBuilds, bk) // same critical section: no window with neither flight nor session
	}
	nzbSessionsMu.Unlock()
	if discard != nil {
		go discard.discard()
	}
	res = nzbCreateResult{key: sess.key}
	return res
}

// nzbBuildSession fetches and parses the NZB and returns a new, unregistered
// session, or the error response to send.
func nzbBuildSession(key, sig, nzbURL string, cfg nzb.ServerConfig) (*nzbSession, nzbCreateResult) {
	// Fetch the NZB file from the provided URL. validateFetchHost is an
	// early 403; archiveFetchGet (archive.go, same package) performs the
	// actual fetch through archiveFetchClient, whose dialer re-validates
	// the resolved IP at connect time and whose redirect policy
	// re-validates every hop — api.go's getClient/httpGet must not be used
	// here since this route fetches a caller-supplied URL unauthenticated.
	if err := validateFetchHost(nzbURL); err != nil {
		return nil, nzbCreateResult{status: http.StatusBadGateway, msg: err.Error()}
	}
	nzbData, err := archiveFetchGet(nzbURL, nzbMaxDownloadBytes)
	if err != nil {
		return nil, nzbCreateResult{status: http.StatusBadGateway, msg: err.Error()}
	}

	// Parse the NZB XML.
	files, err := nzb.Parse(nzbData)
	if err != nil {
		return nil, nzbCreateResult{status: http.StatusUnprocessableEntity, msg: "nzb: " + err.Error()}
	}

	// Create an isolated temp directory for assembled file cache.
	tmpDir, err := os.MkdirTemp("", nzbTmpDirPrefix)
	if err != nil {
		return nil, nzbCreateResult{status: http.StatusInternalServerError, msg: err.Error()}
	}

	// Assign a random key when none is provided by the caller.
	if key == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			_ = os.RemoveAll(tmpDir)
			return nil, nzbCreateResult{status: http.StatusInternalServerError, msg: err.Error()}
		}
		key = fmt.Sprintf("%x", b)
	}

	now := time.Now()
	return &nzbSession{
		key:        key,
		sig:        sig,
		cfg:        cfg,
		files:      files,
		tmpDir:     tmpDir,
		created:    now,
		lastAccess: now,
		fileStates: map[string]*nzbFileState{},
	}, nzbCreateResult{}
}

// parseNzbServers accepts either the canonical stremio-core form — a JSON array
// of NNTP URL strings (nntp://user:pass@host:port, nntps:// for TLS) — or the
// legacy array of {host,port,user,pass,ssl,connections} objects.
func parseNzbServers(raw json.RawMessage) ([]nzbServerCfg, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("servers list required")
	}
	// Canonical form: array of NNTP URL strings.
	var urls []string
	if json.Unmarshal(raw, &urls) == nil && len(urls) > 0 {
		out := make([]nzbServerCfg, 0, len(urls))
		for _, u := range urls {
			cfg, err := nzbServerFromURL(u)
			if err != nil {
				return nil, err
			}
			out = append(out, cfg)
		}
		return out, nil
	}
	// Legacy form: array of server objects.
	var objs []nzbServerCfg
	if err := json.Unmarshal(raw, &objs); err != nil {
		return nil, fmt.Errorf("invalid servers: %w", err)
	}
	return objs, nil
}

// nzbServerFromURL parses an NNTP server URL into an nzbServerCfg. The scheme
// selects TLS (nntps/snews → SSL), userinfo carries credentials, and a missing
// port defaults later to 119 (plain) or 563 (TLS).
func nzbServerFromURL(raw string) (nzbServerCfg, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nzbServerCfg{}, fmt.Errorf("invalid nntp server url %q: %w", raw, err)
	}
	cfg := nzbServerCfg{
		Host: u.Hostname(),
		SSL:  u.Scheme == "nntps" || u.Scheme == "snews",
	}
	if cfg.Host == "" {
		return nzbServerCfg{}, fmt.Errorf("nntp server url %q has no host", raw)
	}
	if p := u.Port(); p != "" {
		cfg.Port, _ = strconv.Atoi(p)
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			cfg.Pass = pass
		}
	}
	return cfg, nil
}

// nzbStream handles GET /nzb/stream?key={key} and /nzb/stream/{key}/{file...}.
func (s *server) nzbStream(w http.ResponseWriter, r *http.Request, seg []string) {
	// Resolve key and optional filename from the URL.
	var key, fileName string
	if len(seg) >= 3 {
		// /nzb/stream/{key}[/{file...}]
		key = seg[2]
		if len(seg) >= 4 {
			fileName = strings.Join(seg[3:], "/")
		}
	} else {
		// /nzb/stream?key=...
		key = r.URL.Query().Get("key")
	}

	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "session key required"})
		return
	}

	nzbSessionsMu.Lock()
	sess, ok := nzbSessions[key]
	if ok {
		sess.lastAccess = time.Now()
		sess.refCount++
	}
	nzbSessionsMu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "session not found"})
		return
	}
	defer nzbRelease(sess)

	files := sess.files
	if len(files) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "NZB contains no files"})
		return
	}

	// Resolve which file to serve.
	target := nzbResolveFile(files, fileName)
	if target == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no suitable file found"})
		return
	}

	// Find target's index in sess.files so the temp filename is unique even when
	// two NZB entries share the same basename (prevents collision in tmpDir).
	fileIdx := 0
	for i := range sess.files {
		if sess.files[i].Name == target.Name {
			fileIdx = i
			break
		}
	}

	// Look up or initialise the per-file assembly state.
	sess.mu.Lock()
	fs, exists := sess.fileStates[target.Name]
	if !exists {
		fs = &nzbFileState{}
		sess.fileStates[target.Name] = fs
	}
	sess.mu.Unlock()

	// The first request starts the background assembly (parallel NNTP
	// connections filling a temp file); later ones share it. A previously
	// failed assembly is restarted from scratch.
	asm, assembledPath, modTime, err := fs.ensure(sess, target, fileIdx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	// The decoded size is known as soon as the first article's yEnc header has
	// been read — long before the file is complete — so the response can start
	// right away. r.Context() only bounds this request: if the client goes
	// away the shared assembly carries on for the next one.
	ctx := r.Context()
	size, err := asm.Size(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // client disconnected
		}
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "nzb assemble: " + err.Error()})
		return
	}

	// Open and serve the assembled file with full Range/HEAD support.
	f, err := os.Open(assembledPath)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	defer func() { _ = f.Close() }()

	w.Header().Set("Content-Type", mimeByName(target.Name))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("transferMode.dlna.org", "Streaming")
	w.Header().Set("contentFeatures.dlna.org",
		"DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000")

	if asm.Complete() {
		// Fully assembled: serve the plain file (keeps the sendfile fast path).
		http.ServeContent(w, r, target.Name, modTime, f)
		return
	}
	// Still downloading: each Read waits only for the segments it needs.
	http.ServeContent(w, r, target.Name, modTime, &nzbProgressiveFile{ctx: ctx, asm: asm, f: f, size: size})
}

// ensure returns the file's assembly, starting it when there is none or the
// previous one failed. The temp file is created before the assembly starts and
// closed once its workers have exited.
func (fs *nzbFileState) ensure(sess *nzbSession, target *nzb.File, fileIdx int) (*nzb.Assembly, string, time.Time, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.asm != nil && fs.asm.Err() == nil {
		return fs.asm, fs.path, fs.started, nil
	}
	prefix := strconv.Itoa(fileIdx)
	if fs.asm != nil {
		// Failed (or cancelled): drop the broken partial file. Readers still
		// attached to it keep their own open descriptor; the retry writes a
		// new, differently named file so stale workers can never touch it.
		fs.asm.Cancel()
		_ = os.Remove(fs.path)
		fs.restart++
		prefix += "r" + strconv.Itoa(fs.restart)
	}

	safeName := filepath.Base(target.Name)
	if safeName == "" || safeName == "." || safeName == "/" {
		safeName = "media.bin"
	}
	// Prefix with the file index to make the temp name unique per NZB entry.
	tmpPath := filepath.Join(sess.tmpDir, prefix+"-"+safeName)

	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, "", time.Time{}, err
	}
	// context.Background(), not the request's: the assembly outlives any one
	// client. It is stopped by nzbSession.discard (eviction / replacement).
	asm, err := nzb.NewSession(sess.cfg, sess.files).StartAssembly(context.Background(), target.Name, f)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return nil, "", time.Time{}, fmt.Errorf("nzb assemble: %w", err)
	}
	// Close the temp file once the workers are gone, then announce it:
	// discard waits on closed before deleting the directory. prev chains the
	// files of earlier (failed, restarted) assemblies so closed means all of
	// them are closed.
	prev, closed := fs.closed, make(chan struct{})
	go func() {
		<-asm.Done()
		_ = sess.closeTemp(f)
		if prev != nil {
			<-prev
		}
		close(closed)
	}()

	fs.asm, fs.path, fs.started, fs.closed = asm, tmpPath, time.Now(), closed
	return fs.asm, fs.path, fs.started, nil
}

// closeTemp closes one of the session's assembly temp files.
func (sess *nzbSession) closeTemp(f *os.File) error {
	if sess.closeFile != nil {
		return sess.closeFile(f)
	}
	return f.Close()
}

// nzbProgressiveFile is the io.ReadSeeker http.ServeContent reads while the
// file is still being assembled. Seek is pure arithmetic on the already-known
// size; Read blocks only until the segment holding its next byte is on disk
// (or the request's context ends) and then returns what is present.
type nzbProgressiveFile struct {
	ctx  context.Context
	asm  *nzb.Assembly
	f    *os.File
	size int64
	pos  int64
}

func (p *nzbProgressiveFile) Read(b []byte) (int, error) {
	if p.pos >= p.size {
		return 0, io.EOF
	}
	n := p.size - p.pos
	if int64(len(b)) < n {
		n = int64(len(b))
	}
	if n <= 0 {
		return 0, nil
	}
	// Wait for the first byte only, then read whatever contiguous run is on
	// disk (up to len(b)): the client gets data as it lands instead of the
	// handler stalling until a whole copy buffer's worth has been downloaded.
	avail, err := p.asm.WaitAvailable(p.ctx, p.pos, n)
	if err != nil {
		return 0, err
	}
	m, err := p.f.ReadAt(b[:avail], p.pos)
	p.pos += int64(m)
	switch {
	case int64(m) == avail:
		err = nil // ReadAt may report io.EOF together with a full read
	case errors.Is(err, io.EOF):
		err = io.ErrUnexpectedEOF // the assembly said these bytes were written
	}
	return m, err
}

func (p *nzbProgressiveFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = p.pos + offset
	case io.SeekEnd:
		abs = p.size + offset
	default:
		return 0, errors.New("nzb: invalid seek whence")
	}
	if abs < 0 {
		return 0, errors.New("nzb: negative seek position")
	}
	p.pos = abs
	return abs, nil
}

// nzbResolveFile selects which file to serve from the NZB.
//   - If fileName is given, match by name (basename comparison).
//   - Otherwise return the largest video-like file.
//   - Final fallback: first file.
func nzbResolveFile(files []nzb.File, fileName string) *nzb.File {
	if fileName != "" {
		base := filepath.Base(fileName)
		for i := range files {
			if files[i].Name == fileName || filepath.Base(files[i].Name) == base {
				return &files[i]
			}
		}
	}
	return nzbLargestVideo(files)
}

// nzbVideoExts lists recognised video file extensions for file selection.
var nzbVideoExts = map[string]bool{
	".mkv": true, ".mp4": true, ".avi": true, ".mov": true,
	".wmv": true, ".flv": true, ".webm": true, ".m4v": true,
	".mpg": true, ".mpeg": true, ".ts": true, ".m2ts": true,
}

// nzbLargestVideo returns the largest file whose extension is video-like,
// falling back to the first file when none matches.
func nzbLargestVideo(files []nzb.File) *nzb.File {
	var best *nzb.File
	for i := range files {
		ext := strings.ToLower(filepath.Ext(files[i].Name))
		if nzbVideoExts[ext] {
			if best == nil || files[i].Size > best.Size {
				best = &files[i]
			}
		}
	}
	if best == nil && len(files) > 0 {
		best = &files[0]
	}
	return best
}
