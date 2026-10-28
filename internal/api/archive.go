// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — archive streaming for zip / rar / 7zip / tar / tgz
//
// Routes dispatched by handleArchive (seg[0] is the format extension):
//
//	GET|POST /{ext}/create             — create session, return {"key":…} or redirect
//	GET|POST /{ext}/create/{key}       — same, with caller-supplied key
//	GET      /{ext}/stream             — ?key=&file= redirect to full stream URL
//	GET      /{ext}/stream/{key}       — redirect to /{ext}/stream/{key}/{selectedFile}
//	GET      /{ext}/stream/{key}/{…}   — serve the entry with Range support: stored zip/tar entries in place,
//	                                     everything else extracted once in the background and served while it grows
//
// Create payload (JSON; may be gzip+base62 encoded in ?lz= query param):
//
//	Object  {"url":"…","from":"…","fileIdx":0,"fileMustInclude":"…"}
//	Array   [{"url":"…",…}]   — first element is used
//
// "url" and "from" are synonyms for the archive source. http/https sources are
// downloaded to a temp file; everything else is treated as a local path.
//
// Re-creating an existing key with an equivalent payload (same sources,
// fileIdx and fileMustInclude — see archiveCreateSig) reuses the live session
// instead of building a new one: players such as ExoPlayer re-request the
// create URL on every open/seek, and a rebuild would download the archive and
// extract its entries all over again. The session is reused only while it is
// healthy (no failed extraction or checksum mismatch), younger than
// archiveSessionTTL and, for a local archive, while the file's size and mtime
// are unchanged; a remote URL is not re-checked upstream, so a changed remote
// archive is picked up at most one TTL later. A different payload replaces the
// session as before.
//
// For zip, tar and tgz no archive file handle outlives a request, and none is
// held while background work is merely parked: the listing and the in-place
// extents (offset, size, CRC-32) are cached in the session, and every request,
// extraction and verification reads the archive through its own short-lived
// handle (archive.OpenReaderAt; an in-place response holds one for its
// duration). rar and 7zip libraries keep the archive open while an entry is
// being extracted. On Windows an open handle blocks deleting or replacing the
// file, so the user can still remove their local archive between requests, and
// a session teardown waits for its goroutines before it deletes the downloaded
// archive and the extracted entries. A temp entry file of a failed extraction
// is removed only after its last reader closed it (archiveExtractFlight.dropFile).
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	lzstring "github.com/daku10/go-lz-string"

	"github.com/rivulet-kodi/rivulet-engine/internal/archive"
	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// ── session store ────────────────────────────────────────────────────────────

type archiveSession struct {
	mu            sync.Mutex
	key           string
	archivePath   string // path to archive file (downloaded temp or original local)
	isTempArch    bool   // true → archivePath is a temp file owned by this session
	archiveBytes  int64  // on-disk size of a session-owned temp archive (counts toward archiveMaxTempBytes)
	ext           string // archive format: zip|rar|7zip|tar|tgz
	tmpDir        string // temp dir for extracted entries
	selectedFile  string // default entry selected at create time
	created       time.Time
	lastAccess    time.Time
	refCount      int                              // in-flight requests; >0 blocks eviction (guarded by mu)
	retired       bool                             // key re-created while refCount>0; the last release destroys the session (guarded by mu)
	sig           string                           // normalized create payload (archiveCreateSig); "" never matches, so such sessions are always replaced
	source        string                           // archive source of the create payload; a local path is re-resolved when reusing
	built         archiveFileStat                  // size and mtime of archivePath when the session was built
	noReuse       bool                             // failed: an equivalent create must rebuild (guarded by mu)
	extracted     map[string]string                // entry name → fully extracted temp file path (cache)
	extractedSize map[string]int64                 // entry name → bytes of the extracted file (guarded by mu)
	inflight      map[string]*archiveExtractFlight // entry name → in-progress extraction (guarded by mu)
	direct        map[string]archiveDirect         // entry name → in-place extent verdict, zip/tar only (guarded by mu)

	listMu  sync.Mutex
	listing map[string]archive.Entry // entry name → entry (first wins); listed once per session (guarded by listMu)
}

// archiveDirect caches whether one entry can be served in place from the
// archive file, where its bytes are, and (stored zip entries) the state of the
// CRC-32 verification of those bytes.
type archiveDirect struct {
	ext archive.Extent
	ok  bool
	v   *archiveVerify // nil when the archive records no checksum for the entry (tar)
}

var (
	archiveSessions    = map[string]*archiveSession{}
	archiveSessionsMu  sync.Mutex
	archiveJanitorOnce sync.Once
	// archiveRetired holds sessions whose key was re-created while requests were
	// still reading them: out of archiveSessions (nothing new can reach them) but
	// alive until their last release(). The sweeper must not reap their temp
	// files and the byte cap still counts them. Guarded by archiveSessionsMu.
	archiveRetired = map[*archiveSession]struct{}{}
	// archiveBuilds holds the in-progress builds of keyed sessions: identical
	// creates arriving meanwhile wait for the build instead of downloading and
	// building again. Guarded by archiveSessionsMu.
	archiveBuilds = map[archiveBuildKey]*archiveBuildFlight{}
)

const archiveSessionTTL = time.Hour

// Global caps on what idle archive sessions may keep on disk. They are checked
// when a session is created or an extraction starts; the least recently used
// idle sessions (no in-flight request) are dropped first, never the session
// being served. They are variables so tests can lower them.
var (
	// archiveMaxSessions bounds the number of live archive sessions.
	archiveMaxSessions = 32
	// archiveMaxTempBytes bounds the temp bytes held by all sessions: downloaded
	// archives plus extracted (or about-to-be-extracted) entries.
	archiveMaxTempBytes int64 = 32 << 30 // 32 GiB
)

// archiveMaxDownloadBytes is the maximum number of bytes that archiveDownload
// will stream from a remote URL to a local temp file. Archives larger than
// this limit are rejected to prevent unbounded disk consumption.
// 4 GiB covers the largest single-file archives seen in practice.
const archiveMaxDownloadBytes int64 = 4 << 30 // 4 GiB

// archiveMaxLZEncoded is the maximum accepted byte length of a raw ?lz= query
// value before decompression. Prevents memory blowup before decode begins.
const archiveMaxLZEncoded = 1 << 20 // 1 MiB

// archiveMaxLZDecoded is the maximum byte length of the JSON string produced
// by lz-string decompression. Guards against decompression bombs.
const archiveMaxLZDecoded = 8 << 20 // 8 MiB

// archiveMaxEntryBytes is the maximum declared (uncompressed) size accepted
// for a single archive entry during extraction. Entries claiming more than
// this are rejected before any data is copied, preventing integer overflow
// (declaredSize+1 wraps negative for values near math.MaxInt64) and unbounded
// disk writes from malicious ZIP64/tar archives. 64 GiB is far beyond any
// legitimate video entry seen in practice.
const archiveMaxEntryBytes int64 = 64 << 30 // 64 GiB

// archiveStartJanitor starts a background goroutine that evicts idle sessions.
// It is started at most once, on the first incoming request.
func archiveStartJanitor() {
	archiveJanitorOnce.Do(func() {
		go func() {
			// Reclaim temp files/dirs leaked by a previous process (crash,
			// SIGTERM, restart) before serving; then keep sweeping.
			archiveSweepStale(os.TempDir())
			t := time.NewTicker(10 * time.Minute)
			defer t.Stop()
			for range t.C {
				archiveEvict()
				archiveSweepStale(os.TempDir())
			}
		}()
	})
}

// Temp-name prefixes created by this server for archive sessions. Only names
// carrying exactly these prefixes are ever swept.
const (
	archiveTmpDirPrefix = "rivulet-engine-archive-"    // per-session entry dirs (os.MkdirTemp)
	archiveTmpDLPrefix  = "rivulet-engine-archive-dl-" // downloaded archive files (os.CreateTemp)
)

// archiveSweepStale removes archive session temp dirs and downloaded archive
// files under root that no live session owns and that have not been modified
// within archiveSessionTTL. It reclaims space leaked when the process died
// before its janitor could evict sessions. The age guard keeps a session being
// created concurrently (or by another instance) safe.
func archiveSweepStale(root string) {
	live := map[string]struct{}{}
	archiveSessionsMu.Lock()
	markLive := func(sess *archiveSession) {
		sess.mu.Lock()
		live[sess.tmpDir] = struct{}{}
		live[sess.archivePath] = struct{}{}
		sess.mu.Unlock()
	}
	for _, sess := range archiveSessions {
		markLive(sess)
	}
	for sess := range archiveRetired { // replaced, but still being read
		markLive(sess)
	}
	archiveSessionsMu.Unlock()
	sweepStaleTemp(root, live, archiveSessionTTL, func(e os.DirEntry) bool {
		if e.IsDir() {
			return strings.HasPrefix(e.Name(), archiveTmpDirPrefix)
		}
		return e.Type().IsRegular() && strings.HasPrefix(e.Name(), archiveTmpDLPrefix)
	})
}

// sweepStaleTemp removes direct children of root that match, are not in live
// (full paths), and whose modification time is older than minAge. Symlinks are
// never followed or removed.
func sweepStaleTemp(root string, live map[string]struct{}, minAge time.Duration, match func(os.DirEntry) bool) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-minAge)
	for _, e := range entries {
		if e.Type()&os.ModeSymlink != 0 || !match(e) {
			continue
		}
		full := filepath.Join(root, e.Name())
		if _, ok := live[full]; ok {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		_ = os.RemoveAll(full)
	}
}

func archiveEvict() {
	now := time.Now()
	var victims []*archiveSession

	archiveSessionsMu.Lock()
	for k, sess := range archiveSessions {
		sess.mu.Lock()
		idle := now.Sub(sess.lastAccess)
		inUse := sess.refCount > 0
		sess.mu.Unlock()
		if !inUse && idle > archiveSessionTTL {
			victims = append(victims, sess)
			delete(archiveSessions, k)
		}
	}
	archiveSessionsMu.Unlock()

	// Perform disk I/O outside the lock so concurrent archive handlers are not
	// blocked while RemoveAll/Remove walk the filesystem.
	for _, v := range victims {
		archiveDestroySession(v)
	}
}

// archiveDestroyWait bounds how long archiveDestroySession waits for the
// session's background goroutines to stop. A variable so tests can lower it.
var archiveDestroyWait = 30 * time.Second

// archiveDestroySession cancels the session's background extractions and
// verifications, waits for them to let go of their files, and removes its temp
// dir (and, when it owns it, the downloaded archive). The caller must already
// have unregistered the session, and no request may still be reading it (see
// archiveRetireLocked). It blocks until the goroutines stopped (bounded by
// archiveDestroyWait), so callers run it off the request path.
func archiveDestroySession(sess *archiveSession) {
	sess.mu.Lock()
	flights := make([]*archiveExtractFlight, 0, len(sess.inflight))
	for _, fl := range sess.inflight {
		flights = append(flights, fl)
	}
	var verifies []*archiveVerify
	for _, d := range sess.direct {
		if d.v != nil {
			verifies = append(verifies, d.v)
		}
	}
	tmpDir, archPath, isTmp := sess.tmpDir, sess.archivePath, sess.isTempArch
	sess.mu.Unlock()

	for _, fl := range flights {
		if fl.cancel != nil {
			fl.cancel()
		}
	}
	for _, v := range verifies {
		v.cancel()
	}
	// Windows cannot delete a file that still has an open handle, and the
	// goroutines may be reading the archive or writing an entry right now:
	// removing the files first would leave them (a downloaded archive can be
	// gigabytes) behind until the next stale sweep.
	archiveAwaitStopped(flights, verifies)
	archiveRemoveAll(tmpDir)
	if isTmp {
		archiveRemove(archPath)
	}
}

// archiveAwaitStopped blocks until every flight and verification has finished
// (they were cancelled and notice at their next chunk) or archiveDestroyWait
// passed.
func archiveAwaitStopped(flights []*archiveExtractFlight, verifies []*archiveVerify) {
	if len(flights)+len(verifies) == 0 {
		return
	}
	timer := time.NewTimer(archiveDestroyWait)
	defer timer.Stop()
	for _, fl := range flights {
		select {
		case <-fl.done:
		case <-timer.C:
			logging.For("archive").Warn("session teardown gave up waiting for an extraction to stop")
			return
		}
	}
	for _, v := range verifies {
		select {
		case <-v.done:
		case <-timer.C:
			logging.For("archive").Warn("session teardown gave up waiting for a verification to stop")
			return
		}
	}
}

// archiveRemoveTestHook, when set, is called with the path right before the
// session machinery removes a temp file or directory. Tests only: Windows
// refuses to delete a file that still has an open handle, so they assert none
// is left open at that moment. An atomic pointer because removals run on
// background goroutines.
var archiveRemoveTestHook atomic.Pointer[func(path string)]

func archiveRemoveNotify(path string) {
	if h := archiveRemoveTestHook.Load(); h != nil {
		(*h)(path)
	}
}

// archiveRemove deletes a temp file the session owns. Every handle on it must
// already be closed (see archiveExtractFlight.dropFile).
func archiveRemove(path string) {
	archiveRemoveNotify(path)
	_ = os.Remove(path)
}

// archiveRemoveAll is archiveRemove for a temp directory tree.
func archiveRemoveAll(path string) {
	archiveRemoveNotify(path)
	_ = os.RemoveAll(path)
}

// archiveRetireLocked unregisters s's key for good after the key was re-created
// (the caller has already replaced the map entry under archiveSessionsMu, which
// it holds). A session no request is reading is returned as destroyNow: the
// caller destroys it. One that is being read — typically a progressive/ranged
// stream whose player re-requests /{ext}/create/{key} on every open or seek —
// is only marked retired: destroying it now would cancel the extraction those
// readers depend on and cut their response mid-body. Its last release() destroys
// it, so nothing leaks and nothing new can reach it (it is out of the map).
func archiveRetireLocked(s *archiveSession) (destroyNow bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refCount == 0 {
		return true
	}
	s.retired = true
	archiveRetired[s] = struct{}{}
	return false
}

// tempBytesLocked is the disk the session holds or has reserved: the
// downloaded archive, extracted entries, and in-flight extractions (counted at
// their declared size). Callers must hold sess.mu.
func (s *archiveSession) tempBytesLocked() int64 {
	total := s.archiveBytes
	for _, n := range s.extractedSize {
		total += n
	}
	for _, fl := range s.inflight {
		if fl.size >= 0 {
			total += fl.size
		} else {
			total += fl.progress()
		}
	}
	return total
}

// archiveEnforceCaps drops least-recently-used idle sessions until the global
// session-count and temp-byte caps hold again. keep (the session being created
// or extracted for) and any session with in-flight requests are never evicted.
//
// Eviction only happens where it helps the cap that is exceeded. A session
// that holds no bytes (served in place, or a local-path archive) is never
// evicted for the byte cap; and when even dropping every idle session could not
// bring the total under the byte cap — the overage belongs to the kept,
// in-use or retired sessions — nothing is evicted for it, since that would only
// 404 other users' next Range request without freeing the disk that matters.
func archiveEnforceCaps(keep *archiveSession) {
	type candidate struct {
		key   string
		sess  *archiveSession
		last  time.Time
		bytes int64
	}
	var (
		cands     []candidate
		total     int64
		candBytes int64 // what evicting every candidate would free
	)
	archiveSessionsMu.Lock()
	count := len(archiveSessions)
	for k, s := range archiveSessions {
		s.mu.Lock()
		b := s.tempBytesLocked()
		total += b
		if s != keep && s.refCount == 0 {
			cands = append(cands, candidate{key: k, sess: s, last: s.lastAccess, bytes: b})
			candBytes += b
		}
		s.mu.Unlock()
	}
	for s := range archiveRetired { // replaced sessions still being read hold disk too
		s.mu.Lock()
		total += s.tempBytesLocked()
		s.mu.Unlock()
	}
	if count <= archiveMaxSessions && total <= archiveMaxTempBytes {
		archiveSessionsMu.Unlock()
		return
	}
	slices.SortFunc(cands, func(a, b candidate) int { return a.last.Compare(b.last) })
	byteCapReachable := total-candBytes <= archiveMaxTempBytes
	var victims []*archiveSession
	var freed int64
	for _, c := range cands {
		overCount := count > archiveMaxSessions
		overBytes := byteCapReachable && total > archiveMaxTempBytes
		if !overCount && !overBytes {
			break
		}
		if !overCount && c.bytes == 0 {
			continue // frees nothing towards the byte cap
		}
		delete(archiveSessions, c.key)
		victims = append(victims, c.sess)
		count--
		total -= c.bytes
		freed += c.bytes
	}
	archiveSessionsMu.Unlock()

	for _, v := range victims {
		archiveDestroySession(v)
	}
	if len(victims) > 0 {
		logging.For("archive").Info("evicted idle archive sessions over cap",
			"sessions", len(victims), "freed_bytes", freed)
	}
}

// archiveAcquireSession looks key up and pins the session (refCount++) in one
// step under archiveSessionsMu, so eviction — which also decides under that
// lock — can never drop a session between lookup and pin. Callers must call
// release when done. It returns nil when the session does not exist.
func archiveAcquireSession(key string) *archiveSession {
	archiveSessionsMu.Lock()
	defer archiveSessionsMu.Unlock()
	sess, ok := archiveSessions[key]
	if !ok {
		return nil
	}
	sess.mu.Lock()
	sess.lastAccess = time.Now()
	sess.refCount++
	sess.mu.Unlock()
	return sess
}

// release drops the pin taken by archiveAcquireSession. The last release of a
// retired session (its key was re-created while requests were reading it)
// destroys it.
func (s *archiveSession) release() {
	s.mu.Lock()
	s.refCount--
	last := s.retired && s.refCount == 0
	s.mu.Unlock()
	if last {
		archiveSessionsMu.Lock()
		delete(archiveRetired, s)
		archiveSessionsMu.Unlock()
		go archiveDestroySession(s) // disk I/O off the request path
	}
}

// archiveIndexEntries maps entry name → entry; the first of duplicate names
// wins, matching what Reader.Open resolves.
func archiveIndexEntries(entries []archive.Entry) map[string]archive.Entry {
	m := make(map[string]archive.Entry, len(entries))
	for _, e := range entries {
		if _, ok := m[e.Name]; !ok {
			m[e.Name] = e
		}
	}
	return m
}

// entry returns the archive's record for name from the per-session listing,
// listing the archive at most once (create normally primes it; a session built
// without one lists lazily here). A failed listing is not cached.
func (s *archiveSession) entry(name string) (archive.Entry, error) {
	s.listMu.Lock()
	defer s.listMu.Unlock()
	if s.listing == nil {
		r, err := archive.OpenFile(s.archivePath, s.ext)
		if err != nil {
			return archive.Entry{}, fmt.Errorf("open archive: %w", err)
		}
		entries, err := r.List()
		_ = r.Close()
		if err != nil {
			return archive.Entry{}, fmt.Errorf("list archive: %w", err)
		}
		s.listing = archiveIndexEntries(entries)
	}
	e, ok := s.listing[name]
	if !ok {
		return archive.Entry{}, fmt.Errorf("entry %q %w", name, errArchiveEntryNotFound)
	}
	return e, nil
}

// ── create-payload parsing ────────────────────────────────────────────────────

type archivePayload struct {
	URL             string          `json:"url"`
	From            string          `json:"from"`
	URLs            json.RawMessage `json:"urls"`
	FileIdx         json.RawMessage `json:"fileIdx"`
	FileMustInclude json.RawMessage `json:"fileMustInclude"`
}

// archiveParsePayload decodes the request payload. It first checks the ?lz=
// query parameter (lz-string compressed JSON), then falls back to the request
// body. Both array and object forms are accepted; the first array element is
// used when the payload is an array.
func archiveParsePayload(r *http.Request) (*archivePayload, error) {
	var raw []byte

	if lzParam := r.URL.Query().Get("lz"); lzParam != "" {
		if len(lzParam) > archiveMaxLZEncoded {
			return nil, fmt.Errorf("lz param too large (%d bytes; limit %d)", len(lzParam), archiveMaxLZEncoded)
		}
		decoded, err := lzstring.DecompressFromEncodedURIComponent(lzParam)
		if err != nil {
			return nil, fmt.Errorf("lz decode: %w", err)
		}
		if len(decoded) > archiveMaxLZDecoded {
			return nil, fmt.Errorf("lz decoded payload too large (%d bytes; limit %d)", len(decoded), archiveMaxLZDecoded)
		}
		raw = []byte(decoded)
	} else if r.Body != nil {
		var err error
		raw, err = io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("empty payload: provide ?lz= query param or a JSON request body")
	}

	// Accept both [{…}] (array) and {…} (object).
	var arr []archivePayload
	if json.Unmarshal(raw, &arr) == nil && len(arr) > 0 {
		p := arr[0]
		return &p, nil
	}
	var obj archivePayload
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("parse payload JSON: %w", err)
	}
	return &obj, nil
}

// source returns the archive source URL/path. It accepts the legacy `url`/`from`
// string fields and the canonical `urls` array used by stremio-core, whose
// entries are `[url, bytes?]` tuples (it also tolerates `["url"]` strings or
// `{"url":…}` objects). The first entry is used.
func (p *archivePayload) source() string {
	if p.URL != "" {
		return p.URL
	}
	if p.From != "" {
		return p.From
	}
	if len(p.URLs) == 0 {
		return ""
	}
	var entries []json.RawMessage
	if json.Unmarshal(p.URLs, &entries) != nil || len(entries) == 0 {
		return ""
	}
	return archiveURLFromEntry(entries[0])
}

// sources returns every part URL (url/from first, else all `urls` entries).
func (p *archivePayload) sources() []string {
	if p.URL != "" {
		return []string{p.URL}
	}
	if p.From != "" {
		return []string{p.From}
	}
	var entries []json.RawMessage
	if len(p.URLs) == 0 || json.Unmarshal(p.URLs, &entries) != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if u := archiveURLFromEntry(e); u != "" {
			out = append(out, u)
		}
	}
	return out
}

// archiveURLFromEntry extracts a URL string from one `urls` entry, which may be
// a bare string, a `[url, bytes?]` tuple, or a `{"url":…}` object.
func archiveURLFromEntry(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var tuple []json.RawMessage
	if json.Unmarshal(raw, &tuple) == nil && len(tuple) > 0 {
		if json.Unmarshal(tuple[0], &s) == nil {
			return s
		}
	}
	var obj struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(raw, &obj) == nil {
		return obj.URL
	}
	return ""
}

// ── entry selection ──────────────────────────────────────────────────────────

// archiveVideoExts lists media container extensions that qualify as "video"
// files for the largest-video fallback selection heuristic.
var archiveVideoExts = map[string]bool{
	".mkv": true, ".mp4": true, ".avi": true, ".mov": true,
	".wmv": true, ".flv": true, ".webm": true, ".m4v": true,
	".mpg": true, ".mpeg": true, ".ts": true, ".m2ts": true,
}

// archiveSelectEntry picks a single file entry from entries using the
// following priority:
//  1. Explicit numeric index in payload.FileIdx.
//  2. First entry matching any fileMustInclude filter (case-insensitive substring).
//  3. Largest video-extension file; falls back to largest file overall.
func archiveSelectEntry(entries []archive.Entry, payload *archivePayload) (string, error) {
	// Collect non-directory entries.
	var files []archive.Entry
	for _, e := range entries {
		if !e.IsDir {
			files = append(files, e)
		}
	}
	if len(files) == 0 {
		return "", fmt.Errorf("archive contains no files")
	}

	// 1. Explicit index.
	if idx := archiveFileIdx(payload.FileIdx); idx >= 0 && idx < len(files) {
		return files[idx].Name, nil
	}

	// 2. fileMustInclude filter.
	if filters := archiveFilters(payload.FileMustInclude); len(filters) > 0 {
		lower := func(t string) string { return strings.ToLower(t) }
		for _, f := range files {
			for _, filt := range filters {
				if strings.Contains(lower(f.Name), lower(filt)) {
					return f.Name, nil
				}
			}
		}
	}

	// 3. Largest video file, or largest file overall as a final fallback.
	return archiveLargestVideo(files), nil
}

// archiveFileIdx decodes payload.fileIdx: a JSON number or a numeric string (a
// non-numeric string reads as 0, as it always has). It returns -1 when the
// field is absent, null or of any other type.
func archiveFileIdx(raw json.RawMessage) int {
	if len(raw) == 0 || string(raw) == "null" {
		return -1
	}
	var n int
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		idx, _ := strconv.Atoi(s)
		return idx
	}
	return -1
}

// archiveFilters decodes payload.fileMustInclude: a string array or a single
// string. It returns nil when the field is absent, null or of another type.
func archiveFilters(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var filters []string
	var s string
	if json.Unmarshal(raw, &filters) != nil {
		if json.Unmarshal(raw, &s) == nil {
			filters = []string{s}
		}
	}
	return filters
}

func archiveLargestVideo(files []archive.Entry) string {
	var best archive.Entry
	for _, f := range files {
		ext := strings.ToLower(filepath.Ext(f.Name))
		if archiveVideoExts[ext] && (best.Name == "" || f.Size > best.Size) {
			best = f
		}
	}
	if best.Name != "" {
		return best.Name
	}
	// Fallback: largest file regardless of extension.
	best = files[0]
	for _, f := range files[1:] {
		if f.Size > best.Size {
			best = f
		}
	}
	return best.Name
}

// ── SSRF pre-flight / guarded outbound fetch ────────────────────────────────
//
// api.go's getClient is a shared HTTP client reused by trusted-caller routes
// and is hard-coded to netguard.DialControl(false) (only the cloud-metadata
// address is blocked); it is not touched here. archiveDownload and
// nzb.go's nzbCreate both fetch a caller-supplied URL for an unauthenticated
// route, so both validate the target host up front (validateFetchHost) and
// then fetch through archiveFetchClient below, whose dialer re-validates the
// actual resolved IP at connect time and whose redirect policy re-validates
// every hop — closing the DNS-rebinding / redirect-to-loopback gap that a
// pure pre-flight check cannot foreclose by itself.

// archiveAllowPrivateHosts reports whether RIVULET_ARCHIVE_ALLOW_PRIVATE
// opts /create endpoints (archive download, NZB fetch) into reaching
// private/loopback/RFC1918 hosts. Default is false (secure): only public
// addresses are reachable. The cloud-metadata address is always blocked
// regardless, via netguard.ValidateIP's unconditional check.
func archiveAllowPrivateHosts() bool {
	v := strings.TrimSpace(os.Getenv("RIVULET_ARCHIVE_ALLOW_PRIVATE"))
	return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "on")
}

// archiveDialControl is archiveFetchClient's net.Dialer.Control hook. It
// re-reads archiveAllowPrivateHosts() on every dial — rather than baking the
// decision in once at package-init time — so the pooled client's behaviour
// always reflects the current opt-in state (mirrors ftpstream's
// ftpDialControl).
func archiveDialControl(network, address string, c syscall.RawConn) error {
	return netguard.DialControl(!archiveAllowPrivateHosts())(network, address, c)
}

// archiveMaxRedirects caps the number of redirects archiveFetchClient will
// follow, matching net/http's own default limit.
const archiveMaxRedirects = 5

// archiveCheckRedirect re-validates every redirect hop's target host before
// following it. Without this, a public URL that 30x-redirects to
// 127.0.0.1/RFC1918 would still be fetched: the dialer's Control hook only
// ever sees a resolved IP, so pairing it with a redirect-time host check
// (the same validateFetchHost used for the initial URL) closes the
// redirect-to-loopback gap, in addition to the dial-time DNS-rebinding
// guard.
func archiveCheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= archiveMaxRedirects {
		return fmt.Errorf("stopped after %d redirects", archiveMaxRedirects)
	}
	if err := validateFetchHost(req.URL.String()); err != nil {
		return err
	}
	return nil
}

// archiveFetchClient is the dedicated HTTP client for /create-style outbound
// fetches (archive download, NZB XML fetch): its dialer enforces the SSRF
// guard at connect time (see archiveDialControl) and its CheckRedirect
// re-validates every redirect target, unlike api.go's getClient which is
// shared with trusted-caller routes and only blocks cloud-metadata.
var archiveFetchClient = &http.Client{
	Timeout:       30 * time.Second,
	Transport:     newArchiveTransport(),
	CheckRedirect: archiveCheckRedirect,
}

// archiveDownloadClient is archiveFetchClient's sibling for large archive
// bodies (up to archiveMaxDownloadBytes): it has no overall Timeout, which
// would abort a slow-but-healthy transfer mid-body, but bounds time-to-first-
// byte via ResponseHeaderTimeout. It keeps the same SSRF guards (dial-time
// Control hook and per-hop CheckRedirect).
var archiveDownloadClient = &http.Client{
	Transport:     newArchiveTransport(),
	CheckRedirect: archiveCheckRedirect,
}

func newArchiveTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
			Control: archiveDialControl,
		}).DialContext,
		ResponseHeaderTimeout: 30 * time.Second,
	}
}

// archiveFetchGet performs a guarded GET against u via archiveFetchClient
// and returns up to maxBytes of the response body. Used by archiveDownload
// and nzb.go's nzbCreate (fetching the NZB XML) — both fetch a
// caller-supplied URL for an unauthenticated route, so neither may use
// api.go's getClient/httpGet.
func archiveFetchGet(u string, maxBytes int64) ([]byte, error) {
	resp, err := archiveFetchClient.Get(u)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxBytes))
}

// errFetchHostNotAllowed is returned by validateFetchHost for every blocked
// target, regardless of *why* the specific IP is blocked (cloud-metadata vs.
// private) or whether the host would otherwise be reachable. Callers surface
// this fixed message verbatim so the response never distinguishes "internal
// host exists and refused" from "internal host doesn't exist" from "internal
// host returned garbage" — closing the internal-port-scan oracle a
// differentiated error would otherwise provide.
var errFetchHostNotAllowed = errors.New("remote host not allowed")

// validateFetchHost resolves rawURL's host and rejects it when every
// candidate address is disallowed by netguard. It is a validate-then-fetch
// check (not a dial-time hook), so unlike netguard.DialControl it cannot by
// itself foreclose a DNS-rebinding TOCTOU window; it exists to close the
// coarse-grained "reach my LAN / cloud-metadata" SSRF on /create endpoints
// that download from a caller-supplied URL, which is the finding in scope.
// A DNS resolution failure is not itself treated as a block — the real fetch
// surfaces that error naturally, and it is not a distinguishing oracle here
// since it says nothing about the reachability of any internal address.
func validateFetchHost(rawURL string) error {
	if archiveAllowPrivateHosts() {
		return nil
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return errFetchHostNotAllowed
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, u.Hostname())
	if err != nil || len(addrs) == 0 {
		return nil
	}
	ips := make([]net.IP, len(addrs))
	for i, a := range addrs {
		ips[i] = a.IP
	}
	if anyAddrAllowed(ips) {
		return nil
	}
	return errFetchHostNotAllowed
}

// anyAddrAllowed reports whether at least one address in addrs is allowed by
// netguard.ValidateIP(ip, true) (public, i.e. neither cloud-metadata nor
// private/loopback/RFC1918). Factored out of validateFetchHost so the
// mixed-DNS-answer behavior — a hostname resolving to both a public and a
// private address passes this pre-flight check — is unit-testable with a
// literal []net.IP, without a real DNS lookup. This is intentionally a
// coarse pre-flight only: archiveFetchClient's dial-time Control hook is
// what actually forecloses the case where the connection ends up going to
// the disallowed address (DNS-rebinding or round-robin to the private
// answer).
func anyAddrAllowed(addrs []net.IP) bool {
	for _, ip := range addrs {
		if netguard.ValidateIP(ip, true) == nil {
			return true
		}
	}
	return false
}

// archiveLocalPathErrMsg is returned for every local-path rejection: local
// archives disabled, path escapes the configured root, or the path genuinely
// does not exist. Keeping the message and status identical across all three
// closes the filesystem-existence oracle (an attacker probing paths cannot
// distinguish "not configured" / "denied" / "not found").
const archiveLocalPathErrMsg = "local path not found"

// archiveLocalRoot returns the directory local archive sources are confined
// to, or "" when local-path archives are disabled (the default — without
// this the "url"/"from" field could name any file on disk; see
// archiveResolveLocalPath). RIVULET_ARCHIVE_LOCAL_ROOT is an archive-specific
// override; LOCAL_FILES_DIR (internal/api/localaddon.go's existing
// local-files-addon root) is reused as a fallback so operators who already
// mount a media directory get local-archive support without a second knob.
func archiveLocalRoot() string {
	if root := strings.TrimSpace(os.Getenv("RIVULET_ARCHIVE_LOCAL_ROOT")); root != "" {
		return root
	}
	return strings.TrimSpace(os.Getenv("LOCAL_FILES_DIR"))
}

// archiveResolveLocalPath confines source to archiveLocalRoot(), returning an
// error when local-path archives are disabled (no root configured) or when
// the resolved path escapes the root via ".." traversal or a symlink. Both
// the requested path and the configured root are resolved with
// filepath.EvalSymlinks so a symlink inside (or as) the root cannot be used
// to point outside it. When source itself does not exist yet,
// EvalSymlinks(source) fails, so its parent directory is resolved instead and
// the leaf name re-joined — the parent's symlink chain still cannot escape
// the root, and the subsequent os.Stat uniformly reports non-existence.
func archiveResolveLocalPath(source string) (string, error) {
	root := archiveLocalRoot()
	if root == "" {
		return "", errors.New("local archive sources are disabled")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", err
	}

	srcAbs, err := filepath.Abs(source)
	if err != nil {
		return "", err
	}
	real, err := filepath.EvalSymlinks(srcAbs)
	if err != nil {
		parent, perr := filepath.EvalSymlinks(filepath.Dir(srcAbs))
		if perr != nil {
			return "", perr
		}
		real = filepath.Join(parent, filepath.Base(srcAbs))
	}

	rel, err := filepath.Rel(rootReal, real)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes allowed root")
	}
	return real, nil
}

// archiveDownloadParts downloads every part in order and concatenates them
// into one temp file (split archives: .001/.002/…, raw multi-part bodies).
// The total size is still bounded by archiveMaxDownloadBytes.
func archiveDownloadParts(urls []string) (string, error) {
	out, err := os.CreateTemp("", archiveTmpDLPrefix+"*")
	if err != nil {
		return "", err
	}
	fail := func(e error) (string, error) {
		_ = out.Close()
		_ = os.Remove(out.Name())
		return "", e
	}
	var total int64
	for _, u := range urls {
		if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
			return fail(fmt.Errorf("part %q is not an http(s) URL", u))
		}
		part, err := archiveDownload(u)
		if err != nil {
			return fail(err)
		}
		f, err := os.Open(part)
		if err != nil {
			_ = os.Remove(part)
			return fail(err)
		}
		n, cerr := io.Copy(out, f)
		_ = f.Close()
		_ = os.Remove(part)
		if cerr != nil {
			return fail(cerr)
		}
		total += n
		if total > archiveMaxDownloadBytes {
			return fail(fmt.Errorf("combined parts exceed size limit (%d bytes)", archiveMaxDownloadBytes))
		}
	}
	name := out.Name()
	if err := out.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// ── helper: download / key / path-encoding ───────────────────────────────────

// archiveDownload fetches u and streams it to a new temp file.
// The caller owns the file and must remove it when done.
func archiveDownload(u string) (string, error) {
	if err := validateFetchHost(u); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := archiveDownloadClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body := &archiveCountingReader{r: resp.Body}
	stopWatch := archiveWatchStall(body, cancel)
	defer stopWatch()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	f, err := os.CreateTemp("", archiveTmpDLPrefix+"*")
	if err != nil {
		return "", err
	}
	// Cap at archiveMaxDownloadBytes; use +1 so we can detect an overflow by
	// checking whether we copied strictly more than the limit.
	n, copyErr := io.Copy(f, io.LimitReader(body, archiveMaxDownloadBytes+1))
	if copyErr != nil && errors.Is(context.Cause(ctx), errArchiveStalled) {
		copyErr = errArchiveStalled
	}
	if copyErr != nil || n > archiveMaxDownloadBytes {
		_ = f.Close()
		_ = os.Remove(f.Name())
		if copyErr != nil {
			return "", fmt.Errorf("download %s: %w", u, copyErr)
		}
		return "", fmt.Errorf("download %s: response exceeds size limit (%d bytes)", u, archiveMaxDownloadBytes)
	}
	name := f.Name()
	_ = f.Close()
	return name, nil
}

// Archive downloads have no overall timeout (see archiveDownloadClient), so an
// upstream that trickles bytes forever would hold the create request — and
// every waiter on its build flight — open until the size cap. Instead each
// window of archiveStallWindow must deliver at least archiveMinWindowBytes.
var (
	archiveStallWindow    = 60 * time.Second
	archiveMinWindowBytes = int64(1 << 20) // ≈17 KiB/s sustained
)

// errArchiveStalled reports a download below the minimum throughput.
var errArchiveStalled = errors.New("download stalled: below minimum throughput")

// archiveCountingReader counts bytes for archiveWatchStall.
type archiveCountingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *archiveCountingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// archiveWatchStall cancels the download (via cancel, with cause
// errArchiveStalled) when a window passes with fewer than
// archiveMinWindowBytes read. It also covers a fully silent upstream, which
// would otherwise block in Read forever. The returned stop ends the watch.
func archiveWatchStall(c *archiveCountingReader, cancel context.CancelCauseFunc) (stop func()) {
	done := make(chan struct{})
	window, minBytes := archiveStallWindow, archiveMinWindowBytes
	go func() {
		t := time.NewTicker(window)
		defer t.Stop()
		var last int64
		for {
			select {
			case <-done:
				return
			case <-t.C:
				cur := c.n.Load()
				if cur-last < minBytes {
					cancel(errArchiveStalled)
					return
				}
				last = cur
			}
		}
	}()
	return func() { close(done) }
}

// archiveNewKey returns a random 32-hex-char session key.
func archiveNewKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// archiveEncodePath URL-encodes each component of an archive entry path
// (which uses forward slashes as separators), preserving the slash structure.
func archiveEncodePath(name string) string {
	parts := strings.Split(name, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// ── extraction ───────────────────────────────────────────────────────────────

// archiveCopyBufSize is the buffer used to copy a decompressed entry to disk;
// progress is published to waiting readers once per buffer.
const archiveCopyBufSize = 256 << 10

// errArchiveOpenFile marks failures to open the extracted/archive file so the
// handler keeps its historical "open extracted file: …" message.
var errArchiveOpenFile = errors.New("open extracted file")

// errArchiveEntryNotFound marks a request for an entry name that is not in the
// archive: the caller's mistake, answered 404 rather than 500.
var errArchiveEntryNotFound = errors.New("not found in archive")

// archiveExtractFlight is one background extraction of a single archive entry
// into a temp file. The goroutine that started it does the work; every request
// for the entry (first or later) shares it. Readers may consume the file while
// it is still growing (archiveProgressReader), so time-to-first-byte no longer
// includes the whole extraction. The extraction runs on its own context — a
// client disconnect never cancels it; only session eviction does.
type archiveExtractFlight struct {
	done   chan struct{} // closed once the extraction finished; path/err are then final
	path   string        // temp file being written (immutable)
	err    error         // set (under mu) before done is closed
	size   int64         // declared entry size; -1 when the archive does not record it (immutable)
	cancel context.CancelFunc

	// Test hooks, captured from the package-level vars when the flight is
	// created so the goroutine never reads those vars (immutable).
	startHook func(entryName string)
	chunkHook func(entryName string, written int64)

	mu       sync.Mutex
	written  int64         // bytes written to path and safe to read (never beyond size when known)
	finished bool          // done has been (or is being) closed
	wake     chan struct{} // closed and replaced whenever written advances; closed for good on finish
	readers  int           // requests holding (or about to open) a handle on path: acquireReader … releaseReader
	dropped  bool          // the extraction failed: path is deleted once no reader holds it
	removed  bool          // path has been deleted
}

func newArchiveExtractFlight(path string, size int64, cancel context.CancelFunc) *archiveExtractFlight {
	return &archiveExtractFlight{
		done:   make(chan struct{}),
		path:   path,
		size:   size,
		cancel: cancel,
		wake:   make(chan struct{}),
	}
}

// acquireReader registers a request that is about to open path. It fails when
// the extraction already failed and dropped its file. Every success must be
// paired with a releaseReader after the request's handle (if it got one) is
// closed.
func (fl *archiveExtractFlight) acquireReader() bool {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	if fl.dropped {
		return false
	}
	fl.readers++
	return true
}

// releaseReader unregisters a reader whose handle is closed. The last one out
// removes the file of a failed extraction.
func (fl *archiveExtractFlight) releaseReader() {
	fl.mu.Lock()
	fl.readers--
	remove := fl.dropped && fl.readers == 0 && !fl.removed
	if remove {
		fl.removed = true
	}
	fl.mu.Unlock()
	if remove {
		archiveRemove(fl.path)
	}
}

// dropFile discards the partial file of a failed extraction. The extraction's
// own handle must already be closed. Windows cannot delete a file somebody
// still has open — a request may be reading it — so while a reader holds it the
// removal is left to the last releaseReader.
func (fl *archiveExtractFlight) dropFile() {
	fl.mu.Lock()
	fl.dropped = true
	remove := fl.readers == 0 && !fl.removed
	if remove {
		fl.removed = true
	}
	fl.mu.Unlock()
	if remove {
		archiveRemove(fl.path)
	}
}

// advance publishes that n bytes of the entry are on disk and wakes waiters.
func (fl *archiveExtractFlight) advance(n int64) {
	fl.mu.Lock()
	fl.written = n
	close(fl.wake)
	fl.wake = make(chan struct{})
	fl.mu.Unlock()
}

// finish records the final outcome and releases every waiter.
func (fl *archiveExtractFlight) finish(err error) {
	fl.mu.Lock()
	fl.err = err
	fl.finished = true
	close(fl.wake)
	fl.mu.Unlock()
	close(fl.done)
}

// progress returns the number of bytes currently readable.
func (fl *archiveExtractFlight) progress() int64 {
	fl.mu.Lock()
	defer fl.mu.Unlock()
	return fl.written
}

// waitFor blocks until at least n bytes are readable, the extraction failed or
// ended short of n, or ctx is done. It returns the readable byte count.
func (fl *archiveExtractFlight) waitFor(ctx context.Context, n int64) (int64, error) {
	for {
		fl.mu.Lock()
		w, ch, fin, ferr := fl.written, fl.wake, fl.finished, fl.err
		fl.mu.Unlock()
		if fin && ferr != nil {
			return w, ferr
		}
		if w >= n {
			return w, nil
		}
		if fin {
			return w, io.ErrUnexpectedEOF // finished cleanly but shorter than promised
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return w, ctx.Err()
		}
	}
}

// waitDone blocks until the extraction finished (returning its error) or ctx is done.
func (fl *archiveExtractFlight) waitDone(ctx context.Context) error {
	select {
	case <-fl.done:
		return fl.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var (
	// archiveExtractTestHook, when non-nil, is called at the start of every real
	// (non-cached, non-waiting) extraction. Tests only.
	archiveExtractTestHook func(entryName string)
	// archiveExtractChunkHook, when non-nil, is called after each chunk of an
	// extraction is written and published, with the readable byte count. Tests only.
	archiveExtractChunkHook func(entryName string, written int64)
)

// archiveStartExtract returns how to obtain entryName as a file: either the
// path of an already extracted copy (flight == nil) or the in-progress flight,
// which it starts when none exists. Setup errors (unknown entry, declared size
// over the limit, temp file creation) are returned synchronously and not
// cached. Concurrent calls for the same entry share one flight (single-flight).
func archiveStartExtract(sess *archiveSession, entryName string) (*archiveExtractFlight, string, error) {
	if fl, p, ok := sess.lookupExtraction(entryName); ok {
		return fl, p, nil
	}

	entry, err := sess.entry(entryName)
	if err != nil {
		return nil, "", err
	}
	// The declared (uncompressed) size caps the extraction and prevents
	// zip-bomb decompression; a streamed RAR records none and is bounded by
	// archiveMaxEntryBytes instead.
	size, declared := entry.Size, entry.Size
	if entry.SizeUnknown {
		size, declared = -1, archiveMaxEntryBytes
	}
	if declared > archiveMaxEntryBytes {
		return nil, "", fmt.Errorf("entry %q: declared size %d exceeds limit %d bytes", entryName, declared, archiveMaxEntryBytes)
	}
	f, err := os.CreateTemp(sess.tmpDir, "entry-*"+filepath.Ext(entryName))
	if err != nil {
		return nil, "", fmt.Errorf("create temp: %w", err)
	}

	sess.mu.Lock()
	// Lost a race with a concurrent first request: join it instead.
	if p, ok := sess.extracted[entryName]; ok {
		sess.mu.Unlock()
		_ = f.Close()
		archiveRemove(f.Name())
		return nil, p, nil
	}
	if fl, ok := sess.inflight[entryName]; ok {
		sess.mu.Unlock()
		_ = f.Close()
		archiveRemove(f.Name())
		return fl, "", nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	fl := newArchiveExtractFlight(f.Name(), size, cancel)
	fl.startHook, fl.chunkHook = archiveExtractTestHook, archiveExtractChunkHook
	if sess.inflight == nil {
		sess.inflight = make(map[string]*archiveExtractFlight)
	}
	sess.inflight[entryName] = fl
	sess.mu.Unlock()

	go archiveRunExtract(ctx, sess, entryName, fl, f, declared)
	go archiveEnforceCaps(sess) // the new flight reserves disk; make room from idle sessions
	return fl, "", nil
}

// lookupExtraction reports an already extracted copy or an in-progress flight.
func (s *archiveSession) lookupExtraction(entryName string) (*archiveExtractFlight, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := s.extracted[entryName]; ok {
		return nil, p, true
	}
	if fl, ok := s.inflight[entryName]; ok {
		return fl, "", true
	}
	return nil, "", false
}

// archiveRunExtract is the body of a flight's goroutine. It always discards the
// partial file on failure (once nobody has it open) and always finishes the
// flight.
func archiveRunExtract(ctx context.Context, sess *archiveSession, entryName string, fl *archiveExtractFlight, f *os.File, declared int64) {
	defer fl.cancel()
	err := archiveCopyEntry(ctx, sess, entryName, fl, f, declared)
	if cerr := f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("extract %q: %w", entryName, cerr)
	}
	if err != nil {
		fl.dropFile() // after f.Close: the writer's handle is gone
	}

	sess.mu.Lock()
	delete(sess.inflight, entryName)
	if err == nil {
		if sess.extracted == nil {
			sess.extracted = make(map[string]string)
		}
		if sess.extractedSize == nil {
			sess.extractedSize = make(map[string]int64)
		}
		sess.extracted[entryName] = fl.path
		sess.extractedSize[entryName] = fl.progress()
	} else {
		sess.noReuse = true // a create retried for this payload must rebuild, not replay the failure
	}
	sess.mu.Unlock()
	fl.finish(err)
}

// archiveCopyEntry streams the decompressed entry into f, publishing progress
// after every chunk. At most declared bytes become readable; if the archive
// yields more, the extraction fails (zip-bomb guard) and the file is dropped.
func archiveCopyEntry(ctx context.Context, sess *archiveSession, entryName string, fl *archiveExtractFlight, f *os.File, declared int64) error {
	if fl.startHook != nil {
		fl.startHook(entryName)
	}
	r, err := archive.OpenFile(sess.archivePath, sess.ext)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer func() { _ = r.Close() }()
	rc, err := r.Open(entryName)
	if err != nil {
		return fmt.Errorf("open entry %q: %w", entryName, err)
	}
	defer func() { _ = rc.Close() }()

	// LimitReader(declared+1) lets us detect an over-long entry by reading one
	// byte past the declared size without ever writing more than that.
	src := io.LimitReader(rc, declared+1)
	buf := make([]byte, archiveCopyBufSize)
	var n int64
	for {
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("extract %q: %w", entryName, cerr)
		}
		nr, rerr := src.Read(buf)
		if nr > 0 {
			if _, werr := f.Write(buf[:nr]); werr != nil {
				return fmt.Errorf("extract %q: %w", entryName, werr)
			}
			n += int64(nr)
			visible := archiveMin64(n, declared)
			fl.advance(visible)
			if fl.chunkHook != nil {
				fl.chunkHook(entryName, visible)
			}
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			return fmt.Errorf("extract %q: %w", entryName, rerr)
		}
	}
	if n > declared {
		return fmt.Errorf("extract %q: content (%d bytes) exceeds declared size (%d bytes)", entryName, n, declared)
	}
	return nil
}

// ── serving ──────────────────────────────────────────────────────────────────

// archiveMin64 is the smaller of a and b. The builtin min is shadowed by a
// package-level test helper, so it cannot be used in this package.
func archiveMin64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// archiveProgressReader is an io.ReadSeekCloser over an entry that is still
// being extracted. Read blocks (cancellable via ctx) until the requested bytes
// are on disk, so Range requests are answered as soon as their bytes exist. The
// final chunk is only released once the whole extraction has verified OK (zip
// CRC/size checks run at EOF), so a corrupt entry ends in a truncated response
// rather than a clean-looking one.
type archiveProgressReader struct {
	ctx  context.Context
	f    *os.File
	fl   *archiveExtractFlight
	size int64
	pos  int64
	held bool // registered with fl (acquireReader); Close releases it
}

func (p *archiveProgressReader) Read(b []byte) (int, error) {
	if p.pos >= p.size {
		return 0, io.EOF
	}
	if len(b) == 0 {
		return 0, nil
	}
	avail, err := p.fl.waitFor(p.ctx, p.pos+1)
	if err != nil {
		return 0, err
	}
	n := archiveMin64(archiveMin64(int64(len(b)), p.size-p.pos), avail-p.pos)
	if p.pos+n >= p.size {
		if err := p.fl.waitDone(p.ctx); err != nil {
			return 0, err
		}
	}
	nr, err := p.f.ReadAt(b[:n], p.pos)
	p.pos += int64(nr)
	if nr > 0 && errors.Is(err, io.EOF) {
		err = nil
	}
	return nr, err
}

func (p *archiveProgressReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = p.pos + offset
	case io.SeekEnd:
		abs = p.size + offset
	default:
		return 0, errors.New("archive: invalid seek whence")
	}
	if abs < 0 {
		return 0, errors.New("archive: negative seek position")
	}
	p.pos = abs
	return abs, nil
}

func (p *archiveProgressReader) Close() error {
	err := p.f.Close()
	if p.held { // the handle is closed: a failed extraction's file may go now
		p.held = false
		p.fl.releaseReader()
	}
	return err
}

// archiveVerify is the CRC-32 verification of one stored zip entry served in
// place. It runs once per session and entry, in the background, on its own
// context (a client disconnect never cancels it; only session destruction does).
type archiveVerify struct {
	done   chan struct{} // closed once err is final
	err    error         // nil: the bytes match the recorded CRC-32 (set before done is closed)
	cancel context.CancelFunc
}

// finished reports whether the verdict is final.
func (v *archiveVerify) finished() bool {
	select {
	case <-v.done:
		return true
	default:
		return false
	}
}

// wait blocks until the verdict is final (returning it) or ctx is done.
func (v *archiveVerify) wait(ctx context.Context) error {
	select {
	case <-v.done:
		return v.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var (
	// archiveEagerVerifyBytes is the largest stored entry that is verified
	// before its first byte is served. Beyond it, serving starts at once and
	// only the entry's final chunk waits for the verification. A variable so
	// tests can lower it.
	archiveEagerVerifyBytes int64 = 16 << 20
	// archiveVerifyTestHook, when non-nil, is called before each read of a
	// verification with the offset (within the entry) about to be hashed.
	// Tests only.
	archiveVerifyTestHook func(entryName string, off int64)
)

// archiveVerifyReaderAt reports each read to a test hook.
type archiveVerifyReaderAt struct {
	r      io.ReaderAt
	base   int64 // archive offset of the entry
	name   string
	onRead func(entryName string, off int64)
}

func (a archiveVerifyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	a.onRead(a.name, off-a.base)
	return a.r.ReadAt(p, off)
}

// archiveRunVerify is the body of a verification goroutine: it hashes the
// extent and publishes the verdict.
func archiveRunVerify(ctx context.Context, archivePath, name string, ext archive.Extent, v *archiveVerify, hook func(string, int64)) {
	defer v.cancel()
	v.err = archiveVerifyExtent(ctx, archivePath, name, ext, hook)
	close(v.done)
}

// archiveVerifyExtent hashes the extent through a reader that opens the archive
// only for each read: the verification of a large entry can run for minutes in
// the background and must not pin the user's archive for that long.
func archiveVerifyExtent(ctx context.Context, archivePath, name string, ext archive.Extent, hook func(string, int64)) error {
	r, err := archive.OpenReaderAt(archivePath)
	if err != nil {
		return fmt.Errorf("verify %q: %w", name, err)
	}
	if hook != nil {
		r = archiveVerifyReaderAt{r: r, base: ext.Offset, name: name, onRead: hook}
	}
	if err := ext.Verify(ctx, r); err != nil {
		return fmt.Errorf("verify %q: %w", name, err)
	}
	return nil
}

// archiveSectionFile serves an in-place extent of the archive file. While the
// extent's CRC-32 is still being verified (v != nil), a Read that could reach
// the entry's last byte waits for the verdict and fails if it is negative, so a
// corrupt entry ends in a truncated response rather than a clean-looking one.
// It deliberately exposes only Read/Seek/Close (no ReadAt) so nothing can
// bypass that gate.
type archiveSectionFile struct {
	sr   *io.SectionReader
	f    *os.File
	ctx  context.Context
	size int64
	v    *archiveVerify // nil once verified (or when nothing is recorded)
}

func (s *archiveSectionFile) Read(b []byte) (int, error) {
	if s.v != nil && len(b) > 0 {
		pos, err := s.sr.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		if pos+int64(len(b)) >= s.size {
			if err := s.v.wait(s.ctx); err != nil {
				return 0, err
			}
			s.v = nil
		}
	}
	return s.sr.Read(b)
}

func (s *archiveSectionFile) Seek(offset int64, whence int) (int64, error) {
	return s.sr.Seek(offset, whence)
}

func (s *archiveSectionFile) Close() error { return s.f.Close() }

func archiveOpenFile(path string) (io.ReadSeekCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errArchiveOpenFile, err)
	}
	return f, nil
}

// archiveDirectExtent reports where a stored zip/tar entry's bytes sit inside
// the archive file, so it can be served with ReadAt and no extraction at all.
// The verdict is cached per entry. Compressed, encrypted, directory, sparse and
// unknown-size entries (and every other archive format) return a zero value
// (ok false). For a stored zip entry the first call also starts the one
// background CRC-32 verification of its bytes (archiveVerify) that every
// request for the entry then consults.
func archiveDirectExtent(sess *archiveSession, name string) archiveDirect {
	if sess.ext != "zip" && sess.ext != "tar" {
		return archiveDirect{}
	}
	sess.mu.Lock()
	d, known := sess.direct[name]
	sess.mu.Unlock()
	if known {
		return d
	}
	entry, err := sess.entry(name)
	if err != nil {
		return archiveDirect{} // the extraction path reports the error
	}
	if !entry.IsDir && !entry.SizeUnknown && entry.Size <= archiveMaxEntryBytes {
		r, err := archive.OpenFile(sess.archivePath, sess.ext)
		if err != nil {
			return archiveDirect{}
		}
		if loc, ok := r.(archive.Locator); ok {
			if ext, ok := loc.Locate(name); ok && ext.Size == entry.Size {
				d = archiveDirect{ext: ext, ok: true}
			}
		}
		_ = r.Close()
	}

	sess.mu.Lock()
	if cur, ok := sess.direct[name]; ok {
		// Lost a race with a concurrent first request: share its verdict (and
		// its verification) instead of starting a second one.
		sess.mu.Unlock()
		return cur
	}
	var (
		vctx context.Context
		hook func(string, int64)
	)
	if d.ok && d.ext.HasCRC {
		var cancel context.CancelFunc
		vctx, cancel = context.WithCancel(context.Background())
		d.v = &archiveVerify{done: make(chan struct{}), cancel: cancel}
		hook = archiveVerifyTestHook
	}
	if sess.direct == nil {
		sess.direct = make(map[string]archiveDirect)
	}
	sess.direct[name] = d
	sess.mu.Unlock()
	if d.v != nil {
		go func() {
			archiveRunVerify(vctx, sess.archivePath, name, d.ext, d.v, hook)
			if errors.Is(d.v.err, archive.ErrChecksum) {
				sess.markFailed() // the bytes on disk do not match: a retried create must rebuild
			}
		}()
	}
	return d
}

// archiveDisableDirect records that name must not be served in place any more
// (its bytes could not be verified); requests then extract it.
func (s *archiveSession) archiveDisableDirect(name string) {
	s.mu.Lock()
	s.direct[name] = archiveDirect{}
	s.mu.Unlock()
}

// archiveOpenInPlace opens d for serving straight from the archive file. A
// stored zip entry is CRC-checked like zip.File.Open does at EOF, but without
// extracting it:
//   - entries up to archiveEagerVerifyBytes are verified before the first byte,
//     so no response (not even a ranged one) can carry unverified bytes;
//   - larger entries start serving at once and withhold their final chunk until
//     the background verification succeeded (archiveSectionFile), the same
//     contract as archiveProgressReader. Ranges that end before the entry's end
//     are answered unverified; a mismatch found later only affects requests that
//     reach the end or arrive afterwards.
//
// A mismatch is returned as an error (a clean 500 before any byte). A failed
// verification that is not a mismatch (I/O error) makes it return (nil, nil):
// the caller extracts the entry instead.
func archiveOpenInPlace(ctx context.Context, sess *archiveSession, name string, d archiveDirect) (io.ReadSeekCloser, error) {
	gate := d.v
	if gate != nil && d.ext.Size <= archiveEagerVerifyBytes {
		select {
		case <-gate.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if gate != nil && gate.finished() {
		if err := gate.err; err != nil {
			if errors.Is(err, archive.ErrChecksum) {
				sess.markFailed()
				return nil, fmt.Errorf("entry %q: %w", name, err)
			}
			sess.archiveDisableDirect(name)
			return nil, nil
		}
		gate = nil // verified: nothing to wait for
	}
	f, err := os.Open(sess.archivePath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errArchiveOpenFile, err)
	}
	return &archiveSectionFile{
		sr: io.NewSectionReader(f, d.ext.Offset, d.ext.Size), f: f,
		ctx: ctx, size: d.ext.Size, v: gate,
	}, nil
}

// archiveOpenEntry returns a seekable body for entryName. Stored zip/tar
// entries are served in place (after/while their zip CRC-32 is verified, see
// archiveOpenInPlace); others come from a background extraction that is
// consumed while it is still running. Errors detectable before any byte is
// produced (unknown entry, unopenable/corrupt entry start, oversize, a stored
// entry whose CRC-32 does not match) are returned here so the handler can still
// answer 500.
func archiveOpenEntry(ctx context.Context, sess *archiveSession, entryName string) (io.ReadSeekCloser, error) {
	if d := archiveDirectExtent(sess, entryName); d.ok {
		body, err := archiveOpenInPlace(ctx, sess, entryName, d)
		if body != nil || err != nil {
			return body, err
		}
		// The in-place bytes could not be checked: extract (and verify) instead.
	}

	fl, path, err := archiveStartExtract(sess, entryName)
	if err != nil {
		return nil, err
	}
	if fl == nil {
		return archiveOpenFile(path)
	}
	if fl.size < 0 {
		// Streamed RAR: the size is only known once the entry is fully out.
		if err := fl.waitDone(ctx); err != nil {
			return nil, err
		}
		return archiveOpenFile(fl.path)
	}

	// Register before opening: a failed extraction removes its file only once
	// no registered reader is left (Windows cannot delete an open file).
	if !fl.acquireReader() {
		// The extraction already failed and dropped its file; finish is imminent.
		if err := fl.waitDone(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: extraction file is gone", errArchiveOpenFile)
	}
	f, err := os.Open(fl.path)
	if err != nil {
		fl.releaseReader()
		select {
		case <-fl.done:
			if fl.err != nil {
				return nil, fl.err // the extraction already failed and dropped its file
			}
		default:
		}
		return nil, fmt.Errorf("%w: %w", errArchiveOpenFile, err)
	}
	// Wait for the first byte (or the end, for an empty entry) so an entry that
	// cannot be opened or decoded still yields a clean 500 instead of a 200 that
	// dies after the headers.
	if fl.size == 0 {
		err = fl.waitDone(ctx)
	} else {
		_, err = fl.waitFor(ctx, 1)
	}
	if err != nil {
		_ = f.Close()
		fl.releaseReader() // after the close: it may remove the failed file
		return nil, err
	}
	return &archiveProgressReader{ctx: ctx, f: f, fl: fl, size: fl.size, held: true}, nil
}

// ── handler entry point ──────────────────────────────────────────────────────

// handleArchive is the top-level dispatcher for /{ext}/… routes where ext is
// one of zip, rar, 7zip, tar, tgz. It is wired by route() in api.go.
func (s *server) handleArchive(w http.ResponseWriter, r *http.Request, seg []string, ext string) {
	archiveStartJanitor()

	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}

	switch seg[1] {
	case "create":
		key := ""
		if len(seg) >= 3 {
			key = seg[2]
		}
		s.archiveHandleCreate(w, r, seg, ext, key)
	case "stream":
		s.archiveHandleStream(w, r, seg, ext)
	default:
		http.NotFound(w, r)
	}
}

// archiveHandleCreate processes /{ext}/create and /{ext}/create/{key}.
//
//   - POST → resolve archive, select entry, store session, respond {"key":…}.
//   - GET  → same, then 307-redirect to the stream URL.
//
// An equivalent create for a key whose session is still usable reuses that
// session (no download, no extraction) — see the package comment.
func (s *server) archiveHandleCreate(w http.ResponseWriter, r *http.Request, seg []string, ext, key string) {
	payload, err := archiveParsePayload(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Resolve source URL or local path.
	source := payload.source()
	if source == "" {
		http.Error(w, "payload missing url or from field", http.StatusBadRequest)
		return
	}

	// A caller-supplied key is how players re-request the same stream (every
	// open/seek): an equivalent payload keeps the live session — and the
	// download and extractions it holds — instead of building a new one.
	// Identical creates in flight share one build. Without a key every create
	// gets a fresh random key, so there is nothing to reuse.
	var res archiveCreateResult
	if key == "" {
		res = archiveBuildAndRegister(ext, "", "", nil, payload, source)
	} else {
		var ok bool
		res, ok = archiveCreateOrReuse(r.Context(), ext, key, archiveCreateSig(ext, payload), payload, source)
		if !ok {
			return // the client went away while waiting for an identical build
		}
	}
	if res.status != 0 {
		http.Error(w, res.msg, res.status)
		return
	}

	if r.Method == http.MethodPost {
		writeJSON(w, http.StatusOK, map[string]string{"key": res.key})
		return
	}
	// GET → redirect straight to the stream URL.
	http.Redirect(w, r,
		"/"+ext+"/stream/"+url.PathEscape(res.key)+"/"+archiveEncodePath(res.selected),
		http.StatusTemporaryRedirect)
}

// archiveCreateResult is the outcome of a create: the session's key and
// default entry, or the error response (status != 0) that every request which
// shared the build answers with.
type archiveCreateResult struct {
	key      string
	selected string
	status   int
	msg      string
}

// archiveBuildKey identifies one in-progress build: a key and the normalized
// payload it builds for.
type archiveBuildKey struct{ key, sig string }

// archiveBuildFlight is one in-progress build of a keyed session. The request
// that started it is the leader; identical requests wait on done and answer
// with res.
type archiveBuildFlight struct {
	done chan struct{}
	res  archiveCreateResult // set before done is closed
}

// archiveFileStat is the identity of an archive file for staleness checks.
type archiveFileStat struct {
	size  int64
	mtime time.Time
	ok    bool // false when the file could not be stat'ed: never matches
}

func archiveStatFile(path string) archiveFileStat {
	fi, err := os.Stat(path)
	if err != nil {
		return archiveFileStat{}
	}
	return archiveFileStat{size: fi.Size(), mtime: fi.ModTime(), ok: true}
}

// matches reports whether path still has the recorded size and mtime.
func (a archiveFileStat) matches(path string) bool {
	if !a.ok {
		return false
	}
	cur := archiveStatFile(path)
	return cur.ok && cur.size == a.size && cur.mtime.Equal(a.mtime)
}

// archiveCreateSig is the normalized create payload: everything the built
// session depends on besides the archive's bytes. It is computed from the
// decoded payload, not the raw JSON, so equivalent spellings of a request
// (array vs object, url vs from vs urls, null or omitted options, a numeric
// fileIdx as a number or a string, any case of fileMustInclude) share one
// signature.
func archiveCreateSig(ext string, p *archivePayload) string {
	idx := archiveFileIdx(p.FileIdx)
	if idx < 0 {
		idx = -1 // none
	}
	filters := archiveFilters(p.FileMustInclude)
	for i, f := range filters {
		filters[i] = strings.ToLower(f) // matching is case-insensitive
	}
	b, err := json.Marshal(struct {
		Ext     string
		Sources []string
		FileIdx int
		Filters []string
	}{ext, p.sources(), idx, filters})
	if err != nil {
		return "" // never matches: always rebuilt
	}
	return string(b)
}

// archiveCreateOrReuse answers a keyed create. In order: an equivalent live
// session is reused; an identical build already in progress is waited for;
// otherwise this request builds (and replaces whatever the key held). It
// returns ok=false when ctx ends while waiting.
func archiveCreateOrReuse(ctx context.Context, ext, key, sig string, payload *archivePayload, source string) (res archiveCreateResult, ok bool) {
	if sig == "" {
		return archiveBuildAndRegister(ext, key, sig, nil, payload, source), true
	}
	for {
		sess, fl, leader := archiveReuseOrLead(key, sig)
		switch {
		case sess != nil:
			// The file checks hit the disk, so they run outside every lock; the
			// pin keeps the session from being evicted meanwhile.
			fresh := sess.sourceUnchanged()
			if !fresh {
				sess.markFailed() // stale for good: the retry — and every later create — rebuilds
			}
			selected := sess.selectedFile
			sess.release()
			if fresh {
				return archiveCreateResult{key: key, selected: selected}, true
			}
		case leader:
			return archiveBuildAndRegister(ext, key, sig, fl, payload, source), true
		default:
			select {
			case <-fl.done:
				return fl.res, true
			case <-ctx.Done():
				return archiveCreateResult{}, false
			}
		}
	}
}

// archiveReuseOrLead atomically decides what a keyed create does. It returns
// the live session to reuse — pinned (refCount) and with lastAccess refreshed;
// the caller must release it — or the build already in progress for the same
// payload to wait for, or (leader) a new flight this request must complete.
// Doing all of it under one lock means a create can never slip between a
// finishing build and its registration and build a duplicate.
func archiveReuseOrLead(key, sig string) (sess *archiveSession, fl *archiveBuildFlight, leader bool) {
	archiveSessionsMu.Lock()
	defer archiveSessionsMu.Unlock()
	if cur, ok := archiveSessions[key]; ok && cur.pinIfReusable(sig, time.Now()) {
		return cur, nil, false
	}
	bk := archiveBuildKey{key, sig}
	if cur, ok := archiveBuilds[bk]; ok {
		return nil, cur, false
	}
	fl = &archiveBuildFlight{done: make(chan struct{})}
	archiveBuilds[bk] = fl
	return nil, fl, true
}

// pinIfReusable pins s (refCount++, lastAccess refreshed) when an equivalent
// create may reuse it: same payload, not failed or retired, and within the TTL
// since it was built (and since it was last used). Callers hold archiveSessionsMu.
func (s *archiveSession) pinIfReusable(sig string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sig != sig || s.noReuse || s.retired ||
		now.Sub(s.created) >= archiveSessionTTL || now.Sub(s.lastAccess) >= archiveSessionTTL {
		return false
	}
	s.refCount++
	s.lastAccess = now
	return true
}

// markFailed records that the session must not be reused by an equivalent
// create any more (failed extraction, checksum mismatch, changed source file).
func (s *archiveSession) markFailed() {
	s.mu.Lock()
	s.noReuse = true
	s.mu.Unlock()
}

// sourceUnchanged reports whether the archive the session was built from is
// still what it was. A local archive must resolve to the same file (symlinks
// and the configured root are re-checked) with unchanged size and mtime; a
// downloaded archive must still be intact on disk. A remote URL is not asked
// again: it is trusted for the session's TTL like any cache entry.
func (s *archiveSession) sourceUnchanged() bool {
	if !s.isTempArch {
		real, err := archiveResolveLocalPath(s.source)
		if err != nil || real != s.archivePath {
			return false
		}
	}
	return s.built.matches(s.archivePath)
}

// archiveBuildAndRegister builds a session and registers it under its key. When
// fl is non-nil the caller leads that flight, and it is always completed — also
// on failure or panic — so waiters are never stranded.
func archiveBuildAndRegister(ext, key, sig string, fl *archiveBuildFlight, payload *archivePayload, source string) (res archiveCreateResult) {
	bk := archiveBuildKey{key, sig}
	if fl != nil {
		res = archiveCreateResult{status: http.StatusInternalServerError, msg: "archive session build aborted"}
		defer func() {
			archiveSessionsMu.Lock()
			if archiveBuilds[bk] == fl {
				delete(archiveBuilds, bk)
			}
			archiveSessionsMu.Unlock()
			fl.res = res
			close(fl.done)
		}()
	}

	sess, fail := archiveBuildSession(ext, key, sig, payload, source)
	if sess == nil {
		res = fail
		return res
	}

	archiveSessionsMu.Lock()
	old, replaced := archiveSessions[sess.key]
	archiveSessions[sess.key] = sess
	destroyOld := false
	if replaced && old.tmpDir != "" {
		destroyOld = archiveRetireLocked(old)
	}
	if fl != nil && archiveBuilds[bk] == fl {
		delete(archiveBuilds, bk) // same critical section: no window with neither flight nor session
	}
	archiveSessionsMu.Unlock()
	if destroyOld {
		// A prior session used this key and nothing is reading it. Remove its
		// temp dir (and, if the archive itself was a downloaded temp file, the
		// archive too) asynchronously to avoid leaking disk space without
		// blocking the request. A session that is still being read is instead
		// retired: its last release() destroys it (see archiveRetireLocked).
		go archiveDestroySession(old)
	}
	go archiveEnforceCaps(sess)
	res = archiveCreateResult{key: sess.key, selected: sess.selectedFile}
	return res
}

// archiveBuildSession resolves (downloads) the archive, lists it, selects the
// default entry and returns a new, unregistered session — or the error
// response to send. An empty key gets a fresh random one.
func archiveBuildSession(ext, key, sig string, payload *archivePayload, source string) (*archiveSession, archiveCreateResult) {
	failure := func(status int, msg string) (*archiveSession, archiveCreateResult) {
		return nil, archiveCreateResult{status: status, msg: msg}
	}

	var archivePath string
	var isTempArch bool

	if sources := payload.sources(); len(sources) > 1 && strings.HasPrefix(source, "http") {
		var err error
		archivePath, err = archiveDownloadParts(sources)
		if err != nil {
			return failure(http.StatusBadGateway, "download failed: "+err.Error())
		}
		isTempArch = true
	} else if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		var err error
		archivePath, err = archiveDownload(source)
		if err != nil {
			return failure(http.StatusBadGateway, "download failed: "+err.Error())
		}
		isTempArch = true
	} else {
		resolved, resolveErr := archiveResolveLocalPath(source)
		if resolveErr != nil {
			return failure(http.StatusBadRequest, archiveLocalPathErrMsg)
		}
		if _, statErr := os.Stat(resolved); statErr != nil {
			return failure(http.StatusBadRequest, archiveLocalPathErrMsg)
		}
		archivePath = resolved
	}
	// Taken before the archive is read: a file changed after this point no
	// longer matches, so a stale listing is never reused.
	built := archiveStatFile(archivePath)
	dropTemp := func() {
		if isTempArch {
			_ = os.Remove(archivePath)
		}
	}

	// Open archive, list entries, select the target file.
	ar, err := archive.OpenFile(archivePath, ext)
	if err != nil {
		dropTemp()
		return failure(http.StatusUnprocessableEntity, "open archive: "+err.Error())
	}
	entries, listErr := ar.List()
	_ = ar.Close()
	if listErr != nil {
		dropTemp()
		return failure(http.StatusUnprocessableEntity, "list archive: "+listErr.Error())
	}

	selected, err := archiveSelectEntry(entries, payload)
	if err != nil {
		dropTemp()
		return failure(http.StatusUnprocessableEntity, err.Error())
	}

	// Allocate session.
	if key == "" {
		key = archiveNewKey()
	}
	tmpDir, err := os.MkdirTemp("", archiveTmpDirPrefix)
	if err != nil {
		dropTemp()
		return failure(http.StatusInternalServerError, "mkdirtemp: "+err.Error())
	}

	now := time.Now()
	var archiveBytes int64
	if isTempArch && built.ok {
		archiveBytes = built.size
	}
	return &archiveSession{
		key:          key,
		sig:          sig,
		source:       source,
		built:        built,
		archivePath:  archivePath,
		isTempArch:   isTempArch,
		archiveBytes: archiveBytes,
		ext:          ext,
		tmpDir:       tmpDir,
		selectedFile: selected,
		created:      now,
		lastAccess:   now,
		extracted:    make(map[string]string),
		listing:      archiveIndexEntries(entries), // reuse the listing instead of re-listing per extraction
	}, archiveCreateResult{}
}

// archiveHandleStream processes /{ext}/stream[/{key}[/{file…}]].
func (s *server) archiveHandleStream(w http.ResponseWriter, r *http.Request, seg []string, ext string) {
	// /{ext}/stream?key=…&file=…
	if len(seg) == 2 {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key query param", http.StatusBadRequest)
			return
		}
		archiveSessionsMu.Lock()
		sess, ok := archiveSessions[key]
		archiveSessionsMu.Unlock()
		if !ok {
			http.Error(w, "session not found", http.StatusNotFound)
			return
		}
		file := r.URL.Query().Get("file")
		if file == "" {
			sess.mu.Lock()
			file = sess.selectedFile
			sess.mu.Unlock()
		}
		http.Redirect(w, r,
			"/"+ext+"/stream/"+url.PathEscape(key)+"/"+archiveEncodePath(file),
			http.StatusTemporaryRedirect)
		return
	}

	key := seg[2]
	sess := archiveAcquireSession(key)
	if sess == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	defer sess.release()

	// /{ext}/stream/{key} → redirect to selected file.
	if len(seg) == 3 {
		sess.mu.Lock()
		selected := sess.selectedFile
		sess.mu.Unlock()
		http.Redirect(w, r,
			"/"+ext+"/stream/"+url.PathEscape(key)+"/"+archiveEncodePath(selected),
			http.StatusTemporaryRedirect)
		return
	}

	// /{ext}/stream/{key}/{file…} → serve (in place, or while extracting).
	entryName := strings.Join(seg[3:], "/")

	body, err := archiveOpenEntry(r.Context(), sess, entryName)
	if err != nil {
		if errors.Is(err, errArchiveEntryNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if errors.Is(err, errArchiveOpenFile) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			http.Error(w, "extract: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer func() { _ = body.Close() }()

	sess.mu.Lock()
	sess.lastAccess = time.Now()
	sess.mu.Unlock()

	w.Header().Set("Content-Type", mimeByName(entryName))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("transferMode.dlna.org", "Streaming")
	w.Header().Set("contentFeatures.dlna.org",
		"DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000")

	http.ServeContent(w, r, entryName, time.Time{}, body)
}
