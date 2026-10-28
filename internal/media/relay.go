// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// relayTokenTTL is how long a registered relay token remains valid after
// being handed to ffmpeg/ffprobe. Generous enough to outlive a full HLS
// transcode session (see sessionTTL); expired entries are swept by the
// janitor, not evicted on read, since a long-paused player may still come
// back and re-request a segment.
const relayTokenTTL = 2 * time.Hour

// relayMaxTokens caps the number of live relay tokens. Playlist rewriting
// registers one token per URI line, so an attacker-controlled upstream could
// otherwise grow the map without bound within relayTokenTTL. When the cap is
// hit, expired tokens are dropped first, then the least recently registered
// (or re-registered) ones.
const relayMaxTokens = 16384

// relayJanitorInterval is how often expired relay tokens are swept.
const relayJanitorInterval = 5 * time.Minute

// relayMaxPlaylistBytes caps the size of an HLS playlist the relay will
// rewrite and serve, guarding against a malicious/huge upstream playlist
// exhausting memory. Real playlists are a few KB; 4 MiB is a generous cap.
const relayMaxPlaylistBytes = 4 << 20 // 4 MiB

// relayMaxRedirects is the hard cap on HTTP redirects the relay's own fetch
// will follow on behalf of ffmpeg/ffprobe.
const relayMaxRedirects = 5

// relayURIAttr matches a quoted URI="..." attribute inside an HLS tag line
// (e.g. #EXT-X-KEY, #EXT-X-MEDIA, #EXT-X-MAP).
var relayURIAttr = regexp.MustCompile(`URI="([^"]*)"`)

// hopHeaders are per-connection headers that must never be forwarded verbatim
// between the relay and either side of it.
var hopHeaders = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
	"Te":                  true,
	"Trailer":             true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
}

// relayEntry is one registered upstream URL a relay token maps to. It is the
// value of an element of mediaRelay.order.
type relayEntry struct {
	token     string
	url       string
	expiresAt time.Time
	// self is the server's own origin (selfBase) when url is a media route
	// on it (classifySelfPath), fetched through selfClient; "" for every
	// other URL, fetched through the guarded client.
	self string
	// fail is the latest upstream failure seen while serving this token
	// (zero if none); see recordFailure and failureSince.
	fail relayFailure
}

// relayFailure describes an upstream failure the relay saw while serving a
// token, so a transcode that then fails can say why (see failureSince).
// reason never contains the upstream URL, which may carry credentials.
type relayFailure struct {
	at      time.Time
	reason  string
	timeout bool
}

// mediaRelay is a loopback-only HTTP proxy that sits between ffmpeg/ffprobe
// and every remote (non-selfBase) media URL (SEC-4).
//
// Without it, this process's pre-flight IP validation (validateRemoteURL) is
// the only SSRF check: ffmpeg/ffprobe are handed the real hostname and do
// their own DNS resolution (open to DNS rebinding), follow HTTP redirects
// themselves, and — for HLS — fetch nested playlist/segment URLs referenced
// inside the playlist body, none of which this process ever sees or
// re-validates.
//
// With the relay, ffmpeg/ffprobe are instead given a
// "http://127.0.0.1:<port>/r/<token>" URL. This process resolves the token,
// fetches the real upstream URL itself through client (whose dialer Control
// hook re-validates the actually-resolved IP at connect time — the same
// netguard.DialControl pattern used by openSubClient), and streams the
// response back. Nested HLS playlist URIs are rewritten to fresh relay
// tokens before the playlist is handed to ffmpeg, so every hop of a
// multi-level HLS fetch is funneled back through the same guarded client.
type mediaRelay struct {
	client *http.Client
	// selfClient fetches self entries (relayEntry.self): URLs on the
	// server's own origin that classifySelfPath admits (issue #65). Its
	// dialer connects only to the host:port of that origin, carried in the
	// request context (relaySelfAddrKey), and it never follows a redirect:
	// proxy re-registers the Location instead, so every hop is classified
	// again.
	selfClient *http.Client

	// privateAllow holds the private destinations (RIVULET_HLS_PRIVATE_ALLOW,
	// HLSConfig.PrivateAllow) that stay reachable while blockPrivate is set;
	// nil (the default) allows none. The dialer reads it on every connect, so the IP actually dialed
	// for the initial URL, every redirect and every nested playlist or
	// segment is checked against it. Cloud-metadata is never allowed.
	privateAllow atomic.Pointer[netguard.Allow]

	startOnce sync.Once
	startErr  error
	ln        net.Listener
	srv       *http.Server

	mu     sync.Mutex
	tokens map[string]*list.Element // token → element of order (*relayEntry)
	byURL  map[string]string        // relayEntryKey → token (dedupe)
	// order holds every live entry, least recently (re-)registered at the
	// front. Every registration or refresh sets expiresAt = now+relayTokenTTL
	// and moves the entry to the back, so the list is also sorted by expiry:
	// both the expired-first sweep and the oldest-first eviction are pops from
	// the front, O(1) amortized, with no scan or allocation.
	order *list.List
}

// globalRelay is the process-wide relay instance used by every ffmpeg/
// ffprobe call site in this package. It blocks private/loopback/
// cloud-metadata addresses (blockPrivate=true) — the correct posture for
// externally-supplied media URLs.
var globalRelay = newMediaRelay(true)

// newMediaRelay constructs a relay whose outbound fetches are guarded by
// netguard.DialControlAllow(blockPrivate, privateAllow). Tests that need to
// point the relay at an httptest.Server (necessarily loopback) construct
// their own instance with blockPrivate=false instead of mutating globalRelay.
func newMediaRelay(blockPrivate bool) *mediaRelay {
	r := &mediaRelay{
		tokens: make(map[string]*list.Element),
		byURL:  make(map[string]string),
		order:  list.New(),
	}
	r.client = &http.Client{
		// No overall Timeout: http.Client.Timeout also bounds reading
		// the response body, which would cut every media stream relayed
		// through io.Copy after 30 s. Only the time to first response
		// byte is bounded; the body lives as long as the caller (ffmpeg)
		// keeps the request context open.
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
				Control: func(network, address string, c syscall.RawConn) error {
					return netguard.DialControlAllow(blockPrivate, r.privateAllow.Load())(network, address, c)
				},
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			// ffmpeg fetches many segments from the same few hosts;
			// keep a handful of idle connections per host (the
			// net/http default of 2 forces re-dials under the
			// parallel segment prefetch) but drop them when unused.
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= relayMaxRedirects {
				return fmt.Errorf("media relay: stopped after %d redirects", relayMaxRedirects)
			}
			// No separate host check here: the dialer's Control hook
			// re-validates the resolved IP of every redirect target at
			// connect time, exactly as it does for the initial request.
			return nil
		},
	}
	r.selfClient = &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if want, _ := ctx.Value(relaySelfAddrKey{}).(string); want == "" || addr != want {
					return nil, fmt.Errorf("media relay: self dial to %s refused", addr)
				}
				return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
			},
			ResponseHeaderTimeout: 60 * time.Second, // /yt runs yt-dlp before answering
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return r
}

// relaySelfAddrKey is the request-context key carrying the host:port that
// selfClient may dial for a self entry.
type relaySelfAddrKey struct{}

// selfHostPort returns the host:port of origin (scheme://host[:port]), with
// the scheme's default port filled in, as net/http passes it to DialContext.
func selfHostPort(origin string) string {
	u, err := url.Parse(origin)
	if err != nil {
		return ""
	}
	if u.Port() != "" {
		return u.Host
	}
	if u.Scheme == "https" {
		return net.JoinHostPort(u.Hostname(), "443")
	}
	return net.JoinHostPort(u.Hostname(), "80")
}

// relayEntryKey is the byURL dedupe key: the same URL registered as a self
// entry and as a guarded one gets two separate tokens.
func relayEntryKey(rawURL, self string) string {
	if self == "" {
		return rawURL
	}
	return "self\x00" + self + "\x00" + rawURL
}

// setPrivateAllow sets the private destinations the relay may reach
// (HLSConfig.PrivateAllow; media.New applies it to globalRelay). nil
// restores the default: every private, loopback and link-local address
// blocked. Idle connections are dropped so a host removed from the list is
// re-checked on its next fetch.
func (r *mediaRelay) setPrivateAllow(a *netguard.Allow) {
	r.privateAllow.Store(a)
	r.client.CloseIdleConnections()
}

// start lazily binds the loopback listener and HTTP server, the first time
// it's needed. Safe to call repeatedly and concurrently.
func (r *mediaRelay) start() error {
	r.startOnce.Do(func() {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			r.startErr = fmt.Errorf("media relay: listen: %w", err)
			return
		}
		r.ln = ln
		mux := http.NewServeMux()
		mux.HandleFunc("/r/", r.handle)
		r.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() { _ = r.srv.Serve(ln) }()
		go r.janitor()
	})
	return r.startErr
}

// port returns the loopback port the relay is listening on. Must be called
// after a successful start().
func (r *mediaRelay) port() int {
	return r.ln.Addr().(*net.TCPAddr).Port
}

// register starts the relay if needed and returns a fresh
// "http://127.0.0.1:<port>/r/<token>" URL that proxies to rawURL through the
// guarded client.
func (r *mediaRelay) register(rawURL string) (string, error) {
	return r.registerAs(rawURL, "")
}

// registerAs is register for a self entry when self (the server's own
// origin) is set: the caller has checked that rawURL is a media route on
// self (classifySelfPath), and the token's fetches go through selfClient.
func (r *mediaRelay) registerAs(rawURL, self string) (string, error) {
	if err := r.start(); err != nil {
		return "", err
	}
	now := time.Now()
	exp := now.Add(relayTokenTTL)
	r.mu.Lock()
	defer r.mu.Unlock()
	key := relayEntryKey(rawURL, self)
	token, ok := r.byURL[key]
	if ok {
		if el, live := r.tokens[token]; live {
			if e := el.Value.(*relayEntry); e.url == rawURL && e.self == self {
				// Same upstream URL already has a token: reuse it (refreshing
				// its expiry and recency) so re-fetched playlists don't mint
				// fresh tokens.
				e.expiresAt = exp
				r.order.MoveToBack(el)
				return fmt.Sprintf("http://127.0.0.1:%d/r/%s", r.port(), token), nil
			}
		}
	}
	r.evictLocked(now)
	token = randomRelayToken()
	r.tokens[token] = r.order.PushBack(&relayEntry{token: token, url: rawURL, expiresAt: exp, self: self})
	r.byURL[key] = token
	return fmt.Sprintf("http://127.0.0.1:%d/r/%s", r.port(), token), nil
}

// evictLocked makes room for one new token: if the map is at relayMaxTokens
// it drops expired tokens, then the oldest registrations. r.mu must be held.
func (r *mediaRelay) evictLocked(now time.Time) {
	if len(r.tokens) < relayMaxTokens {
		return
	}
	r.sweepLocked(now)
	for len(r.tokens) >= relayMaxTokens && r.order.Len() > 0 {
		r.deleteElemLocked(r.order.Front())
	}
}

// sweepLocked drops expired tokens. order is sorted by expiry (see
// mediaRelay.order), so this only pops the expired prefix. r.mu must be held.
func (r *mediaRelay) sweepLocked(now time.Time) {
	for el := r.order.Front(); el != nil && now.After(el.Value.(*relayEntry).expiresAt); el = r.order.Front() {
		r.deleteElemLocked(el)
	}
}

// deleteElemLocked removes one entry from every index. r.mu must be held.
func (r *mediaRelay) deleteElemLocked(el *list.Element) {
	e := r.order.Remove(el).(*relayEntry)
	delete(r.tokens, e.token)
	if key := relayEntryKey(e.url, e.self); r.byURL[key] == e.token {
		delete(r.byURL, key)
	}
}

func randomRelayToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// resolveEntry is resolve that also returns the entry's self origin ("" for
// a guarded entry).
func (r *mediaRelay) resolveEntry(token string) (upstream, self string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	el, ok := r.tokens[token]
	if !ok {
		return "", "", false
	}
	e := el.Value.(*relayEntry)
	if time.Now().After(e.expiresAt) {
		return "", "", false
	}
	return e.url, e.self, true
}

// janitor periodically sweeps expired tokens so long-running processes don't
// accumulate an unbounded token map across many playback sessions.
func (r *mediaRelay) janitor() {
	ticker := time.NewTicker(relayJanitorInterval)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		r.mu.Lock()
		r.sweepLocked(now)
		r.mu.Unlock()
	}
}

// handle serves GET/HEAD requests under /r/<token>, proxying to the
// registered upstream URL.
func (r *mediaRelay) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token := strings.TrimPrefix(req.URL.Path, "/r/")
	if idx := strings.IndexByte(token, '/'); idx >= 0 {
		token = token[:idx]
	}
	upstream, self, ok := r.resolveEntry(token)
	if !ok {
		http.Error(w, "unknown or expired relay token", http.StatusNotFound)
		return
	}
	r.proxy(w, req, token, upstream, self)
}

// recordFailure notes an upstream failure for token, unless the downstream
// request (ffmpeg) was itself cancelled, in which case the failure is ours,
// not the source's.
func (r *mediaRelay) recordFailure(req *http.Request, token, reason string, timeout bool) {
	if req.Context().Err() != nil {
		return
	}
	r.storeFailure(token, reason, timeout)
}

// storeFailure records an upstream failure for token unconditionally.
func (r *mediaRelay) storeFailure(token, reason string, timeout bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if el, ok := r.tokens[token]; ok {
		el.Value.(*relayEntry).fail = relayFailure{at: time.Now(), reason: reason, timeout: timeout}
	}
}

// failureSince returns the latest upstream failure recorded at or after
// since for the relay URL relayURL (as returned by register), if any. Only
// the token itself is consulted: a failure on a nested playlist's segment
// token is not attributed to the top-level URL. Tokens are shared per
// upstream URL, so a failure seen by a concurrent segment of the same
// source counts too: the source was failing either way.
func (r *mediaRelay) failureSince(relayURL string, since time.Time) (relayFailure, bool) {
	u, err := url.Parse(relayURL)
	if err != nil || !strings.HasPrefix(u.Path, "/r/") {
		return relayFailure{}, false
	}
	token := strings.TrimPrefix(u.Path, "/r/")
	if i := strings.IndexByte(token, '/'); i >= 0 {
		token = token[:i] // a rewritten entry's /s.<ext> suffix
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	el, ok := r.tokens[token]
	if !ok {
		return relayFailure{}, false
	}
	f := el.Value.(*relayEntry).fail
	if f.reason == "" || f.at.Before(since) {
		return relayFailure{}, false
	}
	return f, true
}

// isTimeout reports whether err is a network or context timeout.
func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// proxy fetches upstream through r.client, or for a self entry (self != "")
// through r.selfClient (forwarding Range and the method), rewriting nested
// URIs if the response looks like an HLS playlist, and otherwise streaming
// status/headers/body straight through. A self entry's redirect is not
// followed here: its Location is registered like a nested playlist entry
// and handed back to ffmpeg as a redirect to that token.
func (r *mediaRelay) proxy(w http.ResponseWriter, req *http.Request, token, upstream, self string) {
	ctx, client := req.Context(), r.client
	if self != "" {
		ctx, client = context.WithValue(ctx, relaySelfAddrKey{}, selfHostPort(self)), r.selfClient
	}
	outReq, err := http.NewRequestWithContext(ctx, req.Method, upstream, nil)
	if err != nil {
		http.Error(w, "bad upstream url", http.StatusBadGateway)
		return
	}
	if rng := req.Header.Get("Range"); rng != "" {
		outReq.Header.Set("Range", rng)
	}
	// The body must reach the sniffing below exactly as ffmpeg will read
	// it, and ffmpeg inflates a gzip/deflate body before it probes. Ask for
	// it uncompressed (this also stops net/http's own transparent gzip).
	outReq.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(outReq)
	if err != nil {
		if isTimeout(err) {
			r.recordFailure(req, token, "upstream timed out", true)
		} else {
			r.recordFailure(req, token, "upstream fetch failed", false)
		}
		http.Error(w, "upstream fetch failed", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if self != "" && isRedirect(resp.StatusCode) {
		loc, lerr := outReq.URL.Parse(resp.Header.Get("Location"))
		if lerr != nil || resp.Header.Get("Location") == "" {
			http.Error(w, "bad redirect from upstream", http.StatusBadGateway)
			return
		}
		next, ok := r.registerNested(loc.String(), self)
		if !ok || next == "" {
			http.Error(w, "redirect target refused", http.StatusBadGateway)
			return
		}
		http.Redirect(w, req, next, http.StatusTemporaryRedirect)
		return
	}
	// 416 is a healthy answer to a read past the end (ffmpeg probing the
	// tail), not a source failure.
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		r.recordFailure(req, token, "upstream returned "+resp.Status,
			resp.StatusCode == http.StatusGatewayTimeout || resp.StatusCode == http.StatusRequestTimeout)
	}

	if req.Method == http.MethodGet {
		if resp.StatusCode/100 == 2 && ffmpegInflates(resp.Header.Values("Content-Encoding")) {
			// Sent compressed anyway. ffmpeg would inflate it before
			// probing, so decode it here, sniff what ffmpeg would see and
			// pass that on uncompressed. Only a 2xx is decoded: ffmpeg
			// reads no body from an error status, so an error passes
			// through untouched, status, coding and all.
			if resp.StatusCode == http.StatusPartialContent && !rangeStartsAtZero(resp.Header.Get("Content-Range")) {
				// A slice from the middle of a compressed stream: ffmpeg's
				// inflate of it is garbage or nothing, and the decoded bytes
				// have no offset in the resource ffmpeg asked about.
				r.recordFailure(req, token, "upstream sent a compressed partial response", false)
				http.Error(w, "compressed partial upstream response", http.StatusBadGateway)
				return
			}
			cbr := bufio.NewReader(resp.Body)
			cbody := io.Reader(cbr)
			// An empty body (a 204, say) has nothing to decode or sniff.
			if first, _ := cbr.Peek(1); len(first) > 0 {
				src := &errTrackingReader{r: cbr}
				dec, err := newInflater(src)
				if err != nil {
					// ffmpeg would read nothing from it. Refused rather
					// than relayed uninspected.
					r.recordFailure(req, token, "upstream sent an undecodable compressed body", false)
					http.Error(w, "undecodable compressed upstream response", http.StatusBadGateway)
					return
				}
				cbody = &ffmpegInflateReader{dec: dec, src: src}
				// The decoded body has no known length, and Content-Range
				// and Accept-Ranges would count compressed bytes: deliver
				// it as a plain, non-seekable response (a 206 from byte 0
				// becomes the 200 it now is).
				resp.Header.Del("Content-Encoding")
				resp.Header.Del("Content-Length")
				resp.Header.Del("Content-Range")
				resp.Header.Del("Accept-Ranges")
				resp.ContentLength = -1
				if resp.StatusCode == http.StatusPartialContent {
					resp.StatusCode = http.StatusOK
				}
			}
			resp.Body = struct {
				io.Reader
				io.Closer
			}{cbody, resp.Body}
		}
		// Sniff the body prefix: ffmpeg detects HLS/DASH by content, not by
		// Content-Type or extension, so a mislabelled playlist must still
		// have its nested URIs rewritten through the relay.
		br := bufio.NewReaderSize(resp.Body, relaySniffBytes)
		head, _ := br.Peek(relaySniffBytes)
		var body io.Reader = br
		dash := false
		if looksLikeXML(head) {
			// An MPD root can sit behind an XML declaration, comments or a
			// DOCTYPE longer than relaySniffBytes, and ffmpeg reads far
			// more than that before deciding. Only XML-looking bodies get
			// the larger reader and the deeper look. Media segments,
			// playlists and WebVTT don't start with '<' or whitespace; a
			// byte range from mid-file may, and xmlBodyMayBeDASH lets
			// binary (with a NUL and no "<MPD") through.
			xr := bufio.NewReaderSize(br, relayXMLSniffBytes)
			xhead, _ := xr.Peek(relayXMLSniffBytes)
			dash = xmlBodyMayBeDASH(xhead)
			body = xr
		}
		resp.Body = struct {
			io.Reader
			io.Closer
		}{body, resp.Body}
		if dash || looksLikeDASH(resp, upstream, head) {
			// DASH manifests reference media via BaseURL/SegmentTemplate,
			// which we do not rewrite; relaying one would let ffmpeg fetch
			// nested absolute URLs unguarded. Refuse rather than leak.
			http.Error(w, "dash manifests are not supported by the relay", http.StatusUnsupportedMediaType)
			return
		}
		if looksLikePlaylist(resp, upstream) || hasPlaylistMagic(head) {
			r.servePlaylist(w, resp, upstream, self)
			return
		}
	}

	copyProxyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if req.Method != http.MethodHead {
		rd := &errTrackingReader{r: resp.Body}
		_, _ = io.Copy(w, rd)
		if rd.err != nil && !errors.Is(rd.err, io.EOF) {
			// Not recordFailure: ffmpeg often drops its request as soon
			// as the body breaks, so its context is no reliable sign that
			// the break was ours; only a cancelled upstream read is.
			if !errors.Is(rd.err, context.Canceled) {
				r.storeFailure(token, "upstream body ended early", isTimeout(rd.err))
			}
			// The upstream body broke mid-stream (reset, timeout, short
			// read vs Content-Length). Abort the downstream connection so
			// ffmpeg sees a failed transfer rather than a cleanly
			// terminated (chunked) but truncated one.
			panic(http.ErrAbortHandler)
		}
	}
}

// ffmpegInflates reports whether ffmpeg's http protocol would inflate a
// response with these Content-Encoding header lines: it checks every line
// for a "gzip" or "deflate" prefix, case-insensitively, and once one
// matches it decodes with zlib's gzip/zlib auto-detection. Any other coding
// reaches ffmpeg's demuxers raw, exactly as the relay sniffs it.
func ffmpegInflates(lines []string) bool {
	for _, ce := range lines {
		ce = strings.ToLower(strings.TrimSpace(ce))
		if strings.HasPrefix(ce, "gzip") || strings.HasPrefix(ce, "deflate") {
			return true
		}
	}
	return false
}

// rangeStartsAtZero reports whether a 206's Content-Range ("bytes 0-N/L")
// starts at the first byte, where a compressed stream can be decoded from
// its beginning.
func rangeStartsAtZero(cr string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(cr)), "bytes 0-")
}

// newInflater decodes body the way ffmpeg does (zlib inflateInit2 with
// gzip/zlib auto-detection): a gzip stream if it starts with the gzip
// magic, a zlib stream otherwise. Like zlib there, it stops after the first
// gzip member.
func newInflater(body io.Reader) (io.Reader, error) {
	br := bufio.NewReader(body)
	if magic, _ := br.Peek(2); len(magic) == 2 && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		gz.Multistream(false)
		return gz, nil
	}
	return zlib.NewReader(br)
}

// ffmpegInflateReader ends a decoded body the way ffmpeg's http protocol
// does: a decoding error (bad checksum, truncated stream) ends it cleanly
// after the bytes decoded so far, while an error reading the upstream body
// itself still surfaces, so the relay aborts as for any broken upstream.
type ffmpegInflateReader struct {
	dec io.Reader
	src *errTrackingReader
}

func (f *ffmpegInflateReader) Read(p []byte) (int, error) {
	n, err := f.dec.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && (f.src.err == nil || errors.Is(f.src.err, io.EOF)) {
		return n, io.EOF
	}
	return n, err
}

// errTrackingReader records the first read error from the wrapped reader so
// the caller can tell an upstream failure from a downstream write failure.
type errTrackingReader struct {
	r   io.Reader
	err error
}

func (e *errTrackingReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && e.err == nil {
		e.err = err
	}
	return n, err
}

// relaySniffBytes bounds how much of a response body is inspected to detect
// a playlist/manifest regardless of its declared type.
const relaySniffBytes = 512

// relayXMLSniffBytes bounds the deeper look at a body that starts like XML
// (see looksLikeXML and xmlRootIsMPDOrUnknown).
const relayXMLSniffBytes = 16 << 10

// xmlRootIsMPDOrUnknown walks the XML prolog in head (declaration, comments,
// processing instructions, DOCTYPE) to the root element. It reports true,
// meaning "treat as DASH", when the root is MPD in any case (ffmpeg's dashdec
// matches it case-insensitively) or when no root element is found within
// head, a DOCTYPE carries an internal subset or the declared encoding is
// not one whose markup bytes are plain ASCII, because ffmpeg probes far
// beyond relayXMLSniffBytes and a manifest the relay can't rule out must be
// refused rather than streamed. Any other root (TTML, HTML, an error page)
// reports false.
func xmlRootIsMPDOrUnknown(head []byte) bool {
	head = bytes.TrimPrefix(head, []byte("\xef\xbb\xbf"))
	if !xmlDeclEncodingInspectable(head) {
		return true
	}
	d := xml.NewDecoder(bytes.NewReader(head))
	d.Strict = false
	// Only markup matters here. In an ASCII-compatible single-byte
	// encoding it is the same bytes as in ASCII, so read those as-is.
	// Anything else (UTF-7, say, where libxml2 decodes "+ADw-" to '<')
	// would make the walk see a different document than libxml2 does:
	// fail, and so fail closed. xmlDeclEncodingInspectable has already
	// vetted the declaration; this catches what it and encoding/xml
	// might read differently.
	d.CharsetReader = func(charset string, r io.Reader) (io.Reader, error) {
		if asciiCompatibleCharset(charset) {
			return r, nil
		}
		return nil, fmt.Errorf("charset %q not inspected", charset)
	}
	for {
		tok, err := d.RawToken()
		if err != nil {
			return true
		}
		switch t := tok.(type) {
		case xml.StartElement:
			return strings.EqualFold(t.Name.Local, "MPD")
		case xml.Directive:
			// A DOCTYPE internal subset can hold markup that encoding/xml
			// and libxml2 split differently; don't trust the walk past it.
			if bytes.ContainsRune(t, '[') {
				return true
			}
		}
	}
}

// looksLikeXML reports whether head starts with '<' after an optional UTF-8
// BOM and leading whitespace, or holds nothing but those: libxml2 accepts
// any amount of whitespace before the root when there is no XML
// declaration, and dash_probe finds "<MPD" wherever it sits, so a
// whitespace-only head is undecided and gets the deeper look. An empty head
// (an empty body) is not XML.
//
// UTF-16/32 bodies need no look: dash_probe matches with av_stristr, which
// stops at the first NUL byte, and every UTF-16/32 '<' has one beside it, so
// ffmpeg can't detect such a manifest by content (its dash demuxer declares
// no extension or MIME type either).
func looksLikeXML(head []byte) bool {
	if len(head) == 0 {
		return false
	}
	head = bytes.TrimPrefix(head, []byte("\xef\xbb\xbf"))
	head = bytes.TrimLeft(head, " \t\r\n")
	return len(head) == 0 || head[0] == '<'
}

// xmlBodyMayBeDASH decides, for a body that looks like XML (see
// looksLikeXML), whether ffmpeg could open it as a DASH manifest. It fails
// closed: true when "<MPD" appears anywhere in head in any case (ffmpeg's
// dash_probe matches the raw bytes with av_stristr, so this also catches
// markup that encoding/xml and libxml2 would split differently), or when
// xmlRootIsMPDOrUnknown can't rule it out. The one exemption is a head
// with a NUL byte and no "<MPD": av_stristr stops at the first NUL, so
// dash_probe can't see past it, whatever the encoding. That keeps binary
// media, such as a 206 slice that happens to start with '<', out of the
// prolog walk.
func xmlBodyMayBeDASH(head []byte) bool {
	if containsMPDTag(head) {
		return true
	}
	if bytes.IndexByte(head, 0) >= 0 {
		return false
	}
	return xmlRootIsMPDOrUnknown(head)
}

// xmlDeclEncodingInspectable reports whether the prolog walk can read head
// as the bytes libxml2 will decode: head has no XML declaration, or one
// that parses and declares no encoding, UTF-8 or an ASCII-compatible
// single-byte encoding (asciiCompatibleCharset). It parses the declaration
// itself because encoding/xml only finds an encoding written with nothing
// around the '=', while libxml2 allows XML whitespace there; with
// encoding = "UTF-7" the walk would read "+ADw-" as text where libxml2
// decodes '<'. A declaration it can't parse, or any pseudo-attribute other
// than version, encoding and standalone, reports false.
func xmlDeclEncodingInspectable(head []byte) bool {
	const ws = " \t\r\n"
	head = bytes.TrimLeft(head, ws)
	if len(head) < 6 || !bytes.EqualFold(head[:5], []byte("<?xml")) {
		return true
	}
	if !isXMLSpace(head[5]) && head[5] != '?' {
		return true // another PI, such as <?xml-stylesheet ...?>
	}
	if !bytes.Equal(head[:5], []byte("<?xml")) {
		return false
	}
	end := bytes.Index(head, []byte("?>"))
	if end < 0 {
		return false
	}
	decl := head[5:end]
	for {
		decl = bytes.TrimLeft(decl, ws)
		if len(decl) == 0 {
			return true
		}
		n := 0
		for n < len(decl) && isXMLDeclNameByte(decl[n]) {
			n++
		}
		name := string(decl[:n])
		rest := bytes.TrimLeft(decl[n:], ws)
		if n == 0 || len(rest) == 0 || rest[0] != '=' {
			return false
		}
		rest = bytes.TrimLeft(rest[1:], ws)
		if len(rest) == 0 || (rest[0] != '"' && rest[0] != '\'') {
			return false
		}
		closing := bytes.IndexByte(rest[1:], rest[0])
		if closing < 0 {
			return false
		}
		value := string(rest[1 : 1+closing])
		switch name {
		case "version", "standalone":
		case "encoding":
			if !asciiCompatibleCharset(value) {
				return false
			}
		default:
			return false
		}
		decl = rest[2+closing:]
	}
}

func isXMLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

func isXMLDeclNameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
		c == '_' || c == '-' || c == '.' || c == ':'
}

// asciiCompatibleCharset reports whether an XML declaration's encoding is
// UTF-8 or one whose bytes 0x00-0x7F are ASCII and that has no multi-byte
// forms: US-ASCII, ISO-8859-1 to -16 and windows-1250 to -1258, in any
// case.
func asciiCompatibleCharset(charset string) bool {
	switch cs := strings.ToLower(charset); {
	case cs == "utf-8", cs == "utf8", cs == "us-ascii", cs == "ascii", cs == "latin1":
		return true
	case strings.HasPrefix(cs, "iso-8859-"):
		n, err := strconv.Atoi(cs[len("iso-8859-"):])
		return err == nil && n >= 1 && n <= 16 && n != 12
	case strings.HasPrefix(cs, "windows-125"):
		n, err := strconv.Atoi(cs[len("windows-125"):])
		return err == nil && n >= 0 && n <= 8 && len(cs) == len("windows-125")+1
	}
	return false
}

// containsMPDTag reports whether b contains "<MPD" in any case.
func containsMPDTag(b []byte) bool {
	return bytes.Contains(bytes.ToUpper(b), []byte("<MPD"))
}

// hasPlaylistMagic reports whether head starts with "#EXTM3U", allowing an
// optional UTF-8 BOM and leading whitespace.
func hasPlaylistMagic(head []byte) bool {
	head = bytes.TrimPrefix(head, []byte("\xef\xbb\xbf"))
	head = bytes.TrimLeft(head, " \t\r\n")
	return bytes.HasPrefix(head, []byte("#EXTM3U"))
}

// looksLikeDASH reports whether the response is a DASH manifest: the
// requested or the final (post-redirect) path ends in .mpd, the Content-Type
// is application/dash+xml, or the body prefix contains an MPD element in any
// case (ffmpeg's dash_probe uses a case-insensitive match).
func looksLikeDASH(resp *http.Response, upstream string, head []byte) bool {
	if u, err := url.Parse(upstream); err == nil && strings.HasSuffix(strings.ToLower(u.Path), ".mpd") {
		return true
	}
	if resp.Request != nil && resp.Request.URL != nil && strings.HasSuffix(strings.ToLower(resp.Request.URL.Path), ".mpd") {
		return true
	}
	if mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err == nil && mt == "application/dash+xml" {
		return true
	}
	return bytes.Contains(bytes.ToUpper(head), []byte("<MPD"))
}

// looksLikePlaylist reports whether resp is (very likely) an HLS playlist,
// based on Content-Type or the upstream URL's extension — checked without
// reading the body, so ordinary media segments are streamed straight through
// instead of being buffered.
func looksLikePlaylist(resp *http.Response, upstream string) bool {
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "mpegurl") {
		return true
	}
	if u, err := url.Parse(upstream); err == nil && strings.HasSuffix(strings.ToLower(u.Path), ".m3u8") {
		return true
	}
	return false
}

// servePlaylist reads resp's body (capped at relayMaxPlaylistBytes), rewrites
// every referenced URI (segment or nested playlist) to a fresh relay token
// resolved against the playlist's own URL, and serves the rewritten playlist.
// That is the final URL after any redirects (resp.Request.URL), as RFC 8216
// section 4.1 requires and as ffmpeg itself resolves; upstream, the URL the
// relay was asked for, is only the fallback.
func (r *mediaRelay) servePlaylist(w http.ResponseWriter, resp *http.Response, upstream, self string) {
	var base *url.URL
	if resp.Request != nil && resp.Request.URL != nil {
		base = resp.Request.URL
	} else {
		u, err := url.Parse(upstream)
		if err != nil {
			http.Error(w, "bad upstream url", http.StatusBadGateway)
			return
		}
		base = u
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, relayMaxPlaylistBytes+1))
	if err != nil {
		http.Error(w, "reading upstream playlist", http.StatusBadGateway)
		return
	}
	if len(body) > relayMaxPlaylistBytes {
		http.Error(w, "upstream playlist too large", http.StatusBadGateway)
		return
	}
	rewritten := r.rewritePlaylist(string(body), base, self)

	copyProxyHeaders(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Length", strconv.Itoa(len(rewritten)))
	w.WriteHeader(resp.StatusCode)
	_, _ = io.WriteString(w, rewritten)
}

// rewritePlaylist rewrites every URI reference in an HLS playlist body
// (plain segment/playlist lines, and quoted URI="..." tag attributes) to a
// fresh relay token, resolving relative references against base. self is
// the playlist's own entry's self origin (see registerNested).
func (r *mediaRelay) rewritePlaylist(body string, base *url.URL, self string) string {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "#"):
			if relayURIAttr.MatchString(line) {
				lines[i] = relayURIAttr.ReplaceAllStringFunc(line, func(m string) string {
					sub := relayURIAttr.FindStringSubmatch(m)
					return `URI="` + r.resolveAndRegister(sub[1], base, self) + `"`
				})
			}
		case strings.TrimSpace(line) == "":
			// blank line, pass through unchanged
		default:
			lines[i] = r.resolveAndRegister(line, base, self)
		}
	}
	return strings.Join(lines, "\n")
}

// resolveAndRegister resolves ref against base and registers it for a fresh
// relay token, returning that token's URL. On any error the original ref is
// returned unchanged (best-effort; the eventual fetch through the relay
// simply 404s or fails rather than corrupting the playlist). A reference
// refused by registerNested becomes a dead relay URL instead, so ffmpeg
// never fetches it at all.
func (r *mediaRelay) resolveAndRegister(ref string, base *url.URL, self string) string {
	refURL, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	resolved := base.ResolveReference(refURL).String()
	token, ok := r.registerNested(resolved, self)
	if !ok {
		return fmt.Sprintf("http://127.0.0.1:%d/r/refused", r.port())
	}
	if token == "" {
		return ref
	}
	return token + relayExtSuffix(resolved)
}

// relayExtSuffix returns "/s.<ext>" for rawURL whose last path element has a
// short alphanumeric extension, else "". A rewritten playlist entry keeps
// its upstream extension this way: ffmpeg's HLS demuxer refuses a segment
// URL without an allowed extension (extension_picky, the default since
// ffmpeg 7.1), and handle ignores everything after the token.
func relayExtSuffix(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	p := u.Path
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		p = p[i+1:]
	}
	i := strings.LastIndexByte(p, '.')
	if i < 0 || i == len(p)-1 || len(p)-i-1 > 8 {
		return ""
	}
	ext := strings.ToLower(p[i+1:])
	for _, c := range ext {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return ""
		}
	}
	return "/s." + ext
}

// registerNested registers rawURL, a nested playlist entry or redirect
// target of an entry whose self origin is self. Under a self entry, a URL
// on that same origin stays a self entry only if classifySelfPath admits it
// (ok=false otherwise: refused, never fetched); every other URL, and every
// URL under a guarded entry, becomes a guarded entry. So the self exception
// never reaches a path the top-level check would refuse, and a nested
// remote entry is checked by the guarded client's dialer like any other.
// token is "" (ok=true) when the relay could not register it.
func (r *mediaRelay) registerNested(rawURL, self string) (token string, ok bool) {
	entrySelf := ""
	if self != "" && isSelfOrigin(rawURL, self) {
		if selfURLKind(rawURL) == selfPathRefused {
			return "", false
		}
		rawURL, entrySelf = localize(rawURL), self
	}
	token, err := r.registerAs(rawURL, entrySelf)
	if err != nil {
		return "", true
	}
	return token, true
}

// isRedirect reports whether code is an HTTP redirect status.
func isRedirect(code int) bool {
	switch code {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}

// copyProxyHeaders copies every header from src to dst except hop-by-hop
// headers, which must never be forwarded verbatim across a proxy boundary.
func copyProxyHeaders(dst, src http.Header) {
	for k, vv := range src {
		if hopHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// relayInput returns the URL ffmpeg/ffprobe should be given for mediaURL,
// plus the -protocol_whitelist value to pair with it.
//
// mediaURL on selfBase (this server's own origin — see validateRemoteURL's
// selfBase carve-out) depends on its route (classifySelfPath, issue #65). A
// route that serves local content is passed through directly: it's already
// loopback-only and this process is both ends of that request, so relaying
// it would just add a hop. A route that passes a remote body through
// (/proxy, /ftp, /yt) is registered as a self entry on globalRelay, so its
// nested playlist entries and redirects are re-checked like any remote URL.
// Every other self route is refused. localize has already mapped the https
// UI origin onto plain http, so the whitelist is "http,tcp" either way.
//
// Every other (remote, untrusted) mediaURL is routed through globalRelay
// instead of being handed to ffmpeg/ffprobe directly (SEC-4): the relay is
// always plain loopback HTTP, so its
// protocol_whitelist is deliberately narrow ("http,tcp" — no https/tls/
// crypto, and never "file", which would reopen the local-file-read path
// validateRemoteURL exists to close).
func relayInput(mediaURL, selfBase string) (inputURL, protocolWhitelist string, err error) {
	localized := localize(mediaURL)
	if isSelfOrigin(localized, selfBase) {
		switch selfURLKind(localized) {
		case selfPathLocal:
			return localized, "http,tcp", nil
		case selfPathRelayed:
			tok, err := globalRelay.registerAs(localized, selfBase)
			if err != nil {
				return "", "", fmt.Errorf("media relay: %w", err)
			}
			return tok, "http,tcp", nil
		}
		return "", "", fmt.Errorf("media relay: %w", validateSelfURL(localized))
	}
	tok, err := globalRelay.register(localized)
	if err != nil {
		return "", "", fmt.Errorf("media relay: %w", err)
	}
	return tok, "http,tcp", nil
}
