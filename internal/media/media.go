// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package media implements types.MediaProber, backing the ffprobe/ffmpeg helper
// routes. All external I/O is done via os/exec (ffprobe) or the standard
// net/http client; no third-party dependencies are required.
package media

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

const chunkSize = 65536 // 64 KiB — OpenSubtitles hash window

// probeTracksCacheMaxSize is the maximum number of entries kept in each of
// prober's probeCache and tracksCache.  Expired entries are swept on every
// insert; when the map still exceeds this limit after the TTL sweep, the
// soonest-expiring entry is evicted until it's back under the cap.
const probeTracksCacheMaxSize = 512

// openSubClient is a shared HTTP client for OpenSubHash (HEAD + Range GETs) and
// subtitle fetches. One transport means all requests to the same host reuse a
// single TCP connection pool instead of allocating a fresh one per call. The
// dialer's Control hook re-validates the resolved IP at connect time, closing
// the DNS-rebinding TOCTOU gap left by the validateRemoteURL pre-flight check.
var openSubClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
			Control: netguard.DialControl(true),
		}).DialContext,
	},
}

// openSubSelfClient is the openSubClient counterpart for this server's own
// origin (selfBase). validateRemoteURL deliberately exempts that origin from
// the private-address check, but openSubClient's dialer would then refuse the
// loopback connection, so self-origin URLs go through this client instead. It
// is only ever selected by subClientFor for URLs that match selfBase, and it
// refuses to follow a redirect off that origin, so it can never be steered at
// another loopback/private target.
var openSubSelfClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return fmt.Errorf("stopped after %d redirects", len(via))
		}
		if !isSelfOrigin(req.URL.String(), selfBaseOf(via[0].URL.String())) {
			return fmt.Errorf("redirect to %s leaves the server's own origin", req.URL.Host)
		}
		if err := validateSelfURL(req.URL.String()); err != nil {
			return fmt.Errorf("redirect: %w", err)
		}
		return nil
	},
}

// selfBaseOf returns the scheme://host part of raw, used by
// openSubSelfClient's redirect check to recover the origin the request was
// pinned to.
func selfBaseOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// isSelfOrigin reports whether raw (after localize) is selfBase or a path
// under it. selfBase == "" never matches.
func isSelfOrigin(raw, selfBase string) bool {
	if selfBase == "" {
		return false
	}
	l := localize(raw)
	return l == selfBase || strings.HasPrefix(l, selfBase+"/")
}

// selfPathKind is how a URL on this server's own origin may be used as a
// media or subtitle URL; see classifySelfPath.
type selfPathKind int

const (
	// selfPathRefused: not a media URL. Control routes (/removeAll,
	// /{infoHash}/remove, /casting, …) and anything not listed below.
	selfPathRefused selfPathKind = iota
	// selfPathLocal: serves local content (or redirects to a route that
	// does); ffmpeg reads it directly.
	selfPathLocal
	// selfPathRelayed: passes a remote HTTP body through (/proxy, /ftp,
	// /yt); ffmpeg reads it through the media relay, so nested playlist
	// entries and redirects are re-checked like any remote URL.
	selfPathRelayed
)

// classifySelfPath classifies escaped, the escaped path of a URL on this
// server's own origin, mirroring internal/api's router (issue #65):
//
//   - local: a torrent file (/{infoHash}/{fileIdx}[/…] and its
//     /stream/{infoHash}/{fileIdx} alias); an archive or NZB session
//     (/{zip,rar,7zip,tar,tgz,nzb}/create[/{key}], which stremio-core sends
//     as the playable URL and which redirects to /{ext}/stream/…, and
//     /{ext}/stream[/…]); a local-addon file (/local-addon/file/{id}).
//   - relayed: /proxy/… (stremio-core's proxyHeaders form
//     /proxy/d=…&h=…/{path}, and /proxy/{stream,hls,…}), /ftp/{file} and
//     /yt/{id}.
//   - refused: everything else, including /extractor, /base64, /hlsv2 and
//     every control route.
//
// Segments are unescaped one at a time. A "." or ".." segment is refused so
// a path cannot be normalized onto another route on the way. Outside /proxy
// (whose handler splits the escaped path itself) an empty segment or an
// encoded "/" is refused too, so the router, which splits the decoded path,
// sees the same segments.
func classifySelfPath(escaped string) selfPathKind {
	raw := strings.Split(strings.TrimPrefix(escaped, "/"), "/")
	seg := make([]string, len(raw))
	for i, r := range raw {
		d, err := url.PathUnescape(r)
		if err != nil || d == "." || d == ".." {
			return selfPathRefused
		}
		seg[i] = d
	}
	if seg[0] == "proxy" {
		if len(seg) < 2 || seg[1] == "" {
			return selfPathRefused
		}
		return selfPathRelayed
	}
	for _, s := range seg {
		if s == "" || strings.Contains(s, "/") {
			return selfPathRefused
		}
	}
	switch {
	case isInfoHashSeg(seg[0]):
		if len(seg) < 2 {
			return selfPathRefused
		}
		switch seg[1] {
		case "create", "remove", "stats.json", "peers":
			return selfPathRefused
		}
		if len(seg) == 2 || seg[2] != "stats.json" {
			return selfPathLocal
		}
	case seg[0] == "stream":
		if len(seg) >= 3 && isInfoHashSeg(seg[1]) {
			return selfPathLocal
		}
	case seg[0] == "zip", seg[0] == "rar", seg[0] == "7zip", seg[0] == "tar", seg[0] == "tgz", seg[0] == "nzb":
		if len(seg) >= 2 && (seg[1] == "stream" || (seg[1] == "create" && len(seg) <= 3)) {
			return selfPathLocal
		}
	case seg[0] == "local-addon":
		if len(seg) == 3 && seg[1] == "file" {
			return selfPathLocal
		}
	case seg[0] == "ftp":
		if len(seg) == 2 && seg[1] != "create" && seg[1] != "stream" {
			return selfPathRelayed
		}
	case seg[0] == "yt":
		if len(seg) == 2 {
			return selfPathRelayed
		}
	}
	return selfPathRefused
}

// selfURLKind is classifySelfPath for raw, a URL already known to be on this
// server's own origin.
func selfURLKind(raw string) selfPathKind {
	u, err := url.Parse(raw)
	if err != nil {
		return selfPathRefused
	}
	return classifySelfPath(u.EscapedPath())
}

// validateSelfURL refuses raw, a URL on this server's own origin, unless it
// may be used as a media URL (local or relayed; see classifySelfPath).
func validateSelfURL(raw string) error {
	if selfURLKind(raw) == selfPathRefused {
		path := raw
		if u, err := url.Parse(raw); err == nil {
			path = u.EscapedPath()
		}
		return fmt.Errorf("self-origin path %q is not a media route", path)
	}
	return nil
}

// isInfoHashSeg reports whether s is a 40-hex-digit infohash, matching
// internal/api's isInfoHash.
func isInfoHashSeg(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// subClientFor picks the HTTP client for rawURL: the loopback-permitting
// openSubSelfClient for this server's own origin, the SSRF-guarded
// openSubClient for everything else.
func subClientFor(rawURL, selfBase string) *http.Client {
	if isSelfOrigin(rawURL, selfBase) {
		return openSubSelfClient
	}
	return openSubClient
}

// probeResultEntry holds a cached Probe() result (or error) with a wall-clock expiry.
type probeResultEntry struct {
	result    interface{}
	err       error
	expiresAt time.Time
}

// tracksCacheEntry holds a cached Tracks() result (or error) with a wall-clock expiry.
type tracksCacheEntry struct {
	result    interface{}
	err       error
	expiresAt time.Time
}

// prober is the concrete implementation of types.MediaProber.
type prober struct {
	// baseURLLocal is the local server URL (e.g. "http://127.0.0.1:11470"),
	// used to prefix scheme-less stream URLs passed to Probe.
	baseURLLocal string
	hls          *hlsManager
	// privateAllow is HLSConfig.PrivateAllow: the private hosts Probe and
	// Tracks accept (see validateMediaURL).
	privateAllow *netguard.Allow
	// Probe and Tracks result caches keyed by resolved URL; short-TTL prevents
	// repeated ffprobe spawns for the same URL across consecutive player requests.
	probeMu     sync.Mutex
	probeCache  map[string]probeResultEntry
	tracksMu    sync.Mutex
	tracksCache map[string]tracksCacheEntry
	// guard bounds concurrent probes and dedupes identical in-flight ones.
	guard probeGuard
}

// New returns a MediaProber backed by system ffprobe/ffmpeg. baseURLLocal
// should include scheme and host with no trailing slash (e.g.
// "http://127.0.0.1:11470"). cfg configures the HLS transcode manager
// (issue #20; DefaultHLSConfig() reproduces the historical hardcoded
// defaults). settings is consulted for live /settings overrides at HLS
// session-creation time — see HLSConfig and effectiveSessionConfig; pass nil
// to skip /settings integration entirely.
func New(baseURLLocal string, cfg HLSConfig, settings SettingsSource) types.MediaProber {
	base := strings.TrimRight(baseURLLocal, "/")
	p := &prober{
		baseURLLocal: base,
		hls:          newHLS(base, cfg, settings),
		privateAllow: cfg.PrivateAllow,
		probeCache:   make(map[string]probeResultEntry), // must be non-nil before first write
		tracksCache:  make(map[string]tracksCacheEntry), // must be non-nil before first write
	}
	// Probe/Tracks/OpenSubHash/subtitles and the HLS initial probe share one
	// concurrency budget.
	p.hls.shared = &p.guard
	// The relay is process-wide; the most recent New's list applies, which
	// is the running server's (internal/app builds one prober per Run).
	globalRelay.setPrivateAllow(cfg.PrivateAllow)
	return p
}

// Probe runs ffprobe on streamURL and returns the parsed JSON map.
// If streamURL has no scheme it is prefixed with p.baseURLLocal.
// A 30-second context timeout is applied to the child process.
func (p *prober) Probe(streamURL string) (interface{}, error) {
	return p.ProbeContext(context.Background(), streamURL)
}

// ProbeContext is Probe with caller cancellation. Concurrent calls for the
// same URL share one ffprobe (singleflight) and at most maxConcurrentProbes
// ffprobe/ffmpeg/HTTP probes run at once; a cancelled ctx stops this caller
// waiting without killing a probe other callers share.
func (p *prober) ProbeContext(ctx context.Context, streamURL string) (interface{}, error) {
	if !strings.Contains(streamURL, "://") {
		streamURL = p.baseURLLocal + "/" + strings.TrimLeft(streamURL, "/")
	}
	// A scheme-less path is a self-origin URL too, so it gets the same
	// self-origin path check (issue #65).
	if err := validateMediaURL(streamURL, p.baseURLLocal, p.privateAllow); err != nil {
		return nil, err
	}
	// Rewrite loopback HTTPS to plain HTTP (ffprobe rejects self-signed
	// certs); matches Tracks(), which applies the same rewrite before its
	// own ffprobe invocation.
	streamURL = localize(streamURL)

	// Serve from cache — avoids repeated ffprobe spawns for the same URL.
	if res, ok, err := p.cachedProbe(streamURL); ok {
		return res, err
	}

	v, err := p.guard.do(ctx, "probe|"+normalizeProbeKey(streamURL), func(runCtx context.Context) (interface{}, error) {
		if res, ok, err := p.cachedProbe(streamURL); ok {
			return res, err
		}
		return p.runProbeFFprobe(runCtx, streamURL)
	})
	return v, err
}

func (p *prober) cachedProbe(streamURL string) (interface{}, bool, error) {
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	if e, ok := p.probeCache[streamURL]; ok && time.Now().Before(e.expiresAt) {
		return e.result, true, e.err
	}
	return nil, false, nil
}

func (p *prober) runProbeFFprobe(parent context.Context, streamURL string) (interface{}, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := p.guard.acquire(ctx); err != nil {
		return nil, err
	}
	defer p.guard.release()

	// SEC-4: never hand ffprobe the real remote URL directly — it does its
	// own DNS resolution (DNS-rebinding) and would follow redirects itself.
	// Route it through the loopback relay instead; see relayInput.
	inputURL, protoWL, err := relayInput(streamURL, p.baseURLLocal)
	var out []byte
	if err == nil {
		out, err = runCapped(exec.CommandContext(ctx, "ffprobe",
			"-v", "quiet",
			"-print_format", "json",
			"-protocol_whitelist", protoWL,
			"-show_format",
			"-show_streams",
			inputURL,
		), ffprobeOutputLimit) // MED-1: bound ffprobe stdout
	}
	var result map[string]interface{}
	if err != nil {
		err = fmt.Errorf("ffprobe: %w", err)
	} else if jErr := json.Unmarshal(out, &result); jErr != nil {
		err = fmt.Errorf("ffprobe json decode: %w", jErr)
		result = nil
	}

	// Cache with TTL: 5 min on success, 30 s on error (bounds broken-URL hammering).
	p.probeMu.Lock()
	if p.probeCache == nil {
		p.probeCache = make(map[string]probeResultEntry)
	}
	if len(p.probeCache) >= probeTracksCacheMaxSize {
		now := time.Now()
		for k, v := range p.probeCache {
			if now.After(v.expiresAt) {
				delete(p.probeCache, k)
			}
		}
	}
	// Hard size cap: evict soonest-expiring entry until under limit.
	for len(p.probeCache) >= probeTracksCacheMaxSize {
		var evict string
		var evictExp time.Time
		for k, v := range p.probeCache {
			if evict == "" || v.expiresAt.Before(evictExp) {
				evict = k
				evictExp = v.expiresAt
			}
		}
		delete(p.probeCache, evict)
	}
	ttl := 5 * time.Minute
	if err != nil {
		ttl = 30 * time.Second
	}
	p.probeCache[streamURL] = probeResultEntry{result: result, err: err, expiresAt: time.Now().Add(ttl)}
	p.probeMu.Unlock()

	// Keep the historical typed-nil-map behaviour for callers.
	return result, err
}

// Tracks returns embedded non-video stream metadata for rawURL.
// It runs ffprobe -show_streams and returns a JSON-compatible slice of maps
// (one per audio or subtitle stream) in the shape:
//
//	{ "id":<stream_index>, "type":"audio"|"subtitle", "codec":<codec_name>,
//	  "lang":<tags.language>, "label":<tags.title>, "channels":<channels> }
//
// Scheme-less URLs are prefixed with p.baseURLLocal; URLs that carry their own
// scheme are validated (http/https only, non-private host) before reaching
// ffprobe, exactly as in Probe.
// The loopback HTTPS→HTTP rewrite (localize) is applied so ffprobe can read
// self-signed TLS streams.
func (p *prober) Tracks(rawURL string) (interface{}, error) {
	return p.TracksContext(context.Background(), rawURL)
}

// TracksContext is Tracks with caller cancellation, bounded concurrency and
// per-URL singleflight (see ProbeContext).
func (p *prober) TracksContext(ctx context.Context, rawURL string) (interface{}, error) {
	streamURL := rawURL
	if !strings.Contains(streamURL, "://") {
		streamURL = p.baseURLLocal + "/" + strings.TrimLeft(streamURL, "/")
	}
	// A scheme-less path is a self-origin URL too, so it gets the same
	// self-origin path check (issue #65).
	if err := validateMediaURL(streamURL, p.baseURLLocal, p.privateAllow); err != nil {
		return nil, err
	}
	// Rewrite loopback HTTPS to plain HTTP (ffprobe rejects self-signed certs).
	streamURL = localize(streamURL)

	// Serve from cache — avoids repeated ffprobe spawns for the same URL.
	if res, ok, err := p.cachedTracks(streamURL); ok {
		return res, err
	}
	return p.guard.do(ctx, "tracks|"+normalizeProbeKey(streamURL), func(runCtx context.Context) (interface{}, error) {
		if res, ok, err := p.cachedTracks(streamURL); ok {
			return res, err
		}
		return p.runTracksFFprobe(runCtx, streamURL)
	})
}

func (p *prober) cachedTracks(streamURL string) (interface{}, bool, error) {
	p.tracksMu.Lock()
	defer p.tracksMu.Unlock()
	if e, ok := p.tracksCache[streamURL]; ok && time.Now().Before(e.expiresAt) {
		return e.result, true, e.err
	}
	return nil, false, nil
}

func (p *prober) runTracksFFprobe(parent context.Context, streamURL string) (interface{}, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := p.guard.acquire(ctx); err != nil {
		return nil, err
	}
	defer p.guard.release()

	// SEC-4: route through the loopback relay instead of handing ffprobe the
	// real remote URL; see relayInput / Probe().
	var out []byte
	inputURL, protoWL, err := relayInput(streamURL, p.baseURLLocal)
	if err == nil {
		out, err = runCapped(exec.CommandContext(ctx, "ffprobe",
			"-v", "quiet", "-print_format", "json",
			"-protocol_whitelist", protoWL,
			"-show_streams", inputURL,
		), ffprobeOutputLimit) // MED-1: bound ffprobe stdout
	}

	var result interface{}
	if err != nil {
		err = fmt.Errorf("ffprobe: %w", err)
	} else {
		var r struct {
			Streams []struct {
				Index     int    `json:"index"`
				CodecType string `json:"codec_type"`
				CodecName string `json:"codec_name"`
				Channels  int    `json:"channels"`
				Tags      struct {
					Language string `json:"language"`
					Title    string `json:"title"`
				} `json:"tags"`
			} `json:"streams"`
		}
		if jErr := json.Unmarshal(out, &r); jErr != nil {
			err = fmt.Errorf("ffprobe json decode: %w", jErr)
		} else {
			tracks := make([]interface{}, 0, len(r.Streams))
			for _, st := range r.Streams {
				if st.CodecType == "video" {
					continue
				}
				tracks = append(tracks, map[string]interface{}{
					"id":       st.Index,
					"type":     st.CodecType,
					"codec":    st.CodecName,
					"lang":     st.Tags.Language,
					"label":    st.Tags.Title,
					"channels": st.Channels,
				})
			}
			result = tracks
		}
	}

	// Cache with TTL: 5 min on success, 30 s on error.
	p.tracksMu.Lock()
	if p.tracksCache == nil {
		p.tracksCache = make(map[string]tracksCacheEntry)
	}
	if len(p.tracksCache) >= probeTracksCacheMaxSize {
		now := time.Now()
		for k, v := range p.tracksCache {
			if now.After(v.expiresAt) {
				delete(p.tracksCache, k)
			}
		}
	}
	// Hard size cap: evict soonest-expiring entry until under limit.
	for len(p.tracksCache) >= probeTracksCacheMaxSize {
		var evict string
		var evictExp time.Time
		for k, v := range p.tracksCache {
			if evict == "" || v.expiresAt.Before(evictExp) {
				evict = k
				evictExp = v.expiresAt
			}
		}
		delete(p.tracksCache, evict)
	}
	ttl := 5 * time.Minute
	if err != nil {
		ttl = 30 * time.Second
	}
	p.tracksCache[streamURL] = tracksCacheEntry{result: result, err: err, expiresAt: time.Now().Add(ttl)}
	p.tracksMu.Unlock()

	return result, err
}

// OpenSubHash computes the OpenSubtitles 64-bit file hash for videoURL.
//
// Algorithm (per the OpenSubtitles spec):
//
//	hash = (filesize
//	        + Σ uint64LE over first 64 KiB
//	        + Σ uint64LE over last  64 KiB) mod 2^64
//
// The result is formatted as a 16-character lower-case hex string.
// Returns map[string]interface{}{"hash": <hex>, "size": <int64>}.
//
// Only http/https URLs on a public host are accepted (validateRemoteURL);
// videoURL never reaches the filesystem, closing the arbitrary local-file-read
// this endpoint was directly exposed to via the unauthenticated
// /opensubHash?videoUrl= query parameter.
func (p *prober) OpenSubHash(videoURL string) (interface{}, error) {
	return p.OpenSubHashContext(context.Background(), videoURL)
}

// OpenSubHashContext is OpenSubHash with caller cancellation, bounded
// concurrency and per-URL singleflight (see ProbeContext).
func (p *prober) OpenSubHashContext(ctx context.Context, videoURL string) (interface{}, error) {
	if err := validateRemoteURL(videoURL, p.baseURLLocal); err != nil {
		return nil, err
	}
	// Rewrite loopback HTTPS to plain HTTP: fetchHTTPChunks' openSubClient
	// is plain net/http and cannot complete a TLS handshake against this
	// server's self-signed :12470 certificate.
	videoURL = localize(videoURL)

	return p.guard.do(ctx, "opensub|"+normalizeProbeKey(videoURL), func(runCtx context.Context) (interface{}, error) {
		if err := p.guard.acquire(runCtx); err != nil {
			return nil, err
		}
		defer p.guard.release()
		size, head, tail, err := fetchHTTPChunks(runCtx, videoURL, p.baseURLLocal)
		if err != nil {
			return nil, err
		}
		h := computeOpenSubHash(size, head, tail)
		return map[string]interface{}{
			"hash": fmt.Sprintf("%016x", h),
			"size": size,
		}, nil
	})
}

// computeOpenSubHash accumulates the uint64 checksum.
// Natural uint64 overflow implements mod 2^64.
func computeOpenSubHash(size int64, head, tail []byte) uint64 {
	var h uint64
	h += uint64(size)
	for i := 0; i+8 <= len(head); i += 8 {
		h += binary.LittleEndian.Uint64(head[i : i+8])
	}
	for i := 0; i+8 <= len(tail); i += 8 {
		h += binary.LittleEndian.Uint64(tail[i : i+8])
	}
	return h
}

// validateRemoteURL is the single SSRF/local-file-read choke point for every
// externally-supplied media URL that reaches an ffprobe/ffmpeg subprocess or
// an outbound HTTP client (Probe, StartHLS, OpenSubHash, fetchSubBytes). Only
// http/https schemes are accepted — file://, pipe:, concat:, data:, bare
// paths, and every other ffmpeg-supported protocol are rejected outright —
// and the resolved host must not be a private, loopback, link-local, or
// cloud-metadata address (checked via netguard.ValidateIP). IP-literal hosts
// are checked directly with no DNS lookup; hostnames are resolved and every
// returned address is checked, so a DNS answer mixing public and private
// addresses is still rejected.
//
// selfBase (this server's own local base URL, e.g. "http://127.0.0.1:11470")
// is exempted from the IP check: when the HTTPS UI on :12470 asks the server
// to transcode or probe a stream the server itself is serving on :11470, the
// media URL legitimately points at loopback. Only that exact origin is
// allowed — every other loopback or private target is still rejected — and
// localize() is applied first so the self-signed https://…:12470 form maps
// onto the same origin. On that origin only media routes are accepted (see
// classifySelfPath). Pass "" to disallow
// self-references entirely.
func validateRemoteURL(raw, selfBase string) error {
	return validateMediaURL(raw, selfBase, nil)
}

// validateMediaURL is validateRemoteURL for a media URL that ffmpeg/ffprobe
// will read through the relay (Probe, Tracks, StartHLS): addresses on allow
// (HLSConfig.PrivateAllow) pass even though they are private, matching the
// relay's own dial-time check. A nil allow is exactly validateRemoteURL.
func validateMediaURL(raw, selfBase string, allow *netguard.Allow) error {
	if err := checkMediaURL(raw, selfBase, allow); err != nil {
		return invalidMediaURLError{err}
	}
	return nil
}

// invalidMediaURLError marks a media URL the caller supplied as unusable
// (malformed, wrong scheme, refused target, unresolvable host). It keeps the
// underlying message and matches types.ErrInvalidMediaURL, which the API layer
// maps to 400 instead of 500.
type invalidMediaURLError struct{ err error }

func (e invalidMediaURLError) Error() string { return e.err.Error() }
func (e invalidMediaURLError) Unwrap() error { return e.err }
func (e invalidMediaURLError) Is(target error) bool {
	return target == types.ErrInvalidMediaURL
}

func checkMediaURL(raw, selfBase string, allow *netguard.Allow) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme %q (only http/https allowed)", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("URL %q has no host", raw)
	}
	if isSelfOrigin(raw, selfBase) {
		return validateSelfURL(raw)
	}
	ips := []net.IP{net.ParseIP(host)}
	if ips[0] == nil {
		if ips, err = net.LookupIP(host); err != nil {
			return fmt.Errorf("resolving host %q: %w", host, err)
		}
	}
	for _, ip := range ips {
		if err := netguard.ValidateIPAllow(ip, true, allow); err != nil {
			return fmt.Errorf("URL %q: %w", raw, err)
		}
	}
	return nil
}

// fetchHTTPChunks fetches the first and last 64 KiB of a remote file
// using HTTP Range requests, returning (size, head, tail, err).
func fetchHTTPChunks(ctx context.Context, url, selfBase string) (size int64, head, tail []byte, err error) {
	client := subClientFor(url, selfBase)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("HEAD %s: %w", url, err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("HEAD %s: %w", url, err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, nil, nil, fmt.Errorf("HEAD %s: unexpected status %d", url, resp.StatusCode)
	}
	if resp.ContentLength <= 0 {
		return 0, nil, nil, fmt.Errorf("cannot determine Content-Length for %s", url)
	}
	size = resp.ContentLength

	headEnd := min(size, int64(chunkSize)) - 1
	head, err = httpRangeGetCtx(ctx, client, url, 0, headEnd)
	if err != nil {
		return 0, nil, nil, err
	}

	tailStart := max(size-chunkSize, 0)
	tail, err = httpRangeGetCtx(ctx, client, url, tailStart, size-1)
	if err != nil {
		return 0, nil, nil, err
	}

	return size, head, tail, nil
}

// httpRangeGet performs a Range GET [from, to] and returns exactly
// to-from+1 bytes, or an error. A 15-second context timeout is applied and
// the read is bounded by the requested length, so a server that ignores the
// Range header cannot cause an OOM.
//
// A 206 response must carry a Content-Range (when present) matching the
// request. A 200 response means the server ignored Range and is streaming the
// whole file from byte 0: that is only usable for a request starting at 0;
// for any other offset it is rejected, since returning the file's prefix as
// the "tail" would silently produce a wrong OpenSubtitles hash.
func httpRangeGet(client *http.Client, url string, from, to int64) ([]byte, error) {
	return httpRangeGetCtx(context.Background(), client, url, from, to)
}

// httpRangeGetCtx is httpRangeGet bounded additionally by ctx.
func httpRangeGetCtx(parent context.Context, client *http.Client, url string, from, to int64) ([]byte, error) {
	want := to - from + 1
	if want <= 0 {
		return nil, fmt.Errorf("range GET %s: invalid range %d-%d", url, from, to)
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", from, to))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		if cr := resp.Header.Get("Content-Range"); cr != "" {
			var gotFrom, gotTo int64
			if _, err := fmt.Sscanf(cr, "bytes %d-%d/", &gotFrom, &gotTo); err != nil || gotFrom != from || gotTo != to {
				return nil, fmt.Errorf("range GET %s: Content-Range %q does not match requested %d-%d", url, cr, from, to)
			}
		}
	case http.StatusOK:
		if from != 0 {
			return nil, fmt.Errorf("range GET %s: server ignored Range (status 200) for offset %d", url, from)
		}
	default:
		return nil, fmt.Errorf("range GET %s: unexpected status %d", url, resp.StatusCode)
	}

	buf := make([]byte, want)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil, fmt.Errorf("range GET %s: short body (want %d bytes): %w", url, want, err)
	}
	return buf, nil
}
