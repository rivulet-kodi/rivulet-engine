// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Tests for the loopback relay (SEC-4): internal/media/relay.go.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// ── relayInput: selfBase and remote inputs both go through the relay ────────

func TestRelayInputSelfBaseRoutesThroughRelay(t *testing.T) {
	const self = "http://127.0.0.1:11470"
	inputURL, protoWL, err := relayInput(self+testSelfPath, self)
	if err != nil {
		t.Fatalf("relayInput: %v", err)
	}
	// Never the direct URL (issue #69): local content may be an HLS playlist
	// whose entries ffmpeg would fetch itself.
	if !strings.HasPrefix(inputURL, "http://127.0.0.1:") || !strings.Contains(inputURL, "/r/") {
		t.Errorf("selfBase input = %q, want a loopback relay token URL", inputURL)
	}
	// Plain loopback HTTP only (issue #65): localize already maps the https UI
	// origin onto http, so https/tls/crypto are never needed here.
	if protoWL != "http,tcp" {
		t.Errorf("protocol whitelist = %q for selfBase input, want the narrow \"http,tcp\" set", protoWL)
	}
}

func TestRelayInputRemoteRoutesThroughRelay(t *testing.T) {
	inputURL, protoWL, err := relayInput("http://93.184.216.34/movie.mkv", "http://127.0.0.1:11470")
	if err != nil {
		t.Fatalf("relayInput: %v", err)
	}
	if !strings.HasPrefix(inputURL, "http://127.0.0.1:") || !strings.Contains(inputURL, "/r/") {
		t.Errorf("remote input = %q, want a loopback relay token URL (http://127.0.0.1:<port>/r/<token>)", inputURL)
	}
	// Never "file", and never https/tls/crypto — the relay itself only ever
	// speaks plain loopback HTTP.
	if protoWL != "http,tcp" {
		t.Errorf("protocol whitelist = %q for relayed input, want the narrow \"http,tcp\" set", protoWL)
	}
}

// ── relay guard: default blocks loopback, tests can inject a relaxed guard ──

// TestMediaRelayBlocksLoopbackUpstreamByDefault is the regression test for
// SEC-4's core guarantee: a relay instance built the way globalRelay is
// (blockPrivate=true) must refuse to connect to a loopback upstream, exactly
// like openSubClient already does for direct fetches. httptest.Server only
// ever binds to loopback, which is exactly the class of address ffmpeg/
// ffprobe must never be allowed to reach through a relayed "public-looking"
// URL (DNS rebinding / redirect to loopback / etc).
func TestMediaRelayBlocksLoopbackUpstreamByDefault(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("should never be reached"))
	}))
	defer upstream.Close()

	r := newMediaRelay(true) // production posture: block private/loopback
	relayURL, err := r.register(upstream.URL)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	resp, err := http.Get(relayURL) //nolint:gosec // relayURL is our own loopback listener
	if err != nil {
		t.Fatalf("GET relay URL: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want %d (upstream dial must be blocked by DialControl)", resp.StatusCode, http.StatusBadGateway)
	}
}

// TestMediaRelayServesAllowedUpstream exercises the full proxy path (GET,
// HEAD, Range forwarding) against a relay instance whose dial guard has been
// relaxed for the test (blockPrivate=false) — the injectable hook tests use
// in place of production's blockPrivate=true, since httptest can only bind
// to loopback.
func TestMediaRelayServesAllowedUpstream(t *testing.T) {
	const body = "hello from upstream"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if rng := req.Header.Get("Range"); rng != "" {
			w.Header().Set("X-Got-Range", rng)
		}
		w.Header().Set("Content-Type", "video/mp2t")
		if req.Method == http.MethodHead {
			w.Header().Set("Content-Length", "20")
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	defer upstream.Close()

	r := newMediaRelay(false)
	relayURL, err := r.register(upstream.URL)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// GET
	resp, err := http.Get(relayURL) //nolint:gosec
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(got) != body {
		t.Errorf("GET status=%d body=%q, want 200 %q", resp.StatusCode, got, body)
	}

	// HEAD
	headResp, err := http.Head(relayURL) //nolint:gosec
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	_ = headResp.Body.Close()
	if headResp.StatusCode != http.StatusOK {
		t.Errorf("HEAD status = %d, want 200", headResp.StatusCode)
	}

	// Range forwarding
	req, _ := http.NewRequest(http.MethodGet, relayURL, nil)
	req.Header.Set("Range", "bytes=0-3")
	rangeResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Range GET: %v", err)
	}
	defer func() { _ = rangeResp.Body.Close() }()
	if got := rangeResp.Header.Get("X-Got-Range"); got != "bytes=0-3" {
		t.Errorf("upstream saw Range = %q, want forwarded \"bytes=0-3\"", got)
	}
}

// TestMediaRelayUnknownTokenReturns404 verifies expired/unregistered tokens
// are rejected rather than silently proxying to a stale or empty target.
func TestMediaRelayUnknownTokenReturns404(t *testing.T) {
	r := newMediaRelay(false)
	if err := r.start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(r.port()) + "/r/does-not-exist")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for an unknown relay token", resp.StatusCode)
	}
}

// ── HLS playlist rewriting ───────────────────────────────────────────────────

// TestMediaRelayRewritesPlaylist is the regression test for SEC-4's HLS
// coverage: nested playlist URIs (plain segment references and quoted
// URI="..." tag attributes) must be resolved against the upstream URL and
// rewritten to fresh relay tokens, so ffmpeg's own nested-playlist/segment
// fetches are funneled back through this process's guarded client instead of
// going directly to the untrusted origin.
func TestMediaRelayRewritesPlaylist(t *testing.T) {
	const segBody = "segment-bytes"
	const keyBody = "key-bytes"

	mux := http.NewServeMux()
	mux.HandleFunc("/hls/master.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n"+
			"#EXT-X-VERSION:3\n"+
			"#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n"+
			"#EXT-X-TARGETDURATION:4\n"+
			"#EXTINF:4.0,\n"+
			"seg0.ts\n"+
			"#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/hls/seg0.ts", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, segBody)
	})
	mux.HandleFunc("/hls/key.bin", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, keyBody)
	})
	upstream := httptest.NewServer(mux)
	defer upstream.Close()

	r := newMediaRelay(false)
	relayURL, err := r.register(upstream.URL + "/hls/master.m3u8")
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	resp, err := http.Get(relayURL) //nolint:gosec
	if err != nil {
		t.Fatalf("GET playlist: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	playlist := string(body)

	if strings.Contains(playlist, "seg0.ts") {
		t.Errorf("playlist still references the raw upstream segment name:\n%s", playlist)
	}
	if strings.Contains(playlist, "key.bin") {
		t.Errorf("playlist still references the raw upstream key name:\n%s", playlist)
	}

	var segURL, keyURL string
	for _, line := range strings.Split(playlist, "\n") {
		switch {
		case strings.HasPrefix(line, "#EXT-X-KEY"):
			if i := strings.Index(line, `URI="`); i >= 0 {
				rest := line[i+len(`URI="`):]
				keyURL = rest[:strings.IndexByte(rest, '"')]
			}
		case strings.HasPrefix(line, "http://127.0.0.1:"):
			segURL = line
		}
	}
	if segURL == "" || !strings.Contains(segURL, "/r/") {
		t.Fatalf("segment line not rewritten to a relay token URL, playlist:\n%s", playlist)
	}
	if keyURL == "" || !strings.Contains(keyURL, "/r/") {
		t.Fatalf("URI=\"...\" attribute not rewritten to a relay token URL, playlist:\n%s", playlist)
	}

	segResp, err := http.Get(segURL) //nolint:gosec
	if err != nil {
		t.Fatalf("GET rewritten segment URL: %v", err)
	}
	segGot, _ := io.ReadAll(segResp.Body)
	_ = segResp.Body.Close()
	if string(segGot) != segBody {
		t.Errorf("rewritten segment URL body = %q, want %q", segGot, segBody)
	}

	keyResp, err := http.Get(keyURL) //nolint:gosec
	if err != nil {
		t.Fatalf("GET rewritten key URL: %v", err)
	}
	keyGot, _ := io.ReadAll(keyResp.Body)
	_ = keyResp.Body.Close()
	if string(keyGot) != keyBody {
		t.Errorf("rewritten key URL body = %q, want %q", keyGot, keyBody)
	}
}

// TestMediaRelayPlaylistSizeCap verifies an oversized upstream playlist is
// rejected instead of being buffered without bound.
func TestMediaRelayPlaylistSizeCap(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		buf := make([]byte, relayMaxPlaylistBytes+1)
		for i := range buf {
			buf[i] = '\n'
		}
		// A real playlist start: a body of nothing but whitespace is
		// refused as undecided XML before the size cap is reached.
		copy(buf, "#EXTM3U\n")
		_, _ = w.Write(buf)
	}))
	defer upstream.Close()

	r := newMediaRelay(false)
	relayURL, err := r.register(upstream.URL)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	resp, err := http.Get(relayURL) //nolint:gosec
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502 for an oversized playlist", resp.StatusCode)
	}
}

// TestMediaRelayResolvesRelativeAfterRedirect: a playlist reached through a
// redirect resolves its relative entries (segments and URI="..." attributes)
// against its own, post-redirect URL, not the URL the relay was asked for.
func TestMediaRelayResolvesRelativeAfterRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/watch/start", func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/cdn/hls/index.m3u8", http.StatusFound)
	})
	mux.HandleFunc("/cdn/hls/index.m3u8", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = io.WriteString(w, "#EXTM3U\n"+
			"#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n"+
			"#EXT-X-TARGETDURATION:4\n"+
			"#EXTINF:4.0,\n"+
			"seg0.ts\n"+
			"#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/cdn/hls/seg0.ts", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "segment-bytes")
	})
	mux.HandleFunc("/cdn/hls/key.bin", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "key-bytes")
	})
	upstream := httptest.NewServer(mux)
	defer upstream.Close()

	r := newMediaRelay(false)
	relayURL, err := r.register(upstream.URL + "/watch/start")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	resp, err := http.Get(relayURL) //nolint:gosec // our own loopback relay
	if err != nil {
		t.Fatalf("GET playlist: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("playlist status = %d, want 200", resp.StatusCode)
	}

	var segURL, keyURL string
	for _, line := range strings.Split(string(body), "\n") {
		switch {
		case strings.HasPrefix(line, "#EXT-X-KEY"):
			if i := strings.Index(line, `URI="`); i >= 0 {
				rest := line[i+len(`URI="`):]
				keyURL = rest[:strings.IndexByte(rest, '"')]
			}
		case strings.HasPrefix(line, "http://127.0.0.1:"):
			segURL = line
		}
	}
	for name, c := range map[string]struct{ url, want string }{
		"segment": {segURL, "segment-bytes"},
		"key":     {keyURL, "key-bytes"},
	} {
		if c.url == "" {
			t.Fatalf("%s not rewritten to a relay URL:\n%s", name, body)
		}
		got, err := http.Get(c.url) //nolint:gosec // our own loopback relay
		if err != nil {
			t.Fatalf("GET %s: %v", name, err)
		}
		b, _ := io.ReadAll(got.Body)
		_ = got.Body.Close()
		if got.StatusCode != http.StatusOK || string(b) != c.want {
			t.Errorf("%s: status %d body %q, want 200 %q", name, got.StatusCode, b, c.want)
		}
	}
}
