// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Tests for the self-origin media-route check (issue #65): a mediaURL on
// this server's own origin is read directly when its route serves local
// content, read through the media relay when the route passes a remote body
// through (/proxy, /ftp, /yt), and refused otherwise.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// testSelfIH is a syntactically valid infohash for self-origin test URLs.
const testSelfIH = "0123456789abcdef0123456789abcdef01234567"

// testSelfPath is a torrent-file path on this server's own origin: the
// canonical local-content self path every self-origin test URL uses.
const testSelfPath = "/" + testSelfIH + "/0"

func TestClassifySelfPath(t *testing.T) {
	cases := map[selfPathKind][]string{
		selfPathLocal: {
			testSelfPath,
			"/0123456789ABCDEF0123456789ABCDEF01234567/0",
			"/" + testSelfIH + "/-1",
			"/" + testSelfIH + "/video.mkv",
			"/" + testSelfIH + "/0/subtitles.vtt",
			"/stream/" + testSelfIH + "/0",
			// stremio-core's playable archive and NZB URLs: GET create
			// redirects to the matching /{ext}/stream/… route.
			"/zip/create", "/rar/create", "/7zip/create", "/tar/create", "/tgz/create", "/nzb/create",
			"/rar/create/key",
			"/nzb/create/key",
			"/zip/stream",
			"/rar/stream/key/movie.mkv",
			"/7zip/stream/key",
			"/tar/stream/key/a/b.mkv",
			"/tgz/stream/key/b.mkv",
			"/nzb/stream",
			"/nzb/stream/key",
			"/nzb/stream/key/movie.mkv",
			"/local-addon/file/abc123",
		},
		selfPathRelayed: {
			// stremio-core's proxyHeaders form: /proxy/{d=…&h=…}/{path}.
			"/proxy/d=http%3A%2F%2Fexample.com&h=User-Agent%3Ax/path/playlist.m3u8",
			"/proxy/d=http%3A%2F%2Fexample.com/",
			"/proxy/stream",
			"/proxy/hls/manifest.m3u8",
			"/ftp/movie.mkv",
			"/yt/dQw4w9WgXcQ",
		},
		selfPathRefused: {
			"",
			"/",
			"/removeAll",
			"/removeAll/x.ts", // the router dispatches on the first segment
			"/" + testSelfIH,
			"/" + testSelfIH + "/create",
			"/" + testSelfIH + "/remove",
			"/" + testSelfIH + "/stats.json",
			"/" + testSelfIH + "/0/stats.json",
			"/" + testSelfIH + "/peers",
			"/stream/" + testSelfIH,
			"/stream/not-an-infohash/0",
			"/proxy",
			"/proxy/",
			"/proxy/%2E%2E/removeAll",
			"/extractor/x",
			"/base64/aGVsbG8=",
			"/hlsv2/x/master.m3u8",
			"/rar",
			"/rar/create/key/extra",
			"/nzb",
			"/ftp",
			"/ftp/create", // 501 on this server; not a stream
			"/ftp/stream",
			"/ftp/a/b",
			"/yt",
			"/yt/a/b",
			"/local-addon/manifest.json",
			"/local-addon/file",
			"/casting/x",
			"/record/x",
			"/settings",
			"/subtitles.vtt",
			"/probe",
			// Traversal, empty segments and encoded slashes never normalize
			// (or split) onto another route.
			"/stream/../removeAll",
			"/" + testSelfIH + "/0/../../removeAll",
			"//removeAll",
			"/" + testSelfIH + "//0",
			"/./removeAll",
			"/stream%2F" + testSelfIH + "%2F0",
			"/rar%2Fcreate",
			"/%2E%2E/removeAll",
		},
	}
	for want, paths := range cases {
		for _, p := range paths {
			if got := classifySelfPath(p); got != want {
				t.Errorf("classifySelfPath(%q) = %d, want %d", p, got, want)
			}
		}
	}
}

// TestValidateMediaURLSelfOriginPaths: the check runs on the escaped path,
// segment by segment, so percent-encoding cannot smuggle a control route
// past it, and the https UI twin is held to the same rule.
func TestValidateMediaURLSelfOriginPaths(t *testing.T) {
	const self = "http://127.0.0.1:11470"
	cases := []struct {
		url     string
		wantErr bool
	}{
		{self + testSelfPath, false},
		{self + testSelfPath + "?tr=udp%3A%2F%2Ftracker.example%3A80", false},
		{"https://127.0.0.1:12470" + testSelfPath, false},
		{self + "/stream/" + testSelfIH + "/1", false},
		{self + "/rar/stream?key=k", false},
		{self + "/rar/create?lz=x", false},
		{self + "/nzb/create?lz=x", false},
		{self + "/nzb/stream/key", false},
		{self + "/proxy/d=http%3A%2F%2Fexample.com&h=a%3Ab/v/playlist.m3u8", false},
		{self + "/ftp/movie.mkv?lz=x", false},
		{self + "/yt/dQw4w9WgXcQ", false},
		{self + "/removeAll", true},
		{"https://127.0.0.1:12470/removeAll", true},
		{self + "/%72emoveAll", true},
		{self + "/stream/%2E%2E/removeAll", true},
		{self + "/stream%2F..%2FremoveAll", true},
		{self + "/extractor/x", true},
		{self + "/ftp/stream", true},
		{self + "/" + testSelfIH + "/remove", true},
	}
	for _, tc := range cases {
		err := validateMediaURL(tc.url, self, nil)
		if (err != nil) != tc.wantErr {
			t.Errorf("validateMediaURL(%q) error = %v, wantErr %v", tc.url, err, tc.wantErr)
		}
		if perr := validatePersistedURL(tc.url, self, nil); (perr != nil) != tc.wantErr {
			t.Errorf("validatePersistedURL(%q) error = %v, wantErr %v", tc.url, perr, tc.wantErr)
		}
	}
}

// TestRelayInputSelfRoutes: local routes go to ffmpeg directly, pass-through
// routes go through the relay, and anything else is refused here too, so a
// caller that skipped validateMediaURL still cannot hand ffmpeg a control
// route. The whitelist is "http,tcp" for all of them.
func TestRelayInputSelfRoutes(t *testing.T) {
	const self = "http://127.0.0.1:11470"
	for _, tc := range []struct {
		path    string
		relayed bool
	}{
		{testSelfPath, false},
		{"/rar/create?lz=x", false},
		{"/nzb/stream/key", false},
		{"/proxy/d=http%3A%2F%2Fexample.com/v.m3u8", true},
		{"/ftp/movie.mkv?lz=x", true},
		{"/yt/dQw4w9WgXcQ", true},
	} {
		in, wl, err := relayInput(self+tc.path, self)
		if err != nil {
			t.Errorf("relayInput(%s): %v", tc.path, err)
			continue
		}
		if wl != "http,tcp" {
			t.Errorf("relayInput(%s) whitelist = %q, want http,tcp", tc.path, wl)
		}
		if got := strings.Contains(in, "/r/"); got != tc.relayed {
			t.Errorf("relayInput(%s) = %q, relayed = %v, want %v", tc.path, in, got, tc.relayed)
		}
	}
	for _, p := range []string{"/removeAll", "/extractor/x", "/x/0"} {
		if in, _, err := relayInput(self+p, self); err == nil {
			t.Errorf("relayInput(%s) = %q; want it refused", p, in)
		}
	}
}

// TestRelayExtSuffix: rewritten playlist entries keep the upstream
// extension, which ffmpeg >= 7.1 requires of an HLS segment URL.
func TestRelayExtSuffix(t *testing.T) {
	for in, want := range map[string]string{
		"http://h/a/seg0.ts":                     "/s.ts",
		"http://h/a/SEG.M4S?x=1":                 "/s.m4s",
		"http://h/a/sub.vtt#t":                   "/s.vtt",
		"http://h/a/index.m3u8":                  "/s.m3u8",
		"http://h/a/noext":                       "",
		"http://h/a.dir/noext":                   "",
		"http://h/a/trailing.":                   "",
		"http://h/a/x.waytoolongext":             "",
		"http://h/a/x.t%2Fs":                     "",
		"http://h/proxy/d=x/v/seg1.ts?token=a.b": "/s.ts",
	} {
		if got := relayExtSuffix(in); got != want {
			t.Errorf("relayExtSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}

// hitRecorder is an httptest handler that records every path requested of it.
type hitRecorder struct {
	mu   sync.Mutex
	hits []string
	h    http.HandlerFunc
}

func (r *hitRecorder) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	r.hits = append(r.hits, req.URL.Path)
	r.mu.Unlock()
	if r.h != nil {
		r.h(w, req)
	}
}

func (r *hitRecorder) paths() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.hits...)
}

func (r *hitRecorder) hit(path string) bool {
	return slices.Contains(r.paths(), path)
}

// fakeSelfOrigin is a stand-in for this server's own origin: /proxy/d=x/…
// passes a playlist through (as /proxy does for a remote HLS source), /yt/…
// redirects (as /yt does to the extracted URL). private is a second loopback
// server: not this origin, so the relay must never reach it.
func fakeSelfOrigin(t *testing.T) (self, private *hitRecorder, selfURL, privateURL string) {
	t.Helper()
	private = &hitRecorder{h: func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "PRIVATE") }}
	ps := httptest.NewServer(private)
	t.Cleanup(ps.Close)
	self = &hitRecorder{}
	ss := httptest.NewServer(self)
	t.Cleanup(ss.Close)
	self.h = func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxy/d=x/list.m3u8":
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n"+
				"#EXTINF:4,\nseg0.ts\n"+ // relative: <self>/proxy/d=x/seg0.ts
				"#EXTINF:4,\n%s/removeAll\n"+
				"#EXTINF:4,\n%s/private.ts\n"+
				"#EXTINF:4,\n%s%s\n"+
				"#EXT-X-ENDLIST\n", ss.URL, ps.URL, ss.URL, testSelfPath)
		case "/proxy/d=x/hostile.m3u8":
			// The hostile entries come first: ffprobe opens a playlist's
			// segments in order and skips the ones that fail. They carry a
			// media extension, which FFmpeg 8 requires of a segment
			// (extension_picky); the router dispatches /removeAll/x.ts on
			// its first segment alone, so it is still /removeAll.
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n"+
				"#EXTINF:4,\n%s/private.ts\n"+
				"#EXTINF:4,\n%s/removeAll/x.ts\n"+
				"#EXTINF:4,\nseg0.ts\n"+
				"#EXT-X-ENDLIST\n", ps.URL, ss.URL)
		case "/proxy/d=x/seg0.ts":
			_, _ = io.WriteString(w, "SEG")
		case testSelfPath:
			_, _ = io.WriteString(w, "LOCAL")
		case "/yt/to-removeall":
			http.Redirect(w, r, "/removeAll", http.StatusTemporaryRedirect)
		case "/yt/to-private":
			http.Redirect(w, r, ps.URL+"/private.ts", http.StatusTemporaryRedirect)
		case "/yt/to-self-proxy":
			http.Redirect(w, r, ss.URL+"/proxy/d=x/seg0.ts", http.StatusTemporaryRedirect)
		default:
			http.NotFound(w, r)
		}
	}
	return self, private, ss.URL, ps.URL
}

var selfRelayTestClient = &http.Client{Timeout: 15 * time.Second}

func selfRelayGet(t *testing.T, u string) (int, string) {
	t.Helper()
	resp, err := selfRelayTestClient.Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestRelaySelfProxyNestedPlaylist: a self /proxy URL is relayed. In the
// playlist it serves, a relative entry (which resolves onto self /proxy)
// and a local-content entry stay reachable, a self control route becomes a
// dead token, and an entry on another (private) host goes through the
// guarded client, which refuses to dial it.
func TestRelaySelfProxyNestedPlaylist(t *testing.T) {
	self, private, selfURL, privateURL := fakeSelfOrigin(t)
	in, _, err := relayInput(selfURL+"/proxy/d=x/list.m3u8", selfURL)
	if err != nil {
		t.Fatal(err)
	}
	code, body := selfRelayGet(t, in)
	if code != http.StatusOK {
		t.Fatalf("relayed playlist = %d %q", code, body)
	}
	if strings.Contains(body, selfURL) || strings.Contains(body, privateURL) {
		t.Fatalf("rewritten playlist still names an upstream host:\n%s", body)
	}
	var entries []string
	for _, l := range strings.Split(body, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			entries = append(entries, l)
		}
	}
	if len(entries) != 4 {
		t.Fatalf("rewritten playlist has %d entries, want 4:\n%s", len(entries), body)
	}
	if !strings.HasSuffix(entries[0], "/s.ts") {
		t.Errorf("relative entry rewritten to %q, want the upstream .ts extension kept", entries[0])
	}
	if code, b := selfRelayGet(t, entries[0]); code != http.StatusOK || b != "SEG" {
		t.Errorf("relative entry via self /proxy = %d %q, want 200 SEG", code, b)
	}
	if !strings.HasSuffix(entries[1], "/r/refused") {
		t.Errorf("self /removeAll entry rewritten to %q, want a dead /r/refused token", entries[1])
	}
	if code, _ := selfRelayGet(t, entries[1]); code != http.StatusNotFound {
		t.Errorf("dead token = %d, want 404", code)
	}
	if code, _ := selfRelayGet(t, entries[2]); code != http.StatusBadGateway {
		t.Errorf("private-host entry = %d, want 502 (dial refused)", code)
	}
	if code, b := selfRelayGet(t, entries[3]); code != http.StatusOK || b != "LOCAL" {
		t.Errorf("local-content entry = %d %q, want 200 LOCAL", code, b)
	}
	if self.hit("/removeAll") {
		t.Error("self /removeAll was requested through the relay")
	}
	if hits := private.paths(); len(hits) != 0 {
		t.Errorf("private host was requested %v through the relay", hits)
	}
}

// TestRelaySelfRedirects: a relayed self route's redirect (as /yt answers)
// is re-registered rather than followed: a redirect to a self control route
// is refused, one to another host goes through the guarded client, and one
// to a self media route is followed through a fresh self token.
func TestRelaySelfRedirects(t *testing.T) {
	self, private, selfURL, _ := fakeSelfOrigin(t)
	get := func(path string) (int, string) {
		t.Helper()
		in, _, err := relayInput(selfURL+path, selfURL)
		if err != nil {
			t.Fatal(err)
		}
		return selfRelayGet(t, in)
	}
	if code, _ := get("/yt/to-removeall"); code != http.StatusBadGateway {
		t.Errorf("redirect to self /removeAll = %d, want 502", code)
	}
	if code, _ := get("/yt/to-private"); code != http.StatusBadGateway {
		t.Errorf("redirect to a private host = %d, want 502", code)
	}
	if code, b := get("/yt/to-self-proxy"); code != http.StatusOK || b != "SEG" {
		t.Errorf("redirect to self /proxy = %d %q, want 200 SEG", code, b)
	}
	if self.hit("/removeAll") {
		t.Error("self /removeAll was requested through a relayed redirect")
	}
	if hits := private.paths(); len(hits) != 0 {
		t.Errorf("private host was requested %v through a relayed redirect", hits)
	}
}

// TestRelaySelfDialPinned: a self token's client dials only the host:port of
// its own origin.
func TestRelaySelfDialPinned(t *testing.T) {
	_, private, _, privateURL := fakeSelfOrigin(t)
	r := newMediaRelay(true)
	tok, err := r.registerAs(privateURL+"/private.ts", "http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := selfRelayGet(t, tok); code != http.StatusBadGateway {
		t.Errorf("self token for another host:port = %d, want 502", code)
	}
	if hits := private.paths(); len(hits) != 0 {
		t.Errorf("self client dialed another host:port: %v", hits)
	}
}

// TestProbeContextSelfProxyNestedPlaylist is the nested-playlist case of
// issue #65 through ffprobe itself: ProbeContext on a self /proxy playlist
// whose entries name self /removeAll and a private host must reach
// neither. Skipped without ffprobe.
func TestProbeContextSelfProxyNestedPlaylist(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not on PATH")
	}
	self, private, selfURL, _ := fakeSelfOrigin(t)
	p := newTestProber()
	p.baseURLLocal = selfURL
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, perr := p.ProbeContext(ctx, selfURL+"/proxy/d=x/hostile.m3u8")
	t.Logf("probe err %v; self hits %v; private hits %v", perr, self.paths(), private.paths())
	if !self.hit("/proxy/d=x/hostile.m3u8") {
		t.Fatalf("ffprobe never read the relayed playlist; hits %v", self.paths())
	}
	if self.hit("/removeAll/x.ts") {
		t.Error("ffprobe reached self /removeAll through a nested playlist entry")
	}
	if hits := private.paths(); len(hits) != 0 {
		t.Errorf("ffprobe reached the private host %v through a nested playlist entry", hits)
	}
}

// TestProbeContextRefusesSelfControlRoutes: ProbeContext and TracksContext
// refuse a self control route before ffprobe runs, as an absolute URL or a
// scheme-less path, so it is never requested.
func TestProbeContextRefusesSelfControlRoutes(t *testing.T) {
	rec := &hitRecorder{}
	ts := httptest.NewServer(rec)
	defer ts.Close()
	p := newTestProber()
	p.baseURLLocal = ts.URL
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, in := range []string{
		ts.URL + "/removeAll",
		ts.URL + "/extractor/x/playlist.m3u8",
		ts.URL + "/" + testSelfIH + "/remove",
		"removeAll",
		"/extractor/x",
	} {
		if _, err := p.ProbeContext(ctx, in); err == nil {
			t.Errorf("ProbeContext(%q) succeeded; want the self-origin path refused", in)
		}
		if _, err := p.TracksContext(ctx, in); err == nil {
			t.Errorf("TracksContext(%q) succeeded; want the self-origin path refused", in)
		}
	}
	if hits := rec.paths(); len(hits) != 0 {
		t.Errorf("self origin was requested %v; a refused mediaURL must never be fetched", hits)
	}
}

// TestStartHLSRefusesSelfControlRoute: StartHLS (GET /hlsv2/{id}/master.m3u8)
// refuses a self-origin control route without requesting it.
func TestStartHLSRefusesSelfControlRoute(t *testing.T) {
	rec := &hitRecorder{}
	ts := httptest.NewServer(rec)
	defer ts.Close()

	m := newTestHLSManager(t)
	m.selfBase = ts.URL
	for _, in := range []string{ts.URL + "/removeAll", ts.URL + "/extractor/x"} {
		if _, err := m.StartHLS("sess-ctl", in, types.HLSSessionOptions{}); err == nil {
			t.Errorf("StartHLS(%q) succeeded; want the self-origin path refused", in)
		}
	}
	if hits := rec.paths(); len(hits) != 0 {
		t.Errorf("self origin was requested %v; a refused mediaURL must never be fetched", hits)
	}
}

// TestHLSPersistDisallowedSelfURLRemoved: a persisted session whose mediaURL
// is a self-origin control route (e.g. one saved before this check existed)
// is discarded on restore, while ones on local and relayed routes are kept.
func TestHLSPersistDisallowedSelfURLRemoved(t *testing.T) {
	const self = "http://127.0.0.1:11470"
	work := t.TempDir()
	m1 := newTestPersistManagerSelf(t, work, HLSConfig{}, nil, self)
	bad := seedPersistedSessionWith(t, m1, "bad-self", time.Now(), func(s *hlsSession) { s.mediaURL = self + "/removeAll" })
	ext := seedPersistedSessionWith(t, m1, "extractor-self", time.Now(), func(s *hlsSession) { s.mediaURL = self + "/extractor/x" })
	good := seedPersistedSessionWith(t, m1, "good-self", time.Now(), func(s *hlsSession) { s.mediaURL = self + testSelfPath })
	proxy := seedPersistedSessionWith(t, m1, "proxy-self", time.Now(), func(s *hlsSession) { s.mediaURL = self + "/proxy/d=x/a.m3u8" })
	m1.CloseHLS()

	m2 := newTestPersistManagerSelf(t, work, HLSConfig{}, nil, self)
	assertGone(t, bad)
	assertGone(t, ext)
	assertExists(t, good)
	assertExists(t, proxy)
	if n := m2.Sessions(); n != 2 {
		t.Errorf("Sessions() = %d, want 2 (good-self, proxy-self)", n)
	}
}

// TestValidateMediaURLErrorsAreInvalidMediaURL: every rejection of a caller
// supplied URL matches types.ErrInvalidMediaURL, so the API answers 400
// rather than 500, and the original message is kept.
func TestValidateMediaURLErrorsAreInvalidMediaURL(t *testing.T) {
	const self = "http://127.0.0.1:11470"
	for _, raw := range []string{"", "ftp://example.com/x", "http://", self + "/removeAll", self + "/", "http://127.0.0.2:1/x", "%zz"} {
		err := validateMediaURL(raw, self, nil)
		if err == nil {
			t.Errorf("validateMediaURL(%q) accepted", raw)
			continue
		}
		if !errors.Is(err, types.ErrInvalidMediaURL) {
			t.Errorf("validateMediaURL(%q) = %v, want ErrInvalidMediaURL", raw, err)
		}
	}
	err := validateMediaURL("ftp://example.com/x", self, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported URL scheme") {
		t.Errorf("message lost: %v", err)
	}
}
