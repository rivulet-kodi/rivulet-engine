// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api integration and unit tests.
// Runs in the same package so unexported helpers are directly callable.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// ─────────────────────────────────────────────────────────────────────────────
// Fakes
// ─────────────────────────────────────────────────────────────────────────────

// seekCloser wraps bytes.Reader with a no-op Close to satisfy io.ReadSeekCloser.
type seekCloser struct{ *bytes.Reader }

func (s seekCloser) Close() error { return nil }

// fakeEngine implements types.Engine backed by a static byte slice.
type fakeEngine struct {
	ih    string
	data  []byte
	files []types.FileInfo
}

func (e *fakeEngine) InfoHash() string              { return e.ih }
func (e *fakeEngine) Ready(_ context.Context) error { return nil }
func (e *fakeEngine) Files() []types.FileInfo       { return e.files }
func (e *fakeEngine) GuessFileIdx() int {
	if len(e.files) > 0 {
		return 0
	}
	return -1
}
func (e *fakeEngine) NewReader(_ int) (io.ReadSeekCloser, int64, error) {
	return seekCloser{bytes.NewReader(e.data)}, int64(len(e.data)), nil
}
func (e *fakeEngine) Stats(idx int) *types.Stats {
	st := &types.Stats{
		InfoHash:          e.ih,
		Name:              "fake torrent",
		Peers:             2,
		Unchoked:          1,
		Queued:            0,
		Unique:            1,
		ConnectionTries:   3,
		SwarmPaused:       false,
		SwarmConnections:  2,
		SwarmSize:         10,
		Selections:        []any{},
		Wires:             []types.Wire{},
		Files:             e.files,
		Downloaded:        100,
		Uploaded:          50,
		DownloadSpeed:     1024,
		UploadSpeed:       512,
		Sources:           []types.Source{},
		Opts:              types.Options{DHT: true, Path: "/cache", PeerSearch: types.PeerSearch{Min: 40, Max: 200, Sources: []string{"dht:abc"}}, Tracker: true},
		PeerSearchRunning: false,
	}
	if idx >= 0 {
		l := int64(len(e.data))
		name := "fake.mkv"
		prog := 0.5
		st.StreamLen = &l
		st.StreamName = &name
		st.StreamProgress = &prog
	}
	return st
}

// fakeEM implements types.EngineManager.
type fakeEM struct {
	m map[string]*fakeEngine
}

func newFakeEM(engines ...*fakeEngine) *fakeEM {
	m := &fakeEM{m: make(map[string]*fakeEngine)}
	for _, e := range engines {
		m.m[e.ih] = e
	}
	return m
}

func (m *fakeEM) EnsureEngine(ih string, _ types.AddOptions) (types.Engine, error) {
	if e, ok := m.m[ih]; ok {
		return e, nil
	}
	return nil, errors.New("engine not found: " + ih)
}
func (m *fakeEM) GetEngine(ih string) (types.Engine, bool) {
	e, ok := m.m[ih]
	if !ok {
		return nil, false
	}
	return e, true
}
func (m *fakeEM) RemoveEngine(ih string) error { delete(m.m, ih); return nil }
func (m *fakeEM) RemoveAll()                   { m.m = make(map[string]*fakeEngine) }
func (m *fakeEM) ListEngines() []string {
	out := make([]string, 0, len(m.m))
	for ih := range m.m {
		out = append(out, ih)
	}
	return out
}
func (m *fakeEM) AllStats() map[string]*types.Stats {
	out := make(map[string]*types.Stats, len(m.m))
	for ih, e := range m.m {
		out[ih] = e.Stats(-1)
	}
	return out
}
func (m *fakeEM) Close() error { return nil }

// fakeSS implements types.SettingsStore.
type fakeSS struct {
	vals map[string]any
}

func (s *fakeSS) Values() map[string]any {
	if s.vals == nil {
		return map[string]any{}
	}
	return s.vals
}
func (s *fakeSS) OptionsSchema(_ []string) []map[string]any {
	return []map[string]any{{"key": "cacheSize", "type": "number"}}
}
func (s *fakeSS) Extend(patch map[string]any) {
	if s.vals == nil {
		s.vals = make(map[string]any)
	}
	for k, v := range patch {
		s.vals[k] = v
	}
}
func (s *fakeSS) Get(key string) any {
	if s.vals == nil {
		return nil
	}
	return s.vals[key]
}
func (s *fakeSS) Save() error { return nil }

// fakeProber implements types.MediaProber with no-op methods. deleteHLS, when
// set, backs DeleteHLS (nil = succeed); deleted records every id DeleteHLS
// was called with. startHLSErr, when set, is returned by StartHLS; the last
// StartHLS options are recorded in lastHLSOpts so handler tests can assert
// query pass-through.
type fakeProber struct {
	deleteHLS func(id string) error
	deleted   []string

	startHLSErr error
	lastHLSOpts types.HLSSessionOptions
	hlsFileErr  error // returned by HLSFile when set
	probeErr    error // returned by Probe/OpenSubHash/SubtitlesTracks when set

	writeSubsErr error // returned by WriteSubtitles when set
}

func (p *fakeProber) Probe(_ string) (any, error) {
	if p.probeErr != nil {
		return nil, p.probeErr
	}
	return map[string]any{}, nil
}
func (p *fakeProber) Tracks(_ string) (any, error) {
	if p.probeErr != nil {
		return nil, p.probeErr
	}
	return []any{}, nil
}
func (p *fakeProber) OpenSubHash(_ string) (any, error) {
	if p.probeErr != nil {
		return nil, p.probeErr
	}
	return "abc123", nil
}
func (p *fakeProber) SubtitlesTracks(_ string) (any, error) {
	if p.probeErr != nil {
		return nil, p.probeErr
	}
	return []any{}, nil
}
func (p *fakeProber) WriteSubtitles(_ io.Writer, _, _ string, _ int) error { return p.writeSubsErr }
func (p *fakeProber) StartHLS(_, _ string, opts types.HLSSessionOptions) (string, error) {
	p.lastHLSOpts = opts
	return "", p.startHLSErr
}
func (p *fakeProber) HLSFile(_ context.Context, _, _ string) (string, string, error) {
	return "", "", p.hlsFileErr
}
func (p *fakeProber) DeleteHLS(id string) error {
	p.deleted = append(p.deleted, id)
	if p.deleteHLS != nil {
		return p.deleteHLS(id)
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Test helpers
// ─────────────────────────────────────────────────────────────────────────────

// testIH is a valid 40-hex info hash used across streaming and stats tests.
const testIH = "aabbccddeeff00112233445566778899aabbccdd"

// testEngine returns a fakeEngine with known 32-byte data and one .mkv file.
func testEngine() *fakeEngine {
	return &fakeEngine{
		ih:   testIH,
		data: []byte("hello world stream test data xyz"),
		files: []types.FileInfo{
			{Name: "video.mkv", Path: "video.mkv", Length: 32, Offset: 0},
		},
	}
}

// newHandler builds an http.Handler with fake dependencies.
func newHandler(t *testing.T, engines ...*fakeEngine) http.Handler {
	t.Helper()
	cfg := types.Config{
		HTTPPort:           11470,
		WebUI:              "https://web.stremio.com/",
		EnableDLNA:         false,
		CreateMetadataWait: 90 * time.Second,
		AppPath:            t.TempDir(), // settings, recordings and cert files stay out of the package dir
	}
	return New(newFakeEM(engines...), &fakeSS{}, &fakeProber{}, cfg)
}

// newHandlerWithCfg builds an http.Handler like newHandler but lets the test
// customize cfg (e.g. AllowedOrigins/AllowAllOrigins for origin-allowlist
// tests, SEC-1 / Contract 2).
func newHandlerWithCfg(t *testing.T, mutate func(*types.Config), engines ...*fakeEngine) http.Handler {
	t.Helper()
	cfg := types.Config{
		HTTPPort:           11470,
		WebUI:              "https://web.stremio.com/",
		EnableDLNA:         false,
		CreateMetadataWait: 90 * time.Second,
		AppPath:            t.TempDir(), // settings, recordings and cert files stay out of the package dir
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return New(newFakeEM(engines...), &fakeSS{}, &fakeProber{}, cfg)
}

// decodeJSON unmarshals a JSON object from data; fails t on error.
func decodeJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("JSON decode failed: %v\nbody: %s", err, data)
	}
	return m
}

// decodeJSONArray unmarshals a JSON array; fails t on error.
func decodeJSONArray(t *testing.T, data []byte) []any {
	t.Helper()
	var arr []any
	if err := json.Unmarshal(data, &arr); err != nil {
		t.Fatalf("JSON array decode failed: %v\nbody: %s", err, data)
	}
	return arr
}

// serve is a test convenience: fires a request at the handler and returns the recorder.
func serve(t *testing.T, h http.Handler, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// ═════════════════════════════════════════════════════════════════════════════
// 1. HELPER UNIT TESTS (unexported functions)
// ═════════════════════════════════════════════════════════════════════════════

// ─── parseRange ───────────────────────────────────────────────────────────────

func TestParseRange(t *testing.T) {
	const length = int64(1000)
	tests := []struct {
		name   string
		h      string
		length int64
		wStart int64
		wEnd   int64
		wOk    bool
		wUnsat bool
	}{
		{"no header → 200", "", length, 0, 0, false, false},
		{"valid bytes=0-99 → 206", "bytes=0-99", length, 0, 99, true, false},
		{"suffix bytes=-100 → 206", "bytes=-100", length, 900, 999, true, false},
		{"open bytes=500- → 206", "bytes=500-", length, 500, 999, true, false},
		{"start>=len → 416", "bytes=1000-", length, 0, 0, false, true},
		{"zero-length file suffix → 416", "bytes=-100", 0, 0, 0, false, true},
		{"malformed startS → 200", "bytes=abc-def", length, 0, 0, false, false},
		{"wrong prefix → 200", "not-bytes=0-99", length, 0, 0, false, false},
		{"end<start → 416", "bytes=50-10", length, 0, 0, false, true},
		{"end clamped to len-1", "bytes=0-9999", length, 0, 999, true, false},
		{"suffix clamp: bytes=-100 file=50", "bytes=-100", 50, 0, 49, true, false},
		{"suffix zero → 416", "bytes=-0", length, 0, 0, false, true},
		{"multi-range uses first only", "bytes=0-9,20-29", length, 0, 9, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, e, ok, unsat := parseRange(tc.h, tc.length)
			if s != tc.wStart || e != tc.wEnd || ok != tc.wOk || unsat != tc.wUnsat {
				t.Errorf("parseRange(%q, %d) = (%d, %d, %v, %v); want (%d, %d, %v, %v)",
					tc.h, tc.length, s, e, ok, unsat, tc.wStart, tc.wEnd, tc.wOk, tc.wUnsat)
			}
		})
	}
}

// ─── resolveIndex ─────────────────────────────────────────────────────────────

func TestResolveIndex(t *testing.T) {
	files := []types.FileInfo{
		{Name: "a.mkv"},
		{Name: "b.mp4"},
	}
	eng := &fakeEngine{files: files}

	tests := []struct {
		name    string
		seg     string
		mustInc []*regexp.Regexp
		want    int
	}{
		{"numeric seg 0", "0", nil, 0},
		{"numeric seg 1", "1", nil, 1},
		{"seg -1 → GuessFileIdx()==0", "-1", nil, 0},
		{"fileMustInclude match b.mp4 → idx 1", "0", []*regexp.Regexp{regexp.MustCompile(`b\.mp4`)}, 1},
		{"fileMustInclude no match → -1", "0", []*regexp.Regexp{regexp.MustCompile(`nomatch`)}, -1},
		{"filename match a.mkv → 0", "a.mkv", nil, 0},
		{"filename match b.mp4 → 1", "b.mp4", nil, 1},
		{"unknown filename → GuessFileIdx()==0", "unknown.avi", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveIndex(tc.seg, files, tc.mustInc, eng)
			if got != tc.want {
				t.Errorf("resolveIndex(%q) = %d; want %d", tc.seg, got, tc.want)
			}
		})
	}
}

// ─── compileMustInclude ───────────────────────────────────────────────────────

func TestCompileMustInclude(t *testing.T) {
	t.Run("nil input → nil output", func(t *testing.T) {
		if out := compileMustInclude(nil); out != nil {
			t.Errorf("want nil, got %v", out)
		}
	})
	t.Run("empty string skipped", func(t *testing.T) {
		if out := compileMustInclude([]string{""}); len(out) != 0 {
			t.Errorf("want 0 regexps, got %d", len(out))
		}
	})
	t.Run("literal string", func(t *testing.T) {
		out := compileMustInclude([]string{"movie.mkv"})
		if len(out) != 1 {
			t.Fatalf("want 1 regexp, got %d", len(out))
		}
		if !out[0].MatchString("movie.mkv") {
			t.Error("literal regexp must match exact string")
		}
	})
	t.Run("regex /pattern/i case-insensitive", func(t *testing.T) {
		out := compileMustInclude([]string{"/movie/i"})
		if len(out) != 1 {
			t.Fatalf("want 1 regexp, got %d", len(out))
		}
		if !out[0].MatchString("MOVIE.mkv") {
			t.Error("case-insensitive regex should match MOVIE.mkv")
		}
	})
}

// ─── mimeByName ───────────────────────────────────────────────────────────────

func TestMimeByName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"test.mkv", "video/x-matroska"},
		{"test.mp4", "video/mp4"},
		{"test.avi", "video/x-msvideo"},
		{"test.webm", "video/webm"},
		{"test.ts", "video/mp2t"},
		{"test.xyz", "application/octet-stream"},
		{"noextension", "application/octet-stream"},
		{"TEST.MKV", "video/x-matroska"},   // case-insensitive
		{"path/to/video.mp4", "video/mp4"}, // path with dir separator
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := mimeByName(tc.in)
			if got != tc.want {
				t.Errorf("mimeByName(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ─── contentDisposition + rfc5987Encode ──────────────────────────────────────

func TestRfc5987Encode(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"simple", "simple"},
		{"abc123", "abc123"},
		{"file-name.mkv", "file-name.mkv"}, // - and . are attr-chars
		{"hello world", "hello%20world"},   // space → %20
		{"tëst", "t%C3%ABst"},              // ë (U+00EB) → %C3%AB in UTF-8
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := rfc5987Encode(tc.in)
			if got != tc.want {
				t.Errorf("rfc5987Encode(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestContentDisposition(t *testing.T) {
	t.Run("ASCII filename", func(t *testing.T) {
		got := contentDisposition("simple.mkv")
		if !strings.Contains(got, `filename="simple.mkv"`) {
			t.Errorf("want filename=\"simple.mkv\" in %q", got)
		}
		if !strings.Contains(got, "filename*=UTF-8''simple.mkv") {
			t.Errorf("want filename*=UTF-8''simple.mkv in %q", got)
		}
	})
	t.Run("filename with spaces", func(t *testing.T) {
		got := contentDisposition("my file.mkv")
		// space (0x20) is within the printable ASCII range → kept in filename=
		if !strings.Contains(got, `filename="my file.mkv"`) {
			t.Errorf("want filename=\"my file.mkv\" in %q", got)
		}
		// space must be percent-encoded in filename*=
		if !strings.Contains(got, "my%20file.mkv") {
			t.Errorf("want my%%20file.mkv in filename*= of %q", got)
		}
	})
	t.Run("non-ASCII filename", func(t *testing.T) {
		// ë = U+00EB; multi-byte in UTF-8 → must be '_' in ASCII fallback
		got := contentDisposition("tëst.mkv")
		if !strings.Contains(got, `filename="t_st.mkv"`) {
			t.Errorf("want filename=\"t_st.mkv\" in %q", got)
		}
		if !strings.Contains(got, "filename*=UTF-8''") {
			t.Errorf("want filename*=UTF-8'' prefix in %q", got)
		}
		// ë (U+00EB) = 0xC3 0xAB in UTF-8 → %C3%AB
		if !strings.Contains(got, "%C3%AB") {
			t.Errorf("want %%C3%%AB percent-encoding in %q", got)
		}
	})
	t.Run("quotes and backslash replaced", func(t *testing.T) {
		got := contentDisposition(`file"with\quotes.mkv`)
		// " and \ replaced by _ in ASCII filename=
		if strings.Contains(got, `filename="file"`) {
			t.Errorf("filename= should not contain unescaped quote in %q", got)
		}
	})
}

// ─── archiveURLFromEntry ──────────────────────────────────────────────────────

func TestArchiveURLFromEntry(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"bare string", `"http://example.com/file.zip"`, "http://example.com/file.zip"},
		{"tuple [url, bytes]", `["http://example.com/file.zip", 12345]`, "http://example.com/file.zip"},
		{"tuple [url]", `["http://example.com/file.zip"]`, "http://example.com/file.zip"},
		{"object {url}", `{"url":"http://example.com/file.zip"}`, "http://example.com/file.zip"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := archiveURLFromEntry(json.RawMessage(tc.raw))
			if got != tc.want {
				t.Errorf("archiveURLFromEntry(%s) = %q; want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// ─── archiveParsePayload + (*archivePayload).source() ────────────────────────

func makePostRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/zip/create", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestArchiveParsePayload(t *testing.T) {
	const wantURL = "http://example.com/file.zip"
	tests := []struct {
		name string
		body string
	}{
		{"urls tuple [[url,bytes]]", `{"urls":[["http://example.com/file.zip",1024]]}`},
		{"urls tuple [[url]]", `{"urls":[["http://example.com/file.zip"]]}`},
		{"urls bare string", `{"urls":["http://example.com/file.zip"]}`},
		{"urls object {url}", `{"urls":[{"url":"http://example.com/file.zip"}]}`},
		{"legacy url field", `{"url":"http://example.com/file.zip"}`},
		{"legacy from field", `{"from":"http://example.com/file.zip"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := makePostRequest(t, tc.body)
			p, err := archiveParsePayload(req)
			if err != nil {
				t.Fatalf("archiveParsePayload error: %v", err)
			}
			if got := p.source(); got != wantURL {
				t.Errorf("source() = %q; want %q", got, wantURL)
			}
		})
	}

	t.Run("array form [{url:…}] → first element", func(t *testing.T) {
		req := makePostRequest(t, `[{"url":"http://example.com/file.zip"}]`)
		p, err := archiveParsePayload(req)
		if err != nil {
			t.Fatalf("archiveParsePayload error: %v", err)
		}
		if got := p.source(); got != wantURL {
			t.Errorf("source() = %q; want %q", got, wantURL)
		}
	})

	t.Run("empty body → error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/zip/create", nil)
		_, err := archiveParsePayload(req)
		if err == nil {
			t.Fatal("expected error for empty body, got nil")
		}
	})
}

// ─── parseNzbServers + nzbServerFromURL ──────────────────────────────────────

func TestNzbServerFromURL(t *testing.T) {
	t.Run("nntps with credentials and explicit port", func(t *testing.T) {
		cfg, err := nzbServerFromURL("nntps://user:pass@news.example.com:563")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Host != "news.example.com" {
			t.Errorf("Host = %q; want news.example.com", cfg.Host)
		}
		if !cfg.SSL {
			t.Error("SSL should be true for nntps://")
		}
		if cfg.Port != 563 {
			t.Errorf("Port = %d; want 563", cfg.Port)
		}
		if cfg.User != "user" {
			t.Errorf("User = %q; want user", cfg.User)
		}
		if cfg.Pass != "pass" {
			t.Errorf("Pass = %q; want pass", cfg.Pass)
		}
	})

	t.Run("plain nntp no credentials no port", func(t *testing.T) {
		cfg, err := nzbServerFromURL("nntp://news.example.com")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Host != "news.example.com" {
			t.Errorf("Host = %q; want news.example.com", cfg.Host)
		}
		if cfg.SSL {
			t.Error("SSL should be false for nntp://")
		}
		if cfg.Port != 0 {
			t.Errorf("Port = %d; want 0 (unset)", cfg.Port)
		}
		if cfg.User != "" || cfg.Pass != "" {
			t.Errorf("unexpected credentials: user=%q pass=%q", cfg.User, cfg.Pass)
		}
	})

	t.Run("snews scheme → SSL", func(t *testing.T) {
		cfg, err := nzbServerFromURL("snews://host:563")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.SSL {
			t.Error("SSL should be true for snews://")
		}
	})

	t.Run("URL with no host → error", func(t *testing.T) {
		_, err := nzbServerFromURL("nntp://")
		if err == nil {
			t.Fatal("expected error for missing host, got nil")
		}
	})
}

func TestParseNzbServers(t *testing.T) {
	t.Run("canonical URL strings", func(t *testing.T) {
		raw := json.RawMessage(`["nntps://user:pass@news.host:563"]`)
		servers, err := parseNzbServers(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(servers) != 1 {
			t.Fatalf("want 1 server, got %d", len(servers))
		}
		s := servers[0]
		if s.Host != "news.host" || !s.SSL || s.Port != 563 || s.User != "user" || s.Pass != "pass" {
			t.Errorf("unexpected server: %+v", s)
		}
	})

	t.Run("plain nntp URL", func(t *testing.T) {
		raw := json.RawMessage(`["nntp://news.host"]`)
		servers, err := parseNzbServers(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(servers) != 1 || servers[0].SSL {
			t.Errorf("unexpected: %+v", servers)
		}
	})

	t.Run("legacy object array", func(t *testing.T) {
		raw := json.RawMessage(`[{"host":"news.host","port":563,"ssl":true,"connections":10}]`)
		servers, err := parseNzbServers(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(servers) != 1 {
			t.Fatalf("want 1 server, got %d", len(servers))
		}
		s := servers[0]
		if s.Host != "news.host" || s.Port != 563 || !s.SSL || s.Connections != 10 {
			t.Errorf("unexpected server: %+v", s)
		}
	})

	t.Run("empty raw message → error", func(t *testing.T) {
		_, err := parseNzbServers(json.RawMessage(nil))
		if err == nil {
			t.Fatal("expected error for nil raw, got nil")
		}
	})
}

// ─── ftpExtractRangeStart ────────────────────────────────────────────────────

func TestFtpExtractRangeStart(t *testing.T) {
	tests := []struct {
		name   string
		h      string
		wStart int64
		wOk    bool
	}{
		{"explicit bytes=100-", "bytes=100-", 100, true},
		{"explicit bytes=100-200", "bytes=100-200", 100, true},
		{"suffix bytes=-100 → not ok", "bytes=-100", 0, false},
		{"no header → not ok", "", 0, false},
		{"wrong prefix → not ok", "not-bytes=100-", 0, false},
		{"multi-range uses first", "bytes=100-200,300-400", 100, true},
		{"zero start ok", "bytes=0-", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			start, ok := ftpExtractRangeStart(tc.h)
			if start != tc.wStart || ok != tc.wOk {
				t.Errorf("ftpExtractRangeStart(%q) = (%d, %v); want (%d, %v)",
					tc.h, start, ok, tc.wStart, tc.wOk)
			}
		})
	}
}

// ─── statusFor / errStr ──────────────────────────────────────────────────────

func TestStatusFor(t *testing.T) {
	if got := statusFor(nil); got != http.StatusOK {
		t.Errorf("statusFor(nil) = %d; want 200", got)
	}
	if got := statusFor(errors.New("boom")); got != http.StatusInternalServerError {
		t.Errorf("statusFor(err) = %d; want 500", got)
	}
}

func TestErrStr(t *testing.T) {
	if got := errStr(nil); got != nil {
		t.Errorf("errStr(nil) = %v; want nil", got)
	}
	if got, ok := errStr(errors.New("boom")).(string); !ok || got != "boom" {
		t.Errorf(`errStr(err) = %v; want "boom"`, errStr(errors.New("boom")))
	}
}

// ─── availableInterfaces ─────────────────────────────────────────────────────

func TestAvailableInterfaces(t *testing.T) {
	result := availableInterfaces()
	if result == nil {
		t.Fatal("availableInterfaces() returned nil; want non-nil slice")
	}
	for _, addr := range result {
		if net.ParseIP(addr) == nil {
			t.Errorf("availableInterfaces() returned non-IP value %q", addr)
		}
	}
}

// ─── reshapeProbeResult ───────────────────────────────────────────────────────

func TestReshapeProbeResult(t *testing.T) {
	t.Run("nil input → zero value", func(t *testing.T) {
		out := reshapeProbeResult(nil)
		if out["duration"] != float64(0) {
			t.Errorf("duration = %v; want 0", out["duration"])
		}
		if streams, ok := out["streams"].([]any); !ok || len(streams) != 0 {
			t.Errorf("streams = %v; want empty slice", out["streams"])
		}
	})

	t.Run("format duration + format_name extracted", func(t *testing.T) {
		raw := map[string]interface{}{
			"format": map[string]interface{}{
				"duration":    "120.5",
				"format_name": "matroska",
			},
			"streams": []interface{}{},
		}
		out := reshapeProbeResult(raw)
		if got := out["duration"].(float64); got != 120.5 {
			t.Errorf("duration = %v; want 120.5", got)
		}
		fmtOut, ok := out["format"].(map[string]any)
		if !ok {
			t.Fatalf("format not a map: %T", out["format"])
		}
		if fmtOut["name"] != "matroska" {
			t.Errorf("format.name = %q; want matroska", fmtOut["name"])
		}
	})

	t.Run("streams reshaped with index/track/codec/fps/channels", func(t *testing.T) {
		raw := map[string]interface{}{
			"format": map[string]interface{}{"duration": float64(60)},
			"streams": []interface{}{
				map[string]interface{}{
					"index":        float64(0),
					"codec_type":   "video",
					"codec_name":   "h264",
					"r_frame_rate": "24/1",
					"width":        float64(1920),
					"height":       float64(1080),
				},
				map[string]interface{}{
					"index":      float64(1),
					"codec_type": "audio",
					"codec_name": "aac",
					"channels":   float64(2),
					"bit_rate":   "128000",
				},
			},
		}
		out := reshapeProbeResult(raw)
		streams, ok := out["streams"].([]any)
		if !ok || len(streams) != 2 {
			t.Fatalf("streams: got %T %v", out["streams"], out["streams"])
		}
		v := streams[0].(map[string]any)
		if v["track"] != "video" {
			t.Errorf("stream[0].track = %v; want video", v["track"])
		}
		if v["codec"] != "h264" {
			t.Errorf("stream[0].codec = %v; want h264", v["codec"])
		}
		if v["fps"].(float64) != 24.0 {
			t.Errorf("stream[0].fps = %v; want 24.0", v["fps"])
		}
		if v["width"].(int) != 1920 {
			t.Errorf("stream[0].width = %v; want 1920", v["width"])
		}
		a := streams[1].(map[string]any)
		if a["channels"].(int) != 2 {
			t.Errorf("stream[1].channels = %v; want 2", a["channels"])
		}
		if a["bitrate"].(int64) != 128000 {
			t.Errorf("stream[1].bitrate = %v; want 128000", a["bitrate"])
		}
	})
}

// ─── parseRational ───────────────────────────────────────────────────────────

func TestParseRational(t *testing.T) {
	tests := []struct {
		in   string
		want float64
	}{
		{"24/1", 24.0},
		{"30000/1001", 30000.0 / 1001.0},
		{"0/0", 0},
		{"", 0},
		{"25", 25.0},
		{"bad", 0},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := parseRational(tc.in)
			// Use a small epsilon for floating point.
			if diff := got - tc.want; diff > 1e-9 || diff < -1e-9 {
				t.Errorf("parseRational(%q) = %v; want %v", tc.in, got, tc.want)
			}
		})
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// 2. HANDLER INTEGRATION TESTS
// ═════════════════════════════════════════════════════════════════════════════

// ─── CORS / preflight ────────────────────────────────────────────────────────

func TestHandlerCORS_Preflight(t *testing.T) {
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodOptions, "/heartbeat", nil)
	req.Header.Set("Access-Control-Request-Headers", "Range, Content-Type")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("OPTIONS status = %d; want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q; want *", got)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("preflight response must have empty body, got %d bytes", rec.Body.Len())
	}
}

func TestHandlerCORS_OnEveryResponse(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/heartbeat", nil)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin missing on normal GET: %q", got)
	}
}

// ─── GET /heartbeat ──────────────────────────────────────────────────────────

func TestHandlerHeartbeat(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/heartbeat", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if s, ok := m["success"].(bool); !ok || !s {
		t.Errorf("heartbeat = %v; want {\"success\":true}", m)
	}
}

// ─── GET /network-info ────────────────────────────────────────────────────────

func TestHandlerNetworkInfo(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/network-info", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	ifaces, ok := m["availableInterfaces"]
	if !ok {
		t.Fatalf("response missing availableInterfaces key: %v", m)
	}
	if _, ok := ifaces.([]any); !ok {
		t.Errorf("availableInterfaces not an array: %T", ifaces)
	}
}

// ─── GET /device-info ────────────────────────────────────────────────────────

func TestHandlerDeviceInfo(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/device-info", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m["availableHardwareAccelerations"]; !ok {
		t.Errorf("response missing availableHardwareAccelerations: %v", m)
	}
}

// ─── GET /settings ────────────────────────────────────────────────────────────

func TestHandlerGetSettings(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/settings", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	for _, key := range []string{"options", "values", "baseUrl"} {
		if _, ok := m[key]; !ok {
			t.Errorf("settings response missing key %q; got keys: %v", key, m)
		}
	}
	if _, ok := m["options"].([]any); !ok {
		t.Errorf("options not a JSON array: %T", m["options"])
	}
	if _, ok := m["values"].(map[string]any); !ok {
		t.Errorf("values not a JSON object: %T", m["values"])
	}
	if _, ok := m["baseUrl"].(string); !ok {
		t.Errorf("baseUrl not a string: %T", m["baseUrl"])
	}
}

// ─── POST /settings ───────────────────────────────────────────────────────────

func TestHandlerPostSettings(t *testing.T) {
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(`{"cacheSize":2048}`))
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if s, ok := m["success"].(bool); !ok || !s {
		t.Errorf("POST /settings response = %v; want {\"success\":true}", m)
	}
}

// ─── Origin allowlist (SEC-1 / Contract 2) ──────────────────────────────────

func TestOriginDisallowed403(t *testing.T) {
	h := newHandler(t)
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/heartbeat", nil)
			req.Header.Set("Origin", "https://evil.example.com")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d; want 403", rec.Code)
			}
			m := decodeJSON(t, rec.Body.Bytes())
			if m["error"] != "origin not allowed" {
				t.Errorf(`error = %v; want "origin not allowed"`, m["error"])
			}
			if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Errorf("ACAO should not be set on a rejected origin, got %q", got)
			}
		})
	}
}

func TestOriginDisallowedNullOrigin(t *testing.T) {
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req.Header.Set("Origin", "null")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d; want 403 for literal null origin", rec.Code)
	}
}

func TestOriginAllowedWebStremioEchoedWithVary(t *testing.T) {
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req.Header.Set("Origin", "https://web.stremio.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://web.stremio.com" {
		t.Errorf("ACAO = %q; want echoed origin", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Errorf("Vary = %q; want Origin", got)
	}
}

func TestOriginNoHeaderKeepsWildcardACAO(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/heartbeat", nil)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q; want * when no Origin header is sent", got)
	}
	if got := rec.Header().Get("Vary"); got != "" {
		t.Errorf("Vary should not be set when no Origin header is sent, got %q", got)
	}
}

func TestOriginAllowAllLegacy(t *testing.T) {
	h := newHandlerWithCfg(t, func(c *types.Config) { c.AllowAllOrigins = true })
	req := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 (legacy allow-all)", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("ACAO = %q; want * in legacy mode", got)
	}
	if got := rec.Header().Get("Vary"); got != "" {
		t.Errorf("Vary should not be set in legacy mode, got %q", got)
	}
}

func TestOriginConfiguredExtraAllowed(t *testing.T) {
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.AllowedOrigins = []string{"https://extra.example.com"}
	})
	req := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req.Header.Set("Origin", "https://extra.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 for configured extra origin", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://extra.example.com" {
		t.Errorf("ACAO = %q; want echoed configured origin", got)
	}
}

func TestOriginConfiguredWildcardAllowed(t *testing.T) {
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.AllowedOrigins = []string{"*.example.com"}
	})
	req := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req.Header.Set("Origin", "https://sub.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 for wildcard-matched origin", rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req2.Header.Set("Origin", "https://example.com")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Errorf("bare domain should not match *.example.com wildcard, status = %d", rec2.Code)
	}
}

func TestOriginOwnInterfaceIPAllowed(t *testing.T) {
	ifaces := availableInterfaces()
	if len(ifaces) == 0 {
		t.Skip("no non-loopback interfaces available on this host")
	}
	ip := ifaces[0]
	origin := "http://" + ip + ":11470"
	if strings.Contains(ip, ":") {
		origin = "http://[" + ip + "]:11470"
	}
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodGet, "/heartbeat", nil)
	req.Header.Set("Origin", origin)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 for own-IP origin %q", rec.Code, origin)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("ACAO = %q; want echoed own-IP origin %q", got, origin)
	}
}

func TestPreflightPrivateNetworkHeader(t *testing.T) {
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodOptions, "/heartbeat", nil)
	req.Header.Set("Origin", "https://web.stremio.com")
	req.Header.Set("Access-Control-Request-Private-Network", "true")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Private-Network"); got != "true" {
		t.Errorf("Access-Control-Allow-Private-Network = %q; want true", got)
	}
}

func TestPreflightNoPrivateNetworkHeaderWithoutRequest(t *testing.T) {
	h := newHandler(t)
	req := httptest.NewRequest(http.MethodOptions, "/heartbeat", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Private-Network"); got != "" {
		t.Errorf("Access-Control-Allow-Private-Network should be unset without a request, got %q", got)
	}
}

// ─── certReload concurrency (API-1) ─────────────────────────────────────────

func TestCertReloadConcurrency(t *testing.T) {
	s := &server{}
	var calls int64
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.SetCertReloadHook(func() { atomic.AddInt64(&calls, 1) })
		}()
		go func() {
			defer wg.Done()
			if p := s.certReload.Load(); p != nil {
				(*p)()
			}
		}()
	}
	wg.Wait()
}

// ─── POST /settings validation (API-2) ──────────────────────────────────────

func TestHandlerPostSettingsValidation(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantErrSub string
	}{
		{"malformed json", `{"cacheSize":`, http.StatusBadRequest, "invalid JSON"},
		{"non-object json", `"cacheSize"`, http.StatusBadRequest, "invalid JSON"},
		{"empty body", ``, http.StatusOK, ""},
		{"cacheSize null ok", `{"cacheSize":null}`, http.StatusOK, ""},
		{"cacheSize number ok", `{"cacheSize":2048}`, http.StatusOK, ""},
		{"cacheSize negative rejected", `{"cacheSize":-1}`, http.StatusBadRequest, "cacheSize"},
		{"cacheSize string rejected", `{"cacheSize":"2GB"}`, http.StatusBadRequest, "cacheSize"},
		{"btMaxConnections string rejected", `{"btMaxConnections":"55"}`, http.StatusBadRequest, "btMaxConnections"},
		{"btMaxConnections negative rejected", `{"btMaxConnections":-5}`, http.StatusBadRequest, "btMaxConnections"},
		{"btMaxConnections ok", `{"btMaxConnections":100}`, http.StatusOK, ""},
		{"transcodeHorsepower number ok", `{"transcodeHorsepower":0.5}`, http.StatusOK, ""},
		{"transcodeHorsepower string rejected", `{"transcodeHorsepower":"high"}`, http.StatusBadRequest, "transcodeHorsepower"},
		{"localAddonEnabled bool ok", `{"localAddonEnabled":true}`, http.StatusOK, ""},
		{"localAddonEnabled non-bool rejected", `{"localAddonEnabled":"yes"}`, http.StatusBadRequest, "localAddonEnabled"},
		{"remoteHttps string ok", `{"remoteHttps":"1.2.3.4"}`, http.StatusOK, ""},
		{"remoteHttps non-string rejected", `{"remoteHttps":5}`, http.StatusBadRequest, "remoteHttps"},
		{"transcodeProfile null ok", `{"transcodeProfile":null}`, http.StatusOK, ""},
		{"transcodeProfile string ok", `{"transcodeProfile":"h264"}`, http.StatusOK, ""},
		{"transcodeProfile non-string rejected", `{"transcodeProfile":5}`, http.StatusBadRequest, "transcodeProfile"},
		{"unknown key still merged", `{"someFutureKey":"x"}`, http.StatusOK, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHandler(t)
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(http.MethodPost, "/settings", body)
			req.RemoteAddr = "127.0.0.1:5555"
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d; want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			m := decodeJSON(t, rec.Body.Bytes())
			switch {
			case tc.wantStatus == http.StatusOK:
				if s, ok := m["success"].(bool); !ok || !s {
					t.Errorf("response = %v; want success:true", m)
				}
			case tc.wantErrSub != "":
				errMsg, _ := m["error"].(string)
				if !strings.Contains(errMsg, tc.wantErrSub) {
					t.Errorf("error = %q; want substring %q", errMsg, tc.wantErrSub)
				}
			}
		})
	}
}

func TestHandlerPostSettingsDropsReadOnlyKeys(t *testing.T) {
	ss := &fakeSS{}
	cfg := types.Config{HTTPPort: 11470}
	h := New(newFakeEM(), ss, &fakeProber{}, cfg)
	body := `{"appPath":"/evil","cacheRoot":"/evil","serverVersion":"9.9.9","cacheSize":1024}`
	req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:5555"
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200", rec.Code)
	}
	for _, key := range []string{"appPath", "cacheRoot", "serverVersion"} {
		if v := ss.Get(key); v != nil {
			t.Errorf("%s should have been dropped from the patch, got %v", key, v)
		}
	}
	if v := ss.Get("cacheSize"); v != 1024.0 {
		t.Errorf("cacheSize = %v; want 1024", v)
	}
}

// ─── GET /list ────────────────────────────────────────────────────────────────

func TestHandlerList(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/list", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	arr := decodeJSONArray(t, rec.Body.Bytes())
	found := false
	for _, v := range arr {
		if s, ok := v.(string); ok && s == testIH {
			found = true
		}
	}
	if !found {
		t.Errorf("/list = %v; must contain %q", arr, testIH)
	}
}

func TestHandlerList_Empty(t *testing.T) {
	h := newHandler(t) // no engines
	rec := serve(t, h, http.MethodGet, "/list", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	arr := decodeJSONArray(t, rec.Body.Bytes())
	if len(arr) != 0 {
		t.Errorf("/list with no engines = %v; want []", arr)
	}
}

// ─── GET /{ih}/stats.json (torrent-level) ─────────────────────────────────────

func TestHandlerTorrentStats_RequiredKeys(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/stats.json", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())

	required := []string{
		"infoHash", "name", "peers", "unchoked", "queued", "unique",
		"connectionTries", "swarmPaused", "swarmConnections", "swarmSize",
		"selections", "wires", "files", "downloaded", "uploaded",
		"downloadSpeed", "uploadSpeed", "sources", "opts", "peerSearchRunning",
	}
	for _, k := range required {
		if _, ok := m[k]; !ok {
			t.Errorf("torrent stats missing required key %q", k)
		}
	}

	// Per-file extras must NOT appear at torrent level (idx < 0 → omitempty omits nil pointers)
	for _, k := range []string{"streamLen", "streamName", "streamProgress"} {
		if _, ok := m[k]; ok {
			t.Errorf("torrent stats must NOT contain %q (only at file level)", k)
		}
	}
}

func TestHandlerTorrentStats_UnknownIH(t *testing.T) {
	h := newHandler(t) // no engines
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/stats.json", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	// engine not found → writeStats returns null
	if strings.TrimSpace(rec.Body.String()) != "null" {
		t.Errorf("unknown-IH stats = %s; want null", rec.Body.String())
	}
}

// ─── GET /{ih}/0/stats.json (file-level) ─────────────────────────────────────

func TestHandlerFileStats_StreamFields(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/0/stats.json", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())

	for _, k := range []string{"streamLen", "streamName", "streamProgress"} {
		if _, ok := m[k]; !ok {
			t.Errorf("file-level stats missing required key %q", k)
		}
	}
}

// ─── GET /get-https missing params → 400 ─────────────────────────────────────

func TestHandlerGetHTTPS_MissingIPAddress(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/get-https", nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (missing ipAddress)", rec.Code)
	}
}

func TestHandlerGetHTTPS_MissingAuthKey(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/get-https?ipAddress=1.2.3.4", nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (missing authKey)", rec.Code)
	}
}

// ─── GET /casting (DLNA disabled) ────────────────────────────────────────────

func TestHandlerCasting_DLNADisabled_EmptyList(t *testing.T) {
	h := newHandler(t) // EnableDLNA=false by default in newHandler
	rec := serve(t, h, http.MethodGet, "/casting", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	arr := decodeJSONArray(t, rec.Body.Bytes())
	if len(arr) != 0 {
		t.Errorf("/casting (DLNA off) = %v; want []", arr)
	}
}

func TestHandlerCasting_DLNADisabled_DeviceRoute404(t *testing.T) {
	h := newHandler(t) // EnableDLNA=false
	rec := serve(t, h, http.MethodGet, "/casting/somedeviceid", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404 (DLNA disabled, sub-route)", rec.Code)
	}
}

// ─── Unknown route → 404 ─────────────────────────────────────────────────────

func TestHandlerUnknownRoute(t *testing.T) {
	h := newHandler(t)
	for _, path := range []string{
		"/totally-unknown",
		"/zzz/not/a/route",
	} {
		t.Run(path, func(t *testing.T) {
			rec := serve(t, h, http.MethodGet, path, nil)
			if rec.Code != http.StatusNotFound {
				t.Errorf("status = %d; want 404 for %s", rec.Code, path)
			}
		})
	}
}

// ─── GET /nzb/create → 501 ───────────────────────────────────────────────────

func TestHandlerNZBCreate_GetReturns501(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/nzb/create", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("GET /nzb/create status = %d; want 501", rec.Code)
	}
}

// ─── GET /ftp/create → 501 ───────────────────────────────────────────────────

func TestHandlerFTPCreate_GetReturns501(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/ftp/create", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("GET /ftp/create status = %d; want 501", rec.Code)
	}
}

func TestHandlerFTPStream_GetReturns501(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/ftp/stream", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Errorf("GET /ftp/stream status = %d; want 501", rec.Code)
	}
}

// ─── GET / → 307 redirect ────────────────────────────────────────────────────

func TestHandlerLanding_Redirect(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/", nil)
	if rec.Code != http.StatusTemporaryRedirect {
		t.Errorf("status = %d; want 307", rec.Code)
	}
	loc := rec.Header().Get("Location")
	if !strings.Contains(loc, "web.stremio.com") {
		t.Errorf("Location = %q; must contain web.stremio.com", loc)
	}
	if !strings.Contains(loc, "streamingServer=") {
		t.Errorf("Location = %q; must contain streamingServer=", loc)
	}
}

// TestHandlerLanding_PreservesWebUIQuery checks that the streamingServer
// parameter is merged into WEB_UI_LOCATION rather than appended blindly, so a
// configured query string survives.
func TestHandlerLanding_PreservesWebUIQuery(t *testing.T) {
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.WebUI = "https://web.stremio.com/?theme=dark"
	})
	rec := serve(t, h, http.MethodGet, "/", nil)
	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d; want 307", rec.Code)
	}
	loc, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("Location is not a URL: %v", err)
	}
	if got := loc.Query().Get("theme"); got != "dark" {
		t.Errorf("theme = %q; want the configured query preserved", got)
	}
	if got := loc.Query().Get("streamingServer"); got == "" {
		t.Error("streamingServer missing from the redirect")
	}
}

// ─── Streaming ───────────────────────────────────────────────────────────────

func TestHandlerStream_RangeRequest206(t *testing.T) {
	h := newHandler(t, testEngine())
	req := httptest.NewRequest(http.MethodGet, "/"+testIH+"/0", nil)
	req.Header.Set("Range", "bytes=0-3")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Errorf("status = %d; want 206", rec.Code)
	}
	cr := rec.Header().Get("Content-Range")
	if !strings.HasPrefix(cr, "bytes 0-3/") {
		t.Errorf("Content-Range = %q; want prefix bytes 0-3/", cr)
	}
	body := rec.Body.Bytes()
	if len(body) != 4 {
		t.Errorf("body length = %d; want 4 (Range bytes=0-3)", len(body))
	}
	// testEngine data = "hello world stream test data xyz"
	if string(body) != "hell" {
		t.Errorf("body = %q; want \"hell\"", body)
	}
}

func TestHandlerStream_FullGet200(t *testing.T) {
	eng := testEngine()
	h := newHandler(t, eng)
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/0", nil)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	// DLNA streaming headers required by stremio clients
	if got := rec.Header().Get("transferMode.dlna.org"); got != "Streaming" {
		t.Errorf("transferMode.dlna.org = %q; want Streaming", got)
	}
	if got := rec.Header().Get("contentFeatures.dlna.org"); !strings.Contains(got, "DLNA.ORG_OP=01") {
		t.Errorf("contentFeatures.dlna.org = %q; want DLNA.ORG_OP=01", got)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q; want bytes", got)
	}
	if rec.Body.String() != string(eng.data) {
		t.Errorf("body = %q; want %q", rec.Body.Bytes(), eng.data)
	}
}

func TestHandlerStream_HeadNoBody(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodHead, "/"+testIH+"/0", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("HEAD status = %d; want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("HEAD response must have no body; got %d bytes", rec.Body.Len())
	}
}

func TestHandlerStream_Unsatisfiable416(t *testing.T) {
	h := newHandler(t, testEngine())
	req := httptest.NewRequest(http.MethodGet, "/"+testIH+"/0", nil)
	req.Header.Set("Range", "bytes=99999-") // beyond 32-byte file
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("status = %d; want 416", rec.Code)
	}
}

func TestHandlerStream_MidRange(t *testing.T) {
	h := newHandler(t, testEngine())
	req := httptest.NewRequest(http.MethodGet, "/"+testIH+"/0", nil)
	req.Header.Set("Range", "bytes=6-10") // "world" in "hello world..."
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusPartialContent {
		t.Errorf("status = %d; want 206", rec.Code)
	}
	if rec.Body.String() != "world" {
		t.Errorf("body = %q; want \"world\"", rec.Body.Bytes())
	}
}

func TestHandlerStream_UnknownEngine(t *testing.T) {
	h := newHandler(t) // no engines registered
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/0", nil)
	// EnsureEngine returns error → 500
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d; want 500 for unknown engine", rec.Code)
	}
}

// ─── thumb.jpg cosmetic 404 ───────────────────────────────────────────────────

func TestHandlerThumbJPG_404(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/thumb.jpg", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("/thumb.jpg status = %d; want 404", rec.Code)
	}
}

// ─── GET /removeAll ───────────────────────────────────────────────────────────

func TestHandlerRemoveAll(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/removeAll", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
}
