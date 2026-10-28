// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — additional handler integration tests and helper unit tests
// to push coverage well above 70%.
package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/archive"
	"github.com/rivulet-kodi/rivulet-engine/internal/nzb"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// ═════════════════════════════════════════════════════════════════════════════
// HANDLER INTEGRATION TESTS — routes not covered by api_test.go
// ═════════════════════════════════════════════════════════════════════════════

// ─── GET /stats.json ─────────────────────────────────────────────────────────

func TestHandlerAllStats_WithEngines(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/stats.json", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m[testIH]; !ok {
		t.Errorf("/stats.json missing engine %q; keys: %v", testIH, m)
	}
}

func TestHandlerAllStats_SysParam(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/stats.json?sys=1", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m["sys"]; !ok {
		t.Errorf("/stats.json?sys=1 missing sys key; keys: %v", m)
	}
}

func TestHandlerAllStats_SysParam_ExactMatchOnly(t *testing.T) {
	// Regression: the gate must use an exact query-param check, not a
	// substring match on RawQuery — "nosys=1" and "xsys=1" both contain the
	// text "sys=1" but must NOT trigger sys augmentation.
	cases := []struct {
		query   string
		wantSys bool
	}{
		{"sys=1", true},
		{"nosys=1", false},
		{"xsys=1", false},
		{"sys=0", false},
	}
	for _, c := range cases {
		h := newHandler(t, testEngine())
		rec := serve(t, h, http.MethodGet, "/stats.json?"+c.query, nil)
		if rec.Code != http.StatusOK {
			t.Errorf("query %q: status = %d; want 200", c.query, rec.Code)
		}
		m := decodeJSON(t, rec.Body.Bytes())
		_, hasSys := m["sys"]
		if hasSys != c.wantSys {
			t.Errorf("query %q: sys key present = %v; want %v (keys: %v)", c.query, hasSys, c.wantSys, m)
		}
	}
}

func TestHandlerAllStats_Empty(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/stats.json", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if len(m) != 0 {
		t.Errorf("/stats.json with no engines = %v; want {}", m)
	}
}

// ─── GET /{ih}/remove ─────────────────────────────────────────────────────────

func TestHandlerRemove(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/remove", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if len(m) != 0 {
		t.Errorf("/remove response = %v; want {}", m)
	}
}

// ─── GET /{ih}/peers ─────────────────────────────────────────────────────────

func TestHandlerPeers_KnownEngine(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/peers", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	arr := decodeJSONArray(t, rec.Body.Bytes())
	// fakeEngine.Stats returns Wires: []Wire{} → empty peer list
	if arr == nil {
		t.Error("peers response must not be nil")
	}
}

func TestHandlerPeers_UnknownEngine(t *testing.T) {
	h := newHandler(t) // no engines
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/peers", nil)
	// GetEngine returns false → 404 with empty array
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404 for unknown engine", rec.Code)
	}
}

// ─── GET /{ih}/create ────────────────────────────────────────────────────────

func TestHandlerCreate_ReturnsStats(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/create", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m["infoHash"]; !ok {
		t.Errorf("create response missing infoHash; got %v", m)
	}
}

func TestHandlerCreate_UnknownEngine500(t *testing.T) {
	h := newHandler(t) // no engines → EnsureEngine returns error
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/create", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d; want 500 for missing engine", rec.Code)
	}
}

// ─── GET /probe ───────────────────────────────────────────────────────────────

func TestHandlerProbe_Returns200(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/probe?url=http://example.com/video.mkv", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	// reshapeProbeResult always returns these keys
	for _, k := range []string{"format", "duration", "streams"} {
		if _, ok := m[k]; !ok {
			t.Errorf("probe response missing key %q; got %v", k, m)
		}
	}
}

// ─── GET /hlsv2/probe ────────────────────────────────────────────────────────

func TestHandlerHLSProbe_Returns200(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/hlsv2/probe?mediaURL=http://example.com/video.mkv", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m["streams"]; !ok {
		t.Errorf("hlsv2/probe missing streams key; got %v", m)
	}
}

// ─── GET /hlsv2/* ────────────────────────────────────────────────────────────

func TestHandlerHLS_MasterPlaylist(t *testing.T) {
	h := newHandler(t)
	// StartHLS returns "" → empty body but correct content-type
	rec := serve(t, h, http.MethodGet, "/hlsv2/session123/master.m3u8?mediaURL=http://example.com", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "mpegurl") {
		t.Errorf("Content-Type = %q; want application/vnd.apple.mpegurl", ct)
	}
}

func TestHandlerHLS_TooFewSegments404(t *testing.T) {
	h := newHandler(t)
	// seg = ["hlsv2"] → len < 3 → 404
	rec := serve(t, h, http.MethodGet, "/hlsv2", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("/hlsv2 (no sub-path) status = %d; want 404", rec.Code)
	}
}

// ─── DELETE /hlsv2/{id} ──────────────────────────────────────────────────────

func newHandlerWithProber(t *testing.T, p *fakeProber) http.Handler {
	t.Helper()
	cfg := types.Config{
		HTTPPort:           11470,
		WebUI:              "https://web.stremio.com/",
		CreateMetadataWait: 90 * time.Second,
	}
	return New(newFakeEM(), &fakeSS{}, p, cfg)
}

func TestHandlerHLSDelete(t *testing.T) {
	cases := []struct {
		name        string
		path        string
		deleteErr   error
		wantCode    int
		wantDeleted []string // ids DeleteHLS must have been called with
	}{
		{"success", "/hlsv2/sess1", nil, http.StatusNoContent, []string{"sess1"}},
		{"trailing slash", "/hlsv2/sess1/", nil, http.StatusNoContent, []string{"sess1"}},
		{"unknown session", "/hlsv2/nope", fmt.Errorf("hls: %w nope", types.ErrHLSSessionNotFound), http.StatusNotFound, []string{"nope"}},
		{"invalid id", "/hlsv2/a..b", fmt.Errorf("hls: %w %q", types.ErrHLSInvalidSessionID, "a..b"), http.StatusBadRequest, []string{"a..b"}},
		{"other error", "/hlsv2/sess1", errors.New("disk on fire"), http.StatusInternalServerError, []string{"sess1"}},
		{"no id", "/hlsv2", nil, http.StatusNotFound, nil},
		{"file path is not deletable", "/hlsv2/sess1/seg0.ts", nil, http.StatusNotFound, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakeProber{deleteHLS: func(string) error { return c.deleteErr }}
			rec := serve(t, newHandlerWithProber(t, p), http.MethodDelete, c.path, nil)
			if rec.Code != c.wantCode {
				t.Errorf("DELETE %s status = %d; want %d (body %q)", c.path, rec.Code, c.wantCode, rec.Body.String())
			}
			if c.wantCode == http.StatusNoContent && rec.Body.Len() != 0 {
				t.Errorf("204 response must have an empty body, got %q", rec.Body.String())
			}
			if strings.Join(p.deleted, ",") != strings.Join(c.wantDeleted, ",") {
				t.Errorf("DeleteHLS called with %q; want %q", p.deleted, c.wantDeleted)
			}
		})
	}
}

func TestHandlerHLSDeleteDoesNotAffectGet(t *testing.T) {
	p := &fakeProber{}
	h := newHandlerWithProber(t, p)
	rec := serve(t, h, http.MethodGet, "/hlsv2/sess1/master.m3u8?mediaURL=http://example.com", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("GET master.m3u8 status = %d; want 200", rec.Code)
	}
	if len(p.deleted) != 0 {
		t.Errorf("GET must not call DeleteHLS, got %q", p.deleted)
	}
}

func TestHandlerHLSDeleteOriginGate(t *testing.T) {
	cases := []struct {
		name       string
		origin     string
		wantCode   int
		wantCalled bool
	}{
		{"no origin (native client/curl)", "", http.StatusNoContent, true},
		{"allowed origin", "https://web.stremio.com", http.StatusNoContent, true},
		{"disallowed origin", "https://evil.example.com", http.StatusForbidden, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakeProber{}
			req := httptest.NewRequest(http.MethodDelete, "/hlsv2/sess1", nil)
			if c.origin != "" {
				req.Header.Set("Origin", c.origin)
			}
			rec := httptest.NewRecorder()
			newHandlerWithProber(t, p).ServeHTTP(rec, req)
			if rec.Code != c.wantCode {
				t.Errorf("status = %d; want %d", rec.Code, c.wantCode)
			}
			if called := len(p.deleted) > 0; called != c.wantCalled {
				t.Errorf("DeleteHLS called = %v; want %v", called, c.wantCalled)
			}
		})
	}
}

func TestHandlerCORSPreflightAllowsDelete(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/hlsv2/sess1", nil)
	req.Header.Set("Origin", "https://web.stremio.com")
	req.Header.Set("Access-Control-Request-Method", http.MethodDelete)
	rec := httptest.NewRecorder()
	newHandler(t).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("preflight status = %d; want 200", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got != "POST, GET, DELETE, OPTIONS" {
		t.Errorf("Access-Control-Allow-Methods = %q; want %q", got, "POST, GET, DELETE, OPTIONS")
	}
}

// ─── GET /tracks/* ───────────────────────────────────────────────────────────

func TestHandlerTracks_Returns200(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/tracks/http%3A%2F%2Fexample.com%2Fvideo.mkv", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
}

// ─── GET /opensubHash ────────────────────────────────────────────────────────

func TestHandlerOpenSubHash(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/opensubHash?videoUrl=http://example.com/video.mkv", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m["result"]; !ok {
		t.Errorf("opensubHash missing result key; got %v", m)
	}
}

// ─── GET /subtitlesTracks ────────────────────────────────────────────────────

func TestHandlerSubtitlesTracks(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/subtitlesTracks?subsUrl=http://example.com/subs.srt", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	m := decodeJSON(t, rec.Body.Bytes())
	if _, ok := m["result"]; !ok {
		t.Errorf("subtitlesTracks missing result key; got %v", m)
	}
}

// ─── GET /subtitles.{ext} ────────────────────────────────────────────────────

func TestHandlerSubtitles_VTT(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/subtitles.vtt?from=http://example.com/subs.srt", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "vtt") {
		t.Errorf("Content-Type = %q; want text/vtt", ct)
	}
}

func TestHandlerSubtitles_SRT(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/subtitles.srt?from=http://example.com/subs.srt", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
}

func TestHandlerSubtitles_WithOffset(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/subtitles.vtt?from=http://example.com/subs.srt&offset=500", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
}

// ─── GET /metrics ─────────────────────────────────────────────────────────────

func TestHandlerMetrics(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/metrics", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q; want text/plain", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "# HELP rivulet_engine_goroutines") {
		t.Errorf("metrics body missing goroutines gauge; got: %s", body[:min(200, len(body))])
	}
}

// ─── GET /hwaccel-profiler ───────────────────────────────────────────────────

func TestHandlerHwaccelProfiler(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/hwaccel-profiler", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	// must be a JSON array
	decodeJSONArray(t, rec.Body.Bytes())
}

// ─── GET /{ih}/0/subtitles.vtt ───────────────────────────────────────────────

func TestHandlerStreamSubtitles(t *testing.T) {
	h := newHandler(t, testEngine())
	rec := serve(t, h, http.MethodGet, "/"+testIH+"/0/subtitles.vtt", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "vtt") {
		t.Errorf("Content-Type = %q; want text/vtt", ct)
	}
}

// ─── /stream alias ───────────────────────────────────────────────────────────

func TestHandlerStreamAlias_FullGet(t *testing.T) {
	eng := testEngine()
	h := newHandler(t, eng)
	rec := serve(t, h, http.MethodGet, "/stream/"+testIH+"/0", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("/stream alias status = %d; want 200", rec.Code)
	}
	if rec.Body.String() != string(eng.data) {
		t.Errorf("stream alias body mismatch")
	}
}

func TestHandlerStreamAlias_InvalidIH404(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/stream/not-a-valid-infohash/0", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404 for invalid IH", rec.Code)
	}
}

// ─── POST /create (createBlob error paths) ───────────────────────────────────

func TestHandlerCreateBlob_MissingBody(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodPost, "/create", nil)
	// body is nil → missing from/blob → client error 400
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (missing from/blob)", rec.Code)
	}
}

func TestHandlerCreateBlob_BadBlob400(t *testing.T) {
	h := newHandler(t)
	for name, body := range map[string]string{
		"not hex":     `{"blob":"zz-not-hex"}`,
		"not torrent": `{"blob":"00ff"}`,
	} {
		rec := serve(t, h, http.MethodPost, "/create", strings.NewReader(body))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d; want 400", name, rec.Code)
		}
	}
}

func TestHandlerCreateBlob_LocalPath400(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodPost, "/create", strings.NewReader(`{"from":"/local/path"}`))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (local path rejected)", rec.Code)
	}
}

// ─── archive route (error path — no body) ────────────────────────────────────

func TestHandlerArchive_CreateNoBody400(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodPost, "/zip/create", nil)
	// archiveHandleCreate → archiveParsePayload → empty body → error 400
	if rec.Code != http.StatusBadRequest {
		t.Errorf("GET /zip/create no-body status = %d; want 400", rec.Code)
	}
}

func TestHandlerArchive_StreamNoKey404(t *testing.T) {
	h := newHandler(t)
	// /zip/stream with no key → session not found → 404
	rec := serve(t, h, http.MethodGet, "/zip/stream", nil)
	// stream with no key → bad request or not found
	if rec.Code == http.StatusOK {
		t.Errorf("GET /zip/stream with no key must not be 200")
	}
}

// ─── nzb stream (no session) ─────────────────────────────────────────────────

func TestHandlerNZBStream_NoKey(t *testing.T) {
	h := newHandler(t)
	// GET /nzb/stream without key → 400 (key required)
	rec := serve(t, h, http.MethodGet, "/nzb/stream", nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d; want 400 (no key)", rec.Code)
	}
}

func TestHandlerNZBStream_UnknownKey404(t *testing.T) {
	h := newHandler(t)
	rec := serve(t, h, http.MethodGet, "/nzb/stream?key=notakey", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d; want 404 (unknown session key)", rec.Code)
	}
}

// ═════════════════════════════════════════════════════════════════════════════
// HELPER UNIT TESTS — unexported functions not covered by api_test.go
// ═════════════════════════════════════════════════════════════════════════════

// ─── truthy ──────────────────────────────────────────────────────────────────

func TestTruthy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"nil → false", "", false},
		{"false → false", "false", false},
		{"null → false", "null", false},
		{"zero → false", "0", false},
		{`"" → false`, `""`, false},
		{"true → true", "true", true},
		{"1 → true", "1", true},
		{"number → true", "42", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := truthy(json.RawMessage(tc.in))
			if got != tc.want {
				t.Errorf("truthy(%q) = %v; want %v", tc.in, got, tc.want)
			}
		})
	}
}

// ─── announceFromTorrent ──────────────────────────────────────────────────────

func TestAnnounceFromTorrent(t *testing.T) {
	t.Run("nil input → nil", func(t *testing.T) {
		if got := announceFromTorrent(nil); got != nil {
			t.Errorf("want nil, got %v", got)
		}
	})
	t.Run("announce field", func(t *testing.T) {
		raw := json.RawMessage(`{"announce":"udp://tracker.example.com:6969"}`)
		got := announceFromTorrent(raw)
		if len(got) != 1 || got[0] != "udp://tracker.example.com:6969" {
			t.Errorf("got %v; want [udp://tracker.example.com:6969]", got)
		}
	})
	t.Run("announce-list", func(t *testing.T) {
		raw := json.RawMessage(`{"announce-list":[["url1","url2"]]}`)
		got := announceFromTorrent(raw)
		if len(got) != 2 {
			t.Errorf("got %v; want [url1, url2]", got)
		}
	})
	t.Run("sources with tracker: prefix", func(t *testing.T) {
		raw := json.RawMessage(`{"sources":["tracker:udp://t.example.com:6969"]}`)
		got := announceFromTorrent(raw)
		if len(got) != 1 || got[0] != "udp://t.example.com:6969" {
			t.Errorf("got %v; want [udp://t.example.com:6969]", got)
		}
	})
	t.Run("invalid JSON → nil", func(t *testing.T) {
		if got := announceFromTorrent(json.RawMessage(`{invalid`)); got != nil {
			t.Errorf("want nil for invalid JSON, got %v", got)
		}
	})
}

// ─── trackersFromSources ──────────────────────────────────────────────────────

func TestTrackersFromSources(t *testing.T) {
	src := []string{
		"tracker:udp://t1.example.com:6969",
		"dht:abc123",
		"tracker:udp://t2.example.com:6969",
	}
	got := trackersFromSources(src)
	if len(got) != 2 {
		t.Fatalf("got %v; want 2 trackers", got)
	}
	if got[0] != "udp://t1.example.com:6969" || got[1] != "udp://t2.example.com:6969" {
		t.Errorf("got %v; want stripped tracker URLs", got)
	}
	if out := trackersFromSources(nil); out != nil {
		t.Errorf("nil input: got %v; want nil", out)
	}
}

// ─── peerSearchSources ────────────────────────────────────────────────────────

func TestPeerSearchSources(t *testing.T) {
	t.Run("nil PeerSearch → nil", func(t *testing.T) {
		b := &createBody{}
		if got := peerSearchSources(b); got != nil {
			t.Errorf("got %v; want nil", got)
		}
	})
	t.Run("with PeerSearch.Sources", func(t *testing.T) {
		b := &createBody{
			PeerSearch: &struct {
				Sources []string `json:"sources"`
			}{Sources: []string{"tracker:udp://t.example.com"}},
		}
		got := peerSearchSources(b)
		if len(got) != 1 || got[0] != "tracker:udp://t.example.com" {
			t.Errorf("got %v; want [tracker:udp://t.example.com]", got)
		}
	})
}

// ─── hexDecode ───────────────────────────────────────────────────────────────

func TestHexDecode(t *testing.T) {
	t.Run("valid hex", func(t *testing.T) {
		got, err := hexDecode("deadbeef")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 4 {
			t.Errorf("len = %d; want 4", len(got))
		}
	})
	t.Run("with whitespace", func(t *testing.T) {
		got, err := hexDecode("  deadbeef  ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 4 {
			t.Errorf("len = %d; want 4", len(got))
		}
	})
	t.Run("invalid hex → error", func(t *testing.T) {
		_, err := hexDecode("zzzz")
		if err == nil {
			t.Fatal("expected error for invalid hex, got nil")
		}
	})
}

// ─── loadavg ─────────────────────────────────────────────────────────────────

func TestLoadavg(t *testing.T) {
	got := loadavg()
	if len(got) != 3 {
		t.Errorf("loadavg() returned %d values; want 3", len(got))
	}
	for i, v := range got {
		if v < 0 {
			t.Errorf("loadavg()[%d] = %v; must be >= 0", i, v)
		}
	}
}

// ─── cpus ────────────────────────────────────────────────────────────────────

func TestCPUs(t *testing.T) {
	got := cpus()
	n := runtime.NumCPU()
	if len(got) != n {
		t.Errorf("cpus() len = %d; want %d", len(got), n)
	}
	for i, v := range got {
		if _, ok := v.(map[string]any); !ok {
			t.Errorf("cpus()[%d] = %T; want map[string]any", i, v)
		}
	}
}

// ─── buildCertDomain ─────────────────────────────────────────────────────────

func TestBuildCertDomain(t *testing.T) {
	tests := []struct {
		cn   string
		ip   string
		want string
	}{
		{"*.abc123.stremio.rocks", "192.168.0.62", "192-168-0-62.abc123.stremio.rocks"},
		{"*.foo.stremio.rocks", "10.0.0.1", "10-0-0-1.foo.stremio.rocks"},
		// no wildcard → prefix with dashed IP
		{"host.stremio.rocks", "192.168.1.1", "192-168-1-1.host.stremio.rocks"},
	}
	for _, tc := range tests {
		t.Run(tc.ip, func(t *testing.T) {
			got := buildCertDomain(tc.cn, tc.ip)
			if got != tc.want {
				t.Errorf("buildCertDomain(%q, %q) = %q; want %q", tc.cn, tc.ip, got, tc.want)
			}
		})
	}
}

// ─── CacheAuthKey + CachedAuthKey ────────────────────────────────────────────

func TestCacheAndCachedAuthKey(t *testing.T) {
	dir := t.TempDir()

	t.Run("empty key ignored", func(t *testing.T) {
		CacheAuthKey(dir, "")
		if got := CachedAuthKey(dir); got != "" {
			t.Errorf("expected empty, got %q", got)
		}
	})
	t.Run("write then read", func(t *testing.T) {
		CacheAuthKey(dir, "myauthkey123")
		got := CachedAuthKey(dir)
		if got != "myauthkey123" {
			t.Errorf("CachedAuthKey = %q; want myauthkey123", got)
		}
	})
	t.Run("non-existent dir → empty", func(t *testing.T) {
		if got := CachedAuthKey("/nonexistent/dir/xyz"); got != "" {
			t.Errorf("expected empty for missing dir, got %q", got)
		}
	})
}

// ─── PrimaryIPv4 ─────────────────────────────────────────────────────────────

func TestPrimaryIPv4(t *testing.T) {
	// Must not panic; returns "" or a valid IPv4.
	got := PrimaryIPv4()
	if got != "" {
		// If a value is returned it must parse as IPv4 (no colons).
		if strings.Contains(got, ":") {
			t.Errorf("PrimaryIPv4() = %q; must be IPv4 (no colons)", got)
		}
	}
}

// ─── decodePEMField ──────────────────────────────────────────────────────────

func TestDecodePEMField(t *testing.T) {
	t.Run("empty → empty", func(t *testing.T) {
		if got := decodePEMField(""); got != "" {
			t.Errorf("got %q; want empty", got)
		}
	})
	t.Run("raw PEM passthrough", func(t *testing.T) {
		pem := "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"
		if got := decodePEMField(pem); got != pem {
			t.Errorf("raw PEM should pass through unchanged")
		}
	})
	t.Run("base64 PEM decoded", func(t *testing.T) {
		plain := "-----BEGIN CERTIFICATE-----\nhello"
		b64 := base64.StdEncoding.EncodeToString([]byte(plain))
		got := decodePEMField(b64)
		if got != plain {
			t.Errorf("decodePEMField(base64) = %q; want %q", got, plain)
		}
	})
	t.Run("non-base64 → returned as-is", func(t *testing.T) {
		if got := decodePEMField("notbase64!!!"); got != "notbase64!!!" {
			t.Errorf("got %q; want original string", got)
		}
	})
}

// ─── secsToHHMMSS ────────────────────────────────────────────────────────────

func TestSecsToHHMMSS(t *testing.T) {
	tests := []struct {
		secs float64
		want string
	}{
		{0, "00:00:00"},
		{90, "00:01:30"},
		{3661, "01:01:01"},
		{3600, "01:00:00"},
		{-5, "00:00:00"}, // negative clamped to 0
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := secsToHHMMSS(tc.secs)
			if got != tc.want {
				t.Errorf("secsToHHMMSS(%v) = %q; want %q", tc.secs, got, tc.want)
			}
		})
	}
}

// ─── xmlEscape ───────────────────────────────────────────────────────────────

func TestXMLEscape(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"hello", "hello"},
		{"<tag>", "&lt;tag&gt;"},
		{"a & b", "a &amp; b"},
		{`"quoted"`, "&#34;quoted&#34;"},
		{"'apostrophe'", "&#39;apostrophe&#39;"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := xmlEscape(tc.in)
			if got != tc.want {
				t.Errorf("xmlEscape(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ─── humanizeSize ────────────────────────────────────────────────────────────

func TestHumanizeSize(t *testing.T) {
	tests := []struct {
		in      int64
		wantSfx string
	}{
		{2 * (1 << 30), "GB"},   // 2 GiB
		{512 * (1 << 20), "MB"}, // 512 MiB
		{5 * (1 << 30), "GB"},   // 5 GiB
		{100 * (1 << 20), "MB"}, // 100 MiB
	}
	for _, tc := range tests {
		t.Run(tc.wantSfx, func(t *testing.T) {
			got := humanizeSize(tc.in)
			if !strings.HasSuffix(got, tc.wantSfx) {
				t.Errorf("humanizeSize(%d) = %q; want suffix %q", tc.in, got, tc.wantSfx)
			}
		})
	}
}

// ─── isProgressive / pickYTFormat ────────────────────────────────────────────

func TestIsProgressive(t *testing.T) {
	t.Run("both codecs → progressive", func(t *testing.T) {
		f := &ytFormat{VCodec: "h264", ACodec: "aac"}
		if !isProgressive(f) {
			t.Error("h264+aac should be progressive")
		}
	})
	t.Run("video only → not progressive", func(t *testing.T) {
		f := &ytFormat{VCodec: "h264", ACodec: "none"}
		if isProgressive(f) {
			t.Error("video-only should not be progressive")
		}
	})
	t.Run("empty codecs → not progressive", func(t *testing.T) {
		f := &ytFormat{}
		if isProgressive(f) {
			t.Error("no codecs should not be progressive")
		}
	})
}

func TestPickYTFormat(t *testing.T) {
	t.Run("nil input → nil", func(t *testing.T) {
		if pickYTFormat(nil) != nil {
			t.Error("expected nil for empty formats")
		}
	})
	t.Run("prefers mp4 progressive", func(t *testing.T) {
		formats := []ytFormat{
			{VCodec: "h264", ACodec: "aac", Ext: "webm", URL: "webm-url"},
			{VCodec: "h264", ACodec: "aac", Ext: "mp4", URL: "mp4-url"},
		}
		got := pickYTFormat(formats)
		if got == nil || got.URL != "mp4-url" {
			t.Errorf("got %v; want mp4-url", got)
		}
	})
	t.Run("falls back to any progressive", func(t *testing.T) {
		formats := []ytFormat{
			{VCodec: "vp9", ACodec: "opus", Ext: "webm", URL: "webm-url"},
			{VCodec: "none", ACodec: "aac", Ext: "mp4", URL: "audio-only"},
		}
		got := pickYTFormat(formats)
		if got == nil || got.URL != "webm-url" {
			t.Errorf("got %v; want webm-url", got)
		}
	})
	t.Run("no progressive → nil", func(t *testing.T) {
		formats := []ytFormat{
			{VCodec: "none", ACodec: "aac", Ext: "mp4"},
			{VCodec: "h264", ACodec: "none", Ext: "mp4"},
		}
		if pickYTFormat(formats) != nil {
			t.Error("expected nil when no progressive format")
		}
	})
}

// ─── archiveSelectEntry ───────────────────────────────────────────────────────

func TestArchiveSelectEntry(t *testing.T) {
	entries := []archive.Entry{
		{Name: "a.txt", Size: 100},
		{Name: "b.mkv", Size: 500},
		{Name: "c.mp4", Size: 200},
		{Name: "dir/", IsDir: true, Size: 0},
	}

	t.Run("empty archive → error", func(t *testing.T) {
		_, err := archiveSelectEntry(nil, &archivePayload{})
		if err == nil {
			t.Fatal("expected error for empty entries")
		}
	})

	t.Run("only dirs → error", func(t *testing.T) {
		_, err := archiveSelectEntry([]archive.Entry{{Name: "d/", IsDir: true}}, &archivePayload{})
		if err == nil {
			t.Fatal("expected error for dir-only archive")
		}
	})

	t.Run("explicit fileIdx 0 → first file (a.txt)", func(t *testing.T) {
		p := &archivePayload{FileIdx: json.RawMessage("0")}
		got, err := archiveSelectEntry(entries, p)
		if err != nil || got != "a.txt" {
			t.Errorf("got %q %v; want a.txt", got, err)
		}
	})

	t.Run("explicit fileIdx 1 → second file (b.mkv)", func(t *testing.T) {
		p := &archivePayload{FileIdx: json.RawMessage("1")}
		got, err := archiveSelectEntry(entries, p)
		if err != nil || got != "b.mkv" {
			t.Errorf("got %q %v; want b.mkv", got, err)
		}
	})

	t.Run("fileMustInclude substring match", func(t *testing.T) {
		p := &archivePayload{FileMustInclude: json.RawMessage(`["c.mp4"]`)}
		got, err := archiveSelectEntry(entries, p)
		if err != nil || got != "c.mp4" {
			t.Errorf("got %q %v; want c.mp4", got, err)
		}
	})

	t.Run("fileMustInclude case-insensitive", func(t *testing.T) {
		p := &archivePayload{FileMustInclude: json.RawMessage(`["B.MKV"]`)}
		got, err := archiveSelectEntry(entries, p)
		if err != nil || got != "b.mkv" {
			t.Errorf("got %q %v; want b.mkv", got, err)
		}
	})

	t.Run("no filter → largest video (b.mkv at 500)", func(t *testing.T) {
		got, err := archiveSelectEntry(entries, &archivePayload{})
		if err != nil || got != "b.mkv" {
			t.Errorf("got %q %v; want b.mkv (largest video)", got, err)
		}
	})
}

// ─── archiveLargestVideo ──────────────────────────────────────────────────────

func TestArchiveLargestVideo(t *testing.T) {
	t.Run("picks largest video by size", func(t *testing.T) {
		files := []archive.Entry{
			{Name: "small.mkv", Size: 100},
			{Name: "large.mp4", Size: 999},
			{Name: "doc.txt", Size: 5000},
		}
		got := archiveLargestVideo(files)
		if got != "large.mp4" {
			t.Errorf("got %q; want large.mp4", got)
		}
	})
	t.Run("no video → largest file overall", func(t *testing.T) {
		files := []archive.Entry{
			{Name: "a.txt", Size: 50},
			{Name: "b.doc", Size: 200},
		}
		got := archiveLargestVideo(files)
		if got != "b.doc" {
			t.Errorf("got %q; want b.doc (largest overall)", got)
		}
	})
}

// ─── archiveEncodePath ────────────────────────────────────────────────────────

func TestArchiveEncodePath(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"simple.mkv", "simple.mkv"},
		{"dir/file.mkv", "dir/file.mkv"},
		{"my file.mkv", "my%20file.mkv"},
		{"a/b c/d.mkv", "a/b%20c/d.mkv"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got := archiveEncodePath(tc.in)
			if got != tc.want {
				t.Errorf("archiveEncodePath(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ─── archiveNewKey ────────────────────────────────────────────────────────────

func TestArchiveNewKey(t *testing.T) {
	key := archiveNewKey()
	if len(key) != 32 {
		t.Errorf("archiveNewKey() len = %d; want 32", len(key))
	}
	// Two calls must produce different keys.
	if key2 := archiveNewKey(); key == key2 {
		t.Error("archiveNewKey() produced identical keys on successive calls")
	}
}

// ─── nzbResolveFile ──────────────────────────────────────────────────────────

func TestNzbResolveFile(t *testing.T) {
	files := []nzb.File{
		{Name: "documentary.mp4", Size: 800},
		{Name: "readme.txt", Size: 100},
		{Name: "movie.mkv", Size: 2000},
	}

	t.Run("by exact name", func(t *testing.T) {
		got := nzbResolveFile(files, "documentary.mp4")
		if got == nil || got.Name != "documentary.mp4" {
			t.Errorf("got %v; want documentary.mp4", got)
		}
	})
	t.Run("no name → largest video (movie.mkv)", func(t *testing.T) {
		got := nzbResolveFile(files, "")
		if got == nil || got.Name != "movie.mkv" {
			t.Errorf("got %v; want movie.mkv", got)
		}
	})
	t.Run("unknown name → falls back to largest video", func(t *testing.T) {
		got := nzbResolveFile(files, "unknown.avi")
		if got == nil || got.Name != "movie.mkv" {
			t.Errorf("got %v; want movie.mkv (largest video fallback)", got)
		}
	})
}

// ─── nzbLargestVideo ─────────────────────────────────────────────────────────

func TestNzbLargestVideo(t *testing.T) {
	t.Run("picks largest video", func(t *testing.T) {
		files := []nzb.File{
			{Name: "small.mkv", Size: 100},
			{Name: "large.mp4", Size: 999},
			{Name: "doc.nfo", Size: 5000},
		}
		got := nzbLargestVideo(files)
		if got == nil || got.Name != "large.mp4" {
			t.Errorf("got %v; want large.mp4", got)
		}
	})
	t.Run("no video → first file", func(t *testing.T) {
		files := []nzb.File{
			{Name: "info.nfo", Size: 10},
			{Name: "readme.txt", Size: 500},
		}
		got := nzbLargestVideo(files)
		if got == nil || got.Name != "info.nfo" {
			t.Errorf("got %v; want info.nfo (first file)", got)
		}
	})
	t.Run("nil files → nil", func(t *testing.T) {
		if got := nzbLargestVideo(nil); got != nil {
			t.Errorf("got %v; want nil for empty files", got)
		}
	})
}

// ─── archiveSelectEntry with string fileIdx ───────────────────────────────────

func TestArchiveSelectEntry_StringFileIdx(t *testing.T) {
	entries := []archive.Entry{
		{Name: "a.txt", Size: 100},
		{Name: "b.mkv", Size: 500},
	}
	p := &archivePayload{FileIdx: json.RawMessage(`"1"`)} // string "1"
	got, err := archiveSelectEntry(entries, p)
	if err != nil || got != "b.mkv" {
		t.Errorf("string fileIdx=1: got %q %v; want b.mkv", got, err)
	}
}

// ─── writeTempPEM ────────────────────────────────────────────────────────────

func TestWriteTempPEM(t *testing.T) {
	dir := t.TempDir()
	content := "-----BEGIN CERTIFICATE-----\nhello"
	path, err := writeTempPEM(dir, "test-*.pem", content)
	if err != nil {
		t.Fatalf("writeTempPEM error: %v", err)
	}
	defer os.Remove(path)

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	if string(got) != content {
		t.Errorf("written = %q; want %q", got, content)
	}
}

// min is a helper for go < 1.21; in 1.26 the built-in works but let's be safe.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
