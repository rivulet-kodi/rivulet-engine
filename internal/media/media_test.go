// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Tests for pure and offline-testable helpers in internal/media.
//
// Excluded (require real ffprobe/ffmpeg invocation):
//   - Probe, Tracks, probeMedia, transcodeSegment, HLSFile (segment transcode)
//   - verifyEncoder, selectEncoder, encodersList, extractSubtitle

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// ── test helper ───────────────────────────────────────────────────────────────

// newTestHLSManager constructs an hlsManager with a temp base dir and no
// running reaper goroutine.  This avoids calling newHLS() (which invokes
// ffmpeg to probe encoders).
func newTestHLSManager(t *testing.T) *hlsManager {
	t.Helper()
	return &hlsManager{
		base:       t.TempDir(),
		cfg:        DefaultHLSConfig().normalize(1),
		sessions:   map[string]*hlsSession{},
		probeCache: map[string]probeCacheEntry{},
		stopCh:     make(chan struct{}),
	}
}

// stubOpenSubClientTransport redirects the package's openSubClient at ts for
// the duration of the test (restored via t.Cleanup) and returns a
// syntactically public base URL that passes validateRemoteURL's IP check
// while every request actually lands on ts. httptest can only bind to
// loopback addresses, which validateRemoteURL must reject in production, so
// this lets tests exercise SubtitlesTracks/WriteSubtitles/OpenSubHash's full
// public API — including the SSRF pre-flight — against a real local server.
func stubOpenSubClientTransport(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	stub := func() *http.Transport {
		return &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, ts.Listener.Addr().String())
			},
		}
	}
	orig, origSelf := openSubClient.Transport, openSubSelfClient.Transport
	openSubClient.Transport = stub()
	openSubSelfClient.Transport = stub()
	t.Cleanup(func() {
		openSubClient.Transport = orig
		openSubSelfClient.Transport = origSelf
	})
	return "http://93.184.216.34"
}

// ── hls.go: sanitizeM3U8Attr ──────────────────────────────────────────────────

func TestSanitizeM3U8Attr(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"clean", "English", "English"},
		// Title with embedded double-quotes (e.g. English "HD") — quotes stripped.
		{"title with double quotes", `English "HD"`, "English HD"},
		// Newline in the middle of a name.
		{"title with LF", "English\nHD", "EnglishHD"},
		// CR+LF Windows line ending — both bytes removed.
		{"title with CRLF", "English\r\nHD", "EnglishHD"},
		// Comma would break the M3U8 attribute list.
		{"title with comma", "English, HD", "English HD"},
		// All four illegal chars together.
		{"all illegal chars", "\",\r\n", ""},
		// Already clean with spaces and dashes.
		{"clean with spaces", "English - HD", "English - HD"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeM3U8Attr(tc.in)
			if got != tc.want {
				t.Errorf("sanitizeM3U8Attr(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ── hls.go: localize ──────────────────────────────────────────────────────────

func TestLocalize(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"https://127.0.0.1:12470/stream", "http://127.0.0.1:11470/stream"},
		{"http://127.0.0.1:11470/stream", "http://127.0.0.1:11470/stream"},    // already local http
		{"https://example.com/stream", "https://example.com/stream"},          // external — unchanged
		{"https://127.0.0.1:12470/a/b?c=d", "http://127.0.0.1:11470/a/b?c=d"}, // path + query
		{"", ""},
	}
	for _, tc := range cases {
		got := localize(tc.in)
		if got != tc.want {
			t.Errorf("localize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// ── hls.go: ftoa ─────────────────────────────────────────────────────────────

func TestFtoa(t *testing.T) {
	cases := []struct {
		f    float64
		want string
	}{
		{0, "0.000"},
		{4.0, "4.000"},
		{3.14159, "3.142"},
		{1.5, "1.500"},
		{120, "120.000"},
	}
	for _, tc := range cases {
		got := ftoa(tc.f)
		if got != tc.want {
			t.Errorf("ftoa(%v) = %q, want %q", tc.f, got, tc.want)
		}
	}
}

// ── hls.go: hlsSession.lockFor ────────────────────────────────────────────────

func TestHLSSessionLockFor(t *testing.T) {
	s := &hlsSession{segLocks: map[string]*sync.Mutex{}}

	// Same filename → same mutex pointer.
	mu1 := s.lockFor("seg0.ts")
	mu2 := s.lockFor("seg0.ts")
	if mu1 != mu2 {
		t.Error("lockFor returned different mutexes for the same filename")
	}

	// Different filename → different mutex.
	mu3 := s.lockFor("a1seg0.ts")
	if mu1 == mu3 {
		t.Error("lockFor returned the same mutex for different filenames")
	}

	// Mutex must be usable.
	if !mu1.TryLock() {
		t.Error("expected to acquire freshly returned mutex")
	}
	mu1.Unlock()
}

// ── hls.go: hlsSession.writePlaylist ─────────────────────────────────────────

func TestHLSSessionWritePlaylist(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{
		dir:      dir,
		duration: 10.0, // 10 s → ceil(10/4) = 3 segments
		segLocks: map[string]*sync.Mutex{},
	}

	path := filepath.Join(dir, "playlist.m3u8")
	if err := s.writePlaylist(path, ""); err != nil {
		t.Fatalf("writePlaylist: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	content := string(raw)

	for _, want := range []string{
		"#EXTM3U",
		"#EXT-X-VERSION:3",
		"#EXT-X-PLAYLIST-TYPE:VOD",
		"#EXT-X-ENDLIST",
		"seg0.ts",
		"seg1.ts",
		"seg2.ts",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("writePlaylist output missing %q:\n%s", want, content)
		}
	}

	// Duration 0 must return an error.
	s2 := &hlsSession{dir: dir, duration: 0, segLocks: map[string]*sync.Mutex{}}
	if err := s2.writePlaylist(filepath.Join(dir, "bad.m3u8"), ""); err == nil {
		t.Error("writePlaylist with duration=0 should return an error")
	}
}

// playlist.m3u8 and video.m3u8 share segPrefix "" but are distinct files; both
// must be written regardless of request order.
func TestHLSSessionWritePlaylistSharedPrefix(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, duration: 10.0, segLocks: map[string]*sync.Mutex{}}
	for _, name := range []string{"playlist.m3u8", "video.m3u8"} {
		p := filepath.Join(dir, name)
		if err := s.writePlaylist(p, ""); err != nil {
			t.Fatalf("writePlaylist(%s): %v", name, err)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("%s not written: %v", name, err)
		}
		if !strings.Contains(string(raw), "seg2.ts") {
			t.Errorf("%s content wrong:\n%s", name, raw)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "video.m3u8.tmp")); err == nil {
		t.Error("temp file left behind")
	}
}

func TestHLSSessionWritePlaylistAudioPrefix(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, duration: 8.0, segLocks: map[string]*sync.Mutex{}}
	path := filepath.Join(dir, "audio1.m3u8")
	if err := s.writePlaylist(path, "a1"); err != nil {
		t.Fatalf("writePlaylist: %v", err)
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "a1seg0.ts") {
		t.Errorf("prefix not applied: %s", raw)
	}
}

// ── hls.go: hlsSession.writeSubPlaylist ──────────────────────────────────────

func TestHLSSessionWriteSubPlaylist(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, duration: 90.0, segLocks: map[string]*sync.Mutex{}}
	path := filepath.Join(dir, "sub0.m3u8")
	if err := s.writeSubPlaylist(path, 0); err != nil {
		t.Fatalf("writeSubPlaylist: %v", err)
	}
	raw, _ := os.ReadFile(path)
	content := string(raw)
	for _, want := range []string{"#EXTM3U", "#EXT-X-ENDLIST", "sub0.vtt"} {
		if !strings.Contains(content, want) {
			t.Errorf("writeSubPlaylist missing %q:\n%s", want, content)
		}
	}
}

// ── hls.go: hlsManager lifecycle ─────────────────────────────────────────────

func TestHLSManagerIdGuard(t *testing.T) {
	m := newTestHLSManager(t)
	base := m.base

	badIDs := []string{
		".",
		"..",
		"../escape",
		"foo/bar",
		"a/..b",
		"foo..bar",    // contains ".."
		"",            // empty
		"/etc/passwd", // absolute
	}

	// A public IP literal: passes validateRemoteURL's format/IP checks with no
	// DNS lookup, so these cases exercise the id guard itself rather than
	// short-circuiting on URL rejection. No connection is ever attempted since
	// every id here is rejected before StartHLS reaches probing.
	const validMediaURL = "http://93.184.216.34/dummy.mkv"

	for _, id := range badIDs {
		t.Run(fmt.Sprintf("id=%q", id), func(t *testing.T) {
			_, err := m.StartHLS(id, validMediaURL, types.HLSSessionOptions{})
			if err == nil {
				t.Errorf("StartHLS(%q) should have returned an error", id)
			}
		})
	}

	// Regression: base dir must still exist after all rejected calls.
	if _, err := os.Stat(base); os.IsNotExist(err) {
		t.Error("regression: base dir was removed after id-guard rejection")
	}
}

// TestHLSManagerRejectsPrivateMediaURL is the regression test for the SSRF fix:
// a mediaURL resolving to a private/loopback address must be rejected before
// the session directory is created or any ffprobe/ffmpeg subprocess spawned,
// even though a real, responsive server sits at that address.
func TestHLSManagerRejectsPrivateMediaURL(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not a media file"))
	}))
	defer ts.Close()

	m := newTestHLSManager(t)

	_, err := m.StartHLS("sess-abc", ts.URL+"/dummy.mkv", types.HLSSessionOptions{})
	if err == nil {
		t.Fatal("StartHLS with a loopback mediaURL should have been rejected")
	}

	sessDir := filepath.Join(m.base, "sess-abc")
	if _, statErr := os.Stat(sessDir); !os.IsNotExist(statErr) {
		t.Error("session dir was created for a rejected mediaURL")
	}
}

// TestHLSManagerAcceptsSelfMediaURL guards the self-reference carve-out: the
// https UI on :12470 routinely asks the server to transcode a stream this same
// server is serving on :11470, so a mediaURL matching selfBase must pass the
// SSRF gate even though it is loopback. A blanket loopback rejection would
// break HLS transcoding of torrent streams entirely.
func TestHLSManagerAcceptsSelfMediaURL(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a media file"))
	}))
	defer ts.Close()

	m := newTestHLSManager(t)
	m.selfBase = ts.URL

	// probeMedia will fail on this non-media body, but the failure must come
	// from probing, not from the SSRF pre-flight rejecting the URL.
	_, err := m.StartHLS("sess-self", ts.URL+testSelfPath, types.HLSSessionOptions{})
	if err != nil && strings.Contains(err.Error(), "only http/https allowed") {
		t.Fatalf("self-origin mediaURL rejected by the SSRF gate: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "blocked") {
		t.Fatalf("self-origin mediaURL rejected as a private address: %v", err)
	}
}

func TestHLSManagerSessions(t *testing.T) {
	m := newTestHLSManager(t)
	if n := m.Sessions(); n != 0 {
		t.Errorf("initial Sessions() = %d, want 0", n)
	}

	// Directly inject a session (bypassing probeMedia).
	m.mu.Lock()
	m.sessions["s1"] = &hlsSession{segLocks: map[string]*sync.Mutex{}}
	m.mu.Unlock()

	if n := m.Sessions(); n != 1 {
		t.Errorf("Sessions() after add = %d, want 1", n)
	}
}

func TestHLSManagerEvictIdle(t *testing.T) {
	m := newTestHLSManager(t)

	// Expired session: last access well in the past.
	dirExpired := filepath.Join(m.base, "expired")
	_ = os.MkdirAll(dirExpired, 0o755)
	sessExpired := &hlsSession{dir: dirExpired, segLocks: map[string]*sync.Mutex{}}
	sessExpired.lastAccess.Store(time.Now().Add(-2 * m.cfg.SessionTTL).UnixNano())

	// Fresh session: just accessed.
	dirFresh := filepath.Join(m.base, "fresh")
	_ = os.MkdirAll(dirFresh, 0o755)
	sessFresh := &hlsSession{dir: dirFresh, segLocks: map[string]*sync.Mutex{}}
	sessFresh.lastAccess.Store(time.Now().UnixNano())

	m.mu.Lock()
	m.sessions["expired"] = sessExpired
	m.sessions["fresh"] = sessFresh
	m.mu.Unlock()

	m.evictIdle()

	// Expired session and its dir must be gone.
	if _, err := os.Stat(dirExpired); !os.IsNotExist(err) {
		t.Error("evictIdle did not remove expired session dir")
	}
	m.mu.Lock()
	_, hasExpired := m.sessions["expired"]
	_, hasFresh := m.sessions["fresh"]
	m.mu.Unlock()
	if hasExpired {
		t.Error("evictIdle did not remove expired session from map")
	}
	// Fresh session must be kept.
	if !hasFresh {
		t.Error("evictIdle removed a fresh session")
	}
	if _, err := os.Stat(dirFresh); os.IsNotExist(err) {
		t.Error("evictIdle removed fresh session dir")
	}
}

// TestHLSManagerEvictIdleSkipsInFlight is the regression test for the HLS
// reaper deleting a session dir while its transcode is still running: a
// session with in-flight work (transcodeSegment/HLSFile) must never be
// evicted no matter how stale lastAccess is, and must be evicted on the
// very next reaper pass once that work completes and the TTL window has
// elapsed. Drives evictIdle directly instead of sleeping past sessionTTL.
func TestHLSManagerEvictIdleSkipsInFlight(t *testing.T) {
	m := newTestHLSManager(t)

	dir := filepath.Join(m.base, "busy")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sess := &hlsSession{dir: dir, segLocks: map[string]*sync.Mutex{}}
	// lastAccess is already well past sessionTTL, as it would be for a real
	// segment transcode that has been running for minutes.
	sess.lastAccess.Store(time.Now().Add(-2 * m.cfg.SessionTTL).UnixNano())
	sess.inFlight.Add(1) // simulates an HLSFile call still executing

	m.mu.Lock()
	m.sessions["busy"] = sess
	m.mu.Unlock()

	m.evictIdle()

	m.mu.Lock()
	_, stillPresent := m.sessions["busy"]
	m.mu.Unlock()
	if !stillPresent {
		t.Fatal("evictIdle removed a session with in-flight work despite a stale lastAccess")
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Fatal("evictIdle removed the session dir while work was in flight")
	}

	// The work finishes: HLSFile's defer decrements inFlight to 0. lastAccess
	// stays stale (it was set before the simulated transcode began), so the
	// very next reaper pass must now evict the session.
	sess.inFlight.Add(-1)
	m.evictIdle()

	m.mu.Lock()
	_, stillPresent = m.sessions["busy"]
	m.mu.Unlock()
	if stillPresent {
		t.Fatal("evictIdle did not remove the session once work completed and the TTL elapsed")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("evictIdle did not remove the session dir once genuinely idle")
	}
}

// TestHLSFileTracksInFlight verifies HLSFile itself increments/decrements
// hlsSession.inFlight around the whole call (not just at entry), on both the
// success and the error return path, using the pure-Go playlist.m3u8 branch
// so no real ffmpeg/ffprobe invocation is required.
func TestHLSFileTracksInFlight(t *testing.T) {
	m := newTestHLSManager(t)
	dir := filepath.Join(m.base, "playlist-sess")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sess := &hlsSession{
		dir:          dir,
		duration:     8,
		segLocks:     map[string]*sync.Mutex{},
		playlistData: map[string]struct{}{},
	}
	sess.lastAccess.Store(time.Now().UnixNano())
	m.mu.Lock()
	m.sessions["playlist-sess"] = sess
	m.mu.Unlock()

	if _, _, err := m.HLSFile(context.Background(), "playlist-sess", "playlist.m3u8"); err != nil {
		t.Fatalf("HLSFile(playlist.m3u8) = %v, want nil", err)
	}
	if n := sess.inFlight.Load(); n != 0 {
		t.Fatalf("inFlight = %d after a successful HLSFile call, want 0 (leaked guard)", n)
	}

	// Error path (malformed segment name) must also release the guard.
	if _, _, err := m.HLSFile(context.Background(), "playlist-sess", "seg-bad.ts"); err == nil {
		t.Fatal("HLSFile with a malformed segment name should have errored")
	}
	if n := sess.inFlight.Load(); n != 0 {
		t.Fatalf("inFlight = %d after an error return, want 0 (leaked guard on error path)", n)
	}
}

// TestHLSManagerCloseHLSRemovesBaseDir is the regression test for MED-2: the
// HLS base directory is now created per-manager by os.MkdirTemp (unique,
// 0700, never shared with another process or user), so unlike the old fixed
// "$TMPDIR/rivulet-engine-hls" path there is no reason to preserve it across
// CloseHLS — removing it, session sub-dirs included, in one shot is both
// simpler and correct.
func TestHLSManagerCloseHLSRemovesBaseDir(t *testing.T) {
	m := newTestHLSManager(t)
	base := m.base

	// Manually create a session with a sub-directory.
	sessDir := filepath.Join(base, "sess-cleanup")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	sess := &hlsSession{dir: sessDir, segLocks: map[string]*sync.Mutex{}}
	sess.lastAccess.Store(time.Now().UnixNano())
	m.sessions["sess-cleanup"] = sess
	m.mu.Unlock()

	m.CloseHLS()

	if _, err := os.Stat(base); !os.IsNotExist(err) {
		t.Error("CloseHLS did not remove the per-manager base dir")
	}
	if _, err := os.Stat(sessDir); !os.IsNotExist(err) {
		t.Error("CloseHLS did not remove session dir")
	}
}

// TestNewHLSBaseDirIsPrivate is the regression test for MED-2: newHLSBaseDir
// must create a fresh, unpredictable, owner-only (0700) base directory —
// never the old fixed, world-readable "$TMPDIR/rivulet-engine-hls" path a
// co-resident user on a multi-user host could pre-create or read. Calls
// newHLSBaseDir directly rather than newHLS, which shells out to ffmpeg to
// probe encoders.
func TestNewHLSBaseDirIsPrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	base := newHLSBaseDir("")
	defer os.RemoveAll(base)

	if filepath.Base(base) == "rivulet-engine-hls" {
		t.Errorf("base dir = %q, want a randomized os.MkdirTemp name, not the old predictable one", base)
	}
	fi, err := os.Stat(base)
	if err != nil {
		t.Fatalf("stat base dir: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Errorf("base dir mode = %o, want 0700", perm)
	}
}

// ── media.go: validateRemoteURL (SSRF / local-file-read guard) ──────────────

func TestValidateRemoteURL(t *testing.T) {
	const self = "http://127.0.0.1:11470"
	cases := []struct {
		name     string
		url      string
		selfBase string
		wantErr  bool
	}{
		{"file scheme rejected", "file:///etc/passwd", "", true},
		{"bare absolute path rejected", "/etc/passwd", "", true},
		{"pipe scheme rejected", "pipe:0", "", true},
		{"concat scheme rejected", "concat:/etc/passwd|/etc/shadow", "", true},
		{"data scheme rejected", "data:text/plain,hello", "", true},
		{"cloud metadata address rejected", "http://169.254.169.254/latest/meta-data/", "", true},
		{"loopback address rejected", "http://127.0.0.1/dummy.mkv", "", true},
		{"private RFC1918 address rejected", "http://192.168.1.5/dummy.mkv", "", true},
		{"link-local address rejected", "http://169.254.1.1/dummy.mkv", "", true},
		{"public http URL accepted", "http://93.184.216.34/dummy.mkv", "", false},
		{"public https URL accepted", "https://93.184.216.34/dummy.mkv", "", false},
		// selfBase carve-out: the https UI on :12470 asking this server to
		// transcode a stream it serves on :11470 must keep working.
		{"own origin accepted", "http://127.0.0.1:11470" + testSelfPath, self, false},
		{"own origin via self-signed https twin accepted", "https://127.0.0.1:12470" + testSelfPath, self, false},
		{"own origin bare rejected", "http://127.0.0.1:11470", self, true},
		{"file scheme still rejected with selfBase", "file:///etc/passwd", self, true},
		{"other loopback port still rejected", "http://127.0.0.1:22/dummy.mkv", self, true},
		{"metadata still rejected with selfBase", "http://169.254.169.254/latest/meta-data/", self, true},
		{"prefix-confusion host rejected", "http://127.0.0.1:11470.evil.com/x", self, true},
		{"self host with different scheme rejected", "https://127.0.0.1:11470" + testSelfPath, self, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRemoteURL(tc.url, tc.selfBase)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateRemoteURL(%q, %q) error = %v, wantErr %v", tc.url, tc.selfBase, err, tc.wantErr)
			}
		})
	}
}

// ── media.go: Probe/Tracks cache hard-cap eviction ───────────────────────────

// newTestProber constructs a prober with initialized caches but no hlsManager
// (Probe and Tracks never touch p.hls).
func newTestProber() *prober {
	return &prober{
		probeCache:  map[string]probeResultEntry{},
		tracksCache: map[string]tracksCacheEntry{},
	}
}

// TestProbeAndTracksRejectLocalURLs is the regression test for the
// unauthenticated ffprobe SSRF/local-file-read surface: /probe and /tracks
// both hand their url query parameter to an ffprobe subprocess, which speaks
// file://, concat: and friends, so validation must happen before the spawn.
func TestProbeAndTracksRejectLocalURLs(t *testing.T) {
	bad := []string{
		"file:///etc/passwd",
		"concat:/etc/passwd|/etc/shadow",
		"data:text/plain,hello",
		"http://169.254.169.254/latest/meta-data/",
		"http://127.0.0.1:22/dummy.mkv",
		"http://192.168.1.5/dummy.mkv",
	}
	for _, raw := range bad {
		t.Run(raw, func(t *testing.T) {
			p := newTestProber()
			if _, err := p.Probe(raw); err == nil {
				t.Errorf("Probe(%q) should have been rejected", raw)
			}
			if _, err := p.Tracks(raw); err == nil {
				t.Errorf("Tracks(%q) should have been rejected", raw)
			}
		})
	}
}

// TestProbeCacheHardCap is the regression test for the unbounded-growth bug:
// a burst of distinct URLs, all still within their TTL, must never push
// p.probeCache past probeTracksCacheMaxSize.  http://127.0.0.1:1 is a
// privileged, unused port, so ffprobe fails near-instantly on "connection
// refused" with no real network I/O — deterministic and offline whether or
// not the ffprobe binary is even installed (a missing binary fails just as
// fast); only the resulting cache size is under test, not ffprobe's output.
func TestProbeCacheHardCap(t *testing.T) {
	p := newTestProber()
	notExpired := time.Now().Add(time.Hour)
	for i := range probeTracksCacheMaxSize {
		p.probeCache[fmt.Sprintf("http://127.0.0.1:1/pre%d", i)] = probeResultEntry{expiresAt: notExpired}
	}

	if _, err := p.Probe("http://127.0.0.1:1/new"); err == nil {
		t.Fatal("Probe against a refused port unexpectedly succeeded")
	}

	if got := len(p.probeCache); got > probeTracksCacheMaxSize {
		t.Errorf("len(probeCache) = %d, want <= %d (hard cap not enforced)", got, probeTracksCacheMaxSize)
	}
}

// TestTracksCacheHardCap mirrors TestProbeCacheHardCap for p.tracksCache.
func TestTracksCacheHardCap(t *testing.T) {
	p := newTestProber()
	notExpired := time.Now().Add(time.Hour)
	for i := range probeTracksCacheMaxSize {
		p.tracksCache[fmt.Sprintf("http://127.0.0.1:1/pre%d", i)] = tracksCacheEntry{expiresAt: notExpired}
	}

	if _, err := p.Tracks("http://127.0.0.1:1/new"); err == nil {
		t.Fatal("Tracks against a refused port unexpectedly succeeded")
	}

	if got := len(p.tracksCache); got > probeTracksCacheMaxSize {
		t.Errorf("len(tracksCache) = %d, want <= %d (hard cap not enforced)", got, probeTracksCacheMaxSize)
	}
}

// TestProbeAppliesLocalize is the regression test for Probe() skipping the
// localize() rewrite: a mediaURL of the self-signed https://…:12470 UI-origin
// form must reach ffprobe already rewritten to http://…:11470 — exactly like
// Tracks() and every hls.go ffmpeg/ffprobe call already do — otherwise
// ffprobe attempts a real TLS handshake against this server's self-signed
// cert and always fails for exactly the self-reference case the selfBase
// exemption in validateRemoteURL was written for.
//
// A PATH-shim stub replaces the real ffprobe binary so the rewritten URL can
// be asserted directly from the argv it was invoked with, without spawning
// real ffprobe or any network I/O.
func TestProbeAppliesLocalize(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffprobe stub is a /bin/sh script")
	}
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	stub := filepath.Join(dir, "ffprobe")
	script := "#!/bin/sh\necho \"$@\" > \"" + argsFile + "\"\necho '{}'\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	p := newTestProber()
	p.baseURLLocal = "http://127.0.0.1:11470"
	raw := "https://127.0.0.1:12470" + testSelfPath

	if _, err := p.Probe(raw); err != nil {
		t.Fatalf("Probe with stub ffprobe: %v", err)
	}

	got, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("stub ffprobe was never invoked: %v", err)
	}
	argv := string(got)
	want := localize(raw)
	if !strings.Contains(argv, want) {
		t.Errorf("ffprobe argv = %q, want it to contain the localized URL %q", argv, want)
	}
	if strings.Contains(argv, "12470") {
		t.Errorf("ffprobe argv = %q, still contains the raw un-localized :12470 URL", argv)
	}
}

// TestProbeRoutesRemoteURLThroughRelay is the regression test for SEC-4: a
// remote (non-selfBase) streamURL must never reach ffprobe as the literal
// argv value — it must be rewritten to a "http://127.0.0.1:<port>/r/<token>"
// loopback relay URL, with a narrow "-protocol_whitelist http,tcp" (the
// relay only ever speaks plain loopback HTTP). A PATH-stub stands in for
// ffprobe so the argv can be asserted directly, without spawning real
// ffprobe or any network I/O.
func TestProbeRoutesRemoteURLThroughRelay(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffprobe stub is a /bin/sh script")
	}
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	stub := filepath.Join(dir, "ffprobe")
	script := "#!/bin/sh\necho \"$@\" > \"" + argsFile + "\"\necho '{}'\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	p := newTestProber()
	p.baseURLLocal = "http://127.0.0.1:11470"
	raw := "http://93.184.216.34/videos/movie.mkv"

	if _, err := p.Probe(raw); err != nil {
		t.Fatalf("Probe with stub ffprobe: %v", err)
	}

	got, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("stub ffprobe was never invoked: %v", err)
	}
	argv := string(got)
	if strings.Contains(argv, "93.184.216.34") {
		t.Errorf("ffprobe argv = %q, must never contain the raw remote host directly", argv)
	}
	if !strings.Contains(argv, "http://127.0.0.1:") || !strings.Contains(argv, "/r/") {
		t.Errorf("ffprobe argv = %q, want a loopback relay token URL", argv)
	}
	if !strings.Contains(argv, "-protocol_whitelist http,tcp") {
		t.Errorf("ffprobe argv = %q, want the narrow \"-protocol_whitelist http,tcp\" for a relayed input", argv)
	}
}

// TestProbeCapsFfprobeOutput is the regression test for MED-1: ffprobe
// stdout must be capped instead of buffered without bound. A PATH-stub
// replaces ffprobe with a program that floods stdout past ffprobeOutputLimit;
// Probe must fail with errOutputTooLarge rather than allocate an
// unbounded amount of memory reading it all in.
func TestProbeCapsFfprobeOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("PATH-shim ffprobe stub is a /bin/sh script")
	}
	dir := t.TempDir()
	stub := filepath.Join(dir, "ffprobe")
	// dd floods stdout with well over ffprobeOutputLimit (8 MiB) of zero
	// bytes; runCapped must abort the read (and kill the process) instead of
	// buffering all of it.
	script := "#!/bin/sh\ndd if=/dev/zero bs=1M count=9 2>/dev/null\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	p := newTestProber()
	p.baseURLLocal = "http://127.0.0.1:11470"

	_, err := p.Probe("http://93.184.216.34/videos/movie.mkv")
	if err == nil {
		t.Fatal("expected an error from an oversized ffprobe output, got nil")
	}
	if !errors.Is(err, errOutputTooLarge) {
		t.Errorf("err = %v, want it to wrap errOutputTooLarge", err)
	}
}

// ── media.go: computeOpenSubHash ─────────────────────────────────────────────

func TestComputeOpenSubHash(t *testing.T) {
	// Regression constant: pre-computed with known inputs.
	//
	// size=1000, head=[1,0,0,0,0,0,0,0] (LE uint64=1),
	//            tail=[2,0,0,0,0,0,0,0] (LE uint64=2)
	// → h = 1000 + 1 + 2 = 1003 = 0x00000000000003eb
	const wantConst = uint64(1003)
	head := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	tail := []byte{2, 0, 0, 0, 0, 0, 0, 0}
	got := computeOpenSubHash(1000, head, tail)
	if got != wantConst {
		t.Errorf("regression constant failed: got %d (0x%016x), want %d", got, got, wantConst)
	}

	// Formatted output must be 16 lowercase hex digits.
	hex := fmt.Sprintf("%016x", got)
	if hex != "00000000000003eb" {
		t.Errorf("hex format: got %q, want %q", hex, "00000000000003eb")
	}

	// All-zero buffers: hash equals size.
	zeroHead := make([]byte, 8)
	zeroTail := make([]byte, 8)
	for _, size := range []int64{0, 1, 99, 65536, 1<<32 - 1} {
		h := computeOpenSubHash(size, zeroHead, zeroTail)
		if h != uint64(size) {
			t.Errorf("all-zero buffers with size=%d: got %d, want %d", size, h, size)
		}
	}

	// Size must contribute: same buffers, different sizes → different hashes.
	h1 := computeOpenSubHash(100, head, tail)
	h2 := computeOpenSubHash(101, head, tail)
	if h1 == h2 {
		t.Error("size does not contribute to hash: h(100)==h(101)")
	}

	// Buffer content must contribute.
	h3 := computeOpenSubHash(100, make([]byte, 8), make([]byte, 8))
	if h1 == h3 {
		t.Error("buffer content does not contribute to hash")
	}

	// Natural uint64 overflow must not panic.
	maxBuf := make([]byte, 8)
	for i := range maxBuf {
		maxBuf[i] = 0xff
	}
	_ = computeOpenSubHash(math.MaxInt64, maxBuf, maxBuf)
}

// ── media.go: OpenSubHash rejects non-http(s) input ──────────────────────────

// TestOpenSubHashRejectsLocalPath is the regression test for the local-file-read
// fix: OpenSubHash must never open a bare filesystem path or file:// URI —
// only http/https are supported, and that support was directly reachable from
// the unauthenticated /opensubHash?videoUrl= query parameter.
func TestOpenSubHashRejectsLocalPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "video.bin")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	p := &prober{}
	cases := []string{path, "file://" + path}
	for _, videoURL := range cases {
		if _, err := p.OpenSubHash(videoURL); err == nil {
			t.Errorf("OpenSubHash(%q) accepted a local path; want rejection", videoURL)
		}
	}
}

// ── media.go: OpenSubHash via HTTP (exercises fetchHTTPChunks/httpRangeGet) ──

func TestOpenSubHashHTTP(t *testing.T) {
	const fileSize = 200_000
	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i % 127)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// http.ServeContent handles HEAD, Range GET, and Content-Length automatically.
		http.ServeContent(w, r, "video.bin", time.Time{}, bytes.NewReader(content))
	}))
	defer ts.Close()
	base := stubOpenSubClientTransport(t, ts)

	p := &prober{}
	result, err := p.OpenSubHash(base + "/video.bin")
	if err != nil {
		t.Fatalf("OpenSubHash HTTP: %v", err)
	}
	m := result.(map[string]interface{})

	hash, _ := m["hash"].(string)
	size, _ := m["size"].(int64)

	if len(hash) != 16 {
		t.Errorf("hash len = %d, want 16", len(hash))
	}
	if size != fileSize {
		t.Errorf("size = %d, want %d", size, fileSize)
	}

	// Must agree with the local-file computation.
	wantHash := fmt.Sprintf("%016x", computeOpenSubHash(
		int64(fileSize),
		content[:chunkSize],
		content[fileSize-chunkSize:],
	))
	if hash != wantHash {
		t.Errorf("HTTP hash = %q, want %q (from local computation)", hash, wantHash)
	}
}

// TestOpenSubHashAppliesLocalize is the regression test for OpenSubHash()
// skipping the localize() rewrite: a videoURL of the self-signed
// https://…:12470 UI-origin form must be rewritten to http://…:11470 before
// it reaches fetchHTTPChunks' plain net/http client — exactly like Tracks()
// and every hls.go ffmpeg call already do. stubOpenSubClientTransport
// redirects openSubClient's dial target to a plain-HTTP httptest.Server
// regardless of the request's declared host, so the https scheme alone is
// enough to prove the point: without localize(), OpenSubHash would attempt a
// real TLS handshake against that plain-HTTP listener and fail outright.
func TestOpenSubHashAppliesLocalize(t *testing.T) {
	const fileSize = 4096
	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "video.bin", time.Time{}, bytes.NewReader(content))
	}))
	defer ts.Close()
	stubOpenSubClientTransport(t, ts)

	p := &prober{baseURLLocal: "http://127.0.0.1:11470"}
	// Raw self-signed UI-origin form; validateRemoteURL's selfBase exemption
	// allows it via its own internal localize() check, but OpenSubHash must
	// also localize() before calling fetchHTTPChunks.
	videoURL := "https://127.0.0.1:12470" + testSelfPath

	result, err := p.OpenSubHash(videoURL)
	if err != nil {
		t.Fatalf("OpenSubHash did not apply localize() before fetching: %v", err)
	}
	m := result.(map[string]interface{})
	if size, _ := m["size"].(int64); size != fileSize {
		t.Errorf("size = %v, want %d", m["size"], fileSize)
	}
}

// ── subtitles.go: pure helpers ───────────────────────────────────────────────

func TestFmtTimestamp(t *testing.T) {
	cases := []struct {
		ms    int
		isVTT bool
		want  string
	}{
		{0, false, "00:00:00,000"},
		{0, true, "00:00:00.000"},
		{1000, false, "00:00:01,000"},
		{1001, false, "00:00:01,001"},
		{60000, false, "00:01:00,000"},
		{3600000, false, "01:00:00,000"},
		{3661500, false, "01:01:01,500"},
		{3661500, true, "01:01:01.500"},
		{-1, false, "00:00:00,000"}, // clamped to 0
		{999, true, "00:00:00.999"},
	}
	for _, tc := range cases {
		got := fmtTimestamp(tc.ms, tc.isVTT)
		if got != tc.want {
			t.Errorf("fmtTimestamp(%d, isVTT=%v) = %q, want %q", tc.ms, tc.isVTT, got, tc.want)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	cases := []struct {
		s    string
		want int
	}{
		{"00:00:01,000", 1000},
		{"00:00:01.000", 1000},    // VTT dot separator
		{"01:00:00,000", 3600000}, // one hour
		{"01:02:03,456", 3723456},
		{"02:30,500", 150500},             // MM:SS,mmm form
		{"00:00:01,000 align:left", 1000}, // WEBVTT cue settings discarded
	}
	for _, tc := range cases {
		got := parseTimestamp(tc.s)
		if got != tc.want {
			t.Errorf("parseTimestamp(%q) = %d, want %d", tc.s, got, tc.want)
		}
	}
}

func TestParseSecMs(t *testing.T) {
	cases := []struct {
		s       string
		wantSec int
		wantMs  int
	}{
		{"01.500", 1, 500},
		{"00.000", 0, 0},
		{"59.999", 59, 999},
		{"10", 10, 0}, // no decimal part
	}
	for _, tc := range cases {
		sec, ms := parseSecMs(tc.s)
		if sec != tc.wantSec || ms != tc.wantMs {
			t.Errorf("parseSecMs(%q) = (%d, %d), want (%d, %d)", tc.s, sec, ms, tc.wantSec, tc.wantMs)
		}
	}
}

func TestParseTimestampRoundTrip(t *testing.T) {
	// fmtTimestamp and parseTimestamp must be inverses for valid timestamps.
	for _, ms := range []int{0, 1, 999, 1000, 3661500, 86399999} {
		for _, isVTT := range []bool{false, true} {
			formatted := fmtTimestamp(ms, isVTT)
			got := parseTimestamp(formatted)
			if got != ms {
				t.Errorf("round-trip (isVTT=%v): ms=%d → %q → %d", isVTT, ms, formatted, got)
			}
		}
	}
}

func TestParseASSTimestamp(t *testing.T) {
	cases := []struct {
		s    string
		want int
	}{
		{"0:00:01.00", 1000},    // 1 second, 0 centiseconds
		{"0:01:00.00", 60000},   // 1 minute
		{"1:00:00.00", 3600000}, // 1 hour
		{"0:00:00.50", 500},     // 50 centiseconds = 500 ms
		{"0:00:01.25", 1250},    // 1s + 25cs = 1250 ms
		{"2:30:45.12", 9045120}, // 2h+30m+45s+12cs = 9045120 ms
	}
	for _, tc := range cases {
		got := parseASSTimestamp(tc.s)
		if got != tc.want {
			t.Errorf("parseASSTimestamp(%q) = %d, want %d", tc.s, got, tc.want)
		}
	}
}

func TestStripASSMarkup(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"{\\an8}Hello", "Hello"},                        // positional tag
		{"{\\b1}bold{\\b0}", "bold"},                     // bold on/off
		{"Hello\\NWorld", "Hello\nWorld"},                // soft break \N
		{"Hello\\nWorld", "Hello\nWorld"},                // soft break \n
		{"No tags here", "No tags here"},                 // passthrough
		{"{unclosed", ""},                                // unclosed tag: skip to end
		{"{\\an8}Hi\\NThere{\\c&HFF0000&}", "Hi\nThere"}, // mixed
		{"\\\\escaped", "\\\\escaped"},                   // non-N backslash passthrough
		{"", ""},
	}
	for _, tc := range cases {
		got := stripASSMarkup(tc.in)
		if got != tc.want {
			t.Errorf("stripASSMarkup(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestLooksLikeBinary(t *testing.T) {
	// Text content: no null bytes → not binary.
	if looksLikeBinary([]byte("1\n00:00:01,000 --> 00:00:02,000\nHello")) {
		t.Error("plain SRT text incorrectly flagged as binary")
	}
	// Null byte → binary.
	if !looksLikeBinary([]byte("PGS\x00data")) {
		t.Error("data with null byte should be flagged as binary")
	}
	// Null byte beyond first 512 bytes → not flagged (check window is 512 bytes).
	data := make([]byte, 600)
	for i := range data {
		data[i] = 'A'
	}
	data[513] = 0x00 // outside the check window
	if looksLikeBinary(data) {
		t.Error("null byte after 512 bytes should not trigger binary flag")
	}
	// Null byte at exactly byte 511 (within window) → binary.
	data2 := make([]byte, 600)
	for i := range data2 {
		data2[i] = 'B'
	}
	data2[511] = 0x00
	if !looksLikeBinary(data2) {
		t.Error("null byte at index 511 should trigger binary flag")
	}
}

func TestIsASS(t *testing.T) {
	assScriptInfo := "[Script Info]\nScriptType: v4.00+\n[Events]\n"
	assEvents := "[Events]\nFormat: Layer, Start, End, Style, Name\n"
	ssaScript := "ScriptType: v4.00\n"
	notASS := "1\n00:00:01,000 --> 00:00:02,000\nHello"

	if !isASS(assScriptInfo) {
		t.Error("[Script Info] should be detected as ASS")
	}
	if !isASS(assEvents) {
		t.Error("[Events] should be detected as ASS")
	}
	if !isASS(ssaScript) {
		t.Error("ScriptType: v4 should be detected as ASS")
	}
	if isASS(notASS) {
		t.Error("SRT content should not be detected as ASS")
	}
}

// ── subtitles.go: parseSubtitles ─────────────────────────────────────────────

func TestParseSubtitlesSRT(t *testing.T) {
	srt := "1\n00:00:01,000 --> 00:00:02,000\nHello World\n\n" +
		"2\n00:00:03,500 --> 00:00:05,000\nSecond cue\nwith two lines\n\n"

	tracks := parseSubtitles([]byte(srt))
	if len(tracks) != 2 {
		t.Fatalf("expected 2 tracks, got %d", len(tracks))
	}
	if tracks[0]["startTime"] != 1000 || tracks[0]["endTime"] != 2000 {
		t.Errorf("track 0 timestamps wrong: %v", tracks[0])
	}
	if tracks[0]["text"] != "Hello World" {
		t.Errorf("track 0 text = %q", tracks[0]["text"])
	}
	if tracks[1]["text"] != "Second cue\nwith two lines" {
		t.Errorf("track 1 text = %q", tracks[1]["text"])
	}
}

func TestParseSubtitlesVTT(t *testing.T) {
	vtt := "WEBVTT\n\n" +
		"00:00:01.000 --> 00:00:02.500\nHello VTT\n\n" +
		"NOTE this block has no cue text and should be skipped\n\n" +
		"00:00:05.000 --> 00:00:06.000 align:left\nCue with settings\n\n"

	tracks := parseSubtitles([]byte(vtt))
	if len(tracks) != 2 {
		t.Fatalf("expected 2 tracks, got %d: %v", len(tracks), tracks)
	}
	if tracks[0]["startTime"] != 1000 || tracks[0]["endTime"] != 2500 {
		t.Errorf("track 0 timestamps wrong: %v", tracks[0])
	}
	if tracks[0]["text"] != "Hello VTT" {
		t.Errorf("track 0 text = %q", tracks[0]["text"])
	}
	// VTT cue settings (align:left) must be discarded from the timestamp.
	if tracks[1]["startTime"] != 5000 {
		t.Errorf("track 1 startTime = %v, want 5000", tracks[1]["startTime"])
	}
}

func TestParseSubtitlesBOM(t *testing.T) {
	// UTF-8 BOM prefix must be stripped.
	srt := "\xEF\xBB\xBF1\n00:00:00,100 --> 00:00:01,000\nBOM test\n\n"
	tracks := parseSubtitles([]byte(srt))
	if len(tracks) != 1 {
		t.Fatalf("expected 1 track, got %d", len(tracks))
	}
}

func TestParseSubtitlesBinary(t *testing.T) {
	// Null byte in data → treated as binary → returns nil.
	binary := []byte("PGS\x00binary subtitle payload")
	tracks := parseSubtitles(binary)
	if tracks != nil {
		t.Errorf("binary data should return nil tracks, got %v", tracks)
	}
}

func TestParseSubtitlesEmpty(t *testing.T) {
	tracks := parseSubtitles([]byte(""))
	if len(tracks) != 0 {
		t.Errorf("empty input: expected 0 tracks, got %d", len(tracks))
	}
}

// ── subtitles.go: parseASSSubtitles ──────────────────────────────────────────

func TestParseASSSubtitles(t *testing.T) {
	ass := `[Script Info]
ScriptType: v4.00+

[V4+ Styles]
Format: Name, Fontname, Fontsize
Style: Default,Arial,20

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
Dialogue: 0,0:00:01.00,0:00:02.50,Default,,0,0,0,,Hello {\\b1}World
Dialogue: 0,0:00:05.00,0:00:06.00,Default,,0,0,0,,Second\\Nline
`

	tracks := parseASSSubtitles(ass)
	if len(tracks) != 2 {
		t.Fatalf("expected 2 tracks, got %d: %v", len(tracks), tracks)
	}
	if tracks[0]["startTime"] != 1000 || tracks[0]["endTime"] != 2500 {
		t.Errorf("track 0 timestamps: start=%v end=%v", tracks[0]["startTime"], tracks[0]["endTime"])
	}
	text0, _ := tracks[0]["text"].(string)
	if !strings.Contains(text0, "Hello") || !strings.Contains(text0, "World") {
		t.Errorf("track 0 text = %q, want to contain 'Hello World'", text0)
	}
	text1, _ := tracks[1]["text"].(string)
	if !strings.Contains(text1, "\n") {
		t.Errorf("track 1 text = %q, want soft line break converted to newline", text1)
	}
}

// TestParseASSSubtitlesNonCanonicalFormat is the regression test for BUG-1:
// a Format: line whose Start/End columns come after Text (so the field that
// SplitN lets absorb trailing commas is not actually Text) must never panic,
// and must simply skip every Dialogue line rather than emit
// wrongly-extracted text. A Format: line that reorders columns but still
// keeps Text last must continue to parse correctly, absorbing commas inside
// the subtitle text exactly as the canonical v4.00+ order does.
func TestParseASSSubtitlesNonCanonicalFormat(t *testing.T) {
	t.Run("Text before Start/End panics without the bounds fix", func(t *testing.T) {
		// The exact crash repro from the review report: nFields used to be
		// textIdx+1 (=2) here, so parts[startIdx=2] and parts[endIdx=3] read
		// past a 2-element slice ("index out of range [2] with length 2").
		ass := "[Events]\n" +
			"Format: Layer, Text, Start, End\n" +
			"Dialogue: 0,Hello World,0:00:01.00,0:00:02.00\n"

		tracks := parseASSSubtitles(ass) // must not panic
		if len(tracks) != 0 {
			t.Errorf("expected 0 tracks (Text not last => line skipped safely), got %d: %v", len(tracks), tracks)
		}
	})

	t.Run("reordered columns with Text last parse correctly", func(t *testing.T) {
		ass := "[Events]\n" +
			"Format: Start, End, Name, Text\n" +
			"Dialogue: 0:00:01.00,0:00:02.50,Default,Hello, World\n"

		tracks := parseASSSubtitles(ass)
		if len(tracks) != 1 {
			t.Fatalf("expected 1 track, got %d: %v", len(tracks), tracks)
		}
		if tracks[0]["startTime"] != 1000 || tracks[0]["endTime"] != 2500 {
			t.Errorf("timestamps: start=%v end=%v", tracks[0]["startTime"], tracks[0]["endTime"])
		}
		if tracks[0]["text"] != "Hello, World" {
			t.Errorf("text = %q, want %q (Text as the last field must still absorb the comma)", tracks[0]["text"], "Hello, World")
		}
	})
}

func TestParseSubtitlesASS(t *testing.T) {
	// parseSubtitles auto-dispatches to parseASSSubtitles for ASS content.
	ass := "[Script Info]\nScriptType: v4.00+\n\n[Events]\n" +
		"Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n" +
		"Dialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,Via parseSubtitles\n"

	tracks := parseSubtitles([]byte(ass))
	if len(tracks) != 1 {
		t.Fatalf("expected 1 track, got %d", len(tracks))
	}
}

// ── subtitles.go: fetchSubBytes and friends (via httptest) ───────────────────

func TestFetchSubBytesRejectsNonHTTP(t *testing.T) {
	_, err := fetchSubBytes("/local/path.srt", "")
	if err == nil {
		t.Error("fetchSubBytes should reject non-http paths")
	}
	_, err = fetchSubBytes("file:///tmp/test.srt", "")
	if err == nil {
		t.Error("fetchSubBytes should reject file:// scheme")
	}
}

func TestSubtitlesTracks(t *testing.T) {
	srt := "1\n00:00:01,000 --> 00:00:03,000\nTest cue\n\n"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, srt)
	}))
	defer ts.Close()
	base := stubOpenSubClientTransport(t, ts)

	p := &prober{}
	result, err := p.SubtitlesTracks(base + "/sub.srt")
	if err != nil {
		t.Fatalf("SubtitlesTracks: %v", err)
	}
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("result type = %T", result)
	}
	tracksRaw, ok := m["tracks"]
	if !ok {
		t.Fatal("result missing 'tracks' key")
	}
	tracks, ok := tracksRaw.([]map[string]interface{})
	if !ok {
		t.Fatalf("tracks type = %T", tracksRaw)
	}
	if len(tracks) != 1 {
		t.Fatalf("expected 1 track, got %d", len(tracks))
	}
	if tracks[0]["startTime"] != 1000 || tracks[0]["endTime"] != 3000 {
		t.Errorf("track timestamps: %v", tracks[0])
	}
}

func TestWriteSubtitlesSRT(t *testing.T) {
	srt := "1\n00:00:01,000 --> 00:00:02,000\nHello & World\n\n"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, srt)
	}))
	defer ts.Close()
	base := stubOpenSubClientTransport(t, ts)

	p := &prober{}
	var buf strings.Builder
	if err := p.WriteSubtitles(&buf, base+"/sub.srt", "srt", 0); err != nil {
		t.Fatalf("WriteSubtitles: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "00:00:01,000 --> 00:00:02,000") {
		t.Errorf("SRT output missing timestamp:\n%s", out)
	}
	// Ampersands must be escaped as &amp;
	if !strings.Contains(out, "Hello &amp; World") {
		t.Errorf("SRT output: ampersand not escaped:\n%s", out)
	}
}

func TestWriteSubtitlesVTT(t *testing.T) {
	srt := "1\n00:00:02,000 --> 00:00:03,500\nVTT cue\n\n"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, srt)
	}))
	defer ts.Close()
	base := stubOpenSubClientTransport(t, ts)

	p := &prober{}
	var buf strings.Builder
	if err := p.WriteSubtitles(&buf, base+"/sub.srt", "vtt", 0); err != nil {
		t.Fatalf("WriteSubtitles VTT: %v", err)
	}

	out := buf.String()
	if !strings.HasPrefix(out, "WEBVTT") {
		t.Errorf("VTT output must start with WEBVTT:\n%s", out)
	}
	// VTT uses dot separator, not comma.
	if !strings.Contains(out, "00:00:02.000 --> 00:00:03.500") {
		t.Errorf("VTT timestamp format wrong:\n%s", out)
	}
}

func TestWriteSubtitlesOffset(t *testing.T) {
	// 500ms offset added to both timestamps.
	srt := "1\n00:00:01,000 --> 00:00:02,000\nOffset test\n\n"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, srt)
	}))
	defer ts.Close()
	base := stubOpenSubClientTransport(t, ts)

	p := &prober{}
	var buf strings.Builder
	if err := p.WriteSubtitles(&buf, base+"/sub.srt", "srt", 500); err != nil {
		t.Fatalf("WriteSubtitles offset: %v", err)
	}

	out := buf.String()
	// 1000 + 500 = 1500ms, 2000 + 500 = 2500ms
	if !strings.Contains(out, "00:00:01,500 --> 00:00:02,500") {
		t.Errorf("offset not applied correctly:\n%s", out)
	}
}

func TestWriteSubtitlesNegativeOffset(t *testing.T) {
	// Negative offset clamped to 0.
	srt := "1\n00:00:00,500 --> 00:00:01,000\nNeg test\n\n"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, srt)
	}))
	defer ts.Close()
	base := stubOpenSubClientTransport(t, ts)

	p := &prober{}
	var buf strings.Builder
	if err := p.WriteSubtitles(&buf, base+"/sub.srt", "srt", -1000); err != nil {
		t.Fatalf("WriteSubtitles negative offset: %v", err)
	}

	out := buf.String()
	// 500 - 1000 = -500 → clamped to 0; 1000 - 1000 = 0
	if !strings.Contains(out, "00:00:00,000 --> 00:00:00,000") {
		t.Errorf("negative offset not clamped to 0:\n%s", out)
	}
}

// ── Extras: strconv helpers used in WriteSubtitles (errWriter) ───────────────

func TestErrWriterStickyError(t *testing.T) {
	// errWriter must not overwrite the first error.
	ew := &errWriter{w: &failWriter{}, err: nil}
	ew.printf("test %d", 1)
	if ew.err == nil {
		t.Fatal("errWriter should have captured write error")
	}
	first := ew.err
	ew.printf("another write") // must no-op after first error
	if !errors.Is(ew.err, first) {
		t.Error("errWriter replaced error on second write")
	}
}

// failWriter always returns an error on Write.
type failWriter struct{}

func (f *failWriter) Write(_ []byte) (int, error) {
	return 0, fmt.Errorf("write failed")
}

// ── Extras: strconv helpers sanity ───────────────────────────────────────────

func TestFtoaBasic(t *testing.T) {
	got := ftoa(4.123456789)
	// strconv.FormatFloat with 'f', prec=3 → "4.123"
	want := strconv.FormatFloat(4.123456789, 'f', 3, 64)
	if got != want {
		t.Errorf("ftoa mismatch: got %q, want %q", got, want)
	}
}

// TestWriteSubtitlesUpstreamStatus: a 404/410 source matches
// types.ErrSubtitleNotFound, any other non-2xx status is a plain error, and
// neither is turned into an empty-but-successful conversion.
func TestWriteSubtitlesUpstreamStatus(t *testing.T) {
	for _, tc := range []struct {
		status   int
		notFound bool
	}{
		{http.StatusNotFound, true},
		{http.StatusGone, true},
		{http.StatusInternalServerError, false},
		{http.StatusForbidden, false},
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = fmt.Fprint(w, "1\n00:00:01,000 --> 00:00:02,000\nnot a subtitle\n\n")
		}))
		base := stubOpenSubClientTransport(t, ts)
		p := &prober{}
		var buf strings.Builder
		err := p.WriteSubtitles(&buf, base+"/sub.srt", "srt", 0)
		ts.Close()
		if err == nil {
			t.Errorf("status %d: WriteSubtitles succeeded, want an error", tc.status)
			continue
		}
		if got := errors.Is(err, types.ErrSubtitleNotFound); got != tc.notFound {
			t.Errorf("status %d: errors.Is(ErrSubtitleNotFound) = %v, want %v (err %v)", tc.status, got, tc.notFound, err)
		}
	}
}
