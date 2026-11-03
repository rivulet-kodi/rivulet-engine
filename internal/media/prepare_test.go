// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Tests for engine-side preparation (prepare.go) and the low-priority lane
// of slotSem it runs through. All offline: sessions are injected directly
// and the job's transcodes go through the hlsManager.transcode seam, which
// writes a fake segment file instead of running ffmpeg.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// newPrepManager returns a test manager with preparation on.
func newPrepManager(t *testing.T) *hlsManager {
	t.Helper()
	m := newTestHLSManager(t)
	m.cfg.Prepare = true
	return m
}

// injectPrepSession registers a session of dur seconds named id; streams
// non-nil with two or more entries makes it multi-audio.
func injectPrepSession(t *testing.T, m *hlsManager, id string, dur float64, streams []audioStream) *hlsSession {
	t.Helper()
	dir := filepath.Join(m.base, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &hlsSession{
		dir:          dir,
		duration:     dur,
		audioStreams: streams,
		multiAudio:   len(streams) >= 2,
		segLocks:     map[string]*sync.Mutex{},
		playlistData: map[string]struct{}{},
		tc:           m.effectiveSessionConfig(sessionOverrides{}),
	}
	s.lastAccess.Store(time.Now().UnixNano())
	m.mu.Lock()
	m.sessions[id] = s
	m.mu.Unlock()
	return s
}

// writeFakeSegment caches a fake segment file in s, as a successful
// transcode would.
func writeFakeSegment(t *testing.T, s *hlsSession, name string) string {
	t.Helper()
	p := filepath.Join(s.dir, name)
	if err := os.WriteFile(p, []byte("segment"), 0o644); err != nil {
		t.Error(err)
	}
	s.noteSegment(name, int64(len("segment")), time.Now())
	return p
}

func segName(n int, kind segKind, audioIdx int) string {
	if kind == segAudioOnly {
		return fmt.Sprintf("a%dseg%d.ts", audioIdx, n)
	}
	return fmt.Sprintf("seg%d.ts", n)
}

// fakeTranscoder records the segments it was asked for and caches each one.
type fakeTranscoder struct {
	t     *testing.T
	mu    sync.Mutex
	calls []string
	bg    []bool
}

func (f *fakeTranscoder) fn(_ context.Context, s *hlsSession, n int, kind segKind, audioIdx int, bg bool) (string, error) {
	name := segName(n, kind, audioIdx)
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.bg = append(f.bg, bg)
	f.mu.Unlock()
	return writeFakeSegment(f.t, s, name), nil
}

func (f *fakeTranscoder) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// waitPrepState polls id's status until its state is want.
func waitPrepState(t *testing.T, m *hlsManager, id, want string) types.HLSPrepareStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := m.HLSPrepareStatus(id)
		if err != nil {
			t.Fatalf("HLSPrepareStatus(%q) = %v", id, err)
		}
		if st.State == want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("state = %q, want %q (status %+v)", st.State, want, st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitInFlight polls until s has n in-flight holds.
func waitInFlight(t *testing.T, s *hlsSession, n int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.inFlight.Load() != n {
		if time.Now().After(deadline) {
			t.Fatalf("inFlight = %d, want %d", s.inFlight.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

var twoAudio = []audioStream{{Index: 1, Language: "eng"}, {Index: 2, Language: "ita", IsDefault: true}}

func TestPrepPlan(t *testing.T) {
	cases := []struct {
		name      string
		multi     bool
		streams   []audioStream
		audio     string
		wantPls   []string
		pinned    []string
		notPinned []string
	}{
		{"single audio", false, nil, prepAudioDefault, []string{"playlist.m3u8"},
			[]string{"seg0.ts", "seg12.ts"}, []string{"a0seg0.ts", "sub0.vtt", "playlist.m3u8"}},
		{"multi audio, default", true, twoAudio, prepAudioDefault, []string{"video.m3u8", "audio1.m3u8"},
			[]string{"seg3.ts", "a1seg3.ts"}, []string{"a0seg3.ts", "a10seg3.ts", "aseg3.ts"}},
		{"multi audio, all", true, twoAudio, prepAudioAll, []string{"video.m3u8", "audio0.m3u8", "audio1.m3u8"},
			[]string{"seg3.ts", "a0seg3.ts", "a1seg3.ts"}, []string{"a2seg3.ts"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rends, pins := prepPlan(c.multi, c.streams, c.audio)
			var pls []string
			for _, r := range rends {
				pls = append(pls, r.playlist)
			}
			if strings.Join(pls, ",") != strings.Join(c.wantPls, ",") {
				t.Errorf("playlists = %v, want %v", pls, c.wantPls)
			}
			for _, n := range c.pinned {
				if !pins.has(n) {
					t.Errorf("%s not pinned", n)
				}
			}
			for _, n := range c.notPinned {
				if pins.has(n) {
					t.Errorf("%s pinned", n)
				}
			}
		})
	}
	var none *pinSet
	if none.has("seg0.ts") {
		t.Error("a nil pinSet pins seg0.ts")
	}
}

// Pinned segments survive eviction and don't count against the cap; the cap
// still applies to the unpinned ones.
func TestEvictSegmentsSkipsPinned(t *testing.T) {
	m := newTestHLSManager(t)
	s := injectPrepSession(t, m, "p", 40, twoAudio)
	old := time.Now().Add(-time.Hour)
	for _, name := range []string{"seg0.ts", "seg1.ts", "a1seg0.ts", "a0seg0.ts", "a0seg1.ts"} {
		writeFakeSegment(t, s, name)
		s.noteSegment(name, 100, old)
	}
	_, pins := prepPlan(true, twoAudio, prepAudioDefault)
	s.pins.Store(pins)
	// 300 pinned bytes, 200 unpinned: a 150-byte cap must evict one
	// unpinned segment and keep every pinned one.
	s.evictSegments(150, "", time.Now())
	for _, name := range []string{"seg0.ts", "seg1.ts", "a1seg0.ts"} {
		if !segCached(s.dir, name) {
			t.Errorf("pinned %s was evicted", name)
		}
	}
	left := 0
	for _, name := range []string{"a0seg0.ts", "a0seg1.ts"} {
		if segCached(s.dir, name) {
			left++
		}
	}
	if left != 1 {
		t.Errorf("%d unpinned segments left, want 1", left)
	}
	// Unpinned, the same cap evicts down to one segment.
	s.pins.Store(nil)
	s.evictSegments(150, "", time.Now())
	if n := len(s.cachedSegments()); n != 1 {
		t.Errorf("%d segments cached after unpinning, want 1", n)
	}
}

func TestPrepareHLSPreparesEverySegment(t *testing.T) {
	m := newPrepManager(t)
	f := &fakeTranscoder{t: t}
	m.transcode = f.fn
	s := injectPrepSession(t, m, "film", 10, twoAudio) // 3 segments
	writeFakeSegment(t, s, "seg1.ts")                  // already cached: skipped

	st, err := m.PrepareHLS("film", "")
	if err != nil {
		t.Fatalf("PrepareHLS = %v", err)
	}
	if st.Audio != prepAudioDefault || st.Segments != 3 || st.Total != 6 {
		t.Errorf("initial status = %+v", st)
	}
	st = waitPrepState(t, m, "film", prepDone)
	want := []string{"seg0.ts", "a1seg0.ts", "a1seg1.ts", "seg2.ts", "a1seg2.ts"}
	if got := f.got(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("transcoded %v, want %v (playback order, cached skipped)", got, want)
	}
	for i, bg := range f.bg {
		if !bg {
			t.Errorf("transcode %d was not a background transcode", i)
		}
	}
	if st.Cached != 6 || st.Uncached != 0 || st.Failed != 0 || st.CachedBytes != 6*int64(len("segment")) {
		t.Errorf("done status = %+v", st)
	}
	if len(st.Renditions) != 2 || st.Renditions[0].Playlist != "video.m3u8" || st.Renditions[0].Map != "ccc" ||
		st.Renditions[1].Playlist != "audio1.m3u8" || st.Renditions[1].Map != "ccc" {
		t.Errorf("renditions = %+v", st.Renditions)
	}
	if st.EstimatedBytes <= 0 {
		t.Errorf("EstimatedBytes = %d", st.EstimatedBytes)
	}
	if s.pins.Load() == nil {
		t.Error("prepared segments are not pinned")
	}
	waitInFlight(t, s, 0)
}

// A job holds the session in flight, so the reaper can't evict it however
// stale it looks.
func TestPrepareHoldsSessionAgainstReaper(t *testing.T) {
	m := newPrepManager(t)
	release := make(chan struct{})
	m.transcode = func(ctx context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		return writeFakeSegment(t, s, segName(n, kind, k)), nil
	}
	s := injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", prepAudioDefault); err != nil {
		t.Fatal(err)
	}
	waitInFlight(t, s, 1)
	s.lastAccess.Store(time.Now().Add(-time.Hour).UnixNano())
	m.evictIdle()
	if m.Sessions() != 1 {
		t.Fatal("reaper evicted a session that is being prepared")
	}
	close(release)
	waitPrepState(t, m, "film", prepDone)
	waitInFlight(t, s, 0)
}

func TestPrepareHLSErrors(t *testing.T) {
	m := newPrepManager(t)
	m.transcode = (&fakeTranscoder{t: t}).fn
	injectPrepSession(t, m, "film", 8, nil)
	injectPrepSession(t, m, "noprobe", 0, nil)
	cases := []struct {
		id, audio string
		want      error
	}{
		{"film", "commentary", types.ErrInvalidHLSOption},
		{"a..b", "", types.ErrHLSInvalidSessionID},
		{"nope", "", types.ErrHLSSessionNotFound},
		{"noprobe", "", types.ErrHLSDurationUnknown},
	}
	for _, c := range cases {
		if _, err := m.PrepareHLS(c.id, c.audio); !errors.Is(err, c.want) {
			t.Errorf("PrepareHLS(%q, %q) = %v, want %v", c.id, c.audio, err, c.want)
		}
	}
	if _, err := m.HLSPrepareStatus("nope"); !errors.Is(err, types.ErrHLSSessionNotFound) {
		t.Errorf("HLSPrepareStatus(unknown) = %v", err)
	}
	if err := m.StopPrepareHLS("a..b"); !errors.Is(err, types.ErrHLSInvalidSessionID) {
		t.Errorf("StopPrepareHLS(invalid) = %v", err)
	}
}

// An explicit cache cap refuses a session whose prepared size exceeds it;
// the derived default cap does not.
func TestPrepareHLSRefusesOverExplicitCap(t *testing.T) {
	m := newPrepManager(t)
	m.transcode = (&fakeTranscoder{t: t}).fn
	injectPrepSession(t, m, "film", 3600, nil) // an hour at 8M + 192k ≈ 3.7 GB
	m.cfg.SegmentCacheBytes = 1 << 30
	if _, err := m.PrepareHLS("film", ""); !errors.Is(err, types.ErrHLSPrepareTooLarge) {
		t.Fatalf("PrepareHLS over a 1 GiB cap = %v, want ErrHLSPrepareTooLarge", err)
	}
	if st, _ := m.HLSPrepareStatus("film"); st.State != prepIdle {
		t.Errorf("state after refusal = %q, want idle", st.State)
	}
	m.cfg.SegmentCacheBytes = 0
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatalf("PrepareHLS with the derived cap = %v", err)
	}
	waitPrepState(t, m, "film", prepDone)
}

// Transcode errors are retried once after the first pass; a job stops as
// failed after prepMaxConsecutiveFailures errors in a row.
func TestPrepareHLSFailures(t *testing.T) {
	t.Run("retried once", func(t *testing.T) {
		m := newPrepManager(t)
		var seg1 atomic.Int32
		m.transcode = func(_ context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
			if n == 1 && seg1.Add(1) == 1 {
				return "", errors.New("hls: transcode seg1.ts: boom")
			}
			return writeFakeSegment(t, s, segName(n, kind, k)), nil
		}
		injectPrepSession(t, m, "film", 12, nil)
		if _, err := m.PrepareHLS("film", ""); err != nil {
			t.Fatal(err)
		}
		st := waitPrepState(t, m, "film", prepDone)
		if seg1.Load() != 2 || st.Cached != 3 || st.Failed != 0 || len(st.Issues) != 0 {
			t.Errorf("seg1 attempts = %d, status %+v", seg1.Load(), st)
		}
	})
	t.Run("stops after repeated errors", func(t *testing.T) {
		m := newPrepManager(t)
		var calls atomic.Int32
		m.transcode = func(context.Context, *hlsSession, int, segKind, int, bool) (string, error) {
			calls.Add(1)
			return "", errors.New("hls: transcode: source failed: 429")
		}
		injectPrepSession(t, m, "film", 40, nil)
		if _, err := m.PrepareHLS("film", ""); err != nil {
			t.Fatal(err)
		}
		st := waitPrepState(t, m, "film", prepFailed)
		if calls.Load() != prepMaxConsecutiveFailures || st.Failed != prepMaxConsecutiveFailures ||
			!strings.Contains(st.Error, "429") || st.Renditions[0].Map != "fff-------" {
			t.Errorf("calls = %d, status %+v", calls.Load(), st)
		}
		if len(st.Issues) != prepMaxConsecutiveFailures || st.Issues[0].File != "seg0.ts" {
			t.Errorf("issues = %+v", st.Issues)
		}
	})
}

// A segment that comes out uncached is transcoded once more and reported
// 'u' with its reason when it still isn't cached.
func TestPrepareHLSUncached(t *testing.T) {
	m := newPrepManager(t)
	var seg0 atomic.Int32
	m.transcode = func(_ context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
		name := segName(n, kind, k)
		if n != 0 {
			return writeFakeSegment(t, s, name), nil
		}
		seg0.Add(1)
		tmp := filepath.Join(s.dir, name+".tmp.ts")
		if err := os.WriteFile(tmp, []byte("short"), 0o644); err != nil {
			t.Error(err)
		}
		return s.installSegment(name, tmp, "video ends 1.2s early")
	}
	injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	st := waitPrepState(t, m, "film", prepDone)
	if seg0.Load() != 2 {
		t.Errorf("seg0 transcoded %d times, want 2", seg0.Load())
	}
	if st.Cached != 1 || st.Uncached != 1 || st.Renditions[0].Map != "uc" ||
		len(st.Issues) != 1 || st.Issues[0].Reason != "video ends 1.2s early" {
		t.Errorf("status = %+v", st)
	}
}

// A segment a live request is transcoding (errSegmentBusy) is revisited
// after the rest, and counts as prepared once the live request cached it.
func TestPrepareHLSRevisitsBusySegments(t *testing.T) {
	saved := prepBusyRetry
	prepBusyRetry = 10 * time.Millisecond
	t.Cleanup(func() { prepBusyRetry = saved })
	m := newPrepManager(t)
	var seg0 atomic.Int32
	m.transcode = func(_ context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
		name := segName(n, kind, k)
		if n == 0 && seg0.Add(1) == 1 {
			writeFakeSegment(t, s, name) // the live request finishes it
			return "", errSegmentBusy
		}
		return writeFakeSegment(t, s, name), nil
	}
	injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	st := waitPrepState(t, m, "film", prepDone)
	if seg0.Load() != 1 || st.Cached != 2 {
		t.Errorf("seg0 calls = %d (busy segment re-transcoded?), status %+v", seg0.Load(), st)
	}
}

// blockingTranscoder blocks every call until its context is done.
func blockingTranscoder(started chan<- struct{}) func(context.Context, *hlsSession, int, segKind, int, bool) (string, error) {
	return func(ctx context.Context, _ *hlsSession, _ int, _ segKind, _ int, _ bool) (string, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return "", ctx.Err()
	}
}

func TestStopPrepareHLS(t *testing.T) {
	m := newPrepManager(t)
	started := make(chan struct{}, 1)
	m.transcode = blockingTranscoder(started)
	s := injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := m.StopPrepareHLS("film"); err != nil {
		t.Fatalf("StopPrepareHLS = %v", err)
	}
	st, _ := m.HLSPrepareStatus("film")
	if st.State != prepIdle || st.Audio != "" {
		t.Errorf("status after stop = %+v", st)
	}
	if s.pins.Load() != nil {
		t.Error("segments still pinned after stop")
	}
	if s.inFlight.Load() != 0 {
		t.Errorf("inFlight = %d after stop, want 0", s.inFlight.Load())
	}
	if m.Sessions() != 1 {
		t.Error("stopping preparation removed the session")
	}
}

// A second POST with the same audio leaves the job alone; another audio
// selection replaces it.
func TestPrepareHLSRestart(t *testing.T) {
	m := newPrepManager(t)
	started := make(chan struct{}, 1)
	m.transcode = blockingTranscoder(started)
	s := injectPrepSession(t, m, "film", 8, twoAudio)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	<-started
	s.prep.mu.Lock()
	first := s.prep.done
	s.prep.mu.Unlock()
	if _, err := m.PrepareHLS("film", prepAudioDefault); err != nil {
		t.Fatal(err)
	}
	s.prep.mu.Lock()
	same := s.prep.done == first
	s.prep.mu.Unlock()
	if !same {
		t.Error("a repeated POST with the same audio restarted the job")
	}
	st, err := m.PrepareHLS("film", prepAudioAll)
	if err != nil {
		t.Fatal(err)
	}
	if st.Audio != prepAudioAll || st.Total != 6 {
		t.Errorf("status after switching to all = %+v", st)
	}
	select {
	case <-first:
	default:
		t.Error("the replaced job is still running")
	}
	if !s.pins.Load().has("a0seg0.ts") {
		t.Error("audio0 not pinned after switching to all")
	}
	if err := m.StopPrepareHLS("film"); err != nil {
		t.Fatal(err)
	}
}

// DeleteHLS stops a running job; the session directory goes once the job
// has exited.
func TestDeleteHLSStopsPreparation(t *testing.T) {
	m := newPrepManager(t)
	started := make(chan struct{}, 1)
	m.transcode = blockingTranscoder(started)
	s := injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := m.DeleteHLS("film"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for dirExists(s.dir) {
		if time.Now().After(deadline) {
			t.Fatal("session dir not removed after DeleteHLS stopped the job")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := m.HLSPrepareStatus("film"); !errors.Is(err, types.ErrHLSSessionNotFound) {
		t.Errorf("status after delete = %v", err)
	}
}

// CloseHLS (closing stopCh) stops running jobs.
func TestCloseHLSStopsPreparation(t *testing.T) {
	m := newPrepManager(t)
	started := make(chan struct{}, 1)
	m.transcode = blockingTranscoder(started)
	s := injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	<-started
	s.prep.mu.Lock()
	done := s.prep.done
	s.prep.mu.Unlock()
	m.CloseHLS()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("job still running after CloseHLS")
	}
}

// With persistence on, the request is kept in prepare.json and the job
// resumes for a restored session; stopping removes the file.
func TestPreparePersistAndResume(t *testing.T) {
	m := newPrepManager(t)
	m.persist = true
	m.transcode = (&fakeTranscoder{t: t}).fn
	s := injectPrepSession(t, m, "film", 8, twoAudio)
	if _, err := m.PrepareHLS("film", prepAudioAll); err != nil {
		t.Fatal(err)
	}
	waitPrepState(t, m, "film", prepDone)
	audio, ok, err := readPrepFile(s.dir)
	if err != nil || !ok || audio != prepAudioAll {
		t.Fatalf("readPrepFile = %q, %v, %v", audio, ok, err)
	}

	// A "restarted" manager over the same directory resumes the job.
	m2 := newPrepManager(t)
	m2.base = m.base
	m2.persist = true
	f2 := &fakeTranscoder{t: t}
	m2.transcode = f2.fn
	s2 := injectPrepSession(t, m2, "film", 8, twoAudio)
	if err := os.Remove(filepath.Join(s2.dir, "a0seg1.ts")); err != nil {
		t.Fatal(err)
	}
	m2.resumePrepared()
	st := waitPrepState(t, m2, "film", prepDone)
	if st.Audio != prepAudioAll || st.Cached != 6 {
		t.Errorf("resumed status = %+v", st)
	}
	if got := f2.got(); len(got) != 1 || got[0] != "a0seg1.ts" {
		t.Errorf("resumed job transcoded %v, want only the missing a0seg1.ts", got)
	}

	if err := m2.StopPrepareHLS("film"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := readPrepFile(s2.dir); ok {
		t.Error("prepare.json still present after stop")
	}
}

func TestReadPrepFileRejectsInvalid(t *testing.T) {
	for _, body := range []string{`{"audio":"commentary"}`, `{"audio":"all","x":1}`, `not json`, `{"audio":"` + strings.Repeat("a", prepMaxFileSize) + `"}`} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, prepFileName), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := readPrepFile(dir); err == nil || ok {
			t.Errorf("readPrepFile(%.40q) = ok %v, err %v; want an error", body, ok, err)
		}
	}
	// An invalid file is discarded on resume rather than retried forever.
	m := newPrepManager(t)
	s := injectPrepSession(t, m, "film", 8, nil)
	if err := os.WriteFile(filepath.Join(s.dir, prepFileName), []byte(`{"audio":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	m.resumePrepared()
	if _, err := os.Stat(filepath.Join(s.dir, prepFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("invalid prepare.json not removed: %v", err)
	}
}

// The status of a session no job was started for covers the default plan,
// so a caller can see what is cached already.
func TestHLSPrepareStatusIdle(t *testing.T) {
	m := newPrepManager(t)
	s := injectPrepSession(t, m, "film", 9, nil)
	writeFakeSegment(t, s, "seg2.ts")
	before := s.lastAccess.Load()
	st, err := m.HLSPrepareStatus("film")
	if err != nil {
		t.Fatal(err)
	}
	if st.State != prepIdle || st.Audio != "" || st.Segments != 3 || st.Cached != 1 ||
		len(st.Renditions) != 1 || st.Renditions[0].Map != "--c" {
		t.Errorf("idle status = %+v", st)
	}
	if s.lastAccess.Load() != before {
		t.Error("a status request counted as an access")
	}
}

// A background transcode leaves a segment that a live request is working on
// alone, and takes a cached one from the cache, without running ffmpeg.
func TestTranscodeSegmentBackground(t *testing.T) {
	m := newPrepManager(t)
	s := injectPrepSession(t, m, "film", 8, nil)
	l := s.lockFor("seg0.ts")
	l.Lock()
	_, err := m.transcodeSegment(context.Background(), s, 0, segMuxed, 0, true)
	l.Unlock()
	if !errors.Is(err, errSegmentBusy) {
		t.Errorf("background transcode of a locked segment = %v, want errSegmentBusy", err)
	}
	want := writeFakeSegment(t, s, "seg1.ts")
	if p, err := m.transcodeSegment(context.Background(), s, 1, segMuxed, 0, true); err != nil || p != want {
		t.Errorf("background transcode of a cached segment = %q, %v", p, err)
	}
	m.slots.mu.Lock()
	held, lowHeld := m.slots.held, m.slots.lowHeld
	m.slots.mu.Unlock()
	if held != 0 || lowHeld != 0 {
		t.Errorf("slots leaked: held %d, lowHeld %d", held, lowHeld)
	}
}

// waitLowWaiters spins (bounded) until the low-priority lane has n queued
// waiters.
func waitLowWaiters(t *testing.T, s *slotSem, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		got := s.low.Len()
		s.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("low waiters = %d, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// A low-priority waiter is served only after every regular waiter, and the
// lane never holds more than its limit.
func TestSlotSemLowPriority(t *testing.T) {
	limitFn := func() int { return 3 }
	var s slotSem
	bg := context.Background()
	for range 3 {
		if err := s.acquire(bg, limitFn); err != nil {
			t.Fatal(err)
		}
	}
	order := make(chan string, 3)
	go func() {
		if s.acquireLow(bg, limitFn, 1) == nil {
			order <- "low"
		}
	}()
	waitLowWaiters(t, &s, 1)
	go func() {
		if s.acquire(bg, limitFn) == nil {
			order <- "regular"
		}
	}()
	waitSemWaiters(t, &s, 1)
	s.release(limitFn)
	if got := <-order; got != "regular" {
		t.Fatalf("first grant went to %q, want the regular waiter", got)
	}
	s.release(limitFn)
	if got := <-order; got != "low" {
		t.Fatalf("second grant went to %q, want the low waiter", got)
	}
	// Free the last original slot: one is free, but the lane is at its
	// limit of 1.
	s.release(limitFn)
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	if err := s.acquireLow(ctx, limitFn, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second low acquire = %v, want it to wait past the lane limit", err)
	}
	// A regular acquire still gets the free slot at once.
	if err := s.acquire(bg, limitFn); err != nil {
		t.Fatal(err)
	}
	s.releaseLow(limitFn)
	s.release(limitFn) // the regular waiter's slot
	s.release(limitFn) // the last regular acquire's
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held != 0 || s.lowHeld != 0 || s.low.Len() != 0 || s.waiters.Len() != 0 {
		t.Errorf("after releasing everything: held %d, lowHeld %d, low %d, waiters %d", s.held, s.lowHeld, s.low.Len(), s.waiters.Len())
	}
}

// A lane limit of 0 never grants; a cancelled low waiter leaves the queue.
func TestSlotSemLowLimitZeroAndCancel(t *testing.T) {
	limitFn := func() int { return 4 }
	var s slotSem
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.acquireLow(ctx, limitFn, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquireLow with lane limit 0 = %v, want a timeout", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.low.Len() != 0 || s.held != 0 || s.lowHeld != 0 {
		t.Errorf("after a cancelled low waiter: low %d, held %d, lowHeld %d", s.low.Len(), s.held, s.lowHeld)
	}
}

// The job refreshes the session's lastAccess as it goes, so with
// persistence a restart in the middle of a long job doesn't find the session
// idle past its TTL (flushAccess writes what the job stored).
func TestPrepareRefreshesLastAccess(t *testing.T) {
	m := newPrepManager(t)
	old := time.Now().Add(-time.Hour).UnixNano()
	var sawRefresh atomic.Bool
	m.transcode = func(_ context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
		switch n {
		case 0:
			s.lastAccess.Store(old) // as if seg0 took an hour
		case 1:
			sawRefresh.Store(s.lastAccess.Load() > old)
		}
		return writeFakeSegment(t, s, segName(n, kind, k)), nil
	}
	injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	waitPrepState(t, m, "film", prepDone)
	if !sawRefresh.Load() {
		t.Error("lastAccess was not refreshed after a prepared segment")
	}
}

// A single-audio session has one muxed rendition, so "all" is the same job
// as "default": no restart, and the status says default.
func TestPrepareHLSSingleAudioNormalizesAudio(t *testing.T) {
	m := newPrepManager(t)
	started := make(chan struct{}, 1)
	m.transcode = blockingTranscoder(started)
	s := injectPrepSession(t, m, "film", 8, []audioStream{{Index: 1}})
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	<-started
	s.prep.mu.Lock()
	first := s.prep.done
	s.prep.mu.Unlock()
	st, err := m.PrepareHLS("film", prepAudioAll)
	if err != nil {
		t.Fatal(err)
	}
	s.prep.mu.Lock()
	same := s.prep.done == first
	s.prep.mu.Unlock()
	if !same || st.Audio != prepAudioDefault {
		t.Errorf("audio=all on a single-audio session: restarted %v, audio %q", !same, st.Audio)
	}
	if err := m.StopPrepareHLS("film"); err != nil {
		t.Fatal(err)
	}
}

// A job that stops because shutdown began reports idle, not running.
func TestPrepareStoppedByShutdownIsIdle(t *testing.T) {
	m := newPrepManager(t)
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	m.transcode = func(_ context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
		started <- struct{}{}
		<-release
		return writeFakeSegment(t, s, segName(n, kind, k)), nil
	}
	s := injectPrepSession(t, m, "film", 8, nil)
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	<-started
	s.prep.mu.Lock()
	done := s.prep.done
	s.prep.mu.Unlock()
	m.BeginShutdown()
	close(release)
	<-done
	if st, _ := m.HLSPrepareStatus("film"); st.State != prepIdle {
		t.Errorf("state after shutdown stopped the job = %q, want idle", st.State)
	}
}

// End to end through loadPersisted: a session restored with a prepare.json
// and a lastAccess written mid-job resumes once ResumePrepared runs (once),
// and its restored segments are pinned again.
func TestPrepareResumeAfterLoadPersisted(t *testing.T) {
	work := t.TempDir()
	cfg := HLSConfig{Prepare: true, SessionTTL: time.Hour}
	m := newTestPersistManager(t, work, cfg, nil)
	dir := seedPersistedSession(t, m, "film", time.Now()) // 20 s: 5 segments
	for _, name := range []string{"seg0.ts", "seg1.ts"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("segment"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := writePrepFile(dir, prepAudioDefault); err != nil {
		t.Fatal(err)
	}
	assertMode(t, filepath.Join(dir, prepFileName), 0o600)
	m.CloseHLS()

	m2 := newTestPersistManager(t, work, cfg, nil)
	f := &fakeTranscoder{t: t}
	m2.transcode = f.fn
	if m2.Sessions() != 1 {
		t.Fatalf("restored %d sessions, want 1", m2.Sessions())
	}
	m2.ResumePrepared()
	m2.ResumePrepared() // runs once
	st := waitPrepState(t, m2, "film", prepDone)
	if st.Cached != 5 {
		t.Errorf("resumed status = %+v", st)
	}
	if got := f.got(); strings.Join(got, ",") != "seg2.ts,seg3.ts,seg4.ts" {
		t.Errorf("resumed job transcoded %v", got)
	}
	m2.mu.Lock()
	s := m2.sessions["film"]
	m2.mu.Unlock()
	if !s.pins.Load().has("seg0.ts") {
		t.Error("restored segments are not pinned after resume")
	}
}

// ResumePrepared does nothing without persistence or with preparation off.
func TestResumePreparedNeedsPersistAndPrepare(t *testing.T) {
	for _, c := range []struct{ prepare, persist bool }{{true, false}, {false, true}} {
		m := newTestHLSManager(t)
		m.cfg.Prepare, m.persist = c.prepare, c.persist
		f := &fakeTranscoder{t: t}
		m.transcode = f.fn
		s := injectPrepSession(t, m, "film", 8, nil)
		if err := writePrepFile(s.dir, prepAudioDefault); err != nil {
			t.Fatal(err)
		}
		m.ResumePrepared()
		time.Sleep(20 * time.Millisecond)
		if st, _ := m.HLSPrepareStatus("film"); st.State != prepIdle || len(f.got()) != 0 {
			t.Errorf("prepare=%v persist=%v: resumed (status %+v)", c.prepare, c.persist, st)
		}
	}
}

// After a /settings raise of the limit, an arriving regular acquire takes
// the freed slot even though a low-priority waiter queued first.
func TestSlotSemRaisedLimitPrefersRegular(t *testing.T) {
	// The low waiter re-reads a limit of 1, so its own recheck tick never
	// grants; the regular caller sees the raised limit of 2. Before the fix,
	// acquire's grant handed the freed slot to the low waiter and then queued.
	one := func() int { return 1 }
	two := func() int { return 2 }
	var s slotSem
	bg := context.Background()
	if err := s.acquire(bg, one); err != nil {
		t.Fatal(err)
	}
	lowCtx, lowCancel := context.WithCancel(bg)
	defer lowCancel()
	lowDone := make(chan error, 1)
	go func() { lowDone <- s.acquireLow(lowCtx, one, 1) }()
	waitLowWaiters(t, &s, 1)
	ctx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	if err := s.acquire(ctx, two); err != nil {
		t.Fatalf("regular acquire after a limit raise = %v, want the freed slot", err)
	}
	s.mu.Lock()
	lowHeld := s.lowHeld
	s.mu.Unlock()
	if lowHeld != 0 {
		t.Error("the low-priority waiter took the freed slot ahead of the regular acquire")
	}
	lowCancel()
	<-lowDone
}

// Pins are restored at load, before the resumed job starts.
func TestLoadPersistedRestoresPins(t *testing.T) {
	work := t.TempDir()
	cfg := HLSConfig{Prepare: true, SessionTTL: time.Hour}
	m := newTestPersistManager(t, work, cfg, nil)
	dir := seedPersistedSession(t, m, "film", time.Now())
	if err := writePrepFile(dir, prepAudioDefault); err != nil {
		t.Fatal(err)
	}
	m.CloseHLS()
	m2 := newTestPersistManager(t, work, cfg, nil)
	m2.mu.Lock()
	s := m2.sessions["film"]
	m2.mu.Unlock()
	if s == nil || !s.pins.Load().has("seg0.ts") {
		t.Error("segments not pinned right after load")
	}
}

// An unreadable prepare.json is kept on resume (only invalid ones go).
func TestResumePreparedKeepsUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permissions not enforced for root")
	}
	m := newPrepManager(t)
	s := injectPrepSession(t, m, "film", 8, nil)
	path := filepath.Join(s.dir, prepFileName)
	if err := writePrepFile(s.dir, prepAudioDefault); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	m.resumePrepared()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("unreadable prepare.json removed: %v", err)
	}
}

// The consecutive-failure count restarts with the retry pass.
func TestPrepareConsecutiveResetsPerPass(t *testing.T) {
	m := newPrepManager(t)
	var calls atomic.Int32
	m.transcode = func(_ context.Context, s *hlsSession, n int, kind segKind, k int, _ bool) (string, error) {
		// Pass 1: seg0 ok, seg1 and seg2 fail. Pass 2: seg1 fails again
		// (3 errors overall, but only 2 in a row per pass), seg2 succeeds.
		c := calls.Add(1)
		if c == 2 || c == 3 || c == 4 {
			return "", errors.New("boom")
		}
		return writeFakeSegment(t, s, segName(n, kind, k)), nil
	}
	injectPrepSession(t, m, "film", 12, nil) // 3 segments
	if _, err := m.PrepareHLS("film", ""); err != nil {
		t.Fatal(err)
	}
	waitPrepState(t, m, "film", prepDone)
}
