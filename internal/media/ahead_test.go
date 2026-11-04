// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newAheadManager returns a test manager with transcode-ahead set to n
// segments and a gated fake transcoder as its seam.
func newAheadManager(t *testing.T, n int) (*hlsManager, *gateTranscoder) {
	t.Helper()
	m := newTestHLSManager(t)
	m.cfg.TranscodeAhead = n
	g := &gateTranscoder{t: t, started: make(chan string, 64), release: make(chan struct{}), errs: map[string]error{}}
	m.transcode = g.fn
	return m, g
}

// gateTranscoder is a fake transcoder whose calls each wait for a value on
// release (or a closed release) or their context. It records every call
// and every cancelled one, and caches the segments it lets through.
type gateTranscoder struct {
	t        *testing.T
	mu       sync.Mutex
	calls    []string
	prios    []segPriority
	canceled []string
	errs     map[string]error // per segment file, what the call returns
	started  chan string
	release  chan struct{}
	// lock makes each call hold the segment's lock (TryLock, or
	// errSegmentBusy) while it waits, as a background transcodeSegment does.
	lock bool
}

func (g *gateTranscoder) fn(ctx context.Context, s *hlsSession, n int, kind segKind, audioIdx int, prio segPriority) (string, error) {
	name := segName(n, kind, audioIdx)
	g.mu.Lock()
	g.calls = append(g.calls, name)
	g.prios = append(g.prios, prio)
	err := g.errs[name]
	g.mu.Unlock()
	if g.lock {
		l := s.lockFor(name)
		if !l.TryLock() {
			return "", errSegmentBusy
		}
		defer l.Unlock()
	}
	g.started <- name
	select {
	case <-g.release:
	case <-ctx.Done():
		g.mu.Lock()
		g.canceled = append(g.canceled, name)
		g.mu.Unlock()
		return "", ctx.Err()
	}
	if err != nil {
		return "", err
	}
	return writeFakeSegment(g.t, s, name), nil
}

func (g *gateTranscoder) got() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.calls)
}

func (g *gateTranscoder) gotCanceled() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.canceled)
}

// next waits for the fake's next call and returns its segment.
func (g *gateTranscoder) next(t *testing.T) string {
	t.Helper()
	select {
	case name := <-g.started:
		return name
	case <-time.After(5 * time.Second):
		t.Fatalf("no transcode started; calls so far %v", g.got())
		return ""
	}
}

// waitNoWorkers waits until s has no transcode-ahead worker and no
// in-flight hold left.
func waitNoWorkers(t *testing.T, s *hlsSession) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.ahead.mu.Lock()
		n := len(s.ahead.workers)
		s.ahead.mu.Unlock()
		if n == 0 && s.inFlight.Load() == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers %d, inFlight %d, want none", n, s.inFlight.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// serve requests name from session id through HLSFile; the segment must be
// cached already, so no ffmpeg runs.
func serve(t *testing.T, m *hlsManager, id, name string) {
	t.Helper()
	if _, _, err := m.HLSFile(context.Background(), id, name); err != nil {
		t.Fatalf("HLSFile(%q) = %v", name, err)
	}
}

// Off by default: a served segment starts nothing.
func TestTranscodeAheadOffByDefault(t *testing.T) {
	if DefaultHLSConfig().TranscodeAhead != 0 {
		t.Fatal("transcode-ahead is on by default")
	}
	m, g := newAheadManager(t, 0)
	s := injectPrepSession(t, m, "film", 40, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	waitNoWorkers(t, s)
	if got := g.got(); len(got) != 0 {
		t.Errorf("transcoded %v with transcode-ahead off", got)
	}
	if s.ahead.workers != nil {
		t.Error("a worker map was created with transcode-ahead off")
	}
}

// The window: the next N segments, cached ones skipped, at the ahead
// priority, and nothing past the end.
func TestTranscodeAheadWindow(t *testing.T) {
	m, g := newAheadManager(t, 3)
	close(g.release)
	s := injectPrepSession(t, m, "film", 40, nil) // 10 segments
	writeFakeSegment(t, s, "seg0.ts")
	writeFakeSegment(t, s, "seg2.ts")
	serve(t, m, "film", "seg0.ts")
	waitNoWorkers(t, s)
	if got, want := g.got(), []string{"seg1.ts", "seg3.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v", got, want)
	}
	for i, p := range g.prios {
		if p != segAhead {
			t.Errorf("transcode %d had priority %d, want segAhead", i, p)
		}
	}

	// Near the end the window stops at the last segment; at it, nothing
	// starts.
	g.mu.Lock()
	g.calls, g.prios = nil, nil
	g.mu.Unlock()
	writeFakeSegment(t, s, "seg8.ts")
	serve(t, m, "film", "seg8.ts")
	waitNoWorkers(t, s)
	serve(t, m, "film", "seg9.ts")
	waitNoWorkers(t, s)
	if got, want := g.got(), []string{"seg9.ts"}; !slices.Equal(got, want) {
		t.Errorf("near the end transcoded %v, want %v", got, want)
	}
}

// An unknown duration starts nothing.
func TestTranscodeAheadUnknownDuration(t *testing.T) {
	m, g := newAheadManager(t, 3)
	s := injectPrepSession(t, m, "film", 0, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	waitNoWorkers(t, s)
	if got := g.got(); len(got) != 0 {
		t.Errorf("transcoded %v for a session of unknown duration", got)
	}
}

// Playing on through the window moves the same worker along; it is not
// restarted, and the segment it is transcoding is not cancelled.
func TestTranscodeAheadMovesAlong(t *testing.T) {
	m, g := newAheadManager(t, 2)
	s := injectPrepSession(t, m, "film", 40, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	if got := g.next(t); got != "seg1.ts" {
		t.Fatalf("first ahead transcode %q, want seg1.ts", got)
	}
	s.ahead.mu.Lock()
	w := s.ahead.workers[""]
	s.ahead.mu.Unlock()
	// The client gets to seg1 (cached by someone else meanwhile, say) while
	// the worker is still on it... it's in the window, so the window moves.
	writeFakeSegment(t, s, "seg1.ts")
	serve(t, m, "film", "seg1.ts")
	s.ahead.mu.Lock()
	same := s.ahead.workers[""] == w
	end := w.end
	s.ahead.mu.Unlock()
	if !same {
		t.Fatal("a request inside the window replaced the worker")
	}
	if end != 3 {
		t.Errorf("window end = %d, want 3", end)
	}
	close(g.release)
	waitNoWorkers(t, s)
	if got, want := g.got(), []string{"seg1.ts", "seg2.ts", "seg3.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v", got, want)
	}
	if c := g.gotCanceled(); len(c) != 0 {
		t.Errorf("cancelled %v while playing on", c)
	}
}

// A seek out of the window cancels the worker, including the transcode it
// is running, and starts one after the new position.
func TestTranscodeAheadSeekReplaces(t *testing.T) {
	m, g := newAheadManager(t, 2)
	s := injectPrepSession(t, m, "film", 80, nil) // 20 segments
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	if got := g.next(t); got != "seg1.ts" {
		t.Fatalf("first ahead transcode %q, want seg1.ts", got)
	}
	writeFakeSegment(t, s, "seg10.ts")
	serve(t, m, "film", "seg10.ts")
	if got := g.next(t); got != "seg11.ts" {
		t.Fatalf("after the seek the worker started %q, want seg11.ts", got)
	}
	close(g.release)
	waitNoWorkers(t, s)
	if c := g.gotCanceled(); !slices.Equal(c, []string{"seg1.ts"}) {
		t.Errorf("cancelled %v, want [seg1.ts]", c)
	}
	if got, want := g.got(), []string{"seg1.ts", "seg11.ts", "seg12.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v", got, want)
	}
	// Going back is a seek too; the cancelled seg1 was never cached.
	g.mu.Lock()
	g.calls, g.canceled = nil, nil
	g.mu.Unlock()
	writeFakeSegment(t, s, "seg2.ts")
	serve(t, m, "film", "seg0.ts")
	waitNoWorkers(t, s)
	if got, want := g.got(), []string{"seg1.ts"}; !slices.Equal(got, want) {
		t.Errorf("after seeking back transcoded %v, want %v (seg2 is cached)", got, want)
	}
}

// Each audio rendition has its own worker, transcoding its own segments.
func TestTranscodeAheadAudioRendition(t *testing.T) {
	m, g := newAheadManager(t, 1)
	close(g.release)
	streams := []audioStream{{}, {}}
	s := injectPrepSession(t, m, "film", 40, streams)
	writeFakeSegment(t, s, "seg0.ts")
	writeFakeSegment(t, s, "a1seg0.ts")
	serve(t, m, "film", "seg0.ts")
	serve(t, m, "film", "a1seg0.ts")
	waitNoWorkers(t, s)
	got := g.got()
	slices.Sort(got)
	if want := []string{"a1seg1.ts", "seg1.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v", got, want)
	}
}

// A failed transcode stops the worker; a busy segment is skipped.
func TestTranscodeAheadErrors(t *testing.T) {
	m, g := newAheadManager(t, 3)
	close(g.release)
	g.errs["seg1.ts"] = errSegmentBusy
	g.errs["seg2.ts"] = errors.New("source gone")
	s := injectPrepSession(t, m, "film", 40, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	waitNoWorkers(t, s)
	if got, want := g.got(), []string{"seg1.ts", "seg2.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v (busy skipped, stop at the failure)", got, want)
	}
}

// DeleteHLS stops a running worker, and the directory goes once it has
// exited.
func TestTranscodeAheadDelete(t *testing.T) {
	m, g := newAheadManager(t, 2)
	s := injectPrepSession(t, m, "film", 40, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	g.next(t)
	if err := m.DeleteHLS("film"); err != nil {
		t.Fatal(err)
	}
	waitNoWorkers(t, s)
	if c := g.gotCanceled(); !slices.Equal(c, []string{"seg1.ts"}) {
		t.Errorf("cancelled %v, want [seg1.ts]", c)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(s.dir); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session directory still there after the worker exited")
		}
		time.Sleep(time.Millisecond)
	}
	if got := g.got(); len(got) != 1 {
		t.Errorf("transcoded %v after the delete", got)
	}
}

// After BeginShutdown a worker finishes the segment it is on and starts no
// other, and no new worker starts; CloseHLS stops a running one.
func TestTranscodeAheadShutdown(t *testing.T) {
	m, g := newAheadManager(t, 3)
	s := injectPrepSession(t, m, "film", 40, nil)
	writeFakeSegment(t, s, "seg0.ts")
	writeFakeSegment(t, s, "seg5.ts")
	serve(t, m, "film", "seg0.ts")
	g.next(t)
	m.BeginShutdown()
	serve(t, m, "film", "seg5.ts") // a seek: cancels, starts nothing new
	waitNoWorkers(t, s)
	if c := g.gotCanceled(); !slices.Equal(c, []string{"seg1.ts"}) {
		t.Errorf("cancelled %v, want [seg1.ts]", c)
	}
	if got := g.got(); !slices.Equal(got, []string{"seg1.ts"}) {
		t.Errorf("transcoded %v while shutting down, want only [seg1.ts]", got)
	}

	// A worker already past the entry check when shutdown begins.
	m2, g2 := newAheadManager(t, 3)
	s2 := injectPrepSession(t, m2, "film", 40, nil)
	writeFakeSegment(t, s2, "seg0.ts")
	serve(t, m2, "film", "seg0.ts")
	g2.next(t)
	m2.BeginShutdown()
	g2.release <- struct{}{} // seg1 finishes
	waitNoWorkers(t, s2)
	if got := g2.got(); !slices.Equal(got, []string{"seg1.ts"}) {
		t.Errorf("transcoded %v after BeginShutdown, want only [seg1.ts]", got)
	}

	// CloseHLS cancels a running worker.
	m3, g3 := newAheadManager(t, 3)
	s3 := injectPrepSession(t, m3, "film", 40, nil)
	writeFakeSegment(t, s3, "seg0.ts")
	serve(t, m3, "film", "seg0.ts")
	g3.next(t)
	m3.CloseHLS()
	waitNoWorkers(t, s3)
	if c := g3.gotCanceled(); !slices.Equal(c, []string{"seg1.ts"}) {
		t.Errorf("CloseHLS cancelled %v, want [seg1.ts]", c)
	}
}

// Requests that finish out of order, or a short step back, don't count as a
// seek: the worker carries on, and its window never moves backwards.
func TestTranscodeAheadOutOfOrder(t *testing.T) {
	m, g := newAheadManager(t, 2)
	s := injectPrepSession(t, m, "film", 40, nil)
	for _, n := range []string{"seg0.ts", "seg1.ts", "seg2.ts"} {
		writeFakeSegment(t, s, n)
	}
	serve(t, m, "film", "seg2.ts") // window 3..4
	if got := g.next(t); got != "seg3.ts" {
		t.Fatalf("worker started %q, want seg3.ts", got)
	}
	s.ahead.mu.Lock()
	w := s.ahead.workers[""]
	s.ahead.mu.Unlock()
	serve(t, m, "film", "seg1.ts") // finishes late
	serve(t, m, "film", "seg0.ts") // two back: still within N of the furthest
	s.ahead.mu.Lock()
	same, base := s.ahead.workers[""] == w, w.base
	s.ahead.mu.Unlock()
	if !same || base != 2 {
		t.Errorf("after late requests: same worker %v, base %d; want the same worker, base 2", same, base)
	}
	close(g.release)
	waitNoWorkers(t, s)
	if c := g.gotCanceled(); len(c) != 0 {
		t.Errorf("cancelled %v on out-of-order requests", c)
	}
	if got, want := g.got(), []string{"seg3.ts", "seg4.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v", got, want)
	}
}

// A seek cancels the worker when the request arrives, before the requested
// segment is transcoded (here the request itself fails, so no new worker
// starts).
func TestTranscodeAheadSeekCancelsFirst(t *testing.T) {
	m, g := newAheadManager(t, 2)
	s := injectPrepSession(t, m, "film", 80, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	g.next(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the live transcode of seg10 fails at its slot wait, before ffmpeg
	if _, _, err := m.HLSFile(ctx, "film", "seg10.ts"); err == nil {
		t.Fatal("HLSFile with a cancelled context succeeded")
	}
	waitNoWorkers(t, s)
	if c := g.gotCanceled(); !slices.Equal(c, []string{"seg1.ts"}) {
		t.Errorf("cancelled %v, want [seg1.ts]", c)
	}
	if got := g.got(); !slices.Equal(got, []string{"seg1.ts"}) {
		t.Errorf("transcoded %v, want only [seg1.ts]", got)
	}
}

// A client asking for the segment the worker is transcoding waits for that
// transcode and gets its result, without transcoding it again.
func TestTranscodeAheadLiveWaitsForWorker(t *testing.T) {
	m, g := newAheadManager(t, 2)
	g.lock = true
	s := injectPrepSession(t, m, "film", 40, nil)
	writeFakeSegment(t, s, "seg0.ts")
	serve(t, m, "film", "seg0.ts")
	if got := g.next(t); got != "seg1.ts" {
		t.Fatalf("worker started %q, want seg1.ts", got)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := m.HLSFile(context.Background(), "film", "seg1.ts")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("live request returned (%v) while the worker held the segment", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.release <- struct{}{} // seg1 is cached and unlocked; the live request takes it
	if err := <-done; err != nil {
		t.Fatalf("live request = %v", err)
	}
	close(g.release)
	waitNoWorkers(t, s)
	if got, want := g.got(), []string{"seg1.ts", "seg2.ts", "seg3.ts"}; !slices.Equal(got, want) {
		t.Errorf("transcoded %v, want %v (seg1 once)", got, want)
	}
}

// A real background transcode at the ahead priority takes the ahead lane:
// it leaves a locked segment alone and returns its slot.
func TestTranscodeSegmentAheadLane(t *testing.T) {
	m := newTestHLSManager(t)
	s := injectPrepSession(t, m, "film", 8, nil)
	l := s.lockFor("seg0.ts")
	l.Lock()
	_, err := m.transcodeSegment(context.Background(), s, 0, segMuxed, 0, segAhead)
	l.Unlock()
	if !errors.Is(err, errSegmentBusy) {
		t.Errorf("ahead transcode of a locked segment = %v, want errSegmentBusy", err)
	}
	m.slots.mu.Lock()
	defer m.slots.mu.Unlock()
	if m.slots.held != 0 || m.slots.lanes[laneAhead].held != 0 || m.slots.lanes[lanePrepare].held != 0 {
		t.Errorf("slots leaked: held %d, lanes %+v", m.slots.held, m.slots.lanes)
	}
	if m.slots.lanes[laneAhead].limit != m.cfg.AheadConcurrency {
		t.Errorf("ahead lane limit = %d, want %d", m.slots.lanes[laneAhead].limit, m.cfg.AheadConcurrency)
	}
}

// Queued waiters are served regular first, then the ahead lane, then the
// preparation lane, each lane within its own limit.
func TestSlotSemLaneOrder(t *testing.T) {
	var limit atomic.Int32
	limit.Store(3)
	limitFn := func() int { return int(limit.Load()) }
	var s slotSem
	bg := context.Background()
	for range 3 {
		if err := s.acquire(bg, limitFn); err != nil {
			t.Fatal(err)
		}
	}
	order := make(chan string, 3)
	go func() {
		if s.acquireLow(bg, limitFn, lanePrepare, 1) == nil {
			order <- "prepare"
		}
	}()
	waitLaneWaiters(t, &s, lanePrepare, 1)
	go func() {
		if s.acquireLow(bg, limitFn, laneAhead, 1) == nil {
			order <- "ahead"
		}
	}()
	waitLaneWaiters(t, &s, laneAhead, 1)
	go func() {
		if s.acquire(bg, limitFn) == nil {
			order <- "regular"
		}
	}()
	waitSemWaiters(t, &s, 1)
	for _, want := range []string{"regular", "ahead", "prepare"} {
		s.release(limitFn) // one of the three original holders'
		if got := <-order; got != want {
			t.Fatalf("grant went to %q, want %q", got, want)
		}
	}
	s.mu.Lock()
	held, ah, pr := s.held, s.lanes[laneAhead].held, s.lanes[lanePrepare].held
	s.mu.Unlock()
	if held != 3 || ah != 1 || pr != 1 {
		t.Fatalf("held %d, ahead %d, prepare %d; want 3, 1, 1", held, ah, pr)
	}
	// Both lanes are at their limits of 1: with a slot free (the limit
	// raised), a further ahead acquire still waits.
	limit.Store(4)
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	if err := s.acquireLow(ctx, limitFn, laneAhead, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ahead acquire past its lane limit = %v, want a timeout", err)
	}
	s.release(limitFn) // the regular waiter's
	s.releaseLow(limitFn, laneAhead)
	s.releaseLow(limitFn, lanePrepare)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held != 0 || s.lanes[laneAhead].held != 0 || s.lanes[lanePrepare].held != 0 || s.queuedLocked() {
		t.Errorf("after releasing everything: held %d, lanes %+v", s.held, s.lanes)
	}
}

// A lane at its limit doesn't hold the other one back: with the ahead lane
// full and its waiter queued, a preparation acquire still takes a free slot.
func TestSlotSemLanesIndependent(t *testing.T) {
	limitFn := func() int { return 3 }
	var s slotSem
	bg := context.Background()
	if err := s.acquireLow(bg, limitFn, laneAhead, 1); err != nil {
		t.Fatal(err)
	}
	aheadCtx, aheadCancel := context.WithCancel(bg)
	defer aheadCancel()
	aheadDone := make(chan error, 1)
	go func() { aheadDone <- s.acquireLow(aheadCtx, limitFn, laneAhead, 1) }()
	waitLaneWaiters(t, &s, laneAhead, 1)
	ctx, cancel := context.WithTimeout(bg, time.Second)
	defer cancel()
	if err := s.acquireLow(ctx, limitFn, lanePrepare, 1); err != nil {
		t.Fatalf("preparation acquire with the ahead lane full = %v, want a slot", err)
	}
	aheadCancel()
	if err := <-aheadDone; !errors.Is(err, context.Canceled) {
		t.Errorf("queued ahead waiter = %v, want cancelled", err)
	}
	s.releaseLow(limitFn, lanePrepare)
	s.releaseLow(limitFn, laneAhead)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held != 0 || s.queuedLocked() {
		t.Errorf("after releasing everything: held %d, lanes %+v", s.held, s.lanes)
	}
}

// waitLaneWaiters spins (bounded) until lane has n queued waiters.
func waitLaneWaiters(t *testing.T, s *slotSem, lane, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		got := s.lanes[lane].waiters.Len()
		s.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lane %d waiters = %d, want %d", lane, got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// normalize bounds TranscodeAhead and resolves AheadConcurrency.
func TestNormalizeTranscodeAhead(t *testing.T) {
	for _, c := range []struct{ in, want int }{{-1, 0}, {0, 0}, {5, 5}, {MaxTranscodeAhead + 1, MaxTranscodeAhead}} {
		cfg := DefaultHLSConfig()
		cfg.TranscodeAhead = c.in
		if got := cfg.normalize(1).TranscodeAhead; got != c.want {
			t.Errorf("normalize(TranscodeAhead %d) = %d, want %d", c.in, got, c.want)
		}
	}
	cfg := DefaultHLSConfig()
	cfg.AheadConcurrency = 0
	if got := cfg.normalize(1).AheadConcurrency; got != defaultAheadConcurrency {
		t.Errorf("normalize(AheadConcurrency 0) = %d, want %d", got, defaultAheadConcurrency)
	}
}
