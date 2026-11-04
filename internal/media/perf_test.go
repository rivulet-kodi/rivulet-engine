// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Tests for the transcode-slot semaphore, the per-session segment-cache cap,
// lockFor's read-mostly fast path, the O(1) relay token eviction and the
// subtitle/playlist serialization. All offline: no ffmpeg is spawned.

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingSettings counts Get calls and serves a mutable transcodeConcurrency.
type countingSettings struct {
	gets  atomic.Int64
	limit atomic.Int64
}

func (c *countingSettings) Get(key string) interface{} {
	c.gets.Add(1)
	if key == "transcodeConcurrency" {
		return float64(c.limit.Load())
	}
	return nil
}

// ── slot semaphore ───────────────────────────────────────────────────────────

// A blocked waiter must not poll the settings store: one lookup on entry plus
// at most one per slotRecheckInterval tick (it used to be one per 20 ms).
func TestAcquireTranscodeSlotWaiterDoesNotPoll(t *testing.T) {
	cs := &countingSettings{}
	cs.limit.Store(1)
	m := &hlsManager{cfg: HLSConfig{SegmentConcurrency: 1}, settings: cs}
	if err := m.acquireTranscodeSlot(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	before := cs.gets.Load()
	if err := m.acquireTranscodeSlot(ctx); err == nil {
		t.Fatal("second acquire should have timed out")
	}
	// 600 ms / 20 ms = 30 lookups with the old poll loop.
	if got := cs.gets.Load() - before; got > 6 {
		t.Errorf("blocked waiter read the settings store %d times in 600ms, want <= 6", got)
	}
}

// Releasing wakes the waiter promptly (no poll interval to wait out).
func TestAcquireTranscodeSlotReleaseWakesPromptly(t *testing.T) {
	m := &hlsManager{cfg: HLSConfig{SegmentConcurrency: 1}}
	if err := m.acquireTranscodeSlot(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan time.Time, 1)
	go func() {
		_ = m.acquireTranscodeSlot(context.Background())
		done <- time.Now()
	}()
	time.Sleep(50 * time.Millisecond)
	t0 := time.Now()
	m.releaseTranscodeSlot()
	select {
	case at := <-done:
		if d := at.Sub(t0); d > 15*time.Millisecond {
			t.Errorf("handoff took %v, want well under the old 20ms poll", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never woke")
	}
	m.releaseTranscodeSlot()
}

// Raising the live concurrency setting admits blocked waiters without any
// release happening.
func TestAcquireTranscodeSlotLimitRaisedWhileBlocked(t *testing.T) {
	cs := &countingSettings{}
	cs.limit.Store(2)
	m := &hlsManager{cfg: HLSConfig{SegmentConcurrency: 1}, settings: cs}
	for range 2 {
		if err := m.acquireTranscodeSlot(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	got := make(chan error, 1)
	go func() { got <- m.acquireTranscodeSlot(context.Background()) }()
	select {
	case <-got:
		t.Fatal("acquired beyond the limit")
	case <-time.After(100 * time.Millisecond):
	}
	cs.limit.Store(3)
	select {
	case err := <-got:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("raised limit never admitted the waiter")
	}
}

// Waiters are served in arrival order.
func TestAcquireTranscodeSlotFIFO(t *testing.T) {
	m := &hlsManager{cfg: HLSConfig{SegmentConcurrency: 1}}
	if err := m.acquireTranscodeSlot(context.Background()); err != nil {
		t.Fatal(err)
	}
	const n = 5
	order := make(chan int, n)
	for i := range n {
		go func() {
			_ = m.acquireTranscodeSlot(context.Background())
			order <- i
			m.releaseTranscodeSlot()
		}()
		// Wait until waiter i is queued before starting i+1.
		deadline := time.Now().Add(2 * time.Second)
		for {
			m.slots.mu.Lock()
			q := m.slots.waiters.Len()
			m.slots.mu.Unlock()
			if q == i+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("waiter %d never queued", i)
			}
			time.Sleep(time.Millisecond)
		}
	}
	m.releaseTranscodeSlot()
	for want := range n {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("served waiter %d, want %d (FIFO)", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for waiters")
		}
	}
}

// Stress: acquire/release/cancel races must neither leak nor over-grant slots.
func TestSlotSemStressNoLeakNoOvergrant(t *testing.T) {
	const limit = 3
	m := &hlsManager{cfg: HLSConfig{SegmentConcurrency: limit}}
	var inUse, maxInUse atomic.Int32
	var wg sync.WaitGroup
	for g := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				ctx := context.Background()
				cancel := func() {}
				if (g+i)%3 == 0 {
					ctx, cancel = context.WithTimeout(ctx, time.Duration(i%5)*50*time.Microsecond)
				}
				if err := m.acquireTranscodeSlot(ctx); err == nil {
					n := inUse.Add(1)
					for {
						old := maxInUse.Load()
						if n <= old || maxInUse.CompareAndSwap(old, n) {
							break
						}
					}
					inUse.Add(-1)
					m.releaseTranscodeSlot()
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	if got := maxInUse.Load(); got > limit {
		t.Errorf("observed %d concurrent holders, limit %d", got, limit)
	}
	m.slots.mu.Lock()
	held, q := m.slots.held, m.slots.waiters.Len()
	m.slots.mu.Unlock()
	if held != 0 || q != 0 {
		t.Errorf("after the storm held=%d queued=%d, want 0/0 (leak)", held, q)
	}
}

// ── lockFor ──────────────────────────────────────────────────────────────────

func TestLockForConcurrentSameMutex(t *testing.T) {
	s := &hlsSession{} // nil segLocks map must be tolerated
	const n = 64
	got := make([]*sync.Mutex, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got[i] = s.lockFor("seg0.ts")
		}()
	}
	wg.Wait()
	for i := 1; i < n; i++ {
		if got[i] != got[0] {
			t.Fatalf("lockFor returned different mutexes for one filename (%d)", i)
		}
	}
}

// ── segment cache cap ────────────────────────────────────────────────────────

func writeSeg(t *testing.T, dir, name string, size int) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

func TestEvictSegmentsOldestFirstRespectsKeepAndGrace(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, segLocks: map[string]*sync.Mutex{}}
	now := time.Now()
	for i := range 6 {
		name := fmt.Sprintf("seg%d.ts", i)
		writeSeg(t, dir, name, 100)
		// seg0 oldest ... seg5 newest; seg5 is within the serve grace.
		age := time.Duration(6-i) * time.Minute
		if i == 5 {
			age = time.Second
		}
		s.noteSegment(name, 100, now.Add(-age))
	}
	// A work file must never be indexed or touched.
	writeSeg(t, dir, "seg9.ts.tmp.ts", 500)

	// Cap 350: total 600 → must drop the 3 oldest eligible (seg0..seg2).
	s.evictSegments(350, "seg3.ts", now)
	for _, n := range []string{"seg0.ts", "seg1.ts", "seg2.ts"} {
		if exists(dir, n) {
			t.Errorf("%s should have been evicted (oldest first)", n)
		}
	}
	for _, n := range []string{"seg3.ts", "seg4.ts", "seg5.ts", "seg9.ts.tmp.ts"} {
		if !exists(dir, n) {
			t.Errorf("%s must survive", n)
		}
	}

	// Cap far below what protected entries use: keep (seg3) and the
	// grace-protected seg5 stay; only seg4 can go. Cache may stay over cap.
	s.evictSegments(1, "seg3.ts", now)
	if exists(dir, "seg4.ts") {
		t.Error("seg4.ts should be evicted under a tiny cap")
	}
	if !exists(dir, "seg3.ts") || !exists(dir, "seg5.ts") {
		t.Error("keep / recently used segment was evicted")
	}
	if s.segIdx.total != 200 {
		t.Errorf("index total = %d, want 200", s.segIdx.total)
	}
}

func TestEvictSegmentsSkipsLockedSegment(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, segLocks: map[string]*sync.Mutex{}}
	now := time.Now()
	writeSeg(t, dir, "seg0.ts", 100)
	writeSeg(t, dir, "seg1.ts", 100)
	s.noteSegment("seg0.ts", 100, now.Add(-time.Hour))
	s.noteSegment("seg1.ts", 100, now.Add(-time.Minute))

	l := s.lockFor("seg0.ts") // e.g. mid-transcode / being returned
	l.Lock()
	s.evictSegments(100, "", now)
	l.Unlock()
	if !exists(dir, "seg0.ts") {
		t.Error("segment with a held lock was evicted")
	}
	if exists(dir, "seg1.ts") {
		t.Error("next-oldest segment should have been evicted instead")
	}
}

func TestEvictSegmentsLoadsExistingDirAndUnlimited(t *testing.T) {
	dir := t.TempDir()
	// Pre-existing (e.g. restored persisted session) segments, old mtimes.
	old := time.Now().Add(-time.Hour)
	for i := range 4 {
		name := fmt.Sprintf("seg%d.ts", i)
		writeSeg(t, dir, name, 100)
		if err := os.Chtimes(filepath.Join(dir, name), old.Add(time.Duration(i)*time.Minute), old.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	writeSeg(t, dir, "a1seg0.ts", 100)
	if err := os.Chtimes(filepath.Join(dir, "a1seg0.ts"), old, old); err != nil {
		t.Fatal(err)
	}
	writeSeg(t, dir, "playlist.m3u8", 999)
	s := &hlsSession{dir: dir, segLocks: map[string]*sync.Mutex{}}

	s.evictSegments(0, "", time.Now()) // unlimited: nothing happens
	if !exists(dir, "a1seg0.ts") {
		t.Fatal("limit 0 must be a no-op")
	}
	s.evictSegments(300, "", time.Now()) // 500 → 300: drops 2 oldest (audio seg + seg0)
	if exists(dir, "a1seg0.ts") || exists(dir, "seg0.ts") {
		t.Error("two oldest restored segments should be evicted")
	}
	if !exists(dir, "seg1.ts") || !exists(dir, "seg3.ts") || !exists(dir, "playlist.m3u8") {
		t.Error("newer segments / playlist must survive")
	}
}

func TestSegmentCacheBytes(t *testing.T) {
	m := &hlsManager{}
	s := &hlsSession{}
	s.tc.maxrateBps = 8_000_000
	want := int64(8_000_000 / 8 * 4 * segCacheSegments)
	if got := m.segmentCacheBytes(s); got != want {
		t.Errorf("derived cap = %d, want %d", got, want)
	}
	s.tc.maxrateBps = 1000
	if got := m.segmentCacheBytes(s); got != segCacheMinBytes {
		t.Errorf("tiny-bitrate cap = %d, want floor %d", got, segCacheMinBytes)
	}
	m.cfg.SegmentCacheBytes = 12345
	if got := m.segmentCacheBytes(s); got != 12345 {
		t.Errorf("explicit cap = %d, want 12345", got)
	}
	m.cfg.SegmentCacheBytes = -1
	if got := m.segmentCacheBytes(s); got != 0 {
		t.Errorf("disabled cap = %d, want 0 (unlimited)", got)
	}
}

// transcodeSegment's cache-hit path refreshes recency and never evicts what
// it serves; a fresh install over the cap trims older segments only.
func TestTranscodeSegmentCacheHitTouchesIndex(t *testing.T) {
	m := newTestHLSManager(t)
	dir := t.TempDir()
	s := &hlsSession{dir: dir, duration: 40, segLocks: map[string]*sync.Mutex{}}
	writeSeg(t, dir, "seg2.ts", 64)
	p, err := m.transcodeSegment(context.Background(), s, 2, segMuxed, 0, segLive)
	if err != nil || filepath.Base(p) != "seg2.ts" {
		t.Fatalf("cache hit = %q, %v", p, err)
	}
	if e, ok := s.segIdx.files["seg2.ts"]; !ok || e.size != 64 {
		t.Errorf("cache hit not recorded in the index: %+v ok=%v", e, ok)
	}
}

// ── subtitle slot bound + playlist serialization ─────────────────────────────

func TestExtractSubtitleBoundedByCtxWhenSlotsBusy(t *testing.T) {
	m := newTestHLSManager(t)
	dir := t.TempDir()
	s := &hlsSession{
		dir: dir, segLocks: map[string]*sync.Mutex{},
		subtitleStreams: []subtitleStream{{SubIdx: 0}},
	}
	for range subtitleConcurrency {
		if err := m.subs.acquire(context.Background(), subtitleLimit); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := m.extractSubtitle(ctx, s, 0); err == nil {
		t.Fatal("extractSubtitle ran past the subtitle slot bound")
	}
	// An already-extracted track is served without needing a slot.
	writeSeg(t, dir, "sub0.vtt", 10)
	if _, err := m.extractSubtitle(ctx, s, 0); err != nil {
		t.Errorf("cached subtitle needed a slot: %v", err)
	}
}

func TestPlaylistWritersConcurrentAndCached(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, duration: 100, segLocks: map[string]*sync.Mutex{}}
	pl := filepath.Join(dir, "playlist.m3u8")
	sub := filepath.Join(dir, "sub0.m3u8")
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(2)
		go func() { defer wg.Done(); _ = s.writePlaylist(pl, "") }()
		go func() { defer wg.Done(); _ = s.writeSubPlaylist(sub, 0) }()
	}
	wg.Wait()
	raw, err := os.ReadFile(pl)
	if err != nil || !strings.HasSuffix(string(raw), "#EXT-X-ENDLIST\n") || strings.Count(string(raw), "#EXTINF") != 25 {
		t.Fatalf("playlist corrupt: err=%v\n%s", err, raw)
	}
	// Second call must not rewrite: remove the file, it stays removed.
	if err := os.Remove(sub); err != nil {
		t.Fatal(err)
	}
	if err := s.writeSubPlaylist(sub, 0); err != nil {
		t.Fatal(err)
	}
	if exists(dir, "sub0.m3u8") {
		t.Error("sub playlist was rewritten on every request")
	}
	if exists(dir, "playlist.m3u8.tmp") || exists(dir, "sub0.m3u8.tmp") {
		t.Error("tmp file left behind")
	}
}

func TestWriteSubPlaylistUnknownDurationNotCached(t *testing.T) {
	dir := t.TempDir()
	s := &hlsSession{dir: dir, segLocks: map[string]*sync.Mutex{}}
	p := filepath.Join(dir, "sub0.m3u8")
	if err := s.writeSubPlaylist(p, 0); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.duration = 90
	s.mu.Unlock()
	if err := s.writeSubPlaylist(p, 0); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(p)
	if !strings.Contains(string(raw), "#EXTINF:90.000") {
		t.Errorf("playlist not refreshed once the duration became known:\n%s", raw)
	}
}

// ── relay eviction ───────────────────────────────────────────────────────────

// A re-registered (refreshed) URL becomes the most recent: at capacity the
// eviction takes the genuinely oldest entries, not the refreshed one.
func TestRelayRefreshedTokenSurvivesEviction(t *testing.T) {
	r := newMediaRelay(false)
	first, err := r.register("http://93.184.216.34/first.ts")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < relayMaxTokens; i++ {
		if _, err := r.register(fmt.Sprintf("http://93.184.216.34/f%d.ts", i)); err != nil {
			t.Fatal(err)
		}
	}
	if again, _ := r.register("http://93.184.216.34/first.ts"); again != first {
		t.Fatal("re-register minted a new token")
	}
	for i := range 10 {
		if _, err := r.register(fmt.Sprintf("http://93.184.216.34/new%d.ts", i)); err != nil {
			t.Fatal(err)
		}
	}
	tok := first[strings.LastIndex(first, "/")+1:]
	if _, ok := r.resolve(tok); !ok {
		t.Error("refreshed token was evicted ahead of older ones")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.tokens) != relayMaxTokens || r.order.Len() != relayMaxTokens || len(r.byURL) != relayMaxTokens {
		t.Errorf("index sizes diverged: tokens=%d order=%d byURL=%d", len(r.tokens), r.order.Len(), len(r.byURL))
	}
}

func TestRelaySweepDropsOnlyExpiredPrefix(t *testing.T) {
	r := newMediaRelay(false)
	for i := range 10 {
		if _, err := r.register(fmt.Sprintf("http://93.184.216.34/s%d.ts", i)); err != nil {
			t.Fatal(err)
		}
	}
	r.mu.Lock()
	el := r.order.Front()
	for range 4 {
		el.Value.(*relayEntry).expiresAt = time.Now().Add(-time.Second)
		el = el.Next()
	}
	r.sweepLocked(time.Now())
	n, nu, no := len(r.tokens), len(r.byURL), r.order.Len()
	r.mu.Unlock()
	if n != 6 || nu != 6 || no != 6 {
		t.Errorf("after sweep tokens=%d byURL=%d order=%d, want 6 each", n, nu, no)
	}
}

func TestRelayTransportKeepsIdleConns(t *testing.T) {
	r := newMediaRelay(false)
	tr, ok := r.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", r.client.Transport)
	}
	if tr.MaxIdleConnsPerHost < 4 || tr.IdleConnTimeout <= 0 {
		t.Errorf("MaxIdleConnsPerHost=%d IdleConnTimeout=%v: idle connections must be pooled and reaped", tr.MaxIdleConnsPerHost, tr.IdleConnTimeout)
	}
}
