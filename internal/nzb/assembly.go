// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

const (
	// defaultConnections is the parallelism used when ServerConfig.Connections
	// is unset. stremio-core's canonical nntp:// server URL carries no
	// connection count, so this is what most sessions get. Connections the
	// server refuses beyond its limit are tolerated, so a modest default is
	// safe even against providers that allow only one or two.
	defaultConnections = 4
	// maxConnections caps ServerConfig.Connections: it is enough to saturate a
	// fast link with ~700 KiB articles without hammering the provider or
	// exhausting local file descriptors.
	maxConnections = 16
)

// workerCount returns how many parallel connections to use for a file of
// segments articles when the server allows configured connections.
func workerCount(configured, segments int) int {
	n := configured
	if n <= 0 {
		n = defaultConnections
	}
	n = min(n, maxConnections, segments)
	return max(n, 1)
}

// errNoPartInfo reports a multi-segment file whose articles carry no =ypart
// byte range, so a segment cannot be placed without its predecessors.
var errNoPartInfo = errors.New("nzb: multi-segment article has no =ypart range")

type segStatus uint8

const (
	segPending segStatus = iota
	segInflight
	segDone
)

type layoutMode uint8

const (
	// layoutUnknown: the first article's header has not been read yet.
	layoutUnknown layoutMode = iota
	// layoutParallel: every article declares its own byte range (=ypart) and
	// the file size (=ybegin size=), so segments are written at their final
	// offset in any order and the size is known after the first header.
	layoutParallel
	// layoutSequential: the articles are not self-describing; segments are
	// appended in order by a single worker and the size is only known once the
	// last one is written.
	layoutSequential
)

// extent is a half-open byte range [start, end).
type extent struct{ start, end int64 }

// segWriter writes one decoded article at its place in the destination and
// refuses to run past the extent the article declared.
type segWriter struct {
	w      io.WriterAt
	off    int64
	remain int64
}

func (s *segWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > s.remain {
		return 0, errors.New("nzb: assembled output exceeds the declared size")
	}
	n, err := s.w.WriteAt(p, s.off)
	s.off += int64(n)
	s.remain -= int64(n)
	return n, err
}

// Assembly is a background, parallel, progressive download of one NZB file
// into an io.WriterAt. Segments are fetched over several NNTP connections and
// written at the offset their yEnc =ypart header declares, so the file can be
// read while it is still being assembled: WaitRange blocks only until the
// requested byte range is present, and a waiter for a region far from the
// download cursor steers the workers there first.
//
// An Assembly is shared state: any number of readers may call Size/WaitRange
// concurrently. A reader giving up (its context ending) never stops the
// download — only Cancel, Session.Close, or the parent context passed to
// StartAssembly does.
type Assembly struct {
	cfg      ServerConfig
	segs     []Segment
	dst      io.WriterAt
	capBytes int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{} // closed once every worker has exited

	mu        sync.Mutex
	changed   chan struct{} // closed and replaced on every state change
	state     []segStatus
	pending   int // segments in segPending
	remaining int // segments not yet segDone
	next      int // scan cursor of the next pick (the elevator head)
	alive     int // running workers
	layout    layoutMode
	seqOwner  int // worker that owns a layoutSequential assembly
	seqOff    int64
	total     int64 // decoded file size; valid once sizeKnown
	sizeKnown bool
	partSize  int64    // decoded size of the first article: seek estimator
	covered   []extent // sorted, merged ranges that are fully written
	complete  bool     // every byte of [0, total) is present
	err       error    // first failure (including cancellation)
}

// StartAssembly begins assembling the file named name into dst in the
// background and returns immediately. Up to ServerConfig.Connections NNTP
// connections (default 4, capped at 16) fetch segments in parallel; extra
// connections the server refuses are tolerated.
//
// ctx bounds the whole download — pass a context that outlives any single
// reader (not an HTTP request's), because the assembly is shared by every
// reader of the file. dst must support concurrent WriteAt at disjoint offsets
// (an *os.File does).
func (sess *Session) StartAssembly(ctx context.Context, name string, dst io.WriterAt) (*Assembly, error) {
	target, err := sess.file(name)
	if err != nil {
		return nil, err
	}
	n := len(target.Segments)
	actx, cancel := context.WithCancel(ctx)
	a := &Assembly{
		cfg:       sess.cfg,
		segs:      target.Segments,
		dst:       dst,
		capBytes:  sizeCap(target),
		ctx:       actx,
		cancel:    cancel,
		done:      make(chan struct{}),
		changed:   make(chan struct{}),
		state:     make([]segStatus, n),
		pending:   n,
		remaining: n,
	}
	if n == 0 {
		// Nothing to download: an empty, complete file.
		a.layout = layoutParallel
		a.sizeKnown = true
		a.complete = true
		cancel()
		close(a.done)
		return a, nil
	}

	conns := workerCount(sess.cfg.Connections, n)
	a.alive = conns
	stopAfter := context.AfterFunc(actx, func() { a.fail(actx.Err()) })
	a.wg.Add(conns)
	for id := range conns {
		go a.worker(id)
	}
	go func() {
		a.wg.Wait()
		stopAfter()
		cancel()
		close(a.done)
	}()
	return a, nil
}

// Cancel stops the download and closes its connections. Ranges that were
// already fully written stay readable; WaitRange on anything else fails.
// It is a no-op once the assembly has completed.
func (a *Assembly) Cancel() { a.fail(context.Canceled) }

// Done is closed once every worker has exited and the connections are
// released; after that nothing writes to dst any more.
func (a *Assembly) Done() <-chan struct{} { return a.done }

// Err returns the failure that stopped the assembly, or nil while it is
// running or after it completed successfully.
func (a *Assembly) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// Complete reports whether every byte of the file is present.
func (a *Assembly) Complete() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.complete
}

// Size blocks until the decoded file size is known and returns it. For
// self-describing (multi-part yEnc) files that is as soon as the first article
// header has been read; otherwise it is when the assembly completes. It
// returns the assembly's error if it failed first, or ctx.Err().
func (a *Assembly) Size(ctx context.Context) (int64, error) {
	for {
		a.mu.Lock()
		if a.sizeKnown {
			t := a.total
			a.mu.Unlock()
			return t, nil
		}
		if a.err != nil {
			err := a.err
			a.mu.Unlock()
			return 0, err
		}
		ch := a.changed
		a.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// WaitRange blocks until bytes [off, off+n) of the file are fully written,
// then returns nil — it does not wait for the rest of the file. If the range
// is not yet present it moves the download cursor to it. It returns the
// assembly's error when the assembly failed before the range was written and
// ctx.Err() when ctx ends first; the latter never affects the download.
func (a *Assembly) WaitRange(ctx context.Context, off, n int64) error {
	if n <= 0 {
		return nil
	}
	end := off + n
	a.mu.Lock()
	known, total := a.sizeKnown, a.total
	a.mu.Unlock()
	if known && end > total {
		return fmt.Errorf("nzb: range [%d,%d) is beyond the %d-byte file", off, end, total)
	}
	for off < end {
		runEnd, err := a.awaitRun(ctx, off)
		if err != nil {
			return err
		}
		off = runEnd
	}
	return nil
}

// WaitAvailable blocks until the byte at off is written and returns how many
// bytes starting there are present without a gap, at most limit. It is the
// streaming primitive: a reader gets whatever is on disk right now instead of
// waiting for a full buffer's worth, so it never stalls on bytes that exist.
// Errors are as for WaitRange.
func (a *Assembly) WaitAvailable(ctx context.Context, off, limit int64) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	runEnd, err := a.awaitRun(ctx, off)
	if err != nil {
		return 0, err
	}
	return min(runEnd-off, limit), nil
}

// awaitRun blocks until the byte at off is written and returns the end of the
// contiguous written run containing it. While it is missing it steers the
// download toward off.
func (a *Assembly) awaitRun(ctx context.Context, off int64) (int64, error) {
	for {
		a.mu.Lock()
		if i := a.runIndexLocked(off); i >= 0 {
			end := a.covered[i].end
			a.mu.Unlock()
			return end, nil
		}
		if a.err != nil {
			err := a.err
			a.mu.Unlock()
			return 0, err
		}
		if a.sizeKnown && off >= a.total {
			total := a.total
			a.mu.Unlock()
			return 0, fmt.Errorf("nzb: offset %d is beyond the %d-byte file", off, total)
		}
		a.hintLocked(off)
		ch := a.changed
		a.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// runIndexLocked returns the index of the covered run containing byte off, or
// -1 when that byte is not written yet.
func (a *Assembly) runIndexLocked(off int64) int {
	i := sort.Search(len(a.covered), func(i int) bool { return a.covered[i].end > off })
	if i < len(a.covered) && a.covered[i].start <= off {
		return i
	}
	return -1
}

// hintLocked moves the pick cursor to the segment expected to hold byte pos,
// so workers fetch what a reader is waiting for before continuing the
// sequential sweep (which then wraps around to fill the skipped region). The
// estimate assumes equally sized articles; when it is off the sweep still
// reaches the range, just not first. Correctness never depends on it.
func (a *Assembly) hintLocked(pos int64) {
	if a.layout != layoutParallel || a.partSize <= 0 || a.pending == 0 {
		return
	}
	idx := int(min(pos/a.partSize, int64(len(a.state)-1)))
	for j := idx; j < len(a.state); j++ {
		if a.state[j] == segPending {
			a.next = j
			return
		}
	}
}

// addCoveredLocked merges e into the sorted, disjoint covered set.
func (a *Assembly) addCoveredLocked(e extent) {
	s := a.covered
	i := sort.Search(len(s), func(i int) bool { return s[i].end >= e.start })
	j := i
	for j < len(s) && s[j].start <= e.end {
		e.start = min(e.start, s[j].start)
		e.end = max(e.end, s[j].end)
		j++
	}
	a.covered = slices.Replace(s, i, j, e)
}

func (a *Assembly) notifyLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}

func (a *Assembly) fail(err error) {
	a.mu.Lock()
	a.failLocked(err)
	a.mu.Unlock()
}

// failLocked records the first failure, wakes every waiter and cancels the
// workers. A completed assembly can no longer fail.
func (a *Assembly) failLocked(err error) {
	if a.err != nil || a.complete {
		return
	}
	a.err = err
	a.notifyLocked()
	a.cancel()
}

// finalizeLocked runs when the last segment is written: the size becomes
// known for sequential assemblies and the written ranges must tile the file.
func (a *Assembly) finalizeLocked() {
	if a.layout == layoutSequential {
		a.total = a.seqOff
		a.sizeKnown = true
	}
	var got int64
	for _, e := range a.covered {
		got += e.end - e.start
	}
	tiled := a.total == 0 && len(a.covered) == 0 ||
		len(a.covered) == 1 && a.covered[0].start == 0 && a.covered[0].end == a.total
	if !tiled {
		a.failLocked(fmt.Errorf("nzb: assembled %d of %d bytes: segments missing or inconsistent", got, a.total))
		return
	}
	a.complete = true
}

// requeue returns an abandoned segment to the pending pool.
func (a *Assembly) requeue(idx int) {
	a.mu.Lock()
	a.state[idx] = segPending
	a.pending++
	a.next = min(a.next, idx)
	a.notifyLocked()
	a.mu.Unlock()
}

// nextSegment blocks until there is a segment for worker id to fetch. ok is
// false when the worker should exit (finished, failed, cancelled).
func (a *Assembly) nextSegment(id int) (idx int, ok bool) {
	for {
		a.mu.Lock()
		if a.err != nil || a.complete || a.ctx.Err() != nil ||
			(a.layout == layoutSequential && id != a.seqOwner) {
			a.mu.Unlock()
			return 0, false
		}
		switch {
		case a.layout == layoutUnknown:
			// Exactly one worker fetches the first article alone: its header
			// decides how the rest are placed. The others wait for it.
			if a.state[0] == segPending {
				a.state[0] = segInflight
				a.pending--
				a.mu.Unlock()
				return 0, true
			}
		case a.pending > 0:
			if idx = a.pickLocked(); idx >= 0 {
				a.mu.Unlock()
				return idx, true
			}
		}
		ch := a.changed
		a.mu.Unlock()
		select {
		case <-ch:
		case <-a.ctx.Done():
			return 0, false
		}
	}
}

// pickLocked claims the first pending segment at or after the cursor,
// wrapping around, and advances the cursor past it.
func (a *Assembly) pickLocked() int {
	n := len(a.state)
	for i := range n {
		j := a.next + i
		if j >= n {
			j -= n
		}
		if a.state[j] == segPending {
			a.state[j] = segInflight
			a.pending--
			a.next = j + 1
			if a.next >= n {
				a.next = 0
			}
			return j
		}
	}
	return -1
}

// asmConn is a worker's NNTP connection plus the hook that aborts it when the
// assembly is cancelled.
type asmConn struct {
	c    *Client
	stop func() bool
}

func (a *Assembly) dial() (*asmConn, error) {
	c, err := DialContext(a.ctx, a.cfg)
	if err != nil {
		return nil, err
	}
	return &asmConn{c: c, stop: context.AfterFunc(a.ctx, c.abort)}, nil
}

func (ac *asmConn) close(graceful bool) {
	ac.stop()
	if graceful {
		_ = ac.c.Close()
		return
	}
	ac.c.abort()
}

// worker is one NNTP connection's fetch loop.
func (a *Assembly) worker(id int) {
	defer a.wg.Done()

	// Dial eagerly so extra connections come up while the first article is
	// being fetched. A refused connection only costs parallelism.
	ac, err := a.dial()
	if err != nil {
		a.workerExit(id, err)
		return
	}
	defer func() {
		if ac != nil {
			ac.close(a.ctx.Err() == nil)
		}
	}()

	for {
		idx, ok := a.nextSegment(id)
		if !ok {
			a.workerExit(id, nil)
			return
		}
		err := a.fetch(id, ac.c, idx)
		if err == nil {
			continue
		}
		if a.ctx.Err() != nil {
			a.workerExit(id, nil)
			return
		}
		if isConnErr(err) {
			// Dead or stale connection: re-dial once and retry the segment.
			// Retrying is safe — the segment is rewritten at the same offset
			// and only counts as present once it decodes cleanly.
			ac.close(false)
			ac = nil
			nc, derr := a.dial()
			if derr != nil {
				a.requeue(idx)
				a.workerExit(id, fmt.Errorf("reconnect after connection error: %w", derr))
				return
			}
			ac = nc
			if err = a.fetch(id, ac.c, idx); err == nil {
				continue
			}
			if a.ctx.Err() != nil {
				a.workerExit(id, nil)
				return
			}
		}
		a.fail(fmt.Errorf("nzb: segment %s: %w", a.segs[idx].MessageID, err))
		a.workerExit(id, nil)
		return
	}
}

// workerExit accounts for a worker leaving. cause is non-nil when it left
// because it could not connect; the assembly only fails once the last worker
// is gone with work still outstanding.
func (a *Assembly) workerExit(id int, cause error) {
	if cause != nil {
		logging.For("nzb").Debug("nzb worker retired", "worker", id, "err", cause)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.alive--
	if a.alive > 0 || a.complete || a.err != nil {
		return
	}
	switch {
	case cause != nil:
		a.failLocked(fmt.Errorf("nzb: %w", cause))
	case a.ctx.Err() != nil:
		a.failLocked(a.ctx.Err())
	default:
		a.failLocked(errors.New("nzb: assembly stopped before all segments were fetched"))
	}
}

// fetch downloads segment idx on c and records it as present.
func (a *Assembly) fetch(id int, c *Client, idx int) error {
	var ext extent
	_, n, err := c.body(a.segs[idx].MessageID, func(h yencHeader) (io.Writer, error) {
		w, e, err := a.openSegment(id, idx, h)
		ext = e
		return w, err
	})
	if err != nil {
		return err
	}
	return a.completeSegment(idx, ext, n)
}

// openSegment is called with the article's headers parsed, before any data is
// written. It fixes the file layout on the first article and returns the
// writer (and extent) for this one.
func (a *Assembly) openSegment(id, idx int, h yencHeader) (io.Writer, extent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.layout == layoutUnknown {
		if selfDescribing(h, len(a.segs)) {
			if h.Size > a.capBytes {
				return nil, extent{}, fmt.Errorf("nzb: file size %d exceeds declared size %d bytes", h.Size, a.capBytes)
			}
			a.layout = layoutParallel
			a.total = h.Size
			a.sizeKnown = true
			a.partSize = h.Size
			if h.HasPart {
				a.partSize = h.End - h.Begin + 1
			}
		} else {
			a.layout = layoutSequential
			a.seqOwner = id
		}
		a.notifyLocked()
	}

	if a.layout == layoutSequential {
		return &segWriter{w: a.dst, off: a.seqOff, remain: a.capBytes - a.seqOff}, extent{start: a.seqOff}, nil
	}

	var ext extent
	switch {
	case h.HasPart:
		if h.Size != a.total {
			return nil, extent{}, fmt.Errorf("nzb: article declares file size %d, expected %d", h.Size, a.total)
		}
		if h.Begin < 1 || h.End < h.Begin || h.End > a.total {
			return nil, extent{}, fmt.Errorf("nzb: article range %d-%d is outside the %d-byte file", h.Begin, h.End, a.total)
		}
		ext = extent{start: h.Begin - 1, end: h.End}
	case len(a.segs) == 1 && h.Size == a.total:
		ext = extent{start: 0, end: a.total}
	default:
		return nil, extent{}, errNoPartInfo
	}
	return &segWriter{w: a.dst, off: ext.start, remain: ext.end - ext.start}, ext, nil
}

// selfDescribing reports whether the first article's yEnc headers reliably
// place every segment of a file of nsegs articles, so they can be downloaded
// and written out of order. It must declare the whole file's size, and for a
// multi-part file its part must start the file and leave room for the rest —
// anything else (no =ypart, a size= that is just the part's size, a first
// segment that is not the file's first bytes) is not trusted and falls back to
// ordered appends.
func selfDescribing(h yencHeader, nsegs int) bool {
	if h.Size <= 0 {
		return false
	}
	if nsegs == 1 {
		return !h.HasPart || (h.Begin == 1 && h.End == h.Size)
	}
	return h.HasPart && h.Begin == 1 && h.End >= h.Begin && h.End < h.Size
}

// completeSegment records segment idx as fully written (n decoded bytes, all
// verified by the yEnc trailer).
func (a *Assembly) completeSegment(idx int, ext extent, n int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.layout == layoutParallel {
		if want := ext.end - ext.start; n != want {
			return fmt.Errorf("nzb: article decoded %d bytes, its =ypart declares %d", n, want)
		}
	} else {
		ext.end = ext.start + n
		a.seqOff = ext.end
	}
	a.state[idx] = segDone
	a.remaining--
	if n > 0 {
		a.addCoveredLocked(ext)
	}
	if a.remaining == 0 {
		a.finalizeLocked()
	}
	a.notifyLocked()
	return nil
}
