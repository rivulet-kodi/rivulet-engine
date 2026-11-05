// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package engine

// In-RAM, bounded torrent storage backend (opt-in).
//
// This file implements a storage.ClientImplCloser that keeps piece data in
// memory instead of on disk, bounded by a single byte budget shared across all
// torrents. It lets the server stream without ever writing piece data to disk
// (mobile / Termux / low-disk / HuggingFace), mirroring Elementum's sliding RAM
// window. It is enabled only when Config.MemoryCacheSize > 0; the default path
// keeps storage.NewFileByInfoHash unchanged.
//
// Mechanism (proven against anacrolix/torrent v1.61.0):
//
// The backend combines two cooperating bounds that anacrolix is explicitly
// designed to support for capacity-limited storage:
//
//  1. Request bounding via TorrentImpl.Capacity. We hand anacrolix a single
//     shared *func() (cap int64, capped bool) returning (budget, true). The
//     request strategy (internal/request-strategy/order.go GetRequestablePieces)
//     walks pieces in priority order, subtracts each piece length from the
//     remaining capacity, and stops once the budget is exhausted. Because the
//     order is keyed on reader proximity, only the highest-priority pieces that
//     fit in the budget are ever requested; as a reader advances, the window
//     slides forward. Sharing one Capacity pointer across torrents makes the
//     budget global (torrent-piece-request-order.go keys the shared request
//     order on the Capacity pointer). The walk counts every piece it passes,
//     complete or evicted, so only what a stream is about to read is wanted:
//     the file is not downloaded whole, and the stream's window is half the
//     budget (engine.ensureDownloading, engine.streamReadahead).
//
//  2. A hard byte cap enforced by this storage via eviction of COMPLETE
//     pieces. On a write that would exceed the budget we free complete
//     piece(s): drop the buffer and flip the piece to not-complete. Victims
//     come in two classes. Pieces the reader has already consumed (read, or
//     skipped over by a forward seek) go first, least recently read first.
//     Read-ahead pieces — completed but not yet read — go only once no
//     consumed piece is left, farthest-ahead (highest index) first. Plain LRU
//     got this backwards (issue rivulet-kodi/plugin.video.rivulet#96): a
//     read-ahead piece kept its completion stamp while the pieces being played
//     took newer read stamps, so at high bitrates the window ahead of the
//     reader was evicted before the data behind it and a 4K stream stalled on
//     "piece not resident" re-downloads of the very pieces it was about to read.
//     Read-ahead pieces a forward seek skipped (below the reader position)
//     lead that class, as they will not be read. "Read" means read by a
//     player stream, which the engine reports through a memCursor, never a
//     storage ReadAt: anacrolix serves peer uploads through ReadAt as well,
//     and counting those reads let a peer's request write off the window
//     just ahead of the player (see memCursor). The piece a player stream is
//     reading is in neither class: it is pinned out of the heap until the
//     stream moves on, since the next download evicted it mid-read once it
//     was the only played piece left (memCursor.read).
//
// Concurrency model:
//
//   - Piece bytes are guarded by a per-piece RWMutex (memPiece.mu), so reads and
//     writes of different pieces — in particular of different torrents — never
//     contend, and a ReadAt copies under its own piece's read lock only.
//   - The budget (used) and the eviction order (an LRU min-heap of the complete
//     pieces) are guarded by one store-wide mutex (memStorage.mu). It is taken
//     only for the rare accounting events: first write into a piece, completion,
//     eviction, torrent close, and a player stream entering a new piece
//     (memCursor.read). ReadAt and chunk WriteAt never take it.
//   - Lock order is allocMu -> memStorage.mu -> memPiece.mu. A piece lock is a
//     leaf with respect to the other two: it is never held while acquiring
//     either of them.
//   - Read recency is an atomic stamp (memPiece.lastUsed); the heap keeps a
//     lower bound of it per piece and refreshes a stale key only when the piece
//     surfaces as the eviction candidate, so a repeat read costs no heap
//     operation and eviction is O(log n) amortised instead of a scan over every
//     piece. Moving a piece from the read-ahead class to the consumed class
//     happens in memCursor.read, under memStorage.mu, once per piece a stream
//     enters.
//
// Correctness — we never serve wrong or stale bytes:
//
//   - A non-resident piece reports Completion{Ok: true, Complete: false}, and its
//     ReadAt returns (0, errPieceEvicted), never (0, nil) — the storage.Piece /
//     storagePieceReader wrappers panic on a (0, nil) read.
//   - Evicting bytes is NOT enough on its own: anacrolix caches each piece's
//     completion and is not told of an eviction, so an evicted piece keeps
//     reading as available and reader.readAt retries the failing storage read
//     instead of waiting for the data. To close this, ReadAt on an evicted
//     piece calls the refetch hook (wired by the manager to
//     Torrent.Piece(i).UpdateCompletion), which re-reads the completion from
//     this storage: the piece turns incomplete, anacrolix re-requests it, and
//     the reader waits for the re-download. The hook returns a channel closed
//     once that resync has landed; the evicted read waits for it (instead of
//     polling on a fixed sleep) and returns, so anacrolix's next attempt sees
//     the piece as unavailable and blocks until it is downloaded. The wait is
//     capped by refetchBackoff, and repeated evicted reads of the same piece
//     are throttled by an escalating floor, so anacrolix's retry recursion
//     stays bounded even when the resync never lands. The hook used to
//     re-hash the piece (VerifyData), which failed on the empty piece and
//     logged a "finished hashing piece ... correct=false" per evicted read,
//     peer uploads included (plugin.video.rivulet#96). The hasher's own read of
//     an evicted piece goes through WriteTo, which fails fast without waiting.
//   - anacrolix's reader retried every failed read on capped storage, by
//     recursion, including a wait that failed because the stream's context
//     ended or the torrent closed: closing a stream whose read was waiting for
//     a piece overflowed the stack. Fixed upstream by anacrolix/torrent#1168:
//     only storage-read failures on a live torrent are retried now.
//   - We only ever evict COMPLETE pieces, so in-flight (partially written)
//     pieces are never dropped and anacrolix's chunk bookkeeping is never
//     invalidated mid-download.
//
// Bound vs. correctness: if every resident piece is still in-flight (rare; the
// Capacity request bound keeps the in-flight set small), eviction may find no
// complete victim and resident bytes briefly exceed the budget rather than
// dropping live data. This is the deliberate conservative direction — correct
// bytes always, with at most the bounded in-flight working set as overage.

import (
	"container/heap"
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// errPieceEvicted is returned by ReadAt when a piece's bytes are not resident
// (never written, or evicted to stay within budget). Returning a non-nil error
// with n == 0 is required by the storage.Piece / storagePieceReader wrappers
// (which panic on a 0, nil read) and signals anacrolix to re-fetch the piece.
var errPieceEvicted = errors.New("engine: memstorage: piece not resident")

// refetchSettle is the first throttle step applied to a repeated evicted read
// of the same piece (see memPiece.waitRefetch). It doubles per further read up
// to memStorage.refetchBackoff.
const refetchSettle = time.Millisecond

// memBufPool maps piece length (int64) -> *sync.Pool of []byte.
// Keyed by length because the final piece usually has a different length than
// the rest; handing back an undersized buffer would corrupt the next writer.
var memBufPool sync.Map

// getPieceBuf returns a pooled, zero-or-reused byte slice of exactly length n.
// No zeroing is needed: anacrolix writes every chunk before MarkComplete, and
// ReadAt only serves data once the piece is complete.
func getPieceBuf(n int64) []byte {
	v, ok := memBufPool.Load(n)
	if !ok {
		v, _ = memBufPool.LoadOrStore(n, &sync.Pool{New: func() any {
			b := make([]byte, n)
			return &b
		}})
	}
	return *(v.(*sync.Pool).Get().(*[]byte))
}

// putPieceBuf returns b to the pool it was drawn from (keyed by cap).
// The caller must not use b after this call.
func putPieceBuf(b []byte) {
	if v, ok := memBufPool.Load(int64(cap(b))); ok {
		v.(*sync.Pool).Put(&b)
	}
}

// writeToChunk is the scratch size memPiece.WriteTo copies through, so the
// destination writer (a hasher) is never called with a piece lock held.
const writeToChunk = 64 << 10

var writeToPool = sync.Pool{New: func() any {
	b := make([]byte, writeToChunk)
	return &b
}}

// memStorage is an opt-in storage.ClientImplCloser that keeps piece data in RAM
// bounded by a byte budget shared across all torrents it opens. It is safe for
// concurrent use: anacrolix calls ReadAt/WriteAt/Completion/MarkComplete on
// pieces concurrently across torrents.
type memStorage struct {
	capacity int64                   // hard byte budget across all torrents
	capFn    storage.TorrentCapacity // shared pointer handed to every torrent

	// mu guards the budget and the eviction order below, plus every
	// memPiece.key / memPiece.lruIdx. It also serialises the transitions that
	// move a piece in or out of the LRU or free its buffer. It is NOT taken on
	// the read or chunk-write fast paths (those use memPiece.mu only).
	mu   sync.Mutex
	used int64 // resident bytes currently accounted (sum of resident piece lengths)

	// lru is a min-heap (oldest recency first) of the resident, hash-verified
	// pieces. Eviction pops it exclusively, so in-flight (incomplete) pieces are
	// never visited.
	lru lruHeap

	// torrents maps each open torrent's lower-case hex infohash to its view,
	// so the engine can open a memCursor for a player stream (openCursor).
	torrents map[string]*memTorrent

	// tick is the recency clock: every read and completion takes the next
	// value. A counter, not wall time, because a coarse clock (Windows) gives
	// consecutive accesses the same timestamp and makes the victim arbitrary.
	tick atomic.Int64

	// refetch, when set by the manager, forces anacrolix to re-download a piece
	// whose bytes were evicted and returns a channel closed once the re-download
	// has been requested (nil when there is nothing to wait for). refetchBackoff
	// caps how long an evicted read waits for that signal and is the sleep used
	// when the hook returns no channel, so anacrolix's reader.readAt retry loop
	// cannot recurse into a stack overflow before the re-download is requested.
	// Both are written once before any read can occur and are zero in the direct
	// unit tests, which drive the backend without a live torrent.
	refetch        func(ih metainfo.Hash, piece int) <-chan struct{}
	refetchBackoff time.Duration
}

var _ storage.ClientImplCloser = (*memStorage)(nil)

// newMemStorage returns an in-RAM storage backend bounded by capacity bytes,
// shared across every torrent opened on it.
func newMemStorage(capacity int64) *memStorage {
	s := &memStorage{
		capacity:       capacity,
		torrents:       map[string]*memTorrent{},
		refetchBackoff: 100 * time.Millisecond,
	}
	// One shared Capacity function for all torrents => one global budget. The
	// pointer identity is what anacrolix uses to share the request-order/budget
	// across torrents, so it must be the same pointer for every OpenTorrent.
	capFn := func() (int64, bool) { return s.capacity, true }
	s.capFn = &capFn
	return s
}

// OpenTorrent binds a new torrent to the shared RAM budget. Only Piece, Close
// and Capacity are set; anacrolix falls back from PieceWithHash to Piece.
func (s *memStorage) OpenTorrent(
	_ context.Context,
	info *metainfo.Info,
	ih metainfo.Hash,
) (storage.TorrentImpl, error) {
	mt := &memTorrent{
		store:    s,
		infoHash: ih,
		pieces:   make([]atomic.Pointer[memPiece], info.NumPieces()),
		cursors:  map[*memCursor]struct{}{},
		readPos:  -1,
	}
	s.mu.Lock()
	s.torrents[ih.HexString()] = mt
	s.mu.Unlock()
	return storage.TorrentImpl{
		Piece:    mt.piece,
		Close:    mt.close,
		Capacity: s.capFn,
	}, nil
}

// Close drops all resident data. anacrolix calls each torrent's Close on Drop;
// this is the client-level Close (manager shutdown).
func (s *memStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Pool complete-piece buffers on shutdown; incomplete pieces are handled by
	// each torrent's close which anacrolix should have called before this.
	for _, mp := range s.lru {
		mp.lruIdx = -1
		mp.mu.Lock()
		if mp.data != nil {
			putPieceBuf(mp.data)
			mp.data = nil
		}
		mp.complete = false
		mp.mu.Unlock()
	}
	s.lru = nil
	s.used = 0
	return nil
}

// dropLocked frees a resident piece: removes it from the LRU, returns its
// buffer to the pool, decrements the budget, and flips it to not-complete so a
// subsequent ReadAt fails and anacrolix re-fetches it. Caller holds s.mu.
func (s *memStorage) dropLocked(mp *memPiece) {
	if mp.lruIdx >= 0 {
		heap.Remove(&s.lru, mp.lruIdx)
	}
	s.freeLocked(mp)
}

// freeLocked releases mp's buffer and accounting. mp must already be out of the
// LRU. Caller holds s.mu; mp.mu is taken here (order s.mu -> mp.mu), which waits
// for any in-flight ReadAt copy on that piece to finish.
func (s *memStorage) freeLocked(mp *memPiece) {
	mp.mu.Lock()
	if mp.data != nil {
		// Return buffer to pool; no zeroing needed — anacrolix writes all chunks
		// before MarkComplete and ReadAt only serves resident complete data.
		putPieceBuf(mp.data)
		s.used -= mp.length
		mp.data = nil
	}
	mp.complete = false
	mp.consumed.Store(false)
	mp.mu.Unlock()
}

// reserve accounts need bytes for a piece about to become resident, evicting
// complete pieces first so the budget still holds afterwards (barring the
// no-victim overage described in the file comment).
func (s *memStorage) reserve(need int64) {
	s.mu.Lock()
	s.evictLocked(need, nil)
	s.used += need
	s.mu.Unlock()
}

// evictLocked frees COMPLETE pieces until used+need fits within capacity, or
// no evictable piece remains. Victims come off the LRU heap, so in-flight
// (incomplete) pieces are never visited and each eviction is O(log n). keep, if
// non-nil, is never chosen (the piece whose completion triggered the pass).
// Caller holds s.mu.
func (s *memStorage) evictLocked(need int64, keep *memPiece) {
	if s.used+need <= s.capacity {
		return
	}
	// Park keep outside the heap while victims are chosen, then put it back.
	parked := keep != nil && keep.lruIdx >= 0
	if parked {
		heap.Remove(&s.lru, keep.lruIdx)
	}
	for s.used+need > s.capacity {
		victim := s.popOldestLocked()
		if victim == nil {
			break // nothing safe to evict; tolerate a transient overage
		}
		s.freeLocked(victim)
	}
	if parked {
		heap.Push(&s.lru, keep)
	}
}

// popOldestLocked removes and returns the next eviction victim: the consumed
// piece with the smallest lastUsed, else the read-ahead victim chosen by
// popAheadLocked, or nil when the LRU is empty.
//
// Repeat reads only bump memPiece.lastUsed (atomically, without s.mu); the heap key is
// a lower bound of it. A top whose lastUsed has moved past its key is re-keyed
// and sifted down; the first top whose key is current is then the true minimum,
// because every other key is a lower bound of that piece's lastUsed and is >=
// this one. Caller holds s.mu.
func (s *memStorage) popOldestLocked() *memPiece {
	for len(s.lru) > 0 {
		top := s.lru[0]
		if top.key >= aheadKeyBase {
			return s.popAheadLocked()
		}
		if lu := top.lastUsed.Load(); lu > top.key {
			top.key = lu
			heap.Fix(&s.lru, 0)
			continue
		}
		return heap.Pop(&s.lru).(*memPiece)
	}
	return nil
}

// aheadKeyBase separates the two eviction classes in the heap. A consumed
// piece's key is a tick (a read count, far below 2^62); a read-ahead piece's
// key is aheadKeyBase + (MaxInt32 - index), so every read-ahead piece sorts
// after every consumed one, and among themselves the highest index — the piece
// the reader needs last — is evicted first.
const aheadKeyBase = int64(1) << 62

func aheadKey(index int) int64 { return aheadKeyBase + int64(math.MaxInt32-index) }

// stamp records a read of a consumed piece for eviction ordering, lock-free,
// and is a no-op when mp already holds the newest stamp (a stream reading one
// piece sequentially therefore never writes the shared clock). A read-ahead
// piece is left alone: ReadAt also serves peer uploads, and only a player
// stream's memCursor may consume it.
func (s *memStorage) stamp(mp *memPiece) {
	if !mp.consumed.Load() {
		return
	}
	if mp.lastUsed.Load() != s.tick.Load() {
		mp.lastUsed.Store(s.tick.Add(1))
	}
}

// consumeLocked moves mp from the read-ahead class to the consumed class with
// the newest tick; an already consumed piece only refreshes its tick. Caller
// holds s.mu.
func (s *memStorage) consumeLocked(mp *memPiece) {
	stamp := s.tick.Add(1)
	mp.lastUsed.Store(stamp)
	if mp.consumed.Load() {
		return
	}
	mp.consumed.Store(true)
	if mp.lruIdx >= 0 {
		mp.key = stamp
		heap.Fix(&s.lru, mp.lruIdx)
	}
}

// popAheadLocked removes and returns the read-ahead victim once no consumed
// piece is left: a piece below its torrent's reader position (skipped by a
// forward seek), else the one farthest ahead (the heap top). It scans the
// heap, but only in this state — normally consumed pieces are evicted first,
// in O(log n). A scan rather than a heap key because the reader position moves
// and would invalidate every ahead key; tying the skipped test to the position
// at eviction time also keeps a second reader of the same torrent (a probe of
// the file's tail) from writing off the first reader's window. Caller holds
// s.mu and the heap is non-empty.
func (s *memStorage) popAheadLocked() *memPiece {
	victim := 0
	for i, mp := range s.lru {
		if mp.index < mp.mt.readPos && (s.lru[victim].index >= s.lru[victim].mt.readPos || mp.index < s.lru[victim].index) {
			victim = i
		}
	}
	return heap.Remove(&s.lru, victim).(*memPiece)
}

// lruHeap is a min-heap of resident complete pieces ordered by memPiece.key.
// It implements heap.Interface and keeps memPiece.lruIdx in sync so an
// arbitrary piece can be removed in O(log n). All access is under memStorage.mu.
type lruHeap []*memPiece

func (h lruHeap) Len() int           { return len(h) }
func (h lruHeap) Less(i, j int) bool { return h[i].key < h[j].key }
func (h lruHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].lruIdx = i
	h[j].lruIdx = j
}

func (h *lruHeap) Push(x any) {
	mp := x.(*memPiece)
	mp.lruIdx = len(*h)
	*h = append(*h, mp)
}

func (h *lruHeap) Pop() any {
	old := *h
	n := len(old)
	mp := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	mp.lruIdx = -1
	return mp
}

// memTorrent is the per-torrent view onto the shared store. pieces is indexed by
// piece index and lazily populated; the slice never grows, so the slots are
// stable and are filled lock-free (compare-and-swap).
type memTorrent struct {
	store    *memStorage
	infoHash metainfo.Hash
	pieces   []atomic.Pointer[memPiece]

	// cursors are the open player streams on this torrent (see memCursor), and
	// readPos is the lowest piece any of them last read (-1 while none has
	// read). Once only read-ahead pieces are left, popAheadLocked evicts those
	// below readPos first: every stream has moved past them (a forward seek
	// skipped them), so they will not be read. The minimum, not the latest
	// read, so a second stream on the same torrent (a player's probe of the
	// file's tail) cannot write off the first stream's window. closed is set
	// once the torrent is dropped. All guarded by store.mu.
	cursors map[*memCursor]struct{}
	readPos int
	closed  bool
}

// updateReadPosLocked recomputes readPos from the open cursors. Caller holds
// store.mu.
func (mt *memTorrent) updateReadPosLocked() {
	mt.readPos = -1
	for c := range mt.cursors {
		if c.pos >= 0 && (mt.readPos < 0 || c.pos < mt.readPos) {
			mt.readPos = c.pos
		}
	}
}

// memCursor is one player stream's position in a torrent's in-RAM cache. The
// engine reports every piece a stream reads through it, and only those reads
// move a piece from the read-ahead class to the consumed class.
//
// Classifying on storage ReadAt instead (as before) cannot tell the player from
// a peer: anacrolix serves uploads through the same ReadAt, and the engine
// seeds. A peer's request for a read-ahead piece then marked it consumed, so it
// was evicted before the player reached it, and moved readPos past the player,
// so the pieces just ahead of it read as skipped and went first — the
// "piece not resident" refetch bursts of rivulet-kodi/plugin.video.rivulet#96.
type memCursor struct {
	mt *memTorrent

	// pos is the last piece this stream read (-1 before its first read) and
	// closed is set by close. pinned is the piece the stream is reading now,
	// held out of eviction until the stream moves past it (nil when there is
	// none resident to hold). All guarded by store.mu.
	pos    int
	closed bool
	pinned *memPiece

	// seen is the piece the stream holds pinned (-1 when none), owned by the
	// reading goroutine (a stream is read by one goroutine at a time), so a
	// run of reads within one piece takes no lock.
	seen int
}

// openCursor registers a player stream on the torrent ih (lower-case hex) and
// returns its cursor, or nil when s is nil or the torrent is not open on it.
// The caller must close the cursor when the stream ends.
func (s *memStorage) openCursor(ih string) *memCursor {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mt := s.torrents[ih]
	if mt == nil {
		return nil
	}
	c := &memCursor{mt: mt, pos: -1, seen: -1}
	mt.cursors[c] = struct{}{}
	return c
}

// read records that the stream read pieces first..last. Each resident complete
// piece it has moved past (first..last-1, and the piece it held pinned until
// now) moves to the consumed class with the newest tick; last, the piece it is
// reading now, is pinned; and the stream's position becomes last. A nil cursor
// is a no-op.
//
// The current piece is pinned rather than consumed because consumed pieces are
// the first victims: once the played pieces behind the reader were gone, the
// piece being played was the only consumed one left, and the next piece to
// download evicted it under the player mid-read — the repeated
// "initial read failed" refetches of rivulet-kodi/plugin.video.rivulet#96 with
// a full budget.
func (c *memCursor) read(first, last int) {
	if c == nil || (first == c.seen && last == c.seen) {
		return
	}
	s := c.mt.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.closed || c.mt.closed {
		return
	}
	for i := max(first, 0); i < last && i < len(c.mt.pieces); i++ {
		if mp := c.mt.pieces[i].Load(); mp != nil && mp.lruIdx >= 0 {
			s.consumeLocked(mp)
		}
	}
	c.pos = last
	c.mt.updateReadPosLocked()
	var cur *memPiece
	if last >= 0 && last < len(c.mt.pieces) {
		cur = c.mt.pieces[last].Load()
	}
	if cur != c.pinned {
		s.unpinLocked(c.pinned, true)
		c.pinned = nil
		if cur != nil && s.pinLocked(cur) {
			c.pinned = cur
		}
	}
	// Skip the lock for further reads only while the piece is held: a piece
	// evicted between the read and this call is pinned once it is back.
	c.seen = -1
	if c.pinned != nil {
		c.seen = last
	}
}

// close unregisters the stream; its position no longer holds back readPos, and
// the piece it held returns to the class it had (the stream did not finish it).
// Idempotent; a nil cursor is a no-op.
func (c *memCursor) close() {
	if c == nil {
		return
	}
	s := c.mt.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	s.unpinLocked(c.pinned, false)
	c.pinned = nil
	delete(c.mt.cursors, c)
	c.mt.updateReadPosLocked()
}

// pinLocked holds mp out of eviction while a stream reads it, and reports
// whether it did: a piece that is not resident and complete has nothing to
// hold. Pins nest (two streams in one piece). A piece is in the eviction heap
// exactly while it is resident, complete and unpinned. Caller holds s.mu.
func (s *memStorage) pinLocked(mp *memPiece) bool {
	if mp.pins == 0 {
		if mp.lruIdx < 0 {
			return false
		}
		heap.Remove(&s.lru, mp.lruIdx)
	}
	mp.pins++
	return true
}

// unpinLocked releases one pin on mp (a nil mp is a no-op). Once the last pin
// goes, a piece still resident and complete re-enters the eviction heap: in
// the consumed class with the newest tick when played (the stream moved past
// it), else in the class it had. Caller holds s.mu.
func (s *memStorage) unpinLocked(mp *memPiece, played bool) {
	if mp == nil {
		return
	}
	mp.pins--
	if mp.pins > 0 {
		return
	}
	mp.mu.RLock()
	resident := mp.complete && mp.data != nil
	mp.mu.RUnlock()
	if !resident {
		return
	}
	switch {
	case played:
		stamp := s.tick.Add(1)
		mp.lastUsed.Store(stamp)
		mp.consumed.Store(true)
		mp.key = stamp
	case mp.consumed.Load():
		mp.key = mp.lastUsed.Load()
	default:
		mp.key = aheadKey(mp.index)
	}
	heap.Push(&s.lru, mp)
}

// piece returns the (lazily created) memPiece for p. anacrolix calls it for
// every chunk write and state query, so it must not take a lock.
func (mt *memTorrent) piece(p metainfo.Piece) storage.PieceImpl {
	idx := p.Index()
	slot := &mt.pieces[idx]
	if mp := slot.Load(); mp != nil {
		return mp
	}
	mp := &memPiece{store: mt.store, mt: mt, index: idx, length: p.Length(), lruIdx: -1}
	if slot.CompareAndSwap(nil, mp) {
		return mp
	}
	return slot.Load()
}

// close drops every resident piece belonging to this torrent (called by
// anacrolix when the torrent is dropped).
func (mt *memTorrent) close() error {
	s := mt.store
	s.mu.Lock()
	defer s.mu.Unlock()
	mt.closed = true
	if key := mt.infoHash.HexString(); s.torrents[key] == mt {
		delete(s.torrents, key)
	}
	for i := range mt.pieces {
		if mp := mt.pieces[i].Load(); mp != nil {
			s.dropLocked(mp)
		}
	}
	return nil
}

// memPiece holds one piece's bytes in RAM. data and complete are guarded by mu;
// key and lruIdx by store.mu; lastUsed is updated lock-free via atomic after
// each successful read.
type memPiece struct {
	store  *memStorage
	mt     *memTorrent // owning torrent (for infohash on evicted-read refetch)
	index  int         // piece index (for evicted-read refetch)
	length int64       // full length of this piece (bytes)

	// mu guards data and complete. Readers share it; WriteAt, eviction and the
	// completion transitions take it exclusively. Never held while acquiring
	// allocMu or store.mu.
	mu       sync.RWMutex
	data     []byte // nil when not resident (never written or evicted)
	complete bool   // hash-verified by anacrolix and still resident

	// allocMu serialises the first write into a non-resident piece so racing
	// chunk writers reserve the budget (and allocate the buffer) only once.
	allocMu sync.Mutex

	// lastUsed records the store tick of the most recent read of this piece
	// since it was consumed (its completion tick until then), updated
	// atomically without holding any lock. popOldestLocked uses it to pick the
	// least-recently-read consumed piece.
	lastUsed atomic.Int64

	// consumed is true once a player stream has moved past this piece
	// (reported by its memCursor, not by ReadAt, which peer uploads share);
	// until then a complete piece is read-ahead and evicted last. Set under
	// store.mu, cleared when the buffer is freed or the piece re-completes;
	// loaded lock-free by stamp.
	consumed atomic.Bool

	// key and lruIdx are the piece's heap key (a lower bound of lastUsed) and
	// position in store.lru (-1 when not in the LRU). pins counts the streams
	// reading this piece now (see memCursor.pinned); a pinned piece stays out
	// of the LRU. Guarded by store.mu.
	key    int64
	lruIdx int
	pins   int

	// evictedReads counts consecutive reads that found the piece non-resident
	// in rapid succession (a retry spin), and evictedAt is when the last such
	// wait finished (unix nanos), so waitRefetch can escalate its throttle for
	// a spin but not for an unrelated later read. Reset once the piece is back.
	evictedReads atomic.Int32
	evictedAt    atomic.Int64
}

var _ storage.PieceImpl = (*memPiece)(nil)
var _ io.WriterTo = (*memPiece)(nil)

// lockResident returns with mp.mu held for writing and mp.data allocated. On
// first write the budget is reserved (evicting complete pieces) and the buffer
// allocated with no piece lock held, preserving the allocMu -> store.mu ->
// memPiece.mu order.
func (mp *memPiece) lockResident() {
	mp.mu.Lock()
	if mp.data != nil {
		return
	}
	mp.mu.Unlock()

	mp.allocMu.Lock()
	defer mp.allocMu.Unlock()
	mp.mu.Lock()
	if mp.data != nil {
		return // a racing writer made it resident while we waited
	}
	mp.mu.Unlock()

	// First write: make room for this piece, then allocate it resident. mp is
	// not in the LRU (incomplete), so eviction cannot target it.
	mp.store.reserve(mp.length)
	buf := getPieceBuf(mp.length) // pooled; no zeroing needed (all chunks written before MarkComplete)
	mp.mu.Lock()
	// Only allocMu holders install a buffer and we hold it, so data is still
	// nil here (an eviction or torrent close can only have cleared it).
	mp.data = buf
	mp.evictedReads.Store(0)
}

// WriteAt stores chunk bytes at off within the piece, allocating the full piece
// buffer from the pool (and making room in the budget) on first write.
func (mp *memPiece) WriteAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("engine: memstorage: negative offset")
	}
	if off >= mp.length {
		return 0, io.EOF
	}
	mp.lockResident()
	n := copy(mp.data[off:], b)
	mp.mu.Unlock()
	return n, nil
}

// ReadAt serves bytes from the resident buffer. It holds only this piece's read
// lock, so reads of other pieces and torrents — and of this piece — run in
// parallel, and no store-wide lock is involved. Recency (for eviction ordering)
// is recorded via an atomic stamp after releasing the lock. A non-resident
// piece returns errPieceEvicted so anacrolix re-fetches it rather than reading
// stale data.
func (mp *memPiece) ReadAt(b []byte, off int64) (int, error) {
	s := mp.store
	mp.mu.RLock()
	// Fast path: resident bytes in range. This is the ONLY way ReadAt returns a
	// non-zero count; every other outcome is a zero-byte "miss" handled below.
	if mp.data != nil && off >= 0 && off < mp.length {
		n := copy(b, mp.data[off:mp.length])
		mp.mu.RUnlock()
		// A tiny lag between the copy and the stamp is acceptable for eviction
		// ordering.
		s.stamp(mp)
		if int64(n) < int64(len(b)) {
			// Filled to the end of the piece without satisfying the whole request.
			return n, io.EOF
		}
		return n, nil
	}
	evicted := mp.data == nil
	mp.mu.RUnlock()

	// Handle non-eviction cases immediately — no refetch or sleep needed.
	if off < 0 {
		return 0, errors.New("engine: memstorage: negative offset")
	}
	if !evicted {
		// Resident piece, offset at/past piece end — legitimate EOF; the
		// read is satisfied and anacrolix will not spin on this path.
		// Returning immediately avoids a spurious refetch and the backoff
		// that were incorrectly applied to this branch.
		return 0, io.EOF
	}
	// Only the genuine evicted-piece case reaches here. Force a re-download:
	// the hook resyncs anacrolix's cached completion from this storage, the
	// piece turns incomplete and is re-requested, so reader.readAt waits for
	// the re-fetch instead of retrying a read that cannot succeed. Wait until
	// that has landed rather than sleeping blindly.
	if s.refetch != nil {
		mp.waitRefetch(s.refetch(mp.mt.infoHash, mp.index), s.refetchBackoff)
	}
	return 0, errPieceEvicted
}

// waitRefetch blocks an evicted read until the refetch it triggered has landed
// (done closed), then returns so anacrolix retries and finds the piece pending.
//
// It is bounded two ways so the retry recursion in reader.readAt cannot run
// away if the re-pend does not take effect: the wait for done is capped at
// backoff (also the plain sleep when there is no done to wait on), and an
// evicted read that follows the previous one within backoff — a retry spin, not
// a fresh seek — additionally sleeps an escalating floor (refetchSettle,
// doubling per repeat, capped at backoff). backoff <= 0 disables both.
func (mp *memPiece) waitRefetch(done <-chan struct{}, backoff time.Duration) {
	if backoff <= 0 {
		return
	}
	if last := mp.evictedAt.Load(); last != 0 && time.Now().UnixNano()-last > int64(backoff) {
		mp.evictedReads.Store(0) // idle since the last one: not a spin
	}
	reads := mp.evictedReads.Add(1)
	defer func() { mp.evictedAt.Store(time.Now().UnixNano()) }()
	if done == nil {
		time.Sleep(backoff)
		return
	}
	timer := time.NewTimer(backoff)
	select {
	case <-done:
	case <-timer.C:
	}
	timer.Stop()
	if reads > 1 {
		floor := backoff
		if shift := reads - 2; shift < 31 {
			floor = min(backoff, refetchSettle<<shift)
		}
		time.Sleep(floor)
	}
}

// WriteTo streams the resident bytes to w (io.WriterTo). anacrolix hashes a
// piece through it; implementing it keeps a hash of an evicted piece (a
// VerifyData) out of ReadAt, whose refetch wait would stall the hasher on a
// re-download while the hash can only fail. A non-resident piece fails fast
// with errPieceEvicted (so the hash fails and the piece is re-requested). The
// copy goes through a bounded scratch buffer so w is never called with the
// piece lock held and a slow writer cannot stall eviction.
func (mp *memPiece) WriteTo(w io.Writer) (int64, error) {
	bufp := writeToPool.Get().(*[]byte)
	defer writeToPool.Put(bufp)
	scratch := *bufp

	var total int64
	for total < mp.length {
		mp.mu.RLock()
		if mp.data == nil {
			mp.mu.RUnlock()
			return total, errPieceEvicted
		}
		n := copy(scratch, mp.data[total:mp.length])
		mp.mu.RUnlock()

		wn, err := w.Write(scratch[:n])
		total += int64(wn)
		if err != nil {
			return total, err
		}
		if wn < n {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

// MarkComplete records that anacrolix verified the piece hash. The piece becomes
// eligible for eviction; we trim the budget now that a complete victim exists.
func (mp *memPiece) MarkComplete() error {
	s := mp.store
	s.mu.Lock()
	defer s.mu.Unlock()
	mp.mu.Lock()
	if mp.data == nil {
		// Cannot be complete without resident data; should not happen because
		// anacrolix writes all chunks before marking complete.
		mp.mu.Unlock()
		return errPieceEvicted
	}
	mp.complete = true
	mp.mu.Unlock()
	// A freshly verified piece is read-ahead until the reader reads it: it is
	// keyed to be evicted after every consumed piece (see aheadKeyBase).
	mp.lastUsed.Store(s.tick.Add(1))
	mp.consumed.Store(false)
	mp.key = aheadKey(mp.index)
	if mp.lruIdx >= 0 {
		heap.Fix(&s.lru, mp.lruIdx)
	} else if mp.pins == 0 { // a pinned piece re-enters the LRU on unpin
		heap.Push(&s.lru, mp)
	}
	mp.evictedReads.Store(0)
	s.evictLocked(0, mp)
	return nil
}

// MarkNotComplete flips the piece to incomplete. The buffer is kept: anacrolix
// calls this on a hash failure or chunk race and then rewrites the same offsets,
// so reusing the buffer avoids a reallocation and the bytes are overwritten
// before they could be read as complete.
func (mp *memPiece) MarkNotComplete() error {
	s := mp.store
	s.mu.Lock()
	defer s.mu.Unlock()
	mp.mu.Lock()
	mp.complete = false
	mp.mu.Unlock()
	if mp.lruIdx >= 0 {
		heap.Remove(&s.lru, mp.lruIdx) // no longer eligible for eviction
	}
	return nil
}

// Completion reports the piece state. Ok is always true: this storage is the
// definitive source of truth. Complete is true only while the verified bytes are
// still resident, so an evicted piece reads as not-complete and is re-fetched.
func (mp *memPiece) Completion() storage.Completion {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return storage.Completion{
		Ok:       true,
		Complete: mp.complete && mp.data != nil,
	}
}
