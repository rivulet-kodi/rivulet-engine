// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// syntheticInfo builds a single-file v1 metainfo.Info with numPieces full-length
// pieces. The piece hashes are zero (never verified by these white-box tests,
// which drive the storage backend directly without a real torrent/peers).
func syntheticInfo(pieceLen int64, numPieces int) *metainfo.Info {
	return &metainfo.Info{
		Name:        "memtest",
		PieceLength: pieceLen,
		Length:      pieceLen * int64(numPieces),
		Pieces:      make([]byte, metainfo.HashSize*numPieces),
	}
}

// fillPattern returns a deterministic pieceLen-byte pattern unique per index.
func fillPattern(idx int, pieceLen int64) []byte {
	return bytes.Repeat([]byte{byte(idx*7 + 1)}, int(pieceLen))
}

// readFull reads the whole piece via ReadAt. io.EOF at the boundary is not an
// error for our purposes (the io.ReaderAt contract allows it).
func readFull(p storage.PieceImpl, pieceLen int64) ([]byte, error) {
	buf := make([]byte, pieceLen)
	n, err := p.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

func TestMemStorageWriteCompleteRead(t *testing.T) {
	const (
		pieceLen  = 256
		numPieces = 4
	)
	info := syntheticInfo(pieceLen, numPieces)
	s := newMemStorage(pieceLen * numPieces) // budget large enough to hold all
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(context.Background(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})

	if ti.Capacity == nil {
		t.Fatal("TorrentImpl.Capacity must be set so anacrolix bounds requests")
	}
	if cap, capped := (*ti.Capacity)(); !capped || cap != pieceLen*numPieces {
		t.Fatalf("Capacity() = (%d, %v), want (%d, true)", cap, capped, pieceLen*numPieces)
	}

	p := ti.Piece(info.Piece(0))

	// Before any write the piece is known-incomplete (Ok true so anacrolix trusts
	// it and requests the data), and reading it must error rather than return
	// (0, nil) — which the storage wrappers treat as a fatal protocol violation.
	if c := p.Completion(); !c.Ok || c.Complete {
		t.Fatalf("unwritten piece Completion = %+v, want {Ok:true, Complete:false}", c)
	}
	if _, err := p.ReadAt(make([]byte, pieceLen), 0); err == nil {
		t.Fatal("ReadAt on unwritten piece returned nil error; must signal not-resident")
	}

	want := fillPattern(0, pieceLen)
	// Write in two chunks to exercise offset handling.
	if n, err := p.WriteAt(want[:100], 0); n != 100 || err != nil {
		t.Fatalf("WriteAt(0) = (%d, %v), want (100, nil)", n, err)
	}
	if n, err := p.WriteAt(want[100:], 100); n != pieceLen-100 || err != nil {
		t.Fatalf("WriteAt(100) = (%d, %v), want (%d, nil)", n, err, pieceLen-100)
	}

	// Resident but not yet hash-verified.
	if c := p.Completion(); !c.Ok || c.Complete {
		t.Fatalf("written-but-unmarked piece Completion = %+v, want {Ok:true, Complete:false}", c)
	}
	// Data is readable before MarkComplete (anacrolix reads to verify the hash).
	got, err := readFull(p, pieceLen)
	if err != nil {
		t.Fatalf("ReadAt before MarkComplete: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("ReadAt before MarkComplete returned wrong bytes")
	}

	if err := p.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if c := p.Completion(); !c.Ok || !c.Complete {
		t.Fatalf("post-MarkComplete Completion = %+v, want {Ok:true, Complete:true}", c)
	}
	got, err = readFull(p, pieceLen)
	if err != nil {
		t.Fatalf("ReadAt after MarkComplete: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("ReadAt after MarkComplete returned wrong bytes")
	}
}

// TestMemStorageEvictsPlayedBeforeReadAhead is the regression for
// rivulet-kodi/plugin.video.rivulet#96: under plain LRU a read-ahead piece kept
// its (older) completion stamp while the pieces being played took newer read
// stamps, so the window ahead of the reader was evicted first and a 4K stream
// stalled re-downloading the pieces it was about to read.
func TestMemStorageEvictsPlayedBeforeReadAhead(t *testing.T) {
	const (
		pieceLen  = 256
		numPieces = 8
		capacity  = 3 * pieceLen // room for exactly three resident pieces
	)
	s := newMemStorage(capacity)
	t.Cleanup(func() { _ = s.Close() })
	ti, info := openMemTorrent(t, s, 0, pieceLen, numPieces)

	pieces := make([]storage.PieceImpl, numPieces)
	writeComplete := func(idx int) {
		t.Helper()
		p := ti.Piece(info.Piece(idx))
		if n, err := p.WriteAt(fillPattern(idx, pieceLen), 0); n != pieceLen || err != nil {
			t.Fatalf("piece %d WriteAt = (%d, %v)", idx, n, err)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatalf("piece %d MarkComplete: %v", idx, err)
		}
		pieces[idx] = p
	}
	read := func(idx int) {
		t.Helper()
		got, err := readFull(pieces[idx], pieceLen)
		if err != nil || !bytes.Equal(got, fillPattern(idx, pieceLen)) {
			t.Fatalf("read p%d: err=%v or wrong bytes", idx, err)
		}
	}
	resident := func(want ...int) {
		t.Helper()
		for idx, p := range pieces {
			if p == nil {
				continue
			}
			if got, w := p.Completion().Complete, slices.Contains(want, idx); got != w {
				t.Fatalf("p%d resident = %v, want %v (want resident %v)", idx, got, w, want)
			}
		}
	}

	writeComplete(0)
	writeComplete(1)
	writeComplete(2) // p2 downloaded ahead, completed before p0/p1 are played
	read(0)
	read(1)
	writeComplete(3)
	resident(1, 2, 3) // the oldest played piece goes, not the unread p2

	if _, err := pieces[0].ReadAt(make([]byte, pieceLen), 0); err == nil {
		t.Fatal("evicted p0 ReadAt returned nil error; must signal not-resident so anacrolix re-fetches")
	}

	writeComplete(4)
	resident(2, 3, 4) // the remaining played piece goes next

	// Only read-ahead left: the piece the reader needs last goes first.
	writeComplete(5)
	resident(2, 3, 5)

	writeComplete(6)
	resident(2, 3, 6)

	// A forward seek to p6: once played it is evicted first again, and the
	// unread p2 and p3 are now behind the reader, so they lead the read-ahead
	// class even though p7 is farther ahead.
	read(6)
	writeComplete(7)
	resident(2, 3, 7)
	writeComplete(0)
	resident(0, 3, 7)
}

// TestMemStorageConcurrentNeverWrongBytes hammers the backend from many
// goroutines under a tight budget (heavy eviction) and asserts the core
// correctness invariant: a successful ReadAt always returns the exact bytes
// written for that piece — never stale or wrong data. Run with -race, it also
// proves the backend is free of data races. Deterministic in outcome.
func TestMemStorageConcurrentNeverWrongBytes(t *testing.T) {
	const (
		pieceLen  = 128
		numPieces = 64
		capacity  = 8 * pieceLen // forces continual eviction
	)
	info := syntheticInfo(pieceLen, numPieces)
	s := newMemStorage(capacity)
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(context.Background(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})

	var wg sync.WaitGroup
	for i := 0; i < numPieces; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := ti.Piece(info.Piece(idx))
			want := fillPattern(idx, pieceLen)
			if n, err := p.WriteAt(want, 0); n != pieceLen || err != nil {
				t.Errorf("piece %d WriteAt = (%d, %v)", idx, n, err)
				return
			}
			if err := p.MarkComplete(); err != nil {
				t.Errorf("piece %d MarkComplete: %v", idx, err)
				return
			}
			// Concurrently read back and query completion. If the read succeeds
			// the bytes MUST match; if the piece was evicted the read errors —
			// both are correct, returning the wrong bytes is not.
			for k := 0; k < 8; k++ {
				buf := make([]byte, pieceLen)
				n, rerr := p.ReadAt(buf, 0)
				if rerr == nil || errors.Is(rerr, io.EOF) {
					if !bytes.Equal(buf[:n], want[:n]) || n != pieceLen {
						t.Errorf("piece %d ReadAt returned wrong bytes", idx)
						return
					}
				}
				_ = p.Completion()
			}
		}(i)
	}
	wg.Wait()

	// Once quiescent, every piece is complete and the budget is enforced.
	ms := s
	ms.mu.Lock()
	used := ms.used
	ms.mu.Unlock()
	if used > capacity {
		t.Fatalf("resident bytes = %d exceed budget %d after settle", used, int64(capacity))
	}
}

// TestMemStorageWriteAtBoundsCheck covers the two error returns in WriteAt:
// a negative offset and an offset at-or-beyond the piece length.
func TestMemStorageWriteAtBoundsCheck(t *testing.T) {
	const pieceLen = 64
	info := syntheticInfo(pieceLen, 1)
	s := newMemStorage(pieceLen)
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(t.Context(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})
	p := ti.Piece(info.Piece(0))

	cases := []struct {
		name string
		off  int64
	}{
		{"negative", -1},
		{"at length", pieceLen},
		{"beyond length", pieceLen + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, err := p.WriteAt([]byte("x"), tc.off)
			if err == nil {
				t.Errorf("WriteAt(off=%d): expected error, got nil", tc.off)
			}
			if n != 0 {
				t.Errorf("WriteAt(off=%d): expected 0 bytes written, got %d", tc.off, n)
			}
		})
	}
}

// TestMemStorageReadAtBoundsCheck covers the negative-offset and out-of-range
// paths in ReadAt, plus the partial-fill (short read returning io.EOF).
func TestMemStorageReadAtBoundsCheck(t *testing.T) {
	const pieceLen = 64
	info := syntheticInfo(pieceLen, 1)
	s := newMemStorage(pieceLen)
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(t.Context(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})
	p := ti.Piece(info.Piece(0))

	// Write data to make the piece resident.
	data := fillPattern(0, pieceLen)
	if _, err := p.WriteAt(data, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}

	t.Run("negative offset", func(t *testing.T) {
		n, err := p.ReadAt(make([]byte, 1), -1)
		if err == nil {
			t.Error("ReadAt(-1): expected error, got nil")
		}
		if n != 0 {
			t.Errorf("ReadAt(-1): expected 0 bytes, got %d", n)
		}
	})

	t.Run("at length", func(t *testing.T) {
		n, err := p.ReadAt(make([]byte, 1), pieceLen)
		if !errors.Is(err, io.EOF) {
			t.Errorf("ReadAt(at length): expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("ReadAt(at length): expected 0 bytes, got %d", n)
		}
	})

	t.Run("beyond length", func(t *testing.T) {
		n, err := p.ReadAt(make([]byte, 1), pieceLen+10)
		if !errors.Is(err, io.EOF) {
			t.Errorf("ReadAt(beyond length): expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("ReadAt(beyond length): want 0 bytes, got %d", n)
		}
	})

	t.Run("partial fill at end", func(t *testing.T) {
		const start = 10
		buf := make([]byte, pieceLen) // bigger than remaining (pieceLen-start)
		n, err := p.ReadAt(buf, start)
		if !errors.Is(err, io.EOF) {
			t.Errorf("ReadAt partial: expected io.EOF, got %v", err)
		}
		want := pieceLen - start
		if n != want {
			t.Errorf("ReadAt partial: got %d bytes, want %d", n, want)
		}
		if !bytes.Equal(buf[:n], data[start:]) {
			t.Error("ReadAt partial: wrong bytes returned")
		}
	})
}

// TestMemStorageMarkNotComplete verifies that MarkNotComplete flips the piece
// back to incomplete while keeping the resident buffer intact (anacrolix can
// then overwrite and re-verify without a reallocation).
func TestMemStorageMarkNotComplete(t *testing.T) {
	const pieceLen = 64
	info := syntheticInfo(pieceLen, 1)
	s := newMemStorage(pieceLen)
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(t.Context(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})
	p := ti.Piece(info.Piece(0))
	want := fillPattern(0, pieceLen)

	if _, err := p.WriteAt(want, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := p.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if c := p.Completion(); !c.Complete {
		t.Fatal("post-MarkComplete: Complete must be true")
	}

	// MarkNotComplete flips Complete back to false.
	if err := p.MarkNotComplete(); err != nil {
		t.Fatalf("MarkNotComplete: %v", err)
	}
	c := p.Completion()
	if c.Complete {
		t.Error("post-MarkNotComplete: Complete must be false")
	}
	if !c.Ok {
		t.Error("post-MarkNotComplete: Ok must remain true")
	}
	// Data must still be resident so anacrolix can overwrite and re-verify.
	got, err := readFull(p, pieceLen)
	if err != nil {
		t.Fatalf("ReadAt after MarkNotComplete: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Error("ReadAt after MarkNotComplete: buffer must retain original bytes")
	}
}

// TestMemStorageMarkCompleteBeforeWrite covers the nil-data guard in MarkComplete:
// calling it on an unwritten piece must return errPieceEvicted, not panic.
func TestMemStorageMarkCompleteBeforeWrite(t *testing.T) {
	const pieceLen = 64
	info := syntheticInfo(pieceLen, 1)
	s := newMemStorage(pieceLen)
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(t.Context(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})
	p := ti.Piece(info.Piece(0))

	if err := p.MarkComplete(); err == nil {
		t.Error("MarkComplete on unwritten piece: expected error (errPieceEvicted), got nil")
	}
}

// TestMemStorageTorrentCloseFreesMemory verifies that closing a torrent via the
// TorrentImpl.Close callback drops all its resident pieces and decrements used
// bytes back to zero.
func TestMemStorageTorrentCloseFreesMemory(t *testing.T) {
	const (
		pieceLen  = 64
		numPieces = 2
	)
	info := syntheticInfo(pieceLen, numPieces)
	s := newMemStorage(pieceLen * numPieces)
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(t.Context(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}

	// Write and complete both pieces so they are resident.
	for i := 0; i < numPieces; i++ {
		p := ti.Piece(info.Piece(i))
		if _, err := p.WriteAt(fillPattern(i, pieceLen), 0); err != nil {
			t.Fatalf("piece %d WriteAt: %v", i, err)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatalf("piece %d MarkComplete: %v", i, err)
		}
	}

	ms := s
	ms.mu.Lock()
	before := ms.used
	ms.mu.Unlock()
	if before != pieceLen*numPieces {
		t.Fatalf("before Close: used = %d, want %d", before, int64(pieceLen*numPieces))
	}

	if ti.Close == nil {
		t.Fatal("TorrentImpl.Close must be set")
	}
	if err := ti.Close(); err != nil {
		t.Fatalf("torrent Close: %v", err)
	}

	ms.mu.Lock()
	after := ms.used
	ms.mu.Unlock()
	if after != 0 {
		t.Errorf("after torrent Close: used = %d, want 0", after)
	}
}

// TestMemStorageEvictLockedNoVictim covers the "no evictable victim" early
// return in evictLocked. When all resident pieces are incomplete (in-flight),
// eviction cannot free space and the implementation tolerates a transient
// overage rather than dropping live data. The test writes two pieces into a
// budget sized for one, without completing either, and asserts both remain
// resident (used = 2×pieceLen > capacity).
func TestMemStorageEvictLockedNoVictim(t *testing.T) {
	const pieceLen = 64
	info := syntheticInfo(pieceLen, 2)
	s := newMemStorage(pieceLen) // only room for one piece
	t.Cleanup(func() { _ = s.Close() })

	ti, err := s.OpenTorrent(t.Context(), info, metainfo.Hash{})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})

	p0 := ti.Piece(info.Piece(0))
	p1 := ti.Piece(info.Piece(1))

	// Write piece 0 without completing it — it is in-flight (incomplete).
	if _, err := p0.WriteAt(fillPattern(0, pieceLen), 0); err != nil {
		t.Fatalf("piece 0 WriteAt: %v", err)
	}

	// Write piece 1: evictLocked(pieceLen, p1) searches the LRU for a complete
	// victim, finds none (p0 is incomplete), and returns without evicting.
	// The implementation tolerates this in-flight overage.
	if _, err := p1.WriteAt(fillPattern(1, pieceLen), 0); err != nil {
		t.Fatalf("piece 1 WriteAt with no evictable victim: %v", err)
	}

	// Both pieces are resident: used must equal 2×pieceLen, exceeding capacity.
	ms := s
	ms.mu.Lock()
	used := ms.used
	ms.mu.Unlock()
	if used != 2*pieceLen {
		t.Errorf("used = %d; want %d (in-flight overage tolerated)", used, int64(2*pieceLen))
	}

	// Verify both pieces still return their correct bytes.
	for idx, p := range []storage.PieceImpl{p0, p1} {
		got, err := readFull(p, pieceLen)
		if err != nil {
			t.Fatalf("piece %d readFull: %v", idx, err)
		}
		if !bytes.Equal(got, fillPattern(idx, pieceLen)) {
			t.Errorf("piece %d: wrong bytes after no-victim eviction", idx)
		}
	}
}

// refetchCall records one invocation of the memStorage refetch hook.
type refetchCall struct {
	ih    metainfo.Hash
	piece int
}

// TestMemStorageEvictedReadTriggersRefetch verifies the stack-overflow guard:
// reading a piece whose bytes were evicted invokes the refetch hook with the
// torrent's infohash and the piece index (so the engine can force a real
// re-download), and still returns errPieceEvicted so anacrolix re-fetches rather
// than serving stale bytes.
func TestMemStorageEvictedReadTriggersRefetch(t *testing.T) {
	const (
		pieceLen  = 256
		numPieces = 3
	)
	info := syntheticInfo(pieceLen, numPieces)
	s := newMemStorage(pieceLen) // budget = exactly one piece → forces eviction
	s.refetchBackoff = 0         // no artificial delay in tests
	t.Cleanup(func() { _ = s.Close() })

	var (
		mu    sync.Mutex
		calls []refetchCall
	)
	s.refetch = func(ih metainfo.Hash, piece int) <-chan struct{} {
		mu.Lock()
		calls = append(calls, refetchCall{ih: ih, piece: piece})
		mu.Unlock()
		return nil
	}

	ih := metainfo.NewHashFromHex("0123456789abcdef0123456789abcdef01234567")
	ti, err := s.OpenTorrent(context.Background(), info, ih)
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})

	// Complete piece 0, then complete piece 1; the one-piece budget evicts piece 0.
	p0 := ti.Piece(info.Piece(0))
	if _, err := p0.WriteAt(fillPattern(0, pieceLen), 0); err != nil {
		t.Fatalf("WriteAt p0: %v", err)
	}
	if err := p0.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete p0: %v", err)
	}
	p1 := ti.Piece(info.Piece(1))
	if _, err := p1.WriteAt(fillPattern(1, pieceLen), 0); err != nil {
		t.Fatalf("WriteAt p1: %v", err)
	}
	if err := p1.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete p1: %v", err)
	}

	// Piece 0's bytes are gone: it now reads as not-complete and ReadAt errors.
	if c := p0.Completion(); !c.Ok || c.Complete {
		t.Fatalf("evicted p0 Completion = %+v, want {Ok:true, Complete:false}", c)
	}
	if _, err := p0.ReadAt(make([]byte, pieceLen), 0); !errors.Is(err, errPieceEvicted) {
		t.Fatalf("ReadAt(evicted) err = %v, want errPieceEvicted", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("refetch invoked %d times, want 1: %+v", len(calls), calls)
	}
	if calls[0].ih != ih || calls[0].piece != 0 {
		t.Fatalf("refetch(%x, %d), want (%x, 0)", calls[0].ih, calls[0].piece, ih)
	}
}

// TestMemStoragePastEndReadNoRefetch verifies that a RESIDENT, complete piece
// read at/after its end returns the standard (0, io.EOF) io.ReaderAt result
// WITHOUT invoking the refetch+backoff guard. io.EOF is terminal, so anacrolix
// does not spin on it; firing refetch (and the 100 ms backoff) on this branch
// would add a spurious VerifyData call and latency to every legitimate
// end-of-piece read. The refetch guard is reserved for genuinely evicted
// pieces (covered by TestMemStorageEvictedReadTriggersRefetch).
func TestMemStoragePastEndReadNoRefetch(t *testing.T) {
	const pieceLen = 256
	info := syntheticInfo(pieceLen, 2)
	s := newMemStorage(pieceLen * 2)
	s.refetchBackoff = 0
	t.Cleanup(func() { _ = s.Close() })

	var (
		mu    sync.Mutex
		calls []refetchCall
	)
	s.refetch = func(ih metainfo.Hash, piece int) <-chan struct{} {
		mu.Lock()
		calls = append(calls, refetchCall{ih: ih, piece: piece})
		mu.Unlock()
		return nil
	}

	ih := metainfo.NewHashFromHex("89abcdef0123456789abcdef0123456789abcdef")
	ti, err := s.OpenTorrent(context.Background(), info, ih)
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})

	p := ti.Piece(info.Piece(1))
	if _, err := p.WriteAt(fillPattern(1, pieceLen), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := p.MarkComplete(); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}

	// In-bounds read still succeeds without any refetch.
	if n, err := p.ReadAt(make([]byte, 16), 0); n != 16 || err != nil {
		t.Fatalf("in-bounds ReadAt = (%d, %v), want (16, nil)", n, err)
	}
	// Read at the piece end: standard zero-byte io.EOF, and NO refetch fires.
	if n, err := p.ReadAt(make([]byte, 16), pieceLen); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("past-end ReadAt = (%d, %v), want (0, io.EOF)", n, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 0 {
		t.Fatalf("resident past-end read must not refetch; got calls = %+v", calls)
	}
}

// openMemTorrent opens a torrent with numPieces pieces on s and returns its
// storage and info; ti.Close is registered for cleanup.
func openMemTorrent(t *testing.T, s *memStorage, id byte, pieceLen int64, numPieces int) (storage.TorrentImpl, *metainfo.Info) {
	t.Helper()
	info := syntheticInfo(pieceLen, numPieces)
	ti, err := s.OpenTorrent(context.Background(), info, metainfo.Hash{id})
	if err != nil {
		t.Fatalf("OpenTorrent: %v", err)
	}
	t.Cleanup(func() {
		if ti.Close != nil {
			_ = ti.Close()
		}
	})
	return ti, info
}

// TestMemStorageEvictionMatchesModel drives a random mix of completions and
// reads and checks, after every step, that the resident set equals a reference
// model of the eviction policy: consumed (read) pieces go first, least recently
// read first; then read-ahead pieces below the reader position, lowest index
// first; then the read-ahead piece farthest ahead. The heap, its lazily
// refreshed keys and the ahead scan must agree with it however reads and
// completions interleave.
func TestMemStorageEvictionMatchesModel(t *testing.T) {
	const (
		pieceLen  = 64
		numPieces = 200
		resident  = 16
		steps     = 5000
	)
	s := newMemStorage(resident * pieceLen)
	t.Cleanup(func() { _ = s.Close() })
	ti, info := openMemTorrent(t, s, 1, pieceLen, numPieces)
	pieces := make([]storage.PieceImpl, numPieces)
	for i := range pieces {
		pieces[i] = ti.Piece(info.Piece(i))
	}

	rng := rand.New(rand.NewPCG(7, 7))
	var consumed []int      // read pieces, least recently read first
	ahead := map[int]bool{} // complete, not yet read
	readPos := -1           // index of the last first-read piece
	isResident := func(idx int) bool { return ahead[idx] || slices.Contains(consumed, idx) }
	evict := func() {
		if len(consumed) > 0 {
			consumed = consumed[1:]
			return
		}
		victim, skipped := -1, false
		for idx := range ahead {
			switch sk := idx < readPos; {
			case sk && (!skipped || idx < victim):
				victim, skipped = idx, true
			case !sk && !skipped && idx > victim:
				victim = idx
			}
		}
		delete(ahead, victim)
	}

	buf := make([]byte, 8)
	for step := range steps {
		n := len(consumed) + len(ahead)
		if n > 0 && rng.IntN(3) > 0 {
			idx := rng.IntN(numPieces)
			for !isResident(idx) {
				idx = rng.IntN(numPieces)
			}
			if n, err := pieces[idx].ReadAt(buf, 0); n != len(buf) || err != nil {
				t.Fatalf("step %d: ReadAt(%d) = (%d, %v)", step, idx, n, err)
			}
			if ahead[idx] {
				delete(ahead, idx)
				readPos = idx
			} else {
				consumed = slices.DeleteFunc(consumed, func(v int) bool { return v == idx })
			}
			consumed = append(consumed, idx)
		} else {
			idx := rng.IntN(numPieces)
			if isResident(idx) {
				continue
			}
			if _, err := pieces[idx].WriteAt(fillPattern(idx, pieceLen), 0); err != nil {
				t.Fatalf("step %d: WriteAt(%d): %v", step, idx, err)
			}
			if err := pieces[idx].MarkComplete(); err != nil {
				t.Fatalf("step %d: MarkComplete(%d): %v", step, idx, err)
			}
			if n == resident {
				evict()
			}
			ahead[idx] = true
		}
		for idx, p := range pieces {
			if want, got := isResident(idx), p.Completion().Complete; want != got {
				t.Fatalf("step %d: piece %d resident = %v, model says %v", step, idx, got, want)
			}
		}
	}
}

// TestMemStorageConcurrentFirstWriteReservesOnce races many chunk writers into
// one non-resident piece: the budget must be charged exactly once and every
// chunk must land, so a duplicated reservation never evicts a neighbour.
func TestMemStorageConcurrentFirstWriteReservesOnce(t *testing.T) {
	const (
		pieceLen = 1024
		chunkLen = 64
		writers  = pieceLen / chunkLen
	)
	for round := range 50 {
		s := newMemStorage(2 * pieceLen)
		ti, info := openMemTorrent(t, s, byte(round), pieceLen, 2)
		neighbour := ti.Piece(info.Piece(1))
		if _, err := neighbour.WriteAt(fillPattern(1, pieceLen), 0); err != nil {
			t.Fatal(err)
		}
		if err := neighbour.MarkComplete(); err != nil {
			t.Fatal(err)
		}

		p := ti.Piece(info.Piece(0))
		want := fillPattern(0, pieceLen)
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				off := int64(w * chunkLen)
				if n, err := p.WriteAt(want[off:off+chunkLen], off); n != chunkLen || err != nil {
					t.Errorf("WriteAt(%d) = (%d, %v)", off, n, err)
				}
			}()
		}
		wg.Wait()

		s.mu.Lock()
		used := s.used
		s.mu.Unlock()
		if used != 2*pieceLen {
			t.Fatalf("round %d: used = %d, want %d (piece charged once, neighbour kept)", round, used, int64(2*pieceLen))
		}
		if c := neighbour.Completion(); !c.Complete {
			t.Fatalf("round %d: neighbour evicted by a duplicated reservation", round)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatalf("round %d: MarkComplete: %v", round, err)
		}
		if got, err := readFull(p, pieceLen); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("round %d: read back = (%d bytes, %v), want the written pattern", round, len(got), err)
		}
		_ = s.Close()
	}
}

// TestMemStorageStressAccounting hammers shared-budget pieces across torrents
// with every storage operation, including torrent close/reopen, then checks the
// bookkeeping invariants: successful reads return exact bytes, resident bytes
// equal the accounted total, and the LRU holds exactly the complete pieces.
// Run under -race it also exercises the lock ordering (a deadlock would hang).
func TestMemStorageStressAccounting(t *testing.T) {
	const (
		pieceLen   = 64
		numPieces  = 32
		stable     = 2 // torrents the workers share
		workers    = 6
		iterations = 3000
	)
	s := newMemStorage(12 * pieceLen)
	t.Cleanup(func() { _ = s.Close() })

	var all []*memPiece
	pieceAt := make([][]storage.PieceImpl, stable)
	for tor := range stable {
		ti, info := openMemTorrent(t, s, byte(tor), pieceLen, numPieces)
		for i := range numPieces {
			p := ti.Piece(info.Piece(i))
			pieceAt[tor] = append(pieceAt[tor], p)
			all = append(all, p.(*memPiece))
		}
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			buf := make([]byte, pieceLen)
			for range iterations {
				tor, idx := rng.IntN(stable), rng.IntN(numPieces)
				p := pieceAt[tor][idx]
				want := fillPattern(tor*numPieces+idx, pieceLen)
				switch rng.IntN(6) {
				case 0, 1:
					if _, err := p.WriteAt(want, 0); err != nil {
						t.Errorf("WriteAt: %v", err)
						return
					}
					if err := p.MarkComplete(); err != nil && !errors.Is(err, errPieceEvicted) {
						t.Errorf("MarkComplete: %v", err)
						return
					}
				case 2, 3:
					n, err := p.ReadAt(buf, 0)
					if err == nil && !bytes.Equal(buf[:n], want[:n]) {
						t.Error("ReadAt returned wrong bytes")
						return
					}
				case 4:
					_ = p.MarkNotComplete()
				default:
					var sink bytes.Buffer
					if n, err := p.(io.WriterTo).WriteTo(&sink); err == nil && (n != pieceLen || !bytes.Equal(sink.Bytes(), want)) {
						t.Errorf("WriteTo = (%d, nil) with wrong bytes", n)
						return
					}
				}
			}
		}()
	}
	// A torrent that is repeatedly filled and closed while the workers run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := range 200 {
			info := syntheticInfo(pieceLen, 8)
			ti, err := s.OpenTorrent(context.Background(), info, metainfo.Hash{byte(100 + round%100)})
			if err != nil {
				t.Errorf("OpenTorrent: %v", err)
				return
			}
			for i := range 8 {
				p := ti.Piece(info.Piece(i))
				_, _ = p.WriteAt(fillPattern(i, pieceLen), 0)
				_ = p.MarkComplete()
			}
			_ = ti.Close()
		}
	}()
	wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	var resident int64
	for _, mp := range all {
		mp.mu.RLock()
		hasData, complete := mp.data != nil, mp.complete
		mp.mu.RUnlock()
		if hasData {
			resident += mp.length
		}
		if inLRU := mp.lruIdx >= 0; inLRU != (complete && hasData) {
			t.Errorf("piece %d: inLRU = %v but complete = %v, resident = %v", mp.index, inLRU, complete, hasData)
		}
	}
	if resident != s.used {
		t.Errorf("resident bytes = %d, accounted used = %d", resident, s.used)
	}
	for i, mp := range s.lru {
		if mp.lruIdx != i {
			t.Errorf("lru[%d].lruIdx = %d", i, mp.lruIdx)
		}
	}
}

// TestMemStorageWriteTo covers the io.WriterTo fast path the hasher uses: a
// resident piece streams its exact bytes, a non-resident one fails fast with
// errPieceEvicted without invoking the refetch hook or waiting (the hook's own
// VerifyData is what is hashing, so waiting would block on itself), and writer
// errors surface.
func TestMemStorageWriteTo(t *testing.T) {
	const pieceLen = 3*writeToChunk + 123 // several scratch chunks plus a remainder
	s := newMemStorage(2 * pieceLen)
	s.refetchBackoff = 10 * time.Second // a wait on the hook would blow the deadline below
	var calls int
	s.refetch = func(metainfo.Hash, int) <-chan struct{} { calls++; return nil }
	t.Cleanup(func() { _ = s.Close() })
	ti, info := openMemTorrent(t, s, 1, pieceLen, 3)

	p0 := ti.Piece(info.Piece(0))
	want := make([]byte, pieceLen)
	_, _ = rand.NewChaCha8([32]byte{3}).Read(want)
	if _, err := p0.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := p0.MarkComplete(); err != nil {
		t.Fatal(err)
	}

	var got bytes.Buffer
	if n, err := p0.(io.WriterTo).WriteTo(&got); n != pieceLen || err != nil {
		t.Fatalf("WriteTo(resident) = (%d, %v), want (%d, nil)", n, err, pieceLen)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatal("WriteTo(resident) wrote wrong bytes")
	}

	// A failing writer's error is returned as-is, with the partial count.
	boom := errors.New("boom")
	if n, err := p0.(io.WriterTo).WriteTo(failingWriter{after: 10, err: boom}); !errors.Is(err, boom) || n != 10 {
		t.Fatalf("WriteTo(failing writer) = (%d, %v), want (10, boom)", n, err)
	}
	if _, err := p0.(io.WriterTo).WriteTo(shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("WriteTo(short writer) err = %v, want io.ErrShortWrite", err)
	}

	// Evict p0 by playing it, then completing p1 and p2 in a two-piece budget.
	if _, err := readFull(p0, pieceLen); err != nil {
		t.Fatal(err)
	}
	for _, idx := range []int{1, 2} {
		p := ti.Piece(info.Piece(idx))
		if _, err := p.WriteAt(make([]byte, pieceLen), 0); err != nil {
			t.Fatal(err)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatal(err)
		}
	}
	if c := p0.Completion(); c.Complete {
		t.Fatal("p0 should have been evicted")
	}
	start := time.Now()
	n, err := p0.(io.WriterTo).WriteTo(io.Discard)
	if n != 0 || !errors.Is(err, errPieceEvicted) {
		t.Fatalf("WriteTo(evicted) = (%d, %v), want (0, errPieceEvicted)", n, err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("WriteTo(evicted) took %v; it must not wait on the refetch hook", d)
	}
	if calls != 0 {
		t.Fatalf("WriteTo(evicted) called the refetch hook %d times, want 0", calls)
	}
}

// failingWriter accepts after bytes and then fails with err.
type failingWriter struct {
	after int
	err   error
}

func (w failingWriter) Write(b []byte) (int, error) {
	if len(b) > w.after {
		return w.after, w.err
	}
	return len(b), nil
}

// shortWriter reports one byte short without an error.
type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }

// evictedMemPiece returns an evicted piece of a fresh single-slot store.
func evictedMemPiece(t *testing.T, s *memStorage) storage.PieceImpl {
	t.Helper()
	const pieceLen = 64
	ti, info := openMemTorrent(t, s, 9, pieceLen, 2)
	for idx := range 2 { // the second completion evicts the first
		p := ti.Piece(info.Piece(idx))
		if _, err := p.WriteAt(fillPattern(idx, pieceLen), 0); err != nil {
			t.Fatal(err)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatal(err)
		}
	}
	p0 := ti.Piece(info.Piece(0))
	if p0.Completion().Complete {
		t.Fatal("piece 0 should have been evicted")
	}
	return p0
}

// TestMemStorageEvictedReadWaitsForRefetchSignal verifies the event-driven
// wait: an evicted read blocks until the hook's channel closes — however long
// the safety cap — and returns promptly once it does, instead of sleeping a
// fixed backoff.
func TestMemStorageEvictedReadWaitsForRefetchSignal(t *testing.T) {
	s := newMemStorage(64)
	s.refetchBackoff = time.Minute // the cap must not be what ends the wait
	t.Cleanup(func() { _ = s.Close() })
	done := make(chan struct{})
	called := make(chan struct{})
	s.refetch = func(metainfo.Hash, int) <-chan struct{} {
		close(called)
		return done
	}
	p := evictedMemPiece(t, s)

	ret := make(chan error, 1)
	go func() {
		_, err := p.ReadAt(make([]byte, 8), 0)
		ret <- err
	}()
	<-called
	select {
	case err := <-ret:
		t.Fatalf("evicted ReadAt returned (%v) before the refetch signal", err)
	default:
	}
	close(done)
	select {
	case err := <-ret:
		if !errors.Is(err, errPieceEvicted) {
			t.Fatalf("evicted ReadAt err = %v, want errPieceEvicted", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("evicted ReadAt did not return after the refetch signal")
	}
}

// TestMemStorageEvictedReadCapAndThrottle verifies the two bounds that keep
// anacrolix's retry recursion finite when the re-pend never lands: a signal
// that never fires is cut off at refetchBackoff, and repeated evicted reads of
// one piece sleep an escalating floor even when the signal is already closed.
func TestMemStorageEvictedReadCapAndThrottle(t *testing.T) {
	t.Run("cap", func(t *testing.T) {
		s := newMemStorage(64)
		s.refetchBackoff = 30 * time.Millisecond
		t.Cleanup(func() { _ = s.Close() })
		s.refetch = func(metainfo.Hash, int) <-chan struct{} { return make(chan struct{}) } // never closes
		p := evictedMemPiece(t, s)
		start := time.Now()
		if _, err := p.ReadAt(make([]byte, 8), 0); !errors.Is(err, errPieceEvicted) {
			t.Fatalf("err = %v, want errPieceEvicted", err)
		}
		if d := time.Since(start); d < s.refetchBackoff {
			t.Fatalf("returned after %v, before the %v cap", d, s.refetchBackoff)
		}
	})

	t.Run("throttle", func(t *testing.T) {
		s := newMemStorage(64)
		s.refetchBackoff = time.Second
		t.Cleanup(func() { _ = s.Close() })
		closed := make(chan struct{})
		close(closed)
		s.refetch = func(metainfo.Hash, int) <-chan struct{} { return closed }
		p := evictedMemPiece(t, s)
		for read, floor := range []time.Duration{0, refetchSettle, 2 * refetchSettle, 4 * refetchSettle, 8 * refetchSettle} {
			start := time.Now()
			if _, err := p.ReadAt(make([]byte, 8), 0); !errors.Is(err, errPieceEvicted) {
				t.Fatalf("read %d: err = %v, want errPieceEvicted", read, err)
			}
			if d := time.Since(start); d < floor {
				t.Fatalf("read %d returned after %v, want at least the %v throttle floor", read, d, floor)
			}
		}

		// Becoming resident again clears the escalation.
		if _, err := p.WriteAt(fillPattern(0, 64), 0); err != nil {
			t.Fatal(err)
		}
		if got := p.(*memPiece).evictedReads.Load(); got != 0 {
			t.Fatalf("evictedReads = %d after the piece became resident, want 0", got)
		}
	})

	t.Run("idle gap resets the escalation", func(t *testing.T) {
		s := newMemStorage(64)
		s.refetchBackoff = 100 * time.Millisecond // far above the few ms the spin's own floors take
		t.Cleanup(func() { _ = s.Close() })
		closed := make(chan struct{})
		close(closed)
		s.refetch = func(metainfo.Hash, int) <-chan struct{} { return closed }
		p := evictedMemPiece(t, s)
		mp := p.(*memPiece)
		for range 3 { // a spin escalates
			_, _ = p.ReadAt(make([]byte, 8), 0)
		}
		if got := mp.evictedReads.Load(); got != 3 {
			t.Fatalf("evictedReads = %d after a rapid spin, want 3", got)
		}
		time.Sleep(3 * s.refetchBackoff) // an unrelated read much later is not part of the spin
		_, _ = p.ReadAt(make([]byte, 8), 0)
		if got := mp.evictedReads.Load(); got != 1 {
			t.Fatalf("evictedReads = %d after an idle gap, want 1 (fresh start)", got)
		}
	})

	t.Run("no hook result sleeps the backoff", func(t *testing.T) {
		s := newMemStorage(64)
		s.refetchBackoff = 20 * time.Millisecond
		t.Cleanup(func() { _ = s.Close() })
		s.refetch = func(metainfo.Hash, int) <-chan struct{} { return nil }
		p := evictedMemPiece(t, s)
		start := time.Now()
		if _, err := p.ReadAt(make([]byte, 8), 0); !errors.Is(err, errPieceEvicted) {
			t.Fatalf("err = %v, want errPieceEvicted", err)
		}
		if d := time.Since(start); d < s.refetchBackoff {
			t.Fatalf("returned after %v, want at least the %v backoff", d, s.refetchBackoff)
		}
	})
}
