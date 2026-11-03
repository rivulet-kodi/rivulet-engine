// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package engine

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"

	"github.com/anacrolix/torrent"
)

// sliceTorrentReader is a torrent.Reader over an in-memory file, so a
// pinnedReader can be driven offline with real Read/Seek positions.
type sliceTorrentReader struct{ *bytes.Reader }

func (r sliceTorrentReader) ReadContext(_ context.Context, b []byte) (int, error) {
	return r.Read(b)
}
func (sliceTorrentReader) SetContext(context.Context)             {}
func (sliceTorrentReader) Close() error                           { return nil }
func (sliceTorrentReader) SetReadahead(int64)                     {}
func (sliceTorrentReader) SetReadaheadFunc(torrent.ReadaheadFunc) {}
func (sliceTorrentReader) SetResponsive()                         {}

var _ torrent.Reader = sliceTorrentReader{}

// TestPinnedReaderReportsPiecesToCursor checks the stream-offset to piece
// mapping that tells the in-RAM cache what the player has read: the file's
// torrent offset is added, a read straddling a piece boundary consumes the
// piece it left and pins the one it entered, and a seek moves the position, so
// pieces it jumped over stay read-ahead and the stream's position (and pin) is
// the piece it now reads.
func TestPinnedReaderReportsPiecesToCursor(t *testing.T) {
	const (
		pieceLen  = 100
		numPieces = 10
		base      = 250 // the file starts mid piece 2
	)
	s := newMemStorage(numPieces * pieceLen)
	t.Cleanup(func() { _ = s.Close() })
	ti, info := openMemTorrent(t, s, 3, pieceLen, numPieces)
	for idx := 2; idx <= 6; idx++ {
		p := ti.Piece(info.Piece(idx))
		if _, err := p.WriteAt(fillPattern(idx, pieceLen), 0); err != nil {
			t.Fatal(err)
		}
		if err := p.MarkComplete(); err != nil {
			t.Fatal(err)
		}
	}
	cur := openTestCursor(t, s, 3)
	pr := &pinnedReader{
		tr:       sliceTorrentReader{bytes.NewReader(make([]byte, 500))},
		e:        &engine{},
		ctx:      context.Background(),
		cancel:   func() {},
		cur:      cur,
		base:     base,
		pieceLen: pieceLen,
	}

	read := func(n int) {
		t.Helper()
		if got, err := pr.Read(make([]byte, n)); got != n || err != nil {
			t.Fatalf("Read(%d) = (%d, %v)", n, got, err)
		}
	}
	check := func(readPos, pinned int, consumed ...int) {
		t.Helper()
		s.mu.Lock()
		defer s.mu.Unlock()
		if gotPos := cur.mt.readPos; gotPos != readPos {
			t.Fatalf("readPos = %d, want %d", gotPos, readPos)
		}
		for idx := 2; idx <= 6; idx++ {
			mp := cur.mt.pieces[idx].Load()
			if got, want := mp.consumed.Load(), slices.Contains(consumed, idx); got != want {
				t.Fatalf("piece %d consumed = %v, want %v", idx, got, want)
			}
			want := 0
			if idx == pinned {
				want = 1 // the piece being read
			}
			if mp.pins != want {
				t.Fatalf("piece %d pins = %d, want %d", idx, mp.pins, want)
			}
		}
	}

	read(40) // torrent bytes 250..289
	check(2, 2)
	read(20) // 290..309: straddles into piece 3
	check(3, 3, 2)
	if _, err := pr.Seek(300, io.SeekStart); err != nil { // torrent offset 550
		t.Fatal(err)
	}
	read(1)
	check(5, 5, 2, 3) // piece 4 was jumped over: still read-ahead
}
