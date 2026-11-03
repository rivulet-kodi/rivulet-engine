// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package engine

import (
	"testing"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// TestRefetchPieceNothingToResync: with no live torrent (unknown engine) or no
// such piece (metadata not fetched yet, or an out-of-range index) there is no
// resync to wait for, so the hook returns a nil channel and starts nothing; the
// evicted read then falls back to the plain backoff sleep.
func TestRefetchPieceNothingToResync(t *testing.T) {
	em, err := New(newJanitorTestCfg(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = em.Close() })
	m := em.(*manager)

	ih := metainfo.NewHashFromHex(scanIhOld)
	if ch := m.refetchPiece(ih, 0); ch != nil {
		t.Fatal("unknown engine: want a nil channel")
	}
	if _, err := m.EnsureEngine(scanIhOld, types.AddOptions{}); err != nil {
		t.Fatalf("EnsureEngine: %v", err)
	}
	for _, piece := range []int{-1, 0, 1 << 20} {
		if ch := m.refetchPiece(ih, piece); ch != nil {
			t.Fatalf("piece %d without metadata: want a nil channel", piece)
		}
	}
	n := 0
	m.refetching.Range(func(_, _ any) bool { n++; return true })
	if n != 0 {
		t.Fatalf("%d refetches registered, want none", n)
	}
}
