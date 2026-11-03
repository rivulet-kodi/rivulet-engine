// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package engine

import (
	"bytes"
	"expvar"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// newPayloadTorrent writes numPieces pieces of random bytes to a file under dir
// and returns the file's bytes and its single-file metainfo.
func newPayloadTorrent(t *testing.T, dir string, pieceLen int64, numPieces int) ([]byte, *metainfo.MetaInfo) {
	t.Helper()
	data := make([]byte, pieceLen*int64(numPieces))
	_, _ = rand.NewChaCha8([32]byte{96}).Read(data)
	src := filepath.Join(dir, "payload.bin")
	if err := os.WriteFile(src, data, 0o600); err != nil {
		t.Fatal(err)
	}
	info := metainfo.Info{PieceLength: pieceLen}
	if err := info.BuildFromFilePath(src); err != nil {
		t.Fatal(err)
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	return data, &metainfo.MetaInfo{InfoBytes: infoBytes}
}

// newMemEngine starts a manager with an in-RAM budget of budget bytes and adds
// mi to it.
func newMemEngine(t *testing.T, budget int64, mi *metainfo.MetaInfo) (*manager, *engine) {
	t.Helper()
	cfg := newJanitorTestCfg(t)
	cfg.MemoryCacheSize = budget
	em, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = em.Close() })
	var miBuf bytes.Buffer
	if err := mi.Write(&miBuf); err != nil {
		t.Fatal(err)
	}
	eng, err := em.EnsureEngine(mi.HashInfoBytes().HexString(), types.AddOptions{MetaInfo: miBuf.Bytes()})
	if err != nil {
		t.Fatal(err)
	}
	e := eng.(*engine)
	<-e.t.GotInfo()
	return em.(*manager), e
}

// TestMemStorageClosingStalledReadReturns: closing a stream whose read is
// waiting for data must end that read with an error. anacrolix retried every
// failed read on capped storage by recursing, the cancelled context included,
// so this overflowed the stack and took the whole engine down with the in-RAM
// cache on (fixed in the anacrolix reader, anacrolix/torrent#1168).
func TestMemStorageClosingStalledReadReturns(t *testing.T) {
	_, mi := newPayloadTorrent(t, t.TempDir(), 64<<10, 4)
	_, e := newMemEngine(t, 1<<20, mi) // no peers: the data never arrives
	r, _, err := e.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 1))
		readErr <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the read block waiting for the piece
	_ = r.Close()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("read of a torrent with no peers returned no error after Close")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("read still blocked 30 s after Close")
	}
}

// TestMemStorageStreamRecoversEvictedPieces streams a torrent from a local
// seeder through the real engine with an in-RAM budget far below the file
// size, then seeks back into pieces the budget evicted. The bytes must match
// the source every time, and recovering an evicted piece must not hash it: the
// refetch hook syncs anacrolix's cached completion from storage
// (Piece.UpdateCompletion), which is enough for the reader to wait for the
// re-download. Hashing the empty piece instead (VerifyData, as before) logged a
// "finished hashing piece ... piece not resident" failure per evicted read
// (rivulet-kodi/plugin.video.rivulet#96).
func TestMemStorageStreamRecoversEvictedPieces(t *testing.T) {
	if testing.Short() {
		t.Skip("starts two torrent clients")
	}
	// The head and tail windows primeBoundary raises to Now are counted first
	// against the request budget, so the budget must exceed them for the
	// stream's own window to be requested at all.
	const (
		pieceLen  = 256 << 10
		boundary  = int((headWindowBytes + tailWindowBytes) / pieceLen)
		numPieces = 2 * boundary
		budget    = int64(boundary+8) * pieceLen
	)
	dir := t.TempDir()
	want, mi := newPayloadTorrent(t, dir, pieceLen, numPieces)

	scfg := torrent.NewDefaultClientConfig()
	scfg.DataDir = dir
	scfg.Seed = true
	scfg.NoDHT = true
	scfg.DisableTrackers = true
	scfg.DisableIPv6 = true
	scfg.DisableUTP = true
	scfg.NoDefaultPortForwarding = true
	scfg.ListenPort = 0
	scfg.ListenHost = func(string) string { return "127.0.0.1" }
	seeder, err := torrent.NewClient(scfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { seeder.Close() })
	st, err := seeder.AddTorrent(mi)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.VerifyDataContext(t.Context()); err != nil {
		t.Fatal(err)
	}

	m, e := newMemEngine(t, budget, mi)
	var refetches atomic.Int64
	m.mem.refetch = func(ih metainfo.Hash, piece int) <-chan struct{} {
		refetches.Add(1)
		return m.refetchPiece(ih, piece)
	}
	e.t.AddClientPeer(seeder)

	failedBefore := expvar.Get("pieceHashedNotCorrect").(*expvar.Int).Value()
	r, n, err := e.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if n != int64(len(want)) {
		t.Fatalf("file length %d, want %d", n, len(want))
	}
	timer := time.AfterFunc(2*time.Minute, func() { _ = r.Close() }) // a stall fails the read
	t.Cleanup(func() { timer.Stop() })
	readRange := func(off int64, length int) {
		t.Helper()
		if _, err := r.Seek(off, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, length)
		if _, err := io.ReadFull(r, got); err != nil {
			t.Fatalf("read %d bytes at %d: %v", length, off, err)
		}
		if !bytes.Equal(got, want[off:off+int64(length)]) {
			t.Fatalf("bytes at %d..%d differ from the source", off, off+int64(length))
		}
	}
	readRange(0, len(want)/2)
	readRange(0, 4*pieceLen) // back into pieces the budget evicted
	readRange(int64(len(want)/2), len(want)/2)

	if refetches.Load() == 0 {
		t.Fatal("no evicted piece was read; the test did not exercise recovery")
	}
	if failed := expvar.Get("pieceHashedNotCorrect").(*expvar.Int).Value() - failedBefore; failed != 0 {
		t.Fatalf("%d pieces failed their hash while recovering %d evicted reads; want none", failed, refetches.Load())
	}
}
