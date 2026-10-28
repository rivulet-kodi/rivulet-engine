// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

func TestEngineCapEvictsOldestIdleThenRejects(t *testing.T) {
	em, err := New(newJanitorTestCfg(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = em.Close() }()
	m := em.(*manager)
	m.maxEngines = 2

	const a = "08ada5a7a6183aae1e09d831df6748d566095a10"
	const b = "18ada5a7a6183aae1e09d831df6748d566095a10"
	const c = "28ada5a7a6183aae1e09d831df6748d566095a10"
	const d = "38ada5a7a6183aae1e09d831df6748d566095a10"
	for _, ih := range []string{a, b} {
		if _, err := m.EnsureEngine(ih, types.AddOptions{}); err != nil {
			t.Fatalf("EnsureEngine %s: %v", ih, err)
		}
	}
	m.engines[a].mu.Lock()
	m.engines[a].lastAccess = time.Now().Add(-time.Hour)
	m.engines[a].mu.Unlock()

	if _, err := m.EnsureEngine(c, types.AddOptions{}); err != nil {
		t.Fatalf("EnsureEngine c (should evict a): %v", err)
	}
	if _, ok := m.engines[a]; ok {
		t.Fatal("oldest idle engine a not evicted")
	}
	if len(m.engines) != 2 {
		t.Fatalf("engines = %d, want 2", len(m.engines))
	}

	for _, ih := range []string{b, c} {
		m.engines[ih].mu.Lock()
		m.engines[ih].openReaders = 1
		m.engines[ih].mu.Unlock()
	}
	_, err = m.EnsureEngine(d, types.AddOptions{})
	if !errors.Is(err, ErrTooManyEngines) || !errors.Is(err, types.ErrTooManyEngines) {
		t.Fatalf("err = %v, want ErrTooManyEngines", err)
	}
}

// TestEnsureEngineRejectsZeroInfoHash: anacrolix/torrent panics on the
// all-zero info hash while the manager lock is held, which wedged every later
// request. It must be refused up front and leave the manager usable.
func TestEnsureEngineRejectsZeroInfoHash(t *testing.T) {
	em, err := New(newJanitorTestCfg(t))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = em.Close() }()

	for _, ih := range []string{"0000000000000000000000000000000000000000"} {
		if _, err := em.EnsureEngine(ih, types.AddOptions{}); !errors.Is(err, types.ErrInvalidInfoHash) {
			t.Fatalf("EnsureEngine(%s) err = %v, want ErrInvalidInfoHash", ih, err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := em.EnsureEngine("08ada5a7a6183aae1e09d831df6748d566095a10", types.AddOptions{})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("EnsureEngine after zero hash: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("manager wedged after a zero info hash")
	}
}
