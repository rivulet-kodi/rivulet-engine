// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// segCacheSegments is how many segments' worth of bytes (at the
	// session's max bitrate) the default per-session cache cap allows:
	// 300 × segDur ≈ 20 minutes of media, enough for a long backwards seek
	// to stay cached while bounding disk use for a session that plays for
	// hours. At the default 8 Mbit/s that is ≈ 1.2 GB.
	segCacheSegments = 300

	// segCacheMinBytes floors the derived cap so a very low bitrate
	// session still keeps a useful working set.
	segCacheMinBytes = 64 << 20

	// segServeGrace protects recently used segments from eviction. HLSFile
	// returns a path and the HTTP layer opens it afterwards, so a segment
	// touched within this window may be about to be (or still be) served.
	segServeGrace = 30 * time.Second
)

// segEntry is one cached segment file tracked by segIndex.
type segEntry struct {
	size int64
	last time.Time // last transcode, cache hit or restore (file mtime)
}

// segIndex tracks the bytes of the cached seg<n>.ts / a<k>seg<n>.ts files in
// one session directory so the cache can be capped. The zero value is ready
// to use; the directory is scanned lazily, once, so segments restored from a
// persisted session are accounted for too.
type segIndex struct {
	mu     sync.Mutex
	loaded bool
	files  map[string]segEntry
	total  int64
}

// isCachedSegName reports whether name is a finished cached segment file (as
// opposed to a playlist, subtitle or any *.tmp* work/provisional file).
func isCachedSegName(name string) bool {
	if !strings.HasSuffix(name, ".ts") || strings.Contains(name, ".tmp") {
		return false
	}
	return strings.HasPrefix(name, "seg") || (strings.HasPrefix(name, "a") && strings.Contains(name, "seg"))
}

// loadLocked scans dir once. ix.mu must be held.
func (ix *segIndex) loadLocked(dir string) {
	if ix.loaded {
		return
	}
	ix.loaded = true
	ix.files = make(map[string]segEntry)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if e.IsDir() || !isCachedSegName(e.Name()) {
			continue
		}
		fi, err := e.Info()
		if err != nil || fi.Size() <= 0 {
			continue
		}
		ix.files[e.Name()] = segEntry{size: fi.Size(), last: fi.ModTime()}
		ix.total += fi.Size()
	}
}

// cachedSegments returns the size of every cached segment file in the
// session directory, keyed by filename.
func (s *hlsSession) cachedSegments() map[string]int64 {
	ix := &s.segIdx
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.loadLocked(s.dir)
	out := make(map[string]int64, len(ix.files))
	for name, e := range ix.files {
		out[name] = e.size
	}
	return out
}

// noteSegment records that cached segment filename (size bytes) was just
// produced or served, refreshing its recency.
func (s *hlsSession) noteSegment(filename string, size int64, now time.Time) {
	ix := &s.segIdx
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.loadLocked(s.dir)
	ix.total += size - ix.files[filename].size
	ix.files[filename] = segEntry{size: size, last: now}
}

// segmentCacheBytes returns the effective per-session cache cap in bytes, or
// 0 for unlimited: HLSConfig.SegmentCacheBytes when > 0, unlimited when < 0,
// otherwise segCacheSegments segments at the session's max bitrate.
func (m *hlsManager) segmentCacheBytes(s *hlsSession) int64 {
	switch {
	case m.cfg.SegmentCacheBytes > 0:
		return m.cfg.SegmentCacheBytes
	case m.cfg.SegmentCacheBytes < 0:
		return 0
	}
	bps := s.tc.maxrateBps
	if bps <= 0 {
		bps = legacyMaxrateBps
	}
	return max(segCacheMinBytes, bps/8*int64(segDur)*segCacheSegments)
}

// evictSegments deletes the least recently used cached segments until the
// session's cache is within limit bytes (0 = unlimited, no-op). It never
// removes keep (the segment the caller is about to serve), anything used
// within segServeGrace, or a segment whose per-file lock is held (being
// transcoded or concurrently returned). If everything left is protected the
// cache temporarily exceeds limit rather than breaking a request.
//
// Segments pinned by a preparation job (s.pins, see prepare.go) are never
// removed and don't count against limit: the cap bounds the unpinned
// working set that live playback builds up around them.
//
// The caller typically holds lockFor(keep); the other locks are only ever
// TryLock'd, so there is no lock-order hazard.
func (s *hlsSession) evictSegments(limit int64, keep string, now time.Time) {
	if limit <= 0 {
		return
	}
	pins := s.pins.Load()
	ix := &s.segIdx
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.loadLocked(s.dir)
	total := ix.total
	if pins != nil {
		for name, e := range ix.files {
			if pins.has(name) {
				total -= e.size
			}
		}
	}
	if total <= limit {
		return
	}
	cands := make([]string, 0, len(ix.files))
	for name, e := range ix.files {
		if name != keep && now.Sub(e.last) >= segServeGrace && !pins.has(name) {
			cands = append(cands, name)
		}
	}
	sort.Slice(cands, func(i, j int) bool { return ix.files[cands[i]].last.Before(ix.files[cands[j]].last) })
	for _, name := range cands {
		if total <= limit {
			return
		}
		l := s.lockFor(name)
		if !l.TryLock() {
			continue
		}
		err := os.Remove(filepath.Join(s.dir, name))
		if err == nil || errors.Is(err, os.ErrNotExist) {
			total -= ix.files[name].size
			ix.total -= ix.files[name].size
			delete(ix.files, name)
		}
		l.Unlock()
	}
}
