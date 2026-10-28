// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package streamproxy

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// defaultCacheMaxBytes is the default total-byte cap for the segment cache (~256 MB).
const defaultCacheMaxBytes = 256 * 1024 * 1024

// maxSegmentBytes is the per-segment read ceiling inside cachedFetch.
// Upstream responses exceeding this value are refused rather than buffered
// unboundedly; 50 MiB is generous for any real HLS/DASH segment.
const maxSegmentBytes int64 = 50 * 1024 * 1024

// inflightWaitTimeout bounds how long a request that must buffer a whole
// segment (DRM decrypt) waits for in-flight budget before answering 503.
const inflightWaitTimeout = 30 * time.Second

// cacheEntry holds a cached segment with its metadata.
type cacheEntry struct {
	key       string
	val       []byte
	hdr       http.Header
	status    int
	expiresAt time.Time
	size      int64 // len(val), tracked for byte-budget eviction
}

// segCache is an in-memory LRU segment cache bounded by entry count, total bytes, and TTL.
type segCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxEntries int
	maxBytes   int64
	totalBytes int64
	items      map[string]*list.Element
	lru        *list.List

	janitorOnce sync.Once
	sweepEvery  time.Duration // janitor interval override (0 = derived from ttl)
	stopOnce    sync.Once
	stopCh      chan struct{}
	stopped     sync.Once
}

// newSegCache creates a segCache with the given TTL and entry cap.
// The total-byte budget defaults to defaultCacheMaxBytes.
func newSegCache(ttl time.Duration, maxEntries int) *segCache {
	return &segCache{
		ttl:        ttl,
		maxEntries: maxEntries,
		maxBytes:   defaultCacheMaxBytes,
		items:      make(map[string]*list.Element),
		lru:        list.New(),
	}
}

// stop terminates the janitor goroutine (idempotent).
func (c *segCache) stop() {
	c.stopped.Do(func() {
		c.janitorOnce.Do(func() {}) // prevent a later start
		c.stopOnce.Do(func() { c.stopCh = make(chan struct{}) })
		close(c.stopCh)
	})
}

// startJanitor launches, at most once, the background sweep that drops
// expired entries. Without it an expired entry would linger until it was
// touched again or pushed out by LRU/byte-budget eviction.
func (c *segCache) startJanitor() {
	c.janitorOnce.Do(func() {
		interval := c.sweepEvery
		if interval <= 0 {
			interval = min(max(c.ttl/2, time.Second), time.Minute)
		}
		c.stopOnce.Do(func() { c.stopCh = make(chan struct{}) })
		stop := c.stopCh
		go func() {
			t := time.NewTicker(interval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					c.sweep()
				case <-stop:
					return
				}
			}
		}()
	})
}

// sweep removes every expired entry.
func (c *segCache) sweep() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.lru.Back(); el != nil; {
		prev := el.Prev()
		e := el.Value.(*cacheEntry)
		if now.After(e.expiresAt) {
			c.totalBytes -= e.size
			delete(c.items, e.key)
			c.lru.Remove(el)
		}
		el = prev
	}
}

// putFull stores a full response entry (body + headers + status).
// It evicts least-recently-used entries to satisfy both the entry cap and the byte budget.
// An entry larger than the whole byte budget is not stored: it could never
// fit, and admitting it would flush every other entry and still overshoot.
func (c *segCache) putFull(key string, val []byte, hdr http.Header, status int) {
	c.startJanitor()
	c.mu.Lock()
	defer c.mu.Unlock()
	newSize := int64(len(val))
	if newSize > c.maxBytes {
		if el, ok := c.items[key]; ok {
			c.removeLocked(el) // never leave a stale copy behind
		}
		return
	}
	if el, ok := c.items[key]; ok {
		c.lru.MoveToFront(el)
		e := el.Value.(*cacheEntry)
		c.totalBytes -= e.size
		c.totalBytes += newSize
		e.val = val
		e.hdr = hdr
		e.status = status
		e.size = newSize
		e.expiresAt = time.Now().Add(c.ttl)
		// A larger replacement can push the cache over budget: evict from the
		// cold end (never the entry just refreshed at the front).
		for c.totalBytes > c.maxBytes {
			back := c.lru.Back()
			if back == nil || back == el {
				break
			}
			c.removeLocked(back)
		}
		return
	}
	// Evict until both constraints are satisfied.
	for c.lru.Len() >= c.maxEntries || c.totalBytes+newSize > c.maxBytes {
		back := c.lru.Back()
		if back == nil {
			break
		}
		c.removeLocked(back)
	}
	entry := &cacheEntry{
		key:       key,
		val:       val,
		hdr:       hdr,
		status:    status,
		expiresAt: time.Now().Add(c.ttl),
		size:      newSize,
	}
	c.totalBytes += newSize
	c.items[key] = c.lru.PushFront(entry)
}

// removeLocked drops one entry and its byte accounting. Caller holds c.mu.
func (c *segCache) removeLocked(el *list.Element) {
	e := el.Value.(*cacheEntry)
	c.totalBytes -= e.size
	delete(c.items, e.key)
	c.lru.Remove(el)
}

// getFull returns the full cache entry, or nil when absent or expired.
func (c *segCache) getFull(key string) *cacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil
	}
	entry := el.Value.(*cacheEntry)
	if time.Now().After(entry.expiresAt) {
		c.totalBytes -= entry.size
		c.lru.Remove(el)
		delete(c.items, key)
		return nil
	}
	c.lru.MoveToFront(el)
	return entry
}

// sha256Pool recycles hash.Hash instances to avoid per-call allocations on
// the segment-request hot path.  Reset is called before each use.
var sha256Pool = sync.Pool{New: func() any { return sha256.New() }}

// cacheKey derives a cache lookup key from the raw URL plus every forwarded
// request header in hdr (the full h_-forwarded set, not just Authorization/
// Cookie), so two requests for the same URL but different credentials —
// however those credentials are carried — never collide. Header names are
// canonicalised (net/http case folding) and sorted before hashing so the
// key is independent of header order and name case. The result is a
// hex-encoded SHA-256 digest, safe to use as a map key.
func cacheKey(rawurl string, hdr http.Header) string {
	// F4: pool the hasher; avoids sha256.New() + fmt.Fprintf reflection on every request.
	h := sha256Pool.Get().(hash.Hash)
	h.Reset()
	_, _ = h.Write([]byte(rawurl))
	// Fold in every forwarded header (canonical name, sorted) so a crafted
	// URL or reordered/differently-cased header set cannot collide with a
	// distinct credential set. NUL/colon/SOH separators bound each field:
	// header names can contain neither NUL nor ':', so the boundary between
	// one header's trailing SOH-terminated values and the next header's NUL
	// prefix is always unambiguous.
	if len(hdr) > 0 {
		type namedValues struct {
			name string
			vals []string
		}
		pairs := make([]namedValues, 0, len(hdr))
		for name, vs := range hdr {
			if len(vs) == 0 {
				continue
			}
			pairs = append(pairs, namedValues{name: http.CanonicalHeaderKey(name), vals: vs})
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].name < pairs[j].name })
		for _, p := range pairs {
			_, _ = h.Write([]byte{0})
			_, _ = h.Write([]byte(p.name))
			_, _ = h.Write([]byte{':'})
			for _, v := range p.vals {
				_, _ = h.Write([]byte(v))
				_, _ = h.Write([]byte{1})
			}
		}
	}
	var buf [32]byte
	h.Sum(buf[:0])
	sha256Pool.Put(h)
	return hex.EncodeToString(buf[:])
}

// cacheableResponse reports whether resp may be buffered into the segment
// cache: a 200 with a known Content-Length no larger than maxSegmentBytes.
// Anything else must be streamed to avoid unbounded buffering.
func cacheableResponse(resp *http.Response) bool {
	return resp.StatusCode == http.StatusOK &&
		resp.ContentLength >= 0 && resp.ContentLength <= maxSegmentBytes
}

// cacheLookup returns a cached response for (rawurl, hdr), if present.
func (h *Handler) cacheLookup(rawurl string, hdr http.Header) ([]byte, http.Header, int, bool) {
	if h.cache == nil {
		return nil, nil, 0, false
	}
	entry := h.cache.getFull(cacheKey(rawurl, hdr))
	if entry == nil {
		return nil, nil, 0, false
	}
	if entry.hdr != nil {
		// F6: return stored clone directly; callers only read the map.
		return entry.val, entry.hdr, entry.status, true
	}
	// hdr was nil at store time — synthesise a minimal Content-Length header.
	outHdr := make(http.Header)
	outHdr.Set("Content-Length", strconv.Itoa(len(entry.val)))
	return entry.val, outHdr, entry.status, true
}

// cachedFetchMode is cachedFetch with a choice of what happens when the
// in-flight budget is exhausted: wait for it (wait == true) or give up with
// errInflightBusy (best-effort callers such as prefetch).
func (h *Handler) cachedFetchMode(ctx context.Context, rawurl string, hdr http.Header, proxyURL string, wait bool) ([]byte, http.Header, int, error) {
	if h.cache == nil || h.cfg.SegCacheTTL == 0 {
		// Caching disabled — fetch directly, but cap the read to prevent OOM.
		return h.fetchBuffered(ctx, "", rawurl, hdr, proxyURL, wait)
	}

	// Cache hit.
	if data, respHdr, status, ok := h.cacheLookup(rawurl, hdr); ok {
		return data, respHdr, status, nil
	}

	// Cache miss — single-flight: concurrent misses for the same key share
	// one upstream fetch.
	key := cacheKey(rawurl, hdr)
	call, leader := h.flightJoin(key)
	if !leader {
		select {
		case <-call.done:
		case <-ctx.Done():
			return nil, nil, 0, ctx.Err()
		}
		if call.err == nil {
			return call.data, call.hdr, call.status, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, 0, err
		}
		// The leader failed (possibly because its own context was cancelled):
		// retry independently rather than inherit its error.
		return h.fetchBuffered(ctx, key, rawurl, hdr, proxyURL, wait)
	}
	defer func() { h.flightFinish(key, call) }()
	// Another leader may have populated the cache between the lookup above
	// and winning the flight.
	if data, respHdr, status, ok := h.cacheLookup(rawurl, hdr); ok {
		call.data, call.hdr, call.status = data, respHdr, status
		return data, respHdr, status, nil
	}
	call.data, call.hdr, call.status, call.err = h.fetchBuffered(ctx, key, rawurl, hdr, proxyURL, wait)
	return call.data, call.hdr, call.status, call.err
}

// fetchBuffered performs an upstream GET and buffers the body, which must not
// exceed maxSegmentBytes. The buffer is sized exactly from Content-Length when
// the upstream sent one, and its bytes are reserved in the in-flight budget
// for the duration of the call, so concurrent fetches cannot pile up beyond
// it. A non-empty key stores a 200 response in the cache under that key.
func (h *Handler) fetchBuffered(ctx context.Context, key, rawurl string, hdr http.Header, proxyURL string, wait bool) ([]byte, http.Header, int, error) {
	resp, err := h.fetch(ctx, http.MethodGet, rawurl, hdr, nil, proxyURL)
	if err != nil {
		return nil, nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	tooLarge := func() ([]byte, http.Header, int, error) {
		if key == "" {
			return nil, nil, http.StatusBadGateway,
				fmt.Errorf("upstream segment too large (> %d bytes)", maxSegmentBytes)
		}
		return nil, nil, http.StatusBadGateway,
			fmt.Errorf("upstream segment too large to cache (> %d bytes)", maxSegmentBytes)
	}
	// An advertised length over the ceiling is refused before reading a byte.
	if resp.ContentLength > maxSegmentBytes {
		return tooLarge()
	}
	n := segmentReservation(resp.ContentLength)
	if err := h.reserveInflight(ctx, n, wait); err != nil {
		return nil, nil, 0, err
	}
	defer h.inflight.release(n)
	data, err := readSegment(resp.Body, resp.ContentLength)
	if errors.Is(err, errSegmentTooLarge) {
		return tooLarge()
	}
	if err != nil {
		return nil, nil, resp.StatusCode, err
	}
	if key != "" && resp.StatusCode == http.StatusOK {
		h.cache.putFull(key, data, resp.Header.Clone(), resp.StatusCode)
	}
	return data, resp.Header, resp.StatusCode, nil
}

// errSegmentTooLarge is returned by readSegment when a body exceeds maxSegmentBytes.
var errSegmentTooLarge = errors.New("upstream segment too large")

// segmentReservation is the in-flight budget a body of the advertised length
// needs: exactly that length when known, else the worst case (maxSegmentBytes).
func segmentReservation(contentLength int64) int64 {
	if contentLength < 0 {
		return maxSegmentBytes
	}
	return contentLength
}

// reserveInflight reserves n bytes of the in-flight budget, waiting for them
// (bounded by ctx) when wait is set and failing fast with errInflightBusy
// otherwise.
func (h *Handler) reserveInflight(ctx context.Context, n int64, wait bool) error {
	if wait {
		return h.inflight.acquire(ctx, n)
	}
	if !h.inflight.tryAcquire(n) {
		return errInflightBusy
	}
	return nil
}

// readSegment reads body fully, refusing anything over maxSegmentBytes with
// errSegmentTooLarge. When contentLength is known the buffer is allocated at
// exactly that size and filled in place — no append growth, which would
// otherwise hold up to ~2x the segment (old backing array + new) at peak.
// net/http enforces Content-Length on the body, so a short read surfaces as
// io.ErrUnexpectedEOF rather than a silently truncated segment.
func readSegment(body io.Reader, contentLength int64) ([]byte, error) {
	if contentLength > maxSegmentBytes {
		return nil, errSegmentTooLarge
	}
	if contentLength >= 0 {
		buf := make([]byte, contentLength)
		if _, err := io.ReadFull(body, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	data, err := io.ReadAll(io.LimitReader(body, maxSegmentBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxSegmentBytes {
		return nil, errSegmentTooLarge
	}
	return data, nil
}

// flightCall is one in-flight upstream segment fetch shared by concurrent
// callers; the result fields are valid once done is closed.
type flightCall struct {
	done   chan struct{}
	data   []byte
	hdr    http.Header
	status int
	err    error
}

// flightJoin registers interest in key. The first caller becomes the leader
// (leader == true) and must call flightFinish; later callers receive the same
// call and wait on call.done.
func (h *Handler) flightJoin(key string) (call *flightCall, leader bool) {
	h.flightMu.Lock()
	defer h.flightMu.Unlock()
	if c, ok := h.flights[key]; ok {
		return c, false
	}
	c := &flightCall{done: make(chan struct{})}
	h.flights[key] = c
	return c, true
}

// flightFinish unregisters the call and releases its waiters.
func (h *Handler) flightFinish(key string, c *flightCall) {
	h.flightMu.Lock()
	delete(h.flights, key)
	h.flightMu.Unlock()
	close(c.done)
}

// awaitFlight blocks while a fetch for key is in flight (e.g. a prefetch
// warming the segment a client has just requested), bounded by ctx. It does
// not register a new flight; callers re-check the cache afterwards.
func (h *Handler) awaitFlight(ctx context.Context, key string) {
	h.flightMu.Lock()
	c, ok := h.flights[key]
	h.flightMu.Unlock()
	if !ok {
		return
	}
	select {
	case <-c.done:
	case <-ctx.Done():
	}
}

// prefetch asynchronously warms the cache for up to cfg.Prebuffer URLs.
// Each goroutine runs under a fresh context bounded by prefetchTimeout so it
// cannot outlive a slow upstream indefinitely. Concurrent goroutines are
// capped by h.prefetchSem; if all slots are occupied the remaining URLs are
// skipped rather than blocked. A prefetch that finds the in-flight buffering
// budget (h.inflight) exhausted gives up instead of queueing behind real
// client requests. Errors are silently ignored.
// No-op when Prebuffer <= 0 or SegCacheTTL == 0.
func (h *Handler) prefetch(ctx context.Context, urls []string, hdr http.Header, proxyURL string) {
	if h.cfg.Prebuffer <= 0 || h.cache == nil {
		return
	}
	limit := h.cfg.Prebuffer
	if limit > len(urls) {
		limit = len(urls)
	}
	for _, u := range urls[:limit] {
		// Non-blocking semaphore acquire — skip remaining URLs when all slots busy.
		select {
		case h.prefetchSem <- struct{}{}:
		default:
			return
		}
		go func() {
			defer func() { <-h.prefetchSem }()
			// Detach from the request's cancellation (prefetch must outlive the
			// originating response) while keeping its values and an explicit
			// wall-clock bound.
			pCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), prefetchTimeout)
			defer cancel()
			_, _, _, _ = h.cachedFetchMode(pCtx, u, hdr, proxyURL, false)
		}()
	}
}

// CacheStats returns the number of entries and total bytes held by the segment
// cache.  Returns (0, 0) when the cache is not configured.
func (h *Handler) CacheStats() (entries int, bytes int64) {
	if h.cache == nil {
		return 0, 0
	}
	h.cache.mu.Lock()
	defer h.cache.mu.Unlock()
	return len(h.cache.items), h.cache.totalBytes
}
