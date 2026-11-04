// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// relayCache keeps byte ranges of relayed sources in memory
// (RIVULET_HLS_RELAY_CACHE_BYTES, HLSConfig.RelayCacheBytes), so that each
// segment transcode doesn't fetch the same container header and index again.
//
// Every segment is a fresh ffmpeg, and each one opens the source at byte 0,
// reads the index (a Matroska Cues element or an MP4 moov, often at the end
// of the file), goes back to the first cluster or the start of the media
// data while it probes the streams, and only then seeks to the segment's
// data: about four range requests per segment, of which only the last one
// differs from segment to segment. Against a host that takes ~400 ms to the
// first byte, the three repeated ones cost over a second per segment.
//
// The cache is content-agnostic: it records the bytes relayed from an offset
// ffmpeg has asked a source for before (so a data read at a new offset is
// never buffered), and answers the next request at that offset from memory,
// fetching only what ffmpeg reads past the recorded bytes. A source is its
// relay token (one per upstream URL). Only a plain 206 that answers exactly
// the single range asked, with a known total length, is recorded or
// compared: never a playlist, a manifest, a decoded compressed body or an
// error status. Each one is compared on its total length, ETag and
// Last-Modified, and a difference turns the cache off for that token for
// good: its source changed, or its servers disagree.
//
// The zero value is disabled; setBytes enables it.
type relayCache struct {
	// budget caps the bytes of every recorded range, across sources; 0
	// disables the cache. Reads being recorded reserve up to as much again
	// (see reserve). Atomic so the disabled path takes no lock.
	budget atomic.Int64

	mu       sync.Mutex
	used     int64 // bytes of recorded ranges
	inflight int64 // bytes reserved by reads being recorded
	tick     uint64
	// lru holds every recorded range (*relaySpan), least recently used at
	// the front.
	lru *list.List
	// sources holds one entry per relay token registered while the cache
	// is enabled (addToken), removed with the token (dropToken).
	sources map[string]*relaySource
}

// relaySource is what the cache holds for one relay token.
type relaySource struct {
	meta  relayCacheMeta
	spans []*relaySpan
	// seen maps each offset ffmpeg asked for to the tick it was last asked,
	// capped at relayCacheSeenPerSource. A range is recorded only from an
	// offset already in it.
	seen map[int64]uint64
	// off is set once a response disagreed with the recorded meta: the
	// token is never cached again, and reads in flight are not committed.
	off bool
}

// relayCacheSeenPerSource bounds the offsets remembered per source. The
// repeated ones (header, index, probe) are asked for on every segment and
// stay; the per-segment data offsets age out.
const relayCacheSeenPerSource = 16

// relayCacheContinueDelay is how long a cache hit for an open range waits,
// after the recorded bytes, for ffmpeg to close the request before fetching
// the rest from upstream. ffmpeg normally reads exactly what it read last
// time and then seeks away, so the rest is rarely wanted; waiting avoids an
// upstream request that would only be cancelled.
const relayCacheContinueDelay = 200 * time.Millisecond

// relayCacheMeta identifies a source's content and holds the headers a hit
// replays.
type relayCacheMeta struct {
	total   int64
	etag    string
	lastMod string
	header  http.Header
}

// sameSource reports whether two responses came from the same content.
func (m relayCacheMeta) sameSource(o relayCacheMeta) bool {
	return m.total == o.total && m.etag == o.etag && m.lastMod == o.lastMod
}

// relaySpan is one recorded byte range. Its fields never change after
// commit, so a hit reads data without holding the lock.
type relaySpan struct {
	token string
	start int64
	data  []byte
	el    *list.Element
}

func (s *relaySpan) end() int64 { return s.start + int64(len(s.data)) }

// setBytes sets the cache budget; 0 or less disables it and drops every
// recorded range. Tokens registered while it was disabled are never cached.
func (c *relayCache) setBytes(n int64) {
	n = max(n, 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.budget.Store(n)
	if n == 0 {
		c.lru, c.sources, c.used = nil, nil, 0
		return
	}
	if c.sources == nil {
		c.sources, c.lru = map[string]*relaySource{}, list.New()
	}
	c.evictLocked(nil)
}

func (c *relayCache) enabled() bool { return c.budget.Load() > 0 }

// spanCap is the most bytes recorded from one request: a quarter of the
// budget, so a few sources' headers and indexes fit at once.
func (c *relayCache) spanCap() int64 { return c.budget.Load() / 4 }

// addToken starts tracking a newly registered relay token (no-op when the
// cache is disabled or the token is known). Called with the relay's mu held.
func (c *relayCache) addToken(token string) {
	if !c.enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sources != nil && c.sources[token] == nil {
		c.sources[token] = &relaySource{seen: map[int64]uint64{}}
	}
}

// lookup notes that ffmpeg asked token's source for the bytes at start and
// returns the recorded range holding them (nil if none) with the source's
// meta, and whether a fetch from start should be recorded: start was asked
// for before.
func (c *relayCache) lookup(token string, start int64) (hit *relaySpan, meta relayCacheMeta, record bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	src := c.sources[token]
	if src == nil || src.off {
		return nil, relayCacheMeta{}, false
	}
	c.tick++
	_, record = src.seen[start]
	src.seen[start] = c.tick
	if len(src.seen) > relayCacheSeenPerSource {
		oldest, at := int64(0), c.tick
		for off, t := range src.seen {
			if t < at {
				oldest, at = off, t
			}
		}
		delete(src.seen, oldest)
	}
	for _, s := range src.spans {
		if s.start <= start && start < s.end() && (hit == nil || s.end() > hit.end()) {
			hit = s
		}
	}
	if hit != nil {
		c.lru.MoveToBack(hit.el)
	}
	return hit, src.meta, record
}

// observe checks a partial response for token's source against the
// recorded meta (taking it as the meta if there is none yet). A mismatch
// turns the source off; observe reports whether the response matched.
func (c *relayCache) observe(token string, meta relayCacheMeta) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	src := c.sources[token]
	switch {
	case src == nil || src.off:
		return true
	case src.meta.header == nil:
		src.meta = meta
		return true
	case src.meta.sameSource(meta):
		return true
	}
	c.offLocked(src)
	return false
}

// invalidate turns token's source off (it changed under a hit).
func (c *relayCache) invalidate(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if src := c.sources[token]; src != nil {
		c.offLocked(src)
	}
}

// offLocked drops src's recorded ranges and stops caching it. c.mu must be
// held.
func (c *relayCache) offLocked(src *relaySource) {
	c.dropSpansLocked(src)
	src.off, src.meta, src.seen = true, relayCacheMeta{}, nil
}

// commit records data as the bytes of token's source from start, unless
// the source was dropped or turned off while the read was in flight, meta
// is not the source's, or a recorded range already covers it.
func (c *relayCache) commit(token string, meta relayCacheMeta, start int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	src := c.sources[token]
	if src == nil || src.off || len(data) == 0 || src.meta.header != nil && !src.meta.sameSource(meta) {
		return
	}
	src.meta = meta
	end := start + int64(len(data))
	for _, s := range src.spans {
		if s.start <= start && end <= s.end() {
			c.lru.MoveToBack(s.el)
			return // already covered
		}
	}
	kept := src.spans[:0]
	for _, s := range src.spans {
		if start <= s.start && s.end() <= end {
			c.lru.Remove(s.el) // covered by the new range
			c.used -= int64(len(s.data))
			continue
		}
		kept = append(kept, s)
	}
	s := &relaySpan{token: token, start: start, data: data}
	s.el = c.lru.PushBack(s)
	src.spans = append(kept, s)
	c.used += int64(len(data))
	c.evictLocked(s)
}

// reserve claims n bytes for a read about to be recorded, failing when the
// reads already in flight hold the whole budget. release returns them.
func (c *relayCache) reserve(n int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inflight+n > c.budget.Load() {
		return false
	}
	c.inflight += n
	return true
}

func (c *relayCache) release(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inflight -= n
}

// dropToken forgets token's source entirely (its relay token was removed).
func (c *relayCache) dropToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if src := c.sources[token]; src != nil {
		c.dropSpansLocked(src)
		delete(c.sources, token)
	}
}

// dropSpansLocked removes every recorded range of src. c.mu must be held.
func (c *relayCache) dropSpansLocked(src *relaySource) {
	for _, s := range src.spans {
		c.lru.Remove(s.el)
		c.used -= int64(len(s.data))
	}
	src.spans = nil
}

// evictLocked drops the least recently used ranges, never keep, until the
// recorded bytes fit the budget. c.mu must be held.
func (c *relayCache) evictLocked(keep *relaySpan) {
	for c.lru != nil && c.used > c.budget.Load() {
		el := c.lru.Front()
		if el == nil {
			return
		}
		s := el.Value.(*relaySpan)
		if s == keep {
			if el = el.Next(); el == nil {
				return
			}
			s = el.Value.(*relaySpan)
		}
		c.lru.Remove(el)
		c.used -= int64(len(s.data))
		if src := c.sources[s.token]; src != nil {
			for i, o := range src.spans {
				if o == s {
					src.spans = append(src.spans[:i], src.spans[i+1:]...)
					break
				}
			}
		}
	}
}

// parseByteRange parses a single-range Range header, "bytes=S-" or
// "bytes=S-E" (end -1 when open). Suffix ranges ("bytes=-N") and multiple
// ranges are not handled by the cache.
func parseByteRange(h string) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(h), "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	a, b, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(a), 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if b = strings.TrimSpace(b); b == "" {
		return start, -1, true
	}
	end, err = strconv.ParseInt(b, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
}

// parseContentRange parses "bytes S-E/L" with a known total length L.
func parseContentRange(h string) (start, end, total int64, ok bool) {
	spec, found := strings.CutPrefix(strings.TrimSpace(h), "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	rng, size, found := strings.Cut(spec, "/")
	if !found {
		return 0, 0, 0, false
	}
	a, b, found := strings.Cut(rng, "-")
	if !found {
		return 0, 0, 0, false
	}
	var errs [3]error
	start, errs[0] = strconv.ParseInt(a, 10, 64)
	end, errs[1] = strconv.ParseInt(b, 10, 64)
	total, errs[2] = strconv.ParseInt(size, 10, 64)
	if errors.Join(errs[:]...) != nil || start < 0 || end < start || total <= end {
		return 0, 0, 0, false
	}
	return start, end, total, true
}

// rangeLast is the last byte a request for bytes from a start to end (-1:
// open) gets from a resource of total bytes.
func rangeLast(total, end int64) int64 {
	if end >= 0 && end < total-1 {
		return end
	}
	return total - 1
}

// relayCacheMetaOf returns the meta of a response the cache may record or
// compare: a 206 with no content coding whose Content-Range answers exactly
// bytes start-end (end -1: to the last byte) of a known total length. A
// server that shortens a range is not cached.
func relayCacheMetaOf(resp *http.Response, start, end int64) (relayCacheMeta, bool) {
	if resp.StatusCode != http.StatusPartialContent {
		return relayCacheMeta{}, false
	}
	for _, ce := range resp.Header.Values("Content-Encoding") {
		if ce = strings.TrimSpace(ce); ce != "" && !strings.EqualFold(ce, "identity") {
			return relayCacheMeta{}, false
		}
	}
	s, e, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if !ok || s != start || e != rangeLast(total, end) {
		return relayCacheMeta{}, false
	}
	return relayCacheMeta{
		total:   total,
		etag:    resp.Header.Get("ETag"),
		lastMod: resp.Header.Get("Last-Modified"),
		header:  resp.Header.Clone(),
	}, true
}

// relayRecorder records the bytes of a response body read through it as
// the range from start of token's source, up to max bytes (reserved from
// the cache). It commits as soon as it holds max bytes of a longer body, or
// when the read ends (finish); never more bytes than the response promised
// (want), and nothing from a body that turned out longer.
type relayRecorder struct {
	c     *relayCache
	token string
	meta  relayCacheMeta
	start int64
	r     io.Reader
	buf   []byte
	max   int64
	want  int64 // bytes from start the response promised
	read  int64 // bytes from start seen so far
	done  bool
}

// newRecorder returns a recorder for a response promising want bytes from
// start, holding prefix (the bytes before the response's own, from a cache
// hit), or nil if there is no room for one.
func (c *relayCache) newRecorder(token string, meta relayCacheMeta, start, want int64, prefix []byte) *relayRecorder {
	n := min(c.spanCap(), want)
	if int64(len(prefix)) >= n || !c.reserve(n) {
		return nil
	}
	return &relayRecorder{c: c, token: token, meta: meta, start: start, max: n, want: want,
		buf: slices.Clone(prefix), read: int64(len(prefix))}
}

func (t *relayRecorder) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	t.read += int64(n)
	if !t.done && n > 0 {
		t.buf = append(t.buf, p[:min(int64(n), t.max-int64(len(t.buf)))]...)
		if int64(len(t.buf)) == t.max && t.max < t.want {
			t.finish(true) // full: the rest of a long read isn't buffered
		}
	}
	return n, err
}

// finish commits what was recorded if ok (and the body was no longer than
// promised) and releases the reservation. Only the first call counts.
func (t *relayRecorder) finish(ok bool) {
	if t.done {
		return
	}
	t.done = true
	if ok && t.read <= t.want {
		t.c.commit(t.token, t.meta, t.start, slices.Clip(t.buf))
	}
	t.buf = nil
	t.c.release(t.max)
}

// sniffClean reports whether the miss path's content sniff would pass b,
// the body from the requested offset, straight through as media: no
// playlist magic and nothing a DASH manifest could be. b holds either the
// whole response or at least relayXMLSniffBytes, as much as the miss path
// looks at.
func sniffClean(b []byte) bool {
	head := b[:min(len(b), relaySniffBytes)]
	if hasPlaylistMagic(head) || containsMPDTag(head) {
		return false
	}
	return !looksLikeXML(head) || !xmlBodyMayBeDASH(b[:min(len(b), relayXMLSniffBytes)])
}

// serveCached answers a GET for bytes start-end (end -1: open) of token's
// source from the recorded range hit, then relays whatever ffmpeg reads past
// it from upstream. It reports false, having written nothing, when the miss
// path must serve the request instead.
func (r *mediaRelay) serveCached(w http.ResponseWriter, req *http.Request, token, upstream, self string, start, end int64, hit *relaySpan, meta relayCacheMeta) bool {
	last := rangeLast(meta.total, end)
	if start > last || start < hit.start {
		return false
	}
	cached := hit.data[start-hit.start:]
	if n := last - start + 1; int64(len(cached)) > n {
		cached = cached[:n]
	}
	next := start + int64(len(cached)) // first byte not recorded
	// The miss path sniffs up to relayXMLSniffBytes of every response.
	// Fewer recorded bytes than that (with more to come) can't be judged
	// the same way.
	if next <= last && len(cached) < relayXMLSniffBytes || !sniffClean(cached) {
		return false
	}
	h := w.Header()
	copyProxyHeaders(h, meta.header)
	h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, last, meta.total))
	h.Set("Content-Length", strconv.FormatInt(last-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	if _, err := w.Write(cached); err != nil || next > last {
		return true
	}
	if end < 0 {
		// An open read: ffmpeg wants more than was recorded only now and
		// then (even a range recorded up to the cap is usually a probe of
		// the first cluster, recorded once from a read that ran on into an
		// early segment's data). A closed range was asked for in full.
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-req.Context().Done():
			return true
		case <-time.After(relayCacheContinueDelay):
		}
	}
	rng := fmt.Sprintf("bytes=%d-", next)
	if end >= 0 {
		rng += strconv.FormatInt(end, 10)
	}
	outReq, client, err := r.upstreamRequest(req.Context(), http.MethodGet, upstream, self, rng)
	if err != nil {
		panic(http.ErrAbortHandler) // headers are out: fail the transfer
	}
	resp, err := client.Do(outReq)
	if err != nil {
		r.recordFetchError(req, token, err)
		panic(http.ErrAbortHandler)
	}
	defer func() { _ = resp.Body.Close() }()
	m, ok := relayCacheMetaOf(resp, next, end)
	if !ok || !m.sameSource(meta) {
		r.cache.invalidate(token)
		if !r.recordStatusFailure(req, token, resp) {
			r.recordFailure(req, token, "upstream changed while relayed from the cache", false)
		}
		panic(http.ErrAbortHandler)
	}
	rec := r.cache.newRecorder(token, meta, start, last-start+1, cached)
	r.copyUpstreamBody(w, req, token, resp.Body, rec)
	return true
}

// copyUpstreamBody streams body to w. If rec is set (its r is filled in
// here), the bytes read are recorded, unless the upstream body broke. A
// broken upstream body aborts the downstream connection, so ffmpeg sees a
// failed transfer rather than a cleanly ended, truncated one.
func (r *mediaRelay) copyUpstreamBody(w http.ResponseWriter, req *http.Request, token string, body io.Reader, rec *relayRecorder) {
	rd := &errTrackingReader{r: body}
	var src io.Reader = rd
	if rec != nil {
		rec.r, src = rd, rec
	}
	_, _ = io.Copy(w, src)
	broken := rd.err != nil && !errors.Is(rd.err, io.EOF)
	if rec != nil {
		// A cancelled read is ffmpeg going away: what was read is still
		// the source's bytes.
		rec.finish(!broken || errors.Is(rd.err, context.Canceled))
	}
	if broken {
		// Not recordFailure: ffmpeg often drops its request as soon as the
		// body breaks, so its context is no reliable sign that the break
		// was ours; only a cancelled upstream read is.
		if !errors.Is(rd.err, context.Canceled) {
			r.storeFailure(token, "upstream body ended early", isTimeout(rd.err))
		}
		panic(http.ErrAbortHandler)
	}
}
