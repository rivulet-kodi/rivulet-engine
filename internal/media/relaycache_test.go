// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

// Tests for the relay's header/index cache: internal/media/relaycache.go.

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// rangeOrigin is an upstream that serves one body with Range support and
// logs the Range header of every request.
type rangeOrigin struct {
	mu     sync.Mutex
	body   []byte
	etag   string
	ranges []string
	// serve, if set, replaces the default handler.
	serve func(w http.ResponseWriter, req *http.Request, body []byte)
	srv   *httptest.Server
}

func newRangeOrigin(t *testing.T, body []byte) *rangeOrigin {
	t.Helper()
	o := &rangeOrigin{body: body, etag: `"v1"`}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o.mu.Lock()
		body, etag, serve := o.body, o.etag, o.serve
		o.ranges = append(o.ranges, req.Header.Get("Range"))
		o.mu.Unlock()
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "video/x-matroska")
		if serve != nil {
			serve(w, req, body)
			return
		}
		http.ServeContent(w, req, "", time.Time{}, bytes.NewReader(body))
	}))
	t.Cleanup(o.srv.Close)
	return o
}

func (o *rangeOrigin) requests() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.ranges...)
}

func (o *rangeOrigin) replace(body []byte, etag string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.body, o.etag = body, etag
}

// mediaBody returns n bytes that look like a Matroska file to the sniff: the
// EBML magic, then a pattern with no playlist or manifest markers.
func mediaBody(n int) []byte {
	b := make([]byte, n)
	copy(b, "\x1a\x45\xdf\xa3")
	for i := 4; i < n; i++ {
		b[i] = byte(i*7+i>>8) | 0x80
	}
	return b
}

// cachedRelay returns a loopback-permitting relay with a cache of budget
// bytes and the relay URL for o.
func cachedRelay(t *testing.T, o *rangeOrigin, budget int64) (*mediaRelay, string) {
	t.Helper()
	r := newMediaRelay(false)
	r.setCacheBytes(budget)
	u, err := r.register(o.srv.URL + "/movie.mkv")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return r, u
}

// cacheGet fetches rng ("" for none) of u and reads the whole body. The
// tests count upstream requests right after a request returns, which works
// because the default client reuses its keep-alive connection: the relay
// finishes one request (and its commit) before it reads the next.
func cacheGet(t *testing.T, u, rng string) (*http.Response, []byte, error) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", rng, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp, body, err
}

// mustCacheGet is cacheGet that fails the test on a body error and checks the
// status and bytes against want (the source bytes from start).
func mustCacheGet(t *testing.T, u, rng string, want []byte) *http.Response {
	t.Helper()
	resp, body, err := cacheGet(t, u, rng)
	if err != nil {
		t.Fatalf("GET %s: body: %v", rng, err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("GET %s: status %d, want 206", rng, resp.StatusCode)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("GET %s: %d bytes differ from the source's %d", rng, len(body), len(want))
	}
	return resp
}

func TestRelayCacheOffByDefault(t *testing.T) {
	src := mediaBody(64 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 0)
	for range 3 {
		mustCacheGet(t, u, "bytes=0-", src)
	}
	if n := len(o.requests()); n != 3 {
		t.Errorf("upstream requests = %d, want 3 (no cache by default)", n)
	}
}

func TestRelayCacheAnswersARepeatedOffsetFromMemory(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)

	first := mustCacheGet(t, u, "bytes=0-", src) // first sight: not recorded
	mustCacheGet(t, u, "bytes=0-", src)          // asked before: recorded
	hit := mustCacheGet(t, u, "bytes=0-", src)   // answered from memory
	if n := len(o.requests()); n != 2 {
		t.Fatalf("upstream requests = %d (%q), want 2", n, o.requests())
	}
	for _, h := range []string{"Content-Range", "Content-Length", "ETag", "Content-Type", "Accept-Ranges"} {
		if got, want := hit.Header.Get(h), first.Header.Get(h); got != want {
			t.Errorf("hit %s = %q, upstream sent %q", h, got, want)
		}
	}

	// A range inside the recorded one, open or closed, needs no request.
	mustCacheGet(t, u, "bytes=1000-", src[1000:])
	resp := mustCacheGet(t, u, "bytes=1000-1999", src[1000:2000])
	if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes 1000-1999/%d", len(src)); got != want {
		t.Errorf("closed hit Content-Range = %q, want %q", got, want)
	}
	if got := resp.Header.Get("Content-Length"); got != "1000" {
		t.Errorf("closed hit Content-Length = %q, want 1000", got)
	}
	if n := len(o.requests()); n != 2 {
		t.Errorf("upstream requests after inner hits = %d (%q), want 2", n, o.requests())
	}
}

func TestRelayCacheFetchesWhatFfmpegReadsPastTheRecordedBytes(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)

	mustCacheGet(t, u, "bytes=0-32767", src[:32768])
	mustCacheGet(t, u, "bytes=0-32767", src[:32768]) // recorded: 32 KiB
	mustCacheGet(t, u, "bytes=0-", src)              // 32 KiB from memory, the rest fetched
	reqs := o.requests()
	if len(reqs) != 3 || reqs[2] != "bytes=32768-" {
		t.Fatalf("upstream requests = %q, want the third to fetch only bytes=32768-", reqs)
	}
	// That read was recorded too: the whole body is now in memory.
	mustCacheGet(t, u, "bytes=0-", src)
	if n := len(o.requests()); n != 3 {
		t.Errorf("upstream requests = %d (%q), want 3", n, o.requests())
	}
}

func TestRelayCacheCapsARecordedRange(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	r, u := cachedRelay(t, o, 4*40000) // ranges of at most 40000 bytes

	mustCacheGet(t, u, "bytes=0-", src)
	mustCacheGet(t, u, "bytes=0-", src) // recorded: the first 40000 bytes only
	mustCacheGet(t, u, "bytes=0-", src)
	reqs := o.requests()
	if len(reqs) != 3 || reqs[2] != "bytes=40000-" {
		t.Fatalf("upstream requests = %q, want the third to fetch only bytes=40000-", reqs)
	}
	r.cache.mu.Lock()
	used := r.cache.used
	r.cache.mu.Unlock()
	if used != 40000 {
		t.Errorf("cache holds %d bytes, want 40000 (one capped range)", used)
	}
}

func TestRelayCacheDropsAChangedSourceUnderAHit(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])

	o.replace(mediaBody(300<<10), `"v2"`)
	// The recorded 32 KiB go out, then the rest comes from a different file:
	// the transfer must fail rather than mix the two.
	resp, body, err := cacheGet(t, u, "bytes=0-")
	if err == nil {
		t.Fatalf("GET after the source changed: %d, %d bytes and no error; want an aborted transfer", resp.StatusCode, len(body))
	}
	before := len(o.requests())
	newSrc := mediaBody(300 << 10)
	mustCacheGet(t, u, "bytes=0-32767", newSrc[:32768])
	if n := len(o.requests()); n != before+1 {
		t.Errorf("request after the change went to upstream %d times, want 1 (cache dropped)", n-before)
	}
}

func TestRelayCacheDropsAChangedSourceOnAMiss(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])

	newSrc := mediaBody(300 << 10)
	newSrc[100] = 0x81 // differs inside the recorded range
	o.replace(newSrc, `"v2"`)
	mustCacheGet(t, u, "bytes=50000-", newSrc[50000:]) // a miss sees the new ETag
	mustCacheGet(t, u, "bytes=0-32767", newSrc[:32768])
	if reqs := o.requests(); len(reqs) != 4 {
		t.Errorf("upstream requests = %q, want 4 (the recorded range dropped)", reqs)
	}
}

func TestRelayCacheRecordsOnlyPlainExactPartialResponses(t *testing.T) {
	src := mediaBody(64 << 10)
	cases := map[string]func(w http.ResponseWriter, req *http.Request, body []byte){
		"200 without ranges": func(w http.ResponseWriter, _ *http.Request, body []byte) {
			_, _ = w.Write(body)
		},
		"shortened range": func(w http.ResponseWriter, _ *http.Request, body []byte) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-999/%d", len(body)))
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[:1000])
		},
		"unknown total": func(w http.ResponseWriter, _ *http.Request, body []byte) {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/*", len(body)-1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)
		},
		"content coding": func(w http.ResponseWriter, _ *http.Request, body []byte) {
			w.Header().Set("Content-Encoding", "br")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)
		},
	}
	for name, serve := range cases {
		t.Run(name, func(t *testing.T) {
			o := newRangeOrigin(t, src)
			o.serve = serve
			_, u := cachedRelay(t, o, 64<<20)
			for range 3 {
				resp, _, err := cacheGet(t, u, "bytes=0-")
				if err != nil || resp.StatusCode >= 400 {
					t.Fatalf("GET: %v, status %d", err, resp.StatusCode)
				}
			}
			if n := len(o.requests()); n != 3 {
				t.Errorf("upstream requests = %d, want 3 (not recorded)", n)
			}
		})
	}
}

func TestRelayCacheNeverRecordsPlaylists(t *testing.T) {
	pl := []byte("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nseg0.ts\n#EXT-X-ENDLIST\n")
	o := newRangeOrigin(t, pl)
	_, u := cachedRelay(t, o, 64<<20)
	for range 3 {
		resp, body, err := cacheGet(t, u, "bytes=0-")
		if err != nil || !strings.Contains(string(body), "/r/") {
			t.Fatalf("GET playlist: %v, status %d, body %q; want a rewritten playlist", err, resp.StatusCode, body)
		}
	}
	if n := len(o.requests()); n != 3 {
		t.Errorf("upstream requests = %d, want 3", n)
	}
}

// A hit at an offset where the recorded bytes would look like a playlist
// goes to upstream, so the sniff decides exactly as it would without the
// cache.
func TestRelayCacheHitIsSniffedAtItsOwnOffset(t *testing.T) {
	src := mediaBody(200 << 10)
	copy(src[40000:], "#EXTM3U\n")
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	mustCacheGet(t, u, "bytes=0-", src)
	mustCacheGet(t, u, "bytes=0-", src)
	before := len(o.requests())
	_, _, _ = cacheGet(t, u, "bytes=40000-")
	if n := len(o.requests()); n != before+1 {
		t.Errorf("upstream requests for the playlist-looking offset = %d, want 1", n-before)
	}
}

func TestRelayCacheSkipsABrokenUpstreamBody(t *testing.T) {
	src := mediaBody(64 << 10)
	o := newRangeOrigin(t, src)
	o.serve = func(w http.ResponseWriter, _ *http.Request, body []byte) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(body)-1, len(body)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[:len(body)/2]) // then the connection closes short
	}
	_, u := cachedRelay(t, o, 64<<20)
	for range 3 {
		if _, _, err := cacheGet(t, u, "bytes=0-"); err == nil {
			t.Fatal("GET of a short upstream body: no error, want an aborted transfer")
		}
	}
	if n := len(o.requests()); n != 3 {
		t.Errorf("upstream requests = %d, want 3 (a broken body is never recorded)", n)
	}
}

func TestRelayCacheForgetsADeletedToken(t *testing.T) {
	src := mediaBody(64 << 10)
	o := newRangeOrigin(t, src)
	r, u := cachedRelay(t, o, 64<<20)
	mustCacheGet(t, u, "bytes=0-", src)
	mustCacheGet(t, u, "bytes=0-", src)

	r.mu.Lock()
	for _, el := range r.tokens {
		r.deleteElemLocked(el)
	}
	r.mu.Unlock()
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	if len(r.cache.sources) != 0 || r.cache.used != 0 || r.cache.lru.Len() != 0 {
		t.Errorf("after the token's removal the cache holds %d sources, %d bytes, %d ranges; want none",
			len(r.cache.sources), r.cache.used, r.cache.lru.Len())
	}
}

func TestRelayCacheEvictsLeastRecentlyUsed(t *testing.T) {
	var c relayCache
	c.setBytes(100)
	meta := relayCacheMeta{total: 1000, header: http.Header{}}
	for _, tok := range []string{"a", "b", "c"} {
		c.addToken(tok)
		c.lookup(tok, 0)
		c.commit(tok, meta, 0, make([]byte, 40))
	}
	// a was recorded first and not used since: it goes.
	if hit, _, _ := c.lookup("a", 0); hit != nil {
		t.Error("a still cached, want it evicted (least recently used)")
	}
	if hit, _, _ := c.lookup("b", 0); hit == nil {
		t.Error("b evicted, want it kept")
	}
	if c.used != 80 {
		t.Errorf("used = %d, want 80", c.used)
	}
	// A range covering recorded ones replaces them.
	c.commit("b", meta, 0, make([]byte, 60))
	if c.used != 100 || len(c.sources["b"].spans) != 1 {
		t.Errorf("after a covering range: used %d, b has %d ranges; want 100 and 1", c.used, len(c.sources["b"].spans))
	}
	c.setBytes(0)
	if c.used != 0 || c.sources != nil || c.enabled() {
		t.Errorf("setBytes(0): used %d, sources %v, enabled %v; want everything dropped", c.used, c.sources, c.enabled())
	}
}

func TestRelayCacheRemembersFewOffsetsPerSource(t *testing.T) {
	var c relayCache
	c.setBytes(1 << 20)
	c.addToken("a")
	for off := range int64(relayCacheSeenPerSource * 4) {
		c.lookup("a", 0) // asked on every segment: never forgotten
		c.lookup("a", 1000+off)
	}
	if n := len(c.sources["a"].seen); n > relayCacheSeenPerSource {
		t.Errorf("remembered %d offsets, want at most %d", n, relayCacheSeenPerSource)
	}
	if _, _, record := c.lookup("a", 0); !record {
		t.Error("offset 0, asked for every time, was forgotten")
	}
}

func TestParseByteRange(t *testing.T) {
	cases := []struct {
		in         string
		start, end int64
		ok         bool
	}{
		{"bytes=0-", 0, -1, true},
		{"bytes=114305939-", 114305939, -1, true},
		{"bytes=10-19", 10, 19, true},
		{" bytes=5-5 ", 5, 5, true},
		{"bytes=-500", 0, 0, false},
		{"bytes=0-1,5-9", 0, 0, false},
		{"bytes=9-5", 0, 0, false},
		{"items=0-", 0, 0, false},
		{"", 0, 0, false},
	}
	for _, c := range cases {
		s, e, ok := parseByteRange(c.in)
		if ok != c.ok || ok && (s != c.start || e != c.end) {
			t.Errorf("parseByteRange(%q) = %d, %d, %v; want %d, %d, %v", c.in, s, e, ok, c.start, c.end, c.ok)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in                string
		start, end, total int64
		ok                bool
	}{
		{"bytes 0-99/100", 0, 99, 100, true},
		{"bytes 40000-204799/204800", 40000, 204799, 204800, true},
		{"bytes 0-99/*", 0, 0, 0, false},
		{"bytes */100", 0, 0, 0, false},
		{"bytes 0-100/100", 0, 0, 0, false},
		{"bytes 5-4/100", 0, 0, 0, false},
		{"", 0, 0, 0, false},
	}
	for _, c := range cases {
		s, e, l, ok := parseContentRange(c.in)
		if ok != c.ok || ok && (s != c.start || e != c.end || l != c.total) {
			t.Errorf("parseContentRange(%q) = %d, %d, %d, %v; want %d, %d, %d, %v", c.in, s, e, l, ok, c.start, c.end, c.total, c.ok)
		}
	}
}

// recordFirst32K makes the relay record bytes 0-32767 of src: the first
// request at the offset is only noted, the second is recorded.
func recordFirst32K(t *testing.T, u string, src []byte) {
	t.Helper()
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])
}

func cacheUsed(r *mediaRelay) (used, inflight int64) {
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	return r.cache.used, r.cache.inflight
}

// A body longer than its Content-Range is relayed as before but never
// recorded: its extra bytes are no part of the source.
func TestRelayCacheIgnoresAnOverlongBody(t *testing.T) {
	src := mediaBody(200)
	o := newRangeOrigin(t, src)
	o.serve = func(w http.ResponseWriter, _ *http.Request, body []byte) {
		w.Header().Set("Content-Range", "bytes 0-99/100")
		w.WriteHeader(http.StatusPartialContent)
		w.(http.Flusher).Flush() // chunked: no Content-Length to stop at 100
		_, _ = w.Write(body)
	}
	r, u := cachedRelay(t, o, 64<<20)
	for range 3 {
		if resp, _, err := cacheGet(t, u, "bytes=0-"); err != nil || resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("GET: %v", err)
		}
	}
	if used, inflight := cacheUsed(r); used != 0 || inflight != 0 {
		t.Errorf("cache holds %d bytes (%d reserved), want none", used, inflight)
	}
	if n := len(o.requests()); n != 3 {
		t.Errorf("upstream requests = %d, want 3", n)
	}
}

// A read recorded under one version of a source that finishes after
// another version was seen is discarded, not committed over the change.
func TestRelayCacheDiscardsAStaleRecording(t *testing.T) {
	var c relayCache
	c.setBytes(1 << 20)
	c.addToken("a")
	v1 := relayCacheMeta{total: 100, etag: `"v1"`, header: http.Header{}}
	v2 := relayCacheMeta{total: 100, etag: `"v2"`, header: http.Header{}}
	c.lookup("a", 0)
	c.observe("a", v1)
	rec := c.newRecorder("a", v1, 0, 100, nil)
	if rec == nil {
		t.Fatal("newRecorder: nil")
	}
	c.observe("a", v2) // another request sees the new version
	rec.r = bytes.NewReader(make([]byte, 100))
	_, _ = io.ReadAll(rec)
	rec.finish(true)
	if c.used != 0 || c.inflight != 0 {
		t.Errorf("after a stale commit: used %d, reserved %d; want 0, 0", c.used, c.inflight)
	}
	if hit, _, record := c.lookup("a", 0); hit != nil || record {
		t.Errorf("lookup after the change: hit %v, record %v; want the source off", hit != nil, record)
	}
}

// Servers that disagree on validators (a CDN whose nodes send different
// ETags) turn the cache off for the token instead of failing transfers.
func TestRelayCacheTurnsOffForDisagreeingServers(t *testing.T) {
	src := mediaBody(64 << 10)
	o := newRangeOrigin(t, src)
	var n atomic.Int32
	o.serve = func(w http.ResponseWriter, req *http.Request, body []byte) {
		w.Header().Set("ETag", fmt.Sprintf(`"node%d"`, n.Add(1)%2))
		http.ServeContent(w, req, "", time.Time{}, bytes.NewReader(body))
	}
	r, u := cachedRelay(t, o, 64<<20)
	for range 6 {
		mustCacheGet(t, u, "bytes=0-", src)
	}
	if used, _ := cacheUsed(r); used != 0 || len(o.requests()) != 6 {
		t.Errorf("cache holds %d bytes after %d upstream requests, want 0 after 6", used, len(o.requests()))
	}
}

func TestRelayCacheClosedRangeContinuesAtOnce(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	recordFirst32K(t, u, src)
	began := time.Now()
	resp := mustCacheGet(t, u, "bytes=0-99999", src[:100000])
	if took := time.Since(began); took >= relayCacheContinueDelay {
		t.Errorf("closed range took %v, want no wait for ffmpeg to leave (%v)", took, relayCacheContinueDelay)
	}
	if got := resp.Header.Get("Content-Length"); got != "100000" {
		t.Errorf("Content-Length = %q, want 100000", got)
	}
	if reqs := o.requests(); len(reqs) != 3 || reqs[2] != "bytes=32768-99999" {
		t.Errorf("upstream requests = %q, want the third to be bytes=32768-99999", reqs)
	}
}

// ffmpeg reading the recorded bytes and closing (its seek away) costs no
// upstream request.
func TestRelayCacheNoUpstreamRequestWhenFfmpegLeaves(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	recordFirst32K(t, u, src)
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	req.Header.Set("Range", "bytes=0-")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	got := make([]byte, 32768)
	if _, err := io.ReadFull(resp.Body, got); err != nil || !bytes.Equal(got, src[:32768]) {
		t.Fatalf("recorded bytes: %v", err)
	}
	_ = resp.Body.Close() // unread body: the connection closes
	time.Sleep(2 * relayCacheContinueDelay)
	if n := len(o.requests()); n != 2 {
		t.Errorf("upstream requests = %d (%q), want 2", n, o.requests())
	}
}

func TestRelayCacheContinuationFailureFailsTheTransfer(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	r, u := cachedRelay(t, o, 64<<20)
	recordFirst32K(t, u, src)
	o.mu.Lock()
	o.serve = func(w http.ResponseWriter, req *http.Request, body []byte) {
		if req.Header.Get("Range") == "bytes=32768-" {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		http.ServeContent(w, req, "", time.Time{}, bytes.NewReader(body))
	}
	o.mu.Unlock()
	began := time.Now()
	if _, _, err := cacheGet(t, u, "bytes=0-"); err == nil {
		t.Fatal("GET with a failing continuation: no error, want an aborted transfer")
	}
	if f, ok := r.failureSince(u, began); !ok || !strings.Contains(f.reason, "503") {
		t.Errorf("failure recorded = %+v, %v; want the upstream 503", f, ok)
	}
	before := len(o.requests())
	mustCacheGet(t, u, "bytes=0-32767", src[:32768])
	if n := len(o.requests()); n != before+1 {
		t.Errorf("after a failed continuation the next read went upstream %d times, want 1 (cache off)", n-before)
	}
}

func TestRelayCacheBypassesHEAD(t *testing.T) {
	src := mediaBody(64 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	mustCacheGet(t, u, "bytes=0-", src)
	mustCacheGet(t, u, "bytes=0-", src)
	req, _ := http.NewRequest(http.MethodHead, u, nil)
	req.Header.Set("Range", "bytes=0-")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	_ = resp.Body.Close()
	if n := len(o.requests()); n != 3 {
		t.Errorf("upstream requests = %d, want 3 (HEAD goes upstream)", n)
	}
}

// A recorded range shorter than the miss path's sniff answers only a request
// it covers whole.
func TestRelayCacheShortRangeAnswersOnlyWhatItCovers(t *testing.T) {
	src := mediaBody(64 << 10)
	o := newRangeOrigin(t, src)
	_, u := cachedRelay(t, o, 64<<20)
	mustCacheGet(t, u, "bytes=0-999", src[:1000])
	mustCacheGet(t, u, "bytes=0-999", src[:1000])
	mustCacheGet(t, u, "bytes=0-499", src[:500]) // covered: from memory
	if n := len(o.requests()); n != 2 {
		t.Fatalf("upstream requests = %d, want 2", n)
	}
	mustCacheGet(t, u, "bytes=0-", src) // 1000 bytes can't be sniffed as the miss path would
	if reqs := o.requests(); len(reqs) != 3 || reqs[2] != "bytes=0-" {
		t.Errorf("upstream requests = %q, want the open read to go upstream whole", reqs)
	}
}

func TestRelayCacheTracksOnlyRegisteredTokens(t *testing.T) {
	var c relayCache
	c.setBytes(1 << 20)
	if _, _, record := c.lookup("x", 0); record || len(c.sources) != 0 {
		t.Fatalf("lookup of an unknown token created a source")
	}
	c.addToken("x")
	c.dropToken("x")
	c.lookup("x", 0)
	c.lookup("x", 0)
	if len(c.sources) != 0 {
		t.Errorf("lookup after dropToken left %d sources, want 0", len(c.sources))
	}
}

func TestRelayCacheReservesInFlightRecordings(t *testing.T) {
	var c relayCache
	c.setBytes(100) // ranges of at most 25 bytes
	c.addToken("a")
	meta := relayCacheMeta{total: 1000, header: http.Header{}}
	var recs []*relayRecorder
	for range 4 {
		recs = append(recs, c.newRecorder("a", meta, 0, 1000, nil))
	}
	if slices.Contains(recs, nil) || c.newRecorder("a", meta, 0, 1000, nil) != nil {
		t.Fatalf("recorders: want 4 to fit a budget of 100 and a 5th refused")
	}
	recs[0].finish(false)
	if c.newRecorder("a", meta, 0, 1000, nil) == nil {
		t.Error("no recorder after one released its reservation")
	}
}

// Concurrent reads with evictions keep the bookkeeping consistent (run with
// -race).
func TestRelayCacheConcurrentReads(t *testing.T) {
	src := mediaBody(200 << 10)
	o := newRangeOrigin(t, src)
	r, u := cachedRelay(t, o, 4*20000)
	offsets := []int{0, 1000, 50000, 120000, 150000}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 15 {
				off := offsets[(g+i)%len(offsets)]
				rng := fmt.Sprintf("bytes=%d-%d", off, off+29999)
				resp, body, err := cacheGet(t, u, rng)
				if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, src[off:off+30000]) {
					t.Errorf("GET %s: %v, status %d, %d bytes", rng, err, resp.StatusCode, len(body))
					return
				}
			}
		})
	}
	wg.Wait()
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	var sum int64
	for _, s := range r.cache.sources {
		for _, sp := range s.spans {
			sum += int64(len(sp.data))
		}
	}
	if sum != r.cache.used || r.cache.used > 4*20000 || r.cache.inflight != 0 || r.cache.lru.Len() == 0 {
		t.Errorf("used %d, spans sum %d, reserved %d, %d ranges; want used = sum <= budget, none reserved",
			r.cache.used, sum, r.cache.inflight, r.cache.lru.Len())
	}
}
