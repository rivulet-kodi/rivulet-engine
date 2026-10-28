// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ===== fake NNTP server ======================================================

// fakeNNTP is an in-process NNTP server (loopback listener) that serves
// pre-encoded yEnc articles. It records concurrency and request order and can
// delay, gate or drop BODY requests, so tests can prove parallelism and
// progressive availability without timing assumptions.
type fakeNNTP struct {
	t  testing.TB
	ln net.Listener
	wg sync.WaitGroup

	mu       sync.Mutex
	articles map[string][]byte
	latency  time.Duration    // per-BODY delay
	hook     func(id string)  // runs before the article is sent; may block
	dropOnce map[string]bool  // close the connection on the first BODY of these ids
	limit    int              // refuse connections beyond this many open (0 = unlimited)
	open     int              // currently open connections
	maxOpen  int              // peak open connections
	inflight int              // BODY requests currently being served
	maxInfl  int              // peak concurrent BODY requests
	order    []string         // BODY requests in arrival order
	live     map[net.Conn]int // open connections
}

func newFakeNNTP(t testing.TB, articles map[string][]byte) *fakeNNTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNNTP{t: t, ln: ln, articles: articles, live: map[net.Conn]int{}}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.serve(c)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		f.mu.Lock()
		for c := range f.live {
			_ = c.Close()
		}
		f.mu.Unlock()
		f.wg.Wait()
	})
	return f
}

// The fields below are read by the serving goroutines, so tests change them
// through these setters (the listener is already accepting).
func (f *fakeNNTP) setHook(fn func(id string)) {
	f.mu.Lock()
	f.hook = fn
	f.mu.Unlock()
}

func (f *fakeNNTP) setLatency(d time.Duration) {
	f.mu.Lock()
	f.latency = d
	f.mu.Unlock()
}

func (f *fakeNNTP) setLimit(n int) {
	f.mu.Lock()
	f.limit = n
	f.mu.Unlock()
}

func (f *fakeNNTP) setDropOnce(ids ...string) {
	f.mu.Lock()
	f.dropOnce = map[string]bool{}
	for _, id := range ids {
		f.dropOnce[id] = true
	}
	f.mu.Unlock()
}

func (f *fakeNNTP) removeArticle(id string) {
	f.mu.Lock()
	delete(f.articles, id)
	f.mu.Unlock()
}

func (f *fakeNNTP) cfg(connections int) ServerConfig {
	return ServerConfig{
		Host:        "127.0.0.1",
		Port:        f.ln.Addr().(*net.TCPAddr).Port,
		Connections: connections,
	}
}

func (f *fakeNNTP) serve(c net.Conn) {
	defer c.Close()
	f.mu.Lock()
	f.open++
	f.maxOpen = max(f.maxOpen, f.open)
	refuse := f.limit > 0 && f.open > f.limit
	f.live[c] = 0
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.open--
		delete(f.live, c)
		f.mu.Unlock()
	}()

	tp := textproto.NewConn(c)
	if refuse {
		_ = tp.PrintfLine("502 too many connections")
		return
	}
	_ = tp.PrintfLine("200 ready")
	for {
		line, err := tp.ReadLine()
		if err != nil {
			return
		}
		switch {
		case strings.HasPrefix(line, "AUTHINFO USER"):
			_ = tp.PrintfLine("381 password required")
		case strings.HasPrefix(line, "AUTHINFO PASS"):
			_ = tp.PrintfLine("281 ok")
		case strings.HasPrefix(line, "QUIT"):
			_ = tp.PrintfLine("205 bye")
			return
		case strings.HasPrefix(line, "BODY "):
			id := strings.Trim(strings.TrimPrefix(line, "BODY "), "<>")
			f.mu.Lock()
			f.order = append(f.order, id)
			f.inflight++
			f.maxInfl = max(f.maxInfl, f.inflight)
			art, ok := f.articles[id]
			hook, lat := f.hook, f.latency
			drop := f.dropOnce[id]
			if drop {
				delete(f.dropOnce, id)
			}
			f.mu.Unlock()

			if hook != nil {
				hook(id)
			}
			if lat > 0 {
				time.Sleep(lat)
			}
			switch {
			case drop:
				f.done()
				return // connection closed with no response
			case !ok:
				_ = tp.PrintfLine("430 no such article")
			default:
				_ = tp.PrintfLine("222 0 <%s>", id)
				dw := tp.DotWriter()
				_, _ = dw.Write(art)
				_ = dw.Close()
			}
			f.done()
		default:
			_ = tp.PrintfLine("500 unknown command")
		}
	}
}

func (f *fakeNNTP) done() {
	f.mu.Lock()
	f.inflight--
	f.mu.Unlock()
}

func (f *fakeNNTP) stats() (maxOpen, maxInflight int, order []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxOpen, f.maxInfl, append([]string(nil), f.order...)
}

func (f *fakeNNTP) openConns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open
}

// ===== fixtures ==============================================================

// testFile is a synthetic multi-part file: its decoded content, the NZB File
// describing it and the articles a fakeNNTP serves for it.
type testFile struct {
	content  []byte
	file     File
	articles map[string][]byte
	segSize  int
}

// makeTestFile builds a file of nseg parts of segSize bytes (the last part is
// shorter by lastShort bytes). Segment.Bytes is the encoded article size, as
// in a real NZB, so Size (their sum) exceeds the decoded size.
func makeTestFile(name string, nseg, segSize, lastShort int, seed int64) *testFile {
	total := nseg*segSize - lastShort
	content := pseudoRandom(total, seed)
	tf := &testFile{content: content, articles: map[string][]byte{}, segSize: segSize}
	tf.file = File{Name: name, Subject: name}
	for i := range nseg {
		lo := i * segSize
		hi := min(lo+segSize, total)
		art := encodeYencPart(content[lo:hi], int64(total), int64(lo+1), i+1, nseg)
		id := fmt.Sprintf("%s-%d@test", name, i+1)
		tf.articles[id] = art
		tf.file.Segments = append(tf.file.Segments, Segment{MessageID: id, Bytes: int64(len(art)), Number: i + 1})
		tf.file.Size += int64(len(art))
	}
	return tf
}

func (tf *testFile) segID(i int) string { return tf.file.Segments[i].MessageID }

// tempDst creates a destination file under t.TempDir().
func tempDst(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "out.bin"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func waitDone(t *testing.T, a *Assembly) {
	t.Helper()
	select {
	case <-a.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("assembly did not finish")
	}
}

func readAll(t *testing.T, f *os.File) []byte {
	t.Helper()
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func shortCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// ===== tests =================================================================

func TestWorkerCount(t *testing.T) {
	cases := []struct{ configured, segments, want int }{
		{0, 100, defaultConnections},
		{-3, 100, defaultConnections},
		{1, 100, 1},
		{8, 100, 8},
		{500, 100, maxConnections},
		{8, 3, 3},
		{0, 1, 1},
		{8, 0, 1},
	}
	for _, c := range cases {
		if got := workerCount(c.configured, c.segments); got != c.want {
			t.Errorf("workerCount(%d, %d) = %d, want %d", c.configured, c.segments, got, c.want)
		}
	}
}

func TestAssembly_ParallelRoundTrip(t *testing.T) {
	tf := makeTestFile("movie.mkv", 24, 20_000, 7_777, 11)
	srv := newFakeNNTP(t, tf.articles)
	sess := NewSession(srv.cfg(4), []File{tf.file})
	dst := tempDst(t)

	a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}
	size, err := a.Size(shortCtx(t, 10*time.Second))
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != int64(len(tf.content)) {
		t.Fatalf("Size = %d, want %d (decoded size, not the NZB's encoded byte sum %d)", size, len(tf.content), tf.file.Size)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatalf("Err: %v", err)
	}
	if !a.Complete() {
		t.Fatal("assembly not complete")
	}
	if !bytes.Equal(readAll(t, dst), tf.content) {
		t.Fatal("assembled content differs from source")
	}
	if err := a.WaitRange(shortCtx(t, time.Second), 0, size); err != nil {
		t.Fatalf("WaitRange over a complete file: %v", err)
	}
	if err := a.WaitRange(shortCtx(t, time.Second), size-10, 11); err == nil {
		t.Fatal("WaitRange past EOF must fail")
	}
	// Workers released their connections (QUIT) once finished.
	deadline := time.Now().Add(5 * time.Second)
	for srv.openConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := srv.openConns(); n != 0 {
		t.Errorf("%d NNTP connections still open after completion", n)
	}
	if peak, _, _ := srv.stats(); peak > 4 {
		t.Errorf("opened %d connections, configured 4", peak)
	}
}

// TestAssembly_UsesConfiguredConnections proves the downloads really overlap
// without a timing assumption: the first K BODY requests (after segment 0,
// which is fetched alone to fix the layout) are held at a barrier until K of
// them are in flight at once, which a serial fetcher could never satisfy.
func TestAssembly_UsesConfiguredConnections(t *testing.T) {
	const conns = 5
	tf := makeTestFile("movie.mkv", 20, 5_000, 0, 12)
	srv := newFakeNNTP(t, tf.articles)

	var (
		bmu     sync.Mutex
		arrived int
		all     = make(chan struct{})
	)
	srv.setHook(func(id string) {
		if id == tf.segID(0) {
			return // fetched alone first: its header fixes the layout
		}
		bmu.Lock()
		arrived++
		n := arrived
		if n == conns {
			close(all)
		}
		bmu.Unlock()
		if n <= conns {
			select {
			case <-all:
			case <-time.After(5 * time.Second): // fail via the assertions below
			}
		}
	})

	sess := NewSession(srv.cfg(conns), []File{tf.file})
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	peak, inflight, _ := srv.stats()
	if inflight < conns {
		t.Errorf("peak concurrent BODY = %d, want %d", inflight, conns)
	}
	if peak > conns {
		t.Errorf("peak connections = %d, configured %d", peak, conns)
	}
}

func TestAssembly_ProgressiveRangeBeforeCompletion(t *testing.T) {
	tf := makeTestFile("movie.mkv", 12, 10_000, 0, 13)
	srv := newFakeNNTP(t, tf.articles)

	// Hold every article from segment 4 on until released.
	gate := make(chan struct{})
	gated := map[string]bool{}
	for i := 4; i < 12; i++ {
		gated[tf.segID(i)] = true
	}
	srv.setHook(func(id string) {
		if gated[id] {
			<-gate
		}
	})
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
		}
	}
	defer release()

	sess := NewSession(srv.cfg(2), []File{tf.file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}

	// The head of the file is answerable while the tail is still blocked.
	headLen := int64(4 * tf.segSize)
	if err := a.WaitRange(shortCtx(t, 10*time.Second), 0, headLen); err != nil {
		t.Fatalf("WaitRange(head): %v", err)
	}
	if a.Complete() {
		t.Fatal("assembly completed although the tail is gated")
	}
	// WaitAvailable hands back whatever is present right now (the four
	// finished segments) instead of waiting for the requested 1 MiB.
	avail, err := a.WaitAvailable(shortCtx(t, 10*time.Second), 0, 1<<20)
	if err != nil {
		t.Fatalf("WaitAvailable: %v", err)
	}
	if avail < headLen || avail >= int64(len(tf.content)) {
		t.Fatalf("WaitAvailable = %d, want a partial run >= %d and < %d", avail, headLen, len(tf.content))
	}
	got := make([]byte, headLen)
	if _, err := dst.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, tf.content[:headLen]) {
		t.Fatal("head bytes served before completion are wrong")
	}

	// A waiter that gives up must not stop the shared download.
	err = a.WaitRange(shortCtx(t, 50*time.Millisecond), headLen, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitRange on gated tail = %v, want deadline exceeded", err)
	}
	if err := a.Err(); err != nil {
		t.Fatalf("a waiter's context expiring failed the assembly: %v", err)
	}

	release()
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, dst), tf.content) {
		t.Fatal("final content differs")
	}
}

// TestAssembly_SeekPrioritisesRequestedRange: with one connection the order of
// fetches is fully determined, so a waiter for the end of the file must make
// the downloader jump there instead of finishing the sweep first.
func TestAssembly_SeekPrioritisesRequestedRange(t *testing.T) {
	const nseg = 30
	tf := makeTestFile("movie.mkv", nseg, 4_000, 0, 14)
	srv := newFakeNNTP(t, tf.articles)
	srv.setLatency(5 * time.Millisecond)

	sess := NewSession(srv.cfg(1), []File{tf.file})
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	size, err := a.Size(shortCtx(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	tail := size - 100
	if err := a.WaitRange(shortCtx(t, 10*time.Second), tail, 100); err != nil {
		t.Fatal(err)
	}
	_, _, order := srv.stats()
	pos := -1
	for i, id := range order {
		if id == tf.segID(nseg-1) {
			pos = i
		}
	}
	if pos < 0 || pos > 3 {
		t.Fatalf("last segment fetched at position %d of %v; want within the first few (seek hint)", pos, order)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	if !a.Complete() {
		t.Fatal("not complete")
	}
}

func TestAssembly_MissingArticleFailsButKeepsFinishedRanges(t *testing.T) {
	tf := makeTestFile("movie.mkv", 10, 8_000, 0, 15)
	srv := newFakeNNTP(t, tf.articles)
	srv.removeArticle(tf.segID(6)) // 430 for segment 6

	// One connection keeps the order deterministic: 0..5 land before 6 fails.
	sess := NewSession(srv.cfg(1), []File{tf.file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if a.Err() == nil || !strings.Contains(a.Err().Error(), tf.segID(6)) {
		t.Fatalf("Err = %v, want failure naming %s", a.Err(), tf.segID(6))
	}
	if a.Complete() {
		t.Fatal("must not be complete")
	}
	// The six segments that made it are still readable...
	if err := a.WaitRange(shortCtx(t, time.Second), 0, 6*8_000); err != nil {
		t.Fatalf("finished range must stay readable: %v", err)
	}
	// ...the rest fails fast with the assembly error instead of hanging.
	if err := a.WaitRange(shortCtx(t, 5*time.Second), 6*8_000, 10); err == nil {
		t.Fatal("range beyond the failure must error")
	}
	if _, err := a.Size(shortCtx(t, time.Second)); err != nil {
		t.Fatalf("Size stays known after a mid-file failure: %v", err)
	}
}

func TestAssembly_CancelStopsWorkersAndReleasesConnections(t *testing.T) {
	tf := makeTestFile("movie.mkv", 16, 6_000, 0, 16)
	srv := newFakeNNTP(t, tf.articles)
	gate := make(chan struct{})
	defer close(gate)
	srv.setHook(func(id string) {
		if id != tf.segID(0) {
			<-gate
		}
	})
	sess := NewSession(srv.cfg(3), []File{tf.file})
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitRange(shortCtx(t, 10*time.Second), 0, 100); err != nil {
		t.Fatal(err)
	}
	a.Cancel()
	waitDone(t, a)
	if !errors.Is(a.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", a.Err())
	}
	if err := a.WaitRange(shortCtx(t, time.Second), 100_000-1, 1); err == nil {
		t.Fatal("WaitRange on unfetched data after Cancel must error")
	}
	if err := a.WaitRange(shortCtx(t, time.Second), 0, 100); err != nil {
		t.Fatalf("already-written bytes stay readable after Cancel: %v", err)
	}
}

func TestAssembly_ParentContextCancel(t *testing.T) {
	tf := makeTestFile("movie.mkv", 8, 5_000, 0, 18)
	srv := newFakeNNTP(t, tf.articles)
	gate := make(chan struct{})
	defer close(gate)
	srv.setHook(func(id string) {
		if id != tf.segID(0) {
			<-gate
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	sess := NewSession(srv.cfg(2), []File{tf.file})
	a, err := sess.StartAssembly(ctx, "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitRange(shortCtx(t, 10*time.Second), 0, 10); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitDone(t, a)
	if !errors.Is(a.Err(), context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", a.Err())
	}
}

func TestAssembly_ToleratesRefusedExtraConnections(t *testing.T) {
	tf := makeTestFile("movie.mkv", 10, 5_000, 123, 19)
	srv := newFakeNNTP(t, tf.articles)
	srv.setLimit(1) // the provider allows a single connection
	sess := NewSession(srv.cfg(8), []File{tf.file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatalf("refused extra connections must only cost parallelism, got: %v", err)
	}
	if !bytes.Equal(readAll(t, dst), tf.content) {
		t.Fatal("content differs")
	}
}

func TestAssembly_AllDialsFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close() // nothing listens any more
	tf := makeTestFile("movie.mkv", 4, 1_000, 0, 20)
	sess := NewSession(ServerConfig{Host: "127.0.0.1", Port: port, Connections: 3}, []File{tf.file})
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Size(shortCtx(t, 10*time.Second)); err == nil || !strings.Contains(err.Error(), "dial") {
		t.Fatalf("Size error = %v, want the dial failure", err)
	}
	waitDone(t, a)
	if a.Err() == nil {
		t.Fatal("expected failure")
	}
}

func TestAssembly_ReconnectsAfterDroppedConnection(t *testing.T) {
	tf := makeTestFile("movie.mkv", 6, 5_000, 0, 21)
	srv := newFakeNNTP(t, tf.articles)
	srv.setDropOnce(tf.segID(2))
	sess := NewSession(srv.cfg(1), []File{tf.file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatalf("a dropped connection must be re-dialled transparently: %v", err)
	}
	if !bytes.Equal(readAll(t, dst), tf.content) {
		t.Fatal("content differs after reconnect")
	}
}

// Articles without =ypart (not self-describing) cannot be placed
// independently: the assembly falls back to ordered appends by one worker and
// reports the size only at the end.
func TestAssembly_SequentialFallbackWithoutYpart(t *testing.T) {
	content := pseudoRandom(30_000, 22)
	articles := map[string][]byte{}
	file := File{Name: "legacy.bin"}
	for i := range 3 {
		part := content[i*10_000 : (i+1)*10_000]
		// buildYenc emits a plain =ybegin/=yend article with no =ypart.
		id := fmt.Sprintf("legacy-%d@test", i+1)
		articles[id] = buildYenc(part)
		file.Segments = append(file.Segments, Segment{MessageID: id, Bytes: int64(len(articles[id])), Number: i + 1})
		file.Size += int64(len(articles[id]))
	}
	srv := newFakeNNTP(t, articles)
	sess := NewSession(srv.cfg(4), []File{file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "legacy.bin", dst)
	if err != nil {
		t.Fatal(err)
	}
	size, err := a.Size(shortCtx(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(len(content)) {
		t.Fatalf("Size = %d, want %d", size, len(content))
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, dst), content) {
		t.Fatal("sequential fallback assembled wrong content")
	}
}

func TestSelfDescribing(t *testing.T) {
	cases := []struct {
		name string
		h    yencHeader
		n    int
		want bool
	}{
		{"multipart, first part starts the file", yencHeader{Size: 900, HasPart: true, Begin: 1, End: 300}, 3, true},
		{"multipart without =ypart", yencHeader{Size: 900}, 3, false},
		{"size= is only the part size", yencHeader{Size: 300, HasPart: true, Begin: 1, End: 300}, 3, false},
		{"first segment is not the file start", yencHeader{Size: 900, HasPart: true, Begin: 301, End: 600}, 3, false},
		{"no size", yencHeader{HasPart: true, Begin: 1, End: 300}, 3, false},
		{"single article, plain", yencHeader{Size: 500}, 1, true},
		{"single article, whole-file part", yencHeader{Size: 500, HasPart: true, Begin: 1, End: 500}, 1, true},
		{"single article, partial part", yencHeader{Size: 500, HasPart: true, Begin: 1, End: 200}, 1, false},
	}
	for _, c := range cases {
		if got := selfDescribing(c.h, c.n); got != c.want {
			t.Errorf("%s: selfDescribing = %v, want %v", c.name, got, c.want)
		}
	}
}

// A poster whose =ybegin size= is just the part size (not the file size) must
// not be placed by its =ypart ranges: the assembly appends in order instead,
// as a sequential assembly does.
func TestAssembly_UntrustedPartHeadersFallBackToSequential(t *testing.T) {
	content := pseudoRandom(30_000, 29)
	articles := map[string][]byte{}
	file := File{Name: "odd.bin"}
	for i := range 3 {
		part := content[i*10_000 : (i+1)*10_000]
		id := fmt.Sprintf("odd-%d@test", i+1)
		articles[id] = encodeYencPart(part, int64(len(part)), int64(i*10_000+1), i+1, 3) // size= is the part size
		file.Segments = append(file.Segments, Segment{MessageID: id, Bytes: int64(len(articles[id])), Number: i + 1})
		file.Size += int64(len(articles[id]))
	}
	srv := newFakeNNTP(t, articles)
	sess := NewSession(srv.cfg(4), []File{file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "odd.bin", dst)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatalf("untrusted headers must degrade gracefully, got: %v", err)
	}
	if !bytes.Equal(readAll(t, dst), content) {
		t.Fatal("sequential fallback assembled wrong content")
	}
}

func TestAssembly_SingleSegmentFile(t *testing.T) {
	tf := makeTestFile("small.nfo", 1, 3_000, 500, 23)
	srv := newFakeNNTP(t, tf.articles)
	sess := NewSession(srv.cfg(4), []File{tf.file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "small.nfo", dst)
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAll(t, dst), tf.content) {
		t.Fatal("content differs")
	}
	if peak, _, _ := srv.stats(); peak != 1 {
		t.Errorf("single-segment file opened %d connections, want 1", peak)
	}
}

func TestAssembly_EmptyAndMissingFile(t *testing.T) {
	sess := NewSession(ServerConfig{Host: "127.0.0.1"}, []File{{Name: "empty.bin"}})
	a, err := sess.StartAssembly(context.Background(), "empty.bin", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	size, err := a.Size(shortCtx(t, time.Second))
	if err != nil || size != 0 || !a.Complete() {
		t.Fatalf("empty file: size=%d err=%v complete=%v", size, err, a.Complete())
	}
	<-a.Done()

	if _, err := sess.StartAssembly(context.Background(), "nope.bin", tempDst(t)); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing file error = %v", err)
	}
}

// A file whose declared size exceeds what the NZB's segment sizes allow is
// refused up front (disk-exhaustion cap), not written.
func TestAssembly_SizeCapEnforced(t *testing.T) {
	tf := makeTestFile("movie.mkv", 4, 5_000, 0, 24)
	tf.file.Size = 1_000 // NZB claims a much smaller file than the articles describe
	srv := newFakeNNTP(t, tf.articles)
	sess := NewSession(srv.cfg(2), []File{tf.file})
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if a.Err() == nil || !strings.Contains(a.Err().Error(), "exceeds declared size") {
		t.Fatalf("Err = %v, want size-cap failure", a.Err())
	}
}

// Segments omitted from the NZB leave a hole: the assembly must not claim
// success, while ranges before the hole stay readable.
func TestAssembly_DetectsIncompleteCoverage(t *testing.T) {
	tf := makeTestFile("movie.mkv", 6, 4_000, 0, 25)
	srv := newFakeNNTP(t, tf.articles)
	file := tf.file
	file.Segments = append(append([]Segment(nil), file.Segments[:3]...), file.Segments[4:]...)
	sess := NewSession(srv.cfg(2), []File{file})
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, a)
	if a.Err() == nil || !strings.Contains(a.Err().Error(), "missing") {
		t.Fatalf("Err = %v, want incomplete-coverage failure", a.Err())
	}
	if a.Complete() {
		t.Fatal("must not report complete")
	}
	if err := a.WaitRange(shortCtx(t, time.Second), 0, 3*4_000); err != nil {
		t.Fatalf("segments before the hole must stay readable: %v", err)
	}
	if err := a.WaitRange(shortCtx(t, time.Second), 3*4_000, 10); err == nil {
		t.Fatal("the hole must not be readable")
	}
}

func TestAssembly_ConcurrentWaitersAllServed(t *testing.T) {
	tf := makeTestFile("movie.mkv", 24, 6_000, 99, 26)
	srv := newFakeNNTP(t, tf.articles)
	srv.setLatency(time.Millisecond)
	sess := NewSession(srv.cfg(4), []File{tf.file})
	dst := tempDst(t)
	a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}
	size, err := a.Size(shortCtx(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for w := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			off := int64(w) * size / 16
			n := min(int64(5_000), size-off)
			if err := a.WaitRange(shortCtx(t, 15*time.Second), off, n); err != nil {
				errs <- fmt.Errorf("waiter %d: %w", w, err)
				return
			}
			buf := make([]byte, n)
			if _, err := dst.ReadAt(buf, off); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(buf, tf.content[off:off+n]) {
				errs <- fmt.Errorf("waiter %d: wrong bytes at %d", w, off)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	waitDone(t, a)
}

// TestAssembly_ParallelBeatsSerialLatency measures total assembly time with a
// per-article latency. The serial figure has a hard lower bound (n × latency,
// each BODY sleeps that long on the single connection) while the parallel one
// is ~n/conns × latency, so the comparison needs no tuning.
func TestAssembly_ParallelBeatsSerialLatency(t *testing.T) {
	const (
		nseg    = 24
		latency = 15 * time.Millisecond
		conns   = 8
	)
	tf := makeTestFile("movie.mkv", nseg, 4_000, 0, 27)
	srv := newFakeNNTP(t, tf.articles)
	srv.setLatency(latency)

	// One connection fetches the articles one after another: the serial baseline.
	sess := NewSession(srv.cfg(1), []File{tf.file})
	t0 := time.Now()
	sa, err := sess.StartAssembly(context.Background(), "movie.mkv", tempDst(t))
	if err != nil {
		t.Fatal(err)
	}
	waitDone(t, sa)
	if err := sa.Err(); err != nil {
		t.Fatal(err)
	}
	serial := time.Since(t0)

	psess := NewSession(srv.cfg(conns), []File{tf.file})
	dst := tempDst(t)
	t0 = time.Now()
	a, err := psess.StartAssembly(context.Background(), "movie.mkv", dst)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.WaitRange(shortCtx(t, 10*time.Second), 0, 1); err != nil {
		t.Fatal(err)
	}
	ttfb := time.Since(t0)
	waitDone(t, a)
	parallel := time.Since(t0)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("serial total %v | parallel(%d conns): first byte %v, total %v", serial, conns, ttfb, parallel)

	if floor := nseg * latency; serial < floor {
		t.Fatalf("serial = %v, below its %v lower bound — fake latency not applied", serial, floor)
	}
	if parallel > serial*3/4 {
		t.Errorf("parallel assembly took %v, not clearly faster than serial %v", parallel, serial)
	}
	if ttfb > serial/4 {
		t.Errorf("first byte after %v; serial total was %v — serving is not progressive", ttfb, serial)
	}
}

// BenchmarkAssemble compares Assemblies of increasing connection counts (1 = serial)
// under a per-article latency, reporting time-to-first-byte and total.
func BenchmarkAssemble(b *testing.B) {
	const (
		nseg    = 32
		latency = 3 * time.Millisecond
	)
	tf := makeTestFile("movie.mkv", nseg, 32*1024, 0, 28)
	srv := newFakeNNTP(b, tf.articles)
	srv.setLatency(latency)

	for _, conns := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("parallel-%d", conns), func(b *testing.B) {
			b.ReportAllocs()
			var ttfb time.Duration
			var iters int
			for b.Loop() {
				sess := NewSession(srv.cfg(conns), []File{tf.file})
				dst, err := os.CreateTemp(b.TempDir(), "asm")
				if err != nil {
					b.Fatal(err)
				}
				t0 := time.Now()
				a, err := sess.StartAssembly(context.Background(), "movie.mkv", dst)
				if err != nil {
					b.Fatal(err)
				}
				if err := a.WaitRange(context.Background(), 0, 1); err != nil {
					b.Fatal(err)
				}
				ttfb += time.Since(t0)
				iters++
				<-a.Done()
				if err := a.Err(); err != nil {
					b.Fatal(err)
				}
				_ = dst.Close()
			}
			b.ReportMetric(float64(ttfb.Microseconds())/float64(iters), "ttfb-µs")
		})
	}
}
