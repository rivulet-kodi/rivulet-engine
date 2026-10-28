// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/archive"
)

// ── fixtures & helpers ───────────────────────────────────────────────────────

func progNoise(n int, seed uint64) []byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.UintN(256))
	}
	return b
}

type progEntry struct {
	name   string
	method uint16
	data   []byte
}

// progZip writes a zip with the given entries and returns its path.
func progZip(t *testing.T, entries ...progEntry) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "prog.zip")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for _, e := range entries {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: e.name, Method: e.method})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

func progTar(t *testing.T, entries ...progEntry) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "prog.tar")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	tw := tar.NewWriter(f)
	for _, e := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: e.name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(e.data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

// progCreate registers a session for archivePath through the real create
// handler (so the per-session listing is primed exactly as in production) and
// tears it down with the test.
func progCreate(t *testing.T, h http.Handler, archivePath, ext string) (string, *archiveSession) {
	t.Helper()
	return progCreateWith(t, h, archivePath, ext, nil)
}

// progCreateWith is progCreate with extra members in the create payload. A
// create whose payload differs from the live session's replaces it; an
// identical one reuses it.
func progCreateWith(t *testing.T, h http.Handler, archivePath, ext string, extra map[string]any) (string, *archiveSession) {
	t.Helper()
	t.Setenv("RIVULET_ARCHIVE_LOCAL_ROOT", filepath.Dir(archivePath))
	key := strings.ReplaceAll(strings.ToLower(t.Name()), "/", "-")
	payload := map[string]any{"url": archivePath}
	for k, v := range extra {
		payload[k] = v
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+ext+"/create/"+key, bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: status %d: %s", rec.Code, rec.Body.String())
	}
	archiveSessionsMu.Lock()
	sess := archiveSessions[key]
	archiveSessionsMu.Unlock()
	if sess == nil {
		t.Fatal("create did not register a session")
	}
	t.Cleanup(func() {
		archiveSessionsMu.Lock()
		if archiveSessions[key] == sess {
			delete(archiveSessions, key)
		}
		archiveSessionsMu.Unlock()
		archiveDestroySession(sess)
	})
	return key, sess
}

func progGet(h http.Handler, method, ext, key, name, rng string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/"+ext+"/stream/"+key+"/"+archiveEncodePath(name), nil)
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// progGate blocks the extraction goroutine (after at least limit bytes became
// readable) until Release, making "the response arrived while the extraction
// was still running" a deterministic fact instead of a timing guess.
type progGate struct {
	ch   chan struct{}
	once sync.Once
}

func (g *progGate) Release() { g.once.Do(func() { close(g.ch) }) }

// progInstallHooks installs the extraction hooks for the test and restores them
// last (after in-flight extractions are done, so no goroutine races the reset).
func progInstallHooks(t *testing.T, sess *archiveSession, limit int64) (*progGate, *atomic.Int32) {
	t.Helper()
	g := &progGate{ch: make(chan struct{})}
	var extractions atomic.Int32
	t.Cleanup(func() { archiveExtractTestHook, archiveExtractChunkHook = nil, nil })
	t.Cleanup(func() {
		g.Release()
		progWaitFlights(t, sess)
	})
	archiveExtractTestHook = func(string) { extractions.Add(1) }
	if limit >= 0 {
		archiveExtractChunkHook = func(_ string, written int64) {
			if written >= limit {
				<-g.ch
			}
		}
	}
	return g, &extractions
}

func progWaitFlights(t *testing.T, sess *archiveSession) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		sess.mu.Lock()
		n := len(sess.inflight)
		sess.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d extraction(s) still in flight", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func progInflight(sess *archiveSession) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.inflight)
}

// ── progressive serving ──────────────────────────────────────────────────────

// A ranged request for bytes that are already on disk must be answered while
// the extraction is still running; a request for bytes beyond the extraction
// frontier must wait for them (and only them).
func TestArchiveStream_ProgressiveRangeBeforeExtractionFinishes(t *testing.T) {
	content := progNoise(3<<20, 1)
	h := newHandler(t)
	key, sess := progCreate(t, h, progZip(t, progEntry{"movie.mkv", zip.Deflate, content}), "zip")
	gate, extractions := progInstallHooks(t, sess, 1<<20)

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- progGet(h, http.MethodGet, "zip", key, "movie.mkv", "bytes=100-199") }()
	var rec *httptest.ResponseRecorder
	select {
	case rec = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ranged request for already-extracted bytes blocked until the whole entry was extracted")
	}
	if rec.Code != http.StatusPartialContent || !bytes.Equal(rec.Body.Bytes(), content[100:200]) {
		t.Fatalf("early range: status %d, body ok=%v", rec.Code, bytes.Equal(rec.Body.Bytes(), content[100:200]))
	}
	if got, want := rec.Header().Get("Content-Range"), fmt.Sprintf("bytes 100-199/%d", len(content)); got != want {
		t.Errorf("Content-Range = %q, want %q", got, want)
	}
	if progInflight(sess) != 1 {
		t.Fatal("extraction should still be running while the gate is closed")
	}

	// Bytes past the frontier: must block until the extraction gets there.
	const from = 2<<20 + 17
	late := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		late <- progGet(h, http.MethodGet, "zip", key, "movie.mkv", fmt.Sprintf("bytes=%d-%d", from, from+99))
	}()
	select {
	case <-late:
		t.Fatal("range beyond the extraction frontier was answered before those bytes existed")
	case <-time.After(100 * time.Millisecond):
	}
	gate.Release()
	select {
	case rec = <-late:
	case <-time.After(10 * time.Second):
		t.Fatal("late range never completed after the extraction was released")
	}
	if !bytes.Equal(rec.Body.Bytes(), content[from:from+100]) {
		t.Error("late range body mismatch")
	}

	progWaitFlights(t, sess)
	full := progGet(h, http.MethodGet, "zip", key, "movie.mkv", "")
	if full.Code != http.StatusOK || !bytes.Equal(full.Body.Bytes(), content) {
		t.Fatalf("full GET after extraction: status %d, %d bytes (want %d)", full.Code, full.Body.Len(), len(content))
	}
	if n := extractions.Load(); n != 1 {
		t.Errorf("extractions = %d, want exactly 1 shared by every request", n)
	}
	sess.mu.Lock()
	_, cached := sess.extracted["movie.mkv"]
	sess.mu.Unlock()
	if !cached {
		t.Error("completed extraction was not cached")
	}
}

// A client that goes away mid-stream must neither wedge its handler (the wait
// is cancellable) nor kill the extraction other/later requests share.
func TestArchiveStream_ClientDisconnectKeepsExtraction(t *testing.T) {
	content := progNoise(3<<20, 2)
	h := newHandler(t)
	key, sess := progCreate(t, h, progZip(t, progEntry{"movie.mkv", zip.Deflate, content}), "zip")
	gate, extractions := progInstallHooks(t, sess, 1<<20)

	srv := httptest.NewServer(h)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/zip/stream/"+key+"/movie.mkv", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 4096)
	if _, err := io.ReadFull(resp.Body, first); err != nil || !bytes.Equal(first, content[:4096]) {
		t.Fatalf("first bytes: err=%v match=%v", err, bytes.Equal(first, content[:4096]))
	}
	cancel() // client disconnects while the server waits for bytes past the gate
	_ = resp.Body.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		sess.mu.Lock()
		pinned := sess.refCount
		sess.mu.Unlock()
		if pinned == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handler did not return after the client disconnected (wait is not cancellable)")
		}
		time.Sleep(time.Millisecond)
	}
	if progInflight(sess) != 1 {
		t.Fatal("client disconnect killed the shared extraction")
	}

	gate.Release()
	progWaitFlights(t, sess)
	sess.mu.Lock()
	p, ok := sess.extracted["movie.mkv"]
	sess.mu.Unlock()
	if !ok {
		t.Fatal("extraction did not complete after the client left")
	}
	if got, err := os.ReadFile(p); err != nil || !bytes.Equal(got, content) {
		t.Fatalf("extracted file mismatch (err=%v)", err)
	}
	if rec := progGet(h, http.MethodGet, "zip", key, "movie.mkv", "bytes=-100"); !bytes.Equal(rec.Body.Bytes(), content[len(content)-100:]) {
		t.Error("later request was not served from the finished extraction")
	}
	if n := extractions.Load(); n != 1 {
		t.Errorf("extractions = %d, want 1", n)
	}
}

// ── in-place serving of stored entries ───────────────────────────────────────

func TestArchiveStream_StoredEntriesServedInPlace(t *testing.T) {
	content := progNoise(1<<20+333, 3)
	cases := []struct {
		ext  string
		path func(*testing.T) string
	}{
		{"zip", func(t *testing.T) string {
			return progZip(t, progEntry{"other.txt", zip.Store, []byte("x")}, progEntry{"dir/movie.mkv", zip.Store, content})
		}},
		{"tar", func(t *testing.T) string {
			return progTar(t, progEntry{"other.txt", 0, []byte("x")}, progEntry{"dir/movie.mkv", 0, content})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.ext, func(t *testing.T) {
			h := newHandler(t)
			key, sess := progCreate(t, h, tc.path(t), tc.ext)
			var extractions atomic.Int32
			archiveExtractTestHook = func(string) { extractions.Add(1) }
			t.Cleanup(func() { archiveExtractTestHook = nil })

			const name = "dir/movie.mkv"
			for _, rng := range []string{"", "bytes=0-1023", "bytes=1048576-", "bytes=-500", "bytes=7-7"} {
				rec := progGet(h, http.MethodGet, tc.ext, key, name, rng)
				want := content
				switch rng {
				case "bytes=0-1023":
					want = content[:1024]
				case "bytes=1048576-":
					want = content[1048576:]
				case "bytes=-500":
					want = content[len(content)-500:]
				case "bytes=7-7":
					want = content[7:8]
				}
				wantCode := http.StatusOK
				if rng != "" {
					wantCode = http.StatusPartialContent
				}
				if rec.Code != wantCode || !bytes.Equal(rec.Body.Bytes(), want) {
					t.Errorf("Range %q: status %d (want %d), body match=%v", rng, rec.Code, wantCode, bytes.Equal(rec.Body.Bytes(), want))
				}
			}
			head := progGet(h, http.MethodHead, tc.ext, key, name, "")
			if head.Code != http.StatusOK || head.Header().Get("Content-Length") != fmt.Sprint(len(content)) {
				t.Errorf("HEAD: status %d, Content-Length %q", head.Code, head.Header().Get("Content-Length"))
			}

			if n := extractions.Load(); n != 0 {
				t.Errorf("stored %s entry was extracted %d time(s); it must be served in place", tc.ext, n)
			}
			if ents, _ := os.ReadDir(sess.tmpDir); len(ents) != 0 {
				t.Errorf("tmpDir holds %d file(s); in-place serving must not write temp bytes", len(ents))
			}
			sess.mu.Lock()
			nExtracted := len(sess.extracted)
			sess.mu.Unlock()
			if nExtracted != 0 {
				t.Errorf("extracted cache has %d entries, want 0", nExtracted)
			}
		})
	}
}

// Response headers must be byte-for-byte the same shape whether an entry is
// served in place (stored) or from a progressive extraction (deflated).
func TestArchiveStream_ResponseShapeIdenticalStoredVsExtracted(t *testing.T) {
	content := bytes.Repeat([]byte("0123456789abcdef"), 1<<14) // 256 KiB
	h := newHandler(t)
	keyS, _ := progCreate(t, h, progZip(t, progEntry{"movie.mkv", zip.Store, content}), "zip")
	// progCreate derives the key from the test name; use a sub-test for the second session.
	var keyD string
	t.Run("deflated", func(t *testing.T) {
		keyD, _ = progCreate(t, h, progZip(t, progEntry{"movie.mkv", zip.Deflate, content}), "zip")
		for _, rng := range []string{"", "bytes=100-299", "bytes=-64"} {
			a := progGet(h, http.MethodGet, "zip", keyS, "movie.mkv", rng)
			b := progGet(h, http.MethodGet, "zip", keyD, "movie.mkv", rng)
			if a.Code != b.Code || !bytes.Equal(a.Body.Bytes(), b.Body.Bytes()) {
				t.Errorf("Range %q: status/body differ (%d vs %d)", rng, a.Code, b.Code)
			}
			for _, hk := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "transferMode.dlna.org", "contentFeatures.dlna.org"} {
				if a.Header().Get(hk) != b.Header().Get(hk) {
					t.Errorf("Range %q: header %s differs: %q vs %q", rng, hk, a.Header().Get(hk), b.Header().Get(hk))
				}
			}
		}
	})
}

// ── failure shapes ───────────────────────────────────────────────────────────

func TestArchiveStream_ErrorsBeforeFirstByteAre500(t *testing.T) {
	good := bytes.Repeat([]byte("hello "), 5000)
	p := progZip(t, progEntry{"ok.mkv", zip.Deflate, good}, progEntry{"bad.mkv", zip.Deflate, good})

	// Corrupt bad.mkv's deflate stream header (BTYPE=3 is reserved) so the very
	// first read fails.
	zr, err := zip.OpenReader(p)
	if err != nil {
		t.Fatal(err)
	}
	var off int64
	for _, f := range zr.File {
		if f.Name == "bad.mkv" {
			if off, err = f.DataOffset(); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = zr.Close()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[off] = 0x07
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	h := newHandler(t)
	key, sess := progCreate(t, h, p, "zip")
	// A corrupt entry is a server-side fault (500); a name that is not in the
	// archive is the caller's mistake (404).
	rec := progGet(h, http.MethodGet, "zip", key, "bad.mkv", "")
	if rec.Code != http.StatusInternalServerError || !strings.HasPrefix(rec.Body.String(), "extract: ") {
		t.Errorf("bad.mkv: status %d body %q, want 500 \"extract: …\"", rec.Code, rec.Body.String())
	}
	rec = progGet(h, http.MethodGet, "zip", key, "missing.mkv", "")
	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), `entry "missing.mkv" not found in archive`) {
		t.Errorf("missing.mkv: status %d body %q, want 404 with the entry message", rec.Code, rec.Body.String())
	}
	progWaitFlights(t, sess)
	sess.mu.Lock()
	n := len(sess.extracted)
	sess.mu.Unlock()
	if n != 0 {
		t.Errorf("failed extractions were cached (%d)", n)
	}
	if ents, _ := os.ReadDir(sess.tmpDir); len(ents) != 0 {
		t.Errorf("failed extraction left %d temp file(s)", len(ents))
	}
	// A healthy entry in the same archive is unaffected.
	if rec := progGet(h, http.MethodGet, "zip", key, "ok.mkv", ""); rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), good) {
		t.Errorf("healthy entry: status %d", rec.Code)
	}
}

// Corruption found only after bytes were already served must never look like a
// clean, complete response: the final chunk is withheld until the extraction
// verified (zip CRC), and the broken copy is not cached.
func TestArchiveStream_MidStreamCorruptionNeverLooksComplete(t *testing.T) {
	content := progNoise(1<<20, 4)
	p := progZip(t, progEntry{"movie.mkv", zip.Deflate, content})
	zr, err := zip.OpenReader(p)
	if err != nil {
		t.Fatal(err)
	}
	off, err := zr.File[0].DataOffset()
	if err != nil {
		t.Fatal(err)
	}
	_ = zr.Close()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[off+int64(len(content))/2] ^= 0xff // flip a payload byte → CRC mismatch at EOF
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	h := newHandler(t)
	key, sess := progCreate(t, h, p, "zip")
	rec := progGet(h, http.MethodGet, "zip", key, "movie.mkv", "")
	if rec.Code == http.StatusOK && bytes.Equal(rec.Body.Bytes(), content) {
		t.Fatal("corrupt entry was delivered as a complete 200 response")
	}
	if rec.Code == http.StatusOK && rec.Body.Len() >= len(content) {
		t.Fatalf("corrupt entry: body not truncated (%d of %d bytes)", rec.Body.Len(), len(content))
	}
	progWaitFlights(t, sess)
	sess.mu.Lock()
	n := len(sess.extracted)
	sess.mu.Unlock()
	if n != 0 {
		t.Error("corrupt extraction was cached")
	}
}

func TestArchiveStream_EmptyEntry(t *testing.T) {
	h := newHandler(t)
	key, _ := progCreate(t, h, progZip(t, progEntry{"empty.mkv", zip.Deflate, nil}, progEntry{"x.txt", zip.Deflate, []byte("x")}), "zip")
	rec := progGet(h, http.MethodGet, "zip", key, "empty.mkv", "")
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || rec.Header().Get("Content-Length") != "0" {
		t.Fatalf("empty entry: status %d, %d bytes, Content-Length %q", rec.Code, rec.Body.Len(), rec.Header().Get("Content-Length"))
	}
}

// ── size policy ──────────────────────────────────────────────────────────────

func TestArchiveStream_SizeUnknownWaitsForFullExtraction(t *testing.T) {
	content := progNoise(600<<10, 5)
	h := newHandler(t)
	key, sess := progCreate(t, h, progZip(t, progEntry{"movie.mkv", zip.Deflate, content}), "zip")
	// Pretend the archive did not record the size (streamed RAR).
	sess.listMu.Lock()
	sess.listing["movie.mkv"] = archive.Entry{Name: "movie.mkv", SizeUnknown: true}
	sess.listMu.Unlock()

	for _, rng := range []string{"", "bytes=10-19", "bytes=-5"} {
		rec := progGet(h, http.MethodGet, "zip", key, "movie.mkv", rng)
		want := content
		switch rng {
		case "bytes=10-19":
			want = content[10:20]
		case "bytes=-5":
			want = content[len(content)-5:]
		}
		if !bytes.Equal(rec.Body.Bytes(), want) {
			t.Errorf("Range %q: body mismatch (status %d, %d bytes)", rng, rec.Code, rec.Body.Len())
		}
	}
}

func TestArchiveExtract_DeclaredSizePolicy(t *testing.T) {
	content := progNoise(200<<10, 6)
	h := newHandler(t)
	_, sess := progCreate(t, h, progZip(t, progEntry{"a.bin", zip.Deflate, content}), "zip")

	// Declared size above the hard limit is refused before any I/O.
	sess.listMu.Lock()
	sess.listing["a.bin"] = archive.Entry{Name: "a.bin", Size: archiveMaxEntryBytes + 1}
	sess.listMu.Unlock()
	if _, err := archiveExtractEntry(sess, "a.bin"); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("oversize declared size: err = %v", err)
	}

	// Content longer than declared is a zip-bomb signal: fail, drop the file.
	sess.listMu.Lock()
	sess.listing["a.bin"] = archive.Entry{Name: "a.bin", Size: 1000}
	sess.listMu.Unlock()
	if _, err := archiveExtractEntry(sess, "a.bin"); err == nil || !strings.Contains(err.Error(), "exceeds declared size") {
		t.Fatalf("overlong content: err = %v", err)
	}
	progWaitFlights(t, sess)
	if ents, _ := os.ReadDir(sess.tmpDir); len(ents) != 0 {
		t.Errorf("zip-bomb extraction left %d temp file(s)", len(ents))
	}
}

// ── listing cache ────────────────────────────────────────────────────────────

func TestArchiveSession_ListingPrimedAtCreateAndNeverRelisted(t *testing.T) {
	h := newHandler(t)
	p := progZip(t, progEntry{"a.mkv", zip.Deflate, []byte("aaa")}, progEntry{"b.srt", zip.Deflate, []byte("bbb")})
	_, sess := progCreate(t, h, p, "zip")
	if err := os.Remove(p); err != nil { // a re-list would now fail
		t.Fatal(err)
	}
	for range 3 {
		e, err := sess.entry("b.srt")
		if err != nil || e.Size != 3 {
			t.Fatalf("entry(b.srt) = %+v, %v", e, err)
		}
	}
	if _, err := sess.entry("zzz"); err == nil || !strings.Contains(err.Error(), `entry "zzz" not found in archive`) {
		t.Fatalf("unknown entry error = %v", err)
	}
}

func TestArchiveSession_LazyListingIsSharedAndFailureNotCached(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "later.zip")
	sess := &archiveSession{archivePath: missing, ext: "zip", tmpDir: t.TempDir()}
	if _, err := sess.entry("a.bin"); err == nil {
		t.Fatal("expected error while the archive does not exist")
	}
	src := progZip(t, progEntry{"a.bin", zip.Deflate, []byte("x")})
	if err := os.Rename(src, missing); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.entry("a.bin"); err != nil {
		t.Fatalf("retry after a failed listing: %v", err)
	}
}

// ── global caps / LRU ────────────────────────────────────────────────────────

func isolateArchiveSessions(t *testing.T) {
	t.Helper()
	archiveSessionsMu.Lock()
	saved := archiveSessions
	archiveSessions = map[string]*archiveSession{}
	archiveSessionsMu.Unlock()
	archiveSessionsMu.Lock()
	savedMaxSessions, savedMaxBytes := archiveMaxSessions, archiveMaxTempBytes
	archiveSessionsMu.Unlock()
	t.Cleanup(func() {
		archiveSessionsMu.Lock()
		archiveSessions = saved
		archiveMaxSessions, archiveMaxTempBytes = savedMaxSessions, savedMaxBytes
		archiveSessionsMu.Unlock()
	})
}

// setArchiveCaps changes the caps under archiveSessionsMu, the lock
// archiveEnforceCaps reads them under (a stray async enforcement spawned by an
// earlier test would otherwise race with the write).
func setArchiveCaps(maxSessions int, maxBytes int64) {
	archiveSessionsMu.Lock()
	archiveMaxSessions, archiveMaxTempBytes = maxSessions, maxBytes
	archiveSessionsMu.Unlock()
}

func capSession(t *testing.T, key string, age time.Duration, bytesOnDisk int64) *archiveSession {
	t.Helper()
	s := &archiveSession{
		key:          key,
		tmpDir:       t.TempDir(),
		archiveBytes: bytesOnDisk,
		lastAccess:   time.Now().Add(-age),
	}
	archiveSessionsMu.Lock()
	archiveSessions[key] = s
	archiveSessionsMu.Unlock()
	return s
}

func hasSession(key string) bool {
	archiveSessionsMu.Lock()
	defer archiveSessionsMu.Unlock()
	_, ok := archiveSessions[key]
	return ok
}

func TestArchiveEnforceCaps_SessionCountLRU(t *testing.T) {
	isolateArchiveSessions(t)
	setArchiveCaps(2, archiveMaxTempBytes)
	oldest := capSession(t, "oldest", 3*time.Minute, 0)
	middle := capSession(t, "middle", 2*time.Minute, 0)
	newest := capSession(t, "newest", time.Minute, 0)
	fresh := capSession(t, "fresh", 0, 0)

	archiveEnforceCaps(fresh)

	if hasSession("oldest") || hasSession("middle") {
		t.Error("least recently used sessions must be evicted first")
	}
	if !hasSession("newest") || !hasSession("fresh") {
		t.Error("most recently used sessions must survive")
	}
	for _, s := range []*archiveSession{oldest, middle} {
		if exists(s.tmpDir) {
			t.Errorf("evicted session's tmpDir %s still exists", s.tmpDir)
		}
	}
	if !exists(newest.tmpDir) || !exists(fresh.tmpDir) {
		t.Error("surviving sessions lost their tmpDir")
	}
}

func TestArchiveEnforceCaps_TempBytesCap(t *testing.T) {
	isolateArchiveSessions(t)
	setArchiveCaps(archiveMaxSessions, 100)
	capSession(t, "a", 4*time.Minute, 60)
	capSession(t, "b", 3*time.Minute, 60)
	c := capSession(t, "c", 2*time.Minute, 30)
	// An in-flight extraction counts at its declared size.
	c.inflight = map[string]*archiveExtractFlight{"x": newArchiveExtractFlight(filepath.Join(c.tmpDir, "x"), 5, nil)}
	d := capSession(t, "d", 0, 0)
	d.extractedSize = map[string]int64{"e": 20}

	archiveEnforceCaps(d) // total = 60+60+35+20 = 175 > 100 → drop a, then b (→ 55)

	if hasSession("a") || hasSession("b") {
		t.Error("oldest sessions should have been evicted to get under the byte cap")
	}
	if !hasSession("c") || !hasSession("d") {
		t.Error("sessions should stop being evicted once under the cap")
	}
}

func TestArchiveEnforceCaps_NeverEvictsPinnedOrKeptSession(t *testing.T) {
	isolateArchiveSessions(t)
	setArchiveCaps(0, 0)
	pinned := capSession(t, "pinned", 5*time.Minute, 50)
	keep := capSession(t, "keep", 4*time.Minute, 50)
	idle := capSession(t, "idle", time.Minute, 50)
	pinnedAcquired := archiveAcquireSession("pinned")
	if pinnedAcquired != pinned {
		t.Fatal("acquire returned the wrong session")
	}

	archiveEnforceCaps(keep)
	if !hasSession("pinned") || !hasSession("keep") {
		t.Error("a session with in-flight requests, or the one being served, must never be evicted")
	}
	if hasSession("idle") || exists(idle.tmpDir) {
		t.Error("an idle session over the cap should have been evicted and cleaned")
	}

	pinned.release()
	archiveEnforceCaps(keep)
	if hasSession("pinned") {
		t.Error("session must become evictable again once its last request finished")
	}
	if archiveAcquireSession("pinned") != nil {
		t.Error("evicted session must no longer be acquirable")
	}
}

// Evicting a session cancels its background extraction and removes the partial
// file; the flight reports the cancellation to anyone still waiting.
func TestArchiveDestroySession_CancelsBackgroundExtraction(t *testing.T) {
	content := progNoise(2<<20, 7)
	h := newHandler(t)
	key, sess := progCreate(t, h, progZip(t, progEntry{"movie.mkv", zip.Deflate, content}), "zip")
	gate, _ := progInstallHooks(t, sess, 256<<10)

	fl, _, err := archiveStartExtract(sess, "movie.mkv")
	if err != nil || fl == nil {
		t.Fatalf("start: fl=%v err=%v", fl, err)
	}
	if _, err := fl.waitFor(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	archiveSessionsMu.Lock()
	delete(archiveSessions, key)
	archiveSessionsMu.Unlock()
	// The teardown waits for the parked extraction to stop before it removes
	// anything, so it runs beside the test until the gate opens.
	destroyed := make(chan struct{})
	go func() {
		archiveDestroySession(sess)
		close(destroyed)
	}()
	gate.Release()

	select {
	case <-fl.done:
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled extraction did not stop")
	}
	select {
	case <-destroyed:
	case <-time.After(10 * time.Second):
		t.Fatal("teardown did not finish after the extraction stopped")
	}
	if !errors.Is(fl.err, context.Canceled) {
		t.Errorf("flight err = %v, want context.Canceled", fl.err)
	}
	if exists(fl.path) {
		t.Error("partial file survived cancellation")
	}
	sess.mu.Lock()
	_, cached := sess.extracted["movie.mkv"]
	sess.mu.Unlock()
	if cached {
		t.Error("cancelled extraction was cached")
	}
}

// ── progress reader ──────────────────────────────────────────────────────────

func TestArchiveProgressReader_WaitIsCancellable(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "p-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fl := newArchiveExtractFlight(f.Name(), 100, func() {})
	ctx, cancel := context.WithCancel(context.Background())
	pr := &archiveProgressReader{ctx: ctx, f: f, fl: fl, size: 100}

	errc := make(chan error, 1)
	go func() {
		_, err := pr.Read(make([]byte, 10))
		errc <- err
	}()
	select {
	case err := <-errc:
		t.Fatalf("Read returned %v before any bytes existed", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Read err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not return after ctx was cancelled")
	}
	if fl.progress() != 0 {
		t.Error("cancelling a reader must not touch the flight")
	}
}

func TestArchiveProgressReader_SeekAndShortExtraction(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "p-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	// Promised 20 bytes, only 10 ever arrive, and the extraction ends cleanly.
	fl := newArchiveExtractFlight(f.Name(), 20, func() {})
	fl.advance(10)
	fl.finish(nil)
	pr := &archiveProgressReader{ctx: context.Background(), f: f, fl: fl, size: 20}

	if end, err := pr.Seek(0, io.SeekEnd); err != nil || end != 20 {
		t.Fatalf("Seek end = %d, %v", end, err)
	}
	if _, err := pr.Seek(-3, io.SeekStart); err == nil {
		t.Error("negative seek must fail")
	}
	if _, err := pr.Seek(4, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if n, err := pr.Read(buf); err != nil || string(buf[:n]) != "4567" {
		t.Fatalf("Read = %q, %v", buf[:n], err)
	}
	if _, err := pr.Seek(10, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := pr.Read(buf); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Read past a short extraction: err = %v, want io.ErrUnexpectedEOF", err)
	}
}
