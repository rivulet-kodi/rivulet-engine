// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"runtime"
	"strings"
	"testing"
)

// encodeYencPart builds a realistic multi-part yEnc article for data, which
// occupies [begin-1, begin-1+len(data)) of a file of totalSize bytes: 128-byte
// encoded lines, CRLF line ends, =ybegin/=ypart/=yend with part sizes and
// pcrc32. The result is the raw article body as it appears (pre dot-stuffing)
// between the 222 response and the terminating ".".
func encodeYencPart(data []byte, totalSize, begin int64, part, parts int) []byte {
	var out bytes.Buffer
	fmt.Fprintf(&out, "=ybegin part=%d total=%d line=128 size=%d name=movie.mkv\r\n", part, parts, totalSize)
	fmt.Fprintf(&out, "=ypart begin=%d end=%d\r\n", begin, begin+int64(len(data))-1)
	col := 0
	for _, b := range data {
		enc := b + 42
		switch enc {
		case 0x00, 0x0A, 0x0D, 0x3D:
			out.WriteByte('=')
			out.WriteByte(enc + 64)
			col += 2
		default:
			out.WriteByte(enc)
			col++
		}
		if col >= 128 {
			out.WriteString("\r\n")
			col = 0
		}
	}
	if col > 0 {
		out.WriteString("\r\n")
	}
	fmt.Fprintf(&out, "=yend size=%d part=%d pcrc32=%08x\r\n", len(data), part, crc32.ChecksumIEEE(data))
	return out.Bytes()
}

// pseudoRandom returns n deterministic pseudo-random bytes.
func pseudoRandom(n int, seed int64) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(seed), 0x9e3779b97f4a7c15)) //nolint:gosec // deterministic test data
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// segmentSize is a typical Usenet article payload (~700 KiB decoded).
const benchSegmentSize = 700 * 1024

// BenchmarkDecodeYenc measures one article decode (the per-segment hot path):
// allocations per segment are what the pooled scanner/writer buffers remove.
func BenchmarkDecodeYenc(b *testing.B) {
	data := pseudoRandom(benchSegmentSize, 1)
	enc := encodeYencPart(data, int64(len(data)), 1, 1, 1)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		if err := decodeYencTo(bytes.NewReader(enc), io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}

// TestDecodeYenc_AllocsPerSegmentBounded guards the buffer pooling: a decode
// must not allocate the old 256 KiB scanner buffer + 128 KiB writer buffer
// (~390 KiB) per segment. After warm-up the steady-state allocation is a
// handful of small objects, orders of magnitude below one segment's payload.
func TestDecodeYenc_AllocsPerSegmentBounded(t *testing.T) {
	data := pseudoRandom(benchSegmentSize, 2)
	enc := encodeYencPart(data, int64(len(data)), 1, 1, 1)

	run := func() {
		if err := decodeYencTo(bytes.NewReader(enc), io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	run() // warm the pool

	allocs := testing.AllocsPerRun(20, run)
	if allocs > 16 {
		t.Errorf("decodeYenc allocs/op = %v, want <= 16", allocs)
	}
	if allocs < 1 {
		t.Errorf("allocs/op = %v: AllocsPerRun is not measuring the decode", allocs)
	}

	// Bytes allocated per decode, measured via a single-goroutine loop.
	const n = 50
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range n {
		run()
	}
	runtime.ReadMemStats(&after)
	perOp := (after.TotalAlloc - before.TotalAlloc) / n
	limit := uint64(16 * 1024)
	if raceEnabled {
		// -race makes sync.Pool drop ~1 in 4 Puts, so a fraction of decodes
		// rebuild the ~190 KiB state; still far below the old ~390 KiB each.
		limit = 100 * 1024
	}
	if perOp > limit {
		t.Errorf("decodeYenc allocated %d B/op, want <= %d B (pooled buffers; was ~393 KiB)", perOp, limit)
	}
}

// TestDecodeYenc_PooledBuffersDoNotLeakAcrossCalls decodes different articles
// back to back (including error paths that leave the pooled writer holding
// unflushed bytes) and verifies each output is exactly its own payload.
func TestDecodeYenc_PooledBuffersDoNotLeakAcrossCalls(t *testing.T) {
	a := pseudoRandom(5000, 3)
	b := pseudoRandom(3000, 4)

	// A truncated article leaves decoded-but-unflushed bytes in the writer.
	trunc := encodeYencPart(a, int64(len(a)), 1, 1, 1)
	trunc = trunc[:len(trunc)-len("=yend size=5000 part=1 pcrc32=00000000\r\n")]
	var junk bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(trunc), &junk); err == nil {
		t.Fatal("truncated article must error")
	}

	for i := range 5 {
		var out bytes.Buffer
		if err := decodeYencTo(bytes.NewReader(encodeYencPart(b, int64(len(b)), 1, 1, 1)), &out); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if !bytes.Equal(out.Bytes(), b) {
			t.Fatalf("iteration %d: output differs from payload (len %d vs %d)", i, out.Len(), len(b))
		}
	}
}

func TestEncodeYencPartRoundTrip(t *testing.T) {
	data := pseudoRandom(10_000, 5)
	var out bytes.Buffer
	if err := decodeYencTo(strings.NewReader(string(encodeYencPart(data, 20_000, 5_001, 2, 2))), &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatal("round trip mismatch")
	}
}
