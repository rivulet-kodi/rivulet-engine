// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"sync"
)

const (
	// yencMaxLine is the longest encoded line accepted (the bufio.Scanner
	// token limit). Real encoders emit ~128-byte lines (the spec allows at
	// most ~1 KiB), so anything near this is pathological input.
	yencMaxLine = 256 * 1024
	// yencScanBuf is the initial (pooled) scanner buffer; it only grows
	// toward yencMaxLine for pathologically long lines.
	yencScanBuf = 64 * 1024
	// yencWriteBuf batches the per-line writes into fewer, larger Write
	// calls (was one syscall per ~128-byte encoded line before).
	yencWriteBuf = 128 * 1024
)

var (
	ybeginTag = []byte("=ybegin")
	ypartTag  = []byte("=ypart")
	yendTag   = []byte("=yend")
)

// yencState is the per-decode scratch memory. Decoding one article used to
// allocate a 256 KiB scanner buffer and a 128 KiB bufio writer (~390 KiB per
// ~700 KiB segment); both are now pooled, so a steady-state decode allocates
// only a few small objects regardless of how many segments are fetched.
type yencState struct {
	scan []byte        // initial bufio.Scanner buffer
	out  []byte        // decoded bytes of the current line
	bw   *bufio.Writer // batches writes to the destination
}

var yencPool = sync.Pool{New: func() any {
	return &yencState{
		scan: make([]byte, yencScanBuf),
		out:  make([]byte, 0, 1024),
		bw:   bufio.NewWriterSize(io.Discard, yencWriteBuf),
	}
}}

// yencHeader carries the layout fields of an article's =ybegin/=ypart lines.
type yencHeader struct {
	Size    int64 // =ybegin size=: size of the whole file; 0 when absent
	Part    int   // =ybegin part=; 0 when absent
	Begin   int64 // =ypart begin= (1-based, inclusive); 0 when absent
	End     int64 // =ypart end= (1-based, inclusive); 0 when absent
	HasPart bool  // an =ypart line was present (a multi-part article)
}

// parseYbegin extracts size= and part= from a "=ybegin ..." line. The name=
// field runs to the end of the line and may contain spaces, so parsing stops
// there.
func parseYbegin(line []byte, h *yencHeader) {
	for _, f := range strings.Fields(string(line[len(ybeginTag):])) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "size":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				h.Size = n
			}
		case "part":
			if n, err := strconv.Atoi(v); err == nil {
				h.Part = n
			}
		case "name":
			return
		}
	}
}

// parseYpart extracts begin= and end= from a "=ypart ..." line.
func parseYpart(line []byte, h *yencHeader) {
	for _, f := range strings.Fields(string(line[len(ypartTag):])) {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "begin":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				h.Begin = n
			}
		case "end":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				h.End = n
			}
		}
	}
}

// yencOpenFunc is called once per article, after its headers are parsed and
// before the first decoded byte is written, to choose the destination. It
// lets a caller place a multi-part article at the offset its =ypart header
// declares. Returning an error aborts the decode.
type yencOpenFunc func(h yencHeader) (io.Writer, error)

// decodeYenc decodes a yEnc-encoded article body from r and writes the decoded
// bytes to the writer chosen by open, which is invoked once the article's
// =ybegin/=ypart headers are known. It returns the parsed header and the number
// of decoded bytes written.
//
// yEnc encoding: each original byte b is stored as (b + 42) % 256. Four values
// in the encoded stream (NUL, CR, LF, '=') must be further escaped: the escape
// character '=' is emitted, followed by (b + 42 + 64) % 256. Decoding reverses
// this: a plain byte p decodes to (p - 42) % 256; an escaped byte e (after '=')
// decodes to (e - 64 - 42) % 256.
//
// Line endings added by the encoder for line-length management are not part of
// the original data and are discarded by the scanner.
//
// The article must be well-formed: decodeYenc returns an error when no
// =ybegin header or no =yend trailer is found (error text, other encodings,
// truncated body), when the decoded length differs from the trailer's size=,
// or when a pcrc32= (or, for single-part articles, crc32=) is present and does
// not match. Callers rely on this so a bad article is never treated as data.
func decodeYenc(r io.Reader, open yencOpenFunc) (yencHeader, int64, error) {
	st := yencPool.Get().(*yencState)
	defer func() {
		// Drop the destination reference and any unflushed bytes so nothing
		// leaks into the next article that reuses this writer.
		st.bw.Reset(io.Discard)
		yencPool.Put(st)
	}()

	scanner := bufio.NewScanner(r)
	// The pooled buffer is the scanner's initial buffer; it still grows up to
	// yencMaxLine for an unusually long line, exactly as before.
	scanner.Buffer(st.scan, yencMaxLine)

	var (
		hdr     yencHeader
		inBody  bool
		opened  bool
		written int64
		sum     uint32 // running CRC-32 (IEEE) of the decoded bytes
	)
	// bind attaches the destination once, at the first data line (or at the
	// trailer of an empty part) — by then =ybegin/=ypart have been parsed.
	bind := func() error {
		if opened {
			return nil
		}
		opened = true
		w, err := open(hdr)
		if err != nil {
			return err
		}
		st.bw.Reset(w)
		return nil
	}

	for scanner.Scan() {
		// scanner.Bytes() is a zero-copy view into the scanner's internal
		// buffer, valid until the next Scan(). Replaces scanner.Text() +
		// []byte(line) which allocated two heap objects per line.
		raw := scanner.Bytes()

		switch {
		case bytes.HasPrefix(raw, ybeginTag):
			inBody = true
			parseYbegin(raw, &hdr)
			continue
		case bytes.HasPrefix(raw, ypartTag):
			// multi-part header: still inside the data section
			hdr.HasPart = true
			parseYpart(raw, &hdr)
			continue
		case bytes.HasPrefix(raw, yendTag):
			// end-of-part marker; flush the buffer, verify and stop decoding.
			if !inBody {
				return hdr, written, fmt.Errorf("yenc: =yend without =ybegin")
			}
			if err := bind(); err != nil {
				return hdr, written, err
			}
			if err := st.bw.Flush(); err != nil {
				return hdr, written, err
			}
			return hdr, written, verifyYencTrailer(string(raw), written, sum, hdr.HasPart)
		}

		if !inBody {
			continue
		}
		if err := bind(); err != nil {
			return hdr, written, err
		}

		// st.out is reused across all lines: only reset to length 0.
		out := st.out[:0]
		for i := 0; i < len(raw); i++ {
			b := raw[i]
			if b == '=' {
				i++
				if i >= len(raw) {
					return hdr, written, fmt.Errorf("yenc: escape character at end of line")
				}
				// escaped byte: subtract 64 + 42 (wraps as uint8)
				out = append(out, raw[i]-64-42)
			} else {
				// plain byte: subtract 42 (wraps as uint8)
				out = append(out, b-42)
			}
		}
		if cap(out) <= yencScanBuf {
			st.out = out // keep growth for the next line / article (bounded)
		}

		if _, err := st.bw.Write(out); err != nil {
			return hdr, written, err
		}
		written += int64(len(out))
		sum = crc32.Update(sum, crc32.IEEETable, out)
	}
	if err := scanner.Err(); err != nil {
		return hdr, written, err
	}
	// Whatever was decoded is still flushed, but the article is not usable.
	if opened {
		_ = st.bw.Flush()
	}
	if !inBody {
		return hdr, written, fmt.Errorf("yenc: no =ybegin header found (not a yEnc article)")
	}
	return hdr, written, fmt.Errorf("yenc: no =yend trailer found (truncated article)")
}

// verifyYencTrailer checks the decoded part length and CRC against the values
// declared on the =yend line. Absent or unparsable fields are not enforced,
// except that a size= that parses must match exactly.
func verifyYencTrailer(line string, written int64, sum uint32, multipart bool) error {
	var crcStr string
	for _, f := range strings.Fields(line)[1:] {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "size":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n != written {
				return fmt.Errorf("yenc: decoded %d bytes, =yend declares size=%d", written, n)
			}
		case "pcrc32":
			crcStr = v
		case "crc32":
			// For a multi-part article crc32= covers the whole file, not this
			// part; only trust it for single-part articles.
			if !multipart && crcStr == "" {
				crcStr = v
			}
		}
	}
	if crcStr == "" {
		return nil
	}
	want, err := strconv.ParseUint(crcStr, 16, 32)
	if err != nil {
		return nil
	}
	if uint32(want) != sum {
		return fmt.Errorf("yenc: crc32 mismatch: decoded %08x, =yend declares %08x", sum, want)
	}
	return nil
}
