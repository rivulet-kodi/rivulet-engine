// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"
)

func TestDecodeYenc_RejectsMalformed(t *testing.T) {
	// "*+," decodes to bytes 0,1,2.
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"error text body", "430 no such article\n"},
		{"yend without ybegin", "=yend size=0\n"},
		{"missing yend (truncated)", "=ybegin line=128 size=3 name=t.bin\n*+,\n"},
		{"size mismatch short", "=ybegin line=128 size=4 name=t.bin\n*+,\n=yend size=4\n"},
		{"size mismatch long", "=ybegin line=128 size=2 name=t.bin\n*+,\n=yend size=2\n"},
		{"crc mismatch", "=ybegin line=128 size=3 name=t.bin\n*+,\n=yend size=3 crc32=deadbeef\n"},
		{"pcrc mismatch multipart", "=ybegin part=1 line=128 size=9 name=t.bin\n=ypart begin=1 end=3\n*+,\n=yend size=3 part=1 pcrc32=deadbeef\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := decodeYencTo(strings.NewReader(tc.input), &out); err == nil {
				t.Fatalf("expected error, got nil (output %v)", out.Bytes())
			}
		})
	}
}

func TestDecodeYenc_AcceptsMatchingCRC(t *testing.T) {
	want := []byte{0, 1, 2}
	sum := crc32.ChecksumIEEE(want)
	cases := map[string]string{
		"crc32":  fmt.Sprintf("=ybegin line=128 size=3 name=t.bin\n*+,\n=yend size=3 crc32=%08x\n", sum),
		"pcrc32": fmt.Sprintf("=ybegin line=128 size=3 name=t.bin\n*+,\n=yend size=3 pcrc32=%08X\n", sum),
		// crc32= of a multipart article covers the whole file; it must not be
		// compared against the part.
		"multipart whole-file crc ignored": "=ybegin part=1 line=128 size=9 name=t.bin\n=ypart begin=1 end=3\n*+,\n=yend size=3 part=1 crc32=deadbeef\n",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			if err := decodeYencTo(strings.NewReader(in), &out); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(out.Bytes(), want) {
				t.Errorf("got %v, want %v", out.Bytes(), want)
			}
		})
	}
}

// decodeYencTo decodes a single-part article from r into w.
func decodeYencTo(r io.Reader, w io.Writer) error {
	_, _, err := decodeYenc(r, func(yencHeader) (io.Writer, error) { return w, nil })
	return err
}

// dial is DialContext with a background context.
func dial(cfg ServerConfig) (*Client, error) {
	return DialContext(context.Background(), cfg)
}
