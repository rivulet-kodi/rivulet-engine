// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"bytes"
	"testing"
)

// ---- Parse tests -----------------------------------------------------------

const testNZB = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="[1/1] &quot;big.buck.bunny.mkv&quot; yEnc (1/2) 300 bytes">
    <segments>
      <segment bytes="200" number="2">def456@news.example.com</segment>
      <segment bytes="100" number="1">abc123@news.example.com</segment>
    </segments>
  </file>
  <file subject="just a plain subject without dot">
    <segments>
      <segment bytes="50" number="1">ghi789@news.example.com</segment>
    </segments>
  </file>
</nzb>`

func TestParse_FilesAndSegments(t *testing.T) {
	files, err := Parse([]byte(testNZB))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("got %d files, want 2", len(files))
	}

	// First file: quoted filename should be extracted.
	f := files[0]
	if f.Name != "big.buck.bunny.mkv" {
		t.Errorf("Name = %q, want %q", f.Name, "big.buck.bunny.mkv")
	}
	if f.Size != 300 {
		t.Errorf("Size = %d, want 300", f.Size)
	}
	if len(f.Segments) != 2 {
		t.Fatalf("segment count = %d, want 2", len(f.Segments))
	}

	// Segments must be sorted by Number.
	if f.Segments[0].Number != 1 {
		t.Errorf("Segments[0].Number = %d, want 1", f.Segments[0].Number)
	}
	if f.Segments[0].Bytes != 100 {
		t.Errorf("Segments[0].Bytes = %d, want 100", f.Segments[0].Bytes)
	}
	if f.Segments[0].MessageID != "abc123@news.example.com" {
		t.Errorf("Segments[0].MessageID = %q", f.Segments[0].MessageID)
	}
	if f.Segments[1].Number != 2 {
		t.Errorf("Segments[1].Number = %d, want 2", f.Segments[1].Number)
	}

	// Second file: no quoted name with dot → full subject.
	f2 := files[1]
	if f2.Name == "" {
		t.Error("Name must not be empty for subject-fallback file")
	}
	if f2.Size != 50 {
		t.Errorf("Size = %d, want 50", f2.Size)
	}
}

func TestParse_InvalidXML(t *testing.T) {
	_, err := Parse([]byte("<not valid xml"))
	if err == nil {
		t.Error("expected error for invalid XML, got nil")
	}
}

func TestParse_Empty(t *testing.T) {
	files, err := Parse([]byte(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"></nzb>`))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

// ---- decodeYenc tests ------------------------------------------------------

// buildYenc constructs a minimal yEnc-encoded block for a slice of bytes.
// Bytes that would become NUL, CR, LF, or '=' in the encoded form are escaped.
func buildYenc(data []byte) []byte {
	var body bytes.Buffer
	for _, b := range data {
		enc := b + 42 // yEnc encoding (uint8 wraps)
		switch enc {
		case 0x00, 0x0A, 0x0D, 0x3D: // NUL, LF, CR, '='
			body.WriteByte('=')
			body.WriteByte(enc + 64) // escape offset
		default:
			body.WriteByte(enc)
		}
	}

	var out bytes.Buffer
	out.WriteString("=ybegin line=128 size=")
	out.WriteString(itoa(len(data)))
	out.WriteString(" name=test.bin\r\n")
	out.Write(body.Bytes())
	out.WriteString("\r\n=yend size=")
	out.WriteString(itoa(len(data)))
	out.WriteString("\r\n")
	return out.Bytes()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestDecodeYenc_RoundTrip(t *testing.T) {
	// Bytes that do not produce encoded special characters.
	// original b → encoded: b+42 must not be 0,10,13,61 and must fit byte.
	original := []byte{1, 2, 3, 4, 5, 100, 150, 200}
	encoded := buildYenc(original)

	var out bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(encoded), &out); err != nil {
		t.Fatalf("decodeYenc: %v", err)
	}
	if !bytes.Equal(out.Bytes(), original) {
		t.Errorf("decoded %v, want %v", out.Bytes(), original)
	}
}

func TestDecodeYenc_WithEscapedBytes(t *testing.T) {
	// byte 214: 214+42 = 0 (mod 256) → must be escaped.
	// byte 19:  19+42  = 61 ('=')   → must be escaped.
	// byte 220: 220+42 = 6           → no escape needed, but make sure the
	//           escape decoding doesn't corrupt surrounding bytes.
	original := []byte{214, 19, 5}
	encoded := buildYenc(original)

	var out bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(encoded), &out); err != nil {
		t.Fatalf("decodeYenc: %v", err)
	}
	if !bytes.Equal(out.Bytes(), original) {
		t.Errorf("decoded %v, want %v", out.Bytes(), original)
	}
}

func TestDecodeYenc_AllBytesRoundTrip(t *testing.T) {
	// Full 0–255 round-trip via buildYenc + decodeYenc.
	original := make([]byte, 256)
	for i := range original {
		original[i] = byte(i)
	}
	encoded := buildYenc(original)

	var out bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(encoded), &out); err != nil {
		t.Fatalf("decodeYenc: %v", err)
	}
	if !bytes.Equal(out.Bytes(), original) {
		// Show first diff
		for i := range original {
			if i >= out.Len() || out.Bytes()[i] != original[i] {
				t.Errorf("first diff at index %d: got %d, want %d", i, out.Bytes()[i], original[i])
				break
			}
		}
	}
}

func TestDecodeYenc_NoYbegin(t *testing.T) {
	// Without =ybegin the article is not yEnc: must be an error, no output.
	var out bytes.Buffer
	err := decodeYencTo(bytes.NewReader([]byte("some random line\n")), &out)
	if err == nil {
		t.Fatal("expected error for article without =ybegin")
	}
	if out.Len() != 0 {
		t.Errorf("expected empty output without ybegin, got %d bytes", out.Len())
	}
}
