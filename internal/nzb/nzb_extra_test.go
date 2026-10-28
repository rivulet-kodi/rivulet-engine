// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"bytes"
	"fmt"
	"net"
	"net/textproto"
	"strings"
	"testing"
)

// ===== decodeYenc additional cases ===========================================

func TestDecodeYenc_MultiLineBlock(t *testing.T) {
	// yEnc body split across multiple encoded lines.
	// Bytes 0-3 encode to '*','+',',','-' (42-45) — all safe, no escaping.
	var sb strings.Builder
	sb.WriteString("=ybegin line=128 size=4 name=test.bin\n")
	sb.WriteString("*+\n") // bytes 0, 1 decoded as (42-42)%256=0, (43-42)%256=1
	sb.WriteString(",-\n") // bytes 2, 3 decoded as (44-42)%256=2, (45-42)%256=3
	sb.WriteString("=yend size=4\n")

	var out bytes.Buffer
	if err := decodeYencTo(strings.NewReader(sb.String()), &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []byte{0, 1, 2, 3}; !bytes.Equal(out.Bytes(), want) {
		t.Errorf("got %v, want %v", out.Bytes(), want)
	}
}

func TestDecodeYenc_EscapeProducing0x00(t *testing.T) {
	// Byte 214: (214+42) % 256 == 0 (NUL) — encoder must escape it.
	data := []byte{214}
	var out bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(buildYenc(data)), &out); err != nil {
		t.Fatalf("decodeYenc error: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("got %#x, want %#x", out.Bytes(), data)
	}
}

func TestDecodeYenc_EscapeProducing0x3D(t *testing.T) {
	// Byte 19: (19+42) == 61 == 0x3D ('=') — encoder must escape it.
	data := []byte{19}
	var out bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(buildYenc(data)), &out); err != nil {
		t.Fatalf("decodeYenc error: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("got %#x, want %#x", out.Bytes(), data)
	}
}

func TestDecodeYenc_ByteWrapping(t *testing.T) {
	// Bytes > 213 wrap mod 256 during encoding: (b+42) overflows uint8.
	data := []byte{215, 240, 255}
	var out bytes.Buffer
	if err := decodeYencTo(bytes.NewReader(buildYenc(data)), &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Errorf("got %v, want %v", out.Bytes(), data)
	}
}

func TestDecodeYenc_GarbageMustNotPanic(t *testing.T) {
	// None of these must panic or hang regardless of input shape.
	cases := []struct {
		name  string
		input string
	}{
		{"pure garbage", "garbage\xffdata\x00\nhereafter\n"},
		{"only yend", "=yend size=0\n"},
		{"completely empty", ""},
		{"ybegin no yend (EOF mid-body)", "=ybegin line=128 size=3 name=t.bin\n*+,"},
		{"binary noise after ybegin", "=ybegin\n\x00\xff\x80\x7f\n=yend\n"},
		{"ypart without ybegin", "=ypart begin=1 end=128\nsome data\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			_ = decodeYencTo(strings.NewReader(tc.input), &out)
		})
	}
}

func TestDecodeYenc_EscapeAtEndOfLine(t *testing.T) {
	// '=' at end of an encoded line is an incomplete escape — must return error.
	input := "=ybegin line=128 size=1 name=t.bin\nabc=\n=yend\n"
	var out bytes.Buffer
	if err := decodeYencTo(strings.NewReader(input), &out); err == nil {
		t.Error("expected error for escape character at end of line, got nil")
	}
}

func TestDecodeYenc_YpartSkipped(t *testing.T) {
	// =ypart header must be ignored; data after it must still decode.
	var sb strings.Builder
	sb.WriteString("=ybegin line=128 size=2 name=t.bin\n")
	sb.WriteString("=ypart begin=1 end=2\n")
	sb.WriteString("*+\n") // bytes 0, 1
	sb.WriteString("=yend size=2\n")

	var out bytes.Buffer
	if err := decodeYencTo(strings.NewReader(sb.String()), &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []byte{0, 1}; !bytes.Equal(out.Bytes(), want) {
		t.Errorf("got %v, want %v", out.Bytes(), want)
	}
}

// ===== Parse additional cases ================================================

func TestParse_QuotedNameMultipleDots(t *testing.T) {
	// Quoted filename with multiple dots must be extracted in full.
	nzb := `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="[1/2] &quot;my.movie.1080p.BluRay.mkv&quot; yEnc (1/2)">
    <segments>
      <segment bytes="500" number="1">id@host.com</segment>
    </segments>
  </file>
</nzb>`
	files, err := Parse([]byte(nzb))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("got %d files, want 1", len(files))
	}
	const want = "my.movie.1080p.BluRay.mkv"
	if files[0].Name != want {
		t.Errorf("Name = %q, want %q", files[0].Name, want)
	}
}

func TestParse_EntityHeavy(t *testing.T) {
	// XML with many character entities must parse without error.
	nzb := `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="[1/1] &quot;&amp;lt;A &amp;amp; B&gt;&quot; yEnc (1/1)">
    <segments>
      <segment bytes="1000" number="1">entity@srv.com</segment>
    </segments>
  </file>
  <file subject="&lt;plain&gt; subject no quotes">
    <segments>
      <segment bytes="200" number="1">plain@srv.com</segment>
    </segments>
  </file>
</nzb>`
	files, err := Parse([]byte(nzb))
	if err != nil {
		t.Fatalf("entity-heavy XML: unexpected error: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("got %d files, want 2", len(files))
	}
}

func TestParse_SegmentsOutOfOrder(t *testing.T) {
	// Segments in reverse order must be sorted ascending by Number.
	nzb := `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="&quot;ordered.bin&quot;">
    <segments>
      <segment bytes="30" number="3">seg3@host</segment>
      <segment bytes="10" number="1">seg1@host</segment>
      <segment bytes="20" number="2">seg2@host</segment>
    </segments>
  </file>
</nzb>`
	files, err := Parse([]byte(nzb))
	if err != nil {
		t.Fatal(err)
	}
	segs := files[0].Segments
	for i := 1; i < len(segs); i++ {
		if segs[i-1].Number >= segs[i].Number {
			t.Errorf("segments not sorted: [%d].Number=%d >= [%d].Number=%d",
				i-1, segs[i-1].Number, i, segs[i].Number)
		}
	}
	if got := files[0].Size; got != 60 {
		t.Errorf("Size = %d, want 60", got)
	}
}

func TestParse_SubjectFallback(t *testing.T) {
	// Subject without a quoted "name.ext" uses the trimmed subject as Name.
	nzb := `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="  plain subject without dot  ">
    <segments>
      <segment bytes="10" number="1">id@host</segment>
    </segments>
  </file>
</nzb>`
	files, err := Parse([]byte(nzb))
	if err != nil {
		t.Fatal(err)
	}
	const want = "plain subject without dot"
	if files[0].Name != want {
		t.Errorf("Name = %q, want %q (trimmed subject)", files[0].Name, want)
	}
}

func TestParse_ThreeFiles(t *testing.T) {
	nzb := `<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file subject="&quot;a.mkv&quot;">
    <segments><segment bytes="100" number="1">a@host</segment></segments>
  </file>
  <file subject="&quot;b.mkv&quot;">
    <segments><segment bytes="200" number="1">b@host</segment></segments>
  </file>
  <file subject="&quot;c.mkv&quot;">
    <segments><segment bytes="300" number="1">c@host</segment></segments>
  </file>
</nzb>`
	files, err := Parse([]byte(nzb))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3", len(files))
	}
	for i, tc := range []struct {
		name string
		size int64
	}{{"a.mkv", 100}, {"b.mkv", 200}, {"c.mkv", 300}} {
		if files[i].Name != tc.name {
			t.Errorf("files[%d].Name = %q, want %q", i, files[i].Name, tc.name)
		}
		if files[i].Size != tc.size {
			t.Errorf("files[%d].Size = %d, want %d", i, files[i].Size, tc.size)
		}
	}
}

// ===== containsCRLF tests ====================================================

func TestContainsCRLF(t *testing.T) {
	cases := []struct {
		s    string
		want bool
	}{
		{"normal", false},
		{"has\r", true},
		{"has\n", true},
		{"has\r\n", true},
		{"", false},
		{"a\rb", true},
		{"a\nb", true},
		{"abc", false},
		{"\rleading", true},
		{"trailing\n", true},
	}
	for _, tc := range cases {
		if got := containsCRLF(tc.s); got != tc.want {
			t.Errorf("containsCRLF(%q) = %v, want %v", tc.s, got, tc.want)
		}
	}
}

// ===== NNTP Client tests — no real NNTP server; use net.Pipe / net.Listener ==

// makeClient returns a Client backed by the client side of a net.Pipe and the
// server-side net.Conn. The caller is responsible for closing both.
func makeClient(t *testing.T) (*Client, net.Conn) {
	t.Helper()
	srv, cli := net.Pipe()
	c := &Client{tp: textproto.NewConn(cli), conn: cli}
	return c, srv
}

// serveDial starts a goroutine that accepts one TCP connection on ln, sends
// greeting immediately, then for each response in responses reads one line
// from the client and sends that response. ln is not closed by serveDial.
func serveDial(t *testing.T, ln net.Listener, greeting string, responses ...string) {
	t.Helper()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		tp := textproto.NewConn(conn)
		if err := tp.PrintfLine("%s", greeting); err != nil {
			return
		}
		for _, resp := range responses {
			if _, err := tp.ReadLine(); err != nil {
				return
			}
			if err := tp.PrintfLine("%s", resp); err != nil {
				return
			}
		}
	}()
}

// --- CRLF injection guards (pure, no network I/O) ---

func TestAuthinfo_RejectsCRLFInUser(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	if err := c.authinfo("user\rinjected", "pass"); err == nil {
		t.Error("expected error for CR in username, got nil")
	}
}

func TestAuthinfo_RejectsCRLFInPass(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	if err := c.authinfo("validuser", "pass\ninjected"); err == nil {
		t.Error("expected error for LF in password, got nil")
	}
}

func TestBody_RejectsCRLFInMessageID(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	var buf bytes.Buffer
	if err := c.Body("msg\r\nInjected-Header: x", &buf); err == nil {
		t.Error("expected error for CRLF in messageID, got nil")
	}
}

// --- authinfo full protocol flow (net.Pipe fake server) ---

func TestAuthinfo_AcceptedWithoutPassword(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                                //nolint:errcheck // AUTHINFO USER …
		tp.PrintfLine("281 Authentication accepted") //nolint:errcheck
	}()
	if err := c.authinfo("user", "any"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAuthinfo_RequiresPassword_Success(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                                //nolint:errcheck // AUTHINFO USER
		tp.PrintfLine("381 Password required")       //nolint:errcheck
		tp.ReadLine()                                //nolint:errcheck // AUTHINFO PASS
		tp.PrintfLine("281 Authentication accepted") //nolint:errcheck
	}()
	if err := c.authinfo("user", "pass"); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAuthinfo_UnexpectedCode(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                        //nolint:errcheck
		tp.PrintfLine("500 Unknown command") //nolint:errcheck
	}()
	if err := c.authinfo("user", "pass"); err == nil {
		t.Error("expected error for unexpected code 500, got nil")
	}
}

func TestAuthinfo_BadPassword(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                                //nolint:errcheck
		tp.PrintfLine("381 Password required")       //nolint:errcheck
		tp.ReadLine()                                //nolint:errcheck
		tp.PrintfLine("482 Authentication rejected") //nolint:errcheck
	}()
	if err := c.authinfo("user", "wrongpass"); err == nil {
		t.Error("expected error for rejected password, got nil")
	}
}

// --- Body full protocol flow (net.Pipe fake server) ---

func TestBody_Success(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()

	// Bytes 0-3 encode to '*','+',',','-' (42-45) — all safe, no escaping.
	original := []byte{0, 1, 2, 3}
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                          //nolint:errcheck // BODY <…>
		tp.PrintfLine("222 0 <msg@host> body") //nolint:errcheck
		dw := tp.DotWriter()
		fmt.Fprintf(dw, "=ybegin line=128 size=4 name=test.bin\r\n")
		fmt.Fprintf(dw, "*+,-\r\n") // bytes 0,1,2,3 encoded
		fmt.Fprintf(dw, "=yend size=4\r\n")
		dw.Close()
	}()

	var buf bytes.Buffer
	if err := c.Body("msg@host", &buf); err != nil {
		t.Fatalf("Body error: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), original) {
		t.Errorf("got %v, want %v", buf.Bytes(), original)
	}
}

func TestBody_ErrorResponse(t *testing.T) {
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                        //nolint:errcheck
		tp.PrintfLine("430 No Such Article") //nolint:errcheck
	}()
	var buf bytes.Buffer
	if err := c.Body("msg@host", &buf); err == nil {
		t.Error("expected error for 430 response, got nil")
	}
}

func TestBody_AngleBracketsAlreadyPresent(t *testing.T) {
	// messageID already wrapped in < > must not be double-wrapped.
	c, srv := makeClient(t)
	defer srv.Close()
	defer c.conn.Close()
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                        //nolint:errcheck
		tp.PrintfLine("430 No Such Article") //nolint:errcheck
	}()
	var buf bytes.Buffer
	if err := c.Body("<already@bracketed>", &buf); err == nil {
		t.Error("expected error for 430 response, got nil")
	}
}

// --- Dial via local net.Listener (no real NNTP server) ---

func TestDial_PlainTextGreeting200(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveDial(t, ln, "200 Welcome", "205 Closing")

	port := ln.Addr().(*net.TCPAddr).Port
	c, err := dial(ServerConfig{Host: "127.0.0.1", Port: port})
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}
	_ = c.Close()
}

func TestDial_Greeting201(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveDial(t, ln, "201 Read-only welcome", "205 Closing")

	port := ln.Addr().(*net.TCPAddr).Port
	c, err := dial(ServerConfig{Host: "127.0.0.1", Port: port})
	if err != nil {
		t.Fatalf("Dial error: %v", err)
	}
	_ = c.Close()
}

func TestDial_UnexpectedGreeting(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serveDial(t, ln, "400 Service temporarily unavailable")

	port := ln.Addr().(*net.TCPAddr).Port
	_, err = dial(ServerConfig{Host: "127.0.0.1", Port: port})
	if err == nil {
		t.Error("expected error for unexpected greeting code 400, got nil")
	}
}

func TestDial_WithAuth(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// greeting → read AUTHINFO USER → 381 → read AUTHINFO PASS → 281 → read QUIT → 205
	serveDial(t, ln, "200 Welcome",
		"381 Password required",
		"281 Authentication accepted",
		"205 Closing",
	)

	port := ln.Addr().(*net.TCPAddr).Port
	c, err := dial(ServerConfig{Host: "127.0.0.1", Port: port, User: "u", Pass: "p"})
	if err != nil {
		t.Fatalf("Dial with auth error: %v", err)
	}
	_ = c.Close()
}

func TestDial_AuthAcceptedWithoutPassword(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// greeting → read AUTHINFO USER → 281 (no password needed) → read QUIT → 205
	serveDial(t, ln, "200 Welcome",
		"281 Authentication accepted",
		"205 Closing",
	)

	port := ln.Addr().(*net.TCPAddr).Port
	c, err := dial(ServerConfig{Host: "127.0.0.1", Port: port, User: "u", Pass: "p"})
	if err != nil {
		t.Fatalf("Dial auth-without-password error: %v", err)
	}
	_ = c.Close()
}

func TestDial_AuthFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// greeting → read AUTHINFO USER → 500 (auth error)
	serveDial(t, ln, "200 Welcome", "500 Error")

	port := ln.Addr().(*net.TCPAddr).Port
	_, err = dial(ServerConfig{Host: "127.0.0.1", Port: port, User: "u", Pass: "p"})
	if err == nil {
		t.Error("expected error when authentication fails, got nil")
	}
}

func TestDial_ConnectionRefused(t *testing.T) {
	// Open then immediately close a listener so the port is not in use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	_, err = dial(ServerConfig{Host: "127.0.0.1", Port: port})
	if err == nil {
		t.Error("expected dial error for closed port, got nil")
	}
}

// --- Close ---

func TestClient_Close(t *testing.T) {
	c, srv := makeClient(t)
	go func() {
		tp := textproto.NewConn(srv)
		tp.ReadLine()                           //nolint:errcheck // QUIT
		tp.PrintfLine("205 Closing connection") //nolint:errcheck
		srv.Close()
	}()
	if err := c.Close(); err != nil {
		t.Errorf("Close error: %v", err)
	}
}
