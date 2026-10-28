// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// dialTimeout is the maximum time allowed to establish a TCP/TLS connection.
	dialTimeout = 30 * time.Second
	// opTimeout is the per-operation deadline applied around greeting, auth, and
	// each BODY fetch. It is refreshed before every Body call.
	opTimeout = 60 * time.Second
	// closeTimeout bounds the QUIT exchange in Close so a wedged server cannot
	// stall connection teardown for the remainder of opTimeout.
	closeTimeout = 5 * time.Second
)

// ServerConfig holds the connection parameters for one NNTP server.
type ServerConfig struct {
	Host string
	Port int
	User string
	Pass string
	SSL  bool
	// Connections is how many parallel NNTP connections StartAssembly may
	// open for one file (the server's allowed connection count). Values <= 0
	// select defaultConnections; the effective value is capped at
	// maxConnections. Extra connections the server refuses are tolerated —
	// the assembly just runs with fewer.
	Connections int
	// Control, when set, is passed as the net.Dialer.Control hook for the
	// TCP dial (and, for SSL, the TLS handshake's underlying TCP dial),
	// letting callers apply an SSRF guard (e.g. netguard.DialControl) that
	// re-validates the actual resolved IP at connect time — closing the
	// gap where a caller-supplied NNTP host previously reached internal
	// hosts with no guard at all. nil means no additional restriction.
	Control func(network, address string, c syscall.RawConn) error
}

// Client is a sequential NNTP connection. A single connection is used; callers
// must not invoke methods concurrently.
type Client struct {
	tp   *textproto.Conn
	conn net.Conn
}

// containsCRLF reports whether s contains a carriage return or line feed,
// which would allow injection into the NNTP command stream.
func containsCRLF(s string) bool {
	return strings.ContainsAny(s, "\r\n")
}

// DialContext opens a TCP (or TLS) connection to the NNTP server described by
// cfg, performs the initial handshake, and authenticates when credentials are
// set. Default ports: 119 (plain), 563 (SSL) when cfg.Port == 0.
// A 30 s dial timeout and 60 s per-operation deadlines are enforced.
// Cancelling ctx aborts a dial or handshake in progress (the connection is
// closed, the error is returned).
func DialContext(ctx context.Context, cfg ServerConfig) (*Client, error) {
	port := cfg.Port
	if port == 0 {
		if cfg.SSL {
			port = 563
		} else {
			port = 119
		}
	}

	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(port))

	var conn net.Conn
	var err error
	dialer := &net.Dialer{Timeout: dialTimeout, Control: cfg.Control}
	if cfg.SSL {
		td := tls.Dialer{NetDialer: dialer, Config: &tls.Config{ServerName: cfg.Host}}
		conn, err = td.DialContext(ctx, "tcp", addr)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("nntp: dial %s: %w", addr, err)
	}
	// Unblock the greeting/auth exchange below if ctx is cancelled mid-way.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	// Set a deadline covering the greeting and any authentication exchange.
	if err := conn.SetDeadline(time.Now().Add(opTimeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("nntp: set deadline: %w", err)
	}

	tp := textproto.NewConn(conn)

	// Read server greeting: 200 (post allowed) or 201 (read-only).
	code, _, err := tp.ReadResponse(0)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("nntp: greeting: %w", err)
	}
	if code != 200 && code != 201 {
		_ = conn.Close()
		return nil, fmt.Errorf("nntp: unexpected greeting code %d", code)
	}

	c := &Client{tp: tp, conn: conn}

	if cfg.User != "" {
		if err := c.authinfo(cfg.User, cfg.Pass); err != nil {
			_ = c.Close()
			return nil, err
		}
	}

	// Clear the handshake deadline; Body sets a fresh deadline per request.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("nntp: clear deadline: %w", err)
	}

	return c, nil
}

// authinfo performs AUTHINFO USER / PASS authentication.
func (c *Client) authinfo(user, pass string) error {
	if containsCRLF(user) {
		return fmt.Errorf("nntp: AUTHINFO: username contains invalid characters")
	}
	if containsCRLF(pass) {
		return fmt.Errorf("nntp: AUTHINFO: password contains invalid characters")
	}

	if err := c.tp.PrintfLine("AUTHINFO USER %s", user); err != nil {
		return fmt.Errorf("nntp: AUTHINFO USER: %w", err)
	}
	code, _, err := c.tp.ReadResponse(0)
	if err != nil {
		return fmt.Errorf("nntp: AUTHINFO USER response: %w", err)
	}
	switch code {
	case 281:
		// accepted without password
		return nil
	case 381:
		// password required
	default:
		return fmt.Errorf("nntp: AUTHINFO USER: unexpected code %d", code)
	}

	if err := c.tp.PrintfLine("AUTHINFO PASS %s", pass); err != nil {
		return fmt.Errorf("nntp: AUTHINFO PASS: %w", err)
	}
	if _, _, err := c.tp.ReadResponse(281); err != nil {
		return fmt.Errorf("nntp: authentication failed: %w", err)
	}
	return nil
}

// Body fetches the body of the article identified by messageID, yEnc-decodes
// it, and writes the decoded bytes to w. The leading/trailing angle brackets
// on messageID are optional; Body adds them if absent.
func (c *Client) Body(messageID string, w io.Writer) error {
	_, _, err := c.body(messageID, func(yencHeader) (io.Writer, error) { return w, nil })
	return err
}

// body is Body with a caller-chosen destination (see yencOpenFunc). It
// returns the article's parsed yEnc header and the decoded byte count.
func (c *Client) body(messageID string, open yencOpenFunc) (yencHeader, int64, error) {
	if containsCRLF(messageID) {
		return yencHeader{}, 0, fmt.Errorf("nntp: Body: messageID contains invalid characters")
	}

	// Normalise message-id: ensure angle brackets.
	if len(messageID) == 0 || messageID[0] != '<' {
		messageID = "<" + messageID + ">"
	}

	// Refresh the per-operation deadline before each fetch.
	if err := c.conn.SetDeadline(time.Now().Add(opTimeout)); err != nil {
		return yencHeader{}, 0, fmt.Errorf("nntp: set deadline: %w", err)
	}

	if err := c.tp.PrintfLine("BODY %s", messageID); err != nil {
		return yencHeader{}, 0, fmt.Errorf("nntp: BODY send: %w", err)
	}

	// Expect 222 (body follows).
	if _, _, err := c.tp.ReadResponse(222); err != nil {
		return yencHeader{}, 0, fmt.Errorf("nntp: BODY %s: %w", messageID, err)
	}

	// DotReader handles dot-unstuffing of the dot-terminated article body.
	dr := c.tp.DotReader()
	hdr, n, err := decodeYenc(dr, open)
	// Drain any unread bytes so the connection is left at a clean command
	// boundary regardless of how decodeYenc exited (early =yend return,
	// write error, etc.). Without this, leftover body bytes would be read
	// as the next server response, silently desyncing the protocol stream.
	_, _ = io.Copy(io.Discard, dr)
	return hdr, n, err
}

// abort closes the underlying connection immediately, without the QUIT
// exchange. It is safe to call from another goroutine and unblocks a Body
// call that is waiting on the network.
func (c *Client) abort() {
	_ = c.conn.Close()
}

// Close sends QUIT and closes the underlying connection.
func (c *Client) Close() error {
	_ = c.conn.SetDeadline(time.Now().Add(closeTimeout))
	_ = c.tp.PrintfLine("QUIT")
	_, _, _ = c.tp.ReadResponse(205)
	return c.conn.Close()
}
