// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package ftpstream provides a unified Open function for streaming files from
// FTP/FTPS servers and HTTP/HTTPS URLs.
//
// FTP connections are established using the RFC 959 protocol. FTPS uses
// implicit TLS (the connection is wrapped in TLS from the start). Logged-in
// control connections are pooled per (server, credentials) for a short TTL so
// the many short ranged opens of a seeking player skip dial+login (see
// pool.go). HTTP/HTTPS connections use the standard net/http client with a
// Range header when an offset is requested.
package ftpstream

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jlaffaye/ftp"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// ftpAllowPrivate reports whether RIVULET_FTP_ALLOW_PRIVATE opts into
// reaching private/loopback/RFC1918 addresses (e.g. a LAN NAS) from the
// unauthenticated /ftp?lz= route. Default is false (secure): only public
// addresses are reachable. The cloud-metadata address (169.254.169.254) is
// always blocked regardless, via netguard.ValidateIP's unconditional check.
func ftpAllowPrivate() bool {
	v := strings.TrimSpace(os.Getenv("RIVULET_FTP_ALLOW_PRIVATE"))
	return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "on")
}

// preflightHost resolves host and rejects it up front when every candidate
// address is disallowed by netguard.ValidateIP, giving a fast, clear error
// before any protocol handshake begins. The real enforcement point remains
// the dialer's Control hook (netguard.DialControl via ftpDialControl /
// the FTP session dialer), which re-validates the specific resolved IP at
// connect time and forecloses DNS-rebinding; this is a defense-in-depth
// fast-fail layer, not a substitute for it. A DNS resolution failure is not
// itself treated as a block — the real dial/request attempt surfaces that
// error naturally.
//
// Verdicts (but not resolution failures) are cached for preflightTTL per
// (host, blockPrivate) so a burst of ranged requests costs one DNS lookup.
func preflightHost(ctx context.Context, host string, blockPrivate bool) error {
	k := preflightKey{host: host, blockPrivate: blockPrivate}
	if hit, verdict := preflightGet(k); hit {
		return verdict
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addrs) == 0 {
		return nil
	}
	var firstErr error
	for _, a := range addrs {
		if verr := netguard.ValidateIP(a.IP, blockPrivate); verr != nil {
			if firstErr == nil {
				firstErr = verr
			}
			continue
		}
		firstErr = nil
		break
	}
	preflightPut(k, firstErr)
	return firstErr
}

// ftpDialControl is httpGuardedClient's net.Dialer.Control hook. It re-reads
// ftpAllowPrivate() on every dial — rather than baking the decision in at
// package-init time — so the pooled client's behaviour always reflects the
// current opt-in state.
func ftpDialControl(network, address string, c syscall.RawConn) error {
	return netguard.DialControl(!ftpAllowPrivate())(network, address, c)
}

// httpGuardedClient is a package-level HTTP client whose dialer rejects the
// cloud-metadata address and, by default, all private/loopback/RFC1918
// addresses at connect time (see ftpDialControl / RIVULET_FTP_ALLOW_PRIVATE).
// Reused across requests to pool connections.
var httpGuardedClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
			Control: ftpDialControl,
		}).DialContext,
	},
}

// ftpParsed holds the components extracted from an ftp:// or ftps:// URL.
type ftpParsed struct {
	addr string // host:port
	host string // hostname only, used as TLS ServerName
	user string
	pass string
	path string
	tls  bool // true for ftps://
}

// parseFTPURL extracts the dial address, credentials, path, and TLS flag from
// an ftp:// or ftps:// URL.
//
// Defaults: port 21, user "anonymous", empty password.
func parseFTPURL(rawURL string) (*ftpParsed, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("ftpstream: parse URL: %w", err)
	}
	if u.Scheme != "ftp" && u.Scheme != "ftps" {
		return nil, fmt.Errorf("ftpstream: unsupported scheme %q (want ftp or ftps)", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("ftpstream: missing host in URL %q", rawURL)
	}
	port := u.Port()
	if port == "" {
		port = "21"
	}
	user, pass := "anonymous", ""
	if u.User != nil {
		if n := u.User.Username(); n != "" {
			user = n
		}
		pass, _ = u.User.Password()
	}
	return &ftpParsed{
		addr: host + ":" + port,
		host: host,
		user: user,
		pass: pass,
		path: u.Path,
		tls:  u.Scheme == "ftps",
	}, nil
}

// ftpIdleTimeout bounds how long any single read or write on an FTP control
// or data connection may block with no progress. It is a variable so tests can
// shorten it.
var ftpIdleTimeout = 30 * time.Second

// ftpDialTimeout bounds the TCP connect of each FTP control/data connection.
const ftpDialTimeout = 10 * time.Second

// ftpAbortGrace bounds the read of the server's final reply to a transfer
// whose request context was cancelled (an aborted range request). The context
// can no longer bound the read, so a short grace does: long enough for a
// healthy server to notice the dropped data connection and answer (426/226),
// short enough that a stalled one cannot hold the handler. It is a variable so
// tests can shorten it.
var ftpAbortGrace = 2 * time.Second

// deadlineConn re-arms a per-operation deadline before every Read and Write so
// a stalling server cannot block the caller forever. The deadline is the
// earliest of now+idle, the deadline of the request context the session is
// currently leased to (when it has one) and, while a cancelled transfer is
// being settled, the short abort grace that replaces the dead context's.
type deadlineConn struct {
	net.Conn
	idle time.Duration
	sess *ftpSession
}

func (c *deadlineConn) arm() {
	dl := time.Now().Add(c.idle)
	if l := c.sess.lease.Load(); l != nil {
		bound := l.ctxDL
		if st := l.settleDL.Load(); st != 0 {
			bound = time.Unix(0, st)
		}
		if !bound.IsZero() && bound.Before(dl) {
			dl = bound
		}
	}
	_ = c.SetDeadline(dl)
}

// Close closes the wrapped connection. One the session's cancel hook already
// force-closed is not an error: otherwise ftp.Response.Close would report the
// double close next to the server's final reply and hide that reply from
// ftpReplySynced.
func (c *deadlineConn) Close() error {
	if err := c.Conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (c *deadlineConn) Read(p []byte) (int, error) {
	c.arm()
	return c.Conn.Read(p)
}

func (c *deadlineConn) Write(p []byte) (int, error) {
	c.arm()
	return c.Conn.Write(p)
}

// ftpConns tracks every raw connection of one FTP session so ctx cancellation
// can force-close them all, unblocking any goroutine stuck in a Read.
type ftpConns struct {
	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

// add registers c. It reports false (and closes c) when the set was already
// force-closed.
func (t *ftpConns) add(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = c.Close()
		return false
	}
	t.conns = append(t.conns, c)
	return true
}

func (t *ftpConns) closeAll() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for _, c := range t.conns {
		_ = c.Close()
	}
	t.conns = nil
}

// isClosed reports whether closeAll ran.
func (t *ftpConns) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// ftpReadCloser wraps an FTP data response and the session it was opened on.
// Close drains the data response and, when the control connection is provably
// still in sync, parks the session in the idle pool for the next request
// instead of quitting it.
type ftpReadCloser struct {
	resp *ftp.Response
	sess *ftpSession
	stop func() bool // detaches the ctx-cancellation hook

	once sync.Once
	err  error
}

func (f *ftpReadCloser) Read(p []byte) (int, error) {
	return f.resp.Read(p)
}

// Close is idempotent: the session must be released exactly once.
//
// stop reports false when the ctx hook already fired (or is firing), i.e. the
// request was aborted mid-transfer — the usual way a player seeks. The hook
// then closed only the data connection (see bind), so the control connection
// is intact and the session still poolable: the server's reply to the
// abandoned transfer is read, within ftpAbortGrace rather than the dead
// request context, and the session is parked when that reply proves the
// control connection in sync. Anything else — no reply in time, a transport
// error, a preliminary or 421 reply, a closed control connection — discards.
func (f *ftpReadCloser) Close() error {
	f.once.Do(func() {
		if !f.stop() {
			f.sess.settle(ftpAbortGrace)
		}
		f.err = f.resp.Close()
		if ftpReplySynced(f.err) && !f.sess.ctl.isClosed() {
			ftpPoolPut(f.sess)
		} else {
			f.sess.discard()
		}
	})
	return f.err
}

// openFTPAt is openFTP with the OpenRanged size policy (see there); pos is the
// position the reader starts at.
//
// A logged-in control connection is taken from the idle pool when one exists
// for this (server, credentials) and otherwise dialled; either way every
// control and data connection carries a per-operation deadline
// (ftpIdleTimeout) and is force-closed when ctx is cancelled, so neither a
// stalling server nor a disconnected client can pin the goroutine and the FTP
// session. A reused connection that turns out stale (server idle timeout,
// restart, 421) is discarded and the open retried once on a fresh connection;
// a healthy session that merely met a refusal (see ftpSessionHealthy) is
// returned to the pool and the refusal reported as is.
func openFTPAt(ctx context.Context, rawURL string, offset int64, requireSize bool) (io.ReadCloser, int64, int64, error) {
	p, err := parseFTPURL(rawURL)
	if err != nil {
		return nil, -1, 0, err
	}
	allowPrivate := ftpAllowPrivate()
	key := ftpPoolKey{
		addr:         p.addr,
		user:         p.user,
		passSum:      sha256.Sum256([]byte(p.pass)),
		tls:          p.tls,
		allowPrivate: allowPrivate,
	}

	// dialFresh runs the (cached) SSRF pre-flight and opens on a new session.
	dialFresh := func() (io.ReadCloser, int64, int64, error) {
		if err := preflightHost(ctx, p.host, !allowPrivate); err != nil {
			return nil, -1, 0, fmt.Errorf("ftpstream: %w", err)
		}
		return (&ftpSession{key: key}).open(ctx, p, offset, requireSize)
	}

	s := ftpPoolGet(key)
	if s == nil {
		return dialFresh()
	}
	rc, size, pos, err := s.open(ctx, p, offset, requireSize)
	if err != nil && ctx.Err() == nil && !ftpSessionHealthy(err) {
		logging.For("ftpstream").Debug("pooled ftp session unusable, redialling", "err", err)
		return dialFresh()
	}
	return rc, size, pos, err
}

// open binds the session to ctx and starts the transfer: connect+login when
// the session is new, SIZE (cached for seeks, always fresh for offset 0), then
// RETR (with REST when offset > 0). On any error the session is discarded,
// except when the server merely refused the transfer and the session is
// provably healthy (ftpSessionHealthy): it then goes back to the pool.
func (s *ftpSession) open(ctx context.Context, p *ftpParsed, offset int64, requireSize bool) (io.ReadCloser, int64, int64, error) {
	stop := s.bind(ctx)
	fail := func(err error) (io.ReadCloser, int64, int64, error) {
		stop()
		s.discard()
		return nil, -1, 0, err
	}
	// refused ends the open on a failure that left the control connection in
	// sync. stop reports false when ctx fired first, which closed it.
	refused := func(err error) (io.ReadCloser, int64, int64, error) {
		if stop() {
			ftpPoolPut(s)
		} else {
			s.discard()
		}
		return nil, -1, 0, err
	}

	if s.conn == nil {
		if err := s.connect(p); err != nil {
			return fail(err)
		}
	}

	// Best-effort size; -1 when the server does not support SIZE or the command
	// fails (e.g., the path does not exist — RETR will surface the real error).
	sk := ftpSizeKey{addr: p.addr, user: p.user, path: p.path}
	size, cached := int64(-1), false
	if offset > 0 {
		size, cached = ftpSizeGet(sk)
	}
	if !cached {
		size = -1
		if sz, ferr := s.conn.FileSize(p.path); ferr == nil && sz >= 0 {
			size = sz
			ftpSizePut(sk, sz)
		}
	}
	if requireSize && offset > 0 && size < 0 {
		offset = 0
	}

	var resp *ftp.Response
	var err error
	if offset > 0 {
		resp, err = s.conn.RetrFrom(p.path, uint64(offset))
	} else {
		resp, err = s.conn.Retr(p.path)
	}
	if err != nil {
		ftpSizeForget(sk)
		err = fmt.Errorf("ftpstream: RETR %s: %w", p.path, err)
		if ftpSessionHealthy(err) {
			return refused(err)
		}
		return fail(err)
	}
	if cerr := ctx.Err(); cerr != nil {
		_ = resp.Close()
		return fail(fmt.Errorf("ftpstream: %w", cerr))
	}

	// From here no control-connection read is pending until Close: a ctx
	// cancel need only tear down the data connection. beginTransfer loses
	// only to a hook that already closed the control connection.
	if !s.beginTransfer() {
		_ = resp.Close()
		return fail(fmt.Errorf("ftpstream: %w", ctx.Err()))
	}
	return &ftpReadCloser{resp: resp, sess: s, stop: stop}, size, offset, nil
}

// ErrRangeNotSatisfiable is returned (wrapped) by Open when the requested
// offset lies beyond the end of the resource.
var ErrRangeNotSatisfiable = errors.New("requested range not satisfiable")

// openHTTP opens an HTTP or HTTPS connection for rawURL, positioned at offset.
//
// When offset > 0, a Range header is sent. The total resource size is inferred
// from Content-Range (206 response) or Content-Length (200 response).
func openHTTP(ctx context.Context, rawURL string, offset int64) (io.ReadCloser, int64, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, -1, fmt.Errorf("ftpstream: parse URL: %w", err)
	}
	if err := preflightHost(ctx, u.Hostname(), !ftpAllowPrivate()); err != nil {
		return nil, -1, fmt.Errorf("ftpstream: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, -1, fmt.Errorf("ftpstream: build request for %s: %w", rawURL, err)
	}
	if offset > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	}

	resp, err := httpGuardedClient.Do(req)
	if err != nil {
		return nil, -1, fmt.Errorf("ftpstream: GET %s: %w", rawURL, err)
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		_ = resp.Body.Close()
		return nil, -1, fmt.Errorf("ftpstream: GET %s: %w", rawURL, ErrRangeNotSatisfiable)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		_ = resp.Body.Close()
		return nil, -1, fmt.Errorf("ftpstream: GET %s: unexpected status %d", rawURL, resp.StatusCode)
	}
	// Range ignored: body starts at 0, so skip to the requested offset. When
	// the offset is already past a known Content-Length, leave the body alone
	// so the caller can answer 416 using the reported size.
	if offset > 0 && resp.StatusCode == http.StatusOK && (resp.ContentLength < 0 || offset < resp.ContentLength) {
		if _, derr := io.CopyN(io.Discard, resp.Body, offset); derr != nil {
			_ = resp.Body.Close()
			if errors.Is(derr, io.EOF) {
				return nil, -1, fmt.Errorf("ftpstream: GET %s: %w", rawURL, ErrRangeNotSatisfiable)
			}
			return nil, -1, fmt.Errorf("ftpstream: GET %s: skip to offset: %w", rawURL, derr)
		}
	}

	size := int64(-1)
	// Content-Range is present on 206 responses: "bytes start-end/total"
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			if n, perr := strconv.ParseInt(cr[i+1:], 10, 64); perr == nil && n >= 0 {
				size = n
			}
		}
	}
	// Fall back to Content-Length; for a full (200) response this is the total
	// size; for a partial (206) response it is the remaining byte count.
	if size < 0 && resp.ContentLength > 0 {
		if resp.StatusCode == http.StatusOK {
			size = resp.ContentLength
		} else {
			// 206: total = start_offset + remaining
			size = offset + resp.ContentLength
		}
	}

	return resp.Body, size, nil
}

// OpenRanged is Open for callers that can only answer a ranged request when
// the total size is known (a 206 needs "Content-Range: bytes a-b/total"). It
// opens at offset when the size is known; when offset > 0 and the size cannot
// be determined it opens from byte 0 instead and reports that in pos, so the
// caller serves the full body without a wasted seek-then-reopen. For FTP the
// size is learned (SIZE) before the transfer starts, so no second open is
// ever needed; HTTP only learns it from the response and reopens in that
// rare case.
//
// pos is the byte position the returned reader starts at: offset, or 0 on the
// unknown-size fallback.
func OpenRanged(ctx context.Context, rawURL string, offset int64) (rc io.ReadCloser, size, pos int64, err error) {
	return openAt(ctx, rawURL, offset, true)
}

func openAt(ctx context.Context, rawURL string, offset int64, requireSize bool) (io.ReadCloser, int64, int64, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, -1, 0, fmt.Errorf("ftpstream: parse URL: %w", err)
	}
	switch u.Scheme {
	case "ftp", "ftps":
		return openFTPAt(ctx, rawURL, offset, requireSize)
	case "http", "https":
		rc, size, herr := openHTTP(ctx, rawURL, offset)
		if herr != nil {
			return nil, -1, 0, herr
		}
		if requireSize && offset > 0 && size < 0 {
			// No total size => no valid Content-Range; the caller serves
			// the whole resource, so start over from byte 0.
			_ = rc.Close()
			rc, size, herr = openHTTP(ctx, rawURL, 0)
			if herr != nil {
				return nil, -1, 0, herr
			}
			return rc, size, 0, nil
		}
		return rc, size, offset, nil
	default:
		return nil, -1, 0, fmt.Errorf("ftpstream: unsupported URL scheme %q", u.Scheme)
	}
}
