// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package ftpstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jlaffaye/ftp"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// A player seeking inside a remote file issues one short ranged request per
// seek. Dialling, greeting, USER/PASS/FEAT/TYPE and SIZE cost ~6 round trips
// each time, so logged-in control connections are kept in a small idle pool
// and the SIZE answer is cached for a few seconds. Follows the repo's session
// cache pattern: package-level map + mutex + a janitor started once.
const (
	// ftpPoolTTL is how long an idle control connection may sit in the pool.
	// It stays far below the idle timeouts of common servers (vsftpd 300s,
	// FileZilla 120s) so a pooled connection is almost always still alive;
	// a stale one is detected on first use and transparently replaced.
	ftpPoolTTL = 30 * time.Second
	// ftpPoolMaxIdle bounds idle connections across all servers and
	// ftpPoolMaxPerKey per (server, credentials) so we never hog the
	// per-client connection slots small FTP servers hand out.
	ftpPoolMaxIdle   = 8
	ftpPoolMaxPerKey = 2

	// ftpSizeTTL is how long a SIZE answer is trusted for seeks. A request
	// at offset 0 (the start of a playback) always refreshes it.
	ftpSizeTTL        = 15 * time.Second
	ftpSizeMaxEntries = 256

	// preflightTTL caches the SSRF pre-flight verdict of a host. The dialer
	// Control hook still validates the actual IP of every dial.
	preflightTTL        = 30 * time.Second
	preflightMaxEntries = 256

	ftpJanitorEvery = 10 * time.Second
)

// ftpPoolKey identifies interchangeable logged-in control connections: same
// server, same credentials, same TLS mode and the same private-address policy
// the connection was dialled under (so flipping RIVULET_FTP_ALLOW_PRIVATE
// never reuses a connection dialled under the other policy).
type ftpPoolKey struct {
	addr         string
	user         string
	pass         string
	tls          bool
	allowPrivate bool
}

type ftpSizeKey struct{ addr, user, path string }

type ftpSizeEntry struct {
	size    int64
	expires time.Time
}

type preflightKey struct {
	host         string
	blockPrivate bool
}

type preflightEntry struct {
	err     error
	expires time.Time
}

var (
	ftpPoolMu   sync.Mutex
	ftpPool     = map[ftpPoolKey][]*ftpSession{}
	ftpPoolIdle int // total idle sessions; guarded by ftpPoolMu

	ftpMetaMu  sync.Mutex // guards ftpSizes and preflights
	ftpSizes   = map[ftpSizeKey]ftpSizeEntry{}
	preflights = map[preflightKey]preflightEntry{}

	ftpJanitorOnce sync.Once
)

// ftpStartJanitor starts the background sweeper (at most once).
func ftpStartJanitor() {
	ftpJanitorOnce.Do(func() {
		go func() {
			t := time.NewTicker(ftpJanitorEvery)
			defer t.Stop()
			for now := range t.C {
				ftpPoolEvict(now)
				ftpMetaEvict(now)
			}
		}()
	})
}

// Phases of an ftpLease (ftpLease.phase).
const (
	// leaseOpening: connecting, logging in, SIZE, RETR. A goroutine may be
	// blocked reading the control connection.
	leaseOpening int32 = iota
	// leaseTransferring: the stream is in the caller's hands. No control
	// read is pending until Close reads the final reply.
	leaseTransferring
	// leaseAborted: the context fired while opening and the hook closed
	// the control connection; the session is dead.
	leaseAborted
)

// ftpLease is the per-request binding of a (possibly reused) session: the
// request context, its deadline and the data connections of that transfer.
type ftpLease struct {
	ctx   context.Context
	ctxDL time.Time
	data  ftpConns

	// phase decides what the ctx hook may tear down. Moving out of
	// leaseOpening is a compare-and-swap, so exactly one of the hook
	// (-> leaseAborted, closes the control connection) and beginTransfer
	// (-> leaseTransferring, the control connection is then never closed by
	// the hook) wins.
	phase atomic.Int32
	// settleDL, when non-zero (unix nanoseconds), replaces ctxDL as the
	// bound of control reads: the context is dead, but the reply to an
	// aborted transfer is still read, within ftpAbortGrace.
	settleDL atomic.Int64
}

// ftpSession is one logged-in FTP control connection plus the dial state
// needed to open data connections on it. It is owned by exactly one request
// at a time (ServerConn is not concurrency-safe) or sits idle in ftpPool.
type ftpSession struct {
	key       ftpPoolKey
	conn      *ftp.ServerConn // nil until connected
	ctl       ftpConns        // the control connection; closing it kills the session
	ctlDialed bool            // request-goroutine only: first dial is the control connection
	lease     atomic.Pointer[ftpLease]
	idleSince time.Time // guarded by ftpPoolMu
}

// bind attaches the session to ctx for one transfer. The returned stop
// detaches the cancellation hook and reports whether it did so before the hook
// fired; on false the hook ran (or is running) and the session's data
// connections were force-closed.
//
// While the session is opening, a goroutine may be blocked reading the
// control connection, so the hook closes it as well and the session is dead.
// Once the transfer is under way (beginTransfer) the hook closes only the
// data connections — enough to unblock a Read — and leaves the control
// connection alone: the request being aborted is the normal way a player
// seeks, and the session stays reusable if the server's reply to the
// abandoned transfer can still be read (see ftpReadCloser.Close).
func (s *ftpSession) bind(ctx context.Context) (stop func() bool) {
	l := &ftpLease{ctx: ctx}
	l.ctxDL, _ = ctx.Deadline()
	s.lease.Store(l)
	return context.AfterFunc(ctx, func() {
		l.data.closeAll()
		if l.phase.CompareAndSwap(leaseOpening, leaseAborted) {
			s.ctl.closeAll()
		}
	})
}

// beginTransfer moves the current lease from opening to transferring (see
// ftpLease.phase). It reports false when the ctx hook got there first and
// closed the control connection: the session is dead.
func (s *ftpSession) beginTransfer() bool {
	l := s.lease.Load()
	return l != nil && l.phase.CompareAndSwap(leaseOpening, leaseTransferring)
}

// settle bounds control reads of the current lease by grace from now instead
// of by its (cancelled) context.
func (s *ftpSession) settle(grace time.Duration) {
	if l := s.lease.Load(); l != nil {
		l.settleDL.Store(time.Now().Add(grace).UnixNano())
	}
}

// connect dials the control connection and logs in. Every dial — the control
// connection and each passive data connection — goes through a dialer whose
// Control hook is netguard.DialControl, so the SSRF guard applies per dial no
// matter how long the session lives.
func (s *ftpSession) connect(p *ftpParsed) error {
	dialer := net.Dialer{
		Timeout: ftpDialTimeout,
		Control: netguard.DialControl(!s.key.allowPrivate),
	}
	var tlsCfg *tls.Config
	if p.tls {
		tlsCfg = &tls.Config{ServerName: p.host}
	}
	// dialFunc serves the control connection (first call) and every data
	// connection, always bound to the current lease's context.
	dialFunc := func(network, address string) (net.Conn, error) {
		l := s.lease.Load()
		if l == nil {
			return nil, errors.New("ftpstream: session not bound to a request")
		}
		raw, derr := dialer.DialContext(l.ctx, network, address)
		if derr != nil {
			if s.ctlDialed {
				// A data connection: the PASV/EPSV reply that named it
				// was read, so the control connection is still in sync.
				return nil, &ftpDataDialError{err: derr}
			}
			return nil, derr
		}
		set := &l.data
		if !s.ctlDialed {
			s.ctlDialed = true
			set = &s.ctl
		}
		if !set.add(raw) {
			if cerr := l.ctx.Err(); cerr != nil {
				return nil, cerr
			}
			return nil, errors.New("ftpstream: connection closed")
		}
		c := raw
		if tlsCfg != nil {
			// Handshake is deferred to the first Read/Write, which the
			// deadline wrapper below bounds.
			c = tls.Client(raw, tlsCfg)
		}
		return &deadlineConn{Conn: c, idle: ftpIdleTimeout, sess: s}, nil
	}

	conn, err := ftp.Dial(p.addr, ftp.DialWithDialFunc(dialFunc))
	if err != nil {
		return fmt.Errorf("ftpstream: dial %s: %w", p.addr, err)
	}
	s.conn = conn
	if err := conn.Login(p.user, p.pass); err != nil {
		return fmt.Errorf("ftpstream: login as %q: %w", p.user, err)
	}
	return nil
}

// discard tears the session down: polite QUIT (best effort, not awaited) and a
// force-close of every connection. Safe to call more than once.
func (s *ftpSession) discard() {
	if s.conn != nil {
		_ = s.conn.Quit()
	}
	if l := s.lease.Swap(nil); l != nil {
		l.data.closeAll()
	}
	s.ctl.closeAll()
}

// ftpReplySynced reports whether err, as returned by Response.Close, proves
// the control connection is still in sync with the server: either no error,
// or a complete final server reply (e.g. 426 after an aborted transfer) was
// read. A transport error, a preliminary 1xx reply or 421 (server closing)
// means the session must not be reused.
func ftpReplySynced(err error) bool {
	if err == nil {
		return true
	}
	var te *textproto.Error
	return errors.As(err, &te) && te.Code >= 200 && te.Code != 421
}

// ftpDataDialError marks the failure to dial a passive data connection (an
// unreachable port, a dial the SSRF guard rejects, a cancelled context). The
// control connection is not at fault.
type ftpDataDialError struct{ err error }

func (e *ftpDataDialError) Error() string { return e.err.Error() }
func (e *ftpDataDialError) Unwrap() error { return e.err }

// ftpReplyRefused reports whether err carries the server's complete negative
// reply to a command (4xx/5xx): the control connection is in sync and the
// server is still there. Not so 421 (service closing) and 530/532 (not logged
// in: a pooled session whose login the server dropped), which say the session
// itself is no good.
func ftpReplyRefused(err error) bool {
	var te *textproto.Error
	if !errors.As(err, &te) {
		return false
	}
	switch te.Code {
	case 421, 530, 532:
		return false
	}
	return te.Code >= 400
}

// ftpSessionHealthy reports whether a failed open proves the session itself
// healthy — the failure was the server refusing the request or a data
// connection that could not be dialled, not a dead control connection — so
// that it is worth keeping and not worth retrying on a fresh one.
func ftpSessionHealthy(err error) bool {
	var dd *ftpDataDialError
	return ftpReplyRefused(err) || errors.As(err, &dd)
}

// ftpPoolGet returns an idle session for key, or nil. The caller owns it.
func ftpPoolGet(key ftpPoolKey) *ftpSession {
	now := time.Now()
	var got *ftpSession
	var expired []*ftpSession
	ftpPoolMu.Lock()
	idle := ftpPool[key]
	for len(idle) > 0 {
		s := idle[len(idle)-1] // most recently used first
		idle = idle[:len(idle)-1]
		ftpPoolIdle--
		if now.Sub(s.idleSince) >= ftpPoolTTL {
			expired = append(expired, s)
			continue
		}
		got = s
		break
	}
	if len(idle) == 0 {
		delete(ftpPool, key)
	} else {
		ftpPool[key] = idle
	}
	ftpPoolMu.Unlock()
	for _, s := range expired {
		s.discard()
	}
	return got
}

// ftpPoolPut parks a healthy session for reuse. When a cap is reached the
// oldest idle session (of this key, then of the whole pool) is quit to make
// room: the session that was just used is the likeliest to be used again,
// while an old one may belong to a server nobody streams from any more.
func ftpPoolPut(s *ftpSession) {
	// Drop the finished request's context so an idle session retains
	// nothing from it.
	if l := s.lease.Swap(nil); l != nil {
		l.data.closeAll()
	}
	ftpStartJanitor()
	var evicted []*ftpSession
	ftpPoolMu.Lock()
	// Slices are appended in time order, so idle[0] is a key's oldest.
	if idle := ftpPool[s.key]; len(idle) >= ftpPoolMaxPerKey {
		evicted = append(evicted, idle[0])
		ftpPool[s.key] = idle[1:]
		ftpPoolIdle--
	}
	for ftpPoolIdle >= ftpPoolMaxIdle {
		var (
			oldKey ftpPoolKey
			oldAt  time.Time
			found  bool
		)
		for k, idle := range ftpPool {
			if len(idle) > 0 && (!found || idle[0].idleSince.Before(oldAt)) {
				oldKey, oldAt, found = k, idle[0].idleSince, true
			}
		}
		if !found {
			break
		}
		idle := ftpPool[oldKey]
		evicted = append(evicted, idle[0])
		if len(idle) == 1 {
			delete(ftpPool, oldKey)
		} else {
			ftpPool[oldKey] = idle[1:]
		}
		ftpPoolIdle--
	}
	s.idleSince = time.Now()
	ftpPool[s.key] = append(ftpPool[s.key], s)
	ftpPoolIdle++
	ftpPoolMu.Unlock()
	for _, v := range evicted {
		v.discard()
	}
}

// ftpPoolEvict discards idle sessions older than ftpPoolTTL as of now.
func ftpPoolEvict(now time.Time) {
	var expired []*ftpSession
	ftpPoolMu.Lock()
	for k, idle := range ftpPool {
		keep := idle[:0]
		for _, s := range idle {
			if now.Sub(s.idleSince) >= ftpPoolTTL {
				expired = append(expired, s)
				ftpPoolIdle--
				continue
			}
			keep = append(keep, s)
		}
		if len(keep) == 0 {
			delete(ftpPool, k)
		} else {
			ftpPool[k] = keep
		}
	}
	ftpPoolMu.Unlock()
	for _, s := range expired {
		s.discard()
	}
}

// ftpSizeGet returns the cached SIZE for k when still fresh.
func ftpSizeGet(k ftpSizeKey) (int64, bool) {
	ftpMetaMu.Lock()
	defer ftpMetaMu.Unlock()
	e, ok := ftpSizes[k]
	if !ok || !time.Now().Before(e.expires) {
		return -1, false
	}
	return e.size, true
}

func ftpSizePut(k ftpSizeKey, size int64) {
	ftpStartJanitor()
	now := time.Now()
	ftpMetaMu.Lock()
	defer ftpMetaMu.Unlock()
	if _, ok := ftpSizes[k]; !ok && len(ftpSizes) >= ftpSizeMaxEntries {
		ftpSizesTrimLocked(now)
	}
	ftpSizes[k] = ftpSizeEntry{size: size, expires: now.Add(ftpSizeTTL)}
}

func ftpSizeForget(k ftpSizeKey) {
	ftpMetaMu.Lock()
	delete(ftpSizes, k)
	ftpMetaMu.Unlock()
}

// ftpSizesTrimLocked makes room: drops expired entries, then an arbitrary one
// if the map is still full. ftpMetaMu must be held.
func ftpSizesTrimLocked(now time.Time) {
	for k, e := range ftpSizes {
		if !now.Before(e.expires) {
			delete(ftpSizes, k)
		}
	}
	for k := range ftpSizes {
		if len(ftpSizes) < ftpSizeMaxEntries {
			break
		}
		delete(ftpSizes, k)
	}
}

// preflightGet returns a cached pre-flight verdict for k.
func preflightGet(k preflightKey) (hit bool, verdict error) {
	ftpMetaMu.Lock()
	defer ftpMetaMu.Unlock()
	e, ok := preflights[k]
	if !ok || !time.Now().Before(e.expires) {
		return false, nil
	}
	return true, e.err
}

func preflightPut(k preflightKey, verdict error) {
	ftpStartJanitor()
	now := time.Now()
	ftpMetaMu.Lock()
	defer ftpMetaMu.Unlock()
	if _, ok := preflights[k]; !ok && len(preflights) >= preflightMaxEntries {
		for pk, e := range preflights {
			if !now.Before(e.expires) {
				delete(preflights, pk)
			}
		}
		for pk := range preflights {
			if len(preflights) < preflightMaxEntries {
				break
			}
			delete(preflights, pk)
		}
	}
	preflights[k] = preflightEntry{err: verdict, expires: now.Add(preflightTTL)}
}

// ftpMetaEvict drops expired size and pre-flight entries.
func ftpMetaEvict(now time.Time) {
	ftpMetaMu.Lock()
	defer ftpMetaMu.Unlock()
	for k, e := range ftpSizes {
		if !now.Before(e.expires) {
			delete(ftpSizes, k)
		}
	}
	for k, e := range preflights {
		if !now.Before(e.expires) {
			delete(preflights, k)
		}
	}
}
