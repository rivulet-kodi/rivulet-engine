// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package nzb — regression tests for SEC-2 (review 2026-09-22): Dial dialed
// the client-supplied NNTP host/port with a bare net.Dialer{Timeout:
// dialTimeout} and no Control hook at all, so a server list pointing at an
// internal host reached it with no guard whatsoever (unlike nzbUrl, which
// api/nzb.go pre-validated with validateFetchHost). ServerConfig.Control lets
// the caller (api/nzb.go) wire netguard.DialControl(!archiveAllowPrivateHosts())
// into the dial itself, closing that gap.
//
// These tests use a real net.Listener bound to 127.0.0.1 with a fake NNTP
// greeting (serveDial, defined in nzb_extra_test.go) — no real network I/O
// beyond loopback, deterministic and offline.
package nzb

import (
	"net"
	"testing"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// TestDial_ControlBlocksLoopbackByDefault proves that when Control is set to
// the same guard api/nzb.go wires in by default (blockPrivate=true, i.e.
// RIVULET_ARCHIVE_ALLOW_PRIVATE unset), Dial refuses a loopback target before
// ever reaching the fake server's protocol handshake.
func TestDial_ControlBlocksLoopbackByDefault(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	accepted := false
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted = true
			_ = conn.Close()
		}
	}()

	port := ln.Addr().(*net.TCPAddr).Port
	_, err = dial(ServerConfig{
		Host:    "127.0.0.1",
		Port:    port,
		Control: netguard.DialControl(true),
	})
	if err == nil {
		t.Fatal("Dial with blockPrivate Control on loopback = nil error, want blocked")
	}
	if accepted {
		t.Error("listener accepted a connection; the Control hook should have blocked the dial before connect")
	}
}

// TestDial_ControlAllowsLoopbackWhenOptedIn proves that when Control is set
// to the opted-in guard (blockPrivate=false, i.e. RIVULET_ARCHIVE_ALLOW_PRIVATE
// set), Dial still reaches a loopback server and completes the handshake
// normally.
func TestDial_ControlAllowsLoopbackWhenOptedIn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	serveDial(t, ln, "200 Welcome", "205 Closing")

	port := ln.Addr().(*net.TCPAddr).Port
	c, err := dial(ServerConfig{
		Host:    "127.0.0.1",
		Port:    port,
		Control: netguard.DialControl(false),
	})
	if err != nil {
		t.Fatalf("Dial with allow-private Control on loopback = %v, want success", err)
	}
	_ = c.Close()
}

// TestDial_NoControlReachesLoopback proves the zero-value ServerConfig (no
// Control set) behaves exactly as before this fix — trusted-caller code
// paths that never set Control are unaffected.
func TestDial_NoControlReachesLoopback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	serveDial(t, ln, "200 Welcome", "205 Closing")

	port := ln.Addr().(*net.TCPAddr).Port
	c, err := dial(ServerConfig{Host: "127.0.0.1", Port: port})
	if err != nil {
		t.Fatalf("Dial with nil Control on loopback = %v, want success", err)
	}
	_ = c.Close()
}
