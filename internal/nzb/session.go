// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package nzb

import (
	"errors"
	"fmt"
	"io"
	"net"
)

// nzbDefaultMaxBytes is the write-cap applied when the NZB declares no segment
// sizes (or their sum is non-positive). 50 GiB comfortably covers the largest
// legitimate Usenet releases while still preventing disk exhaustion from
// adversarial or corrupt NZB data.
const nzbDefaultMaxBytes = 50 << 30

// Session holds the NNTP server configuration and the parsed file list for one
// NZB job. StartAssembly is its download entry point: each assembly dials its
// own connections.
type Session struct {
	cfg   ServerConfig
	files []File
}

// NewSession creates a Session from server configuration and pre-parsed files.
func NewSession(cfg ServerConfig, files []File) *Session {
	return &Session{cfg: cfg, files: files}
}

// file returns the File named name, or an error when the NZB has none.
func (sess *Session) file(name string) (*File, error) {
	for i := range sess.files {
		if sess.files[i].Name == name {
			return &sess.files[i], nil
		}
	}
	return nil, fmt.Errorf("nzb: file %q not found in NZB", name)
}

// sizeCap is the write-cap for file f: the declared size, or
// nzbDefaultMaxBytes when the NZB omits or zeroes segment sizes so the cap is
// never disabled by missing metadata.
func sizeCap(f *File) int64 {
	if f.Size <= 0 {
		return nzbDefaultMaxBytes
	}
	return f.Size
}

// isConnErr reports whether err indicates a broken or closed underlying TCP
// connection (EOF, broken pipe, timeout, connection reset) rather than an NNTP
// protocol-level error (e.g. 430 No Such Article). Used to gate reconnects.
func isConnErr(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne)
}
