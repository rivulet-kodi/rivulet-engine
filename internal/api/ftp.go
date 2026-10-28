// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — FTP/FTPS and HTTP(S) direct streaming via the /ftp route.
//
// GET|HEAD /ftp/{filename}?lz=<encoded>
//
// The lz parameter is an lz-string (compressToEncodedURIComponent) encoded
// JSON object {"ftpUrl":"<url>"} where url may be ftp://, ftps://, http://, or
// https://. The file is streamed to the client with Accept-Ranges / 206 partial
// content support when the total resource size is known.
//
// Session routes respond 501 — the direct ?lz route is the supported path:
//
//	/ftp/create, /ftp/create/{key}
//	/ftp/stream, /ftp/stream/{key}/{file...}
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	lzstring "github.com/daku10/go-lz-string"

	"github.com/rivulet-kodi/rivulet-engine/internal/ftpstream"
)

// ftpMaxLZEncoded is the maximum accepted byte length of a raw ?lz= query
// value before decompression. Prevents memory blowup before decode begins.
const ftpMaxLZEncoded = 1 << 20 // 1 MiB

// ftpMaxLZDecoded is the maximum byte length of the JSON string produced
// by lz-string decompression. Guards against decompression bombs.
const ftpMaxLZDecoded = 8 << 20 // 8 MiB

// ftpPayload is the JSON structure carried in the lz-encoded query parameter.
type ftpPayload struct {
	FtpURL string `json:"ftpUrl"`
}

// ftpDLNAHeaders sets the DLNA streaming headers required by Stremio clients.
func ftpDLNAHeaders(h http.Header) {
	h.Set("transferMode.dlna.org", "Streaming")
	h.Set("contentFeatures.dlna.org", "DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000")
}

// handleFTP dispatches /ftp/* requests.
//
// Supported route:
//
//	GET|HEAD /ftp/{filename}?lz=<encoded>  — stream the URL encoded in lz
//
// Intentionally unsupported routes (501):
//
//	GET|POST /ftp/create
//	GET|POST /ftp/create/{key}
//	GET|POST /ftp/stream
//	GET|POST /ftp/stream/{key}/{file...}
func (s *server) handleFTP(w http.ResponseWriter, r *http.Request, seg []string) {
	// seg[0] == "ftp"; seg[1] is the next path component (if present).
	if len(seg) >= 2 && (seg[1] == "create" || seg[1] == "stream") {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error": "ftp session routes are not supported; use GET /ftp/{filename}?lz=<encoded>",
		})
		return
	}

	if len(seg) < 2 || seg[1] == "" {
		http.NotFound(w, r)
		return
	}

	filename := seg[1]

	lzParam := r.URL.Query().Get("lz")
	if lzParam == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "lz query parameter is required",
		})
		return
	}
	if len(lzParam) > ftpMaxLZEncoded {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": fmt.Sprintf("lz param too large (%d bytes; limit %d)", len(lzParam), ftpMaxLZEncoded),
		})
		return
	}

	jsonStr, err := lzstring.DecompressFromEncodedURIComponent(lzParam)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "lz decode error: " + err.Error(),
		})
		return
	}
	if len(jsonStr) > ftpMaxLZDecoded {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": fmt.Sprintf("lz decoded payload too large (%d bytes; limit %d)", len(jsonStr), ftpMaxLZDecoded),
		})
		return
	}

	var payload ftpPayload
	if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "invalid JSON in lz payload: " + err.Error(),
		})
		return
	}
	if payload.FtpURL == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "ftpUrl is required in the lz payload",
		})
		return
	}

	// Seek to the requested start for the common case (FTP REST / HTTP Range).
	// A valid 206 Content-Range needs the total size, so when the size cannot
	// be determined OpenRanged opens from byte 0 itself (pos != start) and we
	// serve a 200 instead — no second open of the resource.
	rangeHdr := r.Header.Get("Range")
	start, hasRange := ftpExtractRangeStart(rangeHdr)

	rc, size, pos, err := ftpstream.OpenRanged(r.Context(), payload.FtpURL, start)
	if errors.Is(err, ftpstream.ErrRangeNotSatisfiable) {
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if err != nil {
		http.Error(w, "stream open: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = rc.Close() }()
	if pos != start {
		hasRange = false
	}

	hdr := w.Header()
	hdr.Set("Content-Type", mimeByName(filename))
	hdr.Set("Accept-Ranges", "bytes")
	ftpDLNAHeaders(hdr)

	// Serve 206 Partial Content when the range is satisfiable and the total
	// resource size is known.
	if hasRange && size >= 0 {
		rStart, rEnd, ok, unsat := parseRange(rangeHdr, size)
		if unsat {
			hdr.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if ok {
			copyLen := rEnd - rStart + 1
			hdr.Set("Content-Length", strconv.FormatInt(copyLen, 10))
			hdr.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rStart, rEnd, size))
			w.WriteHeader(http.StatusPartialContent)
			if r.Method == http.MethodHead {
				return
			}
			bufp := streamBufPool.Get().(*[]byte)
			defer streamBufPool.Put(bufp)
			copyStream(w, io.LimitReader(rc, copyLen), *bufp)
			return
		}
	}

	// 200 full or unbounded streaming response.
	if size >= 0 {
		hdr.Set("Content-Length", strconv.FormatInt(size, 10))
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	bufp := streamBufPool.Get().(*[]byte)
	defer streamBufPool.Put(bufp)
	copyStream(w, rc, *bufp)
}

// ftpExtractRangeStart parses the start byte offset from a Range header for
// use as the initial read position before the total resource size is known.
//
// Only explicit start ranges ("bytes=N-" or "bytes=N-M") are supported.
// Suffix ranges ("bytes=-N") return ok=false because the absolute start byte
// cannot be determined without the total size.
//
// The end token, if present, must also parse as a valid integer. parseRange
// treats a malformed end token (e.g. "bytes=100-xyz") as if the Range header
// were absent entirely (RFC 7233), which yields a 200 response covering the
// full resource; if this function reported ok=true for such a header, the
// reader would already be seeked past byte 0 while the response claimed the
// full Content-Length, truncating the body. Rejecting a malformed end token
// here keeps the two interpretations in agreement.
func ftpExtractRangeStart(h string) (start int64, ok bool) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	// Multi-range: use only the first range.
	if i := strings.IndexByte(spec, ','); i >= 0 {
		spec = spec[:i]
	}
	dash := strings.IndexByte(spec, '-')
	// dash <= 0: no dash found, or suffix range like "bytes=-N" (dash at index 0).
	if dash <= 0 {
		return 0, false
	}
	s, err := strconv.ParseInt(spec[:dash], 10, 64)
	if err != nil || s < 0 {
		return 0, false
	}
	if end := spec[dash+1:]; end != "" {
		if _, err := strconv.ParseInt(end, 10, 64); err != nil {
			return 0, false
		}
	}
	return s, true
}
