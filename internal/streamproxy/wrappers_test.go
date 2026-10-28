// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package streamproxy

import (
	"context"
	"net/http"
)

// Test-only wrappers. Production code calls the variants below them; these keep
// the simpler signatures for tests (see scripts/deadcode-check.sh).

// drmDecrypt dispatches segment decryption based on p.Method. It never
// modifies segment: the plaintext is a separate buffer (Method "" returns
// segment itself).
func drmDecrypt(h *Handler, p DecryptParams, segment []byte) ([]byte, error) {
	return drmDecryptBuf(h, p, segment, false)
}

// drmDecryptCBC decrypts a full HLS segment using AES-128-CBC and strips PKCS7
// padding. data is left untouched.
func drmDecryptCBC(key, iv, data []byte) ([]byte, error) {
	return drmCBC(key, iv, data, false)
}

// cachedFetch fetches rawurl, using the segment cache when configured.
// Returns body, response headers, HTTP status, and any error. When the
// in-flight buffering budget is exhausted it waits (bounded by ctx).
func (h *Handler) cachedFetch(ctx context.Context, rawurl string, hdr http.Header, proxyURL string) ([]byte, http.Header, int, error) {
	return h.cachedFetchMode(ctx, rawurl, hdr, proxyURL, true)
}
