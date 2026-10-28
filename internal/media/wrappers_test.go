// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package media

import (
	"context"
	"net/http"
)

// Test-only wrappers. Production code calls the variants below them; these keep
// the simpler signatures for tests (see scripts/deadcode-check.sh).

// httpRangeGet performs a Range GET [from, to] and returns exactly
// to-from+1 bytes, or an error. A 15-second context timeout is applied and
// the read is bounded by the requested length, so a server that ignores the
// Range header cannot cause an OOM.
//
// A 206 response must carry a Content-Range (when present) matching the
// request. A 200 response means the server ignored Range and is streaming the
// whole file from byte 0: that is only usable for a request starting at 0;
// for any other offset it is rejected, since returning the file's prefix as
// the "tail" would silently produce a wrong OpenSubtitles hash.
func httpRangeGet(client *http.Client, url string, from, to int64) ([]byte, error) {
	return httpRangeGetCtx(context.Background(), client, url, from, to)
}

// resolve looks up the upstream URL for token, rejecting unknown or expired
// tokens.
func (r *mediaRelay) resolve(token string) (string, bool) {
	upstream, _, ok := r.resolveEntry(token)
	return upstream, ok
}
