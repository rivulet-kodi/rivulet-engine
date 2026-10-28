// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package ftpstream

import (
	"context"
	"io"
)

// Test-only wrappers. Production code calls the variants below them; these keep
// the simpler signatures for tests (see scripts/deadcode-check.sh).

// openFTP opens a data connection for path on the FTP server described by
// rawURL, positioned at offset bytes from the beginning.
func openFTP(ctx context.Context, rawURL string, offset int64) (io.ReadCloser, int64, error) {
	rc, size, _, err := openFTPAt(ctx, rawURL, offset, false)
	return rc, size, err
}

// Open returns a ReadCloser for the resource at rawURL starting at byte offset
// (0 = from the beginning), the total resource size if known (−1 otherwise),
// and any error.
//
// Supported schemes: ftp, ftps, http, https.
// The caller must close the returned ReadCloser when done.
func Open(ctx context.Context, rawURL string, offset int64) (rc io.ReadCloser, size int64, err error) {
	rc, size, _, err = openAt(ctx, rawURL, offset, false)
	return rc, size, err
}
