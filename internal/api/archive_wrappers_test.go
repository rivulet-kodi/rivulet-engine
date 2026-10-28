// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

// Test-only wrappers. Production code calls the variants below them; these keep
// the simpler signatures for tests (see scripts/deadcode-check.sh).

// archiveExtractEntry extracts entryName from the session's archive to a temp
// file under sess.tmpDir and returns its path once the extraction is complete.
// Subsequent calls return the cached path without re-extraction; concurrent
// calls share one extraction. A failed extraction is not cached, so a later
// request may retry. The stream handler does not use this: it serves the file
// while it is still being written (see archiveOpenEntry).
func archiveExtractEntry(sess *archiveSession, entryName string) (string, error) {
	fl, path, err := archiveStartExtract(sess, entryName)
	if err != nil {
		return "", err
	}
	if fl == nil {
		return path, nil
	}
	<-fl.done
	if fl.err != nil {
		return "", fl.err
	}
	return fl.path, nil
}
