// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// An upstream that trickles (or stops sending) must not pin archiveDownload
// until the size cap: it fails once a window falls below the minimum.
func TestArchiveDownloadAbortsStalledUpstream(t *testing.T) {
	t.Setenv("RIVULET_ARCHIVE_ALLOW_PRIVATE", "1")
	oldWin, oldMin := archiveStallWindow, archiveMinWindowBytes
	archiveStallWindow, archiveMinWindowBytes = 100*time.Millisecond, 1024
	t.Cleanup(func() { archiveStallWindow, archiveMinWindowBytes = oldWin, oldMin })

	cases := map[string]func(w http.ResponseWriter, r *http.Request){
		"trickle": func(w http.ResponseWriter, r *http.Request) {
			f := w.(http.Flusher)
			for {
				if _, err := w.Write([]byte{0}); err != nil {
					return
				}
				f.Flush()
				select {
				case <-r.Context().Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		},
		"silent": func(w http.ResponseWriter, r *http.Request) {
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(h))
			defer srv.Close()
			done := make(chan error, 1)
			go func() {
				_, err := archiveDownload(srv.URL)
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, errArchiveStalled) {
					t.Fatalf("err = %v, want errArchiveStalled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("archiveDownload did not abort a stalled upstream")
			}
		})
	}
}
