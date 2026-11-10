// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — regression test for the SSRF pre-flight added to nzb.go's
// nzbCreate (shares validateFetchHost with archive.go; see archive_test.go).
package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestHandlerNZBCreate_BlocksPrivateURLByDefault verifies POST /nzb/create
// rejects a private-address nzbUrl by default and never invokes the local
// handler — closing the SSRF finding for the NZB-fetch path.
func TestHandlerNZBCreate_BlocksPrivateURLByDefault(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := newHandler(t)
	body := fmt.Sprintf(`{"servers":["nntp://news.example.com"],"nzbUrl":"%s/test.nzb"}`, srv.URL)
	req := httptest.NewRequest(http.MethodPost, "/nzb/create", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Errorf("status = %d; want %d; body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if called {
		t.Error("local handler was invoked; the NZB fetch should have been rejected before connecting")
	}
}
