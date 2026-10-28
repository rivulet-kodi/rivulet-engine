// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

func TestWriteEngineErrStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"too many engines", fmt.Errorf("engine: 3 engines active: %w", types.ErrTooManyEngines), http.StatusServiceUnavailable},
		{"invalid info hash", fmt.Errorf("engine: info hash: %w", types.ErrInvalidInfoHash), http.StatusBadRequest},
		{"other", fmt.Errorf("boom"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			writeEngineErr(rec, tc.err)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

// TestMediaRoutesMapCallerErrorsTo400: a bad media URL / HLS option is the
// caller's fault (400, JSON error body on the JSON routes), while any other
// prober failure stays 500. /probe used to answer 500 with the body `null`.
func TestMediaRoutesMapCallerErrorsTo400(t *testing.T) {
	bad := fmt.Errorf("unsupported URL scheme %q: %w", "", types.ErrInvalidMediaURL)
	for _, tc := range []struct {
		path string
		err  error
		want int
	}{
		{"/probe", bad, http.StatusBadRequest},
		{"/probe?url=http://x/y", fmt.Errorf("ffprobe exploded"), http.StatusInternalServerError},
		{"/hlsv2/probe", bad, http.StatusBadRequest},
		{"/opensubHash", bad, http.StatusBadRequest},
		{"/subtitlesTracks", bad, http.StatusBadRequest},
		{"/opensubHash?videoUrl=http://x/y", fmt.Errorf("HEAD failed"), http.StatusInternalServerError},
	} {
		h := New(newFakeEM(), &fakeSS{}, &fakeProber{probeErr: tc.err}, types.Config{
			HTTPPort: 11470, WebUI: "https://web.stremio.com/", AppPath: t.TempDir(),
		})
		rec := serve(t, h, http.MethodGet, tc.path, nil)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (body %q)", tc.path, rec.Code, tc.want, rec.Body.String())
		}
		if tc.path == "/probe" || strings.HasPrefix(tc.path, "/probe?") {
			var m map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil || m["error"] == nil {
				t.Errorf("%s: body %q is not a JSON error object", tc.path, rec.Body.String())
			}
		}
	}
}
