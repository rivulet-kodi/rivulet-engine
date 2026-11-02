// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// fakePreparer is a fakeProber that also implements types.HLSPreparer.
type fakePreparer struct {
	*fakeProber
	enabled bool
	err     error // returned by every HLSPreparer method when set
	calls   []string
}

func (p *fakePreparer) HLSPrepareEnabled() bool { return p.enabled }

func (p *fakePreparer) PrepareHLS(id, audio string) (types.HLSPrepareStatus, error) {
	p.calls = append(p.calls, "prepare "+id+" "+audio)
	return types.HLSPrepareStatus{State: "running", Audio: "default", Renditions: []types.HLSPrepareRendition{}}, p.err
}

func (p *fakePreparer) HLSPrepareStatus(id string) (types.HLSPrepareStatus, error) {
	p.calls = append(p.calls, "status "+id)
	return types.HLSPrepareStatus{State: "idle", Renditions: []types.HLSPrepareRendition{}}, p.err
}

func (p *fakePreparer) StopPrepareHLS(id string) error {
	p.calls = append(p.calls, "stop "+id)
	return p.err
}

func newHandlerWithPreparer(p *fakePreparer) http.Handler {
	cfg := types.Config{
		HTTPPort:           11470,
		WebUI:              "https://web.stremio.com/",
		CreateMetadataWait: 90 * time.Second,
	}
	return New(newFakeEM(), &fakeSS{}, p, cfg)
}

func TestHandlerHLSPrepare(t *testing.T) {
	cases := []struct {
		name      string
		method    string
		path      string
		err       error
		wantCode  int
		wantCall  string
		wantState string // JSON "state" in the body, when one is expected
	}{
		{"start", http.MethodPost, "/hlsv2/s1/prepare", nil, http.StatusAccepted, "prepare s1 ", "running"},
		{"start all audio", http.MethodPost, "/hlsv2/s1/prepare?audio=all", nil, http.StatusAccepted, "prepare s1 all", "running"},
		{"status", http.MethodGet, "/hlsv2/s1/prepare", nil, http.StatusOK, "status s1", "idle"},
		{"stop", http.MethodDelete, "/hlsv2/s1/prepare", nil, http.StatusNoContent, "stop s1", ""},
		{"unknown session", http.MethodGet, "/hlsv2/nope/prepare", fmt.Errorf("hls: %w nope", types.ErrHLSSessionNotFound), http.StatusNotFound, "status nope", ""},
		{"invalid id", http.MethodPost, "/hlsv2/a..b/prepare", fmt.Errorf("hls: %w", types.ErrHLSInvalidSessionID), http.StatusBadRequest, "prepare a..b ", ""},
		{"invalid audio", http.MethodPost, "/hlsv2/s1/prepare?audio=x", fmt.Errorf("hls: %w", types.ErrInvalidHLSOption), http.StatusBadRequest, "prepare s1 x", ""},
		{"duration unknown", http.MethodPost, "/hlsv2/s1/prepare", fmt.Errorf("hls: %w", types.ErrHLSDurationUnknown), http.StatusConflict, "prepare s1 ", ""},
		{"too large", http.MethodPost, "/hlsv2/s1/prepare", fmt.Errorf("hls: %w", types.ErrHLSPrepareTooLarge), http.StatusInsufficientStorage, "prepare s1 ", ""},
		{"other error", http.MethodDelete, "/hlsv2/s1/prepare", errors.New("disk on fire"), http.StatusInternalServerError, "stop s1", ""},
		{"other method", http.MethodPut, "/hlsv2/s1/prepare", nil, http.StatusMethodNotAllowed, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &fakePreparer{fakeProber: &fakeProber{}, enabled: true, err: c.err}
			rec := serve(t, newHandlerWithPreparer(p), c.method, c.path, nil)
			if rec.Code != c.wantCode {
				t.Errorf("%s %s status = %d; want %d (body %q)", c.method, c.path, rec.Code, c.wantCode, rec.Body.String())
			}
			if got := strings.Join(p.calls, ";"); got != c.wantCall {
				t.Errorf("calls = %q; want %q", got, c.wantCall)
			}
			if c.wantState != "" {
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q", ct)
				}
				if m := decodeJSON(t, rec.Body.Bytes()); m["state"] != c.wantState {
					t.Errorf("state = %v; want %q", m["state"], c.wantState)
				}
			}
			if c.wantCode == http.StatusNoContent && rec.Body.Len() != 0 {
				t.Errorf("204 response must have an empty body, got %q", rec.Body.String())
			}
		})
	}
}

// With preparation off (or a prober without it) the prepare paths answer
// exactly as before: GET and POST go to HLSFile, DELETE is the plain 404.
func TestHandlerHLSPrepareDisabled(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		p := &fakePreparer{fakeProber: &fakeProber{hlsFileErr: fmt.Errorf("hls: not found %q", "prepare")}}
		rec := serve(t, newHandlerWithPreparer(p), method, "/hlsv2/s1/prepare", nil)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s with preparation off: status = %d; want 404", method, rec.Code)
		}
		if len(p.calls) != 0 {
			t.Errorf("%s with preparation off called %q", method, p.calls)
		}
		if len(p.deleted) != 0 {
			t.Errorf("%s with preparation off called DeleteHLS(%q)", method, p.deleted)
		}
	}
	rec := serve(t, newHandlerWithProber(t, &fakeProber{hlsFileErr: errors.New("hls: not found")}), http.MethodPost, "/hlsv2/s1/prepare", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("prober without HLSPreparer: status = %d; want 404", rec.Code)
	}
}

// POST and DELETE are state-changing: a foreign Origin is refused before the
// handler runs.
func TestHandlerHLSPrepareOriginGate(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		p := &fakePreparer{fakeProber: &fakeProber{}, enabled: true}
		req := httptest.NewRequest(method, "/hlsv2/s1/prepare", nil)
		req.Header.Set("Origin", "https://evil.example.com")
		rec := httptest.NewRecorder()
		newHandlerWithPreparer(p).ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden || len(p.calls) != 0 {
			t.Errorf("%s from a foreign origin: status %d, calls %q; want 403 and none", method, rec.Code, p.calls)
		}
	}
}
