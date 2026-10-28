// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package streamproxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// credUpstream records every request the origin receives (URL and headers).
type credUpstream struct {
	srv  *httptest.Server
	mu   sync.Mutex
	seen []string
}

func newCredUpstream(t *testing.T) *credUpstream {
	t.Helper()
	u := &credUpstream{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.mu.Lock()
		u.seen = append(u.seen, r.Method+" "+r.URL.String()+"\n"+fmtHeaders(r.Header))
		u.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, ".m3u8"):
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = w.Write([]byte("#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\"\n#EXTINF:4,\nseg0.ts\n#EXT-X-ENDLIST\n"))
		case strings.HasSuffix(r.URL.Path, ".mpd"):
			w.Header().Set("Content-Type", "application/dash+xml")
			_, _ = w.Write([]byte(`<?xml version="1.0"?><MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" mediaPresentationDuration="PT4S"><Period><AdaptationSet mimeType="video/mp4"><Representation id="v" bandwidth="1000"><BaseURL>seg.mp4</BaseURL></Representation></AdaptationSet></Period></MPD>`))
		default:
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write([]byte("segment-bytes"))
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func fmtHeaders(h http.Header) string {
	var b strings.Builder
	for k, vs := range h {
		for _, v := range vs {
			b.WriteString(k + ": " + v + "\n")
		}
	}
	return b.String()
}

func (u *credUpstream) requests() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.seen...)
}

// TestProxyNeverForwardsAuthCredsUpstream proves that rivulet's own proxy
// credentials (api_password / signed token) reach the upstream origin neither
// in the request URL nor in any header, on the stream, HLS and MPD paths and
// on the follow-up requests for URLs rewritten into manifests, while the
// rewritten rivulet URLs keep authenticating the next hop.
func TestProxyNeverForwardsAuthCredsUpstream(t *testing.T) {
	const pw = "s3cr3t-pw-value"
	up := newCredUpstream(t)
	allow, _ := netguard.ParseAllow("127.0.0.1")
	h := New(Config{Password: pw, Secret: testSecret, PrivateAllow: allow, PublicURL: "https://ext.example"})
	t.Cleanup(h.Close)

	exp := time.Now().Add(time.Hour).Unix()
	mkTok := func(endpoint, dest string) string {
		tok, err := h.signToken(token{Endpoint: endpoint, Params: map[string]string{"d": dest}, Exp: exp})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}

	type tc struct {
		name, path, dest, auth string
	}
	hlsDest := up.srv.URL + "/live/index.m3u8"
	mpdDest := up.srv.URL + "/live/manifest.mpd"
	segDest := up.srv.URL + "/live/seg0.ts"
	var cases []tc
	for _, c := range []struct{ n, ep, d string }{
		{"stream", "/proxy/stream", segDest},
		{"hls", "/proxy/hls/manifest.m3u8", hlsDest},
		{"mpd", "/proxy/mpd/manifest.mpd", mpdDest},
	} {
		cases = append(cases,
			tc{c.n + "/password", c.ep, c.d, "api_password=" + url.QueryEscape(pw)},
			tc{c.n + "/token", c.ep, c.d, "token=" + url.QueryEscape(mkTok(c.ep, c.d))},
		)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := len(up.requests())
			target := c.path + "?d=" + url.QueryEscape(c.dest) + "&h_X-Test=1&" + c.auth
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", target, nil)
			r.RemoteAddr = "203.0.113.7:5000"
			// Client-side credentials must not be relayed either.
			r.Header.Set("Authorization", "Bearer "+pw)
			r.Header.Set("Cookie", "api_password="+pw)
			if !h.Route(w, r, []string{"proxy", strings.Split(strings.TrimPrefix(c.path, "/proxy/"), "/")[0]}) {
				t.Fatal("route not handled")
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}

			// Follow every rewritten rivulet URL (segments, keys, variants).
			for _, line := range strings.Split(w.Body.String(), "\n") {
				for _, f := range strings.FieldsFunc(line, func(r rune) bool { return r == '"' || r == ' ' || r == '<' || r == '>' }) {
					if !strings.HasPrefix(f, "https://ext.example/proxy/") {
						continue
					}
					u, err := url.Parse(strings.ReplaceAll(f, "&amp;", "&"))
					if err != nil {
						t.Fatal(err)
					}
					sub := httptest.NewRecorder()
					sr := httptest.NewRequest("GET", u.RequestURI(), nil)
					sr.RemoteAddr = "203.0.113.7:5000"
					segs := strings.Split(strings.Trim(u.Path, "/"), "/")
					if !h.Route(sub, sr, segs) {
						t.Fatalf("sub route not handled: %s", u.Path)
					}
					if sub.Code != http.StatusOK {
						t.Errorf("rewritten URL %s unauthorized/failed: %d", u.Path, sub.Code)
					}
				}
			}

			reqs := up.requests()[before:]
			if len(reqs) == 0 {
				t.Fatal("upstream saw no requests")
			}
			if !strings.HasPrefix(c.name, "stream") && len(reqs) < 2 {
				t.Fatalf("manifest sub-URLs were not followed upstream (%d requests)", len(reqs))
			}
			tokVal := strings.TrimPrefix(c.auth, "token=")
			for _, rq := range reqs {
				for _, bad := range []string{pw, url.QueryEscape(pw), "api_password", "token=", "Authorization", "Cookie", tokVal} {
					if bad != "" && strings.Contains(rq, bad) {
						t.Errorf("upstream received %q in request:\n%s", bad, rq)
					}
				}
			}
		})
	}
}
