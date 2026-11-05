// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

const iptvFixture = "\ufeff#EXTM3U\n" +
	`#EXTINF:-1 tvg-id="a.it" tvg-logo="https://logo.example/a.png" group-title="News;Italy",Rai News, 24` + "\n" +
	"https://cdn.example/rainews/index.m3u8\n" +
	`#EXTINF:-1 group-title="Sports",Sport One` + "\n" +
	"#EXTVLCOPT:http-referrer=https://player.example/\n" +
	"#EXTVLCOPT:http-user-agent=Mozilla/5.0\n" +
	"https://cdn.example/sport/live.m3u8\n" +
	`#EXTINF:-1,Kodi Style` + "\n" +
	"#EXTGRP:Music\n" +
	"https://cdn.example/music.ts|Referer=https%3A%2F%2Fk.example%2F&User-Agent=Kodi\n" +
	`#EXTINF:-1 group-title="News",Relative` + "\n" +
	"rel/chan.mpd\n" +
	`#EXTINF:-1,Not HTTP` + "\n" +
	"rtmp://cdn.example/live\n" +
	`#EXTINF:-1,Duplicate` + "\n" +
	"https://cdn.example/rainews/index.m3u8\n"

func TestParseM3U(t *testing.T) {
	chs, err := parseM3U(strings.NewReader(iptvFixture), "https://lists.example/p/playlist.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	if len(chs) != 4 {
		t.Fatalf("got %d channels, want 4 (rtmp and duplicate dropped): %+v", len(chs), chs)
	}
	c := chs[0]
	if c.Name != "Rai News, 24" || c.Logo != "https://logo.example/a.png" || strings.Join(c.Groups, "|") != "News|Italy" || c.Headers != nil {
		t.Errorf("channel 0 = %+v", c)
	}
	if !strings.HasPrefix(c.ID, "iptv:") || len(c.ID) != len("iptv:")+32 {
		t.Errorf("id = %q", c.ID)
	}
	if h := chs[1].Headers; h.Get("Referer") != "https://player.example/" || h.Get("User-Agent") != "Mozilla/5.0" {
		t.Errorf("EXTVLCOPT headers = %v", h)
	}
	k := chs[2]
	if k.URL != "https://cdn.example/music.ts" || k.Headers.Get("Referer") != "https://k.example/" || k.Headers.Get("User-Agent") != "Kodi" || strings.Join(k.Groups, "|") != "Music" {
		t.Errorf("kodi channel = %+v", k)
	}
	if chs[3].URL != "https://lists.example/p/rel/chan.mpd" {
		t.Errorf("relative url = %q", chs[3].URL)
	}
	// Ids depend on the playlist too, so two playlists never collide.
	other, _ := parseM3U(strings.NewReader(iptvFixture), "https://other.example/x.m3u")
	if other[0].ID == c.ID {
		t.Error("same id for the same channel in two playlists")
	}
}

func TestParseM3UChannelCap(t *testing.T) {
	var b strings.Builder
	for i := range iptvMaxChannels + 5 {
		b.WriteString("#EXTINF:-1,c\nhttps://cdn.example/" + strconv.Itoa(i) + ".ts\n")
	}
	chs, err := parseM3U(strings.NewReader(b.String()), "https://l.example/p.m3u")
	if err != nil || len(chs) != iptvMaxChannels {
		t.Fatalf("got %d channels (%v), want %d", len(chs), err, iptvMaxChannels)
	}
}

func TestIPTVDisabled(t *testing.T) {
	h := newHandler(t)
	for _, p := range []string{"/iptv/manifest.json", "/iptv/catalog/tv/iptv0.json", "/iptv/stream/tv/iptv:x.json"} {
		if rec := serve(t, h, http.MethodGet, p, nil); rec.Code != http.StatusNotFound {
			t.Errorf("%s = %d; want 404 when RIVULET_IPTV_PLAYLISTS is unset", p, rec.Code)
		}
	}
}

func iptvServer(t *testing.T, body string, status *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if c := status.Load(); c != 0 {
			w.WriteHeader(int(c))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func getJSON(t *testing.T, h http.Handler, p string, v any) {
	t.Helper()
	rec := serve(t, h, http.MethodGet, p, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s = %d %s", p, rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("%s: %v", p, err)
	}
}

type iptvMetas struct {
	Metas []struct {
		ID, Name string
		Genres   []string
	}
}

func TestIPTVAddon(t *testing.T) {
	var status atomic.Int32
	srv := iptvServer(t, iptvFixture, &status)
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.IPTVPlaylists = []string{srv.URL + "/playlist.m3u8"}
		c.ProxyPassword = "pw"
	})

	var man struct {
		Catalogs []struct {
			ID    string
			Extra []struct {
				Name    string
				Options []string
			}
		}
	}
	getJSON(t, h, "/iptv/manifest.json", &man)
	if len(man.Catalogs) != 1 || man.Catalogs[0].ID != "iptv0" {
		t.Fatalf("catalogs = %+v", man.Catalogs)
	}
	if g := man.Catalogs[0].Extra[0]; g.Name != "genre" || strings.Join(g.Options, "|") != "Italy|Music|News|Sports" {
		t.Errorf("genre extra = %+v", g)
	}

	cases := []struct {
		path string
		want []string
	}{
		{"/iptv/catalog/tv/iptv0.json", []string{"Rai News, 24", "Sport One", "Kodi Style", "Relative"}},
		{"/iptv/catalog/tv/iptv0/genre=News.json", []string{"Rai News, 24", "Relative"}},
		{"/iptv/catalog/tv/iptv0/genre=News&skip=1.json", []string{"Relative"}},
		{"/iptv/catalog/tv/iptv0/search=SPORT.json", []string{"Sport One"}},
		{"/iptv/catalog/tv/iptv0?search=kodi", []string{"Kodi Style"}},
		{"/iptv/catalog/tv/iptv0/search=" + url.QueryEscape("news, 24") + ".json", []string{"Rai News, 24"}},
		{"/iptv/catalog/movie/iptv0.json", nil},
		{"/iptv/catalog/tv/iptv1.json", nil},
		{"/iptv/catalog/tv/other.json", nil},
	}
	var ids = map[string]string{}
	for _, c := range cases {
		var got iptvMetas
		getJSON(t, h, c.path, &got)
		var names []string
		for _, m := range got.Metas {
			names = append(names, m.Name)
			ids[m.Name] = m.ID
		}
		if strings.Join(names, "|") != strings.Join(c.want, "|") {
			t.Errorf("%s = %q; want %q", c.path, names, c.want)
		}
	}

	var meta struct {
		Meta *struct{ ID, Type, Name, Poster string }
	}
	getJSON(t, h, "/iptv/meta/tv/"+ids["Rai News, 24"]+".json", &meta)
	if meta.Meta == nil || meta.Meta.Type != "tv" || meta.Meta.Poster != "https://logo.example/a.png" {
		t.Errorf("meta = %+v", meta.Meta)
	}
	meta.Meta = nil
	getJSON(t, h, "/iptv/meta/tv/iptv:unknown.json", &meta)
	if meta.Meta != nil {
		t.Errorf("unknown meta = %+v; want null", meta.Meta)
	}

	type streams struct{ Streams []struct{ URL string } }
	var direct streams
	getJSON(t, h, "/iptv/stream/tv/"+ids["Rai News, 24"]+".json", &direct)
	if len(direct.Streams) != 1 || direct.Streams[0].URL != "https://cdn.example/rainews/index.m3u8" {
		t.Errorf("headerless stream = %+v; want the raw url", direct)
	}

	for name, endpoint := range map[string]string{"Sport One": "/proxy/hls/manifest.m3u8", "Kodi Style": "/proxy/stream"} {
		var px streams
		getJSON(t, h, "/iptv/stream/tv/"+ids[name], &px)
		if len(px.Streams) != 1 {
			t.Fatalf("%s streams = %+v", name, px)
		}
		u, err := url.Parse(px.Streams[0].URL)
		if err != nil || u.Path != endpoint || !u.IsAbs() {
			t.Fatalf("%s url = %q; want absolute %s", name, px.Streams[0].URL, endpoint)
		}
		q := u.Query()
		d, _ := base64.RawURLEncoding.DecodeString(q.Get("d"))
		if q.Get("api_password") != "pw" || q.Get("h_Referer") == "" || q.Get("h_User-Agent") == "" || !strings.HasPrefix(string(d), "https://cdn.example/") {
			t.Errorf("%s proxy url = %q", name, px.Streams[0].URL)
		}
	}

	// A failed refresh keeps the cached channels.
	iptvMu.Lock()
	pl := iptvLists[srv.URL+"/playlist.m3u8"]
	iptvMu.Unlock()
	pl.mu.Lock()
	pl.fetched = pl.fetched.Add(-2 * iptvTTL)
	pl.mu.Unlock()
	status.Store(http.StatusBadGateway)
	var stale iptvMetas
	getJSON(t, h, "/iptv/catalog/tv/iptv0.json", &stale)
	if len(stale.Metas) != 4 {
		t.Errorf("after failed refresh got %d channels; want the 4 cached", len(stale.Metas))
	}
}

func TestIPTVUnreachablePlaylist(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusNotFound)
	srv := iptvServer(t, "", &status)
	h := newHandlerWithCfg(t, func(c *types.Config) { c.IPTVPlaylists = []string{srv.URL + "/gone.m3u"} })
	var got iptvMetas
	getJSON(t, h, "/iptv/catalog/tv/iptv0.json", &got)
	if len(got.Metas) != 0 {
		t.Errorf("metas = %+v; want empty", got.Metas)
	}
}
