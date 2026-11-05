// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

func TestParseXMLTVTime(t *testing.T) {
	cases := []struct {
		in   string
		want string // RFC3339 UTC; "" = invalid
	}{
		{"20261105200000 +0100", "2026-11-05T19:00:00Z"},
		{"20261105200000+0100", "2026-11-05T19:00:00Z"},
		{"20261105200000 -0530", "2026-11-06T01:30:00Z"},
		{"20261105200000 +01:00", "2026-11-05T19:00:00Z"},
		{"20261105200000 +01", "2026-11-05T19:00:00Z"},
		{"20261105200000", "2026-11-05T20:00:00Z"},
		{"20261105200000 UTC", "2026-11-05T20:00:00Z"},
		{" 202611052000 Z ", "2026-11-05T20:00:00Z"},
		{"20261105", "2026-11-05T00:00:00Z"},
		{"", ""},
		{"2026110520", ""},
		{"20261305200000", ""},
		{"20261105200000 CET", ""},
		{"20261105200000 +9", ""},
	}
	for _, c := range cases {
		got, ok := parseXMLTVTime(c.in)
		if c.want == "" {
			if ok {
				t.Errorf("%q parsed as %v; want invalid", c.in, got)
			}
			continue
		}
		if !ok || got.Format(time.RFC3339) != c.want {
			t.Errorf("%q = %v (%v); want %s", c.in, got, ok, c.want)
		}
	}
}

func TestXMLTVNSEpisode(t *testing.T) {
	for in, want := range map[string]string{"0.5.": "S1E6", "1/8.0/10.0/1": "S2E1", ".3.": "E4", "2..": "", "x.y.": "", "": ""} {
		if got := xmltvNSEpisode(in); got != want {
			t.Errorf("xmltvNSEpisode(%q) = %q; want %q", in, got, want)
		}
	}
}

func xmltvTime(t time.Time, zone string) string {
	if zone == "+0100" {
		t = t.In(time.FixedZone("", 3600))
	}
	return t.Format("20060102150405") + " " + zone
}

func xmltvProg(ch string, start, stop time.Time, inner string) string {
	return fmt.Sprintf(`<programme start=%q stop=%q channel=%q>%s</programme>`,
		xmltvTime(start, "+0100"), xmltvTime(stop, "+0000"), ch, inner)
}

func TestParseXMLTV(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	h := time.Hour
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="x">` +
		`<channel id="A.it"><display-name>A</display-name></channel>`)
	// Out of order, overlapping the window edges, duplicate start, unknown channel.
	b.WriteString(xmltvProg(" a.IT ", now.Add(h), now.Add(2*h),
		`<title lang="it">News</title><sub-title>Edition &amp; more</sub-title><desc>Daily &lt;news&gt;</desc>`+
			`<category>News</category><category>News</category><category>Info</category>`+
			`<icon src=" https://img.example/n.png "/><episode-num system="xmltv_ns">0.4.</episode-num>`))
	b.WriteString(xmltvProg("a.it", now.Add(-h), now.Add(h), `<title>Now</title><episode-num system="onscreen">S02E05</episode-num><episode-num system="xmltv_ns">0.0.</episode-num>`))
	b.WriteString(xmltvProg("a.it", now.Add(-h), now.Add(h), `<title>Duplicate start</title>`))
	b.WriteString(xmltvProg("a.it", now.Add(-30*h), now.Add(-25*h), `<title>Too old</title>`))
	b.WriteString(xmltvProg("a.it", now.Add(-25*h), now.Add(-23*h), `<title>Edge, still running in the window</title>`))
	b.WriteString(xmltvProg("a.it", now.Add(49*h), now.Add(50*h), `<title>Too far</title>`))
	b.WriteString(xmltvProg("a.it", now.Add(5*h), now.Add(6*h), `<title></title>`)) // no title
	b.WriteString(xmltvProg("other", now, now.Add(h), `<title>Other</title>`))
	b.WriteString(`<programme start="` + now.Add(3*h).Format("20060102150405") + `" channel="a.it"><title>No stop</title></programme>`)
	b.WriteString(`<programme start="` + now.Add(4*h).Format("20060102150405") + `" channel="a.it"><title>Last, no stop</title></programme>`)
	b.WriteString(`</tv>`)

	progs, st, err := parseXMLTV(strings.NewReader(b.String()), map[string]struct{}{"a.it": {}}, now)
	if err != nil || st != (iptvEPGStats{}) {
		t.Fatalf("err=%v stats=%+v", err, st)
	}
	if len(progs) != 1 {
		t.Fatalf("channels = %v; want only a.it", progs)
	}
	var titles []string
	for _, p := range progs["a.it"] {
		titles = append(titles, p.Title)
	}
	want := "Edge, still running in the window|Now|News - Edition & more|No stop"
	if strings.Join(titles, "|") != want {
		t.Fatalf("titles = %q; want %q", strings.Join(titles, "|"), want)
	}
	ps := progs["a.it"]
	if !ps[1].Start.Equal(now.Add(-h)) || !ps[1].End.Equal(now.Add(h)) || ps[1].Episode != "S02E05" {
		t.Errorf("Now = %+v (+0100 start must convert to UTC; onscreen episode wins)", ps[1])
	}
	n := ps[2]
	if n.Desc != "Daily <news>" || n.Icon != "https://img.example/n.png" || strings.Join(n.Genres, "|") != "News|Info" || n.Episode != "S1E5" {
		t.Errorf("News = %+v", n)
	}
	if !ps[3].End.Equal(now.Add(4 * h)) {
		t.Errorf("a programme without stop ends at the next start; got %v, want %v", ps[3].End, now.Add(4*h))
	}
}

func TestParseXMLTVCaps(t *testing.T) {
	now := time.Now().UTC()
	var b strings.Builder
	b.WriteString(`<tv>`)
	for i := range iptvEPGMaxPerChannel + 10 {
		s := now.Add(time.Duration(i) * time.Minute)
		b.WriteString(xmltvProg("a", s, s.Add(time.Minute), `<title>`+strings.Repeat("x", 5000)+`</title><desc>`+strings.Repeat("é", 5000)+`</desc>`))
	}
	b.WriteString(`</tv>`)
	progs, st, err := parseXMLTV(strings.NewReader(b.String()), map[string]struct{}{"a": {}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(progs["a"]); got != iptvEPGMaxPerChannel || st.DroppedPerChannel != 10 {
		t.Errorf("kept %d programmes, dropped %d; want the per-channel cap %d and 10 dropped", got, st.DroppedPerChannel, iptvEPGMaxPerChannel)
	}
	p := progs["a"][0]
	if len(p.Title) != iptvEPGMaxTitle || len(p.Desc) > iptvEPGMaxDesc || strings.ContainsRune(p.Desc, '\uFFFD') {
		t.Errorf("text not clipped on a rune boundary: title %d bytes, desc %d bytes", len(p.Title), len(p.Desc))
	}
}

func TestParseXMLTVLatin1(t *testing.T) {
	now := time.Now().UTC()
	doc := []byte(`<?xml version="1.0" encoding="ISO-8859-1"?><tv>` +
		xmltvProg("a", now, now.Add(time.Hour), "<title>Caff\xe8 \xfcber</title>") + `</tv>`)
	progs, _, err := parseXMLTV(bytes.NewReader(doc), map[string]struct{}{"a": {}}, now)
	if err != nil || len(progs["a"]) != 1 || progs["a"][0].Title != "Caffè über" {
		t.Fatalf("progs = %+v, err = %v", progs, err)
	}
	// windows-1252 maps 0x80-0x9F to punctuation (€ “ ” …), ISO-8859-1 to C1 controls.
	doc = []byte(`<?xml version="1.0" encoding="windows-1252"?><tv>` +
		xmltvProg("a", now, now.Add(time.Hour), "<title>\x80 \x93Hi\x94 \x85 caff\xe8</title>") + `</tv>`)
	progs, _, err = parseXMLTV(bytes.NewReader(doc), map[string]struct{}{"a": {}}, now)
	if err != nil || len(progs["a"]) != 1 || progs["a"][0].Title != "€ “Hi” … caffè" {
		t.Fatalf("cp1252 progs = %+v, err = %v", progs, err)
	}
	_, _, err = parseXMLTV(strings.NewReader(`<?xml version="1.0" encoding="EBCDIC"?><tv/>`), map[string]struct{}{"a": {}}, now)
	if err == nil {
		t.Error("unsupported charset accepted")
	}
}

func TestIPTVCapReader(t *testing.T) {
	r := &iptvCapReader{r: strings.NewReader(strings.Repeat("x", 100)), max: 99}
	buf := make([]byte, 64)
	var err error
	for err == nil {
		_, err = r.Read(buf)
	}
	if !errors.Is(err, errIPTVEPGTooLarge) {
		t.Errorf("err = %v; want errIPTVEPGTooLarge", err)
	}
	ok := &iptvCapReader{r: strings.NewReader(strings.Repeat("x", 100)), max: 100}
	for err = nil; err == nil; {
		_, err = ok.Read(buf)
	}
	if err.Error() != "EOF" {
		t.Errorf("exactly-at-cap read ended with %v; want EOF", err)
	}
}

func TestSortByChno(t *testing.T) {
	mk := func(name string, n float64) iptvChannel { return iptvChannel{Name: name, Chno: n} }
	chs := sortByChno([]iptvChannel{mk("x", 0), mk("c10", 10), mk("y", 0), mk("c2", 2), mk("c2.5", 2.5), mk("z", 0)})
	var names []string
	for _, c := range chs {
		names = append(names, c.Name)
	}
	if strings.Join(names, ",") != "c2,c2.5,c10,x,y,z" {
		t.Errorf("order = %v", names)
	}
}

func TestParseM3UAttributes(t *testing.T) {
	const pl = `#EXTM3U
#EXTINF:-1 tvg-id=" Rai1.IT " tvg-chno="2" http-user-agent="UA/1" http-referrer="https://ref.example/" http-origin="https://o.example",Rai 1
https://cdn.example/1.m3u8
#EXTINF:-1 tvg-id="x" tvg-chno="abc",Unnumbered
https://cdn.example/2.m3u8
#EXTINF:-1 tvg-chno="1" http-referer="https://attr.example/" http-user-agent="Attr",Rai 0
#EXTVLCOPT:http-referrer=https://vlc.example/
https://cdn.example/3.m3u8|User-Agent=Pipe
#EXTINF:-1 tvg-chno="0",Zero
https://cdn.example/4.m3u8
`
	chs, err := parseM3U(strings.NewReader(pl), "https://l.example/p.m3u")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range chs {
		names = append(names, c.Name)
	}
	if strings.Join(names, "|") != "Rai 0|Rai 1|Unnumbered|Zero" {
		t.Fatalf("order = %v; numbered first ascending, then playlist order", names)
	}
	r0, r1 := chs[0], chs[1]
	if r1.TVGID != "Rai1.IT" || r1.Chno != 2 {
		t.Errorf("Rai 1 = %+v", r1)
	}
	if h := r1.Headers; h.Get("User-Agent") != "UA/1" || h.Get("Referer") != "https://ref.example/" || h.Get("Origin") != "https://o.example" {
		t.Errorf("EXTINF headers = %v", h)
	}
	if h := r0.Headers; h.Get("Referer") != "https://vlc.example/" || h.Get("User-Agent") != "Pipe" {
		t.Errorf("EXTVLCOPT must win over the EXTINF attribute and |Header= over both: %v", h)
	}
	if chs[3].Chno != 0 {
		t.Errorf("tvg-chno=0 = %v; want unnumbered", chs[3].Chno)
	}
}

// ---- add-on with a guide ---------------------------------------------------

const iptvGuidePlaylist = "#EXTM3U\n" +
	`#EXTINF:-1 tvg-id="a.it" tvg-logo="https://logo.example/a.png" group-title="News",Chan A` + "\n" +
	"https://cdn.example/a.m3u8\n" +
	`#EXTINF:-1 tvg-id="B.IT",Chan B` + "\n" +
	"https://cdn.example/b.m3u8\n" +
	`#EXTINF:-1,No Id` + "\n" +
	"https://cdn.example/c.m3u8\n"

// guideServer serves a gzip XMLTV with programmes for a.it only; status
// non-zero fails the request; gate, when set, blocks it until closed.
type guideServer struct {
	*httptest.Server
	hits   atomic.Int32
	status atomic.Int32
	gate   chan struct{}
}

func newGuideServer(t *testing.T, doc string) *guideServer {
	t.Helper()
	g := &guideServer{}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(doc))
	_ = zw.Close()
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		g.hits.Add(1)
		if g.gate != nil {
			<-g.gate
		}
		if c := g.status.Load(); c != 0 {
			w.WriteHeader(int(c))
			return
		}
		w.Header().Set("Content-Type", "application/gzip")
		_, _ = w.Write(gz.Bytes())
	}))
	t.Cleanup(g.Close)
	return g
}

func guideDoc(now time.Time) string {
	h := time.Hour
	return `<tv>` +
		xmltvProg("A.IT", now.Add(-3*h), now.Add(-h), `<title>Earlier</title>`) +
		xmltvProg("a.it", now.Add(-h), now.Add(h), `<title>Current</title><desc>On now</desc>`) +
		xmltvProg("a.it", now.Add(h), now.Add(3*h), `<title>Next</title>`) +
		xmltvProg("a.it", now.Add(30*h), now.Add(31*h), `<title>Tomorrow</title>`) +
		xmltvProg("zzz", now, now.Add(h), `<title>Not in playlist</title>`) +
		`</tv>`
}

type iptvGuideManifest struct {
	Version       string
	BehaviorHints map[string]any
	Catalogs      []struct {
		Type, ID, Name string
		Extra          []struct{ Name string }
	}
}

type iptvDetailedMeta struct {
	ID, Type, Name, PosterShape string
	BehaviorHints               map[string]any
	Videos                      []map[string]any
}

func iptvTitles(vs []map[string]any) string {
	var out []string
	for _, v := range vs {
		out = append(out, v["title"].(string))
	}
	return strings.Join(out, "|")
}

func TestIPTVGuide(t *testing.T) {
	now := time.Now().UTC()
	guide := newGuideServer(t, guideDoc(now))
	var status atomic.Int32
	pls := iptvServer(t, iptvGuidePlaylist, &status)
	cfgFn := func(c *types.Config) {
		c.IPTVPlaylists = []string{pls.URL + "/p.m3u"}
		c.IPTVEPG = []string{guide.URL + "/epg.xml.gz"}
	}
	h := newHandlerWithCfg(t, cfgFn)

	var man iptvGuideManifest
	getJSON(t, h, "/iptv/manifest.json", &man)
	if man.Version != "0.2.0" || man.BehaviorHints["epgProvider"] != true || len(man.Catalogs) != 2 {
		t.Fatalf("manifest = %+v", man)
	}
	if c := man.Catalogs[1]; c.ID != "iptv0-guide" || c.Type != "tv" || c.Name != "IPTV guide" || len(c.Extra) != 2 || c.Extra[0].Name != "date" || c.Extra[1].Name != "skip" {
		t.Errorf("guide catalog = %+v", c)
	}

	// Channel catalog previews: square, live.
	var cat struct{ Metas []iptvDetailedMeta }
	getJSON(t, h, "/iptv/catalog/tv/iptv0.json", &cat)
	if len(cat.Metas) != 3 || cat.Metas[0].PosterShape != "square" || cat.Metas[2].PosterShape != "square" || cat.Metas[2].BehaviorHints["isLive"] != true || cat.Metas[0].Videos != nil {
		t.Errorf("channel catalog = %+v", cat.Metas)
	}

	// Guide catalog for today and tomorrow (UTC).
	today := now.Format(time.DateOnly)
	var gc struct {
		Metas         []any
		MetasDetailed []iptvDetailedMeta
	}
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide/date="+today+".json", &gc)
	if gc.Metas != nil || len(gc.MetasDetailed) != 3 {
		t.Fatalf("guide catalog = %+v", gc)
	}
	a := gc.MetasDetailed[0]
	if a.Name != "Chan A" || a.BehaviorHints["hasScheduledVideos"] != true || a.BehaviorHints["isLive"] != true {
		t.Errorf("Chan A = %+v", a)
	}
	dayStart, _ := time.Parse(time.DateOnly, today)
	for _, v := range a.Videos { // every listed programme overlaps the UTC date
		s, _ := time.Parse(time.RFC3339, v["startTime"].(string))
		e, _ := time.Parse(time.RFC3339, v["endTime"].(string))
		if !e.After(dayStart) || !s.Before(dayStart.Add(24*time.Hour)) {
			t.Errorf("programme %v outside %s", v, today)
		}
	}
	if len(a.Videos) == 0 {
		t.Fatalf("no programmes for Chan A today: %+v", a)
	}
	if b := gc.MetasDetailed[1]; b.Videos == nil || len(b.Videos) != 0 || b.BehaviorHints["hasScheduledVideos"] != false {
		t.Errorf("a channel without programmes must list empty videos: %+v", b)
	}

	// Paging, extras in the query string, and the fallback to today.
	var page struct{ MetasDetailed []iptvDetailedMeta }
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide/date=nonsense&skip=2.json", &page)
	if len(page.MetasDetailed) != 1 || page.MetasDetailed[0].Name != "No Id" {
		t.Errorf("skip=2 page = %+v", page.MetasDetailed)
	}
	var tomorrow struct{ MetasDetailed []iptvDetailedMeta }
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide?date="+now.Add(24*time.Hour).Format(time.DateOnly), &tomorrow)
	if len(tomorrow.MetasDetailed) != 3 {
		t.Fatalf("tomorrow = %+v", tomorrow)
	}
	// Unknown guide catalog / disabled guide.
	getJSON(t, h, "/iptv/catalog/tv/iptv9-guide.json", &page)

	// Meta: now-2h .. now+24h, sorted, programme ids <channel>:<startUnix>.
	var meta struct{ Meta iptvDetailedMeta }
	getJSON(t, h, "/iptv/meta/tv/"+a.ID+".json", &meta)
	if got := iptvTitles(meta.Meta.Videos); got != "Earlier|Current|Next" {
		t.Fatalf("meta videos = %q; want Earlier|Current|Next (Tomorrow is past now+24h)", got)
	}
	if meta.Meta.BehaviorHints["hasScheduledVideos"] != true || meta.Meta.BehaviorHints["isLive"] != true {
		t.Errorf("meta hints = %v", meta.Meta.BehaviorHints)
	}
	cur := meta.Meta.Videos[1]
	start, _ := time.Parse(time.RFC3339, cur["startTime"].(string))
	if cur["id"] != a.ID+":"+strconv.FormatInt(start.Unix(), 10) || cur["released"] != cur["startTime"] || cur["overview"] != "On now" || !start.Equal(now.Add(-time.Hour).Truncate(time.Second)) {
		t.Errorf("current programme = %v", cur)
	}

	// The guide is fetched once, not per request.
	if n := guide.hits.Load(); n != 1 {
		t.Errorf("guide fetched %d times; want 1", n)
	}

	// A failed refresh keeps the cached programmes.
	iptvEPGSourceFor(guide.URL + "/epg.xml.gz").mu.Lock()
	iptvEPGSourceFor(guide.URL + "/epg.xml.gz").fetched = now.Add(-2 * iptvTTL)
	iptvEPGSourceFor(guide.URL + "/epg.xml.gz").mu.Unlock()
	guide.status.Store(http.StatusBadGateway)
	getJSON(t, h, "/iptv/meta/tv/"+a.ID+".json", &meta)
	deadline := time.Now().Add(5 * time.Second)
	for guide.hits.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	getJSON(t, h, "/iptv/meta/tv/"+a.ID+".json", &meta)
	if got := iptvTitles(meta.Meta.Videos); got != "Earlier|Current|Next" {
		t.Errorf("after a failed refresh meta videos = %q; want the cached ones", got)
	}
}

func TestIPTVNoGuide(t *testing.T) {
	var status atomic.Int32
	pls := iptvServer(t, iptvGuidePlaylist, &status)
	h := newHandlerWithCfg(t, func(c *types.Config) { c.IPTVPlaylists = []string{pls.URL + "/p.m3u"} })

	var man iptvGuideManifest
	getJSON(t, h, "/iptv/manifest.json", &man)
	if man.Version != "0.2.0" || man.BehaviorHints != nil || len(man.Catalogs) != 1 || man.Catalogs[0].ID != "iptv0" {
		t.Errorf("manifest without a guide = %+v", man)
	}
	var cat struct{ Metas []iptvDetailedMeta }
	getJSON(t, h, "/iptv/catalog/tv/iptv0.json", &cat)
	var meta struct{ Meta map[string]any }
	getJSON(t, h, "/iptv/meta/tv/"+cat.Metas[0].ID+".json", &meta)
	if _, ok := meta.Meta["videos"]; ok || meta.Meta["behaviorHints"].(map[string]any)["hasScheduledVideos"] != false {
		t.Errorf("meta without a guide = %v", meta.Meta)
	}
	var gc map[string]any
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide.json", &gc)
	if v, ok := gc["metasDetailed"].([]any); !ok || len(v) != 0 {
		t.Errorf("guide catalog without a guide = %v", gc)
	}
}

func TestIPTVGuideFirstLoadWait(t *testing.T) {
	old := iptvEPGFirstWait
	iptvEPGFirstWait = 50 * time.Millisecond
	t.Cleanup(func() { iptvEPGFirstWait = old })

	now := time.Now().UTC()
	guide := newGuideServer(t, guideDoc(now))
	guide.gate = make(chan struct{})
	var status atomic.Int32
	pls := iptvServer(t, iptvGuidePlaylist, &status)
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.IPTVPlaylists = []string{pls.URL + "/p.m3u"}
		c.IPTVEPG = []string{guide.URL + "/slow.xml"}
	})

	var gc struct{ MetasDetailed []iptvDetailedMeta }
	began := time.Now()
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide.json", &gc)
	if d := time.Since(began); d > 2*time.Second {
		t.Errorf("request waited %v for the first load", d)
	}
	if len(gc.MetasDetailed) != 3 || len(gc.MetasDetailed[0].Videos) != 0 {
		t.Fatalf("before the guide loads: %+v", gc.MetasDetailed)
	}

	close(guide.gate) // the load finishes in the background
	src := iptvEPGSourceFor(guide.URL + "/slow.xml")
	deadline := time.Now().Add(5 * time.Second)
	for len(src.snapshot()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	var meta struct{ Meta iptvDetailedMeta }
	getJSON(t, h, "/iptv/meta/tv/"+gc.MetasDetailed[0].ID+".json", &meta)
	if len(meta.Meta.Videos) == 0 {
		t.Errorf("no programmes after the load finished: %+v", meta.Meta)
	}
	if n := guide.hits.Load(); n != 1 {
		t.Errorf("guide fetched %d times; want 1", n)
	}
}

func TestIPTVGuideNeverLoadedKeepsAnswering(t *testing.T) {
	guide := newGuideServer(t, "")
	guide.status.Store(http.StatusInternalServerError)
	var status atomic.Int32
	pls := iptvServer(t, iptvGuidePlaylist, &status)
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.IPTVPlaylists = []string{pls.URL + "/p.m3u"}
		c.IPTVEPG = []string{guide.URL + "/down.xml"}
	})
	var gc struct{ MetasDetailed []iptvDetailedMeta }
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide.json", &gc)
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide.json", &gc) // inside the retry gap: no second fetch
	if len(gc.MetasDetailed) != 3 || guide.hits.Load() != 1 {
		t.Errorf("channels = %d, guide fetches = %d; want 3 channels and 1 fetch", len(gc.MetasDetailed), guide.hits.Load())
	}
}

func TestIPTVGuideNoTVGIDSkipsDownload(t *testing.T) {
	guide := newGuideServer(t, "<tv/>")
	var status atomic.Int32
	pls := iptvServer(t, "#EXTM3U\n#EXTINF:-1,Plain\nhttps://cdn.example/p.m3u8\n", &status)
	h := newHandlerWithCfg(t, func(c *types.Config) {
		c.IPTVPlaylists = []string{pls.URL + "/p.m3u"}
		c.IPTVEPG = []string{guide.URL + "/g.xml"}
	})
	var gc struct{ MetasDetailed []iptvDetailedMeta }
	getJSON(t, h, "/iptv/catalog/tv/iptv0-guide.json", &gc)
	if len(gc.MetasDetailed) != 1 || guide.hits.Load() != 0 {
		t.Errorf("channels = %d, guide fetches = %d; want 1 and 0 (no channel has a tvg-id)", len(gc.MetasDetailed), guide.hits.Load())
	}
}

func TestIPTVGuideMerge(t *testing.T) {
	now := time.Now().UTC()
	h := time.Hour
	g := iptvGuide{enabled: true, srcs: []map[string][]iptvProgramme{
		{"a": {{Start: now, End: now.Add(h), Title: "one"}, {Start: now.Add(2 * h), End: now.Add(3 * h), Title: "three"}}},
		{"a": {{Start: now, End: now.Add(h), Title: "dup"}, {Start: now.Add(h), End: now.Add(2 * h), Title: "two"}}},
	}}
	var titles []string
	for _, p := range g.programmes(&iptvChannel{TVGID: " A "}) {
		titles = append(titles, p.Title)
	}
	if strings.Join(titles, ",") != "one,two,three" {
		t.Errorf("merged = %v", titles)
	}
	if g.programmes(&iptvChannel{}) != nil {
		t.Error("a channel without tvg-id has programmes")
	}
}

func TestIPTVTVGSig(t *testing.T) {
	ch := func(ids ...string) []iptvChannel {
		var out []iptvChannel
		for _, id := range ids {
			out = append(out, iptvChannel{TVGID: id})
		}
		return out
	}
	base := iptvTVGSig(ch("a", "b", "c"))
	for name, same := range map[string][]iptvChannel{
		"order":         ch("c", "a", "b"),
		"case, padding": ch(" A", "B ", "c"),
		"duplicates":    ch("a", "b", "b", "c", "c", "c"),
		"no id":         append(ch("a", "b", "c"), iptvChannel{}),
	} {
		if got := iptvTVGSig(same); got != base {
			t.Errorf("%s changed the signature: %s vs %s", name, got, base)
		}
	}
	for name, other := range map[string][]iptvChannel{
		"swapped id":      ch("a", "b", "d"),
		"extra id":        ch("a", "b", "c", "d"),
		"dup then swap":   ch("a", "a", "b", "b", "d"), // equal-hash XOR cancellation must not hide this
		"missing channel": ch("a", "b"),
	} {
		if iptvTVGSig(other) == base {
			t.Errorf("%s left the signature unchanged", name)
		}
	}
}

func TestIPTVRunReader(t *testing.T) {
	read := func(s string, max int) error {
		_, err := io.Copy(io.Discard, &iptvRunReader{r: iotestOneByte(strings.NewReader(s)), max: max})
		return err
	}
	// A run counts every byte after a '<' (tag name included): "a>" + 8 = 10.
	if err := read("<a>"+strings.Repeat("x", 8)+"</a><b>"+strings.Repeat("y", 8)+"</b>", 10); err != nil {
		t.Errorf("runs of exactly max rejected: %v", err)
	}
	for name, doc := range map[string]string{
		"text node":    "<a>" + strings.Repeat("x", 11) + "</a>",
		"trailing":     "<a>" + strings.Repeat("x", 11),
		"leading":      strings.Repeat("x", 11) + "<a/>",
		"attribute":    `<a b="` + strings.Repeat("x", 11) + `"/>`,
		"split chunks": "<a>" + strings.Repeat("x", 6) + strings.Repeat("y", 6),
	} {
		if err := read(doc, 10); !errors.Is(err, errIPTVEPGTextRun) {
			t.Errorf("%s: err = %v; want errIPTVEPGTextRun", name, err)
		}
	}
}

// iotestOneByte returns a reader yielding one byte per Read, so the run
// counting is exercised across chunk boundaries.
func iotestOneByte(r io.Reader) io.Reader { return oneByteReader{r} }

type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) { return o.r.Read(p[:min(1, len(p))]) }

func TestParseXMLTVBoundedProgramme(t *testing.T) {
	now := time.Now().UTC()
	var b strings.Builder
	b.WriteString(`<tv><programme start="` + xmltvTime(now, "+0000") + `" stop="` + xmltvTime(now.Add(time.Hour), "+0000") + `" channel="a">`)
	for i := range 2000 { // far more children than anything is kept of
		fmt.Fprintf(&b, `<title>t%d</title><desc>d%d</desc><category>c%d</category><icon src="i%d"/><episode-num system="onscreen">e%d</episode-num><x><title>nested</title></x>`, i, i, i, i, i)
	}
	b.WriteString(`</programme></tv>`)
	progs, _, err := parseXMLTV(strings.NewReader(b.String()), map[string]struct{}{"a": {}}, now)
	if err != nil || len(progs["a"]) != 1 {
		t.Fatalf("progs = %+v, err = %v", progs, err)
	}
	p := progs["a"][0]
	if p.Title != "t0" || p.Desc != "d0" || p.Icon != "i0" || p.Episode != "e0" || len(p.Genres) != iptvEPGMaxGenres {
		t.Errorf("programme = %+v; want the first title/desc/icon/episode and %d genres", p, iptvEPGMaxGenres)
	}
}

func TestParseXMLTVTotalCap(t *testing.T) {
	old := iptvEPGMaxProgrammes
	iptvEPGMaxProgrammes = 5
	t.Cleanup(func() { iptvEPGMaxProgrammes = old })
	now := time.Now().UTC()
	var b strings.Builder
	b.WriteString(`<tv>`)
	for _, ch := range []string{"a", "b", "c"} { // file order: a, then b, then c
		for i := range 3 {
			s := now.Add(time.Duration(i) * time.Hour)
			b.WriteString(xmltvProg(ch, s, s.Add(time.Hour), `<title>`+ch+`</title>`))
		}
	}
	b.WriteString(`</tv>`)
	progs, st, err := parseXMLTV(strings.NewReader(b.String()), map[string]struct{}{"a": {}, "b": {}, "c": {}}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(progs["a"]) != 3 || len(progs["b"]) != 2 || len(progs["c"]) != 0 {
		t.Errorf("kept a=%d b=%d c=%d; want 3, 2, 0 (the channels after the cutoff lose out)", len(progs["a"]), len(progs["b"]), len(progs["c"]))
	}
	if st.DroppedTotal != 4 || st.ChannelsEmpty != 1 || st.DroppedPerChannel != 0 {
		t.Errorf("stats = %+v; want 4 dropped over the total cap and 1 channel left empty", st)
	}
}

func TestIPTVEPGFetch(t *testing.T) {
	now := time.Now().UTC()
	doc := `<tv>` + xmltvProg("a", now, now.Add(time.Hour), `<title>One</title>`) + `</tv>`
	gz := func(s string) []byte {
		var b bytes.Buffer
		zw := gzip.NewWriter(&b)
		_, _ = zw.Write([]byte(s))
		_ = zw.Close()
		return b.Bytes()
	}
	serve := func(body []byte, code int) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write(body)
		}))
		t.Cleanup(srv.Close)
		return srv.URL + "/guide.xml.gz" // the name is irrelevant: detection is by magic bytes
	}
	caps := func(compressed, decoded int64) {
		oc, od := iptvEPGMaxCompressed, iptvEPGMaxBytes
		iptvEPGMaxCompressed, iptvEPGMaxBytes = compressed, decoded
		t.Cleanup(func() { iptvEPGMaxCompressed, iptvEPGMaxBytes = oc, od })
	}
	wanted := map[string]struct{}{"a": {}}
	ctx := t.Context()

	for name, body := range map[string][]byte{"plain": []byte(doc), "gzip": gz(doc)} {
		progs, _, err := iptvEPGFetch(ctx, serve(body, http.StatusOK), wanted, now)
		if err != nil || len(progs["a"]) != 1 || progs["a"][0].Title != "One" {
			t.Errorf("%s: progs = %+v, err = %v", name, progs, err)
		}
	}

	if _, _, err := iptvEPGFetch(ctx, serve([]byte(doc), http.StatusNotFound), wanted, now); err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("non-200: err = %v; want HTTP 404", err)
	}
	if _, _, err := iptvEPGFetch(ctx, serve([]byte{0x1f, 0x8b, 0, 0}, http.StatusOK), wanted, now); err == nil {
		t.Error("a truncated gzip header was accepted")
	}

	// Compressed cap: a gzip body over it fails, even though it would decode small.
	small := gz(doc)
	caps(int64(len(small))-1, 1<<20)
	if _, _, err := iptvEPGFetch(ctx, serve(small, http.StatusOK), wanted, now); !errors.Is(err, errIPTVEPGTooLarge) {
		t.Errorf("compressed cap: err = %v; want errIPTVEPGTooLarge", err)
	}
	// ... and exactly at the cap it is fine.
	caps(int64(len(small)), 1<<20)
	if _, _, err := iptvEPGFetch(ctx, serve(small, http.StatusOK), wanted, now); err != nil {
		t.Errorf("gzip at the compressed cap: %v", err)
	}

	// Decompressed cap: a small gzip that inflates past it (a gzip bomb).
	bomb := gz("<tv>" + strings.Repeat("<!-- -->\n", 50000) + "</tv>")
	caps(1<<20, 100<<10)
	if len(bomb) >= 100<<10 {
		t.Fatalf("test bomb is %d bytes compressed; want it far below the decoded cap", len(bomb))
	}
	if _, _, err := iptvEPGFetch(ctx, serve(bomb, http.StatusOK), wanted, now); !errors.Is(err, errIPTVEPGTooLarge) {
		t.Errorf("decompressed cap: err = %v; want errIPTVEPGTooLarge", err)
	}
	// A plain body counts against the decoded cap, not the gzip cap.
	caps(1, 1<<20)
	if _, _, err := iptvEPGFetch(ctx, serve([]byte(doc), http.StatusOK), wanted, now); err != nil {
		t.Errorf("plain body under the decoded cap: %v", err)
	}

	// An oversized text node fails the load instead of being buffered.
	caps(1<<20, 1<<30)
	old := iptvEPGMaxTextRun
	iptvEPGMaxTextRun = 1 << 10
	t.Cleanup(func() { iptvEPGMaxTextRun = old })
	huge := `<tv><programme start="x" channel="a"><desc>` + strings.Repeat("d", 4<<10) + `</desc></programme></tv>`
	if _, _, err := iptvEPGFetch(ctx, serve([]byte(huge), http.StatusOK), wanted, now); !errors.Is(err, errIPTVEPGTextRun) {
		t.Errorf("oversized text node: err = %v; want errIPTVEPGTextRun", err)
	}
}

func TestParseXMLTVCapKeepsClosestToNow(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Minute)
	var b strings.Builder
	b.WriteString(`<tv>`)
	// Far-past programmes first (over the compaction threshold), the airing
	// one and the upcoming ones last.
	n := 3 * iptvEPGMaxPerChannel
	for i := range n {
		s := now.Add(-time.Duration(i+2) * time.Minute)
		b.WriteString(xmltvProg("a", s, s.Add(time.Minute), `<title>past</title>`))
	}
	b.WriteString(xmltvProg("a", now.Add(-time.Minute), now.Add(time.Minute), `<title>current</title>`))
	for i := range 5 {
		s := now.Add(time.Duration(i+1) * time.Minute)
		b.WriteString(xmltvProg("a", s, s.Add(time.Minute), `<title>next</title>`))
	}
	b.WriteString(`</tv>`)
	progs, st, err := parseXMLTV(strings.NewReader(b.String()), map[string]struct{}{"a": {}}, now)
	if err != nil {
		t.Fatal(err)
	}
	ps := progs["a"]
	if len(ps) != iptvEPGMaxPerChannel || st.DroppedPerChannel != n+6-iptvEPGMaxPerChannel {
		t.Fatalf("kept %d, dropped %d", len(ps), st.DroppedPerChannel)
	}
	cur, nexts := 0, 0
	for _, p := range ps {
		switch p.Title {
		case "current":
			cur++
		case "next":
			nexts++
		}
	}
	if cur != 1 || nexts != 5 {
		t.Errorf("current kept %d, upcoming kept %d; want 1 and 5", cur, nexts)
	}
}
