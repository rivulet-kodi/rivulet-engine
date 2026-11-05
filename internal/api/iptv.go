// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — M3U/IPTV Stremio add-on at /iptv/*.
//
// Routes:
//
//	GET /iptv/manifest.json
//	     → Stremio addon manifest (id community.rivulet-engine.iptv, v0.2.0),
//	       one "tv" catalog per playlist with genre/search/skip extras; with
//	       RIVULET_IPTV_EPG also behaviorHints.epgProvider and one guide
//	       catalog (date/skip extras) per playlist
//	GET /iptv/catalog/tv/<catalog>[/<extras>].json
//	     → {metas:[{id,type,name,poster?,posterShape,genres?,behaviorHints}]},
//	       100 per page; a guide catalog (iptv<N>-guide) answers
//	       {metasDetailed:[meta + videos]} for one UTC date
//	GET /iptv/meta/tv/<id>.json
//	     → {meta:{…preview, behaviorHints:{isLive,hasScheduledVideos}, videos?}}
//	       (meta null when unknown)
//	GET /iptv/stream/tv/<id>.json
//	     → {streams:[{name,title,url}]}
//
// Playlists come from RIVULET_IPTV_PLAYLISTS (comma-separated M3U URLs, e.g.
// Free-TV's https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8).
// Unset → every /iptv route answers 404. Each playlist is fetched lazily,
// cached for iptvTTL and served stale when a refresh fails. The keys are the
// fixed configured URLs, so the cache needs no janitor.
//
// Channels whose entry carries request headers (EXTINF http-user-agent /
// http-referrer / http-origin attributes, #EXTVLCOPT:http-referrer /
// http-user-agent / http-origin, or Kodi's "url|Header=value&…" suffix; the
// later, more specific forms win) are handed to the player through /proxy,
// which sends those headers upstream and adds CORS; every other channel URL
// is returned as-is.
//
// The live-TV model (isLive previews, epgProvider guide catalogs, programme
// videos) follows stremio-core 0.66.0; the XMLTV guide itself is in
// iptv_epg.go.
package api

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

const (
	iptvTTL          = 6 * time.Hour
	iptvRetryAfter   = time.Minute // minimum gap between failed fetches
	iptvFetchTimeout = 20 * time.Second
	iptvMaxBytes     = 16 << 20 // playlist body cap
	iptvMaxChannels  = 20000
	iptvMaxGenres    = 200
	iptvPageSize     = 100
	iptvMetaPast     = 2 * time.Hour  // meta videos: programmes from now-2h …
	iptvMetaFuture   = 24 * time.Hour // … to now+24h
)

var iptvClient = &http.Client{Timeout: iptvFetchTimeout}

// iptvChannel is one playable playlist entry.
type iptvChannel struct {
	ID      string
	Name    string
	Logo    string
	URL     string
	TVGID   string  // EPG key (tvg-id), trimmed; "" when the entry has none
	Chno    float64 // tvg-chno; 0 when absent or not a positive number
	Groups  []string
	Headers http.Header // request headers the upstream needs; nil when none
}

// iptvPlaylist is the cached state of one configured playlist.
type iptvPlaylist struct {
	mu       sync.Mutex // serialises refreshes and guards the fields below
	channels []iptvChannel
	byID     map[string]int
	genres   []string
	tvgSig   string    // fingerprint of the channels' tvg-ids, see iptvTVGSig
	fetched  time.Time // last successful fetch
	failed   time.Time // last failed fetch
}

var (
	iptvMu    sync.Mutex
	iptvLists = map[string]*iptvPlaylist{}
)

// iptvList returns the playlist state for u, refreshing it when stale. A
// failed refresh keeps the previous channels.
func iptvList(ctx context.Context, u string) *iptvPlaylist {
	iptvMu.Lock()
	pl := iptvLists[u]
	if pl == nil {
		pl = &iptvPlaylist{}
		iptvLists[u] = pl
	}
	iptvMu.Unlock()

	pl.mu.Lock()
	defer pl.mu.Unlock()
	now := time.Now()
	if now.Sub(pl.fetched) < iptvTTL || now.Sub(pl.failed) < iptvRetryAfter {
		return pl
	}
	chs, err := iptvFetch(ctx, u)
	if err != nil {
		pl.failed = now
		logging.For("iptv").Warn("playlist fetch failed", "url", redactURL(u), "err", err)
		return pl
	}
	pl.channels = chs
	pl.byID = make(map[string]int, len(chs))
	seen := map[string]struct{}{}
	for i, c := range chs {
		pl.byID[c.ID] = i
		for _, g := range c.Groups {
			if _, ok := seen[g]; !ok && len(seen) < iptvMaxGenres {
				seen[g] = struct{}{}
			}
		}
	}
	pl.tvgSig = iptvTVGSig(chs)
	pl.genres = make([]string, 0, len(seen))
	for g := range seen {
		pl.genres = append(pl.genres, g)
	}
	sort.Strings(pl.genres)
	pl.fetched = now
	pl.failed = time.Time{}
	logging.For("iptv").Info("playlist loaded", "url", redactURL(u), "channels", len(chs))
	return pl
}

// snapshot returns the current channels, index and genres. The slices are
// replaced, never mutated, on refresh, so they are safe to read unlocked.
func (pl *iptvPlaylist) snapshot() ([]iptvChannel, map[string]int, []string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return pl.channels, pl.byID, pl.genres
}

// guideInput returns the channels and the tvg-id fingerprint the XMLTV loader
// filters programmes by.
func (pl *iptvPlaylist) guideInput() ([]iptvChannel, string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return pl.channels, pl.tvgSig
}

// redactURL drops userinfo and query (which may carry credentials) for logs.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid>"
	}
	return u.Scheme + "://" + u.Host + u.Path
}

func iptvFetch(ctx context.Context, u string) ([]iptvChannel, error) {
	ctx, cancel := context.WithTimeout(ctx, iptvFetchTimeout)
	defer cancel()
	resp, err := iptvGet(ctx, iptvClient, u, "playlist")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return parseM3U(io.LimitReader(resp.Body, iptvMaxBytes), u)
}

// iptvGet issues a GET and returns the 200 response; what names the resource
// in errors. The caller closes the body.
func iptvGet(ctx context.Context, c *http.Client, u, what string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	resp, err := c.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			// *url.Error embeds the full URL, which may carry credentials.
			return nil, fmt.Errorf("%s %s: %w", ue.Op, what, ue.Err)
		}
		return nil, fmt.Errorf("do request: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp, nil
}

// ---- M3U parsing -----------------------------------------------------------

var iptvAttrRe = regexp.MustCompile(`([A-Za-z0-9_-]+)="([^"]*)"`)

// iptvVLCHeaders maps #EXTVLCOPT option names to the HTTP header they set.
var iptvVLCHeaders = map[string]string{
	"http-referrer":   "Referer",
	"http-referer":    "Referer",
	"http-user-agent": "User-Agent",
	"http-origin":     "Origin",
}

// parseM3U parses an extended M3U playlist. base resolves relative entry
// URLs and seeds the channel ids; only http(s) entries are kept.
func parseM3U(rd io.Reader, base string) ([]iptvChannel, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("parse playlist url: %w", err)
	}
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)

	var (
		out    []iptvChannel
		seen   = map[string]struct{}{}
		cur    iptvChannel
		extgrp string
		first  = true
	)
	reset := func() { cur, extgrp = iptvChannel{}, "" }
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if first {
			line = strings.TrimPrefix(line, "\ufeff")
			first = false
		}
		switch {
		case line == "":
		case strings.HasPrefix(line, "#EXTINF:"):
			reset()
			info := line[len("#EXTINF:"):]
			attrs, name := splitExtinf(info)
			for _, m := range iptvAttrRe.FindAllStringSubmatch(attrs, -1) {
				switch strings.ToLower(m[1]) {
				case "tvg-logo":
					cur.Logo = strings.TrimSpace(m[2])
				case "tvg-id":
					cur.TVGID = strings.TrimSpace(m[2])
				case "tvg-chno":
					if n, err := strconv.ParseFloat(strings.TrimSpace(m[2]), 64); err == nil && n > 0 && !math.IsInf(n, 0) {
						cur.Chno = n
					}
				case "group-title":
					cur.Groups = splitGroups(m[2])
				case "tvg-name":
					if name == "" {
						name = m[2]
					}
				default:
					// http-user-agent / http-referrer / http-origin; the
					// #EXTVLCOPT lines and "|Header=" suffix below overwrite.
					if h := iptvVLCHeaders[strings.ToLower(m[1])]; h != "" {
						cur.setHeader(h, m[2])
					}
				}
			}
			cur.Name = strings.TrimSpace(name)
		case strings.HasPrefix(line, "#EXTGRP:"):
			extgrp = line[len("#EXTGRP:"):]
		case strings.HasPrefix(line, "#EXTVLCOPT:"):
			k, v, ok := strings.Cut(line[len("#EXTVLCOPT:"):], "=")
			if h := iptvVLCHeaders[strings.ToLower(strings.TrimSpace(k))]; ok && h != "" {
				cur.setHeader(h, v)
			}
		case strings.HasPrefix(line, "#"):
		default:
			ch, grp := cur, extgrp
			reset()
			raw, pipe, _ := strings.Cut(line, "|")
			ref, err := url.Parse(strings.TrimSpace(raw))
			if err != nil {
				continue
			}
			abs := baseURL.ResolveReference(ref)
			if (abs.Scheme != "http" && abs.Scheme != "https") || abs.Host == "" {
				continue
			}
			ch.URL = abs.String()
			if pipe != "" {
				ch.Headers = mergePipeHeaders(ch.Headers, pipe)
			}
			if len(ch.Groups) == 0 && grp != "" {
				ch.Groups = splitGroups(grp)
			}
			if ch.Name == "" {
				ch.Name = path.Base(abs.Path)
			}
			sum := sha256.Sum256([]byte(base + "\n" + ch.URL))
			ch.ID = "iptv:" + hex.EncodeToString(sum[:16])
			if _, dup := seen[ch.ID]; dup {
				continue
			}
			seen[ch.ID] = struct{}{}
			out = append(out, ch)
			if len(out) >= iptvMaxChannels {
				return sortByChno(out), nil
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read playlist: %w", err)
	}
	return sortByChno(out), nil
}

// setHeader sets a request header the upstream needs; empty values are ignored.
func (c *iptvChannel) setHeader(name, value string) {
	if value = strings.TrimSpace(value); value == "" {
		return
	}
	if c.Headers == nil {
		c.Headers = http.Header{}
	}
	c.Headers.Set(name, value)
}

// sortByChno orders numbered channels (tvg-chno) first, ascending, then the
// rest in playlist order; the sort is stable, so a playlist without numbers
// keeps its order.
func sortByChno(chs []iptvChannel) []iptvChannel {
	sort.SliceStable(chs, func(i, j int) bool {
		a, b := chs[i].Chno, chs[j].Chno
		if a == 0 || b == 0 {
			return a != 0 && b == 0
		}
		return a < b
	})
	return chs
}

// splitExtinf splits the text after "#EXTINF:" at the first comma outside
// double quotes into the attribute part and the display name.
func splitExtinf(s string) (attrs, name string) {
	inQ := false
	for i := range len(s) {
		switch s[i] {
		case '"':
			inQ = !inQ
		case ',':
			if !inQ {
				return s[:i], s[i+1:]
			}
		}
	}
	return s, ""
}

func splitGroups(s string) []string {
	var out []string
	for _, g := range strings.Split(s, ";") {
		if g = strings.TrimSpace(g); g != "" {
			out = append(out, g)
		}
	}
	return out
}

// mergePipeHeaders applies Kodi's "Header=value&Header2=value2" URL suffix.
func mergePipeHeaders(h http.Header, s string) http.Header {
	for _, kv := range strings.Split(s, "&") {
		k, v, ok := strings.Cut(kv, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" || v == "" {
			continue
		}
		if dv, err := url.QueryUnescape(v); err == nil {
			v = dv
		}
		if h == nil {
			h = http.Header{}
		}
		h.Set(k, v)
	}
	return h
}

// ---- HTTP handlers ---------------------------------------------------------

// handleIPTV dispatches all /iptv/* routes; 404 when no playlist is configured.
func (s *server) handleIPTV(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(s.cfg.IPTVPlaylists) == 0 || len(seg) < 2 {
		http.NotFound(w, r)
		return
	}
	if seg[1] == "manifest.json" && len(seg) == 2 {
		s.iptvManifest(w, r)
		return
	}
	if len(seg) < 4 {
		http.NotFound(w, r)
		return
	}
	typ, id := seg[2], strings.TrimSuffix(seg[3], ".json")
	switch seg[1] {
	case "catalog":
		s.iptvCatalog(w, r, typ, id)
	case "meta":
		s.iptvMeta(w, r, typ, id)
	case "stream":
		s.iptvStream(w, r, typ, id)
	default:
		http.NotFound(w, r)
	}
}

// iptvCtx detaches playlist fetches from the client so an aborted request
// does not leave the cache marked as failed.
func iptvCtx(r *http.Request) context.Context { return context.WithoutCancel(r.Context()) }

func (s *server) iptvManifest(w http.ResponseWriter, r *http.Request) {
	guide := len(s.cfg.IPTVEPG) > 0
	if guide {
		s.iptvGuide(r, false) // start the guide load now; the catalogs wait for it
	}
	catalogs := make([]any, 0, len(s.cfg.IPTVPlaylists))
	for i, u := range s.cfg.IPTVPlaylists {
		_, _, genres := iptvList(iptvCtx(r), u).snapshot()
		name := "IPTV"
		if len(s.cfg.IPTVPlaylists) > 1 {
			name += " " + strconv.Itoa(i+1)
		}
		genre := map[string]any{"name": "genre"}
		if len(genres) > 0 {
			genre["options"] = genres
		}
		catalogs = append(catalogs, map[string]any{
			"type":  "tv",
			"id":    "iptv" + strconv.Itoa(i),
			"name":  name,
			"extra": []any{genre, map[string]any{"name": "search"}, map[string]any{"name": "skip"}},
		})
		if guide {
			// stremio-core (Manifest catalog is_epg_guide): a catalog with a
			// `date` extra on an epgProvider add-on is a guide catalog.
			catalogs = append(catalogs, map[string]any{
				"type":  "tv",
				"id":    "iptv" + strconv.Itoa(i) + iptvGuideSuffix,
				"name":  name + " guide",
				"extra": []any{map[string]any{"name": "date"}, map[string]any{"name": "skip"}},
			})
		}
	}
	m := map[string]any{
		"id":          "community.rivulet-engine.iptv",
		"version":     "0.2.0",
		"name":        "IPTV",
		"description": "Live TV channels from M3U playlists.",
		"resources":   []string{"catalog", "meta", "stream"},
		"types":       []string{"tv"},
		"idPrefixes":  []string{"iptv:"},
		"catalogs":    catalogs,
	}
	if guide {
		m["behaviorHints"] = map[string]any{"epgProvider": true}
	}
	writeJSON(w, http.StatusOK, m)
}

// iptvGuideSuffix marks a guide catalog id ("iptv0-guide").
const iptvGuideSuffix = "-guide"

// iptvMetaPreview is a channel as listed in a catalog. stremio-core treats
// behaviorHints.isLive (or type "tv") as a live item; channel logos are
// square.
func iptvMetaPreview(c *iptvChannel) map[string]any {
	m := map[string]any{
		"id":            c.ID,
		"type":          "tv",
		"name":          c.Name,
		"posterShape":   "square",
		"behaviorHints": map[string]any{"isLive": true},
	}
	if c.Logo != "" {
		m["poster"] = c.Logo
		m["logo"] = c.Logo
	}
	if len(c.Groups) > 0 {
		m["genres"] = c.Groups
	}
	return m
}

// iptvExtras returns the catalog extras from the path segment after the
// catalog id ("genre=News&skip=100.json") merged with the query string. The
// escaped path is used so a value containing an encoded "/" or "&" survives.
func iptvExtras(r *http.Request) url.Values {
	extras := r.URL.Query()
	ep := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	if len(ep) >= 5 {
		if v, err := url.ParseQuery(strings.TrimSuffix(strings.Join(ep[4:], "/"), ".json")); err == nil {
			for k, vs := range v {
				extras[k] = vs
			}
		}
	}
	return extras
}

// iptvCatalogID parses a catalog id: "iptv<N>" (channels) or "iptv<N>-guide".
func (s *server) iptvCatalogID(typ, id string) (idx int, guide, ok bool) {
	rest, ok := strings.CutPrefix(id, "iptv")
	if !ok || typ != "tv" {
		return 0, false, false
	}
	rest, guide = strings.CutSuffix(rest, iptvGuideSuffix)
	idx, err := strconv.Atoi(rest)
	if err != nil || idx < 0 || idx >= len(s.cfg.IPTVPlaylists) {
		return 0, false, false
	}
	return idx, guide, true
}

func (s *server) iptvCatalog(w http.ResponseWriter, r *http.Request, typ, id string) {
	idx, guide, ok := s.iptvCatalogID(typ, id)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"metas": []any{}})
		return
	}
	extras := iptvExtras(r)
	skip, _ := strconv.Atoi(extras.Get("skip"))
	skip = max(skip, 0)
	chs, _, _ := iptvList(iptvCtx(r), s.cfg.IPTVPlaylists[idx]).snapshot()
	if guide {
		s.iptvGuideCatalog(w, r, chs, extras.Get("date"), skip)
		return
	}
	genre := extras.Get("genre")
	search := strings.ToLower(strings.TrimSpace(extras.Get("search")))

	metas := []any{}
	matched := 0
	for i := range chs {
		c := &chs[i]
		if genre != "" && !containsString(c.Groups, genre) {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(c.Name), search) {
			continue
		}
		matched++
		if matched <= skip {
			continue
		}
		metas = append(metas, iptvMetaPreview(c))
		if len(metas) == iptvPageSize {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"metas": metas})
}

// iptvGuideCatalog answers a guide catalog request (stremio-core
// live_tv_guide.rs): {"metasDetailed": [...]}, one full channel meta per
// channel with the programmes overlapping the UTC date. Channels without
// programmes that day are still listed (empty videos). A missing or invalid
// date means today (UTC). Without a configured guide the catalog is empty.
func (s *server) iptvGuideCatalog(w http.ResponseWriter, r *http.Request, chs []iptvChannel, date string, skip int) {
	metas := []any{}
	if len(s.cfg.IPTVEPG) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"metasDetailed": metas})
		return
	}
	day, err := time.Parse(time.DateOnly, date)
	if err != nil {
		day = time.Now().UTC().Truncate(24 * time.Hour)
	}
	from, to := day, day.Add(24*time.Hour)
	g := s.iptvGuide(r, true)
	for i := skip; i < len(chs) && len(metas) < iptvPageSize; i++ {
		metas = append(metas, iptvMetaDetailed(&chs[i], g, from, to))
	}
	writeJSON(w, http.StatusOK, map[string]any{"metasDetailed": metas})
}

// iptvMetaDetailed is a channel meta whose videos are its programmes
// overlapping [from, to) (stremio-core: schedule = the meta's videos;
// hasScheduledVideos marks a channel with one). videos is omitted when no
// guide is configured.
func iptvMetaDetailed(c *iptvChannel, g iptvGuide, from, to time.Time) map[string]any {
	m := iptvMetaPreview(c)
	hints := map[string]any{"isLive": true, "hasScheduledVideos": false}
	m["behaviorHints"] = hints
	if !g.enabled {
		return m
	}
	videos := []any{}
	for _, p := range g.programmes(c) {
		if p.End.After(from) && p.Start.Before(to) {
			videos = append(videos, p.video(c.ID))
		}
	}
	hints["hasScheduledVideos"] = len(videos) > 0
	m["videos"] = videos
	return m
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// iptvFind looks a channel id up across all configured playlists.
func (s *server) iptvFind(r *http.Request, typ, id string) (iptvChannel, bool) {
	if typ != "tv" || !strings.HasPrefix(id, "iptv:") {
		return iptvChannel{}, false
	}
	for _, u := range s.cfg.IPTVPlaylists {
		chs, byID, _ := iptvList(iptvCtx(r), u).snapshot()
		if i, ok := byID[id]; ok {
			return chs[i], true
		}
	}
	return iptvChannel{}, false
}

func (s *server) iptvMeta(w http.ResponseWriter, r *http.Request, typ, id string) {
	c, ok := s.iptvFind(r, typ, id)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"meta": nil})
		return
	}
	now := time.Now()
	g := s.iptvGuide(r, true)
	writeJSON(w, http.StatusOK, map[string]any{"meta": iptvMetaDetailed(&c, g, now.Add(-iptvMetaPast), now.Add(iptvMetaFuture))})
}

func (s *server) iptvStream(w http.ResponseWriter, r *http.Request, typ, id string) {
	c, ok := s.iptvFind(r, typ, id)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"streams": []any{}})
		return
	}
	streamURL := c.URL
	if len(c.Headers) > 0 {
		streamURL = s.sp.ProxyURL(r, iptvProxyEndpoint(c.URL), c.URL, c.Headers)
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": []any{map[string]any{
		"name":  "IPTV",
		"title": c.Name,
		"url":   streamURL,
	}}})
}

// iptvProxyEndpoint picks the /proxy endpoint from the channel URL's extension.
func iptvProxyEndpoint(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "/proxy/stream"
	}
	switch strings.ToLower(path.Ext(u.Path)) {
	case ".m3u8", ".m3u":
		return "/proxy/hls/manifest.m3u8"
	case ".mpd":
		return "/proxy/mpd/manifest.m3u8"
	}
	return "/proxy/stream"
}
