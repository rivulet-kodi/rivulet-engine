// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — M3U/IPTV Stremio add-on at /iptv/*.
//
// Routes:
//
//	GET /iptv/manifest.json
//	     → Stremio addon manifest (id community.rivulet-engine.iptv, v0.1.0),
//	       one "tv" catalog per playlist with genre/search/skip extras
//	GET /iptv/catalog/tv/<catalog>[/<extras>].json
//	     → {metas:[{id,type,name,poster?,genres?}]}, 100 per page
//	GET /iptv/meta/tv/<id>.json
//	     → {meta:{id,type,name,poster?,genres?}} (meta null when unknown)
//	GET /iptv/stream/tv/<id>.json
//	     → {streams:[{name,title,url}]}
//
// Playlists come from RIVULET_IPTV_PLAYLISTS (comma-separated M3U URLs, e.g.
// Free-TV's https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8).
// Unset → every /iptv route answers 404. Each playlist is fetched lazily,
// cached for iptvTTL and served stale when a refresh fails. The keys are the
// fixed configured URLs, so the cache needs no janitor.
//
// Channels whose entry carries request headers (#EXTVLCOPT:http-referrer /
// http-user-agent / http-origin, or Kodi's "url|Header=value&…" suffix) are
// handed to the player through /proxy, which sends those headers upstream
// and adds CORS; every other channel URL is returned as-is.
package api

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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
)

var iptvClient = &http.Client{Timeout: iptvFetchTimeout}

// iptvChannel is one playable playlist entry.
type iptvChannel struct {
	ID      string
	Name    string
	Logo    string
	URL     string
	Groups  []string
	Headers http.Header // request headers the upstream needs; nil when none
}

// iptvPlaylist is the cached state of one configured playlist.
type iptvPlaylist struct {
	mu       sync.Mutex // serialises refreshes and guards the fields below
	channels []iptvChannel
	byID     map[string]int
	genres   []string
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	resp, err := iptvClient.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			// *url.Error embeds the full URL, which may carry credentials.
			return nil, fmt.Errorf("%s playlist: %w", ue.Op, ue.Err)
		}
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return parseM3U(io.LimitReader(resp.Body, iptvMaxBytes), u)
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
				case "group-title":
					cur.Groups = splitGroups(m[2])
				case "tvg-name":
					if name == "" {
						name = m[2]
					}
				}
			}
			cur.Name = strings.TrimSpace(name)
		case strings.HasPrefix(line, "#EXTGRP:"):
			extgrp = line[len("#EXTGRP:"):]
		case strings.HasPrefix(line, "#EXTVLCOPT:"):
			k, v, ok := strings.Cut(line[len("#EXTVLCOPT:"):], "=")
			if h := iptvVLCHeaders[strings.ToLower(strings.TrimSpace(k))]; ok && h != "" && v != "" {
				if cur.Headers == nil {
					cur.Headers = http.Header{}
				}
				cur.Headers.Set(h, strings.TrimSpace(v))
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
				return out, nil
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read playlist: %w", err)
	}
	return out, nil
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
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          "community.rivulet-engine.iptv",
		"version":     "0.1.0",
		"name":        "IPTV",
		"description": "Live TV channels from M3U playlists.",
		"resources":   []string{"catalog", "meta", "stream"},
		"types":       []string{"tv"},
		"idPrefixes":  []string{"iptv:"},
		"catalogs":    catalogs,
	})
}

func iptvMetaPreview(c *iptvChannel) map[string]any {
	m := map[string]any{"id": c.ID, "type": "tv", "name": c.Name}
	if c.Logo != "" {
		m["poster"] = c.Logo
		m["posterShape"] = "square"
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

func (s *server) iptvCatalog(w http.ResponseWriter, r *http.Request, typ, id string) {
	metas := []any{}
	idx, err := strconv.Atoi(strings.TrimPrefix(id, "iptv"))
	if typ != "tv" || !strings.HasPrefix(id, "iptv") || err != nil || idx < 0 || idx >= len(s.cfg.IPTVPlaylists) {
		writeJSON(w, http.StatusOK, map[string]any{"metas": metas})
		return
	}
	extras := iptvExtras(r)
	genre := extras.Get("genre")
	search := strings.ToLower(strings.TrimSpace(extras.Get("search")))
	skip, _ := strconv.Atoi(extras.Get("skip"))
	skip = max(skip, 0)

	chs, _, _ := iptvList(iptvCtx(r), s.cfg.IPTVPlaylists[idx]).snapshot()
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
	writeJSON(w, http.StatusOK, map[string]any{"meta": iptvMetaPreview(&c)})
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
