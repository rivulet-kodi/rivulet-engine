// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — Torznab Stremio add-on at /torznab/*.
//
// Routes:
//
//	GET /torznab/manifest.json
//	     → Stremio addon manifest (id community.rivulet-engine.torznab, v0.1.0)
//	GET /torznab/stream/<type>/<id>.json
//	     → {streams:[{infoHash,...}]} from a Torznab indexer (Prowlarr/Jackett/NZBHydra/Bitmagnet)
//
// The indexer is queried via its Torznab RSS/XML API (GET to RIVULET_TORZNAB_URL).
// Titles are resolved via Cinemeta (v3-cinemeta.strem.io) and cached for 6 hours.
// If RIVULET_TORZNAB_URL is unset the manifest still serves but all stream
// requests return an empty result set.
package api

import (
	"encoding/base32"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

// ---- HTTP client -----------------------------------------------------------

// tnClient is the dedicated HTTP client for Torznab indexer requests.
var tnClient = &http.Client{Timeout: 15 * time.Second}

// tnRespLimit caps the XML response body to bound memory from hostile indexers.
const tnRespLimit = 4 << 20 // 4 MiB

// ---- XML RSS types (local to this file) ------------------------------------

// tnRSS is the top-level RSS envelope returned by a Torznab indexer.
type tnRSS struct {
	Channel tnChannel `xml:"channel"`
}

// tnChannel holds the list of search results from the RSS feed.
type tnChannel struct {
	Items []tnItem `xml:"item"`
}

// tnItem is a single torrent result inside the Torznab RSS channel.
type tnItem struct {
	Title     string      `xml:"title"`
	Enclosure tnEnclosure `xml:"enclosure"`
	Attrs     []tnAttr    `xml:"attr"`
}

// tnEnclosure carries the magnet URL and declared file size.
type tnEnclosure struct {
	URL    string `xml:"url,attr"`
	Length string `xml:"length,attr"`
}

// tnAttr is a torznab:attr element. encoding/xml matches the local name "attr"
// and ignores the "torznab:" namespace prefix, so this struct correctly captures
// every <torznab:attr name="..." value="..."/> entry in the feed.
type tnAttr struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

// ---- infohash helpers (local) ----------------------------------------------

// btihRE matches the raw hash inside a urn:btih: URN (magnet link or attr).
var btihRE = regexp.MustCompile(`(?i)urn:btih:([a-z0-9]+)`)

// tnAttrVal returns the value of the first torznab:attr whose name matches,
// or the empty string when no such attribute is present.
func tnAttrVal(item tnItem, name string) string {
	for _, a := range item.Attrs {
		if a.Name == name {
			return a.Value
		}
	}
	return ""
}

// tnResolveInfoHash extracts and normalizes an info-hash from a tnItem.
// Resolution order:
//  1. torznab attr "infohash"
//  2. xt=urn:btih: extracted from the enclosure URL
//  3. xt=urn:btih: extracted from the torznab attr "magneturl"
//
// 40-character tokens are interpreted as lowercase hex; 32-character tokens are
// decoded from base32 and returned as lowercase hex. Returns "" when no valid
// hash can be resolved.
func tnResolveInfoHash(item tnItem) string {
	// 1. explicit infohash attr
	if h := tnNormalizeHash(tnAttrVal(item, "infohash")); h != "" {
		return h
	}
	// 2. enclosure URL magnet xt
	if m := btihRE.FindStringSubmatch(item.Enclosure.URL); len(m) == 2 {
		if h := tnNormalizeHash(m[1]); h != "" {
			return h
		}
	}
	// 3. magneturl attr
	if m := btihRE.FindStringSubmatch(tnAttrVal(item, "magneturl")); len(m) == 2 {
		if h := tnNormalizeHash(m[1]); h != "" {
			return h
		}
	}
	return ""
}

// tnNormalizeHash normalizes a raw info-hash token to a lowercase hex string.
// Returns "" for tokens that are neither 40-char hex nor 32-char base32.
func tnNormalizeHash(raw string) string {
	switch len(raw) {
	case 40:
		lower := strings.ToLower(raw)
		for _, ch := range lower {
			if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
				return ""
			}
		}
		return lower
	case 32:
		b, err := base32.StdEncoding.DecodeString(strings.ToUpper(raw))
		if err != nil {
			return ""
		}
		return hex.EncodeToString(b)
	}
	return ""
}

// tnSeeders returns the seeders count from item attrs, defaulting to 0.
func tnSeeders(item tnItem) int {
	v := tnAttrVal(item, "seeders")
	if v == "" {
		return 0
	}
	n, _ := strconv.Atoi(v)
	return n
}

// tnItemSize returns the byte size of the torrent. Resolution order:
//  1. torznab attr "size"
//  2. enclosure length attribute
//  3. magnet xl= query parameter (enclosure URL, then magneturl attr)
func tnItemSize(item tnItem) int64 {
	if v := tnAttrVal(item, "size"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	if item.Enclosure.Length != "" {
		if n, err := strconv.ParseInt(item.Enclosure.Length, 10, 64); err == nil {
			return n
		}
	}
	rawURL := item.Enclosure.URL
	if rawURL == "" {
		rawURL = tnAttrVal(item, "magneturl")
	}
	if rawURL != "" {
		if u, err := url.Parse(rawURL); err == nil {
			if xl := u.Query().Get("xl"); xl != "" {
				if n, err := strconv.ParseInt(xl, 10, 64); err == nil {
					return n
				}
			}
		}
	}
	return 0
}

// ---- Dispatch -------------------------------------------------------------

// handleTorznab dispatches all /torznab/* routes.
func (s *server) handleTorznab(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}
	switch seg[1] {
	case "manifest.json":
		s.torznabManifest(w)
	default:
		if seg[1] == "stream" && len(seg) >= 4 {
			idJSON := strings.TrimSuffix(seg[3], ".json")
			s.torznabStream(w, r, seg[2], idJSON)
		} else {
			http.NotFound(w, r)
		}
	}
}

// ---- Manifest -------------------------------------------------------------

func (s *server) torznabManifest(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          "community.rivulet-engine.torznab",
		"version":     "0.1.0",
		"name":        "Torznab Indexer",
		"description": "Torrent streams from a Torznab indexer (Prowlarr/Jackett/NZBHydra/Bitmagnet).",
		"resources":   []string{"stream"},
		"types":       []string{"movie", "series"},
		"idPrefixes":  []string{"tt"},
		"catalogs":    []any{},
	})
}

// ---- Stream handler -------------------------------------------------------

func (s *server) torznabStream(w http.ResponseWriter, r *http.Request, contentType, id string) {
	empty := map[string]any{"streams": []any{}}

	// Manifest is always available but queries are inert without an endpoint.
	if s.cfg.TorznabURL == "" {
		writeJSON(w, http.StatusOK, empty)
		return
	}

	if contentType != "movie" && contentType != "series" {
		writeJSON(w, http.StatusOK, empty)
		return
	}

	// Parse IMDB id and (for series) season + episode from "tt…:S:E".
	parts := strings.SplitN(id, ":", 3)
	baseImdb := parts[0]
	var season, episode int
	if contentType == "series" && len(parts) == 3 {
		season, _ = strconv.Atoi(parts[1])
		episode, _ = strconv.Atoi(parts[2])
	}

	// Numeric IMDB id (strip leading "tt").
	numeric := strings.TrimPrefix(baseImdb, "tt")

	// Primary query: search by IMDB id.
	items, err := tnQueryIMDB(r, s.cfg.TorznabURL, s.cfg.TorznabAPIKey, contentType, numeric, season, episode)
	if err != nil {
		logging.For("torznab").Error("imdb query failed", "type", contentType, "imdb", baseImdb, "err", err)
		writeJSON(w, http.StatusOK, empty)
		return
	}

	// Fallback: if no items returned, resolve title via Cinemeta and retry with q=.
	if len(items) == 0 {
		title, _ := resolveCinemeta(r, s.cfg.MetadataURL, contentType, baseImdb)
		if title != "" {
			items, err = tnQueryTitle(r, s.cfg.TorznabURL, s.cfg.TorznabAPIKey, contentType, title, season, episode)
			if err != nil {
				logging.For("torznab").Warn("title query failed", "type", contentType, "title", title, "err", err)
			}
		}
	}

	// Resolve infohash for each item; collect fully-resolved results.
	type tnResolved struct {
		hash       string
		title      string
		size       int64
		seeders    int
		resolution string
	}
	var rv []tnResolved
	for _, item := range items {
		hash := tnResolveInfoHash(item)
		if hash == "" {
			continue
		}
		rv = append(rv, tnResolved{
			hash:       hash,
			title:      item.Title,
			size:       tnItemSize(item),
			seeders:    tnSeeders(item),
			resolution: tnAttrVal(item, "resolution"),
		})
	}

	// Sort by seeders descending; cap at 20.
	sort.Slice(rv, func(i, j int) bool {
		return rv[i].seeders > rv[j].seeders
	})
	if len(rv) > 20 {
		rv = rv[:20]
	}

	streams := make([]bmStream, 0, len(rv))
	for _, res := range rv {
		strmName := "Torznab"
		if res.resolution != "" {
			strmName += "\n" + res.resolution
		}
		streams = append(streams, bmStream{
			InfoHash: res.hash,
			Name:     strmName,
			Title: res.title +
				"\n" + humanizeSize(res.size) +
				" | seeders: " + strconv.Itoa(res.seeders),
			BehaviorHints: &bmBehavior{
				BingeGroup: "torznab|" + res.resolution,
			},
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

// ---- Torznab queries -------------------------------------------------------

// tnQueryIMDB executes a Torznab search by IMDB id and returns the RSS items.
//
//	movie:  t=movie&imdbid=<numeric>&cat=2000
//	series: t=tvsearch&imdbid=<numeric>&season=<S>&ep=<E>&cat=5000
func tnQueryIMDB(r *http.Request, baseURL, apiKey, contentType, imdbNumeric string, season, episode int) ([]tnItem, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}
	q := u.Query()
	if contentType == "series" {
		q.Set("t", "tvsearch")
		q.Set("cat", "5000")
		if season > 0 {
			q.Set("season", strconv.Itoa(season))
		}
		if episode > 0 {
			q.Set("ep", strconv.Itoa(episode))
		}
	} else {
		q.Set("t", "movie")
		q.Set("cat", "2000")
	}
	q.Set("imdbid", imdbNumeric)
	if apiKey != "" {
		q.Set("apikey", apiKey)
	}
	u.RawQuery = q.Encode()
	return tnFetch(r, u.String())
}

// tnQueryTitle executes a Torznab free-text search and returns the RSS items.
//
//	movie:  t=search&q=<title>&cat=2000
//	series: t=tvsearch&season=<S>&ep=<E>&q=<title>&cat=5000
func tnQueryTitle(r *http.Request, baseURL, apiKey, contentType, title string, season, episode int) ([]tnItem, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}
	q := u.Query()
	if contentType == "series" {
		q.Set("t", "tvsearch")
		q.Set("cat", "5000")
		if season > 0 {
			q.Set("season", strconv.Itoa(season))
		}
		if episode > 0 {
			q.Set("ep", strconv.Itoa(episode))
		}
	} else {
		q.Set("t", "search")
		q.Set("cat", "2000")
	}
	q.Set("q", title)
	if apiKey != "" {
		q.Set("apikey", apiKey)
	}
	u.RawQuery = q.Encode()
	return tnFetch(r, u.String())
}

// tnFetch performs the HTTP GET to the Torznab endpoint and decodes the XML
// response into a slice of tnItem. Any HTTP or decode error is returned; the
// caller is responsible for logging and returning an empty stream list.
func tnFetch(r *http.Request, fullURL string) ([]tnItem, error) {
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	resp, err := tnClient.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			// *url.Error embeds the full URL (incl. apikey); drop it.
			return nil, fmt.Errorf("do request: %s torznab: %w", ue.Op, ue.Err)
		}
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var rss tnRSS
	if err := xml.NewDecoder(io.LimitReader(resp.Body, tnRespLimit)).Decode(&rss); err != nil {
		return nil, fmt.Errorf("decode XML: %w", err)
	}
	return rss.Channel.Items, nil
}
