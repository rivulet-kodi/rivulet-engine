// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — Bitmagnet Stremio add-on at /bitmagnet/*.
//
// Routes:
//
//	GET /bitmagnet/manifest.json
//	     → Stremio addon manifest (id community.rivulet-engine.bitmagnet, v0.1.0)
//	GET /bitmagnet/stream/<type>/<id>.json
//	     → {streams:[{infoHash,...}]} from self-hosted Bitmagnet DHT index
//
// Bitmagnet is queried via its GraphQL API (POST to RIVULET_BITMAGNET_URL).
// Title and year are resolved via a Cinemeta-compatible metadata addon
// (RIVULET_METADATA_URL, default Cinemeta) and cached
// for 6 hours. If RIVULET_BITMAGNET_URL is unset the manifest still serves but
// all stream requests return an empty result set.
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

// ---- HTTP clients ---------------------------------------------------------

var (
	bmClient   = &http.Client{Timeout: 10 * time.Second}
	cineClient = &http.Client{Timeout: 10 * time.Second}
)

// ---- Cinemeta resolution cache --------------------------------------------

type cineMetaEntry struct {
	name    string
	year    int
	expires time.Time
}

const cineMetaTTL = 6 * time.Hour

// Response-body read limits to bound memory from a hostile or oversized response.
const (
	bmRespLimit   = 4 << 20 // 4 MiB — Bitmagnet GraphQL (up to 50 results)
	cineMetaLimit = 1 << 20 // 1 MiB — Cinemeta single-item metadata
)

var (
	cineMetaMu          sync.Mutex
	cineMetaCache       = map[string]cineMetaEntry{}
	cineMetaJanitorOnce sync.Once
)

// cineMetaEvict deletes expired entries from cineMetaCache.
// Must be called with cineMetaMu held.
func cineMetaEvict() {
	now := time.Now()
	for k, e := range cineMetaCache {
		if now.After(e.expires) {
			delete(cineMetaCache, k)
		}
	}
}

// cineMetaStartJanitor starts a background goroutine (once) that evicts
// expired Cinemeta cache entries every cineMetaTTL period.
func cineMetaStartJanitor() {
	cineMetaJanitorOnce.Do(func() {
		go func() {
			tick := time.NewTicker(cineMetaTTL)
			defer tick.Stop()
			for range tick.C {
				cineMetaMu.Lock()
				cineMetaEvict()
				cineMetaMu.Unlock()
			}
		}()
	})
}

// ---- GraphQL request/response types (local to this file) ------------------

// bmGraphQLQuery is the Bitmagnet torrent-content search query.
// Only the fields consumed by the handler are requested.
const bmGraphQLQuery = `query($input: TorrentContentSearchQueryInput!) {
  torrentContent { search(input: $input) {
    items {
      infoHash
      contentType
      title
      seeders
      videoResolution
      languages { id name }
      episodes { seasons { season episodes } }
      content { releaseYear title }
      torrent { name size filesCount }
    }
  } }
}`

type bmReq struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables"`
}

type bmResp struct {
	Data struct {
		TorrentContent struct {
			Search struct {
				Items []bmItem `json:"items"`
			} `json:"search"`
		} `json:"torrentContent"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type bmItem struct {
	InfoHash        string `json:"infoHash"`
	ContentType     string `json:"contentType"`
	Title           string `json:"title"`
	Seeders         int    `json:"seeders"`
	VideoResolution string `json:"videoResolution"`
	Languages       []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"languages"`
	Episodes struct {
		Seasons []struct {
			Season   int   `json:"season"`
			Episodes []int `json:"episodes"`
		} `json:"seasons"`
	} `json:"episodes"`
	Content struct {
		ReleaseYear int    `json:"releaseYear"`
		Title       string `json:"title"`
	} `json:"content"`
	Torrent struct {
		Name       string `json:"name"`
		Size       int64  `json:"size"`
		FilesCount int    `json:"filesCount"`
	} `json:"torrent"`
}

// ---- Stremio stream shape -------------------------------------------------

type bmStream struct {
	InfoHash      string      `json:"infoHash"`
	Name          string      `json:"name"`
	Title         string      `json:"title"`
	BehaviorHints *bmBehavior `json:"behaviorHints,omitempty"`
}

type bmBehavior struct {
	BingeGroup string `json:"bingeGroup"`
}

// ---- Dispatch -------------------------------------------------------------

// handleBitmagnet dispatches all /bitmagnet/* routes.
func (s *server) handleBitmagnet(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}
	switch seg[1] {
	case "manifest.json":
		s.bitmagnetManifest(w)
	default:
		if seg[1] == "stream" && len(seg) >= 4 {
			idJSON := strings.TrimSuffix(seg[3], ".json")
			s.bitmagnetStream(w, r, seg[2], idJSON)
		} else {
			http.NotFound(w, r)
		}
	}
}

// ---- Manifest -------------------------------------------------------------

func (s *server) bitmagnetManifest(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          "community.rivulet-engine.bitmagnet",
		"version":     "0.1.0",
		"name":        "Bitmagnet (self-hosted DHT)",
		"description": "Decentralized torrent streams from a self-hosted Bitmagnet DHT index.",
		"resources":   []string{"stream"},
		"types":       []string{"movie", "series"},
		"idPrefixes":  []string{"tt"},
		"catalogs":    []any{},
	})
}

// ---- Stream handler -------------------------------------------------------

func (s *server) bitmagnetStream(w http.ResponseWriter, r *http.Request, contentType, id string) {
	empty := map[string]any{"streams": []any{}}

	// Manifest is always available but queries are inert without an endpoint.
	if s.cfg.BitmagnetURL == "" {
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

	// Resolve title + year via Cinemeta; fall back to the IMDB id on failure.
	metaName, year := resolveCinemeta(r, s.cfg.MetadataURL, contentType, baseImdb)
	if metaName == "" {
		logging.For("bitmagnet").Warn("cinemeta lookup failed; using id as query", "type", contentType, "imdb", baseImdb)
		metaName = baseImdb
	}

	// Build Bitmagnet query string.
	var queryString string
	if contentType == "movie" {
		if year > 0 {
			queryString = fmt.Sprintf("%s %d", metaName, year)
		} else {
			queryString = metaName
		}
	} else {
		// Series: title only — season/episode filtering happens after retrieval.
		queryString = metaName
	}

	items, err := queryBitmagnet(r, s.cfg.BitmagnetURL, queryString)
	if err != nil {
		logging.For("bitmagnet").Error("search failed", "query", queryString, "err", err)
		writeJSON(w, http.StatusOK, empty)
		return
	}

	// Bitmagnet contentType strings.
	var bmType string
	if contentType == "movie" {
		bmType = "movie"
	} else {
		bmType = "tv_show"
	}

	// Filter: wrong type or irrelevant season/episode.
	kept := items[:0]
	for _, item := range items {
		if item.ContentType != bmType {
			continue
		}
		if contentType == "series" && !bmItemMatchesSeries(item, season, episode) {
			continue
		}
		kept = append(kept, item)
	}

	// Sort by seeders descending, cap at 20.
	sort.Slice(kept, func(i, j int) bool {
		return kept[i].Seeders > kept[j].Seeders
	})
	if len(kept) > 20 {
		kept = kept[:20]
	}

	streams := make([]bmStream, 0, len(kept))
	for _, item := range kept {
		strmName := "Bitmagnet"
		if item.VideoResolution != "" {
			strmName += "\n" + item.VideoResolution
		}

		strmTitle := item.Torrent.Name +
			"\n" + humanizeSize(item.Torrent.Size) +
			" | seeders: " + strconv.Itoa(item.Seeders)
		if len(item.Languages) > 0 {
			langs := make([]string, 0, len(item.Languages))
			for _, l := range item.Languages {
				langs = append(langs, l.Name)
			}
			strmTitle += " | " + strings.Join(langs, ", ")
		}

		streams = append(streams, bmStream{
			InfoHash: strings.ToLower(item.InfoHash),
			Name:     strmName,
			Title:    strmTitle,
			BehaviorHints: &bmBehavior{
				BingeGroup: "bitmagnet|" + item.VideoResolution,
			},
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

// bmItemMatchesSeries reports whether item should be kept for the requested
// season and episode. Items with no season data are treated as whole-series
// packs and always kept. Items whose season entry has no episodes listed are
// treated as season packs and kept when the season matches.
func bmItemMatchesSeries(item bmItem, season, episode int) bool {
	if len(item.Episodes.Seasons) == 0 {
		// No episode metadata — whole-series pack; keep.
		return true
	}
	for _, s := range item.Episodes.Seasons {
		if s.Season != season {
			continue
		}
		if len(s.Episodes) == 0 {
			// Season pack.
			return true
		}
		for _, ep := range s.Episodes {
			if ep == episode {
				return true
			}
		}
	}
	return false
}

// ---- Cinemeta resolution --------------------------------------------------

// resolveCinemeta fetches title and release year from Cinemeta for the given
// content type and IMDB id. Results are cached for cineMetaTTL. Returns empty
// strings/zero on any error; the caller is responsible for fallback behaviour.
func resolveCinemeta(r *http.Request, base, contentType, imdbID string) (name string, year int) {
	if base == "" {
		return "", 0
	}
	cacheKey := contentType + ":" + imdbID

	cineMetaMu.Lock()
	if e, ok := cineMetaCache[cacheKey]; ok && time.Now().Before(e.expires) {
		cineMetaMu.Unlock()
		return e.name, e.year
	}
	cineMetaMu.Unlock()

	metaURL := strings.TrimRight(base, "/") + "/meta/" + url.PathEscape(contentType) + "/" + url.PathEscape(imdbID) + ".json"
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, metaURL, nil)
	if err != nil {
		return "", 0
	}
	resp, err := cineClient.Do(req)
	if err != nil {
		return "", 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", 0
	}

	var result struct {
		Meta struct {
			Name        string          `json:"name"`
			Year        json.RawMessage `json:"year"`
			ReleaseInfo string          `json:"releaseInfo"`
		} `json:"meta"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, cineMetaLimit)).Decode(&result); err != nil {
		return "", 0
	}

	name = result.Meta.Name

	// year may be a JSON number (2008) or a string ("2008" or "2008-2013").
	if len(result.Meta.Year) > 0 {
		raw := strings.Trim(string(result.Meta.Year), `"`)
		if len(raw) >= 4 {
			year, _ = strconv.Atoi(raw[:4])
		}
	}
	if year == 0 && len(result.Meta.ReleaseInfo) >= 4 {
		year, _ = strconv.Atoi(result.Meta.ReleaseInfo[:4])
	}

	if name != "" {
		cineMetaMu.Lock()
		cineMetaEvict() // opportunistic sweep on insert
		cineMetaCache[cacheKey] = cineMetaEntry{name: name, year: year, expires: time.Now().Add(cineMetaTTL)}
		cineMetaMu.Unlock()
	}
	cineMetaStartJanitor()

	return name, year
}

// ---- Bitmagnet GraphQL query ----------------------------------------------

// queryBitmagnet posts the search query to the Bitmagnet GraphQL endpoint and
// returns the matching items. Any network, HTTP, or GraphQL-level error is
// returned; the caller logs and returns an empty stream list.
func queryBitmagnet(r *http.Request, endpoint, queryString string) ([]bmItem, error) {
	body, err := json.Marshal(bmReq{
		Query: bmGraphQLQuery,
		Variables: map[string]any{
			"input": map[string]any{
				"queryString": queryString,
				"limit":       50,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := bmClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	var result bmResp
	if err := json.NewDecoder(io.LimitReader(resp.Body, bmRespLimit)).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("graphql: %s", result.Errors[0].Message)
	}

	return result.Data.TorrentContent.Search.Items, nil
}

// ---- helpers --------------------------------------------------------------

// humanizeSize formats a byte count as a human-readable GB or MB string.
func humanizeSize(b int64) string {
	const gb = 1 << 30
	const mb = 1 << 20
	if b >= gb {
		return fmt.Sprintf("%.2f GB", float64(b)/float64(gb))
	}
	return fmt.Sprintf("%.0f MB", float64(b)/float64(mb))
}
