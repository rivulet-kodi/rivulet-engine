// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — local-files Stremio addon at /local-addon/*.
//
// Routes:
//
//	GET /local-addon/manifest.json
//	     → Stremio addon manifest (id community.rivulet-engine.local, v1.0.0)
//	GET /local-addon/catalog/:type/:id[.json]
//	     → {metas:[{id,type,name,poster?}]} — scans LOCAL_FILES_DIR for video files
//	GET /local-addon/meta/:type/:id[.json]
//	     → {meta:{id,type,name,poster?}} for a scanned file (local: or tt id)
//	GET /local-addon/stream/:type/:id[.json]
//	     → {streams:[{url:"file://..."}]} for a scanned file
//
// IMDB resolution: filenames are parsed for title + year + season/episode via
// regexes mirroring the reference scanner.rs. The resolved title+year is queried
// against the public IMDB suggestion API:
//
//	https://v3.sg.media-imdb.com/suggestion/x/<urlencoded-title>.json
//	Response: {"d":[{"id":"tt…","l":"title","y":year,"qid":"movie|tvSeries","i":{"imageUrl":"…"}}]}
//
// Match heuristic (highest score wins):
//   - Exact title match: +10; prefix match: +5
//   - Year match (exact): +8; within 1 year: +4
//   - qid matches content type (movie/tvSeries): +3
//
// Resolution is lazy/background (goroutine per file, one attempt, 8 s timeout)
// so catalog requests are never blocked. Results are cached in-memory keyed by
// localID (SHA-256 of abs path). When an IMDB tt id is resolved it replaces the
// local: id in catalog/meta output so Stremio shows real IMDB metadata + poster.
// Falls back to local: id if resolution fails.
//
// If LOCAL_FILES_DIR is unset or empty the manifest still serves and all
// catalogs/meta/stream return empty results.
package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// videoExtsLocal is the set of extensions we consider video files.
// Deliberately a superset — unknown containers are tried.
var videoExtsLocal = map[string]bool{
	".mkv":  true,
	".mp4":  true,
	".avi":  true,
	".mov":  true,
	".m4v":  true,
	".webm": true,
	".flv":  true,
	".wmv":  true,
	".mpg":  true,
	".mpeg": true,
	".ts":   true,
	".m2ts": true,
	".ogv":  true,
	".3gp":  true,
}

// localMeta describes one scanned video file.
type localMeta struct {
	ID        string // primary Stremio id: "tt<imdb>" when resolved, else "local:<sha256hex>"
	LocalHex  string // SHA-256 hex of abs path — stable internal key for cache lookup
	Name      string // parsed clean title (from filename) or raw stem
	Path      string // absolute path
	Type      string // movie | series | other
	Poster    string // IMDB poster URL; empty until resolved
	SeriesKey string // normalized parsed series title; empty for non-series content
	Season    int
	Episode   int
}

// — Filename parsing (mirrors reference server/src/local_addon/parser.rs) —

var (
	// SxxExx: S01E02, s1e3, …
	reFileSeason = regexp.MustCompile(`(?i)[Ss](\d{1,2})[Ee](\d{2})`)
	// Alternate: 1x02, 2X12, …
	reFileAlt = regexp.MustCompile(`(?i)(\d{1,2})[xX](\d{2})`)
	// Four-digit year 19xx or 20xx.
	reFileYear = regexp.MustCompile(`\b(19|20)\d{2}\b`)
	// Common quality / release keywords that signal "movie" and truncate the title.
	reFileQuality = regexp.MustCompile(
		`(?i)\b(1080p|720p|480p|2160p|4k|bluray|bdrip|brrip|dvdrip|hdtv|webrip|web[-.]dl|xvid|x264|x265|hevc|avc)\b`,
	)
	// Filename separators to normalise to spaces.
	reFileSep = regexp.MustCompile(`[._-]+`)
)

// parsedFilename holds the results of filename analysis.
type parsedFilename struct {
	name    string // clean title
	year    int    // 0 if not found
	ctype   string // "movie" | "series" | "other"
	season  int
	episode int
}

func normalizeSeriesKey(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(name), " "))
}

func localSeriesID(seriesKey string) string {
	h := sha256.Sum256([]byte("series:" + seriesKey))
	return "local:" + hex.EncodeToString(h[:8])
}

func localSeriesKeyForID(items []localMeta, id string) (string, bool) {
	for _, m := range items {
		if m.Type != "series" || m.SeriesKey == "" {
			continue
		}
		if m.ID == id || localSeriesID(m.SeriesKey) == id {
			return m.SeriesKey, true
		}
	}
	return "", false
}

func preferredSeriesMeta(items []localMeta, seriesKey string) (localMeta, bool) {
	var fallback localMeta
	found := false

	for _, m := range items {
		if m.Type != "series" || m.SeriesKey != seriesKey {
			continue
		}
		if !found {
			fallback = m
			found = true
		}
		if strings.HasPrefix(m.ID, "tt") {
			return m, true
		}
	}

	return fallback, found
}

func dedupeLocalMetas(items []localMeta, catType string) []localMeta {
	seen := make(map[string]int)
	out := make([]localMeta, 0, len(items))

	for _, m := range items {
		if m.Type != catType {
			continue
		}

		key := m.ID
		if m.Type == "series" && m.SeriesKey != "" {
			key = "series:" + m.SeriesKey
		}

		if idx, ok := seen[key]; ok {
			if m.Type == "series" &&
				strings.HasPrefix(m.ID, "tt") &&
				!strings.HasPrefix(out[idx].ID, "tt") {
				out[idx] = m
			}
			continue
		}

		seen[key] = len(out)
		out = append(out, m)
	}

	return out
}

func localSeriesVideos(items []localMeta, id string) []map[string]any {
	seriesKey, ok := localSeriesKeyForID(items, id)
	if !ok {
		return []map[string]any{}
	}

	videoBaseID := localSeriesID(seriesKey)
	if preferred, ok := preferredSeriesMeta(items, seriesKey); ok && strings.HasPrefix(preferred.ID, "tt") {
		videoBaseID = preferred.ID
	}

	videos := make([]map[string]any, 0)
	seen := make(map[[2]int]struct{})

	for _, ep := range items {
		if ep.Type != "series" ||
			ep.SeriesKey != seriesKey ||
			ep.Season <= 0 ||
			ep.Episode <= 0 {
			continue
		}

		episodeKey := [2]int{ep.Season, ep.Episode}
		if _, ok := seen[episodeKey]; ok {
			continue
		}
		seen[episodeKey] = struct{}{}

		videos = append(videos, map[string]any{
			"id":      fmt.Sprintf("%s:%d:%d", videoBaseID, ep.Season, ep.Episode),
			"title":   fmt.Sprintf("S%02dE%02d", ep.Season, ep.Episode),
			"season":  ep.Season,
			"episode": ep.Episode,
		})
	}

	sort.Slice(videos, func(i, j int) bool {
		si, _ := videos[i]["season"].(int)
		sj, _ := videos[j]["season"].(int)
		if si != sj {
			return si < sj
		}
		ei, _ := videos[i]["episode"].(int)
		ej, _ := videos[j]["episode"].(int)
		return ei < ej
	})

	return videos
}

var reSeasonDir = regexp.MustCompile(`(?i)^(season|staffel|s)[ ._-]*\d+$`)

// bareSeriesDirName returns a series name derived from the parent directory
// (or grandparent for season dirs) when the parsed title is empty or just the
// filename stem. Returns "" when the parsed title is usable.
func bareSeriesDirName(path, stem, title string) string {
	t := normalizeSeriesKey(title)
	if t != "" && t != normalizeSeriesKey(stem) {
		return ""
	}
	dir := filepath.Dir(path)
	name := filepath.Base(dir)
	if reSeasonDir.MatchString(name) {
		dir = filepath.Dir(dir)
		name = filepath.Base(dir)
	}
	if name == "." || name == string(filepath.Separator) || name == "" {
		return ""
	}
	return name
}

func localSeriesEpisode(items []localMeta, id string) (localMeta, bool) {
	parts := strings.Split(id, ":")
	if len(parts) < 3 {
		return localMeta{}, false
	}

	season, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil {
		return localMeta{}, false
	}
	episode, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return localMeta{}, false
	}

	baseID := strings.Join(parts[:len(parts)-2], ":")
	seriesKey, ok := localSeriesKeyForID(items, baseID)
	if !ok {
		return localMeta{}, false
	}

	for _, m := range items {
		if m.Type == "series" &&
			m.SeriesKey == seriesKey &&
			m.Season == season &&
			m.Episode == episode {
			return m, true
		}
	}

	return localMeta{}, false
}

// parseFilenameToMeta extracts title, year, and content type from a video
// filename stem (without extension). Mirrors the logic of reference parser.rs.
func parseFilenameToMeta(stem string) parsedFilename {
	s := reFileSep.ReplaceAllString(stem, " ")

	var p parsedFilename
	p.ctype = "other"

	// Series detection: SxxExx or 1x02.
	if m := reFileSeason.FindStringSubmatchIndex(s); m != nil {
		p.ctype = "series"
		p.name = strings.TrimSpace(s[:m[0]])
		p.season, _ = strconv.Atoi(s[m[2]:m[3]])
		p.episode, _ = strconv.Atoi(s[m[4]:m[5]])
	} else if m := reFileAlt.FindStringSubmatchIndex(s); m != nil {
		p.ctype = "series"
		p.name = strings.TrimSpace(s[:m[0]])
		p.season, _ = strconv.Atoi(s[m[2]:m[3]])
		p.episode, _ = strconv.Atoi(s[m[4]:m[5]])
	}

	// Extract year.
	if ym := reFileYear.FindString(s); ym != "" {
		p.year, _ = strconv.Atoi(ym)
	}

	// If not series, extract title = words before first year or quality token.
	if p.ctype != "series" {
		parts := strings.Fields(s)
		var titleParts []string
		for _, part := range parts {
			if reFileYear.MatchString(part) || reFileQuality.MatchString(part) {
				break
			}
			titleParts = append(titleParts, part)
		}
		if len(titleParts) > 0 {
			p.name = strings.Join(titleParts, " ")
		} else {
			p.name = stem
		}
		if p.year > 0 || reFileQuality.MatchString(s) {
			p.ctype = "movie"
		}
	}

	if p.name == "" {
		p.name = stem
	}
	return p
}

// — IMDB resolution cache —

type imdbEntry struct {
	TTID       string    // "" means lookup ran but found no match
	Poster     string    // IMDB poster image URL
	resolvedAt time.Time // when the last lookup completed (zero for old/missing entries)
}

// imdbNegativeTTL is how long a failed IMDb lookup is cached before the resolver
// is allowed to retry it. This prevents hammering the API every scan cycle for
// files whose titles do not resolve.
const imdbNegativeTTL = 24 * time.Hour

var (
	imdbCacheMu sync.RWMutex
	imdbByLocal = map[string]imdbEntry{} // localHex → entry (set once per file)

	imdbPendMu  sync.Mutex
	imdbPending = map[string]bool{} // localHex → resolution goroutine in flight

	ttPathMu sync.RWMutex
	ttToPath = map[string]string{} // ttID → abs path (for stream/meta lookup by tt id)
)

// imdbSem caps the number of concurrent IMDB-resolution goroutines so a large
// first scan does not open thousands of outbound HTTP connections at once.
var imdbSem = make(chan struct{}, 20)

// localIMDBDisabled gates the local-files add-on's IMDB resolution
// (RIVULET_LOCAL_IMDB). Zero value = enabled; New() sets it from cfg.LocalIMDB.
var localIMDBDisabled atomic.Bool

// — Scan cache —

const scanCacheTTL = 30 * time.Second

var (
	scanCacheMu sync.Mutex
	scanCache   []localMeta
	scanCacheAt time.Time
)

// scanLocalFilesCached returns scanLocalFiles results, re-scanning only when
// the cached result is absent or older than scanCacheTTL.
func scanLocalFilesCached() []localMeta {
	scanCacheMu.Lock()
	defer scanCacheMu.Unlock()
	if scanCache != nil && time.Since(scanCacheAt) < scanCacheTTL {
		return scanCache
	}
	scanCache = scanLocalFiles()
	scanCacheAt = time.Now()
	return scanCache
}

// imdbSuggestionResp is the JSON shape returned by:
//
//	https://v3.sg.media-imdb.com/suggestion/x/<query>.json
type imdbSuggestionResp struct {
	D []struct {
		ID  string `json:"id"`  // "tt1234567"
		L   string `json:"l"`   // title label
		Y   int    `json:"y"`   // release year
		QID string `json:"qid"` // "movie" | "tvSeries" | "tvMiniSeries" | …
		I   struct {
			ImageURL string `json:"imageUrl"` // poster thumbnail
		} `json:"i"`
	} `json:"d"`
}

// resolveIMDB queries the IMDB suggestion API and returns the best-matching
// tt id and poster URL. Empty strings are returned on any failure.
//
// Suggestion URL: https://v3.sg.media-imdb.com/suggestion/x/<urlencoded>.json
//
// Scoring (highest wins):
//   - Exact title (case-insensitive):   +10
//   - Title is a prefix of result:      +5
//   - Year exact match:                 +8
//   - Year within 1:                    +4
//   - qid matches content type:         +3  (movie→"movie", series→"tvSeries")
func resolveIMDB(title string, year int, ctype string) (ttID, poster string) {
	if title == "" {
		return "", ""
	}
	apiURL := fmt.Sprintf(
		"https://v3.sg.media-imdb.com/suggestion/x/%s.json",
		url.QueryEscape(title),
	)
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 128<<10))
	if err != nil {
		return "", ""
	}

	var sug imdbSuggestionResp
	if err := json.Unmarshal(body, &sug); err != nil {
		return "", ""
	}
	if len(sug.D) == 0 {
		return "", ""
	}

	wantQID := "movie"
	if ctype == "series" {
		wantQID = "tvSeries"
	}
	titleLow := strings.ToLower(title)

	bestIdx := -1
	bestScore := 0
	for i, d := range sug.D {
		if !strings.HasPrefix(d.ID, "tt") {
			continue
		}
		score := 0
		dl := strings.ToLower(d.L)
		switch {
		case dl == titleLow:
			score += 10
		case strings.HasPrefix(dl, titleLow):
			score += 5
		}
		if year > 0 {
			diff := d.Y - year
			if diff < 0 {
				diff = -diff
			}
			switch diff {
			case 0:
				score += 8
			case 1:
				score += 4
			}
		}
		if d.QID == wantQID {
			score += 3
		}
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}

	if bestIdx < 0 {
		return "", ""
	}
	return sug.D[bestIdx].ID, sug.D[bestIdx].I.ImageURL
}

// ensureIMDBResolved triggers a background IMDB resolution goroutine for the
// given file if no result is cached and no goroutine is already in flight.
// The goroutine updates imdbByLocal and ttToPath when done.
func ensureIMDBResolved(localHex, title string, year int, ctype, absPath string) {
	if localIMDBDisabled.Load() {
		return
	}
	if title == "" {
		return
	}
	imdbCacheMu.RLock()
	e, done := imdbByLocal[localHex]
	imdbCacheMu.RUnlock()
	// Treat a recently-failed lookup as done to avoid retrying every scan cycle.
	// A successful lookup (e.TTID != "") is permanently cached; a failed one is
	// retried after imdbNegativeTTL has elapsed.
	if done && (e.TTID != "" || time.Since(e.resolvedAt) < imdbNegativeTTL) {
		return
	}

	imdbPendMu.Lock()
	if imdbPending[localHex] {
		imdbPendMu.Unlock()
		return
	}
	// Acquire a semaphore slot before spawning: when all slots are busy the
	// file is left unresolved and retried on the next scan, so a large library
	// never parks one goroutine per file waiting on the semaphore.
	select {
	case imdbSem <- struct{}{}:
	default:
		imdbPendMu.Unlock()
		return
	}
	imdbPending[localHex] = true
	imdbPendMu.Unlock()

	go func() {
		defer func() {
			<-imdbSem // release slot
			imdbPendMu.Lock()
			delete(imdbPending, localHex)
			imdbPendMu.Unlock()
		}()

		ttID, poster := resolveIMDB(title, year, ctype)

		// Always record the attempt so we don't re-fire on every scan cycle.
		// On success also register the reverse tt→path mapping.
		imdbCacheMu.Lock()
		imdbByLocal[localHex] = imdbEntry{TTID: ttID, Poster: poster, resolvedAt: time.Now()}
		imdbCacheMu.Unlock()

		if ttID != "" {
			ttPathMu.Lock()
			ttToPath[ttID] = absPath
			ttPathMu.Unlock()
		}
	}()
}

// — HTTP handlers —

// handleLocalAddon dispatches all /local-addon/* routes.
func (s *server) handleLocalAddon(w http.ResponseWriter, r *http.Request, seg []string) {
	// Honour the localAddonEnabled toggle; when off, every route returns 404.
	if enabled, _ := s.ss.Get("localAddonEnabled").(bool); !enabled {
		http.NotFound(w, r)
		return
	}
	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}
	switch seg[1] {
	case "manifest.json":
		s.localAddonManifest(w, r)

	case "catalog":
		if len(seg) < 4 {
			http.NotFound(w, r)
			return
		}
		s.localAddonCatalog(w, r, seg[2], strings.TrimSuffix(seg[3], ".json"))

	case "meta":
		if len(seg) < 4 {
			http.NotFound(w, r)
			return
		}
		s.localAddonMeta(w, r, seg[2], strings.TrimSuffix(seg[3], ".json"))

	case "stream":
		if len(seg) < 4 {
			http.NotFound(w, r)
			return
		}
		s.localAddonStream(w, r, seg[2], strings.TrimSuffix(seg[3], ".json"))

	case "file":
		if len(seg) < 3 {
			http.NotFound(w, r)
			return
		}
		s.localAddonFile(w, r, seg[2])

	default:
		http.NotFound(w, r)
	}
}

// localAddonManifest serves the Stremio addon manifest.
// idPrefixes includes both "local:" (unresolved) and "tt" (IMDB-resolved).
func (s *server) localAddonManifest(w http.ResponseWriter, r *http.Request) {
	manifest := map[string]any{
		"id":          "community.rivulet-engine.local",
		"version":     "1.0.0",
		"name":        "Local Files (Go)",
		"description": "Local video files served by rivulet-engine with IMDB resolution",
		"resources": []any{
			"catalog",
			map[string]any{
				"name":       "meta",
				"types":      []string{"movie", "series", "other"},
				"idPrefixes": []string{"local:", "tt"},
			},
			map[string]any{
				"name":       "stream",
				"types":      []string{"movie", "series", "other"},
				"idPrefixes": []string{"local:", "tt"},
			},
		},
		"types": []string{"movie", "series", "other"},
		"catalogs": []any{
			map[string]any{"type": "movie", "id": "localmovies", "name": "Local Movies"},
			map[string]any{"type": "series", "id": "localseries", "name": "Local Series"},
			map[string]any{"type": "other", "id": "localother", "name": "Local Other"},
		},
		"idPrefixes": []string{"local:", "tt"},
	}
	writeJSON(w, http.StatusOK, manifest)
}

// localAddonCatalog returns {metas:[{id,type,name,poster?}]} for catType.
// Entries use tt ids when IMDB-resolved (with poster), else local: ids.
func (s *server) localAddonCatalog(w http.ResponseWriter, r *http.Request, catType, _ string) {
	items := dedupeLocalMetas(scanLocalFilesCached(), catType)
	metas := make([]map[string]any, 0, len(items))
	for _, m := range items {
		entry := map[string]any{
			"id":   m.ID,
			"type": m.Type,
			"name": m.Name,
		}
		if m.Poster != "" {
			entry["poster"] = m.Poster
		}
		metas = append(metas, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{"metas": metas})
}

// localAddonMeta returns {meta:{id,type,name,poster?}} for id.
// Handles local: ids, tt ids (via scan + tt→path reverse map), and
// the local:hex form even when the catalog currently presents a tt id.
func (s *server) localAddonMeta(w http.ResponseWriter, r *http.Request, _, id string) {
	items := scanLocalFilesCached()

	emit := func(m localMeta, resolvedID string) {
		entry := map[string]any{
			"id":   resolvedID,
			"type": m.Type,
			"name": m.Name,
		}
		if m.Poster != "" {
			entry["poster"] = m.Poster
		}
		if m.Type == "series" {
			entry["videos"] = localSeriesVideos(items, resolvedID)
		}
		writeJSON(w, http.StatusOK, map[string]any{"meta": entry})
	}

	// Resolve series metadata through the IMDb-independent grouping key so a
	// partially resolved show still returns one series meta. A resolved IMDb id
	// is preferred when any episode in the group has one.
	if seriesKey, ok := localSeriesKeyForID(items, id); ok {
		if m, ok := preferredSeriesMeta(items, seriesKey); ok {
			emit(m, m.ID)
			return
		}
	}

	// Direct match: works for both local: and tt ids already in the scan list.
	for _, m := range items {
		if m.ID == id || "local:"+m.LocalHex == id {
			emit(m, m.ID)
			return
		}
	}

	// Fallback for tt id: look up via reverse path map, then find in scan.
	if strings.HasPrefix(id, "tt") {
		ttPathMu.RLock()
		path, ok := ttToPath[id]
		ttPathMu.RUnlock()
		if ok {
			for _, m := range items {
				if m.Path == path {
					emit(m, id)
					return
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"meta": nil})
}

// localAddonStream returns {streams:[{url:"file://..."}]} for id.
// Handles local: and tt ids; maps tt id back to the local file path.
func (s *server) localAddonStream(w http.ResponseWriter, r *http.Request, _, id string) {
	items := scanLocalFilesCached()

	makeStream := func(m localMeta) {
		stream := map[string]any{
			"title": m.Name,
			"url":   s.localFileStreamURL(m),
		}
		if s.cfg.LocalFilesPublicURL == "" {
			stream["behaviorHints"] = map[string]any{
				"notWebReady": true,
			}
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"streams": []any{stream},
		})
	}

	// Series episode IDs use:
	//   <meta-id>:<season>:<episode>
	// e.g. tt1234567:1:3 or local:<series-hash>:1:3.
	if strings.Count(id, ":") >= 2 {
		if m, ok := localSeriesEpisode(items, id); ok {
			makeStream(m)
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{"streams": []any{}})
		return
	}

	// Direct match.
	for _, m := range items {
		if m.ID == id || "local:"+m.LocalHex == id {
			makeStream(m)
			return
		}
	}

	// Fallback for tt id via reverse path map.
	if strings.HasPrefix(id, "tt") {
		ttPathMu.RLock()
		path, ok := ttToPath[id]
		ttPathMu.RUnlock()
		if ok {
			for _, m := range items {
				if m.Path == path {
					makeStream(m)
					return
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"streams": []any{}})
}

// localFileStreamURL returns an HTTP URL for remote clients when configured,
// otherwise preserving the historical file:// URL used for same-host playback.
func (s *server) localFileStreamURL(m localMeta) string {
	if s.cfg.LocalFilesPublicURL == "" {
		return fileURL(m.Path)
	}

	return s.cfg.LocalFilesPublicURL + "/local-addon/file/" + m.LocalHex
}

// localAddonFile serves only files already present in the local add-on index.
// Using the opaque local hash instead of accepting a filesystem path prevents
// arbitrary path access. http.ServeFile provides HEAD and byte-range support.
//
// The endpoint is opt-in: it only exists when RIVULET_LOCAL_FILES_PUBLIC_URL
// is set. Without it the add-on emits same-host file:// URLs and nothing needs
// HTTP access, so indexed files are not downloadable by other hosts that can
// reach the (by default all-interfaces) listener.
func (s *server) localAddonFile(w http.ResponseWriter, r *http.Request, localHex string) {
	if s.cfg.LocalFilesPublicURL == "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	items := scanLocalFilesCached()
	for _, m := range items {
		if m.LocalHex != localHex {
			continue
		}

		http.ServeFile(w, r, m.Path)
		return
	}

	http.NotFound(w, r)
}

// fileURL builds a properly percent-encoded file:// URL from an absolute path.
// Each path segment is encoded via url.PathEscape so spaces and special
// characters are safe for URL parsers, while directory separators (/) are preserved.
func fileURL(absPath string) string {
	segs := strings.Split(absPath, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return "file://" + strings.Join(segs, "/")
}

// scanLocalFiles walks LOCAL_FILES_DIR and returns a list of localMeta entries.
// For each file the filename is parsed for title/year/type; IMDB resolution is
// triggered lazily in a background goroutine. The catalog reflects whatever is
// in the cache at call time — on subsequent calls (or the next catalog refresh)
// resolved tt ids will appear.
func scanLocalFiles() []localMeta {
	root := os.Getenv("LOCAL_FILES_DIR")
	if root == "" {
		return nil
	}

	var items []localMeta
	seen := make(map[string]struct{}) // localHex values present in this scan
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if !videoExtsLocal[ext] {
			return nil
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			abs = path
		}
		stem := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		hex := localID(abs)
		seen[hex] = struct{}{}
		parsed := parseFilenameToMeta(stem)
		if parsed.ctype == "series" {
			if dirName := bareSeriesDirName(path, stem, parsed.name); dirName != "" {
				parsed.name = dirName
			}
		}

		// Check IMDB cache first.
		imdbCacheMu.RLock()
		entry, resolved := imdbByLocal[hex]
		imdbCacheMu.RUnlock()

		seriesKey := ""
		if parsed.ctype == "series" {
			seriesKey = normalizeSeriesKey(parsed.name)
		}

		var id, poster string
		if resolved && entry.TTID != "" {
			id = entry.TTID
			poster = entry.Poster
			// Keep reverse map current so stream/meta lookups by tt id work.
			ttPathMu.Lock()
			ttToPath[entry.TTID] = abs
			ttPathMu.Unlock()
		} else {
			if parsed.ctype == "series" && seriesKey != "" {
				id = localSeriesID(seriesKey)
			} else {
				id = "local:" + hex
			}
			// Trigger background IMDB resolution if not already in flight.
			ensureIMDBResolved(hex, parsed.name, parsed.year, parsed.ctype, abs)
		}

		name := parsed.name
		if name == "" {
			name = stem
		}

		items = append(items, localMeta{
			ID:        id,
			LocalHex:  hex,
			Name:      name,
			Path:      abs,
			Type:      parsed.ctype,
			Poster:    poster,
			SeriesKey: seriesKey,
			Season:    parsed.season,
			Episode:   parsed.episode,
		})
		return nil
	})
	pruneLocalCaches(seen, len(items))
	return items
}

// localCacheSlack is how many stale (no longer on disk) entries the IMDb and
// tt→path caches may hold above the live library size before a scan prunes
// them. It keeps the common case (stable library) free of any pruning work.
const localCacheSlack = 1024

// pruneLocalCaches drops IMDb/tt-path cache entries for files that no longer
// exist, so a library with churning files cannot grow imdbByLocal/ttToPath
// without bound. It never prunes after an empty scan (an unmounted or
// temporarily unreadable library must not discard resolved ids) and only once
// the caches exceed the live size by localCacheSlack.
func pruneLocalCaches(seenHex map[string]struct{}, live int) {
	if live == 0 {
		return
	}
	imdbCacheMu.Lock()
	if len(imdbByLocal) > live+localCacheSlack {
		for k := range imdbByLocal {
			if _, ok := seenHex[k]; !ok {
				delete(imdbByLocal, k)
			}
		}
	}
	imdbCacheMu.Unlock()

	// ttToPath values are absolute paths; localID(path) is the imdbByLocal key.
	ttPathMu.Lock()
	if len(ttToPath) > live+localCacheSlack {
		for tt, p := range ttToPath {
			if _, ok := seenHex[localID(p)]; !ok {
				delete(ttToPath, tt)
			}
		}
	}
	ttPathMu.Unlock()
}

// localID returns a short SHA-256 hex of the absolute path, used as the
// stable identifier component after "local:". 16 hex chars (8 bytes) gives
// negligible collision probability for local libraries.
func localID(absPath string) string {
	h := sha256.Sum256([]byte(absPath))
	return hex.EncodeToString(h[:8])
}
