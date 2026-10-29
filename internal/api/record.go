// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

// MediaFlow/EasyProxy-compatible DVR. Contract matched (EasyProxy
// routes/recordings.py + services/recording_manager.py and the
// addon-side DVR client):
//
//	GET    /record?url=&name=&duration=[&extractor=|host=][&proxy=][&api_password=]
//	         starts a background recording, 302s to the live proxy URL
//	GET    /record/stop/{id}            stop, 302 to .../api/recordings/{id}/stream
//	GET    /api/recordings[?status=]    {"recordings":[...],"active_count":N,"system_stats":{}}
//	GET    /api/recordings/active       {"recordings":[...]}
//	POST   /api/recordings/start        JSON {url,name,duration,extractor,proxy} -> 201 recording
//	GET    /api/recordings/{id}         recording | 404
//	POST   /api/recordings/{id}/stop    recording | 404
//	DELETE /api/recordings/{id}         {"success":true} | 404
//	GET    /api/recordings/{id}/delete  text/plain (GET-only players)
//	DELETE /api/recordings[/all][?status=]  {"success":true,"deleted":N}
//	GET    /api/recordings/{id}/stream    video/mp2t, Range when finished, live-follow when active
//	GET    /api/recordings/{id}/download  attachment, Range
//
// Everything requires api_password (query or X-Api-Password) when
// RIVULET_PROXY_PASSWORD is set, through the same Authorize helper /proxy uses.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/rivulet-kodi/rivulet-engine/internal/dvr"
	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/streamproxy"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// Test seams: nil means production behaviour (exec ffmpeg found via PATH).
var (
	dvrStartFunc dvr.StartFunc
	dvrLookPath  func(string) (string, error)
)

// newDVR builds the recording manager from cfg; (nil, nil) when DVR is off.
func newDVR(cfg types.Config) (*dvr.Manager, error) {
	if !cfg.DVREnabled {
		return nil, nil
	}
	dir := cfg.DVRDir
	if dir == "" {
		dir = filepath.Join(cfg.AppPath, "recordings")
	}
	m, err := dvr.New(dvr.Config{
		Dir:             dir,
		DefaultDuration: cfg.DVRDefaultDuration,
		MaxDuration:     cfg.DVRMaxDuration,
		MaxActive:       cfg.DVRMaxActive,
		MaxBytes:        cfg.DVRMaxBytes,
		Retention:       time.Duration(cfg.DVRRetentionDays) * 24 * time.Hour,
		Start:           dvrStartFunc,
		LookPath:        dvrLookPath,
	})
	if err != nil {
		return nil, err
	}
	log := logging.For("dvr")
	log.Info("dvr enabled", "dir", m.Dir(), "max_active", m.MaxActive(), "max_duration", m.MaxDuration().String(),
		"ffmpeg", m.FFmpegAvailable())
	if cfg.ProxyPassword == "" && cfg.ProxyIPACL == "" {
		log.Warn("dvr is enabled without RIVULET_PROXY_PASSWORD or RIVULET_PROXY_IP_ACL: anyone who can reach this server can record and read recordings")
	}
	return m, nil
}

// dvrRoute reports whether the request addresses the DVR route family.
func dvrRoute(r *http.Request) bool {
	p := strings.Trim(r.URL.Path, "/")
	return p == "record" || strings.HasPrefix(p, "record/") || p == "api/recordings" || strings.HasPrefix(p, "api/recordings/")
}

// dvrSideEffecting reports whether a DVR request mutates state or spawns a
// process: /record, /record/stop/*, non-GET methods and GET .../delete.
// Playback (/stream, /download) and JSON reads are not, so players keep
// working cross-site while a hostile page cannot start/stop/delete by <img>.
func dvrSideEffecting(r *http.Request) bool {
	p := strings.Trim(r.URL.Path, "/")
	if p == "record" || strings.HasPrefix(p, "record/") {
		return true
	}
	if !dvrRoute(r) {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return true
	}
	return strings.HasSuffix(p, "/delete")
}

func (s *server) selfBase() string {
	if s.cfg.SelfURL != "" {
		return strings.TrimRight(s.cfg.SelfURL, "/")
	}
	return fmt.Sprintf("http://127.0.0.1:%d", s.cfg.HTTPPort)
}

func dvrJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, v)
}

func dvrErr(w http.ResponseWriter, code int, msg string) {
	dvrJSON(w, code, map[string]string{"error": msg})
}

// dvrGate rejects the request when DVR is off or unavailable, or the caller is
// not authorised. It reports whether the handler may proceed.
func (s *server) dvrGate(w http.ResponseWriter, r *http.Request) bool {
	if !s.cfg.DVREnabled {
		dvrErr(w, http.StatusNotFound, "DVR is disabled")
		return false
	}
	if s.dvr == nil {
		dvrErr(w, http.StatusServiceUnavailable, "DVR unavailable: "+s.dvrInitErr)
		return false
	}
	ar := r
	if hv := r.Header.Get("X-Api-Password"); hv != "" && r.URL.Query().Get("api_password") == "" {
		ar = r.Clone(r.Context())
		q := ar.URL.Query()
		q.Set("api_password", hv)
		ar.URL.RawQuery = q.Encode()
	}
	if err := s.sp.Authorize(ar); err != nil {
		if streamproxy.IsForbidden(err) {
			dvrErr(w, http.StatusForbidden, "Forbidden")
		} else {
			dvrErr(w, http.StatusUnauthorized, "Unauthorized")
		}
		return false
	}
	return true
}

// handleRecord serves /record and /record/stop/{id}.
func (s *server) handleRecord(w http.ResponseWriter, r *http.Request, seg []string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		dvrErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.dvrGate(w, r) {
		return
	}
	switch {
	case len(seg) == 1:
		s.recordViaGet(w, r)
	case len(seg) == 3 && seg[1] == "stop":
		s.stopAndStream(w, r, seg[2])
	default:
		http.NotFound(w, r)
	}
}

func parseDurationSeconds(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, errors.New("Duration must be a number") //nolint:staticcheck // ST1005: verbatim EasyProxy message clients may match on
	}
	if n <= 0 {
		return 0, nil // default
	}
	const ceiling = 10 * 365 * 24 * 3600
	if n > ceiling {
		n = ceiling
	}
	return time.Duration(n) * time.Second, nil
}

// startRecording resolves src, launches the recorder and returns the
// proxy-relative URL a player can watch live. It writes the error response
// itself and returns ok=false on failure.
func (s *server) startRecording(w http.ResponseWriter, r *http.Request, src streamproxy.RecordSource, name string, dur time.Duration) (rec dvr.Recording, watch string, dup, ok bool) {
	if !s.dvr.FFmpegAvailable() {
		dvrErr(w, http.StatusServiceUnavailable, "ffmpeg not found: DVR recording requires ffmpeg in PATH")
		return dvr.Recording{}, "", false, false
	}
	rel, err := s.sp.ResolveRecordSource(r, src)
	if err != nil {
		var re *streamproxy.RecordError
		if errors.As(err, &re) {
			dvrErr(w, re.Status, re.Msg)
		} else {
			dvrErr(w, http.StatusBadRequest, err.Error())
		}
		return dvr.Recording{}, "", false, false
	}
	rec, err = s.dvr.Start(dvr.StartRequest{Name: name, Source: src.URL, Input: s.selfBase() + rel, Duration: dur})
	switch {
	case err == nil:
		return rec, rel, false, true
	case errors.Is(err, dvr.ErrDuplicate):
		return rec, rel, true, true
	case errors.Is(err, dvr.ErrNoFFmpeg):
		dvrErr(w, http.StatusServiceUnavailable, "ffmpeg not found: DVR recording requires ffmpeg in PATH")
	case errors.Is(err, dvr.ErrBusy):
		w.Header().Set("Retry-After", "60")
		dvrErr(w, http.StatusTooManyRequests, fmt.Sprintf("too many active recordings (max %d)", s.dvr.MaxActive()))
	case errors.Is(err, dvr.ErrDiskFull):
		dvrErr(w, http.StatusInsufficientStorage, "recordings storage quota reached")
	default:
		logging.For("dvr").Warn("start failed", "err", err)
		dvrErr(w, http.StatusInternalServerError, "Failed to start recording")
	}
	return dvr.Recording{}, "", false, false
}

// recordViaGet is GET /record: start in the background, then redirect to the
// live stream so the player starts watching immediately (EasyProxy "Smart Mode").
func (s *server) recordViaGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dur, err := parseDurationSeconds(q.Get("duration"))
	if err != nil {
		dvrErr(w, http.StatusBadRequest, err.Error())
		return
	}
	host := q.Get("extractor")
	if host == "" {
		host = q.Get("host")
	}
	rec, rel, _, ok := s.startRecording(w, r, streamproxy.RecordSource{URL: q.Get("url"), Host: host, Proxy: q.Get("proxy")}, q.Get("name"), dur)
	if !ok {
		return
	}
	// rel is always a path on this server; refuse anything that could be read
	// as a scheme-relative or absolute URL.
	if !strings.HasPrefix(rel, "/") || strings.HasPrefix(rel, "//") || strings.HasPrefix(rel, "/\\") {
		dvrErr(w, http.StatusInternalServerError, "invalid redirect target")
		return
	}
	w.Header().Set("X-Recording-Id", rec.ID)
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, rel, http.StatusFound)
}

// dvrBase is the externally visible base for absolute redirects.
func (s *server) dvrBase(r *http.Request) string {
	switch {
	case s.cfg.ProxyPublicURL != "":
		return strings.TrimRight(s.cfg.ProxyPublicURL, "/")
	case s.cfg.PublicURL != "":
		return strings.TrimRight(s.cfg.PublicURL, "/")
	}
	return streamproxy.ExternalBase(r)
}

// stopAndStream is GET /record/stop/{id}: stop if active, then redirect to the
// recording's stream URL (absolute, for Stremio players).
func (s *server) stopAndStream(w http.ResponseWriter, r *http.Request, id string) {
	if !dvr.ValidID(id) {
		dvrErr(w, http.StatusNotFound, "Recording not found")
		return
	}
	rec, ok := s.dvr.Get(id)
	if !ok {
		dvrErr(w, http.StatusNotFound, "Recording not found")
		return
	}
	if rec.IsActive {
		if rec, _ = s.dvr.Stop(id); rec.ID == "" {
			dvrErr(w, http.StatusNotFound, "Recording not found")
			return
		}
	}
	if rec.FileSizeBytes == nil || *rec.FileSizeBytes == 0 {
		dvrErr(w, http.StatusNotFound, "Recording file not available yet")
		return
	}
	target := s.dvrBase(r) + "/api/recordings/" + id + "/stream"
	if s.cfg.ProxyPassword != "" {
		target += "?api_password=" + url.QueryEscape(s.cfg.ProxyPassword)
	}
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, target, http.StatusFound)
}

// handleRecordings serves /api/recordings...; seg[0]=="api", seg[1]=="recordings".
func (s *server) handleRecordings(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) < 2 || seg[1] != "recordings" {
		http.NotFound(w, r)
		return
	}
	if !s.dvrGate(w, r) {
		return
	}
	get := r.Method == http.MethodGet || r.Method == http.MethodHead
	switch len(seg) {
	case 2:
		switch {
		case get:
			s.listRecordings(w, r)
		case r.Method == http.MethodDelete:
			s.deleteAllRecordings(w, r)
		default:
			dvrErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case 3:
		switch {
		case seg[2] == "active" && get:
			dvrJSON(w, http.StatusOK, map[string]any{"recordings": nonNil(s.dvr.Active())})
		case seg[2] == "start" && r.Method == http.MethodPost:
			s.startViaAPI(w, r)
		case seg[2] == "all" && r.Method == http.MethodDelete:
			s.deleteAllRecordings(w, r)
		case seg[2] == "active" || seg[2] == "start" || seg[2] == "all":
			dvrErr(w, http.StatusMethodNotAllowed, "method not allowed")
		case get:
			if rec, ok := s.recordingByID(w, seg[2]); ok {
				dvrJSON(w, http.StatusOK, rec)
			}
		case r.Method == http.MethodDelete:
			s.deleteRecording(w, seg[2], false)
		default:
			dvrErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case 4:
		id := seg[2]
		switch {
		case seg[3] == "stop" && r.Method == http.MethodPost:
			rec, err := s.dvr.Stop(id)
			if err != nil || !dvr.ValidID(id) {
				dvrErr(w, http.StatusNotFound, "Recording not found or already stopped")
				return
			}
			dvrJSON(w, http.StatusOK, rec)
		case seg[3] == "delete" && get:
			s.deleteRecording(w, id, true)
		case seg[3] == "stream" && get:
			s.serveRecording(w, r, id, false)
		case seg[3] == "download" && get:
			s.serveRecording(w, r, id, true)
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}

func nonNil(v []dvr.Recording) []dvr.Recording {
	if v == nil {
		return []dvr.Recording{}
	}
	return v
}

func (s *server) recordingByID(w http.ResponseWriter, id string) (dvr.Recording, bool) {
	if dvr.ValidID(id) {
		if rec, ok := s.dvr.Get(id); ok {
			return rec, true
		}
	}
	dvrErr(w, http.StatusNotFound, "Recording not found")
	return dvr.Recording{}, false
}

func (s *server) listRecordings(w http.ResponseWriter, r *http.Request) {
	recs := nonNil(s.dvr.List(r.URL.Query().Get("status")))
	active := 0
	for _, rec := range recs {
		if rec.IsActive {
			active++
		}
	}
	dvrJSON(w, http.StatusOK, map[string]any{
		"recordings":   recs,
		"active_count": active,
		"system_stats": map[string]any{
			"recordings_bytes":     s.dvr.UsedBytes(),
			"max_active":           s.dvr.MaxActive(),
			"max_duration_seconds": int64(s.dvr.MaxDuration() / time.Second),
			"ffmpeg":               s.dvr.FFmpegAvailable(),
		},
	})
}

type startBody struct {
	URL       string          `json:"url"`
	Name      string          `json:"name"`
	Duration  json.RawMessage `json:"duration"`
	Extractor string          `json:"extractor"`
	Host      string          `json:"host"`
	Proxy     string          `json:"proxy"`
}

// startViaAPI is POST /api/recordings/start (background only, no redirect).
func (s *server) startViaAPI(w http.ResponseWriter, r *http.Request) {
	var b startBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&b); err != nil {
		dvrErr(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if strings.TrimSpace(b.URL) == "" {
		dvrErr(w, http.StatusBadRequest, "URL is required")
		return
	}
	dur, err := parseDurationSeconds(strings.Trim(strings.TrimSpace(string(b.Duration)), `"`))
	if err != nil {
		dvrErr(w, http.StatusBadRequest, err.Error())
		return
	}
	host := b.Extractor
	if host == "" {
		host = b.Host
	}
	rec, _, dup, ok := s.startRecording(w, r, streamproxy.RecordSource{URL: b.URL, Host: host, Proxy: b.Proxy}, b.Name, dur)
	if !ok {
		return
	}
	// A repeat of an already running source answers 200 with that recording.
	code := http.StatusCreated
	if dup {
		code = http.StatusOK
	}
	dvrJSON(w, code, rec)
}

func (s *server) deleteRecording(w http.ResponseWriter, id string, plain bool) {
	if !dvr.ValidID(id) || s.dvr.Delete(id) != nil {
		dvrErr(w, http.StatusNotFound, "Recording not found")
		return
	}
	if plain {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, "Recording deleted successfully. Close this and refresh the catalog.")
		return
	}
	dvrJSON(w, http.StatusOK, map[string]bool{"success": true})
}

func (s *server) deleteAllRecordings(w http.ResponseWriter, r *http.Request) {
	n := s.dvr.DeleteAll(r.URL.Query().Get("status"))
	dvrJSON(w, http.StatusOK, map[string]any{"success": true, "deleted": n})
}

// serveRecording serves the media file. Finished recordings support Range via
// http.ServeContent; an active one is streamed live while the file grows.
func (s *server) serveRecording(w http.ResponseWriter, r *http.Request, id string, download bool) {
	if !dvr.ValidID(id) {
		dvrErr(w, http.StatusNotFound, "Recording not found")
		return
	}
	f, rec, err := s.dvr.Open(id)
	if err != nil {
		if _, ok := s.dvr.Get(id); ok {
			dvrErr(w, http.StatusNotFound, "Recording file not found")
		} else {
			dvrErr(w, http.StatusNotFound, "Recording not found")
		}
		return
	}
	defer func() { _ = f.Close() }()
	h := w.Header()
	h.Set("Content-Type", "video/mp2t")
	h.Set("X-Content-Type-Options", "nosniff")
	if download {
		h.Set("Content-Disposition", `attachment; filename="`+rec.FilePath+`"`)
	}
	if rec.IsActive && !download {
		s.followRecording(w, r, f, id)
		return
	}
	st, err := f.Stat()
	if err != nil {
		dvrErr(w, http.StatusNotFound, "Recording file not found")
		return
	}
	http.ServeContent(w, r, rec.FilePath, st.ModTime(), f)
}

// followRecording streams a growing recording from the start until it ends.
func (s *server) followRecording(w http.ResponseWriter, r *http.Request, f io.Reader, id string) {
	h := w.Header()
	h.Set("Cache-Control", "no-cache")
	h.Set("Accept-Ranges", "none")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	rc := http.NewResponseController(w)
	buf := make([]byte, 64<<10)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			_ = rc.Flush()
		}
		if n > 0 && err == nil {
			continue
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return
		}
		if !s.dvr.IsActive(id) {
			// Recorder finished: drain whatever it flushed last.
			for {
				n, err := f.Read(buf)
				if n > 0 {
					if _, werr := w.Write(buf[:n]); werr != nil {
						return
					}
				}
				if n == 0 || err != nil {
					return
				}
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-tick.C:
		}
	}
}
