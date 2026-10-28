// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api — DLNA/UPnP casting device discovery and AVTransport control.
//
// GET /casting         → discover UPnP MediaRenderers, return
//
//	JSON array [{id, name, type, model, location}] (may be empty)
//
// GET /casting/:id     → single device by UDN/USN id (404 if unknown)
// GET|POST /casting/:id/player[/:cmd] → AVTransport:1 control
//
// Discovery: Uses goupnp to search for urn:schemas-upnp-org:device:MediaRenderer:1
// devices on the LAN. Only devices that expose an AVTransport:1 service are
// listed. Results are cached for 30 seconds; /casting triggers discovery only
// on a cache miss so cache hits never block.
//
// Player control (UPnP AVTransport:1 via goupnp):
//
//	Command → AVTransport action
//	  load / setUrl → SetAVTransportURI(CurrentURI, DIDL-Lite meta) then Play,
//	                   then Seek(REL_TIME) if the body's "time" (ms) > 0
//	  play          → Play(Speed=1)
//	  pause         → Pause
//	  stop          → Stop (core sends this as {"source": null})
//	  seek          → Seek(Unit=REL_TIME, Target=HH:MM:SS from "time" param)
//	  subtitles     → acknowledged only; no standard AVTransport:1 action
//	                   exists for live subtitle track/offset changes
//	  status        → GetPositionInfo → {trackDuration, relTime, trackURI}
//
// Command resolution: (1) path suffix /casting/:id/player/:cmd, (2) ?command=,
// (3) inferred from param presence (source→load, null source→stop,
// subtitlesSrc/subtitlesDelay→subtitles, stop→stop, etc.).
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/huin/goupnp"
	"github.com/huin/goupnp/dcps/av1"
	"golang.org/x/sync/singleflight"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// upnpMaxBody caps any single UPnP description/SOAP response body.
const upnpMaxBody = 1 << 20

// upnpHTTPClient is the only HTTP client used for UPnP description fetches and
// SOAP control. SSDP Location and control URLs are attacker-influenced, so the
// dialer admits only LAN addresses (private/link-local, never loopback,
// unspecified or cloud-metadata), no proxies and no redirects, and response
// bodies are size-bounded.
var upnpHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &limitedBodyTransport{
		rt: &http.Transport{
			Proxy: nil,
			DialContext: (&net.Dialer{
				Timeout: 3 * time.Second,
				Control: upnpDialControl,
			}).DialContext,
			ResponseHeaderTimeout: 5 * time.Second,
			DisableKeepAlives:     true,
		},
		max: upnpMaxBody,
	},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

func init() {
	// goupnp fetches device descriptions with this package-level client.
	goupnp.HTTPClientDefault = upnpHTTPClient
}

// validateUPnPIP reports whether ip is an acceptable UPnP peer: a LAN
// (private or link-local) unicast address that is not loopback, unspecified
// or the cloud-metadata address.
func validateUPnPIP(ip net.IP) error {
	switch {
	case ip == nil:
		return fmt.Errorf("upnp: not an IP address")
	case ip.IsLoopback() || ip.IsUnspecified() || ip.IsMulticast():
		return fmt.Errorf("upnp: blocked address %s", ip)
	case netguard.IsCloudMetadata(ip):
		return fmt.Errorf("upnp: blocked cloud-metadata address %s", ip)
	case !netguard.IsPrivate(ip) && !ip.IsLinkLocalUnicast():
		return fmt.Errorf("upnp: non-LAN address %s", ip)
	}
	return nil
}

// upnpDialControl validates the resolved IP at connect time (rebinding-safe).
func upnpDialControl(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("upnp: cannot parse dial address %q: %w", address, err)
	}
	return validateUPnPIP(net.ParseIP(host))
}

// upnpLocationOK accepts only http(s) Location URLs with a host.
func upnpLocationOK(u *url.URL) bool {
	return u != nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

// upnpSameHost reports whether b has the same hostname as a (port may differ,
// devices commonly serve control on another port) and is http(s).
func upnpSameHost(a, b *url.URL) bool {
	if a == nil || b == nil || (b.Scheme != "http" && b.Scheme != "https") {
		return false
	}
	return strings.EqualFold(a.Hostname(), b.Hostname())
}

// limitedBodyTransport caps response bodies at max bytes.
type limitedBodyTransport struct {
	rt  http.RoundTripper
	max int64
}

func (t *limitedBodyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.ContentLength > t.max {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("upnp: response body too large (%d bytes)", resp.ContentLength)
	}
	resp.Body = struct {
		io.Reader
		io.Closer
	}{io.LimitReader(resp.Body, t.max), resp.Body}
	return resp, nil
}

// CastingDevice is one UPnP MediaRenderer discovered on the LAN.
type CastingDevice struct {
	ID       string `json:"id"`       // USN from SSDP (unique service name) or UDN
	Name     string `json:"name"`     // friendlyName from device description
	Type     string `json:"type"`     // always "dlna" — required by stremio-core PlaybackDevice
	Model    string `json:"model"`    // modelName from device description
	Location string `json:"location"` // URL of device description
}

// Package-level device cache — shared across all requests.
var (
	devicesMu     sync.RWMutex
	deviceCache   []CastingDevice
	deviceCachAt  time.Time
	deviceClients map[string]*av1.AVTransport1 // device id → AVTransport client

	// discoverGroup dedupes concurrent cache-miss discoveries: only one SSDP
	// search runs at a time; all concurrent callers share its result.
	discoverGroup singleflight.Group
)

const (
	ssdpSearchTarget = "urn:schemas-upnp-org:device:MediaRenderer:1"
	deviceCacheTTL   = 30 * time.Second
	discoverTimeout  = 10 * time.Second // 2 s SSDP + sequential desc fetches
	soapTimeout      = 5 * time.Second
)

// handleCasting dispatches /casting, /casting/:id, /casting/:id/player[/:cmd].
func (s *server) handleCasting(w http.ResponseWriter, r *http.Request, seg []string) {
	// DLNA is opt-in (RIVULET_ENABLE_DLNA). When disabled, advertise no devices
	// so clients show nothing castable, and 404 any device/control sub-route — no
	// SSDP discovery runs in this mode.
	if !s.cfg.EnableDLNA {
		if len(seg) == 1 {
			writeJSON(w, http.StatusOK, []CastingDevice{})
		} else {
			http.NotFound(w, r)
		}
		return
	}
	switch {
	case len(seg) == 1:
		// GET /casting — run discovery and return device list.
		devices := refreshDevices()
		writeJSON(w, http.StatusOK, devices)

	case len(seg) == 2:
		// GET /casting/:id — single device details.
		devID := seg[1]
		devices := refreshDevices()
		var found *CastingDevice
		for i := range devices {
			if devices[i].ID == devID {
				found = &devices[i]
				break
			}
		}
		if found == nil {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, found)

	case len(seg) >= 3 && seg[2] == "player":
		// GET|POST /casting/:id/player[/:cmd] — AVTransport control.
		s.handlePlayerControl(w, r, seg)

	default:
		http.NotFound(w, r)
	}
}

// handlePlayerControl implements GET|POST /casting/:id/player[/:cmd].
//
// Command dispatch via goupnp AVTransport:1 client. The command may come from:
//   - Path suffix:    /casting/:id/player/play
//   - Query param:    ?command=play
//   - Param inference: ?source=… → load (null → stop), ?stop=1 → stop, ?paused=true → pause, ?time=N → seek
//
// All AVTransport calls time out after 5 s.
func (s *server) handlePlayerControl(w http.ResponseWriter, r *http.Request, seg []string) {
	devID := seg[1]

	// Locate the device in the cache.
	devices := refreshDevices()
	var dev *CastingDevice
	for i := range devices {
		if devices[i].ID == devID {
			dev = &devices[i]
			break
		}
	}
	if dev == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": fmt.Sprintf("device %q not found", devID),
		})
		return
	}

	// Retrieve the AVTransport client built during discovery.
	devicesMu.RLock()
	client := deviceClients[devID]
	devicesMu.RUnlock()

	if client == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "device has no AVTransport service; either not a MediaRenderer or description XML unavailable",
		})
		return
	}

	params := castingParams(r)
	cmd := castingCommand(seg, params)

	switch cmd {
	case "load", "seturl":
		s.castingLoad(w, client, dev, params)
	case "play":
		s.castingPlay(w, client, dev)
	case "pause":
		s.castingPause(w, client, dev)
	case "stop":
		s.castingStop(w, client, dev)
	case "seek":
		s.castingSeek(w, client, dev, params)
	case "subtitles":
		s.castingSubtitles(w, dev, params)
	case "status":
		s.castingStatus(w, client, dev)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": fmt.Sprintf("unknown command %q; valid: load, setUrl, play, pause, stop, seek, subtitles, status", cmd),
		})
	}
}

// castingParams returns consolidated query/form/JSON params for a casting request.
//
// GET → query string. POST → x-www-form-urlencoded form, OR — when the request
// carries Content-Type: application/json (as stremio-core does for
// /casting/{id}/player with {"source","time"}) — the decoded JSON body merged
// onto any query params. Numbers are preserved verbatim via UseNumber so a u64
// `time` is never mangled into float/scientific notation.
func castingParams(r *http.Request) url.Values {
	if r.Method != http.MethodPost {
		return r.URL.Query()
	}
	if ct := r.Header.Get("Content-Type"); strings.Contains(strings.ToLower(ct), "application/json") {
		params := url.Values{}
		for k, vs := range r.URL.Query() {
			params[k] = append(params[k], vs...)
		}
		var obj map[string]any
		dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
		dec.UseNumber()
		if err := dec.Decode(&obj); err == nil {
			for k, v := range obj {
				switch val := v.(type) {
				case string:
					params.Set(k, val)
				case json.Number:
					params.Set(k, val.String())
				case bool:
					params.Set(k, strconv.FormatBool(val))
				case nil:
					// Explicit JSON null (core sends Stop as {"source":null}
					// and a cleared subtitlesSrc as {"subtitlesSrc":null,…};
					// see casting.rs:288 and msg/action.rs CastingSubtitles).
					// Record presence with an empty-but-non-nil slice:
					// params.Get can't distinguish "explicit null" from
					// "absent", so isJSONNull checks the map directly.
					params[k] = []string{}
				}
			}
		}
		return params
	}
	_ = r.ParseForm()
	return r.Form
}

// isJSONNull reports whether key was present in the JSON body with an
// explicit null value, as recorded by castingParams. url.Values.Get cannot
// distinguish "explicit null" from "absent" or "empty string", so this
// checks the underlying map for a present-but-zero-length slice.
func isJSONNull(params url.Values, key string) bool {
	vs, ok := params[key]
	return ok && len(vs) == 0
}

// castingCommand resolves the player command from the path suffix or params.
func castingCommand(seg []string, params url.Values) string {
	// RESTful suffix: /casting/:id/player/:cmd
	if len(seg) >= 4 && seg[3] != "" {
		return strings.ToLower(seg[3])
	}
	// Explicit command= param.
	if c := params.Get("command"); c != "" {
		return strings.ToLower(c)
	}
	// Core sends Stop as the bare {"source": null} (casting.rs cast_request);
	// without this check it falls through to "status" instead (COMPAT-2).
	if isJSONNull(params, "source") {
		return "stop"
	}
	// Infer from param presence (mirrors reference PlayerParams semantics).
	if params.Get("source") != "" {
		return "load"
	}
	// Core sends subtitle updates as the bare CastingSubtitles object
	// ({"subtitlesSrc":…,"subtitlesDelay":…}, msg/action.rs), with no
	// source/command/stop/paused/time field of its own.
	if _, ok := params["subtitlesSrc"]; ok {
		return "subtitles"
	}
	if _, ok := params["subtitlesDelay"]; ok {
		return "subtitles"
	}
	if params.Get("stop") != "" {
		return "stop"
	}
	if strings.EqualFold(params.Get("paused"), "true") {
		return "pause"
	}
	if params.Get("time") != "" {
		return "seek"
	}
	return "status"
}

// castingLoad sends SetAVTransportURI(source) followed by Play.
// AVTransport actions: SetAVTransportURI → Play.
func (s *server) castingLoad(w http.ResponseWriter, client *av1.AVTransport1, dev *CastingDevice, params url.Values) {
	mediaURI := params.Get("source")
	if mediaURI == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing source param"})
		return
	}
	// Reject non-http(s) schemes before passing the URI to the device.
	if u, err := url.Parse(mediaURI); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "source must be an http or https URL"})
		return
	}

	// Build DIDL-Lite metadata. The URI inside <res> is XML-escaped because it
	// is embedded in manually-constructed XML. The full DIDL string is passed
	// raw to SetAVTransportURI — goupnp encodes it for the SOAP body.
	didl := fmt.Sprintf(
		`<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/"`+
			` xmlns:dc="http://purl.org/dc/elements/1.1/"`+
			` xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/">`+
			`<item id="1" parentID="0" restricted="1">`+
			`<dc:title>Stream</dc:title>`+
			`<upnp:class>object.item.videoItem</upnp:class>`+
			`<res>%s</res>`+
			`</item></DIDL-Lite>`,
		xmlEscape(mediaURI),
	)

	ctx, cancel := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel()
	if err := client.SetAVTransportURICtx(ctx, 0, mediaURI, didl); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel2()
	if err := client.PlayCtx(ctx2, 0, "1"); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": "SetAVTransportURI OK but Play failed: " + err.Error(),
		})
		return
	}
	resp := map[string]any{
		"status":   "ok",
		"action":   "load+play",
		"deviceId": dev.ID,
		"uri":      mediaURI,
	}

	// Core always sends "time" alongside "source" for Play/load
	// (PlayOnDeviceArgs.time in runtime/msg/action.rs; cast_request in
	// casting.rs sends args.time.unwrap_or(0)), in milliseconds — the same
	// unit as the rest of the wire API's resume-position fields (e.g.
	// LibraryItemState.time_offset in models/player.rs). Seek only for an
	// actual resume position; time == 0 means "start from the beginning",
	// which SetAVTransportURI+Play already does. A seek failure here must
	// not fail the load response: playback already started successfully.
	if rawTime := params.Get("time"); rawTime != "" {
		if ms, err := strconv.ParseFloat(rawTime, 64); err == nil && ms > 0 && validSeekSecs(ms/1000) {
			target, err := seekToSecs(client, ms/1000)
			if err != nil {
				logging.For("casting").Debug("resume seek after load failed",
					"device_id", dev.ID, "target", target, "err", err)
			} else {
				resp["seek"] = target
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// castingPlay sends Play(Speed=1).
func (s *server) castingPlay(w http.ResponseWriter, client *av1.AVTransport1, dev *CastingDevice) {
	ctx, cancel := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel()
	if err := client.PlayCtx(ctx, 0, "1"); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "action": "play", "deviceId": dev.ID})
}

// castingPause sends Pause.
func (s *server) castingPause(w http.ResponseWriter, client *av1.AVTransport1, dev *CastingDevice) {
	ctx, cancel := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel()
	if err := client.PauseCtx(ctx, 0); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "action": "pause", "deviceId": dev.ID})
}

// castingStop sends Stop.
func (s *server) castingStop(w http.ResponseWriter, client *av1.AVTransport1, dev *CastingDevice) {
	ctx, cancel := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel()
	if err := client.StopCtx(ctx, 0); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "action": "stop", "deviceId": dev.ID})
}

// castingSubtitles acknowledges a subtitle-track/offset update from core
// (CastingSubtitles{subtitlesSrc, subtitlesDelay}, runtime/msg/action.rs),
// sent bare via cast_request's Command::Subtitles arm in
// streaming_server/casting.rs — no "source" field, so castingCommand routes
// it here instead of falling through to "status".
//
// AVTransport:1 has no standard action for changing the subtitle track or
// timing offset of an item that is already loaded. The closest analogue —
// re-issuing SetAVTransportURI with a vendor-specific sec:CaptionInfo /
// sec:CaptionInfoEx attribute in the DIDL-Lite <res> element (a Samsung DLNA
// extension some renderers honor) — causes most renderers to reload the
// stream from its current position, trading a subtitle change for a visible
// playback interruption; that tradeoff cannot be safely evaluated without a
// matrix of real hardware to test against. So this deliberately does not
// touch AVTransport state (no GetPositionInfo/SetAVTransportURI/Seek call):
// it just answers 200 so core does not surface the update as a failure, and
// logs at debug for diagnosis. It intentionally does NOT reuse castingStatus
// — GetPositionInfo after a subtitle-only change is unnecessary chatter to
// the device and would misreport the response shape as a poll result.
func (s *server) castingSubtitles(w http.ResponseWriter, dev *CastingDevice, params url.Values) {
	logging.For("casting").Debug("subtitles update acknowledged (no DLNA action taken)",
		"device_id", dev.ID,
		"subtitles_src", params.Get("subtitlesSrc"),
		"subtitles_delay", params.Get("subtitlesDelay"))
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"action":   "subtitles",
		"deviceId": dev.ID,
	})
}

// seekToSecs issues Seek(Unit=REL_TIME) to the given offset (seconds) and
// returns the HH:MM:SS target that was sent, even on error, so callers can
// log/report it. Shared by castingSeek (direct "time" param, seconds) and
// castingLoad (resume position after Play, converted from core's ms unit).
func seekToSecs(client *av1.AVTransport1, secs float64) (string, error) {
	target := secsToHHMMSS(secs)
	ctx, cancel := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel()
	if err := client.SeekCtx(ctx, 0, "REL_TIME", target); err != nil {
		return target, err
	}
	return target, nil
}

// castingSeek sends Seek(Unit=REL_TIME, Target=HH:MM:SS).
// The "time" query/form param is seconds (int or float).
func (s *server) castingSeek(w http.ResponseWriter, client *av1.AVTransport1, dev *CastingDevice, params url.Values) {
	rawTime := params.Get("time")
	if rawTime == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing or invalid time parameter"})
		return
	}
	secs, err := strconv.ParseFloat(rawTime, 64)
	if err != nil || !validSeekSecs(secs) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "missing or invalid time parameter"})
		return
	}
	target, err := seekToSecs(client, secs)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":   "ok",
		"action":   "seek",
		"deviceId": dev.ID,
		"target":   target,
	})
}

// castingStatus sends GetPositionInfo and returns {trackDuration, relTime, trackURI}.
func (s *server) castingStatus(w http.ResponseWriter, client *av1.AVTransport1, dev *CastingDevice) {
	ctx, cancel := context.WithTimeout(context.Background(), soapTimeout)
	defer cancel()
	// GetPositionInfo returns: Track, TrackDuration, TrackMetaData, TrackURI,
	// RelTime, AbsTime, RelCount, AbsCount, err
	_, trackDuration, _, trackURI, relTime, _, _, _, err := client.GetPositionInfoCtx(ctx, 0)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"deviceId":      dev.ID,
		"trackDuration": trackDuration,
		"relTime":       relTime,
		"trackURI":      trackURI,
	})
}

// xmlEscape returns s with XML special characters (&, <, >, ', ") escaped.
// Used to safely embed values inside manually-constructed DIDL-Lite XML.
func xmlEscape(s string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

// maxSeekSecs bounds accepted seek targets (100 hours).
const maxSeekSecs = 100 * 3600

// validSeekSecs reports whether secs is finite, non-negative and sane.
func validSeekSecs(secs float64) bool {
	return !math.IsNaN(secs) && !math.IsInf(secs, 0) && secs >= 0 && secs <= maxSeekSecs
}

// secsToHHMMSS converts seconds (float) to "HH:MM:SS" for a UPnP Seek target.
// Non-finite or negative values clamp to 0; huge values clamp to maxSeekSecs.
func secsToHHMMSS(secs float64) string {
	if math.IsNaN(secs) || secs < 0 {
		secs = 0
	} else if secs > maxSeekSecs {
		secs = maxSeekSecs
	}
	total := int(secs)
	h := total / 3600
	m := (total % 3600) / 60
	sec := total % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
}

// refreshDevices returns the cached device list, re-running discovery if the
// cache has expired (or is empty). Never blocks longer than discoverTimeout on
// a miss; cache hits return immediately.
func refreshDevices() []CastingDevice {
	devicesMu.RLock()
	if time.Since(deviceCachAt) < deviceCacheTTL && deviceCache != nil {
		// Cache hit: all call sites are read-only (lines 97, 103, 147), so return
		// the shared slice directly — avoids an alloc + copy on every hot path.
		out := deviceCache
		devicesMu.RUnlock()
		return out
	}
	devicesMu.RUnlock()

	// On a cache miss, use singleflight so that only one SSDP discovery runs
	// at a time. Concurrent callers block here and receive the same result.
	type discoverResult struct {
		devs    []CastingDevice
		clients map[string]*av1.AVTransport1
	}
	v, _, _ := discoverGroup.Do("discover", func() (interface{}, error) {
		devs, clients := discoverDevices()
		devicesMu.Lock()
		deviceCache = devs
		deviceClients = clients
		deviceCachAt = time.Now()
		devicesMu.Unlock()
		return discoverResult{devs: devs, clients: clients}, nil
	})
	return v.(discoverResult).devs
}

// discoverDevices runs goupnp UPnP discovery for MediaRenderer:1 devices,
// filters to those exposing AVTransport:1, and returns the device list paired
// with an id→AVTransport1 client map for control.
//
// On any discovery failure an empty (non-nil) slice is returned so that
// /casting always produces [] rather than null.
func discoverDevices() ([]CastingDevice, map[string]*av1.AVTransport1) {
	ctx, cancel := context.WithTimeout(context.Background(), discoverTimeout)
	defer cancel()

	// goupnp.DiscoverDevicesCtx: sends SSDP M-SEARCH (2 s internal window),
	// then fetches and parses the device description XML for each responder.
	maybeDevs, err := goupnp.DiscoverDevicesCtx(ctx, ssdpSearchTarget)
	if err != nil {
		return []CastingDevice{}, nil
	}

	clients := make(map[string]*av1.AVTransport1)
	devices := make([]CastingDevice, 0, len(maybeDevs))

	for _, mrd := range maybeDevs {
		if mrd.Err != nil || mrd.Root == nil {
			continue
		}

		// av1.NewAVTransport1ClientsFromRootDevice: pure in-memory; searches
		// the device tree for AVTransport:1 services and wraps each in a
		// goupnp.ServiceClient with the resolved control URL.
		avClients, err := av1.NewAVTransport1ClientsFromRootDevice(mrd.Root, mrd.Location)
		if err != nil || len(avClients) == 0 {
			// Device is a MediaRenderer but lacks AVTransport:1; skip.
			continue
		}
		// The SOAP control URL comes from the (untrusted) description XML.
		// Require it to stay on the host that answered SSDP and route all
		// control traffic through the LAN-only client.
		if !upnpLocationOK(mrd.Location) || !upnpSameHost(mrd.Location, &avClients[0].SOAPClient.EndpointURL) {
			logging.For("casting").Warn("ignoring UPnP device with off-host control URL", "location", mrd.Location.Redacted())
			continue
		}
		avClients[0].SOAPClient.HTTPClient = *upnpHTTPClient

		d := &mrd.Root.Device
		id := mrd.USN
		if id == "" {
			id = d.UDN
		}
		name := d.FriendlyName
		if name == "" {
			name = mrd.Location.String()
		}

		dev := CastingDevice{
			ID:       id,
			Name:     name,
			Type:     "dlna",
			Model:    d.ModelName,
			Location: mrd.Location.String(),
		}

		devices = append(devices, dev)
		clients[id] = avClients[0] // use the first (and usually only) AVTransport instance
	}

	return devices, clients
}
