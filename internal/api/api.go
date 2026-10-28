// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package api implements the enginefs-compatible HTTP surface that stremio-web
// expects from a Stremio streaming server. It depends only on the interfaces in
// internal/types, so the engine/settings/media implementations are pluggable.
//
// Niche routes fully wired: /list, /:ih/peers, /stream alias, /get-https (cert
// fetch from api.strem.io), /yt (yt-dlp shell-out), /casting (SSDP discovery),
// /local-addon (local-files Stremio addon), and HLS transcoding (/hlsv2 via the
// ffmpeg transcoder in internal/media). thumb.jpg is a cosmetic 404 stub.
package api

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/rivulet-kodi/rivulet-engine/internal/dvr"
	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
	"github.com/rivulet-kodi/rivulet-engine/internal/streamproxy"
	"github.com/rivulet-kodi/rivulet-engine/internal/types"
)

// isInfoHash reports whether s is a valid 40-hex info hash without regexp.
// Zero allocations: simple length + byte-range check, much faster than regex on the hot routing path.
func isInfoHash(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < 40; i++ {
		c := s[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}

// videoMimes augments mime.TypeByExtension for containers Go often misses.
var videoMimes = map[string]string{
	".mkv":  "video/x-matroska",
	".mp4":  "video/mp4",
	".m4v":  "video/x-m4v",
	".avi":  "video/x-msvideo",
	".mov":  "video/quicktime",
	".webm": "video/webm",
	".flv":  "video/x-flv",
	".wmv":  "video/x-ms-wmv",
	".mpg":  "video/mpeg",
	".mpeg": "video/mpeg",
	".ts":   "video/mp2t",
	".m2ts": "video/mp2t",
	".ogv":  "video/ogg",
}

type server struct {
	em        types.EngineManager
	ss        types.SettingsStore
	prober    types.MediaProber
	cfg       types.Config
	logReq    bool
	accessLog *slog.Logger
	sp        *streamproxy.Handler
	// certReload hot-swaps the live HTTPS cert after /get-https writes new
	// files (wired by cmd). atomic.Pointer because it is set once from main's
	// startup goroutine but read from every request goroutine — GET
	// /get-https can race that write on a freshly started process (API-1).
	certReload  atomic.Pointer[func()]
	proxyClient *http.Client
	blobClient  *http.Client
	// dvr is the opt-in recorder behind /record and /api/recordings (nil
	// unless RIVULET_DVR_ENABLED; dvrInitErr says why when enabled but unusable).
	dvr        *dvr.Manager
	dvrInitErr string
}

// New returns the HTTP handler for the streaming server.
func New(em types.EngineManager, ss types.SettingsStore, prober types.MediaProber, cfg types.Config) http.Handler {
	localIMDBDisabled.Store(!cfg.LocalIMDB)
	// The signing secret is not part of this: it is auto-generated on every
	// install, so counting it made RIVULET_PROXY_ALLOW_PRIVATE a no-op. Only
	// a password or IP ACL signals that untrusted clients can reach the proxy.
	blockPrivate := cfg.ProxyPassword != "" || cfg.ProxyIPACL != "" || !proxyAllowPrivate()
	privateAllow := parsePrivateAllow(cfg.ProxyPrivateAllow)
	pc := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
				Control: netguard.DialControlAllow(blockPrivate, privateAllow),
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 15 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}
	bc := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
				Control: netguard.DialControl(blockPrivate),
			}).DialContext,
		},
	}
	s := &server{
		em:          em,
		ss:          ss,
		prober:      prober,
		cfg:         cfg,
		logReq:      cfg.HTTPLog,
		accessLog:   logging.For("http"),
		sp:          streamproxy.New(buildStreamProxyConfig(cfg, pc, privateAllow)),
		proxyClient: pc,
		blobClient:  bc,
	}
	if m, err := newDVR(cfg); err != nil {
		s.dvrInitErr = err.Error()
		logging.For("dvr").Error("dvr disabled: cannot initialise", "err", err)
	} else {
		s.dvr = m
	}
	return s
}

// Shared, immutable CORS header values, assigned directly into the response
// header map (canonical keys) to avoid a per-request slice allocation and key
// canonicalization on every request.
var (
	corsAllowOrigin   = []string{"*"}
	corsExposeHeaders = []string{"Content-Range, Accept-Ranges, Content-Length, Content-Type"}
)

// builtinAllowedOrigins are the official stremio-web deployments, always
// allowed regardless of RIVULET_ALLOWED_ORIGINS (SEC-1 / Contract 2).
var builtinAllowedOrigins = []string{
	"https://web.stremio.com",
	"https://web.strem.io",
	"https://app.strem.io",
	"https://staging.strem.io",
}

// stremioRocksSuffix matches any https://*.stremio.rocks subdomain (staging /
// preview deployments), mirroring the built-in wildcard entry in Contract 2.
const stremioRocksSuffix = ".stremio.rocks"

// originHostAllowed reports whether host (already lower-cased, no port, no
// IPv6 brackets — i.e. url.URL.Hostname()) is localhost, a loopback literal,
// or one of this server's own interface addresses. Allowed under http(s) on
// any port, matching Contract 2's "the server's own origins" clause.
func originHostAllowed(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	for _, ip := range availableInterfaces() {
		if strings.EqualFold(ip, host) {
			return true
		}
	}
	return false
}

// originPortAllowed restricts loopback/own-IP origins to this server's own
// HTTP/HTTPS ports; other local web apps must be listed in
// RIVULET_ALLOWED_ORIGINS. Unconfigured ports (0) leave it unrestricted.
func originPortAllowed(cfg types.Config, u *url.URL, scheme string) bool {
	if cfg.HTTPPort == 0 && cfg.HTTPSPort == 0 {
		return true
	}
	port := u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	return p == cfg.HTTPPort || p == cfg.HTTPSPort
}

// originEntryMatches reports whether a single RIVULET_ALLOWED_ORIGINS entry
// matches the request Origin. entry, normOrigin and host are already
// lower-cased by the caller. An entry may be:
//   - "*"                    — matches any origin.
//   - "scheme://host[:port]" — exact match against normOrigin.
//   - "host[:port]"          — matches host, any http/https scheme.
//   - "*.domain"             — matches any subdomain of domain, any scheme.
func originEntryMatches(entry, normOrigin, host string) bool {
	entry = strings.TrimSuffix(strings.TrimSpace(entry), "/")
	switch {
	case entry == "":
		return false
	case entry == "*":
		return true
	case strings.Contains(entry, "://"):
		return entry == normOrigin
	case strings.HasPrefix(entry, "*."):
		domain := entry[1:] // leading "." + domain
		return len(host) > len(domain) && strings.HasSuffix(host, domain)
	default:
		if i := strings.Index(normOrigin, "://"); i >= 0 && entry == normOrigin[i+3:] {
			return true
		}
		return entry == host
	}
}

// originAllowed implements the Contract 2 allowlist. A request with no Origin
// header (native players, curl, same-process fetches) is always allowed;
// cfg.AllowAllOrigins (RIVULET_ALLOWED_ORIGINS="*") restores the legacy
// allow-everything behavior; otherwise the origin must match the built-in
// Stremio-web/localhost/own-IP allowlist or a configured extra. The literal
// "null" origin (sandboxed iframes, file:// pages) is always rejected here
// since url.Parse leaves it with no Host.
func originAllowed(cfg types.Config, origin string) bool {
	if cfg.AllowAllOrigins {
		return true
	}
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if originHostAllowed(host) && originPortAllowed(cfg, u, scheme) {
		return true
	}
	normOrigin := scheme + "://" + strings.ToLower(u.Host)
	for _, b := range builtinAllowedOrigins {
		if strings.ToLower(b) == normOrigin {
			return true
		}
	}
	if scheme == "https" && len(host) > len(stremioRocksSuffix) && strings.HasSuffix(host, stremioRocksSuffix) {
		return true
	}
	for _, entry := range cfg.AllowedOrigins {
		if originEntryMatches(strings.ToLower(entry), normOrigin, host) {
			return true
		}
	}
	return false
}

// crossSiteBlocked reports whether r is a browser-originated cross-site request
// that carries no Origin header and targets a side-effecting route. Browsers
// omit Origin on cross-site <img>/<iframe>/link GETs, so originAllowed cannot
// stop them; they do however always send Sec-Fetch-Site: cross-site. Requests
// without Sec-Fetch-* (native shells, ffmpeg, curl) and same-origin/same-site/
// none are unaffected, as is everything when cfg.AllowAllOrigins is set.
// Media-playback routes (/{ih}/{idx}, /proxy, /hlsv2/..., /yt/{id}) are NOT
// gated: <video src> and Safari's native HLS legitimately load them
// cross-site without an Origin (including master.m3u8?mediaURL=).
func crossSiteBlocked(cfg types.Config, r *http.Request) bool {
	if cfg.AllowAllOrigins || r.Header.Get("Origin") != "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site") {
		return false
	}
	return sideEffectingRoute(r)
}

// sideEffectingRoute reports whether the request path addresses a route that
// mutates state or spawns processes/outbound calls on a plain GET.
func sideEffectingRoute(r *http.Request) bool {
	seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	first := seg[0]
	if isInfoHash(first) {
		return len(seg) >= 2 && (seg[1] == "create" || seg[1] == "remove")
	}
	switch first {
	case "removeAll", "get-https", "casting", "probe", "tracks", "opensubHash",
		"subtitlesTracks", "nzb", "ftp", "bitmagnet", "torznab":
		return true
	case "zip", "rar", "7zip", "tar", "tgz":
		return len(seg) >= 2 && seg[1] == "create"
	case "hlsv2":
		return strings.Contains(r.URL.Path, "probe")
	case "record", "api":
		return dvrSideEffecting(r)
	case "proxy", "yt", "extractor":
		// <video>/<audio>/<track> loads of these are legitimate cross-site
		// (stremio-web); any other destination is an attack vector.
		switch strings.ToLower(r.Header.Get("Sec-Fetch-Dest")) {
		case "video", "audio", "track":
			return false
		}
		return true
	}
	return false
}

// mediaRoute reports whether the path is a playback route exempt from the
// Host-header (DNS-rebinding) check.
func mediaRoute(r *http.Request) bool {
	seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	first := seg[0]
	if isInfoHash(first) {
		return len(seg) >= 2 && seg[1] != "create" && seg[1] != "remove"
	}
	switch first {
	case "stream", "hlsv2", "yt", "proxy", "base64", "extractor":
		return true
	case "zip", "rar", "7zip", "tar", "tgz":
		return len(seg) >= 2 && seg[1] == "stream"
	}
	return strings.HasPrefix(first, "subtitles.")
}

func hostOf(raw string) string {
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// hostAllowed defends against DNS rebinding: a non-media request must carry
// a Host that is loopback, an own interface IP, an IP literal, a .local name,
// the configured public URL host, or listed in RIVULET_ALLOWED_HOSTS.
func hostAllowed(cfg types.Config, r *http.Request) bool {
	if cfg.AllowAllOrigins || mediaRoute(r) {
		return true
	}
	// DVR routes are fetched by remote addons/players under whatever public
	// name they were configured with; with a password set the Host check adds
	// nothing (a rebinding page cannot know it).
	if cfg.ProxyPassword != "" && dvrRoute(r) {
		return true
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.Trim(host, "[]"))
	host = strings.TrimSuffix(host, ".")
	if host == "" || host == "localhost" || net.ParseIP(host) != nil || strings.HasSuffix(host, ".local") {
		return true
	}
	if originHostAllowed(host) {
		return true
	}
	for _, u := range []string{cfg.PublicURL, cfg.ProxyPublicURL, cfg.LocalFilesPublicURL} {
		if u != "" && hostOf(u) == host {
			return true
		}
	}
	for _, e := range cfg.AllowedOrigins {
		if h := hostOf(strings.TrimPrefix(e, "*.")); h != "" && (h == host || (strings.HasPrefix(e, "*.") && strings.HasSuffix(host, "."+h))) {
			return true
		}
	}
	for _, e := range strings.Split(os.Getenv("RIVULET_ALLOWED_HOSTS"), ",") {
		if e = strings.ToLower(strings.TrimSpace(e)); e != "" && e == host {
			return true
		}
	}
	return false
}

// remoteIP returns the immediate TCP peer address (never X-Forwarded-For).
func remoteIP(r *http.Request) net.IP {
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		h = r.RemoteAddr
	}
	return net.ParseIP(h)
}

// proxyAllowPrivate reports RIVULET_PROXY_ALLOW_PRIVATE=1, which re-enables
// proxying to loopback/private destinations on an unprotected server.
func proxyAllowPrivate() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("RIVULET_PROXY_ALLOW_PRIVATE")))
	return v == "1" || v == "true" || v == "yes"
}

// parsePrivateAllow parses RIVULET_PROXY_PRIVATE_ALLOW (IPs, CIDRs and
// hostnames that stay reachable through the proxy even when private
// destinations are blocked). Invalid entries are logged and skipped.
func parsePrivateAllow(spec string) *netguard.Allow {
	a, bad := netguard.ParseAllow(spec)
	if len(bad) > 0 {
		logging.For("config").Warn("ignoring invalid RIVULET_PROXY_PRIVATE_ALLOW entries", "entries", strings.Join(bad, ","))
	}
	if a != nil {
		logging.For("streamproxy").Info("private destinations allowlisted", "allow", a.String())
	}
	return a
}

// parseTrustedProxies parses RIVULET_TRUSTED_PROXIES (comma-separated CIDRs
// or bare IPs).
func parseTrustedProxies(s string) []*net.IPNet {
	var out []*net.IPNet
	for _, e := range strings.Split(s, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if !strings.Contains(e, "/") {
			if ip := net.ParseIP(e); ip != nil {
				if ip.To4() != nil {
					e += "/32"
				} else {
					e += "/128"
				}
			}
		}
		if _, n, err := net.ParseCIDR(e); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// Shared, immutable streaming header values — pre-canonicalized keys to avoid
// per-request key canonicalization (mirrors CORS pattern above).
// textproto.CanonicalMIMEHeaderKey("transferMode.dlna.org")    = "Transfermode.dlna.org"
// textproto.CanonicalMIMEHeaderKey("contentFeatures.dlna.org") = "Contentfeatures.dlna.org"
var (
	streamAcceptRanges    = []string{"bytes"}
	streamCacheControl    = []string{"max-age=0, no-cache"}
	streamTransferMode    = []string{"Streaming"}
	streamContentFeatures = []string{"DLNA.ORG_OP=01;DLNA.ORG_CI=0;DLNA.ORG_FLAGS=01700000000000000000000000000000"}
	streamNoSniff         = []string{"nosniff"}
	streamCSPSandbox      = []string{"sandbox"}
)

// setActiveContentGuards stops upstream/torrent bytes served from this trusted
// origin from executing as active content: nosniff pins the declared type, and
// CSP sandbox gives any rendered document an opaque origin (its fetches carry
// "Origin: null", which originAllowed rejects). Harmless for <video>/<audio>.
func setActiveContentGuards(h http.Header) {
	h["X-Content-Type-Options"] = streamNoSniff
	h["Content-Security-Policy"] = streamCSPSandbox
}

// sensitiveQueryKeys are query parameters whose values must never reach the
// access log: lz carries reversible lz-string payloads holding URLs with
// embedded ftp/nntp/http credentials; the rest are common secret-bearing names.
var sensitiveQueryKeys = map[string]struct{}{
	"lz": {}, "apikey": {}, "api_key": {}, "authkey": {}, "token": {},
	"access_token": {}, "password": {}, "api_password": {}, "pass": {}, "key": {}, "auth": {},
}

// userinfoRe matches the userinfo of a URL embedded in a request URI, either
// literal ("://user:pass@") or percent-encoded ("%3A%2F%2Fuser%3Apass%40").
var userinfoRe = regexp.MustCompile(`(?i)(://|%3A%2F%2F)[^/?&#\s]*?(?:@|%40)`)

// redactRequestURI returns u.RequestURI() with sensitive query values and any
// embedded URL credentials replaced by "REDACTED", for the HTTP access log.
func redactRequestURI(u *url.URL) string {
	uri := u.RequestURI()
	if i := strings.IndexByte(uri, '?'); i >= 0 && i+1 < len(uri) {
		pairs := strings.Split(uri[i+1:], "&")
		for j, p := range pairs {
			k, _, found := strings.Cut(p, "=")
			if !found {
				continue
			}
			name, err := url.QueryUnescape(k)
			if err != nil {
				name = k
			}
			if _, ok := sensitiveQueryKeys[strings.ToLower(name)]; ok {
				pairs[j] = k + "=REDACTED"
			}
		}
		uri = uri[:i+1] + strings.Join(pairs, "&")
	}
	return userinfoRe.ReplaceAllString(uri, "${1}REDACTED@")
}

// stripTrackerPrefixes reduces request-supplied `tr` values to bare announce
// URLs. stremio-core builds a torrent stream URL's `tr` params verbatim from
// the stream's announce/sources list (stream.rs, StreamSource::Torrent), and
// Torrentio-style addons ship that list as "tracker:<announce URL>" and
// "dht:<infohash>" entries — the same source syntax peerSearch.sources uses
// (see trackersFromSources). Without this a "tracker:"-wrapped URL parses as an
// opaque URL with no host and filterRequestTrackers silently drops it, so the
// engine never learns the stream's own trackers. "dht:" entries carry no
// announce URL (anacrolix's DHT finds peers on its own) and are skipped.
func stripTrackerPrefixes(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := make([]string, 0, len(in))
	for _, tr := range in {
		tr = strings.TrimSpace(tr)
		if strings.HasPrefix(tr, "dht:") {
			continue
		}
		tr = strings.TrimSpace(strings.TrimPrefix(tr, "tracker:"))
		if tr == "" {
			continue
		}
		out = append(out, tr)
	}
	return out
}

// filterRequestTrackers drops request-supplied tracker URLs whose host is
// localhost or a literal loopback, unspecified, link-local or cloud-metadata
// IP, so a request cannot make the torrent client announce to local services
// (blind SSRF). Public and LAN trackers pass through unchanged.
func filterRequestTrackers(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := make([]string, 0, len(in))
	for _, tr := range in {
		u, err := url.Parse(strings.TrimSpace(tr))
		if err != nil || u.Hostname() == "" {
			continue
		}
		host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
		if host == "localhost" || strings.HasSuffix(host, ".localhost") {
			continue
		}
		if ip := net.ParseIP(host); ip != nil {
			if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() ||
				ip.IsLinkLocalMulticast() || netguard.IsCloudMetadata(ip) {
				continue
			}
		}
		out = append(out, tr)
	}
	return out
}

// ServeHTTP enforces the Origin allowlist (SEC-1 / Contract 2), applies CORS,
// handles preflight, and dispatches to the router. A disallowed Origin is
// rejected with 403 before any routing — including the OPTIONS preflight —
// so a hostile page can never reach a state-changing route.
func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if !originAllowed(s.cfg, origin) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "origin not allowed"})
		return
	}
	if crossSiteBlocked(s.cfg, r) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "cross-site request not allowed"})
		return
	}
	if !hostAllowed(s.cfg, r) {
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "host not allowed"})
		return
	}
	hdr := w.Header()
	if s.cfg.AllowAllOrigins || origin == "" {
		hdr["Access-Control-Allow-Origin"] = corsAllowOrigin
	} else {
		hdr.Set("Access-Control-Allow-Origin", origin)
		hdr.Add("Vary", "Origin")
	}
	hdr["Access-Control-Expose-Headers"] = corsExposeHeaders
	if r.Method == http.MethodOptions {
		hdr.Set("Access-Control-Allow-Methods", "POST, GET, DELETE, OPTIONS")
		hdr.Set("Access-Control-Allow-Headers", "Range, Content-Type, Accept, Authorization")
		hdr.Set("Access-Control-Max-Age", "1728000")
		if r.Header.Get("Access-Control-Request-Private-Network") == "true" {
			hdr.Set("Access-Control-Allow-Private-Network", "true")
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if s.logReq {
		start := time.Now()
		rec := logging.NewResponseRecorder(w)
		s.route(rec, r)
		s.accessLog.Info("request",
			"method", r.Method,
			"uri", redactRequestURI(r.URL),
			"status", rec.StatusOrOK(),
			"duration_ms", time.Since(start).Milliseconds(),
			"bytes", rec.Bytes,
			"remote", r.RemoteAddr,
		)
		return
	}
	s.route(w, r)
}

// splitEncodedPath splits the request path into segments like
// strings.Split(path, "/"), except seg[1] is taken from the still
// percent-encoded wire path (r.URL.EscapedPath()) instead of the already
// unescaped r.URL.Path. stremio-core embeds a percent-encoded opts string as
// one path segment (proxy/<opts>/<path>, e.g. "d=http%3A%2F%2Fhost"); by the
// time routing sees r.URL.Path, net/url has already decoded it, so a raw
// "%2F" inside opts becomes a literal "/" and strings.Split shatters that
// segment. seg[1]'s consumers (url.ParseQuery, base64 decoding) do their own
// decoding, so it must arrive exactly as sent. The trailing segments
// (seg[2:], the upstream path) are decoded per-segment, matching the prior
// (correct) behavior for that part.
func splitEncodedPath(r *http.Request) []string {
	seg := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	for i := 2; i < len(seg); i++ {
		if dec, err := url.PathUnescape(seg[i]); err == nil {
			seg[i] = dec
		}
	}
	return seg
}

func (s *server) route(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	if path == "" {
		s.handleLanding(w, r)
		return
	}

	// Derive the first path segment without allocating a slice.
	// Full split is deferred until a route actually requires sub-segments,
	// so single-segment hot routes (heartbeat, stats.json, settings, list…)
	// pay no Split cost at all.
	firstSep := strings.IndexByte(path, '/')
	first := path
	if firstSep >= 0 {
		first = path[:firstSep]
	}

	// infoHash-scoped routes: always need at least seg[1].
	// Use isInfoHash instead of regexp to avoid regex overhead on every request.
	if isInfoHash(first) {
		ih := strings.ToLower(first)
		if firstSep < 0 {
			// bare /:infoHash with no trailing segment
			http.NotFound(w, r)
			return
		}
		seg := strings.Split(path, "/")
		switch {
		case len(seg) == 1:
			http.NotFound(w, r)
		case seg[1] == "create":
			s.handleCreate(w, r, ih)
		case seg[1] == "remove":
			s.handleRemove(w, r, ih)
		case seg[1] == "stats.json":
			s.writeStats(w, ih, -1)
		case len(seg) >= 3 && seg[2] == "stats.json":
			idx, _ := strconv.Atoi(seg[1])
			s.writeStats(w, ih, idx)
		// /:infoHash/peers — peer list (best-effort via Stats.Wires)
		case seg[1] == "peers":
			s.handlePeers(w, r, ih)
		// /:infoHash/:fileIdx/subtitles.vtt — subtitle conversion for a torrent file
		case len(seg) >= 3 && seg[2] == "subtitles.vtt":
			idx, _ := strconv.Atoi(seg[1])
			s.handleStreamSubtitles(w, r, ih, idx)
		default:
			s.handleStream(w, r, ih, seg[1])
		}
		return
	}

	// top-level routes — single-segment paths (the hot routes) use only `first`
	// and skip the Split entirely.
	switch first {
	case "create":
		s.handleCreateBlob(w, r)
	case "removeAll":
		s.handleRemoveAll(w, r)
	case "stats.json":
		s.handleAllStats(w, r)
	case "network-info":
		s.handleNetworkInfo(w, r)
	case "device-info":
		s.handleDeviceInfo(w, r)
	case "settings":
		s.handleSettings(w, r)
	case "heartbeat":
		s.handleHeartbeat(w, r)
	case "probe":
		s.handleProbe(w, r)
	case "opensubHash":
		s.handleOpenSubHash(w, r)
	case "subtitlesTracks":
		s.handleSubtitlesTracks(w, r)
	case "hwaccel-profiler":
		s.handleHwaccelProfiler(w, r)
	case "list":
		s.handleList(w, r)
	case "generate_url":
		s.sp.HandleGenerateURL(w, r)
	case "get-https":
		s.handleGetHTTPS(w, r)
	case "thumb.jpg":
		http.NotFound(w, r) // no thumbnail service; cosmetic 404
	case "metrics":
		s.handleMetrics(w, r)
	// Routes that need sub-segments: split only when dispatched.
	case "hlsv2":
		s.handleHLS(w, r, strings.Split(path, "/"))
	case "tracks":
		s.handleTracks(w, r, strings.Split(path, "/"))
	case "proxy":
		seg := splitEncodedPath(r)
		if s.sp.Route(w, r, seg) {
			return
		}
		s.handleProxy(w, r, seg)
	case "base64":
		s.sp.HandleBase64(w, r, splitEncodedPath(r))
	case "extractor":
		s.sp.HandleExtractor(w, r, splitEncodedPath(r))
	case "record":
		s.handleRecord(w, r, strings.Split(path, "/"))
	case "api":
		if s.handleAPIReference(w, r, path) {
			return
		}
		s.handleRecordings(w, r, strings.Split(path, "/"))
	case "openapi.json":
		s.handleOpenAPI(w, r)
	// /list — active infohash array handled above (single-segment hot path)
	// /stream/:infoHash/:fileIdx — alias to /:infoHash/:fileIdx
	case "stream":
		if firstSep < 0 {
			http.NotFound(w, r)
			return
		}
		seg := strings.Split(path, "/")
		if len(seg) >= 3 && isInfoHash(seg[1]) {
			s.handleStream(w, r, strings.ToLower(seg[1]), seg[2])
		} else {
			http.NotFound(w, r)
		}
	case "casting":
		s.handleCasting(w, r, strings.Split(path, "/"))
	case "yt":
		seg := strings.Split(path, "/")
		if len(seg) < 2 {
			http.NotFound(w, r)
			return
		}
		s.handleYT(w, r, seg[1])
	case "local-addon":
		s.handleLocalAddon(w, r, strings.Split(path, "/"))
	case "bitmagnet":
		s.handleBitmagnet(w, r, strings.Split(path, "/"))
	case "torznab":
		s.handleTorznab(w, r, strings.Split(path, "/"))
	case "zip", "rar", "7zip", "tar", "tgz":
		s.handleArchive(w, r, strings.Split(path, "/"), first)
	case "nzb":
		s.handleNZB(w, r, strings.Split(path, "/"))
	case "ftp":
		s.handleFTP(w, r, strings.Split(path, "/"))
	default:
		if strings.HasPrefix(first, "subtitles.") {
			s.handleSubtitles(w, r, strings.TrimPrefix(first, "subtitles."))
			return
		}
		if strings.HasSuffix(first, ".m3u8") || strings.HasSuffix(path, ".m3u8") {
			http.Error(w, "transcoding not implemented", http.StatusNotImplemented)
			return
		}
		http.NotFound(w, r)
	}
}

// ---- inline-route handlers -----------------------------------------------

func (s *server) handleRemoveAll(w http.ResponseWriter, _ *http.Request) {
	s.em.RemoveAll()
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (s *server) handleNetworkInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"availableInterfaces": availableInterfaces()})
}

func (s *server) handleDeviceInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"availableHardwareAccelerations": detectHWAccels()})
}

func (s *server) handleHeartbeat(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// handleMetrics serves GET /metrics in Prometheus text exposition format (v0.0.4).
// No authentication, and — like every other route — it is served with the
// permissive CORS headers ServeHTTP sets unconditionally (Access-Control-Allow-
// Origin: *), so any web page can read it. Intended for localhost-trust
// scraping (e.g. a Prometheus instance on the same host); do not expose the
// listener beyond loopback without a firewall in front.
func (s *server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var b strings.Builder

	writeGauge := func(name, help string, value float64) {
		fmt.Fprintf(&b, "# HELP %s %s\n", name, help)
		fmt.Fprintf(&b, "# TYPE %s gauge\n", name)
		fmt.Fprintf(&b, "%s %g\n", name, value)
	}

	// Runtime gauges
	writeGauge("rivulet_engine_goroutines",
		"Current number of goroutines.",
		float64(runtime.NumGoroutine()))
	writeGauge("rivulet_engine_memstats_alloc_bytes",
		"Bytes of allocated heap objects (runtime.MemStats.Alloc).",
		float64(ms.Alloc))
	writeGauge("rivulet_engine_memstats_sys_bytes",
		"Total bytes of memory obtained from the OS (runtime.MemStats.Sys).",
		float64(ms.Sys))

	// App gauges — structural assertions keep the api package interface-clean.
	torrents := 0
	if v, ok := s.em.(interface{ NumTorrents() int }); ok {
		torrents = v.NumTorrents()
	}
	writeGauge("rivulet_engine_torrents_active",
		"Number of active torrent engines.",
		float64(torrents))

	hlsSessions := 0
	if v, ok := s.prober.(interface{ HLSSessions() int }); ok {
		hlsSessions = v.HLSSessions()
	}
	writeGauge("rivulet_engine_hls_sessions",
		"Number of active HLS transcode sessions.",
		float64(hlsSessions))

	cacheEntries, cacheBytes := s.sp.CacheStats()
	writeGauge("rivulet_engine_proxy_cache_entries",
		"Number of entries in the stream-proxy segment cache.",
		float64(cacheEntries))
	writeGauge("rivulet_engine_proxy_cache_bytes",
		"Total bytes used by the stream-proxy segment cache.",
		float64(cacheBytes))

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, b.String())
}

func (s *server) handleHwaccelProfiler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, detectHWAccels())
}

func (s *server) handleList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.em.ListEngines())
}

func (s *server) handleRemove(w http.ResponseWriter, _ *http.Request, ih string) {
	_ = s.em.RemoveEngine(ih)
	writeJSON(w, http.StatusOK, map[string]any{})
}

// ---- streaming ------------------------------------------------------------

func (s *server) handleStream(w http.ResponseWriter, r *http.Request, ih, idxSeg string) {
	// Guard the query parse: skip url.Values map alloc on requests with no query string.
	// url.Values.Get and nil map index are both nil-safe.
	var q url.Values
	if r.URL.RawQuery != "" {
		q = r.URL.Query()
	}
	trackers := q["tr"]
	mustInc := compileMustInclude(q["f"])

	eng, err := s.em.EnsureEngine(ih, types.AddOptions{Trackers: filterRequestTrackers(stripTrackerPrefixes(trackers))})
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	ctx, cancel := withTimeout(r, s.cfg.CreateMetadataWait)
	defer cancel()
	if err := eng.Ready(ctx); err != nil {
		http.Error(w, "timed out waiting for metadata: "+err.Error(), http.StatusGatewayTimeout)
		return
	}
	files := eng.Files()

	idx := resolveIndex(idxSeg, files, mustInc, eng)
	if idx < 0 || idx >= len(files) {
		http.Error(w, "file index not found", http.StatusNotFound)
		return
	}
	f := files[idx]

	// ?external=1 -> redirect to the named URL
	if q.Get("external") != "" {
		loc := "/" + ih + "/" + url.PathEscape(f.Name)
		if q.Get("download") != "" {
			loc += "?download=1"
		}
		http.Redirect(w, r, loc, http.StatusTemporaryRedirect)
		return
	}

	rc, length, err := eng.NewReader(idx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() { _ = rc.Close() }()

	// If the client disconnects (tab closed, seek aborted) while a Read is
	// blocked waiting on the swarm, nothing would otherwise wake it: io.Copy*
	// below only observes rc via Read, and the deferred Close above only runs
	// after the copy already returned. That pins e.openReaders > 0 forever,
	// which permanently exempts this torrent from both evict and evictIdle.
	// This watcher closes rc as soon as the request context is cancelled,
	// which (via pinnedReader's long-lived context) unblocks a stalled Read
	// immediately. done is closed via defer so the watcher always exits once
	// the copy loop returns, whether or not the client ever disconnected —
	// it never leaks. defer ordering matters: this defer is registered after
	// the rc.Close() defer above, so on the normal (LIFO) return path done is
	// closed first (retiring the watcher without it touching rc), and the
	// pre-existing rc.Close() defer performs the only real close.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-r.Context().Done():
			_ = rc.Close()
		case <-done:
		}
	}()

	// Static DLNA/cache headers: direct canonical-key map assignment avoids
	// per-request key canonicalization (mirrors the CORS pattern in ServeHTTP).
	hdr := w.Header()
	setActiveContentGuards(hdr)
	hdr["Accept-Ranges"] = streamAcceptRanges
	hdr["Content-Type"] = []string{mimeByName(f.Name)}
	hdr["Cache-Control"] = streamCacheControl
	hdr["Transfermode.dlna.org"] = streamTransferMode
	hdr["Contentfeatures.dlna.org"] = streamContentFeatures
	if q.Get("download") != "" {
		hdr.Set("Content-Disposition", contentDisposition(f.Name))
	}
	if subs := q.Get("subtitles"); subs != "" {
		hdr.Set("CaptionInfo.sec", subs)
	}

	bufp := streamBufPool.Get().(*[]byte)
	defer streamBufPool.Put(bufp)

	start, end, isRange, unsat := parseRange(r.Header.Get("Range"), length)
	if unsat {
		// Build Content-Range without fmt.Sprintf to avoid reflect overhead on range responses.
		var buf [32]byte
		b := append(buf[:0], "bytes */"...)
		b = strconv.AppendInt(b, length, 10)
		hdr["Content-Range"] = []string{string(b)}
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if isRange {
		hdr.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		// Build "bytes start-end/length" in a stack buffer to avoid fmt.Sprintf reflect.
		var buf [80]byte
		b := append(buf[:0], "bytes "...)
		b = strconv.AppendInt(b, start, 10)
		b = append(b, '-')
		b = strconv.AppendInt(b, end, 10)
		b = append(b, '/')
		b = strconv.AppendInt(b, length, 10)
		hdr["Content-Range"] = []string{string(b)}
		if r.Method != http.MethodHead {
			if _, err := rc.Seek(start, io.SeekStart); err != nil {
				http.Error(w, "seek: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.WriteHeader(http.StatusPartialContent)
		if r.Method == http.MethodHead {
			return
		}
		copyStream(w, io.LimitReader(rc, end-start+1), *bufp)
		return
	}

	hdr.Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	copyStream(w, rc, *bufp)
}

// resolveIndex picks the file index from the URL segment, a fileMustInclude
// match, an exact filename, or the guess heuristic.
func resolveIndex(seg string, files []types.FileInfo, mustInc []*regexp.Regexp, eng types.Engine) int {
	if len(mustInc) > 0 {
		for i, f := range files {
			for _, re := range mustInc {
				if re.MatchString(f.Name) {
					return i
				}
			}
		}
		return -1 // mustInc filters provided but no file matched → caller returns 404
	}
	if n, err := strconv.Atoi(seg); err == nil {
		if n == -1 {
			return eng.GuessFileIdx()
		}
		return n
	}
	// non-numeric: treat as a (url-encoded) filename
	name, _ := url.PathUnescape(seg)
	for i, f := range files {
		if f.Name == name {
			return i
		}
	}
	return eng.GuessFileIdx()
}

// compileMustInclude compiles the request's fileMustInclude values (f= query /
// POST body), reusing compiled patterns from a bounded LRU so repeat requests
// do not pay regexp.Compile each time.
func compileMustInclude(vals []string) []*regexp.Regexp {
	var out []*regexp.Regexp
	for _, v := range vals {
		if v == "" {
			continue
		}
		if re := cachedMustInclude(v); re != nil {
			out = append(out, re)
		}
	}
	return out
}

// parseRange parses a single "bytes=start-end" range. Returns ok=true for a
// satisfiable range (→ 206), unsatisfiable=true when the Range header is
// syntactically valid but no byte is satisfiable (→ 416 per RFC 7233 §4.4),
// and both false when the header is absent or syntactically invalid (→ 200).
func parseRange(h string, length int64) (start, end int64, ok bool, unsatisfiable bool) {
	if h == "" || !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if i := strings.IndexByte(spec, ','); i >= 0 {
		spec = spec[:i] // only the first range
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, false, false
	}
	startS, endS := spec[:dash], spec[dash+1:]
	if startS == "" { // suffix range: bytes=-N
		n, err := strconv.ParseInt(endS, 10, 64)
		if err != nil {
			return 0, 0, false, false // syntactically invalid
		}
		if n <= 0 {
			return 0, 0, false, true // suffix-length 0 is unsatisfiable (RFC 7233 §4.4)
		}
		if n > length {
			n = length
		}
		if n == 0 { // length==0: suffix of empty file is unsatisfiable (RFC 7233 §4.4)
			return 0, 0, false, true
		}
		return length - n, length - 1, true, false
	}
	s, err := strconv.ParseInt(startS, 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false, false // syntactically invalid
	}
	if s >= length {
		return 0, 0, false, true // first-byte-pos beyond EOF → 416
	}
	end = length - 1
	if endS != "" {
		e, err := strconv.ParseInt(endS, 10, 64)
		if err != nil {
			return 0, 0, false, false // malformed end token → treat header as absent (RFC 7233)
		}
		if e < s {
			return 0, 0, false, true // end < start → 416
		}
		end = e
	}
	if end >= length {
		end = length - 1
	}
	return s, end, true, false
}

// ---- engine lifecycle -----------------------------------------------------

type createBody struct {
	Torrent         json.RawMessage `json:"torrent"`
	FileMustInclude []string        `json:"fileMustInclude"`
	GuessFileIdx    json.RawMessage `json:"guessFileIdx"`
	PeerSearch      *struct {
		Sources []string `json:"sources"`
	} `json:"peerSearch"`
}

func (s *server) handleCreate(w http.ResponseWriter, r *http.Request, ih string) {
	var body createBody
	if r.Body != nil {
		dec := json.NewDecoder(io.LimitReader(r.Body, 3<<20))
		_ = dec.Decode(&body) // empty/invalid body is acceptable
	}

	opts := types.AddOptions{Torrent: body.Torrent}
	opts.Trackers = append(opts.Trackers, trackersFromSources(peerSearchSources(&body))...)
	opts.Trackers = append(opts.Trackers, announceFromTorrent(body.Torrent)...)
	opts.Trackers = filterRequestTrackers(opts.Trackers)

	eng, err := s.em.EnsureEngine(ih, opts)
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	ctx, cancel := withTimeout(r, s.cfg.CreateMetadataWait)
	defer cancel()
	if err := eng.Ready(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}

	stats := eng.Stats(-1)
	if stats == nil {
		http.Error(w, "no stats", http.StatusInternalServerError)
		return
	}
	// guessedFileIdx
	if mi := compileMustInclude(body.FileMustInclude); len(mi) > 0 {
		files := eng.Files()
	match:
		for i, f := range files {
			for _, re := range mi {
				if re.MatchString(f.Name) {
					gi := i
					stats.GuessedFileIdx = &gi
					break match
				}
			}
		}
	}
	if stats.GuessedFileIdx == nil && truthy(body.GuessFileIdx) {
		gi := eng.GuessFileIdx()
		stats.GuessedFileIdx = &gi
	}
	writeJSON(w, http.StatusOK, stats)
}

// handleCreateBlob handles POST /create with {from:<url|path>} or {blob:<hex>}.
func (s *server) handleCreateBlob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		From string `json:"from"`
		Blob string `json:"blob"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&body)
	}
	var raw []byte
	var err error
	// errStatus is what a non-nil err maps to: a malformed request is the
	// caller's fault (400), a failed fetch of the remote torrent is upstream's (502).
	errStatus := http.StatusBadRequest
	switch {
	case body.Blob != "":
		raw, err = hexDecode(body.Blob)
	case strings.HasPrefix(body.From, "http://") || strings.HasPrefix(body.From, "https://"):
		if verr := s.sp.ValidateDest(body.From); verr != nil {
			http.Error(w, "forbidden target", http.StatusForbidden)
			return
		}
		errStatus = http.StatusBadGateway
		var blobResp *http.Response
		blobResp, err = s.blobClient.Get(body.From)
		if err == nil {
			defer func() { _ = blobResp.Body.Close() }()
			if blobResp.StatusCode != http.StatusOK {
				err = fmt.Errorf("GET %s: status %d", body.From, blobResp.StatusCode)
			} else {
				raw, err = io.ReadAll(io.LimitReader(blobResp.Body, 16<<20))
			}
		}
	case body.From != "":
		http.Error(w, "from: only http/https URLs are accepted", http.StatusBadRequest)
		return
	default:
		err = fmt.Errorf("missing from/blob")
	}
	if err != nil {
		http.Error(w, err.Error(), errStatus)
		return
	}
	mi, err := metainfo.Load(strings.NewReader(string(raw)))
	if err != nil {
		http.Error(w, "parse torrent: "+err.Error(), http.StatusBadRequest)
		return
	}
	ih := mi.HashInfoBytes().HexString()
	eng, err := s.em.EnsureEngine(ih, types.AddOptions{MetaInfo: raw})
	if err != nil {
		writeEngineErr(w, err)
		return
	}
	ctx, cancel := withTimeout(r, s.cfg.CreateMetadataWait)
	defer cancel()
	if err := eng.Ready(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	writeJSON(w, http.StatusOK, eng.Stats(-1))
}

func (s *server) writeStats(w http.ResponseWriter, ih string, idx int) {
	eng, ok := s.em.GetEngine(ih)
	if !ok {
		writeJSON(w, http.StatusOK, nil)
		return
	}
	writeJSON(w, http.StatusOK, eng.Stats(idx))
}

func (s *server) handleAllStats(w http.ResponseWriter, r *http.Request) {
	all := s.em.AllStats()
	// Common path: avoid url.Values map alloc by checking RawQuery directly.
	// Pass the typed map straight to writeJSON — JSON output is identical to
	// the original map[string]any because the encoder reflects on the values.
	if r.URL.Query().Get("sys") != "1" {
		writeJSON(w, http.StatusOK, all)
		return
	}
	// sys=1: augment with system stats.
	out := make(map[string]any, len(all)+1)
	for ih, st := range all {
		out[ih] = st
	}
	out["sys"] = map[string]any{"loadavg": loadavg(), "cpus": cpus()}
	writeJSON(w, http.StatusOK, out)
}

// handlePeers serves GET /:infoHash/peers — a JSON array of connected peers.
// Peer data is sourced from Stats.Wires (already available on the types.Engine
// interface) so we don't need to reach into the concrete engine internals.
func (s *server) handlePeers(w http.ResponseWriter, r *http.Request, ih string) {
	eng, ok := s.em.GetEngine(ih)
	if !ok {
		writeJSON(w, http.StatusNotFound, []any{})
		return
	}
	st := eng.Stats(-1)
	if st == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	type peerInfo struct {
		Addr      string  `json:"addr"`
		AmInt     bool    `json:"amInterested"`
		IsSeeder  bool    `json:"isSeeder"`
		DownSpeed float64 `json:"downloadSpeed"`
		UpSpeed   float64 `json:"uploadSpeed"`
		Requests  int     `json:"requests"`
	}
	out := make([]peerInfo, 0, len(st.Wires))
	for _, wire := range st.Wires {
		out = append(out, peerInfo{
			Addr:      wire.Address,
			AmInt:     wire.AmInterested,
			IsSeeder:  wire.IsSeeder,
			DownSpeed: wire.DownSpeed,
			UpSpeed:   wire.UpSpeed,
			Requests:  wire.Requests,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleStreamSubtitles serves GET /:infoHash/:fileIdx/subtitles.vtt.
// It constructs a local stream URL for the torrent file and delegates subtitle
// conversion to the existing prober.WriteSubtitles path.
func (s *server) handleStreamSubtitles(w http.ResponseWriter, r *http.Request, ih string, idx int) {
	// Build the local HTTP URL that the subtitle prober will pull from.
	streamURL := fmt.Sprintf("http://127.0.0.1:%d/%s/%d", s.cfg.HTTPPort, ih, idx)
	setActiveContentGuards(w.Header())
	w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	// best-effort: WriteSubtitles may fail if the file isn't a subtitle stream
	_ = s.prober.WriteSubtitles(w, streamURL, "vtt", 0)
}

// ---- info / settings ------------------------------------------------------

// settingsPinnedNumericKeys are settings whose value must be a non-negative
// JSON number when present in a POST /settings patch (API-2). cacheSize is
// handled separately since it also accepts null (unlimited).
var settingsPinnedNumericKeys = []string{
	"btMaxConnections",
	"btHandshakeTimeout",
	"btRequestTimeout",
	"btDownloadSpeedSoftLimit",
	"btDownloadSpeedHardLimit",
	"btMinPeersForStable",
	"transcodeMaxBitRate",
	"transcodeConcurrency",
	"transcodeTrackConcurrency",
	"transcodeMaxWidth",
}

// settingsBoolKeys are settings whose value must be a JSON boolean when
// present in a POST /settings patch (API-2).
var settingsBoolKeys = []string{
	"localAddonEnabled",
	"transcodeHardwareAccel",
	"proxyStreamsEnabled",
	"seedingEnabled",
	"autoUpdateEnabled",
}

// settingsMaxBody caps a POST /settings body.
const settingsMaxBody = 64 << 10

// settingsAllowedKeys is the allowlist of keys a POST /settings patch may
// set; anything else is silently dropped (stremio-web round-trips extras).
var settingsAllowedKeys = map[string]struct{}{
	"cacheSize": {}, "btMaxConnections": {}, "btHandshakeTimeout": {}, "btRequestTimeout": {},
	"btDownloadSpeedSoftLimit": {}, "btDownloadSpeedHardLimit": {}, "btMinPeersForStable": {},
	"remoteHttps": {}, "localAddonEnabled": {}, "transcodeHorsepower": {}, "transcodeMaxBitRate": {},
	"transcodeConcurrency": {}, "transcodeTrackConcurrency": {}, "transcodeHardwareAccel": {},
	"transcodeProfile": {}, "allTranscodeProfiles": {}, "transcodeMaxWidth": {}, "proxyStreamsEnabled": {},
	"seedingEnabled": {}, "trackersSourceUrl": {}, "cachedTrackers": {}, "trackersLastUpdated": {},
	"autoUpdateEnabled": {}, "updateChannel": {}, "updateCheckIntervalHours": {},
}

// settingsReadOnlyKeys are server-computed fields a client patch must never
// override. Present but silently dropped rather than rejected, since
// stremio-web round-trips the full GET /settings values object on save.
var settingsReadOnlyKeys = []string{"appPath", "cacheRoot", "serverVersion"}

// settingsWriteAllowed refuses Origin-less POSTs from non-loopback peers
// (a LAN/remote script, not a browser page) unless an api password is
// configured and the request supplies it.
func (s *server) settingsWriteAllowed(r *http.Request) bool {
	if r.Header.Get("Origin") != "" || s.cfg.AllowAllOrigins {
		return true
	}
	if ip := remoteIP(r); ip != nil && ip.IsLoopback() {
		return true
	}
	return s.cfg.ProxyPassword != "" && s.sp.Authorize(r) == nil
}

// validateSettingsPatch type-checks the known keys of a POST /settings body
// (API-2). Unknown keys are left untouched — stremio-web sends extras this
// server doesn't model — and settingsReadOnlyKeys are the caller's
// responsibility to strip once validation succeeds. Returns "" when patch is
// acceptable, otherwise a message naming the offending key.
func validateSettingsPatch(patch map[string]any) string {
	if v, ok := patch["cacheSize"]; ok && v != nil {
		if n, isNum := v.(float64); !isNum || n < 0 {
			return "cacheSize must be null or a non-negative number"
		}
	}
	for _, key := range settingsPinnedNumericKeys {
		v, ok := patch[key]
		if !ok {
			continue
		}
		if n, isNum := v.(float64); !isNum || n < 0 {
			return key + " must be a non-negative number"
		}
	}
	if v, ok := patch["transcodeHorsepower"]; ok {
		if _, isNum := v.(float64); !isNum {
			return "transcodeHorsepower must be a number"
		}
	}
	for _, key := range settingsBoolKeys {
		v, ok := patch[key]
		if !ok {
			continue
		}
		if _, isBool := v.(bool); !isBool {
			return key + " must be a boolean"
		}
	}
	if v, ok := patch["remoteHttps"]; ok {
		if _, isStr := v.(string); !isStr {
			return "remoteHttps must be a string"
		}
	}
	if v, ok := patch["transcodeProfile"]; ok && v != nil {
		if _, isStr := v.(string); !isStr {
			return "transcodeProfile must be a string or null"
		}
	}
	// The tracker and update-policy keys are echoed back by GET /settings, whose
	// stremio-core deserializer is strict about their types: a null or wrongly
	// typed value stored here would break every later read.
	for _, key := range []string{"allTranscodeProfiles", "cachedTrackers"} {
		v, ok := patch[key]
		if !ok {
			continue
		}
		list, isList := v.([]any)
		if !isList {
			return key + " must be an array of strings"
		}
		for _, e := range list {
			if _, isStr := e.(string); !isStr {
				return key + " must be an array of strings"
			}
		}
	}
	for _, key := range []string{"trackersSourceUrl", "updateChannel"} {
		if v, ok := patch[key]; ok {
			if _, isStr := v.(string); !isStr {
				return key + " must be a string"
			}
		}
	}
	if v, ok := patch["trackersLastUpdated"]; ok {
		if n, isNum := v.(float64); !isNum || n < 0 || n != math.Trunc(n) {
			return "trackersLastUpdated must be a non-negative integer"
		}
	}
	if v, ok := patch["updateCheckIntervalHours"]; ok {
		if n, isNum := v.(float64); !isNum || n < 0 {
			return "updateCheckIntervalHours must be a non-negative number"
		}
	}
	return ""
}

func (s *server) handleSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if !s.settingsWriteAllowed(r) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "settings write requires a browser origin, a loopback peer, or api_password"})
			return
		}
		var patch map[string]any
		if r.Body != nil {
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, settingsMaxBody)).Decode(&patch); err != nil && !errors.Is(err, io.EOF) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body: " + err.Error()})
				return
			}
		}
		if patch != nil {
			if msg := validateSettingsPatch(patch); msg != "" {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": msg})
				return
			}
			for _, key := range settingsReadOnlyKeys {
				delete(patch, key)
			}
			for key := range patch {
				if _, ok := settingsAllowedKeys[key]; !ok {
					delete(patch, key)
				}
			}
			s.ss.Extend(patch)
			if err := s.ss.Save(); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true})
		return
	}
	ifaces := availableInterfaces()
	writeJSON(w, http.StatusOK, map[string]any{
		"options": s.ss.OptionsSchema(ifaces),
		"values":  s.ss.Values(),
		"baseUrl": s.baseURL(),
	})
}

func (s *server) handleLanding(w http.ResponseWriter, r *http.Request) {
	base := s.cfg.PublicURL
	if base == "" {
		base = streamproxy.ExternalBase(r)
	}
	loc, err := url.Parse(s.cfg.WebUI)
	if err != nil {
		http.Error(w, "invalid WEB_UI_LOCATION", http.StatusInternalServerError)
		return
	}
	// Set the parameters on the parsed URL so an existing query string in
	// WEB_UI_LOCATION is preserved and the query is encoded for us. The
	// app.strem.io shell reads `streamingServer`; stremio-web reads
	// `streamingServerUrl` (src/App/SearchParamsHandler.js), so set both and
	// the redirect works whichever shell WEB_UI_LOCATION points at.
	q := loc.Query()
	q.Set("streamingServer", base)
	q.Set("streamingServerUrl", base)
	loc.RawQuery = q.Encode()
	http.Redirect(w, r, loc.String(), http.StatusTemporaryRedirect)
}

// ---- media helpers --------------------------------------------------------

// parseRational parses an ffprobe r_frame_rate string like "30000/1001" into a
// float64 frame rate.  Returns 0 for missing or malformed values.
func parseRational(s string) float64 {
	if s == "" || s == "0/0" {
		return 0
	}
	parts := strings.SplitN(s, "/", 2)
	if len(parts) == 1 {
		v, _ := strconv.ParseFloat(s, 64)
		return v
	}
	num, err1 := strconv.ParseFloat(parts[0], 64)
	den, err2 := strconv.ParseFloat(parts[1], 64)
	if err1 != nil || err2 != nil || den == 0 {
		return 0
	}
	return num / den
}

// reshapeProbeResult converts the raw ffprobe JSON map returned by Probe() into
// the enriched format the reference server returns.  The result includes:
//   - top-level duration (format.duration as float64)
//   - format.name (format_name)
//   - per-stream: index, track, codec, channels, width, height, fps, bitrate,
//     lang, default, profile — all sourced from the corresponding ffprobe fields.
//
// All new fields are ADDITIVE: existing callers (canPlayStream) still see
// track/codec/channels on every stream entry.
func reshapeProbeResult(raw map[string]interface{}) map[string]any {
	out := map[string]any{
		"format":   map[string]any{"name": ""},
		"duration": float64(0),
		"streams":  []any{},
	}
	if raw == nil {
		return out
	}
	if f, ok := raw["format"].(map[string]interface{}); ok {
		name, _ := f["format_name"].(string)
		var dur float64
		switch v := f["duration"].(type) {
		case float64:
			dur = v
		case string:
			dur, _ = strconv.ParseFloat(v, 64)
		}
		out["format"] = map[string]any{"name": name}
		out["duration"] = dur
	}
	if ss, ok := raw["streams"].([]interface{}); ok {
		streams := make([]any, 0, len(ss))
		for _, sv := range ss {
			sm, ok := sv.(map[string]interface{})
			if !ok {
				continue
			}
			// channels: only audio streams carry this; video/subtitle default to 0.
			ch, _ := sm["channels"].(float64)
			// fps: r_frame_rate is a fraction string ("25/1", "30000/1001", …).
			fps := parseRational(fmt.Sprintf("%v", sm["r_frame_rate"]))
			// bitrate: bit_rate is a decimal string in ffprobe JSON.
			var bitrate int64
			if brs, ok := sm["bit_rate"].(string); ok {
				bitrate, _ = strconv.ParseInt(brs, 10, 64)
			}
			// lang from tags sub-object.
			var lang string
			if tags, ok := sm["tags"].(map[string]interface{}); ok {
				lang, _ = tags["language"].(string)
			}
			// default from disposition sub-object (ffprobe stores it as a number).
			var isDefault bool
			if disp, ok := sm["disposition"].(map[string]interface{}); ok {
				if dv, ok := disp["default"].(float64); ok {
					isDefault = dv != 0
				}
			}
			profile, _ := sm["profile"].(string)
			width, _ := sm["width"].(float64)
			height, _ := sm["height"].(float64)
			idx, _ := sm["index"].(float64)

			entry := map[string]any{
				"index":    int(idx),
				"track":    sm["codec_type"],
				"codec":    sm["codec_name"],
				"channels": int(ch),
				"width":    int(width),
				"height":   int(height),
				"fps":      fps,
				"bitrate":  bitrate,
				"lang":     lang,
				"default":  isDefault,
				"profile":  profile,
			}
			streams = append(streams, entry)
		}
		out["streams"] = streams
	}
	return out
}

// handleProbe answers /probe?url=… by reshaping the raw ffprobe output into the
// enriched reference format (index/track/codec/channels/width/height/fps/
// bitrate/lang/default/profile per stream, plus top-level duration).
func (s *server) handleProbe(w http.ResponseWriter, r *http.Request) {
	res, err := s.prober.Probe(r.URL.Query().Get("url"))
	if err != nil {
		writeJSON(w, statusFor(err), map[string]any{"error": err.Error()})
		return
	}
	raw, _ := res.(map[string]interface{})
	writeJSON(w, http.StatusOK, reshapeProbeResult(raw))
}

// handleHLSProbe answers /hlsv2/probe?mediaURL=… in the shape stremio-video's
// canPlayStream expects.  The response includes all enriched fields from the
// reference: index/track/codec/channels/width/height/fps/bitrate/lang/default/
// profile per stream, plus top-level duration and format.name.
func (s *server) handleHLSProbe(w http.ResponseWriter, r *http.Request) {
	res, err := s.prober.Probe(r.URL.Query().Get("mediaURL"))
	if err != nil {
		http.Error(w, err.Error(), statusFor(err))
		return
	}
	raw, _ := res.(map[string]interface{})
	writeJSON(w, http.StatusOK, reshapeProbeResult(raw))
}

// handleHLS dispatches /hlsv2/* : probe, the per-session master playlist, and
// the live media playlist + .ts segments produced by ffmpeg.
//
// With RIVULET_HLS_SESSION_OVERRIDES enabled, the master.m3u8 request may
// carry per-session overrides (ttl, maxWidth, maxHeight, bitrate, maxRate,
// bufSize); they are passed through verbatim and validated by the prober,
// and an invalid value is answered with 400. Names are case-sensitive:
// unknown or mis-cased query parameters are ignored, and a repeated
// parameter uses its first value.
func (s *server) handleHLS(w http.ResponseWriter, r *http.Request, seg []string) {
	if r.Method == http.MethodDelete {
		s.handleHLSDelete(w, r, seg)
		return
	}
	if len(seg) >= 2 && seg[1] == "probe" {
		s.handleHLSProbe(w, r)
		return
	}
	if len(seg) < 3 {
		http.NotFound(w, r)
		return
	}
	id, file := seg[1], seg[2]
	if file == "master.m3u8" {
		q := r.URL.Query()
		master, err := s.prober.StartHLS(id, q.Get("mediaURL"), types.HLSSessionOptions{
			TTL:       q.Get("ttl"),
			MaxWidth:  q.Get("maxWidth"),
			MaxHeight: q.Get("maxHeight"),
			Bitrate:   q.Get("bitrate"),
			MaxRate:   q.Get("maxRate"),
			BufSize:   q.Get("bufSize"),
		})
		if err != nil {
			http.Error(w, err.Error(), statusFor(err))
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write([]byte(master))
		return
	}
	path, ct, err := s.prober.HLSFile(r.Context(), id, file)
	if err != nil {
		// A source failure is distinguishable from a missing segment, so a
		// client can back off or retry instead of treating it as the end.
		code := http.StatusNotFound
		switch {
		case errors.Is(err, types.ErrHLSSegmentTimeout):
			code = http.StatusGatewayTimeout
		case errors.Is(err, types.ErrHLSSourceFailed):
			code = http.StatusBadGateway
		}
		http.Error(w, err.Error(), code)
		return
	}
	setActiveContentGuards(w.Header())
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFile(w, r, path)
}

// handleHLSDelete ends an HLS session on request (DELETE /hlsv2/{id}): the
// session is unregistered at once and its segment cache is removed from disk
// as soon as no request for it is still being served. Lets a front end that
// manages its own session lifecycle clean up without waiting for the idle
// reaper, and is the only way to end a session when
// RIVULET_HLS_SESSION_TTL=0 disables idle eviction. Like every other route
// it is gated by the Origin allowlist in ServeHTTP.
func (s *server) handleHLSDelete(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) != 2 {
		http.NotFound(w, r)
		return
	}
	err := s.prober.DeleteHLS(seg[1])
	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, types.ErrHLSInvalidSessionID):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, types.ErrHLSSessionNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) handleTracks(w http.ResponseWriter, r *http.Request, seg []string) {
	raw := ""
	if len(seg) > 1 {
		raw, _ = url.PathUnescape(strings.Join(seg[1:], "/"))
	}
	res, err := s.prober.Tracks(raw)
	if err != nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *server) handleOpenSubHash(w http.ResponseWriter, r *http.Request) {
	res, err := s.prober.OpenSubHash(r.URL.Query().Get("videoUrl"))
	writeJSON(w, statusFor(err), map[string]any{"error": errStr(err), "result": res})
}

func (s *server) handleSubtitlesTracks(w http.ResponseWriter, r *http.Request) {
	res, err := s.prober.SubtitlesTracks(r.URL.Query().Get("subsUrl"))
	writeJSON(w, statusFor(err), map[string]any{"error": errStr(err), "result": res})
}

func (s *server) handleSubtitles(w http.ResponseWriter, r *http.Request, ext string) {
	q := r.URL.Query()
	offset := 0
	if o := q.Get("offset"); o != "" {
		offset, _ = strconv.Atoi(o)
	}
	setActiveContentGuards(w.Header())
	if ext == "vtt" {
		w.Header().Set("Content-Type", "text/vtt; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	if err := s.prober.WriteSubtitles(w, q.Get("from"), ext, offset); err != nil {
		// headers may already be sent; best effort
		return
	}
}

// ---- helpers --------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func withTimeout(r *http.Request, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), d)
}

func hexDecode(s string) ([]byte, error) {
	return hex.DecodeString(strings.TrimSpace(s))
}

func mimeByName(name string) string {
	// Find the dot first on the original string, then lowercase only the
	// extension — avoids lowercasing the entire filename path (smaller alloc).
	i := strings.LastIndexByte(name, '.')
	if i < 0 {
		return "application/octet-stream"
	}
	ext := strings.ToLower(name[i:])
	if m, ok := videoMimes[ext]; ok {
		return m
	}
	if m := mime.TypeByExtension(ext); m != "" {
		return m
	}
	return "application/octet-stream"
}

func peerSearchSources(b *createBody) []string {
	if b.PeerSearch != nil {
		return b.PeerSearch.Sources
	}
	return nil
}

func trackersFromSources(sources []string) []string {
	var out []string
	for _, src := range sources {
		if strings.HasPrefix(src, "tracker:") {
			out = append(out, strings.TrimPrefix(src, "tracker:"))
		}
	}
	return out
}

func announceFromTorrent(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var m struct {
		Announce     string     `json:"announce"`
		AnnounceList [][]string `json:"announce-list"`
		Sources      []string   `json:"sources"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	var out []string
	if m.Announce != "" {
		out = append(out, m.Announce)
	}
	for _, tier := range m.AnnounceList {
		out = append(out, tier...)
	}
	out = append(out, trackersFromSources(m.Sources)...)
	return out
}

func truthy(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "false" && s != "null" && s != "0" && s != `""`
}

// Interface enumeration (net.Interfaces + Addrs) is a syscall-heavy operation
// that dominated /settings and /network-info latency, both of which stremio-web
// polls. Cache the result briefly; interfaces rarely change within the TTL.
var (
	ifaceCacheMu sync.RWMutex
	ifaceCache   []string
	ifaceCacheAt time.Time
)

const ifaceCacheTTL = 30 * time.Second

// availableInterfaces returns the cached interface list, recomputing it at most
// once per ifaceCacheTTL. The returned slice is shared and must not be mutated.
func availableInterfaces() []string {
	ifaceCacheMu.RLock()
	if ifaceCache != nil && time.Since(ifaceCacheAt) < ifaceCacheTTL {
		v := ifaceCache
		ifaceCacheMu.RUnlock()
		return v
	}
	ifaceCacheMu.RUnlock()

	v := computeInterfaces()
	ifaceCacheMu.Lock()
	ifaceCache, ifaceCacheAt = v, time.Now()
	ifaceCacheMu.Unlock()
	return v
}

// availableInterfaces returns non-internal, non-link-local IPv4 + global IPv6
// addresses. This is the IPv6 fix: the original server returned IPv4 only.
func computeInterfaces() []string {
	var out []string
	seen := map[string]bool{}
	ifaces, err := net.Interfaces()
	if err != nil {
		return []string{}
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ip net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
				continue
			}
			s := ip.String()
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	if out == nil {
		out = []string{}
	}
	return out
}

func (s *server) baseURL() string {
	if s.cfg.PublicURL != "" {
		return s.cfg.PublicURL
	}
	for _, a := range availableInterfaces() {
		if net.ParseIP(a) != nil && strings.Count(a, ":") == 0 { // first IPv4
			return fmt.Sprintf("http://%s:%d", a, s.cfg.HTTPPort)
		}
	}
	return fmt.Sprintf("http://127.0.0.1:%d", s.cfg.HTTPPort)
}

func loadavg() []float64 {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return []float64{0, 0, 0}
	}
	f := strings.Fields(string(b))
	out := make([]float64, 0, 3)
	for i := 0; i < 3 && i < len(f); i++ {
		v, _ := strconv.ParseFloat(f[i], 64)
		out = append(out, v)
	}
	for len(out) < 3 {
		out = append(out, 0)
	}
	return out
}

func cpus() []any {
	n := runtime.NumCPU()
	out := make([]any, n)
	for i := range out {
		out[i] = map[string]any{}
	}
	return out
}

func statusFor(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, types.ErrInvalidMediaURL), errors.Is(err, types.ErrInvalidHLSOption):
		return http.StatusBadRequest // the caller's URL/option, not a server fault
	}
	return http.StatusInternalServerError
}

func errStr(err error) any {
	if err != nil {
		return err.Error()
	}
	return nil
}

// streamBufPool holds reusable 256 KB buffers for streaming copies.
var streamBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 256<<10)
		return &buf
	},
}

// writerOnly hides an http.ResponseWriter's io.ReaderFrom. io.CopyBuffer
// ignores the buffer it is given when the destination implements ReaderFrom,
// and *http.response does: its ReadFrom only uses sendfile for *os.File and
// otherwise copies through net/http's own 32 KB pool, so the pooled 256 KB
// buffer was never used and torrent readers were read 32 KB at a time.
type writerOnly struct{ io.Writer }

// streamWriteIdleTimeout bounds how long a single response write may block on
// a client that has stopped reading (socket send buffer / flow-control window
// full). The deadline is armed immediately before every write and cleared
// right after it returns, so a healthy stream of any total duration is never
// cut — only a client that makes no progress for this long is dropped,
// releasing its goroutine, torrent reader and pooled buffer. A paused player
// also stops reading, and not every player reconnects cleanly on resume, so
// the bound is long enough for ordinary pauses (an hour) while still
// reclaiming connections whose client is gone or wedged. 0 disables.
var streamWriteIdleTimeout = time.Hour

// deadlineWriter brackets each Write with the connection's write deadline:
// armed just before the write, cleared just after. Clearing matters on
// HTTP/2 (the HTTPS listener), where SetWriteDeadline arms a timer that resets
// the stream whether or not a write is in flight; a deadline left pending
// while copyStream is blocked reading a stalled SOURCE (a torrent with no
// peers) would reset a perfectly healthy client. rc is held by value so the
// wrapper costs a single allocation per stream.
type deadlineWriter struct {
	w       http.ResponseWriter
	rc      http.ResponseController
	timeout time.Duration
	off     bool // deadlines unsupported by w: pass writes straight through
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	if d.off {
		return d.w.Write(p)
	}
	if err := d.rc.SetWriteDeadline(time.Now().Add(d.timeout)); err != nil {
		// Not supported by this writer (e.g. httptest recorder): stop trying.
		d.off = true
		return d.w.Write(p)
	}
	n, err := d.w.Write(p)
	_ = d.rc.SetWriteDeadline(time.Time{})
	return n, err
}

// copyStream copies src to w through buf (a streamBufPool buffer), in
// buf-sized reads. When w is an http.ResponseWriter each write is bounded by
// the idle deadline (streamWriteIdleTimeout); no deadline is pending while
// src is being read.
func copyStream(w io.Writer, src io.Reader, buf []byte) {
	if rw, ok := w.(http.ResponseWriter); ok && streamWriteIdleTimeout > 0 {
		dw := &deadlineWriter{w: rw, rc: *http.NewResponseController(rw), timeout: streamWriteIdleTimeout}
		_, _ = io.CopyBuffer(dw, src, buf)
		return
	}
	_, _ = io.CopyBuffer(writerOnly{w}, src, buf)
}

// buildStreamProxyConfig converts types.Config proxy fields to a streamproxy.Config.
func buildStreamProxyConfig(cfg types.Config, client *http.Client, privateAllow *netguard.Allow) streamproxy.Config {
	// Decode ProxySecret: try hex then base64url then base64std.
	var secret []byte
	if cfg.ProxySecret != "" {
		if b, err := hex.DecodeString(cfg.ProxySecret); err == nil {
			secret = b
		} else if b, err := base64.RawURLEncoding.DecodeString(cfg.ProxySecret); err == nil {
			secret = b
		} else if b, err := base64.StdEncoding.DecodeString(cfg.ProxySecret); err == nil {
			secret = b
		}
	}

	// Parse ProxyIPACL: comma-separated CIDR strings.
	var ipACL []*net.IPNet
	for _, cidr := range strings.Split(cfg.ProxyIPACL, ",") {
		cidr = strings.TrimSpace(cidr)
		if cidr == "" {
			continue
		}
		if _, ipNet, err := net.ParseCIDR(cidr); err == nil {
			ipACL = append(ipACL, ipNet)
		}
	}

	return streamproxy.Config{
		Password:         cfg.ProxyPassword,
		Secret:           secret,
		IPACL:            ipACL,
		Prebuffer:        cfg.ProxyPrebuffer,
		SegCacheTTL:      time.Duration(cfg.ProxySegCacheTTL) * time.Second,
		PublicURL:        cfg.ProxyPublicURL,
		Client:           client,
		UpstreamProxy:    cfg.ProxyUpstream,
		BlockPrivate:     !proxyAllowPrivate(),
		PrivateAllow:     privateAllow,
		TrustedProxies:   parseTrustedProxies(os.Getenv("RIVULET_TRUSTED_PROXIES")),
		AppPath:          cfg.AppPath,
		ExtractorsFile:   cfg.ExtractorsFile,
		ExtractorsURL:    cfg.ExtractorsURL,
		ExtractorsPubKey: cfg.ExtractorsPubKey,
	}
}

// handleProxy implements /proxy/<opts>/<path> used by stremio-video to fetch an
// external stream with injected request/response headers (proxyStreamsEnabled or
// per-stream proxyHeaders). opts is a URLSearchParams string: d=<origin>,
// repeated h=<reqHeader>, repeated r=<respHeader>.
func (s *server) handleProxy(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}
	if err := s.sp.Authorize(r); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.sp.Protected() && r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	opts, err := url.ParseQuery(seg[1])
	if err != nil {
		http.Error(w, "bad proxy options", http.StatusBadRequest)
		return
	}
	origin := opts.Get("d")
	if origin == "" {
		http.Error(w, "missing proxy target", http.StatusBadRequest)
		return
	}
	target := strings.TrimRight(origin, "/") + "/" + strings.Join(seg[2:], "/")
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	if err := s.sp.ValidateDest(target); err != nil {
		http.Error(w, "forbidden target", http.StatusForbidden)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	for _, h := range opts["h"] { // injected request headers "key:value"
		if i := strings.IndexByte(h, ':'); i > 0 {
			name := strings.TrimSpace(h[:i])
			if streamproxy.BlockedReqHeader(name) {
				continue
			}
			req.Header.Set(name, strings.TrimSpace(h[i+1:]))
		}
	}
	if rng := r.Header.Get("Range"); rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := s.proxyClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	for _, k := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag"} {
		if v := resp.Header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	setActiveContentGuards(w.Header())
	for _, h := range opts["r"] { // injected response headers; skip CORS headers the caller must not override
		if i := strings.IndexByte(h, ':'); i > 0 {
			k := http.CanonicalHeaderKey(strings.TrimSpace(h[:i]))
			if streamproxy.BlockedRespHeader(k) || k == "X-Content-Type-Options" {
				continue
			}
			w.Header().Set(k, strings.TrimSpace(h[i+1:]))
		}
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		bufp := streamBufPool.Get().(*[]byte)
		defer streamBufPool.Put(bufp)
		copyStream(w, resp.Body, *bufp)
	}
}

var (
	hwAccelsOnce  sync.Once
	hwAccelsCache []any
)

// detectHWAccels reports available hardware acceleration profiles (VAAPI on the
// render node) for device-info / hwaccel-profiler. Cached after first call.
func detectHWAccels() []any {
	hwAccelsOnce.Do(func() {
		hwAccelsCache = []any{}
		if _, err := os.Stat("/dev/dri/renderD128"); err != nil {
			return
		}
		out, err := exec.Command("ffmpeg", "-hide_banner", "-hwaccels").Output()
		if err == nil && strings.Contains(string(out), "vaapi") {
			hwAccelsCache = []any{"vaapi"}
		}
	})
	return hwAccelsCache
}

// contentDisposition builds a Content-Disposition attachment header value with
// an ASCII-safe filename= fallback (RFC 2183) and an RFC 5987 filename*= field
// for proper handling of non-ASCII filenames by modern browsers.
func contentDisposition(name string) string {
	ascii := strings.Map(func(r rune) rune {
		if r >= 0x20 && r <= 0x7E && r != '"' && r != '\\' {
			return r
		}
		return '_'
	}, name)
	return "attachment; filename=\"" + ascii + "\"; filename*=UTF-8''" + rfc5987Encode(name)
}

// rfc5987Encode percent-encodes a UTF-8 string for use as an RFC 5987 ext-value.
// attr-char octets (ALPHA / DIGIT / selected symbols) pass through unchanged;
// all other bytes are %XX encoded over their UTF-8 representation.
func rfc5987Encode(s string) string {
	const hx = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '!' || c == '#' || c == '$' || c == '&' || c == '+' || c == '-' ||
			c == '.' || c == '^' || c == '_' || c == '`' || c == '|' || c == '~' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(hx[c>>4])
			b.WriteByte(hx[c&0xF])
		}
	}
	return b.String()
}

// Close releases background resources owned by the handler (stream-proxy
// segment-cache janitor). Safe to call more than once.
func (s *server) Close() error {
	s.sp.Close()
	if s.dvr != nil {
		_ = s.dvr.Close() // stops active recordings gracefully so the files stay playable
	}
	return nil
}

// writeEngineErr maps an EnsureEngine failure to an HTTP status: 503 when the
// active-engine cap is hit, 500 otherwise.
func writeEngineErr(w http.ResponseWriter, err error) {
	if errors.Is(err, types.ErrTooManyEngines) {
		w.Header().Set("Retry-After", "5")
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if errors.Is(err, types.ErrInvalidInfoHash) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
