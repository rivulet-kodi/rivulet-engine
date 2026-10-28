// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

// Package streamproxy implements an HTTP stream proxy with HLS/DASH manifest
// rewriting, optional segment decryption, signed URLs, and caching.
package streamproxy

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/proxy"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
	"github.com/rivulet-kodi/rivulet-engine/internal/netguard"
)

// Config holds the runtime configuration for the proxy handler.
type Config struct {
	Password      string        // api_password; "" disables password auth
	Secret        []byte        // 32-byte key for signed-URL tokens (AES-GCM)
	IPACL         []*net.IPNet  // client-IP allowlist; empty = allow all
	Prebuffer     int           // upcoming segments to prefetch (0 = off)
	SegCacheTTL   time.Duration // segment cache TTL (0 = caching off)
	PublicURL     string        // explicit external base; "" = derive from request
	Client        *http.Client  // shared streaming HTTP client
	UpstreamProxy string        // global outbound proxy URL; "" = direct (socks5/http/https)
	// TrustedProxies lists extra reverse-proxy CIDRs (beyond loopback) whose
	// X-Forwarded-* headers are honoured (RIVULET_TRUSTED_PROXIES).
	TrustedProxies []*net.IPNet
	// BlockPrivate rejects loopback/private/link-local destinations even when
	// no auth is configured. The server sets it unless
	// RIVULET_PROXY_ALLOW_PRIVATE=1. Zero value keeps the legacy behaviour.
	BlockPrivate bool
	// PrivateAllow lists private destinations that stay reachable even when
	// private addresses are blocked (RIVULET_PROXY_PRIVATE_ALLOW). nil = none.
	PrivateAllow *netguard.Allow
	// AppPath is where extractors.json and the cached remote list live.
	AppPath string
	// ExtractorsFile overrides <AppPath>/extractors.json.
	ExtractorsFile string
	// ExtractorsURL is a remote definitions list; it is only used together
	// with a valid ExtractorsPubKey (ed25519, hex/base64), and its detached
	// signature is fetched from ExtractorsURL+".sig".
	ExtractorsURL     string
	ExtractorsPubKey  string
	ExtractorsRefresh time.Duration // remote refresh interval (0 = 6h)
}

// ipCacheEntry holds a resolved public egress IP with an expiry timestamp.
type ipCacheEntry struct {
	ip        string
	expiresAt time.Time
}

// proxyClientEntry holds a cached upstream proxy client with an expiry
// timestamp, mirroring ipCacheEntry's TTL bookkeeping.
type proxyClientEntry struct {
	client    *http.Client
	expiresAt time.Time
}

// prefetchTimeout is the wall-clock deadline for each prefetch goroutine.
const prefetchTimeout = 30 * time.Second

// maxConcurrentPrefetch caps the total number of prefetch goroutines that may
// run simultaneously across all requests handled by a single Handler.
const maxConcurrentPrefetch = 8

// segmentWriteChunk is the size of each response write of a buffered
// (decrypted) segment. The write deadline is re-armed per chunk, so it bounds
// the time a client may take to accept one chunk rather than the whole
// segment: a healthy transfer of any duration is never cut.
const segmentWriteChunk = 256 << 10

// segmentWriteIdleTimeout is how long the proxy waits for a client to accept
// one segmentWriteChunk before dropping it. The server has no WriteTimeout, so
// without this a client that stops reading would hold a fully buffered
// segment, and its reservation in the in-flight budget, indefinitely. The
// limit also sets the slowest sustained rate that is still served
// (segmentWriteChunk / segmentWriteIdleTimeout, about 8.7 KiB/s).
const segmentWriteIdleTimeout = 30 * time.Second

// Handler is the stream proxy request handler.
type Handler struct {
	cfg          Config
	cache        *segCache
	closeOnce    sync.Once
	proxyMu      sync.Mutex
	proxyClients map[string]proxyClientEntry
	ipMu         sync.Mutex
	ipCache      map[string]ipCacheEntry
	// prefetchSem is a semaphore that bounds the number of goroutines spawned
	// by prefetch across all concurrent requests.
	prefetchSem chan struct{}
	// inflight bounds the segment bytes buffered in memory at once while an
	// upstream body is being read (cache fills, prefetches, DRM decrypts);
	// the segment cache's own budget only counts entries already stored.
	inflight *byteBudget
	// writeIdle overrides segmentWriteIdleTimeout when non-zero (tests).
	writeIdle time.Duration
	// signingGCM is the pre-built AES-GCM cipher for token sign/verify (F7).
	// nil when Secret is empty. cipher.AEAD is goroutine-safe.
	signingGCM cipher.AEAD
	// passwordBytes is cfg.Password pre-converted to []byte to avoid a
	// per-request allocation in the constant-time password comparison (F11).
	passwordBytes []byte
	// flightMu guards flights, the in-flight segment fetches used to
	// de-duplicate concurrent cache misses (see cachedFetch).
	flightMu sync.Mutex
	flights  map[string]*flightCall
	// defs holds the declarative /extractor definitions.
	defs *defRegistry
	// embeds caches embed-page resolutions for /proxy/stream.
	embeds embedCache
}

// Close stops background goroutines (segment-cache janitor). Idempotent.
func (h *Handler) Close() {
	h.closeOnce.Do(func() {
		if h.cache != nil {
			h.cache.stop()
		}
		if h.defs != nil {
			h.defs.close()
		}
	})
}

// New creates a Handler. A nil Client is replaced with http.DefaultClient.
func New(cfg Config) *Handler {
	if cfg.Client == nil {
		cfg.Client = http.DefaultClient
	}
	if cfg.TrustedProxies != nil {
		SetTrustedProxies(cfg.TrustedProxies)
	}
	var c *segCache
	if cfg.SegCacheTTL > 0 {
		c = newSegCache(cfg.SegCacheTTL, 256)
	}
	// Pre-build the signing GCM once; cipher.AEAD (GCM) is goroutine-safe,
	// so it can be shared across all concurrent requests (F7).
	var gcm cipher.AEAD
	if len(cfg.Secret) > 0 {
		if block, err := aes.NewCipher(cfg.Secret); err == nil {
			gcm, _ = cipher.NewGCM(block)
		}
	}
	if gcm == nil && len(cfg.Secret) > 0 {
		// Secret was set but is not a valid AES key length (16/24/32 bytes);
		// leaving gcm nil here would let signToken/verifyToken's cheap
		// len(h.cfg.Secret)==0 guard pass through to a nil-interface panic.
		// Clearing Secret makes that guard fire instead, degrading cleanly
		// to "signing disabled" rather than crashing every token operation.
		logging.For("streamproxy").Warn("invalid signing secret length; disabling token signing", "len", len(cfg.Secret))
		cfg.Secret = nil
	}
	return &Handler{
		cfg:          cfg,
		cache:        c,
		proxyClients: make(map[string]proxyClientEntry),
		ipCache:      make(map[string]ipCacheEntry),
		prefetchSem:  make(chan struct{}, maxConcurrentPrefetch),
		inflight:     newByteBudget(defaultInflightMaxBytes),
		signingGCM:   gcm,
		// Pre-convert password bytes once to avoid per-request allocation (F11).
		passwordBytes: []byte(cfg.Password),
		flights:       make(map[string]*flightCall),
		defs:          newDefRegistry(cfg),
	}
}

// Options carries decoded proxy request parameters.
type Options struct {
	Dest        string
	ReqHeaders  http.Header
	RespHeaders http.Header
	APIPassword string
	Proxy       string // per-request upstream proxy override (socks5/http/https)

	// subTokenExp/subTokenIP carry the validity of the signed token that
	// authorised this request (zero when it was not token-authorised or the
	// server has no password). When set, rewritten sub-URLs get a freshly
	// minted per-URL token instead of an unauthenticated URL.
	subTokenExp int64
	subTokenIP  string
}

// DecryptParams carries segment decryption parameters.
type DecryptParams struct {
	Method string
	Key    []byte
	KeyID  []byte
	IV     []byte
}

// Registration hooks set by feature files in init(); foundation compiles with them nil.
var hlsHandler func(h *Handler, w http.ResponseWriter, r *http.Request)
var mpdHandler func(h *Handler, w http.ResponseWriter, r *http.Request)

// segmentDecryptor may decrypt segment in place: the caller hands over
// ownership and must not use segment afterwards (only the returned slice).
var segmentDecryptor func(h *Handler, p DecryptParams, segment []byte) ([]byte, error)

// writeIdleTimeout returns the effective per-chunk write idle timeout.
func (h *Handler) writeIdleTimeout() time.Duration {
	if h.writeIdle > 0 {
		return h.writeIdle
	}
	return segmentWriteIdleTimeout
}

// writeSegment writes p to w in segmentWriteChunk pieces, re-arming the
// connection's write deadline to idle before each one (via
// http.ResponseController, which also reaches through wrappers that implement
// Unwrap). A client that makes no progress for idle therefore fails the
// pending Write with a timeout and the caller returns, releasing whatever it
// reserved for p. The last deadline is deliberately left armed: it also bounds
// the flush of the buffered tail when the handler returns, and net/http clears
// it itself once the response is finished. Writers without deadline support
// (e.g. httptest recorders, which never block) are written without one.
func writeSegment(w http.ResponseWriter, p []byte, idle time.Duration) error {
	rc := http.NewResponseController(w)
	armed := idle > 0
	for len(p) > 0 {
		n := min(len(p), segmentWriteChunk)
		if armed {
			if err := rc.SetWriteDeadline(time.Now().Add(idle)); err != nil {
				armed = false
			}
		}
		if _, err := w.Write(p[:n]); err != nil {
			return err
		}
		p = p[n:]
	}
	return nil
}

// Route dispatches /proxy/* sub-paths. seg[0] is "proxy". Returns true if handled.
func (h *Handler) Route(w http.ResponseWriter, r *http.Request, seg []string) bool {
	if len(seg) < 2 {
		return false
	}
	switch seg[1] {
	case "stream":
		h.serveStream(w, r)
		return true
	case "hls":
		if hlsHandler != nil {
			hlsHandler(h, w, r)
		} else {
			http.Error(w, "HLS proxy not implemented", http.StatusNotImplemented)
		}
		return true
	case "mpd":
		if mpdHandler != nil {
			mpdHandler(h, w, r)
		} else {
			http.Error(w, "MPD proxy not implemented", http.StatusNotImplemented)
		}
		return true
	case "ip":
		h.serveIP(w, r)
		return true
	default:
		return false
	}
}

// maxGenerateURLBody caps the /generate_url request body.
const maxGenerateURLBody = 64 << 10

// maxTokenExpirySeconds bounds expiry_seconds accepted by /generate_url (one
// year); larger values risk time.Duration overflow and effectively-permanent
// bearer tokens.
const maxTokenExpirySeconds = 365 * 24 * 3600

// validEndpoint reports whether ep is a proxy path a token may be issued for.
func validEndpoint(ep string) bool {
	switch {
	case ep == "/proxy/stream", ep == "/proxy/ip":
		return true
	case strings.HasPrefix(ep, "/proxy/hls/"), strings.HasPrefix(ep, "/proxy/mpd/"), strings.HasPrefix(ep, "/proxy/stream/"):
		return !strings.ContainsAny(ep, "?#\\ ")
	}
	return false
}

// validateGenerateRequest checks the /generate_url input: the endpoint must be
// a known proxy path, expiry_seconds must be within (0, maxTokenExpirySeconds],
// and a bound destination (params.d, plain or base64) must be an http(s) URL.
func validateGenerateRequest(endpoint string, params map[string]string, expirySeconds int) error {
	if !validEndpoint(endpoint) {
		return errors.New("unknown endpoint")
	}
	if expirySeconds < 1 || expirySeconds > maxTokenExpirySeconds {
		return fmt.Errorf("expiry_seconds must be between 1 and %d", maxTokenExpirySeconds)
	}
	if d, ok := params["d"]; ok {
		u, err := url.Parse(decodeDest(d))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errors.New("params.d must be an http(s) URL")
		}
	}
	return nil
}

// HandleGenerateURL handles POST /generate_url and returns a signed token URL.
func (h *Handler) HandleGenerateURL(w http.ResponseWriter, r *http.Request) {
	if err := h.authorize(r); err != nil {
		writeAuthError(w, err)
		return
	}
	if len(h.cfg.Secret) == 0 {
		http.Error(w, "token signing not configured", http.StatusBadRequest)
		return
	}
	var req struct {
		Endpoint      string            `json:"endpoint"`
		Params        map[string]string `json:"params"`
		ExpirySeconds int               `json:"expiry_seconds"`
		IP            string            `json:"ip"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxGenerateURLBody)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if err := validateGenerateRequest(req.Endpoint, req.Params, req.ExpirySeconds); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ipBind := ""
	if req.IP != "" {
		ip := net.ParseIP(strings.TrimSpace(req.IP))
		if ip == nil {
			http.Error(w, "invalid ip", http.StatusBadRequest)
			return
		}
		// Canonical form: verifyToken compares against net.IP.String().
		ipBind = ip.String()
	}
	exp := time.Now().Add(time.Duration(req.ExpirySeconds) * time.Second).Unix()
	tok := token{
		Endpoint: req.Endpoint,
		Params:   req.Params,
		Exp:      exp,
		IP:       ipBind,
	}
	signed, err := h.signToken(tok)
	if err != nil {
		http.Error(w, "failed to sign token", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"url":        req.Endpoint + "?token=" + signed,
		"expires_at": exp,
	})
}

// HandleBase64 handles /base64/encode and /base64/check.
func (h *Handler) HandleBase64(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) < 2 {
		http.NotFound(w, r)
		return
	}
	d := r.URL.Query().Get("d")
	w.Header().Set("Content-Type", "application/json")
	switch seg[1] {
	case "encode":
		enc := base64.RawURLEncoding.EncodeToString([]byte(d))
		_ = json.NewEncoder(w).Encode(map[string]string{"encoded_url": enc})
	case "check":
		decoded, err := tryBase64Decode(d)
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"is_base64": false, "decoded": d})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"is_base64": true, "decoded": decoded})
		}
	default:
		http.NotFound(w, r)
	}
}

// decodeDest decodes the d parameter value into a destination URL: a plain
// http(s) URL is used as-is; otherwise it is decoded as base64url (raw) or
// standard base64 and, failing both, returned unchanged for the caller to
// validate. A literal '+' in a plain URL is preserved (url.Query already
// turns unencoded '+' into space; %2B stays '+'). For standard base64 a space
// is mapped back to '+', the only way an unencoded '+' survives query parsing.
func decodeDest(raw string) string {
	switch {
	case strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://"):
		return raw
	case raw == "":
		return ""
	}
	if b, err := base64.RawURLEncoding.DecodeString(raw); err == nil {
		return string(b)
	}
	if b, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(raw, " ", "+")); err == nil {
		return string(b)
	}
	return raw // keep as-is; caller validates
}

// parseOptions decodes proxy request parameters from the query string.
func (h *Handler) parseOptions(r *http.Request) (*Options, error) {
	q := r.URL.Query()
	dest := decodeDest(q.Get("d"))

	// Validate proxy param: accept only socks5/socks5h/http/https schemes.
	var proxyParam string
	if raw := q.Get("proxy"); raw != "" {
		if u, err := url.Parse(raw); err == nil {
			switch u.Scheme {
			case "socks5", "socks5h", "http", "https":
				proxyParam = raw
			}
		}
	}

	opts := &Options{
		Dest:        dest,
		ReqHeaders:  make(http.Header),
		RespHeaders: make(http.Header),
		APIPassword: q.Get("api_password"),
		Proxy:       proxyParam,
	}
	for k, vs := range q {
		if after, ok := strings.CutPrefix(k, "h_"); ok {
			for _, v := range vs {
				opts.ReqHeaders.Add(after, v)
			}
		} else if after, ok := strings.CutPrefix(k, "r_"); ok {
			for _, v := range vs {
				opts.RespHeaders.Add(after, v)
			}
		}
	}
	// Token-authorised manifest requests carry no api_password, so rewritten
	// sub-URLs would be unauthenticated and rejected when a password is set.
	// Remember the parent token's expiry and IP binding so buildProxyURL can
	// mint per-sub-URL tokens (a token is bound to its endpoint and params
	// and cannot simply be copied across).
	if opts.APIPassword == "" && h.cfg.Password != "" && len(h.cfg.Secret) > 0 {
		if tok := q.Get("token"); tok != "" {
			if t, err := h.verifyToken(tok, clientIP(r)); err == nil {
				opts.subTokenExp = t.Exp
				opts.subTokenIP = t.IP
			}
		}
	}
	return opts, nil
}

// parseDecryptParams reads AES decryption parameters from the query string.
// key, key_id, and iv are accepted as hex or base64 (url or std).
func (h *Handler) parseDecryptParams(r *http.Request) (DecryptParams, error) {
	q := r.URL.Query()
	return DecryptParams{
		Method: q.Get("method"),
		Key:    decodeKeyParam(q.Get("key")),
		KeyID:  decodeKeyParam(q.Get("key_id")),
		IV:     decodeKeyParam(q.Get("iv")),
	}, nil
}

// decodeKeyParam decodes a hex- or base64-encoded key/iv parameter.
func decodeKeyParam(s string) []byte {
	if s == "" {
		return nil
	}
	if b, err := hex.DecodeString(strings.TrimSpace(s)); err == nil {
		return b
	}
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b
	}
	return nil
}

// fetch performs an HTTP request via the given upstream proxy (or the default
// client when proxyURL is ""). Caller is responsible for closing the response Body.
func (h *Handler) fetch(ctx context.Context, method, rawurl string, hdr http.Header, body io.Reader, proxyURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawurl, body)
	if err != nil {
		return nil, err
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return h.clientFor(proxyURL).Do(req)
}

// firstForwardedValue returns the first comma-separated element of a
// forwarded header value, trimmed.
func firstForwardedValue(v string) string {
	first, _, _ := strings.Cut(v, ",")
	return strings.TrimSpace(first)
}

// validForwardedHost reports whether s is a syntactically valid host[:port]
// (DNS name, IPv4, or bracketed IPv6, with an optional numeric port).
func validForwardedHost(s string) bool {
	if s == "" || len(s) > 255 {
		return false
	}
	var port string
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 || net.ParseIP(s[1:end]) == nil {
			return false
		}
		rest := s[end+1:]
		if rest != "" {
			if rest[0] != ':' {
				return false
			}
			port = rest[1:]
			if port == "" {
				return false
			}
		}
	} else {
		host := s
		if i := strings.LastIndexByte(s, ':'); i >= 0 {
			host, port = s[:i], s[i+1:]
			if port == "" {
				return false
			}
		}
		if host == "" {
			return false
		}
		for i := range len(host) {
			c := host[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-', c == '_':
			default:
				return false
			}
		}
	}
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || len(port) > 5 {
			return false
		}
		for i := range len(port) {
			if port[i] < '0' || port[i] > '9' {
				return false
			}
		}
	}
	return true
}

// externalBase returns the external base URL (no trailing slash).
// cfg.PublicURL wins when set. Otherwise the base is derived from r.Host, and
// X-Forwarded-Proto / X-Forwarded-Host are honoured only when the immediate
// peer is a loopback/private address (a reverse proxy); only the first
// comma-separated element is used, the proto must be http or https, and the
// host must be a syntactically valid host[:port]. Invalid values are ignored.
func (h *Handler) externalBase(r *http.Request) string {
	if h.cfg.PublicURL != "" {
		return strings.TrimRight(h.cfg.PublicURL, "/")
	}
	return ExternalBase(r)
}

// ExternalBase returns the externally visible base URL for a request: the
// scheme and host as the client sees them, honouring a TLS-terminated
// connection (r.TLS), the headers set by a reverse proxy, or r.Host otherwise.
//
// X-Forwarded-Proto/Host are honoured only when the immediate peer is a trusted
// (loopback or private) proxy, like the rest of the proxy's forwarded handling,
// so a public client cannot spoof the base. They are single-valued in practice
// but may be comma-joined lists; only the first element is used, and the scheme
// is accepted only when it is http or https.
//
// It is shared by the stream proxy and the GET / landing redirect so both
// derive the same base.
func ExternalBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if isTrustedProxy(peerIP(r)) {
		switch p := strings.ToLower(firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))); p {
		case "http", "https":
			scheme = p
		}
		if fh := firstForwardedValue(r.Header.Get("X-Forwarded-Host")); validForwardedHost(fh) {
			host = fh
		}
	}
	return scheme + "://" + host
}

// buildProxyURL constructs a proxy URL for the given destination.
// Format: <extBase><endpoint>?d=<base64url(dest)>[&h_/r_ headers][&api_password|&token][&proxy].
// Callers building many URLs for the same request (playlist rewrites) should
// use newProxyURLBuilder once and call build per URL.
func (h *Handler) buildProxyURL(extBase, endpoint, dest string, opts *Options) string {
	return h.newProxyURLBuilder(opts).build(extBase, endpoint, dest)
}

// proxyURLBuilder holds the per-request parts of a proxy URL — escaped
// header overrides, credential and upstream proxy — that are identical for
// every URL of one rewritten playlist, so they are escaped once instead of
// once per segment.
type proxyURLBuilder struct {
	h       *Handler
	opts    *Options
	headers string // "&h_…=…&r_…=…", already escaped
	cred    string // "&api_password=…" (password mode)
	tokens  bool   // mint a per-URL sub-token instead (token mode)
	tail    string // "&proxy=…"
}

func (h *Handler) newProxyURLBuilder(opts *Options) *proxyURLBuilder {
	pb := &proxyURLBuilder{h: h, opts: opts}
	if opts == nil {
		return pb
	}
	var b strings.Builder
	for k, vs := range opts.ReqHeaders {
		for _, v := range vs {
			b.WriteString("&h_")
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	for k, vs := range opts.RespHeaders {
		for _, v := range vs {
			b.WriteString("&r_")
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	pb.headers = b.String()
	if opts.APIPassword != "" {
		pb.cred = "&api_password=" + url.QueryEscape(opts.APIPassword)
	} else {
		// Same preconditions as subToken: only token-authorised requests on a
		// signing-enabled handler can mint sub-tokens; skip the per-URL work
		// otherwise.
		pb.tokens = opts.subTokenExp != 0 && len(h.cfg.Secret) > 0
	}
	if opts.Proxy != "" {
		pb.tail = "&proxy=" + url.QueryEscape(opts.Proxy)
	}
	return pb
}

// build returns the proxy URL for one destination with a single allocation
// (plus the sub-token in token mode).
func (pb *proxyURLBuilder) build(extBase, endpoint, dest string) string {
	enc := base64.RawURLEncoding
	var tok string
	if pb.tokens {
		if t := pb.h.subToken(pb.opts, endpoint, map[string]string{"d": enc.EncodeToString([]byte(dest))}); t != "" {
			tok = "&token=" + url.QueryEscape(t)
		}
	}
	var b strings.Builder
	b.Grow(len(extBase) + len(endpoint) + len("?d=") + enc.EncodedLen(len(dest)) +
		len(pb.headers) + len(pb.cred) + len(tok) + len(pb.tail))
	b.WriteString(extBase)
	b.WriteString(endpoint)
	b.WriteString("?d=")
	// Encode in 30-byte chunks (a multiple of 3, so no padding between
	// chunks): string→[]byte conversions of ≤32 bytes stay on the stack, so
	// this adds no allocation regardless of the destination's length.
	var buf [40]byte
	for i := 0; i < len(dest); i += 30 {
		j := min(i+30, len(dest))
		n := enc.EncodedLen(j - i)
		enc.Encode(buf[:n], []byte(dest[i:j]))
		b.Write(buf[:n])
	}
	b.WriteString(pb.headers)
	b.WriteString(pb.cred)
	b.WriteString(tok)
	b.WriteString(pb.tail)
	return b.String()
}

// peerIP returns the IP of the immediate TCP peer (r.RemoteAddr), or nil.
func peerIP(r *http.Request) net.IP {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

// trustedProxyNets holds the operator-configured reverse-proxy CIDRs
// (RIVULET_TRUSTED_PROXIES) whose forwarded headers are honoured.
var trustedProxyNets atomic.Pointer[[]*net.IPNet]

// SetTrustedProxies installs the CIDRs of reverse proxies whose
// X-Forwarded-* headers are trusted in addition to loopback.
func SetTrustedProxies(nets []*net.IPNet) { trustedProxyNets.Store(&nets) }

// isTrustedProxy reports whether ip is a loopback address or falls inside a
// configured trusted-proxy CIDR. Private/LAN peers are NOT implicitly trusted:
// any LAN host could otherwise spoof X-Forwarded-For to bypass the IP ACL.
func isTrustedProxy(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if p := trustedProxyNets.Load(); p != nil {
		for _, n := range *p {
			if n.Contains(ip) {
				return true
			}
		}
	}
	return false
}

// clientIP returns the effective client IP.
// X-Forwarded-For is honoured only when the immediate peer (RemoteAddr) is a
// trusted proxy (loopback or a RIVULET_TRUSTED_PROXIES CIDR; see
// isTrustedProxy). Other clients cannot spoof XFF to bypass IP-ACL checks.
//
// The chain is walked from the right (the hop appended by the nearest proxy)
// and the first address that is not itself a trusted proxy is returned; the
// left-most entries are client-controlled and are never preferred over a
// proxy-appended one. When every hop is a trusted (private) address the
// left-most entry is used (a LAN client behind a local proxy). An
// unparseable hop aborts the walk and the immediate peer is returned.
func clientIP(r *http.Request) net.IP {
	peer := peerIP(r)
	if !isTrustedProxy(peer) {
		return peer
	}
	vals := r.Header.Values("X-Forwarded-For")
	if len(vals) == 0 {
		return peer
	}
	hops := strings.Split(strings.Join(vals, ","), ",")
	var leftmost net.IP
	for i := len(hops) - 1; i >= 0; i-- {
		ip := net.ParseIP(strings.TrimSpace(hops[i]))
		if ip == nil {
			return peer
		}
		if !isTrustedProxy(ip) {
			return ip
		}
		leftmost = ip
	}
	if leftmost != nil {
		return leftmost
	}
	return peer
}

// resolveURL resolves ref against base via net/url.ResolveReference.
func resolveURL(base, ref string) string {
	b, err := url.Parse(base)
	if err != nil {
		return ref
	}
	rv, err := url.Parse(ref)
	if err != nil {
		return ref
	}
	return b.ResolveReference(rv).String()
}

// tryBase64Decode tries base64url then base64std decoding.
func tryBase64Decode(s string) (string, error) {
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return string(b), nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err == nil {
		return string(b), nil
	}
	return "", err
}

// writeAuthError maps authorize errors to HTTP status codes.
func writeAuthError(w http.ResponseWriter, err error) {
	if errors.Is(err, errForbidden) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

// allowedUpstreamHeaders is the forwarding allowlist for upstream response headers.
var allowedUpstreamHeaders = []string{
	"Content-Type",
	"Content-Length",
	"Content-Range",
	"Accept-Ranges",
	"Last-Modified",
	"ETag",
}

// copyAllowedHeaders copies the allowlisted upstream response headers to w.
func copyAllowedHeaders(w http.ResponseWriter, hdr http.Header) {
	for _, k := range allowedUpstreamHeaders {
		if v := hdr.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
}

// setProxySecurityHeaders hardens proxied upstream content: nosniff stops
// browsers from MIME-sniffing attacker-controlled bytes into HTML/script, and
// a sandbox CSP strips script/origin privileges should the response be
// rendered as a document. Set after applyRespHeaders so r_ overrides cannot
// weaken them.
func setProxySecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox")
}

// BlockedRespHeader reports whether a client-injected response header (r_ /
// r=) must be refused: cookie/redirect/refresh/CSP/CORS headers would let a
// crafted proxy URL plant cookies on this origin, navigate the browser, or
// weaken the sandbox.
func BlockedRespHeader(name string) bool {
	k := strings.ToLower(strings.TrimSpace(name))
	switch k {
	case "set-cookie", "set-cookie2", "refresh", "location", "content-security-policy",
		"content-security-policy-report-only", "x-content-type-options":
		return true
	}
	return strings.HasPrefix(k, "access-control-")
}

// BlockedReqHeader reports whether a client-injected upstream request header
// (h_ / h=) must be dropped: hop-by-hop and routing headers, and headers that
// identify or authenticate against this server (X-Rivulet-*, forwarded-for).
func BlockedReqHeader(name string) bool {
	k := strings.ToLower(strings.TrimSpace(name))
	switch k {
	case "host", "connection", "keep-alive", "proxy-authorization", "proxy-authenticate",
		"proxy-connection", "te", "trailer", "transfer-encoding", "upgrade", "content-length",
		"forwarded", "x-real-ip":
		return true
	}
	return strings.HasPrefix(k, "x-rivulet-") || strings.HasPrefix(k, "x-forwarded-")
}

// applyRespHeaders writes RespHeaders overrides to w, skipping blocked keys.
func applyRespHeaders(w http.ResponseWriter, rh http.Header) {
	for k, vs := range rh {
		if BlockedRespHeader(k) {
			continue
		}
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
}

// copyBufPool holds pooled 256 KB buffers for the passthrough streaming copy.
var copyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 256*1024)
		return &b
	},
}

// serveStream handles GET /proxy/stream — generic stream proxy with optional decryption.
func (h *Handler) serveStream(w http.ResponseWriter, r *http.Request) {
	if err := h.authorize(r); err != nil {
		writeAuthError(w, err)
		return
	}

	opts, err := h.parseOptions(r)
	if err != nil || opts.Dest == "" {
		http.Error(w, "missing or invalid destination URL", http.StatusBadRequest)
		return
	}

	if err := h.ValidateDest(opts.Dest); err != nil {
		http.Error(w, "forbidden destination", http.StatusForbidden)
		return
	}

	// SSRF guard for the client-supplied "proxy" query param (as opposed to
	// ValidateDest, which guards opts.Dest above). Rejects outright rather
	// than silently falling back to the base client, which would mask the
	// probe while still fetching opts.Dest.
	if h.rejectBlockedProxyHost(w, opts.Proxy) {
		return
	}

	// An embed page matching a definition is resolved to its media first.
	if h.resolveEmbed(w, r, opts, "/proxy/stream") {
		return
	}

	params, _ := h.parseDecryptParams(r)

	// Build upstream request headers from opts. Range is forwarded only on the
	// passthrough path; the decrypt path always issues a full GET without Range.
	upHdr := make(http.Header)
	for k, vs := range opts.ReqHeaders {
		if BlockedReqHeader(k) {
			continue
		}
		upHdr[k] = append([]string(nil), vs...)
	}

	ctx := r.Context()

	// Effective upstream proxy: per-request override takes priority over global config.
	effProxy := opts.Proxy
	if effProxy == "" {
		effProxy = h.cfg.UpstreamProxy
	}

	// Decrypt path: fetch the full segment without a Range header, decrypt in
	// memory, and respond 200 with the complete plaintext body.
	if params.Method != "" && len(params.Key) > 0 {
		if segmentDecryptor != nil {
			resp, fetchErr := h.fetch(ctx, http.MethodGet, opts.Dest, upHdr, nil, effProxy)
			if fetchErr != nil {
				http.Error(w, "upstream error", http.StatusBadGateway)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			// An advertised oversize body is refused before reading a byte.
			// Otherwise the buffer is reserved in the in-flight budget
			// (waiting a bounded time) so concurrent decrypts cannot pile up
			// beyond it.
			if resp.ContentLength > maxSegmentBytes {
				http.Error(w, "upstream segment too large", http.StatusBadGateway)
				return
			}
			held := segmentReservation(resp.ContentLength)
			waitCtx, cancelWait := context.WithTimeout(ctx, inflightWaitTimeout)
			acqErr := h.inflight.acquire(waitCtx, held)
			cancelWait()
			if acqErr != nil {
				http.Error(w, "proxy busy", http.StatusServiceUnavailable)
				return
			}
			defer func() { h.inflight.release(held) }()
			raw, readErr := readSegment(resp.Body, resp.ContentLength)
			if errors.Is(readErr, errSegmentTooLarge) {
				http.Error(w, "upstream segment too large", http.StatusBadGateway)
				return
			}
			if readErr != nil {
				http.Error(w, "upstream read error", http.StatusBadGateway)
				return
			}
			// An unknown length reserved the worst case; keep only what was read.
			if extra := held - int64(len(raw)); extra > 0 {
				h.inflight.release(extra)
				held -= extra
			}
			// raw is exclusively ours: the decryptor works on it in place
			// instead of allocating a second full-size copy.
			decrypted, decErr := segmentDecryptor(h, params, raw)
			if decErr != nil {
				http.Error(w, "decryption error", http.StatusBadGateway)
				return
			}
			if ct := resp.Header.Get("Content-Type"); ct != "" {
				w.Header().Set("Content-Type", ct)
			}
			applyRespHeaders(w, opts.RespHeaders)
			setProxySecurityHeaders(w)
			w.Header().Set("Content-Length", strconv.Itoa(len(decrypted)))
			w.WriteHeader(http.StatusOK)
			if r.Method != http.MethodHead {
				// The reservation stays held until the plaintext has been
				// written (the buffer is live until then), so the write is
				// bounded: a client that stops reading is dropped after
				// writeIdleTimeout instead of pinning up to maxSegmentBytes of
				// the in-flight budget for as long as it keeps the connection.
				_ = writeSegment(w, decrypted, h.writeIdleTimeout())
			}
			return
		}
		// segmentDecryptor nil — fall through to passthrough.
	}

	// Passthrough path: forward the client Range header upstream.
	if rng := r.Header.Get("Range"); rng != "" {
		upHdr.Set("Range", rng)
	}

	// Non-Range GET: use the segment cache when configured. Only responses
	// with a known, small Content-Length are buffered and cached; anything
	// else (large or chunked bodies, non-200 statuses) is streamed through.
	useCache := r.Method == http.MethodGet && r.Header.Get("Range") == "" &&
		h.cfg.SegCacheTTL > 0 && h.cache != nil
	if useCache {
		// A prefetch for this very segment may already be in flight; wait for
		// it (bounded by the request context) instead of fetching twice.
		h.awaitFlight(ctx, cacheKey(opts.Dest, upHdr))
		if data, respHdr, status, ok := h.cacheLookup(opts.Dest, upHdr); ok {
			copyAllowedHeaders(w, respHdr)
			applyRespHeaders(w, opts.RespHeaders)
			setProxySecurityHeaders(w)
			w.WriteHeader(status)
			_, _ = w.Write(data)
			return
		}
	}

	resp, fetchErr := h.fetch(ctx, r.Method, opts.Dest, upHdr, nil, effProxy)
	if fetchErr != nil {
		http.Error(w, "upstream error", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Buffer only what the cache will actually keep (200, known length, within
	// both the segment ceiling and the cache budget) and only while the
	// in-flight budget has room; otherwise stream straight through unbuffered.
	if useCache && cacheableResponse(resp) && resp.ContentLength <= h.cache.maxBytes {
		if n := resp.ContentLength; h.inflight.tryAcquire(n) {
			held := n
			defer func() { h.inflight.release(held) }()
			data, readErr := readSegment(resp.Body, n)
			if readErr != nil {
				http.Error(w, "upstream read error", http.StatusBadGateway)
				return
			}
			h.cache.putFull(cacheKey(opts.Dest, upHdr), data, resp.Header.Clone(), resp.StatusCode)
			// The segment cache now owns data and counts it in its own byte
			// budget, so the in-flight reservation is returned before the
			// response write: a slow client must not hold the same bytes in
			// both budgets (and starve other fills) while it drains them.
			h.inflight.release(held)
			held = 0
			copyAllowedHeaders(w, resp.Header)
			applyRespHeaders(w, opts.RespHeaders)
			setProxySecurityHeaders(w)
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(data)
			return
		}
		logging.For("streamproxy").Debug("inflight segment budget busy; streaming uncached", "bytes", resp.ContentLength)
	}

	// Direct streaming path (Range requests, HEAD, caching off, or a response
	// that is unknown-length, too large, or not cacheable).
	copyAllowedHeaders(w, resp.Header)
	applyRespHeaders(w, opts.RespHeaders)
	setProxySecurityHeaders(w)
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		bufp := copyBufPool.Get().(*[]byte)
		_, _ = io.CopyBuffer(w, resp.Body, *bufp)
		copyBufPool.Put(bufp)
	}
}

// ---------------------------------------------------------------------------
// SSRF guard helpers
// ---------------------------------------------------------------------------

// SSRF guard: private-range and cloud-metadata detection is delegated to
// netguard.IsPrivate / netguard.IsCloudMetadata (SEC-5) so this package
// cannot drift out of sync with the shared, tested definitions used by
// every other outbound-fetch path (archive, nzb, ftpstream).

// ValidateDest is an SSRF guard for proxy destination URLs.
// It rejects non-http(s) schemes, resolves the hostname, and blocks:
//   - 169.254.169.254 (cloud-metadata endpoint) — always.
//   - Loopback, RFC 1918, link-local, and ULA addresses — when the proxy has
//     a password or IP ACL, or RIVULET_PROXY_ALLOW_PRIVATE is unset, unless
//     the address is on cfg.PrivateAllow (RIVULET_PROXY_PRIVATE_ALLOW).
//
// Returns nil when the destination is permitted.
func (h *Handler) ValidateDest(rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("disallowed scheme %q: only http and https are permitted", u.Scheme)
	}

	host := u.Hostname()

	// Collect IPs to check: direct parse for numeric hosts, DNS lookup otherwise.
	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		addrs, lookupErr := net.LookupHost(host)
		if lookupErr != nil {
			return fmt.Errorf("cannot resolve %q: %w", host, lookupErr)
		}
		for _, a := range addrs {
			if ip := net.ParseIP(a); ip != nil {
				ips = append(ips, ip)
			}
		}
	}

	// Private ranges are blocked when the proxy is exposed (password / IP ACL)
	// or RIVULET_PROXY_ALLOW_PRIVATE is unset, except for addresses on the
	// RIVULET_PROXY_PRIVATE_ALLOW allowlist.
	protected := h.proxyBlockPrivate()

	for _, ip := range ips {
		if netguard.IsCloudMetadata(ip) {
			return fmt.Errorf("destination resolves to disallowed cloud-metadata address %s", ip)
		}
		if protected && netguard.IsPrivate(ip) && !h.cfg.PrivateAllow.Contains(ip) {
			return fmt.Errorf("destination resolves to private address %s (proxy is protected; see RIVULET_PROXY_PRIVATE_ALLOW)", ip)
		}
	}
	return nil
}

// Protected reports whether any proxy auth mechanism (password, IP ACL or
// signing secret) is configured, i.e. the proxy is exposed to untrusted clients.
func (h *Handler) Protected() bool {
	return h.cfg.Password != "" || len(h.cfg.IPACL) > 0 || len(h.cfg.Secret) > 0
}

// Authorize is the exported wrapper over the internal authorize method.
// It is consumed by the api package to gate requests before routing.
// Returns the same sentinel errors as authorize (errUnauthorized, errForbidden).
func (h *Handler) Authorize(r *http.Request) error {
	return h.authorize(r)
}

// ---------------------------------------------------------------------------
// Upstream proxy host validation (SSRF guard for the client "proxy" param)
// ---------------------------------------------------------------------------

// proxyBlockPrivate reports whether this handler must block private/loopback
// destinations for outbound requests made on a client's behalf: either the
// operator left RIVULET_PROXY_ALLOW_PRIVATE unset (cfg.BlockPrivate) or the
// proxy is exposed to untrusted clients (a password or IP ACL is set).
//
// The signing secret deliberately does not count: it is auto-generated on
// every install, so counting it made RIVULET_PROXY_ALLOW_PRIVATE a no-op.
// Signed URLs are a capability mechanism, not a sign of exposure. Mirrors
// the "protected" signal in ValidateDest exactly, so a proxy URL and a
// destination URL are held to the same trust boundary. Addresses on
// cfg.PrivateAllow stay reachable either way.
func (h *Handler) proxyBlockPrivate() bool {
	return h.cfg.BlockPrivate || h.cfg.Password != "" || len(h.cfg.IPACL) > 0
}

// validateProxyHost is an SSRF guard for the client-supplied "proxy" query
// parameter, as opposed to ValidateDest which guards the destination URL.
// The scheme is assumed already restricted to socks5/socks5h/http/https by
// the caller; this resolves the proxy URL's host and rejects it unless every
// resolved IP passes netguard.ValidateIP under the same blockPrivate policy
// as ValidateDest.
func (h *Handler) validateProxyHost(rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return fmt.Errorf("invalid proxy URL: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("proxy URL has no host")
	}

	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		addrs, lookupErr := net.LookupHost(host)
		if lookupErr != nil {
			return fmt.Errorf("cannot resolve proxy host %q: %w", host, lookupErr)
		}
		for _, a := range addrs {
			if ip := net.ParseIP(a); ip != nil {
				ips = append(ips, ip)
			}
		}
	}
	if len(ips) == 0 {
		return fmt.Errorf("proxy host %q did not resolve to any usable IP", host)
	}

	blockPrivate := h.proxyBlockPrivate()
	for _, ip := range ips {
		if verr := netguard.ValidateIPAllow(ip, blockPrivate, h.cfg.PrivateAllow); verr != nil {
			return fmt.Errorf("proxy host %q: %w", host, verr)
		}
	}
	return nil
}

// rejectBlockedProxyHost validates a client-supplied "proxy" query value and,
// if it resolves to a disallowed host, writes a 403 response and logs once.
// Returns true when the request was rejected — the caller must return
// immediately. An empty proxyURL (no client override present) is always
// allowed; cfg.UpstreamProxy is admin-configured and not client-controlled,
// so it is never passed through this guard.
func (h *Handler) rejectBlockedProxyHost(w http.ResponseWriter, proxyURL string) bool {
	if proxyURL == "" {
		return false
	}
	if err := h.validateProxyHost(proxyURL); err != nil {
		logging.For("streamproxy").Warn("blocked proxy host", "proxy_url", proxyURL, "err", err)
		http.Error(w, "forbidden proxy", http.StatusForbidden)
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Upstream proxy client management
// ---------------------------------------------------------------------------

// clientFor returns an *http.Client whose transport routes outbound connections
// through proxyURL. When proxyURL is "" the configured base client is returned
// directly. Built clients are cached in h.proxyClients; on build error the base
// client is returned and the error is logged once.
func (h *Handler) clientFor(proxyURL string) *http.Client {
	base := h.cfg.Client
	if base == nil {
		base = http.DefaultClient
	}
	if proxyURL == "" {
		return base
	}
	blockPrivate := h.proxyBlockPrivate()
	key := proxyClientKey(proxyURL, blockPrivate)
	h.proxyMu.Lock()
	defer h.proxyMu.Unlock()
	if e, ok := h.proxyClients[key]; ok {
		if time.Now().Before(e.expiresAt) {
			return e.client
		}
		e.client.CloseIdleConnections()
		delete(h.proxyClients, key)
	}
	var c *http.Client
	var err error
	if h.cfg.UpstreamProxy != "" && proxyURL == h.cfg.UpstreamProxy {
		// Operator-configured upstream: the TCP hop to this exact host:port is
		// exempt from the private-range guard (it is typically a loopback ssh
		// -D tunnel or LAN proxy). Destinations are still checked by
		// ValidateDest before every fetch.
		c, err = buildProxyClientControl(proxyURL, upstreamHopControl(proxyURL))
	} else {
		c, err = buildProxyClient(proxyURL, blockPrivate, h.cfg.PrivateAllow)
	}
	if err != nil {
		logging.For("streamproxy").Warn("cannot build proxy client; using default", "proxy_url", proxyURL, "err", err)
		return base
	}
	h.proxyClients[key] = proxyClientEntry{client: c, expiresAt: time.Now().Add(proxyClientTTL)}
	if len(h.proxyClients) > proxyClientMaxEntries {
		h.sweepProxyClients()
	}
	return c
}

// proxyClientTTL is how long a built upstream proxy client is cached per
// proxy URL before it is rebuilt.
const proxyClientTTL = 5 * time.Minute

// proxyClientMaxEntries is the hard cap on the number of distinct proxy URL
// keys held in proxyClients. Each client-supplied "proxy" query value gets
// its own entry, so without a cap this map (and its transports' connection
// pools) would grow unboundedly.
const proxyClientMaxEntries = 16

// sweepProxyClients removes expired entries from proxyClients and, when the
// size still exceeds proxyClientMaxEntries after the TTL sweep, evicts the
// soonest-expiring entry until back under the limit. Evicted clients have
// their transport's idle connections closed so the pool is actually
// released, not just unreferenced. Must be called with h.proxyMu held.
func (h *Handler) sweepProxyClients() {
	now := time.Now()
	for k, e := range h.proxyClients {
		if now.After(e.expiresAt) {
			e.client.CloseIdleConnections()
			delete(h.proxyClients, k)
		}
	}
	// Hard size cap: evict soonest-expiring entry until under limit.
	for len(h.proxyClients) > proxyClientMaxEntries {
		var evict string
		var evictEntry proxyClientEntry
		found := false
		for k, e := range h.proxyClients {
			if !found || e.expiresAt.Before(evictEntry.expiresAt) {
				evict = k
				evictEntry = e
				found = true
			}
		}
		evictEntry.client.CloseIdleConnections()
		delete(h.proxyClients, evict)
	}
}

// proxyClientKey computes the cache key for h.proxyClients, folding in
// blockPrivate so a guarded and an unguarded client built for the same
// proxyURL can never be confused with one another. blockPrivate is fixed for
// a given Handler's lifetime (it derives from h.cfg), so this is defensive
// rather than load-bearing today — but it keeps the invariant explicit and
// keeps clientFor's cache correct if that ever changes.
func proxyClientKey(proxyURL string, blockPrivate bool) string {
	if blockPrivate {
		return "p:" + proxyURL
	}
	return "u:" + proxyURL
}

// buildProxyClient constructs an *http.Client whose transport routes through
// the given proxy URL (socks5/socks5h/http/https). blockPrivate mirrors the
// same "protected" signal as ValidateDest/proxyBlockPrivate: the dialer used
// to connect to the proxy host itself (not the ultimate destination, which
// the caller reaches through the proxy) is guarded with netguard.DialControl,
// closing the DNS-rebinding TOCTOU gap between validateProxyHost's pre-flight
// check and the actual dial.
func buildProxyClient(proxyURL string, blockPrivate bool, allow *netguard.Allow) (*http.Client, error) {
	return buildProxyClientControl(proxyURL, netguard.DialControlAllow(blockPrivate, allow))
}

// upstreamHopControl returns a dialer Control for the TCP hop to the
// operator-configured upstream proxy. It only accepts connections to the
// proxy's own port and still refuses cloud-metadata addresses; any other
// address (the dialer is only ever used for the proxy hop) is rejected by the
// full private-range guard.
func upstreamHopControl(proxyURL string) func(network, address string, c syscall.RawConn) error {
	wantPort := ""
	if u, err := url.Parse(proxyURL); err == nil {
		wantPort = u.Port()
		if wantPort == "" {
			switch u.Scheme {
			case "socks5", "socks5h":
				wantPort = "1080"
			case "https":
				wantPort = "443"
			default:
				wantPort = "80"
			}
		}
	}
	relaxed := netguard.DialControl(false)
	strict := netguard.DialControl(true)
	return func(network, address string, c syscall.RawConn) error {
		if _, port, err := net.SplitHostPort(address); err == nil && port == wantPort {
			return relaxed(network, address, c)
		}
		return strict(network, address, c)
	}
}

// buildProxyClientControl is buildProxyClient with an explicit dialer
// Control for the hop to the proxy host.
func buildProxyClientControl(proxyURL string, control func(network, address string, c syscall.RawConn) error) (*http.Client, error) {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("parse proxy URL: %w", err)
	}
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   control,
	}
	tr := &http.Transport{
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		IdleConnTimeout:       90 * time.Second,
	}
	switch u.Scheme {
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if u.User != nil {
			pw, _ := u.User.Password()
			auth = &proxy.Auth{User: u.User.Username(), Password: pw}
		}
		socksDialer, err := proxy.SOCKS5("tcp", u.Host, auth, dialer)
		if err != nil {
			return nil, fmt.Errorf("SOCKS5 dialer: %w", err)
		}
		cd, ok := socksDialer.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("SOCKS5 dialer does not implement ContextDialer")
		}
		tr.DialContext = cd.DialContext
	case "http", "https":
		tr.Proxy = http.ProxyURL(u)
		tr.DialContext = dialer.DialContext
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	return &http.Client{Transport: tr}, nil
}

// ---------------------------------------------------------------------------
// /proxy/ip — egress IP discovery
// ---------------------------------------------------------------------------

// ipCacheTTL is how long a resolved egress IP is cached per effective proxy.
const ipCacheTTL = 5 * time.Minute

// ipCacheMaxEntries is the hard cap on the number of distinct proxy keys
// held in ipCache. Each client-supplied "proxy" query value gets its own
// entry, so without a cap this map would grow unboundedly.
const ipCacheMaxEntries = 16

// ipServices is an ordered list of plain-text / JSON IP-echo endpoints.
var ipServices = []string{
	"https://api.ipify.org",
	"https://checkip.amazonaws.com",
}

// serveIP handles GET /proxy/ip — returns the public egress IP as JSON.
// The effective proxy (opts.Proxy or cfg.UpstreamProxy) is used to fetch.
func (h *Handler) serveIP(w http.ResponseWriter, r *http.Request) {
	if err := h.authorize(r); err != nil {
		writeAuthError(w, err)
		return
	}

	// Determine effective proxy from the request query (same validation as
	// parseOptions), then apply the same host-level SSRF guard used by
	// serveStream/hlsServe/dashServe before it is ever dialed.
	effProxy := ""
	if raw := r.URL.Query().Get("proxy"); raw != "" {
		if u, err := url.Parse(raw); err == nil {
			switch u.Scheme {
			case "socks5", "socks5h", "http", "https":
				effProxy = raw
			}
		}
	}
	if h.rejectBlockedProxyHost(w, effProxy) {
		return
	}
	if effProxy == "" {
		effProxy = h.cfg.UpstreamProxy
	}

	// Serve from cache when still fresh.
	h.ipMu.Lock()
	if e, ok := h.ipCache[effProxy]; ok && time.Now().Before(e.expiresAt) {
		h.ipMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ip": e.ip})
		return
	}
	h.ipMu.Unlock()

	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	client := h.clientFor(effProxy)

	var ipStr string
	for _, svcURL := range ipServices {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, svcURL, nil)
		if reqErr != nil {
			continue
		}
		resp, doErr := client.Do(req)
		if doErr != nil {
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64))
		resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		candidate := strings.TrimSpace(string(body))
		// Try JSON {"ip":"..."} before treating the body as plain text.
		var j struct {
			IP string `json:"ip"`
		}
		if json.Unmarshal(body, &j) == nil && j.IP != "" {
			candidate = j.IP
		}
		if ip := net.ParseIP(candidate); ip != nil {
			ipStr = ip.String()
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if ipStr == "" {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "could not determine egress IP"})
		return
	}

	// Refresh the cache. Distinct proxy URLs are effectively unbounded (each
	// client-supplied "proxy" query value gets its own entry), so this map
	// can grow without limit if left untended (same rationale as
	// proxyClients above). Insert first, then sweep: a TTL-only pass removes
	// expired entries, and — mirroring media.hlsManager's sweepProbeCache —
	// a hard-cap loop evicts the soonest-expiring entry until back at
	// ipCacheMaxEntries, so the map is always bounded regardless of how many
	// distinct proxy values a client bursts through.
	h.ipMu.Lock()
	h.ipCache[effProxy] = ipCacheEntry{ip: ipStr, expiresAt: time.Now().Add(ipCacheTTL)}
	if len(h.ipCache) > ipCacheMaxEntries {
		h.sweepIPCache()
	}
	h.ipMu.Unlock()

	_ = json.NewEncoder(w).Encode(map[string]string{"ip": ipStr})
}

// sweepIPCache removes expired entries from the egress-IP cache and, when
// the size still exceeds ipCacheMaxEntries after the TTL sweep, evicts the
// soonest-expiring entry until back under the limit. Must be called with
// h.ipMu held.
func (h *Handler) sweepIPCache() {
	now := time.Now()
	for k, e := range h.ipCache {
		if now.After(e.expiresAt) {
			delete(h.ipCache, k)
		}
	}
	// Hard size cap: evict soonest-expiring entry until under limit.
	for len(h.ipCache) > ipCacheMaxEntries {
		var evict string
		var evictExp time.Time
		found := false
		for k, e := range h.ipCache {
			if !found || e.expiresAt.Before(evictExp) {
				evict = k
				evictExp = e.expiresAt
				found = true
			}
		}
		delete(h.ipCache, evict)
	}
}
