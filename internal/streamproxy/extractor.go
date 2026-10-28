// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package streamproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/rivulet-kodi/rivulet-engine/internal/logging"
)

// MediaFlow-compatible /extractor/video endpoint: resolve a hoster page
// (host=…, d=<page url>) into a playable URL and either redirect to the
// matching /proxy/* endpoint (redirect_stream=true) or return MediaFlow's
// JSON description of it (redirect_stream=false).

// maxExtractorPage caps how much of an upstream hoster page is read.
const maxExtractorPage = 4 << 20

// extractorUA is sent to hosters; several refuse non-browser user agents.
const extractorUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

// errExtract marks a page that was fetched but could not be resolved.
var errExtract = errors.New("extraction failed")

// extractResult is a resolved stream plus the headers needed to fetch it.
type extractResult struct {
	URL      string
	Headers  map[string]string
	Endpoint string // /proxy/stream or /proxy/hls/manifest.m3u8
}

// pageFetcher fetches a page (GET, or POST with body) with the given headers
// and returns its body.
type pageFetcher func(r *http.Request, method, rawurl string, hdr map[string]string, body []byte) (string, error)

// mediaflowEndpointName maps proxy paths to MediaFlow's endpoint identifiers.
var mediaflowEndpointName = map[string]string{
	"/proxy/stream":            "proxy_stream_endpoint",
	"/proxy/hls/manifest.m3u8": "hls_manifest_proxy",
	"/proxy/mpd/manifest.m3u8": "mpd_manifest_proxy",
}

// HandleExtractor handles GET /extractor/video[.m3u8|.mp4].
func (h *Handler) HandleExtractor(w http.ResponseWriter, r *http.Request, seg []string) {
	if len(seg) < 2 || strings.TrimSuffix(strings.TrimSuffix(seg[1], ".m3u8"), ".mp4") != "video" {
		http.NotFound(w, r)
		return
	}
	if err := h.authorize(r); err != nil {
		writeAuthError(w, err)
		return
	}
	q := r.URL.Query()
	hostName := strings.ToLower(strings.TrimSpace(q.Get("host")))
	var ex *Extractor
	ok := false
	if h.defs != nil {
		ex, ok = h.defs.lookup(hostName)
	}
	if !ok {
		writeExtractorError(w, http.StatusBadRequest, "unsupported host: "+q.Get("host"))
		return
	}
	page := decodeDest(q.Get("d"))
	if err := h.ValidateDest(page); err != nil {
		writeExtractorError(w, http.StatusBadRequest, err.Error())
		return
	}
	opts, err := h.parseOptions(r)
	if err != nil {
		writeExtractorError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), maxDefTimeout)
	defer cancel()
	res, err := ex.run(r.WithContext(ctx), page, h.extractorFetch(opts.Proxy))
	if err != nil {
		logging.For("extractor").Warn("extract failed", "host", hostName, "err", err)
		writeExtractorError(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := h.ValidateDest(res.URL); err != nil {
		writeExtractorError(w, http.StatusBadGateway, "resolved URL rejected: "+err.Error())
		return
	}
	for k, v := range res.Headers {
		opts.ReqHeaders.Set(k, v)
	}
	base := h.externalBase(r)
	if b, _ := strconv.ParseBool(q.Get("redirect_stream")); b {
		http.Redirect(w, r, h.buildProxyURL(base, res.Endpoint, res.URL, opts), http.StatusFound)
		return
	}
	qp := map[string]string{}
	if opts.APIPassword != "" {
		qp["api_password"] = opts.APIPassword
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"destination_url":     res.URL,
		"request_headers":     res.Headers,
		"mediaflow_endpoint":  mediaflowEndpointName[res.Endpoint],
		"mediaflow_proxy_url": base + res.Endpoint,
		"query_params":        qp,
	})
}

func writeExtractorError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"detail": msg})
}

// extractorFetch returns a pageFetcher using the SSRF-guarded proxy client.
func (h *Handler) extractorFetch(proxyURL string) pageFetcher {
	if proxyURL == "" {
		proxyURL = h.cfg.UpstreamProxy
	}
	return func(r *http.Request, method, rawurl string, hdr map[string]string, body []byte) (string, error) {
		if err := h.ValidateDest(rawurl); err != nil {
			return "", err
		}
		hh := http.Header{"User-Agent": {extractorUA}}
		for k, v := range hdr {
			hh.Set(k, v)
		}
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		resp, err := h.fetch(r.Context(), method, rawurl, hh, rd, proxyURL)
		if err != nil {
			return "", err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK && (method != http.MethodPost || resp.StatusCode/100 != 2) {
			return "", fmt.Errorf("%w: upstream status %d", errExtract, resp.StatusCode)
		}
		b, err := io.ReadAll(io.LimitReader(resp.Body, maxExtractorPage))
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
