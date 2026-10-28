// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"io"
	"net/http"
	"strings"
)

// openapiSpec is the hand-maintained OpenAPI 3.1 description of the enginefs
// API, served at GET /openapi.json. TestOpenAPISpecMatchesRoutes keeps it in
// step with route(); scripts/api-fuzz.sh checks it against the running server.
//
//go:embed openapi.json
var openapiSpec []byte

// The Scalar API reference is served from the binary: no CDN, so it works
// offline and under a strict CSP. scalarBundle is the standalone build of
// @scalar/api-reference (MIT), vendored gzip-compressed by
// scripts/update-scalar.sh; its pinned version is part of the file name.
const scalarBundleName = "scalar-1.76.0.js"

//go:embed apiref/scalar-1.76.0.js.gz
var scalarBundle []byte

//go:embed apiref/init.js
var scalarInit []byte

const (
	openapiPath      = "/openapi.json"
	apiReferencePath = "/api/reference"
	scalarAssetDir   = apiReferencePath + "/assets/"

	// apiReferenceCSP lets the page run only same-origin scripts (Scalar
	// injects its stylesheet at runtime, hence the inline styles) and talk
	// only to its own origin.
	apiReferenceCSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
		"img-src 'self' data:; font-src 'self' data:; connect-src 'self'; " +
		"object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

	apiReferenceHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>rivulet-engine API reference</title>
<link rel="icon" href="data:,">
</head>
<body>
<div id="app"></div>
<script src="` + scalarAssetDir + scalarBundleName + `"></script>
<script src="` + scalarAssetDir + `init.js"></script>
</body>
</html>
`
)

// handleOpenAPI serves GET /openapi.json.
func (s *server) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", "application/json")
	hd.Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(openapiSpec)
}

// handleAPIReference serves the Scalar API reference page and its assets under
// /api/reference. path is the request path without the leading slash. It
// returns false when path is not one of them, so the DVR /api/recordings
// routes keep their own handler.
func (s *server) handleAPIReference(w http.ResponseWriter, r *http.Request, path string) bool {
	rest := strings.TrimPrefix(path, "api/reference")
	if rest == path || (rest != "" && rest[0] != '/') {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	hd := w.Header()
	switch rest {
	case "", "/":
		hd.Set("Content-Type", "text/html; charset=utf-8")
		hd.Set("Content-Security-Policy", apiReferenceCSP)
		hd.Set("Cache-Control", "no-cache")
		hd.Set("X-Content-Type-Options", "nosniff")
		_, _ = io.WriteString(w, apiReferenceHTML)
	case "/assets/init.js":
		hd.Set("Content-Type", "text/javascript; charset=utf-8")
		hd.Set("Cache-Control", "no-cache")
		hd.Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(scalarInit)
	case "/assets/" + scalarBundleName:
		serveScalarBundle(w, r)
	default:
		http.NotFound(w, r)
	}
	return true
}

// serveScalarBundle serves the vendored bundle. The file name carries the
// pinned version, so the response is immutable. It is stored gzip-compressed
// and sent as is to clients that accept gzip.
func serveScalarBundle(w http.ResponseWriter, r *http.Request) {
	hd := w.Header()
	hd.Set("Content-Type", "text/javascript; charset=utf-8")
	hd.Set("Cache-Control", "public, max-age=31536000, immutable")
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Add("Vary", "Accept-Encoding")

	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		hd.Set("Content-Encoding", "gzip")
		_, _ = w.Write(scalarBundle)
		return
	}

	zr, err := gzip.NewReader(bytes.NewReader(scalarBundle))
	if err != nil {
		http.Error(w, "failed to read the API reference bundle", http.StatusInternalServerError)
		return
	}
	defer func() { _ = zr.Close() }()

	_, _ = io.Copy(w, zr) //nolint:gosec // embedded, trusted asset
}
