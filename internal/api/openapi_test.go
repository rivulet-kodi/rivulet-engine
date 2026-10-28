// SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
//
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type openapiDoc struct {
	OpenAPI string `json:"openapi"`
	Info    struct {
		Title   string `json:"title"`
		Version string `json:"version"`
	} `json:"info"`
	Paths      map[string]map[string]json.RawMessage `json:"paths"`
	Components struct {
		Schemas   map[string]json.RawMessage `json:"schemas"`
		Responses map[string]json.RawMessage `json:"responses"`
	} `json:"components"`
}

var openapiMethods = map[string]bool{"get": true, "put": true, "post": true, "delete": true, "patch": true, "head": true, "options": true, "trace": true}

func loadOpenAPI(t *testing.T) openapiDoc {
	t.Helper()
	var doc openapiDoc
	if err := json.Unmarshal(openapiSpec, &doc); err != nil {
		t.Fatalf("openapi.json: %v", err)
	}
	return doc
}

func serveGET(t *testing.T, path string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	newHandler(t).ServeHTTP(rec, req)
	return rec
}

// collectRefs walks a decoded JSON value and appends every "$ref" string.
func collectRefs(v any, out *[]string) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if s, ok := e.(string); ok && k == "$ref" {
				*out = append(*out, s)
				continue
			}
			collectRefs(e, out)
		}
	case []any:
		for _, e := range x {
			collectRefs(e, out)
		}
	}
}

// TestOpenAPISpecWellFormed checks what the fuzzer cannot: structure the
// document needs to be usable at all (version, unique operation ids, resolvable
// $refs, a response set per operation). Full schema validity is checked by
// scripts/api-fuzz.sh, which loads the spec into Schemathesis.
func TestOpenAPISpecWellFormed(t *testing.T) {
	doc := loadOpenAPI(t)
	if doc.OpenAPI != "3.1.1" {
		t.Errorf("openapi = %q, want 3.1.1", doc.OpenAPI)
	}
	if doc.Info.Title == "" || doc.Info.Version == "" {
		t.Errorf("info.title/version missing: %+v", doc.Info)
	}
	if len(doc.Paths) == 0 {
		t.Fatal("no paths")
	}

	var whole any
	if err := json.Unmarshal(openapiSpec, &whole); err != nil {
		t.Fatal(err)
	}
	var refs []string
	collectRefs(whole, &refs)
	for _, ref := range refs {
		switch {
		case strings.HasPrefix(ref, "#/components/schemas/"):
			if _, ok := doc.Components.Schemas[strings.TrimPrefix(ref, "#/components/schemas/")]; !ok {
				t.Errorf("dangling $ref %s", ref)
			}
		case strings.HasPrefix(ref, "#/components/responses/"):
			if _, ok := doc.Components.Responses[strings.TrimPrefix(ref, "#/components/responses/")]; !ok {
				t.Errorf("dangling $ref %s", ref)
			}
		default:
			t.Errorf("unsupported $ref %s", ref)
		}
	}

	ids := map[string]string{}
	for p, item := range doc.Paths {
		for m, raw := range item {
			if !openapiMethods[m] {
				continue
			}
			var op struct {
				OperationID string                     `json:"operationId"`
				Summary     string                     `json:"summary"`
				Responses   map[string]json.RawMessage `json:"responses"`
			}
			if err := json.Unmarshal(raw, &op); err != nil {
				t.Errorf("%s %s: %v", m, p, err)
				continue
			}
			label := strings.ToUpper(m) + " " + p
			if op.OperationID == "" || op.Summary == "" || len(op.Responses) == 0 {
				t.Errorf("%s: operationId, summary and responses are required", label)
			}
			if prev, dup := ids[op.OperationID]; dup {
				t.Errorf("%s: operationId %q also used by %s", label, op.OperationID, prev)
			}
			ids[op.OperationID] = label
		}
	}
}

// routeFirstSegments returns the string literals of the `switch first` cases in
// server.route(), i.e. every top-level path segment the dispatcher serves.
func routeFirstSegments(t *testing.T) map[string]bool {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "api.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	segs := map[string]bool{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "route" || fn.Recv == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			if id, ok := sw.Tag.(*ast.Ident); !ok || id.Name != "first" {
				return true
			}
			for _, c := range sw.Body.List {
				for _, e := range c.(*ast.CaseClause).List {
					if lit, ok := e.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						s, err := strconv.Unquote(lit.Value)
						if err != nil {
							t.Fatal(err)
						}
						segs[s] = true
					}
				}
			}
			return false
		})
	}
	if len(segs) == 0 {
		t.Fatal("found no `switch first` in route(): update routeFirstSegments")
	}
	return segs
}

// TestOpenAPISpecMatchesRoutes fails when route() serves a top-level path
// segment the spec does not document, or the spec documents one route() does
// not dispatch. Sub-routes live inside the handlers; they are exercised by the
// fuzzer (scripts/api-fuzz.sh), which flags undocumented behavior.
func TestOpenAPISpecMatchesRoutes(t *testing.T) {
	doc := loadOpenAPI(t)

	specFirst := map[string]bool{}
	for p := range doc.Paths {
		seg := strings.SplitN(strings.TrimPrefix(p, "/"), "/", 2)[0]
		specFirst[seg] = true
	}

	// Segments route() handles through a parameter rather than a case label.
	// infoHash-scoped routes and subtitles.<ext> have templated spec paths, and
	// "/" (the landing redirect) is handled before the switch.
	templated := map[string]bool{"": true, "{infoHash}": true, "{archiveType}": true, "subtitles.{ext}": true}
	// Served but intentionally not described: the spec itself (the reference
	// page lives under the DVR-shared "api" segment, which the spec documents
	// through /api/recordings).
	notInSpec := map[string]bool{"openapi.json": true}

	routes := routeFirstSegments(t)

	var missing, stale []string
	for seg := range routes {
		if !specFirst[seg] && !notInSpec[seg] {
			missing = append(missing, seg)
		}
	}
	for seg := range specFirst {
		if !routes[seg] && !templated[seg] {
			stale = append(stale, seg)
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	if len(missing) > 0 {
		t.Errorf("route() serves top-level segments missing from openapi.json: %v", missing)
	}
	if len(stale) > 0 {
		t.Errorf("openapi.json documents top-level segments route() does not serve: %v", stale)
	}
}

func TestOpenAPISpecServed(t *testing.T) {
	rec := serveGET(t, "/openapi.json")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content type = %q", ct)
	}
	if !bytes.Equal(rec.Body.Bytes(), openapiSpec) {
		t.Error("served body differs from the embedded spec")
	}
	if !json.Valid(rec.Body.Bytes()) {
		t.Error("served spec is not valid JSON")
	}

	req := httptest.NewRequest(http.MethodPost, "/openapi.json", nil)
	post := httptest.NewRecorder()
	newHandler(t).ServeHTTP(post, req)
	if post.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST status = %d, want 405", post.Code)
	}
}

func TestAPIReferencePage(t *testing.T) {
	for _, p := range []string{"/api/reference", "/api/reference/"} {
		rec := serveGET(t, p)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d", p, rec.Code)
		}
		if !strings.Contains(rec.Header().Get("Content-Type"), "text/html") {
			t.Errorf("content type = %q", rec.Header().Get("Content-Type"))
		}

		csp := rec.Header().Get("Content-Security-Policy")
		for _, d := range []string{"default-src 'self'", "script-src 'self'", "connect-src 'self'", "frame-ancestors 'none'"} {
			if !strings.Contains(csp, d) {
				t.Errorf("CSP %q lacks %q", csp, d)
			}
		}
		for _, bad := range []string{"unsafe-eval", "http"} {
			if strings.Contains(csp, bad) {
				t.Errorf("CSP %q must not contain %q", csp, bad)
			}
		}

		body := rec.Body.String()
		for _, want := range []string{`src="/api/reference/assets/scalar-1.76.0.js"`, `src="/api/reference/assets/init.js"`} {
			if !strings.Contains(body, want) {
				t.Errorf("page lacks %s", want)
			}
		}
		// no inline script and nothing from another origin
		for _, bad := range []string{"<script>", "http://", "https://"} {
			if strings.Contains(body, bad) {
				t.Errorf("page must not contain %q", bad)
			}
		}
	}
}

func TestAPIReferenceAssets(t *testing.T) {
	initJS := serveGET(t, "/api/reference/assets/init.js")
	if initJS.Code != http.StatusOK {
		t.Fatalf("init.js status = %d", initJS.Code)
	}
	if !strings.Contains(initJS.Body.String(), "/openapi.json") {
		t.Error("init.js does not load /openapi.json")
	}

	gz := serveGET(t, "/api/reference/assets/scalar-1.76.0.js", "Accept-Encoding", "gzip")
	if gz.Code != http.StatusOK || gz.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("gzip bundle: status %d, encoding %q", gz.Code, gz.Header().Get("Content-Encoding"))
	}
	if !strings.Contains(gz.Header().Get("Cache-Control"), "immutable") {
		t.Errorf("bundle Cache-Control = %q", gz.Header().Get("Cache-Control"))
	}
	zr, err := gzip.NewReader(gz.Body)
	if err != nil {
		t.Fatal(err)
	}
	fromGzip, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(fromGzip), "createApiReference") {
		t.Error("bundle does not expose createApiReference")
	}

	plain := serveGET(t, "/api/reference/assets/scalar-1.76.0.js")
	if plain.Code != http.StatusOK || plain.Header().Get("Content-Encoding") != "" {
		t.Fatalf("plain bundle: status %d, encoding %q", plain.Code, plain.Header().Get("Content-Encoding"))
	}
	if plain.Body.String() != string(fromGzip) {
		t.Error("identity response differs from the decompressed gzip response")
	}
}

// TestAPIReferenceRouting: the reference must not shadow or be shadowed by the
// DVR /api/recordings routes, and unknown names under it are plain 404s.
func TestAPIReferenceRouting(t *testing.T) {
	if rec := serveGET(t, "/api/reference/assets/nope.js"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown asset = %d, want 404", rec.Code)
	}
	if rec := serveGET(t, "/api/referencex"); rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "<html") {
		t.Errorf("/api/referencex = %d, want 404 without the page", rec.Code)
	}
	// DVR is disabled in the default test config: its own handler answers, not the page.
	rec := serveGET(t, "/api/recordings")
	if strings.Contains(rec.Body.String(), "<html") {
		t.Errorf("/api/recordings served the reference page")
	}

	req := httptest.NewRequest(http.MethodPost, "/api/reference", nil)
	post := httptest.NewRecorder()
	newHandler(t).ServeHTTP(post, req)
	if post.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/reference = %d, want 405", post.Code)
	}
}

// TestAPIReferenceForeignOrigin: the page and spec sit behind the same Origin
// allowlist as every other route.
func TestAPIReferenceForeignOrigin(t *testing.T) {
	for _, p := range []string{"/openapi.json", "/api/reference", "/api/reference/assets/init.js"} {
		if rec := serveGET(t, p, "Origin", "https://evil.example"); rec.Code != http.StatusForbidden {
			t.Errorf("%s with a foreign Origin = %d, want 403", p, rec.Code)
		}
	}
}
