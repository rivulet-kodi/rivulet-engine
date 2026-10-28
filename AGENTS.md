<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# Repository Guidelines

## Project Overview

`rivulet-engine` is a pure-Go, IPv6-capable drop-in replacement for Stremio's
closed-source streaming server (`server.js`), built on
[`anacrolix/torrent`](https://github.com/anacrolix/torrent). It serves the exact
**enginefs HTTP API** that `stremio-web`/`stremio-core` expect on `:11470`
(HTTPS `:12470`), so it can be pointed at instead of the official binary. It is a
localhost-trust service with **no authentication** by design — any process
that can reach the port is trusted. Browser-originated requests are an
exception: state-changing routes and `/proxy` also check the request's
`Origin` header against an allowlist (`RIVULET_ALLOWED_ORIGINS` + built-in
Stremio web/localhost/self-IP defaults, see `docs/CONFIGURATION.md`) so an arbitrary page open
in the user's browser cannot drive the API merely because the port is
reachable.

Response shapes (e.g. `stats.json`, `/settings`, `/create`) MUST stay
byte-compatible with the official `server.js` and the strict `stremio-core`
deserializers — treat `internal/types` as a wire contract, not just structs.

## Architecture & Data Flow

`cmd/rivulet-engine/main.go` reads env config, then wires the four subsystems and
injects them into the HTTP layer:

```text
main → engine.New(cfg) ─┐
       settings.New(cfg)─┼→ api.New(em, ss, prober, cfg) → http.Handler
       media.New(cfg) ───┘
```

Request path: `server.ServeHTTP` (applies CORS) → `route()` (manual dispatch on
the first path segment) → `func (s *server) handleX(w, r, seg)`.

Streaming (`GET /{infoHash}/{fileIdx}`): `EnsureEngine(ih)` adds the torrent →
`Ready(ctx)` blocks for metadata → `NewReader(idx)` returns a piece-prioritized
`io.ReadSeekCloser` → the handler honors `Range`, copies via a pooled 256 KB
buffer, and sets DLNA headers. The engine prioritizes the streamed file
(demoting others), primes moov/header pieces, scales readahead by bandwidth, and
runs an LRU janitor against `settings.cacheSize`.

Archive/NZB/FTP follow the same shape (see Patterns): a package-level
`map[key]*session` guarded by a mutex, with a lazily-started TTL janitor.
Settings persist to `<APP_PATH>/server-settings.json` via atomic temp+rename.

## Key Directories

| Path | Purpose |
|---|---|
| `cmd/rivulet-engine` | entrypoint: env parsing, wiring, TLS, graceful shutdown, pprof |
| `internal/types` | shared contract: `Config` + `Engine`/`EngineManager`/`SettingsStore`/`MediaProber` interfaces + `Stats`/`Options` wire structs |
| `internal/api` | enginefs routes, streaming, proxy, casting, addons (`archive.go`, `nzb.go`, `ftp.go`, `bitmagnet.go`, `torznab.go`, `localaddon.go`, `youtube.go`, `gethttps.go`) |
| `internal/engine` | anacrolix client wrapper, readers, trackers, LRU cache eviction, in-RAM piece storage |
| `internal/media` | `ffprobe`/`ffmpeg` shell-outs: probe, HLS transcode, subtitles, opensub hash |
| `internal/streamproxy` | `/proxy` HLS/DASH rewrite, DRM decrypt, signed URLs, segment cache |
| `internal/netguard` | SSRF guard: private/loopback/cloud-metadata IP checks + a dialer `Control` hook (DNS-rebinding-safe), shared by the proxy, `/create`, and ftpstream |
| `internal/settings` | `server-settings.json` store (atomic persist) |
| `internal/logging` | `slog`-based structured logger (text/json, leveled, component-tagged) |
| `internal/archive` `internal/nzb` `internal/ftpstream` | format/protocol backends behind the matching api handlers |

## Development Commands

```sh
make build        # host binary (CGO_ENABLED=0) → ./rivulet-engine
make run          # build + run
make test         # go test -p 1 -race ./...   (serial, race detector)
make vet          # go vet ./...
make fmt          # gofmt -s -w .
make fmt-check    # fail if not gofmt -s clean
make lint         # golangci-lint run ./... (if installed)
make api-fuzz     # fuzz the HTTP API with Schemathesis against internal/api/openapi.json (docker/podman; see Testing & QA)
make dup-baseline # re-lock the dupl duplication ratchet (.github/dup-baseline.json) after deliberate changes
make build-all    # cross-compile all 11 release targets → dist/ (android/{arm,arm64} need ANDROID_ARM_CC / ANDROID_ARM64_CC)
make smoke        # ./scripts/smoke.sh end-to-end API test (needs a running build)
```

Cross-compile targets: `linux/{amd64,arm64,armv7}`, `darwin/{amd64,arm64}`,
`windows/{amd64,arm64}` — all `CGO_ENABLED=0`. `android/{armv7,arm64,amd64,386}` are the
exception: Android ships no `/etc/resolv.conf`, so a pure-Go binary's resolver
falls back to `127.0.0.1:53` (nothing listens there) and every DNS lookup
fails, and the Go toolchain hard-requires external linking for `android/arm`
regardless. Both need `CGO_ENABLED=1` plus an NDK clang via
`ANDROID_ARM_CC`/`ANDROID_ARM_CXX` and `ANDROID_ARM64_CC`/`ANDROID_ARM64_CXX`
(and `ANDROID_AMD64_CC`/`ANDROID_386_CC` likewise, which also pulls in the C++ `go-libutp` instead of the pure-Go uTP fallback),
plus `-ldflags=-checklinkname=0` (needed anyway for `github.com/wlynxg/anet`
on Go 1.23+).
Benchmarks: `go test -bench . -benchmem ./internal/api/`.

## Code Conventions & Common Patterns

- **Dependency injection** via `api.New(em, ss, prober, cfg)` returning interface
  types from `internal/types`; never reach into concrete impls from `api`.
- **Handlers** are methods: `func (s *server) handleX(w http.ResponseWriter, r *http.Request, seg []string)`, dispatched from `route()`. JSON via `writeJSON(w, code, map[string]any{...})`.
- **Session caches** are package-level `map[key]*state` + `sync.Mutex` + a
  `time.Time` lastAccess + a janitor started once via `sync.Once`
  (`archiveStartJanitor`, `nzbStartJanitor`, casting `deviceCache`,
  `availableInterfaces` TTL cache). Reuse this pattern; don't invent new ones.
- **Logging**: `logging.For("component").Info("lowercase event", "snake_case_key", val)`.
  Errors as `..., "err", err`. NEVER assign `logging.For(...)` to a package-level
  var (it captures the pre-`Setup` default) — call it inline or store it in a
  constructor that runs after `main` calls `logging.Setup()`.
- **Errors**: every return checked or explicitly `_ =`-discarded; use
  `errors.Is(err, target)` for sentinels (never `==`) and `%w` to wrap.
- **Env config** lives only in `main.go` via `getenv`/`envInt`/`envBool`, or a
  `LookupEnv`-based helper when empty must mean "disabled" (see `metadataURL()`,
  `trackersURL()` — `off`/`0`/`false`/empty → disabled); the parsed value is
  threaded through `types.Config` (`RIVULET_HTTP_LOG` → `Config.HTTPLog`,
  parsed once in `main.go` via `envBool` and passed to `api.New`, is the
  reference example — it used to be read ad hoc as `os.Getenv(...) != ""` in
  `internal/api`, so `RIVULET_HTTP_LOG=0` now *disables* logging like every
  other `envBool` knob instead of enabling it). Sanctioned exceptions that
  read `os.Getenv`/`os.LookupEnv` directly in their own package instead —
  because the value is needed before `types.Config` exists, or must be
  re-read per call rather than pinned at startup — and must stay that way:
  - `internal/logging`: `RIVULET_LOG_LEVEL` / `RIVULET_LOG_FORMAT` — `Setup()`
    runs before `main` builds `types.Config`.
  - `internal/api/archive.go`, `internal/ftpstream/ftpstream.go`:
    `RIVULET_ARCHIVE_ALLOW_PRIVATE` / `RIVULET_FTP_ALLOW_PRIVATE` — re-checked
    on every fetch.
  - `internal/api/archive.go`, `internal/api/localaddon.go`:
    `RIVULET_ARCHIVE_LOCAL_ROOT` / `LOCAL_FILES_DIR` — local-root resolution
    for archive and local-files sources.
  - `internal/media/hls.go`: `RIVULET_HWACCEL` — probed lazily at first
    transcode, not at startup.
  Do not add a new outside-`main.go` env read without adding it to this list.
- **Imports**: the project module goes in its **own** goimports group
  (`goimports local-prefixes: github.com/rivulet-kodi/rivulet-engine`); keep
  `gofmt -s` clean.
- **Pure Go only**: `CGO_ENABLED=0`. Avoid new dependencies and never add cgo or
  shell-outs to new binaries (existing exec is limited to `ffmpeg`/`yt-dlp`).
- **Licensing (REUSE)**: every file starts with an SPDX header (copyright
  holder "The rivulet-engine Authors", year 2026, MIT license identifier) —
  never an individual's name or email; contributors are credited in `AUTHORS`
  instead. The header goes before any `//go:build` line and is separated from
  package docs by a blank line; use `reuse annotate --copyright 'The
  rivulet-engine Authors' --year 2026 --license MIT <file>`. Files without
  comment syntax or regenerated by tools go in `REUSE.toml`. CI runs
  `reuse lint`. When merging a contributor's first PR, add them to `AUTHORS`.

## Important Files

- `cmd/rivulet-engine/main.go` — wiring, env, TLS hot-swap, shutdown.
- `internal/api/api.go` — `route()` dispatcher, streaming, `parseRange`, CORS, shared helpers.
- `internal/types/types.go` — the interface + wire-shape contract (`Stats`, `Options`).
- `internal/engine/engine.go` + `trackers.go` — torrent client, readers, ranked tracker list.
- `internal/media/hls.go` — HLS session manager (path-traversal-guarded session ids).
- `.golangci.yml`, `Makefile`, `Dockerfile`, `.goreleaser.yml`, `internal/api/openapi.json` (hand-maintained OpenAPI 3.1 spec, served at `/openapi.json`; Scalar reference at `/api/reference`).

## Runtime / Tooling Preferences

- **Go versions:** `go.mod` directive is **1.27.3** (latest patch; `govulncheck` in CI scans the stdlib of exactly this version, so bump it when a Go point release fixes a reachable stdlib vuln); Docker build image is **`golang:1.27.3-alpine3.24`** (keep it on the same patch as `go.mod`). CI uses `go-version-file: go.mod`. Keep the go.mod directive build-compatible across the cross-compile matrix.
- **`CGO_ENABLED=0`** everywhere — no native deps.
- **golangci-lint v2** (`bodyclose, errcheck, errorlint, gosec, govet, ineffassign, misspell, revive, staticcheck, unconvert, unused, whitespace`). `gosec` excludes by-design rules (variable URLs/subprocess/file paths — this is a localhost media server). `_test.go` files skip `errcheck`/`gosec`/`bodyclose`/`unparam` but still face `gofmt`/`staticcheck`/`revive`.
- **Dev tools** (`govulncheck`, `actionlint`, `deadcode`, `dupl`) are pinned in the separate module `tools/go.mod` as `go tool` directives; run them with `go tool -modfile=tools/go.mod <name>` (Dependabot tracks `/tools`). CI also runs `go mod tidy -diff` (both modules), `scripts/dup-check.sh` (dupl ratchet), actionlint + zizmor, dependency-review (PRs), report-only Trivy (image/fs/config, SARIF), CodeQL, Scorecard and a weekly Trivy scan of published ghcr.io images. `deadcode` is not gated: it reports unreachable funcs from the `cmd/rivulet-engine` roots (the `librivulet-engine` cgo entry points, `internal/nzb`, parts of `ftpstream`/`streamproxy`).
- **Runtime deps** for full functionality: `ffmpeg`/`ffprobe` (HLS/probe/subtitles), `yt-dlp` (`/yt`). Bundled in the container image.
- Config knobs are env vars (`RIVULET_*`, `HTTP_PORT`, `APP_PATH`, …) — see `docs/CONFIGURATION.md`; add new ones in `main.go` and document them there.

## Testing & QA

- **Framework**: stdlib `testing` + `net/http/httptest` only — no external test
  libraries. Tests are table-driven and live in-package as `*_test.go`.
- **HTTP handler tests** use in-package fakes in `internal/api/api_test.go`
  (`fakeEngine`/`fakeEM`/`fakeSS`/`fakeProber`, `newHandler(t, …)`,
  `testEngine()`) to drive `api.New` through `httptest`. Reuse these.
- **Offline & deterministic** is mandatory: no real network, torrents, ffmpeg, or
  NNTP. Use `httptest.Server` for HTTP, `t.TempDir()` for files, in-test fixtures
  for zip/NZB/yEnc, and `net.Pipe` for NNTP.
- **Run**: `make test` (= `go test -p 1 -race ./...`); per-package
  `go test ./internal/api/`; coverage `go test -cover ./...`.
- **Expectations for new code**: add tests in the owning package; assert response
  shapes against the `server.js`/`stremio-core` contract (see
  `internal/types/types_test.go` `TestOptionsJSONShape`); keep benchmarks
  (`bench_test.go`) green as perf-regression guards. `cmd/` has no tests (thin wiring).
- **OpenAPI spec**: `internal/api/openapi.json` (OpenAPI 3.1.1, hand-maintained,
  `go:embed`ded, served at `GET /openapi.json`; Scalar reference at
  `/api/reference`, vendored bundle updated with `scripts/update-scalar.sh`).
  A new or changed route, parameter, status code or response body MUST be
  reflected in it in the same change. `TestOpenAPISpecMatchesRoutes` keeps the
  top-level path segments of `route()` and the spec in step; the fuzzer below
  checks the rest.
- **API fuzzing** (`make api-fuzz` = `scripts/api-fuzz.sh pr`; CI workflow
  `api-fuzz.yml`: fixed-seed PR run + nightly random seed): builds the image,
  runs the engine on an `--internal` container network (no egress; trackers,
  metadata and IMDb lookups off) and runs the digest-pinned Schemathesis
  image against `openapi.json` with `not_a_server_error`,
  `response_schema_conformance`, `status_code_conformance` and the custom
  `foreign_origin_rejected` check (`tests/apifuzz/hooks.py`: every operation
  replayed with `Origin: https://evil.example` must answer 403).
  `tests/apifuzz/schemathesis.toml` lists the operations that are never fuzzed
  (each with its reason) and the known issues deferred from triage. Env knobs:
  `RIVULET_FUZZ_SEED`, `RIVULET_FUZZ_EXAMPLES`, `RIVULET_FUZZ_REPORT_DIR`,
  `RIVULET_FUZZ_DOCKER` (docker or podman), `RIVULET_FUZZ_IMAGE` (prebuilt engine
  image), `RIVULET_FUZZ_URL` (fuzz a running server) with
  `SCHEMATHESIS='uvx schemathesis@4.32.5'` for a local CLI. A failure replays with
  `RIVULET_FUZZ_SEED=<seed from the log>`; the JUnit report, VCR cassette and engine log are
  in the report dir (uploaded as the `api-fuzz-report` CI artifact).

## Review skills

`.agents/skills/` vendors Go review skills (samber/cc-skills-golang, MIT). Which to load for which area, and the conflict rule (this file wins), is in `.agents/rules/review-skills.md`; reusable workflows (`ci-fix`, `pr`, `review`, `smoke`) are in `.agents/commands/`.
