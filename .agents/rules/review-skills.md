---
description: "Which vendored review skills (.agents/skills) to load for which kind of code or review"
alwaysApply: false
---
<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# Review skills

`.agents/skills/` vendors Go skills from [samber/cc-skills-golang](https://github.com/samber/cc-skills-golang) (MIT, pinned commit in `.agents/skills/README.md`). When to load them:

- `golang-security`, `golang-safety`: **reviews** of `internal/streamproxy` (`/proxy` rewrite, signed URLs, DRM), `internal/netguard` (SSRF: private/loopback/metadata IPs, dialer `Control` hook), and the archive/nzb/ftpstream/proxy fetchers that read untrusted remote bytes (size limits, path traversal, zip/NZB/yEnc parsing, integer overflow on offsets).
- `golang-concurrency`, `golang-context`: engine readers and janitors in `internal/engine`, LRU cache eviction, session caches (HLS sessions in `internal/media`, segment cache in `internal/streamproxy`), anything with goroutines, locks or request cancellation.
- `golang-performance`, `golang-benchmark`: hot paths (readers, piece storage, segment rewrite) and `bench_test.go`; measure with benchmarks/pprof before optimizing (`go test -bench . -benchmem ./internal/api/`).
- `golang-security` also applies to `internal/media` ffmpeg/ffprobe `exec` calls: argument injection, untrusted URLs/paths, process lifetime and output limits.
- `golang-observability`: logging and metrics changes.
- `golang-testing`: writing or reviewing tests (offline, table-driven, in-package fakes).
- `golang-troubleshooting`: debugging a concrete failure (flaky test, deadlock, leak, panic), not reviews.

Skip them for routine implementation. Use their review mode only, not their sub-agent fan-out. `AGENTS.md` wins on conflict: JSON responses go through `writeJSON`, logging through `logging.For` (not `slog` directly or `samber/oops`), tests use stdlib `testing` only (no testify/gomock), no new dependencies, pure Go (`CGO_ENABLED=0`).
