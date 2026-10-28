<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# Agent skills

Vendored, unmodified, so updates are a plain re-copy:

- `golang-*`: [samber/cc-skills-golang](https://github.com/samber/cc-skills-golang) at commit `8e899e2`, MIT (`LICENSES/MIT.txt`, copyright Samuel Berthe). Only `SKILL.md` and `references/` are kept.

| Skill | Use for |
|---|---|
| `golang-security` | Trust boundaries, SSRF, injection, secrets, resource limits |
| `golang-safety` | Nil, slice/map aliasing, numeric overflow |
| `golang-concurrency` | Goroutine lifecycle, locks, channels, worker pools |
| `golang-context` | Cancellation, deadlines, request-scoped values |
| `golang-performance` | Allocation, I/O, hot paths; optimization patterns |
| `golang-benchmark` | Benchmarks, benchstat, pprof, trace |
| `golang-observability` | Metrics, logging, tracing |
| `golang-testing` | Test design, fuzzing, races, flaky tests |
| `golang-troubleshooting` | Debugging concrete failures |

When to load them and how they rank against repo conventions: see `.agents/rules/review-skills.md` and "Review skills" in `AGENTS.md`.

Update: copy the skill from a reviewed upstream commit and bump the commit above. Do not track upstream `main` automatically.
