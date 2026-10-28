---
description: Run the changed build on a free port and exercise the changed path with curl
argument-hint: "<topic> [what to exercise]"
---
<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

Smoke-test: $ARGUMENTS (first word = topic). Run from the worktree holding the change.

- Build: `make build` (binary `./rivulet-engine`), or `docker build -t rivulet-engine:<topic> .` to include ffmpeg/yt-dlp.
- Pick a free port and run in the background: `HTTP_PORT=<port> ./rivulet-engine &` (or `docker run -d --name rivulet-<topic> -e HTTP_PORT=<port> --network host rivulet-engine:<topic>`).
- Exercise the changed path with `curl` against `http://127.0.0.1:<port>`; check logs. For the broad end-to-end check run `BASE=http://127.0.0.1:<port> scripts/smoke.sh` (needs network: it pulls a public torrent).
- State-changing routes and `/proxy` are protected by the browser Origin allowlist; send a matching `Origin` header when testing them.
- Always clean up: kill the process / `docker rm -f rivulet-<topic>`, remove the temp image and any temp data dir.

Hard rules: never use the default :11470 instance of someone else; never push `main`.
