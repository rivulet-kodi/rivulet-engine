<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# Architecture

Streaming flow: `EnsureEngine(ih)` adds the torrent, `Ready(ctx)` waits for metadata, `NewReader(idx)` returns a piece-prioritised reader, and the handler honours `Range` with DLNA headers.

## Layout

| Path | Responsibility |
|---|---|
| `cmd/rivulet-engine` | entrypoint, wiring, TLS, version |
| `internal/types` | shared contract (structs + interfaces) |
| `internal/engine` | anacrolix client, readers, stats, trackers, cache eviction |
| `internal/api` | enginefs routes, streaming, proxy, casting, local-addon, youtube, archive/nzb/ftp |
| `internal/streamproxy` | `/proxy` HLS/DASH rewrite, DRM decrypt, signed URLs, segment cache |
| `internal/dvr` | DVR recorder: ffmpeg stream-copy supervisor, recording store, retention |
| `internal/netguard` | SSRF guard: private/loopback/cloud-metadata checks + dial-time `Control` hook, shared by the proxy, archive/nzb/ftp fetches, and the media relay |
| `internal/settings` | settings store (`server-settings.json`) |
| `internal/media` | ffprobe, HLS transcode, subtitles, opensub hash |
| `internal/archive` | uniform reader over zip / rar / 7zip / tar / tgz entries |
| `internal/nzb` | NZB parser, yEnc decoder, NNTP client, segment assembler |
| `internal/ftpstream` | FTP/FTPS + HTTP(S) byte-range stream opener |
| `internal/logging` | structured slog logger (leveled, component-tagged, text/json) |
| `internal/api/openapi.json` | hand-maintained OpenAPI 3.1 spec (embedded; `GET /openapi.json`, Scalar reference at `/api/reference`) |
| `scripts/smoke.sh` | end-to-end API smoke test |
