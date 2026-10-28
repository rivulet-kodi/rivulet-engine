<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

<div align="center">

# Rivulet Engine

**A fast, IPv6-capable, pure-Go streaming server for Rivulet and other Stremio-compatible clients.**

Torrents, archives, Usenet and FTP in; HLS, DLNA and plain Range streams out — one static binary, no runtime dependencies besides optional `ffmpeg`.

[![CI](https://github.com/rivulet-kodi/rivulet-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/rivulet-kodi/rivulet-engine/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/rivulet-kodi/rivulet-engine?label=release)](https://github.com/rivulet-kodi/rivulet-engine/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go](https://img.shields.io/github/go-mod/go-version/rivulet-kodi/rivulet-engine)](go.mod)
</div>

---

Rivulet Engine is an open-source drop-in replacement for Stremio's
closed-source streaming server (`server.js`), built on
[`anacrolix/torrent`](https://github.com/anacrolix/torrent). It serves the
exact `enginefs` HTTP API that `stremio-web` and the
[Rivulet](https://github.com/rivulet-kodi/plugin.video.rivulet) Kodi addon
expect, so you point a client at it and it just works.

> **Not affiliated with or endorsed by Stremio.**

## Contents

- [Features](#features)
- [Install](#install)
- [Run](#run)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Security](#security)
- [Development](#development)

## Features

- **Dual-stack torrent engine** — IPv4 + IPv6, TCP + uTP, DHT, PEX,
  WebTorrent and webseeds, with RTT-ranked public trackers.
- **Full `enginefs` API** — create/stats/remove, Range streaming with DLNA
  headers, subtitles (SRT/VTT/ASS → WebVTT), OpenSubtitles hashing, probing.
- **HLS transcoding** — on-demand `ffmpeg` with VAAPI, NVENC, QSV,
  VideoToolbox and V4L2M2M acceleration (libx264 fallback), multi-audio and
  embedded-subtitle renditions, HDR → SDR tone mapping.
- **More than torrents** — stream from ZIP/RAR/7z/TAR archives, Usenet
  (NZB), FTP/FTPS and YouTube, all pure Go.
- **Stream proxy** — HLS/DASH rewriting, signed URLs, MediaFlow/EasyProxy
  compatible, with declarative [extractors](docs/EXTRACTORS.md).
- **DLNA casting** and an opt-in [DVR](docs/DVR.md) for live streams.
- **Bounded resources** — LRU disk cache, optional RAM-only mode, idle
  torrent reclaim, Prometheus `/metrics`.
- **Self-hosted indexers** — [Bitmagnet](docs/BITMAGNET.md) and
  [Torznab](docs/TORZNAB.md) add-ons built in.

The full list, with routes and behaviours, is in [docs/FEATURES.md](docs/FEATURES.md).

## Install

Prebuilt binaries for **Linux, macOS, Windows and Android**
(arm64 / armv7 / x86_64 / x86) are attached to each
[release](https://github.com/rivulet-kodi/rivulet-engine/releases). The Rivulet
addon downloads and runs the right one for you.

| Method | Command |
| --- | --- |
| **Go** (1.27+, no CGO) | `go install github.com/rivulet-kodi/rivulet-engine/cmd/rivulet-engine@latest` |
| **From source** | `make build` → `./rivulet-engine` (`make build-all` cross-compiles every target) |
| **Container** | Podman/Docker or a HuggingFace Space — see [docs/CONTAINER.md](docs/CONTAINER.md) |

## Run

```sh
./rivulet-engine            # http://127.0.0.1:11470  (https on :12470)
./rivulet-engine version
```

Then set your client's **streaming server URL** to `http://127.0.0.1:11470`.
For a Stremio web UI over HTTPS, see [docs/HTTPS.md](docs/HTTPS.md).

## Configuration

Everything is an environment variable. Unprefixed names are the
`server.js`-compatible ones; the rest use the `RIVULET_` prefix. The most
common ones:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BIND_ADDRESS` | all interfaces | Interface to listen on. **Set `127.0.0.1` unless you need LAN access.** |
| `HTTP_PORT` / `HTTPS_PORT` | `11470` / `12470` | Listener ports (`HTTPS_PORT=0` disables HTTPS). |
| `APP_PATH` | `~/.rivulet-engine` | Data and cache root. |
| `RIVULET_PUBLIC_URL` | derived | External base URL, for use behind a reverse proxy. |
| `RIVULET_ALLOWED_ORIGINS` | built-in list | Extra browser `Origin`s allowed to call the API. |
| `RIVULET_MEMORY_CACHE_SIZE` | `0` (disk) | RAM-only piece cache in bytes, for low-disk devices. |
| `RIVULET_TORRENT_IDLE_TIMEOUT` | `300` | Seconds before an unused torrent is dropped. |
| `RIVULET_HWACCEL` | auto | `0` forces software transcode, or pin `vaapi`, `nvenc`, … |
| `RIVULET_ENABLE_DLNA` | off | Enable casting on `/casting`. |
| `RIVULET_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error`. |

The full list, about 100 knobs, is in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md). A ready-made systemd user unit
and env file live in [`deploy/systemd`](deploy/systemd).

## Documentation

| Guide | Covers |
| --- | --- |
| [Features](docs/FEATURES.md) | Full feature and route reference |
| [Configuration](docs/CONFIGURATION.md) | Every environment variable |
| [HLS transcoding](docs/HLS.md) | Per-session overrides, persistent sessions, tone mapping |
| [BitTorrent tuning](docs/BITTORRENT.md) | Censorship resistance, start-up priming, speed limits |
| [Stream proxy](docs/PROXY.md) | `/proxy` rewriting, DRM, signed URLs |
| [Extractors](docs/EXTRACTORS.md) | Declarative host definitions for `/extractor` |
| [DVR](docs/DVR.md) | Recording live streams |
| [HTTPS](docs/HTTPS.md) | Browser-trusted certificate for the Stremio web UI |
| [Container](docs/CONTAINER.md) | Podman, Docker, HuggingFace Spaces |
| [Bitmagnet](docs/BITMAGNET.md) · [Torznab](docs/TORZNAB.md) | Indexer add-ons |
| [Platforms](docs/PLATFORMS.md) | Build targets, Android, library mode |
| [Architecture](docs/ARCHITECTURE.md) | Package layout |
| [API reference](internal/api/openapi.json) | OpenAPI 3.1 spec (also `GET /openapi.json`; interactive page at `/api/reference`) |

## Security

Rivulet Engine is a **localhost service with no authentication**: anything that
can reach the port is trusted. The default binds every interface, so set
`BIND_ADDRESS=127.0.0.1` and never expose `:11470` / `:12470` to untrusted
networks. State-changing routes and `/proxy` additionally check the browser
`Origin`, and every outbound fetch goes through an SSRF guard. Details in
[docs/SECURITY.md](docs/SECURITY.md).

## Development

```sh
make build      # host binary
make test       # go test -p 1 -race ./...
make lint       # golangci-lint
make api-fuzz   # fuzz the HTTP API against the OpenAPI spec (docker/podman)
make smoke      # end-to-end API test against a running build
```

See [AGENTS.md](AGENTS.md) for conventions and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
for the package layout.

## License

[MIT](LICENSE) — Copyright (c) 2026 [The rivulet-engine Authors](AUTHORS).
Contributions are accepted under the same license; new files carry the
project's SPDX header (`reuse annotate --copyright 'The rivulet-engine Authors'
--license MIT <file>`), and first-time contributors add themselves to
[AUTHORS](AUTHORS).
