<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# Feature reference

The complete feature list, with the routes and behaviours behind the summary in the [README](../README.md).

- **Dual-stack torrent engine** - IPv4 + IPv6, TCP + uTP, BEP32 DHT, PEX,
  WebTorrent, webseeds; curated public trackers with RTT ranking + 24h refresh.
- **Full enginefs API** - `create`/`stats.json`/`remove`, Range/206 streaming
  with DLNA headers, `network-info`, `device-info`, `settings`, `opensubHash`,
  `subtitlesTracks`, `/probe`, `/tracks`, `/list`, `/:ih/peers`.
- **HLSv2 transcoding** - on-demand ffmpeg with multi-platform hardware accel
  (VAAPI / NVENC / QSV / VideoToolbox / V4L2M2M, verified at startup, libx264
  fallback), multi-audio + embedded-subtitle renditions, and seek-aware segments.
- **Subtitles** - SRT / WebVTT / ASS-SSA to WebVTT, OpenSubtitles hashing.
- **Reverse proxy** (`/proxy`), **DLNA casting** (`/casting`, SSDP discovery +
  UPnP AVTransport control), **local-files addon** (`/local-addon`, IMDB-resolved),
  **IPTV addon** (`/iptv`, opt-in M3U playlists as `tv` catalogs, optional XMLTV programme guide; see [IPTV](IPTV.md)),
  **YouTube** (`/yt`, via `yt-dlp`), and **`/get-https`** (Stremio cert provisioning).
- **MediaFlow / EasyProxy compatible** - `/proxy/stream`, `/proxy/hls/manifest.m3u8`,
  `/proxy/mpd/manifest.m3u8`, `/proxy/ip`, `/generate_url`, plus a MediaFlow-style
  `/extractor/video[.m3u8|.mp4]?host=…&d=<page>&redirect_stream=true|false` resolver
  driven by external, declarative definitions (see [Extractors](EXTRACTORS.md); no
  hosts are built in). Addons with a MediaFlow/EasyProxy setting can use this server
  as their proxy URL with `RIVULET_PROXY_PASSWORD` as the proxy password.
- **DVR recording** (opt-in) - EasyProxy-compatible `/record` + `/api/recordings`:
  an addon's DVR entry records a live stream in the background with `ffmpeg`
  (stream copy) while the player watches it; see [DVR](DVR.md).
- **Archive streaming** - direct playback of media inside ZIP / RAR / 7z / TAR /
  TGZ containers (`/zip`, `/rar`, `/7zip`, `/tar`, `/tgz`), plus **Usenet/NZB**
  (`/nzb`, NNTP + yEnc) and **FTP/FTPS** (`/ftp`) streaming - all pure-Go.
  Playback starts before the whole file is ready: stored (uncompressed) ZIP/TAR
  entries are served in place, other entries and NZB files stream while they
  are extracted/assembled (NZB over parallel NNTP connections). Repeating an
  identical `…/create/{key}` request (players re-request it on every seek)
  reuses the live session for up to an hour instead of rebuilding it; local
  archives are re-checked by size and modification time, remote URLs are not
  re-fetched within that hour.
- **Disk-bounded cache** - LRU eviction honouring the `cacheSize` setting: `0` is a true "no caching" mode (every torrent with zero open readers is purged once its idle grace window elapses; a negative/unlimited value never evicts on size), plus idle-torrent removal after inactivity (`RIVULET_TORRENT_IDLE_TIMEOUT`).
- Self-signed HTTPS on `:12470` for HTTPS web UIs (e.g. WebKitGTK shells).
- **Metrics** - `GET /metrics` exposes Prometheus-format gauges (goroutines, heap, active torrents, HLS sessions, proxy cache) with permissive CORS headers (`Access-Control-Allow-Origin: *`); **do not expose to untrusted networks** — bind to loopback only.
