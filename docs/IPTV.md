<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# IPTV Add-on

The `/iptv` add-on turns one or more M3U playlists into Stremio `tv` catalogs:
each playlist becomes a catalog, each entry a channel with its logo and groups,
and a stream request returns the channel URL. It is **off by default**.

[Free-TV/IPTV](https://github.com/Free-TV/IPTV) is a good starting point: it
lists only free-to-air channels, grouped by country.

---

## Configure

| Variable | Default | Purpose |
|---|---|---|
| `RIVULET_IPTV_PLAYLISTS` | _(unset)_ | Comma-separated M3U playlist URLs (`http`/`https`). Unset / empty / `off` = add-on disabled: every `/iptv` route answers 404. Invalid entries are dropped with a warning. |

```sh
RIVULET_IPTV_PLAYLISTS=https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8 ./rivulet-engine
```

Several playlists:

```sh
RIVULET_IPTV_PLAYLISTS=https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8,https://example.org/my-list.m3u
```

---

## Install in Stremio

Open the Stremio app, go to **Addons**, and paste:

```text
http://<server-host>:11470/iptv/manifest.json
```

The channels appear under **Discover → TV**, one catalog per playlist (`IPTV`,
or `IPTV 1`, `IPTV 2`, … with several).

---

## How it works

- **Fetching.** A playlist is downloaded on first use, cached for 6 hours and
  kept when a refresh fails (a failed fetch is retried after a minute at the
  earliest). Playlists are capped at 16 MiB and 20 000 channels.
- **Parsing.** `#EXTINF` gives the name (text after the first unquoted comma,
  else `tvg-name`), `tvg-logo` and `group-title` (`;`-separated groups;
  `#EXTGRP` as fallback). Relative entry URLs resolve against the playlist URL;
  non-`http(s)` entries (`rtmp://`, `udp://`, …) and duplicates are skipped.
- **Catalog extras.** `genre` (one of the playlist's groups, offered in the
  manifest), `search` (case-insensitive name match) and `skip` (pages of 100).
- **Ids.** `iptv:` + 32 hex chars of SHA-256 over the playlist URL and the
  entry URL, so an id survives playlist refreshes and reordering.
- **Streams and headers.** A channel whose entry sets request headers
  (`#EXTVLCOPT:http-referrer`, `http-user-agent`, `http-origin`, or Kodi's
  `url|Referer=…&User-Agent=…` suffix) is returned as an absolute `/proxy`
  URL: the [stream proxy](PROXY.md) sends those headers upstream and adds the
  CORS headers stremio-web needs. `.m3u8` entries go through
  `/proxy/hls/manifest.m3u8`, `.mpd` through `/proxy/mpd/manifest.m3u8`,
  anything else through `/proxy/stream`. With `RIVULET_PROXY_PASSWORD` set the
  URL carries `api_password`. Every other channel URL is returned unchanged.
- **Recording.** A channel can be recorded with the [DVR](DVR.md) like any
  other live HLS URL.

---

## Caveats

- **No EPG.** XMLTV guides are not read.
- **Dead channels.** Public playlists contain offline or geo-blocked
  entries; the add-on does not probe them.
- **Unauthenticated routes.** Like the other add-ons, `/iptv/*` needs no
  credentials. It returns channel metadata and URLs; content only flows through
  `/proxy` for header-dependent channels, under the proxy's usual rules.
- **Content is your responsibility.** The add-on loads whatever playlist it is
  given. Use lists of channels you are allowed to watch.
