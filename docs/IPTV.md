<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# IPTV Add-on

The `/iptv` add-on turns one or more M3U playlists into Stremio `tv` catalogs:
each playlist becomes a catalog, each entry a channel with its logo and groups,
and a stream request returns the channel URL. An optional XMLTV guide adds
now/next programmes. It is **off by default**.

[Free-TV/IPTV](https://github.com/Free-TV/IPTV) is a good starting point: it
lists only free-to-air channels, grouped by country.

---

## Configure

| Variable | Default | Purpose |
|---|---|---|
| `RIVULET_IPTV_PLAYLISTS` | _(unset)_ | Comma-separated M3U playlist URLs (`http`/`https`). Unset / empty / `off` = add-on disabled: every `/iptv` route answers 404. Invalid entries are dropped with a warning. |
| `RIVULET_IPTV_EPG` | _(unset)_ | Comma-separated XMLTV guide URLs, plain or gzip. Unset / empty / `off` = no guide. See [Programme guide](#programme-guide-epg). |

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
or `IPTV 1`, `IPTV 2`, … with several). With a guide configured the add-on also
publishes one guide catalog per playlist; Stremio hides those from Discover and
uses them for its live-TV guide.

---

## How it works

- **Fetching.** A playlist is downloaded on first use, cached for 6 hours and
  kept when a refresh fails (a failed fetch is retried after a minute at the
  earliest). Playlists are capped at 16 MiB and 20 000 channels.
- **Parsing.** `#EXTINF` gives the name (text after the first unquoted comma,
  else `tvg-name`), `tvg-logo` and `group-title` (`;`-separated groups;
  `#EXTGRP` as fallback), `tvg-id` (the guide key) and `tvg-chno`. Relative entry URLs resolve against the playlist URL;
  non-`http(s)` entries (`rtmp://`, `udp://`, …) and duplicates are skipped.
- **Order.** Channels with a positive `tvg-chno` come first, by number
  ascending; the rest follow in playlist order (stable). A playlist without
  numbers keeps its order.
- **Catalog extras.** `genre` (one of the playlist's groups, offered in the
  manifest), `search` (case-insensitive name match) and `skip` (pages of 100).
- **Ids.** `iptv:` + 32 hex chars of SHA-256 over the playlist URL and the
  entry URL, so an id survives playlist refreshes and reordering.
- **Live items.** Previews are `type: "tv"` with `posterShape: "square"` and
  `behaviorHints.isLive: true`, which is how Stremio recognises live TV (and
  keeps channels out of Continue Watching).
- **Streams and headers.** A channel whose entry sets request headers
  (EXTINF attributes `http-user-agent`, `http-referrer`/`http-referer`,
  `http-origin`; the same names as `#EXTVLCOPT:` lines; or Kodi's
  `url|Referer=…&User-Agent=…` suffix; when several set the same header the
  `|Header=` suffix wins over `#EXTVLCOPT`, which wins over the EXTINF
  attribute) is returned as an absolute `/proxy`
  URL: the [stream proxy](PROXY.md) sends those headers upstream and adds the
  CORS headers stremio-web needs. `.m3u8` entries go through
  `/proxy/hls/manifest.m3u8`, `.mpd` through `/proxy/mpd/manifest.m3u8`,
  anything else through `/proxy/stream`. With `RIVULET_PROXY_PASSWORD` set the
  URL carries `api_password`. Every other channel URL is returned unchanged.
- **Recording.** A channel can be recorded with the [DVR](DVR.md) like any
  other live HLS URL.

---

## Programme guide (EPG)

Set `RIVULET_IPTV_EPG` to one or more XMLTV URLs (plain `.xml` or gzip, detected
by content, not by name) to get now/next programmes. The model is Stremio's own
(stremio-core 0.66), so the official apps and other live-TV add-ons interoperate:

- **Manifest** (version `0.2.0`) gets `behaviorHints.epgProvider: true` and, per
  playlist, a guide catalog `iptvN-guide` (type `tv`, name `<playlist name>
  guide`, extras `date` and `skip`). Without a guide the manifest has neither.
- **Guide catalog** `/iptv/catalog/tv/iptvN-guide/date=YYYY-MM-DD&skip=N.json`
  answers `{"metasDetailed": [...]}`: the playlist's channels in order, 100 per
  page, each a full meta whose `videos` are its programmes overlapping that
  **UTC** date. Channels without a programme that day are listed with empty
  `videos`. A missing or invalid date means today (UTC).
- **Meta** `/iptv/meta/tv/<id>.json` has `behaviorHints.hasScheduledVideos` and
  `videos`: the programmes from now − 2 h to now + 24 h, sorted by start.
- **Programme** (a `Video` with Stremio's epg fields): `id`
  `<channelId>:<startUnix>`, `title` (`Title - Sub-title` when the guide has a
  sub-title), `released` = `startTime`, `startTime`/`endTime` (RFC 3339, UTC),
  `overview` (`desc`), `thumbnail` (`icon`), `genres` (`category`) and
  `releaseInfo` (the `onscreen` `episode-num`, else `xmltv_ns` as `S1E6`).
  XMLTV times such as `20261105200000 +0100` are converted to UTC.
- **Matching.** A programme belongs to the channel whose `tvg-id` equals the
  XMLTV `channel` attribute (trimmed, case-insensitive). Channels without a
  `tvg-id` have no programmes. A playlist's `x-tvg-url`/`url-tvg` header is
  **never** used: some (Free-TV) point at guides of hundreds of MB; configure
  the guide you want explicitly.
- **Loading.** Guides load in the background, as a stream: only programmes of
  configured channels within now − 1 day … now + 2 days are kept. They are cached for 6 hours
  and the previous data is served when a refresh fails (a failed load is retried
  after a minute at the earliest). A catalog or meta request never waits more
  than about 2 seconds for the *first* load and otherwise answers without
  programmes; later requests see them. The guide load starts when the manifest
  is fetched.
- **Limits.** Memory is bounded by these, not by the guide's size:

  | Limit | Value |
  |---|---|
  | gzip download | 64 MiB |
  | decoded XML (and a plain download) | 512 MiB |
  | one text node or tag (the bytes between two `<`) | 256 KiB |
  | programmes kept per channel | 300 (the first in the guide, then sorted by start) |
  | programmes kept in total | 300 000 |
  | title / sub-title / description / icon URL | 200 / 200 / 1000 / 500 bytes |
  | categories per programme | 8 × 60 bytes |
  | load timeout | 3 min |

  A guide over a size cap, or with a text node or tag over 256 KiB (the XML
  decoder buffers each one whole, so this is what bounds it), fails to load
  (logged) and the previous data, if any, stays. Per programme only the first
  title, sub-title, description and icon, up to 8 categories and the first
  episode number of each kind are read, however many the guide has.

  **Peak memory.** While loading, the decoder holds one token (at most 256 KiB)
  at a time; what stays resident is the kept programmes: roughly 0.7 KB each
  for a typical guide (the Italian EPG's 5 162 matching programmes ran at 46 MiB total RSS), and at most
  about 3 KB each if every text field is at its cap, so the 300 000-programme
  limit is a theoretical ceiling of ~0.9 GB and ~0.2 GB for typical content.
  Point `RIVULET_IPTV_EPG` at a guide for the channels you use rather than a
  worldwide one if memory matters.

  **When a cap drops programmes.** Over 300 programmes for a channel, the
  300 closest to now are kept (airing or upcoming ones ahead of past ones),
  whatever their order in the file; the others are dropped. Over 300 000 programmes in
  total, the channels whose programmes come **later in the file** lose them
  (the earlier channels are kept whole): the file is still read to the end and
  the log reports `guide programmes dropped by the caps` with the number of
  programmes dropped and of channels left without any. Use a smaller guide, or
  one that lists only your channels, if that happens.

  Guides in ISO-8859-1 or Windows-1252 are read; other non-UTF-8 charsets are
  rejected. With several guide URLs, programmes of a channel are merged (the
  first guide wins on equal start times).

---

## Caveats

- **Guide quality.** Programmes are only as good as the guide and the
  `tvg-id` match; channels whose id is absent from the guide show none.
- **Dead channels.** Public playlists contain offline or geo-blocked
  entries; the add-on does not probe them.
- **Unauthenticated routes.** Like the other add-ons, `/iptv/*` needs no
  credentials. It returns channel metadata and URLs; content only flows through
  `/proxy` for header-dependent channels, under the proxy's usual rules.
- **Content is your responsibility.** The add-on loads whatever playlist it is
  given. Use lists of channels you are allowed to watch.
