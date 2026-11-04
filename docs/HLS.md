<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# HLS transcoding

On-demand `ffmpeg` transcoding behind `/hlsv2`. Settings live in [Configuration](CONFIGURATION.md); this page covers behaviour, per-session overrides and persistence.

The HLS master playlist's `BANDWIDTH` and video `CODECS` (H.264 profile-level)
are derived from the effective bitrate cap and output resolution — the variant's
peak for `BANDWIDTH` (the `RIVULET_TRANSCODE_MAXRATE`/`transcodeMaxBitRate`
bits/second value plus `RIVULET_TRANSCODE_AUDIO_BITRATE`), and the output
width×height mapped to the matching H.264 level (≤1280×720 → Level 3.1,
≤1920×1080 → Level 4.1, ≤2560×1440 → Level 5.0, else Level 5.1). An unconfigured
server (no bitrate/resolution knob touched) keeps advertising the historical
fixed `BANDWIDTH=4000000`/`avc1.640029` regardless of actual source resolution,
so upgrading never changes existing playback.
A per-session quality override (below) always counts as "configured" and
takes the derived path, even when its values equal the defaults.

## Per-session HLS overrides

With `RIVULET_HLS_SESSION_OVERRIDES=1`, a front end serving several
independent sessions can give each its own quality and lifetime, e.g. one
watch-party room at 5 Mbps for a friend on a slow link and another at
12 Mbps, or a title prepared hours ahead of a scheduled party with a longer
idle TTL than casual sessions. The request that creates a session may add:

| Parameter | Meaning | Accepted values |
|---|---|---|
| `ttl` | idle-eviction TTL for this session | whole seconds or a Go duration (`3600`, `90m`), from `60s` (the stock default, so an override never makes a session less durable) to `30 days` |
| `maxWidth`, `maxHeight` | downscale caps, same rules as `RIVULET_TRANSCODE_MAX_WIDTH`/`_HEIGHT` (aspect-preserving, never upscales) | `16`–`7680`, rounded down to even |
| `bitrate` | video target (`-b:v`); unless given explicitly, `maxRate` = `bitrate` and `bufSize` = 2× `bitrate` | ffmpeg syntax, decimals allowed as for the `RIVULET_TRANSCODE_*` env knobs (`6M`, `1.5M`, `800k`), `100k`–`200M` |
| `maxRate`, `bufSize` | explicit `-maxrate` / `-bufsize`; when `bitrate` and `maxRate` are both given, `maxRate` must be at least `bitrate` | as `bitrate`; `bufSize` up to `400M` |

```text
GET /hlsv2/room-a/master.m3u8?mediaURL=…&bitrate=5M&maxHeight=720&ttl=4h
```

- **Precedence:** per-session override > `/settings` (`transcodeMaxBitRate`,
  `transcodeMaxWidth`) > env. Knobs not overridden keep their `/settings`/env
  value; the master playlist `BANDWIDTH`/`CODECS` follow the effective values.
  A per-session `ttl` beats `RIVULET_HLS_SESSION_TTL=0`
  (`DisableIdleEviction`) too: a session created with an explicit `ttl` is
  still evicted by the reaper on schedule even while global idle eviction is
  off; only a session with no `ttl` of its own is kept forever in that mode.
- **Validation:** an invalid or out-of-range value fails the request with
  `400` and a message naming the parameter; no session is created. Parameter
  names are case-sensitive camelCase as above: unknown or mis-cased names
  (e.g. `maxrate`) are ignored, and a repeated parameter uses its first
  value. The `mediaURL` SSRF check and session-id guard run first and are
  unaffected.
- **Trust:** overrides are not clamped to the operator's limits. With the
  gate on, clients can raise as well as lower the env and `/settings` limits
  (e.g. `maxWidth=7680&bitrate=200M`), and a long `ttl` can hold
  `RIVULET_HLS_MAX_SESSIONS` slots and disk for days. Enable it only when
  every client that can reach the server is trusted.
- **Existing sessions:** a session's transcode settings are frozen when it is
  created, so quality parameters on later `master.m3u8` requests for the same
  id are ignored. `ttl` is the exception: it replaces the session's TTL, so a
  caller can extend (or shorten, never below `60s`) a session's lifetime.
- **TTL granularity:** idle sessions are reaped on each
  `RIVULET_HLS_REAPER_INTERVAL` tick, so a session is removed between `ttl`
  and `ttl` + one reaper interval (≤30s by default) after its last access. A
  long-TTL session keeps its slot in `RIVULET_HLS_MAX_SESSIONS` (and its
  segment cache on disk) until then.

`DELETE /hlsv2/{id}` ends an HLS transcode session explicitly, for front ends
that manage their own session lifecycle rather than relying on the idle
reaper (and the only way to end one when `RIVULET_HLS_SESSION_TTL=0`). The
session is unregistered at once, so later requests for it get `unknown
session`, and its segment cache is removed from disk as soon as no request
for it is still being served. Responses: `204` deleted, `404` unknown
session, `400` invalid id (the same ids `master.m3u8` rejects). Like every
route it is subject to the `RIVULET_ALLOWED_ORIGINS` Origin allowlist.
Re-creating the same id is refused until a deferred removal has finished.

`GET /hlsv2/{id}/{file}` answers `404` for an unknown session, a segment that
is missing or past the end, and any other transcode failure. When the source
fails while a segment is transcoded, the segment is answered `502` (for
example, the origin answered `429`), or `504` when the origin timed out or the
segment ran past its time limit (`RIVULET_HLS_SEGMENT_TIMEOUT`, doubled when a
hardware attempt precedes the software fallback). The reason is in the body.
Before answering, the server makes its own transcode attempts: a software
fallback after a failed hardware attempt, and at most one retry with a longer
input seek. A client should back off before it requests the segment again.

HDR→SDR tone mapping (`RIVULET_TRANSCODE_TONEMAP`) applies only to sources
whose first video stream reports a PQ (`smpte2084`) or HLG (`arib-std-b67`)
transfer; SDR sources (including 10-bit SDR) are transcoded exactly as before.
Missing or unexpected primaries/matrix/range tags fall back to BT.2020
limited range.
The chain is `zscale` (linearise, and downscale when a max size is set) →
`tonemap` → `zscale` to BT.709, done in software before each encoder's usual
input (`format=yuv420p`, or `format=nv12,hwupload` for VAAPI), and the output
is tagged BT.709. On an 8-core Ryzen 7 7735HS with a 4K HEVC HDR10 source and
1080p libx264 output, a 4-second segment took roughly 1–1.5 s longer with
tone mapping than without; without a downscale (4K output) it runs about 2x
slower than real time here, so set a max width/height on low-power hardware.
Dolby Vision profile 5 (no HDR10/HLG base layer) is detected and left
untouched, since its colours are only correct after applying the DV
reshaping this chain does not do; profiles 7/8.1/8.4 go through the HDR10 or
HLG path via their base layer.

## Persistent HLS sessions

By default HLS sessions live in memory and in a fresh random
`rivulet-engine-hls-*` directory that is deleted on shutdown, so a restart loses
every transcoded segment. `RIVULET_HLS_PERSIST=1` (with
`RIVULET_HLS_WORK_DIR` set; otherwise a warning is logged and nothing
persists) keeps them in `<RIVULET_HLS_WORK_DIR>/rivulet-engine-hls-persist/<id>/`
with a `session.json` each, so a front end can pre-transcode a title hours
ahead and still have it after a restart, crash or update.

- **Raise `RIVULET_HLS_SESSION_TTL`.** The TTL still applies: with the
  default `60`, a session is evicted a minute after its last use and
  discarded by any restart longer than a minute, which makes persistence
  nearly useless. Set it to cover the gap between pre-transcoding and
  watching (e.g. `12h`), or set it to `0` to keep sessions until they're
  deleted. A warning is logged if it's left at the default.
- **On start**, each session is restored without re-probing and its
  existing segments are served from disk. Sessions idle past their TTL
  (a per-session `ttl` if they have one, otherwise
  `RIVULET_HLS_SESSION_TTL`, and never when that is `0`),
  unreadable or invalid ones, and ones whose segments no longer match the
  current process-wide encoder settings (`RIVULET_TRANSCODE_X264_CRF`,
  `_VAAPI_QP`, `_AUDIO_BITRATE`, `_AUDIO_CHANNELS`) are deleted, as are
  partial `*.tmp*` files; beyond `RIVULET_HLS_MAX_SESSIONS`, the least
  recently used are deleted. A session keeps its own bitrate/size/preset
  snapshot, so later `/settings` or env changes to those only affect new
  sessions, exactly as without persistence.
- **One process per directory.** The directory is locked exclusively (flock
  on POSIX, an exclusive open on Windows); a second server pointed at the
  same `RIVULET_HLS_WORK_DIR` warns and runs without persistence.
- **Security.** `session.json` holds the media URL, which can embed
  credentials (e.g. debrid links). It's written `0600`, and the persist
  directory and session directories are `0700`. On POSIX an existing
  `rivulet-engine-hls-persist` that isn't owned by the server's user, or is a
  symlink, is refused (the server falls back to non-persistent mode); one
  that is ours but more open than `0700` is tightened to `0700`. Use a
  dedicated `RIVULET_HLS_WORK_DIR` owned by the server's user, not a shared
  directory such as `/tmp`.

## Preparing HLS sessions

`RIVULET_HLS_PREPARE=1` adds `/hlsv2/{id}/prepare`, so a front end can have
a title transcoded ahead of time without requesting every segment itself.
Off by default: the path then answers `404` exactly as before. Create the
session first with `GET /hlsv2/{id}/master.m3u8?mediaURL=…`, then:

- **`POST /hlsv2/{id}/prepare`** starts a background job that transcodes
  every segment of the session in playback order, skipping the cached ones,
  and answers `202` with the status below. For a multi-audio session it
  prepares the video and the default audio rendition, or every audio
  rendition with `?audio=all`. A job already running with the same `audio`
  is left alone, one with another `audio` is replaced, and a finished one is
  started again (it re-checks every segment).
- **`GET /hlsv2/{id}/prepare`** answers `200` with the status. It works
  before any job, so a front end can check what is cached already, and it
  doesn't count as an access to the session.
- **`DELETE /hlsv2/{id}/prepare`** stops the job and unpins the segments
  (`204`). The cached ones stay, and the eviction after the session's next
  cached live transcode trims them back under its cache cap; the session
  itself is kept.

Errors: `400` invalid id or `audio`, `404` unknown session, `409` the
session's duration is unknown (its probe failed), `507` the estimated
prepared size is over an explicit `RIVULET_HLS_SEGMENT_CACHE_BYTES`. POST
and DELETE are subject to the Origin allowlist like every state-changing
route.

The status:

```json
{
  "state": "running",
  "audio": "default",
  "segments": 3,
  "total": 6,
  "cached": 3,
  "uncached": 1,
  "failed": 0,
  "cachedBytes": 15040000,
  "estimatedBytes": 25560000,
  "renditions": [
    { "playlist": "video.m3u8", "map": "cc-" },
    { "playlist": "audio1.m3u8", "map": "cu-" }
  ],
  "issues": [{ "file": "a1seg1.ts", "reason": "no audio PES" }]
}
```

`state` is `idle` (no job, or stopped), `running`, `done` (the job finished
its passes: compare `cached` with `total`; a segment that live requests kept
busy through both passes stays `-`) or `failed` (the job stopped after
three transcode errors in a row, with the last one in `error`). Each `map`
has one character per segment: `c` cached, `-` not cached (not transcoded
yet, or evicted), `u` transcoded but not cached because the output failed
its check, `f` the transcode failed; `issues` gives the reason for up to 100
of them. `estimatedBytes` is the size at the session's peak bitrate
(maxrate plus audio), an upper bound in practice.

How the job behaves:

- **Live requests first.** Preparation transcodes share the
  `RIVULET_TRANSCODE_CONCURRENCY` slots with live requests, but take one only
  while no live request is waiting, and at most
  `RIVULET_HLS_PREPARE_CONCURRENCY` (default `1`) at once across every
  session, after any waiting [transcode-ahead](#transcode-ahead). A running
  preparation transcode is not interrupted, so with a single slot a live
  request can wait for up to one segment. A segment a live request is
  transcoding is left to it.
- **Pinned segments.** The segments a job prepares are never evicted and
  don't count against the per-session cache cap, so a long film isn't
  evicted while it is prepared; the cap still bounds what live playback
  adds around them (other audio renditions). With an explicit
  `RIVULET_HLS_SEGMENT_CACHE_BYTES`, a session whose estimated prepared size
  is over it is refused instead (`507`). Pinned segments still go with the
  session when it is deleted or evicted.
- **Retries.** Segments that failed or came out uncached are tried once
  more after the first pass.
- **Session lifetime.** A session being prepared is never evicted as idle.
  Once the job is done the session's idle TTL applies again, so give it a
  long `ttl` (see [Per-session HLS overrides](#per-session-hls-overrides)) or
  set `RIVULET_HLS_SESSION_TTL` to cover the wait until it is watched.
  `DELETE /hlsv2/{id}` stops the job and removes the session.
- **Restarts.** With `RIVULET_HLS_PERSIST`, the request is kept in the
  session directory's `prepare.json`, and the job resumes on start, once the
  server's own HTTP listener answers (torrent sources are read through it),
  skipping the segments already on disk. Each prepared segment counts as a
  use of the session, so a restart in the middle of a job keeps it; the
  session's TTL still applies to the time the server is down. Without
  persistence, jobs end with the process.

## Transcode-ahead

`RIVULET_HLS_TRANSCODE_AHEAD=N` makes the engine read ahead for its clients:
once it has answered a request for segment `n` of a rendition (`seg<n>.ts`,
or `a<k>seg<n>.ts` for audio stream `k`), it transcodes segments `n+1` to
`n+N` of that rendition in the background, not past the last one, so they
are cached by the time the client asks for them. After a seek, the first
segment is then the only one the client waits for a full transcode of.
`0`, the default, turns it off exactly as before; values above `30` (two
minutes) are capped. A session whose duration is unknown (its probe found
none) gets no transcode-ahead.

- **One worker per session and rendition.** It transcodes one segment at a
  time, in order, and skips the cached ones. While the client plays on
  through the window, the same worker carries on and the window moves
  along; requests that finish out of order, or a step back of up to `N`
  segments, don't count as a seek. Any other request is a seek: the worker
  is cancelled as the request arrives, including a transcode it is running,
  since nobody wants that segment now, and a new one starts once the
  request is answered. The worker belongs to the session, not to a client,
  so clients sharing a session far apart replace each other's.
- **Live requests first.** Transcode-ahead shares the
  `RIVULET_TRANSCODE_CONCURRENCY` slots with live requests, but takes one
  only while no live request is waiting, and at most
  `RIVULET_HLS_TRANSCODE_AHEAD_CONCURRENCY` (default `1`) at once across
  every session. It goes before [preparation](#preparing-hls-sessions),
  which uses the same low-priority scheduler. A running transcode is not
  interrupted for a live request (other than the seek above), so with a
  single slot a live request can wait for up to one segment. A client that
  asks for a segment the worker is transcoding waits for that transcode
  instead of starting another.
- **Failures.** A segment a live request or preparation is transcoding is
  left to it. A failed transcode stops the worker; the client's next
  request starts a new one.
- **Cache and lifetime.** The segments are cached like any other, under the
  session's cache cap, and pinned when a preparation of the session covers
  that rendition. A worker keeps its session from being evicted while it
  runs, but doesn't count as an access, so a session nobody plays still
  goes idle by its TTL. `DELETE /hlsv2/{id}` and shutdown stop it.

## Relay cache for remote sources

Every HLS segment is a fresh `ffmpeg` run, and each one reads the source's
container header and index again before it seeks to the segment's data. For
Matroska, or MP4 with its index at the end, that is about four range
requests per segment, of which only the last one is new; a faststart MP4
takes two. Against a host that takes ~400 ms to the first byte (debrid
hosts often do), the repeated requests cost over a second per segment.

`RIVULET_HLS_RELAY_CACHE_BYTES` (off by default) gives the loopback relay
that every remote `ffmpeg` input goes through that much memory to answer
those reads itself:

- **What is kept.** The bytes relayed from an offset `ffmpeg` has asked a
  source for before, up to a quarter of the budget per range; the least
  recently used ranges go first. A data read at a new offset is never
  buffered, so in steady state each segment makes one upstream request.
  Reads being recorded reserve their bytes too, up to the budget again, so
  the cache can use up to twice the setting at once.
- **What is answered.** A request at a recorded offset, or inside a
  recorded range, gets the recorded `206` headers and bytes. Bytes past the
  recorded ones are fetched from upstream as a range of their own: at once
  for a closed range, and for an open one after a short wait for `ffmpeg`
  to close the request first, which it usually does.
- **What is compared.** Only a single-range `GET` answered by a plain `206`
  for exactly the range asked, with a known total length, is recorded or
  compared; never a playlist, a manifest, a compressed or shortened
  response, a body longer than its range, or an error. Each one is compared
  on its total length, `ETag` and `Last-Modified`, and a difference turns
  the cache off for that source for good, whether the file changed or the
  servers behind the URL disagree. When the difference shows up while
  finishing an answer from memory, that one transfer fails rather than mix
  two files. A source with neither validator is compared on its length
  alone.
- **Lifetime.** Ranges belong to the relay token, one per upstream URL, so
  sessions of the same URL share them, and go with the token.

An answer from memory is sniffed at its own offset, as an upstream response
would be, and every upstream request, continuations included, goes through
the same client as without the cache.
