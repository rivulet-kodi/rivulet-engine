<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# Security

This is a **localhost service with no authentication** - treat anything that
can reach the port as fully trusted. **Recommended:** set
`BIND_ADDRESS=127.0.0.1` — the default binds **every** interface, including
any globally routable IPv6 address, to an unauthenticated API. Do not expose
`:11470`/`:12470` to untrusted networks.

Any website open in the user's browser can reach `http://127.0.0.1:11470`
directly (ordinary same-origin-policy behavior — the browser allows the
request, it just can't read a cross-origin response without CORS). To stop an
arbitrary page from silently driving the API just because the port is
reachable, state-changing routes and `/proxy` check the request's `Origin`
header against an allowlist (see `RIVULET_ALLOWED_ORIGINS` in [Configuration](CONFIGURATION.md)); requests
with no `Origin` header (native players, curl, most non-browser clients) are
unaffected. Browsers omit `Origin` on cross-site links, images and iframes, so
a request with no `Origin` but `Sec-Fetch-Site: cross-site` is refused (`403`)
on side-effecting routes (`/removeAll`, `/{infoHash}/create|remove`,
`/get-https`, `/casting`, the DVR `/record` family); media routes loaded by `<video>` or native HLS are
not affected. Proxied and streamed content is served with
`X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox`, so an
HTML/SVG body cannot run script on the server's own origin.

By design the server shells out to `ffmpeg`/`yt-dlp` and acts as an open
reverse proxy (`/proxy`) for the local web UI. Remote URLs given to
`ffmpeg`/`ffprobe` (`/probe`, `/tracks`, `/hlsv2` `mediaURL=`) are never
dialed by the ffmpeg/yt-dlp subprocess directly — they're fetched through a
guarded loopback relay, so the subprocess only ever talks to this server and
the SSRF guard (private/loopback/link-local/cloud-metadata blocking,
re-checked at dial time to defeat DNS rebinding) governs the actual fetch.
The same dial-time guard, including redirects, covers archive/NZB downloads
(`RIVULET_ARCHIVE_ALLOW_PRIVATE`) and FTP (`RIVULET_FTP_ALLOW_PRIVATE`). The
relay recognises HLS playlists by content as well as by type, and refuses
DASH manifests (`415`), whose nested URLs it does not rewrite. A `mediaURL`
on the server's own origin is accepted only for media routes (torrent, archive
and local-addon files, `/proxy`, `/ftp`, `/yt`) and goes through the same relay,
local files included: a local file that is itself an HLS playlist has its
entries re-checked, so one naming a control route (`/removeAll`) or a
private host is refused rather than fetched by ffmpeg.

For `/proxy` and `/extractor`, private destinations are blocked unless
`RIVULET_PROXY_ALLOW_PRIVATE=1` and no proxy password or IP ACL is set. To let
an exposed, password-protected server reach a specific LAN host (a NAS, a local
embed page), list it in `RIVULET_PROXY_PRIVATE_ALLOW` instead of opening all
private ranges.

Media URLs (`/hlsv2` `mediaURL`, `/probe`, `/tracks`) always refuse private
destinations; those routes can't be password-gated, so there is no allow-all
switch. To transcode from a specific LAN host, list it in
`RIVULET_HLS_PRIVATE_ALLOW`. Entries match by IP on any port, so list the
media host itself, never loopback, which would also expose this server's own
API.
