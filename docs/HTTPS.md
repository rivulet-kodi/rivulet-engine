<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

# HTTPS for the Stremio UI ("HTTPS endpoint for streaming")

Stremio's settings panel shows an **"HTTPS endpoint for streaming"**
(it. *"Endpoint HTTPS per lo streaming"*) dropdown listing your machine's LAN IP
addresses, plus a **remote URL** field that becomes something like
`https://192-168-0-62.<hash>.stremio.rocks`.

This page explains what that control does, how `rivulet-engine` implements it,
and how to set it up.

---

## Why HTTPS is needed

The Stremio web app (`https://web.stremio.com`) and the desktop app's web shell
are served over **HTTPS**. Browsers refuse to let an HTTPS page talk to a
plaintext `http://<LAN-IP>:11470` streaming server (mixed-content / secure-context
rules). To stream from your own server you therefore need it reachable over
**HTTPS with a certificate the browser trusts**.

A self-signed cert on `:12470` works for shells that ignore cert errors, but the
browser web app will reject it. The "HTTPS endpoint for streaming" dropdown
solves this with a **browser-trusted certificate** that still points at your
local server.

---

## How it works

When you pick a LAN IP (e.g. `192.168.0.62`) in the dropdown, Stremio calls the
streaming server's `/get-https` endpoint with your account's **authKey**:

```
GET /get-https?ipAddress=192.168.0.62      (authKey via X-Stremio-AuthKey header)
```

The server then:

1. Requests a Let's Encrypt wildcard certificate for `*.<hash>.stremio.rocks`
   from `api.strem.io` using your authKey.
2. Builds the per-IP domain by dashing the IP:
   `192.168.0.62` → `192-168-0-62.<hash>.stremio.rocks`.
3. Installs `https-cert.pem` / `https-key.pem` under `APP_PATH` and **hot-swaps**
   the live HTTPS listener on `HTTPS_PORT` (default `12470`) — no restart.
4. Returns `{ ipAddress, domain, port }`; the UI then talks to
   `https://192-168-0-62.<hash>.stremio.rocks:12470`.

`*.stremio.rocks` is a public wildcard DNS zone whose records resolve a dashed-IP
hostname back to that exact IP. So `192-168-0-62.<hash>.stremio.rocks` resolves to
`192.168.0.62` (your LAN server), while the certificate is a real, browser-trusted
Let's Encrypt cert. That is what makes the green-padlock connection to a local
device possible.

> The authKey is cached under `APP_PATH` so the cert can be renewed later without
> another click (see [Auto-renewal](#unattended-auto-renewal)).

---

## Prerequisites

- **A logged-in Stremio account.** `/get-https` needs a valid authKey issued by
  `api.strem.io`. Without an account login there is no way to provision the cert.
- **`HTTPS_PORT` enabled and reachable.** Default `12470` for the binary; the
  client connects to the chosen IP on that port.
- **The server reachable at the chosen LAN IP.** Pick an address that the device
  running the Stremio UI can actually route to.

---

## Setup — binary

1. Run the server with HTTPS enabled (default):

   ```sh
   ./rivulet-engine         # http on :11470, https on :12470
   ```

2. In Stremio, open **Settings → Streaming server**. Confirm it is connected to
   your server, then use the **"HTTPS endpoint for streaming"** dropdown to pick
   your LAN IP. Stremio provisions the cert and switches to the
   `…stremio.rocks` URL automatically.

That's it — no env vars required for the interactive flow.

---

## Setup — container / Podman / Docker

The image ships with `HTTPS_PORT=0` (disabled), so you must **enable and publish**
the HTTPS port and **persist `APP_PATH`** (`/data`) so the issued cert survives
restarts:

```sh
podman run -d \
  --name rivulet-engine \
  -p 11470:11470 \
  -p 12470:12470 \
  -e HTTPS_PORT=12470 \
  -v rivulet-engine-data:/data \
  rivulet-engine
```

Then complete the dropdown step above. (Swap `podman`→`docker`; flags are
identical.)

> **Not applicable behind an edge proxy / HuggingFace Spaces.** When a platform
> terminates TLS at the edge and routes only one port (HF routes `app_port`
> 11470), the in-container HTTPS listener and the `…stremio.rocks` flow are not
> used — the UI talks to the platform's own HTTPS origin. See
> [CONTAINER.md](CONTAINER.md).

---

## Unattended / auto-renewal

Set the authKey explicitly so the server can provision and **auto-renew** the cert
on its own (headless setups, or to refresh before expiry without re-clicking):

| Variable | Purpose |
|---|---|
| `RIVULET_CERT_AUTHKEY` | Stremio authKey used to auto-provision/renew a trusted cert from `api.strem.io`. If unset, the key cached from a prior `/get-https` call is reused. |
| `RIVULET_CERT_IP` | IP encoded into the provisioned cert's domain. Defaults to the first non-loopback IPv4. |

The background renewer is a **no-op** until an authKey is available, so it never
disturbs a plain HTTP or self-signed setup unless you opt in.

```sh
podman run -d \
  --name rivulet-engine \
  -p 11470:11470 -p 12470:12470 \
  -e HTTPS_PORT=12470 \
  -e RIVULET_CERT_AUTHKEY=<your-stremio-authkey> \
  -e RIVULET_CERT_IP=192.168.0.62 \
  -v rivulet-engine-data:/data \
  rivulet-engine
```

### Getting your authKey

The authKey is the session token of a logged-in Stremio account. The interactive
dropdown supplies it for you; for the headless `RIVULET_CERT_AUTHKEY` path you can
read it from a logged-in Stremio web session (browser dev tools → the
`api.strem.io` login response / stored auth) or from the Stremio app's local
state. Treat it like a password — it is cached at `APP_PATH/cert-authkey` with
mode `0600`.

---

## Self-signed fallback

If you only use a web shell that tolerates self-signed certs (e.g. a WebKitGTK
desktop wrapper), you don't need `/get-https` at all — just enable `HTTPS_PORT`.
The server serves a persisted cert if present, otherwise a self-signed one. The
browser web app at `web.stremio.com` will **not** trust this; use the
`…stremio.rocks` flow above for the browser.

---

## Troubleshooting

- **Dropdown is empty / no IP listed.** The UI lists the addresses the server
  reports. Make sure the client is actually connected to the streaming server and
  that the host has a non-loopback IPv4. Pin one with `RIVULET_CERT_IP`.
- **Provisioning fails (HTTP 400 / "missing authKey").** You are not logged in,
  or the authKey expired — log in to Stremio again and retry.
- **Cert provisioned but the page still warns.** Confirm you opened the
  `…stremio.rocks` URL on `HTTPS_PORT` (default `12470`) and that the port is
  published/reachable. In containers, both `-p 12470:12470` and
  `-e HTTPS_PORT=12470` are required.
- **Cert lost after restart.** Persist `APP_PATH` (`-v …:/data`); the cert lives
  at `APP_PATH/https-cert.pem` + `https-key.pem`. Set `RIVULET_CERT_AUTHKEY` for
  automatic re-provisioning.
- **Renewal not happening.** Ensure `RIVULET_CERT_AUTHKEY` is set (or a prior
  interactive `/get-https` cached one) and that `APP_PATH` is persistent.

---

## Related

- Env vars: `HTTPS_PORT`, `RIVULET_CERT_AUTHKEY`, `RIVULET_CERT_IP` — see the
  [configuration reference](CONFIGURATION.md).
- Container/HuggingFace specifics: [CONTAINER.md](CONTAINER.md).
- API reference: `GET /get-https` in the [OpenAPI spec](../internal/api/openapi.json).
