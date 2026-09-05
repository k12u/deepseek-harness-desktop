# Standalone Web + Remote (server deployment)

Run DeepSeek Harness and the DSH Remote translation proxy on a headless server
(for example, a Mac or Linux box on your tailnet), so that:

- **Browsers** keep using the Harness Web UI directly over Tailscale.
- **iPhone/Android DSH Remote** connects over Tailscale to the translation
  proxy, which speaks the Remote v1 contract and carries the Harness browser
  session cookie internally.

No desktop app is required. The phone never needs the Harness startup token;
tailnet membership is the boundary, exactly like a user-managed HTTPS endpoint.

## What it runs

`scripts/standalone-web-remote.mjs` supervises two child processes:

1. `dsh web --host 127.0.0.1 --port <web-port> --no-open` — the Harness Web UI,
   loopback only. The launcher captures the per-launch startup token from the
   readiness line and prints a browser bootstrap URL once.
2. `scripts/lan-remote-proxy.mjs` — the Remote v1 → Harness 0.1.2 translation
   proxy on `<remote-port>`, bound to loopback (bearer optional) or the LAN
   interface (bearer required).

When the Harness restarts, its launch token changes; the launcher restarts the
proxy with the new token automatically. Browser sessions survive Harness
restarts (the Harness cookie is signed by a persistent secret in `DSH_HOME`),
and the phone needs no re-pairing.

## Requirements

- Node.js 22.19 or newer.
- The Harness runtime: `npm install @deepseek-ai/dsh` (or a global install)
  somewhere Node can resolve, or this repository checked out with `npm ci`.
- The `ws` package next to the scripts (`npm ci` in this repository already
  installs it; for a minimal copy, run `npm install ws` in the scripts folder).

## Run

From a checkout of this repository:

```bash
node scripts/standalone-web-remote.mjs \
  --dsh-bin "$(npm root -g)/@deepseek-ai/dsh/lib/bin.js" \
  --web-port 8080 \
  --remote-port 8766 \
  --harness-version "$(node -p "require('@deepseek-ai/dsh/package.json').version")"
```

`--dsh-bin` accepts either the path to `@deepseek-ai/dsh/lib/bin.js` (spawned
with `node --expose-internals`) or a `dsh` command name.

The launcher prints:

```text
browser bootstrap URL (open once, the session cookie then persists):
  http://127.0.0.1:8080/?token=<startup-token>
remote proxy ready: http://127.0.0.1:8766/
```

## Tailscale Serve

```bash
# Browsers: the Harness Web UI directly (token auth handled by the Harness).
tailscale serve --bg --https=443 http://127.0.0.1:8080
# Phones: the translation proxy (Remote v1).
tailscale serve --bg --https=8443 http://127.0.0.1:8766
```

- Browser: open `https://<machine>.ts.net/?token=<startup-token>` once; the
  Harness sets a 30-day cookie and the URL can be bookmarked without the token.
- iPhone: enter `https://<machine>.ts.net:8443/` in DSH Remote's **HTTPS
  address** field. No QR code and no token are required.

## Optional hardening

By default the proxy accepts unauthenticated phone requests (the tailnet is
the boundary). To additionally require a bearer credential, pass
`--remote-token <64 lowercase hex>` to the launcher, add
`--transport=lan`-style pairing to the phone URL —
`harnessremote://connect?url=https%3A%2F%2F<machine>.ts.net%3A8443%2F&token=<same value>`
— and import it on the phone, or rely on the desktop pairing flow instead.
`--bind lan` serves the proxy on the LAN interface and always requires the
bearer credential; use it only for same-Wi-Fi phone access.

## systemd example

```ini
[Unit]
Description=DeepSeek Harness web + DSH Remote proxy
After=network-online.target

[Service]
ExecStart=/usr/bin/node /opt/dsh-remote/scripts/standalone-web-remote.mjs --dsh-bin /opt/dsh/node_modules/@deepseek-ai/dsh/lib/bin.js
Environment=DSH_HOME=/var/lib/dsh
Restart=on-failure
User=dsh

[Install]
WantedBy=multi-user.target
```

Keep the launcher's stdout out of persistent logs: it contains the one-time
bootstrap URL (the Harness logs its own readiness line with the token
redacted, and the launcher never logs the proxy bearer).
