# zonepair

`stub-server` runs on machine B and spawns a working `claude` command for each
connection. This guide deploys the server using the published Docker image.
Claude's own configuration is outside this guide.

## 1. Create `compose.yaml` on machine B

```yaml
services:
  b:
    image: ${B_IMAGE:-ghcr.io/samuka007/zonepair-server:latest}
    restart: unless-stopped
    environment:
      TOKEN: ${B_TOKEN:-}
      CLAUDE_CMD: ${B_CLAUDE_CMD:-}
    ports:
      - "${B_PUBLISH:-127.0.0.1:7789:7789}"
    volumes:
      - stub-data:/data
      - b-home:/root

volumes:
  stub-data:
  b-home:
```

The repository's `compose.yaml` contains this server configuration too.

## 2. Configure `.env` in the same directory

Generate the server's bearer token once:

```bash
umask 077
printf 'B_TOKEN=%s\n' "$(openssl rand -hex 32)" > .env
```

Keep this file for subsequent restarts. Docker Compose reads it automatically.

| Setting in `.env` | Meaning |
| --- | --- |
| `B_TOKEN` | Server bearer token. An empty value disables authentication; keep it set. |
| `B_PUBLISH` | Defaults to `127.0.0.1:7789:7789`. Set `0.0.0.0:7789:7789` only when another machine must reach the port directly. |
| `B_CLAUDE_CMD` | Command the server spawns inside the container. Defaults to the image's `claude`. |
| `B_IMAGE` | Defaults to `ghcr.io/samuka007/zonepair-server:latest`. Set a release tag here to pin a version. |

## 3. Pull and start

Run from the directory containing both files:

```bash
docker compose pull b
docker compose up -d b
docker compose logs --tail=50 b
```

## 4. Expose through Cloudflare Tunnel

If `cloudflared` runs directly on the same host, keep the default loopback
port mapping and set the tunnel's service URL to:

```text
http://127.0.0.1:7789
```

If `cloudflared` is another container on the same Compose network, use
`http://b:7789` instead. The tunnel must forward both HTTP and WebSocket
requests. Cloudflare provides public TLS; the bearer token provides server
authentication. The server itself listens on plain HTTP/WS.

## 5. Check the server

Without a token, the published port should return **401**:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:7789/v1/store/sessions
```

Check an authenticated request using the token already inside the container:

```bash
docker compose exec -T b sh -c 'curl -fsS -H "Authorization: Bearer $TOKEN" http://127.0.0.1:7789/v1/store/sessions'
```

Expected: `{"ok":true,"sessions":[...]}`. These checks do not invoke Claude.

## 6. Use it from machine A

Machine A runs stock OpenCode plus the `stub-client` binary. All Claude
traffic relays to machine B through the server URL; A never needs Claude
credentials. OpenCode must be **≥ 2.2.3**.

Download `stub-client-<system>` for your platform from
[Releases](https://github.com/Samuka007/zonepair/releases)
(`stub-client-x86_64-linux`, `stub-client-aarch64-linux`,
`stub-client-aarch64-darwin`, `stub-client-x86_64-windows.exe`) and install
it as `~/.local/bin/stub-client`. On macOS, remove the quarantine flag once
after a browser download:
`xattr -d com.apple.quarantine ~/.local/bin/stub-client`.

Then configure. Interactive on a terminal — it asks for the server URL, the
bearer token (hidden input; empty = none) and the mirror directory:

```bash
stub-client setup
```

Scripts pass all three flags together (`--token ""` for a tokenless server);
partial flags fail without writing anything:

```bash
stub-client setup --url wss://zonepair.example.com \
  --token "$(sed -n 's/^B_TOKEN=//p' .env)" \
  --mirror "$HOME/.claude-remote-mirror"
```

Everything lands in the platform config directory at
`zonepair/client.toml`, permissions 0600 (`~/.config/zonepair/client.toml`
on Linux, `~/Library/Application Support/zonepair/client.toml` on macOS).
`setup` also
points OpenCode's server env at the stub, bootstraps a local mirror of B's
session store, and runs a preflight — re-check anytime with
`stub-client doctor --url <URL>`. Use OpenCode normally and pick the Claude
Agent provider (`opencode auth login claude-agent`); `stub-client sync`
re-synchronizes the mirror on demand, `stub-client unset` removes everything.
Upgrading from a rc.5-or-earlier install:
[docs/CLIENT.md](docs/CLIENT.md#migrating-from-rc5-and-earlier-installs).

Alternative for a first try on one host: the published
`ghcr.io/samuka007/zonepair-client` image bundles OpenCode and `stub-client`
preconfigured (`CLAUDE_STUB_URL=ws://b:7789`). Run it on the same Compose
network as `b` and pass the token through:

```bash
docker run --rm -it --network zonepair_default \
  -e CLAUDE_STUB_TOKEN="$(sed -n 's/^B_TOKEN=//p' .env)" \
  ghcr.io/samuka007/zonepair-client
```

(`zonepair_default` is the network Compose created for `b` — check the real
name with `docker network ls`, it is `<directory>_default`.) The container
keeps no volume: its OpenCode state is ephemeral.

## Persistent data

`stub-data` stores sessions and configuration under `/data/claude`, plus
server logs under `/data/logs`. The server sets the spawned process's
`CLAUDE_CONFIG_DIR` to `/data/claude`. `b-home` preserves `/root`.

Recreating the container keeps these volumes. **Do not run
`docker compose down -v` unless you intend to delete the stored data.**

[Protocol reference](docs/PROTOCOL.md) · [Other documentation](docs/)
