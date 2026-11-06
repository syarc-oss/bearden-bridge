# AGENTS.md — bearden-bridge

Standalone Rust daemon bridging a Bear Den MeshCore radio (serial) to
internet chat backends, plus a store-and-forward tunnel between two
bridges on distant meshes. Independent of any console product; a web
console may act as a client of this API, but the bridge never depends
on one.

## Architecture (src/)

- `main.rs` — startup: store, serial link, backend spawn loop, API bind.
  Defaults: serial `/dev/bearden`, SQLite DB `bridge.db` in the repo, API
  `127.0.0.1:8095`. All overridable by env (below).
- `serial.rs` — MeshCore serial reader (dedicated thread) + inject.
- `store.rs` — SQLite: users, events, tunnel_queue, config (kv). Approve-
  before-transmit policy for internet users lives here.
- `backends.rs` — `Backend` trait, `BackendCtx`, `Egress` (policy gate for
  anything going to RF). Each backend is an independent task; mesh events
  fan out to all via a broadcast channel. **Multiple backends run
  simultaneously by design** (e.g. XMPP + Matrix + Tunnel + Log).
- `xmpp_backend.rs` / `matrix_backend.rs` — env-configured chat backends.
- `tunnel_backend.rs` — inter-bridge store-and-forward relay (see below).
- `api.rs` — admin API + built-in web UI (`ui.html`; paste the admin
  token in the token box, or every fetch 401s silently).
- `events.rs` — RoomEvent/BridgeCmd types.

## Tunnel (inter-bridge relay)

Pairs two bridges on distant meshes over the internet. Far-mesh messages
queue in `tunnel_queue` and go out as rate-limited digests
(`[US 14:30] alice: hi | bob: wx`) every flush interval; local mesh events
batch to the peer's `POST /tunnel/ingest` (or poll `GET /tunnel/peek` in
poll mode). Digests are recognized and never re-tunneled (echo-loop safe).

**Config is runtime-editable** (persisted in the `config` table; env vars
`BEARDEN_TUNNEL_*` are only the initial default):

- `GET /tunnel/config` — current config incl. shared secret (admin token)
- `POST /tunnel/config` — `{enabled, peer, token, tag, peer_tag,
  flush_interval_secs, max_msgs, max_posts, rf_gap_secs, mode}`; omitted
  fields keep current values; `enabled:false` disables and un-persists.
- `GET /tunnel/status` (no admin token), `POST /tunnel/flush` (admin).
- Peer routes `/tunnel/ingest`, `/tunnel/peek` auth via `X-Tunnel-Token`,
  outside the admin-token middleware.

The tunnel backend task always runs; it idles while config is None and
re-reads the shared config every 5s tick, so changes apply without restart.

Example pairing (bridge A on 192.168.1.10, bridge B on 192.168.1.20; in
real deployments each side is usually behind a public URL or port forward):

    # bridge A (tag US)                    # bridge B (tag AU)
    BEARDEN_TUNNEL_PEER=http://192.168.1.20:8095
    BEARDEN_TUNNEL_TOKEN=<shared secret>   # same on both
    BEARDEN_TUNNEL_TAG=US                  BEARDEN_TUNNEL_TAG=AU
    BEARDEN_TUNNEL_PEER_TAG=AU             BEARDEN_TUNNEL_PEER_TAG=US

## Admin API auth

If `BEARDEN_ADMIN_TOKEN` is set, every UI/JSON route requires
`Authorization: Bearer <t>` or `?token=<t>` (except the peer tunnel routes
above). Unset = unauthenticated local-only mode.

## Environment

`BEARDEN_PORT` (default /dev/bearden), `BEARDEN_DB`, `BEARDEN_API`,
`BEARDEN_ADMIN_TOKEN`, `BEARDEN_XMPP_*` (JID/PASSWORD/ROOM/NICK),
`BEARDEN_MATRIX_*` (HOMESERVER/USER/PASSWORD/ROOM),
`BEARDEN_TUNNEL_*` (initial tunnel defaults only; see
src/tunnel_backend.rs header for the full list).

## Build / test / run

    cargo build --release
    cargo test                      # digest/echo-loop unit tests
    python3 tools/tunnel_test.py    # E2E: two instances over PTYs
    ./target/release/bearden-bridge # run with defaults

Dev without a radio: point `BEARDEN_PORT` at a PTY, use a throwaway
`BEARDEN_DB`, and a spare `BEARDEN_API` port:

    python3 - <<'EOF'
    import os, pty
    master, slave = pty.openpty()
    print("BEARDEN_PORT=" + os.ttyname(slave))
    EOF

Deployment: a sample systemd unit ships as `bearden-bridge.service`
(edit User, paths, and tokens; never ship real credentials).

## Gotchas

- One TLS provider: `rustls::crypto::ring::default_provider()` is installed
  first thing in main — reqwest and tokio-xmpp otherwise fight over it.
- The XMPP Agent can't be reused after disconnect; backends are rebuilt in
  a restart loop (10s) by design.
- The serial reader `exit(2)`s on fatal errors so systemd restarts it.
- Tunnel `flush_interval` is checked against a 5s tick in the backend, not
  a tokio interval — keep it that way so runtime config changes apply.
- `build_digest` coalesces by `cfg.max_msgs * cfg.max_posts` with a
  `(+N more)` overflow note; RF lines are hard-capped at `MAX_RF_CHARS`.
- If a udev symlink names the radio (e.g. /dev/bearden via
  `ATTRS{idVendor}=="303a", ATTRS{serial}=="..."`), the board dropping off
  USB makes the symlink vanish and the service crash-loops until replug.
