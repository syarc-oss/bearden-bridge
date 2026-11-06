# bearden-bridge

Standalone daemon bridging the Bear Den MeshCore room server (serial,
/dev/bearden) to internet messaging backends.

Run: `cargo build --release && ./target/release/bearden-bridge`
API: http://127.0.0.1:8095 (status/events/users/approve/ban/send/killswitch)
See AGENTS.md for architecture, configuration, and the tunnel.

## Inter-bridge tunnel (bulletin-board relay)

Store-and-forward relay between two bridges on distant meshes (e.g. USA
<-> Australia). Far-mesh messages are queued and injected as periodic
rate-limited digests (`[AU 14:30] alice: hello | bob: wx`), never live;
local mesh events are batched to the peer every flush interval. Peer auth
is a shared secret (`X-Tunnel-Token` header). Loop-safe: digest posts are
recognized and never re-tunneled.

Enable on both bridges (env; see src/tunnel_backend.rs for the full list):

    BEARDEN_TUNNEL_PEER=https://peer.example.net
    BEARDEN_TUNNEL_TOKEN=<shared secret>
    BEARDEN_TUNNEL_TAG=US            # peer sets AU
    BEARDEN_TUNNEL_PEER_TAG=AU

Routes: `POST /tunnel/ingest` and `GET /tunnel/peek?since=<id>` (peer,
token-authed), `GET /tunnel/status`, `POST /tunnel/flush` (admin token).
E2E test with two instances over PTYs: `python3 tools/tunnel_test.py`.

## Media Mesh (Open TAK interop)

Reassembles Open TAK voice/photo clips (0x5643 datagrams on a shared PSK
channel over the second LoRa profile, "radio2") between meshes: salted
retransmits, 0x72 binary-fetch responder, session reopen on late packets,
dual-packetization-aware assembly, and cross-channel forwarding. See
`src/media.rs`.

## Installing

Requirements: Rust (stable), Linux, a MeshCore room-server node on a
serial port (e.g. `/dev/ttyACM0`), optionally SQLCipher not required
(plain SQLite via rusqlite).

    cargo build --release
    sudo install -m 755 target/release/bearden-bridge /usr/local/bin/
    sudo cp bearden-bridge.service /etc/systemd/system/   # edit env vars!
    sudo systemctl enable --now bearden-bridge

Edit the unit first: set `BEARDEN_ADMIN_TOKEN`, serial port, and any
backends (XMPP/Matrix env, tunnel peer). The admin API binds 127.0.0.1
by default; put nginx TLS in front if you need remote access.
API + web UI: http://127.0.0.1:8095 (paste the admin token in the UI).

Configuration is entirely via environment variables — see the commented
list in `bearden-bridge.service` and `AGENTS.md`.

## Reporting bugs

Open a GitHub issue with: bridge version (`git rev-parse HEAD`), the
relevant `journalctl -u bearden-bridge` excerpt (redact tokens!), radio
firmware version/profile, and steps or a packet capture that reproduce
it. For media issues, `GET /media/debug` output (session table) is very
helpful.

## Contributing

Bug fixes are welcome — please open an issue first describing the
problem and your planned approach, then submit a PR referencing it.
Small, focused changes only. Run `cargo test` and `cargo clippy` before
submitting; new features should come with a unit test where feasible.

## The syarc ecosystem

bearden-bridge is one component of a small set of self-hostable applications
designed to let an individual or a community operate independently and
securely on their own hardware and networks:

- **bearden-bridge** (this project) — bridges a MeshCore LoRa room-server
  radio to Matrix/XMPP chat, relays store-and-forward tunnels between
  distant meshes, and carries Open TAK voice/photo media over a second
  LoRa profile.
- **[syarc-console](https://github.com/syarc-oss/syarc-console)** — the web
  console: accounts, app permissions, messaging, library/media,
  calendar/tasks, encrypted financial ledgers, surveillance, knowledge
  base, and mesh platform tooling.

Each app is standalone and speaks open protocols (Matrix, XMPP, HTTP), so
deployments can be decentralized: one bridge per mesh, one console per
household, federated over chat and radio with no cloud dependency.

## Acknowledgements

Significant portions of this codebase were developed with the Z.AI GLM
large language model acting as an autonomous coding agent (Crush CLI),
under human direction and review.
