//! Store-and-forward inter-bridge tunnel ("bulletin board" relay).
//!
//! Pairs two bridges on distant meshes (e.g. USA <-> Australia) over the
//! internet. Mesh traffic is NOT forwarded live: far-mesh messages land in
//! a persistent queue and are injected onto the local LoRa room as periodic
//! digests (default every 30 min), rate-limited to protect airtime. Local
//! mesh events are batched and pushed to the peer's /tunnel/ingest between
//! flushes.
//!
//! Transport auth is a shared secret (BEARDEN_TUNNEL_TOKEN) checked via the
//! `X-Tunnel-Token` header on both sides.
//!
//! Env:
//!   BEARDEN_TUNNEL_PEER       peer base URL, e.g. https://au.example.net  (required)
//!   BEARDEN_TUNNEL_TOKEN      shared secret with the peer                (required)
//!   BEARDEN_TUNNEL_TAG        our region tag shown in digests, e.g. "US" (default: hostname)
//!   BEARDEN_TUNNEL_PEER_TAG   peer's tag, used to skip digest echo loops  (default "PEER")
//!   BEARDEN_TUNNEL_FLUSH_SECS digest flush interval                       (default 1800)
//!   BEARDEN_TUNNEL_MAX_MSGS   messages coalesced per digest post          (default 8)
//!   BEARDEN_TUNNEL_MAX_POSTS  RF posts per flush                          (default 2)
//!   BEARDEN_TUNNEL_RF_GAP_SECS minimum gap between RF posts in a flush    (default 60)
//!   BEARDEN_TUNNEL_MODE       push (we POST batches) | poll (we GET peek) (default push)

use crate::backends::{Backend, BackendCtx};
use crate::events::{RoomEvent, RoomEventKind};
use crate::store::{Store, MAX_RF_CHARS};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_OUTBOX: usize = 200; // far-mesh events buffered between pushes
const MAX_INGEST: usize = 100; // per request

/// Runtime-mutable tunnel configuration, shared between the admin API
/// (writer) and the tunnel backend loop + peer routes (readers).
pub type SharedTunnelCfg = Arc<std::sync::RwLock<Option<TunnelConfig>>>;

pub fn shared_cfg(cfg: Option<TunnelConfig>) -> SharedTunnelCfg {
    Arc::new(std::sync::RwLock::new(cfg))
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TunnelMode {
    Push,
    Poll,
}

#[derive(Clone, PartialEq)]
pub struct TunnelConfig {
    pub peer: String,
    pub token: String,
    pub tag: String,
    pub peer_tag: String,
    pub flush_interval: Duration,
    pub max_msgs: usize,
    pub max_posts: usize,
    pub rf_gap: Duration,
    pub mode: TunnelMode,
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

impl TunnelConfig {
    pub fn from_env() -> Option<Self> {
        let peer = std::env::var("BEARDEN_TUNNEL_PEER").ok()?;
        let token = std::env::var("BEARDEN_TUNNEL_TOKEN")
            .ok()
            .filter(|t| !t.is_empty())?;
        let tag = std::env::var("BEARDEN_TUNNEL_TAG")
            .ok()
            .unwrap_or_else(hostname_tag);
        let peer_tag = std::env::var("BEARDEN_TUNNEL_PEER_TAG").unwrap_or_else(|_| "PEER".into());
        Some(TunnelConfig {
            peer: peer.trim_end_matches('/').to_string(),
            token,
            tag,
            peer_tag,
            flush_interval: Duration::from_secs(env_num("BEARDEN_TUNNEL_FLUSH_SECS", 1800)),
            max_msgs: env_num("BEARDEN_TUNNEL_MAX_MSGS", 8),
            max_posts: env_num("BEARDEN_TUNNEL_MAX_POSTS", 2),
            rf_gap: Duration::from_secs(env_num("BEARDEN_TUNNEL_RF_GAP_SECS", 60)),
            mode: if std::env::var("BEARDEN_TUNNEL_MODE")
                .unwrap_or_default()
                .eq_ignore_ascii_case("poll")
            {
                TunnelMode::Poll
            } else {
                TunnelMode::Push
            },
        })
    }

    /// Runtime configuration from the admin API / UI. `peer` and `token`
    /// are required; the rest fall back to sensible defaults.
    pub fn from_json(v: &Value) -> Result<Self, String> {
        let peer = v["peer"].as_str().unwrap_or_default().trim().trim_end_matches('/');
        if peer.is_empty() {
            return Err("peer URL required".into());
        }
        if !peer.starts_with("http://") && !peer.starts_with("https://") {
            return Err("peer must be an http(s) URL".into());
        }
        let token = v["token"].as_str().unwrap_or_default().trim().to_string();
        if token.is_empty() {
            return Err("shared tunnel token required".into());
        }
        let flush = v["flush_interval_secs"].as_u64().unwrap_or(1800).clamp(30, 86_400);
        let max_msgs = v["max_msgs"].as_u64().unwrap_or(8).clamp(1, 32) as usize;
        let max_posts = v["max_posts"].as_u64().unwrap_or(2).clamp(1, 10) as usize;
        let rf_gap = v["rf_gap_secs"].as_u64().unwrap_or(60).clamp(0, 600);
        let tag = v["tag"].as_str().unwrap_or_default().trim().to_string();
        Ok(TunnelConfig {
            peer: peer.to_string(),
            token,
            tag: if tag.is_empty() { hostname_tag() } else { tag },
            peer_tag: v["peer_tag"].as_str().unwrap_or("PEER").trim().to_string(),
            flush_interval: Duration::from_secs(flush),
            max_msgs,
            max_posts,
            rf_gap: Duration::from_secs(rf_gap),
            mode: if v["mode"].as_str().unwrap_or_default().eq_ignore_ascii_case("poll") {
                TunnelMode::Poll
            } else {
                TunnelMode::Push
            },
        })
    }

    pub fn to_json(&self) -> Value {
        json!({
            "peer": self.peer,
            "token": self.token,
            "tag": self.tag,
            "peer_tag": self.peer_tag,
            "flush_interval_secs": self.flush_interval.as_secs(),
            "max_msgs": self.max_msgs,
            "max_posts": self.max_posts,
            "rf_gap_secs": self.rf_gap.as_secs(),
            "mode": if self.mode == TunnelMode::Poll { "poll" } else { "push" },
        })
    }

    /// Digest posts open with "[TAG hh:mm]" so mesh readers see origin and
    /// both bridges can detect cross-tunnel echo loops.
    pub fn digest_prefix(&self, tag: &str) -> String {
        let (h, m) = utc_hhmm();
        format!("[{tag} {h:02}:{m:02}]")
    }

    /// Text produced by a digest post (ours or the peer's) must not be
    /// re-tunneled, otherwise digests would ping-pong forever.
    pub fn is_digest(&self, text: &str) -> bool {
        text.starts_with(&format!("[{} ", self.tag))
            || text.starts_with(&format!("[{} ", self.peer_tag))
    }
}

fn hostname_tag() -> String {
    std::fs::read_to_string("/etc/hostname")
        .unwrap_or_default()
        .trim()
        .to_uppercase()
        .chars()
        .take(8)
        .collect::<String>()
        .replace(['.', '-', '_'], "")
}

fn utc_hhmm() -> (u64, u64) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let day = secs % 86_400;
    (day / 3600, (day % 3600) / 60)
}

/// Build one or more digest lines (each <= MAX_RF_CHARS) from queued
/// messages. Returns (lines, consumed_count). Unconsumed messages stay
/// queued for the next flush; a `(+N more)` note is appended when the
/// backlog outgrows this flush.
pub fn build_digest(cfg: &TunnelConfig, origin: &str, msgs: &[(i64, String, String, String)]) -> (Vec<String>, usize) {
    let prefix = cfg.digest_prefix(origin);
    let mut lines = Vec::new();
    let mut line = prefix.clone();
    let mut consumed = 0;
    let budget = msgs.len().min(cfg.max_msgs * cfg.max_posts.max(1));
    for (i, (_, _, sender, text)) in msgs.iter().enumerate() {
        if i >= budget {
            break;
        }
        let item: String = format!(
            "{}: {}",
            sender,
            text.chars().take(64).collect::<String>()
        );
        let candidate = if line == prefix {
            format!("{line}{item}")
        } else {
            format!("{line} | {item}")
        };
        if candidate.chars().count() > MAX_RF_CHARS {
            if line != prefix {
                lines.push(line);
                line = format!("{prefix}{item}");
                if line.chars().count() > MAX_RF_CHARS {
                    line = line.chars().take(MAX_RF_CHARS).collect();
                }
            } else {
                line = candidate.chars().take(MAX_RF_CHARS).collect();
            }
        } else {
            line = candidate;
        }
        consumed += 1;
        if lines.len() + 1 >= cfg.max_posts && i + 1 < budget {
            // Room for exactly one more line: stop consuming new items.
            break;
        }
    }
    let leftover = msgs.len().saturating_sub(consumed);
    if leftover > 0 {
        let note = format!(" (+{leftover} more)");
        let room = MAX_RF_CHARS.saturating_sub(note.chars().count());
        line = line.chars().take(room).collect::<String>() + &note;
    }
    if line != prefix {
        lines.push(line);
    }
    (lines, consumed)
}

/// Drain the pending queue into rate-limited RF digest posts. Returns the
/// number of messages put on the radio. Called by the tunnel backend loop
/// and by the admin /tunnel/flush route.
pub async fn run_flush(
    store: &Arc<Store>,
    serial: &Arc<crate::serial::SerialLink>,
    cfg: &TunnelConfig,
) -> usize {
    if store.kill_switch() {
        return 0;
    }
    let mut sent_total = 0;
    let pending = store.tunnel_pending(1000);
    if pending.is_empty() {
        return 0;
    }
    // Group consecutive messages by origin so each region gets its digest.
    let mut idx = 0;
    while idx < pending.len() {
        let origin = pending[idx].1.clone();
        let mut group: Vec<(i64, String, String, String)> = Vec::new();
        while idx < pending.len() && pending[idx].1 == origin {
            group.push(pending[idx].clone());
            idx += 1;
        }
        let (lines, consumed) = build_digest(cfg, &origin, &group);
        if consumed == 0 {
            break; // malformed group; retry next flush
        }
        let ids: Vec<i64> = group[..consumed].iter().map(|(id, ..)| *id).collect();
        let mut failed = false;
        for (n, line) in lines.iter().enumerate() {
            store.remember_injected(line);
            if let Err(e) = serial.post(line) {
                eprintln!("[tunnel] rf post failed: {e}");
                failed = true;
                break;
            }
            store.log_event("tunnel_out", &origin, line);
            sent_total += if n == 0 { consumed } else { 0 };
            if n + 1 < lines.len() {
                tokio::time::sleep(cfg.rf_gap).await;
            }
        }
        if failed {
            store.tunnel_mark(&ids, "pending"); // retry next flush
            break;
        } else {
            store.tunnel_mark(&ids, "sent");
        }
    }
    sent_total
}

pub struct TunnelBackend {
    /// Shared runtime config; the backend idles while it is None.
    pub cfg: SharedTunnelCfg,
}


#[async_trait]
impl Backend for TunnelBackend {

    async fn run(self: Box<Self>, mut ctx: BackendCtx) -> anyhow::Result<()> {
        let shared = self.cfg;
        let store = ctx.egress.store.clone();
        let serial = ctx.egress.serial.clone();
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        // Mesh events are pushed in batches between flushes; on restart we
        // start from "now" (no history replay) but keep the buffer in memory
        // across push failures.
        let mut outbox: Vec<RoomEvent> = Vec::new();
        let mut poll_cursor: i64 = 0;
        // Short tick; the flush interval is checked against `last_flush`
        // each tick so runtime config changes take effect without restart.
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_flush = Instant::now();
        let mut logged_cfg: Option<TunnelConfig> = None;
        loop {
            tokio::select! {
                ev = ctx.inbound.recv() => {
                    if let Ok(ev) = ev {
                        let is_digest = shared.read().unwrap()
                            .as_ref().map(|c| c.is_digest(&ev.text))
                            .unwrap_or(false);
                        if ev.kind == RoomEventKind::Message
                            && !is_digest
                            && !store.was_injected(&ev.text)
                        {
                            if outbox.len() >= MAX_OUTBOX {
                                outbox.remove(0); // drop oldest
                            }
                            outbox.push(ev);
                        }
                    }
                }
                _ = tick.tick() => {
                    let cfg = shared.read().unwrap().clone();
                    let Some(cfg) = cfg else { continue }; // disabled: idle
                    if logged_cfg.as_ref() != Some(&cfg) {
                        println!(
                            "[tunnel] enabled: peer={} tag={} mode={:?} flush={}s",
                            cfg.peer, cfg.tag, cfg.mode, cfg.flush_interval.as_secs()
                        );
                        logged_cfg = Some(cfg.clone());
                        last_flush = Instant::now(); // fresh interval after change
                    }
                    if last_flush.elapsed() < cfg.flush_interval {
                        continue;
                    }
                    last_flush = Instant::now();
                    if cfg.mode == TunnelMode::Poll {
                        // Pull the peer's mesh events since our cursor.
                        let url = format!(
                            "{}/tunnel/peek?since={}&limit={}",
                            cfg.peer, poll_cursor, MAX_INGEST
                        );
                        if let Ok(resp) = http
                            .get(&url)
                            .header("X-Tunnel-Token", &cfg.token)
                            .send()
                            .await
                        {
                            if resp.status().is_success() {
                                if let Ok(body) = resp.json::<serde_json::Value>().await {
                                    if let Some(msgs) = body["msgs"].as_array() {
                                        for m in msgs {
                                            let id = m["id"].as_i64().unwrap_or(0);
                                            if id > poll_cursor {
                                                poll_cursor = id;
                                            }
                                            let (key, text) = (
                                                m["key"].as_str().unwrap_or("?"),
                                                m["text"].as_str().unwrap_or(""),
                                            );
                                            if !cfg.is_digest(text) {
                                                store.tunnel_enqueue(&cfg.peer_tag, key, text);
                                            }
                                        }
                                    }
                                }
                            } else {
                                eprintln!("[tunnel] poll failed: {}", resp.status());
                            }
                        }
                    } else if !outbox.is_empty() {
                        // Push our mesh events to the peer in one batch.
                        let msgs: Vec<_> = outbox
                            .iter()
                            .map(|e| json!({"key": e.key, "ts": e.ts, "text": e.text}))
                            .collect();
                        let url = format!("{}/tunnel/ingest", cfg.peer);
                        match http
                            .post(&url)
                            .header("X-Tunnel-Token", &cfg.token)
                            .json(&json!({"tag": cfg.tag, "msgs": msgs}))
                            .send()
                            .await
                        {
                            Ok(resp) if resp.status().is_success() => outbox.clear(),
                            Ok(resp) => {
                                eprintln!("[tunnel] push failed: {}", resp.status());
                                // 4xx means the peer rejected the batch for
                                // good: drop it rather than retry forever.
                                if resp.status().is_client_error() {
                                    outbox.clear();
                                }
                            }
                            Err(e) => eprintln!("[tunnel] push error: {e}"),
                        }
                    }
                    let sent = run_flush(&store, &serial, &cfg).await;
                    if sent > 0 {
                        println!("[tunnel] digest flush: {sent} msgs on RF");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> TunnelConfig {
        TunnelConfig {
            peer: "http://peer".into(),
            token: "t".into(),
            tag: "US".into(),
            peer_tag: "AU".into(),
            flush_interval: Duration::from_secs(60),
            max_msgs: 4,
            max_posts: 2,
            rf_gap: Duration::from_secs(1),
            mode: TunnelMode::Push,
        }
    }

    fn msgs(n: usize) -> Vec<(i64, String, String, String)> {
        (0..n)
            .map(|i| (i as i64, "AU".into(), format!("user{i}"), "hello world".into()))
            .collect()
    }

    #[test]
    fn digest_fits_budget() {
        let c = cfg();
        let (lines, consumed) = build_digest(&c, "AU", &msgs(3));
        assert_eq!(consumed, 3);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with("[AU "));
        assert!(lines[0].chars().count() <= MAX_RF_CHARS);
        assert!(lines[0].contains("user0: hello world"));
    }

    #[test]
    fn digest_overflow_notes_leftovers() {
        let c = cfg();
        let (lines, consumed) = build_digest(&c, "AU", &msgs(20));
        assert_eq!(consumed, c.max_msgs * c.max_posts);
        assert!(lines.len() <= c.max_posts);
        assert!(lines.last().unwrap().contains("(+"));
        for l in &lines {
            assert!(l.chars().count() <= MAX_RF_CHARS, "line too long: {l}");
        }
    }

    #[test]
    fn echo_detection() {
        let c = cfg();
        assert!(c.is_digest("[US 12:00] x"));
        assert!(c.is_digest("[AU 12:00] y"));
        assert!(!c.is_digest("US 12:00 plain"));
        assert!(!c.is_digest("[AU] no time"));
    }
}
