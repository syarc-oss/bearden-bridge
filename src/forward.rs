//! Channel forwarding: re-emit media packets and channel text received on
//! one bridge channel onto another (e.g. Bears -> Bears2 for den-to-den
//! routing). Buffered in SQLite with a configurable release rate so the
//! RF egress pace can match the site's tolerance for mesh traffic.

use crate::events::BridgeLine;
use crate::serial::SerialLink;
use crate::store::Store;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const CH_WORDS: [&str; 3] = ["bears", "public", "bears2"];

pub fn ch_word(idx: i64) -> Option<&'static str> {
    CH_WORDS.get(idx as usize).copied()
}

pub fn ch_idx(word: &str) -> Option<i64> {
    CH_WORDS.iter().position(|w| *w == word).map(|i| i as i64)
}

#[derive(Debug, Clone)]
pub struct ForwardConfig {
    pub enabled: bool,
    pub in_idx: i64,
    pub out_idx: i64,
    pub pace_ms: u64,
}

impl Default for ForwardConfig {
    fn default() -> Self {
        ForwardConfig { enabled: false, in_idx: 0, out_idx: 2, pace_ms: 1500 }
    }
}

/// Strip up to 3 leading "<name>: " prefixes (sender labels added by each
/// forwarding hop). Names are short word-ish tokens ending in a colon+space.
pub fn strip_name_prefixes(mut t: &str) -> &str {
    for _ in 0..3 {
        let Some(colon) = t.find(": ") else { break };
        let name = &t[..colon];
        if name.is_empty() || name.len() > 24 || !name.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '_' || c == '-') {
            break;
        }
        t = &t[colon + 2..];
    }
    t
}

/// Stable 64-bit FNV-1a hash (dedup keys must survive restarts).
pub fn fnv1a(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

pub struct Forwarder {
    pub store: Arc<Store>,
    pub serial: Arc<SerialLink>,
    pub media: Arc<crate::media::MediaManager>,
    pub cfg: RwLock<ForwardConfig>,
    seen: Mutex<HashSet<u64>>,
}

impl Forwarder {
    /// Build from the config table (keys forward_enabled / forward_in /
    /// forward_out / forward_pace_ms) with env fallbacks
    /// (BEARDEN_FORWARD_ENABLED/IN/OUT/PACE_MS). Returns None when
    /// forwarding is not configured at all.
    pub fn from_config(
        store: Arc<Store>,
        serial: Arc<SerialLink>,
        media: Arc<crate::media::MediaManager>,
    ) -> Option<Arc<Self>> {
        let env_or_cfg = |key: &str, env: &str| -> Option<String> {
            store.get_config(key).or_else(|| std::env::var(env).ok())
        };
        let mut cfg = ForwardConfig::default();
        if let Some(v) = env_or_cfg("forward_in", "BEARDEN_FORWARD_IN") {
            cfg.in_idx = ch_idx(&v)?;
        }
        if let Some(v) = env_or_cfg("forward_out", "BEARDEN_FORWARD_OUT") {
            cfg.out_idx = ch_idx(&v)?;
        }
        if let Some(v) = env_or_cfg("forward_pace_ms", "BEARDEN_FORWARD_PACE_MS") {
            cfg.pace_ms = v.parse().unwrap_or(1500).max(200);
        }
        cfg.enabled = matches!(
            env_or_cfg("forward_enabled", "BEARDEN_FORWARD_ENABLED").as_deref(),
            Some("1") | Some("true") | Some("on")
        );
        if cfg.in_idx == cfg.out_idx {
            return None; // nonsense config: refuse to run
        }
        Some(Arc::new(Self {
            store,
            serial,
            media,
            cfg: RwLock::new(cfg),
            seen: Mutex::new(HashSet::new()),
        }))
    }

    pub fn config(&self) -> ForwardConfig {
        self.cfg.read().unwrap().clone()
    }

    pub fn set_config(&self, cfg: ForwardConfig) {
        self.store.set_config("forward_enabled", if cfg.enabled { "1" } else { "0" });
        self.store.set_config("forward_in", CH_WORDS[cfg.in_idx as usize]);
        self.store.set_config("forward_out", CH_WORDS[cfg.out_idx as usize]);
        self.store.set_config("forward_pace_ms", &cfg.pace_ms.to_string());
        *self.cfg.write().unwrap() = cfg;
    }

    /// Dedup: true when the key is new (and remembers it).
    fn admit(&self, key: u64) -> bool {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        self.store.admit_forward_dedup(key, now as i64)
    }

    /// Called from the serial reader for every media packet event.
    pub fn handle_media(&self, ev: &BridgeLine, payload: &[u8]) {
        let cfg = self.config();
        if !cfg.enabled || ev.ch != cfg.in_idx {
            return;
        }
        let key = fnv1a(payload);
        if !self.admit(key) {
            return; // already forwarded (loop guard) or duplicate
        }
        let b64 = crate::media::b64_encode(payload);
        self.store.forward_enqueue("media", &b64);
        self.store.log_event("forward", "mesh", &format!("queued media packet ch={} {}", ev.ch, &b64[..b64.len().min(16)]));
    }

    /// Called from the serial reader for every channel text event.
    pub fn handle_text(&self, ev: &BridgeLine) {
        let cfg = self.config();
        if !cfg.enabled || ev.ch != cfg.in_idx || ev.text.is_empty() {
            return;
        }
        // Loop guard: each RF hop prepends "<sender>: " to the text, so a
        // message that travels Den -> Den2 -> Den comes back with extra
        // prefixes and would otherwise ping-pong forever. Dedup on the
        // text with leading "<name>: " prefixes stripped so any number of
        // hops of the same message map to one key.
        let key = fnv1a(strip_name_prefixes(&ev.text).as_bytes());
        if !self.admit(key | 1) {
            return;
        }
        self.store.forward_enqueue("text", &ev.text);
        self.store.log_event("forward", "mesh", &format!("queued text ch={} {} bytes", ev.ch, ev.text.len()));
    }

    /// Paced drain loop; run on a dedicated thread.
    pub fn run(self: Arc<Self>) {
        loop {
            let cfg = self.config();
            if let Some((id, kind, payload)) = self.store.forward_pop() {
                let out = ch_word(cfg.out_idx).unwrap_or("bears2");
                let res = if kind == "media" {
                    // Send once with a fresh salt: the originator already
                    // floods two lossy passes and MeshCore drops
                    // byte-identical retransmits; receivers dedup media
                    // parts by sid+index and 0x72-fetch any gap (we cache
                    // forwarded parts below to answer those).
                    crate::media::b64_decode(&payload)
                        .ok_or_else(|| "bad b64".to_string())
                        .and_then(|p| {
                            self.serial.send_media(&crate::media::resalt(&p), out).map_err(|e| e.to_string())?;
                            // Cache forwarded parts (keyed by sid) so 0x72
                            // fetches for the forwarded clip can be answered.
                            if p.len() >= 6 {
                                let sid = u32::from_be_bytes([p[1], p[2], p[3], p[4]]);
                                self.media.merge_outbound(sid, vec![p]);
                            }
                            Ok(())
                        })
                } else {
                    self.serial.send_text(&payload, out).map_err(|e| e.to_string())
                };
                match res {
                    Ok(()) => self.store.forward_mark(id, "sent"),
                    Err(e) => {
                        self.store.forward_mark(id, &format!("error: {e}"));
                        eprintln!("bearden: forward send failed: {e}");
                    }
                }
                std::thread::sleep(Duration::from_millis(cfg.pace_ms));
            } else {
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}
