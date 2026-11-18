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

/// Lowercase base36 (VE3/VM1/VI1 announce fields).
fn b36(mut v: u32) -> String {
    if v == 0 { return "0".into(); }
    let mut s = String::new();
    while v > 0 {
        s.insert(0, char::from_digit(v % 36, 36).unwrap());
        v /= 36;
    }
    s
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

    /// Called from the serial reader for every media packet event. With
    /// complete-clip forwarding we do NOT queue the packet — the local
    /// reassembler collects it, and `on_clip_complete` fires when the whole
    /// clip is in hand (partial streams are never re-flooded, so a mid-clip
    /// link loss can no longer corrupt the far end).
    pub fn handle_media(&self, ev: &BridgeLine, _payload: &[u8]) {
        let cfg = self.config();
        if !cfg.enabled || ev.ch != cfg.in_idx {
            return;
        }
        // Loop guard for legacy per-packet rows still in the queue: nothing
        // to do here anymore; kept as a no-op seam for the serial reader.
    }

    /// Called by the media manager when a NEW clip completes. If it arrived
    /// on our in-channel, queue the complete bytes as one "clip" row; the
    /// drain re-chunks it under a fresh sid with its own announce.
    pub fn on_clip_complete(&self, sid_hex: &str, kind: &str, mode: u32, buf: &[u8], chan: &str) {
        let cfg = self.config();
        if !cfg.enabled || ch_word(cfg.in_idx) != Some(chan) {
            return;
        }
        // Echo of a clip we forwarded ourselves reassembles to the same
        // bytes — one fnv admit key stops the ping-pong permanently.
        let key = fnv1a(buf) | 2;
        if !self.admit(key) {
            return;
        }
        if buf.is_empty() || buf.len() > crate::media::MAX_CLIP_BYTES {
            self.store.log_event("forward", "mesh", &format!("clip {sid_hex} too large to forward ({} B)", buf.len()));
            return;
        }
        let payload = serde_json::json!({
            "b64": crate::media::b64_encode(buf),
            "kind": kind,
            "mode": mode,
        });
        self.store.forward_enqueue("clip", &payload.to_string());
        self.store.log_event(
            "forward",
            "mesh",
            &format!("queued complete {kind} clip {sid_hex} ({} B) ch={chan}", buf.len()),
        );
    }

    /// Called from the serial reader for every channel text event.
    pub fn handle_text(&self, ev: &BridgeLine) {
        let cfg = self.config();
        if !cfg.enabled || ev.ch != cfg.in_idx || ev.text.is_empty() {
            return;
        }
        // Media announces are not forwarded as text: complete-clip
        // forwarding emits its OWN announce for the fresh sid. Forwarding
        // the original would advertise a sid whose packets never come.
        if ev.text.contains("VE3:") || ev.text.contains("VM1:") || ev.text.contains("VI1:") {
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

    /// Forward a COMPLETE clip: re-chunk under a fresh sid (single clean
    /// packet class — no dual-packetization mixing), announce it, and run
    /// one paced salted pass with the packet-0 pre-send settle. The whole
    /// thing goes through the serial TX path so /media/tx records it.
    fn send_complete_clip(&self, payload_json: &str, out: &str) -> Result<(), String> {
        let v: serde_json::Value =
            serde_json::from_str(payload_json).map_err(|e| format!("bad clip json: {e}"))?;
        let data = crate::media::b64_decode(v["b64"].as_str().unwrap_or(""))
            .ok_or("bad b64 in clip row")?;
        let mode = v["mode"].as_u64().unwrap_or(1) as u32;
        let kind = v["kind"].as_str().unwrap_or("voice").to_string();
        let (sid, packets) =
            crate::media::MediaManager::chunk_outbound(&data, mode).ok_or("clip too large")?;
        let sid_hex = format!("{sid:08x}");
        let n = packets.len();
        let pace = self.config().pace_ms.max(crate::media::SEND_PACE_MS);
        let bitrate = match mode {
            1 => 1200, 2 => 2400, 3 => 1300, 4 => 1400, 5 => 1600, 6 => 3200, _ => 700,
        };
        let dur = if mode == 40 { 100 } else { (data.len() as u64 * 8 / bitrate).max(1) as u32 };
        let verb = match mode { 40 => "VI1", 32 => "VM1", _ => "VE3" };
        let announce = format!("{}:{}:{}:{}:{}", verb, b36(sid), b36(mode), b36(n as u32), b36(dur));
        self.media.record_outbound(sid, &packets); // answer 0x72 fetches for the forwarded copy
        self.media.register_envelope(&announce, out); // echo of our own flood completes locally
        self.serial.send_text(&announce, out).map_err(|e| e.to_string())?;
        std::thread::sleep(Duration::from_secs(2)); // let the announce land first
        // First radio2 datagram after the announce is often dropped; send
        // packet 0 alone and let the radio settle before the paced pass.
        self.serial
            .send_media(&crate::media::resalt(&packets[0]), out)
            .map_err(|e| e.to_string())?;
        std::thread::sleep(Duration::from_secs(2));
        for p in &packets {
            self.serial
                .send_media(&crate::media::resalt(p), out)
                .map_err(|e| e.to_string())?;
            std::thread::sleep(Duration::from_millis(pace));
        }
        self.store.log_event(
            "forward",
            "mesh",
            &format!("forwarded {kind} clip {sid_hex}: {n} packets on {out}"),
        );
        Ok(())
    }

    /// Paced drain loop; run on a dedicated thread.
    pub fn run(self: Arc<Self>) {
        loop {
            let cfg = self.config();
            if let Some((id, kind, payload)) = self.store.forward_pop() {
                let out = ch_word(cfg.out_idx).unwrap_or("bears2");
                let res = if kind == "clip" {
                    self.send_complete_clip(&payload, out)
                } else if kind == "media" {
                    // Legacy per-packet rows (queued before this build):
                    // send once with a fresh salt and keep the fetch cache.
                    crate::media::b64_decode(&payload)
                        .ok_or_else(|| "bad b64".to_string())
                        .and_then(|p| {
                            self.serial.send_media(&crate::media::resalt(&p), out).map_err(|e| e.to_string())?;
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
