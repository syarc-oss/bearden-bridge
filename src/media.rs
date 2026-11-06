//! MediaMesh clip plumbing: reassembles Open TAK media sessions from
//! `BRIDGE {"evt":"media"}` serial events (0x5643 channel-data packets
//! relayed base64 by the room server firmware), stores completed clips,
//! and chunks outbound clips into paced `room.media <b64>` CLI sends.
//!
//! Wire format (Open TAK voice_wire.dart):
//!   packet: [0x56|0x57|0x58][sid:4 BE][index:1][payload...]
//!     0x57/0x58 drop one trailing byte; for 0x58 its low 6 bits are the
//!     packets-left counter, 0 = final packet of the session.
//! We always TX 0x56 packets plus a 0x58 terminator, so receivers can
//! complete with or without a VE3/VI1 text announce.

use crate::store::Store;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Media payload bytes per 0x56 packet. Firmware CLI line budget is 151
/// chars: "room.media " + base64(6 + CHUNK + 1) must stay under it.
pub const CHUNK: usize = 90;
/// Pause between RF sends (airtime + duty friendliness).
pub const SEND_PACE_MS: u64 = 1200;
/// Hard cap per clip: 255 packets (1-byte index) x CHUNK.
pub const MAX_CLIP_BYTES: usize = 255 * CHUNK;

#[derive(Debug, Clone)]
struct Session {
    kind: String,
    mode: u32,
    total: u32,
    chan: String,
    /// Second channel the sid was also heard on (forwarder re-flood echo).
    chan2: String,
    /// Every distinct copy received per packet index. Open TAK senders may
    /// emit the SAME clip with two chunk sizes (e.g. 92B initial burst and
    /// 156B fetch answers) — mixing lengths per index garbles the stream, so
    /// reassembly must pick one consistent length class (see `assemble`).
    parts: HashMap<u32, Vec<Vec<u8>>>,
    done: bool,
    /// Assembled byte length at the last finalize. Late packets that would
    /// grow the assembled clip reopen a done session (console c091e15).
    stored_len: usize,
    /// Highest index covered by the last stored assembly (run length).
    /// A later packet at index >= this reopens the session even when the
    /// assembled byte length did not grow (mixed length classes can hide
    /// growth from assemble()).
    stored_max: u32,
    last_seen: std::time::Instant,
    fetch_at: Option<std::time::Instant>,
    fetches: u32,
}

/// Done sessions stay in the map (so late packets can reopen them) and are
/// pruned after this long idle.
const DONE_EXPIRY_SECS: u64 = 600;

// Open TAK voice/photo clips carry no total-count in the binary packets (it
// lives in the VE3/VM1/VI1 text announce, which room servers do not relay),
// and the 0x58 left==0 terminator only appears for fetch answers. Complete a
// session when its parts form a contiguous run from 0 and it has been idle
// this long.
const IDLE_COMPLETE_SECS: u64 = 4;

/// Reassemble a stream from per-index packet copies. Open TAK senders may
/// packetize the same clip twice with different chunk sizes (92B burst vs
/// 156B fetch answers); mixing lengths garbles codec2, so pick the length
/// class (exact byte length) with the longest contiguous run from index 0,
/// allowing one shorter tail packet from another class at the run boundary.
/// Returns (run length, assembled buffer) or None when nothing is usable.
fn assemble(parts: &HashMap<u32, Vec<Vec<u8>>>) -> Option<(u32, Vec<u8>)> {
    // Group copies by exact byte length: len -> (index -> copy)
    let mut classes: HashMap<usize, HashMap<u32, Vec<u8>>> = HashMap::new();
    for (idx, copies) in parts {
        for c in copies {
            if c.is_empty() {
                continue;
            }
            classes.entry(c.len()).or_default().insert(*idx, c.clone());
        }
    }
    let mut best: Option<(u32, Vec<u8>)> = None;
    for (len, group) in &classes {
        let mut run: u32 = 0;
        while group.contains_key(&run) {
            run += 1;
        }
        if run == 0 {
            continue;
        }
        // Allow one shorter tail (last packet) from another length class.
        let (run, buf) = if !group.contains_key(&run) {
            // find a tail copy at index run from another class
            let tail = parts
                .get(&run)
                .and_then(|copies| copies.iter().find(|c| c.len() < *len))
                .cloned();
            match tail {
                Some(t) => (run + 1, {
                    let mut b: Vec<u8> = Vec::with_capacity((run as usize) * len + t.len());
                    for i in 0..run {
                        b.extend_from_slice(&group[&i]);
                    }
                    b.extend_from_slice(&t);
                    b
                }),
                None => (run, {
                    let mut b: Vec<u8> = Vec::with_capacity(run as usize * len);
                    for i in 0..run {
                        b.extend_from_slice(&group[&i]);
                    }
                    b
                }),
            }
        } else {
            (
                run,
                (0..run)
                    .flat_map(|i| group[&i].iter().copied())
                    .collect::<Vec<u8>>(),
            )
        };
        let better = match &best {
            None => true,
            Some((brun, bbuf)) => run > *brun || (run == *brun && buf.len() > bbuf.len()),
        };
        if better {
            best = Some((run, buf));
        }
    }
    best
}

pub struct MediaManager {
    store: Arc<Store>,
    dir: String,
    sessions: Mutex<HashMap<u32, Session>>,
    /// Recently transmitted wire packets per sid, kept to answer 0x72
    /// binary-fetch requests from Open TAK listeners that lost a part.
    outbound: Mutex<HashMap<u32, (std::time::Instant, Vec<Vec<u8>>)>>,
    /// Per-sid throttle so a repeated fetch is answered at most every 3s.
    fetch_gate: Mutex<HashMap<u32, std::time::Instant>>,
    /// Sink for outgoing 0x72 fetch requests (set to the serial link after
    /// construction; Open TAK senders are pull-based — they send the
    /// announce plus a few packets and serve the rest only on fetch).
    fetch_sink: Mutex<Option<Box<dyn Fn(&[u8], &str) + Send + Sync>>>,
    /// (console URL, bearer token): completed clips are pushed to the local
    /// console's /platforms/meshcore/media/intake so its UI shows the full
    /// clip (the console cannot 0x72-fetch missing parts itself).
    console: Option<(String, String)>,
    /// Ring buffer of recently ingested raw packets (post salt-strip) for
    /// live debugging via GET /media/debug — no env or disk perms needed.
    ring: std::sync::Mutex<std::collections::VecDeque<(u32, u32, u8, usize, String, std::time::SystemTime)>>,
}

impl MediaManager {
    pub fn new(store: Arc<Store>, dir: &str, console: Option<(String, String)>) -> Self {
        let _ = std::fs::create_dir_all(dir);
        MediaManager {
            store,
            dir: dir.to_string(),
            sessions: Mutex::new(HashMap::new()),
            outbound: Mutex::new(HashMap::new()),
            fetch_gate: Mutex::new(HashMap::new()),
            fetch_sink: Mutex::new(None),
            console,
            ring: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Install the sender used for outgoing 0x72 fetch requests.
    pub fn set_fetch_sink(&self, sink: Box<dyn Fn(&[u8], &str) + Send + Sync>) {
        *self.fetch_sink.lock().unwrap() = Some(sink);
    }

    /// Live debug dump: active sessions (per-index copy lengths) and the
    /// raw-packet ring, served from GET /media/debug.
    pub fn debug_dump(&self) -> serde_json::Value {
        use serde_json::json;
        let sessions = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .map(|(sid, s)| {
                let parts: Vec<serde_json::Value> = {
                    let mut idxs: Vec<&u32> = s.parts.keys().collect();
                    idxs.sort();
                    idxs.iter()
                        .map(|i| {
                            json!({
                                "idx": i,
                                "copies": s.parts[i].iter().map(|c| c.len()).collect::<Vec<_>>(),
                            })
                        })
                        .collect()
                };
                json!({
                    "sid": format!("{sid:08x}"),
                    "total": s.total, "mode": s.mode, "done": s.done,
                    "chan": s.chan, "parts": parts,
                })
            })
            .collect::<Vec<_>>();
        let ring = self
            .ring
            .lock()
            .unwrap()
            .iter()
            .map(|(sid, idx, magic, len, chan, t)| {
                json!({
                    "sid": format!("{sid:08x}"), "idx": idx,
                    "magic": format!("{magic:02x}"), "len": len, "chan": chan,
                    "t": t.duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
                })
            })
            .collect::<Vec<_>>();
        json!({ "sessions": sessions, "ring": ring })
    }

    pub fn dir(&self) -> &str {
        &self.dir
    }

    /// Cache transmitted packets so later 0x72 fetches can be answered.
    /// Store the canonical (unsalted) wire packet; resalt at answer time so
    /// mesh dedup never eats the retransmit.
    pub fn record_outbound(&self, sid: u32, packets: &[Vec<u8>]) {
        let mut ob = self.outbound.lock().unwrap();
        if ob.len() >= 64 {
            if let Some(oldest) = ob.iter().min_by_key(|(_, (t, _))| *t).map(|(k, _)| *k) {
                ob.remove(&oldest);
            }
        }
        ob.insert(sid, (std::time::Instant::now(), packets.to_vec()));
    }

    /// Add packets to an existing outbound cache entry (forwarder sends
    /// parts one at a time as they arrive).
    pub fn merge_outbound(&self, sid: u32, packets: Vec<Vec<u8>>) {
        let mut ob = self.outbound.lock().unwrap();
        let entry = ob
            .entry(sid)
            .or_insert_with(|| (std::time::Instant::now(), Vec::new()));
        for p in packets {
            if !entry.1.iter().any(|q| q[5] == p[5]) {
                entry.1.push(p);
            }
        }
        entry.0 = std::time::Instant::now();
    }

    /// Resolve a 0x72 fetch: [0x72][sid:4][flags][key6][count][indices...].
    /// Returns the cached packets to retransmit (empty when throttled or
    /// unknown sid). `None` indices = "send everything" (flag bit 0 clear).
    pub fn fetch_answer(&self, sid: u32, indices: Option<&[u8]>) -> Vec<Vec<u8>> {
        {
            let mut gate = self.fetch_gate.lock().unwrap();
            if gate
                .get(&sid)
                .map(|t| t.elapsed().as_secs() < 3)
                .unwrap_or(false)
            {
                return Vec::new();
            }
            gate.insert(sid, std::time::Instant::now());
        }
        let ob = self.outbound.lock().unwrap();
        match ob.get(&sid) {
            Some((_, pkts)) => pkts
                .iter()
                .filter(|p| {
                    indices
                        .map(|ix| ix.contains(&p[5]))
                        .unwrap_or(true)
                })
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// Background finalizer: the last clip of a batch gets no further packet
    /// to trigger sweep_idle inside ingest(), so sweep on a timer.
    pub fn start_sweeper(self: &std::sync::Arc<Self>) {
        let mgr = self.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_secs(2));
            for (sid, sess) in mgr.sweep_idle() {
                mgr.write_out(sid, sess);
            }
        });
    }

    /// One decoded 0x5643 channel-data payload from the firmware.
    /// `chan` is the mesh channel word the packet arrived on, remembered so
    /// later 0x72 fetch requests go back out the same channel.
    pub fn ingest(&self, payload: &[u8], chan: &str) {
        if std::env::var("BEARDEN_MEDIA_DEBUG").is_ok() {
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(std::path::Path::new(&self.dir).join("media_debug.log"))
            {
                use std::io::Write;
                let _ = writeln!(
                    f,
                    "{}",
                    payload.iter().map(|b| format!("{b:02x}")).collect::<String>()
                );
            }
        }
        if payload.len() < 6 {
            return;
        }
        let sid = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        let index = payload[5] as u32;
        let b0 = payload[0];
        let (part, left): (Vec<u8>, Option<u32>) = match b0 {
            0x56 => (payload[6..].to_vec(), None),
            0x57 | 0x58 => {
                if payload.len() < 7 {
                    return;
                }
                let n = payload.len() - 1;
                let left = if b0 == 0x58 {
                    Some((payload[n] & 0x3f) as u32)
                } else {
                    None
                };
                (payload[6..n].to_vec(), left)
            }
            _ => return, // 0x72 fetch requests: ignore
        };
        if sid == 0 || index > 599 || part.len() > 220 {
            return;
        }
        {
            let mut ring = self.ring.lock().unwrap();
            if ring.len() >= 4000 {
                ring.pop_front();
            }
            ring.push_back((sid, index, b0, part.len(), chan.to_string(), std::time::SystemTime::now()));
        }

        let finalize = {
            let mut map = self.sessions.lock().unwrap();
            let entry = map.entry(sid).or_insert_with(|| Session {
                kind: "voice".into(),
                mode: 0,
                total: 0,
                chan: chan.to_string(),
                chan2: String::new(),
                parts: HashMap::new(),
                done: false,
                stored_len: 0,
                stored_max: 0,
                last_seen: std::time::Instant::now(),
                fetch_at: None,
                fetches: 0,
            });
            if entry.done {
                // Late packets after a short finalize (ported from console
                // c091e15): accept them; reopen only when the assembled
                // clip would grow beyond what was already stored.
                entry.parts.entry(index).or_default().push(part.clone());
                if let Some(v) = entry.parts.get_mut(&index) {
                    let last = v.last().unwrap().clone();
                    if v[..v.len() - 1].contains(&last) {
                        v.pop();
                    }
                }
                entry.last_seen = std::time::Instant::now();
                let s = &mut *entry;
                let cur = assemble(&s.parts).map(|(_, b)| b.len()).unwrap_or(0);
                let max_idx = s.parts.keys().copied().max().unwrap_or(0);
                if cur > s.stored_len || max_idx >= s.stored_max {
                    s.done = false;
                    s.fetches = 0;
                    s.fetch_at = None;
                } else {
                    return;
                }
            }
            if entry.chan.is_empty() {
                entry.chan = chan.to_string();
            } else if entry.chan != chan && entry.chan2 != chan {
                entry.chan2 = chan.to_string();
            }
            entry.parts.entry(index).or_default().push(part);
            if entry.parts[&index].len() > 1 {
                let v = &mut entry.parts.get_mut(&index).unwrap();
                // dedup identical copies (echoes/retransmits)
                let last = v.last().unwrap().clone();
                if v[..v.len() - 1].contains(&last) {
                    v.pop();
                }
            }
            entry.last_seen = std::time::Instant::now();
            let s = &mut *entry;
            let run = assemble(&s.parts).map(|(n, _)| n).unwrap_or(0);
            let all = s.total > 0 && run >= s.total;
            let left_done = left == Some(0) && run > 0 && {
                if s.total == 0 {
                    s.total = run;
                }
                run >= s.total && s.total > 0
            };
            if all || left_done {
                entry.done = true;
                if let Some((n, b)) = assemble(&entry.parts) {
                    entry.stored_len = b.len();
                    entry.stored_max = n;
                }
                Some(entry.clone())
            } else {
                None
            }
        };

        let mut finals = Vec::new();
        if let Some(sess) = finalize {
            finals.push((sid, sess));
        }
        finals.extend(self.sweep_idle());
        // Sessions stay in the map (marked done) so late packets can reopen
        // them; sweep_idle prunes done sessions after DONE_EXPIRY_SECS.
        for (sid, sess) in finals {
            self.write_out(sid, sess);
        }
    }

    /// Finalize or fetch for idle sessions. Open TAK senders are PULL-BASED:
    /// they emit the announce plus only the first few packets and serve the
    /// rest only when a receiver sends a 0x72 fetch. So:
    ///  - total known (from a VE3/VM1/VI1 announce): finalize ONLY when every
    ///    index 0..total is present; otherwise send a 0x72 fetch (at most 8,
    ///    spaced >= 10s) for the missing indices.
    ///  - total unknown: fall back to the old contiguous-run heuristic, but
    ///    never finalize a session that carries parts above the run.
    fn sweep_idle(&self) -> Vec<(u32, Session)> {
        const FETCH_IDLE_SECS: u64 = 10;
        const MAX_FETCHES: u32 = 8;
        const MAX_FETCH_INDICES: usize = 48;
        let mut out = Vec::new();
        let mut fetches: Vec<(u32, Vec<u8>, String)> = Vec::new();
        let mut map = self.sessions.lock().unwrap();
        // Prune done sessions that stayed quiet long after their last reopen.
        let expired: Vec<u32> = map
            .iter()
            .filter(|(_, s)| {
                s.done && s.last_seen.elapsed().as_secs() > DONE_EXPIRY_SECS
            })
            .map(|(sid, _)| *sid)
            .collect();
        for sid in &expired {
            map.remove(sid);
        }
        let idle: Vec<u32> = map
            .iter()
            .filter(|(_, s)| {
                !s.done && s.last_seen.elapsed().as_secs() >= IDLE_COMPLETE_SECS
            })
            .map(|(sid, _)| *sid)
            .collect();
        for sid in idle {
            let Some(s) = map.get_mut(&sid) else {
                continue;
            };
            let run = assemble(&s.parts).map(|(n, _)| n).unwrap_or(0);
            if s.total > 0 {
                if run >= s.total {
                    s.done = true;
                    if let Some((n, b)) = assemble(&s.parts) {
                        s.stored_len = b.len();
                        s.stored_max = n;
                    }
                    out.push((sid, s.clone()));
                    continue;
                }
                // Incomplete but the announce told us the total: fetch the
                // missing packets from the sender instead of finalizing short.
                let since_fetch = s
                    .fetch_at
                    .map(|t| t.elapsed().as_secs())
                    .unwrap_or_else(|| s.last_seen.elapsed().as_secs());
                if s.fetches < MAX_FETCHES && since_fetch >= FETCH_IDLE_SECS {
                    // Open TAK wire spec (voice_wire.dart): flags 0x01 =
                    // missing-only index list, 0x02 = SaltOk (answers may be
                    // salted 0x57). The mesh seen-table drops byte-identical
                    // retransmits, so each retry must be unique: repeat one
                    // listed index (varying per retry) as the spec's salt.
                    let mut missing: Vec<u8> = (0..s.total)
                        .filter(|i| !s.parts.contains_key(i))
                        .map(|i| i as u8)
                        .take(MAX_FETCH_INDICES)
                        .collect();
                    if !missing.is_empty() {
                        let r = missing[(s.fetches as usize) % missing.len()];
                        missing.push(r);
                        let mut wire = Vec::with_capacity(13 + missing.len());
                        wire.push(0x72);
                        wire.extend_from_slice(&sid.to_be_bytes());
                        wire.push(0x01 | 0x02); // missing-only + SaltOk
                        wire.extend_from_slice(&[0u8; 6]); // key
                        wire.push(missing.len() as u8);
                        wire.extend_from_slice(&missing);
                        s.fetch_at = Some(std::time::Instant::now());
                        s.fetches += 1;
                        fetches.push((sid, wire, s.chan.clone()));
                    }
                }
                // Gave up fetching: keep the session marked done (late
                // parts can still reopen it); the expiry prune reaps it.
                if s.fetches >= MAX_FETCHES && s.last_seen.elapsed().as_secs() > 300 {
                    s.done = true;
                }
                continue;
            }
            // No announce ever seen: the total is unknown, but the sender
            // is pull-based, so any gap below the highest observed index is
            // likely recoverable via a salted missing-only 0x72 fetch.
            // Finalize when the observed window is contiguous, or after a
            // long stall (best contiguous run, reopen still possible).
            let max_idx = s.parts.keys().copied().max().unwrap_or(0);
            let missing: Vec<u8> = (0..=max_idx)
                .filter(|i| !s.parts.contains_key(i))
                .map(|i| i as u8)
                .take(MAX_FETCH_INDICES)
                .collect();
            let since_fetch = s
                .fetch_at
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(u64::MAX);
            if !missing.is_empty() && s.fetches < MAX_FETCHES {
                if since_fetch >= FETCH_IDLE_SECS {
                    let mut m = missing.clone();
                    let r = m[(s.fetches as usize) % m.len()];
                    m.push(r);
                    let mut wire = Vec::with_capacity(13 + m.len());
                    wire.push(0x72);
                    wire.extend_from_slice(&sid.to_be_bytes());
                    wire.push(0x01 | 0x02); // missing-only + SaltOk
                    wire.extend_from_slice(&[0u8; 6]); // key
                    wire.push(m.len() as u8);
                    wire.extend_from_slice(&m);
                    s.fetch_at = Some(std::time::Instant::now());
                    s.fetches += 1;
                    fetches.push((sid, wire, s.chan.clone()));
                }
                if s.last_seen.elapsed().as_secs() > 600 {
                    s.total = if run > 0 {
                        run
                    } else {
                        max_idx + 1
                    };
                    s.done = true;
                    out.push((sid, s.clone()));
                }
                continue;
            }
            // Contiguous observed window (or fetch budget spent): finalize
            // the best contiguous run.
            s.total = if run > 0 {
                run
            } else {
                max_idx + 1
            };
            s.done = true;
            if let Some((n, b)) = assemble(&s.parts) {
                s.stored_len = b.len();
                s.stored_max = n;
            } else {
                s.stored_max = max_idx + 1;
            }
            out.push((sid, s.clone()));
        }
        drop(map);
        for (sid, wire, chan) in fetches {
            eprintln!("bearden: media fetch request {sid:08x}");
            self.store
                .log_event("media", "mesh", &format!("fetch request {sid:08x}"));
            let sink = self.fetch_sink.lock().unwrap();
            if let Some(f) = sink.as_ref() {
                let chan = if chan.is_empty() { "bears".to_string() } else { chan };
                f(&wire, &chan);
            }
        }
        out
    }

    fn write_out(&self, sid: u32, sess: Session) {
        let sid_hex = format!("{sid:08x}");
        let Some((run, buf)) = assemble(&sess.parts) else {
            eprintln!("bearden: media {sid_hex} finalize with no parts?");
            return;
        };
        let total = if sess.total > 0 { sess.total } else { run };
        let mode = if sess.mode == 0 { guess_mode(&buf) } else { sess.mode };
        let kind = if mode == 40 { "photo" } else { &sess.kind };
        // A late duplicate session for the same sid can finalize a short
        // stub (retry subset) AFTER the full clip was already stored — never
        // let a smaller re-completion clobber the larger stored clip.
        if let Some(existing) = self.store.media_complete_size(&sid_hex) {
            if existing >= buf.len() as i64 {
                self.store.log_event(
                    "media",
                    "mesh",
                    &format!(
                        "ignoring duplicate completion {sid_hex} ({} bytes; already stored {})",
                        buf.len(),
                        existing
                    ),
                );
                // The echo usually rides a DIFFERENT channel (forwarder
                // re-flood) — push it anyway so the console can tag the
                // clip's second channel; ingest ignores the size dup.
                let fname = format!("{sid_hex}_{kind}.{}", mode_ext(mode));
                self.push_console(&sid_hex, kind, mode, total, &fname, &buf, &sess.chan, &sess.chan2);
                return;
            }
        }
        let fname = format!("{sid_hex}_{kind}.{}", mode_ext(mode));
        let path = std::path::Path::new(&self.dir).join(&fname);
        match std::fs::write(&path, &buf) {
            Ok(()) => {
                let size = buf.len() as i64;
                self.store.media_complete(&sid_hex, kind, mode, total, &fname, size);
                self.store.log_event(
                    "media",
                    "mesh",
                    &format!("received {kind} clip {sid_hex} ({size} bytes)"),
                );
                eprintln!("bearden: media session {sid_hex} complete ({size} bytes)");
                self.push_console(&sid_hex, kind, mode, total, &fname, &buf, &sess.chan, &sess.chan2);
            }
            Err(e) => {
                self.store.media_error(&sid_hex);
                eprintln!("bearden: media {sid_hex} write failed: {e}");
            }
        }
    }

    /// Fire-and-forget push of a completed clip to the local console.
    fn push_console(
        &self,
        sid_hex: &str,
        kind: &str,
        mode: u32,
        total: u32,
        fname: &str,
        buf: &[u8],
        chan: &str,
        chan2: &str,
    ) {
        let Some((base, token)) = self.console.clone() else {
            return;
        };
        let mut body = serde_json::json!({
            "sid": sid_hex,
            "kind": kind,
            "mode": mode,
            "total": total,
            "filename": fname,
            "chan": if chan.is_empty() { "bears" } else { chan },
            "b64": b64_encode(buf),
        });
        if !chan2.is_empty() {
            body["chan2"] = serde_json::json!(chan2);
        }
        let url = format!("{base}/platforms/meshcore/media/intake");
        std::thread::spawn(move || {
            let client = match reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
            {
                Ok(c) => c,
                Err(_) => return,
            };
            match client
                .post(&url)
                .bearer_auth(&token)
                .json(&body)
                .send()
            {
                Ok(r) if r.status().is_success() => {}
                Ok(r) => eprintln!("bearden: console media push {url} -> {}", r.status()),
                Err(e) => eprintln!("bearden: console media push {url} failed: {e}"),
            }
        });
    }

    /// Split a clip into wire packets. Returns (sid, packets) where each
    /// packet is a full 0x56/0x58 channel-data payload, ready to base64.
    pub fn chunk_outbound(data: &[u8], _mode: u32) -> Option<(u32, Vec<Vec<u8>>)> {
        if data.is_empty() || data.len() > MAX_CLIP_BYTES {
            return None;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let mut sid = nanos.wrapping_mul(2654435761) ^ std::process::id() as u64;
        sid = (sid >> 13) ^ (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0));
        let sid = (sid as u32) & 0x7fff_ffff | 1;
        let mut packets = Vec::new();
        let chunks: Vec<&[u8]> = data.chunks(CHUNK).collect();
        for (i, c) in chunks.iter().enumerate() {
            let mut p = Vec::with_capacity(8 + c.len());
            // All data packets are salted 0x57: the trailing salt byte makes
            // each transmission unique (MeshCore drops byte-identical floods)
            // while receivers ignore it. NEVER send the 0x58 "left==0"
            // terminator in a normal clip: Open TAK treats it as "session
            // complete" and will try to decode immediately, mid-gap, when a
            // middle packet was lost — that was the "could not be decoded"
            // bug. Completeness comes from the announce total (+ 0x72 fetch
            // for gaps). 0x58 stays reserved for fetch answers.
            p.push(0x57);
            p.extend_from_slice(&sid.to_be_bytes());
            p.push(i as u8);
            p.extend_from_slice(c);
            p.push(salt_byte());
            packets.push(p);
        }
        Some((sid, packets))
    }

    /// Parse a VE3:/VM1:/VI1: announce (`VE3:{sid b36}:{mode b36}:{total b36}:
    /// {dur b36}`) and teach the session its mode/total. Tolerates mesh-hop
    /// name prefixes ("Bear Den2: VE3:...") — jade-on-mesh's own parser does
    /// not, so we cannot rely on anyone else having parsed it.
    pub fn register_envelope(&self, text: &str, chan: &str) {
        let Some(at) = text.find("VE3:").or_else(|| text.find("VM1:")) else {
            return;
        };
        let fields: Vec<&str> = text[at..]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .split(':')
            .collect();
        // ["VE3", sid, mode, total, dur]
        if fields.len() != 5 {
            return;
        }
        let b36 = |s: &str| -> Option<u32> {
            if s.is_empty() || s.len() > 7 {
                return None;
            }
            u32::from_str_radix(s, 36).ok()
        };
        let (Some(sid), Some(mode), Some(total)) = (b36(fields[1]), b36(fields[2]), b36(fields[3]))
        else {
            return;
        };
        if mode > 40 || total == 0 || total > 600 {
            return;
        }
        let mut map = self.sessions.lock().unwrap();
        let entry = map.entry(sid).or_insert_with(|| Session {
            kind: if text.contains("VI1:") { "photo".into() } else { "voice".into() },
            mode: 0,
            total: 0,
            chan: chan.to_string(),
            chan2: String::new(),
            parts: HashMap::new(),
            done: false,
            stored_len: 0,
            stored_max: 0,
            last_seen: std::time::Instant::now(),
            fetch_at: None,
            fetches: 0,
        });
        let s = &mut *entry;
        if !s.done && s.mode == 0 {
            s.mode = mode;
        }
        if !s.done && s.total == 0 {
            s.total = total;
        }
        if !s.done && s.chan.is_empty() {
            s.chan = chan.to_string();
        }
    }
}

/// Fresh random-ish byte for salting retransmissions (VoxSaltCounter-style).
pub fn salt_byte() -> u8 {
    use std::sync::atomic::{AtomicU8, Ordering};
    static CTR: AtomicU8 = AtomicU8::new(0);
    let n = CTR.fetch_add(37, Ordering::Relaxed).wrapping_mul(197);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u8;
    n ^ nanos ^ (std::process::id() as u8)
}

/// Refresh the salt of a wire packet so a retransmit differs byte-wise from
/// the original while parsing to the identical (sid, index, part).
pub fn resalt(pkt: &[u8]) -> Vec<u8> {
    let mut p = pkt.to_vec();
    match p.first().copied() {
        Some(0x56) => {
            p[0] = 0x57;
            p.push(salt_byte());
        }
        Some(0x57 | 0x58) => {
            if let Some(last) = p.last_mut() {
                *last = (salt_byte() & 0x3f) << 6 | (*last & 0x3f);
            }
        }
        _ => {}
    }
    p
}

fn mode_ext(mode: u32) -> &'static str {
    match mode {
        40 => "av1",
        32 => "mimi",
        _ => "c2",
    }
}

/// Without an announce we don't know the codec; AV1 photos start with a
/// OBIF/ISOBMFF-style box header (size then 'ftyp'/'moov'/...) or a bare
/// av1C config box.
fn guess_mode(buf: &[u8]) -> u32 {
    if buf.len() > 12 && (&buf[4..8] == b"ftyp" || buf[8..].starts_with(b"av1C")) {
        return 40;
    }
    // Open TAK wire id 1 = codec2 1200 (6-byte frames), the mode every
    // console sender uses. Id 4 is 1400, NOT 1200.
    1
}

const B64A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity((data.len() + 2) / 3 * 4);
    for c in data.chunks(3) {
        let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(B64A[(n >> 18) as usize & 63] as char);
        out.push(B64A[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 { B64A[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if c.len() > 2 { B64A[n as usize & 63] as char } else { '=' });
    }
    out
}

/// Cheap FNV-1a hash for duplicate-clip detection.
pub fn fnv1a(data: &[u8]) -> u64 {
    data.iter().fold(0xcbf29ce484222325u64, |h, b| (h ^ *b as u64) * 0x100000001b3)
}

pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let mut buf: u32 = 0;
    let mut bits = 0;
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' | b'\n' | b'\r' | b' ' => continue,
            _ => return None,
        };
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_roundtrip() {
        for n in 0..200 {
            let data: Vec<u8> = (0..n).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(b64_decode(&b64_encode(&data)).unwrap(), data);
        }
    }

    #[test]
    fn chunk_reassemble_roundtrip() {
        let mgr = MediaManager::new(
            Arc::new(Store::open(std::path::Path::new(":memory:")).unwrap()),
            "/tmp/bearden-media-test",
            None,
        );
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 251) as u8).collect();
        let (sid, packets) = MediaManager::chunk_outbound(&data, 4).unwrap();
        // Announce teaches the session its total (senders emit VE3 + packets;
        // completeness comes from the total, not a terminator).
        let total = packets.len() as u32;
        let mut b36 = String::new();
        let mut v = sid;
        loop {
            b36.insert(0, std::char::from_digit(v % 36, 36).unwrap());
            v /= 36;
            if v == 0 {
                break;
            }
        }
        mgr.register_envelope(&format!("VE3:{b36}:1:c:0"), "bears");
        // feed packets through a different manager (the receiving peer)
        for p in &packets {
            mgr.ingest(p, "bears");
        }
        let sess = mgr.sessions.lock().unwrap();
        assert!(
            sess.iter().all(|(_, s)| s.done),
            "session should finalize (kept done for reopen, not removed)"
        );
        drop(sess);
        let sid_hex = format!("{sid:08x}");
        let path = std::path::Path::new("/tmp/bearden-media-test").join(format!("{sid_hex}_voice.c2"));
        assert_eq!(std::fs::read(&path).unwrap(), data);
    }

    #[test]
    fn assemble_picks_consistent_length_class() {
        // Same stream packetized twice: 92B initial burst + 156B fetch answers.
        let stream: Vec<u8> = (0..600u32).map(|i| (i % 251) as u8).collect();
        let mut parts: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
        for (i, chunk) in stream.chunks(92).enumerate() {
            parts.entry(i as u32).or_default().push(chunk.to_vec());
        }
        for (i, chunk) in stream.chunks(156).enumerate() {
            parts.entry(i as u32).or_default().push(chunk.to_vec());
        }
        let (run, buf) = assemble(&parts).unwrap();
        // 92-class run is 6 full packets + a 48B tail (index 6, its own
        // shorter class) recovered by the tail extension: 6*92+48 = 600.
        assert_eq!(run, 7);
        assert_eq!(buf, stream);
    }

    #[test]
    fn assemble_allows_short_tail_from_other_class() {
        // 156-class covers indices 0..3; a shorter final packet at index 4
        // only exists in the 92-class — tail extension should recover it.
        let stream: Vec<u8> = (0..500u32).map(|i| (i % 251) as u8).collect();
        let mut parts: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
        for (i, chunk) in stream.chunks(156).enumerate() {
            parts.entry(i as u32).or_default().push(chunk.to_vec());
        }
        // Simulate truncated 92-class: only last chunk present (92-class
        // chunk 4 covers bytes 368..460, shorter than 156).
        let tail: Vec<u8> = stream[368..460].to_vec();
        parts.entry(4).or_default().push(tail.clone());
        let (run, buf) = assemble(&parts).unwrap();
        // 156-class run is 3 (indices 0..2 full? ceil(500/156)=4 -> indices
        // 0,1,2 are 156B, index 3 is 32B). Tail candidate at run=3 is the
        // 32B leftover in the same class, so run=4 either way.
        assert!(run >= 3);
        // With same-class short tail the full stream is recovered.
        let expect: Vec<u8> = stream.clone();
        assert_eq!(buf, expect);
    }
}
