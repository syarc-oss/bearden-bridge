//! Serial link to the Bear Den room server firmware.
//!
//! Reads `BRIDGE {...}` event lines; injects room messages via the stock
//! `room.post <text>` CLI command (terminated with `\r`).

use crate::events::BridgeLine;
use crate::media::MediaManager;
use crate::store::Store;
use anyhow::{Context, Result};
use serialport::SerialPort;
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};

/// One outbound record: everything we hand to the firmware CLI, so GET
/// /media/tx can show what actually went out on RF (initial bursts, fetch
/// answers, forwarder floods, announces).
pub struct TxRec {
    pub ts: std::time::SystemTime,
    pub kind: &'static str, // "media" | "fetch" | "text"
    pub chan: String,
    pub sid: Option<u32>,
    pub idx: Option<u8>,
    pub len: usize,
    pub text: String, // first chars, for kind == "text"
}

/// Process-global ring (cap 2000) of outbound serial writes.
pub static TX_RING: Mutex<std::collections::VecDeque<TxRec>> =
    Mutex::new(std::collections::VecDeque::new());

const TX_RING_CAP: usize = 2000;

fn tx_record(kind: &'static str, chan: &str, payload: Option<&[u8]>, text: &str) {
    let mut rec = TxRec {
        ts: std::time::SystemTime::now(),
        kind,
        chan: chan.to_string(),
        sid: None,
        idx: None,
        len: payload.map(|p| p.len()).unwrap_or(text.len()),
        text: text.chars().take(60).collect(),
    };
    if let Some(p) = payload {
        // Media wire: [0x56/0x57/0x58][sid 4 BE][idx][...]; fetch: [0x72][sid 4 BE][flags]...
        if p.len() >= 6 {
            if matches!(p[0], 0x56 | 0x57 | 0x58) {
                rec.sid = Some(u32::from_be_bytes([p[1], p[2], p[3], p[4]]));
                rec.idx = Some(p[5]);
            } else if p[0] == 0x72 {
                rec.sid = Some(u32::from_be_bytes([p[1], p[2], p[3], p[4]]));
            }
        }
    }
    let mut ring = TX_RING.lock().unwrap();
    if ring.len() >= TX_RING_CAP {
        ring.pop_front();
    }
    ring.push_back(rec);
}

pub struct SerialLink {
    writer: Mutex<Box<dyn SerialPort>>,
}

impl SerialLink {
    pub fn open(path: &str) -> Result<Self> {
        let port = serialport::new(path, 115_200)
            .timeout(std::time::Duration::from_millis(200))
            .open()
            .with_context(|| format!("opening {path}"))?;
        Ok(SerialLink {
            writer: Mutex::new(port),
        })
    }

    /// Send a room post. Text must be single-line and short; the policy
    /// layer guarantees that before we get here.
    pub fn post(&self, text: &str) -> Result<()> {
        assert!(!text.contains('\n') && !text.contains('\r'));
        let mut w = self.writer.lock().unwrap();
        w.write_all(format!("room.post {text}\r").as_bytes())?;
        w.flush()?;
        Ok(())
    }

    /// Send one media channel-data payload (already the full 0x56/0x58
    /// packet) via the firmware `room.media` CLI. `chan` is "bears"
    /// (default) or "public". Payload must be short enough for one
    /// 151-char CLI line.
    pub fn send_media(&self, payload: &[u8], chan: &str) -> Result<()> {
        // Firmware room.media CLI decodes into a 180-byte buffer; keep this
        // an Err (not assert) so an oversized packet can't kill the drain loop.
        if payload.len() > 180 {
            anyhow::bail!("media packet too long for CLI line ({} B)", payload.len());
        }
        let b64 = crate::media::b64_encode(payload);
        let kind = if payload.first() == Some(&0x72) { "fetch" } else { "media" };
        tx_record(kind, chan, Some(payload), "");
        let mut w = self.writer.lock().unwrap();
        w.write_all(format!("room.media {chan} {b64}\r").as_bytes())?;
        w.flush()?;
        Ok(())
    }

    /// Send a channel TEXT message (e.g. a VE3/VM1/VI1 media announce) via
    /// the firmware `room.text` CLI. `chan` is "bears" or "public".
    pub fn send_text(&self, text: &str, chan: &str) -> Result<()> {
        tx_record("text", chan, None, text);
        let mut w = self.writer.lock().unwrap();
        w.write_all(format!("room.text {chan} {text}\r").as_bytes())?;
        w.flush()?;
        Ok(())
    }

    /// Blocking read loop (run inside `tokio::task::spawn_blocking`).
    /// Emits accepted events into the store log and the broadcast queue.
    pub fn read_loop(
        self: Arc<Self>,
        store: Arc<Store>,
        media: Arc<MediaManager>,
        forward: Option<Arc<crate::forward::Forwarder>>,
        tx: tokio::sync::broadcast::Sender<crate::events::RoomEvent>,
    ) -> Result<()> {
        // Duplicate the fd for reading (try_clone, not a second open:
        // the port may be exclusive) and give it a long timeout.
        let mut port = self
            .writer
            .lock()
            .unwrap()
            .try_clone()
            .context("cloning serial port")?;
        port.set_timeout(std::time::Duration::from_secs(3600))
            .context("setting read timeout")?;
        let reader = BufReader::new(port);
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
                Err(e) => return Err(e).context("serial read"),
            };
            let Some(json) = line.strip_prefix("BRIDGE ") else {
                continue; // CLI echo, debug, stats: not ours
            };
            let Ok(ev) = serde_json::from_str::<BridgeLine>(json) else {
                eprintln!("bearden: unparsable BRIDGE line: {json}");
                continue;
            };
            match ev.evt.as_str() {
                "media" => {
                    if let Some(payload) = crate::media::b64_decode(&ev.b64) {
                        // 0x72 binary fetch: an Open TAK listener is missing
                        // parts of a clip we sent. Answer from the outbound
                        // cache on a side thread (paced sends must not block
                        // the reader).
                        if payload.len() >= 13 && payload[0] == 0x72 {
                            let sid = u32::from_be_bytes([
                                payload[1], payload[2], payload[3], payload[4],
                            ]);
                            let flags = payload[5];
                            let count = payload[12] as usize;
                            let indices = if flags & 1 != 0
                                && payload.len() >= 13 + count
                            {
                                Some(payload[13..13 + count].to_vec())
                            } else {
                                None
                            };
                            let answer = media.fetch_answer(sid, indices.as_deref());
                            if !answer.is_empty() {
                                store.log_event(
                                    "media",
                                    "mesh",
                                    &format!("fetch answer {sid:08x}: resending {} parts", answer.len()),
                                );
                                let chan = crate::forward::ch_word(ev.ch)
                                    .unwrap_or("bears")
                                    .to_string();
                                let serial = self.clone();
                                std::thread::spawn(move || {
                                    for pkt in &answer {
                                        let wire = crate::media::resalt(pkt);
                                        if serial.send_media(&wire, &chan).is_err() {
                                            break;
                                        }
                                        std::thread::sleep(std::time::Duration::from_millis(
                                            crate::media::SEND_PACE_MS,
                                        ));
                                    }
                                });
                            }
                        }
                        media.ingest(
                            &payload,
                            &crate::forward::ch_word(ev.ch).unwrap_or("bears"),
                        );
                        if let Some(fwd) = &forward {
                            fwd.handle_media(&ev, &payload);
                        }
                    } else {
                        eprintln!("bearden: media event with bad base64");
                    }
                }
                "chtext" => {
                    // Channel text (e.g. VE3/VM1/VI1 announces). Forwarded
                    // across channels when configured; not room chat. Also
                    // teach the media reassembler its session mode/total.
                    media.register_envelope(
                        &ev.text,
                        &crate::forward::ch_word(ev.ch).unwrap_or("bears"),
                    );
                    if let Some(fwd) = &forward {
                        fwd.handle_text(&ev);
                    }
                }
                "msg" => {
                    if ev.sys == 1 {
                        // Our own injection echoing back: feed the loop
                        // guard, do not fan out to backends.
                        store.remember_injected(&ev.text);
                        continue;
                    }
                    // Loop guard round 2: even a sys:0 post whose text was
                    // recently injected by us is an echo from another
                    // bridge or a re-shaped bounce.
                    if store.was_injected(&ev.text) {
                        continue;
                    }
                    store.log_event("mesh_msg", &ev.key, &ev.text);
                    let _ = tx.send(crate::events::RoomEvent {
                        kind: crate::events::RoomEventKind::Message,
                        key: ev.key.clone(),
                        ts: ev.ts,
                        text: ev.text.clone(),
                    });
                }
                "join" => {
                    store.log_event("mesh_join", &ev.key, "");
                    let _ = tx.send(crate::events::RoomEvent {
                        kind: crate::events::RoomEventKind::Join,
                        key: ev.key.clone(),
                        ts: ev.ts,
                        text: String::new(),
                    });
                }
                other => eprintln!("bearden: unknown event {other}"),
            }
        }
        unreachable!("serial EOF")
    }
}
