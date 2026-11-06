//! Admin API + built-in web UI: status, user approval, manual send,
//! kill switch. When BEARDEN_ADMIN_TOKEN is set, every route (UI and
//! JSON) requires the token via `Authorization: Bearer <t>` or
//! `?token=<t>`; unset means unauthenticated local-only mode.

use crate::events::BridgeCmd;
use crate::store::{Store, UserState};
use crate::tunnel_backend::{SharedTunnelCfg, TunnelConfig};
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast;

type ApiState = (
    Arc<Store>,
    crate::backends::Egress,
    broadcast::Sender<BridgeCmd>,
    SharedTunnelCfg,
    Arc<crate::media::MediaManager>,
    Option<Arc<crate::forward::Forwarder>>,
);

pub fn router(
    store: Arc<Store>,
    egress: crate::backends::Egress,
    token: Option<String>,
    cmd_tx: broadcast::Sender<BridgeCmd>,
    tunnel_cfg: SharedTunnelCfg,
    media: Arc<crate::media::MediaManager>,
    forward: Option<Arc<crate::forward::Forwarder>>,
) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/status", get(status))
        .route("/events", get(events))
        .route("/users", get(users))
        .route("/users/{backend}/{uid}/approve", post(approve))
        .route("/users/{backend}/{uid}/ban", post(ban))
        .route("/users/{backend}/{uid}/deny", post(deny))
        .route("/users/{backend}/{uid}/link", post(link))
        .route("/users/{backend}/{uid}/verify", post(verify))
        .route("/users/{backend}/{uid}/vouch", post(vouch))
        .route("/send", post(send))
        .route("/media", get(media_list))
        .route("/media/{id}/file", get(media_file))
        .route("/media/send", post(media_send))
        .route("/media/debug", get(media_debug))
        .route("/killswitch/{on}", post(killswitch))
        .route("/tunnel/flush", post(tunnel_flush))
        .route("/tunnel/config", get(tunnel_config_get).post(tunnel_config_set))
        .route("/forward/config", get(forward_config_get).post(forward_config_set))
        .route("/forward/queue", get(forward_queue))
        .route_layer(middleware::from_fn_with_state(
            token.clone(),
            auth,
        ))
        // Tunnel peer routes: outside the admin token; they carry their
        // own shared-secret auth (X-Tunnel-Token) and reject if unset.
        .route("/tunnel/ingest", post(tunnel_ingest))
        .route("/tunnel/peek", get(tunnel_peek))
        .route("/tunnel/status", get(tunnel_status))
        .with_state((store, egress, cmd_tx, tunnel_cfg, media, forward))
}

async fn auth(
    State(token): State<Option<String>>,
    req: Request,
    next: Next,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(want) = token {
        let given = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|s| s.to_string())
            .or_else(|| {
                req.uri().path_and_query()?.query()?.split('&').find_map(|p| {
                    let (k, v) = p.split_once('=')?;
                    (k == "token").then(|| urldecode(v))
                })
            });
        if given.as_deref() != Some(want.as_str()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(req).await)
}

fn urldecode(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(v);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn index() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}

async fn status(State((store, _, _, _, _, _)): State<ApiState>) -> Json<Value> {
    Json(json!({
        "kill_switch": store.kill_switch(),
        "users": store.list_users().len(),
    }))
}

async fn events(State((store, _, _, _, _, _)): State<ApiState>) -> Json<Value> {
    Json(json!(store
        .recent_events(100)
        .into_iter()
        .map(|(ts, kind, source, text)| json!({"ts": ts, "kind": kind, "source": source, "text": text}))
        .collect::<Vec<_>>()))
}

async fn users(State((store, _, _, _, _, _)): State<ApiState>) -> Json<Value> {
    Json(json!(store
        .list_users()
        .into_iter()
        .map(|(k, n, s, c)| json!({"key": k, "name": n, "state": s, "console_uid": c}))
        .collect::<Vec<_>>()))
}

/// Console-initiated identity linking: stores a 6-digit challenge and asks
/// the backend to DM it to the user (for XMPP, a room private message to
/// the nick). Ownership is proven by calling /verify with the code.
async fn link(
    State((store, _, cmd_tx, _, _, _)): State<ApiState>,
    Path((backend, uid)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let console_uid = body["console_uid"].as_str().unwrap_or_default().to_string();
    let display_name = body["display_name"].as_str().unwrap_or(&uid).to_string();
    if console_uid.is_empty() {
        return Json(json!({"ok": false, "error": "console_uid required"}));
    }
    // Coarse randomness from the clock: codes are 6 digits, 15-min TTL,
    // rate-limited by being console-triggered.
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let code = format!("{:06}", (nanos.wrapping_mul(2654435761)) % 1_000_000);
    if let Err(reason) = store.begin_link(&backend, &uid, &display_name, &console_uid, &code) {
        return Json(json!({"ok": false, "error": reason}));
    }
    store.log_event("link_challenge", &uid, "verification code sent");
    let _ = cmd_tx.send(BridgeCmd {
        backend: backend.clone(),
        backend_uid: uid.clone(),
        text: format!("Bear Den bridge verification code: {code} (valid 15 minutes)"),
    });
    Json(json!({"ok": true, "expires_in": 900}))
}

/// Token-authenticated vouch: the console provisioned this account itself,
/// so ownership is known and approval is direct (no challenge round trip).
async fn vouch(
    State((store, _, _, _, _, _)): State<ApiState>,
    Path((backend, uid)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let console_uid = body["console_uid"].as_str().unwrap_or_default().to_string();
    let display_name = body["display_name"].as_str().unwrap_or(&uid).to_string();
    if console_uid.is_empty() {
        return Json(json!({"ok": false, "error": "console_uid required"}));
    }
    store.upsert_user(&backend, &uid, &display_name);
    store.set_console_uid(&backend, &uid, &console_uid);
    store.set_state(&backend, &uid, UserState::Approved);
    store.log_event("vouched", &uid, "provisioned by console");
    Json(json!({"ok": true}))
}

async fn verify(
    State((store, _, _, _, _, _)): State<ApiState>,
    Path((backend, uid)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let code = body["code"].as_str().unwrap_or_default();
    match store.verify_link(&backend, &uid, code) {
        Ok(()) => {
            store.log_event("link_verified", &uid, "identity verified via code");
            Json(json!({"ok": true}))
        }
        Err(reason) => Json(json!({"ok": false, "error": reason})),
    }
}

async fn approve(
    State((store, _, _, _, _, _)): State<ApiState>,
    Path((backend, uid)): Path<(String, String)>,
) -> Json<Value> {
    store.set_state(&backend, &uid, UserState::Approved);
    Json(json!({"ok": true}))
}

async fn ban(
    State((store, _, _, _, _, _)): State<ApiState>,
    Path((backend, uid)): Path<(String, String)>,
) -> Json<Value> {
    store.set_state(&backend, &uid, UserState::Banned);
    Json(json!({"ok": true}))
}

/// Deny a pending request: the user row (and any outstanding challenge)
/// is deleted, so the request disappears. If they show up again they
/// start fresh as pending.
async fn deny(
    State((store, _, _, _, _, _)): State<ApiState>,
    Path((backend, uid)): Path<(String, String)>,
) -> Json<Value> {
    store.remove_user(&backend, &uid);
    Json(json!({"ok": true}))
}

/// Admin test-send: posts to the room as the given name, bypassing
/// approval (admins are trusted) but still subject to the kill switch.
async fn send(
    State((store, egress, _, _, _, _)): State<ApiState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let name = body["name"].as_str().unwrap_or("bridge").to_string();
    let text = body["text"].as_str().unwrap_or_default().to_string();
    if store.kill_switch() {
        return Json(json!({"ok": false, "error": "kill switch engaged"}));
    }
    let staged: String = text
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(crate::store::MAX_RF_CHARS)
        .collect();
    let staged = format!("{name}: {staged}");
    store.log_event("rf_out", "admin", &staged);
    store.remember_injected(&staged);
    match egress.serial.post(&staged) {
        Ok(()) => Json(json!({"ok": true, "sent": staged})),
        Err(e) => Json(json!({"ok": false, "error": e.to_string()})),
    }
}

async fn killswitch(
    State((store, _, _, _, _, _)): State<ApiState>,
    Path(on): Path<String>,
) -> Json<Value> {
    let on = on == "on" || on == "1" || on == "true";
    store.set_kill_switch(on);
    Json(json!({"ok": true, "kill_switch": on}))
}

// ---- inter-bridge tunnel (peer-facing + admin) ----

/// Check the tunnel shared secret. Returns Err(status) on failure.
fn tunnel_auth(
    cfg: &SharedTunnelCfg,
    req_token: Option<&str>,
) -> Result<(), StatusCode> {
    let guard = cfg.read().unwrap();
    match guard.as_ref() {
        Some(cfg) if !cfg.token.is_empty() && req_token == Some(cfg.token.as_str()) => Ok(()),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

/// Peer pushes a batch of its mesh messages; they are queued locally for
/// the next RF digest flush (store-and-forward, never immediate).
async fn tunnel_ingest(
    State((store, _, _, cfg, _, _)): State<ApiState>,
    req_headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    tunnel_auth(&cfg, req_headers.get("x-tunnel-token").and_then(|v| v.to_str().ok()))?;
    let Some(cfg) = cfg.read().unwrap().clone() else {
        return Err(StatusCode::NOT_FOUND);
    };
    let tag = body["tag"].as_str().unwrap_or("?").to_string();
    let Some(msgs) = body["msgs"].as_array() else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let mut queued = 0usize;
    for m in msgs.iter().take(100) {
        let (key, text) = (
            m["key"].as_str().unwrap_or("?"),
            m["text"].as_str().unwrap_or(""),
        );
        if text.is_empty() || cfg.is_digest(text) {
            continue; // never re-tunnel a digest (echo loop)
        }
        store.tunnel_enqueue(&tag, key, &text.split_whitespace().collect::<Vec<_>>().join(" "));
        queued += 1;
    }
    store.log_event("tunnel_in", &tag, &format!("{queued} msgs queued"));
    Ok(Json(json!({"ok": true, "queued": queued})))
}

/// Peer pulls our mesh events since a cursor (poll mode counterpart).
async fn tunnel_peek(
    State((store, _, _, cfg, _, _)): State<ApiState>,
    req_headers: axum::http::HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    tunnel_auth(&cfg, req_headers.get("x-tunnel-token").and_then(|v| v.to_str().ok()))?;
    let since: i64 = params.get("since").and_then(|s| s.parse().ok()).unwrap_or(0);
    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
        .min(100);
    let msgs: Vec<_> = store
        .tunnel_peek(since, limit)
        .into_iter()
        .map(|(id, ts, key, text)| json!({"id": id, "ts": ts, "key": key, "text": text}))
        .collect();
    Ok(Json(json!({"ok": true, "msgs": msgs})))
}

async fn tunnel_status(State((store, _, _, cfg, _, _)): State<ApiState>) -> Json<Value> {
    let cfg = cfg.read().unwrap().clone();
    let (pending, sent) = store.tunnel_queue_depth();
    Json(json!({
        "enabled": cfg.is_some(),
        "peer": cfg.as_ref().map(|c| c.peer.clone()),
        "tag": cfg.as_ref().map(|c| c.tag.clone()),
        "flush_interval_secs": cfg.as_ref().map(|c| c.flush_interval.as_secs()),
        "queue_pending": pending,
        "queue_sent": sent,
    }))
}

/// Admin: force an immediate digest flush (still rate-limited per post).
async fn tunnel_flush(
    State((store, egress, _, cfg, _, _)): State<ApiState>,
) -> Json<Value> {
    let Some(cfg) = cfg.read().unwrap().clone() else {
        return Json(json!({"ok": false, "error": "tunnel not configured"}));
    };
    if store.kill_switch() {
        return Json(json!({"ok": false, "error": "kill switch engaged"}));
    }
    let sent = crate::tunnel_backend::run_flush(&store, &egress.serial, &cfg).await;
    Json(json!({"ok": true, "sent": sent}))
}

/// Admin: current tunnel configuration (includes the shared secret).
async fn tunnel_config_get(State((_, _, _, cfg, _, _)): State<ApiState>) -> Json<Value> {
    let cfg = cfg.read().unwrap().clone();
    Json(json!({
        "ok": true,
        "enabled": cfg.is_some(),
        "config": cfg.as_ref().map(|c| c.to_json()),
    }))
}

/// Admin: enable/reconfigure/disable the tunnel at runtime. Body:
/// {"enabled": bool, "peer", "token", "tag", "peer_tag",
///  "flush_interval_secs", "mode", ...}. Persisted in the DB so it
/// survives restarts; env vars act only as the initial default.
async fn tunnel_config_set(
    State((store, _, _, cfg, _, _)): State<ApiState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let enabled = body["enabled"].as_bool().unwrap_or(true);
    if !enabled {
        *cfg.write().unwrap() = None;
        // Persist a disabled sentinel (NOT a delete): deleting the row
        // would let startup fall back to BEARDEN_TUNNEL_* env and silently
        // re-enable the tunnel after a restart.
        store.set_config("tunnel", "{\"disabled\":true}");
        store.log_event("tunnel_cfg", "admin", "tunnel disabled");
        return Json(json!({"ok": true, "enabled": false}));
    }
    let merged = {
        // Fields omitted from the request keep their current/env values.
        let current = cfg.read().unwrap().clone().map(|c| c.to_json());
        let mut m = body.clone();
        if let Some(cur) = current {
            for k in ["peer", "token", "tag", "peer_tag", "flush_interval_secs",
                      "max_msgs", "max_posts", "rf_gap_secs", "mode"] {
                if m.get(k).map(|v| v.is_null()).unwrap_or(true) {
                    if let Some(v) = cur.get(k) {
                        m[k] = v.clone();
                    }
                }
            }
        }
        m
    };
    match TunnelConfig::from_json(&merged) {
        Ok(new_cfg) => {
            store.set_config("tunnel", &new_cfg.to_json().to_string());
            *cfg.write().unwrap() = Some(new_cfg.clone());
            store.log_event(
                "tunnel_cfg",
                "admin",
                &format!("tunnel configured toward {} (tag {})", new_cfg.peer, new_cfg.tag),
            );
            Json(json!({"ok": true, "enabled": true, "config": new_cfg.to_json()}))
        }
        Err(e) => Json(json!({"ok": false, "error": e})),
    }
}

// ---- channel forwarding (Bears <-> Bears2 den-to-den routing) ----

async fn forward_config_get(
    State((store, _, _, _, _, forward)): State<ApiState>,
) -> Json<Value> {
    match forward {
        Some(f) => {
            let cfg = f.cfg.read().unwrap().clone();
            Json(json!({
                "ok": true,
                "enabled": cfg.enabled,
                "in": crate::forward::ch_word(cfg.in_idx),
                "out": crate::forward::ch_word(cfg.out_idx),
                "pace_ms": cfg.pace_ms,
                "queue_pending": store.forward_depth(),
            }))
        }
        None => Json(json!({"ok": false, "error": "forwarding not configured"})),
    }
}

/// Admin: configure forwarding at runtime. Body: {"enabled", "in", "out",
/// "pace_ms"} where in/out are channel names (bears|public|bears2).
/// Persisted in the DB config table.
async fn forward_config_set(
    State((store, _, _, _, _, forward)): State<ApiState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let Some(f) = forward else {
        return Json(json!({"ok": false, "error": "forwarding not configured"}));
    };
    let cur = f.cfg.read().unwrap().clone();
    let in_idx = match body["in"].as_str() {
        Some(w) => match crate::forward::ch_idx(w) {
            Some(i) => i,
            None => return Json(json!({"ok": false, "error": format!("unknown channel {w}")})),
        },
        None => cur.in_idx,
    };
    let out_idx = match body["out"].as_str() {
        Some(w) => match crate::forward::ch_idx(w) {
            Some(i) => i,
            None => return Json(json!({"ok": false, "error": format!("unknown channel {w}")})),
        },
        None => cur.out_idx,
    };
    if in_idx == out_idx {
        return Json(json!({"ok": false, "error": "in and out must differ"}));
    }
    let enabled = body["enabled"].as_bool().unwrap_or(cur.enabled);
    let pace_ms = body["pace_ms"].as_u64().unwrap_or(cur.pace_ms).clamp(200, 60000);
    let new_cfg = crate::forward::ForwardConfig { enabled, in_idx, out_idx, pace_ms };
    f.set_config(new_cfg.clone());
    *f.cfg.write().unwrap() = new_cfg;
    store.log_event(
        "forward_cfg",
        "admin",
        &format!("forward enabled={} in={} out={} pace_ms={}", enabled, in_idx, out_idx, pace_ms),
    );
    Json(json!({"ok": true, "enabled": enabled}))
}

async fn forward_queue(State((store, _, _, _, _, _)): State<ApiState>) -> Json<Value> {
    Json(json!({"ok": true, "pending": store.forward_depth()}))
}

// ---- media (voice/photo clips over the mesh) ----

async fn media_list(State((store, _, _, _, _, _)): State<ApiState>) -> Json<Value> {
    Json(json!(store
        .media_list()
        .into_iter()
        .map(|(id, sid, kind, mode, status, filename, size, ts)| json!({
            "id": id, "sid": sid, "kind": kind, "mode": mode,
            "status": status, "filename": filename, "size_bytes": size, "ts": ts
        }))
        .collect::<Vec<_>>()))
}

async fn media_debug(State((_, _, _, _, media, _)): State<ApiState>) -> Json<Value> {
    Json(media.debug_dump())
}

async fn media_file(
    State((store, _, _, _, media, _)): State<ApiState>,
    Path(id): Path<i64>,
) -> Result<Vec<u8>, (StatusCode, &'static str)> {
    let Some((filename, _)) = store.media_file(id) else {
        return Err((StatusCode::NOT_FOUND, "no such completed clip"));
    };
    let path = std::path::Path::new(media.dir()).join(&filename);
    std::fs::read(&path).map_err(|_| (StatusCode::NOT_FOUND, "file missing on disk"))
}

/// Chunk a base64 clip into paced radio2 media packets. Returns
/// immediately; completion is observable via GET /media on the peers.
async fn media_send(
    State((store, egress, _, _, media, _)): State<ApiState>,
    Json(body): Json<Value>,
) -> Json<Value> {
    if store.kill_switch() {
        return Json(json!({"ok": false, "error": "kill switch engaged"}));
    }
    let Some(data) = body["b64"].as_str().and_then(crate::media::b64_decode) else {
        return Json(json!({"ok": false, "error": "b64 required"}));
    };
    // Duplicate-send guard: a double click (or console retry) of the same
    // clip bytes within this window would flood two interleaved copies of
    // every packet — heavy airtime and lost tails at the receivers.
    {
        use std::sync::Mutex;
        static RECENT: Mutex<Vec<(u64, u64)>> = Mutex::new(Vec::new());
        const WINDOW_SECS: u64 = 120;
        let h = crate::media::fnv1a(&data);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut r = RECENT.lock().unwrap();
        r.retain(|(_, t)| now.saturating_sub(*t) < WINDOW_SECS);
        if r.iter().any(|(eh, _)| *eh == h) {
            return Json(json!({"ok": false, "error": "duplicate clip already flooding (within 2 min)"}));
        }
        r.push((h, now));
    }
    let kind = body["kind"].as_str().unwrap_or("voice").to_string();
    let chan = match body["chan"].as_str().unwrap_or("bears") {
        "public" => "public",
        "bears2" => "bears2",
        _ => "bears",
    };
    let mode = match kind.as_str() {
        "photo" => 40,
        _ => body["mode"].as_u64().unwrap_or(1) as u32,
    };
    let Some((sid, packets)) = crate::media::MediaManager::chunk_outbound(&data, mode) else {
        return Json(json!({"ok": false, "error":
            format!("clip must be 1..{} bytes", crate::media::MAX_CLIP_BYTES)}));
    };
    let n = packets.len();
    let eta = n as u64 * crate::media::SEND_PACE_MS / 1000;
    // VE3/VM1/VI1 announce (base36 fields) so Open TAK clients render the
    // clip in the channel feed; the data packets alone are not displayed.
    fn b36(v: u32) -> String {
        let mut s = String::new();
        let mut v = v;
        if v == 0 { return "0".into(); }
        while v > 0 { s.insert(0, char::from_digit(v % 36, 36).unwrap()); v /= 36; }
        s
    }
    let bitrate = match mode {
        1 => 1200,
        2 => 2400,
        3 => 1300,
        4 => 1400,
        5 => 1600,
        6 => 3200,
        _ => 700,
    };
    let dur = if mode == 40 {
        body["aspect"].as_u64().unwrap_or(100).clamp(1, 600) as u32
    } else {
        (data.len() as u64 * 8 / bitrate as u64).max(1) as u32
    };
    // Open TAK vox-voice spec: VE3 = Codec2, VM1 = Mimi (mode 32), VI1 = photo
    // (mode 40; dur field = aspect ratio x100). Fields are lowercase base36,
    // NO caption suffix.
    let verb = match mode { 40 => "VI1", 32 => "VM1", _ => "VE3" };
    let announce = format!("{}:{}:{}:{}:{}", verb, b36(sid), b36(mode), b36(n as u32), b36(dur));
    store.log_event("media_out", "admin", &format!("{kind} clip {sid:08x} ({n} packets)"));
    let serial = egress.serial.clone();
    let store2 = store.clone();
    media.record_outbound(sid, &packets); // enables 0x72 fetch answers
    // Our own announce never comes back to us as chtext, but our packets DO
    // (echoed/forwarded back by the peer den) — self-register the total so
    // that echo session completes at full size instead of early-finalizing.
    media.register_envelope(&announce, chan);
    // Self-originated floods are never echoed back as serial receive events,
    // so the forwarder would never see them — enqueue explicitly so the clip
    // (and its announce) also lands on the forwarder's out channel.
    store.forward_enqueue("text", &announce);
    for p in &packets {
        store.forward_enqueue("media", &crate::media::b64_encode(p));
    }
    tokio::spawn(async move {
        if let Err(e) = serial.send_text(&announce, chan) {
            store2.log_event("media_out", "admin", &format!("announce failed: {e}"));
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await; // let the announce land first
        }
        // Two salted passes: RF delivery of individual flood packets is lossy
        // and MeshCore dedups byte-identical floods, so every transmission is
        // resalted (receivers dedup media parts by sid+index).
        for pass in 0..2 {
            // The first radio2 datagram right after the text announce is
            // frequently dropped (timeslice switch) — send packet 0 alone,
            // let the radio settle, then run the full pass.
            if let Err(e) = serial.send_media(&crate::media::resalt(&packets[0]), chan) {
                store2.log_event("media_out", "admin", &format!("packet 0 pre-send failed: {e}"));
                return;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            for (i, p) in packets.iter().enumerate() {
                let wire = crate::media::resalt(p);
                if let Err(e) = serial.send_media(&wire, chan) {
                    store2.log_event("media_out", "admin", &format!("packet {i} pass {pass} failed: {e}"));
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(crate::media::SEND_PACE_MS)).await;
            }
            if pass == 0 {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
        store2.log_event("media_out", "admin", &format!("clip {sid:08x} fully handed to radio"));
        // Opt-in 0x58 left=0 terminator (A/B test for Open TAK clients that
        // keyed session completion on it before we stopped sending it).
        if body["terminator"].as_bool().unwrap_or(false) {
            let mut last = packets[n - 1].clone();
            last[0] = 0x58;
            if let Some(t) = last.last_mut() {
                *t = 0; // packets-left = 0 => complete
            }
            let _ = serial.send_media(&last, chan);
            store2.log_event("media_out", "admin", &format!("clip {sid:08x} 0x58 terminator sent"));
        }
    });
    Json(json!({"ok": true, "sid": format!("{sid:08x}"), "kind": kind, "mode": mode, "chan": chan,
                "packets": n, "eta_secs": eta}))
}
