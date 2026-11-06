//! Matrix backend: bridges the mesh room to a Matrix room on a local (or
//! remote) homeserver, using the plain client-server API — no E2EE, which
//! is fine for a LAN bridge room and keeps dependencies light.
//!
//! Mesh events are sent to the room as the bridge user; messages typed in
//! the room by other members go through the policy engine and out over RF.
//! Verification codes are delivered as DMs (auto-created direct rooms).
//!
//! Config (all required to enable): BEARDEN_MATRIX_HOMESERVER (base URL,
//! e.g. http://matrix.server1.example.net:8008), BEARDEN_MATRIX_USER,
//! BEARDEN_MATRIX_PASSWORD, BEARDEN_MATRIX_ROOM (room id or alias).

use std::collections::HashMap;
use std::time::Duration;

use crate::backends::{Backend, BackendCtx};
use async_trait::async_trait;
use serde_json::{json, Value};

pub struct MatrixBackend {
    pub homeserver: String,
    pub user: String,
    pub password: String,
    pub room: String,
}

impl MatrixBackend {
    pub fn from_env() -> Option<Self> {
        let homeserver = std::env::var("BEARDEN_MATRIX_HOMESERVER").ok()?;
        let user = std::env::var("BEARDEN_MATRIX_USER").ok()?;
        let password = std::env::var("BEARDEN_MATRIX_PASSWORD").ok()?;
        let room = std::env::var("BEARDEN_MATRIX_ROOM").ok()?;
        Some(MatrixBackend {
            homeserver: homeserver.trim_end_matches('/').to_string(),
            user,
            password,
            room,
        })
    }
}

/// Minimal Matrix client: access token + reqwest. Errors bubble up as
/// strings; the run loop decides what is fatal.
struct Mx {
    http: reqwest::Client,
    hs: String,
    token: String,
    user_id: String,
    /// Cached DM room ids by user id.
    dms: HashMap<String, String>,
}

impl Mx {
    async fn login(hs: &str, user: &str, password: &str) -> anyhow::Result<Mx> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(70)) // long-poll /sync up to 60s
            .build()?;
        // Prefer a pre-supplied access token (avoids rate-limited logins on
        // restart); fall back to password login if it is rejected.
        if let Ok(tok) = std::env::var("BEARDEN_MATRIX_TOKEN") {
            if !tok.is_empty() {
                let url = format!("{hs}/_matrix/client/r0/account/whoami");
                let sep = if url.contains('?') { '&' } else { '?' };
                let url = format!("{url}{sep}access_token={tok}");
                if let Ok(v) = http.get(&url).send().await {
                    if v.status().is_success() {
                        if let Ok(w) = v.json::<Value>().await {
                            if let Some(uid) = w["user_id"].as_str() {
                                eprintln!("[matrix backend] using env access token for {uid}");
                                return Ok(Mx {
                                    http,
                                    hs: hs.to_string(),
                                    token: tok,
                                    user_id: uid.to_string(),
                                    dms: HashMap::new(),
                                });
                            }
                        }
                    }
                }
            }
        }
        let url = format!("{hs}/_matrix/client/r0/login");
        let resp: Value = http
            .post(&url)
            .json(&json!({
                "type": "m.login.password",
                "identifier": { "type": "m.id.user", "user": user },
                "password": password,
            }))
            .send()
            .await?
            .json()
            .await?;
        let token = resp["access_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("login failed: {resp}"))?
            .to_string();
        let user_id = resp["user_id"].as_str().unwrap_or_default().to_string();
        Ok(Mx {
            http,
            hs: hs.to_string(),
            token,
            user_id,
            dms: HashMap::new(),
        })
    }

    async fn get(&self, path: &str) -> Result<Value, String> {
        let sep = if path.contains('?') { '&' } else { '?' };
        let url = format!("{}/_matrix/client/r0{path}{sep}access_token={}", self.hs, self.token);
        let resp = self.http.get(&url).send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("{} {}", status, body["error"].as_str().unwrap_or("")));
        }
        Ok(body)
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let sep = if path.contains('?') { '&' } else { '?' };
        let url = format!("{}/_matrix/client/r0{path}{sep}access_token={}", self.hs, self.token);
        let resp = self
            .http
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let out: Value = resp.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("{} {}", status, out["error"].as_str().unwrap_or("")));
        }
        Ok(out)
    }

    async fn put(&self, path: &str, body: &Value) -> Result<Value, String> {
        let sep = if path.contains('?') { '&' } else { '?' };
        let url = format!("{}/_matrix/client/r0{path}{sep}access_token={}", self.hs, self.token);
        let resp = self
            .http
            .put(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let out: Value = resp.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("{} {}", status, out["error"].as_str().unwrap_or("")));
        }
        Ok(out)
    }

    /// Resolve a room alias to a room id (returns input if already an id).
    async fn resolve_room(&self, room: &str) -> Result<String, String> {
        if room.starts_with('!') {
            return Ok(room.to_string());
        }
        let enc = room.replace('#', "%23").replace(':', "%3A");
        let v = self.get(&format!("/directory/room/{enc}")).await?;
        v["room_id"]
            .as_str()
            .map(String::from)
            .ok_or_else(|| format!("alias {room} did not resolve"))
    }

    async fn join(&self, room: &str) -> Result<String, String> {
        let v = self.post(&format!("/join/{room}"), &json!({})).await?;
        Ok(v["room_id"].as_str().unwrap_or(room).to_string())
    }

    async fn send_text(&self, room_id: &str, text: &str) -> Result<(), String> {
        let txn = format!("b{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis());
        self.put(
            &format!("/rooms/{room_id}/send/m.room.message/{txn}"),
            &json!({ "msgtype": "m.text", "body": text }),
        )
        .await
        .map(|_| ())
    }

    /// Send a DM: reuse the cached direct room with this user, else create
    /// one (inviting them) and remember it.
    async fn dm(&mut self, user_id: &str, text: &str) -> Result<(), String> {
        let room_id = match self.dms.get(user_id) {
            Some(r) => r.clone(),
            None => {
                let v = self
                    .post(
                        "/createRoom",
                        &json!({
                            "is_direct": true,
                            "preset": "trusted_private_chat",
                            "invite": [user_id],
                        }),
                    )
                    .await?;
                let r = v["room_id"].as_str().ok_or("createRoom returned no id")?.to_string();
                self.dms.insert(user_id.to_string(), r.clone());
                r
            }
        };
        self.send_text(&room_id, text).await
    }
}

#[async_trait]
impl Backend for MatrixBackend {

    async fn run(self: Box<Self>, mut ctx: BackendCtx) -> anyhow::Result<()> {
        let mut mx = Mx::login(&self.homeserver, &self.user, &self.password).await?;
        println!("[matrix backend] logged in as {}", mx.user_id);
        let room_id = {
            let resolved = mx.resolve_room(&self.room).await.map_err(anyhow::Error::msg)?;
            mx.join(&resolved).await.map_err(anyhow::Error::msg)?
        };
        println!("[matrix backend] joined {room_id}");
        // Use a fresh sync so we only see messages from now on.
        let init = mx
            .get("/sync?timeout=0")
            .await
            .map_err(anyhow::Error::msg)?;
        let mut since = init["next_batch"].as_str().unwrap_or_default().to_string();

        loop {
            tokio::select! {
                ev = ctx.inbound.recv() => {
                    let Ok(ev) = ev else { continue };
                    let line = match ev.kind {
                        crate::events::RoomEventKind::Join => {
                            format!("[mesh] {} joined the room", ev.key)
                        }
                        crate::events::RoomEventKind::Message => {
                            format!("[mesh] {}: {}", ev.key, ev.text)
                        }
                    };
                    if let Err(e) = mx.send_text(&room_id, &line).await {
                        eprintln!("[matrix backend] send failed: {e}");
                    }
                }
                cmd = ctx.cmd_rx.recv() => {
                    let Ok(cmd) = cmd else { continue };
                    if cmd.backend != "matrix" { continue; }
                    // Verification DM: backend_uid is a Matrix user id
                    // (bare localpart or full @user:server form).
                    let uid = if cmd.backend_uid.starts_with('@') {
                        cmd.backend_uid.clone()
                    } else {
                        let server = mx.hs
                            .trim_start_matches("http://")
                            .trim_start_matches("https://")
                            .trim_end_matches(":8008");
                        format!("@{}:{server}", cmd.backend_uid)
                    };
                    match mx.dm(&uid, &cmd.text).await {
                        Ok(()) => println!("[matrix backend] verification DM sent to {uid}"),
                        Err(e) => eprintln!("[matrix backend] DM to {uid} failed: {e}"),
                    }
                }
                sync_path = async {
                    let path = format!("/sync?timeout=60000&since={since}");
                    mx.get(&path).await
                } => {
                    let v = match sync_path {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[matrix backend] sync error: {e}; retrying in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            // Re-login in case the token expired.
                            mx = Mx::login(&self.homeserver, &self.user, &self.password).await?;
                            let _ = mx.join(&room_id).await;
                            continue;
                        }
                    };
                    since = v["next_batch"].as_str().unwrap_or(&since).to_string();
                    // Accept invites.
                    if let Some(invites) = v["rooms"]["invite"].as_object() {
                        for r in invites.keys() {
                            println!("[matrix backend] accepting invite to {r}");
                            let _ = mx.join(r).await;
                        }
                    }
                    let Some(joined) = v["rooms"]["join"].as_object() else { continue };
                    let Some(events) = joined.get(&room_id).and_then(|r| r["timeline"]["events"].as_array()) else { continue };
                    for ev in events {
                        if ev["type"] != "m.room.message" { continue; }
                        let sender = ev["sender"].as_str().unwrap_or_default();
                        if sender == mx.user_id || sender.is_empty() { continue; }
                        let Some(body) = ev["content"]["body"].as_str() else { continue };
                        // localpart of the sender is the backend identity
                        let uid = sender.trim_start_matches('@').split(':').next().unwrap_or(sender);
                        if let Err(reason) = ctx.egress.submit(crate::events::OutboundMessage {
                            backend: "matrix".into(),
                            backend_uid: uid.to_string(),
                            display_name: uid.to_string(),
                            text: body.to_string(),
                        }).await {
                            let _ = mx.send_text(&room_id, &format!("{uid}: not sent ({reason})")).await;
                        }
                    }
                }
            }
        }
    }
}
