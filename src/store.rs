//! Identity registry + policy state, persisted in SQLite.
//!
//! Approve-before-transmit: internet users must be approved before their
//! first RF message. Quotas and clipping apply to everyone.

use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const MAX_RF_CHARS: usize = 180; // MeshCore packet budget, conservative
const QUOTA_WINDOW: Duration = Duration::from_secs(3600);
const QUOTA_MESSAGES: usize = 20;

#[derive(Debug, Clone, PartialEq)]
pub enum UserState {
    Pending,
    Approved,
    Banned,
}

pub struct Store {
    db: Mutex<Connection>,
    /// (user_key, timestamps of accepted messages) for quota accounting.
    quota: Mutex<Vec<(String, Instant)>>,
    /// Recently injected content hashes: mesh->backend->mesh loop guard.
    recent_hashes: Mutex<HashSet<u64>>,
    kill_switch: Mutex<bool>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS users (
                key TEXT PRIMARY KEY,           -- '<backend>:<backend_uid>'
                display_name TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                approved_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                kind TEXT NOT NULL,
                source TEXT NOT NULL,
                text TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tunnel_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                origin TEXT NOT NULL,          -- region tag of the far mesh
                sender TEXT NOT NULL,          -- mesh key prefix on the far mesh
                text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'  -- pending|sent
            );",
        )?;
        // Linking columns (added later; harmless if they already exist).
        let _ = db.execute_batch("ALTER TABLE users ADD COLUMN console_uid TEXT;");
        let _ = db.execute_batch("ALTER TABLE users ADD COLUMN challenge TEXT;");
        let _ = db.execute_batch("ALTER TABLE users ADD COLUMN challenged_at INTEGER;");
        let _ = db.execute_batch(
            "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        );
        let _ = db.execute_batch(
            "CREATE TABLE IF NOT EXISTS bridge_media (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sid TEXT UNIQUE,
                kind TEXT NOT NULL,
                mode INTEGER NOT NULL DEFAULT 0,
                total INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'partial',
                filename TEXT,
                size_bytes INTEGER,
                created_at INTEGER NOT NULL
            );",
        );
        let _ = db.execute_batch(
            "CREATE TABLE IF NOT EXISTS forward_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                created_at INTEGER NOT NULL
            );",
        );
        let _ = db.execute_batch(
            "CREATE TABLE IF NOT EXISTS forward_dedup (k INTEGER PRIMARY KEY, ts INTEGER NOT NULL);",
        );
        Ok(Store {
            db: Mutex::new(db),
            quota: Mutex::new(Vec::new()),
            recent_hashes: Mutex::new(HashSet::new()),
            kill_switch: Mutex::new(false),
        })
    }

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    pub fn log_event(&self, kind: &str, source: &str, text: &str) {
        let _ = self.db.lock().unwrap().execute(
            "INSERT INTO events (ts, kind, source, text) VALUES (?, ?, ?, ?)",
            rusqlite::params![Self::now(), kind, source, text],
        );
    }

    pub fn media_complete_size(&self, sid: &str) -> Option<i64> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT size_bytes FROM bridge_media WHERE sid = ? AND status = 'complete'",
                rusqlite::params![sid],
                |r| r.get(0),
            )
            .ok()
    }

    pub fn media_complete(
        &self,
        sid: &str,
        kind: &str,
        mode: u32,
        total: u32,
        filename: &str,
        size: i64,
    ) {
        let _ = self.db.lock().unwrap().execute(
            "INSERT INTO bridge_media (sid, kind, mode, total, status, filename, size_bytes, created_at) \
             VALUES (?, ?, ?, ?, 'complete', ?, ?, ?) \
             ON CONFLICT(sid) DO UPDATE SET kind=excluded.kind, mode=excluded.mode, \
             total=excluded.total, status='complete', filename=excluded.filename, \
             size_bytes=excluded.size_bytes",
            rusqlite::params![sid, kind, mode as i64, total as i64, filename, size, Self::now()],
        );
    }

    pub fn media_error(&self, sid: &str) {
        let _ = self.db.lock().unwrap().execute(
            "UPDATE bridge_media SET status='error' WHERE sid = ?",
            [sid],
        );
    }

    pub fn media_list(&self) -> Vec<(i64, String, String, i64, String, String, i64, i64)> {
        self.db
            .lock()
            .unwrap()
            .prepare(
                "SELECT id, sid, kind, mode, status, COALESCE(filename,''), \
                 COALESCE(size_bytes,0), created_at FROM bridge_media ORDER BY id DESC LIMIT 100",
            )
            .and_then(|mut s| {
                let rows = s.query_map([], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                        r.get(7)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default()
    }

    pub fn media_file(&self, id: i64) -> Option<(String, String)> {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT COALESCE(filename,''), status FROM bridge_media WHERE id = ?",
                [id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .ok()
            .filter(|(f, status)| !f.is_empty() && status == "complete")
    }

    pub fn recent_events(&self, limit: usize) -> Vec<(i64, String, String, String)> {
        self.db
            .lock()
            .unwrap()
            .prepare("SELECT ts, kind, source, text FROM events ORDER BY id DESC LIMIT ?")
            .and_then(|mut s| {
                let rows = s.query_map([limit as i64], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default()
    }

    /// Upsert a backend user; returns their current state.
    pub fn upsert_user(&self, backend: &str, uid: &str, name: &str) -> UserState {
        let key = format!("{backend}:{uid}");
        let db = self.db.lock().unwrap();
        let _ = db.execute(
            "INSERT OR IGNORE INTO users (key, display_name, state, created_at) VALUES (?, ?, 'pending', ?)",
            rusqlite::params![key, name, Self::now()],
        );
        let _ = db.execute(
            "UPDATE users SET display_name = ? WHERE key = ? AND display_name = ''",
            rusqlite::params![name, key],
        );
        let state: String = db
            .query_row("SELECT state FROM users WHERE key = ?", [&key], |r| r.get(0))
            .unwrap_or_else(|_| "pending".into());
        match state.as_str() {
            "approved" => UserState::Approved,
            "banned" => UserState::Banned,
            _ => UserState::Pending,
        }
    }

    pub fn set_state(&self, backend: &str, uid: &str, state: UserState) {
        let key = format!("{backend}:{uid}");
        let _ = self.db.lock().unwrap().execute(
            "UPDATE users SET state = ?, approved_at = CASE WHEN ? = 'approved' THEN ? ELSE approved_at END WHERE key = ?",
            rusqlite::params![
                match state {
                    UserState::Pending => "pending",
                    UserState::Approved => "approved",
                    UserState::Banned => "banned",
                },
                if state == UserState::Approved { "approved" } else { "" },
                Self::now(),
                key
            ],
        );
    }

    /// Remove a user entirely (deny): pending requests and their challenges
    /// disappear; a returning user starts fresh as pending.
    pub fn remove_user(&self, backend: &str, uid: &str) {
        let key = format!("{backend}:{uid}");
        let _ = self.db.lock().unwrap().execute("DELETE FROM users WHERE key = ?", [&key]);
    }

    pub fn list_users(&self) -> Vec<(String, String, String, Option<String>)> {
        self.db
            .lock()
            .unwrap()
            .prepare("SELECT key, display_name, state, console_uid FROM users ORDER BY created_at")
            .and_then(|mut s| {
                let rows = s.query_map([], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default()
    }

    /// Start a verification challenge for a user being linked from the
    /// console. Does not change approval state except that banned users
    /// cannot relink. Returns Err(reason) on refusal.
    pub fn begin_link(
        &self,
        backend: &str,
        uid: &str,
        display_name: &str,
        console_uid: &str,
        code: &str,
    ) -> Result<(), String> {
        let key = format!("{backend}:{uid}");
        let db = self.db.lock().unwrap();
        let state: Option<String> = db
            .query_row("SELECT state FROM users WHERE key = ?", [&key], |r| r.get(0))
            .ok();
        if state.as_deref() == Some("banned") {
            return Err("user is banned".into());
        }
        let _ = db.execute(
            "INSERT INTO users (key, display_name, state, created_at, console_uid, challenge, challenged_at)
             VALUES (?, ?, COALESCE((SELECT state FROM users WHERE key = ?), 'pending'), ?, ?, ?, ?)
             ON CONFLICT(key) DO UPDATE SET
                display_name = excluded.display_name,
                console_uid = excluded.console_uid,
                challenge = excluded.challenge,
                challenged_at = excluded.challenged_at",
            rusqlite::params![key, display_name, key, Self::now(), console_uid, code, Self::now()],
        );
        Ok(())
    }

    /// Record which console user owns this identity.
    pub fn set_console_uid(&self, backend: &str, uid: &str, console_uid: &str) {
        let key = format!("{backend}:{uid}");
        let _ = self.db.lock().unwrap().execute(
            "UPDATE users SET console_uid = ? WHERE key = ?",
            rusqlite::params![console_uid, key],
        );
    }

    /// Check a challenge code; on success the user is approved (JID/nick
    /// ownership proven) and the challenge is cleared.
    pub fn verify_link(&self, backend: &str, uid: &str, code: &str) -> Result<(), String> {
        let key = format!("{backend}:{uid}");
        let db = self.db.lock().unwrap();
        let row: Option<(Option<String>, Option<i64>)> = db
            .query_row(
                "SELECT challenge, challenged_at FROM users WHERE key = ?",
                [&key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        let Some((challenge, challenged_at)) = row else {
            return Err("no link pending for this user".into());
        };
        let fresh = challenged_at
            .map(|t| Self::now() - t < 900)
            .unwrap_or(false);
        if !fresh {
            return Err("code expired, request a new one".into());
        }
        if challenge.as_deref() != Some(code) {
            return Err("incorrect code".into());
        }
        let _ = db.execute(
            "UPDATE users SET state = 'approved', approved_at = ?, challenge = NULL WHERE key = ?",
            rusqlite::params![Self::now(), key],
        );
        Ok(())
    }

    pub fn kill_switch(&self) -> bool {
        *self.kill_switch.lock().unwrap()
    }

    pub fn set_kill_switch(&self, on: bool) {
        *self.kill_switch.lock().unwrap() = on;
        self.log_event("kill_switch", "admin", if on { "engaged" } else { "released" });
    }

    // ---- runtime config (kv store; currently the tunnel settings) ----

    pub fn get_config(&self, key: &str) -> Option<String> {
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT value FROM config WHERE key = ?", [key], |r| r.get(0))
            .ok()
    }

    pub fn set_config(&self, key: &str, value: &str) {
        let _ = self.db.lock().unwrap().execute(
            "INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [key, value],
        );
    }

    pub fn delete_config(&self, key: &str) {
        let _ = self.db.lock().unwrap().execute("DELETE FROM config WHERE key = ?", [key]);
    }

    // ---- inter-bridge tunnel (store-and-forward bulletin relay) ----

    /// Queue a far-mesh message for the next RF digest flush.
    /// Queue an item for channel forwarding (oldest-first drain).
    pub fn forward_enqueue(&self, kind: &str, payload: &str) {
        let _ = self.db.lock().unwrap().execute(
            "INSERT INTO forward_queue (kind, payload, status, created_at) VALUES (?, ?, 'new', ?)",
            rusqlite::params![kind, payload, Self::now()],
        );
    }

    /// Pop the oldest new queue row (marks it 'sending').
    pub fn forward_pop(&self) -> Option<(i64, String, String)> {
        let db = self.db.lock().unwrap();
        let row = db
            .query_row(
                "SELECT id, kind, payload FROM forward_queue WHERE status = 'new' ORDER BY id LIMIT 1",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )
            .ok()?;
        let _ = db.execute(
            "UPDATE forward_queue SET status = 'sending' WHERE id = ?1",
            rusqlite::params![row.0],
        );
        Some(row)
    }

    pub fn forward_mark(&self, id: i64, status: &str) {
        let _ = self.db.lock().unwrap().execute(
            "UPDATE forward_queue SET status = ?2 WHERE id = ?1",
            rusqlite::params![id, status],
        );
    }

    pub fn forward_depth(&self) -> i64 {
        self.db
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM forward_queue WHERE status = 'new'", [], |r| r.get(0))
            .unwrap_or(0)
    }

    /// Dedup admission: true when the key has not been seen; prunes rows
    /// older than a day so the table stays small.
    pub fn admit_forward_dedup(&self, k: u64, ts: i64) -> bool {
        let db = self.db.lock().unwrap();
        let _ = db.execute("DELETE FROM forward_dedup WHERE ts < ?1", rusqlite::params![ts - 86400]);
        db.execute(
            "INSERT OR IGNORE INTO forward_dedup (k, ts) VALUES (?1, ?2)",
            rusqlite::params![k as i64, ts],
        )
        .map(|n| n == 1)
        .unwrap_or(false)
    }

    pub fn tunnel_enqueue(&self, origin: &str, sender: &str, text: &str) {
        let _ = self.db.lock().unwrap().execute(
            "INSERT INTO tunnel_queue (ts, origin, sender, text) VALUES (?, ?, ?, ?)",
            rusqlite::params![Self::now(), origin, sender, text],
        );
    }

    /// Up to `limit` pending tunnel messages, oldest first.
    pub fn tunnel_pending(&self, limit: usize) -> Vec<(i64, String, String, String)> {
        self.db
            .lock()
            .unwrap()
            .prepare(
                "SELECT id, origin, sender, text FROM tunnel_queue \
                 WHERE status = 'pending' ORDER BY id LIMIT ?",
            )
            .and_then(|mut s| {
                let rows = s.query_map([limit as i64], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default()
    }

    pub fn tunnel_mark(&self, ids: &[i64], status: &str) {
        let db = self.db.lock().unwrap();
        for id in ids {
            let _ = db.execute(
                "UPDATE tunnel_queue SET status = ? WHERE id = ?",
                rusqlite::params![status, id],
            );
        }
    }

    pub fn tunnel_queue_depth(&self) -> (i64, i64) {
        self.db
            .lock()
            .unwrap()
            .query_row(
                "SELECT \
                    (SELECT COUNT(*) FROM tunnel_queue WHERE status = 'pending'), \
                    (SELECT COUNT(*) FROM tunnel_queue WHERE status = 'sent')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap_or((0, 0))
    }

    /// Mesh messages after `since` (id, ts, key, text) for /tunnel/peek.
    pub fn tunnel_peek(&self, since: i64, limit: usize) -> Vec<(i64, i64, String, String)> {
        self.db
            .lock()
            .unwrap()
            .prepare(
                "SELECT id, ts, source, text FROM events \
                 WHERE kind = 'mesh_msg' AND id > ? ORDER BY id LIMIT ?",
            )
            .and_then(|mut s| {
                let rows = s.query_map(rusqlite::params![since, limit as i64], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                })?;
                rows.collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default()
    }

    fn fnv(data: &str) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in data.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    /// Remember an injected text so the mesh-side echo of it is ignored
    /// (loop guard). Called when we accept something for RF egress.
    pub fn remember_injected(&self, text: &str) {
        let mut hs = self.recent_hashes.lock().unwrap();
        if hs.len() > 256 {
            hs.clear(); // crude; fine at LoRa rates
        }
        hs.insert(Self::fnv(text));
    }

    /// True if this exact text was injected by us recently.
    pub fn was_injected(&self, text: &str) -> bool {
        self.recent_hashes.lock().unwrap().contains(&Self::fnv(text))
    }

    /// Full policy check for an outbound (internet -> mesh) message.
    /// Returns the text to transmit or a rejection reason.
    pub fn check_outbound(&self, backend: &str, uid: &str, name: &str, text: &str) -> Result<String, String> {
        if self.kill_switch() {
            return Err("kill switch engaged".into());
        }
        let text = text.trim();
        if text.is_empty() {
            return Err("empty message".into());
        }
        // Collapse whitespace/newlines: RF gets a single line.
        let text: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
        match self.upsert_user(backend, uid, name) {
            UserState::Banned => return Err("user banned".into()),
            UserState::Pending => return Err("user not approved (awaiting admin approval)".into()),
            UserState::Approved => {}
        }
        {
            let key = format!("{backend}:{uid}");
            let mut q = self.quota.lock().unwrap();
            let cutoff = Instant::now() - QUOTA_WINDOW;
            q.retain(|(_, t)| *t > cutoff);
            let mine = q.iter().filter(|(k, _)| k == &key).count();
            if mine >= QUOTA_MESSAGES {
                return Err(format!("quota exceeded ({QUOTA_MESSAGES} msgs/hour)"));
            }
            q.push((key, Instant::now()));
        }
        let clipped: String = text.chars().take(MAX_RF_CHARS).collect();
        let _ = name;
        Ok(format!("{name}: {clipped}"))
    }
}
