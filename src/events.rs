//! Shared types: room events from the Bear Den firmware serial link and
//! the backend-facing message model.

use serde::{Deserialize, Serialize};

/// One `BRIDGE {...}` line emitted by the patched room server firmware.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeLine {
    pub evt: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub ts: u64,
    #[serde(default)]
    pub sys: u8,
    #[serde(default)]
    pub perm: u8,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub b64: String,
    #[serde(default)]
    pub snr: i32,
    /// Bridge channel index: 0=bears, 1=public, 2=bears2 (-1 unknown).
    #[serde(default = "default_ch")]
    pub ch: i64,
}

fn default_ch() -> i64 {
    -1
}

/// Mesh-side room event, after filtering (system echoes dropped).
#[derive(Debug, Clone, Serialize)]
pub struct RoomEvent {
    pub kind: RoomEventKind,
    /// 8-hex public key prefix of the mesh sender.
    pub key: String,
    pub ts: u64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RoomEventKind {
    Message,
    Join,
}

/// A message accepted from a backend, awaiting policy + RF egress.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub backend: String,
    pub backend_uid: String,
    pub display_name: String,
    pub text: String,
}

/// Core -> backend command (e.g. deliver a verification code DM).
#[derive(Debug, Clone)]
pub struct BridgeCmd {
    pub backend: String,
    pub backend_uid: String,
    pub text: String,
}
