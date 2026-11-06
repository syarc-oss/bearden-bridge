//! Internet-side backends. Each backend runs as an independent task:
//! it receives inbound (mesh->internet) events through `Fanout` and
//! submits outbound messages through `Egress`, which applies policy.

use crate::events::{BridgeCmd, OutboundMessage, RoomEvent};
use crate::serial::SerialLink;
use crate::store::Store;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

/// What a backend gets from the core.
pub struct BackendCtx {
    pub egress: Egress,
    /// New mesh events (already filtered for loops/system echoes).
    pub inbound: broadcast::Receiver<RoomEvent>,
    /// Core -> backend commands (verification code DMs etc.). Backends that
    /// cannot deliver DMs should drain and ignore this channel.
    pub cmd_rx: broadcast::Receiver<BridgeCmd>,
}

/// Policy-gated path to the radio.
#[derive(Clone)]
pub struct Egress {
    pub store: Arc<Store>,
    pub serial: Arc<SerialLink>,
}

impl Egress {
    /// Backend calls this when its user says something. Returns Ok(())
    /// once the message is on the wire (or queued), Err(reason) if the
    /// policy layer rejected it (backend should tell the user why).
    pub async fn submit(&self, msg: OutboundMessage) -> Result<(), String> {
        let staged = self
            .store
            .check_outbound(&msg.backend, &msg.backend_uid, &msg.display_name, &msg.text)?;
        self.store.log_event("rf_out", &msg.display_name, &staged);
        self.store.remember_injected(&staged);
        self.serial
            .post(&staged)
            .map_err(|e| format!("radio error: {e}"))
    }
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn run(self: Box<Self>, ctx: BackendCtx) -> anyhow::Result<()>;
}

/// Phase-1 backend: logs mesh events to stdout and exposes no users.
/// Proves the fan-out wiring; replaced by XMPP/Matrix/Nostr next.
pub struct LogBackend;

#[async_trait]
impl Backend for LogBackend {
    async fn run(self: Box<Self>, mut ctx: BackendCtx) -> anyhow::Result<()> {
        while let Ok(ev) = ctx.inbound.recv().await {
            println!(
                "[log backend] {} {}: {}",
                match ev.kind {
                    crate::events::RoomEventKind::Message => "msg",
                    crate::events::RoomEventKind::Join => "join",
                },
                ev.key,
                ev.text
            );
            while ctx.cmd_rx.try_recv().is_ok() {} // drain: no DM support
        }
        Ok(())
    }
}
