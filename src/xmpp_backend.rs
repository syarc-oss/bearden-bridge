//! XMPP MUC backend: bridges the mesh room to a Prosody chat room.
//!
//! Mesh events are sent to the room as the bridge nick; messages typed
//! in the room by other participants go through the policy engine and
//! out over RF. Config (all required to enable this backend):
//!   BEARDEN_XMPP_JID, BEARDEN_XMPP_PASSWORD, BEARDEN_XMPP_ROOM (bare JID,
//!   e.g. mesh@conference.localhost), BEARDEN_XMPP_NICK (default "bridge").

use std::str::FromStr;

use crate::backends::{Backend, BackendCtx};
use async_trait::async_trait;
use xmpp::RoomNick;
use xmpp::jid::{BareJid, ResourcePart};
use xmpp::muc::private_message::RoomPrivateMessageSettings;
use xmpp::muc::room::{JoinRoomSettings, RoomMessageSettings};
use xmpp::{Agent, ClientBuilder, Event};

pub struct XmppBackend {
    pub jid: String,
    pub password: String,
    pub room: String,
    pub nick: String,
}

impl XmppBackend {
    pub fn from_env() -> Option<Self> {
        let jid = std::env::var("BEARDEN_XMPP_JID").ok()?;
        let password = std::env::var("BEARDEN_XMPP_PASSWORD").ok()?;
        let room = std::env::var("BEARDEN_XMPP_ROOM").ok()?;
        let nick = std::env::var("BEARDEN_XMPP_NICK").unwrap_or_else(|_| "bridge".into());
        Some(XmppBackend {
            jid,
            password,
            room,
            nick,
        })
    }
}

#[async_trait]
impl Backend for XmppBackend {

    async fn run(self: Box<Self>, mut ctx: BackendCtx) -> anyhow::Result<()> {
        let jid: BareJid = self.jid.parse()?;
        let room: BareJid = self.room.parse()?;
        let resource = ResourcePart::try_from(self.nick.as_str())?;
        let my_nick = self.nick.clone();

        let mut agent: Agent = ClientBuilder::new(jid, &self.password)
            .set_resource(&self.nick)
            .build();

        // The agent auto-reconnects on connection loss but does NOT rejoin
        // rooms, so re-issue the join on every Online event.
        agent
            .join_room(JoinRoomSettings::new(room.clone()).with_nick(&resource))
            .await;
        println!("[xmpp backend] joining {room} as {my_nick}");

        loop {
            // Set on (re)join: the MUC server replays recent history right
            // after we join, which would otherwise be re-transmitted to RF as
            // fresh messages (the user1 30-minute repeat loop). Suppress
            // room messages during that burst window.
            let mut suppress_room_msgs_until: Option<std::time::Instant> = Some(
                std::time::Instant::now() + std::time::Duration::from_secs(8),
            );
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
                    agent.send_room_message(RoomMessageSettings::new(room.clone(), &line)).await;
                }
                cmd = ctx.cmd_rx.recv() => {
                    let Ok(cmd) = cmd else { continue };
                    if cmd.backend != "xmpp" { continue; }
                    // Verification DM: private message to the occupant nick.
                    match RoomNick::from_str(&cmd.backend_uid) {
                        Ok(nick) => {
                            agent.send_room_private_message(
                                RoomPrivateMessageSettings::new(room.clone(), nick, &cmd.text),
                            ).await;
                            println!("[xmpp backend] verification DM sent to {}", cmd.backend_uid);
                        }
                        Err(e) => eprintln!("[xmpp backend] bad nick {}: {e}", cmd.backend_uid),
                    }
                }
                batch = tokio::time::timeout(
                    std::time::Duration::from_secs(1800),
                    agent.wait_for_events(),
                ) => {
                    // Watchdog: a wedged event stream (stalled reconnect) must
                    // not hang the backend forever — bail so main rebuilds us.
                    // No client-side keepalive exists, so this also fires on
                    // legitimately idle-but-quiet connections; reconnect +
                    // rejoin is cheap and correct either way.
                    let batch = match batch {
                        Ok(b) => b,
                        Err(_) => anyhow::bail!("xmpp event stream idle >10min (watchdog)"),
                    };
                    for event in batch {
                        match event {
                            Event::Online => {
                                println!("[xmpp backend] online");
                                agent
                                    .join_room(JoinRoomSettings::new(room.clone()).with_nick(&resource))
                                    .await;
                                suppress_room_msgs_until =
                                    Some(std::time::Instant::now() + std::time::Duration::from_secs(8));
                                println!("[xmpp backend] rejoined {room} after reconnect");
                            }
                            Event::RoomJoined(joined) => {
                                if suppress_room_msgs_until.is_none() {
                                    suppress_room_msgs_until =
                                        Some(std::time::Instant::now() + std::time::Duration::from_secs(8));
                                }
                                println!("[xmpp backend] joined {joined}");
                            }
                            Event::RoomMessage(_, from, nick, body, _) => {
                                if from == room && nick.as_ref().as_ref() != my_nick {
                                    if let Some(t) = suppress_room_msgs_until {
                                        if std::time::Instant::now() < t {
                                            let head: String = body.chars().take(60).collect();
                                            println!("[xmpp backend] suppressing history replay from {nick}: {head}");
                                            continue;
                                        }
                                    }
                                    let name = nick.to_string();
                                    if let Err(reason) = ctx.egress.submit(crate::events::OutboundMessage {
                                        backend: "xmpp".into(),
                                        backend_uid: name.clone(),
                                        display_name: name.clone(),
                                        text: body.clone(),
                                    }).await {
                                        // policy rejection goes back to the room
                                        agent.send_room_message(RoomMessageSettings::new(
                                            room.clone(),
                                            &format!("{name}: not sent ({reason})"),
                                        )).await;
                                    }
                                }
                            }
                            Event::Disconnected(e) => anyhow::bail!("xmpp disconnected: {e}"),
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
