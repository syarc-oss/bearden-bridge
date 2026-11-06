//! bearden-bridge: bridges the Bear Den MeshCore room server to the
//! internet. Phase 1: serial observe/inject + policy + admin API + a
//! logging backend; real backends (XMPP/Matrix/Nostr) come next.

mod api;
mod backends;
mod events;
mod forward;
mod matrix_backend;
mod xmpp_backend;
mod media;
mod serial;
mod store;
mod tunnel_backend;

use anyhow::Result;
use backends::Backend;
use std::sync::Arc;
use tokio::sync::broadcast;

const DEFAULT_PORT: &str = "/dev/bearden";
const DEFAULT_DB: &str = "/usr/local/dev_apps/bearden-bridge/bridge.db";
const DEFAULT_API: &str = "127.0.0.1:8095";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let port_path = env_or("BEARDEN_PORT", DEFAULT_PORT);
    // One TLS provider (ring) for the whole process: reqwest and tokio-xmpp
    // otherwise fight over rustls' default CryptoProvider.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let db_path = env_or("BEARDEN_DB", DEFAULT_DB);
    let api_addr = env_or("BEARDEN_API", DEFAULT_API);

    let store = Arc::new(store::Store::open(std::path::Path::new(&db_path))?);
    let link = Arc::new(serial::SerialLink::open(&port_path)?);
    let mut media_dir = env_or("BEARDEN_MEDIA_DIR", "/usr/local/dev_apps/bearden-bridge/media");
    // The default dir may be owned by another user when the bridge runs as
    // a service account; fall back to a writable location so completed
    // clips are never silently lost.
    let probe = std::path::Path::new(&media_dir);
    let writable = std::fs::create_dir_all(probe)
        .ok()
        .and_then(|_| {
            let p = probe.join(".write_test");
            std::fs::write(&p, b"x").ok()?;
            std::fs::remove_file(&p).ok()
        })
        .is_some();
    if !writable {
        let fallback = "/usr/local/syarc/bearden-bridge/media";
        eprintln!(
            "bearden: media dir {media_dir} not writable, falling back to {fallback}"
        );
        std::fs::create_dir_all(fallback).ok();
        media_dir = fallback.to_string();
    }
    // Completed clips are pushed to the local console so its Media Clips UI
    // shows full clips (the console cannot 0x72-fetch missing parts itself).
    let console_push = std::env::var("BEARDEN_ADMIN_TOKEN").ok().map(|tok| {
        (env_or("BEARDEN_CONSOLE_URL", "http://127.0.0.1:8090"), tok)
    });
    let media_mgr =
        Arc::new(media::MediaManager::new(store.clone(), &media_dir, console_push));
    {
        // 0x72 fetch requests for pull-based Open TAK senders go out the
        // serial link on the session's channel.
        let link = link.clone();
        media_mgr.set_fetch_sink(Box::new(move |wire, chan| {
            let _ = link.send_media(wire, chan);
        }));
    }
    media_mgr.start_sweeper();
    let egress = backends::Egress {
        store: store.clone(),
        serial: link.clone(),
    };

    // mesh -> backends fan-out: every backend subscribes its own receiver.
    let (ev_tx, _ev_rx) = broadcast::channel::<crate::events::RoomEvent>(256);
    // core -> backend commands (verification DMs). broadcast so each
    // backend gets its own receiver; senders ignore no-listener errors.
    let (cmd_tx, cmd_rx) = broadcast::channel::<crate::events::BridgeCmd>(64);
    let cmd_any = cmd_rx.resubscribe();

    // Channel forwarder (Bears -> Bears2 den routing): optional; the
    // paced drain thread + runtime config endpoints come with it.
    let forwarder = forward::Forwarder::from_config(store.clone(), link.clone(), media_mgr.clone());
    if let Some(fwd) = &forwarder {
        let cfg = fwd.config();
        eprintln!(
            "bearden: forwarding {} ({} -> {}, pace {}ms)",
            if cfg.enabled { "ENABLED" } else { "configured but disabled" },
            forward::CH_WORDS[cfg.in_idx as usize],
            forward::CH_WORDS[cfg.out_idx as usize],
            cfg.pace_ms
        );
        let f = fwd.clone();
        std::thread::spawn(move || f.run());
    }

    // Serial reader is blocking; run it on a dedicated thread.
    {
        let link = link.clone();
        let store = store.clone();
        let media = media_mgr.clone();
        let forward = forwarder.clone();
        let tx = ev_tx.clone();
        std::thread::spawn(move || {
            if let Err(e) = link.read_loop(store, media, forward, tx) {
                eprintln!("bearden: serial reader died: {e}");
                std::process::exit(2); // systemd restarts us
            }
        });
    }

    // Backends. The log backend always runs; XMPP runs when configured.
    let log_backend: Box<dyn Backend> = Box::new(backends::LogBackend);
    tokio::spawn(log_backend.run(backends::BackendCtx {
        egress: egress.clone(),
        inbound: ev_tx.subscribe(),
        cmd_rx: cmd_rx.resubscribe(),
    }));
    if let Some(cfg) = xmpp_backend::XmppBackend::from_env() {
        println!("bearden-bridge: xmpp backend enabled as {}", cfg.jid);
        let egress2 = egress.clone();
        let ev2 = ev_tx.subscribe();
        tokio::spawn(async move {
            loop {
                // The XMPP Agent can't be reused after disconnect; rebuild.
                let cfg = xmpp_backend::XmppBackend::from_env().unwrap();
                let xb: Box<dyn Backend> = Box::new(cfg);
                let _ = xb.run(backends::BackendCtx {
                    egress: egress2.clone(),
                    inbound: ev2.resubscribe(),
                    cmd_rx: cmd_rx.resubscribe(),
                }).await;
                println!("[xmpp backend] restarting in 10s");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    if let Some(cfg) = matrix_backend::MatrixBackend::from_env() {
        println!(
            "bearden-bridge: matrix backend enabled as {} on {}",
            cfg.user, cfg.homeserver
        );
        let egress3 = egress.clone();
        let ev3 = ev_tx.subscribe();
        let cmd3 = cmd_any.resubscribe();
        tokio::spawn(async move {
            loop {
                let cfg = matrix_backend::MatrixBackend::from_env().unwrap();
                let mb: Box<dyn Backend> = Box::new(cfg);
                if let Err(e) = mb.run(backends::BackendCtx {
                    egress: egress3.clone(),
                    inbound: ev3.resubscribe(),
                    cmd_rx: cmd3.resubscribe(),
                }).await {
                    eprintln!("[matrix backend] died: {e}");
                }
                println!("[matrix backend] restarting in 10s");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    // Tunnel: runtime-configurable (admin API / Chat Link UI). Precedence:
    // persisted DB config > env vars > disabled. Disabling persists a
    // sentinel row ({"disabled":true}) so a restart must NOT fall back to
    // BEARDEN_TUNNEL_* env and silently re-enable a tunnel the admin
    // turned off. The backend task always runs and idles while cfg is None.
    let initial_cfg = match store.get_config("tunnel") {
        Some(s) if s.contains("\"disabled\":true") => None,
        Some(s) => serde_json::from_str::<serde_json::Value>(&s)
            .ok()
            .and_then(|v| tunnel_backend::TunnelConfig::from_json(&v).ok()),
        None => tunnel_backend::TunnelConfig::from_env(),
    };
    if let Some(cfg) = &initial_cfg {
        println!(
            "bearden-bridge: tunnel backend enabled toward {} (tag {}, flush {}s)",
            cfg.peer,
            cfg.tag,
            cfg.flush_interval.as_secs()
        );
    }
    let tunnel_cfg = tunnel_backend::shared_cfg(initial_cfg);
    {
        let shared = tunnel_cfg.clone();
        let egress4 = egress.clone();
        let ev4 = ev_tx.subscribe();
        let cmd4 = cmd_any.resubscribe();
        tokio::spawn(async move {
            loop {
                let tb: Box<dyn Backend> = Box::new(tunnel_backend::TunnelBackend { cfg: shared.clone() });
                if let Err(e) = tb.run(backends::BackendCtx {
                    egress: egress4.clone(),
                    inbound: ev4.resubscribe(),
                    cmd_rx: cmd4.resubscribe(),
                }).await {
                    eprintln!("[tunnel backend] died: {e}");
                }
                println!("[tunnel backend] restarting in 10s");
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    let app = api::router(
        store.clone(),
        egress.clone(),
        std::env::var("BEARDEN_ADMIN_TOKEN").ok(),
        cmd_tx,
        tunnel_cfg,
        media_mgr,
        forwarder.clone(),
    );
    let listener = tokio::net::TcpListener::bind(&api_addr).await?;
    println!("bearden-bridge: serial={port_path} db={db_path} api=http://{api_addr}");
    axum::serve(listener, app).await?;
    Ok(())
}
