//! Entrypoint for the GOG storefront plugin process.
//!
//! Binds an ephemeral port, registers `Gog` with core's Registry
//! service, keeps that registration alive with a heartbeat loop, then
//! serves `Store` for the lifetime of the process. Exits immediately if
//! another process already owns the `Gog` storefront.

use std::{sync::Arc, time::Duration};

use bottles_core::credentials::os::OsCredentialStore;
use next_proto::bottles::{
    common::v1::Storefront,
    registry::v1::{
        HeartbeatRequest, RegisterOutcome, RegisterPluginRequest, registry_client::RegistryClient,
    },
    store::v1::store_server::StoreServer,
};
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use crate::service::GogStoreService;

const REGISTRY_ENDPOINT: &str = "http://127.0.0.1:50250"; // core's Registry service
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1); // < HEARTBEAT_TIMEOUT/2

mod depot;
mod error;
mod gamesdb;
mod service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("next_plugin_gog=trace")),
        )
        .init();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;
    let endpoint = format!("http://{local_addr}");

    let credentials = Arc::new(OsCredentialStore::new());
    let store_service = GogStoreService::new(credentials);

    // --- Register with core before accepting any Store traffic ---
    let mut registry = RegistryClient::connect(REGISTRY_ENDPOINT).await?;

    let register_response = registry
        .register_plugin(RegisterPluginRequest {
            storefront: Storefront::Gog as i32,
            endpoint: endpoint.clone(),
            plugin_version: env!("CARGO_PKG_VERSION").to_string(),
        })
        .await?
        .into_inner();

    if register_response.outcome() != RegisterOutcome::Accepted {
        // Someone else already owns Gog. Log who, and exit rather than
        // silently serving traffic nobody will ever route to us.
        if let Some(owner) = register_response.current_owner {
            tracing::error!(
                "Gog already owned by {} (v{}); refusing to start",
                owner.endpoint,
                owner.plugin_version
            );
        }
        return Err("storefront already owned by another plugin".into());
    }

    let registration_token = register_response
        .registration_token
        .expect("registration_token must be set on Accepted");

    // --- Heartbeat loop, runs for the lifetime of the process ---
    let heartbeat_registry = registry.clone();
    let heartbeat_token = registration_token.clone();
    tokio::spawn(async move {
        let mut registry = heartbeat_registry;
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            interval.tick().await;
            let result = registry
                .heartbeat(HeartbeatRequest {
                    registration_token: heartbeat_token.clone(),
                })
                .await;
            if let Err(err) = result {
                // Registry may have restarted and lost state, or our
                // registration expired despite our best effort (e.g.
                // this process was suspended past the timeout). Either
                // way there's nothing productive to do but log it —
                // core will simply stop routing to us until we're
                // reaped and something re-registers this storefront.
                tracing::warn!("heartbeat failed: {err}");
            }
        }
    });

    tracing::info!("GOG plugin listening on {endpoint}");

    Server::builder()
        .add_service(StoreServer::new(store_service))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;

    Ok(())
}
