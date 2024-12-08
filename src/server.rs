use crate::{cache::MarketCache, types::MarketAccount};
use anyhow::{Context, Result};
use axum::{extract::State, routing::get, Json, Router};
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::net::TcpListener;
use tracing::info;

pub async fn serve(cache: Arc<MarketCache>) -> Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let app = Router::new()
        .route("/markets", get(get_markets))
        .route("/health", get(health_check))
        .with_state(cache);

    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;

    info!("Server listening on {}", addr);

    axum::serve(listener, app).await.context("Server error")?;

    Ok(())
}

async fn get_markets(
    State(cache): State<Arc<MarketCache>>,
) -> Json<HashMap<Pubkey, HashSet<MarketAccount>>> {
    let markets = cache.get_markets().await;
    Json(markets)
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }))
}
