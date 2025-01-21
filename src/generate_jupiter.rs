use anyhow::Result;
use axum::{
    extract::State,
    response::{IntoResponse, Response},
    Json,
};
use reqwest::StatusCode;
use serde_json::json;
use std::{process::Command, sync::Arc};
use tokio::time::Duration;
use tracing::{error, info};

use crate::{cache::MarketCache, error::CacheError};

pub async fn generate_jupiter_endpoint(State(cache): State<Arc<MarketCache>>) -> Response {
    info!("Starting Jupiter market data generation");
    match cache.export_to_jupiter_format().await {
        Ok(_) => {
            info!("Successfully exported to Jupiter format");
        }
        Err(e) => {
            error!("Failed to export to Jupiter format: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "message": format!("Failed to export to Jupiter format: {}", e)
                })),
            )
                .into_response();
        }
    }

    info!("Launching Jupiter application");
    let _ = Command::new("RUST_LOG=debug ./jupiter-swap-api --market-cache valid-markets.json --market-mode file --rpc-url \"http://84.32.186.51:8899\"").spawn();

    Json(json!({
        "status": "success",
        "message": "Successfully generated Jupiter markets"
    }))
    .into_response()
}
