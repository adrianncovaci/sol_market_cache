use anyhow::Result;
use jup_cache_tracker::{cache::MarketCache, server, types::CacheConfig};
use std::{collections::HashSet, net::SocketAddr, sync::Arc};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(SocketAddr::from(([127, 0, 0, 1], 9000)))
        .install()?;

    let markets_txt = include_str!("cache_markets.json");
    let indexed_pubkeys: HashSet<String> = serde_json::from_str(markets_txt)?;

    let config = CacheConfig::default();
    let cache = MarketCache::new(
        "http://84.32.186.51:8899".to_string(),
        indexed_pubkeys,
        "redis://localhost".to_string(),
        Some(config),
    )
    .await?;

    let cache = Arc::new(cache);

    cache.clone().start_background_refresh().await;
    server::serve(cache).await?;

    Ok(())
}
