use anyhow::Result;
use market_cache::{CacheConfig, MarketCache};
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

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

    let config = CacheConfig {
        refresh_interval: Duration::from_secs(10),
        request_timeout: Duration::from_secs(60),
        max_retries: 3,
        retry_delay: Duration::from_secs(1),
    };

    let cache = Arc::new(MarketCache::new(
        "rpc".to_string(),
        indexed_pubkeys,
        Some(config),
    )?);

    cache.clone().start_background_refresh().await;
    market_cache::serve(cache).await?;

    Ok(())
}
