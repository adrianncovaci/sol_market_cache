use serde::{Deserialize, Serialize};
use solana_program::pubkey::Pubkey;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

use crate::error::CacheError;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub struct MarketAccount {
    pub pubkey: Option<Pubkey>,
    pub lamports: Option<u64>,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub owner: Option<Pubkey>,
    pub executable: Option<bool>,
    pub rent_epoch: Option<u64>,
    pub space: Option<u64>,
    pub params: Option<AccountParams>,
    #[serde(skip)]
    pub last_updated: Option<Instant>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub struct AccountParams {
    #[serde(rename = "addressLookupTableAddress")]
    pub address_lookup_table_address: Option<String>,
    #[serde(rename = "routingGroup")]
    pub routing_group: Option<u8>,
    #[serde(rename = "serumAsks")]
    pub serum_asks: Option<String>,
    #[serde(rename = "serumBids")]
    pub serum_bids: Option<String>,
    #[serde(rename = "serumCoinVaultAccount")]
    pub serum_coin_vault_account: Option<String>,
    #[serde(rename = "serumEventQueue")]
    pub serum_event_queue: Option<String>,
    #[serde(rename = "serumPcVaultAccount")]
    pub serum_pc_vault_account: Option<String>,
    #[serde(rename = "serumVaultSigner")]
    pub serum_vault_signer: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub request_timeout: Duration,
    pub refresh_interval: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10 * 60),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
        }
    }
}

#[derive(Debug)]
pub struct RedisConfig {
    pub url: String,
    pub cache_ttl: Duration,
    pub redis_client: Arc<redis::Client>,
    pub redis_connection: Arc<RwLock<redis::aio::MultiplexedConnection>>,
    pub update_duration: Duration,
    pub last_updated: Option<AtomicU64>,
}

impl RedisConfig {
    pub async fn new(
        url: String,
        cache_ttl: Duration,
        update_duration: Duration,
    ) -> Result<Self, CacheError> {
        let redis_client = redis::Client::open(url.clone())?;
        let redis_connection = redis_client.get_multiplexed_async_connection().await?;
        Ok(Self {
            url,
            cache_ttl,
            redis_client: Arc::new(redis_client),
            redis_connection: Arc::new(RwLock::new(redis_connection)),
            update_duration,
            last_updated: None,
        })
    }
}
