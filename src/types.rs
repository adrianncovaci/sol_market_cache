use borsh::BorshDeserialize;
use serde::Serialize;
use solana_program::pubkey::Pubkey;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

use crate::error::CacheError;

#[derive(Debug, Serialize, PartialEq, Eq, Clone, Hash)]
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
    pub is_signer: Option<bool>,
    pub is_writable: Option<bool>,
    #[serde(skip)]
    pub last_updated: Option<Instant>,
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
            request_timeout: Duration::from_secs(60 * 60),
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

#[derive(Debug, Serialize, BorshDeserialize, PartialEq, Eq, Clone, Hash)]
pub struct AccountParams {
    pub routing_group: u8,
    pub address_lookup_table: Pubkey,
    pub serum_asks: Pubkey,
    pub serum_bids: Pubkey,
    pub serum_coin_vault: Pubkey,
    pub serum_event_queue: Pubkey,
    pub serum_pc_vault: Pubkey,
    pub serum_vault_signer: Pubkey,
}
