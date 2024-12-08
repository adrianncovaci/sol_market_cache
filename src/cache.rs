use metrics::{counter, gauge};
use redis::AsyncCommands;
use solana_account_decoder_client_types::{UiAccountEncoding, UiDataSliceConfig};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_program::pubkey::Pubkey;
use solana_sdk::{account::Account, commitment_config::CommitmentConfig};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, str::FromStr, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::error::CacheError;
use crate::types::{CacheConfig, MarketAccount, RedisConfig};

pub struct MarketCache {
    markets: Arc<RwLock<HashMap<Pubkey, HashSet<MarketAccount>>>>,
    rpc_client: Arc<RpcClient>,
    pubkeys_index: Arc<RwLock<HashSet<Pubkey>>>,
    cache_config: CacheConfig,
    redis_config: RedisConfig,
}

impl From<(Pubkey, Account)> for MarketAccount {
    fn from((pubkey, account): (Pubkey, Account)) -> Self {
        MarketAccount {
            pubkey: Some(pubkey),
            lamports: Some(account.lamports),
            space: Some(account.data.len() as u64),
            data: account.data,
            owner: Some(account.owner),
            executable: Some(account.executable),
            rent_epoch: Some(account.rent_epoch),
            params: None,
            last_updated: Some(Instant::now()),
        }
    }
}

impl MarketCache {
    pub async fn new(
        rpc_url: String,
        pubkeys: HashSet<String>,
        redis_url: String,
        config: Option<CacheConfig>,
    ) -> Result<Self, CacheError> {
        let cache_config = config.unwrap_or_default();

        let pubkeys_index = pubkeys
            .iter()
            .map(|x| Pubkey::from_str(x).map_err(|_| CacheError::PubkeyParseError(x.clone())))
            .collect::<Result<HashSet<_>, _>>()?;

        let redis_config = RedisConfig::new(
            redis_url,
            Duration::from_secs(60 * 60),
            Duration::from_secs(15 * 60),
        )
        .await?;

        Ok(Self {
            markets: Arc::new(RwLock::new(HashMap::new())),
            rpc_client: Arc::new(RpcClient::new_with_timeout(
                rpc_url,
                cache_config.request_timeout,
            )),
            pubkeys_index: Arc::new(RwLock::new(pubkeys_index)),
            cache_config,
            redis_config,
        })
    }

    async fn fetch_market(&self, program_id: Pubkey) -> Result<Vec<MarketAccount>, CacheError> {
        let config = RpcProgramAccountsConfig {
            filters: None,
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                data_slice: Some(UiDataSliceConfig {
                    offset: 0,
                    length: 100,
                }),
                commitment: Some(CommitmentConfig::confirmed()),
                min_context_slot: None,
            },
            with_context: Some(true),
            sort_results: Some(false),
        };

        let mut retries = 0;
        let timestamp = Instant::now();

        loop {
            match self
                .rpc_client
                .get_program_accounts_with_config(&program_id, config.clone())
            {
                Ok(accounts) => {
                    let market_accounts: Vec<MarketAccount> = accounts
                        .into_iter()
                        .map(|(pubkey, account)| MarketAccount::from((pubkey, account)))
                        .collect();

                    info!(
                        "Fetched {} accounts for program {}, took {} seconds",
                        market_accounts.len(),
                        program_id,
                        timestamp.elapsed().as_secs()
                    );

                    gauge!("market_accounts_count").set(market_accounts.len() as f64);
                    counter!("successful_fetches").increment(1);

                    return Ok(market_accounts);
                }
                Err(e) => {
                    counter!("failed_fetches").increment(1);

                    if retries >= self.cache_config.max_retries {
                        error!("Max retries reached for program {}: {:?}", program_id, e);
                        return Err(CacheError::RpcError(e));
                    }

                    warn!("Retry {} for program {}: {:?}", retries + 1, program_id, e);
                    retries += 1;
                    tokio::time::sleep(self.cache_config.retry_delay).await;
                }
            }
        }
    }

    pub async fn update_cache(self: Arc<Self>) -> Result<(), CacheError> {
        info!("Starting market cache update");

        let pubkeys = self.pubkeys_index.read().await.clone();
        for &program_id in pubkeys.iter() {
            let self_clone = self.clone();
            let mut interval = tokio::time::interval(self.cache_config.refresh_interval);
            tokio::spawn(async move {
                let timestamp = Instant::now();
                let accounts = self_clone.fetch_market(program_id).await;
                match accounts {
                    Ok(accounts) => {
                        {
                            let mut markets = self_clone.markets.write().await;
                            if let Some(markets) = markets.get_mut(&program_id) {
                                markets.extend(accounts.clone().into_iter());
                            }
                        }

                        gauge!("cache_update_time").set(timestamp.elapsed().as_secs_f64());
                        info!(
                            "Cache update completed in {} seconds for program {}",
                            timestamp.elapsed().as_secs(),
                            program_id
                        );

                        if let Some(last_updated) = &self_clone.redis_config.last_updated {
                            let last_updated_timestamp = last_updated.load(Ordering::Relaxed);
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs();

                            if now - last_updated_timestamp
                                >= self_clone.redis_config.update_duration.as_secs()
                            {
                                let _ = self_clone.store_in_redis(&program_id, &accounts).await;
                                last_updated.store(now, Ordering::Relaxed);
                            }
                        } else {
                            let _ = self_clone.store_in_redis(&program_id, &accounts).await;
                            if let Some(last_updated) = &self_clone.redis_config.last_updated {
                                let start = SystemTime::now();
                                let timestamp = start
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Time went backwards")
                                    .as_secs();
                                last_updated.store(timestamp, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to fetch market accounts: {:?}", e);
                    }
                }
                interval.tick().await;
            });
        }

        Ok(())
    }

    pub async fn start_background_refresh(self: Arc<Self>) {
        info!("Starting background refresh task");

        tokio::spawn(async move {
            let _ = self.clone().update_cache().await;
        });
    }

    pub async fn get_markets(&self) -> HashMap<Pubkey, HashSet<MarketAccount>> {
        self.markets.read().await.clone()
    }

    async fn store_in_redis(
        &self,
        program_id: &Pubkey,
        accounts: &[MarketAccount],
    ) -> Result<(), CacheError> {
        let accounts_json =
            serde_json::to_string(&accounts).map_err(CacheError::SerializationError)?;

        self.redis_config
            .redis_connection
            .write()
            .await
            .set(program_id.to_string(), accounts_json)
            .await
            .map_err(CacheError::RedisError)?;

        self.redis_config
            .redis_connection
            .write()
            .await
            .expire(
                program_id.to_string(),
                self.redis_config.cache_ttl.as_secs() as i64,
            )
            .await
            .map_err(CacheError::RedisError)?;

        Ok(())
    }
}
