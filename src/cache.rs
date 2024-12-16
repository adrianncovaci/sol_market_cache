use futures::future::join_all;
use futures::StreamExt;
use metrics::{counter, gauge};
use redis::AsyncCommands;
use solana_account_decoder_client_types::{UiAccountData, UiAccountEncoding, UiDataSliceConfig};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_program::pubkey::Pubkey;
use solana_sdk::{account::Account, commitment_config::CommitmentConfig};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashSet, str::FromStr, sync::Arc, time::Instant};
use tokio::sync::mpsc;
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
        let mut tasks = Vec::new();
        for &program_id in pubkeys.iter() {
            let self_clone = self.clone();
            let mut interval = tokio::time::interval(self.cache_config.refresh_interval);
            let task = tokio::spawn(async move {
                let timestamp = Instant::now();
                let accounts = self_clone.fetch_market(program_id).await;
                match accounts {
                    Ok(accounts) => {
                        {
                            let mut markets = self_clone.markets.write().await;
                            markets.insert(program_id, accounts.clone().into_iter().collect());
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
            tasks.push(task);
        }

        join_all(tasks).await;
        Ok(())
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

    //async fn start_subscriptions(self: Arc<Self>) -> Result<(), CacheError> {
    //    info!("Starting program subscriptions");

    //    let pubkeys = self.pubkeys_index.read().await.clone();
    //    let ws_url = self
    //        .rpc_client
    //        .url()
    //        .replace("http://", "ws://")
    //        .replace("https://", "wss://");

    //    let pubsub_client = Arc::new(
    //        PubsubClient::new(&ws_url)
    //            .await
    //            .map_err(|e| CacheError::WSError(e))?,
    //    );
    //
    //    let mut subscriptions: HashMap<Pubkey, (Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>, UnboundedReceiver<Response<RpcKeyedAccount>>)> = HashMap::new();

    //    for program_id in pubkeys {
    //        let self_clone = self.clone();
    //        let pubsub_client = Arc::clone(&pubsub_client);

    //        let config = RpcAccountInfoConfig {
    //            encoding: Some(UiAccountEncoding::Base64),
    //            data_slice: Some(UiDataSliceConfig {
    //                offset: 0,
    //                length: 100,
    //            }),
    //            commitment: Some(CommitmentConfig::confirmed()),
    //            min_context_slot: None,
    //        };

    //        let program_config = RpcProgramAccountsConfig {
    //            filters: None,
    //            account_config: config,
    //            with_context: Some(true),
    //            sort_results: Some(false),
    //        };

    //        match pubsub_client.program_subscribe(&program_id, Some(program_config)).await {
    //            Ok((mut notifications, subscription)) => {
    //                info!("Successfully subscribed to program {}", program_id);
    //
    //                let (sender, receiver) = mpsc::unbounded_channel();
    //
    //                subscriptions.insert(program_id, (subscription, receiver));

    //                // Process notifications
    //                while let Some(response) = notifications.next().await {
    //                    let keyed_account = response.value;
    //
    //                    // Convert string pubkey to Pubkey
    //                    if let Ok(pubkey) = Pubkey::from_str(&keyed_account.pubkey) {
    //                        // Convert UiAccount data to Vec<u8>
    //                        if let UiAccountData::Binary(data_str, encoding) = keyed_account.account.data {
    //                            if let Some(data) = Self::decode_account_data(&data_str, encoding) {
    //                                // Convert string owner to Pubkey
    //                                if let Ok(owner) = Pubkey::from_str(&keyed_account.account.owner) {
    //                                    let account = MarketAccount::from((
    //                                        pubkey,
    //                                        Account {
    //                                            lamports: keyed_account.account.lamports,
    //                                            data,
    //                                            owner,
    //                                            executable: keyed_account.account.executable,
    //                                            rent_epoch: keyed_account.account.rent_epoch,
    //                                        }
    //                                    ));

    //                                    let mut markets = self_clone.markets.write().await;
    //                                    if let Some(accounts) = markets.get_mut(&program_id) {
    //                                        accounts.retain(|a| a.pubkey != Some(pubkey));
    //                                        accounts.insert(account);
    //                                    }
    //                                }
    //                            }
    //                        }
    //                    }
    //                }
    //            }
    //            Err(e) => {
    //                error!("Failed to subscribe to program {}: {:?}", program_id, e);
    //            }
    //        };
    //    }

    //    Ok(())
    //}

    pub async fn start_background_refresh(self: Arc<Self>) {
        info!("Starting background refresh and subscriptions");

        // First, populate the initial cache
        if let Err(e) = self.clone().update_cache().await {
            error!("Failed to populate initial cache: {:?}", e);
            return;
        }

        info!("Initial cache population complete, starting subscriptions");

        // Then start the subscriptions
        if let Err(e) = self.clone().start_subscriptions().await {
            error!("Failed to start subscriptions: {:?}", e);
        }
    }

    async fn start_subscriptions(self: Arc<Self>) -> Result<(), CacheError> {
        info!("Starting program subscriptions");

        let pubkeys = self.pubkeys_index.read().await.clone();
        let ws_url = self
            .rpc_client
            .url()
            .replace("https://", "ws://")
            .replace("http://", "ws://")
            .replace(":8899", ":8900");

        info!("WebSocket URL: {}", ws_url);

        // Create a channel for handling subscription updates
        let (update_tx, mut update_rx) = mpsc::unbounded_channel();

        // Spawn a separate task for WebSocket connection and subscription handling
        for program_id in pubkeys {
            let ws_url = ws_url.clone();
            let update_tx = update_tx.clone();
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

            tokio::spawn(async move {
                loop {
                    match PubsubClient::new(&ws_url).await {
                        Ok(pubsub_client) => {
                            match pubsub_client
                                .program_subscribe(&program_id, Some(config.clone()))
                                .await
                            {
                                Ok((mut notifications, _unsubscribe)) => {
                                    info!("Successfully subscribed to program {}", program_id);

                                    while let Some(notification) = notifications.next().await {
                                        if update_tx.send((program_id, notification)).is_err() {
                                            error!("Failed to send update to channel");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to subscribe to program {}: {:?}",
                                        program_id, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to create WebSocket connection: {:?}", e);
                        }
                    }

                    // Wait before attempting to reconnect
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }

        // Process updates in the main subscription handler
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut counter = 0;
            while let Some((program_id, response)) = update_rx.recv().await {
                let keyed_account = response.value;

                if let Ok(pubkey) = Pubkey::from_str(&keyed_account.pubkey) {
                    if let Some(data) = keyed_account.account.data.decode() {
                        if let Ok(owner) = Pubkey::from_str(&keyed_account.account.owner) {
                            let account = MarketAccount::from((
                                pubkey,
                                Account {
                                    lamports: keyed_account.account.lamports,
                                    data,
                                    owner,
                                    executable: keyed_account.account.executable,
                                    rent_epoch: keyed_account.account.rent_epoch,
                                },
                            ));

                            let market = self_clone.markets.read().await.get(&program_id).cloned();
                            if let Some(mut accounts) = market {
                                counter += 1;
                                //if counter % 100 == 0 {
                                //    info!("Processed {:?}", account);
                                //}

                                accounts.retain(|a| a.pubkey != Some(pubkey));
                                accounts.insert(account);
                                self_clone
                                    .markets
                                    .write()
                                    .await
                                    .insert(program_id, accounts);
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}
