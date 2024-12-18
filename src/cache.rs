use futures::future::join_all;
use futures::StreamExt;
use metrics::{counter, gauge};
use redis::AsyncCommands;
use solana_account_decoder_client_types::{UiAccountEncoding, UiDataSliceConfig};
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
use crate::types::{AccountParams, CacheConfig, MarketAccount, RedisConfig};

pub struct MarketCache {
    markets: Arc<RwLock<HashMap<Pubkey, HashSet<MarketAccount>>>>,
    rpc_client: Arc<RpcClient>,
    pubkeys_index: Arc<RwLock<HashSet<Pubkey>>>,
    cache_config: CacheConfig,
    redis_config: RedisConfig,
}

impl From<(Pubkey, Account, Option<AccountParams>)> for MarketAccount {
    fn from((pubkey, account, params): (Pubkey, Account, Option<AccountParams>)) -> Self {
        MarketAccount {
            pubkey: Some(pubkey),
            lamports: Some(account.lamports),
            space: Some(account.data.len() as u64),
            data: account.data,
            owner: Some(account.owner),
            executable: Some(account.executable),
            rent_epoch: Some(account.rent_epoch),
            last_updated: Some(Instant::now()),
            params,
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
                        .map(|(pubkey, account)| MarketAccount::from((pubkey, account, None)))
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

        // Spawn a separate task for each WebSocket connection and subscription
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
                        Ok(client) => {
                            match client
                                .program_subscribe(&program_id, Some(config.clone()))
                                .await
                            {
                                Ok((mut notifications, _unsubscribe)) => {
                                    info!("Successfully subscribed to program {}", program_id);

                                    // Process notifications until connection drops
                                    while let Some(notification) = notifications.next().await {
                                        if update_tx.send((program_id, notification)).is_err() {
                                            error!("Failed to send update to channel");
                                            break;
                                        }
                                    }

                                    // If we get here, the connection was lost
                                    error!(
                                        "Lost connection to program {}, attempting to reconnect",
                                        program_id
                                    );
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
                            error!(
                                "Failed to create client for program {}: {:?}",
                                program_id, e
                            );
                        }
                    }
                    let jitter = Duration::from_millis(rand::random::<u64>() % 500);
                    tokio::time::sleep(jitter).await;
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
                    info!("Processing account: {}", pubkey);
                    if let Some(data) = keyed_account.account.data.decode() {
                        info!("Decoded data length: {}", data.len());
                        if let Ok(owner) = Pubkey::from_str(&keyed_account.account.owner) {
                            info!("Account owner: {}", owner);
                            let params = extract_market_params(&data, &owner);
                            info!("Extracted params: {:?}", params);

                            let account = MarketAccount::from((
                                pubkey,
                                Account {
                                    lamports: keyed_account.account.lamports,
                                    data,
                                    owner,
                                    executable: keyed_account.account.executable,
                                    rent_epoch: keyed_account.account.rent_epoch,
                                },
                                params,
                            ));

                            let market = self_clone.markets.read().await.get(&program_id).cloned();
                            if let Some(mut accounts) = market {
                                counter += 1;
                                if counter % 100 == 0 {
                                    info!("Processed {:?}", account.params);
                                }

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

const ACCOUNT_HEAD_PADDING: &[u8; 5] = b"serum";
const ACCOUNT_TAIL_PADDING: &[u8; 7] = b"padding";

pub fn extract_market_params(data: &[u8], owner: &Pubkey) -> Option<AccountParams> {
    // Check minimum length and header/footer padding
    if data.len() < (ACCOUNT_HEAD_PADDING.len() + ACCOUNT_TAIL_PADDING.len() + 47 * 8) {
        info!("Data length too short: {}", data.len());
        return None;
    }

    // Verify header padding
    if &data[..ACCOUNT_HEAD_PADDING.len()] != ACCOUNT_HEAD_PADDING {
        info!("Invalid header padding");
        return None;
    }

    // Verify footer padding
    let footer_start = data.len() - ACCOUNT_TAIL_PADDING.len();
    if &data[footer_start..] != ACCOUNT_TAIL_PADDING {
        info!("Invalid footer padding");
        return None;
    }

    // Skip the header to get to market data
    let market_data = &data[ACCOUNT_HEAD_PADDING.len()..footer_start];
    info!("Market data length: {}", market_data.len());

    // Helper function to convert [u8; 32] to Pubkey with info logging
    let try_create_pubkey = |offset: usize| {
        let start = offset * 8;
        let end = start + 32;
        if end <= market_data.len() {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&market_data[start..end]);
            let pubkey = Pubkey::new_from_array(bytes);
            info!("Created pubkey at offset {}: {}", offset, pubkey);
            Some(pubkey)
        } else {
            info!(
                "Failed to create pubkey at offset {}: out of bounds",
                offset
            );
            None
        }
    };

    // Extract pubkeys with logging
    let own_address = try_create_pubkey(1).or_else(|| {
        info!("Failed to extract own_address");
        None
    })?;

    let asks = try_create_pubkey(39).or_else(|| {
        info!("Failed to extract asks");
        None
    })?;

    let bids = try_create_pubkey(35).or_else(|| {
        info!("Failed to extract bids");
        None
    })?;

    let event_q = try_create_pubkey(31).or_else(|| {
        info!("Failed to extract event_q");
        None
    })?;

    let coin_vault = try_create_pubkey(14).or_else(|| {
        info!("Failed to extract coin_vault");
        None
    })?;

    let pc_vault = try_create_pubkey(20).or_else(|| {
        info!("Failed to extract pc_vault");
        None
    })?;

    // Get vault signer nonce with logging
    let nonce_start = 5 * 8;
    let nonce_end = nonce_start + 8;
    if nonce_end > market_data.len() {
        info!("Failed to extract vault signer nonce: out of bounds");
        return None;
    }

    let vault_signer_nonce = match market_data[nonce_start..nonce_end].try_into() {
        Ok(bytes) => u64::from_le_bytes(bytes),
        Err(_) => {
            info!("Failed to convert vault signer nonce bytes");
            return None;
        }
    };

    info!("Vault signer nonce: {}", vault_signer_nonce);

    // Derive vault signer
    let (vault_signer, _bump) = Pubkey::find_program_address(
        &[own_address.as_ref(), &vault_signer_nonce.to_le_bytes()],
        owner,
    );

    info!("Derived vault signer: {}", vault_signer);

    let params = AccountParams {
        routing_group: 0,
        address_lookup_table: Pubkey::default(),
        serum_asks: asks,
        serum_bids: bids,
        serum_coin_vault: coin_vault,
        serum_event_queue: event_q,
        serum_pc_vault: pc_vault,
        serum_vault_signer: vault_signer,
    };

    info!("Successfully extracted market params");
    Some(params)
}
