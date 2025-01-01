use futures::future::join_all;
use futures::StreamExt;
use metrics::{counter, gauge};
use redis::AsyncCommands;
use serde_json::{json, Value};
use solana_account_decoder_client_types::{UiAccountEncoding, UiDataSliceConfig};
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::RpcFilterType;
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

use base64::{engine::general_purpose::STANDARD as base64_engine, Engine};

use bytemuck::{Pod, Zeroable};

#[derive(Copy, Clone, Pod, Zeroable, Debug)]
#[repr(C)]
pub struct MarketState {
    pub account_flags: u64,
    pub own_address: [u64; 4],
    pub vault_signer_nonce: u64,
    pub coin_mint: [u64; 4],
    pub pc_mint: [u64; 4],
    pub coin_vault: [u64; 4],
    pub coin_deposits_total: u64,
    pub coin_fees_accrued: u64,
    pub pc_vault: [u64; 4],
    pub pc_deposits_total: u64,
    pub pc_fees_accrued: u64,
    pub pc_dust_threshold: u64,
    pub req_q: [u64; 4],
    pub event_q: [u64; 4],
    pub bids: [u64; 4],
    pub asks: [u64; 4],
    pub coin_lot_size: u64,
    pub pc_lot_size: u64,
    pub fee_rate_bps: u64,
    pub referrer_rebates_accrued: u64,
}

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
            Duration::from_secs(24 * 60 * 60),
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
        let mut retries = 0;
        let timestamp = Instant::now();

        loop {
            let config = if program_id.to_string() == "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX"
            {
                RpcProgramAccountsConfig {
                    filters: Some(vec![
                        RpcFilterType::DataSize(388), // Serum market state size
                    ]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        data_slice: None,
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot: None,
                    },
                    with_context: Some(true),
                    sort_results: Some(false),
                }
            } else {
                RpcProgramAccountsConfig {
                    filters: None,
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        data_slice: None,
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot: None,
                    },
                    with_context: Some(true),
                    sort_results: Some(false),
                }
            };
            match self
                .rpc_client
                .get_program_accounts_with_config(&program_id, config.clone())
            {
                Ok(accounts) => {
                    info!("OWNER: {}", program_id);
                    let market_accounts: Vec<MarketAccount> = accounts
                        .into_iter()
                        .map(|(pubkey, account)| {
                            let market_params = extract_market_params(&account.data, &program_id);
                            MarketAccount::from((pubkey, account, market_params))
                        })
                        .collect();

                    info!(
                        "First account: {:?}",
                        market_accounts.iter().find(|mk| mk.params.is_some())
                    );

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
        info!("Storing valid markets in Jupiter format");

        self.export_to_jupiter_format().await.unwrap();

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
                    data_slice: None,
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
                    if let Some(data) = keyed_account.account.data.decode() {
                        if let Ok(owner) = Pubkey::from_str(&keyed_account.account.owner) {
                            let params = extract_market_params(&data, &owner);
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
                            if account.params.is_some() {
                                info!("Account: {:?}", account);
                            }

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

    pub async fn export_to_jupiter_format(&self) -> Result<Value, CacheError> {
        let mut jupiter_markets = Vec::new();

        for (_program_id, market_set) in self.get_markets().await {
            info!(
                "Processing program: {} len - {}, legit: {}",
                _program_id,
                market_set.len(),
                market_set.iter().filter(|x| x.params.is_some()).count()
            );
            for market in market_set {
                if let (Some(pubkey), Some(owner), Some(params)) =
                    (market.pubkey, market.owner, market.params)
                {
                    let data_base64 = base64_engine.encode(&market.data);

                    let market_entry = json!({
                        "pubkey": pubkey.to_string(),
                        "lamports": market.lamports,
                        "data": [data_base64, "base64"],
                        "owner": market.owner.map(|x| x.to_string()),
                        "executable": market.executable,
                        "rentEpoch": market.rent_epoch,
                        "space": market.space,
                        "params": {
                            "serumBids": params.serum_bids.to_string(),
                            "serumAsks": params.serum_asks.to_string(),
                            "serumEventQueue": params.serum_event_queue.to_string(),
                            "serumCoinVaultAccount": params.serum_coin_vault.to_string(),
                            "serumPcVaultAccount": params.serum_pc_vault.to_string(),
                            "serumVaultSigner": params.serum_vault_signer.to_string()
                        }
                    });
                    jupiter_markets.push(market_entry);
                }
            }
        }

        let json_string = serde_json::to_string(&jupiter_markets)?; // Removed pretty printing
        tokio::fs::write("valid-markets.json", json_string).await?;

        Ok(json!(jupiter_markets))
    }
}

pub fn extract_market_params(data: &[u8], owner: &Pubkey) -> Option<AccountParams> {
    fn u64_to_pubkey(arr: &[u64; 4], field_name: &str) -> Pubkey {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(bytemuck::bytes_of(arr));

        Pubkey::new_from_array(bytes)
    }

    if owner.to_string() == "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX" {
        let state_size = std::mem::size_of::<MarketState>();

        if data.len() < state_size {
            return None;
        }

        if data.len() >= state_size {
            let state: &MarketState = match bytemuck::try_from_bytes(&data[..state_size]) {
                Ok(state) => state,
                Err(_e) => {
                    return None;
                }
            };

            // Add validation checks
            if state.own_address == [0, 0, 0, 0] || state.vault_signer_nonce == 0 {
                return None;
            }

            // Convert addresses
            let bids = u64_to_pubkey(&state.bids, "bids");
            let asks = u64_to_pubkey(&state.asks, "asks");

            // Validate addresses aren't default
            if bids == Pubkey::default() || asks == Pubkey::default() {
                return None;
            }

            if data.len() >= state_size {
                let state: &MarketState = bytemuck::from_bytes(&data[..state_size]);
                // Derive vault signer
                let vault_signer = {
                    let nonce = state.vault_signer_nonce;
                    let market = u64_to_pubkey(&state.own_address, "market_address");

                    match Pubkey::create_program_address(
                        &[market.as_ref(), &nonce.to_le_bytes()],
                        owner,
                    ) {
                        Ok(signer) => signer,
                        Err(_e) => {
                            return None;
                        }
                    }
                };

                let bids = u64_to_pubkey(&state.bids, "bids");
                let asks = u64_to_pubkey(&state.asks, "asks");
                let event_queue = u64_to_pubkey(&state.event_q, "event_queue");
                let coin_vault = u64_to_pubkey(&state.coin_vault, "coin_vault");
                let pc_vault = u64_to_pubkey(&state.pc_vault, "pc_vault");

                if bids == Pubkey::default()
                    || asks == Pubkey::default()
                    || event_queue == Pubkey::default()
                {
                    return None;
                }

                return Some(AccountParams {
                    routing_group: 0,
                    address_lookup_table: Pubkey::default(),
                    serum_bids: bids,
                    serum_asks: asks,
                    serum_event_queue: event_queue,
                    serum_coin_vault: coin_vault,
                    serum_pc_vault: pc_vault,
                    serum_vault_signer: vault_signer,
                });
            } else {
            }
        }
    }
    None
}
