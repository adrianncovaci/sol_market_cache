use futures::future::join_all;
use futures::StreamExt;
use metrics::{counter, gauge};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use redis::AsyncCommands;
use serde_json::{json, Value};
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::RpcFilterType;
use solana_program::pubkey::Pubkey;
use solana_sdk::clock::Slot;
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
            is_signer: Some(false),
            is_writable: Some(false),
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

    async fn fetch_market(
        &self,
        program_id: Pubkey,
        min_context_slot: Option<Slot>,
    ) -> Result<Vec<MarketAccount>, CacheError> {
        let mut retries = 0;
        let outer_timestamp = Instant::now();

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
                        min_context_slot,
                    },
                    with_context: Some(true),
                    sort_results: Some(false),
                }
            } else if program_id.to_string() == "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" {
                RpcProgramAccountsConfig {
                    filters: Some(vec![
                        RpcFilterType::DataSize(752), // Serum market state size
                    ]),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        data_slice: None,
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot,
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
                        min_context_slot,
                    },
                    with_context: Some(true),
                    sort_results: Some(false),
                }
            };
            match self
                .rpc_client
                .get_program_accounts_with_config(&program_id, config.clone())
            {
                Ok(rpc_accounts) => {
                    info!(
                        "Fetched {} accounts for program {}, took {} seconds",
                        rpc_accounts.len(),
                        program_id,
                        outer_timestamp.elapsed().as_secs()
                    );
                    if program_id
                        == Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap()
                    {
                        match reqwest::get("https://api.raydium.io/v2/sdk/liquidity/mainnet.json")
                            .await
                        {
                            Ok(response) => {
                                let json: Value = response
                                    .json()
                                    .await
                                    .map_err(|err| CacheError::Other(err.into()))?;
                                let pools = json["official"].as_array().ok_or_else(|| {
                                    CacheError::Other(anyhow::format_err!(
                                        "No official pools found"
                                    ))
                                })?;
                                info!("pools len: {:?}", pools.len());
                                let unofficial_pools =
                                    json["unOfficial"].as_array().ok_or_else(|| {
                                        CacheError::Other(anyhow::format_err!(
                                            "No un-official pools found"
                                        ))
                                    })?;
                                info!("unofficial pools len: {:?}", unofficial_pools.len());
                                let rpc_accounts_map: HashMap<_, _> = rpc_accounts
                                    .iter()
                                    .map(|(pubkey, account)| (*pubkey, account))
                                    .collect();
                                let program_owner = Pubkey::from_str(
                                    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
                                )
                                .map_err(|e| CacheError::Other(e.into()))?;

                                let official_accounts = pools.par_iter().filter_map(|pool| {
                                    let pubkey = Pubkey::from_str(pool["id"].as_str()?).ok()?;

                                    let rpc_account = rpc_accounts_map.get(&pubkey)?;

                                    let alt = pool["lookupTableAccount"].as_str()?;
                                    let params = AccountParams {
                                        routing_group: 2,
                                        address_lookup_table: Pubkey::from_str(alt).ok()?,
                                        serum_asks: Pubkey::from_str(pool["marketAsks"].as_str()?)
                                            .ok()?,
                                        serum_bids: Pubkey::from_str(pool["marketBids"].as_str()?)
                                            .ok()?,
                                        serum_coin_vault: Pubkey::from_str(
                                            pool["marketBaseVault"].as_str()?,
                                        )
                                        .ok()?,
                                        serum_event_queue: Pubkey::from_str(
                                            pool["marketEventQueue"].as_str()?,
                                        )
                                        .ok()?,
                                        serum_pc_vault: Pubkey::from_str(
                                            pool["marketQuoteVault"].as_str()?,
                                        )
                                        .ok()?,
                                        serum_vault_signer: Pubkey::from_str(
                                            pool["marketAuthority"].as_str()?,
                                        )
                                        .ok()?,
                                    };

                                    Some(MarketAccount {
                                        pubkey: Some(pubkey),
                                        lamports: Some(rpc_account.lamports),
                                        data: rpc_account.data.clone(),
                                        owner: Some(program_owner),
                                        executable: Some(rpc_account.executable),
                                        rent_epoch: Some(rpc_account.rent_epoch),
                                        space: Some(rpc_account.data.len() as u64),
                                        params: Some(params),
                                        is_signer: Some(false),
                                        is_writable: Some(false),
                                        last_updated: Some(Instant::now()),
                                    })
                                });

                                let unofficial_accounts =
                                    unofficial_pools.par_iter().filter_map(|pool| {
                                        let pubkey = Pubkey::from_str(pool["id"].as_str()?).ok()?;

                                        let rpc_account = rpc_accounts_map.get(&pubkey)?;

                                        let alt = pool["lookupTableAccount"].as_str()?;
                                        let params = AccountParams {
                                            routing_group: 2,
                                            address_lookup_table: Pubkey::from_str(alt).ok()?,
                                            serum_asks: Pubkey::from_str(
                                                pool["marketAsks"].as_str()?,
                                            )
                                            .ok()?,
                                            serum_bids: Pubkey::from_str(
                                                pool["marketBids"].as_str()?,
                                            )
                                            .ok()?,
                                            serum_coin_vault: Pubkey::from_str(
                                                pool["marketBaseVault"].as_str()?,
                                            )
                                            .ok()?,
                                            serum_event_queue: Pubkey::from_str(
                                                pool["marketEventQueue"].as_str()?,
                                            )
                                            .ok()?,
                                            serum_pc_vault: Pubkey::from_str(
                                                pool["marketQuoteVault"].as_str()?,
                                            )
                                            .ok()?,
                                            serum_vault_signer: Pubkey::from_str(
                                                pool["marketAuthority"].as_str()?,
                                            )
                                            .ok()?,
                                        };

                                        Some(MarketAccount {
                                            pubkey: Some(pubkey),
                                            lamports: Some(rpc_account.lamports),
                                            data: rpc_account.data.clone(),
                                            owner: Some(program_owner),
                                            executable: Some(rpc_account.executable),
                                            rent_epoch: Some(rpc_account.rent_epoch),
                                            space: Some(rpc_account.data.len() as u64),
                                            params: Some(params),
                                            is_signer: Some(false),
                                            is_writable: Some(false),
                                            last_updated: Some(Instant::now()),
                                        })
                                    });
                                let accounts = official_accounts
                                    .chain(unofficial_accounts)
                                    .collect::<Vec<_>>();
                                info!(
                                    "Fetched {} Raydium pools, took {} seconds",
                                    accounts.len(),
                                    outer_timestamp.elapsed().as_secs()
                                );
                                info!("First raydium pool: {:?}", accounts.first());

                                return Ok(accounts);
                            }
                            Err(err) => {
                                error!("Failed to fetch Raydium pools: {err}");
                                return Err(CacheError::Other(anyhow::Error::new(err)));
                            }
                        }
                    } else {
                        info!(
                            "Fetched {} accounts for program {}, took {} seconds",
                            rpc_accounts.len(),
                            program_id,
                            outer_timestamp.elapsed().as_secs()
                        );
                        return Ok(rpc_accounts
                            .into_iter()
                            .map(|(pubkey, account)| MarketAccount::from((pubkey, account, None)))
                            .collect());
                    }
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
        //info!("Starting market cache update");
        //let prev_slot = self.rpc_client.get_slot().ok();
        //let pubkeys = self.pubkeys_index.read().await.clone();
        //let mut tasks = Vec::new();
        //let self_ref = self.clone();
        //for &program_id in pubkeys.iter() {
        //    let self_clone = self_ref.clone();
        //    let mut interval = tokio::time::interval(self_clone.cache_config.refresh_interval);
        //    let task = tokio::spawn(async move {
        //        let timestamp = Instant::now();
        //        let slot = match self_clone.get_latest_redis_slot(&program_id).await {
        //            Ok(slot) => slot,
        //            Err(_) => {
        //                error!("Failed to fetch latest slot from Redis");
        //                None
        //            }
        //        };
        //        let accounts = self_clone.fetch_market(program_id, slot).await;
        //        match accounts {
        //            Ok(accounts) => {
        //                {
        //                    let mut markets = self_clone.markets.write().await;
        //                    markets.insert(program_id, accounts.clone().into_iter().collect());
        //                }

        //                gauge!("cache_update_time").set(timestamp.elapsed().as_secs_f64());
        //                info!(
        //                    "Cache update completed in {} seconds for program {}",
        //                    timestamp.elapsed().as_secs(),
        //                    program_id
        //                );

        //                let _ = self_clone
        //                    .store_in_redis(&program_id, &accounts, prev_slot)
        //                    .await;
        //                let start = SystemTime::now();
        //                let timestamp = start
        //                    .duration_since(UNIX_EPOCH)
        //                    .expect("Time went backwards")
        //                    .as_secs();
        //                self_clone
        //                    .redis_config
        //                    .last_updated
        //                    .as_ref()
        //                    .map(|x| x.store(timestamp, Ordering::Relaxed));
        //            }
        //            Err(e) => {
        //                error!("Failed to fetch market accounts: {:?}", e);
        //            }
        //        }
        //        interval.tick().await;
        //    });
        //    tasks.push(task);
        //}

        //join_all(tasks).await;
        Ok(())
    }

    pub async fn get_markets(&self) -> HashMap<Pubkey, HashSet<MarketAccount>> {
        self.markets.read().await.clone()
    }

    async fn store_in_redis(
        &self,
        program_id: &Pubkey,
        accounts: &[MarketAccount],
        slot: Option<Slot>,
    ) -> Result<(), CacheError> {
        if let Some(slot) = slot {
            let slot_key = format!("{}_slot", program_id);
            let slot_value = slot.to_string();

            match {
                let mut redis_conn = self.redis_config.redis_connection.write().await;
                redis_conn
                    .set::<String, String, ()>(slot_key, slot_value)
                    .await
            } {
                Ok(_) => {}
                Err(err) => {
                    error!("Failed to store slot in Redis: {:?}", err);
                    return Err(CacheError::RedisError(err));
                }
            }
        } else {
            info!("No slot provided for program {}", program_id);
        }

        const CHUNK_SIZE: usize = 10_000;
        let chunks: Vec<_> = accounts.chunks(CHUNK_SIZE).collect();
        let chunk_futures = chunks.iter().enumerate().map(|(i, chunk)| {
            let self_clone = self.clone();
            let program_id = program_id;
            let chunk = chunk.to_vec();

            async move {
                let chunk_json =
                    serde_json::to_string(&chunk).map_err(CacheError::SerializationError)?;

                let mut redis_conn = self_clone.redis_config.redis_connection.write().await;
                redis_conn
                    .rpush::<String, String, ()>(program_id.to_string(), chunk_json)
                    .await
                    .map_err(|e| {
                        error!("Failed to store chunk {} in Redis: {:?}", i, e);
                        CacheError::RedisError(e)
                    })?;

                Ok::<_, CacheError>(())
            }
        });

        // Process all chunks concurrently
        if let Err(e) = futures::future::try_join_all(chunk_futures).await {
            error!("Error storing chunks for {}: {:?}", program_id, e);
        }

        // Set expiry
        {
            let mut redis_conn = self.redis_config.redis_connection.write().await;
            redis_conn
                .expire(
                    program_id.to_string(),
                    self.redis_config.cache_ttl.as_secs() as i64,
                )
                .await
                .map_err(CacheError::RedisError)?;

            if slot.is_some() {
                redis_conn
                    .expire(
                        format!("{}_slot", program_id),
                        self.redis_config.cache_ttl.as_secs() as i64,
                    )
                    .await
                    .map_err(CacheError::RedisError)?;
            }
        }

        info!("Successfully completed storage for {}", program_id);
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
                            let account = MarketAccount::from((
                                pubkey,
                                Account {
                                    lamports: keyed_account.account.lamports,
                                    data,
                                    owner,
                                    executable: keyed_account.account.executable,
                                    rent_epoch: keyed_account.account.rent_epoch,
                                },
                                None,
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

    pub async fn export_to_jupiter_format(&self) -> Result<(), CacheError> {
        let pubkeys = self.pubkeys_index.read().await.clone();

        let mut jupiter_markets = Vec::new();

        for &program_id in pubkeys.iter() {
            info!("Fetching program {} from Redis", program_id);
            if let Ok(chunk_jsons) = self
                .redis_config
                .redis_connection
                .write()
                .await
                .lrange::<_, Vec<String>>(program_id.to_string(), 0, -1)
                .await
            {
                // Deserialize and flatten all chunks
                let mut accounts = Vec::new();
                for chunk_json in chunk_jsons {
                    if let Ok(chunk) = serde_json::from_str::<Vec<MarketAccount>>(&chunk_json) {
                        accounts.extend(chunk);
                    }
                }

                info!(
                    "Found {} markets for program {} in Redis",
                    accounts.len(),
                    program_id
                );
                if !accounts.is_empty() {
                    for market in accounts {
                        if let (Some(pubkey), Some(_), Some(params)) =
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
                                    "serumVaultSigner": params.serum_vault_signer.to_string(),
                                    "addressLookupTable": params.address_lookup_table.to_string(),
                                    "routingGroup": params.routing_group
                                }
                            });
                            jupiter_markets.push(market_entry);
                        }
                    }
                }
            }
        }

        info!(
            "Found data in Redis, writing {} markets to disk",
            jupiter_markets.len()
        );
        let json_string = serde_json::to_string(&jupiter_markets)?;
        tokio::fs::write("valid-markets.json", json_string).await?;
        Ok(())
    }

    async fn get_latest_redis_slot(&self, program_id: &Pubkey) -> Result<Option<Slot>, CacheError> {
        let result: Option<String> = {
            let mut redis = self.redis_config.redis_connection.write().await;
            redis
                .get(format!("{}_slot", program_id))
                .await
                .map_err(CacheError::RedisError)?
        };

        Ok(result.and_then(|s| s.parse().ok()))
    }
}
