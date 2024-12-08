use crate::{CacheConfig, CacheError, MarketAccount};
use metrics::{counter, gauge};
use solana_account_decoder_client_types::{UiAccountEncoding, UiDataSliceConfig};
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_program::pubkey::Pubkey;
use solana_sdk::{account::Account, commitment_config::CommitmentConfig};
use std::{collections::HashSet, str::FromStr, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use tracing::{error, info, warn};

pub struct MarketCache {
    markets: Arc<RwLock<HashSet<MarketAccount>>>,
    rpc_client: Arc<RpcClient>,
    pubkeys_index: Arc<RwLock<HashSet<Pubkey>>>,
    config: CacheConfig,
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
    pub fn new(
        rpc_url: String,
        pubkeys: HashSet<String>,
        config: Option<CacheConfig>,
    ) -> Result<Self, CacheError> {
        let config = config.unwrap_or_default();

        let pubkeys_index = pubkeys
            .iter()
            .map(|x| Pubkey::from_str(x).map_err(|_| CacheError::PubkeyParseError(x.clone())))
            .collect::<Result<HashSet<_>, _>>()?;

        Ok(Self {
            markets: Arc::new(RwLock::new(HashSet::new())),
            rpc_client: Arc::new(RpcClient::new_with_timeout(rpc_url, config.request_timeout)),
            pubkeys_index: Arc::new(RwLock::new(pubkeys_index)),
            config,
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

                    if retries >= self.config.max_retries {
                        error!("Max retries reached for program {}: {:?}", program_id, e);
                        return Err(CacheError::RpcError(e));
                    }

                    warn!("Retry {} for program {}: {:?}", retries + 1, program_id, e);
                    retries += 1;
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }
    }

    pub async fn update_cache(&self) -> Result<(), CacheError> {
        info!("Starting market cache update");
        let timestamp = Instant::now();

        let mut futures = Vec::new();
        for &program_id in self.pubkeys_index.read().await.iter() {
            let future = self.fetch_market(program_id);
            futures.push(future);
        }

        let results = futures::future::join_all(futures).await;
        let mut markets = self.markets.write().await;
        markets.clear();

        for result in results {
            match result {
                Ok(market_accounts) => {
                    markets.extend(market_accounts);
                }
                Err(e) => {
                    error!("Failed to fetch market accounts: {:?}", e);
                }
            }
        }

        gauge!("cache_update_time").set(timestamp.elapsed().as_secs_f64());
        info!(
            "Cache update completed in {} seconds",
            timestamp.elapsed().as_secs()
        );

        Ok(())
    }

    pub async fn start_background_refresh(self: Arc<Self>) {
        info!("Starting background refresh task");

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.config.refresh_interval);

            loop {
                interval.tick().await;

                match self.update_cache().await {
                    Ok(_) => {
                        info!("Successfully updated market cache");
                    }
                    Err(e) => {
                        error!("Failed to update market cache: {:?}", e);
                    }
                }
            }
        });
    }

    pub async fn get_markets(&self) -> HashSet<MarketAccount> {
        self.markets.read().await.clone()
    }
}
