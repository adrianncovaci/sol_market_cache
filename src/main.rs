mod meteora_fetch_dyn_amm;
use std::str::FromStr;

use anyhow::Result;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use self::meteora_fetch_dyn_amm::{analyze_token_pools, fetch_meteora_pools_for_token, get_token_price_from_pool, SimpleCurveType};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let rpc_url = "http://84.32.186.51:8899".to_string();
    // BONK token
    let token_address = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
    let token_name = "BONK";
    
    println!("Analyzing {} pools on Meteora...\n", token_name);
    analyze_token_pools(&rpc_url, token_address, token_name).await
        .map_err(|e| format!("Analysis failed: {}", e)).unwrap();
    Ok(())
}
