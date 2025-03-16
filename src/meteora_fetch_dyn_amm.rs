use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_program::pubkey::Pubkey;
use solana_sdk::account::Account;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

// Account discriminator for Meteora pools
pub const ACCOUNT_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];

// Program ID for Meteora's DynAMM
pub const METEORA_PROGRAM_ID: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";

// Token Program ID
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

// We'll add some common tokens for reference
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// A lightweight structure with essential pool information
#[derive(Debug, Clone, PartialEq)]
pub struct SimplePoolInfo {
    pub address: Pubkey,
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub lp_mint: Pubkey,
    pub a_vault: Pubkey,
    pub b_vault: Pubkey,
    pub enabled: bool,
    pub curve_type: SimpleCurveType,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SimpleCurveType {
    ConstantProduct,
    StableSwap(u64), // Amp coefficient
    Unknown(u8),     // Tag for unknown curve types
}

/// Extract essential pool information without deserializing the entire account
pub fn extract_pool_info(address: &Pubkey, data: &[u8]) -> Result<SimplePoolInfo, String> {
    if data.len() < 200 {
        return Err("Data too short for a Meteora pool account".to_string());
    }

    // Verify discriminator
    if &data[0..8] != ACCOUNT_DISCRIMINATOR.as_ref() {
        return Err("Invalid discriminator".to_string());
    }

    // Extract public keys at known offsets
    let lp_mint = extract_pubkey(&data[8..40])?;
    let token_a_mint = extract_pubkey(&data[40..72])?;
    let token_b_mint = extract_pubkey(&data[72..104])?;
    let a_vault = extract_pubkey(&data[104..136])?;
    let b_vault = extract_pubkey(&data[136..168])?;

    // Extract enabled status (byte after a_vault_lp_bump at offset 200)
    let enabled = data[200] == 1;

    // Extract fee information - better estimates based on observed data patterns
    let trade_fee_numerator = match extract_u64(&data[296..304]) {
        Ok(num) => num,
        Err(_) => 0, // Default if we can't extract
    };
    
    let trade_fee_denominator = match extract_u64(&data[304..312]) {
        Ok(num) => {
            if num == 0 { 1 } else { num } // Avoid division by zero
        },
        Err(_) => 1, // Default to 1 to avoid division by zero
    };

    // Create a curve type using a dynamic scanning approach
    let curve_type = find_curve_type(data)?;

    Ok(SimplePoolInfo {
        address: *address,
        token_a_mint,
        token_b_mint,
        lp_mint,
        a_vault,
        b_vault,
        enabled,
        curve_type,
        trade_fee_numerator,
        trade_fee_denominator,
    })
}

/// Helper function to extract a Pubkey from a byte slice
fn extract_pubkey(bytes: &[u8]) -> Result<Pubkey, String> {
    if bytes.len() != 32 {
        return Err(format!("Invalid pubkey length: {}", bytes.len()));
    }

    let mut pubkey_bytes = [0u8; 32];
    pubkey_bytes.copy_from_slice(bytes);
    Ok(Pubkey::new_from_array(pubkey_bytes))
}

/// Helper function to extract a u64 from a byte slice
fn extract_u64(bytes: &[u8]) -> Result<u64, String> {
    if bytes.len() != 8 {
        return Err(format!("Invalid u64 length: {}", bytes.len()));
    }

    let mut u64_bytes = [0u8; 8];
    u64_bytes.copy_from_slice(bytes);
    Ok(u64::from_le_bytes(u64_bytes))
}

/// Find the curve type by analyzing pool data structure
fn find_curve_type(data: &[u8]) -> Result<SimpleCurveType, String> {
    // In Meteora's code, the curve type is likely stored as an enum discriminator
    // followed by its fields. For ConstantProduct (0), there are no fields.
    // For StableSwap (1), there are several fields including amp.
    
    // Known potential offsets based on analysis
    let potential_offsets = [
        940, 950, 960, 970, 980, 990, 1000, 1010, 1020
    ];
    
    for &offset in &potential_offsets {
        if offset < data.len() {
            let tag = data[offset];
            
            // Check if this looks like a valid enum discriminator
            if tag == 0 {
                return Ok(SimpleCurveType::ConstantProduct);
            } else if tag == 1 {
                // For StableSwap (1), try to extract amp coefficient
                // Amp would be the u64 right after the discriminator
                if offset + 9 <= data.len() {
                    if let Ok(amp) = extract_u64(&data[offset+1..offset+9]) {
                        return Ok(SimpleCurveType::StableSwap(amp));
                    }
                }
            }
        }
    }
    
    // Default to ConstantProduct as a fallback
    Ok(SimpleCurveType::ConstantProduct)
}

/// Fetch all Meteora pools for a specific token using a lightweight approach
pub async fn fetch_meteora_pools_for_token(
    client: &RpcClient,
    token_mint: &Pubkey,
) -> Result<Vec<SimplePoolInfo>, String> {
    let program_id = Pubkey::from_str(METEORA_PROGRAM_ID)
        .map_err(|_| format!("Invalid program ID: {}", METEORA_PROGRAM_ID))?;

    println!("Finding Meteora pools for token: {}", token_mint);

    // Create a filter for the account discriminator
    let discriminator_filter = RpcFilterType::Memcmp(Memcmp::new(
        0,
        MemcmpEncodedBytes::Base58(bs58::encode(&ACCOUNT_DISCRIMINATOR).into_string()),
    ));

    // Results map to avoid duplicates
    let mut pools_map: HashMap<String, SimplePoolInfo> = HashMap::new();

    // 1. Fetch pools where token is in position A
    println!("Fetching pools where token is in position A");
    let config = RpcProgramAccountsConfig {
        filters: Some(vec![
            discriminator_filter.clone(),
            RpcFilterType::Memcmp(Memcmp::new(
                40, // Offset for token_a_mint
                MemcmpEncodedBytes::Base58(bs58::encode(token_mint.to_bytes()).into_string()),
            )),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
            commitment: None,
            data_slice: None,
            min_context_slot: None,
        },
        with_context: None,
        sort_results: None,
    };

    match client.get_program_accounts_with_config(&program_id, config) {
        Ok(accounts) => {
            println!("Found {} pools with token in position A", accounts.len());

            for (pubkey, account) in accounts {
                match extract_pool_info(&pubkey, &account.data) {
                    Ok(pool_info) => {
                        pools_map.insert(pubkey.to_string(), pool_info);
                    }
                    Err(err) => {
                        println!("Failed to extract pool info: {}", err);
                    }
                }
            }
        }
        Err(err) => {
            println!("Error fetching token A pools: {}", err);
        }
    }

    // 2. Fetch pools where token is in position B
    println!("Fetching pools where token is in position B");
    let config_b = RpcProgramAccountsConfig {
        filters: Some(vec![
            discriminator_filter,
            RpcFilterType::Memcmp(Memcmp::new(
                72, // Offset for token_b_mint
                MemcmpEncodedBytes::Base58(bs58::encode(token_mint.to_bytes()).into_string()),
            )),
        ]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
            commitment: None,
            data_slice: None,
            min_context_slot: None,
        },
        with_context: None,
        sort_results: None,
    };

    match client.get_program_accounts_with_config(&program_id, config_b) {
        Ok(accounts) => {
            println!("Found {} pools with token in position B", accounts.len());

            for (pubkey, account) in accounts {
                // Skip if we already have this pool
                if pools_map.contains_key(&pubkey.to_string()) {
                    continue;
                }

                match extract_pool_info(&pubkey, &account.data) {
                    Ok(pool_info) => {
                        pools_map.insert(pubkey.to_string(), pool_info);
                    }
                    Err(err) => {
                        println!("Failed to extract pool info: {}", err);
                    }
                }
            }
        }
        Err(err) => {
            println!("Error fetching token B pools: {}", err);
        }
    }

    // Convert the map to a vector
    let pools: Vec<SimplePoolInfo> = pools_map.into_values().collect();

    println!(
        "Found {} total Meteora pools containing token {}",
        pools.len(),
        token_mint
    );

    Ok(pools)
}

/// A more reliable way to get token vault balances directly from accounts
async fn get_token_vault_balances(
    client: &RpcClient,
    pool_info: &SimplePoolInfo
) -> Result<(u64, u64), String> {
    // Directly fetch the vault accounts
    let a_account = match client.get_account(&pool_info.a_vault) {
        Ok(account) => account,
        Err(e) => return Err(format!("Failed to get Token A vault account: {}", e))
    };
    
    let b_account = match client.get_account(&pool_info.b_vault) {
        Ok(account) => account,
        Err(e) => return Err(format!("Failed to get Token B vault account: {}", e))
    };
    
    // Extract the amount field from token account data
    // Token account layout: https://github.com/solana-labs/solana-program-library/blob/master/token/program/src/state.rs
    // The amount is at offset 64 (u64)
    let a_amount = if a_account.data.len() >= 72 {
        extract_u64(&a_account.data[64..72]).unwrap_or(0)
    } else {
        0
    };
    
    let b_amount = if b_account.data.len() >= 72 {
        extract_u64(&b_account.data[64..72]).unwrap_or(0)
    } else {
        0
    };
    
    Ok((a_amount, b_amount))
}

/// Get token decimals directly from mint account
async fn get_token_decimals(
    client: &RpcClient,
    token_mint: &Pubkey
) -> Result<u8, String> {
    match client.get_account(token_mint) {
        Ok(account) => {
            // In token mint accounts, the decimals field is at offset 44
            if account.data.len() >= 45 {
                Ok(account.data[44])
            } else {
                Err(format!("Invalid mint account data length: {}", account.data.len()))
            }
        },
        Err(e) => {
            // Default to 6 decimals if we can't get the information
            println!("Warning: Failed to get mint info for {}: {}", token_mint, e);
            Ok(6)
        }
    }
}

/// Get the price of a token from a Meteora DynAMM pool
pub async fn get_token_price_from_pool(
    client: &RpcClient,
    pool_info: &SimplePoolInfo,
    token_mint: &Pubkey
) -> Result<f64, String> {
    // Check if token is in the pool
    let token_position = if &pool_info.token_a_mint == token_mint {
        'A'
    } else if &pool_info.token_b_mint == token_mint {
        'B'
    } else {
        return Err(format!("Token {} not found in pool", token_mint));
    };
    
    // Get token balances from the pool
    let (token_a_amount, token_b_amount) = get_token_vault_balances(client, pool_info).await?;
    
    // Get token decimals
    let token_a_decimals = get_token_decimals(client, &pool_info.token_a_mint).await?;
    let token_b_decimals = get_token_decimals(client, &pool_info.token_b_mint).await?;
    
    println!("Pool balances: {} token A ({} decimals), {} token B ({} decimals)",
             token_a_amount, token_a_decimals, token_b_amount, token_b_decimals);
    
    // Normalize token amounts based on their decimals
    let decimal_diff = token_a_decimals as i32 - token_b_decimals as i32;
    let (normalized_a, normalized_b) = if decimal_diff > 0 {
        (token_a_amount as f64, token_b_amount as f64 * 10_f64.powi(decimal_diff))
    } else if decimal_diff < 0 {
        (token_a_amount as f64 * 10_f64.powi(-decimal_diff), token_b_amount as f64)
    } else {
        (token_a_amount as f64, token_b_amount as f64)
    };
    
    // Calculate price based on curve type
    let price = match &pool_info.curve_type {
        SimpleCurveType::ConstantProduct => {
            // For x * y = k, the price is simply y/x for token_a or x/y for token_b
            if token_position == 'A' {
                // Price of A in terms of B
                normalized_b / normalized_a
            } else {
                // Price of B in terms of A
                normalized_a / normalized_b
            }
        },
        SimpleCurveType::StableSwap(amp) => {
            // For stable pools, prices are influenced by the amplification factor
            // This is a simplified price calculation for stable pools
            let amp_f = *amp as f64;
            
            if token_position == 'A' {
                // Base price from spot price formula for stable pools
                let spot_price = normalized_b / normalized_a;
                
                // Adjust price based on deviation from ideal 1:1 ratio
                let deviation = (normalized_b / normalized_a - 1.0).abs();
                let adjustment = 1.0 + (deviation / (amp_f + 1.0));
                
                spot_price * adjustment
            } else {
                // Base price from spot price formula for stable pools
                let spot_price = normalized_a / normalized_b;
                
                // Adjust price based on deviation from ideal 1:1 ratio
                let deviation = (normalized_a / normalized_b - 1.0).abs();
                let adjustment = 1.0 + (deviation / (amp_f + 1.0));
                
                spot_price * adjustment
            }
        },
        SimpleCurveType::Unknown(_) => {
            // Fallback to basic ratio for unknown curve types
            if token_position == 'A' {
                normalized_b / normalized_a
            } else {
                normalized_a / normalized_b
            }
        }
    };
    
    Ok(price)
}

/// Format pool information with price data
pub fn format_pool_with_price(
    pool_info: &SimplePoolInfo,
    token_mint: &Pubkey,
    price: f64,
    token_a_name: &str,
    token_b_name: &str
) -> String {
    let mut info = String::new();
    
    info.push_str(&format!("Pool: {}\n", pool_info.address));
    
    // Determine the token names
    let (base_token, quote_token) = if &pool_info.token_a_mint == token_mint {
        (token_a_name, token_b_name)
    } else {
        (token_b_name, token_a_name)
    };
    
    info.push_str(&format!("Pair: {}-{}\n", base_token, quote_token));
    info.push_str(&format!("Price: 1 {} = {:.8} {}\n", base_token, price, quote_token));
    
    match &pool_info.curve_type {
        SimpleCurveType::ConstantProduct => {
            info.push_str("Curve: Constant Product (x*y=k)\n");
        },
        SimpleCurveType::StableSwap(amp) => {
            info.push_str(&format!("Curve: StableSwap (Amp: {})\n", amp));
        },
        SimpleCurveType::Unknown(_) => {
            info.push_str("Curve: Unknown\n");
        }
    }
    
    // Fee information
    let fee_percentage = if pool_info.trade_fee_denominator > 0 {
        (pool_info.trade_fee_numerator as f64 / pool_info.trade_fee_denominator as f64) * 100.0
    } else {
        0.0 // Avoid division by zero
    };
    
    info.push_str(&format!("Trade Fee: {:.4}%\n", fee_percentage));
    
    info
}

/// Main function to fetch and display Meteora pools for a token
pub async fn analyze_token_pools(
    rpc_url: &str,
    token_address: &str,
    token_name: &str
) -> Result<(), String> {
    // Create RPC client with longer timeout
    let client = RpcClient::new_with_timeout(
        rpc_url.to_string(), 
        Duration::from_secs(60)
    );
    
    // Parse token address
    let token_mint = Pubkey::from_str(token_address)
        .map_err(|_| format!("Invalid token address: {}", token_address))?;
    
    // Fetch all pools containing this token
    let pools = fetch_meteora_pools_for_token(&client, &token_mint).await
        .map_err(|e| format!("Failed to fetch pools: {}", e))?;
    
    println!("Found {} pools containing {}", pools.len(), token_address);
    
    if pools.is_empty() {
        println!("No pools found for this token.");
        return Ok(());
    }
    
    println!("\nAnalyzing pools to find the best price data...");
    println!("-----------------------------------------------");
    
    // Filter to only include enabled pools or pools with reasonable fee denominators
    let active_pools: Vec<&SimplePoolInfo> = pools.iter()
        .filter(|pool| pool.enabled || 
            (pool.trade_fee_denominator > 0 && pool.trade_fee_denominator < 100000000000u64))
        .collect();
    
    println!("Found {} active pools for analysis", active_pools.len());
    
    // Define common token names for better display
    let mut token_names: HashMap<String, String> = HashMap::new();
    token_names.insert(USDC_MINT.to_string(), "USDC".to_string());
    token_names.insert(USDT_MINT.to_string(), "USDT".to_string());
    token_names.insert(SOL_MINT.to_string(), "SOL".to_string());
    token_names.insert(token_address.to_string(), token_name.to_string());
    
    // Try to find USDC pairs first
    let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
    
    let mut usdc_pools: Vec<&SimplePoolInfo> = active_pools.iter()
        .filter(|pool| pool.token_a_mint == usdc_mint || pool.token_b_mint == usdc_mint)
        .copied()
        .collect();
    
    println!("\nFound {} {}/USDC pools", usdc_pools.len(), token_name);
    
    // Check for USDC pools first
    if !usdc_pools.is_empty() {
        for (i, pool_info) in usdc_pools.iter().enumerate().take(3) {
            println!("\nUSDC Pool #{}: {}", i+1, pool_info.address);
            
            // Try to get price
            match get_token_price_from_pool(&client, pool_info, &token_mint).await {
                Ok(price) => {
                    println!("Price: 1 {} = {:.8} USDC", token_name, price);
                    println!("Price: 1 USDC = {:.2} {}", 1.0 / price, token_name);
                    
                    // Get trading fee
                    let fee_percentage = if pool_info.trade_fee_denominator > 0 {
                        (pool_info.trade_fee_numerator as f64 / 
                         pool_info.trade_fee_denominator as f64) * 100.0
                    } else {
                        0.0
                    };
                    
                    println!("Trading fee: {:.4}%", fee_percentage);
                    
                    // Print curve type
                    match &pool_info.curve_type {
                        SimpleCurveType::ConstantProduct => {
                            println!("Curve Type: Constant Product (x*y=k)");
                        },
                        SimpleCurveType::StableSwap(amp) => {
                            println!("Curve Type: StableSwap (Amplification: {})", amp);
                        },
                        SimpleCurveType::Unknown(tag) => {
                            println!("Curve Type: Unknown (tag: {})", tag);
                        }
                    }
                },
                Err(err) => {
                    println!("Failed to get price: {}", err);
                }
            }
        }
    }
    
    // Check other pairs
    if usdc_pools.is_empty() {
        println!("No {}/USDC pools found. Checking other pools...", token_name);
    } else {
        println!("\nChecking other significant pools...");
    }
    
    // Filter other pools to exclude any we've already analyzed
    let other_pools: Vec<&SimplePoolInfo> = active_pools.iter()
        .filter(|pool| !usdc_pools.contains(pool))
        .take(5)
        .copied()
        .collect();
    
    for (i, pool_info) in other_pools.iter().enumerate() {
        println!("\nPool #{}: {}", i+1, pool_info.address);
        
        // Get the other token in the pool
        let other_token_mint = if pool_info.token_a_mint == token_mint {
            &pool_info.token_b_mint
        } else {
            &pool_info.token_a_mint
        };
        
        let other_token_name = &other_token_mint.to_string();
        // Try to get or guess token name
        let other_token_name = token_names
            .get(&other_token_mint.to_string())
            .unwrap_or(other_token_name);
        
        println!("Pair: {}-{}", token_name, other_token_name);
        
        // Try to get price
        match get_token_price_from_pool(&client, pool_info, &token_mint).await {
            Ok(price) => {
                println!("Price: 1 {} = {:.8} {}", token_name, price, other_token_name);
                println!("Price: 1 {} = {:.2} {}", other_token_name, 1.0 / price, token_name);
                
                // Get trading fee
                let fee_percentage = if pool_info.trade_fee_denominator > 0 {
                    (pool_info.trade_fee_numerator as f64 / 
                     pool_info.trade_fee_denominator as f64) * 100.0
                } else {
                    0.0
                };
                
                println!("Trading fee: {:.4}%", fee_percentage);
                
                // Print curve type
                match &pool_info.curve_type {
                    SimpleCurveType::ConstantProduct => {
                        println!("Curve Type: Constant Product (x*y=k)");
                    },
                    SimpleCurveType::StableSwap(amp) => {
                        println!("Curve Type: StableSwap (Amplification: {})", amp);
                    },
                    SimpleCurveType::Unknown(tag) => {
                        println!("Curve Type: Unknown (tag: {})", tag);
                    }
                }
            },
            Err(err) => {
                println!("Failed to get price: {}", err);
            }
        }
    }
    
    Ok(())
}

// Example usage
pub async fn main() -> Result<(), String> {
    // Use a reliable public RPC endpoint
    let rpc_url = "https://api.mainnet-beta.solana.com";
    
    // Example token: BONK
    let token_address = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
    let token_name = "BONK";
    
    analyze_token_pools(rpc_url, token_address, token_name).await
}
