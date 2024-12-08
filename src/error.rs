use thiserror::Error;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
    #[error("Failed to parse pubkey: {0}")]
    PubkeyParseError(String),
    #[error("Server error: {0}")]
    ServerError(#[from] std::io::Error),
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}
