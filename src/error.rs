use thiserror::Error;

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
    #[error("Failed to parse pubkey: {0}")]
    PubkeyParseError(String),
    #[error("Server error: {0}")]
    ServerError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::error::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("WS error: {0}")]
    WSError(#[from] solana_pubsub_client::nonblocking::pubsub_client::PubsubClientError),
    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}
