use async_trait::async_trait;
use shaku::Interface;

#[async_trait]
pub trait RateLimiter: Interface {
    async fn acquire(&self) -> Result<(), RateLimiterError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RateLimiterError {
    /// Failed to establish or maintain connection to Redis.
    /// Usually indicates network issues or Redis being unavailable.
    #[error("Failed to connect to Redis: {0}")]
    ConnectionError(String),

    /// Redis Lua script execution failed.
    /// Typically points to a logic error in the distributed limiter script.
    #[error("Failed to execute rate limiting script: {0}")]
    ScriptError(String),

    /// An unexpected internal error occurred while enforcing rate limits.
    /// Should not happen under normal conditions.
    #[error("An unexpected error occurred: {0}")]
    Unexpected(String),
}
