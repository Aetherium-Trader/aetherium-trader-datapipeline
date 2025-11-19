use async_trait::async_trait;
use shaku::Interface;

#[async_trait]
pub trait RateLimiter: Interface {
    async fn acquire(&self) -> Result<(), RateLimiterError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RateLimiterError {
    #[error("Failed to connect to Redis: {0}")]
    ConnectionError(String),

    #[error("Failed to execute rate limiting script: {0}")]
    ScriptError(String),

    #[error("An unexpected error occurred: {0}")]
    Unexpected(String),
}
