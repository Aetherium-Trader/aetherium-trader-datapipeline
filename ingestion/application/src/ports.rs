use async_trait::async_trait;
use ingestion_domain::Tick;
use shaku::Interface;

#[async_trait]
pub trait MarketDataGateway: Interface {
    async fn subscribe(&self, symbol: &str) -> Result<TickStream, GatewayError>;
}

#[async_trait]
pub trait TickRepository: Interface {
    async fn save_batch(&self, ticks: Vec<Tick>) -> Result<(), RepositoryError>;
    async fn flush(&self) -> Result<(), RepositoryError>;
    async fn shutdown(&self) -> Result<(), RepositoryError>;
}

pub type TickStream = Box<dyn futures::Stream<Item = Result<Tick, GatewayError>> + Send + Unpin>;

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Subscription failed for symbol {symbol}: {reason}")]
    SubscriptionFailed { symbol: String, reason: String },

    #[error("Data stream error: {0}")]
    StreamError(String),
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("File rotation error: {0}")]
    FileRotationError(String),
}
