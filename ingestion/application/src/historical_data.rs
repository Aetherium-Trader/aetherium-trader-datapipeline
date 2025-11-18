use async_trait::async_trait;
use chrono::NaiveDate;
use ingestion_domain::{DateRange, Tick};
use shaku::Interface;

#[async_trait]
pub trait HistoricalDataGateway: Interface {
    async fn fetch_historical_ticks(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Tick>, HistoricalDataError>;

    fn max_history_days(&self) -> u32;
}

#[async_trait]
pub trait GapDetector: Interface {
    async fn detect_gaps(
        &self,
        symbol: &str,
        range: DateRange,
    ) -> Result<Vec<DateRange>, GapDetectionError>;
}

#[derive(Debug, thiserror::Error)]
pub enum HistoricalDataError {
    #[error("API rate limit exceeded")]
    RateLimitExceeded,

    #[error("Historical data not available for date: {0}")]
    DataNotAvailable(NaiveDate),

    #[error("Gateway error: {0}")]
    GatewayError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum GapDetectionError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid date range")]
    InvalidDateRange,
}
