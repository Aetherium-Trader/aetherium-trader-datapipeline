use async_trait::async_trait;
use chrono::NaiveDate;
use shaku::{Component, Interface};
use std::sync::Arc;

use crate::historical_data::{GapDetector, HistoricalDataGateway};
use crate::ports::TickRepository;
use ingestion_domain::DateRange;

#[async_trait]
pub trait BackfillService: Interface {
    async fn backfill_range(
        &self,
        symbol: &str,
        range: DateRange,
    ) -> Result<BackfillReport, BackfillError>;
}

#[derive(Component)]
#[shaku(interface = BackfillService)]
pub struct BackfillServiceImpl {
    #[shaku(inject)]
    gateway: Arc<dyn HistoricalDataGateway>,

    #[shaku(inject)]
    gap_detector: Arc<dyn GapDetector>,

    #[shaku(inject)]
    repository: Arc<dyn TickRepository>,
}

impl BackfillServiceImpl {
    async fn backfill_single_day(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<usize, BackfillError> {
        let ticks = self.gateway
            .fetch_historical_ticks(symbol, date)
            .await
            .map_err(BackfillError::GatewayError)?;

        let tick_count = ticks.len();

        if !ticks.is_empty() {
            self.repository
                .save_batch(ticks)
                .await
                .map_err(BackfillError::RepositoryError)?;
        }

        Ok(tick_count)
    }
}

#[async_trait]
impl BackfillService for BackfillServiceImpl {
    async fn backfill_range(
        &self,
        symbol: &str,
        range: DateRange,
    ) -> Result<BackfillReport, BackfillError> {
        let gaps = self.gap_detector
            .detect_gaps(symbol, range.clone())
            .await
            .map_err(BackfillError::GapDetectionError)?;

        let mut total_ticks = 0;
        let mut days_processed = 0;
        let mut failed_days = Vec::new();

        for gap in gaps {
            let days = gap.split_by_days();

            for day_range in days {
                let date = day_range.start();

                match self.backfill_single_day(symbol, date).await {
                    Ok(tick_count) => {
                        total_ticks += tick_count;
                        days_processed += 1;
                    }
                    Err(e) => {
                        failed_days.push((date, e.to_string()));
                    }
                }
            }
        }

        self.repository
            .shutdown()
            .await
            .map_err(BackfillError::RepositoryError)?;

        Ok(BackfillReport {
            symbol: symbol.to_string(),
            range,
            days_processed,
            total_ticks,
            failed_days,
        })
    }
}

#[derive(Debug)]
pub struct BackfillReport {
    pub symbol: String,
    pub range: DateRange,
    pub days_processed: usize,
    pub total_ticks: usize,
    pub failed_days: Vec<(NaiveDate, String)>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("Gateway error: {0}")]
    GatewayError(#[from] crate::historical_data::HistoricalDataError),

    #[error("Gap detection error: {0}")]
    GapDetectionError(#[from] crate::historical_data::GapDetectionError),

    #[error("Repository error: {0}")]
    RepositoryError(#[from] crate::ports::RepositoryError),
}