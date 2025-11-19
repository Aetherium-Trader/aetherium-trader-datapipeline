use crate::ports::{MarketDataGateway, TickRepository};
use async_trait::async_trait;
use futures::StreamExt;
use shaku::{Component, Interface};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

#[async_trait]
pub trait IngestionService: Interface {
    async fn run(&self, symbol: &str) -> Result<(), IngestionError>;
}

#[derive(Component)]
#[shaku(interface = IngestionService)]
pub struct IngestionServiceImpl {
    #[shaku(inject)]
    gateway: Arc<dyn MarketDataGateway>,
    #[shaku(inject)]
    repository: Arc<dyn TickRepository>,
    batch_size: usize,
    flush_interval: Duration,
}

#[async_trait]
impl IngestionService for IngestionServiceImpl {
    async fn run(&self, symbol: &str) -> Result<(), IngestionError> {
        info!("Starting ingestion service for symbol: {}", symbol);

        let mut stream = self
            .gateway
            .subscribe(symbol)
            .await
            .map_err(IngestionError::GatewayError)?;

        let mut batch = Vec::with_capacity(self.batch_size);
        let mut flush_timer = tokio::time::interval(self.flush_interval);

        loop {
            tokio::select! {
                Some(tick_result) = stream.next() => {
                    match tick_result {
                        Ok(tick) => {
                            batch.push(tick);
                            if batch.len() >= self.batch_size {
                                self.flush_batch(&mut batch).await?;
                            }
                        }
                        Err(e) => {
                            error!("Stream error: {}", e);
                            return Err(IngestionError::GatewayError(e));
                        }
                    }
                }
                _ = flush_timer.tick() => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch).await?;
                    }
                }
                else => {
                    warn!("Market data stream ended");
                    break;
                }
            }
        }

        if !batch.is_empty() {
            self.flush_batch(&mut batch).await?;
        }

        self.repository.shutdown().await?;

        info!("Ingestion service stopped");
        Ok(())
    }
}

impl IngestionServiceImpl {
    async fn flush_batch(
        &self,
        batch: &mut Vec<ingestion_domain::Tick>,
    ) -> Result<(), IngestionError> {
        let count = batch.len();
        info!("Flushing {} ticks to repository", count);

        self.repository
            .save_batch(batch.clone())
            .await
            .map_err(IngestionError::RepositoryError)?;

        batch.clear();
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Gateway error: {0}")]
    GatewayError(#[from] crate::ports::GatewayError),

    #[error("Repository error: {0}")]
    RepositoryError(#[from] crate::ports::RepositoryError),
}
