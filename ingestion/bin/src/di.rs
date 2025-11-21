use ingestion_application::backfill_service::BackfillServiceImplParameters;
use ingestion_application::services::IngestionServiceImplParameters;
use ingestion_application::{BackfillServiceImpl, IngestionServiceImpl};
use ingestion_infrastructure::detectors::gap::ParquetGapDetectorParameters;
use ingestion_infrastructure::gateways::historical::MockHistoricalDataGatewayParameters;
use ingestion_infrastructure::gateways::market_data::MockMarketDataGatewayParameters;
use ingestion_infrastructure::rate_limiting::redis::RedisConnectionManager;
use ingestion_infrastructure::repositories::parquet::ParquetTickRepositoryParameters;
use ingestion_infrastructure::{
    IbRateLimiter, MockHistoricalDataGateway, MockMarketDataGateway, ParquetGapDetector,
    ParquetTickRepository, RedisJobStateRepository,
};
use shaku::module;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

module! {
    pub AppModule {
        components = [
            IngestionServiceImpl,
            MockMarketDataGateway,
            ParquetTickRepository,
            IbRateLimiter,
            MockHistoricalDataGateway,
            ParquetGapDetector,
            BackfillServiceImpl,
            RedisConnectionManager,
            RedisJobStateRepository
        ],
        providers = []
    }
}

pub fn create_app_module() -> AppModule {
    let output_dir = Path::new("./data/").to_path_buf();
    std::fs::create_dir_all(&output_dir).expect("Failed to create output directory");
    AppModule::builder()
        .with_component_parameters::<IngestionServiceImpl>(IngestionServiceImplParameters {
            batch_size: 1000,
            flush_interval: Duration::from_secs(5),
        })
        .with_component_parameters::<MockMarketDataGateway>(MockMarketDataGatewayParameters {
            tick_interval: Duration::from_millis(100),
            base_price: 16000.0,
        })
        .with_component_parameters::<ParquetTickRepository>(ParquetTickRepositoryParameters {
            output_dir: output_dir.clone(),
            writer: Arc::new(Mutex::new(None)),
            current_hour: Arc::new(Mutex::new(None)),
        })
        .with_component_parameters::<MockHistoricalDataGateway>(
            MockHistoricalDataGatewayParameters {
                base_price: 16000.0,
                max_history_days: 365,
            },
        )
        .with_component_parameters::<ParquetGapDetector>(ParquetGapDetectorParameters {
            data_dir: output_dir,
        })
        .with_component_parameters::<BackfillServiceImpl>(BackfillServiceImplParameters {})
        .build()
}
