use ingestion_application::services::IngestionServiceImplParameters;
use ingestion_application::IngestionServiceImpl;
use ingestion_infrastructure::gateway::MockMarketDataGatewayParameters;
use ingestion_infrastructure::repository::ParquetTickRepositoryParameters;
use ingestion_infrastructure::{MockMarketDataGateway, ParquetTickRepository};
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
            ParquetTickRepository],
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
            output_dir,
            writer: Arc::new(Mutex::new(None)),
            current_hour: Arc::new(Mutex::new(None)),
        })
        .build()
}
