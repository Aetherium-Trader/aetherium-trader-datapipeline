use ingestion_application::TickRepository;
use shaku::HasComponent;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[path = "../di.rs"]
mod di;

use crate::di::create_app_module;
use ingestion_application::services::IngestionService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Ingestion Test (will stop after 15 seconds)");

    let module = create_app_module();
    let service: Arc<dyn IngestionService> = module.resolve();
    let repository: Arc<dyn TickRepository> = module.resolve();

    tokio::select! {
        result = service.run("NQ") => {
            if let Err(e) = result {
                eprintln!("Service error: {}", e);
            }
        }
        _ = tokio::time::sleep(Duration::from_secs(15)) => {
            info!("Test duration reached, stopping...");
        }
    }

    repository.shutdown().await?;
    info!("Test complete - check ./data/ for Parquet files");

    Ok(())
}
