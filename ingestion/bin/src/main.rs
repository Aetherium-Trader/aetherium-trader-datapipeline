mod di;

use crate::di::create_app_module;
use ingestion_application::services::IngestionService;
use ingestion_application::TickRepository;
use shaku::HasComponent;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Aetherium Trader - Ingestion Service");

    let module = create_app_module();
    let service: Arc<dyn IngestionService> = module.resolve();
    let repository: Arc<dyn TickRepository> = module.resolve();

    info!("Starting data ingestion for NQ futures (Press Ctrl+C to stop)");

    tokio::select! {
        result = service.run("NQ") => {
            if let Err(e) = result {
                eprintln!("Service error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, stopping gracefully...");
        }
    }

    repository.shutdown().await?;
    info!("Shutdown complete");

    Ok(())
}
