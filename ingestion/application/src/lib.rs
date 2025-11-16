pub mod ports;
pub mod services;

pub use ports::{MarketDataGateway, TickRepository};
pub use services::IngestionServiceImpl;