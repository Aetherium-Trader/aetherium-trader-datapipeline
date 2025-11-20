use async_trait::async_trait;
use chrono::Utc;
use futures::stream;
use ingestion_application::ports::{GatewayError, MarketDataGateway, TickStream};
use ingestion_domain::Tick;
use rand::Rng;
use rust_decimal::Decimal;
use shaku::Component;
use std::time::Duration;
use tracing::info;

#[derive(Component)]
#[shaku(interface = MarketDataGateway)]
pub struct MockMarketDataGateway {
    tick_interval: Duration,
    base_price: f64,
}

impl MockMarketDataGateway {
    pub fn new(tick_interval: Duration, base_price: f64) -> Self {
        Self {
            tick_interval,
            base_price,
        }
    }

    fn generate_tick(&self, symbol: &str) -> Tick {
        let mut rng = rand::rng();

        let price_change = rng.random_range(-2.0..2.0);
        let last_price = self.base_price + price_change;

        let spread = 0.25;
        let bid_price = last_price - spread / 2.0;
        let ask_price = last_price + spread / 2.0;

        let bid_size = rng.random_range(1..50);
        let ask_size = rng.random_range(1..50);
        let last_size = rng.random_range(1..20);

        Tick::new(
            Utc::now(),
            symbol.to_string(),
            Decimal::from_f64_retain(bid_price).unwrap(),
            bid_size,
            Decimal::from_f64_retain(ask_price).unwrap(),
            ask_size,
            Decimal::from_f64_retain(last_price).unwrap(),
            last_size,
        )
        .expect("Generated tick should always be valid")
    }
}

#[async_trait]
impl MarketDataGateway for MockMarketDataGateway {
    async fn subscribe(&self, symbol: &str) -> Result<TickStream, GatewayError> {
        info!("Mock gateway: Subscribing to symbol {}", symbol);

        let symbol = symbol.to_string();
        let tick_interval = self.tick_interval;
        let base_price = self.base_price;

        // 建立一個無限 stream，定期產生 Tick
        let stream = stream::unfold((), move |_| {
            let symbol = symbol.clone();
            let gateway = MockMarketDataGateway::new(tick_interval, base_price);

            async move {
                tokio::time::sleep(tick_interval).await;
                let tick = gateway.generate_tick(&symbol);
                Some((Ok(tick), ()))
            }
        });

        Ok(Box::new(Box::pin(stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_mock_gateway_generates_ticks() {
        let gateway = MockMarketDataGateway::new(Duration::from_millis(10), 16000.0);

        let mut stream = gateway.subscribe("NQ").await.unwrap();

        // 取得 3 個 Tick
        for _ in 0..3 {
            let tick = stream.next().await.unwrap().unwrap();
            assert_eq!(tick.symbol(), "NQ");
            assert!(tick.last_price() > Decimal::ZERO);
        }
    }
}
