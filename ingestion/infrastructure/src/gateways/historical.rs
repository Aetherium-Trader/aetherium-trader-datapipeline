use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use ingestion_application::{HistoricalDataError, HistoricalDataGateway, RateLimiter};
use ingestion_domain::Tick;
use rust_decimal::Decimal;
use shaku::Component;
use std::sync::Arc;

#[derive(Component)]
#[shaku(interface = HistoricalDataGateway)]
pub struct MockHistoricalDataGateway {
    base_price: f64,
    max_history_days: u32,
    #[shaku(inject)]
    rate_limiter: Arc<dyn RateLimiter>,
}

impl MockHistoricalDataGateway {
    fn generate_tick(&self, symbol: &str, timestamp: DateTime<Utc>) -> Tick {
        let price_offset = (timestamp.timestamp() % 100) as f64;
        let base = Decimal::try_from(self.base_price).unwrap();
        let offset = Decimal::try_from(price_offset).unwrap();

        let last_price = base + offset;
        let spread = Decimal::try_from(0.25).unwrap();
        let bid_price = last_price - spread / Decimal::try_from(2.0).unwrap();
        let ask_price = last_price + spread / Decimal::try_from(2.0).unwrap();

        let bid_size = 10;
        let ask_size = 15;
        let last_size = 5;

        Tick::new(
            timestamp,
            symbol.to_string(),
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            last_price,
            last_size,
        )
        .expect("Generated tick should be valid")
    }
}

#[async_trait]
impl HistoricalDataGateway for MockHistoricalDataGateway {
    async fn fetch_historical_ticks(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Tick>, HistoricalDataError> {
        let days_ago = (Utc::now().date_naive() - date).num_days();
        if days_ago > self.max_history_days as i64 {
            return Err(HistoricalDataError::DataNotAvailable(date));
        }

        self.rate_limiter
            .acquire()
            .await
            .expect("Rate limiter acquired");

        let start_time = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
        let start_datetime = date.and_time(start_time);
        let start_utc = Utc.from_utc_datetime(&start_datetime);

        let mut ticks = Vec::new();
        for minute in 0..(24 * 60) {
            let timestamp = start_utc + Duration::minutes(minute);
            ticks.push(self.generate_tick(symbol, timestamp));
        }

        Ok(ticks)
    }

    fn max_history_days(&self) -> u32 {
        self.max_history_days
    }
}
