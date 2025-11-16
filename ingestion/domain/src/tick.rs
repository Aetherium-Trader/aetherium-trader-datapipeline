use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tick {
    timestamp: DateTime<Utc>,
    symbol: String,
    bid_price: Decimal,
    bid_size: u32,
    ask_price: Decimal,
    ask_size: u32,
    last_price: Decimal,
    last_size: u32,
}

impl Tick {
    pub fn new(
        timestamp: DateTime<Utc>,
        symbol: String,
        bid_price: Decimal,
        bid_size: u32,
        ask_price: Decimal,
        ask_size: u32,
        last_price: Decimal,
        last_size: u32,
    ) -> Result<Self, TickValidationError> {
        if symbol.is_empty() {
            return Err(TickValidationError::EmptySymbol);
        }

        if bid_price <= Decimal::ZERO {
            return Err(TickValidationError::InvalidPrice(
                "bid_price must be positive",
            ));
        }

        if ask_price <= Decimal::ZERO {
            return Err(TickValidationError::InvalidPrice(
                "ask_price must be positive",
            ));
        }

        if last_price <= Decimal::ZERO {
            return Err(TickValidationError::InvalidPrice(
                "last_price must be positive",
            ));
        }

        Ok(Self {
            timestamp,
            symbol,
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            last_price,
            last_size,
        })
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn bid_price(&self) -> Decimal {
        self.bid_price
    }

    pub fn bid_size(&self) -> u32 {
        self.bid_size
    }

    pub fn ask_price(&self) -> Decimal {
        self.ask_price
    }

    pub fn ask_size(&self) -> u32 {
        self.ask_size
    }

    pub fn last_price(&self) -> Decimal {
        self.last_price
    }

    pub fn last_size(&self) -> u32 {
        self.last_size
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TickValidationError {
    #[error("Symbol cannot be empty")]
    EmptySymbol,
    #[error("Invalid price: {0}")]
    InvalidPrice(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_valid_tick_creation() {
        let tick = Tick::new(
            Utc::now(),
            "NQ".to_string(),
            dec!(16000.25),
            10,
            dec!(16000.50),
            15,
            dec!(16000.25),
            5,
        );

        assert!(tick.is_ok());
    }

    #[test]
    fn test_empty_symbol_rejected() {
        let result = Tick::new(
            Utc::now(),
            String::new(),
            dec!(16000.25),
            10,
            dec!(16000.50),
            15,
            dec!(16000.25),
            5,
        );

        assert!(matches!(result, Err(TickValidationError::EmptySymbol)));
    }

    #[test]
    fn test_negative_price_rejected() {
        let result = Tick::new(
            Utc::now(),
            "NQ".to_string(),
            dec!(-100.0),
            10,
            dec!(16000.50),
            15,
            dec!(16000.25),
            5,
        );

        assert!(matches!(result, Err(TickValidationError::InvalidPrice(_))));
    }

    #[test]
    fn test_zero_price_rejected() {
        let result = Tick::new(
            Utc::now(),
            "NQ".to_string(),
            dec!(0.0),
            10,
            dec!(16000.50),
            15,
            dec!(16000.25),
            5,
        );

        assert!(matches!(result, Err(TickValidationError::InvalidPrice(_))));
    }
}
