use crate::redis_connection::RedisConnection;
use async_trait::async_trait;
use ingestion_application::rate_limiter::{RateLimiter, RateLimiterError};
use lazy_static::lazy_static;
use redis::Script;
use shaku::Component;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

lazy_static! {
    static ref LUA_SCRIPT: Script = {
        const SCRIPT_SOURCE: &str = include_str!("rate_limiter.lua");
        Script::new(SCRIPT_SOURCE)
    };
}

#[derive(Clone)]
pub struct RateLimitWindow {
    pub limit: usize,
    pub duration_secs: u64,
}

impl RateLimitWindow {
    pub const fn new(limit: usize, duration_secs: u64) -> Self {
        Self { limit, duration_secs }
    }
}

#[derive(Clone)]
pub struct IbRateLimiterConfig {
    /// IB account id namespace.
    pub account_id: String,
    /// 60 requests per 10-minute rolling window.
    pub ten_minute_window: RateLimitWindow,
    /// 6 requests per 2-second rolling window for the same contract/exchange/tick type.
    pub contract_window: RateLimitWindow,
    /// Prevent identical requests within 15 seconds.
    pub duplicate_request_window: RateLimitWindow,
}

impl Default for IbRateLimiterConfig {
    fn default() -> Self {
        Self {
            account_id: String::from("U12345"),
            ten_minute_window: RateLimitWindow::new(60, 600),
            contract_window: RateLimitWindow::new(6, 2),
            duplicate_request_window: RateLimitWindow::new(1, 15),
        }
    }
}

#[derive(Component)]
#[shaku(interface = RateLimiter)]
pub struct IbRateLimiter {
    #[shaku(inject)]
    redis_client: Arc<dyn RedisConnection>,

    #[shaku(default = IbRateLimiterConfig::default())]
    config: IbRateLimiterConfig,
}

#[async_trait]
impl RateLimiter for IbRateLimiter {
    async fn acquire(&self) -> Result<(), RateLimiterError> {
        // Get a connection from the provider.
        let mut conn = self
            .redis_client
            .get_connection()
            .await
            .map_err(|e| RateLimiterError::ConnectionError(e.to_string()))?;

        let account_id = &self.config.account_id;
        let windows = [
            &self.config.ten_minute_window,
            &self.config.contract_window,
            &self.config.duplicate_request_window,
        ];
        let window_keys = windows.map(|window| {
            format!(
                "rate_limit:ib:historical:{}:{}s",
                account_id, window.duration_secs
            )
        });

        loop {
            let request_id = Uuid::new_v4().to_string();
            let mut script_invocation = LUA_SCRIPT.prepare_invoke();

            for key in &window_keys {
                script_invocation.key(key);
            }

            for window in &windows {
                script_invocation.arg(window.limit);
                script_invocation.arg(window.duration_secs);
            }

            script_invocation.arg(&request_id);

            let result: Result<i32, _> = script_invocation.invoke_async(&mut conn).await;

            match result {
                Ok(1) => {
                    // Allowed
                    return Ok(());
                }
                Ok(0) => {
                    // Denied, wait and retry
                    warn!("Rate limit hit. Retrying shortly...");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Ok(_) => {
                    // Should not happen
                    return Err(RateLimiterError::Unexpected(
                        "Lua script returned an unexpected value.".to_string(),
                    ));
                }
                Err(e) => {
                    return Err(RateLimiterError::ScriptError(e.to_string()));
                }
            }
        }
    }
}
