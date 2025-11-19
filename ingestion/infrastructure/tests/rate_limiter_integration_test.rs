use ingestion_application::rate_limiter::RateLimiter;
use ingestion_infrastructure::rate_limiter::{
    IbRateLimiter, IbRateLimiterConfig, IbRateLimiterParameters, RateLimitWindow,
};
use ingestion_infrastructure::redis_connection::RedisConnectionManager;
use shaku::{module, HasComponent};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

module! {
    TestModule {
        components = [
            RedisConnectionManager,
            IbRateLimiter,
        ],
        providers = []
    }
}

async fn setup_test_module(config: IbRateLimiterConfig) -> TestModule {
    let redis_url =
        env::var("REDIS_URL_TEST").unwrap_or_else(|_| "redis://127.0.0.1:6379/1".to_string());
    env::set_var("REDIS_URL", &redis_url);

    let module_builder =
        TestModule::builder().with_component_parameters::<IbRateLimiter>(IbRateLimiterParameters {
            config: config.clone(),
        });

    let module = module_builder.build();

    clear_rate_limit_keys(&redis_url, &config).await;

    module
}

fn windows(config: &IbRateLimiterConfig) -> [&RateLimitWindow; 3] {
    [
        &config.ten_minute_window,
        &config.contract_window,
        &config.duplicate_request_window,
    ]
}

async fn clear_rate_limit_keys(redis_url: &str, config: &IbRateLimiterConfig) {
    let redis_client = redis::Client::open(redis_url).expect("failed to open Redis client");
    let mut conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to acquire Redis connection");

    let mut del_cmd = redis::cmd("DEL");
    for window in windows(config) {
        del_cmd.arg(format!(
            "rate_limit:ib:historical:{}:{}s",
            config.account_id, window.duration_secs
        ));
    }

    let _: () = del_cmd
        .query_async(&mut conn)
        .await
        .expect("failed to delete rate limiter keys");
}

fn test_config(account_id: String) -> IbRateLimiterConfig {
    IbRateLimiterConfig {
        account_id,
        ten_minute_window: RateLimitWindow::new(20, 10),
        contract_window: RateLimitWindow::new(3, 2),
        duplicate_request_window: RateLimitWindow::new(2, 1),
    }
}

#[tokio::test]
async fn test_rate_limiter_allows_requests_within_limit() {
    let account_id = format!("test-allow-{}", Uuid::new_v4());
    let config = test_config(account_id);
    let module = setup_test_module(config).await;
    let limiter: Arc<dyn RateLimiter> = module.resolve();

    let start = Instant::now();
    limiter.acquire().await.unwrap();
    let duration = start.elapsed();
    assert!(
        duration < Duration::from_millis(50),
        "First request took too long: {:?}",
        duration
    );

    let start = Instant::now();
    limiter.acquire().await.unwrap();
    let duration = start.elapsed();
    assert!(
        duration < Duration::from_millis(50),
        "Second request took too long: {:?}",
        duration
    );
}

#[tokio::test]
async fn test_rate_limiter_blocks_requests_exceeding_short_limit() {
    let account_id = format!("test-block-{}", Uuid::new_v4());
    let config = IbRateLimiterConfig {
        duplicate_request_window: RateLimitWindow::new(2, 1),
        ..test_config(account_id)
    };
    let module = setup_test_module(config).await;
    let limiter: Arc<dyn RateLimiter> = module.resolve();

    let start = Instant::now();
    limiter.acquire().await.unwrap();
    limiter.acquire().await.unwrap();
    limiter.acquire().await.unwrap();
    let duration = start.elapsed();

    assert!(
        duration >= Duration::from_secs(1),
        "Third request should wait for short window to reset"
    );
    assert!(
        duration < Duration::from_millis(1600),
        "Third request waited too long: {:?}",
        duration
    );
}

#[tokio::test]
async fn test_rate_limiter_resets_after_window() {
    let account_id = format!("test-reset-{}", Uuid::new_v4());
    let config = IbRateLimiterConfig {
        duplicate_request_window: RateLimitWindow::new(2, 1),
        ..test_config(account_id)
    };
    let module = setup_test_module(config).await;
    let limiter: Arc<dyn RateLimiter> = module.resolve();

    limiter.acquire().await.unwrap();
    limiter.acquire().await.unwrap();

    sleep(Duration::from_millis(1_100)).await;

    let start = Instant::now();
    limiter.acquire().await.unwrap();
    let duration = start.elapsed();

    assert!(
        duration < Duration::from_millis(50),
        "Request after window reset took too long: {:?}",
        duration
    );
}

#[tokio::test]
async fn test_rate_limiter_respects_multiple_windows() {
    let account_id = format!("test-multi-window-{}", Uuid::new_v4());
    let config = IbRateLimiterConfig {
        ten_minute_window: RateLimitWindow::new(100, 60),
        contract_window: RateLimitWindow::new(3, 3),
        duplicate_request_window: RateLimitWindow::new(10, 1),
        ..test_config(account_id)
    };
    let module = setup_test_module(config).await;
    let limiter: Arc<dyn RateLimiter> = module.resolve();

    for _ in 0..3 {
        limiter.acquire().await.unwrap();
    }

    let start = Instant::now();
    limiter.acquire().await.unwrap();
    let duration = start.elapsed();
    assert!(
        duration >= Duration::from_secs(3),
        "Medium window should enforce ~3 second wait, but was {:?}",
        duration
    );
    assert!(
        duration < Duration::from_secs(5),
        "Medium window wait exceeded expectations: {:?}",
        duration
    );
}
