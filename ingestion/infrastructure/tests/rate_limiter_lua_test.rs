use redis::aio::MultiplexedConnection;
use redis::Script;
use std::env;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

const LUA_SOURCE: &str = include_str!("../src/rate_limiting/limiter.lua");

#[tokio::test]
async fn lua_script_blocks_requests_within_short_window() {
    let mut conn = redis_connection().await;
    let script = Script::new(LUA_SOURCE);
    // (limit, duration_secs): 10 req/60s, 10 req/10s, 1 req/1s (short window focus)
    let windows = [(10, 60), (10, 10), (1, 1)];
    let account_id = format!("test-lua-short-{}", Uuid::new_v4());
    let keys = window_keys(&account_id, &windows);

    clear_keys(&mut conn, &keys).await;

    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 0);

    // Wait for 1s window + 100ms buffer to ensure expiry.
    sleep(Duration::from_millis(1_100)).await;
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
}

#[tokio::test]
async fn lua_script_enforces_contract_window_limits() {
    let mut conn = redis_connection().await;
    let script = Script::new(LUA_SOURCE);
    // (limit, duration_secs): 100 req/60s, 3 req/2s, 10 req/1s (contract window focus)
    let windows = [(100, 60), (3, 2), (10, 1)];
    let account_id = format!("test-lua-contract-{}", Uuid::new_v4());
    let keys = window_keys(&account_id, &windows);

    clear_keys(&mut conn, &keys).await;

    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    // Fourth request should block because the 3-per-2s window is saturated.
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 0);

    // Wait for 2s window + 200ms buffer.
    sleep(Duration::from_millis(2_200)).await;
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
}

async fn clear_keys(conn: &mut MultiplexedConnection, keys: &[String; 3]) {
    let mut cmd = redis::cmd("DEL");
    for key in keys {
        cmd.arg(key);
    }
    let _: () = cmd
        .query_async(conn)
        .await
        .expect("failed to clear rate limiter keys");
}

async fn redis_connection() -> MultiplexedConnection {
    let redis_url =
        env::var("REDIS_URL_TEST").unwrap_or_else(|_| "redis://127.0.0.1:6379/1".to_string());
    let redis_client = redis::Client::open(redis_url.clone()).unwrap_or_else(|e| {
        panic!(
            "failed to open redis client for lua test at {}: {}",
            redis_url, e
        )
    });
    redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to acquire redis connection for lua test")
}

/// Generates keys in the form `rate_limit:lua_test:{account_id}:{duration}s`.
fn window_keys(account_id: &str, windows: &[(usize, u64); 3]) -> [String; 3] {
    std::array::from_fn(|idx| format!("rate_limit:lua_test:{}:{}s", account_id, windows[idx].1))
}

async fn invoke(
    script: &Script,
    keys: &[String; 3],
    windows: &[(usize, u64); 3],
    conn: &mut MultiplexedConnection,
) -> i32 {
    let request_id = Uuid::new_v4().to_string();
    let mut invocation = script.prepare_invoke();

    for key in keys {
        invocation.key(key);
    }
    for (limit, duration) in windows {
        invocation.arg(*limit);
        invocation.arg(*duration);
    }
    invocation.arg(&request_id);

    invocation
        .invoke_async(conn)
        .await
        .expect("lua script invocation failed")
}
