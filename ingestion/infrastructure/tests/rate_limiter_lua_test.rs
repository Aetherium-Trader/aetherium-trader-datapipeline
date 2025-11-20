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
    let windows = [(10, 60), (10, 10), (1, 1)];
    let keys = window_keys("lua-short", &windows);

    clear_keys(&mut conn, &keys).await;

    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 0);

    sleep(Duration::from_millis(1_100)).await;
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
}

#[tokio::test]
async fn lua_script_enforces_contract_window_limits() {
    let mut conn = redis_connection().await;
    let script = Script::new(LUA_SOURCE);
    let windows = [(100, 60), (3, 2), (10, 1)];
    let keys = window_keys("lua-contract", &windows);

    clear_keys(&mut conn, &keys).await;

    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 1);
    // Fourth request should block because the 3-per-2s window is saturated.
    assert_eq!(invoke(&script, &keys, &windows, &mut conn).await, 0);

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
    env::set_var("REDIS_URL", &redis_url);
    let redis_client =
        redis::Client::open(redis_url).expect("failed to open redis client for lua test");
    redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to acquire redis connection for lua test")
}

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
