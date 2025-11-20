use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{Client as RedisClient, RedisResult};
use shaku::{Component, Interface};

#[async_trait]
pub trait RedisConnection: Interface {
    async fn get_connection(&self) -> RedisResult<MultiplexedConnection>;
}

fn create_redis_client() -> RedisClient {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    RedisClient::open(redis_url.clone()).unwrap_or_else(|e| {
        panic!(
            "Failed to create Redis client for '{}': {}",
            sanitize_redis_url(&redis_url),
            e
        )
    })
}

#[derive(Component)]
#[shaku(interface = RedisConnection)]
pub struct RedisConnectionManager {
    #[shaku(default = create_redis_client())]
    client: RedisClient,
}

#[async_trait]
impl RedisConnection for RedisConnectionManager {
    async fn get_connection(&self) -> RedisResult<MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }
}

fn sanitize_redis_url(url: &str) -> String {
    url.split('@').last().unwrap_or(url).to_string()
}
