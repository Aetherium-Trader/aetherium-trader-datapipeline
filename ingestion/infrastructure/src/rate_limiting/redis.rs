use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{Client as RedisClient, RedisResult};
use shaku::{Component, Interface};

#[async_trait]
pub trait RedisConnection: Interface {
    async fn get_connection(&self) -> RedisResult<MultiplexedConnection>;
}

fn create_redis_client() -> RedisClient {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1".to_string());
    RedisClient::open(redis_url).expect("Failed to create Redis client")
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
