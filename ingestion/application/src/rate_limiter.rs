use async_trait::async_trait;
use shaku::Interface;

#[async_trait]
pub trait RateLimiter: Interface {
    async fn acquire(&self);
}