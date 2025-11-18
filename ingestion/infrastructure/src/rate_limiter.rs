use async_trait::async_trait;
use ingestion_application::RateLimiter;
use shaku::Component;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Component)]
#[shaku(interface = RateLimiter)]
pub struct IbRateLimiter {
    min_interval_secs: u64,
    max_requests_per_short_window: usize,
    short_window_secs: u64,
    max_requests_per_long_window: usize,
    long_window_secs: u64,
    #[shaku(default)]
    last_request: Mutex<Option<Instant>>,
    #[shaku(default)]
    request_history: Mutex<VecDeque<Instant>>,
}

impl IbRateLimiter {
    fn min_interval(&self) -> Duration {
        Duration::from_secs(self.min_interval_secs)
    }

    fn short_window_duration(&self) -> Duration {
        Duration::from_secs(self.short_window_secs)
    }

    fn long_window_duration(&self) -> Duration {
        Duration::from_secs(self.long_window_secs)
    }
}

#[async_trait]
impl RateLimiter for IbRateLimiter {
    async fn acquire(&self) {
        loop {
            let now = Instant::now();
            let mut last_request = self.last_request.lock().await;
            let mut history = self.request_history.lock().await;

            history.retain(|&t| now.duration_since(t) < self.long_window_duration());

            if let Some(last) = *last_request {
                let elapsed = now.duration_since(last);
                if elapsed < self.min_interval() {
                    let wait_duration = self.min_interval() - elapsed;
                    drop(last_request);
                    drop(history);
                    tokio::time::sleep(wait_duration).await;
                    continue;
                }
            }

            let short_window_start = now - self.short_window_duration();
            let short_window_count = history.iter().filter(|&&t| t >= short_window_start).count();

            if short_window_count >= self.max_requests_per_short_window {
                drop(last_request);
                drop(history);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            if history.len() >= self.max_requests_per_long_window {
                drop(last_request);
                drop(history);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            *last_request = Some(now);
            history.push_back(now);
            return;
        }
    }
}
