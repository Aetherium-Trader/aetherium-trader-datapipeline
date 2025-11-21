use chrono::Utc;
use ingestion_application::job_state::{
    JobInstanceId, JobState, JobStateError, JobStateRepository, JobStatus,
};
use ingestion_infrastructure::rate_limiting::redis::RedisConnectionManager;
use ingestion_infrastructure::state::RedisJobStateRepository;
use shaku::{module, HasComponent};
use std::env;
use std::sync::Arc;
use uuid::Uuid;

module! {
    TestModule {
        components = [
            RedisConnectionManager,
            RedisJobStateRepository,
        ],
        providers = []
    }
}

#[tokio::test]
async fn upsert_and_fetch_job_state() {
    let redis_url =
        env::var("REDIS_URL_TEST").unwrap_or_else(|_| "redis://127.0.0.1:6379/2".to_string());
    env::set_var("REDIS_URL", &redis_url);
    let module = TestModule::builder().build();

    let repo: Arc<dyn JobStateRepository> = module.resolve();
    let job_key = "ingest:job:NQ:2024-01-01".to_string();
    delete_key(&redis_url, &job_key).await;

    let state = sample_state();
    repo.upsert(&job_key, &state).await.expect("upsert");

    let fetched = repo.get(&job_key).await.expect("get");
    let fetched = fetched.expect("expected state");

    assert_eq!(fetched.status, JobStatus::Running);
    assert_eq!(fetched.cursor, state.cursor);
    assert_eq!(fetched.job_instance_id, state.job_instance_id);
    assert!(fetched.critical_ranges.is_empty());
    assert!(fetched.last_error_type.is_none());
}

#[tokio::test]
async fn update_cursor_enforces_instance_id() {
    let redis_url =
        env::var("REDIS_URL_TEST").unwrap_or_else(|_| "redis://127.0.0.1:6379/2".to_string());
    env::set_var("REDIS_URL", &redis_url);
    let module = TestModule::builder().build();

    let repo: Arc<dyn JobStateRepository> = module.resolve();
    let job_key = "ingest:job:ES:2024-02-02".to_string();
    delete_key(&redis_url, &job_key).await;

    let state = sample_state();
    repo.upsert(&job_key, &state).await.expect("upsert");

    repo.update_cursor(&job_key, &state.job_instance_id, 12345)
        .await
        .expect("cursor update");

    let updated = repo.get(&job_key).await.unwrap().unwrap();
    assert_eq!(updated.cursor, 12345);

    let stale_error = repo
        .update_cursor(&job_key, &stale_instance(), 999)
        .await
        .expect_err("stale instance must fail");
    assert!(matches!(stale_error, JobStateError::StaleInstance(_)));
}

#[tokio::test]
async fn stale_instance_cannot_overwrite_after_restart() {
    let redis_url =
        env::var("REDIS_URL_TEST").unwrap_or_else(|_| "redis://127.0.0.1:6379/2".to_string());
    env::set_var("REDIS_URL", &redis_url);
    let module = TestModule::builder().build();

    let repo: Arc<dyn JobStateRepository> = module.resolve();
    let job_key = "ingest:job:YM:2024-03-15".to_string();
    delete_key(&redis_url, &job_key).await;

    let original = sample_state();
    repo.upsert(&job_key, &original)
        .await
        .expect("upsert original");

    // Worker restart with new instance id
    let mut restarted = sample_state();
    restarted.job_instance_id = Uuid::new_v4().to_string();
    repo.upsert(&job_key, &restarted)
        .await
        .expect("upsert restart");

    // Old instance tries to update cursor -> must fail
    let err = repo
        .update_cursor(&job_key, &original.job_instance_id, 42)
        .await
        .expect_err("stale instance should be rejected");
    assert!(matches!(err, JobStateError::StaleInstance(_)));

    // New instance succeeds
    repo.update_cursor(&job_key, &restarted.job_instance_id, 42)
        .await
        .expect("new instance update");
}

fn sample_state() -> JobState {
    JobState::new(
        Uuid::new_v4().to_string(),
        JobStatus::Running,
        0,
        1_731_853_800_000,
        Utc::now(),
    )
}

async fn delete_key(redis_url: &str, job_key: &str) {
    let client = redis::Client::open(redis_url).expect("open redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect redis");
    let _: () = redis::cmd("DEL")
        .arg(job_key)
        .query_async(&mut conn)
        .await
        .expect("delete key");
}

fn stale_instance() -> JobInstanceId {
    format!("stale-{}", Uuid::new_v4())
}
