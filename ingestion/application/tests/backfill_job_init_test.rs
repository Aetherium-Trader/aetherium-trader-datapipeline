use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration, NaiveDate, Utc};
use ingestion_application::ports::RepositoryError;
use ingestion_application::{
    BackfillError, BackfillService, BackfillServiceImpl, GapDetectionError, GapDetector,
    HistoricalDataError, HistoricalDataGateway, JobState, JobStateError, JobStateRepository,
    JobStatus, TickRepository,
};
use ingestion_domain::{DateRange, Tick};
use tokio::sync::Mutex;

#[tokio::test]
async fn stale_job_takeover_preserves_cursor() {
    let job_key = job_key("ES", day(1));
    let cursor = timestamp_for(day(2), 23, 59);
    let stale_state = JobState {
        status: JobStatus::Running,
        job_instance_id: "old-instance".to_string(),
        cursor,
        end_time: timestamp_for(day(3), 0, 0),
        heartbeat_at: Utc::now() - Duration::seconds(600),
        critical_ranges: Vec::new(),
        last_error_type: None,
    };
    let repo = Arc::new(StubJobStateRepository::new(
        job_key.clone(),
        Some(stale_state.clone()),
    ));
    let service = build_service(repo.clone());

    let range = DateRange::new(day(1), day(1)).unwrap();
    service
        .backfill_range("ES", range)
        .await
        .expect("stale job should be taken over");

    let final_state = repo.snapshot().await.expect("state present");
    assert_ne!(final_state.job_instance_id, stale_state.job_instance_id);
    assert_eq!(final_state.cursor, stale_state.cursor);
    assert_eq!(final_state.status, JobStatus::Completed);
}

#[tokio::test]
async fn active_job_returns_error() {
    let job_key = job_key("NQ", day(1));
    let fresh_state = JobState {
        status: JobStatus::Running,
        job_instance_id: "running".to_string(),
        cursor: timestamp_for(day(1), 12, 0),
        end_time: timestamp_for(day(1), 23, 59),
        heartbeat_at: Utc::now(),
        critical_ranges: Vec::new(),
        last_error_type: None,
    };
    let repo = Arc::new(StubJobStateRepository::new(
        job_key.clone(),
        Some(fresh_state),
    ));
    let service = build_service(repo.clone());

    let range = DateRange::new(day(1), day(1)).unwrap();
    let err = service
        .backfill_range("NQ", range)
        .await
        .expect_err("should reject active job");
    match err {
        BackfillError::JobAlreadyRunning(key) => assert_eq!(key, job_key),
        other => panic!("unexpected error: {other:?}"),
    }

    let final_state = repo.snapshot().await.expect("state present");
    assert_eq!(final_state.job_instance_id, "running");
}

fn build_service(repo: Arc<StubJobStateRepository>) -> Arc<dyn BackfillService> {
    let gateway = Arc::new(NoopHistoricalGateway);
    let gap_detector = Arc::new(NoopGapDetector);
    let repository = Arc::new(NoopTickRepository);
    Arc::new(BackfillServiceImpl::new(
        gateway,
        gap_detector,
        repository,
        repo,
    ))
}

fn day(d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2025, 1, d).unwrap()
}

fn job_key(symbol: &str, start: NaiveDate) -> String {
    format!("ingest:job:{}:{}", symbol, start)
}

fn timestamp_for(date: NaiveDate, hour: u32, minute: u32) -> i64 {
    date.and_hms_opt(hour, minute, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis()
}

struct NoopHistoricalGateway;

#[async_trait]
impl HistoricalDataGateway for NoopHistoricalGateway {
    async fn fetch_historical_ticks(
        &self,
        _symbol: &str,
        _date: NaiveDate,
    ) -> Result<Vec<Tick>, HistoricalDataError> {
        Ok(Vec::new())
    }

    fn max_history_days(&self) -> u32 {
        365
    }
}

struct NoopGapDetector;

#[async_trait]
impl GapDetector for NoopGapDetector {
    async fn detect_gaps(
        &self,
        _symbol: &str,
        _range: DateRange,
    ) -> Result<Vec<DateRange>, GapDetectionError> {
        Ok(Vec::new())
    }
}

struct NoopTickRepository;

#[async_trait]
impl TickRepository for NoopTickRepository {
    async fn save_batch(&self, _ticks: Vec<Tick>) -> Result<(), RepositoryError> {
        Ok(())
    }

    async fn flush(&self) -> Result<(), RepositoryError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RepositoryError> {
        Ok(())
    }
}

struct StubJobStateRepository {
    key: String,
    state: Mutex<Option<JobState>>,
}

impl StubJobStateRepository {
    fn new(key: String, state: Option<JobState>) -> Self {
        Self {
            key,
            state: Mutex::new(state),
        }
    }

    async fn snapshot(&self) -> Option<JobState> {
        self.state.lock().await.clone()
    }

    async fn with_mut<F, R>(&self, job_instance_id: &str, mut f: F) -> Result<R, JobStateError>
    where
        F: FnMut(&mut JobState) -> R,
    {
        let mut guard = self.state.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| JobStateError::NotFound(self.key.clone()))?;
        if state.job_instance_id != job_instance_id {
            return Err(JobStateError::StaleInstance(self.key.clone()));
        }
        Ok(f(state))
    }
}

#[async_trait]
impl JobStateRepository for StubJobStateRepository {
    async fn get(&self, job_key: &str) -> Result<Option<JobState>, JobStateError> {
        if job_key != self.key {
            return Ok(None);
        }
        Ok(self.state.lock().await.clone())
    }

    async fn upsert(&self, job_key: &str, state: &JobState) -> Result<(), JobStateError> {
        if job_key == self.key {
            *self.state.lock().await = Some(state.clone());
        }
        Ok(())
    }

    async fn update_cursor(
        &self,
        _job_key: &str,
        job_instance_id: &String,
        cursor: i64,
    ) -> Result<(), JobStateError> {
        self.with_mut(job_instance_id, |state| state.cursor = cursor)
            .await
    }

    async fn update_status(
        &self,
        _job_key: &str,
        job_instance_id: &String,
        status: JobStatus,
    ) -> Result<(), JobStateError> {
        self.with_mut(job_instance_id, |state| state.status = status.clone())
            .await
    }

    async fn heartbeat(
        &self,
        _job_key: &str,
        job_instance_id: &String,
        heartbeat_at: chrono::DateTime<Utc>,
    ) -> Result<(), JobStateError> {
        self.with_mut(job_instance_id, |state| state.heartbeat_at = heartbeat_at)
            .await
    }

    async fn save_error(
        &self,
        _job_key: &str,
        job_instance_id: &String,
        message: &str,
    ) -> Result<(), JobStateError> {
        self.with_mut(job_instance_id, |state| {
            state.last_error_type = Some(message.to_string())
        })
        .await
    }
}
