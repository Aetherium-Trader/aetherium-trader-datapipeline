use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use ingestion_application::ports::RepositoryError;
use ingestion_application::{
    BackfillService, BackfillServiceImpl, GapDetectionError, GapDetector, HistoricalDataError,
    HistoricalDataGateway, JobState, JobStateError, JobStateRepository, JobStatus, TickRepository,
};
use ingestion_domain::{DateRange, Tick};
use rust_decimal::Decimal;
use tokio::sync::{Mutex, MutexGuard};

#[tokio::test]
async fn resumes_current_day_even_without_gap() {
    let job_repo = Arc::new(InMemoryJobStateRepository::new());
    let job_key = job_key("ES", day(1));
    let cursor = timestamp_for(day(1), 12, 0);
    job_repo
        .insert_state(
            job_key.clone(),
            JobState::new(
                "job-1".to_string(),
                JobStatus::Running,
                cursor,
                end_of_day(day(2)),
                Utc::now() - chrono::Duration::seconds(600),
            ),
        )
        .await;

    let repository = Arc::new(RecordingTickRepository::default());
    let service = build_service(
        vec![(day(1), sample_ticks("ES", day(1), 2))],
        vec![],
        repository.clone(),
        job_repo.clone(),
    );

    let range = DateRange::new(day(1), day(2)).unwrap();
    let report = service.backfill_range("ES", range).await.unwrap();

    assert_eq!(report.days_processed, 1);
    assert_eq!(report.total_ticks, 2);
    assert_eq!(repository.saved_days().await, vec![day(1)]);
    assert!(repository.shutdown_called());

    let final_state = job_repo.snapshot(&job_key).await.unwrap();
    assert_eq!(final_state.status, JobStatus::Completed);
    assert!(final_state.cursor >= timestamp_for(day(1), 11, 0));
}

#[tokio::test]
async fn processes_gap_days_and_updates_job_state() {
    let job_repo = Arc::new(InMemoryJobStateRepository::new());
    let repository = Arc::new(RecordingTickRepository::default());
    let gap_range = DateRange::new(day(1), day(2)).unwrap();
    let service = build_service(
        vec![
            (day(1), sample_ticks("NQ", day(1), 3)),
            (day(2), sample_ticks("NQ", day(2), 1)),
        ],
        vec![gap_range],
        repository.clone(),
        job_repo.clone(),
    );

    let range = DateRange::new(day(1), day(2)).unwrap();
    let report = service.backfill_range("NQ", range).await.unwrap();

    assert_eq!(report.days_processed, 2);
    assert_eq!(report.total_ticks, 4);
    assert_eq!(repository.saved_days().await, vec![day(1), day(2)]);

    let job_key = job_key("NQ", day(1));
    let state = job_repo.snapshot(&job_key).await.unwrap();
    assert_eq!(state.status, JobStatus::Completed);
    assert!(state.cursor >= timestamp_for(day(2), 10, 0));
    assert!(!state.job_instance_id.is_empty());
}

fn build_service(
    ticks: Vec<(NaiveDate, Vec<Tick>)>,
    gaps: Vec<DateRange>,
    repository: Arc<RecordingTickRepository>,
    job_repo: Arc<InMemoryJobStateRepository>,
) -> Arc<dyn BackfillService> {
    let gateway = Arc::new(StubHistoricalGateway::new(ticks));
    let gap_detector = Arc::new(StubGapDetector::new(gaps));
    let service: Arc<dyn BackfillService> = Arc::new(BackfillServiceImpl::new(
        gateway,
        gap_detector,
        repository,
        job_repo,
    ));
    service
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

fn end_of_day(date: NaiveDate) -> i64 {
    date.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap())
        .and_utc()
        .timestamp_millis()
}

fn sample_ticks(symbol: &str, date: NaiveDate, count: usize) -> Vec<Tick> {
    (0..count)
        .map(|idx| make_tick(symbol, date, 10 + idx as u32))
        .collect()
}

fn make_tick(symbol: &str, date: NaiveDate, hour: u32) -> Tick {
    let timestamp = date.and_hms_opt(hour, 0, 0).unwrap();
    Tick::new(
        Utc.from_utc_datetime(&timestamp),
        symbol.to_string(),
        Decimal::new(100_000, 2),
        1,
        Decimal::new(100_500, 2),
        1,
        Decimal::new(100_250, 2),
        1,
    )
    .unwrap()
}

struct StubHistoricalGateway {
    ticks: HashMap<NaiveDate, Vec<Tick>>,
}

impl StubHistoricalGateway {
    fn new(entries: Vec<(NaiveDate, Vec<Tick>)>) -> Self {
        Self {
            ticks: entries.into_iter().collect(),
        }
    }
}

#[async_trait]
impl HistoricalDataGateway for StubHistoricalGateway {
    async fn fetch_historical_ticks(
        &self,
        _symbol: &str,
        date: NaiveDate,
    ) -> Result<Vec<Tick>, HistoricalDataError> {
        Ok(self.ticks.get(&date).cloned().unwrap_or_default())
    }

    fn max_history_days(&self) -> u32 {
        365
    }
}

struct StubGapDetector {
    gaps: Vec<DateRange>,
}

impl StubGapDetector {
    fn new(gaps: Vec<DateRange>) -> Self {
        Self { gaps }
    }
}

#[async_trait]
impl GapDetector for StubGapDetector {
    async fn detect_gaps(
        &self,
        _symbol: &str,
        _range: DateRange,
    ) -> Result<Vec<DateRange>, GapDetectionError> {
        Ok(self.gaps.clone())
    }
}

#[derive(Default)]
struct RecordingTickRepository {
    saved_days: Mutex<Vec<NaiveDate>>,
    shutdown_called: AtomicBool,
}

#[async_trait]
impl TickRepository for RecordingTickRepository {
    async fn save_batch(&self, ticks: Vec<Tick>) -> Result<(), RepositoryError> {
        if let Some(first) = ticks.first() {
            self.saved_days
                .lock()
                .await
                .push(first.timestamp().date_naive());
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), RepositoryError> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), RepositoryError> {
        self.shutdown_called.store(true, Ordering::Relaxed);
        Ok(())
    }
}

impl RecordingTickRepository {
    async fn saved_days(&self) -> Vec<NaiveDate> {
        self.saved_days.lock().await.clone()
    }

    fn shutdown_called(&self) -> bool {
        self.shutdown_called.load(Ordering::Relaxed)
    }
}

struct InMemoryJobStateRepository {
    states: Mutex<HashMap<String, JobState>>,
}

impl InMemoryJobStateRepository {
    fn new() -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
        }
    }

    async fn insert_state(&self, key: String, state: JobState) {
        self.states.lock().await.insert(key, state);
    }

    async fn snapshot(&self, key: &str) -> Option<JobState> {
        self.states.lock().await.get(key).cloned()
    }

    async fn require_state<'a>(
        &'a self,
        key: &str,
    ) -> Result<MutexGuard<'a, HashMap<String, JobState>>, JobStateError> {
        let guard = self.states.lock().await;
        if !guard.contains_key(key) {
            return Err(JobStateError::NotFound(key.to_string()));
        }
        Ok(guard)
    }
}

#[async_trait]
impl JobStateRepository for InMemoryJobStateRepository {
    async fn get(&self, job_key: &str) -> Result<Option<JobState>, JobStateError> {
        Ok(self.states.lock().await.get(job_key).cloned())
    }

    async fn upsert(&self, job_key: &str, state: &JobState) -> Result<(), JobStateError> {
        self.states
            .lock()
            .await
            .insert(job_key.to_string(), state.clone());
        Ok(())
    }

    async fn update_cursor(
        &self,
        job_key: &str,
        job_instance_id: &String,
        cursor: i64,
    ) -> Result<(), JobStateError> {
        let mut states = self.require_state(job_key).await?;
        let entry = states.get_mut(job_key).unwrap();
        if &entry.job_instance_id != job_instance_id {
            return Err(JobStateError::StaleInstance(job_key.to_string()));
        }
        entry.cursor = cursor;
        Ok(())
    }

    async fn update_status(
        &self,
        job_key: &str,
        job_instance_id: &String,
        status: JobStatus,
    ) -> Result<(), JobStateError> {
        let mut states = self.require_state(job_key).await?;
        let entry = states.get_mut(job_key).unwrap();
        if &entry.job_instance_id != job_instance_id {
            return Err(JobStateError::StaleInstance(job_key.to_string()));
        }
        entry.status = status;
        Ok(())
    }

    async fn heartbeat(
        &self,
        job_key: &str,
        job_instance_id: &String,
        heartbeat_at: chrono::DateTime<Utc>,
    ) -> Result<(), JobStateError> {
        let mut states = self.require_state(job_key).await?;
        let entry = states.get_mut(job_key).unwrap();
        if &entry.job_instance_id != job_instance_id {
            return Err(JobStateError::StaleInstance(job_key.to_string()));
        }
        entry.heartbeat_at = heartbeat_at;
        Ok(())
    }

    async fn save_error(
        &self,
        job_key: &str,
        job_instance_id: &String,
        message: &str,
    ) -> Result<(), JobStateError> {
        let mut states = self.require_state(job_key).await?;
        let entry = states.get_mut(job_key).unwrap();
        if &entry.job_instance_id != job_instance_id {
            return Err(JobStateError::StaleInstance(job_key.to_string()));
        }
        entry.last_error_type = Some(message.to_string());
        Ok(())
    }
}
