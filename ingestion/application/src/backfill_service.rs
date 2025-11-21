use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use shaku::{Component, Interface};
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;

use crate::historical_data::{GapDetector, HistoricalDataGateway};
use crate::job_state::{JobInstanceId, JobState, JobStateRepository, JobStatus};
use crate::ports::TickRepository;
use ingestion_domain::DateRange;

const HEARTBEAT_TIMEOUT: Duration = Duration::seconds(300);

#[async_trait]
pub trait BackfillService: Interface {
    async fn backfill_range(
        &self,
        symbol: &str,
        range: DateRange,
    ) -> Result<BackfillReport, BackfillError>;
}

#[derive(Component)]
#[shaku(interface = BackfillService)]
pub struct BackfillServiceImpl {
    #[shaku(inject)]
    gateway: Arc<dyn HistoricalDataGateway>,

    #[shaku(inject)]
    gap_detector: Arc<dyn GapDetector>,

    #[shaku(inject)]
    repository: Arc<dyn TickRepository>,

    #[shaku(inject)]
    job_state_repo: Arc<dyn JobStateRepository>,
}

impl BackfillServiceImpl {
    pub fn new(
        gateway: Arc<dyn HistoricalDataGateway>,
        gap_detector: Arc<dyn GapDetector>,
        repository: Arc<dyn TickRepository>,
        job_state_repo: Arc<dyn JobStateRepository>,
    ) -> Self {
        Self {
            gateway,
            gap_detector,
            repository,
            job_state_repo,
        }
    }

    async fn backfill_single_day(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<DayResult, BackfillError> {
        let ticks = self
            .gateway
            .fetch_historical_ticks(symbol, date)
            .await
            .map_err(BackfillError::GatewayError)?;

        let tick_count = ticks.len();
        let last_timestamp = ticks.last().map(|tick| tick.timestamp().timestamp_millis());

        if !ticks.is_empty() {
            self.repository
                .save_batch(ticks)
                .await
                .map_err(BackfillError::RepositoryError)?;
        }

        Ok(DayResult {
            tick_count,
            last_timestamp,
        })
    }

    async fn initialize_job(
        &self,
        symbol: &str,
        range: &DateRange,
    ) -> Result<JobContext, BackfillError> {
        let job_key = format!("ingest:job:{}:{}", symbol, range.start());
        let now = Utc::now();
        if let Some(mut state) = self.job_state_repo.get(&job_key).await? {
            if matches!(state.status, JobStatus::Running) {
                let heartbeat_age = now.signed_duration_since(state.heartbeat_at);
                if heartbeat_age <= HEARTBEAT_TIMEOUT {
                    return Err(BackfillError::JobAlreadyRunning(job_key));
                }

                state.job_instance_id = Uuid::new_v4().to_string();
                state.status = JobStatus::Running;
                state.heartbeat_at = now;
                self.job_state_repo.upsert(&job_key, &state).await?;
                return Ok(JobContext { job_key, state });
            }
        }

        let job_instance_id = Uuid::new_v4().to_string();
        let initial_cursor = start_of_day_ts(range.start()).saturating_sub(1);
        let state = JobState::new(
            job_instance_id.clone(),
            JobStatus::Running,
            initial_cursor,
            end_of_day_ts(range.end()),
            now,
        );
        self.job_state_repo.upsert(&job_key, &state).await?;
        Ok(JobContext { job_key, state })
    }

    async fn finalize_job(
        &self,
        ctx: &mut JobContext,
        status: JobStatus,
    ) -> Result<(), BackfillError> {
        self.job_state_repo
            .update_status(ctx.job_key(), ctx.job_instance_id(), status.clone())
            .await?;
        ctx.state.status = status;
        self.job_state_repo
            .heartbeat(ctx.job_key(), ctx.job_instance_id(), Utc::now())
            .await?;
        Ok(())
    }

    async fn record_error(&self, ctx: &mut JobContext, message: &str) -> Result<(), BackfillError> {
        self.job_state_repo
            .save_error(ctx.job_key(), ctx.job_instance_id(), message)
            .await?;
        ctx.state.last_error_type = Some(message.to_string());
        Ok(())
    }
}

#[async_trait]
impl BackfillService for BackfillServiceImpl {
    async fn backfill_range(
        &self,
        symbol: &str,
        range: DateRange,
    ) -> Result<BackfillReport, BackfillError> {
        let mut job_ctx = self.initialize_job(symbol, &range).await?;
        let effective_start = resume_start(range.start(), job_ctx.state.cursor);
        if effective_start > range.end() {
            self.finalize_job(&mut job_ctx, JobStatus::Completed)
                .await?;
            return Ok(BackfillReport {
                symbol: symbol.to_string(),
                range,
                days_processed: 0,
                total_ticks: 0,
                failed_days: Vec::new(),
            });
        }
        let effective_range =
            DateRange::new(effective_start, range.end()).expect("effective range must be valid");

        let gaps = self
            .gap_detector
            .detect_gaps(symbol, effective_range.clone())
            .await
            .map_err(BackfillError::GapDetectionError)?;

        let days_to_process = plan_days_to_process(effective_start, range.end(), gaps.as_slice());

        let mut total_ticks = 0;
        let mut days_processed = 0;
        let mut failed_days = Vec::new();
        let mut job_failed = false;

        for date in days_to_process {
            let day_end = end_of_day_ts(date);
            if day_end <= job_ctx.state.cursor {
                continue;
            }

            self.job_state_repo
                .heartbeat(job_ctx.job_key(), job_ctx.job_instance_id(), Utc::now())
                .await?;

            match self.backfill_single_day(symbol, date).await {
                Ok(result) => {
                    total_ticks += result.tick_count;
                    days_processed += 1;
                    let cursor_ts = result.last_timestamp.unwrap_or(day_end);
                    self.job_state_repo
                        .update_cursor(job_ctx.job_key(), job_ctx.job_instance_id(), cursor_ts)
                        .await?;
                    job_ctx.state.cursor = cursor_ts;
                }
                Err(e) => {
                    job_failed = true;
                    let msg = e.to_string();
                    self.record_error(&mut job_ctx, &msg).await?;
                    failed_days.push((date, msg));
                }
            }
        }

        self.repository
            .shutdown()
            .await
            .map_err(BackfillError::RepositoryError)?;

        let final_status = if job_failed {
            JobStatus::Failed
        } else {
            JobStatus::Completed
        };
        self.finalize_job(&mut job_ctx, final_status).await?;

        Ok(BackfillReport {
            symbol: symbol.to_string(),
            range,
            days_processed,
            total_ticks,
            failed_days,
        })
    }
}

#[derive(Debug)]
pub struct BackfillReport {
    pub symbol: String,
    pub range: DateRange,
    pub days_processed: usize,
    pub total_ticks: usize,
    pub failed_days: Vec<(NaiveDate, String)>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("Gateway error: {0}")]
    GatewayError(#[from] crate::historical_data::HistoricalDataError),

    #[error("Gap detection error: {0}")]
    GapDetectionError(#[from] crate::historical_data::GapDetectionError),

    #[error("Repository error: {0}")]
    RepositoryError(#[from] crate::ports::RepositoryError),

    #[error("Job state error: {0}")]
    JobStateError(#[from] crate::job_state::JobStateError),

    #[error("Job already running: {0}")]
    JobAlreadyRunning(String),
}

struct JobContext {
    job_key: String,
    state: JobState,
}

impl JobContext {
    fn job_key(&self) -> &str {
        &self.job_key
    }

    fn job_instance_id(&self) -> &JobInstanceId {
        &self.state.job_instance_id
    }
}

struct DayResult {
    tick_count: usize,
    last_timestamp: Option<i64>,
}

fn start_of_day_ts(date: NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0)
        .expect("valid midnight")
        .and_utc()
        .timestamp_millis()
}

fn end_of_day_ts(date: NaiveDate) -> i64 {
    date.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap())
        .and_utc()
        .timestamp_millis()
}

fn resume_start(range_start: NaiveDate, cursor: i64) -> NaiveDate {
    let start_ts = start_of_day_ts(range_start);
    if cursor < start_ts {
        return range_start;
    }
    cursor.timestamp_to_date().unwrap_or(range_start)
}

fn plan_days_to_process(
    effective_start: NaiveDate,
    range_end: NaiveDate,
    gaps: &[DateRange],
) -> Vec<NaiveDate> {
    let mut days = BTreeSet::new();
    if effective_start <= range_end {
        days.insert(effective_start);
    }

    for gap in gaps {
        for day_range in gap.clone().split_by_days() {
            let date = day_range.start();
            if date < effective_start || date > range_end {
                continue;
            }
            days.insert(date);
        }
    }

    days.into_iter().collect()
}

trait CursorExt {
    fn timestamp_to_date(&self) -> Option<NaiveDate>;
}

impl CursorExt for i64 {
    fn timestamp_to_date(&self) -> Option<NaiveDate> {
        DateTime::<Utc>::from_timestamp_millis(*self).map(|dt| dt.date_naive())
    }
}
