use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ingestion_application::job_state::{
    CriticalRange, JobInstanceId, JobState, JobStateError, JobStateRepository, JobStatus,
};
use lazy_static::lazy_static;
use redis::aio::MultiplexedConnection;
use redis::Script;
use shaku::Component;
use std::borrow::Cow;

use crate::rate_limiting::redis::RedisConnection;

const FIELD_STATUS: &str = "status";
const FIELD_JOB_INSTANCE_ID: &str = "job_instance_id";
const FIELD_CURSOR: &str = "cursor";
const FIELD_END_TIME: &str = "end_time";
const FIELD_HEARTBEAT_AT: &str = "heartbeat_at";
const FIELD_CRITICAL_RANGES: &str = "critical_ranges";
const FIELD_LAST_ERROR_TYPE: &str = "last_error_type";
const FIELD_STATE: &str = "state";

lazy_static! {
    static ref CHECK_AND_SET_SCRIPT: Script = Script::new(
        r#"
        local expected = ARGV[1]
        local current = redis.call('HGET', KEYS[1], 'job_instance_id')
        if not current then
            return -1
        end
        if current ~= expected then
            return 0
        end
        for i = 2, #ARGV, 2 do
            redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
        end
        return 1
    "#
    );
}

#[derive(Component)]
#[shaku(interface = JobStateRepository)]
pub struct RedisJobStateRepository {
    #[shaku(inject)]
    redis: Arc<dyn RedisConnection>,
}

#[async_trait]
impl JobStateRepository for RedisJobStateRepository {
    async fn get(&self, job_key: &str) -> Result<Option<JobState>, JobStateError> {
        let mut conn = self.connection().await?;
        let (
            status,
            job_instance_id,
            cursor,
            end_time,
            heartbeat_at,
            critical_ranges,
            last_error_type,
            legacy_state,
        ): (
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = redis::cmd("HMGET")
            .arg(job_key)
            .arg(FIELD_STATUS)
            .arg(FIELD_JOB_INSTANCE_ID)
            .arg(FIELD_CURSOR)
            .arg(FIELD_END_TIME)
            .arg(FIELD_HEARTBEAT_AT)
            .arg(FIELD_CRITICAL_RANGES)
            .arg(FIELD_LAST_ERROR_TYPE)
            .arg(FIELD_STATE)
            .query_async(&mut conn)
            .await
            .map_err(|e| JobStateError::Backend(e.to_string()))?;

        if let (
            Some(status_raw),
            Some(instance_id),
            Some(cursor),
            Some(end_time),
            Some(heartbeat),
        ) = (
            status,
            job_instance_id.clone(),
            cursor,
            end_time,
            heartbeat_at,
        ) {
            return Ok(Some(JobState {
                status: parse_status(&status_raw)?,
                job_instance_id: instance_id,
                cursor,
                end_time,
                heartbeat_at: parse_heartbeat(heartbeat)?,
                critical_ranges: parse_critical_ranges(critical_ranges)?,
                last_error_type: parse_last_error(last_error_type),
            }));
        }

        match legacy_state {
            None => Ok(None),
            Some(payload) => {
                let mut state: JobState = serde_json::from_str(&payload)
                    .map_err(|e| JobStateError::Backend(e.to_string()))?;
                if let Some(server_id) = job_instance_id {
                    state.job_instance_id = server_id;
                }
                Ok(Some(state))
            }
        }
    }

    async fn upsert(&self, job_key: &str, state: &JobState) -> Result<(), JobStateError> {
        self.write_full_state(job_key, state).await
    }

    async fn update_cursor(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        cursor: i64,
    ) -> Result<(), JobStateError> {
        self.update_with(job_key, job_instance_id, |state| state.cursor = cursor)
            .await
    }

    async fn update_status(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        status: JobStatus,
    ) -> Result<(), JobStateError> {
        let status_clone = status.clone();
        self.update_with(job_key, job_instance_id, move |state| {
            state.status = status_clone.clone();
        })
        .await
    }

    async fn heartbeat(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        heartbeat_at: DateTime<Utc>,
    ) -> Result<(), JobStateError> {
        self.update_with(job_key, job_instance_id, |state| {
            state.heartbeat_at = heartbeat_at;
        })
        .await
    }

    async fn save_error(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        message: &str,
    ) -> Result<(), JobStateError> {
        self.update_with(job_key, job_instance_id, |state| {
            state.last_error_type = Some(message.to_string());
        })
        .await
    }
}

impl RedisJobStateRepository {
    async fn connection(&self) -> Result<MultiplexedConnection, JobStateError> {
        self.redis
            .get_connection()
            .await
            .map_err(|e| JobStateError::Backend(e.to_string()))
    }

    async fn update_with<F>(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        mut updater: F,
    ) -> Result<(), JobStateError>
    where
        F: FnMut(&mut JobState),
    {
        let mut state = self
            .get(job_key)
            .await?
            .ok_or_else(|| JobStateError::NotFound(job_key.to_string()))?;

        if &state.job_instance_id != job_instance_id {
            return Err(JobStateError::StaleInstance(job_key.to_string()));
        }

        updater(&mut state);

        self.persist_state(job_key, job_instance_id, &state).await
    }

    async fn persist_state(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        state: &JobState,
    ) -> Result<(), JobStateError> {
        let mut conn = self.connection().await?;
        let mut script_invocation = CHECK_AND_SET_SCRIPT.prepare_invoke();
        script_invocation.key(job_key).arg(job_instance_id);

        for (field, value) in state_field_values(state)? {
            script_invocation.arg(field);
            script_invocation.arg(value);
        }

        let result: i32 = script_invocation
            .invoke_async(&mut conn)
            .await
            .map_err(|e| JobStateError::Backend(e.to_string()))?;

        match result {
            1 => Ok(()),
            0 => Err(JobStateError::StaleInstance(job_key.to_string())),
            -1 => Err(JobStateError::NotFound(job_key.to_string())),
            _ => Err(JobStateError::Backend(format!(
                "Unexpected script result {}",
                result
            ))),
        }
    }

    async fn write_full_state(&self, job_key: &str, state: &JobState) -> Result<(), JobStateError> {
        let mut conn = self.connection().await?;
        let mut cmd = redis::cmd("HSET");
        cmd.arg(job_key);
        for (field, value) in state_field_values(state)? {
            cmd.arg(field);
            cmd.arg(value);
        }

        cmd.query_async(&mut conn)
            .await
            .map_err(|e| JobStateError::Backend(e.to_string()))
            .map(|_: i32| ())
    }
}

fn state_field_values(state: &JobState) -> Result<Vec<(Cow<'static, str>, String)>, JobStateError> {
    Ok(vec![
        (Cow::from(FIELD_STATUS), state.status.as_str().to_string()),
        (
            Cow::from(FIELD_JOB_INSTANCE_ID),
            state.job_instance_id.clone(),
        ),
        (Cow::from(FIELD_CURSOR), state.cursor.to_string()),
        (Cow::from(FIELD_END_TIME), state.end_time.to_string()),
        (
            Cow::from(FIELD_HEARTBEAT_AT),
            state.heartbeat_at.timestamp_millis().to_string(),
        ),
        (
            Cow::from(FIELD_CRITICAL_RANGES),
            serde_json::to_string(&state.critical_ranges)
                .map_err(|e| JobStateError::Backend(e.to_string()))?,
        ),
        (
            Cow::from(FIELD_LAST_ERROR_TYPE),
            state.last_error_type.clone().unwrap_or_default(),
        ),
        (
            Cow::from(FIELD_STATE),
            serde_json::to_string(state).map_err(|e| JobStateError::Backend(e.to_string()))?,
        ),
    ])
}

fn parse_status(raw: &str) -> Result<JobStatus, JobStateError> {
    JobStatus::from_str(raw)
        .ok_or_else(|| JobStateError::Backend(format!("Unrecognized job status value '{}'", raw)))
}

fn parse_heartbeat(value: i64) -> Result<DateTime<Utc>, JobStateError> {
    DateTime::<Utc>::from_timestamp_millis(value)
        .ok_or_else(|| JobStateError::Backend(format!("Invalid heartbeat timestamp '{}'", value)))
}

fn parse_critical_ranges(payload: Option<String>) -> Result<Vec<CriticalRange>, JobStateError> {
    match payload {
        None => Ok(Vec::new()),
        Some(raw) if raw.is_empty() => Ok(Vec::new()),
        Some(raw) => serde_json::from_str(&raw)
            .map_err(|e| JobStateError::Backend(format!("Invalid critical_ranges: {}", e))),
    }
}

fn parse_last_error(value: Option<String>) -> Option<String> {
    match value {
        Some(raw) if raw.is_empty() => None,
        other => other,
    }
}
