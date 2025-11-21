use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use shaku::Interface;

pub type JobInstanceId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "PENDING",
            JobStatus::Running => "RUNNING",
            JobStatus::Completed => "COMPLETED",
            JobStatus::Failed => "FAILED",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "PENDING" => Some(JobStatus::Pending),
            "RUNNING" => Some(JobStatus::Running),
            "COMPLETED" => Some(JobStatus::Completed),
            "FAILED" => Some(JobStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobState {
    pub status: JobStatus,
    pub job_instance_id: JobInstanceId,
    pub cursor: i64,
    pub end_time: i64,
    pub heartbeat_at: DateTime<Utc>,
    #[serde(default)]
    pub critical_ranges: Vec<CriticalRange>,
    #[serde(default)]
    #[serde(alias = "last_error")]
    pub last_error_type: Option<String>,
}

impl JobState {
    pub fn new(
        job_instance_id: JobInstanceId,
        status: JobStatus,
        cursor: i64,
        end_time: i64,
        heartbeat_at: DateTime<Utc>,
    ) -> Self {
        Self {
            status,
            job_instance_id,
            cursor,
            end_time,
            heartbeat_at,
            critical_ranges: Vec::new(),
            last_error_type: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriticalRange {
    pub start: String,
    pub end: String,
}

#[derive(Debug, thiserror::Error)]
pub enum JobStateError {
    #[error("Job state not found: {0}")]
    NotFound(String),
    #[error("Concurrent modification detected for job {0}")]
    StaleInstance(String),
    #[error("Backend error: {0}")]
    Backend(String),
}

#[async_trait]
pub trait JobStateRepository: Interface {
    async fn get(&self, job_key: &str) -> Result<Option<JobState>, JobStateError>;
    async fn upsert(&self, job_key: &str, state: &JobState) -> Result<(), JobStateError>;
    async fn update_cursor(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        cursor: i64,
    ) -> Result<(), JobStateError>;
    async fn update_status(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        status: JobStatus,
    ) -> Result<(), JobStateError>;
    async fn heartbeat(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        heartbeat_at: DateTime<Utc>,
    ) -> Result<(), JobStateError>;
    async fn save_error(
        &self,
        job_key: &str,
        job_instance_id: &JobInstanceId,
        message: &str,
    ) -> Result<(), JobStateError>;
}
