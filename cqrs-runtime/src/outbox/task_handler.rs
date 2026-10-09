use super::{queue::ClaimedTask, task::TaskFormat};
use async_trait::async_trait;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum TaskExecutionError {
    #[error("{reason}")]
    Retry { reason: String, retry_after: Option<Duration> },
    #[error("{reason}")]
    Permanent { reason: String },
}

#[async_trait]
pub trait TaskHandler: Send {
    fn formats(&self) -> Vec<TaskFormat>;
    async fn handle(&mut self, task: &ClaimedTask) -> Result<(), TaskExecutionError>;
}
