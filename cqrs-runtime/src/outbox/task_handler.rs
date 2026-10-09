//! Handlers for one retryable target command or external effect per task.
//!
//! Tasks may execute concurrently and in any order. Target aggregates check
//! their own phases and invariants; dependent tasks are published only with an
//! accepted event establishing their prerequisite. Exact redelivery must be
//! distinguished from mismatched existing data and classified by the handler.
//! Sender-required idempotency keys live in payload or metadata and are passed
//! unchanged across retries; storing a key alone does not deduplicate an effect.

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
