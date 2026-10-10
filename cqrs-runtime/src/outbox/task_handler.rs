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
    /// Request durable redelivery after optional backoff, subject to max_attempts.
    /// A safe reason is persisted; do not include payloads, credentials, or secrets.
    #[error("{reason}")]
    Retry { reason: String, retry_after: Option<Duration> },
    /// Stop automatic delivery. This does not prove the effect never occurred.
    #[error("{reason}")]
    Permanent { reason: String },
}

/// Application-owned operation handler; safe to repeat from the beginning.
///
/// Local retries and backoff share one invocation timeout. Return success only
/// when the operation's business contract is satisfied; target commands may
/// classify an exact already-applied operation as success. External sends need
/// receiver idempotency or explicitly permitted repeated effects.
///
/// The runtime may redeliver after timeout, crash, or lost acknowledgement even
/// without a returned [`TaskExecutionError::Retry`]. Use cooperative async I/O;
/// cancellation cannot retract a request already accepted by a remote system.
/// Do not construct/reset task lock tokens or lease deadlines in business code.
#[async_trait]
pub trait TaskHandler: Send {
    /// Supported durable type/version pairs; retain formats while queued work exists.
    fn formats(&self) -> Vec<TaskFormat>;
    /// Perform exactly one retryable target command or external effect.
    /// `Ok(())` requests completion; the coordinator separately settles ownership.
    async fn handle(&mut self, task: &ClaimedTask) -> Result<(), TaskExecutionError>;
}
