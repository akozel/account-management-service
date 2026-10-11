//! One application-owned business operation on a decoded task payload.
use crate::task::data::{attempt::TaskAttempt, format::TaskPayload};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum TaskHandlerError {
    /// Request durable redelivery after optional backoff, subject to max_attempts.
    /// A safe reason is persisted; do not include payloads, credentials, or secrets.
    #[error("{reason}")]
    Retry { reason: String, retry_after: Option<Duration> },
    /// Stop automatic delivery. This does not prove the effect never occurred.
    #[error("{reason}")]
    Permanent { reason: String },
}

/// Application-owned operation receiving its decoded durable DTO and original attempt.
///
/// Implement with `#[async_trait]`. Business validation, DTO conversion, exact
/// redelivery classification, and safe error reasons remain with the handler.
/// Payloads and handlers need `Send`, but neither `Clone` nor `Sync`.
///
/// Tasks can be redelivered after timeout, crash or lost acknowledgement. Local
/// retries share the invocation budget; accepted remote effects cannot be undone
/// by cancellation. Every invocation must be safe to repeat from the beginning.
/// Attempt IDs/metadata can carry idempotency context, but lease tokens remain
/// platform-only:
/// ```compile_fail
/// use cqrs_runtime::task::data::attempt::TaskAttempt;
/// fn inspect_lease(attempt: &TaskAttempt) { let _ = attempt.lease(); }
/// ```
#[async_trait]
pub trait TaskHandler<P>: Send
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
{
    /// Perform one replay-safe operation; the executor settles its result.
    async fn handle(&mut self, payload: P, attempt: &TaskAttempt) -> Result<(), TaskHandlerError>;
}
