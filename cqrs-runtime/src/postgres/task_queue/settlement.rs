//! Token/expiry-fenced settlement and confirmed outcome conversion.
use crate::task::{
    data::attempt::TaskAttempt,
    queue::{
        error::{QueueRequestError, TaskProcessingError},
        lease::LeasedTask,
        settlement::{SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause},
    },
};
use sqlx::Row;
use std::time::Duration;
use {
    super::queue::{milliseconds, storage_error},
    crate::postgres::task_queue::queue::PostgresTaskQueue,
};
struct SettlementParameters {
    status: &'static str,
    delay: Duration,
    last_error: Option<String>,
    failure_cause: Option<TaskFailureCause>,
}

fn settlement_parameters(request: SettlementRequest) -> SettlementParameters {
    match request {
        SettlementRequest::Completed => SettlementParameters {
            status: "completed",
            delay: Duration::ZERO,
            last_error: None,
            failure_cause: None,
        },
        SettlementRequest::Retry { delay, reason } => SettlementParameters {
            status: "pending",
            delay,
            last_error: Some(reason),
            failure_cause: Some(TaskFailureCause::AttemptLimitReached),
        },
        SettlementRequest::Failed { reason } => SettlementParameters {
            status: "failed",
            delay: Duration::ZERO,
            last_error: Some(reason),
            failure_cause: Some(TaskFailureCause::PermanentRejection),
        },
    }
}

fn settlement_result(
    delivery: &TaskAttempt,
    status: Option<&str>,
    failure_cause: Option<TaskFailureCause>,
) -> Result<SettlementResult, TaskProcessingError> {
    match status {
        None => Ok(SettlementResult::OwnershipLost),
        Some("completed") => Ok(SettlementResult::Completed),
        Some("pending") => Ok(SettlementResult::RetryScheduled),
        Some("failed") => Ok(SettlementResult::Failed(TaskFailure {
            id: delivery.id(),
            format: delivery.format().clone(),
            attempt: delivery.number(),
            cause: failure_cause.ok_or(TaskProcessingError::InvalidDelivery)?,
        })),
        _ => Err(TaskProcessingError::InvalidDelivery),
    }
}
pub(super) async fn settle(
    queue: &PostgresTaskQueue,
    task: &LeasedTask,
    request: SettlementRequest,
) -> Result<SettlementResult, TaskProcessingError> {
    let mut tx = queue.begin_queue_transaction("settle").await?;
    let parameters = settlement_parameters(request);
    // Lock first, then check time on the locked row. An UPDATE predicate can
    // be evaluated before waiting on a lock and is not necessarily rechecked
    // when the blocker releases an unchanged tuple after our lease expires.
    let row = sqlx::query(r#"
        WITH owned AS MATERIALIZED (
            SELECT id, locked_until FROM transactional_outbox
            WHERE id = $1 AND lock_token = $2 AND status = 'processing'
            FOR UPDATE
        )
        UPDATE transactional_outbox AS o
        SET status = CASE WHEN $3 = 'pending' AND attempts >= max_attempts THEN 'failed' ELSE $3 END,
            available_at = CASE WHEN $3 = 'pending' AND attempts < max_attempts THEN statement_timestamp() + $4 * interval '1 millisecond' ELSE available_at END,
            finished_at = CASE WHEN $3 <> 'pending' OR attempts >= max_attempts THEN statement_timestamp() ELSE NULL END,
            last_error = $5, locked_by = NULL, lock_token = NULL, locked_until = NULL
        FROM owned
        WHERE o.id = owned.id AND owned.locked_until > clock_timestamp()
        RETURNING o.status
    "#).bind(task.attempt().id()).bind(task.lease().lock_token()).bind(parameters.status).bind(milliseconds(parameters.delay, QueueRequestError::InvalidRetryDelay)?)
        .bind(parameters.last_error.map(|reason| reason.chars().take(2048).collect::<String>()))
        .fetch_optional(&mut *tx).await.map_err(|error| storage_error(error, "settle"))?;
    tx.commit().await.map_err(|error| storage_error(error, "settle"))?;
    let status = row
        .map(|row| row.try_get::<String, _>("status"))
        .transpose()
        .map_err(|error| storage_error(error, "settle"))?;
    settlement_result(task.attempt(), status.as_deref(), parameters.failure_cause)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmed_settlement_rejects_unknown_status_and_missing_failure_cause() {
        let task = crate::task::test_support::claimed();
        for status in [Some("unexpected"), Some("failed")] {
            assert_eq!(
                settlement_result(task.attempt(), status, None),
                Err(TaskProcessingError::InvalidDelivery)
            );
        }
        for request in [
            SettlementRequest::Retry {
                delay: Duration::ZERO,
                reason: "retry".into(),
            },
            SettlementRequest::Failed {
                reason: "permanent".into(),
            },
        ] {
            let parameters = settlement_parameters(request);
            let result = settlement_result(task.attempt(), Some("failed"), parameters.failure_cause).unwrap();
            let failure = result.failure().unwrap();
            assert_eq!(failure.id, task.attempt().id());
            assert_eq!(failure.attempt, task.attempt().number());
            assert_eq!(Some(failure.cause), parameters.failure_cause);
        }
    }
}
