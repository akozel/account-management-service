//! PostgreSQL task ownership with transaction-local statement timeouts.
//!
//! Claims, recovery, and settlement use the canonical outbox schema. Each queue
//! transaction applies its own statement timeout, including lock waiting, and
//! restores the shared connection's previous settings on commit/rollback.
//! The coordinator separately bounds pool acquisition, SQL, commit, and responses.
//! Timeout does not establish rollback or cancel an already accepted remote effect.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::{FromRow, PgPool, Postgres, Row, Transaction, postgres::PgRow};
use tokio::time::Instant;

use crate::outbox::{
    ClaimLease, ClaimedTask, LeaseRecoveryReport, OutboxError, OutboxQueue, QueueRequestError, SettlementRequest,
    SettlementResult, TaskDelivery, TaskFailure, TaskFailureCause, TaskFormat, redelivery_backoff,
};

pub struct PostgresOutboxQueue {
    pool: PgPool,
    statement_timeout: Duration,
}

mod config;
pub use config::{PostgresOutboxQueueConfig, PostgresOutboxQueueConfigError};
impl PostgresOutboxQueue {
    pub fn new(pool: PgPool) -> Self {
        Self::with_config(pool, PostgresOutboxQueueConfig::default())
    }

    pub fn with_config(pool: PgPool, config: PostgresOutboxQueueConfig) -> Self {
        Self {
            pool,
            statement_timeout: config.statement_timeout(),
        }
    }

    async fn begin_queue_transaction(&self, operation: &'static str) -> Result<Transaction<'_, Postgres>, OutboxError> {
        let mut tx = self.pool.begin().await.map_err(|error| storage_error(error, operation))?;
        // Transaction-local settings never change the shared pool's session defaults.
        sqlx::query("SELECT set_config('statement_timeout', $1, true)")
            .bind(self.statement_timeout.as_millis().to_string())
            .execute(&mut *tx)
            .await
            .map_err(|error| storage_error(error, operation))?;
        Ok(tx)
    }
}

fn storage_error(error: sqlx::Error, operation: &'static str) -> OutboxError {
    crate::diagnostics::log_sqlx_failure(operation, &error);
    OutboxError::Unavailable
}

fn milliseconds(duration: Duration, invalid: QueueRequestError) -> Result<f64, OutboxError> {
    i64::try_from(duration.as_millis())
        .map(|n| n as f64)
        .map_err(|_| OutboxError::from(invalid))
}

struct ClaimRow {
    id: uuid::Uuid,
    payload: serde_json::Value,
    metadata: serde_json::Value,
    attempts: i32,
    lock_token: uuid::Uuid,
}

impl FromRow<'_, PgRow> for ClaimRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            payload: row.try_get("payload")?,
            metadata: row.try_get("metadata")?,
            attempts: row.try_get("attempts")?,
            lock_token: row.try_get("lock_token")?,
        })
    }
}

impl ClaimRow {
    fn into_claimed_task(self, format: &TaskFormat, lease_deadline: Instant) -> Result<ClaimedTask, OutboxError> {
        let delivery = TaskDelivery::new(self.id, format.clone(), self.payload, self.metadata, self.attempts)
            .map_err(|_| OutboxError::InvalidDelivery)?;
        Ok(ClaimedTask::new(delivery, ClaimLease::new(self.lock_token, lease_deadline)))
    }
}

struct RecoveryRow {
    id: uuid::Uuid,
    task_type: String,
    task_version: i32,
    attempts: i32,
    max_attempts: i32,
}

impl FromRow<'_, PgRow> for RecoveryRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            task_type: row.try_get("task_type")?,
            task_version: row.try_get("task_version")?,
            attempts: row.try_get("attempts")?,
            max_attempts: row.try_get("max_attempts")?,
        })
    }
}

impl RecoveryRow {
    fn into_failure(self) -> Result<TaskFailure, OutboxError> {
        Ok(TaskFailure {
            id: self.id,
            format: TaskFormat::new(self.task_type, self.task_version).map_err(|_| OutboxError::InvalidDelivery)?,
            attempt: self.attempts,
            cause: TaskFailureCause::LeaseExpired,
        })
    }
}

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
    delivery: &TaskDelivery,
    status: Option<&str>,
    failure_cause: Option<TaskFailureCause>,
) -> Result<SettlementResult, OutboxError> {
    match status {
        None => Ok(SettlementResult::OwnershipLost),
        Some("completed") => Ok(SettlementResult::Completed),
        Some("pending") => Ok(SettlementResult::RetryScheduled),
        Some("failed") => Ok(SettlementResult::Failed(TaskFailure {
            id: delivery.id(),
            format: delivery.format().clone(),
            attempt: delivery.attempt(),
            cause: failure_cause.ok_or(OutboxError::InvalidDelivery)?,
        })),
        _ => Err(OutboxError::InvalidDelivery),
    }
}

#[async_trait]
impl OutboxQueue for PostgresOutboxQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<ClaimedTask>, OutboxError> {
        if lease_duration.as_millis() == 0 {
            return Err(QueueRequestError::InvalidLeaseDuration.into());
        }
        // PostgreSQL grants its lease later, during UPDATE. Starting our local
        // clock before any await gives a conservative bound without UTC skew.
        let lease_deadline = Instant::now()
            .checked_add(lease_duration)
            .ok_or(QueueRequestError::InvalidLeaseDuration)?;
        let limit = i64::try_from(limit).map_err(|_| OutboxError::from(QueueRequestError::InvalidLimit))?;
        let mut tx = self.begin_queue_transaction("claim").await?;
        let rows = sqlx::query_as::<_, ClaimRow>(
            r#"
            WITH ready AS (
                SELECT id FROM transactional_outbox
                WHERE status = 'pending' AND task_type = $1 AND task_version = $2
                  AND available_at <= statement_timestamp()
                ORDER BY available_at, id LIMIT $3 FOR UPDATE SKIP LOCKED
            )
            UPDATE transactional_outbox AS o
            SET status = 'processing', attempts = attempts + 1, locked_by = $4,
                lock_token = gen_random_uuid(),
                locked_until = clock_timestamp() + $5 * interval '1 millisecond'
            FROM ready WHERE o.id = ready.id
            RETURNING o.id, o.payload, o.metadata, o.attempts, o.lock_token
        "#,
        )
        .bind(format.task_type())
        .bind(format.task_version())
        .bind(limit)
        .bind(owner)
        .bind(milliseconds(lease_duration, QueueRequestError::InvalidLeaseDuration)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| storage_error(error, "claim"))?;
        let tasks = rows
            .into_iter()
            .map(|row| row.into_claimed_task(format, lease_deadline))
            .collect::<Result<Vec<_>, OutboxError>>()?;
        tx.commit().await.map_err(|error| storage_error(error, "claim"))?;
        Ok(tasks)
    }

    async fn settle(&self, task: &ClaimedTask, request: SettlementRequest) -> Result<SettlementResult, OutboxError> {
        let mut tx = self.begin_queue_transaction("settle").await?;
        let parameters = settlement_parameters(request);
        let row = sqlx::query(r#"
            UPDATE transactional_outbox
            SET status = CASE WHEN $3 = 'pending' AND attempts >= max_attempts THEN 'failed' ELSE $3 END,
                available_at = CASE WHEN $3 = 'pending' AND attempts < max_attempts THEN statement_timestamp() + $4 * interval '1 millisecond' ELSE available_at END,
                finished_at = CASE WHEN $3 <> 'pending' OR attempts >= max_attempts THEN statement_timestamp() ELSE NULL END,
                last_error = $5, locked_by = NULL, lock_token = NULL, locked_until = NULL
            WHERE id = $1 AND lock_token = $2 AND status = 'processing' AND locked_until > clock_timestamp()
            RETURNING status
        "#).bind(task.delivery().id()).bind(task.lease().lock_token()).bind(parameters.status).bind(milliseconds(parameters.delay, QueueRequestError::InvalidRetryDelay)?)
            .bind(parameters.last_error.map(|reason| reason.chars().take(2048).collect::<String>()))
            .fetch_optional(&mut *tx).await.map_err(|error| storage_error(error, "settle"))?;
        tx.commit().await.map_err(|error| storage_error(error, "settle"))?;
        let status = row
            .map(|row| row.try_get::<String, _>("status"))
            .transpose()
            .map_err(|error| storage_error(error, "settle"))?;
        settlement_result(task.delivery(), status.as_deref(), parameters.failure_cause)
    }

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError> {
        let mut tx = self.begin_queue_transaction("recover_expired").await?;
        let rows = sqlx::query_as::<_, RecoveryRow>(
            r#"
            SELECT id, task_type, task_version, attempts, max_attempts FROM transactional_outbox
            WHERE status = 'processing' AND locked_until <= statement_timestamp()
            ORDER BY locked_until, id LIMIT $1 FOR UPDATE SKIP LOCKED
        "#,
        )
        .bind(i64::try_from(limit).map_err(|_| OutboxError::from(QueueRequestError::InvalidLimit))?)
        .fetch_all(&mut *tx)
        .await
        .map_err(|error| storage_error(error, "recover_expired"))?;
        let mut failed = Vec::new();
        let processed_count = rows.len();
        for row in rows {
            sqlx::query(
                r#"
                UPDATE transactional_outbox
                SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
                    available_at = statement_timestamp() + $2 * interval '1 millisecond',
                    finished_at = CASE WHEN attempts >= max_attempts THEN statement_timestamp() ELSE NULL END,
                    last_error = 'worker lease expired', locked_by = NULL, lock_token = NULL, locked_until = NULL
                WHERE id = $1
            "#,
            )
            .bind(row.id)
            .bind(milliseconds(
                redelivery_backoff(row.attempts),
                QueueRequestError::InvalidRetryDelay,
            )?)
            .execute(&mut *tx)
            .await
            .map_err(|error| storage_error(error, "recover_expired"))?;
            if row.attempts >= row.max_attempts {
                failed.push(row.into_failure()?);
            }
        }
        tx.commit().await.map_err(|error| storage_error(error, "recover_expired"))?;
        Ok(LeaseRecoveryReport::new(processed_count - failed.len(), failed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claim_rows_validate_delivery_data_and_preserve_lease_context() {
        use serde_json::json;
        let id = uuid::Uuid::new_v4();
        let lock_token = uuid::Uuid::new_v4();
        let deadline = Instant::now();
        let format = TaskFormat::new("test", 1).unwrap();
        for (payload, metadata, attempts, valid) in [
            (json!({"value": 1}), json!({}), 2, true),
            (json!([]), json!({}), 2, false),
            (json!({}), json!([]), 2, false),
            (json!({}), json!({}), 0, false),
        ] {
            let row = ClaimRow {
                id,
                payload,
                metadata,
                attempts,
                lock_token,
            };
            let result = row.into_claimed_task(&format, deadline);
            if valid {
                let claimed = result.unwrap();
                assert_eq!(claimed.delivery().id(), id);
                assert_eq!(claimed.delivery().attempt(), 2);
                assert_eq!(claimed.delivery().payload(), &json!({"value": 1}));
                assert_eq!(claimed.lease().deadline(), deadline);
                assert_eq!(claimed.lease().lock_token(), lock_token);
            } else {
                assert!(matches!(result, Err(OutboxError::InvalidDelivery)));
            }
        }
    }

    #[test]
    fn confirmed_settlement_rejects_unknown_status_and_missing_failure_cause() {
        let task = crate::outbox::test_support::claimed();
        for status in [Some("unexpected"), Some("failed")] {
            assert_eq!(
                settlement_result(task.delivery(), status, None),
                Err(OutboxError::InvalidDelivery)
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
            let result = settlement_result(task.delivery(), Some("failed"), parameters.failure_cause).unwrap();
            let failure = result.failure().unwrap();
            assert_eq!(failure.id, task.delivery().id());
            assert_eq!(failure.attempt, task.delivery().attempt());
            assert_eq!(Some(failure.cause), parameters.failure_cause);
        }
    }

    #[test]
    fn invalid_recovery_format_is_an_invalid_delivery() {
        let row = RecoveryRow {
            id: uuid::Uuid::new_v4(),
            task_type: "test".into(),
            task_version: 0,
            attempts: 1,
            max_attempts: 1,
        };
        assert_eq!(row.into_failure(), Err(OutboxError::InvalidDelivery));
    }

    #[tokio::test]
    async fn unavailable_queue_logs_operation_and_cause_before_erasing_source() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        pool.close().await;
        let queue = PostgresOutboxQueue::new(pool);
        let task = crate::outbox::test_support::claimed();
        let (_, log) = crate::diagnostics::test_support::capture(async {
            assert_eq!(
                queue
                    .claim(task.delivery().format(), "private_owner", 1, Duration::from_secs(60))
                    .await
                    .unwrap_err(),
                OutboxError::Unavailable
            );
            assert_eq!(
                queue.settle(&task, SettlementRequest::Completed).await.unwrap_err(),
                OutboxError::Unavailable
            );
            assert_eq!(queue.recover_expired(1).await.unwrap_err(), OutboxError::Unavailable);
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), 3);
        for operation in ["claim", "settle", "recover_expired"] {
            assert!(log.contains(operation));
        }
        assert!(log.contains("pool_closed"));
        assert!(!log.contains("private_"));
    }

    #[tokio::test]
    async fn statement_timeout_config_validates_defaults_overrides_and_server_range() {
        assert_eq!(
            PostgresOutboxQueueConfig::from_lookup(|_| None).unwrap().statement_timeout(),
            Duration::from_secs(4)
        );
        assert_eq!(
            PostgresOutboxQueueConfig::from_lookup(|_| Some("125".into()))
                .unwrap()
                .statement_timeout(),
            Duration::from_millis(125)
        );
        assert!(PostgresOutboxQueueConfig::from_lookup(|_| Some(i32::MAX.to_string())).is_ok());
        for value in ["0", "invalid", "-1", "2147483648", "18446744073709551616"] {
            assert!(PostgresOutboxQueueConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for statement_timeout in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            assert!(PostgresOutboxQueueConfig::new(statement_timeout).is_err());
        }
    }
}
