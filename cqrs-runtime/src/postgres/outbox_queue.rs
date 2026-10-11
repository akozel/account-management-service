//! PostgreSQL task ownership with transaction-local statement timeouts.
//!
//! Claims, recovery, and settlement use the canonical outbox schema. Each queue
//! transaction applies its own statement timeout, including lock waiting, and
//! restores the shared connection's previous settings on commit/rollback.
//! The coordinator separately bounds pool acquisition, SQL, commit, and responses.
//! Timeout does not establish rollback or cancel an already accepted remote effect.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};
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
        Self {
            pool,
            statement_timeout: PostgresOutboxQueueConfig::default().statement_timeout(),
        }
    }

    pub fn with_config(pool: PgPool, config: PostgresOutboxQueueConfig) -> Result<Self, PostgresOutboxQueueConfigError> {
        config.validate()?;
        Ok(Self {
            pool,
            statement_timeout: config.statement_timeout(),
        })
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
        let rows = sqlx::query(
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
            .map(|row| {
                let delivery = TaskDelivery::new(
                    row.try_get("id").map_err(|error| storage_error(error, "claim"))?,
                    format.clone(),
                    row.try_get("payload").map_err(|error| storage_error(error, "claim"))?,
                    row.try_get("metadata").map_err(|error| storage_error(error, "claim"))?,
                    row.try_get("attempts").map_err(|error| storage_error(error, "claim"))?,
                )
                .map_err(|_| QueueRequestError::InvalidDelivery)?;
                Ok(ClaimedTask::new(
                    delivery,
                    ClaimLease::new(
                        row.try_get("lock_token").map_err(|error| storage_error(error, "claim"))?,
                        lease_deadline,
                    ),
                ))
            })
            .collect::<Result<Vec<_>, OutboxError>>()?;
        tx.commit().await.map_err(|error| storage_error(error, "claim"))?;
        Ok(tasks)
    }

    async fn settle(&self, task: &ClaimedTask, request: SettlementRequest) -> Result<SettlementResult, OutboxError> {
        let mut tx = self.begin_queue_transaction("settle").await?;
        let failure_cause = match &request {
            SettlementRequest::Completed => None,
            SettlementRequest::Retry { .. } => Some(TaskFailureCause::AttemptLimitReached),
            SettlementRequest::Failed { .. } => Some(TaskFailureCause::HandlerPermanent),
        };
        let (status, delay, error) = match request {
            SettlementRequest::Completed => ("completed", Duration::ZERO, None),
            SettlementRequest::Retry { delay, reason } => ("pending", delay, Some(reason)),
            SettlementRequest::Failed { reason } => ("failed", Duration::ZERO, Some(reason)),
        };
        let row = sqlx::query(r#"
            UPDATE transactional_outbox
            SET status = CASE WHEN $3 = 'pending' AND attempts >= max_attempts THEN 'failed' ELSE $3 END,
                available_at = CASE WHEN $3 = 'pending' AND attempts < max_attempts THEN statement_timestamp() + $4 * interval '1 millisecond' ELSE available_at END,
                finished_at = CASE WHEN $3 <> 'pending' OR attempts >= max_attempts THEN statement_timestamp() ELSE NULL END,
                last_error = $5, locked_by = NULL, lock_token = NULL, locked_until = NULL
            WHERE id = $1 AND lock_token = $2 AND status = 'processing' AND locked_until > clock_timestamp()
            RETURNING status
        "#).bind(task.delivery().id()).bind(task.lease().lock_token()).bind(status).bind(milliseconds(delay, QueueRequestError::InvalidRetryDelay)?)
            .bind(error.map(|e| e.chars().take(2048).collect::<String>()))
            .fetch_optional(&mut *tx).await.map_err(|error| storage_error(error, "settle"))?;
        tx.commit().await.map_err(|error| storage_error(error, "settle"))?;
        let Some(row) = row else {
            return Ok(SettlementResult::OwnershipLost);
        };
        let status: String = row.try_get("status").map_err(|error| storage_error(error, "settle"))?;
        match status.as_str() {
            "completed" => Ok(SettlementResult::Completed),
            "pending" => Ok(SettlementResult::RetryScheduled),
            "failed" => Ok(SettlementResult::Failed(TaskFailure {
                id: task.delivery().id(),
                format: task.delivery().format().clone(),
                attempt: task.delivery().attempt(),
                cause: failure_cause.ok_or(QueueRequestError::InvalidDelivery)?,
            })),
            _ => Err(QueueRequestError::InvalidDelivery.into()),
        }
    }

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, OutboxError> {
        let mut tx = self.begin_queue_transaction("recover_expired").await?;
        let rows = sqlx::query(
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
        for row in &rows {
            let id: uuid::Uuid = row.try_get("id").map_err(|error| storage_error(error, "recover_expired"))?;
            let attempt: i32 = row
                .try_get("attempts")
                .map_err(|error| storage_error(error, "recover_expired"))?;
            let max_attempts: i32 = row
                .try_get("max_attempts")
                .map_err(|error| storage_error(error, "recover_expired"))?;
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
            .bind(id)
            .bind(milliseconds(
                redelivery_backoff(attempt),
                QueueRequestError::InvalidRetryDelay,
            )?)
            .execute(&mut *tx)
            .await
            .map_err(|error| storage_error(error, "recover_expired"))?;
            if attempt >= max_attempts {
                failed.push(TaskFailure {
                    id,
                    format: TaskFormat::new(
                        row.try_get::<String, _>("task_type")
                            .map_err(|error| storage_error(error, "recover_expired"))?,
                        row.try_get("task_version")
                            .map_err(|error| storage_error(error, "recover_expired"))?,
                    )
                    .map_err(|_| QueueRequestError::InvalidDelivery)?,
                    attempt,
                    cause: TaskFailureCause::LeaseExpired,
                });
            }
        }
        tx.commit().await.map_err(|error| storage_error(error, "recover_expired"))?;
        Ok(LeaseRecoveryReport::new(rows.len() - failed.len(), failed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
