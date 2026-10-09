use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::outbox::{
    ClaimedTask, FailedTask, OutboxError, OutboxQueue, RecoveryResult, SettleResult, TaskFormat, TaskOutcome, retry_delay,
};

pub struct PostgresOutboxQueue {
    pool: PgPool,
    statement_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct PostgresOutboxQueueConfig {
    /// Per-statement server budget, including lock waiting; scoped to queue transactions.
    pub statement_timeout: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("invalid PostgreSQL outbox configuration: {0}")]
pub struct PostgresOutboxQueueConfigError(pub &'static str);

impl Default for PostgresOutboxQueueConfig {
    fn default() -> Self {
        Self {
            statement_timeout: Duration::from_secs(4),
        }
    }
}

impl PostgresOutboxQueueConfig {
    pub fn from_env() -> Result<Self, PostgresOutboxQueueConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, PostgresOutboxQueueConfigError> {
        let millis = match lookup("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS") {
            Some(value) => value
                .parse::<u64>()
                .map_err(|_| PostgresOutboxQueueConfigError("OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS"))?,
            None => 4_000,
        };
        let config = Self {
            statement_timeout: Duration::from_millis(millis),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), PostgresOutboxQueueConfigError> {
        if self.statement_timeout.as_millis() == 0 || self.statement_timeout.as_millis() > i32::MAX as u128 {
            return Err(PostgresOutboxQueueConfigError(
                "statement timeout must be 1..=2147483647 milliseconds",
            ));
        }
        Ok(())
    }
}

impl PostgresOutboxQueue {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            statement_timeout: PostgresOutboxQueueConfig::default().statement_timeout,
        }
    }

    pub fn with_config(pool: PgPool, config: PostgresOutboxQueueConfig) -> Result<Self, PostgresOutboxQueueConfigError> {
        config.validate()?;
        Ok(Self {
            pool,
            statement_timeout: config.statement_timeout,
        })
    }

    async fn transaction(&self) -> Result<Transaction<'_, Postgres>, OutboxError> {
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
        // Transaction-local settings never change the shared pool's session defaults.
        sqlx::query("SELECT set_config('statement_timeout', $1, true)")
            .bind(self.statement_timeout.as_millis().to_string())
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
        Ok(tx)
    }
}

fn storage_error(_: sqlx::Error) -> OutboxError {
    OutboxError::Unavailable
}

fn milliseconds(duration: Duration) -> Result<f64, OutboxError> {
    i64::try_from(duration.as_millis())
        .map(|n| n as f64)
        .map_err(|_| OutboxError::InvalidTask)
}

#[async_trait]
impl OutboxQueue for PostgresOutboxQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<ClaimedTask>, OutboxError> {
        if lease.is_zero() {
            return Err(OutboxError::InvalidTask);
        }
        let limit = i64::try_from(limit).map_err(|_| OutboxError::InvalidTask)?;
        let mut tx = self.transaction().await?;
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
        .bind(&format.task_type)
        .bind(format.task_version)
        .bind(limit)
        .bind(owner)
        .bind(milliseconds(lease)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;
        let tasks = rows
            .into_iter()
            .map(|row| {
                Ok(ClaimedTask {
                    id: row.try_get("id")?,
                    format: format.clone(),
                    payload: row.try_get("payload")?,
                    metadata: row.try_get("metadata")?,
                    attempt: row.try_get("attempts")?,
                    lock_token: row.try_get("lock_token")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        Ok(tasks)
    }

    async fn settle(&self, task: &ClaimedTask, outcome: TaskOutcome) -> Result<SettleResult, OutboxError> {
        let mut tx = self.transaction().await?;
        let retry = matches!(outcome, TaskOutcome::Retry { .. });
        let (status, delay, error) = match outcome {
            TaskOutcome::Completed => ("completed", Duration::ZERO, None),
            TaskOutcome::Retry { delay, reason } => ("pending", delay, Some(reason)),
            TaskOutcome::Failed { reason } => ("failed", Duration::ZERO, Some(reason)),
        };
        let row = sqlx::query(r#"
            UPDATE transactional_outbox
            SET status = CASE WHEN $3 = 'pending' AND attempts >= max_attempts THEN 'failed' ELSE $3 END,
                available_at = CASE WHEN $3 = 'pending' AND attempts < max_attempts THEN statement_timestamp() + $4 * interval '1 millisecond' ELSE available_at END,
                finished_at = CASE WHEN $3 <> 'pending' OR attempts >= max_attempts THEN statement_timestamp() ELSE NULL END,
                last_error = $5, locked_by = NULL, lock_token = NULL, locked_until = NULL
            WHERE id = $1 AND lock_token = $2 AND status = 'processing' AND locked_until > clock_timestamp()
            RETURNING status
        "#).bind(task.id).bind(task.lock_token).bind(status).bind(milliseconds(delay)?)
            .bind(error.map(|e| e.chars().take(2048).collect::<String>()))
            .fetch_optional(&mut *tx).await.map_err(storage_error)?;
        tx.commit().await.map_err(storage_error)?;
        let Some(row) = row else {
            return Ok(SettleResult::OwnershipLost);
        };
        let status: String = row.try_get("status").map_err(storage_error)?;
        let failed = (status == "failed").then(|| FailedTask {
            id: task.id,
            format: task.format.clone(),
            attempt: task.attempt,
            reason: if retry { "retry limit reached" } else { "permanent failure" },
        });
        Ok(SettleResult::Settled { failed })
    }

    async fn recover_expired(&self, limit: usize) -> Result<RecoveryResult, OutboxError> {
        let mut tx = self.transaction().await?;
        let rows = sqlx::query(
            r#"
            SELECT id, task_type, task_version, attempts, max_attempts FROM transactional_outbox
            WHERE status = 'processing' AND locked_until <= statement_timestamp()
            ORDER BY locked_until, id LIMIT $1 FOR UPDATE SKIP LOCKED
        "#,
        )
        .bind(i64::try_from(limit).map_err(|_| OutboxError::InvalidTask)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(storage_error)?;
        let mut failed = Vec::new();
        for row in &rows {
            let id: uuid::Uuid = row.try_get("id").map_err(storage_error)?;
            let attempt: i32 = row.try_get("attempts").map_err(storage_error)?;
            let max_attempts: i32 = row.try_get("max_attempts").map_err(storage_error)?;
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
            .bind(milliseconds(retry_delay(attempt))?)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
            if attempt >= max_attempts {
                failed.push(FailedTask {
                    id,
                    format: TaskFormat {
                        task_type: row.try_get("task_type").map_err(storage_error)?,
                        task_version: row.try_get("task_version").map_err(storage_error)?,
                    },
                    attempt,
                    reason: "worker lease expired",
                });
            }
        }
        tx.commit().await.map_err(storage_error)?;
        Ok(RecoveryResult {
            recovered: rows.len() as u64,
            failed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn statement_timeout_config_validates_defaults_overrides_and_server_range() {
        assert_eq!(
            PostgresOutboxQueueConfig::from_lookup(|_| None).unwrap().statement_timeout,
            Duration::from_secs(4)
        );
        assert_eq!(
            PostgresOutboxQueueConfig::from_lookup(|_| Some("125".into()))
                .unwrap()
                .statement_timeout,
            Duration::from_millis(125)
        );
        assert!(PostgresOutboxQueueConfig::from_lookup(|_| Some(i32::MAX.to_string())).is_ok());
        for value in ["0", "invalid", "-1", "2147483648", "18446744073709551616"] {
            assert!(PostgresOutboxQueueConfig::from_lookup(|_| Some(value.into())).is_err());
        }
        for statement_timeout in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            assert!(PostgresOutboxQueueConfig { statement_timeout }.validate().is_err());
        }
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://localhost/test")
            .unwrap();
        assert!(
            PostgresOutboxQueue::with_config(
                pool,
                PostgresOutboxQueueConfig {
                    statement_timeout: Duration::ZERO
                }
            )
            .is_err()
        );
    }
}
