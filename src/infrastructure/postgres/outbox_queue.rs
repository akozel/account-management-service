use std::time::Duration;

use async_trait::async_trait;
use sqlx::{PgPool, Row};

use crate::application::outbox::{
    ClaimedTask, FailedTask, OutboxError, OutboxQueue, RecoveryResult, SettleResult, TaskFormat, TaskOutcome, retry_delay,
};

pub struct PostgresOutboxQueue {
    pool: PgPool,
}

impl PostgresOutboxQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
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
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
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
            .fetch_optional(&self.pool).await.map_err(storage_error)?;
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
        let mut tx = self.pool.begin().await.map_err(storage_error)?;
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
