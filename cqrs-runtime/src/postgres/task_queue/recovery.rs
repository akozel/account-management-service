//! Atomic expired-lease recovery and confirmed failure reporting.
use crate::task::{
    data::format::TaskFormat,
    queue::{
        error::{QueueRequestError, TaskProcessingError},
        retry::redelivery_backoff,
        settlement::{LeaseRecoveryReport, TaskFailure, TaskFailureCause},
    },
};
use sqlx::{FromRow, Row, postgres::PgRow};
use {
    super::queue::{milliseconds, storage_error},
    crate::postgres::task_queue::queue::PostgresTaskQueue,
};
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
    fn into_failure(self) -> Result<TaskFailure, TaskProcessingError> {
        Ok(TaskFailure {
            id: self.id,
            format: TaskFormat::new(self.task_type, self.task_version).map_err(|_| TaskProcessingError::InvalidDelivery)?,
            attempt: self.attempts,
            cause: TaskFailureCause::LeaseExpired,
        })
    }
}
pub(super) async fn recover_expired(queue: &PostgresTaskQueue, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
    let mut tx = queue.begin_queue_transaction("recover_expired").await?;
    let rows = sqlx::query_as::<_, RecoveryRow>(
        r#"
        SELECT id, task_type, task_version, attempts, max_attempts FROM transactional_outbox
        WHERE status = 'processing' AND locked_until <= statement_timestamp()
        ORDER BY locked_until, id LIMIT $1 FOR UPDATE SKIP LOCKED
    "#,
    )
    .bind(i64::try_from(limit).map_err(|_| TaskProcessingError::from(QueueRequestError::InvalidLimit))?)
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_recovery_format_is_an_invalid_delivery() {
        let row = RecoveryRow {
            id: uuid::Uuid::new_v4(),
            task_type: "test".into(),
            task_version: 0,
            attempts: 1,
            max_attempts: 1,
        };
        assert_eq!(row.into_failure(), Err(TaskProcessingError::InvalidDelivery));
    }
}
