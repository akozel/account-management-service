//! Atomic ready-task claims with conservative local lease anchoring.
use crate::task::{
    data::{attempt::TaskAttempt, format::TaskFormat},
    queue::{
        error::{QueueRequestError, TaskProcessingError},
        lease::{LeasedTask, TaskLease},
    },
};
use sqlx::{FromRow, Row, postgres::PgRow};
use std::time::Duration;
use tokio::time::Instant;
use {
    super::queue::{milliseconds, storage_error},
    crate::postgres::task_queue::queue::PostgresTaskQueue,
};
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
    fn into_claimed_task(self, format: &TaskFormat, lease_deadline: Instant) -> Result<LeasedTask, TaskProcessingError> {
        let delivery = TaskAttempt::new(self.id, format.clone(), self.payload, self.metadata, self.attempts)
            .map_err(|_| TaskProcessingError::InvalidDelivery)?;
        Ok(LeasedTask::new(delivery, TaskLease::new(self.lock_token, lease_deadline)))
    }
}
pub(super) async fn claim(
    queue: &PostgresTaskQueue,
    format: &TaskFormat,
    owner: &str,
    limit: usize,
    lease_duration: Duration,
) -> Result<Vec<LeasedTask>, TaskProcessingError> {
    if lease_duration.as_millis() == 0 {
        return Err(QueueRequestError::InvalidLeaseDuration.into());
    }
    // PostgreSQL grants its lease later, during UPDATE. Starting our local
    // clock before any await gives a conservative bound without UTC skew.
    let lease_deadline = Instant::now()
        .checked_add(lease_duration)
        .ok_or(QueueRequestError::InvalidLeaseDuration)?;
    let limit = i64::try_from(limit).map_err(|_| TaskProcessingError::from(QueueRequestError::InvalidLimit))?;
    let mut tx = queue.begin_queue_transaction("claim").await?;
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
        .collect::<Result<Vec<_>, TaskProcessingError>>()?;
    tx.commit().await.map_err(|error| storage_error(error, "claim"))?;
    Ok(tasks)
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
                assert_eq!(claimed.attempt().id(), id);
                assert_eq!(claimed.attempt().number(), 2);
                assert_eq!(claimed.attempt().payload(), &json!({"value": 1}));
                assert_eq!(claimed.lease().deadline(), deadline);
                assert_eq!(claimed.lease().lock_token(), lock_token);
            } else {
                assert!(matches!(result, Err(TaskProcessingError::InvalidDelivery)));
            }
        }
    }
}
