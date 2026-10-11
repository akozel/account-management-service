//! PostgreSQL queue assembly, shared transaction settings and operation delegation.
use crate::task::{
    data::format::TaskFormat,
    queue::{
        error::{QueueRequestError, TaskProcessingError},
        lease::LeasedTask,
        port::TaskQueue,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
    },
};
use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Transaction};
use std::time::Duration;
use {
    super::{claim, recovery, settlement},
    crate::postgres::task_queue::config::PostgresTaskQueueConfig,
};

pub struct PostgresTaskQueue {
    pool: PgPool,
    statement_timeout: Duration,
}

impl PostgresTaskQueue {
    pub fn new(pool: PgPool) -> Self {
        Self::with_config(pool, PostgresTaskQueueConfig::default())
    }

    pub fn with_config(pool: PgPool, config: PostgresTaskQueueConfig) -> Self {
        Self {
            pool,
            statement_timeout: config.statement_timeout(),
        }
    }

    pub(super) async fn begin_queue_transaction(
        &self,
        operation: &'static str,
    ) -> Result<Transaction<'_, Postgres>, TaskProcessingError> {
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

pub(super) fn storage_error(error: sqlx::Error, operation: &'static str) -> TaskProcessingError {
    crate::diagnostics::log_sqlx_failure(operation, &error);
    TaskProcessingError::Unavailable
}

pub(super) fn milliseconds(duration: Duration, invalid: QueueRequestError) -> Result<f64, TaskProcessingError> {
    i64::try_from(duration.as_millis())
        .map(|n| n as f64)
        .map_err(|_| TaskProcessingError::from(invalid))
}

#[async_trait]
impl TaskQueue for PostgresTaskQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        claim::claim(self, format, owner, limit, lease_duration).await
    }
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        settlement::settle(self, task, request).await
    }
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        recovery::recover_expired(self, limit).await
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
        let queue = PostgresTaskQueue::new(pool);
        let task = crate::task::test_support::claimed();
        let (_, log) = crate::diagnostics::test_support::capture(async {
            assert_eq!(
                queue
                    .claim(task.attempt().format(), "private_owner", 1, Duration::from_secs(60))
                    .await
                    .unwrap_err(),
                TaskProcessingError::Unavailable
            );
            assert_eq!(
                queue.settle(&task, SettlementRequest::Completed).await.unwrap_err(),
                TaskProcessingError::Unavailable
            );
            assert_eq!(queue.recover_expired(1).await.unwrap_err(), TaskProcessingError::Unavailable);
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), 3);
        for operation in ["claim", "settle", "recover_expired"] {
            assert!(log.contains(operation));
        }
        assert!(log.contains("pool_closed"));
        assert!(!log.contains("private_"));
    }
}
