use crate::outbox::NewOutboxTask;
use cqrs_es::{
    Aggregate,
    persist::{PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot},
};
use postgres_es::PostgresEventRepository;
use serde_json::Value;
use sqlx::PgPool;
use thiserror::Error;
mod write;

/// Commit one accepted event, its optional snapshot, and immutable tasks atomically.
///
/// Multi-event batches, snapshot-only writes, and tasks without an event are
/// rejected. Bind a fresh repository/task list per command; share the pool.
/// Reads/replay delegate to postgres-es. Its streaming producer may terminate
/// on SQL failure without forwarding that error; EOF alone is not proof of
/// successful projection replay. This limitation is separate from task delivery.
pub struct PostgresEventRepositoryWithOutbox {
    pool: PgPool,
    reader: PostgresEventRepository,
    tasks: Vec<NewOutboxTask>,
}

impl PostgresEventRepositoryWithOutbox {
    pub fn new(pool: PgPool) -> Self {
        Self {
            reader: PostgresEventRepository::new(pool.clone()),
            pool,
            tasks: Vec::new(),
        }
    }

    /// Bind work to one command execution, never to a shared mutable repository.
    pub fn with_outbox(mut self, tasks: Vec<NewOutboxTask>) -> Self {
        self.tasks = tasks;
        self
    }

    async fn persist_command<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match write::WritePlan::new(events, snapshot_update, &self.tasks)? {
            write::WritePlan::NoOp => Ok(()),
            write::WritePlan::OneEvent {
                event,
                sequence,
                snapshot,
            } => write::persist_one::<A>(&self.pool, &self.tasks, event, sequence, snapshot).await,
        }
    }
}

impl PersistedEventRepository for PostgresEventRepositoryWithOutbox {
    async fn get_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.reader.get_events::<A>(aggregate_id).await
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.reader.get_last_events::<A>(aggregate_id, last_sequence).await
    }

    async fn get_snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.reader.get_snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        self.persist_command::<A>(events, snapshot_update).await
    }

    async fn stream_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<ReplayStream, PersistenceError> {
        self.reader.stream_events::<A>(aggregate_id).await
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        self.reader.stream_all_events::<A>().await
    }
}

#[derive(Debug, Error)]
enum WriteValidationError {
    #[error("a command may commit at most one domain event")]
    MultipleEvents,
    #[error("a snapshot requires a committed domain event")]
    SnapshotWithoutEvent,
    #[error("outbox tasks require a committed domain event")]
    TasksWithoutEvent,
    #[error("event sequence does not fit PostgreSQL bigint")]
    SequenceOverflow,
    #[error("write batch exceeds PostgreSQL's bind parameter limit")]
    BindLimitExceeded,
}
fn invalid_write(reason: WriteValidationError) -> PersistenceError {
    PersistenceError::UnknownError(Box::new(reason))
}
fn validate_bind_count(tasks: usize, snapshot_binds: usize) -> Result<(), PersistenceError> {
    const EVENT_BINDS: usize = 7;
    const TASK_BINDS: usize = 11;
    let count = tasks
        .checked_mul(TASK_BINDS)
        .and_then(|count| count.checked_add(EVENT_BINDS))
        .and_then(|count| count.checked_add(snapshot_binds));
    if count.is_some_and(|count| count < u16::MAX as usize) {
        Ok(())
    } else {
        Err(invalid_write(WriteValidationError::BindLimitExceeded))
    }
}
fn sequence_as_i64(sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(sequence).map_err(|_| invalid_write(WriteValidationError::SequenceOverflow))
}

fn map_sqlx_error(error: sqlx::Error) -> PersistenceError {
    let is_optimistic_conflict = matches!(
        &error,
        sqlx::Error::Database(database_error)
            if database_error.code().as_deref() == Some("23505")
                && matches!(database_error.constraint(), Some("events_pkey" | "snapshots_pkey"))
    );
    if is_optimistic_conflict {
        return PersistenceError::OptimisticLockError;
    }
    if matches!(&error, sqlx::Error::Io(_) | sqlx::Error::Tls(_)) {
        return PersistenceError::ConnectionError(Box::new(error));
    }
    PersistenceError::UnknownError(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn converts_supported_sequence() {
        assert_eq!(sequence_as_i64(42).expect("sequence must fit"), 42);
    }

    #[test]
    fn rejects_batches_over_the_postgres_bind_limit() {
        assert!(validate_bind_count(1, 6).is_ok());
        assert!(validate_bind_count(5_957, 0).is_ok());
        assert!(matches!(
            validate_bind_count(5_958, 0),
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(validate_bind_count(usize::MAX, 0).is_err());
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn rejects_overflowing_sequence() {
        let overflowing_sequence = usize::try_from(i64::MAX).expect("64-bit usize must fit i64::MAX") + 1;

        let result = sequence_as_i64(overflowing_sequence);

        assert!(matches!(result, Err(PersistenceError::UnknownError(_))));
    }

    #[test]
    fn maps_non_connection_database_errors_to_unexpected_errors() {
        let result = map_sqlx_error(sqlx::Error::RowNotFound);

        assert!(matches!(result, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn maps_io_errors_to_connection_errors() {
        let result = map_sqlx_error(sqlx::Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "test connection refused",
        )));

        assert!(matches!(result, PersistenceError::ConnectionError(_)));
    }
}
