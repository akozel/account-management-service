use crate::application::outbox::{NewOutboxTask, OutboxError, Schedule};
use cqrs_es::{
    Aggregate,
    persist::{PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot},
};
use postgres_es::PostgresEventRepository;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use thiserror::Error;

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

    // PostgreSQL executes all modifying CTEs as one atomic statement. RETURNING
    // carries the snapshot lock result to event insertion and written events
    // to outbox insertion; sibling CTEs cannot inspect each other's table writes.
    async fn persist_batch<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        if events.is_empty() && snapshot_update.is_none() && self.tasks.is_empty() {
            return Ok(());
        }
        if events.is_empty() && !self.tasks.is_empty() {
            return Err(PersistenceError::UnknownError(Box::new(std::io::Error::other(
                "outbox requires a committed domain event",
            ))));
        }
        let snapshot_binds = snapshot_update
            .as_ref()
            .map(|(_, _, version)| if *version == 1 { 5 } else { 6 })
            .unwrap_or_default();
        validate_bind_count(events.len(), self.tasks.len(), snapshot_binds)?;

        // Validate conversions before sending any write to PostgreSQL.
        let event_sequences = events
            .iter()
            .map(|event| sequence_as_i64(event.sequence))
            .collect::<Result<Vec<_>, _>>()?;
        let schedules = self
            .tasks
            .iter()
            .map(|task| match task.schedule {
                Schedule::Now => Ok((None, 0_i64)),
                Schedule::At(at) => Ok((Some(at), 0)),
                Schedule::After(delay) => i64::try_from(delay.as_millis())
                    .map(|millis| (None, millis))
                    .map_err(|_| PersistenceError::UnknownError(Box::new(OutboxError::InvalidTask) as _)),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let has_snapshot = snapshot_update.is_some();
        let mut query = QueryBuilder::<Postgres>::new("WITH ");
        if let Some((aggregate_id, aggregate, current_snapshot)) = snapshot_update {
            let last_sequence = event_sequences.last().copied().unwrap_or_default();
            let current_snapshot = sequence_as_i64(current_snapshot)?;
            query.push("written_snapshot AS (");
            if current_snapshot == 1 {
                query.push(
                    "INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload) VALUES (",
                );
                query.push_bind(A::TYPE).push(", ");
                query.push_bind(aggregate_id).push(", ");
                query.push_bind(last_sequence).push(", ");
                query.push_bind(current_snapshot).push(", ");
                query.push_bind(aggregate);
                query.push(") RETURNING 1");
            } else {
                query.push("UPDATE snapshots SET last_sequence = ");
                query.push_bind(last_sequence).push(", current_snapshot = ");
                query.push_bind(current_snapshot).push(", payload = ");
                query.push_bind(aggregate).push(" WHERE aggregate_type = ");
                query.push_bind(A::TYPE).push(" AND aggregate_id = ");
                query.push_bind(aggregate_id).push(" AND current_snapshot = ");
                query.push_bind(current_snapshot - 1).push(" RETURNING 1");
            }
            query.push(")");
        }

        if !events.is_empty() {
            if has_snapshot {
                query.push(", ");
            }
            query.push("written_events AS (INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) ");
            if has_snapshot {
                query.push("SELECT * FROM (");
            }
            query.push_values(events.iter().zip(&event_sequences), |mut row, (event, sequence)| {
                row.push_bind(A::TYPE)
                    .push_bind(&event.aggregate_id)
                    .push_bind(sequence)
                    .push_bind(&event.event_type)
                    .push_bind(&event.event_version)
                    .push_bind(&event.payload)
                    .push_bind(&event.metadata);
            });
            if has_snapshot {
                query.push(") AS input(aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) WHERE EXISTS (SELECT 1 FROM written_snapshot)");
            }
            query.push(" RETURNING sequence)");
        }

        if !self.tasks.is_empty() {
            let event = events.last().expect("outbox events were validated above");
            let last_sequence = *event_sequences.last().expect("outbox events were validated above");
            query.push(", written_tasks AS (INSERT INTO transactional_outbox (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, available_at, max_attempts) ");
            query.push("SELECT id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, COALESCE(scheduled_at, statement_timestamp() + delay_ms * interval '1 millisecond'), max_attempts FROM (");
            query.push_values(self.tasks.iter().zip(&schedules), |mut row, (task, (at, delay))| {
                row.push_bind(task.id)
                    .push_bind(A::TYPE)
                    .push_bind(&event.aggregate_id)
                    .push_bind(last_sequence)
                    .push_bind(&task.task_type)
                    .push_bind(task.task_version)
                    .push_bind(&task.payload)
                    .push_bind(&task.metadata)
                    .push_bind(*at)
                    .push_bind(*delay as f64)
                    .push_bind(task.max_attempts);
            });
            query.push(") AS input(id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, scheduled_at, delay_ms, max_attempts) WHERE EXISTS (SELECT 1 FROM written_events) RETURNING id)");
        }

        if has_snapshot {
            query.push(" SELECT COUNT(*)::bigint AS snapshot_count FROM written_snapshot");
        } else {
            query.push(" SELECT 1::bigint AS snapshot_count");
        }
        let row = query.build().fetch_one(&self.pool).await.map_err(map_sqlx_error)?;
        let snapshot_count: i64 = row.try_get("snapshot_count").map_err(map_sqlx_error)?;
        if snapshot_count == 1 {
            Ok(())
        } else {
            Err(PersistenceError::OptimisticLockError)
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
        self.persist_batch::<A>(events, snapshot_update).await
    }

    async fn stream_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<ReplayStream, PersistenceError> {
        self.reader.stream_events::<A>(aggregate_id).await
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        self.reader.stream_all_events::<A>().await
    }
}

#[derive(Debug, Error)]
#[error("event sequence does not fit PostgreSQL bigint")]
struct SequenceOverflow;

#[derive(Debug, Error)]
#[error("write batch exceeds PostgreSQL's bind parameter limit")]
struct BatchTooLarge;

fn validate_bind_count(events: usize, tasks: usize, snapshot_binds: usize) -> Result<(), PersistenceError> {
    let count = events
        .checked_mul(7)
        .and_then(|count| tasks.checked_mul(11).and_then(|tasks| count.checked_add(tasks)))
        .and_then(|count| count.checked_add(snapshot_binds));
    if count.is_some_and(|count| count < u16::MAX as usize) {
        Ok(())
    } else {
        Err(PersistenceError::UnknownError(Box::new(BatchTooLarge)))
    }
}

fn sequence_as_i64(sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(sequence).map_err(|_| PersistenceError::UnknownError(Box::new(SequenceOverflow)))
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
        assert!(validate_bind_count(1, 1, 6).is_ok());
        assert!(validate_bind_count(0, 5_957, 0).is_ok());
        assert!(matches!(
            validate_bind_count(0, 5_958, 0),
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(validate_bind_count(usize::MAX, 1, 0).is_err());
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
