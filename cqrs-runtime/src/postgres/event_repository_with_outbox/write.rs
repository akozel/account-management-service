//! Compile a checked write into one atomic modifying CTE statement.
use super::write_plan::SnapshotWrite;
use crate::task::data::new_task::{NewTask, TaskSchedule};
use chrono::{DateTime, Utc};
use cqrs_es::{
    Aggregate,
    persist::{PersistenceError, SerializedEvent},
};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
fn append_snapshot<A: Aggregate>(snapshot: &SnapshotWrite, query: &mut QueryBuilder<'_, Postgres>, sequence: i64) {
    match snapshot {
        SnapshotWrite::None => {}
        SnapshotWrite::Insert {
            aggregate_id,
            payload,
            version,
        } => {
            query.push("written_snapshot AS (INSERT INTO snapshots (aggregate_type, aggregate_id, last_sequence, current_snapshot, payload) VALUES (");
            query
                .push_bind(A::TYPE)
                .push(", ")
                .push_bind(aggregate_id.clone())
                .push(", ")
                .push_bind(sequence)
                .push(", ")
                .push_bind(*version)
                .push(", ")
                .push_bind(payload.clone())
                .push(") RETURNING 1), ");
        }
        SnapshotWrite::Update {
            aggregate_id,
            payload,
            version,
        } => {
            query.push("written_snapshot AS (UPDATE snapshots SET last_sequence = ");
            query
                .push_bind(sequence)
                .push(", current_snapshot = ")
                .push_bind(*version)
                .push(", payload = ")
                .push_bind(payload.clone())
                .push(" WHERE aggregate_type = ")
                .push_bind(A::TYPE)
                .push(" AND aggregate_id = ")
                .push_bind(aggregate_id.clone())
                .push(" AND current_snapshot = ")
                .push_bind(*version - 1)
                .push(" RETURNING 1), ");
        }
    }
}
fn schedule_parts(schedule: &TaskSchedule) -> (Option<DateTime<Utc>>, f64) {
    match schedule {
        TaskSchedule::Now => (None, 0.0),
        TaskSchedule::At(at) => (Some(*at), 0.0),
        // The DTO constructor already checked this conversion.
        TaskSchedule::After(delay) => (None, delay.as_millis() as f64),
    }
}
pub(super) async fn persist_one<A: Aggregate>(
    pool: &PgPool,
    tasks: &[NewTask],
    event: &SerializedEvent,
    sequence: i64,
    snapshot: SnapshotWrite,
) -> Result<(), PersistenceError> {
    let has_snapshot = !matches!(snapshot, SnapshotWrite::None);
    let mut query = QueryBuilder::<Postgres>::new("WITH ");
    append_snapshot::<A>(&snapshot, &mut query, sequence);
    query.push("written_events AS (INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) ");
    if has_snapshot {
        query.push("SELECT * FROM (");
    }
    query.push_values(std::iter::once(event), |mut row, event| {
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
    if !tasks.is_empty() {
        query.push(", written_tasks AS (INSERT INTO transactional_outbox (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, available_at, max_attempts) ");
        query.push("SELECT id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, COALESCE(scheduled_at, statement_timestamp() + delay_ms * interval '1 millisecond'), max_attempts FROM (");
        query.push_values(tasks, |mut row, task| {
            let (at, delay_ms) = schedule_parts(task.schedule());
            row.push_bind(task.id())
                .push_bind(A::TYPE)
                .push_bind(&event.aggregate_id)
                .push_bind(sequence)
                .push_bind(task.format().task_type())
                .push_bind(task.format().task_version())
                .push_bind(task.payload())
                .push_bind(task.metadata())
                .push_bind(at)
                .push_bind(delay_ms)
                .push_bind(task.max_attempts());
        });
        query.push(") AS input(id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version, payload, metadata, scheduled_at, delay_ms, max_attempts) WHERE EXISTS (SELECT 1 FROM written_events) RETURNING id)");
    }
    match snapshot {
        SnapshotWrite::None => query.push(" SELECT 1::bigint AS snapshot_count"),
        _ => query.push(" SELECT COUNT(*)::bigint AS snapshot_count FROM written_snapshot"),
    };
    let row = query.build().fetch_one(pool).await.map_err(map_sqlx_error)?;
    match row.try_get::<i64, _>("snapshot_count").map_err(map_sqlx_error)? {
        1 => Ok(()),
        _ => Err(PersistenceError::OptimisticLockError),
    }
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

    #[test]
    fn tls_failures_preserve_the_source_as_connection_errors() {
        let result = map_sqlx_error(sqlx::Error::Tls(Box::new(std::io::Error::other("TLS handshake failed"))));
        let PersistenceError::ConnectionError(source) = result else {
            panic!("TLS failures must not become optimistic conflicts");
        };
        assert!(matches!(source.downcast_ref::<sqlx::Error>(), Some(sqlx::Error::Tls(_))));
    }
}
