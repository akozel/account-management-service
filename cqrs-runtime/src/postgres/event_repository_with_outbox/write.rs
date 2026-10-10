//! Validated single-event plan, compiled into one atomic modifying CTE statement.
use super::{WriteValidationError, invalid_write, map_sqlx_error, sequence_as_i64, validate_bind_count};
use crate::outbox::{NewOutboxTask, TaskSchedule};
use chrono::{DateTime, Utc};
use cqrs_es::{
    Aggregate,
    persist::{PersistenceError, SerializedEvent},
};
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

pub(super) enum WritePlan<'a> {
    NoOp,
    OneEvent {
        event: &'a SerializedEvent,
        sequence: i64,
        snapshot: SnapshotWrite,
    },
}
pub(super) enum SnapshotWrite {
    None,
    Insert {
        aggregate_id: String,
        payload: Value,
        version: i64,
    },
    Update {
        aggregate_id: String,
        payload: Value,
        version: i64,
    },
}
impl SnapshotWrite {
    fn new(update: Option<(String, Value, usize)>) -> Result<Self, PersistenceError> {
        match update {
            None => Ok(Self::None),
            Some((aggregate_id, payload, version)) => {
                let version = sequence_as_i64(version)?;
                Ok(if version == 1 {
                    Self::Insert {
                        aggregate_id,
                        payload,
                        version,
                    }
                } else {
                    Self::Update {
                        aggregate_id,
                        payload,
                        version,
                    }
                })
            }
        }
    }
    fn bind_count(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Insert { .. } => 5,
            Self::Update { .. } => 6,
        }
    }
    fn append<A: Aggregate>(&self, query: &mut QueryBuilder<'_, Postgres>, sequence: i64) {
        match self {
            Self::None => {}
            Self::Insert {
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
            Self::Update {
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
}
impl<'a> WritePlan<'a> {
    pub(super) fn new(
        events: &'a [SerializedEvent],
        snapshot: Option<(String, Value, usize)>,
        tasks: &[NewOutboxTask],
    ) -> Result<Self, PersistenceError> {
        match events {
            [] if snapshot.is_some() => Err(invalid_write(WriteValidationError::SnapshotWithoutEvent)),
            [] if !tasks.is_empty() => Err(invalid_write(WriteValidationError::TasksWithoutEvent)),
            [] => Ok(Self::NoOp),
            [event] => {
                let snapshot = SnapshotWrite::new(snapshot)?;
                validate_bind_count(tasks.len(), snapshot.bind_count())?;
                Ok(Self::OneEvent {
                    event,
                    sequence: sequence_as_i64(event.sequence)?,
                    snapshot,
                })
            }
            _ => Err(invalid_write(WriteValidationError::MultipleEvents)),
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
    tasks: &[NewOutboxTask],
    event: &SerializedEvent,
    sequence: i64,
    snapshot: SnapshotWrite,
) -> Result<(), PersistenceError> {
    let has_snapshot = !matches!(snapshot, SnapshotWrite::None);
    let mut query = QueryBuilder::<Postgres>::new("WITH ");
    snapshot.append::<A>(&mut query, sequence);
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
