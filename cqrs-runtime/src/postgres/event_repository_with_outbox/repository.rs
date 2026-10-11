//! CQRS repository adapter with per-command immutable task publication.
use super::{write, write_plan::WritePlan};
use crate::task::data::new_task::NewTask;
use cqrs_es::{
    Aggregate,
    persist::{PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent, SerializedSnapshot},
};
use postgres_es::PostgresEventRepository;
use serde_json::Value;
use sqlx::PgPool;
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
    tasks: Vec<NewTask>,
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
    pub fn with_outbox(mut self, tasks: Vec<NewTask>) -> Self {
        self.tasks = tasks;
        self
    }

    async fn persist_command<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        match WritePlan::new(events, snapshot_update, &self.tasks)? {
            WritePlan::NoOp => Ok(()),
            WritePlan::OneEvent {
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
