use super::{PostgresOutboxQueue, PostgresOutboxQueueConfig};
use crate::worker::{HandlerRegistryFactory, OutboxRuntimeConfig, PreparedOutboxRuntime, WorkerRegistryError, prepare};
use sqlx::PgPool;
use std::sync::Arc;

/// Assemble the outbox runtime's queue, coordinator and all startup registries before opening
/// listeners. Does not connect, execute SQL, read env or start background tasks.
/// The host chooses handlers; the returned object starts only when `run` is polled.
pub fn prepare_outbox_runtime(
    pool: PgPool,
    factory: HandlerRegistryFactory,
    queue_config: PostgresOutboxQueueConfig,
    runtime_config: OutboxRuntimeConfig,
) -> Result<PreparedOutboxRuntime, WorkerRegistryError> {
    let queue = Arc::new(PostgresOutboxQueue::with_config(pool, queue_config));
    prepare(queue, factory, runtime_config)
}
