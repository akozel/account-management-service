use super::{PostgresOutboxQueue, PostgresOutboxQueueConfig, PostgresOutboxQueueConfigError};
use crate::worker::{HandlerRegistryFactory, OutboxPreparationError, OutboxRuntimeConfig, PreparedOutboxWorker, prepare};
use sqlx::PgPool;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PostgresOutboxPreparationError {
    #[error("{0}")]
    QueueConfiguration(#[from] PostgresOutboxQueueConfigError),
    #[error("{0}")]
    Preparation(#[from] OutboxPreparationError),
}

/// Assemble the queue, coordinator and all startup registries before opening
/// listeners. Does not connect, execute SQL, read env or start background tasks.
/// The host chooses handlers; the returned object starts only when `run` is polled.
pub fn prepare_outbox_worker(
    pool: PgPool,
    factory: HandlerRegistryFactory,
    queue_config: PostgresOutboxQueueConfig,
    runtime_config: OutboxRuntimeConfig,
) -> Result<PreparedOutboxWorker, PostgresOutboxPreparationError> {
    let queue = Arc::new(PostgresOutboxQueue::with_config(pool, queue_config)?);
    Ok(prepare(queue, factory, runtime_config)?)
}
