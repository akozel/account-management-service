use crate::postgres::task_queue::{config::PostgresTaskQueueConfig, queue::PostgresTaskQueue};
use crate::runtime::{
    config::TaskRuntimeConfig,
    dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
    preparation::{TaskRuntime, prepare},
};
use sqlx::PgPool;
use std::sync::Arc;

/// Assemble the task queue, executor and all startup dispatchers before opening
/// listeners. Does not connect, execute SQL, read env or start background tasks.
/// The host chooses handlers; the returned object starts only when `run` is polled.
pub fn prepare_task_runtime(
    pool: PgPool,
    factory: TaskDispatcherFactory,
    queue_config: PostgresTaskQueueConfig,
    runtime_config: TaskRuntimeConfig,
) -> Result<TaskRuntime, WorkerSetupError> {
    let queue = Arc::new(PostgresTaskQueue::with_config(pool, queue_config));
    prepare(queue, factory, runtime_config)
}
