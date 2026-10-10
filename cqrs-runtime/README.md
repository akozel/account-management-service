# cqrs-runtime

Reusable CQRS/event-sourcing, transactional outbox, and task-worker runtime.
It has no application-domain dependency and can be copied into another Rust
workspace as a path dependency. Copy the whole crate directory, including
`Cargo.toml`, `src/`, `sql/`, `doc/`, and `tests/`.
The crate requires Rust 1.98 or later and a Tokio runtime for async execution.

## Features

- Base: command execution, optimistic-conflict retry, durable task DTOs,
  handler routing, and task coordination.
- `postgres`: event repository, queue, and canonical SQL assets.
- `worker`: supervised Tokio polling, worker slots, and shutdown draining.

Both optional features are enabled by default. Use
`default-features = false` when only the base contracts are needed.

## Task integration

Read [Task processing](doc/task_processing.md) before implementing handlers or
another queue adapter. It is the complete contract for ownership, ordering,
idempotency, retry, configuration, timeouts, and leases.

Business handlers perform one operation and classify its result. They do not
manage `lease_deadline` or `lock_token`. Aggregates establish business
prerequisites; workers may execute published tasks concurrently and out of
order. Redelivery is expected, including after a successful effect.

The host applies [event-store SQL](sql/001-event-store.sql) and
[outbox SQL](sql/002-transactional-outbox.sql), supplies handler factories and
a database pool, and owns process signals and inbound-adapter draining.
The following composition function requires the `postgres` and `worker`
features and direct host dependencies on SQLx 0.8 and Tokio:

```rust
use std::{error::Error, sync::Arc};

use cqrs_runtime::{
    outbox::{OutboxTaskCoordinatorConfig, outbox_task_coordinator_with_config},
    postgres::{PostgresOutboxQueue, PostgresOutboxQueueConfig},
    worker::{self, OutboxWorkerPoolConfig, TaskHandlerFactory},
};
use sqlx::PgPool;
use tokio::sync::watch;

pub async fn run_tasks(
    pool: PgPool,
    factory: TaskHandlerFactory,
    shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let queue = Arc::new(PostgresOutboxQueue::with_config(
        pool,
        PostgresOutboxQueueConfig::from_env()?,
    )?);
    let coordinator = outbox_task_coordinator_with_config(
        queue,
        factory().formats(),
        OutboxTaskCoordinatorConfig::from_env()?,
    )?;
    let config = OutboxWorkerPoolConfig::from_env()?;
    config.validate_for_storage_timeout(coordinator.storage_timeout())?;

    worker::run(coordinator, factory, config, shutdown).await?;
    Ok(())
}
```

Every factory invocation must return a fresh registry with the same ordered
format list. Defaults do not read environment variables automatically: call
`from_env` or pass explicit configurations as above. The guide lists every
setting, its units/default, and the full lease-budget formula.

Generate local API documentation with:

```sh
cargo doc --all-features --no-deps
```

## PostgreSQL persistence and operational limits

`PostgresEventRepositoryWithOutbox` supports one domain event per command.
One event, its optional snapshot, and explicit tasks are committed atomically.
Multi-event batches, snapshot-only writes, and tasks without an event are
rejected; an empty batch without snapshot/tasks is a no-op. Supporting
multi-event commands requires a separate snapshot-contract change.

CQRS fast retries apply only to optimistic conflicts. Connection,
deserialization, and unexpected framework failures map to the gateway's
`Unavailable` result and are not retried by the command executor. Task handlers
classify these outcomes for their own replay-safe operations.

Replay streaming delegates to the current `postgres-es` 0.5.0 producer, whose
background SQL-read failure can close the feed without forwarding a typed
error. EOF alone is not proof that projection replay completed successfully.
This streaming limitation is separate from normal command reads and task
delivery; it must be addressed before relying on failure-safe replay.

The queue does not purge/archive terminal tasks. A host must define retention
and manual-recovery policies that preserve required failed work and the
receiver's idempotency window.
