# cqrs-runtime

Reusable CQRS/event-sourcing, transactional outbox, and task-worker runtime.
It has no application-domain dependency and can be copied into another Rust
workspace as a path dependency. Copy the whole crate directory, including
`Cargo.toml`, `src/`, `sql/`, `doc/`, and `tests/`.
The crate requires Rust 1.98 or later and a Tokio runtime for async execution.

See [Architecture and call flow](doc/architecture.md) for component
responsibilities and Mermaid diagrams of command publication and task delivery.

## Features

- Base: command execution, optimistic-conflict retry, durable task DTOs,
  handler routing, and task coordination.
- `postgres`: checked command factory, event repository, queue, and canonical SQL assets.
- `worker`: JoinSet-supervised permanent Tokio workers, a bounded async-channel queue, and shutdown draining.

Both optional features are enabled by default. Use
`default-features = false` when only the base contracts are needed.

## Command integration

The host's aggregate gateway selects the aggregate ID and delegates to
`Arc<dyn command::CommandExecutor<A>>`. With the `postgres` feature, assemble
the standard snapshot-backed executor through `postgres::command_executor`:

```rust
use std::sync::Arc;
use cqrs_es::Aggregate;
use cqrs_runtime::{
    command::CommandExecutor,
    postgres::{PostgresCommandConfig, command_executor},
};
use sqlx::PgPool;

fn executor_for<A>(pool: PgPool) -> Arc<dyn CommandExecutor<A>>
where
    A: Aggregate<Services = ()> + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    command_executor::<A>(pool, PostgresCommandConfig::default())
}
```

The default snapshot interval is 25 and the existing optimistic-conflict policy
allows three total attempts. `PostgresCommandConfig::new(interval, policy)`
rejects a zero interval; fields are private and exposed through getters.
Neither configuration nor factory reads the environment. The factory performs
no I/O, so it returns the executor directly, without `Result`.

Each command creates one framework and a fresh repository carrying only that
call's immutable tasks. Conflict retries reuse the framework and reload state;
concurrent calls share only the pool. The ready-made factory uses unit services
and no queries. For custom stores, services, or queries, retain
`CqrsCommandExecutor::new(build_framework, retry_policy)` and inject that executor
through the same trait. The host still chooses business dependencies explicitly.

The current cqrs-es snapshot path reapplies accepted events to already changed
state. Aggregate `apply` must tolerate that repeat, as the service's state
assignments do. Increment/append aggregates need a separate snapshot-contract
fix before using this factory; see the
[snapshot reapplication issue](../doc/known_issues/cqrs_snapshot_event_reapplication.md).

## Task integration

Read [Task processing](doc/task_processing.md) before implementing handlers or
another queue adapter. It is the complete contract for ownership, ordering,
idempotency, retry, configuration, timeouts, and leases.

Business handlers perform one operation through `TaskDelivery` and classify its
result with `TaskHandlerError`. They cannot access the queue's `ClaimLease`.
`ClaimedTask` is reserved for queue/coordinator ownership and settlement. Aggregates establish business
prerequisites; workers may execute published tasks concurrently and out of
order. Redelivery is expected, including after a successful effect.

The host applies [event-store SQL](sql/001-event-store.sql) and
[outbox SQL](sql/002-transactional-outbox.sql), supplies handler factories and
a database pool, and owns process signals and inbound-adapter draining.
Load runtime and queue configs before connecting the pool. The following
preparation function requires the `postgres` and `worker` features and a host
dependency on SQLx 0.8. Start its returned object only after binding listeners,
using Tokio and tokio-util 0.7:

```rust
use cqrs_runtime::{
    postgres::{PostgresOutboxQueueConfig, PostgresOutboxPreparationError, prepare_outbox_worker},
    worker::{OutboxRuntimeConfig, HandlerRegistryFactory, PreparedOutboxWorker},
};
use sqlx::PgPool;

pub fn prepare_tasks(
    pool: PgPool,
    factory: HandlerRegistryFactory,
    queue_config: PostgresOutboxQueueConfig,
    runtime_config: OutboxRuntimeConfig,
) -> Result<PreparedOutboxWorker, PostgresOutboxPreparationError> {
    prepare_outbox_worker(pool, factory, queue_config, runtime_config)
}

```

The host shares a `CancellationToken` with HTTP and the outbox supervisor. Call
`cancel()` and await the supervisor for graceful drain. Fatal supervisor errors
cancel the supplied token before drain, so HTTP can stop admission immediately.
Pass a `child_token()` for an isolated pool. Dropping a token handle does not
cancel it; a lifecycle owner can hold `shutdown.clone().drop_guard()`.

Prepare before binding inbound listeners. `worker::prepare(queue, factory, config)`
works with any queue; `postgres::prepare_outbox_worker` assembles the PostgreSQL
adapter when both `postgres` and `worker` features are enabled. Neither function
performs storage I/O or starts background tasks. Preparation calls the factory
exactly C times, freezes ordered formats, and keeps the first registry for worker
zero. Every fallible factory invocation must return a fresh registry with the same ordered
format list. Defaults do not read environment variables automatically: call
`from_env` or pass explicit configurations as above. The guide lists every
setting, its units/default, and the full lease-budget formula.

`OutboxRuntimeConfig` combines the existing coordinator and worker configs,
checks the complete lease budget at construction/load time, and keeps PostgreSQL
statement timeout separate. The service loads configs before connecting to the
database. `PreparedOutboxWorker` is consumed by `run`; it rebuilds registries only
on restart, preserving each worker's mutable state. Dropping the prepared object
releases handlers without cancellation. A polled run owns a cancellation DropGuard;
a pre-cancelled token starts no tasks or storage operations.

Configuration errors (`OutboxRuntimeConfigError`), preparation errors
(`OutboxPreparationError` / `PostgresOutboxPreparationError`) and runtime exits
(`WorkerPoolError`) remain distinct. Low-level `worker::run` is retained for
custom coordinators; it uses the same supervisor engine and preserves cancelled
startup and actual-coordinator budget validation.

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

## Code organization and validated APIs

The public namespaces are `command`, `outbox`, `postgres`, and `worker`;
their child modules are private implementation details. Command execution and
conflict retry are separate. Outbox modules group wire data, delivery/ownership,
handler routing, queue transitions, and coordination. The worker groups combined config, startup preparation,
supervision, and permanent worker lifecycle. PostgreSQL keeps queue SQL separate from the
single-event write plan and its atomic CTE compiler.

Unit tests stay in inline `#[cfg(test)] mod tests { ... }` blocks alongside
the code they exercise; shared runtime fakes live in `outbox/test_support.rs`.
PostgreSQL integration suites remain under `tests/postgres/`.
Worker unit tests include a 1,000-delivery run through the supervisor and coordinator
with test storage, checking every handler invocation ID and its Completed settlement.
Queue limits, cancellation, restart and shutdown have separate behavioral checks.

`TaskFormat::new/of`, `NewOutboxTask` builders, and configuration constructors
return typed validation errors. Their fields are private: use getters and
checked builders, not struct literals. `AggregateConflictRetryPolicy::new` is
also fallible; only representable backoff budgets are accepted.

`SettlementRequest` expresses intent; `SettlementResult` distinguishes confirmed
completion, scheduled retry, failure, and lost ownership. `TaskFailureCause`
provides fixed safe log reasons, independent of persisted handler error text.
`LeaseRecoveryReport` derives its total from retries and confirmed failures.

`HandlerRegistryFactory` returns `Result<TaskHandlerRegistry, TaskHandlerRegistryError>`.
`PreparedOutboxWorker::run` reports `WorkerPoolError` for runtime failures.
Low-level `worker::run` also reports configuration and registry errors on startup.
Preparation reports typed errors before any workers start. It still supervises
one mutable registry per permanent Tokio task and leaves interrupted work for recovery.
Defaults are 128 concurrent deliveries, 128 queued deliveries, and a 60-second
lease. `OUTBOX_QUEUE_CAPACITY` and `OUTBOX_WORKER_CONCURRENCY` are independent.
The explicit worker config constructor now takes `queue_capacity` after
`concurrency`; the combined lease validation includes queued waves.

The service host uses handler/storage/statement budgets of 15s/6s/4s. With
C=Q=128, lease=60s exceeds the validated 53s minimum. HTTP/DB shutdown limits
remain [known issue #10](../doc/known_issues/http_shutdown.md).

For the host's current lifecycle and the planned 28s envelope, see the
[graceful shutdown timeline](../README.md#graceful-shutdown-timeline).
