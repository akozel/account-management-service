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
  typed handler adaptation, handler routing, and task coordination.
- `postgres`: checked command factory, event repository, queue, and canonical SQL assets.
- `worker`: JoinSet-supervised permanent Tokio workers, a bounded async-channel queue, and shutdown draining.
- `host`: process signals, shared cancellation and inbound/outbox draining; enables `worker` and `tokio/signal`, with no HTTP framework dependency.
- `logging`: explicit fmt logger configuration and fallible installation; independent of other optional features.

`postgres` and `worker` are enabled by default; `host` and `logging` are opt-in.
The base crate emits structured `tracing` diagnostics; callers choose the subscriber. Use
`default-features = false` when only the base contracts are needed.

## Startup and logger integration

The application owns dotenv loading and service settings. In this service, the
binary snapshots supported env keys, rejects non-Unicode values, requires
`DATABASE_URL` and numeric IPv4/IPv6 `BIND_ADDRESS`, and validates all runtime
settings before installing its logger or connecting to PostgreSQL. Missing `.env`
is allowed; other dotenv errors stop startup. No library import installs a
subscriber or loads `.env`.

With `features = ["logging"]`, `logging::LoggingConfig` owns a checked
`tracing::level_filters::LevelFilter`. `new` and `Default` read no environment;
`from_lookup` and `from_env` explicitly load `RUST_LOG`, defaulting to `info` when
absent. Names are case-insensitive; numeric aliases 0–5 and empty input (`error`)
retain the original LevelFilter parser. EnvFilter directives are rejected.

```rust,no_run
use cqrs_runtime::logging::LoggingConfig;

let config = LoggingConfig::from_env()?;
config.try_init()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`try_init` explicitly installs the fmt subscriber with target disabled and thread
names enabled, preserving the other formatting defaults and log compatibility
bridge. It returns `LoggingInitError` if installation fails, including an existing
global subscriber/logger; the original `TryInitError` is retained as source with
safe Display/Debug. This feature installs only a logger.

With `postgres`, `postgres::connect_pool(PgConnectOptions)` returns
`Result<PgPool, sqlx::Error>`, using ten connections and other SQLx defaults.
Parse options before logger setup and connection; the helper accepts ready
options, adds no custom retry policy, and returns the original connection error.
`PgConnectOptions` contains credentials in Debug: callers must redact it and SQLx
error text in their own startup reporting, as the service's private config/error
modules do. Driver PG environment/pgpass option behavior remains SQLx's.

After successful connection, the service awaits all setup/host work and closes
the pool on success or preparation/bind/host error before logging the result.
Cleanup still has no timeout; Drop/abort cannot guarantee it. See the
[startup implementation plan](../doc/backlog/cqrs_runtime_configuration_logging.md).

### Safe technical failure diagnostics

The command executor emits one error event for a terminal technical failure
before mapping it to `Unavailable`: operation `execute`, sanitized aggregate
type, and framework category `connection`, `deserialization` or `unexpected`.
Domain rejections and optimistic conflicts keep their original classification
and retry policy without these error events. PostgreSQL source chains can add
fixed storage category and SQLSTATE.

The PostgreSQL adapter emits operation `connect`, `claim`, `settle` or
`recover_expired` and a fixed storage category: `io`, `tls`, `database`,
`pool_timeout`, `pool_closed`, `decode`, `configuration`, or `other`. Database codes
are emitted only when they are five uppercase ASCII letters/digits. Aggregate
types use ASCII letters/digits/underscore, otherwise `unknown` for diagnostics.
Error text, database message/detail/query/constraint, command, task payload,
metadata, aggregate ID/email and connection URL are omitted. Existing worker
summary events remain; public errors and unknown commit semantics are unchanged.

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

Use `outbox::TypedTaskHandler<P>` for one durable DTO version and adapt it through
`outbox::typed_task_handler::<P, _>(handler)`. The helper returns
`Result<Box<dyn TaskHandler>, TaskFormatError>` for the existing registry. It is
part of the base API, with no optional feature requirement.

```rust
use async_trait::async_trait;
use cqrs_runtime::outbox::{
    TaskDelivery, TaskFormatError, TaskHandler, TaskHandlerError, TaskPayload,
    TypedTaskHandler, typed_task_handler,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct InvoiceTaskV1 { invoice_id: String }
impl TaskPayload for InvoiceTaskV1 {
    const TASK_TYPE: &'static str = "process_invoice";
    const TASK_VERSION: i32 = 1;
}

struct InvoiceHandler;
#[async_trait]
impl TypedTaskHandler<InvoiceTaskV1> for InvoiceHandler {
    async fn handle(&mut self, payload: InvoiceTaskV1, delivery: &TaskDelivery)
        -> Result<(), TaskHandlerError>
    {
        // Invoke one replay-safe business operation using payload.invoice_id.
        // delivery.id()/metadata() carry any required stable operation context.
        let _ = (payload.invoice_id, delivery.id());
        Ok(())
    }
}

fn handler() -> Result<Box<dyn TaskHandler>, TaskFormatError> {
    typed_task_handler::<InvoiceTaskV1, _>(InvoiceHandler)
}
```

The adapter checks the expected format at construction, then compares format and
decodes an owned DTO from borrowed JSON before each invocation. Payloads and
handlers need `Send`, not `Clone` or `Sync`; the original `&TaskDelivery` and
mutable handler state are preserved. Format/decode failures are `Permanent`
with sanitized expected type/version, without payload, metadata or serde error
text. Business errors, including backoff, pass unchanged. Additional fields
follow the DTO's serde policy; current service v1 DTOs keep accepting them.
Use separate adapters for v1/v2 or implement `TaskHandler` for a multi-version
handler. Business validation, DTO conversion and exact-repeat classification
remain application responsibilities. Sender ports may borrow `&TaskDelivery`
throughout async work instead of constructing an owned context or cloning
metadata; lease credentials remain inaccessible.

The host applies [event-store SQL](sql/001-event-store.sql) and
[outbox SQL](sql/002-transactional-outbox.sql), supplies handler factories and
a database pool. The optional `host` facade owns process signals and
inbound-adapter draining; the application constructs its inbound adapter and
closes the pool after the host returns.
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

## Process lifecycle integration

Enable `features = ["host"]` and give the inbound adapter and outbox the same
`CancellationToken`. The inbound future returns `io::Result<()>`; it may borrow
local state and need not be `Send`. The outbox future returns
`Result<(), worker::WorkerPoolError>` and must be `Send + 'static`, because host
spawns it. Signal futures can also borrow local state and need not be `Send`.

```rust,no_run
use std::{future::Future, io};
use cqrs_runtime::{host::{self, HostError}, worker::WorkerPoolError};
use tokio_util::sync::CancellationToken;

async fn serve<Inbound, Outbox>(
    inbound: Inbound, outbox: Outbox, shutdown: CancellationToken,
) -> Result<(), HostError>
where
    Inbound: Future<Output = io::Result<()>>,
    Outbox: Future<Output = Result<(), WorkerPoolError>> + Send + 'static,
{
    host::run(inbound, outbox, shutdown, host::shutdown_signal()).await
}
```

For a prepared worker, pass `outbox.run(shutdown.clone())` directly to `host::run`.
The host installs its cancellation DropGuard before spawning outbox on first
poll; simply creating or dropping an unpolled host future starts nothing and
leaves the token unchanged. `shutdown_signal` observes SIGINT/SIGTERM on Unix
and Ctrl-C elsewhere, returning observation errors, including a closed SIGTERM
stream.

Stop observation uses biased priority: signal → token → inbound exit → outbox
exit. The host cancels the token and awaits both components. An unexpected exit
remains the primary error; requested shutdown prefers signal observation, outbox,
then inbound errors. Secondary failures are logged. `HostError::log` and `Debug`
render task failures as task ID and panic/cancellation flags, preserving the
original `JoinError` as a source without rendering its panic payload.

Normal return happens after both components drain; the caller then closes the
pool even on error. Dropping/aborting a polled host cancels the token and detaches
the outbox task so its independent drain can finish while Tokio remains running.
The inbound future is dropped, and the host cannot await components or guarantee
resource cleanup. Outbox retains its H+S budget; inbound drain and pool closure
have no host timeout ([known issue #10](../doc/known_issues/http_shutdown.md)).

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

The public namespaces are `command`, `outbox`, `postgres`, `worker`, `host`, and `logging`;
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
