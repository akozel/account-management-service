# cqrs-runtime

Reusable CQRS/event-sourcing, transactional outbox, and task-worker runtime.
It has no application-domain dependency and can be copied into another Rust
workspace as a path dependency. Copy the whole crate directory, including
`Cargo.toml`, `src/`, `sql/`, `doc/`, and `tests/`.
The crate requires Rust 1.98 or later and a Tokio runtime for async execution.

See [Architecture and call flow](doc/architecture.md) for component
responsibilities and Mermaid diagrams of command publication and task delivery.

See [Components and terminology](doc/components.md) for the role tree and naming rules.

## Features

- Base: command execution, optimistic-conflict retry, durable task DTOs,
  typed handler adaptation, handler routing, and task execution.
- `postgres`: checked command factory, event repository, queue, and canonical SQL assets.
- `worker`: JoinSet-supervised permanent Tokio workers, a bounded async-channel queue, and shutdown draining.
- `host`: process signals, shared cancellation and inbound/task-runtime draining; enables `worker` and `tokio/signal`, with no HTTP framework dependency.
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

With `postgres`, `postgres::pool::connect_pool(PgConnectOptions)` returns
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
the standard snapshot-backed executor through `postgres::command_factory::command_executor`:

```rust
use cqrs_es::Aggregate;
use cqrs_runtime::{
    command::CommandExecutor,
    postgres::command_factory::{PostgresCommandConfig, command_executor},
};
use sqlx::PgPool;
use std::sync::Arc;

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

Business handlers perform one operation through `TaskAttempt` and classify its
result with `TaskHandlerError`. They cannot access the queue's `TaskLease`.
`LeasedTask` is reserved for queue/executor ownership and settlement. Aggregates establish business
prerequisites; workers may execute published tasks concurrently and out of
order. Redelivery is expected, including after a successful effect.

Use `task::handling::handler::TaskHandler<P>` for one durable DTO version and adapt it through
`task::handling::json_adapter::task_adapter::<P, _>(handler)`. The helper returns
`Result<Box<dyn TaskAdapter>, TaskFormatError>` for the existing dispatcher. It is
part of the base API, with no optional feature requirement.

```rust
use async_trait::async_trait;
use cqrs_runtime::task::{
    data::{
        attempt::TaskAttempt,
        format::{TaskFormatError, TaskPayload},
    },
    handling::{
        adapter::TaskAdapter,
        handler::{TaskHandler, TaskHandlerError},
        json_adapter::task_adapter,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct InvoiceTaskV1 {
    invoice_id: String,
}
impl TaskPayload for InvoiceTaskV1 {
    const TASK_TYPE: &'static str = "process_invoice";
    const TASK_VERSION: i32 = 1;
}

struct InvoiceHandler;
#[async_trait]
impl TaskHandler<InvoiceTaskV1> for InvoiceHandler {
    async fn handle(&mut self, payload: InvoiceTaskV1, delivery: &TaskAttempt) -> Result<(), TaskHandlerError> {
        // Invoke one replay-safe business operation using payload.invoice_id.
        // delivery.id()/metadata() carry any required stable operation context.
        let _ = (payload.invoice_id, delivery.id());
        Ok(())
    }
}

fn invoice_adapter() -> Result<Box<dyn TaskAdapter>, TaskFormatError> {
    task_adapter::<InvoiceTaskV1, _>(InvoiceHandler)
}
```

The adapter checks the expected format at construction, then compares format and
decodes an owned DTO from borrowed JSON before each invocation. Payloads and
handlers need `Send`, not `Clone` or `Sync`; the original `&TaskAttempt` and
mutable handler state are preserved. Format/decode failures are `Permanent`
with sanitized expected type/version, without payload, metadata or serde error
text. Business errors, including backoff, pass unchanged. Additional fields
follow the DTO's serde policy; current service v1 DTOs keep accepting them.
Use separate adapters for v1/v2 or implement `TaskAdapter` for multi-version
adaptation to a business handler. Business validation, DTO conversion and exact-repeat classification
remain application responsibilities. Sender ports may borrow `&TaskAttempt`
throughout async work instead of constructing an owned context or cloning
metadata; lease credentials remain inaccessible.

The host applies [event-store SQL](sql/001-event-store.sql) and
[outbox SQL](sql/002-transactional-outbox.sql), supplies dispatcher factories and
a database pool. The optional `host` facade owns process signals and
inbound-adapter draining; the application constructs its inbound adapter and
closes the pool after the host returns.
Load runtime and queue configs before connecting the pool. The following
preparation function requires the `postgres` and `worker` features and a host
dependency on SQLx 0.8. Start its returned object only after binding listeners,
using Tokio and tokio-util 0.7:

```rust
use cqrs_runtime::{
    postgres::{task_queue::config::PostgresTaskQueueConfig, task_runtime_factory::prepare_task_runtime},
    runtime::{
        config::TaskRuntimeConfig,
        dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
        preparation::TaskRuntime,
    },
};
use sqlx::PgPool;

pub fn prepare_tasks(
    pool: PgPool,
    factory: TaskDispatcherFactory,
    queue_config: PostgresTaskQueueConfig,
    runtime_config: TaskRuntimeConfig,
) -> Result<TaskRuntime, WorkerSetupError> {
    prepare_task_runtime(pool, factory, queue_config, runtime_config)
}
```

The host shares a `CancellationToken` with HTTP and the task supervisor. Call
`cancel()` and await the supervisor for graceful drain. Fatal supervisor errors
cancel the supplied token before drain, so HTTP can stop admission immediately.
Pass a `child_token()` for an isolated pool. Dropping a token handle does not
cancel it; a lifecycle owner can hold `shutdown.clone().drop_guard()`.

Prepare before binding inbound listeners. `runtime::preparation::prepare(queue, factory, config)`
works with any queue; `postgres::task_runtime_factory::prepare_task_runtime` assembles the PostgreSQL
adapter when both `postgres` and `worker` features are enabled. Neither function
performs storage I/O or starts background tasks. Preparation calls the factory
exactly C times, freezes ordered formats, and keeps the first dispatcher for worker
zero. Every fallible factory invocation must return a fresh dispatcher with the same ordered
format list. Defaults do not read environment variables automatically: call
`from_env` or pass explicit configurations as above. The guide lists every
setting, its units/default, and the full lease-budget formula.
The private poller claims and recovers work with the executor's effective storage
budget. TaskExecutor only executes and settles one attempt; it exposes no claim,
recovery or format-routing methods.

`TaskRuntimeConfig` combines storage and worker pool configs,
checks the complete lease budget at construction/load time, and keeps PostgreSQL
statement timeout separate. The service loads configs before connecting to the
database. `TaskRuntime` is consumed by `run`; it rebuilds dispatchers only
on restart, preserving each worker's mutable state. Dropping the prepared object
releases handlers without cancellation. A polled run owns a cancellation DropGuard;
a pre-cancelled token starts no tasks or storage operations.

Configuration errors (`TaskRuntimeConfigError`), preparation errors
(`WorkerSetupError`) and runtime exits
(`TaskRuntimeError`) remain distinct. Low-level `runtime::supervisor::run` is retained for
custom executors; it uses the same supervisor engine and preserves cancelled
startup and actual-executor budget validation.

Configs validate local values at construction; `TaskRuntimeConfig` validates
their combined lease budget. Building a executor or PostgreSQL queue from
checked configs is infallible. Both preparation APIs return `WorkerSetupError`
directly: `Build` retains its source and worker index, while `FormatsMismatch`
identifies the worker with a different ordered format list.
`TaskRuntimeError::{Configuration, WorkerSetup}` covers low-level startup budget
validation and startup/restart dispatcher errors. Dispatcher getters borrow their
stored formats; adapter traits return owned format lists.

Generate local API documentation with:

```sh
cargo doc --all-features --no-deps
```

## Process lifecycle integration

Enable `features = ["host"]` and give the inbound adapter and tasks the same
`CancellationToken`. The inbound future returns `io::Result<()>`; it may borrow
local state and need not be `Send`. The tasks future returns
`Result<(), runtime::supervisor::TaskRuntimeError>` and must be `Send + 'static`, because host
spawns it. Signal futures can also borrow local state and need not be `Send`.

```rust,no_run
use cqrs_runtime::{
    host::{error::HostError, lifecycle::run, signal::shutdown_signal},
    runtime::supervisor::TaskRuntimeError,
};
use std::{future::Future, io};
use tokio_util::sync::CancellationToken;

async fn serve<Inbound, Tasks>(inbound: Inbound, tasks: Tasks, shutdown: CancellationToken) -> Result<(), HostError>
where
    Inbound: Future<Output = io::Result<()>>,
    Tasks: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
{
    run(inbound, tasks, shutdown, shutdown_signal()).await
}
```

For a prepared runtime, pass `tasks.run(shutdown.clone())` directly to `host::lifecycle::run`.
The host installs its cancellation DropGuard before spawning tasks on first
poll; simply creating or dropping an unpolled host future starts nothing and
leaves the token unchanged. `shutdown_signal` observes SIGINT/SIGTERM on Unix
and Ctrl-C elsewhere, returning observation errors, including a closed SIGTERM
stream.

Stop observation uses biased priority: signal → token → inbound exit → tasks
exit. The host cancels the token and awaits both components. An unexpected exit
remains the primary error; requested shutdown prefers signal observation, tasks,
then inbound errors. Secondary failures are logged. `HostError::log` and `Debug`
render task failures as task ID and panic/cancellation flags, preserving the
original `JoinError` as a source without rendering its panic payload.

Normal return happens after both components drain; the caller then closes the
pool even on error. Dropping/aborting a polled host cancels the token and detaches
the tasks task so its independent drain can finish while Tokio remains running.
The inbound future is dropped, and the host cannot await components or guarantee
resource cleanup. Tasks retains its H+S budget; inbound drain and pool closure
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

The public namespaces are `command`, `task`, `postgres`, `runtime`, `host`, and
`logging`. Types and functions are imported from their defining modules; old
flattened paths have no compatibility re-exports. Polling, worker-pool internals,
SQL operations and checked write plans remain private. The
[component source map](doc/components.md#source-layout) describes the directory
layout. Task's `data`, `handling`, `queue`, and `executor` module roots only
declare components. Each implementation has its own file; queue and
executor ports are separate from concrete adapters. Handler-visible attempts
live with data, while leases live with queue ownership.

The runtime separates dispatcher construction, polling, worker-pool ownership,
supervision, and the receive/execute loop. PostgreSQL queue operations each keep
their SQL, row decoding, and complete transaction in one file. The event
repository separates its adapter, checked write plan, and atomic CTE compiler.

Unit tests stay in inline `#[cfg(test)] mod tests { ... }` blocks alongside
the code they exercise; shared runtime fakes live in `task/test_support.rs`.
Public runtime behavior is exercised under `tests/runtime/`, rooted by
`tests/runtime.rs`; that suite owns its queue and handler fixtures. PostgreSQL
integration suites remain under `tests/postgres/`.
The [coverage audit](doc/test_coverage.md) maps the runtime invariants to their
unit, runtime and PostgreSQL regression suites, including concurrent writes, settlement,
recovery and expiry during lock waits.
Runtime integration tests include a 1,000-delivery run through the supervisor and executor
with test storage, checking every handler invocation ID and its Completed settlement.
Queue limits, cancellation, restart and shutdown have separate behavioral checks.

`TaskFormat::new/of`, `NewTask` builders, and configuration constructors
return typed validation errors. Their fields are private: use getters and
checked builders, not struct literals. `AggregateConflictRetryPolicy::new` is
also fallible; only representable backoff budgets are accepted.

`SettlementRequest` expresses intent; `SettlementResult` distinguishes confirmed
completion, scheduled retry, failure, and lost ownership. `TaskFailureCause`
provides fixed safe log reasons, independent of persisted handler error text.
`LeaseRecoveryReport` derives its total from retries and confirmed failures.

`TaskProcessingError::InvalidRequest` only describes invalid queue request parameters.
`InvalidDelivery` describes structurally invalid queue data or settlement results;
`InvalidExecutionBudget` describes an invalid invocation budget. Storage timeout
and exhausted lease budgets remain distinct and do not establish a failed commit.

Host I/O diagnostics expose only the error kind, and dispatcher error formatting
sanitizes task type names. Original sources and duplicate formats remain available
for programmatic inspection, without appearing as arbitrary text in host logs.

`TaskDispatcherFactory` returns `Result<TaskDispatcher, TaskDispatcherError>`.
`TaskRuntime::run` reports `TaskRuntimeError` for runtime failures.
Low-level `runtime::supervisor::run` also reports configuration and dispatcher errors on startup.
Preparation reports typed errors before any workers start. It still supervises
one mutable dispatcher per permanent Tokio task and leaves interrupted work for recovery.
Defaults are 128 concurrent deliveries, 128 queued deliveries, and a 60-second
lease. `OUTBOX_QUEUE_CAPACITY` and `OUTBOX_WORKER_CONCURRENCY` are independent.
The explicit worker config constructor now takes `queue_capacity` after
`concurrency`; the combined lease validation includes queued waves.

The service host uses handler/storage/statement budgets of 15s/6s/4s. With
C=Q=128, lease=60s exceeds the validated 53s minimum. HTTP/DB shutdown limits
remain [known issue #10](../doc/known_issues/http_shutdown.md).

For the host's current lifecycle and the planned 28s envelope, see the
[graceful shutdown timeline](../README.md#graceful-shutdown-timeline).
