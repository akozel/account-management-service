# Components and terminology

The core business flow is `Command → Aggregate → Event`, followed by atomic
publication of the accepted event and application-created `NewTask` values.
Published tasks follow `TaskQueue → TaskRuntime → Worker → Handler`.

Inside the last step, the call flow is
`Worker → TaskExecutor → TaskDispatcher → TaskAdapter → TaskHandler<P>`.
This tree groups architectural roles, not inheritance. Some roles are functions
or private modules rather than public structs.

```text
cqrs-runtime
├── Command execution
│   ├── CommandExecutor — command port; caller selects aggregate identity
│   ├── CqrsCommandExecutor — load/decide/commit with bounded conflict retry
│   └── AggregateConflictRetryPolicy — retry unchanged commands on conflict only
├── Durable task data
│   ├── TaskPayload — versioned business wire DTO contract
│   ├── NewTask — checked immutable operation prepared for publication
│   ├── TaskFormat — complete type/version routing key
│   └── TaskSchedule — earliest eligibility for an initial claim
├── Attempt context
│   ├── TaskAttempt — immutable ID/payload/metadata/positive attempt number
│   ├── TaskLease — platform ownership token and original local deadline
│   └── LeasedTask — attempt plus ownership; only platform code receives both
├── Business and adaptation
│   ├── TaskHandler<P> — one replay-safe operation on an owned decoded DTO
│   ├── TaskAdapter — formats plus invoke(attempt), without settlement/retry
│   ├── JsonTaskAdapter — private format check and borrowed JSON decoding bridge
│   └── TaskDispatcher — dispatch(attempt) selects exactly one adapter
├── Technical execution
│   ├── TaskExecutor — one leased attempt: budget, dispatch, settlement
│   ├── DefaultTaskExecutor — standard queue-backed implementation
│   └── ExecutionPlan — run, retry-only or recover-only decision
├── Runtime (worker Cargo feature)
│   ├── TaskRuntime — checked queue/executor/dispatcher assembly consumed by run
│   ├── Supervisor — observe workers, restart failures, stop admission and drain
│   ├── Poller — private rotating claims and expired-lease recovery
│   ├── WorkerPool — JoinSet ownership and bounded drain of permanent workers
│   ├── TaskWorker — run_task_worker receive/execute loop, one owned dispatcher
│   └── DispatchBuffer — bounded async-channel of waiting LeasedTask values
├── Durable storage
│   ├── TaskQueue — atomic claim, token/expiry fencing, settlement and recovery
│   ├── PostgresTaskQueue — PostgreSQL ownership transitions
│   ├── PostgresEventRepositoryWithOutbox — atomic event/snapshot/task publication
│   ├── WritePlan / SnapshotWrite — checked persistence decisions
│   ├── ClaimRow / RecoveryRow — private decoded PostgreSQL records
│   └── SettlementParameters — private SQL transition parameters
├── Decisions and confirmations
│   ├── SettlementRequest — requested completion, retry or terminal failure
│   ├── SettlementResult — committed outcome or lost ownership
│   ├── TaskFailure / TaskFailureCause — confirmed terminal transition and category
│   └── LeaseRecoveryReport — confirmed retries and terminal failures
├── Assembly
│   ├── task_adapter — checked typed handler to boxed wire adapter
│   ├── TaskDispatcherFactory — fresh dispatcher/handler state per worker/restart
│   ├── runtime::preparation::prepare — retain all C dispatchers before opening listeners
│   ├── postgres::task_runtime_factory::prepare_task_runtime — PostgreSQL-backed runtime assembly
│   ├── task_executor[_with_config] — ready queue-backed attempt executor
│   ├── postgres::command_factory::command_executor — ready snapshot-backed CQRS executor
│   └── postgres::pool::connect_pool — explicit caller-owned database connection pool
├── Checked configuration
│   ├── TaskRuntimeConfig — storage/worker settings and combined lease validation
│   ├── TaskWorkerPoolConfig — concurrency, buffer, polling, handler and lease limits
│   ├── TaskStorageConfig — shared end-to-end queue operation timeout
│   ├── PostgresTaskQueueConfig — separate PostgreSQL statement timeout
│   ├── PostgresCommandConfig — snapshots and conflict retry
│   └── LoggingConfig — explicit process logger settings
├── Process lifecycle
│   ├── host::lifecycle::run — shared shutdown and inbound/task-runtime failure priority
│   ├── shutdown_signal — explicit OS signal observation
│   └── StopReason / TaskRuntimeExit — private lifecycle decisions/results
├── Boundary errors
│   ├── CommandGatewayError — domain rejection/conflict/unavailable
│   ├── TaskFormatError / TaskValidationError — invalid wire envelope/input
│   ├── TaskHandlerError — retry or permanent operation rejection
│   ├── TaskDispatcherError — empty/duplicate formats or adapter construction error
│   ├── QueueRequestError / TaskProcessingError — parameters and technical processing
│   ├── WorkerSetupError — worker-indexed dispatcher construction/mismatch
│   ├── TaskRuntimeError — actual-budget validation or worker setup failure
│   ├── HostError / LoggingInitError — lifecycle or explicit logger initialization
│   └── *ConfigError / *PolicyError / WriteValidationError — invalid settings/writes
└── Technical support
    ├── CancellationToken / DropGuard — shared stop request and lifecycle ownership
    ├── Notify / JoinSet — capacity notification and worker observation
    ├── diagnostics / env / logging — safe categories, parsing and explicit setup
    └── test_support — private fakes and observable test state
```

Command, Aggregate, Event, EventStore and framework repository contracts come
from the application and cqrs-es. Runtime does not choose business transitions.

## Source layout

The top-level namespaces are `command`, `task`, `runtime`, `postgres`,
`host` and `logging`. The task subsystem and PostgreSQL adapter roots below
contain documentation and module declarations. Public types and functions are
imported directly from the modules that define them; there are no flattening
re-exports or compatibility import paths.

For example:

```rust
use cqrs_runtime::{
    runtime::preparation::{TaskRuntime, prepare},
    task::{data::attempt::TaskAttempt, handling::handler::TaskHandler, queue::port::TaskQueue},
};
```

Polling, worker-pool internals, SQL operations and write plans remain private.

```text
src/
├── task.rs
├── task/
│   ├── data.rs
│   ├── data/
│   │   ├── format.rs          TaskPayload, TaskFormat and format errors
│   │   ├── new_task.rs        NewTask, TaskSchedule and checked builders
│   │   ├── attempt.rs         Handler-visible TaskAttempt
│   │   └── validation.rs      Shared structural checks and validation errors
│   ├── handling.rs
│   ├── handling/
│   │   ├── handler.rs         TaskHandler and business outcomes
│   │   ├── adapter.rs         TaskAdapter contract
│   │   ├── json_adapter.rs    Checked JSON bridge and task_adapter factory
│   │   └── dispatcher.rs      Routing and registration errors
│   ├── queue.rs
│   ├── queue/
│   │   ├── port.rs            TaskQueue contract
│   │   ├── lease.rs           TaskLease, LeasedTask and safety margin
│   │   ├── settlement.rs      Requests, confirmed outcomes and recovery report
│   │   ├── error.rs           Queue request and technical processing errors
│   │   ├── storage_config.rs  End-to-end storage budget
│   │   └── retry.rs           Durable redelivery backoff
│   ├── executor.rs
│   ├── executor/
│   │   ├── port.rs            TaskExecutor contract
│   │   ├── default_executor.rs Queue-backed implementation and factories
│   │   └── lease_budget.rs    ExecutionPlan and original-deadline decisions
│   └── test_support.rs        Private unit-test fixtures
├── runtime.rs
├── runtime/
│   ├── config.rs              Combined storage/worker validation
│   ├── worker_pool_config.rs  Worker and waiting-capacity settings
│   ├── preparation.rs         TaskRuntime and synchronous prepare
│   ├── dispatcher_factory.rs  Checked per-worker construction and setup errors
│   ├── supervisor.rs          Admission, polling, restart and stop coordination
│   ├── worker_pool.rs         JoinSet ownership, exit observations and drain
│   ├── poller.rs              Rotating claims and lease recovery
│   └── worker.rs              Permanent receive/execute loop
└── postgres/
    ├── task_queue.rs
    ├── task_queue/
    │   ├── queue.rs           Construction, shared transactions and delegation
    │   ├── config.rs          PostgreSQL statement timeout
    │   ├── claim.rs           Atomic claims and ClaimRow decoding
    │   ├── settlement.rs      Fenced settlement and SQL outcome conversion
    │   └── recovery.rs        Expired ownership recovery and RecoveryRow decoding
    ├── event_repository_with_outbox.rs
    └── event_repository_with_outbox/
        ├── repository.rs      CQRS repository adapter and per-command tasks
        ├── write_plan.rs      Checked single-event/snapshot decisions
        └── write.rs           Atomic SQL compilation and execution
```

The map focuses on reorganized components. Command execution/retry, PostgreSQL
factories/pool, host lifecycle and logging retain their existing locations.

Keep one component's data and implementation together. Split independent
operations, algorithms and I/O boundaries into named files. About 150–250
production lines is a useful working range; approaching 300 prompts a review,
not a mechanical split. Total file size, including tests, also matters.

Each PostgreSQL queue operation retains its row decoding and complete
transaction, preserving the lock/expiry/commit order. Write-plan validation
contains no SQL builder; the writer translates its checked decisions into one
atomic statement.

Private unit tests remain inline. Public multi-component runtime scenarios live
under `tests/runtime/` (preparation, admission, supervision and shutdown),
rooted by `tests/runtime.rs`, with their own fixtures. Database suites remain
under `tests/postgres/`, rooted by `tests/postgres.rs`.

## Naming rules

`Handler` owns business work; `Adapter` translates representations; `Dispatcher`
routes; `Executor` bounds one invocation; `Worker` consumes repeatedly;
`Supervisor` owns group lifecycle; `Runtime` assembles the whole task subsystem.
Dispatcher is neither a TaskAdapter nor a TaskHandler. Custom multi-version
adapters decode/convert their supported formats before invoking business work.

`Task` refers to a durable operation. Use future, worker, join handle and
tokio_task_id for Tokio execution. Use Queue for durable ownership and Buffer
for local waiting. Config contains checked values; Policy expresses a decision
rule; Factory constructs components; Plan precedes effects; Result/Report follow
confirmed outcomes. Read an attempt number with `LeasedTask::attempt().number()`.

The outbox name remains on atomic event/task publication and existing schema,
SQL assets and environment settings. Worker/executor is not a UnitOfWork:
business effects and settlement have separate commits, so redelivery is possible.

## Integration

The public namespaces are command, task, runtime, postgres, host and logging;
import APIs through their defining child modules.
The existing Cargo feature names postgres, worker, host and logging are unchanged;
worker enables the runtime namespace. No compatibility namespace/type aliases remain.

Prefer `runtime::preparation::prepare(queue, factory, TaskRuntimeConfig)` or
`postgres::task_runtime_factory::prepare_task_runtime(pool, factory, queue_config, runtime_config)`.
Both return `Result<TaskRuntime, WorkerSetupError>` without background effects.
Only the first poll of `TaskRuntime::run(token)` starts workers.

Low-level `runtime::supervisor::run(queue, executor, factory, worker_config, token)` accepts
custom executors. The caller wires an executor that settles against that queue.
The private poller uses the executor's actual storage timeout, and the runtime
validates the full queued-wave lease budget against it before any storage work.
Formats come from the first retained dispatcher; every other dispatcher must
match type, version and order. Handler/sender state stays owned by one worker.

See [task processing](task_processing.md) for at-least-once execution, budgets,
idempotency and shutdown, and [architecture](architecture.md) for call flow.
