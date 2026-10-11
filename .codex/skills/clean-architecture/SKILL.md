---
name: clean-architecture
description: Preserve the Account Management Service's domain, application, presentation, and infrastructure boundaries when changing code or wiring.
---

# Clean architecture for Account Management Service

Use this skill for Rust source changes, dependency wiring, and infrastructure
configuration in this repository.

## Boundaries

- `domain` owns business language, invariants, aggregate state, commands, and
  domain events. It is deterministic and does not depend on transport,
  persistence, clocks, network clients, or framework-specific I/O types.
- `application` coordinates workflows through domain types and ports: it
  creates domain commands and maps domain/gateway outcomes into the feature's
  service errors.
- `presentation` maps inbound protocols into domain and application types. It
  owns HTTP DTOs and must not bypass application code or encode domain rules.
  Application must not expose or depend on HTTP DTOs.
- `infrastructure` implements service-specific ports such as aggregate ID
  selection, entropy, clocks, and external delivery. Aggregate gateways
  delegate generic execution to `cqrs-runtime`; do not duplicate its retry or
  persistence behavior.
- `cqrs-runtime` owns domain-agnostic CQRS command execution, outbox
  contracts and coordination, PostgreSQL persistence, canonical SQL, and the
  worker supervisor and optional transport-independent process host. It must
  never depend on `account-managements-service` or Axum.
- `main` composes business dependencies and HTTP adapters, passes inbound and
  outbox futures to `cqrs_runtime::host::run`, then closes the database pool
  regardless of preparation/bind/host results before logging/returning the
  binary ServiceError. Private config/startup modules own process configuration,
  safe error reporting and pool cleanup; host retains component lifecycle.
- The optional `host` feature enables `worker` and `tokio/signal`. Its facade
  exports `run`, `shutdown_signal`, and `HostError`; private `lifecycle`, `error`,
  and `signal` modules own transport-independent coordination. No service-local
  runtime wrapper or Axum dependency belongs in runtime.

## Startup configuration and diagnostics

Private binary ServiceConfig owns PostgreSQL options, numeric SocketAddr, logger,
outbox and queue configs. from_env snapshots supported keys, rejects non-Unicode
values and delegates to injected lookup. Update its key list and .env.example when
adding settings. Required DATABASE_URL uses postgres/postgresql URI; BIND_ADDRESS
requires numeric IPv4/IPv6 plus port. Executable dotenv loading allows only missing
files; other errors are typed. Validate all settings before logger/DB initialization.
Runtime must never load dotenv or install a subscriber implicitly.

Runtime logging is optional and independent; telemetry is deferred. LoggingConfig
has private fields, LevelFilter new/default/getter/from_lookup/from_env and try_init.
Preserve formatter defaults (target false, thread names true) and log bridge.
Caller-provided PgConnectOptions feed postgres::connect_pool (max 10, other SQLx
defaults). Service SQLx dependency is production; no direct postgres-es adapter
construction or connection helper belongs in service code.

Startup errors retain sources but Display/Debug/logging must hide PgConnectOptions
credentials, URL, dotenv lines and arbitrary SQLx/I/O source text. Global logger
setup tests live in an isolated integration executable. Normal await closes the
pool after work (including preparation/bind/host errors), before reporting the
original result. No host/pool deadline or Drop cleanup guarantee is added.

Technical command failures log fixed category, execute operation and safe aggregate
type before mapping to Unavailable. Domain/conflict handling stays unchanged.
PostgreSQL diagnostics classify SQLx sources before they are discarded, emitting
only fixed categories and valid five-character uppercase/digit SQLSTATE. Never
format raw errors, SQL detail/query/constraint, command/task payload/metadata,
aggregate ID/email or connection URL. Private diagnostics/env helpers have no
public facade; keep retries, per-config ranges/errors and backoff ownership intact.

## Application feature layout

Group application code first by domain capability, then by use case:

```text
account-managements-service/src/
  application.rs
  application/
    {capability}.rs
    {capability}/
      service.rs
      gateway.rs
      use_cases.rs
      use_cases/{use_case}.rs
      tasks.rs
      tasks/{task}.rs
      tasks/{task}/v1.rs
```

- `{capability}.rs` declares the feature modules. `service.rs` owns the public
  input service, service errors and factory; `gateway.rs` owns the output port.
  Gateway results use the shared
  `cqrs_runtime::command::CommandGatewayError<DomainError>`; do not define a
  feature-specific gateway-error enum. `use_cases.rs` declares the private
  workflows and their shared constants. Keep test-only fakes out of the
  service's `src/`; application boundary tests under
  `account-managements-service/tests/` own their gateway fakes.
- `{capability}/use_cases/{use_case}.rs` implements one workflow. It
  creates the domain command, calls the feature gateway, and follows
  [Error boundaries](#error-boundaries). It does not define a public API for
  adapters to import.
- Random IDs, codes, and tokens used by a workflow come from an injected
  application port implemented by infrastructure. Generate them before
  issuing the command; events carry the accepted values so replay never
  samples randomness again.
- Versioned durable task DTOs live under `{capability}/tasks/{task}/v1.rs` and
  implement `cqrs_runtime::outbox::TaskPayload`; `{task}.rs` owns the current
  `TypedTaskHandler<P>` implementation and any business sender port. Wrap it
  with `outbox::typed_task_handler` in the feature factory. Keep persisted
  DTOs readable while queued work can still be retried.
- Presentation imports only `application::{capability}::service::{FeatureService,
  FeatureServiceError}`. Infrastructure implements the feature's
  `gateway::FeatureGateway`. `main` composes a gateway through the service factory and
  passes the resulting service to presentation.
- Do not import a nested use case from outside its feature. Use Rust 2024 file
  modules; do not introduce `mod.rs`.

### Feature layout example

```text
account-managements-service/src/application/invoice.rs                 Declares gateway, service, use cases
account-managements-service/src/application/invoice/gateway.rs         InvoiceGateway output port
account-managements-service/src/application/invoice/service.rs         InvoiceService input port, errors and factory
account-managements-service/src/application/invoice/use_cases.rs       Private workflow declarations
account-managements-service/src/application/invoice/use_cases/approve.rs  Command and error mapping
```

`InvoiceService` is the input boundary used by presentation; `InvoiceGateway`
is the output boundary implemented by infrastructure. Keep expected domain
rejections and relevant conflicts mapped to semantic service errors in the
private use case. Preserve unavailable and unexpected rejections separately.

## Error boundaries

Each error type belongs to exactly one boundary; do not merge them into a
cross-layer enum.

- `DomainError` is deterministic aggregate business logic. It never represents
  I/O, persistence, retries, or framework behavior.
- `cqrs_runtime::command::CommandGatewayError<DomainError>` is the shared output-port
  result for command gateways. Infrastructure converts framework failures into
  these categories without deciding what a command means to its caller. Do not
  create a feature-specific duplicate of this enum.
- `FeatureServiceError` is the input-port result. The private use case maps its
  expected domain rejection and relevant conflict to a semantic service error;
  it keeps `Unavailable` distinct and maps an impossible domain rejection to
  `UnexpectedDomainRejection(DomainError)`.
- A presentation resource maps expected service errors to its HTTP contract.
  It maps unavailable and unexpected service errors to safe internal responses
  without exposing domain, framework, or persistence details.

## Retry ownership

- `cqrs_runtime::command::CqrsCommandExecutor` owns the short optimistic-conflict retry.
  An aggregate-specific gateway computes the aggregate ID and delegates the
  unchanged command and prebuilt tasks. The executor builds one framework per
  command and reloads aggregate state for each attempt. `max_attempts` counts
  the initial execution.
- Retry only `AggregateError::AggregateConflict`. Never automatically retry
  connection, timeout, unavailable, deserialization, or unexpected failures:
  their commit outcome may be unknown without an idempotency mechanism.
- Application issues one generated command per workflow invocation. It does
  not regenerate a command for rare random-value collisions such as
  `AccountIdUnchanged`; map that outcome to the feature conflict contract.
- Durable outbox delivery remains the long-lived retry boundary for internal
  handlers. An exhausted fast conflict can become a retryable task outcome,
  but the application layer must not repeat that same conflict first.
- A task handler may own bounded local retries of its one operation. The
  outbox separately owns durable redelivery of the whole task, including
  recovery after a crash or expired lease; the coordinator also retries a
  timed-out handler. Local retry exhaustion does not prevent redelivery until
  a terminal task outcome is durably settled. Every invocation must be safe
  to repeat from the beginning.
- Bound and document local and durable retry budgets together, including
  sender SDK retries. Explicit local retries may multiply with outbox
  deliveries (`10 × 3` can mean thirty sends). Keep local work within the
  handler execution budget; do not duplicate the CQRS executor's conflict
  retries in application code.
- Fast command retry does not make a later client HTTP request idempotent.

## Infrastructure layout

Group outbound adapters by infrastructure technology:

```text
account-managements-service/src/
  infrastructure.rs
  infrastructure/
    console.rs
    cqrs.rs
    cqrs/
      {feature}_gateway.rs
    random.rs
    time.rs
```

- Keep a technology in one module file while it has a single small adapter.
  Split it into a same-named module file and directory only when multiple
  adapters make that clearer. `random` owns entropy-backed generators, `time`
  owns system clocks, and `console` owns development console output adapters.
- `infrastructure/cqrs/{feature}_gateway.rs` implements the feature gateway. It accepts
  the domain command created by application, derives any framework aggregate id,
  delegates it to an injected `Arc<dyn cqrs_runtime::command::CommandExecutor<A>>`, and follows
  [Error boundaries](#error-boundaries).
  It must not map a domain rejection or conflict to a service error.
  Command gateways do not expose reconstructed aggregate state to the
  application. The CQRS engine loads state internally to decide a command and
  uses optimistic version checks on commit. Build query endpoints from event
  streams and projections.
- `cqrs-runtime`'s `postgres` feature owns `sqlx`, `postgres-es`, the event
  repository, queue, and SQL assets. Service infrastructure must not wrap or
  re-export these generic adapters.
- Do not add `pub use` aliases that flatten technology modules. Import a
  concrete adapter through its technology path.
- Runtime diagnostics use structured `tracing` events at the adapter boundary
  that observes the outcome; do not write directly with `println!`, `eprintln!`,
  or process-global stdout/stderr handles. Runtime's optional `logging` facade
  owns LoggingConfig and explicit fallible subscriber installation; the binary
  installs it after validating all settings. The base tracing dependency emits
  diagnostics without installing anything. Default level is info; preserve
  LevelFilter aliases and parsing for RUST_LOG, without EnvFilter. Keep domain
  code free of logging and never include outbox payloads or secrets in failure
  events.
- `main` is the only composition root. It calls the runtime's checked PostgreSQL
  command factory, then constructs the feature gateway, feature service and
  router. Technical repository/store/framework construction belongs to
  `cqrs-runtime`; service infrastructure must not provide factories that compose
  several technologies or duplicate that runtime assembly.

### Explicit transactional outbox

**Critical project invariant: one Task represents exactly one retryable operation.**
For an aggregate command, the sequence is: check invariants against the
aggregate's current state; if accepted, emit a domain event; commit that event
and its follow-up Task atomically. A rejected command commits neither. The
Task payload must contain the canonical data from the accepted event or
aggregate state that its worker needs; do not make a worker reconstruct the
source aggregate by scanning its event history. The receiving aggregate checks
its own invariants. An in-progress aggregate phase blocks conflicting commands
until a later Task's command records completion; it is not a database lock held
while a worker runs.

A TaskHandler performs one target-aggregate command, or one external effect
such as sending an email. It must not issue several aggregate commands as a
single Task's workflow. If a process needs another command, the first command
commits the next Task with its event. Mark the original Task `Completed` only
after its own operation succeeds; this status describes the Task, not the
entire process.

For example, `SubmitRegistrationProfile` commits `RegistrationProfileAccepted`
and `CreateAccountTask`. Its worker commands `UserAccount::CreateAccount`,
which commits `AccountCreated` and `RecordAccountCreatedTask`. The second
worker commands `EmailReservation::RecordAccountCreated`, which commits
`AccountCreationCompleted`. Each worker then settles only its own Task.

Task execution is at least once: the aggregate command can commit before
the separate `Completed` settlement succeeds. A redelivered Task must be safe:
the target aggregate may reject an already-applied command with a distinct
domain outcome for an exact match, without a new event or Task. The TaskHandler
for that Task decides whether each domain outcome means `Completed`, retry, or
permanent failure. Map an exact already-applied outcome to `Completed`; reject
different existing data. Do not treat a generic `AlreadyExists` rejection as
success unless the aggregate checked all identity-defining fields. Keep this
business classification out of the generic queue/coordinator. When retries
are exhausted, persist `Failed` and emit an error-level `tracing` event for the
confirmed transition, including failures found during lease recovery. Log task
identity, attempt, and reason, not secrets or payload. `Failed` does not prove
the aggregate command never committed.

For `CreateAccountTask` specifically, `UserAccount::CreateAccount` rejects an
exact repeat as `AlreadyCreated` after checking the account ID, email, and
canonical profile. Its TaskHandler treats that rejection as `Completed`; a
different existing account is a separate permanent failure.

Use cases choose between `FeatureGateway::execute(command)` (events only) and
`execute_with_outbox(command, tasks)` (events and explicit durable work).
Outbox tasks are application contracts with independent payload versions;
do not add delivery controls to domain events or serialize tasks into event
metadata. Rejected commands or rolled-back writes commit neither events nor
tasks; connection/response failure can leave an unknown commit outcome. With the
current prebuilt-task gateway, ensure the task fields match the event that the
aggregate can actually emit; a rejected exact repeat must not enqueue a new
Task. Exact-repeat success is reserved for internal at-least-once Task handling.
A client profile submission consumes its account-creation token with the first
accepted command; every later submission with that consumed token is a semantic
rejection, even when the request is identical.

`PostgresEventRepositoryWithOutbox` supports at most one event per command.
It rejects a multi-event batch, a snapshot without an event, and tasks without
an event before issuing SQL. The one event, optional snapshot, and tasks are
then committed by one atomic statement. Supporting multi-event commands
requires a separate snapshot-contract change.

`main` injects a ready `CommandExecutor<A>` into each CQRS gateway. The standard
`postgres::command_executor` factory uses unit services and no queries. Its
checked `PostgresCommandConfig` has a positive snapshot interval (default 25)
and the existing conflict retry policy. For each command, the factory creates
a repository carrying that call's immutable tasks, then the snapshot store and
framework. It performs no I/O and reads no environment variables. Share pools,
never mutable per-command task buffers. Keep `CqrsCommandExecutor::new` for custom
stores/services/queries in tests or nonstandard integrations; gateways themselves
do not own factory or retry configuration.

```rust
let executor = cqrs_runtime::postgres::command_executor::<Invoice>(
    pool.clone(), cqrs_runtime::postgres::PostgresCommandConfig::default(),
);
let gateway = CqrsInvoiceGateway::new(executor);
```

The current cqrs-es snapshot path reapplies accepted events to already changed
aggregate state. `apply` must tolerate the repeat (the service assigns accepted
state); increment/append models require a separate snapshot-contract fix. See
[snapshot reapplication](../../../doc/known_issues/cqrs_snapshot_event_reapplication.md).

Application owns versioned business task DTOs, handler implementations, and
sender ports. `cqrs-runtime` owns the generic task, queue and handler traits,
the checked `typed_task_handler` adapter, `OutboxTaskCoordinator`, and
`TaskHandlerRegistry`. The registry
rejects empty and duplicate registrations and routes each type/version to one
handler. The composition root supplies a business handler factory; runtime
preparation creates all C registries before binding HTTP. The first registry
supplies the coordinator's ordered claim formats and becomes worker zero's
registry; the remaining C−1 must match. Unknown formats stay pending.
Typed handlers receive an owned versioned DTO and the original `&TaskDelivery`.
The base runtime adapter returns `Result<Box<dyn TaskHandler>, TaskFormatError>`,
checks format on construction and delivery, and decodes borrowed JSON before
calling business code. Payload/handler need Send, without Clone/Sync requirements.
Format/decode failures are Permanent with safe expected type/version; type names
outside ASCII letters/digits/underscore become `unknown` only in diagnostics.
Never include payload, metadata, actual arbitrary type or serde error text.
Business results and retry_after pass unchanged. Application keeps semantic
validation, wire-to-current-input conversion and exact-repeat classification.
One adapter handles one DTO version; raw TaskHandler supports custom multi-version
handlers. Registry duplicate checks and claim formats remain unchanged.
Application sender ports borrow the same TaskDelivery for ID/attempt/metadata;
do not recreate an owned DeliveryContext or clone metadata just to pass context.
Handler boundary tests and their fakes belong in the service integration suite;
runtime adapter tests use local dummy DTOs and handlers in its private module.
The runtime PostgreSQL adapter owns atomic claims, leases, token-checked settlements
and expired-lease recovery. Concrete senders live under their infrastructure
technology. Versioned wire DTOs are converted into current handler inputs.
The outbox uses `pending`, `processing`, `completed`, and `failed` statuses.
`pending.available_at` is the earliest claim time for both initial schedules
and retry backoff; a zero-delay retry becomes available immediately. A claim
increments attempts and grants a lease and lock token. Only `processing` has
lease ownership; only `completed` and `failed` have `finished_at`. Settlement
and expired-lease recovery return confirmed `failed` transitions to the worker
for safe structured error logging. Unknown formats stay `pending` without being
claimed.

Background polling comes from `cqrs-runtime`'s `worker` feature. Its Tokio
supervisor owns one bounded async-channel queue and a JoinSet of permanent
Tokio workers. Each worker exclusively owns its mutable registry and a cloned
receiver; await recv directly without an external mutex. Configure process-wide
concurrency and queue capacity independently of CPU threads (defaults 128/128).
Only the poller owns a sender. Before claim, snapshot Q - sender.len(); consumers
only free space during that await, so the batch fits through try_send. No hidden
buffer or per-send Tokio tasks. Multiple producers would require a new admission
protocol. Never format TrySendError<ClaimedTask> into logs or panic messages.
Defaults use H=15s/S=6s/SQL=4s and a 60-second lease (C=Q=128 requires lease >53s).
Validate queued waves with checked arithmetic:
lease > 2*S + H + 5s + ceil(Q/C)*(H+S). Original deadlines remain authoritative.
Observe exits through JoinSet and rebuild one checked registry per replacement;
never log panic payloads. Normal shutdown and restart-factory errors share bounded
H+S draining of started work only; cancel the shared CancellationToken before
closing the channel, as closure alone permits buffered receives. Queued claims
remain for recovery. Abort and observe remaining tasks after the deadline.
The composition root shares token clones with HTTP, runtime host, and supervisor.
Fatal startup/restart errors cancel the supplied token before returning/draining,
so HTTP stops admission immediately; a worker panic alone is restarted. Callers
can pass a child token to isolate a pool. Ordinary token-handle Drop does not
cancel; lifecycle owners hold DropGuard. Supervisor panic/abort requests host
cancellation through its guard and aborts JoinSet workers without async drain.
Never race token cancellation against an active handler/settlement invocation.
The runtime host awaits inbound and outbox before caller-owned DB cleanup; preserve the first
unexpected component failure, or signal/outbox/inbound error priority for requested
shutdown, and log secondary failures. Format JoinError only as task ID and
panic/cancellation flags, including HostError Debug used by termination.
Host installs its cancellation DropGuard before spawning the outbox future on
first poll; constructing an unpolled host starts nothing. Inbound/signal futures
need neither Send nor 'static; only outbox does. Dropping/aborting a polled host
cancels its token and detaches outbox for independent drain on a live Tokio
runtime, without awaiting components or guaranteeing DB cleanup. Preserve H+S
outbox drain and no host timeout for inbound/pool closure; force/watchdog policy
remains [known issue #10](../../../doc/known_issues/http_shutdown.md).
Recovery batches are capped at min(C, 32). `main` loads checked
`OutboxRuntimeConfig` before DB connection, builds the business handler factory,
and calls `postgres::prepare_outbox_runtime` before binding HTTP. Queue/coordinator
assembly and combined budget validation belong to runtime, with PostgreSQL statement
cap kept separate. The helper is gated on both postgres and worker; neither feature
implicitly enables the other. Configuration parsing/validation remains covered.

### Task handlers and workers

Before reviewing or changing handlers, queues, retry, or worker configuration,
read the [runtime task processing guide](../../../cqrs-runtime/doc/task_processing.md).
It is the canonical portable contract; keep it with the runtime when moving
the engine to another project.

Preserve this project's aggregate-owned business sequence and one-operation
Task invariant. Import runtime APIs through its public `command`, `outbox`,
`postgres`, `worker`, and `host` facades; child modules are private. Task DTOs and
configs are created through checked constructors/builders, not public-field
mutation. `SettlementRequest` is intent; `SettlementResult` is a confirmed
transition. Do not equate scheduled retry with completed work. Registry factories
are fallible; supervisor errors are distinct from config validation errors.
Keep alternate lifecycle states in enums, and keep deadline/SQL loops explicit
where cancellation or partial results matter.

Sender ports and business outcome classification belong to
application; delivery, storage deadlines, lease budgets, lock tokens, and
monotonic claim deadlines belong to the runtime/queue adapter. Business handlers receive only immutable `TaskDelivery`, not queue ownership.
`ClaimedTask` and `ClaimLease` remain platform context. Configuration parsing/validation
stays in `cqrs-runtime`; `main` loads the combined config before DB connection.
Preparation builds/validates all registries before opening listeners, without
storage operations or background tasks. `PreparedOutboxRuntime` owns the same
registries that workers receive and is consumed by run, without startup rebuild.
Preparation owns no CancellationToken; Drop releases handlers. A polled run owns
DropGuard, skips spawn/storage on pre-cancelled token, and retains restart/drain
semantics. Low-level `worker::run` remains available for custom coordinators and
shares the same engine, including actual-budget validation and startup cancellation. Update the runtime guide and API rustdoc
together when changing this lasting contract.

Checked coordinator and PostgreSQL queue configs validate local values only in
their constructors; assembling adapters from them is infallible. Combined runtime
config owns cross-budget validation, while low-level worker::run retains its
actual-coordinator check. Both preparation APIs return worker::WorkerRegistryError
directly (Build with source/index, FormatsMismatch with index), without generic or
PostgreSQL preparation wrappers. Run returns OutboxWorkerPoolError with
Configuration/Registry variants. Preserve InvalidFormat conversion for typed
handler factories. QueueRequestError covers queue parameters only; invalid queue
data and invocation budgets are OutboxError::InvalidDelivery/InvalidExecutionBudget.

Use registry/registries for handler registries, delivery for TaskDelivery, worker
for permanent consumers and supervisor for pool lifecycle. PreparedOutboxRuntime
owns the full pool assembly. TaskHandlerRegistry::formats borrows its stored slice; trait
methods still return owned lists. Keep result-to-settlement and confirmed-status
mapping pure; random backoff, clocks, logging and SQL remain in effectful code.
Host I/O Debug/logging includes only ErrorKind, never arbitrary source text;
registry error Display/Debug uses the shared safe_type function and preserves raw
formats only as data. Defaults and env fallbacks share private unit-specific
constants. Non-Unicode runtime env behavior is unchanged; binary validation rejects it.

## Presentation layout

Organize HTTP adapters by resource, then use case:

```text
account-managements-service/src/
  presentation.rs
  presentation/
    http.rs
    http/{resource}.rs
    http/{resource}/
      {use_case}.rs
```

- `presentation/http.rs` composes resource routers and contains reusable HTTP
  primitives, such as a common serialized error envelope. It does not contain
  resource-specific status codes or error codes.
- `{resource}.rs` declares every route for that resource, binds the shared
  `ResourceState`, and owns its `ResourceError` enum.
- `{use_case}.rs` owns the handler, its request and response DTOs, and its
  transport-level tests. Keep these DTOs internal to presentation; do not
  create per-use-case `request.rs`, `response.rs`, or `errors.rs` modules.

### Coverage boundary

- `account-managements-service/src/main.rs` is the composition root. Exclude it from line coverage because
  it only wires dependencies and starts the process; do not add brittle unit
  tests around process startup.
- Keep `cqrs-runtime/src/host/` in coverage and test lifecycle/signal multiplexing
  with controlled futures rather than sending OS signals. The real Axum
  admission/drain/cleanup test belongs in the service integration suite.
- Keep `account-managements-service/src/presentation/http.rs` in coverage. Test its top-level router through an
  in-memory Axum request so route composition is verified without PostgreSQL.
- Keep infrastructure adapters in coverage until their integration-test
  strategy is explicitly decided; do not exclude them merely to satisfy the
  global threshold.
- Cover `cqrs-runtime` as part of workspace coverage; feature-specific code
  must be exercised with `--all-features`.

## Test ownership

- Test application command and error matrices through the public input service
  in integration tests under `account-managements-service/tests/`; those tests own their gateway fakes.
- HTTP tests send in-memory Axum requests to a stub input service. They do not
  construct an application service or gateway.
- Runtime unit tests use local dummy aggregate, task, error, and handler types;
  they must not import the service. Test executor retry/error behavior, payload
  construction, coordinator timeout/retry, registry routing, and worker
  supervision in inline `#[cfg(test)] mod tests { ... }` blocks in the module
  file they exercise. Do not create separate `tests.rs` child files for unit
  tests. A shared private `test_support` module may hold reusable runtime fakes;
  integration suites remain under the crate's `tests/` directory.
- `cqrs-runtime/tests/postgres.rs` roots the PostgreSQL suite. Its
  `postgres/support.rs` owns the shared database fixture; persistence, queue,
  processing and worker modules own persisted behavior. Service gateway tests
  under `account-managements-service/tests/cqrs_gateway.rs` use real
  `CqrsFramework` and `MemStore`.

## HTTP errors and mappings

- A `ResourceError` represents semantic failures in that resource's HTTP
  contract, not framework or infrastructure error types.
- Convert request rejections, domain validation errors, and application errors
  to `ResourceError` at the presentation boundary, normally with `From`.
- Implement `IntoResponse` once on `ResourceError`; it maps every variant to
  the resource's status, response code, and safe message through the shared
  HTTP primitive.
- A handler returns `Result<SuccessResponse, ResourceError>` and uses `?` for
  these conversions. Its success response contains the response DTO and the
  intended status code.

For example, a use case can accept `Json<CreateRequest>`, turn it into domain
values, call an application service, and return
`Result<(StatusCode, Json<CreateResponse>), UserAccountsError>`.

Cover the success path and every `ResourceError` mapping with transport-level
tests: malformed input, domain validation failure, expected conflict, and an
unavailable dependency whose details must remain hidden.
