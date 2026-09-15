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
- `infrastructure` implements ports and contains PostgreSQL, `postgres-es`,
  external email, and other I/O integrations. It translates framework and I/O
  failures into gateway failures, but must not interpret their business meaning.
  Do not expose those types through domain APIs.
- `main` only composes dependencies and starts the application.

## Application feature layout

Group application code first by domain capability, then by use case:

```text
src/
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
    outbox.rs
    outbox/{task,queue,task_handler,coordinator,task_handler_registry,error}.rs
```

- `{capability}.rs` declares the feature modules. `service.rs` owns the public
  input service, service errors and factory; `gateway.rs` owns the output port.
  Gateway results use the shared
  `application::CommandGatewayError<DomainError>`; do not define a
  feature-specific gateway-error enum. `use_cases.rs` declares the private
  workflows and their shared retry constants. Its `test_support.rs` is
  compiled only for unit tests and holds the fake gateway.
- `{capability}/use_cases/{use_case}.rs` implements one workflow. It
  creates the domain command, calls the feature gateway, and follows
  [Error boundaries](#error-boundaries). It does not define a public API for
  adapters to import.
- Random IDs, codes, and tokens used by a workflow come from an injected
  application port implemented by infrastructure. Generate them before
  issuing the command; events carry the accepted values so replay never
  samples randomness again.
- Versioned durable task DTOs live under `{capability}/tasks/{task}/v1.rs`;
  `{task}.rs` owns the current `TaskHandler` and any sender port. Keep persisted DTOs
  readable while queued work can still be retried.
- Presentation imports only `application::{capability}::service::{FeatureService,
  FeatureServiceError}`. Infrastructure implements the feature's
  `gateway::FeatureGateway`. `main` composes a gateway through the service factory and
  passes the resulting service to presentation.
- Do not import a nested use case from outside its feature. Use Rust 2024 file
  modules; do not introduce `mod.rs`.

### Feature layout example

```text
src/application/invoice.rs                 Declares gateway, service, use_cases
src/application/invoice/gateway.rs         InvoiceGateway output port
src/application/invoice/service.rs         InvoiceService input port, errors, factory
src/application/invoice/use_cases.rs       Private workflow declarations
src/application/invoice/use_cases/approve.rs  Command and error mapping
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
- `application::CommandGatewayError<DomainError>` is the shared output-port
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

## Infrastructure layout

Group outbound adapters by infrastructure technology:

```text
src/
  infrastructure.rs
  infrastructure/
    cqrs.rs
    cqrs/
      {feature}_gateway.rs
    postgres.rs
    postgres/
      {adapter}.rs
```

- `infrastructure/cqrs/{feature}_gateway.rs` implements the feature gateway. It accepts
  the domain command created by application, derives any framework aggregate id,
  executes it through `cqrs-es`, and follows [Error boundaries](#error-boundaries).
  It must not map a domain rejection or conflict to a service error.
  Command gateways do not expose reconstructed aggregate state to the
  application. The CQRS engine loads state internally to decide a command and
  uses optimistic version checks on commit. Retry a conflict by executing the
  command again; build query endpoints from event streams and projections.
- `infrastructure/postgres/{adapter}.rs` owns `sqlx`, `postgres-es`, SQL, and
  PostgreSQL transaction behavior. It may implement a CQRS persistence trait;
  that is still a PostgreSQL adapter.
- Do not add `pub use` aliases that flatten technology modules. Import a
  concrete adapter through its technology path.
- Runtime diagnostics use structured `tracing` events at the adapter boundary
  that observes the outcome; do not write directly with `println!`, `eprintln!`,
  or process-global stdout/stderr handles. `infrastructure::logging` owns the
  subscriber and logging-backed outbound adapters, while `main` only installs
  it. The default level is `info`, configurable through `RUST_LOG`. Keep domain
  code free of logging and never include outbox payloads or secrets in failure
  events.
- `main` is the only composition root. It constructs the PostgreSQL adapter,
  event store, CQRS framework, feature gateway, feature service, and router in
  that order. Infrastructure modules must not provide factories that compose
  several technologies.

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
are exhausted, persist `Failed` and write the confirmed transition to stderr,
including failures found during lease recovery. Log task identity, attempt,
and reason, not secrets or payload. `Failed` does not prove the aggregate
command never committed.

For `CreateAccountTask` specifically, `UserAccount::CreateAccount` rejects an
exact repeat as `AlreadyCreated` after checking the account ID, email, and
canonical profile. Its TaskHandler treats that rejection as `Completed`; a
different existing account is a separate permanent failure.

Use cases choose between `FeatureGateway::execute(command)` (events only) and
`execute_with_outbox(command, tasks)` (events and explicit durable work).
Outbox tasks are application contracts with independent payload versions;
do not add delivery controls to domain events or serialize tasks into event
metadata. Failed commands or commits must not leave queued work. With the
current prebuilt-task gateway, ensure the task fields match the event that the
aggregate can actually emit; a rejected exact repeat must not enqueue a new
Task. Exact-repeat success is reserved for internal at-least-once Task handling.
A client profile submission consumes its account-creation token with the first
accepted command; every later submission with that consumed token is a semantic
rejection, even when the request is identical.

`main` injects a `build_framework` closure into each CQRS gateway. For each
command, it creates a PostgreSQL repository carrying that call's immutable
task list, then the standard event store and CQRS framework. This keeps
construction of multiple technologies in the composition root while
preserving `cqrs-es` snapshot and replay behavior. Share pools, never mutable
per-command task buffers.

```rust
let gateway = CqrsInvoiceGateway::new(move |tasks| {
    let repository = PostgresEventRepositoryWithOutbox::new(pool.clone())
        .with_outbox(tasks);
    let store = PersistedEventStore::new_snapshot_store(repository, 25);
    CqrsFramework::new(store, vec![], ())
});
```

Application owns task contracts, queue and handler ports, and the
`OutboxTaskCoordinator` for claim, timeout, retry and settlement. A `TaskHandlerRegistry`
rejects empty and duplicate registrations and routes each type/version to one
handler. The composition root builds a separate registry for every worker and
derives the coordinator's claim formats from it. Unknown formats stay pending.
PostgreSQL owns atomic claims, leases, token-checked settlements
and expired-lease recovery. Concrete senders live under their infrastructure
technology. Versioned wire DTOs are converted into current handler inputs.
The outbox uses `pending`, `processing`, `completed`, and `failed` statuses.
`pending.available_at` is the earliest claim time for both initial schedules
and retry backoff; a zero-delay retry becomes available immediately. A claim
increments attempts and grants a lease and lock token. Only `processing` has
lease ownership; only `completed` and `failed` have `finished_at`. Settlement
and expired-lease recovery return confirmed `failed` transitions to the worker
for safe stderr logging. Unknown formats stay `pending` without being claimed.

Background polling is an inbound adapter under `presentation/outbox_worker_pool.rs`. Its
Tokio supervisor owns bounded worker mailboxes, cancellation, polling and worker
restarts; `outbox_worker_pool/worker.rs` invokes the application coordinator with its
registry. `main` composes the queue, coordinator, handler factory and supervisor
alongside HTTP. Configuration
parsing/validation stays outside `main` so it remains testable and covered.

## Presentation layout

Organize HTTP adapters by resource, then use case:

```text
src/
  presentation.rs
  presentation/
    http.rs
    http/{resource}.rs
    http/{resource}/
      {use_case}.rs
    outbox_worker_pool.rs
    outbox_worker_pool/worker.rs
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

- `src/main.rs` is the composition root. Exclude it from line coverage because
  it only wires dependencies and starts the process; do not add brittle unit
  tests around process startup.
- Keep `src/presentation/http.rs` in coverage. Test its top-level router through an
  in-memory Axum request so route composition is verified without PostgreSQL.
- Keep infrastructure adapters in coverage until their integration-test
  strategy is explicitly decided; do not exclude them merely to satisfy the
  global threshold.

## Test ownership

- Unit-test use-case command, error and code-regeneration matrices beside each
  workflow, using the fake gateway in `use_cases/test_support.rs`.
- HTTP tests send in-memory Axum requests to a stub input service. They do not
  construct an application service or gateway.
- Unit-test payload construction in `outbox/task.rs`, retry and timeout in
  `outbox/coordinator.rs`, format routing in `outbox/task_handler_registry.rs`, v1
  task decoding in the handler, output failures in the sender, and worker
  configuration, supervision and shutdown beside the worker pool and worker.
- `tests/outbox.rs` roots the PostgreSQL suite. Its `support.rs` owns the shared
  database fixture; `persistence.rs`, `queue.rs`, `processing.rs` and `worker.rs`
  own persisted behavior. `tests/cqrs_gateway.rs` uses real `CqrsFramework`
  and `MemStore`.

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
