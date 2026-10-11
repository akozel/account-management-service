# Task processing

This is the runtime contract for integrating task handlers, queues, and workers
in another application. The engine owns delivery and lease management;
application aggregates own business transitions.

For component boundaries, relationships, and the order of calls, see
[Architecture and call flow](architecture.md).

## Ownership and delivery

| Owner | Responsibilities |
| --- | --- |
| Domain/application | Check aggregate phases, publish dependent work with the event that establishes its prerequisite, and define completion of the operation. |
| Business handler/sender | Perform one operation, classify its result, and make repeated or concurrent execution safe. |
| Runtime coordinator/worker | Route supported formats, bound execution and storage waits, settle outcomes, and recover expired ownership. |
| Queue adapter | Atomically claim tasks, count attempts, provide a conservative local lease deadline, and fence settlement in storage. |

An accepted event, its optional snapshot, and explicit tasks are committed
atomically by `PostgresEventRepositoryWithOutbox`. A rejected command or rolled-back
write commits neither events nor tasks. A lost write response may leave an unknown
commit outcome; atomicity does not prove nothing committed. The adapter supports
at most one event per command:
multi-event batches, snapshot-only writes, and tasks without an event are
rejected. Supporting more requires a separate snapshot-contract change.

Task execution has at-least-once semantics. A successful operation can be
redelivered after a lost acknowledgement, timeout, crash, or lease recovery.
The finite attempt budget does not guarantee successful external delivery or
even an initial handler invocation. Unknown formats remain pending until a
compatible worker exists. Progress requires healthy storage and polling.

Tasks may run concurrently and in any order, including tasks from one source
aggregate or tasks sharing `aggregate_sequence`. That sequence identifies the
source event; it is not a task position or a delivery barrier. There is no
per-aggregate FIFO or sequential/parallel flag.

## Business handlers

One task represents one target-aggregate command or one external effect.
For another dependent operation, the accepted command commits its event and
the next task atomically. Do not enqueue a whole dependent pipeline before its
prerequisites are confirmed. Aggregate state cannot stop an external task
that has already been published.

A committed successor may run before its predecessor's task acknowledgement:
the prerequisite event is already durable. If several results are required,
track the outstanding completions in aggregate state. Task settlement does
not advance that state automatically.

Handlers return:

| Result | Durable outcome after confirmed settlement |
| --- | --- |
| `Ok(())` | `completed`: the operation's contract was satisfied. |
| `TaskHandlerError::Retry` | `pending` with backoff, or `failed` when the attempt budget is exhausted. |
| `TaskHandlerError::Permanent` | `failed`: automatic processing stopped. |

The handler may omit an optional effect only if its business contract permits
that result. An unavailable required effect is not successful completion.
`failed` does not prove the operation never committed.

Make every invocation safe to repeat from the beginning, including while a
previous remote request is still running. For target commands, distinguish an
exact already-applied operation from mismatched data; a generic `AlreadyExists`
is not enough. Mutable operations may need an operation receipt committed
atomically with their changes.
Keep mutable sender state usable after cancellation of the invocation future.

For external effects, persist a stable idempotency key in the versioned payload
or task metadata and pass it unchanged to the sender. `task.id()` identifies
repeats of one task; different tasks for one logical effect need a shared
operation key. Keep effect-defining data stable across local retry, recovery,
and manual requeue. Attempts and lock tokens are not operation identities.
The receiver must provide the promised deduplication, including concurrent
requests and the allowed replay period; storing a key alone does not do so.

Business handlers receive `&TaskDelivery`: immutable ID, format, payload,
metadata, and positive attempt number through getters. They do not receive
`ClaimedTask` or `ClaimLease`. The latter holds the runtime's ownership token
and conservative monotonic deadline; only queue/coordinator code uses it.
Domain events do not contain delivery controls.

`TaskFormat`, `NewOutboxTask`, and `TaskDelivery` have checked constructors and
private fields. Format names are nonempty, versions/attempt limits are positive,
and payload/metadata must be JSON objects. This structural validation does not
validate a version's business schema: the handler must decode and classify it.
`NewOutboxTask::scheduled`, `with_metadata`, and `with_max_attempts` are fallible
builders that preserve the generated task ID. Defaults remain now/empty metadata/
ten total claims. Invalid DTOs fail before SQL; database constraints still fence
direct writers and enforce durable lifecycle invariants.

## Retry and task lifecycle

Local handler retries repeat its one operation within a single invocation.
Durable outbox redelivery repeats the entire handler. A crash, handler timeout,
or lost acknowledgement can cause redelivery without a returned retryable
error. Stopping local retries does not suppress recovery until a terminal
outcome is durably settled.

Document local, SDK, and durable attempt budgets together. Ten task deliveries
with up to three sends each can cause thirty send attempts. The total handler
budget includes every local attempt, backoff, and overhead:

```text
handler budget >= sum(attempt durations) + sum(local backoff) + overhead
```

A retry count excludes the initial attempt: three retries can mean four sends.
With the default fifteen-second cap, three attempts capped at four seconds and
two one-second backoffs use fourteen seconds, leaving one second for overhead.
This is an illustrative policy, not built-in sender retry behavior.

`pending.available_at` is the earliest claim time, for initial scheduling or
retry backoff. Claim increments `attempts` and grants ownership.
`processing` has an owner, token, and expiry; `completed` and `failed` are
terminal and have `finished_at`. Recovery invalidates expired tokens and
returns work to pending with backoff, or fails an exhausted task.
Confirmed failed transitions are logged with task identity, attempt, and a safe
reason, without payloads or secrets.

Manual requeue of failed tasks uses [requeue-failed-outbox.sql](../sql/requeue-failed-outbox.sql).
It preserves ID, payload, and metadata and resets the delivery counter.
Check the receiver's replay/deduplication contract before replaying old effects.
Completed tasks are not requeued by this statement.

## Requested and confirmed transitions

`SettlementRequest::{Completed, Retry, Failed}` is the coordinator's request,
not a statement that storage accepted it. `SettlementResult` reports only
post-commit `Completed`, `RetryScheduled`, or `Failed(TaskFailure)`;
`OwnershipLost` confirms no owned transition. `is_applied()` includes retry.
Do not infer completion merely from an applied settlement.

`TaskFailureCause::{HandlerPermanent, AttemptLimitReached, LeaseExpired}`
is fixed safe diagnostic context. The handler's persisted reason remains
separate and must not contain secrets. `LeaseRecoveryReport::processed_count()`
includes both rescheduled work and confirmed terminal failures; use
`retried_count()` and `failures()` to distinguish them.

## Timeouts and leases

| Environment setting | Default | Owner and scope |
| --- | --- | --- |
| `OUTBOX_WORKER_CONCURRENCY` | `128` | Permanent Tokio workers / concurrent deliveries per process, including settlement. |
| `OUTBOX_QUEUE_CAPACITY` | `128` | Claimed deliveries waiting locally; independent of worker count. |
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | Worker polling interval. |
| `OUTBOX_HANDLER_TIMEOUT_SECS` | `15` | Total time for one handler invocation, including local retry/backoff. |
| `OUTBOX_LEASE_SECS` | `60` | Queue ownership duration per claim. |
| `OUTBOX_STORAGE_TIMEOUT_MS` | `6000` | Coordinator deadline for the whole multi-format claim batch, or one recovery/settlement call. |
| `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS` | `4000` | PostgreSQL deadline for each statement in queue transactions, including lock waiting. |

Use the configuration types' `from_env` methods to load these settings.
Constructors without explicit configuration use defaults; they do not read
environment variables automatically. Positive values are required. Queue capacity must fit the runtime's validated range;
zero-capacity rendezvous is not supported. Defaults for concurrency and queue
capacity are independent, even when only one environment variable is overridden. The
PostgreSQL setting must fit `1..=2147483647` milliseconds; the other types
validate their supported duration ranges.

The runtime storage deadline includes connection acquisition, SQL, commit,
and response waiting. All formats in a claim batch share one deadline;
a later timeout preserves tasks confirmed by earlier claims.
`OutboxError::TimedOut` means an unknown storage result. The runtime releases
its waiting capacity; a claim or settlement may nevertheless have committed.

PostgreSQL uses `set_config('statement_timeout', value, true)` in each queue
transaction. Commit/rollback restores the connection's previous setting;
event-store operations on the shared pool keep their own policy. Prefer a
server budget shorter than the runtime budget, while accounting for different
clock start points and multi-statement recovery. The server setting does not
bound pool acquisition or network waiting.

Worker startup validates the complete budget against the coordinator's
effective storage timeout:

```text
C = concurrency; Q = queue_capacity; S = storage_timeout; H = handler_timeout
waves = ceil(Q / C)
lease > 2*S + H + LEASE_SAFETY_MARGIN + waves*(H+S)
defaults: C=128, Q=128, lease=60s > 12s + 15s + 5s + 21s = 53s
example: C=2, Q=3 requires lease > 12s + 15s + 5s + 2*21s = 74s
```

`LEASE_SAFETY_MARGIN` is a fixed five-second reserve; defaults leave seven
seconds extra. The queued-wave allowance is conservative for a healthy pool;
it does not guarantee timely execution after panic, scheduler pauses, or storage
failures. Original per-delivery deadline checks still apply. Larger queues require
longer leases or higher concurrency; skipped invocations still consume attempts.

## Worker lifecycle and bounded admission

Every permanent Tokio worker owns one mutable handler registry and performs one
delivery at a time, through handler and settlement. Registry/sender state survives
between deliveries until the worker exits. Concurrency is a process-wide setting,
independent of CPU count or runtime OS threads; HTTP I/O can overlap on one thread.
It does not bound remote effects that continue after a local timeout.

A bounded async-channel queue holds at most Q waiting deliveries. Each worker
owns a cloned receiver and awaits recv directly, without an external receiver
mutex. There is exactly one private sender, owned by the poller.
Before claim, the poller snapshots free capacity as Q - sender.len(). Consumers
can only reduce occupancy during that await, so a batch no larger than this
limit fits through try_send without permits or a second buffer. Full channels
cause no claims; unclaimed work stays pending in PostgreSQL. Changing to multiple
producers would require a new admission protocol.
Q bounds element count, not payload bytes or durable backlog. The pending claim
batch plus queued deliveries fits Q; locally known deliveries fit C+Q. An unknown
claim result can leave additional processing rows in PostgreSQL for recovery.
Cancelling a pending receive does not consume a delivery, including when a sender
has notified the waiter before it is polled again.

Dequeue sends a coalesced Notify to refill the channel without a polling delay.
The interval remains a fallback and runs recovery in batches of min(C, 32), because
recovery currently performs one SQL update per expired row. No new claims/recoveries
are started concurrently by the poller, and completion notifications do not cancel
a storage await already in progress.

JoinSet observes worker exits and panics without waiting for a polling tick. Each
exit removes one Tokio task ID and recreates one checked registry/task for that
worker index. An in-progress bounded storage await may delay this observation.
Panic payloads are not logged by supervision; panic=abort ends the process.

`worker::run` takes a `tokio_util::sync::CancellationToken`. Its clones share
one cancellation request between the host, supervisor, and permanent workers.
Cancellation stops claims and dequeue, including while the supervisor awaits
storage. Workers prioritize cancellation over recv and check it again before
execution. A ready claim batch is discarded if cancellation has been observed;
its already-committed ownership remains for lease recovery.

Startup configuration/registry errors cancel the supplied token before returning.
Restart-factory errors and format mismatches cancel it **before** draining healthy
workers, notifying the host immediately, and return the original `WorkerPoolError`
after drain. A worker panic alone does not cancel it: the supervisor restarts that
worker. A host that needs to isolate a pool passes `parent.child_token()`; cancelling
that child does not cancel the parent. Factories are synchronous and must return
promptly; cancellation cannot preempt a blocking factory call.

The token is cancelled before explicitly closing the channel, because closure
alone still permits receiving buffered deliveries. Only already-started
handler/settlement work drains, for one common H+S deadline (21 seconds with
default budgets). Cancellation is not raced against `execute_and_settle`: current
work retains its original handler, storage, and lease deadlines. Waiting claims
remain durable for expiry recovery. At the drain deadline, remaining workers are
aborted and their join results are observed before return.

The supervisor holds a token drop guard from startup. Dropping/aborting a polled
supervisor or unwinding its panic requests host cancellation and aborts workers
through JoinSet's drop behavior; it cannot perform asynchronous graceful draining.
Dropping an ordinary token handle does not cancel it. Lifecycle owners should
hold a drop guard and explicitly cancel/await components for normal shutdown.
This replaces the former watch receiver API, including its sender-closure and
false-update semantics; no compatibility adapter is provided.

The service host gives HTTP and outbox the same token. It observes process signals,
token cancellation, and critical component exits in that priority order (HTTP
before outbox when neither stop request is ready), then awaits both components.
An unexpected component exit remains the primary failure. With a signal/token
request, failure priority is signal-observation error, outbox failure, then HTTP
failure; other observed failures are logged separately. Supervisor task diagnostics
use task ID and panic/cancellation flags, not JoinError's payload-bearing formatting.
The ordinary panic hook is independent of these structured diagnostics.

HTTP and outbox drain concurrently. An in-flight HTTP request or handler may commit
new durable tasks after outbox admission stops; they are processed by a subsequent
healthy pool. Shutdown does not promise completion of the whole business workflow.
The database pool closes after both components finish. Standard Axum graceful
shutdown stops accepting new TCP connections and waits for current requests.
HTTP shutdown and pool cleanup have no process-wide deadline
([known issue #10](../../doc/known_issues/http_shutdown.md)). The development
console sender performs synchronous stdout writes, which cannot be preempted by
async timers or abort; this remains backlog task #11.

The queue anchors `ClaimedTask::lease().deadline()` to a monotonic instant before
requesting ownership. It is a conservative, process-local budget that includes
connection waits, transport, commit, collection, dispatch, and local queue delay.
Preserve it through clones/dispatch; never restart its clock or derive it by
comparing database UTC with the worker's wall clock.

The coordinator starts the handler only if its full budget, settlement time,
and margin still fit. Otherwise it requests retry without running the effect.
If safe settlement time is exhausted, `LeaseBudgetExhausted` leaves the task
for recovery. Skipped execution still consumes the claim attempt.
Settlement is bounded by the earlier storage deadline and original lease
deadline minus margin.

These estimates do not prove current database ownership. Storage token/expiry
checks remain authoritative. Timers require cooperative async code and cannot
preempt blocking work. Cancellation does not retract a remote request already
accepted by a receiver. There is no heartbeat/renewal or exactly-once guarantee.

## Integrating another application

1. Keep versioned `TaskPayload` DTOs readable while persisted tasks can still
   be retried. Use object payloads, nonempty type names, and positive versions.
   DTOs implement `serde::Serialize`; handlers decode their supported versions.
   Domain event and task payload versions are independent.
2. Publish canonical operation data with accepted events. Keep per-command task
   lists immutable; share pools, not mutable task buffers. The receiver checks
   its own aggregate invariants. Carry required source data in the payload;
   do not make the worker rebuild the source aggregate from its event history.
3. Apply [001-event-store.sql](../sql/001-event-store.sql) and
   [002-transactional-outbox.sql](../sql/002-transactional-outbox.sql) to the
   adapter's database. In the host's composition root, build
   `postgres::command_executor::<A>(pool, PostgresCommandConfig)` and inject the
   returned `Arc<dyn command::CommandExecutor<A>>` into the aggregate gateway.
   The gateway selects the aggregate ID. The factory uses unit services, no
   queries, and a checked positive snapshot interval (default 25); it performs
   no I/O or environment lookup. Use `CqrsCommandExecutor::new` for custom
   framework factories. Current upstream snapshots require repeat-tolerant
   event application; see the [snapshot issue](../../doc/known_issues/cqrs_snapshot_event_reapplication.md).
4. Implement handlers/sender ports, then build a `TaskHandlerRegistry` per
   permanent Tokio worker. It rejects empty and duplicate registrations. Each fallible `HandlerRegistryFactory` invocation
   must expose the same ordered format list as the coordinator.
   Use `#[async_trait::async_trait]` for the async handler/queue implementations.
5. `OutboxWorkerPoolConfig::new(concurrency, queue_capacity, poll_interval,
   handler_timeout, lease_duration)` requires both independent limits. Load and validate `PostgresOutboxQueueConfig`,
   `OutboxTaskCoordinatorConfig`, and `OutboxWorkerPoolConfig`. Compose the
   queue/coordinator and validate the full lease budget before accepting traffic.
   Start `worker::run` with a cancellation token; the host owns process lifecycle
   and may share that token with its other critical adapters. Fatal supervisor
   errors cancel the token before draining; await its result to obtain the error.
   Handle `WorkerPoolError` separately from configuration validation errors.
6. For another queue adapter, enforce atomic claims, attempt limits, durable
   schedules, conditional token/expiry settlement, and expired ownership
   recovery. Supply a conservative monotonic deadline for every claim and
   support cancellation of waits. Business handlers should remain unchanged.
7. Exercise lost replies after real commits, concurrent repeats, expired/stale
   ownership, partial multi-format claims, dispatch delay, and storage timeouts.
   Relevant regression suites are [processing](../tests/postgres/processing.rs),
   [timeouts](../tests/postgres/timeouts.rs), and [worker](../tests/postgres/worker.rs).
8. Define receiver-specific idempotency retention, alerts for failed/unsupported
   work, and terminal-row retention. The engine has no automatic purge/archive.
   Retain failed work required for recovery.

The APIs and their implementer contracts are documented beside
[TaskHandler](../src/outbox/handler.rs),
[OutboxQueue](../src/outbox/queue.rs),
[delivery/lease](../src/outbox/delivery.rs),
[coordinator](../src/outbox/coordinator.rs), and [worker](../src/worker.rs).
