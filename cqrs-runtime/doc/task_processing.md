# Task processing

This is the runtime contract for integrating task handlers, queues, and workers
in another application. The engine owns delivery and lease management;
application aggregates own business transitions.

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
| `TaskExecutionError::Retry` | `pending` with backoff, or `failed` when the attempt budget is exhausted. |
| `TaskExecutionError::Permanent` | `failed`: automatic processing stopped. |

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
or task metadata and pass it unchanged to the sender. `task.id` identifies
repeats of one task; different tasks for one logical effect need a shared
operation key. Keep effect-defining data stable across local retry, recovery,
and manual requeue. Attempts and lock tokens are not operation identities.
The receiver must provide the promised deduplication, including concurrent
requests and the allowed replay period; storing a key alone does not do so.

Business handlers use payload, metadata, and operation identity.
`lock_token` and `lease_deadline` are platform context: do not manufacture,
reset, or modify them inside business logic. Domain events do not contain
delivery controls.

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
With a thirty-second handler cap, four ten-second attempts do not fit even
before backoff. For example, a ten-second initial attempt, three retries capped
at four seconds, and three two-second backoffs use twenty-eight seconds,
leaving two seconds for overhead. This is an example policy, not built-in
sender retry behavior.

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

## Timeouts and leases

| Environment setting | Default | Owner and scope |
| --- | --- | --- |
| `OUTBOX_WORKER_CONCURRENCY` | `4` | Worker slots per process. |
| `OUTBOX_POLL_INTERVAL_MS` | `1000` | Worker polling interval. |
| `OUTBOX_HANDLER_TIMEOUT_SECS` | `30` | Total time for one handler invocation, including local retry/backoff. |
| `OUTBOX_LEASE_SECS` | `60` | Queue ownership duration per claim. |
| `OUTBOX_STORAGE_TIMEOUT_MS` | `5000` | Coordinator deadline for the whole multi-format claim batch, or one recovery/settlement call. |
| `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS` | `4000` | PostgreSQL deadline for each statement in queue transactions, including lock waiting. |

Use the configuration types' `from_env` methods to load these settings.
Constructors without explicit configuration use defaults; they do not read
environment variables automatically. Positive values are required. The
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
lease > claim budget + handler budget + settlement budget + safety margin
lease > 2 * storage_timeout + handler_timeout + LEASE_SAFETY_MARGIN
defaults: 60s > 5s + 30s + 5s + 5s = 45s
```

`LEASE_SAFETY_MARGIN` is a fixed five-second reserve; defaults leave fifteen
seconds extra. Shutdown drains one remaining handler budget plus the actual
settlement timeout.

The queue anchors `ClaimedTask.lease_deadline` to a monotonic instant before
requesting ownership. It is a conservative, process-local budget that includes
connection waits, transport, commit, collection, dispatch, and mailbox delay.
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
   adapter's database. Compose repositories, event stores, and command gateways
   in the host's composition root.
4. Implement handlers/sender ports, then build a `TaskHandlerRegistry` per
   worker. It rejects empty and duplicate registrations. Each factory invocation
   must expose the same ordered format list as the coordinator.
   Use `#[async_trait::async_trait]` for the async handler/queue implementations.
5. Load and validate `PostgresOutboxQueueConfig`,
   `OutboxTaskCoordinatorConfig`, and `OutboxWorkerPoolConfig`. Compose the
   queue/coordinator and validate the full lease budget before accepting traffic.
   Start `worker::run` with a shutdown receiver; the host owns process lifecycle.
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
[TaskHandler](../src/outbox/task_handler.rs),
[OutboxQueue/ClaimedTask](../src/outbox/queue.rs),
[coordinator](../src/outbox/coordinator.rs), and [worker](../src/worker.rs).
