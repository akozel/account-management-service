# Task processing contract

Status: accepted on 2026-10-10.

Business sequence is guaranteed by aggregate state and accepted transitions.
Outbox tasks may execute concurrently and in any order. This contract applies
to application task handlers and the reusable `cqrs-runtime` queue and workers.

## Aggregate and application responsibilities

The aggregate checks the current phase and command invariants. A pending
phase rejects conflicting transitions until a matching completion command
records the result. Concurrent commands are checked against aggregate state;
optimistic persistence conflicts are handled by the existing CQRS executor.
Application attaches explicit tasks to the accepted event, and persistence
commits that event, its optional snapshot, and the tasks atomically.

Create a dependent task only with the event that establishes its prerequisite.
Do not enqueue all stages of a dependent pipeline at initiation and expect
aggregate state to stop already-published external work. A state transition
protects commands to the aggregate; it cannot revoke a task or an external
request already issued by a worker.

For the current registration workflow:

1. `RegistrationProfileAccepted` moves `EmailReservation` to `CreatingAccount`
   and is committed with `CreateAccountTaskV1`.
2. The task commands `UserAccount::CreateAccount`; `AccountCreated` and
   `RecordAccountCreatedTaskV1` are committed together.
3. The completion task commands `EmailReservation::RecordAccountCreated`.
   The aggregate verifies the matching result and enters `AccountCreated`.

The completion task may execute before `CreateAccountTaskV1` is acknowledged:
the account event already proves its prerequisite. Task settlement is not a
workflow transition. Each aggregate owns its own consistency boundary;
cross-aggregate stages communicate through durable tasks.

If a workflow must wait for several parallel results, represent the outstanding
results in aggregate state and publish the next stage only when the required
completion transitions have been accepted. If advancement depends on an
external effect, its confirmed result needs a separate completion operation
in the workflow; an initiation event alone does not prove that result. This
contract does not add such a workflow to the current verification-code sender.

## Queue and worker responsibilities

The queue claims eligible tasks by supported format and readiness, with
database ownership of each task. It does not serialize work by aggregate or
interpret business dependencies. Workers execute claimed tasks within their
configured concurrency and settle their individual outcomes.

`aggregate_sequence` identifies the source event. Several tasks may share one
sequence, and some events may create no tasks. It is not a task position or a
delivery barrier. Tasks with the same sequence may run concurrently; tasks
with higher sequences may be claimed while earlier tasks are pending or
processing. A task author's workflow must remain valid for these schedules.

There is no sequential/parallel flag, ordering group, or per-aggregate FIFO
guarantee in this contract. `available_at` expresses scheduling and retry
backoff, not a business dependency. A task's `completed` or `failed` status
does not advance aggregate state automatically.

This decision requires no change to worker dispatch, claim SQL, handler
signatures, the schema, or persisted task formats. Existing registration
handlers already delegate their operations to aggregate-backed services.

## Handler and sender responsibilities

One task represents one retryable operation: one target-aggregate command or
one external effect. A handler must not combine several aggregate commands
into one task's pipeline. The operation that produces another event commits
its follow-up work atomically with that event.

Delivery can repeat, including after a successful operation whose separate
acknowledgement was lost. Target commands must classify exact already-applied
operations distinctly from mismatched data. The handler treats the former as
successful delivery and classifies retryable and permanent failures according
to that task's business contract. Generic `AlreadyExists` is insufficient
unless all operation-defining data has been checked.

An external operation may store its required idempotency key in either:

- its versioned payload, when the key belongs to the operation's wire contract;
- task metadata, when the key belongs to the adapter's delivery context.

The sender contract defines how the adapter obtains the key. Persist it before
the first attempt and keep it, and the effect-defining request data, unchanged
across retry, recovery, and manual requeue. `task.id` can identify redelivery
of one task and is already exposed as `DeliveryContext.task_id`; identifying
one logical effect represented by different tasks needs a shared operation
key. New intentional effects need distinct identities. Neither an attempt
number nor a lease lock token is an idempotency key.

No dedicated runtime key column or deduplication API is required by this
contract. Storing or forwarding a key does not itself deduplicate effects;
the receiving system's behavior is part of the sender contract. The current
console sender permits repeated output.

## Handler retry and durable task redelivery

The handler's retry policy and the outbox's task delivery policy are separate
contracts. A handler may perform bounded local retries of its one operation
within a single invocation, choosing retryable causes, delays, and the local
attempt budget. After those attempts, it classifies the invocation as success,
retryable failure (with an optional redelivery delay), or permanent failure.
Its policy must state whether an optional effect may be omitted; an unavailable
required effect is not successful completion.

`completed` means the operation's contract was satisfied. A timeout or lost
response leaves its result unknown; the effect may have occurred. `failed`
means automatic processing stopped, not proof that the effect did not happen.
Manual requeue must preserve the operation's identity and replay safety.

The outbox owns durable delivery of the whole task: persistence, claims,
attempt accounting, backoff, settlement, and expired-lease recovery. The
coordinator also schedules redelivery when the handler times out. A crash,
timeout, or lost acknowledgement can therefore cause another invocation even
if the handler never returned a retryable error. A local decision to stop
retrying does not prevent recovery until a terminal task outcome is durably
settled by its current owner.

Task processing has at-least-once semantics; this does not promise successful
external delivery. Attempts are bounded and may be consumed before execution.
Handlers must be safe to invoke again from the beginning, including after a
previous operation succeeded or while an earlier request is still running.
Use the same operation identity and effect-defining data for local retries
and durable redeliveries. Local retry state may reset on each invocation.

Document both budgets and their combined effect. For example, ten task
deliveries with up to three local sends each can produce thirty send attempts,
including attempts hidden in a sender SDK. Bound local work by the handler's
execution budget; retain CQRS optimistic-conflict retries in the command
executor instead of duplicating them inside the application handler.

## Runtime queue-operation deadlines

Status: implemented. `DefaultOutboxTaskCoordinator` bounds storage calls
independently of handler execution, and `PostgresOutboxQueue` additionally
limits SQL execution on the server.

The runtime boundary must enforce a positive, configurable deadline when
calling queue claim, expired-lease recovery, and settlement. It bounds the
whole adapter operation, including connection acquisition, SQL execution,
transaction completion, and waiting for a response. Handler execution has its
own budget. This contract applies to any queue implementation, independently
of PostgreSQL settings.

After a deadline expires, the runtime stops awaiting that operation, reports
the storage failure, and can continue polling or release the worker slot.
Queue implementations must cooperate with async cancellation and avoid
blocking execution or detached work that retains runtime capacity. Repeated
storage unavailability may prevent delivery, but one stalled await must not
occupy the dispatcher or a worker indefinitely.

Timeout does not prove rollback or cancellation at the receiver. A timed-out
claim may have committed without returning its tasks; expired-lease recovery
handles that durable work. A timed-out settlement may also have committed;
the runtime must not infer success or permanent task failure from the missing
response. Keep token-checked settlement and handler replay safety. Preserve
already confirmed tasks when a later claim sub-operation fails or times out.

Optional PostgreSQL limits can additionally abort SQL that exceeds a server
budget. They do not replace the runtime deadline, bound connection acquisition
or network waiting, or guarantee immediate transaction cleanup. See the
[PostgreSQL example](examples/postgres_queue_timeouts.md).

The service reads two positive millisecond settings from
[`account-managements-service/.env.example`](../account-managements-service/.env.example):

- `OUTBOX_STORAGE_TIMEOUT_MS`, default `5000`: one deadline for the full
  multi-format claim batch, or one recovery or settlement call. Later claim
  timeouts preserve the tasks confirmed by earlier formats. A timeout without
  a confirmed batch returns `OutboxError::TimedOut`, an unknown storage result.
- `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS`, default `4000`: one SQL statement's
  server budget, including lock waiting. The PostgreSQL queue applies
  `set_config('statement_timeout', value, true)` inside each claim, recovery,
  or settlement transaction. Commit/rollback restores the previous connection
  setting, leaving event-store operations on the shared pool unaffected.

The server setting must fit PostgreSQL's `1..=2147483647` millisecond range.
Both configurations reject zero, invalid numbers, and unsupported durations
at startup. Prefer a server budget shorter than the runtime budget, while
accounting for their different starting points and multi-statement recovery.
The handler timeout and lease are separate settings; this change does not
introduce lease renewal or a full remaining-lease check before execution.

Regression tests cover stalled queue calls during normal worker operation,
the shared multi-format claim budget, pool-acquisition deadlines, server
cancellation of lock waits and slow statements, pooled-setting restoration,
rollback of cancelled claims, and lost replies after real commits.

## Remaining open question

[Question 3](known_issues/2026-10-10-task_worker_open_questions.md) is closed by
the implemented runtime and PostgreSQL limits and regression tests. Question
4, budgeting the lease from claim through settlement, still requires
discussion. Question 2 remains closed by handler replay safety and separate
retry policies; none of these decisions implies exactly-once external effects.
