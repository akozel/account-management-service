# cqrs-runtime

Workspace-local reusable CQRS/event-sourcing runtime. It contains no account
management domain or use-case types.

The base crate provides generic command execution, optimistic-conflict retry,
durable task contracts, handler routing, and task coordination. Optional
features add concrete runtime adapters:

- `postgres`: `PostgresEventRepositoryWithOutbox`, `PostgresOutboxQueue`, and
  the canonical SQL assets under `sql/`;
- `worker`: the Tokio outbox supervisor and worker pool.

Both features are enabled by default. A consumer that only needs the portable
contracts can depend on the crate with `default-features = false`.

`PostgresEventRepositoryWithOutbox` deliberately supports no more than one
domain event per command. It rejects multi-event batches, snapshot-only writes,
and outbox tasks without an event before issuing SQL. One accepted event, its
optional snapshot, and all explicit tasks are committed by one atomic
PostgreSQL statement.

## Task execution contract

Business sequence is enforced by aggregate state and accepted transitions.
Application commits a dependent task only with the event that establishes its
prerequisite. Workers may execute tasks concurrently and in any order,
including tasks from one aggregate or tasks sharing `aggregate_sequence`.
The sequence identifies the source event; it is not a delivery barrier.

Each handler performs one target command or external effect. A committed
successor may execute before the original task's separate acknowledgement.
Required external idempotency keys belong in the versioned payload or task
metadata and are passed unchanged to the sender; `task.id` identifies repeats
of one task. No sequential/parallel flag is required by this contract.

Handlers may define bounded local retries within one invocation. Outbox
delivery is a separate policy that can restart the whole task after a crash,
handler timeout, or expired lease. A terminal handler decision takes effect
only after durable settlement. Account for both attempt budgets and reuse
the same operation identity at both levels.

See the [accepted task processing contract](../doc/task_processing_contract.md)
for responsibilities and the [open worker questions](../doc/known_issues/2026-10-10-task_worker_open_questions.md)
for delivery uncertainty, storage deadlines, and lease budgeting.

The coordinator enforces `OUTBOX_STORAGE_TIMEOUT_MS` (default `5000`) for
the whole claim batch, recovery, or settlement call. Claims across formats
share one deadline and return already confirmed work if a later format times
out. `OutboxError::TimedOut` means the storage result is unknown; worker
capacity is released and lease recovery remains responsible for durable work.

The PostgreSQL queue applies `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS` (default
`4000`) to each SQL statement in its transactions, including lock waits.
Transaction-local settings restore the connection's previous values on
commit/rollback; event-store operations on the shared pool keep their own
policy. Both settings must be positive; the server setting must fit a signed
32-bit millisecond value. Configuration parsing/validation lives in
`OutboxTaskCoordinatorConfig` and `PostgresOutboxQueueConfig`; the composition
root injects them through `outbox_task_coordinator_with_config` and
`PostgresOutboxQueue::with_config`. The simpler constructors use these defaults.
See the [transaction-local timeout example](../doc/examples/postgres_queue_timeouts.md).

The worker validates `lease > 2 * storage_timeout + handler_timeout + 5s`.
The handler's budget includes all local attempts and backoff; a retry count
is distinct from a total attempt count. Defaults leave additional headroom:
`60s > 5s + 30s + 5s + 5s = 45s`.

Each claimed task carries a conservative monotonic lease deadline from before
its DB claim request, preserved through collection and dispatch. The
coordinator runs the handler only if its full budget and settlement reserve
still fit. Otherwise it retries without executing the handler, or leaves the
task for durable recovery if no safe acknowledgement window remains.
Settlement cannot await beyond the original deadline minus the fixed margin.
This adds no persisted fields, schema migration, heartbeat, or lease renewal.
