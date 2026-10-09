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
