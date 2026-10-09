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
