# Architecture and call flow

The diagrams describe the PostgreSQL integration with the `worker` feature.
`cqrs-es` supplies the framework and event store; the host supplies aggregates,
gateways, handlers, senders, configuration, and process lifecycle.
For the complete delivery, retry, and timeout contract, see
[Task processing](task_processing.md).

## Components and responsibility boundaries

Solid arrows show calls/data flow; dotted arrows show construction, ownership,
or implementation of a port. A source or target gateway delegates to the same
generic executor; the runtime contains no application-domain dependencies.

```mermaid
flowchart TB
    subgraph Host["Host application and domain"]
        Root["Composition root<br/>dependency wiring and process lifecycle"]
        Lifecycle["Process runtime<br/>signal, cancellation and adapter drain"]
        Http["HTTP server<br/>graceful shutdown"]
        Stop["CancellationToken<br/>shared process stop request"]
        Gateway["Application / command gateway<br/>command and explicit NewOutboxTask list"]
        Aggregate["Aggregate<br/>business invariants and workflow phases"]
        Factory["HandlerRegistryFactory<br/>fresh handlers and sender state"]
        Handler["Business TaskHandler<br/>one replay-safe operation"]
        Sender["Sender port / adapter<br/>external-effect contract"]
    end

    subgraph Runtime["cqrs-runtime: reusable orchestration"]
        Executor["CqrsCommandExecutor<br/>bounded optimistic-conflict retry"]
        Preparation["worker::prepare / PreparedOutboxWorker<br/>all C registries checked before HTTP"]
        Supervisor["prepared.run / worker::run<br/>shared polling, restarts, shutdown"]
        Channel["Bounded async-channel queue<br/>Q waiting deliveries"]
        Slot["Permanent Tokio worker<br/>one registry; one in-flight delivery"]
        JoinSet["JoinSet<br/>worker lifecycle, restart and abort"]
        Coordinator["OutboxTaskCoordinator<br/>formats, execution budget, settlement"]
        Registry["TaskHandlerRegistry<br/>route by TaskFormat"]
        QueuePort["OutboxQueue<br/>durable ownership port"]
    end

    subgraph Framework["cqrs-es dependency"]
        Cqrs["CqrsFramework / PersistedEventStore / EventSink<br/>load, replay, decide, collect, commit"]
    end

    subgraph Adapters["cqrs-runtime: PostgreSQL adapters"]
        CommandFactory["postgres::command_executor<br/>checked snapshot and retry configuration"]
        Repository["PostgresEventRepositoryWithOutbox<br/>atomic event, snapshot and tasks"]
        Queue["PostgresOutboxQueue<br/>claim, token fencing and expired-lease recovery"]
    end

    Database[("PostgreSQL<br/>events, snapshots, transactional_outbox")]
    Receiver["External receiver<br/>effect and idempotency enforcement"]

    Root -.->|"pool and PostgresCommandConfig"| CommandFactory
    CommandFactory -.->|"constructs CommandExecutor"| Executor
    Root -.->|"prepare before binding HTTP"| Preparation
    Preparation -.->|"owns checked handlers; run consumes"| Supervisor
    Root -.->|"creates shared token"| Stop
    Root -.->|"starts"| Lifecycle
    Root -.->|"constructs"| Http
    Lifecycle -.->|"signal or critical component exit: cancel"| Stop
    Supervisor -.->|"fatal error: cancel before drain"| Stop
    Stop -.->|"stop HTTP admission"| Http
    Stop -.->|"stop claims and recovery"| Supervisor
    Stop -.->|"stop dequeue; preserve active invocation"| Slot
    Root -.->|"supplies dependencies"| Factory
    Gateway -->|"CommandExecutor: execute(id, command, tasks)"| Executor
    Executor -->|"build once; execute each attempt"| Cqrs
    Cqrs -->|"replay / handle / apply"| Aggregate
    Cqrs -->|"read / persist"| Repository
    Repository -->|"read; atomic write"| Database

    Supervisor -->|"claim / recover_expired"| Coordinator
    Supervisor -->|"snapshot free capacity before claim; publish ClaimedTask"| Channel
    Channel -->|"dequeue; Notify available capacity"| Slot
    Supervisor -.->|"observe exit / restart"| JoinSet
    JoinSet -.->|"owns C worker tasks"| Slot
    Preparation -.->|"calls C times before admission"| Factory
    Supervisor -.->|"calls on restart"| Factory
    Factory -.->|"constructs per worker"| Registry
    Slot -.->|"owns mutable registry"| Registry
    Slot -->|"execute_and_settle"| Coordinator
    Coordinator -->|"handle TaskDelivery"| Registry
    Registry -->|"select handler"| Handler
    Handler -->|"target-aggregate command"| Gateway
    Handler -->|"or one external effect"| Sender
    Sender -->|"stable operation key when required"| Receiver
    Coordinator -->|"claim / settle / recover"| QueuePort
    Queue -.->|"implements"| QueuePort
    Queue -->|"separate queue transactions"| Database
```

`OutboxRuntimeConfig` checks coordinator/worker budgets at construction or env
load time. PostgreSQL statement timeout remains adapter configuration.
`prepare_outbox_worker` constructs the queue and delegates to `worker::prepare`,
without SQL or background tasks. Exactly C registries are retained, including the
first used for ordered format discovery. Errors/drop release prepared handlers;
no cancellation token exists in the prepared object. A polled run binds the host's
token with DropGuard. Low-level run retains custom coordinator support and uses
this same execution engine.

The two database writers have different jobs: the event repository publishes
work with an accepted event, while the queue manages delivery ownership.
Neither worker state nor task settlement advances an aggregate's business phase.

## Command execution and atomic publication

This is the accepted, single-event path. Task data is prepared by application
before execution; it is not generated by the worker or inferred from event
metadata. The framework factory binds the immutable list to a new repository
once per command invocation.

The host assembles the standard executor with `postgres::command_executor`;
construction performs no I/O. `PostgresCommandConfig` defaults to snapshots every
25 events and the existing conflict retry policy. The standard path uses unit
services and no queries. Custom stores/services/queries remain supported through
`CqrsCommandExecutor::new`; both paths implement the same `CommandExecutor<A>`
technical boundary. Service gateways own aggregate-ID selection, not framework
construction. The current upstream snapshot path requires repeat-tolerant `apply`
([known issue](../../doc/known_issues/cqrs_snapshot_event_reapplication.md)).

```mermaid
sequenceDiagram
    autonumber
    box Host application
        participant App as Application / gateway
    end
    box cqrs-runtime command
        participant Executor as CqrsCommandExecutor
    end
    box cqrs-es dependency
        participant Cqrs as CqrsFramework
        participant Store as PersistedEventStore
        participant Sink as EventSink
    end
    box Host domain
        participant Aggregate as Aggregate
    end
    box cqrs-runtime postgres
        participant Repo as Event repository
    end
    participant DB as PostgreSQL

    App->>Executor: execute(aggregate_id, command, tasks)
    Executor->>Executor: build_framework(tasks) once
    Note over Executor,Repo: One repository/task list, unchanged across conflict retries

    loop Bounded attempts, repeat only after optimistic conflict
        Executor->>Cqrs: execute(aggregate_id, original command clone)
        Cqrs->>Store: load_aggregate(aggregate_id)
        Store->>Repo: get_snapshot / get_events / get_last_events as needed
        Repo->>DB: Read source-of-truth data
        DB-->>Repo: Snapshot / events
        Repo-->>Store: Serialized data
        Store->>Aggregate: Rebuild state, apply replayed events
        Store-->>Cqrs: Aggregate context
        Cqrs->>Sink: Create fresh EventSink for this attempt
        Cqrs->>Aggregate: handle(command, services, sink)
        Aggregate->>Aggregate: Check current business invariants
        Aggregate->>Sink: write(event, aggregate)
        Sink->>Aggregate: apply(event) in memory
        Sink-->>Aggregate: Event buffered, not durable yet
        Aggregate-->>Cqrs: Ok(())
        Cqrs->>Sink: collect()
        Sink-->>Cqrs: Accepted event
        Cqrs->>Store: commit(event, context, metadata)
        Store->>Repo: persist(event, optional snapshot)
        Repo->>Repo: Validate WritePlan / SnapshotWrite
        Repo->>DB: One atomic modifying CTE statement
        Note over Repo,DB: Event + optional snapshot + explicit pending tasks
        DB-->>Repo: Write result
        Repo-->>Store: Persistence result
        Store-->>Cqrs: Committed event or error
        Note over Cqrs: Configured Query processors run after commit, outside this atomic write
        Cqrs-->>Executor: Execution result
        opt Optimistic conflict and remaining attempts
            Executor->>Executor: Bounded backoff before reloading state
        end
    end
    Executor-->>App: Ok / Domain / Conflict / Unavailable
```

A domain rejection stops before `commit`. The adapter rejects multi-event,
snapshot-only, or tasks-without-event writes before SQL; an empty write with
no snapshot/tasks is a no-op. A rolled-back write publishes nothing, but a lost
response can hide a successful commit. Only optimistic conflicts receive fast
retries. Tasks become visible at database commit, not at the client response
or completion of any configured projection.

The queue's storage deadline and transaction-local PostgreSQL statement timeout
do **not** automatically apply to event-store reads/writes on the shared pool.

## Claim, execute, settle and release capacity

The supervisor shares one coordinator; each permanent Tokio worker owns a registry.
The diagram shows a free-capacity snapshot followed by one claimed task; other workers and
tasks can run concurrently, including tasks from the same aggregate.

```mermaid
sequenceDiagram
    autonumber
    box cqrs-runtime worker
        participant Supervisor as Supervisor
        participant Channel as Bounded channel
        participant Slot as Permanent Tokio worker
    end
    box cqrs-runtime outbox
        participant Coordinator as Coordinator
        participant Registry as Worker registry
    end
    box cqrs-runtime postgres
        participant Queue as Queue adapter
    end
    participant DB as PostgreSQL
    box Host application
        participant Handler as Business handler
        participant Operation as Sender / target gateway
    end
    participant Receiver as External receiver

    Note over Supervisor,Registry: Before HTTP: combined config validates budget; prepare builds C registries
    Note over Supervisor,Registry: run uses those same registries; low-level run shares the engine
    Note over Supervisor,Slot: JoinSet observes exits; replace one worker with a fresh checked registry
    Supervisor->>Supervisor: Polling tick
    Supervisor->>Coordinator: recover_expired(min(worker count, 32))
    Coordinator->>Queue: recover_expired(limit), bounded storage await
    Queue->>DB: Transaction: lock expired rows, retry or fail, invalidate tokens, COMMIT
    DB-->>Queue: Confirmed transitions
    Queue-->>Coordinator: LeaseRecoveryReport
    Coordinator-->>Supervisor: Recovery report, log confirmed failures

    Supervisor->>Channel: Snapshot free capacity Q - sender.len()
    Supervisor->>Coordinator: claim(owner, Q - sender.len(), lease duration)
    loop Supported formats, one shared claim deadline
        Coordinator->>Queue: claim(format, owner, remaining capacity, lease)
        Queue->>Queue: Anchor local lease deadline BEFORE pool acquisition
        Queue->>DB: Transaction: SKIP LOCKED claim, attempts += 1, fresh token, COMMIT
        DB-->>Queue: Claimed rows / commit acknowledgement
        Queue-->>Coordinator: ClaimedTask batch with original local deadlines
    end
    Coordinator-->>Supervisor: Confirmed tasks, including any earlier partial batch
    Supervisor->>Channel: try_send each ClaimedTask, sole producer
    Slot->>Channel: recv through worker's cloned receiver
    Slot-->>Supervisor: Notify queue capacity available
    Note over Slot,Coordinator: ClaimedTask contains TaskDelivery + ClaimLease
    Slot->>Coordinator: execute_and_settle(registry, task, handler_timeout)
    Coordinator->>Coordinator: Calculate ExecutionPlan from remaining lease

    alt Run or RetryOnly
        alt Run: full handler and settlement budgets fit
            Coordinator->>Registry: handle(task.delivery())
            Registry->>Handler: Route by type/version, handle(TaskDelivery)
            Note over Registry,Handler: No lock token or lease deadline crosses this boundary
            Handler->>Operation: One replay-safe operation, with bounded local retries
            alt Target-aggregate command
                Operation->>Operation: Follow the command publication path above
            else External effect
                Operation->>Receiver: Send with the stable operation key when required
                Receiver-->>Operation: Receiver result
            end
            Operation-->>Handler: Operation result
            Handler-->>Registry: Ok / Retry / Permanent
            Registry-->>Coordinator: Handler result
            Coordinator->>Coordinator: Classify SettlementRequest
            Note over Coordinator,Receiver: Handler timeout requests retry, accepted remote work may still finish
        else RetryOnly: invocation budget no longer fits
            Coordinator->>Coordinator: Request retry without calling handler
        end
        Coordinator->>Queue: settle(ClaimedTask, SettlementRequest), bounded by storage AND remaining lease
        Queue->>DB: Transaction: conditional token/expiry UPDATE, COMMIT
        DB-->>Queue: Changed status or no owned row
        Queue-->>Coordinator: SettlementResult after commit
        Coordinator-->>Slot: Completed / RetryScheduled / Failed / OwnershipLost
    else RecoverOnly: no safe settlement window
        Coordinator-->>Slot: LeaseBudgetExhausted, no handler or settlement
    end

    Slot->>Slot: Continue with same mutable registry; dequeue next task
```

Dequeue frees waiting capacity; finishing execution allows that permanent worker
to dequeue again. Settlement errors do not mark a task completed. A storage timeout
returns `OutboxError::TimedOut` with an unknown database outcome. Panic terminates
a worker; JoinSet reports the exit, the supervisor creates one replacement, and
expired ownership remains for durable recovery. Storage awaits remain bounded.

Queue transactions apply their own statement timeout; the coordinator bounds
the entire await, including pool acquisition and commit. The local lease estimate
never replaces PostgreSQL token/expiry fencing. A retry request may become
`Failed` at the attempt limit. Unknown formats remain pending.

Shutdown cancels the shared CancellationToken before closing the channel and
stops claims and dequeue, then drains started invocations plus settlement for H+S.
Startup/restart errors cancel that token before returning/draining: HTTP begins
graceful shutdown while healthy workers still finish. A worker panic is restarted
without cancelling the process. Waiting claims remain for recovery; remaining
workers are aborted and joined after the deadline. The supervisor's DropGuard also
requests host cancellation on panic/abort, when asynchronous drain is impossible.

The host awaits HTTP and outbox before closing PostgreSQL, preserving the primary
failure and logging secondary errors with safe task diagnostics. HTTP can publish
new durable tasks while its in-flight requests finish; these tasks wait for a
subsequent healthy pool. HTTP/DB cleanup has no overall deadline yet
([known issue #10](../../doc/known_issues/http_shutdown.md)); the synchronous
stdout adapter can exceed cooperative async budgets (backlog task #11).
Interrupted work remains durable for expiry recovery. There is no lease renewal.

A target command may atomically publish a successor task through the command
path above. That successor can run before the original task's settlement.
External success likewise precedes acknowledgement: redelivery can repeat the
effect, so its idempotency belongs to the handler/receiver, not to queue fencing.

## Source map

- [command](../src/command.rs): executor and optimistic-conflict policy.
- [outbox](../src/outbox.rs): delivery/lease types, registry, queue contracts,
  execution budgets, settlement requests/results.
- [worker](../src/worker.rs): combined configuration, synchronous preparation, bounded admission, JoinSet supervision and permanent worker lifecycle.
- [postgres](../src/postgres.rs): event repository, queue and canonical SQL assets.
