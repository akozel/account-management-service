# Task processing contracts

For task handlers, sender adapters, claims, or workflow sequencing, read the
[accepted contract](../../../../doc/task_processing_contract.md). It defines
aggregate-owned business sequencing with parallel task execution and stable
side-effect identity in payload or metadata.

Apply these boundaries when reviewing or changing the implementation:

- Keep transitions and prerequisite checks in the aggregate. Application
  attaches the task to the accepted event; persistence commits both atomically.
- Treat `aggregate_sequence` as source-event provenance. Multiple tasks can
  share it, and later events can produce eligible tasks while earlier tasks
  are still pending or processing. Do not impose a queue ordering barrier.
- Keep each task to one target command or external effect. A dependent task
  is published only when its prerequisite is established by an accepted event.
  A committed successor does not need to wait for its parent's acknowledgement.
- Ensure target commands tolerate exact redelivery and concurrent delivery.
  The aggregate distinguishes an already-applied operation from conflicting
  data; the handler classifies those outcomes for its task.
- Separate bounded retries inside one handler invocation from durable outbox
  redelivery of the whole task. Recovery or handler timeout can restart the
  handler without a returned retryable error. Document the combined attempt
  budget, including sender SDK retries, and keep operation identity stable
  across both levels. A local retry policy cannot suppress recovery of an
  unsettled task.
- Store any required external idempotency key in the versioned payload or task
  metadata. Keep it and the effect-defining data stable across retries/requeue,
  and pass it to the receiving adapter. `task.id` can identify one task's
  repeats; neither attempt nor lock token is an idempotency key.
- Enforce claim, recovery, and settlement deadlines at the runtime boundary,
  covering the whole adapter call. Treat timeout as an unknown storage outcome
  and release the waiting runtime capacity. Use `OutboxTaskCoordinatorConfig`
  for the shared claim/recovery/settlement budget and preserve earlier claim
  results when a later format times out. `PostgresOutboxQueueConfig` applies
  its statement timeout transaction-locally; shared-pool session settings
  must remain intact. Both settings are positive and documented in `.env.example`.
- Validate the whole lease budget using the coordinator's actual storage
  timeout: `lease > claim + handler + settlement + 5s margin`. Local retries
  and backoff share the handler budget; distinguish retry count from total
  attempts. Defaults are `60s > 5s + 30s + 5s + 5s = 45s`.
- Preserve the task's conservative monotonic lease deadline from before claim.
  Do not reset it on dispatch or compare database UTC with local wall time.
  Skip handler execution when its full budget and settlement reserve no longer
  fit. Request retry if safe settlement remains, otherwise use durable recovery.
  Acknowledgement is bounded by storage time and lease minus margin. Database
  token/expiry checks and idempotency remain necessary; no renewal is enabled.

The separate [worker decisions and open questions](../../../../doc/known_issues/2026-10-10-task_worker_open_questions.md)
close questions 2–4 through handler replay safety and separate retry policies,
storage deadlines, and per-task lease budgets. Their closure does not imply
exactly-once external effects or introduce lease renewal or deduplication APIs.
