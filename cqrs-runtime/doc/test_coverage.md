# Test coverage audit

Audit date: 2026-10-12. Tests use local dummy aggregate/task types and the real
PostgreSQL 18.6 image through testcontainers. Every database fixture owns an
isolated container and applies the canonical event-store and outbox schemas;
database tests are neither ignored nor replaced with in-memory storage.

## Results before the directory refactor

| Measurement | Before | After |
| --- | ---: | ---: |
| Workspace line coverage, excluding the service composition root | 97.90% (7239/7394) | 98.47% (7785/7906) |
| Line coverage summed across `cqrs-runtime/src/` files | 97.54% (4714/4833) | 98.41% (5260/5345) |
| Runtime unit tests with all features | 105 | 122 |
| Runtime PostgreSQL integration tests | 26 | 39 |

The standard LLVM report includes inline unit-test bodies. Moving or adding
tests therefore changes its denominator. These percentages are line coverage,
not branch coverage, and do not prove correctness for every concurrent schedule.
The added checks target durable invariants and observable failure behavior.

The follow-up pass raised runtime production-line coverage from 97.31%
(1304/1340) to 99.70% (1336/1340). This separate LCOV analysis counts source lines
before the inline `cfg(test)` modules and omits test-only support files. Its
denominator is unchanged, so the gain represents 32 newly exercised production
lines rather than added test bodies. The required workspace coverage command
still includes all production files and keeps its existing 90% gate.

One new regression exposed a production defect despite the high baseline:
settlement could return `Completed` after its lease expired while waiting for a
lock on an unchanged row. PostgreSQL could evaluate the original `UPDATE`
predicate before the lock wait. Settlement now obtains ownership through a
materialized `FOR UPDATE` CTE and checks expiry on its output. The regression
failed with the original SQL and passed after this change.

## Directory refactor verification

The directory refactor passed workspace formatting, clippy with warnings denied,
the full workspace test suite and the required 90% LLVM line-coverage gate.
All-target compilation also passed with default features disabled, then with
each of `postgres`, `worker`, `host` and `logging` enabled separately.

| Measurement | After directory refactor |
| --- | ---: |
| Workspace line coverage, excluding the service composition root | 98.37% (6986/7102) |
| Line coverage summed across `cqrs-runtime/src/` files | 98.24% (4461/4541) |
| Runtime unit tests with all features | 102 |
| Public runtime integration tests | 21 |
| Runtime PostgreSQL integration tests | 39 |
| Workspace tests across 16 suites, including doctests | 240 |

The direct-import cleanup also passed strict rustdoc generation with warnings
denied, the full workspace suite, clippy, the optional-feature compilation
matrix and the required coverage gate. There are no `pub use` declarations
in runtime source; service and test callers use the defining modules directly.

Twenty-one existing multi-component scenarios moved from inline supervisor
tests to the public runtime integration suite. The attempt/lease check was split
between its two component owners. No original supervisor scenario was removed.
Moving test bodies changes the default LLVM source-line denominator; the earlier
audit percentages above retain their historical denominator.

Queue SQL statements and atomic write SQL fragments were compared with their
pre-refactor versions and are unchanged. A subsequent import cleanup removed
flattening re-exports and migrated callers to the modules defining each API.

After the direct-import cleanup, the largest reorganized implementation file
has 157 lines before its inline unit tests. Supervisor has 208 total lines, down
from 1311. Its diagnostic
check remains inline; private pool drain behavior is tested beside WorkerPool.

## Invariants and branches

| Invariant / branch | Test ownership | Coverage |
| --- | --- | --- |
| Retry only optimistic conflicts; preserve the command and tasks; rebuild state on each attempt | Inline tests in [command/executor.rs](../src/command/executor.rs); [command factory integration](../tests/postgres/command_factory.rs) | Existing executor checks retained; a new real-database race forces both commands to read the same initial state, then checks one accepted command, one reloaded domain rejection and only the winner's task. |
| Backoff respects the attempt cap, exponential growth and delay ceiling; success, domain and technical errors stop retries | Inline tests in [command/retry.rs](../src/command/retry.rs) | Existing policy checks moved beside their implementation; new paused-time checks assert exact attempt times and stop after a conflict followed by any terminal outcome. |
| At most one event per command; snapshots/tasks require an event; event and snapshot sequences and SQL bind counts must fit | Inline tests in [write_plan.rs](../src/postgres/event_repository_with_outbox/write_plan.rs); [persistence integration](../tests/postgres/persistence.rs) | New checks cover retained validation reasons, both sequence overflows and bind-limit boundaries including snapshot parameters. |
| An empty write without tasks or snapshot is a no-op | Inline write-plan tests; persistence integration | New API check closes the actual database pool before the empty write, proving that it does not need a live connection. |
| Event, optional snapshot and tasks have one atomic winner | Persistence integration | New contended writes exercise events-only, initial snapshot and snapshot-update paths; event payload, snapshot version/state and outbox identity must all match the winner. |
| Snapshot/task/event failures roll back the entire publication | Persistence integration | Existing outbox rejection and stale snapshot checks retained; a new duplicate event test verifies rollback of an otherwise valid snapshot update and its tasks. |
| Ready claims skip held rows, respect formats/schedules and never overlap | [Queue integration](../tests/postgres/queue.rs); [worker integration](../tests/postgres/worker.rs) | Existing competing claims, held row locks, unsupported formats, two live pools and durable schedule checks retained. |
| One ownership token can settle at most once, regardless of the competing requested outcomes | Queue integration | New simultaneous Completed/Retry/Failed requests are blocked on the same row before release; exactly one confirmed transition must match the saved state. |
| Expired or stale ownership cannot settle; expiry remains authoritative after waiting for a lock | Queue integration | New regressions cover a concurrently changed expiry and the passage of real database time while an unchanged row is locked. Existing reclaims check token replacement and rejection of old tokens. |
| Concurrent recovery processes each expired row once, respects batch limits, skips locked rows and reports only committed failures | Queue integration | Six concurrent recovery calls process a mixed batch of 24 tasks, preserving one held row for later recovery; assertions check failure IDs, attempts, lifecycle state, payload and metadata. |
| A failed recovery batch exposes no partial state changes | Queue integration | A new database constraint rejects the second ordered recovery update; every row, lease and schedule must equal its pre-batch value, then normal recovery must succeed after removing the fault. |
| Invalid limits and retry delays cannot consume ownership or change data | Queue integration | New overflow and zero-limit checks verify unchanged durable state and a subsequent valid completion using the original token. |
| A rejected COMMIT must not report a durable claim, settlement or failed recovery transition | [Failure integration](../tests/postgres/failures.rs) | A deferred constraint trigger rejects COMMIT for each operation after its UPDATE succeeds; the entire saved row must remain unchanged, session settings must be restored and later recovery must work. |
| Failure to install transaction-local settings must leave all tasks untouched and the pool usable | Failure integration | A restricted PostgreSQL role lacks permission to call `set_config`; claim, recovery and settlement fail without changing state or ownership, then the same pool completes the task after permission is granted. |
| Actual environment loaders preserve defaults, units, validation and the combined lease budget | Inline tests in [storage_config.rs](../src/task/queue/storage_config.rs), [queue config](../src/postgres/task_queue/config.rs), [runtime config](../src/runtime/config.rs) and [worker config](../src/runtime/worker_pool_config.rs) | Fresh child processes exercise each `from_env`, including invalid/zero/overflow values and non-Unicode storage settings, without mutating the parallel test runner's environment. |
| Direct SQL writers must obey structural and lifecycle constraints | [Schema integration](../tests/postgres/schema.rs) | New checks reject 21 invalid records with SQLSTATE 23514 and no inserted rows; valid pending, processing, completed and failed records are accepted. |
| Unknown responses after real commits must preserve durable outcomes; timeouts must not imply rollback | [Timeout integration](../tests/postgres/timeouts.rs); [processing integration](../tests/postgres/processing.rs) | Existing reply-loss wrappers, pool acquisition, SQL timeouts, failed acknowledgements, redelivery and shutdown checks retained. |
| Bounded admission, mutable handler ownership, restart and shutdown preserve tasks and settlement budgets | [Runtime integration](../tests/runtime.rs), inline tests in [worker_pool.rs](../src/runtime/worker_pool.rs), [worker.rs](../src/runtime/worker.rs), [poller.rs](../src/runtime/poller.rs), [default_executor.rs](../src/task/executor/default_executor.rs); PostgreSQL worker integration | Existing cancellation, partial claims, queued waves, worker replacement, drain deadlines and 1,000-delivery tests retained. |
| Typed dispatch validates format and payload before calling business code | Inline tests in [json_adapter.rs](../src/task/handling/json_adapter.rs), [dispatcher.rs](../src/task/handling/dispatcher.rs), [new_task.rs](../src/task/data/new_task.rs) | Existing routing, serde rejection, mutable state, delivery identity and safe diagnostics checks retained. |
| Plain `log` fallback keeps diagnostics usable and private data redacted without a tracing subscriber | [Fallback logging integration](../tests/log_fallback.rs) | An isolated executable installs a real `log::Log` recorder and checks command/storage failures, panic/cancellation context, worker startup and confirmed task failure; raw source errors, payload, metadata and handler reasons must remain absent. |

For contended publication and settlement, tests hold explicit PostgreSQL locks
and observe the competing statements in `pg_stat_activity` before releasing
them. A barrier alone cannot establish that SQL reached the intended race.
Concurrent recovery uses a barrier plus a held row that must be skipped. The
expiry regression waits until PostgreSQL reports actual expiry; elapsed local
time is not used as a substitute for the storage ownership check. Coordination
waits are bounded.

## Layout and scope

The directory refactor retains private unit tests in inline `#[cfg(test)] mod tests` blocks beside the code they
exercise. Executor and retry tests have been moved from `command.rs` to their
implementation modules. PostgreSQL configuration tests now live in
`postgres/task_queue/config.rs` and use a synchronous test. The redundant dummy
payload serialization test was removed; structural task construction remains
covered in `task/data/new_task.rs`.

Public runtime behavior now lives in `tests/runtime/`, rooted by
`tests/runtime.rs`. Its preparation, admission, supervision and shutdown modules
use public contracts directly and own their fixtures in `runtime/support.rs`.
The private pool drain check moved beside WorkerPool; safe diagnostic capture stays
inline beside supervisor.

Database integration modules stay under `cqrs-runtime/tests/postgres/`, rooted
by `cqrs-runtime/tests/postgres.rs`. Shared runtime unit fakes remain in the
private `task/test_support.rs`; database fixtures remain in
`tests/postgres/support.rs`. Process-global logger installation is deliberately
isolated in `tests/logging.rs` and `tests/log_fallback.rs`. The direct `log`
dependency is test-only and reuses the version already in the lockfile.

The follow-up also checks cancellation exactly after dequeue using a notification
waker, draining of a closed channel, cancellation inside the restart factory,
effective startup log fields, task format display, invalid declared payload
formats, TLS error classification, SQL bind-limit propagation, clock subtraction
underflow and both queued-budget multiplication and final-sum overflow.

## Remaining uncovered production lines in the pre-refactor audit

| Source | Reason |
| --- | --- |
| `command/retry.rs:94` | Defensive `unreachable!` after a loop whose checked policy always permits at least one attempt. Reaching it would require bypassing the constructor and fabricating invalid private state. |
| `host/signal.rs:11` | Completion of the Unix system signal wrapper. Its registration and pending observation are exercised in a child process; success/error selection is tested through controlled futures without sending real process signals. |
| `postgres/task_queue/settlement.rs` (post-commit status decoding) | Failure to decode `RETURNING o.status` as a String after commit. The canonical schema supplies non-null text; this guard would require incompatible schema/protocol data, not a valid queue operation. SQL/setting/commit failures are tested separately against PostgreSQL. |
| `runtime/worker_pool.rs` (`next_exit`, successful exit classification) | Defensive classification of a successfully exited worker during active supervision. Workers normally exit on cancellation or channel closure, which the supervisor observes through its shutdown path; unexpected panics and restart cancellation are exercised. |

These four lines remain visible and counted in the report. No private-state
corruption, production hooks or additional source exclusions were introduced
solely to reach them. The report also contains uncovered lines in test-double
trait methods and defensive assertions; those are not production paths. Error
branches specific to 32-bit `usize` conversion cannot be exercised on this
64-bit host.

Remaining limitations worth separate work:

- Replay streams inherit the upstream producer's possibility of returning EOF
  after a background SQL error. Healthy replay tests cannot prove failure-safe
  completion; addressing this requires a streaming contract fix.
- Snapshot event reapplication remains the documented
  [upstream snapshot limitation](../../doc/known_issues/cqrs_snapshot_event_reapplication.md).
  The test aggregates use assignment-based event application, consistent with
  the supported contract.
- Lost-reply tests commit to the real database, then withhold the response in a
  wrapper. They do not simulate every network failure or connection loss during
  COMMIT. Such failures may have an unknown outcome and require receiver-specific
  idempotency, outside the generic runtime.
- OS signal registration is polled in an isolated process and dropped while
  pending. Signal receipt is tested through controlled futures rather than
  sending process signals.
- Tokio concurrency tests cover the listed schedules and invariants; they are
  not an exhaustive scheduler/model-checking proof.

## Reproduction

Run from the workspace root with Rust 1.98.1 and a reachable Docker-compatible
daemon for testcontainers:

```sh
rtk cargo fmt --all --check
rtk cargo clippy --workspace --all-targets --all-features -- -D warnings
rtk cargo test --workspace --all-features
rtk cargo test -p cqrs-runtime --no-default-features --lib
rtk cargo test -p cqrs-runtime --no-default-features --features worker --lib
rtk cargo test -p cqrs-runtime --no-default-features --features postgres --lib
rtk cargo llvm-cov --workspace --all-features --color never --ignore-filename-regex '(^|/)main\.rs$' --fail-under-lines 90
```

For machine-readable evidence, append `--json --output-path /tmp/cqrs-coverage.json`
to the coverage command. The workspace's required threshold remains 90%; the
composition root is the only configured source-file exclusion.

All listed checks passed in the pre-refactor audit. The ordinary workspace run reported
239 passed tests across 15 suites, including doctests; the runtime library run
without default features passed 35 tests, with worker alone 80 and with postgres
alone 52. The coverage run passed its 90% gate.
