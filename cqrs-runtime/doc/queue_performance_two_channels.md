# Local worker queue performance (archived two-channel method)

This document preserves the earlier two-channel setup. The same test command
now runs the expanded four-channel experiment; use the
[current method](queue_performance.md) to rerun it. Original observations remain
in the archived CSV/report linked below.

This comparison measured the current `run_worker` receive loop against a
test-only bounded `async-channel` MPMC prototype. Production still uses Tokio
mpsc behind a shared receiver mutex. `async-channel` is a dev-dependency only.

## Run

```sh
cargo test --release -p cqrs-runtime --lib --all-features \
  worker::worker_task::tests::performance::compare_current_and_mpmc \
  -- --ignored --nocapture --test-threads=1
```

The performance test rejects debug builds and is ignored during ordinary tests.
Do not run compilation, other tests or competing benchmarks alongside the
measurement. Keep CPU quota, runtime threads and power settings stable when
comparing hosts or revisions. There is no CI speed threshold.

Rows prefixed `BENCH,` form CSV after stripping that prefix. `BENCH_META` records
OS, architecture, available parallelism, repetitions and sampling interval.
The regular `comparison_smoke_checks_both_backends_and_all_workloads` test checks
both backends with small batches and all workloads on both runtime types.

## Workloads and timing

| Dimension | Values |
| --- | --- |
| Runtime | Current-thread; multi-thread with 4 runtime threads |
| Permanent workers C | 128; 1000 |
| Bounded waiting capacity Q | C for each case |
| Fast handler | Reads payload through black_box; 50,000 measured deliveries |
| Cooperative handler | One yield_now per delivery; 20,000 measured deliveries |
| Simulated HTTP I/O | One 100 ms Tokio sleep per delivery; 4*C measured deliveries |
| Payload | JSON object containing a 256-byte string |
| Samples | 7 paired repetitions; backend order alternates |

The multi-thread runtime has four scheduler worker threads; its block_on caller
runs the producer on an additional thread. Current-thread runs both on one thread.
No CPU affinity is applied.

Both sides own one mutable registry per permanent Tokio task and use the same
handler, immediate coordinator, shutdown checks, Notify-on-dequeue and payloads.
The baseline calls the actual production `run_worker`; it does not copy the
mutex receive loop. The candidate has a separate test-only receive loop that
uses cloned async-channel receivers. Runtime code is otherwise unchanged.

Each sample constructs DTOs, registries and the pool before measurement, then
completes 2*C warmup deliveries in that same pool. Every case also has one
unreported whole-sample warmup per backend. Time runs from publication of the
measured batch until the last measured handler completion. Sorting/reporting,
registry destruction and joining workers occur outside that elapsed interval.
The synthetic coordinator has immediate successful settlement.

The current producer uses Tokio channel capacity reservations. The candidate
uses a free-capacity snapshot and try_send: it has exactly one producer, so
receivers can only increase available capacity before the next send batch.
Both variants stop producing when their queue is full and wait for the same
coalesced Notify. This measures each proposed queue pipeline, including admission
and wakeups; it does not attribute all differences solely to the receiver mutex.

## Metrics and correctness

- `msg_per_second`: completed measured deliveries divided by elapsed time.
- `queue_p50_us`, `queue_p95_us`, `queue_p99_us`: enqueue-to-handler-start delay.
- `delivery_p95_us`: enqueue-to-handler-completion delay, including simulated I/O.
- Latencies sample every 32nd measured delivery using per-message atomic enqueue
  timestamps and worker-local vectors. Warmup uses a completion counter; measured
  delivery statistics do not take a shared mutex. Each handler records its last
  completion timestamp for the elapsed endpoint. Instrumentation is identical
  for both backends and contributes some overhead.
- Message count, ID sum and ID XOR must match after every run. These checks catch
  common message-loss/duplication errors; benchmark output is not a substitute
  for the runtime's delivery and ownership regression tests.

Summarize paired runs with median throughput, median latency and dispersion.
Report the environment and raw samples with conclusions. A faster synthetic
queue is not evidence of a faster transactional outbox: this test omits PostgreSQL,
claim SQL, lease checks, real HTTP/TLS, failures and receiver rate limits. Tokio
timers also have scheduling and granularity costs; the I/O scenario represents
waiting, not an HTTP network benchmark.

Local measurements are recorded separately in
[queue performance results](queue_performance_results_two_channels.md).
