# Local worker queue performance

This comparison measures the current `run_worker` receive loop against three
test-only bounded MPMC prototypes: async-channel 2.5.0, flume 0.12.0 and kanal 0.1.1.
Production still uses Tokio mpsc behind a shared receiver mutex. All candidate
libraries are dev-dependencies; Flume and Kanal are pinned to those versions.

Flume uses its default async/eventual-fairness/select features, with spin disabled.
Kanal uses its default async feature, with std-mutex disabled. Cargo.lock pins the
resolved async-channel version. The tested versions/features are recorded in
BENCH_META; optional library tuning is outside this comparison.

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
all four backends with small batches and all workloads on both runtime types.
Focused tests cover shutdown of idle workers with the producer still alive and
verify the balanced backend-order schedule.

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
| Samples | 8 repetitions per backend; balanced position and pairwise precedence |

The multi-thread runtime has four scheduler worker threads; its block_on caller
runs the producer on an additional thread. Current-thread runs both on one thread.
No CPU affinity is applied.

All four variants own one mutable registry per permanent Tokio task and use the same
handler, immediate coordinator, shutdown checks, Notify-on-dequeue and payloads.
The baseline calls the actual production `run_worker`; it does not copy the
mutex receive loop. The three candidates share one statically dispatched,
test-only receive loop with cloned receivers: async-channel/kanal recv().await,
flume recv_async().await. The receive adapters return native futures; no boxing
or dynamic receiver dispatch is added. Runtime code is otherwise unchanged.

Each sample constructs DTOs, registries and the pool before measurement, then
completes 2*C warmup deliveries in that same pool. Every case also has one
unreported whole-sample warmup per backend. Time runs from publication of the
measured batch until the last measured handler completion. Sorting/reporting,
registry destruction and joining workers occur outside that elapsed interval.
The synthetic coordinator has immediate successful settlement.

The current producer uses Tokio channel capacity reservations. The candidates
use a free-capacity snapshot and try_send: it has exactly one producer, so
receivers can only increase available capacity before the next send batch.
All variants stop producing when their queue is full and wait for the same
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
  for all backends and contributes some overhead.
- Message count, ID sum and ID XOR must match after every run. These checks catch
  common message-loss/duplication errors; benchmark output is not a substitute
  for the runtime's delivery and ownership regression tests.

The eight orders are four rotations of [Tokio, async-channel, Flume, Kanal],
followed by four rotations of its reverse. Every backend occupies every position
twice, and every pair appears in each precedence order four times. The CSV records
order_position (0..3). There are 12 profiles x 4 backends x 8 repeats = 384 samples.
All four backends are remeasured together; old samples are not pooled into this run.

Kanal try_send returns Result<bool, _>; Ok(false) is a failed send that drops the
message. The producer asserts success rather than treating unwrap alone as success.
End-of-input drops the last sender, allows buffered deliveries to drain and joins
all workers; count, checksum and XOR are checked before accepting the sample.

Summarize paired runs with median throughput, median latency and dispersion.
Report the environment and raw samples with conclusions. A faster synthetic
queue is not evidence of a faster transactional outbox: this test omits PostgreSQL,
claim SQL, lease checks, real HTTP/TLS, failures and receiver rate limits. Tokio
timers also have scheduling and granularity costs; the I/O scenario represents
waiting, not an HTTP network benchmark.

Local measurements are recorded separately in
[queue performance results](queue_performance_results.md).

The previous two-channel experiment and its seven-repetition method are preserved:
[archived report](queue_performance_results_two_channels.md),
[archived CSV](queue_performance_results_two_channels.csv), and
[archived method](queue_performance_two_channels.md).

## Focused 5 ms HTTP-wait profile on four runtime threads

```sh
cargo test --release -p cqrs-runtime --lib --all-features \
  worker::worker_task::tests::performance::compare_http_5ms_four_threads \
  -- --ignored --nocapture --test-threads=1
```

This profile uses only the four-thread Tokio runtime (plus its block_on producer
caller), C=Q=128/1000 and a nominal 5 ms Tokio sleep per delivery. It remeasures
all four backends with eight balanced repetitions: 64 samples in total.

Each sample measures 100*C deliveries (12,800 / 100,000), rather than the four
waves in the original 100 ms profile. The longer batches stabilize throughput
and provide 400 / 3,125 sampled latencies per run at SAMPLE_EVERY=32. The same
2*C warmup, payload, receiver loops, instrumentation and separate per-backend
whole-sample warmup apply. BENCH_META records io_deliveries_per_worker.
Actual sleep duration includes Tokio timer granularity and scheduler delay;
this is simulated waiting rather than network HTTP.

Results are kept separately in [5 ms report](queue_performance_results_io5ms.md)
and [raw CSV](queue_performance_results_io5ms.csv). Earlier experiments are not
overwritten or mixed into this measurement.
