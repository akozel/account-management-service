# Simulated HTTP 5 ms: four Tokio runtime threads

Run: 2026-10-10 15:51 UTC.

Release build, Rust 1.98.1; Linux x86_64; Intel Core i7-12650H;
cgroup quota 8 CPU equivalents. Four Tokio scheduler worker threads plus the
block_on caller running the producer. No CPU affinity/exclusive host reservation.
Benchmark ran independently of cargo builds and other tests.

```text
BENCH_META,os=linux,arch=x86_64,available_parallelism=8,sample_every=32,repetitions=8,io_deliveries_per_worker=100,backends=tokio_mutex|async_channel|flume|kanal,async_channel=2.5.0,flume=0.12.0-default,kanal=0.1.1-default
```

Command:

```sh
cargo test --release -p cqrs-runtime --lib --all-features \
  worker::worker_task::tests::performance::compare_http_5ms_four_threads \
  -- --ignored --nocapture --test-threads=1
```

C=Q=128/1000. Eight balanced repetitions per backend: 64 samples. Each sample
measures 100*C deliveries (12,800 / 100,000) after 2*C warmup deliveries. There
is also an unreported whole-sample warmup per backend. Payload is JSON with a
256-byte string. Latencies sample every 32nd message: 400 / 3,125 observations
per sample. All batches passed count/checksum/XOR checks; order positions and
pairwise precedence are balanced. [Raw CSV](queue_performance_results_io5ms.csv).

Method and version/features: [Local worker queue performance](queue_performance.md).
The previous 100 ms experiment used shorter batches; its samples are preserved
separately and not pooled into these results.

## Median completed deliveries per second

| Workers / capacity | Tokio + Mutex | async-channel | Flume | Kanal |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 20,617 | 20,630 | 20,662 | 20,613 |
| 1000 | 155,169 | 158,196 | 158,366 | 158,302 |

## Candidate throughput relative to Tokio

| Workers / capacity | async-channel / Tokio | Flume / Tokio | Kanal / Tokio |
| ---: | ---: | ---: | ---: |
| 128 | 1.001x | 1.002x | 1.000x |
| 1000 | 1.020x | 1.021x | 1.020x |

## Latency and elapsed-time dispersion

Median of per-run latency percentiles, milliseconds; elapsed IQR is the 25th–75th
percentile range in milliseconds. Queue time excludes producer backpressure
before enqueue; delivery time includes queue wait and simulated I/O.

| Workers | Backend | Queue p50 ms | Queue p95 ms | Queue p99 ms | Delivery p95 ms | Elapsed IQR ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 128 | tokio_mutex | 6.328 | 6.603 | 6.716 | 13.011 | 620.328–621.812 |
| 128 | async_channel | 6.332 | 6.599 | 6.761 | 13.032 | 618.827–622.000 |
| 128 | flume | 6.320 | 6.566 | 6.706 | 12.989 | 618.723–620.602 |
| 128 | kanal | 6.339 | 6.600 | 6.704 | 13.037 | 619.357–622.914 |
| 1000 | tokio_mutex | 6.425 | 7.080 | 7.390 | 13.569 | 643.032–644.899 |
| 1000 | async_channel | 6.366 | 6.642 | 6.781 | 13.153 | 631.091–632.882 |
| 1000 | flume | 6.345 | 6.608 | 6.814 | 13.132 | 630.617–631.959 |
| 1000 | kanal | 6.325 | 6.618 | 6.814 | 13.143 | 630.245–632.092 |

## Limits

The nominal handler wait is tokio::time::sleep(5 ms); actual elapsed delay includes
Tokio timer granularity and scheduler wakeup latency. There are no sockets, TLS,
PostgreSQL or remote rate limits. Identical timing instrumentation contributes
overhead. The comparison measures the local queue pipeline including admission
and wakeups; medians and IQRs describe this host/run, without a statistical
significance claim or a universal library ranking.

## Observed comparison

At C=128, candidate median throughput differs from Tokio by less than 0.3%.
At C=1000, the three MPMC medians are about 1.95–2.06% above Tokio and close to
one another. Tokio delivery p95 is about 13.57 ms versus about 13.13–13.15 ms for
MPMC; compare the dispersion above before treating small ranking differences
as stable. Nominal 5 ms waiting remains a major part of the measured pipeline.

## Verification

The focused release test passed separately: 64 samples validated, balanced order
and delivery-count/checksum/XOR checks passed. fmt, workspace Clippy with
`-D warnings`, workspace tests (141 passed; two manual benchmarks ignored) and
mandatory llvm-cov passed. Workspace line coverage: 97.02%.
