# Four-channel local queue performance results

Run: 2026-10-10 15:38 UTC.

Environment: Rust 1.98.1 (48a229cea), release profile, Linux 7.2.9-arch1-1 x86_64,
Intel Core i7-12650H; cgroup CPU quota `800000/100000` (8 CPU equivalents).
No CPU affinity or exclusive host reservation. Benchmark was run independently
of other cargo builds/tests. Recorded metadata:

```text
BENCH_META,os=linux,arch=x86_64,available_parallelism=8,sample_every=32,repetitions=8,backends=tokio_mutex|async_channel|flume|kanal,async_channel=2.5.0,flume=0.12.0-default,kanal=0.1.1-default
```

Flume 0.12.0: default async/eventual-fairness/select features; spin disabled.
Kanal 0.1.1: default async feature; std-mutex disabled. These feature sets were
verified with cargo tree. async-channel 2.5.0 is resolved by Cargo.lock.

Command:

```sh
cargo test --release -p cqrs-runtime --lib --all-features \
  worker::worker_task::tests::performance::compare_current_and_mpmc \
  -- --ignored --nocapture --test-threads=1
```

All four channels were remeasured in the same experiment: 12 profiles, eight
repetitions per backend, 384 samples. Q=C. Every backend occupied every order
position twice; every pair appeared in each precedence order four times. All
samples passed delivery-count, checksum and XOR checks. [Raw CSV](queue_performance_results.csv).

The four-thread runtime also has its block_on caller running the producer;
current-thread runs producer/workers on one thread. Method and instrumentation:
[Local worker queue performance](queue_performance.md). The previous two-channel
[report](queue_performance_results_two_channels.md), [CSV](queue_performance_results_two_channels.csv)
and [method](queue_performance_two_channels.md) are archived; their numbers are
not mixed with this run.

## Median completed deliveries per second

| Runtime threads | C / Q | Handler | Tokio + Mutex | async-channel | Flume | Kanal |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 128 | Fast | 1,818,391 | 1,984,276 | 2,220,927 | 2,566,464 |
| 1 | 128 | yield_now | 1,549,596 | 2,012,324 | 2,165,946 | 2,233,936 |
| 1 | 128 | 100 ms I/O | 1,262 | 1,264 | 1,263 | 1,263 |
| 1 | 1000 | Fast | 1,904,078 | 2,494,516 | 2,715,772 | 2,792,786 |
| 1 | 1000 | yield_now | 1,604,730 | 1,952,166 | 2,022,880 | 2,031,314 |
| 1 | 1000 | 100 ms I/O | 9,848 | 9,850 | 9,850 | 9,844 |
| 4 | 128 | Fast | 978,388 | 1,159,617 | 1,170,042 | 1,181,008 |
| 4 | 128 | yield_now | 1,023,775 | 1,184,490 | 1,202,315 | 1,235,094 |
| 4 | 128 | 100 ms I/O | 1,260 | 1,264 | 1,264 | 1,264 |
| 4 | 1000 | Fast | 935,992 | 1,135,243 | 1,161,092 | 1,133,819 |
| 4 | 1000 | yield_now | 989,588 | 1,092,176 | 1,100,634 | 1,127,488 |
| 4 | 1000 | 100 ms I/O | 9,783 | 9,789 | 9,798 | 9,801 |

## Relative throughput

Ratio of candidate median throughput to Tokio median throughput.

| Runtime threads | C / Q | Handler | async-channel / Tokio | Flume / Tokio | Kanal / Tokio |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 128 | Fast | 1.091x | 1.221x | 1.411x |
| 1 | 128 | yield_now | 1.299x | 1.398x | 1.442x |
| 1 | 128 | 100 ms I/O | 1.002x | 1.001x | 1.001x |
| 1 | 1000 | Fast | 1.310x | 1.426x | 1.467x |
| 1 | 1000 | yield_now | 1.217x | 1.261x | 1.266x |
| 1 | 1000 | 100 ms I/O | 1.000x | 1.000x | 1.000x |
| 4 | 128 | Fast | 1.185x | 1.196x | 1.207x |
| 4 | 128 | yield_now | 1.157x | 1.174x | 1.206x |
| 4 | 128 | 100 ms I/O | 1.002x | 1.003x | 1.002x |
| 4 | 1000 | Fast | 1.213x | 1.240x | 1.211x |
| 4 | 1000 | yield_now | 1.104x | 1.112x | 1.139x |
| 4 | 1000 | 100 ms I/O | 1.001x | 1.001x | 1.002x |

## Queue-wait percentiles

Median of the eight per-run sampled percentiles, microseconds. Full p50/p95/p99
and delivery p95 values for every sample are in the CSV.

| Runtime threads | C / Q | Handler | Tokio p95 / p99 us | async-channel p95 / p99 us | Flume p95 / p99 us | Kanal p95 / p99 us |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 128 | Fast | 70.7 / 90.9 | 38.6 / 56.9 | 36.4 / 50.8 | 150.0 / 192.5 |
| 1 | 128 | yield_now | 94.7 / 113.2 | 62.4 / 83.5 | 59.1 / 78.7 | 56.8 / 76.1 |
| 1 | 128 | 100 ms I/O | 101,577.4 / 101,577.4 | 101,499.7 / 101,499.7 | 101,605.3 / 101,605.3 | 101,602.9 / 101,602.9 |
| 1 | 1000 | Fast | 548.8 / 565.9 | 321.8 / 345.9 | 314.3 / 340.6 | 4,128.2 / 6,189.5 |
| 1 | 1000 | yield_now | 660.5 / 681.6 | 504.7 / 527.5 | 485.1 / 492.9 | 495.1 / 635.2 |
| 1 | 1000 | 100 ms I/O | 101,780.7 / 101,796.1 | 101,379.2 / 101,402.4 | 101,345.4 / 101,506.5 | 102,350.1 / 102,450.9 |
| 4 | 128 | Fast | 153.0 / 171.9 | 122.4 / 135.0 | 122.2 / 133.0 | 122.3 / 134.1 |
| 4 | 128 | yield_now | 189.6 / 211.8 | 115.7 / 125.8 | 114.3 / 122.8 | 114.5 / 123.1 |
| 4 | 128 | 100 ms I/O | 101,577.1 / 101,577.1 | 101,522.5 / 101,522.5 | 101,576.4 / 101,576.4 | 101,641.4 / 101,641.4 |
| 4 | 1000 | Fast | 1,232.3 / 1,384.6 | 951.4 / 970.4 | 925.0 / 957.3 | 946.9 / 42,416.1 |
| 4 | 1000 | yield_now | 1,162.5 / 1,238.1 | 843.3 / 853.9 | 841.2 / 874.9 | 888.2 / 8,259.0 |
| 4 | 1000 | 100 ms I/O | 102,329.6 / 102,350.0 | 101,991.3 / 102,056.2 | 101,608.8 / 101,658.8 | 102,408.2 / 102,554.0 |

## Elapsed-time dispersion

Interquartile range (25th–75th percentile) of elapsed time, milliseconds.

| Runtime threads | C / Q | Handler | Tokio IQR ms | async-channel IQR ms | Flume IQR ms | Kanal IQR ms |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 1 | 128 | Fast | 27.349–27.894 | 24.768–25.534 | 22.149–22.878 | 19.258–19.788 |
| 1 | 128 | yield_now | 12.781–12.979 | 9.874–9.995 | 9.192–9.333 | 8.882–8.998 |
| 1 | 128 | 100 ms I/O | 405.194–406.018 | 404.938–405.469 | 405.187–405.679 | 405.173–406.043 |
| 1 | 1000 | Fast | 26.078–26.903 | 19.939–20.242 | 18.344–18.515 | 17.850–17.921 |
| 1 | 1000 | yield_now | 12.171–12.687 | 10.179–10.408 | 9.597–9.994 | 9.636–10.054 |
| 1 | 1000 | 100 ms I/O | 405.577–406.596 | 405.701–406.421 | 405.425–406.495 | 405.820–406.737 |
| 4 | 128 | Fast | 50.386–51.243 | 42.254–43.781 | 42.510–43.251 | 42.142–42.797 |
| 4 | 128 | yield_now | 19.224–19.723 | 16.425–17.330 | 16.415–16.952 | 16.031–16.411 |
| 4 | 128 | 100 ms I/O | 405.880–406.305 | 404.220–406.082 | 404.950–405.615 | 405.084–405.714 |
| 4 | 1000 | Fast | 52.639–54.061 | 43.576–44.593 | 42.374–43.748 | 43.720–45.367 |
| 4 | 1000 | yield_now | 19.808–20.571 | 17.817–18.690 | 17.655–18.763 | 17.199–18.315 |
| 4 | 1000 | 100 ms I/O | 407.714–409.249 | 408.237–409.034 | 407.993–408.491 | 407.685–408.822 |

## Interpretation

Kanal has the highest median throughput in the current-thread fast/yield profiles.
With four runtime worker threads, the MPMC candidates are close: Flume leads
fast dispatch at C=1000, while Kanal leads fast dispatch at C=128 and the yield
profiles. Inspect the IQRs before interpreting small differences as a stable
ranking; this experiment does not test statistical significance or enforce a
CI-speed threshold.

For the 100 ms I/O scenario, all candidate medians differ from Tokio by at most 0.28%.
The waiting workload dominates throughput; changing the local queue does not
provide a material throughput improvement in these measured I/O profiles.

This is a queue-pipeline comparison including admission and wakeups, not an
isolated mutex instruction. Latency samples start at enqueue, excluding producer
backpressure before publication and time pending in durable storage. Identical
instrumentation adds overhead. HTTP is represented by a timer; PostgreSQL, sockets,
TLS, connection pools, remote limits and failures are not exercised. The measured
winner is not a universal production recommendation. Re-run on deployment hardware.

## Verification

The manual release performance test passed: all 384 samples and balanced-order
checks are valid. Ordinary checks passed: `cargo fmt --all --check`, workspace
Clippy with `-D warnings`, workspace tests (141 passed; one manual performance test
ignored), and mandatory workspace llvm-cov with a 90% threshold. Line coverage:
97.16%. Smoke coverage includes all four channels/workloads/runtime types, full
finite batches with backpressure and end-of-input draining, and shutdown of idle
workers with pending receive futures while their producer stays alive.
