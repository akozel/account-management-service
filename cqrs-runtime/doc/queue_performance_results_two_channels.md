# Local queue performance results (archived two-channel run)

Run: 2026-10-10 15:07 UTC.

Environment: Rust 1.98.1, release profile, Linux 7.2.9-arch1-1 x86_64,
Intel Core i7-12650H; cgroup CPU quota `800000/100000` (8 CPU equivalents).
No CPU affinity or exclusive host reservation. Reported benchmark metadata:

```text
BENCH_META,os=linux,arch=x86_64,available_parallelism=8,sample_every=32,repetitions=7
```

Command recorded for this historical run (the test now compares four channels):

```sh
cargo test --release -p cqrs-runtime --lib --all-features \
  worker::worker_task::tests::performance::compare_current_and_mpmc \
  -- --ignored --nocapture --test-threads=1
```

Each row summarizes 7 repetitions per backend. Q=C. The four-thread runtime also
has the block_on caller running the producer; current-thread runs everything
on one thread. Method, instrumentation and exclusions are described in
[Local worker queue performance](queue_performance_two_channels.md). All 168 measured samples
passed message-count/checksum checks. [Raw CSV](queue_performance_results_two_channels.csv).

## Median throughput and queue wait

Gain = median MPMC deliveries/s divided by median Tokio deliveries/s.
Queue p95 is the median of each repetition's sampled p95, in microseconds.

| Runtime threads | C / Q | Handler | Tokio deliveries/s | MPMC deliveries/s | Gain | Tokio queue p95 us | MPMC queue p95 us |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 128 | Fast | 1,683,117 | 1,818,259 | 1.080x | 76.2 | 44.6 |
| 1 | 128 | yield_now | 1,466,446 | 1,966,820 | 1.341x | 100.1 | 63.4 |
| 1 | 128 | 100 ms I/O | 1,263 | 1,261 | 0.998x | 101,505.4 | 101,548.8 |
| 1 | 1000 | Fast | 1,894,165 | 2,451,289 | 1.294x | 566.4 | 325.4 |
| 1 | 1000 | yield_now | 1,494,069 | 1,839,865 | 1.231x | 720.8 | 543.4 |
| 1 | 1000 | 100 ms I/O | 9,860 | 9,838 | 0.998x | 101,366.5 | 102,060.8 |
| 4 | 128 | Fast | 1,000,039 | 1,187,985 | 1.188x | 150.4 | 118.0 |
| 4 | 128 | yield_now | 1,018,272 | 1,215,048 | 1.193x | 193.0 | 116.6 |
| 4 | 128 | 100 ms I/O | 1,262 | 1,262 | 1.000x | 101,631.1 | 101,564.8 |
| 4 | 1000 | Fast | 968,596 | 1,181,888 | 1.220x | 1,187.4 | 922.3 |
| 4 | 1000 | yield_now | 997,089 | 1,133,037 | 1.136x | 1,141.4 | 792.8 |
| 4 | 1000 | 100 ms I/O | 9,803 | 9,804 | 1.000x | 102,185.9 | 101,747.6 |

## Elapsed-time dispersion

Interquartile range (25th–75th percentile) of the seven elapsed samples, milliseconds.

| Runtime threads | C / Q | Handler | Tokio elapsed IQR ms | MPMC elapsed IQR ms |
| ---: | ---: | --- | ---: | ---: |
| 1 | 128 | Fast | 28.959–32.224 | 26.477–28.187 |
| 1 | 128 | yield_now | 13.537–13.992 | 10.128–10.322 |
| 1 | 128 | 100 ms I/O | 405.205–405.909 | 405.311–406.028 |
| 1 | 1000 | Fast | 26.312–27.171 | 19.809–20.432 |
| 1 | 1000 | yield_now | 13.235–13.599 | 10.867–10.910 |
| 1 | 1000 | 100 ms I/O | 405.503–406.079 | 406.461–407.216 |
| 4 | 128 | Fast | 47.962–52.671 | 41.718–42.514 |
| 4 | 128 | yield_now | 19.441–19.993 | 16.335–16.771 |
| 4 | 128 | 100 ms I/O | 405.161–406.295 | 405.253–405.924 |
| 4 | 1000 | Fast | 50.286–52.485 | 41.344–42.671 |
| 4 | 1000 | yield_now | 19.629–21.210 | 17.226–17.943 |
| 4 | 1000 | 100 ms I/O | 407.633–408.370 | 407.945–408.655 |

## Interpretation

Both backends sustain the configured I/O concurrency. In the 100 ms waiting
scenarios, their median throughput is essentially equal; the queue is not the
dominant cost in this workload. For fast/yield handlers, consult the measured
gain and dispersion above. Differences in local dispatch do not establish
a corresponding real HTTP/PostgreSQL throughput improvement.

These numbers compare the queue pipelines (including different admission and
wakeup mechanisms), not an isolated mutex instruction. The latency samples exclude
producer backpressure before enqueue and time spent pending in durable storage.
They include shared, identical measurement overhead. Synthetic I/O uses timers;
it does not exercise sockets, TLS, connection pools, rate limits or remote failures.
Host scheduling and cache effects can change results on another run. Re-run the
paired benchmark on deployment hardware before choosing a production queue.

## Verification

The manual release performance test passed separately. Ordinary workspace checks
passed: fmt, Clippy with `-D warnings`, 139 tests (one manual performance test
ignored), and the mandatory workspace llvm-cov threshold of 90%. Line coverage:
96.97%. The regular comparison smoke test executes both queue implementations
and all three workloads on current-thread and four-thread runtimes.
