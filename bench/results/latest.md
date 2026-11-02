# Benchmark results (2026-11-02T06:48:51+00:00)

Commit `455adedc0ccd` · Apple M4 Pro · macOS-15.3.1-arm64-arm-64bit-Mach-O · Docker 29.6.1 (14 CPUs, 8320954368 bytes) · Compose: coordinator (RelWithDebInfo) + 3 workers, 5 slots in total.

Exact nearest-rank percentiles from raw samples, in **milliseconds**. With fewer than 100 samples, p99 is the maximum. Method: [../README.md](../README.md).

## Idle (one job at a time)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 50 | 0.00 | 0.00 | 0.00 | 0.00 |
| Dispatch | 50 | 1.31 | 2.52 | 4.28 | 4.28 |
| Start, cached image | 50 | 105.10 | 120.79 | 127.76 | 127.76 |

## Burst (100 jobs submitted at 423.1/s onto 5 slots)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 100 | 4629.69 | 9144.19 | 9644.03 | 9699.03 |
| Dispatch | 100 | 1.69 | 3.59 | 14.63 | 24.86 |
| Start, cached image | 100 | 5121.78 | 9692.10 | 10026.54 | 10052.49 |

## Cold start (image removed before each job)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 10 | 0.00 | 0.00 | 0.00 | 0.00 |
| Dispatch | 10 | 1.14 | 1.54 | 1.54 | 1.54 |
| Start, pulled image | 10 | 3032.99 | 3803.73 | 3803.73 | 3803.73 |

## Worker crash (`docker kill`: socket closes)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Time to detect (node `dead`) | 5 | 106.75 | 111.03 | 111.03 | 111.03 |
| Time to reassign (attempt 2 sent) | 5 | 107.88 | 112.14 | 112.14 | 112.14 |
| (docker command itself) | 5 | 105.44 | 109.35 | 109.35 | 109.35 |

## Worker hang (`docker pause`: only heartbeats stop)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Time to detect (node `dead`) | 5 | 4820.45 | 4823.13 | 4823.13 | 4823.13 |
| Time to reassign (attempt 2 sent) | 5 | 4822.32 | 4824.07 | 4824.07 | 4824.07 |
| (docker command itself) | 5 | 30.49 | 30.75 | 30.75 | 30.75 |

Failover times are measured by this script from just before the docker command, polling the API every 10 ms, so they include the docker CLI's own latency (last row) and up to one polling interval. Every failover job completed on another worker.
