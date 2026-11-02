# Benchmark results (2026-11-02T06:11:57+00:00)

Commit `6eda301e6bf2` · Apple M4 Pro · macOS-15.3.1-arm64-arm-64bit-Mach-O · Docker 29.6.1 (14 CPUs, 8320954368 bytes) · Compose: coordinator (RelWithDebInfo) + 3 workers, 5 slots in total.

Exact nearest-rank percentiles from raw samples, in **milliseconds**. With fewer than 100 samples, p99 is the maximum. Method: [../README.md](../README.md).

## Idle (one job at a time)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 50 | 0.00 | 0.00 | 0.00 | 0.00 |
| Dispatch | 50 | 0.31 | 0.45 | 0.49 | 0.49 |
| Start, cached image | 50 | 103.74 | 116.70 | 124.25 | 124.25 |

## Burst (100 jobs submitted at 817.9/s onto 5 slots)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 100 | 4597.27 | 9322.93 | 9678.89 | 9707.06 |
| Dispatch | 100 | 0.29 | 0.59 | 0.68 | 0.70 |
| Start, cached image | 100 | 5150.48 | 9696.44 | 10045.63 | 10185.27 |

## Cold start (image removed before each job)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 10 | 0.00 | 0.00 | 0.00 | 0.00 |
| Dispatch | 10 | 0.31 | 0.36 | 0.36 | 0.36 |
| Start, pulled image | 10 | 3026.70 | 3424.85 | 3424.85 | 3424.85 |

## Worker crash (`docker kill`: socket closes)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Time to detect (node `dead`) | 5 | 99.24 | 131.42 | 131.42 | 131.42 |
| Time to reassign (attempt 2 sent) | 5 | 100.29 | 132.39 | 132.39 | 132.39 |
| (docker command itself) | 5 | 97.79 | 130.08 | 130.08 | 130.08 |

## Worker hang (`docker pause`: only heartbeats stop)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Time to detect (node `dead`) | 5 | 4821.58 | 4826.34 | 4826.34 | 4826.34 |
| Time to reassign (attempt 2 sent) | 5 | 4822.68 | 4827.26 | 4827.26 | 4827.26 |
| (docker command itself) | 5 | 30.27 | 32.16 | 32.16 | 32.16 |

Failover times are measured by this script from just before the docker command, polling the API every 10 ms, so they include the docker CLI's own latency (last row) and up to one polling interval. Every failover job completed on another worker.
