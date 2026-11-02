# Benchmark results (2026-11-02T06:20:10+00:00)

Commit `3abee382c54e` · Apple M4 Pro · macOS-15.3.1-arm64-arm-64bit-Mach-O · Docker 29.6.1 (14 CPUs, 8320954368 bytes) · Compose: coordinator (RelWithDebInfo) + 3 workers, 5 slots in total.

Exact nearest-rank percentiles from raw samples, in **milliseconds**. With fewer than 100 samples, p99 is the maximum. Method: [../README.md](../README.md).

## Idle (one job at a time)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 50 | 0.00 | 0.00 | 0.00 | 0.00 |
| Dispatch | 50 | 0.29 | 0.34 | 0.66 | 0.66 |
| Start, cached image | 50 | 102.10 | 112.45 | 115.23 | 115.23 |

## Burst (100 jobs submitted at 877.7/s onto 5 slots)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 100 | 4570.48 | 8951.38 | 9303.99 | 9544.77 |
| Dispatch | 100 | 0.25 | 0.42 | 0.57 | 0.59 |
| Start, cached image | 100 | 4942.36 | 9535.24 | 9912.95 | 9913.48 |

## Cold start (image removed before each job)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Queue wait | 10 | 0.00 | 0.00 | 0.00 | 0.00 |
| Dispatch | 10 | 0.30 | 0.40 | 0.40 | 0.40 |
| Start, pulled image | 10 | 3053.02 | 3298.92 | 3298.92 | 3298.92 |

## Worker crash (`docker kill`: socket closes)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Time to detect (node `dead`) | 5 | 106.34 | 127.86 | 127.86 | 127.86 |
| Time to reassign (attempt 2 sent) | 5 | 107.62 | 129.13 | 129.13 | 129.13 |
| (docker command itself) | 5 | 104.42 | 126.01 | 126.01 | 126.01 |

## Worker hang (`docker pause`: only heartbeats stop)

| Metric | n | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| Time to detect (node `dead`) | 5 | 4809.82 | 4822.71 | 4822.71 | 4822.71 |
| Time to reassign (attempt 2 sent) | 5 | 4811.11 | 4824.96 | 4824.96 | 4824.96 |
| (docker command itself) | 5 | 30.51 | 31.50 | 31.50 | 31.50 |

Failover times are measured by this script from just before the docker command, polling the API every 10 ms, so they include the docker CLI's own latency (last row) and up to one polling interval. Every failover job completed on another worker.
