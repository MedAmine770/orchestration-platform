# Benchmarks

`bench.py` measures the running Compose stack and writes:

- `results/<date>-<commit>.json`: every raw sample, the summary, the environment
  (CPU, OS, Docker version, commit) and the method;
- `results/latest.md`: the summary tables of the most recent run.

Results are committed. Nothing goes in the top-level README unless this script
produced it (DESIGN.md §11).

## Running it

```sh
export CLUSTER_TOKEN="$(openssl rand -hex 32)"
docker compose up --build -d
python3 bench/bench.py          # about 3 minutes; standard library only
```

It needs the stack exactly as `compose.yaml` defines it (3 workers, 5 slots) and
idle. It kills, pauses and restarts worker containers, so don't run it against a
stack you care about.

## What it measures

| Scenario | How | What it shows |
|---|---|---|
| **Idle** | 50 jobs, one at a time: each is submitted only after the previous one finished | The scheduler's own path, with a free slot every time |
| **Burst** | 100 jobs submitted back to back onto 5 slots | Queue wait under backlog; whether dispatch stays small |
| **Cold** | 10 jobs, each after `docker image rm` of its image | Start latency when the image must be pulled |
| **Crash** | `docker kill` the worker running a job, 5 times | Detection by socket close |
| **Hang** | `docker pause` the worker running a job, 5 times | Detection by missed heartbeats (`SUSPECT` 3 s, `DEAD` 5 s) |

Latency definitions are in DESIGN.md §11:
- **queue wait** is `QUEUED` → placed;
- **dispatch** is placed → `ack`, including the write-ahead commit (fsync) before the
  `assign` goes out (DESIGN.md §8.2);
- **start** is `QUEUED` → `started`.

Jobs are `alpine:3.20 true` (cached) and `busybox:1.37 true` (cold).

## Method, and what the numbers do not include

- **Latencies come from the coordinator**, not from this script. They are per-attempt
  integer nanoseconds on its monotonic clock, read back from `GET /v1/jobs/{id}`, so
  HTTP and polling overhead are excluded.
- **Percentiles are exact**, by the nearest-rank method on the raw samples: always
  an observed value, never interpolated. Grafana's `histogram_quantile()` is an
  estimate and can differ by up to one bucket width. With fewer than 100 samples,
  p99 is the maximum.
- **Failover times are measured by this script**: from just before the `docker`
  command, polling the API every 10 ms.
  - They include the docker CLI's own latency, reported in its own row.
  - They can be late by up to one polling interval.
  - Time to reassign is when attempt 2 appears, meaning the `assign` was sent.
- **Start latency includes Docker** creating and starting the container through
  Docker Desktop's Linux VM on macOS. Cold starts also include the registry pull, so
  they depend on the network.
- **One machine.** Coordinator, workers and job containers share the host. These
  are not multi-node network numbers.

## Known limitation

A crashed worker (`docker kill`) cannot remove the job container it started, so the
container finishes on its own and is left stopped. The script removes such leftovers
at the end and records how many there were. Cleaning them up when the worker restarts
is future work.
