# orchestration-platform

A small distributed compute orchestrator: a C++20 coordinator schedules container jobs
onto Python worker agents over WebSockets, with capability-aware placement, at-least-once
execution with attempt fencing, and Prometheus/Grafana observability.

The design, its trade-offs and the decision log are in [docs/DESIGN.md](docs/DESIGN.md);
the worker protocol is in [proto/PROTOCOL.md](proto/PROTOCOL.md).

> **Status:** scheduling, the job state machine, end-to-end execution and worker
> failure detection work and are benchmarked below. Coordinator persistence and
> restart recovery (DESIGN.md §8) are in progress: today a coordinator restart
> loses its job table.

## Results

**Setup: a single host over loopback.** Apple M4 Pro, macOS 15.3, Docker Desktop
29.6 (Linux VM with 14 CPUs and 8 GB). The coordinator (release build), 3 workers
(5 slots) and the job containers all run on that one machine, so network latency
is not included. One run of [`bench/bench.py`](bench/bench.py) at `3abee38`:

| Scenario | Metric | n | p50 | p95 | p99 |
|---|---|---|---|---|---|
| Idle (one job at a time) | Dispatch (`assign` → `ack`) | 50 | 0.29 ms | 0.34 ms | 0.66 ms |
| | Start, cached image (queued → `started`) | 50 | 102 ms | 112 ms | 115 ms |
| Burst (100 jobs onto 5 slots) | Queue wait (queued → `assign`) | 100 | 4.57 s | 8.95 s | 9.30 s |
| | Dispatch | 100 | 0.25 ms | 0.42 ms | 0.57 ms |
| Cold start (image pulled) | Start, pulled image | 10 | 3.05 s | max 3.30 s | |
| Worker crash (`docker kill`) | Time to reassign | 5 | 108 ms | max 129 ms | |
| Worker hang (`docker pause`) | Time to detect / to reassign | 5 | 4.81 / 4.81 s | max 4.82 / 4.83 s | |

- **Percentiles are exact** (nearest rank) over the coordinator's own per-attempt
  nanosecond samples. With fewer than 100 samples, p99 is the maximum, and rows
  with 5 or 10 samples show only the median and the maximum. All tables, raw
  samples and the method: [bench/results/latest.md](bench/results/latest.md),
  [bench/README.md](bench/README.md).
- **Dispatch stays under 1 ms even when 100 jobs queue for 5 slots.** The backlog
  shows up as queue wait, which is a capacity question, not a scheduling one.
- **A cached start is about 100 ms, almost all Docker** creating and starting the
  container. A cold start adds the registry pull, so it depends on the network.
- **Crash numbers are dominated by the `docker kill` command itself** (about 104 ms;
  reported in its own row in the full results). The socket closing is noticed almost
  immediately.
- **Hang detection is about 4.8 s here, but in general can fall anywhere from about
  4 s to 5 s.** A node is declared dead 5 s after its *last heartbeat*, and heartbeats
  are 1 s apart, so the time from a hang to its detection depends on where in that
  interval the hang starts. This run's five samples happened to land at almost the
  same point, so their spread is a few milliseconds; expect up to about 1 s in general.

![Grafana dashboard during the benchmark run](docs/images/grafana-bench.png)

*The dashboard during that run, in order: burst, cold starts, crashes, hangs. Its
quantiles are `histogram_quantile()` estimates over a 1-minute window; the table
above is exact. For example, the dispatch panel's late jump to about 47 ms is a
single 30 ms sample (the first job on a worker that had just been un-paused), which
the estimate places near the top of its histogram bucket.*

## Demo (about 5 minutes)

Needs Docker with Compose v2, `curl` and `jq`. Ports 8080, 9090 and 3000 must be free.

**1. Start the stack** (coordinator, three workers, Prometheus, Grafana):

```sh
export CLUSTER_TOKEN="$(openssl rand -hex 32)"
docker compose up --build -d
```

The first build compiles the coordinator's dependencies and takes a few minutes.

**2. See the workers** (two CPU nodes, one simulated CUDA node):

```sh
curl -s localhost:8080/v1/nodes | jq -c '.nodes[] | {node_id, health, slots, accelerator: .capabilities.accelerator}'
```

**3. Run a job and look at it:**

```sh
JOB=$(curl -s localhost:8080/v1/jobs \
  -d '{"image": "alpine:3.20", "command": ["sh", "-c", "echo hello from orch"]}' | jq -r .job_id)
sleep 3
curl -s localhost:8080/v1/jobs/$JOB | jq '{state, exit_code, attempts}'
```

`attempts` shows the node and the exact queue wait, dispatch and start latency in nanoseconds,
and whether the image had to be pulled. The job's output is in
`docker compose logs worker-cpu worker-cpu-2`.

**4. Open Grafana** at <http://localhost:3000> (no login), then submit a burst of 30
short jobs and watch the panels fill in:

```sh
for i in $(seq 30); do
  curl -s localhost:8080/v1/jobs -d "{\"image\": \"alpine:3.20\", \"command\": [\"sleep\", \"$((i % 3 + 1))\"]}" > /dev/null
done
curl -s localhost:8080/v1/jobs | jq -r '.jobs | group_by(.state)[] | "\(.[0].state): \(length)"'
```

Five slots in total, so the burst queues up and drains in about 15 s. Run the last
command a few times to watch jobs move from `queued` through `running` to `succeeded`.
The latency panels are `histogram_quantile()` estimates over a 1-minute window.

The burst shows up in **queue wait** (seconds: jobs waiting for a free slot), while
**dispatch latency** (assign sent → ack) stays small. DESIGN.md §11 defines all three.

**5. A few more things to try:**

```sh
# A cold start: remove an image first, and the start latency is labelled "pulled".
docker image rm -f busybox:1.37 2>/dev/null
curl -s localhost:8080/v1/jobs -d '{"image": "busybox:1.37", "command": ["true"]}' | jq -r .job_id

# A CUDA job goes only to the (simulated) CUDA node.
curl -s localhost:8080/v1/jobs -d '{"image": "alpine:3.20", "command": ["true"],
  "requirements": {"accelerator": "cuda", "min_gpu_mem_mb": 16384}}' | jq -r .job_id

# No MPS node is connected, so this one waits as "unschedulable" instead of "queued".
curl -s localhost:8080/v1/jobs -d '{"image": "alpine:3.20", "command": ["true"],
  "requirements": {"accelerator": "mps"}}' | jq '{job_id, state}'

# Failures are reported, not retried: a non-zero exit, and a job over its time limit.
curl -s localhost:8080/v1/jobs -d '{"image": "alpine:3.20", "command": ["sh", "-c", "exit 3"]}' | jq -r .job_id
curl -s localhost:8080/v1/jobs -d '{"image": "alpine:3.20", "command": ["sleep", "30"], "max_runtime_s": 2}' | jq -r .job_id

# Cancel a running job (its container is killed).
JOB=$(curl -s localhost:8080/v1/jobs -d '{"image": "alpine:3.20", "command": ["sleep", "60"]}' | jq -r .job_id)
sleep 2; curl -s -X POST localhost:8080/v1/jobs/$JOB/cancel | jq '{state, reason}'
```

**6. Stop it:** `docker compose down`.

To record the demo as a GIF, run [`deploy/demo.sh`](deploy/demo.sh) (a narrated,
paced version of the steps above) through [VHS](https://github.com/charmbracelet/vhs):
`vhs docs/demo.tape`.

## Known limitations

- **A crashed worker leaves its job container behind.** If a worker process dies
  (for example `docker kill`), the job container it started keeps running until its
  command ends and is then left stopped, because only the worker removes it. The job
  itself is requeued and completes elsewhere. The bench removes these leftovers and
  counts them (one per crash). Cleaning them up when the worker restarts is future work.
- **No coordinator persistence yet.** A coordinator restart loses its job table;
  workers reconnect on their own, but jobs submitted before the restart are gone. This
  is the next milestone (DESIGN.md §8).
- **One coordinator** is a single point of failure: scheduling stops while it is down
  (DESIGN.md §8.4).
- **Hang detection takes 4–5 s** by default (DESIGN.md §10). Shorter timeouts recover
  faster but declare slow workers dead more often; both are flags.

## Security notes

- **The workers mount the host's Docker socket** to start job containers. Anything
  that can use that socket is root-equivalent on the host, so this stack is for a
  local demo only.
- Every port is published on `127.0.0.1`. Grafana is read-only and anonymous.
- Workers authenticate with a shared token, which reaches containers as a Compose
  secret file, never as a command-line flag or a container environment variable.
  Traffic is plain `ws://` and `http://`; TLS is out of scope (DESIGN.md §1).

## API

| Method & path | |
|---|---|
| `POST /v1/jobs` | Submit `{image, command, requirements?, max_runtime_s?, max_retries?, idempotency_key?}`. `201`, or `200` with the original job on an idempotency-key replay. |
| `GET /v1/jobs?state=` | List jobs, optionally by state. |
| `GET /v1/jobs/{id}` | One job, with per-attempt latencies. |
| `POST /v1/jobs/{id}/cancel` | Idempotent; `409` if the job already finished otherwise. |
| `GET /v1/nodes` | Workers with capabilities, load and health. |
| `GET /metrics`, `GET /healthz` | Prometheus exposition; liveness. |

## Development

```sh
# Coordinator (C++20, CMake + vcpkg manifest mode; VCPKG_ROOT must point at vcpkg)
cd coordinator && cmake --preset debug-asan && cmake --build --preset debug-asan \
  && ctest --preset debug-asan

# Worker unit tests and integration tests (against the debug-asan coordinator)
python -m venv .venv && .venv/bin/pip install -e "worker[test]" -r tests/integration/requirements.txt
.venv/bin/python -m pytest worker/tests tests/integration

# The whole stack, checked end to end
deploy/smoke.sh
```

CI runs all of the above, plus TSan and a macOS build.
