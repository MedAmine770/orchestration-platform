# orchestration-platform

A small distributed compute orchestrator: a C++20 coordinator schedules container jobs
onto Python worker agents over WebSockets, with capability-aware placement, at-least-once
execution with attempt fencing, and Prometheus/Grafana observability.

The design, its trade-offs and the decision log are in [docs/DESIGN.md](docs/DESIGN.md);
the worker protocol is in [proto/PROTOCOL.md](proto/PROTOCOL.md).

> **Status:** work in progress. Scheduling, the job state machine and end-to-end
> execution work; heartbeat failure detection, benchmarks and persistence are next.
> No performance numbers are published until the bench scripts produce them.

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
