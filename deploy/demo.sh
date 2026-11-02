#!/usr/bin/env bash
# A short narrated walk through the running stack, paced for screen recording
# (docs/demo.tape records it as a GIF with VHS). Start the stack first:
#
#   export CLUSTER_TOKEN="$(openssl rand -hex 32)"; docker compose up --build -d
#   deploy/demo.sh
set -euo pipefail

API=http://localhost:8080
say() { printf '\n\033[1m# %s\033[0m\n' "$*"; sleep 1; }
run() { printf '$ %s\n' "$*"; eval "$@"; sleep 1.5; }
submit() { curl -s "$API/v1/jobs" -d "$1" | jq -r .job_id; }

say "Three workers: two CPU nodes and one simulated CUDA node"
run "curl -s $API/v1/nodes | jq -c '.nodes[] | {node_id, health, slots, accelerator: .capabilities.accelerator}'"

say "Submit one job: it runs as a container on a worker"
JOB=$(submit '{"image": "alpine:3.20", "command": ["sh", "-c", "echo hello from orch"]}')
sleep 2
run "curl -s $API/v1/jobs/$JOB | jq -c '{state, exit_code, node: .attempts[0].node}'"

say "Per-attempt latencies, exact, in nanoseconds"
run "curl -s $API/v1/jobs/$JOB | jq -c '.attempts[0] | {queue_wait_ns, dispatch_latency_ns, start_latency_ns}'"

say "A burst of 20 jobs onto 5 slots: they queue, then drain"
for i in $(seq 20); do submit '{"image": "alpine:3.20", "command": ["sleep", "1"]}' > /dev/null; done
for _ in 1 2 3 4; do
    curl -s "$API/v1/jobs" | jq -r -c '[.jobs | group_by(.state)[] | {(.[0].state): length}] | add'
    sleep 1.5
done

say "A CUDA job only goes to the CUDA node; an MPS job has nowhere to go"
GPU=$(submit '{"image": "alpine:3.20", "command": ["true"], "requirements": {"accelerator": "cuda"}}')
MPS=$(submit '{"image": "alpine:3.20", "command": ["true"], "requirements": {"accelerator": "mps"}}')
sleep 2
run "curl -s $API/v1/jobs/$GPU | jq -c '{state, node: .attempts[0].node}'"
run "curl -s $API/v1/jobs/$MPS | jq -c '{state}'"

say "Kill the worker running a job: it completes on another worker"
JOB=$(submit '{"image": "alpine:3.20", "command": ["sleep", "3"]}')
sleep 1.5
NODE=$(curl -s "$API/v1/jobs/$JOB" | jq -r .node)
CONTAINER=$(docker compose ps -q "$NODE" | cut -c1-12)  # service name == node id
run "docker kill $CONTAINER > /dev/null  # $NODE"
sleep 4
run "curl -s $API/v1/jobs/$JOB | jq -c '{state, attempts: [.attempts[] | {node, outcome}]}'"
docker start "$CONTAINER" > /dev/null  # bring the worker back

say "Open http://localhost:3000 for the Grafana dashboard"
