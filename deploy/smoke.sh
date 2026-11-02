#!/usr/bin/env bash
# Brings up the Compose stack and checks the whole observability path, then
# the worker's shutdown and fatal-exit behaviour inside containers.
#
#   deploy/smoke.sh          (builds images if missing; tears the stack down)
set -euo pipefail
cd "$(dirname "$0")/.."

export CLUSTER_TOKEN="${CLUSTER_TOKEN:-smoke-test-token}"
compose() { docker compose "$@"; }

cleanup() {
    local status=$?
    if [ "$status" -ne 0 ]; then compose logs --no-color --tail 50 || true; fi
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    exit "$status"
}
trap cleanup EXIT

# check DESCRIPTION COMMAND...: retries COMMAND for up to 60 s.
check() {
    local what=$1
    shift
    for _ in $(seq 60); do
        if "$@" >/dev/null 2>&1; then
            echo "ok: $what"
            return 0
        fi
        sleep 1
    done
    echo "FAIL: $what" >&2
    return 1
}

# expect DESCRIPTION COMMAND...: runs COMMAND once.
expect() {
    local what=$1
    shift
    if "$@"; then echo "ok: $what"; else echo "FAIL: $what" >&2; return 1; fi
}

prom_equals() {  # prom_equals QUERY VALUE: an instant query through Prometheus
    [ "$(curl -fsS --get http://127.0.0.1:9090/api/v1/query --data-urlencode "query=$1" \
        | jq -r '.data.result[0].value[1] // empty')" = "$2" ]
}

dashboard_has_seven_panels() {
    [ "$(curl -fsS http://127.0.0.1:3000/api/dashboards/uid/orch | jq '.dashboard.panels | length')" = 7 ]
}

grafana_queries_prometheus() {  # as the anonymous viewer, like a dashboard panel
    curl -fsS -X POST http://127.0.0.1:3000/api/ds/query -H 'Content-Type: application/json' \
        -d '{"from":"now-1m","to":"now","queries":[{"refId":"A","instant":true,
             "datasource":{"type":"prometheus","uid":"prometheus"},"expr":"orch_nodes"}]}' \
        | jq -e '.results.A.frames | length > 0'
}

worker_container() { compose ps --all --quiet "$1"; }
inspect() { docker inspect -f "$2" "$(worker_container "$1")"; }
equals() { [ "$1" = "$2" ]; }
equals_cmd() { [ "$(job_state "$1")" = "$2" ]; }  # re-evaluated on every retry

workers_never_restart() {
    equals "$(inspect worker-cpu '{{.HostConfig.RestartPolicy.Name}}')" no &&
        equals "$(inspect worker-cpu-2 '{{.HostConfig.RestartPolicy.Name}}')" no &&
        equals "$(inspect worker-sim-cuda '{{.HostConfig.RestartPolicy.Name}}')" no
}

wrong_token_exit_code() {
    local code=0
    CLUSTER_TOKEN=wrong-token compose run --rm --no-deps worker-cpu >/dev/null 2>&1 || code=$?
    echo "$code"
}

compose up --detach --wait --wait-timeout 300

check "coordinator /healthz" curl -fsS http://127.0.0.1:8080/healthz
check "Prometheus scrapes the coordinator" prom_equals 'up{job="coordinator"}' 1
check "all three workers connected (seen through Prometheus)" \
    prom_equals 'orch_nodes{health="alive"}' 3
check "Grafana provisioned the 7-panel dashboard" dashboard_has_seven_panels
check "Grafana queries Prometheus" grafana_queries_prometheus

submit_job() {  # prints the new job's id
    curl -fsS http://127.0.0.1:8080/v1/jobs -d "$1" | jq -r .job_id
}
job_state() { curl -fsS "http://127.0.0.1:8080/v1/jobs/$1" | jq -r .state; }

job=$(submit_job '{"image": "alpine:3.20", "command": ["sh", "-c", "echo smoke; exit 0"]}')
check "a container job runs to success" equals_cmd "$job" succeeded
gpu=$(submit_job '{"image": "alpine:3.20", "command": ["true"],
                   "requirements": {"accelerator": "cuda", "min_gpu_mem_mb": 16384}}')
check "a CUDA job runs on the simulated CUDA node" equals_cmd "$gpu" succeeded
expect "it was placed on worker-sim-cuda" equals \
    "$(curl -fsS "http://127.0.0.1:8080/v1/jobs/$gpu" | jq -r '.attempts[0].node')" worker-sim-cuda
check "latency histograms reach Prometheus" prom_equals 'orch_dispatch_latency_seconds_count' 2

# The live endpoint, not just the metrics_sample fixture, passes Prometheus's linter.
curl -fsS http://127.0.0.1:8080/metrics | compose exec -T prometheus promtool check metrics
echo "ok: live /metrics passes promtool"

expect "workers have restart policy \"no\"" workers_never_restart

# docker stop sends SIGTERM: the agent must close the socket and exit 0.
compose stop worker-cpu
expect "worker exits 0 on docker stop" equals "$(inspect worker-cpu '{{.State.ExitCode}}')" 0
check "coordinator saw the disconnect" prom_equals 'orch_nodes{health="alive"}' 2

# A wrong token must end the container with the distinct "rejected" code.
expect "worker with a wrong token exits 3" equals "$(wrong_token_exit_code)" 3

echo "smoke test passed"
