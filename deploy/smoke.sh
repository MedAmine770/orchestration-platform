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

dashboard_has_six_panels() {
    [ "$(curl -fsS http://127.0.0.1:3000/api/dashboards/uid/orch | jq '.dashboard.panels | length')" = 6 ]
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

workers_never_restart() {
    equals "$(inspect worker-cpu '{{.HostConfig.RestartPolicy.Name}}')" no &&
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
check "both workers connected (seen through Prometheus)" prom_equals 'orch_nodes{health="alive"}' 2
check "Grafana provisioned the 6-panel dashboard" dashboard_has_six_panels
check "Grafana queries Prometheus" grafana_queries_prometheus

# The live endpoint, not just the metrics_sample fixture, passes Prometheus's linter.
curl -fsS http://127.0.0.1:8080/metrics | compose exec -T prometheus promtool check metrics
echo "ok: live /metrics passes promtool"

expect "workers have restart policy \"no\"" workers_never_restart

# docker stop sends SIGTERM: the agent must close the socket and exit 0.
compose stop worker-cpu
expect "worker exits 0 on docker stop" equals "$(inspect worker-cpu '{{.State.ExitCode}}')" 0
check "coordinator saw the disconnect" prom_equals 'orch_nodes{health="alive"}' 1

# A wrong token must end the container with the distinct "rejected" code.
expect "worker with a wrong token exits 3" equals "$(wrong_token_exit_code)" 3

echo "smoke test passed"
