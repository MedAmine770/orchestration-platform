"""The real worker process against the real coordinator: exit codes and
SIGTERM (which supervisors in compose.yaml depend on), and jobs running end to
end as real subprocesses. Workers here use the `process` runtime, so these
tests don't need Docker."""

import signal
import sys

from test_job_lifecycle import http
from test_worker_connection import ALIVE, eventually, scrape

EXIT_REJECTED = 3
EXIT_REPLACED = 4
TERMINAL = {"succeeded", "failed", "lost", "cancelled"}


def python(code: str, **spec) -> dict:
    return {"image": "unused-by-process-runtime", "command": [sys.executable, "-c", code],
            **spec}


def submit(coordinator, spec: dict) -> str:
    status, job = http(coordinator, "POST", "/v1/jobs", spec)
    assert status == 201, job
    return job["job_id"]


def wait_for_state(coordinator, job_id: str, states: set[str], timeout: float = 15) -> dict:
    result = {}

    def reached():
        _, result["job"] = http(coordinator, "GET", f"/v1/jobs/{job_id}")
        return result["job"]["state"] in states

    eventually(reached, timeout=timeout)
    return result["job"]


# ---- Lifecycle and exit codes --------------------------------------------------

def test_sigterm_disconnects_cleanly_and_exits_zero(coordinator, start_worker):
    worker = start_worker("w1")
    eventually(lambda: scrape(coordinator, ALIVE) == 1, timeout=10)
    worker.send_signal(signal.SIGTERM)
    assert worker.wait(timeout=10) == 0
    eventually(lambda: scrape(coordinator, ALIVE) == 0)


def test_bad_token_exits_with_rejected_code(start_worker):
    assert start_worker("w1", token="wrong-token").wait(timeout=10) == EXIT_REJECTED


def test_duplicate_node_id_exits_with_replaced_code(coordinator, start_worker):
    first = start_worker("same-node")
    eventually(lambda: scrape(coordinator, ALIVE) == 1, timeout=10)
    second = start_worker("same-node")
    assert first.wait(timeout=10) == EXIT_REPLACED
    assert second.poll() is None, "the newer session must stay connected"


# ---- Jobs end to end -------------------------------------------------------------

def test_jobs_run_to_their_real_outcome(coordinator, start_worker):
    start_worker("w1", slots=2)
    ok = submit(coordinator, python("pass"))
    bad = submit(coordinator, python("raise SystemExit(3)"))
    slow = submit(coordinator, python("import time; time.sleep(30)", max_runtime_s=1))

    job = wait_for_state(coordinator, ok, TERMINAL)
    assert job["state"] == "succeeded" and job["exit_code"] == 0
    [attempt] = job["attempts"]
    assert attempt["dispatch_latency_ns"] > 0 and attempt["start_latency_ns"] > 0

    job = wait_for_state(coordinator, bad, TERMINAL)
    assert job["state"] == "failed" and job["reason"] == "exit code 3"

    job = wait_for_state(coordinator, slow, TERMINAL)
    assert job["state"] == "failed" and job["reason"] == "max_runtime_s exceeded"
    assert scrape(coordinator, 'orch_jobs_finished_total{outcome="failed"}') == 2


def test_cancel_stops_the_running_job_and_frees_the_slot(coordinator, start_worker):
    start_worker("w1", slots=1)
    sleeper = submit(coordinator, python("import time; time.sleep(30)"))
    wait_for_state(coordinator, sleeper, {"running"})
    behind = submit(coordinator, python("pass"))  # QUEUED: the only slot is busy

    status, job = http(coordinator, "POST", f"/v1/jobs/{sleeper}/cancel")
    assert status == 200 and job["state"] == "cancelled"
    # The worker killed the sleeper and its slot went to the queued job.
    assert wait_for_state(coordinator, behind, TERMINAL)["state"] == "succeeded"
    assert scrape(coordinator, 'orch_stale_messages_total{type="finished"}') == 0
