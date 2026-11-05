"""Kill the coordinator mid-run and restart it on the same database (DESIGN.md
§8, milestone 7). Nothing acknowledged before the crash may be lost, running
jobs must not be run twice, and the cluster must carry on."""

import time

import pytest

from conftest import CoordinatorProcess
from test_job_lifecycle import http
from test_worker_agent import TERMINAL, python, submit, wait_for_state
from test_worker_connection import eventually, scrape

# Must stay above the worker's backoff cap (8 s), or a worker can legitimately
# miss the window and its job reruns (DESIGN.md §8.3).
WINDOW_MS = 9000


@pytest.fixture
def coordinator(tmp_path):
    """Overrides the shared fixture: a shorter recovery window keeps this fast."""
    process = CoordinatorProcess(tmp_path, ("--recovery-window-ms", str(WINDOW_MS)))
    process.start()
    yield process
    process.stop()


def job(coordinator, job_id: str) -> dict:
    return http(coordinator, "GET", f"/v1/jobs/{job_id}")[1]


def test_coordinator_crash_loses_nothing_and_reruns_nothing(coordinator, start_worker):
    start_worker("w1", slots=3)
    eventually(lambda: scrape(coordinator, 'orch_nodes{health="alive"}') == 1, timeout=10)

    long_job = submit(coordinator, python("import time; time.sleep(8)"))
    short_job = submit(coordinator, python("import time; time.sleep(1.5)"))
    gpu_job = submit(coordinator, {**python("pass"), "requirements": {"accelerator": "cuda"}})
    wait_for_state(coordinator, long_job, {"running"})
    wait_for_state(coordinator, short_job, {"running"})
    assert job(coordinator, gpu_job)["state"] == "unschedulable"

    coordinator.kill()  # SIGKILL: nothing is flushed on the way down
    time.sleep(2.5)     # short_job finishes while there is no coordinator
    coordinator.start()

    # Every job acknowledged before the crash is still there.
    _, listing = http(coordinator, "GET", "/v1/jobs")
    assert {j["job_id"] for j in listing["jobs"]} == {long_job, short_job, gpu_job}

    # The worker reconnects and reclaims long_job; short_job's result, which it
    # couldn't deliver, arrives after the reconnect. Neither runs again.
    for job_id in (long_job, short_job):
        final = wait_for_state(coordinator, job_id, TERMINAL, timeout=20)
        assert final["state"] == "succeeded", final
        assert final["attempt"] == 1 and final["retries_used"] == 0, final
        assert final["attempts"][0]["outcome"] == "succeeded"
    assert job(coordinator, gpu_job)["state"] == "unschedulable"

    # And the cluster carries on.
    after = submit(coordinator, python("pass"))
    assert wait_for_state(coordinator, after, TERMINAL)["state"] == "succeeded"


def test_job_whose_worker_never_returns_is_requeued_after_the_window(
        coordinator, start_worker):
    w1 = start_worker("w1")
    eventually(lambda: scrape(coordinator, 'orch_nodes{health="alive"}') == 1, timeout=10)
    job_id = submit(coordinator, python("import time; time.sleep(30)"))
    wait_for_state(coordinator, job_id, {"running"})

    coordinator.kill()
    w1.kill()  # the worker dies too while the coordinator is down
    w1.wait()
    coordinator.start()
    start_worker("w2")

    # Nobody reclaims the job within the window, so it moves to w2.
    eventually(lambda: job(coordinator, job_id)["attempt"] == 2, timeout=WINDOW_MS / 1000 + 10)
    rescued = job(coordinator, job_id)
    assert rescued["retries_used"] == 1
    assert (rescued["attempts"][0]["node"], rescued["attempts"][0]["outcome"]) == (
        "w1", "node_dead")
    assert rescued["attempts"][1]["node"] == "w2"
