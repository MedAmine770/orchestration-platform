"""Kill a worker mid-job (DESIGN.md §10, milestone 6): the job must complete on
another worker, exactly one result must be accepted, and the failed worker's
late messages must be fenced out.

Two ways to lose a worker:
- crashed (SIGKILL): the kernel closes its socket, so the coordinator notices
  at once;
- hung (SIGSTOP): the socket stays open and only missing heartbeats reveal it
  (SUSPECT after 3 s, DEAD after 5 s, the coordinator's defaults).
"""

import signal
import time

from test_job_lifecycle import http
from test_worker_agent import TERMINAL, python, submit, wait_for_state
from test_worker_connection import eventually, scrape


def node_health(coordinator, node_id: str) -> str:
    _, body = http(coordinator, "GET", "/v1/nodes")
    return next(n["health"] for n in body["nodes"] if n["node_id"] == node_id)


def start_job_on_first_worker(coordinator, start_worker, seconds: float):
    """w1 runs the job; w2 joins afterwards with a free slot, ready to take over."""
    w1 = start_worker("w1")
    eventually(lambda: scrape(coordinator, 'orch_nodes{health="alive"}') == 1, timeout=10)
    job_id = submit(coordinator, python(f"import time; time.sleep({seconds})"))
    job = wait_for_state(coordinator, job_id, {"running"})
    assert job["node"] == "w1"
    w2 = start_worker("w2")
    eventually(lambda: scrape(coordinator, 'orch_nodes{health="alive"}') == 2, timeout=10)
    return w1, w2, job_id


def test_crashed_worker_job_completes_elsewhere(coordinator, start_worker):
    w1, _, job_id = start_job_on_first_worker(coordinator, start_worker, seconds=1)

    w1.send_signal(signal.SIGKILL)
    job = wait_for_state(coordinator, job_id, TERMINAL)

    assert job["state"] == "succeeded"
    assert [(a["node"], a["outcome"]) for a in job["attempts"]] == [
        ("w1", "node_dead"), ("w2", "succeeded")]
    assert job["retries_used"] == 1
    assert node_health(coordinator, "w1") == "dead"
    assert scrape(coordinator, 'orch_requeues_total{reason="node_dead"}') == 1
    # Detected by the socket closing, not by heartbeats.
    assert scrape(coordinator, "orch_heartbeats_missed_total") == 0


def test_hung_worker_is_detected_by_heartbeats_and_fenced_when_it_wakes(
        coordinator, start_worker):
    w1, _, job_id = start_job_on_first_worker(coordinator, start_worker, seconds=8)

    frozen_at = time.monotonic()
    w1.send_signal(signal.SIGSTOP)  # socket stays open; heartbeats stop
    eventually(lambda: node_health(coordinator, "w1") == "suspect", timeout=10)
    eventually(lambda: node_health(coordinator, "w1") == "dead", timeout=10)
    detected_after = time.monotonic() - frozen_at
    # Up to one heartbeat interval (1 s) may already have passed when w1 froze.
    assert 4.0 <= detected_after <= 7.0, detected_after
    assert scrape(coordinator, "orch_heartbeats_missed_total") == 1

    # Meanwhile attempt 2 went to w2. Its sleep takes 8 s, so attempt 1's
    # process is still running when w1 wakes up.
    eventually(lambda: http(coordinator, "GET", f"/v1/jobs/{job_id}")[1]["attempt"] == 2)
    w1.send_signal(signal.SIGCONT)

    # w1 finds its connection closed (4001), reconnects, reports attempt 1 as
    # still running, and is told to cancel it.
    eventually(lambda: node_health(coordinator, "w1") == "alive", timeout=15)
    job = wait_for_state(coordinator, job_id, TERMINAL, timeout=20)
    assert job["state"] == "succeeded"
    assert [(a["node"], a["outcome"]) for a in job["attempts"]] == [
        ("w1", "node_dead"), ("w2", "succeeded")]
    assert scrape(coordinator, 'orch_jobs_finished_total{outcome="succeeded"}') == 1
