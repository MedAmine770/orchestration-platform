"""Jobs over the real network: REST in, assign out over the WebSocket, worker
messages back. The worker here is scripted message by message, so each test
controls exactly what the coordinator sees."""

import asyncio
import json
import urllib.error
import urllib.request

from websockets.asyncio.client import connect

from test_worker_connection import hello, recv_json, scrape

ECHO = {"image": "alpine:3.20", "command": ["echo", "hi"]}


def http(coordinator, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        coordinator.http_url + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def msg(type_: str, **body) -> str:
    return json.dumps({"v": 1, "type": type_, "body": body})


async def worker(coordinator, node_id: str):
    """Connects and authenticates; returns the open WebSocket."""
    ws = await connect(coordinator.ws_url)
    await ws.send(hello(coordinator.token, node_id))
    assert (await recv_json(ws))["type"] == "welcome"
    return ws


async def submit(coordinator, spec: dict = ECHO) -> str:
    # urllib blocks; run it off the event loop so the worker can keep reading.
    status, job = await asyncio.to_thread(http, coordinator, "POST", "/v1/jobs", spec)
    assert status == 201, job
    return job["job_id"]


def test_job_runs_to_success_with_latencies_recorded(coordinator):
    async def scenario():
        ws = await worker(coordinator, "w1")
        job_id = await submit(coordinator)

        assign = await recv_json(ws)
        assert assign["type"] == "assign"
        assert assign["body"]["job_id"] == job_id
        assert assign["body"]["attempt"] == 1
        assert assign["body"]["spec"]["command"] == ["echo", "hi"]

        ref = {"job_id": job_id, "attempt": 1}
        await ws.send(msg("ack", **ref))
        await ws.send(msg("started", **ref, image_pulled=False))
        await ws.send(msg("finished", **ref, exit_code=0, run_ms=5))
        await asyncio.sleep(0.2)
        await ws.close()
        return job_id

    job_id = asyncio.run(scenario())
    status, job = http(coordinator, "GET", f"/v1/jobs/{job_id}")
    assert status == 200
    assert job["state"] == "succeeded"
    assert job["exit_code"] == 0
    [attempt] = job["attempts"]
    assert attempt["node"] == "w1" and attempt["outcome"] == "succeeded"
    assert attempt["queue_wait_ns"] >= 0
    assert 0 < attempt["dispatch_latency_ns"] <= attempt["start_latency_ns"]
    assert attempt["image_pulled"] is False
    assert scrape(coordinator, "orch_dispatch_latency_seconds_count") == 1
    assert scrape(coordinator, 'orch_start_latency_seconds_count{image="cached"}') == 1
    assert scrape(coordinator, 'orch_jobs_finished_total{outcome="succeeded"}') == 1


def test_missing_ack_times_out_cancels_and_reassigns(coordinator):
    """The deadline timer fires on its own (T_ack is 300 ms in the fixture)."""
    async def scenario():
        ws = await worker(coordinator, "w1")
        job_id = await submit(coordinator)
        first = await recv_json(ws)
        assert first["type"] == "assign" and first["body"]["attempt"] == 1

        # No ack. After T_ack: cancel attempt 1, then the same node gets attempt 2.
        cancel = await recv_json(ws)
        assert cancel == {"v": 1, "type": "cancel", "body": {"job_id": job_id, "attempt": 1}}
        second = await recv_json(ws)
        assert second["type"] == "assign" and second["body"]["attempt"] == 2

        # The late ack for attempt 1 is fenced out and answered with cancel.
        await ws.send(msg("ack", job_id=job_id, attempt=1))
        stale_cancel = await recv_json(ws)
        assert stale_cancel["type"] == "cancel" and stale_cancel["body"]["attempt"] == 1
        # Read before disconnecting: closing the socket is itself a node loss.
        _, job = await asyncio.to_thread(http, coordinator, "GET", f"/v1/jobs/{job_id}")
        await ws.close()
        return job

    job = asyncio.run(scenario())
    assert job["retries_used"] == 1
    assert job["attempts"][0]["outcome"] == "ack_timeout"
    assert scrape(coordinator, 'orch_requeues_total{reason="ack_timeout"}') == 1
    assert scrape(coordinator, 'orch_stale_messages_total{type="ack"}') == 1


def test_disconnect_requeues_running_job_on_another_worker(coordinator):
    async def scenario():
        ws1 = await worker(coordinator, "w1")
        job_id = await submit(coordinator)
        assert (await recv_json(ws1))["type"] == "assign"
        ref = {"job_id": job_id, "attempt": 1}
        await ws1.send(msg("ack", **ref))
        await ws1.send(msg("started", **ref, image_pulled=True))
        await asyncio.sleep(0.1)

        ws2 = await worker(coordinator, "w2")  # least loaded, but w1 already has the job
        await ws1.close()                       # w1 goes away mid-job

        reassign = await recv_json(ws2)
        assert reassign["type"] == "assign"
        assert reassign["body"] == {**reassign["body"], "job_id": job_id, "attempt": 2}
        _, job = await asyncio.to_thread(http, coordinator, "GET", f"/v1/jobs/{job_id}")
        requeues = await asyncio.to_thread(
            scrape, coordinator, 'orch_requeues_total{reason="node_dead"}')
        await ws2.close()
        return job, requeues

    job, requeues = asyncio.run(scenario())
    assert job["retries_used"] == 1
    assert [a["outcome"] for a in job["attempts"]] == ["node_dead", None]
    assert requeues == 1


def test_rest_errors_are_json(coordinator):
    status, body = http(coordinator, "POST", "/v1/jobs", {"image": "a"})
    assert status == 400
    assert body == {"error": "command must be a non-empty array of strings"}
    status, body = http(coordinator, "GET", "/v1/jobs/does-not-exist")
    assert status == 404
