"""--admin-port splits the public worker port from private REST (DESIGN.md §7.4)."""

import asyncio
import urllib.error
import urllib.request

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from conftest import CoordinatorProcess
from test_worker_connection import hello, recv_json


@pytest.fixture
def split(tmp_path):
    process = CoordinatorProcess(tmp_path, extra_args=("--admin-port", "0"))
    process.start()
    yield process
    process.stop()


def status(port: int, path: str, method: str = "GET", body: bytes | None = None) -> tuple[int, bytes]:
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method, data=body)
    try:
        with urllib.request.urlopen(request, timeout=5) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


JOB = b'{"image": "alpine:3.20", "command": ["true"]}'


@pytest.mark.parametrize("path", ["/v1/jobs", "/v1/nodes", "/metrics", "/v1/jobs/some-id"])
def test_worker_port_does_not_serve_rest_or_metrics(split, path):
    assert status(split.port, path)[0] == 404


def test_submitting_on_the_worker_port_creates_nothing(split):
    assert status(split.port, "/v1/jobs", "POST", JOB)[0] == 404
    assert status(split.admin_port, "/v1/jobs")[1].strip() == b'{"jobs":[]}'


def test_admin_port_serves_rest_and_metrics(split):
    assert status(split.admin_port, "/v1/jobs", "POST", JOB)[0] == 201
    assert status(split.admin_port, "/v1/nodes")[0] == 200
    assert b"orch_jobs" in status(split.admin_port, "/metrics")[1]


def test_healthz_is_on_both_ports(split):
    assert status(split.port, "/healthz")[0] == 200
    assert status(split.admin_port, "/healthz")[0] == 200


def test_worker_connects_on_the_worker_port_and_shows_on_the_admin_port(split):
    async def scenario():
        async with connect(split.ws_url) as ws:
            await ws.send(hello(split.token, "node-1"))
            assert (await recv_json(ws))["type"] == "welcome"
            return status(split.admin_port, "/v1/nodes")[1]

    assert b'"node_id":"node-1"' in asyncio.run(scenario())


def test_admin_port_refuses_worker_connections(split):
    async def scenario():
        async with connect(f"ws://127.0.0.1:{split.admin_port}/v1/workers"):
            pass

    with pytest.raises(InvalidStatus) as refused:
        asyncio.run(scenario())
    assert refused.value.response.status_code == 404


def test_without_the_flag_one_port_serves_everything(coordinator):
    assert coordinator.admin_port is None
    assert status(coordinator.port, "/v1/jobs")[0] == 200
    assert status(coordinator.port, "/metrics")[0] == 200
