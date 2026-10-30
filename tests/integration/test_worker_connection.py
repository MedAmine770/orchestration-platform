"""Worker connection lifecycle against the real coordinator (DESIGN.md §7.3)."""

import asyncio
import json
import time
import urllib.request

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

POLICY_VIOLATION = 1008
REPLACED = 4000


def hello(token: str, node_id: str = "node-1") -> str:
    return json.dumps({"v": 1, "type": "hello", "body": {
        "token": token, "node_id": node_id, "agent_version": "test",
        "capabilities": {"cpus": 1, "mem_mb": 1024, "accelerator": "cpu", "runtimes": ["docker"]},
        "slots": 1, "running": [],
    }})


async def recv_json(ws) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=5))


async def expect_closed(ws, code: int, timeout: float = 5) -> str:
    """Waits for the coordinator to close the socket; returns the close reason."""
    with pytest.raises(ConnectionClosed) as closed:
        await asyncio.wait_for(ws.recv(), timeout)
    assert closed.value.rcvd is not None, "connection dropped without a close frame"
    assert closed.value.rcvd.code == code
    return closed.value.rcvd.reason


def scrape(coordinator, series: str) -> int:
    with urllib.request.urlopen(f"{coordinator.http_url}/metrics", timeout=5) as r:
        for line in r.read().decode().splitlines():
            if line.startswith(series + " "):
                return int(line.rsplit(" ", 1)[1])
    raise AssertionError(f"{series} not exported")


def eventually(predicate, timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.05)


ALIVE = 'orch_nodes{health="alive"}'


def test_valid_token_gets_welcome(coordinator):
    async def scenario():
        async with connect(coordinator.ws_url) as ws:
            await ws.send(hello(coordinator.token))
            msg = await recv_json(ws)
            assert msg == {"v": 1, "type": "welcome", "body": {"heartbeat_interval_ms": 1000}}

    asyncio.run(scenario())


def test_bad_token_is_closed_with_policy_violation(coordinator):
    async def scenario():
        async with connect(coordinator.ws_url) as ws:
            await ws.send(hello("wrong-token"))
            assert await expect_closed(ws, POLICY_VIOLATION) == "authentication failed"

    asyncio.run(scenario())
    assert scrape(coordinator, "orch_auth_failures_total") == 1


def test_first_message_must_be_hello(coordinator):
    async def scenario():
        async with connect(coordinator.ws_url) as ws:
            await ws.send(json.dumps({"v": 1, "type": "heartbeat", "body": {
                "running": 0, "cpu_percent": 0, "mem_free_mb": 1}}))
            assert await expect_closed(ws, POLICY_VIOLATION) == "first message must be hello"

    asyncio.run(scenario())


def test_silent_connection_is_closed_after_hello_timeout(coordinator):
    async def scenario():
        async with connect(coordinator.ws_url) as ws:
            started = time.monotonic()
            assert await expect_closed(ws, POLICY_VIOLATION) == "hello timeout"
            # Started with --hello-timeout-ms 300.
            assert 0.25 < time.monotonic() - started < 3

    asyncio.run(scenario())


def test_same_node_id_replaces_the_old_session(coordinator):
    async def scenario():
        async with connect(coordinator.ws_url) as old:
            await old.send(hello(coordinator.token, "node-a"))
            assert (await recv_json(old))["type"] == "welcome"

            async with connect(coordinator.ws_url) as new:
                await new.send(hello(coordinator.token, "node-a"))
                assert (await recv_json(new))["type"] == "welcome"
                await expect_closed(old, REPLACED)
                # The old session closing must not evict its replacement.
                eventually(lambda: scrape(coordinator, ALIVE) == 1)
                await new.ping()  # still connected

    asyncio.run(scenario())


def test_metrics_track_connected_nodes(coordinator):
    assert scrape(coordinator, ALIVE) == 0

    async def scenario():
        async with connect(coordinator.ws_url) as a, connect(coordinator.ws_url) as b:
            await a.send(hello(coordinator.token, "node-a"))
            await b.send(hello(coordinator.token, "node-b"))
            await recv_json(a)
            await recv_json(b)
            eventually(lambda: scrape(coordinator, ALIVE) == 2)

    asyncio.run(scenario())
    eventually(lambda: scrape(coordinator, ALIVE) == 0)
