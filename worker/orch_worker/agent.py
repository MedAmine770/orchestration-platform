"""Connection loop: hello -> welcome -> heartbeats, reconnecting on failure."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass

import psutil
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from . import __version__
from .backoff import Backoff
from .capabilities import MiB
from .protocol import ProtocolError, decode, encode

log = logging.getLogger("orch_worker")

MAX_FRAME_BYTES = 64 * 1024  # proto/PROTOCOL.md "Transport"
HANDSHAKE_TIMEOUT_S = 10

# Process exit codes. A supervisor must not restart after EXIT_REJECTED or
# EXIT_REPLACED: the new process would fail the same way (compose.yaml).
EXIT_CONFIG = 2
EXIT_REJECTED = 3
EXIT_REPLACED = 4

# Close codes that retrying cannot fix (proto/PROTOCOL.md "Closing"), with the
# exit code each one ends the process with.
FATAL_CLOSE_CODES = {
    1008: ("rejected by the coordinator", EXIT_REJECTED),
    4000: ("another worker connected with the same node id", EXIT_REPLACED),
}


@dataclass(frozen=True)
class Config:
    url: str
    token: str
    node_id: str
    slots: int
    capabilities: dict


class FatalError(Exception):
    """The coordinator refused us in a way reconnecting won't fix."""

    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code


async def run(config: Config, backoff: Backoff | None = None) -> None:
    """Runs until cancelled or a FatalError."""
    backoff = backoff or Backoff()
    while True:
        try:
            await _session(config, backoff)
        except ConnectionClosed as e:
            code = e.rcvd.code if e.rcvd else None
            if code in FATAL_CLOSE_CODES:
                why, exit_code = FATAL_CLOSE_CODES[code]
                raise FatalError(f"{why} (close {code}: {e.rcvd.reason})", exit_code) from None
            log.warning("connection closed (code %s)", code)
        except (OSError, TimeoutError, InvalidHandshake, ProtocolError) as e:
            log.warning("coordinator unavailable: %s", e)
        delay = backoff.next()
        log.info("reconnecting in %.1fs", delay)
        await asyncio.sleep(delay)


async def _session(config: Config, backoff: Backoff) -> None:
    async with connect(config.url, max_size=MAX_FRAME_BYTES,
                       open_timeout=HANDSHAKE_TIMEOUT_S) as ws:
        await ws.send(encode("hello", {
            "token": config.token,
            "node_id": config.node_id,
            "agent_version": __version__,
            "capabilities": config.capabilities,
            "slots": config.slots,
            "running": [],  # job execution arrives in M5
        }))
        welcome = decode(await asyncio.wait_for(ws.recv(), HANDSHAKE_TIMEOUT_S))
        if welcome.type != "welcome":
            raise ProtocolError("invalid_value", f"expected welcome, got {welcome.type}")
        backoff.reset()
        interval = welcome.body["heartbeat_interval_ms"] / 1000
        log.info("connected to %s as %s (heartbeat every %.1fs)",
                 config.url, config.node_id, interval)

        heartbeats = asyncio.create_task(_heartbeats(ws, interval))
        try:
            async for frame in ws:
                message = decode(frame)
                if not message.known:
                    log.info("ignoring unknown message type '%s'", message.type)
                else:
                    log.info("received %s (job execution arrives in M5)", message.type)
        finally:
            heartbeats.cancel()
            with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
                await heartbeats


async def _heartbeats(ws, interval: float) -> None:
    psutil.cpu_percent(None)  # the first call only primes the counter
    while True:
        await ws.send(encode("heartbeat", {
            "running": 0,
            "cpu_percent": psutil.cpu_percent(None),
            "mem_free_mb": psutil.virtual_memory().available // MiB,
        }))
        await asyncio.sleep(interval)
