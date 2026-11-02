"""Connection loop and job execution.

hello -> welcome -> heartbeats, reconnecting on failure. Each assigned job runs
in its own task, so a long job never delays a heartbeat, and jobs keep running
across reconnects. A result that can't be sent (no connection, e.g. while the
coordinator restarts) is kept and sent after the next welcome, along with the
`started` of every job still running, in case the coordinator never got it.
"""

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
from .runtimes import Outcome, Runtime, make_runtime

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


async def run(config: Config, backoff: Backoff | None = None,
              runtime: Runtime | None = None) -> None:
    """Runs until cancelled or a FatalError. Jobs use the node's first
    advertised runtime unless one is given."""
    agent = Agent(config, runtime or make_runtime(config.capabilities["runtimes"][0]))
    try:
        await agent.run(backoff or Backoff())
    finally:
        await agent.stop_jobs()


JobKey = tuple[str, int]  # (job_id, attempt)


class Agent:
    def __init__(self, config: Config, runtime: Runtime):
        self.config = config
        self.runtime = runtime
        self.ws = None  # the current connection, if any
        self.jobs: dict[JobKey, asyncio.Task] = {}
        self.started: dict[JobKey, bool] = {}  # running jobs' image_pulled, for replay
        self.unsent: list[dict] = []  # `finished` bodies not yet delivered
        # The coordinator only assigns into free slots; if a race ever sends
        # one too many, the job waits here instead of being rejected (§5.3).
        self.slots = asyncio.Semaphore(config.slots)

    async def run(self, backoff: Backoff) -> None:
        while True:
            try:
                await self._session(backoff)
            except ConnectionClosed as e:
                code = e.rcvd.code if e.rcvd else None
                if code in FATAL_CLOSE_CODES:
                    why, exit_code = FATAL_CLOSE_CODES[code]
                    raise FatalError(f"{why} (close {code}: {e.rcvd.reason})",
                                     exit_code) from None
                log.warning("connection closed (code %s)", code)
            except (OSError, TimeoutError, InvalidHandshake, ProtocolError) as e:
                log.warning("coordinator unavailable: %s", e)
            finally:
                self.ws = None
            delay = backoff.next()
            log.info("reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)

    async def stop_jobs(self) -> None:
        """On shutdown: cancel every job, which kills its process/container."""
        for task in self.jobs.values():
            task.cancel()
        await asyncio.gather(*self.jobs.values(), return_exceptions=True)

    async def _session(self, backoff: Backoff) -> None:
        async with connect(self.config.url, max_size=MAX_FRAME_BYTES,
                           open_timeout=HANDSHAKE_TIMEOUT_S) as ws:
            await ws.send(encode("hello", {
                "token": self.config.token,
                "node_id": self.config.node_id,
                "agent_version": __version__,
                "capabilities": self.config.capabilities,
                "slots": self.config.slots,
                "running": [{"job_id": j, "attempt": a} for j, a in self.jobs],
            }))
            welcome = decode(await asyncio.wait_for(ws.recv(), HANDSHAKE_TIMEOUT_S))
            if welcome.type != "welcome":
                raise ProtocolError("invalid_value", f"expected welcome, got {welcome.type}")
            backoff.reset()
            self.ws = ws
            interval = welcome.body["heartbeat_interval_ms"] / 1000
            log.info("connected to %s as %s (heartbeat every %.1fs)",
                     self.config.url, self.config.node_id, interval)
            await self._replay()

            heartbeats = asyncio.create_task(self._heartbeats(interval))
            try:
                async for frame in ws:
                    await self._handle(decode(frame))
            finally:
                heartbeats.cancel()
                with contextlib.suppress(asyncio.CancelledError, ConnectionClosed):
                    await heartbeats

    async def _replay(self) -> None:
        """After a reconnect. A coordinator that kept this node's jobs (it
        restarted and was waiting for us) accepts these; one that already
        requeued them counts them as stale and answers with cancel."""
        for (job_id, attempt), image_pulled in list(self.started.items()):
            await self._send("started", {"job_id": job_id, "attempt": attempt,
                                         "image_pulled": image_pulled})
        unsent, self.unsent = self.unsent, []
        for body in unsent:
            log.info("job %s attempt %d: delivering its result after reconnecting",
                     body["job_id"], body["attempt"])
            await self._send("finished", body)

    async def _handle(self, message) -> None:
        if not message.known:
            log.info("ignoring unknown message type '%s'", message.type)
        elif message.type == "assign":
            await self._assign(message.body)
        elif message.type == "cancel":
            key = (message.body["job_id"], message.body["attempt"])
            if task := self.jobs.get(key):
                log.info("job %s attempt %d: cancelled by the coordinator", *key)
                task.cancel()
        else:
            log.warning("unexpected %s from the coordinator", message.type)

    async def _assign(self, body: dict) -> None:
        key = (body["job_id"], body["attempt"])
        if key in self.jobs:
            return  # a duplicate assign; the ack already went out
        await self._send("ack", {"job_id": key[0], "attempt": key[1]})
        self.jobs[key] = asyncio.create_task(self._run_job(key, body["spec"]))

    async def _run_job(self, key: JobKey, spec: dict) -> None:
        ref = {"job_id": key[0], "attempt": key[1]}

        async def on_started(image_pulled: bool) -> None:
            self.started[key] = image_pulled
            await self._send("started", {**ref, "image_pulled": image_pulled})

        try:
            async with self.slots:
                log.info("job %s attempt %d: running %s", key[0], key[1], spec["command"])
                try:
                    outcome = await self.runtime.run(key[0], spec, on_started)
                except asyncio.CancelledError:
                    raise  # cancelled: the coordinator already moved on, no result
                except Exception as e:  # a runtime bug must not kill the agent
                    log.exception("job %s attempt %d: runtime error", *key)
                    outcome = Outcome(error=f"worker runtime error: {e}")
            log.info("job %s attempt %d: %s", key[0], key[1],
                     f"exit {outcome.exit_code}" if outcome.error is None else outcome.error)
            result = {"exit_code": outcome.exit_code} if outcome.error is None \
                else {"error": outcome.error}
            await self._send("finished", {**ref, **result, "run_ms": outcome.run_ms})
        finally:
            self.jobs.pop(key, None)
            self.started.pop(key, None)

    async def _send(self, type_: str, body: dict) -> None:
        """Sends on the current connection. Without one, a `finished` is kept
        for _replay() and anything else is dropped (heartbeats are periodic,
        and the coordinator times out or reconciles the rest)."""
        ws = self.ws
        if ws is not None:
            try:
                await ws.send(encode(type_, body))
                return
            except ConnectionClosed:
                pass
        if type_ == "finished":
            self.unsent.append(body)
        elif type_ != "heartbeat":
            log.warning("not connected; dropping %s for job %s", type_, body.get("job_id"))

    async def _heartbeats(self, interval: float) -> None:
        psutil.cpu_percent(None)  # the first call only primes the counter
        while True:
            await self._send("heartbeat", {
                "running": len(self.jobs),
                "cpu_percent": psutil.cpu_percent(None),
                "mem_free_mb": psutil.virtual_memory().available // MiB,
            })
            await asyncio.sleep(interval)
