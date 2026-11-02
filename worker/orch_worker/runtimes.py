"""Job runtimes (DESIGN.md §4.2): `docker` runs the job's image as a sibling
container, `process` runs the command as a native subprocess (needed for MPS,
which Docker on macOS cannot expose).

A runtime's run() calls on_started(image_pulled) once the work is actually
running, enforces max_runtime_s, and kills the work if its task is cancelled.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

log = logging.getLogger("orch_worker")

OnStarted = Callable[[bool], Awaitable[None]]


@dataclass(frozen=True)
class Outcome:
    """Exactly one of exit_code / error is set, as in the `finished` message."""
    exit_code: int | None = None
    error: str | None = None
    run_ms: int = 0


class Runtime(Protocol):
    async def run(self, job_id: str, spec: dict, on_started: OnStarted) -> Outcome: ...


def _elapsed_ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


class ProcessRuntime:
    async def run(self, job_id: str, spec: dict, on_started: OnStarted) -> Outcome:
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec["command"],
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        except OSError as e:
            return Outcome(error=f"cannot start command: {e}")
        start = time.monotonic()
        try:
            await on_started(False)  # nothing to pull
            code = await asyncio.wait_for(proc.wait(), spec["max_runtime_s"])
        except TimeoutError:
            await _kill(proc)
            return Outcome(error="max_runtime_s exceeded", run_ms=_elapsed_ms(start))
        except asyncio.CancelledError:
            await _kill(proc)
            raise
        return Outcome(exit_code=code, run_ms=_elapsed_ms(start))


async def _kill(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is None:
        proc.kill()
        await proc.wait()


class DockerRuntime:
    """Uses the Docker SDK. Its calls block, so each runs in a thread."""

    def __init__(self):
        import docker  # only needed on nodes that use this runtime
        self._errors = docker.errors
        self._client = docker.from_env()

    async def run(self, job_id: str, spec: dict, on_started: OnStarted) -> Outcome:
        image = spec["image"]
        try:
            pulled = await asyncio.to_thread(self._ensure_image, image)
            container = await asyncio.to_thread(
                self._client.containers.run, image, spec["command"], detach=True,
                labels={"orch.job_id": job_id})
        except self._errors.NotFound:  # includes ImageNotFound
            return Outcome(error=f"image not found: {image}")
        except self._errors.APIError as e:
            return Outcome(error=f"docker: {e.explanation or e}")

        start = time.monotonic()
        try:
            await on_started(pulled)
            result = await asyncio.wait_for(
                asyncio.to_thread(container.wait), spec["max_runtime_s"])
            output = await asyncio.to_thread(container.logs, tail=5)
            log.info("job %s output: %s", job_id, output.decode(errors="replace").strip())
            return Outcome(exit_code=result["StatusCode"], run_ms=_elapsed_ms(start))
        except TimeoutError:
            return Outcome(error="max_runtime_s exceeded", run_ms=_elapsed_ms(start))
        finally:
            # Also on cancel: force-remove kills the container if it still runs.
            await asyncio.shield(asyncio.to_thread(container.remove, force=True))

    def _ensure_image(self, image: str) -> bool:
        """Returns True if the image had to be pulled (a cold start)."""
        try:
            self._client.images.get(image)
            return False
        except self._errors.ImageNotFound:
            self._client.images.pull(image)  # raises NotFound if it doesn't exist
            return True


def make_runtime(name: str) -> Runtime:
    return DockerRuntime() if name == "docker" else ProcessRuntime()
