"""ProcessRuntime with real subprocesses. (DockerRuntime is exercised end to
end by deploy/smoke.sh, which runs a real container.)"""

import asyncio
import sys
import time

import pytest

from orch_worker.runtimes import ProcessRuntime


def spec(*command: str, max_runtime_s: int = 30) -> dict:
    return {"image": "unused", "command": list(command), "max_runtime_s": max_runtime_s}


def run(job_spec: dict):
    started = []

    async def on_started(image_pulled: bool) -> None:
        started.append(image_pulled)

    outcome = asyncio.run(ProcessRuntime().run("job", 1, job_spec, on_started))
    return outcome, started


def test_exit_code_is_reported_after_started():
    outcome, started = run(spec(sys.executable, "-c", "raise SystemExit(3)"))
    assert outcome.exit_code == 3 and outcome.error is None
    assert started == [False]  # never "pulled"


def test_missing_command_is_an_error_not_a_crash():
    outcome, started = run(spec("/no/such/binary"))
    assert outcome.error.startswith("cannot start command")
    assert started == []


def test_max_runtime_kills_the_process():
    t0 = time.monotonic()
    outcome, _ = run(spec(sys.executable, "-c", "import time; time.sleep(30)", max_runtime_s=1))
    assert outcome.error == "max_runtime_s exceeded"
    assert time.monotonic() - t0 < 5


def test_cancel_kills_the_process():
    async def scenario():
        started = asyncio.Event()

        async def on_started(_):
            started.set()

        task = asyncio.create_task(ProcessRuntime().run(
            "job", 1, spec(sys.executable, "-c", "import time; time.sleep(30)"), on_started))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    t0 = time.monotonic()
    asyncio.run(scenario())
    assert time.monotonic() - t0 < 5
