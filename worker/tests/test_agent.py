"""The agent's side of the Runtime interface, with no coordinator connected."""

import asyncio

from orch_worker.agent import Agent, Config
from orch_worker.runtimes import Outcome


class RecordingRuntime:
    def __init__(self):
        self.calls = []

    async def run(self, job_id, attempt, spec, on_started):
        self.calls.append((job_id, attempt))
        await on_started(False)
        return Outcome(exit_code=0, run_ms=5)


def test_runtime_gets_the_attempt_and_the_result_carries_the_same_one():
    runtime = RecordingRuntime()
    config = Config(url="ws://unused", token="t", node_id="n", slots=1, capabilities={})
    agent = Agent(config, runtime)

    async def scenario():
        await agent._run_job(("job-1", 7), {"command": ["true"]})

    asyncio.run(scenario())
    assert runtime.calls == [("job-1", 7)]
    # With no connection the result is kept for replay after the next welcome.
    assert agent.unsent == [{"job_id": "job-1", "attempt": 7, "exit_code": 0, "run_ms": 5}]
