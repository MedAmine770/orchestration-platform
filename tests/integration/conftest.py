"""Starts the real coordinator binary for integration tests.

The binary defaults to the debug-asan build, so a memory error triggered by a
test makes the process exit non-zero, which the fixture turns into a failure.
Override with ORCH_COORDINATOR_BIN.
"""

import json
import os
import re
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DEFAULT_BIN = REPO / "coordinator" / "build" / "debug-asan" / "coordinator"


def pytest_collection_modifyitems(items):
    """A hung integration test fails after 60 s with a traceback instead of
    stalling the run (pytest-timeout)."""
    here = Path(__file__).parent
    for item in items:
        if item.path.is_relative_to(here):
            item.add_marker(pytest.mark.timeout(60))


@dataclass(frozen=True)
class Coordinator:
    port: int
    token: str

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/v1/workers"

    @property
    def http_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"


@pytest.fixture
def coordinator(tmp_path):
    binary = Path(os.environ.get("ORCH_COORDINATOR_BIN", DEFAULT_BIN))
    token = "integration-test-token"
    log_path = tmp_path / "coordinator.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [str(binary), "--bind", "127.0.0.1", "--port", "0", "--hello-timeout-ms", "300",
             "--ack-timeout-ms", "300"],
            env={**os.environ, "CLUSTER_TOKEN": token},
            stdout=subprocess.PIPE,
            stderr=log,  # a file, not a pipe, so a chatty coordinator can never block
            text=True,
        )
    # The first stdout line is "listening on 127.0.0.1:<port>" (EOF if it crashed).
    match = re.search(r":(\d+)$", proc.stdout.readline().strip())
    if not match:
        proc.kill()
        proc.wait()
        pytest.fail(f"coordinator did not start:\n{log_path.read_text()}")

    yield Coordinator(port=int(match.group(1)), token=token)

    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        pytest.fail(f"coordinator ignored SIGTERM:\n{log_path.read_text()}")
    assert proc.returncode == 0, (
        f"coordinator exited with {proc.returncode}:\n{log_path.read_text()}"
    )


@pytest.fixture
def process_profile(tmp_path) -> Path:
    """A CPU node whose jobs run as native subprocesses, so tests need no Docker."""
    path = tmp_path / "process.json"
    path.write_text(json.dumps({"accelerator": "cpu", "runtimes": ["process"]}))
    return path


@pytest.fixture
def start_worker(coordinator, process_profile, tmp_path):
    """Starts real worker processes (logs in tmp_path); kills them afterwards."""
    started = []

    def start(node_id: str, token: str | None = None, slots: int = 1) -> subprocess.Popen:
        log = open(tmp_path / f"{node_id}-{len(started)}.log", "w")
        proc = subprocess.Popen(
            [sys.executable, "-m", "orch_worker", "--coordinator", coordinator.ws_url,
             "--node-id", node_id, "--slots", str(slots), "--profile", str(process_profile)],
            env={**os.environ, "CLUSTER_TOKEN": token or coordinator.token},
            stdout=log, stderr=log)
        started.append(proc)
        return proc

    yield start
    for proc in started:
        proc.kill()  # SIGKILL also ends a SIGSTOPped process
        proc.wait()
