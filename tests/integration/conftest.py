"""Starts the real coordinator binary for integration tests.

The binary defaults to the debug-asan build, so a memory error triggered by a
test makes the process exit non-zero, which the fixture turns into a failure.
Override with ORCH_COORDINATOR_BIN.
"""

import os
import re
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
DEFAULT_BIN = REPO / "coordinator" / "build" / "debug-asan" / "coordinator"


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
            [str(binary), "--bind", "127.0.0.1", "--port", "0", "--hello-timeout-ms", "300"],
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
