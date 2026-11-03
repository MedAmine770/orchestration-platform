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


class CoordinatorProcess:
    """The real coordinator binary with a SQLite database in tmp_path. It can
    be killed and started again on the same port and database, as a real
    restart would be."""

    def __init__(self, tmp_path: Path, extra_args: tuple[str, ...] = ()):
        self.binary = Path(os.environ.get("ORCH_COORDINATOR_BIN", DEFAULT_BIN))
        self.token = "integration-test-token"
        self.db = tmp_path / "orch.db"
        self.log_path = tmp_path / "coordinator.log"
        self.extra_args = extra_args
        self.port = 0  # chosen by the OS on the first start, then kept
        self.admin_port: int | None = None  # with --admin-port in extra_args
        self.proc: subprocess.Popen | None = None

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/v1/workers"

    @property
    def http_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        with open(self.log_path, "a") as log:
            self.proc = subprocess.Popen(
                [str(self.binary), "--bind", "127.0.0.1", "--port", str(self.port),
                 "--hello-timeout-ms", "300", "--ack-timeout-ms", "300",
                 "--db", str(self.db), *self.extra_args],
                env={**os.environ, "CLUSTER_TOKEN": self.token},
                stdout=subprocess.PIPE,
                stderr=log,  # a file, not a pipe, so a chatty coordinator can never block
                text=True,
            )
        # The first stdout line is "listening on 127.0.0.1:<port>" (EOF if it crashed).
        match = re.search(r":(\d+)$", self.proc.stdout.readline().strip())
        if not match:
            self.proc.kill()
            self.proc.wait()
            pytest.fail(f"coordinator did not start:\n{self.log_path.read_text()}")
        self.port = int(match.group(1))
        if "--admin-port" in self.extra_args:
            self.admin_port = int(re.search(r":(\d+)$", self.proc.stdout.readline().strip()).group(1))

    def kill(self) -> None:
        """SIGKILL: no shutdown code runs, as in a crash or power loss."""
        self.proc.kill()
        self.proc.wait()

    def stop(self) -> None:
        """SIGTERM, which must exit 0: under ASan, a memory error makes it fail."""
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            pytest.fail(f"coordinator ignored SIGTERM:\n{self.log_path.read_text()}")
        assert self.proc.returncode == 0, (
            f"coordinator exited with {self.proc.returncode}:\n{self.log_path.read_text()}"
        )


@pytest.fixture
def coordinator(tmp_path):
    process = CoordinatorProcess(tmp_path)
    process.start()
    yield process
    process.stop()


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
