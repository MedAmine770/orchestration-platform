"""The real worker process against the real coordinator: exit codes and
SIGTERM, which supervisors (compose.yaml) depend on."""

import os
import signal
import subprocess
import sys

from test_worker_connection import ALIVE, eventually, scrape

EXIT_REJECTED = 3
EXIT_REPLACED = 4


def start_worker(coordinator, node_id: str, token: str | None = None) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "orch_worker", "--coordinator", coordinator.ws_url,
         "--node-id", node_id],
        env={**os.environ, "CLUSTER_TOKEN": token or coordinator.token},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def test_sigterm_disconnects_cleanly_and_exits_zero(coordinator):
    worker = start_worker(coordinator, "w1")
    try:
        eventually(lambda: scrape(coordinator, ALIVE) == 1, timeout=10)
        worker.send_signal(signal.SIGTERM)
        assert worker.wait(timeout=10) == 0
        eventually(lambda: scrape(coordinator, ALIVE) == 0)
    finally:
        worker.kill()


def test_bad_token_exits_with_rejected_code(coordinator):
    worker = start_worker(coordinator, "w1", token="wrong-token")
    try:
        assert worker.wait(timeout=10) == EXIT_REJECTED
    finally:
        worker.kill()


def test_duplicate_node_id_exits_with_replaced_code(coordinator):
    first = start_worker(coordinator, "same-node")
    second = None
    try:
        eventually(lambda: scrape(coordinator, ALIVE) == 1, timeout=10)
        second = start_worker(coordinator, "same-node")
        assert first.wait(timeout=10) == EXIT_REPLACED
        assert second.poll() is None, "the newer session must stay connected"
    finally:
        first.kill()
        if second:
            second.kill()
