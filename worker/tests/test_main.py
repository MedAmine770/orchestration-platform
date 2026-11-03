"""Startup checks in the orch-worker command line, with no fakes."""

from orch_worker import __main__ as cli
from orch_worker.agent import EXIT_CONFIG


def test_no_profile_and_no_docker_exits_instead_of_running_unisolated(monkeypatch, caplog):
    monkeypatch.setenv("CLUSTER_TOKEN", "token")
    monkeypatch.setenv("DOCKER_HOST", "unix:///no/such/docker.sock")

    assert cli.main(["--coordinator", "ws://127.0.0.1:1/v1/workers"]) == EXIT_CONFIG
    assert "cannot use the docker runtime" in caplog.text
