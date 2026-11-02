# syntax=docker/dockerfile:1
FROM python:3.12.11-slim-bookworm

WORKDIR /app
COPY pyproject.toml ./
COPY orch_worker ./orch_worker
RUN pip install --no-cache-dir . && useradd --system --uid 10001 worker
COPY profiles ./profiles

# No Docker socket yet: jobs start running in milestone 5, and the socket
# (root-equivalent on the host) is mounted only then.
USER worker
# Exec form, so the agent is PID 1 and receives docker stop's SIGTERM itself.
ENTRYPOINT ["orch-worker"]
