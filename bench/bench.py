#!/usr/bin/env python3
"""Measures the running Compose stack and writes raw samples plus exact
percentiles to bench/results/ (method: bench/README.md, DESIGN.md §11).

    export CLUSTER_TOKEN=...; docker compose up --build -d
    python3 bench/bench.py

Standard library only. Latencies come from the coordinator itself (per-attempt
integer nanoseconds on its monotonic clock, GET /v1/jobs/{id}); failover times
are measured here, by polling the API.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "http://127.0.0.1:8080"
RESULTS = Path(__file__).resolve().parent / "results"
REPO = Path(__file__).resolve().parents[1]
CACHED_IMAGE = "alpine:3.20"
COLD_IMAGE = "busybox:1.37"
TERMINAL = {"succeeded", "failed", "lost", "cancelled"}
POLL_S = 0.01  # failover polling interval: the resolution of those numbers


# ---- API and Docker helpers --------------------------------------------------

def api(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def submit(image: str, command: list[str]) -> str:
    return api("POST", "/v1/jobs", {"image": image, "command": command})["job_id"]


def job(job_id: str) -> dict:
    return api("GET", f"/v1/jobs/{job_id}")


def wait_until(predicate, timeout: float, what: str):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out waiting for {what}")
        time.sleep(POLL_S)


def wait_terminal(job_id: str, timeout: float = 120) -> dict:
    return wait_until(lambda: (j := job(job_id))["state"] in TERMINAL and j,
                      timeout, f"job {job_id}")


def health() -> dict[str, str]:
    return {n["node_id"]: n["health"] for n in api("GET", "/v1/nodes")["nodes"]}


def total_free_slots() -> int:
    return sum(n["slots"] - n["running"] for n in api("GET", "/v1/nodes")["nodes"]
               if n["health"] == "alive")


def docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=REPO, check=True, capture_output=True)


def container_of(service: str) -> str:
    out = subprocess.run(["docker", "compose", "ps", "-q", service], cwd=REPO,
                         check=True, capture_output=True, text=True).stdout.strip()
    if not out:
        raise RuntimeError(f"no container for {service}")
    return out


# ---- Statistics ----------------------------------------------------------------

def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank: the smallest sample with at least p% of samples <= it.
    Always an observed value, never an interpolation."""
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def summarize(values_ms: list[float]) -> dict:
    if not values_ms:
        return {"n": 0}
    s = sorted(values_ms)
    return {"n": len(s), "p50": percentile(s, 50), "p95": percentile(s, 95),
            "p99": percentile(s, 99), "max": s[-1]}


def latency_samples(jobs: list[dict]) -> dict[str, list[float]]:
    """Milliseconds from the first attempt of each succeeded job."""
    out = {"queue_wait": [], "dispatch": [], "start_cached": [], "start_pulled": []}
    for j in jobs:
        if j["state"] != "succeeded" or len(j["attempts"]) != 1:
            raise RuntimeError(f"unexpected outcome for {j['job_id']}: {j['state']}, "
                               f"{len(j['attempts'])} attempts")
        a = j["attempts"][0]
        out["queue_wait"].append(a["queue_wait_ns"] / 1e6)
        out["dispatch"].append(a["dispatch_latency_ns"] / 1e6)
        key = "start_pulled" if a["image_pulled"] else "start_cached"
        out[key].append(a["start_latency_ns"] / 1e6)
    return out


# ---- Scenarios -----------------------------------------------------------------

def idle(n: int) -> dict:
    """One job at a time, each submitted only once the previous one finished,
    so every job finds a free slot: the scheduler's own path."""
    finished = []
    for _ in range(n):
        wait_until(lambda: total_free_slots() == 5, 30, "idle cluster")
        finished.append(wait_terminal(submit(CACHED_IMAGE, ["true"])))
    return {"jobs": n, "samples_ms": latency_samples(finished)}


def burst(n: int) -> dict:
    """n jobs submitted back to back onto 5 slots: the queue builds up."""
    wait_until(lambda: total_free_slots() == 5, 30, "idle cluster")
    t0 = time.monotonic()
    ids = [submit(CACHED_IMAGE, ["true"]) for _ in range(n)]
    submit_s = time.monotonic() - t0
    finished = [wait_terminal(i) for i in ids]
    return {"jobs": n, "submit_rate_per_s": round(n / submit_s, 1),
            "samples_ms": latency_samples(finished)}


def cold(n: int) -> dict:
    """Each job's image is removed first, so the worker must pull it. Pull time
    depends on the network and the registry, not on this system."""
    finished = []
    for _ in range(n):
        wait_until(lambda: total_free_slots() == 5, 30, "idle cluster")
        subprocess.run(["docker", "image", "rm", "-f", COLD_IMAGE], capture_output=True)
        finished.append(wait_terminal(submit(COLD_IMAGE, ["true"])))
    samples = latency_samples(finished)
    if len(samples["start_pulled"]) != n:
        raise RuntimeError("some cold jobs did not report a pull")
    return {"jobs": n, "samples_ms": samples}


def failover(kind: str, n: int) -> dict:
    """Runs a job, then crashes (`docker kill`: the socket closes) or hangs
    (`docker pause`: the socket stays open, only heartbeats stop) the worker
    running it. Times are from just before the docker command is issued."""
    detect, reassign, docker_cmd = [], [], []
    for _ in range(n):
        wait_until(lambda: set(health().values()) == {"alive"} and total_free_slots() == 5,
                   60, "all workers alive and idle")
        job_id = submit(CACHED_IMAGE, ["sleep", "3"])
        running = wait_until(lambda: (j := job(job_id))["state"] == "running" and j, 30,
                             "job running")
        node = running["node"]  # compose service name == node id
        container = container_of(node)

        t0 = time.monotonic()
        docker("kill" if kind == "crash" else "pause", container)
        docker_cmd.append((time.monotonic() - t0) * 1000)
        wait_until(lambda: health()[node] == "dead", 30, f"{node} declared dead")
        detect.append((time.monotonic() - t0) * 1000)
        wait_until(lambda: len(job(job_id)["attempts"]) >= 2, 30, "reassignment")
        reassign.append((time.monotonic() - t0) * 1000)

        final = wait_terminal(job_id)
        if final["state"] != "succeeded" or final["attempts"][-1]["node"] == node:
            raise RuntimeError(f"job {job_id} did not complete elsewhere: {final['state']}")

        if kind == "crash":
            compose("start", node)
        else:
            docker("unpause", container)
        wait_until(lambda: health().get(node) == "alive", 60, f"{node} back")
    return {"runs": n, "samples_ms": {"time_to_detect": detect, "time_to_reassign": reassign,
                                      "docker_command": docker_cmd}}


# ---- Report --------------------------------------------------------------------

def environment() -> dict:
    def sh(*cmd: str) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            return "unknown"
    return {
        "date_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "commit": sh("git", "-C", str(REPO), "rev-parse", "HEAD"),
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string") if sys.platform == "darwin"
        else platform.processor(),
        "os": platform.platform(),
        "docker_server": sh("docker", "version", "--format", "{{.Server.Version}}"),
        "docker_vm": sh("docker", "info", "--format", "{{.NCPU}} CPUs, {{.MemTotal}} bytes"),
        "cluster": "Compose: coordinator (RelWithDebInfo) + 3 workers, 5 slots in total",
    }


def markdown(report: dict) -> str:
    env, s = report["environment"], report["summary_ms"]

    def row(name: str, d: dict) -> str:
        if d["n"] == 0:
            return f"| {name} | 0 | – | – | – | – |"
        return (f"| {name} | {d['n']} | {d['p50']:.2f} | {d['p95']:.2f} | "
                f"{d['p99']:.2f} | {d['max']:.2f} |")

    lines = [
        f"# Benchmark results ({env['date_utc']})", "",
        f"Commit `{env['commit'][:12]}` · {env['cpu']} · {env['os']} · "
        f"Docker {env['docker_server']} ({env['docker_vm']}) · {env['cluster']}.", "",
        "Exact nearest-rank percentiles from raw samples, in **milliseconds**. With fewer "
        "than 100 samples, p99 is the maximum. Method: [../README.md](../README.md).", "",
    ]
    for scenario, title in [("idle", "Idle (one job at a time)"),
                            ("burst", f"Burst ({report['scenarios']['burst']['jobs']} jobs "
                                      f"submitted at {report['scenarios']['burst']['submit_rate_per_s']}/s "
                                      "onto 5 slots)"),
                            ("cold", "Cold start (image removed before each job)")]:
        lines += [f"## {title}", "", "| Metric | n | p50 | p95 | p99 | max |",
                  "|---|---|---|---|---|---|"]
        names = {"queue_wait": "Queue wait", "dispatch": "Dispatch",
                 "start_cached": "Start, cached image", "start_pulled": "Start, pulled image"}
        for key, name in names.items():
            if s[scenario][key]["n"]:
                lines.append(row(name, s[scenario][key]))
        lines.append("")
    for scenario, title in [("crash", "Worker crash (`docker kill`: socket closes)"),
                            ("hang", "Worker hang (`docker pause`: only heartbeats stop)")]:
        lines += [f"## {title}", "", "| Metric | n | p50 | p95 | p99 | max |",
                  "|---|---|---|---|---|---|",
                  row("Time to detect (node `dead`)", s[scenario]["time_to_detect"]),
                  row("Time to reassign (attempt 2 sent)", s[scenario]["time_to_reassign"]),
                  row("(docker command itself)", s[scenario]["docker_command"]), ""]
    lines += ["Failover times are measured by this script from just before the docker command, "
              f"polling the API every {POLL_S * 1000:.0f} ms, so they include the docker CLI's own "
              "latency (last row) and up to one polling interval. Every failover job completed "
              "on another worker."]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--idle", type=int, default=50)
    parser.add_argument("--burst", type=int, default=100)
    parser.add_argument("--cold", type=int, default=10)
    parser.add_argument("--failover", type=int, default=5)
    args = parser.parse_args()

    try:
        if set(health().values()) != {"alive"} or len(health()) != 3:
            print("bench: expected 3 alive workers; is `docker compose up` running?")
            return 2
    except OSError as e:
        print(f"bench: coordinator not reachable at {API}: {e}")
        return 2

    print("warming up (pulls the cached image once)...", flush=True)
    wait_terminal(submit(CACHED_IMAGE, ["true"]))

    scenarios = {}
    for name, run in [("idle", lambda: idle(args.idle)), ("burst", lambda: burst(args.burst)),
                      ("cold", lambda: cold(args.cold)),
                      ("crash", lambda: failover("crash", args.failover)),
                      ("hang", lambda: failover("hang", args.failover))]:
        print(f"running {name}...", flush=True)
        scenarios[name] = run()
    # Failover can leave a crashed worker's job container behind (see README).
    leftovers = subprocess.run(["docker", "ps", "-aq", "--filter", "label=orch.job_id"],
                               capture_output=True, text=True).stdout.split()
    if leftovers:
        subprocess.run(["docker", "rm", "-f", *leftovers], capture_output=True)

    report = {
        "environment": environment(),
        "method": {"percentile": "nearest-rank on raw samples", "unit": "ms",
                   "latency_source": "coordinator per-attempt ns (GET /v1/jobs/{id})",
                   "failover_poll_interval_ms": POLL_S * 1000},
        "scenarios": scenarios,
        "summary_ms": {name: {k: summarize(v) for k, v in sc["samples_ms"].items()}
                       for name, sc in scenarios.items()},
        "orphaned_job_containers_removed": len(leftovers),
    }
    RESULTS.mkdir(exist_ok=True)
    stem = f"{datetime.now(timezone.utc):%Y-%m-%d}-{report['environment']['commit'][:7]}"
    (RESULTS / f"{stem}.json").write_text(json.dumps(report, indent=1) + "\n")
    (RESULTS / "latest.md").write_text(markdown(report))
    print(f"wrote bench/results/{stem}.json and bench/results/latest.md")
    print(markdown(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
