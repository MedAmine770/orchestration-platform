"""What this node can run: detected from the host, optionally overridden by a
profile file to simulate hardware we don't have (DESIGN.md §4.2)."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psutil

from .protocol import validate_capabilities

MiB = 1024 * 1024
PROFILE_KEYS = {"cpus", "mem_mb", "accelerator", "gpu_name", "gpu_mem_mb", "runtimes"}


def load_profile(path: str | Path) -> dict:
    data = json.loads(Path(path).read_text())
    if type(data) is not dict:
        raise ValueError(f"{path}: profile must be a JSON object")
    unknown = set(data) - PROFILE_KEYS
    if unknown:
        raise ValueError(f"{path}: unknown profile keys {sorted(unknown)}")
    return data


def detect(profile: dict | None = None) -> dict:
    """Returns validated capabilities. Raises ValueError (ProtocolError) if a
    profile produces something the coordinator would reject."""
    caps = {
        "cpus": os.cpu_count() or 1,
        "mem_mb": psutil.virtual_memory().total // MiB,
        # Never detected. Falling back to `process` when Docker looks absent would
        # run jobs with no container at all, and from a worker image (which has
        # no docker CLI) inside a container that holds the Docker socket. A node
        # that wants native processes says so in its profile.
        "runtimes": ["docker"],
        **_accelerator(),
    }
    if profile:
        if "accelerator" in profile:
            # Detected GPU details don't describe a simulated accelerator.
            caps.pop("gpu_name", None)
            caps.pop("gpu_mem_mb", None)
        caps.update(profile)
    return validate_capabilities(caps)


def _accelerator() -> dict:
    try:
        import torch  # optional: only installed on nodes set up for GPU work
    except ImportError:
        return {"accelerator": "cpu"}
    if torch.cuda.is_available():
        props = torch.cuda.get_device_properties(0)
        return {"accelerator": "cuda", "gpu_name": props.name,
                "gpu_mem_mb": props.total_memory // MiB}
    if torch.backends.mps.is_available():
        # Apple GPUs share unified memory with the CPU, so there is no
        # separate GPU memory figure to report.
        return {"accelerator": "mps", "gpu_name": "Apple GPU (MPS)"}
    return {"accelerator": "cpu"}

