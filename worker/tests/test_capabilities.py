import json
from pathlib import Path

import pytest

from orch_worker import capabilities
from orch_worker.capabilities import detect, load_profile
from orch_worker.protocol import ProtocolError

PROFILES = Path(__file__).resolve().parents[1] / "profiles"


def test_simulated_cuda_profile_overrides_detection():
    caps = detect(load_profile(PROFILES / "sim-cuda.json"))
    assert caps["accelerator"] == "cuda"
    assert caps["gpu_mem_mb"] == 24576
    assert caps["runtimes"] == ["docker"]
    assert caps["cpus"] >= 1  # still detected from the host


def test_profile_accelerator_drops_detected_gpu_details(monkeypatch):
    monkeypatch.setattr(capabilities, "_accelerator", lambda: {
        "accelerator": "cuda", "gpu_name": "real GPU", "gpu_mem_mb": 8192})
    caps = detect({"accelerator": "cpu"})
    assert caps["accelerator"] == "cpu"
    assert "gpu_name" not in caps and "gpu_mem_mb" not in caps


def test_unknown_profile_key_is_rejected(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"accelerator": "cuda", "gpus": 2}))
    with pytest.raises(ValueError, match="unknown profile keys"):
        load_profile(path)


def test_profile_values_are_validated_by_the_protocol_rules():
    with pytest.raises(ProtocolError) as error:
        detect({"accelerator": "tpu"})
    assert error.value.code == "invalid_value"
