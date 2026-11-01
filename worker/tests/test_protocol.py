"""The Python codec against the shared vectors (proto/vectors/), which the C++
codec also passes."""

import json
from pathlib import Path

import pytest

from orch_worker.protocol import ProtocolError, decode, encode

VECTORS = Path(__file__).resolve().parents[2] / "proto" / "vectors"


def load(name: str) -> list:
    return json.loads((VECTORS / f"{name}.json").read_text())


def frame(vector: dict) -> str:
    return vector["raw"] if "raw" in vector else json.dumps(vector["message"])


def name(vector: dict) -> str:
    return vector["name"]


@pytest.mark.parametrize("vector", load("valid"), ids=name)
def test_valid_messages_round_trip(vector):
    message = decode(frame(vector))
    expected = vector.get("canonical", vector["message"])
    assert json.loads(encode(message.type, message.body)) == expected


@pytest.mark.parametrize("vector", load("invalid"), ids=name)
def test_invalid_messages_fail_with_the_expected_code(vector):
    with pytest.raises(ProtocolError) as error:
        decode(frame(vector))
    assert error.value.code == vector["error"], str(error.value)


@pytest.mark.parametrize("vector", load("unknown"), ids=name)
def test_unknown_types_are_reported_not_rejected(vector):
    message = decode(frame(vector))
    assert not message.known
    assert message.type == vector["type"]
