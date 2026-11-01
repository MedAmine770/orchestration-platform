"""Worker protocol v1 codec (proto/PROTOCOL.md).

Mirrors coordinator/src/protocol.cpp. The shared vectors in proto/vectors/ keep
the two in step: both test suites must pass them.

Each field rule is a small check function; message schemas are tables of
(field, check) pairs, so the rules read like the spec.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any, Callable

VERSION = 1

Check = Callable[[Any, str], Any]


class ProtocolError(ValueError):
    """A frame that breaks a rule. `code` is one of PROTOCOL.md's error codes."""

    def __init__(self, code: str, detail: str):
        super().__init__(f"{code}: {detail}")
        self.code = code


def _fail(code: str, detail: str):
    raise ProtocolError(code, detail)


# ---- Value rules ------------------------------------------------------------


def _uint(bits: int, minimum: int = 0) -> Check:
    limit = 2**bits - 1

    def check(v, path):
        # `type(v) is int` rejects bool: True is an int subclass in Python, and
        # JSON `true` must not pass as 1.
        if type(v) is not int or v < 0:
            _fail("wrong_type", f"{path} must be an unsigned integer")
        if v > limit:
            _fail("invalid_value", f"{path} out of range")
        if v < minimum:
            _fail("invalid_value", f"{path} must be >= {minimum}")
        return v

    return check


def _int32(v, path):
    if type(v) is not int:
        _fail("wrong_type", f"{path} must be an integer")
    if not -(2**31) <= v < 2**31:
        _fail("invalid_value", f"{path} out of range")
    return v


def _number(v, path):
    if type(v) not in (int, float):
        _fail("wrong_type", f"{path} must be a number")
    if not math.isfinite(v) or v < 0:
        _fail("invalid_value", f"{path} must be >= 0")
    return v


def _string(v, path):
    if type(v) is not str:
        _fail("wrong_type", f"{path} must be a string")
    return v


def _id(v, path):
    if not _string(v, path):
        _fail("invalid_value", f"{path} must not be empty")
    return v


def _bool(v, path):
    if type(v) is not bool:
        _fail("wrong_type", f"{path} must be a boolean")
    return v


def _enum(*allowed: str) -> Check:
    def check(v, path):
        if _string(v, path) not in allowed:
            _fail("invalid_value", f"{path} has unknown value '{v}'")
        return v

    return check


def _array(item: Check, non_empty: bool = False) -> Check:
    def check(v, path):
        if type(v) is not list:
            _fail("wrong_type", f"{path} must be an array")
        if non_empty and not v:
            _fail("invalid_value", f"{path} must not be empty")
        return [item(x, f"{path}[{i}]") for i, x in enumerate(v)]

    return check


@dataclass(frozen=True)
class _Optional:
    check: Check


def _object(fields: list) -> Check:
    return lambda v, path: _fields(v, path, fields)


def _fields(obj, path: str, fields: list) -> dict:
    """Validates `obj` against (name, check) pairs; unknown keys are dropped."""
    if type(obj) is not dict:
        _fail("not_object", f"{path or 'envelope'} must be an object")
    out = {}
    for name, check in fields:
        p = f"{path}.{name}" if path else name
        optional = isinstance(check, _Optional)
        if name not in obj:
            if optional:
                continue
            _fail("missing_field", f"{p} is required")
        out[name] = (check.check if optional else check)(obj[name], p)
    return out


# ---- Schemas ----------------------------------------------------------------

_ACCELERATOR = _enum("cpu", "cuda", "mps")

_JOB = [("job_id", _id), ("attempt", _uint(32, minimum=1))]

CAPABILITIES = [
    ("cpus", _uint(32, minimum=1)),
    ("mem_mb", _uint(64)),
    ("accelerator", _ACCELERATOR),
    ("gpu_name", _Optional(_string)),
    ("gpu_mem_mb", _Optional(_uint(64))),
    ("runtimes", _array(_enum("docker", "process"), non_empty=True)),
]

_SPEC = [
    ("image", _id),
    ("command", _array(_string, non_empty=True)),
    ("requirements", _object([("accelerator", _ACCELERATOR), ("min_gpu_mem_mb", _uint(64))])),
    ("max_runtime_s", _uint(32, minimum=1)),
]

SCHEMAS: dict[str, list] = {
    "hello": [
        ("token", _id),
        ("node_id", _id),
        ("agent_version", _string),
        ("capabilities", _object(CAPABILITIES)),
        ("slots", _uint(32, minimum=1)),
        ("running", _array(_object(_JOB))),
    ],
    "welcome": [("heartbeat_interval_ms", _uint(32, minimum=1))],
    "heartbeat": [("running", _uint(32)), ("cpu_percent", _number), ("mem_free_mb", _uint(64))],
    "assign": _JOB + [("spec", _object(_SPEC))],
    "ack": _JOB,
    "reject": _JOB + [("reason", _string)],
    "started": _JOB + [("image_pulled", _bool)],
    "finished": _JOB
    + [("exit_code", _Optional(_int32)), ("error", _Optional(_string)), ("run_ms", _uint(64))],
    "cancel": _JOB,
}


def _validate_body(type_: str, body) -> dict:
    out = _fields(body, "body", SCHEMAS[type_])
    if type_ == "finished" and ("exit_code" in out) == ("error" in out):
        _fail("invalid_value", "body must contain exactly one of exit_code, error")
    return out


def validate_capabilities(caps) -> dict:
    return _fields(caps, "capabilities", CAPABILITIES)


# ---- Codec ------------------------------------------------------------------


@dataclass(frozen=True)
class Message:
    type: str
    body: dict = field(default_factory=dict)  # validated; empty for unknown types
    known: bool = True


def _reject_constant(name: str):
    # Python's json accepts NaN/Infinity; JSON (and the C++ codec) does not.
    _fail("malformed_json", f"{name} is not valid JSON")


def decode(frame: str | bytes) -> Message:
    """Parses one text frame. Raises ProtocolError on any rule violation."""
    try:
        env = json.loads(frame, parse_constant=_reject_constant)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        _fail("malformed_json", str(e))
    if type(env) is not dict:
        _fail("not_object", "envelope must be an object")
    if "v" not in env:
        _fail("missing_field", "v is required")
    if type(env["v"]) is not int or env["v"] < 0:
        _fail("wrong_type", "v must be an unsigned integer")
    if env["v"] != VERSION:
        _fail("unsupported_version", "only protocol version 1 is supported")
    if "type" not in env:
        _fail("missing_field", "type is required")
    type_ = _string(env["type"], "type")
    if "body" not in env:
        _fail("missing_field", "body is required")
    if type(env["body"]) is not dict:
        _fail("not_object", "body must be an object")
    if type_ not in SCHEMAS:
        # Forward compatibility: newer coordinators may send types we don't know.
        return Message(type_, {}, known=False)
    return Message(type_, _validate_body(type_, env["body"]))


def encode(type_: str, body: dict) -> str:
    """Serialises a message, validating it first: we never send a frame the
    coordinator would reject."""
    if type_ not in SCHEMAS:
        raise ValueError(f"cannot encode unknown message type '{type_}'")
    clean = _validate_body(type_, body)
    return json.dumps({"v": VERSION, "type": type_, "body": clean}, separators=(",", ":"))
