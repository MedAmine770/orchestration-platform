# Worker protocol, version 1

The wire contract between the coordinator and workers (DESIGN.md §7.2). The
shared test vectors in [`vectors/`](vectors/) are normative: the C++ and Python
codecs must both pass them.

## Transport

- One WebSocket per worker at `ws://<coordinator>:8080/v1/workers`.
- Text frames only, one JSON object per frame, at most 64 KiB.

## Envelope

```json
{ "v": 1, "type": "heartbeat", "body": { "running": 0, "cpu_percent": 3.5, "mem_free_mb": 2048 } }
```

| Field | Rule |
|---|---|
| `v` | Unsigned integer, must be `1`, else `unsupported_version`. |
| `type` | String naming the message. An unrecognised type is **not** an error: the decoder reports it as *unknown* and the receiver logs and ignores it (forward compatibility). |
| `body` | Object. |

Unknown fields anywhere are ignored, so newer senders can add optional fields.

## Value rules

| Kind | Accepted | Otherwise |
|---|---|---|
| string | JSON string | `wrong_type` |
| id (`node_id`, `job_id`, `token`, `image`) | non-empty string | `invalid_value` if empty |
| uint | integer literal ≥ 0 (no fraction or exponent; not `true`/`false`) | `wrong_type`; above the field's range: `invalid_value` |
| int | integer literal (as above, may be negative) | `wrong_type` |
| number | any JSON number, finite, ≥ 0 | `wrong_type`; negative: `invalid_value` |
| bool | `true` / `false` | `wrong_type` |
| enum | one of the listed strings | `invalid_value` |

`attempt` is a uint32 that must be ≥ 1; counts and sizes are uint32 unless
marked uint64.

## Messages

`job` below means the two fields `job_id` (id) and `attempt` (uint32 ≥ 1),
written inline in the body.

| Type | Dir | Body |
|---|---|---|
| `hello` | W→C | `token` id, `node_id` id, `agent_version` string, `capabilities` object, `slots` uint32 ≥ 1, `running` array of `{job_id, attempt}` |
| `welcome` | C→W | `heartbeat_interval_ms` uint32 ≥ 1 |
| `heartbeat` | W→C | `running` uint32, `cpu_percent` number, `mem_free_mb` uint64 |
| `assign` | C→W | `job`, `spec` object |
| `ack` | W→C | `job` |
| `reject` | W→C | `job`, `reason` string |
| `started` | W→C | `job`, `image_pulled` bool |
| `finished` | W→C | `job`, exactly one of `exit_code` int32 / `error` string, `run_ms` uint64 |
| `cancel` | C→W | `job` |

`capabilities`: `cpus` uint32 ≥ 1, `mem_mb` uint64, `accelerator` enum
`cpu|cuda|mps`, optional `gpu_name` string, optional `gpu_mem_mb` uint64,
`runtimes` non-empty array of enum `docker|process`.

`spec`: `image` id, `command` non-empty array of strings, `requirements`
object (`accelerator` enum, `min_gpu_mem_mb` uint64), `max_runtime_s` uint32 ≥ 1.

## Errors

A message that fails these rules is rejected with one of the codes below, and
the connection is closed (DESIGN.md §7.3).

| Code | Meaning |
|---|---|
| `malformed_json` | Not valid JSON. |
| `not_object` | The envelope, `body`, or a nested object is not a JSON object. |
| `missing_field` | A required field is absent. |
| `wrong_type` | A field has the wrong JSON type. |
| `invalid_value` | Right type, value not allowed (empty id, out of range, unknown enum, both/neither of `exit_code`/`error`). |
| `unsupported_version` | `v` is not 1. |

The first failing rule wins; the test vectors use inputs with exactly one fault,
so both codecs report the same code regardless of check order.

Known divergence: an integer literal above 2⁶⁴ − 1 is rejected by both codecs,
but the C++ parser reads it as a float (`wrong_type`) while Python reads an
exact integer (`invalid_value`). No vector covers it; no valid sender produces it.

## Closing

The worker's first frame must be `hello`, within the coordinator's hello
timeout (default 5 s). The coordinator closes the WebSocket with:

| Code | When | Reason text |
|---|---|---|
| 1008 policy violation | bad token | `authentication failed` |
| 1008 | first frame isn't `hello` | `first message must be hello` |
| 1008 | no `hello` within the timeout | `hello timeout` |
| 1002 protocol error | a frame fails decoding | the error code, e.g. `wrong_type` |
| 1002 | a second `hello`, or a C→W type sent by the worker | e.g. `duplicate hello` |
| 1009 message too big | a frame over 64 KiB | (sent by the WebSocket layer) |
| 4000 replaced | another session authenticated with the same `node_id` | `replaced by a newer session` |

Close reasons are short ASCII strings; details stay in the coordinator's log.
