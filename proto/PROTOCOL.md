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
