# BigQuery Storage Write Adapter

Python adapter for the
[BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).

It accepts dictionary rows, serializes them into dynamic protobuf messages,
splits payloads into API-safe chunks, writes to a dedicated stream with strict
offset handling, and returns typed outcomes that callers can act on.

This repository contains the destination adapter slice extracted from a larger
ports-and-adapters pipeline (see [Planned expanded project](#planned-expanded-project)).

---

## Introduction

The Storage Write API gives better throughput and stronger delivery guarantees
than `insertAll`, but it also requires more client-side control:

1. Serialize dict rows to protobuf that matches the live table schema.
2. Keep each `AppendRowsRequest` under the 10 MB API limit.
3. Maintain stream offsets and handle status codes correctly.
4. Separate retryable, fatal, and row-level failures.
5. Manage `PENDING` stream lifecycle (`finalize` then `batchCommit`).

This module wraps that complexity behind the
[`Destination`](ports/destination.py) port:

- `write(rows) -> stats`
- `close()`

---

## Architecture

The flow is split into three short diagrams.  
Solid arrows are the main path. Dashed arrows are side channels (errors, retries, stats).

### Diagram 1: Serialization

```text
                    ┌──────────────────────────────────────────────┐
                    │  Input batch                                 │
Caller `write(rows)`│  Sequence[dict]                              │
───────────────────►│  full batch from upstream                    │
                    └──────────────────────┬───────────────────────┘
                                           │ serialize_batch(rows, schema)
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  BQProtoRowSerializer                        │
                    │  returns: good_rows + bad_rows               │
                    └───────────────┬──────────────────────┬───────┘
                                    │                      │
                                    │ good_rows            │ bad_rows (RowSerializationError)
                                    ▼                      ▼
                    ┌──────────────────────────┐   ┌──────────────────────────┐
                    │  plan_append_chunks      │   │ DestinationWriteStats    │
                    │  (size + row budgets)    │   │ serializer_row_failures  │
                    └──────────────┬───────────┘   └──────────────────────────┘
                                   │
                                   │ list[AppendChunk]
                                   ▼
                          ┌──────────────────────────┐
                          │  Append loop input       │
                          │  (Diagram 2 starts here) │
                          └──────────────────────────┘
```

**Steps**

1. **Receive rows.** `write(rows)` gets a full `Sequence[dict]` batch.
2. **Serialize once.** `BQProtoRowSerializer.serialize_batch` returns good row bytes and bad row diagnostics.
3. **Plan chunks.** `plan_append_chunks` packs only good rows into API-safe `AppendChunk` objects.
4. **Store bad rows.** Serialization failures are saved in `DestinationWriteStats.serializer_row_failures`.

### Diagram 2: Append request loop

```text
                    ┌──────────────────────────────────────────────┐
                    │  Input chunks                                │
Diagram 1 output ──►│  list[AppendChunk]                           │
                    │  processed one chunk at a time               │
                    └──────────────────────┬───────────────────────┘
                                           │ for each chunk
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  _append_chunk                               │
                    │  build AppendRowsRequest                     │
                    │  offset = session.next_offset                │
                    └──────────────────────┬───────────────────────┘
                                           │ send over gRPC stream
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  BigQuery AppendRowsResponse                 │
                    └──────────────────────┬───────────────────────┘
                                           │ classify(response / exception)
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  ErrorPolicy.classify                        │
                    │  -> ErrorCategory + policy flags             │
                    └───────────────┬──────────────────────┬───────┘
                                    │                      │
                                    │ row_errors           │ normal / retry / terminal path
                                    ▼                      ▼
                    ┌──────────────────────────┐   ┌──────────────────────────┐
                    │  row_errors_mapping      │   │ DestinationWriteStats    │
                    │  chunk idx -> input row  │   │ counts, error, skips     │
                    └──────────────┬───────────┘   └──────────────────────────┘
                                   │
                                   │ row_error_bad_rows / row_error_good_rows
                                   ▼
                          ┌──────────────────────────┐
                          │  Caller recovery split   │
                          │  DLQ bad + retry good    │
                          └──────────────────────────┘
```

**Steps**

1. **Iterate chunks.** The destination processes `list[AppendChunk]` one chunk at a time.
2. **Send request.** `_append_chunk` builds one `AppendRowsRequest` with `offset = session.next_offset`.
3. **Advance offset safely.** Offset changes only after server ack (or `ALREADY_EXISTS`).
4. **Classify response.** `ErrorPolicy.classify` maps gRPC result to `ErrorCategory` and policy flags.
5. **Map row errors.** `row_errors_mapping` converts chunk-local row indexes back to original input dicts.
6. **Accumulate stats.** Successes, skips, and failures are aggregated in `DestinationWriteStats`.

### Diagram 3: Retries and close lifecycle

```text
                    ┌──────────────────────────────────────────────┐
                    │  Caller                                      │
                    │  uses Destination port                       │
                    └──────────────────────┬───────────────────────┘
                                           │ write(rows)
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  BigQueryWriteRetryOrchestrator              │
                    │  same Destination interface                  │
                    └──────────────────────┬───────────────────────┘
                                           │ delegate write
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  BigQueryStorageWriteDestination             │
                    │  runs Diagram 1 + Diagram 2                  │
                    └──────────────────────┬───────────────────────┘
                                           │ returns DestinationWriteStats
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │  Retry decision                              │
                    │  retryable? -> backoff + same batch retry    │
                    │  else -> finish                              │
                    └───────────────┬──────────────────────┬───────┘
                                    │                      │
                                    │ close()              │ final result
                                    ▼                      ▼
                    ┌──────────────────────────┐   ┌──────────────────────────┐
                    │ Stream lifecycle         │   │ RetryOrchestratorStats   │
                    │ COMMITTED: release only  │   │ destination stats + retry│
                    │ PENDING: finalize+commit │   │ counters + last error    │
                    └──────────────────────────┘   └──────────────────────────┘
```

**Steps**

1. **Wrap destination.** `BigQueryWriteRetryOrchestrator` implements the same `Destination` port.
2. **Delegate write.** It calls destination `write()` and inspects returned stats.
3. **Retry by category.** Retryable categories use configured attempts and backoff.
4. **Guard loop.** A loop-safety cap prevents infinite retry cycles.
5. **Close stream correctly.**
   - `COMMITTED`: release resources only.
   - `PENDING`: finalize and commit only after `mark_job_succeeded()` and at least one append.
6. **Return richer stats.** `RetryOrchestratorStats` adds retry counters and last error metadata.

---

## Package layout

```
adapters/bigquery/storage_write/
├── bq_storage_write_destination.py   # Destination adapter (append loop, lifecycle)
├── bq_storage_write_models.py        # StorageWriteConfig, StorageWriteSession, DestinationWriteStats
├── bq_storage_write_utilities.py     # Session bootstrap, chunk planner, finalize/commit helpers
├── write_limits.py                   # Write-limit validation and derived limits
├── row_errors_mapping.py             # Chunk row_error -> original input mapping
├── proto_schema/                     # BigQuery schema -> dynamic protobuf message class
│   ├── bq_proto_schema.py
│   ├── proto_schema_builder.py
│   └── proto_schema_utilities.py
├── row_serializer/
│   ├── bq_proto_serializer.py
│   └── serializer_models.py
└── retry_handler/
    ├── error_types.py
    ├── error_policy.py
    ├── writeapierror.py
    ├── bq_retry_orchestrator.py
    └── bq_retry_orchestrator_models.py

ports/
└── destination.py                    # Destination protocol

config.py                             # Default append limits
tests/storage_write/                  # Pytest suite
```

---

## Key design choices

1. **Minimal port.** Both destination and retry orchestrator implement:

   ```python
   class Destination(Protocol[TRow, TWriteResult]):
       def write(self, rows: Sequence[TRow]) -> TWriteResult: ...
       def close(self) -> None: ...
   ```

2. **Stats-first contract.** Expected business outcomes are returned in stats, not raised as exceptions.
3. **Explicit offsets.** Every append uses `session.next_offset`; offset advances only on safe outcomes.
4. **Clear stream modes.** `COMMITTED` writes are immediate; `PENDING` requires explicit success then commit.
5. **Recoverable row errors.** Row-level rejections are mapped back to original rows for one-pass DLQ + retry split.
6. **`ALREADY_EXISTS` is safe.** Treated as idempotent success for the covered offset window.
7. **Retry policy is data.** Attempts and backoff are configured by category in one place.

Retry categories: `THROTTLE`, `TRANSPORT`, `DEADLINE`, `INTERNAL`,
`STREAM_INVALIDATED`, `OFFSET_OUT_OF_RANGE`.

Terminal categories: `INVALID_ARGUMENT`, `PERMISSION`, `AUTH`, `NOT_FOUND`,
`ROW_ERRORS`, `ALREADY_EXISTS`, `UNKNOWN`.

---

## Configuration

`StorageWriteConfig` is the main configuration surface.

| Field | Default | Purpose |
|---|---|---|
| `project_id` / `dataset_id` / `table_id` | — | Target table |
| `stream_mode` | — | `COMMITTED` or `PENDING` |
| `schema_supplier` | — | Supplies BigQuery `SchemaField` values once per session |
| `proto_message_name` | — | Dynamic protobuf message name |
| `write_client_factory` | `BigQueryWriteClient` | Allows fake client injection in tests |
| `append_timeout_seconds` | `60` | Per-append deadline |
| `append_request_max_bytes` | `9_500_000` | Stays under 10 MB hard cap |
| `append_request_overhead_bytes` | `256_000` | Reserved budget for framing and schema |
| `append_max_rows` | `500` | Row cap per append request |
| `append_row_max_bytes` | derived | Per-row budget derived from request budget when `None` |

`resolve_write_limits(config)` validates limits before planning starts.

---

## Stats and observability

Each write returns typed stats; no hidden state.

`DestinationWriteStats` includes:

- core counters (`attempted_rows`, `written_rows`, `failed_rows`)
- `skipped_already_exists_rows`
- `error: BigQueryStorageWriteError | None`
- serializer failures (`serializer_row_failures`)
- row-error split (`row_error_bad_rows`, `row_error_good_rows`)
- `stream_mode`

`RetryOrchestratorStats` wraps destination stats and adds:

- `retry_attempts_total`, `retries_by_category`
- `max_retries_for_last_error`
- `last_error`, `last_error_category`, `last_status_code`
- `.flatten()` for metrics pipelines

Logging uses `logging.getLogger(__name__)` with structured fields passed in `extra`.

---

## Error taxonomy

`ErrorCategory` maps low-level gRPC outcomes to pipeline-friendly behavior.

| Category | Source | Retryable? | Policy |
|---|---|---|---|
| `SUCCESS` | `AppendRowsResponse.append_result` | — | Continue |
| `TRANSPORT` | connection errors / transient gRPC | yes | Backoff + retry |
| `DEADLINE` | `DEADLINE_EXCEEDED` / timeout | yes | Backoff + retry |
| `THROTTLE` | `RESOURCE_EXHAUSTED` | yes | Longer backoff |
| `INTERNAL` | `INTERNAL`, `ABORTED` | yes | Retry |
| `STREAM_INVALIDATED` | `FAILED_PRECONDITION` (committed) | yes, once | Reset stream |
| `OFFSET_OUT_OF_RANGE` | `OUT_OF_RANGE` (committed) | yes, once | Realign offset |
| `ALREADY_EXISTS` | `ALREADY_EXISTS` | no | Advance offset, continue |
| `ROW_ERRORS` | `AppendRowsResponse.row_errors` | no | DLQ bad rows, retry good rows |
| `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | no | Fatal, DLQ batch |
| `PERMISSION` / `AUTH` / `NOT_FOUND` | obvious | no | Fatal |
| `UNKNOWN` | unmapped | no | Fatal |

`BigQueryStorageWriteError` exposes helper flags:
`should_retry`, `should_send_to_dlq`, `is_fatal`,
`requires_stream_reset`, `requires_offset_alignment`, `has_row_errors`.

---

## Planned expanded project

This adapter is the destination component of a larger
**Firestore -> BigQuery initial-load + DLQ replay** pipeline.

### Target architecture

```text
                    ┌──────────────────────────────────────────────┐
                    │                  Ports                       │
                    │  Source | Destination | DeadLetterSink       │
                    └───────────────┬──────────────────────┬───────┘
                                    │                      │
                                    │                      │
                                    ▼                      ▼
        ┌──────────────────────────────────────┐   ┌──────────────────────────┐
        │      Initial load pipeline           │   │      DLQ replay flow     │
        │                                      │   │                          │
        │ FirestoreSource                      │   │ DlqSource                │
        │   -> Validator + coercion            │   │   -> RowErrorRetry       │
        │   -> split good/bad                  │   │   -> split good/bad      │
        │   -> Storage Write destination       │   │                          │
        └───────────────────┬──────────────────┘   └───────────┬──────────────┘
                            │                                  │
                            │ good rows                        │ good rows
                            ▼                                  ▼
                   ┌──────────────────────────────────────────────────────────┐
                   │      BigQuery target (committed rows / analytics)        │
                   └──────────────────────────────────────────────────────────┘

                            bad rows from validation / serialization / row_errors
                                               │
                                               ▼
                   ┌──────────────────────────────────────────────────────────┐
                   │      DLQ sink (single schema, multiple producers)        │
                   └──────────────────────────────────────────────────────────┘

                            stats from each stage -> aggregated run outcome
```

### Design intent

1. **Ports first.** `Source`, `Destination`, and `DeadLetterSink` are dependency-free interfaces.
2. **Initial load.** Validate input, split good/bad rows, write good rows, send bad rows to DLQ.
3. **Destination split.** Reuse serializer and row-error outputs for one-pass DLQ + retry flow.
4. **DLQ replay.** Re-validate recoverable rows and retry; keep terminal rows in DLQ.
5. **Unified DLQ schema.** One sink accepts validation, serialization, and row-level write errors.
6. **Per-step stats.** Pipeline aggregates `SourceStats`, `ValidationStats`, destination/retry stats, and `DlqStats`.
7. **Simple logging.** Standard `logging` with structured `extra` fields.

---

## License & Usage

This project is for portfolio and demonstration purposes only. All rights reserved. No permission is granted to use, modify, or redistribute this software for any purpose. It is provided "as is" without warranty of any kind.
