# Retry Handler 

## Introduction

`retry_handler` contains the policy layer for classifying Storage Write failures into actionable outcomes (retry, reset stream, DLQ, advance offset, or fail fast).

Goal: keep write behavior predictable under both:

- response-level API errors (`AppendRowsResponse.error`, `row_errors`)
- Python exceptions raised by the stream client (`send()` / `future.result()`)

This is a draft focused only on Error Policy flow and known edge cases.

---

## Error Policy

ExtendedErrorPolicy classifies Storage Write outcomes into a single typed policy object
(`BigQueryStorageWriteError`) that the caller can act on (retry/reset/DLQ/stop).
It does not execute retries itself.

### Error policy flow

```text
                    ExtendedErrorPolicy
                            │
                            ▼
                  classify_result(input, ctx)
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
          ▼                                   ▼
 A) Exception path                     B) AppendRowsResponse path
    (send()/result() raised)              (result() returned response)
          │                                   │
          ▼                                   ▼
 classify_exception()                 classify_append_rows_response()
          │                                   │
  ┌───────┼───────────────┐          ┌────────┼────────────────┐
  │       │               │          │        │                │
  ▼       ▼               ▼          ▼        ▼                ▼
Timeout Transport  GoogleAPICallError? append_result row_errors response.error?
  │       │               │             │          │            │
  ▼       ▼               ▼             ▼          ▼            ▼
policy  policy    grpc_status_code?   SUCCESS   ROW_ERRORS   grpc_code_mapping()
                        │
                        ├─ yes -> normalize code -> grpc_code_mapping()
                        │
                        ├─ no  -> subclass fallback map -> grpc_code_mapping()
                        │
                        └─ no subclass match -> fallback UNKNOWN (fatal)
```

### How it works

Input is classified via `classify_result(...)` as either:
- exception path (`send()` / `future.result()` raised), or
- response path (`AppendRowsResponse` returned).

---

For case 1 (exception path):

1. Handle local timeout/transport classes first.
2. Unwrap `RetryError` via `_unwrap_retry_error(...)` so classification uses the root cause, not the retry wrapper.
3. If the exception is `google.api_core.exceptions.GoogleAPICallError` and has `grpc_status_code`, normalize it with `_normalize_grpc_status_code(...)`.
4. Route normalized gRPC code through shared `grpc_code_mapping(...)`.
5. If `grpc_status_code` is missing, apply subclass fallback mapping (`google.api_core.exceptions.*` -> canonical gRPC code), then route through `grpc_code_mapping(...)`.
6. If no known mapping applies, use unknown fallback (fatal by default).

---

For case 2 (response path):
1. Check `append_result` first (success).
2. Check `row_errors` and return `ROW_ERRORS` policy (batch rejected; caller can split bad/good rows). 
3. Check `response.error.code` and route through shared `grpc_code_mapping(...)`.
4. If response shape is unexpected (no append_result/row_errors/error), use unknown fallback (fatal by default).

---

### gRPC status normalization coverage

Current parser (`_normalize_grpc_status_code`) supports:

- direct integer status code
- enum-like object with `.name` matching `code_pb2` symbols
- enum-like object with `.value` as int
- enum-like object with `.value` tuple where first item is int

Known uncommon forms not parsed yet:

- plain int-like wrappers (rare custom classes that are not `int`)
- custom wrappers where code is nested in another attribute (for example inner payload object)
- non-standard objects that expose neither `.name` nor `.value` in compatible shape

---

## Error Categories (Current Policy Intent)

- `DEADLINE`: request timeout / deadline exceeded
- `TRANSPORT`: network or stream transport instability
- `THROTTLE`: resource exhaustion / server throttling
- `INTERNAL`: retriable server-side internal/aborted states
- `OFFSET_OUT_OF_RANGE`: offset mismatch handling
- `STREAM_INVALIDATED`: stream lifecycle/precondition invalidation
- `ROW_ERRORS`: row-level corruption/rejection for a batch
- `ALREADY_EXISTS`: idempotent offset already committed/buffered
- `INVALID_ARGUMENT`: request/data contract problem
- `PERMISSION`: permission denied
- `AUTH`: authentication failure
- `NOT_FOUND`: missing stream/resource
- `UNKNOWN`: unsupported or unclassified failure

---

## Edge Cases (Flow-oriented)

| Edge case | Where it appears | Expected handling direction |
| --- | --- | --- |
| `grpc_status_code` is `None` on api-core exception | Exception flow | Use subclass fallback map to canonical gRPC code; if no subclass match -> unknown fallback |
| `RetryError` wraps root cause | Exception flow | Unwrap `.cause` and classify root cause first |
| `Cancelled` from local shutdown vs real failure | Exception flow | Intent-aware rule is not implemented yet; `CANCELLED` is intentionally not mapped for now |
| `StreamClosedError` after stream shutdown/reset race | Exception flow | Usually requires stream recreation/reset path |
| `DeadlineExceeded` appears as response code or exception | Both flows | Map consistently to deadline category regardless of channel |
| `ResourceExhausted` from exception channel | Exception flow | Route to same throttle policy as response-code path |
| `Aborted` with reconnect advice | Both flows (commonly exception) | Treat as retriable internal/stream-reset-aware class |
| Enum mismatch (`grpc.StatusCode` vs `code_pb2` int) | Exception flow to mapper | Normalize code representation before calling `grpc_code_mapping()` |
| Unknown response shape (no append_result/row_errors/error) | Response flow | Fallback unknown fatal (defensive safety) |
| Auth/perms errors surfaced only as exceptions | Exception flow | Must still map to AUTH/PERMISSION categories via shared mapper |

---

## Notes for Next Revision

- Decide and implement explicit precedence rule if backend ever returns both `response.error` and `row_errors` together.

---

## Retry Orchestrator

### Small Intro

`BigQueryWriteRetryOrchestrator` currently acts as a proxy wrapper around destination I/O:

- it wraps destination `write(rows)` and `close()`
- it directly calls destination methods from inside its own `write()` / `close()`
- it reads `DestinationWriteStats.error` (`BigQueryStorageWriteError`) and decides whether to:
  - return immediately (success or terminal failure), or
  - sleep with backoff and retry the same batch

The orchestrator owns retry budget, retry counters, and final retry telemetry.
It does not perform chunk-level appends itself; chunking stays inside destination.

---

### Retry Orchestrator Flow

```text
                     BigQueryWriteRetryOrchestrator
                                  │
                                  ▼
                          write(input_rows)
                                  │
                                  ▼
                    destination.write(input_rows)
                                  │
                                  ▼
                     DestinationWriteStats + error?
                                  │
                  ┌───────────────┴────────────────┐
                  │                                │
                  ▼                                ▼
              error is None                    error exists
                  │                                │
                  ▼                                ▼
      return success RetryOrchestratorStats   classify by category
                                                   │
                                                   ▼
                                    retryable and retry budget left?
                                                   │
                           ┌───────────────────────┴───────────────────────┐
                           │                                               │
                           ▼                                               ▼
                    no (terminal path)                             yes (retry path)
                           │                                               │
                           ▼                                               ▼
        return failed RetryOrchestratorStats                 backoff sleep + retry
                                                              (same input_rows again)
```

---

### How it works

1. Orchestrator receives a full `rows` batch from the caller.
2. It calls `destination.write(rows)` and gets `DestinationWriteStats`.
3. If `destination_stats.error` is `None`, it returns success immediately.
4. If error exists:
   - compute `max_retries` from category policy (`RETRY_ATTEMPTS_BY_CATEGORY`)
   - if non-retryable: return terminal failed stats immediately
   - if retryable but budget exhausted: return terminal failed stats
   - if retryable and budget available: increment retry counters, compute backoff, sleep, and call destination again with the same `rows`
5. On close, orchestrator delegates `close()` to destination; it does not manage stream lifecycle itself.

---

### Retryable Errors (Current Policy)

| ErrorCategory | Retry attempts |
| --- | --- |
| `THROTTLE` | `3` |
| `TRANSPORT` | `2` |
| `DEADLINE` | `2` |
| `INTERNAL` | `1` |
| `STREAM_INVALIDATED` | `1` |
| `OFFSET_OUT_OF_RANGE` | `1` |
| `ROW_ERRORS` | `0` |
| `ALREADY_EXISTS` | `0` |
| `INVALID_ARGUMENT` | `0` |
| `PERMISSION` | `0` |
| `AUTH` | `0` |
| `NOT_FOUND` | `0` |
| `UNKNOWN` | `0` |
| `SUCCESS` | `0` |

---

### Current edge cases and limitations

- **Batch-level retry contract.**
  Orchestrator retries by replaying the same original `rows` batch, not a computed suffix.
- **Chunking happens only in destination.**
  For large payloads, destination may split one input batch into multiple append chunks.
- **Partial progress in chunked writes is possible.**
  If chunk N succeeds and chunk N+1 fails, retry replays original batch; already-accepted chunks may return `ALREADY_EXISTS`.
- **Correctness currently relies on idempotent offset behavior.**
  Replay safety depends on server-side offset semantics (`ALREADY_EXISTS` as benign no-op for already-accepted offsets).
- **Cost/perf trade-off on large chunked batches.**
  Replay of full input can add extra round-trips when many earlier chunks already succeeded.
- **No row-level retry path by design.**
  This module intentionally preserves batch semantics and avoids per-row transport retry behavior.

- **Retry backoff is blocking per worker (`time.sleep`).**
  Retry waits are synchronous in the orchestrator loop. This is simple and deterministic,
  but in high-parallel worker setups it can reduce effective throughput while workers sleep.
  Future hardening: optional non-blocking/asynchronous retry scheduling at pipeline level.

---

## Retry Orchestrator Stats

`RetryOrchestratorStats` is the orchestrator-owned telemetry envelope around one
logical write operation. It includes:

- destination snapshot from the last destination attempt (`destination_stats`)
- retry loop counters accumulated across attempts
- last error metadata used by logs/alerts

### Fields and ownership

| Field | Owned by | Meaning |
| --- | --- | --- |
| `destination_stats` | Destination | Last per-attempt `DestinationWriteStats` snapshot returned by destination. |
| `retry_attempts_total` | Orchestrator | Number of retries performed after the initial attempt. |
| `total_rows_passed_to_retry` | Orchestrator | Sum of `len(rows)` for each scheduled retry. |
| `retries_by_category` | Orchestrator | Retry count grouped by error category label. |
| `max_retries_for_last_error` | Orchestrator | Retry budget configured for the last observed error category. |
| `last_error` | Orchestrator | Last classified error object seen in retry loop. |
| `last_error_category` | Orchestrator | Category label for `last_error`. |
| `last_status_code` | Orchestrator | gRPC/status code for `last_error` when present. |

### Flattened stats purpose

`RetryOrchestratorStats.flatten()` returns `FlattenedRetryOrchestratorStats` for:

- logging and metrics payloads that should avoid nested dataclass traversal
- stable, typed fields for dashboards/alerts
- preserving destination stage counters and orchestrator retry counters in one flat object

Flattened output includes:

- orchestrator counters (`retry_attempts_total`, `total_rows_passed_to_retry`, `retries_by_category`, last error metadata)
- destination counters (`dest_total_rows`, serializer/chunk/send counters, row-error counts, skip counts, stream mode)
- destination error summary (`dest_error_category`, `dest_error_status_code`)

### Contract notes

- Destination stats remain **per-attempt** and are not accumulated across retries.
- Orchestrator stats remain **cross-attempt** for retry behavior only.
- Loop-guard fallback path returns a synthetic destination stats object with safe defaults so flatten stays schema-consistent.
