# Retry Handler (Draft)

## Small Intro

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
