# Storage Write Destination

## Introduction

`storage_write` is the destination-layer implementation for BigQuery Storage Write API writes.

It coordinates:

- destination orchestration (`BigQueryStorageWriteDestination`)
- row serialization diagnostics (`row_serializer`)
- append chunk planning + session utilities (`bq_storage_write_utilities.py`)
- request-size safeguards (`write_limits.py`)
- row error index translation back to original input rows (`row_errors_mapping.py`)
- output write summary (`DestinationWriteStats`)

This package is the seam between pipeline input rows and BigQuery append semantics (offsets, chunk-local row errors, and stream lifecycle).

---

## Destination Write Flow

```text
                        BigQueryStorageWriteDestination.write(rows)
                                        │
                                        ▼
                            resolve_write_limits(config)
                 (payload budget, max rows, row max bytes guardrails)
                                        │
                                        ▼
                           _serialize_with_diagnostics(rows)
                                        │
                         returns (serialized_rows, serializer_bad_rows,
                                  good_row_indices)
                                        │
                      ┌─────────────────┴─────────────────┐
                      │                                   │
                      ▼                                   ▼
             no serialized rows                     serialized rows exist
                      │                                   │
                      ▼                                   ▼
         fail fast with synthetic error          plan_append_chunks(...)
       + serializer diagnostics in stats          (chunk by row count/payload)
                                                          │
                                                          ▼
                                           iterate append chunks in order
                                                          │
                                            _append_chunk + classify_result
                                                          │
                      ┌───────────────────────┬────────────────────────────┬──────────────────────────────┐
                      │                       │                            │                              │
                      ▼                       ▼                            ▼                              ▼
                 success                 benign no-op                 row_errors               non-row policy error
            (advance offset,            (ALREADY_EXISTS,        map_chunk_row_errors_to_original   (transport/classified:
             written_rows += n)         advance offset,             (chunk-local -> original rows)  gRPC, timeout, terminal,
                                        skipped += n)                     │                    retryable non-row failure)
                                                                          ▼                              │
                                                          accumulate row_error_bad/good + row_errors     ▼
                                                                          │                          stop chunk loop
                                                                          ▼                              │
                                                               continue with next chunk                  ▼
                                                                          │                 aggregate DestinationWriteStats
                                                                          └──────────────────────────────► and return to caller
                                                                                                             (retry orchestrator
                                                                                                              consumes result)
```

---

## How It Works

1. `write(...)` validates empty input quickly (`attempted_rows=0`, `ok=True`).
2. `resolve_write_limits(...)` enforces request constraints:
   - request max bytes
   - request overhead bytes
   - max rows per append
   - derived or explicit per-row byte cap
3. Serializer returns:
   - `good_serialized_rows`: protobuf wire rows to send
   - `bad_rows`: per-row serialization failures
   - `good_row_indices`: mapping from serialized position to original input position
4. If nothing serialized, destination returns a synthetic non-retryable invalid-argument style error and includes serializer diagnostics.
5. `plan_append_chunks(...)` splits serialized rows into append-safe chunks by payload and row count.
6. Each chunk is appended with offset bookkeeping; policy classification handles:
   - clean success
   - benign no-op categories (for example offset already applied)
   - row errors
   - non-row failures (for example gRPC transport exception, append timeout, or other classified non-row policy error)
7. Row errors are translated from chunk-local indices into original input rows via `map_chunk_row_errors_to_original(...)`.
8. `_aggregate_write_stats(...)` computes final counters and exposes diagnostics in `DestinationWriteStats`.
9. Destination does not retry by itself; it returns stats/error to the caller, and retry orchestration decides next attempt behavior.

---

## Row Error Mapping Contract

`row_errors_mapping.py` translates API row error indices from append-chunk space back to original input dicts.

Contract for `map_chunk_row_errors_to_original(...)`:

- Output rows preserve original-input order (ascending original index).
- Bad rows are deduplicated by original index.
- Good rows are not deduplicated (valid mapping occurrences are kept).
- Invalid mappings are skipped (no exception is raised).
- Returns `(None, None)` when no row error index can be mapped to a valid original row.

This hardening prevents destination-level crashes if serializer/chunk invariants drift and keeps diagnostics best-effort rather than failing the entire write path.

---

## Models and Stats Semantics

`DestinationWriteStats` fields represent distinct failure surfaces:

- `serializer_row_failures` / `serializer_row_failure_count`:
  rows rejected before append (never sent to BigQuery).
- `row_error_bad_rows` / `row_error_good_rows`:
  rows from append row-error chunks after index remapping.
- `skipped_already_exists_rows`:
  rows skipped due to benign offset-no-op outcomes.
- `failed_rows`:
  computed as `attempted_rows - written_rows - skipped_already_exists_rows` (never negative).

`failed_rows` is an aggregate count; detailed row-level diagnostics come from serializer failures and row-error mapped rows.

---

## Utilities and Limits

- `write_limits.py`:
  validates and resolves guardrail limits (`ResolvedWriteLimits`).
- `bq_storage_write_utilities.py`:
  chunk planner + session construction helpers (stream name, schema/serializer build, append stream creation, finalize/commit helpers).
- `bq_storage_write_models.py`:
  config/session/stats dataclasses and stream state enums.

These modules intentionally isolate transport constraints and session wiring from destination orchestration logic.

---

## Edge Cases

- **All rows fail serialization**
  returns `ok=False`, `written_rows=0`, with serializer diagnostics; no append call is issued.

- **Mixed serializer pass/fail rows**
  only good serialized rows are sent; bad rows stay as serializer diagnostics.

- **Multi-chunk append with row_errors**
  - when the serialized byte list cannot fit one request (payload budget or row-count cap),
  - `plan_append_chunks(...)` splits it into multiple append requests. 
  - The destination still
  attempts subsequent chunks after a row-error chunk, accumulates each chunk's mapped
  `row_error_bad_rows` / `row_error_good_rows`, and returns one aggregate row-error outcome
  at the end of `write(...)`.

- **Benign already-exists/no-op append outcome**
  rows are counted in `skipped_already_exists_rows` and not treated as failed rows.

- **Chunk mapping drift/mismatch**
  invalid serialized/original indices are skipped in row-error mapping rather than raising.

---

## Limitations

- **Observability for mapping drift is currently log-driven by caller policy**
  (for example structured warning logs). Stats model does not yet include dedicated mapping-mismatch counters.

- **Destination stats are batch-level output**
  they summarize one `write(...)` call, not cross-call rolling telemetry.

- **Retry orchestration behavior lives in `retry_handler`**
  destination focuses on one write attempt and policy outcomes per append response.

- **`derived_chunks_len` can be verbose for unusually high chunk counts**
  In common workloads this list is short, but in large batches split into many chunks,
  flattening/logging this full vector may increase telemetry payload size.
  For high-volume metric pipelines, prefer aggregate counters (`derived_chunk_count`,
  append send counters) and sample full chunk vectors only when needed for debugging.

- **Detailed row-error diagnostics can be memory-heavy on bad batches**
  Destination stats may carry full mapped `row_error_bad_rows` and `row_error_good_rows`
  (original input dicts), and serializer diagnostics may also retain original row payloads.
  This is useful for recovery/debugging, but repeated failure-heavy runs with large/nested
  rows can increase memory and log/export payload sizes. Future hardening: configurable
  payload truncation/redaction or summary-only diagnostics mode.

- **`close()` may raise after successful writes when pending finalize/commit fails**
  For pending streams, finalize/commit failures during `close()` are re-raised by design
  to preserve correctness. Operationally, callers must treat `close()` as a fallible step
  and handle/report failures explicitly to avoid noisy teardown crashes.

- **Append response precedence depends on current documented API contract**
  Current destination policy treats `append_result` as success before inspecting
  `row_errors`/`response.error`. This aligns with current BigQuery Storage Write docs:
  `AppendRowsResponse.response` is a union/oneof (`append_result` or `error`), and
  `row_errors` are documented on failed/corrupted-row requests where no rows are appended.
  Future hardening: if backend/client behavior ever surfaces mixed signals in one response,
  add explicit precedence/validation logic and anomaly logging.

---

## Destination Write Stats

`DestinationWriteStats` is a per-attempt snapshot returned by one call to
`BigQueryStorageWriteDestination.write(rows)`. It does not accumulate across retries.
The retry orchestrator may call destination multiple times, but each call produces a fresh stats object.

### Counter flow (destination + retry wrapper context)

```text
                              caller / pipeline
                                    │
                                    │  N rows
                                    ▼
        ┌───────── RetryOrchestrator.write(rows) ─────────────────────────────────┐
        │   loop attempt = 0..max:                                                │
        │     if attempt > 0:                                                     │
        │        total_rows_passed_to_retry += len(rows)                          │
        │        retry_attempts_total += 1                                        │
        │        retries_by_category[cat] += 1                                    │
        │                                                                         │
        │   ┌───────── Destination.write(rows) ─────────────────────────────────┐ │
        │   │  STAGE 1: entry                                                   │ │
        │   │    total_rows = len(rows)                                         │ │
        │   │                                                                   │ │
        │   │  STAGE 2: resolve_write_limits()                                  │ │
        │   │    pass -> resolve_write_limit_passed += N                        │ │
        │   │    fail -> resolve_write_limit_failed += N                        │ │
        │   │            return terminal INVALID_ARGUMENT                       │ │
        │   │                                                                   │ │
        │   │  STAGE 3: serializer                                              │ │
        │   │    serializer_attempted_rows = passed                             │ │
        │   │    serializer_rows_passed = len(good)                             │ │
        │   │    serializer_rows_failed = len(bad)                              │ │
        │   │                                                                   │ │
        │   │  STAGE 4: plan chunks                                             │ │
        │   │    chunk_planner_attempted = serializer_ok                        │ │
        │   │    derived_chunk_count = len(chunks)                              │ │
        │   │    derived_chunks_len = [len(c.rows), ...]                        │ │
        │   │                                                                   │ │
        │   │  STAGE 5: append loop                                             │ │
        │   │    append_rows_send_attempted += chunk_len                        │ │
        │   │    SUCCESS       -> append_rows_send_passed += chunk_len          │ │
        │   │    ALREADY_EXISTS-> skipped_already_exists_rows += chunk_len      │ │
        │   │    ROW_ERRORS    -> append_rows_send_failed += chunk_len          │ │
        │   │                     row_error_bad/good lists + counts accumulate  │ │
        │   │                     continue                                      │ │
        │   │    TERMINAL/TRANSPORT -> append_rows_send_failed += chunk_len     │ │
        │   │                          error=policy_error, break                │ │
        │   │                                                                   │ │
        │   │  STAGE 6: aggregate                                               │ │
        │   │    total_written_rows = append_rows_send_passed                   │ │
        │   │    total_failed_rows = total_rows - total_written_rows - skipped  │ │
        │   │    ok = (error is None)                                           │ │
        │   └───────────────────────────────────────────────────────────────────┘ │                       │
        │                                                                         │
        │   retryable + budget left -> sleep + retry                              │
        │   non-retryable | budget exhausted -> return                            │
        └─────────────────────────────────────────────────────────────────────────┘
```

### Stats groups

- **Outcome:** `ok`, `error`, `stream_mode`
- **Batch totals:** `total_rows`, `total_written_rows`, `total_failed_rows`
- **Limits stage:** `resolve_write_limit_passed`, `resolve_write_limit_failed`
- **Serializer stage:** `serializer_attempted_rows`, `serializer_rows_passed`, `serializer_rows_failed`, `serializer_row_failures`
- **Chunk planner:** `chunk_planner_attempted`, `derived_chunk_count`, `derived_chunks_len`
- **Append send:** `append_rows_send_attempted`, `append_rows_send_passed`, `append_rows_send_failed`, `skipped_already_exists_rows`
- **Row errors diagnostics:** `row_error_bad_count`, `row_error_good_count`, `row_error_bad_rows`, `row_error_good_rows`

### Edge cases and expected counters

| Scenario | Loop behavior | Key counters |
| --- | --- | --- |
| Empty input | immediate return | all counters `0`, `ok=True` |
| `resolve_write_limits` failure | terminal return before serializer | `resolve_write_limit_failed = total_rows`, serializer/chunk/send counters `0` |
| All rows fail serializer | terminal return before chunk planner | `serializer_rows_passed=0`, `serializer_rows_failed=total_rows`, send counters `0` |
| Chunk `ROW_ERRORS` | continue to next chunk | `append_rows_send_failed += chunk_len`, row-error lists/counts accumulate |
| Chunk `ALREADY_EXISTS` | continue to next chunk | `skipped_already_exists_rows += chunk_len`, no send pass/fail increment |
| Retryable transport/terminal | break chunk loop, return error | failed chunk counted in `append_rows_send_failed`; unsent later chunks are represented via `total_failed_rows` formula |
| Non-retryable terminal | break chunk loop, return error | same per-attempt accounting as retryable terminal; orchestrator does not retry |

