# Row Serializer

## Introduction

`row_serializer` converts Python row dicts into protobuf bytes required by BigQuery Storage Write API (`ProtoRows.serialized_rows`).

It provides:

- `BQProtoRowSerializer`: row/batch serialization pipeline
- `SerializationBatchResult`: partitioned good/bad row output
- `RowSerializationError`: structured per-row failure details

This module does not write to BigQuery directly; it only validates/encodes rows against the already-built dynamic message schema.

---

## Row Serializer Flow

```text
                     BQProtoRowSerializer.serialize_rows()
                                   │
                                   ▼
                    input rows + optional row_max_bytes limit
                                   │
                                   ▼
                      iterate rows with original row_index
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                    ▼                             ▼
              serialize_row(row)               exception
                    │                             │
                    ▼                             ▼
          _set_fields(recursive by schema)   map reason enum
                    │                             │
                    ▼                             ▼
            _encode_scalar per field type   RowSerializationError
                    │                       (row_index, reason_enum,
                    │                        error_message, field_path, raw_row)
                    │
                    ▼
             protobuf SerializeToString()
                    │
                    ▼
           enforce row_max_bytes (if provided)
                    │
            ┌───────┴────────┐
            │                │
            ▼                ▼
         within limit      exceeds limit
            │                │
            ▼                ▼
      add to good rows   RowSerializationError
            │                │
            └───────┬────────┘
                    ▼
         return SerializationBatchResult
```

---

## How It Works

1. Serializer is initialized with dynamic protobuf `message_cls` and normalized `field_specs`.
2. `serialize_rows(...)` loops through input rows and serializes each row independently.
3. `serialize_row(...)` creates a new message instance and delegates to `_set_fields(...)`.
4. `_set_fields(...)` walks schema fields:
   - checks missing required fields
   - skips absent/`None` optional fields (treated as unset)
   - routes repeated fields to `_set_repeated_field(...)`
   - recursively handles nested RECORD fields
   - accumulates normalized field path (`user.address.street`, `items[1].price`)
   - encodes scalar fields via `_encode_scalar(...)`
5. `_encode_scalar(...)` applies BigQuery-aware conversions (Decimal/date/datetime/JSON/bytes/etc.).
6. JSON mappings/sequences use strict `json.dumps(...)` (no `default=str` fallback); non-serializable
   values fail with `invalid JSON` classification.
7. Serialized bytes may be filtered by `row_max_bytes`; oversize rows become structured bad-row errors.
8. Any exception during one row serialization is captured as `RowSerializationError`; batch continues.
9. Final result returns:
   - `good_serialized_rows`
   - `good_row_indices` (mapping back to source input rows)
   - `bad_rows` with row-level diagnostics (including `field_path` when available)

---

## Edge Cases

- **Missing REQUIRED field**
  Raises row-level failure (`serialization_missing_required`) for that row only.

- **Optional field omitted or set to `None`**
  Treated as protobuf "unset" (safe skip).

- **REPEATED field receives non-sequence (or string/bytes)**
  Rejected as type error.

- **REPEATED field contains `None` items**
  Rejected; repeated values cannot contain null entries.

- **RECORD value is not a mapping**
  Rejected with clear type error; nested dict shape is required.

- **JSON field provided as string**
  String is validated with `json.loads`; invalid JSON is rejected.

- **JSON field provided as mapping/sequence**
  Value is encoded with strict `json.dumps` (no permissive fallback). Non-serializable nested
  values are rejected and reported as `serialization_invalid_json`.

- **INTERVAL and RANGE**
  Both are currently strict string fields. `RANGE` support is intentionally string-first;
  later, if needed, this can be switched to structured input (`{"start": ..., "end": ...}`).

- **TIMESTAMP normalization**
  Datetime is converted to UTC; `+00:00` suffix is normalized to `Z`.

- **Row too large (`row_max_bytes`)**
  Row is moved to `bad_rows` with `serialization_row_too_large`.

- **Single bad row in batch**
  Does not fail the whole batch; good rows continue serializing.

- **Nested/repeated field failures**
  Errors include `field_path` (for example `user.address.street` or `items[2].price`) to make
  deep payload diagnostics actionable.

---

## Limitations

- **No coercion for some strict types**
  Example: `INT64` requires `int` (bool explicitly rejected), and
  `NUMERIC`/`BIGNUMERIC`/`DECIMAL`/`BIGDECIMAL` require `Decimal`.

- **Limited reason enum taxonomy**
  Errors are mapped into a small set of reason strings; not a fully granular error code system.

- **Partial path coverage for non-field failures**
  `field_path` is present for schema/encoding failures. Batch-level failures (for example
  row byte-size over limit) are not tied to a specific field and therefore keep `field_path=None`.

- **No cross-row optimization**
  Serialization is row-by-row and exception-driven; no vectorized path.

- **Diagnostics can retain full input payloads for failed rows**
  `RowSerializationError.raw_row` keeps the original row object for troubleshooting.
  On large/nested payloads with repeated failure-heavy batches, this can increase memory
  footprint and observability payload size. Future hardening: optional truncation/sampling
  or disabling raw payload capture in high-volume production paths.

