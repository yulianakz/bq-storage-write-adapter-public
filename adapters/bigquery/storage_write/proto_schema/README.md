# Proto Schema

## Introduction

`proto_schema` builds a dynamic protobuf message schema from BigQuery table fields, then exposes:

- `types.ProtoSchema` for BigQuery Storage Write API
- generated `message_cls` used by serializer to produce row bytes
- normalized immutable field metadata (`BQFieldSpec`) for downstream logic

The module is split into:

- `proto_schema_builder.py`: singleton builder + thread-safe cache
- `proto_schema_utilities.py`: schema normalization, recursive descriptor assembly, type/mode mapping
- `bq_proto_schema.py`: compatibility shim export surface

---

## Proto Schema Flow

```text
                      BQProtoSchemaBuilder.build()
                                  │
                                  ▼
                    input schema_fields + message_name
                                  │
                                  ▼
                   _to_field_specs(schema_fields)
                    (normalize name/type/mode/records)
                                  │
                                  ▼
                  _schema_signature(schema_fields)
                     -> cache key (message_name, sig)
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
                 cache hit                  cache miss
                    │                           │
                    ▼                           ▼
            return ProtoBuildResult      build FileDescriptorProto
                                                │
                                                ▼
                                _add_fields_rec(...) recursively
                             (scalars + nested RECORD message types)
                                                │
                                                ▼
                              DescriptorPool -> root descriptor
                                                │
                                                ▼
                          message_factory.GetMessageClass(...)
                                                │
                                                ▼
                      build types.ProtoSchema(proto_descriptor)
                                                │
                                                ▼
                                cache and return ProtoBuildResult
```

---

## How It Works

1. `build(...)` receives BigQuery schema field objects plus `message_name`.
2. `_validate_schema_fields(...)` runs a lightweight recursive pre-check:
   - non-empty field names
   - valid modes only (`NULLABLE`, `REQUIRED`, `REPEATED`)
   - `RECORD` fields must define nested fields
   - non-`RECORD` fields must not define nested fields
3. `_to_field_specs(...)` converts raw SDK field objects into immutable `BQFieldSpec` tuples.
4. Field normalization applies deterministic sibling ordering by
   `((field_name or "").casefold(), field_name or "")` at every nesting level.
   It also validates that no two sibling names collide after casefold normalization.
5. `_schema_signature(...)` computes a deterministic structural signature (including nested RECORD trees).
6. Builder checks class-level cache by `(message_name, signature)`.
7. On cache miss, it creates a `FileDescriptorProto` with `proto2` syntax and root message.
8. `_add_fields_rec(...)` recursively maps fields:
   - mode -> protobuf label (`REQUIRED`, `OPTIONAL`, `REPEATED`)
   - BigQuery scalar type -> protobuf scalar type
   - `RECORD` -> nested protobuf message + `type_name` link
9. Descriptor is loaded into `DescriptorPool`; runtime `message_cls` is generated.
10. `ProtoSchema` is built from copied descriptor and returned as `ProtoBuildResult`.

---

## Edge Cases

- **Unknown BigQuery type**  
  Raises `ValueError` during schema build; unknown scalar type tokens must be explicitly mapped.

- **BigQuery logical types (`NUMERIC`, `DATE`, `TIMESTAMP`, `JSON`, etc.)**  
  Intentionally mapped to protobuf `STRING`; formatting/encoding is handled by serializer layer.
  Includes explicit aliases and extended logical tokens:
  - `DECIMAL` (alias of `NUMERIC`)
  - `BIGDECIMAL` (alias of `BIGNUMERIC`)
  - `INTERVAL`
  - `RANGE`

- **Missing mode on field object**  
  Defaults to `NULLABLE` -> protobuf `OPTIONAL`.

- **Invalid mode or malformed RECORD shape**  
  Fails fast during `_validate_schema_fields(...)` with context-aware `ValueError`.

- **Nested RECORD trees**  
  Recursively converted into nested protobuf message types using `<field_name>_msg`.

- **Concurrent callers building same schema**  
  Double-checked cache with lock ensures one build per key and shared result reuse.

- **Equivalent structure, different input object instances**  
  Signature is based on field structure, not object identity, so cache still hits.

- **Equivalent structure, reordered fields**  
  Deterministic sorting normalizes sibling order (including nested `RECORD` fields), so
  cache keys and descriptor layout stay stable across input order variations.

- **Case-only sibling-name collisions**  
  Names that collide after `casefold()` normalization at the same nesting level
  (for example `UserId` and `userid`) raise `ValueError` during schema normalization.

---

## Limitations

- **Cache lifecycle is process-local and unbounded.**  
  Cache entries are keyed by `(message_name, schema_signature)` and currently live for
  the full process lifetime. There is no eviction/TTL/max-size cap yet, so a long-lived
  process that sees many unique schema/message-name combinations can grow memory usage
  over time. Future hardening: add a bounded cache policy (for example LRU + max entries).

- **Supported scalar types are explicit by design.**  
  Unknown scalar type tokens fail fast and require a code-level mapping update.

- **Schema signature is currently stringified tuple structure (`str(...)`).**  
  This is stable enough for current usage, but canonical JSON serialization or a stable hash
  (for example SHA-256 over canonical JSON) would be more future-proof if signature shape evolves.

- **Validation is intentionally lightweight.**  
  The module performs guardrail checks for common malformed inputs, but it is not a full
  BigQuery schema-spec validator or diagnostics framework.

- **Nested message naming strategy is simple (`<field>_msg`).**  
  Works for current generated structure, but not optimized for custom naming conventions.

