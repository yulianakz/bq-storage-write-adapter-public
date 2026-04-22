"""Smoke tests for BQProtoSchemaBuilder.

Covers the happy path (building a simple schema returns a usable proto message
class whose fields line up with the BigQuery field specs) and the cache path
(building the same schema twice returns the exact same cached ProtoBuildResult).
"""

from types import SimpleNamespace

from adapters.bigquery.storage_write.proto_schema.bq_proto_schema import (
    BQFieldSpec,
    BQProtoSchemaBuilder,
)


def _field(name: str, field_type: str, mode: str = "NULLABLE"):
    return SimpleNamespace(name=name, field_type=field_type, mode=mode, fields=())


def test_build_simple_schema_returns_usable_message_class() -> None:
    builder = BQProtoSchemaBuilder()
    schema_fields = (
        _field("transactionId", "STRING", "REQUIRED"),
        _field("amount", "INT64"),
    )

    result = builder.build(
        schema_fields=schema_fields, message_name="ProtoSchemaSmokeRow"
    )

    assert result.field_specs == (
        BQFieldSpec(name="transactionId", mode="REQUIRED", field_type="STRING"),
        BQFieldSpec(name="amount", mode="NULLABLE", field_type="INT64"),
    )
    # The dynamic message class should be instantiable and expose the BQ field
    # names on its descriptor — this is what the serializer relies on downstream.
    message = result.message_cls()
    descriptor_field_names = {f.name for f in message.DESCRIPTOR.fields}
    assert descriptor_field_names == {"transactionId", "amount"}
    assert result.proto_schema is not None


def test_build_same_schema_twice_returns_cached_result() -> None:
    builder = BQProtoSchemaBuilder()
    schema_fields = (_field("id", "STRING", "REQUIRED"),)

    first = builder.build(schema_fields=schema_fields, message_name="CachedRow")
    second = builder.build(schema_fields=schema_fields, message_name="CachedRow")

    assert first is second
