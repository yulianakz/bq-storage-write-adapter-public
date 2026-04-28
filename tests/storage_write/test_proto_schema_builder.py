"""Smoke tests for BQProtoSchemaBuilder.

Covers the happy path (building a simple schema returns a usable proto message
class whose fields line up with the BigQuery field specs) and the cache path
(building the same schema twice returns the exact same cached ProtoBuildResult).
"""

from types import SimpleNamespace

import pytest

from adapters.bigquery.storage_write.proto_schema.bq_proto_schema import (
    BQFieldSpec,
    BQProtoSchemaBuilder,
)


def _field(name: str, field_type: str, mode: str = "NULLABLE"):
    return SimpleNamespace(name=name, field_type=field_type, mode=mode, fields=())


def _record_field(
    name: str,
    fields: tuple[SimpleNamespace, ...],
    mode: str = "NULLABLE",
):
    return SimpleNamespace(name=name, field_type="RECORD", mode=mode, fields=fields)


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
        BQFieldSpec(name="amount", mode="NULLABLE", field_type="INT64"),
        BQFieldSpec(name="transactionId", mode="REQUIRED", field_type="STRING"),
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


def test_build_accepts_explicit_interval_range_decimal_alias_types() -> None:
    builder = BQProtoSchemaBuilder()
    schema_fields = (
        _field("price", "DECIMAL"),
        _field("priceHighPrecision", "BIGDECIMAL"),
        _field("window", "RANGE"),
        _field("duration", "INTERVAL"),
    )

    result = builder.build(
        schema_fields=schema_fields,
        message_name="ExtendedLogicalTypesRow",
    )

    assert result.field_specs == (
        BQFieldSpec(name="duration", mode="NULLABLE", field_type="INTERVAL"),
        BQFieldSpec(name="price", mode="NULLABLE", field_type="DECIMAL"),
        BQFieldSpec(name="priceHighPrecision", mode="NULLABLE", field_type="BIGDECIMAL"),
        BQFieldSpec(name="window", mode="NULLABLE", field_type="RANGE"),
    )


def test_build_raises_for_unknown_bigquery_type() -> None:
    builder = BQProtoSchemaBuilder()
    schema_fields = (_field("mystery", "CUSTOM_UNKNOWN_TYPE"),)

    with pytest.raises(ValueError, match="Unsupported BigQuery field type"):
        builder.build(schema_fields=schema_fields, message_name="UnknownTypeRow")


def test_build_reordered_top_level_fields_hits_same_cache_key() -> None:
    builder = BQProtoSchemaBuilder()
    first_order = (
        _field("zField", "STRING"),
        _field("aField", "INT64"),
    )
    second_order = (
        _field("aField", "INT64"),
        _field("zField", "STRING"),
    )

    first = builder.build(schema_fields=first_order, message_name="DeterministicTopLevelRow")
    second = builder.build(schema_fields=second_order, message_name="DeterministicTopLevelRow")

    assert first is second
    assert [spec.name for spec in first.field_specs] == ["aField", "zField"]
    message = first.message_cls()
    assert [field.name for field in message.DESCRIPTOR.fields] == ["aField", "zField"]


def test_build_reordered_nested_record_fields_hits_same_cache_key() -> None:
    builder = BQProtoSchemaBuilder()
    first_order = (
        _record_field(
            "meta",
            (
                _field("zNested", "STRING"),
                _field("aNested", "INT64"),
            ),
        ),
    )
    second_order = (
        _record_field(
            "meta",
            (
                _field("aNested", "INT64"),
                _field("zNested", "STRING"),
            ),
        ),
    )

    first = builder.build(schema_fields=first_order, message_name="DeterministicNestedRow")
    second = builder.build(schema_fields=second_order, message_name="DeterministicNestedRow")

    assert first is second
    assert [spec.name for spec in first.field_specs[0].record_fields] == ["aNested", "zNested"]


def test_build_raises_when_casefold_normalized_names_collide_top_level() -> None:
    builder = BQProtoSchemaBuilder()
    schema_fields = (
        _field("UserId", "STRING"),
        _field("userid", "STRING"),
    )

    with pytest.raises(ValueError, match="Duplicate BigQuery field names after casefold normalization"):
        builder.build(schema_fields=schema_fields, message_name="DuplicateNormalizedTopRow")


def test_build_raises_when_casefold_normalized_names_collide_nested_record() -> None:
    builder = BQProtoSchemaBuilder()
    schema_fields = (
        _record_field(
            "payload",
            (
                _field("Start", "STRING"),
                _field("start", "STRING"),
            ),
        ),
    )

    with pytest.raises(ValueError, match="Duplicate BigQuery field names after casefold normalization"):
        builder.build(schema_fields=schema_fields, message_name="DuplicateNormalizedNestedRow")
