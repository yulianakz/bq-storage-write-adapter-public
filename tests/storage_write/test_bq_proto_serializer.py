from types import SimpleNamespace

import pytest

from adapters.bigquery.storage_write.proto_schema.bq_proto_schema import BQProtoSchemaBuilder
from adapters.bigquery.storage_write.row_serializer.bq_proto_serializer import (
    BQProtoRowSerializer,
)


def _field(name: str, field_type: str, mode: str = "NULLABLE", fields=None):
    return SimpleNamespace(name=name, field_type=field_type, mode=mode, fields=fields or ())


def _build_serializer(schema_fields):
    build_result = BQProtoSchemaBuilder.instance().build(
        schema_fields=schema_fields,
        message_name="SerializerDiagnosticsRow",
    )
    return BQProtoRowSerializer(
        message_cls=build_result.message_cls,
        field_specs=build_result.field_specs,
    )


def test_serialize_rows_all_good_rows() -> None:
    serializer = _build_serializer(
        (
            _field("transactionId", "STRING", "REQUIRED"),
            _field("amount", "INT64"),
        )
    )
    rows = [{"transactionId": "tx-1", "amount": 1}, {"transactionId": "tx-2", "amount": 2}]

    result = serializer.serialize_rows(rows, row_max_bytes=None)

    assert len(result.good_serialized_rows) == 2
    assert result.bad_rows == []
    assert result.good_row_indices == [0, 1]
    assert all(len(b) > 0 for b in result.good_serialized_rows)


def test_serialize_rows_missing_required_field() -> None:
    serializer = _build_serializer((_field("transactionId", "STRING", "REQUIRED"),))
    rows = [{}, {"transactionId": "tx-2"}]

    result = serializer.serialize_rows(rows, row_max_bytes=None)

    assert len(result.good_serialized_rows) == 1
    assert len(result.bad_rows) == 1
    failed = result.bad_rows[0]
    assert failed.row_index == 0
    assert failed.reason_enum == "serialization_missing_required"
    assert "Missing REQUIRED field 'transactionId'" in failed.error_message


def test_serialize_rows_type_mismatch() -> None:
    serializer = _build_serializer((_field("count", "INT64"),))
    rows = [{"count": "not-an-int"}]

    result = serializer.serialize_rows(rows, row_max_bytes=None)

    assert result.good_serialized_rows == []
    assert len(result.bad_rows) == 1
    failed = result.bad_rows[0]
    assert failed.row_index == 0
    assert failed.reason_enum == "serialization_type_error"
    assert "Expected int for INT64" in failed.error_message


def test_serialize_rows_invalid_json_string() -> None:
    serializer = _build_serializer((_field("payload", "JSON"),))
    rows = [{"payload": '{"not": "valid"'}]

    result = serializer.serialize_rows(rows, row_max_bytes=None)

    assert result.good_serialized_rows == []
    assert len(result.bad_rows) == 1
    failed = result.bad_rows[0]
    assert failed.row_index == 0
    assert failed.reason_enum == "serialization_invalid_json"
    assert "invalid JSON" in failed.error_message


def test_serialize_rows_mixed_batch_tolerance() -> None:
    serializer = _build_serializer(
        (
            _field("transactionId", "STRING", "REQUIRED"),
            _field("count", "INT64"),
            _field("payload", "JSON"),
            _field("sourceDocumentId", "STRING"),
            _field("metaJobId", "STRING"),
        )
    )
    rows = [
        {
            "transactionId": "tx-good",
            "count": 7,
            "payload": '{"a": 1}',
            "sourceDocumentId": "doc-1",
            "metaJobId": "job-1",
        },
        {"count": 8, "payload": '{"a": 2}'},
        {"transactionId": "tx-bad", "count": "bad", "payload": '{"a": 3}'},
    ]

    result = serializer.serialize_rows(rows, row_max_bytes=None)

    assert len(result.good_serialized_rows) == 1
    assert result.good_row_indices == [0]
    assert [entry.row_index for entry in result.bad_rows] == [1, 2]
    assert result.bad_rows[0].reason_enum == "serialization_missing_required"
    assert result.bad_rows[1].reason_enum == "serialization_type_error"
    assert dict(result.bad_rows[1].raw_row or {}) == {
        "transactionId": "tx-bad",
        "count": "bad",
        "payload": '{"a": 3}',
    }


def test_serialize_rows_row_too_large_classification() -> None:
    serializer = _build_serializer((_field("transactionId", "STRING", "REQUIRED"),))
    rows = [{"transactionId": "tx-very-large"}]

    result = serializer.serialize_rows(rows, row_max_bytes=1)

    assert result.good_serialized_rows == []
    assert result.good_row_indices == []
    assert len(result.bad_rows) == 1
    failed = result.bad_rows[0]
    assert failed.row_index == 0
    assert failed.reason_enum == "serialization_row_too_large"
    assert "exceeds row_max_bytes 1" in failed.error_message


def test_serialize_rows_bad_row_includes_full_raw_row() -> None:
    serializer = _build_serializer((_field("transactionId", "STRING", "REQUIRED"),))
    rows = [
        {
            "transactionId": "tx-1",
            "sourceDocumentId": "doc-1",
            "metaJobId": "job-1",
            "transactionCreatedAt": "2026-01-01T00:00:00Z",
            "sourceSystem": "firestore",
            "schemaVersion": 1,
            "messageId": "msg-1",
            "rawPayload": {"key": "value"},
        }
    ]

    result = serializer.serialize_rows(rows, row_max_bytes=1)

    assert len(result.bad_rows) == 1
    raw = dict(result.bad_rows[0].raw_row or {})
    assert raw["transactionId"] == "tx-1"
    assert raw["rawPayload"] == {"key": "value"}


def test_serialize_rows_unexpected_error_classification(monkeypatch) -> None:
    serializer = _build_serializer((_field("transactionId", "STRING", "REQUIRED"),))

    def _raise_runtime_error(_row):
        raise RuntimeError("unexpected runtime issue")

    monkeypatch.setattr(serializer, "serialize_row", _raise_runtime_error)
    result = serializer.serialize_rows([{"transactionId": "tx-1"}], row_max_bytes=None)

    assert len(result.bad_rows) == 1
    assert result.bad_rows[0].reason_enum == "serialization_unexpected"
