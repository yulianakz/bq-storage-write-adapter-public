from types import SimpleNamespace

from adapters.bigquery.storage_write.bq_storage_write_destination import (
    BigQueryStorageWriteDestination,
)
from adapters.bigquery.storage_write.bq_storage_write_models import (
    StorageWriteConfig,
    StorageWriteSession,
    StreamMode,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.row_serializer.serializer_models import (
    RowSerializationError,
    SerializationBatchResult,
)


class _FakeFuture:
    def __init__(self, response: object) -> None:
        self._response = response

    def result(self, timeout=None):
        return self._response


class _FakeAppendRowsStream:
    def __init__(self) -> None:
        self.requests = []

    def send(self, request):
        self.requests.append(request)
        return _FakeFuture(SimpleNamespace())

    def close(self) -> None:
        return None


class _FakeWriteClient:
    def close(self) -> None:
        return None

    def table_path(self, project: str, dataset: str, table: str) -> str:
        return f"projects/{project}/datasets/{dataset}/tables/{table}"


class _FakeSerializer:
    def __init__(self, result: SerializationBatchResult) -> None:
        self._result = result
        self.calls = []

    def serialize_rows(self, rows, *, row_max_bytes):
        self.calls.append((rows, row_max_bytes))
        return self._result


def _config(**overrides) -> StorageWriteConfig:
    return StorageWriteConfig(
        project_id=overrides.get("project_id", "project"),
        dataset_id=overrides.get("dataset_id", "dataset"),
        table_id=overrides.get("table_id", "table"),
        stream_mode=overrides.get("stream_mode", StreamMode.COMMITTED),
        schema_supplier=overrides.get("schema_supplier", lambda: []),
        proto_message_name=overrides.get("proto_message_name", "RowMessage"),
        append_request_max_bytes=overrides.get("append_request_max_bytes", 1_000_000),
        append_request_overhead_bytes=overrides.get("append_request_overhead_bytes", 1_000),
        append_row_max_bytes=overrides.get("append_row_max_bytes", 256_000),
    )


def test_write_skips_append_when_all_rows_fail_serializer(monkeypatch) -> None:
    fake_serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[],
            bad_rows=[
                RowSerializationError(
                    row_index=0,
                    reason_enum="serialization_missing_required",
                    error_message="Missing REQUIRED field",
                ),
                RowSerializationError(
                    row_index=1,
                    reason_enum="serialization_type_error",
                    error_message="Expected int",
                ),
            ],
            good_row_indices=[],
        )
    )
    fake_stream = _FakeAppendRowsStream()
    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="stream",
        proto_schema=SimpleNamespace(),
        row_serializer=fake_serializer,
        append_rows_stream=fake_stream,
    )

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )

    destination = BigQueryStorageWriteDestination(config=_config())
    rows = [{"id": 1}, {"id": 2}]

    stats = destination.write(rows)

    # When every input row is dropped by the serializer there is nothing to send
    # on the wire, so the destination surfaces a synthetic INVALID_ARGUMENT
    # error (ok=False) alongside the per-row serializer diagnostics that the
    # pipeline routes to DLQ.
    assert stats.ok is False
    assert stats.attempted_rows == 2
    assert stats.written_rows == 0
    assert stats.failed_rows == 2
    assert stats.serializer_row_failure_count == 2
    assert stats.serializer_row_failures is not None
    assert len(stats.serializer_row_failures) == 2
    assert stats.error is not None
    assert stats.error.category == ErrorCategory.INVALID_ARGUMENT
    assert stats.error.send_to_dlq is True
    assert len(fake_stream.requests) == 0


def test_write_appends_only_good_serialized_rows(monkeypatch) -> None:
    fake_serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[b"row-a", b"row-c"],
            bad_rows=[
                RowSerializationError(
                    row_index=1,
                    reason_enum="serialization_row_too_large",
                    error_message="too large",
                )
            ],
            good_row_indices=[0, 2],
        )
    )
    fake_stream = _FakeAppendRowsStream()
    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="stream",
        proto_schema=SimpleNamespace(),
        row_serializer=fake_serializer,
        append_rows_stream=fake_stream,
    )

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ErrorPolicy.classify_exception",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ErrorPolicy.classify_append_rows_response",
        lambda *args, **kwargs: None,
    )

    destination = BigQueryStorageWriteDestination(config=_config())
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]

    stats = destination.write(rows)

    assert stats.ok is True
    assert stats.attempted_rows == 3
    assert stats.written_rows == 2
    assert stats.failed_rows == 1
    assert stats.serializer_row_failure_count == 1
    assert stats.serializer_row_failures is not None
    assert len(stats.serializer_row_failures) == 1
    assert len(fake_stream.requests) == 1
    request = fake_stream.requests[0]
    assert list(request.proto_rows.rows.serialized_rows) == [b"row-a", b"row-c"]


def test_write_splits_large_batch_into_multiple_appends(monkeypatch) -> None:
    fake_serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[b"aaaa", b"bbbb", b"cccc"],
            bad_rows=[],
            good_row_indices=[0, 1, 2],
        )
    )
    fake_stream = _FakeAppendRowsStream()
    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="stream",
        proto_schema=SimpleNamespace(),
        row_serializer=fake_serializer,
        append_rows_stream=fake_stream,
    )

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ErrorPolicy.classify_exception",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ErrorPolicy.classify_append_rows_response",
        lambda *args, **kwargs: None,
    )

    destination = BigQueryStorageWriteDestination(
        config=_config(
            append_max_rows=10,
            append_request_max_bytes=16,
            append_request_overhead_bytes=8,
            append_row_max_bytes=None,
        )
    )
    stats = destination.write([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    assert stats.ok is True
    assert stats.written_rows == 3
    assert stats.failed_rows == 0
    assert len(fake_stream.requests) == 2
    assert list(fake_stream.requests[0].proto_rows.rows.serialized_rows) == [b"aaaa", b"bbbb"]
    assert list(fake_stream.requests[1].proto_rows.rows.serialized_rows) == [b"cccc"]


def test_write_tiny_budget_splits_on_exact_payload_boundary(monkeypatch) -> None:
    fake_serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[b"aaaa", b"bbbb", b"cc"],
            bad_rows=[],
            good_row_indices=[0, 1, 2],
        )
    )
    fake_stream = _FakeAppendRowsStream()
    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="stream",
        proto_schema=SimpleNamespace(),
        row_serializer=fake_serializer,
        append_rows_stream=fake_stream,
    )

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ErrorPolicy.classify_exception",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ErrorPolicy.classify_append_rows_response",
        lambda *args, **kwargs: None,
    )

    destination = BigQueryStorageWriteDestination(
        config=_config(
            append_max_rows=10,
            append_request_max_bytes=16,
            append_request_overhead_bytes=8,
            append_row_max_bytes=None,
        )
    )
    stats = destination.write([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    assert stats.ok is True
    assert stats.written_rows == 3
    assert stats.failed_rows == 0
    assert len(fake_stream.requests) == 2
    # First request fills payload budget exactly: 4 + 4 == 8 bytes.
    assert list(fake_stream.requests[0].proto_rows.rows.serialized_rows) == [b"aaaa", b"bbbb"]
    assert list(fake_stream.requests[1].proto_rows.rows.serialized_rows) == [b"cc"]
