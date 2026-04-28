from types import SimpleNamespace

from adapters.bigquery.storage_write.bq_storage_write_destination import (
    BigQueryStorageWriteDestination,
)
from adapters.bigquery.storage_write.bq_storage_write_models import (
    DestinationWriteStats,
    StorageWriteConfig,
    StorageWriteSession,
    StreamMode,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.write_api_error import (
    BigQueryStorageWriteError,
)
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

    def serialize_rows(self, rows, *, row_max_bytes):
        return self._result


def _config(**overrides) -> StorageWriteConfig:
    return StorageWriteConfig(
        project_id=overrides.get("project_id", "project"),
        dataset_id=overrides.get("dataset_id", "dataset"),
        table_id=overrides.get("table_id", "table"),
        stream_mode=overrides.get("stream_mode", StreamMode.COMMITTED),
        schema_supplier=overrides.get("schema_supplier", lambda: []),
        proto_message_name=overrides.get("proto_message_name", "RowMessage"),
        append_max_rows=overrides.get("append_max_rows", 10),
        append_request_max_bytes=overrides.get("append_request_max_bytes", 1_000_000),
        append_request_overhead_bytes=overrides.get("append_request_overhead_bytes", 1_000),
        append_row_max_bytes=overrides.get("append_row_max_bytes", 256_000),
    )


def _install_session(
    monkeypatch,
    *,
    serializer_result: SerializationBatchResult,
) -> _FakeAppendRowsStream:
    fake_stream = _FakeAppendRowsStream()
    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="projects/p/datasets/d/tables/t/streams/s",
        proto_schema=SimpleNamespace(),
        row_serializer=_FakeSerializer(serializer_result),
        append_rows_stream=fake_stream,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )
    return fake_stream


def _assert_destination_invariants(stats: DestinationWriteStats) -> None:
    assert stats.total_rows == stats.resolve_write_limit_passed + stats.resolve_write_limit_failed
    if stats.resolve_write_limit_passed > 0:
        assert stats.serializer_attempted_rows == stats.resolve_write_limit_passed
        assert stats.serializer_attempted_rows == stats.serializer_rows_passed + stats.serializer_rows_failed
        assert stats.chunk_planner_attempted == stats.serializer_rows_passed
    assert (
        stats.append_rows_send_attempted
        == stats.append_rows_send_passed
        + stats.append_rows_send_failed
        + stats.skipped_already_exists_rows
    )
    assert stats.total_written_rows == stats.append_rows_send_passed
    assert (
        stats.total_failed_rows
        == stats.total_rows - stats.total_written_rows - stats.skipped_already_exists_rows
    )
    assert len(stats.row_error_bad_rows or []) + len(stats.row_error_good_rows or []) <= (
        stats.append_rows_send_failed
    )
    assert stats.row_error_bad_count == len(stats.row_error_bad_rows or [])
    assert stats.row_error_good_count == len(stats.row_error_good_rows or [])


def test_stats_flow_resolve_limits_failure() -> None:
    destination = BigQueryStorageWriteDestination(config=_config(append_request_max_bytes=0))
    stats = destination.write([{"id": "a"}, {"id": "b"}])

    assert stats.ok is False
    assert stats.error is not None
    assert stats.error.category == ErrorCategory.INVALID_ARGUMENT
    assert stats.total_rows == 2
    assert stats.resolve_write_limit_passed == 0
    assert stats.resolve_write_limit_failed == 2
    assert stats.serializer_attempted_rows == 0
    assert stats.append_rows_send_attempted == 0
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 2
    _assert_destination_invariants(stats)


def test_stats_flow_serializer_all_bad_rows(monkeypatch) -> None:
    _install_session(
        monkeypatch,
        serializer_result=SerializationBatchResult(
            good_serialized_rows=[],
            bad_rows=[
                RowSerializationError(
                    row_index=0,
                    reason_enum="serialization_missing_required",
                    error_message="Missing field",
                ),
                RowSerializationError(
                    row_index=1,
                    reason_enum="serialization_type_error",
                    error_message="Expected int",
                ),
            ],
            good_row_indices=[],
        ),
    )
    destination = BigQueryStorageWriteDestination(config=_config())
    stats = destination.write([{"id": "a"}, {"id": "b"}])

    assert stats.ok is False
    assert stats.error is not None
    assert stats.serializer_attempted_rows == 2
    assert stats.serializer_rows_passed == 0
    assert stats.serializer_rows_failed == 2
    assert stats.chunk_planner_attempted == 0
    assert stats.derived_chunk_count == 0
    assert stats.append_rows_send_attempted == 0
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 2
    _assert_destination_invariants(stats)


def test_stats_flow_serializer_mixed_rows_and_success(monkeypatch) -> None:
    fake_stream = _install_session(
        monkeypatch,
        serializer_result=SerializationBatchResult(
            good_serialized_rows=[b"r1", b"r3"],
            bad_rows=[
                RowSerializationError(
                    row_index=1,
                    reason_enum="serialization_type_error",
                    error_message="bad type",
                )
            ],
            good_row_indices=[0, 2],
        ),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        lambda *args, **kwargs: None,
    )
    destination = BigQueryStorageWriteDestination(config=_config(append_max_rows=10))
    stats = destination.write([{"id": 1}, {"id": "bad"}, {"id": 3}])

    assert stats.ok is True
    assert stats.total_rows == 3
    assert stats.serializer_attempted_rows == 3
    assert stats.serializer_rows_passed == 2
    assert stats.serializer_rows_failed == 1
    assert stats.chunk_planner_attempted == 2
    assert stats.derived_chunk_count == 1
    assert stats.derived_chunks_len == [2]
    assert stats.append_rows_send_attempted == 2
    assert stats.append_rows_send_passed == 2
    assert stats.append_rows_send_failed == 0
    assert stats.total_written_rows == 2
    assert stats.total_failed_rows == 1
    assert len(fake_stream.requests) == 1
    _assert_destination_invariants(stats)


def test_stats_flow_row_errors_accumulate_across_chunks(monkeypatch) -> None:
    _install_session(
        monkeypatch,
        serializer_result=SerializationBatchResult(
            good_serialized_rows=[b"a", b"b", b"c", b"d"],
            bad_rows=[],
            good_row_indices=[0, 1, 2, 3],
        ),
    )
    row_error = BigQueryStorageWriteError(
        "row errors",
        retryable=False,
        send_to_dlq=True,
        category=ErrorCategory.ROW_ERRORS,
        row_errors=[SimpleNamespace(index=0)],
    )
    classify_calls = {"n": 0}

    def fake_classify_result(event, **kwargs):
        if isinstance(event, Exception):
            return None
        classify_calls["n"] += 1
        return row_error

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        fake_classify_result,
    )
    destination = BigQueryStorageWriteDestination(config=_config(append_max_rows=2))
    rows = [{"id": "r0"}, {"id": "r1"}, {"id": "r2"}, {"id": "r3"}]
    stats = destination.write(rows)

    assert stats.ok is False
    assert stats.error is not None
    assert stats.error.category == ErrorCategory.ROW_ERRORS
    assert stats.derived_chunk_count == 2
    assert stats.derived_chunks_len == [2, 2]
    assert stats.append_rows_send_attempted == 4
    assert stats.append_rows_send_passed == 0
    assert stats.append_rows_send_failed == 4
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 4
    assert stats.row_error_bad_count == 2
    assert stats.row_error_good_count == 2
    assert stats.row_error_bad_rows == [{"id": "r0"}, {"id": "r2"}]
    assert stats.row_error_good_rows == [{"id": "r1"}, {"id": "r3"}]
    assert classify_calls["n"] == 2
    _assert_destination_invariants(stats)


def test_stats_flow_terminal_break_before_all_chunks_attempted(monkeypatch) -> None:
    fake_stream = _install_session(
        monkeypatch,
        serializer_result=SerializationBatchResult(
            good_serialized_rows=[b"a", b"b", b"c"],
            bad_rows=[],
            good_row_indices=[0, 1, 2],
        ),
    )
    terminal_error = BigQueryStorageWriteError(
        "transport failure",
        retryable=True,
        needs_reset=True,
        category=ErrorCategory.TRANSPORT,
    )
    classify_calls = {"n": 0}

    def fake_classify_result(event, **kwargs):
        if isinstance(event, Exception):
            return None
        classify_calls["n"] += 1
        return terminal_error

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        fake_classify_result,
    )
    destination = BigQueryStorageWriteDestination(config=_config(append_max_rows=1))
    stats = destination.write([{"id": "r0"}, {"id": "r1"}, {"id": "r2"}])

    assert stats.ok is False
    assert stats.error is terminal_error
    assert stats.chunk_planner_attempted == 3
    assert stats.derived_chunk_count == 3
    assert stats.derived_chunks_len == [1, 1, 1]
    assert stats.append_rows_send_attempted == 1
    assert stats.append_rows_send_attempted < stats.chunk_planner_attempted
    assert stats.append_rows_send_failed == 1
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 3
    assert len(fake_stream.requests) == 1
    assert classify_calls["n"] == 1
    _assert_destination_invariants(stats)


def test_aggregate_stats_logs_warning_when_failed_rows_clamp_triggers(caplog) -> None:
    destination = BigQueryStorageWriteDestination(config=_config())
    caplog.set_level("WARNING", logger="adapters.bigquery.storage_write.bq_storage_write_destination")

    stats = destination._aggregate_write_stats(
        ok=True,
        total_rows=1,
        append_rows_send_passed=1,
        skipped_already_exists_rows=1,
    )

    assert stats.total_failed_rows == 0
    warning_messages = [rec.getMessage() for rec in caplog.records if rec.levelname == "WARNING"]
    assert any("computed total_failed_rows was negative" in msg for msg in warning_messages)
