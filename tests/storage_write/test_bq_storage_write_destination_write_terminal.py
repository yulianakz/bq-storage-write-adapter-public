"""Smoke tests for BigQueryStorageWriteDestination.write() stats contract.

Covers two gaps left open by the ALREADY_EXISTS / serializer-diagnostics /
session-swap-race suites:

- Empty-rows short-circuit: write([]) must return ok=True with zero counts
  and must NOT lazily create a session or touch the serializer.
- Non-row-errors terminal policy error: the destination must stop issuing
  further AppendRowsRequests, preserve total_written_rows from earlier chunks (here
  zero, since the very first chunk fails), and surface the policy error on
  DestinationWriteStats.error with ok=False.
"""

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
from adapters.bigquery.storage_write.retry_handler.write_api_error import (
    BigQueryStorageWriteError,
)
from adapters.bigquery.storage_write.row_serializer.serializer_models import (
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
        self.calls = 0

    def serialize_rows(self, rows, *, row_max_bytes):
        self.calls += 1
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


def test_write_translates_invalid_limit_config_into_stats(monkeypatch) -> None:
    # If limit config is invalid, write() must not raise; it should return
    # structured stats with a non-retryable error.
    destination = BigQueryStorageWriteDestination(
        config=_config(append_request_max_bytes=0)
    )

    stats = destination.write([{"id": "a"}])

    assert stats.ok is False
    assert stats.total_rows == 1
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 1
    assert stats.error is not None
    assert stats.error.retryable is False
    assert stats.error.category == ErrorCategory.INVALID_ARGUMENT
    assert "append_request_max_bytes must be > 0" in str(stats.error)


def test_write_caches_resolved_limits(monkeypatch) -> None:
    calls = {"n": 0}

    def fake_resolve_write_limits(config):
        calls["n"] += 1
        # Only the fields used by write() matter here.
        return type(
            "_Limits",
            (object,),
            {
                "row_max_bytes": 256_000,
                "max_rows": 10,
                "request_payload_budget_bytes": 1_000_000,
            },
        )()

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.resolve_write_limits",
        fake_resolve_write_limits,
    )

    # Avoid needing a real session; both writes can short-circuit on serializer returning nothing.
    class _FakeSerializer:
        def serialize_rows(self, rows, *, row_max_bytes):
            return SerializationBatchResult(good_serialized_rows=[], bad_rows=[], good_row_indices=[])

    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="projects/p/datasets/d/tables/t/streams/s",
        proto_schema=SimpleNamespace(),
        row_serializer=_FakeSerializer(),
        append_rows_stream=_FakeAppendRowsStream(),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )

    destination = BigQueryStorageWriteDestination(config=_config())

    destination.write([{"id": "a"}])
    destination.write([{"id": "b"}])

    assert calls["n"] == 1


def test_write_empty_rows_returns_ok_without_creating_session(monkeypatch) -> None:
    session_factory_calls = {"n": 0}

    def fake_create_session(config):
        session_factory_calls["n"] += 1
        raise AssertionError("write([]) must not create a session")

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        fake_create_session,
    )

    destination = BigQueryStorageWriteDestination(config=_config())

    stats = destination.write([])

    assert session_factory_calls["n"] == 0
    assert stats.ok is True
    assert stats.error is None
    assert stats.total_rows == 0
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 0
    assert stats.skipped_already_exists_rows == 0
    assert stats.serializer_rows_failed == 0
    assert stats.serializer_row_failures is None
    assert stats.stream_mode == StreamMode.COMMITTED.value


def test_write_surfaces_non_retryable_terminal_policy_error_in_stats(monkeypatch) -> None:
    fake_serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[b"row-a", b"row-b"],
            bad_rows=[],
            good_row_indices=[0, 1],
        )
    )
    fake_stream = _FakeAppendRowsStream()
    fake_session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name="projects/p/datasets/d/tables/t/streams/s",
        proto_schema=SimpleNamespace(),
        row_serializer=fake_serializer,
        append_rows_stream=fake_stream,
    )

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        lambda config: fake_session,
    )

    terminal_error = BigQueryStorageWriteError(
        "BigQuery Storage Write invalid argument",
        stream=fake_session.stream_name,
        status_code=3,
        retryable=False,
        needs_reset=False,
        fatal_state=True,
        advance_offset=False,
        send_to_dlq=True,
        category=ErrorCategory.INVALID_ARGUMENT,
    )

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        lambda event, **kwargs: terminal_error if not isinstance(event, Exception) else None,
    )

    destination = BigQueryStorageWriteDestination(config=_config())
    stats = destination.write([{"id": "a"}, {"id": "b"}])

    # Exactly one AppendRowsRequest was attempted; the terminal error breaks the chunk loop.
    assert len(fake_stream.requests) == 1

    assert stats.ok is False
    assert stats.error is terminal_error
    assert stats.total_rows == 2
    assert stats.total_written_rows == 0
    assert stats.total_failed_rows == 2
    assert stats.skipped_already_exists_rows == 0
    # No row_errors were attached to this terminal, so the mapping stays empty.
    assert stats.row_error_bad_rows is None
    assert stats.row_error_good_rows is None
    # Offset must NOT advance on a terminal non-advance error.
    assert fake_session.next_offset == 0
    assert stats.stream_mode == StreamMode.COMMITTED.value
