"""
Covers the ALREADY_EXISTS loop-continuation contract in
BigQueryStorageWriteDestination.write():

- ALREADY_EXISTS is a benign server-side no-op (offset already committed /
  pending-buffered). The write() loop must NOT break out when it sees one;
  it must advance the offset, log an INFO skip line, bump the
  `skipped_already_exists_rows` counter, and keep attempting remaining chunks.
- `total_written_rows` must NOT be incremented for a skipped chunk (this call did
  not perform that append).
- `total_failed_rows` must NOT be incremented either (rows are safe on the server).
- The final stats must remain `ok=True` if no other error is terminal.

Three positional scenarios are exercised: first chunk, middle chunk, last chunk.
A fourth test pins the degenerate "every chunk is ALREADY_EXISTS" case.
"""

import logging
from types import SimpleNamespace

from google.rpc import code_pb2

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


_DESTINATION_LOGGER = "adapters.bigquery.storage_write.bq_storage_write_destination"


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


def _already_exists_policy_error(stream: str) -> BigQueryStorageWriteError:
    # Mirrors the shape produced by ExtendedErrorPolicy.grpc_code_mapping for
    # code_pb2.ALREADY_EXISTS so the destination sees a realistic object.
    return BigQueryStorageWriteError(
        "BigQuery Storage Write append: offset already committed",
        stream=stream,
        status_code=code_pb2.ALREADY_EXISTS,
        status_message="Already Exists",
        row_errors=[],
        original_exception=None,
        retryable=False,
        needs_reset=False,
        offset_alignment=False,
        fatal_state=False,
        advance_offset=True,
        send_to_dlq=False,
        needs_continue_loop=True,
        category=ErrorCategory.ALREADY_EXISTS,
    )


def _config(**overrides) -> StorageWriteConfig:
    return StorageWriteConfig(
        project_id=overrides.get("project_id", "project"),
        dataset_id=overrides.get("dataset_id", "dataset"),
        table_id=overrides.get("table_id", "table"),
        stream_mode=overrides.get("stream_mode", StreamMode.COMMITTED),
        schema_supplier=overrides.get("schema_supplier", lambda: []),
        proto_message_name=overrides.get("proto_message_name", "RowMessage"),
        append_max_rows=overrides.get("append_max_rows", 1),
        append_request_max_bytes=overrides.get("append_request_max_bytes", 1_000_000),
        append_request_overhead_bytes=overrides.get("append_request_overhead_bytes", 1_000),
        append_row_max_bytes=overrides.get("append_row_max_bytes", 256_000),
    )


def _build_destination_three_chunks(
    monkeypatch, *, already_exists_chunk_indices: set[int]
):
    """
    Build a destination whose serializer emits 3 one-byte rows and whose config
    forces `append_max_rows=1` so the planner splits into exactly 3 one-row
    chunks. The patched classifier returns an
    ALREADY_EXISTS policy error for the chunk indices in
    `already_exists_chunk_indices` (by call order) and None for all others.
    """
    fake_serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[b"a", b"b", b"c"],
            bad_rows=[],
            good_row_indices=[0, 1, 2],
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

    call_counter = {"n": 0}

    def fake_classify_result(event, *, stream, stream_mode):
        if isinstance(event, Exception):
            return None
        index = call_counter["n"]
        call_counter["n"] += 1
        if index in already_exists_chunk_indices:
            return _already_exists_policy_error(stream)
        return None

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        fake_classify_result,
    )

    destination = BigQueryStorageWriteDestination(config=_config(append_max_rows=1))
    return destination, fake_stream, fake_session


def _assert_skip_log_for_chunk(caplog, chunk_index: int) -> None:
    marker = f"chunk {chunk_index} skipped"
    matching = [
        rec
        for rec in caplog.records
        if rec.name == _DESTINATION_LOGGER
        and rec.levelno == logging.INFO
        and marker in rec.getMessage()
    ]
    assert len(matching) == 1, (
        f"Expected exactly one INFO skip log for chunk {chunk_index}; "
        f"got {[rec.getMessage() for rec in matching]}"
    )
    # Sanity: the log should mention the category name so operators can grep it.
    assert ErrorCategory.ALREADY_EXISTS.value in matching[0].getMessage()


def test_already_exists_on_first_chunk_skips_and_continues(monkeypatch, caplog) -> None:
    destination, fake_stream, fake_session = _build_destination_three_chunks(
        monkeypatch, already_exists_chunk_indices={0}
    )
    caplog.set_level(logging.INFO, logger=_DESTINATION_LOGGER)

    stats = destination.write([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    # All three chunks were attempted on the wire — the first being
    # ALREADY_EXISTS must NOT short-circuit the loop.
    assert len(fake_stream.requests) == 3
    # Offsets: chunk 0 used offset 0 and policy advanced it to 1; chunks 1/2
    # used offsets 1 and 2 normally.
    assert [req.offset for req in fake_stream.requests] == [0, 1, 2]
    assert fake_session.next_offset == 3

    assert stats.ok is True
    assert stats.error is None
    assert stats.total_rows == 3
    assert stats.total_written_rows == 2
    assert stats.skipped_already_exists_rows == 1
    assert stats.total_failed_rows == 0
    # Accounting invariant.
    assert (
        stats.total_rows
        == stats.total_written_rows + stats.total_failed_rows + stats.skipped_already_exists_rows
    )

    _assert_skip_log_for_chunk(caplog, chunk_index=0)


def test_already_exists_on_middle_chunk_skips_and_continues(monkeypatch, caplog) -> None:
    destination, fake_stream, fake_session = _build_destination_three_chunks(
        monkeypatch, already_exists_chunk_indices={1}
    )
    caplog.set_level(logging.INFO, logger=_DESTINATION_LOGGER)

    stats = destination.write([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    assert len(fake_stream.requests) == 3
    assert [req.offset for req in fake_stream.requests] == [0, 1, 2]
    assert fake_session.next_offset == 3

    assert stats.ok is True
    assert stats.error is None
    assert stats.total_rows == 3
    assert stats.total_written_rows == 2
    assert stats.skipped_already_exists_rows == 1
    assert stats.total_failed_rows == 0
    assert (
        stats.total_rows
        == stats.total_written_rows + stats.total_failed_rows + stats.skipped_already_exists_rows
    )

    _assert_skip_log_for_chunk(caplog, chunk_index=1)


def test_already_exists_on_last_chunk_still_returns_ok(monkeypatch, caplog) -> None:
    destination, fake_stream, fake_session = _build_destination_three_chunks(
        monkeypatch, already_exists_chunk_indices={2}
    )
    caplog.set_level(logging.INFO, logger=_DESTINATION_LOGGER)

    stats = destination.write([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    assert len(fake_stream.requests) == 3
    assert [req.offset for req in fake_stream.requests] == [0, 1, 2]
    assert fake_session.next_offset == 3

    assert stats.ok is True
    assert stats.error is None
    assert stats.total_rows == 3
    assert stats.total_written_rows == 2
    assert stats.skipped_already_exists_rows == 1
    assert stats.total_failed_rows == 0
    assert (
        stats.total_rows
        == stats.total_written_rows + stats.total_failed_rows + stats.skipped_already_exists_rows
    )

    _assert_skip_log_for_chunk(caplog, chunk_index=2)


def test_already_exists_on_every_chunk_is_still_ok_with_zero_writes(monkeypatch, caplog) -> None:
    destination, fake_stream, fake_session = _build_destination_three_chunks(
        monkeypatch, already_exists_chunk_indices={0, 1, 2}
    )
    caplog.set_level(logging.INFO, logger=_DESTINATION_LOGGER)

    stats = destination.write([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    # Every chunk was still sent; the loop never broke out.
    assert len(fake_stream.requests) == 3
    assert [req.offset for req in fake_stream.requests] == [0, 1, 2]
    assert fake_session.next_offset == 3

    # No data was actually written by THIS call (server already had it), but
    # nothing failed either, so the call remains ok=True with zero writes and
    # three skipped rows for reconciliation.
    assert stats.ok is True
    assert stats.error is None
    assert stats.total_rows == 3
    assert stats.total_written_rows == 0
    assert stats.skipped_already_exists_rows == 3
    assert stats.total_failed_rows == 0
    assert (
        stats.total_rows
        == stats.total_written_rows + stats.total_failed_rows + stats.skipped_already_exists_rows
    )

    # One skip log per chunk, all at INFO.
    for idx in (0, 1, 2):
        _assert_skip_log_for_chunk(caplog, chunk_index=idx)
