"""End-to-end happy-path flow tests for BigQueryStorageWriteDestination.

These tests intentionally drive write() and close() without monkey-patching
`ErrorPolicy`. That exercises the real classification (`append_result` present
-> None), the real write-limits resolution, the real chunk planner, and the
real lock-protected offset bookkeeping in _append_chunk — the pieces the
narrower per-scenario tests each stub out individually.

Two flows:

1. Committed stream: lazy session creation -> multi-chunk write across a tiny
   payload budget -> clean `DestinationWriteStats` -> close() tears down the
   session resources without issuing finalize/commit.
2. Pending stream: write -> mark_job_succeeded() -> close() runs the full
   pending finalize + batch_commit handshake and still releases resources.

AppendRowsStream and WriteClient are fakes so no real gRPC traffic happens,
but the AppendRowsResponse shape (`SimpleNamespace(append_result=...)`) is
real enough to satisfy `ErrorPolicy.classify_append_rows_response`.
"""

from types import SimpleNamespace

from adapters.bigquery.storage_write.bq_storage_write_destination import (
    BigQueryStorageWriteDestination,
)
from adapters.bigquery.storage_write.bq_storage_write_models import (
    StorageWriteConfig,
    StorageWriteSession,
    StreamLifecycleState,
    StreamMode,
)
from adapters.bigquery.storage_write.row_serializer.serializer_models import (
    SerializationBatchResult,
)


class _FakeFuture:
    def __init__(self, response) -> None:
        self._response = response

    def result(self, timeout=None):
        return self._response


class _FakeAppendRowsStream:
    """AppendRowsStream fake that echoes success responses.

    Each `send(request)` records the request and returns a future whose
    `.result()` carries an `append_result` — enough for the real
    `ErrorPolicy.classify_append_rows_response` to return None.
    """

    def __init__(self) -> None:
        self.requests: list = []
        self.close_calls = 0

    def send(self, request):
        self.requests.append(request)
        response = SimpleNamespace(
            append_result=SimpleNamespace(offset=request.offset),
            row_errors=None,
            error=None,
        )
        return _FakeFuture(response)

    def close(self) -> None:
        self.close_calls += 1


class _FakeWriteClient:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1

    def table_path(self, project: str, dataset: str, table: str) -> str:
        return f"projects/{project}/datasets/{dataset}/tables/{table}"


class _FakeSerializer:
    """Deterministic serializer: keeps every input row as a fixed 4-byte blob.

    4 bytes per row lines up with the tiny payload budget the test chooses,
    so we can force a multi-chunk split deterministically without depending
    on the real proto serializer's exact encoding.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[tuple[dict, ...], int | None]] = []

    def serialize_rows(self, rows, *, row_max_bytes):
        rows_tuple = tuple(rows)
        self.calls.append((rows_tuple, row_max_bytes))
        serialized = [b"r___" for _ in rows_tuple]
        return SerializationBatchResult(
            good_serialized_rows=serialized,
            bad_rows=[],
            good_row_indices=list(range(len(rows_tuple))),
        )


def _config(stream_mode: StreamMode, **overrides) -> StorageWriteConfig:
    # Tiny request budget (16 - 8 = 8 payload bytes) so 3 x 4-byte rows split
    # into two AppendRowsRequests [row0, row1] + [row2].
    return StorageWriteConfig(
        project_id=overrides.get("project_id", "project"),
        dataset_id=overrides.get("dataset_id", "dataset"),
        table_id=overrides.get("table_id", "table"),
        stream_mode=stream_mode,
        schema_supplier=overrides.get("schema_supplier", lambda: []),
        proto_message_name=overrides.get("proto_message_name", "RowMessage"),
        append_max_rows=overrides.get("append_max_rows", 10),
        append_request_max_bytes=overrides.get("append_request_max_bytes", 16),
        append_request_overhead_bytes=overrides.get("append_request_overhead_bytes", 8),
        append_row_max_bytes=overrides.get("append_row_max_bytes", None),
    )


def _install_fake_session_factory(
    monkeypatch, *, stream_name: str
) -> tuple[_FakeAppendRowsStream, _FakeWriteClient, _FakeSerializer, dict]:
    fake_stream = _FakeAppendRowsStream()
    fake_client = _FakeWriteClient()
    fake_serializer = _FakeSerializer()
    session_container: dict = {"session": None, "factory_calls": 0}

    def fake_create_session(config):
        session_container["factory_calls"] += 1
        session = StorageWriteSession(
            write_client=fake_client,
            stream_name=stream_name,
            proto_schema=SimpleNamespace(),
            row_serializer=fake_serializer,
            append_rows_stream=fake_stream,
        )
        session_container["session"] = session
        return session

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.create_storage_write_session",
        fake_create_session,
    )
    return fake_stream, fake_client, fake_serializer, session_container


def test_committed_stream_full_flow_write_then_close(monkeypatch) -> None:
    stream_name = "projects/project/datasets/dataset/tables/table/streams/commit-1"
    fake_stream, fake_client, fake_serializer, container = _install_fake_session_factory(
        monkeypatch, stream_name=stream_name
    )

    # Guard against pending-only code paths running in committed mode.
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.finalize_write_stream",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("finalize must not run on committed streams")
        ),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.batch_commit_write_streams",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("batch_commit must not run on committed streams")
        ),
    )

    destination = BigQueryStorageWriteDestination(config=_config(StreamMode.COMMITTED))

    # Session is lazy: nothing is created until the first write().
    assert container["factory_calls"] == 0
    assert destination._session is None

    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    stats = destination.write(rows)

    # Session was created exactly once.
    assert container["factory_calls"] == 1
    session = container["session"]
    assert destination._session is session

    # Serializer saw the full input batch with the derived row_max_bytes (the
    # resolved payload budget == request_max - overhead == 16 - 8 == 8).
    assert len(fake_serializer.calls) == 1
    serialize_call_rows, serialize_call_row_max = fake_serializer.calls[0]
    assert serialize_call_rows == tuple(rows)
    assert serialize_call_row_max == 8

    # Planner split 3 x 4-byte rows into 2 AppendRowsRequests at payload bytes
    # 8 and 4 respectively. Offsets are consecutive across chunks.
    assert len(fake_stream.requests) == 2
    first_req, second_req = fake_stream.requests
    assert first_req.offset == 0
    assert second_req.offset == 2
    assert list(first_req.proto_rows.rows.serialized_rows) == [b"r___", b"r___"]
    assert list(second_req.proto_rows.rows.serialized_rows) == [b"r___"]

    # next_offset advanced once per successful chunk.
    assert session.next_offset == 3
    assert session.state == StreamLifecycleState.OPEN

    # DestinationWriteStats is clean on a full success.
    assert stats.ok is True
    assert stats.error is None
    assert stats.attempted_rows == 3
    assert stats.written_rows == 3
    assert stats.failed_rows == 0
    assert stats.skipped_already_exists_rows == 0
    assert stats.serializer_row_failure_count == 0
    assert stats.serializer_row_failures is None
    assert stats.row_error_bad_rows is None
    assert stats.row_error_good_rows is None
    assert stats.stream_mode == StreamMode.COMMITTED.value

    # close() releases resources without finalize/commit on a committed stream.
    destination.close()

    assert fake_stream.close_calls == 1
    assert fake_client.close_calls == 1
    assert destination._session is None
    assert session.state == StreamLifecycleState.CLOSED
    assert destination.close_failed() is False

    # close() is idempotent.
    destination.close()
    assert fake_stream.close_calls == 1
    assert fake_client.close_calls == 1


def test_pending_stream_full_flow_write_mark_succeeded_then_close(monkeypatch) -> None:
    stream_name = "projects/project/datasets/dataset/tables/table/streams/pending-1"
    fake_stream, fake_client, fake_serializer, container = _install_fake_session_factory(
        monkeypatch, stream_name=stream_name
    )

    finalize_calls: list[str] = []
    commit_calls: list[tuple[str, list[str]]] = []

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.finalize_write_stream",
        lambda client, sname: finalize_calls.append(sname),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.batch_commit_write_streams",
        lambda client, parent, streams: commit_calls.append((parent, list(streams))),
    )

    destination = BigQueryStorageWriteDestination(config=_config(StreamMode.PENDING))

    rows = [{"id": "a"}, {"id": "b"}]
    stats = destination.write(rows)

    # Write succeeded end-to-end.
    session = container["session"]
    assert stats.ok is True
    assert stats.error is None
    assert stats.attempted_rows == 2
    assert stats.written_rows == 2
    assert stats.failed_rows == 0
    assert stats.stream_mode == StreamMode.PENDING.value
    assert session.next_offset == 2
    assert session.state == StreamLifecycleState.OPEN

    # Pending streams only finalize + commit when the job is marked succeeded.
    # Calling close() before mark_job_succeeded would skip the handshake; pin
    # the expected lifecycle by calling it here.
    destination.mark_job_succeeded()
    destination.close()

    # Full finalize + batch_commit handshake ran exactly once in order.
    assert finalize_calls == [stream_name]
    assert commit_calls == [
        ("projects/project/datasets/dataset/tables/table", [stream_name]),
    ]

    # Session lifecycle walked OPEN -> FINALIZED -> COMMITTED before CLOSED.
    assert session.state == StreamLifecycleState.CLOSED

    # Resource cleanup still happened after the handshake.
    assert fake_stream.close_calls == 1
    assert fake_client.close_calls == 1
    assert destination._session is None
    assert destination.close_failed() is False
