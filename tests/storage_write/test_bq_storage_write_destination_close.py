"""Smoke tests for BigQueryStorageWriteDestination.close() lifecycle.

Covers three behaviors that the other destination test files do not touch:

- Committed-stream close: tears down the stream + write client resources
  without issuing finalize / batch_commit (those are pending-only concerns).
- Pending-stream close after mark_job_succeeded(): transitions the session
  through FINALIZED -> COMMITTED and calls finalize_write_stream +
  batch_commit_write_streams exactly once.
- Resource-cleanup failure bookkeeping: when the AppendRowsStream.close()
  raises, close() itself must not propagate and close_failed() must flip
  to True so the pipeline layer can record the cleanup anomaly.

Each test drives close() via a real BigQueryStorageWriteDestination instance
with fake client/session objects; the session is installed directly on the
destination to avoid going through write() solely for setup.
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


class _FakeAppendRowsStream:
    def __init__(self, *, raise_on_close: bool = False) -> None:
        self.close_calls = 0
        self._raise_on_close = raise_on_close

    def close(self) -> None:
        self.close_calls += 1
        if self._raise_on_close:
            raise RuntimeError("stream close failure")


class _FakeWriteClient:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1

    def table_path(self, project: str, dataset: str, table: str) -> str:
        return f"projects/{project}/datasets/{dataset}/tables/{table}"


class _FakeSerializer:
    def serialize_rows(self, rows, *, row_max_bytes):
        return SerializationBatchResult(
            good_serialized_rows=[],
            bad_rows=[],
            good_row_indices=[],
        )


def _config(stream_mode: StreamMode = StreamMode.COMMITTED) -> StorageWriteConfig:
    return StorageWriteConfig(
        project_id="project",
        dataset_id="dataset",
        table_id="table",
        stream_mode=stream_mode,
        schema_supplier=lambda: [],
        proto_message_name="RowMessage",
    )


def _install_session(
    destination: BigQueryStorageWriteDestination,
    *,
    stream_name: str = "projects/p/datasets/d/tables/t/streams/s",
    next_offset: int = 0,
    raise_on_stream_close: bool = False,
) -> tuple[_FakeAppendRowsStream, _FakeWriteClient, StorageWriteSession]:
    fake_stream = _FakeAppendRowsStream(raise_on_close=raise_on_stream_close)
    fake_client = _FakeWriteClient()
    session = StorageWriteSession(
        write_client=fake_client,
        stream_name=stream_name,
        proto_schema=SimpleNamespace(),
        row_serializer=_FakeSerializer(),
        append_rows_stream=fake_stream,
    )
    session.next_offset = next_offset
    destination._session = session
    return fake_stream, fake_client, session


def test_close_committed_mode_tears_down_without_finalize_or_commit(monkeypatch) -> None:
    finalize_calls: list[str] = []
    commit_calls: list[tuple[str, list[str]]] = []

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.finalize_write_stream",
        lambda client, stream_name: finalize_calls.append(stream_name),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.batch_commit_write_streams",
        lambda client, parent, streams: commit_calls.append((parent, list(streams))),
    )

    destination = BigQueryStorageWriteDestination(config=_config(StreamMode.COMMITTED))
    fake_stream, fake_client, session = _install_session(destination, next_offset=5)

    destination.close()

    # Resource cleanup happened exactly once.
    assert fake_stream.close_calls == 1
    assert fake_client.close_calls == 1

    # Committed streams do NOT finalize / commit on close.
    assert finalize_calls == []
    assert commit_calls == []

    # The session is released and its lifecycle reached CLOSED.
    assert destination._session is None
    assert session.state == StreamLifecycleState.CLOSED

    # Cleanup was clean, so close_failed() stays False.
    assert destination.close_failed() is False

    # Idempotent: a second close() must be a no-op (no double cleanup).
    destination.close()
    assert fake_stream.close_calls == 1
    assert fake_client.close_calls == 1


def test_close_pending_mode_after_job_success_finalizes_and_commits(monkeypatch) -> None:
    finalize_calls: list[str] = []
    commit_calls: list[tuple[str, list[str]]] = []

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.finalize_write_stream",
        lambda client, stream_name: finalize_calls.append(stream_name),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.batch_commit_write_streams",
        lambda client, parent, streams: commit_calls.append((parent, list(streams))),
    )

    destination = BigQueryStorageWriteDestination(config=_config(StreamMode.PENDING))
    fake_stream, fake_client, session = _install_session(
        destination,
        stream_name="projects/project/datasets/dataset/tables/table/streams/pending-1",
        # next_offset > 0 required by _should_finalize_commit so the pending
        # commit path is actually taken.
        next_offset=3,
    )
    destination.mark_job_succeeded()

    destination.close()

    # Finalize + commit each ran exactly once, against the correct stream name
    # and table parent derived from the config.
    assert finalize_calls == [session.stream_name]
    assert commit_calls == [
        (
            "projects/project/datasets/dataset/tables/table",
            [session.stream_name],
        )
    ]

    # Resource cleanup still runs after finalize + commit.
    assert fake_stream.close_calls == 1
    assert fake_client.close_calls == 1

    # State transitioned OPEN -> FINALIZED -> COMMITTED, then CLOSED by close().
    assert session.state == StreamLifecycleState.CLOSED
    assert destination._session is None
    assert destination.close_failed() is False


def test_close_records_close_failure_when_stream_close_raises(monkeypatch) -> None:
    # No pending finalize/commit on a committed stream, so we only need to
    # ensure the helpers are not called. Patch them defensively anyway.
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.finalize_write_stream",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("finalize must not be called for committed streams")
        ),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.batch_commit_write_streams",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("commit must not be called for committed streams")
        ),
    )

    destination = BigQueryStorageWriteDestination(config=_config(StreamMode.COMMITTED))
    fake_stream, fake_client, session = _install_session(
        destination, raise_on_stream_close=True
    )

    # close() must swallow stream.close() errors and record the failure.
    destination.close()

    assert fake_stream.close_calls == 1
    # The write client should still be closed even if the stream close raised.
    assert fake_client.close_calls == 1
    assert destination.close_failed() is True
    assert destination._session is None
    assert session.state == StreamLifecycleState.CLOSED
