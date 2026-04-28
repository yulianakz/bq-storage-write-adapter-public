"""
Covers the session-swap race guard inside
`BigQueryStorageWriteDestination._append_chunk`.

Scenario reproduced:
  Between `session = self._get_or_create_session()` (outside the write lock)
  and the `with self._write_lock:` block, another code path (e.g. a previous
  chunk's policy-driven `_reset_session`) swaps `self._session` out from under
  us. Without the guard the append would go to a stale / closed session.

The `while True:` loop plus the `if session is not self._session: continue`
branch exists to re-resolve to the fresh session and retry. This branch is
rarely hit in the happy path, but it is load-bearing for correctness under
concurrent reset + append workloads, so we pin it down with explicit tests.

The race is simulated deterministically by monkey-patching
`_get_or_create_session` so that:
  - call #1 (from `_serialize_with_diagnostics`): installs `fresh_session` on
    `self._session` and returns it (serialization uses the fresh serializer);
  - call #2 (first `_append_chunk` iteration): returns `stale_session` while
    `self._session` is still `fresh_session` -> guard fires, `continue`;
  - call #3 (retry iteration): returns `fresh_session`, which matches
    `self._session` -> append proceeds on the fresh stream.
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
        append_max_rows=overrides.get("append_max_rows", 1),
        append_request_max_bytes=overrides.get("append_request_max_bytes", 1_000_000),
        append_request_overhead_bytes=overrides.get("append_request_overhead_bytes", 1_000),
        append_row_max_bytes=overrides.get("append_row_max_bytes", 256_000),
    )


def _build_session(stream_name: str) -> tuple[StorageWriteSession, _FakeAppendRowsStream]:
    fake_stream = _FakeAppendRowsStream()
    serializer = _FakeSerializer(
        SerializationBatchResult(
            good_serialized_rows=[b"a"],
            bad_rows=[],
            good_row_indices=[0],
        )
    )
    session = StorageWriteSession(
        write_client=_FakeWriteClient(),
        stream_name=stream_name,
        proto_schema=SimpleNamespace(),
        row_serializer=serializer,
        append_rows_stream=fake_stream,
    )
    return session, fake_stream


def test_session_swap_between_resolve_and_lock_acquire_triggers_retry(monkeypatch) -> None:
    stale_session, stale_stream = _build_session("projects/p/datasets/d/tables/t/streams/stale")
    fresh_session, fresh_stream = _build_session("projects/p/datasets/d/tables/t/streams/fresh")

    destination = BigQueryStorageWriteDestination(config=_config())

    calls = {"n": 0}

    def fake_get_or_create_session(self):
        calls["n"] += 1
        n = calls["n"]
        if n == 1:
            # _serialize_with_diagnostics: install the fresh session and use its serializer.
            self._session = fresh_session
            return fresh_session
        if n == 2:
            # First _append_chunk iteration: return a stale session while
            # self._session is still the fresh one. Guard must fire.
            return stale_session
        # n >= 3: retry iteration. Return the fresh session so it matches self._session.
        return fresh_session

    monkeypatch.setattr(
        BigQueryStorageWriteDestination,
        "_get_or_create_session",
        fake_get_or_create_session,
    )
    # Make sure the real ExtendedErrorPolicy doesn't interfere: the fake stream never
    # raises and response.append_result is a truthy SimpleNamespace so the
    # real classifier returns None anyway, but patching
    # keeps this test hermetic.
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        lambda *args, **kwargs: None,
    )

    stats = destination.write([{"id": "a"}])

    # _get_or_create_session was called 3 times: once for serialization, then
    # twice inside _append_chunk (first=stale, then retry=fresh).
    assert calls["n"] == 3

    # The stale session must NEVER receive a send — that's the whole point
    # of the guard.
    assert stale_stream.requests == []

    # The fresh session received exactly one append at offset 0.
    assert len(fresh_stream.requests) == 1
    assert fresh_stream.requests[0].offset == 0

    # Offset bookkeeping happens on the fresh session only.
    assert fresh_session.next_offset == 1
    assert stale_session.next_offset == 0

    # End-to-end write() remains ok.
    assert stats.ok is True
    assert stats.error is None
    assert stats.total_rows == 1
    assert stats.total_written_rows == 1
    assert stats.total_failed_rows == 0
    assert stats.skipped_already_exists_rows == 0


def test_no_session_swap_appends_exactly_once_without_retry(monkeypatch) -> None:
    """
    Happy-path counterpart: when `self._session` never swaps, the `while True:`
    loop must enter exactly once and fall through to `break`. Pins down that
    the guard adds zero overhead on the hot path.
    """
    fresh_session, fresh_stream = _build_session("projects/p/datasets/d/tables/t/streams/only")

    destination = BigQueryStorageWriteDestination(config=_config())

    calls = {"n": 0}

    def fake_get_or_create_session(self):
        calls["n"] += 1
        self._session = fresh_session
        return fresh_session

    monkeypatch.setattr(
        BigQueryStorageWriteDestination,
        "_get_or_create_session",
        fake_get_or_create_session,
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.bq_storage_write_destination.ExtendedErrorPolicy.classify_result",
        lambda *args, **kwargs: None,
    )

    stats = destination.write([{"id": "a"}])

    # One call from serialize + one call from _append_chunk (no retry).
    assert calls["n"] == 2
    assert len(fresh_stream.requests) == 1
    assert fresh_stream.requests[0].offset == 0
    assert fresh_session.next_offset == 1

    assert stats.ok is True
    assert stats.total_written_rows == 1
    assert stats.total_failed_rows == 0
