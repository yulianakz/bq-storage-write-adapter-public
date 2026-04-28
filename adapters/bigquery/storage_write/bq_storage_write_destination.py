import logging
from threading import Lock
from typing import Any, Sequence

from adapters.bigquery.storage_write.bq_storage_write_models import (
    DestinationWriteStats,
    StorageWriteConfig,
    StorageWriteSession,
    StreamMode,
    StreamLifecycleState,
)
from adapters.bigquery.storage_write.bq_storage_write_utilities import (
    batch_commit_write_streams,
    build_table_parent,
    create_storage_write_session,
    finalize_write_stream,
    plan_append_chunks,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.write_api_error import (
    BigQueryStorageWriteError,
)
from adapters.bigquery.storage_write.row_errors_mapping import (
    map_chunk_row_errors_to_original,
)
from adapters.bigquery.storage_write.row_serializer.serializer_models import (
    RowSerializationError,
)
from adapters.bigquery.storage_write.write_limits import resolve_write_limits
from adapters.bigquery.storage_write.write_limits import ResolvedWriteLimits
from adapters.bigquery.storage_write.retry_handler.extended_error_policy import (
    ExtendedErrorPolicy,
)
from ports import Destination
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1.types import RowError

logger = logging.getLogger(__name__)


class BigQueryStorageWriteDestination(Destination[dict[str, Any], DestinationWriteStats]):
    """
    BigQuery Storage Write API destination using dynamic protobuf serialization.
    Dedicated streams use offsets for exactly-once appends within this adapter instance:
    `committed` (rows visible per commit semantics) and `pending` (finalize stream separately).
    """

    def __init__(self, *, config: StorageWriteConfig) -> None:
        self._config = config
        self._write_lock = Lock()
        self._session_lock = Lock()

        # Lock order rule: _session_lock must never be acquired while holding _write_lock.
        self._session: StorageWriteSession | None = None

        # Resolved and validated write limits; cached since config is immutable.
        self._limits: ResolvedWriteLimits | None = None

        # Latched by the job layer on full success; pending streams commit only when set.
        self._job_succeeded = False

        # Best-effort cleanup telemetry (close() does not raise for all underlying errors).
        self._close_failed = False

    def mark_job_succeeded(self) -> None:
        self._job_succeeded = True

    def close_failed(self) -> bool:
        return self._close_failed

    def _get_or_create_session(self) -> StorageWriteSession:
        session = self._session
        if session is not None:
            return session

        with self._session_lock:
            session = self._session
            if session is None:
                session = create_storage_write_session(
                    config=self._config,
                )
                self._session = session
            return session

    def _close_session_resources(self, session: StorageWriteSession) -> None:
        close_fn = getattr(session.append_rows_stream, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception:
                self._close_failed = True
                logger.exception("AppendRowsStream close failed")
        try:
            session.write_client.close()
        except Exception:
            self._close_failed = True
            logger.exception("BigQuery write_client close failed")


    def _reset_session(self) -> None:
        with self._session_lock:
            session = self._session
            self._session = None

        if session is None:
            return

        self._close_session_resources(session)

    @staticmethod
    def _finalize_stream(session: StorageWriteSession) -> None:
        finalize_write_stream(session.write_client, session.stream_name)

    def _commit_stream(self, session: StorageWriteSession) -> None:
        parent = build_table_parent(
            write_client=session.write_client,
            project_id=self._config.project_id,
            dataset_id=self._config.dataset_id,
            table_id=self._config.table_id,
        )
        batch_commit_write_streams(
            session.write_client,
            parent,
            [session.stream_name],
        )

    def _should_finalize_commit(self, session: StorageWriteSession) -> bool:
        return (
            self._config.stream_mode == StreamMode.PENDING
            and self._job_succeeded
            and session.next_offset > 0
        )

    def _pending_stream_finalize_commit(self, session: StorageWriteSession) -> None:
        if not self._should_finalize_commit(session):
            return

        if session.state == StreamLifecycleState.OPEN:
            try:
                self._finalize_stream(session)
                session.state = StreamLifecycleState.FINALIZED
            except Exception:
                self._close_failed = True
                logger.exception(
                    "Pending stream finalize failed during close",
                    extra={"stream": session.stream_name},
                )
                raise

        if session.state == StreamLifecycleState.FINALIZED:
            try:
                self._commit_stream(session)
                session.state = StreamLifecycleState.COMMITTED
            except Exception:
                self._close_failed = True
                logger.exception(
                    "Pending stream commit failed during close",
                    extra={"stream": session.stream_name},
                )
                raise

    def close(self) -> None:
        session_to_close: StorageWriteSession | None = None
        try:
            with self._session_lock:
                session = self._session
                if session is None or session.state == StreamLifecycleState.CLOSED:
                    return
                session_to_close = session
                try:
                    with self._write_lock:
                        self._pending_stream_finalize_commit(session)
                finally:
                    session.state = StreamLifecycleState.CLOSED
                    self._session = None
        finally:
            if session_to_close is not None:
                self._close_session_resources(session_to_close)

    def _serialize_with_diagnostics(
        self, rows: Sequence[dict[str, Any]], *, row_max_bytes: int
    ) -> tuple[list[bytes], list[RowSerializationError], list[int]]:
        session = self._get_or_create_session()
        serialization_result = session.row_serializer.serialize_rows(
            rows,
            row_max_bytes=row_max_bytes,
        )
        good_idx = serialization_result.good_row_indices or []
        return (
            serialization_result.good_serialized_rows,
            serialization_result.bad_rows,
            list(good_idx),
        )


    def _append_chunk(self, *, chunk_rows: Sequence[bytes],
                      ) -> tuple[StorageWriteSession, int | None, BigQueryStorageWriteError | None]:

        proto_rows = types.ProtoRows(serialized_rows=list(chunk_rows))

        session: StorageWriteSession
        expected_next_offset: int | None = None
        policy_error: BigQueryStorageWriteError | None = None
        send_future = None
        row_count = len(chunk_rows)

        # The `while True` loop handles a narrow race: between resolving
        # `session` via `_get_or_create_session()` and acquiring `_write_lock`,
        # another code path (e.g. a previous chunk's policy-driven reset) may
        # have swapped `self._session`. In that case we `continue` to re-resolve
        # the current session instead of appending against a stale one. On the
        # happy path we enter the lock exactly once and fall through to `break`.
        while True:

            session = self._get_or_create_session()
            request = types.AppendRowsRequest(
                proto_rows=types.AppendRowsRequest.ProtoData(rows=proto_rows),
            )

            with self._write_lock:

                if session is not self._session:
                    continue  # concurrent `_reset_session` swapped self._session between resolve and write-lock acquire; retry against the fresh session (rare, but load-bearing for correctness — do not delete).
                current_offset = session.next_offset
                expected_next_offset = current_offset + row_count
                request.offset = current_offset

                try:
                    send_future = session.append_rows_stream.send(request)
                except Exception as exc:
                    policy_error = ExtendedErrorPolicy.classify_result(
                        exc,
                        stream=session.stream_name,
                        stream_mode=self._config.stream_mode,
                    )
            break

        if policy_error is None:

            try:
                response = send_future.result(timeout=self._config.append_timeout_seconds)
                policy_error = ExtendedErrorPolicy.classify_result(
                    response,
                    stream=session.stream_name,
                    stream_mode=self._config.stream_mode,
                )

            except Exception as exc:
                policy_error = ExtendedErrorPolicy.classify_result(
                    exc,
                    stream=session.stream_name,
                    stream_mode=self._config.stream_mode,
                )

        return session, expected_next_offset, policy_error


    def _apply_policy_outcome(
        self,
        *,
        session: StorageWriteSession,
        expected_next_offset: int | None,
        policy_error: BigQueryStorageWriteError,
    ) -> None:

        if policy_error.advance_offset:
            with self._write_lock:
                if session is self._session and expected_next_offset is not None:
                    session.next_offset = expected_next_offset

        if policy_error.requires_stream_reset:
            try:
                self._reset_session()
            except Exception:
                logger.exception(
                    "Failed to reset Storage Write session after append failure",
                    extra={"stream": session.stream_name},
                )

    def _aggregate_write_stats(
        self,
        *,
        ok: bool,
        total_rows: int,
        resolve_write_limit_passed: int = 0,
        resolve_write_limit_failed: int = 0,
        serializer_attempted_rows: int = 0,
        serializer_rows_passed: int = 0,
        serializer_row_failures: list[RowSerializationError] | None = None,
        chunk_planner_attempted: int = 0,
        derived_chunk_count: int = 0,
        derived_chunks_len: list[int] | None = None,
        append_rows_send_attempted: int = 0,
        append_rows_send_passed: int = 0,
        append_rows_send_failed: int = 0,
        error: BigQueryStorageWriteError | None = None,
        row_error_bad_rows: list[dict[str, Any]] | None = None,
        row_error_good_rows: list[dict[str, Any]] | None = None,
        row_error_bad_count: int = 0,
        row_error_good_count: int = 0,
        skipped_already_exists_rows: int = 0,
    ) -> DestinationWriteStats:

        # `total_failed_rows` excludes ALREADY_EXISTS skips.
        total_written_rows = append_rows_send_passed
        total_failed_rows = total_rows - total_written_rows - skipped_already_exists_rows
        if total_failed_rows < 0:
            logger.warning(
                "Storage Write stats clamp: computed total_failed_rows was negative; clamping to zero",
                extra={
                    "total_rows": total_rows,
                    "append_rows_send_passed": append_rows_send_passed,
                    "skipped_already_exists_rows": skipped_already_exists_rows,
                    "computed_total_failed_rows": total_failed_rows,
                },
            )
            total_failed_rows = 0

        return DestinationWriteStats(
            ok=ok,
            total_rows=total_rows,
            total_written_rows=total_written_rows,
            total_failed_rows=total_failed_rows,
            resolve_write_limit_passed=resolve_write_limit_passed,
            resolve_write_limit_failed=resolve_write_limit_failed,
            serializer_attempted_rows=serializer_attempted_rows,
            serializer_rows_passed=serializer_rows_passed,
            serializer_rows_failed=len(serializer_row_failures) if serializer_row_failures is not None else 0,
            serializer_row_failures=serializer_row_failures or None,
            chunk_planner_attempted=chunk_planner_attempted,
            derived_chunk_count=derived_chunk_count,
            derived_chunks_len=derived_chunks_len,
            append_rows_send_attempted=append_rows_send_attempted,
            append_rows_send_passed=append_rows_send_passed,
            append_rows_send_failed=append_rows_send_failed,
            skipped_already_exists_rows=skipped_already_exists_rows,
            row_error_bad_count=row_error_bad_count,
            row_error_good_count=row_error_good_count,
            row_error_bad_rows=row_error_bad_rows,
            row_error_good_rows=row_error_good_rows,
            error=error,
            stream_mode=self._config.stream_mode.value,
        )

    def _get_or_resolve_limits(self) -> ResolvedWriteLimits:
        limits = self._limits
        if limits is None:
            limits = resolve_write_limits(self._config)
            self._limits = limits
        return limits

    def write(self, rows: Sequence[dict[str, Any]]) -> DestinationWriteStats:
        total_rows = len(rows)

        if not rows:
            return self._aggregate_write_stats(
                ok=True,
                total_rows=0,
            )

        resolve_write_limit_passed = 0
        resolve_write_limit_failed = 0

        try:
            limits = self._get_or_resolve_limits()
            resolve_write_limit_passed = total_rows
        except ValueError as exc:
            resolve_write_limit_failed = total_rows
            config_error = BigQueryStorageWriteError(
                str(exc),
                stream=None,
                original_exception=exc,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.INVALID_ARGUMENT,
            )
            return self._aggregate_write_stats(
                ok=False,
                total_rows=total_rows,
                resolve_write_limit_passed=resolve_write_limit_passed,
                resolve_write_limit_failed=resolve_write_limit_failed,
                error=config_error,
                serializer_row_failures=None,
            )
        serialized_rows, serializer_row_failures, good_row_indices = self._serialize_with_diagnostics(
            rows,
            row_max_bytes=limits.row_max_bytes,
        )
        serializer_attempted_rows = resolve_write_limit_passed
        serializer_rows_passed = len(serialized_rows)
        chunk_planner_attempted = serializer_rows_passed

        if not serialized_rows:
            all_serialization_failed_error = BigQueryStorageWriteError(
                "All input rows failed protobuf serialization for Storage Write",
                stream=None,
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=True,
                category=ErrorCategory.INVALID_ARGUMENT,
            )
            return self._aggregate_write_stats(
                ok=False,
                total_rows=total_rows,
                resolve_write_limit_passed=resolve_write_limit_passed,
                resolve_write_limit_failed=resolve_write_limit_failed,
                serializer_attempted_rows=serializer_attempted_rows,
                serializer_rows_passed=serializer_rows_passed,
                serializer_row_failures=serializer_row_failures,
                error=all_serialization_failed_error,
            )

        append_chunks = plan_append_chunks(
            serialized_rows=serialized_rows,
            max_rows=limits.max_rows,
            max_payload_bytes=limits.request_payload_budget_bytes,
        )
        derived_chunk_count = len(append_chunks)
        derived_chunks_len = [len(chunk.rows) for chunk in append_chunks]

        append_rows_send_attempted = 0
        append_rows_send_passed = 0
        append_rows_send_failed = 0
        skipped_already_exists_rows = 0
        chunk_ser_start = 0

        # Each AppendRowsRequest is its own transaction: a row_errors rejection in
        # chunk N does not invalidate chunks N+1..M. We keep iterating on row_errors
        # so later chunks still get a chance to write, and accumulate their bad/good
        # splits for the pipeline's one-pass recovery path.
        accumulated_row_error_bad: list[dict[str, Any]] = []
        accumulated_row_error_good: list[dict[str, Any]] = []
        accumulated_row_errors: list[RowError] = []
        row_error_bad_count = 0
        row_error_good_count = 0
        terminal_error: BigQueryStorageWriteError | None = None
        last_row_errors_stream: str | None = None

        for chunk_index, append_chunk in enumerate(append_chunks):
            session, expected_next_offset, policy_error = self._append_chunk(
                chunk_rows=append_chunk.rows,
            )

            chunk_len = len(append_chunk.rows)
            append_rows_send_attempted += chunk_len

            if policy_error is None:
                with self._write_lock:
                    if session is self._session and expected_next_offset is not None:
                        session.next_offset = expected_next_offset
                append_rows_send_passed += chunk_len
                chunk_ser_start += chunk_len
                continue

            self._apply_policy_outcome(
                session=session,
                expected_next_offset=expected_next_offset,
                policy_error=policy_error,
            )

            # Benign server-side no-op (currently: ALREADY_EXISTS). Offset has
            # already been advanced by _apply_policy_outcome; the rows are safe
            # on the server (committed or pending-buffered) and must not be
            # DLQ. Skip this chunk and keep attempting the rest of the batch.
            if policy_error.should_continue_loop:
                logger.info(
                    "Storage Write chunk %d skipped: server reported %s at offset %s "
                    "(stream=%s, rows=%d). Offset advanced; continuing with next chunk.",
                    chunk_index,
                    policy_error.category.value if policy_error.category is not None else "no-op",
                    expected_next_offset,
                    policy_error.stream,
                    chunk_len,
                )
                skipped_already_exists_rows += chunk_len
                chunk_ser_start += chunk_len
                continue

            if policy_error.has_row_errors:
                append_rows_send_failed += chunk_len
                chunk_bad, chunk_good = map_chunk_row_errors_to_original(
                    rows=rows,
                    good_row_indices=good_row_indices,
                    chunk_ser_start=chunk_ser_start,
                    chunk_len=chunk_len,
                    row_errors=policy_error.row_errors,
                )
                if chunk_bad:
                    accumulated_row_error_bad.extend(chunk_bad)
                    row_error_bad_count += len(chunk_bad)
                if chunk_good:
                    accumulated_row_error_good.extend(chunk_good)
                    row_error_good_count += len(chunk_good)
                accumulated_row_errors.extend(policy_error.row_errors)
                last_row_errors_stream = policy_error.stream
                chunk_ser_start += chunk_len
                continue

            # Non-row-errors terminal (or retryable): stop issuing further AppendRowsRequests.
            append_rows_send_failed += chunk_len
            terminal_error = policy_error
            break

        row_err_bad_out = accumulated_row_error_bad or None
        row_err_good_out = accumulated_row_error_good or None

        if terminal_error is not None:
            return self._aggregate_write_stats(
                ok=False,
                total_rows=total_rows,
                resolve_write_limit_passed=resolve_write_limit_passed,
                resolve_write_limit_failed=resolve_write_limit_failed,
                serializer_attempted_rows=serializer_attempted_rows,
                serializer_rows_passed=serializer_rows_passed,
                error=terminal_error,
                serializer_row_failures=serializer_row_failures,
                chunk_planner_attempted=chunk_planner_attempted,
                derived_chunk_count=derived_chunk_count,
                derived_chunks_len=derived_chunks_len,
                append_rows_send_attempted=append_rows_send_attempted,
                append_rows_send_passed=append_rows_send_passed,
                append_rows_send_failed=append_rows_send_failed,
                row_error_bad_rows=row_err_bad_out,
                row_error_good_rows=row_err_good_out,
                row_error_bad_count=row_error_bad_count,
                row_error_good_count=row_error_good_count,
                skipped_already_exists_rows=skipped_already_exists_rows,
            )

        if accumulated_row_errors:
            combined_row_errors_policy = BigQueryStorageWriteError(
                "BigQuery Storage Write append rejected due to row errors",
                stream=last_row_errors_stream,
                row_errors=accumulated_row_errors,
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=True,
                category=ErrorCategory.ROW_ERRORS,
            )
            return self._aggregate_write_stats(
                ok=False,
                total_rows=total_rows,
                resolve_write_limit_passed=resolve_write_limit_passed,
                resolve_write_limit_failed=resolve_write_limit_failed,
                serializer_attempted_rows=serializer_attempted_rows,
                serializer_rows_passed=serializer_rows_passed,
                error=combined_row_errors_policy,
                serializer_row_failures=serializer_row_failures,
                chunk_planner_attempted=chunk_planner_attempted,
                derived_chunk_count=derived_chunk_count,
                derived_chunks_len=derived_chunks_len,
                append_rows_send_attempted=append_rows_send_attempted,
                append_rows_send_passed=append_rows_send_passed,
                append_rows_send_failed=append_rows_send_failed,
                row_error_bad_rows=row_err_bad_out,
                row_error_good_rows=row_err_good_out,
                row_error_bad_count=row_error_bad_count,
                row_error_good_count=row_error_good_count,
                skipped_already_exists_rows=skipped_already_exists_rows,
            )

        return self._aggregate_write_stats(
            ok=True,
            total_rows=total_rows,
            resolve_write_limit_passed=resolve_write_limit_passed,
            resolve_write_limit_failed=resolve_write_limit_failed,
            serializer_attempted_rows=serializer_attempted_rows,
            serializer_rows_passed=serializer_rows_passed,
            serializer_row_failures=serializer_row_failures,
            chunk_planner_attempted=chunk_planner_attempted,
            derived_chunk_count=derived_chunk_count,
            derived_chunks_len=derived_chunks_len,
            append_rows_send_attempted=append_rows_send_attempted,
            append_rows_send_passed=append_rows_send_passed,
            append_rows_send_failed=append_rows_send_failed,
            row_error_bad_count=row_error_bad_count,
            row_error_good_count=row_error_good_count,
            skipped_already_exists_rows=skipped_already_exists_rows,
        )

