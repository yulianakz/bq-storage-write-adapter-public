from types import SimpleNamespace
from typing import Sequence

from adapters.bigquery.storage_write.bq_storage_write_models import (
    DestinationWriteStats,
    StreamMode,
)
from adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator import (
    BigQueryWriteRetryOrchestrator,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.write_api_error import (
    BigQueryStorageWriteError,
)


class _SequenceDestination:
    def __init__(self, stats_sequence: list[DestinationWriteStats]) -> None:
        self._stats_sequence = stats_sequence
        self._config = SimpleNamespace(stream_mode=StreamMode.COMMITTED)
        self.write_calls = 0

    def write(self, rows: Sequence[dict]) -> DestinationWriteStats:
        self.write_calls += 1
        idx = self.write_calls - 1
        if idx < len(self._stats_sequence):
            return self._stats_sequence[idx]
        return self._stats_sequence[-1]

    def close(self) -> None:
        return None

    def mark_job_succeeded(self) -> None:
        return None


def _retryable_error(category: ErrorCategory, status_code: int) -> BigQueryStorageWriteError:
    return BigQueryStorageWriteError(
        f"retryable {category.value}",
        stream="projects/p/datasets/d/tables/t/streams/s",
        status_code=status_code,
        retryable=True,
        category=category,
    )


def _terminal_error(category: ErrorCategory, status_code: int) -> BigQueryStorageWriteError:
    return BigQueryStorageWriteError(
        f"terminal {category.value}",
        stream="projects/p/datasets/d/tables/t/streams/s",
        status_code=status_code,
        retryable=False,
        fatal_state=True,
        category=category,
    )


def _dest_success(total_rows: int) -> DestinationWriteStats:
    return DestinationWriteStats(
        ok=True,
        total_rows=total_rows,
        total_written_rows=total_rows,
        total_failed_rows=0,
        resolve_write_limit_passed=total_rows,
        serializer_attempted_rows=total_rows,
        serializer_rows_passed=total_rows,
        chunk_planner_attempted=total_rows,
        derived_chunk_count=1,
        derived_chunks_len=[total_rows],
        append_rows_send_attempted=total_rows,
        append_rows_send_passed=total_rows,
        append_rows_send_failed=0,
        stream_mode=StreamMode.COMMITTED.value,
    )


def _dest_error(total_rows: int, error: BigQueryStorageWriteError) -> DestinationWriteStats:
    return DestinationWriteStats(
        ok=False,
        total_rows=total_rows,
        total_written_rows=0,
        total_failed_rows=total_rows,
        resolve_write_limit_passed=total_rows,
        serializer_attempted_rows=total_rows,
        serializer_rows_passed=total_rows,
        chunk_planner_attempted=total_rows,
        derived_chunk_count=1,
        derived_chunks_len=[total_rows],
        append_rows_send_attempted=total_rows,
        append_rows_send_passed=0,
        append_rows_send_failed=total_rows,
        error=error,
        stream_mode=StreamMode.COMMITTED.value,
    )


def test_retry_stats_success_first_attempt(monkeypatch) -> None:
    destination = _SequenceDestination([_dest_success(total_rows=3)])
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _seconds: None,
    )

    out = orchestrator.write([{"id": 1}, {"id": 2}, {"id": 3}])
    flat = out.flatten()

    assert destination.write_calls == 1
    assert out.retry_attempts_total == 0
    assert out.total_rows_passed_to_retry == 0
    assert out.retries_by_category is None
    assert out.destination_stats.ok is True
    assert flat.dest_total_rows == 3
    assert flat.dest_append_rows_send_failed == 0


def test_retry_stats_one_retry_then_success(monkeypatch) -> None:
    transport_error = _retryable_error(ErrorCategory.TRANSPORT, status_code=14)
    destination = _SequenceDestination(
        [
            _dest_error(total_rows=4, error=transport_error),
            _dest_success(total_rows=4),
        ]
    )
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    out = orchestrator.write([{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}])
    flat = out.flatten()

    assert destination.write_calls == 2
    assert out.retry_attempts_total == 1
    assert out.total_rows_passed_to_retry == 4
    assert out.retries_by_category == {ErrorCategory.TRANSPORT.value: 1}
    assert out.last_error_category == ErrorCategory.TRANSPORT.value
    assert len(sleep_calls) == 1
    assert out.destination_stats.ok is True
    assert flat.total_rows_passed_to_retry == 4
    assert flat.dest_total_rows == 4
    assert flat.dest_total_written_rows == 4


def test_retry_stats_non_retryable_terminal_no_retry(monkeypatch) -> None:
    terminal_error = _terminal_error(ErrorCategory.INVALID_ARGUMENT, status_code=3)
    destination = _SequenceDestination([_dest_error(total_rows=2, error=terminal_error)])
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _seconds: (_ for _ in ()).throw(AssertionError("no sleep expected")),
    )

    out = orchestrator.write([{"id": "x"}, {"id": "y"}])

    assert destination.write_calls == 1
    assert out.retry_attempts_total == 0
    assert out.total_rows_passed_to_retry == 0
    assert out.last_error is terminal_error
    assert out.last_error_category == ErrorCategory.INVALID_ARGUMENT.value
    assert out.destination_stats.ok is False


def test_retry_stats_loop_guard_fallback_has_consistent_defaults(monkeypatch) -> None:
    destination = _SequenceDestination([_dest_success(total_rows=1)])
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        BigQueryWriteRetryOrchestrator,
        "max_loop_iterations",
        classmethod(lambda cls: 0),
    )

    out = orchestrator.write([{"id": "a"}, {"id": "b"}])
    flat = out.flatten()

    assert destination.write_calls == 0
    assert out.retry_attempts_total == 0
    assert out.total_rows_passed_to_retry == 0
    assert out.last_error_category == ErrorCategory.UNKNOWN.value
    assert out.destination_stats.total_rows == 2
    assert out.destination_stats.total_written_rows == 0
    assert out.destination_stats.total_failed_rows == 2
    # Defaults on synthesized fallback stats.
    assert out.destination_stats.resolve_write_limit_passed == 0
    assert out.destination_stats.resolve_write_limit_failed == 0
    assert out.destination_stats.serializer_attempted_rows == 0
    assert out.destination_stats.append_rows_send_attempted == 0
    assert flat.dest_total_failed_rows == 2
