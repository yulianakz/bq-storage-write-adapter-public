"""Smoke tests for BigQueryWriteRetryOrchestrator.write().

Covers the first-attempt success path (no retries, no sleep) and the
non-retryable terminal path (orchestrator returns immediately with the
destination error surfaced on RetryOrchestratorStats).
"""

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


class _FakeDestination:
    def __init__(self, stats_to_return: DestinationWriteStats) -> None:
        self._stats = stats_to_return
        self._config = SimpleNamespace(stream_mode=StreamMode.COMMITTED)
        self.write_calls = 0

    def write(self, rows: Sequence[dict]) -> DestinationWriteStats:
        self.write_calls += 1
        return self._stats

    def close(self) -> None:
        return None

    def mark_job_succeeded(self) -> None:
        return None


class _SequenceFakeDestination:
    def __init__(self, stats_sequence: list[DestinationWriteStats]) -> None:
        if not stats_sequence:
            raise ValueError("stats_sequence must not be empty")
        self._stats_sequence = list(stats_sequence)
        self._config = SimpleNamespace(stream_mode=StreamMode.COMMITTED)
        self.write_calls = 0

    def write(self, rows: Sequence[dict]) -> DestinationWriteStats:
        self.write_calls += 1
        index = self.write_calls - 1
        if index < len(self._stats_sequence):
            return self._stats_sequence[index]
        return self._stats_sequence[-1]

    def close(self) -> None:
        return None

    def mark_job_succeeded(self) -> None:
        return None


class _ShouldNotWriteDestination:
    def __init__(self) -> None:
        self._config = SimpleNamespace(stream_mode=StreamMode.COMMITTED)
        self.write_calls = 0

    def write(self, rows: Sequence[dict]) -> DestinationWriteStats:
        self.write_calls += 1
        raise AssertionError("destination.write must not be called in this branch")

    def close(self) -> None:
        return None

    def mark_job_succeeded(self) -> None:
        return None


def _retryable_error(category: ErrorCategory, *, status_code: int) -> BigQueryStorageWriteError:
    return BigQueryStorageWriteError(
        f"retryable {category.value}",
        stream="projects/p/datasets/d/tables/t/streams/s",
        status_code=status_code,
        retryable=True,
        fatal_state=False,
        send_to_dlq=False,
        category=category,
    )


def _stats_with_error(error: BigQueryStorageWriteError) -> DestinationWriteStats:
    return DestinationWriteStats(
        ok=False,
        total_rows=1,
        total_written_rows=0,
        total_failed_rows=1,
        error=error,
        stream_mode=StreamMode.COMMITTED.value,
    )


def test_orchestrator_returns_success_without_retry(monkeypatch) -> None:
    successful_stats = DestinationWriteStats(
        ok=True,
        total_rows=2,
        total_written_rows=2,
        total_failed_rows=0,
        stream_mode=StreamMode.COMMITTED.value,
    )
    destination = _FakeDestination(successful_stats)
    orchestrator = BigQueryWriteRetryOrchestrator(destination)

    # Guard against accidental sleeps: a happy path must not retry.
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _s: (_ for _ in ()).throw(AssertionError("no sleep expected on success")),
    )

    result = orchestrator.write([{"id": "a"}, {"id": "b"}])

    assert destination.write_calls == 1
    assert result.destination_stats is successful_stats
    assert result.retry_attempts_total == 0
    assert result.last_error is None
    assert result.last_error_category is None


def test_orchestrator_returns_immediately_on_non_retryable_error(monkeypatch) -> None:
    # INVALID_ARGUMENT is non-retryable per RETRY_ATTEMPTS_BY_CATEGORY policy.
    terminal_error = BigQueryStorageWriteError(
        "invalid argument",
        stream="projects/p/datasets/d/tables/t/streams/s",
        status_code=3,
        retryable=False,
        fatal_state=True,
        send_to_dlq=True,
        category=ErrorCategory.INVALID_ARGUMENT,
    )
    failing_stats = DestinationWriteStats(
        ok=False,
        total_rows=1,
        total_written_rows=0,
        total_failed_rows=1,
        error=terminal_error,
        stream_mode=StreamMode.COMMITTED.value,
    )
    destination = _FakeDestination(failing_stats)
    orchestrator = BigQueryWriteRetryOrchestrator(destination)

    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _s: (_ for _ in ()).throw(AssertionError("no sleep expected on non-retryable")),
    )

    result = orchestrator.write([{"id": "a"}])

    assert destination.write_calls == 1
    assert result.retry_attempts_total == 0
    assert result.last_error is terminal_error
    assert result.last_error_category == ErrorCategory.INVALID_ARGUMENT.value
    assert result.last_status_code == 3


def test_orchestrator_loop_guard_is_derived_from_retry_policy() -> None:
    expected = max(
        BigQueryWriteRetryOrchestrator.RETRY_ATTEMPTS_BY_CATEGORY.values(),
        default=0,
    ) + 2
    assert BigQueryWriteRetryOrchestrator.max_loop_iterations() == expected


def test_orchestrator_loop_guard_handles_empty_retry_policy() -> None:
    original_policy = BigQueryWriteRetryOrchestrator.RETRY_ATTEMPTS_BY_CATEGORY
    try:
        BigQueryWriteRetryOrchestrator.RETRY_ATTEMPTS_BY_CATEGORY = {}
        assert BigQueryWriteRetryOrchestrator.max_loop_iterations() == 2
    finally:
        BigQueryWriteRetryOrchestrator.RETRY_ATTEMPTS_BY_CATEGORY = original_policy


def test_orchestrator_tracks_retries_by_category_across_mixed_retryable_errors(
    monkeypatch,
) -> None:
    deadline_error = _retryable_error(ErrorCategory.DEADLINE, status_code=4)
    transport_error = _retryable_error(ErrorCategory.TRANSPORT, status_code=14)
    success_stats = DestinationWriteStats(
        ok=True,
        total_rows=1,
        total_written_rows=1,
        total_failed_rows=0,
        stream_mode=StreamMode.COMMITTED.value,
    )
    destination = _SequenceFakeDestination(
        [
            _stats_with_error(deadline_error),
            _stats_with_error(transport_error),
            success_stats,
        ]
    )
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    sleep_calls: list[float] = []
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    result = orchestrator.write([{"id": "a"}])

    assert destination.write_calls == 3
    assert result.destination_stats is success_stats
    assert result.retry_attempts_total == 2
    assert result.retries_by_category == {
        ErrorCategory.DEADLINE.value: 1,
        ErrorCategory.TRANSPORT.value: 1,
    }
    assert result.last_error is transport_error
    assert result.last_error_category == ErrorCategory.TRANSPORT.value
    assert result.last_status_code == 14
    assert len(sleep_calls) == 2


def test_orchestrator_repeated_retryable_errors_stop_at_retry_budget_not_infinite_loop(
    monkeypatch,
) -> None:
    throttle_error = _retryable_error(ErrorCategory.THROTTLE, status_code=8)
    destination = _SequenceFakeDestination([_stats_with_error(throttle_error)])
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _seconds: None,
    )

    result = orchestrator.write([{"id": "a"}])

    # THROTTLE max retries is 3, so we expect 1 initial attempt + 3 retries.
    assert destination.write_calls == 4
    assert result.retry_attempts_total == 3
    assert result.retries_by_category == {ErrorCategory.THROTTLE.value: 3}
    assert result.last_error is throttle_error
    assert result.last_error_category == ErrorCategory.THROTTLE.value
    assert result.last_status_code == 8


def test_orchestrator_loop_iteration_guard_counts_whole_write_cycle(monkeypatch) -> None:
    throttle_error = _retryable_error(ErrorCategory.THROTTLE, status_code=8)
    destination = _SequenceFakeDestination([_stats_with_error(throttle_error)])
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        BigQueryWriteRetryOrchestrator,
        "max_loop_iterations",
        classmethod(lambda cls: 3),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _seconds: None,
    )

    result = orchestrator.write([{"id": "a"}])

    # Guard check runs once per while-loop iteration for the single write() call.
    assert destination.write_calls == 3
    assert result.retry_attempts_total == 3
    assert result.last_error is not None
    assert result.last_error_category == ErrorCategory.UNKNOWN.value
    assert "safety iteration limit" in result.last_error.message


def test_orchestrator_loop_guard_with_tiny_limit_surfaces_synthesized_error(monkeypatch) -> None:
    throttle_error = _retryable_error(ErrorCategory.THROTTLE, status_code=8)
    destination = _SequenceFakeDestination([_stats_with_error(throttle_error)])
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        BigQueryWriteRetryOrchestrator,
        "max_loop_iterations",
        classmethod(lambda cls: 1),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _seconds: None,
    )

    result = orchestrator.write([{"id": "a"}])

    assert result.last_error is not None
    assert result.last_error_category == ErrorCategory.UNKNOWN.value
    assert result.destination_stats.error is not None
    assert "safety iteration limit" in result.destination_stats.error.message


def test_orchestrator_loop_guard_before_first_destination_attempt_builds_fallback_stats(
    monkeypatch,
) -> None:
    destination = _ShouldNotWriteDestination()
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        BigQueryWriteRetryOrchestrator,
        "max_loop_iterations",
        classmethod(lambda cls: 0),
    )

    rows = [{"id": "a"}, {"id": "b"}]
    result = orchestrator.write(rows)

    assert destination.write_calls == 0
    assert result.destination_stats.total_rows == 2
    assert result.destination_stats.total_failed_rows == 2
    assert result.destination_stats.total_written_rows == 0
    assert result.last_error_category == ErrorCategory.UNKNOWN.value


def test_orchestrator_loop_guard_boundary_fires_only_after_exceeding_max(monkeypatch) -> None:
    throttle_error = _retryable_error(ErrorCategory.THROTTLE, status_code=8)
    success_stats = DestinationWriteStats(
        ok=True,
        total_rows=1,
        total_written_rows=1,
        total_failed_rows=0,
        stream_mode=StreamMode.COMMITTED.value,
    )
    destination = _SequenceFakeDestination(
        [
            _stats_with_error(throttle_error),
            _stats_with_error(throttle_error),
            success_stats,
        ]
    )
    orchestrator = BigQueryWriteRetryOrchestrator(destination)
    monkeypatch.setattr(
        BigQueryWriteRetryOrchestrator,
        "max_loop_iterations",
        classmethod(lambda cls: 3),
    )
    monkeypatch.setattr(
        "adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator.time.sleep",
        lambda _seconds: None,
    )

    result = orchestrator.write([{"id": "a"}])

    assert destination.write_calls == 3
    assert result.destination_stats is success_stats
    assert result.last_error_category == ErrorCategory.THROTTLE.value
