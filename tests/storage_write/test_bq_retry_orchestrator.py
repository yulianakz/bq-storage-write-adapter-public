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
from adapters.bigquery.storage_write.retry_handler.writeapierror import (
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


def test_orchestrator_returns_success_without_retry(monkeypatch) -> None:
    successful_stats = DestinationWriteStats(
        ok=True,
        attempted_rows=2,
        written_rows=2,
        failed_rows=0,
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
        attempted_rows=1,
        written_rows=0,
        failed_rows=1,
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
