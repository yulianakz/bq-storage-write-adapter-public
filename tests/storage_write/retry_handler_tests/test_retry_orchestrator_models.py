"""Smoke tests for RetryOrchestratorStats.flatten()."""

from adapters.bigquery.storage_write.bq_storage_write_models import (
    DestinationWriteStats,
    StreamMode,
)
from adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator_models import (
    RetryOrchestratorStats,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.write_api_error import (
    BigQueryStorageWriteError,
)


def test_flatten_success_case_has_zero_wire_rejected() -> None:
    dest_stats = DestinationWriteStats(
        ok=True,
        total_rows=3,
        total_written_rows=3,
        total_failed_rows=0,
        stream_mode=StreamMode.COMMITTED.value,
    )
    composite = RetryOrchestratorStats(destination_stats=dest_stats)

    flat = composite.flatten()

    assert flat.dest_ok is True
    assert flat.dest_total_rows == 3
    assert flat.dest_total_written_rows == 3
    assert flat.dest_total_failed_rows == 0
    assert flat.dest_serializer_rows_failed == 0
    assert flat.dest_append_rows_send_failed == 0
    assert flat.retry_attempts_total == 0
    assert flat.last_error_category is None
    assert flat.dest_error_category is None


def test_flatten_failure_splits_serializer_vs_wire_failures() -> None:
    dest_error = BigQueryStorageWriteError(
        "append rejected due to row errors",
        status_code=9,
        retryable=False,
        fatal_state=False,
        send_to_dlq=True,
        category=ErrorCategory.ROW_ERRORS,
    )
    dest_stats = DestinationWriteStats(
        ok=False,
        total_rows=5,
        total_written_rows=2,
        # 3 failed total: 1 dropped by the serializer, 2 rejected on the wire.
        total_failed_rows=3,
        serializer_rows_failed=1,
        append_rows_send_failed=2,
        error=dest_error,
        stream_mode=StreamMode.COMMITTED.value,
    )
    composite = RetryOrchestratorStats(
        destination_stats=dest_stats,
        retry_attempts_total=2,
        last_error=dest_error,
        last_error_category=ErrorCategory.ROW_ERRORS.value,
        last_status_code=9,
    )

    flat = composite.flatten()

    assert flat.dest_ok is False
    assert flat.dest_total_failed_rows == 3
    assert flat.dest_serializer_rows_failed == 1
    assert flat.dest_append_rows_send_failed == 2
    assert flat.retry_attempts_total == 2
    assert flat.last_error_category == ErrorCategory.ROW_ERRORS.value
    assert flat.last_status_code == 9
    assert flat.dest_error_category == ErrorCategory.ROW_ERRORS.value
    assert flat.dest_error_status_code == 9
