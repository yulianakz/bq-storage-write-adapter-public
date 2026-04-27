"""Smoke tests for RetryOrchestratorStats.flatten().

Pins the wire_rejected = failed_rows - serializer_failure_count formula and
the error-category/status_code passthrough used by downstream logging.
"""

from adapters.bigquery.storage_write.bq_storage_write_models import (
    DestinationWriteStats,
    StreamMode,
)
from adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator_models import (
    RetryOrchestratorStats,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.writeapierror import (
    BigQueryStorageWriteError,
)


def test_flatten_success_case_has_zero_wire_rejected() -> None:
    dest_stats = DestinationWriteStats(
        ok=True,
        attempted_rows=3,
        written_rows=3,
        failed_rows=0,
        stream_mode=StreamMode.COMMITTED.value,
    )
    composite = RetryOrchestratorStats(destination_stats=dest_stats)

    flat = composite.flatten()

    assert flat.dest_ok is True
    assert flat.dest_attempted_rows == 3
    assert flat.dest_written_rows == 3
    assert flat.dest_failed_rows == 0
    assert flat.dest_serializer_row_failure_count == 0
    assert flat.dest_wire_rejected_rows == 0
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
        attempted_rows=5,
        written_rows=2,
        # 3 failed total: 1 dropped by the serializer, 2 rejected on the wire.
        failed_rows=3,
        serializer_row_failure_count=1,
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
    assert flat.dest_failed_rows == 3
    assert flat.dest_serializer_row_failure_count == 1
    assert flat.dest_wire_rejected_rows == 2
    assert flat.retry_attempts_total == 2
    assert flat.last_error_category == ErrorCategory.ROW_ERRORS.value
    assert flat.last_status_code == 9
    assert flat.dest_error_category == ErrorCategory.ROW_ERRORS.value
    assert flat.dest_error_status_code == 9
