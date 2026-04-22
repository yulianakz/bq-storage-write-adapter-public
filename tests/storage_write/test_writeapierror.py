"""Smoke tests for BigQueryStorageWriteError derived properties.

Pins the should_retry rule (retryable AND not fatal) and the has_row_errors
check, since both are consumed by the destination/orchestrator control flow.
"""

from types import SimpleNamespace

from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.writeapierror import (
    BigQueryStorageWriteError,
)


def test_retryable_non_fatal_error_is_retried_and_has_no_row_errors() -> None:
    err = BigQueryStorageWriteError(
        "transient transport glitch",
        stream="projects/p/datasets/d/tables/t/streams/s",
        retryable=True,
        needs_reset=True,
        fatal_state=False,
        category=ErrorCategory.TRANSPORT,
    )

    assert err.should_retry is True
    assert err.requires_stream_reset is True
    assert err.is_fatal is False
    assert err.has_row_errors is False
    assert err.row_errors == []


def test_retryable_but_fatal_error_is_not_retried_and_surfaces_row_errors() -> None:
    row_errors = [SimpleNamespace(index=0, code=1, message="bad row")]
    err = BigQueryStorageWriteError(
        "rejected with row errors",
        stream="projects/p/datasets/d/tables/t/streams/s",
        row_errors=row_errors,
        retryable=True,
        fatal_state=True,
        send_to_dlq=True,
        category=ErrorCategory.ROW_ERRORS,
    )

    assert err.should_retry is False
    assert err.requires_stream_reset is False
    assert err.is_fatal is True
    assert err.has_row_errors is True
    assert err.should_send_to_dlq is True
