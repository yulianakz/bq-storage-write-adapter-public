"""Smoke tests for ErrorPolicy.

Covers the happy path on AppendRowsResponse (append_result present → None)
and one transport-level exception classification (TimeoutError → retryable
DEADLINE with a stream reset signal).
"""

from types import SimpleNamespace

from adapters.bigquery.storage_write.bq_storage_write_models import StreamMode
from adapters.bigquery.storage_write.retry_handler.error_policy import ErrorPolicy
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory


def test_classify_append_rows_response_success_returns_none() -> None:
    response = SimpleNamespace(append_result=SimpleNamespace(offset=0))

    result = ErrorPolicy.classify_append_rows_response(
        response, stream="projects/p/datasets/d/tables/t/streams/s",
        stream_mode=StreamMode.COMMITTED,
    )

    assert result is None


def test_classify_exception_timeout_is_retryable_deadline() -> None:
    exc = TimeoutError("append timed out")

    err = ErrorPolicy.classify_exception(
        exc,
        stream="projects/p/datasets/d/tables/t/streams/s",
        stream_mode=StreamMode.COMMITTED,
    )

    assert err.category == ErrorCategory.DEADLINE
    assert err.retryable is True
    assert err.needs_reset is True
    assert err.advance_offset is False
    assert err.send_to_dlq is False
    assert err.original_exception is exc
