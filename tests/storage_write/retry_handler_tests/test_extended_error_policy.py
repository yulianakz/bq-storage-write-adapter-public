from types import SimpleNamespace

import grpc
from google.api_core import exceptions as gax_exceptions
from google.rpc import code_pb2

from adapters.bigquery.storage_write.bq_storage_write_models import StreamMode
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.extended_error_policy import (
    ExtendedErrorPolicy,
)

STREAM = "projects/p/datasets/d/tables/t/streams/s"


def test_classify_exception_subclass_fallbacks_without_grpc_status_code() -> None:
    cases = (
        (gax_exceptions.ResourceExhausted("throttle"), ErrorCategory.THROTTLE),
        (gax_exceptions.ServiceUnavailable("svc down"), ErrorCategory.TRANSPORT),
        (gax_exceptions.Aborted("aborted"), ErrorCategory.INTERNAL),
        (gax_exceptions.Unauthenticated("auth"), ErrorCategory.AUTH),
    )

    for exc, expected in cases:
        exc.grpc_status_code = None
        err = ExtendedErrorPolicy.classify_exception(
            exc, stream=STREAM, stream_mode=StreamMode.COMMITTED
        )
        assert err.category == expected
        assert err.original_exception is exc


def test_classify_exception_retry_error_unwraps_cause() -> None:
    cause = gax_exceptions.ResourceExhausted("throttle root cause")
    cause.grpc_status_code = None
    wrapped = gax_exceptions.RetryError("retries exhausted", cause)

    err = ExtendedErrorPolicy.classify_exception(
        wrapped, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )

    assert err.category == ErrorCategory.THROTTLE
    assert err.original_exception is wrapped


def test_classify_exception_unknown_google_api_call_error_is_unknown_fatal() -> None:
    exc = gax_exceptions.GoogleAPICallError("unknown call failure")
    exc.grpc_status_code = None

    err = ExtendedErrorPolicy.classify_exception(
        exc, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )

    assert err.category == ErrorCategory.UNKNOWN
    assert err.fatal_state is True


def test_classify_exception_timeout_is_retryable_deadline() -> None:
    exc = TimeoutError("append timed out")

    err = ExtendedErrorPolicy.classify_exception(
        exc, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )

    assert err.category == ErrorCategory.DEADLINE
    assert err.retryable is True
    assert err.needs_reset is True
    assert err.advance_offset is False
    assert err.send_to_dlq is False
    assert err.original_exception is exc


def test_grpc_status_normalization_equivalence_resource_exhausted() -> None:
    enum_exc = gax_exceptions.GoogleAPICallError("enum status")
    enum_exc.grpc_status_code = grpc.StatusCode.RESOURCE_EXHAUSTED
    int_exc = gax_exceptions.GoogleAPICallError("int status")
    int_exc.grpc_status_code = code_pb2.RESOURCE_EXHAUSTED

    enum_err = ExtendedErrorPolicy.classify_exception(
        enum_exc, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )
    int_err = ExtendedErrorPolicy.classify_exception(
        int_exc, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )

    assert enum_err.category == ErrorCategory.THROTTLE
    assert int_err.category == ErrorCategory.THROTTLE
    assert enum_err.retryable == int_err.retryable
    assert enum_err.needs_reset == int_err.needs_reset


def test_classify_append_rows_response_variants() -> None:
    success = SimpleNamespace(append_result=SimpleNamespace(offset=0))
    row_errors = SimpleNamespace(row_errors=[SimpleNamespace(index=0)])
    grpc_error = SimpleNamespace(
        error=SimpleNamespace(code=code_pb2.RESOURCE_EXHAUSTED, message="throttled")
    )
    unknown = SimpleNamespace()

    success_result = ExtendedErrorPolicy.classify_append_rows_response(
        success, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )
    row_error_result = ExtendedErrorPolicy.classify_append_rows_response(
        row_errors, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )
    grpc_error_result = ExtendedErrorPolicy.classify_append_rows_response(
        grpc_error, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )
    unknown_result = ExtendedErrorPolicy.classify_append_rows_response(
        unknown, stream=STREAM, stream_mode=StreamMode.COMMITTED
    )

    assert success_result is None
    assert row_error_result is not None
    assert row_error_result.category == ErrorCategory.ROW_ERRORS
    assert row_error_result.send_to_dlq is True
    assert grpc_error_result is not None
    assert grpc_error_result.category == ErrorCategory.THROTTLE
    assert unknown_result is not None
    assert unknown_result.category == ErrorCategory.UNKNOWN
    assert unknown_result.fatal_state is True


def test_classify_result_dispatch_smoke() -> None:
    exc_result = ExtendedErrorPolicy.classify_result(
        TimeoutError("append timed out"),
        stream=STREAM,
        stream_mode=StreamMode.COMMITTED,
    )
    response_result = ExtendedErrorPolicy.classify_result(
        SimpleNamespace(append_result=SimpleNamespace(offset=0)),
        stream=STREAM,
        stream_mode=StreamMode.COMMITTED,
    )

    assert exc_result is not None
    assert exc_result.category == ErrorCategory.DEADLINE
    assert response_result is None

