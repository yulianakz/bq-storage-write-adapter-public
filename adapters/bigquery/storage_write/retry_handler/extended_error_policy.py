from __future__ import annotations

from concurrent.futures import TimeoutError
from typing import Any

from google.cloud.bigquery_storage_v1.types import AppendRowsResponse
from google.rpc import code_pb2

from adapters.bigquery.storage_write.bq_storage_write_models import StreamMode
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.write_api_error import BigQueryStorageWriteError

try:
    from google.api_core import exceptions as gax_exceptions
except Exception:
    gax_exceptions = None


class ExtendedErrorPolicy:

    """
    Extended policy that unifies gRPC classification for both:
    - response-level failures (AppendRowsResponse.error)
    - exception-level failures (google.api_core.exceptions.* from send/result)
    NOTE: CANCELLED intent-aware handling is not implemented yet.
    Until caller lifecycle context is passed (e.g. intentional close signal),
    CANCELLED is intentionally not mapped in grpc_code_mapping.
    """

    @staticmethod
    def classify_result(
        event: Exception | AppendRowsResponse,
        *,
        stream: str,
        stream_mode: StreamMode
    ) -> BigQueryStorageWriteError | None:

        if isinstance(event, Exception):
            return ExtendedErrorPolicy.classify_exception(
                event, stream=stream, stream_mode=stream_mode
            )
        return ExtendedErrorPolicy.classify_append_rows_response(
            event, stream=stream, stream_mode=stream_mode
        )

    @staticmethod
    def classify_exception(
        exc: Exception, *, stream: str, stream_mode: StreamMode
    ) -> BigQueryStorageWriteError:

        """
        Classify transport-level exceptions (timeouts, connection resets, etc.).
        The destination/orchestrator decides how to act on the returned flags:
        retry, reset stream, and whether the offset may be consumed.
        """

        # Timeout: retry safely with session reset.
        if isinstance(exc, TimeoutError):
            return BigQueryStorageWriteError(
                "BigQuery Storage Write append timed out",
                stream=stream,
                original_exception=exc,
                retryable=True,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.DEADLINE,
            )

        # Transport/network failures: retry and reset session.
        if isinstance(exc, (ConnectionError, BrokenPipeError, OSError)):
            return BigQueryStorageWriteError(
                "BigQuery Storage Write transport failure",
                stream=stream,
                original_exception=exc,
                retryable=True,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.TRANSPORT,
            )

        # API-core failures are common for bidi stream failures.
        root = ExtendedErrorPolicy._unwrap_retry_error(exc)
        mapped = ExtendedErrorPolicy._classify_google_api_exception(
            root, stream=stream, stream_mode=stream_mode
        )
        if mapped is None:
            return ExtendedErrorPolicy.fallback_classifier(exc, stream)

        # Preserve top-level exception for diagnostics even if we classified
        # using an unwrapped cause.
        mapped.original_exception = exc
        return mapped


    @staticmethod
    def classify_append_rows_response(
        response: AppendRowsResponse, *, stream: str, stream_mode: StreamMode
    ) -> BigQueryStorageWriteError | None:

        """
        Classify an AppendRowsResponse.
        Returns None if the response indicates success (AppendRowsResponse.append_result).
        Otherwise, returns a BigQueryStorageWriteError describing what the caller should do.
        """

        # Contract: append_result implies success.
        if getattr(response, "append_result", None) is not None:
            return None

        # BigQuery Storage Write API contract:
        # If row_errors are returned, the request failed due to corrupted rows and
        # the entire batch is rejected:
        # - no rows are appended
        # - safe to remove bad rows and retry the same batch
        # Ref: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsresponse
        row_errors = getattr(response, "row_errors", None)
        row_errors_list = list(row_errors) if row_errors is not None else []
        if row_errors_list:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write append rejected due to row errors",
                stream=stream,
                row_errors=row_errors_list,
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=True,
                category=ErrorCategory.ROW_ERRORS,
            )

        # Response-level error (gRPC status via response.error).
        status = getattr(response, "error", None)
        if status is not None:
            code = ExtendedErrorPolicy._normalize_grpc_status_code(getattr(status, "code", None))
            status_message = getattr(status, "message", None)
            return ExtendedErrorPolicy.grpc_code_mapping(
                code=code,
                status_message=status_message,
                stream=stream,
                stream_mode=stream_mode,
            )

        return BigQueryStorageWriteError(
            "Unknown AppendRowsResponse failure (no append_result, no row_errors, no error)",
            stream=stream,
            original_exception=None,
            retryable=False,
            needs_reset=False,
            offset_alignment=False,
            fatal_state=True,
            advance_offset=False,
            send_to_dlq=False,
            category=ErrorCategory.UNKNOWN,
        )

    @staticmethod
    def _unwrap_retry_error(exc: Exception) -> Exception:
        if gax_exceptions is None:
            return exc

        current: Exception = exc
        visited: set[int] = set()
        # Guard against accidental cause cycles.
        while id(current) not in visited:
            visited.add(id(current))
            if not isinstance(current, gax_exceptions.RetryError):
                break
            cause = getattr(current, "cause", None)
            if not isinstance(cause, Exception):
                break
            current = cause
        return current


    @staticmethod
    def _classify_google_api_exception(
        exc: Exception, *, stream: str, stream_mode: StreamMode
    ) -> BigQueryStorageWriteError | None:
        if gax_exceptions is None:
            return None

        if not isinstance(exc, gax_exceptions.GoogleAPICallError):
            return None

        grpc_code = ExtendedErrorPolicy._normalize_grpc_status_code(
            getattr(exc, "grpc_status_code", None)
        )
        if grpc_code is not None:
            return ExtendedErrorPolicy.grpc_code_mapping(
                code=grpc_code,
                status_message=str(exc),
                stream=stream,
                stream_mode=stream_mode,
            )

        # Fallback for api-core exceptions without grpc_status_code:
        # map known subclasses into grpc_code_mapping to avoid UNKNOWN fatal
        # classification for common service/client errors.
        exception_code_fallbacks = (
            (gax_exceptions.ResourceExhausted, code_pb2.RESOURCE_EXHAUSTED),
            (gax_exceptions.TooManyRequests, code_pb2.RESOURCE_EXHAUSTED),
            (gax_exceptions.ServiceUnavailable, code_pb2.UNAVAILABLE),
            (gax_exceptions.DeadlineExceeded, code_pb2.DEADLINE_EXCEEDED),
            (gax_exceptions.InternalServerError, code_pb2.INTERNAL),
            (gax_exceptions.Aborted, code_pb2.ABORTED),
            (gax_exceptions.InvalidArgument, code_pb2.INVALID_ARGUMENT),
            (gax_exceptions.BadRequest, code_pb2.INVALID_ARGUMENT),
            (gax_exceptions.PermissionDenied, code_pb2.PERMISSION_DENIED),
            (gax_exceptions.Forbidden, code_pb2.PERMISSION_DENIED),
            (gax_exceptions.Unauthenticated, code_pb2.UNAUTHENTICATED),
            (gax_exceptions.NotFound, code_pb2.NOT_FOUND),
            (gax_exceptions.AlreadyExists, code_pb2.ALREADY_EXISTS),
            (gax_exceptions.FailedPrecondition, code_pb2.FAILED_PRECONDITION),
            (gax_exceptions.OutOfRange, code_pb2.OUT_OF_RANGE),
        )

        for exception_type, fallback_code in exception_code_fallbacks:
            if isinstance(exc, exception_type):
                return ExtendedErrorPolicy.grpc_code_mapping(
                    code=fallback_code,
                    status_message=str(exc),
                    stream=stream,
                    stream_mode=stream_mode,
                )

        return None

    @staticmethod
    def _normalize_grpc_status_code(code: Any) -> int | None:
        if code is None:
            return None
        if isinstance(code, int):
            return code

        # grpc.StatusCode (enum) usually exposes .name; map by symbolic name.
        name = getattr(code, "name", None)
        if isinstance(name, str) and hasattr(code_pb2, name):
            return int(getattr(code_pb2, name))

        # Some wrappers may expose integer value as a tuple in .value.
        value = getattr(code, "value", None)
        if isinstance(value, int):
            return value
        if (
            isinstance(value, tuple)
            and len(value) > 0
            and isinstance(value[0], int)
        ):
            return value[0]
        return None


    @staticmethod
    def grpc_code_mapping(
        *,
        code: int | None,
        status_message: str | None,
        stream: str,
        stream_mode: StreamMode,
    ) -> BigQueryStorageWriteError:

        """
        Map gRPC/BigQuery status codes to a Phase-1 retry/reset/offset policy.
        """

        if code == code_pb2.ALREADY_EXISTS:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write append: offset already committed",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=True,
                send_to_dlq=False,
                needs_continue_loop=True,
                category=ErrorCategory.ALREADY_EXISTS,
            )

        if code == code_pb2.RESOURCE_EXHAUSTED:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write append throttled / resource exhausted",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=True,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.THROTTLE,
            )

        if code == code_pb2.UNAVAILABLE:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write append service unavailable",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=True,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.TRANSPORT,
            )

        if code == code_pb2.DEADLINE_EXCEEDED:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write append deadline exceeded",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=True,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.DEADLINE,
            )

        if code in (code_pb2.INTERNAL, code_pb2.ABORTED):
            return BigQueryStorageWriteError(
                "BigQuery Storage Write internal/aborted server error",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=True,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=False,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.INTERNAL,
            )

        if code == code_pb2.OUT_OF_RANGE:
            if stream_mode == StreamMode.COMMITTED:
                return BigQueryStorageWriteError(
                    "BigQuery Storage Write offset mismatch (committed stream): re-sync needed",
                    stream=stream,
                    status_code=code,
                    status_message=status_message,
                    row_errors=[],
                    original_exception=None,
                    retryable=True,
                    needs_reset=True,
                    offset_alignment=True,
                    fatal_state=False,
                    advance_offset=False,
                    send_to_dlq=False,
                    category=ErrorCategory.OFFSET_OUT_OF_RANGE,
                )

            return BigQueryStorageWriteError(
                "BigQuery Storage Write offset mismatch (pending stream)",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.OFFSET_OUT_OF_RANGE,
            )

        if code == code_pb2.FAILED_PRECONDITION:
            if stream_mode == StreamMode.COMMITTED:
                return BigQueryStorageWriteError(
                    "BigQuery Storage Write stream invalidated (committed stream)",
                    stream=stream,
                    status_code=code,
                    status_message=status_message,
                    row_errors=[],
                    original_exception=None,
                    retryable=True,
                    needs_reset=True,
                    offset_alignment=False,
                    fatal_state=False,
                    advance_offset=False,
                    send_to_dlq=False,
                    category=ErrorCategory.STREAM_INVALIDATED,
                )

            return BigQueryStorageWriteError(
                "BigQuery Storage Write pending stream invalidated/finalized",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.STREAM_INVALIDATED,
            )

        if code == code_pb2.INVALID_ARGUMENT:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write invalid argument",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=True,
                category=ErrorCategory.INVALID_ARGUMENT,
            )

        if code == code_pb2.PERMISSION_DENIED:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write permission denied",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.PERMISSION,
            )

        if code == code_pb2.UNAUTHENTICATED:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write authentication failed",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=False,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.AUTH,
            )

        if code == code_pb2.NOT_FOUND:
            return BigQueryStorageWriteError(
                "BigQuery Storage Write resource not found; stream recreation required",
                stream=stream,
                status_code=code,
                status_message=status_message,
                row_errors=[],
                original_exception=None,
                retryable=False,
                needs_reset=True,
                offset_alignment=False,
                fatal_state=True,
                advance_offset=False,
                send_to_dlq=False,
                category=ErrorCategory.NOT_FOUND,
            )

        return BigQueryStorageWriteError(
            f"BigQuery Storage Write unknown gRPC error code: {code}",
            stream=stream,
            status_code=code,
            status_message=status_message,
            row_errors=[],
            original_exception=None,
            retryable=False,
            needs_reset=False,
            offset_alignment=False,
            fatal_state=True,
            advance_offset=False,
            send_to_dlq=False,
            category=ErrorCategory.UNKNOWN,
        )

    @staticmethod
    def fallback_classifier(exc: Exception, stream: str) -> BigQueryStorageWriteError:
        """
        Fallback classification for unexpected exceptions.
        Phase-1 policy: treat as fatal to avoid accidental retries/offset consumption.
        """
        return BigQueryStorageWriteError(
            str(exc),
            stream=stream,
            original_exception=exc,
            retryable=False,
            needs_reset=False,
            offset_alignment=False,
            fatal_state=True,
            advance_offset=False,
            send_to_dlq=False,
            category=ErrorCategory.UNKNOWN,
        )

