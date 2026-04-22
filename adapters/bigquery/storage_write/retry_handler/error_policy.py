from concurrent.futures import TimeoutError

from google.cloud.bigquery_storage_v1.types import AppendRowsResponse
from google.rpc import code_pb2

from adapters.bigquery.storage_write.bq_storage_write_models import StreamMode
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.writeapierror import BigQueryStorageWriteError


class ErrorPolicy:

    @staticmethod
    def classify_exception(exc: Exception, *, stream: str, stream_mode: StreamMode) -> BigQueryStorageWriteError:
        """
        Classify transport-level exceptions (timeouts, connection resets, etc.).
        The destination/orchestrator decides how to act on the returned flags:
        retry, reset stream, and whether the offset may be consumed.
        """

        # Timeout: retry safely by resetting the session; do not consume offset.
        # Note: in Python 3.10+ `socket.timeout` is an alias of the built-in
        # `TimeoutError`, and in 3.11+ so is `concurrent.futures.TimeoutError`.
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

        # Transport/network failures: retry and reset the session; do not consume offset.
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

        # Unknown exception: treat as fatal for safety (no retry/reset/offset advance).
        return ErrorPolicy.fallback_classifier(exc, stream)


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
            code = getattr(status, "code", None)
            status_message = getattr(status, "message", None)
            return ErrorPolicy.grpc_code_mapping(
                code=code,
                status_message=status_message,
                stream=stream,
                stream_mode=stream_mode,
            )

        # Defensive fallback: unexpected response shape.
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
            # Pure offset idempotency signal from the server: the rows at this
            # offset window are already committed (COMMITTED stream) or already
            # buffered (PENDING stream). The current chunk is a no-op: do not
            # DLQ, do not reset, advance the local offset, and keep attempting
            # subsequent chunks in the same write() call.
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
            # Throttle: retry without resetting the session.
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

        if code in (code_pb2.UNAVAILABLE,):
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
                # Commit mode should re-sync offsets from server before retrying.
                # This flag asks caller to reset/fetch before retry.
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

            # Pending mode has no strict offset contract; fail current run safely.
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

            # Pending stream often means the stream lifecycle ended/finalized.
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

        # Unknown/unsupported gRPC status code: fatal by default.
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
    def fallback_classifier(
        exc: Exception, stream: str
    ) -> BigQueryStorageWriteError:
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

