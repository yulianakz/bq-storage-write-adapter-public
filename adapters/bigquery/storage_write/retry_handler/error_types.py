from enum import Enum


class ErrorCategory(str, Enum):
    """
    High-level classification for BigQuery Storage Write failures.

    These categories are used for metrics/logging and for policy decisions.
    They intentionally group multiple low-level gRPC codes / exception types.
    """

    # Successful append (AppendRowsResponse.append_result present).
    SUCCESS = "success"

    # Transport / network failures (timeouts, connection resets, transient transport issues).
    TRANSPORT = "transport"

    # Deadline exceeded (request exceeded configured deadline / RPC deadline).
    DEADLINE = "deadline"

    # Unexpected internal server-side failures (e.g., INTERNAL/ABORTED).
    INTERNAL = "internal"

    # Throttling / quota / resource exhaustion (e.g., RESOURCE_EXHAUSTED).
    THROTTLE = "throttle"

    # Offset mismatch for committed streams (e.g., OUT_OF_RANGE).
    OFFSET_OUT_OF_RANGE = "offset_out_of_range"

    # Stream invalidated / needs reset (e.g., FAILED_PRECONDITION).
    STREAM_INVALIDATED = "stream_invalidated"

    # AppendRows contract: row_errors present implies the batch was rejected (no rows appended).
    ROW_ERRORS = "row_errors"

    # Offset already committed (e.g., ALREADY_EXISTS).
    ALREADY_EXISTS = "already_exists"

    # Client-side data/contract issues (e.g., INVALID_ARGUMENT).
    INVALID_ARGUMENT = "invalid_argument"

    # Authorization/permission failures (e.g., PERMISSION_DENIED).
    PERMISSION = "permission"

    # Authentication failures (e.g., UNAUTHENTICATED).
    AUTH = "auth"

    # Missing resource failures (e.g., NOT_FOUND).
    NOT_FOUND = "not_found"

    # Anything not explicitly mapped.
    UNKNOWN = "unknown"
