from dataclasses import dataclass
from typing import Sequence

from google.cloud.bigquery_storage_v1.types import RowError
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory


@dataclass(init=False)
class BigQueryStorageWriteError(Exception):
    message: str
    stream: str | None

    status_code: int | None
    status_message: str | None

    row_errors: list[RowError]
    original_exception: Exception | None

    retryable: bool
    needs_reset: bool
    offset_alignment: bool
    fatal_state: bool
    advance_offset: bool
    send_to_dlq: bool
    needs_continue_loop: bool

    category: ErrorCategory | None

    def __init__(
        self,
        message: str,
        *,
        stream: str | None = None,

        status_code: int | None = None,
        status_message: str | None = None,

        row_errors: Sequence[RowError] | None = None,
        original_exception: Exception | None = None,

        retryable: bool = False,
        needs_reset: bool = False,
        offset_alignment: bool = False,
        fatal_state: bool = False,
        advance_offset: bool = False,
        send_to_dlq: bool = False,
        needs_continue_loop: bool = False,

        category: ErrorCategory | None = None,
    ) -> None:

        super().__init__(message)
        self.message = message
        self.stream = stream
        self.status_code = status_code
        self.status_message = status_message
        self.row_errors = list(row_errors) if row_errors is not None else []
        self.original_exception = original_exception
        self.retryable = retryable
        self.needs_reset = needs_reset
        self.offset_alignment = offset_alignment
        self.fatal_state = fatal_state
        self.advance_offset = advance_offset
        self.send_to_dlq = send_to_dlq
        self.needs_continue_loop = needs_continue_loop
        self.category = category

    @property
    def has_row_errors(self) -> bool:
        return bool(self.row_errors)

    @property
    def should_send_to_dlq(self) -> bool:
        return self.send_to_dlq

    @property
    def should_retry(self) -> bool:
        return self.retryable and not self.fatal_state

    @property
    def requires_stream_reset(self) -> bool:
        return self.needs_reset and not self.fatal_state

    @property
    def requires_offset_alignment(self) -> bool:
        return (
            self.offset_alignment
            and self.should_retry
            and self.requires_stream_reset
        )

    @property
    def is_fatal(self) -> bool:
        return self.fatal_state

    @property
    def should_continue_loop(self) -> bool:
        """Non-fatal signal that the current chunk is a no-op at the
        # server (e.g. offset already committed). The write() loop should
        skip the chunk and keep attempting subsequent chunks."""
        return self.needs_continue_loop and not self.fatal_state

