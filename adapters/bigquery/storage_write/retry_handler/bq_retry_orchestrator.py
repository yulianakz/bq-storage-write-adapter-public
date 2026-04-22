import logging
import random
import time
from dataclasses import replace
from typing import Any, Sequence

from adapters.bigquery.storage_write.bq_storage_write_destination import BigQueryStorageWriteDestination
from adapters.bigquery.storage_write.bq_storage_write_models import DestinationWriteStats
from adapters.bigquery.storage_write.retry_handler.bq_retry_orchestrator_models import (
    RetryOrchestratorStats,
)
from adapters.bigquery.storage_write.retry_handler.error_types import ErrorCategory
from adapters.bigquery.storage_write.retry_handler.writeapierror import (
    BigQueryStorageWriteError,
)
from ports import Destination

logger = logging.getLogger(__name__)


class BigQueryWriteRetryOrchestrator(Destination[dict[str, Any], RetryOrchestratorStats]):

    _DEFAULT_ROWS_PREVIEW_LIMIT = 3
    _MIN_BACKOFF_SECONDS = 0.1
    _MAX_LOOP_ITERATIONS = 10

    RETRY_ATTEMPTS_BY_CATEGORY: dict[ErrorCategory, int] = {
        ErrorCategory.THROTTLE: 3,
        ErrorCategory.TRANSPORT: 2,
        ErrorCategory.DEADLINE: 2,
        ErrorCategory.INTERNAL: 1,
        ErrorCategory.STREAM_INVALIDATED: 1,
        ErrorCategory.OFFSET_OUT_OF_RANGE: 1,
        ErrorCategory.ROW_ERRORS: 0,
        ErrorCategory.ALREADY_EXISTS: 0,
        ErrorCategory.INVALID_ARGUMENT: 0,
        ErrorCategory.PERMISSION: 0,
        ErrorCategory.AUTH: 0,
        ErrorCategory.NOT_FOUND: 0,
        ErrorCategory.UNKNOWN: 0,
        ErrorCategory.SUCCESS: 0,
    }

    def __init__(self, destination: BigQueryStorageWriteDestination) -> None:
        self._destination = destination

    def mark_job_succeeded(self) -> None:
        self._destination.mark_job_succeeded()

    def _stream_mode_label(self) -> str | None:
        """
        Best-effort stream mode extraction for logging/metrics.
        """
        config = getattr(self._destination, "_config", None)
        mode = getattr(config, "stream_mode", None)
        if mode is None:
            return None
        return getattr(mode, "value", str(mode))

    @classmethod
    def max_attempts_for_error(cls, error: BigQueryStorageWriteError) -> int:
        if error.category is None:
            return 0
        return cls.RETRY_ATTEMPTS_BY_CATEGORY.get(error.category, 0)

    @staticmethod
    def _compute_backoff_seconds(
        *,
        category: ErrorCategory | None,
        retry_attempt: int,
    ) -> float:
        """
        Compute exponential backoff with full jitter.

        retry_attempt is 1-based:
        - retry_attempt=1 means first retry after initial failure.
        - retry_attempt=2 means second retry, etc.
        """
        base_by_category: dict[ErrorCategory, float] = {
            ErrorCategory.THROTTLE: 1.0,
            ErrorCategory.TRANSPORT: 0.5,
            ErrorCategory.DEADLINE: 1.0,
            ErrorCategory.INTERNAL: 0.5,
            ErrorCategory.STREAM_INVALIDATED: 0.25,
            ErrorCategory.OFFSET_OUT_OF_RANGE: 0.25,
        }
        cap_by_category: dict[ErrorCategory, float] = {
            ErrorCategory.THROTTLE: 15.0,
            ErrorCategory.TRANSPORT: 8.0,
            ErrorCategory.DEADLINE: 10.0,
            ErrorCategory.INTERNAL: 5.0,
            ErrorCategory.STREAM_INVALIDATED: 2.0,
            ErrorCategory.OFFSET_OUT_OF_RANGE: 2.0,
        }

        default_base = 0.5
        default_cap = 5.0

        base = base_by_category.get(category, default_base)
        cap = cap_by_category.get(category, default_cap)
        exponential = min(cap, base * (2 ** max(0, retry_attempt - 1)))

        # Keep a tiny floor to avoid immediate hot-loop retries.
        floor = BigQueryWriteRetryOrchestrator._MIN_BACKOFF_SECONDS
        if exponential <= floor:
            return exponential
        return random.uniform(floor, exponential)

    def _finalize_loop_guard_exit(
        self,
        *,
        rows: Sequence[dict[str, Any]],
        rows_preview: list[dict[str, Any]],
        loop_iterations: int,
        retry_attempt: int,
        retries_by_category: dict[str, int],
        max_retries_for_last_error: int | None,
        last_error: BigQueryStorageWriteError | None,
        last_destination_stats: DestinationWriteStats | None,
    ) -> RetryOrchestratorStats:
        """Terminate the retry loop and honour the hybrid return contract.

        "Retry loop exhausted / pathological retry cycle" is a business outcome,
        not an invariant fault, so we return a RetryOrchestratorStats carrying a
        synthesized fatal error. The pipeline's main-write branch then applies
        the same terminal-log + should-fail + raise policy it uses for every
        other non-recoverable error.
        """

        if last_destination_stats is None:
            logger.error(
                "Orchestrator loop guard fired without any destination attempt recorded",
                extra={
                    "loop_iterations": loop_iterations,
                    "rows_count": len(rows),
                },
            )
            last_destination_stats = DestinationWriteStats(
                ok=False,
                attempted_rows=len(rows),
                written_rows=0,
                failed_rows=len(rows),
                stream_mode=self._stream_mode_label(),
            )

        loop_guard_error = BigQueryStorageWriteError(
            "Storage Write retry loop exceeded safety iteration limit",
            stream=(last_error.stream if last_error else None),
            original_exception=last_error,
            retryable=False,
            needs_reset=True,
            offset_alignment=False,
            fatal_state=True,
            advance_offset=False,
            send_to_dlq=False,
            category=ErrorCategory.UNKNOWN,
        )

        logger.error(
            "Storage Write aborted by orchestrator loop safety guard",
            extra={
                "loop_iterations": loop_iterations,
                "rows_count": len(rows),
                "rows_preview": rows_preview,
                "retry_attempts_total": retry_attempt,
                "retries_by_category": retries_by_category or None,
                "last_error_category": (
                    last_error.category.value if last_error and last_error.category else None
                ),
                "last_status_code": last_error.status_code if last_error else None,
                "last_dest_written_rows": last_destination_stats.written_rows,
                "last_dest_failed_rows": last_destination_stats.failed_rows,
                "last_dest_serializer_failure_count": last_destination_stats.serializer_row_failure_count,
                "last_dest_stream_mode": last_destination_stats.stream_mode,
            },
        )

        # Surface the synthesized loop-guard error via destination_stats.error so the
        # pipeline's terminal-log/raise branch picks it up (it reads destination_stats.error,
        # not last_error, for business decisions).
        dest_stats_with_loop_guard_error = replace(last_destination_stats, error=loop_guard_error)

        return RetryOrchestratorStats(
            destination_stats=dest_stats_with_loop_guard_error,
            retry_attempts_total=retry_attempt,
            retries_by_category=retries_by_category or None,
            max_retries_for_last_error=max_retries_for_last_error,
            last_error=loop_guard_error,
            last_error_category=ErrorCategory.UNKNOWN.value,
            last_status_code=None,
        )

    def write(self, rows: Sequence[dict[str, Any]]) -> RetryOrchestratorStats:
        """
        Hybrid contract:
        - Expected business outcomes return RetryOrchestratorStats.
        - Invariant/unexpected runtime faults raise.
        """

        # Retry-attempt counter is retries after first failed write.
        retry_attempt = 0
        loop_iterations = 0
        rows_preview = list(rows[: self._DEFAULT_ROWS_PREVIEW_LIMIT])
        retries_by_category: dict[str, int] = {}
        max_retries_for_last_error: int | None = None
        last_error: BigQueryStorageWriteError | None = None
        last_destination_stats: DestinationWriteStats | None = None

        while True:
            loop_iterations += 1
            if loop_iterations > self._MAX_LOOP_ITERATIONS:
                return self._finalize_loop_guard_exit(
                    rows=rows,
                    rows_preview=rows_preview,
                    loop_iterations=loop_iterations,
                    retry_attempt=retry_attempt,
                    retries_by_category=retries_by_category,
                    max_retries_for_last_error=max_retries_for_last_error,
                    last_error=last_error,
                    last_destination_stats=last_destination_stats,
                )

            try:
                destination_stats = self._destination.write(rows)

            except Exception:
                logger.exception(
                    "Unexpected exception from destination.write",
                    extra={"rows_count": len(rows), "rows_preview": rows_preview},
                )
                raise

            last_destination_stats = destination_stats
            err = destination_stats.error
            # Prefer stream mode from returned stats; helper remains a fallback.
            stream_mode = destination_stats.stream_mode or self._stream_mode_label()

            # Success path: first-attempt success or success after retries.
            if err is None:
                return RetryOrchestratorStats(
                    destination_stats=destination_stats,
                    retry_attempts_total=retry_attempt,
                    retries_by_category=retries_by_category or None,
                    max_retries_for_last_error=max_retries_for_last_error,
                    last_error=last_error,
                    last_error_category=(
                        last_error.category.value if (last_error and last_error.category) else None
                    ),
                    last_status_code=(last_error.status_code if last_error else None),
                )

            last_error = err
            max_retries = self.max_attempts_for_error(err)
            max_retries_for_last_error = max_retries

            category_label = err.category.value if err.category else "unknown"
            retry_reason = (
                f"{stream_mode}:{category_label}" if err.should_retry and stream_mode else category_label
            )

            # Terminal branch A: error is not retryable.
            if not err.should_retry:
                logger.error(
                    "Storage Write failed (non-retryable)",
                    extra={
                        "error": err.message,
                        "category": err.category.value if err.category else None,
                        "retry_reason": retry_reason,
                        "retry_attempts_used": retry_attempt,
                        "max_retries": max_retries,
                        "rows_count": len(rows),
                        "rows_preview": rows_preview,
                        "stream_mode": stream_mode,
                        "stream": err.stream,
                        "status_code": err.status_code,
                        "requires_stream_reset": err.requires_stream_reset,
                        "requires_offset_alignment": err.requires_offset_alignment,
                        "has_row_errors": err.has_row_errors,
                        "should_send_to_dlq": err.should_send_to_dlq,
                        "is_fatal": err.is_fatal,
                    },
                )

                return RetryOrchestratorStats(
                    destination_stats=destination_stats,
                    retry_attempts_total=retry_attempt,
                    retries_by_category=retries_by_category or None,
                    max_retries_for_last_error=max_retries_for_last_error,
                    last_error=err,
                    last_error_category=(err.category.value if err.category else None),
                    last_status_code=err.status_code,
                )

            # Terminal branch B: retry budget exhausted.
            if retry_attempt >= max_retries:
                logger.error(
                    "Storage Write failed (retries exhausted)",
                    extra={
                        "error": err.message,
                        "category": err.category.value if err.category else None,
                        "retry_reason": retry_reason,
                        "retry_attempts_used": retry_attempt,
                        "max_retries": max_retries,
                        "rows_count": len(rows),
                        "rows_preview": rows_preview,
                        "stream_mode": stream_mode,
                        "stream": err.stream,
                        "status_code": err.status_code,
                        "requires_stream_reset": err.requires_stream_reset,
                        "requires_offset_alignment": err.requires_offset_alignment,
                        "has_row_errors": err.has_row_errors,
                        "should_send_to_dlq": err.should_send_to_dlq,
                        "is_fatal": err.is_fatal,
                    },
                )

                return RetryOrchestratorStats(
                    destination_stats=destination_stats,
                    retry_attempts_total=retry_attempt,
                    retries_by_category=retries_by_category or None,
                    max_retries_for_last_error=max_retries_for_last_error,
                    last_error=err,
                    last_error_category=(err.category.value if err.category else None),
                    last_status_code=err.status_code,
                )

            # Retry branch.
            retry_attempt += 1
            retries_by_category[category_label] = retries_by_category.get(category_label, 0) + 1
            sleep_seconds = self._compute_backoff_seconds(
                category=err.category,
                retry_attempt=retry_attempt,
            )

            logger.info(
                "Retrying Storage Write append after retryable error",
                extra={
                    "error": err.message,
                    "category": err.category.value if err.category else None,
                    "retry_reason": retry_reason,
                    "retry_attempt": retry_attempt,
                    "max_retries": max_retries,
                    "sleep_seconds": sleep_seconds,
                    "rows_count": len(rows),
                    "rows_preview": rows_preview,
                    "stream_mode": stream_mode,
                    "stream": err.stream,
                    "status_code": err.status_code,
                    "requires_stream_reset": err.requires_stream_reset,
                    "requires_offset_alignment": err.requires_offset_alignment,
                },
            )
            time.sleep(sleep_seconds)

    def close(self) -> None:
        self._destination.close()

    def close_failed(self) -> bool:
        close_failed = getattr(self._destination, "close_failed", None)
        if callable(close_failed):
            return bool(close_failed())
        return False