from dataclasses import dataclass

from adapters.bigquery.storage_write.bq_storage_write_models import DestinationWriteStats
from adapters.bigquery.storage_write.retry_handler.writeapierror import BigQueryStorageWriteError


@dataclass(frozen=True)
class FlattenedRetryOrchestratorStats:
    """
    Flattened view of retry + destination write stats.
    Useful for typed logging/metrics payloads without nested object traversal.
    """

    retry_attempts_total: int
    retries_by_category: dict[str, int] | None
    max_retries_for_last_error: int | None
    last_error_category: str | None
    last_status_code: int | None

    dest_ok: bool
    dest_attempted_rows: int
    dest_written_rows: int
    dest_failed_rows: int
    dest_serializer_row_failure_count: int
    # Rows that reached wire and were not written (failed_rows minus serializer-only drops).
    # Pipeline consumes this for main_failed accounting when there is no row_errors recovery path.
    dest_wire_rejected_rows: int
    dest_stream_mode: str | None
    dest_error_category: str | None
    dest_error_status_code: int | None


@dataclass(frozen=True)
class RetryOrchestratorStats:
    """
    Composite write result produced by the retry orchestrator.
    DestinationWriteStats captures row-level destination outcome, while this model
    adds retry-loop telemetry owned by the orchestrator layer.
    """

    destination_stats: DestinationWriteStats

    retry_attempts_total: int = 0
    retries_by_category: dict[str, int] | None = None
    max_retries_for_last_error: int | None = None

    last_error: BigQueryStorageWriteError | None = None
    last_error_category: str | None = None
    last_status_code: int | None = None

    def flatten(self) -> FlattenedRetryOrchestratorStats:
        dest_error = self.destination_stats.error
        failed_rows = self.destination_stats.failed_rows
        serializer_failure_count = self.destination_stats.serializer_row_failure_count
        wire_rejected = max(0, failed_rows - serializer_failure_count)

        return FlattenedRetryOrchestratorStats(
            retry_attempts_total=self.retry_attempts_total,
            retries_by_category=self.retries_by_category,
            max_retries_for_last_error=self.max_retries_for_last_error,
            last_error_category=self.last_error_category,
            last_status_code=self.last_status_code,
            dest_ok=self.destination_stats.ok,
            dest_attempted_rows=self.destination_stats.attempted_rows,
            dest_written_rows=self.destination_stats.written_rows,
            dest_failed_rows=failed_rows,
            dest_serializer_row_failure_count=serializer_failure_count,
            dest_wire_rejected_rows=wire_rejected,
            dest_stream_mode=self.destination_stats.stream_mode,
            dest_error_category=(dest_error.category.value if dest_error and dest_error.category else None),
            dest_error_status_code=(dest_error.status_code if dest_error else None),
        )

