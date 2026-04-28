from dataclasses import dataclass

from adapters.bigquery.storage_write.bq_storage_write_models import DestinationWriteStats
from adapters.bigquery.storage_write.retry_handler.write_api_error import BigQueryStorageWriteError


@dataclass(frozen=True)
class FlattenedRetryOrchestratorStats:
    """
    Flattened view of retry + destination write stats.
    Useful for typed logging/metrics payloads without nested object traversal.
    """

    retry_attempts_total: int
    total_rows_passed_to_retry: int
    retries_by_category: dict[str, int] | None
    max_retries_for_last_error: int | None
    last_error_category: str | None
    last_status_code: int | None

    dest_ok: bool
    dest_total_rows: int
    dest_total_written_rows: int
    dest_total_failed_rows: int
    dest_resolve_write_limit_passed: int
    dest_resolve_write_limit_failed: int
    dest_serializer_attempted_rows: int
    dest_serializer_rows_passed: int
    dest_serializer_rows_failed: int
    dest_chunk_planner_attempted: int
    dest_derived_chunk_count: int
    dest_derived_chunks_len: list[int] | None
    dest_append_rows_send_attempted: int
    dest_append_rows_send_passed: int
    dest_append_rows_send_failed: int
    dest_skipped_already_exists_rows: int
    dest_row_error_bad_count: int
    dest_row_error_good_count: int
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
    total_rows_passed_to_retry: int = 0
    retries_by_category: dict[str, int] | None = None
    max_retries_for_last_error: int | None = None

    last_error: BigQueryStorageWriteError | None = None
    last_error_category: str | None = None
    last_status_code: int | None = None

    def flatten(self) -> FlattenedRetryOrchestratorStats:
        dest_error = self.destination_stats.error
        return FlattenedRetryOrchestratorStats(
            retry_attempts_total=self.retry_attempts_total,
            total_rows_passed_to_retry=self.total_rows_passed_to_retry,
            retries_by_category=self.retries_by_category,
            max_retries_for_last_error=self.max_retries_for_last_error,
            last_error_category=self.last_error_category,
            last_status_code=self.last_status_code,
            dest_ok=self.destination_stats.ok,
            dest_total_rows=self.destination_stats.total_rows,
            dest_total_written_rows=self.destination_stats.total_written_rows,
            dest_total_failed_rows=self.destination_stats.total_failed_rows,
            dest_resolve_write_limit_passed=self.destination_stats.resolve_write_limit_passed,
            dest_resolve_write_limit_failed=self.destination_stats.resolve_write_limit_failed,
            dest_serializer_attempted_rows=self.destination_stats.serializer_attempted_rows,
            dest_serializer_rows_passed=self.destination_stats.serializer_rows_passed,
            dest_serializer_rows_failed=self.destination_stats.serializer_rows_failed,
            dest_chunk_planner_attempted=self.destination_stats.chunk_planner_attempted,
            dest_derived_chunk_count=self.destination_stats.derived_chunk_count,
            dest_derived_chunks_len=self.destination_stats.derived_chunks_len,
            dest_append_rows_send_attempted=self.destination_stats.append_rows_send_attempted,
            dest_append_rows_send_passed=self.destination_stats.append_rows_send_passed,
            dest_append_rows_send_failed=self.destination_stats.append_rows_send_failed,
            dest_skipped_already_exists_rows=self.destination_stats.skipped_already_exists_rows,
            dest_row_error_bad_count=self.destination_stats.row_error_bad_count,
            dest_row_error_good_count=self.destination_stats.row_error_good_count,
            dest_stream_mode=self.destination_stats.stream_mode,
            dest_error_category=(dest_error.category.value if dest_error and dest_error.category else None),
            dest_error_status_code=(dest_error.status_code if dest_error else None),
        )

