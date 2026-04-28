from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Sequence

import config
from adapters.bigquery.storage_write.proto_schema.bq_proto_schema import BQProtoSchemaBuilder
from adapters.bigquery.storage_write.row_serializer.bq_proto_serializer import BQProtoRowSerializer
from adapters.bigquery.storage_write.row_serializer.serializer_models import (
    RowSerializationError,
)
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer

# Avoid a circular import at runtime: `retry_handler.extended_error_policy` imports
# `StreamMode` from this module, and `retry_handler/__init__.py` eagerly loads
# `extended_error_policy`. Keeping the error type as a forward reference here breaks the
# cycle without affecting dataclass semantics.
if TYPE_CHECKING:
    from adapters.bigquery.storage_write.retry_handler.write_api_error import (
        BigQueryStorageWriteError,
    )


class StreamMode(str, Enum):
    COMMITTED = "committed"
    PENDING = "pending"

class StreamLifecycleState(Enum):
    OPEN = "open"
    FINALIZED = "finalized"
    COMMITTED = "committed"
    CLOSED = "closed"


@dataclass(frozen=True)
class StorageWriteConfig:
    # --- Target table identity ---
    project_id: str
    dataset_id: str
    table_id: str

    # --- Stream behavior ---
    stream_mode: StreamMode

    # --- Schema/proto contract (explicit by caller; no hardcoded defaults) ---
    schema_supplier: Callable[[], Sequence[Any]]
    proto_message_name: str

    # --- Client/build/runtime knobs ---
    write_client_factory: Callable[[], bigquery_storage_v1.BigQueryWriteClient] = (
        bigquery_storage_v1.BigQueryWriteClient
    )
    proto_schema_builder: BQProtoSchemaBuilder | None = None
    append_timeout_seconds: int = 60

    # --- AppendRows request limits (wire/protobuf transport constraints) ---
    # Target upper bound for one AppendRowsRequest in bytes.
    append_request_max_bytes: int = config.DEFAULT_APPEND_REQUEST_MAX_BYTES
    # Reserved bytes to account for request framing/metadata/schema overhead.
    append_request_overhead_bytes: int = config.DEFAULT_APPEND_REQUEST_OVERHEAD_BYTES
    # Max rows per append request before planner starts a new chunk.
    append_max_rows: int = config.DEFAULT_APPEND_MAX_ROWS
    # Optional explicit row payload cap in bytes; if None, derived as a
    # defensive fraction of request budget.
    append_row_max_bytes: int | None = config.DEFAULT_APPEND_ROW_MAX_BYTES


@dataclass
class StorageWriteSession:
    # --- Session resources ---
    write_client: bigquery_storage_v1.BigQueryWriteClient
    stream_name: str
    proto_schema: types.ProtoSchema
    row_serializer: BQProtoRowSerializer
    append_rows_stream: writer.AppendRowsStream

    # --- Session state ---
    next_offset: int = 0
    state: StreamLifecycleState = field(default=StreamLifecycleState.OPEN)


@dataclass(frozen=True)
class DestinationWriteStats:
    # --- Outcome ---
    ok: bool

    # --- Batch totals (per write() attempt) ---
    total_rows: int
    total_written_rows: int
    total_failed_rows: int

    # --- Limits stage telemetry ---
    # Row-count totals for resolve_write_limits() stage.
    # Exactly one of these is non-zero for non-empty input.
    resolve_write_limit_passed: int = 0
    resolve_write_limit_failed: int = 0

    # --- Serializer stage telemetry ---
    # Rows that entered serializer stage (0 when limits stage failed).
    serializer_attempted_rows: int = 0
    serializer_rows_passed: int = 0
    serializer_rows_failed: int = 0

    # Detailed serializer diagnostics (structured row-level reasons).
    serializer_row_failures: list[RowSerializationError] | None = None

    # --- Chunk planner telemetry ---
    chunk_planner_attempted: int = 0
    derived_chunk_count: int = 0
    # Ordered row-counts per planned chunk, e.g. [200, 200, 100].
    derived_chunks_len: list[int] | None = None

    # --- AppendRows send telemetry ---
    append_rows_send_attempted: int = 0
    append_rows_send_passed: int = 0
    append_rows_send_failed: int = 0

    # Rows whose chunk was skipped because the server reported ALREADY_EXISTS
    # at their offset window.
    skipped_already_exists_rows: int = 0

    # --- Row-errors diagnostics ---
    row_error_bad_count: int = 0
    row_error_good_count: int = 0

    # When append fails with row_errors, indices refer to serialized chunk rows;
    # these lists map back to original input dicts.
    row_error_bad_rows: list[dict[str, Any]] | None = None
    row_error_good_rows: list[dict[str, Any]] | None = None

    # --- Error / context ---
    error: BigQueryStorageWriteError | None = None
    stream_mode: str | None = None

