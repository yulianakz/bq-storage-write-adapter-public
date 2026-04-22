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

# Avoid a circular import at runtime: `retry_handler.error_policy` imports
# `StreamMode` from this module, and `retry_handler/__init__.py` eagerly loads
# `error_policy`. Keeping the error type as a forward reference here breaks the
# cycle without affecting dataclass semantics.
if TYPE_CHECKING:
    from adapters.bigquery.storage_write.retry_handler.writeapierror import (
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
    # Optional explicit row payload cap in bytes; if None, derived from request budget.
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

    # --- Batch counts ---
    attempted_rows: int
    written_rows: int
    failed_rows: int

    # Rows whose chunk was skipped because the server reported ALREADY_EXISTS at their offset window.
    skipped_already_exists_rows: int = 0

    # --- Error ---
    error: BigQueryStorageWriteError | None = None

    # --- Serializer diagnostics ---
    serializer_row_failures: list[RowSerializationError] | None = None
    serializer_row_failure_count: int = 0

    # --- Context ---
    stream_mode: str | None = None

    # When append fails with row_errors, indices refer to serialized chunk rows; these lists map
    # back to original input dicts (only set when the destination could derive a split).
    row_error_bad_rows: list[dict[str, Any]] | None = None
    row_error_good_rows: list[dict[str, Any]] | None = None

