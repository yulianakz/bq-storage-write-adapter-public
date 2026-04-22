from dataclasses import dataclass
from typing import Any, Callable, Sequence

from adapters.bigquery.storage_write.proto_schema.bq_proto_schema import BQProtoSchemaBuilder
from adapters.bigquery.storage_write.row_serializer.bq_proto_serializer import BQProtoRowSerializer
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from adapters.bigquery.storage_write.bq_storage_write_models import (
    StorageWriteConfig,
    StorageWriteSession,
    StreamMode,
)


@dataclass(frozen=True)
class AppendChunk:
    rows: list[bytes]
    payload_bytes: int

def sum_payload_bytes(rows: Sequence[bytes]) -> int:
    return sum(len(row) for row in rows)

def plan_append_chunks(
    *,
    serialized_rows: Sequence[bytes],
    max_rows: int,
    max_payload_bytes: int,
) -> list[AppendChunk]:

    if max_rows < 1:
        raise ValueError(f"max_rows must be >= 1, got {max_rows}")
    if max_payload_bytes < 1:
        raise ValueError(f"max_payload_bytes must be >= 1, got {max_payload_bytes}")

    planned_chunks: list[AppendChunk] = []
    current_rows: list[bytes] = []
    current_payload_bytes = 0

    for row in serialized_rows:
        row_size_bytes = len(row)
        if row_size_bytes > max_payload_bytes:
            raise ValueError(
                f"Serialized row size {row_size_bytes} exceeds max payload budget "
                f"{max_payload_bytes}"
            )

        row_limit_reached = len(current_rows) >= max_rows
        payload_limit_reached = (current_payload_bytes + row_size_bytes) > max_payload_bytes

        if current_rows and (row_limit_reached or payload_limit_reached):
            planned_chunks.append(
                AppendChunk(
                    rows=current_rows,
                    payload_bytes=current_payload_bytes,
                )
            )
            current_rows = []
            current_payload_bytes = 0

        current_rows.append(row)
        current_payload_bytes += row_size_bytes

    if current_rows:
        planned_chunks.append(
            AppendChunk(
                rows=current_rows,
                payload_bytes=current_payload_bytes,
            )
        )

    return planned_chunks


def build_table_parent(
    *,
    write_client: bigquery_storage_v1.BigQueryWriteClient,
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> str:
    return write_client.table_path(project_id, dataset_id, table_id)


def build_stream_name(
    *,
    write_client: bigquery_storage_v1.BigQueryWriteClient,
    parent: str,
    stream_mode: StreamMode,
) -> str:
    if stream_mode == StreamMode.COMMITTED:
        write_stream = types.WriteStream(type_=types.WriteStream.Type.COMMITTED)
    elif stream_mode == StreamMode.PENDING:
        write_stream = types.WriteStream(type_=types.WriteStream.Type.PENDING)
    else:
        raise ValueError(f"Unsupported stream_mode: {stream_mode!r}")
    created = write_client.create_write_stream(
        parent=parent,
        write_stream=write_stream,
    )
    return created.name


def build_proto_schema_and_serializer(
    *,
    schema_supplier: Callable[[], Sequence[Any]],
    proto_message_name: str,
    proto_schema_builder: BQProtoSchemaBuilder | None,
) -> tuple[types.ProtoSchema, BQProtoRowSerializer]:
    schema_fields = schema_supplier()
    builder = proto_schema_builder or BQProtoSchemaBuilder.instance()
    proto_build = builder.build(schema_fields=schema_fields, message_name=proto_message_name)
    row_serializer = BQProtoRowSerializer(
        message_cls=proto_build.message_cls,
        field_specs=proto_build.field_specs,
    )
    return proto_build.proto_schema, row_serializer


def build_append_rows_stream(
    *,
    write_client: bigquery_storage_v1.BigQueryWriteClient,
    stream_name: str,
    proto_schema: types.ProtoSchema,
) -> writer.AppendRowsStream:
    request_template = types.AppendRowsRequest(
        write_stream=stream_name,
        proto_rows=types.AppendRowsRequest.ProtoData(writer_schema=proto_schema),
    )
    return writer.AppendRowsStream(write_client, request_template)


def finalize_write_stream(
    client: bigquery_storage_v1.BigQueryWriteClient,
    stream_name: str,
) -> types.FinalizeWriteStreamResponse:
    request = types.FinalizeWriteStreamRequest(name=stream_name)
    return client.finalize_write_stream(request=request)


def batch_commit_write_streams(
    client: bigquery_storage_v1.BigQueryWriteClient,
    parent: str,
    stream_names: Sequence[str],
) -> types.BatchCommitWriteStreamsResponse:
    request = types.BatchCommitWriteStreamsRequest(
        parent=parent,
        write_streams=list(stream_names),
    )
    return client.batch_commit_write_streams(request=request)


def create_storage_write_session(*, config: StorageWriteConfig) -> StorageWriteSession:
    write_client = config.write_client_factory()
    parent = build_table_parent(
        write_client=write_client,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        table_id=config.table_id,
    )
    stream_name = build_stream_name(
        write_client=write_client,
        parent=parent,
        stream_mode=config.stream_mode,
    )
    proto_schema, row_serializer = build_proto_schema_and_serializer(
        schema_supplier=config.schema_supplier,
        proto_message_name=config.proto_message_name,
        proto_schema_builder=config.proto_schema_builder,
    )
    append_rows_stream = build_append_rows_stream(
        write_client=write_client,
        stream_name=stream_name,
        proto_schema=proto_schema,
    )
    return StorageWriteSession(
        write_client=write_client,
        stream_name=stream_name,
        proto_schema=proto_schema,
        row_serializer=row_serializer,
        append_rows_stream=append_rows_stream,
    )

