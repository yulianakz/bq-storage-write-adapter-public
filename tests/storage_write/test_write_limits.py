import pytest

from adapters.bigquery.storage_write.bq_storage_write_models import (
    StorageWriteConfig,
    StreamMode,
)
from adapters.bigquery.storage_write.write_limits import (
    WRITE_API_HARD_MAX_REQUEST_BYTES,
    resolve_write_limits,
)


def _config(**overrides) -> StorageWriteConfig:
    base = StorageWriteConfig(
        project_id="project",
        dataset_id="dataset",
        table_id="table",
        stream_mode=StreamMode.COMMITTED,
        schema_supplier=lambda: [],
        proto_message_name="RowMessage",
    )
    if not overrides:
        return base
    return StorageWriteConfig(
        project_id=overrides.get("project_id", base.project_id),
        dataset_id=overrides.get("dataset_id", base.dataset_id),
        table_id=overrides.get("table_id", base.table_id),
        stream_mode=overrides.get("stream_mode", base.stream_mode),
        schema_supplier=overrides.get("schema_supplier", base.schema_supplier),
        proto_message_name=overrides.get("proto_message_name", base.proto_message_name),
        write_client_factory=overrides.get("write_client_factory", base.write_client_factory),
        proto_schema_builder=overrides.get("proto_schema_builder", base.proto_schema_builder),
        append_timeout_seconds=overrides.get(
            "append_timeout_seconds", base.append_timeout_seconds
        ),
        append_request_max_bytes=overrides.get(
            "append_request_max_bytes", base.append_request_max_bytes
        ),
        append_request_overhead_bytes=overrides.get(
            "append_request_overhead_bytes", base.append_request_overhead_bytes
        ),
        append_max_rows=overrides.get("append_max_rows", base.append_max_rows),
        append_row_max_bytes=overrides.get(
            "append_row_max_bytes", base.append_row_max_bytes
        ),
    )


def test_resolve_write_limits_happy_path_with_defaults() -> None:
    limits = resolve_write_limits(_config())
    assert limits.request_max_bytes == 9_500_000
    assert limits.request_overhead_bytes == 256_000
    assert limits.request_payload_budget_bytes == 9_244_000
    assert limits.max_rows == 500
    assert limits.row_max_bytes == 2_311_000


def test_resolve_write_limits_default_row_max_is_defensive_fraction_of_budget() -> None:
    limits = resolve_write_limits(
        _config(
            append_request_max_bytes=2_000_000,
            append_request_overhead_bytes=200_000,
            append_row_max_bytes=None,
        )
    )
    assert limits.request_payload_budget_bytes == 1_800_000
    assert limits.row_max_bytes == limits.request_payload_budget_bytes // 4


def test_resolve_write_limits_happy_path_with_explicit_row_max() -> None:
    limits = resolve_write_limits(_config(append_row_max_bytes=512_000))
    assert limits.row_max_bytes == 512_000


def test_resolve_write_limits_request_max_must_be_below_hard_limit() -> None:
    with pytest.raises(ValueError, match="append_request_max_bytes must be <"):
        resolve_write_limits(
            _config(append_request_max_bytes=WRITE_API_HARD_MAX_REQUEST_BYTES)
        )


def test_resolve_write_limits_overhead_must_be_lower_than_request_max() -> None:
    with pytest.raises(
        ValueError,
        match="append_request_overhead_bytes must be < append_request_max_bytes",
    ):
        resolve_write_limits(
            _config(
                append_request_max_bytes=1_000_000,
                append_request_overhead_bytes=1_000_000,
            )
        )


def test_resolve_write_limits_max_rows_must_be_positive() -> None:
    with pytest.raises(ValueError, match="append_max_rows must be >= 1"):
        resolve_write_limits(_config(append_max_rows=0))


def test_resolve_write_limits_explicit_row_max_cannot_exceed_budget() -> None:
    with pytest.raises(
        ValueError,
        match="append_row_max_bytes must be <= \\(append_request_max_bytes - append_request_overhead_bytes\\)",
    ):
        resolve_write_limits(
            _config(
                append_request_max_bytes=2_000_000,
                append_request_overhead_bytes=200_000,
                append_row_max_bytes=1_900_001,
            )
        )


def test_resolve_write_limits_accepts_row_max_equal_to_budget_edge_case() -> None:
    limits = resolve_write_limits(
        _config(
            append_request_max_bytes=2_000_000,
            append_request_overhead_bytes=200_000,
            append_row_max_bytes=1_800_000,
        )
    )
    assert limits.row_max_bytes == 1_800_000


def test_resolve_write_limits_rejects_negative_overhead_edge_case() -> None:
    with pytest.raises(ValueError, match="append_request_overhead_bytes must be >= 0"):
        resolve_write_limits(_config(append_request_overhead_bytes=-1))
