"""Integration test for real Storage Write stream semantics.

This test is intentionally opt-in and self-skipping unless environment
variables are provided for a sandbox project or emulator endpoint.
It validates the real `AppendRowsStream.send()` path through destination.write(),
then `finalize` + `batchCommit` during destination.close() for a pending stream.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.exceptions import DefaultCredentialsError
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery_storage_v1

from adapters.bigquery.storage_write.bq_storage_write_destination import (
    BigQueryStorageWriteDestination,
)
from adapters.bigquery.storage_write.bq_storage_write_models import (
    StorageWriteConfig,
    StreamMode,
)


def _integration_target_from_env() -> tuple[str, str, str]:
    project_id = os.getenv("BQ_STORAGE_WRITE_IT_PROJECT_ID")
    dataset_id = os.getenv("BQ_STORAGE_WRITE_IT_DATASET_ID")
    table_id = os.getenv("BQ_STORAGE_WRITE_IT_TABLE_ID")
    if not (project_id and dataset_id and table_id):
        pytest.skip(
            "Set BQ_STORAGE_WRITE_IT_PROJECT_ID, BQ_STORAGE_WRITE_IT_DATASET_ID, "
            "and BQ_STORAGE_WRITE_IT_TABLE_ID to run integration test."
        )
    return project_id, dataset_id, table_id


def _write_client_factory_from_env():
    emulator_host = os.getenv("BQ_STORAGE_EMULATOR_HOST")
    if emulator_host:
        return bigquery_storage_v1.BigQueryWriteClient(
            client_options=ClientOptions(api_endpoint=emulator_host),
            credentials=AnonymousCredentials(),
        )

    try:
        return bigquery_storage_v1.BigQueryWriteClient()
    except DefaultCredentialsError as exc:
        pytest.skip(
            "No default credentials found. Configure ADC or set BQ_STORAGE_EMULATOR_HOST."
        )
        raise AssertionError("unreachable") from exc


def _schema_supplier():
    # Integration table contract for this test:
    # event_id STRING REQUIRED, value INT64 NULLABLE.
    return [
        SimpleNamespace(
            name="event_id",
            field_type="STRING",
            mode="REQUIRED",
            fields=(),
        ),
        SimpleNamespace(
            name="value",
            field_type="INT64",
            mode="NULLABLE",
            fields=(),
        ),
    ]


def test_pending_stream_send_then_finalize_and_commit_integration() -> None:
    project_id, dataset_id, table_id = _integration_target_from_env()
    destination = BigQueryStorageWriteDestination(
        config=StorageWriteConfig(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            stream_mode=StreamMode.PENDING,
            schema_supplier=_schema_supplier,
            proto_message_name="StorageWriteIntegrationRow",
            write_client_factory=_write_client_factory_from_env,
            append_timeout_seconds=30,
            append_max_rows=100,
            append_request_max_bytes=1_000_000,
            append_request_overhead_bytes=1_000,
        )
    )

    rows = [
        {"event_id": f"it-{uuid4().hex}", "value": 1},
        {"event_id": f"it-{uuid4().hex}", "value": 2},
    ]

    try:
        stats = destination.write(rows)
        assert stats.ok is True
        assert stats.error is None
        assert stats.total_written_rows == len(rows)
        assert stats.total_failed_rows == 0
        assert stats.stream_mode == StreamMode.PENDING.value

        destination.mark_job_succeeded()
    finally:
        destination.close()

    assert destination.close_failed() is False
