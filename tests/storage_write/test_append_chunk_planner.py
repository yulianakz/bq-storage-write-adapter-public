import pytest

from adapters.bigquery.storage_write.bq_storage_write_utilities import (
    plan_append_chunks,
    sum_payload_bytes,
)


def test_sum_payload_bytes() -> None:
    assert sum_payload_bytes([b"a", b"bb", b"ccc"]) == 6


def test_plan_append_chunks_splits_by_payload_budget() -> None:
    chunks = plan_append_chunks(
        serialized_rows=[b"aaaa", b"bbbb", b"cc"],
        max_rows=100,
        max_payload_bytes=8,
    )
    assert [chunk.payload_bytes for chunk in chunks] == [8, 2]
    assert [chunk.rows for chunk in chunks] == [[b"aaaa", b"bbbb"], [b"cc"]]


def test_plan_append_chunks_splits_by_max_rows() -> None:
    chunks = plan_append_chunks(
        serialized_rows=[b"a", b"b", b"c"],
        max_rows=2,
        max_payload_bytes=10,
    )
    assert [chunk.rows for chunk in chunks] == [[b"a", b"b"], [b"c"]]


def test_plan_append_chunks_raises_when_single_row_exceeds_payload() -> None:
    with pytest.raises(ValueError, match="exceeds max payload budget"):
        plan_append_chunks(
            serialized_rows=[b"too-large-row"],
            max_rows=2,
            max_payload_bytes=3,
        )
