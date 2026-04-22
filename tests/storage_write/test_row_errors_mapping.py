"""Smoke tests for row_errors_mapping helpers.

Covers the happy path (one chunk-local index maps back to its original input
row, non-error rows fall into the "good" side) and the degenerate path (all
indices unmappable yields (None, None)).
"""

from types import SimpleNamespace

from adapters.bigquery.storage_write.row_errors_mapping import (
    map_chunk_row_errors_to_original,
    parse_row_error_local_index,
)


def test_parse_row_error_local_index_reads_index_attribute() -> None:
    assert parse_row_error_local_index(SimpleNamespace(index=2)) == 2


def test_parse_row_error_local_index_returns_none_when_missing() -> None:
    assert parse_row_error_local_index(SimpleNamespace()) is None


def test_map_chunk_row_errors_to_original_happy_path() -> None:
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    # Serializer kept all three rows; single chunk covering all serialized rows.
    good_row_indices = [0, 1, 2]
    row_errors = [SimpleNamespace(index=1)]

    bad, good = map_chunk_row_errors_to_original(
        rows=rows,
        good_row_indices=good_row_indices,
        chunk_ser_start=0,
        chunk_len=3,
        row_errors=row_errors,
    )

    assert bad == [{"id": "b"}]
    assert good == [{"id": "a"}, {"id": "c"}]


def test_map_chunk_row_errors_to_original_returns_none_when_no_valid_indices() -> None:
    rows = [{"id": "a"}, {"id": "b"}]
    # Index 99 is outside the chunk window and must be discarded.
    row_errors = [SimpleNamespace(index=99)]

    bad, good = map_chunk_row_errors_to_original(
        rows=rows,
        good_row_indices=[0, 1],
        chunk_ser_start=0,
        chunk_len=2,
        row_errors=row_errors,
    )

    assert bad is None
    assert good is None
