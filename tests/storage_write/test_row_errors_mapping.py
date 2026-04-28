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


def test_map_chunk_row_errors_to_original_overrun_window_partial_mapping() -> None:
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    # Overrun window: chunk expects serialized indices 0..5 while only 0..2 exist.
    # Only index 1 is a valid row error mapping; index 5 is invalid and must be skipped.
    row_errors = [SimpleNamespace(index=1), SimpleNamespace(index=5)]

    bad, good = map_chunk_row_errors_to_original(
        rows=rows,
        good_row_indices=[0, 1, 2],
        chunk_ser_start=0,
        chunk_len=6,
        row_errors=row_errors,
    )

    assert bad == [{"id": "b"}]
    # Valid non-error serialized indices in the window are 0 and 2.
    assert good == [{"id": "a"}, {"id": "c"}]


def test_map_chunk_row_errors_to_original_mixed_valid_and_invalid_bad_indices() -> None:
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    # Absolute bad serialized indices become {1, 5}; 1 is valid, 5 is invalid.
    row_errors = [SimpleNamespace(index=1), SimpleNamespace(index=5)]

    bad, good = map_chunk_row_errors_to_original(
        rows=rows,
        good_row_indices=[0, 1, 2],
        chunk_ser_start=0,
        chunk_len=6,
        row_errors=row_errors,
    )

    assert bad == [{"id": "b"}]
    assert good == [{"id": "a"}, {"id": "c"}]


def test_map_chunk_row_errors_to_original_returns_none_none_when_all_bad_mappings_invalid() -> None:
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    # Row error local index is in-window, but absolute serialized index 5 is out of
    # good_row_indices bounds, so no valid bad mapping can be derived.
    row_errors = [SimpleNamespace(index=0)]

    bad, good = map_chunk_row_errors_to_original(
        rows=rows,
        good_row_indices=[0, 1, 2],
        chunk_ser_start=5,
        chunk_len=1,
        row_errors=row_errors,
    )

    assert bad is None
    assert good is None


def test_map_chunk_row_errors_to_original_skips_invalid_original_index() -> None:
    rows = [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}]
    # Serialized index 1 resolves to original index 999 (invalid), while index 2 is
    # valid; mapper should skip the invalid one and keep the valid mapping.
    row_errors = [SimpleNamespace(index=1), SimpleNamespace(index=2)]

    bad, good = map_chunk_row_errors_to_original(
        rows=rows,
        good_row_indices=[0, 999, 2, 3],
        chunk_ser_start=0,
        chunk_len=4,
        row_errors=row_errors,
    )

    assert bad == [{"id": "c"}]
    # Non-error valid mappings remain: j=0 -> a, j=3 -> d.
    assert good == [{"id": "a"}, {"id": "d"}]
