"""Helpers for mapping BigQuery Storage Write row_errors back to original input rows.

The Storage Write API reports row_errors with indices that refer to the serialized
chunk of rows actually sent on the wire. These helpers translate those chunk-local
indices back to the caller's original input dicts, taking into account:

- Rows dropped by the serializer (skipped before send).
- Multi-chunk batches where one AppendRowsRequest's row_errors indices are local
  to that chunk.
"""

from typing import Any, Sequence


def parse_row_error_local_index(row_err: Any) -> int | None:
    """Extract the chunk-local row index from a BQ RowError-like object.

    Accepts both `index` and the alternate `row_index` attribute names so the
    adapter stays compatible with proto shape variations. Returns None when the
    attribute is missing or not coercible to int.
    """

    raw = getattr(row_err, "index", None)
    if raw is None:
        raw = getattr(row_err, "row_index", None)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def map_chunk_row_errors_to_original(
    *,
    rows: Sequence[dict[str, Any]],
    good_row_indices: Sequence[int],
    chunk_ser_start: int,
    chunk_len: int,
    row_errors: Sequence[Any],
) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]] | None]:

    """Translate one chunk's row_errors into (bad_rows, good_rows) of the original input.

    Both lists are returned in original-input order (ascending index). Returns
    (None, None) when no row error index could be mapped to a valid chunk row.

    - `rows` is the full original input sequence.
    - `good_row_indices` maps serialized-good positions back to original indices.
    - `chunk_ser_start` / `chunk_len` identify the serialized-row window for the chunk.
    - `row_errors` is the chunk's RowError objects (chunk-local indices).
    """

    bad_ser_abs: set[int] = {
        chunk_ser_start + local
        for local in (parse_row_error_local_index(e) for e in row_errors)
        if local is not None and 0 <= local < chunk_len
    }

    if not bad_ser_abs:
        return None, None

    bad_orig_sorted = sorted({good_row_indices[j] for j in bad_ser_abs})
    row_err_bad = [rows[i] for i in bad_orig_sorted]
    row_err_good = [
        rows[good_row_indices[j]]
        for j in range(chunk_ser_start, chunk_ser_start + chunk_len)
        if j not in bad_ser_abs
    ]
    return row_err_bad, row_err_good
