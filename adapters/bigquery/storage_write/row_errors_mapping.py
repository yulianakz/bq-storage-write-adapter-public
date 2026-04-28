"""Helpers for mapping BigQuery Storage Write row_errors back to original input rows.

The Storage Write API reports row_errors with indices that refer to the serialized
chunk of rows actually sent on the wire. These helpers translate those chunk-local
indices back to the caller's original input dicts, taking into account:

- Rows dropped by the serializer (skipped before send).
- Multi-chunk batches where one AppendRowsRequest's row_errors indices are local
  to that chunk.
"""

import logging
from typing import Any, Sequence

logger = logging.getLogger(__name__)


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

    Contract:
    - Returned rows preserve original-input order (ascending original index).
    - bad_rows are deduplicated by original row index.
    - good_rows are not deduplicated; each valid mapping in the chunk window is
      included once, except indices flagged by row_errors.
    - Invalid mappings are skipped (no exception is raised).
    - Returns (None, None) when no row error index could be mapped to a valid
      original row.

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

    invalid_j_count = 0
    invalid_orig_count = 0

    def _safe_map_serialized_to_original(serialized_index: int) -> int | None:
        nonlocal invalid_j_count, invalid_orig_count
        if not (0 <= serialized_index < len(good_row_indices)):
            invalid_j_count += 1
            return None
        original_index = good_row_indices[serialized_index]
        if not (0 <= original_index < len(rows)):
            invalid_orig_count += 1
            return None
        return original_index

    valid_bad_orig: set[int] = set()
    for j in bad_ser_abs:
        original_index = _safe_map_serialized_to_original(j)
        if original_index is not None:
            valid_bad_orig.add(original_index)
    if not valid_bad_orig:
        return None, None

    bad_orig_sorted = sorted(valid_bad_orig)
    row_err_bad = [rows[i] for i in bad_orig_sorted]
    row_err_good: list[dict[str, Any]] = []
    for j in range(chunk_ser_start, chunk_ser_start + chunk_len):
        if j in bad_ser_abs:
            continue
        original_index = _safe_map_serialized_to_original(j)
        if original_index is None:
            continue
        row_err_good.append(rows[original_index])

    if invalid_j_count or invalid_orig_count:
        logger.warning(
            "Storage Write row error mapping skipped invalid indices",
            extra={
                "chunk_ser_start": chunk_ser_start,
                "chunk_len": chunk_len,
                "good_row_indices_len": len(good_row_indices),
                "row_errors_count": len(row_errors),
                "invalid_j_count": invalid_j_count,
                "invalid_orig_count": invalid_orig_count,
                "mapped_bad_count": len(row_err_bad),
                "mapped_good_count": len(row_err_good),
            },
        )
    return row_err_bad, row_err_good
