from dataclasses import dataclass
from typing import Protocol

# Storage Write API contract: one AppendRowsRequest must be smaller than 10 MB.
WRITE_API_HARD_MAX_REQUEST_BYTES = 10_000_000


class SupportsWriteLimits(Protocol):
    append_request_max_bytes: int
    append_request_overhead_bytes: int
    append_max_rows: int
    append_row_max_bytes: int | None


@dataclass(frozen=True)
class ResolvedWriteLimits:

    request_max_bytes: int
    request_overhead_bytes: int
    request_payload_budget_bytes: int
    max_rows: int
    row_max_bytes: int


def resolve_write_limits(config: SupportsWriteLimits) -> ResolvedWriteLimits:

    request_max_bytes = config.append_request_max_bytes
    request_overhead_bytes = config.append_request_overhead_bytes
    max_rows = config.append_max_rows
    explicit_row_max_bytes = config.append_row_max_bytes

    if request_max_bytes <= 0:
        raise ValueError(
            f"append_request_max_bytes must be > 0, got {request_max_bytes}"
        )
    if request_max_bytes >= WRITE_API_HARD_MAX_REQUEST_BYTES:
        raise ValueError(
            "append_request_max_bytes must be < "
            f"{WRITE_API_HARD_MAX_REQUEST_BYTES}, got {request_max_bytes}"
        )
    if request_overhead_bytes < 0:
        raise ValueError(
            f"append_request_overhead_bytes must be >= 0, got {request_overhead_bytes}"
        )
    if request_overhead_bytes >= request_max_bytes:
        raise ValueError(
            "append_request_overhead_bytes must be < append_request_max_bytes, "
            f"got overhead={request_overhead_bytes}, max={request_max_bytes}"
        )
    if max_rows < 1:
        raise ValueError(f"append_max_rows must be >= 1, got {max_rows}")

    request_payload_budget_bytes = request_max_bytes - request_overhead_bytes

    if explicit_row_max_bytes is None:
        # Defensive default: keep room for multiple rows per request and
        # surface likely upstream payload-quality bugs earlier.
        row_max_bytes = request_payload_budget_bytes // 4
    else:
        if explicit_row_max_bytes <= 0:
            raise ValueError(
                "append_row_max_bytes must be > 0 when provided, "
                f"got {explicit_row_max_bytes}"
            )
        if explicit_row_max_bytes > request_payload_budget_bytes:
            raise ValueError(
                "append_row_max_bytes must be <= (append_request_max_bytes - "
                "append_request_overhead_bytes), got "
                f"row_max={explicit_row_max_bytes}, "
                f"budget={request_payload_budget_bytes}"
            )
        row_max_bytes = explicit_row_max_bytes

    return ResolvedWriteLimits(
        request_max_bytes=request_max_bytes,
        request_overhead_bytes=request_overhead_bytes,
        request_payload_budget_bytes=request_payload_budget_bytes,
        max_rows=max_rows,
        row_max_bytes=row_max_bytes,
    )
