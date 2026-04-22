from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class RowSerializationError:
    row_index: int
    reason_enum: str
    error_message: str
    raw_row: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class SerializationBatchResult:
    good_serialized_rows: list[bytes]
    bad_rows: list[RowSerializationError]
    good_row_indices: list[int] | None = None
