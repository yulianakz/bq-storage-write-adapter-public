import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from adapters.bigquery.storage_write.proto_schema.bq_proto_schema import BQFieldSpec
from adapters.bigquery.storage_write.row_serializer.serializer_models import (
    RowSerializationError,
    SerializationBatchResult,
)


class BQProtoRowSerializer:
    """
    Converts a list of dict rows into the byte-level representation required by
    BigQuery Storage Write API `ProtoRows.serialized_rows`.

    Incoming dict rows are assumed to already match the BigQuery table schema
    (keys = field names, values = Python-native types).
    """

    def __init__(self, *, message_cls: type, field_specs: Sequence[BQFieldSpec]) -> None:
        self._message_cls = message_cls
        self._field_specs = tuple(field_specs)

    def serialize_rows(self, rows: Sequence[Mapping[str, Any]], *, row_max_bytes: int | None
                       ) -> SerializationBatchResult:

        good_serialized_rows: list[bytes] = []
        good_row_indices: list[int] = []
        bad_rows: list[RowSerializationError] = []

        for row_index, row in enumerate(rows):

            try:

                serialized_row = self.serialize_row(row)
                row_size_bytes = len(serialized_row)

                if row_max_bytes is not None and row_size_bytes > row_max_bytes:
                    bad_rows.append(
                        RowSerializationError(
                            row_index=row_index,
                            reason_enum="serialization_row_too_large",
                            error_message=(
                                f"Serialized row size {row_size_bytes} exceeds row_max_bytes "
                                f"{row_max_bytes}"
                            ),
                            raw_row=row,
                        )
                    )
                    continue

                good_serialized_rows.append(serialized_row)
                good_row_indices.append(row_index)

            except Exception as exc:
                bad_rows.append(
                    RowSerializationError(
                        row_index=row_index,
                        reason_enum=self._map_reason_enum(exc),
                        error_message=str(exc),
                        raw_row=row,
                    )
                )

        return SerializationBatchResult(
            good_serialized_rows=good_serialized_rows,
            bad_rows=bad_rows,
            good_row_indices=good_row_indices,
        )

    def serialize_row(self, row: Mapping[str, Any]) -> bytes:
        msg = self._message_cls()
        self._set_fields(msg, self._field_specs, row)
        return msg.SerializeToString()

    @staticmethod
    def _map_reason_enum(exc: Exception) -> str:
        message = str(exc)
        if isinstance(exc, ValueError) and "Missing REQUIRED field" in message:
            return "serialization_missing_required"
        if "invalid JSON" in message:
            return "serialization_invalid_json"
        if isinstance(exc, TypeError):
            return "serialization_type_error"
        return "serialization_unexpected"

    def _set_fields(self, msg: Any, specs: Sequence[BQFieldSpec], data: Mapping[str, Any]) -> None:
        for spec in specs:
            mode = (spec.mode or "NULLABLE").upper()
            if spec.name not in data:
                # If the field is REQUIRED, missing it almost certainly means the row
                # is invalid for the target table schema.
                if mode == "REQUIRED":
                    raise ValueError(f"Missing REQUIRED field '{spec.name}' in row")
                continue

            value = data.get(spec.name)
            if value is None:
                # Upstream mapping is expected to omit None values (`exclude_none=True`),
                # but keep this as a safe "treat as unset" behavior.
                continue

            if mode == "REPEATED":
                self._set_repeated_field(msg, spec, value)
                continue

            if spec.is_record:
                if not isinstance(value, Mapping):
                    raise TypeError(
                        f"RECORD field '{spec.name}' must be a mapping/dict, got {type(value).__name__}"
                    )
                nested_msg = getattr(msg, spec.name)
                self._set_fields(nested_msg, spec.record_fields, value)
            else:
                encoded = self._encode_scalar(spec.field_type, value)
                setattr(msg, spec.name, encoded)

    def _set_repeated_field(self, msg: Any, spec: BQFieldSpec, value: Any) -> None:
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
            raise TypeError(
                f"REPEATED field '{spec.name}' must be a sequence/list, got {type(value).__name__}"
            )

        repeated_container = getattr(msg, spec.name)
        if spec.is_record:
            for item in value:
                if item is None:
                    raise TypeError(
                        f"REPEATED RECORD field '{spec.name}' cannot contain NULL values"
                    )
                if not isinstance(item, Mapping):
                    raise TypeError(
                        f"REPEATED RECORD field '{spec.name}' items must be mappings/dicts, "
                        f"got {type(item).__name__}"
                    )
                nested_msg = repeated_container.add()
                self._set_fields(nested_msg, spec.record_fields, item)
            return

        encoded_values: list[Any] = []
        for item in value:
            if item is None:
                raise TypeError(f"REPEATED field '{spec.name}' cannot contain NULL values")
            encoded = self._encode_scalar(spec.field_type, item)
            if encoded is None:
                raise TypeError(f"REPEATED field '{spec.name}' cannot contain NULL values")
            encoded_values.append(encoded)
        repeated_container.extend(encoded_values)

    def _encode_scalar(self, bq_field_type: str, value: Any) -> Any:
        t = bq_field_type.upper()
        if t in ("NUMERIC", "BIGNUMERIC"):
            if not isinstance(value, Decimal):
                raise TypeError(f"Expected Decimal for {t}, got {type(value).__name__}")
            return str(value)

        if t == "TIMESTAMP":
            if not isinstance(value, datetime):
                raise TypeError(f"Expected datetime for TIMESTAMP, got {type(value).__name__}")
            dt_utc = value.astimezone(timezone.utc)
            iso = dt_utc.isoformat()
            # Normalize +00:00 -> Z for RFC3339-ish compatibility.
            if iso.endswith("+00:00"):
                iso = iso[:-6] + "Z"
            return iso

        if t == "DATE":
            if isinstance(value, datetime):
                value = value.astimezone(timezone.utc).date()
            if not isinstance(value, date):
                raise TypeError(f"Expected date for DATE, got {type(value).__name__}")
            return value.strftime("%Y-%m-%d")

        if t == "JSON":
            if isinstance(value, (Mapping, Sequence)) and not isinstance(
                value, (str, bytes, bytearray)
            ):
                return json.dumps(value, default=str)
            if isinstance(value, str):
                try:
                    json.loads(value)
                except json.JSONDecodeError as exc:
                    raise TypeError(
                        f"Expected valid JSON string for JSON, got invalid JSON: {exc.msg}"
                    ) from exc
                return value
            raise TypeError(
                f"Expected JSON-compatible value for JSON, got {type(value).__name__}"
            )

        if t in ("STRING", "GEOGRAPHY"):
            return str(value)

        if t in ("INT64", "INTEGER"):
            if not isinstance(value, int) or isinstance(value, bool):
                raise TypeError(f"Expected int for {t}, got {type(value).__name__}")
            return value

        if t in ("BOOL", "BOOLEAN"):
            if not isinstance(value, bool):
                raise TypeError(f"Expected bool for {t}, got {type(value).__name__}")
            return value

        if t in ("FLOAT64", "FLOAT"):
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise TypeError(f"Expected float for {t}, got {type(value).__name__}")
            return float(value)

        if t == "BYTES":
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)
            # Best-effort: interpret as UTF-8.
            return str(value).encode("utf-8")

        # Default: pass-through.
        return value

