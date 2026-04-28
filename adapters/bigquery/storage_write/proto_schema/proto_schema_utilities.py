from dataclasses import dataclass
from typing import Any, Dict, Sequence, Tuple

from google.cloud.bigquery_storage_v1 import types
from google.protobuf import descriptor_pb2


@dataclass(frozen=True)
class BQFieldSpec:
    name: str
    mode: str  # REQUIRED, NULLABLE, REPEATED
    field_type: str  # STRING, RECORD, INT64, ...
    record_fields: Tuple["BQFieldSpec", ...] = ()

    @property
    def is_record(self) -> bool:
        return self.field_type.upper() == "RECORD"


@dataclass(frozen=True)
class ProtoBuildResult:
    proto_schema: types.ProtoSchema
    message_cls: type
    field_specs: Tuple[BQFieldSpec, ...]


# Mapping between BigQuery types and Protobuf types.
_BQ_TYPE_TO_PROTO_SCALAR: Dict[str, int] = {
    "STRING": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "BYTES": descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
    "INT64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "INTEGER": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "BOOL": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    "BOOLEAN": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    "FLOAT64": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
    "FLOAT": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
    # BigQuery Storage Write expects ProtoRows.serialized_rows to match the protobuf
    # descriptor. These BQ "logical types" are mapped to STRING fields in the dynamic
    # proto, and the serializer is responsible for formatting values accordingly.
    "NUMERIC": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "BIGNUMERIC": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "DECIMAL": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "BIGDECIMAL": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "DATE": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "TIMESTAMP": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "DATETIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "TIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "JSON": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "GEOGRAPHY": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "INTERVAL": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "RANGE": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
}

_VALID_BQ_MODES = {"NULLABLE", "REQUIRED", "REPEATED"}


def _normalized_field_name(name: Any) -> str:
    return (str(name) if name is not None else "").casefold()


def _field_sort_key(field: Any) -> tuple[str, str]:
    # Important:
    # We sort case-insensitively (BigQuery semantics) with a deterministic
    # tie-break on the original name to keep ordering stable across runs.
    raw_name = getattr(field, "name", None)
    name = str(raw_name) if raw_name is not None else ""
    return (_normalized_field_name(name), name)


def _sorted_fields_with_validation(fields: Sequence[Any], *, context: str) -> Tuple[Any, ...]:
    sorted_fields = tuple(sorted(fields, key=_field_sort_key))
    seen: dict[str, str] = {}
    for field in sorted_fields:
        raw_name = getattr(field, "name", None)
        name = str(raw_name) if raw_name is not None else ""
        normalized = _normalized_field_name(name)
        existing = seen.get(normalized)
        if existing is not None:
            raise ValueError(
                f"Duplicate BigQuery field names after casefold normalization in {context}: "
                f"'{existing}' vs '{name}' (case-insensitive collision)."
            )
        seen[normalized] = name
    return sorted_fields


def _schema_signature(schema_fields: Sequence[Any]) -> str:
    def sig_for_fields(fields: Sequence[Any], *, context: str) -> Tuple[Any, ...]:
        out: list[Any] = []
        for f in _sorted_fields_with_validation(fields, context=context):
            name = getattr(f, "name", None)
            f_type = getattr(f, "field_type", None) or getattr(f, "type", None)
            f_mode = getattr(f, "mode", None) or "NULLABLE"
            nested = getattr(f, "fields", None) or []
            if (f_type or "").upper() == "RECORD":
                out.append(
                    (
                        name,
                        f_type,
                        f_mode,
                        sig_for_fields(
                            nested,
                            context=f"{context}.{name}" if name is not None else f"{context}.<unnamed>",
                        ),
                    )
                )
            else:
                out.append((name, f_type, f_mode))
        return tuple(out)

    return str(sig_for_fields(schema_fields, context="root"))


def _validate_schema_fields(schema_fields: Sequence[Any], *, context: str = "root") -> None:
    for field in schema_fields:
        raw_name = getattr(field, "name", None)
        name = str(raw_name) if raw_name is not None else ""
        if not name:
            raise ValueError(f"Field name cannot be empty in {context}.")

        mode = (getattr(field, "mode", None) or "NULLABLE").upper()
        if mode not in _VALID_BQ_MODES:
            raise ValueError(
                f"Unsupported BigQuery mode '{mode}' for field '{name}' in {context}. "
                f"Valid modes: {sorted(_VALID_BQ_MODES)}."
            )

        f_type = (getattr(field, "field_type", None) or getattr(field, "type", None) or "").upper()
        nested_fields = getattr(field, "fields", None) or []
        field_context = f"{context}.{name}"
        if f_type == "RECORD":
            if not nested_fields:
                raise ValueError(
                    f"RECORD field '{name}' in {context} must define nested fields."
                )
            _validate_schema_fields(nested_fields, context=field_context)
        elif nested_fields:
            raise ValueError(
                f"Non-RECORD field '{name}' in {context} must not define nested fields."
            )


def _to_field_specs(schema_fields: Sequence[Any], *, context: str = "root") -> Tuple[BQFieldSpec, ...]:
    specs: list[BQFieldSpec] = []
    for f in _sorted_fields_with_validation(schema_fields, context=context):
        name = getattr(f, "name")
        mode = (getattr(f, "mode", None) or "NULLABLE").upper()
        f_type = getattr(f, "field_type", None) or getattr(f, "type", None) or ""
        f_type = f_type.upper()
        if f_type == "RECORD":
            nested_fields = getattr(f, "fields", None) or []
            specs.append(
                BQFieldSpec(
                    name=name,
                    mode=mode,
                    field_type="RECORD",
                    record_fields=_to_field_specs(nested_fields, context=f"{context}.{name}"),
                )
            )
        else:
            specs.append(BQFieldSpec(name=name, mode=mode, field_type=f_type))
    return tuple(specs)


def _bq_mode_to_proto_label(mode: str) -> int:
    mode = (mode or "NULLABLE").upper()
    if mode == "REQUIRED":
        return descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED
    if mode == "REPEATED":
        return descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    return descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL


def _resolve_scalar_proto_type(*, field_name: str, field_type: str) -> int:
    scalar_type = _BQ_TYPE_TO_PROTO_SCALAR.get(field_type)
    if scalar_type is not None:
        return scalar_type
    raise ValueError(
        f"Unsupported BigQuery field type '{field_type}' for field '{field_name}'. "
        "Add an explicit mapping in _BQ_TYPE_TO_PROTO_SCALAR."
    )


def _add_fields_rec(
    *,
    msg_proto: descriptor_pb2.DescriptorProto,
    specs: Sequence[BQFieldSpec],
    package: str,
    type_qualifier: str,
) -> None:
    tag = 1
    for spec in specs:
        fd = msg_proto.field.add()
        fd.name = spec.name
        fd.number = tag
        tag += 1
        fd.label = _bq_mode_to_proto_label(spec.mode)

        if spec.is_record:
            nested = msg_proto.nested_type.add()
            nested.name = f"{spec.name}_msg"
            nested_qualifier = f"{type_qualifier}.{nested.name}"
            _add_fields_rec(
                msg_proto=nested,
                specs=spec.record_fields,
                package=package,
                type_qualifier=nested_qualifier,
            )
            fd.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
            fd.type_name = f".{nested_qualifier}"
        else:
            fd.type = _resolve_scalar_proto_type(
                field_name=spec.name,
                field_type=spec.field_type,
            )

