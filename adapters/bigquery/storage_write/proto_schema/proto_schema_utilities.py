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
    "DATE": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "TIMESTAMP": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "DATETIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "TIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "JSON": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "GEOGRAPHY": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
}


def _schema_signature(schema_fields: Sequence[Any]) -> str:
    def sig_for_fields(fields: Sequence[Any]) -> Tuple[Any, ...]:
        out: list[Any] = []
        for f in fields:
            f_type = getattr(f, "field_type", None) or getattr(f, "type", None)
            f_mode = getattr(f, "mode", None) or "NULLABLE"
            nested = getattr(f, "fields", None) or []
            if (f_type or "").upper() == "RECORD":
                out.append((getattr(f, "name", None), f_type, f_mode, sig_for_fields(nested)))
            else:
                out.append((getattr(f, "name", None), f_type, f_mode))
        return tuple(out)

    return str(sig_for_fields(schema_fields))


def _to_field_specs(schema_fields: Sequence[Any]) -> Tuple[BQFieldSpec, ...]:
    specs: list[BQFieldSpec] = []
    for f in schema_fields:
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
                    record_fields=_to_field_specs(nested_fields),
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
            fd.type = _BQ_TYPE_TO_PROTO_SCALAR.get(
                spec.field_type, descriptor_pb2.FieldDescriptorProto.TYPE_STRING
            )

