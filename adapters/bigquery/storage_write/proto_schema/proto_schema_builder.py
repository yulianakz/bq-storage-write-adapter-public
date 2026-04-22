from threading import Lock
from typing import Any, Sequence

from google.cloud.bigquery_storage_v1 import types
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from .proto_schema_utilities import ProtoBuildResult, _add_fields_rec, _schema_signature, _to_field_specs


class BQProtoSchemaBuilder:
    # Cache by (message_name, schema_signature).
    # Stored at the class level so all instances (or the singleton) share it.
    _CACHE: dict[tuple[str, str], ProtoBuildResult] = {}
    _INSTANCE: "BQProtoSchemaBuilder | None" = None
    _INSTANCE_LOCK = Lock()
    _CACHE_LOCK = Lock()

    @classmethod
    def instance(cls) -> "BQProtoSchemaBuilder":
        if cls._INSTANCE is None:
            with cls._INSTANCE_LOCK:
                if cls._INSTANCE is None:
                    cls._INSTANCE = cls()
        return cls._INSTANCE

    def build(
        self,
        *,
        schema_fields: Sequence[Any],
        message_name: str,
        package: str = "dynamic_bq",
    ) -> ProtoBuildResult:
        specs = _to_field_specs(schema_fields)
        signature = _schema_signature(schema_fields)
        cache_key = (message_name, signature)

        cached = self.__class__._CACHE.get(cache_key)
        if cached is not None:
            return cached

        # Thread-safe "build once per key".
        with self.__class__._CACHE_LOCK:
            cached = self.__class__._CACHE.get(cache_key)
            if cached is not None:
                return cached

            file_proto = descriptor_pb2.FileDescriptorProto()
            file_proto.name = f"{message_name.lower()}.proto"
            file_proto.package = package
            file_proto.syntax = "proto2"

            root_msg = file_proto.message_type.add()
            root_msg.name = message_name

            # Qualified name *without* the leading dot (we add it when setting `type_name`).
            type_qualifier = f"{package}.{message_name}"
            _add_fields_rec(
                msg_proto=root_msg,
                specs=specs,
                package=package,
                type_qualifier=type_qualifier,
            )

            pool = descriptor_pool.DescriptorPool()
            pool.Add(file_proto)
            root_descriptor = pool.FindMessageTypeByName(f"{package}.{message_name}")
            message_cls = message_factory.GetMessageClass(root_descriptor)

            descriptor_proto = descriptor_pb2.DescriptorProto()
            root_descriptor.CopyToProto(descriptor_proto)

            proto_schema = types.ProtoSchema(proto_descriptor=descriptor_proto)
            result = ProtoBuildResult(
                proto_schema=proto_schema,
                message_cls=message_cls,
                field_specs=specs,
            )
            self.__class__._CACHE[cache_key] = result
            return result

