"""
Compatibility shim for dynamic protobuf schema building.

Historically, this module contained all implementation details.
The code is now split into:
- `proto_schema_utilities.py` (helpers + dataclasses)
- `proto_schema_builder.py` (singleton builder + cache)
"""

from .proto_schema_builder import BQProtoSchemaBuilder
from .proto_schema_utilities import BQFieldSpec, ProtoBuildResult

__all__ = ["BQProtoSchemaBuilder", "BQFieldSpec", "ProtoBuildResult"]

