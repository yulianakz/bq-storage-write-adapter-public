"""Default configuration constants for the BigQuery Storage Write adapter.

These values drive the `StorageWriteConfig` defaults and, through
`resolve_write_limits`, the per-AppendRowsRequest payload budget used by the
chunk planner.

The BigQuery Storage Write API hard limit is 10 MB per AppendRowsRequest;
defaults here stay safely below that to leave room for request framing and the
proto writer schema that travels with the first request on a stream.

Ref: https://cloud.google.com/bigquery/docs/write-api#quotas
"""

# Target upper bound for a single AppendRowsRequest payload, in bytes.
# Kept under the 10 MB API hard cap with headroom for gRPC/proto framing.
DEFAULT_APPEND_REQUEST_MAX_BYTES: int = 9_500_000

# Bytes reserved inside the request budget for framing/metadata/writer schema.
DEFAULT_APPEND_REQUEST_OVERHEAD_BYTES: int = 256_000

# Max rows per AppendRowsRequest before the planner starts a new chunk.
DEFAULT_APPEND_MAX_ROWS: int = 500

# Optional explicit per-row payload cap in bytes.
# When None, the per-row cap is derived defensively as
# `(append_request_max_bytes - append_request_overhead_bytes) // 4`.
DEFAULT_APPEND_ROW_MAX_BYTES: int | None = None
