# Retry Handler Tests

This folder groups tests for `adapters.bigquery.storage_write.retry_handler`.

It covers current behavior for:

- `ExtendedErrorPolicy`
- `BigQueryWriteRetryOrchestrator`
- `RetryOrchestratorStats` flattening
- `BigQueryStorageWriteError` derived properties

## Extended Error Policy

| Test function | What it verifies |
| --- | --- |
| `test_classify_exception_subclass_fallbacks_without_grpc_status_code` | Known `google.api_core.exceptions.*` subclasses with missing `grpc_status_code` are mapped via fallback to expected categories. |
| `test_classify_exception_retry_error_unwraps_cause` | `RetryError` is unwrapped and classification uses the root cause instead of wrapper. |
| `test_classify_exception_unknown_google_api_call_error_is_unknown_fatal` | Unknown `GoogleAPICallError` without usable gRPC status falls back to `UNKNOWN` fatal. |
| `test_classify_exception_timeout_is_retryable_deadline` | Timeout maps to `DEADLINE` with retry/reset semantics. |
| `test_grpc_status_normalization_equivalence_resource_exhausted` | `grpc.StatusCode.RESOURCE_EXHAUSTED` and `code_pb2.RESOURCE_EXHAUSTED` normalize to same policy/category. |
| `test_classify_append_rows_response_variants` | Response-path variants: success (`append_result`), `row_errors`, gRPC status error, and unknown response shape. |
| `test_classify_result_dispatch_smoke` | `classify_result` dispatches correctly for exception and response inputs. |

## Related Retry Handler Tests

| Test function | What it verifies |
| --- | --- |
| `test_orchestrator_returns_success_without_retry` | Orchestrator returns first-attempt success without retry/sleep. |
| `test_orchestrator_returns_immediately_on_non_retryable_error` | Orchestrator exits immediately for non-retryable terminal errors. |
| `test_flatten_success_case_has_zero_wire_rejected` | `RetryOrchestratorStats.flatten()` preserves success counters and zero rejected rows. |
| `test_flatten_failure_splits_serializer_vs_wire_failures` | `flatten()` correctly separates serializer failures from wire-level rejections. |
| `test_retryable_non_fatal_error_is_retried_and_has_no_row_errors` | `BigQueryStorageWriteError` properties for retryable non-fatal transport case. |
| `test_retryable_but_fatal_error_is_not_retried_and_surfaces_row_errors` | Fatal errors are not retried and row-error/DLQ flags are surfaced correctly. |

