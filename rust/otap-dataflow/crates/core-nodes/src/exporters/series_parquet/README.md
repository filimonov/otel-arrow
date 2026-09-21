# Series Parquet exporter

Build with `--features series_parquet`; add `aws` for S3. The local example
is `configs/series-parquet-local.yaml`. Connect an OTLP receiver directly
with `wait_for_result: true`. An OK response means the request is durable;
retry timeouts and transient failures, allowing duplicates.

Create `/tmp/series-parquet` before starting. Use the existing admin shutdown
endpoint with `timeout_secs=180`; signal shutdown currently grants only 60s.

## Status

This is the first runnable slice. It writes one file set per request: each
request is extracted, admitted to a fresh block, sealed and flushed before it
is acknowledged. Later tasks batch requests into windowed blocks, add
backpressure, telemetry and a drain-aware shutdown. Only logs are accepted;
metrics and traces are permanently refused.

## Configuration

`storage` and `retry` follow the Parquet exporter's object-store settings.
`window` holds the rotation interval and the budgets of one block.
`ingress` holds the four per-request budgets; the two block-level budgets
belong under `window`, and naming one of them under `ingress` is refused
rather than ignored. Byte-valued settings accept human units such as `64MiB`.
`parquet.compression` may only be `zstd`, which is what the sink writes.

## Layout

Files land under
`v=1/signal=<signal>/dataset=<dataset>/date=<date>/hour=<hour>/`. The
`series` dataset holds one descriptor row per identity; the `values` dataset
holds the records, joined back on `series_id`.
