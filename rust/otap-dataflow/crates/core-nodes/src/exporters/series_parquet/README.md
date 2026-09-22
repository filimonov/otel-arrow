# Series Parquet exporter

Build with `--features series_parquet`; add `aws` for S3. The local example
is `configs/series-parquet-local.yaml`. Connect an OTLP receiver directly
with `wait_for_result: true`. An OK response means the request is durable;
retry timeouts and transient failures, allowing duplicates.

Create `/tmp/series-parquet` before starting. Use the existing admin shutdown
endpoint with `timeout_secs=180`; signal shutdown currently grants only 60s.

## Status

The node owns one ACTIVE block and at most one FLUSHING block. Requests are
admitted to the ACTIVE block and acknowledged only once the whole block has
been written and its descriptors marked committed; a failed write nacks every
request of the block as retryable. Admission closes once the ACTIVE block is
waiting to be rotated, so no third block is ever needed and a slow destination
becomes backpressure. A request the ACTIVE block cannot reserve room for is
not refused: its extracted rows are parked, input closes until the next block
opens, and the parked request enters that block before anything newer. Only
one request is ever parked, so the memory a worker holds is the two blocks
plus one request. Preparation runs entirely before the block is reserved
against, so a refused request leaves the block unchanged, and the request's
payload and conversion batches are released as soon as its rows are
extracted. An OTLP body's top-level protobuf framing is validated before it is
converted, because the shared byte views decode lazily: without that check a
truncated request would be acknowledged as stored. Corruption inside a nested
message is not validated and still surfaces as missing fields. Rotation
follows aligned wall-clock windows: a block covers one `window.interval`
window and is sealed when that window ends, so the node writes one file set
per window rather than one per request, and two writers of the same lake agree
on where a window starts. A block that reaches `window.max_block_bytes` or
`window.max_requests_per_block` is sealed early. The waiting is done on the
engine's monotonic clock, so a wall clock that steps backwards cannot reopen a
window that was already written and boundaries missed while the node was busy
coalesce into one rotation. Later tasks add telemetry and a drain-aware
shutdown. Only logs are accepted; metrics and traces are permanently refused.

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
