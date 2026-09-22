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
window and is sealed when that window ends, so the boundary-driven case writes
one file set per window rather than one per request, and two writers of the
same lake agree on where a window starts. A block that reaches
`window.max_block_bytes` or `window.max_requests_per_block` is sealed before
its window ends, which writes more than one file set for that window. The waiting is done on the
engine's monotonic clock, so a wall clock that steps backwards cannot reopen a
window that was already written and boundaries missed while the node was busy
coalesce into one rotation.

The node registers the `exporter.series_parquet` metric set and reports it on
every `CollectTelemetry` message and once more at its terminal state, so the
last interval is not lost. State gauges (block bytes for the active, flushing
and parked request, live requests, the parking slot, queued completions and
their bytes, the oldest undecided request and the accounted-against-budget
memory) are sampled on collection and once more before the terminal handoff,
not on every loop turn: the sample walks both token vectors and the
notification queue, so sampling per turn would cost a block quadratic time in
its request count. Everything that must not be missed between two collections
is a counter recorded at the lifecycle transition itself. A series row
is counted as emitted only once `write_block` has returned success, so an
abandoned or failed block credits nothing. Every label comes from a closed
enumeration -- the rotation trigger, the refusal class, the dataset, the
re-emission cause and the unsupported point kind -- or from a physical column
name fixed by configuration at startup. Error strings, object store paths,
request ids, series ids and producer ids are never used as labels, so no
workload can grow this node's metric cardinality.

Logs and metrics share one admission path and one extraction call per request.
Traces have no dataset in the lake and are permanently refused on the signal
alone, before any conversion. A metrics request may carry points the lake has
no dataset for, namely exponential histograms and summaries; the `unsupported`
setting decides them for the whole request atomically. Under `reject` the
request is refused and nothing is written; under `drop` the unsupported points
are discarded, counted, and the supported rows are acknowledged once they are
durable. A request that extracts no rows at all is acknowledged without
opening a file. Metadata and exemplar attribute tables are neither read nor
validated, under either policy.

## Configuration

`storage` and `retry` follow the Parquet exporter's object-store settings.
`window` holds the rotation interval and the budgets of one block.
`ingress` holds the four per-request budgets; the two block-level budgets
belong under `window`, and naming one of them under `ingress` is refused
rather than ignored. `unsupported` is `reject` or `drop`, and `logs` and
`metrics` carry the per-signal series attributes, denormalized columns and
sort order. Byte-valued settings accept human units such as `64MiB`.
`parquet.compression` may only be `zstd`, which is what the sink writes.

## Layout

Files land under
`v=1/signal=<signal>/dataset=<dataset>/date=<date>/hour=<hour>/`. The
`series` dataset holds one descriptor row per identity; the `values` dataset
holds the records, joined back on `series_id`.
