# Series Parquet Exporter: Design

Date: 2026-09-21
Status: approved for planning
Scope: phases 1 and 2 of the series lake work (core crate and Dataflow
exporter). Introspection HTTP API and traces are separate specs.

## 1. Problem

Collect OTLP logs and metrics from many producers and land them in an object
store as Parquet, with these properties:

- Small, predictable memory: RSS is bounded by a configured block budget plus a
  configured cache budget plus a small constant.
- Deterministic flushing: every 15 seconds on aligned UTC boundaries, or
  earlier when the block byte budget is reached, or on shutdown.
- Two output streams: a slowly changing `series` dimension (resource, scope
  and selected attributes, keyed by a content hash) and narrow `values`
  streams (`logs`, `metrics`) that reference it by `series_id`.
- Series descriptors are written when first seen, when evicted and seen again,
  and at least once per hour partition in which values for them appear.
- Stateless: no WAL, no local disk. Producers wait for their OTLP request to
  be acknowledged; the ACK is sent only after the data is committed to the
  object store. Delivery is at-least-once.
- Values and series files are sorted by configurable physical columns.
- Layout is Hive-style and readable by Spark and DuckDB without extra tooling.
- Selected attributes can be denormalized into the values tables.

## 2. What the repository already provides

Verified on `main` at commit `5588c3e0d`:

- `crates/core-nodes/src/exporters/parquet_exporter`: object-store backends
  (file, S3, Azure), retry options, Arrow-to-Parquet plumbing, timer-driven
  flush, telemetry. It writes the OTAP star schema (one table per payload
  type joined by batch-local ids), has no byte budget, no time partitions, no
  cross-batch dedup, and drops the ack context. Its age-based flush is dead
  when `target_rows_per_file` is set (`writer.rs:298`, `if / else if`).
- OTLP receiver `wait_for_result: true` holds the gRPC/HTTP request open until
  the immediate downstream node acks or nacks. Exporters ack via
  `EffectHandler::notify_ack` / `notify_nack`.
- `policies.resources.core_allocation` bounds how many pipeline instances (and
  therefore how many buffers and caches) run per process.
- `temporal_reaggregation_processor/identity.rs`: an attribute hashing scheme
  (xxh3_128) that informs, but does not match, the canonical encoding below.
- ClickHouse exporter `transform_attributes.rs` and
  `condense_attributes_processor`: examples of grouping attribute rows by
  `parent_id` on Arrow columns.
- No LRU cache exists anywhere in the workspace.

The existing `parquet_exporter` is not modified. The `should_flush` bug is
fixed in a separate upstream PR and is out of scope here.

## 3. Architecture

Two new crates under `rust/otap-dataflow/crates/`, keeping the diff against
upstream limited to additive files plus one feature flag and one
component-inventory entry.

### 3.1 `series-lake` (engine-independent)

Depends on `otel-arrow-dfe-pdata`, `arrow`, `parquet`, `object_store`,
`xxhash-rust`. Modules:

- `canonical`: canonical encoding v1 and `SeriesId` (xxh3_128), golden
  vectors.
- `extract`: `OtapArrowRecords` to per-batch descriptors plus values with
  `series_id`.
- `cache`: LRU keyed by `SeriesId`, bounded by entries and bytes, epoch
  tracking.
- `block`: `Block` with byte accounting, sorted runs, pending series and ack
  contexts.
- `sink`: merge runs, sort series, write Parquet via `object_store`, paths,
  manifest.
- `config`: format configuration types (identity, denormalize, sort, layout).

### 3.2 `exporter:series_parquet`

A local exporter in `core-nodes` behind feature `series_parquet`, URN
`urn:otel:exporter:series_parquet`, metric set `exporter.series_parquet`.
It owns the timer, the ack contexts, the ACTIVE/FLUSHING pair and telemetry,
and delegates all data work to `series-lake`.

One pipeline instance equals one worker with its own cache and block pair.
Configured budgets are per instance. The README states that
`core_allocation` must be set explicitly; with `all_cores` the budget
multiplies by the core count.

## 4. Series identity (canonical encoding v1)

`series_id = XXH3_128(input)`, stored as 16 bytes big-endian (xxHash canonical
representation), rendered as 32 lowercase hex characters in APIs and logs.

`input` is the concatenation of fields, each field being
`u32_be(len(bytes)) ++ bytes`. All fields are UTF-8 strings. Order is fixed:

```text
"1"                                    # format version
signal                                 # "logs" | "metrics"
"resource", N, k1, v1, ..., kN, vN     # resource attributes
"scope", name, version, M, k1, v1, ... # scope attributes
"metric", name, unit, type, temporality, is_monotonic   # metrics only
"attrs", K, k1, v1, ..., kK, vK        # metrics: data point attributes
                                       # logs: allow-listed log attributes
```

Rules:

- Attribute lists are sorted by the raw UTF-8 bytes of the key (`memcmp`
  order, no locale, no case folding). The sort is stable.
- `N`, `M`, `K` are the attribute counts as decimal strings. They make the
  byte stream unambiguous.
- Value rendering: string as is; int as decimal; double as shortest
  round-trip representation (Rust `{}`, Python `repr`, Go `%v`); bool as
  `true` / `false`; bytes as lowercase hex; array and kvlist as compact JSON
  with sorted keys. A missing field is the empty string.
- `type` is one of `gauge`, `sum`, `histogram`, `exp_histogram`, `summary`.
  `temporality` is `delta`, `cumulative` or empty. `is_monotonic` is
  `true`, `false` or empty.
- Duplicate keys inside one attribute list: keep the first occurrence in
  batch order, drop the rest, count `identity.duplicate_keys`.
- Accepted simplifications: value types are not distinguished (`42` and
  `"42"` are the same series); a missing unit and an empty unit are the same.

Not part of the identity: timestamps, values, exemplars, body, severity,
trace and span ids, flags, dropped counts, log attributes outside the
allow-list.

The specification lives in `crates/series-lake/docs/canonical-v1.md` with
20 to 30 golden vectors in `crates/series-lake/tests/golden/*.json`
(descriptor, hex input, hex hash).

## 5. Storage format

### 5.1 Tables

Three tables. Common columns in all of them:

```text
series_id    FIXED_LEN_BYTE_ARRAY(16)
producer_id  STRING      # from the configured resource attribute, "" if absent
writer_id    STRING      # from config
ingest_time  TIMESTAMP(ms, UTC)   # when the batch was accepted
```

`series` (one row per emitted descriptor, both signals):

```text
signal              STRING          # "logs" | "metrics"
first_seen          TIMESTAMP(ms)
resource_schema_url STRING
resource_attrs      MAP<STRING, STRING>
scope_name          STRING
scope_version       STRING
scope_schema_url    STRING
scope_attrs         MAP<STRING, STRING>
attrs               MAP<STRING, STRING>   # metrics: data point attrs; logs: allow-list
metric_name         STRING  null
unit                STRING  null
metric_type         STRING  null
temporality         STRING  null
is_monotonic        BOOLEAN null
description         STRING  null
d_<key>             STRING  ...           # denormalized columns
```

`logs`:

```text
time_unix_nano           TIMESTAMP(ns)
observed_time_unix_nano  TIMESTAMP(ns)
severity_number          INT32
severity_text            STRING
body                     STRING          # non-string bodies rendered as JSON
event_name               STRING
trace_id                 FIXED_LEN_BYTE_ARRAY(16) null
span_id                  FIXED_LEN_BYTE_ARRAY(8)  null
flags                    UINT32
attrs                    MAP<STRING, STRING>      # attributes outside the allow-list
d_<key>                  STRING ...
```

`metrics` (number and histogram points in one table):

```text
time_unix_nano        TIMESTAMP(ns)
start_time_unix_nano  TIMESTAMP(ns) null
flags                 UINT32
value                 DOUBLE null     # number points; int values converted,
                                      # values above 2^53 lose precision
histogram             STRUCT<count UINT64, sum DOUBLE null, min DOUBLE null,
                             max DOUBLE null, bucket_counts LIST<UINT64>,
                             explicit_bounds LIST<DOUBLE>> null
d_<key>               STRING ...
```

The point kind is determined by which column is non-null. Exponential
histograms and summaries are added later as further nullable struct columns.
In v1 those points and all exemplars are dropped and counted by
`dropped_unsupported{kind}`; the request is still acked.

Attribute maps are `MAP<STRING, STRING>` with the same value rendering as the
canonical encoding.

### 5.2 Denormalization

`logs.denormalize` and `metrics.denormalize` list paths of the form
`resource.<key>`, `scope.<key>` or `attrs.<key>`. Each becomes a STRING column
`d_<key>` (dots replaced by underscores) in `series` and in the signal's values
table. Sorting and future filters are allowed only on common columns,
intrinsic columns and `d_*` columns.

### 5.3 Layout and naming

```text
<base>/v=1/table=series/date=2026-09-21/hour=12/
    part-20260921T121500Z-w=<writer_id>-f=<flush_id>.parquet
<base>/v=1/table=logs/date=.../hour=.../part-...
<base>/v=1/table=metrics/date=.../hour=.../part-...
<base>/v=1/_commits/date=2026-09-21/hour=12/<flush_id>.json
```

- `date` and `hour` are derived from the window start (ingest time), never
  from event timestamps, so late data lands in the partition currently being
  written.
- `flush_id` is xxh3_64 (hex) of `writer_id ++ window_start ++ process
  counter`.
- The manifest is written last and lists files, row counts per table, window
  start and end. Readers that need atomicity read only files named in
  manifests. Glob readers see uncommitted files too, which is acceptable under
  at-least-once.
- No files and no manifest are written for an empty window.

### 5.4 Parquet options

ZSTD compression, target row group about 64 MiB, statistics enabled,
dictionary encoding for strings. Key/value metadata: `format_version`,
`series_hash=xxh3_128/canonical_v1`, `writer_id`, `window_start`,
`window_end`, `flush_id`.

## 6. Buffering, memory and flush

### 6.1 Worker state

```text
cache:    LRU<SeriesId, { descriptor_blob, last_emitted_epoch }>
          bounded by max_entries and max_bytes
active:   Block { window_start, series_pending, runs: Vec<SortedRun>,
                  building: Vec<RecordBatch>, bytes, requests: Vec<AckCtx> }
flushing: Option<Block>
```

### 6.2 Ingest of one request

1. `extract` produces descriptors (one per unique series in the batch) and
   values with `series_id` assigned per row. Metrics with unsupported point
   kinds are dropped and counted.
2. For each series: if absent from the cache, or
   `last_emitted_epoch != floor(ingest_time, reemit_interval)`, append its
   descriptor to `series_pending` and set the epoch. Touch the entry; evict
   from the tail while over either limit.
3. Append values to `building`. When `building` reaches `run_target_bytes`
   (default 8 MiB), sort it by the configured keys (default
   `series_id, time_unix_nano`) with a permutation plus `take`, seal it as a
   run and drop the unsorted batch.
4. Byte accounting uses `get_array_memory_size` of retained batches plus
   pending descriptor bytes. If the request does not fit into a non-empty
   `active`, rotate first and place the whole request into the new block. A
   request larger than `max_block_bytes` receives a permanent nack.
5. Store the request's ack context in `active.requests`.

### 6.3 Rotation

Triggered by the aligned window boundary, by `bytes >= max_block_bytes`, or by
shutdown. If `flushing` is occupied, the exporter stops reading its inbox
until the flush completes; the bounded pdata channel fills and the receiver
rejects producers with a retry hint. Otherwise `building` is sealed,
`active` becomes `flushing`, and a new empty `active` is created. Its
`window_start` is the boundary just crossed for time-triggered rotation, or
the current window's start for byte-triggered rotation (several files may
then share a window start and differ by `flush_id`).

### 6.4 Flush

- `series_pending` is sorted once (default `series_id`) and written to
  `series`.
- Values: k-way merge over the runs, emitted in chunks of about
  `merge_chunk_bytes` (default 16 MiB) into `AsyncArrowWriter` on top of
  `object_store::BufWriter` (multipart upload, no full file in memory).
- Then the manifest.
- Success: ack every request in the block, release the block.
- Failure: retry the same immutable block with exponential backoff until
  `flush_retry_deadline` (default 5 min), then nack every request as
  retryable and release the block.

### 6.5 Memory invariant

```text
RSS ~= constant + active + flushing + cache + run_target_bytes
       + merge_chunk_bytes + parquet row group + one incoming batch
```

No external sort, no spill to disk, never a third block.

## 7. Dataflow integration

### 7.1 Node lifecycle

- `start()`: compute the delay to the next aligned boundary, schedule a
  one-shot wakeup, then `start_periodic_timer(window.interval)` so every
  `TimerTick` lands on a boundary.
- `PData`: section 6.2.
- `TimerTick`: rotation.
- `Shutdown`: rotation, then wait for the flush until the deadline; on timeout
  nack all remaining requests.
- `CollectTelemetry`: report metrics.

### 7.2 Acknowledgement

The exporter keeps each `OtapPdata` context in the block and calls
`notify_ack` or `notify_nack` for all of them after the flush outcome. With
`receiver:otlp` configured with `wait_for_result: true` and the exporter
directly downstream, the producer's request stays open until commit.

### 7.3 Backpressure

While `flushing` is busy and `active` is full, the exporter does not take the
next message from its inbox (it awaits the flush future). No new engine
mechanism is needed.

### 7.4 Configuration

```yaml
type: exporter:series_parquet
config:
  storage: { s3: { base_uri: s3://bucket/otel, region: eu-central-1 } }
  retry: { max_retries: 10, init_backoff: 200ms, max_backoff: 30s,
           backoff_base: 2.0, retry_timeout: 2min }
  writer_id: collector-17
  producer_id_attribute: host.id          # resource attribute
  window:
    interval: 15s
    max_block_bytes: 500MiB
    flush_retry_deadline: 5m
  series_cache:
    max_entries: 20000
    max_bytes: 100MiB
    reemit_interval: 1h
  sorting:
    run_target_bytes: 8MiB
    merge_chunk_bytes: 16MiB
  logs:
    series_attributes: [logger.name]      # allow-list
    denormalize: [resource.service.name]
    sort: [series_id, time_unix_nano]
  metrics:
    denormalize: [resource.service.name]
    sort: [series_id, time_unix_nano]
  parquet:
    compression: zstd
    row_group_bytes: 64MiB
```

`producer_id` from a transport header is a planned extension and is not part
of v1.

Example pipeline in `configs/series-parquet-s3.yaml`: `receiver:otlp`
(`wait_for_result: true`, `timeout: 60s`) connected directly to
`exporter:series_parquet`, with an explicit `core_allocation`.

### 7.5 Telemetry

Metric set `exporter.series_parquet`:

```text
series_cache.entries, series_cache.bytes, series_cache.hits,
series_cache.misses, series_cache.evictions
block.active_bytes, block.flushing_bytes, block.requests_pending
flush.count{reason=time|bytes|shutdown}, flush.duration, flush.failures
rows_written{table}
series_emitted{reason=new|epoch}
acks, nacks{reason=storage|too_large|invalid|shutdown}
oldest_unacked_seconds
dropped_unsupported{kind}
identity.duplicate_keys
```

## 8. Error handling

- Invalid OTAP batch (schema violation): permanent nack for that request,
  counter, continue.
- Request larger than `max_block_bytes`: permanent nack.
- Object store failure: retry the block until `flush_retry_deadline`, then a
  retryable nack for the whole block.
- Parquet encoding error (bug, not I/O): nack the block, release it, emit an
  error event. The exporter keeps running.
- Shutdown deadline exceeded: nack all outstanding requests.
- Unsupported point kind or exemplar: drop the rows, count, ack the request.

## 9. Testing

`series-lake`:

- Golden vectors for the canonical encoding and hash.
- `extract` on logs and metrics fixtures (modeled on the existing
  `parquet_exporter/fixtures.rs`).
- Cache: entry and byte limits, eviction order, epoch re-emission.
- Block: byte accounting, rotation triggers, whole-request-in-one-block,
  oversize rejection.
- Sorting: runs plus merge produce a globally sorted output; property test
  over random keys.
- Sink: write to `LocalFileSystem`, read back with `parquet`, assert schema,
  order, paths, manifest contents, empty-window behavior.

Exporter (engine test harness, as used by `parquet_exporter`):

- Ack arrives only after the manifest exists.
- Nack after storage failures using a failing `object_store` mock.
- Backpressure: inbox not drained while `flushing` is busy and `active` full.
- Shutdown with and without meeting the deadline.
- Window alignment with a controllable clock.

Compatibility: one integration test reads the output with DuckDB (skipped when
the binary is unavailable) and joins `logs` with `series` on `series_id`.

Benchmark: criterion on extract plus hash for a 10k-row batch.

Every test carries `Scenario` and `Guarantees` doc comments.

## 10. Out of scope for this spec

- Introspection HTTP API (`/state`, `/buffer/*`, `/series/{id}`, SSE tail).
- Traces.
- Exponential histograms, summaries, exemplars.
- Second-level file coalescing for low-volume deployments.
- Producer id from transport headers.
- Idempotent replay based on producer batch ids.
