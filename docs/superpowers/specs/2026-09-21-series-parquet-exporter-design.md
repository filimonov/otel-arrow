# Series Parquet Exporter: Design

Date: 2026-09-21
Status: approved for planning (revision 2, after two external reviews)
Scope: phases 1 and 2 of the series lake work (core crate and Dataflow
exporter). Introspection HTTP API and traces are separate specs; section 10
records what was deferred and why.

## 1. Problem

Collect OTLP logs and metrics from many producers and land them in an object
store as Parquet, with these properties:

- Small, predictable memory: memory owned by the exporter is bounded by two
  block budgets, one cache budget, one request budget and a fixed workspace.
- Deterministic flushing: on aligned UTC window boundaries (default 15 s),
  earlier when the block byte budget is reached, and on shutdown.
- Two logical streams: a slowly changing `series` dimension (resource, scope
  and selected attributes, keyed by a content hash) and narrow `values`
  streams that reference it by `series_id`.
- Every values partition that contains rows for a series also contains at
  least one descriptor row for that series, written by the same worker.
- Stateless: no WAL, no local disk. Producers wait for their OTLP request to
  be acknowledged; the ACK is sent only after the data is committed to the
  object store. Delivery is at-least-once.
- Values and series files are sorted by configurable physical columns.
- Layout is Hive-style and readable by Spark and DuckDB without extra tooling.
- Selected attributes can be denormalized into the values tables as typed
  columns.

## 2. What the repository already provides

Verified on `main` at commit `5588c3e0d`:

- `crates/core-nodes/src/exporters/parquet_exporter`: object-store backends
  (file, S3, Azure), retry options, Arrow-to-Parquet plumbing, telemetry.
  It writes the OTAP star schema (batch-local ids), has no byte budget, no
  time partitions, no cross-batch dedup, drops the ack context, and its
  age-based flush is dead when `target_rows_per_file` is set
  (`writer.rs:298`). It shows the mandatory input steps: convert the payload
  to `OtapArrowRecords` with `try_into_with_default` and call
  `decode_transport_optimized_ids` before touching `parent_id` columns
  (`mod.rs:335-349`).
- OTLP receiver `wait_for_result: true` holds the gRPC/HTTP request open until
  the immediate downstream node acks or nacks (`otap_grpc/otlp/server_new.rs:521`).
  The receiver awaits the channel send first, so a full pdata channel blocks
  the request until the receiver timeout; exhausted ack slots return
  `RESOURCE_EXHAUSTED`. Exporters ack via `EffectHandler::notify_ack` /
  `notify_nack`; `AckMsg` needs an `OtapPdata`, and the batch processor shows
  how to ack from a stored `Context` with `OtapPayload::empty(signal)`
  (`batch_processor/mod.rs:1611`).
- `ExporterInbox::recv_when(accept_pdata)` (`engine/src/message.rs:874`) lets
  an exporter keep receiving control messages while refusing pdata. During
  shutdown draining, buffered pdata is force-drained regardless of the flag.
- Shutdown order (`engine/src/pipeline_ctrl.rs:541-615`): all engine timers
  are cancelled, receivers get `DrainIngress`, receivers wait for outstanding
  `wait_for_result` slots until the deadline, and only then processors and
  exporters receive `Shutdown`. Engine timers fire at `StartTimer` time plus
  duration and carry no boundary timestamp (`pipeline_ctrl.rs:95,141`).
- `pdata/src/otap/memory.rs` provides `record_batch_pinned_bytes`
  (deduplicated retained-buffer accounting) and `record_batch_logical_bytes`.
- `policies.resources.core_allocation` selects cores per pipeline; the number
  of exporter instances per process is the number of pipeline instances that
  contain the node.
- `temporal_reaggregation_processor/identity.rs`: an attribute hashing scheme
  that informs, but does not match, the canonical encoding below.
- No LRU cache exists anywhere in the workspace.

The existing `parquet_exporter` is not modified. The `should_flush` bug is
fixed in a separate upstream PR and is out of scope here.

## 3. Architecture

Two new crates under `rust/otap-dataflow/crates/`, keeping the diff against
upstream additive: new files, one feature flag, one component-inventory entry.

### 3.1 `series-lake` (engine-independent)

Depends on `otel-arrow-dfe-pdata` (schemas, `OtapArrowRecords`, memory
helpers), `arrow`, `parquet`, `object_store`, `xxhash-rust`. It must not
depend on the engine, `OtapPdata` or `Context`. Modules:

- `canonical`: canonical encoding v1 and `SeriesId` (xxh3_128), golden
  vectors.
- `extract`: `OtapArrowRecords` to per-batch descriptors plus values with
  `series_id` and typed denormalized columns. Validates identity attributes.
- `cache`: LRU keyed by `SeriesId`, bounded by entries and bytes, tracks the
  last committed partition per series.
- `buffer`: one `SortedTableBuffer` (building batch plus sealed sorted runs)
  used for every output table, and `Block` (byte accounting, pending series
  set, request tokens).
- `sink`: k-way merge of runs into Parquet files via `object_store`, paths,
  file naming, per-table write order.
- `config`: format configuration types (identity, denormalize, sort, layout,
  budgets).

### 3.2 `exporter:series_parquet`

A local exporter in `core-nodes` behind feature `series_parquet`, URN
`urn:otel:exporter:series_parquet`, metric set `exporter.series_parquet`.
It owns the window clock, the ack contexts, the ACTIVE/FLUSHING pair, the
state machine of section 6 and telemetry, and delegates all data work to
`series-lake`.

One pipeline instance equals one worker with its own cache and block pair.
Configured budgets are per worker. The README states that
`core_allocation` must be set explicitly and gives the aggregate formula of
section 6.6.

## 4. Series identity (canonical encoding v1)

`series_id = XXH3_128(input)` with seed 0, stored as 16 bytes in the xxHash
canonical big-endian representation, rendered as 32 lowercase hex characters
in APIs and logs.

`input` is a byte string built from typed values. Every value is
`tag:u8 ++ len:u32_be ++ payload`:

```text
tag  type     payload
0x01 string   UTF-8 bytes as received (no normalization)
0x02 bytes    raw bytes
0x03 int64    8 bytes, two's complement, big-endian
0x04 double   8 bytes, IEEE 754 binary64, big-endian (NaN and -0.0 as given)
0x05 bool     1 byte, 0x00 or 0x01
0x06 null     empty payload (unset or absent value)
0x07 array    count:u32_be ++ values*         (elements keep their order)
0x08 kvlist   count:u32_be ++ (string key, value)*  sorted by key bytes
```

Attribute lists are encoded as `kvlist`. Identity fields are appended in this
fixed order:

```text
string "OTEL-SERIES/1"                     # namespace and format version
string signal                              # "logs" | "metrics"
kvlist resource attributes
string resource schema_url
string scope name
string scope version
string scope schema_url
kvlist scope attributes
# metrics only:
string metric name
string metric unit
string metric type            # gauge | sum | histogram | exp_histogram | summary
string temporality            # delta | cumulative | "" (gauge)
bool   is_monotonic           # false for non-sum metrics
kvlist identity attributes    # metrics: data point attributes
                              # logs: attributes named in logs.series_attributes
```

Rules:

- Keys are sorted by raw UTF-8 bytes (`memcmp`, no locale, no case folding),
  recursively inside nested kvlists.
- A duplicate key inside one attribute list makes the request invalid: the
  request is nacked as permanent and counted (`nacks{reason=invalid}`). No
  identity is invented for malformed data.
- Missing string fields are encoded as empty strings; unset attribute values
  are encoded as `null`. `42` (int64) and `"42"` (string) are different
  series.
- Nothing else is part of the identity: timestamps, values, exemplars, body,
  severity, trace and span ids, flags, dropped counts, metric description,
  log attributes outside the allow-list.

Every semantic field stored in a `series` row is either part of the identity
or explicitly listed as non-identity metadata. Non-identity metadata:
`description` (metrics), `emitted_at`, `producer_id`. Consumers must not
expect them to be constant per `series_id`.

The specification lives in `crates/series-lake/docs/canonical-v1.md` with
golden vectors in `crates/series-lake/tests/golden/*.json` (input descriptor,
hex canonical bytes, hex hash). Vectors must cover: empty string, missing
field, int 0 and -0.0, INT64_MIN and INT64_MAX, `42` vs `"42"`, NaN, +Inf,
-Inf, non-ASCII UTF-8 including supplementary planes, embedded NUL, bytes,
nested array, nested kvlist, key ordering with common prefixes. At least one
vector set is generated independently in Python from the spec text.

## 5. Storage format

### 5.1 Datasets

Five datasets, one Parquet schema each:

```text
signal=logs/table=series
signal=logs/table=values
signal=metrics/table=series
signal=metrics/table=number
signal=metrics/table=histogram
```

Later additions (`table=exp_histogram`, `table=summary`, `signal=traces/...`)
add datasets without changing existing schemas.

Common columns in all datasets:

```text
series_id    FIXED_LEN_BYTE_ARRAY(16)   required
producer_id  STRING                     required, "" when absent
```

`producer_id` is the value of the configured resource attribute
(`producer_id_attribute`). Header-based producer ids are a future extension.

`series` (both signals):

```text
emitted_at          TIMESTAMP(us, UTC)   # when this descriptor row was produced
resource_schema_url STRING
resource_attrs      MAP<STRING, STRING>
scope_name          STRING
scope_version       STRING
scope_schema_url    STRING
scope_attrs         MAP<STRING, STRING>
attrs               MAP<STRING, STRING>  # metrics: data point attrs; logs: allow-list
# metrics series only:
metric_name         STRING
unit                STRING
metric_type         STRING
temporality         STRING
is_monotonic        BOOLEAN
description         STRING               # non-identity metadata
# denormalized identity columns (section 5.2)
```

`logs/values`:

```text
time                     TIMESTAMP(us, UTC)   # from time_unix_nano, 0 -> null
time_unix_nano           INT64                # raw, 0 when unset
observed_time            TIMESTAMP(us, UTC)
observed_time_unix_nano  INT64
severity_number          INT32
severity_text            STRING
body                     STRING               # non-string bodies as canonical JSON
event_name               STRING
trace_id                 FIXED_LEN_BYTE_ARRAY(16)  null when absent
span_id                  FIXED_LEN_BYTE_ARRAY(8)   null when absent
flags                    UINT32
attrs                    MAP<STRING, STRING>  # log attributes outside the allow-list
# denormalized columns (section 5.2)
```

`metrics/number`:

```text
metric_name           STRING               # required, dictionary encoded
time                  TIMESTAMP(us, UTC)
time_unix_nano        INT64
start_time            TIMESTAMP(us, UTC) null
start_time_unix_nano  INT64 null
flags                 UINT32
value_int             INT64  null          # set when the point carries as_int
value_double          DOUBLE null          # set when the point carries as_double
# both null when the point has no recorded value (flags carry
# NO_RECORDED_VALUE)
# denormalized columns
```

`metrics/histogram`:

```text
metric_name, time, time_unix_nano, start_time, start_time_unix_nano, flags
count            UINT64
sum              DOUBLE null
min              DOUBLE null
max              DOUBLE null
bucket_counts    LIST<UINT64>
explicit_bounds  LIST<DOUBLE>
# denormalized columns
```

Histogram consistency (`bucket_counts.len == explicit_bounds.len + 1`) is
validated; violations nack the request as invalid.

Timestamps: `time` is `time_unix_nano / 1000` as Parquet `TIMESTAMP(MICROS,
isAdjustedToUTC=true)` for Spark and DuckDB; the raw `INT64` keeps the
nanoseconds. `time_unix_nano == 0` yields `time = null`.

Attribute maps: `MAP<STRING, STRING>` with the value rendering below. The
format is intentionally lossy for attribute value types; identity hashing and
denormalized columns keep types. Rendering: string as is; int decimal; double
shortest round-trip (Rust `{}`); bool `true`/`false`; bytes lowercase hex;
array and kvlist as JSON (`serde_json` compact, keys sorted, non-finite
doubles as `null`). Keys are unique after validation (section 4), so the map
is valid for both Parquet and DuckDB.

Unsupported inputs in v1: exponential histograms, summaries, exemplars.
Policy `unsupported: drop` (default) drops the rows, counts
`dropped_unsupported{kind}` and acks the request; `unsupported: reject` nacks
the request as permanent.

### 5.2 Denormalization

```yaml
logs:
  denormalize:
    - resource.service.name                         # shorthand: string
    - { path: attrs.http.status_code, column: http_status_code, type: int64 }
```

- `path` is `resource.<key>`, `scope.<key>` or `attrs.<key>`.
- `column` defaults to the path with `.` replaced by `_` and the prefix
  dropped. Collisions between denormalized columns, or with intrinsic column
  names, case-insensitively, are a startup configuration error.
- `type` is one of `string`, `int64`, `double`, `bool`. A value of another
  type is rendered as string when `type: string`, otherwise stored as null and
  counted (`denormalize.type_mismatch{column}`).
- A denormalized column appears in `series` only if its path is part of the
  identity (resource, scope, or an allow-listed log attribute / data point
  attribute). Otherwise it appears only in the values table.
- Sorting is allowed only on intrinsic columns and denormalized columns.

### 5.3 Layout and naming

```text
<base>/v=1/signal=logs/table=values/date=2026-09-21/hour=12/
    part-20260921T121500Z-<writer_id>-<boot_id>-<seq>.parquet
```

- `date` and `hour` come from the block's `window_start` (ingest time), never
  from event timestamps.
- `writer_id` from config; `boot_id` is a UUIDv4 generated at exporter start;
  `seq` is a per-worker monotonic counter, zero-padded to 8 digits. Names are
  frozen when the block is sealed and reused verbatim across retries. Uploads
  use create-only semantics (`PutMode::Create`); an `AlreadyExists` on retry
  is treated as success of that file.
- No manifest. Within a block the `series` file is written first, then the
  values files. A crash therefore never leaves committed values whose
  descriptor was due in the same block without that descriptor.
- No files are written for a table with zero rows; an empty window writes
  nothing and immediately acks any requests that produced no rows.
- Interrupted multipart uploads are aborted on failure; leftovers after a
  crash are covered by a bucket lifecycle rule for incomplete uploads.

### 5.4 Parquet options

ZSTD, target row group about 64 MiB, statistics enabled, dictionary encoding
for strings. Key/value metadata: `format_version=1`,
`series_hash=xxh3_128/canonical_v1`, `writer_id`, `boot_id`, `seq`,
`window_start`, `window_end`.

### 5.5 Reading the data

Because descriptors repeat (eviction, new partition, restart, several
workers), a join on `series_id` must go through a canonical view:

```sql
SELECT * FROM series QUALIFY row_number() OVER
  (PARTITION BY series_id ORDER BY emitted_at DESC) = 1
```

The README documents this view for DuckDB and Spark. Descriptor coverage
guarantee: for every values row in partition `P` written by worker `W`, at
least one descriptor row for the same `series_id` exists in the `series`
dataset of the same signal in partition `P`, written by `W`.

## 6. Buffering, memory and flush

### 6.1 Worker state

```text
cache:    LRU<SeriesId, { descriptor_blob, last_committed_partition }>
          bounded by max_entries and max_bytes
active:   Block { window_start, partition, tables: per-table SortedTableBuffer,
                  pending_series: HashSet<SeriesId>, bytes, requests: Vec<AckToken> }
flushing: Option<Block>
```

`AckToken` holds the request's `Context` and signal type only. The original
`OtapPdata` payload is dropped after extraction; on ack or nack the exporter
rebuilds `OtapPdata::new(ctx, OtapPayload::empty(signal))`.

### 6.2 Admission and ingest of one request

1. Convert the payload to `OtapArrowRecords` (`try_into_with_default`) and
   `decode_transport_optimized_ids`. Conversion failure: permanent nack.
2. Reject if the logical size (`record_batch_logical_bytes`) exceeds
   `max_request_bytes`: permanent nack, `nacks{reason=too_large}`.
3. `extract` produces, without touching the block: descriptors for unique
   series in the batch, values batches per output table, and the set of
   `series_id` per row. Validation failures (duplicate keys, histogram
   inconsistency): permanent nack. Unsupported rows per policy.
4. Reserve: `needed = pinned bytes of extracted batches + descriptor bytes`.
   If `active.bytes + needed > max_block_bytes` and `active` is non-empty,
   rotate first (section 6.3). Only then mutate:
   - for each unique series: if `cache.last_committed_partition !=
     active.partition` and `series_id` not in `active.pending_series`, append
     the descriptor to `active.tables.series` and insert into
     `pending_series`; touch the cache entry (insert if absent), evicting from
     the tail while over either limit;
   - append values to their `SortedTableBuffer`; a building batch that reaches
     `run_target_bytes` is sorted (permutation plus `take`) and sealed as a
     run, the unsorted batch dropped;
   - push the `AckToken`.
   A request that produced zero rows (all unsupported, dropped) is acked
   immediately and never enters a block.
5. Between runs and during merges the worker yields (`tokio::task::yield_now`)
   so control messages are handled.

### 6.3 Rotation

Triggers: window boundary, `active.bytes >= max_block_bytes`, shutdown.

- If `flushing` is empty: seal all building batches, move `active` to
  `flushing`, create a new empty `active` whose `window_start` is the current
  window boundary. Start the flush task.
- If `flushing` is occupied: the worker stops accepting pdata
  (`recv_when(false)`), keeps handling control messages, waits for the flush
  to finish, then rotates. A block therefore belongs to exactly one aligned
  window; byte-triggered rotation inside a window creates a second block for
  the same window with the next `seq`.

### 6.4 Window clock

The exporter does not use engine timers (they are cancelled at shutdown and
are not boundary-aligned). It computes `next_boundary = ceil(now, interval)`
on the wall clock and selects on `sleep_until(next_boundary)` alongside the
inbox. Missed boundaries (long flush) coalesce into one rotation. The
`SystemTime` is read once per boundary; the monotonic clock drives the sleep.

### 6.5 Flush and commit

1. Per table, in order `series`, then values tables: k-way merge over sealed
   runs, materialized in chunks of about `merge_chunk_bytes`, written through
   `AsyncArrowWriter` into `object_store::BufWriter` with
   `capacity = upload_part_bytes` and `max_concurrency = upload_concurrency`.
   Tables are written sequentially, one open writer at a time.
2. All files uploaded: the block is committed. Then, in this order:
   `cache.mark_committed(series, active.partition)` for every id in
   `pending_series`; ack every token; drop the block.
3. Any error before commit: abort the multipart upload, back off, retry the
   whole block with the same file names, until the absolute
   `flush_retry_deadline` measured from the first attempt. Past the deadline:
   nack every token as retryable, `nacks{reason=storage}`, drop the block.
   The cache is not updated, so descriptors are re-emitted with the next
   values.
4. An ack or nack delivery failure (channel closed) is logged and counted;
   a committed block is never re-exported.

### 6.6 Memory budget

Exporter-owned memory per worker:

```text
owned <= max_block_bytes * 2          # active + flushing
       + series_cache.max_bytes
       + max_request_bytes             # one request being extracted
       + run_target_bytes * 2          # sort input + output
       + merge_chunk_bytes
       + parquet_writer_bytes          # row group target + encoder state
       + upload_part_bytes * upload_concurrency
```

Process RSS adds, outside this exporter's control: pdata channel capacity
times request size, receiver admission concurrency times request size,
allocator overhead, and the same formula again for every other worker. The
README shows the aggregate.

`max_request_bytes` (default 16 MiB) is checked before any allocation into
the block; `max_block_bytes` is a block limit only.

## 7. Dataflow integration

### 7.1 Node lifecycle

- `start()`: generate `boot_id`, compute the first boundary, enter the loop:
  `select` over `inbox.recv_when(accept)`, the boundary sleep, and the flush
  task. `accept` is false while `flushing` is occupied and `active` cannot
  rotate.
- `PData`: section 6.2.
- Boundary: section 6.3.
- `Shutdown { deadline }`: stop accepting pdata (buffered pdata force-drained
  by the inbox is admitted into `active` while it fits, otherwise nacked);
  if `flushing` is busy, wait for it within the deadline; then flush `active`
  within the remaining deadline; anything still uncommitted at the deadline
  is nacked with `nacks{reason=shutdown}`.
- `CollectTelemetry`: report metrics.

### 7.2 Acknowledgement contract

With `receiver:otlp` configured with `wait_for_result: true` and the
exporter directly downstream, the producer's request stays open until commit
or nack. The receiver `timeout` must exceed `window.interval +
flush_retry_deadline + expected upload time`; the example config uses
`timeout: 120s` with `flush_retry_deadline: 60s`. A request that times out
on the receiver after admission is still committed by the exporter; the
producer's retry then creates duplicates. This is the documented at-least-once
behavior.

### 7.3 Backpressure

While the worker refuses pdata, the bounded pdata channel fills, the receiver
blocks on the channel send until its timeout, and beyond the configured
admission concurrency it answers `RESOURCE_EXHAUSTED`. No new engine mechanism
is needed.

### 7.4 Configuration

```yaml
type: exporter:series_parquet
config:
  storage: { s3: { base_uri: s3://bucket/otel, region: eu-central-1 } }
  retry: { max_retries: 5, init_backoff: 200ms, max_backoff: 10s,
           backoff_base: 2.0, retry_timeout: 30s }
  writer_id: collector-17
  producer_id_attribute: host.id
  window:
    interval: 15s
    max_block_bytes: 500MiB
    flush_retry_deadline: 60s
  ingress:
    max_request_bytes: 16MiB
  series_cache:
    max_entries: 20000
    max_bytes: 100MiB
  sorting:
    run_target_bytes: 8MiB
    merge_chunk_bytes: 16MiB
  upload:
    part_bytes: 8MiB
    concurrency: 2
  parquet:
    compression: zstd
    row_group_bytes: 64MiB
  unsupported: drop
  logs:
    series_attributes: [logger.name]
    denormalize: [resource.service.name]
    sort:
      - { column: series_id, order: asc }
      - { column: time_unix_nano, order: asc, nulls: last }
  metrics:
    denormalize: [resource.service.name]
    sort:
      - { column: series_id, order: asc }
      - { column: time_unix_nano, order: asc }
```

Sort semantics: strings by raw UTF-8 bytes, integers numeric, fixed-size
bytes lexicographic, nulls per `nulls: first|last` (default `last`).
Descriptor re-emission is tied to the `date/hour` partition and has no
separate interval.

Example pipeline in `configs/series-parquet-s3.yaml`: `receiver:otlp`
(`wait_for_result: true`, `timeout: 120s`) connected directly to
`exporter:series_parquet`, with an explicit `core_allocation`.

### 7.5 Telemetry

Metric set `exporter.series_parquet`:

```text
series_cache.{entries,bytes,hits,misses,evictions}
block.{active_bytes,flushing_bytes,requests_pending}
flush.count{reason=time|bytes|shutdown}, flush.duration, flush.failures
rows_written{table}, files_written{table}
series_emitted{reason=new|partition}
acks, nacks{reason=storage|too_large|invalid|shutdown}
oldest_unacked_seconds
dropped_unsupported{kind}, denormalize.type_mismatch{column}
```

## 8. Error handling

- Payload conversion failure or schema violation: permanent nack, continue.
- Request above `max_request_bytes`: permanent nack.
- Duplicate identity attribute keys or inconsistent histogram: permanent nack.
- Object store failure: retry the sealed block until the absolute
  `flush_retry_deadline`, then retryable nack for the whole block.
- `AlreadyExists` on a create-only upload during retry: treat that file as
  written.
- Parquet encoding error (bug, not I/O): nack the block, release it, error
  event; the exporter keeps running.
- Shutdown deadline exceeded: nack all outstanding requests.
- Unsupported point kind or exemplar: per `unsupported` policy.
- Ack/nack delivery failure: log and count; never re-export.

## 9. Testing

`series-lake`:

- Golden vectors for the canonical encoding and hash, including the edge
  cases of section 4; one vector set produced by an independent Python
  implementation.
- `extract` on logs and metrics fixtures, for both OTLP-bytes input and
  transport-optimized OTAP input with dictionaries; malformed parent ids.
- Cache: entry and byte limits, eviction order, partition tracking.
- Block and buffer: byte accounting with shared buffers (pinned, not
  double-counted), reservation before mutation, whole-request-in-one-block,
  oversize rejection, zero-row request acked immediately.
- Sorting: runs plus merge produce a globally sorted output for every table;
  property test over random keys; null ordering.
- Sink: write to `LocalFileSystem`, read back with `parquet`, assert schemas,
  order, paths, write order (series before values), empty-window behavior,
  frozen names across retries.

Exporter (engine test harness with a controllable clock and a failing
`object_store` mock):

- Ack arrives only after all files of the block exist.
- Descriptor flush fails, then the next block containing the same series
  re-emits the descriptor.
- Same series present in FLUSHING and ACTIVE at once.
- Eviction before commit.
- Boundary arrives while FLUSHING is busy: no pdata admitted, control still
  handled, one rotation after the flush.
- Window crossing an hour during a delayed flush: descriptor lands in the
  destination partition.
- Series upload succeeds, values upload fails, retry reuses names.
- Restart within the same window produces distinct file names.
- Producer disconnect before ack does not remove data.
- Original payload released after extraction (retained bytes measured).
- Shutdown with FLUSHING and ACTIVE both non-empty, with and without meeting
  the deadline. End-to-end receiver-to-exporter shutdown with an outstanding
  request completes with an ack, not a drain timeout.
- INT64_MAX value round-trips through `value_int`.

Compatibility: an integration test reads the output with DuckDB (skipped when
the binary is unavailable), applies the canonical series view and checks that
joining does not change the values row count. A Spark check is manual and
documented.

Benchmark: criterion on convert, extract and hash for a 10k-row batch.

Every test carries `Scenario` and `Guarantees` doc comments.

## 10. Deferred work

Each item below was discussed and consciously left out of this spec. The
design above must not make any of them harder.

### 10.1 Live access to buffered data (next spec)

Goal: real-time inspection of data that the exporter has accepted but not yet
committed, the equivalent of `tail -f | grep ...` for telemetry, plus a look
into the pending buffer and the series cache. Agreed direction:

- Two distinct views: `tail` (accepted live stream, not necessarily durable)
  and `buffer` (ACTIVE plus FLUSHING, accepted but not yet acked). Committed
  data disappears from `buffer`.
- Endpoints, subject to the next spec: `GET /v1/state` (block sizes, ages,
  pending requests, cache size, oldest unacked age), `GET /v1/buffer/{logs|
  metrics}` with simple equality and range filters on intrinsic and
  denormalized columns only, bounded by `max_rows`, `max_bytes` and a timeout,
  returning Arrow IPC or JSON; `GET /v1/series/{id}` and a bounded linear
  scan `GET /v1/series?...` over the cache; `GET /v1/tail/{logs|metrics}` as
  Server-Sent Events with the same filters; `POST /v1/admin/flush`.
- Isolation rules: the introspection path never owns a reference to a block
  long enough to delay its release (reads copy small chunks and terminate
  with `truncated: true` when the generation is gone); the tail is a tap on
  the ingest path with a bounded per-subscriber queue that drops events and
  reports `{"type": "dropped", "records": N}` instead of applying
  backpressure to storage.
- Filters are limited to physical typed columns; no SQL, aggregation, joins
  or regex over attribute maps.
- Open decision for that spec: a listener owned by the exporter versus a
  node-state provider registered with the admin server (which has no such
  hook today and no built-in auth or TLS).

Consequences for this spec: `SortedTableBuffer` keeps sealed runs as Arrow
batches until commit, so a snapshot reader can iterate them; the ingest path
has a single point (after extraction) where a tap can be attached.

### 10.2 Other deferred items

- Traces (`signal=traces/table=series|spans`).
- Exponential histograms, summaries, exemplars (dropped or rejected in v1).
- Second-level file coalescing for low-volume deployments.
- Producer id from transport headers.
- Idempotent replay based on producer batch ids.
- Commit manifests (may return as an optional audit record).
- Typed attribute maps (the v1 format is lossy by decision).
