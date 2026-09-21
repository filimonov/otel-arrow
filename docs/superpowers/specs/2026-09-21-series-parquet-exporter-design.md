# Series Parquet Exporter: Design

Date: 2026-09-21
Status: approved for planning (revision 3, after two rounds of external
review)
Scope: phases 1 and 2 of the series lake work (core crate and Dataflow
exporter). Introspection HTTP API and traces are separate specs; section 10
records what was deferred and why.

## 1. Problem

Collect OTLP logs and metrics from many producers and land them in an object
store as Parquet, with these properties:

- Small, predictable memory: memory owned by the exporter is bounded by two
  block budgets, one cache budget, one request budget and a fixed workspace,
  every term of which is enforced, not estimated.
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
- Values files are sorted by configurable physical columns; series files are
  sorted by `series_id`.
- Layout is Hive-style and readable by Spark and DuckDB without extra tooling.
- Selected attributes can be denormalized into the values tables as typed
  columns.

### 1.1 Non-negotiable invariants

Every change to this component is checked against these four properties:

1. No persistent local state. A restart starts from an empty process.
2. Never more than ACTIVE plus FLUSHING. There is no third block and no
   spill.
3. The series cache is an optimization, never correctness state. Losing it
   (restart, eviction, or deleting it outright) can only increase the volume
   of the `series` dataset, never lose or corrupt data.
4. No producer ACK before its block is durable in object storage.

Sorting is likewise an optimization: with `sorting.enabled: false` every
guarantee above and every schema stays the same, only file order changes.

The engine-independent boundary of `series-lake` (section 3.1) is part of
the design: a standalone binary of the shape "OTLP receiver, `series-lake`,
S3" must remain possible without rewriting the storage engine.

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
  (`mod.rs:335-349`). Conversion allocates before any exporter check can
  run (`pdata/src/payload.rs:632`).
- OTLP receiver `wait_for_result: true` holds the gRPC/HTTP request open until
  the immediate downstream node acks or nacks
  (`otap_grpc/otlp/server_new.rs:521`). The receiver awaits the channel send
  first, so a full pdata channel blocks the request until the receiver
  timeout; exhausted ack slots return `RESOURCE_EXHAUSTED`. Exporters ack via
  `EffectHandler::notify_ack` / `notify_nack`; `AckMsg` needs an `OtapPdata`,
  and the batch processor shows how to ack from a stored `Context` with
  `OtapPayload::empty(signal)` (`batch_processor/mod.rs:1611`). Ack delivery
  itself awaits a channel send (`engine/src/effect_handler.rs:301`).
- `ExporterInbox::recv_when(accept_pdata)` (`engine/src/message.rs:874`) lets
  an exporter keep receiving control messages while refusing pdata. On
  shutdown the inbox latches `Shutdown`, force-drains buffered pdata to the
  exporter first, and releases `Shutdown` only when the backlog is empty
  (`message.rs:428-440`, `633`). The topic exporter nacks pdata surfaced this
  way (`topic_exporter/mod.rs:473`).
- Shutdown order (`engine/src/pipeline_ctrl.rs:541-615`): all engine timers
  are cancelled, receivers get `DrainIngress`, receivers wait for outstanding
  `wait_for_result` slots until the deadline, and only then processors and
  exporters receive `Shutdown`. Engine timers fire at `StartTimer` time plus
  duration and carry no boundary timestamp (`pipeline_ctrl.rs:95,141`). The
  engine's simulated clock covers monotonic time only (`engine/src/clock.rs:52`).
- `pdata/src/otap/memory.rs` provides `record_batch_pinned_bytes`
  (deduplicated retained-buffer capacity, excluding struct and allocator
  overhead) and `record_batch_logical_bytes`.
- OTLP timestamps are `u64` and are cast to `i64` during conversion
  (`pdata/src/encode/mod.rs:323`). Nested attribute values are CBOR-encoded
  during conversion, which collapses NaN payloads (`encode/mod.rs:566`,
  `encode/cbor.rs:65`). Number points may carry no value independently of
  flags (`encode/mod.rs:656`).
- `object_store` 0.13.2: `BufWriter` buffers up to `capacity` bytes, then
  streams multipart; `PutMultipartOptions` has no `PutMode`, so create-only
  uploads are not available for multipart objects. `WriteMultipart` holds
  one accumulating buffer plus in-flight parts.
- `policies.resources.core_allocation` selects cores per pipeline; the number
  of exporter instances per process is the number of pipeline instances that
  contain the node.
- Benchmarks: `criterion` in the workspace, an S3-compatible endpoint example
  (`configs/trafficgen-parquet-local-s3.yaml`), a nightly backpressure
  scenario with `wait_for_result` (`docs/benchmarks.md`). No `proptest`, no
  failpoint crate, no soak or chaos suite.
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
  `series_id` and typed denormalized columns, produced in slices no larger
  than `run_target_bytes`. Validates attribute lists and histograms; enforces
  the extracted-output budget while building.
- `cache`: LRU keyed by `SeriesId`, bounded by entries and bytes, tracks the
  last committed partition per series.
- `buffer`: one `SortedTableBuffer` (building batch plus sealed sorted runs)
  used for every output table, and `Block` (byte accounting, pending series
  set, request tokens, generation number).
- `sink`: k-way merge of runs into Parquet files via `object_store`, paths,
  file naming, per-table write order, writer memory limit.
- `clock`: `WallClock` trait (system and test implementations) and the window
  boundary calculation.
- `config`: format configuration types (identity, denormalize, sort, layout,
  budgets).

### 3.2 `exporter:series_parquet`

A local exporter in `core-nodes` behind feature `series_parquet`, URN
`urn:otel:exporter:series_parquet`, metric set `exporter.series_parquet`.
It owns the window clock, the ack contexts, the ACTIVE/FLUSHING pair, the
state machine of sections 6 and 7 and telemetry, and delegates all data work
to `series-lake`.

One pipeline instance equals one worker with its own cache and block pair.
Configured budgets are per worker. The README states that
`core_allocation` must be set explicitly and gives the aggregate formula of
section 6.6.

## 4. Series identity (canonical encoding v1)

`series_id = XXH3_128(identity_bytes)` with seed 0, stored as 16 bytes in the
xxHash canonical big-endian representation, rendered as 32 lowercase hex
characters in APIs and logs. `identity_bytes` is also stored in the `series`
row (section 5.1) so that `xxh3_128(identity_bytes) == series_id` can be
verified by any reader.

`identity_bytes` is built from typed values. Every value is
`tag:u8 ++ len:u32_be ++ payload`:

```text
tag  type     payload
0x01 string   UTF-8 bytes as received (no normalization)
0x02 bytes    raw bytes
0x03 int64    8 bytes, two's complement, big-endian
0x04 double   8 bytes, IEEE 754 binary64, big-endian; any NaN is encoded as
              the canonical quiet NaN 0x7FF8000000000000; -0.0 is preserved
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
- A duplicate key inside any attribute list of the request (identity or not)
  makes the request invalid: permanent nack, `nacks{reason=invalid}`.
- Missing string fields are encoded as empty strings; unset attribute values
  are encoded as `null`. `42` (int64) and `"42"` (string) are different
  series.
- Nothing else is part of the identity: timestamps, values, exemplars, body,
  severity, trace and span ids, flags, dropped counts, metric description,
  log attributes outside the allow-list.
- The encoder never panics on any input; this is a fuzz target.

Every semantic field stored in a `series` row is either part of the identity
or explicitly listed as non-identity metadata. Non-identity metadata:
`description` (metrics), `emitted_at`, `producer_id`. Consumers must not
expect them to be constant per `series_id`.

The encoding is specified in `crates/series-lake/docs/FORMAT.md`, the
implementation-independent format document (canonical identity, dataset
schemas, partition layout, file and delivery semantics, compatibility rules;
no Rust, Dataflow or buffer internals), with golden vectors in
`crates/series-lake/tests/golden/*.json` (input descriptor, hex canonical
bytes, hex hash). Float inputs in vectors are given as bit
patterns. Vectors must cover: empty string, missing field, int 0 and -0.0,
INT64_MIN and INT64_MAX, `42` vs `"42"`, several NaN bit patterns hashing
equal, +Inf, -Inf, non-ASCII UTF-8 including supplementary planes, embedded
NUL, bytes, nested array, nested kvlist, key ordering with common prefixes.
At least one vector set is generated independently in Python from the spec
text, and every vector is also run through OTLP-to-OTAP conversion so the
converted representation hashes identically.

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
It is not part of the identity hash, but the README states the semantic
requirement: it should be stable across producer restarts and unique among
concurrently running producers (the role Thanos external labels play), because
future compaction, deduplication and replay extensions rely on it.

`series` (both signals), always sorted by `series_id`:

```text
identity_bytes      BINARY               # canonical bytes hashed into series_id
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
time                     TIMESTAMP(us, UTC) null
time_unix_nano           INT64 null
observed_time            TIMESTAMP(us, UTC) null
observed_time_unix_nano  INT64 null
severity_number          INT32
severity_text            STRING
body                     STRING               # rendering below
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
time                  TIMESTAMP(us, UTC) null
time_unix_nano        INT64 null
start_time            TIMESTAMP(us, UTC) null
start_time_unix_nano  INT64 null
flags                 UINT32
value_int             INT64  null          # set when the point carries as_int
value_double          DOUBLE null          # set when the point carries as_double
# both null when the point carries no value; flags are stored as received
# and never inferred
# denormalized columns
```

`metrics/histogram`:

```text
metric_name, time, time_unix_nano, start_time, start_time_unix_nano, flags
count            UINT64
sum              DOUBLE null
min              DOUBLE null
max              DOUBLE null
bucket_counts    LIST<UINT64>            # empty when the point has no distribution
explicit_bounds  LIST<DOUBLE>            # empty when the point has no distribution
# denormalized columns
```

Histogram validation: either both lists are empty, or
`bucket_counts.len == explicit_bounds.len + 1`. Anything else nacks the
request as invalid. Bound monotonicity and bucket totals are not checked.

Timestamps: OTLP timestamps are `u64` nanoseconds. Values in
`1..=i64::MAX` are stored raw in the `INT64` column and as
`TIMESTAMP(MICROS, isAdjustedToUTC=true)` after integer division by 1000.
`0` stores null in both columns. Values above `i64::MAX` store null in both
columns and count `timestamp.out_of_range`. This applies to every timestamp
column. Supported readers: DuckDB 1.1 or later (tested), Spark 3.5 or later
(documented, manual check).

Attribute maps: `MAP<STRING, STRING>` with non-null keys and nullable values.
The format is intentionally lossy for attribute value types; identity
hashing, `identity_bytes` and denormalized columns keep types. Rendering
(`render_v1`, one function used everywhere): string as is; int decimal;
double shortest round-trip (Rust `{}`, `NaN`, `inf`, `-inf`); bool
`true`/`false`; bytes lowercase hex; unset value: null; array and kvlist as
`serde_json` compact JSON with keys sorted, nested strings/ints/bools as JSON
values, nested doubles as JSON numbers with non-finite values as strings
`"NaN"`, `"inf"`, `"-inf"`, nested bytes as hex strings, nested unset as
`null`. Log `body` uses the same rendering: string bodies as is, others as
JSON. An empty attribute list is an empty map, never null.

Unsupported inputs in v1 and their policy:

- Exemplars (children of supported points) are always dropped, the parent
  point is kept, `dropped_unsupported{kind=exemplar}` counts exemplars.
- Exponential histogram and summary points: `unsupported: reject` (default)
  nacks the whole request as permanent, `nacks{reason=unsupported}`;
  `unsupported: drop` drops those points, counts them and keeps the rest.
  Rejection is atomic per request. A request that yields zero output rows
  after drops is acked immediately; a request with any output rows is acked
  only when its block commits.

The README lists "exponential histograms and summaries are rejected by
default" as a v1 limitation next to the configuration knob.

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
  type is rendered with `render_v1` when `type: string`, otherwise stored as
  null and counted (`denormalize.type_mismatch{column}`).
- A denormalized column appears in `series` only if its path is part of the
  identity (resource, scope, or an allow-listed log attribute / data point
  attribute). Otherwise it appears only in the values table.

Schema contract per dataset: within one `base_uri`, changes are additive
only (new denormalized columns, new nullable intrinsic columns). Changing the
type or path of an existing column, or reusing a column name for a different
path, requires a new `base_uri`. Each file carries `schema_fingerprint`
(xxh3_64 of the ordered column names and types) in its metadata; the README
explains how to detect mixed fingerprints under one dataset.

### 5.3 Layout and naming

```text
<base>/v=1/signal=logs/table=values/date=2026-09-21/hour=12/
    part-20260921T121500Z-<writer_id>-<boot_id>-<seq>.parquet
```

- `date` and `hour` come from the block's `window_start` (ingest time), never
  from event timestamps.
- `writer_id` from config; `boot_id` is a UUIDv4 generated at exporter start;
  `seq` is a per-worker monotonic counter, zero-padded to 8 digits. Names are
  frozen when the block is sealed and reused verbatim across retries.
  Retries overwrite the same names with the same content; `boot_id` makes
  collisions with other blocks or processes impossible in practice. No
  conditional-put semantics are used.
- No manifest. Within a block the `series` file is written first, then the
  values files in a fixed order. Completed objects become visible atomically
  per object (object store semantics).
- Visibility guarantees, stated narrowly: readers may observe a block
  partially (some files present) while it is being written or after a
  permanent failure; rows of a nacked request may therefore exist in storage
  and appear again after the producer retries; there is no snapshot
  consistency across files. Referential coverage (section 5.5) holds for
  every visible values file because its descriptors were completed earlier.
- No files are written for a table with zero rows; an empty window writes
  nothing.
- Interrupted multipart uploads are aborted on a best-effort basis; leftovers
  after a crash are covered by a bucket lifecycle rule for incomplete
  uploads.

### 5.4 Parquet options

ZSTD, target row group about 64 MiB, statistics enabled, dictionary encoding
for strings. Key/value metadata: `format_version=1`,
`series_hash=xxh3_128/canonical_v1`, `schema_fingerprint`,
`sort_key` (comma-separated `column:asc|desc:nulls_first|nulls_last`, or
`none`), `writer_id`, `boot_id`, `seq`, `window_start`, `window_end`,
`row_count`, `min_time_unix_nano`, `max_time_unix_nano` (values tables).
Together `(signal, table, partition, format_version, schema_fingerprint,
sort_key)` defines a compaction scope: files in the same scope can later be
merged by a k-way merge without re-sorting, files in different scopes are
never merged together.

### 5.5 Reading the data

Because descriptors repeat (eviction, new partition, restart, several
workers), a join on `series_id` must go through a canonical view:

```sql
SELECT * FROM read_parquet('.../table=series/**', filename = true)
QUALIFY row_number() OVER
  (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1
```

The README documents this view for DuckDB and Spark. Descriptor coverage
guarantee: for every values row in partition `P` written by worker `W`, at
least one descriptor row for the same `series_id` exists in the `series`
dataset of the same signal in partition `P`, written by `W`, and that
descriptor became visible before the values file. A reader that lists values
files first and series files second therefore always finds descriptors for
what it read.

## 6. Buffering, memory and flush

### 6.1 Worker state

```text
cache:    LRU<SeriesId, { descriptor_blob, last_committed_partition }>
          bounded by max_entries and max_bytes
active:   Rc<Block> { generation, window_start, partition,
                      tables: per-table SortedTableBuffer,
                      pending_series: HashSet<SeriesId>, bytes, requests: Vec<AckToken> }
flushing: Option<Rc<Block>>          # shared with the flush task
```

`AckToken` holds the request's `Context` and signal type only. The original
`OtapPdata` payload is dropped after extraction; on ack or nack the exporter
rebuilds `OtapPdata::new(ctx, OtapPayload::empty(signal))`. Each token is
charged `context_bytes` (a fixed configured estimate, default 1 KiB) against
the block, and a block holds at most `max_requests_per_block` tokens
(default 4096); reaching that limit triggers rotation like the byte limit.

### 6.2 Admission and ingest of one request

Admission runs inline in the node loop and is bounded by the request budgets,
so the loop is blocked for at most the work of one request.

1. Reject if the payload's logical size (`OtapPayload` cached size) exceeds
   `max_request_bytes`: permanent nack, `nacks{reason=too_large}`. This
   check precedes conversion.
2. Convert to `OtapArrowRecords` (`try_into_with_default`) and
   `decode_transport_optimized_ids`. Conversion failure: permanent nack.
3. `extract` produces, without touching the block: descriptors for unique
   series, values batches per output table in slices of at most
   `run_target_bytes`, and per-row `series_id`. It stops and returns
   `too_large` as soon as the pinned size of its output exceeds
   `max_extracted_bytes` (default `2 * max_request_bytes`). Validation
   failures: permanent nack. Unsupported inputs per section 5.1.
4. Reserve: `needed = pinned bytes of extracted slices + descriptor bytes +
   context_bytes`. If `needed > max_block_bytes`: permanent nack
   (`too_large`), even into an empty block. If `active.bytes + needed >
   max_block_bytes` or `active.requests.len() == max_requests_per_block`,
   rotate first (section 6.3); the extracted output is held meanwhile and
   counts as the request's extraction slot. Only then mutate:
   - for each unique series: if `cache.last_committed_partition !=
     active.partition` and `series_id` not in `active.pending_series`, append
     the descriptor to `active.tables.series` and insert into
     `pending_series`; touch the cache entry (insert if absent), evicting from
     the tail while over either limit;
   - append each values slice to its `SortedTableBuffer`; a building batch
     that reaches `run_target_bytes` is sorted (permutation plus `take`) and
     sealed as a run, the unsorted batch dropped. A run is at most
     `run_target_bytes + one slice`, so at most `2 * run_target_bytes`.
     With `sorting.enabled: false` runs are sealed without sorting and the
     flush concatenates them in arrival order;
   - push the `AckToken`.
   A request that produced zero rows is acked immediately and never enters a
   block.

### 6.3 Rotation

Triggers: window boundary, `active.bytes` or `active.requests` at their
limit, shutdown.

- If `flushing` is empty: seal all building batches, move `active` to
  `flushing`, create a new empty `active` with `generation + 1` and
  `window_start` equal to the current window boundary. Spawn the flush task
  (`spawn_local`) with a clone of the `Rc<Block>` and a cancellation token.
- If `flushing` is occupied: the worker sets `accept = false`, keeps
  handling control messages, and completes the rotation when the flush task
  finishes. A request already extracted and waiting for space stays in its
  extraction slot until then. A block therefore belongs to exactly one
  aligned window; byte-triggered rotation inside a window creates a second
  block for the same window with the next `seq`.

### 6.4 Window clock

The exporter does not use engine timers. It uses the `WallClock` trait from
`series-lake` (system time in production, injectable in tests; the engine's
simulated clock is monotonic-only and is not sufficient).

```text
boundary(t)     = floor(t / interval) * interval
next_boundary   = max(boundary(now) + interval, last_boundary + interval)
```

`last_boundary` starts at `boundary(start_time)`. The loop sleeps on the
monotonic clock for `next_boundary - now` and re-reads the wall clock on
wake; if the wall clock is still before `next_boundary` (backward step) it
sleeps again; if it is beyond several boundaries (forward step or long flush)
all missed boundaries coalesce into one rotation and `last_boundary` jumps to
`boundary(now)`. Boundaries never move backwards, so a window is never
reopened. The `select` in the node loop is biased: boundary first, then flush
completion, then inbox, so a request never lands in a window whose boundary
has already passed.

### 6.5 Flush and commit

The flush task owns the I/O; the node loop keeps polling the inbox, the
boundary sleep and the task's completion.

1. Per table, in order `series`, then values tables: k-way merge over sealed
   runs, materialized in chunks of about `merge_chunk_bytes`, written through
   `AsyncArrowWriter` into `object_store::BufWriter` with
   `capacity = upload.part_bytes` and `max_concurrency = upload.concurrency`.
   After every chunk, if `writer.memory_size() >= parquet.writer_limit_bytes`
   the current row group is closed. Tables are written sequentially, one open
   writer at a time.
2. All files uploaded: the block is committed. The task returns
   `Committed`; the node loop then, in this order:
   `cache.mark_committed(series, block.partition)` for every id in
   `block.pending_series` (the flushed block's own partition, never the
   active one); acks every token; drops the block.
3. Any error before commit: abort the multipart upload best-effort, back off,
   retry the whole block with the same file names, until the absolute
   `flush_retry_deadline` measured from the first attempt. An upload whose
   response was lost is retried like a failure; overwriting an identical
   object is safe. Past the deadline the task returns `Failed`; the node
   loop nacks every token as retryable, `nacks{reason=storage}`, and drops
   the block. The cache is not updated, so descriptors are re-emitted with
   the next values.
4. Cancellation: the node loop cancels the task's token at the shutdown
   deadline; the task stops at the next chunk boundary, aborts the current
   upload best-effort and returns `Failed`. Cleanup is bounded by
   `upload.abort_timeout` (default 5 s).
5. Notifications are sent from the node loop. A send that fails because the
   channel is closed is counted (`notify.failures`) and logged; a committed
   block is never re-exported.

### 6.6 Memory budget

Exporter-owned memory per worker, every term enforced:

```text
owned <= max_block_bytes * 2                       # active + flushing
       + max_requests_per_block * 2 * context_bytes # tokens
       + series_cache.max_bytes
       + max_extracted_bytes                        # one extraction slot
       + run_target_bytes * 3                       # run input (<= 2x) + sorted output
       + merge_chunk_bytes
       + parquet.writer_limit_bytes + merge_chunk_bytes
       + upload.part_bytes * (upload.concurrency + 1)
```

Not counted, and documented as such: the incoming `OtapPdata` before step 1
(bounded by the receiver's request limit and the pdata channel capacity),
struct and allocator overhead, and the same formula again for every other
worker. The README shows the aggregate.

## 7. Dataflow integration

### 7.1 Node loop

```text
loop:
  select (biased):
    boundary sleep elapsed      -> rotation (6.3)
    flush task finished         -> commit or fail handling (6.5), then
                                   complete a pending rotation if any
    inbox.recv_when(accept)     -> PData: admission (6.2)
                                   Control: below
```

`accept` is false while `flushing` is occupied and a rotation is pending,
and after shutdown has begun.

Control messages:

- `Shutdown { deadline }`: pdata force-drained by the inbox before this
  message arrives (section 2) is nacked with `nacks{reason=shutdown}` once
  shutdown has been detected via the first force-drained message or the
  `Shutdown` itself; simplicity wins over admitting into a closing block.
  Then: if `flushing` is busy, wait for it within the deadline; then rotate
  and flush `active` within the remaining deadline; at the deadline cancel
  the flush task; anything uncommitted is nacked with
  `nacks{reason=shutdown}`.
- `CollectTelemetry`: report metrics.
- Others: ignored.

### 7.2 Acknowledgement contract

With `receiver:otlp` configured with `wait_for_result: true` and the
exporter directly downstream, the producer's request stays open until commit
or nack. An admitted request may later commit or fail; producers must retain
and retry on timeout or nack. The receiver `timeout` bounds the producer's
wait and should cover, in the worst case:

```text
channel residence
+ remaining flush of the block ahead (<= flush_retry_deadline + upload time)
+ window.interval
+ own flush (<= flush_retry_deadline + upload time)
```

The example uses `timeout: 180s` with `flush_retry_deadline: 60s` and
`interval: 15s`. This reduces but does not eliminate timeouts after
admission; a request that times out on the receiver is still committed or
nacked by the exporter, and the producer's retry then creates duplicates.
This is the documented at-least-once behavior.

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
    max_requests_per_block: 4096
    flush_retry_deadline: 60s
  ingress:
    max_request_bytes: 16MiB
    max_extracted_bytes: 32MiB
    context_bytes: 1KiB
  series_cache:
    max_entries: 20000
    max_bytes: 100MiB
  sorting:
    enabled: true
    run_target_bytes: 8MiB
    merge_chunk_bytes: 16MiB
  upload:
    part_bytes: 8MiB
    concurrency: 2
    abort_timeout: 5s
  parquet:
    compression: zstd
    row_group_bytes: 64MiB
    writer_limit_bytes: 96MiB
  unsupported: reject
  logs:
    series_attributes: [logger.name]
    denormalize: [resource.service.name]
    values_sort:
      - { column: series_id, order: asc }
      - { column: time_unix_nano, order: asc, nulls: last }
  metrics:
    denormalize: [resource.service.name]
    values_sort:
      - { column: series_id, order: asc }
      - { column: time_unix_nano, order: asc }
```

`values_sort` applies to every values dataset of the signal; keys are
validated at startup against each dataset's physical schema (intrinsic or
denormalized columns only). `series` datasets are always sorted by
`series_id` and have no sort configuration. Sort semantics: strings by raw
UTF-8 bytes, integers numeric, doubles by IEEE total order (`f64::total_cmp`,
so -0.0 < +0.0 and NaN sorts last ascending), booleans false before true,
fixed-size bytes lexicographic, nulls per `nulls: first|last` (default
`last`). Descriptor re-emission is tied to the `date/hour` partition and has
no separate interval.

Sort key guidance for the README: `series_id, time_unix_nano` is a good
locality and compression default. When the dominant queries filter on a
denormalized column (`service_name`, `environment`), placing those columns
first (`service_name, series_id, time_unix_nano`) gives Parquet row-group
statistics real pruning power, at some cost in per-series locality.

Example pipeline in `configs/series-parquet-s3.yaml`: `receiver:otlp`
(`wait_for_result: true`, `timeout: 180s`) connected directly to
`exporter:series_parquet`, with an explicit `core_allocation`.

### 7.5 Telemetry

Metric set `exporter.series_parquet`:

```text
series_cache.{entries,bytes,hits,misses,evictions}
block.{active_bytes,flushing_bytes,requests_pending,generation}
flush.count{reason=time|bytes|requests|shutdown}, flush.duration,
flush.failures, flush.retries
rows_written{table}, files_written{table}
series_emitted{reason=new|partition}
acks, nacks{reason=storage|too_large|invalid|unsupported|shutdown}
oldest_unacked_seconds
dropped_unsupported{kind}, denormalize.type_mismatch{column},
timestamp.out_of_range, notify.failures
memory.budget_bytes, memory.accounted_bytes, memory.unaccounted_rss_bytes
```

`memory.budget_bytes` is the section 6.6 bound computed from configuration,
`memory.accounted_bytes` is the live sum of the enforced terms, and
`memory.unaccounted_rss_bytes` is process RSS (sampled the same way the
memory limiter samples it) minus `accounted_bytes` summed over workers. A
growing unaccounted value is the signal that the invariant is being bypassed
by Arrow, the allocator or `object_store`. The README states the memory
bound as part of the public contract, with the process-level formula.

## 8. Error handling

- Request above `max_request_bytes` or `max_extracted_bytes`, or whose
  reservation exceeds `max_block_bytes`: permanent nack.
- Payload conversion failure or schema violation: permanent nack, continue.
- Duplicate attribute keys or inconsistent histogram: permanent nack.
- Exponential histogram or summary point under `unsupported: reject`:
  permanent nack.
- Object store failure: retry the sealed block until the absolute
  `flush_retry_deadline`, then retryable nack for the whole block.
- Parquet encoding error (bug, not I/O): treated as a block failure without
  retry; nack the block, error event; the exporter keeps running.
- Shutdown deadline exceeded: cancel the flush task, nack all outstanding
  requests.
- Ack/nack delivery failure: log and count; never re-export.

## 9. Testing

### 9.1 `series-lake` unit and property tests

- Golden vectors for the canonical encoding and hash, including the edge
  cases of section 4; one vector set produced by an independent Python
  implementation; every vector also checked through OTLP-to-OTAP conversion.
- `extract` on logs and metrics fixtures, for both OTLP-bytes input and
  transport-optimized OTAP input with dictionaries; malformed parent ids;
  extracted-output budget enforcement.
- Cache: entry and byte limits, eviction order, partition tracking.
- Block and buffer: byte accounting with shared buffers (pinned, not
  double-counted), reservation before mutation, whole-request-in-one-block,
  oversize rejection into an empty block, request-count limit, zero-row
  request acked immediately.
- Reference oracle: random input, split into arbitrary requests and runs,
  flushed through `SortedTableBuffer` and the sink to `LocalFileSystem`; the
  output read back must equal a naive `Vec<Row>` sort of the same input,
  for every dataset. This is the main property test.
- Sink: schemas, order, paths, write order (series before values),
  empty-window behavior, frozen names across retries, writer memory limit
  closing row groups.
- Clock: boundary calculation at exact boundaries, backward and forward wall
  clock steps, coalescing.

### 9.2 Exporter tests (engine harness, injected clocks, failing store)

- Ack arrives only after all files of the block exist.
- Descriptor flush fails, then the next block containing the same series
  re-emits the descriptor.
- Flush completes across an hour boundary: committed partition is the
  flushed block's, and the series first seen in the new hour gets its
  descriptor there.
- Same series present in FLUSHING and ACTIVE at once.
- Eviction before commit.
- Boundary arrives while FLUSHING is busy: no pdata admitted, control still
  handled, one rotation after the flush, request waiting in its extraction
  slot admitted into the new block.
- Series upload succeeds, values upload fails, retry reuses names and
  overwrites.
- Restart within the same window produces distinct file names.
- Producer disconnect before ack does not remove data.
- Original payload released after extraction (retained bytes measured).
- Force-drained pdata during shutdown is nacked; shutdown with FLUSHING and
  ACTIVE both non-empty, with and without meeting the deadline; cancellation
  stops the flush task within `abort_timeout`.
- Mixed request (supported and dropped rows) acked only at commit; reject
  policy nacks atomically.
- INT64_MAX value round-trips through `value_int`; timestamps above
  `i64::MAX` become null and are counted.

### 9.3 Fuzzing

`proptest` (added to the workspace) and `cargo-fuzz` targets for `canonical`
and `extract`: deep arrays and kvlists, arbitrary UTF-8, empty strings, huge
attribute maps, NaN patterns, duplicate keys, malformed parent ids, histogram
shape mismatches, large bodies, shared Arrow buffers. Invariants: never
panics; semantically equal OTLP and OTAP inputs produce the same `series_id`.

### 9.4 End-to-end

Definition: a real OTLP producer over the network, a real `df_engine`
process with `receiver:otlp` (`wait_for_result: true`) and
`exporter:series_parquet`, a real S3 protocol endpoint (MinIO or LocalStack,
as in `configs/trafficgen-parquet-local-s3.yaml`), and DuckDB as the reader.
A mock object store does not count as end-to-end.

The producer knows exactly what it sent (for example 100 producers, 10k
requests, 1M log records, 50k metric points, N known series). Assertions:
acked requests equal expected; values row counts equal expected; descriptor
coverage holds per partition; every file is sorted as configured; every
`series_id` recomputes from `identity_bytes`; the canonical series view join
does not change the values row count. First implemented for logs against
`LocalFileSystem`, then MinIO, then metrics. Runs in CI.

### 9.5 Chaos and failure

- A TCP proxy (toxiproxy or equivalent) between the exporter and the store
  injects latency, bandwidth limits, resets, timeouts and outages while
  producers keep sending. Assertions: `block.active_bytes` and
  `block.flushing_bytes` never exceed their limits, RSS stays within the
  documented bound, producers block or fail with the expected statuses, and
  after recovery `oldest_unacked_seconds` returns to baseline with no
  acknowledged data missing.
- Failpoints in test builds (`after_series_upload`, `mid_values_upload`,
  `after_values_upload`, `before_ack`) combined with SIGKILL, restart and
  producer retry. Invariant: no acknowledged request is missing from storage;
  duplicates are allowed.

### 9.6 Soak and qualification

Three modes: PR (minutes, functional load, forced rotations, one outage and
recovery), nightly (hours, realistic cardinality, periodic slowdowns and
failures, producer reconnects, exporter restarts), qualification (24 to 72
hours, sustained load, cardinality churn, random failures). Cardinality
profiles: stable (10k hot series), churn (1M distinct series against a 20k
cache), mixed (80/20). Assertions on exporter metrics (cache bytes and
entries within limits, block bytes within limits, eviction rate sane) and on
RSS: p99 after the first hour must equal p99 at the end within tolerance; a
rising baseline is a blocking leak. Acceptance criterion: under any supported
storage latency or failure the exporter's memory stays within its configured
bound, and after storage recovers the backlog drains and producers resume
without loss of acknowledged data.

### 9.7 Benchmarks

Layered criterion and pipeline benchmarks so the cost of each layer is
visible: OTLP to noop; plus extract and hash; plus sort; plus Parquet to a
local store; plus ZSTD; plus MinIO. Reported per stage: records per second
per core, CPU per record, bytes allocated per record, peak RSS, output bytes
per input record.

### 9.8 Quality gates

- Core ready: 9.1 and 9.3 pass; streaming output equals the reference
  oracle; declared memory counters never exceeded.
- Integration ready: 9.2 and 9.4 pass for logs and metrics; ack only after
  object completion; restart and failure tests pass.
- Canary ready: nightly soak with storage latency, errors and restarts shows
  no RSS trend and no lost acknowledged records.
- Production ready: shadow deployment next to the current pipeline with
  offline comparison of counts and cardinality, then gradual producer
  migration. This is a rollout step, not part of this design.

Every test carries `Scenario` and `Guarantees` doc comments.

## 10. Deferred work

Each item below was discussed and consciously left out of this spec. The
design above must not make any of them harder.

### 10.1 Live access to buffered data (next spec)

Goal: real-time inspection of data that the exporter has accepted but not yet
committed, the equivalent of `tail -f | grep ...` for telemetry, plus a look
into the pending buffer and the series cache. Agreed direction:

- Three distinct states exposed by the API: `accepted` (admitted into a
  block), `committed` (files complete, ack possibly still in flight), and
  `acked`. `tail` shows accepted rows as they are admitted; `buffer` shows
  ACTIVE plus FLUSHING; both exclude requests that were rejected at
  admission.
- Endpoints, subject to the next spec: `GET /v1/state` (block sizes, ages,
  generations, pending requests, cache size, oldest unacked age),
  `GET /v1/buffer/{logs|metrics}` with simple equality and range filters on
  intrinsic and denormalized columns only, bounded by `max_rows`,
  `max_bytes` and a timeout, returning Arrow IPC or JSON;
  `GET /v1/series/{id}` and a bounded linear scan `GET /v1/series?...` over
  the cache; `GET /v1/tail/{logs|metrics}` as Server-Sent Events with the
  same filters; `POST /v1/admin/flush`.
- Isolation rules: readers copy bounded chunks and never hold a borrow across
  an await; a read that outlives its generation terminates with
  `truncated: true`; the tail is a tap placed after admission (so rejected
  requests never appear) with a bounded per-subscriber queue that drops
  events and reports `{"type": "dropped", "records": N}` instead of applying
  backpressure to storage; a global introspection memory and concurrency
  budget is reserved in the configuration.
- Filters are limited to physical typed columns; no SQL, aggregation, joins
  or regex over attribute maps.
- Open decision for that spec: a listener owned by the exporter versus a
  node-state provider registered with the admin server (which has no such
  hook today and no built-in auth or TLS).

Consequences already honored by this spec: blocks are worker-owned
`Rc<Block>` with a generation number, so a snapshot can name the generation
it read; the flush task shares the block instead of owning it, so FLUSHING
stays readable; `SortedTableBuffer` exposes an iterator over the building
batch and the sealed runs that yields bounded copies (slices pin their parent
allocation, so copies are made per chunk and released before the next
await); admission has a single completion point after which a tap can be
attached; the block is dropped only after notifications are sent, which is
the buffer-removal point.

### 10.2 Other deferred items

- Traces (`signal=traces/table=series|spans`). The trace spec may choose a
  different descriptor model for spans while reusing the hashing mechanism
  and the storage conventions; spans are not forced into the "series" shape.
- Exponential histograms and summaries (rejected by default in v1) and
  exemplars (dropped in v1).
- A separate stateless `series-lake-compactor` batch job that merges
  small files within one compaction scope (section 5.4) into larger ones,
  with a Quickwit-style policy (target size, max merge factor, max merge
  rounds, maturation age after which a partition is never rewritten). The
  writer stays unaware of it; the current layout, metadata and sort keys
  are designed so that it needs nothing from the writer.
- Optional discovery metadata (a per-hour file index) built asynchronously if
  object listing ever becomes the bottleneck; never a correctness
  dependency.
- Producer id from transport headers.
- Idempotent replay based on producer batch ids.
- Commit manifests or a commit index for block-atomic reads (may return as an
  optional audit record).
- Typed attribute maps (the v1 format is lossy by decision).

## 11. Implementation order

1. `series-lake` alone: canonical encoding with golden vectors, extract,
   cache, `SortedTableBuffer`, sink to `LocalFileSystem`, reference oracle
   property test, fuzz targets. No engine, no network, no S3.
2. Vertical slice for logs: `receiver:otlp` to `exporter:series_parquet` to
   `LocalFileSystem`, read with DuckDB, real gRPC producer, real ack. Then
   the same against MinIO.
3. The chaos and soak harness (9.5, 9.6) starts running against the logs
   slice immediately, before metrics exist: continuous load, random storage
   delays and resets, periodic exporter restarts, cardinality changes, RSS
   inspection. The emergent behavior of Arrow ownership, allocator, async
   S3, retry, slow producers, shutdown and timer boundaries is the main
   risk, and unit tests cannot cover it.
4. Metrics (`number`, `histogram`) reusing the same machinery.
5. Benchmark suite and quality gates.
