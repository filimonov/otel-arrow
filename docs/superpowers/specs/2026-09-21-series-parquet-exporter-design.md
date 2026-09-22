# Series Parquet Exporter: Design

Date: 2026-09-21
Status: approved for planning (revision 7, after three rounds of external
review)
Scope: phases 1 and 2 of the series lake work (core crate and Dataflow
exporter). Introspection HTTP API and traces are separate specs; section 10
records what was deferred and why.

What changed in revision 5: merged metrics values (5.1), request cost model
(5.6), workers, sources and threading (6.8), network distribution and
producer timeouts (7.6), and the deferred shared writer with parallel
preparation (10.4).

What changed in revision 6: recommended durable-buffer deployment and the
exporter's acknowledgement invariant (1, 1.1), three deployment topologies
(7.7), strict-mode producer sizing from measured Alloy memory (7.6), a
buffered pipeline with a longer window (7.4), required plan-3 latency,
restart, replay and duplicate measurements (9), and consistent retry,
shutdown and deferred shared-writer wording throughout.

What changed in revision 7: findings from executing plans 1 and 2 and from
three investigations. Recorded the upstream exporter comparison (2),
output-schema and identity-width rationale (3, 4), timestamp compatibility
(5.4), portable point classification (5.5), consolidated limitations
(5.7), the derived shutdown bound (7.1), measured results and required
plan-3 scope (9.8, 9.9), and deferred partition granularity (10.2).

## 1. Problem

Collect OTLP logs and metrics from many producers and land them in an object
store as Parquet, with these properties:

- Small, predictable memory: memory owned by the exporter is bounded by two
  block budgets, one cache budget, one request budget and a fixed workspace.
  Every term is either enforced by accounting or bounded by construction with
  a documented expansion factor; the residual is exposed as a metric.
- Deterministic flushing: on aligned UTC window boundaries (default 15 s),
  earlier when the block byte budget is reached, and on shutdown.
- Two logical streams: a slowly changing `series` dimension (resource, scope
  and selected attributes, keyed by a content hash) and narrow `values`
  streams that reference it by `series_id`.
- Every values partition that contains rows for a series also contains at
  least one descriptor row for that series, written by the same worker.
- This exporter is stateless: no WAL, no local disk. The system guarantees
  at-least-once delivery into object storage; where the producer's
  acknowledgement happens is a deployment choice (section 7.7).
- Values files are sorted by configurable physical columns; series files are
  sorted by `series_id`.
- Layout is Hive-style and readable by Spark and DuckDB without extra tooling.
- Selected attributes can be denormalized into the values tables as typed
  columns.

### 1.1 Non-negotiable invariants

Every change to this component is checked against these four properties:

1. No persistent local state in this node. Its state is empty after restart.
2. Never more than ACTIVE plus FLUSHING. There is no third block and no
   spill.
3. The series cache is an optimization, never correctness state. Losing it
   (restart, eviction, or deleting it outright) can only increase the volume
   of the `series` dataset, never lose or corrupt data.
4. This node acknowledges a request to its upstream only after every row of
   its block is durable in object storage.

The recommended deployment puts a durable buffer upstream so producers are
acknowledged from its write-ahead log on the collector node (section 7.7).

Sorting of values tables is likewise an optimization: with
`sorting.enabled: false` every guarantee above and every schema stays the
same, only the row order inside values files changes. Series files are
always sorted by `series_id`.

The engine-independent boundary of `series-lake` (section 3.1) is part of
the design: a standalone binary of the shape "OTLP receiver, `series-lake`,
S3" must remain possible without rewriting the storage engine.

## 2. What the repository already provides

Verified on `main` at commit `5588c3e0d`:

- `crates/core-nodes/src/exporters/parquet_exporter`: object-store backends
  (file, S3, Azure), retry options, Arrow-to-Parquet plumbing, telemetry.
  It writes the OTAP star schema (batch-local ids), has no byte budget, no
  time partitions and no cross-batch dedup. It shows the mandatory input
  steps: convert the payload
  to `OtapArrowRecords` with `try_into_with_default` and call
  `decode_transport_optimized_ids` before touching `parent_id` columns
  (`mod.rs:335-349`). Conversion allocates a full OTAP representation before
  any exporter check can run (`pdata/src/payload.rs:632`), and
  `OtapPayload::num_bytes` is a logical size, not retained capacity
  (`payload.rs:293`).
- OTLP receiver `wait_for_result: true` holds the gRPC/HTTP request open until
  the immediate downstream node acks or nacks
  (`otap_grpc/otlp/server_new.rs:521`). The receiver awaits the channel send
  first, so a full pdata channel blocks the request until the receiver
  timeout; exhausted ack slots return `RESOURCE_EXHAUSTED`. The receiver
  maps `NackCause::Refused` to a different producer status than a permanent
  server failure (`server_new.rs:202`). Exporters ack via
  `EffectHandler::notify_ack` / `notify_nack`; `AckMsg` needs an `OtapPdata`,
  and the batch processor shows how to ack from a stored `Context` with
  `OtapPayload::empty(signal)` (`processors/batch_processor/mod.rs:1611`).
  Ack delivery awaits a bounded channel send
  (`engine/src/effect_handler.rs:302,367`). `Context` owns a frame stack,
  optional transport headers and optional authorization claims
  (`otap/src/pdata.rs:143-171`); `take_transport_headers` strips the headers
  (`pdata.rs:512`).
- `processor:durable_buffer`, behind feature gate `durable-buffer`, is
  experimental. It acks upstream immediately after the durable local write
  (`durable_buffer_processor/mod.rs:1106`), then forwards finalized bundles
  on timer ticks. Retryable downstream nacks defer the bundle with
  exponential backoff; permanent nacks reject it without retry. Each core
  has isolated WAL and segment storage; `retention_size_cap` is divided
  across the assigned pipeline cores (see the processor's README).
- `ExporterInbox::recv_when(accept_pdata)` (`engine/src/message.rs:874`) lets
  an exporter keep receiving control messages while refusing pdata. On
  shutdown the inbox latches `Shutdown`, force-drains buffered pdata to the
  exporter first, and releases `Shutdown` only when the backlog is empty
  (`message.rs:428-440`, `633`). `Message::PData` carries no force-drain
  marker; pdata returned from `recv_when(false)` is force-drained by
  definition. The topic exporter nacks pdata surfaced this way
  (`topic_exporter/mod.rs:455-479`) and keeps its blocked operation as
  explicit state outside the receive handler (`mod.rs:392`). The gRPC
  exporter nacks a parked batch at `Shutdown` so it is not silently dropped
  (`otlp_grpc_exporter/mod.rs:403-430`).
- Shutdown order (`engine/src/pipeline_ctrl.rs:541-615`): all engine timers
  are cancelled, receivers get `DrainIngress` (exporters do not), receivers
  wait for outstanding `wait_for_result` slots until the deadline, and only
  then processors and exporters receive `Shutdown`. Engine timers fire at
  `StartTimer` time plus duration and carry no boundary timestamp
  (`pipeline_ctrl.rs:95,141`). `clock::sleep_until` / `clock::now`
  (`engine/src/clock.rs:52-95`) honor the simulated clock in tests; the
  simulated clock is monotonic-only. Nodes run on a `LocalSet`
  (`engine/src/runtime_pipeline.rs:484`), so `spawn_local` and `!Send`
  futures are available to a local exporter (`engine/src/local/exporter.rs:54-91`).
- `pdata/src/otap/memory.rs` provides `record_batch_pinned_bytes`
  (deduplicated retained-buffer capacity, excluding struct and allocator
  overhead) and `record_batch_logical_bytes`.
- OTLP timestamps are `u64` and are cast to `i64` during conversion, with
  absent values becoming `0` (`pdata/src/encode/mod.rs:135,167,328`). Nested
  attribute values (arrays, kvlists) are CBOR-encoded into the `ser` column
  during conversion, which collapses NaN payloads (`encode/mod.rs:566`,
  `encode/cbor.rs:55-75`). Number points may carry no value independently of
  flags (`encode/mod.rs:656`). Conversion passes `Unspecified` temporality
  through (`encode/mod.rs:763`).
- `object_store` 0.13.2: `BufWriter` buffers up to `capacity` bytes, then
  streams multipart; `PutMultipartOptions` has no `PutMode`, so create-only
  uploads are not available for multipart objects. `WriteMultipart` holds
  one accumulating buffer plus in-flight parts, and accepts a whole supplied
  buffer per write call.
- `policies.resources.core_allocation` selects cores per pipeline; the number
  of exporter instances per process is the number of pipeline instances that
  contain the node.
- Benchmarks: `criterion` in the workspace, an S3-compatible endpoint example
  (`configs/trafficgen-parquet-local-s3.yaml`), a nightly backpressure
  scenario with `wait_for_result` (`docs/benchmarks.md`). No `proptest`, no
  failpoint crate, no soak or chaos suite.
- `temporal_reaggregation_processor/identity.rs`: an attribute hashing scheme
  that informs, but does not match, the canonical encoding below. The
  durable buffer processor has an LRU of segment summaries
  (`durable_buffer_processor/mod.rs:420`); no reusable cache with an entry
  budget and the semantics needed here exists.

This exporter exists separately from `exporter:parquet` because durable
request decisions, bounded blocks and predictable visibility need a
different ownership and flush model. Three verified properties of the
upstream exporter matter:

- It sends no acknowledgement at all. `mod.rs` drops the context with
  `// Note: context is not used` and carries a TODO for retryable and
  non-retryable nacks. With receiver `wait_for_result: false`, a producer's
  OK means only that bytes were accepted into memory; with
  `wait_for_result: true`, it waits until timeout (section 7.7).
- Flush thresholds are per payload type and per partition, and files close
  separately; there is no atomic file set. The current `writer.rs` delays a
  parent file's close until its child files have closed, including resource,
  scope and log attributes for logs. Partial visibility can expose attribute
  files before log files; log-body files wait while their attributes remain
  buffered (`test_doesnt_autoflush_parent_batch_if_children_not_flushed`).
- In `writer.rs::should_flush`, the row and age conditions are joined by
  `else if`. When `target_rows_per_file` is set, as it is by default to
  100 million rows in `config.rs`, `flush_when_older_than` is never evaluated
  at all. This looks like a defect worth reporting upstream.

The [in-flight upstream patch][upstream-parquet-patch] adds
content-hash ids, dimension-table semantics for the attribute tables and
Hive time partitioning to `exporter:parquet`. The two designs are converging
on the same idea from different directions. Two differences matter for
future convergence:
identity width (64-bit FNV-1a there, 128-bit XXH3 here; section 4), and
re-emission policy. The patch clears its dedup cache on a UTC day change so
each day partition is self-contained, at the cost of a midnight burst.
This design re-emits per storage partition, spreading the cost across
partitions and bounding its cache by entries and their byte charge
(section 6.6).

The existing `parquet_exporter` is not modified by this design. The patch
and any fix to `should_flush` are separate upstream work.

[upstream-parquet-patch]: ../../../otap-dataflow.patch

## 3. Architecture

Two new crates under `rust/otap-dataflow/crates/`, keeping the diff against
upstream additive: new files, one feature flag, one component-inventory entry.

The exporter defines its own output schema instead of passing OTAP batches
through. The Parquet writer tolerates a column switching between a plain
type and a dictionary at the top level, but not inside a struct. The
OTLP-to-OTAP encoder chooses encodings adaptively per batch, so a struct
child switching encoding mid-file breaks the write. The upstream patch
normalizes struct children for this reason; constructing the schemas of
section 5 removes that class of failure here.

### 3.1 `series-lake` (engine-independent)

Depends on `otel-arrow-dfe-pdata` (schemas, `OtapArrowRecords`, memory
helpers, CBOR decoding of nested values), `arrow`, `parquet`,
`object_store`, `xxhash-rust`. It must not depend on the engine, `OtapPdata`
or `Context`. Modules:

- `canonical`: canonical encoding v1 and `SeriesId` (xxh3_128), golden
  vectors. Decodes the OTAP `ser` (CBOR) column into a value tree for nested
  values, with a nesting depth limit.
- `extract`: `OtapArrowRecords` to per-batch descriptors plus values with
  `series_id` and typed denormalized columns, produced in slices no larger
  than `run_target_bytes`. Validates attribute lists, temporality and
  histograms; enforces the extracted-output and row-size budgets while
  building.
- `cache`: LRU keyed by `SeriesId`, value = last committed partition, bounded
  by entries.
- `buffer`: one `SortedTableBuffer` (building batch plus sealed sorted runs)
  used for every output table, and `Block` (byte accounting including runs,
  descriptors, pending-series entries and tokens; request tokens).
- `sink`: k-way merge of runs into Parquet files via `object_store`, paths,
  file naming, per-table write order, writer memory limit, cancellation.
- `clock`: `WallClock` trait (system and test implementations) and the window
  boundary arithmetic. Sleeping is the caller's concern.
- `config`: format configuration types (identity, denormalize, sort, layout,
  budgets).

### 3.2 `exporter:series_parquet`

A local exporter in `core-nodes` behind feature `series_parquet`, URN
`urn:otel:exporter:series_parquet`, metric set `exporter.series_parquet`.
It owns the boundary state and sleeps via the engine's `clock::sleep_until`,
the ack tokens, the ACTIVE/FLUSHING pair, the pending request slot, the
notification queue, the state machine of sections 6 and 7 and telemetry, and
delegates all data work to `series-lake`.

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

The in-flight upstream patch derives ids with FNV-1a over 64 bits. An
identity collision merges two distinct label sets into one descriptor row,
so values are silently joined to the wrong labels. A 64-bit identity reaches
the birthday range at a few billion distinct sets, and FNV additionally has
weak avalanche on structured keys. This design uses 128-bit XXH3 with pinned
golden vectors to give the identity adequate width and keep its encoding and
hash stable across implementations.

`identity_bytes` is built from typed values. Every value is
`tag:u8 ++ len:u32_be ++ payload`:

```text
tag  type     payload
0x01 string   UTF-8 bytes as received (no normalization)
0x02 bytes    raw bytes
0x03 int64    8 bytes, two's complement, big-endian
0x04 double   8 bytes, IEEE 754 binary64, big-endian; any NaN is normalized
              to 0x7FF8000000000000 and -0.0 is normalized to +0.0
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
kvlist resource attributes                 # all of them, including the one
                                           # selected as producer_id_attribute
string resource schema_url
string scope name
string scope version
string scope schema_url
kvlist scope attributes
# metrics only:
string metric name
string metric unit
string metric type            # gauge | sum | histogram | exp_histogram | summary
string temporality            # delta | cumulative | "" (gauge only)
bool   is_monotonic           # false for non-sum metrics
kvlist identity attributes    # metrics: data point attributes
                              # logs: attributes named in logs.series_attributes
```

Rules:

- Keys are sorted by raw UTF-8 bytes (`memcmp`, no locale, no case folding),
  recursively inside nested kvlists.
- Nested values come from the OTAP `ser` column and are decoded from CBOR
  before encoding. Nesting deeper than `max_nesting_depth` (default 32)
  makes the request invalid.
- A duplicate key inside any attribute list of the request (identity or not)
  makes the request invalid: permanent nack with `NackCause::Refused`,
  `nacks{reason=invalid}`.
- A sum or histogram whose temporality is `Unspecified` is invalid (same
  outcome). Gauges encode temporality as the empty string.
- Missing string fields are encoded as empty strings; unset attribute values
  are encoded as `null`. `42` (int64) and `"42"` (string) are different
  series.
- The resource attribute chosen as `producer_id_attribute` stays in the
  identity like every other resource attribute; the `producer_id` column of
  section 5.1 is only a projection of it and is not hashed separately.
- Nothing else is part of the identity: timestamps, values, exemplars, body,
  severity, trace and span ids, flags, dropped counts, metric description,
  log attributes outside the allow-list.
- The encoder never panics on any input; this is a fuzz target.

Every semantic field stored in a `series` row is either part of the identity
or explicitly listed as non-identity metadata. Non-identity metadata:
`description` (metrics) and `emitted_at`. Consumers must not expect them to
be constant per `series_id`.

The encoding is specified in `crates/series-lake/docs/FORMAT.md`, the
implementation-independent format document (canonical identity, dataset
schemas, partition layout, file and delivery semantics, compatibility rules;
no Rust, Dataflow or buffer internals), with golden vectors in
`crates/series-lake/tests/golden/*.json` (input descriptor, hex canonical
bytes, hex hash). Float inputs in vectors are given as bit patterns. Vectors
must cover: empty string, missing field, int 0 and -0.0, INT64_MIN and
INT64_MAX, `42` vs `"42"`, several NaN bit patterns hashing equal, +Inf,
-Inf, non-ASCII UTF-8 including supplementary planes, embedded NUL, bytes,
nested array, nested kvlist, key ordering with common prefixes, two
descriptors differing only in the producer attribute. At least one vector
set is generated independently in Python from the spec text, and every
vector is also run through OTLP-to-OTAP conversion so the converted
representation hashes identically.

## 5. Storage format

### 5.1 Datasets

Four datasets, one Parquet schema each:

```text
signal=logs/dataset=series
signal=logs/dataset=values
signal=metrics/dataset=series
signal=metrics/dataset=values
```

Later additions (`dataset=exp_histogram`, `dataset=summary`,
`signal=traces/...`) add datasets without changing existing schemas.

`series_id FIXED_LEN_BYTE_ARRAY(16)` (required) is present in every
dataset. `producer_id STRING` (required, `""` when absent) is present in
every values dataset and absent from `series`, because one `series_id` can
legitimately be produced by several producers and the canonical view of
section 5.5 keeps one row per id.

`producer_id` is the value of the resource attribute named by
`producer_id_attribute`. Header-based producer ids are a future extension.
The README states the semantic requirement: it should be stable across
producer restarts and unique among concurrently running producers (the role
Thanos external labels play), because future compaction, deduplication and
replay extensions rely on it.

`series` (both signals), always sorted by `series_id`:

```text
identity_bytes      BINARY               # canonical bytes hashed into series_id
emitted_at          TIMESTAMP(us, UTC)   # time the block was sealed
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
flags                    INT32
attrs                    MAP<STRING, STRING>  # log attributes outside the allow-list
# denormalized columns (section 5.2)
```

`metrics/values` (number and histogram points share one schema):

```text
metric_name           STRING               # required, dictionary encoded
time                  TIMESTAMP(us, UTC) null
time_unix_nano        INT64 null
start_time            TIMESTAMP(us, UTC) null
start_time_unix_nano  INT64 null
flags                 INT32
value_int             INT64  null          # set when the point carries as_int
value_double          DOUBLE null          # set when the point carries as_double
count                 INT64 null
sum                   DOUBLE null
min                   DOUBLE null
max                   DOUBLE null
bucket_counts         LIST<INT64> null     # empty for histogram without distribution
explicit_bounds       LIST<DOUBLE> null    # empty for histogram without distribution
# denormalized columns
```

`value_int` and `value_double` are null for histogram points; `count`,
`sum`, `min`, `max`, `bucket_counts` and `explicit_bounds` are null for
number points. For a number point carrying no value, both value columns
are null; flags are stored as received and never inferred. Histogram
optionality is unchanged: `sum`, `min` and `max` may be absent, and a
histogram without a distribution has empty lists, not null lists. The point
kind must come from `metric_type` in
the series descriptor, obtained through the same join the reader already
performs (section 5.5), never from null-ness or `count`. Every other column
is unchanged.

With separate number and histogram datasets, a mixed metrics stream
(gauges, counters and histograms from the same sources) costs two PUTs per
window even when every descriptor is already cached. The goal is one PUT
per window per signal in steady state. An all-null column inside a row
group costs only definition levels plus the column-chunk metadata, far
less than a second file's footer, metadata and request. The tradeoff is
that min/max statistics on `value_double` become less selective when half
the rows are null. Logs and metrics in one pipeline still cost one file
per signal because their schemas differ.

Counts use signed 64-bit integers because Spark maps Parquet `UINT64` to
`DECIMAL(20,0)` while DuckDB maps it to `UBIGINT`; OTLP counts never
approach the signed limit. A count above `i64::MAX` is invalid.

Histogram validation: either both lists are empty, or
`bucket_counts.len == explicit_bounds.len + 1`. Anything else nacks the
request as invalid. Bound monotonicity and bucket totals are not checked;
such malformed distributions are stored as received, and section 8 uses the
same narrow definition of "inconsistent histogram".

Timestamps: the exporter sees timestamps as `i64` nanoseconds after
conversion; absent and zero are already conflated upstream as `0`. Values
`>= 1` are stored raw in the `INT64` column and as `TIMESTAMP(MICROS,
isAdjustedToUTC=true)` after integer division by 1000. `0` stores null in
both columns. Negative values (a `u64` above `i64::MAX` wrapped by the
conversion cast) store null in both columns and count
`timestamp.out_of_range`. This applies to every timestamp column. Supported
readers: DuckDB 1.1 or later (tested), Spark 3.5 or later (documented,
manual check).

Attribute maps: `MAP<STRING, STRING>` with non-null keys and nullable values.
The format is intentionally lossy for attribute value types; identity
hashing, `identity_bytes` and denormalized columns keep types. One recursive
rendering `render_v1(value) -> JSON value` is defined: string to JSON
string; int to JSON number; finite double to JSON number (shortest
round-trip); non-finite double to the JSON strings `"NaN"`, `"inf"`,
`"-inf"`; bool to JSON bool; bytes to a JSON string of lowercase hex; unset
to JSON null; array to JSON array; kvlist to JSON object with keys sorted.
Two entry points use it:

- Attribute map value: for a top-level string the raw string; for a
  top-level unset the SQL null; for any other top-level value the compact
  JSON serialization of `render_v1` (so an int is `42`, bytes `ab12` are
  the JSON string `"ab12"`, a double NaN is `"NaN"`).
- Log body: for a string body the raw string; for an unset body the SQL
  null; otherwise the compact JSON serialization of `render_v1`.

An empty attribute list is an empty map, never null.

Unsupported inputs in v1 and their policy:

- Exemplars (children of supported points) are always dropped, the parent
  point is kept, `dropped_unsupported{kind=exemplar}` counts exemplars.
- Exponential histogram and summary points: `unsupported: reject` (default)
  nacks the whole request with `NackCause::Refused`,
  `nacks{reason=unsupported}`; `unsupported: drop` drops those points,
  counts them and keeps the rest. Rejection is atomic per request. A request
  that yields zero output rows after drops is acked immediately; a request
  with any output rows is acked only when its block commits.
- Traces: a traces request is nacked with `NackCause::Refused`,
  `nacks{reason=unsupported}`, regardless of the policy.

The README lists "exponential histograms and summaries are rejected by
default; traces are rejected" as v1 limitations next to the configuration
knob.

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
  type is rendered as the attribute-map string when `type: string`,
  otherwise stored as null and counted
  (`denormalize.type_mismatch{column}`). Doubles are stored as received
  (NaN payloads are not canonicalized in values columns).
- A denormalized column appears in `series` only if its path is part of the
  identity (resource, scope, or an allow-listed log attribute / data point
  attribute). Otherwise it appears only in the values datasets.

Schema contract per dataset: within one `base_uri`, changes are additive
only (new denormalized columns, new nullable intrinsic columns). Changing the
type or path of an existing column, or reusing a column name for a different
path, requires a new `base_uri`. Each file carries `schema_fingerprint`
(xxh3_64 of that dataset's ordered column names and types; the series and
values datasets of a signal have different fingerprints) in its metadata.
Readers use union-by-name (`union_by_name = true` in DuckDB,
`mergeSchema` in Spark) so additive changes read as nulls; the README shows
both recipes and how to detect incompatible mixes.

### 5.3 Layout and naming

```text
<base>/v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/
    part-20260921T031500Z-<writer_id>-<boot_id>-<seq>.parquet
<base>/v=1/signal=metrics/dataset=values/date=2026-09-21/hour=03/
    part-20260921T031500Z-<writer_id>-<boot_id>-<seq>.parquet
```

- `date` and `hour` (two digits, zero-padded) come from the block's
  `window_start` (ingest time), never from event timestamps.
- `writer_id` from config; `boot_id` is a UUIDv4 generated at exporter start;
  `seq` is a per-worker monotonic counter, zero-padded to 8 digits. Names are
  frozen when the block is sealed and reused verbatim across retries.
  Retries overwrite the same names with the same content; `boot_id` makes
  collisions with other blocks or processes impossible in practice. No
  conditional-put semantics are used.
- No manifest. Within a block, for each signal the `series` file is written
  first, then its single `values` file. Completed objects become visible
  atomically per object (object store semantics).
- Visibility guarantees, stated narrowly: readers may observe a block
  partially (some files present) while it is being written or after a
  permanent failure; rows of a nacked request may therefore exist in storage
  and appear again after the upstream retries (buffer replay or producer
  retry, depending on topology); there is no snapshot consistency across
  files. Referential coverage (section 5.5) holds for every visible values
  file because its descriptors were completed earlier.
- No files are written for a dataset with zero rows; an empty window writes
  nothing.
- Interrupted multipart uploads are aborted on a best-effort basis; leftovers
  after a crash are covered by a bucket lifecycle rule for incomplete
  uploads.

### 5.4 Parquet options

ZSTD, target row group about 64 MiB, statistics enabled, dictionary encoding
for strings. Key/value metadata: `format_version=1`,
`series_hash=xxh3_128/canonical_v1`, `schema_fingerprint`,
`sort_key` (comma-separated `column:asc|desc:nulls_first|nulls_last`, or
`none` when sorting is disabled; series files always carry
`series_id:asc:nulls_last`), `writer_id`, `boot_id`, `seq`, `window_start`,
`window_end` (`window_start + interval`, also for blocks rotated
mid-window), `row_count`, `min_time_unix_nano`, `max_time_unix_nano`
(values datasets). Together `(signal, dataset, partition, format_version,
schema_fingerprint, sort_key)` defines a compaction scope: files in the same
scope with `sort_key != none` can later be merged by a k-way merge without
re-sorting; files with `sort_key=none` must be re-sorted; files in different
scopes are never merged together.

`time` is a microsecond timestamp, while `time_unix_nano` keeps the raw
nanoseconds in a plain integer column. Iceberg v2 has no nanosecond timestamp
type. The upstream patch records the failure mode: catalogs registering
nanosecond files declare the column as microseconds but copy the Parquet
footer min and max verbatim into manifest bounds. The bounds are then wrong
by a factor of a thousand; a bounded time predicate prunes every file and
silently returns no rows. Writing microseconds makes every layer agree by
construction, while the integer column retains the original precision.

### 5.5 Reading the data

Because descriptors repeat (eviction, new partition, restart, several
workers), a join on `series_id` must go through a canonical view:

```sql
SELECT * FROM read_parquet('<base>/v=1/signal=logs/dataset=series/**/*.parquet',
                           hive_partitioning = true, union_by_name = true,
                           filename = true)
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

For metrics, use the same canonical view over
`signal=metrics/dataset=series` and join the single
`signal=metrics/dataset=values` dataset on `series_id`. A reader must
determine a point's kind from the descriptor's `metric_type` through this
join, never from which columns are null and never from `count`:

```sql
WITH series AS (
  SELECT * FROM read_parquet(
    '<base>/v=1/signal=metrics/dataset=series/**/*.parquet',
    hive_partitioning = true, union_by_name = true, filename = true)
  QUALIFY row_number() OVER
    (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1
)
SELECT v.*, s.metric_type
FROM read_parquet('<base>/v=1/signal=metrics/dataset=values/**/*.parquet',
                  hive_partitioning = true, union_by_name = true) AS v
JOIN series AS s USING (series_id)
```

The Spark recipe uses the same paths, `mergeSchema` and canonical
descriptor selection; it needs no union of separate point-kind datasets.

At the Parquet level a number row's list columns are null while a
bucket-less histogram's are empty. DuckDB preserves that difference, but
ClickHouse has no nullable `Array` and renders a null list as an empty one.
List null-ness is therefore not portable; the descriptor join answers the
same way in both readers.

The measured test consequence is that a distribution-less histogram's empty
lists are observable only when the same request also carries a shape that
fills those columns. The OTAP transport drops a column whose every entry
in a request is the type default. A test for empty lists must therefore
include a histogram with buckets and bounds in that same request.

### 5.6 Request cost model

Assumptions: S3 Standard, PUT at 0.005 USD per 1000 requests, a 30-day
month, and one writer is one pipeline worker (section 6.8). One signal
writes one values file per window; the file stays below
`upload.part_bytes`, so one file is one PUT. With a stable series set,
`series` is written once per hour partition.

The files/hour column counts values files; monthly PUTs include `series`.
The 100-writer and 1000-writer columns multiply the stated USD/writer
figures by 100 and 1000.

| Window interval | Values files/hour/writer | PUT/month/writer | USD/month/writer | USD/month, 100 writers | USD/month, 1000 writers |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 s | 720 | 519120 | 2.60 | 260.00 | 2600.00 |
| 15 s | 240 | 173520 | 0.87 | 87.00 | 870.00 |
| 20 s | 180 | 130320 | 0.65 | 65.00 | 650.00 |
| 30 s | 120 | 87120 | 0.44 | 44.00 | 440.00 |
| 60 s | 60 | 43920 | 0.22 | 22.00 | 220.00 |
| 120 s | 30 | 22320 | 0.11 | 11.00 | 110.00 |

The `series` contribution is 720 PUT/month/writer regardless of the
window. Before the merge in section 5.1, the values figures for a mixed
number and histogram stream were exactly double; the `series` contribution
was unchanged.

Three things change the picture:

- Every window containing a new series adds one PUT; churn dominates at
  short windows.
- Logs plus metrics in one pipeline roughly doubles the total, because
  each signal writes its own file set.
- A file above `part_bytes` becomes a multipart upload: one Create, one
  UploadPart per part and one Complete.

Request cost has a fixed part (writers times windows), which shrinks by
using fewer, larger workers, and a variable part (total bytes divided by
`part_bytes`), which shrinks by raising `part_bytes` at the cost of buffer
memory. Below about 15 s, files become too small for efficient row groups
and multiply the reader's and compactor's work.

### 5.7 Limitations

Execution of plans 1 and 2 established these limits of the implementation:

- A histogram `sum` of zero is indistinguishable from an absent sum after an
  OTAP round trip when every sum in the request is the type default: the
  transport drops that column. The same restriction applies to optional
  metrics columns under that condition.
- Only top-level OTLP protobuf framing is validated before conversion.
  Nested messages are not validated; the shared lazy byte views can surface
  corrupt nested content as missing fields instead of refusing it.
- Multipart parts begun in the finalizing phase are not aborted. Once
  Parquet finalization starts, the buffered writer cannot safely abort its
  in-flight upload; those parts are left to a bucket lifecycle rule for
  incomplete multipart uploads, including after a crash.
- Sealing the values tables costs a transient of up to
  `(V + 1) * run_target_bytes` for V buffered values runs: finalizing a values
  dataset adds a run for what is still buffered.
- A table's merge keys remain resident for the whole merge. A wide sort key
  can approach a second copy of the table's payload. Merge chunk sizing is
  based on average row width, so unusually wide rows can overshoot
  `merge_chunk_bytes`.
- Per-worker caches allow the same series to be described by several
  writers. Readers tolerate this through the latest-descriptor join of
  section 5.5, but it inflates `series` volume.

The memory allowances of section 6.6 are design reservations. Plan 3 must
validate them against resident memory, including these transients (9.9).

## 6. Buffering, memory and flush

### 6.1 Worker state

```text
cache:     LRU<SeriesId, PartitionId>       bounded by max_entries
active:    Block { window_start, partition,
                   tables: per-dataset SortedTableBuffer,
                   pending_series: HashSet<SeriesId>, bytes,
                   requests: Vec<AckToken> }
flushing:  Option<FlushHandle>              # task owns the sealed Block
pending:   Option<Extracted>                # one request extracted, waiting
                                            # for rotation (section 6.2)
notify:    VecDeque<Notification>           # acks/nacks not yet delivered
rotation_requested: bool
```

`AckToken` holds the request's `Context` with transport headers and
authorization claims removed (`take_transport_headers` and the identity
counterpart) and its signal type. The token is charged its measured size
(frame stack length times frame size plus a fixed struct overhead) against
`block.bytes`. A block holds at most `max_requests_per_block` tokens
(default 4096); reaching that limit triggers rotation like the byte limit.
On ack or nack the exporter rebuilds
`OtapPdata::new(ctx, OtapPayload::empty(signal))`.

`block.bytes` counts: pinned bytes of sealed runs and building batches
(`record_batch_pinned_bytes`, deduplicated), descriptor rows, one fixed
`pending_series_entry_bytes` (default 64) per entry in `pending_series`, and
token sizes. Sealed runs live inside the block and inside
`max_block_bytes`.

### 6.2 Admission and ingest of one request

Admission runs inline in the node loop. Its work is bounded by
`max_request_bytes`, `max_extracted_bytes` and `max_nesting_depth`, so the
loop is blocked for at most the work of one bounded request; there is no
finer-grained interleaving in v1 (section 6.7 states the responsiveness
bound).

1. If `pending` is occupied, the loop does not receive pdata at all
   (`accept = false`), so this step never sees two requests at once.
2. Reject if the payload's logical size (`OtapPayload::num_bytes`) exceeds
   `max_request_bytes`: `NackCause::Refused`, `nacks{reason=too_large}`.
   This check precedes conversion. A payload whose logical size is
   unavailable is rejected the same way.
3. Convert to `OtapArrowRecords` (`try_into_with_default`) and
   `decode_transport_optimized_ids`. Conversion failure: `Refused`. The
   converted input is the conversion workspace; it is bounded by
   construction to `conversion_expansion * max_request_bytes` (documented
   factor, default 4, validated by the benchmark suite) and is released as
   soon as extraction finishes.
4. `extract` produces, without touching the block: descriptors for unique
   series, values batches per dataset in slices of at most
   `run_target_bytes`, and per-row `series_id`. It stops and returns
   `too_large` as soon as the pinned size of its output exceeds
   `max_extracted_bytes` (default `2 * max_request_bytes`), and as soon as a
   single row (body, attribute map or descriptor) exceeds `max_row_bytes`
   (default 1 MiB, validated at startup to be at most
   `run_target_bytes / 4`). Validation failures: `Refused`. Unsupported
   inputs per section 5.1. The wall clock is read here
   (`admission_time`); it is the request's linearization point for window
   assignment.
5. Reserve: `needed = pinned bytes of extracted slices + descriptor bytes
   for series not already in pending_series and not committed for
   active.partition + pending_series entries + token size`. If
   `needed > max_block_bytes`: `Refused` (`too_large`), even into an empty
   block. If `active.bytes + needed > max_block_bytes` or
   `active.requests.len() == max_requests_per_block`, store the extracted
   output, reservation and token in `pending`, set
   `rotation_requested = true` and return to the loop; the loop resumes this
   request at step 6 after the rotation completes, before receiving any new
   pdata. If `boundary(admission_time) > active.window_start`, the same
   applies (the request belongs to the next window).
6. Mutate `active`:
   - for each unique series: if `cache.get(series_id) !=
     Some(active.partition)` and `series_id` not in `active.pending_series`,
     append the descriptor to `active.tables.series` and insert into
     `pending_series`; touch the cache entry (insert if absent, value
     unchanged), evicting from the tail while over `max_entries`;
   - append each values slice to its `SortedTableBuffer`; a building batch
     that reaches `run_target_bytes` is sorted (permutation plus `take`) and
     sealed as a run, the unsorted batch dropped. A run is at most
     `run_target_bytes + one slice`, so at most `2 * run_target_bytes`.
     With `sorting.enabled: false` values runs are sealed unsorted and the
     flush concatenates them in arrival order; series runs are always
     sorted;
   - push the `AckToken`.
   A request that produced zero rows is acked (queued to `notify`)
   immediately and never enters a block.

### 6.3 Rotation

Triggers: window boundary, `active.bytes` or `active.requests` at their
limit, `pending` needing space, shutdown.

- If `flushing` is empty: seal all building batches, hand `active` to a new
  flush task (`spawn_local`) that takes ownership of the block, store its
  handle in `flushing`, create a new empty `active` with `window_start`
  equal to the effective current boundary (section 6.4), clear
  `rotation_requested`.
- If `flushing` is occupied: set `rotation_requested = true`; the loop keeps
  handling control messages and notifications with `accept = false`, and
  performs the rotation right after the flush task reports its result. A
  block therefore belongs to exactly one aligned window; byte-triggered
  rotation inside a window creates a second block for the same window with
  the next `seq`.

### 6.4 Window clock

The exporter does not use engine timers. Boundary arithmetic lives in
`series-lake` (`WallClock` trait, system time in production, injectable in
tests); sleeping uses the engine's `clock::sleep_until`, so simulated
monotonic time in tests drives the loop and the injected `WallClock`
supplies wall time.

```text
boundary(t)        = floor(t / interval) * interval
effective_boundary = max(boundary(wall_now), last_boundary)
next_boundary      = effective_boundary + interval
```

`last_boundary` starts at `boundary(start_time)`. The loop keeps one armed
sleep until `next_boundary`; when it fires, `rotation_requested` is set,
`last_boundary` becomes `effective_boundary` (which coalesces missed
boundaries after a long flush or a forward clock step) and the sleep is
re-armed immediately for the new `next_boundary`, independently of whether
the rotation can proceed now. If the wall clock on wake is still before the
expected boundary (backward step) the sleep is re-armed without requesting
rotation. Boundaries never move backwards, so a window is never reopened;
every new block's `window_start` is `effective_boundary` at rotation time.

Forward clock jumps while idle are detected at the next wake or the next
admission (step 4 of section 6.2 compares `boundary(admission_time)` with
`active.window_start`); the detection latency is at most one interval.

### 6.5 Flush and commit

The flush task owns the sealed block and the I/O; the node loop keeps
polling the inbox, the boundary sleep, the notification queue and the task's
completion.

1. Per dataset, in order `series`, then values datasets (section 5.3): k-way
   merge over sealed runs (or concatenation when unsorted), materialized in
   chunks of at most `merge_chunk_bytes`, written through `AsyncArrowWriter`
   into `object_store::BufWriter` with `capacity = upload.part_bytes` and
   `max_concurrency = upload.concurrency`. Each `write` call passes at most
   one encoded chunk, so the writer's accumulating buffer never exceeds
   `part_bytes + one encoded chunk`. After every chunk, if
   `writer.memory_size() >= parquet.writer_limit_bytes` the current row
   group is closed. Datasets are written sequentially, one open writer at a
   time.
2. Every `await` in the task is raced against the cancellation token; on
   cancellation the task aborts the current `BufWriter` only if it is still
   in a writable state, bounded by `upload.abort_timeout` (default 5 s), and
   returns `Cancelled`.
3. All files uploaded: the task returns `Committed`. The node loop then, in
   this order: `cache.insert(series_id, block.partition)` for every id in
   `block.pending_series` (the flushed block's own partition); pushes an ack
   for every token to `notify`; drops the block.
4. Any error before commit: abort the multipart upload best-effort, back off,
   retry the whole block with the same file names, until the absolute
   `flush_retry_deadline` taken when the block is sealed; the deadline also
   cancels an in-flight attempt (same mechanism as step 2). An upload whose
   response was lost is retried like a failure; overwriting an identical
   object is safe. Past the deadline the task returns `Failed`; the node
   loop pushes a retryable nack for every token, `nacks{reason=storage}`,
   and drops the block. The cache is not updated, so descriptors are
   re-emitted with the next values.
5. Notifications are delivered by the node loop from `notify`, at most
   `notify_batch` (default 64) per loop iteration, each send raced against
   the inbox select so a full completion channel never stops control
   handling. A send that fails because the channel is closed is counted
   (`notify.failures`) and dropped. Undelivered notifications are the
   "committed but unnotified" state; their block memory is already released,
   and only tokens remain, bounded by `2 * max_requests_per_block`. A
   committed block is never re-exported.

`retry.*` from the shared object-store configuration applies to individual
object-store operations inside one attempt; `flush_retry_deadline` is the
single authority for how long a block is retried; `upload.abort_timeout`
bounds cleanup only.

### 6.6 Memory budget

Exporter-owned memory per worker:

```text
enforced by accounting:
    max_block_bytes * 2                    # active + flushing, including runs,
                                           # descriptors, pending_series, tokens
  + max_extracted_bytes                    # the pending/extraction slot
  + cache.max_entries * cache_entry_bytes  # 16-byte key + partition + overhead
  + 2 * max_requests_per_block * token_bytes   # committed-but-unnotified tokens

bounded by construction (documented expansion factors):
  + conversion_expansion * max_request_bytes   # converted input during extract
  + run_target_bytes * 2                       # sort permutation + sorted output
  + merge_chunk_bytes * 2                      # merge output + encoded chunk
  + parquet.writer_limit_bytes + encoder transient (documented per parquet
    version, validated by the benchmark suite)
  + upload.part_bytes * (upload.concurrency + 1) + one encoded chunk
```

Not counted, and documented as such: the incoming `OtapPdata` before step 2
(bounded by upstream request limits and the pdata channel capacity),
struct and allocator overhead, the upstream durable buffer's own memory,
and the same formula again for every other worker.
`memory.unaccounted_rss_bytes` (section 7.5) exposes the residual. The README
shows the aggregate and accounts for other nodes separately.

### 6.7 Responsiveness bound

Control messages, the boundary sleep and the flush result are observed
between requests. The worst-case latency is the admission of one request of
`max_request_bytes` (conversion, hashing, slicing, one run sort) plus one
`notify_batch`. The flush task runs on the same thread; its CPU work is
interleaved at chunk granularity (`merge_chunk_bytes`) through the awaits on
the writer and the upload. A test measures shutdown latency under a
saturated inbox and asserts it stays below a documented bound.

### 6.8 Workers, sources and threading

One writer is one pipeline worker, not one source. The OTLP receiver
accepts from arbitrarily many producers; `producer_id` comes from a
per-record resource attribute (section 5.1), not from the connection.
Hundreds or thousands of sources share one writer and one file set. The
number of sources does not enter the cost model; it affects series
cardinality and bytes per window.

Workers are shared-nothing: each runs on its own `LocalSet` with `!Send`
nodes and `Rc` state, its own writer id and boot id, its own series cache
and its own budgets. Exporter-owned retained request memory is workers times
(ACTIVE + FLUSHING + one pending request + completion tokens); the cache
and workspace terms in section 6.6 also apply per worker.

Inside a worker, execution is single-threaded. The exporter's admission
state needs no mutex and no atomic on the hot path. The flush runs as a
`spawn_local` task interleaved with admission on the same thread, which
makes the ACTIVE/FLUSHING overlap possible. Parquet encoding and ZSTD run
inline in that task, so encoding sets the per-core throughput ceiling,
not I/O concurrency. Yield points between merge chunks keep control
messages and cancellation responsive; measuring this ceiling is plan 3's
job. `upload.concurrency` bounds in-flight multipart parts: request
concurrency, not threads.

These execution properties follow from `engine/src/runtime_pipeline.rs`
(current-thread runtime and `LocalSet`),
`core-nodes/src/exporters/series_parquet/worker.rs` and `flush.rs`
(per-worker state and `spawn_local`), and `series-lake/src/sink.rs`
(inline encoding, chunk yields and multipart concurrency).

Consequences:

- The same series arriving at two workers is described by both. Descriptor
  rows are duplicated between writers, harmless for readers taking the
  latest descriptor per series id (section 5.5), but inflating `series`
  volume.
- There is no work stealing. A hot producer pinned to one worker saturates
  that core while others idle.

## 7. Dataflow integration

In v1, the shutdown deadline is supplied by the admin shutdown API timeout; a
pipeline configuration field is future work.

### 7.1 Node loop

```text
loop:
  accept = pending.is_none() && !rotation_requested && !shutting_down
  select (biased):
    boundary sleep elapsed        -> mark rotation_requested, re-arm (6.4)
    flush task finished           -> commit or fail handling (6.5);
                                     then if rotation_requested: rotate (6.3);
                                     then if pending: resume it (6.2 step 6)
    notify non-empty              -> deliver up to notify_batch (6.5 step 5)
    rotation_requested && flushing.is_none()
                                  -> rotate (6.3); resume pending
    inbox.recv_when(accept)       -> PData with accept:  admission (6.2)
                                     PData without accept: retryable nack,
                                       reason=shutdown, no conversion
                                     Control: below
```

Pdata returned while `accept` is false can only be force-drained shutdown
pdata (section 2). Pdata admitted with `accept` true before shutdown was
observable is handled normally and flushed by the shutdown path.

Control messages:

- `Shutdown { deadline }`: set `shutting_down`; if `pending` is occupied,
  nack it with `nacks{reason=shutdown}` (its upstream awaits the result);
  if `flushing` is busy, wait for it within the deadline while still
  delivering notifications; then rotate and flush `active` within the
  remaining deadline; at the deadline cancel the flush task (6.5 step 2);
  anything uncommitted is nacked with `nacks{reason=shutdown}`; deliver the
  remaining notifications until the deadline; return. The exporter's `start`
  future must be cancellation-safe: dropping it cancels the flush task's
  token.
- `CollectTelemetry`: report metrics.
- Others: ignored. `DrainIngress` is never delivered to exporters, so the
  exporter cannot flush early when draining begins.

Receivers drain before the exporter is handed `Shutdown`. In the strict
topology an OTLP receiver holds its responses until this node decides, and
this node normally waits for the window boundary before sealing. With a
long window both sides wait until the engine's drain timeout. With a durable
buffer upstream, producer requests finish at the WAL write; bundles not
committed by shutdown remain on disk for replay after restart.

The derived worst case for an orderly shutdown is:

```text
window.interval + 2 * (flush_retry_deadline + upload.abort_timeout) + notification margin
```

A block's absolute retry deadline is taken when it is sealed. The second
block cannot seal until the first releases the flush slot, including abort
cleanup, so the two retry windows are sequential. On the defaults the bound
is `15 s + 2 * (60 s + 5 s) = 145 s`, plus notification margin. The two
retry windows and abort allowances account for 130 s of the 145 s; twice
the retry deadline alone is 120 s. Lowering `flush_retry_deadline` therefore
buys most of the shutdown headroom.

This does not fit the 60 s a SIGINT or SIGTERM grants. An orderly stop must
use the admin shutdown API with an explicit sufficient timeout (180 s for
the defaults), or a lower `flush_retry_deadline` that fits the full bound.
The admin API also defaults to 60 s, so merely switching to it is not enough.
Longer configured windows require a correspondingly longer timeout.

### 7.2 Acknowledgement contract

This node's contract is the same in every topology: ack its upstream only
after the whole block is durable, nack retryably on storage or shutdown
failure, and refuse content permanently with `NackCause::Refused`. The
zero-output-row case remains as specified in section 6.2. This contract
makes the durable buffer's retry loop correct: it retains a bundle until
this node acks, retries retryable nacks with backoff, and stops retrying
permanent refusals.

In the recommended topology, `receiver:otlp` uses `wait_for_result: true`
with `processor:durable_buffer` immediately downstream. The producer's
request ends after the durable local write; the exporter later acknowledges
the buffer after commit. A later content refusal is recorded by the buffer
as a permanent rejection; it cannot change the producer's earlier OK.

In the strict topology, the exporter is directly downstream of the receiver
with `wait_for_result: true`, so the producer's request stays open until
commit or nack. Producers must retain and retry on timeout or retryable
nack; a content refusal reaches the producer as a non-retryable status.
The receiver `timeout` bounds the producer's wait and should cover, in the
worst case:

```text
channel residence
+ remaining flush of the block ahead (<= flush_retry_deadline)
+ window.interval
+ own flush (<= flush_retry_deadline)
+ notification delivery
```

The strict example uses `timeout: 180s` with `flush_retry_deadline: 60s` and
`interval: 15s`. This reduces but does not eliminate timeouts after
admission; a request that times out on the receiver is still committed or
nacked by the exporter, and the producer's retry then creates duplicates.
This is the documented at-least-once behavior.

### 7.3 Backpressure

While the worker refuses pdata, its bounded pdata channel fills. In the
recommended topology the durable buffer keeps bundles on disk and retries
forwarding; producers are backpressured when the buffer reaches its disk
cap under `size_cap_policy: backpressure`. In the strict topology this
pressure reaches the receiver directly: it blocks on the channel send until
its timeout, and beyond the configured admission concurrency it answers
`RESOURCE_EXHAUSTED`. No new engine mechanism is needed.

### 7.4 Configuration

Exporter options, shown with the strict topology's 15 s window:

```yaml
type: exporter:series_parquet
config:
  storage: { s3: { base_uri: s3://bucket/otel, region: eu-central-1 } }
  retry: { max_retries: 5, init_backoff: 200ms, max_backoff: 10s,
           backoff_base: 2.0, retry_timeout: 30s }   # per object-store op
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
    max_row_bytes: 1MiB
    max_nesting_depth: 32
  series_cache:
    max_entries: 200000
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
  notify_batch: 64
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
UTF-8 bytes, integers numeric, doubles by numeric order with -0.0 equal to
+0.0 and every NaN (either sign) after all numbers ascending (an explicit
comparator, not `total_cmp`), booleans false before true, fixed-size bytes
lexicographic, nulls per `nulls: first|last` (default `last`). Descriptor
re-emission is tied to the `date/hour` partition and has no separate
interval.

Sort key guidance for the README: `series_id, time_unix_nano` is a good
locality and compression default. When the dominant queries filter on a
denormalized column (`service_name`, `environment`), placing those columns
first (`service_name, series_id, time_unix_nano`) gives Parquet row-group
statistics real pruning power, at some cost in per-series locality. The
README also notes that byte-triggered rotation inside a window re-emits the
block's descriptors, so under sustained high throughput the `series` volume
grows with the number of blocks per hour.

Strict example pipeline in `configs/series-parquet-s3.yaml`: `receiver:otlp`
(`wait_for_result: true`, `timeout: 180s`) connected directly to
`exporter:series_parquet`, with an explicit `core_allocation`; the shutdown
deadline of section 7.1 is passed through the admin shutdown API timeout in
v1 (see section 6.5).

Recommended pipeline, with the other exporter options as above and feature
gate `durable-buffer` enabled alongside `series_parquet` and the storage
backend feature:

```yaml
version: otel_dataflow/v1
policies:
  resources:
    core_allocation: { type: core_count, count: 1 }
groups:
  default:
    pipelines:
      main:
        nodes:
          receiver:
            type: receiver:otlp
            config:
              protocols:
                grpc:
                  listening_addr: "127.0.0.1:4317"
                  wait_for_result: true
          durable_buffer:
            type: processor:durable_buffer
            config:
              path: /var/lib/otap/series-parquet-buffer
              retention_size_cap: 10 GiB
              size_cap_policy: backpressure
          exporter:
            type: exporter:series_parquet
            config:
              storage:
                s3:
                  base_uri: s3://bucket/otel
                  region: eu-central-1
              writer_id: collector-17
              producer_id_attribute: host.id
              window:
                interval: 120s
                max_block_bytes: 500MiB
                max_requests_per_block: 4096
                flush_retry_deadline: 60s
        connections:
          - { from: receiver, to: durable_buffer }
          - { from: durable_buffer, to: exporter }
```

With buffer capacity available, the longer window is free of producer
latency cost: the producer waits only for the WAL write. It gives fewer,
larger files and lower request cost; byte and request limits still rotate
blocks early. The buffer path must survive process restarts.

### 7.5 Telemetry

Metric set `exporter.series_parquet` (per worker unless stated):

```text
series_cache.{entries,hits,misses,evictions}
block.{active_bytes,flushing_bytes,requests_pending,pending_slot_occupied}
flush.count{reason=time|bytes|requests|shutdown}, flush.duration,
flush.failures, flush.retries, flush.cancelled
rows_written{dataset}, files_written{dataset}
series_emitted{reason=new|partition|rotation}
acks, nacks{reason=storage|too_large|invalid|unsupported|shutdown}
notify.queued, notify.failures
oldest_unacked_seconds
dropped_unsupported{kind}, denormalize.type_mismatch{column},
timestamp.out_of_range
memory.budget_bytes, memory.accounted_bytes
```

Process-scoped, reported once per process: `memory.unaccounted_rss_bytes` =
process RSS (sampled the same way the memory limiter samples it) minus the
sum of `memory.accounted_bytes` over all exporter workers. The residual also
includes the durable buffer and other nodes in the same process; their
memory must be accounted for separately before attributing growth to Arrow,
the allocator or `object_store`. The README states the exporter memory bound
as part of the public contract, with the process-level formula.

### 7.6 Strict topology: network distribution and producer sizing

The producer-timeout and memory requirements below apply when the producer's
call is held until object-storage commit: the strict topology. The
recommended durable-buffer topology removes that window-length hold and
these producer requirements. The connection distribution facts still apply
to both topologies.

The engine creates a `SO_REUSEPORT` listener per worker on the same port
(`engine/src/effect_handler.rs`). The kernel distributes TCP connections
by a hash of the connection four-tuple. This distributes connections, not
requests; it is a lottery rather than a balancer.

For ten sources with one connection each:

| Workers | Connections held by the busiest worker, of 10 | Probability that some worker is idle |
| ---: | ---: | ---: |
| 2 | 6.2 | 0 percent |
| 4 | 4.2 | 22 percent |
| 8 | 3.0 | 97 percent |

More workers than sources leaves workers idle, and a skew like 5/3/2/0 is
ordinary. A gRPC connection is long lived and multiplexes requests as
HTTP/2 streams, so its assignment is sticky until the producer reconnects.
This helps descriptor locality (a source's series are described by one
writer) and hurts balance (a hot source is stuck on one core).
Per-connection stream concurrency follows `max_concurrent_streams`, which
tracks `max_concurrent_requests` by default
(`otap/src/otap_grpc/common.rs`).

The binding operational constraint in strict mode is the producer timeout.
With the exporter directly downstream and `wait_for_result: true`, the
response is held until the request's rows are durable in object storage.
A request arriving just after a rotation waits nearly a whole window plus
the flush. The producer's timeout must exceed
`window.interval` plus the flush time by a clear margin; the reference
configuration uses 180 s (section 7.2 gives the full receiver-side bound).
A shorter timeout makes the producer give up and resend, safe under
at-least-once delivery but multiplying traffic and rows. This was observed
with a six-second exporter timeout in the outage test, which produced
deadline errors and duplicate rows.

The [Alloy measurements][alloy-research] show why timeout tuning alone is
not enough. The producer must buffer at least `input_rate * hold` records;
queue slots stay occupied until the export completes, including retries.
Use `queue_size = input_rate * hold * 1.5` for headroom. For the measured
Alloy v1.19.2 file-tail/Loki pipeline, the queue stores one wrapper per log
record and merges only after dequeue. Batching reduces wire requests but
does not reduce the per-record backlog cost: about 2.0 KB of resident memory
per queued record, even for a roughly 60-byte log line.

The measured sizing model is `RSS ~= 236 MB + queue_size * 2.0 KB`, with a
catch-up peak about `1.6 * RSS` after an outage. Using the measured 2.03 KB
slope and the 1.5 headroom factor gives these estimates, not universal
producer memory bounds:

| Input rate | Hold | Queue with headroom (records) | Steady RSS | Catch-up peak |
| ---: | ---: | ---: | ---: | ---: |
| 1,000/s | 15 s | 22,500 | 280 MB | 450 MB |
| 1,000/s | 60 s | 90,000 | 420 MB | 670 MB |
| 5,000/s | 15 s | 112,500 | 465 MB | 745 MB |
| 5,000/s | 60 s | 450,000 | 1.15 GB | 1.85 GB |

The 60 s rows represent a storage outage holding a request for the flush
retry deadline, not a 60 s normal window or the full worst-case bound in
section 7.2. A gigabyte of producer memory is not a reasonable ask. At those
rates the strict topology depends on the producer parking its source when
the queue fills. For a file source, `block_on_overflow = true` is safe because
the file is the buffer. Blocking also parks the Loki receiver's single
goroutine, so it stalls every Loki-sourced pipeline in a shared producer.

Batching still matters for throughput: the ceiling is
`num_consumers * records_per_export / hold`. Without batching the measured
Loki bridge sends one record per export. Use an item-sized queue and explicit
batching; a request-sized queue of single-record wrappers can cap the batch
itself. Receiver `max_concurrent_requests` must cover producer export
concurrency. Alloy's default per-attempt timeout of 5 s repeatedly expired
against a 15 s hold and delivered zero records in the measurement.

Mitigations for skew: fewer and larger workers (also cheaper by section
5.6), a client configured with several endpoints and a round-robin policy
instead of a single connection, or an L7 proxy balancing per request.
Periodic reconnection would reshuffle the hash, but the gRPC server
currently exposes keepalive settings (TCP and HTTP/2), not a maximum
connection age.

[alloy-research]: ../../../.superpowers/sdd/2026-09-21-series-parquet-exporter-node/alloy-research.md

### 7.7 Deployment topologies

Producer acknowledgement is a deployment choice. Both series-parquet
topologies use `wait_for_result: true` on the receiver; this node's own
contract in section 7.2 does not change. For supported content, with
unacknowledged input retained and retried, the choices are:

| Topology | What the producer's acknowledgement means | Trade-offs |
| --- | --- | --- |
| `receiver -> durable_buffer -> series_parquet` (RECOMMENDED) | Durable on this node's disk. | The producer uses default timeouts and needs no window-sized buffering. The buffer retries this node's retryable nacks with exponential backoff. With lossless retention, data is lost only if the node's disk is lost before it reaches object storage. `retention_size_cap` defaults to 10 GiB, divided across cores; `size_cap_policy: backpressure` loses nothing, while `drop_oldest` is controlled loss. |
| `receiver -> series_parquet`, `wait_for_result: true` (strict) | In object storage. | No loss in any supported failure: acknowledged input is already in object storage; the producer retains and retries unacknowledged input. Its call is held for up to a window plus the flush in the uncontended case (full bound in 7.2), requiring at least `input_rate * hold` records in producer memory or source backpressure (7.6). |
| `receiver -> exporter:parquet` (upstream exporter, contrast only) | Accepted into memory. | That exporter sends no acknowledgement at all. With receiver `wait_for_result: false`, the producer's OK means only memory acceptance; any restart loses buffered data. With `wait_for_result: true`, the receiver waits until timeout rather than receiving a durability ack. |

The durable buffer is `processor:durable_buffer`, feature gate
`durable-buffer`, and is experimental. It persists to its WAL before acking
the receiver, then forwards finalized bundles on a timer. Storage nacks
from this exporter remain retryable; content refusals remain permanent and
are recorded as rejected bundles, not retried. The table's lossless case
uses `backpressure` without age-based expiry; `drop_oldest` or `max_age`
deliberately permits loss. A full disk budget propagates backpressure to
the producer, so default timeouts do not promise unlimited outage buffering.

With the durable buffer, the window interval stops trading against producer
latency and can be chosen purely for storage economics: longer windows,
fewer and larger files, lower request cost (section 5.6). This is a
significant simplification. The exporter still obeys its byte and request
caps; the buffer absorbs the delay on disk.

Validation of this combined topology belongs to the plan-3 measurement
plan (section 9.5); it is not yet covered by the shipped tests. The
recommendation is the design default, with that validation still required.

## 8. Error handling

- Request above `max_request_bytes`, `max_extracted_bytes` or
  `max_row_bytes`, or whose reservation exceeds `max_block_bytes`:
  `NackCause::Refused`.
- Payload conversion failure or schema violation: `Refused`, continue.
- Duplicate attribute keys, nesting too deep, unspecified temporality,
  inconsistent histogram (section 5.1 definition), count above `i64::MAX`:
  `Refused`.
- Exponential histogram or summary point under `unsupported: reject`, and
  any traces request: `Refused`.
- Object store failure: retry the sealed block until the absolute
  `flush_retry_deadline`, then retryable nack for the whole block.
- Parquet encoding error (bug, not I/O): treated as a block failure without
  retry; retryable nack for the block, error event; the exporter keeps
  running.
- Shutdown: pending slot and force-drained pdata get retryable nacks; at the
  deadline the flush task is cancelled and all uncommitted requests get
  retryable nacks.
- Ack/nack delivery failure (channel closed): log and count; never
  re-export.

## 9. Testing

### 9.1 `series-lake` unit and property tests

- Golden vectors for the canonical encoding and hash, including the edge
  cases of section 4; one vector set produced by an independent Python
  implementation; every vector also checked through OTLP-to-OTAP conversion
  (including CBOR-encoded nested values).
- `extract` on logs and metrics fixtures, for both OTLP-bytes input and
  transport-optimized OTAP input with dictionaries; malformed parent ids;
  extracted-output, row-size and nesting-depth enforcement; unspecified
  temporality rejection.
- Cache: entry limit, eviction order, partition tracking.
- Block and buffer: byte accounting with shared buffers (pinned, not
  double-counted), runs counted inside the block, pending-series entries and
  tokens charged, reservation before mutation, descriptor bytes zero when
  already pending or committed, whole-request-in-one-block, oversize
  rejection into an empty block, request-count limit, zero-row request acked
  immediately.
- Reference oracle: random input, split into arbitrary requests and runs,
  flushed through `SortedTableBuffer` and the sink to `LocalFileSystem`; the
  output read back must equal a naive `Vec<Row>` sort of the same input,
  for every dataset, with sorting enabled and disabled (unsorted output must
  be a permutation of the oracle). This is the main property test.
- Sink: schemas, order, paths, write order (series before values),
  empty-window behavior, frozen names across retries, writer memory limit
  closing row groups, cancellation at a chunk boundary and inside an upload.
- Clock: boundary calculation at exact boundaries, backward and forward wall
  clock steps, coalescing, re-arm while rotation is pending.
- Sort comparator: NaN of both signs last, -0.0 equal to +0.0, null
  placement.

### 9.2 Exporter tests (engine harness, injected clocks, failing store)

- This node's ack to its upstream arrives only after all files of the block
  exist.
- Descriptor flush fails, then the next block containing the same series
  re-emits the descriptor.
- Flush completes across an hour boundary: committed partition is the
  flushed block's, and the series first seen in the new hour gets its
  descriptor there.
- Same series present in FLUSHING and ACTIVE at once.
- Eviction before commit.
- Boundary arrives while FLUSHING is busy: sleep re-armed, no pdata
  admitted, control still handled, one rotation after the flush, the request
  in the pending slot admitted into the new block before any new pdata.
- Request admitted just before a boundary lands in the old window; request
  whose admission time is past the boundary waits and lands in the new one.
- Series upload succeeds, values upload fails, retry reuses names and
  overwrites.
- Restart within the same window produces distinct file names.
- Upstream disconnect before this node's ack does not remove admitted data;
  in the strict topology this includes a producer disconnect.
- Original payload and conversion workspace released after extraction
  (retained bytes measured); stored tokens carry no headers or claims.
- Notification delivery with a full completion channel does not stall the
  boundary or shutdown handling.
- Force-drained pdata during shutdown is nacked; pending slot nacked at
  shutdown; shutdown with FLUSHING and ACTIVE both non-empty, with and
  without meeting the deadline; cancellation stops the flush task within
  `abort_timeout`; dropping the `start` future cancels the task.
- Strict receiver-to-exporter shutdown with an outstanding producer request
  and a deadline satisfying section 7.1 completes with an ack, not a drain
  timeout.
- Mixed request (supported and dropped rows) acked only at commit; reject
  policy nacks atomically with `Refused`; traces request refused.
- INT64_MAX value round-trips through `value_int`; negative (wrapped)
  timestamps become null and are counted.
- Shutdown latency under a saturated inbox stays below the documented bound
  (section 6.7).

### 9.3 Fuzzing

`proptest` (added to the workspace) and `cargo-fuzz` targets for `canonical`
and `extract`: deep arrays and kvlists, arbitrary UTF-8, empty strings, huge
attribute maps, NaN patterns, duplicate keys, malformed parent ids, histogram
shape mismatches, large bodies, shared Arrow buffers, malformed CBOR in
`ser`. Invariants: never panics; semantically equal OTLP and OTAP inputs
produce the same `series_id`.

### 9.4 End-to-end

Definition: a real OTLP producer over the network, a real `df_engine`
process with `receiver:otlp` (`wait_for_result: true`), optionally
`processor:durable_buffer`, and `exporter:series_parquet`, a real S3 protocol
endpoint (MinIO or LocalStack, as in
`configs/trafficgen-parquet-local-s3.yaml`), and DuckDB as the reader. A mock
object store does not count as end-to-end. The strict topology remains a
required case; plan 3 adds the recommended topology as specified in 9.5.

The producer knows exactly what it sent (for example 100 producers, 10k
requests, 1M log records, 50k metric points, N known series). After the
pipeline drains, assertions for a healthy run without retries:
producer-acked requests equal expected; values row counts equal expected;
descriptor coverage holds per partition; every file is sorted as configured;
every `series_id` recomputes from `identity_bytes`; the canonical series view
join does not change the values row count; mixed-schema fixtures (an added
denormalized column) read with union-by-name. First implemented for logs
against `LocalFileSystem`, then MinIO, then metrics. Runs in CI.

### 9.5 Failure test in v1

One storage-outage-and-recovery scenario on the strict end-to-end topology:
the store becomes unreachable for longer than `flush_retry_deadline` while
producers keep sending, then recovers. Assertions: `block.active_bytes` and
`block.flushing_bytes` never exceed their limits, exporter RSS stays within
the documented bound, producers receive retryable nacks or timeouts, and
after recovery `oldest_unacked_seconds` returns to baseline with no
acknowledged data missing. This scenario also runs as a short PR-tier soak
(a few minutes, forced rotations, one outage).

Plan 3 must also prove the recommended topology with a real
`receiver -> durable_buffer -> series_parquet` pipeline, persistent buffer
path, `size_cap_policy: backpressure`, no age expiry, and a real object
store. This is required measurement and integration scope, not a claim
about shipped test coverage:

- Measure producer-visible acknowledgement latency with buffer capacity
  available and default producer timeouts. Compare 15 s and 120 s windows
  at the same input rate below early-rotation limits; report p50, p95 and
  p99 latency. ACKs must follow the durable local write without waiting for
  window rotation or object completion, and increasing the window must not
  add a window-sized producer hold.
- After known supported records are producer-acked but before their window
  is flushed, kill the whole process mid-window. Restart with the same
  buffer path and core allocation, without producer resends. Require the
  buffer to replay every acknowledged record into object storage and
  eventually drain; verify descriptor coverage and values by stable input
  record ids. Repeat a storage outage beyond `flush_retry_deadline` and
  observe this node's retryable nacks causing buffer retries with backoff.
- Exercise replay after an ambiguous completion too: kill after objects
  complete but before the buffer records this node's ack, then restart.
  Count duplicates by input record id and report their multiplicity; every
  acknowledged record must have at least one stored copy after recovery.
  Replay can create new files under a new boot id, so duplicates are allowed
  and must retain valid descriptor coverage. Do not assert exactly-once
  delivery or confuse frozen-name retries of one block with replay into a
  new block.

The buffer's memory and disk usage are reported separately from exporter
memory. This topology proof and the thirty-minute soak and failure matrix
of section 9.9 belong to plan 3. The longer nightly and qualification program
remains deferred (section 10.3).

### 9.6 Benchmarks

Layered criterion and pipeline benchmarks so the cost of each layer is
visible: OTLP to noop; plus conversion; plus extract and hash; plus sort;
plus Parquet to a local store; plus ZSTD; plus MinIO. Reported per stage:
records per second per core, CPU per record, bytes allocated per record,
peak RSS, output bytes per input record. The benchmark suite also measures
the documented expansion factors of section 6.6 (conversion, Parquet
encoder transient) and fails when they are exceeded.

### 9.7 Quality gates

- Core ready: 9.1 and 9.3 pass; streaming output equals the reference
  oracle; declared memory counters never exceeded.
- Integration ready: 9.2, 9.4 and 9.5 pass for logs and metrics; this node
  acks its upstream only after object completion; producer acknowledgements
  follow the selected topology. Plan-3 buffered latency, restart, replay and
  duplicate checks pass alongside the strict restart and outage tests, and
  the measurements and acceptance checks of 9.9 are complete.
- Canary ready (after the deferred chaos and soak program of 10.3): nightly
  soak with storage latency, errors and restarts shows no RSS trend and no
  lost acknowledged records.
- Production ready: shadow deployment next to the current pipeline with
  offline comparison of counts and cardinality, then gradual producer
  migration. This is a rollout step, not part of this design.

Every test carries `Scenario` and `Guarantees` doc comments.

### 9.8 What has been measured

These are verified results from plans 1 and 2 and the investigations,
separate from the design requirements above:

- The E2E suite runs 18 tests with Docker required
  (`SERIES_REQUIRE_DOCKER=1`) and zero skips, covering both object stores
  (MinIO and RustFS) and both readers (DuckDB and ClickHouse).
- Duplicates under at-least-once delivery were observed and are tolerated by
  the layout and both readers. A deterministic replay case asserts exact
  multiplicities, including unchanged counts for records not replayed.
- Merging the metrics datasets took a mixed metrics window from three files
  to two: one `series` file and one `values` file.
- The producer memory and throughput figures in section 7.6 come from the
  [measured Alloy rig][alloy-research], with fixed response holds, queue
  occupancy, resident memory and delivered records measured. They are
  producer results, not measurements of this exporter's throughput ceiling.

The exporter's per-core throughput ceiling, memory envelope and the
failure-model behaviour required below have NOT yet been measured. The
existing outage and replay cases do not establish the full failure matrix;
these measurements are the subject of the next plan.

### 9.9 Plan 3 requirements

The next plan must complete all of the following, in addition to the
durable-buffer topology proof already required by revision 6 in section 9.5:

- A thirty-minute soak, reporting input and drain rates, resident memory
  over time, descriptor coverage and stored record multiplicities. Every
  acknowledged supported record must remain present after drain.
- Maximum throughput per core and the share of time spent in extraction,
  encoding and upload, using the staged benchmarks of section 9.6.
- Write speed in records and bytes per second, for local storage and a real
  S3 endpoint, with workload, core count and compression settings recorded.
- Validation of the memory model against real resident memory, including
  conversion, sealing, resident merge keys, encoding and upload transients.
  Report the buffer's memory and disk separately; compare accounted bytes
  and process RSS and explain the residual rather than treating engineering
  reservations as measured ceilings.
- Failure models covering S3 slowdown and server errors, process restarts,
  hard kills, and network, DNS and TCP acknowledgement faults. Every case
  must assert at-least-once after recovery and drain by stable input record
  id: no acknowledged supported record missing, valid descriptor coverage,
  and duplicate multiplicities counted. Run both the strict topology with
  producer retry and the buffered topology with retained disk and lossless
  retention.
- The full section 9.5 durable-buffer proof: acknowledgement latency for
  15 s and 120 s windows, mid-window kill and restart without producer
  resends, retry after storage outage, and replay after ambiguous completion.

These are required plan-3 measurements and acceptance checks. Section 10.3
extends them to nightly and qualification durations; it does not defer this
thirty-minute soak or failure matrix.

## 10. Deferred work

Each item below was discussed and consciously left out of this spec. The
design above must not make any of them harder.

### 10.1 Live access to buffered data (next spec)

Goal: real-time inspection of data that the exporter has accepted but not yet
committed, the equivalent of `tail -f | grep ...` for telemetry, plus a look
into the pending buffer and the series cache. Agreed direction:

- Three distinct states of this exporter exposed by the API: `accepted`
  (admitted into a block), `committed` (files complete, ack possibly still
  queued in `notify`), and `acked` (this node has acknowledged its upstream).
  These do not describe the producer's earlier WAL acknowledgement in the
  recommended topology. `tail` shows accepted rows as they are admitted;
  `buffer` shows ACTIVE plus FLUSHING; both exclude requests that were
  rejected at admission and the request in the pending slot.
- Endpoints, subject to the next spec: `GET /v1/state` (block sizes, ages,
  pending requests, cache size, oldest unacked age),
  `GET /v1/buffer/{logs|metrics}` with simple equality and range filters on
  intrinsic and denormalized columns only, bounded by `max_rows`,
  `max_bytes` and a timeout, returning Arrow IPC or JSON;
  `GET /v1/series/{id}` over the cache (which then needs to carry more than
  the partition, a deliberate change to section 6.1);
  `GET /v1/tail/{logs|metrics}` as Server-Sent Events with the same
  filters; `POST /v1/admin/flush`.
- Isolation rules: readers copy bounded chunks and never hold a borrow across
  an await; a read that outlives its block terminates with
  `truncated: true`; the tail is a tap placed after admission (so rejected
  requests never appear) with a bounded per-subscriber queue that drops
  events and reports `{"type": "dropped", "records": N}` instead of applying
  backpressure to storage; a global introspection memory and concurrency
  budget is reserved in the configuration.
- Filters are limited to physical typed columns; no SQL, aggregation, joins
  or regex over attribute maps.
- Open decisions for that spec: a listener owned by the exporter versus a
  node-state provider registered with the admin server (which has no such
  hook today and no built-in auth or TLS); and how FLUSHING becomes readable
  given that in this spec the flush task owns the sealed block (a snapshot
  message to the task, or shared ownership reintroduced with an explicit
  borrow discipline).

Consequences already honored by this spec: `SortedTableBuffer` exposes an
iterator over the building batch and the sealed runs that yields bounded
copies (slices pin their parent allocation, so copies are made per chunk
and released before the next await); admission has a single completion
point (section 6.2 step 6) after which a tap can be attached; the block is
released when its flush result is handled, and notifications are a separate
queue, which is the buffer-removal point.

### 10.2 Other deferred items

- Traces (`signal=traces/dataset=series|spans`). The trace spec may choose
  a different descriptor model for spans while reusing the hashing mechanism
  and the storage conventions; spans are not forced into the "series" shape.
  In v1 traces requests are refused.
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
- Configurable partition granularity, day or hour, as the upstream patch
  offers. This design fixes the hour; at low volume a day granularity gives
  fewer directories and fewer files.
- Typed attribute maps (the v1 format is lossy by decision).
- Finer-grained interleaving of admission work with control handling
  (resumable extraction), if the section 6.7 bound proves too loose.

### 10.3 Chaos and soak program (deferred, must not be forgotten)

Deferred from v1 by decision, to run right after metrics land, on both
end-to-end topologies of section 9.4, extending the required plan-3 topology
proof, thirty-minute soak and failure matrix in sections 9.5 and 9.9:

- A TCP proxy (toxiproxy or equivalent) between the exporter and the store
  injecting latency, bandwidth limits, resets, timeouts and outages while
  producers keep sending, with the assertions of section 9.5.
- Failpoints in test builds (`after_series_upload`, `mid_values_upload`,
  `after_values_upload`, `before_ack`) combined with SIGKILL, restart and
  buffer replay (disk retained) or strict-mode producer retry. Invariant:
  after recovery and drain, no acknowledged supported request is missing
  from storage; duplicates are allowed.
- Soak modes: nightly (hours, realistic cardinality, periodic slowdowns and
  failures, producer reconnects, exporter restarts) and qualification (24 to
  72 hours, sustained load, cardinality churn, random failures).
  Cardinality profiles: stable (10k hot series), churn (1M distinct series
  against a 200k cache), mixed (80/20). Assertions on exporter metrics
  (cache entries within limit, block bytes within limits, eviction rate
  sane) and on RSS: p99 after the first hour must equal p99 at the end
  within tolerance; a rising baseline is a blocking leak.
- Acceptance criterion: under any supported storage latency or failure the
  exporter's memory stays within its configured bound, and after storage
  recovers the backlog drains and producers resume without loss of
  acknowledged supported data. The buffered case retains its disk and uses
  lossless retention; disk loss and deliberate retention drops have the
  different guarantees stated in section 7.7.

The "canary ready" gate of section 9.7 depends on this program.

### 10.4 Shared writer with parallel preparation

Proposal: one writer per process instead of one per worker. Workers keep
doing extraction and series computation thread-locally. At the flush
deadline a w-way merge combines every worker's sorted runs, row groups are
built once, and the result goes out as a multipart upload with several
parts in parallel when large, or a single PUT when small.

With eight workers and a 15 s window, this gives one file set per window
instead of eight: PUTs divided by eight, eight times fewer files for
readers and compaction, better compression because dictionaries and RLE
see more rows, sharper row-group statistics, and no duplicate descriptors
between writers.

In the recommended topology the durable buffer already removes the producer
hold, so longer windows can reduce file count without increasing producer
acknowledgement latency. The case for a shared writer is therefore storage
economics and per-core throughput, not relief from a producer waiting on
each flush. Strict deployments still have the window and producer-memory
tradeoff in section 7.6.

Obstacles:

- `Block`, `Extracted` and the sink live in the engine's `!Send` LocalSet
  ownership path. The flush shares `Block` and the sink through `Rc`;
  `Extracted` is owned worker-local data. Ownership shared across threads
  must move from `Rc` to `Arc`, and the block's thread-local accounting
  must become transferable. Arrow arrays are already Arc-based, so no
  data copy is required, but the ownership change is pervasive.
- Effect handlers are per worker and `!Send`. The writer cannot
  acknowledge another worker's tokens and must signal durable completion
  back to each worker, which complicates shutdown ordering. With a durable
  buffer upstream, that completion lets the buffer release retained data;
  in the strict topology it releases the producer's call.
- The ACTIVE plus FLUSHING invariant becomes process-wide and needs a
  global reservation.
- The blast radius of a failed write grows from one worker to every
  worker of that window.

If built, share the writer but NOT the cache. Keep per-worker caches and
deduplicate descriptors during the merge, which already runs ordered by
series id. This is correct because a per-worker cache errs only in the
safe direction: it skips emission only for a series it saw committed in
this partition. With a shared writer, "committed" is a global fact, so a
worker may over-emit, which the merge removes, but can never under-emit.
This keeps the per-request hot path lock-free; a shared LRU would have to
be sharded because recency mutates on read.

The merge stays sequential and is cheap: key comparison and interleave.
The expensive part is encoding and compression. Parquet 58 exposes
`ArrowColumnWriter` and `ArrowRowGroupWriterFactory` for encoding columns
on a pool and assembling the row group, after which parts upload
concurrently.

This is not to be built before plan 3 reports the share of time spent in
extraction, encoding and upload and the per-core throughput ceiling. If
one core covers a typical load, one worker per process yields one file
per window with no shared writer at all, and this item reduces to a
deployment recommendation. Only if the measurements demand it does this
become a spec revision touching sections 3, 6 and 7 plus its own
implementation plan.

## 11. Implementation order

1. `series-lake` alone: canonical encoding with golden vectors, extract,
   cache, `SortedTableBuffer`, sink to `LocalFileSystem`, reference oracle
   property test, fuzz targets. No engine, no network, no S3.
2. Strict vertical slice for logs: `receiver:otlp` to
   `exporter:series_parquet` to `LocalFileSystem`, read with DuckDB, real
   gRPC producer, real ack. Then the same against MinIO, plus the strict
   v1 outage test (9.5).
3. Metrics (number and histogram points in `metrics/values`) reusing the
   same machinery.
4. Plan 3: per-core throughput, extraction/encoding/upload time shares,
   write speed, resident-memory validation, a thirty-minute soak and the
   at-least-once failure matrix (9.9), plus the required buffered-topology
   latency, restart, replay and duplicate proof (9.5); quality gates through
   "integration ready".
5. Chaos and soak program (10.3), then the "canary ready" gate.
