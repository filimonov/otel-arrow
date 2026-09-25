# Series Parquet Exporter

## Metadata

- Type: `exporter:series_parquet` (`urn:otel:exporter:series_parquet`)
- Feature gate: `series-parquet` (opt-in, not in `core-exporters`); add `aws`
  for S3-compatible storage
- Metric scope: `exporter.series_parquet`
- Stability: Experimental

## Overview

`exporter:series_parquet` writes logs and metric number and histogram points
as series descriptors plus narrow values datasets, on a local filesystem or on
S3-compatible object storage, under
`v=1/signal=<signal>/dataset=<dataset>/date=<date>/hour=<hour>/`. Values rows
join their `series` descriptor on `series_id`; number and histogram points
share one metrics `values` dataset, and the descriptor's `metric_type` gives
the point kind. The normative format is
[`crates/series-lake/docs/FORMAT.md`](../../../../series-lake/docs/FORMAT.md).

The worker owns one ACTIVE block, at most one FLUSHING block and at most one
parked request. A request is acknowledged only once its whole block has been
written and its descriptors marked committed. Admission closes while the
ACTIVE block waits for the flush slot, so a slow destination becomes
backpressure; a request the ACTIVE block has no room for is parked and enters
the next block before anything newer. A block covers one aligned
`window.interval` window (whole seconds) and is sealed when it ends, or
earlier when it reaches `window.max_block_bytes` or
`window.max_requests_per_block`, which writes another file set for that window.
Waiting uses the monotonic clock, so a wall clock stepping back delays
acknowledgements by at most one interval.

Delivery is at-least-once: producers must retry a request on a retryable
failure or a timeout, and a retry can duplicate rows already committed.

## Getting Started

Use `configs/series-parquet-local.yaml` for a local destination and
`configs/series-parquet-s3.yaml` for S3-compatible storage. From
`rust/otap-dataflow`:

```bash
cargo build -p otel-arrow-dfe --bin df_engine --features series-parquet,aws
mkdir -p /tmp/series-parquet
./target/debug/df_engine --config configs/series-parquet-local.yaml --validate-and-exit
./target/debug/df_engine --config configs/series-parquet-local.yaml --http-admin-bind 127.0.0.1:8080
```

Both examples run on one core; each added core is another worker with its own
budget and descriptor cache (see "Memory").

## Delivery and shutdown

Connect `receiver:otlp` directly to this exporter with
`protocols.grpc.wait_for_result: true` and `timeout: 180s`. An OK response
means every file of that request's block completed in the object store. For
`storage: file` the local backend renames a staging file into place but never
calls `fsync`, so an acknowledged file survives a crash of this process, not
necessarily a host crash; use a cloud store, or a filesystem mounted for
synchronous writes, when an ack must survive the host.

| Outcome | gRPC status | Producer action |
| --- | --- | --- |
| Durable | OK | none |
| Content, schema or budget refusal | INVALID_ARGUMENT | fix the request |
| Storage failure, flush deadline, shutdown, internal error | UNAVAILABLE | retry |
| Receiver admission exhausted | RESOURCE_EXHAUSTED | retry later |
| Producer-side timeout | DEADLINE_EXCEEDED | retry; may duplicate |

Every nack's status message names the rule or limit that decided it and what
to do, for example `request of 20000000 bytes exceeds
ingress.max_request_bytes (16777216 bytes); split the batch upstream or raise
the limit`, with quoted detail cut to 256 bytes on one line. Refusals are
logged at WARN as `series_parquet.request.failed`, at most one line per
second; a size refusal carries `limit_setting`, `observed_bytes` and
`limit_bytes`.

### Failed blocks

A storage failure is retried against the identical sealed block, same file
names and bytes, until an absolute deadline taken when the block was sealed
(`window.flush_retry_deadline`); each failed attempt is logged at WARN as
`series_parquet.flush.attempt_failed`. Encoding failures, refused credentials
and a missing bucket or path are not retried. A write that has finished when
the deadline expires is acknowledged. An unfinished one is cancelled, given at
most `upload.abort_timeout` to unwind while the flush slot stays occupied, and
its requests are nacked as retryable with a fixed reason, never the store's
error text: `could not write to object storage (<class>); retry the request`,
with the class `unavailable`, `rejected by the store`, `cancelled`, `encoding
failed` or `internal error`. Only a fully successful write marks the
descriptor cache.

A multipart completion whose response is lost may still have been applied.
The writer then sends one HEAD for that object before any abort. An object
that exists holds the block's frozen bytes, so the write goes on and the block
is acknowledged; the upload is then aborted, and only an abort answered
`NotFound` shows this completion committed it, which counts a late commit
(`flush.late_commits`, INFO `series_parquet.flush.cleanup`). An object that
does not exist is aborted and the completion's own error decides the retry. A
HEAD that fails otherwise leaves the upload alone, since the completion may
still be applied, and counts it in `flush.abort_failures`.

A failed block can still leave its files behind: the store may finish an
upload after the cancellation, or commit one and lose the response. After
such an ambiguous end the cleanup sends one HEAD per object name, within the
same cutoff. When every object exists the flush is a late commit too; its
requests are still nacked, so its rows may be stored twice once the producers
retry.

### Block admission

A request is judged against `window.max_block_bytes` on its worst case, as if
every series it carries were new to the block, so it gets the same answer after
a restart or a cache eviction; one that fits the worst case but not the space
left waits for the next block. The worst case is `P + T + sum over its series
of (2 * (A - D) + F)`: values rows P, completion token T, each series'
extracted estimate A less its decoded attribute trees D, and a fixed F of 1352
bytes per logs series or 2120 per metrics series, plus 128 per denormalized
series column. Startup requires `window.max_block_bytes` of at least twice
`ingress.max_extracted_bytes`, which covers only the `2 * A` term, so a request
of many small series can still be refused for the block, on every attempt.

### Attempt timeouts

A request waits for the rest of its aligned window before its block is
sealed, so a producer attempt timeout below `window.interval` makes
first-attempt completion depend on arrival time: expired attempts are resent
and may store rows twice, and the symptom is a steady rate of client deadlines
while `acks` still rise. Size the attempt timeout and the receiver `timeout`
as the sum of the channel residence, a flush already in progress plus its
`upload.abort_timeout`, the rest of the window (at most `window.interval`),
its own flush (up to `window.flush_retry_deadline`) and the completion
delivery. The shipped configurations use 180s against a 15s window and a 60s
flush deadline.

No block-atomic snapshot is provided: series files complete before their
values files, but readers can observe a subset of a block, including rows of a
request that was later nacked, so read values first, then descriptors. There
is no write-ahead log and no spill; a restart writes new file names under a
new boot UUID. An hour partition receives no write later than
`window.interval + 2 * (window.flush_retry_deadline + upload.abort_timeout)`
after it ends, 145s at the defaults, except an upload the store completes
after the writer gave up (FORMAT.md, "Partition lateness bound").

### Shutdown waits for the current window

The engine shuts down receiver-first: the exporter is handed `Shutdown` once
every receiver has drained, or at the shared deadline. A receiver with
`wait_for_result: true` holds each response until its block is sealed,
normally at the next window boundary, so a shutdown can spend up to one
`window.interval` before the exporter's drain starts. With a window comparable
to the deadline, the deadline expires first: the admin call returns HTTP 504
and the outstanding requests are nacked as retryable.

### The drain

Until the exporter is handed its shutdown deadline, every block keeps its own
retry deadline. Once the deadline is latched, the exporter finishes only what
it already holds:

- The parked request is nacked as retryable at once.
- The FLUSHING block retries until the earlier of its own and the shutdown
  deadline, with the backoff at its 200ms minimum. An attempt starts only
  before the deadline, and one still running there is cancelled.
- The ACTIVE block is sealed as soon as the flush slot frees and is written
  under the same rules.
- A request force-drained after the latch, and whatever has not finished by
  the deadline, is nacked as retryable with `NodeShutdown`. A decision the
  completion channel will not take is counted as a delivery failure; its
  producer times out and retries.

The node returns as soon as it holds nothing, and at the latest by the
deadline plus `upload.abort_timeout` (refused below 1s) plus the synchronous
decision of the held requests, about 9ms for the 8,191 completions a worker
can hold at the defaults. The bound is absolute, so the drain fits any
deadline: a short one nacks more, a long one commits more. An operation
already issued when the deadline cut its attempt may still complete at the
store; its block is nacked, and the retry may store those rows twice.

### Granting a deadline

Engine signal shutdown (SIGINT, SIGTERM) grants 60s and is not configurable.
The admin shutdown operation takes its own timeout, also 60s by default:

```bash
curl -X POST 'http://127.0.0.1:8080/api/v1/groups/shutdown?wait=true&timeout_secs=180'
```

There is no pipeline YAML shutdown-deadline key. Supervisors must allow the
deadline plus `upload.abort_timeout` plus the held-request decision. On
Kubernetes set `terminationGracePeriodSeconds` (30s by default) to at least
60s plus `upload.abort_timeout` and a margin, for example 75. For a longer
drain, add a `preStop` hook that calls the admin shutdown with the timeout
you want, and set the grace period above that timeout plus
`upload.abort_timeout`, since it also covers the hook.

## Configuration

Byte sizes accept integers or IEC strings such as `16MiB`; durations use
humantime strings. Unknown fields are errors, including a block budget
written under `ingress` instead of `window`. ZSTD is the only compression.

| Setting | Default |
| --- | --- |
| `window.interval` | 15s |
| `window.max_block_bytes` | 500MiB |
| `window.max_requests_per_block` | 4096 |
| `window.flush_retry_deadline` | 60s |
| `ingress.max_request_bytes` | 16MiB |
| `ingress.max_extracted_bytes` | 32MiB |
| `ingress.max_row_bytes` | 1MiB |
| `ingress.max_nesting_depth` | 32 |
| `series_cache.max_entries` | 200000 |
| `sorting.enabled` | true |
| `sorting.run_target_bytes` | 8MiB |
| `sorting.merge_chunk_bytes` | 16MiB |
| `upload.part_bytes` | 8MiB |
| `upload.concurrency` | 2 |
| `upload.abort_timeout` | 5s |
| `parquet.row_group_bytes` | 64MiB |
| `parquet.writer_limit_bytes` | 96MiB |
| `notify_batch` | 64 |
| `unsupported` | drop |
| `metrics.exemplars` | drop |
| `writer_id` | `writer` |
| `producer_id_attribute` | `host.id` |

Rules enforced at startup:

- Counts, byte and depth budgets, cache capacity, upload concurrency,
  `notify_batch` and the retry durations are positive; `upload.abort_timeout`
  is at least 1s; `ingress.max_nesting_depth` is at most 256.
- `ingress.max_row_bytes` is at most a quarter of `sorting.run_target_bytes`,
  and `window.max_block_bytes` at least twice `ingress.max_extracted_bytes`.
- `upload.part_bytes` is between 5MiB and 5GiB; a worker warns
  (`series_parquet.upload.parts_exceed_limit`) when a file of
  `window.max_block_bytes` would need more than S3's 10,000 parts.
- For cloud storage, an explicit `retry.retry_timeout` is strictly less than
  `window.flush_retry_deadline`. Without a `retry` section the store uses
  object_store's defaults with `retry_timeout` half of
  `window.flush_retry_deadline`. Local file storage applies no store retry.
- `writer_id` uses only letters, digits, `_` and `.`, since it sits between
  the `-` separators of file names; it is never part of the series identity.
- `metrics.series_attributes` and `logs.exemplars` are refused, and so is a
  denormalized column named `v`, `signal`, `dataset`, `date` or `hour`.

S3 storage takes `unsigned_payload`: when true, requests are signed with SigV4
`UNSIGNED-PAYLOAD` instead of a SHA-256 of every uploaded byte, which removes
most of the upload CPU (102.5 instead of 376.4 ns per log record against
MinIO) and leaves integrity in transit to TLS. For this exporter an unset
value is true unless the endpoint, or without one the base URI, is a plain
`http://` URL; other exporters sharing the S3 storage section keep signed
payloads. An explicit value wins over this default and `AWS_UNSIGNED_PAYLOAD`.

`producer_id_attribute` projects that resource attribute into the
`producer_id` column of every values row and stays in the identity (see the
[producer id contract](../../../../series-lake/README.md#producer-id-contract)).
Logs put named record attributes into the identity with
`logs.series_attributes`; metric identity includes every point attribute, the
metric type and the temporality. `denormalize` takes a path (`resource.`,
`scope.` or `attrs.` prefix) or an object with `path`, `column` and `type`
(`string`, `int64`, `double`, `bool`); a value of the wrong non-string type is
stored as null and counted in `denormalize.type_mismatch{column}`.

The default values sort is `series_id, time_unix_nano`. For queries that
filter on service or environment, put that denormalized column first, for
example `service_name, series_id, time_unix_nano`. A sort key must be a
sortable column of that signal's values dataset, so `attrs` is refused;
`value_int` is null on every histogram row. Series files always sort by
`series_id`.

## Producers

A request is held for, on average, half a window plus the flush, so one target
rate keeps this many requests in flight:

```text
in_flight = rate * (interval / 2 + flush) / records_per_request
```

Size the producer's concurrency and this engine's receiver
`max_concurrent_requests` (summed over every producer) above it, with margin
for a whole window, and keep `rate * interval / records_per_request` under
`window.max_requests_per_block`. A receiver limit below `in_flight` caps
throughput at the receiver while `admission.closed` stays at zero.
`configs/series-parquet-s3.yaml` is sized for 100k records/s in 512-record
requests.

At-least-once begins when a request reaches this exporter: a producer queue
overflow before that is lost and invisible here, so a file source should block
on overflow. The reference Grafana Alloy producer,
[`configs/series-parquet.alloy`](../../../../../configs/series-parquet.alloy),
its tuning and the end-to-end suite that runs it are described in the
[harness README](../../../../validation/tests/series_parquet/README.md#reference-deployment).

## Running behind durable_buffer

With `processor:durable_buffer` between the receiver and this exporter, the
producer is acknowledged once its request is in the buffer's local WAL, and
this exporter's acknowledgement goes to the buffer: an OK means "durable on
this host's WAL", not "readable in the lake". The producer notices a slow
object store only when the WAL reaches its size cap; then, with
`size_cap_policy: backpressure`, new requests get a retryable refusal.

These losses happen after the WAL acknowledgement, so the producer never sees
them; alert on each:

- A permanent refusal by this exporter (a damaged OTLP body, a request larger
  than a block, traces, or an unsupported point kind under
  `unsupported: reject`) drops the bundle, counted in the buffer's
  `resolved{outcome="permanently_rejected"}`.
- `size_cap_policy: drop_oldest` evicts, and `max_age` expires, acknowledged
  data.
- With `otlp_handling: convert_to_arrow`, or batching in the OTAP format, this
  exporter receives Arrow records no framing check has seen, so a damaged OTLP
  body is stored and acknowledged as a partial or empty batch.

Freshness has no upper bound: with no backlog it is roughly the buffer's
segment finalisation (up to 1s) and poll (100ms), the rest of this exporter's
window and the flush; during an outage it is unbounded. Watch
`oldest_unacked.age` with the buffer's queue age and WAL fill.

Each core runs an independent pipeline with its own receiver, buffer directory
`<path>/core_<core_id>` and exporter; `retention_size_cap` is divided between
cores, and SO_REUSEPORT spreads connections, not bytes, so monitor the maximum
fill across cores. There is no global order, and repeated series rows across
workers are expected. Give every process its own buffer directory, since the
WAL does not lock it. A changed core count moves no queue, so drain the old
shards first and keep old `core_<id>` directories until they are empty.

## Reading and schema changes

Values rows reference repeated descriptors, so a reader joins them through a
view that keeps the latest descriptor per `series_id`, filters metrics on the
descriptor's `metric_type`, and never classifies a values row by which of its
columns are null. The
[series-lake README](../../../../series-lake/README.md#reading-the-data) has
the DuckDB, ClickHouse and Spark recipes and the `schema_fingerprint` check.
Adding nullable or denormalized columns is supported with `union_by_name` or
`mergeSchema`; changing an existing column's type, path or meaning requires a
new base URI.

## Request cost

Object storage requests are the fixed cost of a short window. Assumptions: S3
Standard, PUT at 0.005 USD per 1000 requests, a 30-day month, one writer per
pipeline worker, and one values file per signal and window below
`upload.part_bytes`, so one PUT. With a stable series set, `series` adds 720
PUT/month/writer, one per hour partition.

| Window interval | Values files/hour/writer | PUT/month/writer | USD/month/writer | USD/month, 100 writers | USD/month, 1000 writers |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 s | 720 | 519120 | 2.60 | 260.00 | 2600.00 |
| 15 s | 240 | 173520 | 0.87 | 87.00 | 870.00 |
| 20 s | 180 | 130320 | 0.65 | 65.00 | 650.00 |
| 30 s | 120 | 87120 | 0.44 | 44.00 | 440.00 |
| 60 s | 60 | 43920 | 0.22 | 22.00 | 220.00 |
| 120 s | 30 | 22320 | 0.11 | 11.00 | 110.00 |

Every window with a new series adds one PUT, so churn dominates at short
windows; logs plus metrics roughly doubles the total; a file above
`upload.part_bytes` is a multipart upload (Create, one UploadPart per part,
Complete). Below about 15s files become too small for efficient row groups.

## Telemetry

Every metric set is registered as `exporter.series_parquet` and reported on
every `CollectTelemetry` message and once more at the node's terminal state.
Gauges are sampled on collection; anything that must not be missed between
collections is a counter.

| Metric | Unit | Description |
| --- | --- | --- |
| `series_cache.entries` | `{entry}` | Series ids the bounded descriptor cache holds. |
| `series_cache.hits` | `{lookup}` | Lookups that found a committed descriptor. |
| `series_cache.misses` | `{lookup}` | Lookups that did not. |
| `series_cache.evictions` | `{entry}` | Entries dropped because the bound was reached. |
| `block.active` | `By` | Bytes the ACTIVE block has charged. |
| `block.flushing` | `By` | Bytes the FLUSHING block charged when it was sealed. |
| `block.pending` | `By` | Bytes the one parked request retains. |
| `block.requests_pending` | `{request}` | Requests the worker still owes a decision. |
| `block.pending_slot_occupied` | `{slot}` | Whether the single parking slot is occupied. |
| `flush.duration` | `s` | Wall time one flush took, from rotation to completion. |
| `flush.retries` | `{attempt}` | Write attempts beyond the first of their flush, counted as each starts. |
| `flush.cancelled` | `{flush}` | Flushes that failed because the write was cancelled. |
| `flush.abort_failures` | `{upload}` | Multipart uploads a failed write attempt, retried or not, may have left to the bucket's lifecycle rule: the abort failed or timed out, CreateMultipartUpload got no definite answer, or the write did not unwind by the cleanup cutoff. Alert on it; each has a WARN `abort_failed` cleanup event naming the key. |
| `flush.late_commits` | `{flush}` | Flushes whose files the store committed without confirming it: acknowledged after a lost completion response whose abort was answered `NotFound`, or failed flushes whose every object exists after all, whose nacked rows may be stored twice. |
| `acks` | `{message}` | Requests acknowledged as durable. |
| `notify.queued` | `{request}` | Decided completions still waiting to be delivered. |
| `notify.token_size` | `By` | Bytes the undelivered completions retain. |
| `notify.failures` | `{request}` | Completions the engine would not accept. |
| `oldest_unacked.age` | `s` | Age of the oldest completion the worker still owes. |
| `admission.closed` | `{state}` | 1 while the node is not taking requests from its input channel. |
| `admission.closures` | `{closure}` | Times admission went from open to closed. |
| `admission.closed.duration` | `s` | Total time admission has been closed. |
| `timestamp.out_of_range` | `{timestamp}` | Point timestamps outside the representable range. |
| `memory.budget` | `By` | Bytes the configuration allows this worker to hold. |
| `memory.accounted` | `By` | Bytes the worker is accounted as holding now. |
| `flush.workspace` | `By` | Bytes the write in progress holds beside its block and merge keys (merge chunk, in-progress row group, unacknowledged upload bytes); included in `memory.accounted`. |

| Metric | Unit | Label | Values |
| --- | --- | --- | --- |
| `flushes` | `{flush}` | `reason` | `time`, `bytes`, `requests`, `shutdown` |
| `flush.failures` | `{flush}` | `error.type` | `deadline`, `permanent_storage`, `cancelled`, `encode`, `internal` |
| `nacks` | `{message}` | `error.type` | `storage`, `request_too_large`, `extracted_too_large`, `row_too_large`, `block_too_large`, `too_deep`, `invalid`, `unsupported`, `shutdown`, `internal` |
| `rows.written`, `files.written` | `{row}`, `{file}` | `signal`, `dataset` | `logs`, `metrics`; `series`, `values` |
| `series.emitted` | `{row}` | `reason` | `new`, `partition`, `rotation` |
| `dropped.unsupported` | `{row}` | `kind` | `exp_histogram`, `summary` |
| `dropped.exemplars` | `{exemplar}` | `signal` | `metrics` |
| `repaired.invalid_utf8` | `{value}` | `signal` | `logs`, `metrics` |
| `denormalize.type_mismatch` | `{value}` | `column` | one configured physical column name |

The `*_too_large` values of `nacks` name `ingress.max_request_bytes`,
`ingress.max_extracted_bytes` (the extracted request, decoded attributes
included), `ingress.max_row_bytes` (one row, attribute value or CBOR cell) and
`window.max_block_bytes`; `too_deep` is `ingress.max_nesting_depth`. The
`column` label is fixed by configuration, so no request can add a label value.
The node also registers the shared `exporter.exports` set (`messages`,
`duration`; labels `signal` and `outcome`: `success`, `refused`, `failure`).

`series.emitted{reason=rotation}` rising means early rotations re-emit
descriptors: raise `window.max_block_bytes` or `window.max_requests_per_block`.
While `admission.closed` reads 1 the upstream receiver refuses producers with
its own limits; a rising `admission.closed.duration` with `flush.duration` near
the window interval means the destination is the limit.

### Events

| Event | Level | When |
| --- | --- | --- |
| `series_parquet.start` | INFO | Once per worker: `writer_id`, `boot_id`, `storage`, `num_cores`, `memory_budget_bytes`. |
| `series_parquet.memory_budget.oversubscribed` | WARN | At start, when `memory.budget` times the engine's cores exceeds physical memory. |
| `series_parquet.upload.parts_exceed_limit` | WARN | At start, when a file of `window.max_block_bytes` would need more than 10,000 parts of `upload.part_bytes`: `max_block_bytes`, `part_bytes`, `parts`, `max_parts`. |
| `series_parquet.request.failed` | WARN | A refusal, at most one line per second. |
| `series_parquet.flush.attempt` | DEBUG, INFO on a retry | Before each write attempt: `seq`, `attempt`, `file`, `objects`, `deadline_remaining`. |
| `series_parquet.flush.attempt_failed` | WARN | After each failed write attempt: `seq`, `attempt`, `file`, `retryable`, `deadline_remaining`, `error`. |
| `series_parquet.block.committed` | INFO | A block is durable: `window_start`, `seq`, `path`, `files`, `requests`, `bytes`, `attempts`, `duration`. |
| `series_parquet.flush.failed` | ERROR | A block failed and every request in it is nacked as retryable: `window_start`, `seq`, `file`, `requests`, `bytes`, `attempts`, `error_type`, `error`. |
| `series_parquet.flush.cleanup` | WARN, INFO for a late commit or a partial block | How the write of a failed flush ended: `outcome`, `seq`, `attempt`, `file`. `late_commit` (INFO: every object exists, or a lost completion committed the object and the block was acknowledged), `partial` (INFO: `present` of `objects` exist), `abort_failed` (WARN, with `abort_error`), `unknown` (WARN, with `probe_error`: a HEAD failed or did not finish by the cleanup cutoff); a clean abort is DEBUG `aborted`. |
| `series_parquet.seal.failed` | WARN | A block could not be sealed. |
| `series_parquet.flush.task_failed`, `series_parquet.flush.cleanup_failed` | WARN | The write task or its cleanup panicked or was lost. |
| `series_parquet.notify.failed`, `series_parquet.inbox.failed` | WARN | A completion or the input channel failed. |
| `series_parquet.shutdown` | INFO | The Shutdown control message arrived. |
| `series_parquet.shutdown.deadline_exceeded` | WARN | The shutdown deadline decided what was still held. |
| `series_parquet.shutdown.complete` | INFO | The worker ended: `accepted`, `acked`, `nacked`, `abandoned`, `deadline_exceeded`, `duration`. |

## Memory

Per worker, with B `window.max_block_bytes`, E `ingress.max_extracted_bytes`,
C `series_cache.max_entries`, N `window.max_requests_per_block` and T the
measured completion-token size, retained data is bounded by
`2B + E + 128C + 2NT`: two blocks, one parked request, the cache and at most
`2N - 1` live tokens. Blocks fill to B before a byte rotation, so under
sustained load retained memory approaches `2B` plus the flush workspace. The
workspace allowance is `4 * ingress.max_request_bytes` (conversion),
`2 * sorting.run_target_bytes` (sorting), `2 * sorting.merge_chunk_bytes`
(merge and encoding), `3 * parquet.writer_limit_bytes` (writer),
`upload.part_bytes * (upload.concurrency + 1) + sorting.merge_chunk_bytes`
(upload) and 64MiB of overhead; these are reservations, not measured
ceilings. `memory.accounted` reports the retained data, tokens, cache and,
during a flush, the merge keys and `flush.workspace` of the write.

The engine also publishes one process-scoped gauge,
`memory.unaccounted_rss_bytes` =
`max(0, RSS - sum of every worker's accounted bytes)`, while a worker exists.
Conversion scratch and allocator overhead land there, so a non-zero value is
expected; one that grows while `memory.accounted` is flat points at allocator
retention or workspace. On glibc Linux the engine runs jemalloc with its
background purging thread (`background_thread on` at startup); `MALLOC_CONF`
overrides it.

## What this exporter does not keep

For data this exporter accepts, this is the complete loss list; losses before
it and behind `durable_buffer` are in "Producers" and "Running behind
durable_buffer".

| What | Kept instead | How it shows |
| --- | --- | --- |
| Traces | Nothing: the request is refused. | `nacks{error.type=unsupported}` |
| Exponential histogram and summary points | By default the request's other points. Under `unsupported: reject`, nothing: the request is refused. | `dropped.unsupported{kind}` per point, or `nacks{error.type=unsupported}` |
| Exemplars, with their filtered attributes, trace and span ids | By default the point, without its exemplars. Under `metrics.exemplars: reject`, nothing: the request is refused with a reason naming exemplars. | `dropped.exemplars{signal=metrics}` per exemplar, or `nacks{error.type=unsupported}` |
| Attribute value types in the `attrs`, `resource_attrs` and `scope_attrs` maps | Every value rendered to a string by `render_v1`, so `"42"` and `42` read the same. Identity attributes keep their types in `identity_bytes` and `series_id`, and a typed denormalized column keeps one. Log record attributes outside `logs.series_attributes` keep none. | Documented, by format decision; no counter |
| The type of a log body that is not a string | The body rendered to JSON text, so `"42"` and `42` read the same; a bytes body is a quoted base64 string. | Documented; no counter |
| An optional metrics value, such as a histogram `sum`, `min` or `max`, that is exactly zero in every point of a request | Null: the OTAP transport omits a column whose every entry in a request is the type default. | Documented; no counter |
| A timestamp of zero, or one that does not fit `i64` nanoseconds | Null in both timestamp columns. | `timestamp.out_of_range` for the ones that do not fit |
| Invalid UTF-8 in a top-level string value | The value with U+FFFD, so strings that differ only in invalid bytes share a series id. | `repaired.invalid_utf8{signal}` |
| A denormalized attribute of the wrong type | Null in its typed column; the attribute stays in its map and in the identity. | `denormalize.type_mismatch{column}` |
| Metric metadata attributes | Nothing: they are never read or validated. | Documented; no counter |
| `dropped_attributes_count` of a resource, a scope, a log record or a point | Nothing: no dataset has a column for it. | Documented; no counter |
| Arrival order | Values rows are sorted by `values_sort`; rows with equal sort keys keep no defined order. | Documented |

Values rows are never deduplicated: a retry after a written block whose ack
was lost stores the rows twice. A descriptor is written once per block and not
again in a partition where the cache holds it as committed; an eviction, a
new partition or an early rotation writes it again (`series.emitted{reason}`).

## Limits

- Traces are refused on the signal alone. Unspecified temporality, duplicate
  attribute keys, excessive nesting, invalid histogram list lengths and counts
  above `INT64_MAX` refuse the whole request; duplicate keys are checked in
  the resource, scope, log record and supported point attributes.
- An OTLP body's protobuf framing is checked before conversion, into every
  nested message, so damage at any depth refuses the request. A singular field
  or oneof that occurs twice is refused too, because the byte views would store
  another occurrence than prost keeps; the file, parquet and otap exporters
  accept it. Invalid UTF-8 inside an array or key-value list value is refused
  as undecodable. Nesting deeper than 256 levels is refused by the same walk,
  and nesting beyond `ingress.max_nesting_depth` after conversion.
- Dictionary-encoded columns are read through their dictionary. Every
  attribute key and value is charged as it is read: one longer than
  `ingress.max_row_bytes` refuses the request, and decoded attributes count
  against the same `ingress.max_extracted_bytes` as the extracted rows.
- Configure a bucket lifecycle rule for incomplete multipart uploads (S3
  `AbortIncompleteMultipartUpload`, for example after one day). The writer
  aborts the upload of every failed or cancelled write, but an abort can fail,
  a creation can fail without an answer that tells whether the upload exists,
  and a completion still in flight may be applied after the abort;
  `flush.abort_failures` counts the uploads the writer knows it may have left
  behind and is the alert signal. On a versioned bucket, each retry that
  reached the store leaves a noncurrent version.
- Writer limits (merge keys held during a merge, the average-based merge chunk,
  row-group upload bursts) are in the
  [series-lake README](../../../../series-lake/README.md#limitations-in-version-1),
  and format limits in
  [FORMAT.md](../../../../series-lake/docs/FORMAT.md#limitations-of-version-1).
- There is no compaction, discovery index, manifest, producer replay id or
  introspection endpoint beyond the metrics above.
- A flush runs on the worker's own core in bounded steps between which the
  loop admits requests, delivers completions and watches the shutdown
  deadline. Encoding one chunk (`sorting.merge_chunk_bytes`) and closing one
  row group (`parquet.row_group_bytes`) are not sliced; at the defaults the
  longest step measured about 25 ms.

A series identity is the complete set of resource, scope, metric and point
attributes, so a point attribute unique per point, such as a request id, makes
one series per point and the cache stops hitting (`series_cache.misses` rises
with the point rate). Remove such attributes upstream, in the SDK or with
`processor:attribute`:

```yaml
processor:
  type: processor:attribute
  config:
    apply_to: ["signal"]
    actions:
      - {action: delete, key: request.id}
```

Streams that differed only in the deleted attribute then share one
`series_id` and their points stay separate values rows: delta sums and
histograms stay correct when summed, cumulative metrics and gauges become
ambiguous, so delete attributes only of delta metrics.

## Related Docs

- [Lake format](../../../../series-lake/docs/FORMAT.md): the normative on-disk
  format, identity encoding and compatibility rules.
- [series-lake crate](../../../../series-lake/README.md): the
  engine-independent writer this exporter embeds.
- [Core nodes catalog](../../../README.md): every built-in node and its
  feature gate.
- [Configuration guide](../../../../../docs/configuration.md): writing runtime
  YAML.
