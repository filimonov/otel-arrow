# Series Parquet Exporter

## Metadata

- Type: `exporter:series_parquet` (`urn:otel:exporter:series_parquet`)
- Feature gate: `series-parquet` (opt-in, not in `core-exporters`); add `aws`
  for S3-compatible storage
- Metric scope: `exporter.series_parquet`
- Stability: Experimental

## Overview

`exporter:series_parquet` writes logs and metric number/histogram points as
series descriptors plus narrow values datasets, on a local filesystem or on
S3-compatible object storage. Files land under
`v=1/signal=<signal>/dataset=<dataset>/date=<date>/hour=<hour>/`. The `series`
dataset holds one descriptor row per identity; the `values` dataset of each
signal holds the records and joins back on `series_id`. Metric number and
histogram points share one `signal=metrics/dataset=values` dataset whose
columns are the union of both kinds, each kind leaving the other's columns
null, so a mixed metrics stream costs one PUT request per window instead of
two. The point kind is `metric_type` in the descriptor the join already
supplies. The normative on-disk format is
[`crates/series-lake/docs/FORMAT.md`](../../../../series-lake/docs/FORMAT.md).

The worker owns exactly one ACTIVE block and at most one FLUSHING block, plus
at most one extracted request parked in a pending slot. A request is admitted
to the ACTIVE block and is acknowledged only once the whole block has been
written and its descriptors marked committed. Admission closes once the ACTIVE
block is waiting to be rotated, so a third block is never needed and a slow
destination becomes backpressure rather than unbounded memory. A request the
ACTIVE block cannot reserve room for is not refused: its extracted rows are
parked, input closes until the next block opens, and the parked request enters
that block before anything newer.

Rotation follows aligned wall-clock windows. A block covers one
`window.interval` window and is sealed when that window ends, so the
boundary-driven case writes one file set per window rather than one per request,
and two writers of the same lake agree on where a window starts. Intervals are
positive whole seconds of at least one second. A block that reaches
`window.max_block_bytes` or `window.max_requests_per_block` is sealed before its
window ends, which writes more than one file set for that window and re-emits
that block's descriptors. The waiting is done on the engine's monotonic clock,
so a wall clock that steps backwards cannot reopen a window that was already
written, and boundaries missed while the worker was busy coalesce into one
rotation. A window also ends after one interval of monotonic time: if the wall
clock has stepped back, by any amount, the block is rotated anyway, and its
replacement keeps the same window start and re-emits its descriptors, so a
backward step delays acknowledgements by at most one interval instead of by the
length of the step.

A storage failure is retried against the identical sealed block, with the same
file names and the same bytes, until an absolute deadline taken when the block
was sealed (`window.flush_retry_deadline`). Every failed attempt is logged at
WARN as `series_parquet.flush.attempt_failed` with the error the destination
returned. At that deadline every request of the block is nacked as retryable,
the flush is reported as a deadline expiry rather than as a cancellation, and
the abandoned write is cancelled and
given at most `upload.abort_timeout` to unwind; the flush slot stays occupied
until that cleanup finishes. An encoding failure is not retried at all: only
an object-store or I/O error is, so a bug in encoding fails the block on its
first attempt instead of repeating it until the deadline. Refused credentials
and a missing bucket or path are not retried either, because no retry of the
same write can cure them. A write that has finished by the moment the
deadline expires is reported as the success it is, and its requests are
acknowledged. Only a fully successful write marks the descriptor
cache, so the cache never claims durability for rows that were not stored.

A nacked request's reason is a fixed sentence with a closed class, never the
store's error text, which names the endpoint, bucket and key layout:
`could not write to object storage (<class>); retry the request`, where the
class is `unavailable` (the store kept failing or did not answer until the
deadline), `rejected by the store` (credentials, permissions, or a missing
bucket or path), `cancelled`, `encoding failed` or `internal error`. The
store's error is logged with `series_parquet.flush.attempt_failed` and
`series_parquet.flush.failed`.

A failed block can still leave its files behind: the store may finish an
upload after the write was cancelled, or commit one and lose the response.
After such an ambiguous end the cleanup sends one HEAD per frozen object name,
bounded by the same cleanup cutoff. When every object exists the flush is a
late commit (`flush.late_commits`, INFO `series_parquet.flush.cleanup` naming
the file). Its requests are still nacked, so its rows may be stored twice once
the producers retry.

Delivery is at-least-once. Producers must retain and retry a request on a
retryable failure or a timeout, and a retry can duplicate rows an earlier
attempt already committed.

## Getting Started

Use `configs/series-parquet-local.yaml` for a local destination and
`configs/series-parquet-s3.yaml` for S3-compatible storage. From the
`rust/otap-dataflow` workspace:

```bash
cargo build -p otel-arrow-dfe --bin df_engine --features series-parquet,aws
mkdir -p /tmp/series-parquet
./target/debug/df_engine --config configs/series-parquet-local.yaml --validate-and-exit
./target/debug/df_engine --config configs/series-parquet-local.yaml --http-admin-bind 127.0.0.1:8080
```

Both examples explicitly set `core_allocation` to one core. Each pipeline
instance is a separate worker with a separate budget and a separate descriptor
cache. Increase the core count only after multiplying the memory estimate
below by the worker count.

## Delivery and shutdown

Connect `receiver:otlp` directly to this exporter with
`protocols.grpc.wait_for_result: true` and `timeout: 180s`. An OK response
means every file for that request's block completed in the object store.

For `storage: file`, "completed" is weaker than for a cloud store. The
`object_store` local backend writes to a staging file and renames it into
place, but never calls `fsync`, so an acknowledged file survives a crash of
this process and not necessarily a crash of the operating system or a power
loss. Use a cloud store, or a filesystem mounted for synchronous writes, when
an ack must survive the host.

| Outcome | gRPC status | Producer action |
| --- | --- | --- |
| Durable | OK | none |
| Content, schema or budget refusal | INVALID_ARGUMENT | fix the request |
| Storage failure, flush deadline, shutdown, internal error | UNAVAILABLE | retry |
| Receiver admission exhausted | RESOURCE_EXHAUSTED | retry later |
| Producer-side timeout | DEADLINE_EXCEEDED | retry; may duplicate |

Content, schema and budget rejections are permanent. Storage and shutdown
failures are retryable, and so is a request refused while the destination is
unavailable. An internal error of the exporter itself, such as a broken writer
invariant or an Arrow failure while extracting a request, is retryable too:
the request is not at fault, so it is never reported as a refusal. Producer
disconnect does not remove rows that were already admitted.

The status message of every nack is a sentence naming the rule or limit that
decided it and what to do, for example `request of 20000000 bytes exceeds
ingress.max_request_bytes (16777216 bytes); split the batch upstream or raise
the limit`. Any error detail it quotes is cut to 256 bytes and kept on one
line. The short machine form of the outcome is the `error.type` label of the
`nacks` metric. Refusals are also logged at WARN as
`series_parquet.request.failed` with the signal and the same sentence, at most
one line per second; the next line reports how many were left out. A size
refusal also carries `limit_setting`, `observed_bytes` and `limit_bytes`,
whichever budget refused it: the request, its extracted output, one row or
attribute value, its decoded attribute table, or its worst case in a block.

A request refused for `window.max_block_bytes` is judged on its worst case,
as if every series it carries were new to the block, whatever the descriptor
cache already holds. The same bytes are therefore refused again after a
restart or a cache eviction, and never admitted to one block after being
permanently refused by another. A request that fits the worst case but not
the space left in the ACTIVE block waits for the next block instead.

The worst case of a request, the charge it is judged on, is exactly
`P + T + sum over its series of (2 * (A - D) + 128 * C + 8 + Q)`:

- P: the measured bytes of the request's values rows.
- T: the bytes of its completion token.
- A: one series' extracted estimate, as extraction charged it.
- D: that series' decoded attribute trees, which admission drops.
- C: the series columns, 10 for logs and 16 for metrics, plus one per
  denormalized series column.
- Q: the pending-series entry, 64 bytes.

The fixed part per series, `F = 128 * C + 8 + Q`, is 1352 bytes for logs and
2120 bytes for metrics, plus 128 bytes per denormalized series column. With E
the request's extracted charge and S its number of distinct series, the charge
never exceeds the upper bound `2 * E + T + S * F`. Startup validation covers
only the `2 * E` term, by requiring `window.max_block_bytes` to be at least
twice `ingress.max_extracted_bytes`. The `S * F` term grows with the number of
series, which extraction bounds only through each series' own estimate, so no
startup factor could cover it without refusing the defaults.
A request of many small series can therefore pass `ingress.max_extracted_bytes`
and still be refused as too large for the block; the refusal names
`window.max_block_bytes` and is the same on every attempt.

### Give a producer attempt more than one window

A timeout below `window.interval` does not make a request fail. It makes
first-attempt completion impossible to guarantee, and how often it actually
expires depends on where in the window the request arrives. Windows are
aligned to the wall clock, so a request admitted just before a boundary waits
almost no time before its block is sealed, and a request that fills the byte
or request budget triggers an immediate rotation whenever it arrives. A
request admitted early in a window waits out the rest of it first.

The arithmetic is worth doing once. Absent an early byte or request rotation,
a request admitted at offset `t` into a window waits `window.interval - t`
before its block is sealed, plus the flush. So with a 6s timeout against a 15s
window, a request admitted in the first nine seconds of a window cannot be
acknowledged on its first attempt, while one admitted in the last six seconds
may well be. The result is a partial, position-dependent retry rate rather
than a failure: every expired attempt is resent, and a copy committed just
after the client gave up is stored again by the retry. Under at-least-once
delivery nothing is lost. What it costs is traffic and stored rows, and the
symptom is a steady stream of client-side deadlines with acks and
`rows.written` still rising, rather than an error.

Size an attempt timeout as the sum of the parts one request can wait through:

- channel residence before the worker admits it,
- any flush already in progress plus its `upload.abort_timeout` cleanup,
  because admission closes while the ACTIVE block waits for the flush slot,
- the remainder of the current window, at most `window.interval`,
- its own flush, up to `window.flush_retry_deadline`,
- the delivery of its completion.

The receiver timeout must cover the same sum, which is why the shipped
receiver configuration uses `timeout: 180s` and
`configs/series-parquet.alloy` ships a 180s attempt timeout. With 15s windows
and a 60s flush deadline that leaves margin, but it cannot eliminate all
timeouts under sustained backpressure. A full bounded channel makes producers
wait; exhausted receiver admission slots return RESOURCE_EXHAUSTED.

No block-atomic snapshot is provided. Series files complete before their
values files, but readers can observe a subset of a block, including rows from
a request that was later nacked. Read the values dataset first, then the
descriptors. The exporter has no write-ahead log and no spill. A local
filesystem path is a final storage backend, not recovery state. A restart
creates a new boot UUID, so a restarted engine writes distinct file names for
the same window.

### Keep `window.interval` well below the shutdown deadline

This is the most important sizing rule in this document.

The engine shuts down receiver-first: it sends `DrainIngress` to the
receivers, and the exporter is handed `Shutdown` only once every receiver has
reported that it drained, or at the shared deadline. An OTLP receiver
configured with `wait_for_result: true` holds each response until the exporter
decides that request, and the exporter decides a request when its block is
sealed, which normally happens at the next window boundary. So a shutdown
costs up to one whole `window.interval` before it can even begin, plus the
flush of the two blocks that are then in flight.

If `window.interval` is comparable to the shutdown deadline, the receiver and
the exporter wait for each other until that deadline expires: the admin call
returns HTTP 504, outstanding requests are nacked as retryable, and their
producers have to resend. The end-to-end suite exercises exactly this with a
600s window against a 20s deadline.

### The drain

Until the exporter is handed its shutdown deadline, every block keeps its own
retry deadline, `window.flush_retry_deadline` from the moment it was sealed.
Once the deadline is latched, the exporter finishes only what it already
holds, inside that deadline:

- The parked request, if there is one, is nacked as retryable at once,
  because no block will be opened for it.
- The FLUSHING block keeps retrying, but its deadline becomes the earlier of
  its own and the shutdown deadline, and the backoff between attempts drops
  to its 200ms minimum. An attempt starts only before the deadline, and one
  still running at the deadline is cancelled there.
- The ACTIVE block is sealed as soon as the flush slot frees, without waiting
  for its window, and is written under the same rules.
- A request force-drained after the latch is refused with a retryable
  `NodeShutdown` nack, so a full completion channel cannot stall the drain.
- Whatever has not finished by the deadline is nacked as retryable with
  `NodeShutdown`. Each of those decisions is attempted once, and one the
  completion channel will not take is counted as a delivery failure; its
  producer sees its own timeout and retries.

No attempt starts after the latched deadline. The node returns as soon as it
holds nothing, and at the latest by the deadline plus `upload.abort_timeout`,
the bound on unwinding an abandoned write, plus the time to decide the
requests it still holds at the deadline. That decision is synchronous and
costs about 1 microsecond per request in an unoptimized test build: about 9ms
for the 8,191 completions a worker can hold at the default
`window.max_requests_per_block` of 4096. The bound is absolute: a deadline the
node observes late does not extend it. `upload.abort_timeout` is refused below
1s, so the cleanup always has most of its allowance after that decision. The
drain therefore fits any deadline: a short one nacks more and a long one
commits more, and no setting has to be sized against it.

A storage operation that was already issued when the deadline cut its attempt
may still complete at the store after the deadline: the exporter stops
waiting for it, and a finalizing upload is not aborted. Its block is nacked
all the same, so the producer's retry can write those rows a second time, as
at-least-once delivery allows. Committed blocks are never re-exported merely
because a notification could not be delivered.

### Granting a deadline

Engine signal shutdown (SIGINT, SIGTERM) grants 60s and is not configurable.
The admin shutdown operation takes its own timeout, and its default is also
60s, so ask for more explicitly when a longer drain should commit more:

```bash
curl -X POST 'http://127.0.0.1:8080/api/v1/groups/shutdown?wait=true&timeout_secs=180'
```

There is no pipeline YAML shutdown-deadline key. Supervisors must allow the
deadline plus `upload.abort_timeout` plus the decision of the held requests
described above.

On Kubernetes the kubelet sends SIGTERM and kills the container once
`terminationGracePeriodSeconds` has passed, 30s by default, which is shorter
than the engine's 60s. No attempt starts after the engine's deadline and the
exporter returns by that deadline plus `upload.abort_timeout` plus the
decision of the held requests, so set `terminationGracePeriodSeconds` to at
least the engine's 60s plus `upload.abort_timeout` and a margin, for example
75. For a
longer drain, add a `preStop` hook that calls the admin shutdown operation
with the timeout you want and set `terminationGracePeriodSeconds` above that
timeout plus `upload.abort_timeout`: the grace period starts before the hook
runs, so it has to cover the hook too.

## Configuration

Byte sizes accept integers or IEC strings such as `16MiB`. Durations use
humantime strings. Unknown fields are errors, including a block-level budget
written under `ingress`, which belongs under `window`. ZSTD is the only
supported compression, and naming another codec is refused rather than
silently writing zstd.

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

Cross-field rules enforced at startup: `ingress.max_row_bytes` must be at most a
quarter of `sorting.run_target_bytes`; `window.max_block_bytes` must be at least
twice `ingress.max_extracted_bytes`, because a block charges a request's series
rows at up to twice their extracted estimate; `upload.part_bytes` must be
between 5MiB and 5GiB, the S3 multipart part size limits, and a worker warns
at start (`series_parquet.upload.parts_exceed_limit`) when a file as large as
`window.max_block_bytes` would need more than S3's 10,000 parts; request
counts, byte and depth budgets,
cache capacity, upload concurrency, `notify_batch` and the retry durations
must all be positive, and `upload.abort_timeout` must be at least 1s (see
"The drain"). A logical input size that cannot be measured is
refused before conversion. `retry` settings apply to individual storage
operations; `window.flush_retry_deadline` is the absolute authority for retrying
a whole sealed block. For cloud storage, an explicit `retry.retry_timeout`
must be strictly less than `window.flush_retry_deadline`; otherwise one write
attempt keeps retrying inside the store past the block's deadline and the flush
ends with no error to report. When the `retry` section is omitted, the store
uses object_store's retry defaults with `retry_timeout` set to half of
`window.flush_retry_deadline` (30s with the defaults) instead of object_store's
3m. Local file storage applies no store retry and is not checked.

S3 storage takes `unsigned_payload`. When it is true, requests are signed
with SigV4 `UNSIGNED-PAYLOAD` instead of a SHA-256 of every uploaded byte,
which removes most of the upload's CPU: 102.5 instead of 376.4 ns per log
record against MinIO. The store then no longer checks the body against the
signature, so integrity in transit rests on TLS alone. For this exporter an
unset value is true when the endpoint, or without one the base URI, is not a
plain `http://` URL, and false over plain HTTP; other exporters that share
the S3 storage section keep signed payloads unless it is set. An explicit
value wins over this default and over the `AWS_UNSIGNED_PAYLOAD`
environment variable.

`writer_id` must be nonempty and use only letters, digits, `_` and `.`: it
sits between the `-` separators of every file name, so a hyphen or a slash is
refused. It names the writer process in file names and file metadata and is
never part of the series identity. `ingress.max_nesting_depth` is capped at
256. `metrics.series_attributes` is refused, because a metric's identity
already includes every point attribute. A denormalized column may not be
named like a partition key of the layout: `v`, `signal`, `dataset`, `date` or
`hour`, in any case. `producer_id_attribute` defaults to `host.id`, projects that
resource attribute into the `producer_id` column of every values row, and does
not alter identity membership: all resource attributes remain in the series
hash. Transport-header producer IDs are not supported.

Logs use `logs.series_attributes` to put named record attributes into the
identity; metric identity includes point attributes and the metric type and
temporality. `denormalize` accepts a path shorthand or an object with `path`,
`column` and `type` (`string`, `int64`, `double`, `bool`). Paths are prefixed
`resource.`, `scope.` or `attrs.`, and any other prefix is refused. Physical
names must not collide case-insensitively with an intrinsic column or with
another configured column. A value of the wrong non-string type is stored as
null and increments `denormalize.type_mismatch{column}`.

`series_id, time_unix_nano` is the default values sort, which gives per-series
locality and good compression. For queries that filter on service or
environment, place that denormalized physical column first, for example
`service_name, series_id, time_unix_nano`; this improves row-group pruning at
some cost in per-series locality. A sort key must exist in the values dataset
of that signal and be of a sortable type, so a map column such as `attrs` is
refused. `value_int` is a valid metrics sort key now that both point kinds
share one dataset, but it is null on every histogram row. Null placement
defaults to last. Series
files always sort by `series_id` and this is not configurable. Disabling
values sorting preserves schemas and delivery guarantees.

## Examples

### Reference deployment: Alloy + df_engine + MinIO

The reference topology is a file producer in Docker Alloy, the host's
`df_engine` with its OTLP gRPC receiver, and a Docker MinIO destination:

```text
/input/events.log -> Alloy -> OTLP gRPC -> df_engine -> MinIO Parquet
                                                       |
                                             downloaded object snapshot
                                                       |
                                               DuckDB + ClickHouse
```

Run this example on Linux. Alloy uses host networking to reach the engine's
loopback listener, and MinIO publishes an ephemeral loopback-only S3 port. The
engine YAML is generated from `configs/series-parquet-local.yaml` with S3
storage settings equivalent to `configs/series-parquet-s3.yaml`, an explicit
one-core allocation and `wait_for_result: true`. The test helpers write the
exact launched YAML as `pipeline.yaml` in the test's working directory and
remove only their own containers afterwards.

The complete River configuration is `configs/series-parquet.alloy`, shared
with the normal and the outage end-to-end tests. `/input` is the mounted
producer directory and `OTLP_ENDPOINT` is the engine's `127.0.0.1:<grpc_port>`
passed in by the launcher. The transform stage supplies the resource
attributes the Loki bridge does not: `host.id`, which is the attribute named
by `producer_id_attribute`, and `service.name`, which feeds the denormalized
service column. The inserted `e2e.source` attribute is verified alongside each
log body:

```river
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
loki.source.file "series" {
  targets = [{ __path__ = "/input/events.log", job = "series-e2e" }]
  forward_to = [otelcol.receiver.loki.series.receiver]
  tail_from_end = false
}

otelcol.receiver.loki "series" {
  output {
    logs = [otelcol.processor.transform.series.input]
  }
}

// The lake takes its producer_id column from a resource attribute, which the
// Loki bridge does not supply: without this the rows would carry an empty
// producer. `host.id` is the attribute named by `producer_id_attribute` in the
// series Parquet exporter config, and `service.name` feeds the denormalized
// service column.
otelcol.processor.transform "series" {
  error_mode = "ignore"
  log_statements {
    context = "resource"
    statements = [
      `set(attributes["host.id"], "alloy-producer")`,
      `set(attributes["service.name"], "series-e2e-service")`,
    ]
  }
  output {
    logs = [otelcol.processor.attributes.series.input]
  }
}

otelcol.processor.attributes "series" {
  action {
    key = "e2e.source"
    value = "alloy-file"
    action = "insert"
  }
  output {
    logs = [otelcol.exporter.otlp.series.input]
  }
}

otelcol.exporter.otlp "series" {
  // The engine holds the OTLP response until the whole block is durable, and
  // blocks are sealed on aligned window boundaries. A timeout below
  // `window.interval` therefore cannot guarantee that an attempt completes:
  // requests arriving early in a window commonly expire, while ones arriving
  // near a boundary, or ones that trigger a byte or request rotation, do not.
  // Each expired attempt is resent, which is safe under at-least-once
  // delivery but multiplies both traffic and stored rows. An attempt timeout
  // should cover channel residence, any flush already running plus its
  // cleanup, the rest of the current window, its own flush and the
  // notification; 180s covers the documented 15s window and 60s flush
  // deadline. `SERIES_ALLOY_TIMEOUT` lets a test choose a short value.
  timeout = coalesce(sys.env("SERIES_ALLOY_TIMEOUT"), "180s")
  client {
    endpoint = sys.env("OTLP_ENDPOINT")
    compression = "none"
    tls {
      insecure = true
    }
  }
  sending_queue {
    enabled = true

    // The Loki bridge emits one log record per request, so the queue must be
    // measured in records, not requests. With "requests" the batch can never
    // grow past queue_size and concurrency collapses to one.
    sizer = "items"
    queue_size = 120000

    // Parallel exports. An export is held for half a window plus the flush on
    // average (a whole window at worst), so the exports in flight are
    //   in_flight = rate * (interval / 2 + flush) / records_per_export.
    // Sized for 10000 records/s from one producer at the 20000-record minimum
    // batch against the 15s window: 10000 * (7.5s + 2s) / 20000 ~= 4.8, and
    // 8 leaves room for the whole-window worst case.
    num_consumers = 8

    // The source is a file. Backpressure parks the tailer and leaves unread
    // data on disk, which is durable and free. Dropping is neither.
    block_on_overflow = true

    // Without this block every log line is its own OTLP export.
    batch {
      sizer = "items"
      min_size = 20000
      max_size = 50000
      flush_timeout = "5s"
    }
  }
  retry_on_failure {
    enabled = true
    initial_interval = "200ms"
    max_interval = "1s"
    max_elapsed_time = "0s"
  }
}
```

The shipped attempt timeout is 180s, which is the rule above applied to the
production defaults of a 15s window and a 60s flush deadline. For this example
the engine window is one second. `SERIES_ALLOY_TIMEOUT` overrides the timeout,
and the outage fixture sets it to a deliberately short value so that producer
retry is exercised on purpose rather than by accident. The sending queue and
the file positions are producer state; the exporter retains no write-ahead log
and no spill state. The fixture writes 12 lines, far below `min_size`, so the
batcher releases them on its 5s `flush_timeout` rather than on size.

### Sizing the producer

These figures were measured against `grafana/alloy:v1.19.2` with a server that
holds each export for a fixed time, which is what this exporter does.

The ceiling of any producer that holds one export per window is

```text
records/second = num_consumers * records_per_export / hold_time
```

The hold time is at worst a whole window plus the flush that follows it. At a
15s hold the four-consumer block these figures were measured with sustained
about 5000 records per second; at a 60s hold it sustained about 1050. Raising
`num_consumers` or `min_size` raises the ceiling proportionally.

The same relation, turned around, sizes both ends of a deployment. A request
admitted at a uniformly random point of a window waits on average half a
window, then the flush, so the requests one target rate keeps in flight are

```text
in_flight = rate * (interval / 2 + flush) / records_per_request
```

Size every limit that caps concurrent requests from it: the producer's
`num_consumers` (per producer), and this engine's receiver
`max_concurrent_requests` (summed over every producer that reaches it), with
margin for the whole-window worst case. Also keep
`rate * interval / records_per_request` under `window.max_requests_per_block`,
or blocks rotate on the request count before their window ends. The shipped
`configs/series-parquet-s3.yaml` is sized this way for 100k records/s in
512-record requests: `100000 * (7.5s + 2s) / 512` is about 1860, so it sets
`max_concurrent_requests: 2048`, and one window takes about 2930 requests. The
Alloy block above is sized for 10000 records/s per producer at its 20000-record
minimum batch, which needs about five exports in flight; it runs eight.

Three producer settings decide whether that ceiling is reachable at all.

- **Exporter batching must be switched on explicitly.** `otelcol.receiver.loki`
  turns one log line into one OTLP request holding one record, and the sending
  queue's batcher is off unless a `batch {}` block is present. Without it
  `records_per_export` is 1 and the ceiling is `num_consumers / hold_time`,
  which is under one record per second for any ordinary configuration.
- **The queue must be sized in items.** With the default `sizer = "requests"`
  the queue holds single-record requests and the accumulating batch keeps
  those slots until its export finishes, so a batch can never exceed
  `queue_size` and concurrency collapses to one.
- **Alloy's default `timeout` of 5s is below any usable window.** An attempt
  that expires is retried with a fresh deadline, so a producer left on the
  default completes nothing at all against a 15s window while logging a
  deadline error every five seconds.

Queue overflow is silent loss, and it happens before this exporter sees the
data. Watch `otelcol_exporter_enqueue_failed_log_records_total`, which is the
only loss signal available: `otelcol_exporter_send_failed_log_records_total`
stays permanently zero under `max_elapsed_time = "0s"`, because the timeout
sits inside the retry loop and retry never gives up.

`queue_size` is a memory decision. A queued 60-byte log line costs about 2 KB
of resident memory, and roughly 4 KB once the queue is pinned at capacity and
the garbage collector is under pressure, so budget about 4 KB per `queue_size`
item. An in-flight export still occupies its queue space until the export
completes, retries included.

Finally, this engine's receiver `max_concurrent_requests` must be at least the
sum of `num_consumers` over the producers it serves, and at least the
`in_flight` above. If it is lower, the extra exports queue at the receiver,
their hold time grows past one window, and the ceiling falls with no signal at
the producer and none from this exporter either: `admission.closed` stays at
zero, because the limit is the receiver's, not the exporter's. The local
example sets `max_concurrent_requests: 128`; the S3 example sets 2048.

### Where at-least-once begins

This exporter's at-least-once guarantee begins when a request reaches it.
Anything the producer drops before that point is lost, and this exporter
cannot see it or report it. Queue overflow is exactly such a drop: by default
the sending queue returns a retryable error that the Loki bridge logs and
discards, and the file tailer is never told, so it reads on and the data is
gone. That is why `block_on_overflow = true` is the recommended setting for a
file source. Backpressure parks the tailer and leaves the unread data on disk,
which is already durable storage, instead of discarding it.

MinIO defaults to `minio/minio:RELEASE.2025-04-22T22-12-26Z`, RustFS to
`rustfs/rustfs:1.0.0-rc.3`, and the ClickHouse reader fallback to
`clickhouse/clickhouse-server:26.7.4`. Set `SERIES_MINIO_IMAGE`,
`SERIES_RUSTFS_IMAGE` or `SERIES_CLICKHOUSE_IMAGE` to another locally present
tag. Alloy defaults to `grafana/alloy:v1.19.2`, `SERIES_ALLOY_IMAGE` can
override it, and Alloy is the only image the test runner may pull. A missing
Docker daemon or a missing image skips locally unless `SERIES_REQUIRE_DOCKER`
is `1`; a startup or reader failure always fails. The mandatory
`series-parquet-e2e` workflow provisions the selected images and runs both
stores and both readers with `SERIES_REQUIRE_DOCKER=1`.

This deployment runs as `DockerSlice.test_minio` in
[`test_e2e.py`](../../../../validation/tests/series_parquet/test_e2e.py)
(`test_rustfs` is the same against RustFS). It writes 12 known lines through
Alloy and six metrics requests over OTLP gRPC, downloads the objects, and runs
DuckDB and native `clickhouse-local`, or `docker exec` in the selected local
ClickHouse image, over the same files. Both readers must return every body with
its `e2e.source` attribute, agree on the row counts, and preserve them through
the latest-descriptor join. Native ClickHouse is preferred at
`/usr/bin/clickhouse-local`; `SERIES_CLICKHOUSE_LOCAL` selects another
executable. Missing both reader routes skips locally; a reader that is present
but cannot execute the query fails.

From `rust/otap-dataflow`, after the feature-enabled build above:

```bash
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install --require-hashes -r crates/validation/tests/series_parquet/requirements.lock.txt
SERIES_REQUIRE_DOCKER=1 /tmp/series-parquet-venv/bin/python -m unittest -v \
  crates.validation.tests.series_parquet.test_e2e.DockerSlice.test_minio
```

## Running behind durable_buffer

The shipped buffered topology places `processor:durable_buffer` between the
receiver and this exporter. It changes what a successful response to the
producer means, how backpressure reaches the producer, how fresh the data in
the object store is, and how the process scales across cores. This section
states those effects so that an operator can size, monitor and alert on them.

### What the producer's OK means

Without the buffer, the producer is acknowledged only after the block holding
its request is durable in the object store. With the buffer, the producer is
acknowledged as soon as the request is written to the buffer's local WAL. The
exporter's own acknowledgement then goes to the buffer, not to the producer.
An OK therefore means "durable on this host's WAL", not "readable in the lake".

Backpressure becomes two-stage. When the object store slows down, this
exporter stops admitting new blocks, the buffer keeps the pending bundles on
disk and retries them, and the producer notices nothing until the WAL reaches
its size cap. From then on, with `size_cap_policy: backpressure`, new requests
receive a retryable refusal (UNAVAILABLE for OTLP/gRPC). The producer is
refused and must retry; it is not held inside a request.

### Losses the producer never sees

Everything that happens after the WAL acknowledgement is invisible to the
producer:

- A permanent refusal by this exporter (a damaged OTLP body, a request larger
  than a block can hold, a traces request, or an unsupported point kind under
  `unsupported: reject`) makes the buffer
  drop that bundle. It is counted in the buffer's
  `resolved{outcome="permanently_rejected"}`; without the buffer the same
  request would have received a permanent refusal the producer could act on.
- `size_cap_policy: drop_oldest` evicts acknowledged data when the WAL is full.
- `max_age` expires acknowledged data older than the configured age.

Alert on each of these counters. The first can be removed by validating
requests before the WAL acknowledges them, which is planned but not
implemented.

The OTLP framing check runs only on OTLP bytes. A processor upstream that
converts OTLP to Arrow, such as `durable_buffer` with `otlp_handling:
convert_to_arrow` or `batch` batching in the OTAP format, hands this exporter
Arrow records no framing check has seen: a damaged OTLP body becomes a partial
or empty batch there, and is stored and acknowledged as such.

### Freshness

There is no strict upper bound on the time from a producer's send to the
values file being visible in the object store. In the healthy state, with no
backlog, the delay is roughly the buffer's segment finalisation (up to 1 s by
default), its poll interval (100 ms), the wait for this exporter's window to
end (up to `window.interval`), and the flush and upload of the block. With a
15 s window that is about 16 s plus the write time, not 15 s. Producer-side
batching adds to it. During an object-store outage the delay is unbounded:
`flush_retry_deadline` bounds one series of attempts for one block, after
which the buffer retries later; it does not bound the time to visibility.

Monitor freshness directly: this exporter's `oldest_unacked.age` covers the
blocks it holds, and the buffer's queue age and WAL fill cover the rest. An
end-to-end "age of the oldest accepted but unwritten record" metric is planned.

### Several cores: independent shards

The engine runs one independent copy of the pipeline per configured core. With
four cores there are four receivers, four durable buffers and four exporters.
There is no shared WAL and no work stealing between them:

- Each buffer opens its own directory `<path>/core_<core_id>` and retries only
  its own bundles.
- `retention_size_cap` is divided between the cores: 10 GiB on four cores
  gives each buffer about 2.5 GiB. One overloaded core starts refusing even
  while the other cores' directories have room. `max_in_flight`, by contrast,
  applies to each buffer separately, and this exporter's memory budgets are
  per worker and add up across cores.
- Receivers share their port through SO_REUSEPORT where available. That
  spreads connections, not requests or bytes; one long-lived busy connection
  can load a single WAL. Monitor the maximum fill and queue age across cores,
  not only the process-wide sum.
- There is no global order. Bundles of one series can reach different
  exporters, and a retried bundle can land after newer data. Each worker has
  its own descriptor cache, so repeated series rows across workers are
  expected; readers deduplicate them as the format describes. File names do
  not collide: each exporter uses its own random `boot_id` and local sequence.

### Several processes or pods

Give every process its own physical buffer directory, for example a separate
volume per instance. The same `path` string is only acceptable when it points
to different file systems. Two live processes sharing one `<path>/core_<n>` are
not a supported configuration: the WAL does not take an exclusive lock on its
directory. This also applies to an old and a new pipeline instance overlapping
during a live reconfiguration. Several processes may write to the same bucket;
their random `boot_id`s keep their files apart.

### Changing the core count

A new worker opens only the directory of its own core id; nothing moves a
queue from a core that no longer exists to another one, and a changed core
count also changes each core's share of `retention_size_cap`. Scale a buffered
deployment by stopping intake and draining the old shards first, or by an
explicit WAL migration, never by editing the core count alone. Keep the old
`core_<id>` directories available until they are empty.

The durable buffer's own module documentation still lists `RoundRobin`,
`Random` and `LeastLoaded` dispatch policies; the current configuration offers
`one_of` and `broadcast`. Do not rely on that table for an even distribution
across cores.

## Reading and schema changes

Use DuckDB 1.1 or newer. Values rows reference repeated series descriptors, so
join through a canonical view that keeps the latest descriptor per
`series_id`, breaking ties on the file name:

```sql
CREATE VIEW series AS
SELECT * FROM read_parquet(
  '/tmp/series-parquet/v=1/signal=logs/dataset=series/**/*.parquet',
  hive_partitioning=true, union_by_name=true, filename=true)
QUALIFY row_number() OVER
  (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC)=1;
SELECT v.*, s.resource_attrs FROM read_parquet(
  '/tmp/series-parquet/v=1/signal=logs/dataset=values/**/*.parquet',
  hive_partitioning=true, union_by_name=true) v
JOIN series s USING(series_id);
```

`clickhouse-local` reads the same files with the same join. It has no
`QUALIFY`, and its tie-breaker is the virtual `_path` column rather than
`filename`:

```sql
WITH canonical AS (
    SELECT * FROM (
        SELECT *, row_number() OVER (
            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
        FROM file('v=1/signal=logs/dataset=series/**/*.parquet', 'Parquet')
    ) WHERE rank = 1
)
SELECT v.*, s.resource_attrs
FROM file('v=1/signal=logs/dataset=values/**/*.parquet', 'Parquet') AS v
INNER JOIN canonical AS s ON v.series_id = s.series_id;
```

ClickHouse's `file()` does not synthesize the `date` and `hour` columns from
the path, so a query that compares the two readers column by column should
read DuckDB with `hive_partitioning=false`, which is what the end-to-end suite
does.

For Spark 3.5 or newer, use `mergeSchema` and the same canonical view:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
series_path = "/tmp/series-parquet/v=1/signal=logs/dataset=series"
values_path = "/tmp/series-parquet/v=1/signal=logs/dataset=values"
series = spark.read.option("mergeSchema", "true").parquet(series_path)
series = series.withColumn("filename", F.input_file_name())
order = Window.partitionBy("series_id").orderBy(F.desc("emitted_at"), F.desc("filename"))
series = series.withColumn("rank", F.row_number().over(order)).where("rank = 1").drop("rank")
values = spark.read.option("mergeSchema", "true").parquet(values_path)
joined = values.join(series, "series_id")
```

Here `series_path` and `values_path` are the complete Hive dataset paths under
one base URI. Adding nullable or denormalized columns is supported with
`union_by_name` or `mergeSchema`. Changing an existing column's type, path or
meaning requires a new base URI. Inspect `schema_fingerprint` alongside the
physical column schema to find incompatible mixtures:

```sql
SELECT file_name, decode(value) AS schema_fingerprint
FROM parquet_kv_metadata('/tmp/series-parquet/**/*.parquet')
WHERE decode(key) = 'schema_fingerprint';
SELECT file_name, name, type, logical_type
FROM parquet_schema('/tmp/series-parquet/**/*.parquet');
```

Different fingerprints can still mean a supported additive change; compare the
physical columns before concluding that two files are incompatible.

Metrics read the same way against one values dataset. The descriptor join
supplies the point kind, so no union of per-kind datasets is needed:

```sql
CREATE VIEW metrics_series AS
SELECT * FROM read_parquet(
  '/tmp/series-parquet/v=1/signal=metrics/dataset=series/**/*.parquet',
  hive_partitioning=true, union_by_name=true, filename=true)
QUALIFY row_number() OVER
  (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC)=1;
SELECT v.*, s.metric_name, s.metric_type FROM read_parquet(
  '/tmp/series-parquet/v=1/signal=metrics/dataset=values/**/*.parquet',
  hive_partitioning=true, union_by_name=true) v
JOIN metrics_series s USING(series_id)
WHERE s.metric_type = 'histogram';
```

Filter on `s.metric_type` to read one point kind. The descriptor is the
authoritative source of the point kind; a values row is never classified by
which of its columns are null. That would also not be portable: DuckDB keeps
a null list and an empty list apart, while ClickHouse has no nullable `Array`
and reads a null Parquet list as `[]`.

## Request cost

Requests to object storage are the fixed cost of a short window. Assumptions:
S3 Standard, PUT at 0.005 USD per 1000 requests, a 30-day month, and one
writer is one pipeline worker. One signal writes one values file per window,
and the file stays below `upload.part_bytes`, so one file is one PUT. With a
stable series set, `series` is written once per hour partition.

The files/hour column counts values files; monthly PUTs include `series`. The
100-writer and 1000-writer columns multiply the per-writer USD figures.

| Window interval | Values files/hour/writer | PUT/month/writer | USD/month/writer | USD/month, 100 writers | USD/month, 1000 writers |
| --- | ---: | ---: | ---: | ---: | ---: |
| 5 s | 720 | 519120 | 2.60 | 260.00 | 2600.00 |
| 15 s | 240 | 173520 | 0.87 | 87.00 | 870.00 |
| 20 s | 180 | 130320 | 0.65 | 65.00 | 650.00 |
| 30 s | 120 | 87120 | 0.44 | 44.00 | 440.00 |
| 60 s | 60 | 43920 | 0.22 | 22.00 | 220.00 |
| 120 s | 30 | 22320 | 0.11 | 11.00 | 110.00 |

The `series` contribution is 720 PUT/month/writer whatever the window. Because
number and histogram points share one metrics values dataset, a mixed metrics
stream pays these figures once rather than twice.

Three things change the picture:

- Every window containing a new series adds one PUT; churn dominates at short
  windows.
- Logs plus metrics in one pipeline roughly doubles the total, because each
  signal writes its own file set.
- A file above `upload.part_bytes` becomes a multipart upload: one Create, one
  UploadPart per part and one Complete.

Request cost has a fixed part (writers times windows), which shrinks by using
fewer, larger workers, and a variable part (total bytes divided by
`upload.part_bytes`), which shrinks by raising `part_bytes` at the cost of
buffer memory. Below about 15 s, files become too small for efficient row
groups and multiply the reader's and compactor's work.

## Telemetry

Every metric set is registered under the descriptor name
`exporter.series_parquet` and is reported on every `CollectTelemetry` message
and once more at the node's terminal state, so the last interval is not lost.
State gauges are sampled on collection and once before the terminal handoff,
not on every loop turn; everything that must not be missed between two
collections is a counter recorded at the lifecycle transition itself.

Unlabelled worker state and totals:

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
| `flush.abort_failures` | `{flush}` | Cancelled writes of decided flushes whose multipart abort failed or that did not unwind by the cleanup cutoff; each may leave an upload to the bucket's lifecycle rule. |
| `flush.late_commits` | `{flush}` | Failed flushes whose every object exists after all: the write completed while being cancelled, or a probe after an ambiguous failure found every object. Their requests were nacked, so their rows may be stored twice once the producers retry. |
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
| `flush.workspace` | `By` | Bytes the write in progress holds beside its block and merge keys: the merge chunk, the Parquet encoder's in-progress row group and the upload bytes the store has not acknowledged. Included in `memory.accounted`. |

Labelled sets, each with one closed enumeration:

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

Each `*_too_large` value of `nacks` names the setting it exceeded:
`ingress.max_request_bytes`, `ingress.max_extracted_bytes` (the extracted
request or one decoded attribute table), `ingress.max_row_bytes` (one row,
attribute value or CBOR cell) and `window.max_block_bytes`. `too_deep` is
`ingress.max_nesting_depth`.

The node also registers the shared `exporter.exports` set that every exporter
registers, so it appears in cross-exporter views: `messages` (`{message}`)
and `duration` (`s`, from receipt to decision), labelled `signal` and
`outcome` (`success` for an ack, `refused` for a permanent refusal, `failure`
for a retryable nack).

`denormalize.type_mismatch` is the only label that is not an enumeration, and
its values come from the `denormalize` configuration at startup: one metric
set is registered per configured column and no request can add another. Error
strings, object store paths, request ids, series ids and producer ids are
never used as labels, so no workload can grow this node's cardinality.

A series row is counted in `series.emitted` only once the block write returned
success, so an abandoned or failed block credits nothing. `series.emitted`
with `reason=rotation` rising means byte or request rotations inside single
windows are re-emitting descriptors, which is the signal to raise
`window.max_block_bytes` or `window.max_requests_per_block`.
`flush.retries` counts every attempt beyond the first when it starts, so an
outage in progress shows its retries before the flush resolves.
`flush.failures` says why a flush failed: `deadline` (the store kept failing
or did not answer until the retry deadline), `permanent_storage` (refused
credentials or permissions, or a missing bucket or path, which is not
retried), `cancelled` (the shutdown deadline cancelled the write),
`encode` (the Parquet or Arrow encoding failed) and `internal` (a seal
failure or a lost write task).

`admission.closed` is where a slow destination becomes visible. While it
reads 1 the node takes nothing from its input channel, so the receiver
upstream refuses producers with its own concurrency or memory limit
(`RESOURCE_EXHAUSTED` for OTLP gRPC) rather than this node reporting
anything. A rising `admission.closed.duration` with `flush.duration` near the
window interval means the destination, not the producers, is the limit.

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
| `series_parquet.flush.cleanup` | WARN, INFO for a late commit or a partial block | How the write of a failed flush ended: `outcome`, `seq`, `attempt`, `file`. `late_commit` (INFO: every object exists), `partial` (INFO: `present` of `objects` exist), `abort_failed` (WARN, with `abort_error`), `unknown` (WARN, with `probe_error`: a HEAD failed or did not finish by the cleanup cutoff); a clean abort is DEBUG `aborted`. |
| `series_parquet.seal.failed` | WARN | A block could not be sealed. |
| `series_parquet.flush.task_failed`, `series_parquet.flush.cleanup_failed` | WARN | The write task or its cleanup panicked or was lost. |
| `series_parquet.notify.failed`, `series_parquet.inbox.failed` | WARN | A completion or the input channel failed. |
| `series_parquet.shutdown` | INFO | The Shutdown control message arrived. |
| `series_parquet.shutdown.deadline_exceeded` | WARN | The shutdown deadline decided what was still held. |
| `series_parquet.shutdown.complete` | INFO | The worker ended: `accepted`, `acked`, `nacked`, `abandoned`, `deadline_exceeded`, `duration`. |

### Reading the process residual

`memory.accounted` reports the retained exporter data and the measured
token allocations, including the cache-entry estimate, and, while a block
flushes, the encoded sort keys its merge holds for the table being written
(one key per row of that table, released when the table is written) and the
write's live workspace, also published alone as `flush.workspace`: the merge
chunk being produced, the Parquet encoder's in-progress row group, and the
upload bytes the store has not acknowledged yet. An upload part is a slice of
the row-group buffer the encoder handed over and keeps all of it allocated,
so each such buffer is counted whole until its last byte has landed. The
flush returns to the worker's loop between bounded steps, so a sample taken
during a flush reads what the write holds at that moment.
`memory.budget` reports the configured retained and workspace allowance.

The engine additionally publishes one process-scoped gauge under the same
descriptor name, `memory.unaccounted_rss_bytes`, which is
`max(0, RSS - sum of every worker's accounted bytes)`. Normal conversion and
encoding scratch and allocator overhead appear in this residual, so a non-zero
value is expected. It is published once per process, only while at least one
registered exporter worker exists, and it reuses the engine monitor's single
RSS sample. Watch its trend as well as its absolute value: a residual that
grows while `memory.accounted` is flat points at allocator retention or
at workspace, not at retained block data.

### Memory model

On glibc Linux the engine starts jemalloc with its background purging thread
(the startup line `INFO memory allocator jemalloc, background_thread on` says
so; musl builds keep jemalloc's default and say `off`). Without it, freed
pages go back to the kernel only while the process keeps allocating: a
series_parquet engine's quiet RSS spread by 15 to 34 percent between
identical runs, and by 1.4 to 6.6 percent with it. Retention right after an
allocation burst still varies by several MiB, so a short-lived process's
RSS stays less predictable. `MALLOC_CONF` overrides the setting.

Per worker, let B be `window.max_block_bytes`, E
`ingress.max_extracted_bytes`, C the configured
`series_cache.max_entries`, N `window.max_requests_per_block` and T the
measured retained completion-token size. The retained-data bound is
`2B + E + 128C + 2NT`. Only ACTIVE and FLUSHING exist, and one extracted
request may wait in the pending slot. Normal
admission also reserves notification credit, capped at `2N - 1` live tokens,
with one immediate token held back for a force-drained request during
shutdown.

The construction workspace allowance on top of that is
`4 * ingress.max_request_bytes` for conversion,
`2 * sorting.run_target_bytes` for sorting,
`2 * sorting.merge_chunk_bytes` for merge and encoding,
`3 * parquet.writer_limit_bytes` for the writer plus its encoder transient,
and `upload.part_bytes * (upload.concurrency + 1) + sorting.merge_chunk_bytes`
for upload, plus a fixed 64MiB for container and allocator overhead.
Descriptors become bounded sorted series runs during admission, and sealing
swaps only the `emitted_at` buffers while sharing every other column, so the
eight additional timestamp bytes per series row are reserved inside B and
there is no extra block-sized sealing allowance.

These construction factors are engineering reservations, not measured
ceilings. The process planning bound is the sum of the worker bounds plus
receiver and channel memory, the engine baseline and allocator retention.
Input bytes before admission belong to the receiver and channel limits and are
outside exporter-owned accounting.

## What this exporter does not keep

This is the contract a producer can rely on: everything below is lost on
purpose, and nothing else is. Each loss says how an operator sees it. A
request the exporter refuses is nacked as a permanent `unsupported` refusal
(`nacks{error.type=unsupported}`) and nothing of it is stored. A loss the
exporter accepts is counted where a counter can be exact and documented
where it cannot.

| What | Kept instead | How it shows |
| --- | --- | --- |
| Traces | Nothing: the request is refused. | `nacks{error.type=unsupported}` |
| Exponential histogram and summary points | By default the request's other points. Under an explicit `unsupported: reject`, nothing: the request is refused. | `dropped.unsupported{kind}` per point, or `nacks{error.type=unsupported}` |
| Exemplars, with their filtered attributes, trace and span ids | By default the point, without its exemplars. Under an explicit `metrics.exemplars: reject`, nothing: the request is refused with a reason naming exemplars. | `dropped.exemplars{signal=metrics}` per exemplar, or `nacks{error.type=unsupported}` |
| Attribute value types in the `attrs`, `resource_attrs` and `scope_attrs` maps | Every value rendered to a string by `render_v1`, so `"42"` and `42` read the same. Identity attributes keep their types in `identity_bytes` and `series_id`, and a typed denormalized column keeps one. Log record attributes outside `logs.series_attributes` keep none. | Documented, by format decision; no counter |
| The type of a log body that is not a string | The body rendered to JSON text, so a string body `"42"` and an integer body `42` read the same; a bytes body is a quoted base64 string. | Documented; no counter |
| An optional metrics value, such as a histogram `sum`, `min` or `max`, that is exactly zero in every point of a request | Null. The OTAP transport omits a column whose every entry in a request is the type default, so that zero cannot be told apart from an absent value. | Documented; no counter |
| A timestamp of zero, or one that does not fit `i64` nanoseconds | Null in both timestamp columns. Zero and absent are already one value upstream. | `timestamp.out_of_range` for the ones that do not fit |
| A denormalized attribute of the wrong type | Null in its typed column; the attribute itself stays in its map and in the identity. | `denormalize.type_mismatch{column}` |
| Metric metadata attributes | Nothing: they are never read or validated. | Documented; no counter |
| `dropped_attributes_count` of a resource, a scope, a log record or a point | Nothing: no dataset has a column for it. | Documented; no counter |
| Arrival order | Values rows are sorted by `values_sort`; rows whose sort keys are equal keep no defined order. | Documented |

Values rows are never deduplicated. At-least-once delivery can store a row
twice when a producer retries after its request's block was written but
before the ack reached it; readers deduplicate values if they need to.
Series descriptors are deduplicated: a series is written once per block,
and not again in a partition where the descriptor cache holds it as
committed. An eviction, a new partition or a rotation inside one window
writes it again, and `series.emitted{reason}` counts each such row.

## Limits

### Unsupported signals and points

Traces have no dataset in the lake and are permanently refused on the signal
alone, before any conversion. Points of an unsupported kind, namely
exponential histograms and summaries, are dropped by default and counted in
`dropped.unsupported{kind}`, and the request's other points are stored.
`unsupported: reject` refuses the whole request instead, permanently; behind
`durable_buffer` that refusal drops the bundle without the producer seeing it
(see "Losses the producer never sees"). The policy decides the whole request
atomically.
No dataset stores exemplars, and `metrics.exemplars` alone decides what
happens to them, whatever `unsupported` says. `drop`, the default, keeps the
points and counts the exemplars in `dropped.exemplars`: most SDKs attach
exemplars by default, so refusing them would refuse a large share of real
metrics. `reject` refuses a request whose stored points carry exemplars,
with a reason naming exemplars. `logs.exemplars` is refused at startup,
because log records carry none. An exemplar of a point that
`unsupported: drop` discards goes with that point and is counted. A request that extracts no rows at
all is acknowledged immediately without opening a file; a request that mixes
supported and dropped rows waits for its block to commit.

Unspecified sum or histogram temporality, duplicate attribute keys, excessive
nesting, invalid histogram list lengths and counts above `INT64_MAX` are
refused atomically. Integer values remain INT64. Zero or absent timestamps
become null; a negative converted timestamp becomes null and increments
`timestamp.out_of_range`.

### Validation not performed in v1

- Metric metadata attributes and exemplar attribute payloads are never read,
  so a duplicate key inside either goes undetected. Duplicate-key validation
  covers only the lists this writer decodes: resource, scope, the log
  record's own attributes and a supported data point's attributes.
- An OTLP body's protobuf framing is validated before conversion, because
  the shared byte views decode lazily, and the check follows the OTLP schema
  into every nested message, so damage at any depth refuses the whole
  request. A singular field or oneof that occurs twice in one message is
  refused too, although protobuf allows it, because the byte views would
  store one occurrence where prost keeps another; the file, parquet and otap
  exporters accept it. String fields are not checked for UTF-8: invalid bytes
  are stored as U+FFFD and each repaired value is counted in
  `repaired.invalid_utf8{signal}`, so two strings that differ only in their
  invalid bytes can share a series id. Invalid UTF-8 inside an array or
  key-value list attribute value is refused as undecodable instead, unlike a
  top-level string, which is repaired.
- Dictionary-encoded OTAP Arrow columns are read through their dictionary,
  never expanded first. Every attribute key and value is charged as it is
  read: one longer than `ingress.max_row_bytes`, or an attribute table whose
  decoded strings and bytes pass `ingress.max_extracted_bytes`, refuses the
  request as too large. A value referenced by many rows therefore cannot grow
  memory before the budgets apply.
- The OTLP receiver hands this exporter the raw request bytes, and the
  shared conversion to Arrow encodes nested map and array attribute values
  with a recursive encoder that has no depth limit of its own. The framing
  check above refuses nesting deeper than 256 levels, the largest accepted
  `ingress.max_nesting_depth`, before that conversion; nesting between the
  configured limit and 256 is refused by `ingress.max_nesting_depth` only
  after it.

### Format and storage limits in v1

- `attrs` maps are lossy for readers: values are rendered to strings, so a
  string `"42"` and an integer `42` look the same in the map, although they
  remain different series. Bytes values render as a quoted JSON string of
  padded standard base64, as in OTLP JSON; a dedicated `body_bytes` binary
  column for the log body is deferred to a later format version.
- A histogram `sum` of exactly zero cannot be told apart from an absent sum
  after an OTAP round trip, because the transport omits a column whose every
  entry in a request is the type default. The same holds for any optional
  metrics column.
- A cancellation that lands after a file's Parquet finalization has begun
  cannot abort that file's in-flight multipart upload, because the buffered
  writer's abort is only safe before finalization starts. Configure a bucket
  lifecycle rule that removes incomplete multipart uploads; the exporter does
  not reclaim those parts. Multipart uploads begun before finalization are
  aborted within `upload.abort_timeout`, including one whose creation was
  still in flight when the cancellation came: that creation is allowed to
  finish within the same allowance so the abort has an upload to abort.
- On a versioned bucket every retry of a block rewrites the same frozen
  object names, so each attempt that reached the store keeps a noncurrent
  version until a lifecycle rule expires it.
- One Parquet row group can start several multipart upload parts at once
  whatever `upload.concurrency` says. The burst is bounded by
  `parquet.row_group_bytes`.
- Every merge key of a table being merged is resident for the whole merge, so
  sorting by a wide column such as `body` can hold close to a second copy of
  the table's payload.
- `merge_chunk_bytes` is an average-based approximation from the mean row
  width, so a chunk whose rows are much wider than the average overshoots it.
  With sorting disabled it is ignored altogether and the buffered runs go to
  the writer as they are, each at most `run_target_bytes`.
- Finalizing a values dataset flushes whatever is still buffered into one
  further run bounded by `run_target_bytes`, so the transient at seal time can
  reach `(V + 1) * run_target_bytes` for V buffered runs.

### High-cardinality point attributes

A series identity is the complete set of resource, scope, metric and point
attributes, as OpenTelemetry defines a stream. A point attribute that is unique
per point, such as a request id, a trace id or a user id, therefore makes one
series per point: the `series` dataset grows as fast as `values` and the
descriptor cache stops hitting. The exporter cannot drop a varying attribute
from the identity without merging streams that the producer reported as
distinct, so remove or bucket such attributes upstream, in the SDK with Views
or in the pipeline. The exporter has no series-to-points ratio signal yet;
`series_cache.misses` rising with the point rate is the nearest indicator.

Deleting the attribute with `processor:attribute` in front of this exporter
works and is covered end to end:

```yaml
processor:
  type: processor:attribute
  config:
    apply_to: ["signal"]
    actions:
      - {action: delete, key: request.id}
```

Two streams that differ only in the deleted attribute arrive with the same
identity. They get one `series_id` and one descriptor, written once per
partition and worker, and their points are stored as separate `values` rows
under that `series_id`, even when the timestamps are equal. Nothing is refused,
deduplicated or merged. Whether the collapsed rows still mean something
depends on the point kind:

- Delta sums and delta histograms stay correct. A reader sums the rows:
  `value_int` or `value_double`, `count`, `sum` and the bucket counts element
  by element give exactly the totals of the original streams.
- Cumulative sums and cumulative histograms become wrong. Each row is one
  stream's running total, and the totals of different streams interleave under
  one `series_id` with nothing to tell them apart. A reader taking the latest
  value picks one stream arbitrarily, and a rate over consecutive rows sees
  false resets. Summing rows that share one timestamp happens to give the
  combined total, but streams rarely report at the same instant and reset
  independently, so that does not generalize.
- Gauges become ambiguous. The collapsed series holds several samples for one
  instant and no rule for combining them; last, mean and maximum are all
  plausible and the files do not say which was meant.

The `hash` action keeps the cardinality and only obscures the value. Spatial
aggregation, dropping attributes and merging the colliding streams with a
temporality-correct function (per-stream cumulative totals with reset
handling, summed deltas, a chosen function for gauges), is not available in
otap-dataflow today: `processor:temporal_reaggregation` aggregates over time
only. Until such a processor exists, delete only attributes of delta metrics,
or aggregate cumulative metrics and gauges in the SDK.

### Operational limits

- Stage benchmarks and a measurement harness exist
  (`crates/series-lake/benches`, `crates/validation/tests/series_parquet`),
  but the memory-bound validation, expansion-factor measurement and soak runs
  they support are still in progress; the workspace factors in the memory
  model above are reservations, not measured ceilings.
- There is no compaction, no discovery index, no manifest and no producer
  replay id.
- There is no live buffer, tail or series introspection endpoint beyond the
  metrics above.
- A flush runs on the worker's own core, beside the loop that admits
  requests, delivers acks and nacks, answers telemetry and watches the
  shutdown deadline. It works in bounded steps and returns to that loop
  between every two. A step encodes merge keys, pops merge rows or copies
  the chunk's values until it has done about 8,192 elements of work, where
  a row counts one and each list item or map entry one more, or 1 MiB of
  key bytes. Every column buffer, list and map children included, is sized
  up front and filled in place, so nothing is reallocated inside a step and
  finishing a column copies nothing more. Producing, encoding and flushing
  a chunk each get a poll of their own. The two steps that cannot be sliced
  without changing the file are encoding one chunk, bounded by
  `sorting.merge_chunk_bytes`, and closing one row group, bounded by
  `parquet.row_group_bytes`. At the defaults, on the largest block the
  default budgets admit, the longest step measured 18 to 25 ms, and the
  flush acted on a cancellation within 12 to 22 ms of it. Moving the flush
  off the core is future work.

## Related Docs

- [Lake format](../../../../series-lake/docs/FORMAT.md): the normative on-disk
  format, identity encoding and compatibility rules.
- [series-lake crate](../../../../series-lake/README.md): the
  engine-independent writer this exporter embeds.
- [Core nodes catalog](../../../README.md): every built-in node and its
  feature gate.
- [Configuration guide](../../../../../docs/configuration.md): writing runtime
  YAML.
