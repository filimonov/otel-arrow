# Series Parquet Exporter

## Metadata

- Type: `exporter:series_parquet` (`urn:otel:exporter:series_parquet`)
- Feature gate: `series_parquet`; add `aws` for S3-compatible storage
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
boundary-driven case writes one file set per window rather than one per
request, and two writers of the same lake agree on where a window starts.
Intervals are positive whole seconds of at least one second. A block that
reaches `window.max_block_bytes` or `window.max_requests_per_block` is sealed
before its window ends, which writes more than one file set for that window
and re-emits that block's descriptors. The waiting is done on the engine's
monotonic clock, so a wall clock that steps backwards cannot reopen a window
that was already written, and boundaries missed while the worker was busy
coalesce into one rotation. A window also ends after one interval of monotonic
time: if the wall clock has stepped back by more than about a second, the
block is rotated anyway, and its replacement keeps the same window start and
re-emits its descriptors, so a backward step delays acknowledgements by at
most one interval instead of by the length of the step.

A storage failure is retried against the identical sealed block, with the same
file names and the same bytes, until an absolute deadline taken when the block
was sealed (`window.flush_retry_deadline`). Every failed attempt is logged at
WARN as `series_parquet.flush_attempt_failed` with the error the destination
returned. At that deadline every request of the block is nacked as retryable
with a reason that carries the last attempt's error, the flush is reported as
a deadline expiry rather than as a cancellation, and the abandoned write is
cancelled and
given at most `upload.abort_timeout` to unwind; the flush slot stays occupied
until that cleanup finishes. An encoding failure is not retried at all: only
an object-store or I/O error is, so a bug in encoding fails the block on its
first attempt instead of repeating it until the deadline. Refused credentials
and a missing bucket or path are not retried either, because no retry of the
same write can cure them. A write that has finished by the moment the
deadline expires is reported as the success it is. Its requests are
still nacked as retryable, because the producer holds the only copy of rows
that are not durable. Only a fully successful write marks the descriptor
cache, so the cache never claims durability for rows that were not stored.

Delivery is at-least-once. Producers must retain and retry a request on a
retryable failure or a timeout, and a retry can duplicate rows an earlier
attempt already committed.

## Getting Started

Use `configs/series-parquet-local.yaml` for a local destination and
`configs/series-parquet-s3.yaml` for S3-compatible storage. From the
`rust/otap-dataflow` workspace:

```bash
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet,aws
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
line. The short machine form of the outcome is the `reason` label of the
`nacks` metric. Refusals are also logged at WARN as
`series_parquet.request_failed` with the signal and the same sentence, at most
one line per second; the next line reports how many were left out.

A request refused for `window.max_block_bytes` is judged on its worst case,
as if every series it carries were new to the block, whatever the descriptor
cache already holds. The same bytes are therefore refused again after a
restart or a cache eviction, and never admitted to one block after being
permanently refused by another. A request that fits the worst case but not
the space left in the ACTIVE block waits for the next block instead.

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
`rows_written` still rising, rather than an error.

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

Size the deadline for the two blocks that can be in flight, not for one. The
FLUSHING block must finish and release the slot before the ACTIVE block can be
sealed, the slot stays held through the abandoned write's
`upload.abort_timeout` cleanup, and the ACTIVE block's own retry deadline is
taken when it is sealed, which is after all of that. The two retry windows are
therefore sequential rather than overlapping, and one
`window.flush_retry_deadline` is not a safe ceiling. The conservative bound is:

```text
window.interval + 2 * (flush_retry_deadline + upload.abort_timeout) + notification margin
```

With the defaults that is 15s + 2 * (60s + 5s) = 145s plus the notification
margin, which fits the admin API's 180s timeout below and does not fit the 60s
that a SIGINT or SIGTERM grants. Note which term dominates: twice the flush
retry deadline is 130s of that 145s, so lowering `flush_retry_deadline` buys
far more shutdown headroom than lowering `window.interval` does. Keep
`window.interval` a small fraction of the deadline anyway, for the reason
above.

### Granting a deadline

Engine signal shutdown (SIGINT, SIGTERM) currently grants 60s and is not
configurable. The admin shutdown operation takes its own timeout, and its
default is also 60s, so ask for more explicitly:

```bash
curl -X POST 'http://127.0.0.1:8080/api/v1/groups/shutdown?wait=true&timeout_secs=180'
```

There is no pipeline YAML shutdown-deadline key. Supervisors must allow the
drain plus `upload.abort_timeout` of cleanup.

An orderly shutdown runs in a fixed order: the parked request is nacked the
moment the deadline is latched, because nothing will open a block for it; the
outstanding FLUSHING block is finished, so a block that still reaches storage
is acknowledged rather than refused; and only then is the ACTIVE block rotated
and flushed. A request force-drained after the latch is refused immediately
with a retryable `NodeShutdown` nack rather than being parked, so a full
completion channel cannot stall the drain. At the deadline each remaining
decision is attempted once, and whatever the completion channel will not take
is counted as a delivery failure and released: the producer sees its own
timeout and retries. Committed blocks are never re-exported merely because a
notification could not be delivered.

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
| `unsupported` | reject |
| `writer_id` | `writer` |
| `producer_id_attribute` | `host.id` |

Cross-field rules enforced at startup: `ingress.max_row_bytes` must be at most
a quarter of `sorting.run_target_bytes`; `window.max_block_bytes` must be at
least twice `ingress.max_extracted_bytes`, because a block charges a request's
series rows at up to twice their extracted estimate; `upload.part_bytes` must
be at least 5MiB for the S3 multipart minimum; request counts, byte and depth budgets, cache
capacity, upload concurrency, `notify_batch` and the abort and retry durations
must all be positive. A logical input size that cannot be measured is refused
before conversion. `retry` settings apply to individual storage operations;
`window.flush_retry_deadline` is the absolute authority for retrying a whole
sealed block. For cloud storage, `retry.retry_timeout` must be strictly less
than `window.flush_retry_deadline`, and the rule applies to the object store
default of 3m when the `retry` section is omitted, so the default 60s deadline
needs an explicit `retry` section. Otherwise one write attempt keeps retrying
inside the store past the block's deadline and the flush ends with no error to
report. Local file storage applies no store retry and is not checked.

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
exact launched YAML into `SERIES_REFERENCE_DIR/pipeline.yaml` and remove only
their own containers afterwards. This example keeps the downloaded Parquet
files for the two-reader check below.

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

    // Parallel exports. Each one is held for a full window, so this is the
    // only multiplier on the ceiling besides batch size.
    num_consumers = 4

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

The hold time is a whole window plus the flush that follows it. At a 15s hold
the recommended block above sustains about 5000 records per second; at a 60s
hold the same block sustains about 1050. Raising `num_consumers` or `min_size`
raises the ceiling proportionally.

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
producer's `num_consumers`. If it is lower, the extra exports queue at the
receiver, their hold time grows past one window, and the ceiling falls with no
signal at the producer. The shipped pipeline configurations set
`max_concurrent_requests: 128`, which covers the recommended
`num_consumers = 4` with a wide margin.

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

From `rust/otap-dataflow`, after the feature-enabled build above:

```bash
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install -r crates/validation/tests/series_parquet/requirements.txt
export SERIES_REFERENCE_DIR="$(mktemp -d /tmp/series-reference.XXXXXX)"
PYTHONPATH=crates/validation/tests/series_parquet /tmp/series-parquet-venv/bin/python - <<'PY'
import os
from pathlib import Path
from test_e2e import AlloyProducer, DockerStore, Engine, require_clickhouse, wait_for_alloy

require_clickhouse()
root = Path(os.environ["SERIES_REFERENCE_DIR"])
ids = [f"reference-alloy-{i}" for i in range(12)]
with DockerStore("minio") as store, Engine(root, storage=store.storage) as engine:
    with AlloyProducer(root, engine) as alloy:
        alloy.write(ids)
        wait_for_alloy(store, root, ids)
    engine.shutdown(seconds=180)
    store.download(root / "downloaded")
print(root / "pipeline.yaml")
print(root / "downloaded")
PY
```

Save the verification below as `/tmp/series-reference-readers.py` and run it
with `/tmp/series-parquet-venv/bin/python /tmp/series-reference-readers.py -v`
in the same terminal. It independently runs DuckDB and native
`clickhouse-local`, or `docker exec` in the selected local ClickHouse image,
checks all 12 bodies and their `e2e.source` attributes, and proves that the
latest-descriptor join preserves the row count. Native ClickHouse is preferred
at `/usr/bin/clickhouse-local`; `SERIES_CLICKHOUSE_LOCAL` selects another
executable. Missing both reader routes skips locally; a reader that is present
but cannot execute the query fails.

```python
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
import contextlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest
import uuid
import duckdb


def reader_unavailable(reason):
    if os.environ.get("SERIES_REQUIRE_DOCKER") == "1":
        raise AssertionError(reason)
    raise unittest.SkipTest(reason)


@contextlib.contextmanager
def reference_clickhouse(root):
    binary = os.environ.get("SERIES_CLICKHOUSE_LOCAL", "/usr/bin/clickhouse-local")
    if not (Path(binary).is_file() and os.access(binary, os.X_OK)):
        binary = (
            shutil.which("clickhouse-local")
            if "SERIES_CLICKHOUSE_LOCAL" not in os.environ
            else None
        )
    container = None
    try:
        if binary:
            command = [binary]
        else:
            if not shutil.which("docker"):
                reader_unavailable("Neither clickhouse-local nor Docker is available")
            try:
                probe = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
            except subprocess.TimeoutExpired:
                reader_unavailable("ClickHouse fallback Docker daemon did not respond")
            if probe.returncode:
                reader_unavailable("ClickHouse fallback Docker daemon is unavailable")
            image = os.environ.get(
                "SERIES_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:26.7.4"
            )
            if subprocess.run(
                ["docker", "image", "inspect", image], capture_output=True, timeout=10
            ).returncode:
                reader_unavailable(f"Local ClickHouse image is absent: {image}")
            container = subprocess.check_output(
                [
                    "docker", "run", "--pull=never", "--detach", "--network", "none",
                    "--name", "series-reference-reader-" + uuid.uuid4().hex,
                    "--user", f"{os.getuid()}:{os.getgid()}",
                    "--mount", f"type=bind,src={root},dst=/data,readonly",
                    "--entrypoint", "/bin/sleep", image, "infinity",
                ],
                text=True,
                timeout=30,
            ).strip()
            command = [
                "docker", "exec", "--workdir", "/data", container, "clickhouse", "local",
            ]

        def query(sql):
            result = subprocess.run(
                command
                + [
                    "--query",
                    sql + " FORMAT JSONCompactEachRow",
                    "--output_format_json_quote_64bit_integers=0",
                ],
                cwd=root,
                text=True,
                capture_output=True,
                check=True,
                timeout=30,
            )
            return [tuple(json.loads(line)) for line in result.stdout.splitlines() if line.strip()]

        yield query
    finally:
        if container:
            subprocess.run(
                ["docker", "rm", "--force", "--volumes", container],
                capture_output=True,
                check=False,
                timeout=20,
            )


class ReferenceReaders(unittest.TestCase):
    # Scenario: Docker Alloy delivers 12 known file lines through df_engine
    # into MinIO Parquet.
    # Guarantees: both readers preserve latest-descriptor join counts and
    # return every expected body and attribute.
    def test_latest_descriptor_join_and_bodies(self):
        root = (Path(os.environ["SERIES_REFERENCE_DIR"]) / "downloaded").resolve()
        values = "v=1/signal=logs/dataset=values/**/*.parquet"
        series = "v=1/signal=logs/dataset=series/**/*.parquet"
        expected = sorted((f"reference-alloy-{i}", "alloy-file") for i in range(12))
        with duckdb.connect() as db, reference_clickhouse(root) as clickhouse:
            duck_rows = sorted(
                db.execute(
                    """
                WITH canonical AS (
                    SELECT * FROM read_parquet(?, union_by_name=true, filename=true)
                    QUALIFY row_number() OVER (
                        PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1
                )
                SELECT v.body, coalesce(v.attrs['e2e.source'], '')
                FROM read_parquet(?, union_by_name=true) v
                INNER JOIN canonical s ON v.series_id = s.series_id
            """,
                    [str(root / series), str(root / values)],
                ).fetchall()
            )
            ch_rows = sorted(
                clickhouse(
                    f"""
                WITH canonical AS (
                    SELECT * FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
                        FROM file('{series}', 'Parquet')
                    ) WHERE rank = 1
                )
                SELECT v.body, coalesce(v.attrs['e2e.source'], '')
                FROM file('{values}', 'Parquet') AS v
                INNER JOIN canonical AS s ON v.series_id = s.series_id
            """
                )
            )
            duck_count = db.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(root / values)]
            ).fetchone()[0]
            ch_count = int(clickhouse(f"SELECT count(*) FROM file('{values}', 'Parquet')")[0][0])
            self.assertEqual(duck_count, 12)
            self.assertEqual(ch_count, duck_count)
            self.assertEqual(len(ch_rows), ch_count)
            self.assertEqual(len(duck_rows), duck_count)
            self.assertEqual(ch_rows, duck_rows)
            self.assertEqual(duck_rows, expected)


if __name__ == "__main__":
    unittest.main()
```

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
| `block.active_bytes` | `By` | Bytes the ACTIVE block has charged. |
| `block.flushing_bytes` | `By` | Bytes the FLUSHING block charged when it was sealed. |
| `block.pending_bytes` | `By` | Bytes the one parked request retains. |
| `block.requests_pending` | `{request}` | Requests the worker still owes a decision. |
| `block.pending_slot_occupied` | `{slot}` | Whether the single parking slot is occupied. |
| `flush.duration` | `s` | Wall time one flush took, from rotation to completion. |
| `flush.failures` | `{flush}` | Flushes that did not reach object storage. |
| `flush.retries` | `{attempt}` | Write attempts beyond the first, per completed flush. |
| `flush.cancelled` | `{flush}` | Flushes that failed because the write was cancelled. |
| `acks` | `{request}` | Requests acknowledged as durable. |
| `notify.queued` | `{request}` | Decided completions still waiting to be delivered. |
| `notify.token_bytes` | `By` | Bytes the undelivered completions retain. |
| `notify.failures` | `{request}` | Completions the engine would not accept. |
| `oldest_unacked_seconds` | `s` | Age of the oldest completion the worker still owes. |
| `timestamp.out_of_range` | `{timestamp}` | Point timestamps outside the representable range. |
| `memory.budget_bytes` | `By` | Bytes the configuration allows this worker to hold. |
| `memory.accounted_bytes` | `By` | Bytes the worker is accounted as holding now. |

Labelled sets, each with one closed enumeration:

| Metric | Unit | Label | Values |
| --- | --- | --- | --- |
| `flush.count` | `{flush}` | `reason` | `time`, `bytes`, `requests`, `shutdown` |
| `nacks` | `{request}` | `reason` | `storage`, `too_large`, `invalid`, `unsupported`, `shutdown`, `internal` |
| `rows_written`, `files_written` | `{row}`, `{file}` | `dataset` | `logs_series`, `logs_values`, `metrics_series`, `metrics_values` |
| `series_emitted` | `{row}` | `reason` | `new`, `partition`, `rotation` |
| `dropped_unsupported` | `{row}` | `kind` | `exp_histogram`, `summary`, `exemplar` |
| `denormalize.type_mismatch` | `{value}` | `column` | one configured physical column name |

`denormalize.type_mismatch` is the only label that is not an enumeration, and
its values come from the `denormalize` configuration at startup: one metric
set is registered per configured column and no request can add another. Error
strings, object store paths, request ids, series ids and producer ids are
never used as labels, so no workload can grow this node's cardinality.

A series row is counted in `series_emitted` only once the block write returned
success, so an abandoned or failed block credits nothing. `series_emitted`
with `reason=rotation` rising means byte or request rotations inside single
windows are re-emitting descriptors, which is the signal to raise
`window.max_block_bytes` or `window.max_requests_per_block`.
`flush.retries` stays at zero until the sink performs a retry the job can
observe.

### Reading the process residual

`memory.accounted_bytes` reports the retained exporter data and the measured
token allocations, including the cache-entry estimate. `memory.budget_bytes`
reports the configured retained and workspace allowance.

The engine additionally publishes one process-scoped gauge under the same
descriptor name, `memory.unaccounted_rss_bytes`, which is
`max(0, RSS - sum of every worker's accounted bytes)`. Normal conversion and
encoding scratch and allocator overhead appear in this residual, so a non-zero
value is expected. It is published once per process, only while at least one
registered exporter worker exists, and it reuses the engine monitor's single
RSS sample. Watch its trend as well as its absolute value: a residual that
grows while `memory.accounted_bytes` is flat points at allocator retention or
at workspace, not at retained block data.

### Memory model

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

## Limits

### Unsupported signals and points

Traces have no dataset in the lake and are permanently refused on the signal
alone, before any conversion. Points of an unsupported kind, namely
exponential histograms and summaries, are rejected by default;
`unsupported: drop` drops those points instead and counts them in
`dropped_unsupported`. The policy decides the whole request atomically.
Exemplars are always dropped and counted. A request that extracts no rows at
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
- Only an OTLP body's top-level protobuf framing is validated before
  conversion, because the shared byte views decode lazily. Corruption inside
  a nested message is not validated and surfaces as missing fields rather
  than as a refusal.
- Dictionary-encoded OTAP Arrow columns are read through their dictionary,
  never expanded first. Every attribute key and value is charged as it is
  read: one longer than `ingress.max_row_bytes`, or an attribute table whose
  decoded strings and bytes pass `ingress.max_extracted_bytes`, refuses the
  request as too large. A value referenced by many rows therefore cannot grow
  memory before the budgets apply.
- The OTLP receiver hands this exporter the raw request bytes, and the
  shared conversion to Arrow encodes nested map and array attribute values
  with a recursive encoder that has no depth limit of its own. Deep nesting is
  refused by `ingress.max_nesting_depth` only after that conversion.

### Format and storage limits in v1

- `attrs` maps are lossy for readers: values are rendered to strings, so a
  string `"42"` and an integer `42` look the same in the map, although they
  remain different series. Bytes values render as a quoted JSON string of
  lowercase hex; a dedicated `body_bytes` binary column for the log body is
  deferred to a later format version.
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

### Operational limits

- Empirical memory-bound validation, expansion-factor measurement, soak runs
  and benchmarks are not part of this version.
- There is no compaction, no discovery index, no manifest and no producer
  replay id.
- There is no live buffer, tail or series introspection endpoint beyond the
  metrics above.

## Related Docs

- [Lake format](../../../../series-lake/docs/FORMAT.md): the normative on-disk
  format, identity encoding and compatibility rules.
- [series-lake crate](../../../../series-lake/README.md): the
  engine-independent writer this exporter embeds.
- [Core nodes catalog](../../../README.md): every built-in node and its
  feature gate.
- [Configuration guide](../../../../../docs/configuration.md): writing runtime
  YAML.
