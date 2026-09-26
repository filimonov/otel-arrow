# series_parquet exporter: final report of the measurement campaign

Status: final, 2026-09-26.

Every number below comes from a file named in its section. Paths are
relative to the repository root. `FINDINGS.md` is
`docs/superpowers/reports/series-parquet-measurement/FINDINGS.md`. The
evidence tree (run and index JSON, the task reports under `campaign/`) is
archived in the main checkout's ignored
`.measurement-artifacts/evidence/series-parquet-measurement/` and in git
history at commit `75bf2b4a8` under
`docs/superpowers/reports/series-parquet-measurement/`; a bare JSON file
name or `campaign/` path below is relative to that tree. The Task 12 reports
are in `.superpowers/sdd/2026-09-22-series-parquet-measurement/`; the
reference deployment results are the `reference-alloy-*.json` files of the
evidence tree; the canary results are in
`.measurement-artifacts/reference-alloy/canary/`.
A statement the cited files do not support is marked NOT VERIFIED.

## 1. Executive summary

**What was built.** `exporter:series_parquet` is an otap-dataflow exporter
that writes OTLP logs and metrics to an object store as Parquet: a slowly
changing `series` dataset keyed by a content hash, and narrow `values`
datasets that reference it. It holds at most two blocks per worker (ACTIVE
and FLUSHING), keeps no local state and acknowledges its upstream only after
every row of a block is durable in the store (spec sections 1 and 1.1).

**The shipped deployment.** The reference deployment is
`configs/series-parquet-buffered.yaml` with Grafana Alloy running
`configs/series-parquet.alloy`:

```text
log files -> Alloy (file tail, batch 4000, 2 MiB exports, file-backed queue)
          -> OTLP gRPC -> receiver -> durable_buffer (WAL) -> series_parquet -> S3
```

Its contract, stated in the exporter README ("Deploying with Alloy"):

- An OK to the producer means "written to this host's WAL", not "in the
  bucket". It survives a process crash. A host crash or power loss can lose
  about the last 100 ms of acknowledged requests (section 4).
- Delivery is at-least-once. Every duplicate is attributed to an event and
  bounded per event and producer (section 4).
- The producer sees a store outage only when the WAL reaches its size cap;
  then it gets UNAVAILABLE and retries.
- Freshness has no upper bound. The operator alerts on it from outside
  (newest values object per writer) and on WAL fill.
- The strict deployment (`series-parquet-s3.yaml` with
  `series-parquet-strict.alloy`) remains an option. Its OK means the rows are
  in the bucket, at the cost of a producer call held for a window.

**What is proven.**

- No acknowledged record was lost in any judged case: the Task 9-11 failure
  matrices (56 cells, both topologies, MinIO and RustFS), the 13 reference
  deployment cases, the two 30-minute soaks, the chaos dry run, the two 4-hour
  chaos soaks (576M lines each, 30 and 33 fault events) and the cache-pressure
  run (108M lines). Two independent readers (DuckDB
  and clickhouse-local) agree in every case. The one losing case is the
  control with Alloy's in-memory queue, which lost 40,949 lines Alloy had
  read but the engine had not acknowledged.
- Memory stays inside the documented per-worker bound times the stated
  1.25 RSS margin in every reference and canary run (section 3).
- Throughput: 684k records/s sustained on four worker cores into local
  storage, 448k into MinIO and 360k into RustFS (strict, raised receiver
  slots). The buffered topology is bound by the WAL device at 144-152k
  records/s with four workers on one NVMe. 1M records/s was not measured
  (section 2).

**Stores.** Tested on two S3-compatible stores, MinIO and RustFS, including
the multipart path (CreateMultipartUpload, UploadPart, Complete answered 2xx)
and lost or retried completions. Azure Blob Storage passed one smoke case
against Azurite. AWS S3 and other S3-compatible stores are not tested
(backlog P3-6).

**Gates (spec 9.7).**

- Integration ready: met for the measured configurations. The 9.9
  measurements are complete. Part of the 9.5 durable-buffer proof was cut by
  user decision (2026-09-25) and is open as backlog P3-9: acknowledgement
  latency at a 120 s window, mid-window kill replay at both windows, NACK
  backoff timing, lost completion followed by SIGKILL, and a kill within
  100 ms of a block commit.
- Canary ready: met. The churn and the mixed profile each passed a 4-hour
  chaos soak on MinIO, and a 45-minute run held the series cache at its limit
  with sustained evictions (section 6).
- The spec section 9.8 amendment is not applied yet.

## 2. Throughput per core and write speed

Sources: `FINDINGS.md` (Tasks 4, 5, slice S6),
`campaign/reports/task-5-report.md`.

Workload for capacity: 80/20 logs and metric points, 1 KiB bodies, 10k hot
series, 1000 records per request, 256 connections, ZSTD; a winner needs 3 of
3 sustainable 60 s trials (no refusals, no backlog growth, every acknowledged
record stored exactly once). Host: AMD Ryzen 9 9950X, engine pinned to
physical cores 0-7.

Sustainable rate, thousands of records/s:

| Store | Workers | Strict, shipped 128 slots | Strict, 4096 slots | Strict, 15 s window | Buffered |
| --- | --- | --- | --- | --- | --- |
| local | 1 | 128 | 255 | | |
| local | 4 | 288 | 684 | | 144 |
| MinIO | 1 | 128 | 187 | 8.5 | |
| MinIO | 4 | 240 | 448 | 28 | 152 |
| RustFS | 1 | not run | 192 | | |
| RustFS | 4 | 224 | 360 | | |

Durable write speed at the ceiling: local 416 MB/s (684k), MinIO 273 MB/s
(448k), RustFS 220 MB/s (360k).

Per core:

- Engine CPU per record after slice S6: logs 4,205 ns, metrics 2,155 ns
  (Task 4 before S6: 5,345 and 2,453).
- Per worker core: 255k records/s with one worker, 171k with four. Per
  whole-process CPU second: 259k and 213k.
- Four workers give 0.67 of four times one: CPU per record rises 22 percent
  (shared L3 and memory bandwidth, factor 0.81) and the hottest worker
  saturates first (occupancy factor 0.82).

CPU shares (Task 4, before S6, exclusive perf samples):

| Category | Logs | Metrics |
| --- | --- | --- |
| encoding | 33.0% | 11.3% |
| extraction | 16.8% | 24.7% |
| allocator | 12.6% | 15.0% |
| engine runtime | 11.9% | 13.4% |
| sort, seal, merge | 10.7% | 12.9% |
| conversion | 7.5% | 19.8% |
| upload | 5.9% | 1.2% |

After S6, encoding is 39.1 percent of logs CPU, conversion is the largest
metrics category at 24.4 percent, and the logs allocator share fell to
6.6 percent. S6 upload CPU fell from 376.4 to 102.5 ns per logs record
(UNSIGNED-PAYLOAD over TLS).

What limits each topology:

- Strict with shipped slots: the receiver. A request holds its slot until its
  block is durable, so one worker carries at most slots x records per request
  / hold time: 128k/s at a 1 s window, 8.5k/s at the shipped 15 s window.
- Strict with raised slots: the flush on the ingest core. At 722-760k on four
  workers the hottest worker's flush reaches the window and admission closes.
- Buffered: the WAL device. The WAL writes about twice the wire bytes and
  syncs every 25 ms; with the WAL on tmpfs one run sustained at least 256k.
- Reference deployment: one worker was measured at 40k lines/s (8 Alloy
  producers). `max_in_flight` 640 caps a worker at 640 x 4000 / 20 s = 128k
  lines/s of 100-byte lines; the sizing rule is one worker per 100k lines/s
  (104k sustained on MinIO, Task 7).

**The 1M records/s target.** Not measured. The highest sustained rate is 684k
on four worker cores. 1.216M was offered but the run was invalid (the overload
starved the host monitor). From the four-worker figures, 1M/s needs about six
worker cores; no six-worker run was made. The generator was not the limit
(4M/s against a noop exporter). One Alloy producer tails about 30k lines/s
from one file at 1.5 cores, so 1M lines/s through Alloy needs dozens of
producers. The buffered path needs a faster WAL (backlog P1-2) and the strict
path a flush off the ingest core (P1-1) before 1M/s is a realistic target.

## 3. Memory model and bounds

Sources: exporter README ("Memory", "Deploying with Alloy / Sizing"),
`FINDINGS.md` (Tasks 6, 3i, 7), `task-12b-report.md`, `task-12c-report.md`,
reference and canary JSONs.

**Exporter bound.** Per worker, with B `window.max_block_bytes`, E
`ingress.max_extracted_bytes`, C `series_cache.max_entries`, N
`window.max_requests_per_block` and T the completion-token size, retained data
is bounded by `2B + E + 128C + 2NT`, plus a flush workspace reservation. At
the defaults `memory.budget` is 1.64 GB. Merge keys are reserved at admission
and the flush workspace is charged in `memory.accounted` (Task 12b, Task 3i).

**Whole process.** The receiver holds unanswered requests outside the
exporter budget.

- Strict: `workers x (memory.budget + receiver slots x max request size)`;
  the shipped 128 slots x 16 MiB add up to 2 GB per worker. Measured at 4096
  slots on four workers: 16.2 GB allocated against 3.4 GB accounted by the
  exporters.
- Buffered, per worker:
  `memory.budget + max_in_flight x request_bytes + receiver slots x request_bytes`
  = 1.64 + 640 x 2 MiB + 128 x 2 MiB = 3.25 GB (3,250,626,560 B in the canary
  JSON). Budget RSS at 1.25 times this: 4.06 GB.

Measured RSS peak against the buffered bound (one worker, 40k lines/s):

| Run | RSS peak | Ratio to 3.25 GB |
| --- | --- | --- |
| healthy, 30-minute soak (MinIO) | 0.72 GB | 0.22 |
| 150 s store outage (MinIO / RustFS) | 2.55 / 2.60 GB | 0.78 / 0.80 |
| WAL full, 1 GiB cap | 1.93 GB | 0.59 |
| 150 s outage, 4 KiB lines | 4.00 GB | 1.23 |
| chaos 4 h churn r1 / r2 | 3.13 / 3.09 GB | 0.96 / 0.95 |

Earlier 100-byte-line cases ran with `max_in_flight` 1000 and 8 MiB exports;
their in-flight peak was 515, below 640, and they were not rerun.

**The 1.25 margin.** With 4 KiB lines the process high-water mark reached
4.0 GB in the second the store returned, when the buffer sent about 470
bundles at once on top of the exporter's blocks and the allocator's retained
pages. The live formula does not count allocator retention. The margin was
chosen over halving `max_in_flight`, which would cap a worker at about 64k
lines/s. The stacks behind it are not attributed (backlog P1-4).

**Strict soak (Task 7).** 30 minutes at 130,899 records/s: exporter accounted
0.99 of 1.64 GB at the highest fill, RSS 1.31-1.36 GB, receiver in-flight
340 MB at fill, file descriptors flat at 301.

**Where RSS goes (Task 6).** About half of RSS is non-heap, mostly the
binary's file-backed pages (48-50 MB); allocator retention is 30-41 MB. The
memory ledger (RSS minus accounted, control heap, non-heap and retention)
closes at small blocks: 4.3-10.8 MB against the 33.5 MB tolerance. It does
not close in the logs high-rate shape: 2 of 6 pairs pass, and the excess is
one-sample pairing skew between telemetry and allocator prints at a flush's
edges (Task 3i). The engine's `pipeline.memory.usage` counter drifts (up to
10.3 GB against 550 MB RSS) and is not used.

## 4. Delivery contract

Sources: exporter README ("Running behind durable_buffer", "Durability of the
acknowledgement", "Failure behaviour"), `FINDINGS.md` (Tasks 10, 11, 13),
`task-12a-report.md`, `task-12c-report.md`.

**What an OK means.**

| Topology | OK to the producer | Process crash | Host crash or power loss |
| --- | --- | --- | --- |
| buffered (shipped) | written to the WAL, not yet synced | no loss (measured: 8 reference and 6 canary engine SIGKILLs) | can lose acknowledged requests since the last sync, about 100 ms (derived from source, NOT measured) |
| strict | every row of the block is in the bucket | no loss | no loss of acknowledged data |

The WAL syncs a write at least 25 ms after its previous sync, and the buffer's
100 ms tick finalizes and syncs the open segment. The WAL can sync every write,
but `durable_buffer` does not expose that setting (backlog P0-7). Alloy's
file-backed queue with the default `fsync = false` has the same boundary.

**Losses after the WAL acknowledgement.** The producer never sees these; each
is counted.

- A permanent refusal by the exporter (damaged body, over an ingress budget or
  `max_series_per_request`, unsupported kind under `reject`):
  `resolved{outcome="permanently_rejected"}`.
- `drop_oldest` and `max_age`, both off in the shipped config: `loss.*`.
- A bundle that fails conversion: only `conversion_failed`, no `resolved`
  outcome (backlog P0-5).
- The shipped Alloy config cuts lines above 512 KiB so one long line cannot
  refuse its whole export; this changes data and is counted in
  `loki_process_truncated_fields_total` (backlog P0-3).

**Duplicate bounds per event and producer.** Judged per copy: each extra copy
in boot b is charged to the kill that started boot b. Bounds are from
`task-12c-report.md` (fix round 1, F3); measured values from the reference and
canary JSONs.

| Event | Bound per producer | Cause | Largest measured |
| --- | --- | --- | --- |
| engine SIGTERM | 0 | T10-F1 fixed: acknowledgements are recorded before exit | 0 (1 reference, 6 canary restarts) |
| engine SIGKILL | 83,500 | exports in flight (2 x 4000), WAL entries acknowledged within the last 100 ms (T10-F2), one 15 s window of the producer's input | 4,000 (one resent export) |
| Alloy SIGKILL | 70,000 | lines read after the last saved file position (saved every 10 s) | about 10.4k (82,963 over 8 producers) |
| Alloy restart | 54,000 | as above | 0 |
| store latency, 503, outage | 0 | a failed block is rewritten under the same names | 0 |

Two further duplicate paths, measured in Tasks 10-11 and documented:

- A single PUT whose response is lost is nacked and its rows may be stored
  twice.
- A block committed within 100 ms before a SIGKILL may be written again. No
  kill landed there (backlog P3-9).

A lost multipart completion is probed with a HEAD on the frozen name and the
block is acknowledged, counted in `flush.late_commits{outcome=acknowledged}`
(Task 12a, T11-F2).

## 5. Failure behaviour

Sources: `FINDINGS.md` (Tasks 9, 10, 11), `task-9-report.md`,
`task-12c-report.md`, `task-12d-report.md`, reference and canary JSONs.
"Reference" means the Alloy deployment at 40k lines/s on one worker; "canary"
means churn r2 (section 6).

| Failure | Producer sees | Data | Measured |
| --- | --- | --- | --- |
| S3 slowdown | nothing (buffered); a slower ack (strict) | no loss | Task 9: 4 of 4 cells pass, recovery 1.0-1.9 s. Canary: 5 latency events, 123-947 ms for 82-142 s. |
| S3 503 | nothing (buffered) | blocks fail and are retried | Task 9: no loss; 4 of 4 cells failed on F1 orphans, fixed in Task 12a. Canary: 5 bursts of 21-60 s, 0 orphans. |
| S3 outage | nothing until the WAL fills; ack p99 up to 2.5 s | backlog drains after the store returns | Task 9: first values file 1.3-7.7 s after return. Reference: 150 s on MinIO and RustFS, no loss, no duplicates, freshness recovered about 20 s after the store. Canary: 5 outages of 56-114 s. |
| WAL full | UNAVAILABLE, Alloy retries and its tailer parks | lines wait in the log file | Reference, 1 GiB cap: full 53 s after the store stopped, 384 UNAVAILABLE answers, no loss, drained 27 s after recovery. The shipped 32 GiB holds about 34 minutes at 40k lines/s. |
| Engine SIGTERM | UNAVAILABLE while no engine listens | ACTIVE block written, acknowledgements recorded | Task 10 strict: exit in 5.0 s, stored once. Reference: exit in 0.16 s, 0 duplicates. Canary: 5 restarts. |
| Engine SIGKILL | resend of exports in flight | WAL replays after restart | Task 10: no loss. Reference: 8 kills, one 4,000-line duplicate. Canary: 5 kills, 12,000 duplicate lines. |
| Alloy restart or SIGKILL | nothing | file-backed queue survives | Reference: restart 0 duplicates; SIGKILL 82,963 duplicates, 0 loss; the in-memory queue lost 40,949 lines. Canary: 5 restarts. |
| Receiver slots full | UNAVAILABLE, retried | no loss; a request past Alloy's timeout may be stored twice | Before Task 12d the receiver answered RESOURCE_EXHAUSTED and Alloy dropped 8 batches (F-A). After: 0 dropped in a 240 s one-slot probe. |
| Refused connection, reset, NXDOMAIN, silent resolver | nothing (buffered) | retryable flush failure and nack | Task 11: no loss; recovery about 1 s after removal. |
| Lost TCP acknowledgements (35 s) | nothing (buffered) | part upload stalls to its 30 s timeout, one retry | Task 11: no loss. |
| Lost single-PUT response | nothing | the retry gets 200; rows may be stored twice | Task 11: every record stored once in the measured cells. |
| Lost multipart completion | nothing | HEAD probe, block acknowledged, `late_commit` counted | Task 11 on MinIO: detected. RustFS 404 on the abort (T11-F2) caused a duplicate block; fixed in Task 12a. |
| Reset CreateMultipartUpload (T11-F1) | nothing | the upload id is lost; the writer cannot abort it | 54 uploads in three Task 11 cells. Counted in `flush.abort_failures`; the bucket needs an AbortIncompleteMultipartUpload lifecycle rule. |
| Store applies an abandoned request (T11-F3) | nothing | readers see one copy (same names and bytes) | RustFS applied a held completion 145.0 s after the hour ended, against the lateness bound L = 135 s. L holds only when the store drops abandoned requests (MinIO drops them after about 30 s). FORMAT.md states the exception; a compactor needs a seal marker (P2-1). |

Status of the archived failure indexes. The Task 9-11 matrices ran before
the Task 12 fixes; Task 16 reran every failing cell on the fixed build
(`.superpowers/sdd/2026-09-22-series-parquet-measurement/task-16-report.md`):

| Index | Before Task 12 | After (Task 16) | Still failing |
| --- | --- | --- | --- |
| `failure-s3.json` | 7 of 12 | 12 of 12 | none |
| `failure-process.json` | 9 of 12 | 12 of 12 | none |
| `failure-network.json` | 14 of 32 | 29 of 32 | reset buffered MinIO (F-A2), two one-sample RSS band excursions (dns_timeout buffered) |

What the reruns show:

- F1: every phase-2 failure is aborted or counted; one counted failure can
  stand for up to `retry.max_retries + 1` uploads, because object_store retries
  CreateMultipartUpload inside one attempt (54 uploads behind 9 counts).
- F-A2 (open, documented): a Create retried inside an attempt that then
  succeeds leaves uploads the exporter never counts (57 against 54 allowed).
  The AbortIncompleteMultipartUpload lifecycle rule is the only guarantee
  (backlog P3-15).
- T10-F1: 0 records stored again after a graceful restart (10,200 and 2,600
  before).
- T11-F2: a lost completion on RustFS is probed and acknowledged, 0
  duplicates (12,800 and 20,000 before); on MinIO the first attempt's found
  object now counts as a late commit (fixed in Task 16).
- T11-F3: the writer now aborts the upload before a held completion is
  released, so the hour-straddle case on RustFS published nothing late (+51.0 s
  against L = 135 s; +145.0 s before). The store-specific exception in
  FORMAT.md still holds for a completion that lands before the abort.
- T10-F2: the one post-SIGKILL duplicate is a request acknowledged 42.3 ms
  before the kill and replayed from the WAL, inside the documented window.
- Two harness rules changed after a failure, each reviewed: a held completion
  must be the same upload id with a confirmed NotFound before release (a store
  that drops the held request after at least 10 s, as MinIO does at 30 s,
  counts as held), and a lost completion is identified from the proxy log when
  no attempt fails.

## 6. Canary soak (Task 15)

Source: `.measurement-artifacts/reference-alloy/canary/reference-alloy-chaos-minio-churn-dry.json`,
`churn-r1-archive-judgement.json`, `reference-alloy-chaos-minio-churn-r2.json`.

Setup: the shipped configs with site substitutions only; 8 Alloy v1.19.2
producers at 5,000 lines/s each (40k lines/s); one worker (engine on CPUs
0-3,16-19, worker core 2); MinIO; a seeded chaos schedule. Acceptance (spec
10.3): after drain no acknowledged line missing, duplicates within their
bounds, no unexplained orphans, RSS p99 of the last hour within 1.15 of the
second hour, resources within the documented bounds.

Churn profile: 125,000 distinct series per producer (1M in total) against a
200,000-entry cache.

| | Dry run | Churn r1 | Churn r2 |
| --- | --- | --- | --- |
| Build | dbb10f81b (dirty) | not recorded in the judgement file | 8d9a3e350 |
| Input | 1,800 s | 14,400 s | 14,400 s |
| Chaos events, engine boots | 6, 3 | 30, 11 | 30, 11 |
| Lines written | 71,995,032 | 575,995,040 | 575,994,592 |
| Delivery oracle (DuckDB + ClickHouse) | pass | not judged: the read-back ran out of DuckDB memory | pass |
| Duplicate lines | 20,000 (one engine SIGKILL, 5 producers x 4000) | not judged | 12,000 (two SIGKILLs, 3 exports) |
| Incomplete uploads, abort failures | 0, 1 | not judged, 1 | 0, 0 |
| Buffer loss, permanent rejections | 0, 0 | 0, 0 | 0, 0 |
| Flush failures, buffer retries | 6, 1,090 | 28, not recorded | 28, 5,310 |
| Alloy "Dropping data" lines | 0 | not recorded | 0 |
| Ack latency p50 / p99 | <= 25 ms / <= 500 ms | not recorded | <= 25 ms / <= 250 ms |
| Freshness p50 / p99 / max | 10.6 / 88.9 / 102.7 s | not recorded | 10.3 / 78.7 / 138.0 s |
| RSS peak (VmHWM) | 2.55 GB | 3.13 GB | 3.09 GB |
| Exporter accounted peak (budget 1.64 GB) | 0.99 GB | 1.01 GB | 1.01 GB |
| WAL used peak | 1.51 GB | 1.97 GB | 1.93 GB |
| Series cache peak, evictions | 200,000, 452,442 | 188,806, 0 | 188,852, 0 |
| RSS trend, last hour / second hour p99 | not gating (30 min) | 1.196 (fails 1.15) | 1.013 (1 s samples), 1.016 (10 s) |
| Verdict | passed | failed (RSS trend; delivery not judged) | passed, all 11 checks |

Churn r2 schedule (seed 15002): five each of engine SIGKILL, engine SIGTERM,
Alloy restart, HTTP 503 burst (21-60 s), store outage (56-114 s) and S3
latency (123-947 ms for 82-142 s). Freshness recovered after all 30 events.
Every duplicate run is one 4,000-line export resent after an engine SIGKILL,
against a bound of 83,500 per kill and producer.

**The r1 RSS trend.** r1 sampled RSS every 10 s. Its quiet-sample p99 per hour
was 665, 569, 685 and 680 MB, so the gate compared the last hour with a low
second hour. r2 reran the same seed and schedule with 1 s samples and got
1.013 (1.016 on 10 s samples). The r1 excursion was not reproduced; its cause
is NOT VERIFIED (no heap dump was taken).

**Cache pressure.** The 4-hour churn runs never filled the cache (peak
188,852 of 200,000, 0 evictions); the 30-minute dry run filled it and evicted
452,442 entries. The difference is not explained in the cited files, so the
4-hour run does not establish behaviour under sustained eviction.

**Mixed profile** (seed 15003, one record in five on the churn window, the
rest on 10k hot series): 14,400 s, 575,994,656 lines, 33 events, 12 boots.
Passed all 11 checks after one re-judgement: two short store outages (31 and
35 s) were logged by the proxy as 499 (the client gave up) rather than 502,
and the observer now counts 499 inside an executed outage (reviewed).
0 duplicates, 0 orphans, 0 buffer loss; RSS peak 3.00 GB, exporter accounted
0.96 GB, WAL peak 1.99 GB (a 120 s outage against the 2.25 GB sizing rule);
RSS trend 0.980 on 1 s samples (1.017 on 10 s); freshness p99 62 s, max 142 s,
every event recovered within 31.8 s; series cache at its 200,000 limit with
43,872 evictions.

**Cache pressure** (seed 15004, 1M distinct series in 45 minutes against the
shipped 200,000-entry cache): 2,700 s, 107,994,976 lines, 6 events. Passed all
11 checks: entries held at exactly 200,000, 483,680 evictions (179 per second
over about 36 minutes), hit ratio 0.65, 20,000 duplicate lines from one
SIGKILL (5 resent exports), RSS peak 2.27 GB.

Why the 4-hour churn runs never evicted: the cache is empty at every engine
boot and the chaos schedule restarted the engine every 5-43 minutes, while
1M distinct series over 4 hours (69 new per second) needs about 48 minutes to
fill 200,000 entries. The mixed run and the pressure run cover eviction under
pressure.

Not run: the stable profile as a long run (the 30-minute reference soak
covers healthy steady load), RustFS for the long run (one store by user
decision), the metrics signal through Alloy, and 24-72 h qualification
(backlog P3-8).

## 7. What changed in plan 3

Task 12 fixes, one line each (commits in the named reports):

- 12a T10-F1: the durable buffer records the exporter's acknowledgements
  during a graceful shutdown (engine completion phase `shutdown_completions`,
  progress persisted before the storage engine stops).
- 12a F1: a multipart upload that fails while finishing is aborted within
  `upload.abort_timeout`; every possible orphan is counted per attempt in
  `flush.abort_failures`.
- 12a T11-F1: a CreateMultipartUpload without a definite answer counts as a
  possible orphan, and abort failures name the object key.
- 12a T11-F2: after a lost completion the writer HEADs the frozen name before
  aborting; a committed block is acknowledged and counted as a late commit.
- 12b: `ingress.max_series_per_request` with a measured per-series charge and
  the startup inequality `2E + S x F_max + T <= B`.
- 12b: merge keys are reserved at admission; values sort keys are limited to
  the lake's sortable column types.
- 12b: an oversized completion token is a permanent `token_too_large`
  refusal.
- 12b: shipped receiver `max_decoding_message_size` 16 MiB, equal to
  `ingress.max_request_bytes` (test-enforced).
- 12b: README whole-process memory bound.
- 12c: `series-parquet-buffered.yaml` is the shipped default; the Alloy
  reference config with a file-backed queue, 2 MiB exports and 512 KiB line
  truncation; `max_in_flight` 640.
- 12c: operator guide "Deploying with Alloy" (sizing, WAL device, bucket
  permissions and lifecycle, alerts, failure table) and the 13 validation
  cases.
- 12d: OTLP receiver refusals at the concurrency limit, rate limit or memory
  pressure answer UNAVAILABLE and are counted; oversize and burst excess
  answer INVALID_ARGUMENT; HTTP 503 carries `Retry-After: 1`.
- 12e: the durable buffer keeps a reserve for its final persist and releases
  its storage engine off the pipeline thread.
- 12e: a closed pdata channel no longer replaces a queued Shutdown; the
  completion phase honours a tighter Shutdown and a failed completion.
- 12f: S3 `unsigned_payload` defaults to on over TLS only, following the
  resolved endpoint and `AWS_UNSIGNED_PAYLOAD`.
- 12f: the no-dictionary override reaches the nested `attrs.entries.values`
  column.
- 12f: partial retry sections, hyphenated writer ids, a one-day window cap,
  a panicked flush as Internal, `flush.late_commits{outcome}`, rate-limited
  notices.
- 12g: Azure takes an explicit blob service endpoint; an Azurite smoke over
  HTTPS with a bearer token stores 12 logs and 6 metrics requests exactly
  once, read back by DuckDB and ClickHouse.

Earlier plan-3 changes, one line each (from `FINDINGS.md`):

- 3b: store outages surface as deadline outcomes with their last error;
  shutdown drains are not cut by Tokio's cooperative budget.
- 3c: content-keyed metrics memo (metrics extraction 1357.5 to 919.7
  ns/record); the opt-in `series-parquet` feature.
- 3d: format revision 2, native Parquet `SortingColumn` metadata.
- 3g and S2: a schema-aware OTLP framing check refuses damaged bodies that
  were acknowledged before.
- 3i: bounded flush slices (longest stall 49-63 ms down to 20-25 ms, output
  bytes identical); `flush.workspace` measured and charged.
- S5: decoded attribute values are charged to the request budget before they
  are allocated.
- S6: extraction and write speed (logs engine CPU -21.3 percent, metrics
  -12.1 percent).
- S7: shutdown decides every held request once, by the deadline plus
  `upload.abort_timeout`.
- S8: late commits detected by bounded HEAD probes; `unsupported` defaults to
  drop.
- `df_engine` on Linux gnu runs jemalloc with its background thread (quiet
  RSS spread from 15-34 to 1.4-6.6 percent).

## 8. Known limits and the backlog

Limits of this report:

- 1M records/s was not measured (section 2).
- One network cell fails on a documented limit (F-A2), two on one-sample RSS
  band excursions (section 5).
- The long runs cover one store, one worker, logs only, 40k lines/s; no
  engine kill landed within 100 ms of a block commit.
- The stage baselines were recorded with a TLA+ model checker loading the
  other half of the CPU package; a quiet host looks about 5 percent faster.
- The memory ledger does not close in the logs high-rate shape (section 3).
- Freshness has no internal gauge; alerts are external (P0-2).

Backlog, `docs/superpowers/plans/2026-09-23-plan-4-backlog.md`, one line
each.

P0, before production:

- P0-1 Acks to retry and fanout processors after they exit are dropped, so
  the buffer replays written bundles.
- P0-2 An end-to-end freshness gauge (oldest pending age in the buffer).
- P0-3 Refuse permanently invalid requests before the WAL acknowledges them;
  remove the Alloy line truncation.
- P0-4 Validate OTLP framing once, for every converting node.
- P0-5 A conversion failure in durable_buffer gets a `resolved` outcome.
- P0-6 The OTAP Arrow receiver's RESOURCE_EXHAUSTED becomes UNAVAILABLE.
- P0-7 A per-write WAL sync option, with its measured cost.

P1, performance toward 1M records/s:

- P1-1 Shared writer with parallel preparation, flush off the ingest core.
- P1-2 WAL throughput: finalization off the worker runtime, configurable
  sync and segment size.
- P1-3 CPU candidates: fixed-width sort keys, extraction from OTLP bytes and
  others.
- P1-4 jemalloc heap-dump attribution of the residual and the 1.25 margin.
- P1-5 A receiver in-flight byte bound.
- P1-6 Re-measure S6 encode after the ColumnPath fix.
- P1-7 Measure conversion memory before the request budget.
- P1-8 Hourly partition rollover under 100k+ hot series.
- P1-9 A rotation trigger on the oldest pending request's age.
- P1-10 Separate target and maximum block bytes, or split oversize requests.
- P1-11 Resumable extraction, only if the responsiveness bound fails.

P2, format and data:

- P2-1 Compactor with seal markers.
- P2-2 Format batch 2 (identity-config guard, dedup key, v2 path).
- P2-3 Data-model coverage decisions.
- P2-4 Typed attribute maps.
- P2-5 Day or hour partitions.
- P2-6 Idempotent replay by producer batch id.
- P2-7 Producer id from transport headers.
- P2-8 Lossless high-cardinality mode.
- P2-9 One refusal policy for invalid UTF-8 and repeated singular fields.
- P2-10 Commit manifests.
- P2-11 Discovery index.

P3, hygiene, upstream and harness:

- P3-1 Complexity-review refactors.
- P3-2 Engine upstream issues (`pipeline.memory.usage` drift and others).
- P3-3 Receiver refusal hygiene.
- P3-4 Persist the WAL cursor with the segment (T10-F2).
- P3-5 Exporter operability leftovers.
- P3-6 S3 store matrix: AWS S3, Ceph RGW, Garage, SeaweedFS.
- P3-7 Tests for guarantees without one.
- P3-8 24-72 h qualification soak and a nightly lane.
- P3-9 Task 13 proofs not run (120 s window latency and the others in
  section 1).
- P3-10 Harness polish.
- P3-11 Metric sets grouping.
- P3-12 Upstream PR logistics (de-slop, clean branch).
- P3-13 Engine settles contexts when a node task dies.
- P3-14 durable_buffer directory ownership.
- P3-15 Count every CreateMultipartUpload retry that leaves an upload (F-A2).

Deferred features: live access to buffered data, traces and the other
unsupported kinds, a spatial aggregation processor, query-side indexes.

## 9. Evidence

- `docs/superpowers/reports/series-parquet-measurement/FINDINGS.md`: Tasks
  3-11, 13 and slice S6, with their JSON indexes (`stages.json`,
  `attribution.json`, `capacity-*.json`, `memory-*.json`, `soak-*.json`,
  `failure-s3.json`, `failure-process.json`, `failure-network.json`).
- `campaign/reports/` of the evidence tree:
  task reports 1-13.
- `.superpowers/sdd/2026-09-22-series-parquet-measurement/task-12a-report.md`
  to `task-12f-report.md`; Task 12g in `progress.md` (no separate report).
- `reference-alloy-*.json` of the evidence tree:
  the 13 reference deployment cases and their reruns; raw archives in
  `.measurement-artifacts/reference-alloy/<case>-<store>-<epoch>/`.
- `.measurement-artifacts/reference-alloy/canary/`: the chaos dry run, the
  churn r1 archive judgement, churn r2, mixed r1 and the cache-pressure run;
  run logs in `run-logs/`.
- `.superpowers/sdd/2026-09-22-series-parquet-measurement/task-15-report.md`
  (canary) and `task-16-report.md` (failure-cell reruns on the fixed build).
- `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet_exporter/README.md`:
  "Deploying with Alloy", "Memory".
- `configs/series-parquet-buffered.yaml`, `configs/series-parquet.alloy`,
  `configs/series-parquet-s3.yaml`, `configs/series-parquet-strict.alloy`.
- `docs/superpowers/plans/2026-09-23-plan-4-backlog.md`.
