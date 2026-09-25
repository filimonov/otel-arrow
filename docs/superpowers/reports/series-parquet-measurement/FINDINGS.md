# series_parquet measurement campaign: findings

This document states what the measurement campaign has established so far
about the series_parquet exporter. It covers Tasks 3, 4, 6 and 3i of the
plan `docs/superpowers/plans/2026-09-22-series-parquet-measurement.md`, and
lists what the other finished tasks established. Every number comes from a
task report or from a committed JSON file in this directory.

All evidence paths below are relative to this directory,
`docs/superpowers/reports/series-parquet-measurement/`, unless they start
with `docs/`. The task reports are in `campaign/reports/`. The controller's
rulings are in `campaign/ledger.md`.

## The exporter

series_parquet is an exporter node of the otap-dataflow engine
(`df_engine`). It receives OTLP (OpenTelemetry Protocol) logs and metrics.
Each request is converted to OTAP (OpenTelemetry Arrow Protocol) Arrow
records. Extraction then splits every record into a series descriptor,
identified by a hash called `series_id`, and a values row. A series cache
remembers descriptors already written, so a known series is not written
again. Rows are admitted into an in-memory block. When the block is sealed,
its runs are sorted and merged, encoded as Parquet with ZSTD (Zstandard)
compression, and written as a series file and a values file to an object
store: S3 (here MinIO) or the local file system. A request is acknowledged
only after its rows are stored.

The stage names used below follow that path: `convert`, `extract`,
`sort_seal`, `merge`, `encode`, `upload` or `local_write`, and `sink`, which
is merge plus encode plus store for one sealed block.

## Conditions common to all runs

| Item | Value |
| --- | --- |
| Host | AMD Ryzen 9 9950X, 16 physical cores, 32 logical CPUs |
| SMT siblings | logical CPU i and i+16 share one physical core |
| Pin | `taskset -c 0-7,16-23`, physical cores 0-7 with both SMT threads |
| Other load | a TLA+ model checker (TLC) session pinned to 8-15,24-31, 8 workers, nice 10 |
| Engine builds | `release` with jemalloc; `profiling` with the DHAT heap profiler |
| Bench builds | profile `bench` (release plus fat LTO); a timing build on jemalloc with the engine's background thread, and a heap build with the `bench-heap` feature (DHAT) |
| Host lease | an exclusive file lock; a measured run holds it for its whole window |
| Build monitor | a run is invalidated if cargo, rustc, cc1 or ld runs inside a measured window |
| Snapshots | every child records the host at start and end: load, affinity, heaviest other processes |

Stage timings run on jemalloc, configured as the engine is, from commit
d7320d681 and the family `stages-spot-jemalloc` on. Every earlier stage
family, including all of Task 3 and slice S6, timed on glibc malloc; its
baselines carry the `system` allocator in their fingerprint and are never
compared with a jemalloc run. The two S6 families recorded with
`GLIBC_TUNABLES` stay committed as glibc evidence.

SMT means simultaneous multithreading. The pin exists because a TLC thread
on the SMT sibling of a bench core slowed the Task 3 benches by 8 to 47
percent. The ledger ruling of 2026-09-22 23:3x makes the pin mandatory for
every measured launch. Timings taken before the pin are not comparable with
timings taken under it. Counts, verdicts and correctness results from before
the pin still stand.

Each role gets its own physical core, and SMT siblings are never shared
between roles. A publishable run needs at least 8 physical cores. The
allocation is per case, so a case claims only the roles it runs.

Baseline policy (ledger ruling R-T1, plan "Controller baseline policy"):

- The first valid run of a fingerprint writes an immutable baseline.
- The fingerprint covers machine, core allocation, effective configuration,
  workload and build profile.
- A later run with the same fingerprint fails if it regresses by more than
  25 percent.
- A different fingerprint writes a new baseline instead of comparing.
- Correctness, measurement validity and the unexplained memory residual are
  hard checks on every run, whatever the baseline says.
- An aggregate of three repetitions is refused, and writes no baseline, if a
  primary metric has a coefficient of variation (CV) above 0.15.

A refused aggregate is kept as evidence. It is never rerun until green. Its
cause goes to Task 12, the plan's contingency task for bounded fixes.
Architectural causes go to plan 4, `docs/superpowers/plans/2026-09-23-plan-4-backlog.md`.

The rename of the build feature to `series-parquet` in Task 3c changed every
build fingerprint. Later tasks therefore compare with the Task 3 family by
explicit reference and hash, not by fingerprint match.

## Task 3: stage benchmarks

### Question

What does each stage cost per record, on one core, in CPU time, wall time,
allocation, memory and output size? The plan's acceptance for Task 3: every
registered stage with its full metric set, Criterion distributions,
conversion and encoder expansion, equivalence counts and three repetitions,
with failure on wrong output, hidden timed work, too few samples, unstable
repetitions, a hard residual check or a regression.

### Method

- Two bench targets in `crates/series-lake`: `measurement`, one process per
  stage, repetition and profile; and `layered`, cumulative Criterion groups.
- Every stage calls production code. Only the measured operation is inside
  the timer. Setup, such as warming the series cache or building a `Sink`,
  is done before it.
- A timing process takes at least 30 samples and one second of measured
  work. A heap process runs under DHAT and reports allocation and workspace.
- The Criterion ladder is cumulative: `otlp_noop`, `otlp_convert`,
  `otlp_extract_hash`, `otlp_sort`, `otlp_parquet_local`,
  `otlp_parquet_zstd`. `otlp_minio` runs the same ladder through the real
  `Sink` into a pinned MinIO container.
- `otlp_noop` in mode `pipeline` is a real engine with the noop exporter,
  driven by the Python producer.
- Every rate is per logical upstream record: log records or metric points.
- The encoder used for diagnosis is checked against the real `Sink` output:
  schema, values, row groups and per-column codec, statistics and dictionary.

Workloads:

| Id | Signal | Requests | Records | Series | Input bytes |
| --- | --- | --- | --- | --- | --- |
| logs-1k-stable | logs, 1 KiB bodies | 200 | 20,000 | 100 | 21,638,200 |
| metrics-mixed | gauges, sums, histograms | 150 | 15,000 | 600 | 908,550 |
| logs-8k-churn-wide | logs, wide sort key, new series every request | 120 | 6,000 | 120 | 49,504,920 |
| logs-512k-near-limit | logs, 512 KiB bodies | 24 | 96 | 10 | 50,339,640 |

The committed family ran at revision 9d4fd4ad5 and was committed in
42be4c5d3, under the pin, on 2026-09-22 from 23:52 for 795 s. Stages family
roles: observability core 0, bench core 1, engine reservation 2-4, producer
5-6, store 7.

| Result | Value |
| --- | --- |
| Children | 312 of 312 passed |
| Aggregates | 102 of 104 passed |
| Baselines written | 102 |
| Registered stage results | 138 |
| Index status | failed, on two aggregates, as designed |

### Results: isolated stages

CPU ns per record, median of three repetitions, from `stages.json`. Encode
and sink rows use ZSTD unless marked.

| Stage | logs-1k-stable | metrics-mixed | logs-8k-churn-wide | logs-512k-near-limit |
| --- | --- | --- | --- | --- |
| convert | 389.1 | 505.3 | not run | not run |
| extract | 1,114.9 | 1,367.9 | 2,190.8 | 66,305.6 |
| sort_seal | 473.2 | 350.8 | 2,333.6 | 78,795.2 |
| merge | 136.8 | 104.7 | 3,389.9 | 13,968.4 |
| encode, none | 1,090.3 | 166.9 | 4,769.8 | 283,879.0 |
| encode, zstd | 1,913.5 | 225.5 | 8,474.6 | 563,986.5 |
| sink, local store | 2,250.9 | 353.0 | 14,484.4 | 718,202.7 |

### Results: cumulative ladder and store stages

Medians from `stages.json`. The ladder rows are Criterion groups.

| Stage | logs CPU ns/record | logs wall ns/record | metrics CPU ns/record | metrics wall ns/record |
| --- | --- | --- | --- | --- |
| otlp_convert | 392.6 | 396.2 | 499.4 | 508.6 |
| otlp_extract_hash | 1,482.4 | 1,481.8 | 1,870.8 | 1,885.7 |
| otlp_sort | 2,314.9 | 2,269.5 | 2,424.8 | 2,800.6 |
| otlp_parquet_local | 3,442.9 | 3,472.0 | 2,644.0 | 2,669.0 |
| otlp_parquet_zstd | 4,247.4 | 4,269.0 | 2,695.1 | 2,685.2 |
| otlp_minio | 4,842.5 | 8,038.0 | 2,993.1 | 4,116.5 |
| upload, pre-encoded bytes | 402.2 | 5,384.6 | 15.9 | 1,060.1 |
| local_write, pre-encoded bytes | 108.9 | 108.9 | 2.6 | 2.6 |
| otlp_noop, pipeline | 1,738.4 | 121,193.3 | 312.2 | 6,346.2 |

`otlp_minio` runs at 206,506 records/s/core on logs-1k-stable and 334,104 on
metrics-mixed. The gap between its wall and CPU time is store wait that the
CPU clock does not see. The Criterion `otlp_noop` floor is 0.02 ns/record.

### Results: memory and output size, logs-1k-stable

Medians from `stages.json`. Workspace is what DHAT saw the stage allocate
above its fixtures. MiB values are converted from bytes.

| Stage | Allocated B/record | Peak workspace MiB | Peak RSS MiB | Output B/record | Output form |
| --- | --- | --- | --- | --- | --- |
| convert | 5,175.5 | 40.2 | 78.5 | 1,997.0 | Arrow records |
| extract | 9,797.1 | 49.6 | 81.7 | 2,505.2 | extracted rows |
| sort_seal | 2,690.9 | 26.0 | 109.6 | 1,159.1 | sealed block |
| merge | 1,346.7 | 17.0 | 145.0 | 1,168.7 | merged chunks |
| encode, none | 7,162.3 | 51.8 | 142.9 | 1,065.1 | Parquet bytes |
| encode, zstd | 8,727.8 | 34.6 | 152.7 | 755.3 | Parquet bytes |
| sink | 10,077.2 | 35.2 | 217.9 | 755.3 | stored objects |

RSS is resident set size. The largest workspaces are on the large fixtures:

| Stage | Workload | Allocated B/record | Peak workspace MiB | Peak RSS MiB |
| --- | --- | --- | --- | --- |
| sink | logs-8k-churn-wide | 95,180.9 | 153.2 | 332.3 |
| sink | logs-512k-near-limit | 4,137,306.4 | 106.6 | 263.6 |
| encode, none | logs-512k-near-limit | 3,489,247.2 | 140.6 | 252.6 |
| encode, zstd | logs-8k-churn-wide | 69,465.9 | 104.6 | 287.9 |

### Conclusions

- Encoding is the most expensive stage for logs. Extraction is the most
  expensive for metrics.
- ZSTD Parquet is 755 B per 1 KiB log record. Uncompressed Parquet is
  1,065 B, so ZSTD is 1.41 times smaller.
- ZSTD encoding costs about 1.8 times the CPU of uncompressed encoding on
  logs-1k-stable. This is a simple ratio of 1,913.5 to 1,090.3.
- Conversion expands a 1 KiB wire record to 1,997 bytes of Arrow, about 1.85
  times the wire record.
- The whole path to MinIO costs 4,842.5 CPU ns per logs record on one core.
  As a simple ratio, 1M records/s would need about 4.8 cores of this work
  for logs-1k-stable. Task 5 measures the real figure under fan-in.
- The pipeline baseline's CPU is an order-of-magnitude calibration only. Its
  input window is about 0.1 s on metrics-mixed, where the engine spends about
  5 ms of CPU, so one stray millisecond is a fifth of the measurement. Its
  wall time measures the Python producer, not the engine. Ledger ruling:
  "dose problem, not a clock problem".
- `records_per_s_per_core` of `upload` and `local_write` is a per-record
  rate over pre-encoded input. These stages pay per object. Their results
  carry object and byte rates, which are the right rates to use.
- Completion of a store stage means the store reported the object written
  and the bench read it back. It is not a power-loss durability claim, since
  `object_store` does not fsync its local backend.

### Why these numbers are trusted

The committed family is the fourth attempt. Each earlier one is superseded.

| Family | Commit | Host | Status |
| --- | --- | --- | --- |
| first run | 5c7c49e98 | quiet, unpinned | superseded by fix round 1: setup was inside timers |
| fix round 1 | b0a397f8d | quiet, unpinned | superseded: Criterion retries read stale data |
| fix round 2 | 4f8ff1257 | loaded, TLC on SMT siblings | superseded: 95 provisional baselines, 8 to 47 percent slow |
| fix round 3 | 42be4c5d3 | pinned | the family of record |

Seven of the nine refusals of the loaded family vanished under the pin. That
supports the attribution of the slowdown to host load. The pinned medians
are still about 5 percent above the quiet host of fix round 1, and the most
memory-bound rows are 10 to 29 percent above it. Encode on logs-1k-stable is
+18.8 percent. TLC on the other eight cores shares the package's memory
controller and power budget. That is a plausible cause, not a proven one.
The baselines of record therefore encode "TLC running on the other half of
the package". A quiet-host run will look faster against them.

Fix round 3 found that the series cache and `Sink` teardown were timed. A
scratch program put that teardown at 0.04 to 0.3 percent of a stage, at most
1.4 ns/record. The committed numbers carry this defect, but it is far inside
run-to-run spread.

### Deferred

| Item | Where | Reason |
| --- | --- | --- |
| encode logs-1k-stable zstd timing: peak RSS CV 0.21, CPU CV 0.09 | Task 12 | third sighting of a first-repetition RSS high; investigate allocator growth or run order, not a rerun |
| upload logs-1k-stable zstd timing: wall CV 0.21, CPU CV 0.01 | Task 12 | store-bound wall variance, seen before TLC started |
| pipeline baseline CPU window | Task 12 | enlarge the dose to several seconds of engine CPU; do not change the clock or the rule |
| committed index says setup starts rustc once; it starts it five times, each only for `--version` | recorded, not corrected | no tool re-aggregates an immutable family |
| quiet-host re-measurement of the family | Task 12 | the baselines encode a half-loaded package |

### Evidence

- `stages.json`: the index, 46 stage summaries, 138 stage results, 102
  baselines.
- `stages-encode-isolated-logs-1k-stable-zstd-timing-f001.json` and
  `stages-upload-async-logs-1k-stable-zstd-timing-f001.json`: the two
  refused aggregates.
- `stages-otlp_noop-pipeline-metrics-mixed-zstd-pipeline-f001.json`: the
  pipeline baseline aggregate.
- `campaign/reports/task-3-report.md`: fix rounds 1 to 3 and the tables of
  all families.

## Task 4: CPU attribution per record

### Question

Where does the engine spend its CPU per record, measured directly in the
running engine, and does that agree with the stage benches? The plan's
acceptance: exclusive sample shares, CPU per record, upload wait, the
unknown residual, profile overhead and three repetitions, with failure on
double counting, fewer than 10,000 classified samples, missing perf data or
an invalid reconciliation.

### Method

- `perf record -e cpu-clock -F 199 -g --call-graph dwarf` on the engine,
  enabled only from the first request to the last durable acknowledgement.
  perf runs on its own profiler core.
- Each sample is assigned exactly once. The innermost frame that matches a
  production namespace decides it. Runtime libraries such as Tokio decide a
  sample only when no production frame is on its stack.
- Each repetition runs an unprofiled control lifetime and a profiled
  lifetime of the same release engine, with a pinned MinIO, blocks of 128
  requests and 256 requests in flight.
- Binding reconciliation, the plan's stage agreement row: exclusive CPU plus
  the named residual must be within 10 percent of the measured engine CPU,
  and unexplained CPU at most 20 percent. This is a hard gate in every
  repetition.
- The per-stage comparison with the Task 3 family is descriptive and does
  not gate. Its reference is the pinned `stages.json`, verified by hash. The
  spot family `stages-spot.json`, measured at 936ace03a after the Task 3c
  memo, is shown beside it.
- The profiled binary is `df_engine-perf`: the same objects relinked with
  `-z separate-loadable-segments`, because lld's segment layout stops perf's
  unwinder after one frame. Both binaries list the same 191,989 functions
  with the same symbol digest.

Conditions: family f002 at revision 865b7dfc7, under the pin,
`perf_event_paranoid` 1, 3 repetitions per workload, ledgers on tmpfs. One
logs repetition was invalidated by another agent's rustc and rerun as r005.

### Results

Engine totals, f002 aggregate medians:

| Metric | logs-1k-stable | metrics-mixed |
| --- | --- | --- |
| Engine CPU ns/record, profiled | 5,345.2 | 2,452.6 |
| Engine CPU ns/record, control | 5,449.8 | 2,465.7 |
| Throughput, records/s, profiled | 104,087 | 225,370 |
| Classified samples, 3 repetitions | 16,518 | 14,818 |
| Stage agreement error per repetition | 0.19 / 0.26 / 0.26% | 0.30 / 0.30 / 0.23% |
| Unexplained CPU per repetition | 1.6 / 1.7 / 1.5% | 0.4 / 0.4 / 0.4% |
| Profile overhead, CPU per record | -1.9% | +0.1% |
| Flush wall s | 35.2 | 18.7 |
| Upload wait s, flush wall not spent on flush CPU | 24.7 | 14.5 |
| Peak RSS bytes | 150,446,080 | 113,094,656 |
| Aggregate status | failed, `rss_reconciliation` only | passed, baseline written |

Exclusive CPU shares, pooled over 3 repetitions, 95 percent binomial
interval, with the aggregate median in ns per record:

| Category | logs share | logs ns/record | metrics share | metrics ns/record |
| --- | --- | --- | --- | --- |
| encoding | 33.0% +-0.7 | 1,749.4 | 11.3% +-0.5 | 276.9 |
| extraction | 16.8% +-0.6 | 911.8 | 24.7% +-0.7 | 615.5 |
| allocator | 12.6% +-0.5 | 682.4 | 15.0% +-0.6 | 368.9 |
| engine_runtime | 11.9% +-0.5 | 632.9 | 13.4% +-0.5 | 330.4 |
| sort_seal_merge | 10.7% +-0.5 | 574.6 | 12.9% +-0.5 | 327.6 |
| conversion | 7.5% +-0.4 | 382.7 | 19.8% +-0.6 | 488.2 |
| upload | 5.9% +-0.4 | 322.4 | 1.2% +-0.2 | 33.3 |
| buffer, admission | 0.3% +-0.1 | 18.8 | 1.5% +-0.2 | 39.3 |
| unknown | 1.4% +-0.2 | 75.2 | 0.1% +-0.1 | 2.5 |

Descriptive comparison with the stage benches, CPU ns per record. The
attributed cost includes the allocator samples each row's stages called.

| Row | Workload | Attributed | Pinned bench | Ratio | Spot bench | Ratio | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| conversion | logs | 447.6 | 389.1 | 1.15 | 383.1 | 1.17 | in band |
| extraction | logs | 1,200.7 | 1,114.9 | 1.08 | 1,203.1 | 1.00 | in band |
| admission, sort_seal, merge | logs | 624.3 | 610.0 | 1.02 | 602.2 | 1.04 | in band |
| encoding | logs | 1,786.5 | 1,913.5 | 0.93 | 1,633.1 | 1.09 | in band |
| upload | logs | 379.2 | 402.2 | 0.94 | 405.2 | 0.94 | in band |
| engine_runtime | logs | 730.9 | 1,738.4 | 0.42 | none | none | unexplained |
| total | logs | 5,345.2 | 6,580.9 | 0.81 | none | none | in band |
| conversion | metrics | 569.4 | 505.4 | 1.13 | 506.4 | 1.12 | in band |
| extraction | metrics | 777.2 | 1,367.9 | 0.57 | 791.1 | 0.98 | in band |
| admission, sort_seal, merge | metrics | 417.0 | 455.6 | 0.92 | 486.0 | 0.86 | in band |
| encoding | metrics | 307.8 | 225.6 | 1.36 | 226.0 | 1.36 | in band |
| upload | metrics | 36.1 | 15.9 | 2.27 | 14.2 | 2.54 | unexplained |
| engine_runtime | metrics | 369.8 | 312.2 | 1.18 | none | none | in band |
| total | metrics | 2,452.6 | 3,305.3 | 0.74 | none | none | in band |

The band is 0.5 to 2. The engine_runtime reference is the pipeline baseline
of Task 3, and the total reference is that baseline plus `otlp_minio`.

### Conclusions

- The attribution is valid. Exclusive CPU plus the named residual matches
  the engine's scheduler CPU to within 0.3 percent in every repetition.
- Encoding is a third of logs CPU. For metrics, extraction is 24.7 percent
  and conversion 19.8 percent.
- The allocator is 12.6 to 15.0 percent of CPU. Extraction is its largest
  caller: 5.1 percent of logs CPU and 6.0 percent of metrics CPU in family
  f001.
- The Task 3c metrics memo is visible. Metrics extraction is 0.57 of the
  pre-memo pinned bench and 0.98 of the post-memo spot bench.
- The engine worker is about half busy. The Python sender and the durable
  acknowledgement pipeline bound the rate, not the engine. In f001 the
  worker was on-CPU 0.44 of the time for logs and 0.52 for metrics.
- perf's own cost is below repetition noise.
- Two descriptive rows are outside the band and marked unexplained. Logs
  engine_runtime is 0.42 of the pipeline baseline, whose CPU is itself only
  a calibration. Metrics upload is 36 against 16 ns/record, on 1.2 percent
  of the profile.
- `attribution.json` is failed. The only failing gate is the logs
  `rss_reconciliation`: its residual peaks at 48 to 65 MB against a 33.5 MB
  tolerance in every repetition of f001. Task 6 explains most of it.

### Deferred

| Item | Where | Reason |
| --- | --- | --- |
| logs attribution baseline | after the memory ledger fix, Task 12 | blocked by the logs `rss_reconciliation` gate |
| cheap wins: per-column writer properties, S3 unsigned payload over TLS, builder reuse | Task 5a | user decision 2026-09-23 |
| fixed-width sort keys, extraction straight from OTLP bytes | plan 4 | larger changes |
| any later perf task | needs the same relinked engine | lld layout breaks unwinding |

### Evidence

- `attribution.json`: the f002 index, with the binding and descriptive
  reconciliation.
- `attribution-logs-1k-stable-strict-minio-c1-w15-f002.json` and
  `attribution-metrics-mixed-strict-minio-c1-w15-f002.json`: the aggregates.
- `baseline-attribution-10b18890c3147099.json`: the metrics baseline.
- `attribution-e137b294fdec.json`: the superseded f001 index.
- `stages-spot.json`: the spot family at 936ace03a.
- `campaign/reports/task-4-report.md`.

## Task 6: memory model

Superseded in part (slice S6, 2026-09-24): until slice S6 the extracted
batches pinned their builders' default 1024-row capacity and `Block::reserve`
charged it, so a block at the default limits held only about half of
`max_block_bytes` (logs 2503.9 bytes/record charged, metrics 1439.4). After
S6 the charge is what the batch holds (1255.9 and 250.7). The retained
memory figures below were measured under the old overcharge; Tasks 5 and 7
measure memory at the real fill.

### Question

What makes up the engine's RSS, how does it relate to the exporter's own
accounting, and why did the logs RSS gate of Task 4 fail? The plan's
acceptance: RSS, accounted and budget curves, named transients, residual
magnitude and uncertainty, with failure on any hard gate, an unstable
paired baseline or an unexplained residual above tolerance.

### Method

- A pair is two fresh release engines on the same cores and workload: a
  control with the noop exporter, and the measured strict or buffered
  engine.
- Workloads: `harness-rate`, 1,275 requests of 100 records of 1 KiB at 50
  requests/s, every fifth request metrics; and `logs-high-rate`, the Task 4
  shape, 9,025 logs requests, 256 in flight, 128-request blocks, MinIO,
  about 108k records/s.
- Phases: warmup, settle 11 s, idle, load, drain, decay 11 s, retained.
- A 100 ms sampler reads telemetry, `/proc/PID/smaps` and jemalloc's own
  totals. The jemalloc totals are printed after every 4 MiB of allocation,
  which makes them a synchronous view of flush transients.
- RSS is split into non-heap, allocator retention, and jemalloc allocated,
  the live heap. The parts add up.
- Ledger gate: the residual left after subtracting accounted bytes, the
  control's live heap, non-heap and retention must stay within the frozen
  33.5 MB tolerance.
- At least 3 valid pairs per family. A family fails if a primary metric
  spreads by more than 15 percent. Seven pairs invalidated by other agents'
  compilers were rerun and excluded.
- Source: worktree at 936ace03a, commits 73b8aeb59 to d32e543b3, then fix
  rounds 1 and 2. All runs under the pin with the lease.

### Results: the RSS split

Medians of 3 pairs, [min, max], background_thread off.

| Term | strict, harness-rate | strict, logs-high-rate |
| --- | --- | --- |
| RSS peak | 100.3 MB [97.6, 103.6] | 146.1 MB [136.6, 147.2] |
| non-heap | 48.4 MB | 49.8 MB |
| of which file-backed | 43.2 MB | 44.8 MB |
| allocator retention | 30.4 MB [29.9, 32.0] | 41.3 MB [40.9, 45.6] |
| jemalloc allocated, peak | 25.6 MB [25.1, 26.6] | 58.3 MB [57.8, 58.8] |
| exporter accounted, peak | 11.5 MB [10.8, 11.7] | 29.5 MB |
| control live heap | 6.1 MB | 5.8 MB |
| paired RSS delta | 36.4 MB [33.9, 43.0] | 69.8 MB [62.9, 69.8] |

- About half the RSS is non-heap, mostly the binary's file-backed pages.
- In the high-rate shape, about 14 MB of live heap is beyond accounted: 256
  requests in flight in the receiver, converted and not yet admitted, plus
  the flush that is always running.
- After drain and decay the live heap returns to 6.5 MB, the same as idle.
- Buffered, harness-rate: RSS peak 104.7 MB. The buffer adds about 1.2 MB of
  live heap and about 2.4 MB of file-backed WAL (write-ahead log) mapping.

### Results: the Task 4 logs residual explained

The harness's 1 s gauge residual splits exactly into sampling skew,
allocator retention growth and heap the tracked counters missed.

| Family | 1 s gauge peak | Sampling skew | Retention growth | Heap not tracked | 100 ms gauge peak |
| --- | --- | --- | --- | --- | --- |
| strict, background_thread off | 22.2-24.1 MB | 5.2-5.6 | 24.9-25.5 | -6.4 to -8.5 | 16.9-18.5 |
| strict, logs-high-rate | 44.0 / 55.1 / 62.2 MB | 31.6-32.0 | 14.3-27.3 | -3.8 to +8.3 | 12.4-30.2 |
| buffered | 17.5-23.1 MB | 5.5-10.9 | 25.4-31.5 | -17.1 to -18.7 | 6.7-13.2 |
| strict, new default binary | 16.3-21.0 MB | 4.3-6.3 | 15.6-22.9 | -3.6 to -8.1 | 12.0-14.8 |

- The Task 4 failure reproduces at 44 to 62 MB.
- Sampling skew is about 32 MB, 51 to 72 percent of the residual. Blocks
  turn over every 0.14 s, faster than a 1 s gauge can follow.
- Allocator retention growth is 14 to 27 MB.
- The uncharged heap term is small, -4 to +8 MB.

### Results: the two accounting hypotheses

Merge keys were resident and never charged. Confirmed.

| Fixture | Block | Merge keys at the peak table | Ratio |
| --- | --- | --- | --- |
| logs-1k-stable | 23.2 MB | 0.68 MB | 0.029 |
| metrics-mixed | 2.9 MB | 0.51 MB | 0.173 |
| logs-512k-near-limit | 50.3 MB | 0.004 MB | 0.0001 |
| logs-8k-churn-wide, body sort key | 50.0 MB | 51.0 MB | 1.020 |

The keys are now charged in `memory.accounted` (73b8aeb59, bounded for the
merge's whole life in a9835728e). With a wide custom sort key they equal the
whole block.

Values builders keep 1024-row capacity per request. Confirmed, as
over-charging, not as a residual. A logs request of 1 record pins 117,156 B
beyond its rows, 102 times its row bytes. The charge lasts until the run
seals. 4,096 one-record requests would be charged about 480 MB for about
5 MB of rows.

### Results: series row cost against the reserved F

F is the per-series reservation the block charges before admission.

| Signal | F | Full charge per series | Real block heap per series, N >= 1000 |
| --- | --- | --- | --- |
| logs | 1352 | 1964 | 342-365 |
| metrics | 2120 | 2868 | 405-428 |

F alone is about 4.0 times the real cost for logs and 5.2 times for metrics.
The first series of a block costs 19 to 22 KB of fixed structures. The
review ruled that these numbers exclude variable-width keys, fragmentation,
tokens and merge keys, so they cannot alone justify lowering F. F was not
changed.

### Results: verdicts after the ledger correction

Fix round 1 set the ledger's flush workspace term to zero, because no
in-run measurement existed. Ledger residual maximum per pair, tolerance
33.5 MB:

| Family | Ledger max per pair | Ledger gate | Pair stability | Passes every rule |
| --- | --- | --- | --- | --- |
| memory-strict f001, background_thread off | 8.6 / 4.5 / 6.6 MB | pass | fail | no |
| memory-strict f002, new default binary | 7.6 / 8.1 / 7.7 MB | pass | fail, control peak 20% | no |
| memory-strict-logs-high-rate f001 | 38.2 / 33.4 / 36.2 MB | fail | fail | no |
| memory-buffered f001 | 6.2 / 10.8 / 6.9 MB | pass | fail | no |
| memory-strict-bgthread f001 | 8.4 / 9.1 / 4.3 MB | pass | pass | yes, baseline written |
| memory-strict-logs-high-rate-bgthread f001 | 37.6 / 46.7 / 45.6 MB | fail | fail | no |
| memory-buffered-bgthread f001 | 8.7 / 8.7 / 6.6 MB | pass | fail | no |

Pair uncertainty, recomputed in fix round 2 over the full allocator print
stream (r2):

| Family | Unexplained-heap bound, sum of maxima | Estimate, sum of p95 | Allocator movement max |
| --- | --- | --- | --- |
| strict | 30.8-33.6 MB | 16.1-19.3 MB | 17.9-20.7 MB |
| buffered | 32.4-35.0 MB | 16.3-19.0 MB | 19.8-22.7 MB |
| logs high-rate | 72.3-83.4 MB | 56.0-61.6 MB | 50.2-51.7 MB |

### Conclusions

- The memory model closes at small blocks. In strict and buffered families
  the ledger stays at 4.3 to 10.8 MB against 33.5 MB.
- It does not close in the logs high-rate shape. That excess is the subject
  of Task 3i below.
- jemalloc without its background thread makes quiet-phase RSS unstable.
  Decay only progresses while the process allocates. The spread is all
  allocator retention, 8.8 to 30.5 MB.
- With `background_thread:true` the measured engine's quiet-RSS spread fell
  from 15-34 percent to 1.4-6.6 percent. By user decision this is now the
  default of `df_engine` on Linux gnu builds (f64cd11af, 4928e04f6).
- On the new default binary the measured engine is stable, but the noop
  control's peak RSS still spreads by 20.2 percent after its short bursts.
  So there is no strict baseline at the new default fingerprint yet.
- The pipeline `memory.usage` counter keeps bytes freed on other threads.
  With local storage it drifts to +137 MB in strict and +273 MB in
  buffered, against 6.5 MB of real live heap. With MinIO it drifts only
  +0.7 MB. This is an engine defect that can mask a positive residual.
- `process.memory.usage.bytes` refreshes every 5 s whatever `check_interval`
  says, and jemalloc `stats.resident` overstates the heap by 11 to 13 MB.
- The in-process heap profile at `/debug/pprof/heap` is unusable for
  attribution. Its first dump makes the symbolizer hold about 266 MB.
- With `dirty_decay_ms:0, muzzy_decay_ms:0` retention falls to 2.6 to
  4.0 MB while allocated stays 13.4 MB. That confirms the split.

### Deferred

| Item | Where | Reason |
| --- | --- | --- |
| heap term of `rss_reconciliation` from jemalloc allocated | Task 12 | engine `memory.usage` drifts |
| whether the noop control's peak RSS belongs in the stability set | Task 12, with written rationale before any rerun | the rule was pre-registered |
| a reservation term for merge keys of about one block, or wide-key validation at startup | Task 12 | keys are not bounded by the merge target M |
| `shrink_to_fit` of values builders | Task 12 | it breaks an extraction budget test; fix and test must change together |
| `ingress.max_series_per_request` derived from measured F | Task 12 | user approved option A on 2026-09-23 |
| buffer heap and WAL split, backlog and replay | Task 13 | moved in fix round 1 |
| upload concurrency 1 versus 2 burst | Task 5 | moved in fix round 1 |
| engine accounting placement, umbrella finding 8 | plan 4 | architectural |

### Evidence

- `memory-strict.json` (f002) with child index `memory-strict-3e6a81525f11.json`
  (f001, probes and series cost), and `memory-strict-f001.json`,
  `memory-strict-f002.json`.
- `memory-strict-logs-high-rate.json`, `memory-buffered.json`.
- `memory-strict-bgthread.json`, `memory-strict-logs-high-rate-bgthread-f001.json`,
  `memory-buffered-bgthread.json`.
- `baseline-memory-strict-bgthread-30f46f91e7de52d4.json`: the one baseline.
- `memory-strict-decay0-strict-local-c1-w1-r001.json`: the decay0 check.
- `memory-ledger-reaggregation-r1.json` and `memory-ledger-reaggregation-r2.json`:
  the re-judged verdicts and the recomputed uncertainty.
- `stages-sink-async-logs-1k-stable-zstd-heap-f001.json`: the 36.9 MB sink
  workspace the first ledger borrowed.
- `campaign/reports/task-6-report.md`.

## Task 3i: flush stall, cancellation, flush workspace, exemplars

Closed after three review fix rounds. The final code is 8f5dddc4a. Its
probe run is the "round 3" row below; earlier rounds are kept to show how
the bound was reached.

### Question

A flush runs on the same core as ingest. How long can it keep the node loop
from running, and how long does a cancel or shutdown take to be seen? Can
the work be cut into bounded slices without changing a single output byte?
And does the ledger close once the flush workspace is accounted?

### Method

- A new probe, `measurement --flush-stall`, builds the largest block the
  default configuration admits and writes it with `Sink::write_block` on a
  current-thread runtime into a local file store.
- A ticker task on the same runtime stands in for the node loop. The longest
  gap between two ticks is the longest stretch the loop could not run.
- 20 cancelled writes per fixture, signalled at evenly spaced fractions of
  the write. "Observed" is signal to the sink's first clock reading after
  the cancel.
- Before is the library at fcceef306. The final build is 8f5dddc4a, after
  review fix round 3. 7 uncancelled and 20 cancelled writes per fixture,
  under the pin with the lease, on release bench binaries. Every fixture
  runs with the default `max_requests_per_block` of 4096, so the series
  table merges one run per admitted request (236 to 3716 runs).
- The final builder copies at most 8,192 elements per step, where a row
  counts one and each list item or map entry one more, into buffers sized
  before the first copy. Completing a column is a freeze.

| Fixture | Requests admitted | Block charged | Values rows |
| --- | --- | --- | --- |
| logs-1k-stable | 2096 | 242.7 MB | 209,600 |
| metrics-mixed | 3716 | 69.2 MB | 371,600 |
| logs-8k-churn-wide, body sort key | 813 | 338.9 MB | 40,650 |
| logs-512k-near-limit | 236 | 495.1 MB | 944 |

### Results: stall and cancellation

Longest stretch per write, worst over 7 writes:

| Fixture | Before | Round 1 | Round 3, final |
| --- | --- | --- | --- |
| logs-1k-stable | 49.1 ms | 23.9 ms | 22.7 to 25.4 ms |
| metrics-mixed | 34.2 ms | 21.1 ms | 20.0 to 20.8 ms |
| logs-8k-churn-wide | 63.4 ms | 21.4 ms | 17.8 to 19.4 ms |
| logs-512k-near-limit | 33.0 ms | 20.4 ms | 18.7 to 22.2 ms |

Worst cancel observation over 20 cancelled writes:

| Fixture | Before | Round 3, final |
| --- | --- | --- |
| logs-1k-stable | 44.7 ms | 21.7 ms |
| metrics-mixed | 28.5 ms | 13.8 ms |
| logs-8k-churn-wide | 56.3 ms | 13.8 ms |
| logs-512k-near-limit | 30.6 ms | 11.6 ms |

Whole-flush process CPU, median of 7:

| Fixture | Before | Round 3, final |
| --- | --- | --- |
| logs-1k-stable | 459 ms | 441 ms |
| metrics-mixed | 133.9 ms | 139.1 ms |
| logs-8k-churn-wide | 579 ms | 493 ms |
| logs-512k-near-limit | 604 ms | 590 ms |

- All 8 output files, 8 KB to 369 MB, have identical SHA-256 before and
  after every round. The goldens pass unchanged.
- The final builder is cheaper on the logs fixtures, because it copies runs
  of consecutive rows instead of one row at a time. metrics-mixed moves by
  +3.9 percent with overlapping ranges.
- Two steps cannot be sliced without changing bytes: one chunk write,
  bounded by `sorting.merge_chunk_bytes`, and one row-group close, bounded
  by `parquet.row_group_bytes`. Each is about 20 to 25 ms at the defaults.
  They are now the longest stretch.

### Results: flush workspace

The sink now publishes its live flush workspace as `flush.workspace`, and
`memory.accounted` includes it. It has three terms: the merge chunk being
produced, the encoder's in-progress row group, and upload buffers not yet
landed.

| Where | Peak flush workspace |
| --- | --- |
| probe, logs-1k-stable 243 MB block | 166.0 MB |
| probe, logs-8k-churn-wide | 164.9 MB |
| probe, logs-512k-near-limit | 161.7 MB |
| probe, metrics-mixed | 33.1 MB |
| engine, logs high-rate, 13 MB blocks | 20.6 MB |

A large table holds roughly encoder plus one row group plus one chunk:
about 64 + 64 + 16 MB at the defaults. The tail of each row group, shorter
than one part, waits in the buffered writer and pins the whole previous
row-group buffer.

### Results: logs high-rate ledger re-judged

Release engine at da4df0228 plus 1acdb648e, under the pin. Ledger residual
maximum per pair, tolerance 33.5 MB:

| Family | Task 6 | Task 3i | Gate per pair |
| --- | --- | --- | --- |
| background_thread off, now nobgthread f001 | 38.2 / 33.4 / 36.2 MB | 31.0 / 46.5 / 28.9 MB | pass / fail / pass |
| background_thread on, now bgthread f002 | 37.6 / 46.7 / 45.6 MB | 43.6 / 37.0 / 33.7 MB | fail / fail / fail |

- The ledger still does not pass: 2 of 6 pairs pass, and neither family.
- Every positive excursion but one is the first sample of a flush. The
  allocator print already shows the flush's allocation, while the telemetry
  answer still reads `flush.workspace` 0.
- Every negative excursion, -32 to -33 MB, is the mirror at a flush's last
  sample.
- The one other excursion, +46.5 MB with no flush running, is the burst of
  256 in-flight requests in the receiver.
- Inside flushes the ledger now closes. The load median of the residual is
  -4.7 MB with the thread off and -2.1 MB with it on.
- Both families still fail pair stability, on the noop control's RSS only,
  with a control idle spread of 30 to 33 percent.

### Results: CPU per record

Spot family `stages-spot-task3i.json`, f004 at 75732cafd, CPU ns/record,
median of 3, against `stages.json`:

| Stage | Workload | f004 | stages.json | Ratio |
| --- | --- | --- | --- | --- |
| sink | logs-1k-stable | 2274.8 | 2250.9 | 1.011 |
| sink | metrics-mixed | 396.0 | 353.0 | 1.122 |
| sink | logs-8k-churn-wide | 13042 | 14484 | 0.900 |
| sink | logs-512k-near-limit | 675138 | 718203 | 0.940 |
| encode zstd | logs-1k-stable | 1612.4 | 1913.5 | 0.843 |
| encode zstd | metrics-mixed | 222.3 | 225.5 | 0.986 |

A like-for-like comparison on sink metrics-mixed, pre-task build against
HEAD in 5 alternating rounds, gave 378.9 against 390.2 ns/record. So this
task adds 3.0 percent on the smallest block. About 9 of the 12 points
against `stages.json` predate the task. perf did not attribute the 3
percent to the changed code.

### Results: loss contract

- `metrics.exemplars` alone decides. Unset or `drop` keeps the points and
  counts the exemplars in `dropped.exemplars`. Only an explicit `reject`
  refuses the request, as a permanent nack.
- `logs.exemplars` is refused at startup.
- The README section "What this exporter does not keep" lists every
  intentional loss, what is kept instead and how it shows. Examples:
  traces, exponential histograms and summaries, exemplars, attribute value
  types, zero or out-of-range timestamps, and arrival order.

### Conclusions

- Bounded slices cut the longest flush stretch to 20.4 to 23.9 ms on every
  fixture. The wide sort key case drops from 63.4 to 21.4 ms.
- Cancellation is now seen within one stretch plus a step boundary.
- Output bytes are unchanged, so no format or golden moved.
- The flush workspace is now measured and charged. Task 6's verdict "the
  flush workspace takes the ledger past tolerance" is superseded.
- What still exceeds tolerance is one-sample pairing skew between a 100 ms
  telemetry answer and an allocator print at a flush's edge. Closing it
  needs synchronous pairing or a tolerance aware of workspace change, not
  another term.

### Deferred

| Item | Where | Reason |
| --- | --- | --- |
| column allocation builds per-source bookkeeping for every run in one step | Task 12 | bounded by `max_requests_per_block`; inside the measured 18-25 ms at the default 4096 |
| the charge omits the per-source bookkeeping of the column builders | Task 12 | size it with the jemalloc heap dumps, then add a per-run term |
| the fallback path for types the lake never uses is neither step-bounded nor panic-free | slice S4 | narrow the documented promise to the lake's column types |
| synchronous pairing of telemetry and allocator prints | Task 12 | the remaining ledger excess |
| raw jemalloc heap dumps symbolized offline | Task 12, first item | attribute the ledger excess, the 160 MB workspace and the counter drift |
| copy the row-group tail, about 64 MB less per large table | Task 12 | bounded fix |
| +3 percent sink CPU on the small metrics block | Task 12 | not attributed by perf |
| the two unsliced steps | plan 4, the off-core shared writer | slicing them changes files |

### Evidence

- `flush-stall/README.md`, `flush-stall/*-round1-before.json` (before) and
  `flush-stall/*-round3-head.json` (final): the probe runs.
- `flush-stall/files-before.sha256` and `flush-stall/files-head-8f5dddc4a.sha256`:
  the byte-identity manifests of the first and the final build.
- `memory-strict-logs-high-rate-nobgthread.json` and
  `memory-strict-logs-high-rate-bgthread-f002.json`: the re-judged families.
- `stages-spot-task3i.json` (f004) with child `stages-spot-task3i-3a76ac8067b3.json`
  (f003).
- `campaign/reports/task-3i-report.md`.

## Task 5: maximum sustainable throughput and durable write speed

### Question

How many records per second does the exporter sustain per core and on four
cores, into each store, with the shipped configuration and with the receiver
admission raised; what limits it; and what it takes to reach 1M records/s.

### Method

- Workload: 80/20 logs/metric points, 1 KiB bodies, 10k hot series, 1000
  records per request, 256 connections, ZSTD. 15 s warm-up, 60 s measured,
  bounded drain; 1 s windows for the searches, 15 s for the default-window
  cells. A winner needs 3 of 3 sustainable trials; the bracket is at most 10
  percent.
- Sustainable: no receiver or exporter refusals, no backlog growth, every
  acknowledged record stored exactly once. Buffered also needs a bounded WAL.
- Producers: 8 Python sender processes on CPUs 8-15,24-31, one physical core
  each, sending self-verifying template requests; calibrated to 4M/s against
  a noop exporter. The engine keeps its explicit cores in 0-7,16-23.
- Oracle: DuckDB aggregates per producer and signal (count, distinct, sum,
  min and max of the record sequence against the acknowledged requests, a
  200-record field-by-field sample), cross-checked with clickhouse-local.
- Memory: the RSS residual takes its heap term as the band between jemalloc
  `allocated` and `resident`; the engine's `pipeline.memory.usage` counter is
  not used (see below).

### Results: sustainable rate, thousands of records/s

| Store | Workers | Shipped slots (128) | Raised slots (4096) | 15 s window | Buffered |
| --- | --- | --- | --- | --- | --- |
| local | 1 | 128 | 255 | | |
| local | 4 | 288 | 684 (722 flips) | | 144 |
| MinIO | 1 | 128 | 187 | 8.5 | |
| MinIO | 4 | 240 | 448 | 28 | 152 |
| RustFS | 1 | not run | 192 | | |
| RustFS | 4 | 224 | 360 | | |

Durable write speed at the ceiling: local 416 MB/s (684k), MinIO 273 MB/s
(448k), RustFS 220 MB/s (360k).

### What limits it

- Shipped strict: receiver slots, not the exporter. A strict request holds
  its slot until its block is durable, so throughput per worker is at most
  slots x records per request / hold time. Confirmed: 128k per worker at a
  1 s window, 8.5k at the shipped 15 s window (formula 8.53k). Above it the
  receiver sheds with RESOURCE_EXHAUSTED while workers are 25-35 percent on
  CPU and the exporter refuses nothing.
- Raised strict: the exporter's CPU on the ingest core. The flush shares the
  core with ingest; at 722-760k on four workers the hottest worker's flush
  reaches the window, rotation waits and admission closes.
- Buffered: the WAL's device. The buffer writes about twice the wire bytes
  (WAL entry plus the finalized 32 MiB segment), syncs every 25 ms and
  finalizes segments synchronously on the worker's runtime; on this host's
  single NVMe, shared with the store, writes wait 12-50 ms and the worker
  stalls. With the WAL on tmpfs it sustains at least 256k and fails at 384k.
  Engine CPU per record is 5.9 us against 4.6 us strict.

### Scaling and the 1M/s target

- Four workers give 0.67 of four times one worker: 0.81 (CPU per record up
  22 percent from L3 and memory-bandwidth contention on one CCD) x 0.82
  (occupancy: per-window connection-hash imbalance saturates the hottest
  worker first). Whole-process: 259k records per CPU-second at one worker,
  213k at four.
- 1M/s was not measured; the highest sustained rate is 684k on four worker
  cores. The estimate from the four-worker figures is about six worker cores.

### Other results

- Fan-in (MinIO, four workers, 448k offered): one connection reaches 226k
  and uses one worker; 8 connections 396k; 64 connections 439k and still fail
  the backlog rule; 256 sustain 448k. Connections must well outnumber workers.
- Ack latency, strict, 1 s window: p50 0.67 s, p99 1.17 s at 256k; 15 s
  window: p50 8.4 s, p99 15.0 s. Buffered at 152k: freshness p50 0.72 s,
  p99 1.36 s.
- Upload concurrency 1 vs 2: no difference at any ceiling measured.
- jemalloc stats prints (the memory method) cost at most about 6 percent.
- High cardinality (a unique attribute per point): one series per point; the
  series dataset reaches 66 percent of the values dataset and CPU per point
  is 1.9 times the hot workload.
- Alloy as producer: its reference batches (20,000 records, 7.96 MB) are all
  refused by the shipped 4 MiB receiver decoding limit (OUT_OF_RANGE) and it
  retries forever; with 16 MiB it sustains about 29.6k lines/s on one
  connection, every line read back exactly once.

### Findings for Task 12

- The engine's `pipeline.memory.usage` credits frees only to the allocating
  thread; buffers freed on the tokio blocking pool make it grow without bound
  (10.3 GB against 550 MB RSS), buffers freed elsewhere give the other sign.
- In-flight requests in the receiver are outside every memory budget: with
  4096 slots per worker, 16.2 GB allocated against 3.4 GB exporter-accounted;
  with 4096 slots and 8 MB requests the engine vanished without a log line.
- The receiver's load-shed is not counted by
  `receiver.otlp.requests.rejected`, and its message names a per-connection
  limit although the limit is the worker's slots.
- The reference Alloy config and the shipped decoding limit cannot work
  together above about 4k lines/s.
- The durable buffer exposes neither its WAL flush interval nor its segment
  size and finalizes segments on the worker's runtime.

### Evidence

- `capacity-local-*.json`, `capacity-minio-*.json`, `capacity-rustfs-*.json`
  (family indexes, re-judged in family 2) and their `capacity-*-r*.json`
  children; superseded trials carry their reason.
- `campaign/reports/task-5-report.md`.

## Task 7: thirty-minute soaks

### Question

Does the exporter hold a high rate for thirty minutes with blocks really
filling to `max_block_bytes`, without loss, duplication or drift, strict and
behind the durable buffer?

### Method

One worker, MinIO, the shipped 15 s window, Task 5's producers, generator,
oracle and allocator band. Strict: receiver slots raised to 4096, offered at
70 percent of the MinIO one-worker raised ceiling. Buffered: shipped slots,
offered at 70 percent of a measured one-worker buffered ceiling (104k
sustainable, 112k not). 1 s samples, one-minute aggregates, drain after
input stops.

### Results

| | Strict | Buffered |
| --- | --- | --- |
| Offered = acknowledged, records/s | 130,899 | 72,800 |
| Records, objects | 240.2M, 146 GB | 133.6M, 81 GB |
| Missing, duplicated | 0, 0 | 0, 0 |
| Exporter accounted at highest fill | 0.99 of 1.64 GB | 0.81 of 1.64 GB |
| RSS peak | 1.31-1.36 GB | 1.74 GB |
| RSS median, last to first minute | 1.013 | 1.035 |
| Receiver in-flight at fill | 340 MB | |
| WAL peak | | 619 MB of 1 GiB, flat |
| Ack latency p50 / p99 | 3.55 / 5.58 s | p99 0.87 s |
| Freshness p50 / p99 | | 4.6 / 8.3 s |
| Drain after stop | 10.3-10.6 s | 11.0 s |

Blocks rotated on bytes (480) and time (120) in the strict soak; file
descriptors stayed flat at 301; allocated minus accounted stayed at about
0.1 GB, no leak signal. The PR-tier soaks (65 s, forced rotations, a 4 s store
outage) store every record once and run in CI in 163 s with the debug engine.

### The RSS band rule

The strict soak first failed the RSS reconciliation on 2 and 5 samples out of
about 66,700: each in the second a flushing block completed, the first
jemalloc print after `resident` had dropped 360-460 MB while the kernel was
still releasing the pages. The band's upper edge is now the larger `resident`
of this and the previous paired print; the tolerance is unchanged, and a
leak outside the allocator still fails. Re-judged from stored samples, the
second strict run passes and writes the baseline; runs whose failing pairs
were thinned out before the evidence-keeping fix stay failed.

### Findings for Task 12

- Receiver in-flight memory sits beside the exporter budget at real fill
  (340 MB at fill in the strict soak).
- Heap dumps across a soak wait for Task 12's raw-dump mode.

### Evidence

- `soak-strict.json`, `soak-buffered.json` and their children,
  `pr-soak-*.json`, `baseline-soak-strict-5cb03bef4ffc17fe.json`.
- `campaign/reports/task-7-report.md`.

## Task 9: S3 faults on real stores

### Question

How does the exporter behave, observably and in delivery, when the object
store is slow, answers HTTP 503, or disappears, strict and behind the durable
buffer, on MinIO and RustFS?

### Method

12 cells: slow, http503 and store_outage x strict and buffered x MinIO and
RustFS, one worker, 5 s windows, the fault rig of Task 8 (Toxiproxy and an
NGINX fault front), Task 5's generator and aggregate oracle. Each fault is a
state machine with an observable arming condition (for an outage: a flush
failure of class deadline, logged after the flush deadline) and a hold of
at least 15 s past that failure. Every cell also checks the partition
lateness bound (L = 5 + 2 x (60 + 5) = 135 s), multipart on the real store
and incomplete uploads left in the bucket afterwards.

### Results

- In all 12 cells: no record missing, unexpected or corrupt; DuckDB and
  clickhouse-local agree; multipart ran on both stores (CreateMultipartUpload,
  UploadPart, Complete answered 2xx, objects carry `-N` ETags); no lateness
  violation (worst observed 6.7 s after the hour's end, against 135 s; the
  worst-case path was not produced).
- Duplicates occur only inside requests the producer resent (strict) or in
  files a failed block left behind (buffered), 9,000 records in one cell each.
- 7 of 12 cells pass. The five failures fail one hard check,
  `orphaned_uploads_expected`, a product defect (below).
- Recovery after the store returns: 1.3-7.7 s to the first values file plus
  ack, 25-31 s to drained, when the store stays down past about 130 s. When it
  returns earlier, writes stall about 60 s; the NGINX logs place the stall in
  the fault rig's proxy path (upstream status "-", plausibly a TCP connect
  stuck until the kernel's connect timeout), not in the exporter, which ended
  every attempt at its client timeout or flush deadline and nacked the
  requests for retry.

### Findings for Task 12

- F1: a phase-2 failure of a multipart upload (a failed CompleteMultipartUpload
  or a part failing during finalization) is neither aborted by the sink nor
  counted in `flush.abort_failures` (series-lake sink/write.rs:704-714); when
  object_store's own abort also fails, the error comes back as an ordinary
  storage error. Each such attempt leaves one incomplete upload with no
  signal. Seen in 7 runs.
- An old attempt's part upload can keep running up to 132 s.
- `flush.late_commits` undercounts while the store stays down through the
  cleanup cutoff, and the cleanup probe can only report "unknown" during a 503
  fault.
- The rig's health HEAD goes through the general proxy, so it can declare the
  endpoint healthy while the values route still hangs; Task 11 separates rig
  from store with direct and per-proxy requests.

### Evidence

- `failure-s3.json` (index chain with every rerun's purpose) and its
  `failure-s3-*-r*.json` runs; raw archives in `.measurement-artifacts/failure-s3/`.
- `campaign/reports/task-9-report.md`.

## Task 10: graceful restart and hard kill

### Question

What happens to acknowledged and in-flight data when the engine is stopped
gracefully or killed with SIGKILL, strict and behind the durable buffer, on
MinIO and RustFS?

### Method

12 cells: graceful_restart (admin shutdown, then a new engine), kill_active
(SIGKILL 1.3 s into a window, the new block only in memory) and kill_upload
(SIGKILL while a throttled multipart upload is on the wire) x strict and
buffered x MinIO and RustFS; Task 9's fault-case framework, Task 5's generator
and aggregate oracle. The graceful stop is checked against the configured
shutdown deadline (150 s) and the absolute cleanup cutoff (156 s); orphaned
uploads are compared with the expected set after every case.

### Results

- No acknowledged record went missing in any cell; no unexpected, corrupt or
  permanently refused record.
- Strict graceful restart exits in 5.0 s with code 0; the 102-103 requests
  pending at the signal are acknowledged by the old engine and stored once.
- Strict SIGKILL: the in-memory cohort is not stored, the producer resends it,
  every record is stored once; a kill during an upload leaves exactly the one
  expected incomplete upload.
- Buffered SIGKILL of an in-memory block: records the buffer had acknowledged
  but not flushed are replayed once each.
- A store may commit a PUT whose client was killed (RustFS did, about 1.4 s
  after the kill); the object is attributed to the dead boot and readers are
  unaffected.
- 9 of 12 cells pass; the three failures are durable-buffer defects.

### Findings for Task 12

- T10-F1: after a graceful restart the durable buffer redelivers bundles the
  exporter already wrote (10,200 and 2,600 records stored twice):
  `handle_shutdown` (durable_buffer_processor/mod.rs:1643-1737) shuts its
  storage engine down before the exporter's queued acknowledgements reach
  `handle_ack`, so they are never recorded.
- T10-F2: after a SIGKILL two requests the buffer had acknowledged 9 and 59 ms
  earlier were stored twice without a resend; the WAL replayed exactly those
  two entries. Seen once; the hypothesis that the WAL position is not made
  durable together with the segment is unverified. Task 13 reproduces it.

### Evidence

- `failure-process.json` and its `failure-process-*-r*.json` runs; raw archives
  in `.measurement-artifacts/failure-process/`.
- `campaign/reports/task-10-report.md`.

## Slice S6: extraction and write speed (Task 5a)

### Question

Which cheap changes lower CPU per record without changing schema, data,
sort order or golden fingerprints?

### Method

Each change was measured with a stage spot family (3 repetitions, release
bench binaries, taskset -c 0-7,16-23, the host lease) before and after, and
kept only if CPU per record fell beyond noise. The engine was attributed
once per workload at the end, as in Task 4. The builder-sizing families ran
with `GLIBC_TUNABLES=glibc.malloc.trim_threshold=1073741824:glibc.malloc.mmap_threshold=1073741824`
on both sides: without it the bench's glibc allocator trims and refaults
memory and the sort_seal logs iterations split into two modes (about 215
and 355 ns/record). The engine runs on jemalloc.

### Results: stages, CPU ns/record, median of 3

| Stage | Logs before -> after | Metrics before -> after |
| --- | --- | --- |
| extract | 1177.4 -> 817.4 | 766.4 -> 513.9 |
| sort_seal | 429.6 -> 227.4 | 340.3 -> 321.7 |
| merge | 99.4 -> 76.4 | 123.5 -> 70.2 |
| encode zstd | 1582.6 -> 1550.7 | 229.4 -> 226.7 |
| upload, MinIO | 376.4 -> 102.5 | 15.7 -> 6.5 |

The upload gain comes from UNSIGNED-PAYLOAD, which series_parquet uses by
default over TLS only. With equal allocator conditions, builder sizing alone
gives sort_seal -13.7 percent logs and -8.3 percent metrics, merge -12.0 and
-21.6 percent; the earlier sort_seal figure overstated it because part of
the "before" cost was the glibc slow mode.

### Results: engine attribution

| | Task 4 | After S6 |
| --- | --- | --- |
| Logs engine CPU, ns/record | 5,345 | 4,205 (-21.3%) |
| Metrics engine CPU, ns/record | 2,453 | 2,155 (-12.1%) |
| Logs throughput, one core, records/s | | 113,150 |
| Metrics throughput, one core, records/s | | 253,458 |
| Logs allocator share | 12.6% | 6.6% |

Encoding is now 39.1 percent of logs CPU; conversion (OTLP to Arrow) is the
largest metrics category at 24.4 percent. Stored bytes changed by less than
0.1 percent; the flush-stall probe files are row-for-row identical by a
DuckDB join. Tried and not adopted: ZSTD level 3 (+57 percent encode CPU, no
byte gain), a SmallVec for row cells.

### Consequences

- Blocks now fill to `max_block_bytes` for real. At the default limits the
  flush-stall worst case is 32.8 ms longest stretch (logs-1k, 477.8 MB block,
  874 ms whole-flush CPU) and 28.4 ms (logs-512k), against a same-day base
  2-5 ms above the Task 3i figures.
- The largest metrics merge chunk is 1.1 percent above `merge_chunk_bytes`,
  the documented approximation; chunks are sized from the average pinned
  bytes per row, so clustered wide rows can overshoot more (Task 12).

### Evidence

- `stages-spot-s6-*.json`, `attribution-*-f003.json`, `attribution.json`.
- `flush-stall/*-s6-*.json` and the `files-*-956adf0ae.sha256` manifests.
- `campaign/reports/slice-S6-report.md`.

## Other tasks

- Task 1, harness and result contract: found that a worker that does not
  answer a collection reports every gauge as zero. A liveness marker fixed
  the drain proof. Missed collections were a start-up race, 1 of 395
  epochs, not deferral under load. See `campaign/reports/task-1-report.md`.
- Task 2, launchers and host controls: made release engines mandatory for
  measured runs and kept `rss_reconciliation` hard with its frozen
  tolerance. Found a durable-buffer replay of 5000 acknowledged records
  after a graceful restart, routed to Task 12. See
  `campaign/reports/task-2-report.md`.
- Task 3b, exporter correctness under faults: 23 commits. Store outages now
  surface as deadline outcomes with their last error. Internal errors are
  retryable. Shutdown drains are no longer cut short by Tokio's cooperative
  budget. A backward clock step no longer stalls rotation. See
  `campaign/reports/task-3b-report.md`.
- Task 3c, throughput path and hygiene: the metrics memo is now keyed on
  content. A single extract run on metrics-mixed went from 1357.5 to
  919.7 ns/record. The later spot family measured 791.1 ns/record. Also the
  opt-in `series-parquet` feature, semantic metric names and scrubbed JSON.
  See `campaign/reports/task-3c-report.md`.
- Task 3d, format batch: format revision 2 with a crate-owned fingerprint
  vocabulary, native Parquet `SortingColumn` metadata that stops at the
  first float key, and OTLP-JSON spellings. No series id changed. See
  `campaign/reports/task-3d-report.md`.
- Task 3g, damaged OTLP bodies: a schema-aware recursive framing check now
  refuses damage that was acknowledged before. It also fixed a pre-existing
  panic in the OTLP to OTAP encoder and several pdata view misreads. Final
  cost: 5.4 to 6.7 us per logs-1k-stable request, 19 percent of conversion.
  See `campaign/reports/task-3g-report.md`.
- Task 3h, attribute collapse: `processor:attribute` deleting a
  high-cardinality attribute collapses streams correctly for delta sums and
  histograms. A cumulative pair is stored as two rows under one series,
  which is wrong for latest-value reads. See `campaign/reports/task-3h-report.md`.
- Task 8, fault tools: all 28 probes pass on MinIO and RustFS in required
  mode. Six fault classes are available. `disconnect_reset` and
  `dropped_completion_response` wait for Task 11 probes. See
  `campaign/reports/task-8-report.md`.
- Task 3f and slice S4, behaviour-preserving simplification: one refusal
  vocabulary with one exporter Outcome, `Block` without a type parameter,
  one metrics point path, the exporter tests split by topic, sink.rs split
  into five modules, the abort timer supplied by the caller (no tokio time
  in series-lake), one HookStore for test object stores. Goldens, nack
  causes, error.type labels and producer-visible sentences are unchanged.
  One step was reverted: arrow's `make_builder` put a type downcast on
  every extracted cell. See `campaign/reports/task-3f-report.md`.

- Slice S1: a closed pdata channel releases the latched Shutdown with its
  own deadline instead of a synthesized now + 1 s one.
- Slice S2: the OTLP framing check no longer checks UTF-8; file, parquet and
  otap accept repeated singular fields as before the campaign, series_parquet
  refuses them; top-level invalid UTF-8 is stored as U+FFFD in parquet, otap
  and series_parquet (counted in `repaired.invalid_utf8`), refused by the
  file exporter's JSON encoder as before; the framing walk costs 25 percent
  of the OTLP-to-OTAP conversion on logs, 22 on metrics, 5 on traces.
- Slice S5: decoded attribute values are charged to the one request budget
  before they are allocated, CBOR is decoded straight into the lake value
  with capacity-charged reservations and rollback, log bodies and residual
  attribute maps are rendered within the budget. A dictionary value
  referenced by 47 attributes, which allocated 225 MB before, is refused at
  the 32 MiB budget plus 2 MiB.
- Slice S7: forced-drain refusals never take the credit a held block needs
  (no assert on the shutdown path); after Shutdown an attempt starts only
  while the latched deadline has not passed, and every cleanup ends by the
  deadline plus `upload.abort_timeout` (at least 1 s); deciding held
  requests costs about 1 microsecond each; a proptest state machine pins
  exactly one decision per request.
- Slice S8: `unsupported` defaults to drop; a missing retry section derives
  the retry budget; a storage nack is a fixed sentence and a class; flush
  cleanup, failure class and per-attempt retries are reported; a late commit
  is detected by bounded HEAD probes on the frozen names.
## Open questions

| Question | Current state | Resolved in |
| --- | --- | --- |
| Does the high-rate memory ledger close? | 2 of 6 pairs pass; excess is pairing skew at flush edges | Task 12: raw jemalloc dumps, then synchronous pairing |
| Engine `memory.usage` drift on local storage | +137 MB strict, +273 MB buffered | Task 12: heap term from jemalloc allocated; upstream engine bug |
| Is the noop control's RSS a stability metric? | it fails pair stability in every family but memory-strict-bgthread, up to 33 percent spread | Task 12, rationale written before one rerun |
| No strict memory baseline at the new default fingerprint | only the `MALLOC_CONF` variant has one | Task 12, after the control decision |
| Logs attribution baseline | blocked by `rss_reconciliation` | after the ledger fix, Task 12 |
| Pipeline baseline CPU dose | 0.1 s window, calibration only | Task 12 |
| encode logs-1k-stable peak RSS, first repetition high | third sighting, CV 0.21 | Task 12 |
| upload logs-1k-stable wall CV 0.21 | store-bound | Task 12 |
| Logs engine_runtime 0.42 and metrics upload 2.27 of their bench references | marked unexplained; the first explanation was withdrawn | no task named in the reports |
| Stage baselines encode a half-loaded package | TLC paused on 2026-09-23 13:5x; later runs see a quieter host | Task 12 re-measurement; caveat carried to Task 14 |
| Wide sort keys equal the whole block | charged, but no reservation term | Task 12 |
| Values builder over-charge | 102 times the row bytes for a 1-record request | Task 12 |
| Defaults admit request shapes that are permanently refused | about 215k new metric series per request at the defaults | Task 12 `max_series_per_request`; Task 5 high-cardinality shape |
| Unanswered collections at saturating load | measured only under the outage test | Task 5 |
| Per-run bookkeeping of the chunk builders | not charged; allocated in one step per column | Task 12 (heap dumps, then a per-run term) |
| Cores needed for 1M records/s | about 4.8 cores of logs stage CPU, by simple ratio | Task 5 |
