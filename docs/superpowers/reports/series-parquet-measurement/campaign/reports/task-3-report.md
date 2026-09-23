# Task 3 report: layered Criterion and complementary benchmark mechanics

Status: DONE_WITH_CONCERNS. Commits 5c7c49e98, df08c9e04 and b0a397f8d on
series-parquet-exporter (base 69d5b8eea). Not pushed. The
recorded numbers are those of the fix-round re-measurement; see "Fix round
1" at the end.

## What changed

### Rust: two bench targets on the production library path

`crates/series-lake/Cargo.toml` declares `[[bench]] measurement` and
`[[bench]] layered`, both `harness = false`, and adds the dev dependencies
they use: `criterion`, `cpu-time`, `dhat`, `bytes`, `sha2`, `prost`,
`object_store` with `aws`+`fs`, and `otel-arrow-dfe-otap` with `aws` and
`crypto-ring` (the exporter's own object-store constructor, so an upload
stage builds the same client the exporter builds). A new feature
`bench-heap` is empty by design: it only switches on the DHAT global
allocator inside the `measurement` bench executable, so a timed sample is
never taken through an allocation tracker, and the two builds carry
different baseline fingerprints.

`benches/measurement/stages.rs` holds every stage. A stage is three calls:
`prepare` builds one iteration's input, `run` is the measured operation,
`observe` accounts for and verifies the output and removes what it stored.
Only `run` is ever inside a timer. `Timing`/`timed` are the brief's
primitive (thread CPU clock); `timed_process` is its process-CPU twin for
the stages that drive the runtime, so work on the blocking pool is not
lost. Every stage calls production code: `OtapPayload::from(OtlpProtoBytes)`
plus `try_into_with_default`, `extract`, `Block::{reserve, admit, seal}`,
`merge_runs`, `Sink::write_block`, `object_store::buffered::BufWriter` with
the configured part size and concurrency. The only code of its own is the
diagnostic encoder `encode_chunks`, which is the brief's code with the
sink's writer properties and flush predicate; `sink_equivalence` writes the
same sealed block through the actual `Sink` and compares decoded schema,
decoded values, row-group count and per-row-group rows, and per column
chunk the compression codec, the presence of statistics and dictionary use.
That comparison runs both in the self-test and as a fixture check of the
`encode` (ZSTD) and `sink` stages, so a later sink change cannot leave the
diagnostic encoder silently stale.

`benches/measurement.rs` is the fallible CLI of the brief
(`--stage/--input/--config/--output/--iterations/--profile/--compression`),
plus `--self-test` and `--handshake`. It writes JSON with
`serde_json::to_writer_pretty` and prints nothing else. A timing process
takes at least 30 samples and one second of measured work under a
sixty-second deadline, marks itself incomplete otherwise, and keeps at most
2000 samples by uniform thinning (a stage that costs nanoseconds reaches one
second only after millions of samples). A heap process starts one DHAT
profiler after its fixtures exist, records `HeapStats` before, after the run
and after the output is dropped under the profiler, and never reports a
throughput.

`benches/measurement/tests.rs` is invoked by `--self-test` only, with
Scenario/Guarantees comments on each: diagnostic encoding versus actual Sink
on a logs and a metrics fixture with row groups small enough that the flush
predicate cuts several; exact stage row counts for all fifteen stages on
both signals; input generation excluded from the timer (a five-millisecond
preparation, and no sample may carry it); the length-prefixed input round
trip and its refusals; and the registered stage names.

`benches/layered.rs` registers the cumulative Criterion groups in order --
`otlp_noop`, `otlp_convert`, `otlp_extract_hash`, `otlp_sort`,
`otlp_parquet_local`, `otlp_parquet_zstd` -- with `sample_size(30)`,
`warm_up_time(1s)`, `measurement_time(5s)`, `Throughput::Elements` and
`iter_batched` so preparation is outside the timer. It verifies each layer's
output once, untimed, before anything is timed, keeps a failure from a timed
routine in an outer slot and returns it from its fallible `main`.

### Python: `performance.py`, the `measure stages` case

`performance.py` owns the stage contract (`STAGES`, `CRITERION_LAYERS`,
`STAGE_METRICS`, `PROFILE_METRICS`, `validate_stage_result`,
`validate_child_result`), the deterministic inputs, the workload
configurations, the bench-process lifecycle and the aggregation.

- `write_stage_input` writes one signal's `build_request` bytes as
  `u32`-little-endian length-prefixed records with a sidecar naming the
  records, the distinct series and the file hash. The bench refuses a
  sidecar whose hash does not match.
- `locate_benches` never invokes cargo. It takes the executable named by
  `SERIES_STAGE_BENCH`, `SERIES_STAGE_BENCH_HEAP` or `SERIES_LAYERED_BENCH`,
  or scans `target/release/deps` for candidates, and asks each one
  `--describe`: the bench answers with its target name, whether DHAT's
  allocator is installed and whether it carries debug assertions. A debug
  build is refused, and a missing executable fails the family before
  anything is measured, naming the exact `cargo bench --no-run` command that
  builds it.
- Each child is a `measure.run_case` with `evaluate=False`: the exclusive
  lease, the one out-of-process build monitor, both environment snapshots
  and the affinity assertion. An engine-less child registers its benchmark
  process with that same monitor; `RunControls.watch_pinned` re-checks its
  main thread on every tick, and both snapshots compare every thread of the
  pinned role through the new `pinned=` argument of `environment_snapshot`.
  The bench announces `SERIES_STAGE_READY` once its fixtures exist and waits
  for a line before measuring, and `SERIES_STAGE_DONE` before exiting, so
  both snapshots observe the live process that made the measurement.
- `otlp_noop` in `mode: pipeline` is a real engine: `test_e2e.py` gained the
  `noop` topology, which replaces the exporter with the always-enabled
  `exporter:noop` (acknowledging, `wait_for_result: true` receiver
  unchanged), and the harness's own `Producer` sends the workload's requests
  exactly once each. Its paired allocation profile is a second lifetime of
  the `dhat-heap` engine, preceded by an idle control lifetime of the same
  build so the start-up share is visible rather than charged to the records.
- Aggregation: children are joined by `(stage, mode, workload_config_id,
  compression, repetition)`. Per profile the three repetitions become one
  aggregate carrying medians, ranges and coefficients of variation, each
  hard gate derived from every child's own gate, the paired RSS
  reconciliation and the 15 percent stability rule; the Controller baseline
  policy is applied there. Each repetition also becomes one registered stage
  result with the full eight-metric schema and a `metric_sources` map naming
  the exact child that measured each field; a composite missing any
  mandatory measurement is rejected, never completed with a zero.

`measure.py` gained the `stages` subcommand (long-command gated) and
`test_measurement.py` gained 16 stage contracts, including the brief's
`test_otlp_noop_requires_all_metrics` verbatim. The README documents the
command, the four binaries it needs and the `bench-heap` feature.

## Commands and results

From `rust/otap-dataflow`, venv `/tmp/series-parquet-venv`:

- Red, before the implementation: `python3 -m unittest ...test_measurement`
  failed at import (`performance` did not exist) and
  `cargo bench ... --bench measurement --no-run` failed with
  `can't find 'measurement' bench`.
- `cargo check -p otel-arrow-dfe-series-lake --benches`: clean.
- `cargo clippy -p otel-arrow-dfe-series-lake --benches -- -D warnings`,
  with and without `--features bench-heap`: clean, no suppressions added.
- `cargo fmt -p otel-arrow-dfe-series-lake`: applied; the family was rerun
  afterwards so the committed sources are the ones that measured.
- `cargo xtask check-benches`: bench clippy and compile passed.
- `cargo bench -p otel-arrow-dfe-series-lake --bench measurement -- --self-test`:
  exit 0 (five self-tests).
- `cargo bench -p otel-arrow-dfe-series-lake --bench layered --no-run`: built.
- `python3 -m unittest crates.validation.tests.series_parquet.test_measurement`:
  Ran 165, OK, 4.3 s (147 before this task).
- `SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure stages --output-dir /tmp/series-stages`
  (the committed run, after the cargo-free discovery of df08c9e04): 312
  children, all passed; 104 profile aggregates, 102 passed and 102 baselines
  written; 138 registered stage results; 722 s wall; index `stages.json`
  status **failed** because of the two unstable aggregates below. A sampler
  watched for `cargo`, `rustc`, `cc1` and `ld` every two seconds for the
  whole run and saw none.
- `measure stage-results --index .../stages.json` staged 519 files by name.

Host: AMD Ryzen 9 9950X, 32 logical / 16 physical cores. Allocation of every
child: bench [1], bench_reserved [2,3,4], engine_observability [0],
producer [5,6], store [7], reader [8]. The MinIO container was pinned to
core 7 and registered with both snapshots. Builds: benches at profile
`bench` (release + fat LTO), timing build on the system allocator, heap
build with `bench-heap`/DHAT; engines `release`+jemalloc and
`profiling`+dhat-heap.

Workloads (one signal per input file):

| id | signal | requests | records | series | input bytes | cache committed |
| --- | --- | --- | --- | --- | --- | --- |
| logs-1k-stable | logs | 200 | 20,000 | 100 | 21,638,200 | 0.5 |
| metrics-mixed | metrics | 150 | 15,000 | 600 | 908,550 | 0.5 |
| logs-8k-churn-wide | logs | 120 | 6,000 | 120 | 49,504,920 | 0.0 |
| logs-512k-near-limit | logs | 24 | 96 | 10 | 50,339,640 | 0.5 |

`metrics-mixed` carries integer and double gauges and sums plus histograms
of two widths, including the genuinely empty distribution.
`logs-8k-churn-wide` sorts values by `series_id, body, time_unix_nano` (the
wide custom key) and starts a new series with every request.
`logs-512k-near-limit` is the legal near-row-limit fixture: 512KiB bodies
under the 1MiB row limit, checked by `check_fixture` before it is measured.
The first two run the whole fifteen-stage matrix; the other two run
extract, sort_seal, merge, encode (ZSTD and uncompressed) and sink.

## Recorded numbers (medians of three repetitions, from stages.json)

| workload | stage | mode | compression | records/s/core | wall ns/record | CPU ns/record | allocated B/record | peak RSS MiB | peak live heap MiB | peak workspace MiB | output B/record | max CV |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| logs-1k-stable | convert | isolated | zstd | 2,698,389 | 370.58 | 370.59 | 5,176 | 78.8 | 60.8 | 40.2 | 1,997 | 0.02 |
| logs-1k-stable | encode | isolated | none | 1,066,374 | 937.74 | 937.76 | 7,162 | 143.1 | 94.8 | 51.8 | 1,065 | 0.01 |
| logs-1k-stable | encode | isolated | zstd | 623,136 | 1,605 | 1,605 | 8,728 | 153.2 | 77.5 | 34.6 | 755.31 | 0.21 |
| logs-1k-stable | extract | isolated | zstd | 919,373 | 1,088 | 1,088 | 9,797 | 81.6 | 108.4 | 49.6 | 2,505 | 0.00 |
| logs-1k-stable | local_write | async | zstd | 9,966,016 | 100.37 | 100.34 | 0.44 | 175.7 | 35.0 | 0.0 | 755.31 | 0.09 |
| logs-1k-stable | merge | isolated | zstd | 8,164,532 | 122.52 | 122.48 | 1,347 | 145.2 | 59.7 | 17.0 | 1,169 | 0.00 |
| logs-1k-stable | otlp_convert | criterion | zstd | 2,704,040 | 370.61 | 369.82 | 5,176 | 78.4 | 60.8 | 40.2 | 1,997 | 0.01 |
| logs-1k-stable | otlp_extract_hash | criterion | zstd | 695,801 | 1,436 | 1,437 | 14,973 | 80.3 | 70.5 | 49.8 | 2,505 | 0.00 |
| logs-1k-stable | otlp_minio | async | zstd | 230,795 | 7,832 | 4,333 | 27,977 | 148.9 | 82.1 | 61.4 | 755.34 | 0.05 |
| logs-1k-stable | otlp_noop | criterion | zstd | 51,282,051,282 | 0.01 | 0.02 | 0.72 | 74.2 | 20.6 | 0.0 | 0.00 | 0.02 |
| logs-1k-stable | otlp_noop | pipeline | zstd | 722,648 | 118,641 | 1,384 | 35,801 | 67.5 | 15.9 | 15.8 | 0.00 | 0.03 |
| logs-1k-stable | otlp_parquet_local | criterion | zstd | 302,730 | 3,391 | 3,303 | 26,395 | 173.8 | 99.3 | 78.7 | 1,065 | 0.02 |
| logs-1k-stable | otlp_parquet_zstd | criterion | zstd | 246,179 | 4,095 | 4,062 | 27,961 | 149.8 | 82.1 | 61.4 | 755.31 | 0.00 |
| logs-1k-stable | otlp_sort | criterion | zstd | 453,035 | 2,216 | 2,207 | 19,233 | 124.3 | 82.1 | 61.4 | 1,169 | 0.02 |
| logs-1k-stable | sink | async | zstd | 447,776 | 2,234 | 2,233 | 10,077 | 203.2 | 77.9 | 35.2 | 755.34 | 0.00 |
| logs-1k-stable | sort_seal | isolated | zstd | 2,185,571 | 457.59 | 457.55 | 2,691 | 107.5 | 94.4 | 26.0 | 1,159 | 0.01 |
| logs-1k-stable | upload | async | zstd | 2,586,425 | 3,481 | 386.63 | 13.15 | 119.8 | 35.1 | 0.1 | 755.31 | 0.02 |
| logs-512k-near-limit | encode | isolated | none | 3,674 | 272,260 | 272,202 | 3,489,247 | 252.3 | 236.6 | 140.6 | 524,450 | 0.04 |
| logs-512k-near-limit | encode | isolated | zstd | 1,901 | 526,015 | 525,948 | 3,598,114 | 275.7 | 187.2 | 91.1 | 391,172 | 0.02 |
| logs-512k-near-limit | extract | isolated | zstd | 16,415 | 60,918 | 60,921 | 2,531,436 | 117.7 | 151.5 | 53.9 | 553,775 | 0.01 |
| logs-512k-near-limit | merge | isolated | zstd | 78,249 | 12,776 | 12,780 | 538,610 | 140.6 | 111.5 | 15.5 | 524,471 | 0.03 |
| logs-512k-near-limit | sink | async | zstd | 1,529 | 654,160 | 653,956 | 4,137,318 | 263.3 | 202.7 | 106.6 | 391,178 | 0.01 |
| logs-512k-near-limit | sort_seal | isolated | zstd | 14,353 | 69,781 | 69,673 | 1,057,415 | 140.3 | 154.8 | 56.1 | 524,490 | 0.08 |
| logs-8k-churn-wide | encode | isolated | none | 237,367 | 4,213 | 4,213 | 46,735 | 268.6 | 206.2 | 111.2 | 8,238 | 0.01 |
| logs-8k-churn-wide | encode | isolated | zstd | 143,908 | 6,952 | 6,949 | 69,466 | 273.7 | 199.6 | 104.6 | 6,126 | 0.03 |
| logs-8k-churn-wide | extract | isolated | zstd | 495,536 | 2,018 | 2,018 | 51,026 | 132.2 | 190.1 | 75.0 | 12,849 | 0.00 |
| logs-8k-churn-wide | merge | isolated | zstd | 343,842 | 2,908 | 2,908 | 25,706 | 240.8 | 159.6 | 64.7 | 8,346 | 0.01 |
| logs-8k-churn-wide | sink | async | zstd | 72,902 | 13,724 | 13,717 | 95,181 | 332.4 | 248.1 | 153.2 | 6,126 | 0.12 |
| logs-8k-churn-wide | sort_seal | isolated | zstd | 457,809 | 2,185 | 2,184 | 19,246 | 181.3 | 172.8 | 52.1 | 8,337 | 0.01 |
| metrics-mixed | convert | isolated | zstd | 2,016,376 | 495.95 | 495.94 | 7,082 | 40.0 | 29.1 | 28.2 | 1,732 | 0.00 |
| metrics-mixed | encode | isolated | none | 6,142,204 | 162.82 | 162.81 | 966.28 | 43.4 | 7.3 | 3.7 | 58.07 | 0.00 |
| metrics-mixed | encode | isolated | zstd | 4,548,914 | 219.83 | 219.83 | 1,014 | 47.5 | 7.7 | 4.1 | 21.13 | 0.01 |
| metrics-mixed | extract | isolated | zstd | 752,545 | 1,329 | 1,329 | 5,488 | 40.0 | 47.7 | 22.1 | 1,448 | 0.00 |
| metrics-mixed | local_write | async | zstd | 405,515,004 | 2.47 | 2.47 | 0.46 | 46.8 | 1.2 | 0.0 | 21.13 | 0.01 |
| metrics-mixed | merge | isolated | zstd | 9,815,855 | 101.88 | 101.88 | 578.40 | 40.7 | 8.2 | 4.5 | 192.28 | 0.01 |
| metrics-mixed | otlp_convert | criterion | zstd | 2,003,637 | 494.38 | 499.09 | 7,082 | 40.1 | 29.1 | 28.2 | 1,732 | 0.01 |
| metrics-mixed | otlp_extract_hash | criterion | zstd | 546,996 | 1,840 | 1,828 | 12,570 | 34.2 | 23.4 | 22.5 | 1,448 | 0.00 |
| metrics-mixed | otlp_minio | async | zstd | 370,277 | 3,785 | 2,701 | 16,147 | 47.7 | 29.8 | 28.9 | 21.17 | 0.01 |
| metrics-mixed | otlp_noop | criterion | zstd | 46,875,000,000 | 0.01 | 0.02 | 0.72 | 31.5 | 0.9 | 0.0 | 0.00 | 0.00 |
| metrics-mixed | otlp_noop | pipeline | zstd | 2,763,673 | 7,033 | 361.84 | 43,334 | 66.3 | 15.9 | 15.8 | 0.00 | 0.24 |
| metrics-mixed | otlp_parquet_local | criterion | zstd | 388,722 | 2,597 | 2,573 | 16,088 | 47.6 | 29.8 | 28.9 | 58.07 | 0.03 |
| metrics-mixed | otlp_parquet_zstd | criterion | zstd | 381,395 | 2,631 | 2,622 | 16,136 | 47.5 | 29.8 | 28.9 | 21.13 | 0.03 |
| metrics-mixed | otlp_sort | criterion | zstd | 420,693 | 2,370 | 2,377 | 15,122 | 44.1 | 29.8 | 28.9 | 192.28 | 0.01 |
| metrics-mixed | sink | async | zstd | 2,916,526 | 343.01 | 342.87 | 1,596 | 48.1 | 10.3 | 6.6 | 21.17 | 0.01 |
| metrics-mixed | sort_seal | isolated | zstd | 2,928,984 | 341.63 | 341.42 | 1,676 | 42.9 | 26.2 | 4.6 | 199.12 | 0.00 |
| metrics-mixed | upload | async | zstd | 65,147,995 | 1,051 | 15.35 | 7.09 | 46.5 | 1.2 | 0.1 | 21.13 | 0.03 |

`peak RSS` is the resident high-water mark of one steady-state iteration
after the loop (the mark is reset first); `peak live heap` is the DHAT
workspace peak plus the fixture and prepared input the stage held
throughout, measured through Arrow's own deduplicated accounting; `peak
workspace` is what DHAT saw the stage allocate above those fixtures.
`output B/record` is the stage's own output representation per input record:
Arrow pinned bytes after conversion, extracted rows, sealed block bytes,
merged chunk bytes, Parquet bytes, or stored object bytes.

Highlights of the full ladder on `logs-1k-stable` (20,000 log records of
1KiB bodies, one core, ZSTD where it applies):

- `otlp_noop`, mode `criterion` (the synchronous floor: prepared OTLP bytes
  consumed by the noop path): 0.01 ns/record, 0.72 B/record allocated,
  output measured zero with `output_representation: none`.
- `otlp_noop`, mode `pipeline` (the real engine, OTLP receiver to noop
  exporter, acknowledged): 722,648 records/s per CPU-second of engine time,
  1,384 CPU ns/record, 118,641 wall ns/record end to end (the Python
  producer, not the engine, sets that wall time), 66.8 MiB peak RSS,
  35,801 B/record allocated over the whole instrumented lifetime including
  start-up. The two modes are separate rows and neither substitutes for the
  other. Storage oracle: not applicable, recorded as such.
- Criterion medians up the ladder: `otlp_convert` 370.6 ns/record,
  `otlp_extract_hash` 1,435.7, `otlp_sort` 2,215.6, `otlp_parquet_local`
  3,391.0, `otlp_parquet_zstd` 4,094.7. The isolated stages agree: convert
  370, extract 1,096, sort_seal 447, merge 122, encode(none) 941,
  encode(zstd) 1,605.
- `otlp_minio` (the same ladder through the actual Sink into MinIO):
  7,831.6 wall ns/record at 4,332.8 CPU ns/record, 230,795 records/s/core --
  3,499 ns/record of the wall time is store wait that the CPU clock does not
  see. `upload` alone (pre-encoded bytes, real S3 `BufWriter`, HEAD size and
  downloaded hash verified): 3,481.1 wall ns/record at 386.6 CPU ns/record.
  `local_write` through the actual `LocalFileSystem` store: 100.4 wall
  ns/record; `sink` end to end to the local store: 2,233.6 ns/record.
- Conversion expansion: 1,997 output bytes per 1KiB input record, about
  1.85x the wire record; extraction 2,505 B/record; the sealed block
  1,169 B/record; ZSTD Parquet 755 B/record, 0.30x the extracted bytes and
  1.41x smaller than uncompressed Parquet at 1,065 B/record, for 1.7x the
  encoder CPU.
- Encoder expansion on the near-limit fixture: 3.6 MB allocated per 512KiB
  record and a 95.5 MiB workspace peak for a 96-record block.

Stability: every aggregate but two is at or below 0.14, and most are below
0.03. The two exceptions are in the concerns below.

Equivalence: each `encode` (ZSTD) and `sink` fixture ran
`sink_equivalence`, which compared 2 files, their row groups and every
column chunk against actual Sink output with no mismatch; the per-run
counts are in each child's `observation.equivalence`. `merge` verified the
order of every chunk and chunk boundary and the exact row multiset against
its input runs; `extract` verified that every series id is the hash of its
canonical identity bytes and that the distinct series equal the sidecar's;
`upload` verified HEAD size and downloaded hash; `sink` verified that every
column chunk is ZSTD and that the descriptor rows written match the
fixture's series after cache suppression.

## Build activity during the measured phases

The controller asked which of two cases held. **The second one did, in its
milder form, and it is fixed.**

- No measured window ever saw a compiler. All 312 committed children record
  `build_monitor: {"detected": false, "observation_count": 0}`, every one
  passes `no_concurrent_build` and `build_monitor_coverage` (2 to 185 scans
  each, largest tick gap 0.0656 s, limit 0.1 s), and no child carries a build
  or invalidation event. The committed `stages.json` therefore records no
  build detection and no invalidation.
- But the harness did invoke cargo from inside the run, in its setup phase
  before the first lease: `locate_benches` asked
  `cargo bench ... --no-run --message-format=json` where the executables
  were, once without and once with `--features bench-heap`, and switching
  the feature set makes cargo relink both bench targets. That is the rustc
  seen at 20:22, a minute into the third family run, before any lease or
  monitor existed.
- Fix (commit df08c9e04): the family no longer runs cargo anywhere. Both
  benches answer a new `--describe` with their target name, whether DHAT's
  allocator is installed and whether they carry debug assertions;
  `locate_benches` takes `SERIES_STAGE_BENCH`, `SERIES_STAGE_BENCH_HEAP` or
  `SERIES_LAYERED_BENCH`, or scans `target/release/deps`, probes the
  candidates and picks the one that identifies itself as the build the
  profile needs. A debug build is refused, and a missing executable fails
  the family before anything is measured with the exact `cargo bench
  --no-run` command that builds it. Three contract tests cover this: a debug
  build is refused, a missing bench raises without starting any subprocess,
  and two coexisting builds of the same bench are told apart by what they
  are rather than by their file name.
- The committed evidence is a fresh family run made after the fix. A sampler
  looked for `cargo`, `rustc`, `cc1` and `ld` every two seconds from the
  first second of that run to its last and recorded none; the superseded
  evidence of the earlier run was removed and replaced in the same commit.

## Failures, deviations and concerns

1. **The index is `failed` on two aggregates**, both refused by the 15
   percent stability rule, so neither wrote a baseline:
   - `stages-encode-isolated-logs-1k-stable-zstd-timing`: CPU and wall are
     stable to 0.4 percent (1,611 / 1,600 / 1,605 ns/record) but the peak
     RSS of one repetition is 215.5 MiB against 153.2 and 152.8 MiB, a
     coefficient of variation of 0.21.
   - `stages-otlp_noop-pipeline-metrics-mixed-zstd-pipeline`: engine CPU
     over the 0.1 s input window is 290 / 467 / 362 ns/record, a coefficient
     of variation of 0.24, while its wall time varies by 1.9 percent.
   Both are instabilities of the measurement, not of the stage: the resident
   peak of a bench process and the engine CPU of a tenth-of-a-second window.
   The earlier run refused a different aggregate (`sink` timing peak RSS,
   0.17) and passed these two, which is the same pattern. Task 12 items,
   with the obvious remedies: give the pipeline baseline a longer input
   window, and either measure the resident peak over several iterations or
   give it its own stability band. I did not rerun until green and did not
   loosen the rule.
2. **The RSS reconciliation is defined per iteration.** The timing process
   resets the resident high-water mark once more after the loop and runs one
   further iteration, and it is that steady-state growth that is reconciled
   against the paired one-iteration heap workspace, with the whole-loop
   growth recorded beside it. Reconciling the whole loop would compare
   allocator retention accumulated over up to millions of iterations with a
   single iteration's heap, which failed the nanosecond-scale layers by
   80 MiB. Resident growth the profiled heap does not account for still
   fails at the frozen tolerance; growth smaller than the heap workspace is
   recorded as explained, because the fixtures had already made those pages
   resident. Every pair of the committed run is inside the tolerance.
3. **Criterion's one-second rule.** With `iter_batched`, Criterion sizes its
   iterations from a warm-up that includes the batched preparation, so a
   layer whose measured operation is shorter than its preparation finishes
   its five-second group having timed less than one second (0.58 s for
   `otlp_noop`). The Criterion child therefore gates on 30 samples with at
   least one iteration each and records its measured time; the one-second
   rule is enforced on the purpose-built timing process of the same stage,
   which times the same operation without Criterion's scheduling.
4. **The pipeline allocation metric is a whole-lifetime total.** DHAT can
   only be read at process exit, so `allocated_bytes_per_record` of the
   pipeline baseline is the whole instrumented lifetime divided by the
   records, which includes start-up. An idle control lifetime of the same
   build runs first in every heap repetition, and both its totals and the
   difference are recorded (`observations.dhat_idle`,
   `allocated_above_idle_bytes_per_record`). Subtracting the control as the
   metric was tried and rejected: it is a difference of two noisy lifetimes
   and its coefficient of variation reached 1.2.
5. **Files outside the brief's list were changed**, all additively:
   `measurement.py` (unit suffixes for the stage metric names, the `stage`
   and `noop` topologies, `pinned=` snapshots and `watch_pinned`),
   `test_e2e.py` (the `noop` topology and its expected edges) and the
   harness `README.md`. The 18 legacy tests and the 147 earlier contracts
   are unchanged and pass.
6. **The committed evidence is 519 files, 22 MB.** Every stage subprocess is
   its own run file, as the plan requires; roughly 17 KB of each child is
   the two mandatory environment snapshots. If the controller wants the tree
   smaller, the lever is the snapshot detail, not the number of runs.
7. **`records_per_s_per_core` of a store stage is a per-record rate over
   pre-encoded input**, so `upload` and `local_write` show very large values
   (they pay per object, not per record). The denominators are stated in
   each row's `output_representation`; Task 4 should attribute with the
   per-stage CPU, not with these rates.
8. **Not run**: `cargo xtask check` (the full workspace check) was not run;
   `cargo xtask check-benches` was, as the brief asks. Nothing in the brief
   was skipped.

## Artifacts

- `docs/superpowers/reports/series-parquet-measurement/stages.json` (index:
  138 registered stage results, 46 stage summaries, 416 run files, 102
  baselines), plus the 416 child and aggregate run files and the 102
  immutable baselines, all staged by name through `measure stage-results`.
- Raw per-run artifacts (bench reports, Criterion estimates and samples,
  engine logs, `dhat-heap.json`) stay under `/tmp/series-stages/<run_id>/`
  and are referenced with their hashes from each run file.

## Fix round 1

All four findings are addressed; the whole stage matrix was re-measured
once afterwards, because fixes 1 to 3 change the numbers.

The review's line numbers were written against 5c7c49e98, before
df08c9e04; every finding was applied by meaning against the current head,
and **all four still applied there**. df08c9e04 only replaced the cargo
lookup with `--describe` discovery: it touched neither the timers, nor the
Criterion sample gate, nor the recorded denominators, nor the completion
semantics.

One publishable re-measurement was made, after all four fixes were in.
Two earlier attempts were abandoned because they exposed defects in the
fixes themselves -- `objects_count` was inserted into the wrong match arm
so the new object rates were absent, and Criterion sanitizes the " #N" it
appends to a repeated benchmark id, so an attempt's artifacts could not be
read back under the name the attempt knew itself by. Neither attempt's
evidence was committed; the working-tree files were removed before the
next run, so the report directory holds exactly one family.

**1. Setup no longer sits inside a timer (Important).** `Prepared` gained a
`Cumulative` variant that carries the warmed series cache and the
destination of the iteration, and an `Objects` variant that carries the
pre-encoded bytes with the object path each one is written to. `prepare`
now builds: the synthetic committed cache of `otlp_sort`,
`otlp_parquet_local`, `otlp_parquet_zstd` and `otlp_minio`; the local
destination paths those Parquet layers write to; the `Sink` and its file
identity for `otlp_minio` and `sink`; and the `Bytes` clones and object
paths of `upload` and `local_write`. `run` now contains only the stage's
own work. Two self-tests hold this in place: `every_stage_is_handed_its_setup`
asks all fifteen stages for one iteration's input and asserts what each one
carries (`wire`, `wire+cache`, `wire+cache+paths`, `wire+cache+sink`,
`objects+paths`, `sink`, `buffers`, `records`, `extracted+cache`,
`fixture`), and `fixture_cost_does_not_move_the_measurement` samples the
same trivial operation with a two-millisecond and a twenty-millisecond
preparation and requires both medians to stay under a millisecond.

**2. Criterion now has to measure a full second (Important).** The layer
bench times its own untimed verification pass, scales the group's
measurement time by the preparation-to-work ratio it measured (floor: the
brief's five seconds, cap: sixty), reads back the sample file Criterion
just wrote and repeats the group under a new benchmark id, up to three
times, until the measured total reaches one second. `criterion_samples_ok`
in the harness now requires 30 samples and one second of measured work for
Criterion children as well; a short group fails `minimum_samples` and its
aggregate writes no baseline. `test_a_short_criterion_group_fails` pins the
rule with the 0.572 s group the review found.

**3. Denominators are explicit (Important).** Every registered stage result
now carries `input_representation` (what the stage was handed:
`otlp_wire_bytes`, `otap_arrow_records`, `extracted_rows`, `sealed_block`,
`merged_chunks` or `pre_encoded_parquet_bytes`), `denominator` (the
sentence "logical upstream records of the input file (log records or metric
points), whatever form this stage's own input takes") and a `rates` block
with `records_per_s`, `output_bytes_per_s`, `objects_per_s` and
`bytes_per_object`. For `upload` and `local_write`, whose timed input is
already encoded, `input_bytes_per_s` is required too and
`validate_stage_result` rejects a result that reports only a per-record
rate. `test_denominators_are_explicit` covers every one of those refusals.

**4. Completion semantics are recorded (Minor).** `COMPLETION_SEMANTICS` in
stages.rs documents and records, in every `upload`, `local_write` and
`sink` result, that completion means the store reported the object written
and the stage read the bytes back and verified them, and that it is not a
host power-loss durability claim because `object_store` does not fsync its
local backend. It is carried into each stage result as
`completion_semantics`.

### Re-measurement after the fixes

`SERIES_MEASURE_LONG=1 measure stages --output-dir /tmp/series-stages`, with
the four binaries prebuilt and a sampler watching for `cargo`, `rustc`,
`cc1` and `ld` every two seconds (none seen): 312 children, all passed; 104
profile aggregates, 103 passed and 103 baselines written; 138 registered
stage results; 814 s wall. Index `stages.json` is **failed** on one
aggregate, `stages-encode-isolated-logs-1k-stable-zstd-timing`: its CPU and
wall times agree to 0.3 percent across the three repetitions while its peak
RSS is 215.4 MiB against 152.9 and 152.8 MiB, a coefficient of variation of
0.21. That is the same peak-RSS instability the previous run showed on
`sink`; it is recorded, no baseline was written for it, and it was not
rerun until green.

### Is the pipeline baseline's 0.1 s window itself the defect?

Yes, and it is a dose problem rather than a code one. The controller asked
about the `otlp_noop` pipeline CPU refusal of the previous run. The engine
CPU that repetition divides by its records is a very small quantity: over
`metrics-mixed` the producer's whole input lasts 0.106, 0.128 and 0.106 s
and the engine spends 5.3, 5.0 and 5.9 ms of CPU in it; over
`logs-1k-stable` the input lasts about 2.36 s and the engine spends 23.3,
28.3 and 26.9 ms. A stray millisecond -- one scheduling artefact, one
telemetry collection landing inside the window instead of beside it -- is
therefore a fifth of the `metrics-mixed` measurement, which is how that
aggregate reached a coefficient of variation of 0.24 in the previous run
and 0.08 in this one, with the logs one at 0.10 in both.

The clock is not the limit: engine CPU is read from the scheduler's
nanosecond counters, summed over every thread, since the fix that replaced
the ten-millisecond tick counters. What is too small is the dose. Task 12
should give the pipeline baseline a window of several seconds of engine
CPU -- more requests per repetition, or repeated passes of the same
workload with the ledger counting each -- rather than change how the CPU is
measured. Until then the pipeline baseline's CPU is an order-of-magnitude
calibration, not a number to attribute against, and its wall time (which
measures the Python producer, not the engine) is stable to 0.5 percent on
`logs-1k-stable`. Every Criterion child now measures at least a second
(4.4 s to 5.0 s for the ordinary layers, 1.0 s to 1.3 s for `otlp_noop`),
and every `local_write` and `upload` result carries its object and byte
rates.

### How much moving setup out of the timers changed each stage

| workload | stage | mode | comp | wall ns/record before | after | change | CPU ns/record before | after | change |
|---|---|---|---|---|---|---|---|---|---|
| logs-1k-stable | convert | isolated | zstd | 370.58 | 365.69 | -1.3% | 370.59 | 365.65 | -1.3% |
| logs-1k-stable | encode | isolated | none | 937.74 | 935.00 | -0.3% | 937.76 | 934.49 | -0.3% |
| logs-1k-stable | encode | isolated | zstd | 1,605 | 1,610 | +0.3% | 1,605 | 1,610 | +0.3% |
| logs-1k-stable | extract | isolated | zstd | 1,088 | 1,078 | -0.9% | 1,088 | 1,078 | -0.9% |
| logs-1k-stable | local_write | async | zstd | 100.37 | 99.47 | -0.9% | 100.34 | 99.42 | -0.9% |
| logs-1k-stable | merge | isolated | zstd | 122.52 | 124.27 | +1.4% | 122.48 | 124.24 | +1.4% |
| logs-1k-stable | otlp_convert | criterion | zstd | 370.61 | 366.90 | -1.0% | 369.82 | 365.25 | -1.2% |
| logs-1k-stable | otlp_extract_hash | criterion | zstd | 1,436 | 1,434 | -0.1% | 1,437 | 1,432 | -0.3% |
| logs-1k-stable | otlp_minio | async | zstd | 7,832 | 7,807 | -0.3% | 4,333 | 4,383 | +1.2% |
| logs-1k-stable | otlp_noop | criterion | zstd | 0.01 | 0.01 | +0.9% | 0.02 | 0.02 | +2.6% |
| logs-1k-stable | otlp_noop | pipeline | zstd | 118,641 | 118,221 | -0.4% | 1,384 | 1,344 | -2.9% |
| logs-1k-stable | otlp_parquet_local | criterion | zstd | 3,391 | 3,300 | -2.7% | 3,303 | 3,485 | +5.5% |
| logs-1k-stable | otlp_parquet_zstd | criterion | zstd | 4,095 | 4,105 | +0.2% | 4,062 | 4,083 | +0.5% |
| logs-1k-stable | otlp_sort | criterion | zstd | 2,216 | 2,187 | -1.3% | 2,207 | 2,204 | -0.1% |
| logs-1k-stable | sink | async | zstd | 2,234 | 2,230 | -0.2% | 2,233 | 2,230 | -0.1% |
| logs-1k-stable | sort_seal | isolated | zstd | 457.59 | 457.31 | -0.1% | 457.55 | 457.31 | -0.1% |
| logs-1k-stable | upload | async | zstd | 3,481 | 3,454 | -0.8% | 386.63 | 395.73 | +2.4% |
| logs-512k-near-limit | encode | isolated | none | 272,260 | 271,824 | -0.2% | 272,202 | 271,839 | -0.1% |
| logs-512k-near-limit | encode | isolated | zstd | 526,015 | 525,851 | -0.0% | 525,948 | 525,678 | -0.1% |
| logs-512k-near-limit | extract | isolated | zstd | 60,918 | 60,556 | -0.6% | 60,921 | 60,559 | -0.6% |
| logs-512k-near-limit | merge | isolated | zstd | 12,776 | 12,373 | -3.2% | 12,780 | 12,378 | -3.1% |
| logs-512k-near-limit | sink | async | zstd | 654,160 | 652,952 | -0.2% | 653,956 | 652,799 | -0.2% |
| logs-512k-near-limit | sort_seal | isolated | zstd | 69,781 | 70,447 | +1.0% | 69,673 | 70,423 | +1.1% |
| logs-8k-churn-wide | encode | isolated | none | 4,213 | 4,170 | -1.0% | 4,213 | 4,170 | -1.0% |
| logs-8k-churn-wide | encode | isolated | zstd | 6,952 | 7,144 | +2.8% | 6,949 | 7,143 | +2.8% |
| logs-8k-churn-wide | extract | isolated | zstd | 2,018 | 2,014 | -0.2% | 2,018 | 2,014 | -0.2% |
| logs-8k-churn-wide | merge | isolated | zstd | 2,908 | 2,783 | -4.3% | 2,908 | 2,783 | -4.3% |
| logs-8k-churn-wide | sink | async | zstd | 13,724 | 13,976 | +1.8% | 13,717 | 13,968 | +1.8% |
| logs-8k-churn-wide | sort_seal | isolated | zstd | 2,185 | 2,173 | -0.5% | 2,184 | 2,172 | -0.6% |
| metrics-mixed | convert | isolated | zstd | 495.95 | 505.90 | +2.0% | 495.94 | 505.91 | +2.0% |
| metrics-mixed | encode | isolated | none | 162.82 | 163.70 | +0.5% | 162.81 | 163.67 | +0.5% |
| metrics-mixed | encode | isolated | zstd | 219.83 | 221.16 | +0.6% | 219.83 | 221.14 | +0.6% |
| metrics-mixed | extract | isolated | zstd | 1,329 | 1,343 | +1.0% | 1,329 | 1,342 | +1.0% |
| metrics-mixed | local_write | async | zstd | 2.47 | 2.43 | -1.5% | 2.47 | 2.43 | -1.5% |
| metrics-mixed | merge | isolated | zstd | 101.88 | 101.43 | -0.4% | 101.88 | 101.44 | -0.4% |
| metrics-mixed | otlp_convert | criterion | zstd | 494.38 | 504.30 | +2.0% | 499.09 | 503.93 | +1.0% |
| metrics-mixed | otlp_extract_hash | criterion | zstd | 1,840 | 1,840 | +0.0% | 1,828 | 1,842 | +0.8% |
| metrics-mixed | otlp_minio | async | zstd | 3,785 | 4,048 | +6.9% | 2,701 | 2,844 | +5.3% |
| metrics-mixed | otlp_noop | criterion | zstd | 0.01 | 0.02 | +3.4% | 0.02 | 0.02 | +3.1% |
| metrics-mixed | otlp_noop | pipeline | zstd | 7,033 | 7,078 | +0.6% | 361.84 | 354.50 | -2.0% |
| metrics-mixed | otlp_parquet_local | criterion | zstd | 2,597 | 2,548 | -1.9% | 2,573 | 2,618 | +1.8% |
| metrics-mixed | otlp_parquet_zstd | criterion | zstd | 2,631 | 2,676 | +1.7% | 2,622 | 2,661 | +1.5% |
| metrics-mixed | otlp_sort | criterion | zstd | 2,370 | 2,337 | -1.4% | 2,377 | 2,322 | -2.3% |
| metrics-mixed | sink | async | zstd | 343.01 | 343.55 | +0.2% | 342.87 | 343.57 | +0.2% |
| metrics-mixed | sort_seal | isolated | zstd | 341.63 | 347.32 | +1.7% | 341.42 | 347.24 | +1.7% |
| metrics-mixed | upload | async | zstd | 1,051 | 1,073 | +2.0% | 15.35 | 15.17 | -1.2% |

The setup that moved -- warming the synthetic committed cache, cloning
already reference-counted `Bytes`, building a `Sink` and formatting object
paths -- is small beside the work each stage does, so no stage moved beyond
its own run-to-run spread: every change is within 3 percent except
`metrics-mixed` `otlp_minio` (+6.9 percent wall) and `logs-1k-stable`
`otlp_parquet_local` (+5.5 percent CPU), both store-bound or Criterion-timed
stages whose repetition coefficients of variation are themselves several
percent. The numbers are therefore confirmed rather than corrected, and they
are now measured under the rule rather than close to it.

### Recorded numbers after the fixes (medians of three repetitions)

| workload | stage | mode | compression | input | records/s/core | wall ns/record | CPU ns/record | allocated B/record | peak RSS MiB | peak live heap MiB | peak workspace MiB | output B/record | max CV |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| logs-1k-stable | convert | isolated | zstd | otlp_wire_bytes | 2,734,830 | 365.69 | 365.65 | 5,176 | 78.6 | 60.8 | 40.2 | 1,997 | 0.00 |
| logs-1k-stable | encode | isolated | none | merged_chunks | 1,070,105 | 935.00 | 934.49 | 7,162 | 143.3 | 94.8 | 51.8 | 1,065 | 0.00 |
| logs-1k-stable | encode | isolated | zstd | merged_chunks | 621,084 | 1,610 | 1,610 | 8,728 | 152.9 | 77.5 | 34.6 | 755.31 | 0.21 |
| logs-1k-stable | extract | isolated | zstd | otap_arrow_records | 927,516 | 1,078 | 1,078 | 9,797 | 81.4 | 108.4 | 49.6 | 2,505 | 0.00 |
| logs-1k-stable | local_write | async | zstd | pre_encoded_parquet_bytes | 10,058,035 | 99.47 | 99.42 | 0.41 | 175.7 | 35.0 | 0.0 | 755.31 | 0.09 |
| logs-1k-stable | merge | isolated | zstd | sealed_block | 8,048,776 | 124.27 | 124.24 | 1,347 | 144.9 | 59.7 | 17.0 | 1,169 | 0.01 |
| logs-1k-stable | otlp_convert | criterion | zstd | otlp_wire_bytes | 2,737,868 | 366.90 | 365.25 | 5,176 | 78.7 | 60.8 | 40.2 | 1,997 | 0.01 |
| logs-1k-stable | otlp_extract_hash | criterion | zstd | otlp_wire_bytes | 698,171 | 1,434 | 1,432 | 14,973 | 80.4 | 70.5 | 49.8 | 2,505 | 0.00 |
| logs-1k-stable | otlp_minio | async | zstd | otlp_wire_bytes | 228,140 | 7,807 | 4,383 | 27,754 | 152.6 | 78.5 | 57.8 | 755.34 | 0.03 |
| logs-1k-stable | otlp_noop | criterion | zstd | otlp_wire_bytes | 50,000,000,000 | 0.01 | 0.02 | 0.72 | 74.5 | 20.6 | 0.0 | 0.00 | 0.01 |
| logs-1k-stable | otlp_noop | pipeline | zstd | otlp_wire_bytes | 744,245 | 118,221 | 1,344 | 35,632 | 67.2 | 15.4 | 15.3 | 0.00 | 0.10 |
| logs-1k-stable | otlp_parquet_local | criterion | zstd | otlp_wire_bytes | 286,962 | 3,300 | 3,485 | 26,172 | 173.8 | 95.1 | 74.4 | 1,065 | 0.03 |
| logs-1k-stable | otlp_parquet_zstd | criterion | zstd | otlp_wire_bytes | 244,913 | 4,105 | 4,083 | 27,738 | 149.2 | 77.8 | 57.2 | 755.31 | 0.01 |
| logs-1k-stable | otlp_sort | criterion | zstd | otlp_wire_bytes | 453,648 | 2,187 | 2,204 | 19,010 | 124.1 | 77.8 | 57.2 | 1,169 | 0.01 |
| logs-1k-stable | sink | async | zstd | sealed_block | 448,434 | 2,230 | 2,230 | 10,077 | 217.5 | 77.9 | 35.2 | 755.34 | 0.00 |
| logs-1k-stable | sort_seal | isolated | zstd | extracted_rows | 2,186,703 | 457.31 | 457.31 | 2,691 | 106.0 | 94.4 | 26.0 | 1,159 | 0.00 |
| logs-1k-stable | upload | async | zstd | pre_encoded_parquet_bytes | 2,526,953 | 3,454 | 395.73 | 13.13 | 119.7 | 35.1 | 0.1 | 755.31 | 0.03 |
| logs-512k-near-limit | encode | isolated | none | merged_chunks | 3,679 | 271,824 | 271,839 | 3,489,247 | 260.2 | 236.6 | 140.6 | 524,450 | 0.03 |
| logs-512k-near-limit | encode | isolated | zstd | merged_chunks | 1,902 | 525,851 | 525,678 | 3,598,114 | 271.9 | 187.2 | 91.1 | 391,172 | 0.08 |
| logs-512k-near-limit | extract | isolated | zstd | otap_arrow_records | 16,513 | 60,556 | 60,559 | 2,531,436 | 117.6 | 151.5 | 53.9 | 553,775 | 0.01 |
| logs-512k-near-limit | merge | isolated | zstd | sealed_block | 80,789 | 12,373 | 12,378 | 538,610 | 148.6 | 111.5 | 15.5 | 524,471 | 0.03 |
| logs-512k-near-limit | sink | async | zstd | sealed_block | 1,532 | 652,952 | 652,799 | 4,137,318 | 269.0 | 202.7 | 106.6 | 391,178 | 0.01 |
| logs-512k-near-limit | sort_seal | isolated | zstd | extracted_rows | 14,200 | 70,447 | 70,423 | 1,057,415 | 140.3 | 154.8 | 56.1 | 524,490 | 0.08 |
| logs-8k-churn-wide | encode | isolated | none | merged_chunks | 239,825 | 4,170 | 4,170 | 46,735 | 268.7 | 206.2 | 111.2 | 8,238 | 0.01 |
| logs-8k-churn-wide | encode | isolated | zstd | merged_chunks | 139,997 | 7,144 | 7,143 | 69,466 | 287.7 | 199.6 | 104.6 | 6,126 | 0.02 |
| logs-8k-churn-wide | extract | isolated | zstd | otap_arrow_records | 496,571 | 2,014 | 2,014 | 51,026 | 132.7 | 190.1 | 75.0 | 12,849 | 0.00 |
| logs-8k-churn-wide | merge | isolated | zstd | sealed_block | 359,288 | 2,783 | 2,783 | 25,706 | 240.7 | 159.6 | 64.7 | 8,346 | 0.01 |
| logs-8k-churn-wide | sink | async | zstd | sealed_block | 71,590 | 13,976 | 13,968 | 95,181 | 331.7 | 248.1 | 153.2 | 6,126 | 0.13 |
| logs-8k-churn-wide | sort_seal | isolated | zstd | extracted_rows | 460,413 | 2,173 | 2,172 | 19,246 | 180.1 | 172.8 | 52.1 | 8,337 | 0.01 |
| metrics-mixed | convert | isolated | zstd | otlp_wire_bytes | 1,976,654 | 505.90 | 505.91 | 7,082 | 40.1 | 29.1 | 28.2 | 1,732 | 0.02 |
| metrics-mixed | encode | isolated | none | merged_chunks | 6,109,930 | 163.70 | 163.67 | 966.28 | 43.7 | 7.3 | 3.7 | 58.07 | 0.01 |
| metrics-mixed | encode | isolated | zstd | merged_chunks | 4,521,981 | 221.16 | 221.14 | 1,014 | 47.5 | 7.7 | 4.1 | 21.13 | 0.00 |
| metrics-mixed | extract | isolated | zstd | otap_arrow_records | 745,179 | 1,343 | 1,342 | 5,488 | 40.2 | 47.7 | 22.1 | 1,448 | 0.12 |
| metrics-mixed | local_write | async | zstd | pre_encoded_parquet_bytes | 411,522,634 | 2.43 | 2.43 | 0.43 | 47.1 | 1.2 | 0.0 | 21.13 | 0.06 |
| metrics-mixed | merge | isolated | zstd | sealed_block | 9,858,271 | 101.43 | 101.44 | 578.40 | 40.6 | 8.2 | 4.5 | 192.28 | 0.00 |
| metrics-mixed | otlp_convert | criterion | zstd | otlp_wire_bytes | 1,984,403 | 504.30 | 503.93 | 7,082 | 40.1 | 29.1 | 28.2 | 1,732 | 0.02 |
| metrics-mixed | otlp_extract_hash | criterion | zstd | otlp_wire_bytes | 542,794 | 1,840 | 1,842 | 12,570 | 34.1 | 23.4 | 22.5 | 1,448 | 0.01 |
| metrics-mixed | otlp_minio | async | zstd | otlp_wire_bytes | 351,586 | 4,048 | 2,844 | 15,848 | 51.1 | 25.5 | 24.7 | 21.17 | 0.07 |
| metrics-mixed | otlp_noop | criterion | zstd | otlp_wire_bytes | 45,454,545,455 | 0.02 | 0.02 | 0.72 | 31.6 | 0.9 | 0.0 | 0.00 | 0.02 |
| metrics-mixed | otlp_noop | pipeline | zstd | otlp_wire_bytes | 2,820,912 | 7,078 | 354.50 | 41,610 | 64.9 | 15.4 | 15.3 | 0.00 | 0.11 |
| metrics-mixed | otlp_parquet_local | criterion | zstd | otlp_wire_bytes | 381,920 | 2,548 | 2,618 | 15,790 | 46.8 | 25.5 | 24.7 | 58.07 | 0.08 |
| metrics-mixed | otlp_parquet_zstd | criterion | zstd | otlp_wire_bytes | 375,760 | 2,676 | 2,661 | 15,838 | 49.5 | 25.5 | 24.7 | 21.13 | 0.02 |
| metrics-mixed | otlp_sort | criterion | zstd | otlp_wire_bytes | 430,723 | 2,337 | 2,322 | 14,824 | 45.7 | 25.5 | 24.7 | 192.28 | 0.02 |
| metrics-mixed | sink | async | zstd | sealed_block | 2,910,626 | 343.55 | 343.57 | 1,596 | 48.4 | 10.3 | 6.6 | 21.17 | 0.00 |
| metrics-mixed | sort_seal | isolated | zstd | extracted_rows | 2,879,825 | 347.32 | 347.24 | 1,676 | 42.9 | 26.2 | 4.6 | 199.12 | 0.01 |
| metrics-mixed | upload | async | zstd | pre_encoded_parquet_bytes | 65,932,617 | 1,073 | 15.17 | 7.07 | 46.7 | 1.2 | 0.1 | 21.13 | 0.15 |

## Session state at handoff

Work is stopped and everything is saved.

- Commits on `series-parquet-exporter`: 5c7c49e98, df08c9e04, b0a397f8d
  (base 69d5b8eea). Nothing pushed, as instructed. The working tree is
  clean apart from two files that predate this task, `chatgpt-discussion.md`
  and `otap-dataflow.patch`, both untracked and untouched here.
- No measurement, engine, bench or watcher process is running. The MinIO
  container an abandoned attempt had left behind was removed; the only
  container still up, `buildx_buildkit_clickhouse-regression-builder`,
  belongs to another project and was left alone. The host measurement lease
  is free: its file still holds the last run's record, which is how a lease
  whose holder has exited looks, and acquiring it succeeds.
- The committed evidence is one family: `stages.json` plus 416 run files and
  103 baselines in
  `docs/superpowers/reports/series-parquet-measurement/`. Raw per-run
  artifacts of that run are under `/tmp/series-stages/<run_id>/` and are
  referenced by hash from each run file; they do not survive a reboot.
- Open items for Task 12, both deliberately preserved failures: the peak-RSS
  stability refusal of a timing aggregate (`encode` on `logs-1k-stable` in
  the committed run, `sink` in the one before), and the pipeline baseline's
  CPU measured over a 0.1 s window, which the section above argues is a dose
  problem rather than a code one.
- To rerun the family, build the four binaries first -- the run invokes no
  cargo and refuses to start without them -- and remove the published
  `stages*.json` and `baseline-stages-*.json` from the working tree if the
  new run is meant to replace this family rather than to be published beside
  it under the next ordinal.

## Fix round 2

Fix round 2 was started by the previous implementer, whose session was
restarted mid-round, and was finished by a second one. Its source edits were
on disk and uncommitted, and a measured run it had launched at 22:27 was
still in flight. That run's evidence was not published, for the reasons
under "Why a replacement run was needed" below. Commits: d6454dbb8 (source)
and 4f8ff1257 (the replacing family). Nothing pushed.

### 1. Each Criterion attempt is read under its own id

`layered.rs` now reads every attempt back through
`stages::criterion_measured_seconds(home, group, function_id)` with the id
that attempt ran under. The directory name goes through
`criterion_directory_name`, a copy of Criterion 0.8.2's
`report::make_filename_safe` on Linux: the ten unsafe characters become `_`
and the name is truncated to 64 bytes at a character boundary. A missing
`sample.json` is an error that names the id and lists what the group
directory holds; nothing falls back to another id. The harness's own reader,
`performance.criterion_estimates`, uses the same sanitization and refuses a
missing artifact the same way; its truncation is by UTF-8 bytes, as
Criterion's is.

The second implementer added one more link, because the reader alone did
not pin the call site. Each attempt in the layer summary now records its
`function_id`, and `performance.criterion_attempts_agree` reads every
attempt's own artifact and refuses the child unless the recorded seconds are
that artifact's and the last attempt is the one published. The stale bench
fails this check: every attempt repeated the first attempt's seconds.

Regression tests:
- `criterion_artifacts_are_read_per_attempt` (bench self-test) reads two
  attempts under their own ids and one sanitized id, and requires a missing
  id to be refused by name.
- `test_criterion_artifacts_are_read_per_attempt` (Python) does the same for
  the harness reader.
- `test_criterion_attempts_are_their_own_artifacts` (Python) replays the
  committed defect: three attempts all recording 0.780718548 s while the
  published third one measured 1.8843071 s. The check refuses it, and it
  refuses a missing attempt artifact and an attempt without an id.

In the new family, the three `otlp_noop` Criterion children on
`metrics-mixed` needed a second attempt. Each recorded its own seconds: for
example, 0.662 s for the first attempt and 1.264 s for `-attempt2`, and the
published artifact measured 1.263790956 s. All three repetitions finished
on the same attempt count, so this family publishes no mixture of first and
repeated attempts.

### 2. The two weak self-tests are replaced

- `run_never_builds_its_own_setup` replaces
  `every_stage_is_handed_its_setup`. Every one of the fifteen stages
  prepares and then actually runs one iteration. A per-stage counter,
  `setup_built`, advances once per warmed cache, sink, and destination path
  set. The test requires `prepare` to advance it for the eight stages that
  have setup, and requires `run` never to advance it for any stage.
  Destinations are now compared: two preparations of each of the six
  writing stages must name different paths, objects or sink identities. The
  layer bench applies the same counter to its own timed verification pass,
  fails if `run` built setup, and records `setup_built_in_prepare` and
  `setup_built_in_run` in each layer summary.
- `a_heavier_fixture_does_not_move_the_measurement` replaces
  `fixture_cost_does_not_move_the_measurement`. It samples the real
  `otlp_sort` layer, 30 samples through the real `sample_loop` with the
  stage's own `prepare` and `run`, twice. The first pass uses the ordinary
  fixture. The second adds 200,000 synthetic committed series, through
  `BenchConfig.extra_committed_series`, that the input never carries, so
  the admitted work is identical. The test requires the heavy fixture's
  preparation to cost more than three times the light one's, which is the
  order-of-magnitude check that the fixture really grew. It requires one
  preparation per sample. The stated tolerance is that the heavy median
  per-iteration time must stay below 1.25 times the light one. Measured
  configurations set `extra_committed_series` to 0, and
  `bench_config_document` writes that explicitly.
- Some stages cannot be pinned by this timing test, and are covered another
  way. Only `otlp_sort` carries the heavy knob. The other stages whose setup
  can grow are the Parquet, sink and object stages, whose setup is paths, a
  `Sink` and `Bytes` clones rather than a cache. For those the structural
  counter above is the pin: `run` cannot build any setup the counter
  tracks. `upload` and `otlp_minio` need an S3 store. The self-test runs
  them against the local filesystem backend that `bench_config` gives them,
  so their S3 client path is exercised only by the measured run itself.

### 3. A missing completion_semantics fails

`validate_stage_result` now refuses a result for `local_write`, `upload`,
`sink` or `otlp_minio` whose `completion_semantics` is absent, or does not
state that it is not power-loss durability.
`test_store_stages_record_completion_semantics` covers absent, weak and
correct values for each of the four stages. On the Rust side,
`store_stages_record_completion_semantics` runs `local_write`, `upload` and
`sink` and requires the recorded text to say "read back" and "not host
power-loss". It also requires `objects_count` to be positive. In the new
family, all 30 stage results of those four stages carry the field.

### 4. Setup-phase subprocesses and the scope of the compiler-free claim

`performance.SETUP_SUBPROCESSES` is written into the family result as
`environment.setup_subprocesses`. Before any child takes the host lease, the
setup phase starts these:
- `git status` and `git rev-parse HEAD`, for provenance.
- `rustc --version`, once per build fingerprint. It starts the compiler
  executable only to print its version, and compiles nothing.
- Each prebuilt bench executable with `--describe`.
- `docker info` and `docker image inspect`. The previous implementer's draft
  of this list left these two out.
- `docker run`, `docker port`, `docker inspect` and `docker update`, to start
  and pin the object store.

Inside each measured window, the only processes started are the bench or
engine under measurement, `host_monitor.py`, and that monitor's `docker ps`
and `docker events`. The recorded `compiler_free_claim` states that the
claim covers measured windows only. The build monitor saw no cargo, rustc,
cc1 or ld inside any child's window. The setup phase starts rustc once, for
`--version`, and never invokes cargo. `test_setup_subprocesses_are_recorded`
reads the source of each function that starts one of those commands. It
requires each command to appear in the recorded list, and requires the claim
to say "measured windows only".

### Verification

All of the following ran from `rust/otap-dataflow` after the last source
edit and before the replacing run:

```
/tmp/series-parquet-venv/bin/python3 -m unittest crates.validation.tests.series_parquet.test_measurement
  Ran 171 tests ... OK
cargo fmt -p otel-arrow-dfe-series-lake -- --check                                     exit 0
cargo clippy -p otel-arrow-dfe-series-lake --benches [--all-features] -- -D warnings   clean
cargo xtask check-benches                                                              Bench targets compiled successfully
target/release/deps/measurement-8bac121a1ae33e80 --self-test   (timing build)          exit 0
target/release/deps/measurement-4a1374d1f5daacab --self-test   (bench-heap build)      exit 0
```

The layered binaries were rebuilt for the `function_id` change, both the
timing and the `bench-heap` builds, before the lease was taken. No cargo or
rustc process was running when the run started.

### Why a replacement run was needed

The run launched at 22:27 already carried the previous implementer's fix
round 2 source. Its evidence went stale because of two later changes:

- **The layer bench now records each attempt's `function_id` in
  `layered.rs`.** The harness now refuses a Criterion child whose attempts
  do not match their own artifacts, in `criterion_attempts_agree`. The
  22:27 run's layer summaries carry no per-attempt ids, so the published
  harness would refuse every one of its Criterion children. They could not
  be committed as evidence for the code being committed.
- **That family's `setup_subprocesses` list left out `docker info` and
  `docker image inspect`.** It would have been an inaccurate record under
  item 4.

Independently, that run's tail was invalid. At 22:39:29 a TLA+ model
checker from another project started with `-workers auto`, 32 threads. From
that second on, 57 children failed `build_monitor_coverage`: monitor ticks
arrived 100 to 124 ms apart. That run finished at 22:41 with 271 of 312
children passed and 87 of 104 aggregates passed. It was not published. Its
raw artifacts are archived at
`.measurement-artifacts/stages-raw-20260922-2241.tgz`.

### The replacing family

The command was
`SERIES_MEASURE_LONG=1 measure stages --output-dir /tmp/series-stages`. It
started at 22:51:41 and ran for 826 s, at git revision d6454dbb8. The tree
was dirty only because the previous family's report files had been deleted
before the run.

| Measure | Result |
|---|---|
| Children | 312 of 312 passed |
| Aggregates | 95 of 104 passed |
| Baselines written | 95 |
| Registered stage results | 138 |
| Index | failed |

Before publishing, the 100 fix-round-1 baselines and all `stages*.json`
files were removed, so the report directory holds exactly one family.

**Environment.** Throughout the run, the user's parallel TLA+/TLC session
was running on this host with 8 workers, an 8 GB heap and nice 10. A
long-lived `codex` process was also present. The team lead relayed the
user's ruling that this is acceptable noise, not a run invalidator. The
children's start and end snapshots record the load average: 1-minute
values of about 2.5 to 12 during this run, against about 1.5 in fix round
1.

The nine refusals are preserved, not rerun:

| Aggregate | Check | Coefficient of variation or residual |
|---|---|---|
| encode, logs-1k-stable, zstd, timing | repetition_stability | CPU 0.29; one repetition at 2,598 against 1,609 ns/record |
| encode, metrics-mixed, none, timing | repetition_stability | CPU 0.27; one repetition at 280 against 180 |
| encode, metrics-mixed, zstd, timing | repetition_stability | CPU 0.25 |
| extract, logs-8k-churn-wide, zstd, timing | repetition_stability | CPU 0.19 |
| merge, logs-8k-churn-wide, zstd, timing | repetition_stability | CPU 0.19 |
| otlp_noop pipeline, metrics-mixed | repetition_stability | CPU 0.34; the known 0.1 s window dose problem |
| local_write, logs-1k-stable, zstd, heap | repetition_stability | peak workspace 0.18 |
| sink, logs-512k-near-limit, zstd, timing and heap | rss_reconciliation | residual 38.8 MB against a 33.5 MB tolerance, repetition 2 |

In each CPU refusal, one of three pinned single-thread repetitions is 40
to 60 percent above the other two. That fits host load, most plausibly the
TLC workers running on a bench core's hyperthread sibling. The snapshots do
not record TLC's affinity, so the sibling mechanism is not proven; see fix
round 3. The fix-round-1
peak-RSS refusal on encode, logs-1k-stable, zstd, timing did not recur;
its peak-RSS coefficient of variation was 0.0018 this time. The sink
512 KiB RSS residual also appeared in the 22:27 run, before TLC started. It
is new relative to fix round 1 and is routed to Task 12 with the other
stability refusals.

### Did a headline number move?

Yes. The committed snapshots show the load rising from about 2.5 to about
12, and no code change explains the slowdown. The cause is host load, most
plausibly TLC on SMT siblings; the snapshots record the bench's affinity but
not TLC's, so they do not prove that mechanism. Most stages are 8 to 47
percent slower than in fix round 1; the full table is below. The 22:27 run
provides the control. Its stages that ran before TLC started used the same
measured code as the replacing run, since the round-2 changes touch only
what `run` records, not what it does. At a load of about 1.6, those stages
reproduce fix round 1 within 3 percent. In the replacing run they slow down
in step with load:

| Stage, workload | Round 1, CPU ns/record | 22:27 run, before TLC | Replacing run | Load, 1 min, replacing run |
|---|---|---|---|---|
| sort_seal, metrics-mixed | 347 | 336 | 468 | 11.8 to 12.2 |
| extract, metrics-mixed | 1,342 | 1,339 | 1,598 | 11.6 to 12.2 |
| convert, logs-1k-stable | 366 | 365 | 394 | 2.5 |
| merge, logs-1k-stable | 124 | 123 | 122 | 2.5 to 2.7 |

Consequence: the 95 new baselines were measured on a loaded host. A later
run on a quiet host will look faster against them and pass the 25 percent
rule. A later run under heavier load than this one can fail that rule with
no code change. This should be weighed before these baselines are used to
judge Task 12.

#### Medians, fix round 1 against the replacing family

| workload | stage | mode | comp | wall ns/record round 1 | round 2 | change | CPU ns/record round 1 | round 2 | change | max CV |
|---|---|---|---|---|---|---|---|---|---|---|
| logs-1k-stable | convert | isolated | zstd | 365.69 | 394.06 | +7.8% | 365.65 | 394.04 | +7.8% | 0.01 |
| logs-1k-stable | encode | isolated | none | 935.00 | 941.44 | +0.7% | 934.49 | 941.38 | +0.7% | 0.00 |
| logs-1k-stable | encode | isolated | zstd | 1,610 | 1,610 | +0.0% | 1,610 | 1,609 | -0.1% | 0.30 |
| logs-1k-stable | extract | isolated | zstd | 1,078 | 1,092 | +1.3% | 1,078 | 1,092 | +1.3% | 0.00 |
| logs-1k-stable | local_write | async | zstd | 99.47 | 102.42 | +3.0% | 99.42 | 102.40 | +3.0% | 0.18 |
| logs-1k-stable | merge | isolated | zstd | 124.27 | 122.35 | -1.5% | 124.24 | 122.36 | -1.5% | 0.01 |
| logs-1k-stable | otlp_convert | criterion | zstd | 366.90 | 382.89 | +4.4% | 365.25 | 385.92 | +5.7% | 0.01 |
| logs-1k-stable | otlp_extract_hash | criterion | zstd | 1,434 | 1,487 | +3.7% | 1,432 | 1,465 | +2.3% | 0.01 |
| logs-1k-stable | otlp_minio | async | zstd | 7,807 | 8,192 | +4.9% | 4,383 | 4,778 | +9.0% | 0.01 |
| logs-1k-stable | otlp_noop | criterion | zstd | 0.01 | 0.01 | +1.2% | 0.02 | 0.02 | +0.0% | 0.03 |
| logs-1k-stable | otlp_noop | pipeline | zstd | 118,221 | 120,436 | +1.9% | 1,344 | 2,114 | +57.3% | 0.12 |
| logs-1k-stable | otlp_parquet_local | criterion | zstd | 3,300 | 3,196 | -3.2% | 3,485 | 3,299 | -5.3% | 0.04 |
| logs-1k-stable | otlp_parquet_zstd | criterion | zstd | 4,105 | 4,088 | -0.4% | 4,083 | 4,099 | +0.4% | 0.01 |
| logs-1k-stable | otlp_sort | criterion | zstd | 2,187 | 2,292 | +4.8% | 2,204 | 2,295 | +4.1% | 0.02 |
| logs-1k-stable | sink | async | zstd | 2,230 | 2,212 | -0.8% | 2,230 | 2,211 | -0.8% | 0.04 |
| logs-1k-stable | sort_seal | isolated | zstd | 457.31 | 462.80 | +1.2% | 457.31 | 462.58 | +1.2% | 0.02 |
| logs-1k-stable | upload | async | zstd | 3,454 | 3,552 | +2.8% | 395.73 | 386.71 | -2.3% | 0.04 |
| logs-512k-near-limit | encode | isolated | none | 271,824 | 313,637 | +15.4% | 271,839 | 313,627 | +15.4% | 0.04 |
| logs-512k-near-limit | encode | isolated | zstd | 525,851 | 628,805 | +19.6% | 525,678 | 628,791 | +19.6% | 0.03 |
| logs-512k-near-limit | extract | isolated | zstd | 60,556 | 72,873 | +20.3% | 60,559 | 72,811 | +20.2% | 0.01 |
| logs-512k-near-limit | merge | isolated | zstd | 12,373 | 17,600 | +42.2% | 12,378 | 17,601 | +42.2% | 0.06 |
| logs-512k-near-limit | sink | async | zstd | 652,952 | 766,795 | +17.4% | 652,799 | 766,459 | +17.4% | 0.05 |
| logs-512k-near-limit | sort_seal | isolated | zstd | 70,447 | 95,014 | +34.9% | 70,423 | 94,994 | +34.9% | 0.08 |
| logs-8k-churn-wide | encode | isolated | none | 4,170 | 5,054 | +21.2% | 4,170 | 5,054 | +21.2% | 0.00 |
| logs-8k-churn-wide | encode | isolated | zstd | 7,144 | 8,120 | +13.7% | 7,143 | 8,118 | +13.6% | 0.01 |
| logs-8k-churn-wide | extract | isolated | zstd | 2,014 | 2,324 | +15.4% | 2,014 | 2,323 | +15.4% | 0.19 |
| logs-8k-churn-wide | merge | isolated | zstd | 2,783 | 3,649 | +31.1% | 2,783 | 3,649 | +31.1% | 0.19 |
| logs-8k-churn-wide | sink | async | zstd | 13,976 | 16,415 | +17.5% | 13,968 | 16,403 | +17.4% | 0.00 |
| logs-8k-churn-wide | sort_seal | isolated | zstd | 2,173 | 2,600 | +19.6% | 2,172 | 2,599 | +19.6% | 0.02 |
| metrics-mixed | convert | isolated | zstd | 505.90 | 578.94 | +14.4% | 505.91 | 578.09 | +14.3% | 0.00 |
| metrics-mixed | encode | isolated | none | 163.70 | 180.51 | +10.3% | 163.67 | 180.51 | +10.3% | 0.27 |
| metrics-mixed | encode | isolated | zstd | 221.16 | 243.75 | +10.2% | 221.14 | 243.70 | +10.2% | 0.25 |
| metrics-mixed | extract | isolated | zstd | 1,343 | 1,599 | +19.1% | 1,342 | 1,598 | +19.1% | 0.01 |
| metrics-mixed | local_write | async | zstd | 2.43 | 2.80 | +15.0% | 2.43 | 2.79 | +15.0% | 0.01 |
| metrics-mixed | merge | isolated | zstd | 101.43 | 110.84 | +9.3% | 101.44 | 110.83 | +9.3% | 0.01 |
| metrics-mixed | otlp_convert | criterion | zstd | 504.30 | 587.64 | +16.5% | 503.93 | 563.23 | +11.8% | 0.01 |
| metrics-mixed | otlp_extract_hash | criterion | zstd | 1,840 | 2,016 | +9.6% | 1,842 | 2,009 | +9.1% | 0.00 |
| metrics-mixed | otlp_minio | async | zstd | 4,048 | 5,939 | +46.7% | 2,844 | 4,671 | +64.2% | 0.01 |
| metrics-mixed | otlp_noop | criterion | zstd | 0.02 | 0.02 | +18.8% | 0.02 | 0.02 | +6.1% | 0.05 |
| metrics-mixed | otlp_noop | pipeline | zstd | 7,078 | 6,404 | -9.5% | 354.50 | 355.96 | +0.4% | 0.34 |
| metrics-mixed | otlp_parquet_local | criterion | zstd | 2,548 | 2,883 | +13.2% | 2,618 | 2,563 | -2.1% | 0.07 |
| metrics-mixed | otlp_parquet_zstd | criterion | zstd | 2,676 | 2,688 | +0.5% | 2,661 | 3,026 | +13.7% | 0.09 |
| metrics-mixed | otlp_sort | criterion | zstd | 2,337 | 2,645 | +13.2% | 2,322 | 2,622 | +12.9% | 0.04 |
| metrics-mixed | sink | async | zstd | 343.55 | 385.11 | +12.1% | 343.57 | 385.11 | +12.1% | 0.01 |
| metrics-mixed | sort_seal | isolated | zstd | 347.32 | 467.78 | +34.7% | 347.24 | 467.70 | +34.7% | 0.01 |
| metrics-mixed | upload | async | zstd | 1,073 | 1,133 | +5.6% | 15.17 | 19.70 | +29.9% | 0.03 |

### Artifacts

- The committed family is `stages.json` plus its run files and 95
  baselines, in `docs/superpowers/reports/series-parquet-measurement/`.
- The replacing run's raw non-Parquet artifacts are archived at
  `.measurement-artifacts/stages-raw-20260922-2305.tgz`. The unpublished
  22:27 run's are at `.measurement-artifacts/stages-raw-20260922-2241.tgz`.
  That directory is git-ignored.
- Bulk Parquet output is deliberately not retained. Sizes and hashes of
  every stored object are in the committed results.
- The compiler-watch sampler the previous implementer started beside the
  22:27 run died with the session restart. Its empty output file is not
  evidence. For both runs, the evidence is the harness's per-child build
  monitor, whose checks are in each child result.

## Fix round 3

Commit ed4bc1e0e (source only). Nothing pushed. The committed family,
4f8ff1257, is unchanged. The attempted re-measurement could not start, for
the reasons under "Re-measurement" below.

### 1. The heavy-fixture control now holds the timed work identical

- **The control is now sort-and-seal.** It records the cache's own hit,
  miss and eviction counters in every output, which the cumulative sort
  layer does not. Both runs use a cache of 300,000 entries, with room for
  every real and synthetic id. `committed_series` now inserts the synthetic
  ids before the real ones, so the real ids are the most recently used and
  stay resident. Measured configurations set `extra_committed_series` to
  0, so the new order changes nothing they do.
- **Before comparing time, the test checks the cache counts.** Every sample
  of both runs must record the same (hits, misses, evictions). Evictions
  must be zero, and hits and misses must both be non-zero. The light and
  heavy counts must be equal. On this host every sample recorded (6, 6, 0).
- **With the control in place, the test exposed a real defect.** The series
  cache and the `Sink` were moved into the timed `run` and dropped there,
  so their teardown was timed. The exporter keeps both across every block,
  so production never runs that teardown per block. With 200,000 entries,
  dropping the cache took about 6 ms against about 0.15 ms of admission.
  The old 1,000-entry cache capped its size, which hid this. `run` now
  hands both back in `Output._kept`, which is dropped with the output after
  the timer stops. Sealed blocks are still dropped inside `run`, because
  the exporter drops each block after flushing it.
- **Prepare order now matches production.** After that fix, the heavy run
  was still about 36 percent slower. The heavy warm-up ran after the input
  conversion and wrote tens of megabytes, which pushed the prepared rows
  out of the CPU caches before the timer started. Every cache-carrying
  stage now warms its cache first and builds the input it admits last, as
  the exporter extracts a request right before admitting it.
- **The stated tolerance holds.** Over five runs, the heavy median
  per-iteration time was 12 to 17 percent above the light one. The
  tolerance is 25 percent. The heavy preparation was about 6.5 times the
  light one, and the test requires more than three times. The remaining
  gap is a larger cache table and heap, not setup inside the timer; the
  identical counters show the timed admission itself is the same.
- **The setup counter now covers more.** It counts every wire clone
  through a new counting `wire` method; `run` never calls it. It also
  counts the prepared conversion of `extract` and `sort_seal`, and the
  output buffer set of `encode`, in addition to caches, sinks, destination
  path sets and object sets. `run_never_builds_its_own_setup` now requires
  each stage's exact count per preparation. Setup moved into `run` lowers
  the count and fails. A clone, conversion or buffer allocation added
  inside `run` raises it and fails.

**Effect on the committed numbers.** The committed family was measured
with the cache and sink teardown inside the timer. A scratch program
measured the teardown of the same cache type: an `lru` 0.18.4 cache with
200,000 preallocated entries, pinned to core 1.

| Entries resident | Median drop |
|---|---|
| 24 | 4.7 us |
| 120 | 6.9 us |
| 200 | 8.3 us |
| 900 | 20.8 us |

Admission in the measured workloads touches 24 to 900 series per block.
That puts the teardown at 0.04 to 0.3 percent of a committed stage's time,
at most 1.4 ns per record, on metrics-mixed `sort_seal`. It is far inside
run-to-run spread. The committed numbers are therefore not materially
biased, but they were measured with this defect.

### 2. completion_semantics must be the bench's constant

`validate_stage_result` now accepts only a value in
`ALLOWED_COMPLETION_SEMANTICS`, which holds exactly one string: the bench's
`COMPLETION_SEMANTICS`, character for character. There is no substring
search. `test_store_stages_record_completion_semantics` refuses these
values for each storing stage:
- absent and empty;
- "the object was written";
- the reversed claim "this provides host power-loss durability";
- a variant claiming power-loss durability;
- a paraphrase of the correct meaning.

It accepts the constant. `test_completion_semantics_match_the_bench` parses
the Rust literal out of `stages.rs` and requires it to equal the Python
constant, so the two cannot drift. The Rust self-test now requires the
recorded value to equal `stages::COMPLETION_SEMANTICS`, which is now
public. All 30 committed store-stage results carry exactly that string, so
they pass the stricter rule.

### 3. The rustc count in the setup list

The setup phase starts `rustc --version` once per `engine_build` call. A
stages family makes five such calls: three bench builds and two engines.
`SETUP_SUBPROCESSES` now says so, and its claim reads "starts rustc five
times, each only for --version". `test_setup_subprocesses_are_recorded`
pins the number to the code. It requires one `engine_build` in
`bench_build`, three `bench_build` calls in `run_stages`, two
`engine_build` calls in `engine_binaries`, and the word "five" in both
recorded texts.

**The committed index was not corrected.** The harness has no command that
re-aggregates an index from committed children. Rebuilding one by hand
would mean re-running `publish_stages` against an immutable, already
baselined family. The committed `stages.json` therefore still says the
setup phase starts rustc "once". The truth is five times, each only for
`--version`, and none inside a measured window.

### Attribution of the round-2 slowdown

The fix round 2 wording is downgraded in place above. The committed
snapshots prove that the load average went from about 2.5 to about 12, and
that no code change explains the slowdown. They do not prove the
SMT-sibling mechanism, because they record the bench's affinity but not
TLC's. The cause is host load, most plausibly TLC on SMT siblings.

TLC is now pinned to cores 8-15 and 24-31; its process affinity read
8-15,24-31 at 23:46. Every later measured launch is meant to run under
`taskset -c 0-7,16-23`. The Task 3 baselines are therefore provisional
until they are re-measured under that regime.

### Re-measurement

Two changes move timed work and would normally require re-measuring the
family: the teardown fix and the new prepare order. The launch under
`taskset -c 0-7,16-23` was refused at setup:

```
AssertionError: no physical core is left for reader: the run needs one physical core per role and SMT siblings are never shared
```

The stages family needs nine physical cores:

| Role | Physical cores |
|---|---|
| Observability | 1 |
| Engine reservation | 4 |
| Producer | 2 |
| Store | 1 |
| Reader | 1 |

The pinned set gives eight. Making it fit means loosening the role
allocation, and a non-strict allocation fails `physical_cores_sufficient`
and can write no baseline. Either way breaks a rule, so the family was
not re-measured. The working-tree files the aborted launch removed were
restored from HEAD, and the report directory still holds the one
committed family.

Making the family re-measurable under the new regime needs a controller
decision. One option is a pinned set of at least nine physical cores.
Another is a stages allocation that drops roles it does not use; the
reader is not exercised by any stage.

### Verification

From `rust/otap-dataflow`, after the last source edit:

```
/tmp/series-parquet-venv/bin/python3 -m unittest crates.validation.tests.series_parquet.test_measurement
  Ran 172 tests ... OK
cargo fmt -p otel-arrow-dfe-series-lake -- --check                                     exit 0
cargo clippy -p otel-arrow-dfe-series-lake --benches [--all-features] -- -D warnings   clean
cargo xtask check-benches                                                              Bench targets compiled successfully
target/release/deps/measurement-8bac121a1ae33e80 --self-test   (timing build)          exit 0
target/release/deps/measurement-4a1374d1f5daacab --self-test   (bench-heap build)      exit 0
```

The four bench binaries were rebuilt from ed4bc1e0e. The self-test passed
three runs in a row before the margin was measured, and passed again after
the diagnostic print was removed.

## Fix round 3, re-measurement

Commits:
- 9d4fd4ad5: source, the per-case allocator and its tests.
- 42be4c5d3: evidence, the pinned family.

Nothing pushed. This family replaces the 95 provisional baselines of
4f8ff1257. The controller sanctioned that replacement because those
baselines were written on a loaded host whose TLC job shared SMT siblings
with the bench. The "Re-measurement" subsection of fix round 3 above is
superseded: the allocator no longer claims a reader core for stages.

### Per-case role allocation

`role_allocation` now requires a `roles` argument, and every case passes
its entry from `measurement.CASE_ROLES`:

| Case | Roles beyond the engine and its observability core | Physical cores |
|---|---|---|
| Engine case, `measure.py` | producer 2, store 1, reader 1 | 9 |
| Stages family | producer 2, store 1 | 8 |

A stages family runs the engine for its pipeline baseline, sends through
the producer, and uploads to the store, but never runs a reader. The
four-core engine reservation stays whole for both cases, because both
launch the engine binary. A declared set that does not fit still raises.
The 8-physical-core floor for a publishable run is unchanged.

`test_each_case_claims_only_its_own_roles` offers cores 0-7 with siblings
16-23 to both role sets:
- The stages set is placed on exactly the eight physical cores: 0
  observability, 1 bench, 2-4 reserved, 5-6 producer, 7 store. It claims no
  reader.
- The engine set, with the reader and the full reservation, is still
  refused with "no physical core is left for reader".

The family result also records the host at start and end, as
`environment.host_at_start` and `host_at_end`. Each holds the load average,
the family's own affinity, and the five heaviest other processes by CPU
time, each with its `Cpus_allowed_list`. `host_neighbours` produces the
record, and `test_neighbours_are_recorded_with_their_cores` covers it
against a fake process table. Python contract tests: 174, OK.

### The run

The command was `SERIES_MEASURE_LONG=1 taskset -c 0-7,16-23 measure stages
--output-dir /tmp/series-stages`. It started at 23:52:21 on 2026-09-22 and
ran for 795 s, at revision 9d4fd4ad5. The tree was dirty only because the
previous family's files had been removed. The four bench binaries were
rebuilt before the run with no source change since ed4bc1e0e. The release
engine was relinked and hashes identically to the one the committed family
used.

| What the family result records | At start | At end |
|---|---|---|
| Family affinity | 0-7, 16-23 | 0-7, 16-23 |
| Load average, 1/5/15 min | 11.05, 9.73, 9.50 | 9.77, 9.92, 9.75 |
| Heaviest other process | java, TLC, `Cpus_allowed_list` 8-15,24-31 | the same |
| Core allocation | observability 0, bench 1, reserved 2-4, producer 5-6, store 7 | |

The load average is host-wide, so it still counts TLC's eight workers.
They now run only on the other half of the cores.

| Measure | Result |
|---|---|
| Children | 312 of 312 passed |
| Aggregates | 102 of 104 passed |
| Baselines written | 102 |
| Registered stage results | 138 |
| Index | failed, on two aggregates |

The two refusals are preserved, not rerun:

| Aggregate | Check | Coefficient of variation |
|---|---|---|
| encode, logs-1k-stable, zstd, timing | repetition_stability, peak RSS | 0.21, with CPU at 0.09 |
| upload, logs-1k-stable, zstd, timing | repetition_stability, wall time | 0.21, with CPU at 0.01 |

These are the two host-independent refusals seen before. The encode
peak-RSS instability is the same one fix round 1 recorded. The upload
wall variance is store-bound, and the 22:27 run showed it before TLC
started.

**Seven of the nine loaded-host refusals vanished under the pin:**
- all five CPU repetition-stability refusals;
- the pipeline-baseline CPU refusal on metrics-mixed;
- the local_write heap-workspace refusal;
- the sink 512 KiB RSS reconciliation, which passed in both its timing and
  heap aggregates.

The CPU stability refusals disappearing once TLC left the bench's physical
cores supports the attribution to host load. The pipeline-baseline CPU
window is still the 0.1 s dose problem, so its pass here is luck of the
draw rather than a fix.

### Headline medians

CPU ns/record, medians of three repetitions, zstd. Round 1 is the quiet
host before TLC, family b0a397f8d. Round 2 is the loaded host, 4f8ff1257.
Round 3 is this pinned family.

| Workload | Stage | Round 1, quiet | Round 2, loaded | Round 3, pinned | Round 3 against round 1 |
|---|---|---|---|---|---|
| logs-1k-stable | convert | 365.65 | 394.04 | 389.07 | +6.4% |
| logs-1k-stable | extract | 1,078 | 1,092 | 1,115 | +3.4% |
| logs-1k-stable | sort_seal | 457.31 | 462.58 | 473.21 | +3.5% |
| logs-1k-stable | merge | 124.24 | 122.36 | 136.76 | +10.1% |
| logs-1k-stable | encode | 1,610 | 1,609 | 1,914 | +18.8% |
| logs-1k-stable | sink | 2,230 | 2,211 | 2,251 | +0.9% |
| logs-1k-stable | otlp_parquet_zstd | 4,083 | 4,099 | 4,247 | +4.0% |
| logs-1k-stable | otlp_minio | 4,383 | 4,778 | 4,842 | +10.5% |
| metrics-mixed | convert | 505.91 | 578.09 | 505.35 | -0.1% |
| metrics-mixed | extract | 1,342 | 1,598 | 1,368 | +1.9% |
| metrics-mixed | sort_seal | 347.24 | 467.70 | 350.82 | +1.0% |
| metrics-mixed | merge | 101.44 | 110.83 | 104.73 | +3.2% |
| metrics-mixed | encode | 221.14 | 243.70 | 225.55 | +2.0% |
| metrics-mixed | sink | 343.57 | 385.11 | 352.95 | +2.7% |
| metrics-mixed | otlp_parquet_zstd | 2,661 | 3,026 | 2,695 | +1.3% |
| metrics-mixed | otlp_minio | 2,844 | 4,671 | 2,993 | +5.2% |

The pin removed the large loaded-host slowdowns. For example, metrics-mixed
`sort_seal` went from 467.70 back to 350.82, within 1 percent of the quiet
host.

The pinned medians are still not the quiet host's. Across all 46 summaries
the median change against round 1 is about +5 percent. The logs encode,
merge and pipeline rows are 10 to 29 percent higher:
- encode logs-1k-stable none: +16.7%;
- encode logs-1k-stable zstd: +18.8%;
- encode logs-8k-churn-wide: +14% to +19%;
- merge logs-8k-churn-wide: +21.8%;
- otlp_noop pipeline logs-1k-stable: +29%.

Encode and merge are the most memory-bound stages. TLC on the other eight
cores still shares the package's memory controller and power budget with
the bench. That is a plausible cause of this residual, but these results
do not prove it. Nothing in the code changed any of these stages' timed
work, beyond the cache and sink teardown this round removed, which encode
and merge never had. The new baselines therefore encode the pinned regime
with TLC running on the other half of the package. A quiet-host run will
look faster against them.

The full table of all 46 summaries follows. Its "max CV" column is the
largest repetition coefficient of variation over all metrics.

| workload | stage | mode | comp | CPU ns/record, round 1 (quiet) | round 2 (loaded) | round 3 (pinned) | pinned vs quiet | wall ns/record, pinned | max CV |
|---|---|---|---|---|---|---|---|---|---|
| logs-1k-stable | convert | isolated | zstd | 365.65 | 394.04 | 389.07 | +6.4% | 389.07 | 0.01 |
| logs-1k-stable | encode | isolated | none | 934.49 | 941.38 | 1,090 | +16.7% | 1,090 | 0.07 |
| logs-1k-stable | encode | isolated | zstd | 1,610 | 1,609 | 1,914 | +18.8% | 1,913 | 0.21 |
| logs-1k-stable | extract | isolated | zstd | 1,078 | 1,092 | 1,115 | +3.4% | 1,115 | 0.00 |
| logs-1k-stable | local_write | async | zstd | 99.42 | 102.40 | 108.93 | +9.6% | 108.94 | 0.05 |
| logs-1k-stable | merge | isolated | zstd | 124.24 | 122.36 | 136.76 | +10.1% | 136.80 | 0.02 |
| logs-1k-stable | otlp_convert | criterion | zstd | 365.25 | 385.92 | 392.63 | +7.5% | 396.20 | 0.00 |
| logs-1k-stable | otlp_extract_hash | criterion | zstd | 1,432 | 1,465 | 1,482 | +3.5% | 1,482 | 0.09 |
| logs-1k-stable | otlp_minio | async | zstd | 4,383 | 4,778 | 4,842 | +10.5% | 8,038 | 0.07 |
| logs-1k-stable | otlp_noop | criterion | zstd | 0.02 | 0.02 | 0.02 | +5.0% | 0.02 | 0.01 |
| logs-1k-stable | otlp_noop | pipeline | zstd | 1,344 | 2,114 | 1,738 | +29.4% | 121,193 | 0.02 |
| logs-1k-stable | otlp_parquet_local | criterion | zstd | 3,485 | 3,299 | 3,443 | -1.2% | 3,472 | 0.06 |
| logs-1k-stable | otlp_parquet_zstd | criterion | zstd | 4,083 | 4,099 | 4,247 | +4.0% | 4,269 | 0.08 |
| logs-1k-stable | otlp_sort | criterion | zstd | 2,204 | 2,295 | 2,315 | +5.0% | 2,269 | 0.03 |
| logs-1k-stable | sink | async | zstd | 2,230 | 2,211 | 2,251 | +0.9% | 2,251 | 0.10 |
| logs-1k-stable | sort_seal | isolated | zstd | 457.31 | 462.58 | 473.21 | +3.5% | 473.23 | 0.01 |
| logs-1k-stable | upload | async | zstd | 395.73 | 386.71 | 402.21 | +1.6% | 5,385 | 0.21 |
| logs-512k-near-limit | encode | isolated | none | 271,839 | 313,627 | 283,879 | +4.4% | 283,880 | 0.04 |
| logs-512k-near-limit | encode | isolated | zstd | 525,678 | 628,791 | 563,986 | +7.3% | 564,148 | 0.04 |
| logs-512k-near-limit | extract | isolated | zstd | 60,559 | 72,811 | 66,306 | +9.5% | 66,306 | 0.01 |
| logs-512k-near-limit | merge | isolated | zstd | 12,378 | 17,601 | 13,968 | +12.9% | 13,967 | 0.06 |
| logs-512k-near-limit | sink | async | zstd | 652,799 | 766,459 | 718,203 | +10.0% | 718,496 | 0.01 |
| logs-512k-near-limit | sort_seal | isolated | zstd | 70,423 | 94,994 | 78,795 | +11.9% | 78,809 | 0.03 |
| logs-8k-churn-wide | encode | isolated | none | 4,170 | 5,054 | 4,770 | +14.4% | 4,771 | 0.03 |
| logs-8k-churn-wide | encode | isolated | zstd | 7,143 | 8,118 | 8,475 | +18.6% | 8,476 | 0.09 |
| logs-8k-churn-wide | extract | isolated | zstd | 2,014 | 2,323 | 2,191 | +8.8% | 2,191 | 0.06 |
| logs-8k-churn-wide | merge | isolated | zstd | 2,783 | 3,649 | 3,390 | +21.8% | 3,390 | 0.08 |
| logs-8k-churn-wide | sink | async | zstd | 13,968 | 16,403 | 14,484 | +3.7% | 14,489 | 0.09 |
| logs-8k-churn-wide | sort_seal | isolated | zstd | 2,172 | 2,599 | 2,334 | +7.4% | 2,334 | 0.01 |
| metrics-mixed | convert | isolated | zstd | 505.91 | 578.09 | 505.35 | -0.1% | 505.35 | 0.01 |
| metrics-mixed | encode | isolated | none | 163.67 | 180.51 | 166.90 | +2.0% | 166.91 | 0.00 |
| metrics-mixed | encode | isolated | zstd | 221.14 | 243.70 | 225.55 | +2.0% | 225.55 | 0.01 |
| metrics-mixed | extract | isolated | zstd | 1,342 | 1,598 | 1,368 | +1.9% | 1,368 | 0.00 |
| metrics-mixed | local_write | async | zstd | 2.43 | 2.79 | 2.57 | +5.6% | 2.57 | 0.00 |
| metrics-mixed | merge | isolated | zstd | 101.44 | 110.83 | 104.73 | +3.2% | 104.72 | 0.01 |
| metrics-mixed | otlp_convert | criterion | zstd | 503.93 | 563.23 | 499.42 | -0.9% | 508.57 | 0.01 |
| metrics-mixed | otlp_extract_hash | criterion | zstd | 1,842 | 2,009 | 1,871 | +1.5% | 1,886 | 0.11 |
| metrics-mixed | otlp_minio | async | zstd | 2,844 | 4,671 | 2,993 | +5.2% | 4,116 | 0.05 |
| metrics-mixed | otlp_noop | criterion | zstd | 0.02 | 0.02 | 0.02 | +3.0% | 0.02 | 0.03 |
| metrics-mixed | otlp_noop | pipeline | zstd | 354.50 | 355.96 | 312.18 | -11.9% | 6,346 | 0.02 |
| metrics-mixed | otlp_parquet_local | criterion | zstd | 2,618 | 2,563 | 2,644 | +1.0% | 2,669 | 0.03 |
| metrics-mixed | otlp_parquet_zstd | criterion | zstd | 2,661 | 3,026 | 2,695 | +1.3% | 2,685 | 0.04 |
| metrics-mixed | otlp_sort | criterion | zstd | 2,322 | 2,622 | 2,425 | +4.4% | 2,801 | 0.10 |
| metrics-mixed | sink | async | zstd | 343.57 | 385.11 | 352.95 | +2.7% | 352.94 | 0.01 |
| metrics-mixed | sort_seal | isolated | zstd | 347.24 | 467.70 | 350.82 | +1.0% | 350.85 | 0.00 |
| metrics-mixed | upload | async | zstd | 15.17 | 19.70 | 15.89 | +4.8% | 1,060 | 0.10 |

### Artifacts

- The committed family is `stages.json` plus its run files and 102
  baselines, in `docs/superpowers/reports/series-parquet-measurement/`.
- Raw non-Parquet artifacts are archived at
  `.measurement-artifacts/stages-raw-20260923-0005.tgz`, which is
  git-ignored. Bulk Parquet output is deliberately not retained, because
  sizes and hashes are in the committed results.
- The MinIO container of the run was removed by the harness.
