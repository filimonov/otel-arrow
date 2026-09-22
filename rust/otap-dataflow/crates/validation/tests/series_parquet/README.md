# Series Parquet measurement harness

Two lanes live in this directory. `test_e2e.py` is the standing end-to-end
suite: a real `df_engine` process, real object stores in containers, Grafana
Alloy, and two independent readers. `measurement.py`, `measure.py` and
`test_measurement.py` are the measurement lane built on top of it. The
measurement lane extends the end-to-end helpers; it never replaces them.

## Commands

Every command runs from `rust/otap-dataflow`.

```bash
# The standing suite: 18 tests, no skips when Docker is required.
SERIES_REQUIRE_DOCKER=1 python3 -m unittest \
  crates.validation.tests.series_parquet.test_e2e -v

# The measurement contracts: fast, no engine, no container, no build.
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v

# The same contracts as a published, committable evidence file.
python3 -m crates.validation.tests.series_parquet.measure run \
  --case harness-contracts --output-dir /tmp/series-contracts

# Stage one published evidence tree by exact file name, before a commit.
python3 -m crates.validation.tests.series_parquet.measure stage-results \
  --index ../../docs/superpowers/reports/series-parquet-measurement/harness-contracts.json
```

`run --case harness-local` is registered and its spec validates, but its
measured body arrives with the topology, launcher and enforced host controls
of plan 3 task 2. The remaining subcommands (`stages`, `attribution`,
`capacity`, `memory`, `soak`, `fault-preflight`, `failures`, `buffered`,
`remediate`, `report`) are named here so the command line is one contract;
each is implemented by its own task.

## Environment variables

| Variable | Effect |
| --- | --- |
| `DF_ENGINE` | The engine binary to run. Defaults to `target/debug/df_engine`. |
| `SERIES_REQUIRE_DOCKER` | `1` makes missing images and Docker a failure rather than a skip. |
| `SERIES_REQUIRE_FAULT_TOOLS` | `1` makes missing fault tooling a failure rather than a skip. |
| `SERIES_MEASURE_LONG` | `1` opts in to throughput sweeps, profiled memory runs, the soak and long failure runs. |
| `SERIES_MEASURE_LEASE` | The exclusive host measurement lease file. Defaults to `/tmp/series-measure-host.lease`. |
| `SERIES_ARTIFACT_DIR` | Where measurement tests retain their logs, results and ledgers. |
| `SERIES_MINIO_IMAGE`, `SERIES_RUSTFS_IMAGE`, `SERIES_CLICKHOUSE_IMAGE`, `SERIES_ALLOY_IMAGE` | The container images the end-to-end lane uses. |

Python dependencies are in `requirements.txt`.

## What a measurement is allowed to claim

These rules are enforced in code, not by convention. `measurement.py` holds
each of them once.

- **Delivery.** The producer's ledger is the oracle. Every record carries a
  fixed-width stable id and a canonical payload hash, and the comparison is a
  SQL join rather than a Python set. Missing, unexpected, corrupt and
  duplicated records are counted separately, so a duplicate can never mask a
  loss in a row total. A healthy no-retry run requires multiplicity exactly
  one; a fault run allows duplicates and counts every one.
- **Collection epochs.** Only an advance of a worker's collection-updated
  `pipeline.uptime` counts as a new observation. Three HTTP responses and the
  scrape's own timestamp prove nothing. An uptime that decreases within one
  deployment generation is an error; a new generation is a restart and is
  counted as one.
- **Drain.** Three consecutive advances of every worker's uptime, each with
  the exporter holding no block, no pending request and no queued
  notification, and in the buffered topology with nothing queued or in
  flight. A nonempty observation resets that worker's streak.
- **Gauges.** A required gauge that is absent, non-numeric or published by
  more than one entity of a worker is an error, never a zero.
- **Liveness.** Presence is not enough. A snapshot the exporter did not
  answer carries its metric names with every value zero, which looks exactly
  like a drained worker. `memory.budget_bytes` is computed from
  configuration constants and is never zero while the worker is alive, so it
  is the marker: a worker whose budget reads zero is not an observation, its
  epoch counts towards neither an empty nor a nonempty streak, and the drain
  report records how many such epochs it saw.
- **Environment.** Every run records a start and an end snapshot with the CPU
  model, logical and physical core counts, total RAM, kernel, load averages
  and the observed per-thread affinity from `/proc/PID/task/*/status`. A
  requested-versus-observed affinity mismatch or an ambiguous worker mapping
  aborts the run. A concurrent compiler or image build invalidates it, and
  nobody else's process is ever stopped. One exclusive host lease covers the
  whole run.
- **Units.** Every metric name ends in its unit (`_bytes`, `_s`, `_ns`,
  `_records_per_s`, `_cpu_ns_per_record`, ...). An unavailable value is null
  with a reason and never zero, and a passed run cannot be missing a
  mandatory metric.
- **No fixed sleeps.** Every wait is a poll under a monotonic deadline that
  returns an observation. Elapsed time alone never proves readiness,
  drainage or fault activation.

## Acceptance policy

The first valid run, with its required repetitions, writes and commits a
baseline fingerprinted by machine, core allocation, effective configuration,
workload and build profile. A later run with the same fingerprint fails on a
regression worse than 25 percent. A different fingerprint writes a new
baseline rather than comparing against one that does not describe it. The
original matching baseline is immutable, so small regressions cannot ratchet
it upward. The source revision and binary hash are recorded as provenance
outside the fingerprint, so a new implementation is compared rather than
excused.

Correctness, measurement validity and an unexplained accounted-versus-RSS
residual are hard checks on every run, whatever the baseline says. A run that
fails one of them can never establish a baseline. The single numerical
boundary lives in `measurement.REGRESSION_LIMIT` and nowhere else.

## Evidence files

One compact JSON document per run, named
`{case}-{topology}-{store}-c{cores}-w{interval}-r{ordinal}.json`. A family
summary index lists its children in `run_files`, `baseline_files` and
`child_indexes` with their exact names, sizes and SHA-256 hashes; it never
replaces the per-run files. Committed evidence lives in
`docs/superpowers/reports/series-parquet-measurement`. Published run and
baseline file names are immutable: a re-execution uses a new artifact
directory rather than overwriting evidence. Raw profiles, Parquet files,
packet captures, logs and ledgers stay outside tracked source and are
referenced by path, hash, size and retention location.

`stage-results` reads one published index, enumerates the tree recursively,
rejects a path that is not a plain JSON file name in the report directory,
rejects cycles, verifies every recorded hash, and hands the files to
`git add` by name in bounded batches. Nothing is ever staged by glob or by
directory.

## Reproducing a measurement

1. Build the engine with the features the case needs:
   `cargo build --locked -p otel-arrow-dfe --bin df_engine --features series_parquet,aws`.
2. Read the run's `environment` block. Match the machine, the core
   allocation and the build profile, or expect a new baseline rather than a
   comparison.
3. Run the case with its recorded `config.requested` and `workload`.
4. Compare the produced JSON against the committed baseline named in
   `baseline_decision`.
