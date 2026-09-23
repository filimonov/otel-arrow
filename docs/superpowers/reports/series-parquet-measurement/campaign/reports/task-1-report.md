# Task 1 report: deterministic measurement harness and durable result contract

Status: DONE_WITH_CONCERNS. Every binding ruling in the brief is implemented
and tested. The one deliberate deviation is that `run_case` carries the run
contract but no measured body, which is what the brief assigns to task 2.

## What was built

### `crates/validation/tests/series_parquet/test_e2e.py` (modified)

One additive change: the workload-independent half of `verify_files` was
extracted into `scan_objects(test, root, db)`, which returns the part files,
descriptor coverage set, values keys, logs bodies and metrics row count.
`verify_files` now calls it and keeps its own signature and every fixture
assertion unchanged. The diff is a pure move: a line-by-line comparison of
the diff shows lines added and none removed other than by re-indentation.
The measurement oracle calls `scan_objects`, so layout, recorded row counts,
declared sort order, descriptor identity hashes, per-partition descriptor
coverage and the null-versus-empty list shape are checked by exactly the code
the fixture suite uses rather than by a second copy of it.

### `crates/validation/tests/series_parquet/measurement.py` (new, 2776 lines)

- **Inputs.** `Workload` and `RunSpec` exactly as the plan specifies, with
  validation of topology, store, distinct allowed cores, positive counts,
  body size sufficient for the record id, and `max_in_flight` against
  receiver capacity. `RunSpec.build_run_id` produces the plan's one-file-per-
  trial name.
- **Deterministic workload.** `build_request(workload, index)` returns the
  signal, deterministic protobuf bytes and `(record_id, kind,
  expected_sha256)` rows. Logs carry the fixed-width stable id as a body
  prefix followed by seeded printable padding, keep `host.id`, service name
  and scope compatible with the existing fixture readers, and vary
  `logger.name` by `index % series` (the configured logs series attribute).
  Metrics remove `request.id` entirely and encode identity in `(metric name,
  time_unix_nano)` with `time = 1789960500000000000 + ordinal * 1000`; six
  distinct stable kind names cover integer and double gauge, integer and
  double sum, a populated histogram and a distribution-less histogram. Values
  and bucket arrays derive from the seed and index. The per-point attribute is
  a finite series slot, never the record id.
- **Canonical payloads.** The expected hash is computed from the columns the
  exporter actually stores, with doubles rendered as IEEE-754 bit patterns
  and nulls rendered explicitly, so exporter rounding and nullable
  representation define the expectation. Producer protobuf bytes are never
  compared to Parquet bytes. `METRIC_DESCRIPTOR_FIXTURE` and
  `METRIC_LIST_SHAPE` are independent explicit fixtures per kind.
- **Ledger.** SQLite with `journal_mode=WAL` and `synchronous=FULL`, tables
  `requests`, `attempts` and `records` with the plan's columns, unique record
  ids and an immutable per-request wire hash. Attempts are classified as
  acknowledgement, retryable NACK, permanent NACK, partial rejection or
  producer-local failure, each counted separately. `RETRYABLE_CODES` is
  imported from `test_e2e`, not copied; a test asserts it contains CANCELLED
  and excludes ABORTED.
- **Environment.** `environment_snapshot` records CPU model, logical and
  physical core counts (from the kernel's own thread-sibling lists), total
  RAM, kernel, load averages and every thread's observed `Cpus_allowed_list`
  from `/proc/PID/task/*/status`. `select_worker_threads` maps each telemetry
  worker identity to exactly one thread through the fifteen-byte `comm`
  truncation of the controller's `pipeline-<group>-<pipeline>-core-<id>-gen-
  <n>` name; zero or several candidates, or two workers sharing a truncation,
  is an ambiguous mapping. `assert_affinity` aborts on ambiguity or on a
  requested-versus-observed mismatch. `BuildMonitor` scans procfs for
  compilers and image builds, preserves evidence and never signals anything.
  `HostLease` is an `O_EXCL` lease held for the whole run, reclaimed only
  when its holder's pid is gone.
- **Strict sampler.** `parse_telemetry` attributes every metric set to one
  worker by group, pipeline, core and generation, and within a worker to one
  entity by node identity, so two nodes publishing the same metric name is an
  ambiguity rather than a collapsed value. The engine's own `system` group is
  not a worker. Process-wide values (including
  `memory.unaccounted_rss_bytes`) are kept apart and never summed. A missing
  or non-numeric required gauge is an error, never zero.
  `procfs_process_sample` records RSS, stat, smaps-rollup and descriptor
  counts on the same monotonic timeline; `directory_bytes` counts allocated
  blocks.
- **Epochs and drain.** `EpochTracker` counts only advances of each worker's
  collection-updated `pipeline.uptime`; a decrease within one generation is
  an error and a generation change is a restart. `observe_drain` requires
  three consecutive advances per worker with the exporter holding no active
  or flushing block, no pending bytes, no pending request and no queued
  notification, plus, when buffered, nothing queued or in flight. A repeated
  response at an unchanged uptime adds no observation; a nonempty observation
  resets that worker's streak; a deadline that expires fails.
- **Oracle.** `read_oracle` runs `scan_objects`, checks list shapes, records
  series cardinality separately from record cardinality, streams
  `(record_id, signal, payload_sha256)` rows from DuckDB into the ledger in
  bounded batches, and then counts missing, unexpected, corrupt and
  duplicated records in SQL joins. `require_all=False` restricts the
  expectation to acknowledged requests. Cross-reader agreement runs the same
  canonical payload expression written independently for DuckDB and for
  clickhouse-local through the existing `clickhouse_reader`, comparing an
  order-independent digest (or, above two million rows, count and bounds).
- **Results.** `write_result` validates before writing and replaces
  atomically; the encoding is sorted, indented, ASCII and rejects non-finite
  values. `validate_result` enforces the mandatory fields, both environment
  snapshots, a unit suffix on every metric name, null-with-a-reason for an
  unavailable value, and that a passed run has no missing mandatory metric
  and no failed check.
- **Baselines.** `evaluate_baseline` is the sole evaluator. It checks hard
  gates first, canonicalizes the fingerprint over machine identity, core
  allocation, effective configuration, workload and build profile, resolves
  an immutable matching baseline or uses test-supplied baselines, and returns
  the decision with per-metric signed regressions. A new fingerprint writes a
  candidate baseline rather than comparing. A hard-gate failure or a
  non-passed run creates no baseline and raises. `REGRESSION_LIMIT = 0.25`
  exists in exactly one place. Source revision and binary hash are recorded
  as provenance outside the fingerprint.
- **Publication and staging.** `publish_result_tree` enumerates the tree
  recursively, requires a hash on every reference, checks it, rejects cycles
  and path escape, and copies by exact name. Run and baseline names are
  immutable. An index may advance only by enumerating the published index it
  replaces as an immutable child; `archive_published_index` produces that
  child. `stage_run_files` enumerates the published tree and hands absolute
  paths to `git add --` in bounded batches, never a glob or a directory.

### `crates/validation/tests/series_parquet/measure.py` (new, 343 lines)

`run`, `stage-results` and the ten later-task subcommands. Long commands
refuse without `SERIES_MEASURE_LONG=1` and say how to opt in. `run_named`
resolves a registered case and names the registry when it cannot.
`harness-contracts` runs `test_measurement` through
`unittest.TextTestRunner`, collects `testsRun`, failures, errors and skips,
writes the verification-only index and publishes it; it launches no engine
and writes no baseline. `harness-local` is registered with 100 requests of
mixed supported signals, one-second windows and exact no-retry multiplicity,
and its spec validates. `run_case(spec, output_dir, experiment=None,
report_dir=None)` owns the contract: both environment snapshots, the
environment match, the result written in `finally` and published, and a
nonzero exit on failure.

### `crates/validation/tests/series_parquet/test_measurement.py` (new, 1388 lines)

79 tests, each with immediately preceding `Scenario:` and `Guarantees:`
comments, in nine classes: record contracts, spec contracts, ledger
contracts, oracle contracts, telemetry contracts, drain contracts, affinity
contracts, build and lease contracts, schema contracts, baseline contracts,
publication contracts, command contracts and helper contracts. The brief's
three named tests are present verbatim.

### `crates/validation/tests/series_parquet/README.md` (new)

Commands, environment variables, what a measurement may claim, the
acceptance policy, the evidence file rules and how to reproduce a run.

## Result-file schema

One JSON document per run. `schema_version`, `run_id`, `case`,
`artifact_kind`, `status`, `started_utc`, `elapsed_s`, `environment`,
`config`, `workload`, `metrics`, `metrics_unavailable`, `mandatory_metrics`,
`samples`, `events`, `checks`, `artifacts`, `run_files`, `baseline_files`,
`child_indexes`, `report_dir`. `environment.start` and `environment.end` each
carry `cpu_model`, `logical_core_count`, `physical_core_count`,
`sibling_groups`, `ram_bytes`, `kernel`, `load_average_1_5_15`,
`thread_affinity` (pid, tid, role, name, observed `Cpus_allowed_list`,
expected cores, observation time), `absent_roles` and, when workers are
given, `worker_threads` and `ambiguous_workers`.

The committed contract-check index, abridged (the two environment snapshots
hold 32 thread observations each and are omitted here):

```json
{
  "artifact_kind": "contract_checks",
  "artifacts": [
    {"kind": "unittest_output", "name": "harness-contracts.log",
     "retention": "/tmp/series-contracts", "sha256": "69b20391...", "size_bytes": 12825}
  ],
  "baseline_files": [],
  "case": "harness-contracts",
  "checks": [
    {"detail": "79 tests", "kind": "hard", "name": "contract_tests_ran", "status": "passed"},
    {"detail": "run=79 failures=0 errors=0 skipped=0 []", "kind": "hard",
     "name": "contract_tests_successful", "status": "passed"},
    {"detail": "{}", "kind": "hard", "name": "environment_matched", "status": "passed"}
  ],
  "child_indexes": [],
  "elapsed_s": 1.227785228,
  "mandatory_metrics": ["contract_errors_count", "contract_expected_failures_count",
                        "contract_failures_count", "contract_skips_count",
                        "contract_tests_count"],
  "metrics": {"contract_errors_count": 0, "contract_expected_failures_count": 0,
              "contract_failures_count": 0, "contract_skips_count": 0,
              "contract_tests_count": 79},
  "report_dir": "docs/superpowers/reports/series-parquet-measurement",
  "run_files": [],
  "run_id": "harness-contracts",
  "schema_version": 1,
  "status": "passed"
}
```

## Commands and output

All commands run from `rust/otap-dataflow` unless stated, with
`/tmp/series-parquet-venv/bin` on PATH.

### Red, before the module existed

```text
$ python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
ImportError: Failed to import test module: test_measurement
ModuleNotFoundError: No module named 'measurement'
```

An import failure of the new module, not a missing dependency or binary, as
step 2 requires.

### Contract tests

```text
$ python3 -m unittest crates.validation.tests.series_parquet.test_measurement
Ran 79 tests in 1.228s
OK
```

### Contract-check run

```text
$ python3 -m crates.validation.tests.series_parquet.measure run \
    --case harness-contracts --output-dir /tmp/series-contracts
harness-contracts: passed {"contract_errors_count": 0,
 "contract_expected_failures_count": 0, "contract_failures_count": 0,
 "contract_skips_count": 0, "contract_tests_count": 79}
exit=0
```

### Staging

```text
$ python3 -m crates.validation.tests.series_parquet.measure stage-results \
    --index ../../docs/superpowers/reports/series-parquet-measurement/harness-contracts.json
$ git diff --cached --name-only
docs/superpowers/reports/series-parquet-measurement/harness-contracts.json
```

### Repository checks

```text
$ python3 tools/sanitycheck.py      # from the repository root
(exit 0)
$ npx markdownlint-cli2 "rust/otap-dataflow/crates/validation/tests/series_parquet/*.md"
Summary: 0 issues in 0 files
```

Every file this task touched was checked for non-ASCII bytes; none found.

### Full end-to-end suite

```text
$ SERIES_REQUIRE_DOCKER=1 python3 -m unittest \
    crates.validation.tests.series_parquet.test_e2e -v
Ran 18 tests in 150.729s
OK
WALL 150.89 s
```

Eighteen tests, zero skips, on the first run.

Two later runs of the same suite each failed one test:

```text
FAIL: test_storage_outage_recovers_without_losing_acked_data
      (OutageSlice) (store='minio')
  File ".../test_e2e.py", line 2743, in exercise_outage
    self.assertGreater(budget, 0, "missing budget telemetry is a failure")
AssertionError: 0 not greater than 0 : missing budget telemetry is a failure
Ran 18 tests in 145.751s
FAILED (failures=1)
```

This is not caused by this task. `test_e2e.py` was replaced with the exact
`HEAD` content (sha256 `72b59b12456eadaec45c553ac65bfe979c60854b5dc71360d7554be78f734a19`)
and the same subset was run:

```text
$ python3 -m unittest crates.validation.tests.series_parquet.test_e2e.OutageSlice
FAIL: test_storage_outage_recovers_without_losing_acked_data (store='minio')
AssertionError: 0 not greater than 0 : missing budget telemetry is a failure
Ran 2 tests in 63.715s
FAILED (failures=1)
```

The unmodified baseline fails identically, so the flake pre-exists this
branch. The working file was restored afterwards and its hash re-verified.
A line-by-line analysis of the `test_e2e.py` diff confirms that no line was
removed and that nothing outside the `scan_objects` extraction changed; the
extraction runs in `verify_files`, long after the telemetry assertion that
fails.

**Root cause, for task 12.** `metric_max(document, name)` returns
`max(values, default=0)`, so a metric set that is absent from one snapshot is
indistinguishable from a genuinely zero gauge. The recovery loop samples
every 0.2 seconds and requires `memory.budget_bytes > 0` in every sample, so
one snapshot without the exporter's metric set fails the run. This is exactly
the conflation the measurement lane refuses: `measurement.require_gauge`
treats an absent or non-numeric gauge as an error and a zero as an
observation. A bounded fix belongs in the contingency task, not here.

## Live validation of the measurement path

The contract tests do not start an engine, so the sampler, drain proof and
oracle were exercised against a real `df_engine` process separately during
development. Twelve requests of mixed signals through the local file backend
produced:

- a strict sample naming one worker, `default/main/core0`, with all eleven
  required exporter gauges present and numeric;
- a drain proof reaching three empty collection epochs after 34 samples;
- an oracle reporting 144 expected and 144 actual records, zero missing,
  zero unexpected, zero corrupt, a multiplicity histogram of `{1: 144}`, all
  six metric kinds present with the declared list shapes, three logs series
  and eighteen metrics series, and DuckDB and clickhouse-local agreeing on an
  exact payload digest for both signals.

Negative control: deleting one logs values part file from the same tree made
the oracle report twelve missing records and fail, so the pass is a
measurement and not a vacuous one.

## Deviations

- **1. `run_case` has no measured body.** It carries the full contract (both
   environment snapshots, the environment match, the result written in
   `finally`, publication, nonzero exit) and takes an `experiment` callable.
   Called without one it raises, and the failed result is still written and
   published. This follows the brief, which assigns the measured
   `harness-local` run to task 2 after the topology, launcher and enforced
   host controls exist. Task 2 supplies the callable; no signature changes.
- **2. `worker_key` excludes the deployment generation.** The plan's wording
   implies a worker identity that survives a restart, since restarts are
   "tracked separately" from uptime decreases. Including the generation would
   make a restarted worker a second identity whose uptime starts again, which
   no epoch rule could then reject. Two generations of one worker in a single
   scrape is an explicit error.
- **3. The canonical payload collapses a null list and an empty list.**
   ClickHouse has no nullable array, exactly as `canonical_column` already
   documents. That one distinction is checked separately and per kind in
   DuckDB alone by `_check_list_shapes`, and by `scan_objects` at the file
   level.
- **4. Cross-reader agreement is an aggregate digest, not a row-by-row
   comparison.** Materialising a soak's rows in Python is what the plan
   forbids. Both readers sort and hash the same canonical payload strings
   inside the engine; above two million rows the check degrades to count and
   bounds and records `cross_reader_mode` so the weaker check is visible.
   `verify_readers` still performs the full row comparison for the fixture
   suite.
- **5. An index may advance by archiving its predecessor.** A strict reading of
   immutability would make the documented `harness-contracts` command fail on
   its second execution. `archive_published_index` implements the plan's own
   escape: the superseded index is preserved under a run-id and content
   derived name, enumerated as a child of the advancing index, and published
   with it. Run and baseline names remain strictly immutable.

## Concerns

- **1. The oracle's SQL is validated live but not by a contract test.**
   `_compare`, the loss-detection SQL, is covered by five tests. The DuckDB
   and ClickHouse canonical payload expressions are covered only by the live
   probes described above, because building a Parquet tree with the
   exporter's key-value metadata without the exporter is not something the
   contract lane can do cheaply. Task 2's first real `harness-local` run is
   what puts them under standing test.
- **2. Buffer gauges are unexercised.** `BUFFER_EMPTY_GAUGES` names
   `processor.durable_buffer.items:queued` and
   `processor.durable_buffer:in_flight`, read from the processor's metric
   definitions. No buffered pipeline has been run, so the names are correct
   by source inspection rather than by observation. Task 2 or the buffered
   task should confirm them on the first buffered run.
- **3. The RSS residual gate is a policy, not yet a computation.**
   `evaluate_baseline` fails on any hard check that is not passed, and the
   tests prove a failed `rss_reconciliation` check blocks a baseline. The
   check itself is produced by the memory task, which owns the independently
   measured runtime, allocator, buffer and workspace terms.
- **4. `harness-contracts` runs its own test module in-process.** The
   environment snapshot it records is therefore the harness process, not an
   engine. This is correct for a verification-only index, but it means the
   file's `thread_affinity` block is 32 threads of Python and is evidence of
   nothing about a worker. The index declares `artifact_kind:
   contract_checks` so no reader can mistake it for a measurement.

## Follow-up: metric_max flake

Controller ruling: fix the flake inside this plan rather than deferring it to
task 12. This section records what the defect actually was, which differs
from the diagnosis in the section above, and what was changed.

### The diagnosis in the first report was wrong

The first report attributed the flake to `metric_max` returning
`max(values, default=0)` for a metric that was absent from the snapshot. That
is a real defect, but it is not this one.

A tracing run against the unmodified `test_e2e.py` captured the failing
snapshot. The exporter metric set was present, and `memory.budget_bytes` was
among its metric names. The value was zero:

```text
ATTEMPT 2: success=False zero_budget_snapshots=1
CAPTURED: {
 "all_sets": ["channel.receiver", ..., "exporter.series_parquet", ...],
 "exporter_group_count": 21,
 "exporter_metric_names": [..., "memory.accounted_bytes",
                           "memory.budget_bytes", ...],
```

So the metric was present and reported zero. An absent-versus-zero fix alone
would not have changed the outcome.

`memory.budget_bytes` is computed in `worker.rs::sample_metrics` from
configuration constants: the sort, merge, writer, upload and conversion
reservations plus a fixed workspace. It cannot be zero while the worker is
alive. A zero therefore means the worker did not answer the collection that
snapshot was built from, and every one of its gauges reads zero in that
snapshot. The recovery loop then asserted this process's resident size
against a budget of zero, and failed.

Which engine path produces such a snapshot is not settled. Two candidate
mechanisms were examined and neither was confirmed: `metrics.rs`
`visit_and_reset_with_item_attrs` skips a bucket whose export is in flight,
and `visit_admin_metrics_and_reset` zeroes the admin accumulator after
visiting it. The second predicts that rapid scrapes within one collection
interval read zero, and a direct probe refuted that: twelve scrapes at 50ms
intervals against a one-second collection interval all read the real budget.
The fix does not depend on which mechanism it is, because it keys on the
marker rather than on the cause.

### What changed

**`test_e2e.py`, the helper.** `metric_max` no longer defaults an absent
metric to zero. `metric_values` returns every numeric value a snapshot
reports for a metric, and an empty list means absence. `metric_present`
answers presence directly. `metric_max` now requires presence and fails with
the metric set and metric named, unless the caller passes an explicit
`default`, which is how a poll that is waiting for a metric to appear opts in
to absence. This is the defect the first report described; it is fixed
because it is real, and because an upper-bound assertion against an absent
metric passed vacuously.

**`test_e2e.py`, the liveness marker.** `exporter_reported(document)` answers
whether the exporter's own sample is in a snapshot, using
`memory.budget_bytes > 0`. Both memory samplers in `exercise_outage`, during
the outage and during recovery, now keep only snapshots the exporter
answered, and the assertion loop is preceded by
`assertTrue(samples, ...)` so that skipping can never empty the evidence.
`settle()` no longer treats a snapshot the exporter did not answer as a
drained backlog, which it previously did because an unanswered snapshot
reports zero pending requests and a zero unacked age.

**`test_e2e.py`, the other callers.** Every call site was classified.
`wait_for_metrics`, the two admitted-request polls in `DockerSlice` and
`RestartSlice` pass `default=0` and say why. The flush-counter seed in
`exercise_outage` now takes its maximum over `metric_values` across earlier
snapshots, so a snapshot without the metric contributes no value rather than
a zero.

**`measurement.py`.** The same defect was latent in this task's own drain
proof, and it mattered more there: a worker that did not answer a collection
reports every emptiness gauge as zero, which is exactly what a drained worker
looks like, so a stalled exporter could have proved its own drainage.
`worker_reported` derives liveness from the sample's own gauges,
`worker_is_empty` refuses a worker that did not report, and `observe_drain`
counts such an epoch as neither empty nor nonempty and reports
`unanswered_epochs_by_worker`. Finding this is the reason the controller's
instruction to fix it now rather than at task 12 was correct.

**`README.md`.** A "Liveness" rule was added beside the "Gauges" rule.

### Regression tests

Five tests in `SnapshotReadingContracts` and one in `DrainContracts`, all
fast and engine-free. Each fails against the old helper:

- absent and present-at-zero are distinguishable, which
  `max(values, default=0)` made impossible;
- a required metric that is absent fails with the metric named;
- tolerating absence is explicit and yields the caller's own default;
- an upper-bound assertion against an absent metric fails instead of passing
  vacuously;
- a zero budget marks a snapshot the exporter did not answer;
- a worker whose uptime advances three times while reporting a zero budget
  cannot prove its own drainage, although every emptiness gauge reads zero.

### E2E verification of the fix

Seven runs, all with `SERIES_REQUIRE_DOCKER=1`, run sequentially with
nothing else started beside them. All seven passed; none skipped.

| Run | Scope | Tests | Result | Duration |
| --- | --- | --- | --- | --- |
| 1 | full suite | 18 | OK | 148.5 s |
| 2 | full suite | 18 | OK | 144.7 s |
| 3 | full suite | 18 | OK | 150.2 s |
| 4 | `OutageSlice` only | 2 | OK | 63.9 s |
| 5 | `OutageSlice` only | 2 | OK | 65.8 s |
| 6 | `OutageSlice` only | 2 | OK | 65.9 s |
| 7 | `OutageSlice` only | 2 | OK | 64.9 s |

Run 1 may have imported `test_e2e.py` a few seconds before the last edit,
which changed two docstrings and nothing else; runs 2 to 7 used the committed
file.

The fix was committed as `24eceba60` "test(series_parquet): distinguish
absent telemetry from zero and unanswered collections", staging the four
files by name: `test_e2e.py`, `measurement.py`, `test_measurement.py` and
`README.md`. `tools/sanitycheck.py` passed and the contract suite stands at
85 tests passing.

### Why a worker fails to answer a collection

**Answer: the evidence supports outcome (a), the benign race, and shows no
sign of (b).** One unanswered collection was observed in 395 epochs. It
lasted a single scrape, well under one collection interval, and fell one
second after engine start, outside any flush window. No collection went
unanswered in the roughly 1,070 scrapes taken while a flush was in progress.

**Method.** A tracer recorded exactly the scrapes the outage test already
makes, adding no scrape load. It captured the worker's `pipeline.uptime`,
which the pipeline thread advances once per collection, and the exporter's
`memory.budget_bytes`, which is nonzero only when the exporter node answered.
An epoch is one distinct uptime value; it is unanswered when no scrape inside
it carried a nonzero budget. Engine logs were captured before the test's
temporary directory was removed. Flush windows were parsed from them with the
colour codes stripped: a commit covers its own logged duration, a retried
block runs from its first logged retry to its commit or failure, and a
failure with no logged retry covers the configured three-second retry
deadline.

**Mechanism from source.** The pipeline control loop in `pipeline_ctrl.rs`
updates and reports its own metrics, including `uptime`, and only then sends
`CollectTelemetry` to the nodes at the end of the same tick. A scrape landing
between those two points sees an advanced uptime with no exporter sample.
That send is non-blocking: on a full control channel the message waits in
`pending_sends` and is retried, which is the path outcome (b) would take.

| Run set | Outage runs | Engines | Scrapes | Epochs | Unanswered epochs | Scrapes inside a flush window |
| --- | --- | --- | --- | --- | --- | --- |
| Quiet, first pass | 6 | 12 | 640 | 167 | 0 | not parsed |
| Quiet, flush-aware | 4 | 8 | 438 | 114 | 0 | 65 to 82 percent per engine |
| Two busy loops pinned to the engine's core 0 | 4 | 8 | 445 | 114 | 1 | 70 to 79 percent per engine |
| Total | 14 | 28 | 1,523 | 395 | 1 | |

The one miss lasted a single scrape, one second after its engine started,
outside every flush window and within half a second of one.

Miss rate:

```text
1 unanswered epoch in 395 = 0.25 percent of collections
1 unanswered scrape in 1,523 = 0.07 percent of scrapes
```

**Against the pre-registered criteria.** The observed miss is short and
falls at an edge, so it matches the race and not deferral. Nothing clusters
against flush windows, although about 70 percent of scrapes were taken during
a flush. Even with the worker's core deliberately contended, there were no
misses during flushes. This is a harness finding, not a product one, and the
liveness marker is the whole fix. A single scrape can miss a worker sample by
construction, so every assertion must be made over several collections. The
three-epoch drain proof already is. At this rate it needs no widening: with
the streak reset on an unanswered epoch, a miss can only delay a drain proof,
never falsify one.

**What remains unexplained.** Before the fix, zero-budget snapshots appeared
in four of seven outage-test executions. After it, they appeared in one of 21.
I could not find the difference:

- The engine binary is byte-identical throughout; it was built at 11:58.
- `sar` shows a load average between 0.4 and 1.6 on 32 cores, and core 0
  about 88 percent idle, in both periods.
- The test changes cannot alter engine behaviour.

I did not capture the timing of the pre-fix zeros relative to flushes or
start-up. So I cannot rule out that those had a different cause, possibly
(b). The measurement lanes must keep recording unanswered epochs:
`observe_drain` reports `unanswered_epochs_by_worker`. A later run that shows
misses clustering against flushes would reopen this.

## Fix round 1

Review verdict: needs fixes. All eleven findings were fixed in one commit,
`fcb71d492` "test(series_parquet): enforce the measurement lifecycle and
baseline policy". The four source files and both evidence files were staged
by name, with the exact trailers.

### Critical

- **1. `run_case` applies the baseline policy.** After the body runs, the
   host controls close and the status settles. `evaluate_baseline` then
   runs, unless `evaluate=False` marks a family child whose policy is
   applied to the family. On a new fingerprint the candidate is written by
   `write_json_atomic` (temporary file, `fsync`, rename) beside the result.
   It is referenced by name, size and SHA-256 in `baseline_files` and
   published with the result. Publication happens in `finally`, including
   for failed runs.
- **2. The host controls are mandatory lifecycle steps.** `RunControls`
   takes the lease, then starts the build monitor. The body must call
   `snapshot("start", ...)` and `snapshot("end", ...)`, and each snapshot
   calls `assert_affinity` and aborts the run on a mismatch. `close` records
   six hard checks whatever happened: `host_lease_held`,
   `no_concurrent_build`, `environment_snapshots_complete`,
   `environment_matched`, `affinity_matched` and
   `physical_cores_sufficient`. The body adds `delivery`,
   `minimum_samples` and `rss_reconciliation`. `evaluate_baseline` requires
   all nine, and an empty or partial set fails with each absent gate named.
   An edge the body never reached is filled from the harness process and
   marked `fallback`, so every file still carries both snapshots.
- **3. The fingerprint requires every input.** A missing, null or empty input
   raises with its path named. The fingerprint now covers the machine
   identity and topology, including sibling groups; the cores available to
   the run; normalized role placement; the effective configuration; the
   workload with its rate, duration and concurrency; and the build profile,
   features, allocator and toolchain. Only paths under the run's own
   directory are replaced by `<run_dir>`, so a sibling directory such as
   `/runs/t-0011` is kept exactly. Tests: sibling topology changes the hash,
   each of seven missing inputs raises, and two runs differing only in run
   directory share a fingerprint.
- **4. An unanswered epoch resets the drain streak.** The sequence empty,
   unanswered, empty, empty counts as two consecutive empty epochs, not
   three, and one more answered empty epoch completes a genuine three. The
   old logic accepted the four-epoch sequence.

### Important

- **5. Worker mapping uses core evidence.** Every default-group worker
   truncates to `pipeline-defaul`, so the name only selects candidates. Each
   worker maps to the one candidate whose last-run CPU, field 39 of
   `/proc/PID/task/TID/stat`, is that worker's core. A worker with zero or
   several such candidates, or whose thread another worker already claimed,
   aborts as ambiguous. Each worker is compared with `[core_id]`, never
   with the process-wide list, and a worker core set different from the
   requested one aborts. The tests use real threads named with
   `prctl(PR_SET_NAME)` and pinned with `sched_setaffinity`. A live probe
   against `df_engine` mapped its worker to the right thread, passed, and
   aborted on a wrong core request.
- **6. Metric schemas must match exactly.** The error names both missing and
   extra metrics.
- **7. Directions are explicit.** The suffix rule and `metric_direction` are
   gone. Every metric declares `higher_is_better` or `lower_is_better` in
   `metric_directions`. `validate_result` rejects an unknown direction,
   `evaluate_baseline` rejects a missing one, baselines store directions,
   and a direction that changed since the baseline is refused. Tests cover
   a rising backlog ratio declared lower-is-better failing as a regression.
- **8. The lease is an exclusive `fcntl.flock`.** It is held on an open
   descriptor for the whole run. The kernel releases it when the holder
   dies, and the file is never unlinked, which removes the check-then-unlink
   race. `assert_held` also detects a lease file replaced under the lock.
   Tests cover a SIGKILLed holder in a child process, a leftover file that
   holds no lock, and a replaced file.
- **9. The index is republished at 102 tests.** The previous 79-test index is
   preserved as the immutable child `harness-contracts-84ea8da674d1.json`,
   referenced by hash.

### Minor

- **10. Every test carries its comments.** A mechanical check found one test
    without `Scenario:`/`Guarantees:` comments, the nested test in the
    `MeasurementTestCase` contract. It now has both and a descriptive name.
    The check reports none missing in either test file.
- **11. No fixed sleeps in the tests.** The dummy build process and every
    other helper child now block on their own stdin and end when it closes.
    Tests read their readiness line under a `select` deadline.

### Verification

```text
contract suite: Ran 102 tests in 2.570s  OK  (-W error::ResourceWarning)
measure run --case harness-contracts: passed, contract_tests_count 102
full E2E, SERIES_REQUIRE_DOCKER=1: Ran 18 tests in 147.499s  OK, no skips
tools/sanitycheck.py: exit 0
markdownlint: 0 issues; ASCII check: clean; git diff --check: clean
```

### Fix round 1 concerns

- **The core-count gate counts the whole machine.** `physical_cores_sufficient`
  compares the machine's physical core count with eight. The plan asks for
  eight *available* physical cores, which means sibling groups that
  intersect the run's allowed cores. On a restricted cgroup or `taskset`
  these differ. This was not a review finding, so I left it. It is a small
  change plus one test, and it would republish the contract index again.
- **The pre-fix zero cluster is unexplained.** See the section above. It
  does not change the classification of the misses I could observe, but it
  is the one open question on the liveness marker.
- **`harness-local` still has no measured body.** That is by design for
  task 2, which now receives a lifecycle that refuses to pass without its
  snapshots and checks.

## Fix round 2

Re-review of `fcb71d492`: needs fixes, three items. All three are fixed in
`57467a583` "test(series_parquet): validate nested fingerprint inputs, count
available cores, always release the lease". The commit stages
`measurement.py`, `test_measurement.py` and both evidence files by name, and
carries both trailers. Nothing the re-review confirmed as fixed was changed.

- **1. Fingerprint inputs are validated at every depth.** Every `Workload`
  member is now required by name, so `workload={"requests": 100}` fails on
  its missing `seed`. After the material is assembled, `_reject_nulls` walks
  it and fails on a null at any depth, naming the path. Tests cover a
  missing seed, a workload with a single member, and nulls in the workload
  seed, in a nested configuration value and in a sibling-group member.
- **2. The core gate counts cores available to the run.**
  `available_physical_cores` counts the sysfs thread-sibling groups that
  intersect the effective `sched_getaffinity` set, which already reflects
  the cgroup cpuset. Every snapshot records the result as
  `available_physical_core_count`, and `physical_cores_sufficient` gates on
  it, with the machine total kept only in the detail text. Tests cover the
  pure rule, including two SMT siblings counting once and cores outside the
  affinity not counting. A full run confined to one core with two required
  fails the gate and writes no baseline. A full run confined to both SMT
  siblings of one real core reports one physical core and fails.
- **3. The lock is released on every exit path.** `HostLease.held` reports
  whether the lock descriptor is actually held, and `release` is idempotent.
  `RunControls.open` releases the lease if the build monitor fails to start.
  `RunControls.close` records its outcomes in `try` and releases the lease
  in `finally` whenever it is held. A monitor that fails to stop becomes a
  failed `no_concurrent_build` check, because build activity is then
  unobservable, so the remaining checks and the release still happen.
  Tests: when the monitor fails to start, the failed run is published and
  the lease can be taken again. When the monitor fails to stop, the release
  still happens, the failed check names the error, a second close records
  nothing twice, and the lease can be taken again.

Each of the seven new tests fails against the code at `fcb71d492`. I ran
them against that commit's `measurement.py`, `measure.py` and `test_e2e.py`:
nine subtest failures and one error, from the pure rule's function not
existing there.

```text
contract suite: Ran 108 tests in 2.673s  OK  (-W error::ResourceWarning), 0 skips
measure run --case harness-contracts: passed, contract_tests_count 108
index chain: harness-contracts.json (108) -> harness-contracts-66401b6abb33.json (102)
             -> harness-contracts-84ea8da674d1.json (79)
tools/sanitycheck.py: exit 0; ASCII: clean; git diff --check: clean;
Scenario/Guarantees check: none missing; no fixed sleeps
```

E2E was not rerun. No shared launcher path changed: `test_e2e.py` and
`measure.py` are untouched in this round.

### Fix round 2 concern

Rejecting a null at any depth will refuse the buffered topology's planned
configuration as written. The plan specifies `max_age: null` for the durable
buffer, so a buffered run whose effective configuration records that null
cannot be fingerprinted. Whichever task introduces the buffered launcher
should record that setting as an explicit value, or omit it and record the
default elsewhere in the result. The alternative is to exempt configuration
values that are genuinely null in the engine's own configuration. That is
your call; I applied the ruling as given.

## Fix round 3

One finding, closing the round 2 concern. Fixed in `114605eb3`
"test(series_parquet): hash explicit engine configuration nulls as
settings", staged by name with both trailers.

Null rejection at any depth now covers the harness-supplied fingerprint
inputs only: machine, available cores, role placement, workload, schedule
and build identity. The effective engine configuration is hashed verbatim,
explicit nulls included, because a null there is a real setting. The
durable buffer's `max_age: null` is the case the plan requires. The
configuration must still be present and non-empty, and a missing or empty
one still raises.

Tests: the nested-null test now uses a workload member, a build member of
the environment, a sibling-group member and a schedule member. It no longer
uses a configuration value. Two new tests show that a configuration with
`max_age: null` fingerprints successfully, and that `max_age: null` and
`max_age: "1h"` fingerprint differently. Both fail against `57467a583`.

```text
contract suite: Ran 110 tests in 2.658s  OK, 0 skips
index chain: 110 -> 108 (harness-contracts-da32ed89b4af.json) -> 102 -> 79
tools/sanitycheck.py: exit 0; ASCII and whitespace: clean
```

E2E was not rerun; no launcher path changed.
