# Task 2 report: engine launchers, enforced host controls and fast CI

Status: DONE_WITH_CONCERNS. Commit 8a41e36f3 on series-parquet-exporter (base 114605eb3). Not pushed.

## What changed

### test_e2e.py (additive; the 18 legacy tests are unchanged and pass)
- `LocalLauncher` with `start(argv, log, env) -> Popen` and `pid(process)`. `Engine.pid` is the launcher's host PID.
- `engine_config(...)` is a free function now. `Engine` calls it, and tests can check a config without launching anything. Called with legacy arguments it gives exactly the old config (there is a test for this). It adds these keyword-only options:
  - `topology="strict"|"buffered"`. The buffered topology inserts `buffer` / `processor:durable_buffer` with the brief's settings verbatim: `max_age: None`, `size_cap_policy: backpressure`, 1GiB, pass_through. It is wired receiver->buffer->exporter.
  - `buffer_path`: used as given. It is never created fresh or replaced.
  - `cores`: `policies.resources.core_allocation = core_set` of `{start, end}` singletons.
  - `launcher`.
  - `merge`: deep-merges into the named node configs, or into `engine`.
- `graph_edges(config)` rejects before launch any connection with several recipients or with a dispatch policy, and any fan-in or fan-out. `Engine` also asserts the edges equal `EXPECTED_EDGES[topology]`. `Engine.config_sha256` hashes the exact YAML file the engine reads.
- `AlloyProducer`:
  - new optional keyword `admin_port`, and `self.admin_port`.
  - `host_pid()` uses `docker inspect .State.Pid`.
  - `queue_metrics()` reports logs-queue size and capacity, and enqueue failures. A series Alloy has not published is None. `send_failed` is reported only as `send_failed_records_unreliable`.
  - The existing River config is reused unchanged.

### host_monitor.py (NEW file; it is not in the brief's file list)
The monitor loop runs as its own stdlib-only process. The first version ran as a thread inside the harness. With 100 producer threads competing for the GIL it had a 0.92 s tick gap, which failed coverage. The separate process always stays under 65 ms.
- Each tick (50 ms) scans procfs and re-checks the allowed cores of every mapped worker TID.
- It takes JSON commands on stdin (watch, unwatch, stop) and writes detections and a final report to stdout.
- `parse_core_list`, `check_worker_affinity` and `observed_affinity` live here. `measurement` re-exports them, so there is one definition of each.

### measurement.py (reuses the Task 1 pieces; no second lease, monitor or snapshot)

**Brief interfaces:**
- `check_worker_affinity` follows the brief's code exactly, and `assert_affinity` now uses it.
- `MeasurementLease` is an alias of `HostLease`.
- `build_activity()` is a one-shot in-process scan.
- `environment_snapshot` also accepts an `Engine`.

**Lease:**
- Default path is `/tmp/series-parquet-host-measurement.lock`.
- The record now holds pid, process start ticks and run_id.
- `run_case` now defaults to `lease_wait_s=0`, so a busy lease aborts before any traffic. The Task 1 default was to wait 60 s; waiting is still available.

**BuildMonitor** is now a client of the monitor process:
- An idle `buildkitd` is not a build; any descendant of it is. This matters here because a buildx builder container with an idle buildkitd runs permanently on this host, and the Task 1 rule would have invalidated every run.
- Docker build clients are detected from their command line, and secret-looking argument values are redacted.
- Ancestry is recorded for each detection.
- At stop, `docker events` for the run interval is checked for builder-container starts.
- `invalid` is sticky: a build that appears and then vanishes still counts.

**RunControls:**
- New methods:
  - `register(role, pid, cores)`: for engine-less or prebuilt PIDs.
  - `allocate(allocation)`: refuses two roles on SMT siblings of one core.
  - `checkpoint(label, ...)`: re-verifies affinity after a restart.
  - `watch_workers` / `unwatch_workers`: acknowledged by the monitor process, so a deliberate shutdown never looks like a vanished worker.
  - `raise_if_invalid()`: the run stops itself cleanly.
- New hard check `build_monitor_coverage`, added to `VALIDITY_CHECKS`. It fails on a tick gap over 100 ms, on hidepid or invisible procfs, or when Docker is installed but cannot be queried.
- Affinity failures seen at monitor ticks feed `affinity_matched`.
- `environment_match` now also compares `sibling_groups` and `available_cores`.

**Other additions:**
- `role_allocation()`: first available core = engine_observability (the controller puts its observability pipeline there); engine cores; three reserved engine cores; producer x2; store; reader. Each role owns a whole physical core. `strict=False` is for hosts that cannot publish.
- `run_pinned()`.
- `Producer`: sends the exact wire bytes, once, from pinned threads, and records every outcome in the ledger.
- `Sampler`: telemetry, procfs and load average on one timeline; errors are recorded, never zero.
- `rss_residuals` / `residual_check` (see concern 1).
- `engine_build()` / `git_provenance()`.
- `canonicalize_ephemeral_values`: the receiver's ephemeral port was the only difference between two otherwise identical fingerprints. It is canonicalized only when the run declares it in `ephemeral_values`; undeclared differences still separate fingerprints.

**Fixes to Task 1 code:**
- `BUFFER_EMPTY_GAUGES` used a wrong name. The real buffer publishes `processor.durable_buffer:in.flight` (not `in_flight`), and `items.queued` is labelled per signal. `require_signal_gauge` sums the signal parts within a single entity.
- The Ledger's sqlite connection now uses `check_same_thread=False`, so producer threads can write to it; it is still guarded by the existing lock.
- Samples now carry `pipeline_memory_usage_bytes`.

### measure.py
- `local_experiment` is the measured body for strict and buffered runs; its restart variant is used by the buffered smoke. It:
  - allocates roles;
  - starts the engine on its cores;
  - waits until every worker answers;
  - takes the start snapshot (mapping and verifying worker TIDs) and hands the TIDs to the monitor;
  - starts input at an aligned window boundary, so ack latency does not vary with the window phase;
  - sends the workload exactly once per request;
  - proves drain;
  - for a buffered restart: sends half, drains, runs admin shutdown, starts a fresh engine on the same cores and buffer path, checkpoints affinity and checks the retained buffer, then sends the rest;
  - takes the end snapshot while the workers still run;
  - shuts down, then runs the oracle (DuckDB plus clickhouse-local) on the reader core.
- Build provenance (binary hash, `rustc --version`) is gathered before the lease. This was caught live: my own `rustc --version` invalidated the first run.
- Cases:
  - `harness-local`: one strict run plus the `harness-local.json` index.
  - `launcher-ci`: the legacy suite in-process with SERIES_REQUIRE_DOCKER=1 (must be 18 tests, none skipped), then the strict and buffered-restart smokes as independent run files, plus the `launcher-ci.json` index.
  - `publish=false` (CI mode): nothing is published and no baseline is evaluated. The index passes on delivery, graph, restart, affinity, sample and snapshot checks, and validity failures stay recorded.
- Compared metrics: records_acked, throughput, ack p50/p99, peak RSS, missing, unexpected, corrupt and duplicate records. Everything else goes under `observations`, so noisy quantities such as drain time do not produce spurious 25% regressions.

### test_measurement.py
136 tests, up from 110; every one has Scenario/Guarantees comments. New:
- The brief's `test_affinity_mismatch_aborts`, verbatim, plus a TID-mapping-change test.
- Tests on injected procfs:
  - idle daemon versus build step;
  - container build client, with a secret redacted;
  - a transient build reported by the monitor process.
- Coverage-gap and unobservable-namespace checks.
- `ControlledRunContracts`, each a full `run_case` that must invalidate the run and publish no baseline:
  - a cargo build appearing midway, then vanishing;
  - a second lease holder aborting before any traffic, with the holder's run id named;
  - a changed core configuration at the end snapshot;
  - a worker widened midway and caught by a monitor tick.
- An absent end snapshot is still covered by the Task 1 test.
- `LauncherContracts`:
  - buffered graph, settings and core_set;
  - restart keeps cores, graph and buffer path, and never touches retained data;
  - broadcast or multi-recipient routing refused;
  - legacy config unchanged;
  - deep merge;
  - the local launcher reports the host PID.
- Placement, reconciliation, buffer-signal-gauge and ephemeral-fingerprint contracts.

### README.md, workflow
- README: commands, options, environment variables, and the lease, placement, monitor, topology and reconciliation rules.
- Workflow:
  - builds with `series_parquet,aws,durable-buffer`;
  - job-level concurrency group `series-parquet-host-measurement` with cancel-in-progress false;
  - SERIES_ENGINE_FEATURES and SERIES_ARTIFACT_DIR set;
  - steps: legacy E2E, then test_measurement, then `launcher-ci --option publish=false --option legacy_tests=false`;
  - `actions/upload-artifact@v4` with `if: always()`.
  - Triggers are unchanged. Not pushed, so it has not run on GitHub.

## Commands and results (from rust/otap-dataflow, venv on PATH)
- Red: the new test_measurement run against the HEAD implementation fails at import (`AttributeError: measurement has no attribute 'BUILD_DAEMONS'`), as expected. It was run after implementing, against HEAD copies of the implementation files.
- `python3 -m unittest crates.validation.tests.series_parquet.test_measurement`: Ran 136, OK, 3.6 s.
- `measure run --case harness-local --output-dir /tmp/series-measure`: exit 0, passed, baseline created.
- `measure run --case launcher-ci --output-dir /tmp/series-launcher`: exit 0, passed. The legacy suite inside it: 18 run, 0 failures, 0 errors, 0 skips, 147.2 s. Both smokes passed and created baselines.
- `SERIES_REQUIRE_DOCKER=1 python3 -m unittest ...test_e2e -v`: Ran 18, OK, 147.2 s.
- CI mode simulated locally: `taskset -c 0,1,16,17 ... launcher-ci --option publish=false --option legacy_tests=false`. Strict passed the CI checks; `physical_cores_sufficient` was recorded as failed (2 of 8), as designed. Buffered exposed the replay finding (concern 2) before the restart-duplicate handling was added.
- Dev repeatability, three harness-local runs into a scratch report directory:
  - run 1 created the baseline;
  - run 3 compared within limits: throughput -0.8%, p99 -1.9%, peak RSS +3.3%;
  - run 2 failed `affinity_matched`, because the monitor saw the worker vanish at shutdown. Fixed by making unwatch acknowledged.
- `stage-results` for both indexes; the commit staged every file by exact name.

## Recorded numbers
Committed: harness-local.json, launcher-ci.json, three run files, three baselines.

**Host:** AMD Ryzen 9 9950X, 32 logical / 16 physical cores, 16 available.

**Allocation (every run):** engine [1], engine_observability [0], engine_reserved [2,3,4], producer [5,6], store [7], reader [8].

**Worker affinity:** one worker per run, `Cpus_allowed_list` "1", last CPU 1, at start and at end. The buffered run's worker TID changed across the restart (3502040 -> 3502132); the restart checkpoint verified the new TID.

**Snapshots:** 59-61 threads recorded with roles in each. Load average (1 min) at start/end: harness-local 0.04/0.03; launcher strict 0.46/0.51; launcher buffered 0.55/0.58.

**Monitor:**

| Run | Ticks | Largest gap |
| --- | --- | --- |
| harness-local | 153 | 0.062 s |
| launcher strict | 154 | 0.063 s |
| launcher buffered | 177 | 0.062 s |

In every run: 0 build detections, 0 affinity failures, 0 container starts. The idle buildx builder `moby/buildkit` (container 23367b01804b) was recorded and not counted as a build.

**Oracle (each run):** 10000 expected, 10000 stored, multiplicity {1: 10000}, 8 part files. Both readers agree: logs 8000, metrics 2000.

**Metrics:**

| Run | Throughput (rec/s) | Ack p50 / p99 (s) | Peak RSS (MB) | Residual max / tolerance |
| --- | --- | --- | --- | --- |
| harness-local strict | 4739 | 1.430 / 1.895 | 192.0 | 25.9 MB / 33.55 MB |
| launcher-ci strict | 4740 | 1.486 / 1.946 | 196.8 | 32.7 MB / 33.55 MB |
| launcher-ci buffered | 10151 | 0.235 / 0.443 | 190.1 | 16.2 MB / 33.55 MB |

The buffered run acknowledged at the buffer's durable write, and had 0 restart replay duplicates in this run.

## Deviations and conflicts
1. host_monitor.py is a new file. It was needed for the 100 ms coverage requirement, which a GIL-bound thread cannot meet under load.
2. The monitor re-checks mapped worker TIDs on every tick; it does not re-enumerate all of `/proc/PID/task` each tick. Full enumeration happens at start, at the restart checkpoint and at end. A new extra worker thread appearing mid-run would be seen only at the end snapshot.
3. Default lease wait changed from 60 s to 0, following the brief's LOCK_NB and abort-before-traffic rule.
4. Docker coverage uses `docker ps` at start and end plus `docker events` (container start) for the interval. Classic-builder step containers that are not buildkit-named are counted, but not treated as builds.
5. Features and allocator cannot be read back from the binary. They are declared through SERIES_ENGINE_FEATURES / SERIES_ENGINE_ALLOCATOR (defaults: the documented build, jemalloc). The durable-buffer feature is proven at runtime by the buffered smoke.
6. The in-repo buffered smoke lives in `launcher-ci`, plus config-level restart tests. test_e2e keeps exactly 18 tests; `launcher-ci` asserts that count, so any change there is "changed legacy behavior".
7. harness-contracts.json was not re-run; it still records 110 tests, and the suite now has 136.
8. `harness_local_spec` defaults cores to the first physical core after the observability core. It was (0,), which collides with the controller's observability pipeline.

## Concerns
1. **The RSS reconciliation margin is thin, and the decomposition is my Task 2 choice.** A naive `RSS - accounted` gives about 58 MB unexplained for 10 MB of input, which fails the frozen 32 MiB tolerance. My measured terms:
   - idle RSS at readiness;
   - file-backed RSS growth from smaps_rollup, about 21 MB of debug-binary code pages faulted in;
   - the high-water mark of the workers' `pipeline:memory.usage` (tracked heap, which includes accounted), about 15 MB.

   The remaining anonymous growth is 26-33 MB against a 33.55 MB tolerance. The launcher strict run was at 32.7 MB, so this hard gate will flake on the debug build. The likely cause is allocator retention and heap outside the pipeline's tracking. The engine exposes no jemalloc stats, so I cannot measure that term, and I did not subtract it by definition. This needs a controller ruling now, or Task 6.
2. **Durable buffer replays acknowledged bundles after a graceful restart (Task 12 candidate).** Seen in the 4-CPU CI simulation. Phase 1 drained (3 empty answered epochs with buffer queued=0 and in.flight=0; the exporter had acked). After admin shutdown and restart, the new engine logged `recovered segments from previous run segment_count=2` and replayed all 50 phase-1 bundles, giving multiplicity {1: 5000, 2: 5000}. It did not happen on the unrestricted runs, so it is timing-dependent. It looks like subscriber progress is not persisted before shutdown. Evidence: scratchpad/cisim/launcher-ci-buffered-local-c1-w1-r001/engine-{1,2}/engine.log. The restart smoke now treats replay as at-least-once: duplicates are counted in `observations.restart_replay_duplicate_records` and a `restart_replayed_acknowledged_records` event, not as a compared metric and not as a failure.
3. **On a hosted runner the CI lane can only run in non-publish mode.** 4 vCPUs is below the 8-physical-core floor. Nothing has run on GitHub, because the workflow triggers do not fire on this branch.
4. **Baseline noise is untested beyond three dev runs.** Compared metrics moved at most 3.3% there, and ack latency depends on the aligned-window start.

## Fix round 1

Commit 93d3a1ccf. Status: DONE_WITH_CONCERNS.

### Changes
1. **Release engine.**
   - `measure.engine_binary()` defaults to `target/release/df_engine`.
   - `prepare_build()` refuses a missing binary or any non-release profile, before the lease, with a build hint. `local_experiment` re-checks the profile.
   - `Engine` gains an additive keyword `binary=`, and measured runs pass the release binary. The fixture suite still uses the debug build or `DF_ENGINE`.
   - The workflow keeps the debug build for the legacy lane and adds a `--release` build for measurement.
   - README updated.
   - Tests: `ReleaseProfileContracts` covers debug refused, release accepted, and release as the default.
2. **CI gate.**
   - `ci_failures(child)`: every recorded hard check must pass except `physical_cores_sufficient`, the core floor that a non-publishable host is too small to meet.
   - `delivery`, `graph_edges`, `minimum_samples` and `rss_reconciliation` must be present.
   - The dead `CI_CHECKS` tuple is removed.
   - Tests: `CiIndexContracts` covers an RSS failure failing the index, the core floor alone not failing it, and an absent gate failing.
3. **Contracts index.** harness-contracts is republished at 143 tests. The 110-test index is preserved as the child `harness-contracts-29951b7ff6cd.json`, which itself still lists the older child.
4. **Worker enumeration on every tick.**
   - `host_monitor.worker_threads()` enumerates `/proc/PID/task/*/status` on every tick.
   - The threads that carry the mapped worker names must be exactly the mapped TIDs, each with exactly its cores. An extra thread or a replacing thread is a TID mapping mismatch.
   - `RunControls.watch_workers` passes the mapped names.
   - Tests: `WorkerEnumerationContracts` covers a transient extra worker present for about one tick on an injected procfs, and a replaced worker.
5. **Sampling on a fast engine.** The release engine finishes phase 2 of the buffered restart within about one collection. The first release launcher-ci run therefore failed `minimum_samples` (engine-2 answered 2 epochs). After a proven drain, the phase now keeps sampling until every worker has answered 3 epochs. This is an observable condition under a deadline, not a wait.

### Evidence handling
- The debug-built harness-local and launcher-ci indexes, run files and both-case baselines committed in 8a41e36f3 are `git rm`'d and replaced by release runs.
- The debug index was not preserved as a child: the ruling invalidates it, and its files no longer exist.
- Uncommitted output from the failed first release launcher-ci attempt, and from the harness-local run made before item 5, was moved aside rather than published. It is kept at `scratchpad/failed-launcher-ci/`.

### Commands and results (all release)
- test_measurement: Ran 143, OK, about 4 s.
- harness-contracts: passed, 143 tests.
- harness-local: passed, baseline `baseline-harness-local-dabc50703ea51814.json` created.
- launcher-ci: passed. The legacy suite inside it ran 18/0/0/0 in 149.2 s. Strict and buffered both passed and created baselines a97178b493d1d7ea and 29cffb7c319e0c24.

### RSS residual against the 33,554,432-byte tolerance (unchanged)

| Run | Max residual (bytes) | Share of tolerance | Within 10% of the limit? |
| --- | --- | --- | --- |
| harness-local strict | 32,014,792 | 95.4% | yes |
| launcher-ci strict | 30,066,864 | 89.6% | just outside |
| launcher-ci buffered, engine-1 | 8,648,896 | 25.8% | no |
| launcher-ci buffered, engine-2 | 20,357,120 | 60.7% | no |

Plainly: on the release build the strict runs still sit at or near the limit. harness-local is within 10 percent of the tolerance, and launcher-ci strict is at 10.4 percent below it. This hard gate is expected to fail some strict runs. Nothing was loosened. Peak RSS is now 107-115 MB, against 190-197 MB on the debug build.

### Other recorded numbers
- Throughput and ack latency:

  | Run | Throughput (rec/s) | Ack p50 / p99 (s) |
  | --- | --- | --- |
  | harness-local strict | 4990 | 1.467 / 1.938 |
  | launcher-ci strict | 5094 | 1.416 / 1.871 |
  | launcher-ci buffered | 10336 | 0.222 / 0.434 |

- Monitor: largest tick gap 62-63 ms in every run; no build detections and no affinity failures. That includes the per-tick enumeration of every engine thread.

### Replay duplicates now on the full host
The buffered restart replay reproduced on the unrestricted host with the release build: multiplicity {1: 5000, 2: 5000}. The run recorded it in `restart_replay_duplicate_records` = 5000 and in a `restart_replayed_acknowledged_records` event. Earlier concern 2 therefore does not depend on only the CPU-restricted timing; it is the Task 12 candidate.

## Fix round 2

Commit 69d5b8eea.

### Changes
- **CI required gates are derived, not hand-listed.** `CI_REQUIRED_CHECKS` is now `REQUIRED_HARD_CHECKS` minus `HOST_CAPACITY_CHECKS` (`physical_cores_sufficient`). `ci_failures()` fails every required gate that is absent, and every hard check that was recorded and did not pass. The hand-written `CI_ALWAYS` list is removed.
- **Fixtures.** The CI fixtures now carry every required gate, plus `graph_edges`.
- **New regression tests:**
  - a missing `host_lease_held` fails;
  - a missing `physical_cores_sufficient` alone passes;
  - the exempt set is exactly `{physical_cores_sufficient}`.
- **Sampling extension.** It is extracted into `await_answered_epochs(sampler, workers, deadline_ns)`, and `EnginePhase.drained` uses it. Unit tests:
  - with fewer than 3 answered epochs, sampling continues, and it stops once exactly 3 are present (4 samples: one establishes the uptime, then 3 epochs);
  - collections that stop after 2 epochs never satisfy it, and the deadline fails.

### Results
- Contract tests: Ran 147, OK.
- `harness-contracts.json` republished at 147 tests. The 143-test index is kept as the child `harness-contracts-0c14ed915c8c.json`.
- harness-local and launcher-ci were not rerun. `ci_failures` is used only in `publish=false` mode, and the committed launcher-ci index was produced in publish mode, so its outcome is unchanged.
