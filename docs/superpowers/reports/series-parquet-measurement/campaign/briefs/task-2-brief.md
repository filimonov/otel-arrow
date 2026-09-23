### Task 2: Engine launchers, enforced host controls and fast CI

**Expected wall-clock cost:** 30-60 seconds for launcher/control checks; 5-15 minutes for the existing 18-test lane.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Record: `docs/superpowers/reports/series-parquet-measurement/harness-local.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/launcher-ci.json`

**Interfaces:**
- Consumes Task 1's `RunSpec`, JSON schema, baseline evaluator, oracle and sampler.
- Adds keyword-only `topology="strict"`, `buffer_path=None`, `cores=None`, `launcher=None` to `Engine`, preserving legacy calls. A launcher implements `start(argv: list[str], log, env: dict) -> subprocess.Popen` and `pid(process) -> int`; the default is local, and Task 8 supplies the container launcher. All RSS/affinity/kill operations use the engine host PID, never a Docker CLI PID.
- `MeasurementLease(path: Path)` holds an exclusive `fcntl.flock` through run finalization. `environment_snapshot(engine: Engine | None) -> dict`, `check_worker_affinity(observed: dict[int, set[int]], expected: dict[int, set[int]]) -> None` and `build_activity() -> list[dict]` provide the shared environment policy. `run_case` starts a monitor before launching any measured process, captures both snapshots in `finally`, records invalidation events and releases the lease last. Register prebuilt benchmark/producer/store PIDs with the same monitor so engine-less stage runs also record observed thread affinity and both snapshots.

- [ ] **Step 1: Add failing launcher and control tests**

```python
# Scenario: a worker expected on core 4 can actually run on cores 4 and 5.
# Guarantees: runtime pinning warnings cannot silently validate a measurement.
def test_affinity_mismatch_aborts(self):
    with self.assertRaisesRegex(AssertionError, "affinity"):
        check_worker_affinity({123: {4, 5}}, {123: {4}})
```

Add tests with injected procfs observations for a cargo build appearing midway through a run, a second lease holder, absent end snapshot and a changed CPU/core configuration. Each must invalidate the result and prevent baseline publication. These test monitor decisions; real-run verification still reads the actual host. Add a buffered smoke asserting graph edges, unchanged core IDs and retained buffer path across restart.

- [ ] **Step 2: Run red**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing affinity/lease checks or launcher options; existing Task 1 contracts stay green.

- [ ] **Step 3: Add topology and launcher options before configuration serialization**

Insert the buffer into the actual config graph before launch:

```python
nodes["buffer"] = {
    "type": "processor:durable_buffer",
    "config": {
        "path": str(buffer_path),
        "retention_size_cap": "1GiB",
        "size_cap_policy": "backpressure",
        "max_age": None,
        "otlp_handling": "pass_through",
    },
}
pipeline["connections"] = [
    {"from": "receiver", "to": "buffer"},
    {"from": "buffer", "to": "exporter"},
]
```

Single-destination connections resolve to `one_of` in the current config API; do not invent a `round_robin` YAML setting from older README terminology. Verify each input reaches one buffer instance and fail preflight on any broadcast/multiple-recipient routing. Set `config["policies"]["resources"]["core_allocation"] = {"type": "core_set", "set": [{"start": core, "end": core} for core in cores]}`. Do not force a new buffer path on restart or replace a pre-existing retained directory. For measurement overrides deep-merge nested maps into the example config, then serialize the final file and hash it. Keep legacy Engine calls unchanged.

- [ ] **Step 4: Enforce the environment policy for every run**

```python
def check_worker_affinity(observed, expected):
    if observed.keys() != expected.keys():
        raise AssertionError("affinity: worker TID mapping mismatch")
    for tid, wanted in expected.items():
        if observed[tid] != wanted:
            raise AssertionError(f"affinity: tid={tid} actual={observed[tid]} expected={wanted}")
```

Use the shared host-visible path `/tmp/series-parquet-host-measurement.lock` for all checkout/container launchers; acquire `LOCK_EX | LOCK_NB` and record owner PID/start time/run ID. A busy lease aborts before traffic. CI build/provision steps run before acquisition, with a runner-wide concurrency group so jobs sharing a physical host cannot measure concurrently.

Enumerate `/proc/PID/task/*/status` at readiness and on every monitor tick; parse `Name`, `Pid`, `NSpid` and `Cpus_allowed_list`. Map pipeline TIDs using existing pipeline-thread identity/log context plus the unique requested singleton CPU per worker. Linux truncates thread names, so never infer the core ID from a truncated `Name`; fail an ambiguous mapping. Require the expected count and exact per-worker singleton set before input, after restart, throughout measurement and at end. Capture all observed thread affinities, with process role, in both snapshots. `controller/src/lib.rs:2790` only warns if pinning fails, so configuration alone is insufficient. Resolve physical cores/SMT siblings through sysfs and enforce the minimum/role allocation in Global Constraints.

Poll host `/proc/*/cmdline` and process ancestry at most 100ms apart for cargo/rustc compilation and docker build/buildx/buildctl clients; include running build containers via Docker events/inspect for the run interval. Distinguish an idle build daemon from an active build; capture command/PID/start time or build ID without environment secrets. A coverage gap or inaccessible host/build namespace invalidates publishable evidence. Any concurrent build detected after preflight invalidates the whole run even if it later disappears. Stop this run cleanly, retain observations and never terminate unrelated work. Record load at start/end and during sampling; invalid environment, affinity or build checks cannot be overridden by a new fingerprint.

- [ ] **Step 5: Wire fast CI and verify the real launcher**

Add buffered smoke with the same oracle. CI builds with `series_parquet,aws,durable-buffer`, runs all original E2E tests, then `test_measurement`; set no long flag. Retain results/logs with `actions/upload-artifact` on both success and failure. Extend AlloyProducer with optional admin metrics port and host PID discovery, reuse its existing River config, and sample queue size/capacity and enqueue failures without treating infinite-retry `send_failed` as a reliable counter.

Run `harness-local` under the enforced host controls. Register `launcher-ci` to execute strict and buffered local smokes and persist their independent result files plus an index. Apply the Controller baseline policy and direct discovered defects to Task 12. CI retains JSON/logs on success or failure and serializes publishable measurement jobs by physical host.

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
python3 -m crates.validation.tests.series_parquet.measure run --case harness-local --output-dir /tmp/series-measure
python3 -m crates.validation.tests.series_parquet.measure run --case launcher-ci --output-dir /tmp/series-launcher
SERIES_REQUIRE_DOCKER=1 python3 -m unittest crates.validation.tests.series_parquet.test_e2e -v
```

**Recorded numbers:** observed PID/TID affinity, role/core counts, start/end environment, monitor coverage/build detections, strict/buffered oracle counts and legacy test outcomes in `launcher-ci.json`. **Failure:** wrong graph, lost retained path, affinity mismatch, unavailable cores/lease, concurrent build, missing snapshot, changed legacy behavior or absent required Docker coverage.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/harness-local.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/launcher-ci.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/harness-local.json docs/superpowers/reports/series-parquet-measurement/launcher-ci.json
git commit -m "chore: enforce series measurement launch and host controls" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

