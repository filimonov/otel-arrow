### Task 7: Thirty-minute soak and bounded PR-tier coverage

**Expected wall-clock cost:** 70-85 minutes for two 30-minute input phases plus verification; 2-3 minutes for the fast PR-tier pair, inside the five-minute added-suite budget.

**Files:**
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Record: `docs/superpowers/reports/series-parquet-measurement/soak-strict.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/soak-buffered.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `capacity_search` results, `run_named`, `Ledger`, strict sampler, `drain`, oracle and Task 6 measured memory baselines and hard residual checks.
- Produces cases `soak-strict`, `soak-buffered`, `pr-soak-strict`, `pr-soak-buffered`, and `soak_checks(result: dict) -> None`.
- Soak output uses the common schema with full 1s samples and one-minute aggregate rate/RSS series. `metrics.input_phase_s` measures actual producer-active monotonic duration, excluding startup/warm-up/drain.

- [ ] **Step 1: Write the opt-in acceptance tests before implementing soak checks**

```python
class LongSoakTests(MeasurementTestCase):
    # Scenario: mixed supported telemetry flows for thirty minutes in strict mode.
    # Guarantees: all ACKed IDs survive drain and RSS observations satisfy validity and the Controller baseline policy.
    def test_strict_thirty_minutes(self):
        require_long()
        result = run_named("soak-strict", self.output_dir)
        self.assertGreaterEqual(result["metrics"]["input_phase_s"], 1800)
        soak_checks(result)

    # Scenario: mixed telemetry flows for thirty minutes through a persistent buffer.
    # Guarantees: WAL ACKs reconcile with stored IDs and both memory domains drain.
    def test_buffered_thirty_minutes(self):
        require_long()
        result = run_named("soak-buffered", self.output_dir)
        self.assertGreaterEqual(result["metrics"]["input_phase_s"], 1800)
        soak_checks(result)
```

Use Task 1's `MeasurementTestCase` to retain `self.output_dir`, rather than a TemporaryDirectory deleted on failure. Add a fast arithmetic regression with a synthetic rising RSS series; paired with a matching synthetic baseline it must fail the common regression check without launching an engine; without a baseline it becomes a candidate only after validity checks. Synthetic samples test analysis only, never stand in for a failure injection or a measured soak.

- [ ] **Step 2: Run red on analysis, then implement checks**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_soak -v
```

Expected: short analysis test fails due to absent `soak_checks`; long tests skip without the opt-in variable.

```python
def soak_checks(result):
    metrics = result["metrics"]
    failures = []
    for name in ("missing_acked_ids", "missing_intended_ids", "descriptor_violations",
                 "reader_disagreements", "permanent_rejections", "buffer_loss_items"):
        if metrics[name] != 0:
            failures.append(f"{name}={metrics[name]}")
    if metrics["sample_coverage_ratio"] < 0.99:
        failures.append("insufficient RSS/rate samples")
    evaluate_baseline(result)  # Shared Controller policy; never an absolute slope gate.
    if failures:
        raise AssertionError("; ".join(failures))
```

The same function requires explicit zero buffer-loss metrics in strict mode tagged `not_applicable: no_buffer`; absence of a required buffered metric is an instrumentation error, not a zero default.

- [ ] **Step 3: Run sustained load with a bounded producer and explicit drain phase**

Choose 70% of the lower repeatable local/S3 sustainable mixed-workload rate from Task 5 for one worker, 15s windows, 10k hot series with deterministic 1% churn, 1KiB bodies and 20% metric points. Set `Workload.requests = ceil(offered_records_per_s * 1800 / records_per_request)` plus the separately labelled warm-up cohort before starting; do not accidentally retain the 100-request smoke default. Use MinIO for strict and RustFS for buffered; failure tasks cover both stores in both topologies. Record the actual offered rate, series count and churn count. Input lasts at least 1,800 monotonic seconds after readiness and warm-up; no reduction to 30 minutes including drain is accepted. If generation exhausts its planned cohort early, the duration gate fails instead of counting idle time as load.

Every 1s sample records attempted, accepted and unique committed record deltas; wire and object bytes; in-flight requests; ACTIVE/FLUSHING/pending/notify gauges; oldest unacked; RSS and FD count; per-worker cache occupancy; store/proxy/producer RSS; buffered disk/queued/in-flight/retry gauges. Unique committed IDs can be enumerated asynchronously from completed files into the disk-backed oracle; keep reader work on separate cores and record lag. Do not read partial uploads or make the writer wait for a full-table query every second. List new completed objects incrementally and verify them after input stops; report provisional physical-row rate separately if unique-ID lag prevents an instantaneous unique rate.

After generation stops, continue authorized strict retries and buffer replay. Measure backlog at stop, time-to-drain and unique drain records/s, plus full-run average and final five-minute input/storage rates. Verify every acknowledged and every eventually accepted intended ID, exact payload semantics, descriptors and multiplicity histogram after all files are complete. Healthy soak has no injected failures and should be duplicate-free; any actual timeout/retry changes the run to a retry-bearing result with counted duplicates and an explained cause, not an unqualified healthy pass.

Retain Alloy as an additional short producer compatibility run using the shipped batching/retry config and existing `AlloyProducer`. Collect producer `/metrics` and RSS; reject enqueue loss. Do not use its historical queue calculations as a substitute for exact synthetic-producer ID accounting or as this exporter's peak throughput number.

- [ ] **Step 4: Keep the PR-tier test short and real**

Add two 60-75s cases with 1s windows, forced byte/request rotations, and one `DockerStore.stop()` outage past an overridden 3s flush deadline. Gate stop on a successful baseline object and nonempty ACTIVE; keep producing a finite ledger-backed workload. Gate recovery on a storage failure/NACK and elapsed deadline, not a fixed sleep. Use the existing `DockerStore.stop()` and `DockerStore.recover()` helpers here, so this task is independently runnable before proxy tooling exists. Require retryable NACK/local deadline classification in strict mode and buffer retries in buffered mode; recover, drain, assert the common oracle, retained-state caps and hard residual check; record memory/oldest age under the Controller baseline policy. Add them to the fast CI lane after `test_measurement`.

- [ ] **Step 5: Verify, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_soak -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure soak --output-dir /tmp/series-soak
```

**Recorded numbers:** at least 1,800 samples/input seconds per topology, input/ACK/storage/drain records and byte rates, duration, RSS curve/slope/median change/peak, FD peak, descriptor count/violations, duplicate histogram, missing IDs, buffer memory/disk and retry counts. **Failure:** any common gate, insufficient duration or samples, non-draining backlog, a regression under the Controller baseline policy or unexplained residual, missing ACKed record, descriptor/reader disagreement, unrecorded duplicate, producer enqueue loss or buffer retention loss.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/soak-strict.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/soak-buffered.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/soak-strict.json docs/superpowers/reports/series-parquet-measurement/soak-buffered.json
git commit -m "chore: record thirty-minute series parquet soak acceptance" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

**Amendment (user decision 2026-09-23): heap dumps across the soak.** Using the raw-dump profiling mode from the first Task 12 item, each 30-minute soak also runs once with allocation sampling and takes a raw jemalloc dump after warm-up, at the midpoint and at the end of the input phase. `jeprof --base` between the first and last dump names every stack that grew. Growth that the ledger does not explain is a Task 12 finding. The profiled soak is diagnostic and never sets the soak baseline.

