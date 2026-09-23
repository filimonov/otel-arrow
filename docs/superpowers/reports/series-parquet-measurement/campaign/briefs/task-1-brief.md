### Task 1: Deterministic measurement harness and durable result contract

**Expected wall-clock cost:** 15-30 seconds for harness/schema contract tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/harness-contracts.json`

**Interfaces:**
- Consumes existing `Engine`, `DockerStore(kind)`, `AlloyProducer`, `rss_bytes`, `verify_layout`, `verify_readers`, `clickhouse_reader`, and canonical helpers without removing their signatures.
- Produces the shared contracts, strict sampler/drain and atomic results. Task 2 adds topology/launcher options and enforced host-run controls before publishing performance evidence.
- `evaluate_baseline(result: dict, *, baselines: dict | None = None) -> dict` is the sole evaluator for the Controller baseline policy. It checks hard gates first, canonicalizes the fingerprint, resolves an immutable matching baseline (or uses test-supplied baselines), and returns the successful decision plus per-metric signed regressions. A hard failure or failed regression attaches the decision to the result and raises AssertionError; `run_case` still publishes the failed JSON in `finally`. For repeated families, evaluate only after all required child runs and stability checks are complete, never after the first child. On valid new fingerprints it atomically writes a candidate baseline and references it in `baseline_files`; the commit step publishes it. A failed hard gate or unstable sample never creates a baseline. Default loading uses the result's recorded report directory; no hidden current-directory lookup.

- [ ] **Step 1: Add failing ledger/oracle tests with exact negative controls**

```python
# Scenario: an acknowledged histogram is absent although its sibling gauge exists.
# Guarantees: per-kind stable IDs catch loss that aggregate metric counts conceal.
def test_missing_metric_kind_fails(self):
    expected = {"r:gauge": "a", "r:histogram": "b"}
    actual = {"r:gauge": ["a", "a"]}
    with self.assertRaisesRegex(AssertionError, "r:histogram"):
        assert_records(expected, actual, healthy=False)

# Scenario: replay preserves payloads but adds two copies of one supported record.
# Guarantees: duplicates are counted independently from missing or corrupt records.
def test_replay_multiplicity(self):
    actual = {"r:log": ["a", "a", "a"], "s:log": ["b"]}
    self.assertEqual(
        assert_records({"r:log": "a", "s:log": "b"}, actual, healthy=False),
        {1: 1, 3: 1},
    )

# Scenario: rebuilding a workload request for retry uses the original index and seed.
# Guarantees: retry bytes and all expected record IDs are identical.
def test_retry_is_deterministic(self):
    workload = Workload(seed=73, records_per_request=9)
    self.assertEqual(build_request(workload, 7), build_request(workload, 7))
```

`assert_records(expected: dict[str, str], actual: dict[str, list[str]], *, healthy: bool) -> dict[int, int]` is the small in-memory contract test adapter. Production `read_oracle` performs the same comparisons using SQLite/DuckDB joins so the soak's cardinality does not inflate producer RSS.

- [ ] **Step 2: Run red from the Rust workspace**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: import/name failure for the new measurement module, not a missing dependency or binary.

- [ ] **Step 3: Implement deterministic IDs, payloads and loss detection**

```python
def assert_records(expected, actual, *, healthy):
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    if missing or unexpected:
        raise AssertionError(f"missing={missing[:10]} unexpected={unexpected[:10]}")
    histogram = collections.Counter()
    for record_id, expected_hash in expected.items():
        copies = actual[record_id]
        if not copies or any(value != expected_hash for value in copies):
            raise AssertionError(f"corrupt={record_id}")
        if healthy and len(copies) != 1:
            raise AssertionError(f"unexpected multiplicity={record_id}:{len(copies)}")
        histogram[len(copies)] += 1
    return dict(sorted(histogram.items()))
```

For logs, derive from existing `log_request`; put the stable ID as a fixed-width body prefix followed by seeded printable padding. Keep `host.id`, service name and scope compatible with existing fixture readers. Vary `logger.name` by `request_index % series`; do not add the ID to configured series identity. For metrics, derive from `metric_request`, remove `request.id`, and construct the requested number of points with fixed series attributes. Encode uniqueness in `(metric name, time_unix_nano)` with `time_unix_nano = 1789960500000000000 + ordinal * 1000`; give gauge, sum, populated histogram and empty-distribution histogram distinct stable kind names. Integer/double values and bucket arrays derive from the seed/index. Keep metric names/attributes from a finite series pool. Exporter rounding and supported nullable representations define the expected canonical payload, with an independent explicit fixture for each kind; do not hash producer protobuf bytes and compare them to unrelated Parquet bytes.

Use deterministic protobuf serialization. Count actual supported points, not requests or `POINTS_PER_METRIC * request_count` for general workloads. Preserve small legacy request.id fixtures for existing tests. Record exact series cardinality separately from record cardinality.

Create SQLite with WAL mode and `synchronous=FULL`; retain bounded request metadata in memory and regenerate retry bytes by index. Enforce unique ID primary keys and immutable request hashes. Record attempted IDs, successful RPC ACKs, retryable NACKs, local deadlines, partial rejections, and outstanding IDs separately. Import and reuse `RETRYABLE_CODES` from `test_e2e.py:2327` without copying its set. It includes CANCELLED and excludes ABORTED. Add ABORTED only after repository/real-run evidence and a regression test that actually produces it. Classify producer-local deadline/connection failures separately; unexpected permanent statuses fail. Retries have a bounded deadline and capped exponential scheduling, driven by next-attempt monotonic time, not a fixed test sleep.

- [ ] **Step 4: Implement the strict JSON telemetry/drain path**

Reuse `engine_metrics(engine)` from `test_e2e.py:2240` and parse its `metric_sets` with original names/attributes. Require zeroes to be present and group/pipeline/node/core identities to select every worker exactly once. Require ACTIVE/FLUSHING/pending/token/cache/accounted/budget/oldest gauges. For each worker record `pipeline.uptime` from the `pipeline` metric set: `pipeline_metrics.rs:545` updates it during collection. The API's top-level timestamp is generated per scrape (`telemetry.rs:433`) and Prometheus suppresses zeroes (`telemetry.rs:1515`), so neither is an epoch discriminator. Reject uptime decreases within one process generation; track restarts separately. Only accumulate interval deltas on an uptime advance. Persist raw JSON and labels; sample procfs RSS/stat/smaps/FDs and buffer allocated disk bytes on the same monotonic timeline, keeping process roles separate.

Drain records each worker's initial uptime, then requires three subsequent consecutive increases with empty exporter requests/notifications and, when buffered, no queued/in-flight/retry work. Repeated responses with identical uptime add zero observations; a nonempty observation resets the empty streak. Missing gauges or no advances fail under the monotonic deadline. After those observations, admin-shutdown and inspect the store. The oracle checks all acknowledged and eventually accepted intended IDs, descriptor coverage per `(signal,date,hour,writer_id,boot_id,series_id)`, hashes, sort metadata/order, joins, payloads, multiplicities and both readers using SQL rather than unbounded Python collections.

- [ ] **Step 5: Write atomic results and baseline decisions**

```python
def write_result(path, result):
    encoded = json.dumps(result, sort_keys=True, indent=2, ensure_ascii=True,
                         allow_nan=False) + "\n"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(encoded, encoding="ascii")
    temporary.replace(path)
```

Validate mandatory fields, units, checks and status before writing; a failed result may omit unavailable measured fields only with explicit reasons. Register `harness-local` with 100 requests, mixed supported signals, 1s windows and exact no-retry multiplicity; its real measurement runs in Task 2 after host controls exist. Add synthetic contract tests for three unchanged uptime responses (no drain), three increasing empty epochs (drain), a zero-valued required gauge, a mismatched fingerprint (new baseline), and matching-fingerprint improvement/regression. Test that a correctness or residual failure cannot establish a baseline. Keep the configured numerical boundary in the shared evaluator alone. Test `stage_run_files` with a temporary index/child/baseline tree and a mocked subprocess invocation; assert every exact path is staged, hashes are checked and path escape is rejected. Each test carries specific Scenario/Guarantees comments.

Register `harness-contracts` to run `test_measurement` through `unittest.TextTestRunner`, collect `testsRun`, failures, errors and skips, write the verification-only index below and publish it with `publish_result_tree`. Exit nonzero if the runner was unsuccessful. It launches no measured engine and writes no measured baseline.

- [ ] **Step 6: Verify, record numbers and commit**

```bash
python3 -m crates.validation.tests.series_parquet.measure run --case harness-contracts --output-dir /tmp/series-contracts
```

**Recorded numbers:** contract-test counts and ledger/oracle/schema/epoch/baseline/staging decisions in `harness-contracts.json`, a verification index with `artifact_kind: contract_checks`, `run_files: []` and `baseline_files: []`. This is no performance measurement and cannot create a measured baseline. **Failure:** broken contract or changed legacy assertions. Task 2 produces the real `harness-local.json` under the Controller baseline/environment policy; discovered defects enter Task 12.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/harness-contracts.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/harness-contracts.json
git commit -m "chore: add deterministic series measurement harness" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

