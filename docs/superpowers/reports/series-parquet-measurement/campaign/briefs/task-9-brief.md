### Task 9: S3 fault state machines and recovery evidence

**Expected wall-clock cost:** 15-25 minutes for both stores/topologies; 20-40 seconds for the optional fast proxy activation smoke.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-s3.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes Task 2 launcher/lease, Task 8 FaultRig and successful probes, Engine/DockerStore, run schema, ledger, sampler and oracle.
- `failure_case(family: str, fault: str, topology: str, store: str, output_dir: Path) -> dict` runs one matrix cell; family `s3` supports `slow`, `http503`, `store_outage`. Every cell's result includes immutable before/during/after evidence, records, duplicates, timings and peak memory.
- `fault_check(result: dict) -> None` requires `fault_observed`, `recovered`, `drained`, zero missing/coverage/semantic violations, measured duplicates and bounded resources. Families in Tasks 10-11 reuse this exact check.

- [ ] **Step 1: Add a failing both-topology recovery test**

```python
class S3FailureTests(MeasurementTestCase):
    # Scenario: a real S3 HTTP 503 reaches each topology before service recovers.
    # Guarantees: recovery preserves every ACKed ID and reports replay multiplicities.
    def test_http503_recovery(self):
        require_long()
        for topology in ("strict", "buffered"):
            for store in ("minio", "rustfs"):
                with self.subTest(topology=topology, store=store):
                    result = failure_case("s3", "http503", topology, store,
                                          self.output_dir)
                    self.assertGreater(result["metrics"]["http_503_responses"], 0)
                    fault_check(result)
```

Also add short non-Docker checks that missing prerequisites yield a clean SkipTest only in optional mode, required mode fails, and a post-preflight fault activation error is never converted into a skip. Give each its Scenario/Guarantees comments.

- [ ] **Step 2: Run red with already provisioned tools**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
```

Expected: missing S3 family registration after Task 8 preflight succeeds; prerequisite tests remain green. Image provisioning belongs to Task 8.

- [ ] **Step 3: Implement the three S3 cases as observable state machines**

```python
def fault_check(result):
    checks = result["checks"]
    for name in ("fault_observed", "recovered", "drained", "at_least_once",
                 "descriptor_coverage", "reader_agreement", "bounded_resources"):
        if checks.get(name) is not True:
            raise AssertionError(f"failed required fault check: {name}")
    if result["metrics"]["missing_acked_ids"] != 0:
        raise AssertionError("acknowledged supported record missing")
    if not isinstance(result["metrics"]["multiplicity_histogram"], dict):
        raise AssertionError("multiplicity histogram absent")
```

For each fault, start with a known durable baseline, keep a finite mixed-signal producer active, then arm the external fault. Gate injection on baseline reader/HEAD evidence and nonzero active input. For slowdown require measured upload/response delay, nonempty FLUSHING plus ACTIVE or pending work, and backpressure/in-flight plateau before recovery. For 503 require an actual PUT/POST 503 log and exporter retry. For store outage stop the container and require failure beyond the configured flush deadline. Full cases use the default 60s deadline and observe at least one exporter storage NACK; a timer alone does not establish this. While faulted, sample resource limits and producer/buffer behavior. Recover only once the intended condition has been observed, then restart/retry/drain under a fixed total deadline.

Strict mode retries the original serialized request bytes and distinguishes server retryable NACK from producer-local timeout. Buffered mode stops resending any request after its durable producer ACK, retains disk across all actions, and waits for exporter NACK -> buffer retry evidence. Require ACTIVE and FLUSHING each within B, cache within C, pending slot at most one, oldest unacked age returning to baseline after recovery, RSS reconciled with Task 6's measured terms and compared under the Controller baseline policy and no buffer expiry/eviction. Report upload retries separately from replay duplicates. Recovery deadline is 300s after endpoint health, raised only by a recorded bound from backlog bytes/minimum measured drain rate before the case begins.

- [ ] **Step 4: Run all cells, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family s3 --output-dir /tmp/series-failure-s3
```

**Recorded numbers:** per cell actual fault duration/status count/bandwidth/latency, requests and supported IDs offered/ACKed/stored, retries/NACKs/timeouts, duplicate histogram, recovery/drain seconds, throughput before/during/after, RSS/accounted/buffer disk peaks and coverage violations. **Failure:** activation not proven; a supported record lost; descriptor/reader mismatch; no retry/backpressure when required; violated retained-state/capacity invariant or Controller baseline policy; recovery deadline; or unexplained duplicates outside the replayed ID set. No exactly-once assertion is made for ambiguous requests.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/failure-s3.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/failure-s3.json
git commit -m "chore: measure series recovery from real S3 faults" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

**Amendment (compaction contract, 2026-09-23):** every S3 fault run in this task also checks the partition lateness bound: no object may become visible in partition hour H later than L = window.interval + 2 * (flush_retry_deadline + upload.abort_timeout) after the end of H. Record, per run, the latest visibility time of any object in each hour relative to that hour's end (HEAD/LIST timestamps from the store, not the writer's clock), and report every violation as a finding for Task 12.


**Amendment (S3 compatibility note, 2026-09-23):** (a) one case per real store (MinIO, RustFS) writes a file above `upload.part_bytes` and asserts from the server trace that CreateMultipartUpload, UploadPart and CompleteMultipartUpload ran, so the multipart path is exercised on a real store and not only on the fault store; (b) after every S3 fault case and every hard-kill case of Task 10, list the bucket's incomplete multipart uploads and compare with the expected count (zero, or the uploads the scenario is known to orphan); an unexpected orphan is a Task 12 finding.

