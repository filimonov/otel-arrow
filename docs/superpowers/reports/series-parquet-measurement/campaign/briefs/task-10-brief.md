### Task 10: Graceful process restart and ungraceful hard kill

**Expected wall-clock cost:** 8-15 minutes for both stores/topologies and kill phases; fast controller tests under one second.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-process.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `failure_case`, `fault_check`, Engine restart options, ledger and retained destination/buffer paths.
- Adds family `process` cases `graceful_restart`, `kill_active`, `kill_upload`; `restart_engine(previous: Engine, *, retain_buffer: bool) -> Engine` records old/new PID, core IDs and boot IDs.
- Process liveness is checked with `poll`/`wait` and the admin endpoint under deadlines; no sleeping to guess that the engine has stopped.

- [ ] **Step 1: Write failing kill/restart tests**

```python
class ProcessFailureTests(MeasurementTestCase):
    # Scenario: SIGKILL interrupts a real upload with acknowledged history on disk.
    # Guarantees: restart and topology-appropriate replay preserve every supported ID.
    def test_kill_during_upload(self):
        require_long()
        for topology in ("strict", "buffered"):
            for store in ("minio", "rustfs"):
                with self.subTest(topology=topology, store=store):
                    result = failure_case("process", "kill_upload", topology,
                                          store, self.output_dir)
                    self.assertNotEqual(result["metrics"]["old_pid"],
                                        result["metrics"]["new_pid"])
                    self.assertTrue(result["checks"]["new_boot_id"])
                    fault_check(result)
```

- [ ] **Step 2: Run the named case red**

```bash
SERIES_MEASURE_LONG=1 python3 -m unittest crates.validation.tests.series_parquet.test_failures.ProcessFailureTests -v
```

Expected: unregistered process family, after successful preflight; do not accept a skipped test as red/green proof.

- [ ] **Step 3: Implement exact lifecycle boundaries**

```python
def kill_engine(engine):
    pid = engine.launcher.pid(engine.process) if engine.launcher else engine.process.pid
    os.kill(pid, signal.SIGKILL)
    wait_until(engine.process.poll, lambda code: code is not None,
               deadline_ns=time.monotonic_ns() + 10_000_000_000,
               description="killed engine process exits")
```

For a container Engine use `docker kill --signal KILL` against its recorded container ID and `docker inspect` exit state; the launcher owns the process handle and real PID mapping. Do not send SIGKILL to the docker client wrapper. This is the only launcher-specific branch.

`graceful_restart`: start pending work, request admin shutdown with the documented drain deadline, assert successful exit and prior ACK coverage, then launch against the same destination. Buffered restart reuses the same buffer path and core IDs. Continue with new IDs and verify both generations and distinct boot metadata.

`kill_active`: gate on nonzero ACTIVE bytes, zero FLUSHING for the selected new cohort, and no completed values containing that cohort. In strict mode these records may not be ACKed yet; retain and retry them after reconnect. In buffered mode gate on durable producer ACKs first, then kill. Task 13 provides the stricter no-resend proof. If a window rotated before the gate, discard the setup attempt without claiming a fault hit and retry within a bounded setup deadline; repeated inability to hit the boundary fails the test.

`kill_upload`: set real upstream bandwidth limit, use enough incompressible seeded payload to force multipart upload, and observe a store multipart upload plus positive transferred bytes and nonzero FLUSHING. Kill the engine before completion; remove the toxic and restart. Also include a small single-PUT interrupted request so success is not specific to multipart. Record incomplete upload IDs separately from completed object files; incomplete multipart leftovers are not acknowledged-record loss or successful output. Cleanup is test-owned after evidence collection.

On every restart verify identical source bytes on retry, new writer boot UUID, valid descriptor coverage within each boot/partition, no loss in already ACKed historical IDs, and no permanent rejection. Compute exact pre/post multiplicity by ID, listing changes only for cohorts eligible for replay. The stable ledger survives the killed engine and is not recreated from the output being checked.

- [ ] **Step 4: Run green, record and commit**

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family process --output-dir /tmp/series-failure-process
```

**Recorded numbers:** time to exit/restart/readiness/drain, old/new PIDs and boot IDs, known ACKed/pending cohort sizes, replayed IDs, exact duplicate histogram, missing/coverage counts, incomplete multipart count and peak memory/disk. **Failure:** lifecycle gate missed, wrong retained path/core allocation, unacknowledged strict requests abandoned, buffered ACKed IDs absent, false complete-file accounting, descriptor/reader disagreement or deadline overrun.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/failure-process.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py docs/superpowers/reports/series-parquet-measurement/failure-process.json
git commit -m "chore: measure series restart and hard-kill recovery" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

