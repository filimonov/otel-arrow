### Task 4: Direct performance attribution and stage reconciliation

**Expected wall-clock cost:** 10-20 minutes for profiled pipeline repetitions; under one second for classifier tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/attribution.json`

**Interfaces:**
- Consumes Task 3's registered stages and full metric schema, Task 2's lease/launcher and Task 1's result/ledger/oracle contracts.
- `classify_cpu(samples: list[dict]) -> dict[str, int]` assigns each weighted perf sample exactly once to conversion, extraction, sort/seal/merge, encoding, upload, buffer, engine/runtime, allocator or unknown, using the innermost matching production frame. Upload wait is separate wall duration.
- `run_attribution(spec: RunSpec, output_dir: Path) -> dict` implements `measure attribution`, writing independent repeated run files and `attribution.json`; link stage evidence by case/workload/config and retain full per-profile fingerprints.

- [ ] **Step 1: Add a failing stage-reconciliation test**

```python
# Scenario: extraction, encoding and upload have distinct CPU stacks and durations.
# Guarantees: overlapping async wall intervals cannot inflate CPU percentages.
def test_cpu_attribution_is_exclusive(self):
    samples = [
        {"frames": ["Worker::extract", "extract::logs::extract"], "weight": 7},
        {"frames": ["Sink::write_block", "parquet::arrow::arrow_writer"], "weight": 5},
        {"frames": ["Sink::write_block", "object_store::client::http"], "weight": 2},
    ]
    cpu = classify_cpu(samples)
    self.assertEqual(cpu["extraction"], 7)
    self.assertEqual(cpu["encoding"], 5)
    self.assertEqual(cpu["upload"], 2)
    self.assertEqual(sum(cpu.values()), 14)
```

- [ ] **Step 2: Run red, then implement exclusive classification**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: absent classifier. Walk frames from innermost to outermost and use the first matching production namespace; encoder frames take precedence over ancestor Sink frames. Add sample weights once, leave unmatched samples in `unknown`, and assert classified plus unknown equals the input weight. Reject zero/negative weights and retain the mapping rules with the result.

- [ ] **Step 3: Measure pipeline CPU shares directly**

Run `perf record -F 199 -g --call-graph dwarf -p ENGINE_PID -o perf.data` while Task 2 drives the actual engine. Resolve the PID from Engine, place perf output in that run's artifacts, and stop perf after the observable input phase ends. Classify `perf script` samples by the explicit stages above, with encoding taking precedence over ancestor sink/upload frames. Report per-core CPU seconds, CPU ns/record, sample count/confidence, extraction/encoding/upload CPU percentages, scheduler/off-CPU wall fraction and named residual. Require at least 10,000 classified samples across repeated runs; lengthen the opt-in profile if needed. If perf permissions prevent profiling, preflight skips the attribution run; mandatory acceptance remains incomplete until it is run on a permitted host. Do not infer CPU shares by subtracting two end-to-end throughput numbers or normalize overlapping upload waits into a CPU pie chart.

Report per stage records/s/core, CPU ns/record, allocated bytes/record, peak RSS, peak live heap/workspace and output bytes/input-record. Task 3 supplies full per-stage timing/allocation/RSS/output metrics; attribution supplies exclusive CPU shares and wait time. Join the evidence without pretending different output representations share a byte-rate denominator.

- [ ] **Step 4: Verify, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure attribution --output-dir /tmp/series-attribution
```

**Recorded numbers:** exclusive sample counts/shares, CPU/core seconds, CPU ns/record, upload wait, unknown/reconciliation residual, profile overhead and three repetitions in `attribution.json`. **Failure:** overlap/double counting, fewer than 10,000 classified samples, unavailable mandatory perf data or invalid reconciliation. Apply the Controller baseline policy to numerical performance; route defects to Task 12.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/attribution.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py docs/superpowers/reports/series-parquet-measurement/attribution.json
git commit -m "chore: attribute series parquet pipeline CPU costs" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

