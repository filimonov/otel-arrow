### Task 6: Validate retained memory, transients and the RSS residual

**Amendment (umbrella review, 2026-09-22):** two accounting gaps found by static review are the first hypotheses for the unexplained residual (38.8 MB against a 33.5 MB tolerance in the Task 3 sink family, 90-95 percent of tolerance in Task 2): merge keys for the whole block are resident but never charged (`sort.rs:225-237`, about 20-30 percent of block bytes at peak), and values builders keep 1024-row capacity per request (`extract/mod.rs:355-380`, about 110 KB accounted per small request). Task 6 measures both terms explicitly before decomposing the rest, and a confirmed gap is a bounded Task 12 fix (charge the term), not a tolerance change. Task 6 also states the block-pair uncertainty of the in-process residual (umbrella finding 8). Related perf hypothesis for Tasks 4/5: series rows form one sorted run per request, so churn yields up to 4096 runs for the merge (`buffer.rs:165-199`).

**Expected wall-clock cost:** 75-135 minutes for at least three independent paired release runs per topology/configuration plus separate profiles; under one second for accounting tests.

**Files:**
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs`
- Modify: `rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/memory-strict.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/memory-buffered.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `sample_engine`, staged heap profiles, `RunSpec`, `read_oracle` and the existing engine `dhat-heap` profiling feature; no new runtime metric or introspection endpoint.
- Produces `memory_experiment(spec: RunSpec, output_dir: Path) -> dict` and `residual(rss: int, accounted: int, runtime: int, buffer: int, workspace: int, allocator: int) -> int`.
- Memory output includes timestamped `rss_bytes`, `accounted_bytes`, `budget_bytes`, all named component estimates with measurement/bound provenance, and signed unexplained bytes. Disjoint categories and overlap flags prevent subtracting the same allocation twice.

- [ ] **Step 1: Add a failing signed-residual test**

```python
# Scenario: a double-counted workspace estimate exceeds observed process RSS.
# Guarantees: reconciliation retains a negative residual instead of clamping it away.
def test_residual_preserves_negative_discrepancy(self):
    self.assertEqual(residual(100, 60, 15, 10, 20, 5), -10)
```

- [ ] **Step 2: Run red**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing memory module or residual function.

- [ ] **Step 3: Implement the source-derived ledger and capture paired observations**

```python
def residual(rss, accounted, runtime, buffer, workspace, allocator):
    return rss - accounted - runtime - buffer - workspace - allocator
```

Transcribe the current worker accounting into the report, without claiming it measures every allocation:

```text
retained reservation = 2*B + E + 128*C + 2*N*T
workspace reservation = 2*R + 2*M + 3*W + P*(U+1) + M + 4*I + 64MiB
current accounted = active + flushing_or_cleaning + pending_extracted_and_descriptors
                    + 128*cache_entries + notify_token_bytes + spare_token_capacity
```

Here `B` is maximum block bytes, `E` maximum extracted request bytes, `C` cache capacity, `N` requests/block, `T` observed retained token high-water, `R` run target, `M` merge target, `W` writer limit, `P` upload part bytes, `U` upload concurrency, and `I` maximum wire request bytes. Flushing and cleanup are one slot, not two blocks. ACTIVE/FLUSHING already charge their retained tokens; do not add those again. Pending descriptors, queued/in-flight notification tokens and spare vector capacity must not vanish from the accounted comparison.

Sample normal release engine RSS/smaps and gauges at 100ms and telemetry at 100ms during targeted short memory runs; use 1s for soak. Sampling misses shorter allocation peaks, so use synchronous heap measurements and conservative overlap bounds below as well. Start with receiver/noop idle baseline, strict idle baseline, buffered idle baseline, and equal-workload strict/buffered runs on the same cores. Measure data pages, mappings/stacks, allocator retained pages, engine receiver queues, channels and gRPC buffers. A paired RSS difference is an estimate with an interval, not an exact allocation label. `memory_experiment` owns at least three independent pairs per topology and exact configuration: each pair launches fresh noop/control and measured release processes with matching cores/workload, warmed separately. Profiled runs supplement these pairs and never substitute for release repetitions. For every idle/load/seal/merge/encode/upload/recovery phase require at least 30 fresh telemetry epochs and 30 paired RSS observations; repeat a short transient until sample count is met and capture allocation peaks synchronously. A missed transient or phase is invalid, not zero.

Report each pair, per-phase median and min/max range (or confidence interval), and aggregate median/range across independent pairs. Reject a baseline if any primary positive memory metric has `(max - min) / median > 0.15`, if a paired signed difference crosses zero outside its recorded measurement uncertainty, or if samples/phase coverage/environment matching are incomplete. Investigate and rerun unstable candidates; retain all candidates and never commit an unstable one as the baseline. The Controller baseline policy applies only after this validity test.

- [ ] **Step 4: Exercise and account for each transient explicitly**

| Transient | Experiment and observation | Attribution and comparison |
| --- | --- | --- |
| Conversion | Requests near 16MiB with wide strings, nested attributes and histogram arrays; measure wire overlap, converted pinned Arrow bytes and DHAT peak under the conversion stage | Compare measured workspace to `4*I`; report actual peak/wire expansion separately. Record the reservation discrepancy; apply the Controller baseline policy to the measured peak. |
| Admission/final values sealing | Build several values runs plus unfinished building batches; retain pre-seal snapshots, seal, record old/new uniquely pinned buffers and allocation peak | Compare measured extra retained/copy space with revision-7 `(V+1)*R`, including run overshoot and series timestamp buffers. Record `V`, actual largest run and the resulting conservative bound; explain any discrepancy without turning this engineering estimate into a first-run ceiling. A stamp-only series test is insufficient. |
| Resident merge keys | Narrow default keys, then wide custom body/attribute keys across the whole block; allocate/consume actual MergeIter in the isolated profile | Attribute Arrow RowConverter/key_rows allocations and heap OwnedRows; compare narrow/wide results to a second-payload-scale bound derived from actual row lengths plus row offsets/heap capacity. Keys remain live until iterator drop, not just one output chunk. |
| Merge output | Skew widths so a chunk groups wide rows after many narrow ones; record pinned bytes of every yielded chunk | Report maximum/target ratio, `rows_per_chunk * max_row_bytes` conservative payload bound and Arrow buffer overhead. Unexplained allocation/RSS residual remains a hard failure; do not assert `actual <= M` when source uses average width. |
| Encoding | High-cardinality strings/dictionaries, null-heavy histograms and near-row-group limits; record ArrowWriter memory before/after write/flush/close and peak stage heap | Separate input chunk, output Vec capacity and Parquet workspace; compare workspace to `3*W`. Check final flush/close peak, not only steady writes. |
| Upload | Pre-encoded payload above three part sizes through real S3 at concurrency 1 and 2, plus actual Sink row-group flush; record allocation profile and in-flight part count | Compare live part buffers to `P*(U+1)` and separately measure current encoded chunk and HTTP/TLS client buffers. A whole supplied row group can launch a burst above nominal concurrency; measure and bound that burst by actual encoded row-group bytes, then report how the reservation compares with observations; never assume it is a measured hard concurrency cap. |
| Buffer | Same rate/record set with pass-through WAL, then outstanding backlog and replay; sample disk allocation plus process profile | Attribute Quiver/WAL/bundle stacks, file mappings and caches; report buffer heap/mapped RSS interval and physical/logical disk bytes separately from exporter. No expiry/drop-oldest loss or capacity overrun. |

For final values sealing, `V` is the number of buffered values runs in the spec bound; record both run count and dataset count to prevent interpreting it as signal count. Deduplicate shared Arrow allocations across snapshots with `CountedAllocations`. Distinguish the maximum of individually isolated transients from their possible overlap in the real sink. Include concurrently admitted ACTIVE work while FLUSHING encodes/uploads; two isolated peak measurements cannot prove that their sum never overlaps.

Use the existing engine `dhat-heap` feature in a separate profiling binary with default allocator features disabled. Reproduce the release pipeline's functional features explicitly (`series_parquet,aws,durable-buffer`, one crypto provider); run profiling with its working directory set to the run artifact directory so `dhat-heap.json` is isolated. Record DHAT's allocator change and slowdown. For normal-release allocator retention use `/proc/PID/smaps_rollup` plus the selected allocator's available statistics or a Valgrind Massif confirmation (`--pages-as-heap=yes` for page residency attribution, a separate ordinary heap run for stacks). Tool output must substantiate any large allocator/runtime term; do not assign the unexplained residual to "allocator" by definition.

If observed wide-key/sealing behavior exceeds an engineering reservation, record the counterexample and explain the measured allocations. Apply the Controller baseline policy and the hard residual check, then route any defect to the contingency task for bounded remediation or an evidenced architectural plan 4 decision. Do not relabel an engineering reservation as a measured ceiling.

- [ ] **Step 5: Verify, record numbers and commit**

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure memory --output-dir /tmp/series-memory
```

**Recorded numbers:** RSS/accounted/budget curves, conversion/seal/keys/chunk/encoder/upload peaks and ratios, concurrent peak envelope, cache/token terms, exporter attribution, buffer heap/mapped RSS interval and disk usage, runtime/allocator categories, residual magnitude/uncertainty and profile overhead. **Failure:** any common hard gate, missing named transient/sample count, unstable paired baseline, unexplained residual above the declared tolerance, per-worker retained-block overrun, buffer loss, or regression under the Controller baseline policy. An engineering `budget + idle RSS + 128MiB` comparison alone cannot pass this task.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/memory-strict.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/memory-buffered.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/memory-strict.json docs/superpowers/reports/series-parquet-measurement/memory-buffered.json
git commit -m "chore: validate series parquet memory accounting against RSS" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

