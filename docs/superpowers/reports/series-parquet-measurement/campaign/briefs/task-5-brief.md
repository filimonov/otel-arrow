### Task 5: Maximum sustainable throughput and durable write speed

**Expected wall-clock cost:** 90-180 minutes for capacity search and three repetitions across stores/core counts; under one second for arithmetic tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/capacity-local.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/capacity-minio.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `run_case`, `RunSpec`, `Ledger`, `read_oracle`, strict telemetry and `run_stages` output.
- Produces `capacity_search(spec: RunSpec, output_dir: Path) -> dict` and `rates(unique_records: int, wire_bytes: int, object_bytes: int, seconds: float, workers: int) -> dict`.
- Each capacity result index references individual trial/repetition JSON files; its winning rate is the median sustainable unique-record rate from three independently drained repetitions, with a failed higher offered-rate bracket.

- [ ] **Step 1: Add a failing rate-units test**

```python
# Scenario: two workers complete 12,000 unique records and 3MB of objects in 3s.
# Guarantees: records, input bytes and output bytes retain distinct denominators.
def test_capacity_units(self):
    measured = rates(12000, 12000000, 3000000, 3.0, 2)
    self.assertEqual(measured["records_per_s"], 4000)
    self.assertEqual(measured["records_per_s_per_core"], 2000)
    self.assertEqual(measured["input_bytes_per_s"], 4000000)
    self.assertEqual(measured["object_bytes_per_s"], 1000000)
```

- [ ] **Step 2: Run red**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing `rates`, with earlier contracts still green.

- [ ] **Step 3: Implement rate arithmetic and a bounded search**

```python
def rates(unique_records, wire_bytes, object_bytes, seconds, workers):
    if seconds <= 0 or workers <= 0:
        raise ValueError("positive duration and worker count required")
    return {
        "records_per_s": unique_records / seconds,
        "records_per_s_per_core": unique_records / seconds / workers,
        "input_bytes_per_s": wire_bytes / seconds,
        "object_bytes_per_s": object_bytes / seconds,
    }
```

Calibrate sender capacity against OTLP-to-noop first, on separate cores, and record generation CPU, RPC concurrency saturation and queue depth. Require sender/noop headroom at least 1.5 times the claimed exporter rate. Maintain open-loop offered-rate scheduling against monotonic target timestamps with bounded in-flight requests; record delayed sends rather than silently redefining offered rate as achieved rate. If Python is the bottleneck, split deterministic request-index ranges across spawned Python processes on reserved producer cores; maintain separate ledgers and merge them by disjoint record IDs. Do not increase exporter worker count as a producer workaround.

Start at 1,000 records/s. Double the offered rate until the stability rule fails; cap at 12 trials and mark an unbracketed result as a lower bound, not a maximum. Bisect sustainable/unsustainable rates until the bracket width is at most 10%. Each trial has 15s warm-up, 60s measurement and bounded drain; three winning-rate repetitions repeat the full trial. The duration is a measurement interval, not a readiness sleep: start only after first complete flush and required telemetry are visible. Stop generation on its monotonic phase deadline, then prove drain.

Use 1s measurement windows initially so producer concurrency cannot cap the low-cardinality case. Record this override prominently and add a default-15s confirmation at the winning rate. Let early byte/request rotations occur and record their reasons. A default-window run with input concurrency as the limiting factor is reported as a topology limit, not as the encoding ceiling. Check unique durably stored rate by completed-object interval counts plus final ledger reconciliation. In buffered mode producer ACK rate is not object drain rate; use strict topology for the ceiling and add a buffered run at 80% of that rate to expose WAL cost.

Matrix: local, MinIO and RustFS; one and four explicitly allocated physical cores; logs-only and 80/20 logs/metric-point mixed workloads; 1KiB bodies and a documented 8KiB variant; 10k hot series and cardinality churn; production ZSTD. Use the same seed and record counts for comparable trials. Limit the primary exhaustive search to mixed/1KiB/hot-series; run the other workload rows as fixed-rate confirmations at 80% of its capacity and bracket separately if they fail. These confirmations cannot be labelled their own maxima. Diagnostic uncompressed numbers come only from Task 3.

Producer fan-in is a required dimension of this task, because the deployment target is dozens to hundreds of senders, not a handful. Repeat the winning rate with 1, 8, 64 and 256 concurrent OTLP client connections at the same offered rate and the same record count, spread across the reserved producer cores. Record, per producer count: accepted records/s, the number of connections each worker terminates, the spread of that distribution across workers, engine CPU per worker, exporter RSS, series-cache occupancy and descriptor duplication across workers, plus producer p50/p95/p99 ACK latency. A connection distribution that leaves any worker idle while another saturates is reported as a fan-in limit with its SO_REUSEPORT hashing evidence, not as an encoding ceiling.

Fifth review (2026-09-23): (a) high-cardinality metrics: measure the ratio of new series to points for a workload where one point attribute is unique per point (for example request.id) and for the healthy workload, report descriptor bytes versus values bytes, and add an exporter signal for excessive growth (a series/points ratio gauge per window and a rate-limited WARN event above a configurable threshold); document the strategy in the README: filter or aggregate such attributes upstream (OTel Views), because series identity is the full attribute set by OTel semantics and cannot drop a varying attribute without merging distinct streams. (b) Worst-case runtime stall: measure the longest single uninterrupted stretch of the worker thread (merge key building, chunk encoding) on the largest block, and the reaction time to cancel and shutdown during a flush, not only throughput.

Memory at scale (user note 2026-09-23): Task 6 validated the memory model at small blocks (tens of MB) against a 2 x 500 MiB budget and the user accepted it without a separate large-block family. Task 5 and Task 7 therefore record, at no extra run cost, peak RSS, memory.accounted and the Task 6 residual split at their highest block fill, and report the ratio of accounted to budget reached; a residual outside tolerance at high fill is a finding for Task 12, not a reason to rerun Task 6.

From Task 6 (2026-09-23): measure the upload burst with `upload.concurrency` 1 versus 2 at the winning rate (peak RSS and completed-object latency), which Task 6 did not cover.

Umbrella finding 10: run one launcher case with `max_concurrent_requests` raised well above the shipped 128 (for example 4096) and report it beside the shipped-config run, so a receiver admission ceiling is separated from the exporter ceiling; report the admission-closed gauge from Task 3c item I in both.

The campaign's headline acceptance number is 100,000 to 1,000,000 records/s sustained, where a record is one metric point or one compact log line. Report the measured sustainable rate, the number of physical cores required to reach 1,000,000 records/s, and, when that rate is not reachable on this host, the measured ceiling with the limiting stage named from the Task 3 and Task 4 attribution. Neither a shortfall nor an extrapolation is a failure of this task; an unreported one is.

- [ ] **Step 4: Record completed bytes and per-core costs without double counting**

Maintain separate denominators for offered wire bytes, accepted supported records, unique stored values, physical stored rows including duplicates, and completed Parquet object bytes. Local byte totals use completed files; S3 uses HEAD content lengths, cross-checked against downloads. Exclude incomplete multipart parts from successful write speed and report their bytes separately where the store exposes them. Report both steady-state interval write speed and `total completed bytes / time from first send to final completion`, so tail drain cannot disappear from the throughput claim.

Record CPU seconds for engine, producer and store; total engine CPU/core occupancy; output/input compression ratio; average file size; objects/s; number of descriptor duplicates across workers; and producer p50/p95/p99 ACK latency. Join Task 4 stage shares by identical workload/config/core allocation and binary provenance, retaining the full environment fingerprint and profile mode, never by a convenient nearby run. The shared-writer discussion consumes measured per-worker descriptor overhead, scaling efficiency and stage saturation; this task implements no shared writer.

- [ ] **Step 5: Verify, record numbers and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure capacity --output-dir /tmp/series-capacity
```

**Failure:** common correctness/validity checks; claimed maximum without an unsustainable bracket; producer bottleneck; increasing backlog at the accepted rate; permanent rejection; missing byte/compression/core metadata; unstable repetitions; nonpositive throughput. Apply the Controller baseline policy in Global Constraints to performance and memory; preserve failed measurements and route defects to the contingency task.

**Recorded numbers:** three per-store JSONs contain every trial, sustainable/unsustainable bounds, records/s/core, total records/s, wire and Parquet bytes/s, CPU shares, scaling efficiency, compression, ACK percentiles, drain duration and oracle counts. A failed trial used to bracket capacity is marked `unsustainable` inside a valid search; semantic loss is always a failed run, never a useful bracket.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/capacity-local.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/capacity-minio.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/capacity-local.json docs/superpowers/reports/series-parquet-measurement/capacity-minio.json docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json
git commit -m "chore: establish series parquet throughput and write rates" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```


**Amendment (umbrella review 2026-09-23):** measure the synchronous stretch on the admission path (run sort in `append` -> `seal` up to `run_target_bytes`, and `rotate()`'s `recount()`) with the flush-stall probe method, at the largest run size of each shape, and record it beside the flush-side figures of Task 3i. A stretch above the flush side's worst figure is a Task 12 finding.

