### Task 3: Layered Criterion and complementary benchmark mechanics

**Expected wall-clock cost:** 15-30 minutes for the full stage matrix; 15-30 seconds for reduced contract tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/series-lake/Cargo.toml`
- Modify if resolution changes: `rust/otap-dataflow/Cargo.lock`
- Create: `rust/otap-dataflow/crates/series-lake/benches/layered.rs`
- Create: `rust/otap-dataflow/crates/series-lake/benches/measurement.rs`
- Create: `rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs`
- Create/Test: `rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/stages.json`

**Interfaces:**
- Consumes `build_request`, `RunSpec`, `write_result`, `DockerStore`, and actual `extract`, `Block`, `SeriesCache`, `sort_batch`, `merge_runs`, `Sink` APIs in the ground-truth table.
- Produces bench CLI `measurement --stage NAME --input PATH --config PATH --output PATH --iterations N --profile timing|heap --compression zstd|none`; input is deterministic length-prefixed OTLP requests (u32 little-endian length, one signal per file), with sidecar workload JSON.
- `run_stages(spec: RunSpec, output_dir: Path) -> dict` invokes one new process per stage/repetition/profile, aggregates into `stages.json`, and exports exact stage names `otlp_noop`, `otlp_convert`, `otlp_extract_hash`, `otlp_sort`, `otlp_parquet_local`, `otlp_parquet_zstd`, `otlp_minio`, `convert`, `extract`, `sort_seal`, `merge`, `encode`, `local_write`, `upload`, and `sink`.

- [ ] **Step 1: Add failing stage-schema and equivalence tests**

```python
# Scenario: the registered OTLP-to-noop stage omits allocation data.
# Guarantees: every stage, including the pipeline baseline, has the full metric schema.
def test_otlp_noop_requires_all_metrics(self):
    with self.assertRaisesRegex(AssertionError, "allocated_bytes_per_record"):
        validate_stage_result({"stage": "otlp_noop", "metrics": {}})
```

`validate_stage_result(result: dict) -> None` checks the mandatory stage fields listed below in fixed order beginning with `allocated_bytes_per_record`, requiring numeric finite values and positive sample counts. It is owned by `performance.py`; unmeasured mandatory fields invalidate the run.

Place this test in the existing `test_measurement.py`. Since the bench has `harness = false`, its Rust checks are explicitly invoked by `--self-test`; do not put unreachable `#[test]` functions behind a custom harness and assume cargo runs them. Declare `mod stages; mod tests;` with explicit `#[path = "measurement/stages.rs"]` and `#[path = "measurement/tests.rs"]` in the bench root. The `--self-test` branch invokes `tests::run() -> Result<(), Box<dyn std::error::Error>>`, then exits without a measurement. Include self-test calls for diagnostic encoding versus actual Sink, exact stage row counts and input generation excluded from timing, with Scenario/Guarantees comments; use the public pdata fixture constructors and independent readers already used by library sink tests. Each is called by `tests::run`; any failed assertion/Result exits nonzero. Conversion/encoder peaks are measured and evaluated through the shared Controller baseline policy; do not add a `check_expansion` helper that rejects engineering reservations as hard ceilings.

- [ ] **Step 2: Run red before adding the bench**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
cargo bench -p otel-arrow-dfe-series-lake --bench measurement --no-run
```

Expected: missing stage validator and bench targets. Cargo build/self-test commands here are setup verification outside all measurement leases; only the prebuilt executable runs launched by `measure stages` produce publishable timing evidence. Cargo is executed only by the future implementer.

- [ ] **Step 3: Add the purpose-built target and timing primitive**

Add workspace dev dependencies `criterion`, `cpu-time`, `dhat`, and `bytes` only where used; enable object_store's `aws` feature for the bench through its dev dependency. Declare:

```toml
[[bench]]
name = "measurement"
harness = false

[[bench]]
name = "layered"
harness = false
```

The entry point is `fn main() -> Result<(), Box<dyn std::error::Error>>`; it parses the documented flags, constructs a current-thread Tokio runtime and writes JSON through `serde_json::to_writer_pretty`. Do not run the whole asynchronous upload future inside an extraction timer. Use this synchronous primitive for CPU stages:

```rust
#[derive(serde::Serialize)]
struct Timing {
    wall_ns: u128,
    cpu_ns: u128,
}

fn timed<T, E>(run: impl FnOnce() -> Result<T, E>) -> Result<(T, Timing), E> {
    let wall = std::time::Instant::now();
    let cpu = cpu_time::ThreadTime::now();
    let value = run()?;
    let timing = Timing {
        wall_ns: wall.elapsed().as_nanos(),
        cpu_ns: cpu.elapsed().as_nanos(),
    };
    Ok((value, timing))
}
```

For the single-thread async stage measure process CPU before/after `runtime.block_on` with `cpu_time::ProcessTime`, and wall time separately; there are no concurrent benchmark stages. Hold the host lease around directly invoked Criterion/diagnostic subprocesses as well as engine runs; `run_stages` launches prebuilt bench executables, never cargo during measured intervals. Run fixture construction, input cloning, filesystem setup, warm-up and output verification outside the timed region. Use black_box on retained inputs/outputs, not a discarded future. Each timing process executes at least 30 samples and one second of accumulated measured work; cap iterations by a 60s deadline and mark incomplete if the minimum is not met.

For allocation mode use the safe `dhat::Alloc` global allocator in this bench executable only and one DHAT profiler per process. Start it after fixtures are created; record `HeapStats` before/after and maximum live allocation during the stage, then drop the stage output under the profiler. Profile fixture-retained bytes separately to distinguish input from workspace. DHAT instrumentation results never supply throughput numbers. No unsafe allocator wrapper or global profiler is added to production library code.

- [ ] **Step 4: Implement and validate every stage against the production path**

| Stage | Timed operation | Untimed input and correctness check |
| --- | --- | --- |
| `otlp_noop` | Same Python sender into real engine OTLP receiver plus existing noop exporter, with acknowledgements enabled | Calibrate producer/network capacity and process CPU; this is a pipeline baseline, not a library stage or durable-storage claim. |
| `convert` | Construct `OtapPayload` from serialized `OtlpProtoBytes`, then `try_into_with_default::<OtapArrowRecords>` using the production trait call | Prepared wire bytes; count resulting rows and pinned Arrow buffers with `CountedAllocations`. Include conversion lifetime overlap with input bytes. |
| `extract` | `extract(&mut records, &cfg)` including identity canonicalization/hash | Fresh converted input; verify descriptors, IDs, row types and semantic hashes. |
| `sort_seal` | `reserve`, `admit` through bounded runs, then `seal(fixed_microseconds)` | Pre-extracted batches, cache with specified hit/miss distribution; record admission and final seal timings separately as nested non-overlapping sub-stages. |
| `merge` | Construct and fully consume `merge_runs` | Prepared sorted runs; verify merged order and exact row multiset, retain at most one output chunk. |
| `encode` | ArrowWriter encoding of prepared merged chunks, first uncompressed then ZSTD | Identical chunk stream. Match production dictionary/statistics/row-group settings and memory-triggered flush predicate. Record encoded bytes and writer memory maxima; record output sink capacity separately and exclude it only from a contemporaneous workspace measurement. |
| `local_write` | Write pre-encoded bytes through actual LocalFileSystem object_store | New object path; verify byte hash after completion. Also report actual Sink-to-local end-to-end cost; state object_store completion semantics, without claiming host power-loss durability. |
| `upload` | Pre-encoded bytes through actual S3 object_store `BufWriter`, production part size/concurrency, complete multipart or PUT | MinIO/RustFS container from DockerStore. Verify HEAD size and downloaded hash. No extraction or compression is inside this stage. |
| `sink` | Actual `Sink::write_block(&sealed_block, &CancellationToken)` | Cross-check descriptor coverage, values, compression metadata, row-group policy and output lengths against diagnostic pipeline; byte-for-byte equality is unnecessary when file metadata differs. |

Add `encode_chunks(chunks: &[RecordBatch], cfg: &LakeConfig, compression: Compression) -> Result<Vec<u8>>` in stages.rs. Use `parquet::arrow::ArrowWriter`, copy the actual writer-property construction from `sink.rs`, write each chunk, flush when `memory_size() >= writer_limit_bytes` or `in_progress_size() >= row_group_bytes`, then close/return bytes. The equivalence test checks properties against actual Sink output so a later sink change cannot silently stale the diagnostic bench. No alternative encoding implementation or exporter compression knob is introduced.

The encoder's concrete core is below; its `Result` is `std::result::Result<T, Box<dyn std::error::Error>>`. The caller collects the allocation/timing measurements around this operation and checks row-group/compression semantics against Sink outside the timed region:

```rust
fn encode_chunks(
    chunks: &[RecordBatch],
    cfg: &LakeConfig,
    compression: Compression,
) -> Result<Vec<u8>> {
    let first = chunks.first().ok_or_else(|| std::io::Error::other("empty input"))?;
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(None)
        .build();
    let mut encoded = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut encoded, first.schema(), Some(properties))?;
    for chunk in chunks {
        writer.write(chunk)?;
        if writer.memory_size() >= cfg.parquet.writer_limit_bytes
            || writer.in_progress_size() >= cfg.parquet.row_group_bytes
        {
            writer.flush()?;
        }
    }
    let _metadata = writer.close()?;
    Ok(encoded)
}
```

Diagnostic bytes deliberately omit file-identity metadata; actual Sink output supplies the authoritative format checks. For timed runs use an output-capacity warm-up estimate and record Vec capacity/allocation separately; allocation-mode runs must include all growth and finalization. Do not subtract the entire output length from an unrelated heap peak or remove encoder allocations from allocated-bytes/record.

Run default keys and wide custom log body keys, small/large bodies, stable/churning series and mixed histogram widths. Include 1KiB and 8KiB bodies, plus a legal near-row-limit fixture at 512KiB. Reject any fixture that violates the current config constraints; it cannot be used to claim supported throughput.

- [ ] **Step 5: Register the cumulative Criterion layers and full metric contract**

`benches/layered.rs` uses workspace Criterion 0.8 with `harness = false`, a fallible `main`, the stage functions above and no production allocator changes. Register these cumulative groups in order: `otlp_noop` (prepared OTLP bytes consumed by the synchronous noop path), `otlp_convert` (plus conversion), `otlp_extract_hash` (plus extraction/hash), `otlp_sort` (plus admission/seal/merge), `otlp_parquet_local` (plus uncompressed Parquet/local persistence), `otlp_parquet_zstd` (same with ZSTD). The real OTLP network-to-noop pipeline is also registered as `otlp_noop` with `mode: pipeline`; Criterion's synchronous noop floor has `mode: criterion`. Neither mode substitutes for the other. Actual sink/MinIO cumulative `otlp_minio` and isolated upload are complementary async runs in `measurement`; spec 9.6's full layer ladder therefore reaches the real store.

Use `criterion.benchmark_group(stage)`, `sample_size(30)`, `warm_up_time(Duration::from_secs(1))`, `measurement_time(Duration::from_secs(5))` and `Throughput::Elements(record_count)`. `iter_batched` prepares/clones inputs outside timing; the timed closure invokes all cumulative operations for that layer and `black_box`s retained outputs. Keep each Result error in an outer error slot and return it from fallible `main` after the group finishes; never time silently failed operations. Local persistence uses the prepared per-iteration path and synchronous file writes/completion; the complementary actual LocalFileSystem/Sink run validates its output and states completion semantics. Criterion is used for cumulative wall-time distributions, not async wait or allocation attribution. A process per layer/repetition prevents previous stages from contaminating RSS peaks.

All registered stage results, including `otlp_noop`, require `stage`, `mode`, `sample_count`, `repetition`, `fingerprint`, and metrics `allocated_bytes_per_record`, `records_per_s_per_core`, `cpu_ns_per_record`, `peak_rss_bytes`, `output_bytes_per_input_record`, `wall_ns_per_record`, `peak_live_heap_bytes`, `peak_workspace_bytes`; record `output_representation`, CPU/core allocation and config. Noop emits no stored output: report output bytes as measured zero with `output_representation: none`; count successful zero-partial-rejection RPCs and reconcile sent supported counts. Noop has no descriptor/storage oracle, recorded as not applicable; all storage stages require the common oracle. Collect its real engine CPU/RSS and paired heap allocation profile under the same workload/config; profiling has a distinct build/profile fingerprint linked by `workload_config_id`. Missing profiling data is incomplete acceptance, never a fabricated zero.

The full metric schema belongs to each registered composite stage result; its timing, heap and pipeline child runs have explicitly declared profile-specific mandatory fields. A heap child never supplies throughput. Each child still has the common environment/config/workload/provenance and hard validity checks, and references in the composite identify exactly which child measured each field. Completeness rejects a composite with any missing required measurement; differing profile fingerprints are not regression-comparison matches.

`run_stages` combines Criterion's estimates/raw sample artifact hashes with complementary CPU/DHAT/RSS runs using `(stage, mode, workload_config_id, repetition)`. Keep separate complete fingerprints per profile and compare each only within its profile. Require three independent repetitions of each group and 30 samples/at least one second of measured work per timing process; publish medians/ranges. Baseline decisions use the Controller policy in Global Constraints. Route equivalence, validity or measured regression defects to Task 12.

- [ ] **Step 6: Run green, apply failure gates, record and commit**

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
cargo bench -p otel-arrow-dfe-series-lake --bench measurement -- --self-test
cargo bench -p otel-arrow-dfe-series-lake --bench layered --no-run
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure stages --output-dir /tmp/series-stages
```

**Recorded numbers:** Criterion distributions and all registered stage metrics above, conversion/encoder expansion, equivalence counts and three repetition dispersions in `stages.json`. **Failure:** incorrect output, hidden timed work, missing Criterion group/registered stage/mandatory metric, insufficient samples or unstable repetitions, hard residual/validity checks, or regression under the Controller baseline policy. Preserve engineering-reservation comparisons as measurements and route discovered defects to Task 12. Reduced tests run in fast CI; full/profiled measurements are opt-in.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/stages.json)
git add rust/otap-dataflow/crates/series-lake/Cargo.toml rust/otap-dataflow/crates/series-lake/benches/layered.rs rust/otap-dataflow/crates/series-lake/benches/measurement.rs rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs rust/otap-dataflow/Cargo.lock rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py docs/superpowers/reports/series-parquet-measurement/stages.json
git commit -m "chore: measure layered series parquet benchmarks" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

