// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Self-tests of the measurement bench, run by `measurement --self-test`.
//!
//! The bench has no test harness, so `cargo test` never sees these. Each is
//! called explicitly by [`run`], and the first failed check or error ends
//! the process nonzero.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use prost::Message as _;

use super::stages::{
    self, BenchConfig, Clock, Input, Result, Sidecar, Signal, Stage, StageName, hex_digest,
};

/// Window of every fixture block.
const WINDOW_START: i64 = 1_789_960_500;
/// Seal stamp of every fixture block.
const SEAL_AT_US: i64 = 1_789_960_500_000_000;
/// Distinct `logger.name` values in the logs fixture.
const LOGGERS: usize = 7;
/// Distinct `series.slot` values in the metrics fixture.
const SLOTS: usize = 5;
/// Samples the fixture-weight comparison takes of each configuration.
const SAMPLES: usize = 30;

/// A failed expectation, as an error the process exits on.
fn ensure(condition: bool, what: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(what.into().into())
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn resource() -> Option<Resource> {
    Some(Resource {
        attributes: vec![kv("host.id", "producer-1"), kv("service.name", "bench")],
        ..Default::default()
    })
}

/// One logs request of `records` records with distinct bodies.
fn logs_request(request: usize, records: usize, body: usize) -> Bytes {
    let logger = format!("logger.{:03}", request % LOGGERS);
    let data = LogsData {
        resource_logs: vec![ResourceLogs {
            resource: resource(),
            scope_logs: vec![ScopeLogs {
                log_records: (0..records)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_789_960_500_000_000_000 + (request * records + i) as u64,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(format!(
                                "{request:06}:{i:06}:{}",
                                "x".repeat(body)
                            ))),
                        }),
                        attributes: vec![kv("logger.name", &logger)],
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    Bytes::from(data.encode_to_vec())
}

/// One metrics request: a gauge, a histogram with buckets and an empty
/// histogram, `points` points each, all on one `series.slot`.
fn metrics_request(request: usize, points: usize) -> Bytes {
    let slot = format!("{:03}", request % SLOTS);
    let time = |i: usize| 1_789_960_500_000_000_000 + (request * points + i) as u64;
    let gauge = Metric {
        name: "bench.gauge".into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: (0..points)
                .map(|i| NumberDataPoint {
                    time_unix_nano: time(i),
                    attributes: vec![kv("series.slot", &slot)],
                    value: Some(number_data_point::Value::AsInt(i as i64)),
                    ..Default::default()
                })
                .collect(),
        })),
        ..Default::default()
    };
    let histogram = |name: &str, width: usize| Metric {
        name: name.into(),
        data: Some(metric::Data::Histogram(Histogram {
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            data_points: (0..points)
                .map(|i| HistogramDataPoint {
                    time_unix_nano: time(i),
                    attributes: vec![kv("series.slot", &slot)],
                    count: width as u64 + 1,
                    sum: Some(i as f64),
                    bucket_counts: vec![1; width],
                    explicit_bounds: (1..width).map(|b| b as f64).collect(),
                    ..Default::default()
                })
                .collect(),
        })),
        ..Default::default()
    };
    let data = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: resource(),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    gauge,
                    histogram("bench.histogram", 4),
                    histogram("bench.histogram_empty", 0),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    Bytes::from(data.encode_to_vec())
}

/// Write `requests` as a length-prefixed input file with its sidecar.
fn write_input(
    path: &Path,
    signal: Signal,
    requests: &[Bytes],
    records: usize,
    expected_series: usize,
) -> Result<()> {
    let mut data = Vec::new();
    for request in requests {
        data.extend_from_slice(&u32::try_from(request.len())?.to_le_bytes());
        data.extend_from_slice(request);
    }
    std::fs::write(path, &data)?;
    let sidecar = Sidecar {
        format: "u32le-length-prefixed-otlp".into(),
        signal,
        requests: requests.len(),
        records,
        expected_series,
        sha256: hex_digest(&data),
        extra: serde_json::Map::new(),
    };
    std::fs::write(stages::sidecar_path(path), serde_json::to_vec(&sidecar)?)?;
    Ok(())
}

/// The exporter's example lake configuration, with small row groups so
/// that the byte-driven flush predicate cuts several of them.
fn lake_config() -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = vec!["logger.name".into()];
    cfg.parquet.row_group_bytes = 4 << 10;
    cfg.sorting.merge_chunk_bytes = 16 << 10;
    cfg
}

fn bench_config(root: &Path) -> BenchConfig {
    bench_config_with(root, 0)
}

/// The same configuration with `extra` synthetic series marked committed,
/// which makes the per-iteration cache warm-up heavier without changing
/// anything a stage admits.
fn bench_config_with(root: &Path, extra: usize) -> BenchConfig {
    BenchConfig {
        extra_committed_series: extra,
        workload_config_id: "self-test".into(),
        lake: lake_config(),
        storage: Some(serde_json::json!({"file": {"base_uri": root.join("store")}})),
        scratch_dir: root.join("scratch"),
        cache_entries: 1000,
        committed_fraction: 0.5,
        window_start_secs: WINDOW_START,
        seal_at_us: SEAL_AT_US,
    }
}

/// Both fixture inputs, written to disk and read back through the bench's
/// own reader.
fn fixture_inputs(root: &Path) -> Result<Vec<Input>> {
    let logs: Vec<Bytes> = (0..12).map(|r| logs_request(r, 40, 300)).collect();
    let metrics: Vec<Bytes> = (0..10).map(|r| metrics_request(r, 30)).collect();
    let logs_path = root.join("logs.otlp");
    let metrics_path = root.join("metrics.otlp");
    write_input(&logs_path, Signal::Logs, &logs, 12 * 40, LOGGERS)?;
    write_input(
        &metrics_path,
        Signal::Metrics,
        &metrics,
        10 * 30 * 3,
        SLOTS * 3,
    )?;
    Ok(vec![
        stages::read_input(&logs_path)?,
        stages::read_input(&metrics_path)?,
    ])
}

/// A sealed block of one fixture, built through the production path.
fn sealed(input: &Input, cfg: &LakeConfig) -> Result<Block<()>> {
    let mut cache = SeriesCache::new(1000);
    let mut block: Block<()> = Block::new(WINDOW_START, 1, cfg);
    for bytes in &input.requests {
        let wire = match input.sidecar.signal {
            Signal::Logs => OtlpProtoBytes::ExportLogsRequest(bytes.clone()),
            Signal::Metrics => OtlpProtoBytes::ExportMetricsRequest(bytes.clone()),
        };
        let payload: OtapPayload = wire.into();
        let mut records: OtapArrowRecords = payload.try_into_with_default()?;
        let extracted = extract(&mut records, cfg)?;
        let reservation = block.reserve(&extracted, &mut cache, 0, cfg)?;
        block.admit(extracted, reservation, ())?;
    }
    block.seal(SEAL_AT_US)?;
    Ok(block)
}

// Scenario: the diagnostic encoder and the actual Sink encode the same
// sealed logs and metrics blocks, with row groups small enough that the
// byte-driven flush predicate cuts several of them.
// Guarantees: decoded schema and values, row-group boundaries, per-column
// compression, statistics and dictionary use agree with real Sink output,
// so a sink change cannot silently leave the diagnostic encoder stale.
fn diagnostic_encoding_matches_sink(root: &Path) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let cfg = lake_config();
    for input in fixture_inputs(root)? {
        let block = sealed(&input, &cfg)?;
        let report = stages::sink_equivalence(&block, &cfg, &runtime, root)?;
        ensure(
            report.mismatches.is_empty(),
            format!(
                "diagnostic encoding differs from the sink: {:?}",
                report.mismatches
            ),
        )?;
        ensure(
            report.files_compared == 2,
            format!("{} files compared", report.files_compared),
        )?;
        ensure(
            report.row_groups_compared > report.files_compared,
            format!(
                "only {} row groups in {} files: the flush predicate was never exercised",
                report.row_groups_compared, report.files_compared
            ),
        )?;
        ensure(
            report.rows_compared == input.sidecar.records + input.sidecar.expected_series,
            format!("{} rows compared", report.rows_compared),
        )?;
    }
    Ok(())
}

// Scenario: every exported stage runs once on the logs and the metrics
// fixture, the store-backed ones against a local object store.
// Guarantees: each stage's output holds exactly the input's records, its
// own verifications pass, and descriptor coverage and cache suppression
// match the fixture's series.
fn stage_row_counts_are_exact(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("store"))?;
    for input in fixture_inputs(root)? {
        for name in StageName::ALL {
            for compression in [stages::zstd(), parquet::basic::Compression::UNCOMPRESSED] {
                if compression != stages::zstd() && name != StageName::Encode {
                    continue;
                }
                let stage = Stage::new(name, bench_config(root), input.clone(), compression, true)?;
                let output = stage.run(stage.prepare()?)?;
                let observation = stage.observe(&output)?;
                let failed: Vec<_> = observation
                    .checks
                    .iter()
                    .filter(|check| !check.passed)
                    .collect();
                ensure(
                    failed.is_empty(),
                    format!(
                        "{} on {:?}: failed checks {failed:?}",
                        name.as_str(),
                        input.sidecar.signal
                    ),
                )?;
                ensure(
                    observation.values_rows == input.sidecar.records,
                    format!(
                        "{} on {:?}: {} rows for {} records",
                        name.as_str(),
                        input.sidecar.signal,
                        observation.values_rows,
                        input.sidecar.records
                    ),
                )?;
                ensure(
                    observation
                        .checks
                        .iter()
                        .any(|check| check.name == "values_rows"),
                    format!("{} recorded no row check", name.as_str()),
                )?;
            }
        }
    }
    Ok(())
}

// Scenario: preparing each sample's input takes five milliseconds of busy
// work while the measured operation itself is trivial.
// Guarantees: input generation and observation are outside the timer: no
// sample is charged the preparation, which is accounted separately.
fn input_generation_is_excluded_from_timing() -> Result<()> {
    let spin = Duration::from_millis(5);
    let busy = |length: Duration| {
        let started = Instant::now();
        let mut work = 0u64;
        while started.elapsed() < length {
            work = std::hint::black_box(work.wrapping_add(1));
        }
        work
    };
    let samples = super::sample_loop(
        Clock::Thread,
        30,
        Duration::ZERO,
        || Ok(busy(spin)),
        |value| Ok(value.wrapping_mul(3)),
        |_| {
            let _ = busy(spin);
            Ok(Vec::new())
        },
    )?;
    ensure(samples.sample_count >= 30, "fewer than 30 samples")?;
    let slowest = samples
        .samples
        .iter()
        .map(|s| s.wall_ns)
        .max()
        .unwrap_or(u128::MAX);
    ensure(
        slowest < spin.as_nanos() / 5,
        format!("a sample took {slowest} ns: preparation leaked into the timer"),
    )?;
    ensure(
        samples.prepare_ns_total >= spin.as_nanos() * samples.sample_count as u128,
        "preparation time was not accounted separately",
    )?;
    Ok(())
}

// Scenario: an input file is written in the length-prefixed format, read
// back, and then read again after its last byte has been cut off.
// Guarantees: every request comes back byte for byte, and a truncated file
// or a stale sidecar hash is refused rather than read as a shorter input.
fn input_file_round_trips(root: &Path) -> Result<()> {
    let requests: Vec<Bytes> = (0..3).map(|r| logs_request(r, 5, 10)).collect();
    let path = root.join("round-trip.otlp");
    write_input(&path, Signal::Logs, &requests, 15, 3)?;
    let input = stages::read_input(&path)?;
    ensure(
        input.requests == requests,
        "requests changed in the round trip",
    )?;
    let mut data = std::fs::read(&path)?;
    let _ = data.pop();
    ensure(
        stages::split_requests(&Bytes::from(data.clone())).is_err(),
        "a truncated body was accepted",
    )?;
    std::fs::write(&path, &data)?;
    ensure(
        stages::read_input(&path).is_err(),
        "a stale sidecar hash was accepted",
    )?;
    Ok(())
}

// Scenario: every stage prepares one iteration's input and then runs it,
// with the pieces of per-iteration setup it builds counted.
// Guarantees: `run` builds none of its own setup -- the counter never
// advances across a timed call -- while `prepare` builds it, and two
// preparations of a stage that writes name different destinations, so no
// iteration can overwrite what an earlier one wrote.
fn run_never_builds_its_own_setup(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("store"))?;
    let needs_setup = [
        StageName::OtlpSort,
        StageName::OtlpParquetLocal,
        StageName::OtlpParquetZstd,
        StageName::OtlpMinio,
        StageName::SortSeal,
        StageName::LocalWrite,
        StageName::Upload,
        StageName::Sink,
    ];
    let needs_cache = [
        StageName::OtlpSort,
        StageName::OtlpParquetLocal,
        StageName::OtlpParquetZstd,
        StageName::OtlpMinio,
        StageName::SortSeal,
    ];
    let writes = [
        StageName::OtlpParquetLocal,
        StageName::OtlpParquetZstd,
        StageName::OtlpMinio,
        StageName::LocalWrite,
        StageName::Upload,
        StageName::Sink,
    ];
    let input = fixture_inputs(root)?.remove(0);
    for name in StageName::ALL {
        let stage = Stage::new(
            name,
            bench_config(root),
            input.clone(),
            stages::zstd(),
            true,
        )?;
        let before = stage.setup_built();
        let prepared = stage.prepare()?;
        let destinations = prepared.destinations();
        let carries = prepared.carries();
        ensure(
            !carries.is_empty(),
            format!("{} says nothing about what it was handed", name.as_str()),
        )?;
        ensure(
            carries.contains("cache") == needs_cache.contains(&name),
            format!("{} reports carrying {carries:?}", name.as_str()),
        )?;
        let prepared_count = stage.setup_built();
        if needs_setup.contains(&name) {
            ensure(
                prepared_count > before,
                format!("{} built no setup while preparing", name.as_str()),
            )?;
        }
        let output = stage.run(prepared)?;
        ensure(
            stage.setup_built() == prepared_count,
            format!(
                "{} built {} pieces of setup inside the timed run",
                name.as_str(),
                stage.setup_built() - prepared_count
            ),
        )?;
        let _ = stage.observe(&output)?;
        drop(output);
        if writes.contains(&name) {
            ensure(
                !destinations.is_empty(),
                format!("{} names no destination", name.as_str()),
            )?;
            let next = stage.prepare()?.destinations();
            ensure(
                next != destinations,
                format!(
                    "{} prepares the same destination twice: {destinations:?}",
                    name.as_str()
                ),
            )?;
        }
    }
    Ok(())
}

// Scenario: the cumulative sort layer is sampled twice through the real
// sampling loop, once with its ordinary fixture and once with a cache
// warm-up two hundred thousand entries heavier, which is more than an
// order of magnitude more per-iteration setup for exactly the same
// admitted work.
// Guarantees: the preparation really did get heavier, the loop prepares
// once per sample, and the measured per-iteration time stays within a
// quarter of the light fixture's -- the fixture is built outside the timer.
fn a_heavier_fixture_does_not_move_the_measurement(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("store"))?;
    let input = fixture_inputs(root)?.remove(0);
    let sample = |extra: usize| -> Result<(u128, u128, u64)> {
        let stage = Stage::new(
            StageName::OtlpSort,
            bench_config_with(root, extra),
            input.clone(),
            stages::zstd(),
            true,
        )?;
        let before = stage.setup_built();
        let samples = super::sample_loop(
            Clock::Thread,
            SAMPLES,
            Duration::ZERO,
            || stage.prepare(),
            |prepared| stage.run(prepared),
            |_| Ok(Vec::new()),
        )?;
        ensure(
            stage.setup_built() - before == samples.sample_count as u64,
            format!(
                "{} caches for {} samples: preparation is not once per sample",
                stage.setup_built() - before,
                samples.sample_count
            ),
        )?;
        let mut walls: Vec<u128> = samples.samples.iter().map(|s| s.wall_ns).collect();
        walls.sort_unstable();
        Ok((
            walls[walls.len() / 2],
            samples.prepare_ns_total / samples.sample_count.max(1) as u128,
            samples.sample_count as u64,
        ))
    };
    let (light_run, light_prepare, count) = sample(0)?;
    let (heavy_run, heavy_prepare, _) = sample(200_000)?;
    ensure(count >= SAMPLES as u64, "too few samples to compare")?;
    ensure(
        heavy_prepare > light_prepare * 3,
        format!(
            "the heavy fixture prepared in {heavy_prepare} ns against \
             {light_prepare} ns: it is not heavier, so the test proves nothing"
        ),
    )?;
    let tolerance = light_run / 4;
    ensure(
        heavy_run < light_run + tolerance,
        format!(
            "the measured run grew from {light_run} ns to {heavy_run} ns when only \
             the fixture grew: preparation is inside the timer"
        ),
    )?;
    Ok(())
}

// Scenario: the artifacts of two Criterion attempts of one layer, written
// under the ids those attempts ran with, are read back.
// Guarantees: each attempt is read under its own id, an id whose artifact
// is missing is a loud error naming it rather than another attempt's
// numbers, and Criterion's own sanitization of an id is applied.
fn criterion_artifacts_are_read_per_attempt(root: &Path) -> Result<()> {
    let home = root.join("criterion-home");
    let write = |function: &str, times: &[f64]| -> Result<()> {
        let directory = home
            .join(stages::criterion_directory_name("otlp_sort"))
            .join(stages::criterion_directory_name(function))
            .join("new");
        std::fs::create_dir_all(&directory)?;
        std::fs::write(
            directory.join("sample.json"),
            serde_json::to_vec(&serde_json::json!({"times": times, "iters": [1.0]}))?,
        )?;
        Ok(())
    };
    write("logs", &[1e9, 5e8])?;
    write("logs-attempt2", &[2e9])?;
    write("logs/slash", &[4e9])?;
    let first = stages::criterion_measured_seconds(&home, "otlp_sort", "logs")?;
    let second = stages::criterion_measured_seconds(&home, "otlp_sort", "logs-attempt2")?;
    ensure(
        (first - 1.5).abs() < 1e-9 && (second - 2.0).abs() < 1e-9,
        format!("attempts read back as {first} and {second} seconds"),
    )?;
    let sanitized = stages::criterion_measured_seconds(&home, "otlp_sort", "logs/slash")?;
    ensure(
        (sanitized - 4.0).abs() < 1e-9,
        format!("a sanitized id read back as {sanitized} seconds"),
    )?;
    match stages::criterion_measured_seconds(&home, "otlp_sort", "logs-attempt3") {
        Ok(value) => {
            return Err(format!("a missing attempt answered with {value} seconds").into());
        }
        Err(error) => ensure(
            error.to_string().contains("logs-attempt3"),
            format!("the refusal does not name the missing id: {error}"),
        )?,
    }
    Ok(())
}

// Scenario: the stages that store an object are run and their results are
// inspected for what a completed write means.
// Guarantees: every upload, local write and sink result records that
// completion is object-store visibility with verified bytes and not host
// power-loss durability, so no reader can take it for a durability claim.
fn store_stages_record_completion_semantics(root: &Path) -> Result<()> {
    std::fs::create_dir_all(root.join("store"))?;
    let input = fixture_inputs(root)?.remove(0);
    for name in [StageName::LocalWrite, StageName::Upload, StageName::Sink] {
        let stage = Stage::new(
            name,
            bench_config(root),
            input.clone(),
            stages::zstd(),
            true,
        )?;
        let output = stage.run(stage.prepare()?)?;
        let observation = stage.observe(&output)?;
        drop(output);
        let recorded = observation
            .extra
            .get("completion_semantics")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        ensure(
            recorded.contains("read back") && recorded.contains("not host power-loss"),
            format!("{} records completion as {recorded:?}", name.as_str()),
        )?;
        ensure(
            observation
                .extra
                .get("objects_count")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default()
                > 0,
            format!("{} counted no objects", name.as_str()),
        )?;
    }
    Ok(())
}

// Scenario: the exported stage names are read back from the bench.
// Guarantees: the registered names are exactly the contract's names in
// order, and the Criterion layers are its first six.
fn stage_names_are_the_contract() -> Result<()> {
    let names: Vec<&str> = StageName::ALL.iter().map(|s| s.as_str()).collect();
    ensure(
        names
            == [
                "otlp_noop",
                "otlp_convert",
                "otlp_extract_hash",
                "otlp_sort",
                "otlp_parquet_local",
                "otlp_parquet_zstd",
                "otlp_minio",
                "convert",
                "extract",
                "sort_seal",
                "merge",
                "encode",
                "local_write",
                "upload",
                "sink",
            ],
        format!("stage names {names:?}"),
    )?;
    ensure(
        StageName::LAYERS[..] == StageName::ALL[..6],
        "layers are not the first six",
    )?;
    Ok(())
}

/// Run every self-test; the first failure ends the process nonzero.
///
/// # Errors
/// Returns the first failed check or error.
pub fn run() -> Result<()> {
    let holder = tempfile::tempdir()?;
    let root = holder.path();
    stage_names_are_the_contract()?;
    input_file_round_trips(root)?;
    input_generation_is_excluded_from_timing()?;
    diagnostic_encoding_matches_sink(root)?;
    stage_row_counts_are_exact(root)?;
    run_never_builds_its_own_setup(root)?;
    a_heavier_fixture_does_not_move_the_measurement(root)?;
    criterion_artifacts_are_read_per_attempt(root)?;
    store_stages_record_completion_semantics(root)?;
    Ok(())
}
