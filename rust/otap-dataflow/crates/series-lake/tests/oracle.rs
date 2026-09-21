// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reference-oracle property test (spec section 9.1).
//!
//! The oracle computes the expected identities independently of `extract`: it
//! builds a [`Descriptor`] straight from the generated OTLP input and hashes it
//! with `canonical_bytes` plus `series_id`. Nothing in the reference path calls
//! extraction, so the two implementations can genuinely disagree.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::canonical::{
    Descriptor, MetricDescriptor, MetricKind, SeriesId, Signal, Temporality, canonical_bytes,
    series_id,
};
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use otel_arrow_dfe_series_lake::schema::Dataset;
use otel_arrow_dfe_series_lake::sink::{FileNaming, Sink};
use otel_arrow_dfe_series_lake::sort::{SortSpec, is_sorted};
use otel_arrow_dfe_series_lake::value::Value;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use proptest::prelude::*;
use tokio_util::sync::CancellationToken;

const WINDOW_START: i64 = 1_789_960_500;
const SEAL_AT_US: i64 = 1_789_960_500_000_000;

fn kv(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.into())),
        }),
    }
}

fn attr(k: &str, v: &str) -> (String, Value) {
    (k.to_string(), Value::Str(v.to_string()))
}

fn base_cfg(sorting: bool) -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = vec!["logger.name".into()];
    cfg.sorting.enabled = sorting;
    // Seal a run per request: maximal merge pressure. `validate()` is not called
    // here, so the run_target/max_row relation does not apply.
    cfg.sorting.run_target_bytes = 1;
    cfg.sorting.merge_chunk_bytes = 1;
    cfg.ingress.max_row_bytes = 1 << 20;
    cfg
}

/// The reference identity of a logs series, built without touching `extract`.
fn logs_series_id(host: u8, logger: u8) -> SeriesId {
    series_id(&canonical_bytes(&Descriptor {
        signal: Signal::Logs,
        resource_attrs: vec![attr("host.id", &format!("h{host}"))],
        resource_schema_url: String::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_schema_url: String::new(),
        scope_attrs: vec![],
        metric: None,
        attrs: vec![attr("logger.name", &format!("L{logger}"))],
    }))
}

/// The reference identity of a metrics series, built without touching `extract`.
fn metrics_series_id(
    host: u8,
    name: &str,
    kind: MetricKind,
    temporality: Temporality,
    dp: u8,
) -> SeriesId {
    series_id(&canonical_bytes(&Descriptor {
        signal: Signal::Metrics,
        resource_attrs: vec![attr("host.id", &format!("h{host}"))],
        resource_schema_url: String::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_schema_url: String::new(),
        scope_attrs: vec![],
        metric: Some(MetricDescriptor {
            name: name.to_string(),
            unit: String::new(),
            kind,
            temporality,
            is_monotonic: false,
            description: String::new(),
        }),
        attrs: vec![attr("dp", &format!("d{dp}"))],
    }))
}

/// Admit every request into one block, seal it, write it and read the files back.
async fn round_trip(
    cfg: &LakeConfig,
    requests: Vec<OtapArrowRecords>,
) -> Result<(BTreeMap<Dataset, Vec<RecordBatch>>, tempfile::TempDir), TestCaseError> {
    let dir = tempfile::tempdir().map_err(|e| TestCaseError::fail(e.to_string()))?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(dir.path())
            .map_err(|e| TestCaseError::fail(e.to_string()))?,
    );
    let mut cache = SeriesCache::new(10_000);
    let mut block: Block<usize> = Block::new(WINDOW_START, 1, cfg);
    for (i, mut records) in requests.into_iter().enumerate() {
        let e = extract(&mut records, cfg).map_err(|e| TestCaseError::fail(format!("{e}")))?;
        let r = block
            .reserve(&e, &mut cache, 8, cfg)
            .map_err(|e| TestCaseError::fail(format!("{e}")))?;
        block
            .admit(e, r, i)
            .map_err(|e| TestCaseError::fail(format!("{e}")))?;
    }
    block
        .seal(SEAL_AT_US)
        .map_err(|e| TestCaseError::fail(format!("{e}")))?;
    let sink = Sink::new(store, cfg.clone(), FileNaming::new("oracle"));
    let report = sink
        .write_block(&block, &CancellationToken::new())
        .await
        .map_err(|e| TestCaseError::fail(format!("{e}")))?;

    let mut out: BTreeMap<Dataset, Vec<RecordBatch>> = BTreeMap::new();
    for (ds, path, _) in &report.files {
        let file = std::fs::File::open(dir.path().join(path.as_ref()))
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| TestCaseError::fail(e.to_string()))?
            .build()
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        for b in reader {
            out.entry(*ds)
                .or_default()
                .push(b.map_err(|e| TestCaseError::fail(e.to_string()))?);
        }
    }
    Ok((out, dir))
}

fn series_id_at(b: &RecordBatch, row: usize) -> Vec<u8> {
    b.column_by_name("series_id")
        .expect("series_id")
        .as_fixed_size_binary()
        .value(row)
        .to_vec()
}

fn time_at(b: &RecordBatch, row: usize) -> Option<i64> {
    let t = b
        .column_by_name("time_unix_nano")
        .expect("time_unix_nano")
        .as_primitive::<Int64Type>();
    t.is_valid(row).then(|| t.value(row))
}

/// Map column read back as a sorted key/value vector.
fn map_at(b: &RecordBatch, name: &str, row: usize) -> Vec<(String, String)> {
    let m = b.column_by_name(name).expect("map column").as_map();
    let entries = m.value(row);
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1).as_string::<i32>();
    let mut out: Vec<(String, String)> = (0..entries.len())
        .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
        .collect();
    out.sort();
    out
}

fn string_at(b: &RecordBatch, name: &str, row: usize) -> String {
    b.column_by_name(name)
        .expect("utf8 column")
        .as_string::<i32>()
        .value(row)
        .to_string()
}

/// Assert the values file is globally ordered when sorting is on.
fn check_order(
    cfg: &LakeConfig,
    batches: &[RecordBatch],
    signal: Signal,
) -> Result<(), TestCaseError> {
    if !cfg.sorting.enabled {
        return Ok(());
    }
    let keys = if signal == Signal::Logs {
        &cfg.logs.values_sort
    } else {
        &cfg.metrics.values_sort
    };
    let spec = SortSpec::new(keys.clone());
    let schema = batches[0].schema();
    let all = arrow::compute::concat_batches(&schema, batches)
        .map_err(|e| TestCaseError::fail(e.to_string()))?;
    let ordered = is_sorted(&all, &spec).map_err(|e| TestCaseError::fail(format!("{e}")))?;
    prop_assert!(ordered, "values file is globally ordered by the sort spec");
    Ok(())
}

// ---------------------------------------------------------------- logs

#[derive(Debug, Clone)]
struct LogRec {
    host: u8,
    logger: u8,
    time: u64,
    body: String,
}

fn log_rec() -> impl Strategy<Value = LogRec> {
    (
        0u8..3,
        0u8..3,
        prop_oneof![Just(0u64), 1u64..1_000_000],
        "[a-z]{0,8}",
    )
        .prop_map(|(host, logger, time, body)| LogRec {
            host,
            logger,
            time,
            body,
        })
}

fn logs_request(recs: &[LogRec]) -> OtapArrowRecords {
    let mut hosts: Vec<u8> = recs.iter().map(|r| r.host).collect();
    hosts.sort_unstable();
    hosts.dedup();
    encode_logs(&LogsData {
        resource_logs: hosts
            .iter()
            .map(|h| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", &format!("h{h}"))],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: recs
                        .iter()
                        .filter(|r| r.host == *h)
                        .map(|r| LogRecord {
                            time_unix_nano: r.time,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(r.body.clone())),
                            }),
                            attributes: vec![kv("logger.name", &format!("L{}", r.logger))],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
    })
}

fn split<T: Clone>(items: &[T], sizes: &[u8]) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let mut idx = 0;
    let mut which = 0;
    while idx < items.len() {
        let n = usize::from(sizes[which % sizes.len()].max(1)).min(items.len() - idx);
        out.push(items[idx..idx + n].to_vec());
        idx += n;
        which += 1;
    }
    out
}

async fn logs_case(recs: Vec<LogRec>, sizes: Vec<u8>, sorting: bool) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let requests: Vec<OtapArrowRecords> = split(&recs, &sizes)
        .iter()
        .map(|c| logs_request(c))
        .collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    // Model, computed without extraction.
    let mut model: Vec<(Vec<u8>, Option<i64>, String)> = recs
        .iter()
        .map(|r| {
            (
                logs_series_id(r.host, r.logger).to_vec(),
                (r.time > 0).then_some(r.time as i64),
                r.body.clone(),
            )
        })
        .collect();
    let model_series: BTreeSet<Vec<u8>> = recs
        .iter()
        .map(|r| logs_series_id(r.host, r.logger).to_vec())
        .collect();

    let values = files.get(&Dataset::LogsValues).expect("values file");
    let mut actual: Vec<(Vec<u8>, Option<i64>, String)> = Vec::new();
    for b in values {
        for row in 0..b.num_rows() {
            actual.push((
                series_id_at(b, row),
                time_at(b, row),
                string_at(b, "body", row),
            ));
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "logs value rows as a multiset of (series_id, time, body)"
    );
    check_order(&cfg, values, Signal::Logs)?;

    // Descriptor content, per series.
    let series = files.get(&Dataset::LogsSeries).expect("series file");
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for b in series {
        for row in 0..b.num_rows() {
            let id = series_id_at(b, row);
            prop_assert!(
                seen.insert(id.clone()),
                "a series is described at most once"
            );
            let host_attr = map_at(b, "resource_attrs", row);
            let ident = map_at(b, "attrs", row);
            prop_assert_eq!(host_attr.len(), 1);
            prop_assert_eq!(ident.len(), 1);
            let expected = logs_series_id(
                host_attr[0]
                    .1
                    .trim_start_matches('h')
                    .parse::<u8>()
                    .expect("host"),
                ident[0]
                    .1
                    .trim_start_matches('L')
                    .parse::<u8>()
                    .expect("logger"),
            );
            prop_assert_eq!(
                id,
                expected.to_vec(),
                "descriptor content hashes to its series_id"
            );
        }
    }
    prop_assert_eq!(seen, model_series, "descriptor set");
    Ok(())
}

// ------------------------------------------------------- metric numbers

#[derive(Debug, Clone)]
struct NumRec {
    host: u8,
    metric: u8,
    dp: u8,
    time: u64,
    int_value: Option<i64>,
    double_value: Option<f64>,
}

fn num_rec() -> impl Strategy<Value = NumRec> {
    (
        0u8..2,
        0u8..2,
        0u8..3,
        prop_oneof![Just(0u64), 1u64..1_000_000],
        any::<bool>(),
        -1000i64..1000,
    )
        .prop_map(|(host, metric, dp, time, is_int, v)| NumRec {
            host,
            metric,
            dp,
            time,
            int_value: is_int.then_some(v),
            double_value: (!is_int).then_some(v as f64),
        })
}

fn metrics_request(build: impl Fn(u8) -> Vec<Metric>, hosts: &[u8]) -> OtapArrowRecords {
    encode_metrics(&MetricsData {
        resource_metrics: hosts
            .iter()
            .map(|h| ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", &format!("h{h}"))],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: build(*h),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
    })
}

fn number_request(recs: &[NumRec]) -> OtapArrowRecords {
    let mut hosts: Vec<u8> = recs.iter().map(|r| r.host).collect();
    hosts.sort_unstable();
    hosts.dedup();
    metrics_request(
        |h| {
            let mut names: Vec<u8> = recs
                .iter()
                .filter(|r| r.host == h)
                .map(|r| r.metric)
                .collect();
            names.sort_unstable();
            names.dedup();
            names
                .iter()
                .map(|m| Metric {
                    name: format!("m{m}"),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: recs
                            .iter()
                            .filter(|r| r.host == h && r.metric == *m)
                            .map(|r| NumberDataPoint {
                                time_unix_nano: r.time,
                                attributes: vec![kv("dp", &format!("d{}", r.dp))],
                                value: Some(match (r.int_value, r.double_value) {
                                    (Some(i), _) => number_data_point::Value::AsInt(i),
                                    (_, Some(d)) => number_data_point::Value::AsDouble(d),
                                    _ => number_data_point::Value::AsInt(0),
                                }),
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                })
                .collect()
        },
        &hosts,
    )
}

async fn number_case(
    recs: Vec<NumRec>,
    sizes: Vec<u8>,
    sorting: bool,
) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let requests: Vec<OtapArrowRecords> = split(&recs, &sizes)
        .iter()
        .map(|c| number_request(c))
        .collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    let mut model: Vec<(Vec<u8>, Option<i64>, Option<i64>, Option<String>)> = recs
        .iter()
        .map(|r| {
            (
                metrics_series_id(
                    r.host,
                    &format!("m{}", r.metric),
                    MetricKind::Gauge,
                    Temporality::Unspecified,
                    r.dp,
                )
                .to_vec(),
                (r.time > 0).then_some(r.time as i64),
                r.int_value,
                r.double_value.map(|d| d.to_string()),
            )
        })
        .collect();
    let model_series: BTreeSet<Vec<u8>> = model.iter().map(|m| m.0.clone()).collect();

    let values = files.get(&Dataset::MetricsNumber).expect("number file");
    let mut actual: Vec<(Vec<u8>, Option<i64>, Option<i64>, Option<String>)> = Vec::new();
    for b in values {
        let vi = b
            .column_by_name("value_int")
            .expect("value_int")
            .as_primitive::<Int64Type>();
        let vd = b
            .column_by_name("value_double")
            .expect("value_double")
            .as_primitive::<Float64Type>();
        for row in 0..b.num_rows() {
            actual.push((
                series_id_at(b, row),
                time_at(b, row),
                vi.is_valid(row).then(|| vi.value(row)),
                vd.is_valid(row).then(|| vd.value(row).to_string()),
            ));
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "number rows as a multiset of (series_id, time, int, double)"
    );
    check_order(&cfg, values, Signal::Metrics)?;

    let series = files.get(&Dataset::MetricsSeries).expect("series file");
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for b in series {
        for row in 0..b.num_rows() {
            let id = series_id_at(b, row);
            prop_assert!(
                seen.insert(id.clone()),
                "a series is described at most once"
            );
            prop_assert_eq!(string_at(b, "metric_type", row), "gauge".to_string());
            prop_assert_eq!(string_at(b, "temporality", row), String::new());
            let host_attr = map_at(b, "resource_attrs", row);
            let ident = map_at(b, "attrs", row);
            let expected = metrics_series_id(
                host_attr[0]
                    .1
                    .trim_start_matches('h')
                    .parse::<u8>()
                    .expect("host"),
                &string_at(b, "metric_name", row),
                MetricKind::Gauge,
                Temporality::Unspecified,
                ident[0]
                    .1
                    .trim_start_matches('d')
                    .parse::<u8>()
                    .expect("dp"),
            );
            prop_assert_eq!(
                id,
                expected.to_vec(),
                "descriptor content hashes to its series_id"
            );
        }
    }
    prop_assert_eq!(seen, model_series, "descriptor set");
    Ok(())
}

// ---------------------------------------------------- metric histograms

#[derive(Debug, Clone)]
struct HistRec {
    host: u8,
    metric: u8,
    dp: u8,
    time: u64,
    count: u32,
}

fn hist_rec() -> impl Strategy<Value = HistRec> {
    (
        0u8..2,
        0u8..2,
        0u8..3,
        prop_oneof![Just(0u64), 1u64..1_000_000],
        0u32..100,
    )
        .prop_map(|(host, metric, dp, time, count)| HistRec {
            host,
            metric,
            dp,
            time,
            count,
        })
}

fn histogram_request(recs: &[HistRec]) -> OtapArrowRecords {
    let mut hosts: Vec<u8> = recs.iter().map(|r| r.host).collect();
    hosts.sort_unstable();
    hosts.dedup();
    metrics_request(
        |h| {
            let mut names: Vec<u8> = recs
                .iter()
                .filter(|r| r.host == h)
                .map(|r| r.metric)
                .collect();
            names.sort_unstable();
            names.dedup();
            names
                .iter()
                .map(|m| Metric {
                    name: format!("m{m}"),
                    data: Some(metric::Data::Histogram(Histogram {
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        data_points: recs
                            .iter()
                            .filter(|r| r.host == h && r.metric == *m)
                            .map(|r| HistogramDataPoint {
                                time_unix_nano: r.time,
                                attributes: vec![kv("dp", &format!("d{}", r.dp))],
                                count: u64::from(r.count),
                                sum: Some(f64::from(r.count)),
                                bucket_counts: vec![u64::from(r.count), 0],
                                explicit_bounds: vec![1.0],
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                })
                .collect()
        },
        &hosts,
    )
}

async fn histogram_case(
    recs: Vec<HistRec>,
    sizes: Vec<u8>,
    sorting: bool,
) -> Result<(), TestCaseError> {
    let cfg = base_cfg(sorting);
    let requests: Vec<OtapArrowRecords> = split(&recs, &sizes)
        .iter()
        .map(|c| histogram_request(c))
        .collect();
    let (files, _dir) = round_trip(&cfg, requests).await?;

    let mut model: Vec<(Vec<u8>, Option<i64>, i64)> = recs
        .iter()
        .map(|r| {
            (
                metrics_series_id(
                    r.host,
                    &format!("m{}", r.metric),
                    MetricKind::Histogram,
                    Temporality::Delta,
                    r.dp,
                )
                .to_vec(),
                (r.time > 0).then_some(r.time as i64),
                i64::from(r.count),
            )
        })
        .collect();
    let model_series: BTreeSet<Vec<u8>> = model.iter().map(|m| m.0.clone()).collect();

    let values = files
        .get(&Dataset::MetricsHistogram)
        .expect("histogram file");
    let mut actual: Vec<(Vec<u8>, Option<i64>, i64)> = Vec::new();
    for b in values {
        let count = b
            .column_by_name("count")
            .expect("count")
            .as_primitive::<Int64Type>();
        for row in 0..b.num_rows() {
            actual.push((series_id_at(b, row), time_at(b, row), count.value(row)));
        }
    }
    model.sort();
    actual.sort();
    prop_assert_eq!(
        actual,
        model,
        "histogram rows as a multiset of (series_id, time, count)"
    );
    check_order(&cfg, values, Signal::Metrics)?;

    let series = files.get(&Dataset::MetricsSeries).expect("series file");
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for b in series {
        for row in 0..b.num_rows() {
            let id = series_id_at(b, row);
            prop_assert!(
                seen.insert(id.clone()),
                "a series is described at most once"
            );
            prop_assert_eq!(string_at(b, "metric_type", row), "histogram".to_string());
            prop_assert_eq!(string_at(b, "temporality", row), "delta".to_string());
        }
    }
    prop_assert_eq!(seen, model_series, "descriptor set");
    Ok(())
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt")
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, .. ProptestConfig::default() })]

    /// Scenario: random log records split into random requests, sorting on and off.
    /// Guarantees: the values file equals the independently computed model as a multiset of
    /// (series_id, time, body), the descriptor set matches and each descriptor's content
    /// hashes back to its own series_id; with sorting on the file is globally ordered.
    #[test]
    fn oracle_logs(
        recs in prop::collection::vec(log_rec(), 1..40),
        sizes in prop::collection::vec(1u8..6, 1..8),
        sorting in any::<bool>(),
    ) {
        rt().block_on(logs_case(recs, sizes, sorting))?;
    }

    /// Scenario: random gauge data points split into random requests, sorting on and off.
    /// Guarantees: the number file equals the independently computed model as a multiset of
    /// (series_id, time, value_int, value_double) and the descriptors match.
    #[test]
    fn oracle_metric_numbers(
        recs in prop::collection::vec(num_rec(), 1..30),
        sizes in prop::collection::vec(1u8..6, 1..8),
        sorting in any::<bool>(),
    ) {
        rt().block_on(number_case(recs, sizes, sorting))?;
    }

    /// Scenario: random delta histogram points split into random requests, sorting on and off.
    /// Guarantees: the histogram file equals the independently computed model as a multiset of
    /// (series_id, time, count) and the descriptors carry kind and temporality.
    #[test]
    fn oracle_metric_histograms(
        recs in prop::collection::vec(hist_rec(), 1..30),
        sizes in prop::collection::vec(1u8..6, 1..8),
        sorting in any::<bool>(),
    ) {
        rt().block_on(histogram_case(recs, sizes, sorting))?;
    }
}
