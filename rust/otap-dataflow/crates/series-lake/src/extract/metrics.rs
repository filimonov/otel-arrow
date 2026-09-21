// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics extraction: number and histogram points (spec section 5.1).

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, Float64Type, Int32Type, Int64Type, TimeUnit, UInt8Type, UInt32Type, UInt64Type,
};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use super::{
    Budget, Col, DescriptorRow, ExtractStats, Extracted, RowSink, ValuesRow, attr_table, attrs_of,
    denorm_bytes, denorm_lookup, descriptor_row, i64_at, opt_u16_at, plain, producer_id, str_at,
    struct_child, timestamp_pair,
};
use crate::attrs::AttrTable;
use crate::canonical::{Descriptor, MetricDescriptor, MetricKind, SeriesId, Signal, Temporality};
use crate::config::{LakeConfig, UnsupportedPolicy};
use crate::error::{Error, RefuseReason, Result};
use crate::schema::{Dataset, denorm_columns};
use crate::value::Value;

/// Per-metric fields read once from the `UnivariateMetrics` batch.
///
/// Only the finished `descriptor_base` is kept: the resource and scope ids were
/// already resolved into attribute lists here, so nothing downstream reads them.
struct MetricRow {
    descriptor_base: Descriptor,
}

/// Memo key for a metrics series: the metric and the point's attribute parent.
///
/// Both ids are optional, because pdata writes a null id for a point that has
/// no attributes; `None` must stay distinct from attribute parent 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MemoKey {
    metric_id: u32,
    attrs_id: Option<u32>,
}

/// The OTLP `u32` flags bit set reinterpreted into the signed storage column.
///
/// Stored as received; readers treat it as a bit set, exactly as in logs.
#[allow(clippy::cast_possible_wrap)]
fn flags(a: &Option<ArrayRef>, row: usize) -> i32 {
    a.as_ref()
        .and_then(|a| {
            a.is_valid(row)
                .then(|| a.as_primitive::<UInt32Type>().value(row))
        })
        .unwrap_or(0) as i32
}

fn metric_rows(
    records: &OtapArrowRecords,
    resource_attrs: &AttrTable,
    scope_attrs: &AttrTable,
    cfg: &LakeConfig,
) -> Result<HashMap<u32, MetricRow>> {
    let Some(m) = records.get(ArrowPayloadType::UnivariateMetrics) else {
        return Ok(HashMap::new());
    };
    let id = plain(m, "id", &DataType::UInt16)?;
    let kind = plain(m, "metric_type", &DataType::UInt8)?;
    let name = plain(m, "name", &DataType::Utf8)?;
    let temporality = plain(m, "aggregation_temporality", &DataType::Int32)?;
    let description = plain(m, "description", &DataType::Utf8)?;
    let is_monotonic = plain(m, "is_monotonic", &DataType::Boolean)?;
    let unit = plain(m, "unit", &DataType::Utf8)?;
    let scope_schema = plain(m, "schema_url", &DataType::Utf8)?;
    let res_id = struct_child(m, "resource", "id", &DataType::UInt16)?;
    let res_schema = struct_child(m, "resource", "schema_url", &DataType::Utf8)?;
    let scope_id = struct_child(m, "scope", "id", &DataType::UInt16)?;
    let scope_name = struct_child(m, "scope", "name", &DataType::Utf8)?;
    let scope_version = struct_child(m, "scope", "version", &DataType::Utf8)?;
    let mut out = HashMap::with_capacity(m.num_rows());
    for row in 0..m.num_rows() {
        let kind_u8 = kind
            .as_ref()
            .map_or(0, |a| a.as_primitive::<UInt8Type>().value(row));
        let kind = match kind_u8 {
            1 => MetricKind::Gauge,
            2 => MetricKind::Sum,
            3 => MetricKind::Histogram,
            4 => MetricKind::ExpHistogram,
            5 => MetricKind::Summary,
            other => return Err(Error::invalid(format!("metric_type {other}"))),
        };
        let temporality = match temporality.as_ref().and_then(|a| {
            a.is_valid(row)
                .then(|| a.as_primitive::<Int32Type>().value(row))
        }) {
            Some(1) => Temporality::Delta,
            Some(2) => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        };
        // Order matters: an unsupported kind is settled by the policy first, and
        // its temporality is never validated. pdata only supplies temporality for
        // sums and histograms, so a summary would otherwise be refused as invalid
        // even under the drop policy.
        match kind {
            MetricKind::ExpHistogram | MetricKind::Summary => {
                if cfg.unsupported == UnsupportedPolicy::Reject {
                    return Err(Error::Refused(RefuseReason::Unsupported(
                        kind.as_str().to_string(),
                    )));
                }
                // Dropped: the point rows are counted from the unsupported
                // payloads, and no values row can reference this metric.
                continue;
            }
            MetricKind::Sum | MetricKind::Histogram => {
                if temporality == Temporality::Unspecified {
                    return Err(Error::invalid(
                        "sum or histogram with unspecified temporality",
                    ));
                }
            }
            MetricKind::Gauge => {}
        }
        let rid = opt_u16_at(&res_id, row);
        let sid = opt_u16_at(&scope_id, row);
        let descriptor_base = Descriptor {
            signal: Signal::Metrics,
            resource_attrs: attrs_of(resource_attrs, rid).to_vec(),
            resource_schema_url: str_at(&res_schema, row),
            scope_name: str_at(&scope_name, row),
            scope_version: str_at(&scope_version, row),
            scope_schema_url: str_at(&scope_schema, row),
            scope_attrs: attrs_of(scope_attrs, sid).to_vec(),
            metric: Some(MetricDescriptor {
                name: str_at(&name, row),
                unit: str_at(&unit, row),
                kind,
                temporality: if kind == MetricKind::Gauge {
                    Temporality::Unspecified
                } else {
                    temporality
                },
                is_monotonic: kind == MetricKind::Sum
                    && is_monotonic
                        .as_ref()
                        .is_some_and(|a| a.is_valid(row) && a.as_boolean().value(row)),
                description: str_at(&description, row),
            }),
            attrs: vec![],
        };
        let metric_id =
            opt_u16_at(&id, row).ok_or_else(|| Error::invalid("metric row without id"))?;
        let _ = out.insert(metric_id, MetricRow { descriptor_base });
    }
    Ok(out)
}

struct Common<'a> {
    cfg: &'a LakeConfig,
    metrics: &'a HashMap<u32, MetricRow>,
    descriptors: Vec<DescriptorRow>,
    seen: HashSet<SeriesId>,
    memo: HashMap<MemoKey, SeriesId>,
    stats: ExtractStats,
}

/// The metric a data point belongs to.
///
/// A free function rather than a method on `Common`, so that holding the
/// borrowed `MetricRow` does not block `&mut c.stats` while building the row.
fn metric_of(metrics: &HashMap<u32, MetricRow>, metric_id: u32) -> Result<&MetricRow> {
    metrics
        .get(&metric_id)
        .ok_or_else(|| Error::invalid("data point references unknown metric"))
}

impl Common<'_> {
    /// Series id for (metric, point attrs), memoized before any hashing.
    ///
    /// Without the memo, `canonical_bytes` plus XXH3 plus the denormalized
    /// lookups would run once per data point instead of once per series, and
    /// `stats.denorm_type_mismatch` would count points rather than series.
    fn series_for(
        &mut self,
        metric_id: u32,
        attrs_id: Option<u32>,
        attrs: &[(String, Value)],
        budget: &mut Budget,
    ) -> Result<SeriesId> {
        let key = MemoKey {
            metric_id,
            attrs_id,
        };
        if let Some(id) = self.memo.get(&key) {
            return Ok(*id);
        }
        let mut d = metric_of(self.metrics, metric_id)?.descriptor_base.clone();
        d.attrs = attrs.to_vec();
        let dr = descriptor_row(d, Dataset::MetricsSeries, self.cfg, &mut self.stats, budget)?;
        let id = dr.series_id;
        if self.seen.insert(id) {
            self.descriptors.push(dr);
        } else {
            // Two memo keys can hash to the same series: the duplicate is not
            // retained, so give its charge back.
            budget.uncharge(dr.approx_bytes);
        }
        let _ = self.memo.insert(key, id);
        Ok(id)
    }
}

fn opt_i64(a: &Option<ArrayRef>, row: usize) -> Option<i64> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_primitive::<Int64Type>().value(row))
    })
}

fn opt_f64(a: &Option<ArrayRef>, row: usize) -> Option<f64> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_primitive::<Float64Type>().value(row))
    })
}

fn opt_u32_at(a: &Option<ArrayRef>, row: usize) -> Option<u32> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_primitive::<UInt32Type>().value(row))
    })
}

/// The eight leading columns shared by both values datasets, plus their bytes.
fn common_cols(
    id: SeriesId,
    m: &MetricRow,
    cfg: &LakeConfig,
    t_ns: i64,
    s_ns: i64,
    fl: i32,
    stats: &mut ExtractStats,
) -> (Vec<Col>, usize) {
    let (t_ns, t_us) = timestamp_pair(t_ns, stats);
    let (s_ns, s_us) = timestamp_pair(s_ns, stats);
    let producer = producer_id(
        &m.descriptor_base.resource_attrs,
        &cfg.producer_id_attribute,
    );
    let name = m
        .descriptor_base
        .metric
        .as_ref()
        .map(|x| x.name.clone())
        .unwrap_or_default();
    let bytes = 16 + producer.len() + name.len() + 8 * 5 + 48;
    let cols = vec![
        Col::Fixed(Some(id.to_vec())),
        Col::Str(Some(producer)),
        Col::Str(Some(name)),
        Col::TsUs(t_us),
        Col::Int(t_ns),
        Col::TsUs(s_us),
        Col::Int(s_ns),
        Col::Int32(Some(fl)),
    ];
    (cols, bytes)
}

/// Append the denormalized cells of `ds` and return the bytes they add.
fn push_denorm(
    cols: &mut Vec<Col>,
    ds: Dataset,
    m: &MetricRow,
    attrs: &[(String, Value)],
    cfg: &LakeConfig,
    stats: &mut ExtractStats,
) -> usize {
    let mut bytes = 0;
    for d in denorm_columns(ds, cfg) {
        let v = denorm_lookup(
            d,
            &m.descriptor_base.resource_attrs,
            &m.descriptor_base.scope_attrs,
            attrs,
            stats,
        );
        bytes += denorm_bytes(&v);
        cols.push(Col::from(v));
    }
    bytes
}

pub(crate) fn extract_metrics(
    records: &OtapArrowRecords,
    cfg: &LakeConfig,
    budget: &mut Budget,
) -> Result<Extracted> {
    let depth = cfg.ingress.max_nesting_depth;
    let resource_attrs = attr_table(records, ArrowPayloadType::ResourceAttrs, depth)?;
    let scope_attrs = attr_table(records, ArrowPayloadType::ScopeAttrs, depth)?;
    let metrics = metric_rows(records, &resource_attrs, &scope_attrs, cfg)?;
    let mut c = Common {
        cfg,
        metrics: &metrics,
        descriptors: Vec::new(),
        seen: HashSet::new(),
        memo: HashMap::new(),
        stats: ExtractStats::default(),
    };

    // Unsupported point kinds. `metric_rows` has already refused the request
    // under the reject policy, so reaching a non-empty payload here means drop.
    for pt in [
        ArrowPayloadType::ExpHistogramDataPoints,
        ArrowPayloadType::SummaryDataPoints,
    ] {
        if let Some(b) = records.get(pt)
            && b.num_rows() > 0
        {
            match cfg.unsupported {
                UnsupportedPolicy::Reject => {
                    return Err(Error::Refused(RefuseReason::Unsupported(format!("{pt:?}"))));
                }
                UnsupportedPolicy::Drop => c.stats.dropped_unsupported += b.num_rows() as u64,
            }
        }
    }

    // Exemplars are not part of the v1 format (spec 5.1). Count the rows we
    // drop; their own attribute payloads are ignored and not validated.
    for pt in [
        ArrowPayloadType::NumberDpExemplars,
        ArrowPayloadType::HistogramDpExemplars,
        ArrowPayloadType::ExpHistogramDpExemplars,
    ] {
        if let Some(b) = records.get(pt) {
            c.stats.dropped_exemplars += b.num_rows() as u64;
        }
    }

    let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let mut values = Vec::new();
    let mut pinned_bytes = 0;

    // Number points.
    if let Some(b) = records.get(ArrowPayloadType::NumberDataPoints) {
        let attrs = attr_table(records, ArrowPayloadType::NumberDpAttrs, depth)?;
        let parent = plain(b, "parent_id", &DataType::UInt16)?;
        let pid = plain(b, "id", &DataType::UInt32)?;
        let start = plain(b, "start_time_unix_nano", &ts_ns)?;
        let time = plain(b, "time_unix_nano", &ts_ns)?;
        let iv = plain(b, "int_value", &DataType::Int64)?;
        let dv = plain(b, "double_value", &DataType::Float64)?;
        let fl = plain(b, "flags", &DataType::UInt32)?;
        let mut sink = RowSink::new(Dataset::MetricsNumber, cfg)?;
        for row in 0..b.num_rows() {
            let metric_id = opt_u16_at(&parent, row)
                .ok_or_else(|| Error::invalid("number point without parent metric id"))?;
            let attrs_id = opt_u32_at(&pid, row);
            let point_attrs = match attrs_id {
                Some(id) => attrs.get(id),
                None => &[],
            };
            let id = c.series_for(metric_id, attrs_id, point_attrs, budget)?;
            let m = metric_of(&metrics, metric_id)?;
            let (mut cols, mut approx) = common_cols(
                id,
                m,
                cfg,
                i64_at(&time, row),
                i64_at(&start, row),
                flags(&fl, row),
                &mut c.stats,
            );
            cols.push(Col::Int(opt_i64(&iv, row)));
            cols.push(Col::Double(opt_f64(&dv, row)));
            approx += 16;
            approx += push_denorm(
                &mut cols,
                Dataset::MetricsNumber,
                m,
                point_attrs,
                cfg,
                &mut c.stats,
            );
            sink.push(
                &ValuesRow {
                    cols,
                    approx_bytes: approx,
                },
                budget,
            )?;
            c.stats.rows += 1;
        }
        let (batches, pinned) = sink.finish(budget)?;
        pinned_bytes += pinned;
        if !batches.is_empty() {
            values.push((Dataset::MetricsNumber, batches));
        }
    }

    // Histogram points.
    if let Some(b) = records.get(ArrowPayloadType::HistogramDataPoints) {
        let attrs = attr_table(records, ArrowPayloadType::HistogramDpAttrs, depth)?;
        let parent = plain(b, "parent_id", &DataType::UInt16)?;
        let pid = plain(b, "id", &DataType::UInt32)?;
        let start = plain(b, "start_time_unix_nano", &ts_ns)?;
        let time = plain(b, "time_unix_nano", &ts_ns)?;
        let count = plain(b, "count", &DataType::UInt64)?;
        let sum = plain(b, "sum", &DataType::Float64)?;
        let min = plain(b, "min", &DataType::Float64)?;
        let max = plain(b, "max", &DataType::Float64)?;
        let fl = plain(b, "flags", &DataType::UInt32)?;
        let bc = b.column_by_name("bucket_counts");
        let eb = b.column_by_name("explicit_bounds");
        let mut sink = RowSink::new(Dataset::MetricsHistogram, cfg)?;
        for row in 0..b.num_rows() {
            let metric_id = opt_u16_at(&parent, row)
                .ok_or_else(|| Error::invalid("histogram point without parent metric id"))?;
            let attrs_id = opt_u32_at(&pid, row);
            let point_attrs = match attrs_id {
                Some(id) => attrs.get(id),
                None => &[],
            };
            let id = c.series_for(metric_id, attrs_id, point_attrs, budget)?;
            let m = metric_of(&metrics, metric_id)?;
            let counts: Vec<i64> = match bc {
                Some(a) if a.is_valid(row) => {
                    let list = a.as_list::<i32>().value(row);
                    let vals = arrow::compute::cast(&list, &DataType::UInt64)?;
                    let mut out = Vec::with_capacity(vals.len());
                    for v in vals.as_primitive::<UInt64Type>().iter() {
                        let v = v.unwrap_or(0);
                        out.push(
                            i64::try_from(v)
                                .map_err(|_| Error::invalid("bucket count above i64::MAX"))?,
                        );
                    }
                    out
                }
                _ => vec![],
            };
            let bounds: Vec<f64> = match eb {
                Some(a) if a.is_valid(row) => {
                    let list = a.as_list::<i32>().value(row);
                    let vals = arrow::compute::cast(&list, &DataType::Float64)?;
                    vals.as_primitive::<Float64Type>()
                        .iter()
                        .map(|v| v.unwrap_or(0.0))
                        .collect()
                }
                _ => vec![],
            };
            let ok = (counts.is_empty() && bounds.is_empty()) || counts.len() == bounds.len() + 1;
            if !ok {
                return Err(Error::invalid(
                    "histogram bucket_counts.len != explicit_bounds.len + 1",
                ));
            }
            let cnt = count
                .as_ref()
                .and_then(|a| {
                    a.is_valid(row)
                        .then(|| a.as_primitive::<UInt64Type>().value(row))
                })
                .unwrap_or(0);
            let cnt =
                i64::try_from(cnt).map_err(|_| Error::invalid("histogram count above i64::MAX"))?;
            let (mut cols, mut approx) = common_cols(
                id,
                m,
                cfg,
                i64_at(&time, row),
                i64_at(&start, row),
                flags(&fl, row),
                &mut c.stats,
            );
            cols.push(Col::Int(Some(cnt)));
            cols.push(Col::Double(opt_f64(&sum, row)));
            cols.push(Col::Double(opt_f64(&min, row)));
            cols.push(Col::Double(opt_f64(&max, row)));
            approx += 32 + counts.len() * 8 + bounds.len() * 8 + 48;
            cols.push(Col::ListI64(counts));
            cols.push(Col::ListF64(bounds));
            approx += push_denorm(
                &mut cols,
                Dataset::MetricsHistogram,
                m,
                point_attrs,
                cfg,
                &mut c.stats,
            );
            sink.push(
                &ValuesRow {
                    cols,
                    approx_bytes: approx,
                },
                budget,
            )?;
            c.stats.rows += 1;
        }
        let (batches, pinned) = sink.finish(budget)?;
        pinned_bytes += pinned;
        if !batches.is_empty() {
            values.push((Dataset::MetricsHistogram, batches));
        }
    }

    // Descriptors of metrics that only had unsupported points are not emitted.
    let descriptors = c.descriptors;
    Ok(Extracted {
        signal: Signal::Metrics,
        descriptors,
        values,
        pinned_bytes,
        stats: c.stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LakeConfig, UnsupportedPolicy};
    use crate::error::{Error, RefuseReason};
    use crate::schema::Dataset;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
        Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum, Summary, SummaryDataPoint, metric, number_data_point,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_metrics;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        }
    }

    fn dp(t: u64, v: number_data_point::Value, attrs: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            time_unix_nano: t,
            value: Some(v),
            attributes: attrs,
            ..Default::default()
        }
    }

    fn data(metrics: Vec<Metric>) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn gauge_and_hist() -> MetricsData {
        data(vec![
            Metric {
                name: "cpu".into(),
                unit: "1".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![
                        dp(
                            10,
                            number_data_point::Value::AsInt(i64::MAX),
                            vec![kv("cpu", "0")],
                        ),
                        dp(
                            20,
                            number_data_point::Value::AsDouble(0.5),
                            vec![kv("cpu", "0")],
                        ),
                        dp(
                            30,
                            number_data_point::Value::AsDouble(0.7),
                            vec![kv("cpu", "1")],
                        ),
                    ],
                })),
                ..Default::default()
            },
            Metric {
                name: "lat".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    data_points: vec![HistogramDataPoint {
                        time_unix_nano: 40,
                        count: 3,
                        sum: Some(6.0),
                        bucket_counts: vec![1, 2],
                        explicit_bounds: vec![5.0],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ])
    }

    /// Scenario: a gauge with two series and a cumulative histogram.
    /// Guarantees: three descriptors, number rows keep int and double separately with
    /// INT64_MAX intact, histogram lists are stored as signed integers.
    #[test]
    fn extracts_number_and_histogram() {
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&encode_metrics(&gauge_and_hist()), &cfg, &mut budget)
            .expect("extract");
        assert_eq!(out.descriptors.len(), 3);
        let number = out
            .values
            .iter()
            .find(|(d, _)| *d == Dataset::MetricsNumber)
            .expect("number")
            .1[0]
            .clone();
        assert_eq!(number.num_rows(), 3);
        let vi = number
            .column_by_name("value_int")
            .expect("vi")
            .as_primitive::<Int64Type>();
        let vd = number
            .column_by_name("value_double")
            .expect("vd")
            .as_primitive::<Float64Type>();
        assert_eq!(vi.value(0), i64::MAX);
        assert!(vd.is_null(0));
        assert!(vi.is_null(1));
        assert_eq!(vd.value(1), 0.5);
        let ids = number
            .column_by_name("series_id")
            .expect("id")
            .as_fixed_size_binary();
        assert_eq!(ids.value(0), ids.value(1));
        assert_ne!(ids.value(0), ids.value(2));
        let name = number
            .column_by_name("metric_name")
            .expect("n")
            .as_string::<i32>();
        assert_eq!(name.value(0), "cpu");
        // Every remaining column of the number row 0, a point with time 10ns,
        // no start time, no flags and an integer value.
        assert_eq!(number.num_columns(), 10);
        assert_eq!(
            number
                .column_by_name("producer_id")
                .expect("p")
                .as_string::<i32>()
                .value(0),
            "h1"
        );
        assert_eq!(
            number
                .column_by_name("time")
                .expect("time")
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            0
        );
        assert_eq!(
            number
                .column_by_name("time_unix_nano")
                .expect("tns")
                .as_primitive::<Int64Type>()
                .value(0),
            10
        );
        assert!(number.column_by_name("start_time").expect("st").is_null(0));
        assert!(
            number
                .column_by_name("start_time_unix_nano")
                .expect("stns")
                .is_null(0)
        );
        assert_eq!(
            number
                .column_by_name("flags")
                .expect("f")
                .as_primitive::<Int32Type>()
                .value(0),
            0
        );
        let hist = out
            .values
            .iter()
            .find(|(d, _)| *d == Dataset::MetricsHistogram)
            .expect("hist")
            .1[0]
            .clone();
        assert_eq!(hist.num_rows(), 1);
        let bc = hist
            .column_by_name("bucket_counts")
            .expect("bc")
            .as_list::<i32>();
        assert_eq!(bc.value(0).as_primitive::<Int64Type>().values(), &[1i64, 2]);
        // Every remaining column of the single histogram row.
        assert_eq!(hist.num_columns(), 14);
        assert_eq!(
            hist.column_by_name("series_id")
                .expect("hid")
                .as_fixed_size_binary()
                .value(0)
                .len(),
            16
        );
        assert_eq!(
            hist.column_by_name("producer_id")
                .expect("hp")
                .as_string::<i32>()
                .value(0),
            "h1"
        );
        assert_eq!(
            hist.column_by_name("metric_name")
                .expect("hn")
                .as_string::<i32>()
                .value(0),
            "lat"
        );
        assert_eq!(
            hist.column_by_name("time")
                .expect("ht")
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            0
        );
        assert_eq!(
            hist.column_by_name("time_unix_nano")
                .expect("htns")
                .as_primitive::<Int64Type>()
                .value(0),
            40
        );
        assert!(hist.column_by_name("start_time").expect("hst").is_null(0));
        assert!(
            hist.column_by_name("start_time_unix_nano")
                .expect("hstns")
                .is_null(0)
        );
        assert_eq!(
            hist.column_by_name("flags")
                .expect("hf")
                .as_primitive::<Int32Type>()
                .value(0),
            0
        );
        assert_eq!(
            hist.column_by_name("count")
                .expect("hc")
                .as_primitive::<Int64Type>()
                .value(0),
            3
        );
        assert_eq!(
            hist.column_by_name("sum")
                .expect("hs")
                .as_primitive::<Float64Type>()
                .value(0),
            6.0
        );
        assert!(hist.column_by_name("min").expect("hmin").is_null(0));
        assert!(hist.column_by_name("max").expect("hmax").is_null(0));
        let eb = hist
            .column_by_name("explicit_bounds")
            .expect("eb")
            .as_list::<i32>();
        assert_eq!(
            eb.value(0).as_primitive::<Float64Type>().values(),
            &[5.0f64]
        );
        // The histogram descriptor keeps its cumulative temporality and is not
        // marked monotonic.
        let h = out
            .descriptors
            .iter()
            .find(|d| {
                d.descriptor
                    .metric
                    .as_ref()
                    .is_some_and(|m| m.kind == MetricKind::Histogram)
            })
            .expect("histogram descriptor")
            .descriptor
            .metric
            .as_ref()
            .expect("m");
        assert_eq!(h.temporality, Temporality::Cumulative);
        assert!(!h.is_monotonic);
        let kinds: Vec<_> = out
            .descriptors
            .iter()
            .map(|d| d.descriptor.metric.as_ref().expect("m").kind)
            .collect();
        assert!(kinds.contains(&MetricKind::Histogram));
    }

    /// Scenario: a sum without temporality.
    /// Guarantees: the request is refused as invalid.
    #[test]
    fn unspecified_temporality_is_refused() {
        let md = data(vec![Metric {
            name: "s".into(),
            data: Some(metric::Data::Sum(Sum {
                aggregation_temporality: AggregationTemporality::Unspecified as i32,
                is_monotonic: true,
                data_points: vec![dp(1, number_data_point::Value::AsInt(1), vec![])],
            })),
            ..Default::default()
        }]);
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        assert!(matches!(
            extract_metrics(&encode_metrics(&md), &cfg, &mut budget),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Scenario: an exponential histogram under reject and under drop.
    /// Guarantees: reject refuses the whole request; drop yields zero rows and counts one drop.
    #[test]
    fn exp_histogram_policy() {
        let md = data(vec![Metric {
            name: "e".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1,
                    count: 1,
                    ..Default::default()
                }],
            })),
            ..Default::default()
        }]);
        let records = encode_metrics(&md);
        let reject = LakeConfig::default();
        let mut budget = Budget::new(&reject);
        assert!(matches!(
            extract_metrics(&records, &reject, &mut budget),
            Err(Error::Refused(RefuseReason::Unsupported(_)))
        ));
        let cfg = LakeConfig {
            unsupported: UnsupportedPolicy::Drop,
            ..Default::default()
        };
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&records, &cfg, &mut budget).expect("drop");
        assert_eq!(out.stats.rows, 0);
        assert_eq!(out.stats.dropped_unsupported, 1);
        assert!(out.values.is_empty());
    }

    /// Scenario: a gauge and a summary in the same request under the drop policy.
    /// Guarantees: the summary never reaches temporality validation (pdata supplies
    /// no temporality for summaries), the request succeeds, the gauge rows survive
    /// and the summary points are counted as dropped.
    #[test]
    fn summary_is_dropped_without_failing_temporality_validation() {
        let md = data(vec![
            Metric {
                name: "cpu".into(),
                unit: "1".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![dp(
                        10,
                        number_data_point::Value::AsDouble(0.5),
                        vec![kv("cpu", "0")],
                    )],
                })),
                ..Default::default()
            },
            Metric {
                name: "q".into(),
                data: Some(metric::Data::Summary(Summary {
                    data_points: vec![SummaryDataPoint {
                        time_unix_nano: 20,
                        count: 2,
                        sum: 4.0,
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ]);
        let records = encode_metrics(&md);
        let cfg = LakeConfig {
            unsupported: UnsupportedPolicy::Drop,
            ..Default::default()
        };
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&records, &cfg, &mut budget).expect("drop");
        assert_eq!(out.stats.rows, 1);
        assert_eq!(out.stats.dropped_unsupported, 1);
        assert_eq!(out.descriptors.len(), 1);
        let number = out
            .values
            .iter()
            .find(|(d, _)| *d == Dataset::MetricsNumber)
            .expect("number");
        assert_eq!(number.1[0].num_rows(), 1);
    }

    /// Scenario: a histogram whose bucket_counts length is not bounds + 1.
    /// Guarantees: refused as invalid.
    #[test]
    fn inconsistent_histogram_is_refused() {
        let md = data(vec![Metric {
            name: "h".into(),
            data: Some(metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: vec![HistogramDataPoint {
                    count: 1,
                    bucket_counts: vec![1, 1, 1],
                    explicit_bounds: vec![1.0],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        }]);
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        assert!(matches!(
            extract_metrics(&encode_metrics(&md), &cfg, &mut budget),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }
}
