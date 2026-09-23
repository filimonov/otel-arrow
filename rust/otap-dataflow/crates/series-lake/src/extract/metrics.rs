// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics extraction: number and histogram points (FORMAT.md section 2).
//!
//! Both point kinds write into the single `metrics/values` dataset through one
//! [`RowSink`], each leaving the other kind's columns null. The point kind is
//! not stored in the row: readers take it from `metric_type` in the series
//! descriptor through the join of FORMAT.md section 6.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef, AsArray, ListArray};
use arrow::datatypes::{
    DataType, Float64Type, Int32Type, Int64Type, TimeUnit, TimestampNanosecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otlp::metrics::MetricType;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::AggregationTemporality;
use otel_arrow_dfe_pdata::schema::consts::{
    AGGREGATION_TEMPORALITY, DESCRIPTION, DOUBLE_VALUE, FLAGS, HISTOGRAM_BUCKET_COUNTS,
    HISTOGRAM_COUNT, HISTOGRAM_EXPLICIT_BOUNDS, HISTOGRAM_MAX, HISTOGRAM_MIN, HISTOGRAM_SUM, ID,
    INT_VALUE, IS_MONOTONIC, METRIC_TYPE, NAME, PARENT_ID, RESOURCE, SCHEMA_URL, SCOPE,
    START_TIME_UNIX_NANO, TIME_UNIX_NANO, UNIT, VERSION,
};

use super::{
    Budget, Col, DescriptorRow, ExtractStats, Extracted, RowSink, SharedLists, ValuesRow,
    attr_table, attrs_of, denorm_bytes, denorm_lookup, descriptor_row, flags_at, identity, plain,
    producer_id, str_at, struct_child, timestamp_pair, typed_col,
};
use crate::attrs::{AttrTable, bool_at, prim_at};
use crate::canonical::{
    Descriptor, MetricDescriptor, MetricKind, SeriesId, Signal, Temporality, canonical_double_bits,
};
use crate::config::{ExemplarPolicy, LakeConfig, UnsupportedPolicy};
use crate::error::{Error, RefuseReason, Result};
use crate::schema::{Dataset, denorm_columns};
use crate::value::{DecodeLimits, Value};

/// Per-metric fields read once from the `UnivariateMetrics` batch.
///
/// The resource and scope attribute *ids* are kept, not their attribute lists:
/// many metrics of one request share a single resource, and copying that list
/// per metric here would allocate a multiple of the request's attribute volume
/// before any [`Budget`] charge could refuse it. The lists are resolved,
/// charged and copied once per request, by [`SharedLists`], the first time a
/// new series needs them, and shared by every descriptor after that.
struct MetricRow {
    /// Resource attribute parent id, `None` when the metric carries none.
    resource_id: Option<u32>,
    /// Scope attribute parent id, `None` when the metric carries none.
    scope_id: Option<u32>,
    resource_schema_url: String,
    scope_name: String,
    scope_version: String,
    scope_schema_url: String,
    metric: MetricDescriptor,
}

/// Memo key for a metrics series: the metric and the *content* of the point's
/// attributes.
///
/// Not the point's attribute parent id: pdata gives every point its own id,
/// so two points of one series never share it, and a memo keyed on it would
/// never hit. The attribute list is borrowed from the point kind's attribute
/// table, which lives as long as that kind's loop, so the memo is one per
/// point kind. A metric has exactly one kind, so no series spans the two
/// memos. Equality and hashing both compare doubles by
/// [`canonical_double_bits`], the bits the canonical encoding writes, so two
/// lists are one key exactly when they encode to the same identity: `0.0`
/// and `-0.0` are one key, every NaN is one key and equals itself, and a hash
/// collision can never merge two series because equality is exact.
#[derive(Debug, Clone, Copy)]
struct MemoKey<'t> {
    metric_id: u32,
    attrs: &'t [(String, Value)],
}

impl PartialEq for MemoKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.metric_id == other.metric_id && kv_eq(self.attrs, other.attrs)
    }
}

/// Attribute lists equal under the canonical encoding's value rules.
fn kv_eq(a: &[(String, Value)], b: &[(String, Value)]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((ka, va), (kb, vb))| ka == kb && value_eq(va, vb))
}

/// Values equal under the canonical encoding's value rules: doubles by
/// [`canonical_double_bits`], so the relation is reflexive for NaN and
/// treats both zeros as one value, exactly as [`hash_value`] hashes them.
fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Bytes(x), Value::Bytes(y)) => x == y,
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Double(x), Value::Double(y)) => {
            canonical_double_bits(*x) == canonical_double_bits(*y)
        }
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| value_eq(p, q))
        }
        (Value::KvList(x), Value::KvList(y)) => kv_eq(x, y),
        _ => false,
    }
}

impl Eq for MemoKey<'_> {}

impl std::hash::Hash for MemoKey<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.metric_id.hash(state);
        self.attrs.len().hash(state);
        for (key, value) in self.attrs {
            key.hash(state);
            hash_value(value, state);
        }
    }
}

/// Hash one attribute value consistently with [`value_eq`]: equal values hash
/// alike, a double by its canonical bits.
fn hash_value<H: std::hash::Hasher>(value: &Value, state: &mut H) {
    use std::hash::Hash;
    match value {
        Value::Null => 0_u8.hash(state),
        Value::Str(s) => {
            1_u8.hash(state);
            s.hash(state);
        }
        Value::Bytes(b) => {
            2_u8.hash(state);
            b.hash(state);
        }
        Value::Int(i) => {
            3_u8.hash(state);
            i.hash(state);
        }
        Value::Double(d) => {
            4_u8.hash(state);
            canonical_double_bits(*d).hash(state);
        }
        Value::Bool(b) => {
            5_u8.hash(state);
            b.hash(state);
        }
        Value::Array(items) => {
            6_u8.hash(state);
            items.len().hash(state);
            for item in items {
                hash_value(item, state);
            }
        }
        Value::KvList(entries) => {
            7_u8.hash(state);
            entries.len().hash(state);
            for (key, value) in entries {
                key.hash(state);
                hash_value(value, state);
            }
        }
    }
}

/// The metrics of a `UnivariateMetrics` batch by id; none when it is absent.
fn metric_rows(
    m: Option<&arrow::record_batch::RecordBatch>,
    cfg: &LakeConfig,
) -> Result<HashMap<u32, MetricRow>> {
    let Some(m) = m else {
        return Ok(HashMap::new());
    };
    let id = plain(m, ID, &DataType::UInt16)?;
    let kind = plain(m, METRIC_TYPE, &DataType::UInt8)?;
    let name = plain(m, NAME, &DataType::Utf8)?;
    let temporality = plain(m, AGGREGATION_TEMPORALITY, &DataType::Int32)?;
    let description = plain(m, DESCRIPTION, &DataType::Utf8)?;
    let is_monotonic = plain(m, IS_MONOTONIC, &DataType::Boolean)?;
    let unit = plain(m, UNIT, &DataType::Utf8)?;
    let scope_schema = plain(m, SCHEMA_URL, &DataType::Utf8)?;
    let res_id = struct_child(m, RESOURCE, ID, &DataType::UInt16)?;
    let res_schema = struct_child(m, RESOURCE, SCHEMA_URL, &DataType::Utf8)?;
    let scope_id = struct_child(m, SCOPE, ID, &DataType::UInt16)?;
    let scope_name = struct_child(m, SCOPE, NAME, &DataType::Utf8)?;
    let scope_version = struct_child(m, SCOPE, VERSION, &DataType::Utf8)?;
    let mut out = HashMap::with_capacity(m.num_rows());
    let mut ids = HashSet::with_capacity(m.num_rows());
    for row in 0..m.num_rows() {
        // A repeated id would put one metric's points under another's series.
        let metric_id = prim_at::<UInt16Type>(&id, row)
            .map(u32::from)
            .ok_or_else(|| Error::invalid("metric row without id"))?;
        if !ids.insert(metric_id) {
            return Err(Error::invalid(format!("duplicate metric id {metric_id}")));
        }
        let kind_u8 = prim_at::<UInt8Type>(&kind, row)
            .ok_or_else(|| Error::invalid("metric row without metric_type"))?;
        // pdata already owns the OTAP metric type tags; reuse its enum rather
        // than a second copy of the same numbering. `Empty` has no kind of its
        // own and is refused exactly like an unknown tag.
        let kind = match MetricType::try_from(kind_u8) {
            Ok(MetricType::Gauge) => MetricKind::Gauge,
            Ok(MetricType::Sum) => MetricKind::Sum,
            Ok(MetricType::Histogram) => MetricKind::Histogram,
            Ok(MetricType::ExponentialHistogram) => MetricKind::ExpHistogram,
            Ok(MetricType::Summary) => MetricKind::Summary,
            Ok(MetricType::Empty) | Err(_) => {
                return Err(Error::invalid(format!("metric_type {kind_u8}")));
            }
        };
        // Decoded through the proto enum, so an unknown value is unspecified
        // by the enum's own definition rather than by a local literal.
        let temporality =
            match prim_at::<Int32Type>(&temporality, row).map(AggregationTemporality::try_from) {
                Some(Ok(AggregationTemporality::Delta)) => Temporality::Delta,
                Some(Ok(AggregationTemporality::Cumulative)) => Temporality::Cumulative,
                Some(Ok(AggregationTemporality::Unspecified) | Err(_)) | None => {
                    Temporality::Unspecified
                }
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
        let row_out = MetricRow {
            resource_id: prim_at::<UInt16Type>(&res_id, row).map(u32::from),
            scope_id: prim_at::<UInt16Type>(&scope_id, row).map(u32::from),
            resource_schema_url: str_at(&res_schema, row),
            scope_name: str_at(&scope_name, row),
            scope_version: str_at(&scope_version, row),
            scope_schema_url: str_at(&scope_schema, row),
            metric: MetricDescriptor {
                name: str_at(&name, row),
                unit: str_at(&unit, row),
                kind,
                temporality: if kind == MetricKind::Gauge {
                    Temporality::Unspecified
                } else {
                    temporality
                },
                is_monotonic: kind == MetricKind::Sum
                    && bool_at(&is_monotonic, row).unwrap_or(false),
                description: str_at(&description, row),
            },
        };
        let _ = out.insert(metric_id, row_out);
    }
    Ok(out)
}

struct Common<'a> {
    cfg: &'a LakeConfig,
    metrics: &'a HashMap<u32, MetricRow>,
    resource_attrs: &'a AttrTable,
    scope_attrs: &'a AttrTable,
    /// Resource lists shared by the request's descriptors, one per parent.
    resources: SharedLists,
    /// Scope lists shared by the request's descriptors, one per parent.
    scopes: SharedLists,
    descriptors: Vec<DescriptorRow>,
    seen: HashSet<SeriesId>,
    stats: ExtractStats,
    /// [`LakeConfig::series_columns`] of metrics.
    series_columns: usize,
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
    /// Series id for (metric, point attrs), memoized on content before any
    /// encoding or hashing.
    ///
    /// Without the memo, `canonical_bytes` plus XXH3 would run once per data
    /// point instead of once per series. On a miss the identity is computed
    /// from a descriptor whose resource and scope lists are the request's
    /// shared copies, and it is checked against the series already held
    /// before the row is built: a duplicate is neither built nor charged, and
    /// its denormalized columns are not looked up, so
    /// `stats.denorm_type_mismatch` counts series rather than points.
    fn series_for<'t>(
        &mut self,
        memo: &mut HashMap<MemoKey<'t>, SeriesId>,
        metric_id: u32,
        attrs: &'t [(String, Value)],
        budget: &mut Budget,
    ) -> Result<SeriesId> {
        let key = MemoKey { metric_id, attrs };
        if let Some(id) = memo.get(&key) {
            self.stats.series_memo_hits += 1;
            return Ok(*id);
        }
        self.stats.series_memo_misses += 1;
        let m = metric_of(self.metrics, metric_id)?;
        let resource_attrs = self
            .resources
            .get(self.resource_attrs, m.resource_id, budget)?;
        let scope_attrs = self.scopes.get(self.scope_attrs, m.scope_id, budget)?;
        let d = Descriptor {
            signal: Signal::Metrics,
            resource_attrs,
            resource_schema_url: m.resource_schema_url.clone(),
            scope_name: m.scope_name.clone(),
            scope_version: m.scope_version.clone(),
            scope_schema_url: m.scope_schema_url.clone(),
            scope_attrs,
            metric: Some(m.metric.clone()),
            attrs: attrs.to_vec(),
        };
        let identified = identity(&d);
        let id = identified.1;
        if self.seen.insert(id) {
            let dr = descriptor_row(
                d,
                identified,
                Dataset::MetricsSeries,
                self.series_columns,
                self.cfg,
                &mut self.stats,
                budget,
            )?;
            self.descriptors.push(dr);
        }
        let _ = memo.insert(key, id);
        Ok(id)
    }

    /// Append one values row per point of one point kind.
    ///
    /// The one path number and histogram points share. Each point is resolved
    /// to its metric and its series through [`Common::series_for`], memoized
    /// on content for this kind's attribute table; its resource, scope and
    /// point attributes are looked up; the shared leading columns and the
    /// denormalized columns are built around the kind's own; and the row is
    /// charged to the request's budget as it enters the sink.
    ///
    /// `kind` returns the eight per-kind columns of one point -- the other
    /// kind's columns null, not zero -- and the bytes they retain beyond
    /// [`PER_KIND_FIXED_BYTES`]. It runs once the point's series is known and
    /// may refuse the request.
    #[allow(clippy::too_many_arguments)]
    fn append_points(
        &mut self,
        rows: usize,
        columns: &PointColumns,
        attrs: &AttrTable,
        orphan: &'static str,
        sink: &mut RowSink,
        budget: &mut Budget,
        mut kind: impl FnMut(usize) -> Result<([Col; 8], usize)>,
    ) -> Result<()> {
        let metrics = self.metrics;
        let (resource_attrs, scope_attrs) = (self.resource_attrs, self.scope_attrs);
        let mut memo = HashMap::new();
        for row in 0..rows {
            let metric_id = prim_at::<UInt16Type>(&columns.parent, row)
                .map(u32::from)
                .ok_or_else(|| Error::invalid(orphan))?;
            let point_attrs = match prim_at::<UInt32Type>(&columns.attrs_id, row) {
                Some(id) => attrs.get(id),
                None => &[],
            };
            let id = self.series_for(&mut memo, metric_id, point_attrs, budget)?;
            let m = metric_of(metrics, metric_id)?;
            let (kind_cols, kind_bytes) = kind(row)?;
            let resource = attrs_of(resource_attrs, m.resource_id);
            let scope = attrs_of(scope_attrs, m.scope_id);
            let (mut cols, mut approx) = common_cols(
                id,
                m,
                resource,
                self.cfg,
                prim_at::<TimestampNanosecondType>(&columns.time, row).unwrap_or(0),
                prim_at::<TimestampNanosecondType>(&columns.start, row).unwrap_or(0),
                flags_at(&columns.flags, row),
                &mut self.stats,
            );
            cols.extend(kind_cols);
            approx += PER_KIND_FIXED_BYTES + kind_bytes;
            approx += push_denorm(
                &mut cols,
                Dataset::MetricsValues,
                resource,
                scope,
                point_attrs,
                self.cfg,
                &mut self.stats,
            );
            sink.push(
                &ValuesRow {
                    cols,
                    approx_bytes: approx,
                },
                budget,
            )?;
            self.stats.rows += 1;
        }
        Ok(())
    }
}

/// The columns of a point table that every point kind reads besides its own.
struct PointColumns {
    /// Parent metric id of each point.
    parent: Option<ArrayRef>,
    /// Point attribute parent id of each point.
    attrs_id: Option<ArrayRef>,
    /// `start_time_unix_nano`.
    start: Option<ArrayRef>,
    /// `time_unix_nano`.
    time: Option<ArrayRef>,
    /// `flags`.
    flags: Option<ArrayRef>,
}

/// One row of `bucket_counts`, cast to the signed storage type.
///
/// A null element would silently become a bucket of 0, which is a different
/// histogram, so it refuses the request instead. An absent or null list row is
/// an empty list, which is the "no buckets" case.
fn bucket_counts_at(a: &Option<ListArray>, row: usize) -> Result<Vec<i64>> {
    let Some(list) = a.as_ref().filter(|a| a.is_valid(row)).map(|a| a.value(row)) else {
        return Ok(vec![]);
    };
    let vals = arrow::compute::cast(&list, &DataType::UInt64)?;
    let mut out = Vec::with_capacity(vals.len());
    for v in vals.as_primitive::<UInt64Type>().iter() {
        let v = v.ok_or_else(|| Error::invalid("null bucket count"))?;
        out.push(i64::try_from(v).map_err(|_| Error::invalid("bucket count above i64::MAX"))?);
    }
    Ok(out)
}

/// One row of `explicit_bounds`.
///
/// A null element would silently become a bound of 0.0, which is a different
/// histogram, so it refuses the request instead.
fn explicit_bounds_at(a: &Option<ListArray>, row: usize) -> Result<Vec<f64>> {
    let Some(list) = a.as_ref().filter(|a| a.is_valid(row)).map(|a| a.value(row)) else {
        return Ok(vec![]);
    };
    let vals = arrow::compute::cast(&list, &DataType::Float64)?;
    vals.as_primitive::<Float64Type>()
        .iter()
        .map(|v| v.ok_or_else(|| Error::invalid("null explicit bound")))
        .collect()
}

/// Bytes a merged values row's fixed per-kind cells retain: the two value
/// columns, the four histogram scalars and the two list cells' overhead. Every
/// row pays them because every row carries all fourteen columns, null or not.
const PER_KIND_FIXED_BYTES: usize = 16 + 32 + 48;

/// The eight leading columns shared by both point kinds, plus their bytes.
fn common_cols(
    id: SeriesId,
    m: &MetricRow,
    resource: &[(String, Value)],
    cfg: &LakeConfig,
    t_ns: i64,
    s_ns: i64,
    fl: i32,
    stats: &mut ExtractStats,
) -> (Vec<Col>, usize) {
    let (t_ns, t_us) = timestamp_pair(t_ns, stats);
    let (s_ns, s_us) = timestamp_pair(s_ns, stats);
    let producer = producer_id(resource, &cfg.producer_id_attribute);
    let name = m.metric.name.clone();
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
    resource: &[(String, Value)],
    scope: &[(String, Value)],
    attrs: &[(String, Value)],
    cfg: &LakeConfig,
    stats: &mut ExtractStats,
) -> usize {
    let mut bytes = 0;
    for d in denorm_columns(ds, cfg) {
        let v = denorm_lookup(d, resource, scope, attrs, stats);
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
    let limits = DecodeLimits::new(cfg.ingress.max_nesting_depth, cfg.ingress.max_row_bytes);
    let resource_attrs = attr_table(records, ArrowPayloadType::ResourceAttrs, limits, budget)?;
    let scope_attrs = attr_table(records, ArrowPayloadType::ScopeAttrs, limits, budget)?;
    let metrics = metric_rows(records.get(ArrowPayloadType::UnivariateMetrics), cfg)?;
    let mut c = Common {
        cfg,
        metrics: &metrics,
        resource_attrs: &resource_attrs,
        scope_attrs: &scope_attrs,
        resources: SharedLists::default(),
        scopes: SharedLists::default(),
        descriptors: Vec::new(),
        seen: HashSet::new(),
        stats: ExtractStats::default(),
        series_columns: cfg.series_columns(Signal::Metrics),
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
                UnsupportedPolicy::Drop => {
                    let rows = b.num_rows() as u64;
                    c.stats.dropped_unsupported += rows;
                    match pt {
                        ArrowPayloadType::ExpHistogramDataPoints => {
                            c.stats.dropped_exp_histogram += rows;
                        }
                        ArrowPayloadType::SummaryDataPoints => c.stats.dropped_summary += rows,
                        _ => unreachable!("loop contains only the two unsupported point tables"),
                    }
                }
            }
        }
    }

    // Exemplars are not part of the v1 format (FORMAT.md section 2). An explicit
    // `metrics.exemplars: reject` refuses a request whose stored points carry
    // any; otherwise, the default, the rows are counted as dropped. An exemplar of an exponential histogram point goes
    // with that point, which the drop policy has already discarded, so it is
    // counted but never refuses the request. Their own attribute payloads are
    // ignored and not validated.
    for pt in [
        ArrowPayloadType::NumberDpExemplars,
        ArrowPayloadType::HistogramDpExemplars,
        ArrowPayloadType::ExpHistogramDpExemplars,
    ] {
        if let Some(b) = records.get(pt)
            && b.num_rows() > 0
        {
            if pt != ArrowPayloadType::ExpHistogramDpExemplars
                && cfg.exemplar_policy() == ExemplarPolicy::Reject
            {
                return Err(Error::Refused(RefuseReason::Unsupported(
                    "exemplars".to_string(),
                )));
            }
            c.stats.dropped_exemplars += b.num_rows() as u64;
        }
    }

    let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let mut values = Vec::new();
    let mut pinned_bytes = 0;
    // Number and histogram points share one dataset (FORMAT.md section 2), so they share
    // one sink; each kind writes the other's columns as null.
    let mut sink = RowSink::new(Dataset::MetricsValues, cfg)?;

    // Number points. Each column is read in the order the kinds always read
    // them, so a request with several malformed columns is refused for the
    // same one.
    if let Some(b) = records.get(ArrowPayloadType::NumberDataPoints) {
        let attrs = attr_table(records, ArrowPayloadType::NumberDpAttrs, limits, budget)?;
        let parent = plain(b, PARENT_ID, &DataType::UInt16)?;
        let attrs_id = plain(b, ID, &DataType::UInt32)?;
        let start = plain(b, START_TIME_UNIX_NANO, &ts_ns)?;
        let time = plain(b, TIME_UNIX_NANO, &ts_ns)?;
        let iv = plain(b, INT_VALUE, &DataType::Int64)?;
        let dv = plain(b, DOUBLE_VALUE, &DataType::Float64)?;
        let flags = plain(b, FLAGS, &DataType::UInt32)?;
        let columns = PointColumns {
            parent,
            attrs_id,
            start,
            time,
            flags,
        };
        c.append_points(
            b.num_rows(),
            &columns,
            &attrs,
            "number point without parent metric id",
            &mut sink,
            budget,
            |row| {
                Ok((
                    [
                        Col::Int(prim_at::<Int64Type>(&iv, row)),
                        Col::Double(prim_at::<Float64Type>(&dv, row)),
                        // Histogram columns of a number point: null, not zero.
                        Col::Int(None),
                        Col::Double(None),
                        Col::Double(None),
                        Col::Double(None),
                        Col::ListI64(None),
                        Col::ListF64(None),
                    ],
                    0,
                ))
            },
        )?;
        attrs.release(budget);
    }

    // Histogram points.
    if let Some(b) = records.get(ArrowPayloadType::HistogramDataPoints) {
        let attrs = attr_table(records, ArrowPayloadType::HistogramDpAttrs, limits, budget)?;
        let parent = plain(b, PARENT_ID, &DataType::UInt16)?;
        let attrs_id = plain(b, ID, &DataType::UInt32)?;
        let start = plain(b, START_TIME_UNIX_NANO, &ts_ns)?;
        let time = plain(b, TIME_UNIX_NANO, &ts_ns)?;
        let count = plain(b, HISTOGRAM_COUNT, &DataType::UInt64)?;
        let sum = plain(b, HISTOGRAM_SUM, &DataType::Float64)?;
        let min = plain(b, HISTOGRAM_MIN, &DataType::Float64)?;
        let max = plain(b, HISTOGRAM_MAX, &DataType::Float64)?;
        let flags = plain(b, FLAGS, &DataType::UInt32)?;
        let bc = typed_col::<ListArray>(b, HISTOGRAM_BUCKET_COUNTS, "a list")?.cloned();
        let eb = typed_col::<ListArray>(b, HISTOGRAM_EXPLICIT_BOUNDS, "a list")?.cloned();
        let columns = PointColumns {
            parent,
            attrs_id,
            start,
            time,
            flags,
        };
        c.append_points(
            b.num_rows(),
            &columns,
            &attrs,
            "histogram point without parent metric id",
            &mut sink,
            budget,
            |row| {
                let counts = bucket_counts_at(&bc, row)?;
                let bounds = explicit_bounds_at(&eb, row)?;
                let ok =
                    (counts.is_empty() && bounds.is_empty()) || counts.len() == bounds.len() + 1;
                if !ok {
                    return Err(Error::invalid(
                        "histogram bucket_counts.len != explicit_bounds.len + 1",
                    ));
                }
                let cnt = prim_at::<UInt64Type>(&count, row).unwrap_or(0);
                let cnt = i64::try_from(cnt)
                    .map_err(|_| Error::invalid("histogram count above i64::MAX"))?;
                let bytes = counts.len() * 8 + bounds.len() * 8;
                Ok((
                    [
                        // Value columns of a histogram point: null, not zero.
                        Col::Int(None),
                        Col::Double(None),
                        Col::Int(Some(cnt)),
                        Col::Double(prim_at::<Float64Type>(&sum, row)),
                        Col::Double(prim_at::<Float64Type>(&min, row)),
                        Col::Double(prim_at::<Float64Type>(&max, row)),
                        Col::ListI64(Some(counts)),
                        Col::ListF64(Some(bounds)),
                    ],
                    bytes,
                ))
            },
        )?;
        attrs.release(budget);
    }

    // One sink for both point kinds, so a mixed request still produces a single
    // values file. Number rows precede histogram rows within the request.
    let (batches, pinned) = sink.finish(budget)?;
    pinned_bytes += pinned;
    if !batches.is_empty() {
        values.push((Dataset::MetricsValues, batches));
    }

    // Descriptors of metrics that only had unsupported points are not emitted.
    let descriptors = c.descriptors;
    Ok(Extracted {
        signal: Signal::Metrics,
        descriptors,
        values,
        pinned_bytes,
        shared_bytes: c.resources.bytes() + c.scopes.bytes(),
        stats: c.stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExemplarPolicy, LakeConfig, UnsupportedPolicy};
    use crate::error::{Error, RefuseReason};
    use crate::schema::Dataset;
    use arrow::array::{Array, AsArray, Float64Builder, ListBuilder, UInt64Builder};
    use arrow::datatypes::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint,
        Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, exemplar, metric,
        number_data_point,
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

    /// One exemplar carrying filtered attributes of its own, so that the
    /// encoder emits both an exemplar payload and an exemplar attribute
    /// payload. Neither is stored by the v1 format.
    fn ex(t: u64) -> Exemplar {
        Exemplar {
            time_unix_nano: t,
            value: Some(exemplar::Value::AsInt(7)),
            filtered_attributes: vec![kv("exemplar.only", "x")],
            span_id: vec![0xBB; 8],
            trace_id: vec![0xAA; 16],
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
                        NumberDataPoint {
                            exemplars: vec![ex(9)],
                            ..dp(
                                10,
                                number_data_point::Value::AsInt(i64::MAX),
                                vec![kv("cpu", "0")],
                            )
                        },
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
                        exemplars: vec![ex(39)],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ])
    }

    /// Many metrics under one resource whose single attribute is `bytes` long.
    fn many_metrics_one_big_resource(count: usize, bytes: usize) -> MetricsData {
        let metrics = (0..count)
            .map(|i| Metric {
                name: format!("m{i}"),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![dp(10, number_data_point::Value::AsInt(1), vec![])],
                })),
                ..Default::default()
            })
            .collect();
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("big", &"x".repeat(bytes))],
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

    /// Scenario: forty metrics share one resource carrying a 64 KiB attribute,
    /// so the descriptors together copy roughly 2.5 MiB of attributes.
    /// Guarantees: the copies are charged to the extracted budget, one per
    /// distinct series, and nothing is copied before the charge. Under the
    /// default 32 MiB budget all forty descriptors are produced; under a 1 MiB
    /// budget the request is refused as too large instead of allocating forty
    /// copies first.
    #[test]
    fn shared_resource_attributes_are_charged_before_they_are_copied() {
        const METRICS: usize = 40;
        const ATTR_BYTES: usize = 64 << 10;
        let d = many_metrics_one_big_resource(METRICS, ATTR_BYTES);

        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        let records = encode_metrics(&d);
        let out = extract_metrics(&records, &cfg, &mut budget).expect("within the default budget");
        assert_eq!(out.descriptors.len(), METRICS);

        let mut small = LakeConfig::default();
        small.ingress.max_extracted_bytes = 1 << 20;
        let mut budget = Budget::new(&small);
        let records = encode_metrics(&d);
        assert!(matches!(
            extract_metrics(&records, &small, &mut budget),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: one metric under a resource carrying a 256 KiB attribute,
    /// with `max_extracted_bytes` set just above what the request actually
    /// retains: its attribute tables, its descriptor rows, its values and its
    /// one shared resource copy.
    /// Guarantees: the request is accepted. The shared copy is charged once,
    /// when it is made, and the descriptor row that holds it charges only its
    /// own rendering of it, so the two are never counted together and a
    /// request that fits its budget is not refused transiently.
    #[test]
    fn the_shared_resource_copy_is_charged_once() {
        // Large enough that the copy outweighs the fixed Arrow builder
        // overhead the values batch adds at the end, so the descriptor charge
        // is the peak the limit below binds on.
        const ATTR_BYTES: usize = 256 << 10;
        let d = many_metrics_one_big_resource(1, ATTR_BYTES);

        // Measure what the request retains, under a budget that cannot bind.
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&encode_metrics(&d), &cfg, &mut budget).expect("measure");
        let tables = crate::extract::tests::table_bytes(
            &encode_metrics(&d),
            &cfg,
            &[
                ArrowPayloadType::ResourceAttrs,
                ArrowPayloadType::ScopeAttrs,
            ],
        );
        let retained: usize = out
            .descriptors
            .iter()
            .map(|r| r.approx_bytes)
            .sum::<usize>()
            + out.pinned_bytes
            + out.shared_bytes
            + tables;
        // The premise: the copy alone is a large fraction of the retained size,
        // so counting it twice would take the request past the limit below.
        assert!(retained > ATTR_BYTES);

        let mut tight = LakeConfig::default();
        tight.ingress.max_extracted_bytes = retained + 4096;
        let mut budget = Budget::new(&tight);
        let out = extract_metrics(&encode_metrics(&d), &tight, &mut budget)
            .expect("a request that fits its budget is accepted");
        assert_eq!(out.descriptors.len(), 1);
    }

    /// Scenario: a gauge with two series and a cumulative histogram, each
    /// point kind carrying one exemplar, under `unsupported: drop`.
    /// Guarantees: three descriptors, number rows keep int and double separately with
    /// INT64_MAX intact, histogram lists are stored as signed integers, and
    /// both exemplars are dropped and counted.
    #[test]
    fn extracts_number_and_histogram() {
        let cfg = LakeConfig {
            unsupported: UnsupportedPolicy::Drop,
            ..LakeConfig::default()
        };
        let mut budget = Budget::new(&cfg);
        let records = encode_metrics(&gauge_and_hist());
        // The assertion on dropped_exemplars below is only meaningful if the
        // encoder really produced the exemplar payloads.
        for pt in [
            ArrowPayloadType::NumberDpExemplars,
            ArrowPayloadType::HistogramDpExemplars,
            ArrowPayloadType::NumberDpExemplarAttrs,
            ArrowPayloadType::HistogramDpExemplarAttrs,
        ] {
            assert_eq!(
                records
                    .get(pt)
                    .map(arrow::record_batch::RecordBatch::num_rows),
                Some(1),
                "{pt:?}"
            );
        }
        let out = extract_metrics(&records, &cfg, &mut budget).expect("extract");
        assert_eq!(out.descriptors.len(), 3);
        // One number exemplar and one histogram exemplar, both dropped. Their
        // filtered attributes are never read, so they cannot reach a series.
        assert_eq!(out.stats.dropped_exemplars, 2);
        assert_eq!(out.stats.dropped_unsupported, 0);
        for d in &out.descriptors {
            assert!(
                d.descriptor.attrs.iter().all(|(k, _)| k != "exemplar.only"),
                "exemplar attributes leaked into a series"
            );
        }
        // One dataset holds both point kinds: the three number rows come first,
        // the single histogram row last, in one batch.
        let vals = out
            .values
            .iter()
            .find(|(d, _)| *d == Dataset::MetricsValues)
            .expect("values")
            .1[0]
            .clone();
        assert_eq!(
            out.values.len(),
            1,
            "metrics write exactly one values dataset"
        );
        assert_eq!(vals.num_rows(), 4);
        let number = vals.slice(0, 3);
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
        assert_eq!(number.num_columns(), 16);
        // The histogram columns of a number row are null, not zero.
        for col in [
            "count",
            "sum",
            "min",
            "max",
            "bucket_counts",
            "explicit_bounds",
        ] {
            assert!(
                number.column_by_name(col).expect(col).is_null(0),
                "{col} must be null on a number row"
            );
        }
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
        let hist = vals.slice(3, 1);
        assert_eq!(hist.num_rows(), 1);
        // The value columns of a histogram row are null.
        assert!(hist.column_by_name("value_int").expect("vi").is_null(0));
        assert!(hist.column_by_name("value_double").expect("vd").is_null(0));
        let bc = hist
            .column_by_name("bucket_counts")
            .expect("bc")
            .as_list::<i32>();
        assert_eq!(bc.value(0).as_primitive::<Int64Type>().values(), &[1i64, 2]);
        // Every remaining column of the single histogram row.
        assert_eq!(hist.num_columns(), 16);
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

    /// Scenario: the gauge-and-histogram request, whose number point and
    /// histogram point each carry an exemplar, under the default
    /// configuration (`unsupported: reject`, `metrics.exemplars` unset) and
    /// under an explicit `metrics.exemplars: reject`.
    /// Guarantees: by default every point is kept and both exemplars are
    /// counted as dropped, `unsupported: reject` notwithstanding; only the
    /// explicit reject refuses the whole request, with a reason naming
    /// exemplars.
    #[test]
    fn exemplars_are_dropped_by_default_and_refused_only_when_asked() {
        let records = encode_metrics(&gauge_and_hist());
        let default = LakeConfig::default();
        assert_eq!(default.unsupported, UnsupportedPolicy::Reject);
        let mut budget = Budget::new(&default);
        let out = extract_metrics(&records, &default, &mut budget).expect("admitted");
        assert_eq!(out.descriptors.len(), 3);
        assert_eq!(out.stats.dropped_exemplars, 2);
        let mut reject = LakeConfig::default();
        reject.metrics.exemplars = Some(ExemplarPolicy::Reject);
        let mut budget = Budget::new(&reject);
        match extract_metrics(&records, &reject, &mut budget) {
            Err(Error::Refused(RefuseReason::Unsupported(what))) => {
                assert_eq!(what, "exemplars");
            }
            other => panic!("expected an exemplar refusal, got {other:?}"),
        }
    }

    /// Scenario: an exponential histogram point carrying an exemplar, beside
    /// a gauge point without one, under `unsupported: drop` and
    /// `metrics.exemplars: reject`.
    /// Guarantees: the exemplar goes with the unsupported point it belongs
    /// to: the request is not refused, the gauge point is kept, and the
    /// exemplar is counted as dropped beside its point.
    #[test]
    fn an_exemplar_of_a_dropped_point_is_dropped_with_it() {
        let md = data(vec![
            Metric {
                name: "cpu".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![dp(10, number_data_point::Value::AsInt(1), vec![])],
                })),
                ..Default::default()
            },
            Metric {
                name: "e".into(),
                data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                    data_points: vec![ExponentialHistogramDataPoint {
                        time_unix_nano: 1,
                        count: 1,
                        exemplars: vec![ex(3)],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ]);
        let records = encode_metrics(&md);
        assert_eq!(
            records
                .get(ArrowPayloadType::ExpHistogramDpExemplars)
                .map(arrow::record_batch::RecordBatch::num_rows),
            Some(1),
            "the encoder produced the exemplar"
        );
        let mut cfg = LakeConfig {
            unsupported: UnsupportedPolicy::Drop,
            ..LakeConfig::default()
        };
        cfg.metrics.exemplars = Some(ExemplarPolicy::Reject);
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&records, &cfg, &mut budget).expect("not refused");
        assert_eq!(out.stats.rows, 1);
        assert_eq!(out.stats.dropped_exp_histogram, 1);
        assert_eq!(out.stats.dropped_exemplars, 1);
    }

    /// Scenario: an exponential histogram under reject and under drop.
    /// Guarantees: reject refuses the whole request; drop yields zero rows,
    /// counts one drop in the aggregate and attributes it to the exponential
    /// histogram counter specifically, leaving the summary counter at zero.
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
        assert_eq!(out.stats.dropped_exp_histogram, 1);
        assert_eq!(out.stats.dropped_summary, 0);
        assert!(out.values.is_empty());
    }

    /// Scenario: an exponential histogram and a summary, with a different
    /// point count each, dropped together in one request.
    /// Guarantees: the two per-kind counters split the aggregate exactly by
    /// kind rather than merging or double counting, so the domain stays
    /// closed to the two unsupported point kinds (FORMAT.md section 2).
    #[test]
    fn exp_histogram_and_summary_drops_are_split_by_kind() {
        let md = data(vec![
            Metric {
                name: "e".into(),
                data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                    data_points: vec![
                        ExponentialHistogramDataPoint {
                            time_unix_nano: 1,
                            count: 1,
                            ..Default::default()
                        },
                        ExponentialHistogramDataPoint {
                            time_unix_nano: 2,
                            count: 1,
                            ..Default::default()
                        },
                    ],
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
        assert_eq!(out.stats.dropped_exp_histogram, 2);
        assert_eq!(out.stats.dropped_summary, 1);
        assert_eq!(out.stats.dropped_unsupported, 3);
        assert_eq!(
            out.stats.dropped_exp_histogram + out.stats.dropped_summary,
            out.stats.dropped_unsupported,
            "the per-kind split sums to the aggregate with no other kind mixed in"
        );
    }

    /// Scenario: a gauge and a summary in the same request under the drop policy.
    /// Guarantees: the summary never reaches temporality validation (pdata supplies
    /// no temporality for summaries), the request succeeds, the gauge rows survive,
    /// the summary points are counted as dropped in the aggregate, and attributed
    /// to the summary counter specifically, leaving the exponential-histogram
    /// counter at zero.
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
        assert_eq!(out.stats.dropped_summary, 1);
        assert_eq!(out.stats.dropped_exp_histogram, 0);
        assert_eq!(out.descriptors.len(), 1);
        let number = out
            .values
            .iter()
            .find(|(d, _)| *d == Dataset::MetricsValues)
            .expect("values");
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

    /// Scenario: two gauge points of the same metric, the first carrying an
    /// attribute and so becoming attribute parent 0, the second carrying none.
    /// pdata still numbers the second point, but writes no attribute row for it.
    /// Guarantees: the point without attributes resolves to an empty attribute
    /// list rather than inheriting attribute parent 0, so the two points land in
    /// two distinct series and the memo does not collapse them.
    #[test]
    fn points_without_attributes_do_not_inherit_parent_zero() {
        let md = data(vec![Metric {
            name: "cpu".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![
                    dp(10, number_data_point::Value::AsInt(1), vec![kv("cpu", "0")]),
                    dp(20, number_data_point::Value::AsInt(2), vec![]),
                ],
            })),
            ..Default::default()
        }]);
        let records = encode_metrics(&md);
        // The premise: pdata really does leave the second point's id null.
        let points = records
            .get(ArrowPayloadType::NumberDataPoints)
            .expect("number points");
        let ids = plain(points, "id", &DataType::UInt32)
            .expect("id column")
            .expect("id present");
        let point_ids: Vec<Option<u32>> = (0..points.num_rows())
            .map(|r| prim_at::<UInt32Type>(&Some(ids.clone()), r))
            .collect();
        assert_eq!(point_ids, vec![Some(0), Some(1)]);
        // Point 1 has its own id but no rows in the attribute batch, so the
        // table must answer with the empty list, not with parent 0's attributes.
        let cfg = LakeConfig::default();
        let table = attr_table(
            &records,
            ArrowPayloadType::NumberDpAttrs,
            DecodeLimits::new(cfg.ingress.max_nesting_depth, cfg.ingress.max_row_bytes),
            &mut Budget::new(&cfg),
        )
        .expect("attrs");
        assert_eq!(table.get(0).len(), 1);
        assert!(table.get(1).is_empty());
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&records, &cfg, &mut budget).expect("extract");
        assert_eq!(out.descriptors.len(), 2);
        let with_attrs = out
            .descriptors
            .iter()
            .find(|d| !d.descriptor.attrs.is_empty())
            .expect("attributed descriptor");
        let without = out
            .descriptors
            .iter()
            .find(|d| d.descriptor.attrs.is_empty())
            .expect("unattributed descriptor");
        assert_eq!(with_attrs.descriptor.attrs.len(), 1);
        assert_ne!(with_attrs.series_id, without.series_id);
        let number = out
            .values
            .iter()
            .find(|(d, _)| *d == Dataset::MetricsValues)
            .expect("values")
            .1[0]
            .clone();
        let row_ids = number
            .column_by_name("series_id")
            .expect("id")
            .as_fixed_size_binary();
        assert_ne!(row_ids.value(0), row_ids.value(1));
    }

    /// Scenario: `bucket_counts` and `explicit_bounds` rows that hold a null
    /// element, and rows that are absent or null altogether.
    /// Guarantees: a null element refuses the request as invalid rather than
    /// being stored as a 0 bucket or a 0.0 bound; an absent or null list row is
    /// read as the empty list.
    #[test]
    fn null_list_elements_are_refused() {
        let mut counts = ListBuilder::new(UInt64Builder::new());
        counts.values().append_value(1);
        counts.values().append_null();
        counts.append(true);
        counts.append(false);
        let counts = counts.finish();

        let mut bounds = ListBuilder::new(Float64Builder::new());
        bounds.values().append_value(1.0);
        bounds.values().append_null();
        bounds.append(true);
        bounds.append(false);
        let bounds = bounds.finish();

        assert!(matches!(
            bucket_counts_at(&Some(counts.clone()), 0),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        assert!(matches!(
            explicit_bounds_at(&Some(bounds.clone()), 0),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        // A null list row, and an absent column, are both the empty list.
        assert_eq!(
            bucket_counts_at(&Some(counts), 1).expect("null row"),
            Vec::<i64>::new()
        );
        assert_eq!(
            explicit_bounds_at(&Some(bounds), 1).expect("null row"),
            Vec::<f64>::new()
        );
        assert_eq!(
            bucket_counts_at(&None, 0).expect("absent"),
            Vec::<i64>::new()
        );
        assert_eq!(
            explicit_bounds_at(&None, 0).expect("absent"),
            Vec::<f64>::new()
        );
    }

    /// The `UnivariateMetrics` batch of `records` with column `name` replaced
    /// by `column`.
    fn patched_metrics(
        records: &OtapArrowRecords,
        name: &str,
        column: ArrayRef,
    ) -> arrow::record_batch::RecordBatch {
        use arrow::datatypes::{Field, Schema};
        use arrow::record_batch::RecordBatch;
        let batch = records
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("metrics");
        let mut fields: Vec<Field> = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for (i, f) in batch.schema().fields().iter().enumerate() {
            if f.name() == name {
                fields.push(Field::new(name, column.data_type().clone(), true));
                cols.push(std::sync::Arc::clone(&column));
            } else {
                fields.push(f.as_ref().clone());
                cols.push(std::sync::Arc::clone(batch.column(i)));
            }
        }
        RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), cols).expect("batch")
    }

    /// Scenario: the gauge's `metric_type` cell is null while its value slot
    /// still holds the gauge tag; pdata's schema check refuses such a batch,
    /// so `metric_rows` is given it directly.
    /// Guarantees: the batch is refused as invalid instead of the slot under
    /// the null being read as the metric's kind.
    #[test]
    fn a_null_metric_type_is_refused() {
        use arrow::array::UInt8Array;
        use arrow::buffer::NullBuffer;
        let records = encode_metrics(&gauge_and_hist());
        let kinds = records
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("metrics")
            .column_by_name(METRIC_TYPE)
            .expect("metric_type")
            .as_primitive::<UInt8Type>()
            .values()
            .clone();
        let valid = (0..kinds.len()).map(|row| row != 0).collect::<Vec<_>>();
        let nulled = UInt8Array::new(kinds, Some(NullBuffer::from(valid)));
        let batch = patched_metrics(&records, METRIC_TYPE, std::sync::Arc::new(nulled));
        assert!(matches!(
            metric_rows(Some(&batch), &LakeConfig::default()),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Scenario: the gauge and the histogram carry the same metric id, which
    /// pdata's schema check accepts.
    /// Guarantees: the request is refused as invalid instead of one metric's
    /// row replacing the other's and its points being stored under the wrong
    /// series.
    #[test]
    fn a_duplicate_metric_id_is_refused() {
        use arrow::array::UInt16Array;
        let mut records = encode_metrics(&gauge_and_hist());
        let ids = std::sync::Arc::new(UInt16Array::from(vec![0u16; 2]));
        let batch = patched_metrics(&records, ID, ids);
        records
            .set(ArrowPayloadType::UnivariateMetrics, batch)
            .expect("pdata accepts a repeated id");
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        assert!(matches!(
            extract_metrics(&records, &cfg, &mut budget),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// A request shaped like a Kubernetes node's metrics: one resource with
    /// 25 attributes of realistic size, `metrics` gauges, each with
    /// `sets` distinct point attribute sets and `points` points per set.
    fn k8s_request(metrics: usize, sets: usize, points: usize) -> MetricsData {
        let resource = (0..25)
            .map(|i| {
                kv(
                    &format!("k8s.resource.attribute.{i:02}"),
                    &format!("{i:02}-{}", "v".repeat(60)),
                )
            })
            .collect();
        let metrics = (0..metrics)
            .map(|m| Metric {
                name: format!("k8s.container.metric.{m}"),
                unit: "1".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: (0..sets)
                        .flat_map(|set| {
                            (0..points).map(move |p| {
                                dp(
                                    10 + p as u64,
                                    number_data_point::Value::AsInt(p as i64),
                                    vec![
                                        kv("k8s.pod.name", &format!("pod-{set:05}")),
                                        kv("k8s.container.name", "app"),
                                        kv("k8s.namespace.name", "default"),
                                    ],
                                )
                            })
                        })
                        .collect(),
                })),
                ..Default::default()
            })
            .collect();
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource,
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

    /// Scenario: 8192 points under one 25-attribute resource at the default
    /// limits: eight gauges, 512 point attribute sets each, two points per
    /// set, so 4096 series of two points.
    /// Guarantees: the request is accepted; the memo answers the second point
    /// of every series without encoding or hashing it again (4096 hits, 4096
    /// misses); every series is built once; and the resource list is decoded
    /// and charged once for the whole request rather than once per series.
    /// Before the memo was keyed on content and the resource shared, the same
    /// request was refused as too large for `ingress.max_extracted_bytes`.
    #[test]
    fn many_points_under_a_wide_resource_hit_the_memo_and_fit_the_default_budget() {
        let d = k8s_request(8, 512, 2);
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&encode_metrics(&d), &cfg, &mut budget)
            .expect("8k points under a wide resource fit the default budget");
        assert_eq!(out.stats.rows, 8192);
        assert_eq!(out.descriptors.len(), 4096);
        assert_eq!(out.stats.series_memo_hits, 4096);
        assert_eq!(out.stats.series_memo_misses, 4096);
        let resource = &out.descriptors[0].descriptor.resource_attrs;
        assert_eq!(resource.len(), 25);
        assert_eq!(
            out.shared_bytes,
            crate::value::kv_bytes(resource),
            "one shared copy of the resource, no scope attributes"
        );
        assert!(
            out.descriptors
                .iter()
                .all(|row| std::sync::Arc::ptr_eq(&row.descriptor.resource_attrs, resource)),
            "every descriptor holds the one shared resource list"
        );
    }

    fn kvd(k: &str, v: f64) -> KeyValue {
        KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::DoubleValue(v)),
            }),
        }
    }

    /// Scenario: memo keys built from attribute lists holding `0.0` and
    /// `-0.0`, two NaNs with different bit patterns, and a NaN nested in an
    /// array, compared and hashed with the map's own hasher.
    /// Guarantees: keys the canonical encoding cannot tell apart are equal
    /// and hash alike, a NaN key equals itself, and a key with a different
    /// value is not equal, so the memo honours the `Eq`/`Hash` contract.
    #[test]
    fn memo_keys_follow_the_canonical_double_rules() {
        use std::hash::BuildHasher;
        let hasher = std::collections::hash_map::RandomState::new();
        let list = |v: Value| vec![("x".to_owned(), v)];
        let pairs = [
            (Value::Double(0.0), Value::Double(-0.0)),
            (
                Value::Double(f64::NAN),
                Value::Double(f64::from_bits(0xFFF8_0000_0000_0001)),
            ),
            (
                Value::Array(vec![Value::Double(f64::NAN)]),
                Value::Array(vec![Value::Double(-f64::NAN)]),
            ),
        ];
        for (a, b) in pairs {
            let (la, lb) = (list(a), list(b));
            let (ka, kb) = (
                MemoKey {
                    metric_id: 1,
                    attrs: &la,
                },
                MemoKey {
                    metric_id: 1,
                    attrs: &lb,
                },
            );
            assert!(ka == ka, "a key equals itself: {la:?}");
            assert!(ka == kb, "{la:?} and {lb:?} encode alike");
            assert_eq!(hasher.hash_one(ka), hasher.hash_one(kb), "{la:?}");
            assert_eq!(
                crate::canonical::canonical_bytes(&Descriptor {
                    signal: Signal::Logs,
                    resource_attrs: std::sync::Arc::from([]),
                    resource_schema_url: String::new(),
                    scope_name: String::new(),
                    scope_version: String::new(),
                    scope_schema_url: String::new(),
                    scope_attrs: std::sync::Arc::from([]),
                    metric: None,
                    attrs: la.clone(),
                }),
                crate::canonical::canonical_bytes(&Descriptor {
                    signal: Signal::Logs,
                    resource_attrs: std::sync::Arc::from([]),
                    resource_schema_url: String::new(),
                    scope_name: String::new(),
                    scope_version: String::new(),
                    scope_schema_url: String::new(),
                    scope_attrs: std::sync::Arc::from([]),
                    metric: None,
                    attrs: lb.clone(),
                }),
                "the premise: the canonical encoding cannot tell them apart"
            );
        }
        let (one, two) = (list(Value::Double(1.0)), list(Value::Double(2.0)));
        assert!(
            MemoKey {
                metric_id: 1,
                attrs: &one
            } != MemoKey {
                metric_id: 1,
                attrs: &two
            }
        );
    }

    /// Scenario: one gauge with six points whose only attribute is a double:
    /// `0.0`, `-0.0`, NaN, a NaN with other bits, `1.5` and `1.5` again.
    /// Guarantees: the memo answers every point whose value encodes like an
    /// earlier one, so the memo misses equal the distinct canonical series
    /// (three) and every other point is a hit.
    #[test]
    fn signed_zero_and_nan_attributes_hit_the_memo() {
        let values = [
            0.0,
            -0.0,
            f64::NAN,
            f64::from_bits(0xFFF8_0000_0000_0001),
            1.5,
            1.5,
        ];
        let d = data(vec![Metric {
            name: "g".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        dp(
                            10 + i as u64,
                            number_data_point::Value::AsInt(1),
                            vec![kvd("x", *v)],
                        )
                    })
                    .collect(),
            })),
            ..Default::default()
        }]);
        let cfg = LakeConfig::default();
        let mut budget = Budget::new(&cfg);
        let out = extract_metrics(&encode_metrics(&d), &cfg, &mut budget).expect("extract");
        assert_eq!(out.stats.rows, 6);
        assert_eq!(out.descriptors.len(), 3, "three distinct canonical series");
        assert_eq!(out.stats.series_memo_misses, 3);
        assert_eq!(out.stats.series_memo_hits, 3);
    }
}
