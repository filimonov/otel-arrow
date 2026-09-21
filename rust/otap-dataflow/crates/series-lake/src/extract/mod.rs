// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extraction of descriptors and values from OTAP records (spec section 6.2 step 4).

pub mod logs;
pub mod metrics;

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder,
    Float64Builder, Int32Builder, Int64Builder, ListArray, ListBuilder, MapBuilder, StringBuilder,
    StructArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    DataType, Float64Type, Int64Type, SchemaRef, TimeUnit, TimestampNanosecondType, UInt16Type,
    UInt32Type,
};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use crate::attrs::{AnyValueColumns, AttrTable};
use crate::canonical::{Descriptor, SeriesId, Signal};
use crate::config::{DenormSource, DenormType, Denormalize, LakeConfig};
use crate::error::{Error, RefuseReason, Result};
use crate::schema::{Dataset, dataset_schema, denorm_columns};
use crate::value::{DecodeLimits, Value, map_string, value_bytes};

/// The single "cast this batch column to a plain type" helper of the crate.
///
/// Re-exported from `attrs` so that `extract::logs` and `extract::metrics` share
/// one copy instead of each defining its own `plain_col`.
pub(crate) use crate::attrs::plain;

/// A typed denormalized value.
#[derive(Debug, Clone, PartialEq)]
pub enum DenormValue {
    /// String.
    Str(String),
    /// Int64.
    Int(i64),
    /// Double.
    Double(f64),
    /// Bool.
    Bool(bool),
}

/// One descriptor ready to become a `series` row.
#[derive(Debug, Clone)]
pub struct DescriptorRow {
    /// Series id.
    pub series_id: SeriesId,
    /// Canonical bytes.
    pub identity_bytes: Vec<u8>,
    /// Descriptor.
    pub descriptor: Descriptor,
    /// Denormalized identity columns, in `denorm_columns(series dataset)` order.
    pub denorm: Vec<Option<DenormValue>>,
    /// Approximate retained bytes of the row.
    pub approx_bytes: usize,
}

/// Counters produced by extraction.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExtractStats {
    /// Values rows produced.
    pub rows: usize,
    /// Points dropped under the drop policy.
    pub dropped_unsupported: u64,
    /// Exemplar rows dropped (spec 5.1 `dropped_unsupported{kind=exemplar}`).
    pub dropped_exemplars: u64,
    /// Timestamps outside `1..=i64::MAX`.
    pub timestamp_out_of_range: u64,
    /// Denormalized values stored as null because of a type mismatch.
    pub denorm_type_mismatch: u64,
}

/// Result of extracting one request.
#[derive(Debug)]
pub struct Extracted {
    /// Signal.
    pub signal: Signal,
    /// Unique descriptors of the request.
    pub descriptors: Vec<DescriptorRow>,
    /// Values batches per dataset, each batch at most `run_target_bytes`.
    pub values: Vec<(Dataset, Vec<RecordBatch>)>,
    /// Pinned bytes of all values batches.
    pub pinned_bytes: usize,
    /// Counters.
    pub stats: ExtractStats,
}

/// Extract descriptors and values from one OTAP request.
///
/// Takes `&mut` because OTAP attribute batches carry quasi-delta encoded
/// `parent_id` columns: spec section 6.2 step 3 requires
/// `decode_transport_optimized_ids` before any `parent_id` is read. Decoding is
/// idempotent, so a request whose ids are already plain is unaffected.
///
/// `cfg.ingress.max_extracted_bytes` is enforced on the measured extracted
/// output: see [`Budget`] for how row estimates are replaced by measurements.
///
/// # Errors
/// Refuses traces, a malformed request, or a request past its budgets.
pub fn extract(records: &mut OtapArrowRecords, cfg: &LakeConfig) -> Result<Extracted> {
    if matches!(records, OtapArrowRecords::Traces(_)) {
        return Err(Error::Refused(RefuseReason::Unsupported("traces".into())));
    }
    records
        .decode_transport_optimized_ids()
        .map_err(|e| Error::Pdata(e.to_string()))?;
    let mut budget = Budget::new(cfg);
    match records {
        OtapArrowRecords::Logs(_) => logs::extract_logs(records, cfg, &mut budget),
        OtapArrowRecords::Metrics(_) => metrics::extract_metrics(records, cfg, &mut budget),
        OtapArrowRecords::Traces(_) => {
            Err(Error::Refused(RefuseReason::Unsupported("traces".into())))
        }
    }
}

/// The single byte accountant of one request (spec section 6.2 step 4).
///
/// One `Budget` is created in [`extract`] and threaded through every descriptor
/// row and every values row, so that a request with many small datasets cannot
/// spend the whole limit once per dataset.
///
/// A values row is charged its approximate size while the run is being built,
/// because nothing is measurable until the run is sealed. When the run *is*
/// sealed, [`RowSink::seal`] replaces that estimate with the measured pinned
/// bytes of the Arrow batch: it uncharges the run's accumulated estimate and
/// charges the measurement, so the estimate is never counted alongside the
/// measurement. `max_extracted_bytes` therefore bounds the measured extracted
/// output plus the descriptor rows, which are estimated throughout because they
/// are never sealed into an Arrow batch here.
pub(crate) struct Budget {
    limit: usize,
    max_row: usize,
    used: usize,
}

impl Budget {
    /// New accountant for one request.
    pub(crate) fn new(cfg: &LakeConfig) -> Self {
        Self {
            limit: cfg.ingress.max_extracted_bytes,
            max_row: cfg.ingress.max_row_bytes,
            used: 0,
        }
    }

    /// Charge one row -- a values row or a descriptor row.
    ///
    /// A single row larger than `max_row_bytes` refuses the request, as does a
    /// running total past `max_extracted_bytes`.
    pub(crate) fn charge_row(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.max_row {
            return Err(Error::Refused(RefuseReason::RequestTooLarge));
        }
        self.charge(bytes)
    }

    /// Charge bytes that are not one row, such as a sealed batch.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<()> {
        self.used = self.used.saturating_add(bytes);
        if self.used > self.limit {
            return Err(Error::Refused(RefuseReason::RequestTooLarge));
        }
        Ok(())
    }

    /// Give bytes back, saturating at zero.
    ///
    /// Used when an estimate is superseded by a measurement, and when a row
    /// that was charged turns out to be a duplicate that is not retained.
    pub(crate) fn uncharge(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
    }
}

/// Approximate retained bytes of a sorted attribute list.
pub(crate) fn kv_bytes(list: &[(String, Value)]) -> usize {
    list.iter()
        .map(|(k, v)| k.len() + 24 + value_bytes(v))
        .sum()
}

/// Approximate retained bytes of one denormalized cell.
pub(crate) fn denorm_bytes(v: &Option<DenormValue>) -> usize {
    match v {
        None => 8,
        Some(DenormValue::Str(s)) => s.len() + 24,
        Some(_) => 16,
    }
}

/// Spec 5.1 timestamp rule on the converted `i64` nanoseconds.
pub(crate) fn timestamp_pair(ns: i64, stats: &mut ExtractStats) -> (Option<i64>, Option<i64>) {
    if ns == 0 {
        (None, None)
    } else if ns < 0 {
        stats.timestamp_out_of_range += 1;
        (None, None)
    } else {
        (Some(ns), Some(ns / 1000))
    }
}

fn lookup<'a>(list: &'a [(String, Value)], key: &str) -> Option<&'a Value> {
    list.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Resolve one denormalized column for a row.
pub(crate) fn denorm_lookup(
    d: &Denormalize,
    resource: &[(String, Value)],
    scope: &[(String, Value)],
    attrs: &[(String, Value)],
    stats: &mut ExtractStats,
) -> Option<DenormValue> {
    let (src, key) = d.source().ok()?;
    let v = match src {
        DenormSource::Resource => lookup(resource, key),
        DenormSource::Scope => lookup(scope, key),
        DenormSource::Attrs => lookup(attrs, key),
    }?;
    match (d.ty, v) {
        (DenormType::String, v) => map_string(v).map(DenormValue::Str),
        (DenormType::Int64, Value::Int(i)) => Some(DenormValue::Int(*i)),
        (DenormType::Double, Value::Double(f)) => Some(DenormValue::Double(*f)),
        (DenormType::Bool, Value::Bool(b)) => Some(DenormValue::Bool(*b)),
        (_, Value::Null) => None,
        _ => {
            stats.denorm_type_mismatch += 1;
            None
        }
    }
}

/// Producer id projection of a resource attribute list.
pub(crate) fn producer_id(resource: &[(String, Value)], attribute: &str) -> String {
    lookup(resource, attribute)
        .and_then(map_string)
        .unwrap_or_default()
}

/// A typed cell of a values row, in dataset schema order.
#[derive(Debug, Clone)]
pub(crate) enum Col {
    /// Utf8.
    Str(Option<String>),
    /// Int64.
    Int(Option<i64>),
    /// Int32.
    Int32(Option<i32>),
    /// Float64.
    Double(Option<f64>),
    /// Boolean.
    Bool(Option<bool>),
    /// Timestamp(us).
    TsUs(Option<i64>),
    /// FixedSizeBinary.
    Fixed(Option<Vec<u8>>),
    /// Map<Utf8, Utf8>.
    Map(Vec<(String, Option<String>)>),
    /// List<Int64>. Constructed by the histogram dataset.
    ListI64(Vec<i64>),
    /// List<Float64>. Constructed by the histogram dataset.
    ListF64(Vec<f64>),
    /// Binary.
    Bytes(Vec<u8>),
}

impl From<Option<DenormValue>> for Col {
    fn from(v: Option<DenormValue>) -> Self {
        match v {
            None => Col::Str(None),
            Some(DenormValue::Str(s)) => Col::Str(Some(s)),
            Some(DenormValue::Int(i)) => Col::Int(Some(i)),
            Some(DenormValue::Double(f)) => Col::Double(Some(f)),
            Some(DenormValue::Bool(b)) => Col::Bool(Some(b)),
        }
    }
}

/// One values row.
#[derive(Debug, Clone)]
pub(crate) struct ValuesRow {
    /// Cells in dataset schema order.
    pub cols: Vec<Col>,
    /// Approximate retained bytes.
    pub approx_bytes: usize,
}

enum AnyBuilder {
    Str(StringBuilder),
    Int(Int64Builder),
    Int32(Int32Builder),
    Double(Float64Builder),
    Bool(BooleanBuilder),
    TsUs(TimestampMicrosecondBuilder),
    Fixed(FixedSizeBinaryBuilder),
    Map(MapBuilder<StringBuilder, StringBuilder>),
    ListI64(ListBuilder<Int64Builder>),
    ListF64(ListBuilder<Float64Builder>),
    Bytes(BinaryBuilder),
}

fn builder_for(dt: &DataType) -> Result<AnyBuilder> {
    Ok(match dt {
        DataType::Utf8 => AnyBuilder::Str(StringBuilder::new()),
        DataType::Int64 => AnyBuilder::Int(Int64Builder::new()),
        DataType::Int32 => AnyBuilder::Int32(Int32Builder::new()),
        DataType::Float64 => AnyBuilder::Double(Float64Builder::new()),
        DataType::Boolean => AnyBuilder::Bool(BooleanBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            AnyBuilder::TsUs(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
        }
        DataType::FixedSizeBinary(n) => AnyBuilder::Fixed(FixedSizeBinaryBuilder::new(*n)),
        DataType::Map(_, _) => AnyBuilder::Map(MapBuilder::new(
            None,
            StringBuilder::new(),
            StringBuilder::new(),
        )),
        DataType::List(f) if f.data_type() == &DataType::Int64 => {
            AnyBuilder::ListI64(ListBuilder::new(Int64Builder::new()).with_field(f.clone()))
        }
        DataType::List(f) => {
            AnyBuilder::ListF64(ListBuilder::new(Float64Builder::new()).with_field(f.clone()))
        }
        DataType::Binary => AnyBuilder::Bytes(BinaryBuilder::new()),
        other => return Err(Error::invalid(format!("unsupported builder type {other}"))),
    })
}

fn append(b: &mut AnyBuilder, c: &Col) -> Result<()> {
    match (b, c) {
        (AnyBuilder::Str(b), Col::Str(v)) => b.append_option(v.as_deref()),
        (AnyBuilder::Int(b), Col::Int(v)) => b.append_option(*v),
        (AnyBuilder::Int32(b), Col::Int32(v)) => b.append_option(*v),
        (AnyBuilder::Double(b), Col::Double(v)) => b.append_option(*v),
        (AnyBuilder::Bool(b), Col::Bool(v)) => b.append_option(*v),
        (AnyBuilder::TsUs(b), Col::TsUs(v)) => b.append_option(*v),
        (AnyBuilder::Fixed(b), Col::Fixed(v)) => match v {
            Some(bytes) => b.append_value(bytes)?,
            None => b.append_null(),
        },
        (AnyBuilder::Map(b), Col::Map(entries)) => {
            for (k, v) in entries {
                b.keys().append_value(k);
                b.values().append_option(v.as_deref());
            }
            b.append(true)?;
        }
        (AnyBuilder::ListI64(b), Col::ListI64(items)) => {
            b.values().append_slice(items);
            b.append(true);
        }
        (AnyBuilder::ListF64(b), Col::ListF64(items)) => {
            b.values().append_slice(items);
            b.append(true);
        }
        (AnyBuilder::Bytes(b), Col::Bytes(v)) => b.append_value(v),
        // Denormalized columns arrive as Col::Str(None) when absent, whatever their type.
        (AnyBuilder::Int(b), Col::Str(None)) => b.append_null(),
        (AnyBuilder::Double(b), Col::Str(None)) => b.append_null(),
        (AnyBuilder::Bool(b), Col::Str(None)) => b.append_null(),
        (_, c) => return Err(Error::invalid(format!("column/builder mismatch for {c:?}"))),
    }
    Ok(())
}

fn finish(b: &mut AnyBuilder) -> ArrayRef {
    match b {
        AnyBuilder::Str(b) => Arc::new(b.finish()),
        AnyBuilder::Int(b) => Arc::new(b.finish()),
        AnyBuilder::Int32(b) => Arc::new(b.finish()),
        AnyBuilder::Double(b) => Arc::new(b.finish()),
        AnyBuilder::Bool(b) => Arc::new(b.finish()),
        AnyBuilder::TsUs(b) => Arc::new(b.finish()),
        AnyBuilder::Fixed(b) => Arc::new(b.finish()),
        AnyBuilder::Map(b) => Arc::new(b.finish()),
        AnyBuilder::ListI64(b) => Arc::new(b.finish()),
        AnyBuilder::ListF64(b) => Arc::new(b.finish()),
        AnyBuilder::Bytes(b) => Arc::new(b.finish()),
    }
}

/// Accumulates rows of one dataset into slices of at most `run_target_bytes`.
///
/// The slice is sealed *before* appending a row that would take it past
/// `run_target_bytes`, so a sealed slice never exceeds the target (spec 6.2
/// step 4). Row and request limits are enforced by the shared [`Budget`], which
/// every dataset of the request shares.
pub(crate) struct RowSink {
    schema: SchemaRef,
    builders: Vec<AnyBuilder>,
    slice_bytes: usize,
    rows_in_slice: usize,
    batches: Vec<RecordBatch>,
    pinned: usize,
    seen: CountedAllocations,
    run_target: usize,
}

impl RowSink {
    /// New sink for one dataset.
    pub(crate) fn new(ds: Dataset, cfg: &LakeConfig) -> Result<Self> {
        let schema = dataset_schema(ds, cfg);
        let builders = schema
            .fields()
            .iter()
            .map(|f| builder_for(f.data_type()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            builders,
            slice_bytes: 0,
            rows_in_slice: 0,
            batches: Vec::new(),
            pinned: 0,
            seen: CountedAllocations::default(),
            run_target: cfg.sorting.run_target_bytes,
        })
    }

    /// Append one row, sealing the current slice first when it would overflow.
    pub(crate) fn push(&mut self, row: &ValuesRow, budget: &mut Budget) -> Result<()> {
        budget.charge_row(row.approx_bytes)?;
        if row.cols.len() != self.builders.len() {
            return Err(Error::invalid("row width does not match dataset schema"));
        }
        // Seal first: a slice must not grow past run_target_bytes.
        if self.rows_in_slice > 0 && self.slice_bytes + row.approx_bytes > self.run_target {
            self.seal(budget)?;
        }
        for (b, c) in self.builders.iter_mut().zip(&row.cols) {
            append(b, c)?;
        }
        self.slice_bytes += row.approx_bytes;
        self.rows_in_slice += 1;
        Ok(())
    }

    fn seal(&mut self, budget: &mut Budget) -> Result<()> {
        if self.rows_in_slice == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = self.builders.iter_mut().map(finish).collect();
        let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
        let pinned = record_batch_pinned_bytes(&batch, &mut self.seen);
        self.pinned += pinned;
        // The run's rows were charged as estimates while it was being built;
        // now that it is measurable, swap the estimate for the measurement.
        budget.uncharge(self.slice_bytes);
        budget.charge(pinned)?;
        self.batches.push(batch);
        self.slice_bytes = 0;
        self.rows_in_slice = 0;
        Ok(())
    }

    /// Seal the last slice and return every batch with their pinned bytes.
    pub(crate) fn finish(mut self, budget: &mut Budget) -> Result<(Vec<RecordBatch>, usize)> {
        self.seal(budget)?;
        Ok((self.batches, self.pinned))
    }
}

/// A `Map<Utf8, Utf8>` cell built from a sorted attribute list, with the bytes
/// the rendered cell retains.
///
/// Rendering, not the decoded value tree, decides how large the stored cell is:
/// `render_v1` hex-encodes a bytes value (doubling its length) and JSON-escapes
/// strings (which can expand them several-fold). Charging the tree's size would
/// therefore admit a row whose stored form is far past `max_row_bytes`, which no
/// later recount can undo. The caller charges the returned size.
pub(crate) fn map_cell(list: &[(String, Value)]) -> (Col, usize) {
    let entries: Vec<(String, Option<String>)> = list
        .iter()
        .map(|(k, v)| (k.clone(), map_string(v)))
        .collect();
    let bytes = entries
        .iter()
        .map(|(k, v)| k.len() + 24 + v.as_ref().map_or(0, String::len))
        .sum();
    (Col::Map(entries), bytes)
}

/// Bytes a sorted attribute list occupies once rendered into a map cell.
///
/// Measured by rendering, because the expansion is serde_json's escaping and
/// `render_v1`'s hex encoding and cannot be predicted from the value tree. Used
/// where the cell itself is built later (a descriptor row becomes a `series`
/// row only at seal time) and only its size is needed now.
pub(crate) fn rendered_kv_bytes(list: &[(String, Value)]) -> usize {
    list.iter()
        .map(|(k, v)| k.len() + 24 + map_string(v).map_or(0, |s| s.len()))
        .sum()
}

/// The children of a struct column with the parent's validity applied.
///
/// A null struct row has no children at all, but a valid Arrow array may still
/// hold arbitrary values in the child buffers underneath it. Reading a child
/// directly would therefore fabricate a resource id, a scope name or a log body
/// that the request never carried. `StructArray::flatten` unions the parent's
/// null buffer into every child, which is exactly the masking wanted here; a
/// struct with no null buffer needs no masking and its children are returned
/// unchanged.
fn flat_children(s: &StructArray) -> Vec<(String, ArrayRef)> {
    if s.nulls().is_none() {
        return s
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .zip(s.columns().iter().map(Arc::clone))
            .collect();
    }
    let (fields, columns) = s.flatten();
    fields
        .iter()
        .map(|f| f.name().clone())
        .zip(columns)
        .collect()
}

/// One child of a struct column with the parent's validity applied.
///
/// See [`flat_children`] for why the parent's null buffer matters.
fn flat_child(s: &StructArray, child: &str) -> Option<ArrayRef> {
    if s.nulls().is_none() {
        return s.column_by_name(child).map(Arc::clone);
    }
    let (fields, columns) = s.flatten();
    let idx = fields.iter().position(|f| f.name() == child)?;
    columns.get(idx).map(Arc::clone)
}

/// Child of a struct column, cast to a plain type, or `None` when absent.
///
/// The parent struct's validity is applied first: a child value under a null
/// parent reads as null, never as a fabricated value.
pub(crate) fn struct_child(
    batch: &RecordBatch,
    parent: &str,
    child: &str,
    to: &DataType,
) -> Result<Option<ArrayRef>> {
    let Some(col) = batch.column_by_name(parent) else {
        return Ok(None);
    };
    let s = col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::invalid(format!("column {parent} is not a struct")))?;
    match flat_child(s, child) {
        None => Ok(None),
        Some(c) if c.data_type() == to => Ok(Some(c)),
        Some(c) => Ok(Some(arrow::compute::cast(&c, to)?)),
    }
}

/// The `AnyValue` struct column `name` of a batch, or `None` when it is absent.
///
/// The downcast is checked: a column that is present but is not a struct
/// refuses the request instead of panicking. `OtapArrowRecords` validates the
/// OTAP schema on the way in, so this can only be reached by a batch built
/// outside that validation.
///
/// The struct's own validity is applied to its children (see
/// [`flat_children`]), so a null body row reads as a null type tag and becomes
/// [`Value::Null`] rather than whatever the child buffers happen to hold.
pub(crate) fn any_value_col(batch: &RecordBatch, name: &str) -> Result<Option<AnyValueColumns>> {
    let Some(col) = batch.column_by_name(name) else {
        return Ok(None);
    };
    let s = col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::invalid(format!("column {name} is not an AnyValue struct")))?;
    let children = flat_children(s);
    Ok(Some(AnyValueColumns::new(&|n| {
        children
            .iter()
            .find(|(k, _)| k == n)
            .map(|(_, a)| Arc::clone(a))
    })?))
}

/// Attribute table for a payload type, empty when the payload is absent.
pub(crate) fn attr_table(
    records: &OtapArrowRecords,
    pt: ArrowPayloadType,
    limits: DecodeLimits,
) -> Result<AttrTable> {
    match records.get(pt) {
        Some(b) => AttrTable::from_batch(b, limits),
        None => Ok(AttrTable::default()),
    }
}

/// A `UInt16` id column read as `Option<u32>`.
///
/// `None` means "no parent": pdata writes a null id for a record or point that
/// carries no attributes. A `None` must never be turned into id 0, which would
/// make such a record inherit the attributes of parent 0.
pub(crate) fn opt_u16_at(a: &Option<ArrayRef>, row: usize) -> Option<u32> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| u32::from(a.as_primitive::<UInt16Type>().value(row)))
    })
}

/// Attributes of an optional parent id: the empty list when the id is `None`.
pub(crate) fn attrs_of(table: &AttrTable, id: Option<u32>) -> &[(String, Value)] {
    match id {
        Some(id) => table.get(id),
        None => &[],
    }
}

/// A Utf8 column read as an owned `String`, empty when null or absent.
pub(crate) fn str_at(a: &Option<ArrayRef>, row: usize) -> String {
    a.as_ref()
        .and_then(|a| {
            a.is_valid(row)
                .then(|| a.as_string::<i32>().value(row).to_string())
        })
        .unwrap_or_default()
}

/// A `Timestamp(ns)` column read as `i64`, `0` when null or absent.
pub(crate) fn i64_at(a: &Option<ArrayRef>, row: usize) -> i64 {
    a.as_ref()
        .and_then(|a| {
            a.is_valid(row)
                .then(|| a.as_primitive::<TimestampNanosecondType>().value(row))
        })
        .unwrap_or(0)
}

/// An `Int64` column read as `Option<i64>`, `None` when null or absent.
pub(crate) fn opt_i64(a: &Option<ArrayRef>, row: usize) -> Option<i64> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_primitive::<Int64Type>().value(row))
    })
}

/// A `Float64` column read as `Option<f64>`, `None` when null or absent.
pub(crate) fn opt_f64(a: &Option<ArrayRef>, row: usize) -> Option<f64> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_primitive::<Float64Type>().value(row))
    })
}

/// A `UInt32` id column read as `Option<u32>`.
///
/// Like [`opt_u16_at`], `None` means "no parent" and must never become id 0.
pub(crate) fn opt_u32_at(a: &Option<ArrayRef>, row: usize) -> Option<u32> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_primitive::<UInt32Type>().value(row))
    })
}

/// A `UInt32` OTLP flags column reinterpreted into the signed storage column.
///
/// The flags are a bit set, stored as received; readers interpret the bits. The
/// storage column is `Int32` for Parquet portability, so the top bit wraps into
/// the sign bit rather than being lost.
#[allow(clippy::cast_possible_wrap)]
pub(crate) fn flags_at(a: &Option<ArrayRef>, row: usize) -> i32 {
    a.as_ref()
        .and_then(|a| {
            a.is_valid(row)
                .then(|| a.as_primitive::<UInt32Type>().value(row))
        })
        .unwrap_or(0) as i32
}

/// A `List` column of a batch, or `None` when it is absent.
///
/// The downcast is checked: a column that is present but is not a list refuses
/// the request instead of panicking, exactly as [`struct_child`] and
/// [`any_value_col`] do for their own shapes.
pub(crate) fn list_col(batch: &RecordBatch, name: &str) -> Result<Option<ListArray>> {
    let Some(col) = batch.column_by_name(name) else {
        return Ok(None);
    };
    Ok(Some(
        col.as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| Error::invalid(format!("column {name} is not a list")))?
            .clone(),
    ))
}

/// A `FixedSizeBinary` column read as owned bytes, `None` when null or absent.
pub(crate) fn fixed_at(a: &Option<ArrayRef>, row: usize) -> Option<Vec<u8>> {
    a.as_ref().and_then(|a| {
        a.is_valid(row)
            .then(|| a.as_fixed_size_binary().value(row).to_vec())
    })
}

/// Build a `series` batch from descriptor rows.
///
/// # Errors
/// Refuses a metrics descriptor without a metric block, or a schema mismatch.
pub fn series_batch(
    rows: &[&DescriptorRow],
    emitted_at_us: i64,
    ds: Dataset,
    cfg: &LakeConfig,
) -> Result<RecordBatch> {
    let schema = dataset_schema(ds, cfg);
    let mut builders = schema
        .fields()
        .iter()
        .map(|f| builder_for(f.data_type()))
        .collect::<Result<Vec<_>>>()?;
    for r in rows {
        let d = &r.descriptor;
        let mut cols: Vec<Col> = vec![
            Col::Fixed(Some(r.series_id.to_vec())),
            Col::Bytes(r.identity_bytes.clone()),
            Col::TsUs(Some(emitted_at_us)),
            Col::Str(Some(d.resource_schema_url.clone())),
            map_cell(&d.resource_attrs).0,
            Col::Str(Some(d.scope_name.clone())),
            Col::Str(Some(d.scope_version.clone())),
            Col::Str(Some(d.scope_schema_url.clone())),
            map_cell(&d.scope_attrs).0,
            map_cell(&d.attrs).0,
        ];
        if ds == Dataset::MetricsSeries {
            let m = d
                .metric
                .as_ref()
                .ok_or_else(|| Error::invalid("metrics descriptor without metric"))?;
            cols.extend([
                Col::Str(Some(m.name.clone())),
                Col::Str(Some(m.unit.clone())),
                Col::Str(Some(m.kind.as_str().to_string())),
                Col::Str(Some(m.temporality.as_str().to_string())),
                Col::Bool(Some(m.is_monotonic)),
                Col::Str(Some(m.description.clone())),
            ]);
        }
        cols.extend(r.denorm.iter().cloned().map(Col::from));
        for (b, c) in builders.iter_mut().zip(&cols) {
            append(b, c)?;
        }
    }
    let arrays: Vec<ArrayRef> = builders.iter_mut().map(finish).collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Descriptor row constructor shared by logs and metrics.
///
/// The row is charged to `budget` like any other row, so an oversized
/// descriptor refuses the request through `max_row_bytes` and a request with
/// very many series refuses through `max_extracted_bytes`.
pub(crate) fn descriptor_row(
    descriptor: Descriptor,
    ds_series: Dataset,
    cfg: &LakeConfig,
    stats: &mut ExtractStats,
    budget: &mut Budget,
) -> Result<DescriptorRow> {
    let identity_bytes = crate::canonical::canonical_bytes(&descriptor);
    let series_id = crate::canonical::series_id(&identity_bytes);
    let denorm: Vec<Option<DenormValue>> = denorm_columns(ds_series, cfg)
        .into_iter()
        .map(|d| {
            denorm_lookup(
                d,
                &descriptor.resource_attrs,
                &descriptor.scope_attrs,
                &descriptor.attrs,
                stats,
            )
        })
        .collect();
    // series row: series_id + identity_bytes + emitted_at + the four schema/scope
    // strings + three attribute maps + the metric block + denormalized columns.
    //
    // Each attribute list is counted twice on purpose: the `DescriptorRow` keeps
    // the decoded value tree until the block seals (`kv_bytes`), and the `series`
    // row it becomes holds the rendered map cell (`rendered_kv_bytes`), which
    // hex encoding and JSON escaping can make much larger than the tree.
    let approx_bytes = 16
        + identity_bytes.len()
        + 8
        + descriptor.resource_schema_url.len()
        + descriptor.scope_name.len()
        + descriptor.scope_version.len()
        + descriptor.scope_schema_url.len()
        + kv_bytes(&descriptor.resource_attrs)
        + kv_bytes(&descriptor.scope_attrs)
        + kv_bytes(&descriptor.attrs)
        + rendered_kv_bytes(&descriptor.resource_attrs)
        + rendered_kv_bytes(&descriptor.scope_attrs)
        + rendered_kv_bytes(&descriptor.attrs)
        + descriptor.metric.as_ref().map_or(0, |m| {
            m.name.len() + m.unit.len() + m.description.len() + 32
        })
        + denorm.iter().map(denorm_bytes).sum::<usize>();
    budget.charge_row(approx_bytes)?;
    Ok(DescriptorRow {
        series_id,
        identity_bytes,
        descriptor,
        denorm,
        approx_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RefuseReason;
    use arrow::array::{StringArray, UInt8Array, UInt16Array};
    use arrow::buffer::{BooleanBuffer, NullBuffer};
    use arrow::datatypes::{Field, Schema};

    fn batch_with_utf8(name: &str) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(name, DataType::Utf8, true)]);
        let cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["x"]))];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: `struct_child` is given a batch whose `resource` column is a
    /// string column rather than a struct.
    /// Guarantees: the request is refused as invalid content, not panicked on by
    /// an unchecked downcast.
    #[test]
    fn struct_child_refuses_a_non_struct_column() {
        let b = batch_with_utf8("resource");
        assert!(matches!(
            struct_child(&b, "resource", "id", &DataType::UInt16),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        // An absent parent column is simply absent, not an error.
        assert!(
            struct_child(&b, "scope", "id", &DataType::UInt16)
                .expect("absent parent")
                .is_none()
        );
    }

    /// Scenario: `list_col` is given a batch whose `bucket_counts` column is a
    /// string column rather than a list.
    /// Guarantees: the request is refused as invalid content, not panicked on by
    /// an unchecked downcast; an absent column is simply absent.
    #[test]
    fn list_col_refuses_a_non_list_column() {
        let b = batch_with_utf8("bucket_counts");
        assert!(matches!(
            list_col(&b, "bucket_counts"),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        assert!(
            list_col(&b, "explicit_bounds")
                .expect("absent column")
                .is_none()
        );
    }

    /// Scenario: `any_value_col` is given a batch whose `body` column is a string
    /// column rather than the OTAP `AnyValue` struct.
    /// Guarantees: the request is refused as invalid content, not panicked on by
    /// an unchecked downcast; a real struct body is accepted.
    #[test]
    fn any_value_col_refuses_a_non_struct_body() {
        assert!(matches!(
            any_value_col(&batch_with_utf8("body"), "body"),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));

        let inner = Field::new("type", DataType::UInt8, false);
        let body = Field::new("body", DataType::Struct(vec![inner.clone()].into()), false);
        let struct_col: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(inner),
            Arc::new(UInt8Array::from(vec![1u8])) as ArrayRef,
        )]));
        let b = RecordBatch::try_new(Arc::new(Schema::new(vec![body])), vec![struct_col])
            .expect("batch");
        let any = any_value_col(&b, "body")
            .expect("struct body")
            .expect("present");
        // Type 1 is TYPE_STR with no `str` column: the tag names the variant and
        // the missing column means the type's default, so this is `Str("")`.
        assert_eq!(
            any.value_at(0, DecodeLimits::new(32, usize::MAX))
                .expect("value"),
            Value::Str(String::new())
        );
        assert!(any_value_col(&b, "absent").expect("absent").is_none());
    }

    /// Scenario: an attribute list holding a bytes value, which `render_v1`
    /// hex-encodes, and a nested value whose strings are full of characters
    /// JSON must escape.
    /// Guarantees: the rendered size counts both expansions, exceeds the
    /// decoded tree size `kv_bytes` reports, and agrees exactly with the cell
    /// [`map_cell`] builds -- so the two ways a row is charged cannot drift.
    #[test]
    fn rendered_bytes_count_hex_encoding_and_json_escaping() {
        let list = vec![
            ("b".to_string(), Value::Bytes(vec![0xFF; 100])),
            (
                "n".to_string(),
                Value::KvList(vec![(
                    "inner".to_string(),
                    Value::Array(vec![Value::Str("\"\\\n\t".repeat(50))]),
                )]),
            ),
        ];
        let (cell, bytes) = map_cell(&list);
        assert_eq!(bytes, rendered_kv_bytes(&list));
        assert!(rendered_kv_bytes(&list) > kv_bytes(&list));
        match cell {
            Col::Map(entries) => {
                assert_eq!(entries.len(), 2);
                // 100 bytes become 200 hex digits inside a pair of JSON quotes.
                let hex = entries[0].1.as_deref().expect("rendered bytes cell");
                assert_eq!(hex.len(), 202);
                // Every one of the 200 escaped characters becomes two.
                let nested = entries[1].1.as_deref().expect("rendered nested cell");
                assert!(nested.len() > 400);
            }
            other => unreachable!("expected a map cell, got {other:?}"),
        }
    }

    /// Scenario: a `resource` struct and an `AnyValue` `body` struct whose
    /// second row is null while their child buffers still hold live values --
    /// a shape a valid Arrow producer is free to emit.
    /// Guarantees: the parent struct's validity wins. The masked child reads as
    /// null instead of a fabricated resource id, and the masked body decodes to
    /// [`Value::Null`] instead of a fabricated log body.
    #[test]
    fn null_parent_struct_masks_its_children() {
        let nulls = NullBuffer::new(BooleanBuffer::from(vec![true, false]));
        let id_field = Arc::new(Field::new("id", DataType::UInt16, false));
        let resource = StructArray::new(
            vec![id_field].into(),
            vec![Arc::new(UInt16Array::from(vec![7u16, 9])) as ArrayRef],
            Some(nulls.clone()),
        );
        let body = StructArray::new(
            vec![
                Arc::new(Field::new("type", DataType::UInt8, false)),
                Arc::new(Field::new("str", DataType::Utf8, false)),
            ]
            .into(),
            vec![
                Arc::new(UInt8Array::from(vec![1u8, 1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["kept", "fabricated"])) as ArrayRef,
            ],
            Some(nulls),
        );
        let schema = Schema::new(vec![
            Field::new("resource", resource.data_type().clone(), true),
            Field::new("body", body.data_type().clone(), true),
        ]);
        let cols: Vec<ArrayRef> = vec![Arc::new(resource), Arc::new(body)];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");

        let ids = struct_child(&b, "resource", "id", &DataType::UInt16).expect("child");
        assert_eq!(opt_u16_at(&ids, 0), Some(7));
        assert_eq!(opt_u16_at(&ids, 1), None);

        let any = any_value_col(&b, "body").expect("body").expect("present");
        assert_eq!(
            any.value_at(0, DecodeLimits::new(32, usize::MAX))
                .expect("row 0"),
            Value::Str("kept".into())
        );
        assert_eq!(
            any.value_at(1, DecodeLimits::new(32, usize::MAX))
                .expect("row 1"),
            Value::Null
        );
    }
}
