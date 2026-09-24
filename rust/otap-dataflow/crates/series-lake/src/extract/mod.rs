// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extraction of descriptors and values from OTAP records (FORMAT.md
//! sections 1 to 3).

pub mod logs;
pub mod metrics;

use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder,
    Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructArray,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit, UInt32Type};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use crate::attrs::{AnyValueColumns, AttrTable, bytes_ref, prim_at, readable, str_ref};
use crate::canonical::{Descriptor, SeriesId, Signal, canonical_double_bits};
use crate::config::{DenormSource, DenormType, Denormalize, LakeConfig};
use crate::error::{Error, RefuseReason, Result};
use crate::schema::{Dataset, dataset_schema, denorm_columns};
use crate::value::{
    BUFFER_HEADER_BYTES, DecodeLimits, Value, kv_bytes, map_string, map_string_reserving,
    rendered_len,
};

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
    ///
    /// Counts the row's own decoded identity attributes, but not the resource
    /// and scope lists, which are shared by every row of the request that
    /// carries them and are charged once per request instead (see
    /// [`SharedLists`]).
    pub approx_bytes: usize,
    /// The decoded attribute tree bytes `approx_bytes` includes.
    ///
    /// Trees die at admission, so the block subtracts exactly these from the
    /// row's charge.
    pub decoded_bytes: usize,
    /// Columns of the series dataset the row is written to.
    columns: usize,
}

impl DescriptorRow {
    /// Conservative Arrow series-row estimate, including stamp-swap headroom;
    /// see [`series_row_charge`].
    #[must_use]
    pub fn series_row_bytes(&self) -> usize {
        series_row_charge(
            self.approx_bytes.saturating_sub(self.decoded_bytes),
            self.columns,
        )
    }
}

/// Bytes a block charges per series cell for builder growth, offsets and
/// validity.
pub(crate) const SERIES_CELL_BYTES: usize = 64;

/// Bytes a block charges per series row beyond its cells.
pub(crate) const SERIES_ROW_BYTES: usize = 8;

/// The block charge of one series row of `columns` cells whose content is
/// `content` bytes: twice content and cell overhead, the headroom of a
/// builder that doubles, plus [`SERIES_ROW_BYTES`]. Decoded attribute trees
/// die at admission and are not part of `content`.
pub(crate) fn series_row_charge(content: usize, columns: usize) -> usize {
    2 * (content + columns * SERIES_CELL_BYTES) + SERIES_ROW_BYTES
}

/// The resource or scope attribute lists of one request, decoded once per
/// parent id and shared by every descriptor that carries them.
///
/// A list is copied out of its attribute table and charged to the request's
/// budget the first time a new series needs it, never again: a request of
/// many series under one large resource holds and charges one copy.
#[derive(Default)]
pub(crate) struct SharedLists {
    lists: std::collections::HashMap<Option<u32>, Arc<[(String, Value)]>>,
}

impl SharedLists {
    /// Decoded bytes of every list handed out so far.
    pub(crate) fn bytes(&self) -> usize {
        self.lists.values().map(|list| kv_bytes(list)).sum()
    }

    /// The shared list of parent `id` in `table`, copied and charged on first
    /// use.
    pub(crate) fn get(
        &mut self,
        table: &AttrTable,
        id: Option<u32>,
        budget: &mut Budget,
    ) -> Result<Arc<[(String, Value)]>> {
        if let Some(list) = self.lists.get(&id) {
            return Ok(Arc::clone(list));
        }
        let source = attrs_of(table, id);
        budget.charge(kv_bytes(source))?;
        let list: Arc<[(String, Value)]> = Arc::from(source);
        let _ = self.lists.insert(id, Arc::clone(&list));
        Ok(list)
    }
}

/// Counters produced by extraction.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExtractStats {
    /// Values rows produced.
    pub rows: usize,
    /// Points dropped under the drop policy.
    pub dropped_unsupported: u64,
    /// Exemplar rows dropped (`dropped.exemplars`).
    pub dropped_exemplars: u64,
    /// Timestamps outside `1..=i64::MAX`.
    pub timestamp_out_of_range: u64,
    /// Denormalized values stored as null because of a type mismatch.
    pub denorm_type_mismatch: u64,
    /// Dropped exponential histogram points.
    pub dropped_exp_histogram: u64,
    /// Dropped summary points.
    pub dropped_summary: u64,
    /// Mismatches keyed only by configured physical column name.
    ///
    /// Bounded by the number of configured denormalize columns (the
    /// configuration fixes that set at startup), never by request content: the key is
    /// always one of `cfg.logs.denormalize`/`cfg.metrics.denormalize`'s
    /// `column` names, so no attacker-controlled label can grow this map.
    pub denorm_type_mismatch_by_column: std::collections::BTreeMap<String, u64>,
    /// Points whose series the per-request memo already knew, so no identity
    /// was encoded or hashed for them (metrics only).
    pub series_memo_hits: u64,
    /// Series identities encoded and hashed because the memo did not know
    /// the point's content (metrics only).
    pub series_memo_misses: u64,
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
    /// Decoded bytes of the resource and scope attribute lists the
    /// descriptors share, charged once per request.
    ///
    /// Retained with the descriptors until admission drops their trees, so
    /// whoever holds an extraction holds these bytes too.
    pub shared_bytes: usize,
    /// Counters.
    pub stats: ExtractStats,
}

/// Extract descriptors and values from one OTAP request.
///
/// Takes `&mut` because OTAP attribute batches carry quasi-delta encoded
/// `parent_id` columns, which `decode_transport_optimized_ids` must decode
/// before any `parent_id` is read. Decoding is
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
        .map_err(|e| Error::invalid(format!("transport-optimized ids: {e}")))?;
    let mut budget = Budget::new(cfg);
    match records {
        OtapArrowRecords::Logs(_) => logs::extract_logs(records, cfg, &mut budget),
        OtapArrowRecords::Metrics(_) => metrics::extract_metrics(records, cfg, &mut budget),
        OtapArrowRecords::Traces(_) => {
            Err(Error::Refused(RefuseReason::Unsupported("traces".into())))
        }
    }
}

/// The single byte accountant of one request.
///
/// One `Budget` is created in [`extract`] and threaded through every decoded
/// attribute table, descriptor row and values row, so that a request with many
/// small datasets or tables cannot spend the whole limit once per each.
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

    /// Charge one values or descriptor row.
    ///
    /// A single row larger than `max_row_bytes` refuses the request, as does a
    /// running total past `max_extracted_bytes`.
    pub(crate) fn charge_row(&mut self, bytes: usize) -> Result<()> {
        self.charge_row_holding(bytes, 0)
    }

    /// Charge a row of `bytes`, `held` of which the budget already holds for
    /// it; the row limit applies to the whole row.
    pub(crate) fn charge_row_holding(&mut self, bytes: usize, held: usize) -> Result<()> {
        self.check_row(bytes)?;
        let rest = bytes
            .checked_sub(held)
            .ok_or_else(|| Error::internal("a row holds more than its size"))?;
        self.charge(rest)
    }

    /// Refuse a row of `bytes` larger than `max_row_bytes`.
    pub(crate) fn check_row(&self, bytes: usize) -> Result<()> {
        if bytes > self.max_row {
            return Err(Error::too_large(
                crate::error::SizeBudget::Row,
                bytes,
                self.max_row,
            ));
        }
        Ok(())
    }

    /// Charge bytes that are not one row, such as a sealed batch.
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<()> {
        self.add(bytes, crate::error::SizeBudget::Extracted)
    }

    /// Charge decoded attribute values; past the limit they refuse the request
    /// as [`crate::error::SizeBudget::Table`].
    pub(crate) fn charge_decoded(&mut self, bytes: usize) -> Result<()> {
        self.add(bytes, crate::error::SizeBudget::Table)
    }

    /// A charge past the limit is refused and not recorded.
    fn add(&mut self, bytes: usize, budget: crate::error::SizeBudget) -> Result<()> {
        let used = self.used.saturating_add(bytes);
        if used > self.limit {
            return Err(Error::too_large(budget, used, self.limit));
        }
        self.used = used;
        Ok(())
    }

    /// Give bytes back, saturating at zero.
    ///
    /// Used when an estimate is superseded by a measurement, and when a row
    /// that was charged turns out to be a duplicate that is not retained.
    pub(crate) fn uncharge(&mut self, bytes: usize) {
        self.used = self.used.saturating_sub(bytes);
    }

    /// The point to return to with [`Budget::rollback`].
    pub(crate) fn mark(&self) -> Mark {
        Mark(self.used)
    }

    /// Return to `mark`, undoing every charge and release made since.
    pub(crate) fn rollback(&mut self, mark: Mark) {
        self.used = mark.0;
    }
}

/// A position of a [`Budget`], taken before work that may fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mark(usize);

/// Rendered cells reserve through the request budget and refuse as the
/// extracted output they are part of.
pub(crate) struct Rendering<'b>(pub(crate) &'b mut Budget);

impl crate::value::Reservations for Rendering<'_> {
    fn reserve(&mut self, bytes: usize) -> Result<()> {
        self.0.charge(bytes)
    }

    fn release(&mut self, bytes: usize) {
        self.0.uncharge(bytes);
    }
}

/// Decoded attribute values reserve through the request budget.
impl crate::value::Reservations for Budget {
    fn reserve(&mut self, bytes: usize) -> Result<()> {
        self.charge_decoded(bytes)
    }

    fn release(&mut self, bytes: usize) {
        self.uncharge(bytes);
    }
}

/// Approximate retained bytes of one denormalized cell.
pub(crate) fn denorm_bytes(v: &Option<DenormValue>) -> usize {
    denorm_cell_bytes(v.as_ref().map(|v| match v {
        DenormValue::Str(s) => Some(s.len()),
        _ => None,
    }))
}

/// [`denorm_bytes`] of an absent cell (`None`), a string of the given
/// length, or a scalar (`Some(None)`).
fn denorm_cell_bytes(cell: Option<Option<usize>>) -> usize {
    match cell {
        None => 8,
        Some(Some(len)) => len + BUFFER_HEADER_BYTES,
        Some(None) => 16,
    }
}

/// The timestamp rule of FORMAT.md section 2, on the converted `i64`
/// nanoseconds.
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

/// [`map_string`] of a value, borrowing a string instead of copying it.
fn map_str(v: &Value) -> Option<Cow<'_, str>> {
    match v {
        Value::Str(s) => Some(Cow::Borrowed(s)),
        other => map_string(other).map(Cow::Owned),
    }
}

/// [`map_string_reserving`] of a value, borrowing a string instead of
/// copying it; the borrowed string's length is reserved all the same.
fn map_str_reserving<'a>(v: &'a Value, budget: &mut Budget) -> Result<Option<Cow<'a, str>>> {
    match v {
        Value::Str(s) => {
            budget.charge(s.len())?;
            Ok(Some(Cow::Borrowed(s)))
        }
        other => Ok(map_string_reserving(other, &mut Rendering(budget))?.map(Cow::Owned)),
    }
}

/// A denormalized value borrowed from the attribute list it was found in.
enum DenormRef<'a> {
    Str(Cow<'a, str>),
    Int(i64),
    Double(f64),
    Bool(bool),
}

/// Resolve one denormalized column for a row.
pub(crate) fn denorm_lookup(
    d: &Denormalize,
    resource: &[(String, Value)],
    scope: &[(String, Value)],
    attrs: &[(String, Value)],
    stats: &mut ExtractStats,
) -> Option<DenormValue> {
    Some(match denorm_ref(d, resource, scope, attrs, stats)? {
        DenormRef::Str(s) => DenormValue::Str(s.into_owned()),
        DenormRef::Int(i) => DenormValue::Int(i),
        DenormRef::Double(f) => DenormValue::Double(f),
        DenormRef::Bool(b) => DenormValue::Bool(b),
    })
}

/// The values-row cell of one denormalized column and the bytes it retains,
/// [`denorm_bytes`] of what [`denorm_lookup`] resolves.
pub(crate) fn denorm_col<'a>(
    d: &Denormalize,
    resource: &'a [(String, Value)],
    scope: &'a [(String, Value)],
    attrs: &'a [(String, Value)],
    stats: &mut ExtractStats,
) -> (Col<'a>, usize) {
    match denorm_ref(d, resource, scope, attrs, stats) {
        None => (Col::Str(None), denorm_cell_bytes(None)),
        Some(DenormRef::Str(s)) => {
            let bytes = denorm_cell_bytes(Some(Some(s.len())));
            (Col::Str(Some(s)), bytes)
        }
        Some(DenormRef::Int(i)) => (Col::Int(Some(i)), denorm_cell_bytes(Some(None))),
        Some(DenormRef::Double(f)) => (Col::Double(Some(f)), denorm_cell_bytes(Some(None))),
        Some(DenormRef::Bool(b)) => (Col::Bool(Some(b)), denorm_cell_bytes(Some(None))),
    }
}

fn denorm_ref<'a>(
    d: &Denormalize,
    resource: &'a [(String, Value)],
    scope: &'a [(String, Value)],
    attrs: &'a [(String, Value)],
    stats: &mut ExtractStats,
) -> Option<DenormRef<'a>> {
    let (src, key) = d.source().ok()?;
    let v = match src {
        DenormSource::Resource => lookup(resource, key),
        DenormSource::Scope => lookup(scope, key),
        DenormSource::Attrs => lookup(attrs, key),
    }?;
    match (d.ty, v) {
        (DenormType::String, v) => map_str(v).map(DenormRef::Str),
        (DenormType::Int64, Value::Int(i)) => Some(DenormRef::Int(*i)),
        (DenormType::Double, Value::Double(f)) => Some(DenormRef::Double(*f)),
        (DenormType::Bool, Value::Bool(b)) => Some(DenormRef::Bool(*b)),
        (_, Value::Null) => None,
        _ => {
            stats.denorm_type_mismatch += 1;
            // Only the first mismatch for a column allocates: a lookup by
            // borrowed `&str` finds every later hit against the same key
            // without cloning it again, which matters here because this runs
            // once per values row in the request's hot path, not just once
            // per descriptor.
            match stats
                .denorm_type_mismatch_by_column
                .get_mut(d.column.as_str())
            {
                Some(count) => *count += 1,
                None => {
                    let _ = stats
                        .denorm_type_mismatch_by_column
                        .insert(d.column.clone(), 1);
                }
            }
            None
        }
    }
}

/// Producer id projection of a resource attribute list.
fn producer_id<'a>(resource: &'a [(String, Value)], attribute: &str) -> Cow<'a, str> {
    lookup(resource, attribute)
        .and_then(map_str)
        .unwrap_or_default()
}

/// The producer ids of one request, projected once per resource id.
pub(crate) struct Producers<'a> {
    attribute: &'a str,
    ids: std::collections::HashMap<Option<u32>, Cow<'a, str>>,
}

impl<'a> Producers<'a> {
    pub(crate) fn new(cfg: &'a LakeConfig) -> Self {
        Self {
            attribute: &cfg.producer_id_attribute,
            ids: std::collections::HashMap::new(),
        }
    }

    /// The producer id of resource `id`, whose attributes are `resource`.
    pub(crate) fn get(&mut self, id: Option<u32>, resource: &'a [(String, Value)]) -> &str {
        self.ids
            .entry(id)
            .or_insert_with(|| producer_id(resource, self.attribute))
    }
}

/// The hasher of the per-request series memos, seeded once per process so
/// request content cannot choose its collisions.
pub(crate) type MemoHasher = hashbrown::DefaultHashBuilder;

/// Attribute lists equal under the canonical encoding's value rules (see
/// [`value_eq`]), so equal lists always encode to the same identity.
pub(crate) fn kv_eq(a: &[(String, Value)], b: &[(String, Value)]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| entry_eq(x, y))
}

/// One attribute entry equal under the canonical encoding's value rules.
pub(crate) fn entry_eq((ka, va): &(String, Value), (kb, vb): &(String, Value)) -> bool {
    ka == kb && value_eq(va, vb)
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

/// Hash an attribute list consistently with [`kv_eq`].
pub(crate) fn hash_kv<'a, H: Hasher>(
    entries: impl ExactSizeIterator<Item = &'a (String, Value)>,
    state: &mut H,
) {
    entries.len().hash(state);
    for (key, value) in entries {
        key.hash(state);
        hash_value(value, state);
    }
}

/// Hash one attribute value consistently with [`value_eq`]: equal values hash
/// alike, a double by its canonical bits.
fn hash_value<H: Hasher>(value: &Value, state: &mut H) {
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
            hash_kv(entries.iter(), state);
        }
    }
}

/// A typed cell of a values row, in dataset schema order, borrowed from the
/// request wherever the stored value is the request's own.
#[derive(Debug, Clone)]
pub(crate) enum Col<'a> {
    /// Utf8.
    Str(Option<Cow<'a, str>>),
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
    Fixed(Option<&'a [u8]>),
    /// Map<Utf8, Utf8>.
    Map(Vec<(&'a str, Option<Cow<'a, str>>)>),
    /// List<Int64>. `None` is a null list, which a number point writes into
    /// the merged metrics values dataset.
    ListI64(Option<Vec<i64>>),
    /// List<Float64>. `None` is a null list, which a number point writes into
    /// the merged metrics values dataset.
    ListF64(Option<Cow<'a, [f64]>>),
    /// Binary.
    Bytes(&'a [u8]),
}

impl<'a> From<&'a Option<DenormValue>> for Col<'a> {
    fn from(v: &'a Option<DenormValue>) -> Self {
        match v {
            None => Col::Str(None),
            Some(DenormValue::Str(s)) => Col::Str(Some(Cow::Borrowed(s))),
            Some(DenormValue::Int(i)) => Col::Int(Some(*i)),
            Some(DenormValue::Double(f)) => Col::Double(Some(*f)),
            Some(DenormValue::Bool(b)) => Col::Bool(Some(*b)),
        }
    }
}

/// A string cell borrowed from the request.
pub(crate) fn str_col(s: &str) -> Col<'_> {
    Col::Str(Some(Cow::Borrowed(s)))
}

/// One values row.
#[derive(Debug, Clone)]
pub(crate) struct ValuesRow<'a> {
    /// Cells in dataset schema order.
    pub cols: Vec<Col<'a>>,
    /// Approximate retained bytes.
    pub approx_bytes: usize,
    /// The part of `approx_bytes` the budget already holds, reserved while a
    /// cell was built.
    pub held_bytes: usize,
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

/// Bytes a string or binary builder starts with per row when nothing better
/// is known about its column.
const DEFAULT_CELL_BYTES: usize = 16;

/// A builder for `dt` with room for `rows` rows and, for a string, binary or
/// list column, `items` bytes or list items (a default per row when `None`).
fn builder_for(dt: &DataType, rows: usize, items: Option<usize>) -> Result<AnyBuilder> {
    let bytes = items.unwrap_or(rows * DEFAULT_CELL_BYTES);
    let items = items.unwrap_or(0);
    Ok(match dt {
        DataType::Utf8 => AnyBuilder::Str(StringBuilder::with_capacity(rows, bytes)),
        DataType::Int64 => AnyBuilder::Int(Int64Builder::with_capacity(rows)),
        DataType::Int32 => AnyBuilder::Int32(Int32Builder::with_capacity(rows)),
        DataType::Float64 => AnyBuilder::Double(Float64Builder::with_capacity(rows)),
        DataType::Boolean => AnyBuilder::Bool(BooleanBuilder::with_capacity(rows)),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => AnyBuilder::TsUs(
            TimestampMicrosecondBuilder::with_capacity(rows).with_timezone_opt(tz.clone()),
        ),
        DataType::FixedSizeBinary(n) => {
            AnyBuilder::Fixed(FixedSizeBinaryBuilder::with_capacity(rows, *n))
        }
        DataType::Map(_, _) => AnyBuilder::Map(MapBuilder::with_capacity(
            None,
            StringBuilder::with_capacity(rows, bytes),
            StringBuilder::with_capacity(rows, bytes),
            rows,
        )),
        DataType::List(f) if f.data_type() == &DataType::Int64 => AnyBuilder::ListI64(
            ListBuilder::with_capacity(Int64Builder::with_capacity(items), rows)
                .with_field(f.clone()),
        ),
        DataType::List(f) => AnyBuilder::ListF64(
            ListBuilder::with_capacity(Float64Builder::with_capacity(items), rows)
                .with_field(f.clone()),
        ),
        DataType::Binary => AnyBuilder::Bytes(BinaryBuilder::with_capacity(rows, bytes)),
        other => return Err(Error::internal(format!("unsupported builder type {other}"))),
    })
}

fn append(b: &mut AnyBuilder, c: &Col<'_>) -> Result<()> {
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
        (AnyBuilder::ListI64(b), Col::ListI64(items)) => match items {
            Some(items) => {
                b.values().append_slice(items);
                b.append(true);
            }
            None => b.append_null(),
        },
        (AnyBuilder::ListF64(b), Col::ListF64(items)) => match items {
            Some(items) => {
                b.values().append_slice(items);
                b.append(true);
            }
            None => b.append_null(),
        },
        (AnyBuilder::Bytes(b), Col::Bytes(v)) => b.append_value(v),
        // Denormalized columns arrive as Col::Str(None) when absent, whatever their type.
        (AnyBuilder::Int(b), Col::Str(None)) => b.append_null(),
        (AnyBuilder::Double(b), Col::Str(None)) => b.append_null(),
        (AnyBuilder::Bool(b), Col::Str(None)) => b.append_null(),
        (_, c) => {
            return Err(Error::internal(format!(
                "column/builder mismatch for {c:?}"
            )));
        }
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
/// `run_target_bytes`, so a sealed slice never exceeds the target. Row and
/// request limits are enforced by the shared [`Budget`], which
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
    /// New sink for one dataset, its builders sized for `rows` rows and for
    /// the known content of the columns `items` names (bytes of a string
    /// column, items of a list column), each at most `run_target_bytes`.
    pub(crate) fn new(
        ds: Dataset,
        cfg: &LakeConfig,
        rows: usize,
        items: &[(&str, usize)],
    ) -> Result<Self> {
        let schema = dataset_schema(ds, cfg);
        let run_target = cfg.sorting.run_target_bytes;
        let builders = schema
            .fields()
            .iter()
            .map(|f| {
                let known = items
                    .iter()
                    .find(|(name, _)| name == f.name())
                    .map(|&(_, n)| n.min(run_target));
                builder_for(f.data_type(), rows, known)
            })
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

    /// Cells of every row of the dataset.
    pub(crate) fn width(&self) -> usize {
        self.builders.len()
    }

    /// Append one row, sealing the current slice first when it would overflow.
    pub(crate) fn push(&mut self, row: &ValuesRow<'_>, budget: &mut Budget) -> Result<()> {
        budget.charge_row_holding(row.approx_bytes, row.held_bytes)?;
        if row.cols.len() != self.builders.len() {
            return Err(Error::internal("row width does not match dataset schema"));
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
/// `render_v1` base64-encodes a bytes value (four characters per three bytes)
/// and JSON-escapes
/// strings (which can expand them several-fold). Charging the tree's size would
/// therefore admit a row whose stored form is far past `max_row_bytes`, which no
/// later recount can undo. The caller charges the returned size.
pub(crate) fn map_cell(list: &[(String, Value)]) -> (Col<'_>, usize) {
    let entries: Vec<(&str, Option<Cow<'_, str>>)> =
        list.iter().map(|(k, v)| (k.as_str(), map_str(v))).collect();
    let bytes = entries
        .iter()
        .map(|(k, v)| rendered_entry_bytes(k, v.as_deref()))
        .sum();
    (Col::Map(entries), bytes)
}

/// [`map_cell`] within the request budget, for a row whose other cells come
/// to `row_before` bytes.
///
/// Each entry's key and rendered value are reserved before they are
/// allocated and stay held for the row; the row is refused as soon as it
/// passes `max_row_bytes`, and a refusal rolls the budget back.
pub(crate) fn map_cell_reserving<'a>(
    list: impl IntoIterator<Item = &'a (String, Value)>,
    row_before: usize,
    budget: &mut Budget,
) -> Result<(Col<'a>, usize)> {
    let mark = budget.mark();
    let cell = map_cell_within(list, row_before, budget);
    if cell.is_err() {
        budget.rollback(mark);
    }
    cell
}

fn map_cell_within<'a>(
    list: impl IntoIterator<Item = &'a (String, Value)>,
    row_before: usize,
    budget: &mut Budget,
) -> Result<(Col<'a>, usize)> {
    let mut entries: Vec<(&str, Option<Cow<'_, str>>)> = Vec::new();
    let mut held = 0_usize;
    for (k, v) in list {
        let key = rendered_entry_bytes(k, None);
        budget.charge(key)?;
        let rendered = map_str_reserving(v, budget)?;
        held += rendered_entry_bytes(k, rendered.as_deref());
        budget.check_row(row_before.saturating_add(held))?;
        entries.push((k, rendered));
    }
    Ok((Col::Map(entries), held))
}

/// Bytes one rendered map entry retains.
fn rendered_entry_bytes(key: &str, rendered: Option<&str>) -> usize {
    key.len() + BUFFER_HEADER_BYTES + rendered.map_or(0, str::len)
}

/// Bytes a sorted attribute list occupies once rendered into a map cell,
/// measured without rendering it. Used where the cell itself is built later
/// (a descriptor is rendered when its request is admitted) and only its size
/// is needed now.
pub(crate) fn rendered_kv_bytes(list: &[(String, Value)]) -> usize {
    list.iter()
        .map(|(k, v)| k.len() + BUFFER_HEADER_BYTES + rendered_len(v))
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

/// Column `name` of `batch` as a `T`, or `None` when it is absent.
///
/// The downcast is checked: a column that is present but is not a `T` refuses
/// the request as invalid instead of panicking. `OtapArrowRecords` validates
/// the OTAP schema on the way in, so this can only be reached by a batch built
/// outside that validation.
pub(crate) fn typed_col<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
    what: &str,
) -> Result<Option<&'a T>> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<T>()
                .ok_or_else(|| Error::invalid(format!("column {name} is not {what}")))
        })
        .transpose()
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
    let Some(s) = typed_col::<StructArray>(batch, parent, "a struct")? else {
        return Ok(None);
    };
    flat_children(s)
        .into_iter()
        .find(|(name, _)| name == child)
        .map(|(_, c)| readable(c, to))
        .transpose()
}

/// The `AnyValue` struct column `name` of a batch, or `None` when it is absent.
///
/// The struct's own validity is applied to its children (see
/// [`flat_children`]), so a null body row reads as a null type tag and becomes
/// [`Value::Null`].
pub(crate) fn any_value_col(batch: &RecordBatch, name: &str) -> Result<Option<AnyValueColumns>> {
    let Some(s) = typed_col::<StructArray>(batch, name, "an AnyValue struct")? else {
        return Ok(None);
    };
    let children = flat_children(s);
    Ok(Some(AnyValueColumns::new(&|n| {
        children
            .iter()
            .find(|(k, _)| k == n)
            .map(|(_, a)| Arc::clone(a))
    })?))
}

/// Attribute table for a payload type, empty when the payload is absent,
/// charged to `budget` (see [`AttrTable::from_batch`]).
pub(crate) fn attr_table(
    records: &OtapArrowRecords,
    pt: ArrowPayloadType,
    limits: DecodeLimits,
    budget: &mut Budget,
) -> Result<AttrTable> {
    match records.get(pt) {
        Some(b) => AttrTable::from_batch(b, limits, budget),
        None => Ok(AttrTable::default()),
    }
}

/// Attributes of an optional parent id: the empty list when the id is `None`.
///
/// `None` means "no parent": pdata writes a null id for a record or point that
/// carries no attributes. A `None` must never be turned into id 0, which would
/// make such a record inherit the attributes of parent 0.
pub(crate) fn attrs_of(table: &AttrTable, id: Option<u32>) -> &[(String, Value)] {
    match id {
        Some(id) => table.get(id),
        None => &[],
    }
}

/// A Utf8 column read as an owned `String`, empty when null or absent.
///
/// Reads through a dictionary (see [`readable`]), so only this one cell is
/// copied.
pub(crate) fn str_at(a: &Option<ArrayRef>, row: usize) -> String {
    str_of(a, row).to_owned()
}

/// A Utf8 column's cell borrowed from it, empty when null or absent.
pub(crate) fn str_of(a: &Option<ArrayRef>, row: usize) -> &str {
    a.as_ref().and_then(|a| str_ref(a, row)).unwrap_or("")
}

/// A `UInt32` OTLP flags column reinterpreted into the signed storage column.
///
/// The flags are a bit set, stored as received; readers interpret the bits. The
/// storage column is `Int32` for Parquet portability, so the top bit wraps into
/// the sign bit.
#[allow(clippy::cast_possible_wrap)]
pub(crate) fn flags_at(a: &Option<ArrayRef>, row: usize) -> i32 {
    prim_at::<UInt32Type>(a, row).unwrap_or(0) as i32
}

/// A `FixedSizeBinary` column's cell borrowed from it, `None` when null or
/// absent.
pub(crate) fn fixed_ref(a: &Option<ArrayRef>, row: usize) -> Option<&[u8]> {
    a.as_ref().and_then(|a| bytes_ref(a, row))
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
        .map(|f| builder_for(f.data_type(), rows.len(), None))
        .collect::<Result<Vec<_>>>()?;
    for r in rows {
        let d = &r.descriptor;
        let mut cols: Vec<Col<'_>> = vec![
            Col::Fixed(Some(&r.series_id)),
            Col::Bytes(&r.identity_bytes),
            Col::TsUs(Some(emitted_at_us)),
            str_col(&d.resource_schema_url),
            map_cell(&d.resource_attrs).0,
            str_col(&d.scope_name),
            str_col(&d.scope_version),
            str_col(&d.scope_schema_url),
            map_cell(&d.scope_attrs).0,
            map_cell(&d.attrs).0,
        ];
        if ds == Dataset::MetricsSeries {
            let m = d
                .metric
                .as_ref()
                .ok_or_else(|| Error::internal("metrics descriptor without metric"))?;
            cols.extend([
                str_col(&m.name),
                str_col(&m.unit),
                str_col(m.kind.as_str()),
                str_col(m.temporality.as_str()),
                Col::Bool(Some(m.is_monotonic)),
                str_col(&m.description),
            ]);
        }
        cols.extend(r.denorm.iter().map(Col::from));
        for (b, c) in builders.iter_mut().zip(&cols) {
            append(b, c)?;
        }
    }
    // The builders are sized from the row count and a per-row guess of each
    // string's bytes, so release what the guess left unused: the block
    // charges what it retains, and a run is measured against `run_target_bytes`
    // straight after this call. `Array::shrink_to_fit` on an `ArrayRef` shrinks
    // through `Arc::get_mut`, which succeeds because `finish` has just produced
    // a uniquely owned array.
    let arrays: Vec<ArrayRef> = builders
        .iter_mut()
        .map(|builder| {
            let mut array = finish(builder);
            array.shrink_to_fit();
            array
        })
        .collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// The canonical identity bytes of a descriptor and the series id they hash
/// to.
pub(crate) fn identity(descriptor: &Descriptor) -> (Vec<u8>, SeriesId) {
    let identity_bytes = crate::canonical::canonical_bytes(descriptor);
    let series_id = crate::canonical::series_id(&identity_bytes);
    (identity_bytes, series_id)
}

/// Build and charge the series row of a descriptor whose identity is already
/// known to be new in this request. Shared by logs and metrics.
///
/// The row is charged to `budget` like any other row, so an oversized
/// descriptor refuses the request through `max_row_bytes` and a request with
/// very many series refuses through `max_extracted_bytes`.
///
/// The caller computes the identity with [`identity`] and checks it against
/// the series it already holds first, so a duplicate is never built, looked
/// up for denormalized columns or charged. `columns` is
/// [`LakeConfig::series_columns`] of the signal, computed once per request.
pub(crate) fn descriptor_row(
    descriptor: Descriptor,
    (identity_bytes, series_id): (Vec<u8>, SeriesId),
    ds_series: Dataset,
    columns: usize,
    cfg: &LakeConfig,
    stats: &mut ExtractStats,
    budget: &mut Budget,
) -> Result<DescriptorRow> {
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
    // The identity attribute list is counted twice on purpose: extraction
    // temporarily retains both the decoded tree and its future rendered series
    // row; admission drops the tree. The rendered map cell
    // (`rendered_kv_bytes`) can be much larger than the tree, through base64
    // encoding and JSON escaping. The resource and scope trees are shared by
    // the request's rows and charged once, by `SharedLists`; each row still
    // renders its own copy of them.
    let decoded_bytes = kv_bytes(&descriptor.attrs);
    let approx_bytes = 16
        + identity_bytes.len()
        + 8
        + descriptor.resource_schema_url.len()
        + descriptor.scope_name.len()
        + descriptor.scope_version.len()
        + descriptor.scope_schema_url.len()
        + decoded_bytes
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
        decoded_bytes,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DenormType, Denormalize};
    use crate::error::RefuseReason;
    use arrow::array::{ListArray, StringArray, UInt8Array, UInt16Array};
    use arrow::buffer::{BooleanBuffer, NullBuffer};
    use arrow::datatypes::UInt16Type;
    use arrow::datatypes::{Field, Schema};

    /// Bytes the attribute tables of `payloads` charge to a request budget.
    pub(in crate::extract) fn table_bytes(
        records: &OtapArrowRecords,
        cfg: &LakeConfig,
        payloads: &[ArrowPayloadType],
    ) -> usize {
        let limits = DecodeLimits::new(cfg.ingress.max_nesting_depth, cfg.ingress.max_row_bytes);
        let mut budget = Budget::new(cfg);
        for &pt in payloads {
            let _table = attr_table(records, pt, limits, &mut budget).expect("attribute table");
        }
        budget.used
    }

    fn batch_with_utf8(name: &str) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(name, DataType::Utf8, true)]);
        let cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["x"]))];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: `struct_child` on a batch whose `resource` column is a string column.
    /// Guarantees: the request is refused as invalid content, not panicked on.
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

    /// Scenario: `typed_col` asked for a list where `bucket_counts` is a string column.
    /// Guarantees: the request is refused as invalid content; an absent column is absent.
    #[test]
    fn typed_col_refuses_a_column_of_another_type() {
        let b = batch_with_utf8("bucket_counts");
        assert!(matches!(
            typed_col::<ListArray>(&b, "bucket_counts", "a list"),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        assert!(
            typed_col::<ListArray>(&b, "explicit_bounds", "a list")
                .expect("absent column")
                .is_none()
        );
    }

    /// Scenario: `any_value_col` on a batch whose `body` column is a string column.
    /// Guarantees: the request is refused as invalid content; a struct body is accepted.
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
        let mut any = any_value_col(&b, "body")
            .expect("struct body")
            .expect("present");
        // Type 1 is TYPE_STR with no `str` column: the tag names the variant and
        // the missing column means the type's default, so this is `Str("")`.
        assert_eq!(
            any.value_at(
                0,
                DecodeLimits::new(32, usize::MAX),
                &mut Budget::new(&LakeConfig::default())
            )
            .expect("value"),
            Value::Str(String::new())
        );
        assert!(any_value_col(&b, "absent").expect("absent").is_none());
    }

    /// Scenario: two configured columns that both mismatch, looked up three times each.
    /// Guarantees: one entry per configured column, each counting its own lookups, summing to the
    /// aggregate.
    #[test]
    fn denorm_type_mismatch_by_column_is_bounded_by_configured_columns() {
        let a = Denormalize {
            path: "resource.svc".into(),
            column: "col_a".into(),
            ty: DenormType::Int64,
        };
        let b = Denormalize {
            path: "resource.svc".into(),
            column: "col_b".into(),
            ty: DenormType::Bool,
        };
        let resource = vec![("svc".to_string(), Value::Str("not-an-int".into()))];
        let mut stats = ExtractStats::default();
        for d in [&a, &a, &a, &b, &b] {
            assert!(denorm_lookup(d, &resource, &[], &[], &mut stats).is_none());
        }
        assert_eq!(
            stats.denorm_type_mismatch_by_column.len(),
            2,
            "one entry per configured column, however many times it is looked up"
        );
        assert_eq!(stats.denorm_type_mismatch_by_column.get("col_a"), Some(&3));
        assert_eq!(stats.denorm_type_mismatch_by_column.get("col_b"), Some(&2));
        assert_eq!(
            stats.denorm_type_mismatch_by_column.values().sum::<u64>(),
            stats.denorm_type_mismatch
        );
    }

    /// Scenario: a residual list of bytes, nested, escaped and null values, and three strings of
    /// 600 000 control characters (3.6 MB escaped each) under a 1 MiB row limit.
    /// Guarantees: the ordinary list renders as `map_cell` does and holds its bytes; the large one
    /// is refused on its first value with the budget at its mark.
    #[test]
    fn a_rendered_map_is_held_and_refused_as_soon_as_the_row_is_too_large() {
        let list = vec![
            ("b".to_string(), Value::Bytes(vec![0xFF; 100])),
            (
                "n".to_string(),
                Value::KvList(vec![("i".into(), Value::Array(vec![Value::Double(0.5)]))]),
            ),
            ("q".to_string(), Value::Str("\"\\\n".repeat(5))),
            ("z".to_string(), Value::Null),
        ];
        let mut budget = Budget::new(&LakeConfig::default());
        let mark = budget.mark();
        let (cell, held) = map_cell_reserving(&list, 100, &mut budget).expect("fits");
        let (expected, bytes) = map_cell(&list);
        assert_eq!(format!("{cell:?}"), format!("{expected:?}"));
        assert_eq!(held, bytes);
        assert_eq!(held, rendered_kv_bytes(&list));
        budget.uncharge(held);
        assert_eq!(budget.mark(), mark);

        let big = Value::Array(vec![Value::Str("\u{1}".repeat(600_000))]);
        let list: Vec<(String, Value)> = (0..3).map(|i| (format!("a{i}"), big.clone())).collect();
        let mut budget = Budget::new(&LakeConfig::default());
        let mark = budget.mark();
        assert!(matches!(
            map_cell_reserving(&list, 100, &mut budget),
            Err(Error::Refused(RefuseReason::RequestTooLarge(
                crate::error::Excess {
                    budget: crate::error::SizeBudget::Row,
                    ..
                }
            )))
        ));
        assert_eq!(budget.mark(), mark);
    }

    /// Scenario: a `resource` and a `body` struct whose second row is null over live child values.
    /// Guarantees: the parent's validity wins: a null resource and a `Value::Null` body.
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
        assert_eq!(prim_at::<UInt16Type>(&ids, 0), Some(7));
        assert_eq!(prim_at::<UInt16Type>(&ids, 1), None);

        let mut any = any_value_col(&b, "body").expect("body").expect("present");
        assert_eq!(
            any.value_at(
                0,
                DecodeLimits::new(32, usize::MAX),
                &mut Budget::new(&LakeConfig::default())
            )
            .expect("row 0"),
            Value::Str("kept".into())
        );
        assert_eq!(
            any.value_at(
                1,
                DecodeLimits::new(32, usize::MAX),
                &mut Budget::new(&LakeConfig::default())
            )
            .expect("row 1"),
            Value::Null
        );
    }
}
