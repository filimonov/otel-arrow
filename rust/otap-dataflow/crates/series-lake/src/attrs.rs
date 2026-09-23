// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reads OTAP attribute record batches into per-parent, sorted attribute lists.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, UInt8Array, UInt16Array,
    UInt32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt8Type, UInt16Type};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::arrays::{
    ByteArrayAccessor, MaybeDictArrayAccessor, NullableArrayAccessor, StringArrayAccessor,
};
use otel_arrow_dfe_pdata::otlp::attributes::AttributeValueType;
use otel_arrow_dfe_pdata::schema::consts::{
    ATTRIBUTE_BOOL, ATTRIBUTE_BYTES, ATTRIBUTE_DOUBLE, ATTRIBUTE_INT, ATTRIBUTE_KEY, ATTRIBUTE_SER,
    ATTRIBUTE_STR, ATTRIBUTE_TYPE, PARENT_ID,
};

use crate::error::{Error, Result};
use crate::extract::Budget;
use crate::value::{DecodeLimits, Value, decode_cbor, entry_bytes, sort_kvlist, value_bytes};

/// Attributes of one OTAP attribute batch, grouped by parent id.
#[derive(Debug, Default)]
pub struct AttrTable {
    groups: HashMap<u32, Vec<(String, Value)>>,
    /// Decoded bytes the table charged to its request's budget.
    bytes: usize,
}

/// Fetch `name` from `batch` as `to`, or `None` when the column is absent.
///
/// This is the single "bring this column to a plain type" helper of the
/// crate; `extract` reuses it instead of defining its own. See [`readable`]
/// for which columns are cast and which are kept as they are.
pub(crate) fn plain(batch: &RecordBatch, name: &str, to: &DataType) -> Result<Option<ArrayRef>> {
    batch
        .column_by_name(name)
        .map(|col| readable(Arc::clone(col), to))
        .transpose()
}

/// `col` in a form the cell readers of this crate can read as `to`.
///
/// A column that already is `to` is returned as it is, and so is a
/// dictionary with `u8` or `u16` keys -- the only key types OTAP uses -- whose
/// values are `to`, for the string, binary, fixed-size binary and 64-bit
/// integer types: [`str_cell`], [`bytes_cell`] and [`prim_at`] read
/// through the dictionary instead. Anything else is cast.
///
/// Casting a dictionary would expand it before any budget applies: one
/// large value referenced by every row becomes one copy per row, and the
/// logical request size a producer is measured against counts that value
/// once. Reading through it keeps the expansion to the cells actually read,
/// each of which is charged as it is read.
pub(crate) fn readable(col: ArrayRef, to: &DataType) -> Result<ArrayRef> {
    let through = match col.data_type() {
        t if t == to => true,
        DataType::Dictionary(key, value) => {
            matches!(**key, DataType::UInt8 | DataType::UInt16)
                && **value == *to
                && matches!(
                    to,
                    DataType::Utf8
                        | DataType::Binary
                        | DataType::FixedSizeBinary(_)
                        | DataType::Int64
                )
        }
        _ => false,
    };
    if through {
        Ok(col)
    } else {
        Ok(cast(&col, to)?)
    }
}

/// Apply `f` to the string at `row` of a [`readable`] `Utf8` column, reading
/// through a dictionary without copying; `None` when the cell is null.
pub(crate) fn str_cell<R>(a: &ArrayRef, row: usize, f: impl FnOnce(&str) -> R) -> Option<R> {
    if let Some(strings) = a.as_string_opt::<i32>() {
        return strings.is_valid(row).then(|| f(strings.value(row)));
    }
    StringArrayAccessor::try_new(a).ok()?.str_at(row).map(f)
}

/// Apply `f` to the bytes at `row` of a [`readable`] binary or fixed-size
/// binary column, reading through a dictionary without copying; `None` when
/// the cell is null.
pub(crate) fn bytes_cell<R>(a: &ArrayRef, row: usize, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
    if let Some(bytes) = a.as_binary_opt::<i32>() {
        return bytes.is_valid(row).then(|| f(bytes.value(row)));
    }
    if let Some(bytes) = a.as_fixed_size_binary_opt() {
        return bytes.is_valid(row).then(|| f(bytes.value(row)));
    }
    ByteArrayAccessor::try_new(a).ok()?.slice_at(row).map(f)
}

/// The value at `row` of a [`readable`] primitive column, reading through a
/// dictionary; `None` when the column is absent or the cell is null.
pub(crate) fn prim_at<T: ArrowPrimitiveType>(
    a: &Option<ArrayRef>,
    row: usize,
) -> Option<T::Native> {
    let a = a.as_ref()?;
    if let Some(values) = a.as_primitive_opt::<T>() {
        return values.is_valid(row).then(|| values.value(row));
    }
    MaybeDictArrayAccessor::<PrimitiveArray<T>>::try_new(a)
        .ok()?
        .value_at(row)
}

/// The value at `row` of a [`readable`] `Boolean` column; `None` when the
/// column is absent or the cell is null.
pub(crate) fn bool_at(a: &Option<ArrayRef>, row: usize) -> Option<bool> {
    let a = a.as_ref()?.as_boolean_opt()?;
    a.is_valid(row).then(|| a.value(row))
}

/// Refuse one cell whose payload alone is larger than a row may be.
///
/// Checked on the borrowed cell, before it is copied: the row that would hold
/// it is refused as too large in any case, and a dictionary value referenced
/// by many rows must not be copied once per row first.
fn cell_fits(len: usize, limits: DecodeLimits) -> Result<()> {
    if len > limits.max_cell_bytes {
        return Err(Error::too_large(
            crate::error::SizeBudget::Cell,
            len,
            limits.max_cell_bytes,
        ));
    }
    Ok(())
}

/// The dictionary values of a `u8`- or `u16`-keyed column and its key at
/// `row` (`None` for a null key), or `None` for a plain column.
fn dictionary_at(a: &ArrayRef, row: usize) -> Option<(&ArrayRef, Option<usize>)> {
    if let Some(d) = a.as_dictionary_opt::<UInt8Type>() {
        return Some((d.values(), d.key(row)));
    }
    a.as_dictionary_opt::<UInt16Type>()
        .map(|d| (d.values(), d.key(row)))
}

/// Rows referencing each of the `values` entries of a dictionary column.
fn reference_counts(a: &ArrayRef, values: usize) -> Vec<u32> {
    let mut refs = vec![0_u32; values];
    for row in 0..a.len() {
        if let Some((_, Some(key))) = dictionary_at(a, row)
            && let Some(count) = refs.get_mut(key)
        {
            *count += 1;
        }
    }
    refs
}

/// The seven `AnyValue` columns (type, str, int, double, bool, bytes, ser) of an
/// attribute batch or a log body struct, cast to plain types.
pub(crate) struct AnyValueColumns {
    types: UInt8Array,
    strs: Option<ArrayRef>,
    ints: Option<ArrayRef>,
    doubles: Option<ArrayRef>,
    bools: Option<ArrayRef>,
    bytes: Option<ArrayRef>,
    sers: Option<ArrayRef>,
    /// Decoded `ser` dictionary values a later row still references, by
    /// dictionary index, with the bytes each charged to the request budget.
    decoded: HashMap<usize, (Value, usize)>,
    /// Rows left to read per `ser` dictionary index, counted on first use.
    refs: Option<Vec<u32>>,
    /// Bytes of `decoded`, charged to the request budget until they leave it.
    decoded_bytes: usize,
}

impl AnyValueColumns {
    /// Build from a column lookup (a `RecordBatch` or a `StructArray`).
    ///
    /// Keeps its own cast helper because it takes a column *lookup*, not a
    /// `RecordBatch`; [`plain`] stays the single batch-column helper.
    pub(crate) fn new(get: &dyn Fn(&str) -> Option<ArrayRef>) -> Result<Self> {
        let cast_opt = |name: &str, to: &DataType| -> Result<Option<ArrayRef>> {
            get(name).map(|c| readable(c, to)).transpose()
        };
        Ok(Self {
            types: cast_opt(ATTRIBUTE_TYPE, &DataType::UInt8)?
                .ok_or_else(|| Error::invalid("missing type column"))?
                .as_primitive_opt::<UInt8Type>()
                .cloned()
                .ok_or_else(|| Error::invalid("type column is not u8"))?,
            strs: cast_opt(ATTRIBUTE_STR, &DataType::Utf8)?,
            ints: cast_opt(ATTRIBUTE_INT, &DataType::Int64)?,
            doubles: cast_opt(ATTRIBUTE_DOUBLE, &DataType::Float64)?,
            bools: cast_opt(ATTRIBUTE_BOOL, &DataType::Boolean)?,
            bytes: cast_opt(ATTRIBUTE_BYTES, &DataType::Binary)?,
            sers: cast_opt(ATTRIBUTE_SER, &DataType::Binary)?,
            decoded: HashMap::new(),
            refs: None,
            decoded_bytes: 0,
        })
    }

    /// Give back the budget charged for the decoded dictionary values.
    pub(crate) fn release(self, budget: &mut Budget) {
        budget.uncharge(self.decoded_bytes);
    }

    /// Typed value at a row.
    ///
    /// The type tag decides the variant; the value column only supplies the
    /// payload. An absent column, or a null cell within it, therefore means
    /// "this type's default value", not "null": pdata's OTAP encoder omits a
    /// value column whose every entry is the default, so a batch of nothing but
    /// empty strings carries no `str` column at all. Reading that back as
    /// [`Value::Null`] would make a series identity depend on how requests
    /// happen to be batched, and would collide with a genuinely null attribute,
    /// which the canonical encoding keeps distinct (`Empty` below, and the
    /// `null_value` golden vector).
    ///
    /// Map and slice values are the exception: they arrive CBOR-encoded, and
    /// even an empty map is a non-empty CBOR payload, so an absent or null
    /// `ser` cell under those tags is malformed content rather than a default.
    ///
    /// A string, bytes or CBOR cell longer than `limits.max_cell_bytes` is
    /// refused as too large before it is copied or decoded. A dictionary-encoded
    /// `ser` value is decoded once and kept, charged to `budget`, until the last
    /// row that references it takes it; the rows before get clones.
    ///
    /// # Errors
    /// Returns [`Error::Refused`] for an unknown type tag, for a map or slice
    /// whose `ser` payload is missing, for a malformed CBOR payload, for an
    /// oversized cell and for a kept copy past the budget.
    pub(crate) fn value_at(
        &mut self,
        row: usize,
        limits: DecodeLimits,
        budget: &mut Budget,
    ) -> Result<Value> {
        let Some(ty) = self.types.value_at(row) else {
            return Ok(Value::Null);
        };
        let ty = AttributeValueType::try_from(ty)
            .map_err(|_| Error::invalid(format!("attribute type {ty}")))?;
        Ok(match ty {
            AttributeValueType::Empty => Value::Null,
            AttributeValueType::Str => {
                let cell = self.strs.as_ref().and_then(|a| {
                    str_cell(a, row, |s| {
                        cell_fits(s.len(), limits).map(|()| s.to_owned())
                    })
                });
                Value::Str(cell.transpose()?.unwrap_or_default())
            }
            AttributeValueType::Int => {
                Value::Int(prim_at::<Int64Type>(&self.ints, row).unwrap_or(0))
            }
            AttributeValueType::Double => {
                Value::Double(prim_at::<Float64Type>(&self.doubles, row).unwrap_or(0.0))
            }
            AttributeValueType::Bool => Value::Bool(bool_at(&self.bools, row).unwrap_or(false)),
            AttributeValueType::Bytes => {
                let cell = self.bytes.as_ref().and_then(|a| {
                    bytes_cell(a, row, |b| cell_fits(b.len(), limits).map(|()| b.to_vec()))
                });
                Value::Bytes(cell.transpose()?.unwrap_or_default())
            }
            AttributeValueType::Map | AttributeValueType::Slice => self
                .ser_at(row, limits, budget)?
                .ok_or_else(|| Error::invalid("map or slice attribute without a ser payload"))?,
        })
    }

    /// The decoded `ser` cell at `row`, `None` when it is absent or null.
    fn ser_at(
        &mut self,
        row: usize,
        limits: DecodeLimits,
        budget: &mut Budget,
    ) -> Result<Option<Value>> {
        let Some(sers) = &self.sers else {
            return Ok(None);
        };
        let Some((values, key)) = dictionary_at(sers, row) else {
            return bytes_cell(sers, row, |b| decode_cbor(b, limits)).transpose();
        };
        let Some(key) = key else {
            return Ok(None);
        };
        let refs = self
            .refs
            .get_or_insert_with(|| reference_counts(sers, values.len()));
        let left = refs
            .get_mut(key)
            .ok_or_else(|| Error::invalid("ser dictionary key out of range"))?;
        *left = left.saturating_sub(1);
        let last = *left == 0;
        if last {
            if let Some((value, bytes)) = self.decoded.remove(&key) {
                budget.uncharge(bytes);
                self.decoded_bytes -= bytes;
                return Ok(Some(value));
            }
        } else if let Some((value, _)) = self.decoded.get(&key) {
            return Ok(Some(value.clone()));
        }
        let Some(value) = bytes_cell(values, key, |b| decode_cbor(b, limits)).transpose()? else {
            return Ok(None);
        };
        if !last {
            let bytes = value_bytes(&value);
            budget.charge_decoded(bytes)?;
            self.decoded_bytes += bytes;
            let _ = self.decoded.insert(key, (value.clone(), bytes));
        }
        Ok(Some(value))
    }
}

fn required(batch: &RecordBatch, name: &str, to: &DataType) -> Result<ArrayRef> {
    plain(batch, name, to)?.ok_or_else(|| Error::invalid(format!("attribute batch lacks {name}")))
}

impl AttrTable {
    /// Build the table from an `attributes_16` or `attributes_32` batch.
    ///
    /// Keys and values are read through dictionary encoding rather than
    /// expanded, and each entry's decoded footprint ([`entry_bytes`]) is
    /// charged to `budget` as it is read, so the table draws from the same
    /// `max_extracted_bytes` as the rest of the request.
    ///
    /// # Errors
    /// Refuses the batch when `parent_id` is missing or null, a required
    /// column is absent, an attribute type or CBOR payload is malformed, a
    /// parent has a duplicate attribute key, a cell is longer than
    /// `limits.max_cell_bytes`, or the budget is exhausted.
    pub(crate) fn from_batch(
        batch: &RecordBatch,
        limits: DecodeLimits,
        budget: &mut Budget,
    ) -> Result<Self> {
        let parent_ids = read_parent_ids(batch)?;
        let keys = required(batch, ATTRIBUTE_KEY, &DataType::Utf8)?;
        let mut any = AnyValueColumns::new(&|n| batch.column_by_name(n).cloned())?;

        let mut groups: HashMap<u32, Vec<(String, Value)>> = HashMap::new();
        let mut bytes = 0_usize;
        for (row, &parent_id) in parent_ids.iter().enumerate() {
            let key = str_cell(&keys, row, |k| {
                cell_fits(k.len(), limits).map(|()| k.to_owned())
            })
            .transpose()?
            .unwrap_or_default();
            let value = any.value_at(row, limits, budget)?;
            let entry = entry_bytes(&key, &value);
            budget.charge_decoded(entry)?;
            bytes += entry;
            groups.entry(parent_id).or_default().push((key, value));
        }
        any.release(budget);
        for list in groups.values_mut() {
            sort_kvlist(list)?;
        }
        Ok(Self { groups, bytes })
    }

    /// Attributes of one parent, sorted by key, or an empty slice.
    #[must_use]
    pub fn get(&self, parent_id: u32) -> &[(String, Value)] {
        self.groups.get(&parent_id).map_or(&[], Vec::as_slice)
    }

    /// Approximate retained bytes of one parent's attributes.
    #[must_use]
    pub fn approx_bytes(&self, parent_id: u32) -> usize {
        self.get(parent_id)
            .iter()
            .map(|(k, v)| k.len() + 24 + value_bytes(v))
            .sum()
    }

    /// Drop the table and give back what it charged to `budget`.
    pub(crate) fn release(self, budget: &mut Budget) {
        budget.uncharge(self.bytes);
    }
}

/// Read `parent_id` as `u32`, refusing a missing column or a null id.
///
/// The column is read through its dictionary, when it has one, rather than
/// cast. A null `parent_id` is malformed input, not parent 0: refuse it
/// rather than silently attributing the row to the first parent.
fn read_parent_ids(batch: &RecordBatch) -> Result<Vec<u32>> {
    let col = batch
        .column_by_name(PARENT_ID)
        .ok_or_else(|| Error::invalid("attribute batch lacks parent_id"))?;
    let value_type = match col.data_type() {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    };
    let null_parent = || Error::invalid("attribute batch has a null parent_id");
    let unreadable = |e: otel_arrow_dfe_pdata::error::Error| {
        Error::invalid(format!("attribute batch parent_id: {e}"))
    };
    match value_type {
        DataType::UInt16 => {
            let ids = MaybeDictArrayAccessor::<UInt16Array>::try_new(col).map_err(unreadable)?;
            (0..ids.len())
                .map(|row| ids.value_at(row).map(u32::from).ok_or_else(null_parent))
                .collect()
        }
        DataType::UInt32 => {
            let ids = MaybeDictArrayAccessor::<UInt32Array>::try_new(col).map_err(unreadable)?;
            (0..ids.len())
                .map(|row| ids.value_at(row).ok_or_else(null_parent))
                .collect()
        }
        other => Err(Error::invalid(format!("parent_id type {other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RefuseReason;
    use arrow::array::{
        ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt16Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Depth 32, no cell-size bound: the size limit has its own tests.
    fn limits() -> DecodeLimits {
        DecodeLimits::new(32, usize::MAX)
    }

    /// A request budget of `limit` bytes.
    fn budget(limit: usize) -> Budget {
        let mut cfg = crate::config::LakeConfig::default();
        cfg.ingress.max_extracted_bytes = limit;
        Budget::new(&cfg)
    }

    /// A CBOR array of `n` zeros: one encoded byte and one decoded node each.
    fn cbor_zeros(n: usize) -> Vec<u8> {
        let mut ser = Vec::new();
        ciborium::into_writer(
            &ciborium::Value::Array(vec![ciborium::Value::Integer(0.into()); n]),
            &mut ser,
        )
        .expect("cbor");
        ser
    }

    /// A one-MiB CBOR cell of small ints, about 32 MiB once decoded.
    fn one_mib_of_ints() -> Vec<u8> {
        let ser = cbor_zeros((1 << 20) - 5);
        assert_eq!(ser.len(), 1 << 20);
        ser
    }

    /// Slice attributes `k`, one parent per row, whose `ser` column is
    /// `Dictionary<U16, Binary>` over `values` and row `i` references key
    /// `keys[i]`: the OTAP schema's encoding of the column.
    fn ser_dictionary_batch(values: &[&[u8]], keys: &[u16]) -> RecordBatch {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::UInt16Type;
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new(
                "ser",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Binary)),
                true,
            ),
        ]);
        let n = u16::try_from(keys.len()).expect("rows fit u16");
        let ser = DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from(keys.to_vec()),
            Arc::new(BinaryArray::from(values.to_vec())),
        )
        .expect("dictionary");
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from_iter_values(0..n)),
            Arc::new(StringArray::from(vec!["k"; keys.len()])),
            Arc::new(UInt8Array::from(vec![6u8; keys.len()])),
            Arc::new(ser),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    fn refused_by_table(result: &Result<AttrTable>) -> bool {
        use crate::error::{Excess, SizeBudget};
        matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(Excess {
                budget: SizeBudget::Table,
                ..
            })))
        )
    }

    fn batch() -> RecordBatch {
        let mut ser = Vec::new();
        ciborium::into_writer(
            &ciborium::Value::Array(vec![ciborium::Value::Integer(7.into())]),
            &mut ser,
        )
        .expect("cbor");
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
            Field::new("int", DataType::Int64, true),
            Field::new("double", DataType::Float64, true),
            Field::new("ser", DataType::Binary, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16, 0, 1, 1])),
            Arc::new(StringArray::from(vec!["z", "a", "n", "arr"])),
            Arc::new(UInt8Array::from(vec![1u8, 2, 3, 6])),
            Arc::new(StringArray::from(vec![Some("v"), None, None, None])),
            Arc::new(Int64Array::from(vec![None, Some(5), None, None])),
            Arc::new(Float64Array::from(vec![None, None, Some(1.5), None])),
            Arc::new(BinaryArray::from(vec![
                None,
                None,
                None,
                Some(ser.as_slice()),
            ])),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// An attribute batch of `rows` string attributes whose parent id, key and
    /// value are all dictionary encoded, every row referencing the single
    /// value `value`, one parent per row.
    fn dictionary_batch(rows: usize, value: &str) -> RecordBatch {
        use arrow::array::{DictionaryArray, UInt8Array as Keys8};
        use arrow::datatypes::{UInt8Type, UInt16Type};
        let key_type = Box::new(DataType::UInt8);
        let schema = Schema::new(vec![
            Field::new(
                "parent_id",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::UInt16)),
                false,
            ),
            Field::new(
                "key",
                DataType::Dictionary(key_type.clone(), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("type", DataType::UInt8, false),
            Field::new(
                "str",
                DataType::Dictionary(key_type, Box::new(DataType::Utf8)),
                true,
            ),
        ]);
        let n = u16::try_from(rows).expect("rows fit u16");
        let parents = DictionaryArray::<UInt16Type>::try_new(
            UInt16Array::from_iter_values(0..n),
            Arc::new(UInt16Array::from_iter_values(0..n)),
        )
        .expect("parents");
        let one = |s: &str| {
            DictionaryArray::<UInt8Type>::try_new(
                Keys8::from(vec![0u8; rows]),
                Arc::new(StringArray::from(vec![s])),
            )
            .expect("dictionary")
        };
        let cols: Vec<ArrayRef> = vec![
            Arc::new(parents),
            Arc::new(one("k")),
            Arc::new(UInt8Array::from(vec![1u8; rows])),
            Arc::new(one(value)),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: an attribute batch whose one two-MiB string value is
    /// dictionary encoded and referenced by 256 rows -- half a GiB once
    /// expanded -- is decoded under a one-MiB cell limit.
    /// Guarantees: the batch is refused as too large on the first cell rather
    /// than after copying the value once per row, so a request whose logical
    /// size counts the value once cannot expand into memory before any budget
    /// applies.
    #[test]
    fn a_large_dictionary_value_is_refused_before_it_is_expanded() {
        let big = "x".repeat(2 << 20);
        let batch = dictionary_batch(256, &big);
        let limits = DecodeLimits::new(32, 1 << 20);
        assert!(matches!(
            AttrTable::from_batch(&batch, limits, &mut budget(32 << 20)),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: a dictionary-encoded string value of half a MiB, under the
    /// one-MiB cell limit, is referenced by 256 rows, so the decoded table
    /// would hold 128 MiB against a 32 MiB table budget.
    /// Guarantees: the table is refused as too large once its decoded content
    /// passes the budget, so many references to a value that fits one row are
    /// bounded as well.
    #[test]
    fn many_references_to_one_dictionary_value_are_bounded() {
        let value = "y".repeat(512 << 10);
        let batch = dictionary_batch(256, &value);
        let limits = DecodeLimits::new(32, 1 << 20);
        assert!(matches!(
            AttrTable::from_batch(&batch, limits, &mut budget(32 << 20)),
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
    }

    /// Scenario: 64 rows reference one dictionary-encoded `ser` value, a
    /// one-MiB CBOR array of small ints, under the default 1 MiB cell limit and
    /// 32 MiB request budget.
    /// Guarantees: the value is decoded once and the table is refused against
    /// the budget on its first copy, not after 64 decodes of 32 MiB each.
    #[test]
    fn a_dictionary_ser_value_is_decoded_once_and_its_copies_are_charged() {
        let value = one_mib_of_ints();
        let batch = ser_dictionary_batch(&[&value], &[0; 64]);
        let before = crate::value::decodes();
        let result = AttrTable::from_batch(
            &batch,
            DecodeLimits::new(32, 1 << 20),
            &mut budget(32 << 20),
        );
        assert!(refused_by_table(&result), "{result:?}");
        assert_eq!(crate::value::decodes() - before, 1);
    }

    /// Scenario: rows alternate between two dictionary-encoded `ser` arrays
    /// under a budget that cannot bind.
    /// Guarantees: each distinct dictionary key is decoded exactly once and
    /// every row still reads back its own value.
    #[test]
    fn each_distinct_dictionary_ser_value_is_decoded_once() {
        let (a, b) = (cbor_zeros(1000), cbor_zeros(2000));
        let batch = ser_dictionary_batch(&[&a, &b], &[0, 1, 0, 1, 0, 1]);
        let before = crate::value::decodes();
        let t = AttrTable::from_batch(&batch, limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(crate::value::decodes() - before, 2);
        for parent in 0..6 {
            let len = if parent % 2 == 0 { 1000 } else { 2000 };
            assert!(matches!(&t.get(parent)[0].1, Value::Array(items) if items.len() == len));
        }
    }

    /// Scenario: one plain (not dictionary-encoded) `ser` cell holds a one-MiB
    /// CBOR array of small ints, decoded against a 16 MiB and a 64 MiB budget.
    /// Guarantees: the table is charged the decoded footprint, about 32 MiB,
    /// not the encoded length or zero, so it is refused under 16 MiB and
    /// accepted under 64 MiB.
    #[test]
    fn a_plain_ser_cell_is_charged_its_decoded_footprint() {
        let value = one_mib_of_ints();
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("ser", DataType::Binary, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16])),
            Arc::new(StringArray::from(vec!["k"])),
            Arc::new(UInt8Array::from(vec![6u8])),
            Arc::new(BinaryArray::from(vec![value.as_slice()])),
        ];
        let batch = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        let limits = DecodeLimits::new(32, 1 << 20);
        let refused = AttrTable::from_batch(&batch, limits, &mut budget(16 << 20));
        assert!(refused_by_table(&refused), "{refused:?}");
        let t = AttrTable::from_batch(&batch, limits, &mut budget(64 << 20)).expect("fits");
        assert!(t.bytes > crate::value::VALUE_NODE_BYTES * ((1 << 20) - 5));
    }

    /// Scenario: a small dictionary-encoded batch -- parent ids, keys and
    /// values all dictionary encoded -- is decoded, and one of its columns is
    /// prepared for reading.
    /// Guarantees: every row reads back its parent, key and value through the
    /// dictionary, and the prepared column keeps its dictionary encoding
    /// rather than being cast to plain strings.
    #[test]
    fn a_dictionary_batch_reads_through_without_expansion() {
        let batch = dictionary_batch(3, "shared");
        let t = AttrTable::from_batch(&batch, limits(), &mut budget(usize::MAX)).expect("table");
        for parent in 0..3 {
            assert_eq!(
                t.get(parent),
                &[("k".to_string(), Value::Str("shared".into()))]
            );
        }
        let col = plain(&batch, "str", &DataType::Utf8)
            .expect("readable")
            .expect("present");
        assert!(matches!(col.data_type(), DataType::Dictionary(_, _)));
        assert_eq!(str_cell(&col, 2, str::len), Some(6));
    }

    /// Scenario: two parents with mixed value types, keys unsorted in the batch.
    /// Guarantees: each parent gets a sorted, typed attribute list; absent parents are empty.
    #[test]
    fn groups_and_sorts_by_parent() {
        let t = AttrTable::from_batch(&batch(), limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(
            t.get(0),
            &[
                ("a".to_string(), Value::Int(5)),
                ("z".to_string(), Value::Str("v".into()))
            ]
        );
        assert_eq!(
            t.get(1),
            &[
                ("arr".to_string(), Value::Array(vec![Value::Int(7)])),
                ("n".to_string(), Value::Double(1.5))
            ]
        );
        assert!(t.get(7).is_empty());
    }

    /// Scenario: the same key appears twice for one parent.
    /// Guarantees: the whole batch is refused as invalid content.
    #[test]
    fn duplicate_key_is_refused() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![3u16, 3])),
            Arc::new(StringArray::from(vec!["k", "k"])),
            Arc::new(UInt8Array::from(vec![1u8, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        assert!(matches!(
            AttrTable::from_batch(&b, limits(), &mut budget(usize::MAX)),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Scenario: an attribute row carries a null `parent_id`.
    /// Guarantees: the batch is refused instead of silently attributing the row to parent 0.
    #[test]
    fn null_parent_id_is_refused() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, true),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![Some(0u16), None])),
            Arc::new(StringArray::from(vec!["k", "j"])),
            Arc::new(UInt8Array::from(vec![1u8, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        assert!(matches!(
            AttrTable::from_batch(&b, limits(), &mut budget(usize::MAX)),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Build a batch of one attribute per row with the given type tags and NO
    /// value columns at all, the shape pdata emits when every value in a batch
    /// is that type's default.
    fn batch_without_value_columns(tags: &[u8]) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
        ]);
        let keys: Vec<String> = (0..tags.len()).map(|i| format!("k{i}")).collect();
        let parents: Vec<u16> = vec![0; tags.len()];
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(parents)),
            Arc::new(StringArray::from(keys)),
            Arc::new(UInt8Array::from(tags.to_vec())),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: a batch whose value columns are all absent, one row per typed
    /// tag, as pdata emits when every value of a column is that type's default.
    /// Guarantees: each attribute decodes to its type's default value, never to
    /// [`Value::Null`]. Reading them as null would make a series identity depend
    /// on how requests are batched and would collide with a genuinely null
    /// attribute, which stays [`Value::Null`] under the `Empty` tag.
    #[test]
    fn absent_value_column_decodes_as_the_type_default() {
        // Tags: str, int, double, bool, bytes, empty.
        let b = batch_without_value_columns(&[1, 2, 3, 4, 7, 0]);
        let t = AttrTable::from_batch(&b, limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(
            t.get(0),
            &[
                ("k0".to_string(), Value::Str(String::new())),
                ("k1".to_string(), Value::Int(0)),
                ("k2".to_string(), Value::Double(0.0)),
                ("k3".to_string(), Value::Bool(false)),
                ("k4".to_string(), Value::Bytes(Vec::new())),
                ("k5".to_string(), Value::Null),
            ]
        );
    }

    /// Scenario: a typed value column that is present but null in this row.
    /// Guarantees: a null cell is the type's default too, for the same reason an
    /// absent column is -- the type tag, not the cell, decides the variant.
    #[test]
    fn null_value_cell_decodes_as_the_type_default() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
            Field::new("int", DataType::Int64, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16, 0])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(UInt8Array::from(vec![1u8, 2])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(Int64Array::from(vec![None::<i64>, None])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        let t = AttrTable::from_batch(&b, limits(), &mut budget(usize::MAX)).expect("table");
        assert_eq!(
            t.get(0),
            &[
                ("a".to_string(), Value::Str(String::new())),
                ("b".to_string(), Value::Int(0)),
            ]
        );
    }

    /// Scenario: a map or slice attribute whose `ser` payload is absent.
    /// Guarantees: the batch is refused. Unlike the scalar types, a map has no
    /// empty encoding to fall back on -- even an empty CBOR map is a non-empty
    /// payload -- so a missing one is malformed content, not a default.
    #[test]
    fn map_or_slice_without_a_ser_payload_is_refused() {
        for tag in [5u8, 6] {
            let b = batch_without_value_columns(&[tag]);
            assert!(matches!(
                AttrTable::from_batch(&b, limits(), &mut budget(usize::MAX)),
                Err(Error::Refused(RefuseReason::Invalid(_)))
            ));
        }
    }
}
