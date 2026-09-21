// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reads OTAP attribute record batches into per-parent, sorted attribute lists.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt8Type, UInt16Type, UInt32Type};
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::value::{Value, decode_cbor, sort_kvlist, value_bytes};

const TYPE_EMPTY: u8 = 0;
const TYPE_STR: u8 = 1;
const TYPE_INT: u8 = 2;
const TYPE_DOUBLE: u8 = 3;
const TYPE_BOOL: u8 = 4;
const TYPE_MAP: u8 = 5;
const TYPE_SLICE: u8 = 6;
const TYPE_BYTES: u8 = 7;

/// Attributes of one OTAP attribute batch, grouped by parent id.
#[derive(Debug, Default)]
pub struct AttrTable {
    groups: HashMap<u32, Vec<(String, Value)>>,
}

/// Fetch `name` from `batch` cast to `to`, or `None` when the column is absent.
///
/// This is the single "cast this column to a plain type" helper of the crate;
/// `extract` reuses it instead of defining its own.
pub(crate) fn plain(batch: &RecordBatch, name: &str, to: &DataType) -> Result<Option<ArrayRef>> {
    let Some(col) = batch.column_by_name(name) else {
        return Ok(None);
    };
    if col.data_type() == to {
        return Ok(Some(Arc::clone(col)));
    }
    Ok(Some(cast(col, to)?))
}

/// The seven `AnyValue` columns (type, str, int, double, bool, bytes, ser) of an
/// attribute batch or a log body struct, cast to plain types.
pub(crate) struct AnyValueColumns {
    types: ArrayRef,
    strs: Option<ArrayRef>,
    ints: Option<ArrayRef>,
    doubles: Option<ArrayRef>,
    bools: Option<ArrayRef>,
    bytes: Option<ArrayRef>,
    sers: Option<ArrayRef>,
}

impl AnyValueColumns {
    /// Build from a column lookup (a `RecordBatch` or a `StructArray`).
    ///
    /// Keeps its own cast helper because it takes a column *lookup*, not a
    /// `RecordBatch`; [`plain`] stays the single batch-column helper.
    pub(crate) fn new(get: &dyn Fn(&str) -> Option<ArrayRef>) -> Result<Self> {
        let cast_opt = |name: &str, to: &DataType| -> Result<Option<ArrayRef>> {
            match get(name) {
                None => Ok(None),
                Some(c) if c.data_type() == to => Ok(Some(c)),
                Some(c) => Ok(Some(cast(&c, to)?)),
            }
        };
        Ok(Self {
            types: cast_opt("type", &DataType::UInt8)?
                .ok_or_else(|| Error::invalid("missing type column"))?,
            strs: cast_opt("str", &DataType::Utf8)?,
            ints: cast_opt("int", &DataType::Int64)?,
            doubles: cast_opt("double", &DataType::Float64)?,
            bools: cast_opt("bool", &DataType::Boolean)?,
            bytes: cast_opt("bytes", &DataType::Binary)?,
            sers: cast_opt("ser", &DataType::Binary)?,
        })
    }

    /// Typed value at a row; nulls in the value column become [`Value::Null`].
    pub(crate) fn value_at(&self, row: usize, max_depth: usize) -> Result<Value> {
        if !self.types.is_valid(row) {
            return Ok(Value::Null);
        }
        let ty = self.types.as_primitive::<UInt8Type>().value(row);
        Ok(match ty {
            TYPE_EMPTY => Value::Null,
            TYPE_STR => match &self.strs {
                Some(a) if a.is_valid(row) => {
                    Value::Str(a.as_string::<i32>().value(row).to_string())
                }
                _ => Value::Null,
            },
            TYPE_INT => match &self.ints {
                Some(a) if a.is_valid(row) => Value::Int(a.as_primitive::<Int64Type>().value(row)),
                _ => Value::Null,
            },
            TYPE_DOUBLE => match &self.doubles {
                Some(a) if a.is_valid(row) => {
                    Value::Double(a.as_primitive::<Float64Type>().value(row))
                }
                _ => Value::Null,
            },
            TYPE_BOOL => match &self.bools {
                Some(a) if a.is_valid(row) => Value::Bool(a.as_boolean().value(row)),
                _ => Value::Null,
            },
            TYPE_BYTES => match &self.bytes {
                Some(a) if a.is_valid(row) => {
                    Value::Bytes(a.as_binary::<i32>().value(row).to_vec())
                }
                _ => Value::Null,
            },
            TYPE_MAP | TYPE_SLICE => match &self.sers {
                Some(a) if a.is_valid(row) => {
                    decode_cbor(a.as_binary::<i32>().value(row), max_depth)?
                }
                _ => Value::Null,
            },
            other => return Err(Error::invalid(format!("attribute type {other}"))),
        })
    }
}

fn required(batch: &RecordBatch, name: &str, to: &DataType) -> Result<ArrayRef> {
    plain(batch, name, to)?.ok_or_else(|| Error::invalid(format!("attribute batch lacks {name}")))
}

impl AttrTable {
    /// Build the table from an `attributes_16` or `attributes_32` batch.
    ///
    /// # Errors
    /// Refuses the batch when `parent_id` is missing or null, a required
    /// column is absent, an attribute type or CBOR payload is malformed, or
    /// a parent has a duplicate attribute key.
    pub fn from_batch(batch: &RecordBatch, max_depth: usize) -> Result<Self> {
        let parent_ids = read_parent_ids(batch)?;
        let keys = required(batch, "key", &DataType::Utf8)?;
        let keys = keys.as_string::<i32>();
        let any = AnyValueColumns::new(&|n| batch.column_by_name(n).cloned())?;

        let mut groups: HashMap<u32, Vec<(String, Value)>> = HashMap::new();
        for (row, &parent_id) in parent_ids.iter().enumerate() {
            let key = keys.value(row).to_string();
            let value = any.value_at(row, max_depth)?;
            groups.entry(parent_id).or_default().push((key, value));
        }
        for list in groups.values_mut() {
            sort_kvlist(list)?;
        }
        Ok(Self { groups })
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
}

/// Read `parent_id` as `u32`, refusing a missing column or a null id.
///
/// A null `parent_id` is malformed input, not parent 0: refuse it rather
/// than silently attributing the row to the first parent (spec 9.3).
fn read_parent_ids(batch: &RecordBatch) -> Result<Vec<u32>> {
    let col = batch
        .column_by_name("parent_id")
        .ok_or_else(|| Error::invalid("attribute batch lacks parent_id"))?;
    let ids = match col.data_type() {
        DataType::Dictionary(_, _) => cast(col, &DataType::UInt32)?,
        _ => Arc::clone(col),
    };
    let null_parent = || Error::invalid("attribute batch has a null parent_id");
    match ids.data_type() {
        DataType::UInt16 => ids
            .as_primitive::<UInt16Type>()
            .iter()
            .map(|v| v.map(u32::from).ok_or_else(null_parent))
            .collect(),
        DataType::UInt32 => ids
            .as_primitive::<UInt32Type>()
            .iter()
            .map(|v| v.ok_or_else(null_parent))
            .collect(),
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

    /// Scenario: two parents with mixed value types, keys unsorted in the batch.
    /// Guarantees: each parent gets a sorted, typed attribute list; absent parents are empty.
    #[test]
    fn groups_and_sorts_by_parent() {
        let t = AttrTable::from_batch(&batch(), 32).expect("table");
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
            AttrTable::from_batch(&b, 32),
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
            AttrTable::from_batch(&b, 32),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }
}
