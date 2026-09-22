// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owned attribute value tree, CBOR decoding of the OTAP `ser` column and the
//! `render_v1` storage rendering (spec section 5.1).

use crate::error::{Error, RefuseReason, Result};

/// Limits applied while decoding one CBOR `ser` cell (spec section 5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum nesting depth of the decoded value.
    pub max_depth: usize,
    /// Maximum byte length of one string, bytes or encoded `ser` cell.
    pub max_cell_bytes: usize,
    /// Maximum string and byte content one attribute table may decode.
    pub max_table_bytes: usize,
}

impl DecodeLimits {
    /// Limits from a depth and a cell byte bound, with no table bound.
    #[must_use]
    pub fn new(max_depth: usize, max_cell_bytes: usize) -> Self {
        Self {
            max_depth,
            max_cell_bytes,
            max_table_bytes: usize::MAX,
        }
    }

    /// The same limits with a bound on what one attribute table may decode.
    #[must_use]
    pub fn with_table_bytes(self, max_table_bytes: usize) -> Self {
        Self {
            max_table_bytes,
            ..self
        }
    }
}

/// An OTLP AnyValue in owned form.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Unset value.
    Null,
    /// UTF-8 string.
    Str(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// 64-bit integer.
    Int(i64),
    /// 64-bit float.
    Double(f64),
    /// Boolean.
    Bool(bool),
    /// Array of values.
    Array(Vec<Value>),
    /// Key/value list, sorted by raw key bytes, keys unique.
    KvList(Vec<(String, Value)>),
}

/// Decode a CBOR blob from the OTAP `ser` column into a [`Value`].
///
/// Kvlist keys are sorted by raw bytes; duplicate keys and nesting deeper
/// than `limits.max_depth` are refused as invalid content.
///
/// Both limits are applied before the work they bound. An encoded cell longer
/// than `limits.max_cell_bytes` is refused without being decoded at all: its
/// decoded tree is a multiple of the encoding, and a cell that cannot fit a row
/// must never be expanded into one first. The depth is handed to ciborium's own
/// parser, whose default cap is a fixed 256 and does not know this crate's
/// configuration, so a deeply nested payload is refused during parsing rather
/// than after its whole tree has been allocated.
///
/// There is no separate node budget: the decoded tree is bounded by the cell
/// cap, because every CBOR item costs at least one encoded byte and expands to
/// at most a small constant of decoded bytes, so a tree from a cell of at most
/// `max_cell_bytes` is at most that many bytes times that constant.
///
/// # Errors
/// Refuses an oversized cell as `RequestTooLarge`, and a malformed payload,
/// a duplicate key or excessive nesting as invalid content.
pub fn decode_cbor(bytes: &[u8], limits: DecodeLimits) -> Result<Value> {
    if bytes.len() > limits.max_cell_bytes {
        return Err(Error::Refused(RefuseReason::RequestTooLarge));
    }
    // One recursion level per container, plus one so that a payload exactly at
    // `max_depth` is settled by the conversion below rather than by the parser:
    // `convert` is the definition of this crate's depth rule.
    let recursion = limits.max_depth.saturating_add(1);
    let raw: ciborium::Value = ciborium::de::from_reader_with_recursion_limit(bytes, recursion)
        .map_err(|e| Error::invalid(format!("cbor decode: {e}")))?;
    convert(raw, limits.max_depth)
}

fn convert(raw: ciborium::Value, depth_left: usize) -> Result<Value> {
    Ok(match raw {
        ciborium::Value::Null => Value::Null,
        ciborium::Value::Bool(b) => Value::Bool(b),
        ciborium::Value::Integer(i) => {
            let i: i64 = i
                .try_into()
                .map_err(|_| Error::invalid("cbor int out of i64"))?;
            Value::Int(i)
        }
        ciborium::Value::Float(f) => Value::Double(f),
        ciborium::Value::Text(s) => Value::Str(s),
        ciborium::Value::Bytes(b) => Value::Bytes(b),
        ciborium::Value::Array(items) => {
            if depth_left == 0 {
                return Err(Error::invalid("cbor nesting too deep"));
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(convert(item, depth_left - 1)?);
            }
            Value::Array(out)
        }
        ciborium::Value::Map(entries) => {
            if depth_left == 0 {
                return Err(Error::invalid("cbor nesting too deep"));
            }
            let mut out: Vec<(String, Value)> = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                let key = match k {
                    ciborium::Value::Text(s) => s,
                    ciborium::Value::Null => String::new(),
                    _ => return Err(Error::invalid("cbor map key is not text")),
                };
                out.push((key, convert(v, depth_left - 1)?));
            }
            sort_kvlist(&mut out)?;
            Value::KvList(out)
        }
        _ => return Err(Error::invalid("unsupported cbor value")),
    })
}

/// Sort a key/value list by raw key bytes and refuse duplicate keys.
pub fn sort_kvlist(list: &mut [(String, Value)]) -> Result<()> {
    list.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if list.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(Error::invalid("duplicate attribute key"));
    }
    Ok(())
}

/// The `render_v1` recursive rendering of spec section 5.1.
pub fn render_v1(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Str(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(hex::encode(b)),
        Value::Int(i) => J::from(*i),
        Value::Double(d) => render_double(*d),
        Value::Bool(b) => J::Bool(*b),
        Value::Array(items) => J::Array(items.iter().map(render_v1).collect()),
        Value::KvList(entries) => J::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), render_v1(v)))
                .collect(),
        ),
    }
}

fn render_double(d: f64) -> serde_json::Value {
    if d.is_nan() {
        serde_json::Value::String("NaN".into())
    } else if d == f64::INFINITY {
        serde_json::Value::String("inf".into())
    } else if d == f64::NEG_INFINITY {
        serde_json::Value::String("-inf".into())
    } else {
        // serde_json renders finite f64 with the shortest round-trip form.
        serde_json::Number::from_f64(d).map_or(serde_json::Value::Null, serde_json::Value::Number)
    }
}

/// Attribute-map entry point: raw string for strings, `None` for unset,
/// compact JSON of `render_v1` otherwise.
#[must_use]
pub fn map_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(s.clone()),
        other => Some(render_v1(other).to_string()),
    }
}

/// Log-body entry point: identical rules to [`map_string`].
#[must_use]
pub fn body_string(v: &Value) -> Option<String> {
    map_string(v)
}

/// Approximate retained size of a value, used for row-size budgets.
pub fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Null | Value::Int(_) | Value::Double(_) | Value::Bool(_) => 8,
        Value::Str(s) => s.len() + 24,
        Value::Bytes(b) => b.len() + 24,
        Value::Array(items) => 24 + items.iter().map(value_bytes).sum::<usize>(),
        Value::KvList(entries) => {
            24 + entries
                .iter()
                .map(|(k, v)| k.len() + 24 + value_bytes(v))
                .sum::<usize>()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a CBOR map with unsorted keys and nested array is decoded.
    /// Guarantees: keys come out sorted by raw bytes and nesting is preserved.
    #[test]
    fn decode_cbor_sorts_keys() {
        let mut buf = Vec::new();
        let v = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("b".into()),
                ciborium::Value::Integer(2.into()),
            ),
            (
                ciborium::Value::Text("a".into()),
                ciborium::Value::Array(vec![ciborium::Value::Bool(true), ciborium::Value::Null]),
            ),
        ]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        let got = decode_cbor(&buf, DecodeLimits::new(32, usize::MAX)).expect("decode");
        assert_eq!(
            got,
            Value::KvList(vec![
                (
                    "a".into(),
                    Value::Array(vec![Value::Bool(true), Value::Null])
                ),
                ("b".into(), Value::Int(2)),
            ])
        );
    }

    /// Scenario: an encoded `ser` cell longer than `max_cell_bytes`, holding a
    /// payload that is otherwise perfectly valid CBOR.
    /// Guarantees: the cell is refused as too large before it is decoded, so a
    /// cell that could never fit a row is never expanded into a value tree.
    #[test]
    fn decode_cbor_refuses_an_oversized_cell_before_decoding() {
        use crate::error::{Error, RefuseReason};
        let mut buf = Vec::new();
        let v = ciborium::Value::Array(vec![ciborium::Value::Bytes(vec![0u8; 4096])]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(buf.len() > 4096);
        assert!(matches!(
            decode_cbor(&buf, DecodeLimits::new(32, 1024)),
            Err(Error::Refused(RefuseReason::RequestTooLarge))
        ));
        // The same payload decodes once the cell fits the limit.
        assert!(decode_cbor(&buf, DecodeLimits::new(32, buf.len())).is_ok());
    }

    /// Scenario: a CBOR map repeats a key.
    /// Guarantees: decoding refuses the input as invalid.
    #[test]
    fn decode_cbor_rejects_duplicate_keys() {
        use crate::error::{Error, RefuseReason};
        let mut buf = Vec::new();
        let v = ciborium::Value::Map(vec![
            (
                ciborium::Value::Text("k".into()),
                ciborium::Value::Integer(1.into()),
            ),
            (
                ciborium::Value::Text("k".into()),
                ciborium::Value::Integer(2.into()),
            ),
        ]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(matches!(
            decode_cbor(&buf, DecodeLimits::new(32, usize::MAX)),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
    }

    /// Scenario: arrays nested deeper than the limit.
    /// Guarantees: decoding refuses instead of recursing without bound.
    #[test]
    fn decode_cbor_rejects_deep_nesting() {
        let mut v = ciborium::Value::Null;
        for _ in 0..5 {
            v = ciborium::Value::Array(vec![v]);
        }
        let mut buf = Vec::new();
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(decode_cbor(&buf, DecodeLimits::new(3, usize::MAX)).is_err());
        assert!(decode_cbor(&buf, DecodeLimits::new(5, usize::MAX)).is_ok());
    }

    /// Scenario: maps nested one level below, exactly at and one level above
    /// the depth limit, and an array inside maps at the limit.
    /// Guarantees: the limit counts every container level alike -- a nested
    /// map is refused exactly where a nested array would be -- and a payload
    /// at the limit decodes, so `max_nesting_depth` is the deepest accepted
    /// nesting, not one less or one more.
    #[test]
    fn decode_cbor_depth_is_exact_for_nested_maps() {
        fn maps(levels: usize, leaf: ciborium::Value) -> Vec<u8> {
            let mut v = leaf;
            for _ in 0..levels {
                v = ciborium::Value::Map(vec![(ciborium::Value::Text("k".into()), v)]);
            }
            let mut buf = Vec::new();
            ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
            buf
        }
        let limit = 8;
        let limits = DecodeLimits::new(limit, usize::MAX);
        let int = || ciborium::Value::Integer(1.into());
        assert!(decode_cbor(&maps(limit - 1, int()), limits).is_ok());
        assert!(decode_cbor(&maps(limit, int()), limits).is_ok());
        assert!(matches!(
            decode_cbor(&maps(limit + 1, int()), limits),
            Err(Error::Refused(RefuseReason::Invalid(_)))
        ));
        let array = ciborium::Value::Array(vec![int()]);
        assert!(decode_cbor(&maps(limit - 1, array.clone()), limits).is_ok());
        assert!(decode_cbor(&maps(limit, array), limits).is_err());
    }

    /// Scenario: render_v1 over every scalar kind and a nested kvlist.
    /// Guarantees: the JSON mapping of spec section 5.1 is produced exactly.
    #[test]
    fn render_v1_mapping() {
        let v = Value::KvList(vec![
            ("b".into(), Value::Bytes(vec![0xab, 0x12])),
            ("d".into(), Value::Double(f64::NAN)),
            ("e".into(), Value::Double(1.5)),
            ("i".into(), Value::Int(42)),
            ("n".into(), Value::Null),
            ("s".into(), Value::Str("x".into())),
            ("t".into(), Value::Bool(true)),
        ]);
        let json = serde_json::to_string(&render_v1(&v)).expect("json");
        assert_eq!(
            json,
            r#"{"b":"ab12","d":"NaN","e":1.5,"i":42,"n":null,"s":"x","t":true}"#
        );
    }

    /// Scenario: map and body entry points on top-level scalars.
    /// Guarantees: strings are raw, null is None, other scalars are compact JSON.
    #[test]
    fn map_and_body_entry_points() {
        assert_eq!(
            map_string(&Value::Str("raw \"q\"".into())).as_deref(),
            Some("raw \"q\"")
        );
        assert_eq!(map_string(&Value::Null), None);
        assert_eq!(map_string(&Value::Int(42)).as_deref(), Some("42"));
        assert_eq!(
            map_string(&Value::Bytes(vec![0xab])).as_deref(),
            Some("\"ab\"")
        );
        assert_eq!(
            map_string(&Value::Double(f64::INFINITY)).as_deref(),
            Some("\"inf\"")
        );
        assert_eq!(
            body_string(&Value::Str("hello".into())).as_deref(),
            Some("hello")
        );
        assert_eq!(body_string(&Value::Null), None);
        assert_eq!(
            body_string(&Value::Array(vec![Value::Int(1)])).as_deref(),
            Some("[1]")
        );
    }
}
