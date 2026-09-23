// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owned attribute value tree, CBOR decoding of the OTAP `ser` column and the
//! `render_v1` storage rendering (FORMAT.md section 2).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;

use crate::error::{Error, RefuseReason, Result};

/// Limits applied while decoding one CBOR `ser` cell (FORMAT.md section 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum nesting depth of the decoded value.
    pub max_depth: usize,
    /// Maximum byte length of one string, bytes or encoded `ser` cell.
    pub max_cell_bytes: usize,
}

impl DecodeLimits {
    /// Limits from a depth and a cell byte bound.
    #[must_use]
    pub fn new(max_depth: usize, max_cell_bytes: usize) -> Self {
        Self {
            max_depth,
            max_cell_bytes,
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
/// Kvlist keys are sorted by raw bytes; duplicate keys are refused as invalid
/// content and nesting deeper than `limits.max_depth` as too deep. An encoded
/// cell longer than `limits.max_cell_bytes` is refused without being decoded.
/// Trailing bytes after the first item are ignored.
///
/// # Errors
/// Refuses an oversized cell as `RequestTooLarge`, nesting deeper than
/// `limits.max_depth` as `TooDeep`, and a malformed payload, a tag other than
/// a bignum, an integer outside `i64` or a duplicate key as invalid content.
pub fn decode_cbor(bytes: &[u8], limits: DecodeLimits) -> Result<Value> {
    decode_cbor_reserving(bytes, limits, &mut |_| Ok(()))
}

/// [`decode_cbor`], calling `reserve` with the bytes of every node below the
/// root and of every string, byte and key content before allocating them.
///
/// The calls add up to the result's [`value_bytes`] less one
/// [`VALUE_NODE_BYTES`], the root node, which lives in its caller's storage.
/// An error from `reserve` stops the decode and is returned as it is.
pub(crate) fn decode_cbor_reserving(
    bytes: &[u8],
    limits: DecodeLimits,
    reserve: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Value> {
    if bytes.len() > limits.max_cell_bytes {
        return Err(Error::too_large(
            crate::error::SizeBudget::Cell,
            bytes.len(),
            limits.max_cell_bytes,
        ));
    }
    #[cfg(test)]
    DECODES.with(|n| n.set(n.get() + 1));
    let mut reader = CborReader {
        decoder: ciborium_ll::Decoder::from(bytes),
        len: bytes.len(),
        max_depth: limits.max_depth,
        reserve,
    };
    reader.item(limits.max_depth)
}

/// A streaming CBOR reader that builds a [`Value`] directly, reserving each
/// allocation before it is made.
struct CborReader<'a, 'r> {
    decoder: ciborium_ll::Decoder<&'a [u8]>,
    len: usize,
    max_depth: usize,
    reserve: &'r mut dyn FnMut(usize) -> Result<()>,
}

/// The bytes one key/value entry occupies inline in its list.
const ENTRY_NODE_BYTES: usize = BUFFER_HEADER_BYTES + VALUE_NODE_BYTES;

fn malformed<E: std::fmt::Debug>(e: ciborium_ll::Error<E>) -> Error {
    Error::invalid(format!("cbor decode: {e:?}"))
}

impl CborReader<'_, '_> {
    fn header(&mut self) -> Result<ciborium_ll::Header> {
        self.decoder.pull().map_err(malformed)
    }

    /// Input bytes not yet read.
    fn remaining(&mut self) -> usize {
        self.len.saturating_sub(self.decoder.offset())
    }

    /// Reserve `count` items of `each` bytes, refusing a count the rest of the
    /// input cannot hold at `min_encoded` bytes per item.
    fn reserve_items(&mut self, count: usize, each: usize, min_encoded: usize) -> Result<()> {
        if count.saturating_mul(min_encoded) > self.remaining() {
            return Err(Error::invalid("cbor decode: truncated container"));
        }
        (self.reserve)(count.saturating_mul(each))
    }

    /// One item whose own node the caller has reserved; `depth_left` more
    /// container levels may open below this point.
    fn item(&mut self, depth_left: usize) -> Result<Value> {
        use ciborium_ll::{Header, simple, tag};
        Ok(match self.header()? {
            Header::Positive(x) => {
                Value::Int(i64::try_from(x).map_err(|_| Error::invalid("cbor int out of i64"))?)
            }
            // The wire value has all bits inverted: -1 - x.
            Header::Negative(x) => Value::Int(
                i64::try_from(x).map_err(|_| Error::invalid("cbor int out of i64"))? ^ !0,
            ),
            Header::Float(f) => Value::Double(f),
            Header::Simple(simple::FALSE) => Value::Bool(false),
            Header::Simple(simple::TRUE) => Value::Bool(true),
            Header::Simple(simple::NULL | simple::UNDEFINED) => Value::Null,
            Header::Simple(_) | Header::Break => {
                return Err(Error::invalid("cbor decode: unexpected simple value"));
            }
            Header::Bytes(len) => Value::Bytes(self.bytes(len)?),
            Header::Text(len) => Value::Str(self.text(len)?),
            Header::Tag(t @ (tag::BIGPOS | tag::BIGNEG)) => self.bignum(t == tag::BIGNEG)?,
            Header::Tag(_) => return Err(Error::invalid("unsupported cbor value")),
            Header::Array(len) => {
                let Some(depth_left) = depth_left.checked_sub(1) else {
                    return Err(Error::Refused(RefuseReason::TooDeep(self.max_depth)));
                };
                let mut items = Vec::new();
                match len {
                    Some(n) => {
                        self.reserve_items(n, VALUE_NODE_BYTES, 1)?;
                        items.reserve_exact(n);
                        for _ in 0..n {
                            items.push(self.item(depth_left)?);
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            self.reserve_items(1, VALUE_NODE_BYTES, 1)?;
                            items.push(self.item(depth_left)?);
                        }
                        items.shrink_to_fit();
                    }
                }
                Value::Array(items)
            }
            Header::Map(len) => {
                let Some(depth_left) = depth_left.checked_sub(1) else {
                    return Err(Error::Refused(RefuseReason::TooDeep(self.max_depth)));
                };
                let mut entries: Vec<(String, Value)> = Vec::new();
                match len {
                    Some(n) => {
                        self.reserve_items(n, ENTRY_NODE_BYTES, 2)?;
                        entries.reserve_exact(n);
                        for _ in 0..n {
                            let key = self.key()?;
                            entries.push((key, self.item(depth_left)?));
                        }
                    }
                    None => {
                        while !self.at_break()? {
                            self.reserve_items(1, ENTRY_NODE_BYTES, 2)?;
                            let key = self.key()?;
                            entries.push((key, self.item(depth_left)?));
                        }
                        entries.shrink_to_fit();
                    }
                }
                sort_kvlist(&mut entries)?;
                Value::KvList(entries)
            }
        })
    }

    /// Whether the next header ends an indefinite container, consuming it if so.
    fn at_break(&mut self) -> Result<bool> {
        match self.header()? {
            ciborium_ll::Header::Break => Ok(true),
            other => {
                self.decoder.push(other);
                Ok(false)
            }
        }
    }

    /// A map key: text, or null for the empty key.
    fn key(&mut self) -> Result<String> {
        use ciborium_ll::{Header, simple};
        match self.header()? {
            Header::Text(len) => self.text(len),
            Header::Simple(simple::NULL | simple::UNDEFINED) => Ok(String::new()),
            _ => Err(Error::invalid("cbor map key is not text")),
        }
    }

    fn bytes(&mut self, len: Option<usize>) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut scratch = [0_u8; 4096];
        let remaining = self.remaining();
        let reserve = &mut *self.reserve;
        let mut segments = self.decoder.bytes(len);
        while let Some(mut segment) = segments.pull().map_err(malformed)? {
            if segment.left() > remaining {
                return Err(Error::invalid("cbor decode: truncated bytes"));
            }
            reserve(segment.left())?;
            out.reserve_exact(segment.left());
            while let Some(chunk) = segment.pull(&mut scratch).map_err(malformed)? {
                out.extend_from_slice(chunk);
            }
        }
        out.shrink_to_fit();
        Ok(out)
    }

    fn text(&mut self, len: Option<usize>) -> Result<String> {
        let mut out = String::new();
        let mut scratch = [0_u8; 4096];
        let remaining = self.remaining();
        let reserve = &mut *self.reserve;
        let mut segments = self.decoder.text(len);
        while let Some(mut segment) = segments.pull().map_err(malformed)? {
            if segment.left() > remaining {
                return Err(Error::invalid("cbor decode: truncated text"));
            }
            reserve(segment.left())?;
            out.reserve_exact(segment.left());
            while let Some(chunk) = segment.pull(&mut scratch).map_err(malformed)? {
                out.push_str(chunk);
            }
        }
        out.shrink_to_fit();
        Ok(out)
    }

    /// A tag 2 or 3 bignum of at most 16 bytes, as an `i64`; any other tagged
    /// item is unsupported.
    fn bignum(&mut self, negative: bool) -> Result<Value> {
        let len = match self.header()? {
            ciborium_ll::Header::Bytes(Some(len)) if len <= 16 => len,
            _ => return Err(Error::invalid("unsupported cbor value")),
        };
        let mut digits = [0_u8; 16];
        let mut scratch = [0_u8; 16];
        let mut read = 0;
        let mut segments = self.decoder.bytes(Some(len));
        while let Some(mut segment) = segments.pull().map_err(malformed)? {
            while let Some(chunk) = segment.pull(&mut scratch).map_err(malformed)? {
                digits[read..read + chunk.len()].copy_from_slice(chunk);
                read += chunk.len();
            }
        }
        let raw = digits[..read]
            .iter()
            .fold(0_u128, |acc, &b| (acc << 8) | u128::from(b));
        let raw = i64::try_from(raw).map_err(|_| Error::invalid("cbor int out of i64"))?;
        Ok(Value::Int(if negative { raw ^ !0 } else { raw }))
    }
}

/// Sort a key/value list by raw key bytes and refuse duplicate keys.
pub fn sort_kvlist(list: &mut [(String, Value)]) -> Result<()> {
    list.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if list.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(Error::invalid("duplicate attribute key"));
    }
    Ok(())
}

/// The `render_v1` recursive rendering of FORMAT.md section 2.
///
/// Non-finite doubles and bytes use the spellings of the workspace's OTLP JSON
/// encoder (`otel_arrow_dfe_pdata::otlp::json`): `"NaN"`, `"Infinity"` and
/// `"-Infinity"`, and padded standard base64 for bytes, so a reader that
/// knows OTLP JSON reads them the same way here. The rendering as a whole is
/// not OTLP JSON: a value is not wrapped in an `AnyValue` object, integers are
/// JSON numbers, and kvlists are JSON objects with sorted keys.
pub fn render_v1(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Str(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(BASE64_STANDARD.encode(b)),
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
        serde_json::Value::String("Infinity".into())
    } else if d == f64::NEG_INFINITY {
        serde_json::Value::String("-Infinity".into())
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

/// Inline bytes of one decoded value node.
pub const VALUE_NODE_BYTES: usize = size_of::<Value>();

/// Inline bytes of one owned string or byte buffer.
pub const BUFFER_HEADER_BYTES: usize = size_of::<String>();

/// Decoded footprint of a value: [`VALUE_NODE_BYTES`] per node plus string,
/// byte and key content.
#[must_use]
pub fn value_bytes(v: &Value) -> usize {
    VALUE_NODE_BYTES
        + match v {
            Value::Null | Value::Int(_) | Value::Double(_) | Value::Bool(_) => 0,
            Value::Str(s) => s.len(),
            Value::Bytes(b) => b.len(),
            Value::Array(items) => items.iter().map(value_bytes).sum(),
            Value::KvList(entries) => kv_bytes(entries),
        }
}

/// Decoded footprint of one key/value entry.
#[must_use]
pub fn entry_bytes(key: &str, value: &Value) -> usize {
    BUFFER_HEADER_BYTES + key.len() + value_bytes(value)
}

/// Decoded footprint of a key/value list.
#[must_use]
pub fn kv_bytes(list: &[(String, Value)]) -> usize {
    list.iter().map(|(k, v)| entry_bytes(k, v)).sum()
}

#[cfg(test)]
thread_local! {
    /// CBOR cells this thread has decoded.
    static DECODES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// CBOR cells this thread has decoded so far.
#[cfg(test)]
pub(crate) fn decodes() -> usize {
    DECODES.with(std::cell::Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RefuseReason;

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
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
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
            Err(Error::Refused(RefuseReason::TooDeep(8)))
        ));
        let array = ciborium::Value::Array(vec![int()]);
        assert!(decode_cbor(&maps(limit - 1, array.clone()), limits).is_ok());
        assert!(decode_cbor(&maps(limit, array), limits).is_err());
    }

    /// Scenario: maps nested one level past the limit, which the conversion
    /// refuses, and far past it, which ciborium's own recursion limit refuses
    /// while parsing.
    /// Guarantees: both are reported as `TooDeep` carrying the configured
    /// limit, never as invalid content, so the refusal names the setting
    /// that governs it whichever layer caught it.
    #[test]
    fn decode_cbor_reports_excess_depth_as_too_deep() {
        let deep = |levels: usize| {
            let mut v = ciborium::Value::Null;
            for _ in 0..levels {
                v = ciborium::Value::Array(vec![v]);
            }
            let mut buf = Vec::new();
            ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
            buf
        };
        let limits = DecodeLimits::new(4, usize::MAX);
        for levels in [5, 64] {
            let result = decode_cbor(&deep(levels), limits);
            assert!(
                matches!(result, Err(Error::Refused(RefuseReason::TooDeep(4)))),
                "levels={levels}: {result:?}"
            );
        }
    }

    /// A reservation callback that refuses a running total past `limit`.
    fn counter(limit: usize) -> impl FnMut(usize) -> Result<()> {
        let mut reserved = 0_usize;
        move |bytes| {
            let next = reserved.saturating_add(bytes);
            if next > limit {
                return Err(Error::too_large(
                    crate::error::SizeBudget::Table,
                    next,
                    limit,
                ));
            }
            reserved = next;
            Ok(())
        }
    }

    fn encode(v: &ciborium::Value) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::into_writer(v, &mut buf).expect("encode test cbor");
        buf
    }

    /// Scenario: a one-MiB flat CBOR array of small ints, 32 MiB once
    /// decoded, is decoded against a 16 MiB reservation limit.
    /// Guarantees: the decode is refused on the array's own reservation,
    /// before any element is allocated: the one request made is the whole
    /// array's nodes and nothing was granted.
    #[test]
    fn a_wide_cell_is_refused_before_its_tree_is_built() {
        let n = (1 << 20) - 5;
        let buf = encode(&ciborium::Value::Array(vec![
            ciborium::Value::Integer(
                0.into()
            );
            n
        ]));
        assert_eq!(buf.len(), 1 << 20);
        let mut requests = Vec::new();
        let mut limit = counter(16 << 20);
        let result = decode_cbor_reserving(&buf, DecodeLimits::new(32, 1 << 20), &mut |bytes| {
            requests.push(bytes);
            limit(bytes)
        });
        assert!(matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
        assert_eq!(requests, vec![n * VALUE_NODE_BYTES]);
    }

    /// Scenario: a CBOR array of 200 000 singleton arrays `[0]`, two encoded
    /// bytes each and 64 decoded, against a 4 MiB reservation limit, and a
    /// singleton chain nested exactly to the depth limit.
    /// Guarantees: the wide cell is refused once its reservations reach the
    /// limit, never holding more than the limit, and the chain reserves
    /// exactly its decoded size.
    #[test]
    fn singleton_nesting_is_reserved_node_by_node() {
        let one = ciborium::Value::Array(vec![ciborium::Value::Integer(0.into())]);
        let wide = encode(&ciborium::Value::Array(vec![one; 200_000]));
        let mut granted = 0_usize;
        let mut limit = counter(4 << 20);
        let result = decode_cbor_reserving(&wide, DecodeLimits::new(32, usize::MAX), &mut |b| {
            limit(b)?;
            granted += b;
            Ok(())
        });
        assert!(matches!(
            result,
            Err(Error::Refused(RefuseReason::RequestTooLarge(_)))
        ));
        assert!(granted <= 4 << 20);

        let mut chain = ciborium::Value::Integer(0.into());
        for _ in 0..32 {
            chain = ciborium::Value::Array(vec![chain]);
        }
        let mut reserved = 0;
        let v = decode_cbor_reserving(
            &encode(&chain),
            DecodeLimits::new(32, usize::MAX),
            &mut |b| {
                reserved += b;
                Ok(())
            },
        )
        .expect("at the depth limit");
        assert_eq!(reserved + VALUE_NODE_BYTES, value_bytes(&v));
        assert_eq!(value_bytes(&v), 33 * VALUE_NODE_BYTES);
    }

    /// Scenario: a hand-encoded payload using indefinite-length text, bytes,
    /// array and map, a null map key, `undefined`, a half float, a negative
    /// integer and a tag-2 bignum.
    /// Guarantees: every form decodes to the expected value, and the
    /// reservations add up to its [`value_bytes`] less the root node.
    #[test]
    fn reservations_add_up_to_the_decoded_size() {
        let buf: Vec<u8> = [
            &[0xbf][..],                           // map, indefinite
            &[0x7f, 0x61, b'a', 0x61, b'b', 0xff], // key "ab", indefinite text
            &[
                0x9f, 0x5f, 0x41, 1, 0x41, 2, 0xff, 0xf7, 0xf9, 0x3c, 0x00, 0xff,
            ], // [h'0102', undefined, 1.0]
            &[0xf6, 0x38, 0x63],                   // null key -> -100
            &[0x61, b'z', 0xc2, 0x42, 0x01, 0x00], // "z" -> bignum 256
            &[0xff],
        ]
        .concat();
        let mut reserved = 0;
        let v = decode_cbor_reserving(&buf, DecodeLimits::new(32, usize::MAX), &mut |b| {
            reserved += b;
            Ok(())
        })
        .expect("decode");
        assert_eq!(
            v,
            Value::KvList(vec![
                (String::new(), Value::Int(-100)),
                (
                    "ab".into(),
                    Value::Array(vec![
                        Value::Bytes(vec![1, 2]),
                        Value::Null,
                        Value::Double(1.0)
                    ])
                ),
                ("z".into(), Value::Int(256)),
            ])
        );
        assert_eq!(reserved + VALUE_NODE_BYTES, value_bytes(&v));
    }

    /// Scenario: render_v1 over every scalar kind and a nested kvlist.
    /// Guarantees: the JSON mapping of FORMAT.md section 2 is produced exactly.
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
            r#"{"b":"qxI=","d":"NaN","e":1.5,"i":42,"n":null,"s":"x","t":true}"#
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
            Some("\"qw==\"")
        );
        assert_eq!(
            map_string(&Value::Double(f64::INFINITY)).as_deref(),
            Some("\"Infinity\"")
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
