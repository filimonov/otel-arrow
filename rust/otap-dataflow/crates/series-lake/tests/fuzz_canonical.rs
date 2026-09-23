// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for the canonical encoder and CBOR decoder (FORMAT.md section 1).

use otel_arrow_dfe_series_lake::canonical::{Descriptor, Signal, canonical_bytes, series_id};
use otel_arrow_dfe_series_lake::value::{DecodeLimits, Value, decode_cbor, sort_kvlist};
use proptest::prelude::*;

fn value_strategy() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<String>().prop_map(Value::Str),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(|b| Value::Double(f64::from_bits(b))),
        any::<bool>().prop_map(Value::Bool),
    ];
    leaf.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((any::<String>(), inner), 0..4).prop_map(|mut kvs| {
                kvs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                kvs.dedup_by(|a, b| a.0 == b.0);
                Value::KvList(kvs)
            }),
        ]
    })
}

fn to_cbor(v: &Value) -> ciborium::Value {
    match v {
        Value::Null => ciborium::Value::Null,
        Value::Str(s) => ciborium::Value::Text(s.clone()),
        Value::Bytes(b) => ciborium::Value::Bytes(b.clone()),
        Value::Int(i) => ciborium::Value::Integer((*i).into()),
        Value::Double(d) => ciborium::Value::Float(*d),
        Value::Bool(b) => ciborium::Value::Bool(*b),
        Value::Array(items) => ciborium::Value::Array(items.iter().map(to_cbor).collect()),
        Value::KvList(kvs) => ciborium::Value::Map(
            kvs.iter()
                .map(|(k, v)| (ciborium::Value::Text(k.clone()), to_cbor(v)))
                .collect(),
        ),
    }
}

fn canon_nan(v: &Value) -> Value {
    match v {
        Value::Double(d) if d.is_nan() => Value::Double(f64::NAN),
        Value::Array(items) => Value::Array(items.iter().map(canon_nan).collect()),
        Value::KvList(kvs) => {
            Value::KvList(kvs.iter().map(|(k, v)| (k.clone(), canon_nan(v))).collect())
        }
        other => other.clone(),
    }
}

fn desc(attrs: Vec<(String, Value)>) -> Descriptor {
    Descriptor {
        signal: Signal::Logs,
        resource_attrs: vec![].into(),
        resource_schema_url: String::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_schema_url: String::new(),
        scope_attrs: vec![].into(),
        metric: None,
        attrs,
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    /// Scenario: arbitrary value trees round-trip through CBOR.
    /// Guarantees: decode never panics and reproduces the tree (NaN payloads excepted, which CBOR
    /// may collapse), so OTAP-converted and original descriptors hash identically.
    #[test]
    fn cbor_round_trip_never_panics(v in value_strategy()) {
        let mut buf = Vec::new();
        ciborium::into_writer(&to_cbor(&v), &mut buf).expect("encode");
        let decoded = decode_cbor(&buf, DecodeLimits::new(32, usize::MAX)).expect("decode");
        let a = canonical_bytes(&desc(vec![("k".into(), canon_nan(&v))]));
        let b = canonical_bytes(&desc(vec![("k".into(), canon_nan(&decoded))]));
        prop_assert_eq!(a, b);
    }

    /// Scenario: arbitrary byte strings, and truncated prefixes of a well-formed encoding,
    /// handed straight to the CBOR decoder.
    /// Guarantees: the decoder never panics on hostile input -- it returns a value or an
    /// error -- and whatever it does return re-encodes into canonical bytes without panicking.
    #[test]
    fn cbor_decoder_survives_arbitrary_bytes(
        junk in prop::collection::vec(any::<u8>(), 0..64),
        v in value_strategy(),
        cut in 0usize..64,
    ) {
        if let Ok(value) = decode_cbor(&junk, DecodeLimits::new(32, usize::MAX)) {
            let _ = canonical_bytes(&desc(vec![("k".into(), value)]));
        }

        let mut buf = Vec::new();
        ciborium::into_writer(&to_cbor(&v), &mut buf).expect("encode");
        let truncated = &buf[..cut.min(buf.len())];
        if let Ok(value) = decode_cbor(truncated, DecodeLimits::new(32, usize::MAX)) {
            let _ = canonical_bytes(&desc(vec![("k".into(), value)]));
        }

        // A depth budget of zero refuses every container instead of recursing.
        let _ = decode_cbor(&buf, DecodeLimits::new(0, usize::MAX));
    }

    /// Scenario: an attribute list of at least two distinct keys, handed to the encoder
    /// twice -- once already sorted, once in a proptest-generated shuffle that is
    /// rejected unless it really is a different order, then normalized by `sort_kvlist`
    /// as the extractor does.
    /// Guarantees: the two encodings and their series ids are byte-identical, so key
    /// order in the input never reaches the identity. The two-distinct-key floor and the
    /// explicit order-differs assertion are what stop the test from silently comparing a
    /// list with itself and proving nothing.
    #[test]
    fn encoding_is_order_independent(
        (sorted, shuffled) in prop::collection::vec((any::<String>(), value_strategy()), 2..7)
            .prop_map(|kvs| {
                let mut sorted = kvs;
                sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                sorted.dedup_by(|a, b| a.0 == b.0);
                sorted
            })
            // Duplicate keys are removed above, so a short list can fall below the floor.
            .prop_filter("at least two distinct keys", |s| s.len() >= 2)
            .prop_flat_map(|sorted| {
                let shuffled = Just(sorted.clone()).prop_shuffle();
                (Just(sorted), shuffled)
            })
            .prop_filter("the two orders must differ", |(sorted, shuffled)| {
                let a: Vec<&str> = sorted.iter().map(|(k, _)| k.as_str()).collect();
                let b: Vec<&str> = shuffled.iter().map(|(k, _)| k.as_str()).collect();
                a != b
            }),
    ) {
        prop_assert!(sorted.len() >= 2, "the property needs at least two keys to mean anything");
        let sorted_keys: Vec<&str> = sorted.iter().map(|(k, _)| k.as_str()).collect();
        let shuffled_keys: Vec<&str> = shuffled.iter().map(|(k, _)| k.as_str()).collect();
        prop_assert_ne!(
            &sorted_keys,
            &shuffled_keys,
            "the shuffled input must reach the encoder in a different order"
        );

        // Encode the shuffled list as the extractor would: normalize, then encode.
        let mut shuffled = shuffled;
        sort_kvlist(&mut shuffled).expect("unique keys");
        let a = canonical_bytes(&desc(sorted));
        let b = canonical_bytes(&desc(shuffled));
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(series_id(&a), series_id(&b));
    }
}

/// The decoder this crate used before it decoded in one pass: ciborium's
/// `Value` tree, converted afterwards. Kept as the oracle `decode_cbor` must
/// agree with; `None` is any refusal.
fn reference_decode(bytes: &[u8], max_depth: usize) -> Option<Value> {
    fn convert(raw: ciborium::Value, depth_left: usize) -> Option<Value> {
        Some(match raw {
            ciborium::Value::Null => Value::Null,
            ciborium::Value::Bool(b) => Value::Bool(b),
            ciborium::Value::Integer(i) => Value::Int(i64::try_from(i).ok()?),
            ciborium::Value::Float(f) => Value::Double(f),
            ciborium::Value::Text(s) => Value::Str(s),
            ciborium::Value::Bytes(b) => Value::Bytes(b),
            ciborium::Value::Array(items) => {
                let depth_left = depth_left.checked_sub(1)?;
                Value::Array(
                    items
                        .into_iter()
                        .map(|item| convert(item, depth_left))
                        .collect::<Option<_>>()?,
                )
            }
            ciborium::Value::Map(entries) => {
                let depth_left = depth_left.checked_sub(1)?;
                let mut out = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    let key = match k {
                        ciborium::Value::Text(s) => s,
                        ciborium::Value::Null => String::new(),
                        _ => return None,
                    };
                    out.push((key, convert(v, depth_left)?));
                }
                sort_kvlist(&mut out).ok()?;
                Value::KvList(out)
            }
            _ => return None,
        })
    }
    let raw: ciborium::Value =
        ciborium::de::from_reader_with_recursion_limit(bytes, max_depth + 1).ok()?;
    convert(raw, max_depth)
}

/// Structural equality with doubles compared by bits.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Double(x), Value::Double(y)) => x.to_bits() == y.to_bits(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        (Value::KvList(x), Value::KvList(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((kp, p), (kq, q))| kp == kq && same(p, q))
        }
        _ => a == b,
    }
}

/// Arbitrary CBOR trees, including what the decoder must refuse: tags,
/// integers outside `i64` (encoded as bignums), non-text map keys and
/// duplicate keys.
fn raw_cbor_strategy() -> impl Strategy<Value = ciborium::Value> {
    let leaf = prop_oneof![
        Just(ciborium::Value::Null),
        any::<bool>().prop_map(ciborium::Value::Bool),
        any::<i64>().prop_map(|i| ciborium::Value::Integer(i.into())),
        any::<u64>().prop_map(|i| ciborium::Value::Integer(i.into())),
        (-(1_i128 << 64)..(1_i128 << 64)).prop_map(|i| ciborium::Value::Integer(
            ciborium::value::Integer::try_from(i).expect("within the CBOR integer range")
        )),
        any::<u64>().prop_map(|b| ciborium::Value::Float(f64::from_bits(b))),
        ".{0,8}".prop_map(ciborium::Value::Text),
        prop::collection::vec(any::<u8>(), 0..8).prop_map(ciborium::Value::Bytes),
    ];
    leaf.prop_recursive(6, 48, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(ciborium::Value::Array),
            prop::collection::vec(
                (
                    prop_oneof![
                        "[ab]{0,2}".prop_map(ciborium::Value::Text),
                        Just(ciborium::Value::Null),
                        any::<u8>().prop_map(|i| ciborium::Value::Integer(i.into())),
                    ],
                    inner.clone()
                ),
                0..4
            )
            .prop_map(ciborium::Value::Map),
            (0_u64..8, inner).prop_map(|(t, v)| ciborium::Value::Tag(t, Box::new(v))),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, .. ProptestConfig::default() })]

    /// Scenario: arbitrary CBOR trees -- tags, bignums, non-text and duplicate
    /// keys, nesting around a depth limit of 4 -- plus arbitrary bytes and
    /// truncations, decoded by `decode_cbor` and by the ciborium-tree decoder
    /// it replaced.
    /// Guarantees: both accept exactly the same payloads and produce the same
    /// value, doubles bit for bit, so identities and rendered cells are
    /// unchanged.
    #[test]
    fn decode_cbor_agrees_with_the_ciborium_tree_decoder(
        raw in raw_cbor_strategy(),
        junk in prop::collection::vec(any::<u8>(), 0..32),
        cut in 0usize..64,
    ) {
        let mut buf = Vec::new();
        ciborium::into_writer(&raw, &mut buf).expect("encode");
        for bytes in [&buf[..], &buf[..cut.min(buf.len())], &junk[..]] {
            let new = decode_cbor(bytes, DecodeLimits::new(4, usize::MAX)).ok();
            let old = reference_decode(bytes, 4);
            match (&new, &old) {
                (Some(a), Some(b)) => prop_assert!(same(a, b), "{a:?} != {b:?}"),
                (None, None) => {}
                _ => prop_assert!(false, "decode {new:?}, reference {old:?} for {bytes:02x?}"),
            }
        }
    }
}
