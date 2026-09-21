// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for the canonical encoder and CBOR decoder (spec section 9.3).

use otel_arrow_dfe_series_lake::canonical::{Descriptor, Signal, canonical_bytes, series_id};
use otel_arrow_dfe_series_lake::value::{Value, decode_cbor, sort_kvlist};
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
        resource_attrs: vec![],
        resource_schema_url: String::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_schema_url: String::new(),
        scope_attrs: vec![],
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
        let decoded = decode_cbor(&buf, 32).expect("decode");
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
        if let Ok(value) = decode_cbor(&junk, 32) {
            let _ = canonical_bytes(&desc(vec![("k".into(), value)]));
        }

        let mut buf = Vec::new();
        ciborium::into_writer(&to_cbor(&v), &mut buf).expect("encode");
        let truncated = &buf[..cut.min(buf.len())];
        if let Ok(value) = decode_cbor(truncated, 32) {
            let _ = canonical_bytes(&desc(vec![("k".into(), value)]));
        }

        // A depth budget of zero refuses every container instead of recursing.
        let _ = decode_cbor(&buf, 0);
    }

    /// Scenario: the same attribute list handed to the encoder in two different orders,
    /// one already sorted and one shuffled and then normalized by `sort_kvlist`.
    /// Guarantees: the two encodings are byte-identical, so key order in the input never
    /// reaches the identity, and the shuffled input really is a different order before
    /// normalization whenever the list has at least two keys.
    #[test]
    fn encoding_is_order_independent(
        kvs in prop::collection::vec((any::<String>(), value_strategy()), 0..6),
        rotate in 0usize..6,
    ) {
        let mut sorted = kvs;
        sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        sorted.dedup_by(|a, b| a.0 == b.0);

        let mut shuffled = sorted.clone();
        if shuffled.len() > 1 {
            // A non-zero rotation of a list of distinct keys is never the identity, so the
            // shuffled input is guaranteed to reach the encoder in a different order.
            // (The brief's rotate-then-reverse is the identity for a two-key list.)
            let by = 1 + rotate % (shuffled.len() - 1);
            shuffled.rotate_left(by);
            let sorted_keys: Vec<&str> = sorted.iter().map(|(k, _)| k.as_str()).collect();
            let shuffled_keys: Vec<&str> = shuffled.iter().map(|(k, _)| k.as_str()).collect();
            prop_assert_ne!(sorted_keys, shuffled_keys, "the shuffled input must differ before sorting");
        }

        // Encode the shuffled list as the extractor would: normalize, then encode.
        sort_kvlist(&mut shuffled).expect("unique keys");
        let a = canonical_bytes(&desc(sorted));
        let b = canonical_bytes(&desc(shuffled));
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(series_id(&a), series_id(&b));
    }
}
