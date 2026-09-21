// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for extraction (spec section 9.3).
//!
//! The generators cover every `AnyValue` variant, including the unset value,
//! empty strings and byte strings, both zeros, subnormals, the integer extremes
//! and bounded nested arrays and key/value lists, because those are exactly the
//! shapes where OTAP's column encoding and the canonical encoding can disagree.

use std::collections::BTreeSet;

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::canonical::hex;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use proptest::prelude::*;

/// One generated attribute: a key and any OTLP value.
#[derive(Debug, Clone)]
struct Attr {
    key: String,
    value: AnyValue,
}

/// Keys are short and drawn from a small alphabet so that collisions, and
/// therefore the deduplication path, happen often.
fn attr_key() -> impl Strategy<Value = String> {
    "[a-z.]{1,4}"
}

/// Scalar values, weighted so that the encoding boundaries are common rather than
/// astronomically rare: the empty string, the empty byte string, both zeros,
/// subnormals and the integer extremes.
fn leaf_value() -> impl Strategy<Value = AnyValue> {
    prop_oneof![
        // The unset value: distinct from every default, and must stay distinct.
        1 => Just(AnyValue { value: None }),
        4 => "[a-zA-Z0-9]{0,6}".prop_map(|s| any_str(&s)),
        1 => Just(any_str("")),
        3 => prop::collection::vec(any::<u8>(), 0..6)
            .prop_map(|b| wrap(any_value::Value::BytesValue(b))),
        1 => Just(wrap(any_value::Value::BytesValue(Vec::new()))),
        3 => (-1000i64..1000).prop_map(|i| wrap(any_value::Value::IntValue(i))),
        1 => prop_oneof![Just(0i64), Just(i64::MIN), Just(i64::MAX)]
            .prop_map(|i| wrap(any_value::Value::IntValue(i))),
        3 => (-1000.0f64..1000.0).prop_map(|d| wrap(any_value::Value::DoubleValue(d))),
        1 => prop_oneof![
                Just(0.0f64),
                Just(-0.0f64),
                Just(f64::from_bits(1)),
                Just(f64::MIN_POSITIVE),
                Just(f64::MAX),
                Just(f64::MIN),
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
            ]
            .prop_map(|d| wrap(any_value::Value::DoubleValue(d))),
        2 => any::<bool>().prop_map(|b| wrap(any_value::Value::BoolValue(b))),
    ]
}

fn wrap(v: any_value::Value) -> AnyValue {
    AnyValue { value: Some(v) }
}

fn any_str(s: &str) -> AnyValue {
    wrap(any_value::Value::StringValue(s.to_string()))
}

/// Values including bounded nested arrays and key/value lists.
///
/// Nested keys are deduplicated: a kvlist with a repeated key is invalid content
/// that the CBOR decoder refuses by design, which is covered elsewhere.
fn any_value_strategy() -> impl Strategy<Value = AnyValue> {
    leaf_value().prop_recursive(3, 24, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3)
                .prop_map(|values| wrap(any_value::Value::ArrayValue(ArrayValue { values }))),
            prop::collection::vec((attr_key(), inner), 0..3).prop_map(|entries| {
                let mut values: Vec<KeyValue> = Vec::new();
                for (key, value) in entries {
                    if !values.iter().any(|k| k.key == key) {
                        values.push(KeyValue {
                            key,
                            value: Some(value),
                        });
                    }
                }
                wrap(any_value::Value::KvlistValue(KeyValueList { values }))
            }),
        ]
    })
}

fn attr() -> impl Strategy<Value = Attr> {
    (attr_key(), any_value_strategy()).prop_map(|(key, value)| Attr { key, value })
}

/// An attribute list with unique keys; duplicate keys are invalid OTLP, tested
/// elsewhere. The count reaches zero so that an attribute-free record is covered.
fn attr_list(max: usize) -> impl Strategy<Value = Vec<Attr>> {
    prop::collection::vec(attr(), 0..max).prop_map(|attrs| {
        let mut out: Vec<Attr> = Vec::new();
        for a in attrs {
            if !out.iter().any(|x| x.key == a.key) {
                out.push(a);
            }
        }
        out
    })
}

fn kv(a: &Attr) -> KeyValue {
    KeyValue {
        key: a.key.clone(),
        value: Some(a.value.clone()),
    }
}

fn kvs(attrs: &[Attr]) -> Vec<KeyValue> {
    attrs.iter().map(kv).collect()
}

/// Split records at generated boundaries rather than at a fixed chunk size.
fn split<T: Clone>(items: &[T], sizes: &[usize]) -> Vec<Vec<T>> {
    let mut out = Vec::new();
    let mut idx = 0;
    let mut which = 0;
    while idx < items.len() {
        let n = sizes[which % sizes.len()].max(1).min(items.len() - idx);
        out.push(items[idx..idx + n].to_vec());
        idx += n;
        which += 1;
    }
    out
}

// ------------------------------------------------------- request builders

fn logs_of(records: &[Vec<Attr>], resource: &[Attr]) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: kvs(resource),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: records
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| LogRecord {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: kvs(attrs),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn metrics_wrap(metrics: Vec<Metric>, resource: &[Attr]) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: kvs(resource),
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

fn numbers_of(points: &[Vec<Attr>], resource: &[Attr]) -> MetricsData {
    metrics_wrap(
        vec![Metric {
            name: "m".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: points
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| NumberDataPoint {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: kvs(attrs),
                        value: Some(number_data_point::Value::AsInt(i as i64)),
                        ..Default::default()
                    })
                    .collect(),
            })),
            ..Default::default()
        }],
        resource,
    )
}

fn histograms_of(points: &[Vec<Attr>], resource: &[Attr]) -> MetricsData {
    metrics_wrap(
        vec![Metric {
            name: "h".into(),
            data: Some(metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: points
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| HistogramDataPoint {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: kvs(attrs),
                        count: i as u64,
                        sum: Some(i as f64),
                        bucket_counts: vec![i as u64, 0],
                        explicit_bounds: vec![1.0],
                        ..Default::default()
                    })
                    .collect(),
            })),
            ..Default::default()
        }],
        resource,
    )
}

// ------------------------------------------------------------ extraction

fn cfg_with(series_attributes: Vec<String>) -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = series_attributes;
    cfg
}

fn logs_ids(data: &LogsData, cfg: &LakeConfig) -> BTreeSet<String> {
    let mut records = encode_logs(data);
    let out = extract(&mut records, cfg).expect("extract logs");
    out.descriptors.iter().map(|d| hex(&d.series_id)).collect()
}

fn metric_ids(data: &MetricsData, cfg: &LakeConfig) -> BTreeSet<String> {
    let mut records = encode_metrics(data);
    let out = extract(&mut records, cfg).expect("extract metrics");
    out.descriptors.iter().map(|d| hex(&d.series_id)).collect()
}

/// Every signal's identity set for one set of records, in one request.
fn all_ids(records: &[Vec<Attr>], resource: &[Attr], cfg: &LakeConfig) -> [BTreeSet<String>; 3] {
    [
        logs_ids(&logs_of(records, resource), cfg),
        metric_ids(&numbers_of(records, resource), cfg),
        metric_ids(&histograms_of(records, resource), cfg),
    ]
}

/// The same, but delivered as several requests split at `sizes`.
fn all_ids_split(
    records: &[Vec<Attr>],
    resource: &[Attr],
    sizes: &[usize],
    cfg: &LakeConfig,
) -> [BTreeSet<String>; 3] {
    let mut out = [BTreeSet::new(), BTreeSet::new(), BTreeSet::new()];
    for chunk in split(records, sizes) {
        let ids = all_ids(&chunk, resource, cfg);
        for (acc, got) in out.iter_mut().zip(ids) {
            acc.extend(got);
        }
    }
    out
}

fn keys_of(records: &[Vec<Attr>]) -> Vec<String> {
    records.iter().flatten().map(|a| a.key.clone()).collect()
}

/// Records paired with a proptest-generated permutation of each one's attributes,
/// and a permutation of the resource attributes.
type Permuted = (Vec<Attr>, Vec<Attr>, Vec<Vec<Attr>>, Vec<Vec<Attr>>);

fn records_with_permutation(min: usize, max: usize) -> impl Strategy<Value = Permuted> {
    (attr_list(4), prop::collection::vec(attr_list(4), min..max)).prop_flat_map(
        |(resource, records)| {
            let shuffled_resource = Just(resource.clone()).prop_shuffle();
            let shuffled_records: Vec<BoxedStrategy<Vec<Attr>>> = records
                .iter()
                .cloned()
                .map(|r| Just(r).prop_shuffle().boxed())
                .collect();
            (
                Just(resource),
                shuffled_resource,
                Just(records),
                shuffled_records,
            )
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, .. ProptestConfig::default() })]

    /// Scenario: random OTLP logs, gauge points and histogram points whose attributes
    /// cover every value variant, including unset, empty, both zeros, subnormals, the
    /// integer extremes and bounded nested arrays and key/value lists.
    /// Guarantees: extraction never panics for any signal, and never produces more
    /// distinct identities than there are rows.
    #[test]
    fn extract_never_panics(
        resource in attr_list(5),
        records in prop::collection::vec(attr_list(5), 1..8),
    ) {
        let cfg = cfg_with(keys_of(&records));
        for ids in all_ids(&records, &resource, &cfg) {
            prop_assert!(ids.len() <= records.len());
        }
    }

    /// Scenario: the same records with every attribute list permuted by proptest, and
    /// the same records delivered as one request or split at generated boundaries, for
    /// logs, gauge points and histogram points alike.
    /// Guarantees: the set of series ids is identical in all three cases and for all
    /// three signals, so neither attribute order nor request framing reaches the
    /// identity.
    #[test]
    fn identity_is_independent_of_attribute_order_and_framing(
        (resource, shuffled_resource, records, shuffled) in records_with_permutation(2, 8),
        sizes in prop::collection::vec(1usize..4, 1..5),
    ) {
        let cfg = cfg_with(keys_of(&records));

        let whole = all_ids(&records, &resource, &cfg);
        let permuted = all_ids(&shuffled, &shuffled_resource, &cfg);
        let framed = all_ids_split(&records, &resource, &sizes, &cfg);

        for (i, name) in ["logs", "number", "histogram"].iter().enumerate() {
            prop_assert_eq!(
                &permuted[i],
                &whole[i],
                "{} identity must not depend on attribute order",
                name
            );
            prop_assert_eq!(
                &framed[i],
                &whole[i],
                "{} identity must not depend on request framing",
                name
            );
        }
    }
}
