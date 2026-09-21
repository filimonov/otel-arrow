// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for extraction (spec section 9.3).

use std::collections::BTreeSet;

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::canonical::hex;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use proptest::prelude::*;

#[derive(Debug, Clone)]
struct Attr {
    key: String,
    value: String,
}

fn attr() -> impl Strategy<Value = Attr> {
    ("[a-z.]{1,6}", "[a-zA-Z0-9]{0,6}").prop_map(|(key, value)| Attr { key, value })
}

fn kv(a: &Attr) -> KeyValue {
    KeyValue {
        key: a.key.clone(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(a.value.clone())),
        }),
    }
}

/// Deduplicate by key: OTLP with duplicate keys is invalid input, tested elsewhere.
fn unique(attrs: &[Attr]) -> Vec<Attr> {
    let mut out: Vec<Attr> = Vec::new();
    for a in attrs {
        if !out.iter().any(|x| x.key == a.key) {
            out.push(a.clone());
        }
    }
    out
}

fn logs_of(records: &[Vec<Attr>], resource: &[Attr]) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource.iter().map(kv).collect(),
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: records
                    .iter()
                    .enumerate()
                    .map(|(i, attrs)| LogRecord {
                        time_unix_nano: 1_000 + i as u64,
                        attributes: attrs.iter().map(kv).collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn metrics_of(points: &[Vec<Attr>], resource: &[Attr]) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource.iter().map(kv).collect(),
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "m".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: points
                            .iter()
                            .enumerate()
                            .map(|(i, attrs)| NumberDataPoint {
                                time_unix_nano: 1_000 + i as u64,
                                attributes: attrs.iter().map(kv).collect(),
                                value: Some(number_data_point::Value::AsInt(i as i64)),
                                ..Default::default()
                            })
                            .collect(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn cfg_with(series_attributes: Vec<String>) -> LakeConfig {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = series_attributes;
    cfg
}

fn logs_series_ids(data: &LogsData, cfg: &LakeConfig) -> BTreeSet<String> {
    let mut records = encode_logs(data);
    let out = extract(&mut records, cfg).expect("extract logs");
    out.descriptors.iter().map(|d| hex(&d.series_id)).collect()
}

fn metric_series_ids(data: &MetricsData, cfg: &LakeConfig) -> BTreeSet<String> {
    let mut records = encode_metrics(data);
    let out = extract(&mut records, cfg).expect("extract metrics");
    out.descriptors.iter().map(|d| hex(&d.series_id)).collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, .. ProptestConfig::default() })]

    /// Scenario: random OTLP logs and metrics with random attribute sets.
    /// Guarantees: extraction never panics, produces one descriptor per distinct identity
    /// and never more descriptors than rows.
    #[test]
    fn extract_never_panics(
        resource in prop::collection::vec(attr(), 0..4),
        records in prop::collection::vec(prop::collection::vec(attr(), 0..4), 1..8),
    ) {
        let resource = unique(&resource);
        let records: Vec<Vec<Attr>> = records.iter().map(|r| unique(r)).collect();
        let keys: Vec<String> = records.iter().flatten().map(|a| a.key.clone()).collect();
        let cfg = cfg_with(keys);
        let logs = logs_series_ids(&logs_of(&records, &resource), &cfg);
        prop_assert!(logs.len() <= records.len());
        let metrics = metric_series_ids(&metrics_of(&records, &resource), &cfg);
        prop_assert!(metrics.len() <= records.len());
    }

    /// Scenario: the same records with their attribute lists permuted, and the same
    /// records delivered as one request or as several.
    /// Guarantees: the set of series ids is identical in all three cases, so neither
    /// attribute order nor request framing reaches the identity.
    #[test]
    fn identity_is_independent_of_attribute_order_and_framing(
        resource in prop::collection::vec(attr(), 1..4),
        records in prop::collection::vec(prop::collection::vec(attr(), 1..4), 2..8),
    ) {
        let resource = unique(&resource);
        let records: Vec<Vec<Attr>> = records.iter().map(|r| unique(r)).collect();
        let keys: Vec<String> = records.iter().flatten().map(|a| a.key.clone()).collect();
        let cfg = cfg_with(keys);

        let whole = logs_series_ids(&logs_of(&records, &resource), &cfg);

        let mut reversed_resource = resource.clone();
        reversed_resource.reverse();
        let reversed: Vec<Vec<Attr>> = records
            .iter()
            .map(|r| {
                let mut r = r.clone();
                r.reverse();
                r
            })
            .collect();
        prop_assert_eq!(logs_series_ids(&logs_of(&reversed, &reversed_resource), &cfg), whole.clone());

        let mut split_ids: BTreeSet<String> = BTreeSet::new();
        for chunk in records.chunks(2) {
            split_ids.extend(logs_series_ids(&logs_of(chunk, &resource), &cfg));
        }
        prop_assert_eq!(split_ids, whole);
    }
}

/// The minimal case proptest shrank the cross-framing property down to.
fn one_attr_records(records: &[(&str, &str)]) -> LogsData {
    let recs: Vec<Vec<Attr>> = records
        .iter()
        .map(|(k, v)| {
            vec![Attr {
                key: (*k).to_string(),
                value: (*v).to_string(),
            }]
        })
        .collect();
    logs_of(
        &recs,
        &[Attr {
            key: "host.id".into(),
            value: "h".into(),
        }],
    )
}

/// Scenario: one log record whose only identity attribute has the empty string as
/// its value, extracted alone and again alongside a record with a non-empty value.
/// Guarantees: the record's series id is the same either way. pdata's OTAP encoder
/// drops a value column whose entries are all the type's default, so a batch of
/// nothing but empty strings carries no `str` column; `AnyValueColumns::value_at`
/// must read that as `Str("")`, the default the tag names, rather than as
/// `Value::Null`, which would both make the identity depend on request framing and
/// collide with the distinct identity the `null_value` golden vector pins down.
#[test]
fn empty_string_attribute_identity_does_not_depend_on_request_framing() {
    let cfg = cfg_with(vec!["a".into()]);
    let alone = logs_series_ids(&one_attr_records(&[("a", "")]), &cfg);
    let together = logs_series_ids(&one_attr_records(&[("a", ""), ("a", "A")]), &cfg);
    assert!(
        together.is_superset(&alone),
        "the empty-string series id must not depend on what else is in the request: \
         alone {alone:?} vs together {together:?}"
    );
}
