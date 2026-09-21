// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Every canonical golden vector, checked through OTLP -> OTAP -> extract.

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
    NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
use otel_arrow_dfe_series_lake::canonical::hex;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;

/// Kinds with no values dataset: `extract` drops them under the default policy,
/// so no descriptor reaches the output and there is nothing to compare.
const UNSUPPORTED_KINDS: [&str; 2] = ["exp_histogram", "summary"];

fn hex_decode(s: &str) -> Vec<u8> {
    ::hex::decode(s).expect("hex")
}

fn any_value_from_json(j: &serde_json::Value) -> AnyValue {
    let v = match j["type"].as_str().expect("type") {
        "null" => None,
        "str" => Some(any_value::Value::StringValue(
            j["value"].as_str().expect("str").to_string(),
        )),
        "bytes" => Some(any_value::Value::BytesValue(hex_decode(
            j["value"].as_str().expect("hex"),
        ))),
        "int" => Some(any_value::Value::IntValue(
            j["value"].as_i64().expect("int"),
        )),
        "double" => Some(any_value::Value::DoubleValue(match j.get("bits") {
            Some(bits) => f64::from_bits(bits.as_u64().expect("bits")),
            None => j["value"].as_f64().expect("double"),
        })),
        "bool" => Some(any_value::Value::BoolValue(
            j["value"].as_bool().expect("bool"),
        )),
        "array" => Some(any_value::Value::ArrayValue(ArrayValue {
            values: j["items"]
                .as_array()
                .expect("items")
                .iter()
                .map(any_value_from_json)
                .collect(),
        })),
        "kvlist" => Some(any_value::Value::KvlistValue(KeyValueList {
            values: kvs_from_json(&j["entries"]),
        })),
        other => panic!("unknown type {other}"),
    };
    AnyValue { value: v }
}

fn kvs_from_json(j: &serde_json::Value) -> Vec<KeyValue> {
    j.as_array()
        .map(|a| {
            a.iter()
                .map(|e| KeyValue {
                    key: e["key"].as_str().expect("key").to_string(),
                    value: Some(any_value_from_json(&e["value"])),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn str_field(j: &serde_json::Value, k: &str) -> String {
    j.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn keys_of(j: &serde_json::Value, field: &str) -> Vec<String> {
    j.get(field)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .map(|e| e["key"].as_str().expect("key").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn logs_for(d: &serde_json::Value) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: kvs_from_json(&d["resource_attrs"]),
                ..Default::default()
            }),
            schema_url: str_field(d, "resource_schema_url"),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: str_field(d, "scope_name"),
                    version: str_field(d, "scope_version"),
                    attributes: kvs_from_json(&d["scope_attrs"]),
                    ..Default::default()
                }),
                schema_url: str_field(d, "scope_schema_url"),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_000,
                    attributes: kvs_from_json(&d["attrs"]),
                    ..Default::default()
                }],
            }],
        }],
    }
}

fn metrics_for(d: &serde_json::Value) -> MetricsData {
    let m = &d["metric"];
    let name = str_field(m, "name");
    let unit = str_field(m, "unit");
    let point = NumberDataPoint {
        time_unix_nano: 1_000,
        attributes: kvs_from_json(&d["attrs"]),
        value: Some(number_data_point::Value::AsInt(1)),
        ..Default::default()
    };
    let temporality = match str_field(m, "temporality").as_str() {
        "delta" => AggregationTemporality::Delta as i32,
        "cumulative" => AggregationTemporality::Cumulative as i32,
        _ => AggregationTemporality::Unspecified as i32,
    };
    let data = match m["kind"].as_str().expect("kind") {
        "gauge" => metric::Data::Gauge(Gauge {
            data_points: vec![point],
        }),
        "sum" => metric::Data::Sum(Sum {
            aggregation_temporality: temporality,
            is_monotonic: m
                .get("is_monotonic")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            data_points: vec![point],
        }),
        "histogram" => metric::Data::Histogram(Histogram {
            aggregation_temporality: temporality,
            data_points: vec![HistogramDataPoint {
                time_unix_nano: 1_000,
                attributes: kvs_from_json(&d["attrs"]),
                count: 1,
                sum: Some(1.0),
                bucket_counts: vec![1],
                explicit_bounds: vec![],
                ..Default::default()
            }],
        }),
        other => panic!("golden vector uses unsupported metric kind {other}"),
    };
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: kvs_from_json(&d["resource_attrs"]),
                ..Default::default()
            }),
            schema_url: str_field(d, "resource_schema_url"),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: str_field(d, "scope_name"),
                    version: str_field(d, "scope_version"),
                    attributes: kvs_from_json(&d["scope_attrs"]),
                    ..Default::default()
                }),
                schema_url: str_field(d, "scope_schema_url"),
                metrics: vec![Metric {
                    name,
                    unit,
                    data: Some(data),
                    ..Default::default()
                }],
            }],
        }],
    }
}

/// Run every golden vector through OTLP -> OTAP -> extract and name the ones
/// whose `series_id` differs from the Python generator's.
fn deviating_vectors() -> Vec<String> {
    let raw = include_str!("golden/canonical_v1.json");
    let doc: serde_json::Value = serde_json::from_str(raw).expect("json");
    let vectors = doc["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 30);
    let mut checked = 0usize;
    let mut skipped: Vec<&str> = Vec::new();
    let mut deviations: Vec<String> = Vec::new();
    for v in vectors {
        let name = v["name"].as_str().expect("name");
        let d = &v["descriptor"];
        let expected = v["series_id_hex"].as_str().expect("id");
        let kind = d.get("metric").map(|m| m["kind"].as_str().expect("kind"));
        if kind.is_some_and(|k| UNSUPPORTED_KINDS.contains(&k)) {
            skipped.push(name);
            continue;
        }
        let mut cfg = LakeConfig::default();
        // Every identity attribute of the vector must stay in the identity.
        cfg.logs.series_attributes = keys_of(d, "attrs");
        // The vectors never project a producer id.
        cfg.producer_id_attribute = "__absent__".into();
        let got = if d["signal"] == "logs" {
            let mut records = encode_logs(&logs_for(d));
            extract(&mut records, &cfg).expect("extract logs")
        } else {
            let mut records = encode_metrics(&metrics_for(d));
            extract(&mut records, &cfg).expect("extract metrics")
        };
        assert_eq!(got.descriptors.len(), 1, "one series for {name}");
        if hex(&got.descriptors[0].series_id) != expected {
            deviations.push(name.to_string());
        }
        checked += 1;
    }
    // Pin the skips: a new vector of an unsupported kind must be noticed, not
    // silently ignored.
    assert_eq!(
        skipped,
        vec!["metrics_exp_histogram_delta", "metrics_summary"],
        "only the kinds with no values dataset may be skipped"
    );
    assert_eq!(checked, vectors.len() - skipped.len());
    deviations
}

/// Scenario: every golden vector rebuilt as OTLP, converted to OTAP by pdata and
/// run through the real extraction path.
/// Guarantees: the converted representation produces exactly the `series_id` the
/// independent Python generator recorded, for every vector, so conversion is
/// identity preserving (spec sections 4 and 9.1).
#[test]
fn all_golden_vectors_survive_otlp_to_otap_conversion() {
    assert_eq!(
        deviating_vectors(),
        Vec::<String>::new(),
        "every golden vector must survive the OTLP to OTAP conversion"
    );
}
