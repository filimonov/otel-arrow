// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The OTLP framing walk beside the OTLP-to-OTAP conversion it guards, on the
//! same logs, metrics and traces bodies, so their times compare directly.

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span};
use otel_arrow_dfe_pdata::views::otlp::bytes::validate::RepeatedSingular;
use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use prost::Message;

/// Records, points or spans per request.
const ITEMS: usize = 1000;

criterion_group!(benches, bench_framing);
criterion_main!(benches);

fn string(value: String) -> Option<AnyValue> {
    Some(AnyValue {
        value: Some(any_value::Value::StringValue(value)),
    })
}

fn attributes(i: usize) -> Vec<KeyValue> {
    (0..5)
        .map(|k| KeyValue {
            key: format!("attribute.{k}"),
            value: string(format!("value-{}", (i + k) % 17)),
        })
        .collect()
}

fn resource() -> Option<Resource> {
    Some(Resource {
        attributes: attributes(0),
        ..Default::default()
    })
}

/// Logs with a 1 KiB string body and five attributes per record.
fn logs() -> Vec<u8> {
    let body = "x".repeat(1024);
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: resource(),
            scope_logs: vec![ScopeLogs {
                log_records: (0..ITEMS)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_789_960_500_000_000_000 + i as u64,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: string(body.clone()),
                        attributes: attributes(i),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// Gauge points with five attributes each, ten points per metric.
fn metrics() -> Vec<u8> {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: resource(),
            scope_metrics: vec![ScopeMetrics {
                metrics: (0..ITEMS / 10)
                    .map(|m| Metric {
                        name: format!("metric.{m}"),
                        unit: "1".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: (0..10)
                                .map(|i| NumberDataPoint {
                                    attributes: attributes(i),
                                    time_unix_nano: 1_789_960_500_000_000_000,
                                    value: Some(number_data_point::Value::AsDouble(i as f64)),
                                    ..Default::default()
                                })
                                .collect(),
                        })),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

/// Spans with five attributes each.
fn traces() -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: resource(),
            scope_spans: vec![ScopeSpans {
                spans: (0..ITEMS)
                    .map(|i| Span {
                        trace_id: vec![(i % 251) as u8; 16],
                        span_id: vec![(i % 241) as u8; 8],
                        name: format!("span.{}", i % 13),
                        start_time_unix_nano: 1_789_960_500_000_000_000,
                        end_time_unix_nano: 1_789_960_500_000_001_000,
                        attributes: attributes(i),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn bench_framing(c: &mut Criterion) {
    for (name, signal, body) in [
        ("logs", SignalType::Logs, logs()),
        ("metrics", SignalType::Metrics, metrics()),
        ("traces", SignalType::Traces, traces()),
    ] {
        let payload = OtapPayload::from(OtlpProtoBytes::new_from_bytes(signal, body.clone()));
        let mut group = c.benchmark_group("otlp_framing");
        let _ = group.throughput(Throughput::Bytes(body.len() as u64));
        for (policy, repeated) in [
            ("accept", RepeatedSingular::Accept),
            ("refuse", RepeatedSingular::Refuse),
        ] {
            let _ = group.bench_with_input(
                BenchmarkId::new(format!("validate_{policy}"), name),
                &payload,
                |b, payload| {
                    b.iter(|| {
                        payload
                            .validate_otlp_framing(repeated)
                            .expect("well-formed")
                    })
                },
            );
        }
        let _ =
            group.bench_with_input(BenchmarkId::new("convert", name), &payload, |b, payload| {
                b.iter_batched(
                    || payload.clone(),
                    |payload| {
                        let records: OtapArrowRecords =
                            payload.try_into_with_default().expect("converts");
                        records
                    },
                    BatchSize::SmallInput,
                )
            });
        group.finish();
    }
}
