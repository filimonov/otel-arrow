// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration adapter tests for the series Parquet exporter.

use super::config::Config;
use super::{Failure, write_request};
use otel_arrow_dfe_engine::control::NackCause;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::encode::{encode_logs_otap_batch, encode_metrics_otap_batch};
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
};
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otel_arrow_dfe_series_lake as lake;
use std::path::Path;
use std::sync::Arc;

/// Scenario: the full adapter maps byte strings and rejects an impossible lake
/// budget.
/// Guarantees: startup validates the same constraints as the public core API,
/// and `window.max_block_bytes` written as `64MiB` reaches
/// `lake.ingress.max_block_bytes` as the exact byte count.
#[test]
fn configuration_maps_and_validates() {
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "15s", "max_block_bytes": "64MiB"},
        "parquet": {"compression": "zstd"}
    }))
    .expect("valid config");
    assert_eq!(cfg.lake.ingress.max_block_bytes, 64 << 20);
    // `upload.part_bytes` below the 5 MiB S3 multipart minimum is refused by
    // `LakeConfig::validate`, which this adapter must call.
    assert!(
        serde_json::from_value::<Config>(serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-test"}},
            "upload": {"part_bytes": "1MiB"}
        }))
        .is_err()
    );
}

/// Scenario: a user names a block-level budget inside `ingress`, where the
/// adapter instead takes it from `window`.
/// Guarantees: the setting is refused rather than silently ignored, so a
/// pipeline never runs with a budget the user believes they set.
#[test]
fn a_block_budget_written_under_ingress_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "ingress": {"max_block_bytes": "64MiB"}
    }))
    .expect_err("max_block_bytes does not belong to ingress");
    assert!(
        err.to_string().contains("unknown ingress setting"),
        "unexpected error: {err}"
    );
}

/// Scenario: `parquet.compression` names a codec the sink does not write.
/// Guarantees: the configuration is refused instead of writing zstd while the
/// document claims another codec.
#[test]
fn a_parquet_compression_other_than_zstd_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "parquet": {"compression": "snappy"}
    }))
    .expect_err("only zstd is written");
    assert!(
        err.to_string().contains("must be zstd"),
        "unexpected error: {err}"
    );
}

/// Scenario: `window.interval` is a sub-second duration.
/// Guarantees: the adapter refuses it, matching the whole-second rule the
/// window arithmetic and the `window_secs` file metadata both rely on.
#[test]
fn a_sub_second_window_interval_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "500ms"}
    }))
    .expect_err("sub-second interval");
    assert!(
        err.to_string().contains("whole seconds"),
        "unexpected error: {err}"
    );
}

/// Scenario: the example pipeline configuration shipped in `configs/` is fed
/// to the same validator the engine uses at startup.
/// Guarantees: the documented example stays loadable, so the manual
/// try-it-out instructions in the module README cannot silently rot.
#[test]
fn the_shipped_example_configuration_is_valid() {
    let yaml = include_str!("../../../../../configs/series-parquet-local.yaml");
    let doc: serde_json::Value = serde_yaml::from_str(yaml).expect("example config parses");
    let exporter = doc
        .pointer("/groups/default/pipelines/main/nodes/exporter/config")
        .expect("example config has an exporter node");
    let cfg: Config = serde_json::from_value(exporter.clone()).expect("example config is valid");
    assert_eq!(cfg.lake.writer_id, "local-1");
}

fn encoded<M: prost::Message>(message: &M) -> Vec<u8> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes).expect("encodes");
    bytes
}

fn logs_payload() -> OtapPayload {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_789_960_500_000_000_000,
                    event_name: "ready".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawLogsData::try_new(&bytes).expect("valid logs bytes");
    OtapPayload::from(encode_logs_otap_batch(&view).expect("encodes to OTAP"))
}

fn metrics_payload() -> OtapPayload {
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests".to_owned(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_789_960_500_000_000_000,
                            value: Some(number_data_point::Value::AsInt(1)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawMetricsData::try_new(&bytes).expect("valid metrics bytes");
    OtapPayload::from(encode_metrics_otap_batch(&view).expect("encodes to OTAP"))
}

fn config_for(base: &Path) -> Config {
    serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": base.to_string_lossy()}},
        "window": {"interval": "15s"}
    }))
    .expect("valid config")
}

fn sink_for(cfg: &Config, root: &Path) -> lake::sink::Sink {
    let store = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(root).expect("local object store"),
    );
    lake::sink::Sink::new(
        store,
        cfg.lake.clone(),
        lake::sink::FileNaming::new(&cfg.lake.writer_id),
    )
}

/// Scenario: a metrics request reaches an exporter that only supports logs,
/// and a logs request larger than `ingress.max_request_bytes` arrives.
/// Guarantees: both are classified `Permanent`, because validation judges the
/// request's own content and the identical bytes would be refused again.
#[tokio::test]
async fn validation_refusals_are_permanent() {
    let dir = tempfile::tempdir().expect("temp dir");
    let cfg = config_for(dir.path());
    let sink = sink_for(&cfg, dir.path());
    let mut cache = lake::cache::SeriesCache::new(cfg.cache_entries);
    let mut seq = 0_u64;
    let wall = lake::clock::SystemWallClock;

    let failure = write_request(metrics_payload(), &cfg, &sink, &mut cache, &mut seq, &wall)
        .await
        .expect_err("metrics are not supported");
    assert!(
        matches!(failure, Failure::Permanent(_)),
        "unsupported signal must be permanent, got {failure:?}"
    );

    let mut tiny = config_for(dir.path());
    tiny.lake.ingress.max_request_bytes = 1;
    let failure = write_request(logs_payload(), &tiny, &sink, &mut cache, &mut seq, &wall)
        .await
        .expect_err("request exceeds its budget");
    assert!(
        matches!(failure, Failure::Permanent(_)),
        "an over-budget request must be permanent, got {failure:?}"
    );
}

/// Scenario: a well-formed logs request passes validation, but the object
/// store cannot be written, because the directory the sink was rooted at has
/// been replaced by a regular file.
/// Guarantees: the storage failure is classified `Retryable`, never as a
/// client refusal, so an unreachable or full destination does not tell the
/// sender to change a request that is perfectly valid. Replacing the root with
/// a file is used rather than dropping its write permission because no user,
/// including root, can create a path below a regular file, so the failure is
/// deterministic everywhere the tests run.
#[tokio::test]
async fn a_storage_failure_after_validation_is_retryable() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("lake");
    std::fs::create_dir(&root).expect("create the lake root");
    let cfg = config_for(&root);
    let sink = sink_for(&cfg, &root);
    let mut cache = lake::cache::SeriesCache::new(cfg.cache_entries);
    let mut seq = 0_u64;
    let wall = lake::clock::SystemWallClock;

    // The sink has already resolved its root, so swapping the directory for a
    // file breaks every write underneath it.
    std::fs::remove_dir_all(&root).expect("remove the lake root");
    std::fs::write(&root, b"not a directory").expect("put a file in its place");

    let failure = write_request(logs_payload(), &cfg, &sink, &mut cache, &mut seq, &wall)
        .await
        .expect_err("the destination is not writable");
    assert!(
        matches!(failure, Failure::Retryable(_)),
        "a storage failure must be retryable, got {failure:?}"
    );
}

/// Scenario: both failure classes are turned into the nack the engine routes.
/// Guarantees: a permanent failure carries `NackCause::Refused` and is marked
/// permanent, while a retryable one is neither, so a retry processor redelivers
/// exactly the requests that can still succeed.
#[test]
fn each_failure_class_maps_to_its_nack() {
    let data =
        || OtapPdata::new_default(OtapPayload::empty(otel_arrow_dfe_config::SignalType::Logs));

    let permanent = Failure::Permanent(lake::Error::Refused(lake::RefuseReason::RequestTooLarge))
        .into_nack(data());
    assert!(permanent.permanent);
    assert_eq!(permanent.cause, NackCause::Refused);

    let retryable = Failure::Retryable(lake::Error::invalid("flush failed")).into_nack(data());
    assert!(!retryable.permanent);
    assert_eq!(retryable.cause, NackCause::Unspecified);
}
