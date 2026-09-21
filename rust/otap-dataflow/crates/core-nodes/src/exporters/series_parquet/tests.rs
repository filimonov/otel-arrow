// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration adapter tests for the series Parquet exporter.

use super::config::Config;
use super::token::{AckToken, Notifier, Outcome};
use super::worker::{Failure, Worker};
use object_store::ObjectStoreExt;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::control::NackCause;
use otel_arrow_dfe_engine::control::{
    PipelineCompletionMsg, PipelineCompletionMsgReceiver, pipeline_completion_msg_channel,
};
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_runtime_services};
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
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
use std::sync::Arc;
use std::time::Duration;

/// Build an exporter effect handler wired to a completion channel of exactly
/// `capacity` slots, so a test can saturate it deterministically.
pub(super) fn effects(
    capacity: usize,
) -> (
    EffectHandler<OtapPdata>,
    PipelineCompletionMsgReceiver<OtapPdata>,
) {
    let (_rx, reporter) =
        otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);
    let mut effects = EffectHandler::new(
        test_node("series"),
        reporter,
        test_pipeline_runtime_services(),
    );
    let (tx, rx) = pipeline_completion_msg_channel(capacity);
    effects.set_pipeline_completion_msg_sender(tx);
    (effects, rx)
}

/// A payload-free request that still carries a routing frame, so the
/// completion it owes is actually routed rather than skipped.
pub(super) fn empty_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, OtapPayload::empty(SignalType::Logs))
}

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

/// Scenario: a metrics request reaches an exporter that only supports logs,
/// and a logs request larger than `ingress.max_request_bytes` arrives.
/// Guarantees: both are refused as permanent client errors with the rule that
/// rejected them, and neither leaves anything in the ACTIVE block, because
/// validation judges the request's own content and the identical bytes would
/// be refused again.
#[tokio::test(flavor = "current_thread")]
async fn validation_refusals_are_permanent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            let mut context = Context::default();
            context.set_source_node(7);
            worker.admit(OtapPdata::new(context, metrics_payload()));
            // The same logs request that is admitted elsewhere, now larger
            // than the budget it is measured against.
            worker.cfg.lake.ingress.max_request_bytes = 1;
            worker.admit(logs_pdata());

            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(!worker.rotation_requested);
            for reason in ["unsupported", "too_large"] {
                assert!(worker.notify.next().await.is_ok());
                match rx.recv().await.expect("a refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(nack.permanent);
                        assert_eq!(nack.cause, NackCause::Refused);
                        assert_eq!(nack.reason, reason);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
        })
        .await;
}

/// Scenario: a well-formed logs request is admitted and rotated, but the
/// object store cannot be written, because the directory the store was rooted
/// at has been replaced by a regular file.
/// Guarantees: every request of the block is nacked as retryable rather than
/// refused, and the descriptor is left uncommitted so the next block writes it
/// again. An unreachable or full destination must not tell the sender to
/// change a request that is perfectly valid. Replacing the root with a file is
/// used rather than dropping its write permission because no user, including
/// root, can create a path below a regular file, so the failure is
/// deterministic everywhere the tests run.
#[tokio::test(flavor = "current_thread")]
async fn a_storage_failure_after_validation_is_retryable() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("lake");
            std::fs::create_dir(&root).expect("create the lake root");
            let store = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&root)
                    .expect("local object store"),
            );
            // The store has already resolved its root, so swapping the
            // directory for a file breaks every write underneath it.
            std::fs::remove_dir_all(&root).expect("remove the lake root");
            std::fs::write(&root, b"not a directory").expect("put a file in its place");

            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            let partition = worker.active.data.partition;
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush task joins").result.is_err(),
                "the destination is not writable"
            );

            worker.complete(done);
            assert!(!worker.cache.is_committed(&id, partition));
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a storage failure") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.reason, "storage");
                }
                other => panic!("expected a nack, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a flush is still outstanding.
/// Guarantees: the write is cancelled and every completion the worker still
/// owns is delivered as a retryable `NodeShutdown` nack, so a request is never
/// dropped undecided along with the worker.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_decides_every_outstanding_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            let elapsed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(elapsed);
            worker.abandon();

            assert!(worker.is_idle());
            match rx.recv().await.expect("a shutdown refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                    assert_eq!(nack.reason, "shutdown");
                }
                other => panic!("expected a nack, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: both failure classes are turned into the outcome the notifier
/// delivers.
/// Guarantees: a size refusal and an unsupported signal keep their own
/// outcome, any other validation refusal is reported as invalid, and every
/// retryable failure becomes a storage outcome, so the phase a failure came
/// from still decides what the sender is told.
#[test]
fn each_failure_class_maps_to_its_outcome() {
    assert_eq!(
        Failure::Permanent(lake::Error::Refused(lake::RefuseReason::RequestTooLarge)).outcome(),
        Outcome::TooLarge
    );
    assert_eq!(
        Failure::Permanent(lake::Error::Refused(lake::RefuseReason::Unsupported(
            "signal".into()
        )))
        .outcome(),
        Outcome::Unsupported
    );
    assert_eq!(
        Failure::Permanent(lake::Error::invalid("undecodable pdata")).outcome(),
        Outcome::Invalid
    );
    assert_eq!(
        Failure::Retryable(lake::Error::invalid("flush failed")).outcome(),
        Outcome::Storage
    );
}

/// Scenario: the bounded engine completion channel fills while a second
/// notification waits.
/// Guarantees: cancelling a poll preserves the second context and sends it
/// exactly once later, so a request never loses its decision because the
/// exporter had to attend to something else.
#[tokio::test(flavor = "current_thread")]
async fn notification_survives_cancelled_poll() {
    let (handler, mut rx) = effects(1);
    let mut notify = Notifier::new(handler, 2);

    // The first completion is handed over before the second is queued, so the
    // notifier never holds more normal completions than its cap allows.
    let (first, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push(first, Outcome::Ack);
    assert!(notify.next().await.is_ok());

    // The completion channel holds one message and nothing has read it, so
    // this second send cannot make progress.
    let (second, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push(second, Outcome::Ack);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), notify.next())
            .await
            .is_err()
    );
    assert_eq!(notify.len(), 1);

    assert!(matches!(
        rx.recv().await.expect("first completion"),
        PipelineCompletionMsg::DeliverAck { .. }
    ));
    assert!(notify.next().await.is_ok());
    assert!(matches!(
        rx.recv().await.expect("second completion"),
        PipelineCompletionMsg::DeliverAck { .. }
    ));
    assert_eq!(notify.len(), 0);
}

/// A worker configuration with a one-second window and a small request bound,
/// so the completion credit arithmetic is exercised at a size a test can
/// reason about. The base URI is never used: these tests hand the worker an
/// in-memory object store directly.
fn worker_config() -> Config {
    serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-unused"}},
        "window": {"interval": "1s", "max_requests_per_block": 4}
    }))
    .expect("valid config")
}

/// One well-formed logs request that still carries a routing frame.
fn logs_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, logs_payload())
}

/// Scenario: a request is admitted, rotated into a real in-memory object store
/// flush, and completed.
/// Guarantees: no completion is emitted before the flush resolved, both files
/// exist in the store by the time the ack is queued, and only a completed
/// flush marks the descriptor committed in the cache under the flushed
/// block's partition.
#[tokio::test(flavor = "current_thread")]
async fn complete_files_before_ack() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            let partition = worker.active.data.partition;
            assert!(!worker.cache.is_committed(&id, partition));

            worker.rotate();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), rx.recv())
                    .await
                    .is_err(),
                "a started flush is not a durable one"
            );

            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let report = done
                .as_ref()
                .expect("the flush task joins")
                .result
                .as_ref()
                .expect("the write succeeds");
            assert_eq!(report.files.len(), 2);
            for (_, path, _) in &report.files {
                assert!(store.head(path).await.is_ok());
            }

            worker.complete(done);
            assert!(worker.cache.is_committed(&id, partition));
            assert!(worker.notify.next().await.is_ok());
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
        })
        .await;
}
