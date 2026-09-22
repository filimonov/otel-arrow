// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration adapter tests for the series Parquet exporter.

use super::config::Config;
use super::token::{AckToken, Notifier, Outcome};
use super::worker::{Failure, Prepared, Worker};
use object_store::ObjectStoreExt;
use otel_arrow_dfe_channel::mpsc;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::Interests;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::control::NackCause;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::control::{
    PipelineCompletionMsg, PipelineCompletionMsgReceiver, pipeline_completion_msg_channel,
};
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_engine::local::message::LocalReceiver;
use otel_arrow_dfe_engine::message::{ExporterInbox, Receiver};
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

/// Scenario: two requests are buffered and a third is sent after the node has
/// already latched shutdown and refused the first two, with the upstream
/// sender still alive throughout.
/// Guarantees: every one of them is decided with a retryable `NodeShutdown`
/// nack and the node returns its terminal state only after the upstream
/// channel closes, so a node that has latched shutdown never returns while
/// requests it could still be handed are outstanding.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_decides_every_force_drained_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(8);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);

            // The engine latches this and releases it only once the upstream
            // pdata channel is both empty and closed, so the two requests
            // below are force-drained before the node ever sees it.
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            for _ in 0..2 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a buffered request enqueues");
            }

            let node = tokio::task::spawn_local(super::run(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
            ));

            for _ in 0..2 {
                expect_shutdown_nack(&mut rx).await;
            }

            // The node has refused everything it was handed and is idle, but
            // the sender is still alive, so this request must still be
            // decided rather than dropped with the inbox.
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("a late request enqueues");
            expect_shutdown_nack(&mut rx).await;

            // Closing the upstream pdata channel is what releases the latched
            // shutdown; the control sender stays alive, as it does in the
            // engine.
            drop(pdata_tx);
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns once the upstream channel closes")
                .expect("the node task joins");
            if let Err(error) = terminal {
                panic!("unexpected node failure: {error}");
            }
            drop(control_tx);
        })
        .await;
}

/// Take one completion and assert it is a retryable shutdown refusal.
async fn expect_shutdown_nack(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a completion arrives")
        .expect("a completion arrives")
    {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_eq!(nack.cause, NackCause::NodeShutdown);
            assert_eq!(nack.reason, "shutdown");
        }
        other => panic!("expected a nack, got {other:?}"),
    }
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

/// A worker configuration whose blocks hold at most `requests` requests, so a
/// third request has to wait for the next block.
fn worker_config_with_requests(requests: usize) -> Config {
    let mut cfg = worker_config();
    cfg.window.max_requests_per_block = requests;
    cfg.lake.ingress.max_requests_per_block = requests;
    cfg
}

/// Scenario: two requests fill a two-request block and a third request needs
/// the next one.
/// Guarantees: exactly one extracted request is parked, admission closes while
/// it waits, and it enters the next block before anything newer, so a request
/// that could not be reserved is neither dropped nor reordered behind later
/// input and the worker still holds no more than two blocks and one request.
#[tokio::test(flavor = "current_thread")]
async fn one_pending_request_resumes_before_new_input() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config_with_requests(2), store, wall, handler);

            worker.admit(logs_pdata());
            worker.admit(logs_pdata());
            worker.admit(logs_pdata());

            assert_eq!(worker.active.tokens.len(), 2);
            assert!(worker.pending.is_some());
            assert!(!worker.accept());

            worker.rotate();
            worker.resume_pending();
            assert!(worker.pending.is_none());
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(worker.live_tokens(), 3);
            assert!(worker.active.data.bytes <= worker.cfg.window.max_block_bytes);
        })
        .await;
}

/// Scenario: a request whose logical size exceeds the input budget is offered
/// to an empty ACTIVE block.
/// Guarantees: nothing is admitted, no request is parked and the sender gets a
/// permanent refusal, because a request that cannot fit an empty block would
/// be refused by every following block as well.
#[tokio::test(flavor = "current_thread")]
async fn oversized_input_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.ingress.max_request_bytes = 1;
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert_eq!(worker.active.data.bytes, 0);
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: preparation consumes Arrow input whose original array is still
/// weakly observed from outside the worker.
/// Guarantees: the parked extraction retains no original input array and no
/// conversion record batch, so parking one request cannot keep a whole
/// request's Arrow buffers resident beside the two blocks.
#[tokio::test(flavor = "current_thread")]
async fn prepare_releases_original_arrow_payload() {
    use otel_arrow_dfe_pdata::TryIntoWithOptions;
    use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    let (context, payload) = logs_pdata().into_parts();
    let records: OtapArrowRecords = payload.try_into_with_default().expect("records");
    let weak = Arc::downgrade(
        records
            .get(ArrowPayloadType::Logs)
            .expect("logs batch")
            .column(0),
    );
    let store = Arc::new(object_store::memory::InMemory::new());
    let (handler, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(worker_config(), store, wall, handler);

    let prepared = worker.prepare(OtapPdata::new(context, records.into()));
    assert!(matches!(prepared, Prepared::Ready(_)));
    assert!(
        weak.upgrade().is_none(),
        "the prepared output cannot pin the input arrays"
    );
}

/// Scenario: a well-formed request is converted and then fails the extraction
/// budget, which is measured only after the conversion has run.
/// Guarantees: the failure is a permanent refusal, nothing is parked and the
/// ACTIVE block is left untouched, because every validation phase completes
/// before the block is reserved against.
#[tokio::test(flavor = "current_thread")]
async fn extraction_failure_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            // Large enough to pass the wire-size check, so the refusal can
            // only come from the measured extracted output.
            cfg.lake.ingress.max_extracted_bytes = 1;
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                }
                other => panic!("expected an extraction refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: an OTLP protobuf body whose top-level framing is truncated
/// reaches preparation after its byte-size check.
/// Guarantees: it is refused permanently and the ACTIVE block is untouched.
/// The shared byte views decode lazily and report no error for such a body, so
/// the exporter checks the framing itself: without that a damaged request
/// would convert to a request carrying no rows and be acknowledged as stored.
#[tokio::test(flavor = "current_thread")]
async fn a_malformed_otlp_body_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _) = logs_pdata().into_parts();
            // Field 1 (`resource_logs`), length-delimited, declaring 127 bytes
            // that the buffer does not contain.
            let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(
                bytes::Bytes::from_static(&[0x0A, 0x7F]),
            );
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            worker.admit(OtapPdata::new(context, payload.into()));
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert_eq!(nack.reason, "invalid");
                }
                other => panic!("expected a framing refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: the wall clock steps backwards between parking a request for a
/// later window and opening the block that request is waiting for.
/// Guarantees: one rotation admits the parked request, which is not parked
/// again. The block a parked request is opened for takes its window from that
/// request, so a clock that steps back cannot make the node spin opening empty
/// blocks the parked request is forever too late for.
#[tokio::test(flavor = "current_thread")]
async fn a_backward_clock_step_does_not_repark_the_pending_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(8);
            // One-second windows, so the block opened at 100 s ends well
            // before the request prepared at 200 s.
            let wall = Arc::new(lake::clock::TestWallClock::new(100 * 1_000_000_000));
            let mut worker = Worker::new(worker_config(), store, Arc::clone(&wall) as _, handler);
            assert_eq!(worker.active.data.window_start_secs, 100);

            wall.set(200 * 1_000_000_000);
            worker.admit(logs_pdata());
            assert!(
                worker.pending.is_some(),
                "a request past the block's window waits for the next block"
            );
            assert!(worker.active.tokens.is_empty());

            // The clock now steps back behind both the parked request and the
            // block that is about to be replaced.
            wall.set(50 * 1_000_000_000);
            worker.rotate();
            worker.resume_pending();

            assert!(worker.pending.is_none(), "one rotation is enough");
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(worker.active.data.window_start_secs, 200);
        })
        .await;
}

/// Scenario: a request whose reservation alone exceeds the block budget is
/// offered to an empty ACTIVE block.
/// Guarantees: `Block::reserve` refuses it as `RequestTooLarge`, the worker
/// reports that permanently rather than parking it, and the block is
/// untouched. An empty block is the largest one the request will ever be
/// offered, so parking it would rotate for ever without admitting it.
#[tokio::test(flavor = "current_thread")]
async fn a_request_too_large_for_an_empty_block_is_refused_not_parked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            // The per-request and extraction budgets stay wide open, so the
            // refusal can only come from the block budget inside `reserve`.
            cfg.window.max_block_bytes = 1;
            cfg.lake.ingress.max_block_bytes = 1;
            let worker = Worker::new(
                cfg.clone(),
                store.clone(),
                Arc::clone(&wall) as _,
                effects(1).0,
            );
            assert!(
                matches!(worker.prepare(logs_pdata()), Prepared::Ready(_)),
                "preparation must succeed, or the refusal would not be the block budget"
            );
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(
                worker.pending.is_none(),
                "an empty block cannot take it, and no later block will either"
            );

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert_eq!(nack.reason, "too_large");
                }
                other => panic!("expected a block-budget refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: a request is parked by the running node, and a newer, distinct
/// request is sent while it waits.
/// Guarantees: the parked request reaches storage before the newer one, and
/// the newer one is not taken off the channel while a request is parked, so
/// backpressure is real rather than a third block. Each request carries its
/// own source node, so the completions say which request was decided first.
#[tokio::test(flavor = "current_thread")]
async fn the_parked_request_is_stored_before_a_newer_one() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(8);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));

            let node = tokio::task::spawn_local(super::run(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                inbox,
                handler,
            ));

            // The first request opens and seals the first block, leaving an
            // empty block aligned to the window that starts at zero.
            pdata_tx
                .send_async(logs_pdata_from(1))
                .await
                .expect("the first request enqueues");
            assert_eq!(expect_ack(&mut rx).await, Some(1));

            // Both of the next two belong to a later window, so the first of
            // them cannot join the block in hand and is parked.
            wall.set(5 * 1_000_000_000);
            pdata_tx
                .send_async(logs_pdata_from(2))
                .await
                .expect("the parked request enqueues");
            pdata_tx
                .send_async(logs_pdata_from(3))
                .await
                .expect("the newer request enqueues");

            assert_eq!(expect_ack(&mut rx).await, Some(2));
            assert_eq!(expect_ack(&mut rx).await, Some(3));

            drop(pdata_tx);
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins");
            if let Err(error) = terminal {
                panic!("unexpected node failure: {error}");
            }
            drop(control_tx);
        })
        .await;
}

/// Take one completion, require it to be an ack, and say which request it
/// belonged to.
async fn expect_ack(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) -> Option<usize> {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a completion arrives")
        .expect("a completion arrives")
    {
        PipelineCompletionMsg::DeliverAck { ack } => ack.accepted.into_parts().0.source_node(),
        other => panic!("expected an ack, got {other:?}"),
    }
}

/// One well-formed logs request carrying `source` as its routing frame, so a
/// completion can be traced back to the request that earned it.
fn logs_pdata_from(source: usize) -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(source);
    OtapPdata::new(context, logs_payload())
}
