// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration adapter tests for the series Parquet exporter.

use super::config::Config;
use super::token::{AckToken, Notifier, Outcome};
use super::worker::{Failure, Prepared, Worker};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};
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
use otel_arrow_dfe_engine::local::exporter::Exporter;
use otel_arrow_dfe_engine::local::message::LocalReceiver;
use otel_arrow_dfe_engine::message::{ExporterInbox, Message, Receiver};
use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_runtime_services};
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::encode::{encode_logs_otap_batch, encode_metrics_otap_batch};
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
    metric, number_data_point,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span};
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;
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

/// One well-formed OTLP traces request, kept in its wire form.
///
/// Traces are refused on the signal alone, before any conversion, so the
/// request never has to be encoded into OTAP records.
fn traces_payload() -> OtapPayload {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    name: "unsupported".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    otel_arrow_dfe_pdata::OtlpProtoBytes::ExportTracesRequest(bytes::Bytes::from(encoded(&request)))
        .into()
}

/// One metrics request carrying a supported gauge point and an unsupported
/// summary point, which is what the `unsupported` policy decides.
fn mixed_metrics_payload() -> OtapPayload {
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: "requests".to_owned(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_789_960_500_000_000_000,
                                value: Some(number_data_point::Value::AsInt(1)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "latency".to_owned(),
                        data: Some(metric::Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                time_unix_nano: 1_789_960_500_000_000_000,
                                count: 1,
                                sum: 2.0,
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawMetricsData::try_new(&bytes).expect("valid metrics bytes");
    OtapPayload::from(encode_metrics_otap_batch(&view).expect("encodes to OTAP"))
}

/// One mixed metrics request that still carries a routing frame.
fn mixed_metrics_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, mixed_metrics_payload())
}

/// Scenario: a traces request reaches an exporter that has no traces schema,
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
            worker.admit(OtapPdata::new(context, traces_payload()));
            // The same logs request that is admitted elsewhere, now larger
            // than the budget it is measured against.
            worker.cfg.lake.ingress.max_request_bytes = 1;
            worker.admit(logs_pdata());

            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(!worker.rotation_requested);
            // The reason is the sentence the producer sees: it names the
            // rule, the limit and the remedy, not a bare metric token.
            for reason in [
                "traces are not stored by series_parquet",
                "exceeds ingress.max_request_bytes (1 bytes); split the batch upstream",
            ] {
                assert!(worker.notify.next().await.is_ok());
                match rx.recv().await.expect("a refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(nack.permanent);
                        assert_eq!(nack.cause, NackCause::Refused);
                        assert!(nack.reason.contains(reason), "reason: {}", nack.reason);
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
            // A broken destination is retried until the block's absolute
            // deadline, so the test gives it one it can reach on a simulated
            // clock rather than waiting out the configured default.
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(1);
            let mut worker = Worker::new(cfg, store, wall, handler);

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
            let ticker = ticking(&sim, Duration::from_millis(50));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
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
                    assert!(nack.reason.contains("retry"), "reason: {}", nack.reason);
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

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let config = worker_config();
            let node = tokio::task::spawn_local(super::run(
                config.clone(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(super::metrics::Metrics::register(&context, &config.lake)),
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
            let terminal = match terminal {
                Ok(terminal) => terminal,
                Err(error) => panic!("unexpected node failure: {error}"),
            };
            // The counters of the interval the node ends must reach the
            // collector with it: nothing else will ever report them.
            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("reason", "shutdown")]),
                3,
                "every force-drained request is counted as a shutdown refusal"
            );
            assert_eq!(
                terminal_value(snapshots, "notify.failures", &[]),
                0,
                "the completion channel took all three immediately"
            );
            assert_eq!(terminal_value(snapshots, "acks", &[]), 0);
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
            assert_eq!(nack.reason, Outcome::Shutdown.sentence());
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
            worker.abandon().await;

            assert!(worker.is_idle());
            match rx.recv().await.expect("a shutdown refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                    assert_eq!(nack.reason, Outcome::Shutdown.sentence());
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
        Failure::Retryable(lake::Error::ObjectStore(object_store::Error::Generic {
            store: "test",
            source: "unreachable".into(),
        }))
        .outcome(),
        Outcome::Storage
    );
    assert_eq!(
        Failure::Retryable(lake::Error::internal("flush failed")).outcome(),
        Outcome::Internal
    );
}

/// Scenario: extraction fails on a writer invariant -- the column/builder
/// mismatch a writer bug produces -- rather than on the request's content,
/// and the failure is classified by the rule the admission path uses and then
/// delivered.
/// Guarantees: only a lake refusal is permanent. The internal error becomes a
/// retryable nack labelled `internal`, and its reason is a sentence carrying
/// the sanitized detail, so a producer never drops data because of an
/// exporter bug and an operator can still see what went wrong.
#[tokio::test(flavor = "current_thread")]
async fn an_internal_extraction_error_is_a_retryable_nack_with_detail() {
    let cfg = worker_config();
    let failure = Failure::classify(lake::Error::internal(
        "column/builder mismatch for Str(None)\nsecond line",
    ));
    assert!(matches!(failure, Failure::Retryable(_)));
    assert_eq!(failure.outcome(), Outcome::Internal);
    assert!(!failure.outcome().refused());
    let sentence = failure.sentence(super::worker::Stage::Extract, &cfg);
    assert!(
        sentence.contains("column/builder mismatch for Str(None) second line"),
        "the detail is kept, on one line: {sentence}"
    );
    assert!(
        Failure::classify(lake::Error::invalid("bad"))
            .outcome()
            .refused()
    );

    let (handler, mut rx) = effects(1);
    let mut notify = Notifier::new(handler, 4);
    let (token, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push_with(token, failure.outcome(), Some(sentence.clone().into()));
    notify.next().await.expect("the completion is accepted");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_ne!(nack.cause, NackCause::Refused);
            assert_eq!(nack.reason, sentence);
        }
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_eq!(notify.outcomes()[Outcome::Internal as usize], 1);
}

/// Scenario: an error detail longer than the reason bound, and one holding
/// control characters and multi-byte characters at the cut.
/// Guarantees: the detail a nack reason carries is bounded, single-line and
/// cut on a character boundary, so request-derived text cannot make a status
/// message unbounded or split a character.
#[test]
fn a_reason_detail_is_bounded_and_single_line() {
    let long = "x".repeat(10_000);
    let cut = super::token::sanitized(&long);
    assert!(cut.len() <= 256 + 3);
    assert!(cut.ends_with("..."));
    let wide = "\u{e9}".repeat(1_000);
    let cut = super::token::sanitized(&wide);
    assert!(cut.len() <= 256 + 3);
    assert_eq!(super::token::sanitized("a\r\nb\tc"), "a  b c");
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
                    assert!(
                        nack.reason
                            .starts_with("invalid request content: malformed OTLP"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a framing refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: a well-formed OTLP metrics request reaches the same worker that
/// admits logs.
/// Guarantees: it is admitted through one extraction into the ACTIVE block and
/// holds its completion there, so metrics travel the single admission state
/// machine rather than a signal-specific path.
#[tokio::test(flavor = "current_thread")]
async fn metrics_are_admitted_like_logs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            let mut context = Context::default();
            context.set_source_node(7);
            worker.admit(OtapPdata::new(context, metrics_payload()));

            assert!(!worker.active.data.is_empty());
            assert_eq!(worker.active.tokens.len(), 1);
            assert!(worker.pending.is_none());
            assert_eq!(worker.notify.len(), 0);
        })
        .await;
}

/// Scenario: an OTLP metrics body whose top-level framing is truncated reaches
/// preparation after its byte-size check.
/// Guarantees: it is refused permanently and the ACTIVE block is untouched.
/// The framing walk is per signal, so admitting metrics must not let a damaged
/// metrics body through the check that already covers logs.
#[tokio::test(flavor = "current_thread")]
async fn a_malformed_otlp_metrics_body_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let mut context = Context::default();
            context.set_source_node(7);
            // Field 1 (`resource_metrics`), length-delimited, declaring 127
            // bytes that the buffer does not contain.
            let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportMetricsRequest(
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
                    assert!(
                        nack.reason
                            .starts_with("invalid request content: malformed OTLP"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a framing refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: one metrics request carries a supported gauge point next to an
/// unsupported summary point, under the default `unsupported: reject`.
/// Guarantees: the whole request is refused as one permanent `unsupported`
/// nack and the ACTIVE block keeps the bytes and the request count it had, so
/// the policy is applied atomically and the supported half of a rejected
/// request is never stored. Nothing is parked, because the refusal judges the
/// request's own content rather than whichever block happened to be active.
#[tokio::test(flavor = "current_thread")]
async fn a_mixed_metrics_request_is_rejected_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let cfg = worker_config();
            assert_eq!(
                cfg.lake.unsupported,
                lake::config::UnsupportedPolicy::Reject
            );
            let mut worker = Worker::new(cfg, store, wall, handler);

            // A request admitted first, so the assertion is that the rejected
            // request left a non-empty block exactly as it found it rather
            // than that an empty block stayed empty.
            worker.admit(logs_pdata());
            let bytes = worker.active.data.bytes;
            let requests = worker.active.tokens.len();
            assert_eq!(requests, 1);

            worker.admit(mixed_metrics_pdata());
            assert_eq!(worker.active.data.bytes, bytes);
            assert_eq!(worker.active.tokens.len(), requests);
            assert!(worker.pending.is_none());
            assert_eq!(worker.live_tokens(), requests + 1);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(
                        nack.reason.contains("set unsupported: drop"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected an unsupported refusal, got {other:?}"),
            }
            // Exactly one completion, and it belongs to the rejected request:
            // the admitted one is still owed by the ACTIVE block.
            assert_eq!(worker.live_tokens(), requests);
        })
        .await;
}

/// Scenario: the same mixed metrics request arrives under
/// `unsupported: drop`, and the block it lands in is rotated and written.
/// Guarantees: the gauge point is admitted, the summary point is counted as
/// dropped rather than stored, and the request is acknowledged only once its
/// block has been written, so a dropped point does not make the request ack
/// early or fail.
#[tokio::test(flavor = "current_thread")]
async fn a_mixed_metrics_request_drops_only_the_unsupported_points() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.unsupported = lake::config::UnsupportedPolicy::Drop;
            let mut worker = Worker::new(cfg, store, wall, handler);

            // Prepared rather than admitted in one step, because the drop
            // counters live in the extraction and are consumed by admission.
            let Prepared::Ready(pending) = worker.prepare(mixed_metrics_pdata()) else {
                panic!("the drop policy admits the supported points");
            };
            assert_eq!(pending.extracted.stats.dropped_unsupported, 1);
            assert_eq!(pending.extracted.stats.rows, 1);
            worker.offer(pending);
            assert_eq!(worker.active.tokens.len(), 1);
            assert!(!worker.active.data.is_empty());
            assert!(worker.pending.is_none());

            worker.rotate();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), rx.recv())
                    .await
                    .is_err(),
                "a dropped point does not acknowledge the request early"
            );
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert!(worker.notify.next().await.is_ok());
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
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
                    assert!(
                        nack.reason.contains("window.max_block_bytes (1 bytes)"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a block-budget refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: three requests reach the running node; the block takes two of
/// them and the third has to wait for the next one. Both clocks are
/// simulated, so the window boundary that seals the second block is reached
/// by advancing them rather than by waiting.
/// Guarantees: the parked request reaches storage before the newer one, and
/// the newer one is not taken off the channel while a request is parked, so
/// backpressure is real rather than a third block. Each request carries its
/// own source node, so the completions say which request was decided first.
#[tokio::test(flavor = "current_thread")]
async fn the_parked_request_is_stored_before_a_newer_one() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
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

            // A block reserves room for two requests but is only rotated by
            // the window or a third one, so the third request is parked
            // rather than left on the channel.
            let mut cfg = worker_config();
            cfg.window.max_requests_per_block = 3;
            cfg.lake.ingress.max_requests_per_block = 2;
            let node = tokio::task::spawn_local(super::run(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            // The first two fill the room the block reserves; the third
            // cannot be reserved against it and waits for the next block.
            for source in 1..=3 {
                pdata_tx
                    .send_async(logs_pdata_from(source))
                    .await
                    .expect("a request enqueues");
            }

            // Parking the third request asks for a rotation, so the first
            // block is written without any boundary being reached and the
            // parked request enters the block that replaces it.
            for source in 1..=2 {
                assert_eq!(expect_ack(&mut rx).await, Some(source));
            }

            // Those acks are delivered after the rotation that admitted the
            // third request, so the block it sits in already exists. Its
            // window is reached by moving both clocks explicitly.
            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
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

/// Scenario: a request joins the ACTIVE block of a fifteen-second window and
/// the wall clock then steps back by an hour, far more than one interval,
/// while the engine's monotonic clock keeps running.
/// Guarantees: the block is still rotated within one interval of monotonic
/// time since the window started, instead of waiting out the hour the wall
/// clock now claims is left; the block that replaces it keeps the floored
/// window start and re-emits its descriptors. A backward step therefore
/// delays acknowledgements by at most one interval and never reopens or
/// reorders a window.
#[tokio::test(flavor = "current_thread")]
async fn a_backward_clock_step_rotates_within_one_monotonic_interval() {
    use futures::FutureExt;

    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(100_000 * 1_000_000_000));
            let mut cfg = worker_config();
            cfg.window.interval = Duration::from_secs(15);
            cfg.lake.window_interval = Duration::from_secs(15);
            let mut worker = Worker::new(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                handler,
            );
            assert_eq!(worker.active.data.window_start_secs, 99_990);
            worker.admit(logs_pdata());
            assert_eq!(worker.active.tokens.len(), 1);

            wall.set((100_000 - 3600) * 1_000_000_000);
            let mut rotated_after = None;
            for elapsed in 1..=3600_u64 {
                sim.advance(Duration::from_secs(1));
                tokio::task::yield_now().await;
                if worker.window.sleep.as_mut().now_or_never().is_some() {
                    worker.wake_window();
                    if worker.rotation_requested {
                        rotated_after = Some(elapsed);
                        break;
                    }
                }
            }
            let rotated_after = rotated_after.expect("the block is rotated");
            assert!(
                rotated_after <= 15,
                "rotated only after {rotated_after} s of monotonic time"
            );
            worker.rotate();
            assert!(worker.flushing.is_some(), "the block is written");
            assert_eq!(
                worker.active.data.window_start_secs, 99_990,
                "the window start never moves backwards"
            );
            assert!(worker.active.reemit, "the same window re-emits descriptors");
        })
        .await;
}

/// Scenario: the wall clock runs half a second slow over one fifteen-second
/// window, so the monotonic interval ends just before the wall clock reaches
/// the boundary.
/// Guarantees: that shortfall is treated as drift, not as a backward step: no
/// early rotation with the old window start happens, and the ordinary
/// boundary rotation follows once the wall clock arrives, so a slewing clock
/// does not add a file set per window.
#[tokio::test(flavor = "current_thread")]
async fn wall_clock_drift_is_not_mistaken_for_a_backward_step() {
    use futures::FutureExt;

    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let wall = Arc::new(lake::clock::TestWallClock::new(99_990 * 1_000_000_000));
    let mut window = super::window::Window::new(Duration::from_secs(15), Arc::clone(&wall) as _);

    sim.advance(Duration::from_secs(15));
    wall.set(100_004_500_000_000);
    tokio::task::yield_now().await;
    assert!(
        window.sleep.as_mut().now_or_never().is_some(),
        "the monotonic bound fires"
    );
    assert!(!window.wake(), "half a second short is drift, not a step");
    assert!(!window.floored);

    sim.advance(Duration::from_millis(500));
    wall.set(100_005 * 1_000_000_000);
    tokio::task::yield_now().await;
    assert!(
        window.sleep.as_mut().now_or_never().is_some(),
        "the drift is slept off"
    );
    assert!(window.wake(), "the boundary itself rotates");
    assert!(
        !window.floored,
        "a boundary rotation moves the window start"
    );
    assert_eq!(window.clock.last_boundary(), 100_005);
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

/// Scenario: a boundary sleep fires after the wall clock has jumped forward
/// past several windows, and afterwards the wall clock steps backwards.
/// Guarantees: the missed windows coalesce into one rotation at the latest
/// boundary, the sleep is re-armed for the window that follows it, and the
/// backward step neither re-fires the boundary that was already consumed nor
/// leaves the node without an armed sleep.
#[tokio::test(flavor = "current_thread")]
async fn busy_rotation_rearms_boundary_sleep() {
    let sim = clock::SimClock::new();
    let _clock_guard = sim.install();
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut window = super::window::Window::new(Duration::from_secs(15), wall.clone());

    // Four windows pass while the node is busy; one sleep covers them all.
    wall.set(65_000_000_000);
    sim.advance(Duration::from_secs(15));
    window.sleep.as_mut().await;
    assert!(window.wake(), "a crossed boundary asks for a rotation");
    assert_eq!(window.clock.last_boundary(), 60);
    assert_eq!(window.clock.next_boundary(65), 75);
    assert!(
        futures::poll!(window.sleep.as_mut()).is_pending(),
        "the next boundary is armed rather than already elapsed"
    );

    // The wall clock steps back behind the boundary that was just consumed.
    wall.set(50_000_000_000);
    sim.advance(Duration::from_secs(10));
    window.sleep.as_mut().await;
    assert!(!window.wake(), "a consumed boundary is not fired twice");
    assert_eq!(window.clock.last_boundary(), 60);
    assert!(
        futures::poll!(window.sleep.as_mut()).is_pending(),
        "a too-early wake still re-arms the sleep"
    );
}

/// Scenario: two requests are extracted immediately before and immediately
/// after a one-second window boundary.
/// Guarantees: the earlier request stays in the ACTIVE block and does not
/// seal it by itself, and the later one is parked for the next window with
/// admission closed, so each request belongs to exactly one window.
#[tokio::test(flavor = "current_thread")]
async fn admission_time_assigns_exactly_one_window() {
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(999_999_999));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        Arc::clone(&wall) as _,
        handler,
    );

    worker.admit(logs_pdata());
    assert_eq!(worker.active.data.window_start_secs, 0);
    assert_eq!(worker.active.tokens.len(), 1);
    assert!(
        !worker.rotation_requested,
        "one request no longer seals a block"
    );

    wall.set(1_000_000_001);
    worker.admit(logs_pdata());
    assert_eq!(worker.active.tokens.len(), 1);
    assert!(worker.pending.is_some());
    assert!(worker.rotation_requested);
    assert!(!worker.accept());
}

/// An object store whose writes park until a test opens its gate.
///
/// This is what keeps a block FLUSHING for as long as a test needs: no write
/// can resolve while the gate holds no permit, so the worker's one flush slot
/// stays taken across as many window boundaries as the test crosses. Opening
/// the gate leaves it open, because a permit is returned as soon as the write
/// that took it has passed.
#[derive(Debug)]
struct GatedStore {
    /// The store that actually holds the objects.
    inner: Arc<object_store::memory::InMemory>,
    /// Permits to write; empty until the test releases the parked flush.
    gate: Arc<tokio::sync::Semaphore>,
    /// Writes that have reached the gate, counted before they park on it.
    ///
    /// This is the signal that a flush has actually started: a test waits for
    /// it instead of guessing that the node has got that far.
    entered: Arc<std::sync::atomic::AtomicUsize>,
}

impl GatedStore {
    /// Announce a write, then wait for the gate.
    ///
    /// The count is raised before the wait, so a test observing it knows the
    /// write is parked rather than still to come.
    async fn pass(&self) {
        let _ = self
            .entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let permit = self.gate.acquire().await.expect("the gate is never closed");
        drop(permit);
    }
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.pass().await;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.pass().await;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Give the node task turns until `condition` holds, or fail the test.
///
/// Both clocks the node reads are simulated, so there is nothing to wait for:
/// the node only needs turns on the single-threaded runtime it shares with
/// the test. The bound is a failure rather than a timeout, so a condition
/// that never holds fails loudly instead of letting the test carry on
/// against a node that has not done what the test is about to assert.
async fn until(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{what} never happened");
}

/// Wait until one row-less marker request has been taken and decided.
///
/// Two observations in one, and neither is a guess about timing. The pdata
/// channel these tests use holds a single message, so handing the marker over
/// proves the request before it has been taken off the channel, and the
/// completion the marker earns proves the node has finished deciding it. A
/// request carrying no rows is decided without touching a block, so the
/// marker changes nothing else about the worker's state.
async fn marker(
    tx: &mpsc::Sender<OtapPdata>,
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
    tag: usize,
) {
    let mut context = Context::default();
    context.set_source_node(tag);
    tx.send_async(OtapPdata::new(
        context,
        OtapPayload::empty(SignalType::Logs),
    ))
    .await
    .expect("the marker enqueues");
    assert_eq!(
        expect_completion(rx).await,
        Some(tag),
        "the marker request is decided"
    );
}

/// Take one completion and say which request it belonged to, ack or nack.
async fn expect_completion(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) -> Option<usize> {
    match tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a completion arrives")
        .expect("a completion arrives")
    {
        PipelineCompletionMsg::DeliverAck { ack } => ack.accepted.into_parts().0.source_node(),
        PipelineCompletionMsg::DeliverNack { nack } => (*nack.refused).into_parts().0.source_node(),
    }
}

/// How many objects the store holds, which is two per written block.
async fn stored_files(store: &object_store::memory::InMemory) -> usize {
    use futures::StreamExt;
    store.list(None).count().await
}

/// Scenario: two window boundaries are crossed while the one flush slot is
/// held by a write the test has parked. Nothing is parked and no block is
/// full, so the boundary is the only thing that can ask for a rotation. The
/// write is then released without the clock moving again.
/// Guarantees: a boundary reached while the flush slot is busy is not lost,
/// the rotation it asks for is served the moment the flush completes rather
/// than at the next boundary, and two missed boundaries produce one rotation
/// rather than two.
#[tokio::test(flavor = "current_thread")]
async fn a_boundary_crossed_while_flushing_rotates_when_the_flush_completes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let inner = Arc::new(object_store::memory::InMemory::new());
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let store = Arc::new(GatedStore {
                inner: Arc::clone(&inner),
                gate: Arc::clone(&gate),
                entered: Arc::clone(&entered),
            });
            let writes = || entered.load(std::sync::atomic::Ordering::SeqCst);

            // One slot in each channel, so handing a message over is itself
            // the proof that the node has taken the previous one.
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(1);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(1);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let node = tokio::task::spawn_local(super::run(
                worker_config(),
                store as Arc<dyn ObjectStore>,
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            // The first request fills the window that starts at 0 s.
            pdata_tx
                .send_async(logs_pdata_from(1))
                .await
                .expect("the first request enqueues");
            marker(&pdata_tx, &mut rx, 10).await;

            // The 1 s boundary seals that block, and its write parks on the
            // gate: the flush slot is now held.
            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
            until("the first block's write reaches the store", || writes() > 0).await;

            // The second request joins the block that replaced it, which
            // cannot be written while the flush slot is held.
            pdata_tx
                .send_async(logs_pdata_from(2))
                .await
                .expect("the second request enqueues");
            marker(&pdata_tx, &mut rx, 11).await;

            // Two more boundaries pass while the write is still parked. No
            // request is parked and no budget is reached, so nothing but
            // these boundaries can ask for the rotation that follows.
            wall.set(3_100_000_000);
            sim.advance(Duration::from_secs(2));

            // Control outranks the inbox but is outranked by the boundary, so
            // two control messages handed over through a one-slot channel
            // prove the node has served the boundary that was ready first.
            for _ in 0..2 {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control_tx.send_async(NodeControlMsg::TimerTick {}),
                )
                .await
                .expect("control is still served while the flush is parked")
                .expect("the control message enqueues");
            }
            assert_eq!(
                stored_files(&inner).await,
                0,
                "nothing is written while the flush is parked"
            );

            // Releasing the write is the only thing that changes: the clock
            // stays where it is, so the second block can only be written by
            // the rotation the boundaries left owed.
            gate.add_permits(1);
            assert_eq!(expect_ack(&mut rx).await, Some(1));
            assert_eq!(
                expect_ack(&mut rx).await,
                Some(2),
                "the boundary's rotation is served on completion, not at the next boundary"
            );

            // The marker gives the node the turns a third rotation would need
            // before the file count is read.
            marker(&pdata_tx, &mut rx, 12).await;
            assert_eq!(
                stored_files(&inner).await,
                4,
                "two boundaries missed while flushing are one rotation, not two"
            );

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

/// Scenario: a request is parked for a later window while the one flush slot
/// is held by a write the test has parked, a newer request queues behind it,
/// and control traffic arrives throughout. The write is then released without
/// the clock moving again.
/// Guarantees: the parked request enters the block the completed flush opens,
/// ahead of the request that was queued behind it, which is not admitted
/// while one is parked; control stays served while pdata admission is closed;
/// and the flush that follows is a single rotation.
#[tokio::test(flavor = "current_thread")]
async fn a_parked_request_enters_the_block_the_finished_flush_opens() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let inner = Arc::new(object_store::memory::InMemory::new());
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let store = Arc::new(GatedStore {
                inner: Arc::clone(&inner),
                gate: Arc::clone(&gate),
                entered: Arc::clone(&entered),
            });
            let writes = || entered.load(std::sync::atomic::Ordering::SeqCst);

            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(1);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(1);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let node = tokio::task::spawn_local(super::run(
                worker_config(),
                store as Arc<dyn ObjectStore>,
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            pdata_tx
                .send_async(logs_pdata_from(1))
                .await
                .expect("the first request enqueues");
            marker(&pdata_tx, &mut rx, 10).await;

            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
            until("the first block's write reaches the store", || writes() > 0).await;

            pdata_tx
                .send_async(logs_pdata_from(2))
                .await
                .expect("the second request enqueues");
            marker(&pdata_tx, &mut rx, 11).await;

            // The wall clock moves past the ACTIVE block's window without the
            // boundary sleep firing, so the third request is parked rather
            // than admitted to a block whose window has ended.
            wall.set(2_100_000_000);
            pdata_tx
                .send_async(logs_pdata_from(3))
                .await
                .expect("the third request enqueues");

            // The one-slot channel takes this only once the third request has
            // been taken off it, so the fourth is queued behind a parked one.
            // Admission is closed, so it stays on the channel.
            pdata_tx
                .send_async(logs_pdata_from(4))
                .await
                .expect("the fourth request enqueues");

            // Control is served even though pdata admission is closed and the
            // flush is parked.
            for _ in 0..2 {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    control_tx.send_async(NodeControlMsg::TimerTick {}),
                )
                .await
                .expect("control is still served while the flush is parked")
                .expect("the control message enqueues");
            }
            assert_eq!(
                stored_files(&inner).await,
                0,
                "nothing is written while the flush is parked"
            );

            // Releasing the write opens the block the parked request has been
            // waiting for, without the clock moving again.
            gate.add_permits(1);
            assert_eq!(expect_ack(&mut rx).await, Some(1));
            assert_eq!(expect_ack(&mut rx).await, Some(2));
            until("both blocks are written", || writes() == 4).await;

            // The marker is taken only once the fourth request has been, so
            // by this point both it and the parked request are admitted.
            marker(&pdata_tx, &mut rx, 12).await;
            assert_eq!(
                stored_files(&inner).await,
                4,
                "the resumed request's block is not sealed before its window ends"
            );

            // The next boundary seals the block the parked request entered,
            // together with the request that was queued behind it.
            wall.set(5_100_000_000);
            sim.advance(Duration::from_secs(2));
            assert_eq!(
                expect_ack(&mut rx).await,
                Some(3),
                "the parked request is stored before the newer one"
            );
            assert_eq!(expect_ack(&mut rx).await, Some(4));

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

/// Scenario: a block reaches its request limit inside one window.
/// Guarantees: the rotation is requested by the limit rather than by the
/// window, so a burst is not held until the boundary, and admission closes
/// until the block has been sealed.
#[tokio::test(flavor = "current_thread")]
async fn the_request_limit_asks_for_a_rotation_within_a_window() {
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config_with_requests(2),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );

    worker.admit(logs_pdata());
    assert!(!worker.rotation_requested);
    worker.admit(logs_pdata());
    assert_eq!(worker.active.tokens.len(), 2);
    assert!(worker.rotation_requested, "a full block is sealed at once");
    assert!(!worker.accept());
}

/// Scenario: telemetry is collected after admitting a request and while a
/// notification waits.
/// Guarantees: gauges include live requests and all required worker
/// instruments have stable names.
#[tokio::test(flavor = "current_thread")]
async fn worker_metrics_cover_live_memory_and_requests() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.metrics = Some(super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    ));

    worker.admit(logs_pdata());
    worker.sample_metrics();

    let metrics = worker.metrics.as_ref().expect("registered");
    assert_eq!(metrics.worker.requests_pending.get(), 1);
    assert!(metrics.worker.memory_accounted_bytes.get() >= worker.active.data.bytes as u64);
    assert!(
        metrics.worker.memory_budget_bytes.get() >= metrics.worker.memory_accounted_bytes.get()
    );
    assert_eq!(
        metrics.worker.snapshot().descriptor().name,
        "exporter.series_parquet"
    );
}

/// Scenario: a worker with registered instruments samples its telemetry.
/// Guarantees: the worker publishes exactly its accounted bytes into the
/// process-wide series accounting, so the engine residual subtracts what this
/// worker actually retains.
#[tokio::test(flavor = "current_thread")]
async fn worker_publishes_accounted_bytes_to_process_accounting() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config(),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.metrics = Some(super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    ));

    assert_eq!(worker.accounting.bytes(), 0);
    worker.admit(logs_pdata());
    worker.sample_metrics();

    let accounted = worker
        .metrics
        .as_ref()
        .expect("registered")
        .worker
        .memory_accounted_bytes
        .get();
    assert!(accounted > 0);
    assert_eq!(worker.accounting.bytes(), accounted);
}

/// Scenario: one block is abandoned after admission and a later block commits
/// successfully.
/// Guarantees: `series_emitted` excludes admitted/abandoned rows and
/// increments only on durable completion.
#[tokio::test(flavor = "current_thread")]
async fn series_emitted_requires_durable_completion() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.metrics = Some(super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            assert!(
                worker
                    .metrics
                    .as_mut()
                    .expect("metrics")
                    .emitted
                    .terminal_snapshots()
                    .is_empty(),
                "admission alone emits nothing"
            );
            worker.fail_active(Outcome::Storage);
            assert!(
                worker
                    .metrics
                    .as_mut()
                    .expect("metrics")
                    .emitted
                    .terminal_snapshots()
                    .is_empty(),
                "an abandoned block emits nothing"
            );

            worker.admit(logs_pdata());
            let expected = worker.active.data.pending_series.len() as u64;
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(done.as_ref().expect("the flush task joins").result.is_ok());
            assert!(
                worker
                    .metrics
                    .as_mut()
                    .expect("metrics")
                    .emitted
                    .terminal_snapshots()
                    .is_empty(),
                "a resolved write that has not been completed emits nothing"
            );

            worker.complete(done);
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(
                metrics
                    .emitted
                    .get(super::metrics::EmitAttrs {
                        reason: super::metrics::EmitReason::New
                    })
                    .series_emitted
                    .get(),
                expected
            );
        })
        .await;
}

/// Scenario: a request-limited block rotates twice inside one window and then
/// a later window opens a block in a different partition, with every flush
/// completing durably.
/// Guarantees: the flush trigger, the re-emission cause and the written
/// dataset are labelled from what the worker actually did. The replacement
/// block inside one window reports `rotation`, a block in a new partition
/// reports `partition`, and an empty rotation is not counted as a flush.
#[tokio::test(flavor = "current_thread")]
async fn rotation_causes_and_flush_reasons_are_labelled() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use super::metrics::{DatasetAttrs, DatasetLabel, EmitAttrs, EmitReason, FlushAttrs};
            use super::metrics::{FlushReason, Metrics};

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config_with_requests(1),
                Arc::new(object_store::memory::InMemory::new()),
                wall.clone(),
                handler,
            );
            worker.metrics = Some(Metrics::register(&context, &worker.cfg.lake));

            /// Seal the ACTIVE block, wait for its write and decide it.
            async fn flush(worker: &mut Worker) {
                worker.rotate();
                let done = worker
                    .flushing
                    .as_mut()
                    .expect("a rotated block is flushing")
                    .finish()
                    .await;
                assert!(done.as_ref().expect("the flush task joins").result.is_ok());
                worker.complete(done);
                // The decided block keeps the flush slot until its supervising
                // task has released it, so the next rotation waits for that.
                drain_cleanup(worker).await;
                // The notifier holds one credit per block here, so the ack is
                // delivered before the next block is decided.
                assert!(worker.notify.next().await.is_ok());
            }

            fn emitted(worker: &Worker, reason: EmitReason) -> u64 {
                worker
                    .metrics
                    .as_ref()
                    .expect("metrics")
                    .emitted
                    .get(EmitAttrs { reason })
                    .series_emitted
                    .get()
            }

            // A full block inside one window: the first copy of the
            // descriptor is new, and the block that replaces it must repeat it
            // because the replaced block is not durable when it is opened.
            worker.admit(logs_pdata());
            assert_eq!(worker.reason, FlushReason::Requests);
            let rows = worker.active.data.pending_series.len() as u64;
            assert!(rows > 0, "the request carries a descriptor");
            flush(&mut worker).await;
            assert!(worker.active.reemit, "the window has not moved on");
            assert_eq!(emitted(&worker, EmitReason::New), rows);

            worker.admit(logs_pdata());
            flush(&mut worker).await;
            assert_eq!(emitted(&worker, EmitReason::Rotation), rows);

            // A later window in a later storage partition: partitions are
            // date/hour, so the clock has to cross an hour for the cached
            // commit to stop applying.
            wall.set(4_000_000_000_000);
            worker.admit(logs_pdata());
            assert!(worker.pending.is_some(), "a later window parks the request");
            worker.rotate();
            assert!(
                worker.flushing.is_none(),
                "an empty block is replaced rather than written"
            );
            assert!(!worker.active.reemit);
            worker.resume_pending();
            flush(&mut worker).await;
            assert_eq!(emitted(&worker, EmitReason::Partition), rows);

            worker.sample_metrics();
            let metrics = worker.metrics.as_mut().expect("metrics");
            assert_eq!(metrics.worker.acks.get(), 3);
            // Three real writes, each sealed by the one-request limit. The
            // empty rotation the parked request forced is not one of them,
            // and the block that took the parked request was still sealed by
            // the limit rather than by its window.
            for (reason, expected) in [
                (FlushReason::Requests, 3),
                (FlushReason::Time, 0),
                (FlushReason::Bytes, 0),
                (FlushReason::Shutdown, 0),
            ] {
                assert_eq!(
                    metrics.flush.get(FlushAttrs { reason }).count.get(),
                    expected,
                    "flush count for {reason:?}"
                );
            }
            for dataset in [DatasetLabel::LogsSeries, DatasetLabel::LogsValues] {
                let written = metrics.written.get(DatasetAttrs { dataset });
                assert_eq!(written.files_written.get(), 3, "files for {dataset:?}");
                assert!(written.rows_written.get() >= 3, "rows for {dataset:?}");
            }
        })
        .await;
}

/// Read one metric value out of a terminal handoff by name and labels.
///
/// Panics rather than returning an option: a test asking for a metric the
/// handoff does not carry has found the regression it was written for.
fn terminal_value(snapshots: &[MetricSetSnapshot], name: &str, labels: &[(&str, &str)]) -> u64 {
    for snapshot in snapshots {
        let Some(index) = snapshot
            .descriptor()
            .metrics
            .iter()
            .position(|metric| metric.name == name)
        else {
            continue;
        };
        let actual: Vec<_> = snapshot.measurement_attributes().collect();
        if actual.as_slice() != labels {
            continue;
        }
        return snapshot.get_metrics()[index].to_u64_lossy();
    }
    panic!("no terminal snapshot carries {name} with {labels:?}")
}

/// Scenario: a node takes several requests, one telemetry collection and a
/// shutdown, driving many turns of its select loop.
/// Guarantees: the worker scans itself exactly once per collection and once
/// more for the terminal handoff. The scan walks both token vectors and the
/// notification queue, so sampling it per loop turn would make a block cost
/// quadratic time in its request count.
#[tokio::test(flavor = "current_thread")]
async fn telemetry_is_scanned_only_when_it_is_collected() {
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
            let (handler, _rx) = effects(64);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (_reporter_rx, metrics_reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);

            for _ in 0..3 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request enqueues");
            }
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry { metrics_reporter })
                .await
                .expect("the collection enqueues");
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Closing the upstream channel is what releases the latched
            // shutdown once the backlog has been drained.
            drop(pdata_tx);

            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                handler,
            );
            worker.metrics = Some(super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));
            let terminal =
                tokio::time::timeout(Duration::from_secs(5), super::drive(&mut worker, inbox))
                    .await
                    .expect("the node returns")
                    .expect("the node does not fail");
            assert!(
                !terminal.metrics().is_empty(),
                "the handoff carries metrics"
            );
            assert_eq!(
                worker.samples, 2,
                "one scan for the collection and one for the terminal handoff"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: a block asks for a rotation because it filled its request budget,
/// and the window boundary fires before that rotation has been served.
/// Guarantees: the flush is reported as time-triggered, because that is what
/// sealed it. A threshold that never got to rotate must not leave its reason
/// behind for every boundary flush that follows.
#[tokio::test(flavor = "current_thread")]
async fn a_boundary_rotation_is_not_reported_under_a_stale_threshold_reason() {
    tokio::task::LocalSet::new()
        .run_until(async {
            use super::metrics::{FlushAttrs, FlushReason, Metrics};

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config_with_requests(1),
                Arc::new(object_store::memory::InMemory::new()),
                wall.clone(),
                handler,
            );
            worker.metrics = Some(Metrics::register(&context, &worker.cfg.lake));

            worker.admit(logs_pdata());
            assert_eq!(worker.reason, FlushReason::Requests);
            assert!(worker.rotation_requested);

            // The one-second window ends before the requested rotation has
            // been served, so the boundary is what actually seals the block.
            wall.set(2_000_000_000);
            worker.wake_window();
            assert_eq!(worker.reason, FlushReason::Time);

            worker.rotate();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(
                metrics
                    .flush
                    .get(FlushAttrs {
                        reason: FlushReason::Time
                    })
                    .count
                    .get(),
                1
            );
            assert_eq!(
                metrics
                    .flush
                    .get(FlushAttrs {
                        reason: FlushReason::Requests
                    })
                    .count
                    .get(),
                0
            );
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a write is outstanding, so
/// the node cancels it instead of waiting for it.
/// Guarantees: the abandoned flush is counted exactly as a write that returned
/// `Cancelled` would be -- one failure, one cancellation and one duration --
/// so a node that always runs out of time does not silently report zero
/// flush failures.
#[tokio::test(flavor = "current_thread")]
async fn an_abandoned_flush_is_counted_as_cancelled() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.metrics = Some(super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            assert!(worker.flushing.is_some(), "a write is outstanding");

            worker.abandon().await;
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_failures.get(), 1);
            assert_eq!(metrics.worker.flush_cancelled.get(), 1);
            assert_eq!(metrics.worker.flush_duration.count, 1);
        })
        .await;
}

/// Scenario: one request fills a block, a second is parked behind it, and the
/// first block's completions are released into the notifier.
/// Guarantees: the parked extraction and the undelivered completions are each
/// reported as their own `By` gauge and are both included in the accounted
/// total, so the memory a saturated worker holds is visible per component
/// rather than only in aggregate.
#[tokio::test(flavor = "current_thread")]
async fn pending_and_notification_bytes_are_reported() {
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (handler, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(
        worker_config_with_requests(1),
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.metrics = Some(super::metrics::Metrics::register(
        &context,
        &worker.cfg.lake,
    ));

    worker.admit(logs_pdata());
    worker.admit(logs_pdata());
    assert!(worker.pending.is_some(), "the second request is parked");
    // Releases the first block's completion into the notifier without needing
    // a write to resolve.
    worker.fail_active(Outcome::Storage);
    assert_eq!(worker.notify.len(), 1);

    worker.sample_metrics();
    let metrics = worker.metrics.as_ref().expect("metrics");
    let pending = metrics.worker.pending_bytes.get();
    let tokens = metrics.worker.notify_token_bytes.get();
    assert!(pending > 0, "the parked request retains rows and a token");
    assert!(
        tokens > 0,
        "an undelivered completion retains its queue cell"
    );
    assert!(metrics.worker.memory_accounted_bytes.get() >= pending + tokens);
    assert_eq!(metrics.worker.pending_slot.get(), 1);
}

/// Injection mode: every request passes through to the inner store.
const FAULT_NONE: u8 = 0;
/// Injection mode: every write of a `series` dataset file fails.
const FAULT_SERIES: u8 = 1;
/// Injection mode: the first write of a `values` dataset file fails, and the
/// store heals itself in the same step so the retry succeeds.
const FAULT_VALUES_ONCE: u8 = 2;
/// Injection mode: every write parks until the test releases it.
const FAULT_PARK: u8 = 3;
/// Injection mode: a `values` multipart upload is initiated and then wedges --
/// its parts never land and its abort never returns -- while everything else
/// passes through.
const FAULT_MULTIPART_WEDGE: u8 = 5;
/// Injection mode: every write spends [`SLOW_FAILURE`] of engine-clock time
/// and then fails, the way a cloud store's own retry loop answers a request
/// once its `retry_timeout` is spent.
const FAULT_SLOW_FAIL: u8 = 6;
/// How long one write takes to fail under [`FAULT_SLOW_FAIL`].
const SLOW_FAILURE: Duration = Duration::from_secs(20);

/// An object store that injects failures at the two entry points a Parquet
/// write actually uses: a small single-shot PUT and the initiation of a
/// multipart upload.
///
/// This is a fault wrapper around an in-memory store, not a network
/// destination: it proves what the exporter does with a store that refuses,
/// stalls or heals, and nothing about real object storage behaviour.
///
/// Recording each PUT before it is allowed to fail is what lets a test compare
/// the bytes and the path of a failed attempt with those of the retry that
/// followed it, which is the observable form of "retries reuse frozen file
/// names and byte-identical objects".
#[derive(Debug, Default)]
struct FaultStore {
    /// The store that actually holds whatever is allowed through.
    inner: object_store::memory::InMemory,
    /// The active injection mode, one of the `FAULT_*` constants.
    mode: std::sync::atomic::AtomicU8,
    /// Path and payload of every PUT, including the ones that then failed.
    writes: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    /// Raised as each write reaches the injection point, so a test can wait
    /// for a flush to have started rather than guess that it has.
    entered: tokio::sync::Notify,
    /// Releases one parked write under `FAULT_PARK`.
    release: tokio::sync::Notify,
    /// Writes currently parked inside the store under `FAULT_PARK`.
    parked: std::sync::atomic::AtomicUsize,
    /// Parked writes whose future was dropped rather than released, which is
    /// what a cancelled node has to produce.
    parked_drops: std::sync::atomic::AtomicUsize,
    /// Parts handed to a wedged multipart upload.
    parts: Arc<std::sync::atomic::AtomicUsize>,
    /// Aborts attempted against a wedged multipart upload.
    aborts: Arc<std::sync::atomic::AtomicUsize>,
}

/// A real multipart upload that is initiated and then never progresses.
///
/// The underlying store initiates it, so the destination genuinely holds an
/// unfinished upload; its parts then park and its abort never returns. That is
/// the shape that makes the cleanup bound matter: abandoning the upload
/// without an abort leaves a partial upload behind, and the abort is exactly
/// the call that may never come back.
#[derive(Debug)]
struct WedgedUpload {
    /// The upload the underlying store really initiated, held so it is only
    /// released when this one is dropped.
    _inner: Box<dyn MultipartUpload>,
    /// Parts handed to this upload, shared with the store the test holds.
    parts: Arc<std::sync::atomic::AtomicUsize>,
    /// Aborts attempted against this upload.
    aborts: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl MultipartUpload for WedgedUpload {
    fn put_part(&mut self, _data: PutPayload) -> UploadPart {
        let _ = self.parts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Parked rather than delivered: the writer must still be in the phase
        // where the sink aborts a failed upload when the deadline arrives. A
        // part that lands immediately lets the writer reach the finalizing
        // phase, which by design leaves a cancelled upload to the bucket's
        // multipart lifecycle rule rather than aborting it.
        Box::pin(std::future::pending())
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        std::future::pending().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let _ = self
            .aborts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::future::pending().await
    }
}

impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("series fault store")
    }
}

impl FaultStore {
    /// Announce a write and apply the active injection mode to it.
    async fn before(&self, path: &object_store::path::Path) -> object_store::Result<()> {
        self.entered.notify_one();
        let mode = self.mode.load(std::sync::atomic::Ordering::SeqCst);
        if mode == FAULT_PARK {
            /// Accounts for one parked write for as long as its future lives.
            ///
            /// A write that is released decrements the live count only; one
            /// whose future is dropped while still parked also counts as a
            /// drop, which is the observable form of "cancellation released
            /// the write rather than leaking it".
            struct Parked<'a> {
                /// Writes still parked.
                live: &'a std::sync::atomic::AtomicUsize,
                /// Parked writes whose future was dropped.
                drops: &'a std::sync::atomic::AtomicUsize,
                /// Whether the park ended by release rather than by a drop.
                released: bool,
            }
            impl Drop for Parked<'_> {
                fn drop(&mut self) {
                    let _ = self.live.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    if !self.released {
                        let _ = self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
            let _ = self
                .parked
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut guard = Parked {
                live: &self.parked,
                drops: &self.parked_drops,
                released: false,
            };
            self.release.notified().await;
            guard.released = true;
        }
        if mode == FAULT_SLOW_FAIL {
            clock::sleep(SLOW_FAILURE).await;
        }
        let path = path.as_ref();
        let fail = mode == 4
            || mode == FAULT_SLOW_FAIL
            || (mode == FAULT_SERIES && path.contains("dataset=series/"))
            || (mode == FAULT_VALUES_ONCE
                && path.contains("dataset=values/")
                && self
                    .mode
                    .compare_exchange(
                        FAULT_VALUES_ONCE,
                        FAULT_NONE,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok());
        if fail {
            return Err(object_store::Error::Generic {
                store: "series-test",
                source: Box::new(std::io::Error::other("injected store failure")),
            });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let bytes = payload.iter().flat_map(|b| b.iter().copied()).collect();
        self.writes
            .lock()
            .expect("writes lock")
            .push((path.to_string(), bytes));
        self.before(path).await?;
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.before(path).await?;
        let inner = self.inner.put_multipart_opts(path, options).await?;
        if self.mode.load(std::sync::atomic::Ordering::SeqCst) == FAULT_MULTIPART_WEDGE
            && path.as_ref().contains("dataset=values/")
        {
            return Ok(Box::new(WedgedUpload {
                _inner: inner,
                parts: Arc::clone(&self.parts),
                aborts: Arc::clone(&self.aborts),
            }));
        }
        Ok(inner)
    }

    async fn get_opts(
        &self,
        path: &object_store::path::Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Advances a simulated clock on every runtime turn, until it is dropped.
///
/// The flush retry backoff and the absolute retry deadline are both measured
/// on the engine clock, so a test that needs a retry to happen has to move
/// that clock rather than sleep on the real one. Advancing on every turn is
/// also self-limiting: the absolute deadline is reached in a finite number of
/// steps, so a condition that never holds fails the flush instead of hanging.
struct Ticker(tokio::task::JoinHandle<()>);

impl Drop for Ticker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Advance both the engine clock and the wall clock, a second per turn.
///
/// Window boundaries are aligned to wall time but waited for on the engine
/// clock, so a test that needs boundaries to keep arriving has to move both.
/// One boundary is reached per turn, and the sleep the node re-arms is not
/// ready again until the next one, so the other select branches keep their
/// turn rather than being starved by a boundary that is always ready.
fn ticking_windows(sim: &clock::SimClock, wall: &Arc<lake::clock::TestWallClock>) -> Ticker {
    let sim = sim.clone();
    let wall = Arc::clone(wall);
    Ticker(tokio::task::spawn_local(async move {
        let mut secs = 0_i64;
        loop {
            tokio::task::yield_now().await;
            secs = secs.saturating_add(1);
            wall.set(secs.saturating_mul(1_000_000_000));
            sim.advance(Duration::from_secs(1));
        }
    }))
}

/// Start advancing `sim` by `step` on every turn of the current runtime.
fn ticking(sim: &clock::SimClock, step: Duration) -> Ticker {
    let sim = sim.clone();
    Ticker(tokio::task::spawn_local(async move {
        loop {
            tokio::task::yield_now().await;
            sim.advance(step);
        }
    }))
}

/// Release a finished flush's cleanup slot so the worker can rotate again.
///
/// A completed flush leaves its supervising task in the same FLUSHING slot
/// until that task has finished releasing the block, sink and write future it
/// owns. The node loop joins it in its own select branch; a test driving a
/// worker directly has to do the same before the next rotation.
async fn drain_cleanup(worker: &mut Worker) {
    if let Some(mut job) = worker.cleaning.take() {
        job.cleanup().await.expect("the cleanup task joins");
    }
}

/// Scenario: the series file is written, the first values file write fails,
/// and the store heals before the retry.
/// Guarantees: the retry re-uses the frozen file names and byte-identical
/// objects, the block is acknowledged exactly once, and the retry is counted
/// without counting a flush failure.
#[tokio::test(flavor = "current_thread")]
async fn values_retry_reuses_paths_and_bytes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_VALUES_ONCE, std::sync::atomic::Ordering::SeqCst);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);
            worker.metrics = Some(super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            let _ticker = ticking(&sim, Duration::from_millis(250));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert_eq!(
                done.as_ref().expect("the flush resolves").attempts,
                2,
                "the failed write is retried once"
            );

            worker.complete(done);
            worker.notify.next().await.expect("the ack is sent");
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));

            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(
                metrics.worker.flush_retries.get(),
                1,
                "the extra attempt is reported as a retry"
            );
            assert_eq!(
                metrics.worker.flush_failures.get(),
                0,
                "a retried flush that succeeded is not a failure"
            );
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            let writes = store.writes.lock().expect("writes lock");
            assert_eq!(writes.len(), 4, "two files, one of them written twice");
            assert_eq!(writes[0], writes[2], "the series file is rewritten as-is");
            assert_eq!(writes[1], writes[3], "the values file is rewritten as-is");
        })
        .await;
}

/// Scenario: the series file write fails until the absolute retry deadline,
/// and a request carrying the same descriptor arrives afterwards.
/// Guarantees: the failed block nacks retryably without marking the cache, and
/// the next block writes that descriptor again.
#[tokio::test(flavor = "current_thread")]
async fn failed_descriptor_does_not_poison_cache() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_SERIES, std::sync::atomic::Ordering::SeqCst);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(30);
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(50));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            worker.complete(done);
            worker.notify.next().await.expect("the nack is sent");
            match rx.recv().await.expect("nack") {
                PipelineCompletionMsg::DeliverNack { nack } => assert!(
                    !nack.permanent,
                    "a storage failure is the sender's to retry"
                ),
                other => panic!("expected a retryable nack, got {other:?}"),
            }
            assert!(
                !worker
                    .cache
                    .is_committed(&id, lake::clock::PartitionId::from_unix_secs(0)),
                "a block that was never written marks nothing durable"
            );
            drain_cleanup(&mut worker).await;

            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            worker.admit(logs_pdata());
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the descriptor is written again rather than assumed durable"
            );
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("the second block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert_eq!(worker.notify.len(), 1, "the second block is acknowledged");
        })
        .await;
}

/// Scenario: a flush parked across an hour boundary is joined by the same
/// series in a later partition, and the cache evicts that series before the
/// flush resolves.
/// Guarantees: the commit names the flushed block's own partition, and it
/// cannot retract the descriptor the next partition still owes.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_series_and_eviction_preserve_partition_coverage() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(3_599_000_000_000));
            let mut cfg = worker_config();
            cfg.cache_entries = 1;
            let mut worker = Worker::new(cfg, store.clone(), Arc::clone(&wall) as _, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            store.entered.notified().await;

            wall.set(3_600_000_000_000);
            worker.admit(logs_pdata());
            assert!(
                worker.pending.is_some(),
                "a request for the next hour waits for its own block"
            );
            worker.cache.touch([99; 16]);

            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_one();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert_eq!(
                worker.cache.last_committed(&id),
                Some(lake::clock::PartitionId::from_unix_secs(3599)),
                "the commit names the partition that was written"
            );
            drain_cleanup(&mut worker).await;

            worker.rotate();
            worker.resume_pending();
            assert_eq!(
                worker.active.data.partition,
                lake::clock::PartitionId::from_unix_secs(3600)
            );
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the new partition writes its own copy of the descriptor"
            );
        })
        .await;
}

/// Scenario: the same series is admitted to the ACTIVE block while the
/// FLUSHING block carrying it is parked in the same window.
/// Guarantees: both blocks keep their own descriptor copy, and the flushed
/// block's commit cannot retract the copy the ACTIVE block still owes.
#[tokio::test(flavor = "current_thread")]
async fn same_window_overlap_keeps_both_descriptors() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, _rx) = effects(8);
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
            worker.rotate();
            store.entered.notified().await;

            worker.admit(logs_pdata());
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the ACTIVE block carries its own copy"
            );
            assert_eq!(
                worker
                    .flushing
                    .as_ref()
                    .expect("a rotated block is flushing")
                    .tokens
                    .len(),
                1,
                "the flushing block still owes its own request"
            );
            let partition = worker.active.data.partition;
            assert!(!worker.cache.is_committed(&id, partition));

            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_one();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert!(worker.cache.is_committed(&id, partition));
            assert!(
                worker.active.data.pending_series.contains(&id),
                "a late commit does not retract a descriptor already reserved"
            );
            drain_cleanup(&mut worker).await;

            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("the second block is flushing")
                .finish()
                .await;
            let report = done
                .as_ref()
                .expect("the flush resolves")
                .result
                .as_ref()
                .expect("the write succeeds");
            assert_eq!(report.files.len(), 2, "the descriptor is written again");
            worker.complete(done);
            assert_eq!(worker.notify.len(), 2);
        })
        .await;
}

/// Scenario: a write is parked in the store when its absolute retry deadline
/// expires, while cleanup is allowed a whole second.
/// Guarantees: the retryable decision is published at the deadline rather than
/// after the cleanup allowance, and the FLUSHING slot stays occupied by the
/// cleanup until it has finished.
#[tokio::test(flavor = "current_thread")]
async fn retry_deadline_publishes_before_cleanup_and_reserves_slot() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            store.entered.notified().await;

            // Only the retry deadline has elapsed. The write is still parked
            // in the store, so a decision that arrives now cannot have waited
            // for either the write or the cleanup allowance.
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the deadline fails the block"
            );
            worker.complete(done);
            assert_eq!(
                worker.notify.outcomes()[Outcome::Storage as usize],
                1,
                "the block is nacked as retryable storage"
            );

            // The slot the cleanup holds is the same FLUSHING slot, so nothing
            // rotates into it while the abandoned write is still unwinding.
            worker.admit(logs_pdata());
            worker.rotate();
            assert!(worker.flushing.is_none(), "no second write is started");
            assert!(worker.cleaning.is_some(), "the slot is still occupied");

            let mut job = worker.cleaning.take().expect("the occupied slot");
            sim.advance(Duration::from_secs(2));
            job.cleanup().await.expect("the cleanup is bounded");

            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            worker.rotate();
            assert!(
                worker.flushing.is_some(),
                "the released slot takes the next block"
            );
        })
        .await;
}

/// Scenario: a cloud store's retry budget -- the three-minute default when no
/// retry section is set, or an explicit one -- is checked against the block's
/// flush deadline, and local file storage is checked with the same values.
/// Guarantees: a store retry budget that is not strictly shorter than
/// `window.flush_retry_deadline` is refused with both values in the message,
/// so one write attempt can never retry inside the store past the deadline;
/// local file storage, which applies no store retry, is never refused for it.
#[test]
fn a_store_retry_budget_must_be_shorter_than_the_flush_deadline() {
    let deadline = Duration::from_secs(60);
    let err = super::config::check_retry_deadline(true, None, deadline)
        .expect_err("the three-minute default outlives the deadline");
    assert!(err.contains("(180s)"), "{err}");
    assert!(err.contains("window.flush_retry_deadline (60s)"), "{err}");
    assert!(err.contains("object store default"), "{err}");
    let retry = |timeout: &str| -> otel_arrow_dfe_otap::object_store::RetryOptions {
        serde_json::from_value(serde_json::json!({ "retry_timeout": timeout }))
            .expect("retry options")
    };
    let equal = super::config::check_retry_deadline(true, Some(&retry("60s")), deadline)
        .expect_err("equal is not strictly less");
    assert!(equal.starts_with("retry.retry_timeout (60s)"), "{equal}");
    assert!(super::config::check_retry_deadline(true, Some(&retry("30s")), deadline).is_ok());
    assert!(super::config::check_retry_deadline(false, None, deadline).is_ok());
    // The shipped local example keeps loading with no retry section at all.
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}}
    }))
    .expect("file storage applies no store retry");
    assert!(cfg.retry.is_none());
}

/// Scenario: the one write of a flush never returns, so the block's retry
/// deadline expires while that first attempt is still in flight.
/// Guarantees: the flush ends as a distinct deadline outcome rather than as a
/// cancellation -- the cancellation counter stays at zero -- and every request
/// of the block is nacked retryably with a reason that says the deadline
/// expired with no attempt returned, so a storage hang is never reported as a
/// shutdown or as an unexplained failure.
#[tokio::test(flavor = "current_thread")]
async fn a_hung_write_expires_the_flush_deadline_as_its_own_outcome() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            store.entered.notified().await;
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 1);
            assert!(
                matches!(
                    finished.result,
                    Err(lake::Error::DeadlineExceeded {
                        attempts: 1,
                        last: None
                    })
                ),
                "a hung write is a deadline, not a cancellation: {:?}",
                finished.result.as_ref().err()
            );
            worker.complete(done);
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_failures.get(), 1);
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert!(
                        nack.reason.contains(
                            "flush retry deadline exceeded after 1 attempt(s); no attempt \
                             returned before the deadline"
                        ),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            sim.advance(Duration::from_secs(10));
            job.cleanup().await.expect("the task is released");
        })
        .await;
}

/// Scenario: every write fails, but only after twenty seconds -- the shape of
/// a cloud store retrying one request internally until its own
/// `retry_timeout` -- under a sixty-second flush deadline.
/// Guarantees: the flush retries the block until the deadline, counts every
/// attempt in `flush.retries`, and nacks the block retryably with the
/// destination's last error in the reason, so an outage is visible to the
/// producer and to the operator with its cause rather than as a bare
/// "cancelled" with zero retries.
#[tokio::test(flavor = "current_thread")]
async fn a_slowly_failing_store_surfaces_its_last_error_at_the_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_SLOW_FAIL, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(100));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            let finished = done.as_ref().expect("the flush resolves");
            // Attempts start at 0 s, 20.2 s and 40.6 s; the third is still
            // failing when the deadline expires at 60 s.
            assert_eq!(finished.attempts, 3);
            match &finished.result {
                Err(lake::Error::DeadlineExceeded {
                    attempts: 3,
                    last: Some(last),
                }) => assert!(
                    last.to_string().contains("injected store failure"),
                    "{last}"
                ),
                other => panic!("expected the deadline with the last error, got {other:?}"),
            }
            worker.complete(done);
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_retries.get(), 2);
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert!(
                        nack.reason.contains("after 3 attempt(s); last error:")
                            && nack.reason.contains("injected store failure"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            let ticker = ticking(&sim, Duration::from_secs(1));
            job.cleanup().await.expect("the task is released");
            drop(ticker);
        })
        .await;
}

/// Scenario: an encoding bug and a storage I/O error arrive as the same lake
/// error type.
/// Guarantees: only a failure with a storage origin earns whole-block retries.
#[test]
fn retry_classifier_distinguishes_encoding_from_storage() {
    assert!(!super::flush::retryable(&lake::Error::Parquet(
        parquet::errors::ParquetError::General("encoding bug".into())
    )));
    assert!(super::flush::retryable(&lake::Error::ObjectStore(
        object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }
    )));
    assert!(super::flush::retryable(&lake::Error::Parquet(
        parquet::errors::ParquetError::External(Box::new(object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }))
    )));
    assert!(!super::flush::retryable(&lake::Error::Cancelled {
        abort_error: None
    }));
}

/// One logs request carrying `records` log records of a single series, so its
/// values file is large enough to span several multipart chunks.
fn bulk_logs_pdata(records: usize) -> OtapPdata {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: (0..records)
                    .map(|i| LogRecord {
                        time_unix_nano: 1_789_960_500_000_000_000 + i as u64,
                        event_name: "ready".to_owned(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = encoded(&request);
    let view = RawLogsData::try_new(&bytes).expect("valid logs bytes");
    let payload = OtapPayload::from(encode_logs_otap_batch(&view).expect("encodes to OTAP"));
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, payload)
}

/// Scenario: a values multipart upload is initiated and then wedges, its parts
/// never landing and its abort never returning, while the block's absolute
/// retry deadline expires.
/// Guarantees: the retryable decision is published at the deadline, the
/// FLUSHING slot stays occupied until the cleanup ends, the cleanup does end
/// once the abort allowance elapses, and no completed values object is left in
/// the store.
#[tokio::test(flavor = "current_thread")]
async fn a_wedged_multipart_abort_is_bounded_and_leaves_no_object() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_MULTIPART_WEDGE, std::sync::atomic::Ordering::SeqCst);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            // Set past validation on purpose. The S3 minimum part size puts a
            // real multipart upload out of reach of any block a unit test can
            // encode in milliseconds; a small buffer capacity reaches the same
            // `put_multipart_opts`, `put_part` and `abort` calls at a block
            // size that costs nothing to build. The small row group is what
            // makes the writer push parts while it is still writing, so the
            // deadline finds it in the phase where an abort is attempted.
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the wedged upload takes a part", || {
                store.parts.load(std::sync::atomic::Ordering::SeqCst) > 0
            })
            .await;

            // Only the retry deadline has elapsed: the abort allowance is
            // untouched, so a decision that arrives now cannot have waited for
            // the cleanup.
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the deadline fails the block"
            );
            worker.complete(done);
            assert_eq!(
                worker.notify.outcomes()[Outcome::Storage as usize],
                1,
                "the block is nacked as retryable storage"
            );

            // The slot the cleanup holds is the same FLUSHING slot.
            worker.rotate();
            assert!(worker.flushing.is_none(), "no second write is started");
            assert!(worker.cleaning.is_some(), "the slot is still occupied");
            until("the wedged upload is aborted", || {
                store.aborts.load(std::sync::atomic::Ordering::SeqCst) > 0
            })
            .await;

            let mut job = worker.cleaning.take().expect("the occupied slot");
            sim.advance(Duration::from_secs(2));
            job.cleanup().await.expect("the cleanup is bounded");
            drop(job);

            let path = lake::sink::object_path(
                lake::schema::Dataset::LogsValues,
                lake::clock::PartitionId::from_unix_secs(0),
                0,
                &lake::sink::FileNaming::new(&worker.cfg.lake.writer_id),
                1,
            );
            assert!(
                store.inner.head(&path).await.is_err(),
                "a wedged upload never completes an object"
            );
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a values multipart upload is
/// wedged, so the abort its cancellation triggers never returns.
/// Guarantees: every request is decided and delivered before any waiting, and
/// the terminal return happens only once the supervising task has been
/// released -- within `upload.abort_timeout`, ended by an abort of the task
/// that is itself awaited.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_returns_only_once_the_flush_task_is_released() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_MULTIPART_WEDGE, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the wedged upload takes a part", || {
                store.parts.load(std::sync::atomic::Ordering::SeqCst) > 0
            })
            .await;
            worker.shutdown(clock::now());

            let cancel = worker
                .flushing
                .as_ref()
                .expect("a rotated block is flushing")
                .cancel
                .clone();
            assert!(
                !cancel.is_cancelled(),
                "nothing is cancelled before the decision is taken"
            );

            {
                let mut abandoning = std::pin::pin!(worker.abandon());
                assert!(
                    futures::poll!(&mut abandoning).is_pending(),
                    "a wedged upload is not released in one turn"
                );
                // No runtime turn has elapsed since that poll, so both of the
                // next two assertions describe the state at the moment the
                // decision phase suspended. The completion is already in the
                // channel, and the write has not yet been able to observe the
                // cancellation, whose store-visible effect is the abort.
                assert_eq!(
                    store.aborts.load(std::sync::atomic::Ordering::SeqCst),
                    0,
                    "the write is cancelled only after the decision is delivered"
                );
                let delivered = futures::poll!(std::pin::pin!(rx.recv()));
                match delivered {
                    std::task::Poll::Ready(Ok(PipelineCompletionMsg::DeliverNack { nack })) => {
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a delivered nack, got {other:?}"),
                }
                // The cancellation belongs to the same turn as the delivery it
                // follows, so it is never deferred past the bounded wait.
                assert!(
                    cancel.is_cancelled(),
                    "the write is cancelled in the turn that decided it"
                );
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                    assert!(
                        futures::poll!(&mut abandoning).is_pending(),
                        "the abort never returns, so the slot is not released"
                    );
                }
                assert!(
                    store.aborts.load(std::sync::atomic::Ordering::SeqCst) > 0,
                    "the abandoned upload is aborted rather than dropped"
                );

                sim.advance(Duration::from_secs(2));
                (&mut abandoning).await;
            }
            assert!(
                worker.is_idle(),
                "both slot holders are gone once the deadline returns"
            );
        })
        .await;
}

/// Build a real exporter inbox over local channels, with `capacity` pdata
/// slots.
///
/// The engine's own inbox is used rather than a stand-in, because the
/// behaviour under test is the engine's: a latched Shutdown is released only
/// after the buffered pdata has been force-drained past a closed admission
/// gate, and it is the inbox that decides when that happens.
fn inbox(
    capacity: usize,
) -> (
    mpsc::Sender<OtapPdata>,
    mpsc::Sender<NodeControlMsg<OtapPdata>>,
    ExporterInbox<OtapPdata>,
) {
    let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
    let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(capacity);
    let inbox = ExporterInbox::new(
        Receiver::Local(LocalReceiver::mpsc(control_rx)),
        Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
        7,
        Interests::empty(),
    );
    (pdata_tx, control_tx, inbox)
}

/// A worker configuration whose storage points at a directory that exists.
///
/// Only the tests that drive the real [`Exporter::start`] entry point need it:
/// `start` builds the configured object store before the test replaces it, and
/// a local file store refuses a base URI that is not there.
fn startable_config(requests: usize) -> Config {
    let mut cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": std::env::temp_dir().to_string_lossy()}},
        "window": {"interval": "1s", "max_requests_per_block": requests}
    }))
    .expect("valid config");
    cfg.lake.ingress.max_requests_per_block = requests;
    cfg
}

/// Scenario: the inbox already holds buffered pdata when a Shutdown is
/// latched, so the request is force-drained past a closed admission gate.
/// Guarantees: the forced message exposes the latched deadline before the
/// Shutdown control message is released, and the request is refused with a
/// retryable `NodeShutdown` nack that is never counted as a delivery failure.
#[tokio::test(flavor = "current_thread")]
async fn forced_pdata_exposes_shutdown_and_is_retryably_nacked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (pdata_tx, control_tx, mut inbox) = inbox(2);
            let deadline = clock::now() + Duration::from_secs(1);
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline,
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");

            // Admission is closed, yet the buffered request is still handed
            // over: that is the force-drain the exporter has to decide.
            let data = match inbox.recv_when(false).await.expect("a forced request") {
                Message::PData(data) => data,
                other => panic!("expected forced pdata, got {other:?}"),
            };
            assert_eq!(
                inbox.shutdown_deadline(),
                Some(deadline),
                "the latched deadline is visible before the control message is released"
            );

            let (handler, mut rx) = effects(1);
            let mut notify = Notifier::new(handler, 2);
            notify.force_shutdown(data);
            assert_eq!(
                notify.failures(),
                0,
                "a free completion slot takes the refusal immediately"
            );
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: the shutdown deadline elapses with a parked write holding the
/// FLUSHING slot, a populated ACTIVE block and one parked request.
/// Guarantees: the parked request is nacked the moment shutdown is latched,
/// every remaining uncommitted token of both blocks is nacked at the deadline,
/// and the worker is left holding nothing.
#[tokio::test(flavor = "current_thread")]
async fn deadline_nacks_both_blocks_and_pending() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(16);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut worker = Worker::new(cfg, store.clone(), wall.clone(), handler);

            // FLUSHING: one request, rotated into a write that never returns.
            worker.admit(logs_pdata());
            worker.rotate();
            store.entered.notified().await;
            // ACTIVE: one request in the block that is still open.
            worker.admit(logs_pdata());
            // Parked: a request whose admission window is later than the one
            // the ACTIVE block was opened for, and no block can be opened for
            // it while the flush slot is held.
            wall.set(1_000_000_000);
            worker.admit(logs_pdata());
            assert!(worker.pending.is_some());
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(
                worker
                    .flushing
                    .as_ref()
                    .expect("a rotated block is flushing")
                    .tokens
                    .len(),
                1
            );

            worker.shutdown(clock::now());
            assert!(
                worker.pending.is_none(),
                "the parked request is decided when shutdown is latched"
            );
            assert_eq!(worker.notify.len(), 1);

            let _ticker = ticking(&sim, Duration::from_millis(50));
            worker.abandon().await;

            for _ in 0..3 {
                match rx.recv().await.expect("a shutdown refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(!nack.permanent);
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
            assert!(worker.flushing.is_none());
            assert!(worker.cleaning.is_none());
            assert!(worker.is_idle(), "nothing is left undecided");
        })
        .await;
}

/// Scenario: shutdown is latched with one block already flushing and a second
/// block open, and storage recovers well before the deadline.
/// Guarantees: the node finishes the outstanding FLUSHING block, then rotates
/// and flushes the ACTIVE one, both requests are acknowledged only after their
/// files exist, and nothing is nacked.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_commits_both_blocks_before_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let cfg = worker_config();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::run(
                cfg,
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(metrics),
            ));

            // Four requests fill a block, so the node rotates it immediately
            // and its write parks.
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the first block enqueues");
            }
            store.entered.notified().await;
            // The fifth joins the block that replaced it; the flush slot is
            // busy, so that block is still ACTIVE when shutdown arrives.
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request of the second block enqueues");
            // The inbox serves control before pdata, so the shutdown below
            // would force-drain the fifth request rather than let it be
            // admitted. The marker is behind it in the same pdata channel, so
            // its completion proves the fifth request is already in a block.
            marker(&pdata_tx, &mut rx, 99).await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Releasing the latched Shutdown is what closing the upstream
            // pdata channel does, exactly as the engine does it.
            drop(pdata_tx);
            // Storage heals, so both blocks can reach object storage inside
            // the deadline.
            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_waiters();

            for _ in 0..5 {
                assert!(
                    matches!(
                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                            .await
                            .expect("a completion arrives")
                            .expect("a completion arrives"),
                        PipelineCompletionMsg::DeliverAck { .. }
                    ),
                    "a block that reached storage before the deadline is acknowledged"
                );
            }
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            let snapshots = terminal.metrics();
            // Five requests in two blocks, plus the row-less marker.
            assert_eq!(terminal_value(snapshots, "acks", &[]), 6);
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("reason", "shutdown")]),
                0
            );
            // One values file per block: the block shutdown found flushing and
            // the block it then rotated and flushed itself.
            let written: Vec<String> = store
                .writes
                .lock()
                .expect("writes lock")
                .iter()
                .map(|(path, _)| path.clone())
                .filter(|path| path.contains("dataset=values/"))
                .collect();
            assert_eq!(written.len(), 2, "both blocks reached storage: {written:?}");
            drop(control_tx);
        })
        .await;
}

/// Scenario: the real `SeriesParquet::start` future is aborted while a storage
/// write is parked and will never return.
/// Guarantees: cancellation drops the parked write and releases the block, the
/// sink and the object store within the configured abort timeout, so a node
/// torn down mid-flush leaves nothing owned by a task nobody joins.
#[tokio::test(flavor = "current_thread")]
async fn dropping_start_cancels_flush_task() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, _rx) = effects(2);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let mut cfg = startable_config(1);
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut exporter = super::SeriesParquet::new(cfg);
            exporter.store_override = Some(store.clone());
            let node = tokio::task::spawn_local(Box::new(exporter).start(inbox, handler));

            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            store.entered.notified().await;
            assert_eq!(
                store.parked.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the write is parked inside the store"
            );

            node.abort();
            match node.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("start was not aborted"),
            }
            until("the cancelled node releases the store", || {
                Arc::strong_count(&store) == 1
                    && store.parked.load(std::sync::atomic::Ordering::SeqCst) == 0
            })
            .await;
            assert_eq!(
                store.parked_drops.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the parked write future was dropped rather than leaked"
            );
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: a backlog of buffered requests is force-drained while the
/// completion channel is already full and nothing will ever read it.
/// Guarantees: every request still gets exactly one shutdown decision, each
/// undeliverable decision is counted as a delivery failure rather than parked,
/// and the node returns instead of waiting for completion credit.
#[tokio::test(flavor = "current_thread")]
async fn saturated_inbox_shutdown_stays_bounded() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (pdata_tx, control_tx, inbox) = inbox(32);
            let (handler, _completion_rx) = effects(1);
            // One delivered completion is enough to fill the channel, so every
            // decision the node takes from here on cannot be handed over.
            // Two slots, because the last one of a notifier is reserved for a
            // shutdown outcome and this priming completion is a normal one.
            let mut prime = Notifier::new(handler.clone(), 2);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::metrics::Metrics::register(&context, &cfg.lake);
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);

            for _ in 0..32 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("the inbox fills");
            }
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "saturated".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);

            let terminal = tokio::time::timeout(
                Duration::from_secs(2),
                super::run(
                    cfg,
                    store,
                    Arc::new(lake::clock::TestWallClock::new(0)),
                    inbox,
                    handler,
                    Some(metrics),
                ),
            )
            .await
            .expect("shutdown stays bounded with a full completion channel")
            .expect("the node succeeds");

            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("reason", "shutdown")]),
                32,
                "every force-drained request is decided exactly once"
            );
            assert_eq!(
                terminal_value(snapshots, "notify.failures", &[]),
                32,
                "every undeliverable decision is counted rather than dropped silently"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: window boundaries keep arriving while the one flush slot is held
/// by a write that never returns and the completion channel is already full,
/// and a telemetry control message and then a shutdown are sent into that.
/// Guarantees: the real node re-arms its boundary timer rather than losing or
/// spinning on it, still serves control messages, and observes the shutdown it
/// is then sent, counting the completion it could never hand over instead of
/// waiting for it.
#[tokio::test(flavor = "current_thread")]
async fn blocked_completion_keeps_boundary_and_control_live() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (pdata_tx, control_tx, inbox) = inbox(4);
            let (handler, _completion_rx) = effects(1);
            // Two slots, because the last one of a notifier is reserved for a
            // shutdown outcome and this priming completion is a normal one.
            let mut prime = Notifier::new(handler.clone(), 2);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::run(
                cfg,
                store.clone(),
                wall.clone(),
                inbox,
                handler,
                Some(metrics),
            ));

            // Boundaries keep arriving for the rest of the test, so the
            // request reaches a sealed block whichever turn the node admits it
            // on, and every later boundary is one the node meets with its
            // flush slot already held.
            let _ticker = ticking_windows(&sim, &wall);
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            store.entered.notified().await;

            let (samples, reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(64);
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter,
                })
                .await
                .expect("the telemetry control enqueues");
            until(
                "telemetry is served while everything else is blocked",
                || samples.try_recv().is_ok(),
            )
            .await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(1),
                    reason: "blocked completions".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_eq!(
                terminal_value(terminal.metrics(), "notify.failures", &[]),
                1,
                "the undeliverable decision is counted, not retried forever"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: the inbox latches a Shutdown it cannot release yet, because an
/// upstream sender is still alive, and hands the node a telemetry control
/// message while it drains.
/// Guarantees: the node takes the latched deadline from the inbox rather than
/// waiting for a Shutdown message it has not been given, so the block it holds
/// is sealed and acknowledged instead of waiting for a window boundary ten
/// minutes away.
#[tokio::test(flavor = "current_thread")]
async fn a_latched_deadline_starts_the_drain_before_the_shutdown_message() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(4);
            let mut cfg = worker_config();
            // Far enough away that nothing but the shutdown can seal a block.
            cfg.window.interval = Duration::from_secs(600);
            let node = tokio::task::spawn_local(super::run(
                cfg,
                store,
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));

            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            // Behind it in the same channel, so its completion proves the
            // request before it is already in the ACTIVE block.
            marker(&pdata_tx, &mut rx, 99).await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "latched".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // The inbox holds that message back while `pdata_tx` is alive, so
            // this is the only thing the node is handed after the latch.
            let (_samples, metrics_reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(8);
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry { metrics_reporter })
                .await
                .expect("the telemetry control enqueues");

            assert!(
                matches!(
                    tokio::time::timeout(Duration::from_secs(5), rx.recv())
                        .await
                        .expect("the held block is sealed without its window ending")
                        .expect("a completion arrives"),
                    PipelineCompletionMsg::DeliverAck { .. }
                ),
                "the block the node was holding reached storage"
            );
            node.abort();
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: the real `drive` loop is polled for the first time in a state
/// where its shutdown deadline has already elapsed and the flush task has
/// already published a successful result that nothing has taken yet. The
/// deadline branch is biased above the branch that awaits that result, so both
/// are ready in the same poll and the deadline wins it.
/// Guarantees: the block whose files exist is acknowledged and its descriptors
/// are committed, rather than refused as uncommitted work, so shutdown never
/// asks a producer to resend rows that are already in object storage.
#[tokio::test(flavor = "current_thread")]
async fn a_flush_ready_at_the_deadline_is_acknowledged_not_nacked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(4);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::metrics::Metrics::register(&context, &cfg.lake);
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(metrics);

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

            // The write parks, then is released, and the supervising task runs
            // to completion. The loop has not been created yet, so nothing can
            // take the result it publishes: that is the state the deadline
            // branch has to be able to win.
            store.entered.notified().await;
            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_waiters();
            until("the flush publishes its result", || {
                worker
                    .flushing
                    .as_ref()
                    .is_some_and(|job| job.task_finished())
            })
            .await;

            // An already-elapsed deadline, so the deadline sleep and the flush
            // result are both ready the first time the loop is polled.
            let elapsed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(elapsed);

            let terminal = super::drive(&mut worker, inbox)
                .await
                .expect("the loop returns at its deadline");
            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("reason", "shutdown")]),
                0,
                "a block whose files exist is never refused"
            );
            assert_eq!(terminal_value(snapshots, "acks", &[]), 1);

            assert!(
                worker.cache.is_committed(&id, partition),
                "a block decided by its own successful result still commits"
            );
            match rx.recv().await.expect("a completion") {
                PipelineCompletionMsg::DeliverAck { .. } => {}
                other => panic!("expected an ack for a durable block, got {other:?}"),
            }
            assert!(worker.is_idle(), "nothing is left holding a slot");
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: configuration contains misspelled settings or cross-field
/// violations.
/// Guarantees: the factory's typed validation rejects every one of them before
/// any network listener starts, so a pipeline never runs with a budget the
/// operator believes they set.
#[test]
fn startup_rejects_invalid_configuration() {
    let base = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-config"}}});
    let cases = [
        ("window", serde_json::json!({"interval": "0s"})),
        ("window", serde_json::json!({"interval": "500ms"})),
        ("window", serde_json::json!({"max_requests_per_block": 0})),
        ("window", serde_json::json!({"max_block_bytes": "1MiB"})),
        ("ingress", serde_json::json!({"max_row_bytes": "3MiB"})),
        ("ingress", serde_json::json!({"max_requset_bytes": "1MiB"})),
        ("upload", serde_json::json!({"concurrency": 0})),
        ("upload", serde_json::json!({"part_bytes": "1MiB"})),
        ("parquet", serde_json::json!({"compression": "snappy"})),
        (
            "metrics",
            serde_json::json!({"values_sort": ["no_such_column"]}),
        ),
        // `attrs` is a Map column of logs/values: present, but not a type the
        // Arrow row format can sort by.
        ("logs", serde_json::json!({"values_sort": ["attrs"]})),
        (
            "logs",
            serde_json::json!({"denormalize": [{"path": "resource.x", "column": "SERIES_ID"}]}),
        ),
        ("logs", serde_json::json!({"denormalize": ["unknown.x"]})),
        ("series_cache", serde_json::json!({"max_entries": 0})),
        ("notify_batch", serde_json::json!(0)),
    ];
    for (section, value) in cases {
        let mut candidate = base.clone();
        candidate[section] = value;
        assert!(
            serde_json::from_value::<Config>(candidate).is_err(),
            "section={section}"
        );
    }
}

/// Scenario: operators read the component README before configuring durable
/// ingest.
/// Guarantees: the documentation states the retry, shutdown, memory and
/// unsupported-signal contracts, and reproduces the exact River configuration
/// the end-to-end tests run, so the documented deployment cannot drift away
/// from the tested one.
#[test]
fn readme_states_operating_contract() {
    let readme = include_str!("README.md");
    for phrase in [
        "at-least-once",
        "wait_for_result",
        "timeout_secs=180",
        "core_allocation",
        "memory.unaccounted_rss_bytes",
        "exponential histograms",
        "union_by_name",
        "mergeSchema",
        "schema_fingerprint",
        "notify_batch",
        "No block-atomic snapshot",
        "Alloy",
        "MinIO",
        "ClickHouse",
        "loki.source.file",
        "e2e.source",
        "row_number()",
        "SERIES_REQUIRE_DOCKER",
    ] {
        assert!(
            readme.contains(phrase),
            "missing documented contract: {phrase}"
        );
    }
    let alloy = include_str!("../../../../../configs/series-parquet.alloy");
    assert!(
        readme.contains(alloy),
        "README must reproduce the exact tested River config"
    );
}
