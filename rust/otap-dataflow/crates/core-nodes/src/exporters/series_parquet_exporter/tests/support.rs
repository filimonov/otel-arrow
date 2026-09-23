// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared test support: request builders, the simulated clocks, the
//! gated and fault-injecting stores the store override takes, and the
//! event capture.

pub(super) use super::super::config::Config;

pub(super) use super::super::outcome::Outcome;

pub(super) use super::super::token::{AckToken, Notifier};

pub(super) use super::super::worker::{Prepared, Worker};

pub(super) use futures::stream::BoxStream;

pub(super) use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
};

pub(super) use otel_arrow_dfe_channel::mpsc;

pub(super) use otel_arrow_dfe_config::SignalType;

pub(super) use otel_arrow_dfe_engine::Interests;

pub(super) use otel_arrow_dfe_engine::clock;

pub(super) use otel_arrow_dfe_engine::control::NackCause;

pub(super) use otel_arrow_dfe_engine::control::NodeControlMsg;

pub(super) use otel_arrow_dfe_engine::control::{
    PipelineCompletionMsg, PipelineCompletionMsgReceiver, pipeline_completion_msg_channel,
};

pub(super) use otel_arrow_dfe_engine::local::exporter::EffectHandler;

pub(super) use otel_arrow_dfe_engine::local::exporter::Exporter;

pub(super) use otel_arrow_dfe_engine::local::message::LocalReceiver;

pub(super) use otel_arrow_dfe_engine::message::{ExporterInbox, Message, Receiver};

pub(super) use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_runtime_services};

pub(super) use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};

pub(super) use otel_arrow_dfe_pdata::OtapPayload;

pub(super) use otel_arrow_dfe_pdata::encode::{encode_logs_otap_batch, encode_metrics_otap_batch};

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, ResourceLogs, ScopeLogs,
};

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Summary, SummaryDataPoint,
    metric, number_data_point,
};

pub(super) use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{
    ResourceSpans, ScopeSpans, Span,
};

pub(super) use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;

pub(super) use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;

pub(super) use otel_arrow_dfe_series_lake as lake;

pub(super) use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;

pub(super) use std::sync::Arc;

pub(super) use std::time::Duration;

/// Build an exporter effect handler wired to a completion channel of exactly
/// `capacity` slots, so a test can saturate it deterministically.
pub(in super::super) fn effects(
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
pub(in super::super) fn empty_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, OtapPayload::empty(SignalType::Logs))
}

pub(super) fn encoded<M: prost::Message>(message: &M) -> Vec<u8> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes).expect("encodes");
    bytes
}

pub(super) fn logs_payload() -> OtapPayload {
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

pub(super) fn metrics_payload() -> OtapPayload {
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
pub(super) fn traces_payload() -> OtapPayload {
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
pub(super) fn mixed_metrics_payload() -> OtapPayload {
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

/// One metrics request whose only gauge point carries an exemplar.
pub(super) fn exemplar_metrics_pdata() -> OtapPdata {
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{Exemplar, exemplar};
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests".to_owned(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_789_960_500_000_000_000,
                            value: Some(number_data_point::Value::AsInt(1)),
                            exemplars: vec![Exemplar {
                                time_unix_nano: 1_789_960_500_000_000_000,
                                value: Some(exemplar::Value::AsInt(1)),
                                ..Default::default()
                            }],
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
    let mut context = Context::default();
    context.set_source_node(9);
    OtapPdata::new(
        context,
        OtapPayload::from(encode_metrics_otap_batch(&view).expect("encodes to OTAP")),
    )
}

/// One mixed metrics request that still carries a routing frame.
pub(super) fn mixed_metrics_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, mixed_metrics_payload())
}

/// Take one completion and assert it is a retryable shutdown refusal.
pub(super) async fn expect_shutdown_nack(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) {
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

/// A worker configuration with a one-second window and a small request bound,
/// so the completion credit arithmetic is exercised at a size a test can
/// reason about. The base URI is never used: these tests hand the worker an
/// in-memory object store directly.
pub(super) fn worker_config() -> Config {
    serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-unused"}},
        "window": {"interval": "1s", "max_requests_per_block": 4}
    }))
    .expect("valid config")
}

/// One well-formed logs request that still carries a routing frame.
pub(super) fn logs_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, logs_payload())
}

/// A worker configuration whose blocks hold at most `requests` requests, so a
/// third request has to wait for the next block.
pub(super) fn worker_config_with_requests(requests: usize) -> Config {
    let mut cfg = worker_config();
    cfg.window.max_requests_per_block = requests;
    cfg.lake.ingress.max_requests_per_block = requests;
    cfg
}

/// Take one completion, require it to be an ack, and say which request it
/// belonged to.
pub(super) async fn expect_ack(rx: &mut PipelineCompletionMsgReceiver<OtapPdata>) -> Option<usize> {
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
pub(super) fn logs_pdata_from(source: usize) -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(source);
    OtapPdata::new(context, logs_payload())
}

/// An object store whose writes park until a test opens its gate.
///
/// This is what keeps a block FLUSHING for as long as a test needs: no write
/// can resolve while the gate holds no permit, so the worker's one flush slot
/// stays taken across as many window boundaries as the test crosses. Opening
/// the gate leaves it open, because a permit is returned as soon as the write
/// that took it has passed.
#[derive(Debug)]
pub(super) struct GatedStore {
    /// The store that actually holds the objects.
    pub(super) inner: Arc<object_store::memory::InMemory>,
    /// Permits to write; empty until the test releases the parked flush.
    pub(super) gate: Arc<tokio::sync::Semaphore>,
    /// Writes that have reached the gate, counted before they park on it.
    ///
    /// This is the signal that a flush has actually started: a test waits for
    /// it instead of guessing that the node has got that far.
    pub(super) entered: Arc<std::sync::atomic::AtomicUsize>,
}

impl GatedStore {
    /// Announce a write, then wait for the gate.
    ///
    /// The count is raised before the wait, so a test observing it knows the
    /// write is parked rather than still to come.
    pub(super) async fn pass(&self) {
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
pub(super) async fn until(what: &str, mut condition: impl FnMut() -> bool) {
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
pub(super) async fn marker(
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
pub(super) async fn expect_completion(
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
) -> Option<usize> {
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
pub(super) async fn stored_files(store: &object_store::memory::InMemory) -> usize {
    use futures::StreamExt;
    store.list(None).count().await
}

/// Read one metric value out of a terminal handoff by name and labels.
///
/// Panics rather than returning an option: a test asking for a metric the
/// handoff does not carry has found the regression it was written for.
pub(super) fn terminal_value(
    snapshots: &[MetricSetSnapshot],
    name: &str,
    labels: &[(&str, &str)],
) -> u64 {
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

/// Injection mode: every request passes through to the inner store.
pub(super) const FAULT_NONE: u8 = 0;

/// Injection mode: every write of a `series` dataset file fails.
pub(super) const FAULT_SERIES: u8 = 1;

/// Injection mode: the first write of a `values` dataset file fails, and the
/// store heals itself in the same step so the retry succeeds.
pub(super) const FAULT_VALUES_ONCE: u8 = 2;

/// Injection mode: every write parks until the test releases it.
pub(super) const FAULT_PARK: u8 = 3;

/// Injection mode: a `values` multipart upload is initiated and then wedges --
/// its parts never land and its abort never returns -- while everything else
/// passes through.
pub(super) const FAULT_MULTIPART_WEDGE: u8 = 5;

/// Injection mode: every write spends [`SLOW_FAILURE`] of engine-clock time
/// and then fails, the way a cloud store's own retry loop answers a request
/// once its `retry_timeout` is spent.
pub(super) const FAULT_SLOW_FAIL: u8 = 6;

/// How long one write takes to fail under [`FAULT_SLOW_FAIL`].
pub(super) const SLOW_FAILURE: Duration = Duration::from_secs(20);

/// Injection mode: every write is refused as `PermissionDenied`, the way a
/// store answers credentials it does not accept.
pub(super) const FAULT_DENIED: u8 = 7;

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
pub(super) struct FaultStore {
    /// The store that actually holds whatever is allowed through.
    pub(super) inner: object_store::memory::InMemory,
    /// The active injection mode, one of the `FAULT_*` constants.
    pub(super) mode: std::sync::atomic::AtomicU8,
    /// Path and payload of every PUT, including the ones that then failed.
    pub(super) writes: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
    /// Raised as each write reaches the injection point, so a test can wait
    /// for a flush to have started rather than guess that it has.
    pub(super) entered: tokio::sync::Notify,
    /// Releases one parked write under `FAULT_PARK`.
    pub(super) release: tokio::sync::Notify,
    /// Writes currently parked inside the store under `FAULT_PARK`.
    pub(super) parked: std::sync::atomic::AtomicUsize,
    /// Parked writes whose future was dropped rather than released, which is
    /// what a cancelled node has to produce.
    pub(super) parked_drops: std::sync::atomic::AtomicUsize,
    /// Parts handed to a wedged multipart upload.
    pub(super) parts: Arc<std::sync::atomic::AtomicUsize>,
    /// Aborts attempted against a wedged multipart upload.
    pub(super) aborts: Arc<std::sync::atomic::AtomicUsize>,
}

/// A real multipart upload that is initiated and then never progresses.
///
/// The underlying store initiates it, so the destination genuinely holds an
/// unfinished upload; its parts then park and its abort never returns. That is
/// the shape that makes the cleanup bound matter: abandoning the upload
/// without an abort leaves a partial upload behind, and the abort is exactly
/// the call that may never come back.
#[derive(Debug)]
pub(super) struct WedgedUpload {
    /// The upload the underlying store really initiated, held so it is only
    /// released when this one is dropped.
    pub(super) _inner: Box<dyn MultipartUpload>,
    /// Parts handed to this upload, shared with the store the test holds.
    pub(super) parts: Arc<std::sync::atomic::AtomicUsize>,
    /// Aborts attempted against this upload.
    pub(super) aborts: Arc<std::sync::atomic::AtomicUsize>,
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
    pub(super) async fn before(&self, path: &object_store::path::Path) -> object_store::Result<()> {
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
        if mode == FAULT_DENIED {
            return Err(object_store::Error::PermissionDenied {
                path: path.to_string(),
                source: "injected: access denied".into(),
            });
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
pub(super) struct Ticker(tokio::task::JoinHandle<()>);

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
pub(super) fn ticking_windows(
    sim: &clock::SimClock,
    wall: &Arc<lake::clock::TestWallClock>,
) -> Ticker {
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
pub(super) fn ticking(sim: &clock::SimClock, step: Duration) -> Ticker {
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
pub(super) async fn drain_cleanup(worker: &mut Worker) {
    if let Some(mut job) = worker.cleaning.take() {
        job.cleanup().await.expect("the cleanup task joins");
    }
}

/// An S3 storage section with the default credential chain, for tests that
/// only load a configuration.
pub(super) fn s3_storage() -> serde_json::Value {
    serde_json::json!({"s3": {"base_uri": "s3://bucket/lake", "auth": {"type": "default"}}})
}

/// A recorded tracing field value, keeping the type it was recorded with.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum FieldValue {
    U64(u64),
    I64(i64),
    Bool(bool),
    Str(String),
    /// A value recorded through `Debug` or `Display` (`?x` or `%x`).
    Debug(String),
}

impl FieldValue {
    /// The text of a string, debug or display value.
    pub(super) fn text(&self) -> &str {
        match self {
            FieldValue::Str(s) | FieldValue::Debug(s) => s,
            _ => "",
        }
    }
}

/// One event recorded by [`capture`]: its level, name and typed fields.
#[derive(Debug, Clone)]
pub(super) struct CapturedEvent {
    pub(super) level: tracing::Level,
    pub(super) name: String,
    pub(super) fields: std::collections::BTreeMap<String, FieldValue>,
}

std::thread_local! {
    /// Events recorded on this thread while a [`Capture`] is alive.
    static CAPTURED: std::cell::RefCell<Option<Vec<CapturedEvent>>> =
        const { std::cell::RefCell::new(None) };
}

/// The process-wide test subscriber behind [`capture`].
///
/// Installed once, globally, rather than per test: a per-test scoped
/// subscriber races the global callsite cache, because a callsite that a
/// concurrently running test registers first is cached as uninteresting to a
/// subscriber it did not see yet. Every callsite is registered here as
/// `sometimes`, so `enabled` is asked per event, and it records only on a
/// thread that holds a [`Capture`] -- which is what keeps parallel tests out
/// of each other's records.
pub(super) struct GlobalCapture;

impl tracing::Subscriber for GlobalCapture {
    fn register_callsite(
        &self,
        _metadata: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }

    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        CAPTURED
            .try_with(|captured| captured.try_borrow().is_ok_and(|c| c.is_some()))
            .unwrap_or(false)
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Fields(std::collections::BTreeMap<String, FieldValue>);
        impl tracing::field::Visit for Fields {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::U64(value));
            }
            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::I64(value));
            }
            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::Bool(value));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                let _ = self
                    .0
                    .insert(field.name().to_owned(), FieldValue::Str(value.to_owned()));
            }
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let _ = self.0.insert(
                    field.name().to_owned(),
                    FieldValue::Debug(format!("{value:?}")),
                );
            }
        }
        let mut fields = Fields(std::collections::BTreeMap::new());
        event.record(&mut fields);
        let recorded = CapturedEvent {
            level: *event.metadata().level(),
            name: event.metadata().name().to_owned(),
            fields: fields.0,
        };
        let _ = CAPTURED.try_with(|captured| {
            if let Ok(mut captured) = captured.try_borrow_mut()
                && let Some(events) = captured.as_mut()
            {
                events.push(recorded);
            }
        });
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

/// Records the events of the current thread for as long as it lives.
pub(super) struct Capture;

impl Capture {
    /// Every event recorded so far with this name.
    pub(super) fn named(&self, name: &str) -> Vec<CapturedEvent> {
        CAPTURED.with(|captured| {
            captured
                .borrow()
                .iter()
                .flatten()
                .filter(|event| event.name == name)
                .cloned()
                .collect()
        })
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        CAPTURED.with(|captured| *captured.borrow_mut() = None);
    }
}

/// Start recording this thread's events through the global test subscriber.
pub(super) fn capture() -> Capture {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    let _ = INSTALLED.get_or_init(|| {
        tracing::subscriber::set_global_default(GlobalCapture)
            .expect("no other global subscriber in this test binary");
    });
    // Callsites registered by other tests before the install are re-asked,
    // and now answer `sometimes`.
    tracing::callsite::rebuild_interest_cache();
    CAPTURED.with(|captured| *captured.borrow_mut() = Some(Vec::new()));
    Capture
}

/// One logs request carrying `records` log records of a single series, so its
/// values file is large enough to span several multipart chunks.
pub(super) fn bulk_logs_pdata(records: usize) -> OtapPdata {
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

/// Build a real exporter inbox over local channels, with `capacity` pdata
/// slots.
///
/// The engine's own inbox is used rather than a stand-in, because the
/// behaviour under test is the engine's: a latched Shutdown is released only
/// after the buffered pdata has been force-drained past a closed admission
/// gate, and it is the inbox that decides when that happens.
pub(super) fn inbox(
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
pub(super) fn startable_config(requests: usize) -> Config {
    let mut cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": std::env::temp_dir().to_string_lossy()}},
        "window": {"interval": "1s", "max_requests_per_block": requests}
    }))
    .expect("valid config");
    cfg.lake.ingress.max_requests_per_block = requests;
    cfg
}
