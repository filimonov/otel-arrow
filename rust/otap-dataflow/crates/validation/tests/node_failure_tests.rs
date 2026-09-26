// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! What a producer sees when a node task dies while it holds a request whose
//! producer waits for the result.
//!
//! The pipeline runs through the real runtime: the OTLP receiver with
//! `wait_for_result: true` in front of an exporter that panics on its first
//! request. The test records a known limitation, not a wanted behaviour: a
//! held request is not nacked when a node fails (see the runtime recovery notes
//! in docs/configuration-model.md). When the runtime learns to nack held
//! requests after a node failure, this test changes with it.

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_config::observed_state::{ObservedStateSettings, SendPolicy};
use otel_arrow_dfe_config::pipeline::PipelineConfig;
use otel_arrow_dfe_config::policy::{ChannelCapacityPolicy, TelemetryPolicy};
use otel_arrow_dfe_config::{DeployedPipelineKey, PipelineGroupId, PipelineId};
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::{ControllerContext, PipelineContext};
use otel_arrow_dfe_engine::control::{
    NodeControlMsg, RuntimeControlMsg, pipeline_completion_msg_channel, runtime_ctrl_msg_channel,
};
use otel_arrow_dfe_engine::entity_context::set_pipeline_entity_key;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::testing::install_test_context_bindings;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::{OTAP_EXPORTER_FACTORIES, OTAP_PIPELINE_FACTORY};
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_dfe_pdata::proto::opentelemetry::collector::logs::v1::logs_service_client::LogsServiceClient;
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;
use std::sync::Arc;
use std::time::{Duration, Instant};
// Links the core nodes, so the OTLP receiver's factory is registered.
use otel_arrow_dfe_core_nodes as _;

const PANICKING_EXPORTER_URN: &str = "urn:otel:exporter:test_panicking";

/// An exporter that panics on the first request it receives, while it owns
/// that request and its completion.
struct PanickingExporter;

#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static PANICKING_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: PANICKING_EXPORTER_URN,
    create:
        |_pipeline: PipelineContext,
         node: NodeId,
         node_config: Arc<NodeUserConfig>,
         exporter_config: &ExporterConfig,
         _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
            Ok(ExporterWrapper::local(
                PanickingExporter,
                node,
                node_config,
                exporter_config,
            ))
        },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: |_| Ok(()),
};

#[async_trait(?Send)]
impl Exporter<OtapPdata> for PanickingExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: ExporterInbox<OtapPdata>,
        _effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    return Ok(TerminalState::default());
                }
                Message::PData(held) => {
                    let _held = held;
                    panic!("test exporter dies while holding a request");
                }
                Message::Control(_) => {}
            }
        }
    }
}

/// Run the pipeline of `yaml` on this thread until it returns; a watchdog
/// asks it to shut down after `watchdog` so a pipeline that survives the
/// failure cannot hang the test.
fn run_pipeline(yaml: &str, watchdog: Duration) -> Result<(), Error> {
    let group = PipelineGroupId::from("node-failure");
    let pipeline = PipelineId::from("main");
    let config = PipelineConfig::from_yaml(group.clone(), pipeline.clone(), yaml)
        .expect("the pipeline configuration is valid");
    let telemetry_system = InternalTelemetrySystem::default();
    let registry = telemetry_system.registry();
    let controller_ctx = ControllerContext::new(registry.clone());
    let mut pipeline_ctx =
        controller_ctx.pipeline_context_with(group.clone(), pipeline.clone(), 0, 1, 0);
    install_test_context_bindings(&mut pipeline_ctx, &OTAP_PIPELINE_FACTORY, config.clone())
        .expect("test context bindings compile");
    let entity_key = pipeline_ctx.register_pipeline_entity();
    let capacity = ChannelCapacityPolicy::default();
    let runtime_pipeline = OTAP_PIPELINE_FACTORY
        .build(
            pipeline_ctx.clone(),
            config,
            capacity.clone(),
            TelemetryPolicy::default(),
            std::collections::BTreeMap::new(),
            None,
            None,
        )
        .expect("the pipeline builds");
    let (runtime_ctrl_tx, runtime_ctrl_rx) = runtime_ctrl_msg_channel(capacity.control.pipeline);
    let (completion_tx, completion_rx) =
        pipeline_completion_msg_channel(capacity.control.completion);
    let watchdog_tx = runtime_ctrl_tx.clone();
    let _watchdog = std::thread::spawn(move || {
        std::thread::sleep(watchdog);
        let _ = watchdog_tx.try_send(RuntimeControlMsg::Shutdown {
            deadline: Instant::now() + Duration::from_secs(1),
            reason: "node failure test watchdog".to_owned(),
        });
    });
    let observed_state_store = ObservedStateStore::new(&ObservedStateSettings::default(), registry);
    let key = DeployedPipelineKey {
        pipeline_group_id: group,
        pipeline_id: pipeline,
        core_id: 0,
        deployment_generation: 0,
    };
    let _entity_guard = set_pipeline_entity_key(pipeline_ctx.metrics_registry(), entity_key);
    let (_memory_pressure_tx, memory_pressure_rx) = tokio::sync::watch::channel(
        otel_arrow_dfe_engine::memory_limiter::MemoryPressureChanged::initial(),
    );
    runtime_pipeline
        .run_forever(
            key,
            pipeline_ctx,
            observed_state_store.reporter(SendPolicy::default()),
            telemetry_system.reporter(),
            Duration::from_secs(1),
            memory_pressure_rx,
            runtime_ctrl_tx,
            runtime_ctrl_rx,
            completion_tx,
            completion_rx,
        )
        .map(|_| ())
}

/// What the producer's export call ended with.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// The export was acknowledged.
    Ok,
    /// The server answered with this gRPC status.
    Status(tonic::Code),
    /// The connection failed before any status arrived.
    ConnectionLost,
    /// No answer within the client deadline.
    TimedOut,
}

/// Send one OTLP logs export to `port` and wait at most `deadline` for its
/// answer, retrying the connection while the receiver starts.
fn export_one(port: u16, deadline: Duration) -> Answer {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a client runtime");
    runtime.block_on(async move {
        let endpoint = format!("http://127.0.0.1:{port}");
        let started = Instant::now();
        let mut client = loop {
            match LogsServiceClient::connect(endpoint.clone()).await {
                Ok(client) => break client,
                Err(error) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(10),
                        "the receiver never listened: {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_789_960_500_000_000_000,
                        event_name: "held".to_owned(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        match tokio::time::timeout(deadline, client.export(request)).await {
            Err(_) => Answer::TimedOut,
            Ok(Ok(_)) => Answer::Ok,
            Ok(Err(status)) => {
                let transport = std::error::Error::source(&status)
                    .is_some_and(|source| source.is::<tonic::transport::Error>());
                if transport {
                    Answer::ConnectionLost
                } else {
                    Answer::Status(status.code())
                }
            }
        }
    })
}

/// Scenario: an OTLP producer waits for the result of an export that an
/// exporter panics while holding, with no other node able to decide it.
/// Guarantees (a recorded limitation, not a wanted behaviour): the pipeline stops at once with the panic as a join error. No
/// node nacks the held request: the runtime drops every remaining node task,
/// the receiver among them, so the producer is not left waiting but sees its
/// connection fail before any gRPC status (tonic reports it as UNKNOWN with a
/// transport error; clients that map a lost connection to UNAVAILABLE retry
/// it).
#[test]
fn a_node_that_panics_while_holding_a_request_drops_its_producers_connection() {
    let port = otel_arrow_dfe_test_net::pick_unused_loopback_tcp_port();
    let yaml = format!(
        r#"
nodes:
  receiver:
    type: receiver:otlp
    config:
      protocols:
        grpc:
          listening_addr: "127.0.0.1:{port}"
          wait_for_result: true
          timeout: 30s
  exporter:
    type: "{PANICKING_EXPORTER_URN}"
    config: null
connections:
  - from: receiver
    to: exporter
"#
    );
    let pipeline = std::thread::spawn(move || run_pipeline(&yaml, Duration::from_secs(20)));

    let answer = export_one(port, Duration::from_secs(15));
    let result = pipeline.join().expect("the pipeline thread joins");

    assert!(
        matches!(result, Err(Error::JoinTaskError { is_panic: true, .. })),
        "{result:?}"
    );
    assert_eq!(answer, Answer::ConnectionLost);
}
