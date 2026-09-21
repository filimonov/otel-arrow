// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Series/values Parquet exporter with durable acknowledgements.
//!
//! This is the first runnable slice of the node: one request at a time is
//! extracted, admitted to a fresh block, sealed and flushed, and only then
//! acknowledged. That keeps the durability contract exact -- an OK response
//! means the request's rows are in object storage -- at the cost of one file
//! set per request. Later tasks replace this loop with bounded per-window
//! batching, an ACTIVE/FLUSHING pair and backpressure, without changing what
//! an ack means.

pub mod config;
#[cfg(test)]
mod tests;

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind, format_error_sources};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::{OTAP_EXPORTER_FACTORIES, object_store::StorageType};
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::TryIntoWithOptions;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use lake::clock::WallClock;

/// Registered component identifier.
pub const SERIES_PARQUET_URN: &str = "urn:otel:exporter:series_parquet";

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = SERIES_PARQUET_URN,
    target = "otel.exporter.series_parquet",
);

/// Declares the series Parquet exporter as a local exporter factory.
///
/// Unsafe code is temporarily used here to allow the use of the
/// `distributed_slice` macro. This macro is part of the `linkme` crate which is
/// considered safe and well maintained.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static SERIES_PARQUET: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SERIES_PARQUET_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: config::Config =
            serde_json::from_value(node_config.config.clone()).map_err(|e| {
                otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                    error: e.to_string(),
                }
            })?;
        let mut exporter = SeriesParquet::new(config);
        if exporter.config.storage.requires_bearer_token_provider() {
            exporter.token_provider = Some(
                capabilities
                    .require_shared::<BearerTokenProvider>()
                    .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                        error: e.to_string(),
                    })?,
            );
        }
        Ok(ExporterWrapper::local(
            exporter,
            node,
            node_config,
            exporter_config,
        ))
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<config::Config>,
};

/// One independently budgeted pipeline worker.
pub struct SeriesParquet {
    config: config::Config,
    token_provider: Option<
        Box<dyn otel_arrow_dfe_engine::shared::capability::auth::bearer_token_provider::BearerTokenProvider>,
    >,
}

impl SeriesParquet {
    /// Construct an exporter from validated configuration.
    #[must_use]
    pub fn new(config: config::Config) -> Self {
        Self {
            config,
            token_provider: None,
        }
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for SeriesParquet {
    async fn start(
        mut self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effects: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        if self.config.retry.is_some() && matches!(&self.config.storage, StorageType::File { .. }) {
            otel_warn!(
                "series_parquet.retry_ignored_for_file_storage",
                message = "retry settings are not applied to local file storage"
            );
        }
        let store =
            otel_arrow_dfe_otap::object_store::from_storage_type_with_retry_and_token_provider(
                &self.config.storage,
                self.config.retry.as_ref(),
                self.token_provider.take(),
            )
            .map_err(|e| Error::ExporterError {
                exporter: effects.exporter_id(),
                kind: ExporterErrorKind::Configuration,
                error: format!("error initializing object store {e}"),
                source_detail: format_error_sources(&e),
            })?;
        let sink = lake::sink::Sink::new(
            store,
            self.config.lake.clone(),
            lake::sink::FileNaming::new(&self.config.lake.writer_id),
        );
        let mut cache = lake::cache::SeriesCache::new(self.config.cache_entries);
        let wall = lake::clock::SystemWallClock;
        let mut seq = 0_u64;
        loop {
            match inbox.recv().await? {
                Message::PData(data) => {
                    let (context, mut payload) = data.into_parts();
                    let signal = payload.signal_type();
                    // `num_bytes` is an estimate of the wire representation, so
                    // the budget is also enforced on the measured extracted
                    // output inside `extract`.
                    let input_ok = payload
                        .num_bytes()
                        .is_some_and(|n| n <= self.config.lake.ingress.max_request_bytes);
                    let result: lake::Result<()> = async {
                        if !input_ok {
                            return Err(lake::Error::Refused(lake::RefuseReason::RequestTooLarge));
                        }
                        if signal != otel_arrow_dfe_config::SignalType::Logs {
                            return Err(lake::Error::Refused(lake::RefuseReason::Unsupported(
                                "signal".into(),
                            )));
                        }
                        let mut records: OtapArrowRecords = payload
                            .try_into_with_default()
                            .map_err(|e| lake::Error::Pdata(format!("{e}")))?;
                        let extracted = lake::extract::extract(&mut records, &self.config.lake)?;
                        drop(records);
                        if extracted.stats.rows == 0 {
                            return Ok(());
                        }
                        let secs = lake::clock::nanos_to_secs(wall.now_unix_nanos());
                        let clock =
                            lake::clock::WindowClock::new(self.config.window.interval, secs);
                        let mut block =
                            lake::buffer::Block::new(clock.last_boundary(), seq, &self.config.lake);
                        seq = seq
                            .checked_add(1)
                            .ok_or_else(|| lake::Error::invalid("sequence exhausted"))?;
                        let reservation =
                            block.reserve(&extracted, &mut cache, 0, &self.config.lake)?;
                        block.admit(extracted, reservation, ())?;
                        block.seal(lake::clock::nanos_to_micros(wall.now_unix_nanos()))?;
                        let _ = sink.write_block(&block, &CancellationToken::new()).await?;
                        // Only a flush that resolved marks the descriptors
                        // committed, so a failed flush re-emits them.
                        for id in &block.pending_series {
                            cache.mark_committed(*id, block.partition);
                        }
                        Ok(())
                    }
                    .await;
                    let data = OtapPdata::new(context, OtapPayload::empty(signal));
                    let delivered = match result {
                        Ok(()) => effects.notify_ack(AckMsg::new(data)).await,
                        Err(
                            e @ (lake::Error::Refused(_)
                            | lake::Error::Pdata(_)
                            | lake::Error::Arrow(_)),
                        ) => {
                            effects
                                .notify_nack(NackMsg::new_permanent_with_cause(
                                    e.to_string(),
                                    data,
                                    NackCause::Refused,
                                ))
                                .await
                        }
                        Err(e) => effects.notify_nack(NackMsg::new(e.to_string(), data)).await,
                    };
                    if let Err(e) = delivered {
                        otel_warn!("series_parquet.notify_failed", error = %e);
                    }
                }
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    // Every request that was acknowledged is already durable,
                    // so there is nothing buffered to flush on the way out.
                    return Ok(TerminalState::new(
                        deadline,
                        std::iter::empty::<MetricSetSnapshot>(),
                    ));
                }
                Message::Control(_) => {}
            }
        }
    }
}
