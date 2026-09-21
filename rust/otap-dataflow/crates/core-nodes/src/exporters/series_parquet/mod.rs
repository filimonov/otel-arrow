// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Series/values Parquet exporter with durable acknowledgements.
//!
//! The node owns exactly one ACTIVE block and at most one FLUSHING block. A
//! request is admitted to the ACTIVE block, and the completion it is owed is
//! held beside the block until the whole block has been written: an ack means
//! the request's rows are in object storage, and a failed write nacks every
//! request of the block as retryable. Admission closes once the ACTIVE block
//! is waiting to be rotated, which is what stops a third block from being
//! needed: a request may still join an empty ACTIVE block while the previous
//! one is being written, but nothing is admitted once that block is itself
//! waiting for the flush slot. A slow destination therefore becomes
//! backpressure rather than unbounded memory.
//!
//! Rotation timing is not yet the window timer: a block is sealed as soon as
//! it holds a request, so this still writes one file set per request. A later
//! task replaces the trigger with the aligned window clock without changing
//! what an ack means.

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind, format_error_sources};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::{OTAP_EXPORTER_FACTORIES, object_store::StorageType};
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot;
use std::sync::Arc;
use std::time::Instant;

/// Registered component identifier.
pub const SERIES_PARQUET_URN: &str = "urn:otel:exporter:series_parquet";

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = SERIES_PARQUET_URN,
    target = "otel.exporter.series_parquet",
);

pub mod config;
mod flush;
#[cfg(test)]
mod tests;
mod token;
mod worker;

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
        inbox: ExporterInbox<OtapPdata>,
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
        run(
            self.config.clone(),
            store,
            Arc::new(lake::clock::SystemWallClock),
            inbox,
            effects,
        )
        .await
    }
}

/// Drive one worker until shutdown completes or its deadline elapses.
///
/// The branches are ordered: the shutdown deadline outranks everything, then a
/// resolved flush, then completion delivery, then rotation, and only then a new
/// message. `accept` is false once the ACTIVE block is waiting to be rotated,
/// which is what turns a slow destination into backpressure on the channel
/// rather than a third block. Once shutdown has been latched the node
/// keeps taking force-drained pdata and refuses each one immediately with a
/// retryable `NodeShutdown` nack, spending the completion credit that was
/// reserved for exactly that.
async fn run(
    cfg: config::Config,
    store: Arc<dyn object_store::ObjectStore>,
    wall: Arc<dyn lake::clock::WallClock>,
    mut inbox: ExporterInbox<OtapPdata>,
    effects: EffectHandler<OtapPdata>,
) -> Result<TerminalState, Error> {
    let mut worker = worker::Worker::new(cfg, store, wall, effects);
    // The Shutdown control message is the end of the inbox, not the start of
    // the drain: the engine latches it, force-drains the pdata backlog past a
    // closed admission gate, and releases it only once the upstream channel is
    // empty and closed, closing the inbox in the same step. So this holds the
    // deadline it carried, and it is what says both that no further request
    // can arrive and that receiving again would fail rather than block.
    // Terminating on an idle worker alone would return while an upstream
    // sender is still alive, dropping whatever it sends next without a
    // decision.
    let mut closed: Option<Instant> = None;
    let mut notify_turns = 0_usize;
    loop {
        if let Some(deadline) = closed
            && worker.is_idle()
        {
            return Ok(TerminalState::new(
                deadline,
                std::iter::empty::<MetricSetSnapshot>(),
            ));
        }
        let accept = worker.accept();
        let deadline = worker.deadline;
        tokio::select! {
            biased;

            // The deadline outranks every other branch: whatever is still
            // outstanding is cancelled and decided rather than waited for.
            () = async {
                match deadline {
                    Some(d) => otel_arrow_dfe_engine::clock::sleep_until(d).await,
                    None => std::future::pending().await,
                }
            } => {
                otel_warn!("series_parquet.shutdown_deadline_elapsed");
                worker.abandon();
                return Ok(TerminalState::new(
                    deadline.expect("the deadline branch only fires with a deadline"),
                    std::iter::empty::<MetricSetSnapshot>(),
                ));
            }

            // A resolved flush is what turns a block's requests into
            // completions, so it is served before anything that could add to
            // the next block.
            done = async {
                match worker.flushing.as_mut() {
                    Some(job) => job.finish().await,
                    None => std::future::pending().await,
                }
            } => {
                worker.complete(done);
                notify_turns = 0;
            }

            // Bounded so a long completion backlog cannot starve the flush and
            // receive branches; `notify_batch` sends are taken before the loop
            // yields and reopens them.
            result = worker.notify.next(),
                if !worker.notify.is_empty() && notify_turns < worker.cfg.notify_batch => {
                if let Err(e) = result {
                    otel_warn!("series_parquet.notify_failed", error = %e);
                }
                notify_turns += 1;
            }

            // Always ready when it is enabled, so a requested rotation happens
            // before the next message is taken.
            () = std::future::ready(()),
                if worker.rotation_requested && worker.flushing.is_none() => {
                worker.rotate();
                notify_turns = 0;
            }

            message = inbox.recv_when(accept), if closed.is_none() => {
                notify_turns = 0;
                match message {
                    Ok(Message::PData(data)) => {
                        if let Some(d) = inbox.shutdown_deadline() {
                            // Force-drained: the node is past admission, so the
                            // request is refused immediately rather than parked.
                            worker.shutdown(d);
                            worker.notify.force_shutdown(data);
                        } else {
                            worker.admit(data);
                        }
                    }
                    Ok(Message::Control(NodeControlMsg::Shutdown { deadline, reason })) => {
                        otel_info!("series_parquet.shutdown", reason = reason);
                        closed = Some(deadline);
                        worker.shutdown(deadline);
                    }
                    Ok(Message::Control(_)) => {}
                    Err(e) => {
                        // The inbox closes only when it releases the Shutdown
                        // it latched, so this is the channel failing rather
                        // than the node shutting down. Nothing more can be
                        // received and no deadline was granted, so everything
                        // still held is decided before the error is reported.
                        otel_warn!("series_parquet.inbox_failed", error = %e);
                        worker.abandon();
                        return Err(e.into());
                    }
                }
            }

            // Reopens the notification branch after its batch, once every other
            // branch has had a turn.
            () = tokio::task::yield_now(), if notify_turns >= worker.cfg.notify_batch => {
                notify_turns = 0;
            }
        }
    }
}
