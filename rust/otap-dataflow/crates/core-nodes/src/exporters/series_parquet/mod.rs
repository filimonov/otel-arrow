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
mod token;

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

/// How a failed request must be reported back to its sender.
///
/// The distinction is the phase the failure came from, not the error type.
/// Validation -- the request budget, the signal check, `extract` and
/// `reserve` -- judges the request's own content, so the identical bytes will
/// be refused again and the client must change the request. Everything after
/// that point (`admit`, `seal`, and the Arrow, Parquet and object store work
/// inside `write_block`) is infrastructure: the same request may well succeed
/// on a retry, so it must not be reported as a client error.
#[derive(Debug)]
enum Failure {
    /// The request's content or size is refused; retrying is futile.
    Permanent(lake::Error),
    /// Writing the request failed; the sender may retry.
    Retryable(lake::Error),
}

impl Failure {
    /// The underlying lake error, whichever phase it came from.
    ///
    /// The error value is dropped together with the payload once the request
    /// is decided, so the call site logs it while the detail still exists.
    fn error(&self) -> &lake::Error {
        match self {
            Failure::Permanent(e) | Failure::Retryable(e) => e,
        }
    }

    /// The completion outcome this failure must be reported as.
    ///
    /// A permanent failure keeps the validation rule that rejected the
    /// request, because the sender can act on it; every retryable failure is
    /// reported as a storage outcome, which is not a client error.
    fn outcome(&self) -> token::Outcome {
        match self {
            Failure::Permanent(lake::Error::Refused(lake::RefuseReason::RequestTooLarge)) => {
                token::Outcome::TooLarge
            }
            Failure::Permanent(lake::Error::Refused(lake::RefuseReason::Unsupported(_))) => {
                token::Outcome::Unsupported
            }
            Failure::Permanent(_) => token::Outcome::Invalid,
            Failure::Retryable(_) => token::Outcome::Storage,
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
        // One credit per in-flight request in each of the two blocks a window
        // pair can hold. The loop below still admits one request at a time, so
        // it never reserves more than one credit; the bound is what later
        // batching will spend against.
        let mut notify = token::Notifier::new(
            effects.clone(),
            2 * self.config.window.max_requests_per_block,
        );
        loop {
            match inbox.recv().await? {
                Message::PData(data) => {
                    // The completion is retained across extraction and a
                    // storage round trip before it is handed back. The token
                    // keeps only the routing frames: the payload is returned
                    // here and the inbound credentials and the claims derived
                    // from them are dropped inside `split`, rather than
                    // staying resident for the duration of the write (spec
                    // section 7).
                    let (token, payload) = token::AckToken::split(data);
                    let outcome = match write_request(
                        payload,
                        &self.config,
                        &sink,
                        &mut cache,
                        &mut seq,
                        &wall,
                    )
                    .await
                    {
                        Ok(()) => token::Outcome::Ack,
                        Err(failure) => {
                            let outcome = failure.outcome();
                            // The error value is dropped with the payload, so
                            // it is reported here while the detail still
                            // exists; the completion carries only the outcome.
                            otel_warn!(
                                "series_parquet.request_failed",
                                outcome = outcome.reason(),
                                error = %failure.error()
                            );
                            outcome
                        }
                    };
                    notify.push(token, outcome);
                    if let Err(e) = notify.next().await {
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

/// Extract one request, write it and report which phase any failure came from.
///
/// Validation runs first and its refusals are [`Failure::Permanent`]: the
/// request budget, the signal check, `extract` and `reserve` all judge the
/// request's own content. A payload that cannot be decoded into OTAP records
/// is counted as validation too, because the same bytes will not decode on a
/// retry either. From `admit` onwards every failure is [`Failure::Retryable`],
/// including the Arrow, Parquet and object store errors raised inside
/// `write_block`, so a full disk or an unreachable bucket is never reported to
/// the client as a request it must change.
async fn write_request(
    mut payload: OtapPayload,
    config: &config::Config,
    sink: &lake::sink::Sink,
    cache: &mut lake::cache::SeriesCache,
    seq: &mut u64,
    wall: &impl WallClock,
) -> Result<(), Failure> {
    let signal = payload.signal_type();
    // `num_bytes` is an estimate of the wire representation, so the budget is
    // also enforced on the measured extracted output inside `extract`.
    if !payload
        .num_bytes()
        .is_some_and(|n| n <= config.lake.ingress.max_request_bytes)
    {
        return Err(Failure::Permanent(lake::Error::Refused(
            lake::RefuseReason::RequestTooLarge,
        )));
    }
    if signal != otel_arrow_dfe_config::SignalType::Logs {
        return Err(Failure::Permanent(lake::Error::Refused(
            lake::RefuseReason::Unsupported("signal".into()),
        )));
    }
    let mut records: OtapArrowRecords = payload
        .try_into_with_default()
        .map_err(|e| Failure::Permanent(lake::Error::invalid(format!("undecodable pdata: {e}"))))?;
    let extracted =
        lake::extract::extract(&mut records, &config.lake).map_err(Failure::Permanent)?;
    drop(records);
    if extracted.stats.rows == 0 {
        return Ok(());
    }
    let secs = lake::clock::nanos_to_secs(wall.now_unix_nanos());
    let clock = lake::clock::WindowClock::new(config.window.interval, secs);
    let mut block = lake::buffer::Block::new(clock.last_boundary(), *seq, &config.lake);
    // Reserving is still validation: it refuses a request too large for any
    // block. The sequence only advances once a block exists to consume it.
    let reservation = block
        .reserve(&extracted, cache, 0, &config.lake)
        .map_err(Failure::Permanent)?;
    *seq = seq
        .checked_add(1)
        .ok_or_else(|| Failure::Retryable(lake::Error::invalid("sequence exhausted")))?;
    block
        .admit(extracted, reservation, ())
        .map_err(Failure::Retryable)?;
    block
        .seal(lake::clock::nanos_to_micros(wall.now_unix_nanos()))
        .map_err(Failure::Retryable)?;
    let _ = sink
        .write_block(&block, &CancellationToken::new())
        .await
        .map_err(Failure::Retryable)?;
    // Only a flush that resolved marks the descriptors committed, so a failed
    // flush re-emits them.
    for id in &block.pending_series {
        cache.mark_committed(*id, block.partition);
    }
    Ok(())
}
