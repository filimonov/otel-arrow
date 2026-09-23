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
//! A request the ACTIVE block cannot take is not refused for it. Its rows are
//! already extracted, so the extraction is parked, admission closes, and it is
//! offered to the next block before any newer request. At most one request is
//! parked, which is what keeps the worker to two blocks and one request.
//!
//! Rotation is driven by aligned wall-clock windows: a block covers one
//! window of the configured interval and is sealed when that window ends, so
//! the boundary-driven case writes one file set per window rather than one
//! per request. That is the normal case rather than a guarantee: a block that
//! fills its byte or request budget is sealed before its window ends, which
//! puts more than one file set in that window. The
//! waiting is done on the engine's monotonic clock rather than on an engine
//! periodic timer, which is cancelled before a node's receivers are drained;
//! see [`window`] for what that buys.

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_series_lake as lake;
use std::sync::Arc;
use std::time::Instant;

/// Registered component identifier.
pub const SERIES_PARQUET_EXPORTER_URN: &str = "urn:otel:exporter:series_parquet";

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = SERIES_PARQUET_EXPORTER_URN,
    target = "otel.exporter.series_parquet",
);

pub mod config;
mod flush;
mod metrics;
mod outcome;
#[cfg(test)]
mod tests;
mod token;
mod window;
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
    name: SERIES_PARQUET_EXPORTER_URN,
    create: |pipeline: PipelineContext,
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
        exporter.metrics = Some(metrics::Metrics::register(&pipeline, &exporter.config.lake));
        exporter.num_cores = pipeline.num_cores();
        exporter.token_provider = otel_arrow_dfe_otap::object_store::required_token_provider(
            &exporter.config.storage,
            capabilities,
        )?;
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
    /// Instruments registered by the factory, moved into the worker at start.
    metrics: Option<metrics::Metrics>,
    /// Cores the engine runs this pipeline on: one worker, and one memory
    /// budget, per core.
    num_cores: usize,
    /// Object store injected in place of the one the configuration names.
    ///
    /// The only seam a test uses to drive the real [`Exporter::start`] entry
    /// point: everything else about the node -- the inbox, the select loop,
    /// the worker and its flush tasks -- is the production path.
    #[cfg(test)]
    store_override: Option<Arc<dyn object_store::ObjectStore>>,
}

impl SeriesParquet {
    /// Construct an exporter from validated configuration.
    #[must_use]
    pub fn new(config: config::Config) -> Self {
        Self {
            config,
            token_provider: None,
            metrics: None,
            num_cores: 1,
            #[cfg(test)]
            store_override: None,
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
        let store = otel_arrow_dfe_otap::object_store::exporter_store(
            effects.exporter_id(),
            &self.config.storage,
            self.config.retry.as_ref(),
            self.token_provider.take(),
        )?;
        #[cfg(test)]
        let store = self.store_override.take().unwrap_or(store);
        let storage = self.config.storage.kind().to_owned();
        run_announced(
            self.config.clone(),
            store,
            Arc::new(lake::clock::SystemWallClock),
            inbox,
            effects,
            self.metrics.take(),
            Startup {
                storage,
                num_cores: self.num_cores,
            },
        )
        .await
    }
}

/// What the start event reports beyond the worker itself.
struct Startup {
    /// Storage backend name.
    storage: String,
    /// Workers the engine runs, one per core.
    num_cores: usize,
}

/// The grace the engine grants a signal-driven shutdown (SIGINT, SIGTERM).
///
/// A worker whose worst-case shutdown -- one window, then a flush and its
/// abort for each of the two blocks -- does not fit it is cut off before it
/// can decide what it holds.
const SIGNAL_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(60);

/// Physical memory of the host, in bytes, where the platform reports it.
fn physical_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kib.checked_mul(1024)
}

/// Emit the start event and the configuration warnings a worker can judge
/// at start-up.
///
/// One event carries everything an operator needs to find this worker's
/// files -- writer id, boot id, storage -- and the budget it runs under.
/// The warnings are conditions that do not stop the node but predict a
/// failure: every core's budget together exceeding physical memory, and a
/// worst-case shutdown longer than the signal shutdown grace.
fn announce(worker: &worker::Worker, startup: &Startup) {
    let budget = worker.budget_bytes();
    let total = budget.saturating_mul(startup.num_cores as u64);
    otel_info!(
        "series_parquet.start",
        writer_id = worker.cfg.lake.writer_id.as_str(),
        boot_id = worker.boot_id.as_str(),
        storage = startup.storage.as_str(),
        num_cores = startup.num_cores,
        memory_budget_bytes = budget
    );
    if let Some(physical) = physical_memory_bytes()
        && total > physical
    {
        otel_warn!(
            "series_parquet.memory_budget.oversubscribed",
            memory_budget_bytes = budget,
            num_cores = startup.num_cores,
            total_budget_bytes = total,
            physical_memory_bytes = physical,
            message = "every worker's memory budget together exceeds physical memory; lower \
                       the block and ingress budgets or run on fewer cores"
        );
    }
    let window = &worker.cfg.window;
    let bound = window.interval.saturating_add(
        window
            .flush_retry_deadline
            .saturating_add(worker.cfg.lake.upload.abort_timeout)
            .saturating_mul(2),
    );
    if bound > SIGNAL_SHUTDOWN_GRACE {
        otel_warn!(
            "series_parquet.shutdown.grace_exceeded",
            shutdown_bound = ?bound,
            signal_grace = ?SIGNAL_SHUTDOWN_GRACE,
            message = "window.interval + 2 * (window.flush_retry_deadline + \
                       upload.abort_timeout) exceeds the signal shutdown grace; a SIGTERM may \
                       cut off the last flush, so shut down through the admin API with a \
                       longer timeout"
        );
    }
}

/// Drive one worker until shutdown completes or its deadline elapses.
///
/// The branches are ordered: the shutdown deadline outranks everything, so a
/// node still cancels on time under a boundary that is always ready; then the
/// window boundary, a resolved flush, completion delivery, the release of a
/// decided block's flush slot, rotation, and only then a new message.
/// `accept` is false once the ACTIVE block is waiting to be rotated, which is
/// what turns a slow destination into backpressure on the channel rather than
/// a third block. Once shutdown has been latched the node keeps taking
/// force-drained pdata and refuses each one with a retryable `NodeShutdown`
/// nack: sent at once when the completion channel has room, and otherwise
/// queued in the notifier, which the loop keeps serving until the deadline,
/// as long as it leaves room for every completion the blocks still hold (see
/// `Notifier::force_shutdown`).
#[cfg(test)]
async fn run(
    cfg: config::Config,
    store: Arc<dyn object_store::ObjectStore>,
    wall: Arc<dyn lake::clock::WallClock>,
    inbox: ExporterInbox<OtapPdata>,
    effects: EffectHandler<OtapPdata>,
    metrics: Option<metrics::Metrics>,
) -> Result<TerminalState, Error> {
    run_announced(
        cfg,
        store,
        wall,
        inbox,
        effects,
        metrics,
        Startup {
            storage: "test".to_owned(),
            num_cores: 1,
        },
    )
    .await
}

/// [`run`], with what the start event reports beyond the worker itself.
async fn run_announced(
    cfg: config::Config,
    store: Arc<dyn object_store::ObjectStore>,
    wall: Arc<dyn lake::clock::WallClock>,
    inbox: ExporterInbox<OtapPdata>,
    effects: EffectHandler<OtapPdata>,
    metrics: Option<metrics::Metrics>,
    startup: Startup,
) -> Result<TerminalState, Error> {
    let mut worker = worker::Worker::new(cfg, store, wall, effects);
    worker.set_metrics(metrics);
    announce(&worker, &startup);
    drive(&mut worker, inbox).await
}

/// Emit the outcome of one shutdown: what the worker took in over its life,
/// how it was decided, and how long the drain took.
fn summarize(worker: &worker::Worker, since: Option<Instant>, deadline_exceeded: bool) {
    let outcomes = worker.notify.outcomes();
    let acked = outcomes[outcome::Outcome::Ack as usize];
    let nacked = outcomes.iter().sum::<u64>() - acked;
    let duration =
        since.map(|since| otel_arrow_dfe_engine::clock::now().saturating_duration_since(since));
    otel_info!(
        "series_parquet.shutdown.complete",
        accepted = worker.accepted,
        acked = acked,
        nacked = nacked,
        abandoned = worker.abandoned,
        deadline_exceeded = deadline_exceeded,
        duration = ?duration
    );
}

/// The select loop itself, over a worker the caller owns.
///
/// Split from [`run`] so a test can inspect the worker the loop drove -- what
/// it still holds, and how often it was asked to scan itself for telemetry --
/// after the loop has returned.
async fn drive(
    worker: &mut worker::Worker,
    mut inbox: ExporterInbox<OtapPdata>,
) -> Result<TerminalState, Error> {
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
    // When the Shutdown control message arrived, for the drain duration.
    let mut closed_at: Option<Instant> = None;
    let mut notify_turns = 0_usize;
    loop {
        if let Some(deadline) = closed
            && worker.is_idle()
        {
            // Sampled here rather than on every turn: the scan is linear in
            // the number of live requests, so a per-turn sample would cost a
            // block quadratic time. Telemetry is a collection-time and
            // terminal-time concern, and the counters that must not be missed
            // are recorded at the lifecycle transitions themselves.
            worker.sample_metrics();
            summarize(worker, closed_at, false);
            return Ok(TerminalState::new(deadline, worker.metric_snapshots()));
        }
        let accept = worker.accept();
        worker.observe_admission(accept);
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
                otel_warn!("series_parquet.shutdown.deadline_exceeded");
                worker.abandon().await;
                worker.sample_metrics();
                summarize(worker, closed_at, true);
                return Ok(TerminalState::new(
                    deadline.expect("the deadline branch only fires with a deadline"),
                    worker.metric_snapshots(),
                ));
            }

            // The window boundary is the rotation trigger, so it is served
            // before anything that could add to the block that is about to be
            // sealed. `wake` re-arms the sleep whether or not it rotated, so a
            // rotation the flush slot cannot take yet still keeps its timer.
            () = worker.window.sleep.as_mut(), if deadline.is_none() => {
                worker.wake_window();
                notify_turns = 0;
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
                // The decided block moves into the cleanup half of the same
                // FLUSHING slot, so no rotation can be served here: the one a
                // parked request waits for is served, with its resume in the
                // same turn, once the cleanup branch below frees the slot.
                worker.complete(done);
                notify_turns = 0;
            }

            // Bounded so a long completion backlog cannot starve the flush and
            // receive branches; `notify_batch` sends are taken before the loop
            // yields and reopens them.
            result = worker.notify.next(),
                if !worker.notify.is_empty() && notify_turns < worker.cfg.notify_batch => {
                if let Err(e) = result {
                    otel_warn!("series_parquet.notify.failed", error = %e);
                }
                notify_turns += 1;
            }

            // A decided block's supervising task still holds the flush slot
            // until it has released the write it was cancelling, so joining it
            // is what frees the slot for the next rotation. It owes no
            // completion, which is why it ranks below the branches that do.
            cleaned = async {
                match worker.cleaning.as_mut() {
                    Some(job) => job.cleanup().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Err(error) = cleaned {
                    otel_warn!("series_parquet.flush.cleanup_failed", error = %error);
                }
                let _ = worker.cleaning.take();
                if worker.rotation_requested {
                    worker.rotate();
                }
                worker.resume_pending();
                notify_turns = 0;
            }

            // Always ready when it is enabled, so a requested rotation happens
            // before the next message is taken. An empty ACTIVE block needs
            // no flush slot to be replaced.
            () = std::future::ready(()),
                if worker.rotation_requested
                    && (worker.active.data.is_empty()
                        || (worker.flushing.is_none() && worker.cleaning.is_none())) => {
                worker.rotate();
                worker.resume_pending();
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
                            worker.force_shutdown(data);
                        } else {
                            worker.admit(data);
                        }
                    }
                    Ok(Message::Control(NodeControlMsg::Shutdown { deadline, reason })) => {
                        otel_info!("series_parquet.shutdown", reason = reason);
                        closed = Some(deadline);
                        closed_at = Some(otel_arrow_dfe_engine::clock::now());
                        worker.shutdown(deadline);
                    }
                    Ok(Message::Control(NodeControlMsg::CollectTelemetry {
                        mut metrics_reporter,
                    })) => {
                        worker.sample_metrics();
                        worker.report_metrics(&mut metrics_reporter);
                    }
                    Ok(Message::Control(_)) => {}
                    Err(e) => {

                        // The inbox closes only when it releases the Shutdown
                        // it latched, so this is the channel failing rather
                        // than the node shutting down. Nothing more can be
                        // received and no deadline was granted, so everything
                        // still held is decided before the error is reported.
                        otel_warn!("series_parquet.inbox.failed", error = %e);
                        worker.abandon().await;
                        return Err(e.into());
                    }
                }
                // Read after every message the inbox hands over, not only
                // after the Shutdown control message: the engine latches the
                // deadline before it releases that message, and anything it
                // hands over in between -- a force-drained request, a
                // telemetry collection -- must not leave the worker believing
                // it still has all the time in the world. The error arm above
                // returns, so this runs only for a message that arrived.
                if let Some(deadline) = inbox.shutdown_deadline() {
                    worker.shutdown(deadline);
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
