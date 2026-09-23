// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! One ACTIVE block, at most one FLUSHING block, and the decisions they owe.
//!
//! The worker holds exactly one block open for admission and at most one block
//! being written. A request is admitted to the ACTIVE block only once
//! `Block::reserve` has accepted it, so a refusal never leaves the block
//! mutated. Rotation seals the ACTIVE block and hands it to a
//! [`FlushJob`](super::flush::FlushJob) together with every completion it
//! owes; nothing is acknowledged until that write has returned success and the
//! descriptors it carried have been marked committed against the flushed
//! block's own partition. A failed write nacks every completion of the block
//! as retryable, because the rows are not in object storage and the sender is
//! the only party that still has them.
//!
//! There is no third block: while a flush is outstanding the node closes pdata
//! admission instead of opening another ACTIVE block, so the memory a worker
//! can hold is bounded by the two blocks and the completions in flight.
//!
//! An OTLP request is checked for protobuf wire framing before it is
//! converted, because the shared byte views decode lazily and report no error
//! for a damaged body: without the check a truncated request would become a
//! request carrying fewer rows than it holds, or none, and be acknowledged as
//! stored. The check follows the OTLP schema into every nested message --
//! resource, scope, record or metric, data point, exemplar, attribute and
//! value -- so damage at any depth refuses the whole request; strings are not
//! checked for UTF-8 there. Nesting deeper than any accepted
//! `ingress.max_nesting_depth` is refused by the same walk, before the
//! conversion's recursive value encoder runs.
//!
//! Logs and metrics are admitted through the same state machine and the same
//! single extraction call; traces have no lake schema and are refused on the
//! signal alone. A metrics request whose points the lake has no dataset for --
//! exponential histograms and summaries -- is decided by the configured
//! `unsupported` policy inside that one extraction call, atomically for the
//! whole request: `reject` refuses it, `drop` keeps the supported points and
//! counts the rest. Exemplars, which no dataset stores, are dropped and
//! counted unless `metrics.exemplars: reject` asks for the request to be
//! refused. Metadata and exemplar attribute tables are neither read nor
//! validated, under any policy.
//!
//! A block-scoped refusal -- a full block, or one already holding its request
//! limit -- is not the request's fault, so the request is not nacked for it.
//! Its extraction is parked in `pending`, admission closes until a block
//! opens, and the parked request is reserved against that block before any
//! newer one. Exactly one request is ever parked, so the bound above becomes
//! two blocks, one request and the completions in flight.

use super::config::Config;
use super::flush::{self, FlushDone, FlushJob};
use super::metrics::{
    DatasetAttrs, EmitAttrs, EmitReason, FlushAttrs, FlushReason, Metrics, NackAttrs,
};
use super::outcome::{self, BlockWriteFailed, Outcome};
use super::token::{AckToken, Notifier};
use super::window::Window;
use lake::buffer::Block;
use lake::cache::SeriesCache;
use lake::clock::{WallClock, nanos_to_micros, nanos_to_secs};
use lake::config::LakeConfig;
use lake::extract::Extracted;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::engine_metrics::SeriesMemoryAccounting;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes, PayloadData, TryIntoWithOptions};
use otel_arrow_dfe_series_lake as lake;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

// The framing walk bounds value nesting on its own; it must never refuse a
// body that the deepest accepted `ingress.max_nesting_depth` would accept.
const _: () = assert!(
    lake::config::MAX_NESTING_DEPTH
        <= otel_arrow_dfe_pdata::views::otlp::bytes::validate::MAX_ANY_VALUE_NESTING_DEPTH
);

/// Bytes charged per bounded descriptor cache entry.
///
/// The cache stores a sixteen-byte series id and an optional partition id per
/// entry inside an LRU whose nodes carry their own links; this is the flat
/// per-entry charge the memory budget is written against.
const CACHE_ENTRY_BYTES: u64 = 128;

/// Allocator, runtime and library overhead a worker is allowed beyond the
/// buffers it accounts for itself.
const FIXED_WORKSPACE_BYTES: u64 = 64 * 1024 * 1024;

/// Shortest interval between two per-request refusal WARN lines.
const REFUSAL_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Rate limit of the per-request refusal WARN.
///
/// A producer that keeps sending a request this node refuses would otherwise
/// write one WARN line per request, at whatever rate it sends. Every refusal
/// is still counted in the `nacks` metric; the log keeps one line per
/// interval and says how many it left out.
#[derive(Debug, Default)]
pub(super) struct RefusalLog {
    /// When the last line was written.
    last: Option<Instant>,
    /// Refusals left out since then.
    suppressed: u64,
}

impl RefusalLog {
    /// Whether a refusal seen at `now` is logged, and if it is, how many were
    /// left out since the previous line.
    pub(super) fn admit(&mut self, now: Instant) -> Option<u64> {
        if self
            .last
            .is_some_and(|last| now.saturating_duration_since(last) < REFUSAL_LOG_INTERVAL)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

/// Open and closed history of pdata admission, for telemetry.
///
/// The node loop reports the gate once per turn; only a change of state reads
/// the clock, so an open gate costs one comparison per turn.
#[derive(Debug, Default)]
pub(super) struct AdmissionGate {
    /// When the current closure began, while admission is closed.
    closed_since: Option<Instant>,
    /// Transitions from open to closed.
    closures: u64,
    /// Time spent closed in closures that have ended.
    closed_total: std::time::Duration,
}

impl AdmissionGate {
    /// Record whether admission is open on this loop turn.
    pub(super) fn observe(&mut self, open: bool) {
        match (open, self.closed_since) {
            (false, None) => {
                self.closed_since = Some(clock::now());
                self.closures += 1;
            }
            (true, Some(since)) => {
                self.closed_total += clock::now().saturating_duration_since(since);
                self.closed_since = None;
            }
            _ => {}
        }
    }

    /// Whether admission is closed at `now`, how many closures began, and
    /// the seconds spent closed, the closure in progress included.
    pub(super) fn sample(&self, now: Instant) -> (bool, u64, f64) {
        let current = self
            .closed_since
            .map_or(std::time::Duration::ZERO, |since| {
                now.saturating_duration_since(since)
            });
        (
            self.closed_since.is_some(),
            self.closures,
            (self.closed_total + current).as_secs_f64(),
        )
    }
}

/// A block together with the completions of every request admitted to it.
///
/// The block itself only counts its requests: the completions are kept beside
/// it so a flush task can own the data without owning the routing contexts.
pub(super) struct OwnedBlock {
    /// Rows, descriptors and accounting.
    pub(super) data: Block,
    /// One completion per admitted request, in admission order.
    pub(super) tokens: Vec<AckToken>,
    /// Whether this block must repeat descriptors the cache already reports
    /// committed in its partition.
    ///
    /// True only for the block that replaces one sealed by a byte or request
    /// threshold inside the same aligned window: that block's descriptors are
    /// not durable yet, so this one cannot assume them (spec 7.4).
    pub(super) reemit: bool,
    /// Descriptor rows admitted to this block, per [`EmitReason`] position.
    ///
    /// Carried with the block and reported only after a successful write, so
    /// an abandoned block credits nothing.
    pub(super) emitted: [u64; 3],
}

/// One extracted request waiting for a block that can take it.
///
/// This is what the worker holds instead of the request: the payload and the
/// record batches the conversion produced are already gone, so parking costs
/// the extracted rows and the completion and nothing else.
pub(super) struct Pending {
    /// The rows the request contributed.
    pub(super) extracted: Extracted,
    /// The completion the request is still owed.
    pub(super) token: AckToken,
    /// Wall-clock second the request was prepared at, for window alignment.
    pub(super) admission_secs: i64,
}

/// What preparing one request produced.
///
/// Preparation always decides the request: either its rows are ready to be
/// offered to a block, or it has failed validation and owes its sender a
/// refusal. This is an enum rather than a `Result` because neither arm is
/// propagated -- both are handled at the single call site -- and because a
/// refusal is a normal outcome of admission, not an error the worker reports.
pub(super) enum Prepared {
    /// The request's rows, ready to be offered to a block.
    Ready(Pending),
    /// The request failed and its completion is still owed; the error's
    /// [`Outcome`] decides how.
    Failed(AckToken, lake::Error),
}

/// The ACTIVE and FLUSHING pair of one exporter instance.
pub(super) struct Worker {
    /// Validated user configuration.
    pub(super) cfg: Config,
    /// The lake configuration every block of this worker is opened under.
    lake_cfg: Arc<LakeConfig>,
    /// The one block open for admission.
    pub(super) active: OwnedBlock,
    /// The one block being written, if any.
    pub(super) flushing: Option<FlushJob>,
    /// The decided block whose supervising task is still releasing what it
    /// owns.
    ///
    /// This is the same FLUSHING slot, not a third block: it holds no
    /// completions and no rows the node still owes anything for, only the task
    /// that has to finish cancelling and aborting an abandoned write before
    /// another block may be written to the same file names.
    pub(super) cleaning: Option<FlushJob>,
    /// The one extracted request no block could take yet.
    pub(super) pending: Option<Pending>,
    /// Descriptors already written for a partition.
    pub(super) cache: SeriesCache,
    /// Delivery of decided completions.
    pub(super) notify: Notifier,
    /// Whether the ACTIVE block should be sealed at the next opportunity.
    pub(super) rotation_requested: bool,
    /// The shutdown deadline, once one has been latched.
    pub(super) deadline: Option<Instant>,
    /// The aligned window the ACTIVE block belongs to, and its boundary sleep.
    pub(super) window: Window,
    /// Source of wall-clock time for window alignment and seal stamps.
    wall: Arc<dyn WallClock>,
    /// Shared with each flush task for the duration of its write.
    pub(super) sink: Rc<lake::sink::Sink>,
    /// Per-worker block sequence, used in file names.
    seq: u64,
    /// What will have asked for the next rotation.
    pub(super) reason: FlushReason,
    /// Largest completion token the worker has held, for capacity reporting.
    pub(super) token_high_water: usize,
    /// How many times the worker has scanned its state for telemetry.
    ///
    /// The scan walks both token vectors and the notification queue, so it is
    /// linear in the number of live requests; a node that sampled it once per
    /// loop turn would spend quadratic time per block. This counter exists so
    /// a regression test can assert the scan happens only when telemetry is
    /// actually collected.
    pub(super) samples: u64,
    /// Rate limit of the per-request refusal WARN.
    refusals: RefusalLog,
    /// Whether pdata admission is open, and how long it has been closed.
    pub(super) admission: AdmissionGate,
    /// Random id of this worker's incarnation, the file-name segment that
    /// keeps two runs of one `writer_id` apart.
    pub(super) boot_id: String,
    /// Requests handed to [`Worker::admit`], whatever became of them.
    pub(super) accepted: u64,
    /// Completions decided by [`Worker::abandon`] at the shutdown deadline.
    pub(super) abandoned: u64,
    /// Registered instruments, once the node has a pipeline context.
    ///
    /// `None` for a worker driven directly by a test, which keeps every call
    /// site a no-op rather than requiring a registry.
    pub(super) metrics: Option<Metrics>,
    /// This worker's share of the process-wide series exporter memory total.
    ///
    /// Held for the worker's whole life so the engine monitor can subtract what
    /// the exporters account for from the one process RSS sample it already
    /// takes; dropping the worker withdraws its bytes and its registration.
    pub(super) accounting: SeriesMemoryAccounting,
}

impl Worker {
    /// Build a worker with an empty ACTIVE block aligned to the current
    /// window.
    pub(super) fn new(
        cfg: Config,
        store: Arc<dyn object_store::ObjectStore>,
        wall: Arc<dyn WallClock>,
        effects: EffectHandler<OtapPdata>,
    ) -> Self {
        let window = Window::new(cfg.window.interval, Arc::clone(&wall));
        let lake_cfg = Arc::new(cfg.lake.clone());
        let active = OwnedBlock {
            data: Block::new(window.clock.last_boundary(), 0, Arc::clone(&lake_cfg)),
            tokens: Vec::new(),
            reemit: false,
            emitted: [0; 3],
        };
        // The sink bounds its abort on the engine clock like every other wait
        // of this node, so a simulated clock governs it too.
        let naming = lake::sink::FileNaming::new(&cfg.lake.writer_id);
        let boot_id = naming.boot_id.clone();
        let sink = Rc::new(
            lake::sink::Sink::new(store, cfg.lake.clone(), naming).with_clock(
                lake::sink::SinkClock {
                    now: clock::now,
                    sleep_until: clock::sleep_until,
                },
            ),
        );
        // One credit per in-flight request in each of the two blocks a window
        // pair can hold. Admission stops one short of it, so the last slot is
        // always free for a force-drained refusal once shutdown is latched.
        let notify = Notifier::new(effects, 2 * cfg.window.max_requests_per_block);
        let cache = SeriesCache::new(cfg.cache_entries);
        Self {
            cfg,
            lake_cfg,
            active,
            flushing: None,
            cleaning: None,
            pending: None,
            cache,
            notify,
            rotation_requested: false,
            deadline: None,
            window,
            wall,
            sink,
            seq: 1,
            reason: FlushReason::Time,
            token_high_water: size_of::<AckToken>(),
            samples: 0,
            refusals: RefusalLog::default(),
            admission: AdmissionGate::default(),
            boot_id,
            accepted: 0,
            abandoned: 0,
            metrics: None,
            accounting: SeriesMemoryAccounting::register(),
        }
    }

    /// Completions the worker still owes, wherever they currently sit.
    ///
    /// A token moves from the ACTIVE block to the FLUSHING one and from there
    /// into the notifier, so the bound that keeps the notifier from
    /// overflowing has to count all three places at once.
    pub(super) fn live_tokens(&self) -> usize {
        self.active.tokens.len()
            + self.flushing.as_ref().map_or(0, |job| job.tokens.len())
            + self.notify.len()
            + usize::from(self.pending.is_some())
    }

    /// Whether one more request may be admitted.
    ///
    /// Admission stops while a rotation is pending, because the request would
    /// land in a block that is about to be sealed and the flush slot is
    /// already spoken for; it stops for good once shutdown has been latched;
    /// and it stops whenever admitting one more request could leave the
    /// notifier without the credit it needs to decide it.
    pub(super) fn accept(&self) -> bool {
        self.deadline.is_none()
            && !self.rotation_requested
            && self.pending.is_none()
            && self.notify.has_credit(self.live_tokens())
    }

    /// Validate one request, extract its rows and release its payload.
    ///
    /// The returned value is what the worker may have to hold until the next
    /// block opens, so nothing of the request's own representation survives
    /// the call: the pdata is consumed, the transport frames and claims are
    /// dropped inside [`AckToken::split`], and the conversion records are
    /// dropped here. Only the extracted rows and the completion remain.
    ///
    /// Nothing in here touches a block, so a request that fails validation
    /// leaves the ACTIVE block exactly as it was.
    pub(super) fn prepare(&self, data: OtapPdata) -> Prepared {
        // The completion is retained across a storage round trip before it is
        // handed back, so the token keeps only the routing frames: the payload
        // is taken out here and the inbound credentials and the claims derived
        // from them are dropped inside `split`, rather than staying resident
        // for the duration of the write (spec section 7).
        let (token, mut payload) = AckToken::split(data);
        // `num_bytes` is an estimate of the wire representation, so the budget
        // is also enforced on the measured extracted output inside `extract`.
        let bytes = payload.num_bytes();
        if !bytes.is_some_and(|n| n <= self.cfg.lake.ingress.max_request_bytes) {
            return Prepared::Failed(
                token,
                lake::Error::Refused(lake::RefuseReason::RequestTooLarge(lake::Excess {
                    budget: lake::SizeBudget::Request,
                    observed: bytes,
                    limit: self.cfg.lake.ingress.max_request_bytes,
                })),
            );
        }
        // Logs and metrics share one admission path; traces have no lake
        // schema at all, so they are refused on the signal alone, before any
        // conversion. The `unsupported` policy governs unsupported metric
        // points inside a request the lake does have a schema for, so it does
        // not apply here.
        if payload.signal_type() == otel_arrow_dfe_config::SignalType::Traces {
            return Prepared::Failed(
                token,
                lake::Error::Refused(lake::RefuseReason::Unsupported("traces".into())),
            );
        }
        if let Err(error) =
            Self::check_wire_format(&payload, self.cfg.lake.ingress.max_nesting_depth)
        {
            return Prepared::Failed(token, error);
        }
        let extracted = match self.extract(payload) {
            Ok(extracted) => extracted,
            Err(error) => return Prepared::Failed(token, error),
        };
        Prepared::Ready(Pending {
            extracted,
            token,
            admission_secs: nanos_to_secs(self.wall.now_unix_nanos()),
        })
    }

    /// Refuse an OTLP body whose protobuf framing is broken at any depth.
    ///
    /// The shared OTLP byte views are deliberately non-validating: the
    /// conversion this node performs reads them lazily and reports no error for
    /// a truncated or corrupt body, so without this check a damaged request
    /// would be converted into a request carrying fewer rows than it holds, or
    /// none, and acknowledged as if it had been stored. The framing walk
    /// follows the OTLP schema into every nested message in one linear pass
    /// with no allocation -- see the module documentation for what that covers.
    /// A body nesting values deeper than the walk's own bound is deeper than
    /// any `ingress.max_nesting_depth` too, so it is refused as that limit
    /// refuses it, `max_nesting_depth` being the configured one.
    ///
    /// A payload that already holds Arrow records has no wire framing to check;
    /// it is validated by the conversion and the extraction instead.
    fn check_wire_format(payload: &OtapPayload, max_nesting_depth: usize) -> lake::Result<()> {
        let PayloadData::OtlpBytes(bytes) = payload.data() else {
            return Ok(());
        };
        let signal = match bytes {
            OtlpProtoBytes::ExportLogsRequest(_) => "logs",
            OtlpProtoBytes::ExportMetricsRequest(_) => "metrics",
            // Traces are decided by the signal check before this runs, so
            // there is no body to walk here.
            OtlpProtoBytes::ExportTracesRequest(_) => return Ok(()),
        };
        bytes.validate_framing().map_err(|error| match error {
            otel_arrow_dfe_pdata::error::Error::OtlpNestingTooDeep { .. } => {
                lake::Error::Refused(lake::RefuseReason::TooDeep(max_nesting_depth))
            }
            error => lake::Error::invalid(format!("malformed OTLP {signal} body: {error}")),
        })
    }

    /// Convert one payload and extract its rows, dropping the conversion.
    ///
    /// Split out so the record batches the conversion produced go out of scope
    /// with the call rather than living as long as the extraction does.
    fn extract(&self, payload: OtapPayload) -> lake::Result<Extracted> {
        let mut records: OtapArrowRecords = payload
            .try_into_with_default()
            .map_err(|e| lake::Error::invalid(format!("undecodable pdata: {e}")))?;
        lake::extract::extract(&mut records, &self.cfg.lake)
    }

    /// Take ownership of one request's completion and try to admit its rows.
    ///
    /// Every path ends with the request decided, its completion held by the
    /// ACTIVE block, or the whole extraction parked; none leaves it without an
    /// owner.
    pub(super) fn admit(&mut self, data: OtapPdata) {
        self.accepted += 1;
        match self.prepare(data) {
            Prepared::Ready(pending) => {
                self.token_high_water = self.token_high_water.max(pending.token.bytes());
                // Extraction outcomes are recorded exactly once, here: a
                // request that is parked and later resumed goes through
                // `offer` again but never through `prepare`, so nothing it
                // reported can be counted twice.
                if let Some(metrics) = &mut self.metrics {
                    metrics.extracted(&pending.extracted.stats);
                }
                self.offer(pending);
            }
            Prepared::Failed(token, error) => {
                self.token_high_water = self.token_high_water.max(token.bytes());
                self.refuse(token, &error);
            }
        }
    }

    /// Offer one prepared request to the ACTIVE block.
    ///
    /// A request that carries no rows is acknowledged without touching a
    /// block. Otherwise the reservation runs first, so the block is mutated
    /// only once it has accepted the request. A block-scoped refusal parks the
    /// extraction and asks for a rotation instead of refusing the sender: the
    /// same rows will fit the next block. Only one request is ever parked,
    /// because admission closes while one is waiting.
    pub(super) fn offer(&mut self, pending: Pending) {
        if pending.extracted.stats.rows == 0 {
            self.notify.push(pending.token, Outcome::Ack);
            return;
        }
        // A request that arrived after the ACTIVE block's window ended belongs
        // to the next block, not to the one still in hand.
        if self.window.admission_boundary(pending.admission_secs)
            > self.active.data.window_start_secs
        {
            self.reason = FlushReason::Time;
            self.park(pending);
            return;
        }
        let token_bytes = pending.token.bytes();
        let reservation = match self.active.data.reserve_with_reemit(
            &pending.extracted,
            &mut self.cache,
            token_bytes,
            self.active.reemit,
        ) {
            Ok(reservation) => reservation,
            // The block-scoped refusals judge whichever block happened to
            // be active, so the request waits for the next one. An empty
            // block cannot refuse this way -- a request it does not fit is
            // `RequestTooLarge` -- so parking here can never become an
            // endless rotation; the guard makes that a checked fact rather
            // than an inference about `reserve`.
            Err(lake::Error::Refused(
                reason @ (lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests),
            )) if !self.active.data.is_empty() => {
                self.reason = match reason {
                    lake::RefuseReason::BlockFull => FlushReason::Bytes,
                    _ => FlushReason::Requests,
                };
                self.park(pending);
                return;
            }
            Err(error) => {
                // The reservation refused before the block was touched, so
                // only this request is affected. Only `RequestTooLarge` judges
                // the request itself; a block-scoped refusal from an empty
                // block, or anything else `reserve` returns, is a broken
                // invariant, which is how [`Outcome::of`] reports it.
                self.refuse(pending.token, &error);
                return;
            }
        };
        // Classified before the descriptors are consumed by `admit`. The
        // reservation has already touched every series id, so an id absent
        // from the cache before this request now reads as present but
        // uncommitted, which is exactly `New`.
        let mut emitted = [0_u64; 3];
        for &index in &reservation.new_series {
            let id = pending.extracted.descriptors[index].series_id;
            let position = if self.active.reemit {
                EmitReason::Rotation as usize
            } else if self.cache.last_committed(&id).is_some() {
                EmitReason::Partition as usize
            } else {
                EmitReason::New as usize
            };
            emitted[position] += 1;
        }
        match self.active.data.admit(pending.extracted, reservation) {
            Ok(()) => {
                self.active.tokens.push(pending.token);
                for (total, count) in self.active.emitted.iter_mut().zip(emitted) {
                    *total += count;
                }
                // The window boundary is the normal rotation trigger; these
                // two only bring it forward, so a burst is written as soon as
                // it has filled a block rather than held until the boundary.
                // A rotation that was already owed stays owed: an admission
                // cannot cancel a boundary that has been consumed.
                if self.active.data.bytes >= self.cfg.window.max_block_bytes {
                    self.reason = FlushReason::Bytes;
                    self.rotation_requested = true;
                } else if self.active.data.request_count() >= self.cfg.window.max_requests_per_block
                {
                    self.reason = FlushReason::Requests;
                    self.rotation_requested = true;
                }
            }
            Err(error) => {
                // A failed admission leaves the block partially updated by
                // contract, so the whole ACTIVE block is failed rather than
                // written.
                self.refuse(pending.token, &error);
                self.fail_active(Outcome::Storage);
            }
        }
    }

    /// Park the one request the ACTIVE block could not take.
    fn park(&mut self, pending: Pending) {
        assert!(
            self.pending.is_none(),
            "admission closes while a request is parked"
        );
        self.pending = Some(pending);
        self.rotation_requested = true;
    }

    /// Offer the parked request to the block that has just opened.
    ///
    /// A no-op while a rotation is still owed, so the request is never offered
    /// to a block that is about to be sealed, and once shutdown has been
    /// latched, because [`Worker::shutdown`] has already decided it. The
    /// reservation is recomputed here against the new block's partition and
    /// the cache as it now stands, so a descriptor the failed block carried is
    /// written again by this one.
    pub(super) fn resume_pending(&mut self) {
        if self.rotation_requested || self.deadline.is_some() {
            return;
        }
        if let Some(pending) = self.pending.take() {
            self.offer(pending);
        }
    }

    /// Report a decided failure and release its completion.
    ///
    /// The error value is dropped with the payload, so it is logged here while
    /// the detail still exists; the completion carries the outcome and the
    /// reason sentence. The line names the signal and, for a size refusal,
    /// the setting that refused it, the observed size and the limit; it is
    /// rate limited (see [`RefusalLog`]).
    fn refuse(&mut self, token: AckToken, error: &lake::Error) {
        let outcome = Outcome::of(error);
        let sentence = Outcome::explain(error);
        if let Some(suppressed) = self.refusals.admit(clock::now()) {
            // Sizes are recorded as numbers, and an absent one is not
            // recorded at all, so telemetry never has to parse them back out
            // of a string.
            let excess = outcome::excess(error);
            let setting = excess.map(|(setting, _, _)| setting);
            let observed = excess
                .and_then(|(_, observed, _)| observed)
                .map(|n| n as u64);
            let limit = excess.map(|(_, _, limit)| limit as u64);
            otel_warn!(
                "series_parquet.request.failed",
                outcome = outcome.label(),
                signal = ?token.signal(),
                limit_setting = setting,
                observed_bytes = observed,
                limit_bytes = limit,
                reason = %sentence,
                error = %error,
                suppressed = suppressed
            );
        }
        self.notify.push_with(token, outcome, Some(sentence.into()));
    }

    /// An empty block for the current window, after the one in hand.
    ///
    /// The window start never moves backwards, so a wall clock that steps back
    /// cannot make a later block claim an earlier partition.
    ///
    /// A parked request also pulls the start forward to its own window, and it
    /// does so through the same floor: parking one consumed its boundary, so
    /// the window clock cannot report anything earlier afterwards. Without
    /// that, a wall clock that steps back between parking a request and opening
    /// the next block would produce a block the parked request is once again
    /// too late for: it would be parked again, rotated again, and the node
    /// would spin opening empty blocks. The parked request is the reason this
    /// block is being opened, so the block is opened for its window.
    fn new_active(&mut self) -> OwnedBlock {
        let secs = nanos_to_secs(self.wall.now_unix_nanos());
        let start = self
            .window
            .admission_boundary(secs)
            .max(self.active.data.window_start_secs);
        let seq = self.seq;
        // A worker would have to seal one block per nanosecond for six hundred
        // years to reach this.
        self.seq = self.seq.saturating_add(1);
        // A block opened for the window the sealed one already covered cannot
        // assume that block's descriptors are durable: it writes its own
        // copies. A byte or request threshold opens one, and so does a window
        // rotated on monotonic time while the wall clock has stepped back
        // (see `window`), which keeps the floored start.
        let reemit = start == self.active.data.window_start_secs
            && (matches!(self.reason, FlushReason::Bytes | FlushReason::Requests)
                || (self.reason == FlushReason::Time && self.window.floored));
        OwnedBlock {
            data: Block::new(start, seq, Arc::clone(&self.lake_cfg)),
            tokens: Vec::new(),
            reemit,
            emitted: [0; 3],
        }
    }

    /// Discard the ACTIVE block and decide every completion it held.
    pub(super) fn fail_active(&mut self, outcome: Outcome) {
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        for token in old.tokens {
            self.notify.push(token, outcome);
        }
    }

    /// Seal the ACTIVE block and start writing it.
    ///
    /// An empty ACTIVE block writes nothing, so it is replaced at once,
    /// whatever holds the flush slot: waiting for an unrelated write to finish
    /// would only keep admission closed across a window boundary. Otherwise
    /// this is a no-op while the flush slot is taken -- by a write, or by the
    /// cleanup of one that has already been decided: the rotation stays
    /// requested and is served once the slot frees, which is what keeps the
    /// worker to two blocks and keeps two writes off the same file names.
    pub(super) fn rotate(&mut self) {
        if self.active.data.is_empty() {
            self.rotation_requested = false;
            // A parked request may be waiting for a later window than the one
            // this empty block was opened for, so the block is replaced rather
            // than kept; otherwise the resume would park it again.
            self.active = self.new_active();
            return;
        }
        if self.flushing.is_some() || self.cleaning.is_some() {
            return;
        }
        self.rotation_requested = false;
        if let Err(error) = self
            .active
            .data
            .seal(nanos_to_micros(self.wall.now_unix_nanos()))
        {
            if let Some(metrics) = &mut self.metrics {
                metrics.worker.flush_failures.add(1);
            }
            otel_warn!("series_parquet.seal.failed", error = %error);
            self.fail_active(Outcome::Storage);
            return;
        }
        // Counted here rather than on every rotation call: an empty block is
        // replaced without a write, so it is not a flush.
        if let Some(metrics) = &mut self.metrics {
            metrics
                .flush
                .with(FlushAttrs {
                    reason: self.reason,
                })
                .count
                .add(1);
        }
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        self.flushing = Some(FlushJob::new(
            old.data,
            old.tokens,
            self.sink.clone(),
            old.emitted,
            self.cfg.window.flush_retry_deadline,
            self.cfg.lake.upload.abort_timeout,
        ));
    }

    /// The outcome a block whose flush did not succeed must be reported as.
    ///
    /// Both are retryable: the rows never reached object storage, so the
    /// sender still holds the only copy.
    fn failed_outcome(&self) -> Outcome {
        if self.deadline.is_some_and(|d| clock::now() >= d) {
            Outcome::Shutdown
        } else {
            Outcome::Storage
        }
    }

    /// Decide the FLUSHING block from the result of its write.
    ///
    /// The cache is marked committed only here, only on success, and only
    /// against the partition of the block that was actually written, so a
    /// descriptor is treated as written exactly when its file exists.
    pub(super) fn complete(
        &mut self,
        done: Result<FlushDone, tokio::sync::oneshot::error::RecvError>,
    ) {
        // One FLUSHING slot: a decided block still unwinding holds it, so no
        // new write can have started, and no second result can arrive.
        debug_assert!(
            self.cleaning.is_none(),
            "a flush completes only while the slot has no cleanup"
        );
        let Some(mut job) = self.flushing.take() else {
            return;
        };
        if let Some(metrics) = &mut self.metrics {
            metrics.worker.flush_duration.record(
                clock::now()
                    .saturating_duration_since(job.started)
                    .as_secs_f64(),
            );
        }
        // The sentence every request of a failed block is told, shared: the
        // block failed once, for one reason.
        let mut reason: Option<Rc<str>> = None;
        let outcome = match &done {
            Ok(finished) => {
                if let Some(metrics) = &mut self.metrics {
                    metrics
                        .worker
                        .flush_retries
                        .add(finished.attempts.saturating_sub(1));
                }
                match &finished.result {
                    Ok(report) => {
                        for id in &finished.data.pending_series {
                            self.cache.mark_committed(*id, finished.data.partition);
                        }
                        // The only place series rows are credited: the write
                        // returned success, so the file the descriptors are in
                        // exists.
                        if let Some(metrics) = &mut self.metrics {
                            for (reason, count) in
                                [EmitReason::New, EmitReason::Partition, EmitReason::Rotation]
                                    .into_iter()
                                    .zip(job.emitted)
                            {
                                if count != 0 {
                                    metrics
                                        .emitted
                                        .with(EmitAttrs { reason })
                                        .series_emitted
                                        .add(count);
                                }
                            }
                            for (dataset, _, rows) in &report.files {
                                let bucket = metrics.written.with(DatasetAttrs::from(*dataset));
                                bucket.rows_written.add(*rows as u64);
                                bucket.files_written.add(1);
                            }
                        }
                        // Every object of a block shares one file name and
                        // differs only in its dataset directory, so the first
                        // path and the count name the whole set.
                        let path = report
                            .files
                            .first()
                            .map_or_else(String::new, |(_, path, _)| path.to_string());
                        otel_info!(
                            "series_parquet.block.committed",
                            window_start = job.window_start_secs,
                            seq = job.seq,
                            path = path,
                            files = report.files.len(),
                            requests = job.tokens.len(),
                            bytes = job.bytes,
                            attempts = finished.attempts,
                            duration = ?clock::now().saturating_duration_since(job.started)
                        );
                        Outcome::Ack
                    }
                    Err(error) => {
                        if let Some(metrics) = &mut self.metrics {
                            metrics.worker.flush_failures.add(1);
                            if error.is_cancelled() {
                                metrics.worker.flush_cancelled.add(1);
                            }
                        }
                        otel_error!(
                            "series_parquet.flush.failed",
                            error = %error,
                            attempts = finished.attempts,
                            message = "Block failed before durable completion"
                        );
                        reason = Some(Rc::from(BlockWriteFailed(error).to_string()));
                        self.failed_outcome()
                    }
                }
            }
            Err(error) => {
                if let Some(metrics) = &mut self.metrics {
                    metrics.worker.flush_failures.add(1);
                }
                otel_warn!("series_parquet.flush.task_failed", error = %error);
                self.failed_outcome()
            }
        };
        // A block decided at the shutdown deadline is told so, whatever the
        // write was doing at the time.
        if outcome != Outcome::Storage {
            reason = None;
        }
        for token in std::mem::take(&mut job.tokens) {
            self.notify.push_with(token, outcome, reason.clone());
        }
        // The job keeps the FLUSHING slot until its task has released the
        // block, the sink handle and the write future it owns. It owes no
        // completion from here on, so nothing a producer waits for is held by
        // it; what it holds back is the next write to the same file names.
        self.cleaning = Some(job);
    }

    /// Bytes this worker's configuration allows it to hold: two blocks, one
    /// parked extraction, a full descriptor cache, a token per request slot,
    /// and the sort, merge, writer, upload and conversion workspaces a flush
    /// may allocate. Those workspace terms are engineering reservations
    /// rather than measurements.
    ///
    /// Every term is derived from unbounded configuration values, so the
    /// arithmetic saturates rather than overflowing: a budget written to mean
    /// "no practical limit" reports `u64::MAX`, not a panic.
    pub(super) fn budget_bytes(&self) -> u64 {
        let token = self.token_high_water.max(self.notify.token_high_water()) as u64;
        let cfg = &self.cfg.lake;
        let bytes = |n: usize| n as u64;
        let sort = bytes(cfg.sorting.run_target_bytes).saturating_mul(2);
        let merge = bytes(cfg.sorting.merge_chunk_bytes).saturating_mul(2);
        let writer = bytes(cfg.parquet.writer_limit_bytes).saturating_mul(3);
        let upload = bytes(cfg.upload.part_bytes)
            .saturating_mul(bytes(cfg.upload.concurrency).saturating_add(1))
            .saturating_add(bytes(cfg.sorting.merge_chunk_bytes));
        let conversion = bytes(cfg.ingress.max_request_bytes).saturating_mul(4);
        [
            bytes(cfg.ingress.max_block_bytes).saturating_mul(2),
            bytes(cfg.ingress.max_extracted_bytes),
            bytes(self.cfg.cache_entries).saturating_mul(CACHE_ENTRY_BYTES),
            bytes(cfg.ingress.max_requests_per_block)
                .saturating_mul(2)
                .saturating_mul(token),
            sort,
            merge,
            writer,
            upload,
            conversion,
            FIXED_WORKSPACE_BYTES,
        ]
        .into_iter()
        .fold(0_u64, u64::saturating_add)
    }

    /// Install the node's registered instruments.
    ///
    /// The shared `exporter.exports` set moves into the notifier, which is
    /// where every request is decided.
    pub(super) fn set_metrics(&mut self, mut metrics: Option<Metrics>) {
        self.notify.exports = metrics.as_mut().and_then(Metrics::take_exports);
        self.metrics = metrics;
    }

    /// Hand every instrument to the collector on a `CollectTelemetry`.
    pub(super) fn report_metrics(
        &mut self,
        reporter: &mut otel_arrow_dfe_telemetry::reporter::MetricsReporter,
    ) {
        if let Some(exports) = &mut self.notify.exports {
            let _ = reporter.report_measurement(exports);
        }
        if let Some(metrics) = &mut self.metrics {
            metrics.report(reporter);
        }
    }

    /// Take every instrument for terminal handoff.
    pub(super) fn metric_snapshots(
        &mut self,
    ) -> Vec<otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot> {
        let mut out = self
            .notify
            .exports
            .as_mut()
            .map_or_else(Vec::new, |exports| exports.terminal_snapshots());
        if let Some(metrics) = &mut self.metrics {
            out.extend(metrics.snapshots());
        }
        out
    }

    /// Record whether pdata admission is open on this loop turn.
    ///
    /// A gate closed by the latched shutdown is not backpressure, so it ends
    /// any closure in progress rather than starting one.
    pub(super) fn observe_admission(&mut self, accept: bool) {
        self.admission.observe(accept || self.deadline.is_some());
    }

    /// Publish everything the worker can be asked about right now.
    ///
    /// Called on `CollectTelemetry` and before every terminal snapshot, so the
    /// gauges describe the state the node is actually in at the moment it is
    /// asked rather than the state it was in when something last happened.
    ///
    /// The accounted total is exactly what the worker retains: the ACTIVE
    /// block, the FLUSHING block with its merge keys and its live flush
    /// workspace, the one parked request, the completions the notifier
    /// holds, the bounded descriptor cache, and the spare capacity of the two
    /// token vectors. The budget is the same shape derived from
    /// configuration, plus the sort, merge, writer, upload and conversion
    /// workspaces a flush may allocate. Those workspace terms are engineering
    /// reservations rather than measurements; validating them empirically is
    /// plan 3's work.
    pub(super) fn sample_metrics(&mut self) {
        // A worker with no registered instruments -- every worker a test
        // drives directly -- would compute the whole sample only to discard
        // it, including the linear scan over the live tokens.
        if self.metrics.is_none() {
            return;
        }
        self.samples += 1;
        // Both slot holders are the one FLUSHING block: a decided block is
        // still accounted for until its supervising task has released it.
        let flushing = self
            .flushing
            .iter()
            .chain(self.cleaning.iter())
            .map(|job| job.bytes)
            .sum::<usize>();
        let pending = self.pending.as_ref().map_or(0, |parked| {
            parked.extracted.pinned_bytes
                + parked.extracted.shared_bytes
                + parked
                    .extracted
                    .descriptors
                    .iter()
                    .map(|descriptor| descriptor.approx_bytes)
                    .sum::<usize>()
                + parked.token.bytes()
        });
        let cache = (self.cache.len() as u64).saturating_mul(CACHE_ENTRY_BYTES);
        let spare_tokens = self
            .active
            .tokens
            .capacity()
            .saturating_sub(self.active.tokens.len())
            + self
                .flushing
                .iter()
                .chain(self.cleaning.iter())
                .map(|job| job.tokens.capacity().saturating_sub(job.tokens.len()))
                .sum::<usize>();
        // The flushing block's merge also holds the encoded sort key of
        // every row of the table it is writing, for as long as that table's
        // write lasts; the sink reports those bytes and they are charged here.
        let merge_keys = self.sink.merge_key_bytes();
        // And its merge chunk, encoder buffers and unacknowledged upload
        // bytes, read live: the write returns to this loop between bounded
        // steps, so a sample taken during a flush sees what it holds now.
        let workspace = self.sink.flush_workspace_bytes();
        let accounted = self.active.data.bytes as u64
            + flushing as u64
            + merge_keys as u64
            + workspace as u64
            + pending as u64
            + cache
            + self.notify.bytes() as u64
            + (spare_tokens * size_of::<AckToken>()) as u64;
        self.accounting.set(accounted);
        let budget = self.budget_bytes();
        let oldest = self
            .active
            .tokens
            .iter()
            .map(AckToken::received)
            .chain(
                self.flushing
                    .iter()
                    .flat_map(|job| job.tokens.iter().map(AckToken::received)),
            )
            .chain(self.pending.iter().map(|parked| parked.token.received()))
            .chain(self.notify.oldest())
            .min();
        let requests = self.live_tokens() as u64;
        let entries = self.cache.len() as u64;
        let stats = self.cache.stats();
        let active_bytes = self.active.data.bytes as u64;
        let queued = self.notify.len() as u64;
        let notify_bytes = self.notify.bytes() as u64;
        let failures = self.notify.failures();
        let outcomes = *self.notify.outcomes();
        let parked = self.pending.is_some();
        let now = clock::now();
        if let Some(metrics) = &mut self.metrics {
            metrics.worker.cache_entries.set(entries);
            metrics.worker.cache_hits.observe(stats.hits);
            metrics.worker.cache_misses.observe(stats.misses);
            metrics.worker.cache_evictions.observe(stats.evictions);
            metrics.worker.active_bytes.set(active_bytes);
            metrics.worker.flushing_bytes.set(flushing as u64);
            metrics.worker.pending_bytes.set(pending as u64);
            metrics.worker.requests_pending.set(requests);
            metrics.worker.pending_slot.set(u64::from(parked));
            metrics.worker.notify_queued.set(queued);
            metrics.worker.notify_token_bytes.set(notify_bytes);
            metrics.worker.notify_failures.observe(failures);
            metrics.worker.acks.observe(outcomes[Outcome::Ack as usize]);
            for error_type in NackAttrs::ERROR_TYPES {
                metrics
                    .nacks
                    .with(NackAttrs { error_type })
                    .nacks
                    .observe(outcomes[error_type as usize]);
            }
            let (closed, closures, closed_secs) = self.admission.sample(now);
            metrics.worker.admission_closed.set(u64::from(closed));
            metrics.worker.admission_closures.observe(closures);
            metrics
                .worker
                .admission_closed_duration
                .observe(closed_secs);
            metrics.worker.oldest.set(oldest.map_or(0.0, |received| {
                now.saturating_duration_since(received).as_secs_f64()
            }));
            metrics.worker.memory_accounted_bytes.set(accounted);
            metrics.worker.flush_workspace_bytes.set(workspace as u64);
            metrics.worker.memory_budget_bytes.set(budget);
        }
    }

    /// Serve a window boundary that has fired.
    ///
    /// The boundary is the trigger of whatever rotation follows it, whatever a
    /// byte or request threshold asked for earlier: a block sealed because its
    /// window ended must not be reported under a reason left behind by a
    /// threshold that never got to rotate.
    pub(super) fn wake_window(&mut self) {
        if self.window.wake() {
            self.reason = FlushReason::Time;
            self.rotation_requested = true;
        }
    }

    /// Latch a shutdown deadline and ask for the ACTIVE block to be sealed.
    ///
    /// The earliest deadline wins, so a second, tighter shutdown cannot extend
    /// the first.
    pub(super) fn shutdown(&mut self, deadline: Instant) {
        // Nothing will open another block, so the parked request is decided
        // now rather than waiting for a rotation that will not serve it.
        if let Some(pending) = self.pending.take() {
            self.notify.push(pending.token, Outcome::Shutdown);
        }
        self.deadline = Some(self.deadline.map_or(deadline, |old| old.min(deadline)));
        self.reason = FlushReason::Shutdown;
        self.rotation_requested = true;
    }

    /// Whether the worker owes nothing further.
    pub(super) fn is_idle(&self) -> bool {
        self.pending.is_none()
            && self.active.tokens.is_empty()
            && self.active.data.is_empty()
            && self.flushing.is_none()
            && self.cleaning.is_none()
            && self.notify.is_empty()
    }

    /// Decide everything still owned, once the shutdown deadline has elapsed.
    ///
    /// A flush that has already published its decision is decided by that
    /// decision first: the deadline branch outranks the branch that awaits the
    /// flush result, so it can win the same poll in which a successful write
    /// finished, and a block whose files exist must be acknowledged rather than
    /// refused.
    ///
    /// The whole decision is taken and delivered before anything is cancelled,
    /// and it is taken without a single await: the parked write cannot run
    /// between the two halves, so no completion can be waiting behind the
    /// unwinding of the write it belongs to. A completion the engine cannot
    /// take immediately is counted as a delivery failure and released, so no
    /// request is left undecided whatever the destination is doing.
    ///
    /// Only then are the two slot holders cancelled and released. Both are
    /// awaited within one shared `upload.abort_timeout`, because a task that is
    /// still unwinding owns the block, the sink handle and possibly a multipart
    /// abort in flight; returning while it does would leave that abort to be
    /// cancelled by runtime teardown and the upload to be reclaimed by the
    /// bucket's lifecycle rule instead. The wait is bounded and ends in an
    /// abort that is itself awaited, so a destination that never answers cannot
    /// hold the node open: this costs at most one abort timeout beyond the
    /// shutdown deadline, and it buys the abort actually being attempted.
    pub(super) async fn abandon(&mut self) {
        // Phase zero: a flush that has already published its decision is
        // decided by that decision, not by the deadline. The loop's deadline
        // branch is biased above the branch that awaits the flush result, so
        // the deadline can win the very poll in which a successful write
        // finished; nacking that block would tell the producer to resend rows
        // whose files already exist. Taking the result here also commits the
        // descriptors it carried, and moves the job into the cleaning slot
        // that phase two releases.
        if let Some(job) = &mut self.flushing
            && let Some(done) = job.try_finish()
        {
            self.complete(Ok(done));
        }
        // Phase one: decide and deliver. Nothing here awaits, and nothing here
        // cancels.
        // Every completion still held by a block or the parking slot is
        // decided here by the deadline; the notifier's own queue was already
        // decided before.
        let mut abandoned = 0_u64;
        if let Some(pending) = self.pending.take() {
            abandoned += 1;
            self.notify.push(pending.token, Outcome::Shutdown);
        }
        let mut flushing = self.flushing.take();
        if let Some(job) = &mut flushing {
            abandoned += job.tokens.len() as u64;
            // Reported exactly as the `Error::Cancelled` completion branch
            // reports it: the write will not put its block in object storage,
            // and the reason it will not is that it is about to be cancelled.
            // Deciding the block here instead of awaiting its result must not
            // make that flush vanish from the counters.
            if let Some(metrics) = &mut self.metrics {
                metrics.worker.flush_duration.record(
                    clock::now()
                        .saturating_duration_since(job.started)
                        .as_secs_f64(),
                );
                metrics.worker.flush_failures.add(1);
                metrics.worker.flush_cancelled.add(1);
                // The retries this write spent are counted like a completed
                // flush counts them, or an outage that runs into the deadline
                // would report none at all.
                metrics
                    .worker
                    .flush_retries
                    .add(job.attempts.get().saturating_sub(1));
            }
            for token in std::mem::take(&mut job.tokens) {
                self.notify.push(token, Outcome::Shutdown);
            }
        }
        // A block whose decision has already been published owes no completion;
        // what it still owns is the write its supervisor is unwinding.
        let mut cleaning = self.cleaning.take();
        abandoned += self.active.tokens.len() as u64;
        self.fail_active(Outcome::Shutdown);
        self.abandoned += abandoned;
        self.notify.drain_now();
        // Phase two: cancel and release, with one shared bound so a node
        // holding a write and a cleanup does not wait twice.
        let deadline = flush::deadline_at(clock::now(), self.cfg.lake.upload.abort_timeout);
        if let Some(job) = &mut flushing {
            job.shutdown(deadline).await;
        }
        if let Some(job) = &mut cleaning {
            job.shutdown(deadline).await;
        }
    }
}
