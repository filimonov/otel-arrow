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
//! An OTLP request is checked for top-level protobuf wire framing before it is
//! converted, because the shared byte views decode lazily and report no error
//! for a damaged body: without the check a truncated request would become a
//! request carrying no rows and be acknowledged as stored. Only the top level
//! is validated -- field tags and the bounds of each length-delimited field.
//! Nested content is still read lazily and is not validated here; corruption
//! inside a submessage surfaces as missing or empty fields rather than a
//! refusal. Covering that is deferred to the chaos tests of plan 3.
//!
//! Logs and metrics are admitted through the same state machine and the same
//! single extraction call; traces have no lake schema and are refused on the
//! signal alone. A metrics request whose points the lake has no dataset for --
//! exponential histograms and summaries -- is decided by the configured
//! `unsupported` policy inside that one extraction call, atomically for the
//! whole request: `reject` refuses it, `drop` keeps the supported points and
//! counts the rest. Metadata and exemplar attribute tables are neither read
//! nor validated, under either policy.
//!
//! A block-scoped refusal -- a full block, or one already holding its request
//! limit -- is not the request's fault, so the request is not nacked for it.
//! Its extraction is parked in `pending`, admission closes until a block
//! opens, and the parked request is reserved against that block before any
//! newer one. Exactly one request is ever parked, so the bound above becomes
//! two blocks, one request and the completions in flight.

use super::config::Config;
use super::flush::{FlushDone, FlushJob};
use super::token::{AckToken, Notifier, Outcome};
use super::window::Window;
use lake::buffer::Block;
use lake::cache::SeriesCache;
use lake::clock::{WallClock, nanos_to_micros, nanos_to_secs};
use lake::extract::Extracted;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otel_arrow_dfe_pdata::{OtapPayload, OtlpProtoBytes, PayloadData, TryIntoWithOptions};
use otel_arrow_dfe_series_lake as lake;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinError;

/// How a failed request must be reported back to its sender.
///
/// The distinction is the phase the failure came from, not the error type.
/// Validation -- the request budget, the signal check, `extract` and the
/// request-level part of `reserve` -- judges the request's own content, so the
/// identical bytes will be refused again and the client must change the
/// request. Everything after that point (`admit`, `seal`, and the Arrow,
/// Parquet and object store work inside `write_block`) is infrastructure: the
/// same request may well succeed on a retry, so it must not be reported as a
/// client error.
#[derive(Debug)]
pub(super) enum Failure {
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
    pub(super) fn error(&self) -> &lake::Error {
        match self {
            Failure::Permanent(e) | Failure::Retryable(e) => e,
        }
    }

    /// The completion outcome this failure must be reported as.
    ///
    /// A permanent failure keeps the validation rule that rejected the
    /// request, because the sender can act on it; every retryable failure is
    /// reported as a storage outcome, which is not a client error.
    pub(super) fn outcome(&self) -> Outcome {
        match self {
            Failure::Permanent(lake::Error::Refused(lake::RefuseReason::RequestTooLarge)) => {
                Outcome::TooLarge
            }
            Failure::Permanent(lake::Error::Refused(lake::RefuseReason::Unsupported(_))) => {
                Outcome::Unsupported
            }
            Failure::Permanent(_) => Outcome::Invalid,
            Failure::Retryable(_) => Outcome::Storage,
        }
    }
}

/// A block together with the completions of every request admitted to it.
///
/// The block itself carries `()` per request: the completions are kept beside
/// it so a flush task can own the data without owning the routing contexts.
pub(super) struct OwnedBlock {
    /// Rows, descriptors and accounting.
    pub(super) data: Block<()>,
    /// One completion per admitted request, in admission order.
    pub(super) tokens: Vec<AckToken>,
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
    /// The request failed validation and its completion is still owed.
    Failed(AckToken, Failure),
}

/// The ACTIVE and FLUSHING pair of one exporter instance.
pub(super) struct Worker {
    /// Validated user configuration.
    pub(super) cfg: Config,
    /// The one block open for admission.
    pub(super) active: OwnedBlock,
    /// The one block being written, if any.
    pub(super) flushing: Option<FlushJob>,
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
    sink: Rc<lake::sink::Sink>,
    /// Per-worker block sequence, used in file names.
    seq: u64,
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
        let active = OwnedBlock {
            data: Block::new(window.clock.last_boundary(), 0, &cfg.lake),
            tokens: Vec::new(),
        };
        let sink = Rc::new(lake::sink::Sink::new(
            store,
            cfg.lake.clone(),
            lake::sink::FileNaming::new(&cfg.lake.writer_id),
        ));
        // One credit per in-flight request in each of the two blocks a window
        // pair can hold, with the last slot reserved for a shutdown outcome.
        let notify = Notifier::new(effects, 2 * cfg.window.max_requests_per_block);
        let cache = SeriesCache::new(cfg.cache_entries);
        Self {
            cfg,
            active,
            flushing: None,
            pending: None,
            cache,
            notify,
            rotation_requested: false,
            deadline: None,
            window,
            wall,
            sink,
            seq: 1,
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

    /// Classify a refusal from `Block::reserve`.
    ///
    /// Only `RequestTooLarge` judges the request itself: it means the request
    /// could not fit a block even if one were empty. The block-full and
    /// request-count refusals are about whichever block happened to be active,
    /// so the same bytes may be admitted to the next one and the sender must
    /// not be told to change them.
    fn reservation_failure(error: lake::Error) -> Failure {
        match error {
            lake::Error::Refused(lake::RefuseReason::RequestTooLarge) => Failure::Permanent(error),
            _ => Failure::Retryable(error),
        }
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
        if !payload
            .num_bytes()
            .is_some_and(|n| n <= self.cfg.lake.ingress.max_request_bytes)
        {
            return Prepared::Failed(
                token,
                Failure::Permanent(lake::Error::Refused(lake::RefuseReason::RequestTooLarge)),
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
                Failure::Permanent(lake::Error::Refused(lake::RefuseReason::Unsupported(
                    "traces".into(),
                ))),
            );
        }
        if let Err(failure) = Self::check_wire_format(&payload) {
            return Prepared::Failed(token, failure);
        }
        let extracted = match self.extract(payload) {
            Ok(extracted) => extracted,
            Err(failure) => return Prepared::Failed(token, failure),
        };
        Prepared::Ready(Pending {
            extracted,
            token,
            admission_secs: nanos_to_secs(self.wall.now_unix_nanos()),
        })
    }

    /// Refuse an OTLP body whose top-level protobuf framing is broken.
    ///
    /// The shared OTLP byte views are deliberately non-validating: the
    /// conversion this node performs reads them lazily and reports no error for
    /// a truncated or corrupt body, so without this check a damaged request
    /// would be converted into a request carrying no rows and acknowledged as
    /// if it had been stored. The framing walk is a single linear pass over the
    /// buffer with no allocation, and only the top level is checked -- see the
    /// module documentation for what that does and does not cover.
    ///
    /// A payload that already holds Arrow records has no wire framing to check;
    /// it is validated by the conversion and the extraction instead.
    fn check_wire_format(payload: &OtapPayload) -> Result<(), Failure> {
        let PayloadData::OtlpBytes(bytes) = payload.data() else {
            return Ok(());
        };
        let (signal, framed) = match bytes {
            OtlpProtoBytes::ExportLogsRequest(buf) => ("logs", RawLogsData::try_new(buf).map(drop)),
            OtlpProtoBytes::ExportMetricsRequest(buf) => {
                ("metrics", RawMetricsData::try_new(buf).map(drop))
            }
            // Traces are decided by the signal check before this runs, so
            // there is no body to walk here.
            OtlpProtoBytes::ExportTracesRequest(_) => return Ok(()),
        };
        framed.map_err(|error| {
            Failure::Permanent(lake::Error::invalid(format!(
                "malformed OTLP {signal} body: {error}"
            )))
        })
    }

    /// Convert one payload and extract its rows, dropping the conversion.
    ///
    /// Split out so the record batches the conversion produced go out of scope
    /// with the call rather than living as long as the extraction does.
    fn extract(&self, payload: OtapPayload) -> Result<Extracted, Failure> {
        let mut records: OtapArrowRecords = payload.try_into_with_default().map_err(|e| {
            Failure::Permanent(lake::Error::invalid(format!("undecodable pdata: {e}")))
        })?;
        lake::extract::extract(&mut records, &self.cfg.lake).map_err(Failure::Permanent)
    }

    /// Take ownership of one request's completion and try to admit its rows.
    ///
    /// Every path ends with the request decided, its completion held by the
    /// ACTIVE block, or the whole extraction parked; none leaves it without an
    /// owner.
    pub(super) fn admit(&mut self, data: OtapPdata) {
        match self.prepare(data) {
            Prepared::Ready(pending) => self.offer(pending),
            Prepared::Failed(token, failure) => self.refuse(token, &failure),
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
            self.park(pending);
            return;
        }
        let token_bytes = pending.token.bytes();
        let reservation = match self.active.data.reserve(
            &pending.extracted,
            &mut self.cache,
            token_bytes,
            &self.cfg.lake,
        ) {
            Ok(reservation) => reservation,
            // The block-scoped refusals judge whichever block happened to
            // be active, so the request waits for the next one. An empty
            // block cannot refuse this way -- a request it does not fit is
            // `RequestTooLarge` -- so parking here can never become an
            // endless rotation; the guard makes that a checked fact rather
            // than an inference about `reserve`.
            Err(lake::Error::Refused(
                lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests,
            )) if !self.active.data.is_empty() => {
                self.park(pending);
                return;
            }
            Err(error) => {
                // The reservation refused before the block was touched, so
                // only this request is affected.
                self.refuse(pending.token, &Self::reservation_failure(error));
                return;
            }
        };
        match self.active.data.admit(pending.extracted, reservation, ()) {
            Ok(()) => {
                self.active.tokens.push(pending.token);
                // The window boundary is the normal rotation trigger; these
                // two only bring it forward, so a burst is written as soon as
                // it has filled a block rather than held until the boundary.
                // A rotation that was already owed stays owed: an admission
                // cannot cancel a boundary that has been consumed.
                self.rotation_requested |= self.active.data.bytes
                    >= self.cfg.window.max_block_bytes
                    || self.active.tokens.len() >= self.cfg.window.max_requests_per_block;
            }
            Err(error) => {
                // A failed admission leaves the block partially updated by
                // contract, so the whole ACTIVE block is failed rather than
                // written.
                self.refuse(pending.token, &Failure::Retryable(error));
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
    /// the detail still exists; the completion carries only the outcome.
    fn refuse(&mut self, token: AckToken, failure: &Failure) {
        let outcome = failure.outcome();
        otel_warn!(
            "series_parquet.request_failed",
            outcome = outcome.reason(),
            error = %failure.error()
        );
        self.notify.push(token, outcome);
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
        OwnedBlock {
            data: Block::new(start, seq, &self.cfg.lake),
            tokens: Vec::new(),
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
    /// A no-op while a flush is already outstanding: the rotation stays
    /// requested and is served once the flush slot frees, which is what keeps
    /// the worker to two blocks.
    pub(super) fn rotate(&mut self) {
        if self.flushing.is_some() {
            return;
        }
        self.rotation_requested = false;
        if self.active.data.is_empty() {
            // A parked request may be waiting for a later window than the one
            // this empty block was opened for, so the block is replaced rather
            // than kept; otherwise the resume would park it again.
            self.active = self.new_active();
            return;
        }
        if let Err(error) = self
            .active
            .data
            .seal(nanos_to_micros(self.wall.now_unix_nanos()))
        {
            otel_warn!("series_parquet.seal_failed", error = %error);
            self.fail_active(Outcome::Storage);
            return;
        }
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        self.flushing = Some(FlushJob::new(old.data, old.tokens, self.sink.clone()));
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
    pub(super) fn complete(&mut self, done: Result<FlushDone, JoinError>) {
        let Some(mut job) = self.flushing.take() else {
            return;
        };
        let outcome = match &done {
            Ok(finished) => match &finished.result {
                Ok(report) => {
                    for id in &finished.data.pending_series {
                        self.cache.mark_committed(*id, finished.data.partition);
                    }
                    otel_info!(
                        "series_parquet.block_committed",
                        files = report.files.len(),
                        requests = job.tokens.len(),
                        bytes = job.bytes,
                        duration = ?clock::now().saturating_duration_since(job.started)
                    );
                    Outcome::Ack
                }
                Err(error) => {
                    otel_warn!("series_parquet.flush_failed", error = %error);
                    self.failed_outcome()
                }
            },
            Err(error) => {
                otel_warn!("series_parquet.flush_task_failed", error = %error);
                self.failed_outcome()
            }
        };
        for token in std::mem::take(&mut job.tokens) {
            self.notify.push(token, outcome);
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
        self.rotation_requested = true;
    }

    /// Whether the worker owes nothing further.
    pub(super) fn is_idle(&self) -> bool {
        self.pending.is_none()
            && self.active.tokens.is_empty()
            && self.active.data.is_empty()
            && self.flushing.is_none()
            && self.notify.is_empty()
    }

    /// Decide everything still owned, once the shutdown deadline has elapsed.
    ///
    /// The outstanding write is cancelled rather than awaited, because the
    /// node has run out of time to wait for it, and every completion is
    /// attempted once without blocking. A completion the engine cannot take
    /// immediately is counted as a delivery failure and released, so the node
    /// returns within its deadline and no request is left undecided.
    pub(super) fn abandon(&mut self) {
        if let Some(pending) = self.pending.take() {
            self.notify.push(pending.token, Outcome::Shutdown);
        }
        if let Some(mut job) = self.flushing.take() {
            job.cancel.cancel();
            for token in std::mem::take(&mut job.tokens) {
                self.notify.push(token, Outcome::Shutdown);
            }
        }
        self.fail_active(Outcome::Shutdown);
        self.notify.drain_now();
    }
}
