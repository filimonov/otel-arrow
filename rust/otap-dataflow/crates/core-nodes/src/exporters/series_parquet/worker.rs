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

use super::config::Config;
use super::flush::{FlushDone, FlushJob};
use super::token::{AckToken, Notifier, Outcome};
use lake::buffer::Block;
use lake::cache::SeriesCache;
use lake::clock::{WallClock, nanos_to_micros, nanos_to_secs};
use lake::extract::Extracted;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::{OtapPayload, TryIntoWithOptions};
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

/// The ACTIVE and FLUSHING pair of one exporter instance.
pub(super) struct Worker {
    /// Validated user configuration.
    pub(super) cfg: Config,
    /// The one block open for admission.
    pub(super) active: OwnedBlock,
    /// The one block being written, if any.
    pub(super) flushing: Option<FlushJob>,
    /// Descriptors already written for a partition.
    pub(super) cache: SeriesCache,
    /// Delivery of decided completions.
    pub(super) notify: Notifier,
    /// Whether the ACTIVE block should be sealed at the next opportunity.
    pub(super) rotation_requested: bool,
    /// The shutdown deadline, once one has been latched.
    pub(super) deadline: Option<Instant>,
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
        let secs = nanos_to_secs(wall.now_unix_nanos());
        let windows = lake::clock::WindowClock::new(cfg.window.interval, secs);
        let active = OwnedBlock {
            data: Block::new(windows.last_boundary(), 0, &cfg.lake),
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
            cache,
            notify,
            rotation_requested: false,
            deadline: None,
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

    /// Validate one request and extract its rows.
    ///
    /// Returns `Ok(None)` for a request that carries no rows at all: there is
    /// nothing to make durable, so it is acknowledged without touching a
    /// block.
    fn prepare(&self, mut payload: OtapPayload) -> Result<Option<Extracted>, Failure> {
        // `num_bytes` is an estimate of the wire representation, so the budget
        // is also enforced on the measured extracted output inside `extract`.
        if !payload
            .num_bytes()
            .is_some_and(|n| n <= self.cfg.lake.ingress.max_request_bytes)
        {
            return Err(Failure::Permanent(lake::Error::Refused(
                lake::RefuseReason::RequestTooLarge,
            )));
        }
        if payload.signal_type() != otel_arrow_dfe_config::SignalType::Logs {
            return Err(Failure::Permanent(lake::Error::Refused(
                lake::RefuseReason::Unsupported("signal".into()),
            )));
        }
        let mut records: OtapArrowRecords = payload.try_into_with_default().map_err(|e| {
            Failure::Permanent(lake::Error::invalid(format!("undecodable pdata: {e}")))
        })?;
        let extracted =
            lake::extract::extract(&mut records, &self.cfg.lake).map_err(Failure::Permanent)?;
        drop(records);
        if extracted.stats.rows == 0 {
            return Ok(None);
        }
        Ok(Some(extracted))
    }

    /// Take ownership of one request's completion and try to admit its rows.
    ///
    /// Every path ends with the request decided or its completion held by the
    /// ACTIVE block; none leaves it without an owner.
    pub(super) fn admit(&mut self, data: OtapPdata) {
        // The completion is retained across a storage round trip before it is
        // handed back, so the token keeps only the routing frames: the payload
        // is returned here and the inbound credentials and the claims derived
        // from them are dropped inside `split`, rather than staying resident
        // for the duration of the write (spec section 7).
        let (token, payload) = AckToken::split(data);
        let token_bytes = token.bytes();
        match self.prepare(payload) {
            Ok(Some(extracted)) => self.admit_extracted(token, extracted, token_bytes),
            Ok(None) => self.notify.push(token, Outcome::Ack),
            Err(failure) => self.refuse(token, &failure),
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

    /// Reserve, then admit, an already validated request.
    fn admit_extracted(&mut self, token: AckToken, extracted: Extracted, token_bytes: usize) {
        let reservation =
            match self
                .active
                .data
                .reserve(&extracted, &mut self.cache, token_bytes, &self.cfg.lake)
            {
                Ok(reservation) => reservation,
                Err(error) => {
                    // The reservation refused before the block was touched, so
                    // only this request is affected.
                    self.refuse(token, &Self::reservation_failure(error));
                    return;
                }
            };
        match self.active.data.admit(extracted, reservation, ()) {
            Ok(()) => {
                self.active.tokens.push(token);
                // Rotation timing is not this task's: until the window timer
                // lands, a block is sealed as soon as it holds a request, so
                // an acknowledged request is durable without waiting for a
                // later one to arrive.
                self.rotation_requested = true;
            }
            Err(error) => {
                // A failed admission leaves the block partially updated by
                // contract, so the whole ACTIVE block is failed rather than
                // written.
                self.refuse(token, &Failure::Retryable(error));
                self.fail_active(Outcome::Storage);
            }
        }
    }

    /// An empty block for the current window, after the one in hand.
    ///
    /// The window start never moves backwards, so a wall clock that steps back
    /// cannot make a later block claim an earlier partition.
    fn new_active(&mut self) -> OwnedBlock {
        let secs = nanos_to_secs(self.wall.now_unix_nanos());
        let windows = lake::clock::WindowClock::new(self.cfg.window.interval, secs);
        let start = windows
            .last_boundary()
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
        self.deadline = Some(self.deadline.map_or(deadline, |old| old.min(deadline)));
        self.rotation_requested = true;
    }

    /// Whether the worker owes nothing further.
    pub(super) fn is_idle(&self) -> bool {
        self.active.tokens.is_empty()
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
