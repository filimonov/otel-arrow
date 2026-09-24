// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The one outstanding flush, split between a task and its owner.
//!
//! A rotated block becomes a [`FlushJob`]: the sealed data is moved into a
//! local task that writes it, while the completions the block still owes stay
//! behind in the job. Both halves remain one logical FLUSHING block. Keeping
//! the tokens outside the task is what lets the node decide them on a shutdown
//! deadline without first waiting for a wedged multipart upload to unwind, and
//! it means a task that panics cannot take the routing contexts with it.
//!
//! The task supervises the write rather than performing a single attempt. A
//! storage failure is retried, with exponential backoff, until an absolute
//! deadline taken once when the block was sealed -- not a per-attempt timeout,
//! which a destination that fails slowly could extend without bound. Retries
//! rewrite the same sealed block, so the file names and the bytes of every
//! attempt are identical and a retry overwrites whatever a failed attempt left
//! behind rather than adding a second copy of the same rows.
//!
//! Shutdown can only bring that deadline forward, to the latched shutdown
//! deadline (see [`Limit`]).
//!
//! The deadline decides the producer immediately. The block's requests are
//! told the write failed as soon as the deadline passes, over a oneshot
//! channel, while the task stays behind to cancel the write and give the sink
//! its bounded chance to abort a multipart upload. That is why the result
//! travels separately from the join handle: waiting for the task would tie a
//! producer's nack to a cleanup that may legitimately take the whole
//! `upload.abort_timeout`. The job keeps holding the FLUSHING slot until that
//! cleanup has finished, so a next block never writes while an abandoned
//! attempt on the same names might still be in flight.
//!
//! Dropping the job cancels the write. The sink treats its cancellation token
//! as a request to abort the upload rather than finish it while the upload is
//! still writable. That does not make a cancelled or expired block leave no
//! file behind: an object whose upload was already being finalized when the
//! token fired may still complete, and so may one whose write finished in the
//! very poll the deadline expired in if the owner has stopped listening. Such
//! a file holds rows whose requests were nacked, which the producer's retry
//! then writes again -- a duplicate that at-least-once delivery permits, never
//! a loss; [`Trace::cleaned_up`] reports how each cancelled write unwound,
//! probing the objects when it cannot tell. What the deadline does guarantee
//! is that a write that has finished when it is polled is reported as the
//! success it is, see [`write_until`].

use super::token::AckToken;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake as lake;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// Delay before the first retry of a failed write.
const FIRST_BACKOFF: Duration = Duration::from_millis(200);

/// Upper bound the doubling backoff is clamped to.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Fallback horizon when `base + delta` is not representable.
const FAR_FUTURE: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// `base + delta`, saturated rather than panicking.
///
/// `Instant::add` panics on overflow, and every duration here is
/// user-configurable: a `flush_retry_deadline` or an `abort_timeout` written
/// as an absurd number of seconds would otherwise take the node down instead
/// of behaving like the unreachable deadline the user asked for. Saturating
/// upwards is the safe direction -- a deadline that is further away only means
/// more retrying -- so the fallback is a year out, and finally `base` itself on
/// a clock so close to the end of its representable range that even that does
/// not fit.
pub(super) fn deadline_at(base: Instant, delta: Duration) -> Instant {
    base.checked_add(delta)
        .or_else(|| base.checked_add(FAR_FUTURE))
        .unwrap_or(base)
}

/// The instant the cleanup of a write decided by `deadline` must end by.
///
/// It is `abort_timeout` after the deadline itself, not after the moment the
/// expiry is observed, so a late wake or a long synchronous drain cannot
/// extend it; and it is never more than `abort_timeout` from `now`, which is
/// the bound when the write is cancelled before any deadline.
pub(super) fn cleanup_cutoff(
    now: Instant,
    deadline: Option<Instant>,
    abort_timeout: Duration,
) -> Instant {
    let fresh = deadline_at(now, abort_timeout);
    deadline.map_or(fresh, |deadline| {
        deadline_at(deadline, abort_timeout).min(fresh)
    })
}

/// The instant one flush must be decided by, shared by the job and its task.
///
/// It is the block's own retry deadline until shutdown latches; from then on
/// it is the earlier of that and the latched deadline, and the backoff between
/// attempts drops to its minimum. An attempt starts only before the deadline
/// and is cancelled at it. Moving it wakes the task, which re-reads it before
/// every attempt and every wait.
struct Limit {
    /// The block's own retry deadline, taken when it was sealed.
    own: Instant,
    /// The latched shutdown deadline, once [`FlushJob::cut_to`] has set it.
    shutdown: Cell<Option<Instant>>,
    /// Wakes the task when the shutdown deadline is set or moved.
    moved: tokio::sync::Notify,
}

impl Limit {
    /// The instant the flush must be decided by.
    fn at(&self) -> Instant {
        self.shutdown
            .get()
            .map_or(self.own, |cut| cut.min(self.own))
    }

    /// The wait before the next retry, which shutdown drops to its minimum.
    fn backoff(&self, delay: Duration) -> Duration {
        if self.shutdown.get().is_some() {
            FIRST_BACKOFF
        } else {
            delay
        }
    }
}

/// A flush that has resolved, with the block it was writing.
pub(super) struct FlushDone {
    /// The sealed block, handed back so its descriptors and partition are
    /// available to the commit that only a successful write may perform.
    ///
    /// Shared rather than moved, because the supervising task may still be
    /// unwinding a cancelled attempt over the same block when the decision is
    /// published.
    pub(super) data: Rc<lake::buffer::Block>,
    /// What the sink returned.
    pub(super) result: lake::Result<lake::sink::FlushReport>,
    /// Write attempts this flush made, including the one that resolved it.
    pub(super) attempts: u64,
}

/// Write one sealed block, retrying storage failures until the deadline
/// `limit` holds.
///
/// The write is polled before the cancellation and the deadline: a write that
/// has finished by the poll in which the deadline also expires is a success
/// whose files exist, and nacking it would tell the producer to resend rows
/// that are already durable. No attempt starts at or after the deadline.
///
/// The result is published over `result_tx` the moment it is known. The task
/// returns only once it has released everything it owns: on the deadline path
/// that means cancelling the attempt in flight and then waiting for the sink
/// to unwind it until [`cleanup_cutoff`], after which the write future is
/// dropped whether or not it cooperated.
async fn write_until(
    sink: Rc<lake::sink::Sink>,
    data: Rc<lake::buffer::Block>,
    cancel: CancellationToken,
    limit: Rc<Limit>,
    abort_timeout: Duration,
    result_tx: tokio::sync::oneshot::Sender<FlushDone>,
    trace: Rc<Trace>,
) {
    let mut attempts = 0_u64;
    let mut delay = FIRST_BACKOFF;
    // The failure of the last attempt, so the deadline reports what the
    // destination actually said rather than the fact that time ran out.
    let mut last: Option<lake::Error> = None;
    let done = |attempts, result| FlushDone {
        data: Rc::clone(&data),
        attempts,
        result,
    };
    loop {
        if cancel.is_cancelled() {
            let error = last.take().unwrap_or(lake::Error::cancelled(None));
            let _ = result_tx.send(done(attempts, Err(error)));
            return;
        }
        if clock::now() >= limit.at() {
            let _ = result_tx.send(done(attempts, Err(expired(attempts, last.take()))));
            return;
        }
        attempts += 1;
        trace.attempts.set(attempts);
        // The names this attempt will write, announced before it writes them,
        // so an operator can see that a retry rewrites objects rather than
        // adding any.
        //
        // The first attempt of a flush is the ordinary case and says nothing
        // an operator needs at INFO -- one line per written block already
        // exists -- so it is emitted at DEBUG. A retry is the event worth
        // reporting, and it is rare by construction. The file name is a log
        // field only: it is unbounded in cardinality (window, boot id and
        // sequence all move) and never labels a metric, where attempts are
        // counted instead.
        let remaining = limit.at().saturating_duration_since(clock::now());
        if attempts > 1 {
            let retries = &trace.shared.tally.retries;
            retries.set(retries.get() + 1);
            otel_info!(
                "series_parquet.flush.attempt",
                seq = trace.seq,
                attempt = attempts,
                file = &*trace.file,
                objects = trace.paths.len(),
                deadline_remaining = ?remaining
            );
        } else {
            otel_debug!(
                "series_parquet.flush.attempt",
                seq = trace.seq,
                attempt = attempts,
                file = &*trace.file,
                objects = trace.paths.len(),
                deadline_remaining = ?remaining
            );
        }
        // A child token so the attempt can be cancelled on the deadline
        // without cancelling the job itself, whose token also serves the
        // owner's drop.
        let attempt_cancel = cancel.child_token();
        let write = sink.write_block(&data, &attempt_cancel);
        tokio::pin!(write);
        // Re-armed whenever shutdown moves the deadline, without dropping the
        // write in flight.
        let result = loop {
            tokio::select! {
                biased;
                result = &mut write => break Ok(result),
                () = cancel.cancelled() => break Err(last
                    .take()
                    .unwrap_or(lake::Error::cancelled(None))),
                () = clock::sleep_until(limit.at()) => break Err(expired(attempts, last.take())),
                () = limit.moved.notified() => {}
            }
        };
        let result = match result {
            Ok(result) => result,
            Err(decided) => {
                // Publish the producer decision before awaiting any cleanup. This
                // task independently owns the block, the sink handle and the
                // pinned write future, so the owner is free to release the block's
                // completions while the attempt is still unwinding.
                let _ = result_tx.send(done(attempts, Err(decided)));
                attempt_cancel.cancel();
                let cutoff = cleanup_cutoff(clock::now(), Some(limit.at()), abort_timeout);
                let unwound = tokio::select! {
                    biased;
                    result = &mut write => Some(result),
                    () = clock::sleep_until(cutoff) => None,
                };
                trace.cleaned_up(attempts, unwound, cutoff).await;
                // Dropping the write after the bound releases the last task-owned
                // resources even if the object store future never cooperates.
                return;
            }
        };
        // Every failed attempt is reported with what the destination said,
        // whether or not it is retried, not only the failure the flush finally
        // ends with: an outage that the next attempt survives would otherwise
        // leave no trace at all, and a failure that ends the flush at once is
        // then logged per attempt exactly like a retried one.
        if let Err(error) = &result {
            trace.attempt_failed(attempts, limit.at(), error);
        }
        match result {
            Ok(report) => {
                let _ = result_tx.send(done(attempts, Ok(report)));
                return;
            }
            Err(error)
                if error.is_retryable() && clock::now() < limit.at() && !cancel.is_cancelled() =>
            {
                last = Some(error);
                // Never past the deadline: the wait itself must not outlive
                // the bound the block was given. Re-armed when shutdown moves
                // the deadline or drops the backoff.
                let waiting_since = clock::now();
                loop {
                    let wake = deadline_at(waiting_since, limit.backoff(delay)).min(limit.at());
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => break,
                        () = clock::sleep_until(wake) => break,
                        () = limit.moved.notified() => {}
                    }
                }
                delay = delay.saturating_mul(2).min(MAX_BACKOFF);
            }
            Err(error) => {
                // A storage failure of the last attempt may be a response the
                // store lost after committing, so the objects are probed
                // within the same cutoff a decided write is given.
                let ambiguous = error.is_retryable();
                let abort_error = abort_failure(&error).map(str::to_owned);
                let _ = result_tx.send(done(attempts, Err(error)));
                if let Some(abort_error) = abort_error {
                    trace.abort_failed(attempts, &abort_error);
                } else if ambiguous {
                    let cutoff = cleanup_cutoff(clock::now(), Some(limit.at()), abort_timeout);
                    trace.probe(attempts, cutoff).await;
                }
                return;
            }
        }
    }
}

/// Counts a flush task records as they happen, which the worker moves into
/// its metrics whenever it samples them.
#[derive(Debug, Default)]
pub(super) struct FlushTally {
    /// Write attempts started beyond the first of their flush.
    pub(super) retries: Cell<u64>,
    /// Failed flushes that may have left a multipart upload behind: the
    /// abort failed, or the write did not unwind in time.
    pub(super) abort_failures: Cell<u64>,
    /// Failed flushes whose every object exists after all.
    pub(super) late_commits: Cell<u64>,
}

/// What every flush task of one worker shares with it.
pub(super) struct FlushShared {
    /// The store the sink writes to, which a cleanup probes for a late commit.
    pub(super) store: Arc<dyn ObjectStore>,
    /// Counts the tasks record as they happen.
    pub(super) tally: FlushTally,
}

/// What a probe of every frozen object of a failed block found.
#[derive(Debug, PartialEq, Eq)]
enum Presence {
    /// Every object exists: the block's rows are stored although its
    /// requests were nacked.
    All,
    /// Only this many of the objects exist.
    Some(usize),
    /// No object exists.
    None,
    /// A HEAD failed or the cleanup cutoff passed first.
    Unknown(String),
}

/// HEAD every path, each bounded by `cutoff`.
///
/// Objects are atomic on the stores this writes to, so a path that exists
/// holds a finished file.
async fn presence(store: &dyn ObjectStore, paths: &[Path], cutoff: Instant) -> Presence {
    let mut present = 0;
    for path in paths {
        let head = tokio::select! {
            biased;
            head = store.head(path) => head,
            () = clock::sleep_until(cutoff) => {
                return Presence::Unknown("the cleanup cutoff passed before the probe finished".into());
            }
        };
        match head {
            Ok(_) => present += 1,
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Presence::Unknown(error.to_string()),
        }
    }
    match present {
        0 => Presence::None,
        n if n == paths.len() => Presence::All,
        n => Presence::Some(n),
    }
}

/// What one flush task reports its attempts and its cleanup under.
pub(super) struct Trace {
    /// The file name every object of the block shares; each object differs
    /// only in its dataset directory.
    pub(super) file: Rc<str>,
    /// Every object one attempt writes.
    paths: Vec<Path>,
    /// The block's per-worker sequence.
    seq: u64,
    /// Attempts the task has started so far, the one in flight included.
    attempts: Cell<u64>,
    /// The store to probe and where the counts go.
    shared: Rc<FlushShared>,
}

#[cfg(test)]
impl Trace {
    /// A trace for block `seq` writing `paths`, for a test that reports a
    /// cleanup without running a flush.
    pub(super) fn for_test(paths: Vec<Path>, seq: u64, shared: Rc<FlushShared>) -> Self {
        Self {
            file: paths.first().and_then(Path::filename).unwrap_or("").into(),
            paths,
            seq,
            attempts: Cell::new(0),
            shared,
        }
    }
}

impl Trace {
    /// Log one failed write attempt at WARN, retried or not.
    ///
    /// The only place the per-attempt WARN is emitted.
    fn attempt_failed(&self, attempt: u64, deadline: Instant, error: &lake::Error) {
        otel_warn!(
            "series_parquet.flush.attempt_failed",
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            retryable = error.is_retryable(),
            deadline_remaining = ?deadline.saturating_duration_since(clock::now()),
            error = %error
        );
    }

    /// Report how the cancelled write of an already decided flush unwound:
    /// `None` when it had not by `cutoff`.
    ///
    /// A write that completed anyway is a late commit. One whose abort
    /// failed, or that never unwound, may have left a multipart upload to the
    /// bucket's lifecycle rule. Any other cancellation is ambiguous -- the
    /// store may have finished the upload and lost its response -- so the
    /// objects are probed until `cutoff`.
    pub(super) async fn cleaned_up(
        &self,
        attempt: u64,
        unwound: Option<lake::Result<lake::sink::FlushReport>>,
        cutoff: Instant,
    ) {
        match &unwound {
            Some(Ok(_)) => self.late_commit(attempt),
            Some(Err(error)) => match abort_failure(error) {
                Some(abort_error) => self.abort_failed(attempt, abort_error),
                None => self.probe(attempt, cutoff).await,
            },
            None => self.abort_failed(attempt, "the write did not unwind by the cleanup cutoff"),
        }
    }

    /// Probe the block's objects until `cutoff` and report what was found.
    ///
    /// Every object present is a late commit; none is a clean abort; some,
    /// or a probe that failed, is reported without counting a late commit.
    async fn probe(&self, attempt: u64, cutoff: Instant) {
        match presence(&*self.shared.store, &self.paths, cutoff).await {
            Presence::All => self.late_commit(attempt),
            Presence::None => otel_debug!(
                "series_parquet.flush.cleanup",
                outcome = "aborted",
                seq = self.seq,
                attempt = attempt,
                file = &*self.file
            ),
            Presence::Some(present) => otel_info!(
                "series_parquet.flush.cleanup",
                outcome = "partial",
                seq = self.seq,
                attempt = attempt,
                file = &*self.file,
                present = present,
                objects = self.paths.len(),
                message = "some objects of a failed block exist; their rows may be stored twice \
                           once the producer retries"
            ),
            Presence::Unknown(probe_error) => otel_warn!(
                "series_parquet.flush.cleanup",
                outcome = "unknown",
                seq = self.seq,
                attempt = attempt,
                file = &*self.file,
                probe_error = probe_error.as_str(),
                message = "could not tell whether a failed block's objects exist"
            ),
        }
    }

    /// Count and log a failed block whose every object exists.
    fn late_commit(&self, attempt: u64) {
        let late = &self.shared.tally.late_commits;
        late.set(late.get() + 1);
        otel_info!(
            "series_parquet.flush.cleanup",
            outcome = "late_commit",
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            message = "a failed block's objects exist although its requests were nacked; its \
                       rows may be stored twice once the producer retries"
        );
    }

    /// Count and log a cleanup that may have left a multipart upload behind.
    pub(super) fn abort_failed(&self, attempt: u64, abort_error: &str) {
        let failures = &self.shared.tally.abort_failures;
        failures.set(failures.get() + 1);
        otel_warn!(
            "series_parquet.flush.cleanup",
            outcome = "abort_failed",
            seq = self.seq,
            attempt = attempt,
            file = &*self.file,
            abort_error = abort_error,
            message = "a multipart upload of a failed block may be left to the bucket \
                       lifecycle rule"
        );
    }
}

/// Why the cleanup abort of a failed or cancelled write did not succeed, if
/// the error says it did not.
fn abort_failure(error: &lake::Error) -> Option<&str> {
    match error {
        lake::Error::Transient(
            lake::TransientError::Cancelled {
                abort_error: Some(abort_error),
            }
            | lake::TransientError::AbortFailed { abort_error, .. },
        ) => Some(abort_error),
        _ => None,
    }
}

/// The outcome of a flush whose retry deadline expired.
///
/// Carries the failure of the last attempt that returned, so the producer
/// and the log are told what the destination said rather than only that time
/// ran out, and it is a distinct error rather than a cancellation, so a
/// storage hang is never counted as a shutdown.
fn expired(attempts: u64, last: Option<lake::Error>) -> lake::Error {
    lake::Error::Transient(lake::TransientError::DeadlineExceeded {
        attempts,
        last: last.map(Box::new),
    })
}

/// One local flush task and the completions its block still owes.
pub(super) struct FlushJob {
    /// The supervising task; joined exactly once through [`FlushJob::cleanup`].
    handle: JoinHandle<()>,
    /// The producer-visible decision, published as soon as it is known.
    result_rx: tokio::sync::oneshot::Receiver<FlushDone>,
    /// Cancels the write, on an explicit request or when the job is dropped.
    pub(super) cancel: CancellationToken,
    /// Completions of every request admitted to the flushing block.
    pub(super) tokens: Vec<AckToken>,
    /// Bytes the block charged when it was sealed, snapshotted before the data
    /// moved into the task.
    pub(super) bytes: usize,
    /// When the write started, for the duration reported on completion.
    pub(super) started: Instant,
    /// Descriptor rows this block carries, per re-emission cause.
    ///
    /// Counted at admission but reported only once the write has returned
    /// success, so a series row is credited exactly when its file exists.
    pub(super) emitted: [u64; 3],
    /// The sealed block's window start, in Unix seconds, for the commit log.
    pub(super) window_start_secs: i64,
    /// The sealed block's per-worker sequence, for the commit log.
    pub(super) seq: u64,
    /// What the task reports its attempts and its cleanup under, shared with
    /// it: the file name, and the attempts started so far.
    pub(super) trace: Rc<Trace>,
    /// The deadline the task retries until, which shutdown can bring forward.
    limit: Rc<Limit>,
}

impl FlushJob {
    /// Seal off a rotated block: move its data into a write task and keep its
    /// completions.
    ///
    /// The block must already be sealed; the sink refuses an unsealed one and
    /// that refusal is reported as a retryable failure like any other. The
    /// retry deadline is absolute from this point, so a destination that fails
    /// slowly cannot extend it.
    pub(super) fn new(
        data: lake::buffer::Block,
        tokens: Vec<AckToken>,
        sink: Rc<lake::sink::Sink>,
        emitted: [u64; 3],
        retry_deadline: Duration,
        abort_timeout: Duration,
        shared: Rc<FlushShared>,
    ) -> Self {
        let cancel = CancellationToken::new();
        let bytes = data.bytes;
        let window_start_secs = data.window_start_secs;
        let seq = data.seq;
        // Every object of a block shares one file name and differs only in
        // its dataset directory, so the name and the paths are the whole set.
        let paths = sink.planned_paths(&data);
        let trace = Rc::new(Trace {
            file: paths.first().and_then(Path::filename).unwrap_or("").into(),
            paths,
            seq,
            attempts: Cell::new(0),
            shared,
        });
        let started = clock::now();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let limit = Rc::new(Limit {
            own: deadline_at(started, retry_deadline),
            shutdown: Cell::new(None),
            moved: tokio::sync::Notify::new(),
        });
        let handle = tokio::task::spawn_local(write_until(
            sink,
            Rc::new(data),
            cancel.clone(),
            Rc::clone(&limit),
            abort_timeout,
            result_tx,
            Rc::clone(&trace),
        ));
        Self {
            handle,
            result_rx,
            cancel,
            tokens,
            bytes,
            started,
            emitted,
            window_start_secs,
            seq,
            trace,
            limit,
        }
    }

    /// Bring the flush's deadline forward to the latched shutdown deadline
    /// (see [`Limit`]). The earliest deadline wins, so a later one never
    /// extends it.
    pub(super) fn cut_to(&self, deadline: Instant) {
        let cut = self
            .limit
            .shutdown
            .get()
            .map_or(deadline, |old| old.min(deadline));
        self.limit.shutdown.set(Some(cut));
        self.limit.moved.notify_one();
    }

    /// Wait for the flush to be decided.
    ///
    /// Cancellation safe: the decision stays in the channel, so a dropped poll
    /// neither cancels the write nor loses what it returned. This resolves at
    /// the retry deadline even when the attempt in flight has not unwound yet;
    /// [`FlushJob::cleanup`] is what waits for that.
    pub(super) async fn finish(
        &mut self,
    ) -> Result<FlushDone, tokio::sync::oneshot::error::RecvError> {
        (&mut self.result_rx).await
    }

    /// Take the flush's decision if it has already been published.
    ///
    /// Non-blocking, and never waits for the supervising task. The node's
    /// shutdown-deadline branch is biased above the branch that awaits this
    /// result, so the deadline can win the very poll in which a successful
    /// write has already published its report. A block whose files exist has
    /// to be acknowledged rather than nacked, or the producer is told to
    /// resend rows that are already durable.
    ///
    /// `None` also covers a supervising task that died without sending, which
    /// the caller then decides exactly as it decides an unresolved flush.
    pub(super) fn try_finish(&mut self) -> Option<FlushDone> {
        self.result_rx.try_recv().ok()
    }

    /// Whether the supervising task has returned, for a test to wait on.
    #[cfg(test)]
    pub(super) fn task_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// Wait for the supervising task to release everything it owns.
    ///
    /// Cancellation safe, and bounded by the sink's `upload.abort_timeout`
    /// once the decision has been published. The FLUSHING slot stays occupied
    /// until this returns, which is what keeps a next block from writing the
    /// same file names while an abandoned attempt might still be in flight.
    pub(super) async fn cleanup(&mut self) -> Result<(), JoinError> {
        (&mut self.handle).await
    }

    /// Cancel the write and release the task, within `deadline`.
    ///
    /// This is the terminal counterpart of [`FlushJob::cleanup`]: the node has
    /// stopped serving its loop, so nothing will join the task later and
    /// returning while it still owns the block, the sink handle and a
    /// half-finished multipart abort would leave that abort to be cancelled by
    /// runtime teardown. Waiting is bounded because the destination may be the
    /// reason the node is shutting down: past `deadline` the task is aborted
    /// and the abort itself is awaited, so the task is provably gone rather
    /// than merely asked to stop. An aborted task that had started a write
    /// is reported as a cleanup whose abort failed.
    pub(super) async fn shutdown(&mut self, deadline: Instant) {
        self.cancel.cancel();
        let joined = tokio::select! {
            biased;
            joined = &mut self.handle => Some(joined),
            () = clock::sleep_until(deadline) => None,
        };
        if joined.is_none() {
            self.handle.abort();
            let _ = (&mut self.handle).await;
            let attempt = self.trace.attempts.get();
            if attempt != 0 {
                self.trace.abort_failed(
                    attempt,
                    "the write task did not unwind by the cleanup cutoff and was aborted",
                );
            }
        }
    }
}

impl Drop for FlushJob {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
