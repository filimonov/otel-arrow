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
//! as a request to abort the upload rather than finish it, so a dropped owner
//! never leaves a half-written object behind as a completed file.

use super::token::AckToken;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake as lake;
use std::rc::Rc;
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

/// A flush that has resolved, with the block it was writing.
pub(super) struct FlushDone {
    /// The sealed block, handed back so its descriptors and partition are
    /// available to the commit that only a successful write may perform.
    ///
    /// Shared rather than moved, because the supervising task may still be
    /// unwinding a cancelled attempt over the same block when the decision is
    /// published.
    pub(super) data: Rc<lake::buffer::Block<()>>,
    /// What the sink returned.
    pub(super) result: lake::Result<lake::sink::FlushReport>,
    /// Write attempts this flush made, including the one that resolved it.
    pub(super) attempts: u64,
}

/// Whether a failed write may be retried with the identical block.
///
/// Only a failure whose origin is the storage layer can succeed on a retry:
/// the bytes are already encoded and the file names are frozen, so a second
/// attempt differs from the first in nothing but the destination's state. An
/// encoding failure would produce the same failure forever, and a cancellation
/// is a decision that has already been taken rather than a transient fault.
///
/// The Parquet writer wraps whatever the object store returned, so the storage
/// origin of a `Parquet` error is found by walking its source chain rather
/// than by its own variant.
pub(super) fn retryable(error: &lake::Error) -> bool {
    match error {
        lake::Error::ObjectStore(_) => true,
        lake::Error::Parquet(parquet::errors::ParquetError::External(source)) => {
            contains_storage_error(source.as_ref())
        }
        lake::Error::AbortFailed { source, .. } => retryable(source),
        _ => false,
    }
}

/// Whether an error or any of its sources came from storage or I/O.
fn contains_storage_error(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error.is::<object_store::Error>() || error.is::<std::io::Error>() {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

/// Write one sealed block, retrying storage failures until `deadline`.
///
/// The result is published over `result_tx` the moment it is known. The task
/// returns only once it has released everything it owns: on the deadline path
/// that means cancelling the attempt in flight and then waiting at most
/// `abort_timeout` for the sink to unwind it, after which the write future is
/// dropped whether or not it cooperated.
async fn write_until(
    sink: Rc<lake::sink::Sink>,
    data: Rc<lake::buffer::Block<()>>,
    cancel: CancellationToken,
    deadline: Instant,
    abort_timeout: Duration,
    result_tx: tokio::sync::oneshot::Sender<FlushDone>,
) {
    let mut attempts = 0_u64;
    let mut delay = FIRST_BACKOFF;
    // The failure of the last attempt, so the deadline reports what the
    // destination actually said rather than the fact that time ran out.
    let mut last: Option<lake::Error> = None;
    loop {
        if cancel.is_cancelled() || clock::now() >= deadline {
            let error = last
                .take()
                .unwrap_or(lake::Error::Cancelled { abort_error: None });
            let _ = result_tx.send(FlushDone {
                data: Rc::clone(&data),
                attempts,
                result: Err(error),
            });
            return;
        }
        attempts += 1;
        // A child token so the attempt can be cancelled on the deadline
        // without cancelling the job itself, whose token also serves the
        // owner's drop.
        let attempt_cancel = cancel.child_token();
        let write = sink.write_block(&data, &attempt_cancel);
        tokio::pin!(write);
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => None,
            () = clock::sleep_until(deadline) => None,
            result = &mut write => Some(result),
        };
        let Some(result) = result else {
            // Publish the producer decision before awaiting any cleanup. This
            // task independently owns the block, the sink handle and the
            // pinned write future, so the owner is free to release the block's
            // completions while the attempt is still unwinding.
            let _ = result_tx.send(FlushDone {
                data: Rc::clone(&data),
                attempts,
                result: Err(last
                    .take()
                    .unwrap_or(lake::Error::Cancelled { abort_error: None })),
            });
            attempt_cancel.cancel();
            let cleanup_deadline = deadline_at(clock::now(), abort_timeout);
            tokio::select! {
                biased;
                _ = &mut write => {}
                () = clock::sleep_until(cleanup_deadline) => {}
            }
            // Dropping the write after the bound releases the last task-owned
            // resources even if the object store future never cooperates.
            return;
        };
        match result {
            Ok(report) => {
                let _ = result_tx.send(FlushDone {
                    data: Rc::clone(&data),
                    attempts,
                    result: Ok(report),
                });
                return;
            }
            Err(error)
                if retryable(&error) && clock::now() < deadline && !cancel.is_cancelled() =>
            {
                last = Some(error);
                // Never past the deadline: the wait itself must not outlive
                // the bound the block was given.
                let wake = deadline_at(clock::now(), delay).min(deadline);
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => {}
                    () = clock::sleep_until(wake) => {}
                }
                delay = delay.saturating_mul(2).min(MAX_BACKOFF);
            }
            Err(error) => {
                let _ = result_tx.send(FlushDone {
                    data: Rc::clone(&data),
                    attempts,
                    result: Err(error),
                });
                return;
            }
        }
    }
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
        data: lake::buffer::Block<()>,
        tokens: Vec<AckToken>,
        sink: Rc<lake::sink::Sink>,
        emitted: [u64; 3],
        retry_deadline: Duration,
        abort_timeout: Duration,
    ) -> Self {
        let cancel = CancellationToken::new();
        let bytes = data.bytes;
        let started = clock::now();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::task::spawn_local(write_until(
            sink,
            Rc::new(data),
            cancel.clone(),
            deadline_at(started, retry_deadline),
            abort_timeout,
            result_tx,
        ));
        Self {
            handle,
            result_rx,
            cancel,
            tokens,
            bytes,
            started,
            emitted,
        }
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
    /// than merely asked to stop.
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
        }
    }
}

impl Drop for FlushJob {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
