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
//! Dropping the job cancels the write. The sink treats its cancellation token
//! as a request to abort the upload rather than finish it, so a dropped owner
//! never leaves a half-written object behind as a completed file.

use super::token::AckToken;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake as lake;
use std::rc::Rc;
use std::time::Instant;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

/// A flush that has resolved, with the block it was writing.
pub(super) struct FlushDone {
    /// The sealed block, handed back so its descriptors and partition are
    /// available to the commit that only a successful write may perform.
    pub(super) data: lake::buffer::Block<()>,
    /// What the sink returned.
    pub(super) result: lake::Result<lake::sink::FlushReport>,
    /// Write attempts this flush made, including the one that resolved it.
    pub(super) attempts: u64,
}

/// One local flush task and the completions its block still owes.
pub(super) struct FlushJob {
    /// The write task; joined exactly once through [`FlushJob::finish`].
    handle: JoinHandle<FlushDone>,
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
    /// that refusal is reported as a retryable failure like any other.
    pub(super) fn new(
        data: lake::buffer::Block<()>,
        tokens: Vec<AckToken>,
        sink: Rc<lake::sink::Sink>,
        emitted: [u64; 3],
    ) -> Self {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let bytes = data.bytes;
        let handle = tokio::task::spawn_local(async move {
            let result = sink.write_block(&data, &task_cancel).await;
            FlushDone {
                data,
                result,
                attempts: 1,
            }
        });
        Self {
            handle,
            cancel,
            tokens,
            bytes,
            started: clock::now(),
            emitted,
        }
    }

    /// Wait for the write to resolve.
    ///
    /// Cancellation safe: the join handle keeps the task's result, so a
    /// dropped poll neither cancels the write nor loses what it returned.
    pub(super) async fn finish(&mut self) -> Result<FlushDone, JoinError> {
        (&mut self.handle).await
    }
}

impl Drop for FlushJob {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
