// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Writes a sealed block to an object store as Parquet files (spec sections 5.3 to 5.4, 6.5).

mod naming;
mod properties;
#[cfg(test)]
mod tests;
mod write;

pub use naming::{FileNaming, object_path};
pub use properties::native_sorting_columns;
use write::{FlushWorkspace, MergeKeys};

use crate::config::LakeConfig;
use crate::schema::Dataset;
use object_store::ObjectStore;
use object_store::path::Path;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::time::Instant;

/// Files written for one block.
#[derive(Debug, Default)]
pub struct FlushReport {
    /// Dataset, path and row count per file, in write order.
    pub files: Vec<(Dataset, Path, usize)>,
}

/// Parquet sink.
#[derive(Debug)]
pub struct Sink {
    store: Arc<dyn ObjectStore>,
    cfg: LakeConfig,
    naming: FileNaming,
    clock: SinkClock,
    merge_keys: MergeKeys,
    workspace: FlushWorkspace,
}

/// A sleep future of the sink's clock.
pub type SinkSleep = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// The monotonic clock the sink bounds its cleanup with.
///
/// Injectable so a caller that runs on a simulated clock -- the engine's, in
/// the exporter -- bounds the abort on that same clock rather than on
/// wall-clock tokio time a test cannot advance.
#[derive(Clone, Copy)]
pub struct SinkClock {
    /// The current instant.
    pub now: fn() -> Instant,
    /// A future that completes at the given instant.
    pub sleep_until: fn(Instant) -> SinkSleep,
}

impl Default for SinkClock {
    fn default() -> Self {
        Self {
            now: Instant::now,
            sleep_until: |at| Box::pin(tokio::time::sleep_until(at.into())),
        }
    }
}

impl std::fmt::Debug for SinkClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SinkClock")
    }
}

impl Sink {
    /// New sink.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, cfg: LakeConfig, naming: FileNaming) -> Self {
        Self {
            store,
            cfg,
            naming,
            clock: SinkClock::default(),
            merge_keys: MergeKeys::default(),
            workspace: FlushWorkspace::default(),
        }
    }

    /// Heap the merge of the table being written holds for its sort keys.
    ///
    /// Zero between tables and outside a write. A block's owner adds it to
    /// what it accounts for while the block flushes.
    #[must_use]
    pub fn merge_key_bytes(&self) -> usize {
        self.merge_keys.current.load(AtomicOrdering::Relaxed)
    }

    /// The most `merge_key_bytes` any table write of this sink has held.
    #[must_use]
    pub fn merge_key_high_water_bytes(&self) -> usize {
        self.merge_keys.high_water.load(AtomicOrdering::Relaxed)
    }

    /// Heap the table being written holds beside its block and its merge
    /// keys, right now.
    ///
    /// Three terms: the chunk the merge is producing or has just produced,
    /// the Parquet encoder's in-progress row group, and the upload bytes the
    /// store has not acknowledged yet -- the buffered part and the parts in
    /// flight, each buffer the encoder handed over counted whole until its
    /// last byte has landed, because a part is a slice of that buffer and
    /// keeps all of it alive. Zero between tables and outside a write. A
    /// block's owner adds it to what it accounts for while the block
    /// flushes.
    #[must_use]
    pub fn flush_workspace_bytes(&self) -> usize {
        self.workspace.bytes()
    }

    /// The most `flush_workspace_bytes` seen at any step of any table write
    /// of this sink.
    #[must_use]
    pub fn flush_workspace_high_water_bytes(&self) -> usize {
        let _ = self.workspace.bytes();
        self.workspace.high_water.load(AtomicOrdering::Relaxed)
    }

    /// The same sink, bounding its cleanup on `clock` instead of tokio time.
    #[must_use]
    pub fn with_clock(self, clock: SinkClock) -> Self {
        Self { clock, ..self }
    }
}
