// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Writes a sealed block to an object store as Parquet files (spec sections 5.3 to 5.4, 6.5).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use parquet::arrow::{ArrowSchemaConverter, AsyncArrowWriter};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{KeyValue, SortingColumn};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tokio_util::sync::CancellationToken;

use crate::buffer::{Block, SortedTableBuffer};
use crate::clock::PartitionId;
use crate::config::{LakeConfig, Nulls, SortOrder};
use crate::error::{Error, Result, TransientError};
use crate::schema::{Dataset, dataset_schema, schema_fingerprint};
use crate::sort::{MergeBuild, MergeIter, MergeStep, SortSpec};

/// Window length assumed when the configured interval does not fit in `i64`.
const DEFAULT_WINDOW_SECS: i64 = 15;

/// Identity components of file names.
#[derive(Debug, Clone)]
pub struct FileNaming {
    /// Configured writer id.
    pub writer_id: String,
    /// Random id of this process incarnation.
    pub boot_id: String,
}

impl FileNaming {
    /// New naming with a fresh UUIDv4 boot id.
    #[must_use]
    pub fn new(writer_id: &str) -> Self {
        Self {
            writer_id: writer_id.to_string(),
            boot_id: uuid::Uuid::new_v4().simple().to_string(),
        }
    }
}

/// `YYYYMMDDTHHMMSSZ` of a Unix timestamp in seconds.
///
/// Negative input is clamped to the epoch, matching
/// [`PartitionId::from_unix_secs`], so a file name and the Hive partition it
/// sits in never disagree about the instant.
fn utc_stamp(unix_secs: i64) -> String {
    DateTime::<Utc>::from_timestamp(unix_secs.max(0), 0).map_or_else(
        || "19700101T000000Z".to_string(),
        |dt| dt.format("%Y%m%dT%H%M%SZ").to_string(),
    )
}

/// Object path of a dataset file (spec section 5.3).
///
/// Built as one [`Path::from_iter`] over path segments, not a single
/// delimiter-joined string: a `/` inside `naming.writer_id` or
/// `naming.boot_id` is then percent-encoded into the file-name segment
/// instead of splitting it into extra directory levels.
#[must_use]
pub fn object_path(
    ds: Dataset,
    partition: PartitionId,
    window_start_secs: i64,
    naming: &FileNaming,
    seq: u64,
) -> Path {
    Path::from_iter([
        "v=1".to_string(),
        format!("signal={}", ds.signal().as_str()),
        format!("dataset={}", ds.name()),
        format!("date={}", partition.date_string()),
        format!("hour={}", partition.hour_string()),
        format!(
            "part-{}-{}-{}-{seq:08}.parquet",
            utc_stamp(window_start_secs),
            naming.writer_id,
            naming.boot_id,
        ),
    ])
}

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

/// The encoded sort keys the table being written keeps resident.
///
/// A sorted table's merge encodes the key of every row of every run up
/// front and holds them until the merge iterator is dropped, beside the
/// block the rows come from. That is heap the block's own accounting does
/// not include, so the sink publishes it for its owner to charge.
#[derive(Debug, Default)]
struct MergeKeys {
    current: AtomicUsize,
    high_water: AtomicUsize,
}

impl MergeKeys {
    /// Record `bytes` as resident until the returned guard is dropped.
    fn hold(&self, bytes: usize) -> MergeKeysHeld<'_> {
        self.current.store(bytes, AtomicOrdering::Relaxed);
        let _ = self.high_water.fetch_max(bytes, AtomicOrdering::Relaxed);
        MergeKeysHeld(self)
    }
}

/// Clears the resident merge keys when a table write ends, however it ends.
struct MergeKeysHeld<'a>(&'a MergeKeys);

impl MergeKeysHeld<'_> {
    /// Record `bytes` as what the table's merge now holds.
    fn set(&self, bytes: usize) {
        self.0.current.store(bytes, AtomicOrdering::Relaxed);
        let _ = self.0.high_water.fetch_max(bytes, AtomicOrdering::Relaxed);
    }
}

impl Drop for MergeKeysHeld<'_> {
    fn drop(&mut self) {
        self.0.current.store(0, AtomicOrdering::Relaxed);
    }
}

/// The live flush workspace of the table being written.
///
/// The merge and encoder terms are published by the write loop at every
/// step; the upload term is read from the table's ledger when asked, so a
/// part that lands between two steps is released at once.
#[derive(Debug, Default)]
struct FlushWorkspace {
    merge: AtomicUsize,
    encoder: AtomicUsize,
    upload: std::sync::Mutex<Option<Arc<UploadLedger>>>,
    high_water: AtomicUsize,
}

impl FlushWorkspace {
    /// Start accounting one table write, whose upload is `ledger`.
    fn begin(&self, ledger: Arc<UploadLedger>) -> FlushWorkspaceHeld<'_> {
        *self
            .upload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ledger);
        FlushWorkspaceHeld(self)
    }

    fn bytes(&self) -> usize {
        let upload = self
            .upload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |ledger| ledger.live());
        let bytes = self.merge.load(AtomicOrdering::Relaxed)
            + self.encoder.load(AtomicOrdering::Relaxed)
            + upload;
        let _ = self.high_water.fetch_max(bytes, AtomicOrdering::Relaxed);
        bytes
    }
}

/// Clears the flush workspace when a table write ends, however it ends.
struct FlushWorkspaceHeld<'a>(&'a FlushWorkspace);

impl FlushWorkspaceHeld<'_> {
    /// Publish what the merge and the encoder hold at this step.
    fn set(&self, merge: usize, encoder: usize) {
        self.0.merge.store(merge, AtomicOrdering::Relaxed);
        self.0.encoder.store(encoder, AtomicOrdering::Relaxed);
        let _ = self.0.bytes();
    }
}

impl Drop for FlushWorkspaceHeld<'_> {
    fn drop(&mut self) {
        self.0.merge.store(0, AtomicOrdering::Relaxed);
        self.0.encoder.store(0, AtomicOrdering::Relaxed);
        *self
            .0
            .upload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

/// The upload bytes one table write holds: what the encoder handed the
/// buffered upload that the store has not acknowledged yet.
///
/// The encoder hands over one buffer per row group; the buffered upload cuts
/// parts out of that byte stream as slices, without copying, so a buffer
/// stays allocated until every part cut from it has landed. The ledger keeps
/// the stream range of every buffer and of every part, and releases a
/// buffer as soon as all of its bytes have landed, in whatever order the
/// parts land: a stalled early part keeps only the buffers it overlaps
/// charged, never the ones later parts have finished.
#[derive(Debug, Default)]
struct UploadLedger {
    state: std::sync::Mutex<LedgerState>,
}

#[derive(Debug, Default)]
struct LedgerState {
    /// Handed buffers not yet released, by stream start offset: stream end
    /// offset, length, and the bytes of it that have not landed.
    buffers: std::collections::BTreeMap<u64, (u64, usize, usize)>,
    /// Bytes handed so far: the stream offset of the next buffer.
    handed: u64,
    /// Stream offset of the next part.
    next_part: u64,
    /// Sum of the lengths of the buffers not yet released.
    live: usize,
}

/// The stream range `[start, end)` of one multipart part.
type PartSpan = (u64, u64);

impl LedgerState {
    /// The bytes `[start, end)` of the stream landed: take them off every
    /// buffer they overlap and release each buffer none of whose bytes is
    /// still outstanding.
    fn land(&mut self, start: u64, end: u64) {
        let overlapping: Vec<u64> = self
            .buffers
            .range(..end)
            .rev()
            .take_while(|(_, (buffer_end, _, _))| *buffer_end > start)
            .map(|(buffer_start, _)| *buffer_start)
            .collect();
        for buffer_start in overlapping {
            let Some(entry) = self.buffers.get_mut(&buffer_start) else {
                continue;
            };
            let overlap = entry.0.min(end) - buffer_start.max(start);
            entry.2 = entry.2.saturating_sub(overlap as usize);
            if entry.2 == 0 {
                let len = entry.1;
                let _ = self.buffers.remove(&buffer_start);
                self.live -= len;
            }
        }
    }
}

impl UploadLedger {
    fn state(&self) -> std::sync::MutexGuard<'_, LedgerState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The encoder handed the upload a buffer of `len` bytes.
    fn handed(&self, len: usize) {
        if len == 0 {
            return;
        }
        let mut state = self.state();
        let start = state.handed;
        state.handed += len as u64;
        let end = state.handed;
        let _ = state.buffers.insert(start, (end, len, len));
        state.live += len;
    }

    /// A multipart part of `len` bytes was started; returns its stream
    /// range, which [`UploadLedger::part_landed`] takes.
    fn part_started(&self, len: usize) -> PartSpan {
        let mut state = self.state();
        let start = state.next_part;
        state.next_part += len as u64;
        (start, state.next_part)
    }

    /// The part `span` landed, or was dropped with its payload.
    fn part_landed(&self, span: PartSpan) {
        self.state().land(span.0, span.1);
    }

    /// A single-request put of everything handed so far landed, or was
    /// dropped with its payload.
    fn put_landed(&self) {
        let mut state = self.state();
        let handed = state.handed;
        state.land(0, handed);
    }

    /// Bytes the upload holds right now.
    fn live(&self) -> usize {
        self.state().live
    }
}

/// Marks a part landed when its upload future completes or is dropped.
struct PartLanded {
    ledger: Arc<UploadLedger>,
    span: PartSpan,
}

impl Drop for PartLanded {
    fn drop(&mut self) {
        self.ledger.part_landed(self.span);
    }
}

/// Marks a single-request put landed when it completes or is dropped.
struct PutLanded(Arc<UploadLedger>);

impl Drop for PutLanded {
    fn drop(&mut self) {
        self.0.put_landed();
    }
}

/// A multipart upload whose parts are entered in the table's ledger.
#[derive(Debug)]
struct LedgeredUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    ledger: Arc<UploadLedger>,
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for LedgeredUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        let landed = PartLanded {
            span: self.ledger.part_started(data.content_length()),
            ledger: Arc::clone(&self.ledger),
        };
        let part = self.inner.put_part(data);
        Box::pin(async move {
            let _landed = landed;
            part.await
        })
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

/// The encoder's side of one table's upload: every buffer it hands over is
/// entered in the table's ledger.
struct LedgeredWriter {
    inner: ParquetObjectWriter,
    ledger: Arc<UploadLedger>,
}

impl LedgeredWriter {
    /// The buffered upload, for an abort.
    fn into_buf_writer(self) -> BufWriter {
        self.inner.into_inner()
    }
}

impl AsyncFileWriter for LedgeredWriter {
    fn write(&mut self, bs: bytes::Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.ledger.handed(bs.len());
        self.inner.write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.inner.complete()
    }
}

/// Heap the merge's work on `chunk` holds beside the block: what producing
/// it pins, plus the chunk itself when the merge allocated it.
///
/// With sorting disabled a chunk is one of the block's own runs, whose
/// buffers the block already accounts for, so it adds nothing.
fn chunk_charge(merge: &MergeIter, chunk: &RecordBatch) -> usize {
    merge.chunk_workspace_bytes()
        + if merge.allocates_chunks() {
            record_batch_pinned_bytes(chunk, &mut CountedAllocations::default())
        } else {
            0
        }
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

/// The store one table is written through: the sink's own store, counting
/// the multipart uploads it is creating.
///
/// `BufWriter::abort` can only abort an upload whose creation has finished;
/// while it is still being created there is nothing to abort, and dropping the
/// creation leaves an upload at the store that no abort is ever sent for. The
/// count is what lets a cancelled write wait for exactly that creation, and
/// for nothing else it may be blocked on.
#[derive(Debug)]
struct CreationWatch {
    inner: Arc<dyn ObjectStore>,
    creating: AtomicUsize,
    settled: tokio::sync::Notify,
    /// Where the table's upload bytes are entered.
    ledger: Arc<UploadLedger>,
}

/// Counts one multipart creation for as long as it is in flight.
struct Creating<'a>(&'a CreationWatch);

impl Drop for Creating<'_> {
    fn drop(&mut self) {
        let _ = self
            .0
            .creating
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        self.0.settled.notify_waiters();
    }
}

impl CreationWatch {
    fn new(inner: Arc<dyn ObjectStore>, ledger: Arc<UploadLedger>) -> Self {
        Self {
            inner,
            creating: AtomicUsize::new(0),
            settled: tokio::sync::Notify::new(),
            ledger,
        }
    }

    /// Whether a multipart upload is being created right now.
    fn creating(&self) -> bool {
        self.creating.load(std::sync::atomic::Ordering::SeqCst) > 0
    }

    /// Resolves once no multipart upload is being created.
    async fn settled(&self) {
        loop {
            let notified = self.settled.notified();
            tokio::pin!(notified);
            // Registered before the check, so a creation that ends between
            // the check and the wait still wakes it.
            let _ = notified.as_mut().enable();
            if !self.creating() {
                return;
            }
            notified.await;
        }
    }
}

impl std::fmt::Display for CreationWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CreationWatch {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let _landed = PutLanded(Arc::clone(&self.ledger));
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let _ = self
            .creating
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _creating = Creating(self);
        let inner = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(LedgeredUpload {
            inner,
            ledger: Arc::clone(&self.ledger),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// `base + delta`, saturated at a year out rather than panicking on overflow.
fn deadline_after(base: Instant, delta: Duration) -> Instant {
    base.checked_add(delta)
        .or_else(|| base.checked_add(Duration::from_secs(365 * 24 * 60 * 60)))
        .unwrap_or(base)
}

/// Smallest and largest `time_unix_nano` across the sealed runs of a table.
///
/// A column of an unexpected type is skipped rather than panicked on: the
/// metadata it feeds is advisory, and no data-derived input may panic here.
fn time_range(batches: &[RecordBatch]) -> (Option<i64>, Option<i64>) {
    let mut lo = None;
    let mut hi = None;
    for c in batches {
        if let Some(a) = c
            .column_by_name("time_unix_nano")
            .and_then(|col| col.as_primitive_opt::<Int64Type>())
        {
            if let Some(mn) = arrow::compute::min(a) {
                lo = Some(lo.map_or(mn, |x: i64| x.min(mn)));
            }
            if let Some(mx) = arrow::compute::max(a) {
                hi = Some(hi.map_or(mx, |x: i64| x.max(mx)));
            }
        }
    }
    (lo, hi)
}

/// Parquet's native `SortingColumn` list for a file sorted by `spec`.
///
/// The list is written into every row group beside the `sort_key` key/value
/// (FORMAT.md section 5), so readers that understand the standard field --
/// DataFusion, DuckDB, ClickHouse -- can use the order without knowing this
/// format. `column_idx` is the index of the column among the Parquet leaf
/// columns, not the Arrow field index: a map column has two leaves and a list
/// column one, so every column after a map shifts by one.
///
/// A `SortingColumn` can only describe a leaf column, and the list is
/// lexicographic, so it holds the longest prefix of `spec` whose columns are
/// top-level primitive, non-floating-point columns, and stops at the first key
/// that is not:
///
/// - a list column, which the row converter can sort as a whole but no leaf
///   order describes;
/// - a floating-point column, which the merge sorts on a normalized copy where
///   `-0.0` equals `+0.0` and every NaN equals every other NaN. Parquet's
///   recommended IEEE 754 total order puts `-0.0` before `+0.0` and gives NaN
///   payloads distinct positions, so declaring such a column sorted would be
///   false, and a false declaration is worse than none.
///
/// Nothing after the first excluded key is declared, since the keys after it
/// are only ordered within its ties. `None` when the prefix is empty,
/// including when sorting is disabled; `sort_key` stays the complete
/// description either way.
///
/// # Errors
/// Returns the Parquet error when the schema cannot be converted, which
/// cannot happen for a dataset schema the writer itself accepts.
pub fn native_sorting_columns(
    spec: &SortSpec,
    schema: &Schema,
) -> Result<Option<Vec<SortingColumn>>> {
    let descr = ArrowSchemaConverter::new().convert(schema)?;
    let mut out = Vec::with_capacity(spec.keys().len());
    for key in spec.keys() {
        let floating = schema
            .field_with_name(&key.column)
            .is_ok_and(|f| f.data_type().is_floating());
        if floating {
            break;
        }
        let leaf = descr.columns().iter().position(|c| {
            let parts = c.path().parts();
            parts.len() == 1 && parts[0] == key.column
        });
        let Some(leaf) = leaf.and_then(|i| i32::try_from(i).ok()) else {
            break;
        };
        out.push(SortingColumn {
            column_idx: leaf,
            descending: key.order == SortOrder::Desc,
            nulls_first: key.nulls == Nulls::First,
        });
    }
    Ok((!out.is_empty()).then_some(out))
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

    /// File metadata of spec section 5.4.
    ///
    /// `rows` and `time_range` are computed from the sealed runs before the merge,
    /// because the merged chunks are produced lazily and are not all available at
    /// the time the writer properties are built.
    #[must_use]
    pub fn file_metadata(
        &self,
        table: &SortedTableBuffer,
        rows: usize,
        time_range: (Option<i64>, Option<i64>),
        seq: u64,
        window_start_secs: i64,
    ) -> Vec<KeyValue> {
        let schema = dataset_schema(table.dataset(), &self.cfg);
        let window_secs =
            i64::try_from(self.cfg.window_interval.as_secs()).unwrap_or(DEFAULT_WINDOW_SECS);
        let mut kv = vec![
            KeyValue::new("format_version".into(), "1".to_string()),
            KeyValue::new("series_hash".into(), "xxh3_128/canonical_v1".to_string()),
            KeyValue::new(
                "schema_fingerprint".into(),
                format!("{:016x}", schema_fingerprint(&schema)),
            ),
            KeyValue::new("sort_key".into(), table.spec().metadata_string()),
            KeyValue::new("writer_id".into(), self.naming.writer_id.clone()),
            KeyValue::new("boot_id".into(), self.naming.boot_id.clone()),
            KeyValue::new("seq".into(), seq.to_string()),
            KeyValue::new("window_start".into(), window_start_secs.to_string()),
            KeyValue::new(
                "window_end".into(),
                window_start_secs.saturating_add(window_secs).to_string(),
            ),
            KeyValue::new("row_count".into(), rows.to_string()),
        ];
        if !table.dataset().is_series()
            && let (Some(lo), Some(hi)) = time_range
        {
            kv.push(KeyValue::new("min_time_unix_nano".into(), lo.to_string()));
            kv.push(KeyValue::new("max_time_unix_nano".into(), hi.to_string()));
        }
        kv
    }

    /// Best-effort abort of a still-writable upload, by `deadline`.
    ///
    /// Bounded, on the sink's clock, so a wedged store cannot block the flush
    /// task. Returns why the abort did not succeed, or `None` when it did.
    async fn abort_upload(
        &self,
        writer: AsyncArrowWriter<LedgeredWriter>,
        deadline: Instant,
    ) -> Option<String> {
        let mut buf: BufWriter = writer.into_inner().into_buf_writer();
        tokio::select! {
            biased;
            aborted = buf.abort() => aborted.err().map(|e| e.to_string()),
            () = (self.clock.sleep_until)(deadline) => Some(format!(
                "abort timed out after {:?}",
                self.cfg.upload.abort_timeout
            )),
        }
    }

    /// Run one writable-phase writer step, racing `cancel`.
    ///
    /// A step that is creating the multipart upload when the token fires is
    /// not dropped at once: `BufWriter::abort` has nothing to abort until that
    /// creation has finished, so dropping it would leave an upload at the
    /// store with no abort ever sent for it. The step keeps being driven until
    /// the creation has finished or the cleanup deadline -- taken once, at the
    /// first cancellation, and shared with the abort that follows -- passes. A
    /// step blocked on anything else, such as a part that does not land, is
    /// dropped at once so the abort starts with the whole allowance. The step
    /// reports `Cancelled` either way.
    async fn step(
        &self,
        op: impl Future<Output = parquet::errors::Result<()>>,
        watch: &CreationWatch,
        cancel: &CancellationToken,
        cleanup: &mut Option<Instant>,
    ) -> Result<()> {
        tokio::pin!(op);
        // The token is polled first: once it has fired, the step is not
        // driven again, so a writer step that would have made more progress
        // on this poll does not run before the cancellation is acted on.
        tokio::select! {
            biased;
            () = cancel.cancelled() => {}
            result = &mut op => return result.map_err(Error::from),
        }
        let deadline = *cleanup.get_or_insert_with(|| self.cleanup_deadline());
        if !watch.creating() {
            return Err(Error::cancelled(None));
        }
        tokio::select! {
            biased;
            _ = &mut op => Err(Error::cancelled(None)),
            () = watch.settled() => Err(Error::cancelled(None)),
            () = (self.clock.sleep_until)(deadline) => Err(Error::cancelled(Some(format!(
                "the write in flight did not finish within {:?}, so a multipart upload \
                 it was creating may be left to the bucket lifecycle rule",
                self.cfg.upload.abort_timeout
            )))),
        }
    }

    /// The cancellation a table write has just observed at one of its step
    /// boundaries.
    ///
    /// The cleanup deadline is taken here, where the token is seen, rather
    /// than after the write has released its merge, so dropping a large
    /// merge is spent out of the abort's allowance instead of postponing
    /// the start of it.
    fn cancelled_here(&self, cleanup: &mut Option<Instant>) -> Error {
        let _ = cleanup.get_or_insert_with(|| self.cleanup_deadline());
        Error::cancelled(None)
    }

    /// The instant the cleanup of a failed or cancelled write must end by.
    fn cleanup_deadline(&self) -> Instant {
        deadline_after((self.clock.now)(), self.cfg.upload.abort_timeout)
    }

    /// Attach the outcome of the cleanup abort to the failure that triggered it.
    ///
    /// A cancellation that already carries a cleanup failure keeps it: the
    /// abort that follows cannot see what the unsettled write left behind.
    fn with_abort(cause: Error, abort_error: Option<String>) -> Error {
        match (cause, abort_error) {
            (
                Error::Transient(TransientError::Cancelled {
                    abort_error: earlier,
                }),
                abort_error,
            ) => Error::cancelled(earlier.or(abort_error)),
            (cause, None) => cause,
            (cause, Some(abort_error)) => Error::Transient(TransientError::AbortFailed {
                source: Box::new(cause),
                abort_error,
            }),
        }
    }

    async fn write_table(
        &self,
        table: &SortedTableBuffer,
        path: &Path,
        seq: u64,
        window_start_secs: i64,
        cancel: &CancellationToken,
    ) -> Result<usize> {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let total_rows: usize = runs.iter().map(RecordBatch::num_rows).sum();
        // A series table has no `time_unix_nano` column, so the scan would be
        // pure overhead and the metadata keys it feeds are values-only anyway.
        let range = if table.dataset().is_series() {
            (None, None)
        } else {
            time_range(&runs)
        };
        let schema = dataset_schema(table.dataset(), &self.cfg);
        let sorting = native_sorting_columns(table.spec(), &schema)?;
        // Spec 5.4 asks for ZSTD, statistics and dictionary encoding explicitly
        // rather than by relying on arrow-rs defaults. An unlimited row count
        // disables the row-count-based split so that the byte-driven flush below
        // owns row group boundaries.
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(true)
            .set_max_row_group_row_count(None)
            .set_sorting_columns(sorting)
            .set_key_value_metadata(Some(self.file_metadata(
                table,
                total_rows,
                range,
                seq,
                window_start_secs,
            )))
            .build();
        let ledger = Arc::new(UploadLedger::default());
        let watch = Arc::new(CreationWatch::new(self.store.clone(), Arc::clone(&ledger)));
        let buf = BufWriter::with_capacity(
            Arc::clone(&watch) as Arc<dyn ObjectStore>,
            path.clone(),
            self.cfg.upload.part_bytes,
        )
        .with_max_concurrency(self.cfg.upload.concurrency);
        let object_writer = LedgeredWriter {
            inner: ParquetObjectWriter::from_buf_writer(buf),
            ledger: Arc::clone(&ledger),
        };
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;
        // Held until this function returns: the merge chunk, the encoder and
        // the upload are the table's own, and none outlives its write.
        let workspace = self.workspace.begin(ledger);

        // Phase 1: the writer is writable. Every await races the token, and every
        // failure aborts the multipart upload.
        let mut rows = 0usize;
        let mut failure: Option<Error> = None;
        let mut cleanup: Option<Instant> = None;
        // The whole write is a sequence of bounded steps with a return to the
        // runtime and a cancellation check between every two of them. The
        // table's task shares its thread with the node loop that admits
        // requests, delivers acks and nacks, answers telemetry and watches the
        // shutdown deadline, so a step is the longest that loop can be kept
        // waiting. Nothing about the steps reaches the file: the chunks, their
        // order and the writer calls are the same as an unsliced write makes.
        //
        // `AsyncArrowWriter::write` usually completes synchronously, and so
        // does a buffered upload whose store is ready, which is why every
        // step yields explicitly rather than relying on the store to suspend.
        let mut build = MergeBuild::new(runs, table.spec(), self.cfg.sorting.merge_chunk_bytes)?;
        // Held until this function returns, which is when the keys are
        // dropped. Raised as the keys are encoded, then set once to the
        // merge's bound for its whole life, so no later chunk is
        // under-charged.
        let keys = self.merge_keys.hold(0);
        // Step 1: encode the merge keys, one bounded slice at a time.
        loop {
            match build.step() {
                Ok(true) => break,
                Ok(false) => keys.set(build.resident_key_bytes()),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                failure = Some(self.cancelled_here(&mut cleanup));
                break;
            }
        }
        let mut merged = match failure {
            Some(_) => None,
            None => match build.finish() {
                Ok(merged) => Some(merged),
                Err(e) => {
                    failure = Some(e);
                    None
                }
            },
        };
        if let Some(merged) = &merged {
            keys.set(merged.resident_key_bytes());
        }
        // Step 2: produce, encode and flush the chunks, one bounded step at a
        // time.
        while let Some(merge) = merged.as_mut() {
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                failure = Some(self.cancelled_here(&mut cleanup));
                break;
            }
            let chunk = match merge.step() {
                Ok(MergeStep::Done) => break,
                Ok(MergeStep::Progress) => {
                    workspace.set(merge.chunk_workspace_bytes(), writer.memory_size());
                    continue;
                }
                Ok(MergeStep::Chunk(c)) => c,
                Ok(MergeStep::Ready) => {
                    // Build the chunk's columns in bounded steps of their
                    // own, returning to the runtime between every two.
                    let built = {
                        let mut builder = merge.chunk_builder();
                        loop {
                            match builder.step() {
                                Ok(true) => break builder.finish(),
                                Ok(false) => {}
                                Err(e) => break Err(e),
                            }
                            workspace.set(
                                merge.chunk_workspace_bytes() + builder.workspace_bytes(),
                                writer.memory_size(),
                            );
                            tokio::task::yield_now().await;
                            if cancel.is_cancelled() {
                                break Err(self.cancelled_here(&mut cleanup));
                            }
                        }
                    };
                    match built {
                        Ok(chunk) => {
                            merge.chunk_taken();
                            chunk
                        }
                        Err(e) => {
                            failure = Some(e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            workspace.set(chunk_charge(merge, &chunk), writer.memory_size());
            // Encoding one chunk is a stretch of its own, bounded by
            // `merge_chunk_bytes`; it does not follow the step that produced
            // the chunk without a return to the runtime in between.
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                failure = Some(self.cancelled_here(&mut cleanup));
                break;
            }
            let step = self
                .step(writer.write(&chunk), &watch, cancel, &mut cleanup)
                .await;
            if let Err(e) = step {
                failure = Some(e);
                break;
            }
            rows += chunk.num_rows();
            drop(chunk);
            workspace.set(merge.chunk_workspace_bytes(), writer.memory_size());
            if writer.memory_size() >= self.cfg.parquet.writer_limit_bytes
                || writer.in_progress_size() >= self.cfg.parquet.row_group_bytes
            {
                // Closing a row group is the other stretch, bounded by
                // `row_group_bytes`, and it too runs in a poll of its own.
                tokio::task::yield_now().await;
                if cancel.is_cancelled() {
                    failure = Some(self.cancelled_here(&mut cleanup));
                    break;
                }
                let step = self
                    .step(writer.flush(), &watch, cancel, &mut cleanup)
                    .await;
                if let Err(e) = step {
                    failure = Some(e);
                    break;
                }
                workspace.set(merge.chunk_workspace_bytes(), writer.memory_size());
            }
        }
        drop(merged);
        workspace.set(0, writer.memory_size());
        if let Some(cause) = failure {
            let deadline = cleanup.unwrap_or_else(|| self.cleanup_deadline());
            let abort_error = self.abort_upload(writer, deadline).await;
            return Err(Self::with_abort(cause, abort_error));
        }
        if cancel.is_cancelled() {
            let abort_error = self.abort_upload(writer, self.cleanup_deadline()).await;
            return Err(Error::cancelled(abort_error));
        }

        // Phase 2: finalizing. `finish` writes the footer and shuts the BufWriter
        // down; `BufWriter::abort` panics once shutdown has started, so nothing is
        // aborted from here on. A partial multipart upload left by a cancellation
        // in this phase is reclaimed by the bucket's multipart lifecycle rule
        // (spec 5.3), not by this crate.
        let finish = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(Error::cancelled(None)),
            r = writer.finish() => r.map(|_metadata| ()).map_err(Error::from),
        };
        finish?;
        Ok(rows)
    }

    /// Object paths this sink will write for `block`, in write order.
    ///
    /// The names follow from the block's own identity -- its partition, its
    /// window, its sequence and this sink's writer and boot ids -- and never
    /// from the attempt that writes them. That is what makes a retry rewrite
    /// the same objects instead of adding a second copy, and it lets a caller
    /// name the objects an attempt is about to touch before it touches them.
    #[must_use]
    pub fn planned_paths(&self, block: &Block) -> Vec<Path> {
        block
            .tables()
            .filter(|table| !table.is_empty())
            .map(|table| {
                object_path(
                    table.dataset(),
                    block.partition,
                    block.window_start_secs,
                    &self.naming,
                    block.seq,
                )
            })
            .collect()
    }

    /// Write every non-empty table of a sealed block, series datasets first.
    ///
    /// # Errors
    ///
    /// Returns [`TransientError::Cancelled`] when `cancel` fires, and the underlying
    /// Arrow, Parquet or object store failure otherwise.
    pub async fn write_block(
        &self,
        block: &Block,
        cancel: &CancellationToken,
    ) -> Result<FlushReport> {
        // Series rows exist from admission onwards, but they carry a
        // placeholder `emitted_at` of zero until the block's stamp transaction
        // commits. Writing an unsealed block would therefore publish unstamped
        // data. This is a runtime check, not a debug assertion: the files would
        // be just as wrong in a release build.
        if !block.is_sealed() {
            return Err(Error::internal("unsealed block"));
        }
        if cancel.is_cancelled() {
            return Err(Error::cancelled(None));
        }
        let mut report = FlushReport::default();
        for (table, path) in block
            .tables()
            .filter(|table| !table.is_empty())
            .zip(self.planned_paths(block))
        {
            let rows = self
                .write_table(table, &path, block.seq, block.window_start_secs, cancel)
                .await?;
            report.files.push((table.dataset(), path, rows));
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Block;
    use crate::cache::SeriesCache;
    use crate::config::LakeConfig;
    use crate::extract::extract;
    use crate::sort::merge_runs;
    use futures::stream::BoxStream;
    use object_store::local::LocalFileSystem;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
        AnyValue, KeyValue as OtlpKeyValue, any_value,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::{encode_logs, encode_metrics};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    const WINDOW_START: i64 = 1_789_960_500;
    const SEAL_AT_US: i64 = 1_789_960_500_000_000;
    const SEQ: u64 = 7;
    /// First `time_unix_nano` of the generated log records; they count down.
    const FIRST_LOG_TIME: u64 = 9_000_000;

    fn kv(k: &str, v: &str) -> OtlpKeyValue {
        OtlpKeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        }
    }

    /// `n` log records whose bodies are `body_len` hex characters wide.
    fn logs(n: usize, body_len: usize) -> LogsData {
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..n)
                        .map(|i| LogRecord {
                            time_unix_nano: FIRST_LOG_TIME - i as u64,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(body(i, body_len))),
                            }),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// A body of `len` hex characters that differs for every `i`.
    ///
    /// The bytes come from a splitmix-style mix so that ZSTD cannot fold the
    /// payload away: the upload tests need the written object to actually pass
    /// the 5 MiB multipart threshold.
    fn body(i: usize, len: usize) -> String {
        let mut s = String::with_capacity(len);
        let mut x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        while s.len() < len {
            x ^= x >> 33;
            x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
            s.push_str(&format!("{x:016x}"));
        }
        s.truncate(len);
        s
    }

    fn seal_logs(cfg: &LakeConfig, n: usize, body_len: usize) -> Block {
        let mut cache = SeriesCache::new(10);
        let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
        let mut records = encode_logs(&logs(n, body_len));
        let e = extract(&mut records, cfg).expect("extract");
        let r = b.reserve(&e, &mut cache, 8).expect("reserve");
        b.admit(e, r).expect("admit");
        b.seal(SEAL_AT_US).expect("seal");
        b
    }

    /// A sealed block of 30 small log rows.
    fn sealed_block(cfg: &LakeConfig, n: usize) -> Block {
        seal_logs(cfg, n, 8)
    }

    /// A sealed block whose values object is comfortably larger than one 5 MiB
    /// multipart part, so that the upload tests reach an in-flight `put_part`
    /// while the writer is still in its writable phase.
    fn sealed_upload_block(cfg: &LakeConfig) -> Block {
        seal_logs(cfg, 24_000, 1_024)
    }

    /// Config whose writer hands the object store several parts during the
    /// chunk loop: real 5 MiB parts, one in-flight part at a time, and row
    /// groups small enough that the loop flushes repeatedly.
    fn upload_config() -> LakeConfig {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_request_bytes = 64 << 20;
        cfg.ingress.max_extracted_bytes = 64 << 20;
        cfg.upload.part_bytes = 5 << 20;
        cfg.upload.concurrency = 1;
        cfg.parquet.row_group_bytes = 256 << 10;
        cfg.sorting.merge_chunk_bytes = 512 << 10;
        cfg.validate().expect("valid config");
        cfg
    }

    fn local(dir: &tempfile::TempDir) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("fs"))
    }

    fn naming(writer_id: &str, boot_id: &str) -> FileNaming {
        FileNaming {
            writer_id: writer_id.into(),
            boot_id: boot_id.into(),
        }
    }

    fn injected() -> object_store::Error {
        object_store::Error::Generic {
            store: "test",
            source: "injected failure".into(),
        }
    }

    /// What the wrapping multipart upload does with each part.
    #[derive(Debug, Clone, Copy)]
    enum PartBehavior {
        /// Never resolve, leaving the part in flight.
        Park,
        /// Fail immediately.
        Fail,
    }

    /// What the wrapping multipart upload does when it is aborted.
    #[derive(Debug, Clone, Copy)]
    enum AbortBehavior {
        /// Delegate to the real upload.
        Delegate,
        /// Fail the abort.
        Fail,
        /// Never resolve, so only the abort timeout ends it.
        Hang,
    }

    /// An `ObjectStore` that starts real multipart uploads and then controls how
    /// their parts and aborts behave.
    ///
    /// `put_multipart_opts` delegates immediately, unlike a wrapper that parks
    /// before delegating: the `BufWriter` must actually reach its `Write` state,
    /// because that is the only state in which `BufWriter::abort` does anything.
    #[derive(Debug)]
    struct ControlledMultipart {
        inner: Arc<dyn ObjectStore>,
        /// Notified when a part upload has started.
        entered: Arc<Notify>,
        /// Set once the upload has been aborted.
        aborted: Arc<AtomicBool>,
        /// Number of parts the writer handed to the upload.
        parts: Arc<AtomicUsize>,
        part: PartBehavior,
        abort: AbortBehavior,
    }

    /// The upload handed back by [`ControlledMultipart`].
    #[derive(Debug)]
    struct ControlledUpload {
        inner: Box<dyn MultipartUpload>,
        entered: Arc<Notify>,
        aborted: Arc<AtomicBool>,
        parts: Arc<AtomicUsize>,
        part: PartBehavior,
        abort: AbortBehavior,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for ControlledUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            let _ = self.parts.fetch_add(1, Ordering::SeqCst);
            // A permit, not a broadcast: the canceller must observe the part
            // even if it subscribes afterwards.
            self.entered.notify_one();
            match self.part {
                PartBehavior::Park => Box::pin(std::future::pending()),
                PartBehavior::Fail => Box::pin(std::future::ready(Err(injected()))),
            }
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborted.store(true, Ordering::SeqCst);
            match self.abort {
                AbortBehavior::Delegate => self.inner.abort().await,
                AbortBehavior::Fail => {
                    let _ = self.inner.abort().await;
                    Err(injected())
                }
                AbortBehavior::Hang => std::future::pending().await,
            }
        }
    }

    impl std::fmt::Display for ControlledMultipart {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ControlledMultipart({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ControlledMultipart {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, options).await?;
            Ok(Box::new(ControlledUpload {
                inner,
                entered: self.entered.clone(),
                aborted: self.aborted.clone(),
                parts: self.parts.clone(),
                part: self.part,
                abort: self.abort,
            }))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// An `ObjectStore` whose multipart creation is slow: it cancels a token as
    /// the values upload is being created and yields before the creation
    /// finishes, so the cancellation lands while `BufWriter` is still
    /// preparing the upload.
    #[derive(Debug)]
    struct CancelDuringCreate {
        inner: Arc<dyn ObjectStore>,
        token: CancellationToken,
    }

    impl std::fmt::Display for CancelDuringCreate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CancelDuringCreate({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CancelDuringCreate {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            if location.as_ref().contains("dataset=values") {
                self.token.cancel();
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
            }
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// An `ObjectStore` that cancels a token the first time the values dataset
    /// object is opened, so a test can land a cancellation inside the chunk loop
    /// without depending on the scheduler.
    #[derive(Debug)]
    struct CancelOnValues {
        inner: Arc<dyn ObjectStore>,
        token: CancellationToken,
        /// Set when the trip happened on a multipart upload, which only starts
        /// while the writer is still writable.
        tripped_multipart: Arc<AtomicBool>,
    }

    impl CancelOnValues {
        fn trip(&self, location: &Path, multipart: bool) -> bool {
            if !location.as_ref().contains("dataset=values") {
                return false;
            }
            if multipart {
                self.tripped_multipart.store(true, Ordering::SeqCst);
            }
            self.token.cancel();
            true
        }
    }

    impl std::fmt::Display for CancelOnValues {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CancelOnValues({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CancelOnValues {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            let _ = self.trip(location, false);
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let _ = self.trip(location, true);
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Parquet files under `root` whose path contains `needle`.
    fn parquet_count(root: &std::path::Path, needle: &str) -> usize {
        fn walk(p: &std::path::Path, needle: &str, n: &mut usize) {
            for e in std::fs::read_dir(p).expect("dir") {
                let e = e.expect("entry");
                let path = e.path();
                if path.is_dir() {
                    walk(&path, needle, n);
                } else if path.extension().is_some_and(|x| x == "parquet")
                    && path.to_string_lossy().contains(needle)
                {
                    *n += 1;
                }
            }
        }
        let mut n = 0;
        walk(root, needle, &mut n);
        n
    }

    /// Every Parquet file under `root`.
    fn walkdir_count(root: &std::path::Path) -> usize {
        parquet_count(root, "")
    }

    /// Key-value metadata of a written file, as a lookup closure.
    fn file_kv(root: &std::path::Path, path: &Path) -> Vec<(String, String)> {
        let file = std::fs::File::open(root.join(path.as_ref())).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("kv")
            .iter()
            .map(|e| (e.key.clone(), e.value.clone().unwrap_or_default()))
            .collect()
    }

    fn get<'a>(kv: &'a [(String, String)], key: &str) -> &'a str {
        kv.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or_else(|| panic!("missing metadata key {key}"))
    }

    /// The native `SortingColumn` list of every row group of a written file,
    /// as `(leaf index, descending, nulls_first)`.
    fn row_group_sorting(
        root: &std::path::Path,
        path: &Path,
    ) -> Vec<Option<Vec<(i32, bool, bool)>>> {
        let file = std::fs::File::open(root.join(path.as_ref())).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        reader
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| {
                rg.sorting_columns().map(|cols| {
                    cols.iter()
                        .map(|c| (c.column_idx, c.descending, c.nulls_first))
                        .collect()
                })
            })
            .collect()
    }

    /// Scenario: sort specifications over the logs and metrics values schemas:
    /// a key after the two-leaf `attrs` map, a descending nulls-first key, a
    /// list key in the middle of the spec, a list key first, a double key in
    /// the middle and first, and no keys.
    /// Guarantees: `column_idx` is the Parquet leaf index, not the Arrow field
    /// index; order and null placement are carried over; the list stops at the
    /// first list or floating-point key, so the native list never claims an
    /// order no leaf column has or an IEEE 754 total order the normalized
    /// double sort does not produce; and an empty prefix emits no list at all.
    #[test]
    fn native_sorting_columns_use_leaf_indexes_and_a_primitive_prefix() {
        use crate::config::{DenormType, Denormalize, SortKey};
        let key = |column: &str, order, nulls| SortKey {
            column: column.into(),
            order,
            nulls,
        };
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![Denormalize {
            path: "resource.service.name".into(),
            column: "service_name".into(),
            ty: DenormType::String,
        }];
        let logs = dataset_schema(Dataset::LogsValues, &cfg);
        // Arrow field 14; `attrs` (field 13) has two leaves, so leaf 15.
        assert_eq!(logs.index_of("service_name").expect("field"), 14);
        let spec = SortSpec::new(vec![
            key("service_name", SortOrder::Asc, Nulls::Last),
            key("time_unix_nano", SortOrder::Desc, Nulls::First),
        ]);
        let cols = native_sorting_columns(&spec, &logs)
            .expect("convert")
            .expect("two keys");
        let got: Vec<_> = cols
            .iter()
            .map(|c| (c.column_idx, c.descending, c.nulls_first))
            .collect();
        assert_eq!(got, [(15, false, false), (3, true, true)]);

        let metrics = dataset_schema(Dataset::MetricsValues, &cfg);
        let spec = SortSpec::new(vec![
            key("series_id", SortOrder::Asc, Nulls::Last),
            key("bucket_counts", SortOrder::Asc, Nulls::Last),
            key("time_unix_nano", SortOrder::Asc, Nulls::Last),
        ]);
        let cols = native_sorting_columns(&spec, &metrics)
            .expect("convert")
            .expect("one key");
        assert_eq!(cols.len(), 1, "stops at the list key");
        assert_eq!(cols[0].column_idx, 0);
        let spec = SortSpec::new(vec![key("bucket_counts", SortOrder::Asc, Nulls::Last)]);
        assert_eq!(
            native_sorting_columns(&spec, &metrics).expect("convert"),
            None
        );
        let spec = SortSpec::new(vec![
            key("series_id", SortOrder::Asc, Nulls::Last),
            key("value_double", SortOrder::Asc, Nulls::Last),
            key("time_unix_nano", SortOrder::Asc, Nulls::Last),
        ]);
        let cols = native_sorting_columns(&spec, &metrics)
            .expect("convert")
            .expect("one key");
        assert_eq!(cols.len(), 1, "stops at the double key");
        assert_eq!(cols[0].column_idx, 0);
        let spec = SortSpec::new(vec![key("value_double", SortOrder::Asc, Nulls::Last)]);
        assert_eq!(
            native_sorting_columns(&spec, &metrics).expect("convert"),
            None
        );
        assert_eq!(
            native_sorting_columns(&SortSpec::new(Vec::new()), &metrics).expect("convert"),
            None
        );
    }

    /// Scenario: a sealed block of 30 log records, whose values table merges
    /// on `series_id` and `time_unix_nano`, is written.
    /// Guarantees: while a table is written the sink reports the heap its
    /// merge holds for the encoded keys -- exactly what the largest table's
    /// merge iterator reports, at least one null byte plus the value bytes of
    /// both keys for every row -- and nothing once the write has returned, so
    /// the exporter can charge the keys for as long as they are resident.
    #[tokio::test]
    async fn the_sink_reports_the_merge_keys_it_holds() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let b = sealed_block(&cfg, 30);
        let expected = b
            .tables()
            .filter(|table| !table.is_empty())
            .map(|table| {
                merge_runs(
                    table.iter_snapshots().cloned().collect(),
                    table.spec(),
                    cfg.sorting.merge_chunk_bytes,
                )
                .expect("merge")
                .resident_key_bytes()
            })
            .max()
            .expect("a table");
        let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "keys"));
        assert_eq!(sink.merge_key_high_water_bytes(), 0);
        let _ = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert_eq!(sink.merge_key_bytes(), 0, "no table is being written");
        assert_eq!(sink.merge_key_high_water_bytes(), expected);
        assert!(
            expected >= 30 * ((1 + 16) + (1 + 8)),
            "{expected} key bytes"
        );
    }

    /// Scenario: a 30-row logs block written with the default sort and with
    /// row groups forced down to a few rows each, then the same block with
    /// values sorting disabled.
    /// Guarantees: every row group of the values file carries the native
    /// `SortingColumn` list of the default `series_id, time_unix_nano` sort
    /// (Parquet leaves 0 and 3, ascending, nulls last) beside the `sort_key`
    /// key/value, every row group of the series file carries `series_id`
    /// ascending, and an unsorted values file carries no native list while its
    /// series file still does. A values sort whose second key is a
    /// denormalized double column declares only `series_id` natively, while
    /// `sort_key` still names all three keys.
    #[tokio::test]
    async fn every_row_group_carries_the_native_sorting_columns() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut cfg = LakeConfig::default();
        cfg.parquet.row_group_bytes = 1;
        cfg.sorting.merge_chunk_bytes = 1;
        let b = sealed_block(&cfg, 30);
        let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "sorted"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        let values = row_group_sorting(dir.path(), &report.files[1].1);
        assert!(values.len() > 1, "row groups: {}", values.len());
        for rg in &values {
            assert_eq!(
                rg.as_deref(),
                Some(&[(0, false, false), (3, false, false)][..])
            );
        }
        assert_eq!(
            get(&file_kv(dir.path(), &report.files[1].1), "sort_key"),
            "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last"
        );
        for rg in row_group_sorting(dir.path(), &report.files[0].1) {
            assert_eq!(rg.as_deref(), Some(&[(0, false, false)][..]));
        }

        let mut unsorted = LakeConfig::default();
        unsorted.logs.values_sort = Vec::new();
        let b = sealed_block(&unsorted, 30);
        let sink = Sink::new(local(&dir), unsorted, naming("w", "unsorted"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert_eq!(
            get(&file_kv(dir.path(), &report.files[1].1), "sort_key"),
            "none"
        );
        for rg in row_group_sorting(dir.path(), &report.files[1].1) {
            assert_eq!(rg, None);
        }
        for rg in row_group_sorting(dir.path(), &report.files[0].1) {
            assert_eq!(rg.as_deref(), Some(&[(0, false, false)][..]));
        }

        let key = |column: &str| crate::config::SortKey {
            column: column.into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        };
        let mut double = LakeConfig::default();
        double.logs.denormalize = vec![crate::config::Denormalize {
            path: "attrs.latency".into(),
            column: "latency".into(),
            ty: crate::config::DenormType::Double,
        }];
        double.logs.values_sort = vec![key("series_id"), key("latency"), key("time_unix_nano")];
        double.validate().expect("valid double sort");
        let b = sealed_block(&double, 30);
        let sink = Sink::new(local(&dir), double, naming("w", "double"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert_eq!(
            get(&file_kv(dir.path(), &report.files[1].1), "sort_key"),
            "series_id:asc:nulls_last,latency:asc:nulls_last,time_unix_nano:asc:nulls_last"
        );
        for rg in row_group_sorting(dir.path(), &report.files[1].1) {
            assert_eq!(rg.as_deref(), Some(&[(0, false, false)][..]));
        }
    }

    /// Scenario: path components for a known window and sequence.
    /// Guarantees: the Hive layout and file name of spec section 5.3 are produced exactly.
    #[test]
    fn object_path_layout() {
        let p = object_path(
            Dataset::LogsValues,
            PartitionId::from_unix_secs(WINDOW_START),
            WINDOW_START,
            &naming("w1", "b"),
            42,
        );
        assert_eq!(
            p.as_ref(),
            "v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/part-20260921T031500Z-w1-b-00000042.parquet"
        );
    }

    /// Scenario: a `writer_id` containing `/` reaches `object_path` despite
    /// `LakeConfig::validate` refusing it (e.g. a caller that skips
    /// validation).
    /// Guarantees: the `/` is encoded into the file-name segment rather than
    /// splitting it into extra path segments: the path still has exactly the
    /// five Hive directory levels plus the file name, and the encoded segment
    /// round-trips back to the original `writer_id` once decoded.
    #[test]
    fn object_path_encodes_slash_in_writer_id_as_one_segment() {
        let p = object_path(
            Dataset::LogsValues,
            PartitionId::from_unix_secs(WINDOW_START),
            WINDOW_START,
            &naming("team/writer", "b"),
            42,
        );
        let raw = p.as_ref();
        assert_eq!(raw.matches('/').count(), 5, "path: {raw}");
        let file_name = raw.rsplit('/').next().expect("at least one segment");
        assert!(
            file_name.contains("team%2Fwriter"),
            "file name: {file_name}"
        );
        let parts: Vec<String> = p.parts().map(|part| part.as_ref().to_string()).collect();
        assert_eq!(parts.len(), 6, "parts: {parts:?}");
    }

    /// Scenario: a block with 30 log rows written to a local directory, then read back.
    /// Guarantees: series file exists next to the values file, values are sorted by the spec,
    /// every metadata key of spec 5.4 carries the expected value, the row count agrees with the
    /// flush report, and the same block rewrites the same names.
    #[tokio::test]
    async fn writes_series_before_values_and_reads_back() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let b = sealed_block(&cfg, 30);
        let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert_eq!(report.files.len(), 2);
        assert_eq!(report.files[0].0, Dataset::LogsSeries);
        assert_eq!(report.files[1].0, Dataset::LogsValues);
        // What the sink announces before writing is what it writes, so an
        // observer can record the names an attempt will touch.
        assert_eq!(
            sink.planned_paths(&b),
            report
                .files
                .iter()
                .map(|(_, path, _)| path.clone())
                .collect::<Vec<_>>()
        );

        let (_, values_path, values_rows) = &report.files[1];
        let kv = file_kv(dir.path(), values_path);
        assert_eq!(get(&kv, "format_version"), "1");
        assert_eq!(get(&kv, "series_hash"), "xxh3_128/canonical_v1");
        assert_eq!(
            get(&kv, "schema_fingerprint"),
            format!(
                "{:016x}",
                schema_fingerprint(&dataset_schema(Dataset::LogsValues, &cfg))
            )
        );
        assert_eq!(
            get(&kv, "sort_key"),
            "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last"
        );
        assert_eq!(get(&kv, "writer_id"), "w");
        assert_eq!(get(&kv, "boot_id"), "boot");
        assert_eq!(get(&kv, "seq"), SEQ.to_string());
        assert_eq!(get(&kv, "window_start"), "1789960500");
        assert_eq!(get(&kv, "window_end"), "1789960515");
        // The metadata row count is the count the sink actually wrote.
        assert_eq!(get(&kv, "row_count"), "30");
        assert_eq!(get(&kv, "row_count"), values_rows.to_string());
        // Record i carries FIRST_LOG_TIME - i, for i in 0..30.
        assert_eq!(
            get(&kv, "min_time_unix_nano"),
            (FIRST_LOG_TIME - 29).to_string()
        );
        assert_eq!(get(&kv, "max_time_unix_nano"), FIRST_LOG_TIME.to_string());

        // A series dataset has no time column, so it carries no time bounds.
        let series_kv = file_kv(dir.path(), &report.files[0].1);
        assert_eq!(get(&series_kv, "row_count"), report.files[0].2.to_string());
        assert!(!series_kv.iter().any(|(k, _)| k == "min_time_unix_nano"));
        assert!(!series_kv.iter().any(|(k, _)| k == "max_time_unix_nano"));
        assert_eq!(
            get(&series_kv, "schema_fingerprint"),
            format!(
                "{:016x}",
                schema_fingerprint(&dataset_schema(Dataset::LogsSeries, &cfg))
            )
        );

        let file = std::fs::File::open(dir.path().join(values_path.as_ref())).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        let batches: Vec<_> = reader
            .build()
            .expect("build")
            .map(|b| b.expect("batch"))
            .collect();
        let all = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
        assert_eq!(all.num_rows(), 30);
        let spec = SortSpec::new(cfg.logs.values_sort.clone());
        assert!(crate::sort::is_sorted(&all, &spec).expect("sorted"));

        // rewrite: same names, still two files on disk
        let report2 = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("rewrite");
        assert_eq!(report.files[1].1, report2.files[1].1);
        assert_eq!(walkdir_count(dir.path()), 2);
    }

    /// Scenario: a metrics block carrying both a gauge and a histogram.
    /// Guarantees: the series dataset is written before the single merged values
    /// dataset, the two point kinds share one file rather than costing a second
    /// PUT, and the file's row count matches the flush report.
    #[tokio::test]
    async fn metrics_block_writes_series_before_the_merged_values_dataset() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(10);
        let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
        let mut records = encode_metrics(&gauge_and_histogram());
        let e = extract(&mut records, &cfg).expect("extract");
        let r = b.reserve(&e, &mut cache, 8).expect("reserve");
        b.admit(e, r).expect("admit");
        b.seal(SEAL_AT_US).expect("seal");

        let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        let order: Vec<Dataset> = report.files.iter().map(|(d, _, _)| *d).collect();
        assert_eq!(order, vec![Dataset::MetricsSeries, Dataset::MetricsValues]);
        assert_eq!(parquet_count(dir.path(), "dataset=series"), 1);
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 1);
        assert_eq!(walkdir_count(dir.path()), 2);
        for (ds, path, rows) in &report.files {
            assert!(*rows > 0, "{ds:?} wrote no rows");
            let kv = file_kv(dir.path(), path);
            assert_eq!(get(&kv, "row_count"), rows.to_string());
            assert_eq!(
                get(&kv, "schema_fingerprint"),
                format!("{:016x}", schema_fingerprint(&dataset_schema(*ds, &cfg)))
            );
            assert_eq!(
                kv.iter().any(|(k, _)| k == "min_time_unix_nano"),
                !ds.is_series()
            );
        }
    }

    fn gauge_and_histogram() -> MetricsData {
        let dp = |t: u64, v: f64| NumberDataPoint {
            time_unix_nano: t,
            value: Some(number_data_point::Value::AsDouble(v)),
            attributes: vec![kv("cpu", "0")],
            ..Default::default()
        };
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "cpu".into(),
                            unit: "1".into(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![dp(10, 0.5), dp(20, 0.7)],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "lat".into(),
                            data: Some(metric::Data::Histogram(Histogram {
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                                data_points: vec![HistogramDataPoint {
                                    time_unix_nano: 40,
                                    count: 3,
                                    sum: Some(6.0),
                                    bucket_counts: vec![1, 2],
                                    explicit_bounds: vec![5.0],
                                    ..Default::default()
                                }],
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Scenario: a sealed block into which nothing was ever admitted.
    /// Guarantees: no dataset file is created for a zero-row dataset (spec 5.3).
    #[tokio::test]
    async fn empty_block_writes_no_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
        b.seal(SEAL_AT_US).expect("seal");
        let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert!(report.files.is_empty());
        assert_eq!(walkdir_count(dir.path()), 0);
    }

    /// Scenario: a block that was never sealed.
    /// Guarantees: the sink refuses to write a block whose series rows still
    /// carry the placeholder `emitted_at` of zero, which would otherwise publish
    /// unstamped data. The refusal is an error in every build profile, not a
    /// debug assertion.
    #[tokio::test]
    async fn write_block_rejects_an_unsealed_block() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let b = Block::new(WINDOW_START, SEQ, cfg.clone());
        assert!(!b.is_sealed());
        let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"));
        let err = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect_err("an unsealed block is refused");
        assert!(err.to_string().contains("unsealed block"));
    }

    /// Scenario: `writer_limit_bytes` and `merge_chunk_bytes` set so low that every
    /// merged chunk closes the current row group.
    /// Guarantees: the file has more than one row group and still holds every row.
    #[tokio::test]
    async fn writer_limit_closes_row_groups() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut cfg = LakeConfig::default();
        cfg.parquet.writer_limit_bytes = 1;
        cfg.sorting.merge_chunk_bytes = 1;
        cfg.validate().expect("valid config");
        let b = sealed_block(&cfg, 40);
        let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        let values = report
            .files
            .iter()
            .find(|(d, _, _)| *d == Dataset::LogsValues)
            .expect("values file");
        let file = std::fs::File::open(dir.path().join(values.1.as_ref())).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        assert!(
            reader.metadata().num_row_groups() > 1,
            "the writer memory limit must close row groups"
        );
        let rows: usize = reader
            .build()
            .expect("build")
            .map(|b| b.expect("batch").num_rows())
            .sum();
        assert_eq!(rows, 40);
    }

    /// Scenario: the token is cancelled before writing.
    /// Guarantees: write_block returns Cancelled and leaves no completed object.
    #[tokio::test]
    async fn cancellation_before_writing_aborts() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let b = sealed_block(&cfg, 5);
        let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"));
        let token = CancellationToken::new();
        token.cancel();
        assert!(
            sink.write_block(&b, &token)
                .await
                .is_err_and(|e| e.is_cancelled())
        );
        assert_eq!(walkdir_count(dir.path()), 0);
    }

    /// Scenario: a block written to an in-memory object store that is ready the
    /// instant it is asked, on a current-thread runtime, while another task on
    /// that same runtime cancels the token.
    /// Guarantees: the chunk loop yields between chunks, so the cancelling task
    /// gets to run and the write stops as cancelled. Without the yield, a store
    /// that never suspends lets the loop write every chunk of every table before
    /// the runtime ever schedules the cancelling task.
    #[tokio::test]
    async fn cancellation_is_observed_with_an_immediately_ready_store() {
        let mut cfg = LakeConfig::default();
        // One row per chunk, so the loop makes many passes over a small block.
        cfg.sorting.merge_chunk_bytes = 1;
        cfg.validate().expect("valid config");
        let b = sealed_block(&cfg, 200);
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let token = CancellationToken::new();
        let canceller = {
            let token = token.clone();
            tokio::spawn(async move { token.cancel() })
        };
        let got = sink.write_block(&b, &token).await;
        assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
        canceller.await.expect("cancelling task");
    }

    /// Scenario: the store cancels the token the moment the values object is opened,
    /// which happens part way through the chunk loop of a multi-part values file.
    /// Guarantees: every run stops with Cancelled after the series file has been
    /// finalized and before any values object is completed.
    #[tokio::test]
    async fn cancellation_at_a_chunk_boundary() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = upload_config();
        let b = sealed_upload_block(&cfg);
        let token = CancellationToken::new();
        let tripped_multipart = Arc::new(AtomicBool::new(false));
        let store: Arc<dyn ObjectStore> = Arc::new(CancelOnValues {
            inner: local(&dir),
            token: token.clone(),
            tripped_multipart: tripped_multipart.clone(),
        });
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let got = sink.write_block(&b, &token).await;
        assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
        assert!(
            tripped_multipart.load(Ordering::SeqCst),
            "the token must fire inside the chunk loop, not at finalization"
        );
        // The series file was finalized before the token fired; a finalized file
        // is never unwritten (spec 6.5 step 2).
        assert_eq!(parquet_count(dir.path(), "dataset=series"), 1);
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
    }

    /// Scenario: cancellation arrives while a real multipart part is in flight, with a
    /// store whose `put_part` never resolves.
    /// Guarantees: the writer is in its `Write` state so the abort takes the real path,
    /// the upload is aborted rather than awaited to completion, `write_block` returns
    /// Cancelled and no values object is completed.
    #[tokio::test]
    async fn cancellation_inside_an_upload() {
        let dir = tempfile::tempdir().expect("tmp");
        let entered = Arc::new(Notify::new());
        let aborted = Arc::new(AtomicBool::new(false));
        let parts = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(ControlledMultipart {
            inner: local(&dir),
            entered: entered.clone(),
            aborted: aborted.clone(),
            parts: parts.clone(),
            part: PartBehavior::Park,
            abort: AbortBehavior::Delegate,
        });
        let cfg = upload_config();
        let b = sealed_upload_block(&cfg);
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let token = CancellationToken::new();
        let canceller = token.clone();
        let waiter = entered.clone();
        let handle = tokio::spawn(async move {
            waiter.notified().await;
            canceller.cancel();
        });
        let got = sink.write_block(&b, &token).await;
        handle.await.expect("canceller");
        assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
        assert!(
            parts.load(Ordering::SeqCst) > 0,
            "the writer must have handed a real part to the upload"
        );
        assert!(
            aborted.load(Ordering::SeqCst),
            "the in-flight multipart upload must be aborted"
        );
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
    }

    /// Scenario: the cancellation lands while the values multipart upload is
    /// still being created, before `BufWriter` has an upload it could abort.
    /// Guarantees: the started creation is allowed to finish within
    /// `upload.abort_timeout`, and the upload it created is then aborted
    /// rather than left orphaned at the store with `abort_error: None`
    /// claiming a clean cleanup; no values object is completed.
    #[tokio::test]
    async fn a_cancellation_during_multipart_creation_still_aborts_the_upload() {
        let dir = tempfile::tempdir().expect("tmp");
        let aborted = Arc::new(AtomicBool::new(false));
        let token = CancellationToken::new();
        let store: Arc<dyn ObjectStore> = Arc::new(CancelDuringCreate {
            inner: Arc::new(ControlledMultipart {
                inner: local(&dir),
                entered: Arc::new(Notify::new()),
                aborted: aborted.clone(),
                parts: Arc::new(AtomicUsize::new(0)),
                part: PartBehavior::Fail,
                abort: AbortBehavior::Delegate,
            }),
            token: token.clone(),
        });
        let cfg = upload_config();
        let b = sealed_upload_block(&cfg);
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let got = sink.write_block(&b, &token).await;
        assert!(got.is_err(), "a cancelled write fails: {got:?}");
        assert!(
            aborted.load(Ordering::SeqCst),
            "the upload created after the cancellation must be aborted"
        );
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
    }

    /// Scenario: a part upload fails while the writer is still writable and the
    /// best-effort abort that follows fails as well.
    /// Guarantees: `AbortFailed` carries both the original write failure and the reason
    /// the cleanup did not succeed, and no values object is completed.
    #[tokio::test]
    async fn write_failure_with_a_failing_abort_reports_both() {
        let dir = tempfile::tempdir().expect("tmp");
        let aborted = Arc::new(AtomicBool::new(false));
        let parts = Arc::new(AtomicUsize::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(ControlledMultipart {
            inner: local(&dir),
            entered: Arc::new(Notify::new()),
            aborted: aborted.clone(),
            parts: parts.clone(),
            part: PartBehavior::Fail,
            abort: AbortBehavior::Fail,
        });
        let cfg = upload_config();
        let b = sealed_upload_block(&cfg);
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let got = sink.write_block(&b, &CancellationToken::new()).await;
        let Err(Error::Transient(TransientError::AbortFailed {
            source,
            abort_error,
        })) = got
        else {
            panic!("expected AbortFailed, got {got:?}");
        };
        // The failure that triggered the cleanup is preserved, not replaced.
        assert!(
            source.to_string().contains("injected failure"),
            "original failure lost: {source}"
        );
        assert!(
            abort_error.contains("injected failure"),
            "abort failure lost: {abort_error}"
        );
        assert!(parts.load(Ordering::SeqCst) > 0);
        assert!(aborted.load(Ordering::SeqCst));
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
    }

    /// Scenario: a part upload fails and the abort then hangs, with an hour of
    /// `upload.abort_timeout`, on a sink whose injected clock reports every
    /// deadline as already reached.
    /// Guarantees: the abort ends on the injected clock -- at once -- rather
    /// than after an hour of tokio time, so a caller running on a simulated
    /// clock governs the sink's cleanup bound as well.
    #[tokio::test]
    async fn the_abort_is_bounded_on_the_injected_clock() {
        let dir = tempfile::tempdir().expect("tmp");
        let store: Arc<dyn ObjectStore> = Arc::new(ControlledMultipart {
            inner: local(&dir),
            entered: Arc::new(Notify::new()),
            aborted: Arc::new(AtomicBool::new(false)),
            parts: Arc::new(AtomicUsize::new(0)),
            part: PartBehavior::Fail,
            abort: AbortBehavior::Hang,
        });
        let mut cfg = upload_config();
        cfg.upload.abort_timeout = Duration::from_secs(3600);
        let b = sealed_upload_block(&cfg);
        let sink = Sink::new(store, cfg, FileNaming::new("w")).with_clock(SinkClock {
            now: Instant::now,
            sleep_until: |_| Box::pin(std::future::ready(())),
        });
        let got = tokio::time::timeout(
            Duration::from_secs(30),
            sink.write_block(&b, &CancellationToken::new()),
        )
        .await
        .expect("the abort is bounded by the injected clock, not by tokio time");
        let Err(Error::Transient(TransientError::AbortFailed { abort_error, .. })) = got else {
            panic!("expected AbortFailed, got {got:?}");
        };
        assert!(abort_error.contains("timed out"), "{abort_error}");
    }

    /// Scenario: a part upload fails and the best-effort abort then hangs, with a
    /// one-millisecond `upload.abort_timeout`.
    /// Guarantees: the timeout branch ends the cleanup, and the reported abort error
    /// names the timeout instead of blocking the flush task forever.
    #[tokio::test]
    async fn abort_timeout_is_reported() {
        let dir = tempfile::tempdir().expect("tmp");
        let store: Arc<dyn ObjectStore> = Arc::new(ControlledMultipart {
            inner: local(&dir),
            entered: Arc::new(Notify::new()),
            aborted: Arc::new(AtomicBool::new(false)),
            parts: Arc::new(AtomicUsize::new(0)),
            part: PartBehavior::Fail,
            abort: AbortBehavior::Hang,
        });
        let mut cfg = upload_config();
        cfg.upload.abort_timeout = Duration::from_millis(1);
        cfg.validate().expect("valid config");
        let b = sealed_upload_block(&cfg);
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let got = sink.write_block(&b, &CancellationToken::new()).await;
        let Err(Error::Transient(TransientError::AbortFailed { abort_error, .. })) = got else {
            panic!("expected AbortFailed, got {got:?}");
        };
        assert!(
            abort_error.contains("timed out"),
            "expected a timeout reason, got {abort_error}"
        );
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
    }

    /// A sealed block of `n` log rows whose one series is already committed
    /// in the block's partition, so the block's only table is its values.
    fn values_only_block(cfg: &LakeConfig, n: usize) -> Block {
        let mut cache = SeriesCache::new(10);
        let mut records = encode_logs(&logs(n, 8));
        let e = extract(&mut records, cfg).expect("extract");
        let partition = PartitionId::from_unix_secs(WINDOW_START);
        for descriptor in &e.descriptors {
            cache.mark_committed(descriptor.series_id, partition);
        }
        let mut b = Block::new(WINDOW_START, SEQ, cfg.clone());
        let r = b.reserve(&e, &mut cache, 8).expect("reserve");
        b.admit(e, r).expect("admit");
        b.seal(SEAL_AT_US).expect("seal");
        assert_eq!(
            b.tables().filter(|table| !table.is_empty()).count(),
            1,
            "only the values table has rows"
        );
        b
    }

    /// Scenario: a 30,000-row logs block, merged into one chunk, is written
    /// to an in-memory store that is ready the instant it is asked, while a
    /// ticker task on the same current-thread runtime counts how often the
    /// runtime schedules it.
    /// Guarantees: the write returns to the runtime between bounded steps of
    /// its work -- merge-key slices and heap-pop slices of at most
    /// `MERGE_STEP_ROWS` rows, interleaved columns of that much work each,
    /// and between producing a chunk, encoding it and flushing its row
    /// group -- so with a 30,000-row chunk the ticker runs at least once per
    /// pop slice and once per output column.
    /// A write that yields only between chunks lets it run a handful of times.
    #[tokio::test]
    async fn the_write_returns_to_the_runtime_between_bounded_slices() {
        let cfg = LakeConfig::default();
        let rows = 30_000;
        let b = sealed_block(&cfg, rows);
        let columns = dataset_schema(Dataset::LogsValues, &cfg).fields().len();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let done = Arc::new(AtomicBool::new(false));
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = {
            let done = Arc::clone(&done);
            let ticks = Arc::clone(&ticks);
            tokio::spawn(async move {
                while !done.load(Ordering::SeqCst) {
                    let _ = ticks.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                }
            })
        };
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        done.store(true, Ordering::SeqCst);
        ticker.await.expect("ticker");
        assert_eq!(report.files.iter().map(|f| f.2).sum::<usize>(), rows + 1);
        let pop_slices = rows.div_ceil(crate::sort::MERGE_STEP_ROWS);
        let ticked = ticks.load(Ordering::SeqCst);
        assert!(
            ticked > pop_slices + columns,
            "the runtime scheduled the ticker {ticked} times during the write, fewer than \
             {pop_slices} pop slices and {columns} columns"
        );
    }

    /// Scenario: a values-only block of 100,000 log rows, whose merge keys
    /// take many slices to encode, is written while a task that the runtime
    /// schedules at the write's first yield cancels the token.
    /// Guarantees: the cancellation is observed between merge-key slices:
    /// the write returns Cancelled having encoded only a fraction of the
    /// keys, as the sink's merge-key high-water mark shows, instead of
    /// encoding the key of every row before it first returns to the runtime.
    #[tokio::test]
    async fn a_cancellation_during_merge_key_building_stops_the_build() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_request_bytes = 64 << 20;
        cfg.ingress.max_extracted_bytes = 64 << 20;
        cfg.validate().expect("valid config");
        let b = values_only_block(&cfg, 100_000);
        let full = b
            .tables()
            .filter(|table| !table.is_empty())
            .map(|table| {
                merge_runs(
                    table.iter_snapshots().cloned().collect(),
                    table.spec(),
                    cfg.sorting.merge_chunk_bytes,
                )
                .expect("merge")
                .resident_key_bytes()
            })
            .sum::<usize>();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let token = CancellationToken::new();
        let canceller = {
            let token = token.clone();
            tokio::spawn(async move { token.cancel() })
        };
        let got = sink.write_block(&b, &token).await;
        canceller.await.expect("cancelling task");
        assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
        let built = sink.merge_key_high_water_bytes();
        assert!(
            built < full / 2,
            "{built} of {full} merge-key bytes were encoded before the cancellation was seen"
        );
    }

    /// Scenario: three 10-byte buffers are handed to the upload and cut
    /// into parts of 8, 8, 8 and 6 bytes; the first part stalls while the
    /// second and third land, then the first lands, then the last; and a
    /// single-request put follows.
    /// Guarantees: a buffer is released as soon as every byte of it has
    /// landed, whatever the order the parts land in, so a stalled early part
    /// keeps only the buffers it overlaps charged -- the middle buffer, fully
    /// covered by the two later parts, is released while the first part is
    /// still in flight -- and a put releases everything handed before it.
    #[test]
    fn the_upload_ledger_releases_each_buffer_when_its_bytes_land() {
        let ledger = UploadLedger::default();
        for _ in 0..3 {
            ledger.handed(10);
        }
        assert_eq!(ledger.live(), 30);
        let first = ledger.part_started(8);
        let second = ledger.part_started(8);
        let third = ledger.part_started(8);
        ledger.part_landed(second);
        assert_eq!(
            ledger.live(),
            30,
            "bytes 8..16 landed: no buffer is whole yet"
        );
        ledger.part_landed(third);
        assert_eq!(
            ledger.live(),
            20,
            "bytes 10..20 have all landed although part 0..8 is still in flight"
        );
        ledger.part_landed(first);
        assert_eq!(ledger.live(), 10, "bytes 0..10 landed");
        let last = ledger.part_started(6);
        ledger.part_landed(last);
        assert_eq!(ledger.live(), 0);
        ledger.handed(7);
        assert_eq!(ledger.live(), 7);
        ledger.put_landed();
        assert_eq!(ledger.live(), 0);
    }

    /// Scenario: the same runs merged sorted and with sorting disabled, and
    /// each first chunk charged as the flush workspace charges it.
    /// Guarantees: an unsorted chunk is one of the block's own runs, whose
    /// buffers the block already accounts for, so it charges nothing; a
    /// sorted chunk is interleaved into buffers of its own and charges at
    /// least their pinned bytes.
    #[test]
    fn only_a_chunk_the_merge_allocated_is_charged() {
        let cfg = LakeConfig::default();
        let b = sealed_block(&cfg, 200);
        let table = b
            .tables()
            .find(|table| !table.dataset().is_series() && !table.is_empty())
            .expect("values");
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        for (spec, owned) in [(table.spec().clone(), true), (SortSpec::new(vec![]), false)] {
            let mut merge = merge_runs(runs.clone(), &spec, 1 << 20).expect("merge");
            let chunk = loop {
                match merge.step().expect("step") {
                    MergeStep::Chunk(chunk) => break chunk,
                    MergeStep::Ready => {
                        let mut builder = merge.chunk_builder();
                        while !builder.step().expect("build") {}
                        let chunk = builder.finish().expect("chunk");
                        merge.chunk_taken();
                        break chunk;
                    }
                    MergeStep::Progress => {}
                    MergeStep::Done => panic!("no chunk"),
                }
            };
            let pinned = record_batch_pinned_bytes(&chunk, &mut CountedAllocations::default());
            let charged = chunk_charge(&merge, &chunk);
            if owned {
                assert!(charged >= pinned, "{charged} charged for {pinned} pinned");
            } else {
                assert_eq!(charged, 0, "the run belongs to the block");
            }
        }
    }

    /// An `ObjectStore` whose multipart parts wait for a permit before they
    /// are uploaded, announcing each part as it is handed over.
    #[derive(Debug)]
    struct GatedParts {
        inner: Arc<dyn ObjectStore>,
        gate: Arc<tokio::sync::Semaphore>,
        entered: Arc<Notify>,
    }

    #[derive(Debug)]
    struct GatedUpload {
        inner: Box<dyn MultipartUpload>,
        gate: Arc<tokio::sync::Semaphore>,
        entered: Arc<Notify>,
    }

    #[async_trait::async_trait]
    impl MultipartUpload for GatedUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let part = self.inner.put_part(data);
            let gate = Arc::clone(&self.gate);
            self.entered.notify_one();
            Box::pin(async move {
                let permit = gate.acquire().await.expect("the gate is never closed");
                drop(permit);
                part.await
            })
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            self.inner.complete().await
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }

    impl std::fmt::Display for GatedParts {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GatedParts({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for GatedParts {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            let inner = self.inner.put_multipart_opts(location, options).await?;
            Ok(Box::new(GatedUpload {
                inner,
                gate: Arc::clone(&self.gate),
                entered: Arc::clone(&self.entered),
            }))
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Scenario: a values file larger than one 5 MiB part is written with
    /// one part in flight at a time, and the store holds the first part
    /// until the test releases it.
    /// Guarantees: while the part is held the sink reports a flush
    /// workspace of at least the part's 5 MiB, because the part's buffer is
    /// still allocated; once the write has returned it reports zero, and its
    /// high-water mark keeps the peak.
    #[tokio::test]
    async fn the_sink_publishes_the_upload_bytes_a_part_in_flight_holds() {
        let dir = tempfile::tempdir().expect("tmp");
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let entered = Arc::new(Notify::new());
        let store: Arc<dyn ObjectStore> = Arc::new(GatedParts {
            inner: local(&dir),
            gate: Arc::clone(&gate),
            entered: Arc::clone(&entered),
        });
        let cfg = upload_config();
        let b = sealed_upload_block(&cfg);
        let sink = Sink::new(store, cfg.clone(), FileNaming::new("w"));
        assert_eq!(sink.flush_workspace_bytes(), 0);
        let token = CancellationToken::new();
        let write = sink.write_block(&b, &token);
        let observe = async {
            entered.notified().await;
            let held = sink.flush_workspace_bytes();
            gate.add_permits(1 << 20);
            held
        };
        let (got, held) = tokio::join!(write, observe);
        let _ = got.expect("write");
        assert!(
            held >= cfg.upload.part_bytes,
            "{held} workspace bytes while a {} byte part was in flight",
            cfg.upload.part_bytes
        );
        assert_eq!(sink.flush_workspace_bytes(), 0, "no table is being written");
        assert!(sink.flush_workspace_high_water_bytes() >= held);
    }

    /// Scenario: a writer step is handed an operation that would complete
    /// on its first poll, with a token that has already fired.
    /// Guarantees: the step reports Cancelled without polling the operation
    /// at all, so once the cancellation is there no further writer work runs
    /// before it is acted on.
    #[tokio::test]
    async fn a_step_whose_token_has_fired_is_not_driven_again() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let sink = Sink::new(
            Arc::clone(&store),
            LakeConfig::default(),
            FileNaming::new("w"),
        );
        let watch = CreationWatch::new(store, Arc::new(UploadLedger::default()));
        let cancel = CancellationToken::new();
        cancel.cancel();
        let polled = std::cell::Cell::new(0);
        let op = std::future::poll_fn(|_| {
            polled.set(polled.get() + 1);
            std::task::Poll::Ready(Ok(()))
        });
        let mut cleanup = None;
        let got = sink.step(op, &watch, &cancel, &mut cleanup).await;
        assert!(got.as_ref().is_err_and(Error::is_cancelled), "got {got:?}");
        assert_eq!(
            polled.get(),
            0,
            "the operation was polled after the cancellation"
        );
    }
}
