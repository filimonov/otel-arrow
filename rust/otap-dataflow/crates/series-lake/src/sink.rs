// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Writes a sealed block to an object store as Parquet files (spec sections 5.3 to 5.4, 6.5).

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tokio_util::sync::CancellationToken;

use crate::buffer::{Block, SortedTableBuffer};
use crate::clock::PartitionId;
use crate::config::LakeConfig;
use crate::error::{Error, Result};
use crate::schema::{Dataset, dataset_schema, schema_fingerprint};
use crate::sort::merge_runs;

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

impl Sink {
    /// New sink.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, cfg: LakeConfig, naming: FileNaming) -> Self {
        Self { store, cfg, naming }
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

    /// Best-effort abort of a still-writable upload.
    ///
    /// Bounded by `upload.abort_timeout`, so a wedged store cannot block the flush
    /// task. Returns why the abort did not succeed, or `None` when it did.
    async fn abort_upload(&self, writer: AsyncArrowWriter<ParquetObjectWriter>) -> Option<String> {
        let mut buf: BufWriter = writer.into_inner().into_inner();
        match tokio::time::timeout(self.cfg.upload.abort_timeout, buf.abort()).await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e.to_string()),
            Err(_elapsed) => Some(format!(
                "abort timed out after {:?}",
                self.cfg.upload.abort_timeout
            )),
        }
    }

    /// Attach the outcome of the cleanup abort to the failure that triggered it.
    fn with_abort(cause: Error, abort_error: Option<String>) -> Error {
        match (cause, abort_error) {
            (Error::Cancelled { .. }, abort_error) => Error::Cancelled { abort_error },
            (cause, None) => cause,
            (cause, Some(abort_error)) => Error::AbortFailed {
                source: Box::new(cause),
                abort_error,
            },
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
        // Spec 5.4 asks for ZSTD, statistics and dictionary encoding explicitly
        // rather than by relying on arrow-rs defaults. An unlimited row count
        // disables the row-count-based split so that the byte-driven flush below
        // owns row group boundaries.
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_dictionary_enabled(true)
            .set_max_row_group_row_count(None)
            .set_key_value_metadata(Some(self.file_metadata(
                table,
                total_rows,
                range,
                seq,
                window_start_secs,
            )))
            .build();
        let buf =
            BufWriter::with_capacity(self.store.clone(), path.clone(), self.cfg.upload.part_bytes)
                .with_max_concurrency(self.cfg.upload.concurrency);
        let object_writer = ParquetObjectWriter::from_buf_writer(buf);
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;

        // Phase 1: the writer is writable. Every await races the token, and every
        // failure aborts the multipart upload.
        let mut rows = 0usize;
        let mut failure: Option<Error> = None;
        let merged = merge_runs(runs, table.spec(), self.cfg.sorting.merge_chunk_bytes)?;
        for chunk in merged {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            };
            let step = tokio::select! {
                biased;
                () = cancel.cancelled() => Err(Error::Cancelled { abort_error: None }),
                r = writer.write(&chunk) => r.map_err(Error::from),
            };
            if let Err(e) = step {
                failure = Some(e);
                break;
            }
            rows += chunk.num_rows();
            if writer.memory_size() >= self.cfg.parquet.writer_limit_bytes
                || writer.in_progress_size() >= self.cfg.parquet.row_group_bytes
            {
                let step = tokio::select! {
                    biased;
                    () = cancel.cancelled() => Err(Error::Cancelled { abort_error: None }),
                    r = writer.flush() => r.map_err(Error::from),
                };
                if let Err(e) = step {
                    failure = Some(e);
                    break;
                }
            }
        }
        if let Some(cause) = failure {
            let abort_error = self.abort_upload(writer).await;
            return Err(Self::with_abort(cause, abort_error));
        }
        if cancel.is_cancelled() {
            let abort_error = self.abort_upload(writer).await;
            return Err(Error::Cancelled { abort_error });
        }

        // Phase 2: finalizing. `finish` writes the footer and shuts the BufWriter
        // down; `BufWriter::abort` panics once shutdown has started, so nothing is
        // aborted from here on. A partial multipart upload left by a cancellation
        // in this phase is reclaimed by the bucket's multipart lifecycle rule
        // (spec 5.3), not by this crate.
        let finish = tokio::select! {
            biased;
            () = cancel.cancelled() => Err(Error::Cancelled { abort_error: None }),
            r = writer.finish() => r.map(|_metadata| ()).map_err(Error::from),
        };
        finish?;
        Ok(rows)
    }

    /// Write every non-empty table of a sealed block, series datasets first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cancelled`] when `cancel` fires, and the underlying
    /// Arrow, Parquet or object store failure otherwise.
    pub async fn write_block<T>(
        &self,
        block: &Block<T>,
        cancel: &CancellationToken,
    ) -> Result<FlushReport> {
        // Descriptor rows only become Arrow rows in the series table when
        // `seal` stamps them, so writing an unsealed block would silently drop
        // every series row. This is a runtime check, not a debug assertion: the
        // rows would be lost just as silently in a release build.
        if !block.is_sealed() {
            return Err(Error::invalid("unsealed block"));
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled { abort_error: None });
        }
        let mut report = FlushReport::default();
        for table in block.tables() {
            if table.is_empty() {
                continue;
            }
            let path = object_path(
                table.dataset(),
                block.partition,
                block.window_start_secs,
                &self.naming,
                block.seq,
            );
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

    fn seal_logs(cfg: &LakeConfig, n: usize, body_len: usize) -> Block<u8> {
        let mut cache = SeriesCache::new(10);
        let mut b: Block<u8> = Block::new(WINDOW_START, SEQ, cfg);
        let mut records = encode_logs(&logs(n, body_len));
        let e = extract(&mut records, cfg).expect("extract");
        let r = b.reserve(&e, &mut cache, 8, cfg).expect("reserve");
        b.admit(e, r, 0).expect("admit");
        b.seal(SEAL_AT_US).expect("seal");
        b
    }

    /// A sealed block of 30 small log rows.
    fn sealed_block(cfg: &LakeConfig, n: usize) -> Block<u8> {
        seal_logs(cfg, n, 8)
    }

    /// A sealed block whose values object is comfortably larger than one 5 MiB
    /// multipart part, so that the upload tests reach an in-flight `put_part`
    /// while the writer is still in its writable phase.
    fn sealed_upload_block(cfg: &LakeConfig) -> Block<u8> {
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
        let spec = crate::sort::SortSpec::new(cfg.logs.values_sort.clone());
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
    /// Guarantees: the series dataset is written before both value datasets, each value
    /// dataset gets its own file under its own Hive prefix, and each file's row count
    /// matches the flush report.
    #[tokio::test]
    async fn metrics_block_writes_series_before_both_value_datasets() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(10);
        let mut b: Block<u8> = Block::new(WINDOW_START, SEQ, &cfg);
        let mut records = encode_metrics(&gauge_and_histogram());
        let e = extract(&mut records, &cfg).expect("extract");
        let r = b.reserve(&e, &mut cache, 8, &cfg).expect("reserve");
        b.admit(e, r, 0).expect("admit");
        b.seal(SEAL_AT_US).expect("seal");

        let sink = Sink::new(local(&dir), cfg.clone(), naming("w", "boot"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        let order: Vec<Dataset> = report.files.iter().map(|(d, _, _)| *d).collect();
        assert_eq!(
            order,
            vec![
                Dataset::MetricsSeries,
                Dataset::MetricsNumber,
                Dataset::MetricsHistogram
            ]
        );
        assert_eq!(parquet_count(dir.path(), "dataset=series"), 1);
        assert_eq!(parquet_count(dir.path(), "dataset=number"), 1);
        assert_eq!(parquet_count(dir.path(), "dataset=histogram"), 1);
        assert_eq!(walkdir_count(dir.path()), 3);
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
        let mut b: Block<u8> = Block::new(WINDOW_START, SEQ, &cfg);
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
    /// Guarantees: the sink refuses to write a block whose descriptor rows have not
    /// been materialized, which would otherwise silently drop every series row.
    /// The refusal is an error in every build profile, not a debug assertion.
    #[tokio::test]
    async fn write_block_rejects_an_unsealed_block() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let b: Block<u8> = Block::new(WINDOW_START, SEQ, &cfg);
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
        assert!(matches!(
            sink.write_block(&b, &token).await,
            Err(Error::Cancelled { .. })
        ));
        assert_eq!(walkdir_count(dir.path()), 0);
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
        assert!(matches!(got, Err(Error::Cancelled { .. })), "got {got:?}");
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
        assert!(matches!(got, Err(Error::Cancelled { .. })), "got {got:?}");
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
        let Err(Error::AbortFailed {
            source,
            abort_error,
        }) = got
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
        let Err(Error::AbortFailed { abort_error, .. }) = got else {
            panic!("expected AbortFailed, got {got:?}");
        };
        assert!(
            abort_error.contains("timed out"),
            "expected a timeout reason, got {abort_error}"
        );
        assert_eq!(parquet_count(dir.path(), "dataset=values"), 0);
    }
}
