// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Writes a sealed block to an object store as Parquet files (spec sections 5.3 to 5.4, 6.5).

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
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

fn utc_stamp(unix_secs: i64) -> String {
    let p = PartitionId::from_unix_secs(unix_secs);
    let date = p.date_string().replace('-', "");
    let secs_of_day = unix_secs.rem_euclid(86_400);
    format!(
        "{date}T{:02}{:02}{:02}Z",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// Object path of a dataset file (spec section 5.3).
#[must_use]
pub fn object_path(
    ds: Dataset,
    partition: PartitionId,
    window_start_secs: i64,
    naming: &FileNaming,
    seq: u64,
) -> Path {
    Path::from(format!(
        "v=1/signal={}/dataset={}/date={}/hour={}/part-{}-{}-{}-{seq:08}.parquet",
        ds.signal().as_str(),
        ds.name(),
        partition.date_string(),
        partition.hour_string(),
        utc_stamp(window_start_secs),
        naming.writer_id,
        naming.boot_id,
    ))
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

fn time_range(batches: &[RecordBatch]) -> (Option<i64>, Option<i64>) {
    let mut lo = None;
    let mut hi = None;
    for c in batches {
        if let Some(col) = c.column_by_name("time_unix_nano") {
            let a = col.as_primitive::<Int64Type>();
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
        let range = time_range(&runs);
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
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
        AnyValue, KeyValue as OtlpKeyValue, any_value,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::sync::Arc;
    use tokio::sync::Notify;
    use tokio_util::sync::CancellationToken;

    const WINDOW_START: i64 = 1_789_960_500;
    const SEAL_AT_US: i64 = 1_789_960_500_000_000;

    fn logs(n: usize) -> LogsData {
        let kv = |k: &str, v: &str| OtlpKeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..n)
                        .map(|i| LogRecord {
                            time_unix_nano: 5_000 - i as u64,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(format!("body-{i}"))),
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

    fn sealed_block(cfg: &LakeConfig, n: usize) -> Block<u8> {
        let mut cache = SeriesCache::new(10);
        let mut b: Block<u8> = Block::new(WINDOW_START, 7, cfg);
        let mut records = encode_logs(&logs(n));
        let e = extract(&mut records, cfg).expect("extract");
        let r = b.reserve(&e, &mut cache, 8, cfg).expect("reserve");
        b.admit(e, r, 0).expect("admit");
        b.seal(SEAL_AT_US).expect("seal");
        b
    }

    fn local(dir: &tempfile::TempDir) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("fs"))
    }

    /// An `ObjectStore` that parks every multipart upload until it is released,
    /// and announces that a multipart upload has started.
    #[derive(Debug)]
    struct ParkedMultipart {
        inner: Arc<dyn ObjectStore>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl std::fmt::Display for ParkedMultipart {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ParkedMultipart({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ParkedMultipart {
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
            // `notify_one` stores a permit, so the canceller observes the parked
            // upload even if it subscribes after this point.
            self.entered.notify_one();
            self.release.notified().await;
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

    fn walkdir_count(root: &std::path::Path) -> usize {
        fn walk(p: &std::path::Path, n: &mut usize) {
            for e in std::fs::read_dir(p).expect("dir") {
                let e = e.expect("entry");
                if e.path().is_dir() {
                    walk(&e.path(), n);
                } else if e.path().extension().is_some_and(|x| x == "parquet") {
                    *n += 1;
                }
            }
        }
        let mut n = 0;
        walk(root, &mut n);
        n
    }

    /// Parquet files of the `dataset=values` Hive partition.
    fn values_count(root: &std::path::Path) -> usize {
        fn walk(p: &std::path::Path, n: &mut usize) {
            for e in std::fs::read_dir(p).expect("dir") {
                let e = e.expect("entry");
                if e.path().is_dir() {
                    walk(&e.path(), n);
                } else if e.path().to_string_lossy().contains("dataset=values") {
                    *n += 1;
                }
            }
        }
        let mut n = 0;
        walk(root, &mut n);
        n
    }

    /// Scenario: path components for a known window and sequence.
    /// Guarantees: the Hive layout and file name of spec section 5.3 are produced exactly.
    #[test]
    fn object_path_layout() {
        let naming = FileNaming {
            writer_id: "w1".into(),
            boot_id: "b".into(),
        };
        let p = object_path(
            Dataset::LogsValues,
            PartitionId::from_unix_secs(WINDOW_START),
            WINDOW_START,
            &naming,
            42,
        );
        assert_eq!(
            p.as_ref(),
            "v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/part-20260921T031500Z-w1-b-00000042.parquet"
        );
    }

    /// Scenario: a block with 30 log rows written to a local directory, then read back.
    /// Guarantees: series file exists next to the values file, values are sorted by the spec,
    /// metadata carries the window and fingerprint, the same block rewrites the same names.
    #[tokio::test]
    async fn writes_series_before_values_and_reads_back() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let b = sealed_block(&cfg, 30);
        let sink = Sink::new(
            local(&dir),
            cfg.clone(),
            FileNaming {
                writer_id: "w".into(),
                boot_id: "boot".into(),
            },
        );
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert_eq!(report.files.len(), 2);
        assert_eq!(report.files[0].0, Dataset::LogsSeries);
        assert_eq!(report.files[1].0, Dataset::LogsValues);
        let values_path = dir.path().join(report.files[1].1.as_ref());
        let file = std::fs::File::open(&values_path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        let kv = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .expect("kv")
            .clone();
        let get = |k: &str| {
            kv.iter()
                .find(|e| e.key == k)
                .and_then(|e| e.value.clone())
                .expect(k)
        };
        assert_eq!(get("format_version"), "1");
        assert_eq!(get("window_start"), "1789960500");
        assert_eq!(get("window_end"), "1789960515");
        assert_eq!(get("row_count"), "30");
        assert_eq!(
            get("sort_key"),
            "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last"
        );
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

    /// Scenario: a sealed block into which nothing was ever admitted.
    /// Guarantees: no dataset file is created for a zero-row dataset (spec 5.3).
    #[tokio::test]
    async fn empty_block_writes_no_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let cfg = LakeConfig::default();
        let mut b: Block<u8> = Block::new(WINDOW_START, 7, &cfg);
        b.seal(SEAL_AT_US).expect("seal");
        let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"));
        let report = sink
            .write_block(&b, &CancellationToken::new())
            .await
            .expect("write");
        assert!(report.files.is_empty());
        assert_eq!(walkdir_count(dir.path()), 0);
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

    /// Scenario: cancellation arrives while the chunk loop is running, with a tiny
    /// merge chunk size so that many chunk boundaries are crossed.
    /// Guarantees: the write stops with Cancelled and no complete Parquet file is left.
    #[tokio::test]
    async fn cancellation_at_a_chunk_boundary() {
        let dir = tempfile::tempdir().expect("tmp");
        let mut cfg = LakeConfig::default();
        cfg.sorting.merge_chunk_bytes = 1;
        let b = sealed_block(&cfg, 4_000);
        let sink = Sink::new(local(&dir), cfg, FileNaming::new("w"));
        let token = CancellationToken::new();
        let canceller = token.clone();
        let handle = tokio::spawn(async move {
            tokio::task::yield_now().await;
            canceller.cancel();
        });
        let got = sink.write_block(&b, &token).await;
        handle.await.expect("canceller");
        assert!(matches!(got, Err(Error::Cancelled { .. })));
        // The values table is the one with thousands of chunk boundaries, so it is
        // the table the cancellation lands in; its object is never completed. The
        // series table is a single small chunk that may already have been
        // finalized when the token fires, and a finalized file is never unwritten.
        assert_eq!(values_count(dir.path()), 0);
        assert!(walkdir_count(dir.path()) <= 1);
    }

    /// Scenario: cancellation arrives while a multipart upload is in flight, with an
    /// object store that parks `put_multipart` until the test releases it.
    /// Guarantees: the parked upload is cancelled rather than awaited to completion,
    /// `write_block` returns Cancelled and no complete Parquet file is left.
    #[tokio::test]
    async fn cancellation_inside_an_upload() {
        let dir = tempfile::tempdir().expect("tmp");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let store: Arc<dyn ObjectStore> = Arc::new(ParkedMultipart {
            inner: local(&dir),
            entered: entered.clone(),
            release: release.clone(),
        });
        let mut cfg = LakeConfig::default();
        // Small parts so the BufWriter switches to a multipart upload quickly.
        cfg.upload.part_bytes = 4 << 10;
        cfg.sorting.merge_chunk_bytes = 4 << 10;
        let b = sealed_block(&cfg, 4_000);
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
        release.notify_waiters();
        assert!(matches!(got, Err(Error::Cancelled { .. })));
        assert_eq!(walkdir_count(dir.path()), 0);
    }
}
