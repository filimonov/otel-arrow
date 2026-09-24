// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The measured stages, shared by the `measurement` and `layered` benches.
//!
//! Every stage calls the production library path: the pdata conversion
//! trait, `extract`, `Block::{reserve, admit, seal}`, `merge_runs`, the
//! actual `Sink`, and real `object_store` clients built by the same
//! constructor the exporter uses. The only code of its own is the
//! diagnostic Parquet encoder, which takes its writer properties and its
//! row group predicate from the sink.
//!
//! A stage is split into three calls so that a caller can time exactly one
//! of them: [`Stage::prepare`] builds one iteration's input, [`Stage::run`]
//! is the measured operation, and [`Stage::observe`] accounts for and
//! verifies what `run` produced. Only `run` is ever inside a timer.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Write as _;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use bytes::Bytes;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use otel_arrow_dfe_otap::object_store::StorageType;
use otel_arrow_dfe_pdata::otap::memory::{
    CountedAllocations, record_batch_logical_bytes, record_batch_pinned_bytes,
};
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::canonical::{SeriesId, series_id};
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::{Extracted, extract};
use otel_arrow_dfe_series_lake::schema::Dataset;
use otel_arrow_dfe_series_lake::sink::{
    FileNaming, FlushReport, Sink, row_group_full, writer_properties,
};
use otel_arrow_dfe_series_lake::sort::{SortSpec, is_sorted, merge_runs};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;
use tokio_util::sync::CancellationToken;

/// The error type of every stage and of both bench entry points.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Sequence number of every block a stage builds.
const SEQ: u64 = 1;

/// What a completed write means in every stage that stores an object.
///
/// The store has reported the object written (a completed multipart upload
/// or `PUT` on S3, a closed file on the local backend) and the stage has read
/// it back and compared its size and bytes. It is not a
/// claim about host power-loss durability: `object_store` does not fsync
/// its local backend, and no backend used here promises the bytes survive
/// a power cut.
pub const COMPLETION_SEMANTICS: &str = "the store reported the object written and \
    the bytes were read back and verified; object_store does not fsync its \
    local backend, so this is object-store visibility, not host power-loss \
    durability";

/// Accounted bytes of one request's acknowledgement token.
///
/// The exporter charges its real token here; the bench has none, so it
/// charges a fixed stand-in. It changes the block's accounted bytes only,
/// never what is stored.
const TOKEN_BYTES: usize = 128;

/// Wall and CPU time of one measured operation.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Timing {
    /// Elapsed wall-clock nanoseconds.
    pub wall_ns: u128,
    /// CPU nanoseconds of the measuring thread, or of the whole process for
    /// an asynchronous stage.
    pub cpu_ns: u128,
}

/// Time a synchronous operation with the calling thread's CPU clock.
///
/// # Errors
/// Returns whatever `run` returns.
pub fn timed<T, E>(
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<(T, Timing), E> {
    let wall = std::time::Instant::now();
    let cpu = cpu_time::ThreadTime::now();
    let value = run()?;
    let timing = Timing {
        wall_ns: wall.elapsed().as_nanos(),
        cpu_ns: cpu.elapsed().as_nanos(),
    };
    Ok((value, timing))
}

/// Time an operation that drives the runtime with the process CPU clock.
///
/// An asynchronous stage's work can run on the runtime's blocking pool, so
/// the calling thread's clock would miss it. No other benchmark stage runs
/// concurrently in the process.
///
/// # Errors
/// Returns whatever `run` returns.
pub fn timed_process<T, E>(
    run: impl FnOnce() -> std::result::Result<T, E>,
) -> std::result::Result<(T, Timing), E> {
    let wall = std::time::Instant::now();
    let cpu = cpu_time::ProcessTime::now();
    let value = run()?;
    let timing = Timing {
        wall_ns: wall.elapsed().as_nanos(),
        cpu_ns: cpu.elapsed().as_nanos(),
    };
    Ok((value, timing))
}

/// Which clock measures a stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Clock {
    /// The calling thread's CPU time, through [`timed`].
    Thread,
    /// The whole process's CPU time, through [`timed_process`].
    Process,
}

/// Every stage name the benches export, in registration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StageName {
    /// Prepared OTLP bytes consumed by the synchronous noop path.
    OtlpNoop,
    /// Plus wire-to-OTAP conversion.
    OtlpConvert,
    /// Plus extraction and identity hashing.
    OtlpExtractHash,
    /// Plus admission, seal and merge.
    OtlpSort,
    /// Plus uncompressed Parquet and synchronous local persistence.
    OtlpParquetLocal,
    /// The same with ZSTD.
    OtlpParquetZstd,
    /// Actual `Sink` into the S3 store, end to end from OTLP bytes.
    OtlpMinio,
    /// Wire-to-OTAP conversion alone.
    Convert,
    /// `extract` alone.
    Extract,
    /// Reservation, admission and seal.
    SortSeal,
    /// `merge_runs`, fully consumed.
    Merge,
    /// Diagnostic Parquet encoding of prepared merged chunks.
    Encode,
    /// Pre-encoded bytes through the actual `LocalFileSystem` store.
    LocalWrite,
    /// Pre-encoded bytes through the actual S3 store.
    Upload,
    /// Actual `Sink::write_block` of a sealed block.
    Sink,
}

impl StageName {
    /// All stages the benches implement, in registration order.
    pub const ALL: [StageName; 15] = [
        StageName::OtlpNoop,
        StageName::OtlpConvert,
        StageName::OtlpExtractHash,
        StageName::OtlpSort,
        StageName::OtlpParquetLocal,
        StageName::OtlpParquetZstd,
        StageName::OtlpMinio,
        StageName::Convert,
        StageName::Extract,
        StageName::SortSeal,
        StageName::Merge,
        StageName::Encode,
        StageName::LocalWrite,
        StageName::Upload,
        StageName::Sink,
    ];

    /// The cumulative synchronous layers Criterion registers, in order.
    pub const LAYERS: [StageName; 6] = [
        StageName::OtlpNoop,
        StageName::OtlpConvert,
        StageName::OtlpExtractHash,
        StageName::OtlpSort,
        StageName::OtlpParquetLocal,
        StageName::OtlpParquetZstd,
    ];

    /// The exported name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            StageName::OtlpNoop => "otlp_noop",
            StageName::OtlpConvert => "otlp_convert",
            StageName::OtlpExtractHash => "otlp_extract_hash",
            StageName::OtlpSort => "otlp_sort",
            StageName::OtlpParquetLocal => "otlp_parquet_local",
            StageName::OtlpParquetZstd => "otlp_parquet_zstd",
            StageName::OtlpMinio => "otlp_minio",
            StageName::Convert => "convert",
            StageName::Extract => "extract",
            StageName::SortSeal => "sort_seal",
            StageName::Merge => "merge",
            StageName::Encode => "encode",
            StageName::LocalWrite => "local_write",
            StageName::Upload => "upload",
            StageName::Sink => "sink",
        }
    }

    /// The stage of an exported name.
    ///
    /// # Errors
    /// Refuses a name no stage has.
    pub fn parse(name: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|stage| stage.as_str() == name)
            .ok_or_else(|| format!("unknown stage {name}").into())
    }

    /// The clock that measures this stage.
    #[must_use]
    pub fn clock(self) -> Clock {
        match self {
            StageName::OtlpMinio | StageName::LocalWrite | StageName::Upload | StageName::Sink => {
                Clock::Process
            }
            _ => Clock::Thread,
        }
    }
}

/// The signal one input file carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    /// OTLP logs requests.
    Logs,
    /// OTLP metrics requests.
    Metrics,
}

/// What the harness declares about one input file, in its sidecar JSON.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Sidecar {
    /// Always `u32le-length-prefixed-otlp`.
    pub format: String,
    /// The one signal of every request in the file.
    pub signal: Signal,
    /// Number of requests.
    pub requests: usize,
    /// Supported records (log records or metric points) over all requests.
    pub records: usize,
    /// Distinct series over all requests.
    pub expected_series: usize,
    /// SHA-256 of the input file.
    pub sha256: String,
    /// Everything else the harness recorded, kept verbatim.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// One input file: deterministic OTLP requests of one signal.
#[derive(Debug, Clone)]
pub struct Input {
    /// The sidecar that describes the file.
    pub sidecar: Sidecar,
    /// Each request's serialized OTLP bytes.
    pub requests: Vec<Bytes>,
}

/// The sidecar path of an input file: the same name with `.json`.
#[must_use]
pub fn sidecar_path(input: &FsPath) -> PathBuf {
    input.with_extension("json")
}

/// Split a length-prefixed file into its requests.
///
/// Every request is a little-endian `u32` length followed by that many
/// bytes. A truncated length or body is an error, never a short input.
///
/// # Errors
/// Refuses a truncated file.
pub fn split_requests(data: &Bytes) -> Result<Vec<Bytes>> {
    let mut requests = Vec::new();
    let mut at = 0usize;
    while at < data.len() {
        let header = data
            .get(at..at + 4)
            .ok_or("truncated request length in the input file")?;
        let length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        at += 4;
        if at + length > data.len() {
            return Err("truncated request body in the input file".into());
        }
        requests.push(data.slice(at..at + length));
        at += length;
    }
    Ok(requests)
}

/// Read one input file and its sidecar, and check they agree.
///
/// # Errors
/// Refuses an unreadable, truncated or mismatched input.
pub fn read_input(path: &FsPath) -> Result<Input> {
    let sidecar: Sidecar = serde_json::from_slice(&std::fs::read(sidecar_path(path))?)?;
    if sidecar.format != "u32le-length-prefixed-otlp" {
        return Err(format!("unknown input format {}", sidecar.format).into());
    }
    let data = Bytes::from(std::fs::read(path)?);
    let digest = hex_digest(&data);
    if digest != sidecar.sha256 {
        return Err(format!(
            "input hash {digest} differs from its sidecar {}",
            sidecar.sha256
        )
        .into());
    }
    let requests = split_requests(&data)?;
    if requests.len() != sidecar.requests {
        return Err(format!(
            "input holds {} requests, its sidecar declares {}",
            requests.len(),
            sidecar.requests
        )
        .into());
    }
    Ok(Input { sidecar, requests })
}

/// Lowercase hexadecimal SHA-256.
#[must_use]
pub fn hex_digest(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut text = String::with_capacity(64);
    for byte in digest {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// The bench configuration file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BenchConfig {
    /// Harness label linking every profile of one workload and configuration.
    pub workload_config_id: String,
    /// The exporter's lake configuration, exactly as the stage uses it.
    pub lake: LakeConfig,
    /// The object store, in the exporter's own `storage` form.
    pub storage: Option<serde_json::Value>,
    /// Directory for synchronous local persistence and temporary files.
    pub scratch_dir: PathBuf,
    /// Series cache capacity.
    pub cache_entries: usize,
    /// Fraction of the input's series marked committed in the block's
    /// partition before admission, which sets the cache hit distribution.
    pub committed_fraction: f64,
    /// Synthetic series marked committed that the input never carries.
    ///
    /// They make the per-iteration cache warm-up heavier without changing
    /// anything the stage admits, which is how a self-test proves that the
    /// warm-up is outside the timer. A measured configuration leaves it at
    /// zero.
    #[serde(default)]
    pub extra_committed_series: usize,
    /// Window start of every block, in Unix seconds.
    pub window_start_secs: i64,
    /// Fixed seal stamp, in Unix microseconds.
    pub seal_at_us: i64,
}

impl BenchConfig {
    /// Read and validate a configuration file.
    ///
    /// # Errors
    /// Refuses an unreadable file or a lake configuration the exporter would
    /// refuse.
    pub fn read(path: &FsPath) -> Result<Self> {
        let cfg: BenchConfig = serde_json::from_slice(&std::fs::read(path)?)?;
        cfg.lake.validate()?;
        if !(0.0..=1.0).contains(&cfg.committed_fraction) {
            return Err("committed_fraction must be within 0..=1".into());
        }
        Ok(cfg)
    }
}

/// Build the configured object store with the exporter's own constructor.
///
/// # Errors
/// Refuses a missing or invalid storage configuration.
pub fn object_store(cfg: &BenchConfig) -> Result<Arc<dyn ObjectStore>> {
    let storage = cfg
        .storage
        .clone()
        .ok_or("this stage needs a storage configuration")?;
    let storage: StorageType = serde_json::from_value(storage)?;
    Ok(otel_arrow_dfe_otap::object_store::from_storage_type(
        &storage,
    )?)
}

/// The production compression.
#[must_use]
pub fn zstd() -> Compression {
    otel_arrow_dfe_series_lake::sink::compression()
}

/// What one diagnostic encode observed of its writer.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct EncodeObservation {
    /// Largest `memory_size()` seen after a write.
    pub writer_memory_max_bytes: usize,
    /// Largest `in_progress_size()` seen after a write.
    pub in_progress_max_bytes: usize,
    /// Row groups flushed by the byte predicate, plus the final one.
    pub flushes: usize,
    /// Encoded bytes.
    pub encoded_bytes: usize,
    /// Capacity of the output vector at close.
    pub output_capacity_bytes: usize,
}

/// Encode a stream of batches exactly as the sink's chunk loop does.
///
/// The flush predicate is the sink's: flush once the writer's memory
/// reaches `writer_limit_bytes` or the in-progress row group reaches
/// `row_group_bytes`. `output` may carry reserved capacity; its growth and
/// the finalization are part of the call.
///
/// # Errors
/// Propagates Arrow and Parquet failures.
pub fn encode_batches(
    schema: arrow::datatypes::SchemaRef,
    batches: impl IntoIterator<Item = Result<RecordBatch>>,
    cfg: &LakeConfig,
    compression: Compression,
    output: Vec<u8>,
) -> Result<(Vec<u8>, EncodeObservation)> {
    let mut encoded = output;
    let mut observed = EncodeObservation::default();
    {
        let properties = writer_properties(compression).build();
        let mut writer = ArrowWriter::try_new(&mut encoded, schema, Some(properties))?;
        for chunk in batches {
            writer.write(&chunk?)?;
            let memory = writer.memory_size();
            let in_progress = writer.in_progress_size();
            observed.writer_memory_max_bytes = observed.writer_memory_max_bytes.max(memory);
            observed.in_progress_max_bytes = observed.in_progress_max_bytes.max(in_progress);
            if row_group_full(&cfg.parquet, memory, in_progress) {
                writer.flush()?;
                observed.flushes += 1;
            }
        }
        let metadata = writer.close()?;
        observed.flushes = metadata.num_row_groups();
    }
    observed.encoded_bytes = encoded.len();
    observed.output_capacity_bytes = encoded.capacity();
    Ok((encoded, observed))
}

/// Encode prepared merged chunks into one Parquet file.
///
/// # Errors
/// Refuses an empty input and propagates Parquet failures.
pub fn encode_chunks(
    chunks: &[RecordBatch],
    cfg: &LakeConfig,
    compression: Compression,
) -> Result<Vec<u8>> {
    let first = chunks
        .first()
        .ok_or_else(|| std::io::Error::other("empty input"))?;
    let (encoded, _observed) = encode_batches(
        first.schema(),
        chunks.iter().cloned().map(Ok),
        cfg,
        compression,
        Vec::new(),
    )?;
    Ok(encoded)
}

/// Every non-empty table of a sealed block, as the sink merges it.
fn merged_tables(block: &Block, cfg: &LakeConfig) -> Result<Vec<(Dataset, Vec<RecordBatch>)>> {
    let mut tables = Vec::new();
    for table in block.tables().filter(|table| !table.is_empty()) {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let merged = merge_runs(runs, table.spec(), cfg.sorting.merge_chunk_bytes)?;
        let chunks = merged.collect::<std::result::Result<Vec<_>, _>>()?;
        tables.push((table.dataset(), chunks));
    }
    Ok(tables)
}

/// Characters Criterion replaces when it turns a benchmark id into a
/// directory name, and the length it truncates that name to.
const CRITERION_UNSAFE: [char; 10] = ['?', '"', '/', '\\', '*', '<', '>', ':', '|', '^'];
const CRITERION_NAME_LEN: usize = 64;

/// Criterion's own directory name for one component of a benchmark id.
///
/// Copied from `criterion::report::make_filename_safe`, so an artifact is
/// read back from the directory Criterion actually wrote.
#[must_use]
pub fn criterion_directory_name(component: &str) -> String {
    let mut name = component.replace(CRITERION_UNSAFE, "_");
    if name.len() > CRITERION_NAME_LEN {
        let mut end = CRITERION_NAME_LEN;
        while end > 0 && !name.is_char_boundary(end) {
            end -= 1;
        }
        name.truncate(end);
    }
    name
}

/// The seconds of work Criterion measured for one benchmark id.
///
/// The id is the one that benchmark actually ran with: a group that is
/// repeated under a new id has its own artifacts, and reading another id's
/// would schedule the next attempt from a stale measurement and publish a
/// distribution that is not the one selected. A missing artifact is an
/// error that names the id and what the group directory does hold; it is
/// never answered from another id's files.
///
/// # Errors
/// Refuses a missing or unreadable artifact, and one carrying no samples.
pub fn criterion_measured_seconds(home: &FsPath, group: &str, function: &str) -> Result<f64> {
    let directory = home
        .join(criterion_directory_name(group))
        .join(criterion_directory_name(function));
    let path = directory.join("new").join("sample.json");
    if !path.is_file() {
        let siblings: Vec<String> = directory
            .parent()
            .and_then(|parent| std::fs::read_dir(parent).ok())
            .map(|entries| {
                entries
                    .filter_map(|entry| Some(entry.ok()?.file_name().to_string_lossy().to_string()))
                    .collect()
            })
            .unwrap_or_default();
        return Err(format!(
            "Criterion wrote no sample for {group}/{function} at {}; the group \
             directory holds {siblings:?}",
            path.display()
        )
        .into());
    }
    let sample: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    let times = sample
        .get("times")
        .and_then(serde_json::Value::as_array)
        .filter(|times| !times.is_empty())
        .ok_or_else(|| format!("{} carries no sample times", path.display()))?;
    Ok(times
        .iter()
        .filter_map(serde_json::Value::as_f64)
        .sum::<f64>()
        / 1e9)
}

/// A process-unique token for scratch and object names.
fn uuid_like() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The wire form of each request, as the receiver hands it on.
fn wire_of(input: &Input) -> Vec<OtlpProtoBytes> {
    input
        .requests
        .iter()
        .map(|bytes| match input.sidecar.signal {
            Signal::Logs => OtlpProtoBytes::ExportLogsRequest(bytes.clone()),
            Signal::Metrics => OtlpProtoBytes::ExportMetricsRequest(bytes.clone()),
        })
        .collect()
}

/// The production conversion: `OtapPayload` from OTLP bytes, then the
/// default-option conversion trait call the exporter's worker makes.
fn convert_one(bytes: OtlpProtoBytes) -> Result<OtapArrowRecords> {
    let payload: OtapPayload = bytes.into();
    Ok(payload.try_into_with_default()?)
}

/// Convert then extract one request at a time, dropping each conversion as
/// the exporter's worker does.
fn convert_extract(wire: Vec<OtlpProtoBytes>, cfg: &LakeConfig) -> Result<Vec<Extracted>> {
    let mut out = Vec::with_capacity(wire.len());
    for bytes in wire {
        let mut records = convert_one(bytes)?;
        out.push(extract(&mut records, cfg)?);
    }
    Ok(out)
}

/// Reserve and admit every request into one block.
fn admit_all(
    extracted: Vec<Extracted>,
    cache: &mut SeriesCache,
    cfg: &BenchConfig,
) -> Result<Block> {
    let mut block = Block::new(cfg.window_start_secs, SEQ, cfg.lake.clone());
    for request in extracted {
        let reservation = block.reserve(&request, cache, TOKEN_BYTES)?;
        block.admit(request, reservation)?;
    }
    Ok(block)
}

/// Merge every table of a sealed block, keeping at most one chunk alive.
fn merge_consume(block: &Block, cfg: &LakeConfig) -> Result<(usize, usize)> {
    let mut values_rows = 0usize;
    let mut series_rows = 0usize;
    for table in block.tables().filter(|table| !table.is_empty()) {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        for chunk in merge_runs(runs, table.spec(), cfg.sorting.merge_chunk_bytes)? {
            let chunk = std::hint::black_box(chunk?);
            if table.dataset().is_series() {
                series_rows += chunk.num_rows();
            } else {
                values_rows += chunk.num_rows();
            }
        }
    }
    Ok((values_rows, series_rows))
}

/// Merge and encode every table of a sealed block, streaming the chunks.
fn encode_block(
    block: &Block,
    cfg: &LakeConfig,
    compression: Compression,
) -> Result<Vec<(Dataset, Vec<u8>)>> {
    let mut files = Vec::new();
    for table in block.tables().filter(|table| !table.is_empty()) {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let schema = runs.first().ok_or("empty table")?.schema();
        let merged = merge_runs(runs, table.spec(), cfg.sorting.merge_chunk_bytes)?;
        let (encoded, _observed) = encode_batches(
            schema,
            merged.map(|chunk| chunk.map_err(Into::into)),
            cfg,
            compression,
            Vec::new(),
        )?;
        files.push((table.dataset(), encoded));
    }
    Ok(files)
}

/// Rows of the values tables and of the series tables of a block.
fn block_rows(block: &Block) -> (usize, usize) {
    let mut values = 0usize;
    let mut series = 0usize;
    for table in block.tables() {
        if table.dataset().is_series() {
            series += table.rows();
        } else {
            values += table.rows();
        }
    }
    (values, series)
}

/// Write bytes through a `BufWriter` exactly as the sink's upload path does.
///
/// Completion means the writer has shut down, the store reports the object
/// written, and the read-back size and hash match; see [`COMPLETION_SEMANTICS`].
async fn put_buffered(
    store: Arc<dyn ObjectStore>,
    path: Path,
    data: Bytes,
    cfg: &LakeConfig,
) -> Result<usize> {
    let length = data.len();
    let mut writer = BufWriter::with_capacity(store, path, cfg.upload.part_bytes)
        .with_max_concurrency(cfg.upload.concurrency);
    writer.put(data).await?;
    writer.shutdown().await?;
    Ok(length)
}

/// A stage's input for one iteration, built outside every timer.
pub enum Prepared {
    /// OTLP bytes in their wire form.
    Wire(Vec<OtlpProtoBytes>),
    /// Converted OTAP records.
    Records(Vec<OtapArrowRecords>),
    /// Extracted requests and a freshly warmed series cache.
    Extracted(Vec<Extracted>, SeriesCache),
    /// Nothing beyond the stage's own retained fixture.
    Fixture,
    /// Pre-reserved output buffers for the encoder, one per table.
    Buffers(Vec<Vec<u8>>),
    /// A sink with a fresh file identity.
    Sink(Box<Sink>),
    /// A cumulative layer: wire bytes, the warmed cache it admits into and
    /// the destination this iteration writes to.
    Cumulative(Vec<OtlpProtoBytes>, SeriesCache, Destination),
    /// Pre-encoded objects with the path each one is written to.
    Objects(Vec<(Path, Bytes)>),
}

/// Where one iteration of a cumulative layer puts what it produced.
pub enum Destination {
    /// Nothing is written; the merge is consumed and dropped.
    None,
    /// One local file per table, in table order, created by the stage.
    Files(Vec<PathBuf>),
    /// A sink with a fresh file identity.
    Sink(Box<Sink>),
}

impl Prepared {
    /// Where this input's iteration will write, if anywhere.
    ///
    /// Two preparations of one stage must name different destinations, so
    /// that no iteration overwrites what an earlier one wrote. The sink is
    /// rendered through its debug form, which carries its file identity.
    #[must_use]
    pub fn destinations(&self) -> Vec<String> {
        match self {
            Prepared::Cumulative(_, _, Destination::Files(paths)) => paths
                .iter()
                .map(|path| path.display().to_string())
                .collect(),
            Prepared::Cumulative(_, _, Destination::Sink(sink)) | Prepared::Sink(sink) => {
                vec![format!("{sink:?}")]
            }
            Prepared::Objects(objects) => {
                objects.iter().map(|(path, _)| path.to_string()).collect()
            }
            _ => Vec::new(),
        }
    }

    /// What this input carries, for the self-test that proves every stage
    /// is handed its whole setup instead of building it while timed.
    #[must_use]
    pub fn carries(&self) -> &'static str {
        match self {
            Prepared::Wire(_) => "wire",
            Prepared::Records(_) => "records",
            Prepared::Extracted(_, _) => "extracted+cache",
            Prepared::Fixture => "fixture",
            Prepared::Buffers(_) => "buffers",
            Prepared::Sink(_) => "sink",
            Prepared::Cumulative(_, _, Destination::None) => "wire+cache",
            Prepared::Cumulative(_, _, Destination::Files(_)) => "wire+cache+paths",
            Prepared::Cumulative(_, _, Destination::Sink(_)) => "wire+cache+sink",
            Prepared::Objects(_) => "objects+paths",
        }
    }
}

/// What one run produced, retained until it has been observed.
pub enum Retained {
    /// Nothing is stored; the payloads are kept only as retained output.
    Payloads(Vec<OtapPayload>),
    /// Converted records.
    Records(Vec<OtapArrowRecords>),
    /// Extracted requests.
    Extracted(Vec<Extracted>),
    /// A sealed block and the cache's hit and miss counts.
    Block(Box<Block>, otel_arrow_dfe_series_lake::cache::CacheStats),
    /// A consumed merge: values and series rows.
    Merged(usize, usize),
    /// Encoded Parquet files and what the writer observed.
    Encoded(Vec<(Dataset, Vec<u8>)>, Vec<EncodeObservation>),
    /// Synchronously persisted files and the bytes each must hold.
    Files(Vec<(Dataset, PathBuf, Vec<u8>)>),
    /// Objects written through a store.
    Objects(Vec<(Path, usize)>),
    /// Files a sink wrote.
    Flushed(FlushReport),
}

/// One run's output and its nested sub-stage timings.
pub struct Output {
    /// What the run produced.
    pub retained: Retained,
    /// Nested, non-overlapping parts of the timed run.
    pub parts: Vec<(&'static str, Timing)>,
    /// State that outlives one block in production, handed back so that
    /// it is dropped with the output, after the timer has stopped. Nothing
    /// reads it; `a_heavier_fixture_does_not_move_the_measurement` fails if
    /// the cache is dropped inside the timer instead.
    pub _kept: Kept,
}

/// What a run hands back only so that its teardown is not timed.
///
/// The exporter keeps one series cache and one sink across every block it
/// writes, so tearing either down is never part of the per-block cost; for a
/// large cache the destructor costs far more than the admission.
#[derive(Default)]
pub struct Kept {
    /// The series cache the run admitted into.
    _cache: Option<SeriesCache>,
    /// The sink the run wrote through.
    _sink: Option<Box<Sink>>,
}

/// One verification a stage made of its output.
#[derive(Debug, Clone, Serialize)]
pub struct Check {
    /// Check name.
    pub name: String,
    /// Whether it held.
    pub passed: bool,
    /// What was compared.
    pub detail: String,
}

impl Check {
    fn new(name: &str, passed: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed,
            detail: detail.into(),
        }
    }
}

/// What observing one output found.
#[derive(Debug, Clone, Serialize)]
pub struct Observation {
    /// How the output is represented, or `none` when nothing is stored.
    pub representation: &'static str,
    /// Output bytes in that representation.
    pub output_bytes: u64,
    /// Values rows (log records or metric points) in the output.
    pub values_rows: usize,
    /// Series rows in the output, when the stage produces any.
    pub series_rows: Option<usize>,
    /// Every verification made.
    pub checks: Vec<Check>,
    /// Stage-specific numbers.
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// A stage bound to its input, configuration and prepared fixture.
pub struct Stage {
    name: StageName,
    cfg: BenchConfig,
    input: Input,
    compression: Compression,
    runtime: tokio::runtime::Runtime,
    store: Option<Arc<dyn ObjectStore>>,
    /// Series marked committed before each admission.
    committed: Vec<SeriesId>,
    /// The sealed block of the block-level stages.
    block: Option<Block>,
    /// Merged chunks of the encoder stage.
    chunks: Vec<(Dataset, Vec<RecordBatch>)>,
    /// Pre-encoded bytes of the persistence stages.
    encoded: Vec<(Dataset, Bytes)>,
    /// The non-empty tables of a sealed block, in write order, so that a
    /// cumulative layer's destination paths are built before it is timed.
    table_names: Vec<&'static str>,
    /// Output capacity estimates from the encoder's warm-up.
    capacity: Vec<usize>,
    /// Whether timed buffers are pre-reserved (timing mode only).
    reserve_output: bool,
    /// How many pieces of per-iteration setup this stage has built: one
    /// count per wire clone, prepared conversion, warmed cache, sink,
    /// destination path set, object set and output buffer set. A timed run
    /// must never advance it, which is what the self-test asserts.
    setup_built: AtomicU64,
    /// Fixture checks made while preparing, reported with every output.
    fixture_checks: Vec<Check>,
    /// Fixture numbers reported with every output.
    fixture_extra: serde_json::Map<String, serde_json::Value>,
    serial: AtomicU64,
}

impl Stage {
    /// Bind a stage and build its retained fixture, outside every timer.
    ///
    /// `reserve_output` lets the encoder pre-reserve its output from a
    /// warm-up estimate; an allocation profile passes false so that all
    /// growth is measured.
    ///
    /// # Errors
    /// Refuses a stage whose fixture cannot be built or verified.
    pub fn new(
        name: StageName,
        cfg: BenchConfig,
        input: Input,
        compression: Compression,
        reserve_output: bool,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let store = match name {
            StageName::OtlpMinio | StageName::Upload | StageName::Sink | StageName::LocalWrite => {
                Some(object_store(&cfg)?)
            }
            _ => None,
        };
        std::fs::create_dir_all(&cfg.scratch_dir)?;
        let mut stage = Self {
            name,
            cfg,
            input,
            compression,
            runtime,
            store,
            committed: Vec::new(),
            block: None,
            chunks: Vec::new(),
            encoded: Vec::new(),
            table_names: Vec::new(),
            capacity: Vec::new(),
            reserve_output,
            setup_built: AtomicU64::new(0),
            fixture_checks: Vec::new(),
            fixture_extra: serde_json::Map::new(),
            serial: AtomicU64::new(0),
        };
        stage.committed = stage.committed_series()?;
        stage.build_fixture()?;
        Ok(stage)
    }

    /// The stage's name.
    #[must_use]
    pub fn name(&self) -> StageName {
        self.name
    }

    /// The input's supported records, the denominator of every per-record
    /// quantity.
    #[must_use]
    pub fn records(&self) -> usize {
        self.input.sidecar.records
    }

    /// Input bytes the stage retains as its fixture, measured through the
    /// same deduplicated Arrow accounting the exporter uses where the
    /// fixture is Arrow data.
    #[must_use]
    pub fn fixture_retained_bytes(&self) -> u64 {
        let mut seen = CountedAllocations::default();
        let wire: usize = self.input.requests.iter().map(Bytes::len).sum();
        let mut bytes = wire;
        if let Some(block) = &self.block {
            for table in block.tables() {
                for batch in table.iter_snapshots() {
                    bytes += record_batch_pinned_bytes(batch, &mut seen);
                }
            }
        }
        for (_, chunks) in &self.chunks {
            for chunk in chunks {
                bytes += record_batch_pinned_bytes(chunk, &mut seen);
            }
        }
        bytes += self
            .encoded
            .iter()
            .map(|(_, data)| data.len())
            .sum::<usize>();
        bytes as u64
    }

    /// Bytes one prepared input holds beyond the retained fixture: the
    /// converted records or extracted requests a stage consumes, measured
    /// through the deduplicated Arrow accounting. Wire bytes share the
    /// fixture's buffers and add nothing.
    #[must_use]
    pub fn input_bytes(&self, prepared: &Prepared) -> u64 {
        let mut seen = CountedAllocations::default();
        let bytes = match prepared {
            Prepared::Records(records) => records
                .iter()
                .flat_map(|request| {
                    request
                        .allowed_payload_types()
                        .iter()
                        .filter_map(|payload| request.get(*payload))
                        .collect::<Vec<_>>()
                })
                .map(|batch| record_batch_pinned_bytes(batch, &mut seen))
                .sum(),
            Prepared::Extracted(extracted, _) => extracted
                .iter()
                .map(|e| {
                    e.pinned_bytes + e.descriptors.iter().map(|d| d.approx_bytes).sum::<usize>()
                })
                .sum(),
            Prepared::Buffers(buffers) => buffers.iter().map(Vec::capacity).sum(),
            Prepared::Cumulative(_, _, _)
            | Prepared::Objects(_)
            | Prepared::Wire(_)
            | Prepared::Fixture
            | Prepared::Sink(_) => 0,
        };
        bytes as u64
    }

    /// The series the configured committed fraction marks committed.
    fn committed_series(&self) -> Result<Vec<SeriesId>> {
        let synthetic: Vec<SeriesId> = (0..self.cfg.extra_committed_series)
            .map(|index| series_id(&format!("bench-synthetic-{index}").into_bytes()))
            .collect();
        if self.cfg.committed_fraction <= 0.0 {
            return Ok(synthetic);
        }
        let mut all = BTreeSet::new();
        for request in convert_extract(self.wire(), &self.cfg.lake)? {
            for row in &request.descriptors {
                let _ = all.insert(row.series_id);
            }
        }
        let keep = (all.len() as f64 * self.cfg.committed_fraction).round() as usize;
        // The synthetic ids go in first, so the real ones are the most
        // recently used entries and a cache large enough for all of them
        // keeps every real id resident.
        Ok(synthetic
            .into_iter()
            .chain(all.into_iter().take(keep))
            .collect())
    }

    /// How many pieces of per-iteration setup this stage has built.
    #[must_use]
    pub fn setup_built(&self) -> u64 {
        self.setup_built.load(Ordering::Relaxed)
    }

    /// Count one piece of setup built outside the timer.
    fn count_setup(&self) {
        let _ = self.setup_built.fetch_add(1, Ordering::Relaxed);
    }

    /// A fresh clone of the input's wire requests, counted as setup.
    ///
    /// `run` never clones the input: every stage is handed its wire form
    /// already built, so a clone reintroduced inside `run` advances the
    /// counter the self-test requires to stay still.
    fn wire(&self) -> Vec<OtlpProtoBytes> {
        self.count_setup();
        wire_of(&self.input)
    }

    /// A series cache warmed to the configured hit distribution.
    fn warm_cache(&self) -> SeriesCache {
        self.count_setup();
        let mut cache = SeriesCache::new(self.cfg.cache_entries);
        let partition = otel_arrow_dfe_series_lake::clock::PartitionId::from_unix_secs(
            self.cfg.window_start_secs,
        );
        for id in &self.committed {
            cache.mark_committed(*id, partition);
        }
        cache
    }

    /// A sink with a file identity no earlier iteration used, so every
    /// iteration writes new objects instead of overwriting one.
    fn new_sink(&self) -> Result<Sink> {
        self.count_setup();
        let store = self.store.clone().ok_or("this stage has no store")?;
        let naming = FileNaming {
            writer_id: self.cfg.lake.writer_id.clone(),
            boot_id: format!("bench{}", uuid_like().replace('-', "x")),
        };
        Ok(Sink::new(store, self.cfg.lake.clone(), naming, |timeout| {
            Box::pin(tokio::time::sleep(timeout))
        }))
    }

    /// A freshly sealed block of the whole input.
    fn sealed_block(&self) -> Result<Block> {
        let extracted = convert_extract(self.wire(), &self.cfg.lake)?;
        let mut cache = self.warm_cache();
        let mut block = admit_all(extracted, &mut cache, &self.cfg)?;
        block.seal(self.cfg.seal_at_us)?;
        Ok(block)
    }

    fn build_fixture(&mut self) -> Result<()> {
        match self.name {
            StageName::Merge => {
                let block = self.sealed_block()?;
                self.fixture_checks
                    .extend(verify_merge(&block, &self.cfg.lake)?);
                self.record_merged_bytes(&block)?;
                self.record_merge_keys(&block)?;
                self.block = Some(block);
            }
            StageName::Extract => {
                self.record_values_capacity()?;
            }
            StageName::OtlpSort => {
                let block = self.sealed_block()?;
                self.record_merged_bytes(&block)?;
            }
            StageName::OtlpParquetLocal | StageName::OtlpParquetZstd => {
                // The tables a block of this input holds, in write order.
                // Their names are what the per-iteration destination paths
                // are built from, outside the timer.
                let block = self.sealed_block()?;
                self.table_names = block
                    .tables()
                    .filter(|table| !table.is_empty())
                    .map(|table| table.dataset().name())
                    .collect();
            }
            StageName::Sink => {
                let block = self.sealed_block()?;
                self.block = Some(block);
            }
            StageName::Encode => {
                let block = self.sealed_block()?;
                self.chunks = merged_tables(&block, &self.cfg.lake)?;
                for (_, chunks) in &self.chunks {
                    let estimate = encode_chunks(chunks, &self.cfg.lake, self.compression)?.len();
                    self.capacity.push(estimate);
                }
            }
            StageName::LocalWrite | StageName::Upload => {
                let block = self.sealed_block()?;
                self.encoded = merged_tables(&block, &self.cfg.lake)?
                    .into_iter()
                    .map(|(dataset, chunks)| {
                        encode_chunks(&chunks, &self.cfg.lake, self.compression)
                            .map(|data| (dataset, Bytes::from(data)))
                    })
                    .collect::<Result<Vec<_>>>()?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Build one iteration's input. Never timed.
    ///
    /// # Errors
    /// Propagates a failure building the input.
    pub fn prepare(&self) -> Result<Prepared> {
        Ok(match self.name {
            StageName::OtlpNoop
            | StageName::OtlpConvert
            | StageName::OtlpExtractHash
            | StageName::Convert => Prepared::Wire(self.wire()),
            // Every cache-carrying stage warms its cache first and builds
            // the input it admits last, as the exporter extracts a request
            // right before admitting it into a cache that persists. A heavy
            // warm-up therefore cannot push the admitted input out of the
            // CPU caches before the timer starts.
            StageName::OtlpSort => {
                let cache = self.warm_cache();
                Prepared::Cumulative(self.wire(), cache, Destination::None)
            }
            StageName::OtlpParquetLocal | StageName::OtlpParquetZstd => {
                let serial = self.serial.fetch_add(1, Ordering::Relaxed);
                self.count_setup();
                let paths = self
                    .table_names
                    .iter()
                    .map(|name| {
                        self.cfg
                            .scratch_dir
                            .join(format!("{name}-{}-{serial:08}.parquet", std::process::id()))
                    })
                    .collect();
                let cache = self.warm_cache();
                Prepared::Cumulative(self.wire(), cache, Destination::Files(paths))
            }
            StageName::OtlpMinio => {
                let cache = self.warm_cache();
                let sink = Box::new(self.new_sink()?);
                Prepared::Cumulative(self.wire(), cache, Destination::Sink(sink))
            }
            StageName::Extract => {
                // The conversion `extract` consumes is setup of its own.
                self.count_setup();
                Prepared::Records(
                    self.wire()
                        .into_iter()
                        .map(convert_one)
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            StageName::SortSeal => {
                let cache = self.warm_cache();
                // The conversion and extraction admission consumes is setup
                // of its own.
                self.count_setup();
                Prepared::Extracted(convert_extract(self.wire(), &self.cfg.lake)?, cache)
            }
            StageName::Merge => Prepared::Fixture,
            StageName::LocalWrite | StageName::Upload => {
                let serial = self.serial.fetch_add(1, Ordering::Relaxed);
                self.count_setup();
                Prepared::Objects(
                    self.encoded
                        .iter()
                        .map(|(dataset, data)| {
                            (
                                Path::from(format!(
                                    "bench/{}/{}-{}-{serial:08}.parquet",
                                    self.name.as_str(),
                                    dataset.name(),
                                    std::process::id()
                                )),
                                data.clone(),
                            )
                        })
                        .collect(),
                )
            }
            StageName::Encode => Prepared::Buffers({
                self.count_setup();
                self.capacity
                    .iter()
                    .map(|&estimate| {
                        if self.reserve_output {
                            Vec::with_capacity(estimate)
                        } else {
                            Vec::new()
                        }
                    })
                    .collect()
            }),
            StageName::Sink => Prepared::Sink(Box::new(self.new_sink()?)),
        })
    }

    /// The measured operation.
    ///
    /// # Errors
    /// Propagates every library failure; a failed run is never timed as a
    /// success by the callers.
    pub fn run(&self, input: Prepared) -> Result<Output> {
        let lake = &self.cfg.lake;
        let mut parts = Vec::new();
        let mut kept = Kept::default();
        let retained = match (self.name, input) {
            (StageName::OtlpNoop, Prepared::Wire(wire)) => Retained::Payloads(
                wire.into_iter()
                    .map(|bytes| std::hint::black_box(OtapPayload::from(bytes)))
                    .collect(),
            ),
            (StageName::OtlpConvert | StageName::Convert, Prepared::Wire(wire)) => {
                Retained::Records(
                    wire.into_iter()
                        .map(convert_one)
                        .collect::<Result<Vec<_>>>()?,
                )
            }
            (StageName::OtlpExtractHash, Prepared::Wire(wire)) => {
                Retained::Extracted(convert_extract(wire, lake)?)
            }
            (StageName::Extract, Prepared::Records(records)) => {
                let mut out = Vec::with_capacity(records.len());
                for mut request in records {
                    out.push(extract(&mut request, lake)?);
                }
                Retained::Extracted(out)
            }
            (StageName::SortSeal, Prepared::Extracted(extracted, mut cache)) => {
                let (mut block, admission) = timed(|| admit_all(extracted, &mut cache, &self.cfg))?;
                let ((), seal) = timed(|| block.seal(self.cfg.seal_at_us))?;
                parts.push(("admission", admission));
                parts.push(("seal", seal));
                let stats = cache.stats();
                kept._cache = Some(cache);
                Retained::Block(Box::new(block), stats)
            }
            (StageName::OtlpSort, Prepared::Cumulative(wire, mut cache, Destination::None)) => {
                let extracted = convert_extract(wire, lake)?;
                let mut block = admit_all(extracted, &mut cache, &self.cfg)?;
                block.seal(self.cfg.seal_at_us)?;
                kept._cache = Some(cache);
                let (values, series) = merge_consume(&block, lake)?;
                Retained::Merged(values, series)
            }
            (StageName::Merge, Prepared::Fixture) => {
                let block = self.block.as_ref().ok_or("merge has no block")?;
                let (values, series) = merge_consume(block, lake)?;
                Retained::Merged(values, series)
            }
            (
                StageName::OtlpParquetLocal | StageName::OtlpParquetZstd,
                Prepared::Cumulative(wire, mut cache, Destination::Files(paths)),
            ) => {
                let compression = if self.name == StageName::OtlpParquetZstd {
                    zstd()
                } else {
                    Compression::UNCOMPRESSED
                };
                let extracted = convert_extract(wire, lake)?;
                let mut block = admit_all(extracted, &mut cache, &self.cfg)?;
                block.seal(self.cfg.seal_at_us)?;
                kept._cache = Some(cache);
                let mut files = Vec::new();
                for ((dataset, encoded), path) in encode_block(&block, lake, compression)?
                    .into_iter()
                    .zip(paths)
                {
                    let mut file = std::fs::File::create(&path)?;
                    file.write_all(&encoded)?;
                    drop(file);
                    files.push((dataset, path, encoded));
                }
                Retained::Files(files)
            }
            (StageName::Encode, Prepared::Buffers(buffers)) => {
                let mut files = Vec::new();
                let mut observed = Vec::new();
                for ((dataset, chunks), buffer) in self.chunks.iter().zip(buffers) {
                    let first = chunks.first().ok_or("empty table")?;
                    let (encoded, observation) = encode_batches(
                        first.schema(),
                        chunks.iter().cloned().map(Ok),
                        lake,
                        self.compression,
                        buffer,
                    )?;
                    files.push((*dataset, encoded));
                    observed.push(observation);
                }
                Retained::Encoded(files, observed)
            }
            (StageName::LocalWrite | StageName::Upload, Prepared::Objects(prepared)) => {
                let store = self.store.clone().ok_or("no store")?;
                let objects = self.runtime.block_on(async {
                    let mut objects = Vec::new();
                    for (path, data) in prepared {
                        let length = put_buffered(store.clone(), path.clone(), data, lake).await?;
                        objects.push((path, length));
                    }
                    Ok::<_, Box<dyn std::error::Error>>(objects)
                })?;
                Retained::Objects(objects)
            }
            (StageName::Sink, Prepared::Sink(sink)) => {
                let block = self.block.as_ref().ok_or("sink has no block")?;
                let report = self
                    .runtime
                    .block_on(sink.write_block(block, &CancellationToken::new()))?;
                kept._sink = Some(sink);
                Retained::Flushed(report)
            }
            (
                StageName::OtlpMinio,
                Prepared::Cumulative(wire, mut cache, Destination::Sink(sink)),
            ) => {
                let (block, prepare) = timed(|| {
                    let extracted = convert_extract(wire, lake)?;
                    let mut block = admit_all(extracted, &mut cache, &self.cfg)?;
                    block.seal(self.cfg.seal_at_us)?;
                    Ok::<_, Box<dyn std::error::Error>>(block)
                })?;
                let (report, write) = timed_process(|| {
                    self.runtime
                        .block_on(sink.write_block(&block, &CancellationToken::new()))
                })?;
                parts.push(("convert_extract_admit_seal", prepare));
                parts.push(("sink_write_block", write));
                kept._cache = Some(cache);
                kept._sink = Some(sink);
                Retained::Flushed(report)
            }
            (name, _) => {
                return Err(format!("{} was given another stage's input", name.as_str()).into());
            }
        };
        Ok(Output {
            retained,
            parts,
            _kept: kept,
        })
    }

    /// Account for and verify one output, then remove what it stored.
    /// Never timed.
    ///
    /// # Errors
    /// Propagates a failure reading the output back; a wrong output is a
    /// failed check, not an error.
    pub fn observe(&self, output: &Output) -> Result<Observation> {
        let expected = self.input.sidecar.records;
        let expected_series = self.input.sidecar.expected_series;
        let mut checks = self.fixture_checks.clone();
        let mut extra = self.fixture_extra.clone();
        let (representation, output_bytes, values_rows, series_rows) = match &output.retained {
            Retained::Payloads(payloads) => {
                checks.push(Check::new(
                    "requests_consumed",
                    payloads.len() == self.input.requests.len(),
                    format!("{} of {}", payloads.len(), self.input.requests.len()),
                ));
                ("none", 0, expected, None)
            }
            Retained::Records(records) => {
                let mut seen = CountedAllocations::default();
                let mut bytes = 0usize;
                let mut buffers = 0usize;
                for request in records {
                    for payload in request.allowed_payload_types() {
                        if let Some(batch) = request.get(*payload) {
                            bytes += record_batch_pinned_bytes(batch, &mut seen);
                            buffers += 1;
                        }
                    }
                }
                let rows: usize = records.iter().map(OtapArrowRecords::num_items).sum();
                let _ = extra.insert("pinned_record_batches".into(), buffers.into());
                ("otap_arrow_records", bytes as u64, rows, None)
            }
            Retained::Extracted(extracted) => {
                let rows: usize = extracted.iter().map(|e| e.stats.rows).sum();
                let bytes: usize = extracted
                    .iter()
                    .map(|e| {
                        e.pinned_bytes + e.descriptors.iter().map(|d| d.approx_bytes).sum::<usize>()
                    })
                    .sum();
                let mut series = HashSet::new();
                let mut hashes_ok = true;
                let mut datasets_ok = true;
                let values_of = Dataset::values_of(match self.input.sidecar.signal {
                    Signal::Logs => otel_arrow_dfe_series_lake::canonical::Signal::Logs,
                    Signal::Metrics => otel_arrow_dfe_series_lake::canonical::Signal::Metrics,
                });
                for request in extracted {
                    for row in &request.descriptors {
                        hashes_ok &= series_id(&row.identity_bytes) == row.series_id;
                        let _ = series.insert(row.series_id);
                    }
                    datasets_ok &= request
                        .values
                        .iter()
                        .all(|(dataset, _)| *dataset == values_of);
                }
                checks.push(Check::new(
                    "semantic_hashes",
                    hashes_ok,
                    "every series id is the hash of its canonical identity bytes",
                ));
                checks.push(Check::new(
                    "row_types",
                    datasets_ok,
                    format!("every values batch belongs to {}", values_of.name()),
                ));
                checks.push(Check::new(
                    "descriptor_coverage",
                    series.len() == expected_series,
                    format!(
                        "{} distinct series, {expected_series} expected",
                        series.len()
                    ),
                ));
                ("extracted_rows", bytes as u64, rows, Some(series.len()))
            }
            Retained::Block(block, stats) => {
                let (values, series) = block_rows(block);
                let emitted = expected_series - self.committed.len().min(expected_series);
                checks.push(Check::new(
                    "series_rows",
                    series == emitted,
                    format!(
                        "{series} series rows, {emitted} expected after {} committed",
                        self.committed.len()
                    ),
                ));
                checks.push(Check::new(
                    "sealed",
                    block.is_sealed(),
                    "the block committed its seal",
                ));
                let _ = extra.insert("cache_hits_count".into(), stats.hits.into());
                let _ = extra.insert("cache_misses_count".into(), stats.misses.into());
                ("sealed_block", block.bytes as u64, values, Some(series))
            }
            Retained::Merged(values, series) => {
                let bytes = extra
                    .get("merged_pinned_bytes")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                ("merged_chunks", bytes, *values, Some(*series))
            }
            Retained::Encoded(files, observed) => {
                let bytes: usize = files.iter().map(|(_, data)| data.len()).sum();
                let (values, series) = parquet_rows(
                    files
                        .iter()
                        .map(|(d, data)| (*d, Bytes::from(data.clone()))),
                )?;
                let _ = extra.insert("writer".into(), serde_json::to_value(observed)?);
                let _ = extra.insert(
                    "output_capacity_bytes".into(),
                    observed
                        .iter()
                        .map(|o| o.output_capacity_bytes)
                        .sum::<usize>()
                        .into(),
                );
                ("parquet_bytes", bytes as u64, values, Some(series))
            }
            Retained::Files(files) => {
                let _ = extra.insert("objects_count".into(), files.len().into());
                let mut matched = true;
                let mut pairs = Vec::new();
                for (dataset, path, data) in files {
                    let written = std::fs::read(path)?;
                    matched &= hex_digest(&written) == hex_digest(data);
                    pairs.push((*dataset, Bytes::from(written)));
                    std::fs::remove_file(path)?;
                }
                checks.push(Check::new(
                    "file_bytes_hash",
                    matched,
                    "every file holds exactly the encoded bytes",
                ));
                let bytes: usize = files.iter().map(|(_, _, data)| data.len()).sum();
                let (values, series) = parquet_rows(pairs)?;
                ("parquet_files", bytes as u64, values, Some(series))
            }
            Retained::Objects(objects) => {
                let store = self.store.clone().ok_or("no store")?;
                let mut sized = true;
                let mut hashed = true;
                let mut bytes = 0usize;
                for ((path, length), (_, data)) in objects.iter().zip(&self.encoded) {
                    let (meta, body) = self.runtime.block_on(async {
                        let meta = store.head(path).await?;
                        let body = store.get(path).await?.bytes().await?;
                        store.delete(path).await?;
                        Ok::<_, object_store::Error>((meta, body))
                    })?;
                    sized &= meta.size == *length as u64;
                    hashed &= hex_digest(&body) == hex_digest(data);
                    bytes += length;
                }
                checks.push(Check::new(
                    "head_size",
                    sized,
                    "HEAD size equals the bytes written",
                ));
                checks.push(Check::new(
                    "downloaded_hash",
                    hashed,
                    "a download hashes to the bytes written",
                ));
                checks.push(Check::new(
                    "objects_written",
                    objects.len() == self.encoded.len(),
                    format!("{} objects", objects.len()),
                ));
                let _ = extra.insert("objects_count".into(), objects.len().into());
                let _ = extra.insert("completion_semantics".into(), COMPLETION_SEMANTICS.into());
                ("stored_objects", bytes as u64, expected, None)
            }
            Retained::Flushed(report) => {
                let store = self.store.clone().ok_or("no store")?;
                let mut bytes = 0u64;
                let mut pairs = Vec::new();
                for (dataset, path, _rows) in &report.files {
                    let body = self.runtime.block_on(async {
                        let body = store.get(path).await?.bytes().await?;
                        store.delete(path).await?;
                        Ok::<_, object_store::Error>(body)
                    })?;
                    bytes += body.len() as u64;
                    let metadata = ParquetRecordBatchReaderBuilder::try_new(body.clone())?
                        .metadata()
                        .clone();
                    let zstd_only = metadata.row_groups().iter().all(|group| {
                        group
                            .columns()
                            .iter()
                            .all(|column| matches!(column.compression(), Compression::ZSTD(_)))
                    });
                    checks.push(Check::new(
                        "sink_compression_zstd",
                        zstd_only,
                        format!("{} column chunks of {}", dataset.name(), path),
                    ));
                    let _ = extra.insert(
                        format!("{}_row_groups_count", dataset.name()),
                        metadata.num_row_groups().into(),
                    );
                    pairs.push((*dataset, body));
                }
                let (values, series) = parquet_rows(pairs)?;
                let _ = extra.insert("objects_count".into(), report.files.len().into());
                let _ = extra.insert("completion_semantics".into(), COMPLETION_SEMANTICS.into());
                checks.push(Check::new(
                    "descriptor_coverage",
                    series == expected_series - self.committed.len().min(expected_series),
                    format!("{series} series rows written"),
                ));
                ("stored_objects", bytes, values, Some(series))
            }
        };
        checks.push(Check::new(
            "values_rows",
            values_rows == expected,
            format!("{values_rows} values rows, {expected} input records"),
        ));
        for (name, timing) in &output.parts {
            let _ = extra.insert(format!("part_{name}"), serde_json::to_value(timing)?);
        }
        Ok(Observation {
            representation,
            output_bytes,
            values_rows,
            series_rows,
            checks,
            extra,
        })
    }

    /// Record the merged chunks' pinned bytes, which a consumed merge does
    /// not retain, as a fixture measurement.
    /// Record the sort keys each table's merge keeps resident beside the
    /// block, and the table's own pinned bytes.
    ///
    /// The sink merges one table at a time and the keys of a table live
    /// until its merge iterator is dropped, so the largest table's keys are
    /// the peak this term adds to a flush; the sum is what the whole block
    /// would add if every table were merged at once.
    fn record_merge_keys(&mut self, block: &Block) -> Result<()> {
        let mut tables = Vec::new();
        let mut total_keys = 0usize;
        let mut total_pinned = 0usize;
        let mut peak_keys = 0usize;
        let mut peak_table_pinned = 0usize;
        let mut seen = CountedAllocations::default();
        for table in block.tables().filter(|table| !table.is_empty()) {
            let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
            let pinned = runs
                .iter()
                .map(|run| record_batch_pinned_bytes(run, &mut seen))
                .sum::<usize>();
            let run_count = runs.len();
            let merge = merge_runs(runs, table.spec(), self.cfg.lake.sorting.merge_chunk_bytes)?;
            let keys = merge.resident_key_bytes();
            total_keys += keys;
            total_pinned += pinned;
            if keys > peak_keys {
                peak_keys = keys;
                peak_table_pinned = pinned;
            }
            tables.push(serde_json::json!({
                "dataset": table.dataset().name(),
                "rows": table.rows(),
                "runs": run_count,
                "pinned_bytes": pinned,
                "resident_key_bytes": keys,
            }));
        }
        let ratio = |keys: usize, pinned: usize| {
            if pinned == 0 {
                serde_json::Value::Null
            } else {
                (keys as f64 / pinned as f64).into()
            }
        };
        let summary = serde_json::json!({
            "tables": tables,
            "block_pinned_bytes": total_pinned,
            "resident_key_bytes_total": total_keys,
            "resident_key_bytes_peak_table": peak_keys,
            "peak_table_pinned_bytes": peak_table_pinned,
            "total_ratio": ratio(total_keys, total_pinned),
            "peak_over_block_ratio": ratio(peak_keys, total_pinned),
        });
        let _ = self.fixture_extra.insert("merge_keys".into(), summary);
        Ok(())
    }

    /// Record, per request, the values bytes extraction pins against the
    /// bytes the rows actually occupy.
    ///
    /// A values builder starts with room for 1024 rows and the extracted
    /// batch keeps that capacity, so a request of a few rows pins far more
    /// than its rows. Both are Arrow buffer sizes: pinned is the deduplicated
    /// capacity the block is charged, logical the used length of the same
    /// buffers.
    fn record_values_capacity(&mut self) -> Result<()> {
        let extracted = convert_extract(self.wire(), &self.cfg.lake)?;
        let mut requests = Vec::with_capacity(extracted.len());
        let mut pinned_total = 0usize;
        let mut logical_total = 0usize;
        let mut rows_total = 0usize;
        for request in &extracted {
            let mut logical = 0usize;
            let mut rows = 0usize;
            let mut batches = 0usize;
            for (_, dataset_batches) in &request.values {
                for batch in dataset_batches {
                    logical += record_batch_logical_bytes(batch)?;
                    rows += batch.num_rows();
                    batches += 1;
                }
            }
            pinned_total += request.pinned_bytes;
            logical_total += logical;
            rows_total += rows;
            requests.push((request.pinned_bytes, logical, rows, batches));
        }
        let mut overheads: Vec<usize> = requests
            .iter()
            .map(|(pinned, logical, _, _)| pinned.saturating_sub(*logical))
            .collect();
        overheads.sort_unstable();
        let median = overheads.get(overheads.len() / 2).copied().unwrap_or(0);
        let largest = overheads.last().copied().unwrap_or(0);
        let summary = serde_json::json!({
            "requests": requests.len(),
            "values_rows": rows_total,
            "values_batches": requests.iter().map(|entry| entry.3).sum::<usize>(),
            "values_pinned_bytes": pinned_total,
            "values_logical_bytes": logical_total,
            "capacity_overhead_bytes": pinned_total.saturating_sub(logical_total),
            "capacity_overhead_bytes_per_request_median": median,
            "capacity_overhead_bytes_per_request_max": largest,
            "pinned_over_logical_ratio": if logical_total == 0 {
                serde_json::Value::Null
            } else {
                (pinned_total as f64 / logical_total as f64).into()
            },
        });
        let _ = self.fixture_extra.insert("values_capacity".into(), summary);
        Ok(())
    }

    fn record_merged_bytes(&mut self, block: &Block) -> Result<()> {
        let mut seen = CountedAllocations::default();
        let mut bytes = 0usize;
        let mut largest = 0usize;
        for (_, chunks) in merged_tables(block, &self.cfg.lake)? {
            for chunk in &chunks {
                let size = record_batch_pinned_bytes(chunk, &mut seen);
                largest = largest.max(size);
                bytes += size;
            }
        }
        let _ = self
            .fixture_extra
            .insert("merged_pinned_bytes".into(), bytes.into());
        let _ = self
            .fixture_extra
            .insert("largest_chunk_pinned_bytes".into(), largest.into());
        Ok(())
    }
}

/// Values and series rows of a set of Parquet files, read back independently.
fn parquet_rows(files: impl IntoIterator<Item = (Dataset, Bytes)>) -> Result<(usize, usize)> {
    let mut values = 0usize;
    let mut series = 0usize;
    for (dataset, data) in files {
        let rows = ParquetRecordBatchReaderBuilder::try_new(data)?
            .metadata()
            .file_metadata()
            .num_rows() as usize;
        if dataset.is_series() {
            series += rows;
        } else {
            values += rows;
        }
    }
    Ok((values, series))
}

/// Rendered rows of a batch, one string per row over every column.
fn row_strings(batch: &RecordBatch) -> Result<Vec<String>> {
    let options = FormatOptions::default();
    let formatters = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut text = String::new();
        for formatter in &formatters {
            text.push_str(&formatter.value(row).to_string());
            text.push('\u{1f}');
        }
        rows.push(text);
    }
    Ok(rows)
}

/// Verify a full merge of every table once, outside every timer: each
/// chunk and each chunk boundary is in order, and the output is exactly
/// the input's row multiset.
fn verify_merge(block: &Block, cfg: &LakeConfig) -> Result<Vec<Check>> {
    let mut checks = Vec::new();
    for table in block.tables().filter(|table| !table.is_empty()) {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let mut input: BTreeMap<String, usize> = BTreeMap::new();
        for run in &runs {
            for row in row_strings(run)? {
                *input.entry(row).or_default() += 1;
            }
        }
        let spec: SortSpec = table.spec().clone();
        let mut ordered = true;
        let mut output: BTreeMap<String, usize> = BTreeMap::new();
        let mut previous: Option<RecordBatch> = None;
        for chunk in merge_runs(runs, &spec, cfg.sorting.merge_chunk_bytes)? {
            let chunk = chunk?;
            ordered &= is_sorted(&chunk, &spec)?;
            if let Some(last) = previous.take() {
                let boundary = arrow::compute::concat_batches(
                    &chunk.schema(),
                    &[last.slice(last.num_rows() - 1, 1), chunk.slice(0, 1)],
                )?;
                ordered &= is_sorted(&boundary, &spec)?;
            }
            for row in row_strings(&chunk)? {
                *output.entry(row).or_default() += 1;
            }
            previous = Some(chunk);
        }
        let name = table.dataset().name();
        checks.push(Check::new(
            &format!("merge_order_{name}"),
            ordered,
            "every chunk and chunk boundary follows the sort spec",
        ));
        checks.push(Check::new(
            &format!("merge_multiset_{name}"),
            input == output,
            format!(
                "{} distinct input rows, {} distinct output rows",
                input.len(),
                output.len()
            ),
        ));
    }
    Ok(checks)
}
