// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! How long a flush holds its thread, run by `measurement --flush-stall`.
//!
//! The exporter writes a sealed block from a task on the same
//! single-threaded runtime as its node loop, so every stretch the write runs
//! without returning to the runtime delays admission, backpressure, the
//! delivery of acks and nacks, telemetry collection, cancellation and the
//! shutdown deadline on that core. This probe measures that directly:
//!
//! 1. One block as large as the default configuration admits: every request
//!    of the input, converted, extracted and admitted through the production
//!    path until the block refuses one as full or runs out of requests, then
//!    sealed.
//! 2. `Sink::write_block` of that block into a local file store in the
//!    configuration's scratch directory. That store does its file I/O on
//!    the runtime's blocking pool, off the measured thread, and never waits
//!    for a network, so every stretch measured is CPU the sink itself spent
//!    without yielding. (An in-memory store would not do: completing a
//!    multipart upload there copies the whole object into one buffer on the
//!    calling thread, a stretch no real store has.)
//! 3. Beside it, on the same current-thread runtime, a ticker task that
//!    stands in for the node loop: it records the instant each time the
//!    runtime schedules it, then yields. The longest gap between two ticks is
//!    the longest the node loop could not run. The flush future's own polls
//!    are timed too, which isolates the sink from the upload tasks it spawns.
//! 4. Cancellation reaction: the same write again, with a signal instant at
//!    evenly spaced fractions of the uncancelled write. The ticker cancels
//!    the token at its first tick at or after the instant, as the node loop
//!    would at its first turn after a shutdown deadline passed; the latency
//!    is from the signal instant to the write returning cancelled.
//!
//! With `--dump DIR`, the files of the first uncancelled write are written
//! there with a fixed file identity, so two builds can be compared byte for
//! byte on the same input.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use futures::StreamExt as _;
use object_store::local::LocalFileSystem;
use object_store::{ObjectStore, ObjectStoreExt as _};
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::error::{Error as LakeError, RefuseReason};
use otel_arrow_dfe_series_lake::extract::extract;
use otel_arrow_dfe_series_lake::schema::dataset_schema;
use otel_arrow_dfe_series_lake::sink::{FileNaming, Sink};
use otel_arrow_dfe_series_lake::sort::merge_runs;
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::stages::{BenchConfig, Input, Result, Signal, hex_digest, writer_properties, zstd};

/// Accounted bytes of one request's stand-in acknowledgement token, as in
/// the stage benches.
const TOKEN_BYTES: usize = 128;

/// Gap thresholds the distribution counts, in milliseconds.
const GAP_THRESHOLDS_MS: [u64; 5] = [1, 5, 10, 50, 100];

/// The block the probe writes.
#[derive(Debug, Serialize)]
pub struct BlockShape {
    /// Requests admitted.
    pub requests: usize,
    /// Requests the input still had when the block refused one as full.
    pub refused_as_full: bool,
    /// Bytes the block charged when sealed.
    pub bytes: usize,
    /// Values rows over every table.
    pub values_rows: usize,
    /// Series rows over every table.
    pub series_rows: usize,
}

/// One uncancelled write.
#[derive(Debug, Serialize)]
pub struct WriteRun {
    /// Wall time of the whole write.
    pub wall_ns: u128,
    /// Process CPU time of the whole write.
    pub cpu_ns: u128,
    /// Times the ticker ran.
    pub ticks: usize,
    /// The longest gap between two ticks.
    pub max_gap_ns: u128,
    /// The 99th percentile gap.
    pub p99_gap_ns: u128,
    /// The median gap.
    pub p50_gap_ns: u128,
    /// Gaps of at least each threshold, keyed `ge_<ms>ms`.
    pub gaps_at_least: std::collections::BTreeMap<String, usize>,
    /// Polls of the write future.
    pub polls: usize,
    /// The longest single poll of the write future.
    pub max_poll_ns: u128,
    /// The largest flush workspace the sink reported at any tick, when it
    /// reports one.
    pub max_workspace_bytes: Option<usize>,
}

/// One cancelled write.
#[derive(Debug, Serialize)]
pub struct CancelRun {
    /// Where in the uncancelled write the signal fell.
    pub fraction: f64,
    /// Signal instant, from the start of the write.
    pub signal_ns: u128,
    /// When the ticker got to cancel the token, after the signal: how long
    /// the node loop would have waited to see it.
    pub ticker_delay_ns: u128,
    /// From the signal to the first poll of the write after the token was
    /// cancelled, where the sink checks the token: when the flush observes
    /// the signal.
    pub observe_latency_ns: u128,
    /// From the signal to the write returning, its abort included.
    pub latency_ns: u128,
    /// Whether the write returned cancelled; false when it finished first.
    pub cancelled: bool,
}

/// One written file.
#[derive(Debug, Serialize)]
pub struct DumpedFile {
    /// Object path.
    pub path: String,
    /// Size.
    pub bytes: usize,
    /// SHA-256.
    pub sha256: String,
}

/// The synchronous steps of writing one table, each timed on its own.
///
/// A diagnostic of where a stretch comes from: the merge is built and
/// drained and every chunk encoded with the sink's writer properties, one
/// step at a time and outside any runtime.
#[derive(Debug, Serialize)]
pub struct TablePhases {
    /// Dataset directory name.
    pub dataset: &'static str,
    /// Rows.
    pub rows: usize,
    /// Runs merged.
    pub runs: usize,
    /// `merge_runs`: encoding every run's sort keys and seeding the heap.
    pub build_ns: u128,
    /// Chunks produced.
    pub chunks: usize,
    /// The longest `MergeIter::next`: heap pops plus the interleave.
    pub max_next_ns: u128,
    /// The longest `ArrowWriter::write` of one chunk.
    pub max_write_ns: u128,
    /// The longest row group flush.
    pub max_flush_ns: u128,
    /// Closing the file: the last row group and the footer.
    pub close_ns: u128,
}

/// The whole result.
#[derive(Debug, Serialize)]
pub struct Report {
    /// Result schema.
    pub schema: &'static str,
    /// Harness label of the input.
    pub workload_config_id: String,
    /// Signal of the input.
    pub signal: Signal,
    /// The block written.
    pub block: BlockShape,
    /// Uncancelled writes.
    pub writes: Vec<WriteRun>,
    /// Cancelled writes.
    pub cancels: Vec<CancelRun>,
    /// The longest gap over every uncancelled write.
    pub max_gap_ns: u128,
    /// The longest time from a signal to the flush observing it.
    pub max_observe_latency_ns: u128,
    /// The longest cancellation latency, abort included, over every
    /// cancelled write.
    pub max_cancel_latency_ns: u128,
    /// Files of the first uncancelled write.
    pub files: Vec<DumpedFile>,
    /// Where the synchronous work of each table goes.
    pub phases: Vec<TablePhases>,
}

/// The probe's command line.
pub struct Args {
    /// Input file.
    pub input: PathBuf,
    /// Bench configuration.
    pub config: PathBuf,
    /// Result file.
    pub output: PathBuf,
    /// Uncancelled writes.
    pub writes: usize,
    /// Cancelled writes.
    pub cancels: usize,
    /// Where to write the files of the first uncancelled write.
    pub dump: Option<PathBuf>,
}

/// Build the largest block the input and the configuration allow.
fn build_block(input: &Input, cfg: &BenchConfig) -> Result<(Block<()>, BlockShape)> {
    let mut block: Block<()> = Block::new(cfg.window_start_secs, 1, &cfg.lake);
    let mut cache = SeriesCache::new(cfg.cache_entries);
    let mut requests = 0usize;
    let mut refused_as_full = false;
    for bytes in &input.requests {
        let wire = match input.sidecar.signal {
            Signal::Logs => OtlpProtoBytes::ExportLogsRequest(bytes.clone()),
            Signal::Metrics => OtlpProtoBytes::ExportMetricsRequest(bytes.clone()),
        };
        let payload: OtapPayload = wire.into();
        let mut records: OtapArrowRecords = payload.try_into_with_default()?;
        let extracted = extract(&mut records, &cfg.lake)?;
        drop(records);
        match block.reserve(&extracted, &mut cache, TOKEN_BYTES, &cfg.lake) {
            Ok(reservation) => {
                block.admit(extracted, reservation, ())?;
                requests += 1;
            }
            Err(LakeError::Refused(RefuseReason::BlockFull)) => {
                refused_as_full = true;
                break;
            }
            Err(other) => return Err(other.into()),
        }
    }
    block.seal(cfg.seal_at_us)?;
    let (mut values_rows, mut series_rows) = (0usize, 0usize);
    for table in block.tables() {
        if table.dataset().is_series() {
            series_rows += table.rows();
        } else {
            values_rows += table.rows();
        }
    }
    let shape = BlockShape {
        requests,
        refused_as_full,
        bytes: block.bytes,
        values_rows,
        series_rows,
    };
    Ok((block, shape))
}

/// A future whose every poll is timed.
struct PollTimer<F> {
    inner: Pin<Box<F>>,
    polls: Rc<Cell<usize>>,
    longest: Rc<Cell<Duration>>,
    /// When the ticker cancelled the token, and the start of the first poll
    /// after that.
    cancelled_at: Rc<Cell<Option<Instant>>>,
    observed_at: Rc<Cell<Option<Instant>>>,
}

impl<F: Future> Future for PollTimer<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let started = Instant::now();
        if self.cancelled_at.get().is_some() && self.observed_at.get().is_none() {
            self.observed_at.set(Some(started));
        }
        let out = self.inner.as_mut().poll(cx);
        let took = started.elapsed();
        self.polls.set(self.polls.get() + 1);
        if took > self.longest.get() {
            self.longest.set(took);
        }
        out
    }
}

/// What one write observed.
struct Observed {
    wall: Duration,
    cpu: Duration,
    gaps: Vec<Duration>,
    polls: usize,
    max_poll: Duration,
    max_workspace: Option<usize>,
    cancelled_at: Option<Instant>,
    observed_at: Option<Instant>,
    started: Instant,
    returned: Instant,
    result: std::result::Result<(), String>,
}

/// The sink's live flush workspace: merge chunk, encoder buffers and the
/// upload bytes the store has not acknowledged.
fn workspace_of(sink: &Sink) -> Option<usize> {
    Some(sink.flush_workspace_bytes())
}

/// Write `block` once, with the ticker beside it.
fn write_once(
    runtime: &tokio::runtime::Runtime,
    block: &Block<()>,
    cfg: &BenchConfig,
    store: Arc<dyn ObjectStore>,
    signal_after: Option<Duration>,
) -> Observed {
    let naming = FileNaming {
        writer_id: cfg.lake.writer_id.clone(),
        boot_id: "flushstall".into(),
    };
    let sink = Rc::new(Sink::new(store, cfg.lake.clone(), naming));
    let local = tokio::task::LocalSet::new();
    local.block_on(runtime, async {
        let cancel = CancellationToken::new();
        let done = Rc::new(Cell::new(false));
        let gaps = Rc::new(RefCell::new(Vec::with_capacity(1 << 16)));
        let cancelled_at: Rc<Cell<Option<Instant>>> = Rc::new(Cell::new(None));
        let max_workspace: Rc<Cell<Option<usize>>> = Rc::new(Cell::new(None));
        let started = Instant::now();
        let signal = signal_after.map(|after| started + after);
        let ticker = {
            let done = Rc::clone(&done);
            let gaps = Rc::clone(&gaps);
            let cancel = cancel.clone();
            let cancelled_at = Rc::clone(&cancelled_at);
            let max_workspace = Rc::clone(&max_workspace);
            let sink = Rc::clone(&sink);
            tokio::task::spawn_local(async move {
                let mut last = Instant::now();
                loop {
                    let now = Instant::now();
                    gaps.borrow_mut().push(now - last);
                    last = now;
                    if let Some(bytes) = workspace_of(&sink) {
                        max_workspace.set(Some(max_workspace.get().unwrap_or(0).max(bytes)));
                    }
                    if let Some(at) = signal
                        && now >= at
                        && !cancel.is_cancelled()
                    {
                        cancel.cancel();
                        cancelled_at.set(Some(now));
                    }
                    if done.get() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
        };
        let observed_at: Rc<Cell<Option<Instant>>> = Rc::new(Cell::new(None));
        let polls = Rc::new(Cell::new(0));
        let longest = Rc::new(Cell::new(Duration::ZERO));
        let cpu = cpu_time::ProcessTime::now();
        let write = PollTimer {
            inner: Box::pin(sink.write_block(block, &cancel)),
            polls: Rc::clone(&polls),
            longest: Rc::clone(&longest),
            cancelled_at: Rc::clone(&cancelled_at),
            observed_at: Rc::clone(&observed_at),
        };
        let result = write.await;
        let returned = Instant::now();
        let cpu = cpu.elapsed();
        done.set(true);
        let _ = ticker.await;
        let gaps = std::mem::take(&mut *gaps.borrow_mut());
        Observed {
            wall: returned - started,
            cpu,
            gaps,
            polls: polls.get(),
            max_poll: longest.get(),
            max_workspace: max_workspace.get(),
            cancelled_at: cancelled_at.get(),
            observed_at: observed_at.get(),
            started,
            returned,
            result: result.map(|_report| ()).map_err(|e| e.to_string()),
        }
    })
}

/// The `q` quantile of sorted durations.
fn quantile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let at = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[at]
}

/// Every object in `store`, read back.
fn objects(
    runtime: &tokio::runtime::Runtime,
    store: &Arc<dyn ObjectStore>,
) -> Result<Vec<(String, bytes::Bytes)>> {
    runtime.block_on(async {
        let mut listed: Vec<_> = store.list(None).collect::<Vec<_>>().await;
        let mut out = Vec::new();
        for meta in listed.drain(..) {
            let meta = meta?;
            let data = store.get(&meta.location).await?.bytes().await?;
            out.push((meta.location.to_string(), data));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    })
}

/// Time the synchronous steps of writing every table of `block`.
fn phases(block: &Block<()>, cfg: &BenchConfig) -> Result<Vec<TablePhases>> {
    let mut out = Vec::new();
    for table in block.tables().filter(|table| !table.is_empty()) {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let run_count = runs.len();
        let schema = dataset_schema(table.dataset(), &cfg.lake);
        let started = Instant::now();
        let mut merged = merge_runs(runs, table.spec(), cfg.lake.sorting.merge_chunk_bytes)?;
        let build = started.elapsed();
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(writer_properties(zstd())))?;
        let (mut chunks, mut rows) = (0usize, 0usize);
        let (mut max_next, mut max_write, mut max_flush) =
            (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        loop {
            let started = Instant::now();
            let Some(chunk) = merged.next() else { break };
            let chunk = chunk?;
            max_next = max_next.max(started.elapsed());
            let started = Instant::now();
            writer.write(&chunk)?;
            max_write = max_write.max(started.elapsed());
            chunks += 1;
            rows += chunk.num_rows();
            if writer.memory_size() >= cfg.lake.parquet.writer_limit_bytes
                || writer.in_progress_size() >= cfg.lake.parquet.row_group_bytes
            {
                let started = Instant::now();
                writer.flush()?;
                max_flush = max_flush.max(started.elapsed());
            }
        }
        let started = Instant::now();
        let _ = writer.close()?;
        out.push(TablePhases {
            dataset: table.dataset().name(),
            rows,
            runs: run_count,
            build_ns: build.as_nanos(),
            chunks,
            max_next_ns: max_next.as_nanos(),
            max_write_ns: max_write.as_nanos(),
            max_flush_ns: max_flush.as_nanos(),
            close_ns: started.elapsed().as_nanos(),
        });
    }
    Ok(out)
}

/// Run the probe.
///
/// # Errors
/// Refuses an unreadable input or configuration, and a write that fails
/// for any reason other than the cancellation the probe asked for.
pub fn run(args: &Args, input: &Input, cfg: &BenchConfig) -> Result<Report> {
    let (block, shape) = build_block(input, cfg)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut writes = Vec::new();
    let mut files = Vec::new();
    let scratch = cfg
        .scratch_dir
        .join(format!("flush-stall-{}", std::process::id()));
    let fresh_store = |name: String| -> Result<(Arc<dyn ObjectStore>, PathBuf)> {
        let dir = scratch.join(name);
        std::fs::create_dir_all(&dir)?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir)?);
        Ok((store, dir))
    };
    for index in 0..args.writes.max(1) {
        let (store, dir) = fresh_store(format!("write-{index}"))?;
        let observed = write_once(&runtime, &block, cfg, Arc::clone(&store), None);
        observed
            .result
            .clone()
            .map_err(|e| format!("uncancelled write failed: {e}"))?;
        if index == 0 {
            // Every file name carries the fixed identity, so a listing is the
            // whole write.
            for (path, data) in objects(&runtime, &store)? {
                if let Some(dir) = &args.dump {
                    let target = dir.join(&path);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&target, &data)?;
                }
                files.push(DumpedFile {
                    sha256: hex_digest(&data),
                    bytes: data.len(),
                    path,
                });
            }
        }
        std::fs::remove_dir_all(&dir)?;
        let mut sorted = observed.gaps.clone();
        sorted.sort();
        let gaps_at_least = GAP_THRESHOLDS_MS
            .iter()
            .map(|ms| {
                let bound = Duration::from_millis(*ms);
                (
                    format!("ge_{ms}ms"),
                    observed.gaps.iter().filter(|gap| **gap >= bound).count(),
                )
            })
            .collect();
        writes.push(WriteRun {
            wall_ns: observed.wall.as_nanos(),
            cpu_ns: observed.cpu.as_nanos(),
            ticks: observed.gaps.len(),
            max_gap_ns: sorted.last().copied().unwrap_or_default().as_nanos(),
            p99_gap_ns: quantile(&sorted, 0.99).as_nanos(),
            p50_gap_ns: quantile(&sorted, 0.5).as_nanos(),
            gaps_at_least,
            polls: observed.polls,
            max_poll_ns: observed.max_poll.as_nanos(),
            max_workspace_bytes: observed.max_workspace,
        });
    }
    let mut walls: Vec<u128> = writes.iter().map(|w| w.wall_ns).collect();
    walls.sort_unstable();
    let median_wall = Duration::from_nanos(walls[walls.len() / 2] as u64);
    let mut cancels = Vec::new();
    for index in 0..args.cancels {
        let fraction = (index as f64 + 0.5) / args.cancels as f64;
        let signal_after = median_wall.mul_f64(fraction);
        let (store, dir) = fresh_store(format!("cancel-{index}"))?;
        let observed = write_once(&runtime, &block, cfg, store, Some(signal_after));
        std::fs::remove_dir_all(&dir)?;
        let signal = observed.started + signal_after;
        let cancelled = observed.result.is_err();
        if let Err(error) = &observed.result
            && !error.contains("cancelled")
        {
            return Err(format!("cancelled write failed otherwise: {error}").into());
        }
        cancels.push(CancelRun {
            fraction,
            signal_ns: signal_after.as_nanos(),
            ticker_delay_ns: observed
                .cancelled_at
                .map_or(0, |at| at.saturating_duration_since(signal).as_nanos()),
            observe_latency_ns: observed
                .observed_at
                .map_or(0, |at| at.saturating_duration_since(signal).as_nanos()),
            latency_ns: observed
                .returned
                .saturating_duration_since(signal)
                .as_nanos(),
            cancelled,
        });
    }
    let _ = std::fs::remove_dir_all(&scratch);
    let max_gap_ns = writes.iter().map(|w| w.max_gap_ns).max().unwrap_or(0);
    let max_observe_latency_ns = cancels
        .iter()
        .filter(|c| c.cancelled)
        .map(|c| c.observe_latency_ns)
        .max()
        .unwrap_or(0);
    let max_cancel_latency_ns = cancels
        .iter()
        .filter(|c| c.cancelled)
        .map(|c| c.latency_ns)
        .max()
        .unwrap_or(0);
    Ok(Report {
        schema: "series-flush-stall/1",
        workload_config_id: cfg.workload_config_id.clone(),
        signal: input.sidecar.signal,
        block: shape,
        writes,
        cancels,
        max_gap_ns,
        max_observe_latency_ns,
        max_cancel_latency_ns,
        files,
        phases: phases(&block, cfg)?,
    })
}

/// Read the inputs, run the probe and write its result.
///
/// # Errors
/// As [`run`], plus an unwritable result file.
pub fn main(args: &Args) -> Result<()> {
    let cfg = BenchConfig::read(&args.config)?;
    let input = super::stages::read_input(&args.input)?;
    let report = run(args, &input, &cfg)?;
    let file = std::fs::File::create(&args.output)?;
    serde_json::to_writer_pretty(std::io::BufWriter::new(file), &report)?;
    Ok(())
}
