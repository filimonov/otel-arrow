// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Complementary stage measurements: wall time, CPU time, resident memory
//! and heap allocation of one stage per process.
//!
//! ```text
//! measurement --stage NAME --input PATH --config PATH --output PATH
//!             --iterations N --profile timing|heap --compression zstd|none
//!             [--handshake]
//! measurement --self-test
//! measurement --describe
//! measurement --series-cost --signal logs|metrics --series N
//!             [--denormalize] --output PATH
//! measurement --flush-stall --input PATH --config PATH --output PATH
//!             [--writes N] [--cancels N] [--dump DIR]
//! ```
//!
//! Built only with the `bench-harness` feature. Run with no measurement
//! argument at all, as `cargo bench` runs every bench, it prints a skip
//! message and exits 0.
//!
//! The input is deterministic length-prefixed OTLP requests of one signal
//! with a sidecar JSON; the configuration is a JSON file holding the lake
//! configuration and the object store. The result is written as JSON to
//! `--output`; nothing is printed except the handshake markers.
//!
//! A timing process runs at least `--iterations` samples and one second of
//! measured work, capped by a sixty-second deadline after which the result
//! is marked incomplete. The timing build runs on the engine's jemalloc
//! configuration; a heap process is built with the `bench-heap` feature,
//! which installs DHAT's global allocator instead (see `allocator.rs`).
//!
//! With `--handshake` the process writes `SERIES_STAGE_READY` once its
//! fixtures exist and waits for one line on stdin before it measures, then
//! writes `SERIES_STAGE_DONE` after its output file and waits for stdin to
//! close, so that the harness can snapshot the live process at both edges.

#[path = "measurement/allocator.rs"]
mod allocator;
#[path = "measurement/flush_stall.rs"]
mod flush_stall;
#[path = "measurement/series_cost.rs"]
mod series_cost;
#[path = "measurement/stages.rs"]
mod stages;
#[path = "measurement/tests.rs"]
mod tests;

use std::collections::BTreeMap;
use std::io::{BufRead as _, Read as _, Write as _};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use parquet::basic::Compression;
use serde::Serialize;

use stages::{
    BenchConfig, Clock, Observation, Result, Stage, StageName, Timing, timed, timed_process,
};

/// Least measured work a timing process accumulates.
const MINIMUM_MEASURED: Duration = Duration::from_secs(1);

/// Longest a timing process keeps sampling before it gives up.
const DEADLINE: Duration = Duration::from_secs(60);

/// Most samples a result file keeps.
///
/// A stage whose operation costs nanoseconds reaches the one second of
/// measured work only after millions of samples, which no result file can
/// carry. Once the kept samples reach this many, every second one is
/// dropped and the stride doubles, so the file holds a uniform thinning of
/// the whole loop. The reported sample count is always the true one.
const MAX_STORED_SAMPLES: usize = 2000;

/// Which kind of measurement a process makes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Profile {
    /// Wall and CPU time on the engine's allocator.
    Timing,
    /// DHAT heap statistics; never a source of throughput.
    Heap,
}

/// The parsed command line.
struct Args {
    stage: StageName,
    input: PathBuf,
    config: PathBuf,
    output: PathBuf,
    iterations: usize,
    profile: Profile,
    compression: Compression,
    compression_name: String,
    handshake: bool,
}

/// One point of the per-series cost measurement.
struct SeriesCostArgs {
    signal: stages::Signal,
    series: usize,
    denormalize: bool,
    output: PathBuf,
}

/// What the command line asked for.
enum Command {
    SelfTest,
    Describe,
    Measure(Box<Args>),
    SeriesCost(SeriesCostArgs),
    /// How long a flush of the largest block holds its thread.
    FlushStall(flush_stall::Args),
    /// Run with no measurement arguments at all, as a workspace-wide
    /// `cargo bench` runs every bench: there is nothing to measure.
    Skip,
}

/// What this executable is, for a harness that must not build anything.
///
/// The harness locates prebuilt executables and asks each one what it is,
/// since asking cargo would build the target. `bench_heap` says whether
/// DHAT's allocator is installed, `allocator` names the installed one
/// ([`allocator::name`]), and `debug_assertions` marks a debug build.
#[derive(Debug, Serialize)]
struct Description {
    bench: &'static str,
    bench_heap: bool,
    allocator: &'static str,
    debug_assertions: bool,
    handshake: bool,
    stages: Vec<&'static str>,
}

fn describe() -> Description {
    Description {
        bench: "measurement",
        bench_heap: cfg!(feature = "bench-heap"),
        allocator: allocator::name(),
        debug_assertions: cfg!(debug_assertions),
        handshake: true,
        stages: StageName::ALL.iter().map(|stage| stage.as_str()).collect(),
    }
}

/// Take one required value out of the parsed flags.
fn required(values: &mut BTreeMap<String, String>, name: &str) -> Result<String> {
    Ok(values
        .remove(name)
        .ok_or_else(|| format!("{name} is required"))?)
}

fn parse_args(arguments: impl IntoIterator<Item = String>) -> Result<Command> {
    let mut values: BTreeMap<String, String> = BTreeMap::new();
    let mut handshake = false;
    let mut series_cost = false;
    let mut denormalize = false;
    let mut flush_stall = false;
    let mut arguments = arguments.into_iter();
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--self-test" => return Ok(Command::SelfTest),
            "--describe" => return Ok(Command::Describe),
            // `cargo bench` passes `--bench` to every harness-less target.
            "--bench" => {}
            "--handshake" => handshake = true,
            "--series-cost" => series_cost = true,
            "--denormalize" => denormalize = true,
            "--flush-stall" => flush_stall = true,
            "--stage" | "--input" | "--config" | "--output" | "--iterations" | "--profile"
            | "--compression" | "--signal" | "--series" | "--writes" | "--cancels" | "--dump" => {
                let value = arguments
                    .next()
                    .ok_or_else(|| format!("{flag} needs a value"))?;
                let _ = values.insert(flag, value);
            }
            other => return Err(format!("unknown argument {other}").into()),
        }
    }
    if values.is_empty() && !handshake {
        return Ok(Command::Skip);
    }
    if flush_stall {
        let writes = values.remove("--writes").map_or(Ok(3), |v| v.parse())?;
        let cancels = values.remove("--cancels").map_or(Ok(20), |v| v.parse())?;
        let dump = values.remove("--dump").map(PathBuf::from);
        return Ok(Command::FlushStall(flush_stall::Args {
            input: PathBuf::from(required(&mut values, "--input")?),
            config: PathBuf::from(required(&mut values, "--config")?),
            output: PathBuf::from(required(&mut values, "--output")?),
            writes,
            cancels,
            dump,
        }));
    }
    let mut take = |name: &str| {
        values
            .remove(name)
            .ok_or_else(|| format!("{name} is required"))
    };
    if series_cost {
        let signal = match take("--signal")?.as_str() {
            "logs" => stages::Signal::Logs,
            "metrics" => stages::Signal::Metrics,
            other => return Err(format!("--signal is logs or metrics, not {other}").into()),
        };
        return Ok(Command::SeriesCost(SeriesCostArgs {
            signal,
            series: take("--series")?.parse()?,
            denormalize,
            output: PathBuf::from(take("--output")?),
        }));
    }
    let stage = StageName::parse(&take("--stage")?)?;
    let input = PathBuf::from(take("--input")?);
    let config = PathBuf::from(take("--config")?);
    let output = PathBuf::from(take("--output")?);
    let iterations: usize = take("--iterations")?.parse()?;
    if iterations == 0 {
        return Err("--iterations must be positive".into());
    }
    let profile = match take("--profile")?.as_str() {
        "timing" => Profile::Timing,
        "heap" => Profile::Heap,
        other => return Err(format!("--profile is timing or heap, not {other}").into()),
    };
    let compression_name = take("--compression")?;
    let compression = match compression_name.as_str() {
        "zstd" => stages::zstd(),
        "none" => Compression::UNCOMPRESSED,
        other => return Err(format!("--compression is zstd or none, not {other}").into()),
    };
    Ok(Command::Measure(Box::new(Args {
        stage,
        input,
        config,
        output,
        iterations,
        profile,
        compression,
        compression_name,
        handshake,
    })))
}

/// One field of `/proc/self/status`, in bytes.
fn status_bytes(field: &str) -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            let kib: u64 = rest
                .trim_start_matches(':')
                .split_whitespace()
                .next()
                .ok_or("empty status field")?
                .parse()?;
            return Ok(kib * 1024);
        }
    }
    Err(format!("/proc/self/status has no {field}").into())
}

/// Reset the resident high-water mark to the current resident size.
fn reset_peak_rss() -> Result<()> {
    std::fs::write("/proc/self/clear_refs", "5")?;
    Ok(())
}

/// Resident memory over the measured part of the process.
#[derive(Debug, Clone, Copy, Default, Serialize)]
struct Resident {
    /// Resident bytes when the high-water mark was reset.
    start_bytes: u64,
    /// High-water mark at the end of the measured part.
    peak_bytes: u64,
    /// Growth of the high-water mark over the measured part.
    growth_bytes: u64,
}

/// What a timing process measured.
#[derive(Debug, Default, Serialize)]
struct Samples {
    /// A uniform thinning of the loop's samples, at most
    /// `MAX_STORED_SAMPLES` of them.
    samples: Vec<Timing>,
    /// How many samples one kept sample stands for.
    sample_stride: usize,
    /// How many samples the loop actually took.
    sample_count: usize,
    parts: BTreeMap<String, Vec<Timing>>,
    prepare_ns_total: u128,
    measured_wall_ns_total: u128,
    complete: bool,
    incomplete_reason: Option<String>,
}

/// Sample `run` until `minimum` samples and `minimum_measured` of measured
/// work, or until the deadline.
///
/// `prepare` builds each sample's input and `observe` inspects its output;
/// neither is inside the timer, which wraps exactly one `run`. The timer is
/// the thread clock for a synchronous stage and the process clock for one
/// that drives the runtime.
fn sample_loop<P, R>(
    clock: Clock,
    minimum: usize,
    minimum_measured: Duration,
    mut prepare: impl FnMut() -> Result<P>,
    mut run: impl FnMut(P) -> Result<R>,
    mut observe: impl FnMut(&R) -> Result<Vec<(&'static str, Timing)>>,
) -> Result<Samples> {
    let started = Instant::now();
    let mut result = Samples {
        sample_stride: 1,
        ..Samples::default()
    };
    loop {
        if result.sample_count >= minimum
            && result.measured_wall_ns_total >= minimum_measured.as_nanos()
        {
            result.complete = true;
            break;
        }
        if started.elapsed() >= DEADLINE {
            result.incomplete_reason = Some(format!(
                "{} samples and {} measured ns within the {}s deadline; {minimum} samples and {} ns required",
                result.sample_count,
                result.measured_wall_ns_total,
                DEADLINE.as_secs(),
                minimum_measured.as_nanos()
            ));
            break;
        }
        let preparing = Instant::now();
        let input = prepare()?;
        result.prepare_ns_total += preparing.elapsed().as_nanos();
        let (output, timing) = match clock {
            Clock::Thread => timed(|| run(input))?,
            Clock::Process => timed_process(|| run(input))?,
        };
        let output = std::hint::black_box(output);
        for (name, part) in observe(&output)? {
            result.parts.entry(name.to_string()).or_default().push(part);
        }
        drop(output);
        result.measured_wall_ns_total += timing.wall_ns;
        result.sample_count += 1;
        if result.sample_count.is_multiple_of(result.sample_stride) {
            result.samples.push(timing);
        }
        if result.samples.len() > MAX_STORED_SAMPLES {
            let mut thinned: Vec<Timing> = result.samples.iter().step_by(2).copied().collect();
            std::mem::swap(&mut result.samples, &mut thinned);
            result.sample_stride *= 2;
        }
    }
    Ok(result)
}

/// DHAT's view of the heap at one point.
#[derive(Debug, Clone, Copy, Serialize)]
struct Heap {
    total_blocks: u64,
    total_bytes: u64,
    curr_blocks: usize,
    curr_bytes: usize,
    max_blocks: usize,
    max_bytes: usize,
}

/// What a heap process measured.
#[derive(Debug, Serialize)]
struct HeapReport {
    /// When the profiler started, after the fixtures existed.
    before: Heap,
    /// After the first run, before its output was observed.
    after_run: Heap,
    /// After every run's output was dropped under the profiler.
    after_drop: Heap,
    /// Bytes allocated by the runs, observation excluded, per run.
    allocated_bytes_per_run: Vec<u64>,
    /// Heap allocated during the first run above the fixtures, at its peak.
    peak_workspace_bytes: u64,
}

/// The result file.
#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    stage: StageName,
    profile: Profile,
    clock: Clock,
    compression: String,
    workload_config_id: String,
    signal: stages::Signal,
    requests: usize,
    records: usize,
    input_bytes: usize,
    iterations: usize,
    sample_count: usize,
    fixture_retained_bytes: u64,
    prepared_input_bytes: u64,
    warmup: Option<Timing>,
    timing: Option<Samples>,
    heap: Option<HeapReport>,
    resident: Resident,
    /// Resident growth of one steady-state iteration, after the loop.
    resident_steady: Resident,
    observation: Option<Observation>,
    verification_passed: bool,
    failed_checks: Vec<String>,
}

fn handshake_ready(enabled: bool) -> Result<()> {
    if enabled {
        let mut stdout = std::io::stdout();
        stdout.write_all(b"SERIES_STAGE_READY\n")?;
        stdout.flush()?;
        let mut line = String::new();
        let _ = std::io::stdin().lock().read_line(&mut line)?;
    }
    Ok(())
}

fn handshake_done(enabled: bool) -> Result<()> {
    if enabled {
        let mut stdout = std::io::stdout();
        stdout.write_all(b"SERIES_STAGE_DONE\n")?;
        stdout.flush()?;
        let mut rest = Vec::new();
        let _ = std::io::stdin().lock().read_to_end(&mut rest)?;
    }
    Ok(())
}

fn measure_timing(stage: &Stage, args: &Args, report: &mut Report) -> Result<()> {
    // One untimed warm-up primes lazily built state, such as a store's
    // connection pool, and is reported but never sampled.
    let warm = stage.prepare()?;
    report.prepared_input_bytes = stage.input_bytes(&warm);
    let (output, warmup) = match stage.name().clock() {
        Clock::Thread => timed(|| stage.run(warm))?,
        Clock::Process => timed_process(|| stage.run(warm))?,
    };
    report.observation = Some(stage.observe(&output)?);
    drop(output);
    report.warmup = Some(warmup);
    reset_peak_rss()?;
    let start = status_bytes("VmRSS")?;
    let mut failed: Vec<String> = Vec::new();
    let samples = sample_loop(
        stage.name().clock(),
        args.iterations,
        MINIMUM_MEASURED,
        || stage.prepare(),
        |input| stage.run(input),
        |output| {
            let observation = stage.observe(output)?;
            for check in observation.checks.iter().filter(|check| !check.passed) {
                failed.push(format!("{}: {}", check.name, check.detail));
            }
            Ok(output.parts.clone())
        },
    )?;
    let peak = status_bytes("VmHWM")?;
    report.resident = Resident {
        start_bytes: start,
        peak_bytes: peak,
        growth_bytes: peak.saturating_sub(start),
    };
    // One more iteration with the mark reset again: the resident growth of
    // a single steady-state iteration is what one iteration's profiled
    // heap workspace can be reconciled with. The loop's own growth also
    // holds whatever the allocator retained over every earlier iteration,
    // which is recorded above, not attributed to the stage.
    reset_peak_rss()?;
    let steady_start = status_bytes("VmRSS")?;
    let input = stage.prepare()?;
    let output = std::hint::black_box(match stage.name().clock() {
        Clock::Thread => timed(|| stage.run(input))?.0,
        Clock::Process => timed_process(|| stage.run(input))?.0,
    });
    let _ = stage.observe(&output)?;
    drop(output);
    let steady_peak = status_bytes("VmHWM")?;
    report.resident_steady = Resident {
        start_bytes: steady_start,
        peak_bytes: steady_peak,
        growth_bytes: steady_peak.saturating_sub(steady_start),
    };
    report.sample_count = samples.sample_count;
    report.failed_checks.extend(failed);
    report.timing = Some(samples);
    Ok(())
}

#[cfg(feature = "bench-heap")]
fn heap_now() -> Heap {
    let stats = dhat::HeapStats::get();
    Heap {
        total_blocks: stats.total_blocks,
        total_bytes: stats.total_bytes,
        curr_blocks: stats.curr_blocks,
        curr_bytes: stats.curr_bytes,
        max_blocks: stats.max_blocks,
        max_bytes: stats.max_bytes,
    }
}

#[cfg(feature = "bench-heap")]
fn measure_heap(stage: &Stage, args: &Args, report: &mut Report) -> Result<()> {
    // Every run's input exists before the profiler starts, so the profile
    // holds the stage's own allocations and nothing of its input.
    let inputs = (0..args.iterations)
        .map(|_| stage.prepare())
        .collect::<Result<Vec<_>>>()?;
    report.prepared_input_bytes = inputs.first().map_or(0, |input| stage.input_bytes(input));
    reset_peak_rss()?;
    let start = status_bytes("VmRSS")?;
    let profiler = dhat::Profiler::builder()
        .testing()
        .trim_backtraces(Some(4))
        .build();
    let before = heap_now();
    let mut first_after_run = None;
    let mut allocated = Vec::with_capacity(inputs.len());
    let mut observation = None;
    for input in inputs {
        let entering = heap_now();
        let output = stage.run(input)?;
        let after_run = heap_now();
        allocated.push(after_run.total_bytes - entering.total_bytes);
        let seen = stage.observe(&output)?;
        drop(output);
        if first_after_run.is_none() {
            first_after_run = Some(after_run);
            observation = Some(seen);
        }
    }
    let after_drop = heap_now();
    drop(profiler);
    let peak = status_bytes("VmHWM")?;
    let after_run = first_after_run.ok_or("no heap run happened")?;
    report.resident = Resident {
        start_bytes: start,
        peak_bytes: peak,
        growth_bytes: peak.saturating_sub(start),
    };
    report.sample_count = allocated.len();
    report.heap = Some(HeapReport {
        before,
        after_run,
        after_drop,
        allocated_bytes_per_run: allocated,
        peak_workspace_bytes: (after_run.max_bytes - before.curr_bytes) as u64,
    });
    report.observation = Some(observation.ok_or("no heap run happened")?);
    Ok(())
}

/// One point of the per-series cost measurement, written as JSON.
///
/// In the DHAT build the whole measurement runs under a profiler, so the
/// block's and the cache's heap are measured; the timing build reports the
/// Arrow and reservation figures only.
fn series_cost(args: &SeriesCostArgs) -> Result<()> {
    #[cfg(feature = "bench-heap")]
    let profiler = dhat::Profiler::builder().testing().build();
    #[cfg(feature = "bench-heap")]
    let heap = || {
        let stats = dhat::HeapStats::get();
        Some(series_cost::HeapNow {
            curr_bytes: stats.curr_bytes as u64,
            max_bytes: stats.max_bytes as u64,
        })
    };
    #[cfg(not(feature = "bench-heap"))]
    let heap = || None;
    let mut point = series_cost::series_cost(args.signal, args.series, args.denormalize, &heap)?;
    #[cfg(feature = "bench-heap")]
    drop(profiler);
    if let Some(object) = point.as_object_mut() {
        let _ = object.insert("allocator".into(), describe().allocator.into());
    }
    std::fs::write(&args.output, serde_json::to_vec_pretty(&point)?)?;
    Ok(())
}

#[cfg(not(feature = "bench-heap"))]
fn measure_heap(_stage: &Stage, _args: &Args, _report: &mut Report) -> Result<()> {
    Err("the heap profile needs the executable built with --features bench-heap".into())
}

fn main() -> Result<()> {
    let args = match parse_args(std::env::args().skip(1))? {
        Command::SelfTest => return tests::run(),
        Command::Describe => {
            let mut stdout = std::io::stdout();
            serde_json::to_writer(&mut stdout, &describe())?;
            stdout.write_all(b"\n")?;
            return Ok(());
        }
        Command::Measure(args) => args,
        Command::SeriesCost(args) => return series_cost(&args),
        Command::FlushStall(args) => return flush_stall::main(&args),
        Command::Skip => {
            writeln!(
                std::io::stderr(),
                "measurement: skipped, no --stage given; this bench is driven by the \
                 series_parquet measurement harness (crates/validation/tests/series_parquet)"
            )?;
            return Ok(());
        }
    };
    if cfg!(feature = "bench-heap") != (args.profile == Profile::Heap) {
        return Err(format!(
            "the {:?} profile needs the executable built {} the bench-heap feature",
            args.profile,
            if args.profile == Profile::Heap {
                "with"
            } else {
                "without"
            }
        )
        .into());
    }
    let cfg = BenchConfig::read(&args.config)?;
    let input = stages::read_input(&args.input)?;
    let workload_config_id = cfg.workload_config_id.clone();
    let signal = input.sidecar.signal;
    let requests = input.requests.len();
    let input_bytes = input.requests.iter().map(bytes::Bytes::len).sum();
    let stage = Stage::new(
        args.stage,
        cfg,
        input,
        args.compression,
        args.profile == Profile::Timing,
    )?;
    let mut report = Report {
        schema: "series-stage-bench/1",
        stage: args.stage,
        profile: args.profile,
        clock: args.stage.clock(),
        compression: args.compression_name.clone(),
        workload_config_id,
        signal,
        requests,
        records: stage.records(),
        input_bytes,
        iterations: args.iterations,
        sample_count: 0,
        fixture_retained_bytes: stage.fixture_retained_bytes(),
        prepared_input_bytes: 0,
        warmup: None,
        timing: None,
        heap: None,
        resident: Resident::default(),
        resident_steady: Resident::default(),
        observation: None,
        verification_passed: false,
        failed_checks: Vec::new(),
    };
    handshake_ready(args.handshake)?;
    match args.profile {
        Profile::Timing => measure_timing(&stage, &args, &mut report)?,
        Profile::Heap => measure_heap(&stage, &args, &mut report)?,
    }
    let observation = report.observation.as_ref().ok_or("nothing was observed")?;
    let failed: Vec<String> = observation
        .checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| format!("{}: {}", check.name, check.detail))
        .collect();
    report.failed_checks.extend(failed);
    report.verification_passed = report.failed_checks.is_empty();
    let file = std::fs::File::create(&args.output)?;
    serde_json::to_writer_pretty(std::io::BufWriter::new(file), &report)?;
    handshake_done(args.handshake)?;
    Ok(())
}
