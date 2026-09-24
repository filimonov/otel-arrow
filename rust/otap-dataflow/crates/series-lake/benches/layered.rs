// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Cumulative Criterion layers from prepared OTLP bytes to local Parquet.
//!
//! The groups are registered in order, each adding one production stage to
//! the one before: `otlp_noop` (prepared OTLP bytes consumed by the
//! synchronous noop path), `otlp_convert`, `otlp_extract_hash`, `otlp_sort`
//! (admission, seal and a fully consumed merge), `otlp_parquet_local`
//! (uncompressed Parquet persisted with synchronous file writes) and
//! `otlp_parquet_zstd`. Criterion measures cumulative wall time only; CPU,
//! resident memory and allocation come from the `measurement` bench.
//!
//! The input comes from the environment, because Criterion owns the
//! command line: `SERIES_STAGE_INPUT` (a length-prefixed OTLP file with its
//! sidecar), `SERIES_STAGE_CONFIG` (the bench configuration), optionally
//! `SERIES_STAGE` to register one group only, `SERIES_STAGE_SUMMARY` for
//! a JSON file describing the untimed verification run of each group, and
//! `SERIES_STAGE_HANDSHAKE=1` to wait for the harness before measuring.

#[path = "measurement/allocator.rs"]
mod allocator;
#[path = "measurement/stages.rs"]
mod stages;

use std::cell::RefCell;
use std::io::{BufRead as _, Read as _, Write as _};
use std::path::PathBuf;
use std::time::Duration;

use criterion::{BatchSize, Criterion, SamplingMode, Throughput};
use stages::Timing;

use stages::{BenchConfig, Result, Stage, StageName};

/// The group measurement time the brief configures. `SERIES_CRITERION_S`
/// overrides the floor, which is how the retry path is exercised.
const MEASUREMENT_TIME: Duration = Duration::from_secs(5);

/// The configured measurement-time floor, or the environment's override.
fn configured_measurement_time() -> Duration {
    std::env::var("SERIES_CRITERION_S")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|seconds| *seconds > 0.0)
        .map_or(MEASUREMENT_TIME, Duration::from_secs_f64)
}

/// The measured work every timing process must accumulate.
const MINIMUM_MEASURED: Duration = Duration::from_secs(1);

/// The longest a group may run to reach that second of measured work.
const MEASUREMENT_TIME_CAP: Duration = Duration::from_secs(60);

/// How many times a group is run to reach that second of measured work.
const GROUP_ATTEMPTS: usize = 3;

/// Where Criterion keeps its artifacts.
fn criterion_home() -> PathBuf {
    std::env::var_os("CRITERION_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/criterion"))
}

/// How long a group must run to time `minimum` of the stage's own work.
///
/// Criterion measures only the routine, but schedules from a warm-up that
/// also pays for the batched preparation, so a group spends
/// `(prepare + run) / run` of its time for every unit it measures. The
/// configured time is the floor: a layer whose preparation is negligible
/// keeps exactly the brief's five seconds.
fn measurement_time(
    prepare: Timing,
    run: Timing,
    configured: Duration,
    minimum: Duration,
) -> Duration {
    let run_ns = run.wall_ns.max(1);
    let overhead = (prepare.wall_ns + run_ns) as f64 / run_ns as f64;
    // Half again what the ratio demands: the warm-up estimate, Criterion's
    // own per-iteration bookkeeping and the batch's drop all sit between
    // the plan and the measured total, and a group that lands just under
    // the required second would fail its sample gate.
    let needed = minimum.as_secs_f64() * overhead * 1.5;
    let seconds = needed.max(configured.as_secs_f64());
    Duration::from_secs_f64(seconds.min(MEASUREMENT_TIME_CAP.as_secs_f64()))
}

fn required(name: &str) -> Result<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{name} must name a file").into())
}

/// Answer `--describe` before Criterion sees the command line, so that a
/// harness can identify a prebuilt executable without building anything.
fn describe() -> serde_json::Value {
    serde_json::json!({
        "bench": "layered",
        "bench_heap": cfg!(feature = "bench-heap"),
        "allocator": allocator::name(),
        "debug_assertions": cfg!(debug_assertions),
        "handshake": true,
        "stages": StageName::LAYERS.iter().map(|stage| stage.as_str()).collect::<Vec<_>>(),
    })
}

fn main() -> Result<()> {
    if std::env::args().any(|argument| argument == "--describe") {
        let mut stdout = std::io::stdout();
        serde_json::to_writer(&mut stdout, &describe())?;
        stdout.write_all(b"\n")?;
        return Ok(());
    }
    // A workspace-wide `cargo bench` runs every bench with no inputs; there
    // is nothing to measure without them.
    if std::env::var_os("SERIES_STAGE_CONFIG").is_none()
        && std::env::var_os("SERIES_STAGE_INPUT").is_none()
    {
        writeln!(
            std::io::stderr(),
            "layered: skipped, SERIES_STAGE_CONFIG and SERIES_STAGE_INPUT are unset; this \
             bench is driven by the series_parquet measurement harness \
             (crates/validation/tests/series_parquet)"
        )?;
        return Ok(());
    }
    let cfg = BenchConfig::read(&required("SERIES_STAGE_CONFIG")?)?;
    let input = stages::read_input(&required("SERIES_STAGE_INPUT")?)?;
    let handshake = std::env::var("SERIES_STAGE_HANDSHAKE").is_ok_and(|value| value == "1");
    let layers: Vec<StageName> = match std::env::var("SERIES_STAGE") {
        Ok(name) => {
            let stage = StageName::parse(&name)?;
            if !StageName::LAYERS.contains(&stage) {
                return Err(format!("{name} is not a Criterion layer").into());
            }
            vec![stage]
        }
        Err(_) => StageName::LAYERS.to_vec(),
    };
    if handshake && layers.len() != 1 {
        return Err("the handshake measures exactly one layer per process".into());
    }
    let mut criterion = Criterion::default().configure_from_args();
    let mut summaries = Vec::new();
    for layer in layers {
        let stage = Stage::new(layer, cfg.clone(), input.clone(), stages::zstd(), false)?;
        // One untimed run proves the layer's output before anything is
        // timed; Criterion never times an operation that fails.
        let (prepared, prepare) = stages::timed(|| stage.prepare())?;
        let prepared_input_bytes = stage.input_bytes(&prepared);
        // What the layer was handed, so the recorded result says that its
        // setup existed before the timer started.
        let prepared_carries = prepared.carries();
        // What this iteration would write, and how much per-iteration setup
        // the layer had built before anything was timed. The counter must
        // not advance across the run below, and the recorded numbers say so.
        let prepared_destinations = prepared.destinations();
        let setup_before_run = stage.setup_built();
        let (output, run) = stages::timed(|| stage.run(prepared))?;
        let setup_after_run = stage.setup_built();
        if setup_after_run != setup_before_run {
            return Err(format!(
                "{} built {} pieces of setup inside its timed run",
                layer.as_str(),
                setup_after_run - setup_before_run
            )
            .into());
        }
        let observation = stage.observe(&output)?;
        drop(output);
        let failed: Vec<String> = observation
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect();
        if !failed.is_empty() {
            return Err(format!("{} failed verification: {failed:?}", layer.as_str()).into());
        }
        let mut group_measurement_time = measurement_time(
            prepare,
            run,
            configured_measurement_time(),
            MINIMUM_MEASURED,
        );
        summaries.push(serde_json::json!({
            "stage": stage.name().as_str(),
            "prepared_input": prepared_carries,
            "prepared_destinations": prepared_destinations,
            "setup_built_in_prepare": setup_before_run,
            "setup_built_in_run": setup_after_run - setup_before_run,
            "clock": layer.clock(),
            "records": stage.records(),
            "fixture_retained_bytes": stage.fixture_retained_bytes(),
            "prepared_input_bytes": prepared_input_bytes,
            "verification_prepare_ns": prepare.wall_ns,
            "verification_run_ns": run.wall_ns,
            "measurement_time_s": group_measurement_time.as_secs_f64(),
            "observation": observation,
        }));
        if handshake {
            let mut stdout = std::io::stdout();
            stdout.write_all(b"SERIES_STAGE_READY\n")?;
            stdout.flush()?;
            let mut line = String::new();
            let _ = std::io::stdin().lock().read_line(&mut line)?;
        }
        // Criterion sizes its iterations from a warm-up that includes the
        // batched preparation, so a layer whose measured operation is much
        // cheaper than its preparation would spend its measurement time
        // preparing and time less than the required second of work. The
        // group's measurement time is therefore scaled by the ratio this
        // process just measured, with the configured five seconds as the
        // floor and a minute as the cap.

        // A failure inside a timed routine is kept here and returned once
        // the group has finished, never timed as a success.
        let failure: RefCell<Option<Box<dyn std::error::Error>>> = RefCell::new(None);
        // Criterion estimates its iteration count from a warm-up that also
        // pays for the batched preparation, and that estimate is only
        // approximate, so the group is run again with a longer measurement
        // time until it has actually timed the required second of the
        // stage's own work.
        let mut attempts = Vec::new();
        let mut function_id = cfg.workload_config_id.clone();
        for attempt in 0..GROUP_ATTEMPTS {
            // Each attempt gets an id of its own. Reusing one id would
            // make Criterion disambiguate it by appending " #N" and then
            // sanitize that into a different directory name, so the
            // artifacts of an attempt could not be read back by the name
            // the attempt knows itself by.
            function_id = if attempt == 0 {
                cfg.workload_config_id.clone()
            } else {
                format!("{}-attempt{}", cfg.workload_config_id, attempt + 1)
            };
            let mut group = criterion.benchmark_group(layer.as_str());
            let _ = group
                .sample_size(30)
                .warm_up_time(Duration::from_secs(1))
                .measurement_time(group_measurement_time)
                .sampling_mode(SamplingMode::Flat)
                .throughput(Throughput::Elements(stage.records() as u64));
            let _ = group.bench_function(function_id.as_str(), |bencher| {
                bencher.iter_batched(
                    || stage.prepare(),
                    |prepared| match prepared.and_then(|input| stage.run(input)) {
                        Ok(output) => Some(output),
                        Err(error) => {
                            let _ = failure.borrow_mut().get_or_insert(error);
                            None
                        }
                    },
                    BatchSize::PerIteration,
                );
            });
            group.finish();
            // Read back the artifact of the id this attempt ran with. Any
            // other id would schedule the next attempt from a measurement
            // that is not this one.
            let measured = stages::criterion_measured_seconds(
                &criterion_home(),
                layer.as_str(),
                &function_id,
            )?;
            // Each attempt names the id it ran under, so the harness can
            // read every attempt's own artifact back and refuse a record
            // whose seconds are not that artifact's.
            attempts.push(serde_json::json!({
                "function_id": function_id.as_str(),
                "measurement_time_s": group_measurement_time.as_secs_f64(),
                "measured_wall_s": measured,
            }));
            if failure.borrow().is_some() || measured >= MINIMUM_MEASURED.as_secs_f64() {
                break;
            }
            let factor = (MINIMUM_MEASURED.as_secs_f64() / measured.max(1e-9)) * 1.2;
            let next = group_measurement_time.as_secs_f64() * factor;
            group_measurement_time =
                Duration::from_secs_f64(next.min(MEASUREMENT_TIME_CAP.as_secs_f64()));
        }
        if let Some(entry) = summaries.last_mut() {
            entry["group_attempts"] = serde_json::Value::Array(attempts);
            entry["criterion_function_id"] = serde_json::Value::String(function_id);
        }
        if let Some(error) = failure.into_inner() {
            return Err(error);
        }
    }
    criterion.final_summary();
    if let Some(path) = std::env::var_os("SERIES_STAGE_SUMMARY") {
        let file = std::fs::File::create(path)?;
        serde_json::to_writer_pretty(std::io::BufWriter::new(file), &summaries)?;
    }
    if handshake {
        let mut stdout = std::io::stdout();
        stdout.write_all(b"SERIES_STAGE_DONE\n")?;
        stdout.flush()?;
        let mut rest = Vec::new();
        let _ = std::io::stdin().lock().read_to_end(&mut rest)?;
    }
    Ok(())
}
