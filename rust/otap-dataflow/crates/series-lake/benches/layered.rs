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

#[path = "measurement/stages.rs"]
mod stages;

use std::cell::RefCell;
use std::io::{BufRead as _, Read as _, Write as _};
use std::path::PathBuf;
use std::time::Duration;

use criterion::{BatchSize, Criterion, SamplingMode, Throughput};

use stages::{BenchConfig, Result, Stage, StageName};

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
        "allocator": "system",
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
        let prepared = stage.prepare()?;
        let prepared_input_bytes = stage.input_bytes(&prepared);
        let output = stage.run(prepared)?;
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
        summaries.push(serde_json::json!({
            "stage": stage.name().as_str(),
            "clock": layer.clock(),
            "records": stage.records(),
            "fixture_retained_bytes": stage.fixture_retained_bytes(),
            "prepared_input_bytes": prepared_input_bytes,
            "observation": observation,
        }));
        if handshake {
            let mut stdout = std::io::stdout();
            stdout.write_all(b"SERIES_STAGE_READY\n")?;
            stdout.flush()?;
            let mut line = String::new();
            let _ = std::io::stdin().lock().read_line(&mut line)?;
        }
        // A failure inside a timed routine is kept here and returned once
        // the group has finished, never timed as a success.
        let failure: RefCell<Option<Box<dyn std::error::Error>>> = RefCell::new(None);
        let mut group = criterion.benchmark_group(layer.as_str());
        let _ = group
            .sample_size(30)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(5))
            .sampling_mode(SamplingMode::Flat)
            .throughput(Throughput::Elements(stage.records() as u64));
        let _ = group.bench_function(cfg.workload_config_id.as_str(), |bencher| {
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
