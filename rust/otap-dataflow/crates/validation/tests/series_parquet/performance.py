# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Layered stage measurements: inputs, bench processes and stage results.

`run_stages` invokes one new process per stage, repetition and profile --
Criterion for the cumulative wall-time layers, the purpose-built
`measurement` bench for CPU, resident memory and DHAT allocation, and a
real engine for the `otlp_noop` pipeline baseline -- and combines them into
one stage result per `(stage, mode, workload_config_id, compression,
repetition)`.

Every process runs under the same host controls as an engine run: the
exclusive lease, the out-of-process build monitor, both environment
snapshots and the per-role affinity assertion. An engine-less run registers
its benchmark process with that one monitor; it never starts a second one.

Nothing here builds anything. The bench executables are located before the
first lease is taken, and a build seen while a measurement runs invalidates
it.
"""
import collections
import concurrent.futures
import dataclasses
import functools
import hashlib
import json
import math
import mmap
import multiprocessing
import os
from pathlib import Path
import re
import select
import shutil
import signal
import struct
import subprocess
import sys
import threading
import time

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement

test_e2e = measurement.test_e2e


# --------------------------------------------------------------------------
# The stage contract
# --------------------------------------------------------------------------

# Every stage the benches export, in registration order. The first six are
# the cumulative Criterion layers.
STAGES = (
    "otlp_noop",
    "otlp_convert",
    "otlp_extract_hash",
    "otlp_sort",
    "otlp_parquet_local",
    "otlp_parquet_zstd",
    "otlp_minio",
    "convert",
    "extract",
    "sort_seal",
    "merge",
    "encode",
    "local_write",
    "upload",
    "sink",
)

CRITERION_LAYERS = STAGES[:6]

# How a stage was measured. `pipeline` is the real engine baseline,
# `criterion` the synchronous cumulative layer, `isolated` a synchronous
# library stage and `async` one that drives the runtime and a store.
MODES = ("pipeline", "criterion", "isolated", "async")

ASYNC_STAGES = ("otlp_minio", "local_write", "upload", "sink")

# The full metric schema of every registered stage result, in the fixed
# order the contract lists it, allocation first.
STAGE_METRICS = (
    "allocated_bytes_per_record",
    "records_per_s_per_core",
    "cpu_ns_per_record",
    "peak_rss_bytes",
    "output_bytes_per_input_record",
    "wall_ns_per_record",
    "peak_live_heap_bytes",
    "peak_workspace_bytes",
)

# The identifying fields of every registered stage result.
STAGE_FIELDS = (
    "stage",
    "mode",
    "sample_count",
    "repetition",
    "fingerprint",
    "input_representation",
    "output_representation",
    "denominator",
)

# What each stage is handed and what its per-record denominator therefore
# divides. Every per-record metric of every stage divides by the input
# file's logical records -- the log records or metric points a producer
# sent -- whatever form the stage's own input takes, so a stage that is
# handed already encoded bytes says so here and reports object and byte
# rates beside its per-record ones.
INPUT_REPRESENTATIONS = {
    "otlp_noop": "otlp_wire_bytes",
    "otlp_convert": "otlp_wire_bytes",
    "otlp_extract_hash": "otlp_wire_bytes",
    "otlp_sort": "otlp_wire_bytes",
    "otlp_parquet_local": "otlp_wire_bytes",
    "otlp_parquet_zstd": "otlp_wire_bytes",
    "otlp_minio": "otlp_wire_bytes",
    "convert": "otlp_wire_bytes",
    "extract": "otap_arrow_records",
    "sort_seal": "extracted_rows",
    "merge": "sealed_block",
    "encode": "merged_chunks",
    "local_write": "pre_encoded_parquet_bytes",
    "upload": "pre_encoded_parquet_bytes",
    "sink": "sealed_block",
}

# The stages whose timed input is already encoded: their per-record rates
# describe work that is not per record at all, so they also report the
# rates their input actually has.
PRE_ENCODED_INPUT_STAGES = ("local_write", "upload")

# The stages that store an object. Each one records what a completed write
# means, so that no reader takes it for a durability claim.
STORE_STAGES = ("local_write", "upload", "sink", "otlp_minio")

# The only completion semantics a storing stage may record: the bench's
# `COMPLETION_SEMANTICS` constant, character for character. The meaning is
# fixed by the constant, not searched for in free text, so a reversed
# claim such as "this provides host power-loss durability" is refused
# however it is worded.
COMPLETION_SEMANTICS = (
    "the store reported the object written and the bytes were read back and "
    "verified; object_store does not fsync its local backend, so this is "
    "object-store visibility, not host power-loss durability"
)
ALLOWED_COMPLETION_SEMANTICS = (COMPLETION_SEMANTICS,)

# The rates a pre-encoded-input stage must report beside its per-record
# metrics.
RATE_FIELDS = ("objects_per_s", "input_bytes_per_s", "output_bytes_per_s")


# What a stages family starts before any child takes the host lease, and
# what runs inside each measured window. The setup phase runs no build:
# `rustc --version` starts the compiler executable only to print its
# version -- once per `engine_build` call, which a stages family makes for
# its three bench builds and its two engines -- and `--describe` asks an
# already built bench what it is. The
# compiler-free claim each child records covers its own measured window,
# between the start and end snapshots the out-of-process build monitor
# brackets, and not this setup phase.
SETUP_SUBPROCESSES = {
    "before_any_lease": [
        "git status and git rev-parse HEAD, for the source provenance",
        "rustc --version, once per build probed: five times in a stages "
        "family, for the three bench builds and the two engines; it compiles "
        "nothing",
        "each prebuilt bench executable with --describe, to identify it "
        "without building anything",
        "docker info and docker image inspect, to confirm the daemon and the "
        "local object store image",
        "docker run, docker port, docker inspect and docker update, to start "
        "and pin the object store container",
    ],
    "inside_each_measured_window": [
        "the prebuilt bench executable or the engine under measurement",
        "host_monitor.py, the out-of-process build monitor",
        "docker ps and docker events, asked by that monitor",
    ],
    "compiler_free_claim": (
        "covers measured windows only: no cargo, rustc, cc1 or ld process "
        "was seen by the build monitor inside any child's measured window. "
        "The setup phase above starts rustc five times, each only for "
        "--version, and never invokes cargo or builds anything"
    ),
}

DENOMINATOR = (
    "logical upstream records of the input file (log records or metric "
    "points), whatever form this stage's own input takes"
)

# What each profile must measure. A heap profile never supplies a
# throughput, and a timing profile never supplies an allocation.
PROFILE_METRICS = {
    "timing": (
        "records_per_s_per_core",
        "cpu_ns_per_record",
        "wall_ns_per_record",
        "peak_rss_bytes",
        "output_bytes_per_input_record",
    ),
    "heap": (
        "allocated_bytes_per_record",
        "peak_live_heap_bytes",
        "peak_workspace_bytes",
    ),
    "criterion": ("wall_ns_per_record",),
    "pipeline": (
        "records_per_s_per_core",
        "cpu_ns_per_record",
        "wall_ns_per_record",
        "peak_rss_bytes",
        "output_bytes_per_input_record",
    ),
    "pipeline_heap": (
        "allocated_bytes_per_record",
        "peak_live_heap_bytes",
        "peak_workspace_bytes",
    ),
}

PROFILES = tuple(PROFILE_METRICS)

# Which profile measures each metric of a composite, per mode. A composite
# names the exact child each of its metrics came from.
COMPOSITE_SOURCES = {
    "criterion": dict(
        {name: "timing" for name in PROFILE_METRICS["timing"]},
        **{name: "heap" for name in PROFILE_METRICS["heap"]},
        wall_ns_per_record="criterion",
    ),
    "isolated": dict(
        {name: "timing" for name in PROFILE_METRICS["timing"]},
        **{name: "heap" for name in PROFILE_METRICS["heap"]},
    ),
    "async": dict(
        {name: "timing" for name in PROFILE_METRICS["timing"]},
        **{name: "heap" for name in PROFILE_METRICS["heap"]},
    ),
    "pipeline": dict(
        {name: "pipeline" for name in PROFILE_METRICS["pipeline"]},
        **{name: "pipeline_heap" for name in PROFILE_METRICS["pipeline_heap"]},
    ),
}

# The direction in which each metric gets worse.
METRIC_DIRECTIONS = {
    "allocated_bytes_per_record": measurement.LOWER_IS_BETTER,
    "records_per_s_per_core": measurement.HIGHER_IS_BETTER,
    "cpu_ns_per_record": measurement.LOWER_IS_BETTER,
    "peak_rss_bytes": measurement.LOWER_IS_BETTER,
    "output_bytes_per_input_record": measurement.LOWER_IS_BETTER,
    "wall_ns_per_record": measurement.LOWER_IS_BETTER,
    "peak_live_heap_bytes": measurement.LOWER_IS_BETTER,
    "peak_workspace_bytes": measurement.LOWER_IS_BETTER,
}

# The fewest timing samples and the least measured work of one timing
# process, and the fewest repetitions of every stage.
MINIMUM_SAMPLES = 30
MINIMUM_MEASURED_S = 1.0
REPETITIONS = 3

# The largest coefficient of variation a stage baseline may show across its
# repetitions. This is a sample-stability rule, not a regression allowance.
MAXIMUM_CV = 0.15


def _finite(value) -> bool:
    """Whether one measured value is a finite number, not a boolean."""
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def validate_stage_result(result: dict) -> None:
    """Refuse a registered stage result that measured less than the contract.

    The mandatory metrics are checked first, in their fixed order beginning
    with `allocated_bytes_per_record`: every one must be present and a
    finite number, because an unmeasured mandatory field invalidates the
    run rather than defaulting to zero. The identifying fields follow, with
    a positive sample count.
    """
    metrics = result.get("metrics") or {}
    for name in STAGE_METRICS:
        if name not in metrics:
            raise AssertionError(
                f"stage result {result.get('stage')!r} is missing "
                f"{name}; an unmeasured mandatory metric is incomplete "
                f"acceptance, never a zero"
            )
        if not _finite(metrics[name]):
            raise AssertionError(
                f"stage result {result.get('stage')!r} reports {name} as "
                f"{metrics[name]!r}"
            )
    for field in STAGE_FIELDS:
        if field not in result:
            raise AssertionError(f"stage result is missing {field}")
    if result["stage"] not in STAGES:
        raise AssertionError(f"unknown stage {result['stage']!r}")
    if result["mode"] not in MODES:
        raise AssertionError(f"unknown mode {result['mode']!r}")
    count = result["sample_count"]
    if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
        raise AssertionError(f"stage result has {count!r} samples")
    sources = result.get("metric_sources") or {}
    missing = [name for name in STAGE_METRICS if name not in sources]
    if missing:
        raise AssertionError(
            f"stage result {result['stage']!r} does not say which child "
            f"measured {missing}"
        )
    if result["input_representation"] != INPUT_REPRESENTATIONS[result["stage"]]:
        raise AssertionError(
            f"stage result {result['stage']!r} claims to have been handed "
            f"{result['input_representation']!r}"
        )
    if not result["denominator"]:
        raise AssertionError(
            f"stage result {result['stage']!r} does not say what its "
            f"per-record metrics divide by"
        )
    if result["stage"] in STORE_STAGES:
        semantics = result.get("completion_semantics")
        if semantics not in ALLOWED_COMPLETION_SEMANTICS:
            raise AssertionError(
                f"stage result {result['stage']!r} stores an object and must "
                f"record what a completed write means as the bench's fixed "
                f"completion semantics, which deny host power-loss "
                f"durability; it records {semantics!r}"
            )
    if result["stage"] in PRE_ENCODED_INPUT_STAGES:
        rates = result.get("rates") or {}
        for name in RATE_FIELDS:
            if not _finite(rates.get(name)):
                raise AssertionError(
                    f"stage result {result['stage']!r} times already encoded "
                    f"input and must report {name}, not only a per-record "
                    f"rate; it reports {rates.get(name)!r}"
                )


def validate_child_result(result: dict, profile: str) -> None:
    """Refuse a child run that did not measure its profile's own fields."""
    if profile not in PROFILE_METRICS:
        raise AssertionError(f"unknown profile {profile!r}")
    metrics = result.get("metrics") or {}
    for name in PROFILE_METRICS[profile]:
        if not _finite(metrics.get(name)):
            raise AssertionError(
                f"{profile} child {result.get('run_id')!r} reports {name} as "
                f"{metrics.get(name)!r}"
            )
    forbidden = [
        name
        for name in metrics
        if name in STAGE_METRICS and name not in PROFILE_METRICS[profile]
    ]
    if forbidden:
        raise AssertionError(
            f"{profile} child {result.get('run_id')!r} reports {forbidden}, "
            f"which its profile does not measure"
        )


# --------------------------------------------------------------------------
# Deterministic inputs
# --------------------------------------------------------------------------

INPUT_FORMAT = "u32le-length-prefixed-otlp"


def request_indexes(workload, signal) -> list:
    """Every request index of `workload` carrying `signal`."""
    return [
        index
        for index in range(workload.requests)
        if workload.signal_of(index) == signal
    ]


def expected_series(workload, indexes, signal) -> int:
    """How many distinct series the named requests carry.

    Logs series identity is the resource, the scope and the configured
    `logger.name` attribute, which the workload varies by request. Metrics
    series identity is the metric, which is one per kind, together with the
    point's `series.slot` attribute.
    """
    if signal == "logs":
        return len({measurement.logger_name(workload, index) for index in indexes})
    series = set()
    for index in indexes:
        for point in range(workload.records_per_request):
            fields = measurement.metric_point(workload, index, point)
            series.add((fields["kind"], fields["slot"]))
    return len(series)


def write_stage_input(workload, signal, path) -> dict:
    """Write one signal's requests as a length-prefixed input plus sidecar.

    Each request is a little-endian `u32` length and that many bytes of the
    deterministic OTLP body `build_request` produced, so the bench measures
    exactly the bytes a producer would send.
    """
    path = Path(path)
    indexes = request_indexes(workload, signal)
    if not indexes:
        raise AssertionError(f"the workload carries no {signal} request")
    records = 0
    with open(path, "wb") as handle:
        for index in indexes:
            actual, wire, rows = measurement.build_request(workload, index)
            if actual != signal:
                raise AssertionError(f"request {index} is {actual}, not {signal}")
            records += len(rows)
            _ = handle.write(struct.pack("<I", len(wire)))
            _ = handle.write(wire)
    sidecar = {
        "format": INPUT_FORMAT,
        "signal": signal,
        "requests": len(indexes),
        "records": records,
        "expected_series": expected_series(workload, indexes, signal),
        "sha256": measurement.file_digest(path),
        "request_indexes": indexes,
        "workload": workload.as_json(),
        "bytes": path.stat().st_size,
    }
    _ = measurement.write_json_atomic(path.with_suffix(".json"), sidecar)
    return sidecar


# --------------------------------------------------------------------------
# Workload configurations
# --------------------------------------------------------------------------

# The lake configuration of the shipped example, as the bench reads it.
def lake_config(**overrides) -> dict:
    """The exporter's effective lake configuration, with named overrides."""
    config = {
        "writer_id": "local_1",
        "producer_id_attribute": "host.id",
        "window_interval": "15s",
        "ingress": {
            "max_request_bytes": 16 << 20,
            "max_extracted_bytes": 32 << 20,
            "max_row_bytes": 1 << 20,
            "max_nesting_depth": 32,
            "max_block_bytes": 500 << 20,
            "max_requests_per_block": 4096,
            "pending_series_entry_bytes": 64,
        },
        "sorting": {
            "enabled": True,
            "run_target_bytes": 8 << 20,
            "merge_chunk_bytes": 16 << 20,
        },
        "upload": {"part_bytes": 8 << 20, "concurrency": 2, "abort_timeout": "5s"},
        "parquet": {"row_group_bytes": 64 << 20, "writer_limit_bytes": 96 << 20},
        "unsupported": "reject",
        "logs": {
            "series_attributes": ["logger.name"],
            "denormalize": ["resource.service.name"],
            "values_sort": [
                {"column": "series_id", "order": "asc"},
                {"column": "time_unix_nano", "order": "asc", "nulls": "last"},
            ],
        },
        "metrics": {
            "series_attributes": [],
            "denormalize": ["resource.service.name"],
            "values_sort": [
                {"column": "series_id", "order": "asc"},
                {"column": "time_unix_nano", "order": "asc"},
            ],
        },
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key] = dict(config[key], **value)
        else:
            config[key] = value
    return config


# The measured workloads. Each names one signal, the exporter configuration
# it runs under and the stages it measures; the primary two run the whole
# matrix, the others the library stages their shape stresses.
LIBRARY_STAGES = ("extract", "sort_seal", "merge", "encode", "sink")

WORKLOAD_CONFIGS = {
    "logs-1k-stable": {
        "signal": "logs",
        "workload": measurement.Workload(
            requests=201, records_per_request=100, body_bytes=1024, series=100,
            metrics_every=1000,
        ),
        "lake": lake_config(),
        "committed_fraction": 0.5,
        "stages": STAGES,
        "description": "default keys, 1KiB bodies, a stable set of 100 series",
    },
    "metrics-mixed": {
        "signal": "metrics",
        "workload": measurement.Workload(
            requests=150, records_per_request=100, body_bytes=1024, series=100,
            metrics_every=1,
        ),
        "lake": lake_config(),
        "committed_fraction": 0.5,
        "stages": STAGES,
        "description": "integer and double gauges and sums with mixed "
        "histogram widths, including empty distributions",
    },
    "logs-8k-churn-wide": {
        "signal": "logs",
        "workload": measurement.Workload(
            requests=121, records_per_request=50, body_bytes=8192, series=100000,
            metrics_every=1000,
        ),
        "lake": lake_config(
            logs={
                "series_attributes": ["logger.name"],
                "denormalize": ["resource.service.name"],
                "values_sort": [
                    {"column": "series_id", "order": "asc"},
                    {"column": "body", "order": "asc", "nulls": "last"},
                    {"column": "time_unix_nano", "order": "asc", "nulls": "last"},
                ],
            }
        ),
        "committed_fraction": 0.0,
        "stages": LIBRARY_STAGES,
        "description": "wide custom log body sort keys, 8KiB bodies and a "
        "series that churns with every request",
    },
    "logs-512k-near-limit": {
        "signal": "logs",
        "workload": measurement.Workload(
            requests=25, records_per_request=4, body_bytes=512 * 1024, series=10,
            metrics_every=1000,
        ),
        "lake": lake_config(),
        "committed_fraction": 0.5,
        "stages": LIBRARY_STAGES,
        "description": "a legal near-row-limit fixture: 512KiB bodies under "
        "the 1MiB row limit",
    },
}

# The workloads whose whole stage matrix, including the real pipeline and
# the object store, is measured.
PRIMARY_CONFIGS = ("logs-1k-stable", "metrics-mixed")


def check_fixture(config_id) -> None:
    """Refuse a fixture the current configuration would not accept."""
    config = WORKLOAD_CONFIGS[config_id]
    workload = config["workload"]
    lake = config["lake"]
    row_bytes = workload.body_bytes + 512
    if row_bytes > lake["ingress"]["max_row_bytes"]:
        raise AssertionError(
            f"{config_id} builds a {row_bytes} byte row, over the "
            f"{lake['ingress']['max_row_bytes']} byte row limit; it cannot "
            f"claim supported throughput"
        )
    request_bytes = workload.body_bytes * workload.records_per_request * 2
    if request_bytes > lake["ingress"]["max_request_bytes"]:
        raise AssertionError(
            f"{config_id} builds a request of about {request_bytes} bytes, "
            f"over the {lake['ingress']['max_request_bytes']} byte limit"
        )


# --------------------------------------------------------------------------
# Locating the prebuilt benches
# --------------------------------------------------------------------------




# Where prebuilt bench executables live, and what names them.
BENCH_DEPS = "target/release/deps"

# The environment variables that name an executable outright.
BENCH_ENV = {
    ("measurement", False): "SERIES_STAGE_BENCH",
    ("measurement", True): "SERIES_STAGE_BENCH_HEAP",
    ("layered", False): "SERIES_LAYERED_BENCH",
}

# The allocator a timing bench must describe: the engine's jemalloc with its
# background thread (the bench's `allocator.rs`). Stage families before it
# timed on the system allocator; their baselines carry that allocator in
# their fingerprint and are never compared with a jemalloc run.
TIMING_ALLOCATOR = "jemalloc+background_thread"

# How each executable is built, named in the error a missing one raises.
# Both benches are gated behind the `bench-harness` feature, which
# `bench-heap` implies, so a workspace-wide `cargo bench` never builds them.
BENCH_BUILD_COMMANDS = {
    ("measurement", False): (
        "cargo bench -p otel-arrow-dfe-series-lake --bench measurement "
        "--bench layered --no-run --features bench-harness"
    ),
    ("layered", False): (
        "cargo bench -p otel-arrow-dfe-series-lake --bench measurement "
        "--bench layered --no-run --features bench-harness"
    ),
    ("measurement", True): (
        "cargo bench -p otel-arrow-dfe-series-lake --bench measurement "
        "--no-run --features bench-heap"
    ),
}


def describe_bench(executable, timeout_s=60) -> dict:
    """Ask one prebuilt executable what it is.

    The executable answers `--describe` with its target name, whether DHAT's
    allocator is installed, the allocator it runs on and whether it carries
    debug assertions. Asking
    cargo instead would build the target it was asked about, and a compiler
    running beside a measurement is exactly what invalidates one.
    """
    done = subprocess.run(
        [str(executable), "--describe"], capture_output=True, text=True,
        timeout=timeout_s,
    )
    if done.returncode != 0:
        raise AssertionError(
            f"{executable} did not answer --describe: {done.stderr[-500:]}"
        )
    return json.loads(done.stdout)


def candidate_executables(name) -> list:
    """Every prebuilt file in the release deps directory named for `name`.

    Cargo names a bench executable `<target>-<hash>`, writes a `.d` file
    beside it and keeps older hashes, so the candidates are filtered to
    executable files with no suffix and ordered newest first.
    """
    directory = Path(test_e2e.WORKSPACE) / BENCH_DEPS
    if not directory.is_dir():
        return []
    found = [
        path
        for path in directory.glob(f"{name}-*")
        if path.is_file() and not path.suffix and os.access(path, os.X_OK)
    ]
    return sorted(found, key=lambda path: path.stat().st_mtime, reverse=True)


def locate_benches(*, features=()) -> dict:
    """Locate prebuilt bench executables without building anything.

    No cargo runs here or anywhere else in a measured family: the run
    launches executables that already exist and refuses to start when one is
    missing, naming the command that builds it. Each candidate identifies
    itself, so the DHAT build and the timing build are told apart by what
    they are rather than by their file name, a debug build is refused, and
    so is a timing build that does not run on `TIMING_ALLOCATOR`.
    """
    heap = "bench-heap" in features
    wanted = [("measurement", heap)] + ([] if heap else [("layered", False)])
    found = {}
    for name, needs_heap in wanted:
        override = os.environ.get(BENCH_ENV[(name, needs_heap)])
        candidates = [Path(override)] if override else candidate_executables(name)
        problems = []
        for path in candidates:
            if not path.is_file():
                problems.append(f"{path}: not a file")
                continue
            if Path("target/debug") in Path(path).parents or "/target/debug/" in str(path):
                problems.append(f"{path}: a debug build is not a measurement")
                continue
            try:
                description = describe_bench(path)
            except (AssertionError, OSError, ValueError, subprocess.SubprocessError) as error:
                problems.append(f"{path}: {error}")
                continue
            if description.get("bench") != name:
                problems.append(f"{path}: is the {description.get('bench')!r} bench")
                continue
            if description.get("debug_assertions"):
                problems.append(
                    f"{path}: carries debug assertions; a measured bench uses "
                    f"the bench profile"
                )
                continue
            if bool(description.get("bench_heap")) != needs_heap:
                continue
            if not needs_heap and description.get("allocator") != TIMING_ALLOCATOR:
                problems.append(
                    f"{path}: runs on the {description.get('allocator')!r} "
                    f"allocator; a timing bench runs on {TIMING_ALLOCATOR!r} "
                    f"like the engine"
                )
                continue
            found[name] = {
                "executable": str(path),
                "description": description,
                "features": sorted(features),
                "mtime": path.stat().st_mtime,
            }
            break
        if name not in found:
            raise AssertionError(
                f"no prebuilt {name} bench"
                + (" with the bench-heap feature" if needs_heap else "")
                + f" was found in {BENCH_DEPS}; build it before measuring with: "
                + BENCH_BUILD_COMMANDS[(name, needs_heap)]
                + (f"; rejected candidates: {problems[:3]}" if problems else "")
            )
    return found


def bench_build(located) -> dict:
    """The build provenance of one bench executable `locate_benches` found.

    The profile is `bench`: cargo's bench profile inherits release and adds
    fat link-time optimization, and its artifacts land beside the release
    ones. The allocator is the one the executable described, so it enters
    the baseline fingerprint: the DHAT and timing builds never share one,
    and neither do timing builds on different allocators.
    """
    allocator = located["description"].get("allocator")
    if not allocator:
        raise AssertionError(
            f"{located['executable']} does not name its allocator in --describe"
        )
    build = measurement.engine_build(Path(located["executable"]))
    build.update(
        {
            "profile": "bench",
            "features": ",".join(located["features"]) or "default",
            "allocator": allocator,
        }
    )
    return build


# --------------------------------------------------------------------------
# Running one bench process
# --------------------------------------------------------------------------

READY_MARKER = "SERIES_STAGE_READY"
DONE_MARKER = "SERIES_STAGE_DONE"

# How long a bench process may take to build its fixtures and to measure.
FIXTURE_DEADLINE_S = 600
MEASURE_DEADLINE_S = 600


class BenchProcess:
    """A prebuilt bench executable, pinned, with its two-edge handshake.

    The process announces `SERIES_STAGE_READY` once its fixtures exist and
    waits for one line before it measures, and `SERIES_STAGE_DONE` once its
    output file is written, waiting for its input to close. Both edges are
    where the harness snapshots the live process, so the environment of a
    measurement is observed on the process that made it.
    """

    def __init__(self, argv, *, cores, log_path, env=None):
        self.argv = [str(item) for item in argv]
        self.cores = sorted(int(core) for core in cores)
        self.log_path = Path(log_path)
        self.env = dict(os.environ if env is None else env)
        self.process = None
        self.markers = {}
        self.lines = []
        self._reader = None

    def _pin(self):
        """Confine the child, and every thread it starts, to its cores."""
        os.sched_setaffinity(0, set(self.cores))

    def start(self):
        """Start the process and read its markers on a reader thread."""
        handle = open(self.log_path, "w", encoding="ascii", errors="replace")
        self.process = subprocess.Popen(
            self.argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=handle,
            env=self.env,
            cwd=str(test_e2e.WORKSPACE),
            text=True,
            preexec_fn=self._pin,  # noqa: PLW1509 - the child is pinned before exec
        )
        self.markers = {READY_MARKER: threading.Event(), DONE_MARKER: threading.Event()}

        def read():
            """Log every line and set the marker events."""
            for line in self.process.stdout:
                self.lines.append(line.rstrip("\n"))
                _ = handle.write(line)
                handle.flush()
                marker = line.strip()
                if marker in self.markers:
                    self.markers[marker].set()
            for event in self.markers.values():
                event.set()
            handle.close()

        self._reader = threading.Thread(target=read, name="bench-reader", daemon=True)
        self._reader.start()
        return self

    @property
    def pid(self):
        """The process id, which is also its main thread id."""
        return self.process.pid

    def await_marker(self, marker, deadline_s):
        """Wait for one marker, or fail with what the process said."""
        if not self.markers[marker].wait(deadline_s):
            raise AssertionError(f"{self.argv[0]} never reported {marker}")
        if self.process.poll() not in (None, 0):
            raise AssertionError(
                f"{self.argv[0]} exited {self.process.returncode} before "
                f"{marker}: {self.tail()}"
            )
        return True

    def go(self):
        """Release the process into its measured part."""
        self.process.stdin.write("go\n")
        self.process.stdin.flush()

    def finish(self, timeout_s=60) -> int:
        """Close the input, wait for the process and return its status."""
        try:
            self.process.stdin.close()
        except OSError:
            pass
        code = self.process.wait(timeout=timeout_s)
        if self._reader is not None:
            self._reader.join(timeout=10)
        return code

    def kill(self):
        """Stop a process that never reached an edge."""
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            _ = self.process.wait(timeout=10)

    def tail(self, lines=20) -> str:
        """The last lines the process wrote."""
        return " | ".join(self.lines[-lines:])


def procfs_cpu_ticks_ns(pid) -> int:
    """One process's user plus system CPU time from its tick counters.

    The counters advance in clock ticks -- ten milliseconds here -- so a
    process that ran for less than one tick reports zero. This is the
    fallback; `procfs_cpu_ns` prefers the scheduler's nanoseconds.
    """
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    ticks = int(fields[11]) + int(fields[12])
    return ticks * (10**9 // os.sysconf("SC_CLK_TCK"))


def procfs_cpu_ns(pid) -> int:
    """One process's CPU time in nanoseconds, over all of its threads.

    The scheduler publishes each thread's time on the CPU in nanoseconds,
    which a short measured window needs: the tick counters advance only
    every ten milliseconds, so a window of a tenth of a second can read as
    zero CPU and turn a real measurement into an unavailable one. A kernel
    without scheduler statistics falls back to the ticks.
    """
    total = 0
    found = False
    task = Path(f"/proc/{pid}/task")
    try:
        entries = sorted(task.iterdir(), key=lambda item: int(item.name))
    except OSError:
        entries = []
    for entry in entries:
        try:
            fields = (entry / "schedstat").read_text().split()
        except OSError:
            continue
        if fields:
            total += int(fields[0])
            found = True
    if found and total > 0:
        return total
    return procfs_cpu_ticks_ns(pid)


def procfs_peak_rss(pid) -> int:
    """One process's resident high-water mark, in bytes."""
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    raise AssertionError(f"process {pid} publishes no VmHWM")


def reset_peak_rss(pid) -> bool:
    """Reset one process's resident high-water mark to its current size."""
    try:
        Path(f"/proc/{pid}/clear_refs").write_text("5")
        return True
    except OSError:
        return False


# --------------------------------------------------------------------------
# Child results
# --------------------------------------------------------------------------


def median(values):
    """The median of a non-empty list of numbers."""
    ordered = sorted(values)
    if not ordered:
        raise AssertionError("a median of no values is undefined")
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def coefficient_of_variation(values) -> float:
    """The relative dispersion of a sample, zero when its mean is zero."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance) / abs(mean)


def stage_rates(stage, timing_child) -> dict:
    """The rates a stage's own input and output have, per measured second.

    A per-record rate divides by the input file's logical records however
    the stage is fed, so a stage handed already encoded bytes -- `upload`
    and `local_write` -- also reports how many objects and how many bytes a
    second of its measured time moves. Everything here is measured: the
    object count and the output bytes come from the stage's own
    verification, and the seconds from its own timer.
    """
    observations = timing_child.get("observations") or {}
    report = observations.get("bench_report") or {}
    observation = report.get("observation") or {}
    extra = observation.get("extra") or {}
    metrics = timing_child.get("metrics") or {}
    wall_ns_per_record = metrics.get("wall_ns_per_record")
    records = report.get("records")
    if not (_finite(wall_ns_per_record) and records and wall_ns_per_record > 0):
        return {}
    seconds = wall_ns_per_record * records / 1e9
    output_bytes = observation.get("output_bytes") or 0
    rates = {
        "records_per_s": records / seconds,
        "output_bytes_per_s": output_bytes / seconds,
    }
    objects = extra.get("objects_count")
    if objects is not None:
        rates["objects_per_s"] = objects / seconds
        rates["bytes_per_object"] = (output_bytes / objects) if objects else 0.0
    if stage in PRE_ENCODED_INPUT_STAGES:
        # The input of these stages is the encoded object itself, so the
        # bytes they were handed are the bytes they wrote.
        rates["input_bytes_per_s"] = output_bytes / seconds
    return rates


def stage_mode(stage) -> str:
    """The mode a stage is measured in by the purpose-built bench."""
    if stage in ASYNC_STAGES:
        return "async"
    if stage in CRITERION_LAYERS:
        return "criterion"
    return "isolated"


def timing_metrics(report, records) -> dict:
    """The timing profile's metrics from one bench report.

    The resident peak is the one a single steady-state iteration reached
    after the loop, which is the figure the paired allocation profile of
    one iteration reconciles with. The peak over the whole loop, which also
    holds whatever the allocator retained across every earlier iteration,
    is recorded as an observation.
    """
    samples = report["timing"]["samples"]
    wall = median([sample["wall_ns"] for sample in samples]) / records
    cpu = median([sample["cpu_ns"] for sample in samples]) / records
    return {
        "records_per_s_per_core": (10**9 / cpu) if cpu > 0 else None,
        "cpu_ns_per_record": cpu,
        "wall_ns_per_record": wall,
        "peak_rss_bytes": report["resident_steady"]["peak_bytes"],
        "output_bytes_per_input_record": report["observation"]["output_bytes"] / records,
    }


def heap_metrics(report, records) -> dict:
    """The heap profile's metrics from one bench report.

    The workspace peak is what DHAT saw the stage allocate above the
    fixtures it was given; the live peak adds the fixture and the prepared
    input the stage held throughout, which were allocated before the
    profiler started and are measured by Arrow's own accounting instead.
    """
    heap = report["heap"]
    runs = heap["allocated_bytes_per_run"]
    fixture = report["fixture_retained_bytes"] + report["prepared_input_bytes"]
    return {
        "allocated_bytes_per_record": (sum(runs) / len(runs)) / records,
        "peak_workspace_bytes": heap["peak_workspace_bytes"],
        "peak_live_heap_bytes": heap["peak_workspace_bytes"] + fixture,
    }


# What Criterion replaces when it turns a benchmark id into a directory
# name, and the length it truncates that name to. Copied from
# `criterion::report::make_filename_safe`.
CRITERION_UNSAFE = '?"/\\*<>:|^'
CRITERION_NAME_LEN = 64


def criterion_directory_name(component) -> str:
    """Criterion's own directory name for one component of a benchmark id."""
    name = "".join("_" if char in CRITERION_UNSAFE else char for char in component)
    # Criterion truncates to a byte length at a character boundary.
    encoded = name.encode("utf-8")[:CRITERION_NAME_LEN]
    return encoded.decode("utf-8", errors="ignore")


def criterion_estimates(home, stage, function) -> dict:
    """Criterion's own estimates and raw samples of one benchmark.

    The artifacts are read under the id the benchmark actually ran with,
    through Criterion's own sanitization of that id: a layer whose group
    was repeated has one directory per attempt, and reading another
    attempt's files would publish a distribution that is not the selected
    one. A missing artifact is refused by name; it is never answered from
    a neighbouring id.

    The artifacts are hashed as they are read, so a stage result names the
    exact files its distribution came from.
    """
    directory = (
        Path(home)
        / criterion_directory_name(stage)
        / criterion_directory_name(function)
        / "new"
    )
    if not (directory / "estimates.json").is_file():
        siblings = sorted(
            path.name for path in (Path(home) / criterion_directory_name(stage)).glob("*")
        ) if (Path(home) / criterion_directory_name(stage)).is_dir() else []
        raise AssertionError(
            f"Criterion wrote no estimates for {stage}/{function} at "
            f"{directory}; the group directory holds {siblings}"
        )
    estimates = json.loads((directory / "estimates.json").read_text(encoding="ascii"))
    samples = json.loads((directory / "sample.json").read_text(encoding="ascii"))
    times = samples["times"]
    iterations = samples["iters"]
    per_iteration = [time / count for time, count in zip(times, iterations)]
    return {
        "estimates": estimates,
        "sample_count": len(times),
        "iterations_per_sample": iterations,
        "measured_wall_s": sum(times) / 1e9,
        "median_ns": estimates["median"]["point_estimate"],
        "mean_ns": estimates["mean"]["point_estimate"],
        "std_dev_ns": estimates["std_dev"]["point_estimate"],
        "min_ns": min(per_iteration),
        "max_ns": max(per_iteration),
        "artifacts": [
            dict(measurement.file_entry(directory / name), kind="criterion")
            for name in ("estimates.json", "sample.json", "benchmark.json")
            if (directory / name).is_file()
        ],
    }


def criterion_attempts_agree(home, stage, summary) -> None:
    """Refuse a layer whose attempt record is not its own artifacts.

    The layer bench repeats a group under a new id until it has measured a
    second of work, and records, per attempt, the id it ran under and the
    seconds it read back. Every attempt's own artifact is read here and has
    to carry exactly those seconds, and the last attempt has to be the one
    the stage result publishes. A bench that read a stale id back -- every
    attempt then repeats the first attempt's seconds -- is refused, as is
    an attempt whose artifact is missing.
    """
    attempts = summary.get("group_attempts")
    if not attempts:
        raise AssertionError(f"the {stage} layer recorded no Criterion attempts")
    for index, attempt in enumerate(attempts, start=1):
        function = attempt.get("function_id")
        if not function:
            raise AssertionError(
                f"attempt {index} of the {stage} layer does not name its id"
            )
        read = criterion_estimates(home, stage, function)["measured_wall_s"]
        recorded = attempt.get("measured_wall_s")
        if not _finite(recorded) or abs(read - recorded) > 1e-9 * max(1.0, read):
            raise AssertionError(
                f"attempt {index} of the {stage} layer recorded {recorded} s but "
                f"its own artifact {function!r} measured {read} s: the attempt "
                f"was scheduled from another attempt's samples"
            )
    if attempts[-1]["function_id"] != summary.get("criterion_function_id"):
        raise AssertionError(
            f"the {stage} layer publishes {summary.get('criterion_function_id')!r} "
            f"but its last attempt ran as {attempts[-1]['function_id']!r}"
        )


def criterion_samples_ok(estimates) -> tuple:
    """Whether one Criterion group is a measurement, and why.

    Every timing process must take at least thirty samples and accumulate
    one second of measured work; Criterion's are no exception. Criterion
    schedules its iterations from a warm-up that also pays for the batched
    preparation, so the layer bench scales its group's measurement time by
    that ratio -- and a group that still measured less than a second fails
    here rather than being excused by the purpose-built timing process of
    the same stage.
    """
    passed = (
        estimates["sample_count"] >= MINIMUM_SAMPLES
        and min(estimates["iterations_per_sample"], default=0) >= 1
        and estimates["measured_wall_s"] >= MINIMUM_MEASURED_S
    )
    detail = (
        f"{estimates['sample_count']} Criterion samples, "
        f"{estimates['measured_wall_s']:.3f}s measured over "
        f"{sum(estimates['iterations_per_sample'])} iterations; "
        f"{MINIMUM_SAMPLES} samples and {MINIMUM_MEASURED_S}s required"
    )
    return passed, detail


def dhat_totals(path) -> dict:
    """Totals of one DHAT heap profile written by an instrumented process."""
    # DHAT writes its own JSON, which carries a non-ASCII time unit; this
    # is an external artifact, not one of this harness's ASCII documents.
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    points = document["pps"]
    return {
        "total_bytes": sum(point["tb"] for point in points),
        "total_blocks": sum(point["tbk"] for point in points),
        "peak_bytes": sum(point["gb"] for point in points),
        "end_bytes": sum(point["eb"] for point in points),
        "program_points": len(points),
    }


# --------------------------------------------------------------------------
# One measured child run
# --------------------------------------------------------------------------


class DirectoryLauncher(test_e2e.LocalLauncher):
    """A local launcher that starts the engine in a directory of its own.

    An instrumented engine writes its heap profile into its working
    directory, so each profiled lifetime gets one that belongs to its run.
    """

    kind = "local-directory"

    def __init__(self, directory):
        self.directory = Path(directory)

    def start(self, argv, log, env):
        """Start `argv` with the run's directory as its working directory."""
        return subprocess.Popen(
            argv, stdout=log, stderr=subprocess.STDOUT, env=env,
            cwd=str(self.directory),
        )


def child_run_id(stage, mode, config_id, compression, profile, ordinal) -> str:
    """The file name of one child measurement.

    The ordinal is unique within the family across executions: a second
    execution on the same machine continues the numbering instead of
    colliding with the evidence the first one published.
    """
    return (
        f"stages-{stage}-{mode}-{config_id}-{compression}-{profile}"
        f"-r{ordinal:03d}"
    )


def family_ordinal(report_dir) -> int:
    """The first family execution number no published aggregate has used."""
    directory = measurement.resolve_report_dir(report_dir)
    highest = 0
    if Path(directory).is_dir():
        for path in Path(directory).glob("stages-*-f[0-9][0-9][0-9].json"):
            try:
                highest = max(highest, int(path.stem.rsplit("-f", 1)[1]))
            except (IndexError, ValueError):
                continue
    return highest + 1


def child_spec(plan, job) -> measurement.RunSpec:
    """The immutable inputs of one child measurement."""
    config = WORKLOAD_CONFIGS[job["config_id"]]
    return measurement.RunSpec(
        run_id=child_run_id(
            job["stage"], job["mode"], job["config_id"], job["compression"],
            job["profile"], job["ordinal"],
        ),
        case=f"stages-{job['stage']}-{job['profile']}",
        topology="noop" if job["profile"].startswith("pipeline") else "stage",
        store=job["store"],
        cores=tuple(plan["bench_cores"]),
        workload=config["workload"],
        interval_s=15,
        duration_s=1,
        max_in_flight=1 if not job["profile"].startswith("pipeline") else 64,
        overrides={
            "stage": job["stage"],
            "mode": job["mode"],
            "profile": job["profile"],
            "compression": job["compression"],
            "workload_config_id": job["config_id"],
            "repetition": job["repetition"],
        },
    )


def bench_config_document(plan, job, run_dir) -> dict:
    """The configuration file one bench process reads.

    Every path in it is inside the child's own run directory, so two
    otherwise identical runs share a fingerprint; the store endpoint is a
    per-run value the result declares as ephemeral.
    """
    config = WORKLOAD_CONFIGS[job["config_id"]]
    if job["store"] == "local":
        storage = {"file": {"base_uri": str(Path(run_dir) / "store")}}
    else:
        storage = plan["store_storage"]
    return {
        "workload_config_id": job["config_id"],
        "lake": config["lake"],
        "storage": storage,
        "scratch_dir": str(Path(run_dir) / "scratch"),
        "cache_entries": 200000,
        "committed_fraction": config["committed_fraction"],
        # A measured configuration marks no synthetic series committed;
        # the knob exists for the self-test that proves the cache warm-up
        # is outside the timer.
        "extra_committed_series": 0,
        "window_start_secs": measurement.LOG_BASE_TIME_NS // 10**9,
        "seal_at_us": measurement.LOG_BASE_TIME_NS // 1000,
    }


def effective_config(plan, job, run_dir, bench_config) -> dict:
    """Everything that decides what a child measured."""
    sidecar = plan["inputs"][job["config_id"]]["sidecar"]
    return {
        "bench": {
            "stage": job["stage"],
            "mode": job["mode"],
            "profile": job["profile"],
            "compression": job["compression"],
            "iterations": job["iterations"],
            "minimum_samples": MINIMUM_SAMPLES,
            "minimum_measured_s": MINIMUM_MEASURED_S,
            "repetition": job["repetition"],
            "criterion": {
                "sample_size": 30,
                "warm_up_time_s": 1,
                "measurement_time_s": 5,
                "sampling_mode": "flat",
            },
        },
        "input": {
            key: sidecar[key]
            for key in ("format", "signal", "requests", "records",
                        "expected_series", "sha256", "bytes")
        },
        "lake": bench_config,
    }


def prepare_child(plan, job, result, run_dir, controls):
    """The part of every child's body that is the same, whatever it runs."""
    controls.allocate(plan["allocation"])
    controls.register("harness", os.getpid())
    if job["store"] != "local" and plan.get("store_pid"):
        controls.register("store", plan["store_pid"], plan["store_cores"])
    result["environment"]["build"] = job["build"]
    result["environment"]["git"] = plan["git"]
    result["ephemeral_values"] = dict(plan["ephemeral"])
    bench_config = bench_config_document(plan, job, run_dir)
    (Path(run_dir) / "store").mkdir(parents=True, exist_ok=True)
    (Path(run_dir) / "scratch").mkdir(parents=True, exist_ok=True)
    result["config"]["effective"] = effective_config(plan, job, run_dir, bench_config)
    return bench_config


def record_stage_checks(result, *, delivered, detail, samples_ok, samples_detail):
    """The two hard gates every child of this family records itself."""
    result["checks"].append(
        measurement.check(
            "delivery",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED if delivered else measurement.STATUS_FAILED,
            detail,
        )
    )
    result["checks"].append(
        measurement.check(
            "minimum_samples",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED if samples_ok else measurement.STATUS_FAILED,
            samples_detail,
        )
    )


def settle_child(result, job, metrics, observations, artifacts=()):
    """Metrics, directions and observations of one child."""
    result["metrics"] = {
        name: metrics[name] for name in PROFILE_METRICS[job["profile"]]
    }
    result["metric_directions"] = {
        name: METRIC_DIRECTIONS[name] for name in result["metrics"]
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    for name, value in result["metrics"].items():
        if value is None:
            result["metrics_unavailable"][name] = "the child did not measure it"
    result["observations"] = observations
    result["artifacts"] = list(artifacts)
    result["stage"] = job["stage"]
    result["mode"] = job["mode"]
    result["profile"] = job["profile"]
    result["compression"] = job["compression"]
    result["workload_config_id"] = job["config_id"]
    result["repetition"] = job["repetition"]
    result["status"] = measurement.STATUS_PASSED
    validate_child_result(result, job["profile"])


def bench_experiment(plan, job, spec, result, run_dir, controls):
    """One prebuilt bench process, measured under the host controls."""
    bench_config = prepare_child(plan, job, result, run_dir, controls)
    run_dir = Path(run_dir)
    config_path = measurement.write_json_atomic(run_dir / "bench-config.json", bench_config)
    output_path = run_dir / "bench-report.json"
    input_path = plan["inputs"][job["config_id"]]["path"]
    argv = [
        job["executable"], "--stage", job["stage"], "--input", str(input_path),
        "--config", str(config_path), "--output", str(output_path),
        "--iterations", str(job["iterations"]), "--profile",
        "heap" if job["profile"] == "heap" else "timing",
        "--compression", job["compression"], "--handshake",
    ]
    process = BenchProcess(
        argv, cores=spec.cores, log_path=run_dir / "bench.log"
    ).start()
    try:
        _ = process.await_marker(READY_MARKER, FIXTURE_DEADLINE_S)
        controls.register("bench", process.pid, spec.cores)
        snapshot = controls.snapshot(
            "start", {"bench": (process.pid, list(spec.cores))}, pinned=("bench",)
        )
        controls.watch_pinned(process.pid, spec.cores)
        measurement.record_event(result, "bench_started", str(process.pid))
        controls.raise_if_invalid()
        process.go()
        _ = process.await_marker(DONE_MARKER, MEASURE_DEADLINE_S)
        controls.raise_if_invalid()
        _ = controls.snapshot(
            "end", {"bench": (process.pid, list(spec.cores))}, pinned=("bench",)
        )
        controls.unwatch_workers()
        code = process.finish()
    finally:
        controls.unwatch_workers()
        process.kill()
    if code != 0:
        raise AssertionError(f"the bench exited {code}: {process.tail()}")
    _ = snapshot
    report = json.loads(output_path.read_text(encoding="ascii"))
    records = report["records"]
    if job["profile"] == "heap":
        metrics = heap_metrics(report, records)
        samples_ok = report["sample_count"] >= job["iterations"]
        samples_detail = f"{report['sample_count']} profiled iterations"
    else:
        metrics = timing_metrics(report, records)
        timing = report["timing"]
        samples_ok = (
            timing["complete"]
            and report["sample_count"] >= MINIMUM_SAMPLES
            and timing["measured_wall_ns_total"] >= MINIMUM_MEASURED_S * 10**9
        )
        samples_detail = (
            f"{report['sample_count']} samples (every "
            f"{timing.get('sample_stride', 1)} kept), "
            f"{timing['measured_wall_ns_total'] / 1e9:.3f}s measured; "
            f"{timing.get('incomplete_reason') or 'complete'}"
        )
    record_stage_checks(
        result,
        delivered=report["verification_passed"],
        detail=f"stage verification: {report['failed_checks'] or 'every check passed'}",
        samples_ok=samples_ok,
        samples_detail=samples_detail,
    )
    result["samples"] = [
        dict(sample, index=index)
        for index, sample in enumerate((report.get("timing") or {}).get("samples", []))
    ]
    settle_child(
        result,
        job,
        metrics,
        {
            "loop_peak_rss_bytes": (report.get("resident") or {}).get("peak_bytes"),
            "bench_report": {
                key: report[key]
                for key in (
                    "schema", "stage", "profile", "clock", "compression",
                    "requests", "records", "input_bytes", "iterations",
                    "sample_count", "fixture_retained_bytes",
                    "prepared_input_bytes", "warmup", "resident",
                    "resident_steady", "heap",
                    "observation", "failed_checks",
                )
                if key in report
            },
            "timing": {
                key: (report.get("timing") or {}).get(key)
                for key in ("prepare_ns_total", "measured_wall_ns_total",
                            "complete", "incomplete_reason", "parts",
                            "sample_stride", "sample_count")
            },
        },
        artifacts=[
            dict(measurement.file_entry(path), kind=kind, retention=str(run_dir))
            for path, kind in (
                (output_path, "bench_report"),
                (config_path, "bench_config"),
                (run_dir / "bench.log", "bench_log"),
            )
            if path.is_file()
        ],
    )


def criterion_experiment(plan, job, spec, result, run_dir, controls):
    """One Criterion layer, measured in a process of its own."""
    bench_config = prepare_child(plan, job, result, run_dir, controls)
    run_dir = Path(run_dir)
    config_path = measurement.write_json_atomic(run_dir / "bench-config.json", bench_config)
    home = run_dir / "criterion"
    summary_path = run_dir / "layered-summary.json"
    environment = dict(os.environ)
    environment.update(
        {
            "SERIES_STAGE": job["stage"],
            "SERIES_STAGE_INPUT": str(plan["inputs"][job["config_id"]]["path"]),
            "SERIES_STAGE_CONFIG": str(config_path),
            "SERIES_STAGE_SUMMARY": str(summary_path),
            "SERIES_STAGE_HANDSHAKE": "1",
            "CRITERION_HOME": str(home),
        }
    )
    process = BenchProcess(
        [job["executable"], "--bench", "--noplot"],
        cores=spec.cores, log_path=run_dir / "criterion.log", env=environment,
    ).start()
    try:
        _ = process.await_marker(READY_MARKER, FIXTURE_DEADLINE_S)
        controls.register("bench", process.pid, spec.cores)
        _ = controls.snapshot(
            "start", {"bench": (process.pid, list(spec.cores))}, pinned=("bench",)
        )
        controls.watch_pinned(process.pid, spec.cores)
        controls.raise_if_invalid()
        process.go()
        _ = process.await_marker(DONE_MARKER, MEASURE_DEADLINE_S)
        controls.raise_if_invalid()
        _ = controls.snapshot(
            "end", {"bench": (process.pid, list(spec.cores))}, pinned=("bench",)
        )
        controls.unwatch_workers()
        code = process.finish()
    finally:
        controls.unwatch_workers()
        process.kill()
    if code != 0:
        raise AssertionError(f"the layered bench exited {code}: {process.tail()}")
    summary = json.loads(summary_path.read_text(encoding="ascii"))[0]
    # Criterion writes each repeated attempt of one benchmark id under its
    # own name, and the layer bench reports which one it finished on.
    criterion_attempts_agree(home, job["stage"], summary)
    estimates = criterion_estimates(home, job["stage"], summary["criterion_function_id"])
    records = summary["records"]
    samples_ok, samples_detail = criterion_samples_ok(estimates)
    failed = [
        check for check in summary["observation"]["checks"] if not check["passed"]
    ]
    record_stage_checks(
        result,
        delivered=not failed,
        detail=f"layer verification: {failed or 'every check passed'}",
        samples_ok=samples_ok,
        samples_detail=samples_detail,
    )
    settle_child(
        result,
        job,
        {"wall_ns_per_record": estimates["median_ns"] / records},
        {
            "criterion": {
                key: estimates[key]
                for key in ("sample_count", "measured_wall_s", "median_ns",
                            "mean_ns", "std_dev_ns", "min_ns", "max_ns",
                            "iterations_per_sample")
            },
            "criterion_estimates": estimates["estimates"],
            "layer": summary,
        },
        artifacts=[
            dict(entry, retention=str(run_dir)) for entry in estimates["artifacts"]
        ]
        + [
            dict(measurement.file_entry(summary_path), kind="layer_summary",
                 retention=str(run_dir))
        ],
    )


# How often the pipeline baseline observes the engine, and how many
# answered collection epochs its samples must span.
PIPELINE_SAMPLE_PERIOD_S = 0.25
PIPELINE_EPOCHS = 3
PIPELINE_READY_DEADLINE_S = 60
# The admin shutdown deadline of every engine this family starts.
SHUTDOWN_DEADLINE_S = 180
PIPELINE_DRAIN_DEADLINE_S = 120


def noop_sample(engine) -> dict:
    """One strict sample of the noop pipeline's single worker.

    The noop exporter publishes no exporter metric set, so the sample is
    the worker's own pipeline metrics: an absent or non-numeric uptime is
    an error, never a zero.
    """
    monotonic_ns = time.monotonic_ns()
    document = test_e2e.engine_metrics(engine)
    parsed = measurement.parse_telemetry(document, expected_workers=1)
    workers = {}
    for key, worker in parsed["workers"].items():
        workers[key] = {
            "key": key,
            "group_id": worker["group_id"],
            "pipeline_id": worker["pipeline_id"],
            "core_id": worker["core_id"],
            "generation": worker["generation"],
            "uptime_s": measurement.require_gauge(worker, "pipeline", "uptime"),
            "pipeline_memory_usage_bytes": measurement.require_gauge(
                worker, "pipeline", "memory.usage"
            ),
        }
    return {
        "monotonic_ns": monotonic_ns,
        "observed_utc": measurement.utc_now(),
        "workers": workers,
        "process": parsed["process"],
        "process_rss_bytes": test_e2e.rss_bytes(engine.pid),
    }


class NoopSampler:
    """Observe the noop pipeline on its own timeline while input flows."""

    def __init__(self, engine, period_s=PIPELINE_SAMPLE_PERIOD_S):
        self.engine = engine
        self.period_s = period_s
        self.samples = []
        self.errors = []
        self._stop = threading.Event()
        self._thread = None

    def once(self):
        """One sample, recording an error rather than a zero."""
        try:
            sample = noop_sample(self.engine)
        except Exception as error:  # noqa: BLE001 - recorded, never silenced
            self.errors.append(f"{type(error).__name__}: {error}")
            return None
        self.samples.append(sample)
        return sample

    def _run(self):
        """Sample until stopped."""
        while not self._stop.is_set():
            _ = self.once()
            _ = self._stop.wait(self.period_s)

    def start(self):
        """Start sampling on a thread of its own."""
        self._thread = threading.Thread(target=self._run, name="noop-sampler")
        self._thread.start()
        return self

    def stop(self):
        """Stop sampling and join the thread."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def epochs(self) -> dict:
        """How many distinct collection epochs each worker answered."""
        seen = collections.defaultdict(set)
        for sample in self.samples:
            for key, worker in sample["workers"].items():
                seen[key].add(worker["uptime_s"])
        return {key: len(values) for key, values in seen.items()}


def idle_heap_profile(job, spec, run_dir) -> dict:
    """The instrumented engine's own heap over a lifetime with no input.

    An engine allocates while it starts, configures itself and reports its
    telemetry, and those bytes belong to no record. This control lifetime
    measures them under the same build and configuration, so the traffic
    lifetime's profile can be reported above its own idle baseline instead
    of charging start-up to the workload.
    """
    root = Path(run_dir) / "engine-idle"
    root.mkdir(parents=True, exist_ok=True)
    engine = test_e2e.Engine(
        root,
        interval="15s",
        topology="noop",
        cores=list(spec.cores),
        binary=Path(job["build"]["binary"]),
        launcher=DirectoryLauncher(root),
    )
    sampler = NoopSampler(engine)
    try:
        _ = measurement.wait_until(
            lambda: sampler.once(),
            lambda sample: bool(sample)
            and all(worker["uptime_s"] > 0 for worker in sample["workers"].values()),
            deadline_ns=time.monotonic_ns() + PIPELINE_READY_DEADLINE_S * 10**9,
            description="the idle control engine answering a collection",
        )
        _ = sampler.start()
        _ = measurement.wait_until(
            lambda: min(sampler.epochs().values(), default=0),
            lambda count: count >= PIPELINE_EPOCHS,
            deadline_ns=time.monotonic_ns() + PIPELINE_DRAIN_DEADLINE_S * 10**9,
            description=f"{PIPELINE_EPOCHS} answered collections with no input",
        )
        sampler.stop()
        engine.shutdown(SHUTDOWN_DEADLINE_S)
        _ = measurement.wait_until(
            lambda: engine.process.poll(),
            lambda code: code is not None,
            deadline_ns=time.monotonic_ns() + 120 * 10**9,
            description="the idle control engine writing its profile",
        )
    finally:
        sampler.stop()
        engine.close()
    profile = root / "dhat-heap.json"
    if not profile.is_file():
        raise AssertionError(
            f"the idle control engine wrote no heap profile at {profile}"
        )
    totals = dhat_totals(profile)
    totals["samples"] = len(sampler.samples)
    return totals


def pipeline_experiment(plan, job, spec, result, run_dir, controls):
    """The `otlp_noop` pipeline baseline: a real engine and the real sender.

    The engine runs the OTLP receiver connected to the always-enabled noop
    exporter with acknowledgements on, and the harness's own producer sends
    exactly the requests of this workload's input, once each. It calibrates
    producer and engine cost; it stores nothing and is never a durable
    write measurement.
    """
    run_dir = Path(run_dir)
    controls.allocate(plan["allocation"])
    controls.register("harness", os.getpid())
    result["environment"]["build"] = job["build"]
    result["environment"]["git"] = plan["git"]
    sidecar = plan["inputs"][job["config_id"]]["sidecar"]
    workload = WORKLOAD_CONFIGS[job["config_id"]]["workload"]
    engine_root = run_dir / "engine"
    engine_root.mkdir(parents=True, exist_ok=True)
    # The control lifetime runs first and alone: two engines never share
    # the measured core.
    idle_totals = (
        idle_heap_profile(job, spec, run_dir)
        if job["profile"] == "pipeline_heap"
        else None
    )
    engine = test_e2e.Engine(
        engine_root,
        interval="15s",
        topology="noop",
        cores=list(spec.cores),
        binary=Path(job["build"]["binary"]),
        launcher=DirectoryLauncher(engine_root),
    )
    ledger = measurement.Ledger(run_dir / "ledger.sqlite")
    sampler = NoopSampler(engine)
    try:
        result["ephemeral_values"] = {
            "<receiver_listening_addr>": f"127.0.0.1:{engine.grpc_port}"
        }
        result["config"]["effective"] = engine.config
        result["config"]["effective_sha256"] = engine.config_sha256
        result["config"]["edges"] = [list(edge) for edge in engine.edges]
        result["config"]["input"] = {
            key: sidecar[key]
            for key in ("format", "signal", "requests", "records",
                        "expected_series", "sha256", "bytes")
        }
        ready = measurement.wait_until(
            lambda: sampler.once(),
            lambda sample: bool(sample)
            and all(worker["uptime_s"] > 0 for worker in sample["workers"].values()),
            deadline_ns=time.monotonic_ns() + PIPELINE_READY_DEADLINE_S * 10**9,
            description="the noop pipeline's worker answering a collection",
        )
        workers = measurement.worker_identities(ready)
        snapshot = controls.snapshot(
            "start", {"engine": (engine.pid, list(spec.cores))}, workers=workers,
            requested_cores=list(spec.cores),
        )
        controls.watch_workers(engine.pid, snapshot)
        sampler.start()
        reset = reset_peak_rss(engine.pid)
        cpu_before = procfs_cpu_ns(engine.pid)
        rss_before = test_e2e.rss_bytes(engine.pid)
        producer = measurement.Producer(
            engine.channel,
            ledger,
            workload,
            cores=plan["allocation"].get("producer", []),
            timeout_s=spec.producer_timeout_s,
            max_in_flight=spec.max_in_flight,
        )
        controls.raise_if_invalid()
        outcome = producer.send(sidecar["request_indexes"])
        controls.raise_if_invalid()
        cpu_after = procfs_cpu_ns(engine.pid)
        peak_rss = procfs_peak_rss(engine.pid)
        _ = measurement.wait_until(
            lambda: min(sampler.epochs().values(), default=0),
            lambda count: count >= PIPELINE_EPOCHS,
            deadline_ns=time.monotonic_ns() + PIPELINE_DRAIN_DEADLINE_S * 10**9,
            description=f"{PIPELINE_EPOCHS} answered collections of every worker",
        )
        _ = controls.snapshot(
            "end", {"engine": (engine.pid, list(spec.cores))}, workers=workers,
            requested_cores=list(spec.cores),
        )
        controls.unwatch_workers()
        sampler.stop()
        engine.shutdown(SHUTDOWN_DEADLINE_S)
        _ = measurement.wait_until(
            lambda: engine.process.poll(),
            lambda code: code is not None,
            deadline_ns=time.monotonic_ns() + 120 * 10**9,
            description="the engine writing its profile and exiting",
        )
    finally:
        controls.unwatch_workers()
        sampler.stop()
        engine.close()
    counts = ledger.counts()
    latencies = ledger.acknowledgement_latencies_s()
    ledger.close()
    records = counts["records_acked_count"]
    duration_ns = outcome["finished_ns"] - outcome["started_ns"]
    cpu_ns = cpu_after - cpu_before
    delivered = (
        counts["requests_acked_count"] == counts["requests_attempted_count"]
        == sidecar["requests"]
        and records == sidecar["records"]
        and outcome["outcomes"].get(measurement.OUTCOME_PARTIAL, 0) == 0
    )
    epochs = sampler.epochs()
    observations = {
        "ledger": counts,
        "outcomes": outcome,
        "answered_epochs_by_worker": epochs,
        "sampler_errors": sampler.errors,
        "engine_cpu_ns": cpu_ns,
        "engine_rss_before_bytes": rss_before,
        "engine_peak_rss_bytes": peak_rss,
        "engine_rss_growth_bytes": max(0, peak_rss - rss_before),
        "peak_rss_was_reset": reset,
        "input_duration_s": duration_ns / 1e9,
        "ack_latency_p50_s": measurement.percentile(latencies, 0.50) if latencies else None,
        "ack_latency_p99_s": measurement.percentile(latencies, 0.99) if latencies else None,
        "output_representation": "none",
        "descriptor_oracle": "not_applicable: the noop exporter stores nothing",
    }
    if idle_totals is not None:
        observations["dhat_idle"] = idle_totals
    if job["profile"] == "pipeline_heap":
        idle = observations["dhat_idle"]
        profile_path = engine_root / "dhat-heap.json"
        if not profile_path.is_file():
            raise AssertionError(
                f"the instrumented engine wrote no heap profile at "
                f"{profile_path}; missing profiling data is incomplete "
                f"acceptance, never a zero"
            )
        totals = dhat_totals(profile_path)
        observations["dhat"] = totals
        # The whole instrumented lifetime is the measurement: an engine
        # allocates while it starts and reports, and those bytes are the
        # same in every repetition of this workload, so the totals are
        # comparable while a difference of two noisy lifetimes is not. The
        # idle control lifetime is recorded beside them, with the
        # difference, so a reader can see the start-up share.
        metrics = {
            "allocated_bytes_per_record": totals["total_bytes"] / records,
            "peak_live_heap_bytes": totals["peak_bytes"],
            # The transient part of the peak: what the heap held at its
            # peak and no longer held when the engine stopped.
            "peak_workspace_bytes": max(0, totals["peak_bytes"] - totals["end_bytes"]),
        }
        observations["allocated_above_idle_bytes_per_record"] = (
            max(0, totals["total_bytes"] - idle["total_bytes"]) / records
        )
        observations["peak_above_idle_bytes"] = max(
            0, totals["peak_bytes"] - idle["peak_bytes"]
        )
        samples_ok = records > 0
        samples_detail = f"{totals['program_points']} program points profiled"
    else:
        metrics = {
            "records_per_s_per_core": (
                records / (cpu_ns / 1e9) if cpu_ns > 0 and records else None
            ),
            "cpu_ns_per_record": cpu_ns / records if cpu_ns > 0 and records else None,
            "wall_ns_per_record": duration_ns / records if records else None,
            "peak_rss_bytes": peak_rss,
            # The noop exporter stores nothing: this is a measured zero
            # with an explicit representation, not an unmeasured field.
            "output_bytes_per_input_record": 0.0,
        }
        samples_ok = (
            counts["requests_acked_count"] >= MINIMUM_SAMPLES
            and min(epochs.values(), default=0) >= PIPELINE_EPOCHS
            and not sampler.errors
            and cpu_ns > 0
            and records > 0
        )
        samples_detail = (
            f"{counts['requests_acked_count']} acknowledged requests, "
            f"{epochs} answered epochs, {len(sampler.samples)} samples, "
            f"{cpu_ns} ns of engine CPU over {records} records"
        )
    record_stage_checks(
        result,
        delivered=delivered,
        detail=(
            f"acked {counts['requests_acked_count']}/{sidecar['requests']} "
            f"requests and {records}/{sidecar['records']} records with "
            f"{outcome['outcomes']} outcomes"
        ),
        samples_ok=samples_ok,
        samples_detail=samples_detail,
    )
    result["samples"] = [
        {
            "monotonic_ns": sample["monotonic_ns"],
            "process_rss_bytes": sample["process_rss_bytes"],
            "uptime_s": [
                worker["uptime_s"] for worker in sorted(sample["workers"].values(), key=lambda w: w["key"])
            ],
        }
        for sample in sampler.samples
    ]
    settle_child(
        result, job, metrics, observations,
        artifacts=[
            dict(measurement.file_entry(path), kind=kind, retention=str(run_dir))
            for path, kind in (
                (engine_root / "engine.log", "engine_log"),
                (engine_root / "pipeline.yaml", "engine_config"),
                (engine_root / "dhat-heap.json", "dhat_profile"),
            )
            if path.is_file()
        ],
    )


# --------------------------------------------------------------------------
# Aggregating repetitions and profiles
# --------------------------------------------------------------------------


def child_key(child) -> tuple:
    """The identity a child is joined on."""
    return (
        child["stage"],
        child["mode"],
        child["workload_config_id"],
        child["compression"],
        child["repetition"],
    )


def rss_reconciliation(timing_children, heap_children) -> dict:
    """Reconcile measured resident growth with the profiled heap peak.

    The timing child and the heap child of one repetition run the same
    stage on the same input with the same configuration, so the resident
    growth of the timing process and the heap workspace DHAT saw must agree
    within the frozen diagnostic tolerance. A residual outside it is
    unexplained and fails the run; it is never explained away by
    subtracting a reservation.
    """
    by_repetition = {child["repetition"]: child for child in heap_children}
    residuals = []
    for timing in timing_children:
        heap = by_repetition.get(timing["repetition"])
        if heap is None:
            residuals.append(
                {"repetition": timing["repetition"], "reason": "no paired heap child"}
            )
            continue
        report = (timing.get("observations") or {}).get("bench_report") or {}
        resident = report.get("resident") or {}
        steady = report.get("resident_steady") or {}
        growth = steady.get("growth_bytes")
        peak = resident.get("peak_bytes")
        workspace = (heap.get("metrics") or {}).get("peak_workspace_bytes")
        if growth is None or peak is None or workspace is None:
            residuals.append(
                {
                    "repetition": timing["repetition"],
                    "reason": "a child of this pair measured nothing to reconcile",
                }
            )
            continue
        residual = growth - workspace
        tolerance = max(
            measurement.RESIDUAL_FLOOR_BYTES,
            int(measurement.RESIDUAL_PEAK_FRACTION * peak),
        )
        residuals.append(
            {
                "repetition": timing["repetition"],
                "steady_rss_growth_bytes": growth,
                "loop_rss_growth_bytes": resident.get("growth_bytes"),
                "peak_rss_bytes": peak,
                "heap_workspace_bytes": workspace,
                "residual_bytes": residual,
                "tolerance_bytes": tolerance,
                # A negative residual is explained: the fixtures the stage
                # was given already made those pages resident, so the
                # allocator satisfies the stage from memory it holds. Only
                # resident growth the profiled heap does not account for is
                # unexplained.
                "within_tolerance": residual <= tolerance,
                "explained_negative": residual < 0,
            }
        )
    unexplained = [
        entry
        for entry in residuals
        if not entry.get("within_tolerance", False)
    ]
    return {
        "residuals": residuals,
        "check": measurement.check(
            "rss_reconciliation",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if unexplained or not residuals
            else measurement.STATUS_PASSED,
            json.dumps(unexplained[:3] if unexplained else residuals[:1], sort_keys=True),
        ),
    }


def pipeline_reconciliation(pipeline_children, heap_children) -> dict:
    """The same reconciliation for the real engine baseline."""
    by_repetition = {child["repetition"]: child for child in heap_children}
    residuals = []
    for child in pipeline_children:
        heap = by_repetition.get(child["repetition"])
        if heap is None:
            residuals.append(
                {"repetition": child["repetition"], "reason": "no paired heap child"}
            )
            continue
        observations = child.get("observations") or {}
        growth = observations.get("engine_rss_growth_bytes")
        peak = observations.get("engine_peak_rss_bytes")
        workspace = (heap.get("metrics") or {}).get("peak_workspace_bytes")
        if growth is None or peak is None or workspace is None:
            residuals.append(
                {
                    "repetition": child["repetition"],
                    "reason": "a child of this pair measured nothing to reconcile",
                }
            )
            continue
        residual = growth - workspace
        tolerance = max(
            measurement.RESIDUAL_FLOOR_BYTES,
            int(measurement.RESIDUAL_PEAK_FRACTION * peak),
        )
        residuals.append(
            {
                "repetition": child["repetition"],
                "rss_growth_bytes": growth,
                "peak_rss_bytes": peak,
                "heap_workspace_bytes": workspace,
                "residual_bytes": residual,
                "tolerance_bytes": tolerance,
                "within_tolerance": residual <= tolerance,
                "explained_negative": residual < 0,
            }
        )
    unexplained = [entry for entry in residuals if not entry.get("within_tolerance", False)]
    return {
        "residuals": residuals,
        "check": measurement.check(
            "rss_reconciliation",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if unexplained or not residuals
            else measurement.STATUS_PASSED,
            json.dumps(unexplained[:3] if unexplained else residuals[:1], sort_keys=True),
        ),
    }


def derived_checks(children) -> list:
    """Each hard gate of the family, derived from every child's own gate.

    A gate is passed only when every child recorded it and passed it: a
    child that never checked its environment has not shown that it held.
    """
    checks = []
    for name in measurement.REQUIRED_HARD_CHECKS:
        if name == "rss_reconciliation":
            continue
        statuses = {}
        for child in children:
            recorded = [
                entry["status"]
                for entry in child["checks"]
                if entry["name"] == name and entry["kind"] == measurement.CHECK_HARD
            ]
            statuses[child["run_id"]] = recorded or ["absent"]
        failed = sorted(
            run_id
            for run_id, recorded in statuses.items()
            if any(status != measurement.STATUS_PASSED for status in recorded)
        )
        checks.append(
            measurement.check(
                name,
                measurement.CHECK_HARD,
                measurement.STATUS_FAILED if failed else measurement.STATUS_PASSED,
                f"{len(children)} children; not passed in {failed[:5]}"
                if failed
                else f"passed in all {len(children)} children",
            )
        )
    return checks


def aggregate_profile(children, *, plan, output_dir, reconciliation) -> dict:
    """One profile's repetitions of one stage, as a comparable result.

    The metrics are the medians of the repetitions; their dispersion is a
    stability check, not a regression allowance. The Controller baseline
    policy is applied here, to the family, rather than to each repetition.
    """
    children = sorted(children, key=lambda child: child["repetition"])
    first = children[0]
    profile = first["profile"]
    run_id = (
        f"stages-{first['stage']}-{first['mode']}-{first['workload_config_id']}"
        f"-{first['compression']}-{profile}-f{first['family_ordinal']:03d}"
    )
    result = measurement.new_result(
        {"run_id": run_id, "case": first["case"]}, artifact_kind="stage_aggregate"
    )
    result["environment"] = {
        "start": first["environment"]["start"],
        "end": children[-1]["environment"]["end"],
        "build": first["environment"]["build"],
        "git": first["environment"]["git"],
        "core_allocation": first["environment"]["core_allocation"],
        "machine_identity_sha256": first["environment"].get("machine_identity_sha256"),
        "children": [child["run_id"] for child in children],
    }
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"]
    )
    result["config"] = first["config"]
    result["workload"] = first["workload"]
    result["workload_schedule"] = first["workload_schedule"]
    result["ephemeral_values"] = first.get("ephemeral_values") or {}
    result["run_dir"] = str(output_dir)
    result["stage"] = first["stage"]
    result["mode"] = first["mode"]
    result["profile"] = profile
    result["compression"] = first["compression"]
    result["workload_config_id"] = first["workload_config_id"]
    names = PROFILE_METRICS[profile]
    by_metric = {
        name: [
            child["metrics"][name]
            for child in children
            if _finite((child.get("metrics") or {}).get(name))
        ]
        for name in names
    }
    result["metrics"] = {
        name: (median(values) if values else None)
        for name, values in by_metric.items()
    }
    for name, value in result["metrics"].items():
        if value is None:
            result["metrics_unavailable"][name] = (
                "no repetition of this profile measured it"
            )
    result["metric_directions"] = {name: METRIC_DIRECTIONS[name] for name in names}
    result["mandatory_metrics"] = sorted(names)
    unstable = sorted(
        name
        for name, values in by_metric.items()
        if not values or coefficient_of_variation(values) > MAXIMUM_CV
    )
    checks = derived_checks(children)
    checks.append(reconciliation["check"])
    checks.append(
        measurement.check(
            "repetition_stability",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED
            if unstable or len(children) < REPETITIONS
            else measurement.STATUS_PASSED,
            f"{len(children)} repetitions; coefficients of variation "
            + json.dumps(
                {
                    name: round(coefficient_of_variation(values), 4)
                    for name, values in sorted(by_metric.items())
                },
                sort_keys=True,
            )
            + (f"; over {MAXIMUM_CV:.0%} in {unstable}" if unstable else ""),
        )
    )
    result["checks"] = checks
    result["observations"] = {
        "repetitions": [
            {
                "repetition": child["repetition"],
                "run_id": child["run_id"],
                "status": child["status"],
                "metrics": child["metrics"],
            }
            for child in children
        ],
        "dispersion": {
            name: {
                "median": median(values) if values else None,
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "coefficient_of_variation": coefficient_of_variation(values),
            }
            for name, values in sorted(by_metric.items())
        },
        "rss_reconciliation": reconciliation["residuals"],
    }
    result["run_files"] = [
        measurement.file_entry(Path(output_dir) / f"{child['run_id']}.json")
        for child in children
    ]
    result["status"] = (
        measurement.STATUS_PASSED
        if all(entry["status"] == measurement.STATUS_PASSED for entry in checks)
        else measurement.STATUS_FAILED
    )
    try:
        decision = measurement.evaluate_baseline(result)
        if decision["action"] == "created":
            candidate = result.pop("baseline_candidate")
            path = measurement.write_published_json(
                Path(output_dir) / decision["baseline_name"], candidate
            )
            result["baseline_files"].append(measurement.file_entry(path))
    except AssertionError as error:
        result["status"] = measurement.STATUS_FAILED
        measurement.record_event(result, "baseline_policy_failed", str(error))
        result["checks"].append(
            measurement.check(
                "baseline_policy",
                measurement.CHECK_HARD,
                measurement.STATUS_FAILED,
                str(error),
            )
        )
    result.pop("baseline_candidate", None)
    _ = measurement.write_result(Path(output_dir) / f"{run_id}.json", result)
    return result


def composite_stage_results(aggregates, children) -> list:
    """One registered stage result per repetition, with the full schema.

    Each metric comes from the child that measured it, named in
    `metric_sources`; a composite missing any mandatory measurement is
    rejected by `validate_stage_result`, never completed with a zero.
    """
    by_profile = collections.defaultdict(dict)
    for child in children:
        by_profile[child_key(child)[:4]][
            (child["profile"], child["repetition"])
        ] = child
    fingerprints = {
        (
            aggregate["stage"],
            aggregate["mode"],
            aggregate["workload_config_id"],
            aggregate["compression"],
            aggregate["profile"],
        ): aggregate.get("baseline_decision", {}).get("fingerprint")
        for aggregate in aggregates
    }
    results = []
    for key, members in sorted(by_profile.items()):
        stage, mode, config_id, compression = key
        sources = COMPOSITE_SOURCES[mode]
        repetitions = sorted({repetition for _, repetition in members})
        for repetition in repetitions:
            metrics = {}
            metric_sources = {}
            sample_counts = {}
            for name in STAGE_METRICS:
                profile = sources[name]
                child = members.get((profile, repetition))
                if child is None:
                    continue
                value = (child.get("metrics") or {}).get(name)
                if not _finite(value):
                    continue
                metrics[name] = value
                metric_sources[name] = child["run_id"]
                sample_counts[profile] = child.get("observations", {}).get(
                    "bench_report", {}
                ).get("sample_count") or child.get("observations", {}).get(
                    "criterion", {}
                ).get("sample_count") or len(child.get("samples") or [])
            noop = stage == "otlp_noop" and mode == "pipeline"
            timing_child = (
                members.get(("timing", repetition))
                or members.get(("pipeline", repetition))
                or {}
            )
            composite = {
                "stage": stage,
                "mode": mode,
                "workload_config_id": config_id,
                "compression": compression,
                "repetition": repetition,
                "sample_count": min(sample_counts.values()) if sample_counts else 0,
                "sample_counts_by_profile": sample_counts,
                "metrics": metrics,
                "metric_sources": metric_sources,
                "fingerprint": {
                    profile: fingerprints.get((stage, mode, config_id, compression, profile))
                    for profile, _ in sorted({(p, r) for p, r in members})
                },
                "output_representation": (
                    "none"
                    if noop
                    else (
                        timing_child.get("observations", {})
                        .get("bench_report", {})
                        .get("observation", {})
                        .get("representation", "none")
                    )
                ),
                # What the stage was handed, what its per-record metrics
                # divide by, and -- where the input is already encoded --
                # the rates that input actually has. A reader never has to
                # guess the denominator of a per-record number.
                "input_representation": INPUT_REPRESENTATIONS[stage],
                "denominator": DENOMINATOR,
                "rates": stage_rates(stage, timing_child),
                "completion_semantics": (
                    timing_child.get("observations", {})
                    .get("bench_report", {})
                    .get("observation", {})
                    .get("extra", {})
                    .get("completion_semantics")
                ),
                "descriptor_oracle": (
                    "not_applicable: the noop pipeline stores nothing"
                    if stage == "otlp_noop"
                    else "checked: every stage output was read back and counted"
                ),
                "cores": timing_child.get("config", {})
                .get("requested", {})
                .get("cores", []),
                "workload": timing_child.get("workload", {}),
            }
            try:
                validate_stage_result(composite)
            except AssertionError as error:
                composite["incomplete"] = str(error)
            results.append(composite)
    return results


def stage_summaries(stage_results) -> list:
    """Medians and ranges of every registered stage result's repetitions."""
    grouped = collections.defaultdict(list)
    for result in stage_results:
        grouped[
            (
                result["stage"],
                result["mode"],
                result["workload_config_id"],
                result["compression"],
            )
        ].append(result)
    summaries = []
    for key, members in sorted(grouped.items()):
        stage, mode, config_id, compression = key
        values = {
            name: [
                member["metrics"][name]
                for member in members
                if _finite(member["metrics"].get(name))
            ]
            for name in STAGE_METRICS
        }
        summaries.append(
            {
                "stage": stage,
                "mode": mode,
                "workload_config_id": config_id,
                "compression": compression,
                "repetitions": len(members),
                "input_representation": members[0]["input_representation"],
                "output_representation": members[0]["output_representation"],
                "denominator": members[0]["denominator"],
                "rates": {
                    name: median(
                        [
                            member["rates"][name]
                            for member in members
                            if _finite((member.get("rates") or {}).get(name))
                        ]
                        or [0.0]
                    )
                    for name in sorted(
                        {key for member in members for key in (member.get("rates") or {})}
                    )
                },
                "completion_semantics": members[0].get("completion_semantics"),
                "descriptor_oracle": members[0]["descriptor_oracle"],
                "metrics": {
                    name: {
                        "median": median(series) if series else None,
                        "min": min(series) if series else None,
                        "max": max(series) if series else None,
                        "coefficient_of_variation": coefficient_of_variation(series),
                        "repetitions_measured": len(series),
                    }
                    for name, series in sorted(values.items())
                },
            }
        )
    return summaries


# --------------------------------------------------------------------------
# The family
# --------------------------------------------------------------------------

# The store the object-store stages write to. MinIO is recovered, never
# started by hand, by the existing container helper.
STORE_KIND = "minio"

# Where the instrumented engine is, unless the environment names another.
DHAT_ENGINE = "target/profiling/df_engine"


def engine_binaries() -> dict:
    """The two engines the pipeline baseline needs, release and profiled."""
    release = Path(
        os.environ.get("DF_ENGINE", test_e2e.WORKSPACE / "target/release/df_engine")
    )
    profiled = Path(
        os.environ.get("SERIES_DHAT_ENGINE", test_e2e.WORKSPACE / DHAT_ENGINE)
    )
    for binary, how in (
        (release, "cargo build --release --locked -p otel-arrow-dfe --bin df_engine "
                  "--features series-parquet,aws,durable-buffer"),
        (profiled, "cargo build --profile profiling --no-default-features -p "
                   "otel-arrow-dfe --bin df_engine --features "
                   "core-nodes,crypto-ring,dhat-heap"),
    ):
        if not binary.is_file():
            raise AssertionError(
                f"the pipeline baseline needs {binary}; build it first with: {how}"
            )
    release_build = measurement.engine_build(release)
    if release_build["profile"] != "release":
        raise AssertionError(
            f"the pipeline baseline measures a release engine, not a "
            f"{release_build['profile']} one"
        )
    profiled_build = measurement.engine_build(profiled)
    profiled_build.update(
        {
            "profile": "profiling",
            "features": "core-nodes,crypto-ring,dhat-heap",
            "allocator": "dhat",
        }
    )
    return {"pipeline": release_build, "pipeline_heap": profiled_build}


def plan_jobs(plan, config_id, stage, repetition) -> list:
    """Every process one stage of one workload needs for one repetition."""
    ordinal = (plan["family_ordinal"] - 1) * plan["repetitions"] + repetition
    store = STORE_KIND if stage in ("otlp_minio", "upload") else "local"
    mode = stage_mode(stage)
    compressions = ("zstd", "none") if stage == "encode" else ("zstd",)
    jobs = []
    if stage == "otlp_noop" and config_id in PRIMARY_CONFIGS:
        for profile in ("pipeline", "pipeline_heap"):
            jobs.append(
                {
                    "stage": stage, "mode": "pipeline", "profile": profile,
                    "compression": "zstd", "config_id": config_id,
                    "repetition": repetition, "ordinal": ordinal,
                    "store": "local", "iterations": 1,
                    "build": plan["engines"][profile],
                    "executable": plan["engines"][profile]["binary"],
                }
            )
    for compression in compressions:
        if stage in CRITERION_LAYERS:
            jobs.append(
                {
                    "stage": stage, "mode": mode, "profile": "criterion",
                    "compression": compression, "config_id": config_id,
                    "repetition": repetition, "ordinal": ordinal,
                    "store": store, "iterations": 30,
                    "build": plan["benches"]["layered"]["build"],
                    "executable": plan["benches"]["layered"]["executable"],
                }
            )
        for profile, iterations, key in (
            ("timing", MINIMUM_SAMPLES, "measurement"),
            ("heap", 1, "measurement_heap"),
        ):
            jobs.append(
                {
                    "stage": stage, "mode": mode, "profile": profile,
                    "compression": compression, "config_id": config_id,
                    "repetition": repetition, "ordinal": ordinal,
                    "store": store, "iterations": iterations,
                    "build": plan["benches"][key]["build"],
                    "executable": plan["benches"][key]["executable"],
                }
            )
    return jobs


def run_child(plan, job, output_dir, report_dir):
    """One child measurement, whose failure is recorded, never raised."""
    try:  # Imported here: the command line imports this module in turn.
        from . import measure
    except ImportError:
        import measure
    spec = child_spec(plan, job)
    run_dir = Path(output_dir) / spec.run_id

    def experiment(spec, result, directory, controls):
        """This child's measured body."""
        if job["profile"].startswith("pipeline"):
            pipeline_experiment(plan, job, spec, result, directory, controls)
        elif job["profile"] == "criterion":
            criterion_experiment(plan, job, spec, result, directory, controls)
        else:
            bench_experiment(plan, job, spec, result, directory, controls)

    try:
        result = measure.run_case(
            spec, run_dir, experiment=experiment, report_dir=report_dir,
            evaluate=False,
        )
    except (KeyboardInterrupt, SystemExit):
        # An operator's interrupt or an exit ends the family, not the child:
        # recording it as one failed child would run every remaining child.
        raise
    except BaseException as error:  # noqa: BLE001 - recorded in the result
        sys.stderr.write(f"{spec.run_id}: {type(error).__name__}: {error}\n")
        result = json.loads((run_dir / f"{spec.run_id}.json").read_text(encoding="ascii"))
    for entry in [{"name": f"{spec.run_id}.json"}] + result.get("baseline_files", []):
        _ = shutil.copyfile(
            run_dir / entry["name"], Path(output_dir) / entry["name"]
        )
    for field, value in (
        ("stage", job["stage"]), ("mode", job["mode"]), ("profile", job["profile"]),
        ("compression", job["compression"]),
        ("workload_config_id", job["config_id"]), ("repetition", job["repetition"]),
        ("family_ordinal", plan["family_ordinal"]),
    ):
        result.setdefault(field, value)
    return result


def pin_container(container, cores) -> bool:
    """Confine a store container to its own cores, or say it could not be."""
    if not cores:
        return False
    done = subprocess.run(
        ["docker", "update", "--cpuset-cpus", ",".join(str(core) for core in cores),
         container],
        capture_output=True, text=True,
    )
    return done.returncode == 0


def container_pid(container):
    """The host PID of a container's main process."""
    done = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Pid}}", container],
        capture_output=True, text=True,
    )
    if done.returncode != 0:
        return None
    return int(done.stdout.strip() or 0) or None


def run_stages(spec: measurement.RunSpec, output_dir, report_dir=None, **options) -> dict:
    """Measure every registered stage and publish `stages.json`.

    One process per stage, repetition and profile; each one holds the host
    lease and is watched by one build monitor. The children are combined
    into one stage result per repetition, with the full metric schema and
    the child that measured each field, and into one summary per stage with
    medians and ranges.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    configs = list(options.get("configs") or WORKLOAD_CONFIGS)
    repetitions = int(options.get("repetitions", REPETITIONS))
    stages_filter = options.get("stages")
    scope = family_scope(configs, stages_filter, options.get("index_name"))
    for config_id in configs:
        check_fixture(config_id)
    # Everything below is prebuilt. This family never invokes cargo: it
    # locates executables that already exist and refuses to start when one
    # is missing, because a compiler running beside a measurement is
    # exactly what the build monitor exists to catch.
    git = measurement.git_provenance()
    # Discovery, not a build: both calls only ask already built executables
    # what they are, so no compiler ever runs inside this family.
    timing = locate_benches()
    heap = locate_benches(features=("bench-heap",))
    plan = {
        "git": git,
        # The host as the family found it, before its first child.
        "host_at_start": host_neighbours(exclude=(os.getpid(),)),
        "benches": {
            "measurement": {
                "executable": timing["measurement"]["executable"],
                "build": bench_build(timing["measurement"]),
            },
            "measurement_heap": {
                "executable": heap["measurement"]["executable"],
                "build": bench_build(heap["measurement"]),
            },
            "layered": {
                "executable": timing["layered"]["executable"],
                "build": bench_build(timing["layered"]),
            },
        },
        "inputs": {},
        "ephemeral": {},
    }
    plan["engines"] = engine_binaries()
    plan["repetitions"] = repetitions
    plan["scope"] = scope
    plan["family_ordinal"] = family_ordinal(report_dir)
    topology = measurement.core_topology()
    # A stages family runs the engine (its pipeline baseline), the producer
    # and the store, but no reader, so it claims no reader core.
    allocation = measurement.role_allocation(
        topology["sibling_groups"], sorted(os.sched_getaffinity(0)), spec.cores,
        roles=measurement.CASE_ROLES["stages"],
    )
    # The benchmark process takes the core the engine's worker would have.
    allocation["bench"] = allocation.pop("engine")
    allocation["bench_reserved"] = allocation.pop("engine_reserved", [])
    plan["allocation"] = allocation
    plan["bench_cores"] = list(spec.cores)
    plan["store_cores"] = allocation.get("store", [])
    inputs_dir = output_dir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)
    for config_id in configs:
        config = WORKLOAD_CONFIGS[config_id]
        path = inputs_dir / f"{config_id}.otlp"
        sidecar = write_stage_input(config["workload"], config["signal"], path)
        plan["inputs"][config_id] = {"path": path, "sidecar": sidecar}
    needs_store = any(
        stage in ("otlp_minio", "upload")
        for config_id in configs
        for stage in (stages_filter or WORKLOAD_CONFIGS[config_id]["stages"])
    )
    children = []
    store = test_e2e.DockerStore(STORE_KIND) if needs_store else None
    try:
        if store is not None:
            _ = store.__enter__()
            plan["store_storage"] = dict(store.storage)
            plan["store_pid"] = container_pid(store.container)
            plan["store_pinned"] = pin_container(store.container, plan["store_cores"])
            plan["ephemeral"]["<store_endpoint>"] = store.endpoint
        for config_id in configs:
            config = WORKLOAD_CONFIGS[config_id]
            for stage in config["stages"]:
                if stages_filter and stage not in stages_filter:
                    continue
                for repetition in range(1, repetitions + 1):
                    for job in plan_jobs(plan, config_id, stage, repetition):
                        children.append(run_child(plan, job, output_dir, report_dir))
    finally:
        if store is not None:
            store.__exit__(None, None, None)
    return publish_stages(
        spec, children, plan, output_dir, report_dir, started, configs, repetitions
    )


# The index of the complete family of record. A family that measures only
# some stages or workloads is a spot family: it publishes under an index
# name of its own, so it can never replace the family of record.
FULL_FAMILY_INDEX = "stages"


def family_scope(configs, stages_filter, index_name=None) -> dict:
    """What one stages family measures and the index it publishes.

    Every stage and workload is the full family, published as
    `stages.json`. A filtered family is a spot family: it must name an
    index of its own, and its coverage checks cover what it asked for
    rather than failing on what it deliberately left out.
    """
    configs = list(configs)
    unknown = sorted(set(configs) - set(WORKLOAD_CONFIGS))
    if unknown:
        raise AssertionError(f"unknown workload configurations {unknown}")
    stages = list(stages_filter or [])
    unknown = sorted(set(stages) - set(STAGES))
    if unknown:
        raise AssertionError(f"unknown stages {unknown}; registered are {list(STAGES)}")
    full = not stages and set(configs) == set(WORKLOAD_CONFIGS)
    name = index_name or FULL_FAMILY_INDEX
    _ = measurement.safe_json_name(f"{name}.json")
    if not full and name == FULL_FAMILY_INDEX:
        raise AssertionError(
            f"a family filtered to stages {stages or 'all'} and workloads "
            f"{configs} is a spot family; it publishes under its own index, "
            f"never as {FULL_FAMILY_INDEX}.json: pass --option "
            f"index_name=stages-spot"
        )
    if full and name != FULL_FAMILY_INDEX:
        raise AssertionError(
            f"the complete family is the family of record and publishes as "
            f"{FULL_FAMILY_INDEX}.json, not {name}.json"
        )
    requested = [
        stage
        for stage in STAGES
        if any(
            stage in WORKLOAD_CONFIGS[config_id]["stages"] for config_id in configs
        )
        and (not stages or stage in stages)
    ]
    return {
        "kind": "full" if full else "spot",
        "index": name,
        "configs": configs,
        "stages": requested,
    }


def host_neighbours(proc_root="/proc", exclude=(), limit=5) -> dict:
    """The host's load and its heaviest other processes, with their cores.

    A family shares its host with whatever else runs there. This records
    the load average, the family's own affinity, and the `limit` processes
    outside `exclude` with the most accumulated CPU time, each with the
    cores it may run on -- so a result says where a neighbour ran, not only
    that the load was high.
    """
    root = Path(proc_root)
    ticks = os.sysconf("SC_CLK_TCK")
    processes = []
    for entry in root.iterdir():
        if not entry.name.isdigit() or int(entry.name) in exclude:
            continue
        try:
            stat = (entry / "stat").read_text(encoding="ascii", errors="replace")
            status = (entry / "status").read_text(encoding="ascii", errors="replace")
        except OSError:
            continue
        name, _, rest = stat.rpartition(")")
        fields = rest.split()
        cpu_s = (int(fields[11]) + int(fields[12])) / ticks
        allowed = next(
            (line.split(":", 1)[1].strip() for line in status.splitlines()
             if line.startswith("Cpus_allowed_list:")),
            "unknown",
        )
        processes.append({
            "pid": int(entry.name),
            "comm": name.partition("(")[2],
            "cpu_s": round(cpu_s, 2),
            "cpus_allowed": allowed,
        })
    processes.sort(key=lambda process: process["cpu_s"], reverse=True)
    load = (root / "loadavg").read_text(encoding="ascii").split()[:3]
    return {
        "load_average_1_5_15": [float(value) for value in load],
        "family_affinity": sorted(os.sched_getaffinity(0)),
        "heaviest_other_processes": processes[:limit],
    }


def publish_stages(spec, children, plan, output_dir, report_dir, started, configs,
                   repetitions) -> dict:
    """Aggregate every child, build the stage results and write the index."""
    output_dir = Path(output_dir)
    grouped = collections.defaultdict(lambda: collections.defaultdict(list))
    for child in children:
        key = (
            child["stage"], child["mode"], child["workload_config_id"],
            child["compression"],
        )
        grouped[key][child["profile"]].append(child)
    aggregates = []
    for key, profiles in sorted(grouped.items()):
        if key[1] == "pipeline":
            reconciliation = pipeline_reconciliation(
                profiles.get("pipeline", []), profiles.get("pipeline_heap", [])
            )
        else:
            reconciliation = rss_reconciliation(
                profiles.get("timing", []), profiles.get("heap", [])
            )
        for profile, members in sorted(profiles.items()):
            aggregates.append(
                aggregate_profile(
                    members, plan=plan, output_dir=output_dir,
                    reconciliation=reconciliation,
                )
            )
    stage_results = composite_stage_results(aggregates, children)
    summaries = stage_summaries(stage_results)
    # A plan without a scope predates spot families: it is the family of
    # record, checked against every registered stage.
    scope = plan.get("scope") or {
        "kind": "full", "index": FULL_FAMILY_INDEX, "configs": list(configs),
        "stages": list(STAGES),
    }
    index = scope["index"]
    result = measurement.new_result(
        {"run_id": index, "case": "stages"}, artifact_kind="index"
    )
    result["family_scope"] = scope
    result["environment"]["start"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    result["environment"]["build"] = {
        key: value["build"] for key, value in plan["benches"].items()
    }
    result["environment"]["engines"] = plan["engines"]
    result["environment"]["core_allocation"] = plan["allocation"]
    result["environment"]["git"] = plan["git"]
    result["environment"]["host_at_start"] = plan["host_at_start"]
    result["environment"]["host_at_end"] = host_neighbours(exclude=(os.getpid(),))
    # What the family's own setup phase starts, and what the compiler-free
    # claim every child records does and does not cover.
    result["environment"]["setup_subprocesses"] = SETUP_SUBPROCESSES
    result["family_ordinal"] = plan["family_ordinal"]
    checks = result["checks"]
    for aggregate in aggregates:
        checks.append(
            measurement.check(
                f"child_{aggregate['run_id']}",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED
                if aggregate["status"] == measurement.STATUS_PASSED
                else measurement.STATUS_FAILED,
                f"{aggregate['run_id']}: {aggregate['status']}; failed "
                + json.dumps(
                    sorted(
                        entry["name"] for entry in aggregate["checks"]
                        if entry["status"] != measurement.STATUS_PASSED
                    )
                ),
            )
        )
    covered = {result_entry["stage"] for result_entry in stage_results}
    # A spot family is checked against the stages it asked for; the full
    # family against every registered stage.
    requested = STAGES if scope["kind"] == "full" else scope["stages"]
    missing = sorted(set(requested) - covered)
    checks.append(
        measurement.check(
            "registered_stages_covered",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if missing else measurement.STATUS_PASSED,
            f"missing {missing}" if missing
            else f"{len(covered)} of the {scope['kind']} family's stages measured",
        )
    )
    layers = {
        entry["stage"] for entry in stage_results if entry["mode"] == "criterion"
    }
    wanted_layers = [layer for layer in CRITERION_LAYERS if layer in requested]
    if wanted_layers:
        missing_layers = sorted(set(wanted_layers) - layers)
        checks.append(
            measurement.check(
                "criterion_layers_registered",
                measurement.CHECK_HARD,
                measurement.STATUS_FAILED if missing_layers else measurement.STATUS_PASSED,
                f"missing {missing_layers}" if missing_layers
                else f"{len(layers)} cumulative layers",
            )
        )
    if "otlp_noop" in requested:
        modes = {
            (entry["stage"], entry["mode"])
            for entry in stage_results
            if entry["stage"] == "otlp_noop"
        }
        checks.append(
            measurement.check(
                "noop_modes_are_separate",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED
                if {("otlp_noop", "pipeline"), ("otlp_noop", "criterion")} <= modes
                else measurement.STATUS_FAILED,
                f"otlp_noop modes {sorted(mode for _, mode in modes)}",
            )
        )
    incomplete = [
        f"{entry['stage']}/{entry['mode']}/{entry['workload_config_id']} "
        f"r{entry['repetition']}: {entry['incomplete']}"
        for entry in stage_results
        if entry.get("incomplete")
    ]
    checks.append(
        measurement.check(
            "stage_schema_complete",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if incomplete else measurement.STATUS_PASSED,
            "; ".join(incomplete[:3]) or f"{len(stage_results)} stage results",
        )
    )
    result["metrics"] = {
        "stage_results_count": len(stage_results),
        "children_count": len(children),
        "children_passed_count": sum(
            1 for child in children if child["status"] == measurement.STATUS_PASSED
        ),
        "aggregates_count": len(aggregates),
        "aggregates_passed_count": sum(
            1 for aggregate in aggregates
            if aggregate["status"] == measurement.STATUS_PASSED
        ),
        "repetitions_count": repetitions,
        "workloads_count": len(configs),
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    result["stage_results"] = stage_results
    result["stage_summaries"] = summaries
    result["workloads"] = {
        config_id: {
            "description": WORKLOAD_CONFIGS[config_id]["description"],
            "signal": WORKLOAD_CONFIGS[config_id]["signal"],
            "workload": WORKLOAD_CONFIGS[config_id]["workload"].as_json(),
            "stages": list(WORKLOAD_CONFIGS[config_id]["stages"]),
            "committed_fraction": WORKLOAD_CONFIGS[config_id]["committed_fraction"],
            "input": plan["inputs"][config_id]["sidecar"],
        }
        for config_id in configs
    }
    result["children"] = [
        {
            "run_id": child["run_id"],
            "status": child["status"],
            "stage": child["stage"],
            "profile": child["profile"],
            "repetition": child["repetition"],
            "metrics": child["metrics"],
            "failed_checks": sorted(
                entry["name"] for entry in child["checks"]
                if entry["status"] != measurement.STATUS_PASSED
            ),
        }
        for child in children
    ]
    result["run_files"] = [
        measurement.file_entry(output_dir / f"{document['run_id']}.json")
        for document in children + aggregates
    ]
    result["baseline_files"] = [
        entry for aggregate in aggregates for entry in aggregate["baseline_files"]
    ]
    previous = measurement.archive_published_index(
        f"{index}.json", output_dir, report_dir
    )
    result["child_indexes"] = [previous] if previous else []
    result["environment"]["end"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"]
    )
    result["status"] = (
        measurement.STATUS_PASSED
        if all(entry["status"] == measurement.STATUS_PASSED for entry in checks)
        else measurement.STATUS_FAILED
    )
    result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
    written = measurement.write_result(output_dir / f"{index}.json", result)
    _ = measurement.publish_result_tree(written, report_dir)
    return result


def stages_spec(**options) -> measurement.RunSpec:
    """The family's own spec: one benchmark core and the primary workload."""
    try:
        from . import measure
    except ImportError:
        import measure
    cores = tuple(options.get("cores") or measure.default_engine_cores(1))
    config = WORKLOAD_CONFIGS[options.get("primary", PRIMARY_CONFIGS[0])]
    return measurement.RunSpec(
        run_id="stages",
        case="stages",
        topology="stage",
        store="minio",
        cores=cores,
        workload=config["workload"],
        interval_s=15,
        duration_s=1,
        max_in_flight=64,
        overrides={"family": "stages"},
    )


# --------------------------------------------------------------------------
# Direct CPU attribution of the real engine
# --------------------------------------------------------------------------
#
# `measure attribution` profiles a real engine with `perf record` while the
# harness's producer drives it, and assigns every sample exactly once to the
# pipeline stage of the innermost production frame on its stack. The stage
# family above supplies per-stage timing, allocation, resident memory and
# output metrics from isolated benches; attribution supplies exclusive CPU
# shares of the running engine and the flush wall time that is not CPU at
# all. The two are joined by workload configuration and never by a shared
# byte-rate denominator.

# Every category a sample can land in, `unknown` last. A sample is counted
# in exactly one of them.
CPU_CATEGORIES = (
    "conversion",
    "extraction",
    "sort_seal_merge",
    "encoding",
    "upload",
    "buffer",
    "engine_runtime",
    "allocator",
    "unknown",
)
UNKNOWN_CATEGORY = "unknown"

# Every production path may be written with or without its crate, so that a
# recorded demangled symbol and a hand-written test frame are read by the
# same rule. A real demangled symbol always starts with its crate.
_LAKE = r"(?:otel_arrow_dfe_series_lake::)?"
_NODE = r"(?:otel_arrow_dfe_core_nodes::exporters::series_parquet_exporter::)?"
_WORKER = _NODE + r"(?:worker::)?Worker::"
_END = r"(?:$|::)"

# The mapping rules, in the order they are tried on one frame. Frames are
# walked from the innermost outwards and the first frame some rule of the
# current pass matches decides the sample. Pass 1 holds the production
# namespaces -- this crate's code, the engine's own crates, and the
# libraries whose work is a stage by itself (Parquet, the object store
# client, the allocator). Pass 2 is the runtime fallback: the async runtime
# and the network server stack decide a sample only when no production
# frame is on its stack at all, so a Tokio or hyper frame inside an upload
# never takes the upload's sample, while the scheduler's own idle polling
# still lands in engine_runtime rather than in `unknown`. Library frames that
# are in neither pass -- Arrow kernels, `core`, `std`, libc copies -- are
# walked past, so their CPU belongs to the stage that called them.
CPU_RULES = (
    (1, "allocator",
     r"^(?:_rjem_|je_|(?:__rustc::)?__rust_(?:alloc|dealloc|realloc|alloc_zeroed)$|__rdl_|"
     r"tikv_jemalloc|tikv_jemallocator::|jemallocator::|malloc$|free$|"
     r"realloc$|calloc$|cfree$|_int_(?:malloc|free|realloc)$|"
     r"__libc_(?:malloc|free|realloc|calloc)$|alloc::alloc::|alloc::raw_vec::|"
     r"std::alloc::)",
     "the global allocator and heap growth, wherever it is called from; "
     "allocator_callers names the stage that called it"),
    (1, "conversion", rf"^{_WORKER}check_wire_format{_END}",
     "the exporter's OTLP framing check before conversion"),
    (1, "conversion",
     r"^otel_arrow_dfe_pdata::(?:encode|views|otlp|payload|arrays|schema|"
     r"validation)::",
     "wire-to-OTAP conversion: the OTLP views and the Arrow encoder"),
    (1, "conversion", r"^otel_arrow_dfe_pdata::otap::OtapArrowRecords::try_from",
     "the payload conversion trait itself"),
    (1, "extraction", rf"^{_WORKER}(?:prepare|extract){_END}",
     "the worker's per-request preparation around the lake extraction"),
    (1, "extraction", rf"^{_LAKE}(?:extract|canonical|attrs|value)::",
     "series/values extraction and series identity hashing"),
    (1, "sort_seal_merge",
     rf"^{_LAKE}buffer::(?:Block|SortedTableBuffer)::"
     rf"(?:seal|stamped|finalized|into_parts){_END}",
     "sealing a block: stamping and sorting its runs"),
    (1, "sort_seal_merge", rf"^{_LAKE}sort::",
     "run sorting and the k-way merge the sink consumes"),
    (1, "sort_seal_merge", rf"^{_WORKER}(?:rotate|fail_active){_END}",
     "rotating the ACTIVE block into a flush"),
    (1, "buffer", rf"^{_LAKE}(?:buffer|cache)::",
     "admission: reservation, the series cache and appending to the block"),
    (1, "buffer",
     rf"^{_WORKER}(?:admit|offer|park|resume_pending|new_active|refuse|"
     rf"reservation_failure){_END}",
     "the worker's admission path"),
    (1, "buffer",
     r"^(?:otel_arrow_dfe_core_nodes::processors::durable_buffer_processor|"
     r"otel_arrow_dfe_quiver)::",
     "the durable buffer of the buffered topology"),
    (1, "encoding", r"^parquet::",
     "Parquet encoding, including its compression and statistics"),
    (1, "encoding",
     rf"^{_LAKE}(?:schema::|sink::(?:time_range|native_sorting_columns){_END}|"
     rf"(?:sink::)?Sink::file_metadata{_END})",
     "the file schema and metadata the sink writes with each file"),
    (1, "upload", r"^(?:object_store|reqwest)::",
     "the object store client and its HTTP client"),
    (1, "upload", r"^otel_arrow_dfe_otap::object_store::",
     "the engine's object store construction"),
    (1, "upload", rf"^{_LAKE}(?:sink::|Sink::|CreationWatch::)",
     "the sink's write orchestration around encoding and the store"),
    (1, "upload", rf"^{_NODE}flush::",
     "the flush task's retry loop around the sink"),
    (1, "engine_runtime",
     r"^otel_arrow_dfe_(?:engine|channel|control_channel|telemetry|controller|"
     r"admin|admin_api|config|state|otap|core_nodes|pdata)::",
     "the engine's own crates: the receiver, channels, telemetry, and the "
     "exporter's control loop and completion routing"),
    (2, "engine_runtime",
     r"^(?:tokio|mio|futures_util|futures_core|futures_executor|"
     r"futures_channel|hyper|hyper_util|h2|http|http_body|tonic|tower|axum|"
     r"std::thread|std::sys|std::rt|std::panicking|core::ops::function)::",
     "the async runtime and the network server stack, only when no "
     "production frame is on the stack"),
    (2, "engine_runtime",
     r"^(?:__libc_start_main|__libc_start_call_main|start_thread|clone3?|"
     r"_start|main|epoll_wait|__GI_epoll_wait)$",
     "thread and process entry points, only when nothing else matched"),
)

_COMPILED_RULES = tuple(
    (pass_, category, re.compile(pattern)) for pass_, category, pattern, _ in CPU_RULES
)

# The order `frames` is read in, stated once because the classifier and the
# perf script parser must agree on it.
FRAME_ORDER = "outermost first, innermost last, as a call path reads"


def classification_rules() -> dict:
    """The mapping rules as recorded with every attribution result."""
    return {
        "categories": list(CPU_CATEGORIES),
        "frame_order": FRAME_ORDER,
        "decision": (
            "walk frames from the innermost outwards; the first frame a "
            "pass-1 rule matches decides the sample, otherwise the first "
            "frame a pass-2 rule matches, otherwise the sample is unknown; "
            "within one frame the first matching rule wins; every sample's "
            "weight is added once"
        ),
        "rules": [
            {"pass": pass_, "category": category, "pattern": pattern, "why": why}
            for pass_, category, pattern, why in CPU_RULES
        ],
    }


_SYMBOL_OFFSET = re.compile(r"\+0x[0-9a-fA-F]+$")
# The crate disambiguator a v0 symbol demangles with: `crate[4f8f0dac]::...`.
_CRATE_DISAMBIGUATOR = re.compile(r"\[[0-9a-f]{4,}\]")
_LEGACY_HASH = re.compile(r"::h[0-9a-f]{16}$")
_SELF_TYPE_PREFIX = re.compile(r"^(?:[&*\[( ]|mut |const |dyn )+")


def _strip_generics(text: str) -> str:
    """Remove every balanced `<...>` group, turbofish included."""
    out = []
    depth = 0
    for position, char in enumerate(text):
        if char == "<":
            depth += 1
        elif char == ">" and text[position - 1:position] != "-":
            depth = max(0, depth - 1)
        elif depth == 0:
            out.append(char)
    stripped = "".join(out)
    while "::::" in stripped:
        stripped = stripped.replace("::::", "::")
    return stripped.rstrip(":").strip()


def _split_self_type(inner: str) -> str:
    """The implementing type of `A as Trait`, or `A` itself."""
    depth = 0
    for position in range(len(inner)):
        char = inner[position]
        if char == "<":
            depth += 1
        elif char == ">" and inner[position - 1:position] != "-":
            depth -= 1
        elif depth == 0 and inner.startswith(" as ", position):
            return inner[:position]
    return inner


@functools.lru_cache(maxsize=65536)
def frame_path(symbol: str) -> str:
    """The module path one demangled symbol names, generics removed.

    `<otel_arrow_dfe_series_lake::sink::Sink>::write_block::{{closure}}`
    and `<crate::Type<T> as Trait<U>>::method+0x1f` both reduce to the
    implementing type's path and the method, so a rule can match a module
    prefix without knowing Rust's symbol syntax.
    """
    text = _LEGACY_HASH.sub("", _SYMBOL_OFFSET.sub("", symbol.strip()))
    text = _CRATE_DISAMBIGUATOR.sub("", text)
    if text.startswith("<"):
        depth = 0
        for position, char in enumerate(text):
            if char == "<":
                depth += 1
            elif char == ">" and text[position - 1:position] != "-":
                depth -= 1
                if depth == 0:
                    self_type = _split_self_type(text[1:position])
                    self_type = _SELF_TYPE_PREFIX.sub("", self_type)
                    if self_type.startswith("<"):
                        self_type = frame_path(self_type)
                    text = self_type + text[position + 1:]
                    break
    return _strip_generics(text)


@functools.lru_cache(maxsize=65536)
def classify_frame(symbol: str, pass_: int):
    """The category a pass's first matching rule gives one frame, or None."""
    path = frame_path(symbol)
    for rule_pass, category, pattern in _COMPILED_RULES:
        if rule_pass == pass_ and pattern.match(path):
            return category
    return None


def classify_frames(frames, *, skip=()):
    """The category of one stack and the position of the deciding frame.

    `frames` is outermost first. Positions in `skip` are passed over, which
    is how `allocator_callers` looks outward past the allocator frame.
    """
    for pass_ in (1, 2):
        for position in range(len(frames) - 1, -1, -1):
            if position in skip:
                continue
            category = classify_frame(frames[position], pass_)
            if category is not None:
                return category, position
    return UNKNOWN_CATEGORY, None


def _checked_weight(sample) -> int:
    """One sample's weight, refused unless it is a positive integer."""
    weight = sample.get("weight") if isinstance(sample, dict) else None
    if isinstance(weight, bool) or not isinstance(weight, int):
        raise ValueError(f"a sample weight must be an integer: {weight!r}")
    if weight <= 0:
        raise ValueError(f"a sample weight must be positive: {weight}")
    frames = sample.get("frames")
    if not isinstance(frames, (list, tuple)) or not all(
        isinstance(frame, str) for frame in frames
    ):
        raise ValueError(f"a sample's frames must be a list of symbols: {frames!r}")
    return weight


def classify_cpu(samples: list) -> dict:
    """Assign each weighted sample exactly once to one CPU category.

    Each sample is `{"frames": [...], "weight": n}` with frames outermost
    first. The innermost frame some production rule matches decides it, so
    an encoder frame takes precedence over the sink and upload frames that
    are its ancestors; a sample no rule matches stays in `unknown`. The
    result names every category, zeros included, and its values add up to
    the input weight, which is asserted rather than assumed.
    """
    totals = dict.fromkeys(CPU_CATEGORIES, 0)
    total = 0
    for sample in samples:
        weight = _checked_weight(sample)
        category, _ = classify_frames(sample["frames"])
        totals[category] += weight
        total += weight
    classified = sum(
        value for name, value in totals.items() if name != UNKNOWN_CATEGORY
    )
    if classified + totals[UNKNOWN_CATEGORY] != total:
        raise AssertionError(
            f"classified {classified} plus unknown {totals[UNKNOWN_CATEGORY]} "
            f"is not the input weight {total}: a sample was counted twice or "
            f"dropped"
        )
    return totals


def allocator_callers(samples: list) -> dict:
    """The stage each allocator sample was called from, by weight.

    The allocator is a category of its own, but every isolated bench stage
    includes the allocations it makes. This looks outward past the deciding
    allocator frame for the next frame another rule matches, so a stage's
    exclusive CPU can be compared with its bench cost with the allocations
    it caused added back. Only allocator samples are counted.
    """
    callers = dict.fromkeys(CPU_CATEGORIES, 0)
    for sample in samples:
        weight = _checked_weight(sample)
        frames = sample["frames"]
        category, position = classify_frames(frames)
        if category != "allocator":
            continue
        skip = {
            index for index in range(len(frames))
            if classify_frame(frames[index], 1) == "allocator"
        }
        skip.add(position)
        caller, _ = classify_frames(frames, skip=skip)
        callers[caller] += weight
    return callers


# How wide a two-sided confidence interval on a share is, in standard errors.
CONFIDENCE_Z = 1.96


def category_statistics(samples: list) -> dict:
    """Per-category weight, sample count, share and its 95% interval.

    The share is by weight -- CPU nanoseconds for a `cpu-clock` profile --
    and its interval is the binomial one over the number of samples, which
    is what bounds how well a share of that many samples is known.
    """
    weights = classify_cpu(samples)
    counts = classify_cpu(
        [{"frames": sample["frames"], "weight": 1} for sample in samples]
    )
    total_weight = sum(weights.values())
    total_count = sum(counts.values())
    categories = {}
    for name in CPU_CATEGORIES:
        share = weights[name] / total_weight if total_weight else 0.0
        interval = (
            CONFIDENCE_Z * math.sqrt(share * (1 - share) / total_count)
            if total_count else None
        )
        categories[name] = {
            "weight": weights[name],
            "samples_count": counts[name],
            "share_ratio": share,
            "share_ci95_ratio": interval,
        }
    return {
        "total_weight": total_weight,
        "samples_count": total_count,
        "classified_samples_count": total_count - counts[UNKNOWN_CATEGORY],
        "categories": categories,
    }


# `perf script` output: a header line per sample, then one line per frame,
# innermost first, then a blank line.
PERF_SCRIPT_FIELDS = "comm,tid,cpu,time,period,event,ip,sym,dso"
_PERF_HEADER = re.compile(
    r"^\s*(?P<comm>.*?)\s+(?P<tid>\d+)\s+\[(?P<cpu>\d+)\]\s+"
    r"(?P<time>\d+\.\d+):\s+(?P<period>\d+)\s+(?P<event>\S+?):?\s*$"
)
_PERF_FRAME = re.compile(r"^\s+(?P<ip>[0-9a-fA-F]+)\s+(?P<rest>.*?)\s*$")
KERNEL_DSO = "[kernel.kallsyms]"


def _perf_frame(rest: str) -> tuple:
    """A frame line's symbol and object, the object in its last parentheses."""
    if rest.endswith(")") and " (" in rest:
        symbol, _, dso = rest.rpartition(" (")
        return symbol.strip() or "[unknown]", dso[:-1]
    return rest.strip() or "[unknown]", "[unknown]"


def parse_perf_script(text: str) -> dict:
    """Samples from `perf script -F PERF_SCRIPT_FIELDS` output.

    Each sample carries its thread, CPU, event, `weight` (the sample
    period: nanoseconds for `cpu-clock`) and `frames` outermost first, the
    reverse of the order perf prints them. A line that is neither a header
    nor a frame is counted, never silently dropped.
    """
    samples = []
    unparsed = []
    current = None

    def finish():
        """Close the sample being read, if any."""
        if current is not None:
            current["frames"] = list(reversed(current.pop("leaf_first")))
            current["dsos"] = list(reversed(current.pop("leaf_first_dsos")))
            current["kernel"] = KERNEL_DSO in current["dsos"]
            samples.append(current)

    for line in text.splitlines():
        if not line.strip():
            finish()
            current = None
            continue
        if current is not None and line[:1] in (" ", "\t"):
            frame = _PERF_FRAME.match(line)
            if frame:
                symbol, dso = _perf_frame(frame.group("rest"))
                current["leaf_first"].append(symbol)
                current["leaf_first_dsos"].append(dso)
                continue
        header = _PERF_HEADER.match(line)
        if header:
            finish()
            current = {
                "comm": header.group("comm").strip(),
                "tid": int(header.group("tid")),
                "cpu": int(header.group("cpu")),
                "time_s": float(header.group("time")),
                "weight": int(header.group("period")),
                "event": header.group("event"),
                "leaf_first": [],
                "leaf_first_dsos": [],
            }
            continue
        unparsed.append(line[:160])
    finish()
    return {
        "samples": [sample for sample in samples if sample["weight"] > 0],
        "zero_weight_samples_count": sum(1 for sample in samples if sample["weight"] <= 0),
        "unparsed_lines_count": len(unparsed),
        "unparsed_lines": unparsed[:5],
    }


def leaf_symbols(samples, category=None, limit=10) -> list:
    """The heaviest innermost symbols, optionally of one category only.

    For `unknown` this is the named residual: what the samples no rule
    matched were actually executing.
    """
    weights = collections.Counter()
    for sample in samples:
        if category is not None and classify_frames(sample["frames"])[0] != category:
            continue
        leaf = frame_path(sample["frames"][-1]) if sample["frames"] else "[no frames]"
        weights[leaf] += sample["weight"]
    total = sum(weights.values())
    return [
        {"symbol": symbol, "weight": weight, "share_ratio": weight / total if total else 0.0}
        for symbol, weight in weights.most_common(limit)
    ]


def profile_summary(samples, *, flush_marker="series_parquet_exporter::flush::") -> dict:
    """Everything one profile says, reduced to what a result keeps.

    Per category statistics, the stage each allocator sample was called
    from, CPU seconds per logical CPU and per thread name, the share of
    samples that carried kernel frames, the flush task's own CPU and the
    named residual. The raw samples stay in the run's artifacts.
    """
    statistics = category_statistics(samples)
    per_cpu = collections.Counter()
    per_comm = collections.Counter()
    flush_task_weight = 0
    kernel = 0
    for sample in samples:
        per_cpu[sample.get("cpu")] += sample["weight"]
        per_comm[sample.get("comm")] += sample["weight"]
        if sample.get("kernel"):
            kernel += 1
        if any(flush_marker in frame for frame in sample["frames"]):
            flush_task_weight += sample["weight"]
    statistics["allocator_callers"] = allocator_callers(samples)
    statistics["cpu_s_by_logical_cpu"] = {
        str(cpu): weight / 1e9 for cpu, weight in sorted(per_cpu.items(), key=str)
    }
    statistics["cpu_s_by_thread_name"] = {
        str(comm): weight / 1e9 for comm, weight in per_comm.most_common()
    }
    statistics["kernel_samples_count"] = kernel
    statistics["events"] = sorted({str(sample.get("event")) for sample in samples})
    # perf names a user-space-only event with a `:u` modifier.
    statistics["user_space_only"] = bool(samples) and all(
        str(sample.get("event", "")).endswith(":u") for sample in samples
    )
    statistics["flush_task_weight"] = flush_task_weight
    statistics["named_residual"] = leaf_symbols(samples, UNKNOWN_CATEGORY)
    statistics["heaviest_leaves_by_category"] = {
        name: leaf_symbols(samples, name, limit=5)
        for name in CPU_CATEGORIES
        if statistics["categories"][name]["samples_count"]
    }
    return statistics


# --------------------------------------------------------------------------
# Recording a profile
# --------------------------------------------------------------------------

# The profile every attribution records: a software CPU clock, so each
# sample's period is CPU nanoseconds whatever the core's frequency, at 199
# samples per second of each thread's CPU time, with DWARF call graphs.
PERF_BINARY_ENV = "SERIES_PERF"
PERF_EVENT = "cpu-clock"
PERF_FREQUENCY_HZ = 199
PERF_CALL_GRAPH = "dwarf"

# How long perf may take to acknowledge a control command, to stop and to
# write the script of one profile.
PERF_ACK_DEADLINE_S = 30
PERF_STOP_DEADLINE_S = 120
PERF_SCRIPT_DEADLINE_S = 1800

# How much CPU the preflight's busy child must burn while perf records it.
PREFLIGHT_BUSY_NS = 300_000_000
PREFLIGHT_DEADLINE_S = 30


# The demangler `perf script` output passes through: binutils' c++filt
# reads Rust's v0 mangling, which perf itself leaves mangled.
DEMANGLER = ("c++filt",)

# The engine an attribution profiles, and how it is built. The workspace's
# default linker, lld, places the executable segment at a virtual address
# that differs from its file offset, and perf's libdw unwinder derives the
# module's base from the file offset: every call-frame lookup lands 4 KiB
# away and each stack ends after its first frame. Relinking only the binary
# crate with `-z separate-loadable-segments` makes every segment's offset
# equal its address and changes no generated code.
ATTRIBUTION_ENGINE_ENV = "SERIES_ATTRIBUTION_ENGINE"
ATTRIBUTION_ENGINE = "target/release/df_engine-perf"
ATTRIBUTION_ENGINE_BUILD = (
    "cargo rustc --release --locked -p otel-arrow-dfe --bin df_engine "
    "--features series-parquet,aws,durable-buffer -- "
    "-C link-arg=-Wl,-z,separate-loadable-segments && "
    "cp target/release/df_engine target/release/df_engine-perf && "
    "cargo build --release --locked -p otel-arrow-dfe --bin df_engine "
    "--features series-parquet,aws,durable-buffer"
)


def attribution_engine() -> Path:
    """The engine binary an attribution profiles."""
    return Path(
        os.environ.get(ATTRIBUTION_ENGINE_ENV)
        or Path(test_e2e.WORKSPACE) / ATTRIBUTION_ENGINE
    )


def elf_load_segments(path) -> list:
    """Every PT_LOAD of a 64-bit little-endian ELF: offset, address, flags."""
    with open(path, "rb") as handle:
        header = handle.read(64)
        if header[:4] != b"\x7fELF" or header[4] != 2 or header[5] != 1:
            raise AssertionError(f"{path} is not a 64-bit little-endian ELF")
        phoff = struct.unpack_from("<Q", header, 32)[0]
        phentsize, phnum = struct.unpack_from("<HH", header, 54)
        handle.seek(phoff)
        table = handle.read(phentsize * phnum)
    segments = []
    for index in range(phnum):
        kind, flags, offset, vaddr = struct.unpack_from("<IIQQ", table, index * phentsize)
        if kind == 1:
            segments.append({"offset": offset, "vaddr": vaddr, "executable": bool(flags & 1)})
    return segments


def unwind_layout(path) -> dict:
    """Whether perf's libdw unwinder can place this binary's code.

    It can when every executable segment sits at the same address-minus-
    offset as the first segment, which is where the unwinder puts the
    module's base.
    """
    segments = elf_load_segments(path)
    base = segments[0]["vaddr"] - segments[0]["offset"] if segments else None
    skewed = [
        segment for segment in segments
        if segment["executable"] and segment["vaddr"] - segment["offset"] != base
    ]
    return {
        "binary": str(path),
        "compatible": bool(segments) and not skewed,
        "skewed_executable_segments": [
            {"offset": hex(segment["offset"]), "vaddr": hex(segment["vaddr"])}
            for segment in skewed
        ],
        "build": ATTRIBUTION_ENGINE_BUILD,
    }


# The canonical release engine, the features both engines are built with,
# and the one flag the profiled engine adds.
CANONICAL_ENGINE = "target/release/df_engine"
ENGINE_FEATURES = "series-parquet,aws,durable-buffer"
PROFILED_LINK_ARG = "link-arg=-Wl,-z,separate-loadable-segments"
ENGINE_BUILD_LOG = "engine-build.log"


def engine_build_commands() -> dict:
    """The cargo commands that build both engines, in order.

    The canonical engine is built first, so it is the release engine of the
    current tree; the profiled one is the same crate graph with the single
    extra linker argument passed to the binary crate only, which relinks it
    and compiles nothing; the last command restores the canonical binary,
    which cargo keeps beside the relinked one.
    """
    base = [
        "cargo", "build", "--release", "--locked", "-p", "otel-arrow-dfe",
        "--bin", "df_engine", "--features", ENGINE_FEATURES,
    ]
    profiled = [
        "cargo", "rustc", "--release", "--locked", "-p", "otel-arrow-dfe",
        "--bin", "df_engine", "--features", ENGINE_FEATURES, "--", "-C",
        PROFILED_LINK_ARG,
    ]
    return {"canonical": base, "profiled": profiled, "restore": base}


def rust_tree_status() -> dict:
    """Whether the Rust tree has any change: tracked, or an untracked file.

    An untracked source file can be compiled in -- a new module, a build
    script input -- so it makes the tree as unrecorded as an edit does;
    ignored build output does not appear here.
    """
    done = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all", "--", "rust"],
        capture_output=True, text=True, timeout=60, cwd=str(measurement.REPO_ROOT),
    )
    changes = [line for line in done.stdout.splitlines() if line.strip()]
    return {
        "clean": done.returncode == 0 and not changes,
        "changes": changes[:10],
        "returncode": done.returncode,
    }


def function_symbols(binary, nm="nm") -> dict:
    """A digest of every defined function symbol with its size.

    Two binaries linked from the same objects list the same functions with
    the same sizes whatever their segment layout; any difference in source,
    features, profile or compiler changes the list. The digest covers the
    sorted `(size, demangled name)` pairs of text and weak symbols.
    """
    done = subprocess.run(
        [nm, "--defined-only", "--size-sort", "-C", str(binary)],
        capture_output=True, text=True, errors="replace", timeout=600,
    )
    if done.returncode != 0:
        raise AssertionError(f"{nm} failed on {binary}: {done.stderr[-300:]}")
    entries = []
    for line in done.stdout.splitlines():
        fields = line.split(" ", 2)
        if len(fields) == 3 and fields[1] in ("t", "T", "w", "W"):
            entries.append(f"{int(fields[0], 16)} {fields[2]}")
    entries.sort()
    return {
        "count": len(entries),
        "sha256": hashlib.sha256("\n".join(entries).encode("utf-8", "replace")).hexdigest(),
        "tool": f"{nm} --defined-only --size-sort -C, types t T w W",
    }


def rustc_version() -> str:
    """The full `rustc -vV` of the toolchain that builds the engines."""
    done = subprocess.run(
        ["rustc", "-vV"], capture_output=True, text=True, timeout=60,
        cwd=str(test_e2e.WORKSPACE),
    )
    return done.stdout.strip()


def source_state() -> dict:
    """The Rust tree's cleanliness and the checked-out revision, now."""
    tree = rust_tree_status()
    return {
        "clean": tree["clean"],
        "changes": tree["changes"],
        "revision": measurement.git_provenance().get("revision"),
    }


def source_changed(before, after) -> list:
    """Why the source two states describe is not the same clean source."""
    problems = []
    if not after["clean"]:
        problems.append(f"the Rust tree has changes {after['changes']}")
    if after["revision"] != before["revision"]:
        problems.append(
            f"the revision moved from {before['revision']} to {after['revision']}"
        )
    return problems


def prepare_profiled_engine(log_dir, *, build=True, lease_wait_s=0.0,
                            lease_path=None) -> dict:
    """Build both engines from the current tree and prove them one engine.

    The Rust tree must be clean, so the revision recorded is the source that
    was compiled. Both engines are built by `engine_build_commands`, holding
    the host lease so no one's measurement overlaps the compile, before any
    of this family's measured windows. Each binary's profile, features,
    allocator, toolchain, `rustc -vV`, hash, segment layout and
    function-symbol digest are recorded, with the exact flag difference. The
    engines are one engine only when their function symbols are identical;
    the profiled one must also have a layout perf can unwind. Any failure
    is a named problem and the attribution is refused.
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    workspace = Path(test_e2e.WORKSPACE)
    canonical = workspace / CANONICAL_ENGINE
    profiled = attribution_engine()
    commands = engine_build_commands()
    facts = {
        "commands": {name: " ".join(argv) for name, argv in commands.items()},
        "environment_rustflags": {
            name: os.environ.get(name)
            for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS",
                         "CARGO_BUILD_RUSTFLAGS")
        },
        "rustflags_difference": {
            "canonical": [],
            "profiled": ["-C", PROFILED_LINK_ARG],
            "scope": "the df_engine binary crate only, through cargo rustc",
        },
        "rust_tree": rust_tree_status(),
        "git": measurement.git_provenance(),
        "built": bool(build),
    }
    initial = {
        "clean": facts["rust_tree"]["clean"],
        "changes": facts["rust_tree"]["changes"],
        "revision": facts["git"].get("revision"),
    }
    facts["source_checks"] = {"before_lease": initial}
    problems = []
    if not facts["rust_tree"]["clean"]:
        problems.append(
            f"the Rust tree has uncommitted changes {facts['rust_tree']['changes']}; "
            f"a profiled engine must be built from a recorded revision"
        )
    if not build:
        problems.append(
            "the engines were not built by this family, so neither is proven to "
            "be the current tree's"
        )
    if not problems:
        lease = measurement.HostLease(lease_path, run_id="attribution-engine-build")
        lease.acquire(deadline_ns=time.monotonic_ns() + int(lease_wait_s * 10**9))
        facts["lease"] = lease.as_json()
        try:
            # The lease may have been waited for; the tree may have moved
            # meanwhile, and the engines must be the recorded source's.
            held = source_state()
            facts["source_checks"]["after_lease"] = held
            problems.extend(
                f"while waiting for the lease, {problem}"
                for problem in source_changed(initial, held)
            )
            commands_to_run = () if problems else ("canonical", "profiled", "restore")
            with open(log_dir / ENGINE_BUILD_LOG, "w", encoding="ascii",
                      errors="replace") as log:
                for name in commands_to_run:
                    done = subprocess.run(
                        commands[name], cwd=str(workspace), stdout=log,
                        stderr=subprocess.STDOUT, timeout=7200,
                    )
                    if done.returncode != 0:
                        problems.append(f"the {name} build failed with {done.returncode}")
                        break
                    if name == "canonical":
                        facts["canonical_sha256_before"] = measurement.file_digest(canonical)
                    if name == "profiled":
                        _ = shutil.copyfile(canonical, profiled)
                        profiled.chmod(0o755)
            if commands_to_run:
                built = source_state()
                facts["source_checks"]["after_builds"] = built
                problems.extend(
                    f"during the builds, {problem}"
                    for problem in source_changed(initial, built)
                )
        finally:
            lease.release()
    for role, path in (("canonical", canonical), ("profiled", profiled)):
        if not path.is_file():
            problems.append(f"no {role} engine at {path}")
            continue
        described = measurement.engine_build(path)
        described["rustc_vv"] = rustc_version()
        described["function_symbols"] = function_symbols(path)
        described["layout"] = unwind_layout(path)
        facts[role] = described
    one, other = facts.get("canonical"), facts.get("profiled")
    if one and other:
        if facts.get("canonical_sha256_before") not in (None, one["binary_sha256"]):
            problems.append(
                "restoring the canonical engine produced a different binary than "
                "the canonical build"
            )
        for key in ("profile", "features", "allocator", "toolchain", "rustc_vv"):
            if one[key] != other[key]:
                problems.append(f"the engines differ in {key}: {one[key]} vs {other[key]}")
        if one["profile"] != "release":
            problems.append(f"the canonical engine is a {one['profile']} build")
        if one["function_symbols"] != other["function_symbols"]:
            problems.append(
                f"the engines' function symbols differ: {one['function_symbols']} "
                f"vs {other['function_symbols']}"
            )
        if one["binary_sha256"] == other["binary_sha256"]:
            problems.append("the profiled engine was not relinked")
        if not other["layout"]["compatible"]:
            problems.append(
                f"the profiled engine's executable segments are skewed: "
                f"{other['layout']['skewed_executable_segments']}"
            )
    facts["problems"] = problems
    facts["valid"] = not problems
    return facts


def perf_binary() -> str:
    """The perf executable: `SERIES_PERF`, else `perf` on the PATH."""
    return os.environ.get(PERF_BINARY_ENV) or shutil.which("perf") or "perf"


def pinned_argv(argv, cores) -> list:
    """`argv` confined to `cores` by `taskset`, which execs it in place."""
    if not cores:
        return list(argv)
    return ["taskset", "-c", ",".join(str(core) for core in cores)] + list(argv)


def perf_record_argv(perf, pid, output, ctl, ack) -> list:
    """The one `perf record` command line every profile and preflight uses.

    The events start disabled (`-D -1`) and are switched on and off through
    the control FIFOs, so the profile covers exactly the input phase.
    `--sample-cpu` records the CPU of each sample, which a per-process
    recording otherwise leaves out.
    """
    return [
        str(perf), "record", "-e", PERF_EVENT, "-F", str(PERF_FREQUENCY_HZ),
        "-g", "--call-graph", PERF_CALL_GRAPH, "--sample-cpu", "-p", str(pid),
        "-o", str(output), "-D", "-1", "--control", f"fifo:{ctl},{ack}",
    ]


# perf record answers an interrupt by writing its file and then ending
# itself with that same signal, so both exits mean a complete recording.
PERF_COMPLETE_EXITS = (0, -signal.SIGINT)


def perf_script_argv(perf, data) -> list:
    """The `perf script` command line that turns a profile into samples."""
    return [
        str(perf), "script", "-i", str(data), "-F", PERF_SCRIPT_FIELDS,
        "--no-inline",
    ]


class PerfRecorder:
    """One `perf record -p` of one process, driven through control FIFOs.

    `start` attaches with the events disabled, `enable` and `disable`
    switch them and return when perf has acknowledged, `stop` interrupts
    perf so it writes its file, and `script` runs `perf script` on it. perf
    runs confined to the profiler's own cores.
    """

    def __init__(self, directory, *, cores=(), perf=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.perf = perf or perf_binary()
        self.cores = [int(core) for core in cores]
        self.data = self.directory / "perf.data"
        self.log_path = self.directory / "perf.log"
        self.ctl = self.directory / "perf.ctl"
        self.ack = self.directory / "perf.ack"
        self.process = None
        self.argv = None
        self.returncode = None
        self.enabled_ns = None
        self.disabled_ns = None
        self._ctl_fd = None
        self._ack_fd = None
        self._log = None
        self.demangler = None

    def start(self, pid):
        """Attach to `pid` with every event disabled."""
        for fifo in (self.ctl, self.ack):
            if fifo.exists():
                fifo.unlink()
            os.mkfifo(fifo)
        # Both ends are opened read-write before perf starts, so neither side
        # ever blocks opening a FIFO that has no peer yet.
        self._ctl_fd = os.open(self.ctl, os.O_RDWR | os.O_NONBLOCK)
        self._ack_fd = os.open(self.ack, os.O_RDWR | os.O_NONBLOCK)
        self.argv = pinned_argv(
            perf_record_argv(self.perf, pid, self.data, self.ctl, self.ack), self.cores
        )
        self._log = open(self.log_path, "w", encoding="ascii", errors="replace")
        self.process = subprocess.Popen(
            self.argv, stdout=self._log, stderr=subprocess.STDOUT
        )
        return self

    @property
    def pid(self):
        """perf's own PID; `taskset` execs perf, so it is the same process."""
        return self.process.pid

    def log_tail(self, lines=12) -> str:
        """The end of perf's own output."""
        if self._log is not None and not self._log.closed:
            self._log.flush()
        try:
            text = self.log_path.read_text(encoding="ascii", errors="replace")
        except OSError:
            return ""
        return " | ".join(text.strip().splitlines()[-lines:])

    def command(self, verb, deadline_s=PERF_ACK_DEADLINE_S) -> int:
        """Send one control command and wait for perf's acknowledgement."""
        if self.process is None:
            raise AssertionError(f"perf was never started, so it cannot {verb}")
        _ = os.write(self._ctl_fd, f"{verb}\n".encode("ascii"))
        received = b""

        def observe():
            """Whatever perf has acknowledged, or its exit."""
            nonlocal received
            code = self.process.poll()
            if code is not None:
                return ("exited", code)
            ready, _, _ = select.select([self._ack_fd], [], [], 0.05)
            if ready:
                try:
                    received += os.read(self._ack_fd, 64)
                except BlockingIOError:
                    pass
            return ("ack", received) if b"ack" in received else ("waiting", received)

        state, value = measurement.wait_until(
            observe,
            lambda observed: observed[0] != "waiting",
            deadline_ns=time.monotonic_ns() + int(deadline_s * 10**9),
            description=f"perf acknowledging {verb}",
        )
        if state == "exited":
            raise AssertionError(
                f"perf exited with {value} before acknowledging {verb}: "
                f"{self.log_tail()}"
            )
        return time.monotonic_ns()

    def enable(self):
        """Start sampling; returns once perf says the events are enabled."""
        self.enabled_ns = self.command("enable")
        return self.enabled_ns

    def disable(self):
        """Stop sampling; returns once perf says the events are disabled."""
        self.disabled_ns = self.command("disable")
        return self.disabled_ns

    def stop(self, deadline_s=PERF_STOP_DEADLINE_S) -> int:
        """Interrupt perf so it finishes its file, and wait for it."""
        if self.process is None:
            return None
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
        try:
            self.returncode = self.process.wait(timeout=deadline_s)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.returncode = self.process.wait(timeout=30)
            raise AssertionError(
                f"perf did not finish within {deadline_s}s of an interrupt: "
                f"{self.log_tail()}"
            )
        return self.returncode

    def script(self, output, deadline_s=PERF_SCRIPT_DEADLINE_S) -> str:
        """Run `perf script` on the recorded file and return its text.

        perf demangles legacy Rust symbols but not v0 ones (`_RNv...`), so
        the text is passed through the demangler when one is installed; the
        file keeps what the classifier read.
        """
        output = Path(output)
        done = subprocess.run(
            pinned_argv(perf_script_argv(self.perf, self.data), self.cores),
            capture_output=True, text=True, errors="replace", timeout=deadline_s,
        )
        if done.returncode != 0:
            raise AssertionError(
                f"perf script failed with {done.returncode}: {done.stderr[-500:]}"
            )
        text = done.stdout
        demangler = shutil.which(DEMANGLER[0])
        if demangler:
            demangled = subprocess.run(
                [demangler] + list(DEMANGLER[1:]), input=text, capture_output=True,
                text=True, errors="replace", timeout=deadline_s,
            )
            if demangled.returncode != 0:
                raise AssertionError(
                    f"{DEMANGLER[0]} failed with {demangled.returncode}"
                )
            text = demangled.stdout
        self.demangler = demangler
        output.write_text(text.encode("ascii", "replace").decode("ascii"), encoding="ascii")
        return output.read_text(encoding="ascii")

    def close(self):
        """Release the FIFOs and perf's log; perf is killed if still alive."""
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.returncode = self.process.wait(timeout=30)
        for name in ("_ctl_fd", "_ack_fd"):
            descriptor = getattr(self, name)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, name, None)
        if self._log is not None and not self._log.closed:
            self._log.close()

    def as_json(self) -> dict:
        """What this recording was, for a result."""
        # The FIFO argument joins its paths with a colon, which the publish
        # scrubber leaves alone, so the recording's own directory is
        # replaced by a token here.
        directory = str(self.directory)
        return {
            "argv": [
                argument.replace(directory, PERF_DIR_TOKEN) for argument in self.argv
            ] if self.argv else None,
            "returncode": self.returncode,
            "data": measurement.file_entry(self.data) if self.data.is_file() else None,
            "log_tail": self.log_tail(),
            "enabled_window_s": (
                (self.disabled_ns - self.enabled_ns) / 1e9
                if self.enabled_ns and self.disabled_ns else None
            ),
            "demangler": self.demangler,
        }


# What a recording's own directory reads as in a published result.
PERF_DIR_TOKEN = "<perf_dir>"


# The busy child the preflight profiles: it burns CPU until killed.
PREFLIGHT_BUSY_CODE = (
    "import time\n"
    "end = time.monotonic() + 60\n"
    "while time.monotonic() < end:\n"
    "    pass\n"
)


def perf_event_paranoid(path="/proc/sys/kernel/perf_event_paranoid"):
    """The kernel's perf access setting, or None where it cannot be read."""
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def perf_preflight(directory, *, cores=(), perf=None, engine=None) -> dict:
    """Whether the exact recording an attribution makes works on this host.

    A busy child is profiled with the same command line, control FIFOs and
    script command a measured run uses. The host can attribute only if perf
    attaches, acknowledges enable and disable, and `perf script` yields at
    least one unwound sample. Nothing here needs the host lease: it runs no
    engine and measures nothing.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    perf = perf or perf_binary()
    facts = {
        "perf": perf,
        "event": PERF_EVENT,
        "frequency_hz": PERF_FREQUENCY_HZ,
        "call_graph": PERF_CALL_GRAPH,
        "perf_event_paranoid": perf_event_paranoid(),
        "cores": list(cores),
        "attached": False,
    }
    if shutil.which(perf) is None and not Path(perf).is_file():
        facts["reason"] = f"no perf executable {perf!r} on this host"
        return facts
    if engine is not None:
        if not Path(engine).is_file():
            facts["reason"] = (
                f"no profilable engine at {engine}; build it with: "
                f"{ATTRIBUTION_ENGINE_BUILD}"
            )
            return facts
        facts["engine_layout"] = unwind_layout(engine)
        if not facts["engine_layout"]["compatible"]:
            facts["reason"] = (
                f"{engine} has executable segments whose address differs from "
                f"their file offset, which perf's unwinder cannot place; relink "
                f"it with: {ATTRIBUTION_ENGINE_BUILD}"
            )
            return facts
    try:
        version = subprocess.run(
            [perf, "--version"], capture_output=True, text=True, timeout=30
        )
        facts["version"] = (version.stdout or version.stderr).strip()
    except (OSError, subprocess.SubprocessError) as error:
        facts["reason"] = f"perf --version failed: {error}"
        return facts
    busy = subprocess.Popen(pinned_argv([sys.executable, "-c", PREFLIGHT_BUSY_CODE], cores))
    recorder = PerfRecorder(directory, cores=cores, perf=perf)
    try:
        recorder.start(busy.pid)
        recorder.enable()
        start = procfs_cpu_ns(busy.pid)
        _ = measurement.wait_until(
            lambda: procfs_cpu_ns(busy.pid) - start,
            lambda burned: burned >= PREFLIGHT_BUSY_NS,
            deadline_ns=time.monotonic_ns() + PREFLIGHT_DEADLINE_S * 10**9,
            description="the preflight child burning CPU while perf records it",
        )
        recorder.disable()
        facts["record_returncode"] = recorder.stop()
        if facts["record_returncode"] not in PERF_COMPLETE_EXITS:
            raise AssertionError(
                f"perf record ended with {facts['record_returncode']}: "
                f"{recorder.log_tail()}"
            )
        parsed = parse_perf_script(recorder.script(directory / "perf-script.txt"))
        samples = parsed["samples"]
        facts["samples_count"] = len(samples)
        facts["unwound_samples_count"] = sum(
            1 for sample in samples if len(sample["frames"]) >= 2
        )
        facts["unparsed_lines_count"] = parsed["unparsed_lines_count"]
        facts["attached"] = facts["unwound_samples_count"] > 0
        if not facts["attached"]:
            facts["reason"] = (
                f"perf recorded {len(samples)} samples of a busy process and "
                f"none with an unwound call chain"
            )
    except (AssertionError, OSError, ValueError, subprocess.SubprocessError) as error:
        facts["reason"] = f"{type(error).__name__}: {error}"
    finally:
        busy.kill()
        _ = busy.wait(timeout=30)
        facts["perf_log_tail"] = recorder.log_tail()
        recorder.close()
    return facts


# --------------------------------------------------------------------------
# What the engine's threads and flushes did while it was profiled
# --------------------------------------------------------------------------


def procfs_user_system_ns(pid) -> dict:
    """One process's user and system CPU time, from its tick counters.

    A profile restricted to user space (`cpu-clock:u`, which perf records
    when the host allows only user-space measurement) is compared with the
    user time alone; the kernel time it could not see is reported beside
    it rather than spread over the categories.
    """
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    tick = 10**9 // os.sysconf("SC_CLK_TCK")
    return {"user_ns": int(fields[11]) * tick, "system_ns": int(fields[12]) * tick}


def thread_times(pid) -> dict:
    """Each live thread's scheduler times, from `/proc/PID/task/*/schedstat`.

    The first field is the time the thread ran, the second the time it sat
    runnable on a run queue waiting for a CPU, both in nanoseconds.
    """
    threads = {}
    task = Path(f"/proc/{pid}/task")
    try:
        entries = list(task.iterdir())
    except OSError:
        return threads
    for entry in entries:
        try:
            fields = (entry / "schedstat").read_text().split()
            comm = (entry / "comm").read_text().strip()
        except OSError:
            continue
        if len(fields) < 3:
            continue
        threads[int(entry.name)] = {
            "comm": comm,
            "on_cpu_ns": int(fields[0]),
            "runqueue_wait_ns": int(fields[1]),
            "timeslices_count": int(fields[2]),
        }
    return threads


def thread_schedule(before, after, window_ns, worker_tids) -> dict:
    """How the engine's threads spent one window, and its workers' fractions.

    Every thread alive at the end is listed with the CPU it used and the
    time it waited for a CPU during the window. For each worker the window
    is split into on-CPU, run-queue (scheduler) and off-CPU (blocked or
    idle) fractions, which add up to one.
    """
    empty = {"on_cpu_ns": 0, "runqueue_wait_ns": 0, "timeslices_count": 0}
    threads = []
    for tid, end in sorted(after.items()):
        start = before.get(tid, empty)
        threads.append(
            {
                "tid": tid,
                "comm": end["comm"],
                "role": "worker" if tid in worker_tids else "other",
                "on_cpu_s": (end["on_cpu_ns"] - start["on_cpu_ns"]) / 1e9,
                "runqueue_wait_s": (
                    end["runqueue_wait_ns"] - start["runqueue_wait_ns"]
                ) / 1e9,
                "timeslices_count": end["timeslices_count"] - start["timeslices_count"],
                "started_in_window": tid not in before,
            }
        )
    workers = {}
    window_s = window_ns / 1e9
    for thread in threads:
        if thread["role"] != "worker" or window_s <= 0:
            continue
        on_cpu = thread["on_cpu_s"] / window_s
        runqueue = thread["runqueue_wait_s"] / window_s
        workers[str(thread["tid"])] = {
            "on_cpu_ratio": on_cpu,
            "runqueue_wait_ratio": runqueue,
            "off_cpu_ratio": max(0.0, 1.0 - on_cpu - runqueue),
        }
    return {
        "window_s": window_s,
        "threads": threads,
        "workers": workers,
        "other_threads_on_cpu_s": sum(
            thread["on_cpu_s"] for thread in threads if thread["role"] != "worker"
        ),
    }


# The exporter's flush wall-time instrument. It is a distribution, which the
# OTLP export drains every collection; the admin API reads the same
# accumulator without resetting it, so what it serves is cumulative over the
# engine's lifetime. A window's flushes are therefore the difference of two
# readings, never a sum over collections.
FLUSH_METRIC = "exporter.series_parquet:flush.duration"


def flush_reading(document) -> dict:
    """The cumulative flush wall time and count one telemetry document shows.

    Every worker's single exporter entity is read and the workers are
    added; a worker with two exporter entities is ambiguous and an error,
    and a worker whose exporter published no flush instrument yet reads as
    no flushes, because the instrument exists only once a flush completed.
    """
    parsed = measurement.parse_telemetry(document, expected_workers=None)
    reading = {"sum_s": 0.0, "count": 0, "max_s": 0.0, "workers_count": 0}
    for key, worker in sorted(parsed["workers"].items()):
        entities = worker["metrics"].get(FLUSH_METRIC) or {}
        if len(entities) > 1:
            raise AssertionError(f"{key} publishes {FLUSH_METRIC} for {sorted(entities)}")
        if not entities:
            continue
        value = next(iter(entities.values()))
        if not isinstance(value, dict):
            raise AssertionError(f"{key} reports {FLUSH_METRIC} as {value!r}")
        count = int(value.get("count") or 0)
        reading["workers_count"] += 1
        reading["sum_s"] += float(value.get("sum") or 0.0)
        reading["count"] += count
        if count:
            reading["max_s"] = max(reading["max_s"], float(value.get("max") or 0.0))
    return reading


def flush_window(before, after) -> dict:
    """The flushes that completed between two cumulative readings."""
    count = after["count"] - before["count"]
    wall = after["sum_s"] - before["sum_s"]
    if count < 0 or wall < -1e-9:
        raise AssertionError(
            f"the cumulative flush instrument went backwards: {before} -> {after}"
        )
    return {
        "flush_count": count,
        "flush_wall_s": max(0.0, wall),
        # The maximum is cumulative too; it covers this window only when the
        # window holds the lifetime's first flush, which it does here.
        "flush_max_s": after["max_s"],
    }


COMMITTED_EVENT = "series_parquet.block.committed"


def committed_blocks(log_path) -> int:
    """How many blocks the engine's own log says it committed."""
    try:
        text = Path(log_path).read_text(encoding="ascii", errors="replace")
    except OSError:
        return 0
    return sum(1 for line in text.splitlines() if COMMITTED_EVENT in line)


def exporter_counter(engine, name) -> float:
    """The largest value of one exporter counter the admin API reports now."""
    values = test_e2e.metric_values(test_e2e.engine_metrics(engine), name)
    if not values:
        raise AssertionError(f"the engine reports no exporter metric {name}")
    return max(values)


# --------------------------------------------------------------------------
# Prebuilt inputs
# --------------------------------------------------------------------------

# A profile needs seconds of engine CPU, which is millions of records, and
# building a 1 KiB log record in Python costs more than the engine spends on
# it. The requests are therefore built once, before any lease, in parallel
# processes, into three files: the concatenated wire bodies, one
# (request index, offset, length) entry per request, and one kind byte and
# raw SHA-256 per record. The sender reads a request back by index; the
# record ids are regenerated from the workload, exactly as `build_request`
# forms them.
PREBUILT_FORMAT = "series-attribution-prebuilt/1"
_PREBUILT_OFFSET = struct.Struct("<QQI")
_PREBUILT_DIGEST_BYTES = 33
PREBUILT_CHUNK_REQUESTS = 128


def _build_prebuilt_chunk(payload) -> list:
    """Build one chunk of requests; runs in a worker process."""
    workload_fields, indexes = payload
    workload = measurement.Workload(**workload_fields)
    built = []
    for index in indexes:
        signal_name, wire, rows = measurement.build_request(workload, index)
        packed = b"".join(
            bytes([measurement.ALL_KINDS.index(kind)]) + bytes.fromhex(digest)
            for _record_id, kind, digest in rows
        )
        built.append((index, signal_name, wire, packed))
    return built


class PrebuiltRequests:
    """One signal's requests of one workload, built once, read by index."""

    FILES = ("wire.bin", "offsets.bin", "digests.bin")

    def __init__(self, directory):
        self.directory = Path(directory)
        self.sidecar = json.loads(
            (self.directory / "sidecar.json").read_text(encoding="ascii")
        )
        if self.sidecar.get("format") != PREBUILT_FORMAT:
            raise AssertionError(f"{directory} is not a {PREBUILT_FORMAT} input")
        self.workload = measurement.Workload(**self.sidecar["workload"])
        self.signal = self.sidecar["signal"]
        offsets = (self.directory / "offsets.bin").read_bytes()
        self.entries = {}
        for ordinal in range(len(offsets) // _PREBUILT_OFFSET.size):
            index, offset, length = _PREBUILT_OFFSET.unpack_from(
                offsets, ordinal * _PREBUILT_OFFSET.size
            )
            self.entries[index] = (ordinal, offset, length)
        self.indexes = sorted(self.entries)
        self._files = []
        self.wire = self._map("wire.bin")
        self.digests = self._map("digests.bin")

    def _map(self, name):
        """A read-only map of one of the input's files."""
        handle = open(self.directory / name, "rb")
        self._files.append(handle)
        if os.fstat(handle.fileno()).st_size == 0:
            return b""
        return mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)

    def request(self, index):
        """Request `index` as `build_request` returns it."""
        ordinal, offset, length = self.entries[index]
        wire = bytes(self.wire[offset:offset + length])
        width = self.workload.records_per_request
        base = ordinal * width * _PREBUILT_DIGEST_BYTES
        rows = []
        for point in range(width):
            start = base + point * _PREBUILT_DIGEST_BYTES
            entry = self.digests[start:start + _PREBUILT_DIGEST_BYTES]
            kind = measurement.ALL_KINDS[entry[0]]
            rows.append(
                (
                    measurement.stable_id(self.workload.seed, index, point, kind),
                    kind,
                    bytes(entry[1:]).hex(),
                )
            )
        return self.signal, wire, rows

    def close(self):
        """Release the maps and files."""
        for mapped in (self.wire, self.digests):
            if isinstance(mapped, mmap.mmap):
                mapped.close()
        for handle in self._files:
            handle.close()
        self._files = []

    def as_json(self) -> dict:
        """The input as a result records it."""
        return {
            key: self.sidecar[key]
            for key in ("format", "signal", "requests", "records", "wire_bytes",
                        "files", "workload", "build_s", "processes")
        }

    @classmethod
    def build(cls, workload, signal_name, directory, *, processes=None):
        """Build the input unless an identical one is already there.

        `processes=0` builds in this process, which only the fast contract
        tests do. Three requests -- the first, the middle and the last --
        are read back and compared byte for byte with `build_request`.
        """
        directory = Path(directory)
        sidecar_path = directory / "sidecar.json"
        if sidecar_path.is_file():
            existing = json.loads(sidecar_path.read_text(encoding="ascii"))
            if (
                existing.get("format") == PREBUILT_FORMAT
                and existing.get("workload") == workload.as_json()
                and existing.get("signal") == signal_name
                and all(
                    measurement.file_entry(directory / name)
                    == existing["files"].get(name)
                    for name in cls.FILES
                )
            ):
                return cls(directory)
        directory.mkdir(parents=True, exist_ok=True)
        indexes = request_indexes(workload, signal_name)
        chunks = [
            (workload.as_json(), indexes[start:start + PREBUILT_CHUNK_REQUESTS])
            for start in range(0, len(indexes), PREBUILT_CHUNK_REQUESTS)
        ]
        started = time.monotonic_ns()
        if processes is None:
            processes = max(1, len(os.sched_getaffinity(0)))
        temporary = {name: directory / f"{name}.partial" for name in cls.FILES}
        offset = 0
        records = 0
        with open(temporary["wire.bin"], "wb") as wire, open(
            temporary["offsets.bin"], "wb"
        ) as offsets, open(temporary["digests.bin"], "wb") as digests:
            if processes == 0:
                results = map(_build_prebuilt_chunk, chunks)
                pool = None
            else:
                pool = concurrent.futures.ProcessPoolExecutor(
                    max_workers=processes,
                    mp_context=multiprocessing.get_context("spawn"),
                )
                results = pool.map(_build_prebuilt_chunk, chunks)
            try:
                for built in results:
                    for index, actual, body, packed in built:
                        if actual != signal_name:
                            raise AssertionError(
                                f"request {index} is {actual}, not {signal_name}"
                            )
                        _ = wire.write(body)
                        _ = offsets.write(_PREBUILT_OFFSET.pack(index, offset, len(body)))
                        _ = digests.write(packed)
                        offset += len(body)
                        records += len(packed) // _PREBUILT_DIGEST_BYTES
            finally:
                if pool is not None:
                    pool.shutdown()
        for name, path in temporary.items():
            os.replace(path, directory / name)
        sidecar = {
            "format": PREBUILT_FORMAT,
            "signal": signal_name,
            "workload": workload.as_json(),
            "requests": len(indexes),
            "records": records,
            "wire_bytes": offset,
            "files": {name: measurement.file_entry(directory / name) for name in cls.FILES},
            "build_s": (time.monotonic_ns() - started) / 1e9,
            "processes": processes,
        }
        _ = measurement.write_json_atomic(sidecar_path, sidecar)
        prebuilt = cls(directory)
        for index in sorted({indexes[0], indexes[len(indexes) // 2], indexes[-1]}):
            if prebuilt.request(index) != measurement.build_request(workload, index):
                prebuilt.close()
                raise AssertionError(
                    f"prebuilt request {index} differs from build_request"
                )
        return prebuilt


# --------------------------------------------------------------------------
# The attribution family
# --------------------------------------------------------------------------

# The acceptance numbers of an attribution family. Each workload needs this
# many classified samples over its repetitions; the input is sized from the
# stage family's cost so that it gets them with the margin. The margin
# covers what the sizing cannot know: the benches' cost is an estimate of
# the engine's (the first rehearsal measured 5.5 us per log record against
# 6.6 us predicted), a user-space-only profile cannot see kernel time, and
# unknown samples do not count.
ATTRIBUTION_MINIMUM_SAMPLES = 10_000
ATTRIBUTION_REPETITIONS = 3
ATTRIBUTION_SAMPLE_MARGIN = 2.0

# The share of the input a repetition's unprofiled control lifetime sends,
# in whole blocks from the start. It measures the same steady state the
# profiled lifetime does -- hundreds of blocks -- for perf's overhead, at a
# third of the cost.
CONTROL_FRACTION = 1 / 3

# The engine configuration a profiled lifetime runs. An acknowledgement is
# durable, so a sender's request waits for its block's flush: a block
# rotates when it holds this many requests, and twice that many are in
# flight, so one block fills while the previous one flushes. The window
# interval is the stage family's.
ATTRIBUTION_BLOCK_REQUESTS = 128
ATTRIBUTION_IN_FLIGHT = 256
ATTRIBUTION_INTERVAL_S = 15

# The plan's Stage agreement rule, the binding reconciliation: the sum of
# the exclusive category CPU and the named residual is compared with the
# engine CPU the scheduler measured over the same window. A reconciliation
# error above 10 percent, or unexplained CPU above 20 percent, invalidates
# the attribution. Unexplained CPU is the named residual (samples no rule
# matched) plus any measured CPU the samples did not cover.
STAGE_AGREEMENT_ERROR_LIMIT = 0.10
UNEXPLAINED_CPU_LIMIT = 0.20

# The band of the DESCRIPTIVE per-stage comparison of engine-attributed
# costs with isolated bench costs. It is not an acceptance model and gates
# nothing: the two measure the same code in different contexts -- block
# sizes, cache state, a shared worker thread. A row outside it must carry a
# measured explanation from published evidence or say it is unexplained.
AGREEMENT_BOUNDS = (0.5, 2.0)

# The stage family of record every attribution is reconciled against: the
# family measured under the campaign pin, by index and hash. The committed
# index was later rescrubbed of host paths, which changed its bytes but no
# measured value; both hashes are recorded.
PINNED_STAGE_FAMILY = {
    "index": "stages",
    "measured_in_commit": "42be4c5d3c687a3780c2d0386c494902502bff6d",
    "sha256_as_measured": (
        "ede76546c1d1ad0b99267be4417a803b77af8f8ff96920eb090709c245908ced"
    ),
    "rescrubbed_in_commit": "bc75f2f6c1feb0290391393455af706951a6690c",
    "sha256": "e4d4b81218e5babfcb32e2de4a7d7d4ab97cba73c204bf3396a468cc659cb1bd",
    "affinity": "taskset -c 0-7,16-23",
}

# The spot family re-measured at the attribution's own source revision, when
# one has been published.
SPOT_STAGE_INDEX = "stages-spot"

# Which stage results each attributed cost is reconciled with. A stage's
# bench cost includes the allocations it makes, so each row compares the
# attributed exclusive cost with the allocator samples its categories
# called added back. `total` compares the whole engine with the real
# engine's noop pipeline plus the cumulative OTLP-to-object-store layer.
RECONCILIATION_ROWS = (
    ("conversion", ("conversion",), (("convert", "isolated"),)),
    ("extraction", ("extraction",), (("extract", "isolated"),)),
    ("admission_sort_seal_merge", ("buffer", "sort_seal_merge"),
     (("sort_seal", "isolated"), ("merge", "isolated"))),
    ("encoding", ("encoding",), (("encode", "isolated"),)),
    ("upload", ("upload",), (("upload", "async"),)),
    ("engine_runtime", ("engine_runtime",), (("otlp_noop", "pipeline"),)),
    ("total", CPU_CATEGORIES, (("otlp_noop", "pipeline"), ("otlp_minio", "async"))),
)

# The metrics every profiled repetition reports, with the direction each
# gets worse in.
ATTRIBUTION_METRIC_DIRECTIONS = dict(
    {
        "engine_cpu_ns_per_record": measurement.LOWER_IS_BETTER,
        "throughput_records_per_s": measurement.HIGHER_IS_BETTER,
        "control_engine_cpu_ns_per_record": measurement.LOWER_IS_BETTER,
        "control_throughput_records_per_s": measurement.HIGHER_IS_BETTER,
        "classified_samples_count": measurement.HIGHER_IS_BETTER,
        "flush_wall_s": measurement.LOWER_IS_BETTER,
        "upload_wait_s": measurement.LOWER_IS_BETTER,
        "peak_rss_bytes": measurement.LOWER_IS_BETTER,
    },
    **{
        f"{name}_cpu_ns_per_record": measurement.LOWER_IS_BETTER
        for name in CPU_CATEGORIES
    },
)

# The metrics whose dispersion across repetitions is a stability gate: the
# CPU costs. A category below `STABLE_SHARE` of the profile is reported,
# but its sampling noise is not a stability failure.
STABLE_SHARE = 0.05


def attribution_family_ordinal(report_dir) -> int:
    """The first attribution family number no published aggregate has used."""
    directory = measurement.resolve_report_dir(report_dir)
    highest = 0
    if Path(directory).is_dir():
        for path in Path(directory).glob("attribution-*-f[0-9][0-9][0-9].json"):
            try:
                highest = max(highest, int(path.stem.rsplit("-f", 1)[1]))
            except (IndexError, ValueError):
                continue
    return highest + 1


def signal_request_count(workload, signal_name) -> int:
    """How many of `workload`'s requests carry `signal_name`."""
    metrics = (workload.requests + workload.metrics_every - 1) // workload.metrics_every
    return metrics if signal_name == "metrics" else workload.requests - metrics


def attribution_workload(config_id, cpu_ns_per_record, *, repetitions,
                         minimum_samples=ATTRIBUTION_MINIMUM_SAMPLES,
                         records=None) -> measurement.Workload:
    """The stage family's workload, lengthened until a profile is enough.

    The shape -- records per request, body size, series and signal mix -- is
    the stage family's, so the two are joined by configuration. Only the
    request count grows: to the records a profile of `repetitions`
    lifetimes needs for `minimum_samples` classified samples at
    `PERF_FREQUENCY_HZ`, with the margin, at `cpu_ns_per_record`, rounded up
    to whole blocks of the signal's requests.
    """
    config = WORKLOAD_CONFIGS[config_id]
    base = config["workload"]
    if records is None:
        if not cpu_ns_per_record or cpu_ns_per_record <= 0:
            raise AssertionError(
                f"sizing {config_id} needs a positive reference CPU cost per "
                f"record, not {cpu_ns_per_record!r}"
            )
        cpu_s = (
            minimum_samples * ATTRIBUTION_SAMPLE_MARGIN
            / (repetitions * PERF_FREQUENCY_HZ)
        )
        records = cpu_s * 1e9 / cpu_ns_per_record
    blocks = max(1, math.ceil(records / base.records_per_request / ATTRIBUTION_BLOCK_REQUESTS))
    wanted = blocks * ATTRIBUTION_BLOCK_REQUESTS
    total = wanted
    while signal_request_count(dataclasses.replace(base, requests=total), config["signal"]) < wanted:
        total += 1
    return dataclasses.replace(base, requests=total)


def stage_family_reference(report_dir, index_name, *, expected_sha256=None,
                           configs=None) -> dict:
    """One published stage family, as the reconciliation reads it.

    The index is identified by name and hash; its summaries are kept for
    the workloads and stages the reconciliation joins, with each stage's
    full metric medians and representations.
    """
    path = measurement.resolve_report_dir(report_dir) / f"{index_name}.json"
    reference = {"index": f"{index_name}.json", "present": path.is_file()}
    if not path.is_file():
        return reference
    entry = measurement.file_entry(path)
    document = json.loads(path.read_text(encoding="ascii"))
    wanted = {stage for _label, _categories, stages in RECONCILIATION_ROWS for stage, _ in stages}
    summaries = {}
    for summary in document.get("stage_summaries", []):
        if summary.get("stage") not in wanted or summary.get("compression") != "zstd":
            continue
        if configs and summary.get("workload_config_id") not in configs:
            continue
        key = f"{summary['stage']}/{summary['mode']}/{summary['workload_config_id']}"
        summaries[key] = {
            "metrics": {
                name: (summary.get("metrics", {}).get(name) or {}).get("median")
                for name in STAGE_METRICS
            },
            "input_representation": summary.get("input_representation"),
            "output_representation": summary.get("output_representation"),
            "denominator": summary.get("denominator"),
            "repetitions": summary.get("repetitions"),
        }
    reference.update(
        {
            "file": entry,
            "expected_sha256": expected_sha256,
            "verified": expected_sha256 is None or entry["sha256"] == expected_sha256,
            "status": document.get("status"),
            "git": (document.get("environment") or {}).get("git"),
            "family_ordinal": document.get("family_ordinal"),
            "family_scope": document.get("family_scope") or {"kind": "full"},
            "summaries": summaries,
        }
    )
    return reference


def stage_evidence(report_dir, options, configs) -> dict:
    """The pinned family and, when published, the spot family."""
    pinned_index = options.get("pinned_index", PINNED_STAGE_FAMILY["index"])
    pinned_sha = options.get("pinned_sha256", PINNED_STAGE_FAMILY["sha256"])
    pinned = stage_family_reference(
        report_dir, pinned_index, expected_sha256=pinned_sha, configs=configs
    )
    pinned["pin"] = dict(PINNED_STAGE_FAMILY)
    spot = stage_family_reference(
        report_dir, options.get("spot_index", SPOT_STAGE_INDEX), configs=configs
    )
    return {"pinned": pinned, "spot": spot}


def reference_cpu_ns_per_record(evidence, config_id):
    """The whole engine's expected cost per record, for sizing a profile.

    The real engine's noop pipeline plus the cumulative OTLP-to-object-store
    layer, from the pinned family: the named reference, never a
    supplementary family in its place.
    """
    total = 0.0
    for stage, mode in (("otlp_noop", "pipeline"), ("otlp_minio", "async")):
        key = f"{stage}/{mode}/{config_id}"
        value = None
        for family in ("pinned",):
            summary = (evidence.get(family) or {}).get("summaries", {}).get(key)
            if summary and _finite(summary["metrics"].get("cpu_ns_per_record")):
                value = summary["metrics"]["cpu_ns_per_record"]
                break
        if value is None:
            raise AssertionError(
                f"no stage family measured {key}; an attribution is sized from it"
            )
        total += value
    return total


def store_objects(store, prefix="otel/") -> list:
    """Every object under `prefix` in the store, with its size."""
    found = []
    for page in store.client.get_paginator("list_objects_v2").paginate(
        Bucket=store.bucket, Prefix=prefix
    ):
        for item in page.get("Contents", []):
            found.append((item["Key"], int(item["Size"])))
    return found


def clear_store(store, prefix="otel/") -> int:
    """Delete every object under `prefix`, so the next lifetime starts empty."""
    keys = [key for key, _size in store_objects(store, prefix)]
    for start in range(0, len(keys), 1000):
        _ = store.client.delete_objects(
            Bucket=store.bucket,
            Delete={"Objects": [{"Key": key} for key in keys[start:start + 1000]]},
        )
    if store_objects(store, prefix):
        raise AssertionError(f"the store still holds objects under {prefix}")
    return len(keys)


def _command():
    """The command-line module, imported late: it imports this one."""
    try:
        from . import measure as command
    except ImportError:
        import measure as command
    return command


def attribution_lifetime(label, plan, job, spec, result, run_dir, controls, *,
                         edge, last, profile) -> dict:
    """One engine lifetime of a repetition: ready, input, drain, oracle.

    The engine starts on the run's worker core against the pinned object
    store, the producer sends every prebuilt request exactly once from a
    window boundary, and the input phase -- first send to last durable
    acknowledgement -- is the measured window: the engine's CPU and each
    thread's scheduler times are read at both ends, and a profiled lifetime
    has perf's events enabled across exactly that window. After the drain
    proof the engine stops, the store's objects are read back by both
    readers against the ledger, and the objects and their local copies are
    deleted.
    """
    run_dir = Path(run_dir)
    prebuilt = plan["inputs"][job["config_id"]]["prebuilt"]
    workload = prebuilt.workload
    root = run_dir / f"engine-{label}"
    root.mkdir(parents=True, exist_ok=True)
    engine = test_e2e.Engine(
        root,
        storage=plan["storage"],
        overrides={"retry": test_e2e.S3_RETRY},
        interval=f"{spec.interval_s}s",
        cores=list(spec.cores),
        merge=plan["merge"],
        binary=Path(plan["provenance"]["build"]["binary"]),
    )
    # Everything after the engine exists runs under the `finally` that
    # closes it, so no failure can leave an engine behind on a measured core.
    phase = None
    ledger = None
    recorder = None
    lifetime = {"label": label, "profiled": profile, "pid": engine.pid,
                "config_sha256": engine.config_sha256}
    try:
        phase = _command().EnginePhase(label, engine, spec, controls, buffered=False)
        ledger = measurement.Ledger(ledger_path(plan, run_dir, label))
        _command().record_graph(result, engine, "strict")
        if last:
            result["config"]["effective"] = engine.config
            result["config"]["effective_sha256"] = engine.config_sha256
            result["config"]["edges"] = [list(edge_) for edge_ in engine.edges]
            result["ephemeral_values"]["<receiver_listening_addr>"] = (
                f"127.0.0.1:{engine.grpc_port}"
            )
        snapshot = phase.ready(edge)
        flush_before = flush_reading(test_e2e.engine_metrics(engine))
        workers = set(measurement.worker_tids(snapshot))
        if profile:
            recorder = PerfRecorder(run_dir / "perf", cores=plan["allocation"]["profiler"])
            _ = recorder.start(engine.pid)
            controls.register("profiler", recorder.pid, plan["allocation"]["profiler"])
        producer = measurement.Producer(
            engine.channel,
            ledger,
            workload,
            cores=plan["allocation"].get("producer", []),
            timeout_s=spec.producer_timeout_s,
            max_in_flight=spec.max_in_flight,
            source=prebuilt.request,
        )
        indexes = lifetime_indexes(prebuilt.indexes, profiled=profile)
        lifetime["request_indexes"] = {
            "first": indexes[0], "last": indexes[-1], "count": len(indexes),
        }
        _ = _command().await_window_start(spec.interval_s)
        controls.raise_if_invalid()
        lifetime["peak_rss_was_reset"] = reset_peak_rss(engine.pid)
        admission_before = exporter_counter(engine, "admission.closed.duration")
        threads_before = thread_times(engine.pid)
        split_before = procfs_user_system_ns(engine.pid)
        cpu_before = procfs_cpu_ns(engine.pid)
        if recorder is not None:
            _ = recorder.enable()
        window_start = time.monotonic_ns()
        outcome = producer.send(indexes)
        window_end = time.monotonic_ns()
        if recorder is not None:
            _ = recorder.disable()
        cpu_after = procfs_cpu_ns(engine.pid)
        split_after = procfs_user_system_ns(engine.pid)
        threads_after = thread_times(engine.pid)
        admission_after = exporter_counter(engine, "admission.closed.duration")
        phase.inputs.append(outcome)
        controls.raise_if_invalid()
        _ = phase.drained()
        lifetime["peak_rss_bytes"] = procfs_peak_rss(engine.pid)
        # The drain proof saw three empty collections, so every flush of the
        # input has completed and been collected by now.
        flush_after = flush_reading(test_e2e.engine_metrics(engine))
        if last:
            _ = controls.snapshot(
                "end",
                {"engine": (engine.pid, list(spec.cores))},
                workers=phase.workers,
                requested_cores=list(spec.cores),
            )
        controls.unwatch_workers()
        if recorder is not None:
            lifetime["perf_returncode"] = recorder.stop()
        engine.shutdown(SHUTDOWN_DEADLINE_S)
        _command().record_event(result, f"{label}_engine_shut_down", str(engine.pid))
    except BaseException:
        if ledger is not None:
            ledger.close()
        raise
    finally:
        controls.unwatch_workers()
        if phase is not None and phase.sampler is not None:
            phase.sampler.stop()
        if recorder is not None:
            recorder.close()
        engine.close()
        if phase is not None:
            lifetime["phase"] = phase.summary()
            lifetime["samples"] = [
                measurement.compact_sample(sample)
                for sample in (phase.sampler.samples if phase.sampler else [])
            ]
    window_ns = window_end - window_start
    counts = ledger.counts()
    latencies = ledger.acknowledgement_latencies_s()
    objects = store_objects(plan["store"])
    local = run_dir / f"store-{label}"
    plan["store"].download(local)
    try:
        oracle = measurement.run_pinned(
            plan["oracle_cores"],
            measurement.read_oracle,
            local,
            ledger,
            require_all=True,
            healthy=True,
            workload=workload,
        )
    finally:
        ledger.close()
        _ = clear_store(plan["store"])
        # The Parquet copies are reproducible and large; only their hashes
        # and counts are evidence.
        shutil.rmtree(local, ignore_errors=True)
    lifetime["ledger_file"] = retire_ledger(ledger.path)
    records = counts["records_acked_count"]
    cpu_ns = cpu_after - cpu_before
    flush_totals = flush_window(flush_before, flush_after)
    flush_totals["committed_blocks_count"] = committed_blocks(root / "engine.log")
    lifetime.update(
        {
            "records": records,
            "requests": len(indexes),
            "ledger": counts,
            "outcome": outcome,
            "window_s": window_ns / 1e9,
            "engine_cpu_ns": cpu_ns,
            "engine_user_cpu_ns": split_after["user_ns"] - split_before["user_ns"],
            "engine_system_cpu_ns": split_after["system_ns"] - split_before["system_ns"],
            "cpu_ns_per_record": cpu_ns / records if records else None,
            "throughput_records_per_s": records / (window_ns / 1e9) if window_ns > 0 else None,
            "ack_latency_p50_s": measurement.percentile(latencies, 0.50) if latencies else None,
            "ack_latency_p99_s": measurement.percentile(latencies, 0.99) if latencies else None,
            "schedule": thread_schedule(threads_before, threads_after, window_ns, workers),
            "admission_closed_s": admission_after - admission_before,
            "flush": flush_totals,
            "objects_count": len(objects),
            "object_bytes": sum(size for _key, size in objects),
            "output_bytes_per_input_record": (
                sum(size for _key, size in objects) / records if records else None
            ),
            "output_representation": "parquet_objects_in_the_store",
            "oracle": {
                key: oracle[key]
                for key in (
                    "passed", "part_file_count", "descriptor_identity_count",
                    "series_cardinality", "actual_row_count", "readers",
                    "expected_record_count", "missing_record_count",
                    "unexpected_record_count", "corrupt_record_count",
                    "multiplicity_histogram", "problems",
                )
                if key in oracle
            },
            "delivered": bool(
                oracle["passed"]
                and counts["requests_acked_count"] == counts["requests_attempted_count"]
                == len(indexes)
                and records == len(indexes) * workload.records_per_request
                and outcome["outcomes"].get(measurement.OUTCOME_PARTIAL, 0) == 0
            ),
        }
    )
    if recorder is not None:
        text = recorder.script(run_dir / "perf" / "perf-script.txt")
        parsed = parse_perf_script(text)
        lifetime["perf"] = recorder.as_json()
        lifetime["perf"]["parse"] = {
            key: parsed[key]
            for key in ("zero_weight_samples_count", "unparsed_lines_count", "unparsed_lines")
        }
        lifetime["profile"] = profile_summary(parsed["samples"])
        # The CPU the samples can have seen: user time alone when perf was
        # only allowed user space, all of it otherwise.
        sampled = (
            lifetime["engine_user_cpu_ns"]
            if lifetime["profile"]["user_space_only"] else cpu_ns
        )
        lifetime["perf"]["sampled_scope"] = (
            "user" if lifetime["profile"]["user_space_only"] else "user_and_kernel"
        )
        lifetime["perf"]["sampled_cpu_ns"] = sampled
        lifetime["perf"]["unsampled_kernel_cpu_ns"] = cpu_ns - sampled if sampled else None
        lifetime["perf"]["sampling_coverage_ratio"] = (
            lifetime["profile"]["total_weight"] / sampled if sampled > 0 else None
        )
    return lifetime


def oracle_cores(allocation, sibling_groups) -> list:
    """Where the read-back runs: the stopped engine's physical cores.

    The oracle starts DuckDB and clickhouse-local, which use every CPU they
    are given. It runs only after an engine has stopped, so it takes the
    engine's worker and reserved cores with their SMT siblings, and the
    build monitor, the harness and the store keep theirs: a starved monitor
    is a coverage gap, and a coverage gap invalidates the run.
    """
    owned = set(allocation.get("engine", [])) | set(allocation.get("engine_reserved", []))
    cores = set()
    for group in sibling_groups:
        if owned & set(group):
            cores |= set(group)
    cores |= owned
    available = set(os.sched_getaffinity(0))
    return sorted(cores & available) or sorted(available)


# Where a lifetime's ledger lives while it is written. The ledger commits
# every request with full synchronisation, three transactions a request; on
# a disk that is three fsyncs, and the sender then runs at the disk's fsync
# rate -- a first full run managed 15,000 records/s with the worker 9
# percent busy, which is the engine idling, not the engine under load. On
# the memory file system the same ledger costs nothing. Millions of records
# make it gigabytes, so it is deleted once the read-back has compared it,
# and the result keeps its hash, size and counts.
LEDGER_DIR_ENV = "SERIES_ATTRIBUTION_LEDGER_DIR"
LEDGER_DEFAULT_PREFIX = "series-attribution-ledgers-"


MEMORY_FILESYSTEMS = ("tmpfs", "ramfs")
MEMORY_FALLBACK = "/dev/shm"


def filesystem_of(path, mountinfo="/proc/self/mountinfo") -> dict:
    """The mount a path lives on and its file system type.

    The longest mount point that contains the path wins, as the kernel
    resolves it; the path need not exist yet.
    """
    target = os.path.realpath(str(path))
    best = {"mount_point": None, "fstype": None}
    for line in Path(mountinfo).read_text().splitlines():
        fields = line.split()
        if " - " not in line or len(fields) < 5:
            continue
        mount_point = fields[4].replace("\\040", " ")
        fstype = line.split(" - ", 1)[1].split()[0]
        inside = target == mount_point or target.startswith(mount_point.rstrip("/") + "/")
        if inside and len(mount_point) >= len(best["mount_point"] or ""):
            best = {"mount_point": mount_point, "fstype": fstype}
    return dict(best, path=target)


def memory_ledger_dir(requested, *, fallback=MEMORY_FALLBACK,
                      mountinfo="/proc/self/mountinfo") -> dict:
    """The ledger directory, proven to be on a memory file system.

    The requested directory is used when it is on tmpfs or ramfs;
    otherwise `fallback` is, when it is; otherwise the family is refused,
    because a disk-backed ledger throttles the sender to the disk's fsync
    rate and the profile then describes an idling engine.
    """
    for candidate, used_fallback in ((Path(requested), False),
                                     (Path(fallback) / Path(requested).name, True)):
        found = filesystem_of(candidate, mountinfo)
        if found["fstype"] in MEMORY_FILESYSTEMS:
            return dict(found, directory=str(candidate), fallback=used_fallback,
                        requested=str(requested))
    raise AssertionError(
        f"neither {requested} ({filesystem_of(requested, mountinfo)['fstype']}) nor "
        f"{fallback} is on a memory file system; a ledger on disk throttles the "
        f"sender to its fsync rate. Name a tmpfs with --option ledger_dir=..."
    )


def ledger_path(plan, run_dir, label) -> Path:
    """The ledger of one lifetime, on the ledger directory of the plan."""
    directory = Path(plan.get("ledger_dir") or run_dir) / Path(run_dir).name
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"ledger-{label}.sqlite"


def retire_ledger(path) -> dict:
    """The ledger's identity, after which the file and its journal go."""
    path = Path(path)
    entry = dict(measurement.file_entry(path), kind="ledger", retention="deleted")
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(f"{path}{suffix}")
        if candidate.exists():
            candidate.unlink()
    return entry


def stage_agreement(attributed_ns, residual_ns, measured_ns) -> dict:
    """The plan's Stage agreement quantities for one profile.

    `attributed_ns` is the sum of every category's sampled CPU, the named
    residual included; `residual_ns` is the named residual alone;
    `measured_ns` is what the scheduler says the engine used in the same
    window. The error is their relative difference; unexplained CPU is the
    residual plus whatever measured CPU no sample covered.
    """
    if not measured_ns or measured_ns <= 0:
        return {"error_ratio": None, "unexplained_ratio": None}
    return {
        "error_ratio": abs(attributed_ns - measured_ns) / measured_ns,
        "unexplained_ratio": (
            residual_ns + max(0, measured_ns - attributed_ns)
        ) / measured_ns,
    }


def lifetime_indexes(indexes, *, profiled) -> list:
    """The requests one lifetime sends: all of them, or the control's share.

    A control lifetime sends the first `CONTROL_FRACTION` of the input in
    whole blocks, so every block it seals rotates on its request count just
    as the profiled lifetime's do.
    """
    indexes = list(indexes)
    if profiled:
        return indexes
    blocks = max(1, int(len(indexes) * CONTROL_FRACTION) // ATTRIBUTION_BLOCK_REQUESTS)
    return indexes[: min(len(indexes), blocks * ATTRIBUTION_BLOCK_REQUESTS)]


def _lifetime_check(name, lifetimes, predicate, describe) -> dict:
    """One hard check passed only when every named lifetime passes it."""
    failed = [lifetime["label"] for lifetime in lifetimes if not predicate(lifetime)]
    return measurement.check(
        name,
        measurement.CHECK_HARD,
        measurement.STATUS_FAILED if failed or not lifetimes else measurement.STATUS_PASSED,
        "; ".join(describe(lifetime) for lifetime in lifetimes)
        + (f"; failed in {failed}" if failed else ""),
    )


def settle_attribution_child(result, plan, job, lifetimes, run_dir):
    """The checks, metrics and observations of one repetition."""
    checks = result["checks"]
    checks.append(
        _lifetime_check(
            "delivery", lifetimes, lambda lifetime: lifetime["delivered"],
            lambda lifetime: (
                f"{lifetime['label']}: acked "
                f"{lifetime['ledger']['requests_acked_count']}/{lifetime['requests']} "
                f"requests, {lifetime['records']} records, oracle problems "
                f"{lifetime['oracle'].get('problems')}"
            ),
        )
    )
    phases = [lifetime["phase"] for lifetime in lifetimes]
    problems = _command().sample_problems(phases)
    checks.append(
        measurement.check(
            "minimum_samples",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if problems or not phases else measurement.STATUS_PASSED,
            "; ".join(problems)
            or f"{sum(phase['sample_count'] for phase in phases)} samples",
        )
    )
    residual_checks = []
    for lifetime in lifetimes:
        peak = max(
            (sample["process_rss_bytes"] for sample in lifetime["samples"]), default=0
        )
        residual_checks.append(
            measurement.residual_check(lifetime["phase"]["residuals"], peak)
        )
    failed = [entry for entry in residual_checks if entry["status"] != measurement.STATUS_PASSED]
    checks.append(failed[0] if failed else residual_checks[0])
    for lifetime in lifetimes:
        flush = lifetime["flush"]
        checks.append(
            measurement.check(
                f"flush_epochs_complete_{lifetime['label']}",
                measurement.CHECK_MEASURED,
                measurement.STATUS_PASSED
                if flush["flush_count"] == flush["committed_blocks_count"]
                else measurement.STATUS_FAILED,
                f"telemetry counted {flush['flush_count']} flushes in the "
                f"lifetime; the engine log committed "
                f"{flush['committed_blocks_count']} blocks",
            )
        )
    control = lifetimes[0]
    profiled = next((lifetime for lifetime in lifetimes if lifetime["profiled"]), None)
    metrics = {
        "control_engine_cpu_ns_per_record": control["cpu_ns_per_record"],
        "control_throughput_records_per_s": control["throughput_records_per_s"],
    }
    observations = {"lifetimes": lifetimes}
    resolution = {}
    if profiled is not None:
        profile = profiled["profile"]
        perf = profiled["perf"]
        coverage = perf["sampling_coverage_ratio"]
        exclusive = sum(entry["weight"] for entry in profile["categories"].values())
        agreement = stage_agreement(
            profile["total_weight"],
            profile["categories"][UNKNOWN_CATEGORY]["weight"],
            profiled["engine_cpu_ns"],
        )
        for name, passed, detail in (
            (
                "perf_recorded",
                profiled.get("perf_returncode") in PERF_COMPLETE_EXITS
                and profile["samples_count"] > 0
                and perf["parse"]["unparsed_lines_count"] == 0,
                f"perf exited {profiled.get('perf_returncode')}; "
                f"{profile['samples_count']} samples, "
                f"{perf['parse']['unparsed_lines_count']} unparsed lines",
            ),
            (
                "stage_agreement_error",
                agreement["error_ratio"] is not None
                and agreement["error_ratio"] <= STAGE_AGREEMENT_ERROR_LIMIT,
                f"exclusive categories plus the named residual hold "
                f"{profile['total_weight']} ns of the {profiled['engine_cpu_ns']} ns "
                f"the scheduler measured ({perf['sampled_scope']} sampled): error "
                f"{agreement['error_ratio']}, limit {STAGE_AGREEMENT_ERROR_LIMIT}",
            ),
            (
                "attribution_exclusive",
                exclusive == profile["total_weight"],
                f"categories hold {exclusive} of {profile['total_weight']} ns",
            ),
            (
                "stage_agreement_unexplained",
                agreement["unexplained_ratio"] is not None
                and agreement["unexplained_ratio"] <= UNEXPLAINED_CPU_LIMIT,
                f"unexplained {agreement['unexplained_ratio']} of the measured CPU "
                f"(named residual and uncovered CPU), limit {UNEXPLAINED_CPU_LIMIT}; "
                f"heaviest residual "
                + json.dumps(profile["named_residual"][:3], sort_keys=True),
            ),
        ):
            checks.append(
                measurement.check(
                    name,
                    measurement.CHECK_HARD,
                    measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED,
                    detail,
                )
            )
        records = profiled["records"]
        # The categories divide the CPU the samples could see. Kernel time a
        # user-space-only profile could not see is reported beside them.
        cpu_ns = perf["sampled_cpu_ns"]
        flush_task_s = profile["flush_task_weight"] / 1e9
        metrics.update(
            {
                "engine_cpu_ns_per_record": profiled["cpu_ns_per_record"],
                "throughput_records_per_s": profiled["throughput_records_per_s"],
                "classified_samples_count": profile["classified_samples_count"],
                "flush_wall_s": profiled["flush"]["flush_wall_s"],
                "upload_wait_s": max(0.0, profiled["flush"]["flush_wall_s"] - flush_task_s),
                "peak_rss_bytes": profiled["peak_rss_bytes"],
            }
        )
        per_sample = (1e9 / PERF_FREQUENCY_HZ) / records if records else None
        with_allocator = {}
        for name in CPU_CATEGORIES:
            share = profile["categories"][name]["share_ratio"]
            metrics[f"{name}_cpu_ns_per_record"] = (
                share * cpu_ns / records if records else None
            )
            resolution[f"{name}_cpu_ns_per_record"] = per_sample
            added = (
                profile["allocator_callers"][name] / profile["total_weight"]
                if profile["total_weight"] and name != "allocator" else 0.0
            )
            with_allocator[name] = (share + added) * cpu_ns / records if records else None
        observations["attribution"] = {
            "categories": profile["categories"],
            "with_allocator_cpu_ns_per_record": with_allocator,
            "allocator_callers": profile["allocator_callers"],
            "named_residual": profile["named_residual"],
            "flush_task_cpu_s": flush_task_s,
            "cpu_s_by_logical_cpu": profile["cpu_s_by_logical_cpu"],
            "cpu_s_by_thread_name": profile["cpu_s_by_thread_name"],
            "worker_schedule": profiled["schedule"]["workers"],
            "admission_closed_s": profiled["admission_closed_s"],
            "sampling_coverage_ratio": coverage,
            "stage_agreement": agreement,
            "sampled_scope": perf["sampled_scope"],
            "unsampled_kernel_cpu_ns_per_record": (
                perf["unsampled_kernel_cpu_ns"] / records
                if records and perf["unsampled_kernel_cpu_ns"] is not None else None
            ),
            "engine_user_cpu_s": profiled["engine_user_cpu_ns"] / 1e9,
            "engine_system_cpu_s": profiled["engine_system_cpu_ns"] / 1e9,
        }
        observations["profile_overhead"] = {
            "cpu_per_record_ratio": (
                profiled["cpu_ns_per_record"] / control["cpu_ns_per_record"] - 1
                if control["cpu_ns_per_record"] else None
            ),
            "throughput_ratio": (
                1 - profiled["throughput_records_per_s"] / control["throughput_records_per_s"]
                if control["throughput_records_per_s"] else None
            ),
            "window_ratio": (
                profiled["window_s"] / control["window_s"] - 1 if control["window_s"] else None
            ),
        }
    result["metrics"] = metrics
    result["metric_directions"] = {
        name: ATTRIBUTION_METRIC_DIRECTIONS[name] for name in metrics
    }
    result["mandatory_metrics"] = sorted(metrics)
    result["resolution_floor"] = resolution
    for name, value in metrics.items():
        if value is None:
            result["metrics_unavailable"][name] = "no acknowledged input to measure"
    result["observations"] = observations
    result["samples"] = [
        dict(sample, lifetime=lifetime["label"])
        for lifetime in lifetimes
        for sample in lifetime.pop("samples")
    ]
    result["artifacts"] = [
        dict(measurement.file_entry(path), kind=kind, retention=str(run_dir))
        for path, kind in sorted(
            [(path, "engine_log") for path in Path(run_dir).glob("engine-*/engine.log")]
            + [(path, "engine_config") for path in Path(run_dir).glob("engine-*/pipeline.yaml")]
            + [(Path(run_dir) / "perf" / "perf.data", "perf_data"),
               (Path(run_dir) / "perf" / "perf-script.txt", "perf_script"),
               (Path(run_dir) / "perf" / "perf.log", "perf_log")]
        )
        if path.is_file()
    ]
    result["artifacts"].extend(
        dict(lifetime["ledger_file"], retention=f"deleted after the read-back of {lifetime['label']}")
        for lifetime in lifetimes if lifetime.get("ledger_file")
    )
    result["workload_config_id"] = job["config_id"]
    result["repetition"] = job["repetition"]
    result["family_ordinal"] = plan["family_ordinal"]
    result["status"] = measurement.STATUS_PASSED


def attribution_experiment(plan, job, spec, result, run_dir, controls):
    """One repetition: an unprofiled control lifetime, then a profiled one.

    Both run the same engine configuration, cores, store and prebuilt
    input, so their difference is perf's own cost. A rehearsal runs the
    control lifetime alone and records no attribution.
    """
    controls.coverage_gaps_hard = True
    controls.allocate(plan["allocation"])
    controls.register("harness", os.getpid())
    if plan.get("store_pid"):
        controls.register("store", plan["store_pid"], plan["store_cores"])
    result["environment"]["build"] = plan["provenance"]["build"]
    result["environment"]["git"] = plan["provenance"]["git"]
    # The setting is not persistent: a reboot restores the distribution's
    # default, so each repetition records the value it actually ran under.
    result["environment"]["perf_event_paranoid"] = perf_event_paranoid()
    result["environment"]["ledger_filesystem"] = plan.get("ledger_filesystem")
    result["ephemeral_values"] = dict(plan["ephemeral"])
    result["config"]["input"] = plan["inputs"][job["config_id"]]["prebuilt"].as_json()
    labels = ("control", "profiled") if plan["profile"] else ("control",)
    # A rehearsal's single lifetime is its control.
    lifetimes = []
    for position, label in enumerate(labels):
        lifetimes.append(
            attribution_lifetime(
                label, plan, job, spec, result, run_dir, controls,
                edge="start" if position == 0 else label,
                last=position == len(labels) - 1,
                profile=label == "profiled",
            )
        )
    settle_attribution_child(result, plan, job, lifetimes, run_dir)


def attribution_child_spec(plan, job) -> measurement.RunSpec:
    """The immutable inputs of one repetition."""
    prebuilt = plan["inputs"][job["config_id"]]["prebuilt"]
    cores = tuple(plan["cores"])
    return measurement.RunSpec(
        run_id=measurement.RunSpec.build_run_id(
            f"attribution-{job['config_id']}", "strict", "minio", cores,
            ATTRIBUTION_INTERVAL_S, job["ordinal"],
        ),
        case="attribution",
        topology="strict",
        store="minio",
        cores=cores,
        workload=prebuilt.workload,
        interval_s=ATTRIBUTION_INTERVAL_S,
        duration_s=1,
        max_in_flight=ATTRIBUTION_IN_FLIGHT,
        overrides={
            "receiver": {"max_concurrent_requests": ATTRIBUTION_IN_FLIGHT},
            "exporter": {"window": {"max_requests_per_block": ATTRIBUTION_BLOCK_REQUESTS}},
            "workload_config_id": job["config_id"],
            "repetition": job["repetition"],
            "profiled": plan["profile"],
        },
    )


# How many times one repetition is run again after a concurrent build
# invalidated it, and how long the host must stay free of builds first.
BUILD_RETRIES = 3
QUIET_HOST_S = 60


def invalidated_by_build(child) -> bool:
    """Whether a build seen inside the measured window invalidated a child."""
    return any(
        entry["name"] == "no_concurrent_build"
        and entry["status"] != measurement.STATUS_PASSED
        for entry in child.get("checks", [])
    )


def wait_for_quiet_host(*, quiet_s=QUIET_HOST_S, deadline_s=600.0, scan=None) -> dict:
    """Wait until no build has run on the host for `quiet_s` seconds.

    Another agent's compile is not this family's to stop; the repetition it
    invalidated is run again only once the host has been observed build-free
    for a whole quiet period, under a deadline.
    """
    scan = scan or measurement.build_activity
    started = time.monotonic_ns()
    state = {"quiet_since": None, "seen": []}

    def observe():
        """How long the host has been build-free, in seconds."""
        builds = scan()
        now = time.monotonic_ns()
        if builds:
            state["quiet_since"] = None
            state["seen"] = [
                {key: build.get(key) for key in ("pid", "comm")} for build in builds[:3]
            ]
            return 0.0
        if state["quiet_since"] is None:
            state["quiet_since"] = now
        return (now - state["quiet_since"]) / 1e9

    _ = measurement.wait_until(
        observe,
        lambda quiet: quiet >= quiet_s,
        deadline_ns=started + int(deadline_s * 10**9),
        description=f"{quiet_s}s without a build on the host",
    )
    return {
        "waited_s": (time.monotonic_ns() - started) / 1e9,
        "quiet_s": quiet_s,
        "last_builds_seen": state["seen"],
        "observed_utc": measurement.utc_now(),
    }


def attribution_child_ordinal(*directories) -> int:
    """The first repetition ordinal no published or local child has used."""
    highest = 0
    for directory in directories:
        directory = measurement.resolve_report_dir(directory)
        if not Path(directory).is_dir():
            continue
        for path in Path(directory).glob("attribution-*-r[0-9][0-9][0-9].json"):
            try:
                highest = max(highest, int(path.stem.rsplit("-r", 1)[1]))
            except (IndexError, ValueError):
                continue
    return highest + 1


def run_attribution_child(plan, job, output_dir, report_dir):
    """One repetition, whose failure is recorded, never raised."""
    try:  # Imported here: the command line imports this module in turn.
        from . import measure as command
    except ImportError:
        import measure as command
    spec = attribution_child_spec(plan, job)
    run_dir = Path(output_dir) / spec.run_id

    def experiment(spec, result, directory, controls):
        """This repetition's measured body."""
        attribution_experiment(plan, job, spec, result, directory, controls)

    try:
        # On a host other measurements share, a repetition may wait its turn
        # for the lease; it measures nothing until it holds it.
        result = command.run_case(
            spec, run_dir, experiment=experiment, report_dir=report_dir, evaluate=False,
            lease_wait_s=plan.get("lease_wait_s", 0.0),
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as error:  # noqa: BLE001 - recorded in the result
        sys.stderr.write(f"{spec.run_id}: {type(error).__name__}: {error}\n")
        result = json.loads((run_dir / f"{spec.run_id}.json").read_text(encoding="ascii"))
    _ = shutil.copyfile(run_dir / f"{spec.run_id}.json", Path(output_dir) / f"{spec.run_id}.json")
    result.setdefault("workload_config_id", job["config_id"])
    result.setdefault("repetition", job["repetition"])
    result["attempt"] = job.get("attempt", 1)
    return result


def _pooled_profile(children) -> dict:
    """Every repetition's profile statistics added together."""
    weights = collections.Counter()
    counts = collections.Counter()
    callers = collections.Counter()
    residual = collections.Counter()
    for child in children:
        attribution = (child.get("observations") or {}).get("attribution")
        if not attribution:
            continue
        for name, entry in attribution["categories"].items():
            weights[name] += entry["weight"]
            counts[name] += entry["samples_count"]
        for name, weight in attribution["allocator_callers"].items():
            callers[name] += weight
        for entry in attribution["named_residual"]:
            residual[entry["symbol"]] += entry["weight"]
    total_weight = sum(weights.values())
    total_count = sum(counts.values())
    categories = {}
    for name in CPU_CATEGORIES:
        share = weights[name] / total_weight if total_weight else 0.0
        categories[name] = {
            "weight": weights[name],
            "samples_count": counts[name],
            "share_ratio": share,
            "share_ci95_ratio": (
                CONFIDENCE_Z * math.sqrt(share * (1 - share) / total_count)
                if total_count else None
            ),
        }
    return {
        "total_weight": total_weight,
        "samples_count": total_count,
        "classified_samples_count": total_count - counts[UNKNOWN_CATEGORY],
        "categories": categories,
        "allocator_callers": dict(callers),
        "allocator_callers_share_ratio": {
            name: weight / total_weight if total_weight else 0.0
            for name, weight in callers.items()
        },
        "named_residual": [
            {"symbol": symbol, "weight": weight,
             "share_ratio": weight / total_weight if total_weight else 0.0}
            for symbol, weight in residual.most_common(10)
        ],
    }


def aggregate_attribution(children, *, plan, output_dir) -> dict:
    """One workload's repetitions as a comparable, baseline-evaluated result.

    The metrics are the medians of the repetitions. The profile shares are
    pooled over every repetition's samples, with their binomial intervals.
    The Controller baseline policy is applied here, to the family, not to
    each repetition, and never to a rehearsal.
    """
    children = sorted(children, key=lambda child: child["repetition"])
    first = children[0]
    config_id = first["workload_config_id"]
    run_id = (
        f"attribution-{config_id}-strict-minio-c{len(plan['cores'])}"
        f"-w{ATTRIBUTION_INTERVAL_S}-f{plan['family_ordinal']:03d}"
    )
    result = measurement.new_result(
        {"run_id": run_id, "case": "attribution"}, artifact_kind="attribution_aggregate"
    )
    environment = first.get("environment") or {}
    result["environment"] = {
        "start": environment.get("start") or {},
        "end": (children[-1].get("environment") or {}).get("end") or {},
        "build": environment.get("build"),
        "git": environment.get("git") or {},
        "core_allocation": environment.get("core_allocation"),
        "machine_identity_sha256": environment.get("machine_identity_sha256"),
        "children": [child["run_id"] for child in children],
    }
    if result["environment"]["start"] and result["environment"]["end"]:
        result["environment"]["match"] = measurement.environment_match(
            result["environment"]["start"], result["environment"]["end"]
        )
    for edge in ("start", "end"):
        if not result["environment"][edge]:
            result["environment"][edge] = measurement.environment_snapshot(
                {"harness": os.getpid()}
            )
            result["environment"][edge]["fallback"] = (
                "no repetition recorded this edge; this is the harness process only"
            )
    result["config"] = first.get("config") or {"requested": {}, "effective": {}}
    result["workload"] = first.get("workload") or {}
    result["workload_schedule"] = first.get("workload_schedule") or {}
    result["ephemeral_values"] = first.get("ephemeral_values") or {}
    result["run_dir"] = str(output_dir)
    result["workload_config_id"] = config_id
    names = sorted(
        set.intersection(*[set(child.get("metrics") or {}) for child in children])
    ) if children else []
    by_metric = {
        name: [
            child["metrics"][name] for child in children
            if _finite((child.get("metrics") or {}).get(name))
        ]
        for name in names
    }
    result["metrics"] = {
        name: (median(values) if values else None) for name, values in by_metric.items()
    }
    for name, value in result["metrics"].items():
        if value is None:
            result["metrics_unavailable"][name] = "no repetition measured it"
    result["metric_directions"] = {
        name: ATTRIBUTION_METRIC_DIRECTIONS[name] for name in result["metrics"]
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    floors = collections.defaultdict(list)
    for child in children:
        for name, value in (child.get("resolution_floor") or {}).items():
            if _finite(value):
                floors[name].append(value)
    result["resolution_floor"] = {
        name: max(values) for name, values in floors.items() if name in result["metrics"]
    }
    pooled = _pooled_profile(children)
    stable = ["engine_cpu_ns_per_record", "control_engine_cpu_ns_per_record"] + [
        f"{name}_cpu_ns_per_record"
        for name in CPU_CATEGORIES
        if pooled["categories"][name]["share_ratio"] >= STABLE_SHARE
    ]
    unstable = sorted(
        name for name in stable
        if name in by_metric and (
            not by_metric[name] or coefficient_of_variation(by_metric[name]) > MAXIMUM_CV
        )
    )
    checks = derived_checks(children)
    wanted = set(range(1, plan["repetitions"] + 1))
    present = {child.get("repetition") for child in children}
    missing = sorted(wanted - present)
    checks.append(
        measurement.check(
            "repetitions_complete",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if missing else measurement.STATUS_PASSED,
            f"repetitions {missing} have no valid run" if missing
            else f"all {len(wanted)} repetitions measured",
        )
    )
    for name in ("rss_reconciliation",) + (
        ("perf_recorded", "stage_agreement_error", "attribution_exclusive",
         "stage_agreement_unexplained") if plan["profile"] else ()
    ):
        failed = [
            child["run_id"] for child in children
            if not any(
                entry["name"] == name and entry["status"] == measurement.STATUS_PASSED
                for entry in child.get("checks", [])
            )
        ]
        checks.append(
            measurement.check(
                name,
                measurement.CHECK_HARD,
                measurement.STATUS_FAILED if failed else measurement.STATUS_PASSED,
                f"not passed in {failed}" if failed
                else f"passed in all {len(children)} repetitions",
            )
        )
    checks.append(
        measurement.check(
            "repetition_stability",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED
            if unstable or len(children) < plan["repetitions"]
            else measurement.STATUS_PASSED,
            f"{len(children)} repetitions; coefficients of variation "
            + json.dumps(
                {
                    name: round(coefficient_of_variation(by_metric[name]), 4)
                    for name in stable if by_metric.get(name)
                },
                sort_keys=True,
            )
            + (f"; over {MAXIMUM_CV:.0%} in {unstable}" if unstable else ""),
        )
    )
    if plan["profile"]:
        checks.append(
            measurement.check(
                "classified_samples_sufficient",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED
                if pooled["classified_samples_count"] >= plan["minimum_samples"]
                else measurement.STATUS_FAILED,
                f"{pooled['classified_samples_count']} classified samples over "
                f"{len(children)} repetitions, {plan['minimum_samples']} required; "
                f"lengthen the profile with --option cpu_ns_per_record=... or "
                f"--option records=... if short",
            )
        )
        # The reference the family is reconciled against must be the one
        # named by hash before anything this family measured can become a
        # baseline: every binding gate is decided before the policy runs.
        pinned = (plan.get("evidence") or {}).get("pinned") or {}
        checks.append(
            measurement.check(
                "reference_family_verified",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED
                if pinned.get("present") and pinned.get("verified")
                else measurement.STATUS_FAILED,
                f"{pinned.get('index')}: present {pinned.get('present')}, sha256 "
                f"{(pinned.get('file') or {}).get('sha256')}, expected "
                f"{pinned.get('expected_sha256')}",
            )
        )
    result["checks"] = checks
    overheads = [
        (child.get("observations") or {}).get("profile_overhead") or {}
        for child in children
    ]
    result["observations"] = {
        "repetitions": [
            {
                "repetition": child["repetition"],
                "run_id": child["run_id"],
                "status": child["status"],
                "metrics": child["metrics"],
            }
            for child in children
        ],
        "dispersion": {
            name: {
                "median": median(values) if values else None,
                "min": min(values) if values else None,
                "max": max(values) if values else None,
                "coefficient_of_variation": coefficient_of_variation(values),
            }
            for name, values in sorted(by_metric.items())
        },
        "pooled_profile": pooled,
        "stage_agreement": {
            "error_limit": STAGE_AGREEMENT_ERROR_LIMIT,
            "unexplained_limit": UNEXPLAINED_CPU_LIMIT,
            "repetitions": [
                (
                    ((child.get("observations") or {}).get("attribution") or {})
                    .get("stage_agreement")
                )
                for child in children
            ],
        },
        "profile_overhead": {
            key: median(values) if values else None
            for key in ("cpu_per_record_ratio", "throughput_ratio", "window_ratio")
            for values in [[entry[key] for entry in overheads if _finite(entry.get(key))]]
        },
    }
    result["run_files"] = [
        measurement.file_entry(Path(output_dir) / f"{child['run_id']}.json")
        for child in children
    ]
    result["status"] = (
        measurement.STATUS_PASSED
        if all(entry["status"] == measurement.STATUS_PASSED for entry in checks)
        else measurement.STATUS_FAILED
    )
    if plan["profile"]:
        try:
            decision = measurement.evaluate_baseline(result)
            if decision["action"] == "created":
                candidate = result.pop("baseline_candidate")
                path = measurement.write_published_json(
                    Path(output_dir) / decision["baseline_name"], candidate
                )
                result["baseline_files"].append(measurement.file_entry(path))
        except AssertionError as error:
            result["status"] = measurement.STATUS_FAILED
            measurement.record_event(result, "baseline_policy_failed", str(error))
            result["checks"].append(
                measurement.check(
                    "baseline_policy", measurement.CHECK_HARD,
                    measurement.STATUS_FAILED, str(error),
                )
            )
    result.pop("baseline_candidate", None)
    _ = measurement.write_result(Path(output_dir) / f"{run_id}.json", result)
    return result


def _family_summary(evidence, family, key):
    """One family's summary of one stage key, or None."""
    return ((evidence.get(family) or {}).get("summaries") or {}).get(key)


def _passed(result, name) -> bool:
    """Whether a result recorded the named check and every copy passed."""
    recorded = [entry for entry in result.get("checks", []) if entry["name"] == name]
    return bool(recorded) and all(
        entry["status"] == measurement.STATUS_PASSED for entry in recorded
    )


# The checks that make up the plan's binding Stage agreement rule for one
# workload's aggregate, beside the verified reference and exclusivity.
BINDING_RECONCILIATION_CHECKS = (
    "attribution_exclusive",
    "stage_agreement_error",
    "stage_agreement_unexplained",
    "reference_family_verified",
)


def reconcile_attribution(aggregates, evidence) -> dict:
    """The binding reconciliation, and the descriptive stage comparison.

    Binding, a hard gate: the plan's Stage agreement rule. In every
    repetition of every workload, exclusive category CPU plus the named
    residual must be within `STAGE_AGREEMENT_ERROR_LIMIT` of the measured
    engine CPU, and unexplained CPU within `UNEXPLAINED_CPU_LIMIT`; the
    profile must be exclusive, and the pinned stage family must be the one
    named by hash.

    Descriptive, never a gate: each attributed stage cost against its
    isolated bench cost in the pinned family, the reference, with the spot
    family reported beside it as supplementary evidence and never
    substituted. A row outside `AGREEMENT_BOUNDS` is explained only by
    published measurement -- the supplementary family bringing the same
    row into the band -- or is marked unexplained. Byte rates are listed per
    stage with their own output representation and never added across
    stages.
    """
    problems = []
    pinned = evidence.get("pinned") or {}
    spot = evidence.get("spot") or {}
    if not pinned.get("present"):
        problems.append(f"the pinned stage family {pinned.get('index')} is not published")
    elif not pinned.get("verified"):
        problems.append(
            f"the pinned stage family {pinned['index']} hashes to "
            f"{pinned['file']['sha256']}, not {pinned.get('expected_sha256')}"
        )
    low, high = AGREEMENT_BOUNDS
    workloads = {}
    for aggregate in aggregates:
        config_id = aggregate["workload_config_id"]
        metrics = aggregate.get("metrics") or {}
        failed = [
            name for name in BINDING_RECONCILIATION_CHECKS if not _passed(aggregate, name)
        ]
        if failed:
            problems.append(f"{config_id}: binding checks not passed: {failed}")
        pooled = (aggregate.get("observations") or {}).get("pooled_profile") or {}
        agreement = (aggregate.get("observations") or {}).get("stage_agreement") or {}
        engine = metrics.get("engine_cpu_ns_per_record")
        rows = []
        for label, categories, stages in RECONCILIATION_ROWS:
            if label == "total":
                attributed = engine
                with_allocator = engine
            else:
                attributed = sum(
                    metrics.get(f"{name}_cpu_ns_per_record") or 0.0 for name in categories
                )
                added = sum(
                    (pooled.get("allocator_callers_share_ratio") or {}).get(name, 0.0)
                    for name in categories
                )
                with_allocator = attributed + added * engine if _finite(engine) else None
            joined = []
            reference = 0.0
            supplementary = 0.0
            complete = True
            spot_complete = bool(spot.get("present"))
            for stage, mode in stages:
                key = f"{stage}/{mode}/{config_id}"
                summary = _family_summary(evidence, "pinned", key)
                extra = _family_summary(evidence, "spot", key)
                if summary is None:
                    complete = False
                    problems.append(f"{config_id}: the pinned family has no {key}")
                    continue
                if summary.get("denominator") != DENOMINATOR:
                    problems.append(f"{key} divides by {summary.get('denominator')!r}")
                cost = summary["metrics"].get("cpu_ns_per_record")
                reference += cost if _finite(cost) else 0.0
                extra_cost = (extra or {}).get("metrics", {}).get("cpu_ns_per_record")
                if _finite(extra_cost):
                    supplementary += extra_cost
                else:
                    spot_complete = False
                joined.append(
                    {
                        "stage": stage,
                        "mode": mode,
                        "reference": "pinned",
                        "metrics": summary["metrics"],
                        "supplementary_spot_cpu_ns_per_record": extra_cost,
                        "input_representation": summary.get("input_representation"),
                        "output_representation": summary.get("output_representation"),
                        "denominator": summary.get("denominator"),
                    }
                )
            ratio = (
                with_allocator / reference
                if complete and reference and _finite(with_allocator) else None
            )
            spot_ratio = (
                with_allocator / supplementary
                if spot_complete and supplementary and _finite(with_allocator) else None
            )
            if ratio is None:
                verdict, explanation = "not_measured", None
            elif low <= ratio <= high:
                verdict, explanation = "within_band", None
            elif spot_ratio is not None and low <= spot_ratio <= high:
                verdict = "outside_band"
                explanation = {
                    "status": "explained",
                    "evidence": {
                        "index": spot.get("index"),
                        "sha256": (spot.get("file") or {}).get("sha256"),
                        "git": spot.get("git"),
                    },
                    "detail": (
                        f"the supplementary stage family, measured at a later "
                        f"revision, gives a ratio of {spot_ratio:.3f}, inside the "
                        f"band: the stage's own cost changed after the pinned "
                        f"family was measured"
                    ),
                }
            else:
                verdict = "outside_band"
                explanation = {
                    "status": "unexplained",
                    "detail": (
                        "no published measurement brings this row into the band"
                        + (
                            f"; the supplementary family gives {spot_ratio:.3f}"
                            if spot_ratio is not None
                            else "; the supplementary family did not measure it"
                        )
                    ),
                }
            rows.append(
                {
                    "row": label,
                    "attribution_categories": list(categories),
                    "attributed_exclusive_cpu_ns_per_record": attributed,
                    "attributed_with_allocator_cpu_ns_per_record": with_allocator,
                    "share_ratio": sum(
                        (pooled.get("categories") or {}).get(name, {}).get("share_ratio", 0.0)
                        for name in categories
                    ),
                    "stages": joined,
                    "reference_cpu_ns_per_record": reference if complete else None,
                    "ratio_to_reference": ratio,
                    "supplementary_spot_cpu_ns_per_record": (
                        supplementary if spot_complete else None
                    ),
                    "ratio_to_supplementary": spot_ratio,
                    "verdict": verdict,
                    "explanation": explanation,
                }
            )
        workloads[config_id] = {
            "binding": {
                "checks": {
                    name: _passed(aggregate, name) for name in BINDING_RECONCILIATION_CHECKS
                },
                "repetitions": agreement.get("repetitions"),
                "error_limit": STAGE_AGREEMENT_ERROR_LIMIT,
                "unexplained_limit": UNEXPLAINED_CPU_LIMIT,
            },
            "engine_cpu_ns_per_record": engine,
            "upload_wait_s": metrics.get("upload_wait_s"),
            "flush_wall_s": metrics.get("flush_wall_s"),
            "descriptive_stage_comparison": rows,
        }
    return {
        "valid": not problems and bool(aggregates),
        "problems": problems,
        "binding_rule": (
            "the plan's Stage agreement row: exclusive category CPU plus the "
            "named residual against the measured engine CPU; an error above "
            f"{STAGE_AGREEMENT_ERROR_LIMIT:.0%} or unexplained CPU above "
            f"{UNEXPLAINED_CPU_LIMIT:.0%} invalidates the attribution"
        ),
        "descriptive_stage_comparison": {
            "gating": False,
            "reference": "pinned",
            "supplementary": "spot",
            "agreement_bounds": list(AGREEMENT_BOUNDS),
            "note": (
                "engine-attributed stage costs against isolated bench costs; "
                "descriptive only, not an acceptance model"
            ),
        },
        "references": {
            family: {
                key: value for key, value in (evidence.get(family) or {}).items()
                if key != "summaries"
            }
            for family in ("pinned", "spot")
        },
        "spot_revision_matches": (
            bool(spot.get("present"))
            and (spot.get("git") or {}).get("revision")
            == (next(iter(aggregates), {}).get("environment", {}).get("git") or {}).get("revision")
        ),
        "denominator": DENOMINATOR,
        "byte_rates": (
            "each stage row carries its own output_bytes_per_input_record with "
            "its output representation; no byte rate is added across stages"
        ),
        "workloads": workloads,
    }


def publish_attribution(spec, plan, children, aggregates, output_dir, report_dir,
                        started, *, preflight, invalidated=()) -> dict:
    """Write `attribution.json` over every repetition and aggregate.

    A host where perf cannot attach publishes the index with the preflight's
    evidence, no repetitions and the mandatory acceptance marked incomplete;
    a rehearsal publishes nowhere but its own directory.
    """
    output_dir = Path(output_dir)
    result = measurement.new_result(
        {"run_id": "attribution", "case": "attribution"}, artifact_kind="index"
    )
    result["environment"]["start"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    result["environment"]["core_allocation"] = plan["allocation"]
    result["environment"]["git"] = plan.get("git")
    result["environment"]["build"] = (plan.get("provenance") or {}).get("build")
    result["environment"]["host_at_start"] = plan.get("host_at_start")
    result["environment"]["host_at_end"] = host_neighbours(exclude=(os.getpid(),))
    result["family_ordinal"] = plan["family_ordinal"]
    result["environment"]["perf_event_paranoid"] = perf_event_paranoid()
    result["environment"]["ledger_filesystem"] = plan.get("ledger_filesystem")
    result["preflight"] = preflight
    result["classification"] = classification_rules()
    result["perf"] = {
        "record_argv": perf_record_argv(
            "perf", "<engine_pid>", "<run_dir>/perf/perf.data",
            "<run_dir>/perf/perf.ctl", "<run_dir>/perf/perf.ack",
        ),
        "script_argv": perf_script_argv("perf", "<run_dir>/perf/perf.data"),
        "event": PERF_EVENT,
        "frequency_hz": PERF_FREQUENCY_HZ,
        "call_graph": PERF_CALL_GRAPH,
        "weight": "the cpu-clock sample period, in nanoseconds of CPU",
        "window": (
            "events are enabled after the start snapshot and the window "
            "boundary, immediately before the first request, and disabled "
            "immediately after the last durable acknowledgement"
        ),
        "minimum_classified_samples_per_workload": plan["minimum_samples"],
    }
    result["workloads"] = {
        config_id: {
            "stage_workload": WORKLOAD_CONFIGS[config_id]["workload"].as_json(),
            "sizing_cpu_ns_per_record": entry.get("sizing_cpu_ns_per_record"),
            "input": entry["prebuilt"].as_json() if entry.get("prebuilt") else None,
        }
        for config_id, entry in plan["inputs"].items()
    }
    result["engine_configuration"] = {
        "block_requests": ATTRIBUTION_BLOCK_REQUESTS,
        "in_flight_requests": ATTRIBUTION_IN_FLIGHT,
        "window_interval_s": ATTRIBUTION_INTERVAL_S,
        "store": STORE_KIND,
        "roles": [list(role) for role in measurement.CASE_ROLES["attribution"]],
    }
    checks = result["checks"]
    attached = bool(preflight.get("attached"))
    rehearsal = not plan["profile"]
    if not rehearsal:
        checks.append(
            measurement.check(
                "perf_attached",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED if attached else measurement.STATUS_FAILED,
                "perf attached to a busy process and unwound its samples"
                if attached else preflight.get("reason", "perf could not attach"),
            )
        )
    for aggregate in aggregates:
        checks.append(
            measurement.check(
                f"child_{aggregate['run_id']}",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED
                if aggregate["status"] == measurement.STATUS_PASSED
                else measurement.STATUS_FAILED,
                f"{aggregate['run_id']}: {aggregate['status']}; failed "
                + json.dumps(
                    sorted(
                        entry["name"] for entry in aggregate["checks"]
                        if entry["status"] != measurement.STATUS_PASSED
                    )
                ),
            )
        )
    if plan["profile"] and attached:
        measured = {aggregate["workload_config_id"] for aggregate in aggregates}
        for config_id in plan["inputs"]:
            if config_id not in measured:
                checks.append(
                    measurement.check(
                        f"workload_measured_{config_id}",
                        measurement.CHECK_HARD,
                        measurement.STATUS_FAILED,
                        f"{config_id} has no valid repetition to aggregate; "
                        f"every attempt is listed in invalidated_children",
                    )
                )
    reconciliation = reconcile_attribution(aggregates, plan["evidence"])
    result["reconciliation"] = reconciliation
    if aggregates and not rehearsal:
        checks.append(
            measurement.check(
                "reconciliation_valid",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED if reconciliation["valid"]
                else measurement.STATUS_FAILED,
                "; ".join(reconciliation["problems"][:5])
                or reconciliation["binding_rule"] + ": held in every workload",
            )
        )
    result["metrics"] = {
        "children_count": len(children),
        "children_passed_count": sum(
            1 for child in children if child["status"] == measurement.STATUS_PASSED
        ),
        "aggregates_count": len(aggregates),
        "aggregates_passed_count": sum(
            1 for aggregate in aggregates
            if aggregate["status"] == measurement.STATUS_PASSED
        ),
        "classified_samples_count": sum(
            ((aggregate.get("observations") or {}).get("pooled_profile") or {}).get(
                "classified_samples_count", 0
            )
            for aggregate in aggregates
        ),
        "repetitions_count": plan["repetitions"],
        "workloads_count": len(plan["inputs"]),
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    result["children"] = [
        {
            "run_id": child["run_id"],
            "status": child["status"],
            "workload_config_id": child.get("workload_config_id"),
            "repetition": child.get("repetition"),
            "metrics": child["metrics"],
            "failed_checks": sorted(
                entry["name"] for entry in child["checks"]
                if entry["status"] != measurement.STATUS_PASSED
            ),
        }
        for child in children
    ]
    invalidated = list(invalidated)
    # Repetitions a concurrent build invalidated stay evidence -- published,
    # hashed and named -- but no aggregate reads them.
    result["invalidated_children"] = [
        {
            "run_id": child["run_id"],
            "workload_config_id": child.get("workload_config_id"),
            "repetition": child.get("repetition"),
            "failed_checks": sorted(
                entry["name"] for entry in child["checks"]
                if entry["status"] != measurement.STATUS_PASSED
            ),
        }
        for child in invalidated
    ]
    result["quiet_host_waits"] = plan.get("quiet_waits", [])
    result["run_files"] = [
        measurement.file_entry(output_dir / f"{document['run_id']}.json")
        for document in list(children) + invalidated + list(aggregates)
    ]
    result["baseline_files"] = [
        entry for aggregate in aggregates for entry in aggregate["baseline_files"]
    ]
    previous = measurement.archive_published_index("attribution.json", output_dir, report_dir)
    result["child_indexes"] = [previous] if previous else []
    passed = bool(checks) and all(
        entry["status"] == measurement.STATUS_PASSED for entry in checks
    )
    if rehearsal:
        result["status"] = measurement.STATUS_SKIPPED
        result["acceptance"] = {
            "mandatory": "incomplete",
            "reason": "a rehearsal runs unprofiled lifetimes only and is never published",
        }
    elif not attached:
        result["status"] = measurement.STATUS_SKIPPED
        result["acceptance"] = {
            "mandatory": "incomplete",
            "reason": (
                "perf could not attach on this host, so the attribution run was "
                "skipped; it stays incomplete until it is run on a host that "
                "permits perf_event_open for this user. "
                + preflight.get("reason", "")
            ),
        }
    else:
        result["status"] = measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED
        result["acceptance"] = {
            "mandatory": "complete" if passed else "failed",
            "reason": "every hard check passed" if passed else "a hard check failed",
        }
    result["environment"]["end"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"]
    )
    result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
    written = measurement.write_result(output_dir / "attribution.json", result)
    _ = measurement.publish_result_tree(written, report_dir)
    return result


def run_attribution(spec: measurement.RunSpec, output_dir, report_dir=None,
                    **options) -> dict:
    """Implement `measure attribution`: profile the real engine, reconcile.

    Before any lease: the perf preflight, the build provenance, the stage
    evidence, the prebuilt inputs and the pinned object store. Then, for each
    workload and repetition, one child under the host controls with a
    control lifetime and a profiled lifetime; then one aggregate per
    workload and `attribution.json`. A host where perf cannot attach skips
    every repetition and publishes the index with acceptance incomplete.

    `rehearsal=true` runs the control lifetimes alone into `output_dir`,
    publishing nothing, to prove the harness and the sizing on a host that
    cannot profile yet.
    """
    try:
        from . import measure as command
    except ImportError:
        import measure as command
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    rehearsal = bool(options.get("rehearsal", False))
    # The stage evidence is always read from the report directory; a
    # rehearsal only publishes somewhere else.
    evidence_dir = report_dir
    if rehearsal:
        report_dir = output_dir
    configs = list(options.get("configs") or PRIMARY_CONFIGS)
    unknown = sorted(set(configs) - set(PRIMARY_CONFIGS))
    if unknown:
        raise AssertionError(
            f"attribution joins the stage family's primary workloads "
            f"{list(PRIMARY_CONFIGS)}, not {unknown}"
        )
    repetitions = int(options.get("repetitions", ATTRIBUTION_REPETITIONS))
    topology = measurement.core_topology()
    allocation = measurement.role_allocation(
        topology["sibling_groups"], sorted(os.sched_getaffinity(0)), spec.cores,
        roles=measurement.CASE_ROLES["attribution"],
    )
    plan = {
        "profile": not rehearsal,
        "repetitions": repetitions,
        "lease_wait_s": float(options.get("lease_wait_s", 0.0)),
        "ledger_filesystem": memory_ledger_dir(
            options.get("ledger_dir") or os.environ.get(LEDGER_DIR_ENV)
            or Path("/tmp") / f"{LEDGER_DEFAULT_PREFIX}{os.getpid()}"
        ),
        "minimum_samples": int(options.get("minimum_samples", ATTRIBUTION_MINIMUM_SAMPLES)),
        "allocation": allocation,
        "oracle_cores": oracle_cores(allocation, topology["sibling_groups"]),
        "cores": list(spec.cores),
        "family_ordinal": attribution_family_ordinal(report_dir),
        "evidence": stage_evidence(evidence_dir, options, configs),
        "git": measurement.git_provenance(),
        "host_at_start": host_neighbours(exclude=(os.getpid(),)),
        "inputs": {config_id: {} for config_id in configs},
    }
    # A directory of this run's own inside the chosen one, so the cleanup
    # at the end can never remove anything it did not create.
    plan["ledger_dir"] = str(
        Path(plan["ledger_filesystem"]["directory"]) / f"attribution-run-{os.getpid()}"
    )
    if rehearsal:
        preflight = {"attached": False, "rehearsal": True,
                     "reason": "a rehearsal does not profile"}
    else:
        # The engines are built and proven one engine before perf is tried:
        # a profile of an engine that is not the canonical one describes
        # nothing this family may claim.
        engines = prepare_profiled_engine(
            output_dir / "engines", lease_wait_s=plan["lease_wait_s"],
        )
        if not engines["valid"]:
            preflight = {
                "attached": False,
                "perf_event_paranoid": perf_event_paranoid(),
                "engine_provenance": engines,
                "reason": "the profiled engine is not proven to be the canonical "
                "release engine: " + "; ".join(engines["problems"]),
            }
        else:
            preflight = perf_preflight(
                output_dir / "preflight", cores=allocation["profiler"],
                engine=attribution_engine(),
            )
            preflight["engine_provenance"] = engines
        if not preflight["attached"]:
            return publish_attribution(
                spec, plan, [], [], output_dir, report_dir, started, preflight=preflight
            )
    if rehearsal:
        plan["provenance"] = command.prepare_build()
    else:
        build = dict(engines["profiled"])
        build["canonical"] = engines["canonical"]
        build["rustflags_difference"] = engines["rustflags_difference"]
        build["build_commands"] = engines["commands"]
        build["identical_function_symbols"] = True
        plan["provenance"] = {"build": build, "git": engines["git"]}
    plan["merge"] = command.engine_merge(
        dataclasses.replace(
            spec,
            overrides={
                "receiver": {"max_concurrent_requests": ATTRIBUTION_IN_FLIGHT},
                "exporter": {"window": {"max_requests_per_block": ATTRIBUTION_BLOCK_REQUESTS}},
            },
        )
    )
    # A rehearsal is sized like the profiled family it stands in for.
    sizing_repetitions = repetitions if not rehearsal else ATTRIBUTION_REPETITIONS
    for config_id in configs:
        sizing = options.get("cpu_ns_per_record") or reference_cpu_ns_per_record(
            plan["evidence"], config_id
        )
        workload = attribution_workload(
            config_id, sizing, repetitions=sizing_repetitions,
            minimum_samples=plan["minimum_samples"], records=options.get("records"),
        )
        plan["inputs"][config_id] = {
            "sizing_cpu_ns_per_record": sizing,
            "prebuilt": PrebuiltRequests.build(
                workload, WORKLOAD_CONFIGS[config_id]["signal"],
                output_dir / "inputs" / f"{config_id}-{workload.requests}",
            ),
        }
    children = []
    invalidated = []
    plan["quiet_waits"] = []
    store = test_e2e.DockerStore(STORE_KIND)
    try:
        _ = store.__enter__()
        plan["store"] = store
        plan["storage"] = dict(store.storage)
        plan["store_pid"] = container_pid(store.container)
        plan["store_cores"] = allocation.get("store", [])
        plan["store_pinned"] = pin_container(store.container, plan["store_cores"])
        plan["ephemeral"] = {"<store_endpoint>": store.endpoint}
        ordinals = iter(range(attribution_child_ordinal(report_dir, output_dir), 10**6))
        for config_id in configs:
            for repetition in range(1, repetitions + 1):
                for attempt in range(1, BUILD_RETRIES + 2):
                    job = {
                        "config_id": config_id,
                        "repetition": repetition,
                        "ordinal": next(ordinals),
                        "attempt": attempt,
                    }
                    # Starting beside a running build would only produce an
                    # invalidated repetition; the host is let settle first.
                    if measurement.build_activity():
                        plan["quiet_waits"].append(
                            wait_for_quiet_host(deadline_s=max(plan["lease_wait_s"], 600.0))
                        )
                    child = run_attribution_child(plan, job, output_dir, report_dir)
                    if not invalidated_by_build(child):
                        children.append(child)
                        break
                    # A compiler inside the measured window invalidates the
                    # repetition: it is kept as evidence, never aggregated, and
                    # the repetition runs again once the host is quiet. When
                    # the retries run out it stays missing, and the aggregate's
                    # completeness gate says so.
                    invalidated.append(child)
                    if attempt > BUILD_RETRIES:
                        break
                    plan["quiet_waits"].append(
                        wait_for_quiet_host(deadline_s=max(plan["lease_wait_s"], 600.0))
                    )
    finally:
        store.__exit__(None, None, None)
        # A lifetime that failed before its read-back leaves its ledger.
        shutil.rmtree(plan["ledger_dir"], ignore_errors=True)
        # The default parent is this run's own too; a directory the caller
        # named is left as it was found.
        parent = Path(plan["ledger_dir"]).parent
        if parent.name == f"{LEDGER_DEFAULT_PREFIX}{os.getpid()}":
            shutil.rmtree(parent, ignore_errors=True)
        for entry in plan["inputs"].values():
            if entry.get("prebuilt") is not None:
                entry["prebuilt"].close()
    aggregates = []
    for config_id in configs:
        members = [child for child in children if child.get("workload_config_id") == config_id]
        if members:
            aggregates.append(aggregate_attribution(members, plan=plan, output_dir=output_dir))
    return publish_attribution(
        spec, plan, children, aggregates, output_dir, report_dir, started,
        preflight=preflight, invalidated=invalidated,
    )


def attribution_spec(**options) -> measurement.RunSpec:
    """The family's own spec: one worker core and the first primary workload."""
    try:
        from . import measure as command
    except ImportError:
        import measure as command
    cores = tuple(options.get("cores") or command.default_engine_cores(1))
    return measurement.RunSpec(
        run_id="attribution",
        case="attribution",
        topology="strict",
        store=STORE_KIND,
        cores=cores,
        workload=WORKLOAD_CONFIGS[PRIMARY_CONFIGS[0]]["workload"],
        interval_s=ATTRIBUTION_INTERVAL_S,
        duration_s=1,
        max_in_flight=ATTRIBUTION_IN_FLIGHT,
        overrides={
            "family": "attribution",
            "receiver": {"max_concurrent_requests": ATTRIBUTION_IN_FLIGHT},
        },
    )
