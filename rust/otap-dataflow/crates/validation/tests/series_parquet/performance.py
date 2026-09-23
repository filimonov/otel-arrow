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
import json
import math
import os
from pathlib import Path
import shutil
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

# How each executable is built, named in the error a missing one raises.
BENCH_BUILD_COMMANDS = {
    ("measurement", False): (
        "cargo bench -p otel-arrow-dfe-series-lake --bench measurement "
        "--bench layered --no-run"
    ),
    ("layered", False): (
        "cargo bench -p otel-arrow-dfe-series-lake --bench measurement "
        "--bench layered --no-run"
    ),
    ("measurement", True): (
        "cargo bench -p otel-arrow-dfe-series-lake --bench measurement "
        "--no-run --features bench-heap"
    ),
}


def describe_bench(executable, timeout_s=60) -> dict:
    """Ask one prebuilt executable what it is.

    The executable answers `--describe` with its target name, whether DHAT's
    allocator is installed and whether it carries debug assertions. Asking
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
    they are rather than by their file name, and a debug build is refused.
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


def bench_build(executable, *, features, allocator) -> dict:
    """The build provenance of one bench executable.

    The profile is `bench`: cargo's bench profile inherits release and adds
    fat link-time optimization, and its artifacts land beside the release
    ones. The features and the allocator separate a timing build from the
    DHAT one, so the two never share a baseline fingerprint.
    """
    executable = Path(executable)
    build = measurement.engine_build(executable)
    build.update(
        {
            "profile": "bench",
            "features": ",".join(sorted(features)) or "default",
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
            path = measurement.write_json_atomic(
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
                  "--features series_parquet,aws,durable-buffer"),
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
                "build": bench_build(
                    timing["measurement"]["executable"], features=(),
                    allocator="system",
                ),
            },
            "measurement_heap": {
                "executable": heap["measurement"]["executable"],
                "build": bench_build(
                    heap["measurement"]["executable"], features=("bench-heap",),
                    allocator="dhat",
                ),
            },
            "layered": {
                "executable": timing["layered"]["executable"],
                "build": bench_build(
                    timing["layered"]["executable"], features=(), allocator="system",
                ),
            },
        },
        "inputs": {},
        "ephemeral": {},
    }
    plan["engines"] = engine_binaries()
    plan["repetitions"] = repetitions
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
    result = measurement.new_result(
        {"run_id": "stages", "case": "stages"}, artifact_kind="index"
    )
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
    missing = sorted(set(STAGES) - covered)
    checks.append(
        measurement.check(
            "registered_stages_covered",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if missing else measurement.STATUS_PASSED,
            f"missing {missing}" if missing else f"{len(covered)} stages measured",
        )
    )
    layers = {
        entry["stage"] for entry in stage_results if entry["mode"] == "criterion"
    }
    missing_layers = sorted(set(CRITERION_LAYERS) - layers)
    checks.append(
        measurement.check(
            "criterion_layers_registered",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if missing_layers else measurement.STATUS_PASSED,
            f"missing {missing_layers}" if missing_layers
            else f"{len(layers)} cumulative layers",
        )
    )
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
    previous = measurement.archive_published_index("stages.json", output_dir, report_dir)
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
    index = measurement.write_result(output_dir / "stages.json", result)
    _ = measurement.publish_result_tree(index, report_dir)
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
