# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Deterministic workloads, ledgers, samplers and result files.

This module holds everything a measurement needs that is not the experiment
itself: the immutable run inputs, the deterministic OTLP batches a producer
sends, the disk-backed acknowledgement ledger those sends are recorded in,
the strict per-worker telemetry sampler, the drain proof, the reader oracle,
and the atomic result/baseline files the plan's acceptance policy is written
against. `measure.py` owns the experiments; this module owns the contracts.

Nothing here starts an engine, a container or a build. The real end-to-end
helpers live in `test_e2e.py` and are extended, never duplicated.
"""
import collections
import contextlib
import dataclasses
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

import duckdb

try:  # Imported as a package module by `python3 -m crates...`.
    from . import test_e2e
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import test_e2e

# The one JSON shape every run file, index and baseline declares.
SCHEMA_VERSION = 1

# Where committed evidence lives. Resolved from this file so that the
# location does not depend on the current working directory.
REPO_ROOT = Path(__file__).resolve().parents[6]
REPORT_DIR_RELATIVE = "docs/superpowers/reports/series-parquet-measurement"
REPORT_DIR = REPO_ROOT / REPORT_DIR_RELATIVE


def resolve_report_dir(value) -> Path:
    """One result's report directory, absolute.

    A committed result records the directory relative to the repository, so
    that the evidence does not carry the absolute path of the machine that
    produced it. A test may record an absolute directory of its own instead.
    """
    path = Path(value or REPORT_DIR_RELATIVE)
    return path if path.is_absolute() else REPO_ROOT / path

# The first nanosecond a measured point may carry. Point identity is
# `(metric name, time_unix_nano)`, so the ordinal is recoverable from the
# stored timestamp alone and no per-point attribute is needed.
METRIC_BASE_TIME_NS = 1789960500000000000
METRIC_TIME_STEP_NS = 1000
LOG_BASE_TIME_NS = 1789960500000000000

# Distinct stable kind names. Every supported point shape the exporter has a
# column set for appears here, including the distribution-less histogram that
# is the only way to observe an empty list against a null list.
METRIC_KINDS = (
    "gauge_int",
    "gauge_double",
    "sum_int",
    "sum_double",
    "hist",
    "hist_empty",
)
METRIC_NAME_PREFIX = "series_measure_"
LOG_KIND = "log"
ALL_KINDS = (LOG_KIND,) + METRIC_KINDS

# A stable id is fixed width so that it can be a body prefix a reader slices
# without parsing: 8 + 1 + 12 + 1 + 6 + 1 = 29 bytes plus the kind name.
ID_FIXED_WIDTH = 8 + 1 + 12 + 1 + 6 + 1

# Metric names must state their unit, so that a result file cannot report a
# number whose meaning depends on the reader's memory of the code.
UNIT_SUFFIXES = (
    "_bytes",
    "_bytes_per_s",
    "_s",
    "_ns",
    "_records",
    "_records_per_s",
    "_requests",
    "_requests_per_s",
    "_cpu_ns_per_record",
    "_count",
    "_ratio",
)

# Higher is better for these; for every other metric lower is better. The
# direction decides the sign of a regression, never its threshold.
HIGHER_IS_BETTER_SUFFIXES = ("_per_s", "_ratio")

# The single numerical boundary of the Controller baseline policy. It lives
# here alone so that no task can quietly choose a kinder one.
REGRESSION_LIMIT = 0.25

# The Controller environment policy's floor for a publishable measurement.
MINIMUM_PHYSICAL_CORES = 8

# Exporter gauges a measured sample must contain for every worker. A missing
# or non-numeric gauge is an error, never a zero.
REQUIRED_EXPORTER_GAUGES = (
    "block.active_bytes",
    "block.flushing_bytes",
    "block.pending_bytes",
    "block.requests_pending",
    "block.pending_slot_occupied",
    "notify.queued",
    "notify.token_bytes",
    "series_cache.entries",
    "memory.accounted_bytes",
    "memory.budget_bytes",
    "oldest_unacked_seconds",
)

# Gauges that must all read zero before a worker counts as drained.
EXPORTER_EMPTY_GAUGES = (
    "block.active_bytes",
    "block.flushing_bytes",
    "block.pending_bytes",
    "block.requests_pending",
    "notify.queued",
)

# The buffered topology's additional emptiness evidence. Task 2 introduces
# the topology; the drain proof already knows what it has to see.
BUFFER_EMPTY_GAUGES = (
    ("processor.durable_buffer.items", "queued"),
    ("processor.durable_buffer", "in_flight"),
)

# How many advances of every worker's collection-updated uptime the drain
# proof requires, each with empty state.
DRAIN_EMPTY_EPOCHS = 3

# The gauge that says whether a worker answered the collection a snapshot was
# built from. `worker.rs::sample_metrics` computes it from configuration
# constants, so it is strictly positive whenever the worker sampled itself;
# a worker that did not answer reports every gauge as zero, which is
# indistinguishable from a drained worker unless this marker is checked.
LIVENESS_GAUGE = "memory.budget_bytes"

# A run file name is a plain file name in the report directory. Nothing else
# may be staged or published by name.
SAFE_JSON_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.json$")

# Commands whose presence means somebody is compiling or building an image
# while a measurement runs.
BUILD_COMMANDS = ("cargo", "rustc", "cc1", "cc1plus", "ld", "buildkitd", "buildx")


def utc_now():
    """An ASCII RFC 3339 timestamp in UTC, to second resolution."""
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def stable_id(seed: int, request: int, point: int, kind: str) -> str:
    """The identity of one record, fixed width and readable from a file."""
    return f"{seed:08x}:{request:012d}:{point:06d}:{kind}"


def require_long() -> None:
    """Skip unless the caller opted in to a long measurement."""
    if os.environ.get("SERIES_MEASURE_LONG") != "1":
        raise unittest.SkipTest("set SERIES_MEASURE_LONG=1 for this measurement")


def wait_until(observe, accept, *, deadline_ns: int, description: str):
    """Poll `observe` until `accept` agrees, or fail at a monotonic deadline.

    Waiting never proves anything on its own: the return value is always an
    observation that `accept` looked at, and a deadline that expires reports
    the last observation rather than assuming the state arrived late.
    """
    wake = threading.Event()
    last = None
    while time.monotonic_ns() < deadline_ns:
        last = observe()
        if accept(last):
            return last
        remaining = (deadline_ns - time.monotonic_ns()) / 1e9
        if remaining > 0:
            wake.wait(min(0.05, remaining))
    raise AssertionError(f"deadline: {description}; last={last!r}")


@dataclasses.dataclass(frozen=True)
class Workload:
    """What a producer sends, derived entirely from `seed` and an index."""

    seed: int = 20260922
    requests: int = 100
    records_per_request: int = 100
    body_bytes: int = 1024
    series: int = 100
    metrics_every: int = 5

    def __post_init__(self):
        for name in ("requests", "records_per_request", "series", "metrics_every"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.body_bytes < ID_FIXED_WIDTH + len(LOG_KIND):
            raise ValueError(
                f"body_bytes must hold the stable id: "
                f"{self.body_bytes} < {ID_FIXED_WIDTH + len(LOG_KIND)}"
            )

    def signal_of(self, request_index: int) -> str:
        """Which signal request `request_index` carries."""
        return "metrics" if request_index % self.metrics_every == 0 else "logs"

    def as_json(self) -> dict:
        """The workload as a plain dictionary for a result file."""
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class RunSpec:
    """The immutable inputs of exactly one experiment."""

    run_id: str
    case: str
    topology: str
    store: str
    cores: tuple
    workload: Workload
    interval_s: int = 15
    duration_s: int = 30
    producer_timeout_s: float = 180.0
    max_in_flight: int = 128
    overrides: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if self.topology not in ("strict", "buffered"):
            raise ValueError(f"topology must be strict or buffered: {self.topology}")
        if self.store not in ("local", "minio", "rustfs"):
            raise ValueError(f"store must be local, minio or rustfs: {self.store}")
        if not self.cores:
            raise ValueError("cores must name at least one physical core")
        if len(set(self.cores)) != len(self.cores):
            raise ValueError(f"cores must be distinct: {self.cores}")
        if any(core < 0 for core in self.cores):
            raise ValueError(f"cores must be allowed ids: {self.cores}")
        for name in ("interval_s", "duration_s", "max_in_flight"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.producer_timeout_s <= 0:
            raise ValueError("producer_timeout_s must be positive")
        if self.max_in_flight > self.receiver_capacity():
            raise ValueError(
                f"max_in_flight {self.max_in_flight} exceeds receiver capacity "
                f"{self.receiver_capacity()}"
            )

    def receiver_capacity(self) -> int:
        """The receiver's concurrent request limit for this run."""
        receiver = self.overrides.get("receiver", {})
        return int(receiver.get("max_concurrent_requests", 128))

    @staticmethod
    def build_run_id(case, topology, store, cores, interval_s, ordinal) -> str:
        """The plan's run id: one file name per trial, never per family."""
        return (
            f"{case}-{topology}-{store}-c{len(cores)}"
            f"-w{interval_s}-r{ordinal:03d}"
        )

    def as_json(self) -> dict:
        """The spec as a plain dictionary for a result file."""
        return {
            "run_id": self.run_id,
            "case": self.case,
            "topology": self.topology,
            "store": self.store,
            "cores": list(self.cores),
            "interval_s": self.interval_s,
            "duration_s": self.duration_s,
            "producer_timeout_s": self.producer_timeout_s,
            "max_in_flight": self.max_in_flight,
            "overrides": self.overrides,
        }


class MeasurementTestCase(unittest.TestCase):
    """Retain each test's logs, results and oracle ledger for review."""

    def setUp(self):
        root = Path(os.environ.get("SERIES_ARTIFACT_DIR", "/tmp/series-measure-tests"))
        root.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^A-Za-z0-9._-]", "-", self.id())
        self.output_dir = Path(
            tempfile.mkdtemp(prefix=safe + "-", dir=root)
        )


# --------------------------------------------------------------------------
# Deterministic workload
# --------------------------------------------------------------------------


def _mix(seed: int, request_index: int, point: int, salt: str) -> int:
    """A reproducible 64-bit draw for one field of one record."""
    material = f"{seed}:{request_index}:{point}:{salt}".encode("ascii")
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big")


# The printable alphabet the padding is drawn from. It excludes the vertical
# bar and the equals sign so that padding can never be mistaken for a field
# separator of the canonical payload below.
_PADDING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


def _padding(seed: int, request_index: int, point: int, width: int) -> str:
    """Seeded printable padding of exactly `width` characters."""
    out = []
    draw = _mix(seed, request_index, point, "pad")
    while len(out) < width:
        if draw == 0:
            draw = _mix(seed, request_index, point, f"pad{len(out)}")
        out.append(_PADDING[draw % len(_PADDING)])
        draw //= len(_PADDING)
    return "".join(out[:width])


def double_bits(value: float) -> str:
    """A double rendered as its IEEE-754 bits, most significant first.

    Two readers print floating point differently and both read the same
    Parquet bytes, so the canonical payload carries the bit pattern. This is
    the same rendering `test_e2e.canonical_column` builds in SQL.
    """
    return format(struct.unpack(">Q", struct.pack(">d", value))[0], "064b")


def _render_int(value):
    """An integer column in the canonical payload, or its null marker."""
    return "null" if value is None else str(value)


def _render_double(value):
    """A double column in the canonical payload, or its null marker."""
    return "null" if value is None else double_bits(value)


def _render_int_list(values):
    """A list-of-integer column. A null list and an empty list both render
    empty, because ClickHouse has no nullable array and cannot tell them
    apart. That single distinction is asserted directly against the files by
    `test_e2e.scan_objects`, which is why collapsing it here is safe."""
    return "" if not values else ",".join(str(item) for item in values)


def _render_double_list(values):
    """A list-of-double column, rendered as bit patterns."""
    return "" if not values else ",".join(double_bits(item) for item in values)


def metric_point(workload: Workload, request_index: int, point: int) -> dict:
    """Every field of one metrics point, chosen from the seed and index.

    The returned dictionary is also the expectation: it names the exact
    columns the exporter stores, with `None` where the point kind leaves a
    column unset, so the canonical payload is built from it once and compared
    to what a reader found.
    """
    kind = METRIC_KINDS[point % len(METRIC_KINDS)]
    ordinal = request_index * workload.records_per_request + point
    fields = {
        "kind": kind,
        "record_id": stable_id(workload.seed, request_index, point, kind),
        "time_unix_nano": METRIC_BASE_TIME_NS + ordinal * METRIC_TIME_STEP_NS,
        "slot": request_index % workload.series,
        "value_int": None,
        "value_double": None,
        "count": None,
        "sum": None,
        "bucket_counts": None,
        "explicit_bounds": None,
        "min": None,
        "max": None,
    }
    draw = _mix(workload.seed, request_index, point, kind)
    if kind in ("gauge_int", "sum_int"):
        fields["value_int"] = (draw % (2**62)) - (2**61)
    elif kind in ("gauge_double", "sum_double"):
        fields["value_double"] = (draw % 10**12) / 1000.0
    elif kind == "hist":
        buckets = [
            (_mix(workload.seed, request_index, point, f"b{index}") % 97) + 1
            for index in range(4)
        ]
        bounds = sorted(
            float((_mix(workload.seed, request_index, point, f"e{index}") % 10**6))
            / 8.0
            + index
            for index in range(3)
        )
        fields["bucket_counts"] = buckets
        fields["explicit_bounds"] = bounds
        fields["count"] = sum(buckets)
        fields["sum"] = (draw % 10**9) / 64.0
    elif kind == "hist_empty":
        # A histogram with a count but no distribution at all: both lists are
        # genuinely empty rather than absent.
        fields["bucket_counts"] = []
        fields["explicit_bounds"] = []
        fields["count"] = (draw % 1000) + 1
        fields["sum"] = (draw % 10**7) / 32.0
    else:
        raise AssertionError(f"unknown metric kind {kind}")
    return fields


def canonical_metric_payload(fields: dict) -> str:
    """The exact string a stored metrics row must hash to."""
    return "|".join(
        (
            fields["kind"],
            fields["record_id"],
            "int=" + _render_int(fields["value_int"]),
            "double=" + _render_double(fields["value_double"]),
            "count=" + _render_int(fields["count"]),
            "sum=" + _render_double(fields["sum"]),
            "min=" + _render_double(fields["min"]),
            "max=" + _render_double(fields["max"]),
            "buckets=" + _render_int_list(fields["bucket_counts"]),
            "bounds=" + _render_double_list(fields["explicit_bounds"]),
            "time=" + str(fields["time_unix_nano"]),
        )
    )


def canonical_log_payload(record_id, body, time_unix_nano, logger) -> str:
    """The exact string a stored logs row must hash to."""
    return "|".join(
        (
            LOG_KIND,
            record_id,
            "body=" + body,
            "time=" + str(time_unix_nano),
            "logger=" + logger,
        )
    )


def payload_sha256(payload: str) -> str:
    """The hash a reader's own rendering of the same row must produce."""
    return hashlib.sha256(payload.encode("ascii")).hexdigest()


def logger_name(workload: Workload, request_index: int) -> str:
    """The series-identifying attribute of one logs request.

    `logger.name` is the configured logs series attribute, so varying it by
    request is what creates distinct series. The record id is deliberately
    not part of it: putting an id into series identity would make every
    record its own series and measure nothing the deployment does.
    """
    return f"series.logger.{request_index % workload.series:06d}"


def build_request(workload: Workload, request_index: int):
    """One request's signal, deterministic OTLP bytes and expected records.

    The bytes are produced with deterministic protobuf serialization, so a
    retry of request `request_index` is byte-identical to its first attempt
    and the ledger can prove that rather than assume it.
    """
    if not 0 <= request_index < workload.requests:
        raise ValueError(f"request index out of range: {request_index}")
    signal = workload.signal_of(request_index)
    rows = []
    if signal == "logs":
        request = test_e2e.logs_pb.ExportLogsServiceRequest()
        resource = request.resource_logs.add()
        resource.resource.attributes.add(
            key="host.id"
        ).value.string_value = "producer-1"
        resource.resource.attributes.add(
            key="service.name"
        ).value.string_value = "series-e2e-service"
        scope = resource.scope_logs.add()
        scope.scope.name = "series-e2e"
        logger = logger_name(workload, request_index)
        for point in range(workload.records_per_request):
            record_id = stable_id(workload.seed, request_index, point, LOG_KIND)
            width = workload.body_bytes - len(record_id)
            body = record_id + _padding(workload.seed, request_index, point, width)
            stamp = LOG_BASE_TIME_NS + (
                request_index * workload.records_per_request + point
            ) * METRIC_TIME_STEP_NS
            record = scope.log_records.add(time_unix_nano=stamp)
            record.body.string_value = body
            record.attributes.add(key="logger.name").value.string_value = logger
            rows.append(
                (
                    record_id,
                    LOG_KIND,
                    payload_sha256(
                        canonical_log_payload(record_id, body, stamp, logger)
                    ),
                )
            )
    else:
        request = test_e2e.metrics_pb.ExportMetricsServiceRequest()
        resource = request.resource_metrics.add()
        resource.resource.attributes.add(
            key="host.id"
        ).value.string_value = "producer-1"
        resource.resource.attributes.add(
            key="service.name"
        ).value.string_value = "series-e2e-service"
        scope = resource.scope_metrics.add()
        scope.scope.name = "series-e2e"
        # One metric per kind per request, each holding the points of that
        # kind. Metric identity is the name, so the pool of names is finite
        # however many points a request carries.
        metrics = {}
        for kind in METRIC_KINDS:
            metrics[kind] = scope.metrics.add(
                name=METRIC_NAME_PREFIX + kind, unit="1"
            )
        for point in range(workload.records_per_request):
            fields = metric_point(workload, request_index, point)
            kind = fields["kind"]
            metric = metrics[kind]
            if kind in ("gauge_int", "gauge_double"):
                data = metric.gauge.data_points.add(
                    time_unix_nano=fields["time_unix_nano"]
                )
            elif kind in ("sum_int", "sum_double"):
                metric.sum.aggregation_temporality = 2
                metric.sum.is_monotonic = True
                data = metric.sum.data_points.add(
                    time_unix_nano=fields["time_unix_nano"]
                )
            else:
                metric.histogram.aggregation_temporality = 2
                data = metric.histogram.data_points.add(
                    time_unix_nano=fields["time_unix_nano"],
                    count=fields["count"],
                    sum=fields["sum"],
                )
                data.bucket_counts.extend(fields["bucket_counts"])
                data.explicit_bounds.extend(fields["explicit_bounds"])
            if fields["value_int"] is not None:
                data.as_int = fields["value_int"]
            if fields["value_double"] is not None:
                data.as_double = fields["value_double"]
            data.attributes.add(
                key="series.slot"
            ).value.string_value = f"{fields['slot']:06d}"
            rows.append(
                (
                    fields["record_id"],
                    kind,
                    payload_sha256(canonical_metric_payload(fields)),
                )
            )
    return signal, request.SerializeToString(deterministic=True), rows


def assert_records(expected: dict, actual: dict, *, healthy: bool) -> dict:
    """Compare per-kind stable ids and report a multiplicity histogram.

    `expected` maps a record id to its canonical payload hash; `actual` maps
    a record id to the hashes a reader found under it, one entry per stored
    copy. Missing, unexpected, corrupt and duplicated records are counted
    independently, because an aggregate row count conceals a loss that is
    balanced by a duplicate.
    """
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    if missing or unexpected:
        raise AssertionError(f"missing={missing[:10]} unexpected={unexpected[:10]}")
    histogram = collections.Counter()
    for record_id, expected_hash in expected.items():
        copies = actual[record_id]
        if not copies or any(value != expected_hash for value in copies):
            raise AssertionError(f"corrupt={record_id}")
        if healthy and len(copies) != 1:
            raise AssertionError(f"unexpected multiplicity={record_id}:{len(copies)}")
        histogram[len(copies)] += 1
    return dict(sorted(histogram.items()))


# What each kind's descriptor must say. These are independent explicit
# fixtures rather than a restatement of the producer: a change in how the
# exporter classifies a point kind has to be noticed here, not absorbed.
METRIC_DESCRIPTOR_FIXTURE = {
    "gauge_int": {"metric_type": "gauge", "temporality": "", "is_monotonic": False},
    "gauge_double": {"metric_type": "gauge", "temporality": "", "is_monotonic": False},
    "sum_int": {
        "metric_type": "sum",
        "temporality": "cumulative",
        "is_monotonic": True,
    },
    "sum_double": {
        "metric_type": "sum",
        "temporality": "cumulative",
        "is_monotonic": True,
    },
    "hist": {
        "metric_type": "histogram",
        "temporality": "cumulative",
        "is_monotonic": False,
    },
    "hist_empty": {
        "metric_type": "histogram",
        "temporality": "cumulative",
        "is_monotonic": False,
    },
}

# Whether each kind's stored list columns must be empty lists or null lists.
# Only DuckDB can see this difference; ClickHouse reads a null Parquet list
# as an empty array, which is why it is checked in one reader alone.
METRIC_LIST_SHAPE = {
    "gauge_int": "null",
    "gauge_double": "null",
    "sum_int": "null",
    "sum_double": "null",
    "hist": "filled",
    "hist_empty": "empty",
}


# --------------------------------------------------------------------------
# Acknowledgement ledger
# --------------------------------------------------------------------------

# What a producer attempt ended as. A retryable NACK and a producer-local
# failure are different observations: only the first is evidence about the
# server, and confusing them would let a broken client look like a healthy
# one.
OUTCOME_ACK = "ack"
OUTCOME_RETRYABLE = "retryable_nack"
OUTCOME_PERMANENT = "permanent_nack"
OUTCOME_PARTIAL = "partial_rejection"
OUTCOME_LOCAL = "producer_local"
OUTCOMES = (
    OUTCOME_ACK,
    OUTCOME_RETRYABLE,
    OUTCOME_PERMANENT,
    OUTCOME_PARTIAL,
    OUTCOME_LOCAL,
)


class Ledger:
    """A disk-backed record of what a producer sent and what it was told.

    The ledger is the delivery oracle, so it has to survive the process that
    writes it: it is opened with write-ahead logging and full synchronisation
    so that an acknowledgement is durable before a test may use it to gate a
    fault. It keeps bounded metadata in memory only, because the soak sends
    more records than a Python set should hold; retry bytes are regenerated
    from the workload by index rather than retained.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path), isolation_level=None)
        self.lock = threading.Lock()
        with self.lock:
            cursor = self.connection.cursor()
            _ = cursor.execute("PRAGMA journal_mode=WAL")
            _ = cursor.execute("PRAGMA synchronous=FULL")
            _ = cursor.execute(
                "CREATE TABLE IF NOT EXISTS requests ("
                "request_id INTEGER PRIMARY KEY, signal TEXT NOT NULL, "
                "wire_sha256 TEXT NOT NULL, first_send_ns INTEGER NOT NULL, "
                "ack_ns INTEGER)"
            )
            _ = cursor.execute(
                "CREATE TABLE IF NOT EXISTS attempts ("
                "request_id INTEGER NOT NULL, ordinal INTEGER NOT NULL, "
                "start_ns INTEGER NOT NULL, finish_ns INTEGER NOT NULL, "
                "outcome TEXT NOT NULL, detail TEXT, "
                "PRIMARY KEY (request_id, ordinal))"
            )
            _ = cursor.execute(
                "CREATE TABLE IF NOT EXISTS records ("
                "record_id TEXT PRIMARY KEY, request_id INTEGER NOT NULL, "
                "kind TEXT NOT NULL, expected_sha256 TEXT NOT NULL)"
            )
            _ = cursor.execute(
                "CREATE INDEX IF NOT EXISTS records_by_request "
                "ON records(request_id)"
            )

    def add_request(self, request_id, signal, wire, rows, *, send_ns):
        """Register one request and its records, once and immutably.

        A request that is registered twice must carry byte-identical wire
        bytes: a retry that rebuilt a different payload would make every
        later comparison meaningless, so it fails here rather than later.
        """
        digest = hashlib.sha256(wire).hexdigest()
        with self.lock:
            cursor = self.connection.cursor()
            _ = cursor.execute("BEGIN IMMEDIATE")
            try:
                existing = cursor.execute(
                    "SELECT wire_sha256 FROM requests WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if existing is None:
                    _ = cursor.execute(
                        "INSERT INTO requests "
                        "(request_id, signal, wire_sha256, first_send_ns, ack_ns) "
                        "VALUES (?, ?, ?, ?, NULL)",
                        (request_id, signal, digest, send_ns),
                    )
                    _ = cursor.executemany(
                        "INSERT INTO records "
                        "(record_id, request_id, kind, expected_sha256) "
                        "VALUES (?, ?, ?, ?)",
                        [
                            (record_id, request_id, kind, expected)
                            for record_id, kind, expected in rows
                        ],
                    )
                elif existing[0] != digest:
                    raise AssertionError(
                        f"request {request_id} was rebuilt with different bytes: "
                        f"{existing[0]} != {digest}"
                    )
                _ = cursor.execute("COMMIT")
            except BaseException:
                _ = cursor.execute("ROLLBACK")
                raise
        return digest

    def attempt(self, request_id, ordinal, start_ns, finish_ns, outcome, detail=""):
        """Record one delivery attempt and how it ended."""
        if outcome not in OUTCOMES:
            raise ValueError(f"unknown outcome {outcome}")
        with self.lock:
            _ = self.connection.execute(
                "INSERT OR REPLACE INTO attempts "
                "(request_id, ordinal, start_ns, finish_ns, outcome, detail) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (request_id, ordinal, start_ns, finish_ns, outcome, detail),
            )

    def ack(self, request_id, ack_ns):
        """Record the first fully successful response for one request.

        Only the first one counts: a duplicate delivery after an at-least-
        once retry does not move the acknowledgement time backwards or
        forwards, and the value is durable before the caller returns.
        """
        with self.lock:
            _ = self.connection.execute(
                "UPDATE requests SET ack_ns = ? "
                "WHERE request_id = ? AND ack_ns IS NULL",
                (ack_ns, request_id),
            )

    def acked_ids(self):
        """Every record id belonging to an acknowledged request."""
        with self.lock:
            for row in self.connection.execute(
                "SELECT r.record_id, r.kind, r.expected_sha256 FROM records r "
                "JOIN requests q ON q.request_id = r.request_id "
                "WHERE q.ack_ns IS NOT NULL"
            ):
                yield row

    def all_ids(self):
        """Every record id the producer intended to deliver."""
        with self.lock:
            for row in self.connection.execute(
                "SELECT record_id, kind, expected_sha256 FROM records"
            ):
                yield row

    def counts(self):
        """Attempt and acknowledgement tallies, each kept separate."""
        with self.lock:
            cursor = self.connection.cursor()
            requests, acked = cursor.execute(
                "SELECT count(*), count(ack_ns) FROM requests"
            ).fetchone()
            records = cursor.execute("SELECT count(*) FROM records").fetchone()[0]
            acked_records = cursor.execute(
                "SELECT count(*) FROM records r JOIN requests q "
                "ON q.request_id = r.request_id WHERE q.ack_ns IS NOT NULL"
            ).fetchone()[0]
            outcomes = dict(
                cursor.execute(
                    "SELECT outcome, count(*) FROM attempts GROUP BY outcome"
                ).fetchall()
            )
        return {
            "requests_attempted_count": requests,
            "requests_acked_count": acked,
            "requests_outstanding_count": requests - acked,
            "records_attempted_count": records,
            "records_acked_count": acked_records,
            "attempts_by_outcome": {name: outcomes.get(name, 0) for name in OUTCOMES},
        }

    def acknowledgement_latencies_s(self):
        """The per-request first-send to acknowledgement delay, in seconds."""
        with self.lock:
            rows = self.connection.execute(
                "SELECT (ack_ns - first_send_ns) / 1e9 FROM requests "
                "WHERE ack_ns IS NOT NULL ORDER BY 1"
            ).fetchall()
        return [row[0] for row in rows]

    def close(self):
        """Flush and close the ledger database."""
        with self.lock:
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# --------------------------------------------------------------------------
# Environment, lease and build monitor
# --------------------------------------------------------------------------


def cpu_model() -> str:
    """The CPU model string, or an explicit marker when it is unreadable."""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def core_topology() -> dict:
    """Logical and physical cores, and each core's sibling set.

    Physical cores are counted from the kernel's own thread sibling lists
    rather than from a division by a guessed thread count, so a machine with
    asymmetric cores is described as it is.
    """
    siblings = {}
    logical = []
    base = Path("/sys/devices/system/cpu")
    if base.is_dir():
        for entry in sorted(base.glob("cpu[0-9]*")):
            name = entry.name
            if not name[3:].isdigit():
                continue
            index = int(name[3:])
            logical.append(index)
            listing = entry / "topology/thread_siblings_list"
            if listing.is_file():
                siblings[index] = parse_core_list(listing.read_text().strip())
            else:
                siblings[index] = [index]
    if not logical:
        logical = list(range(os.cpu_count() or 1))
        siblings = {index: [index] for index in logical}
    groups = sorted({tuple(sorted(value)) for value in siblings.values()})
    return {
        "logical_core_count": len(logical),
        "physical_core_count": len(groups),
        "logical_cores": logical,
        "sibling_groups": [list(group) for group in groups],
    }


def parse_core_list(text: str):
    """Expand a kernel core list such as `0-3,8` into explicit ids."""
    cores = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, high = part.split("-", 1)
            cores.extend(range(int(low), int(high) + 1))
        else:
            cores.append(int(part))
    return sorted(cores)


def ram_bytes() -> int:
    """Total physical memory in bytes."""
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("total memory is unavailable")


def thread_status(pid: int, tid: int) -> dict:
    """One thread's name and allowed cores, read from procfs."""
    fields = {}
    for line in Path(f"/proc/{pid}/task/{tid}/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            fields[key] = value.strip()
    return fields


def thread_affinity(pid, role, expected_cores):
    """Every thread of one process, with its observed allowed core set."""
    observed = []
    task = Path(f"/proc/{pid}/task")
    for entry in sorted(task.iterdir(), key=lambda item: int(item.name)):
        tid = int(entry.name)
        try:
            fields = thread_status(pid, tid)
        except OSError:
            # A thread that exits between listing and reading is not an
            # observation; it is recorded as such rather than as a zero.
            observed.append(
                {
                    "pid": pid,
                    "tid": tid,
                    "role": role,
                    "name": None,
                    "cpus_allowed_list": None,
                    "expected_cores": list(expected_cores or []),
                    "reason": "thread exited during observation",
                    "observed_utc": utc_now(),
                }
            )
            continue
        observed.append(
            {
                "pid": pid,
                "tid": tid,
                "role": role,
                "name": fields.get("Name"),
                "cpus_allowed_list": fields.get("Cpus_allowed_list"),
                "expected_cores": list(expected_cores or []),
                "observed_utc": utc_now(),
            }
        )
    return observed


def worker_thread_name(group_id, pipeline_id, core_id, generation) -> str:
    """The thread name the controller gives one pipeline worker."""
    return f"pipeline-{group_id}-{pipeline_id}-core-{core_id}-gen-{generation}"


# Linux truncates a thread's `comm` to fifteen characters plus a terminator,
# so a worker is identified by the truncation of its full name.
COMM_WIDTH = 15


def select_worker_threads(threads, workers):
    """Map each expected worker identity to exactly one observed thread.

    `workers` are the identities the telemetry reported. A worker with no
    thread, or with more than one, is an ambiguous mapping: the caller must
    abort rather than measure an affinity it cannot attribute.
    """
    by_comm = collections.defaultdict(list)
    for thread in threads:
        if thread.get("name"):
            by_comm[thread["name"]].append(thread)
    selected = {}
    ambiguous = []
    expected_comms = collections.Counter()
    for worker in workers:
        expected_comms[
            worker_thread_name(
                worker["group_id"],
                worker["pipeline_id"],
                worker["core_id"],
                worker["generation"],
            )[:COMM_WIDTH]
        ] += 1
    for worker in workers:
        full = worker_thread_name(
            worker["group_id"],
            worker["pipeline_id"],
            worker["core_id"],
            worker["generation"],
        )
        comm = full[:COMM_WIDTH]
        candidates = by_comm.get(comm, [])
        if expected_comms[comm] != 1:
            ambiguous.append(
                {
                    "worker": worker,
                    "thread_name": full,
                    "reason": f"{expected_comms[comm]} workers share comm {comm!r}",
                }
            )
        elif len(candidates) != 1:
            ambiguous.append(
                {
                    "worker": worker,
                    "thread_name": full,
                    "reason": f"{len(candidates)} threads carry comm {comm!r}",
                }
            )
        else:
            selected[worker["key"]] = candidates[0]
    return selected, ambiguous


def environment_snapshot(roles, *, workers=(), absent=None) -> dict:
    """A start or end environment snapshot.

    `roles` maps a role name to a process id, or to `None` when the role has
    no process in this run. Every role without a process carries an explicit
    reason, because an absent observation can never support a passed
    measurement.
    """
    topology = core_topology()
    with open("/proc/loadavg", encoding="ascii") as handle:
        load = [float(value) for value in handle.read().split()[:3]]
    threads = []
    missing = dict(absent or {})
    for role, value in sorted(roles.items()):
        if value is None:
            missing.setdefault(role, "no process for this role in this run")
            continue
        pid, expected_cores = value if isinstance(value, tuple) else (value, ())
        try:
            threads.extend(thread_affinity(pid, role, expected_cores))
        except OSError as error:
            missing[role] = f"pid {pid} is not readable: {error}"
    snapshot = {
        "observed_utc": utc_now(),
        "monotonic_ns": time.monotonic_ns(),
        "cpu_model": cpu_model(),
        "logical_core_count": topology["logical_core_count"],
        "physical_core_count": topology["physical_core_count"],
        "sibling_groups": topology["sibling_groups"],
        "ram_bytes": ram_bytes(),
        "kernel": platform.platform(),
        "load_average_1_5_15": load,
        "thread_affinity": threads,
        "absent_roles": missing,
    }
    if workers:
        selected, ambiguous = select_worker_threads(threads, workers)
        snapshot["worker_threads"] = sorted(
            (
                {
                    "key": key,
                    "tid": thread["tid"],
                    "name": thread["name"],
                    "cpus_allowed_list": thread["cpus_allowed_list"],
                    "expected_cores": thread["expected_cores"],
                }
                for key, thread in selected.items()
            ),
            key=lambda item: item["key"],
        )
        snapshot["ambiguous_workers"] = ambiguous
    return snapshot


def assert_affinity(snapshot) -> None:
    """Abort when an observed worker affinity is not the requested one.

    An ambiguous worker mapping aborts as well: an affinity that cannot be
    attributed to a known worker is not evidence that the worker was pinned.
    """
    ambiguous = snapshot.get("ambiguous_workers") or []
    if ambiguous:
        raise AssertionError(f"ambiguous worker thread mapping: {ambiguous}")
    mismatched = []
    for thread in snapshot.get("worker_threads", []):
        expected = sorted(thread["expected_cores"])
        if not expected:
            continue
        observed = thread["cpus_allowed_list"]
        if observed is None or parse_core_list(observed) != expected:
            mismatched.append(
                {
                    "tid": thread["tid"],
                    "expected_cores": expected,
                    "cpus_allowed_list": observed,
                }
            )
    if mismatched:
        raise AssertionError(f"worker affinity mismatch: {mismatched}")


def environment_match(start, end) -> dict:
    """Whether the machine the run ended on is the one it started on."""
    keys = (
        "cpu_model",
        "logical_core_count",
        "physical_core_count",
        "ram_bytes",
        "kernel",
    )
    differences = {
        key: [start.get(key), end.get(key)]
        for key in keys
        if start.get(key) != end.get(key)
    }
    return {"matched": not differences, "differences": differences}


class BuildMonitor:
    """Watch for a compiler or image build running beside a measurement.

    The monitor never stops or signals anything: another user's build is
    their business, and the only consequence here is that this run's
    performance numbers are invalid. Evidence of what was seen is preserved.
    """

    def __init__(self, *, interval_s=1.0, own_pids=()):
        self.interval_s = interval_s
        self.own_pids = set(own_pids)
        self.observations = []
        self.scans = 0
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()

    def scan(self):
        """One pass over procfs, returning the build processes found."""
        found = []
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            if pid in self.own_pids or pid == os.getpid():
                continue
            try:
                comm = (entry / "comm").read_text().strip()
                cmdline = (entry / "cmdline").read_bytes().decode(
                    "ascii", "replace"
                ).replace("\x00", " ").strip()
            except OSError:
                continue
            if comm in BUILD_COMMANDS or _is_docker_build(cmdline):
                found.append(
                    {
                        "pid": pid,
                        "comm": comm,
                        "cmdline": cmdline[:200],
                        "observed_utc": utc_now(),
                    }
                )
        with self._lock:
            self.scans += 1
            self.observations.extend(found)
        return found

    def _run(self):
        """Scan on the configured interval until asked to stop."""
        while not self._stop.is_set():
            _ = self.scan()
            _ = self._stop.wait(self.interval_s)

    def start(self):
        """Begin scanning in the background."""
        _ = self.scan()
        self._thread = threading.Thread(target=self._run, name="build-monitor")
        self._thread.daemon = True
        self._thread.start()
        return self

    def stop(self) -> dict:
        """Stop scanning and report everything seen."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s * 5 + 5)
            if self._thread.is_alive():
                raise AssertionError("the build monitor thread did not stop")
            self._thread = None
        _ = self.scan()
        with self._lock:
            return {
                "scans": self.scans,
                "detected": bool(self.observations),
                "observations": self.observations[:50],
                "observation_count": len(self.observations),
            }

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.report = self.stop()


def _is_docker_build(cmdline: str) -> bool:
    """Whether a command line is a container image build."""
    if "docker" not in cmdline and "podman" not in cmdline:
        return False
    return " build" in cmdline or cmdline.endswith(" build")


class HostLease:
    """An exclusive host measurement lease held for a whole run.

    The lease is a file created with O_EXCL so that two measurement
    processes on this machine cannot overlap. A lease whose holder is gone
    is reclaimed, because a crashed run must not block the machine forever.
    """

    def __init__(self, path=None, *, owner=None):
        self.path = Path(
            path
            or os.environ.get(
                "SERIES_MEASURE_LEASE", "/tmp/series-measure-host.lease"
            )
        )
        self.owner = owner or f"{os.getpid()}-{uuid.uuid4().hex}"
        self.acquired_utc = None
        self.released_utc = None
        self._held = False

    def acquire(self, *, deadline_ns=None):
        """Take the lease, waiting for a stale holder to be reclaimed."""
        deadline_ns = deadline_ns or (time.monotonic_ns() + 60 * 10**9)
        while True:
            try:
                handle = os.open(
                    str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
                )
            except FileExistsError:
                if self._reclaim_stale():
                    continue
                remaining = (deadline_ns - time.monotonic_ns()) / 1e9
                if remaining <= 0:
                    raise AssertionError(
                        f"another measurement holds {self.path}: "
                        f"{self._read_holder()}"
                    )
                _ = threading.Event().wait(min(0.2, remaining))
                continue
            with os.fdopen(handle, "w", encoding="ascii") as stream:
                self.acquired_utc = utc_now()
                json.dump(
                    {
                        "owner": self.owner,
                        "pid": os.getpid(),
                        "acquired_utc": self.acquired_utc,
                    },
                    stream,
                    sort_keys=True,
                )
            self._held = True
            return self

    def _read_holder(self):
        """Whatever the lease file currently claims, or None."""
        try:
            return json.loads(self.path.read_text(encoding="ascii"))
        except (OSError, ValueError):
            return None

    def _reclaim_stale(self) -> bool:
        """Remove a lease whose holder no longer exists, if there is one."""
        holder = self._read_holder()
        if holder is None:
            # A half-written lease file left by a killed process.
            with contextlib.suppress(OSError):
                self.path.unlink()
            return True
        pid = holder.get("pid")
        if isinstance(pid, int) and not Path(f"/proc/{pid}").exists():
            with contextlib.suppress(OSError):
                self.path.unlink()
            return True
        return False

    def assert_held(self):
        """Fail unless this process still owns the lease it took."""
        holder = self._read_holder()
        if not self._held or not holder or holder.get("owner") != self.owner:
            raise AssertionError(f"the host measurement lease was lost: {holder}")

    def release(self):
        """Give the lease back, if it is still ours."""
        if self._held and (self._read_holder() or {}).get("owner") == self.owner:
            with contextlib.suppress(OSError):
                self.path.unlink()
        self.released_utc = utc_now()
        self._held = False

    def as_json(self) -> dict:
        """The lease identity and lifetime for a result file."""
        return {
            "path": str(self.path),
            "owner": self.owner,
            "acquired_utc": self.acquired_utc,
            "released_utc": self.released_utc,
        }

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()


# --------------------------------------------------------------------------
# Strict telemetry sampler and drain proof
# --------------------------------------------------------------------------

# The pipeline group the engine runs its own observability pipeline in. It is
# a pipeline like any other in the telemetry, but it is not a worker of the
# measured pipeline and is never counted as one.
SYSTEM_GROUP = "system"


def attribute_value(raw):
    """One telemetry attribute value, unwrapped from its type tag."""
    if isinstance(raw, dict) and len(raw) == 1:
        return next(iter(raw.values()))
    return raw


def attributes_of(entry) -> dict:
    """A metric set's identifying attributes as plain values."""
    return {
        key: attribute_value(value)
        for key, value in (entry.get("attributes") or {}).items()
    }


# The attributes that name a worker. Everything else -- the node id, type
# and urn -- names an entity inside that worker, and two entities of one
# worker publish the same metric name with different values, so an entity is
# part of a metric's slot rather than something to collapse.
WORKER_ATTRIBUTE_KEYS = (
    "pipeline.group.id",
    "pipeline.id",
    "core.id",
    "deployment.generation",
    "numa.node.id",
)


def worker_key(attributes) -> str:
    """The identity of one worker, stable across restarts.

    The deployment generation is deliberately not part of it: a restarted
    worker on the same core is the same worker, and its restart is counted
    rather than turned into a second identity whose uptime starts again.
    """
    return (
        f"{attributes.get('pipeline.group.id')}/"
        f"{attributes.get('pipeline.id')}/"
        f"core{attributes.get('core.id')}"
    )


def entity_key(attributes) -> str:
    """The identity of one instrumented entity inside a worker."""
    return ",".join(
        f"{key}={value}"
        for key, value in sorted(attributes.items())
        if key not in WORKER_ATTRIBUTE_KEYS
    )


def parse_telemetry(document, *, expected_workers, include_system=False) -> dict:
    """Split one JSON telemetry response into per-worker observations.

    Every metric set is attributed to exactly one worker by its own group,
    pipeline, core and generation, and within that worker to one entity by
    its node identity. A metric that is missing, not a number or attributed
    to no worker is an error rather than a zero.
    """
    if "metric_sets" not in document:
        raise AssertionError("the telemetry response carries no metric sets")
    workers = {}
    process = {}
    for entry in document["metric_sets"]:
        name = entry.get("name")
        attributes = attributes_of(entry)
        if not attributes:
            # The engine publishes a handful of process-wide values without
            # a pipeline identity. They are process metrics and are never
            # summed across workers.
            for metric in entry.get("metrics", []):
                if isinstance(metric.get("value"), (int, float)):
                    process[f"{name}.{metric['name']}"] = metric["value"]
            continue
        if attributes.get("pipeline.group.id") is None:
            continue
        if not include_system and attributes.get("pipeline.group.id") == SYSTEM_GROUP:
            continue
        key = worker_key(attributes)
        entity = entity_key(attributes)
        worker = workers.setdefault(
            key,
            {
                "key": key,
                "group_id": attributes.get("pipeline.group.id"),
                "pipeline_id": attributes.get("pipeline.id"),
                "core_id": attributes.get("core.id"),
                "generation": attributes.get("deployment.generation"),
                "metrics": {},
                "labelled": {},
            },
        )
        if worker["generation"] != attributes.get("deployment.generation"):
            raise AssertionError(
                f"one scrape reports worker {key} in generations "
                f"{worker['generation']!r} and "
                f"{attributes.get('deployment.generation')!r}"
            )
        for metric in entry.get("metrics", []):
            value = metric.get("value")
            labels = {
                label: attribute_value(raw)
                for label, raw in (metric.get("attributes") or {}).items()
            }
            slot = f"{name}:{metric['name']}"
            if labels:
                label_key = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
                worker["labelled"].setdefault(slot, {}).setdefault(entity, {})[
                    label_key
                ] = value
                continue
            seen = worker["metrics"].setdefault(slot, {})
            if entity in seen and seen[entity] != value:
                raise AssertionError(
                    f"{slot} was reported twice for {key}/{entity} with "
                    f"different values {seen[entity]!r} and {value!r}"
                )
            seen[entity] = value
    if expected_workers is not None and len(workers) != expected_workers:
        raise AssertionError(
            f"expected {expected_workers} workers, the telemetry named "
            f"{sorted(workers)}"
        )
    return {"workers": workers, "process": process}


def require_gauge(worker, metric_set, name):
    """One numeric gauge of one worker, or an error naming what is absent.

    A metric that several entities of the same worker publish is ambiguous
    and fails: a measurement may not silently pick one of them or add them
    together.
    """
    slot = f"{metric_set}:{name}"
    entities = worker["metrics"].get(slot)
    if not entities:
        raise AssertionError(f"worker {worker['key']} publishes no {slot}")
    if len(entities) != 1:
        raise AssertionError(
            f"worker {worker['key']} publishes {slot} for "
            f"{sorted(entities)}; the sample cannot attribute it"
        )
    value = next(iter(entities.values()))
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise AssertionError(f"worker {worker['key']} reports {slot} as {value!r}")
    return value


def sample_engine(engine, *, expected_workers: int, buffered=False) -> dict:
    """One timestamped strict sample of every worker and of the process.

    The engine's own JSON telemetry is read with zeroes retained, so an
    absent gauge is distinguishable from a zero one. Process values such as
    resident memory are kept apart from per-worker values and are never
    summed across workers.
    """
    monotonic_ns = time.monotonic_ns()
    document = test_e2e.engine_metrics(engine)
    parsed = parse_telemetry(document, expected_workers=expected_workers)
    workers = {}
    for key, worker in parsed["workers"].items():
        gauges = {
            name: require_gauge(worker, "exporter.series_parquet", name)
            for name in REQUIRED_EXPORTER_GAUGES
        }
        uptime = require_gauge(worker, "pipeline", "uptime")
        entry = {
            "key": key,
            "core_id": worker["core_id"],
            "generation": worker["generation"],
            "uptime_s": uptime,
            "gauges": gauges,
            "labelled": worker["labelled"],
            # A snapshot the worker did not answer carries its metric names
            # with every value zero, so presence alone does not make it an
            # observation.
            "reported": gauges[LIVENESS_GAUGE] > 0,
        }
        if buffered:
            entry["buffer"] = {
                f"{metric_set}:{name}": require_gauge(worker, metric_set, name)
                for metric_set, name in BUFFER_EMPTY_GAUGES
            }
        workers[key] = entry
    pid = engine.process.pid
    return {
        "monotonic_ns": monotonic_ns,
        "observed_utc": utc_now(),
        "scrape_timestamp": document.get("timestamp"),
        "workers": workers,
        "process": parsed["process"],
        "process_rss_bytes": test_e2e.rss_bytes(pid),
        "process_pid": pid,
        "raw_sha256": hashlib.sha256(
            json.dumps(document, sort_keys=True).encode("ascii", "replace")
        ).hexdigest(),
    }


def procfs_process_sample(pid: int) -> dict:
    """Resident, stat, smaps-rollup and descriptor counts for one process.

    These are process roles, never worker values: they are recorded on the
    same monotonic timeline as the telemetry so that a residual can be
    computed against the same instant rather than against a nearby one.
    """
    sample = {"pid": pid, "monotonic_ns": time.monotonic_ns()}
    sample["rss_bytes"] = test_e2e.rss_bytes(pid)
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        sample["utime_ticks"] = int(fields[11])
        sample["stime_ticks"] = int(fields[12])
        sample["num_threads"] = int(fields[17])
        sample["vsize_bytes"] = int(fields[20])
        sample["clock_ticks_per_s"] = os.sysconf("SC_CLK_TCK")
    except (OSError, IndexError, ValueError) as error:
        sample["stat_reason"] = str(error)
    rollup = Path(f"/proc/{pid}/smaps_rollup")
    if rollup.is_file():
        try:
            for line in rollup.read_text().splitlines():
                if ":" in line:
                    key, value = line.split(":", 1)
                    parts = value.split()
                    if parts and parts[0].isdigit():
                        sample[f"smaps_{key.strip().lower()}_bytes"] = (
                            int(parts[0]) * 1024
                        )
        except OSError as error:
            sample["smaps_reason"] = str(error)
    else:
        sample["smaps_reason"] = "smaps_rollup is unavailable"
    try:
        sample["open_fd_count"] = len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except OSError as error:
        sample["fd_reason"] = str(error)
    return sample


def directory_bytes(path) -> int:
    """Allocated disk bytes below one directory, blocks rather than length."""
    total = 0
    root = Path(path)
    if not root.exists():
        return 0
    for entry in root.rglob("*"):
        try:
            if entry.is_file():
                total += entry.stat().st_blocks * 512
        except OSError:
            continue
    return total


class EpochTracker:
    """Per-worker collection epochs, discriminated by `pipeline.uptime`.

    The response's own timestamp is generated per scrape and proves nothing
    about a collection having happened, so an epoch is an advance of the
    worker's collection-updated uptime gauge. An uptime that goes backwards
    within one generation is an error; a new generation is a restart and is
    counted separately.
    """

    def __init__(self):
        self.last = {}
        self.advances = collections.Counter()
        self.restarts = collections.Counter()
        self.generations = {}

    def observe(self, sample) -> set:
        """Record one sample and return the workers that advanced."""
        advanced = set()
        for key, worker in sample["workers"].items():
            uptime = worker["uptime_s"]
            generation = worker["generation"]
            previous_generation = self.generations.get(key)
            if previous_generation is not None and generation != previous_generation:
                self.restarts[key] += 1
                self.last.pop(key, None)
            self.generations[key] = generation
            previous = self.last.get(key)
            if previous is None:
                self.last[key] = uptime
                continue
            if uptime < previous:
                raise AssertionError(
                    f"worker {key} reported uptime {uptime} after {previous} "
                    f"within generation {generation}"
                )
            if uptime > previous:
                self.last[key] = uptime
                self.advances[key] += 1
                advanced.add(key)
        return advanced

    def as_json(self) -> dict:
        """What was observed, for a result file."""
        return {
            "advances_by_worker": dict(self.advances),
            "restarts_by_worker": dict(self.restarts),
            "generations": dict(self.generations),
        }


def worker_reported(worker) -> bool:
    """Whether the worker answered the collection this sample was built from.

    Derived from the sample's own gauges rather than from a flag a caller
    might forget to set, so every consumer of a sample gets the check.
    """
    return worker["gauges"].get(LIVENESS_GAUGE, 0) > 0


def worker_is_empty(worker, *, buffered) -> bool:
    """Whether one worker holds no requests, no block and no notifications.

    A worker that did not answer this collection is never empty: its gauges
    all read zero whatever it is actually holding, so reading them as an
    empty state would let a stalled exporter prove its own drainage.
    """
    if not worker_reported(worker):
        return False
    for name in EXPORTER_EMPTY_GAUGES:
        if worker["gauges"][name] != 0:
            return False
    if buffered:
        for metric_set, name in BUFFER_EMPTY_GAUGES:
            if worker["buffer"][f"{metric_set}:{name}"] != 0:
                return False
    return True


def observe_drain(sample_once, *, expected_workers, buffered, deadline_ns):
    """Require three consecutive empty collection epochs of every worker.

    Repeated responses carrying an unchanged uptime add no observation at
    all, so a fast poller cannot manufacture a proof out of one collection.
    Any nonempty worker resets that worker's streak, and a deadline that
    expires without the streaks is a failure, never a pass.
    """
    tracker = EpochTracker()
    wake = threading.Event()
    streak = collections.Counter()
    unanswered = collections.Counter()
    samples = []
    last = None
    while time.monotonic_ns() < deadline_ns:
        sample = sample_once()
        samples.append(sample)
        advanced = tracker.observe(sample)
        for key in advanced:
            worker = sample["workers"][key]
            if not worker_reported(worker):
                # The uptime advanced but this worker did not answer the
                # collection, so the epoch says nothing about it: it is
                # neither an empty observation nor a nonempty one.
                unanswered[key] += 1
                continue
            if worker_is_empty(worker, buffered=buffered):
                streak[key] += 1
            else:
                streak[key] = 0
        last = {key: streak.get(key, 0) for key in sample["workers"]}
        if (
            len(last) == expected_workers
            and last
            and all(value >= DRAIN_EMPTY_EPOCHS for value in last.values())
        ):
            return {
                "drained": True,
                "empty_epochs_by_worker": dict(last),
                "unanswered_epochs_by_worker": dict(unanswered),
                "epochs": tracker.as_json(),
                "sample_count": len(samples),
                "samples": samples,
            }
        remaining = (deadline_ns - time.monotonic_ns()) / 1e9
        if remaining > 0:
            # Only schedules the next observation; elapsed time alone never
            # advances an epoch or proves drainage.
            _ = wake.wait(min(0.05, remaining))
    raise AssertionError(
        f"drain deadline: workers reached empty epochs {last!r}, needed "
        f"{DRAIN_EMPTY_EPOCHS} for each of {expected_workers} workers; "
        f"unanswered={dict(unanswered)}; epochs={tracker.as_json()}"
    )


# --------------------------------------------------------------------------
# Reader oracle
# --------------------------------------------------------------------------

# How many actual rows may be carried into the ledger in one transaction.
# The oracle never holds the run's records in a Python collection: rows are
# streamed from the reader into SQLite in bounded batches and every
# comparison after that is a join.
ORACLE_BATCH = 10000

# Above this many rows the cross-reader agreement falls back from an exact
# ordered digest to bounds, because the digest sorts inside the engine.
CROSS_READER_MAX_ROWS = 2_000_000


def _duck_latest_descriptor(root: Path, signal: str):
    """The values relation, the descriptor relation and the join clause."""
    values_glob = root / f"v=1/signal={signal}/dataset=values/**/*.parquet"
    series_glob = root / f"v=1/signal={signal}/dataset=series/**/*.parquet"
    values = (
        f"read_parquet({test_e2e.sql_string(values_glob)}, union_by_name=true, "
        "hive_partitioning=false)"
    )
    series = (
        f"read_parquet({test_e2e.sql_string(series_glob)}, union_by_name=true, "
        "filename=true, hive_partitioning=false)"
    )
    canonical = (
        f"(SELECT * FROM {series} QUALIFY row_number() OVER ("
        "PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1)"
    )
    return values, canonical


def _duck_record_expression(workload: Workload, signal: str):
    """DuckDB expressions for a row's record id and canonical payload hash."""
    if signal == "logs":
        record_id = f"substr(v.body, 1, {ID_FIXED_WIDTH + len(LOG_KIND)})"
        payload = (
            f"'{LOG_KIND}|' || {record_id} || '|body=' || v.body || '|time=' "
            "|| CAST(v.time_unix_nano AS VARCHAR) || '|logger=' || "
            "coalesce(list_extract(map_extract(s.attrs, 'logger.name'), 1), "
            "'<absent>')"
        )
        return record_id, payload
    ordinal = (
        f"((CAST(v.time_unix_nano AS HUGEINT) - {METRIC_BASE_TIME_NS}) "
        f"// {METRIC_TIME_STEP_NS})"
    )
    kind = f"replace(v.metric_name, '{METRIC_NAME_PREFIX}', '')"
    record_id = (
        f"printf('%08x:%012d:%06d:%s', {workload.seed}, "
        f"{ordinal} // {workload.records_per_request}, "
        f"{ordinal} % {workload.records_per_request}, {kind})"
    )

    def number(column):
        """The reader's rendering of an integer column."""
        return f"coalesce(CAST(v.{column} AS VARCHAR), 'null')"

    def double(column):
        """The reader's rendering of a double column."""
        return f"coalesce(CAST(CAST(v.{column} AS BIT) AS VARCHAR), 'null')"

    def int_list(column):
        """The reader's rendering of a list-of-integer column."""
        return (
            f"coalesce(array_to_string(list_transform(v.{column}, "
            "x -> CAST(x AS VARCHAR)), ','), '')"
        )

    def double_list(column):
        """The reader's rendering of a list-of-double column."""
        return (
            f"coalesce(array_to_string(list_transform(v.{column}, "
            "x -> CAST(CAST(x AS BIT) AS VARCHAR)), ','), '')"
        )

    payload = " || '|' || ".join(
        (
            kind,
            record_id,
            f"'int=' || {number('value_int')}",
            f"'double=' || {double('value_double')}",
            f"'count=' || {number('count')}",
            f"'sum=' || {double('sum')}",
            f"'min=' || {double('min')}",
            f"'max=' || {double('max')}",
            f"'buckets=' || {int_list('bucket_counts')}",
            f"'bounds=' || {double_list('explicit_bounds')}",
            "'time=' || CAST(v.time_unix_nano AS VARCHAR)",
        )
    )
    return record_id, payload


def _clickhouse_record_expression(workload: Workload, signal: str):
    """The same two expressions written for clickhouse-local."""
    if signal == "logs":
        record_id = f"substring(v.body, 1, {ID_FIXED_WIDTH + len(LOG_KIND)})"
        payload = (
            f"concat('{LOG_KIND}|', {record_id}, '|body=', v.body, '|time=', "
            "toString(v.time_unix_nano), '|logger=', "
            "if(has(mapKeys(s.attrs), 'logger.name'), "
            "toString(s.attrs['logger.name']), '<absent>'))"
        )
        return record_id, payload
    ordinal = (
        f"intDiv(toInt128(v.time_unix_nano) - {METRIC_BASE_TIME_NS}, "
        f"{METRIC_TIME_STEP_NS})"
    )
    kind = f"replaceAll(v.metric_name, '{METRIC_NAME_PREFIX}', '')"
    record_id = (
        f"concat(leftPad(lower(hex(toUInt32({workload.seed}))), 8, '0'), ':', "
        f"leftPad(toString(intDiv({ordinal}, {workload.records_per_request})), "
        "12, '0'), ':', "
        f"leftPad(toString(modulo({ordinal}, {workload.records_per_request})), "
        f"6, '0'), ':', {kind})"
    )

    def number(column):
        """The reader's rendering of an integer column."""
        return f"ifNull(toString(v.{column}), 'null')"

    def double(column):
        """The reader's rendering of a double column."""
        return (
            f"if(isNull(v.{column}), 'null', "
            f"bin(reverse(reinterpretAsFixedString(assumeNotNull(v.{column})))))"
        )

    def int_list(column):
        """The reader's rendering of a list-of-integer column."""
        return (
            f"ifNull(arrayStringConcat(arrayMap(x -> toString(x), v.{column}), "
            "','), '')"
        )

    def double_list(column):
        """The reader's rendering of a list-of-double column."""
        return (
            "ifNull(arrayStringConcat(arrayMap(x -> "
            "bin(reverse(reinterpretAsFixedString(assumeNotNull(x)))), "
            f"v.{column}), ','), '')"
        )

    payload = (
        "concat("
        + ", '|', ".join(
            (
                kind,
                record_id,
                f"concat('int=', {number('value_int')})",
                f"concat('double=', {double('value_double')})",
                f"concat('count=', {number('count')})",
                f"concat('sum=', {double('sum')})",
                f"concat('min=', {double('min')})",
                f"concat('max=', {double('max')})",
                f"concat('buckets=', {int_list('bucket_counts')})",
                f"concat('bounds=', {double_list('explicit_bounds')})",
                "concat('time=', toString(v.time_unix_nano))",
            )
        )
        + ")"
    )
    return record_id, payload


def _load_actual(ledger: Ledger, cursor_rows):
    """Stream reader rows into the ledger's `actual` table in batches."""
    connection = ledger.connection
    with ledger.lock:
        _ = connection.execute("DROP TABLE IF EXISTS actual")
        _ = connection.execute(
            "CREATE TABLE actual (record_id TEXT NOT NULL, signal TEXT NOT NULL, "
            "payload_sha256 TEXT NOT NULL)"
        )
        batch = []
        total = 0
        for row in cursor_rows:
            batch.append(row)
            if len(batch) >= ORACLE_BATCH:
                _ = connection.executemany(
                    "INSERT INTO actual VALUES (?, ?, ?)", batch
                )
                total += len(batch)
                batch = []
        if batch:
            _ = connection.executemany("INSERT INTO actual VALUES (?, ?, ?)", batch)
            total += len(batch)
        _ = connection.execute(
            "CREATE INDEX actual_by_record ON actual(record_id)"
        )
    return total


def _duck_rows(db, sql, batch=ORACLE_BATCH):
    """Yield a DuckDB result in bounded batches rather than all at once."""
    result = db.execute(sql)
    while True:
        rows = result.fetchmany(batch)
        if not rows:
            return
        yield from rows


def _reader_digest(db, sql_count, sql_digest, sql_bounds):
    """An order-independent agreement value for one reader."""
    rows = int(db(sql_count)[0][0])
    if rows <= CROSS_READER_MAX_ROWS:
        return {"mode": "digest", "row_count": rows, "digest": db(sql_digest)[0][0]}
    low, high = db(sql_bounds)[0][:2]
    return {"mode": "bounds", "row_count": rows, "min": low, "max": high}


def read_oracle(root, ledger: Ledger, *, require_all: bool, healthy: bool,
                workload: Workload, test=None, cross_reader=True) -> dict:
    """Check every stored row against what the producer intended.

    The comparison is a join rather than a Python set: the reader's rows are
    streamed into the ledger, and missing, unexpected, corrupt and duplicated
    records are then counted in SQL. `require_all` decides whether every
    intended record must be present or only the acknowledged ones; the
    difference matters exactly when a run ended while requests were still
    outstanding.

    Object invariants -- layout, recorded row counts, declared sort order,
    descriptor identity hashes, per-partition descriptor coverage and the
    null-versus-empty list shape -- are checked by `test_e2e.scan_objects`,
    the same code the fixture suite uses.
    """
    root = Path(root).resolve()
    case = test if test is not None else _SilentAsserts()
    report = {
        "root": str(root),
        "require_all": require_all,
        "healthy": healthy,
        "readers": {},
    }
    with duckdb.connect() as db:
        files, coverage, values, _bodies, _metric_rows = test_e2e.scan_objects(
            case, root, db
        )
        report["part_file_count"] = len(files)
        report["descriptor_identity_count"] = len(coverage)
        report["values_key_count"] = len(values)
        _check_list_shapes(case, db, root, workload)
        report["series_cardinality"] = _series_cardinality(db, root)

        def stream():
            """Yield one (record id, signal, payload hash) row per stored row."""
            for signal in ("logs", "metrics"):
                if not any(
                    f"signal={signal}" in path.parts and "dataset=values" in path.parts
                    for path in files
                ):
                    continue
                values_rel, canonical = _duck_latest_descriptor(root, signal)
                record_id, payload = _duck_record_expression(workload, signal)
                sql = (
                    f"SELECT {record_id}, '{signal}', lower(sha256({payload})) "
                    f"FROM {values_rel} v INNER JOIN {canonical} s "
                    "ON v.series_id = s.series_id"
                )
                yield from _duck_rows(db, sql)

        report["actual_row_count"] = _load_actual(ledger, stream())
        if cross_reader:
            report["readers"] = _cross_reader_agreement(db, root, workload, files)
    report.update(_compare(ledger, require_all=require_all, healthy=healthy))
    return report


class _SilentAsserts:
    """The `unittest` assertions the scanner needs, outside a test case."""

    def assertTrue(self, value, message=""):
        """Fail with `message` unless `value` is truthy."""
        if not value:
            raise AssertionError(message or "assertTrue failed")

    def assertEqual(self, one, other, message=""):
        """Fail with `message` unless the two values are equal."""
        if one != other:
            raise AssertionError(message or f"{one!r} != {other!r}")

    def assertGreaterEqual(self, one, other, message=""):
        """Fail with `message` unless `one` is at least `other`."""
        if not one >= other:
            raise AssertionError(message or f"{one!r} < {other!r}")


def _check_list_shapes(case, db, root, workload):
    """Each metric kind's list columns must have the declared shape.

    Only DuckDB can see the difference between a null list and an empty
    list, so this is deliberately a one-reader check: ClickHouse reads a null
    Parquet list as an empty array and would agree with either.
    """
    glob = root / "v=1/signal=metrics/dataset=values/**/*.parquet"
    if not any(root.glob("v=1/signal=metrics/dataset=values/**/*.parquet")):
        return
    relation = (
        f"read_parquet({test_e2e.sql_string(glob)}, union_by_name=true, "
        "hive_partitioning=false)"
    )
    rows = db.execute(
        f"SELECT replace(metric_name, '{METRIC_NAME_PREFIX}', ''), "
        "count(*) FILTER (bucket_counts IS NULL), "
        "count(*) FILTER (bucket_counts IS NOT NULL AND len(bucket_counts) = 0), "
        "count(*) FILTER (bucket_counts IS NOT NULL AND len(bucket_counts) > 0), "
        f"count(*) FROM {relation} GROUP BY 1"
    ).fetchall()
    for kind, null_lists, empty_lists, filled_lists, total in rows:
        shape = METRIC_LIST_SHAPE.get(kind)
        case.assertTrue(shape is not None, f"unexpected metric kind {kind}")
        expected = {"null": null_lists, "empty": empty_lists, "filled": filled_lists}
        case.assertEqual(
            expected[shape],
            total,
            f"{kind} rows must store {shape} bucket_counts: {expected}",
        )


def _series_cardinality(db, root) -> dict:
    """Distinct descriptor identities per signal, kept apart from records."""
    counts = {}
    for signal in ("logs", "metrics"):
        glob = root / f"v=1/signal={signal}/dataset=series/**/*.parquet"
        if not any(root.glob(f"v=1/signal={signal}/dataset=series/**/*.parquet")):
            counts[f"{signal}_series_count"] = 0
            continue
        relation = (
            f"read_parquet({test_e2e.sql_string(glob)}, union_by_name=true, "
            "hive_partitioning=false)"
        )
        counts[f"{signal}_series_count"] = int(
            db.execute(f"SELECT count(DISTINCT series_id) FROM {relation}").fetchone()[
                0
            ]
        )
    return counts


def _cross_reader_agreement(db, root, workload, files) -> dict:
    """DuckDB and clickhouse-local must agree on what the files hold."""
    agreement = {}
    with test_e2e.clickhouse_reader(root) as clickhouse:
        for signal in ("logs", "metrics"):
            if not any(
                f"signal={signal}" in path.parts and "dataset=values" in path.parts
                for path in files
            ):
                continue
            values_rel, canonical = _duck_latest_descriptor(root, signal)
            _, duck_payload = _duck_record_expression(workload, signal)
            duck_from = (
                f"FROM {values_rel} v INNER JOIN {canonical} s "
                "ON v.series_id = s.series_id"
            )
            values_glob = f"v=1/signal={signal}/dataset=values/**/*.parquet"
            series_glob = f"v=1/signal={signal}/dataset=series/**/*.parquet"
            ch_values = f"file({test_e2e.sql_string(values_glob)}, 'Parquet')"
            ch_series = f"file({test_e2e.sql_string(series_glob)}, 'Parquet')"
            ch_canonical = (
                "(SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY "
                "series_id ORDER BY emitted_at DESC, _path DESC) AS rank FROM "
                f"{ch_series}) WHERE rank = 1)"
            )
            _, ch_payload = _clickhouse_record_expression(workload, signal)
            ch_from = (
                f"FROM {ch_values} AS v INNER JOIN {ch_canonical} AS s "
                "ON v.series_id = s.series_id"
            )
            duck = _reader_digest(
                lambda sql: db.execute(sql).fetchall(),
                f"SELECT count(*) {duck_from}",
                f"SELECT lower(sha256(string_agg({duck_payload}, '|#|' ORDER BY "
                f"{duck_payload}))) {duck_from}",
                f"SELECT min({duck_payload}), max({duck_payload}) {duck_from}",
            )
            other = _reader_digest(
                clickhouse,
                f"SELECT count(*) {ch_from}",
                f"SELECT lower(hex(SHA256(arrayStringConcat(arraySort("
                f"groupArray({ch_payload})), '|#|')))) {ch_from}",
                f"SELECT min({ch_payload}), max({ch_payload}) {ch_from}",
            )
            if duck != other:
                raise AssertionError(
                    f"reader disagreement for {signal}: duckdb={duck} "
                    f"clickhouse={other}"
                )
            agreement[signal] = duck
    return agreement


def _compare(ledger: Ledger, *, require_all: bool, healthy: bool) -> dict:
    """Count missing, unexpected, corrupt and duplicated records in SQL."""
    scope = (
        "records"
        if require_all
        else (
            "(SELECT r.* FROM records r JOIN requests q "
            "ON q.request_id = r.request_id WHERE q.ack_ns IS NOT NULL)"
        )
    )
    with ledger.lock:
        cursor = ledger.connection.cursor()
        expected_count = cursor.execute(f"SELECT count(*) FROM {scope}").fetchone()[0]
        missing = cursor.execute(
            f"SELECT count(*) FROM {scope} e WHERE NOT EXISTS "
            "(SELECT 1 FROM actual a WHERE a.record_id = e.record_id)"
        ).fetchone()[0]
        missing_sample = [
            row[0]
            for row in cursor.execute(
                f"SELECT e.record_id FROM {scope} e WHERE NOT EXISTS "
                "(SELECT 1 FROM actual a WHERE a.record_id = e.record_id) "
                "ORDER BY e.record_id LIMIT 10"
            ).fetchall()
        ]
        unexpected = cursor.execute(
            "SELECT count(DISTINCT a.record_id) FROM actual a WHERE NOT EXISTS "
            "(SELECT 1 FROM records r WHERE r.record_id = a.record_id)"
        ).fetchone()[0]
        unexpected_sample = [
            row[0]
            for row in cursor.execute(
                "SELECT DISTINCT a.record_id FROM actual a WHERE NOT EXISTS "
                "(SELECT 1 FROM records r WHERE r.record_id = a.record_id) "
                "ORDER BY a.record_id LIMIT 10"
            ).fetchall()
        ]
        corrupt = cursor.execute(
            f"SELECT count(*) FROM actual a JOIN {scope} e "
            "ON e.record_id = a.record_id "
            "WHERE a.payload_sha256 <> e.expected_sha256"
        ).fetchone()[0]
        corrupt_sample = [
            row[0]
            for row in cursor.execute(
                f"SELECT a.record_id FROM actual a JOIN {scope} e "
                "ON e.record_id = a.record_id "
                "WHERE a.payload_sha256 <> e.expected_sha256 "
                "ORDER BY a.record_id LIMIT 10"
            ).fetchall()
        ]
        histogram = dict(
            (int(copies), int(count))
            for copies, count in cursor.execute(
                "SELECT copies, count(*) FROM (SELECT e.record_id, "
                f"count(a.record_id) AS copies FROM {scope} e LEFT JOIN actual a "
                "ON a.record_id = e.record_id GROUP BY e.record_id) "
                "GROUP BY copies ORDER BY copies"
            ).fetchall()
        )
        by_kind = dict(
            cursor.execute(
                f"SELECT e.kind, count(a.record_id) FROM {scope} e "
                "LEFT JOIN actual a ON a.record_id = e.record_id GROUP BY e.kind"
            ).fetchall()
        )
    result = {
        "expected_record_count": expected_count,
        "missing_record_count": missing,
        "missing_sample": missing_sample,
        "unexpected_record_count": unexpected,
        "unexpected_sample": unexpected_sample,
        "corrupt_record_count": corrupt,
        "corrupt_sample": corrupt_sample,
        "multiplicity_histogram": {str(key): value for key, value in
                                   sorted(histogram.items())},
        "stored_rows_by_kind": by_kind,
    }
    problems = []
    if missing:
        problems.append(f"missing={missing} {missing_sample}")
    if unexpected:
        problems.append(f"unexpected={unexpected} {unexpected_sample}")
    if corrupt:
        problems.append(f"corrupt={corrupt} {corrupt_sample}")
    if healthy and set(histogram) - {1}:
        problems.append(f"multiplicity={sorted(histogram.items())}")
    result["problems"] = problems
    result["passed"] = not problems
    return result


# --------------------------------------------------------------------------
# Result files
# --------------------------------------------------------------------------

STATUS_PASSED = "passed"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUSES = (STATUS_PASSED, STATUS_FAILED, STATUS_SKIPPED)

RESULT_FIELDS = (
    "schema_version",
    "run_id",
    "case",
    "status",
    "started_utc",
    "elapsed_s",
    "environment",
    "config",
    "workload",
    "metrics",
    "samples",
    "events",
    "checks",
    "artifacts",
)

CHECK_HARD = "hard"
CHECK_MEASURED = "measured"


def check(name, kind, status, detail="") -> dict:
    """One acceptance check, recorded whatever it decided."""
    if kind not in (CHECK_HARD, CHECK_MEASURED):
        raise ValueError(f"unknown check kind {kind}")
    if status not in STATUSES:
        raise ValueError(f"unknown check status {status}")
    return {"name": name, "kind": kind, "status": status, "detail": detail}


def metric_direction(name: str) -> str:
    """Whether a larger value of one metric is better or worse."""
    if name.endswith(HIGHER_IS_BETTER_SUFFIXES):
        return "higher_is_better"
    return "lower_is_better"


def validate_result(result) -> None:
    """Refuse to write a result that cannot be read as evidence.

    Every mandatory field must be present, every metric name must state its
    unit, an unavailable metric must be null with a reason, and a passed run
    may not be missing a mandatory metric. This runs before the file is
    written, so an invalid result never reaches the report directory.
    """
    missing = [field for field in RESULT_FIELDS if field not in result]
    if missing:
        raise AssertionError(f"result is missing {missing}")
    if result["schema_version"] != SCHEMA_VERSION:
        raise AssertionError(f"unknown schema version {result['schema_version']}")
    if result["status"] not in STATUSES:
        raise AssertionError(f"unknown status {result['status']}")
    if not isinstance(result["elapsed_s"], (int, float)) or result["elapsed_s"] < 0:
        raise AssertionError(f"elapsed_s must be a duration: {result['elapsed_s']!r}")
    environment = result["environment"]
    for edge in ("start", "end"):
        if edge not in environment:
            raise AssertionError(f"environment.{edge} is required on every run")
        snapshot = environment[edge]
        for field in (
            "cpu_model",
            "logical_core_count",
            "physical_core_count",
            "ram_bytes",
            "kernel",
            "load_average_1_5_15",
            "thread_affinity",
        ):
            if field not in snapshot:
                raise AssertionError(f"environment.{edge} is missing {field}")
    unavailable = result.get("metrics_unavailable", {})
    for name, value in result["metrics"].items():
        if not name.endswith(UNIT_SUFFIXES):
            raise AssertionError(f"metric {name} does not state its unit")
        if value is None:
            if name not in unavailable:
                raise AssertionError(f"metric {name} is null without a reason")
        elif not isinstance(value, (int, float)) or isinstance(value, bool):
            raise AssertionError(f"metric {name} is not a number: {value!r}")
    for name in unavailable:
        if result["metrics"].get(name) is not None:
            raise AssertionError(f"metric {name} has both a value and a reason")
    mandatory = result.get("mandatory_metrics", [])
    if result["status"] == STATUS_PASSED:
        absent = [name for name in mandatory if result["metrics"].get(name) is None]
        if absent:
            raise AssertionError(
                f"a passed run cannot be missing mandatory metrics {absent}"
            )
        failed = [
            entry["name"]
            for entry in result["checks"]
            if entry["status"] == STATUS_FAILED
        ]
        if failed:
            raise AssertionError(f"a passed run cannot carry failed checks {failed}")
    for entry in result["checks"]:
        for field in ("name", "kind", "status"):
            if field not in entry:
                raise AssertionError(f"check {entry!r} is missing {field}")


def write_result(path, result) -> Path:
    """Write one result file atomically, after validating it."""
    path = Path(path)
    validate_result(result)
    encoded = (
        json.dumps(
            result, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    _ = temporary.write_text(encoded, encoding="ascii")
    _ = temporary.replace(path)
    return path


def file_digest(path) -> str:
    """The SHA-256 of one file, read in bounded chunks."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_entry(path) -> dict:
    """One artifact reference: its exact name, hash and size."""
    path = Path(path)
    return {
        "name": path.name,
        "sha256": file_digest(path),
        "size_bytes": path.stat().st_size,
    }


def new_result(spec, *, artifact_kind="run") -> dict:
    """An empty result carrying everything a run cannot start without."""
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": spec.run_id if isinstance(spec, RunSpec) else spec["run_id"],
        "case": spec.case if isinstance(spec, RunSpec) else spec["case"],
        "artifact_kind": artifact_kind,
        "status": STATUS_FAILED,
        "started_utc": utc_now(),
        "elapsed_s": 0.0,
        "environment": {},
        "config": {"requested": {}, "effective": {}},
        "workload": (
            spec.workload.as_json() if isinstance(spec, RunSpec) else {}
        ),
        "metrics": {},
        "metrics_unavailable": {},
        "mandatory_metrics": [],
        "samples": [],
        "events": [],
        "checks": [],
        "artifacts": [],
        "run_files": [],
        "baseline_files": [],
        "child_indexes": [],
        "report_dir": REPORT_DIR_RELATIVE,
    }


def record_event(result, kind, detail="") -> None:
    """Append one timestamped event to a result."""
    result["events"].append(
        {
            "monotonic_ns": time.monotonic_ns(),
            "observed_utc": utc_now(),
            "kind": kind,
            "detail": detail,
        }
    )


# --------------------------------------------------------------------------
# Baselines
# --------------------------------------------------------------------------


def baseline_fingerprint(result) -> str:
    """The identity a baseline may only be compared within.

    Machine, core allocation, effective configuration, workload and build
    profile. The source revision and the binary hash are deliberately not in
    it: they are provenance, so a new implementation on the same machine and
    configuration is compared rather than excused.
    """
    environment = result.get("environment", {})
    start = environment.get("start", {})
    material = {
        "case": result["case"],
        "machine": {
            "identity_sha256": environment.get("machine_identity_sha256"),
            "cpu_model": start.get("cpu_model"),
            "logical_core_count": start.get("logical_core_count"),
            "physical_core_count": start.get("physical_core_count"),
            "ram_bytes": start.get("ram_bytes"),
            "kernel": start.get("kernel"),
        },
        "core_allocation": environment.get("core_allocation"),
        "config": result.get("config", {}).get("effective"),
        "workload": result.get("workload"),
        "build": {
            key: environment.get("build", {}).get(key)
            for key in ("profile", "features", "allocator", "toolchain")
        },
    }
    encoded = json.dumps(material, sort_keys=True, ensure_ascii=True, allow_nan=False)
    return hashlib.sha256(encoded.encode("ascii")).hexdigest()


def baseline_name(case, fingerprint) -> str:
    """The immutable file name of one baseline."""
    return f"baseline-{case}-{fingerprint[:16]}.json"


def new_baseline(result, fingerprint) -> dict:
    """A baseline candidate built from one valid run."""
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "baseline",
        "case": result["case"],
        "fingerprint": fingerprint,
        "created_utc": utc_now(),
        "metric_schema": sorted(
            name for name, value in result["metrics"].items() if value is not None
        ),
        "metrics": {
            name: value
            for name, value in sorted(result["metrics"].items())
            if value is not None
        },
        "resolution_floor": dict(result.get("resolution_floor", {})),
        "source_run_ids": [result["run_id"]],
        "provenance": {
            "source_revision": result.get("environment", {})
            .get("git", {})
            .get("revision"),
            "binary_sha256": result.get("environment", {})
            .get("build", {})
            .get("binary_sha256"),
        },
    }


def load_baselines(report_dir, case) -> dict:
    """Every committed baseline of one case, keyed by fingerprint."""
    found = {}
    report_dir = Path(report_dir)
    if not report_dir.is_dir():
        return found
    for path in sorted(report_dir.glob(f"baseline-{case}-*.json")):
        try:
            document = json.loads(path.read_text(encoding="ascii"))
        except (OSError, ValueError) as error:
            raise AssertionError(f"{path} is not a readable baseline: {error}")
        fingerprint = document.get("fingerprint")
        if not fingerprint:
            raise AssertionError(f"{path} carries no fingerprint")
        if fingerprint in found:
            raise AssertionError(f"two baselines claim fingerprint {fingerprint}")
        found[fingerprint] = document
    return found


def signed_regression(name, value, reference, floor=None):
    """How much worse one metric got, as a signed fraction of the baseline.

    Positive is worse in the metric's own direction, whichever that is. A
    zero reference permits no increase at all unless the baseline declared a
    measurement-resolution floor that covers the new value.
    """
    direction = metric_direction(name)
    if reference == 0:
        if value == 0:
            return 0.0
        if floor is not None and abs(value) <= floor:
            return 0.0
        return float("inf") if direction == "lower_is_better" else float("-inf")
    if direction == "lower_is_better":
        return (value - reference) / abs(reference)
    return (reference - value) / abs(reference)


def hard_gate_failures(result) -> list:
    """Every hard check this run did not pass, whatever its status."""
    return [
        entry["name"]
        for entry in result.get("checks", [])
        if entry["kind"] == CHECK_HARD and entry["status"] != STATUS_PASSED
    ]


def evaluate_baseline(result: dict, *, baselines: dict = None) -> dict:
    """Apply the Controller baseline policy to one completed result.

    Hard gates come first: correctness, measurement validity and the
    accounted-versus-RSS residual fail on every run, whether or not a
    baseline exists, and a run that fails one never creates a baseline. Only
    then is the fingerprint resolved. A matching baseline is compared against
    and left untouched, so small regressions cannot ratchet it upward; a new
    fingerprint writes a candidate baseline beside the result instead of
    comparing against something it does not describe.

    The decision is attached to the result either way. A hard failure or a
    failed regression raises, and the caller still publishes the failed JSON.
    """
    failures = hard_gate_failures(result)
    fingerprint = baseline_fingerprint(result)
    decision = {
        "fingerprint": fingerprint,
        "limit": REGRESSION_LIMIT,
        "action": "rejected",
        "hard_gate_failures": failures,
        "regressions": {},
    }
    result["baseline_decision"] = decision
    if failures:
        raise AssertionError(f"hard gates failed, no baseline may be written: {failures}")
    if result["status"] != STATUS_PASSED:
        decision["action"] = "not_a_baseline_candidate"
        raise AssertionError(
            f"run {result['run_id']} is {result['status']}; it cannot be "
            f"compared or establish a baseline"
        )
    if baselines is None:
        baselines = load_baselines(
            resolve_report_dir(result.get("report_dir")), result["case"]
        )
    reference = baselines.get(fingerprint)
    if reference is None:
        decision["action"] = "created"
        decision["baseline_name"] = baseline_name(result["case"], fingerprint)
        decision["detail"] = (
            "no committed baseline describes this machine, configuration, "
            "workload and build profile"
        )
        result["baseline_candidate"] = new_baseline(result, fingerprint)
        return decision
    decision["action"] = "compared"
    decision["baseline_name"] = baseline_name(result["case"], fingerprint)
    schema = list(reference.get("metric_schema", sorted(reference["metrics"])))
    mismatch = [name for name in schema if result["metrics"].get(name) is None]
    if mismatch:
        raise AssertionError(
            f"the baseline compares metrics this run did not report: {mismatch}"
        )
    floors = reference.get("resolution_floor", {})
    worse = []
    for name in schema:
        value = result["metrics"][name]
        change = signed_regression(
            name, value, reference["metrics"][name], floors.get(name)
        )
        decision["regressions"][name] = {
            "value": value,
            "reference": reference["metrics"][name],
            "direction": metric_direction(name),
            "signed_regression": None if change in (float("inf"), float("-inf"))
            else change,
            "unbounded": change in (float("inf"), float("-inf")),
        }
        if change > REGRESSION_LIMIT:
            worse.append(name)
    decision["worse_than_limit"] = worse
    if worse:
        raise AssertionError(
            f"regression greater than {REGRESSION_LIMIT:.0%} in {worse} "
            f"against baseline {decision['baseline_name']}"
        )
    return decision


# --------------------------------------------------------------------------
# Publication and staging
# --------------------------------------------------------------------------


def safe_json_name(name) -> str:
    """One referenced file name, or an error explaining why it is refused.

    A referenced name is a plain file name in the report directory. Anything
    with a separator, a parent reference or another suffix is rejected, so an
    index can never make the publisher or the stager touch a path outside the
    evidence tree.
    """
    if not isinstance(name, str):
        raise AssertionError(f"a referenced file name must be text: {name!r}")
    if os.sep in name or (os.altsep and os.altsep in name) or "/" in name:
        raise AssertionError(f"a referenced file name may not contain a path: {name}")
    if name in (".", "..") or not SAFE_JSON_NAME.fullmatch(name):
        raise AssertionError(f"not a plain JSON file name: {name}")
    return name


def referenced_files(document) -> list:
    """The run, baseline and child index entries one document names."""
    entries = []
    for field in ("run_files", "baseline_files", "child_indexes"):
        for entry in document.get(field, []) or []:
            if isinstance(entry, str):
                entry = {"name": entry}
            name = safe_json_name(entry.get("name"))
            entries.append(
                {
                    "field": field,
                    "name": name,
                    "sha256": entry.get("sha256"),
                    "size_bytes": entry.get("size_bytes"),
                }
            )
    return entries


def enumerate_tree(index_path, *, directory=None, fallback=None) -> list:
    """Every file one index tree names, the index itself included.

    Each name is resolved in the artifact directory first and, when a
    `fallback` is given, in the already published report directory: an index
    that advances enumerates the index it replaced, and that older index's
    own evidence is already published rather than copied forward again.

    Child indexes are followed recursively; a cycle is an error rather than
    something to stop quietly at, because an index that references itself
    cannot describe a complete evidence tree. Every reference must carry the
    hash it was summarised with, and every hash is checked.

    Returns a list of `(name, resolved path)` in enumeration order.
    """
    index_path = Path(index_path)
    directory = Path(directory) if directory else index_path.parent
    fallback = Path(fallback) if fallback else None

    def resolve(name):
        """Where one referenced file actually is, or an error."""
        candidate = directory / name
        if candidate.is_file():
            return candidate
        if fallback is not None and (fallback / name).is_file():
            return fallback / name
        raise AssertionError(
            f"{name} is referenced but exists in neither {directory} nor "
            f"{fallback}"
            if fallback is not None
            else f"{candidate} is referenced but does not exist"
        )

    order = []
    seen = set()
    pending = [(index_path.name, [index_path.name])]
    while pending:
        name, trail = pending.pop(0)
        if name in seen:
            continue
        seen.add(name)
        path = index_path if name == index_path.name else resolve(name)
        order.append((name, path))
        try:
            document = json.loads(path.read_text(encoding="ascii"))
        except ValueError as error:
            raise AssertionError(f"{path} is not readable JSON: {error}")
        for entry in referenced_files(document):
            child = entry["name"]
            if child in trail:
                raise AssertionError(f"index cycle: {' -> '.join(trail)} -> {child}")
            if not entry["sha256"]:
                raise AssertionError(
                    f"{name} references {child} without a hash; evidence is "
                    f"enumerated with its exact name, size and hash"
                )
            resolved = resolve(child)
            actual = file_digest(resolved)
            if actual != entry["sha256"]:
                raise AssertionError(
                    f"{child} hashes to {actual}, the index recorded "
                    f"{entry['sha256']}"
                )
            if entry["field"] == "child_indexes":
                pending.append((child, trail + [child]))
            elif child not in seen:
                seen.add(child)
                order.append((child, resolved))
    return order


def publish_result_tree(index_path, report_dir=None) -> Path:
    """Copy one complete evidence tree into the report directory.

    Every enumerated file is copied by its exact name after its recorded hash
    has been checked. An existing published file with the same name and a
    different hash is a collision and fails: run and baseline file names are
    immutable, and a re-execution uses a new artifact directory rather than
    overwriting evidence. An index alone may advance, and only by enumerating
    the published index it replaces as an immutable child.
    """
    index_path = Path(index_path)
    report_dir = resolve_report_dir(report_dir)
    entries = enumerate_tree(index_path, fallback=report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    for name, source in entries:
        destination = report_dir / name
        if source.resolve() == destination.resolve():
            continue
        digest = file_digest(source)
        if destination.is_file():
            existing = file_digest(destination)
            if existing == digest:
                continue
            if name != index_path.name:
                raise AssertionError(
                    f"{destination} already exists with a different hash; run "
                    f"and baseline file names are immutable"
                )
            preserved = any(
                entry["field"] == "child_indexes" and entry["sha256"] == existing
                for entry in referenced_files(
                    json.loads(source.read_text(encoding="ascii"))
                )
            )
            if not preserved:
                raise AssertionError(
                    f"{destination} already exists with a different hash; an "
                    f"advancing index must enumerate the published one "
                    f"({existing[:12]}) as an immutable run-id-named child"
                )
        _ = shutil.copyfile(source, destination)
    return report_dir / index_path.name


def archive_published_index(index_name, output_dir, report_dir=None):
    """Preserve a published index as an immutable child before it advances.

    A family index may be re-executed, but the evidence it replaces may not
    vanish. The currently published index is copied into the new run's
    artifact directory under a name derived from its own run id and content,
    so the advancing index can enumerate it as a child and publish it
    alongside itself. Returns the child entry, or None when nothing was
    published yet or the published index is already identical.
    """
    published = resolve_report_dir(report_dir) / safe_json_name(index_name)
    if not published.is_file():
        return None
    document = json.loads(published.read_text(encoding="ascii"))
    digest = file_digest(published)
    child = Path(output_dir) / safe_json_name(
        f"{document.get('run_id', published.stem)}-{digest[:12]}.json"
    )
    if child.is_file() and file_digest(child) == digest:
        return file_entry(child)
    _ = shutil.copyfile(published, child)
    return file_entry(child)


# git accepts long argument lists, but a bounded batch keeps the command
# reproducible and its failure attributable to a known set of files.
STAGE_BATCH = 100


def stage_run_files(index_path) -> None:
    """Stage one published evidence tree by exact file name.

    The tree is read from the report directory, every child is validated as a
    plain JSON file name in that directory, hashes are checked, and the files
    are handed to `git add` by name. Nothing is staged by glob or directory,
    so a commit can never pick up a file the index did not enumerate.
    """
    index_path = Path(index_path).resolve()
    # git runs from the repository root, so every path is made absolute
    # rather than left relative to whichever directory the caller invoked
    # the command from.
    paths = [str(path.resolve()) for _name, path in enumerate_tree(index_path)]
    for start in range(0, len(paths), STAGE_BATCH):
        batch = paths[start:start + STAGE_BATCH]
        outcome = subprocess.run(
            ["git", "add", "--", *batch],
            check=False,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        if outcome.returncode != 0:
            raise AssertionError(
                f"git add failed for {batch}: {outcome.stderr.strip()}"
            )
