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
import concurrent.futures
import dataclasses
import datetime
import fcntl
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
    from . import host_monitor
    from . import test_e2e
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import host_monitor
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
    "_bytes_per_record",
    "_bytes_per_input_record",
    "_ns_per_record",
    "_per_s_per_core",
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


# The single numerical boundary of the Controller baseline policy. It lives
# here alone so that no task can quietly choose a kinder one.
REGRESSION_LIMIT = 0.25

# The Controller environment policy's floor for a publishable measurement.
MINIMUM_PHYSICAL_CORES = 8

# Exporter gauges a measured sample must contain for every worker. A missing
# or non-numeric gauge is an error, never a zero.
REQUIRED_EXPORTER_GAUGES = (
    "block.active",
    "block.flushing",
    "block.pending",
    "block.requests_pending",
    "block.pending_slot_occupied",
    "notify.queued",
    "notify.token_size",
    "series_cache.entries",
    "memory.accounted",
    "memory.budget",
    "oldest_unacked.age",
)

# Gauges that must all read zero before a worker counts as drained.
EXPORTER_EMPTY_GAUGES = (
    "block.active",
    "block.flushing",
    "block.pending",
    "block.requests_pending",
    "notify.queued",
)

# The buffered topology's additional emptiness evidence, named as a running
# buffer publishes it. `items.queued` is partitioned by signal, so its value
# for one buffer instance is the sum over that instance's signal labels;
# `in.flight` is a single gauge.
BUFFER_EMPTY_GAUGES = (
    ("processor.durable_buffer.items", "queued"),
    ("processor.durable_buffer", "in.flight"),
)

# How many advances of every worker's collection-updated uptime the drain
# proof requires, each with empty state.
DRAIN_EMPTY_EPOCHS = 3

# The gauge that says whether a worker answered the collection a snapshot was
# built from. `worker.rs::sample_metrics` computes it from configuration
# constants, so it is strictly positive whenever the worker sampled itself;
# a worker that did not answer reports every gauge as zero, which is
# indistinguishable from a drained worker unless this marker is checked.
LIVENESS_GAUGE = "memory.budget"

# A run file name is a plain file name in the report directory. Nothing else
# may be staged or published by name.
SAFE_JSON_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.json$")

# Commands whose presence means somebody is compiling or building an image
# while a measurement runs.
BUILD_COMMANDS = ("cargo", "rustc", "cc1", "cc1plus", "ld", "buildx")


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
    # Whether series identity varies per request (one logs series and one
    # slot per request) or per record, cycling through `series` slots.
    series_scope: str = "request"

    def __post_init__(self):
        for name in ("requests", "records_per_request", "series", "metrics_every"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.series_scope not in SERIES_SCOPES:
            raise ValueError(f"series_scope must be one of {SERIES_SCOPES}")
        if self.body_bytes < ID_FIXED_WIDTH + len(LOG_KIND):
            raise ValueError(
                f"body_bytes must hold the stable id: "
                f"{self.body_bytes} < {ID_FIXED_WIDTH + len(LOG_KIND)}"
            )

    def signal_of(self, request_index: int) -> str:
        """Which signal request `request_index` carries."""
        return "metrics" if request_index % self.metrics_every == 0 else "logs"

    def slot(self, request_index: int, point: int) -> int:
        """The series slot of one record under `series_scope`."""
        if self.series_scope == "record":
            return (request_index * self.records_per_request + point) % self.series
        return request_index % self.series

    def as_json(self) -> dict:
        """The workload as a plain dictionary for a result file.

        A field at its default in `OPTIONAL_WORKLOAD_FIELDS` is omitted, so
        a workload that predates it keeps its recorded form and fingerprint.
        """
        fields = dataclasses.asdict(self)
        for name, default in OPTIONAL_WORKLOAD_FIELDS.items():
            if fields[name] == default:
                del fields[name]
        return fields


# The per-request and per-record series identities a workload may use.
SERIES_SCOPES = ("request", "record")

# Workload fields added after results were published, with the default an
# older result implies by omitting them.
OPTIONAL_WORKLOAD_FIELDS = {"series_scope": "request"}


# The topologies a run may declare. `strict` and `buffered` are the two
# measured exporter pipelines; `noop` is the receiver-to-noop-exporter
# pipeline baseline of the layered benchmarks, and `stage` is an
# engine-less run of a prebuilt benchmark executable.
TOPOLOGIES = ("strict", "buffered", "noop", "stage")


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
        if self.topology not in TOPOLOGIES:
            raise ValueError(
                f"topology must be one of {TOPOLOGIES}: {self.topology}"
            )
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
        "slot": workload.slot(request_index, point),
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


def logger_name(workload: Workload, request_index: int, point: int = 0) -> str:
    """The series-identifying attribute of one logs record.

    `logger.name` is the configured logs series attribute, so varying it by
    request, or by record under the record scope, is what creates distinct
    series. The record id is deliberately not part of it: putting an id into
    series identity would make every record its own series and measure
    nothing the deployment does.
    """
    return f"series.logger.{workload.slot(request_index, point):06d}"


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
        for point in range(workload.records_per_request):
            logger = logger_name(workload, request_index, point)
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
        # Producer threads record their own attempts. Every use of the
        # connection holds `self.lock`, so sharing it across threads is safe.
        self.connection = sqlite3.connect(
            str(self.path), isolation_level=None, check_same_thread=False
        )
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


# Kernel core lists are parsed by the monitor's own function, so the
# in-process checks and the monitor process read them identically.
parse_core_list = host_monitor.parse_core_list


def available_physical_cores(sibling_groups, available) -> int:
    """How many physical cores a run may use, from its allowed logical cores.

    A physical core is one kernel thread-sibling group; it counts once if the
    run may use any of its logical cores. Two SMT siblings of one core are
    therefore one physical core, and a core outside the effective affinity or
    cgroup cpuset does not count at all, however many the machine has.
    """
    allowed = set(available)
    return sum(1 for group in sibling_groups if allowed & set(group))


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


# `/proc/PID/task/TID/stat` field 39 is the CPU the thread last ran on. The
# fields after the parenthesised command start at field 3.
STAT_LAST_CPU_INDEX = 39 - 3


def thread_last_cpu(pid: int, tid: int):
    """The CPU one thread last ran on, or None when it cannot be read."""
    try:
        stat = Path(f"/proc/{pid}/task/{tid}/stat").read_text()
        return int(stat.rsplit(") ", 1)[1].split()[STAT_LAST_CPU_INDEX])
    except (OSError, IndexError, ValueError):
        return None


def thread_affinity(pid, role, expected_cores):
    """Every thread of one process, with its allowed and last-run cores."""
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
                    "nspid": None,
                    "cpus_allowed_list": None,
                    "last_cpu": None,
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
                # The PID in each nested namespace, outermost first: a
                # container launcher's engine has a host PID and its own.
                "nspid": fields.get("NSpid"),
                "cpus_allowed_list": fields.get("Cpus_allowed_list"),
                "last_cpu": thread_last_cpu(pid, tid),
                "expected_cores": list(expected_cores or []),
                "observed_utc": utc_now(),
            }
        )
    return observed


def worker_thread_name(group_id, pipeline_id, core_id, generation) -> str:
    """The thread name the controller gives one pipeline worker."""
    return f"pipeline-{group_id}-{pipeline_id}-core-{core_id}-gen-{generation}"


# Linux truncates a thread's `comm` to fifteen characters plus a terminator.
# Every worker of the default group therefore reads `pipeline-defaul`, so the
# name narrows the candidates but never identifies a worker on its own.
COMM_WIDTH = 15


def select_worker_threads(threads, workers, previous=None):
    """Map each expected worker to exactly one thread by core evidence.

    The truncated name selects the candidate threads; the worker's own core
    selects among them, through the CPU each candidate last ran on. A worker
    pinned as requested runs only on its core, so it is the one candidate
    that last ran there. A worker with no such candidate, with several, or
    whose thread another worker already claimed is an ambiguous mapping, and
    the caller must abort rather than measure an affinity it cannot
    attribute. A thread that last ran on the right core but may run
    elsewhere is still mapped, and its allowed set then fails the per-worker
    comparison in `assert_affinity`.

    `previous` maps a worker key to the TID an earlier snapshot of the same
    run mapped. Among several candidates on the core, that TID is the worker
    and the others are its runtime's blocking-pool threads, which take the
    worker's name and affinity; without it several candidates stay
    ambiguous.
    """
    previous = previous or {}
    by_comm = collections.defaultdict(list)
    for thread in threads:
        if thread.get("name"):
            by_comm[thread["name"]].append(thread)
    selected = {}
    ambiguous = []
    claimed = {}
    for worker in workers:
        full = worker_thread_name(
            worker["group_id"],
            worker["pipeline_id"],
            worker["core_id"],
            worker["generation"],
        )
        comm = full[:COMM_WIDTH]
        candidates = by_comm.get(comm, [])
        on_core = [
            thread for thread in candidates if thread.get("last_cpu") == worker["core_id"]
        ]
        helpers = []
        if len(on_core) > 1 and worker["key"] in previous:
            mapped = [thread for thread in on_core if thread["tid"] == previous[worker["key"]]]
            if mapped:
                helpers = [thread["tid"] for thread in on_core if thread is not mapped[0]]
                on_core = mapped
        evidence = {
            "worker": worker["key"],
            "thread_name": full,
            "comm": comm,
            "core_id": worker["core_id"],
            "candidates": [
                {"tid": thread["tid"], "last_cpu": thread.get("last_cpu")}
                for thread in candidates
            ],
        }
        if len(on_core) != 1:
            ambiguous.append(
                dict(
                    evidence,
                    reason=(
                        f"{len(on_core)} of {len(candidates)} threads named "
                        f"{comm!r} last ran on core {worker['core_id']}"
                    ),
                )
            )
            continue
        thread = on_core[0]
        if thread["tid"] in claimed:
            ambiguous.append(
                dict(
                    evidence,
                    reason=(
                        f"thread {thread['tid']} is already mapped to worker "
                        f"{claimed[thread['tid']]}"
                    ),
                )
            )
            continue
        claimed[thread["tid"]] = worker["key"]
        selected[worker["key"]] = dict(thread, core_id=worker["core_id"])
        if helpers:
            selected[worker["key"]]["same_named_on_core_tids"] = helpers
    return selected, ambiguous


def environment_snapshot(roles, *, workers=(), requested_cores=None, absent=None,
                        pinned=(), previous_workers=None) -> dict:
    """A start or end environment snapshot.

    `roles` maps a role name to a process id, or to `(pid, cores)`, or to
    `None` when the role has no process in this run. Every role without a
    process carries an explicit reason, because an absent observation can
    never support a passed measurement. `workers` are the identities the
    telemetry reported and `requested_cores` the core set the run asked for;
    each worker is compared with its own core, never with the process-wide
    list.

    `pinned` names roles that are not an engine and whose every thread must
    be confined to that role's cores -- a prebuilt benchmark executable, for
    instance. Each such thread is compared with the role's cores by exactly
    the same rule as a worker thread, so an engine-less run's affinity is
    asserted rather than merely recorded.

    `previous_workers` is the worker mapping of this run's start snapshot,
    for `select_worker_threads`.
    """
    if hasattr(roles, "pid") and hasattr(roles, "launcher"):
        # An engine stands for its own role, on its requested cores.
        roles = {"engine": (roles.pid, list(roles.cores or ()))}
    elif roles is None:
        roles = {"harness": os.getpid()}
    topology = core_topology()
    # The effective affinity of this process already reflects its cgroup
    # cpuset, so it is the set of cores a run started from here may use.
    available = sorted(os.sched_getaffinity(0))
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
        "available_cores": available,
        "available_physical_core_count": available_physical_cores(
            topology["sibling_groups"], available
        ),
        "ram_bytes": ram_bytes(),
        "kernel": platform.platform(),
        "load_average_1_5_15": load,
        "thread_affinity": threads,
        "absent_roles": missing,
    }
    pinned_threads = []
    for role in sorted(set(pinned)):
        expected = sorted({int(core) for core in (roles.get(role) or (None, ()))[1]})
        if not expected:
            raise AssertionError(
                f"pinned role {role} names no cores; a confinement that was "
                f"never requested cannot be asserted"
            )
        for thread in threads:
            if thread["role"] != role:
                continue
            pinned_threads.append(
                {
                    "key": f"{role}/tid{thread['tid']}",
                    "tid": thread["tid"],
                    "name": thread["name"],
                    "cpus_allowed_list": thread["cpus_allowed_list"],
                    "last_cpu": thread.get("last_cpu"),
                    "expected_cores": expected,
                }
            )
    if pinned_threads:
        snapshot["pinned_roles"] = sorted(set(pinned))
        snapshot["worker_threads"] = sorted(pinned_threads, key=lambda item: item["key"])
    if workers:
        selected, ambiguous = select_worker_threads(
            threads, workers, previous=previous_workers
        )
        snapshot["worker_threads"] = sorted(
            (
                dict(
                    {
                        "key": key,
                        "tid": thread["tid"],
                        "name": thread["name"],
                        "cpus_allowed_list": thread["cpus_allowed_list"],
                        "last_cpu": thread.get("last_cpu"),
                        "expected_cores": [thread["core_id"]],
                    },
                    **({"same_named_on_core_tids": thread["same_named_on_core_tids"]}
                       if "same_named_on_core_tids" in thread else {}),
                )
                for key, thread in selected.items()
            ),
            key=lambda item: item["key"],
        )
        snapshot["ambiguous_workers"] = ambiguous
        snapshot["worker_cores"] = sorted(worker["core_id"] for worker in workers)
        if requested_cores is not None:
            snapshot["requested_cores"] = sorted(requested_cores)
    return snapshot


def assert_affinity(snapshot) -> None:
    """Abort when an observed worker affinity is not the requested one.

    Three conditions abort: a worker that cannot be attributed to exactly
    one thread, a worker core set that is not the requested one, and a
    worker thread whose allowed set is anything other than its own core. An
    affinity that cannot be attributed to a known worker is not evidence
    that the worker was pinned.
    """
    ambiguous = snapshot.get("ambiguous_workers") or []
    if ambiguous:
        raise AssertionError(f"ambiguous worker thread mapping: {ambiguous}")
    if "requested_cores" in snapshot and snapshot["requested_cores"] != snapshot.get(
        "worker_cores"
    ):
        raise AssertionError(
            f"workers run on cores {snapshot.get('worker_cores')}, the run "
            f"requested {snapshot['requested_cores']}"
        )
    threads = snapshot.get("worker_threads", [])
    if "worker_cores" in snapshot and len(threads) != len(snapshot["worker_cores"]):
        raise AssertionError(
            f"worker affinity mismatch: {len(threads)} worker threads mapped "
            f"for {len(snapshot['worker_cores'])} workers"
        )
    observed = {
        thread["tid"]: (
            None
            if thread["cpus_allowed_list"] is None
            else set(parse_core_list(thread["cpus_allowed_list"]))
        )
        for thread in threads
    }
    expected = {thread["tid"]: set(thread["expected_cores"]) for thread in threads}
    try:
        check_worker_affinity(observed, expected)
    except AssertionError as error:
        raise AssertionError(f"worker affinity mismatch: {error}")


# The one per-worker affinity rule. The monitor process applies it on every
# tick and the snapshots apply it at each edge.
check_worker_affinity = host_monitor.check_worker_affinity


def worker_tids(snapshot) -> dict:
    """Each mapped worker's TID and the one core it must be allowed."""
    return {
        thread["tid"]: set(thread["expected_cores"])
        for thread in snapshot.get("worker_threads", [])
    }


observed_affinity = host_monitor.observed_affinity


# How many physical cores each role of a publishable measurement may use.
# The engine owns up to four; a one-worker run still reserves the other
# three, so nothing else is placed where a larger run would put a worker.
ENGINE_RESERVED_CORES = 4
ROLE_CORES = (("producer", 2), ("store", 1), ("reader", 1))

# The roles each case actually runs, beside the engine and its
# observability core. A case claims cores only for these: an engine case
# produces, stores and reads its output back, while a stages family sends
# through the producer for its pipeline baseline and uploads to the store,
# but runs no reader. The engine reservation is kept whole for both,
# because both launch the engine binary. An attribution family runs perf
# beside the engine, so it gives the profiler a physical core of its own
# and its producer one: the producer only sends prebuilt bytes. It claims
# no reader: the oracle reads back only after each engine has stopped,
# when nothing is being measured.
CASE_ROLES = {
    "engine": ROLE_CORES,
    "stages": (("producer", 2), ("store", 1)),
    "attribution": (("producer", 1), ("store", 1), ("profiler", 1)),
    # A fault case runs the engine in the fault rig's namespace. NGINX,
    # Toxiproxy and the probes share one physical core of their own; the
    # oracle reads back only after the engine has stopped, as for an
    # attribution, so the case fits eight physical cores.
    "faults": (("producer", 1), ("store", 1), ("fault_tools", 1)),
    # A memory pair writes to the local filesystem, so it runs no store.
    "memory": (("producer", 2), ("reader", 1)),
    # A memory pair on an object store gives up one producer core to it.
    "memory_store": (("producer", 1), ("store", 1), ("reader", 1)),
    # A capacity trial reads back only after its engine stopped, as an
    # attribution does, so a four-worker engine fits beside the producer and
    # the store; the local store keeps the same placement.
    "capacity": (("producer", 2), ("store", 1)),
}


def role_allocation(sibling_groups, available, engine_cores, *, roles,
                    strict=True) -> dict:
    """Place every role on its own physical cores, SMT siblings excluded.

    The controller runs its own observability pipeline on the first core
    the engine may use, so that physical core is never a worker's. The
    engine's workers take `engine_cores`, the rest of its four-core
    reservation follows, and then each role the case declares in `roles`
    -- a `CASE_ROLES` entry, as (role, physical cores) pairs -- takes whole
    physical cores of its own. Only the declared roles are claimed, and a
    declared set that does not fit still raises. A role is given the lowest logical
    core of each physical core it owns; the siblings of every owned core are
    given to nobody. Anything that cannot be placed this way is an error, so
    a run never silently shares a core between roles.

    `strict=False` is for a host too small to publish on: a role that finds
    no free physical core gets fewer, possibly none, and runs unpinned. Such
    a run fails `physical_cores_sufficient` and can never be a baseline.
    """
    allowed = sorted(set(int(core) for core in available))
    if not allowed:
        raise AssertionError("no cores are available to this run")
    group_of = {}
    for group in sibling_groups:
        for core in group:
            group_of[int(core)] = tuple(sorted(int(item) for item in group))
    observability = allowed[0]
    taken = {group_of.get(observability, (observability,))}
    allocation = {"engine_observability": [observability]}
    engine_cores = [int(core) for core in engine_cores]
    if len(engine_cores) > ENGINE_RESERVED_CORES:
        raise AssertionError(
            f"{len(engine_cores)} engine workers exceed the "
            f"{ENGINE_RESERVED_CORES}-core engine reservation"
        )
    for core in engine_cores:
        group = group_of.get(core, (core,))
        if core not in allowed:
            raise AssertionError(f"engine core {core} is not available to this run")
        if group in taken:
            raise AssertionError(
                f"engine core {core} shares physical core {list(group)} with "
                f"another role"
            )
        taken.add(group)
    allocation["engine"] = sorted(engine_cores)
    free = [
        group
        for group in sorted({group_of.get(core, (core,)) for core in allowed})
        if group not in taken and group[0] in allowed
    ]

    def claim(count, role):
        """Give `role` the lowest logical core of `count` free physical cores."""
        if len(free) < count and not strict:
            count = len(free)
        if len(free) < count:
            raise AssertionError(
                f"no physical core is left for {role}: the run needs one "
                f"physical core per role and SMT siblings are never shared"
            )
        owned = [free.pop(0) for _ in range(count)]
        allocation[role] = sorted(
            min(core for core in group if core in allowed) for group in owned
        )

    reserved = ENGINE_RESERVED_CORES - len(engine_cores)
    if reserved:
        claim(reserved, "engine_reserved")
    for role, count in roles:
        claim(count, role)
    return allocation


def run_pinned(cores, function, *args, **kwargs):
    """Run `function` in a thread confined to `cores` and return its result.

    Only that thread and the threads it starts inherit the confinement, so
    the harness's own view of the cores available to the run is unchanged.
    """
    outcome = {}

    def body():
        """Pin, then call."""
        try:
            os.sched_setaffinity(0, set(cores))
            outcome["value"] = function(*args, **kwargs)
        except BaseException as error:
            outcome["error"] = error

    thread = threading.Thread(target=body, name="pinned-role")
    thread.start()
    thread.join()
    if "error" in outcome:
        raise outcome["error"]
    return outcome.get("value")


def environment_match(start, end) -> dict:
    """Whether the machine the run ended on is the one it started on."""
    keys = (
        "cpu_model",
        "logical_core_count",
        "physical_core_count",
        "sibling_groups",
        "available_cores",
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

    The watching runs in `host_monitor.py`, a separate process with no
    dependencies, so that a harness busy producing load cannot stretch the
    time between two observations. Every tick reads the comm, parent and
    start time of every process in `proc_root`, and the command line of any
    candidate. A compiler, linker or container build client is a build. A
    build daemon such as `buildkitd` is not a build while it is idle -- a
    builder container may stay up for days -- but any process descending
    from it is a build step. Detection is sticky: a build seen after
    preflight invalidates the run even if it has finished by the end.

    Ticks are scheduled from their own start times, at most `interval_s`
    apart; the largest gap actually observed is reported, because a gap is a
    window in which a build could have run unobserved. Worker threads
    registered with `watch` are checked against their expected affinity on
    every tick as well. At stop, Docker's own event log for the run interval
    is read for builder containers that started while the run was going.

    The monitor never stops or signals anything: another user's build is
    their business, and the only consequence here is that this run's
    performance numbers are invalid. Evidence of what was seen is preserved.
    """

    # The coverage a publishable run needs: no two ticks further apart.
    COVERAGE_LIMIT_S = 0.1

    def __init__(self, *, interval_s=0.05, own_pids=(), proc_root="/proc",
                 docker=None):
        self.interval_s = interval_s
        self.own_pids = set(own_pids)
        self.proc_root = Path(proc_root)
        self.docker = docker
        self.observations = []
        self.scans = 0
        self.affinity_failures = []
        self.docker_report = {}
        self.visibility = {}
        self.invalid = threading.Event()
        self.started_unix = None
        self.child = None
        self.final = None
        self._reader = None
        self._started = threading.Event()
        self._done = threading.Event()
        self._acknowledged = threading.Event()
        self._lock = threading.Lock()

    def rules(self) -> dict:
        """The detection rules, read when used so that tests may patch them."""
        return {
            "build_commands": list(BUILD_COMMANDS),
            "build_daemons": list(BUILD_DAEMONS),
            "container_clients": list(DOCKER_CLIENTS),
        }

    def scan(self):
        """One in-process pass over procfs with the monitor's own rules."""
        found = host_monitor.scan(
            self.proc_root, self.rules(), self.own_pids | {os.getpid()}
        )
        with self._lock:
            self.scans += 1
            self.observations.extend(found)
        if found:
            self.invalid.set()
        return found

    def _send(self, message):
        """Write one command to the monitor process."""
        if self.child is None or self.child.poll() is not None:
            raise AssertionError("the monitor process is not running")
        self.child.stdin.write(json.dumps(message, sort_keys=True) + "\n")
        self.child.stdin.flush()

    def _read(self):
        """Collect the monitor process's lines until it ends."""
        for line in self.child.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            kind = message.get("type")
            if kind == "started":
                self._started.set()
            elif kind == "build":
                with self._lock:
                    self.observations.append(message["entry"])
                self.invalid.set()
            elif kind == "affinity":
                with self._lock:
                    self.affinity_failures.append(message["entry"])
                self.invalid.set()
            elif kind in ("watching", "unwatched"):
                self._acknowledged.set()
            elif kind == "report":
                self.final = message
        self._started.set()
        self._acknowledged.set()
        self._done.set()

    def _command(self, message):
        """Send one command and wait until the monitor has applied it.

        The monitor applies commands between ticks, so once this returns no
        later tick uses the old watch set: a worker that is about to be shut
        down deliberately is never observed missing.
        """
        self._acknowledged.clear()
        self._send(message)
        if not self._acknowledged.wait(30):
            raise AssertionError(f"the monitor did not apply {message['cmd']}")

    def watch(self, pid, expected, names=()):
        """Check `expected` ({tid: cores}) of process `pid` on every tick.

        With worker thread `names`, every tick enumerates all of the
        process's threads and requires the threads carrying those names to
        be exactly the expected TIDs, so an extra or replacing worker thread
        fails even if it exists for a single tick.
        """
        self._command(
            {
                "cmd": "watch",
                "pid": int(pid),
                "expected": {str(tid): sorted(cores) for tid, cores in expected.items()},
                "names": sorted(set(names)),
            }
        )

    def unwatch(self):
        """Stop checking worker affinity, before a deliberate stop or restart."""
        if self.child is not None and self.child.poll() is None:
            self._command({"cmd": "unwatch"})

    def start(self):
        """Check what can be observed, then start the monitor process."""
        self.started_unix = time.time()
        self.visibility = procfs_visibility(self.proc_root)
        self.docker_report = {"start": docker_builders(self.docker)}
        self.child = subprocess.Popen(
            [sys.executable, str(Path(host_monitor.__file__).resolve())],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            encoding="ascii",
            errors="replace",
        )
        self._reader = threading.Thread(target=self._read, name="monitor-reader")
        self._reader.daemon = True
        self._reader.start()
        self._send(
            {
                "interval_s": self.interval_s,
                "limit_s": self.COVERAGE_LIMIT_S,
                "proc_root": str(self.proc_root),
                "rules": self.rules(),
                "own_pids": sorted(self.own_pids | {os.getpid()}),
            }
        )
        if not self._started.wait(30) or self.child.poll() is not None:
            self._abandon()
            raise AssertionError("the monitor process did not start")
        return self

    def _abandon(self):
        """End a monitor process that did not start or did not stop."""
        if self.child is not None and self.child.poll() is None:
            self.child.kill()
            _ = self.child.wait(10)

    def coverage(self) -> dict:
        """How continuously the ticks covered the run."""
        final = self.final or {}
        return {
            "interval_s": self.interval_s,
            "limit_s": self.COVERAGE_LIMIT_S,
            "ticks": final.get("ticks", 0),
            "max_gap_s": final.get("max_gap_s"),
            "gaps_over_limit_count": final.get("gaps_over_limit_count", 0),
            "gaps_over_limit": final.get("gaps_over_limit", []),
        }

    def stop(self) -> dict:
        """Stop the monitor process and report everything seen."""
        if self.child is not None:
            try:
                self._send({"cmd": "stop"})
            except (AssertionError, OSError):
                pass
            try:
                _ = self.child.wait(30)
            except subprocess.TimeoutExpired:
                self._abandon()
                raise AssertionError("the monitor process did not stop")
            self.child.stdin.close()
            if not self._done.wait(30):
                raise AssertionError("the monitor output was not read to its end")
            self.child.stdout.close()
            if self.final is None:
                raise AssertionError(
                    f"the monitor process ended with status "
                    f"{self.child.returncode} and no report"
                )
            self.child = None
        docker = dict(self.docker_report)
        docker["end"] = docker_builders(self.docker)
        docker["events"] = docker_build_events(
            self.docker, self.started_unix, time.time()
        )
        self.docker_report = docker
        if docker["events"].get("builder_starts"):
            self.invalid.set()
        coverage = self.coverage()
        final = self.final or {}
        with self._lock:
            self.scans += final.get("ticks", 0)
            observations = list(self.observations)
            return {
                "scans": self.scans,
                "detected": bool(observations)
                or bool(docker["events"].get("builder_starts")),
                "observations": observations[:50],
                "observation_count": len(observations),
                "coverage": coverage,
                "visibility": self.visibility,
                "docker": docker,
                "affinity_failures": [
                    {"observed_utc": entry.get("observed_unix"), "detail": entry["detail"]}
                    for entry in self.affinity_failures[:50]
                ],
                "affinity_failure_count": len(self.affinity_failures),
            }

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.report = self.stop()


# A daemon whose presence alone is not a build. Anything descending from it
# is a build step.
BUILD_DAEMONS = ("buildkitd",)

# Clients whose command line decides whether they are building an image.
DOCKER_CLIENTS = ("docker", "podman", "docker-buildx", "buildctl", "nerdctl")

_is_docker_build = host_monitor.is_container_build


def procfs_visibility(proc_root="/proc") -> dict:
    """Whether this process can see every other process in procfs.

    A procfs mounted with `hidepid` hides other users' processes, and a
    build this monitor cannot see is a build it cannot report. PID 1 is
    readable exactly when other processes are visible.
    """
    report = {"proc_root": str(proc_root), "hidepid": None}
    try:
        for line in Path("/proc/self/mountinfo").read_text().splitlines():
            parts = line.split(" - ", 1)
            if len(parts) == 2 and parts[1].startswith("proc "):
                options = parts[1].split()[-1]
                for option in options.split(","):
                    if option.startswith("hidepid="):
                        report["hidepid"] = option.split("=", 1)[1]
    except OSError as error:
        report["mountinfo_error"] = str(error)
    try:
        _ = (Path(proc_root) / "1" / "comm").read_text()
        report["other_processes_visible"] = True
    except OSError:
        report["other_processes_visible"] = False
    report["complete"] = bool(report["other_processes_visible"]) and report[
        "hidepid"
    ] in (None, "0", "off")
    return report


def _docker(arguments, timeout=10):
    """Run one Docker CLI query, returning (ok, stdout or error)."""
    try:
        done = subprocess.run(
            ["docker", *arguments], capture_output=True, text=True, timeout=timeout
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return False, str(error)
    if done.returncode != 0:
        return False, done.stderr.strip()[:500]
    return True, done.stdout


def docker_builders(docker=None) -> dict:
    """The builder containers Docker is running now, or why it cannot say.

    `docker` None means: use Docker when its CLI is installed. A host with
    no Docker CLI has no Docker build namespace to watch; a host whose CLI
    cannot reach its daemon has one that is unobservable.
    """
    if docker is False or (docker is None and shutil.which("docker") is None):
        return {"available": False, "observable": True, "reason": "no docker CLI"}
    ok, output = _docker(
        ["ps", "--no-trunc", "--format", "{{.ID}} {{.Image}} {{.Names}}"]
    )
    if not ok:
        return {"available": True, "observable": False, "reason": output}
    builders = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 3 and _is_builder(parts[1], parts[2]):
            builders.append({"id": parts[0][:12], "image": parts[1], "name": parts[2]})
    return {"available": True, "observable": True, "builders": builders}


def _is_builder(image: str, name: str) -> bool:
    """Whether a container is an image builder."""
    return "buildkit" in image or name.startswith("buildx_buildkit")


def docker_build_events(docker, since_unix, until_unix) -> dict:
    """Builder containers Docker started during the run interval.

    An already running builder is an idle daemon and is only recorded; one
    that started inside the interval was started for a build.
    """
    if docker is False or (docker is None and shutil.which("docker") is None):
        return {"observable": True, "reason": "no docker CLI", "builder_starts": []}
    if since_unix is None:
        return {"observable": False, "reason": "the monitor never started",
                "builder_starts": []}
    ok, output = _docker(
        [
            "events",
            "--since", f"{since_unix:.3f}",
            "--until", f"{max(until_unix, since_unix):.3f}",
            "--filter", "type=container",
            "--filter", "event=start",
            "--format", "{{json .}}",
        ],
        timeout=20,
    )
    if not ok:
        return {"observable": False, "reason": output, "builder_starts": []}
    starts = []
    count = 0
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        count += 1
        attributes = (event.get("Actor") or {}).get("Attributes") or {}
        image = attributes.get("image", event.get("from", ""))
        name = attributes.get("name", "")
        if _is_builder(image, name):
            starts.append({"image": image, "name": name, "time": event.get("time")})
    return {
        "observable": True,
        "container_start_count": count,
        "builder_starts": starts,
    }


def build_activity(proc_root="/proc") -> list:
    """The build processes running on this host right now.

    One in-process scan with the rules the continuous monitor applies, for a
    preflight that must refuse to start next to a build.
    """
    return BuildMonitor(proc_root=proc_root, docker=False).scan()


# The one host-visible lease every launcher, checkout and container shares,
# so that two measurements on one physical host can never overlap.
DEFAULT_LEASE_PATH = "/tmp/series-parquet-host-measurement.lock"


def process_start_ticks(pid):
    """A process's start time in clock ticks since boot, or None.

    With the PID it identifies one process even after the PID is reused.
    """
    try:
        return int(Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()[19])
    except (OSError, IndexError, ValueError):
        return None


class HostLease:
    """An exclusive host measurement lease held for a whole run.

    The lease is an exclusive `fcntl.flock` on an open descriptor of a fixed
    lock file, held from `acquire` to `release`. The kernel releases the
    lock when the holding process exits however it exits, so a crashed run
    frees the machine without any reclamation step. The lock file itself is
    never removed: unlinking a locked file would let a second process create
    a fresh file of the same name and lock that, and both would believe they
    held the lease.
    """

    def __init__(self, path=None, *, owner=None, run_id=None):
        self.path = Path(
            path or os.environ.get("SERIES_MEASURE_LEASE", DEFAULT_LEASE_PATH)
        )
        self.owner = owner or f"{os.getpid()}-{uuid.uuid4().hex}"
        self.run_id = run_id
        self.acquired_utc = None
        self.released_utc = None
        self._fd = None

    def acquire(self, *, deadline_ns=None):
        """Take the lease, waiting under a monotonic deadline for a holder."""
        if self._fd is not None:
            raise AssertionError(f"the lease {self.path} is already held by this run")
        deadline_ns = deadline_ns or (time.monotonic_ns() + 60 * 10**9)
        fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR, 0o644)
        wake = threading.Event()
        try:
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = (deadline_ns - time.monotonic_ns()) / 1e9
                    if remaining <= 0:
                        raise AssertionError(
                            f"another measurement holds {self.path}: "
                            f"{self._read_holder()}"
                        )
                    _ = wake.wait(min(0.2, remaining))
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        self.acquired_utc = utc_now()
        # The holder record is diagnostic only; the lock is the lease.
        record = json.dumps(
            {
                "owner": self.owner,
                "pid": os.getpid(),
                "process_start_ticks": process_start_ticks(os.getpid()),
                "run_id": self.run_id,
                "acquired_utc": self.acquired_utc,
            },
            sort_keys=True,
        ).encode("ascii")
        os.ftruncate(fd, 0)
        _ = os.pwrite(fd, record + b"\n", 0)
        os.fsync(fd)
        return self

    def _read_holder(self):
        """Whatever the lock file's diagnostic record claims, or None."""
        try:
            return json.loads(self.path.read_text(encoding="ascii"))
        except (OSError, ValueError):
            return None

    def assert_held(self):
        """Fail unless this run still holds the lock on the lease file.

        The descriptor must still be open and must still be the file at the
        lease path: a lock on a file that was replaced or removed excludes
        nobody.
        """
        if self._fd is None:
            raise AssertionError(f"the host measurement lease {self.path} is not held")
        try:
            held = os.fstat(self._fd)
            current = os.stat(self.path)
        except OSError as error:
            raise AssertionError(f"the host measurement lease was lost: {error}")
        if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
            raise AssertionError(
                f"the lease file {self.path} was replaced while this run held it"
            )

    @property
    def held(self) -> bool:
        """Whether this object currently owns an acquired lock descriptor."""
        return self._fd is not None

    def release(self):
        """Give the lease back by unlocking and closing the descriptor.

        Idempotent: releasing a lease that is not held does nothing, so every
        cleanup path may call it without knowing how far acquisition got.
        """
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None
        self.released_utc = utc_now()

    def as_json(self) -> dict:
        """The lease identity and lifetime for a result file."""
        return {
            "path": str(self.path),
            "owner": self.owner,
            "pid": os.getpid(),
            "run_id": self.run_id,
            "acquired_utc": self.acquired_utc,
            "released_utc": self.released_utc,
        }

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *exc):
        self.release()


# The plan's name for the lease. It is the same object: one lease, one lock.
MeasurementLease = HostLease


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


def require_signal_gauge(worker, metric_set, name):
    """One gauge of one entity, summed over its signal partitions.

    A gauge published once per signal describes one instance in several
    parts, so the parts are added -- but only within one entity. A gauge two
    entities publish is ambiguous, a part that is not a number is an error,
    and a gauge that is absent in both forms is absent, never zero.
    """
    slot = f"{metric_set}:{name}"
    if slot in worker["metrics"]:
        return require_gauge(worker, metric_set, name)
    entities = worker["labelled"].get(slot)
    if not entities:
        raise AssertionError(f"worker {worker['key']} publishes no {slot}")
    if len(entities) != 1:
        raise AssertionError(
            f"worker {worker['key']} publishes {slot} for "
            f"{sorted(entities)}; the sample cannot attribute it"
        )
    parts = next(iter(entities.values()))
    total = 0
    for label, value in sorted(parts.items()):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise AssertionError(
                f"worker {worker['key']} reports {slot}[{label}] as {value!r}"
            )
        total += value
    return total


def sample_document(document, *, expected_workers: int, buffered=False) -> dict:
    """The strict per-worker part of one sample, from one JSON response."""
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
            # The heap the worker's pipeline holds now, as the engine's own
            # allocation tracking counts it: every node, not only the
            # exporter's accounted share.
            "pipeline_memory_usage_bytes": require_gauge(
                worker, "pipeline", "memory.usage"
            ),
            "group_id": worker["group_id"],
            "pipeline_id": worker["pipeline_id"],
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
                f"{metric_set}:{name}": require_signal_gauge(worker, metric_set, name)
                for metric_set, name in BUFFER_EMPTY_GAUGES
            }
        workers[key] = entry
    return {"workers": workers, "process": parsed["process"]}


def worker_identities(sample) -> list:
    """The worker identities one sample names, as the affinity map needs."""
    return [
        {
            "key": worker["key"],
            "group_id": worker["group_id"],
            "pipeline_id": worker["pipeline_id"],
            "core_id": worker["core_id"],
            "generation": worker["generation"],
        }
        for _key, worker in sorted(sample["workers"].items())
    ]


def sample_engine(engine, *, expected_workers: int, buffered=False) -> dict:
    """One timestamped strict sample of every worker and of the process.

    The engine's own JSON telemetry is read with zeroes retained, so an
    absent gauge is distinguishable from a zero one. Process values such as
    resident memory are kept apart from per-worker values and are never
    summed across workers. The process is the engine's host PID, which the
    launcher reports, never a wrapper's.
    """
    monotonic_ns = time.monotonic_ns()
    document = test_e2e.engine_metrics(engine)
    parsed = sample_document(
        document, expected_workers=expected_workers, buffered=buffered
    )
    pid = getattr(engine, "pid", None) or engine.process.pid
    return {
        "monotonic_ns": monotonic_ns,
        "observed_utc": utc_now(),
        "scrape_timestamp": document.get("timestamp"),
        "workers": parsed["workers"],
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
    A nonempty epoch resets that worker's streak, and so does an epoch the
    worker did not answer, because an unobserved epoch may have been busy. A
    deadline that expires without the streaks is a failure, never a pass.
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
                # collection. Its state during that epoch is unobserved, and
                # an unobserved epoch may have been busy, so the empty
                # epochs either side of it are not consecutive: the streak
                # starts again.
                unanswered[key] += 1
                streak[key] = 0
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
# Producer, background sampler, residual and build provenance
# --------------------------------------------------------------------------

EXPORT_METHODS = {
    "logs": (
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        test_e2e.logs_pb.ExportLogsServiceResponse,
        "rejected_log_records",
    ),
    "metrics": (
        "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
        test_e2e.metrics_pb.ExportMetricsServiceResponse,
        "rejected_data_points",
    ),
}


class Producer:
    """Send deterministic requests exactly once each and ledger every outcome.

    The wire bytes `build_request` produced are what is sent: the call
    passes them through without re-serializing, so the ledger's wire hash is
    the hash of the bytes on the wire. Sender threads are confined to the
    producer's cores. There is no retry: a healthy measurement requires
    multiplicity exactly one, and a failed attempt is recorded with its
    classification and left failed.
    """

    def __init__(self, channel, ledger, workload, *, cores, timeout_s, max_in_flight,
                 source=None):
        self.channel = channel
        self.ledger = ledger
        self.workload = workload
        # Where request bytes come from: `build_request` by default, or a
        # prebuilt input that returns exactly what it would, read by index.
        self.source = source or (lambda index: build_request(self.workload, index))
        self.cores = set(cores)
        self.timeout_s = timeout_s
        self.max_in_flight = max_in_flight
        self.calls = {
            signal: channel.unary_unary(
                method,
                request_serializer=None,
                response_deserializer=response.FromString,
            )
            for signal, (method, response, _field) in EXPORT_METHODS.items()
        }

    def _pin(self):
        """Confine one sender thread to the producer's cores."""
        if self.cores:
            os.sched_setaffinity(0, self.cores)

    def send_one(self, index):
        """Send request `index` once and record what happened."""
        signal, wire, rows = self.source(index)
        start = time.monotonic_ns()
        _ = self.ledger.add_request(index, signal, wire, rows, send_ns=start)
        try:
            response = self.calls[signal](wire, timeout=self.timeout_s)
        except test_e2e.grpc.RpcError as error:
            finish = time.monotonic_ns()
            code = error.code()
            outcome = (
                OUTCOME_RETRYABLE
                if code in test_e2e.RETRYABLE_CODES
                else OUTCOME_PERMANENT
            )
            self.ledger.attempt(index, 1, start, finish, outcome, str(code))
            return outcome
        except Exception as error:
            finish = time.monotonic_ns()
            self.ledger.attempt(
                index, 1, start, finish, OUTCOME_LOCAL, f"{type(error).__name__}"
            )
            return OUTCOME_LOCAL
        finish = time.monotonic_ns()
        rejected = getattr(
            response.partial_success, EXPORT_METHODS[signal][2], 0
        )
        if rejected:
            self.ledger.attempt(
                index, 1, start, finish, OUTCOME_PARTIAL, f"rejected={rejected}"
            )
            return OUTCOME_PARTIAL
        self.ledger.attempt(index, 1, start, finish, OUTCOME_ACK)
        self.ledger.ack(index, finish)
        return OUTCOME_ACK

    def send(self, indexes) -> dict:
        """Send every request in `indexes` with bounded concurrency."""
        indexes = list(indexes)
        workers = max(1, min(self.max_in_flight, len(indexes)))
        started = time.monotonic_ns()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers, initializer=self._pin,
            thread_name_prefix="producer",
        ) as pool:
            outcomes = collections.Counter(pool.map(self.send_one, indexes))
        return {
            "requests": len(indexes),
            "concurrency": workers,
            "started_ns": started,
            "finished_ns": time.monotonic_ns(),
            "outcomes": dict(outcomes),
        }


def compact_sample(sample) -> dict:
    """What a result keeps of one sample: every number, no raw label maps."""
    return {
        "monotonic_ns": sample["monotonic_ns"],
        "observed_utc": sample.get("observed_utc"),
        "process_pid": sample.get("process_pid"),
        "process_rss_bytes": sample.get("process_rss_bytes"),
        "procfs": sample.get("procfs"),
        "load_average_1_5_15": sample.get("load_average_1_5_15"),
        "workers": {
            key: {
                name: worker[name]
                for name in (
                    "core_id", "generation", "uptime_s", "reported", "gauges", "buffer",
                    "pipeline_memory_usage_bytes",
                )
                if name in worker
            }
            for key, worker in sorted(sample["workers"].items())
        },
    }


def load_average():
    """The host's 1, 5 and 15 minute load averages."""
    with open("/proc/loadavg", encoding="ascii") as handle:
        return [float(value) for value in handle.read().split()[:3]]


class Sampler:
    """Sample one engine's telemetry and procfs on a fixed period.

    Each sample carries the strict per-worker telemetry, the process's
    procfs figures and the host load, on one monotonic timeline. A sample
    that fails is recorded as an error with its time, never as a zero.
    """

    def __init__(self, engine, *, expected_workers, buffered, period_s=0.25,
                 controls=None, phase=""):
        self.engine = engine
        self.expected_workers = expected_workers
        self.buffered = buffered
        self.period_s = period_s
        self.controls = controls
        self.phase = phase
        self.samples = []
        self.errors = []
        self._stop = threading.Event()
        self._thread = None

    def once(self):
        """One sample, with the process figures of the same instant."""
        sample = sample_engine(
            self.engine, expected_workers=self.expected_workers, buffered=self.buffered
        )
        sample["procfs"] = procfs_process_sample(sample["process_pid"])
        sample["load_average_1_5_15"] = load_average()
        sample["phase"] = self.phase
        return sample

    def _run(self):
        """Sample until stopped."""
        while not self._stop.is_set():
            try:
                self.samples.append(self.once())
            except Exception as error:
                self.errors.append(
                    {
                        "monotonic_ns": time.monotonic_ns(),
                        "phase": self.phase,
                        "error": f"{type(error).__name__}: {error}"[:500],
                    }
                )
            _ = self._stop.wait(self.period_s)

    def start(self):
        """Begin sampling in the background."""
        self._thread = threading.Thread(target=self._run, name="sampler")
        self._thread.daemon = True
        self._thread.start()
        return self

    def stop(self):
        """Stop sampling and wait for the thread."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
            if self._thread.is_alive():
                raise AssertionError("the sampler thread did not stop")
            self._thread = None
        return self


def _memory_terms(sample) -> dict:
    """The measured memory terms of one sample, all in bytes."""
    procfs = sample["procfs"]
    workers = sample["workers"].values()
    return {
        "rss_bytes": procfs["smaps_rss_bytes"],
        "anonymous_bytes": procfs["smaps_anonymous_bytes"],
        "file_bytes": procfs["smaps_rss_bytes"] - procfs["smaps_anonymous_bytes"],
        "accounted_bytes": sum(
            worker["gauges"]["memory.accounted"] for worker in workers
        ),
        "heap_bytes": sum(worker["pipeline_memory_usage_bytes"] for worker in workers),
    }


def rss_residuals(samples, idle) -> list:
    """The signed unexplained RSS of each sample against one idle reference.

    Every term is measured, none is a reservation. `idle` is a sample taken
    at readiness before any input: its resident set is the runtime term. The
    growth of file-backed resident pages -- executable and library text
    faulted in as new code paths run, and any mapped file -- is read from
    `smaps_rollup` and is a runtime term too. The workers' heap, as the
    engine's own allocation tracking counts it, includes the exporter's
    accounted bytes; because an allocator keeps freed pages resident, the
    heap term is its high-water mark since readiness. What remains of the
    anonymous growth is the residual. It is signed: tracked heap that is not
    resident is a negative residual and is kept, never clamped to zero.
    """
    reference = _memory_terms(idle)
    residuals = []
    heap_high = reference["heap_bytes"]
    for sample in samples:
        workers = sample["workers"].values()
        if not workers or not all(worker_reported(worker) for worker in workers):
            continue
        if "smaps_rss_bytes" not in (sample.get("procfs") or {}):
            continue
        terms = _memory_terms(sample)
        heap_high = max(heap_high, terms["heap_bytes"])
        heap_growth = heap_high - reference["heap_bytes"]
        accounted_growth = terms["accounted_bytes"] - reference["accounted_bytes"]
        file_growth = terms["file_bytes"] - reference["file_bytes"]
        residuals.append(
            {
                "monotonic_ns": sample["monotonic_ns"],
                "rss_bytes": terms["rss_bytes"],
                "rss_growth_bytes": terms["rss_bytes"] - reference["rss_bytes"],
                "file_growth_bytes": file_growth,
                "accounted_growth_bytes": accounted_growth,
                "heap_high_water_growth_bytes": heap_growth,
                "residual_bytes": (terms["rss_bytes"] - reference["rss_bytes"])
                - file_growth
                - heap_growth,
            }
        )
    return residuals


# The plan's frozen diagnostic uncertainty for the RSS reconciliation.
RESIDUAL_FLOOR_BYTES = 32 * 1024 * 1024
RESIDUAL_PEAK_FRACTION = 0.10


def residual_check(residuals, peak_rss_bytes) -> dict:
    """Fail an unexplained positive, or a persistent negative, residual.

    The tolerance is `max(32MiB, 0.10 * peak_RSS)`, declared before the run.
    A positive residual beyond it in any sample fails; a negative one fails
    when it persists, that is in more than half of the samples.
    """
    tolerance = max(RESIDUAL_FLOOR_BYTES, RESIDUAL_PEAK_FRACTION * peak_rss_bytes)
    if not residuals:
        return check(
            "rss_reconciliation", CHECK_HARD, STATUS_FAILED,
            "no sample carried both RSS and every worker's accounted bytes",
        )
    positive = [entry for entry in residuals if entry["residual_bytes"] > tolerance]
    negative = [entry for entry in residuals if entry["residual_bytes"] < -tolerance]
    persistent = len(negative) * 2 > len(residuals)
    largest = max(entry["residual_bytes"] for entry in residuals)
    smallest = min(entry["residual_bytes"] for entry in residuals)
    return check(
        "rss_reconciliation",
        CHECK_HARD,
        STATUS_FAILED if positive or persistent else STATUS_PASSED,
        f"residual range [{smallest}, {largest}] bytes over {len(residuals)} "
        f"samples; tolerance {int(tolerance)} bytes; {len(positive)} positive "
        f"and {len(negative)} negative beyond it",
    )


def percentile(values, fraction):
    """The nearest-rank percentile of a non-empty list."""
    ordered = sorted(values)
    if not ordered:
        raise AssertionError("a percentile of no values is undefined")
    rank = max(1, int(-(-fraction * len(ordered) // 1)))
    return ordered[min(rank, len(ordered)) - 1]


def build_profile(binary) -> str:
    """The cargo profile a binary was built with, from its target directory.

    `debug` or `release`, or `custom:<directory>` for anything else. This is
    the one rule every release check applies; it runs no toolchain, so it is
    safe to call while a build monitor watches the host.
    """
    parent = Path(binary).parent.name
    return parent if parent in ("debug", "release") else "custom:" + parent


def engine_build(binary) -> dict:
    """The engine build a run used: profile, features, allocator, toolchain.

    The profile is read from the target directory the binary was built into.
    Cargo does not record the feature set inside the binary, so the features
    and allocator are the ones the build step declared through
    `SERIES_ENGINE_FEATURES` and `SERIES_ENGINE_ALLOCATOR`, defaulting to the
    documented `series-parquet,aws,durable-buffer` build on the workspace's
    default jemalloc allocator. The binary hash is provenance, outside the
    fingerprint.
    """
    binary = Path(binary)
    profile = build_profile(binary)
    try:
        toolchain = subprocess.run(
            ["rustc", "--version"], capture_output=True, text=True, timeout=30,
            cwd=str(test_e2e.WORKSPACE),
        ).stdout.strip() or "unknown"
    except (OSError, subprocess.TimeoutExpired):
        toolchain = "unknown"
    return {
        "profile": profile,
        "features": os.environ.get(
            "SERIES_ENGINE_FEATURES", "default,series-parquet,aws,durable-buffer"
        ),
        "allocator": os.environ.get("SERIES_ENGINE_ALLOCATOR", "jemalloc"),
        "toolchain": toolchain,
        "binary": str(binary),
        "binary_sha256": file_digest(binary),
        "binary_size_bytes": binary.stat().st_size,
    }


def git_provenance() -> dict:
    """The source revision a run measured, and whether the tree was dirty."""
    def git(*arguments):
        """One git query from the repository root."""
        done = subprocess.run(
            ["git", *arguments], capture_output=True, text=True, timeout=30,
            cwd=str(REPO_ROOT),
        )
        return done.stdout.strip() if done.returncode == 0 else None

    status = git("status", "--porcelain", "--untracked-files=no")
    return {"revision": git("rev-parse", "HEAD"), "dirty": bool(status)}


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
    """Stream reader rows into the ledger's `actual` table in batches.

    The load is one transaction. The ledger runs in autocommit mode with
    full synchronisation, so without it every row would be its own
    committed transaction with its own fsync: free on a memory file system,
    minutes per million rows on a disk.
    """
    connection = ledger.connection
    with ledger.lock:
        _ = connection.execute("BEGIN IMMEDIATE")
        try:
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
            _ = connection.execute("COMMIT")
        except BaseException:
            _ = connection.execute("ROLLBACK")
            raise
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
            case, root, db, collect_bodies=False
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
    for name, direction in (result.get("metric_directions") or {}).items():
        if direction not in DIRECTIONS:
            raise AssertionError(f"metric {name} has unknown direction {direction!r}")
        if name not in result["metrics"]:
            raise AssertionError(f"direction declared for unreported metric {name}")
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


def write_json_atomic(path, document) -> Path:
    """Write one JSON document so that no reader ever sees a partial file.

    The document is encoded strictly -- sorted keys, ASCII, no non-finite
    numbers -- into a temporary file in the destination directory, flushed
    to disk and renamed over the destination in one step.
    """
    path = Path(path)
    encoded = (
        json.dumps(
            document, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    ).encode("ascii")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    with open(temporary, "wb") as handle:
        _ = handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    _ = temporary.replace(path)
    return path


# Keys whose string value is a credential. A published document keeps the key,
# so its shape is unchanged, and carries this token instead of the value. A
# bare `key` is deliberately not matched: the harness publishes worker, label
# and baseline identifiers under `key`, and none of them is a secret.
CREDENTIAL_KEY = re.compile(
    r"(?i)(secret|password|passwd|token|credential|api_?key|access_?key|"
    r"private_?key|secret_?key|key_?id|root_?user)"
)
REDACTED = "<redacted>"
REPO_TOKEN = "<repo>"
HOME_TOKEN = "<home>"
HOST_PATH_TOKEN = "<host-path>"
# `NAME=value` arguments whose value is a credential, as a container command
# line or an environment dump passes them.
CREDENTIAL_ASSIGNMENT = re.compile(
    r"\b([A-Z0-9_]*(?:PASSWORD|SECRET|TOKEN|ACCESS_KEY|SECRET_KEY|KEY_ID|ROOT_USER)"
    r"[A-Z0-9_]*)=([^\s\"',;]+)"
)
# An absolute path under a directory that names the host, its users or its
# scratch space. Everything up to the final component is replaced, so the
# file a path named stays recognisable and the value is stable across hosts.
#
# A path is taken where it starts a token, and also right after a URL's
# `scheme://`, so `file:///tmp/x` loses its host path too; a URL whose
# authority names a remote host (`http://host/tmp/x`) is left alone.
HOST_PATH = re.compile(
    r"(?:(?<=://)|(?<![A-Za-z0-9_.:/>-]))"
    r"/(?:home|Users|root|srv|tmp|var|opt|mnt|data|run|media)"
    r"(?=/|$|[\s\"',;])"
    r"(?:/[^\s\"',;:/]+)*/?"
)
# A container bind specification, `SOURCE:TARGET[:OPTIONS]`, as `docker run
# --volume` takes it. Both paths are scrubbed: HOST_PATH alone stops at the
# first colon, and the target of a bind mounted at its own host path is as
# much a host path as the source. (`--mount source=...,target=...` needs no
# rule of its own: each path follows `=`, where HOST_PATH already starts.)
# A source already reduced to a token (`<host-path>/x`) still matches, so
# scrubbing stays idempotent over evidence scrubbed by an earlier rule.
VOLUME_SPEC = re.compile(
    r"(?<![^\s\"'=,])((?:<[a-z_-]+>)?/[^\s\"',;:]*)((?::/[^\s\"',;:]*)+)(:[A-Za-z,]+)?"
    r"(?=$|[\s\"',;])"
)
# Word characters for the token boundary of a short credential value.
WORD_CHAR = r"A-Za-z0-9_"


def scrub_published(value, *, repo_root=None, home=None):
    """A copy of one document with host paths and credentials removed.

    Every document the harness writes for publication passes through here.
    The repository root becomes `<repo>` and the user's home directory
    `<home>` wherever either appears; any other absolute path under a host,
    user or scratch directory (`/home`, `/tmp`, `/var`, `/srv`, `/opt`,
    `/mnt`, `/data`, ...) keeps only its final component behind
    `<host-path>/`. A credential-named key keeps its key with a `<redacted>`
    value, a `NAME=value` credential assignment keeps its name, and every
    credential value found either way is then removed wherever else it
    appears in the document, a log tail or a command line included. The run
    directory token and the declared ephemeral tokens are applied earlier,
    for fingerprints; this is the last step before a file is written, so
    nothing a fingerprint hashed changes.
    """
    roots = []
    for root, token in (
        (repo_root if repo_root is not None else REPO_ROOT, REPO_TOKEN),
        (home if home is not None else os.path.expanduser("~"), HOME_TOKEN),
    ):
        text = str(root).rstrip(os.sep)
        if text and text != os.sep:
            roots.append((text, token))
            real = os.path.realpath(text)
            if real != text:
                roots.append((real, token))
    # Longest first, so the repository inside the home directory is named as
    # the repository.
    roots.sort(key=lambda pair: len(pair[0]), reverse=True)
    secrets = set()

    def collect(item, key=None):
        """Every credential value in the document, by key or assignment."""
        if isinstance(item, dict):
            for name, child in item.items():
                collect(child, name)
        elif isinstance(item, (list, tuple)):
            for child in item:
                collect(child)
        elif isinstance(item, str):
            if key is not None and CREDENTIAL_KEY.search(str(key)) and item:
                secrets.add(item)
            for match in CREDENTIAL_ASSIGNMENT.finditer(item):
                secrets.add(match.group(2))

    def host_path(match):
        """One host path, reduced to its final component."""
        path = match.group(0).rstrip("/")
        tail = path.rsplit("/", 1)[-1]
        return f"{HOST_PATH_TOKEN}/{tail}" if tail else HOST_PATH_TOKEN

    def text_of(item):
        """One string with every host path and credential replaced."""
        item = CREDENTIAL_ASSIGNMENT.sub(lambda m: f"{m.group(1)}={REDACTED}", item)
        for secret in sorted(secrets, key=len, reverse=True):
            if secret == REDACTED:
                continue
            if len(secret) >= 4:
                item = item.replace(secret, REDACTED)
            else:
                # A short value is removed only as a whole token, so a
                # one-letter user name is scrubbed from a log line without
                # shredding every word that contains the letter.
                item = re.sub(
                    rf"(?<![{WORD_CHAR}]){re.escape(secret)}(?![{WORD_CHAR}])",
                    REDACTED,
                    item,
                )
        for root, token in roots:
            item = item.replace(root, token)

        def volume(match):
            """A bind specification with every host path in it reduced."""
            paths = [match.group(1)] + match.group(2).split(":")[1:]
            options = match.group(3) or ""
            return ":".join(HOST_PATH.sub(host_path, path) for path in paths) + options

        item = VOLUME_SPEC.sub(volume, item)
        return HOST_PATH.sub(host_path, item)

    def rewrite(item, key=None):
        """Rewrite one node of the document."""
        if isinstance(item, dict):
            return {name: rewrite(child, name) for name, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [rewrite(child) for child in item]
        if isinstance(item, str):
            if key is not None and CREDENTIAL_KEY.search(str(key)) and item:
                return REDACTED
            return text_of(item)
        return item

    collect(value)
    return rewrite(value)


def write_published_json(path, document) -> Path:
    """Write one document meant for publication, scrubbed first."""
    return write_json_atomic(path, scrub_published(document))


def write_result(path, result) -> Path:
    """Write one result file atomically, after validating it.

    The written copy is scrubbed of host paths and credentials
    (`scrub_published`); the in-memory result the caller keeps is not.
    """
    validate_result(result)
    return write_published_json(path, result)


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


def machine_identity_sha256():
    """A hash of this machine's persistent identity, or None if unreadable.

    Only the hash is recorded. A machine whose identity cannot be read has
    no fingerprint, so its runs can be recorded but never compared.
    """
    for candidate in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            identity = Path(candidate).read_text(encoding="ascii").strip()
        except OSError:
            continue
        if identity:
            return hashlib.sha256(identity.encode("ascii")).hexdigest()
    return None


class RunControls:
    """The host controls every measured run holds from start to end.

    `open` takes the exclusive host lease and starts the build monitor
    before anything is measured. The experiment calls `snapshot("start",
    ...)` immediately before measured traffic and `snapshot("end", ...)`
    after its final drain, naming the process of each role, the workers the
    telemetry reported and the cores the run requested; every snapshot
    asserts worker affinity and aborts the run on a mismatch or an
    unattributable worker. Between them, `watch_workers` has the monitor
    re-check every mapped worker thread on each tick, and `checkpoint`
    re-verifies the mapping at a phase boundary such as a restart. `close`
    stops the monitor, checks the lease is still held, compares the two
    snapshots and records every outcome as a hard check, whatever happened
    in between. None of these steps is optional: a missing one leaves its
    hard check failed or absent, and the baseline evaluator rejects a run
    without the complete set.

    Processes that are not the engine -- a prebuilt benchmark, a producer, a
    store -- are registered with `register`, so an engine-less run still
    records their observed thread affinity in both snapshots.
    """

    EDGES = ("start", "end")

    def __init__(self, result, *, lease_path=None, lease_wait_s=0.0,
                 build_interval_s=0.05, proc_root="/proc", docker=None):
        self.result = result
        self.lease = HostLease(lease_path, run_id=result.get("run_id"))
        self.lease_wait_s = lease_wait_s
        self.monitor = BuildMonitor(
            interval_s=build_interval_s, proc_root=proc_root, docker=docker
        )
        self.roles = {}
        self.taken = {}
        self.affinity_failures = []
        self.opened = False
        self.closed = False
        # Whether a monitor tick gap over the coverage limit fails the
        # coverage gate. A host that is not a publishable measurement host
        # (the `publish=false` CI mode) records the gaps as an observation
        # instead: a loaded shared runner stalls a 50 ms sampler routinely,
        # and nothing it measures is published.
        self.coverage_gaps_hard = True

    def open(self):
        """Take the lease and start watching for builds, in that order.

        If the monitor cannot start, the lease just taken is released before
        the error propagates: a run that never began measuring must not keep
        the machine locked.
        """
        self.lease.acquire(
            deadline_ns=time.monotonic_ns() + int(self.lease_wait_s * 10**9)
        )
        try:
            _ = self.monitor.start()
        except BaseException:
            self.lease.release()
            raise
        self.opened = True
        environment = self.result["environment"]
        environment["machine_identity_sha256"] = machine_identity_sha256()
        environment["lease"] = self.lease.as_json()
        record_event(self.result, "host_controls_opened", self.lease.owner)
        return self

    def register(self, role, pid, cores=()):
        """Name a process of this run so both snapshots observe it."""
        self.roles[role] = (int(pid), [int(core) for core in cores])

    def allocate(self, allocation):
        """Record the run's role placement, which is part of its fingerprint.

        Two roles on one physical core would compete, so an allocation that
        places them on SMT siblings of one core is refused here.
        """
        groups = {}
        for group in core_topology()["sibling_groups"]:
            for core in group:
                groups[core] = tuple(group)
        owners = {}
        for role, cores in sorted(allocation.items()):
            for core in cores:
                group = groups.get(int(core), (int(core),))
                if group in owners and owners[group] != role:
                    raise AssertionError(
                        f"roles {owners[group]} and {role} share physical core "
                        f"{list(group)}"
                    )
                owners[group] = role
        self.result["environment"]["core_allocation"] = {
            role: sorted(int(core) for core in cores)
            for role, cores in sorted(allocation.items())
        }
        record_event(
            self.result, "core_allocation", json.dumps(allocation, sort_keys=True)
        )

    def _observe(self, edge, roles, workers, requested_cores, pinned=()):
        """One environment snapshot over every registered and named role."""
        merged = dict(self.roles)
        merged.update(roles or {})
        if "engine" in merged and not workers:
            raise AssertionError(
                f"the {edge} snapshot names no workers; a measured run must "
                f"attribute every worker thread"
            )
        if not workers and not pinned and "engine" not in merged:
            raise AssertionError(
                f"the {edge} snapshot asserts no affinity; an engine-less run "
                f"must name the pinned role it measured"
            )
        start = self.result["environment"].get("start") or {}
        previous = (
            {thread["key"]: thread["tid"] for thread in start.get("worker_threads", [])}
            if edge != "start" else None
        )
        return environment_snapshot(
            merged, workers=workers or (), requested_cores=requested_cores,
            pinned=pinned, previous_workers=previous,
        )

    def snapshot(self, edge, roles=None, *, workers=(), requested_cores=None,
                 pinned=()):
        """Record one edge's environment and assert worker affinity on it.

        `roles` maps each role to `(pid, cores)`. The start edge also fixes
        the run's normalized role placement, which is part of its
        fingerprint, unless `allocate` already recorded it.
        """
        if edge not in self.EDGES:
            raise ValueError(f"an environment snapshot edge is start or end: {edge}")
        if not self.opened:
            raise AssertionError("snapshot before the host controls were opened")
        snapshot = self._observe(edge, roles, workers, requested_cores, pinned)
        self.result["environment"][edge] = snapshot
        if edge == "start" and "core_allocation" not in self.result["environment"]:
            merged = dict(self.roles)
            merged.update(roles or {})
            self.result["environment"]["core_allocation"] = {
                role: sorted({int(core) for core in value[1]})
                for role, value in sorted(merged.items())
                if isinstance(value, tuple) and value[1]
            }
        try:
            assert_affinity(snapshot)
        except AssertionError as error:
            self.affinity_failures.append(f"{edge}: {error}")
            self.taken[edge] = snapshot
            raise
        self.taken[edge] = snapshot
        return snapshot

    def checkpoint(self, label, roles=None, *, workers=(), requested_cores=None,
                   pinned=()):
        """Re-verify worker affinity at a phase boundary, such as a restart."""
        if not self.opened:
            raise AssertionError("checkpoint before the host controls were opened")
        snapshot = self._observe(label, roles, workers, requested_cores, pinned)
        self.result["environment"].setdefault("checkpoints", {})[label] = snapshot
        try:
            assert_affinity(snapshot)
        except AssertionError as error:
            self.affinity_failures.append(f"{label}: {error}")
            raise
        return snapshot

    def watch_workers(self, pid, snapshot):
        """Have every monitor tick re-check the workers `snapshot` mapped.

        The worker thread names come from the mapped threads themselves, so
        any other thread by that name appearing later is an extra worker.
        """
        self.monitor.watch(
            pid,
            worker_tids(snapshot),
            names=[
                thread["name"]
                for thread in snapshot.get("worker_threads", [])
                if thread.get("name")
            ],
        )

    def watch_pinned(self, pid, cores):
        """Have every monitor tick re-check one pinned process's main thread.

        Only the main thread is watched: a runtime's blocking-pool threads
        come and go while the process runs, and they inherit the process's
        confinement, so requiring a fixed set of thread ids would fail on a
        thread that merely exited. Both snapshots still compare every thread
        the process had at that edge.
        """
        self.monitor.watch(int(pid), {int(pid): set(int(core) for core in cores)})

    def unwatch_workers(self):
        """Stop re-checking workers, before the engine is deliberately stopped."""
        self.monitor.unwatch()

    def raise_if_invalid(self):
        """Stop the run cleanly as soon as the monitor has invalidated it.

        Nothing is killed: the run stops itself, keeps every observation and
        fails its checks when the controls close.
        """
        if self.monitor.invalid.is_set():
            builds = [
                {key: entry[key] for key in ("pid", "comm", "reason")}
                for entry in self.monitor.observations[:5]
            ]
            raise AssertionError(
                f"the run was invalidated while measuring: builds={builds} "
                f"affinity={self.monitor.affinity_failures[:3]}"
            )

    def close(self):
        """Stop the controls and record each of their outcomes, once.

        The lease is released in `finally` whenever it is actually held,
        whatever else raised, and the release itself is idempotent. A build
        monitor that fails to stop leaves build activity unobservable, so it
        is recorded as a failed hard check rather than allowed to skip the
        remaining checks or the release.
        """
        if self.closed:
            return
        self.closed = True
        try:
            self._record_outcomes()
        finally:
            if self.lease.held:
                self.lease.release()
                self.result["environment"]["lease"] = self.lease.as_json()
            record_event(self.result, "host_controls_closed")

    def _record_outcomes(self):
        """Record every lifecycle check from what the controls observed."""
        environment = self.result["environment"]
        checks = self.result["checks"]
        if self.opened:
            try:
                report = self.monitor.stop()
            except Exception as error:
                report = None
                environment["build_monitor"] = {
                    "detected": None,
                    "error": f"{type(error).__name__}: {error}",
                }
                for name in ("no_concurrent_build", "build_monitor_coverage"):
                    checks.append(
                        check(
                            name,
                            CHECK_HARD,
                            STATUS_FAILED,
                            f"the build monitor failed to stop, so build "
                            f"activity is unobservable: {error}",
                        )
                    )
            if report is not None:
                environment["build_monitor"] = report
                checks.append(
                    check(
                        "no_concurrent_build",
                        CHECK_HARD,
                        STATUS_FAILED if report["detected"] else STATUS_PASSED,
                        f"{report['observation_count']} build processes seen in "
                        f"{report['scans']} scans; builder container starts "
                        f"{len(report['docker']['events'].get('builder_starts', []))}",
                    )
                )
                checks.append(
                    coverage_check(report, gaps_hard=self.coverage_gaps_hard)
                )
                self.affinity_failures.extend(
                    f"tick {entry['observed_utc']}: {entry['detail']}"
                    for entry in report["affinity_failures"]
                )
            try:
                self.lease.assert_held()
                lease_status, lease_detail = STATUS_PASSED, str(self.lease.path)
            except AssertionError as error:
                lease_status, lease_detail = STATUS_FAILED, str(error)
            checks.append(check("host_lease_held", CHECK_HARD, lease_status, lease_detail))
        else:
            checks.append(
                check(
                    "host_lease_held",
                    CHECK_HARD,
                    STATUS_FAILED,
                    "the host controls were never opened",
                )
            )
            for name in ("no_concurrent_build", "build_monitor_coverage"):
                checks.append(
                    check(
                        name,
                        CHECK_HARD,
                        STATUS_FAILED,
                        "no build monitor ran, so build activity is unobservable",
                    )
                )
        complete = all(edge in self.taken for edge in self.EDGES)
        checks.append(
            check(
                "environment_snapshots_complete",
                CHECK_HARD,
                STATUS_PASSED if complete else STATUS_FAILED,
                f"taken: {sorted(self.taken)}",
            )
        )
        if complete:
            match = environment_match(self.taken["start"], self.taken["end"])
            environment["match"] = match
            checks.append(
                check(
                    "environment_matched",
                    CHECK_HARD,
                    STATUS_PASSED if match["matched"] else STATUS_FAILED,
                    json.dumps(match["differences"], sort_keys=True),
                )
            )
        else:
            checks.append(
                check(
                    "environment_matched",
                    CHECK_HARD,
                    STATUS_FAILED,
                    "a start and an end snapshot are both required",
                )
            )
        if self.affinity_failures:
            affinity_status, affinity_detail = STATUS_FAILED, "; ".join(
                self.affinity_failures[:10]
            )
        elif complete:
            affinity_status, affinity_detail = STATUS_PASSED, "every worker on its own core"
        else:
            affinity_status, affinity_detail = (
                STATUS_FAILED,
                "worker affinity was not observed at both edges",
            )
        checks.append(check("affinity_matched", CHECK_HARD, affinity_status, affinity_detail))
        start = self.taken.get("start") or {}
        physical = start.get("available_physical_core_count")
        checks.append(
            check(
                "physical_cores_sufficient",
                CHECK_HARD,
                STATUS_PASSED
                if isinstance(physical, int) and physical >= MINIMUM_PHYSICAL_CORES
                else STATUS_FAILED,
                f"{physical} physical cores available to the run, "
                f"{MINIMUM_PHYSICAL_CORES} required; the machine has "
                f"{start.get('physical_core_count')}",
            )
        )


def coverage_check(report, *, gaps_hard=True) -> dict:
    """Whether the build monitor could have missed a build.

    It could if two ticks were further apart than the coverage limit, if
    procfs hides other processes, or if Docker is installed but could not
    be asked about builder containers. With `gaps_hard=False` tick gaps are
    reported in the detail as an observation and do not fail the check; the
    visibility and Docker conditions still do.
    """
    problems = []
    observed = []
    coverage = report["coverage"]
    if coverage["gaps_over_limit_count"]:
        gaps = (
            f"{coverage['gaps_over_limit_count']} tick gaps over "
            f"{coverage['limit_s']}s, largest {coverage['max_gap_s']}s"
        )
        (problems if gaps_hard else observed).append(gaps)
    if not report["visibility"].get("complete"):
        problems.append(f"procfs visibility {report['visibility']}")
    docker = report["docker"]
    for part in ("start", "end", "events"):
        if not docker.get(part, {}).get("observable", False):
            problems.append(f"docker {part} unobservable: {docker.get(part)}")
    detail = "; ".join(problems) or (
        f"{coverage['ticks']} ticks, largest gap {coverage['max_gap_s']}s"
    )
    if observed:
        detail += "; observed, not gated on this host: " + "; ".join(observed)
    return check(
        "build_monitor_coverage",
        CHECK_HARD,
        STATUS_FAILED if problems else STATUS_PASSED,
        detail,
    )


def settle_status(result) -> None:
    """A passed run with any failed check is a failed run."""
    if result["status"] == STATUS_PASSED and any(
        entry["status"] == STATUS_FAILED for entry in result["checks"]
    ):
        result["status"] = STATUS_FAILED


# --------------------------------------------------------------------------
# Baselines
# --------------------------------------------------------------------------


# The two ways a measured quantity can get worse. Every baseline metric
# carries one of them explicitly: a metric name does not decide it, because
# the same suffix names quantities that improve in opposite directions (a
# throughput ratio and a backlog ratio, for instance).
HIGHER_IS_BETTER = "higher_is_better"
LOWER_IS_BETTER = "lower_is_better"
DIRECTIONS = (HIGHER_IS_BETTER, LOWER_IS_BETTER)

# The hard gates a measured run must carry, each passed, before it may be
# compared or establish a baseline. A missing gate is not a passed gate: a
# run that never checked delivery, never checked its environment or never
# reconciled its memory has not shown that any of them held.
CORRECTNESS_CHECKS = ("delivery",)
VALIDITY_CHECKS = (
    "host_lease_held",
    "no_concurrent_build",
    "build_monitor_coverage",
    "environment_snapshots_complete",
    "environment_matched",
    "affinity_matched",
    "physical_cores_sufficient",
    "minimum_samples",
)
RESIDUAL_CHECKS = ("rss_reconciliation",)
REQUIRED_HARD_CHECKS = CORRECTNESS_CHECKS + VALIDITY_CHECKS + RESIDUAL_CHECKS

RUN_DIR_TOKEN = "<run_dir>"


def canonicalize_run_paths(value, run_dir):
    """Replace the run directory prefix of every path under it by a token.

    Only paths under this run's own directory are rewritten: they are the
    one part of an effective configuration that differs between otherwise
    identical runs. Every other string, including other paths, is kept
    exactly, so two configurations that point at different places outside
    the run directory never share a fingerprint.
    """
    roots = sorted(
        {str(Path(run_dir)), os.path.realpath(str(run_dir))}, key=len, reverse=True
    )

    def rewrite(item):
        """Rewrite one node of the configuration tree."""
        if isinstance(item, dict):
            return {key: rewrite(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [rewrite(child) for child in item]
        if isinstance(item, str):
            for root in roots:
                if item == root or item.startswith(root + os.sep):
                    return RUN_DIR_TOKEN + item[len(root):]
        return item

    return rewrite(value)


def canonicalize_ephemeral_values(value, ephemeral):
    """Replace each declared per-run value, exactly, by its token.

    A launcher binds its receiver to a fresh ephemeral port, which is as
    much a per-run artifact as the run directory. The run declares each such
    value in `ephemeral_values` as `{token: value}`; a string equal to a
    declared value becomes the token, and nothing else is touched, so an
    undeclared difference still separates two fingerprints.
    """
    for token, actual in ephemeral.items():
        if not (isinstance(token, str) and token.startswith("<") and token.endswith(">")):
            raise AssertionError(f"an ephemeral token is written <name>: {token!r}")
        if not isinstance(actual, str) or not actual:
            raise AssertionError(f"ephemeral value {token} must be a non-empty string")
    replacements = {actual: token for token, actual in ephemeral.items()}

    def rewrite(item):
        """Rewrite one node of the configuration tree."""
        if isinstance(item, dict):
            return {key: rewrite(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [rewrite(child) for child in item]
        if isinstance(item, str) and item in replacements:
            return replacements[item]
        return item

    return rewrite(value)


def _required(value, path):
    """One fingerprint input, or an error naming the input that is absent."""
    if value is None or (isinstance(value, (dict, list, str)) and not value):
        raise AssertionError(
            f"fingerprint input {path} is missing; a run whose environment, "
            f"configuration, workload or build is unrecorded cannot be compared"
        )
    return value


def fingerprint_material(result) -> dict:
    """Everything a baseline fingerprint covers, each input required.

    Machine identity and topology, including the sibling groups and the
    cores available to the run; the normalized placement of each role on
    cores; the effective configuration with run-directory paths
    canonicalized; the workload with its offered rate, duration and
    concurrency; and the build profile, features, allocator and toolchain.
    """
    environment = _required(result.get("environment"), "environment")
    start = _required(environment.get("start"), "environment.start")
    machine = {
        "identity_sha256": environment.get("machine_identity_sha256"),
        "cpu_model": start.get("cpu_model"),
        "logical_core_count": start.get("logical_core_count"),
        "physical_core_count": start.get("physical_core_count"),
        "sibling_groups": start.get("sibling_groups"),
        "ram_bytes": start.get("ram_bytes"),
        "kernel": start.get("kernel"),
    }
    for key, value in machine.items():
        _ = _required(value, f"machine.{key}")
    available = _required(start.get("available_cores"), "environment.start.available_cores")
    placement = _required(environment.get("core_allocation"), "environment.core_allocation")
    roles = {}
    for role, cores in placement.items():
        cores = _required(cores, f"environment.core_allocation.{role}")
        roles[str(role)] = sorted({int(core) for core in cores})
    run_dir = _required(result.get("run_dir"), "run_dir")
    config = _required(result.get("config", {}).get("effective"), "config.effective")
    workload = _required(result.get("workload"), "workload")
    for field in dataclasses.fields(Workload):
        if field.name in OPTIONAL_WORKLOAD_FIELDS and field.name not in workload:
            continue
        _ = _required(workload.get(field.name), f"workload.{field.name}")
    schedule = _required(result.get("workload_schedule"), "workload_schedule")
    for key in ("duration_s", "rate_requests_per_s", "max_in_flight"):
        _ = _required(schedule.get(key), f"workload_schedule.{key}")
    build = _required(environment.get("build"), "environment.build")
    build_fields = {}
    for key in ("profile", "features", "allocator", "toolchain"):
        build_fields[key] = _required(build.get(key), f"environment.build.{key}")
    material = {
        "case": _required(result.get("case"), "case"),
        "machine": machine,
        "available_cores": sorted(int(core) for core in available),
        "role_placement": dict(sorted(roles.items())),
        "config": canonicalize_ephemeral_values(
            canonicalize_run_paths(config, run_dir),
            result.get("ephemeral_values") or {},
        ),
        "ephemeral_tokens": sorted(result.get("ephemeral_values") or {}),
        "workload": workload,
        "schedule": {
            key: schedule[key]
            for key in ("duration_s", "rate_requests_per_s", "max_in_flight")
        },
        "build": build_fields,
    }
    # Every harness-supplied input is checked for nulls at any depth. The
    # effective engine configuration is the one exception: it is hashed
    # verbatim, because an explicit null there is a real setting -- the
    # durable buffer's `max_age: null` disables age-based retention -- and
    # not an input the harness failed to record. Its presence is still
    # required above.
    for key, value in material.items():
        if key != "config":
            _reject_nulls(value, f"fingerprint.{key}")
    return material


def _reject_nulls(value, path):
    """Fail on a null anywhere in one harness-supplied input, naming it.

    A null hashes as JSON `null`, so two runs that each failed to record the
    same nested input would share a fingerprint while describing different
    environments. An input that is genuinely absent must be omitted by the
    code that records it, or recorded as an explicit value, never as null.
    This does not apply to the effective engine configuration, whose nulls
    are settings.
    """
    if value is None:
        raise AssertionError(
            f"fingerprint input {path} is null; a nested input that was not "
            f"recorded cannot be hashed as if it were known"
        )
    if isinstance(value, dict):
        for key, child in value.items():
            _reject_nulls(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_nulls(child, f"{path}[{index}]")


def baseline_fingerprint(result) -> str:
    """The identity a baseline may only be compared within.

    The source revision and the binary hash are deliberately not in it: they
    are provenance, so a new implementation on the same machine and
    configuration is compared rather than excused.
    """
    encoded = json.dumps(
        fingerprint_material(result), sort_keys=True, ensure_ascii=True, allow_nan=False
    )
    return hashlib.sha256(encoded.encode("ascii")).hexdigest()


def baseline_name(case, fingerprint) -> str:
    """The immutable file name of one baseline."""
    return f"baseline-{case}-{fingerprint[:16]}.json"


def metric_directions(result) -> dict:
    """The declared direction of every metric, or an error naming the gap."""
    declared = result.get("metric_directions") or {}
    missing = sorted(name for name in result["metrics"] if name not in declared)
    if missing:
        raise AssertionError(
            f"metrics {missing} declare no direction; a regression cannot be "
            f"signed without knowing whether larger is better"
        )
    invalid = {
        name: value for name, value in declared.items() if value not in DIRECTIONS
    }
    if invalid:
        raise AssertionError(f"unknown metric directions {invalid}")
    extra = sorted(set(declared) - set(result["metrics"]))
    if extra:
        raise AssertionError(f"directions declared for unreported metrics {extra}")
    return {name: declared[name] for name in sorted(result["metrics"])}


def new_baseline(result, fingerprint) -> dict:
    """A baseline candidate built from one valid run."""
    unavailable = sorted(
        name for name, value in result["metrics"].items() if value is None
    )
    if unavailable:
        raise AssertionError(
            f"a baseline cannot be built from unavailable metrics {unavailable}"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "baseline",
        "case": result["case"],
        "fingerprint": fingerprint,
        "created_utc": utc_now(),
        "metric_schema": sorted(result["metrics"]),
        "metrics": dict(sorted(result["metrics"].items())),
        "directions": metric_directions(result),
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


def signed_regression(value, reference, direction, floor=None):
    """How much worse one metric got, as a signed fraction of the baseline.

    Positive is worse in the metric's declared direction. A zero reference
    permits no worsening at all unless the baseline declared a
    measurement-resolution floor that covers the new value.
    """
    if direction not in DIRECTIONS:
        raise AssertionError(f"unknown metric direction {direction!r}")
    if reference == 0:
        if value == 0:
            return 0.0
        if floor is not None and abs(value) <= floor:
            return 0.0
        worse = value > 0 if direction == LOWER_IS_BETTER else value < 0
        return float("inf") if worse else float("-inf")
    if direction == LOWER_IS_BETTER:
        return (value - reference) / abs(reference)
    return (reference - value) / abs(reference)


def hard_gate_failures(result) -> list:
    """Every hard check this run did not pass, and every required one absent."""
    present = {}
    for entry in result.get("checks", []):
        if entry["kind"] == CHECK_HARD:
            present.setdefault(entry["name"], []).append(entry["status"])
    failed = sorted(
        name
        for name, statuses in present.items()
        if any(status != STATUS_PASSED for status in statuses)
    )
    missing = sorted(name for name in REQUIRED_HARD_CHECKS if name not in present)
    return failed + [f"{name} (absent)" for name in missing]


def evaluate_baseline(result: dict, *, baselines: dict = None) -> dict:
    """Apply the Controller baseline policy to one completed result.

    Hard gates come first: correctness, measurement validity and the
    accounted-versus-RSS residual fail on every run, whether or not a
    baseline exists, and a run that fails one never creates a baseline. The
    complete required set must be present; an empty or partial set is a
    failure, never a pass. Only then is the fingerprint resolved, and a
    fingerprint with any unrecorded input is itself a failure. A matching
    baseline is compared against and left untouched, so small regressions
    cannot ratchet it upward; a new fingerprint produces a candidate baseline
    for the caller to write beside the result.

    The decision is attached to the result either way. A hard failure or a
    failed regression raises, and the caller still publishes the failed JSON.
    """
    decision = {
        "fingerprint": None,
        "limit": REGRESSION_LIMIT,
        "action": "rejected",
        "hard_gate_failures": [],
        "regressions": {},
    }
    result["baseline_decision"] = decision
    failures = hard_gate_failures(result)
    decision["hard_gate_failures"] = failures
    if failures:
        raise AssertionError(f"hard gates failed, no baseline may be written: {failures}")
    if result["status"] != STATUS_PASSED:
        decision["action"] = "not_a_baseline_candidate"
        raise AssertionError(
            f"run {result['run_id']} is {result['status']}; it cannot be "
            f"compared or establish a baseline"
        )
    fingerprint = baseline_fingerprint(result)
    decision["fingerprint"] = fingerprint
    directions = metric_directions(result)
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
    schema = set(reference.get("metric_schema") or reference["metrics"])
    reported = set(result["metrics"])
    if schema != reported:
        raise AssertionError(
            f"metric schemas differ from baseline {decision['baseline_name']}: "
            f"missing {sorted(schema - reported)}, extra {sorted(reported - schema)}"
        )
    unavailable = sorted(name for name in schema if result["metrics"][name] is None)
    if unavailable:
        raise AssertionError(
            f"the baseline compares metrics this run did not report: {unavailable}"
        )
    reference_directions = reference.get("directions") or {}
    changed = sorted(
        name
        for name in schema
        if reference_directions.get(name) != directions[name]
    )
    if changed:
        raise AssertionError(
            f"metric directions differ from the baseline or are missing there: "
            f"{changed}"
        )
    floors = reference.get("resolution_floor", {})
    worse = []
    for name in sorted(schema):
        value = result["metrics"][name]
        change = signed_regression(
            value, reference["metrics"][name], directions[name], floors.get(name)
        )
        unbounded = change in (float("inf"), float("-inf"))
        decision["regressions"][name] = {
            "value": value,
            "reference": reference["metrics"][name],
            "direction": directions[name],
            "signed_regression": None if unbounded else change,
            "unbounded": unbounded,
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
    # Every published file is checked here, not only the ones this run
    # wrote: an advancing index enumerates the index it replaces, and that
    # older evidence may predate the scrub. Nothing that still carries a
    # host path or a credential is published.
    for name, source in entries:
        document = json.loads(source.read_text(encoding="ascii"))
        if scrub_published(document) != document:
            raise AssertionError(
                f"{source} still carries a host path or a credential; scrub "
                f"it with `measure rescrub --index` before publishing"
            )
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


def rescrub_tree(index_path) -> list:
    """Scrub one published evidence tree in place, keeping it hash-consistent.

    The tree's hashes are verified first, as for publication. Every file is
    then rewritten through `scrub_published`, children before the documents
    that reference them, and each reference's `sha256` and `size_bytes` is
    recomputed from the rewritten child, so the index and its children stay
    consistent. The tree is verified again at the end. Returns the names of
    the files whose content changed.
    """
    index_path = Path(index_path)
    entries = enumerate_tree(index_path)
    paths = dict(entries)
    changed = []
    # Enumeration names a parent before its children, so the reverse order
    # rewrites every child before any document that hashes it.
    for name, path in reversed(entries):
        before = path.read_bytes()
        document = scrub_published(json.loads(before.decode("ascii")))
        for field in ("run_files", "baseline_files", "child_indexes"):
            for entry in document.get(field, []) or []:
                if isinstance(entry, dict) and entry.get("name") in paths:
                    child = paths[entry["name"]]
                    entry["sha256"] = file_digest(child)
                    entry["size_bytes"] = child.stat().st_size
        _ = write_json_atomic(path, document)
        if path.read_bytes() != before:
            changed.append(name)
    _ = enumerate_tree(index_path)
    return changed


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
