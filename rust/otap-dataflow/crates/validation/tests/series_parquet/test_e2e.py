# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Real OTLP producer, df_engine process and Parquet reader."""
import base64
import collections
import concurrent.futures
import contextlib
import hashlib
import http.server
import json
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree

import boto3
import duckdb
import grpc
import xxhash
import yaml
from botocore.config import Config as BotoConfig
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2 as logs_pb
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc as logs_rpc
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2 as metrics_pb
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2_grpc as metrics_rpc,
)
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2 as trace_pb
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc as trace_rpc

WORKSPACE = Path(__file__).resolve().parents[4]


# Ports this process has handed out. The kernel may give the same ephemeral
# port to two back-to-back binds once the first socket is closed, so a port is
# never handed out twice by one harness process.
_ISSUED_PORTS = set()


def free_port(attempts=64):
    """A loopback port that was free when probed and never issued before.

    Probing is check-then-use: another process can still take the port
    before the caller binds it. The dedupe removes the race between two
    callers in this process; a caller that loses the race with another
    process retries through its own start-up path.
    """
    last = None
    for _ in range(attempts):
        try:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
        except OSError as error:
            last = error
            continue
        if port not in _ISSUED_PORTS:
            _ISSUED_PORTS.add(port)
            return port
    raise AssertionError(f"no unissued loopback port after {attempts} probes: {last}")


def sleep_to_window_offset(interval_s, offset_s):
    """Sleep until `offset_s` seconds into the next aligned window.

    The exporter aligns windows to multiples of `interval_s` of Unix time, so
    a test that must act at a known point of a window waits for it here.
    """
    now = time.time()
    time.sleep(interval_s - (now % interval_s) + offset_s)


def window_start(path):
    """The window start recorded in a part file's name.

    Files are named `part-<YYYYMMDDTHHMMSSZ>-<writer>-<boot>-<seq>.parquet`,
    so the second field is the start of the window the block covered. Two
    files of the same window differ only in the sequence number.
    """
    return path.name.split("-")[1]


def by_window(paths):
    """Group part files by the window their name records."""
    grouped = collections.defaultdict(list)
    for path in paths:
        grouped[window_start(path)].append(path)
    return grouped


def log_request(request_id):
    """Build a single-record OTLP logs request whose body is `request_id`."""
    req = logs_pb.ExportLogsServiceRequest()
    resource = req.resource_logs.add()
    attr = resource.resource.attributes.add(key="host.id")
    attr.value.string_value = "producer-1"
    service = resource.resource.attributes.add(key="service.name")
    service.value.string_value = "series-e2e-service"
    scope = resource.scope_logs.add()
    scope.scope.name = "series-e2e"
    record = scope.log_records.add(time_unix_nano=1789960500000000000)
    record.body.string_value = request_id
    logger = record.attributes.add(key="logger.name")
    logger.value.string_value = "series.logger"
    return req


# One metric request carries one gauge point and two histogram points, and all
# three land in the single `signal=metrics/dataset=values` dataset.
POINTS_PER_METRIC = 3


def metric_request(request_id, unsupported=False):
    """Build an OTLP metrics request with one gauge and two histogram points.

    The second histogram carries no distribution at all, which is what makes
    the null-list case observable: its `bucket_counts` and `explicit_bounds`
    are genuinely empty lists, while the gauge row's are Parquet null. A
    request holding both shapes is the only way to tell the two encodings
    apart in a file, because the OTAP transport drops a column whose every
    entry in a request is the type default.

    `unsupported` appends a summary metric, which the lake has no dataset for;
    it is what the `unsupported` policy decides.
    """
    req = metrics_pb.ExportMetricsServiceRequest()
    resource = req.resource_metrics.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    scope = resource.scope_metrics.add()
    scope.scope.name = "series-e2e"
    number = scope.metrics.add(name="integer", unit="1")
    point = number.gauge.data_points.add(time_unix_nano=2**64 - 1, as_int=2**63 - 1)
    point.attributes.add(key="request.id").value.string_value = request_id
    hist = scope.metrics.add(name="histogram", unit="s")
    hist.histogram.aggregation_temporality = 2
    point = hist.histogram.data_points.add(
        time_unix_nano=1789960500000000000, count=3, sum=4.0
    )
    point.bucket_counts.extend([1, 2])
    point.explicit_bounds.append(1.0)
    point.attributes.add(key="request.id").value.string_value = request_id
    # A histogram with a count but no buckets and no bounds: both lists are
    # stored as empty lists, never as null lists.
    flat = scope.metrics.add(name="histogram_no_buckets", unit="s")
    flat.histogram.aggregation_temporality = 2
    point = flat.histogram.data_points.add(
        time_unix_nano=1789960500000000000, count=7, sum=9.0
    )
    point.attributes.add(key="request.id").value.string_value = request_id
    if unsupported:
        scope.metrics.add(name="summary").summary.data_points.add(count=1, sum=2.0)
    return req


# The two streams the collapse test sends. They differ only in `request.id`,
# which the upstream attribute processor deletes, and in their values; every
# stream shares one timestamp, one start time and every other attribute.
COLLAPSE_STREAMS = {
    "req-a-7f3c": {"requests": 3, "latency": (2, 1.5, [1, 1]), "total": 10},
    "req-b-91d2": {"requests": 4, "latency": (3, 2.5, [2, 1]), "total": 20},
}
COLLAPSE_METRICS = ("requests", "latency", "total")


def collapse_request(case, request_ids, time_ns, start_ns):
    """An OTLP metrics request with one point per metric for each stream.

    `requests` is a delta monotonic integer sum, `latency` a delta
    histogram and `total` a cumulative monotonic integer sum. Each point
    carries `request.id`, the attribute to be deleted, and the surviving
    attributes `route` and `case`; `case` names how the streams were sent,
    so each sending mode has its own collapsed identity.
    """
    req = metrics_pb.ExportMetricsServiceRequest()
    resource = req.resource_metrics.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    scope = resource.scope_metrics.add()
    scope.scope.name = "series-e2e"
    requests = scope.metrics.add(name="requests", unit="1")
    requests.sum.aggregation_temporality = 1
    requests.sum.is_monotonic = True
    latency = scope.metrics.add(name="latency", unit="s")
    latency.histogram.aggregation_temporality = 1
    total = scope.metrics.add(name="total", unit="1")
    total.sum.aggregation_temporality = 2
    total.sum.is_monotonic = True
    for request_id in request_ids:
        stream = COLLAPSE_STREAMS[request_id]
        count, sum_, buckets = stream["latency"]
        points = (
            requests.sum.data_points.add(as_int=stream["requests"]),
            latency.histogram.data_points.add(count=count, sum=sum_),
            total.sum.data_points.add(as_int=stream["total"]),
        )
        points[1].bucket_counts.extend(buckets)
        points[1].explicit_bounds.append(1.0)
        for point in points:
            point.time_unix_nano = time_ns
            point.start_time_unix_nano = start_ns
            point.attributes.add(key="route").value.string_value = "/checkout"
            point.attributes.add(key="case").value.string_value = case
            point.attributes.add(key="request.id").value.string_value = request_id
    return req


class LocalLauncher:
    """Start the engine as a child process of this harness, on this host.

    A launcher owns only how the engine process starts and which host PID
    it has. Every RSS, affinity and kill operation uses that host PID, so a
    container launcher returns the engine's host PID, never the PID of a
    Docker CLI that happens to wait for it.
    """

    kind = "local"

    def start(self, argv, log, env):
        """Start `argv` with its output in `log` and return the process."""
        return subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env)

    def pid(self, process):
        """The host PID of the engine this launcher started."""
        return process.pid


# `noop` replaces the exporter with the always-enabled noop exporter, which
# acknowledges every request without storing anything. It is the pipeline
# baseline of the layered benchmarks: the receiver and the engine are the
# real ones, so it calibrates producer and engine cost, and it is never a
# durable-storage measurement.
#
# `processed` inserts one caller-supplied processor node between the receiver
# and the exporter. It exists for functional tests of how the exporter
# behaves behind an upstream transformation and is never measured.
TOPOLOGIES = ("strict", "buffered", "noop", "processed")

# The node the buffered topology inserts between the receiver and the
# exporter, and the settings every buffered measurement runs with: a bounded
# retention that pushes back rather than dropping, and no age limit, so that
# nothing retained is ever discarded for being old.
BUFFER_NODE = "buffer"
BUFFER_RETENTION_SIZE_CAP = "1GiB"

# The node the processed topology inserts between the receiver and the
# exporter.
PROCESSOR_NODE = "processor"


def deep_merge(base, patch):
    """Merge `patch` into `base` in place, map by map, and return `base`.

    A nested map is merged key by key; any other value, lists included,
    replaces what was there. An explicit `None` is kept as a setting.
    """
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def engine_config(
    *,
    grpc_port,
    data,
    storage=None,
    overrides=None,
    interval="1s",
    telemetry_interval=None,
    log_level=None,
    topology="strict",
    buffer_path=None,
    cores=None,
    merge=None,
    grpc_host="127.0.0.1",
    processor=None,
    extensions=None,
    exporter_capabilities=None,
):
    """The complete configuration one engine launch serializes.

    Without the keyword-only measurement settings this is exactly the
    configuration the fixture tests always ran. `topology="buffered"`
    inserts the durable buffer between the receiver and the exporter,
    `cores` pins one worker to each named core through a `core_set`, and
    `merge` deep-merges nested maps into the named nodes' configurations
    (`engine` into the engine section, `policies` into the top-level
    policies and `pipeline_policies` into the measured pipeline's own, which
    holds the channel capacities). The result is what gets hashed
    and recorded, so nothing is changed after it is returned. `grpc_host`
    is the address the receiver binds: a launcher that runs the engine in
    its own network namespace binds every address there, and reaches it
    through a port it publishes on host loopback. `topology="processed"`
    inserts `processor`, a complete node definition, between the receiver
    and the exporter. `extensions` declares the pipeline's extensions and
    `exporter_capabilities` binds the exporter's capabilities to them, as
    Azure storage needs for its bearer token.
    """
    if topology not in TOPOLOGIES:
        raise ValueError(f"topology must be one of {TOPOLOGIES}: {topology}")
    config = yaml.safe_load(
        (WORKSPACE / "configs/series-parquet-local.yaml").read_text()
    )
    pipeline = config["groups"]["default"]["pipelines"]["main"]
    nodes = pipeline["nodes"]
    nodes["receiver"]["config"]["protocols"]["grpc"]["listening_addr"] = (
        f"{grpc_host}:{grpc_port}"
    )
    if topology == "noop":
        # The noop exporter has no node configuration at all, so the
        # exporter section is replaced rather than merged into.
        nodes["exporter"] = {"type": "exporter:noop", "config": {}}
        if overrides:
            raise ValueError("the noop topology has no exporter settings")
    else:
        export = nodes["exporter"]["config"]
        export["storage"] = storage or {"file": {"base_uri": str(data)}}
        export["window"]["interval"] = interval
        if overrides:
            for key, value in overrides.items():
                export[key] = value
        if exporter_capabilities:
            nodes["exporter"]["capabilities"] = dict(exporter_capabilities)
    if extensions:
        pipeline["extensions"] = dict(extensions)
    if log_level:
        # Set explicitly rather than through RUST_LOG, which the engine
        # only consults when the configuration omits a level: a test that
        # reads a DEBUG event must not depend on the caller's environment.
        config["engine"]["telemetry"]["logs"] = {"level": log_level}
    if telemetry_interval:
        # The exporter's gauges are sampled when the engine collects
        # telemetry, so a test that has to observe a short-lived state
        # needs the collection to be faster than that state.
        config["engine"]["telemetry"]["reporting_interval"] = telemetry_interval
    if topology == "buffered":
        if buffer_path is None:
            raise ValueError("the buffered topology needs an explicit buffer_path")
        nodes[BUFFER_NODE] = {
            "type": "processor:durable_buffer",
            "config": {
                "path": str(buffer_path),
                "retention_size_cap": BUFFER_RETENTION_SIZE_CAP,
                "size_cap_policy": "backpressure",
                "max_age": None,
                "otlp_handling": "pass_through",
            },
        }
        # A single destination resolves to `one_of` in the configuration
        # API, so each request reaches exactly one buffer instance.
        pipeline["connections"] = [
            {"from": "receiver", "to": BUFFER_NODE},
            {"from": BUFFER_NODE, "to": "exporter"},
        ]
    elif buffer_path is not None:
        raise ValueError("buffer_path is only meaningful for the buffered topology")
    if topology == "processed":
        if processor is None:
            raise ValueError("the processed topology needs a processor node")
        nodes[PROCESSOR_NODE] = processor
        pipeline["connections"] = [
            {"from": "receiver", "to": PROCESSOR_NODE},
            {"from": PROCESSOR_NODE, "to": "exporter"},
        ]
    elif processor is not None:
        raise ValueError("processor is only meaningful for the processed topology")
    if cores is not None:
        cores = [int(core) for core in cores]
        if not cores or len(set(cores)) != len(cores):
            raise ValueError(f"cores must name distinct core ids: {cores}")
        config["policies"]["resources"]["core_allocation"] = {
            "type": "core_set",
            "set": [{"start": core, "end": core} for core in cores],
        }
    for name, patch in (merge or {}).items():
        if name == "engine":
            deep_merge(config["engine"], patch)
        elif name == "policies":
            deep_merge(config["policies"], patch)
        elif name == "pipeline_policies":
            deep_merge(pipeline.setdefault("policies", {}), patch)
        elif name in nodes:
            deep_merge(nodes[name].setdefault("config", {}), patch)
        else:
            raise ValueError(f"a merged override names no node: {name}")
    return config


def graph_edges(config):
    """The measured pipeline's edges, each checked to have one recipient.

    Every connection must name a single destination and no dispatch policy,
    so each request reaches exactly one instance downstream; a broadcast or
    a multiple-recipient connection is refused here, before launch.
    """
    pipeline = config["groups"]["default"]["pipelines"]["main"]
    edges = []
    for connection in pipeline["connections"]:
        target = connection["to"]
        if isinstance(target, list):
            if len(target) != 1:
                raise AssertionError(
                    f"connection {connection} has {len(target)} recipients; a "
                    f"measured request must reach exactly one instance"
                )
            target = target[0]
        if (connection.get("policies") or {}).get("dispatch") is not None:
            raise AssertionError(
                f"connection {connection} sets a dispatch policy; a measured "
                f"request must reach exactly one instance"
            )
        edges.append((connection["from"], target))
    sources = collections.Counter(source for source, _ in edges)
    targets = collections.Counter(target for _, target in edges)
    fanned = sorted(
        name for name, count in list(sources.items()) + list(targets.items())
        if count > 1
    )
    if fanned:
        raise AssertionError(f"nodes {fanned} fan out or in; the graph is {edges}")
    return edges


EXPECTED_EDGES = {
    "strict": [("receiver", "exporter")],
    "noop": [("receiver", "exporter")],
    "buffered": [("receiver", BUFFER_NODE), (BUFFER_NODE, "exporter")],
    "processed": [("receiver", PROCESSOR_NODE), (PROCESSOR_NODE, "exporter")],
}


class Engine:
    """A real `df_engine` process running the series Parquet example config."""

    def __init__(
        self,
        directory,
        storage=None,
        overrides=None,
        interval="1s",
        telemetry_interval=None,
        log_level=None,
        *,
        topology="strict",
        buffer_path=None,
        cores=None,
        launcher=None,
        merge=None,
        binary=None,
        processor=None,
        extensions=None,
        exporter_capabilities=None,
        env=None,
    ):
        self.root = Path(directory)
        self.data = self.root / "data"
        self.data.mkdir(exist_ok=True)
        self.launcher = launcher or LocalLauncher()
        # A launcher that owns the engine's network namespace also owns its
        # ports: it published them on host loopback before the engine
        # existed, and the engine binds them on every address in there.
        reserve = getattr(self.launcher, "reserve_ports", None)
        if reserve is not None:
            self.grpc_port, self.admin_port = reserve()
        else:
            self.grpc_port = free_port()
            self.admin_port = free_port()
        bind_host = getattr(self.launcher, "bind_host", "127.0.0.1")
        self.topology = topology
        self.cores = None if cores is None else [int(core) for core in cores]
        # The buffer directory is used as given. A restart passes the same
        # path again and finds its retained segments; nothing here creates a
        # fresh one or replaces what is already there.
        self.buffer_path = None if buffer_path is None else Path(buffer_path)
        self.config = engine_config(
            grpc_port=self.grpc_port,
            data=self.data,
            storage=storage,
            overrides=overrides,
            interval=interval,
            telemetry_interval=telemetry_interval,
            log_level=log_level,
            topology=topology,
            buffer_path=self.buffer_path,
            cores=self.cores,
            merge=merge,
            grpc_host=bind_host,
            processor=processor,
            extensions=extensions,
            exporter_capabilities=exporter_capabilities,
        )
        self.edges = graph_edges(self.config)
        if self.edges != EXPECTED_EDGES[topology]:
            raise AssertionError(
                f"the {topology} graph is {self.edges}, expected "
                f"{EXPECTED_EDGES[topology]}"
            )
        self.path = self.root / "pipeline.yaml"
        self.path.write_text(yaml.safe_dump(self.config))
        # The hash of the exact file the engine reads, so a result records
        # the configuration that ran rather than the one that was meant.
        self.config_sha256 = hashlib.sha256(self.path.read_bytes()).hexdigest()
        # A measurement names its release binary; the fixture tests keep the
        # debug build or `DF_ENGINE`, exactly as before.
        binary = Path(
            binary or os.environ.get("DF_ENGINE", WORKSPACE / "target/debug/df_engine")
        )
        if not binary.is_file():
            raise AssertionError(f"build the feature-enabled engine first: {binary}")
        self.binary = binary
        self.log = (self.root / "engine.log").open("w+")
        self.process = self.launcher.start(
            [
                str(binary),
                "--config",
                str(self.path),
                "--http-admin-bind",
                f"{bind_host}:{self.admin_port}",
            ],
            self.log,
            {**os.environ, **(env or {})},
        )
        self.pid = self.launcher.pid(self.process)
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.grpc_port}")
        try:
            self.wait_ready(30)
            grpc.channel_ready_future(self.channel).result(timeout=30)
        except Exception:
            # The caller usually runs inside a TemporaryDirectory that is about
            # to be removed, taking the engine log with it. Print it before
            # closing, or a startup failure leaves nothing to diagnose.
            print(f"engine failed to start, log follows:\n{self.engine_log()}")
            self.close()
            raise
        self.logs = logs_rpc.LogsServiceStub(self.channel)

    def exporter_gauges(self, *names):
        """Largest value of each named exporter gauge, from the admin API.

        Read through the same JSON snapshot and the same `metric_values`
        every other assertion uses, so there is one telemetry reader. A gauge
        the snapshot does not carry, or a snapshot that could not be taken,
        reads as `None`, never as zero: the callers wait for a state to
        appear and must not mistake an absent sample for an empty exporter.
        """
        try:
            document = engine_metrics(self)
        except Exception:
            return dict.fromkeys(names)
        found = {}
        for name in names:
            values = metric_values(document, name)
            found[name] = max(values) if values else None
        return found

    def wait_ready(self, seconds):
        """Poll the admin API's `/api/v1/readyz` until it answers 200.

        The engine's own readiness, the probe the Rust scenario framework
        uses, rather than inferring it from one listener accepting a
        connection.
        """
        url = f"http://127.0.0.1:{self.admin_port}/api/v1/readyz"
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError("the engine exited before it became ready")
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        return
                    last = f"status {response.status}"
            except Exception as error:
                last = error
            time.sleep(0.05)
        raise AssertionError(f"the engine never became ready: {last}")

    def engine_log(self):
        """Return everything the engine has written to its combined log."""
        self.log.flush()
        return Path(self.log.name).read_text(errors="replace")

    def shutdown(self, seconds=180):
        """Ask the admin API to stop every pipeline group and wait for it."""
        url = (
            f"http://127.0.0.1:{self.admin_port}/api/v1/groups/shutdown"
            f"?wait=true&timeout_secs={seconds}"
        )
        with urllib.request.urlopen(
            urllib.request.Request(url, method="POST"), timeout=seconds + 5
        ) as response:
            if response.status != 200:
                raise AssertionError(response.read().decode())

    def close(self):
        """Terminate the engine process and release its resources."""
        self.channel.close()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class LocalSlice(unittest.TestCase):
    """The smallest runnable slice: OTLP logs to local Parquet files."""

    # Scenario: a real gRPC logs request reaches a local series exporter.
    # Guarantees: a successful OTLP response has both descriptor and values
    # files, the values row carries the body that was sent, and the values row
    # joins to exactly one series row on series_id. The response arrives once
    # the window the request was admitted to has been written, which the
    # one-second test interval keeps short.
    def test_local_logs_are_durable_at_ack(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            try:
                engine.logs.Export(log_request("request-0"), timeout=20)
                values = list(
                    engine.data.glob("v=1/signal=logs/dataset=values/**/*.parquet")
                )
                series = list(
                    engine.data.glob("v=1/signal=logs/dataset=series/**/*.parquet")
                )
                self.assertTrue(values, "no values file after a successful export")
                self.assertTrue(series, "no series file after a successful export")
                with duckdb.connect() as db:
                    rows = db.execute(
                        "SELECT body FROM read_parquet(?)",
                        [[str(path) for path in values]],
                    ).fetchall()
                    self.assertEqual(rows, [("request-0",)])
                    joined = db.execute(
                        "SELECT v.body, v.producer_id, s.scope_name, "
                        "s.attrs['logger.name'] "
                        "FROM read_parquet(?) v "
                        "JOIN read_parquet(?) s USING (series_id)",
                        [
                            [str(path) for path in values],
                            [str(path) for path in series],
                        ],
                    ).fetchall()
                self.assertEqual(
                    joined,
                    [("request-0", "producer-1", "series-e2e", "series.logger")],
                )
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise

    # Scenario: four concurrent logs requests are submitted immediately after
    # an aligned boundary of a three-second rotation interval.
    # Guarantees: every request is acknowledged and durable, and every window
    # that received data wrote exactly one file set, so rotation is driven by
    # the window and not by the request. The submissions are aligned to a
    # boundary so that they normally share a single window; grouping the files
    # by the window their name records is what makes the assertion exact
    # rather than a tolerance on the file count, and it stays exact if a
    # submission does straddle a boundary.
    def test_one_window_writes_one_file_set(self):
        with tempfile.TemporaryDirectory() as directory, Engine(
            directory, interval="3s"
        ) as engine:
            try:
                # Boundaries are aligned to Unix time, so this waits for the
                # start of a window rather than for an arbitrary instant.
                time.sleep(-time.time() % 3.0)
                calls = [
                    engine.logs.Export.future(log_request(f"request-{n}"), timeout=30)
                    for n in range(4)
                ]
                for call in calls:
                    call.result()
                values = list(
                    engine.data.glob("v=1/signal=logs/dataset=values/**/*.parquet")
                )
                series = list(
                    engine.data.glob("v=1/signal=logs/dataset=series/**/*.parquet")
                )
                self.assertTrue(values, "no values file after four exports")
                self.assertTrue(series, "no series file after four exports")
                values_by_window = by_window(values)
                for window, paths in sorted(values_by_window.items()):
                    self.assertEqual(
                        len(paths),
                        1,
                        f"window {window} wrote {len(paths)} values files: {paths}",
                    )
                # A descriptor is written once per partition, so a later
                # window need not write a series file at all; the ones that do
                # write exactly one, and they belong to a window that has
                # values.
                for window, paths in sorted(by_window(series).items()):
                    self.assertEqual(
                        len(paths),
                        1,
                        f"window {window} wrote {len(paths)} series files: {paths}",
                    )
                    self.assertIn(window, values_by_window)
                with duckdb.connect() as db:
                    rows = db.execute(
                        "SELECT body FROM read_parquet(?) ORDER BY body",
                        [[str(path) for path in values]],
                    ).fetchall()
                self.assertEqual(rows, [(f"request-{n}",) for n in range(4)])
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise


class MetricsSlice(unittest.TestCase):
    """Metrics admission, unsupported policy and content refusals."""

    # Scenario: a gauge INT64_MAX, a histogram with buckets and a histogram
    # without a distribution arrive in one real OTLP request and land in the
    # single merged metrics values dataset.
    # Guarantees: integer precision, histogram shape and wrapped timestamp
    # nullability survive Parquet; the point kind is read from the series
    # descriptor and never from which columns are null; the three kinds share
    # one file with the other kinds' columns null; and a distribution-less
    # histogram stores empty lists while a number row stores null lists, which
    # is the difference the ClickHouse reader cannot see and no query is
    # allowed to depend on.
    def test_number_and_histogram(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            try:
                metrics_rpc.MetricsServiceStub(engine.channel).Export(
                    metric_request("metric-0"), timeout=20
                )
                with duckdb.connect() as db:
                    values = str(
                        engine.data / "v=1/signal=metrics/dataset=values/**/*.parquet"
                    )
                    # Every kind shares one file: exactly one values file.
                    self.assertEqual(
                        len(
                            list(
                                engine.data.glob(
                                    "v=1/signal=metrics/dataset=values/**/*.parquet"
                                )
                            )
                        ),
                        1,
                    )
                    # One descriptor per series: the newest wins, ties broken
                    # by filename, exactly as the crate README prescribes.
                    series = str(
                        engine.data / "v=1/signal=metrics/dataset=series/**/*.parquet"
                    )
                    # A view cannot take a bound parameter, and the path comes
                    # from this test's own temporary directory.
                    db.execute(
                        "CREATE VIEW latest AS SELECT * EXCLUDE (rn, filename) "
                        "FROM (SELECT *, row_number() OVER (PARTITION BY "
                        "series_id ORDER BY emitted_at DESC, filename DESC) "
                        f"AS rn FROM read_parquet('{series}', "
                        "union_by_name = true, filename = true)) WHERE rn = 1"
                    )
                    # The point kind comes from the descriptor, so every row
                    # below is selected by metric_name and metric_type rather
                    # than by any column being null.
                    joined = db.execute(
                        "SELECT s.metric_name, s.unit, s.metric_type, "
                        "s.temporality, s.scope_name, s.attrs['request.id'], "
                        "v.producer_id "
                        "FROM read_parquet(?) v JOIN latest s USING (series_id) "
                        "ORDER BY 1",
                        [values],
                    ).fetchall()
                    self.assertEqual(
                        joined,
                        [
                            (
                                "histogram",
                                "s",
                                "histogram",
                                "cumulative",
                                "series-e2e",
                                "metric-0",
                                "producer-1",
                            ),
                            (
                                "histogram_no_buckets",
                                "s",
                                "histogram",
                                "cumulative",
                                "series-e2e",
                                "metric-0",
                                "producer-1",
                            ),
                            (
                                "integer",
                                "1",
                                "gauge",
                                "",
                                "series-e2e",
                                "metric-0",
                                "producer-1",
                            ),
                        ],
                    )
                    # The gauge row: INT64_MAX survives, the wrapped timestamp
                    # is null, and every histogram column is null.
                    self.assertEqual(
                        db.execute(
                            "SELECT v.value_int, v.time_unix_nano, v.count, "
                            "v.sum, v.min, v.max, v.bucket_counts, "
                            "v.explicit_bounds FROM read_parquet(?) v "
                            "JOIN latest s USING (series_id) "
                            "WHERE s.metric_type = 'gauge'",
                            [values],
                        ).fetchall(),
                        [(2**63 - 1, None, None, None, None, None, None, None)],
                    )
                    # The histogram rows: both value columns null, the shape
                    # preserved, and the distribution-less one carrying a
                    # count with no buckets.
                    self.assertEqual(
                        db.execute(
                            "SELECT s.metric_name, v.count, v.sum, "
                            "v.bucket_counts, v.explicit_bounds, v.value_int, "
                            "v.value_double FROM read_parquet(?) v "
                            "JOIN latest s USING (series_id) "
                            "WHERE s.metric_type = 'histogram' "
                            "ORDER BY s.metric_name",
                            [values],
                        ).fetchall(),
                        [
                            ("histogram", 3, 4.0, [1, 2], [1.0], None, None),
                            ("histogram_no_buckets", 7, 9.0, [], [], None, None),
                        ],
                    )
                    # The encoding the readers disagree about, asserted
                    # against the file itself: a histogram without a
                    # distribution stores an empty list, a number row stores a
                    # Parquet null. DuckDB can see the difference; ClickHouse
                    # renders both as [], which is why classification goes
                    # through the descriptor above and never through this.
                    self.assertEqual(
                        db.execute(
                            "SELECT s.metric_name, "
                            "v.bucket_counts IS NULL, "
                            "v.explicit_bounds IS NULL, "
                            "len(v.bucket_counts), len(v.explicit_bounds) "
                            "FROM read_parquet(?) v "
                            "JOIN latest s USING (series_id) "
                            "ORDER BY s.metric_name",
                            [values],
                        ).fetchall(),
                        [
                            ("histogram", False, False, 2, 1),
                            ("histogram_no_buckets", False, False, 0, 0),
                            ("integer", True, True, None, None),
                        ],
                    )
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise

    # Scenario: supported points share a request with an unsupported summary.
    # Guarantees: reject is atomic and writes nothing, while drop returns OK
    # only after the supported rows are durable.
    def test_mixed_policy(self):
        for policy in ("reject", "drop"):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as directory:
                with Engine(directory, overrides={"unsupported": policy}) as engine:
                    try:
                        call = metrics_rpc.MetricsServiceStub(engine.channel)
                        if policy == "reject":
                            with self.assertRaises(grpc.RpcError) as caught:
                                call.Export(metric_request("mixed", True), timeout=20)
                            self.assertEqual(
                                caught.exception.code(),
                                grpc.StatusCode.INVALID_ARGUMENT,
                            )
                            self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                        else:
                            call.Export(metric_request("mixed", True), timeout=20)
                            self.assertTrue(
                                list(
                                    engine.data.glob(
                                        "v=1/signal=metrics/dataset=values/"
                                        "**/*.parquet"
                                    )
                                )
                            )
                        engine.shutdown()
                    except Exception:
                        print(engine.engine_log())
                        raise

    # Scenario: a traces request arrives even with unsupported: drop.
    # Guarantees: traces are permanently refused and never produce lake files,
    # because the drop policy governs unsupported metric points and not a
    # signal the lake has no schema for.
    def test_traces_are_refused(self):
        with tempfile.TemporaryDirectory() as directory, Engine(
            directory, overrides={"unsupported": "drop"}
        ) as engine:
            try:
                req = trace_pb.ExportTraceServiceRequest()
                req.resource_spans.add().scope_spans.add().spans.add(
                    name="unsupported"
                )
                with self.assertRaises(grpc.RpcError) as caught:
                    trace_rpc.TraceServiceStub(engine.channel).Export(req, timeout=20)
                self.assertEqual(
                    caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT
                )
                self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise

    # Scenario: invalid temporality, histogram shape/count or duplicate
    # attributes arrive.
    # Guarantees: each request is refused atomically and a subsequent valid
    # request still commits, so a content error does not stop the worker.
    def test_content_failures_do_not_stop_worker(self):
        cases = []
        temporal = metric_request("bad-temporality")
        temporal.resource_metrics[0].scope_metrics[0].metrics[
            1
        ].histogram.aggregation_temporality = 0
        cases.append(temporal)
        shape = metric_request("bad-shape")
        shape.resource_metrics[0].scope_metrics[0].metrics[
            1
        ].histogram.data_points[0].bucket_counts.append(1)
        cases.append(shape)
        count = metric_request("bad-count")
        count.resource_metrics[0].scope_metrics[0].metrics[1].histogram.data_points[
            0
        ].count = 2**63
        cases.append(count)
        duplicate = metric_request("bad-attrs")
        attrs = duplicate.resource_metrics[0].scope_metrics[0].metrics[
            0
        ].gauge.data_points[0].attributes
        attrs.add(key="request.id").value.string_value = "duplicate"
        cases.append(duplicate)
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            try:
                call = metrics_rpc.MetricsServiceStub(engine.channel)
                for req in cases:
                    with self.assertRaises(grpc.RpcError) as caught:
                        call.Export(req, timeout=20)
                    self.assertEqual(
                        caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT
                    )
                self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                call.Export(metric_request("valid-after-errors"), timeout=20)
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise

    # Scenario: the drop policy receives only an unsupported summary point.
    # Guarantees: a request that extracts no rows receives OK without opening
    # any Parquet file.
    def test_zero_output_drop_acks_without_files(self):
        with tempfile.TemporaryDirectory() as directory, Engine(
            directory, overrides={"unsupported": "drop"}
        ) as engine:
            try:
                request = metric_request("unsupported-only", unsupported=True)
                metrics = request.resource_metrics[0].scope_metrics[0].metrics
                # `unsupported=True` appends the summary last, so dropping
                # everything before it leaves a request with no supported
                # points however many the fixture grows to carry.
                del metrics[:-1]
                self.assertEqual(len(metrics), 1)
                metrics_rpc.MetricsServiceStub(engine.channel).Export(
                    request, timeout=20
                )
                self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise

    # Scenario: logs exceed a row, extracted-output or input budget, or
    # contain excessive nesting.
    # Guarantees: each limit rejects the whole request permanently and writes
    # no partial data.
    def test_each_ingress_budget_refuses_atomically(self):
        cases = []
        large = log_request("x" * 4096)
        cases.append(({"max_request_bytes": "1KiB"}, large))
        cases.append(({"max_row_bytes": "1KiB"}, large))
        cases.append(({"max_extracted_bytes": "1KiB"}, large))
        deep = log_request("deep")
        value = deep.resource_logs[0].resource.attributes.add(key="nested").value
        for _ in range(40):
            value = value.array_value.values.add()
        value.string_value = "leaf"
        cases.append(({"max_nesting_depth": 8}, deep))
        for limits, request in cases:
            with self.subTest(limits=limits), tempfile.TemporaryDirectory() as directory:
                with Engine(directory, overrides={"ingress": limits}) as engine:
                    try:
                        with self.assertRaises(grpc.RpcError) as caught:
                            engine.logs.Export(request, timeout=20)
                        self.assertEqual(
                            caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT
                        )
                        self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                        engine.shutdown()
                    except Exception:
                        print(engine.engine_log())
                        raise
    # Scenario: an upstream `processor:attribute` deletes `request.id`, a
    # point attribute unique to each of two otherwise identical streams, in
    # front of the exporter. Each stream sends a delta integer sum, a delta
    # histogram and a cumulative integer sum with one shared timestamp, once
    # with both streams in one request and once as two separate requests.
    # Guarantees: every request is acknowledged; each collapsed identity has
    # exactly one descriptor row, because the descriptor is written once per
    # partition and worker; each collapsed pair is two values rows with the
    # same series_id and timestamp; no `request.id` key or value survives in
    # any written file; and DuckDB and ClickHouse agree that the collapsed
    # delta sum and delta histogram sum to the originals. The cumulative pair
    # is only shown not to break anything: it is stored as two running totals
    # under one series_id, which no reader can tell apart, so its value is
    # not asserted to be meaningful. The exporter has no series-to-points
    # ratio signal at this revision, so none is asserted.
    def test_attribute_delete_collapses_streams(self):
        # Descriptors repeat per hour partition, so a window that straddles
        # an hour boundary would legitimately write a second one.
        if time.time() % 3600 > 3600 - 20:
            time.sleep(3600 - time.time() % 3600 + 1)
        processor = {
            "type": "processor:attribute",
            "config": {
                "apply_to": ["signal"],
                "actions": [{"action": "delete", "key": "request.id"}],
            },
        }
        ids = sorted(COLLAPSE_STREAMS)
        time_ns = time.time_ns() // 10**9 * 10**9
        start_ns = time_ns - 10 * 10**9
        with tempfile.TemporaryDirectory() as directory, Engine(
            directory, topology="processed", processor=processor
        ) as engine:
            try:
                call = metrics_rpc.MetricsServiceStub(engine.channel)
                # Any RpcError here is a nack and fails the test.
                call.Export(collapse_request("one", ids, time_ns, start_ns), timeout=20)
                for request_id in ids:
                    call.Export(
                        collapse_request("two", [request_id], time_ns, start_ns),
                        timeout=20,
                    )
                # The processor's own count of deleted signal attributes: one
                # per point, three metrics times two streams in each mode. It
                # is sampled on the telemetry interval, so it is polled.
                points = 2 * len(COLLAPSE_METRICS) * len(ids)
                deadline = time.monotonic() + 15
                deleted = []
                while time.monotonic() < deadline:
                    deleted = [
                        item["value"]
                        for group in engine_metrics(engine)["metric_sets"]
                        if group["name"] == "processor.attributes.modified"
                        for item in group["metrics"]
                        if item["name"] == "entries"
                        and item["attributes"]
                        == {
                            "action": {"String": "deleted"},
                            "domain": {"String": "signal"},
                        }
                    ]
                    if deleted == [points]:
                        break
                    time.sleep(0.2)
                self.assertEqual(deleted, [points], "processor deletions")
                engine.shutdown()
            except Exception:
                print(engine.engine_log())
                raise
            root = engine.data.resolve()
            files = sorted(root.rglob("*.parquet"))
            self.assertTrue(files, "no Parquet files after three acknowledged exports")
            with duckdb.connect() as db:
                series = sql_string(root / "v=1/signal=metrics/dataset=series/**/*.parquet")
                values = sql_string(root / "v=1/signal=metrics/dataset=values/**/*.parquet")
                # Every descriptor row, deliberately without the
                # latest-descriptor view: a repeated descriptor is what this
                # counts.
                descriptors = db.execute(
                    "SELECT metric_name, attrs['case'], metric_type, temporality, "
                    "count(*), count(DISTINCT series_id) "
                    f"FROM read_parquet({series}, union_by_name=true, "
                    "hive_partitioning=false) GROUP BY ALL ORDER BY ALL"
                ).fetchall()
                self.assertEqual(
                    descriptors,
                    sorted(
                        (name, case, kind, temporality, 1, 1)
                        for case in ("one", "two")
                        for name, kind, temporality in (
                            ("latency", "histogram", "delta"),
                            ("requests", "sum", "delta"),
                            ("total", "sum", "cumulative"),
                        )
                    ),
                )
                keys = db.execute(
                    "SELECT DISTINCT unnest(map_keys(attrs)) "
                    f"FROM read_parquet({series}, union_by_name=true, "
                    "hive_partitioning=false) ORDER BY 1"
                ).fetchall()
                self.assertEqual(keys, [("case",), ("route",)])
                # Each collapsed pair: two rows, one series_id, one
                # timestamp, and the values of both originals.
                pair_sql = (
                    "SELECT s.metric_name, s.attrs['case'], count(*), "
                    "count(DISTINCT v.series_id), count(DISTINCT v.time_unix_nano), "
                    "min(v.time_unix_nano), sum(v.value_int), sum(v.count), "
                    "sum(v.sum), list_sort(list(v.value_int)) "
                    f"FROM read_parquet({values}, union_by_name=true, "
                    "hive_partitioning=false) v "
                    f"JOIN (SELECT DISTINCT series_id, metric_name, attrs "
                    f"FROM read_parquet({series}, union_by_name=true, "
                    "hive_partitioning=false)) s USING (series_id) "
                    "GROUP BY ALL ORDER BY ALL"
                )
                pairs = db.execute(pair_sql).fetchall()
                expected = []
                for name in sorted(COLLAPSE_METRICS):
                    for case in ("one", "two"):
                        streams = [COLLAPSE_STREAMS[i] for i in ids]
                        if name == "latency":
                            numbers = (None, 5, 4.0, [None, None])
                        else:
                            numbers = (
                                sum(stream[name] for stream in streams),
                                None,
                                None,
                                sorted(stream[name] for stream in streams),
                            )
                        expected.append((name, case, 2, 1, 1, time_ns) + numbers)
                self.assertEqual(pairs, expected)
                buckets = db.execute(
                    "SELECT s.attrs['case'], list_sort(list(v.bucket_counts)) "
                    f"FROM read_parquet({values}, union_by_name=true, "
                    "hive_partitioning=false) v "
                    f"JOIN (SELECT DISTINCT series_id, metric_name, attrs "
                    f"FROM read_parquet({series}, union_by_name=true, "
                    "hive_partitioning=false)) s USING (series_id) "
                    "WHERE s.metric_name = 'latency' GROUP BY ALL ORDER BY ALL"
                ).fetchall()
                self.assertEqual(
                    buckets, [("one", [[1, 1], [2, 1]]), ("two", [[1, 1], [2, 1]])]
                )
                # No trace of the deleted attribute anywhere: every column of
                # every row of every file, identity bytes included, and every
                # file's key-value metadata.
                forbidden = ["request.id"] + ids
                for path in files:
                    rendered = repr(
                        db.execute(
                            # Rendered in SQL: a timestamp with a time zone
                            # cannot be fetched without pytz, and a BLOB
                            # renders its printable bytes as text.
                            "SELECT COLUMNS(*)::VARCHAR FROM "
                            f"read_parquet({sql_string(path)})"
                        ).fetchall()
                    ) + repr(
                        db.execute(
                            "SELECT key, value FROM "
                            f"parquet_kv_metadata({sql_string(path)})"
                        ).fetchall()
                    )
                    for word in forbidden:
                        self.assertNotIn(word, rendered, f"{word} survives in {path}")
                        self.assertNotIn(
                            word.encode(), path.read_bytes(), f"{word} in {path}"
                        )
            # ClickHouse reads the same files and computes the same sums.
            with clickhouse_reader(root) as clickhouse:
                ch_pairs = clickhouse(
                    "SELECT s.metric_name, s.attrs['case'], count(), "
                    "uniqExact(v.series_id), uniqExact(v.time_unix_nano), "
                    "min(v.time_unix_nano), sum(v.value_int), sum(v.count), "
                    "sum(v.sum), arraySort(groupArray(v.value_int)) "
                    "FROM file('v=1/signal=metrics/dataset=values/**/*.parquet', "
                    "'Parquet') AS v INNER JOIN (SELECT DISTINCT series_id, "
                    "metric_name, attrs FROM "
                    "file('v=1/signal=metrics/dataset=series/**/*.parquet', "
                    "'Parquet')) AS s ON v.series_id = s.series_id "
                    "GROUP BY 1, 2 ORDER BY 1, 2"
                )
            # Both readers return null for a sum over only nulls; ClickHouse's
            # groupArray skips nulls where DuckDB's list keeps them.
            ch_expected = [
                row[:9] + ([] if row[0] == "latency" else row[9],) for row in expected
            ]
            self.assertEqual([tuple(row) for row in ch_pairs], ch_expected)


def bulk_log_request(tag, records):
    """An OTLP logs request whose bodies are `tag`-0 .. `tag`-(records-1).

    The bulk requests are large enough that writing the block they fill takes
    long enough to be observed through the engine's telemetry, which is how
    the shutdown test establishes that a block really is being written. Every
    body carries its tag, so the read-back can check each request separately.
    """
    req = logs_pb.ExportLogsServiceRequest()
    resource = req.resource_logs.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    scope = resource.scope_logs.add()
    scope.scope.name = "series-e2e"
    for n in range(records):
        record = scope.log_records.add(time_unix_nano=1789960500000000000 + n)
        record.body.string_value = f"{tag}-{n}"
        record.attributes.add(key="logger.name").value.string_value = "series.logger"
    return req


class ShutdownSlice(unittest.TestCase):
    """Two blocks in flight when the admin endpoint stops the engine."""

    # Scenario: three requests are enqueued at once. Two of them are bulk
    # requests that fill a two-request block whose write is still running, and
    # the third opens the next block. The admin shutdown endpoint is called
    # only once the engine's own telemetry reports a non-empty FLUSHING block
    # and a non-empty ACTIVE block at the same moment. The rotation window is
    # ten minutes, so nothing but the shutdown can seal that second block.
    # Guarantees: every producer is decided, and refusals are retryable
    # `UNAVAILABLE` rather than anything a client would treat as final or as a
    # rejection of its data. Every acknowledged request is stored whole. A
    # refused request may also be stored -- delivery is at least once, and a
    # write that completed after its decision was taken is legitimately there
    # to be rewritten by the retry -- but if it is stored it is stored whole,
    # so no block is ever half written.
    #
    # The admin timeout is deliberately short. The receiver holds its OTLP
    # response until the exporter decides the request and drains its ingress
    # before the exporter is handed the Shutdown control message, so with a
    # window this long the two wait for each other until that drain gives up;
    # see the task 11 report. The test asserts the contract rather than which
    # side of that race each request lands on.
    def test_shutdown_drains_two_blocks_in_flight(self):
        bodies = {"bulka": 40000, "bulkb": 40000, "single": 1}
        with tempfile.TemporaryDirectory() as directory, Engine(
            directory,
            overrides={"window": {"interval": "600s", "max_requests_per_block": 2}},
            telemetry_interval="50ms",
        ) as engine:
            try:
                calls = {
                    tag: engine.logs.Export.future(
                        bulk_log_request(tag, records), timeout=120
                    )
                    for tag, records in bodies.items()
                }

                # The gate is the engine itself reporting a block being written
                # while another is open. Without it the test would only assert
                # that a sequence of committed blocks survives.
                deadline = time.monotonic() + 60
                gauges = {}
                while time.monotonic() < deadline:
                    gauges = engine.exporter_gauges(
                        "block.flushing", "block.active"
                    )
                    if all(
                        value is not None and value > 0 for value in gauges.values()
                    ):
                        break
                    time.sleep(0.01)
                else:
                    self.fail(f"the two blocks were never both in flight: {gauges}")

                try:
                    engine.shutdown(seconds=20)
                except urllib.error.HTTPError as error:
                    # The drain deadline is reached rather than the drain
                    # completing, which is exactly the case this test is for:
                    # the engine gives up waiting and the exporter refuses
                    # whatever it could not make durable. Every producer must
                    # still be decided, which is what follows.
                    self.assertEqual(error.code, 504, error.read().decode())

                acked, refused = [], []
                for tag, call in calls.items():
                    try:
                        call.result(timeout=120)
                        acked.append(tag)
                    except grpc.RpcError as error:
                        # The only refusal this pipeline may produce is the
                        # retryable one: the producer still holds the only copy
                        # of rows that are not durable, so it has to be told to
                        # retry. INVALID_ARGUMENT would claim its data was bad,
                        # INTERNAL would claim a permanent server failure, and
                        # DEADLINE_EXCEEDED or CANCELLED would mean the request
                        # was never decided at all.
                        self.assertEqual(
                            error.code(),
                            grpc.StatusCode.UNAVAILABLE,
                            f"{tag} was refused as {error.code()}: {error.details()}",
                        )
                        refused.append(tag)
                self.assertEqual(
                    sorted(acked + refused),
                    sorted(bodies),
                    "every producer is decided, one way or the other",
                )
                # The block that was already being written when shutdown began
                # reaches storage, so both of its requests are acknowledged:
                # shutdown finishes the outstanding FLUSHING block.
                self.assertGreaterEqual(
                    len(acked),
                    2,
                    f"the flushing block was not acknowledged; refused {refused}",
                )

                values = list(
                    engine.data.glob("v=1/signal=logs/dataset=values/**/*.parquet")
                )
                self.assertTrue(values, "no values file after shutdown")
                with duckdb.connect() as db:
                    stored = {
                        tag: (rows, distinct)
                        for tag, rows, distinct in db.execute(
                            "SELECT split_part(body, '-', 1), count(*), "
                            "count(DISTINCT body) FROM read_parquet(?) GROUP BY 1",
                            [[str(path) for path in values]],
                        ).fetchall()
                    }
                self.assertEqual(
                    set(stored) - set(bodies), set(), f"unexpected bodies: {stored}"
                )
                for tag, expected in bodies.items():
                    rows, distinct = stored.get(tag, (0, 0))
                    if tag in acked:
                        self.assertEqual(
                            (rows, distinct),
                            (expected, expected),
                            f"{tag} was acknowledged but is not stored whole",
                        )
                    else:
                        # Allowed to be there: a write that completed after its
                        # decision was taken is a duplicate the producer's
                        # retry will overwrite, not a loss. What is forbidden
                        # is a partially written block.
                        self.assertIn(
                            (rows, distinct),
                            [(0, 0), (expected, expected)],
                            f"{tag} was refused and is stored only in part",
                        )
            except Exception:
                print(engine.engine_log())
                raise


IMAGE_DEFAULTS = {
    "minio": "minio/minio:RELEASE.2025-04-22T22-12-26Z",
    "rustfs": "rustfs/rustfs:1.0.0-rc.3",
    "clickhouse": "clickhouse/clickhouse-server:26.7.4",
    "alloy": "grafana/alloy:v1.19.2",
    # Azurite 3.37.0, pinned by digest: its only tags are mutable.
    "azurite": (
        "mcr.microsoft.com/azure-storage/azurite@sha256:"
        "830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5"
    ),
}


def unavailable(reason):
    """Skip, unless the runner demands that the containers be present.

    A developer without Docker still gets a green suite; a lane that sets
    `SERIES_REQUIRE_DOCKER=1` gets a failure instead, so the container tests
    cannot silently stop running where they are supposed to run.
    """
    if os.environ.get("SERIES_REQUIRE_DOCKER") == "1":
        raise AssertionError(reason)
    raise unittest.SkipTest(reason)


def require_docker_image(kind):
    """The image tag to run, once Docker and the image are known to be there."""
    if not shutil.which("docker"):
        unavailable("Docker CLI absent")
    try:
        probe = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
    except subprocess.TimeoutExpired:
        unavailable("Docker daemon did not respond within 10 seconds")
    if probe.returncode:
        unavailable("Docker daemon unavailable")
    image = os.environ.get("SERIES_" + kind.upper() + "_IMAGE", IMAGE_DEFAULTS[kind])
    present = (
        subprocess.run(
            ["docker", "image", "inspect", image], capture_output=True, timeout=10
        ).returncode
        == 0
    )
    if not present and kind == "alloy":
        # Alloy is the only image this plan authorizes the test runner to pull.
        try:
            pull = subprocess.run(
                ["docker", "pull", image], capture_output=True, text=True, timeout=180
            )
            present = pull.returncode == 0
        except subprocess.TimeoutExpired:
            present = False
    if not present:
        unavailable(f"Selected local {kind} image is absent: {image}")
    return image


def require_clickhouse():
    """A `clickhouse-local` binary, or None when the reader runs in Docker."""
    binary = os.environ.get("SERIES_CLICKHOUSE_LOCAL", "/usr/bin/clickhouse-local")
    if Path(binary).is_file() and os.access(binary, os.X_OK):
        return binary
    binary = (
        shutil.which("clickhouse-local")
        if "SERIES_CLICKHOUSE_LOCAL" not in os.environ
        else None
    )
    if binary:
        return binary
    require_docker_image("clickhouse")
    return None


# The per-attempt client timeout the tests give Alloy. The shipped config
# resolves to 180s, which is sized for a 15s production window and a 60s flush
# deadline; these tests run a one-second window and want an expired attempt to
# be visible inside their own waits, so they set the value explicitly through
# `SERIES_ALLOY_TIMEOUT` rather than inheriting whatever the config ships.
ALLOY_ATTEMPT_TIMEOUT = "6s"

# The reference River config, the `host.id` the fixture producer stamps
# through its SERIES_PRODUCER_ID, and the stability level the reference
# config's file-backed sending queue needs.
ALLOY_REFERENCE_CONFIG = "configs/series-parquet.alloy"
ALLOY_PRODUCER_ID = "alloy-producer"
# The fixture's site values: the file the tests append to, as mounted in the
# container, and the service name of its lines.
ALLOY_LOG_PATH = "/input/events.log"
ALLOY_SERVICE_NAME = "series-e2e-service"
# The attribute the read-back finds every Alloy row by; the shipped configs
# carry no such attribute, so the fixture inserts it.
ALLOY_SOURCE = ("e2e.source", "alloy-file")


def e2e_alloy_config(text):
    """A shipped River config with the fixture's `e2e.source` stage between
    its transform and its batch."""
    route = "logs = [otelcol.processor.batch.series.input]"
    if text.count(route) != 1:
        raise AssertionError("the River config no longer routes transform -> batch -> exporter")
    key, value = ALLOY_SOURCE
    transform_output = text.index(route)
    return (
        text[:transform_output] + "logs = [otelcol.processor.attributes.e2e.input]"
        + text[transform_output + len(route):]
        + f"""
otelcol.processor.attributes "e2e" {{
  action {{
    key = "{key}"
    value = "{value}"
    action = "insert"
  }}
  output {{
    {route}
  }}
}}
"""
    )
ALLOY_STABILITY_LEVEL = "public-preview"


class AlloyProducer:
    """Grafana Alloy tailing a file into the engine's OTLP gRPC receiver.

    The container runs on the host network so that it can reach a receiver
    bound to loopback, and the reference River config the repository ships is
    the one it runs, with one stage added (`e2e_alloy_config`) that marks its
    rows: the test exercises the documented deployment rather than a fixture
    of its own. The site values are set through the variables the config
    reads (`SERIES_LOG_PATH`, `SERIES_SERVICE_NAME`, `SERIES_PRODUCER_ID`,
    `SERIES_ALLOY_TIMEOUT`), so that the test owns the values its timing and
    its read-back depend on.
    """

    def __init__(self, directory, engine, timeout=ALLOY_ATTEMPT_TIMEOUT, *,
                 admin_port=None, config=None, producer_id=None):
        self.root = Path(directory) / "alloy"
        self.engine = engine
        self.timeout = timeout
        # The River file this producer runs, the buffered reference by
        # default, and the `host.id` it stamps through SERIES_PRODUCER_ID.
        self.config = Path(config or WORKSPACE / ALLOY_REFERENCE_CONFIG)
        self.producer_id = producer_id or ALLOY_PRODUCER_ID
        self.name = "series-alloy-" + uuid.uuid4().hex
        self.container = None
        # Alloy's own HTTP server, which serves readiness and its metrics.
        # A measurement may choose the port; otherwise a free one is taken.
        self.admin_port = admin_port

    def __enter__(self):
        if not sys.platform.startswith("linux"):
            unavailable("Alloy host-network fixture requires Linux")
        image = require_docker_image("alloy")
        self.root.mkdir(mode=0o755)
        self.lines = self.root / "events.log"
        self.lines.write_text("")
        config = self.root / "config.alloy"
        config.write_text(e2e_alloy_config(self.config.read_text()))
        port = self.admin_port or free_port()
        self.admin_port = port
        args = [
            "docker", "run", "--pull=never", "--detach", "--name", self.name,
            "--network", "host", "--user", f"{os.getuid()}:{os.getgid()}",
            "--mount", f"type=bind,src={self.root.resolve()},dst=/input,readonly",
            "-e", f"OTLP_ENDPOINT=127.0.0.1:{self.engine.grpc_port}",
            "-e", f"SERIES_ALLOY_TIMEOUT={self.timeout}",
            "-e", f"SERIES_PRODUCER_ID={self.producer_id}",
            "-e", f"SERIES_LOG_PATH={ALLOY_LOG_PATH}",
            "-e", f"SERIES_SERVICE_NAME={ALLOY_SERVICE_NAME}",
            image, "run", f"--stability.level={ALLOY_STABILITY_LEVEL}",
            "--storage.path=/tmp/alloy-state",
            f"--server.http.listen-addr=127.0.0.1:{port}", "/input/config.alloy",
        ]
        try:
            self.container = subprocess.check_output(args, text=True, timeout=30).strip()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                state = subprocess.check_output(
                    [
                        "docker", "inspect", "--format", "{{.State.Running}}",
                        self.container,
                    ],
                    text=True,
                    timeout=10,
                ).strip()
                if state != "true":
                    raise AssertionError(
                        "Alloy exited before becoming ready:\n" + self.logs()
                    )
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{port}/-/ready", timeout=1
                    ) as response:
                        if response.status == 200:
                            return self
                except (OSError, urllib.error.URLError):
                    pass
                time.sleep(0.1)
            raise AssertionError("Alloy readiness deadline exceeded")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def logs(self):
        """Everything the Alloy container has written, for a failure message."""
        if not self.container:
            return ""
        done = subprocess.run(
            ["docker", "logs", self.container],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return done.stdout + done.stderr

    def host_pid(self):
        """The host PID of the Alloy process inside its container.

        Docker reports the container's init process as seen from the host,
        which is Alloy itself because the image runs it directly. RSS and
        affinity are read from this PID, never from the Docker CLI.
        """
        if not self.container:
            raise AssertionError("Alloy is not running")
        pid = int(
            subprocess.check_output(
                ["docker", "inspect", "--format", "{{.State.Pid}}", self.container],
                text=True,
                timeout=10,
            ).strip()
        )
        if pid <= 0:
            raise AssertionError(f"Alloy container reports host pid {pid}")
        return pid

    # The sending-queue series Alloy's exporter publishes. `send_failed` is
    # deliberately not among the reliable ones: with the shipped infinite
    # retry it counts only sends the exporter gave up on, which is none.
    QUEUE_METRICS = {
        "otelcol_exporter_queue_size": "queue_size_records",
        "otelcol_exporter_queue_capacity": "queue_capacity_records",
        "otelcol_exporter_enqueue_failed_log_records": "enqueue_failed_records",
    }

    def queue_metrics(self):
        """Alloy's sending-queue occupancy, capacity and enqueue failures.

        Read from Alloy's own metrics endpoint and restricted to the series
        exporter's logs queue. A series Alloy does not publish -- the enqueue
        failure counter appears only after a first failure -- is reported as
        None, never as zero. `send_failed_log_records` is returned for reference only,
        under a name that says it is not a loss counter.
        """
        url = f"http://127.0.0.1:{self.admin_port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as response:
            body = response.read().decode("ascii", "replace")
        found = dict.fromkeys(self.QUEUE_METRICS.values())
        found["send_failed_records_unreliable"] = None
        for line in body.splitlines():
            # The exporter keeps one queue per data type; this producer only
            # sends logs, so the logs queue is the one it fills.
            if (
                line.startswith("#")
                or "otelcol.exporter.otlp.series" not in line
                or 'data_type="logs"' not in line
            ):
                continue
            name = line.split("{", 1)[0]
            value = float(line.rsplit(" ", 1)[-1])
            if name in self.QUEUE_METRICS:
                key = self.QUEUE_METRICS[name]
                found[key] = (found[key] or 0) + value
            elif name == "otelcol_exporter_send_failed_log_records":
                found["send_failed_records_unreliable"] = (
                    found["send_failed_records_unreliable"] or 0
                ) + value
        return found

    def export_failures(self):
        """Alloy's own record of an export it tried and the engine refused.

        The shipped River config retries forever
        (`max_elapsed_time = "0s"`), so the collector's
        `otelcol_exporter_send_failed_log_records` counter stays at zero: it
        only counts a send the exporter gave up on. What Alloy does report on
        every refused attempt is this line, which carries the gRPC status the
        engine returned, so it proves both that Alloy reached the receiver and
        what it was told.
        """
        return [
            line
            for line in self.logs().splitlines()
            if "Exporting failed" in line
            and "component_id=otelcol.exporter.otlp.series" in line
        ]

    @staticmethod
    def client_deadline(line):
        """Whether one `Exporting failed` line is Alloy's own attempt deadline.

        Such an attempt ended on the client before the engine answered, so it
        carries no status the engine returned. gRPC-Go reports it as
        DeadlineExceeded, or as Canceled "Timeout expired" when the timer
        fires before the stream reports the deadline, which a loaded host
        makes likely.
        """
        return "code = DeadlineExceeded" in line or (
            "code = Canceled" in line and "Timeout expired" in line
        )

    def write(self, ids):
        """Append one line per id and make the bytes visible to the tail."""
        with self.lines.open("a") as stream:
            for item in ids:
                if "\n" in item:
                    raise ValueError("each expected body must be one log line")
                stream.write(item + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def __exit__(self, *exc):
        if self.container:
            try:
                subprocess.run(
                    ["docker", "stop", "--time", "10", self.container],
                    check=True,
                    capture_output=True,
                    timeout=20,
                )
                (self.root / "alloy.log").write_text(self.logs())
            finally:
                subprocess.run(
                    ["docker", "rm", "--force", "--volumes", self.container],
                    check=False,
                    capture_output=True,
                    timeout=20,
                )
                self.container = None


def wait_for_alloy(store, directory, ids, timeout=90):
    """Wait until every line Alloy was given is readable from the store.

    Alloy owns its own file positions and sending queue, so delivery is
    eventual from the test's point of view; the engine's own acknowledgement
    only covers the requests the test itself sent.
    """
    target = Path(directory) / "downloaded"
    deadline = time.monotonic() + timeout
    actual = set()
    while time.monotonic() < deadline:
        store.download(target)
        paths = sorted((target / "v=1/signal=logs/dataset=values").rglob("*.parquet"))
        if paths:
            with duckdb.connect() as db:
                actual = {
                    row[0]
                    for row in db.execute(
                        "SELECT body FROM read_parquet(?, union_by_name=true)",
                        [[str(path) for path in paths]],
                    ).fetchall()
                }
            if set(ids).issubset(actual):
                return
        time.sleep(0.2)
    missing = sorted(set(ids) - actual)
    raise AssertionError(f"Alloy rows did not become durable: {missing}")


def wait_for_local_bodies(engine, count, timeout=90):
    """Wait until the local store holds `count` logs values rows; return their bodies."""
    deadline = time.monotonic() + timeout
    bodies = []
    while time.monotonic() < deadline:
        paths = sorted((engine.data / "v=1/signal=logs/dataset=values").rglob("*.parquet"))
        if paths:
            with duckdb.connect() as db:
                bodies = [
                    row[0]
                    for row in db.execute(
                        "SELECT body FROM read_parquet(?, union_by_name=true)",
                        [[str(path) for path in paths]],
                    ).fetchall()
                ]
            if len(bodies) >= count:
                return bodies
        time.sleep(0.5)
    raise AssertionError(f"{len(bodies)} of {count} Alloy rows became durable")


@contextlib.contextmanager
def clickhouse_reader(root):
    """A `query(sql)` callable backed by clickhouse-local over `root`."""
    root = Path(root).resolve()
    binary = require_clickhouse()
    container = None
    try:
        if binary:
            prefix = [binary]
        else:
            image = require_docker_image("clickhouse")
            name = "series-reader-" + uuid.uuid4().hex
            container = subprocess.check_output(
                [
                    "docker", "run", "--pull=never", "--detach", "--name", name,
                    "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
                    "--mount", f"type=bind,src={root},dst=/data,readonly",
                    "--entrypoint", "/bin/sleep", image, "infinity",
                ],
                text=True,
                timeout=30,
            ).strip()
            prefix = [
                "docker", "exec", "--workdir", "/data", container, "clickhouse", "local",
            ]

        def query(sql):
            result = subprocess.run(
                prefix
                + [
                    "--query",
                    sql + " FORMAT JSONCompactEachRow",
                    "--output_format_json_quote_64bit_integers=0",
                ],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            return [
                tuple(json.loads(line))
                for line in result.stdout.splitlines()
                if line.strip()
            ]

        yield query
    finally:
        if container:
            subprocess.run(
                ["docker", "rm", "--force", "--volumes", container],
                check=False,
                capture_output=True,
                timeout=20,
            )


def sql_string(value):
    """Quote a path as a SQL string literal for both readers."""
    return "'" + str(value).replace(chr(92), chr(92) * 2).replace("'", "''") + "'"


def alloy_producer_id():
    """The `host.id` the fixture's Alloy producer stamps on its resource.

    The reference config takes it from SERIES_PRODUCER_ID, which
    `AlloyProducer` sets; the check that the config still reads that
    variable keeps the fixture from drifting away from the deployment.
    """
    text = (WORKSPACE / ALLOY_REFERENCE_CONFIG).read_text()
    if 'sys.env("SERIES_PRODUCER_ID")' not in text or 'attributes["host.id"]' not in text:
        raise AssertionError("the Alloy config no longer sets host.id from SERIES_PRODUCER_ID")
    return ALLOY_PRODUCER_ID


# Columns every dataset must contribute to the canonical row, checked against
# the columns the files actually carry so the comparison cannot quietly shrink.
REQUIRED_COLUMNS = {
    ("logs", "values"): {
        "series_id", "producer_id", "time", "time_unix_nano", "observed_time",
        "observed_time_unix_nano", "severity_number", "severity_text", "body",
        "event_name", "trace_id", "span_id", "flags", "attrs", "service_name",
    },
    # One merged metrics values dataset: the union of the number and the
    # histogram columns, each kind leaving the other's columns null.
    ("metrics", "values"): {
        "series_id", "producer_id", "metric_name", "time", "time_unix_nano",
        "start_time", "start_time_unix_nano", "flags", "value_int",
        "value_double", "count", "sum", "min", "max", "bucket_counts",
        "explicit_bounds", "service_name",
    },
    ("logs", "series"): {
        "series_id", "identity_bytes", "emitted_at", "resource_schema_url",
        "resource_attrs", "scope_name", "scope_version", "scope_schema_url",
        "scope_attrs", "attrs", "service_name",
    },
    ("metrics", "series"): {
        "series_id", "identity_bytes", "emitted_at", "resource_schema_url",
        "resource_attrs", "scope_name", "scope_version", "scope_schema_url",
        "scope_attrs", "attrs", "metric_name", "unit", "metric_type",
        "temporality", "is_monotonic", "description", "service_name",
    },
}


def canonical_column(kind, column):
    """DuckDB and ClickHouse expressions that render one column identically.

    Every rendering is a string and never NULL, so the per-row strings the two
    readers build can be compared directly. Doubles are rendered as their
    IEEE-754 bit pattern, most significant bit first, rather than as formatted
    decimals: the two engines print floating point differently, and both read
    the same Parquet bytes, so the comparison is exact down to the sign of a
    zero.

    List columns are the one place the two readers cannot be made to agree:
    ClickHouse has no nullable Array, so it reads a null Parquet list as an
    empty array while DuckDB keeps it NULL. Both renderings therefore collapse
    a null list and an empty list to the empty string. The distinction that
    loses -- a number row's null `bucket_counts` against a distribution-less
    histogram's empty one -- is asserted directly against the files in
    `verify_files` and in `MetricsSlice.test_number_and_histogram`.
    """
    if kind == "VARCHAR":
        return f"coalesce({column}, 'null')", f"ifNull({column}, 'null')"
    if kind in ("BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT", "UBIGINT"):
        return (
            f"coalesce(CAST({column} AS VARCHAR), 'null')",
            f"ifNull(toString({column}), 'null')",
        )
    if kind == "BOOLEAN":
        case = (
            f"CASE WHEN {column} IS NULL THEN 'null' "
            f"WHEN {column} THEN 'true' ELSE 'false' END"
        )
        return case, case
    if kind == "DOUBLE":
        return (
            f"coalesce(CAST(CAST({column} AS BIT) AS VARCHAR), 'null')",
            f"if({column} IS NULL, 'null', "
            f"bin(reverse(reinterpretAsFixedString(assumeNotNull({column})))))",
        )
    if kind == "BLOB":
        return f"coalesce(hex({column}), 'null')", f"ifNull(hex({column}), 'null')"
    if kind.startswith("TIMESTAMP"):
        return (
            f"coalesce(CAST(epoch_us({column}) AS VARCHAR), 'null')",
            f"ifNull(toString(toUnixTimestamp64Micro({column})), 'null')",
        )
    if kind == "MAP(VARCHAR, VARCHAR)":
        return (
            f"coalesce(array_to_string(list_transform(list_sort(map_keys({column})), "
            f"k -> k || '=' || coalesce(list_extract(map_extract({column}, k), 1), "
            "'null')), ','), 'null')",
            f"ifNull(arrayStringConcat(arrayMap(k -> concat(k, '=', "
            f"ifNull(toString({column}[k]), 'null')), arraySort(mapKeys({column}))), "
            "','), 'null')",
        )
    if kind in ("BIGINT[]", "INTEGER[]"):
        return (
            f"coalesce(array_to_string(list_transform({column}, "
            "x -> coalesce(CAST(x AS VARCHAR), 'null')), ','), '')",
            f"ifNull(arrayStringConcat(arrayMap(x -> ifNull(toString(x), 'null'), "
            f"{column}), ','), '')",
        )
    if kind == "DOUBLE[]":
        return (
            f"coalesce(array_to_string(list_transform({column}, "
            "x -> coalesce(CAST(CAST(x AS BIT) AS VARCHAR), 'null')), ','), '')",
            "ifNull(arrayStringConcat(arrayMap(x -> if(isNull(x), 'null', "
            f"bin(reverse(reinterpretAsFixedString(assumeNotNull(x))))), {column}), "
            "','), '')",
        )
    raise AssertionError(f"no canonical rendering for {kind} column {column}")


def canonical_row(db, relation, alias, required, skip=("filename",)):
    """One expression per reader that renders a whole row as a string.

    The column list comes from the files themselves, so a column the exporter
    adds is compared from the day it appears; `required` is what must be in
    that list, so a projection that silently loses columns fails instead.
    """
    columns = [
        (name, kind)
        for name, kind, *_ in db.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        if name not in skip
    ]
    names = {name for name, _ in columns}
    missing = set(required) - names
    if missing:
        raise AssertionError(f"{relation} is missing columns {sorted(missing)}")
    duck, clickhouse = [], []
    for name, kind in sorted(columns):
        one, other = canonical_column(kind, f"{alias}.{name}")
        duck.append(f"'{name}=' || {one}")
        clickhouse.append(f"'{name}=' || {other}")
    return " || '|' || ".join(duck), " || '|' || ".join(clickhouse)


def verify_readers(
    test,
    root,
    log_ids,
    metric_count,
    allow_duplicates=False,
    alloy_ids=(),
    metric_ids=(),
):
    """Two independent readers must agree with each other and the producer.

    DuckDB reads the files in process and ClickHouse reads the same files
    through `clickhouse-local`, both with the latest-descriptor window join
    the crate README documents. The comparison is over every column of the
    values row and of the descriptor it joins to, rendered into one string per
    row by expressions written separately for each engine, so a disagreement
    anywhere in the row is caught rather than only in the few columns a test
    happens to name. A disagreement is a defect in what was written, not in
    one reader.
    """
    root = Path(root).resolve()
    alloy_ids = set(alloy_ids)
    producer = alloy_producer_id()
    with duckdb.connect() as db, clickhouse_reader(root) as clickhouse:
        for signal, dataset in (("logs", "values"), ("metrics", "values")):
            relative = f"v=1/signal={signal}/dataset={dataset}/**/*.parquet"
            paths = sorted(root.glob(relative))
            # Every metric request carries one number and one histogram point,
            # and both now land in the same values dataset.
            expected_count = (
                len(log_ids) if signal == "logs" else POINTS_PER_METRIC * metric_count
            )
            if not paths:
                test.assertEqual(expected_count, 0, f"missing {signal}/{dataset}")
                continue
            series = f"v=1/signal={signal}/dataset=series/**/*.parquet"
            # Hive partitioning is turned off so that both readers see the
            # same columns: ClickHouse's file() does not synthesize `date` and
            # `hour` from the path, and the partition values are checked
            # against the file names in verify_files instead.
            duck_values = (
                f"read_parquet({sql_string(root / relative)}, union_by_name=true, "
                "hive_partitioning=false)"
            )
            duck_series = (
                f"read_parquet({sql_string(root / series)}, union_by_name=true, "
                "filename=true, hive_partitioning=false)"
            )
            ch_values = f"file({sql_string(relative)}, 'Parquet')"
            ch_series = f"file({sql_string(series)}, 'Parquet')"
            duck_value_row, ch_value_row = canonical_row(
                db, duck_values, "v", REQUIRED_COLUMNS[(signal, dataset)]
            )
            duck_series_row, ch_series_row = canonical_row(
                db, duck_series, "s", REQUIRED_COLUMNS[(signal, "series")]
            )
            duck_sql = f"""
                WITH canonical AS (
                    SELECT * FROM {duck_series}
                    QUALIFY row_number() OVER (
                        PARTITION BY series_id
                        ORDER BY emitted_at DESC, filename DESC) = 1
                )
                SELECT {{projection}} FROM {duck_values} v
                INNER JOIN canonical s ON v.series_id = s.series_id
            """
            ch_sql = f"""
                WITH canonical AS (
                    SELECT * FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY series_id
                            ORDER BY emitted_at DESC, _path DESC) AS rank
                        FROM {ch_series}
                    ) WHERE rank = 1
                )
                SELECT {{projection}} FROM {ch_values} AS v
                INNER JOIN canonical AS s ON v.series_id = s.series_id
            """
            whole = f"{duck_value_row} || '|' || {duck_series_row}"
            ch_whole = f"{ch_value_row} || '|' || {ch_series_row}"
            duck_all = sorted(
                row[0]
                for row in db.execute(duck_sql.format(projection=whole)).fetchall()
            )
            ch_all = sorted(
                row[0] for row in clickhouse(ch_sql.format(projection=ch_whole))
            )
            test.assertEqual(
                len(ch_all),
                len(duck_all),
                f"row count disagreement for {signal}/{dataset}",
            )
            for one, other in zip(duck_all, ch_all):
                test.assertEqual(
                    other, one, f"reader disagreement for {signal}/{dataset}"
                )
            # The expectations the producer itself can state: what was sent,
            # which producer sent it, and that the descriptor carries the same
            # resource identity the values row was stamped with.
            projection = (
                "v.body, coalesce(v.attrs['e2e.source'], ''), v.producer_id, "
                "coalesce(list_extract(map_extract(s.resource_attrs, 'host.id'), 1), '')"
                if signal == "logs"
                else "s.attrs['request.id'], v.producer_id, "
                "coalesce(list_extract(map_extract(s.resource_attrs, 'host.id'), 1), '')"
            )
            duck_rows = sorted(
                db.execute(duck_sql.format(projection=projection)).fetchall()
            )
            before = db.execute(f"SELECT count(*) FROM {duck_values}").fetchone()[0]
            ch_before = int(clickhouse(f"SELECT count(*) FROM {ch_values}")[0][0])
            test.assertEqual(ch_before, before)
            test.assertEqual(
                len(duck_rows),
                before,
                "latest-descriptor join must preserve cardinality",
            )
            test.assertEqual(
                len(duck_all), before, "the whole-row comparison skipped rows"
            )
            if allow_duplicates:
                test.assertGreaterEqual(before, expected_count)
            else:
                test.assertEqual(before, expected_count)
            for row in duck_rows:
                test.assertTrue(row[-2], f"empty producer_id in {signal}/{dataset}")
                test.assertEqual(
                    row[-1], row[-2], "descriptor and values disagree on host.id"
                )
            if signal == "logs":
                expected = sorted(
                    (
                        body,
                        "alloy-file" if body in alloy_ids else "",
                        producer if body in alloy_ids else "producer-1",
                        producer if body in alloy_ids else "producer-1",
                    )
                    for body in log_ids
                )
                if allow_duplicates:
                    test.assertEqual(set(duck_rows), set(expected))
                else:
                    test.assertEqual(duck_rows, expected)
            elif metric_ids:
                test.assertEqual({row[0] for row in duck_rows}, set(metric_ids))


# Upper bound on any single docker CLI call, so a wedged daemon fails the test
# instead of hanging it.
DOCKER_TIMEOUT_S = 120


class DockerStore:
    """A MinIO or RustFS container serving S3 on a loopback port.

    The store keeps its data in its own container across a stop and start,
    which is what makes an outage recoverable; the exporter itself still holds
    nothing across a restart.
    """

    def __init__(self, kind, *, by_image_id=False):
        self.kind = kind
        # With `by_image_id` the container starts from the image id the tag
        # resolved to when inspected, so a tag moved in between cannot change
        # what runs. The legacy suite keeps starting from the tag.
        self.by_image_id = by_image_id
        self.image = None
        self.image_id = None
        self.name = "series-e2e-" + uuid.uuid4().hex
        self.container = None
        self.bucket = "series-test"
        self.key = "series-test-access"
        self.secret = "series-test-secret-12345"

    def run_args(self, image):
        """The `docker run` command line for the current port."""
        args = [
            "docker", "run", "--pull=never", "--detach", "--name", self.name,
            "--publish", f"127.0.0.1:{self.port}:9000",
        ]
        if self.kind == "minio":
            return args + [
                "-e", f"MINIO_ROOT_USER={self.key}",
                "-e", f"MINIO_ROOT_PASSWORD={self.secret}",
                image, "server", "/data", "--address", ":9000",
            ]
        return args + [
            "-e", f"RUSTFS_ACCESS_KEY={self.key}",
            "-e", f"RUSTFS_SECRET_KEY={self.secret}",
            "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
            "-e", "RUSTFS_VOLUMES=/data",
            image,
        ]

    def start_container(self, image, attempts=3):
        """Run the container, retrying on a port another process took.

        The host port is chosen here rather than by Docker: a container that
        is stopped and started again keeps an explicit mapping, so the
        engine's configured endpoint stays valid across an outage. A failed
        `docker run` can still leave a created container behind under the
        name, so every failure removes it by name before the next attempt.
        """
        for attempt in range(attempts):
            self.port = free_port()
            done = subprocess.run(
                self.run_args(image), capture_output=True, text=True,
                timeout=DOCKER_TIMEOUT_S,
            )
            if done.returncode == 0:
                self.container = done.stdout.strip()
                return
            self.remove()
            taken = "already allocated" in done.stderr or "in use" in done.stderr
            if not taken or attempt + 1 == attempts:
                raise AssertionError(
                    f"docker run of {self.kind} failed: {done.stderr.strip()}"
                )

    def __enter__(self):
        image = require_docker_image(self.kind)
        self.image = image
        if self.by_image_id:
            inspected = subprocess.run(
                ["docker", "image", "inspect", "--format", "{{.Id}}", image],
                capture_output=True, text=True, timeout=DOCKER_TIMEOUT_S,
            )
            self.image_id = inspected.stdout.strip() or None
            if inspected.returncode or not self.image_id:
                unavailable(f"the {self.kind} image {image} could not be inspected")
            image = self.image_id
        try:
            self.start_container(image)
            mapping = subprocess.check_output(
                ["docker", "port", self.container, "9000/tcp"], text=True,
                timeout=DOCKER_TIMEOUT_S,
            ).strip()
            if f":{self.port}" not in mapping:
                raise AssertionError(
                    f"{self.kind} did not publish {self.port}: {mapping}"
                )
            self.endpoint = f"http://127.0.0.1:{self.port}"
            self.client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                region_name="us-east-1",
                aws_access_key_id=self.key,
                aws_secret_access_key=self.secret,
                config=BotoConfig(
                    connect_timeout=5,
                    read_timeout=10,
                    retries={"max_attempts": 0},
                    s3={"addressing_style": "path"},
                ),
            )
            self.ready()
            self.client.create_bucket(Bucket=self.bucket)
            self.storage = {
                "s3": {
                    "base_uri": f"s3://{self.bucket}/otel",
                    "region": "us-east-1",
                    "endpoint": self.endpoint,
                    "allow_http": True,
                    "virtual_hosted_style_request": False,
                    "auth": {
                        "type": "static_credentials",
                        "access_key_id": self.key,
                        "secret_access_key": self.secret,
                    },
                }
            }
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def ready(self):
        """Block until the store answers an S3 request, or fail with its log."""
        deadline = time.monotonic() + 60
        last_error = None
        while time.monotonic() < deadline:
            try:
                self.client.list_buckets()
                return
            except Exception as error:
                last_error = error
                time.sleep(0.2)
        logs = subprocess.check_output(
            ["docker", "logs", self.container], stderr=subprocess.STDOUT, text=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        raise AssertionError(f"{self.kind} never became S3-ready: {last_error}\n{logs}")

    def download(self, directory):
        """Copy every completed Parquet object into a local directory."""
        directory = Path(directory)
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix="otel/"
        ):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith(".parquet"):
                    destination = directory / key.removeprefix("otel/")
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    self.client.download_file(self.bucket, key, str(destination))

    def stop(self):
        """Take the store off the network without losing what it holds."""
        subprocess.run(
            ["docker", "stop", "--time", "0", self.container],
            check=True,
            capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )

    def recover(self):
        """Start the same container again and wait for it to serve S3."""
        subprocess.run(
            ["docker", "start", self.container], check=True, capture_output=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        self.ready()

    def attach(self, network, alias):
        """Also attach the store to `network`, reachable there as `alias`.

        Everything else is unchanged: the loopback port stays published, so
        the harness still reads and downloads the store directly. The
        attachment survives `stop` and `recover`, as Docker keeps it in the
        container's configuration.
        """
        subprocess.run(
            ["docker", "network", "connect", "--alias", alias, network,
             self.container],
            check=True, capture_output=True, timeout=DOCKER_TIMEOUT_S,
        )

    def detach(self, network):
        """Undo `attach`; a store that is already gone is not an error."""
        subprocess.run(
            ["docker", "network", "disconnect", "--force", network, self.name],
            check=False, capture_output=True, timeout=DOCKER_TIMEOUT_S,
        )

    def network_address(self, network):
        """The store's IPv4 address on an attached network, or None.

        Read again after every `recover`: a restarted container may be
        given a different address on a user-defined network.
        """
        done = subprocess.run(
            ["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}",
             self.container],
            capture_output=True, text=True, timeout=DOCKER_TIMEOUT_S,
        )
        if done.returncode != 0:
            return None
        networks = json.loads(done.stdout or "{}") or {}
        for name, entry in networks.items():
            if network in (name, (entry or {}).get("NetworkID")):
                return (entry or {}).get("IPAddress") or None
        return None

    def remove(self):
        """Remove the container by name, whether or not it ever started.

        By name rather than by id, so a container `docker run` created but
        could not start -- which returns no id -- is removed too.
        """
        try:
            subprocess.run(
                ["docker", "rm", "--force", "--volumes", self.name],
                check=False,
                capture_output=True,
                timeout=DOCKER_TIMEOUT_S,
            )
        except subprocess.TimeoutExpired:
            pass
        self.container = None

    def __exit__(self, *exc):
        self.remove()


# Azurite, the Azure Storage emulator, serves this one account; the blob
# service is addressed path style under it.
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_API_VERSION = "2021-08-06"


def azure_bearer_token(lifetime_s=3600):
    """A well-formed, unsigned Entra ID access token for Azurite.

    Azurite's `--oauth basic` decodes the token without checking a
    signature, but requires `iat`, `nbf` and `exp` covering now, an issuer
    under `https://sts.windows.net/` and the storage audience; anything else
    is refused as unauthenticated.
    """
    def part(document):
        raw = json.dumps(document, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    now = int(time.time())
    claims = {
        "aud": "https://storage.azure.com",
        "iss": "https://sts.windows.net/00000000-0000-0000-0000-000000000000/",
        "iat": now - 60,
        "nbf": now - 60,
        "exp": now + lifetime_s,
    }
    return part({"alg": "none", "typ": "JWT"}) + "." + part(claims) + ".c2lnbmF0dXJl"


class TokenEndpoint:
    """A loopback OAuth 2.0 token endpoint serving one static access token.

    The engine's `oauth2_client_auth` extension acquires its token here with
    the client credentials grant and publishes it through the
    `bearer_token_provider` capability the Azure store requires. `grants`
    records the grant type of every request served, so a test can show the
    token path was used.
    """

    def __init__(self, token):
        self.token = token
        self.grants = []
        endpoint = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                form = urllib.parse.parse_qs(self.rfile.read(length).decode())
                endpoint.grants.append(form.get("grant_type", [""])[0])
                body = json.dumps(
                    {
                        "access_token": endpoint.token,
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/token"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


class AzuriteStore:
    """An Azurite blob service over HTTPS that accepts only bearer tokens.

    Azurite runs with `--oauth basic` and a certificate for 127.0.0.1 from
    a throwaway CA; the engine trusts that CA through `SSL_CERT_FILE`,
    which the object store's native root loader reads. The engine obtains
    its token from a `TokenEndpoint` through the `oauth2_client_auth`
    extension, the only credential path Azure storage has.
    """

    def __init__(self):
        self.name = "series-e2e-" + uuid.uuid4().hex
        self.container = None
        self.blob_container = "series-test"
        self.token = azure_bearer_token()
        self.certs = None

    def make_certificate(self):
        """A throwaway CA and the 127.0.0.1 server certificate it signs.

        The engine's TLS stack refuses a CA certificate presented as the
        server's own, so a single self-signed certificate is not enough: the
        CA is what the engine trusts, and the leaf, marked `CA:FALSE` for
        server authentication, is what Azurite serves.
        """
        if not shutil.which("openssl"):
            unavailable("openssl is needed to issue the Azurite certificate")
        self.certs = Path(tempfile.mkdtemp(prefix="series-azurite-"))

        def openssl(*args):
            subprocess.run(
                ["openssl", *args], check=True, capture_output=True, timeout=60,
                cwd=self.certs,
            )

        openssl(
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-subj", "/CN=series-e2e-ca", "-keyout", "ca-key.pem", "-out", "ca.pem",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        )
        openssl(
            "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=127.0.0.1",
            "-keyout", "key.pem", "-out", "server.csr",
        )
        (self.certs / "server.ext").write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
            "subjectAltName=IP:127.0.0.1,DNS:localhost\n"
            "subjectKeyIdentifier=hash\n"
            "authorityKeyIdentifier=keyid,issuer\n"
        )
        openssl(
            "x509", "-req", "-in", "server.csr", "-CA", "ca.pem",
            "-CAkey", "ca-key.pem", "-CAcreateserial", "-days", "2",
            "-extfile", "server.ext", "-out", "cert.pem",
        )
        # The container reads the server pair as its own user.
        for name in ("key.pem", "cert.pem"):
            (self.certs / name).chmod(0o644)
        self.ca_file = str(self.certs / "ca.pem")
        self.tls = ssl.create_default_context(cafile=self.ca_file)

    def __enter__(self):
        image = require_docker_image("azurite")
        try:
            self.make_certificate()
            self.port = free_port()
            done = subprocess.run(
                [
                    "docker", "run", "--pull=never", "--detach", "--name", self.name,
                    "--publish", f"127.0.0.1:{self.port}:10000",
                    "--volume", f"{self.certs / 'cert.pem'}:/certs/cert.pem:ro",
                    "--volume", f"{self.certs / 'key.pem'}:/certs/key.pem:ro",
                    image, "azurite-blob", "--blobHost", "0.0.0.0",
                    "--oauth", "basic",
                    "--cert", "/certs/cert.pem", "--key", "/certs/key.pem",
                    "--skipApiVersionCheck",
                ],
                capture_output=True, text=True, timeout=DOCKER_TIMEOUT_S,
            )
            if done.returncode:
                raise AssertionError(f"docker run of azurite failed: {done.stderr.strip()}")
            self.container = done.stdout.strip()
            self.service = f"https://127.0.0.1:{self.port}/{AZURITE_ACCOUNT}"
            self.ready()
            status, body = self.request("PUT", f"/{self.blob_container}?restype=container")
            if status != 201:
                raise AssertionError(f"container creation returned {status}: {body}")
            self.storage = {
                "azure": {
                    "base_uri": (
                        f"https://{AZURITE_ACCOUNT}.blob.core.windows.net/"
                        f"{self.blob_container}/otel"
                    ),
                    "endpoint": self.service,
                }
            }
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def request(self, method, path, token=True):
        """One blob service call; the status and body, error statuses included."""
        headers = {"x-ms-version": AZURITE_API_VERSION}
        if token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(
            self.service + path, method=method, headers=headers,
            data=b"" if method == "PUT" else None,
        )
        try:
            with urllib.request.urlopen(request, timeout=10, context=self.tls) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def ready(self):
        """Block until Azurite answers an authenticated call, or fail with its log."""
        deadline = time.monotonic() + 60
        last = None
        while time.monotonic() < deadline:
            try:
                status, body = self.request("GET", "?comp=list")
                if status == 200:
                    return
                last = f"status {status}: {body[:300]!r}"
            except Exception as error:
                last = error
            time.sleep(0.2)
        logs = subprocess.run(
            ["docker", "logs", self.container], capture_output=True, text=True,
            timeout=DOCKER_TIMEOUT_S,
        )
        raise AssertionError(
            f"azurite never became ready: {last}\n{logs.stdout}{logs.stderr}"
        )

    def blob_names(self, prefix="otel/"):
        """Every blob name under `prefix`, following the listing's markers."""
        names = []
        marker = ""
        while True:
            query = (
                f"/{self.blob_container}?restype=container&comp=list"
                f"&prefix={urllib.parse.quote(prefix)}"
            )
            if marker:
                query += f"&marker={urllib.parse.quote(marker)}"
            status, body = self.request("GET", query)
            if status != 200:
                raise AssertionError(f"blob listing returned {status}: {body[:300]!r}")
            root = xml.etree.ElementTree.fromstring(body)
            names.extend(node.text for node in root.iter("Name"))
            marker = root.findtext("NextMarker") or ""
            if not marker:
                return names

    def download(self, directory):
        """Copy every completed Parquet object into a local directory."""
        directory = Path(directory)
        for name in self.blob_names():
            if not name.endswith(".parquet"):
                continue
            path = "/" + self.blob_container + "/" + urllib.parse.quote(name)
            status, body = self.request("GET", path)
            if status != 200:
                raise AssertionError(f"GET {name} returned {status}")
            destination = directory / name.removeprefix("otel/")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(body)

    def __exit__(self, *exc):
        try:
            subprocess.run(
                ["docker", "rm", "--force", "--volumes", self.name],
                check=False, capture_output=True, timeout=DOCKER_TIMEOUT_S,
            )
        except subprocess.TimeoutExpired:
            pass
        self.container = None
        if self.certs is not None:
            shutil.rmtree(self.certs, ignore_errors=True)


# The frozen object layout of spec section 5.3. The writer id is matched
# loosely because it is user configured and may itself contain a dash; the
# boot id and sequence have fixed shapes, so the split stays unambiguous.
PART_NAME = re.compile(
    r"^v=1/signal=(?P<signal>logs|metrics)"
    r"/dataset=(?P<dataset>series|values)"
    r"/date=(?P<date>\d{4}-\d{2}-\d{2})/hour=(?P<hour>\d{2})"
    r"/part-(?P<stamp>\d{8}T\d{6}Z)-(?P<writer>.+)"
    r"-(?P<boot>[0-9a-f]{32})-(?P<seq>\d{8,})\.parquet$"
)


def verify_layout(test, root, path, metadata):
    """Check one object's path against the layout and its own metadata.

    The name is not merely well shaped: every component of it has to agree
    with the file metadata, and the Hive partition has to agree with the
    window the name records, so a reader can locate a file from the metadata
    alone and a writer cannot drift from the documented layout.
    """
    relative = Path(path).relative_to(root).as_posix()
    match = PART_NAME.fullmatch(relative)
    test.assertTrue(match, f"object is not in the frozen layout: {relative}")
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(int(metadata["window_start"])))
    test.assertEqual(match["stamp"], stamp, f"window stamp of {relative}")
    test.assertEqual(match["writer"], metadata["writer_id"], f"writer of {relative}")
    test.assertEqual(match["boot"], metadata["boot_id"], f"boot id of {relative}")
    test.assertEqual(int(match["seq"]), int(metadata["seq"]), f"sequence of {relative}")
    test.assertEqual(
        match["date"], f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}", f"date of {relative}"
    )
    test.assertEqual(match["hour"], stamp[9:11], f"hour of {relative}")
    return match


def scan_objects(test, root, db, *, collect_bodies=True):
    """Check every part file's own invariants and report what it holds.

    This is the workload-independent half of `verify_files`: the layout, the
    recorded row count, the declared sort key, the descriptor identity hash,
    the per-point-kind column nullability, and the rule that every values row
    of a partition is covered by a descriptor of the same signal, partition
    and writer. None of it depends on which records a producer sent, so a
    measurement run with its own deterministic workload checks exactly the
    same invariants as the fixture tests rather than restating them.

    Returns the part files, the descriptor coverage set, the values keys, the
    logs bodies and the number of metrics values rows. `collect_bodies=False`
    returns no bodies, for a caller that reads gigabytes of them otherwise.
    """
    files = sorted(Path(root).rglob("*.parquet"))
    test.assertTrue(files)
    coverage = set()
    values = []
    bodies = []
    metric_rows = 0
    for path in files:
        partitions = dict(
            part.split("=", 1) for part in path.parts if "=" in part
        )
        metadata = dict(
            db.execute(
                "SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)",
                [str(path)],
            ).fetchall()
        )
        verify_layout(test, root, path, metadata)
        signal = partitions["signal"]
        dataset = partitions["dataset"]
        worker = (metadata["writer_id"], metadata["boot_id"])
        partition = (partitions["date"], partitions["hour"])
        # Hashing the whole row forces every column to be decoded, so a
        # file that cannot be read at all fails here; the count is what
        # the recorded row count is checked against. The hash is summed in
        # SQL rather than fetched, which keeps timestamp conversion (and
        # its optional pytz dependency) out of the reader.
        rows = db.execute(
            "SELECT count(*), sum(hash(t)) FROM "
            "read_parquet(?, hive_partitioning=false) t",
            [str(path)],
        ).fetchone()[0]
        test.assertEqual(int(metadata["row_count"]), rows)
        if dataset == "series":
            ids = db.execute(
                "SELECT series_id, identity_bytes FROM read_parquet(?)", [str(path)]
            ).fetchall()
            test.assertEqual(
                [row[0] for row in ids], sorted(row[0] for row in ids)
            )
            for series_id, identity in ids:
                test.assertEqual(xxhash.xxh3_128_digest(identity), series_id)
                coverage.add((signal, partition, worker, series_id))
        else:
            keys = db.execute(
                "SELECT series_id, time_unix_nano FROM read_parquet(?)", [str(path)]
            ).fetchall()
            if metadata["sort_key"] != "none":
                test.assertEqual(
                    keys,
                    sorted(
                        keys, key=lambda row: (row[0], row[1] is None, row[1] or 0)
                    ),
                )
            values.extend((signal, partition, worker, row[0]) for row in keys)
            if signal == "logs":
                if collect_bodies:
                    bodies.extend(
                        row[0]
                        for row in db.execute(
                            "SELECT body FROM read_parquet(?)", [str(path)]
                        ).fetchall()
                    )
            else:
                metric_rows += len(keys)
                # Each row fills one point kind's columns and leaves the
                # other kind's null, and no row fills both.
                mixed = db.execute(
                    "SELECT count(*) FROM read_parquet(?) WHERE "
                    "(value_int IS NOT NULL OR value_double IS NOT NULL) "
                    "AND count IS NOT NULL",
                    [str(path)],
                ).fetchone()[0]
                test.assertEqual(
                    mixed, 0, "a values row carries both point kinds"
                )
                # A file-level encoding invariant, not a classification
                # rule: the writer leaves both lists null on a number row
                # and stores empty lists on a distribution-less histogram.
                # Readers classify by the descriptor, because ClickHouse
                # renders a null Parquet list as [] and cannot see this.
                inconsistent = db.execute(
                    "SELECT count(*) FROM read_parquet(?) WHERE "
                    "(bucket_counts IS NULL) <> (count IS NULL) OR "
                    "(explicit_bounds IS NULL) <> (count IS NULL)",
                    [str(path)],
                ).fetchone()[0]
                test.assertEqual(
                    inconsistent,
                    0,
                    "list nullability must follow the point kind",
                )
    test.assertTrue(
        set(values).issubset(coverage),
        "descriptor coverage per partition and worker",
    )
    return files, coverage, values, bodies, metric_rows


def verify_files(test, root, log_ids, metric_count, allow_duplicates=False):
    """Read every downloaded part file and check the lake's own invariants.

    Each file is checked against the metadata it carries: the recorded row
    count, the sort key it claims and, for a descriptor file, that every
    stored identity really hashes to the series id it is filed under. The
    values rows of a partition must then be covered by the descriptors of the
    same signal, partition and writer, which is what makes the lake readable
    without a catalog.
    """
    with duckdb.connect() as db:
        files, _coverage, _values, bodies, metric_rows = scan_objects(
            test, root, db
        )
        test.assertTrue(set(log_ids).issubset(set(bodies)))
        expected_metric_rows = POINTS_PER_METRIC * metric_count
        if not allow_duplicates:
            test.assertEqual(sorted(bodies), sorted(log_ids))
            test.assertEqual(metric_rows, expected_metric_rows)
        else:
            test.assertGreaterEqual(metric_rows, expected_metric_rows)
        for signal, datasets in (("logs", ("values",)), ("metrics", ("values",))):
            series = [
                str(p)
                for p in files
                if f"signal={signal}" in p.parts and "dataset=series" in p.parts
            ]
            if not series:
                continue
            db.execute(
                "CREATE OR REPLACE TEMP TABLE canonical AS SELECT * FROM "
                "read_parquet(?, union_by_name=true, filename=true) QUALIFY "
                "row_number() OVER (PARTITION BY series_id ORDER BY emitted_at "
                "DESC, filename DESC)=1",
                [series],
            )
            for dataset in datasets:
                selected = [
                    str(p)
                    for p in files
                    if f"signal={signal}" in p.parts and f"dataset={dataset}" in p.parts
                ]
                if selected:
                    before = db.execute(
                        "SELECT count(*) FROM read_parquet(?, union_by_name=true)",
                        [selected],
                    ).fetchone()[0]
                    after = db.execute(
                        "SELECT count(*) FROM read_parquet(?, union_by_name=true) v "
                        "JOIN canonical s USING(series_id)",
                        [selected],
                    ).fetchone()[0]
                    test.assertEqual(
                        before, after, "canonical join must preserve values cardinality"
                    )


def flush_attempts(engine):
    """The object names the exporter announced for each write attempt.

    The exporter emits `series_parquet.flush.attempt` before every attempt.
    Every object of one block carries the same file name and differs only in
    its dataset directory, so the announced name and count are the whole set
    of names that attempt was about to write. Reading them back is what lets a
    test compare the names a failed attempt was going to use with the names
    the retry actually wrote.
    """
    return parse_flush_attempts(engine.engine_log())


def parse_flush_attempts(log):
    """`(attempt, file, objects)` of every `series_parquet.flush.attempt` line.

    Each field is read by its name from the event's field list, whatever
    other fields surround it; `flush.attempt_failed` lines are not attempts.
    """
    attempts = []
    for fields in re.findall(r"series_parquet\.flush\.attempt\b\S*\s*\[([^\]]*)\]", log):
        named = dict(
            part.split("=", 1) for part in fields.split(", ") if "=" in part
        )
        if {"attempt", "file", "objects"} <= named.keys():
            attempts.append((int(named["attempt"]), named["file"], int(named["objects"])))
    return attempts


def wait_for_metrics(engine, names, target, timeout, seed=None):
    """Wait for every named exporter metric to reach `target`, in one loop.

    The largest value seen across polls is kept for each name rather than the
    last one: these are counters the engine drains into its reporting
    interval, so one is only visible in the snapshot that carries it. One
    snapshot feeds every name, because waiting for the names one after another
    would miss any that peaked and vanished while another was being waited
    for. `seed` carries in maxima a caller already observed, so snapshots it
    took earlier are not thrown away.

    Returns the best value seen for each name, whether or not it reached the
    target, so the caller can assert on it and report it.
    """
    best = {name: max(0, (seed or {}).get(name, 0)) for name in names}
    deadline = time.monotonic() + timeout
    while True:
        document = engine_metrics(engine)
        for name in names:
            # Waiting for a value to appear: a snapshot that does not
            # carry the metric yet contributes nothing to the maximum.
            best[name] = max(best[name], metric_max(document, name, default=0))
        if all(value >= target for value in best.values()):
            return best
        if time.monotonic() >= deadline:
            return best
        time.sleep(0.05)


def wait_for_metric(engine, name, target, timeout):
    """Wait for one exporter metric to reach `target`, returning the best seen."""
    return wait_for_metrics(engine, (name,), target, timeout)[name]


def export_with_retry(engine, body, attempts=5, timeout=120):
    """Export one log body, retrying the one refusal a producer must retry.

    Delivery is at least once: a refused request may already be stored, and
    the producer still has to resend it, so a retry is allowed to create a
    duplicate but is never allowed to lose the body. The number of extra
    attempts is returned so a test can report how many duplicates it risked.
    """
    for attempt in range(attempts):
        try:
            engine.logs.Export(log_request(body), timeout=timeout)
            return attempt
        except grpc.RpcError as error:
            if error.code() not in (
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ):
                raise
    raise AssertionError(f"{body} was refused {attempts} times")


def stored_bodies(store, directory, name):
    """Every logs body currently readable from the store, with duplicates."""
    target = Path(directory) / name
    store.download(target)
    paths = sorted((target / "v=1/signal=logs/dataset=values").rglob("*.parquet"))
    if not paths:
        return target, []
    with duckdb.connect() as db:
        rows = db.execute(
            "SELECT body FROM read_parquet(?, union_by_name=true)",
            [[str(path) for path in paths]],
        ).fetchall()
    return target, [row[0] for row in rows]


# The store retry every S3 engine runs with. The exporter refuses a cloud
# store whose `retry.retry_timeout` -- 3 minutes when the section is omitted
# -- is not strictly below `window.flush_retry_deadline` (60 s by default),
# because one write attempt would otherwise keep retrying inside the store
# past the block's deadline.
S3_RETRY = {
    "max_retries": 3,
    "init_backoff": "200ms",
    "max_backoff": "2s",
    "backoff_base": 2.0,
    "retry_timeout": "20s",
}


class DockerSlice(unittest.TestCase):
    """Grafana Alloy and a real S3 object store in containers."""

    def exercise(self, kind):
        """Run the Alloy file-tail topology against one object store."""
        require_clickhouse()
        with DockerStore(kind) as store, tempfile.TemporaryDirectory() as directory:
            with Engine(
                directory, storage=store.storage, overrides={"retry": S3_RETRY}
            ) as engine:
                ids = [f"{kind}-alloy-{i}" for i in range(12)]
                metric_ids = [f"{kind}-metric-{i}" for i in range(6)]
                with AlloyProducer(directory, engine) as alloy:
                    alloy.write(ids)
                    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                        metric = metrics_rpc.MetricsServiceStub(engine.channel)
                        results = [
                            pool.submit(metric.Export, metric_request(item), timeout=30)
                            for item in metric_ids
                        ]
                        for result in results:
                            result.result(timeout=35)
                    wait_for_alloy(store, directory, ids)
                engine.shutdown()
                downloaded = Path(directory) / "downloaded"
                store.download(downloaded)
                verify_files(self, downloaded, ids, 6)
                verify_readers(
                    self, downloaded, ids, 6, alloy_ids=ids, metric_ids=metric_ids
                )

    # Scenario: Docker Alloy tails 12 known lines into a real engine using
    # MinIO, alongside synthetic metrics.
    # Guarantees: DuckDB and ClickHouse agree on bodies, attributes, counts and
    # latest-descriptor joins.
    def test_minio(self):
        self.exercise("minio")

    # Scenario: the same Alloy and synthetic-metrics topology writes to the
    # local RustFS image.
    # Guarantees: both readers recover all expected rows with identical bodies,
    # attributes and descriptor coverage.
    def test_rustfs(self):
        self.exercise("rustfs")

    # Scenario: Docker Alloy on the strict River config, which has no bytes
    # cap, tails 4000 lines of 1100 bytes, which its batch processor sends as
    # one OTLP export of more than 4 MiB, into the shipped engine receiver and
    # exporter on MinIO.
    # Guarantees: the export is accepted as one request and every line is
    # stored exactly once, so the receiver's decoding limit admits every
    # request the exporter's ingress.max_request_bytes accepts.
    def test_alloy_batch_above_4mib_is_stored(self):
        with DockerStore("minio") as store, tempfile.TemporaryDirectory() as directory:
            with Engine(
                directory, storage=store.storage, overrides={"retry": S3_RETRY}
            ) as engine:
                try:
                    ids = [f"large-{i:04d}-" + "x" * 1089 for i in range(4000)]
                    self.assertGreater(sum(len(body) for body in ids), 4 << 20)
                    strict = WORKSPACE / "configs/series-parquet-strict.alloy"
                    with AlloyProducer(directory, engine, config=strict) as alloy:
                        alloy.write(ids)
                        try:
                            wait_for_alloy(store, directory, ids, timeout=120)
                        except AssertionError:
                            print(alloy.logs())
                            raise
                    engine.shutdown()
                    requests = [
                        int(count)
                        for count in re.findall(
                            r"series_parquet\.block\.committed\b.*?\brequests=(\d+)",
                            engine.engine_log(),
                        )
                    ]
                    self.assertEqual(
                        sum(requests), 1, "the 4000 lines arrived as one request"
                    )
                    _, bodies = stored_bodies(store, directory, "final")
                    self.assertEqual(sorted(bodies), sorted(ids))
                except Exception:
                    print(engine.engine_log())
                    raise

    # Scenario: Alloy on the reference config tails 4000 lines of 8 KiB, one
    # 4000-record batch of about 34 MB, into a receiver limited to 16MiB.
    # Guarantees: the sending queue's bytes cap splits the batch into exports
    # the receiver takes, so no export is refused and every line is stored once.
    def test_alloy_splits_large_lines_under_the_receiver_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            with Engine(directory) as engine:
                ids = [f"wide-{i:04d}-" + "x" * (8192 - 10) for i in range(4000)]
                self.assertGreater(sum(len(body) for body in ids), 16 << 20)
                with AlloyProducer(directory, engine) as alloy:
                    alloy.write(ids)
                    bodies = wait_for_local_bodies(engine, len(ids), timeout=120)
                    failures = alloy.export_failures()
                engine.shutdown()
                self.assertEqual(failures, [])
                self.assertEqual(sorted(bodies), sorted(ids))
                requests = sum(
                    int(count)
                    for count in re.findall(
                        r"series_parquet\.block\.committed\b.*?\brequests=(\d+)",
                        engine.engine_log(),
                    )
                )
                # 34 MB in exports of at most 2MiB.
                self.assertGreaterEqual(requests, 17)

    # Scenario: Alloy on the reference config tails three lines of 3 MiB each,
    # above the exporter's 1MiB ingress.max_row_bytes.
    # Guarantees: the truncate stage cuts each to 512KiB, suffix included, so
    # the export is stored rather than refused as a whole.
    def test_alloy_truncates_lines_above_the_row_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            with Engine(directory) as engine:
                ids = [f"huge-{i}-" + "y" * (3 << 20) for i in range(3)]
                with AlloyProducer(directory, engine) as alloy:
                    alloy.write(ids)
                    bodies = wait_for_local_bodies(engine, len(ids), timeout=120)
                    failures = alloy.export_failures()
                engine.shutdown()
                self.assertEqual(failures, [])
                suffix = "...[truncated]"
                # The limit counts the suffix.
                keep = (512 << 10) - len(suffix)
                self.assertEqual(sorted(bodies), sorted(body[:keep] + suffix for body in ids))

    # Scenario: the object store is stopped while six producers are exporting
    # and is started again, with its data intact, a few seconds later.
    # Guarantees: the outage refuses requests retryably rather than losing
    # them, every body a producer resent is stored, and the duplicates that
    # at-least-once delivery permits are counted rather than assumed away.
    def test_store_outage_is_at_least_once(self):
        require_clickhouse()
        with DockerStore("minio") as store, tempfile.TemporaryDirectory() as directory:
            overrides = {
                # The flush deadline is shorter than the outage, so the
                # exporter has to give up on a block and refuse its requests
                # retryably rather than waiting the store out.
                "window": {"interval": "1s", "flush_retry_deadline": "5s"},
                # Short per-operation retries so a request finds out quickly
                # that the store is gone and the exporter's own flush retry,
                # not the object store client, is what waits out the outage.
                "retry": {
                    "max_retries": 2,
                    "init_backoff": "200ms",
                    "max_backoff": "1s",
                    "backoff_base": 2.0,
                    # Strictly below the 5 s flush deadline, which the
                    # exporter requires of a cloud store.
                    "retry_timeout": "2s",
                },
            }
            with Engine(
                directory, storage=store.storage, overrides=overrides
            ) as engine:
                try:
                    before = [f"outage-before-{i}" for i in range(3)]
                    during = [f"outage-during-{i}" for i in range(6)]
                    for body in before:
                        self.assertEqual(export_with_retry(engine, body), 0)
                    store.stop()
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=len(during)
                    ) as pool:
                        calls = [
                            pool.submit(export_with_retry, engine, body)
                            for body in during
                        ]
                        # The outage lasts until the engine itself reports a
                        # block it could not write, not until a fixed sleep
                        # expires: that is the state this test is about.
                        failures = wait_for_metric(engine, "flush.failures", 1, 120)
                        self.assertGreaterEqual(
                            failures, 1, "no block failed while the store was stopped"
                        )
                        self.assertEqual(
                            [call.done() for call in calls],
                            [False] * len(calls),
                            "a producer finished while the store was stopped",
                        )
                        store.recover()
                        retries = sum(call.result(timeout=180) for call in calls)
                    engine.shutdown()
                    ids = before + during
                    target, bodies = stored_bodies(store, directory, "downloaded")
                    self.assertEqual(set(bodies), set(ids), "a body was lost")
                    duplicates = len(bodies) - len(set(bodies))
                    print(
                        f"store outage: {retries} producer retries, "
                        f"{duplicates} duplicate rows"
                    )
                    self.assertGreaterEqual(
                        retries, 1, "the outage refused nothing, so nothing was retried"
                    )
                    self.assertLessEqual(duplicates, retries)
                    verify_files(self, target, ids, 0, allow_duplicates=True)
                    verify_readers(self, target, ids, 0, allow_duplicates=True)
                except Exception:
                    print(engine.engine_log())
                    raise

    # Scenario: the engine process is killed while a block holding two
    # admitted requests has not been written, and a second engine is started
    # against the same bucket.
    # Guarantees: nothing that was acknowledged before the kill is lost, the
    # restarted engine writes under a new boot id instead of colliding with
    # the old names, and the producer's resend leaves no body missing.
    def test_engine_restart_is_at_least_once(self):
        require_clickhouse()
        with DockerStore("minio") as store, tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # A block of exactly three requests and a window long enough that
            # only the block's own request count can seal it: the first three
            # bodies seal their block and become durable, and the two that
            # follow stay in an open block that nothing can flush.
            overrides = {
                "window": {"interval": "600s", "max_requests_per_block": 3},
                "retry": S3_RETRY,
            }
            acked = [f"restart-acked-{i}" for i in range(3)]
            lost = [f"restart-inflight-{i}" for i in range(2)]
            first = root / "engine-0"
            first.mkdir()
            with Engine(
                first,
                storage=store.storage,
                overrides=overrides,
                telemetry_interval="50ms",
            ) as engine:
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                        calls = [
                            pool.submit(export_with_retry, engine, body)
                            for body in acked
                        ]
                        for call in calls:
                            self.assertEqual(call.result(timeout=120), 0)
                    inflight = [
                        engine.logs.Export.future(log_request(body), timeout=30)
                        for body in lost
                    ]
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline:
                        pending = metric_max(
                            engine_metrics(engine),
                            "block.requests_pending",
                            default=0,
                        )
                        if pending >= 2:
                            break
                        time.sleep(0.05)
                    else:
                        self.fail("the second block never held both requests")
                    engine.process.kill()
                    for call in inflight:
                        with self.assertRaises(grpc.RpcError):
                            call.result(timeout=30)
                except Exception:
                    print(engine.engine_log())
                    raise
            _, survived = stored_bodies(store, directory, "after-kill")
            self.assertEqual(
                set(survived), set(acked), "an acknowledged body did not survive"
            )
            second = root / "engine-1"
            second.mkdir()
            # The second engine rotates on its window instead: the resent
            # bodies are fewer than a block, so only the window can seal them.
            with Engine(
                second,
                storage=store.storage,
                overrides={"window": {"interval": "1s"}, "retry": S3_RETRY},
            ) as engine:
                try:
                    retries = sum(
                        export_with_retry(engine, body) for body in lost
                    )
                    engine.shutdown()
                except Exception:
                    print(engine.engine_log())
                    raise
            ids = acked + lost
            target, bodies = stored_bodies(store, directory, "downloaded")
            self.assertEqual(set(bodies), set(ids), "a body was lost")
            duplicates = len(bodies) - len(set(bodies))
            print(
                f"engine restart: {retries} producer retries, "
                f"{duplicates} duplicate rows"
            )
            verify_files(self, target, ids, 0, allow_duplicates=True)
            verify_readers(self, target, ids, 0, allow_duplicates=True)

    # Scenario: the store is stopped while a sealed block is being written, so
    # the flush has to retry, and it is started again well inside the flush
    # deadline.
    # Guarantees: the retried block lands under the object names it was given
    # the first time -- same window stamp, writer id, boot id and sequence --
    # and leaves no extra part file behind, so a retry rewrites an object
    # rather than adding one.
    def test_retry_keeps_frozen_object_names(self):
        require_clickhouse()
        with DockerStore("minio") as store, tempfile.TemporaryDirectory() as directory:
            overrides = {
                # The deadline outlasts the outage, so the flush is retried
                # rather than failed; the object store client itself gives up
                # at once, so every attempt is the exporter's own.
                "window": {"interval": "1s", "flush_retry_deadline": "120s"},
                "retry": {
                    "max_retries": 0,
                    "init_backoff": "200ms",
                    "max_backoff": "1s",
                    "backoff_base": 2.0,
                    "retry_timeout": "1s",
                },
            }
            # The exporter announces the first attempt of a flush at DEBUG
            # and only a retry at INFO. This test compares the two, so it asks
            # the engine for DEBUG logs; nothing else here depends on the
            # level.
            with Engine(
                directory,
                storage=store.storage,
                overrides=overrides,
                telemetry_interval="50ms",
                log_level="debug",
            ) as engine:
                try:
                    store.stop()
                    call = engine.logs.Export.future(
                        log_request("frozen-0"), timeout=180
                    )
                    # The first attempt starts against a stopped store and
                    # announces the names it is about to write; those names are
                    # what the retry has to reuse.
                    attempts = []
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline:
                        attempts = flush_attempts(engine)
                        if attempts:
                            break
                        time.sleep(0.05)
                    self.assertTrue(attempts, "no write attempt was announced")
                    first = attempts[0]
                    self.assertEqual(first[0], 1)
                    announced, objects = first[1], first[2]
                    self.assertEqual(objects, 2, first)
                    self.assertFalse(
                        call.done(), "the request was decided while the store was down"
                    )
                    store.recover()
                    call.result(timeout=180)
                    retries = wait_for_metric(engine, "flush.retries", 1, 30)
                    self.assertGreaterEqual(
                        retries, 1, "the flush reached the store on its first attempt"
                    )
                    # Every later attempt of that flush named the same objects.
                    later = [
                        (name, count)
                        for attempt, name, count in flush_attempts(engine)
                        if attempt > 1
                    ]
                    self.assertTrue(later, "the retry announced no attempt")
                    for name, count in later:
                        self.assertEqual(
                            (name, count),
                            (announced, objects),
                            "a retry changed the object names",
                        )
                    engine.shutdown()
                except Exception:
                    print(engine.engine_log())
                    raise
            # The bucket holds exactly the objects the first attempt named:
            # the retry rewrote them and added nothing.
            keys = []
            for page in store.client.get_paginator("list_objects_v2").paginate(
                Bucket=store.bucket, Prefix="otel/"
            ):
                keys += [item["Key"] for item in page.get("Contents", [])]
            self.assertEqual(len(keys), objects, f"the bucket holds {keys}")
            self.assertEqual(
                [key.rsplit("/", 1)[1] for key in keys],
                [announced] * objects,
                f"the retry wrote names the first attempt never announced: {keys}",
            )
            target, bodies = stored_bodies(store, directory, "downloaded")
            self.assertEqual(bodies, ["frozen-0"])
            files = sorted(Path(target).rglob("*.parquet"))
            # One values file and one descriptor file, in two dataset
            # directories: a retry rewrote the same two objects. The two share
            # a file name, which is why the directories are counted too.
            self.assertEqual(
                len(files), 2, f"a retry left extra part files behind: {files}"
            )
            self.assertEqual(len({path.parent for path in files}), 2, files)
            verify_files(self, target, ["frozen-0"], 0)
            verify_readers(self, target, ["frozen-0"], 0)
            # Both files belong to one logical block, and each one's name
            # still carries the sequence, boot id and window its metadata
            # records: the retry rewrote the objects the first attempt named.
            blocks = set()
            for path in files:
                with duckdb.connect() as db:
                    metadata = dict(
                        db.execute(
                            "SELECT decode(key), decode(value) FROM "
                            "parquet_kv_metadata(?)",
                            [str(path)],
                        ).fetchall()
                    )
                blocks.add(
                    (metadata["seq"], metadata["boot_id"], metadata["window_start"])
                )
                self.assertTrue(
                    path.name.endswith(f"-{int(metadata['seq']):08d}.parquet"),
                    path.name,
                )
            self.assertEqual(len(blocks), 1, f"two blocks were written: {blocks}")


# The extension instance that serves the Azure store its bearer token.
AZURE_TOKEN_EXTENSION = "azure_token"


def require_azure_engine(config):
    """Skip, or fail under SERIES_REQUIRE_DOCKER, when the engine binary
    cannot run Azure storage with the OAuth 2.0 token extension.

    The configuration is validated by the binary itself; only a refusal
    that names the missing `azure` variant or the unregistered extension
    means the build lacks the features. Any other refusal is a failure.
    """
    binary = Path(os.environ.get("DF_ENGINE", WORKSPACE / "target/debug/df_engine"))
    if not binary.is_file():
        raise AssertionError(f"build the feature-enabled engine first: {binary}")
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "pipeline.yaml"
        path.write_text(yaml.safe_dump(config))
        done = subprocess.run(
            [str(binary), "--config", str(path), "--validate-and-exit"],
            capture_output=True, text=True, timeout=60,
        )
    output = done.stdout + done.stderr
    if done.returncode == 0:
        return
    if "unknown variant `azure`" in output or (
        "oauth2_client_auth" in output and "Unknown extension" in output
    ):
        unavailable(
            "the engine was built without the azure and oauth2-client-auth "
            f"features: {binary}"
        )
    raise AssertionError(f"the Azure configuration was refused:\n{output}")


class AzureSlice(unittest.TestCase):
    """The exporter on Azure Blob Storage, as emulated by Azurite."""

    # Scenario: logs and metrics requests reach the engine, whose exporter
    # writes to Azurite over HTTPS with a bearer token that the
    # oauth2_client_auth extension acquires from a loopback token endpoint
    # and the Azure store takes through the bearer_token_provider capability.
    # Guarantees: Azurite refuses an unauthenticated call, so the writes went
    # through the token path; every acknowledged log record and metric point
    # is stored exactly once, and DuckDB and ClickHouse read the same rows
    # through the latest-descriptor join.
    def test_azurite_stores_every_acknowledged_record(self):
        require_clickhouse()
        with AzuriteStore() as store, TokenEndpoint(store.token) as tokens, \
                tempfile.TemporaryDirectory() as directory:
            status, _ = store.request("GET", "?comp=list", token=False)
            self.assertIn(status, (401, 403), "Azurite accepted an anonymous call")
            extensions = {
                AZURE_TOKEN_EXTENSION: {
                    "type": "urn:otel:extension:oauth2_client_auth",
                    "config": {
                        "token_url": tokens.url,
                        "client_id": "series-e2e",
                        "client_secret": "series-e2e-secret",
                        "scopes": ["https://storage.azure.com/.default"],
                    },
                }
            }
            capabilities = {"bearer_token_provider": AZURE_TOKEN_EXTENSION}
            overrides = {"retry": S3_RETRY}
            require_azure_engine(
                engine_config(
                    grpc_port=free_port(),
                    data=Path(directory) / "data",
                    storage=store.storage,
                    overrides=overrides,
                    extensions=extensions,
                    exporter_capabilities=capabilities,
                )
            )
            log_ids = [f"azure-log-{i}" for i in range(12)]
            metric_ids = [f"azure-metric-{i}" for i in range(6)]
            with Engine(
                directory,
                storage=store.storage,
                overrides=overrides,
                extensions=extensions,
                exporter_capabilities=capabilities,
                env={"SSL_CERT_FILE": store.ca_file},
            ) as engine:
                try:
                    metrics = metrics_rpc.MetricsServiceStub(engine.channel)
                    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                        results = [
                            pool.submit(engine.logs.Export, log_request(item), timeout=60)
                            for item in log_ids
                        ] + [
                            pool.submit(metrics.Export, metric_request(item), timeout=60)
                            for item in metric_ids
                        ]
                        # Every request is acknowledged; a refusal raises here.
                        for result in results:
                            result.result(timeout=65)
                    engine.shutdown()
                except Exception:
                    print(engine.engine_log())
                    raise
            self.assertTrue(tokens.grants, "the engine acquired no token")
            self.assertEqual(set(tokens.grants), {"client_credentials"})
            downloaded = Path(directory) / "downloaded"
            store.download(downloaded)
            verify_files(self, downloaded, log_ids, len(metric_ids))
            verify_readers(
                self, downloaded, log_ids, len(metric_ids), metric_ids=metric_ids
            )


class RestartSlice(unittest.TestCase):
    """A second engine process, and a producer that walks away."""

    # Scenario: two engine processes write into the same aligned window and
    # base directory.
    # Guarantees: boot IDs prevent object-name collisions and additive columns
    # read with union-by-name.
    def test_restart_names_and_additive_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "destination"
            target.mkdir()
            storage = {"file": {"base_uri": str(target)}}
            names = []
            for index in range(2):
                work = root / f"engine-{index}"
                work.mkdir()
                overrides = {
                    "window": {
                        "interval": "86400s",
                        "max_requests_per_block": 1,
                        "flush_retry_deadline": "10s",
                    }
                }
                if index:
                    overrides["logs"] = {
                        "denormalize": [
                            {
                                "path": "attrs.new.field",
                                "column": "new_field",
                                "type": "string",
                            }
                        ]
                    }
                with Engine(work, storage=storage, overrides=overrides) as engine:
                    try:
                        request = log_request(f"restart-{index}")
                        if index:
                            scope = request.resource_logs[0].scope_logs[0]
                            record = scope.log_records[0]
                            record.attributes.add(
                                key="new.field"
                            ).value.string_value = "added"
                        engine.logs.Export(request, timeout=20)
                        engine.shutdown()
                    except Exception:
                        print(engine.engine_log())
                        raise
                names.append(set(path.name for path in target.rglob("*.parquet")))
            self.assertTrue(names[0] < names[1])
            with duckdb.connect() as db:
                path = str(target / "v=1/signal=logs/dataset=values/**/*.parquet")
                rows = db.execute(
                    "SELECT body, new_field FROM read_parquet(?, union_by_name=true) "
                    "ORDER BY body",
                    [path],
                ).fetchall()
                self.assertEqual(rows, [("restart-0", None), ("restart-1", "added")])
                windows = db.execute(
                    "SELECT DISTINCT decode(value) FROM parquet_kv_metadata(?) "
                    "WHERE decode(key)='window_start'",
                    [path],
                ).fetchall()
                # The one-day window (the longest allowed) keeps both boots in
                # one aligned window; a run straddling 00:00 UTC sees the next.
                starts = sorted(int(value) for (value,) in windows)
                self.assertTrue(all(start % 86400 == 0 for start in starts), starts)
                self.assertTrue(
                    len(starts) == 1
                    or (len(starts) == 2 and starts[1] - starts[0] == 86400),
                    starts,
                )
            verify_files(self, target, ["restart-0", "restart-1"], 0)

    # Scenario: a producer disconnects after admission but before a long
    # window commits.
    # Guarantees: abandoning the client result cannot retract accepted data.
    def test_disconnect_does_not_remove_data(self):
        with tempfile.TemporaryDirectory() as directory:
            overrides = {"window": {"interval": "5s", "flush_retry_deadline": "10s"}}
            with Engine(
                directory, overrides=overrides, telemetry_interval="50ms"
            ) as engine:
                try:
                    # Windows are aligned to multiples of the interval on the
                    # wall clock. Sent at an arbitrary offset, a request near
                    # a boundary is written and acked before the poll below
                    # can see it pending; sent just after a boundary, it stays
                    # pending for the whole observation.
                    sleep_to_window_offset(5.0, 0.2)
                    call = engine.logs.Export.future(
                        log_request("disconnected"), timeout=20
                    )
                    deadline = time.monotonic() + 4
                    while time.monotonic() < deadline:
                        metrics = engine_metrics(engine)
                        if metric_max(
                            metrics, "block.requests_pending", default=0
                        ) >= 1:
                            break
                        time.sleep(0.05)
                    else:
                        self.fail("request was not observed as admitted")
                    call.cancel()
                    engine.shutdown(seconds=30)
                    verify_files(self, engine.data, ["disconnected"], 0)
                except Exception:
                    print(engine.engine_log())
                    raise


def engine_metrics(engine):
    """The engine's own metric snapshot, as JSON, including zero values."""
    url = (
        f"http://127.0.0.1:{engine.admin_port}"
        "/api/v1/telemetry/metrics?format=json&keep_all_zeroes=true"
    )
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


EXPORTER_METRIC_SET = "exporter.series_parquet"


class _Required:
    """Sentinel asking `metric_max` to insist the metric is present."""

    def __repr__(self):
        """Name the sentinel in an assertion message."""
        return "<required>"


REQUIRED = _Required()


def metric_values(document, name, metric_set=EXPORTER_METRIC_SET):
    """Every numeric value one snapshot reports for one metric.

    An empty list means the snapshot does not carry the metric at all, which
    is a different fact from the metric being present and zero.
    """
    return [
        item["value"]
        for group in document["metric_sets"]
        if group["name"] == metric_set
        for item in group["metrics"]
        if item["name"] == name and isinstance(item["value"], (int, float))
    ]


def metric_present(document, name, metric_set=EXPORTER_METRIC_SET):
    """Whether one snapshot carries one metric at all."""
    return bool(metric_values(document, name, metric_set))


def metric_max(document, name, *, default=REQUIRED, metric_set=EXPORTER_METRIC_SET):
    """The largest reported value of one exporter metric in a snapshot.

    A snapshot that does not carry the metric is not a snapshot that reports
    zero. Reading absence as zero makes an upper-bound assertion pass
    vacuously and gives a lower-bound assertion a message that names the
    wrong cause, because the number it failed on was never measured.

    Presence is therefore required unless the caller names a `default`. A
    poll that is waiting for a metric to appear passes `default=0` and says
    so; a caller that is asserting on a value passes nothing and fails with
    the metric named if the snapshot cannot answer.

    Presence alone is not enough to make a snapshot an observation of the
    exporter: see `exporter_reported`.
    """
    values = metric_values(document, name, metric_set)
    if values:
        return max(values)
    if default is REQUIRED:
        raise AssertionError(
            f"the snapshot carries no {metric_set} metric {name!r}; an absent "
            f"metric set is not a zero-valued sample"
        )
    return default


def exporter_reported(document):
    """Whether the exporter's own sample is in this snapshot.

    `memory.budget` is computed in `worker.rs::sample_metrics` from
    configuration constants -- the sort, merge, writer, upload and conversion
    reservations -- so it is strictly positive whenever the worker samples
    itself. A snapshot in which it is absent, or present and zero, therefore
    does not carry that worker's sample.

    Both shapes have been observed. Under outage load one snapshot in a run
    carried the exporter metric set, with `memory.budget` among its
    metric names, reporting zero; the worker had not answered the collection
    that snapshot was built from. Which engine path produces that is not
    settled here, and this check does not depend on it.

    The budget is the only reliable marker. Every value gauge reads zero in
    such a snapshot too, but zero is a legitimate value for each of them, so
    none can distinguish "the exporter is empty" from "the exporter did not
    report".
    """
    return metric_max(document, "memory.budget", default=0) > 0


def rss_bytes(pid):
    """Resident set size of one process, in bytes, from /proc.

    The outage test bounds the exporter's memory against the budget it
    reports, so it needs the real process size rather than an allocator
    statistic.
    """
    status = Path(f"/proc/{pid}/status")
    if not status.exists():
        raise unittest.SkipTest("Outage RSS assertion requires Linux /proc")
    for line in status.read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("process RSS unavailable while engine is running")


def stored_multiplicity(root):
    """How many rows each log body and each metric request id has.

    Metric rows are counted through the latest-descriptor join the crate
    README documents, so a series whose descriptor was written again by a
    later block still contributes exactly one row per values row. This is what
    a resend has to be measured against: an at-least-once duplicate is one
    more row under an id that was already there, never a new id and never a
    row that the join drops or doubles.
    """
    root = Path(root)

    def tally(paths, sql):
        if not paths:
            return collections.Counter()
        rows = db.execute(sql, [[str(path) for path in paths]]).fetchall()
        return collections.Counter(dict(rows))

    counts = {}
    with duckdb.connect() as db:
        counts["logs"] = tally(
            sorted((root / "v=1/signal=logs/dataset=values").rglob("*.parquet")),
            "SELECT body, count(*) FROM read_parquet(?, union_by_name=true) "
            "GROUP BY body",
        )
        series = sorted(
            (root / "v=1/signal=metrics/dataset=series").rglob("*.parquet")
        )
        if series:
            db.execute(
                "CREATE OR REPLACE TEMP TABLE canonical AS SELECT * FROM "
                "read_parquet(?, union_by_name=true, filename=true) QUALIFY "
                "row_number() OVER (PARTITION BY series_id ORDER BY emitted_at "
                "DESC, filename DESC)=1",
                [[str(path) for path in series]],
            )
        # One merged values dataset, so one tally: an id that was stored once
        # contributes POINTS_PER_METRIC rows here.
        counts["metrics"] = tally(
            sorted(
                (root / "v=1/signal=metrics/dataset=values").rglob("*.parquet")
            ),
            "SELECT s.attrs['request.id'], count(*) FROM "
            "read_parquet(?, union_by_name=true) v JOIN canonical s "
            "USING(series_id) GROUP BY 1",
        )
    return counts


# Codes a producer is allowed to see and must retry. A storage failure is a
# transient NACK, which the OTLP receiver reports as UNAVAILABLE.
RETRYABLE_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.CANCELLED,
}
# How long the outage test waits out one ordinary RPC, and one of the cohort
# RPCs that has to stay outstanding across the whole observation window. The
# calls themselves carry no gRPC deadline, so these are waits the test
# performs rather than deadlines the server is told about: a wait that expires
# raises grpc.FutureTimeoutError on a path of its own, which is what makes
# every reported status server-originated by construction.
CALL_WAIT = 6
COHORT_WAIT = 60
# A wait short enough to expire against a stopped store. A producer that gives
# up on a call the exporter has already admitted, and then resends it, is how
# at-least-once delivery produces a duplicate; this is what makes the standing
# suite exercise duplicates rather than merely tolerate them.
CALL_WAIT_SHORT = 1


class FlushAttemptLines(unittest.TestCase):
    """How the retry test reads the exporter's write-attempt announcements."""

    # Scenario: engine log lines of a first attempt and a retry in the current
    # event layout (`seq` before `attempt`, `deadline_remaining` after
    # `objects`), and a failed-attempt line that names the same attempt.
    # Guarantees: both attempts are read with their file and object count,
    # and the failure line is not taken for an attempt.
    def test_attempts_are_read_by_field_name(self):
        name = "part-20260925T094024Z-local_1-3cff9bed-00000000.parquet"
        log = "\n".join([
            "\x1b[2m2026-09-25T09:40:25.000Z\x1b[0m  DEBUG "
            "\x1b[1motel.exporter.series_parquet::series_parquet.flush.attempt\x1b[0m: "
            f"[seq=0, attempt=1, file={name}, objects=2, "
            "deadline_remaining=119.99999077s]\x1b[35m entity/pipeline.attrs: core.id=0",
            "\x1b[1motel.exporter.series_parquet::series_parquet.flush.attempt_failed\x1b[0m: "
            f"[seq=0, attempt=1, file={name}, retryable=true, error=offline]",
            "\x1b[1motel.exporter.series_parquet::series_parquet.flush.attempt\x1b[0m: "
            f"[seq=0, attempt=2, file={name}, objects=2, deadline_remaining=119.8s]",
        ])
        self.assertEqual(parse_flush_attempts(log), [(1, name, 2), (2, name, 2)])


class AlloyFailureLines(unittest.TestCase):
    """How the outage tests read Alloy's export failure lines."""

    # Scenario: Alloy logs an export whose own per-attempt timeout fired, once
    # as DeadlineExceeded and once as the Canceled "Timeout expired" that a
    # loaded host produces, beside a refusal the engine returned.
    # Guarantees: both client deadlines are recognised as Alloy's own, so
    # only statuses the engine sent are held to UNAVAILABLE.
    def test_client_deadlines_are_not_engine_refusals(self):
        prefix = (
            'level=warn msg="Exporting failed. Will retry the request after '
            'interval." component_id=otelcol.exporter.otlp.series '
            'error="rpc error: '
        )
        deadline = prefix + 'code = DeadlineExceeded desc = context deadline exceeded"'
        canceled = prefix + 'code = Canceled desc = Timeout expired"'
        refusal = prefix + 'code = Unavailable desc = series_parquet could not write"'
        self.assertTrue(AlloyProducer.client_deadline(deadline))
        self.assertTrue(AlloyProducer.client_deadline(canceled))
        self.assertFalse(AlloyProducer.client_deadline(refusal))


class OutageSlice(unittest.TestCase):
    """A real object store taken off the network under live producers."""

    # Scenario: Alloy file tailing and independent synthetic logs and metrics
    # requests continue during an eight-second real S3 outage that outlasts the
    # flush deadline.
    # Guarantees: no request of either signal is acknowledged while the store
    # is down, every status the server returned is the retryable UNAVAILABLE,
    # Alloy itself recorded a refused export attempt, the engine recovers
    # without a restart, and both readers recover every Alloy body and every
    # acknowledged synthetic ID within the memory envelope.
    def test_storage_outage_recovers_without_losing_acked_data(self):
        require_clickhouse()
        for kind in ("minio", "rustfs"):
            with self.subTest(store=kind), DockerStore(kind) as store:
                with tempfile.TemporaryDirectory() as directory:
                    self.exercise_outage(kind, store, directory)

    # Scenario: the same outage, with producers that give up on a call after
    # one second and resend it, so requests the exporter has already admitted
    # are sent again.
    # Guarantees: the resends duplicate rows rather than losing any, the
    # duplicates are counted and reported, the layout and the readers'
    # latest-descriptor join tolerate them, and giving up locally never turns
    # a refusal into anything but UNAVAILABLE.
    def test_short_client_waits_duplicate_rather_than_lose(self):
        require_clickhouse()
        with DockerStore("minio") as store, tempfile.TemporaryDirectory() as directory:
            summary = self.exercise_outage(
                "minio", store, directory, call_wait=CALL_WAIT_SHORT, resend=3
            )
        # The point of this variant: the producers really did stop waiting on
        # admitted calls, which is what an at-least-once producer does and the
        # only way the racy duplicate path is reached at all.
        self.assertGreater(
            summary["client_waits"],
            0,
            "no client wait expired, so no request was resent after admission",
        )

    @staticmethod
    def downloaded(store, target):
        """Copy the store's objects into `target` and return the path."""
        store.download(target)
        return target

    def exercise_outage(self, kind, store, directory, call_wait=CALL_WAIT, resend=0):
        """One store: stop it under load, recover it, and check the lake.

        `call_wait` is how long a producer waits out one ordinary call before
        giving up on it and resending. `resend`, when set, replays that many
        already-acknowledged log ids and that many metric ids once the store
        is healthy again, which is what an at-least-once producer does when
        its own wait expired; the duplicates that creates are deterministic
        rather than raced for. Returns what the run observed, so a caller can
        assert on the behavior its own variant is about.
        """
        overrides = {
            # The flush deadline is shorter than the outage, so the exporter
            # cannot wait the store out: it has to fail blocks and refuse
            # their requests retryably while the store is gone.
            "window": {
                "interval": "1s",
                "max_block_bytes": "8MiB",
                "max_requests_per_block": 8,
                "flush_retry_deadline": "3s",
            },
            "ingress": {
                "max_request_bytes": "1MiB",
                "max_extracted_bytes": "2MiB",
                "max_row_bytes": "16KiB",
                "max_nesting_depth": 32,
            },
            "series_cache": {"max_entries": 128},
            "sorting": {
                "enabled": True,
                "run_target_bytes": "64KiB",
                "merge_chunk_bytes": "128KiB",
            },
            "parquet": {
                "compression": "zstd",
                "row_group_bytes": "512KiB",
                "writer_limit_bytes": "1MiB",
            },
            "upload": {"part_bytes": "5MiB", "concurrency": 1, "abort_timeout": "1s"},
            # The object store client gives up at once, so every wait during
            # the outage is the exporter's own bounded flush retry.
            "retry": {
                "max_retries": 0,
                "init_backoff": "50ms",
                "max_backoff": "100ms",
                "backoff_base": 2.0,
                "retry_timeout": "200ms",
            },
        }
        with Engine(
            directory, storage=store.storage, overrides=overrides
        ) as engine, AlloyProducer(directory, engine) as alloy:
            try:
                alloy_ids = [f"{kind}-outage-alloy-{i}" for i in range(12)]
                engine.logs.Export(log_request("warmup"), timeout=10)
                baseline_rss = rss_bytes(engine.process.pid)
                signals = ("logs", "metrics")
                expected_requests = {
                    (signal, f"p{index}-{item}")
                    for index in range(8)
                    for item in range(12)
                    for signal in signals
                }
                expected_metric_ids = {
                    rid for signal, rid in expected_requests if signal == "metrics"
                }
                expected_log_ids = {"warmup", *alloy_ids} | {
                    rid for signal, rid in expected_requests if signal == "logs"
                }
                acknowledged = []
                # Statuses the server actually returned, kept apart from the
                # deadlines this test imposed on its own calls: only the
                # former can be a refusal the exporter is answerable for.
                server_codes = []
                local_waits = []
                cohort = {}
                cohort_finished = {}
                # Eight threads per signal plus this one. Logs and metrics run
                # independently, so neither signal has to wait for the other
                # to be acknowledged before it reaches the stopped store.
                workers = 8 * len(signals)
                cohort_start = threading.Barrier(workers + 1, timeout=30)
                cohort_ready = threading.Barrier(workers + 1, timeout=30)
                samples = []
                lock = threading.Lock()
                end = time.monotonic() + 120

                def producer(signal, index):
                    """Send 12 requests of one signal, retrying every refusal.

                    Each thread starts its own first RPC before the cohort
                    barrier releases, so the test knows an RPC of this signal
                    began while the store was stopped. The identical request
                    is resent on every retry, which is what makes the
                    acknowledged set comparable with what is stored.
                    """
                    if signal == "logs":
                        stub = logs_rpc.LogsServiceStub(engine.channel)
                        build = log_request
                    else:
                        stub = metrics_rpc.MetricsServiceStub(engine.channel)
                        build = metric_request
                    cohort_start.wait()
                    first_started = time.monotonic()
                    # No call in this loop carries a gRPC deadline, so the
                    # only deadline is the one this thread waits with and any
                    # status that comes back is one the server sent.
                    first = stub.Export.future(build(f"p{index}-0"))

                    def first_done(call):
                        with lock:
                            cohort_finished[(signal, index)] = (
                                time.monotonic(),
                                call.code(),
                            )

                    first.add_done_callback(first_done)
                    with lock:
                        cohort[(signal, index)] = (first, first_started)
                    cohort_ready.wait()
                    for item in range(12):
                        request_id = f"p{index}-{item}"
                        request = build(request_id)
                        pending = first if item == 0 else None
                        while time.monotonic() < end:
                            if pending is not None:
                                call, wait = pending, COHORT_WAIT
                                pending = None
                            else:
                                call, wait = stub.Export.future(request), call_wait
                            try:
                                call.result(timeout=wait)
                                with lock:
                                    acknowledged.append(
                                        (signal, request_id, time.monotonic())
                                    )
                                break
                            except grpc.FutureTimeoutError:
                                # This thread stopped waiting; the call was
                                # never refused. Cancel it explicitly and
                                # record it on its own path, so it can never
                                # be mistaken for a status the server sent.
                                call.cancel()
                                with lock:
                                    local_waits.append((signal, wait))
                            except grpc.RpcError as error:
                                with lock:
                                    server_codes.append((signal, error.code()))
                                if error.code() not in RETRYABLE_CODES:
                                    raise
                                time.sleep(0.1)
                        else:
                            raise AssertionError(
                                "producer could not drain after recovery"
                            )

                store.stop()
                outage_started = time.monotonic()
                alloy.write(alloy_ids)
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=workers
                ) as pool:
                    jobs = [
                        pool.submit(producer, signal, index)
                        for signal in signals
                        for index in range(8)
                    ]
                    cohort_start.wait()
                    cohort_ready.wait()
                    self.assertEqual(
                        set(cohort),
                        {(signal, index) for signal in signals for index in range(8)},
                    )
                    outage_end = time.monotonic() + 8
                    while time.monotonic() < outage_end:
                        document = engine_metrics(engine)
                        # A snapshot the exporter did not answer reports
                        # every one of its gauges as zero. That is not an
                        # observation of an empty exporter, so it is not a
                        # memory sample; asserting on it would compare this
                        # process's resident size against a budget of zero.
                        if exporter_reported(document):
                            samples.append(
                                (document, rss_bytes(engine.process.pid))
                            )
                        time.sleep(0.2)
                    # Every known first RPC, of both signals, began while
                    # storage was stopped, and none of them may have been
                    # answered with anything but the retryable refusal.
                    for key, (first, started) in cohort.items():
                        self.assertGreaterEqual(started, outage_started, key)
                        if first.done():
                            self.assertEqual(
                                first.code(), grpc.StatusCode.UNAVAILABLE, key
                            )
                        # Otherwise this exact RPC is still pending at
                        # observation.
                    # The engine must have given up on at least one block and
                    # must have retried at least one write before doing so: a
                    # single-attempt flush would prove nothing about retries.
                    # Both are reset counters, so one loop carries the maxima
                    # of both, seeded with the snapshots taken above: waiting
                    # for one and then the other would miss a value that
                    # appeared and was drained while the first was pending.
                    flushes = ("flush.failures", "flush.retries")
                    counters = wait_for_metrics(
                        engine,
                        flushes,
                        1,
                        30,
                        seed={
                            name: max(
                                (
                                    value
                                    for document, _ in samples
                                    for value in metric_values(document, name)
                                ),
                                default=0,
                            )
                            for name in flushes
                        },
                    )
                    self.assertGreaterEqual(
                        counters["flush.failures"],
                        1,
                        "no block failed while the store was stopped",
                    )
                    self.assertGreaterEqual(
                        counters["flush.retries"],
                        1,
                        "no write was retried while the store was stopped",
                    )
                    # Alloy itself must have tried to export and been refused;
                    # without this the test would pass if Alloy had simply sat
                    # on its queue until the store came back.
                    alloy_failures = []
                    alloy_deadline = time.monotonic() + 30
                    while time.monotonic() < alloy_deadline:
                        alloy_failures = alloy.export_failures()
                        if alloy_failures:
                            break
                        time.sleep(0.25)
                    self.assertTrue(
                        alloy_failures,
                        "Alloy never attempted an export while the store was "
                        "stopped:\n" + alloy.logs(),
                    )
                    # Alloy's own client deadline is ALLOY_ATTEMPT_TIMEOUT,
                    # which this test sets, so a refusal it was still waiting
                    # for shows up as its deadline rather than as a status.
                    # Those prove the attempt just as well; the statuses the
                    # engine did return must all be UNAVAILABLE.
                    alloy_deadlines = [
                        line
                        for line in alloy_failures
                        if AlloyProducer.client_deadline(line)
                    ]
                    alloy_refusals = [
                        line
                        for line in alloy_failures
                        if not AlloyProducer.client_deadline(line)
                    ]
                    for line in alloy_refusals:
                        self.assertIn(
                            "code = Unavailable",
                            line,
                            "Alloy was refused with something other than "
                            "UNAVAILABLE",
                        )
                    recovery_started = time.monotonic()
                    store.recover()
                    while not all(job.done() for job in jobs):
                        self.assertLess(time.monotonic(), end, "recovery deadline")
                        document = engine_metrics(engine)
                        if exporter_reported(document):
                            samples.append(
                                (document, rss_bytes(engine.process.pid))
                            )
                        time.sleep(0.2)
                    for job in jobs:
                        job.result()
                    self.assertEqual(set(cohort_finished), set(cohort))
                    for key, (completed, code) in cohort_finished.items():
                        self.assertTrue(
                            code == grpc.StatusCode.UNAVAILABLE
                            or (
                                code == grpc.StatusCode.OK
                                and completed >= recovery_started
                            ),
                            f"cohort RPC {key} was decided with {code} before "
                            "storage recovery",
                        )
                wait_for_alloy(
                    store, directory, alloy_ids, timeout=max(1, end - time.monotonic())
                )
                self.assertTrue(
                    server_codes, "outage must cause retryable refusals"
                )
                # Both signals were refused by the stopped store. Logs and
                # metrics run in independent threads precisely so that neither
                # can reach the store only after the other was acknowledged.
                self.assertEqual(
                    {signal for signal, _ in server_codes},
                    set(signals),
                    "one signal never met the stopped store",
                )
                # No acknowledgement may predate recovery: every one of these
                # requests was admitted while the store was unreachable, so an
                # OK before the store came back would be an ack without
                # durable data.
                self.assertFalse(
                    [item for item in acknowledged if item[2] < recovery_started],
                    "a request was acknowledged while the store was stopped",
                )
                # Every status the server returned is the one retryable code.
                returned = {code for _, code in server_codes}
                self.assertEqual(
                    returned,
                    {grpc.StatusCode.UNAVAILABLE},
                    f"non-retryable refusal during the outage: {returned}",
                )
                self.assertEqual(
                    {(signal, rid) for signal, rid, _ in acknowledged},
                    expected_requests,
                )
                self.assertEqual(len(acknowledged), len(expected_requests))
                # Skipping snapshots that carry no exporter sample must not
                # be able to empty the evidence: a recovery that produced no
                # usable memory sample at all is a failed measurement.
                self.assertTrue(
                    samples, "no exporter memory sample was taken during the outage"
                )
                for document, rss in samples:
                    self.assertLessEqual(
                        metric_max(document, "block.active"), 8 << 20
                    )
                    self.assertLessEqual(
                        metric_max(document, "block.flushing"), 8 << 20
                    )
                    self.assertLessEqual(
                        metric_max(document, "block.pending_slot_occupied"), 1
                    )
                    self.assertLessEqual(metric_max(document, "notify.queued"), 16)
                    budget = metric_max(document, "memory.budget")
                    self.assertGreater(
                        budget, 0, "missing budget telemetry is a failure"
                    )
                    self.assertLessEqual(rss, baseline_rss + budget + (128 << 20))
                # The engine recovers in place: no restart, and the backlog
                # drains back to an empty pending set.
                self.assertIsNone(engine.process.poll(), "the engine did not survive")

                def settle(seconds=10):
                    """Wait until nothing is pending and nothing is stale."""
                    limit = time.monotonic() + seconds
                    while time.monotonic() < limit:
                        document = engine_metrics(engine)
                        # A snapshot the exporter did not answer reports an
                        # empty backlog and a zero age whatever the real
                        # state is, so it is not a settled observation and
                        # the poll continues rather than returning.
                        if (
                            exporter_reported(document)
                            and metric_max(document, "oldest_unacked.age") <= 1
                            and metric_max(document, "block.requests_pending") == 0
                        ):
                            return
                        time.sleep(0.2)
                    self.fail("oldest unacked age did not return to baseline")

                settle()
                if resend:
                    # Everything acknowledged is durable now, so what the
                    # store holds is the baseline the replay is measured
                    # against.
                    before_counts = stored_multiplicity(
                        self.downloaded(store, Path(directory) / "baseline")
                    )
                    resent = {
                        "logs": [f"p{index}-0" for index in range(resend)],
                        "metrics": [
                            f"p{index}-0" for index in range(resend, 2 * resend)
                        ],
                    }
                    # Byte-identical replays of requests that were already
                    # acknowledged, which is exactly what an at-least-once
                    # producer sends after its own wait expired. The store is
                    # healthy again, so a refusal here is a test failure, not
                    # a retry: these calls are outside the refusal accounting
                    # above and carry an ordinary deadline.
                    for request_id in resent["logs"]:
                        logs_rpc.LogsServiceStub(engine.channel).Export(
                            log_request(request_id), timeout=60
                        )
                    for request_id in resent["metrics"]:
                        metrics_rpc.MetricsServiceStub(engine.channel).Export(
                            metric_request(request_id), timeout=60
                        )
                    settle()
                engine.shutdown(seconds=30)
            except Exception:
                print(engine.engine_log())
                raise
        downloaded = Path(directory) / "downloaded"
        store.download(downloaded)
        expected_logs = sorted(expected_log_ids)
        metric_ids = sorted(expected_metric_ids)
        duplicates = {}
        verify_files(self, downloaded, expected_logs, 96, allow_duplicates=True)
        verify_readers(
            self,
            downloaded,
            expected_logs,
            96,
            allow_duplicates=True,
            alloy_ids=alloy_ids,
            metric_ids=metric_ids,
        )
        with duckdb.connect() as db:
            log_path = str(downloaded / "v=1/signal=logs/dataset=values/**/*.parquet")
            stored_logs = [
                row[0]
                for row in db.execute(
                    "SELECT body FROM read_parquet(?)", [log_path]
                ).fetchall()
            ]
            self.assertEqual(
                set(stored_logs),
                expected_log_ids,
                "all precomputed log IDs must survive",
            )
            duplicates["logs"] = len(stored_logs) - len(expected_log_ids)
            series = str(downloaded / "v=1/signal=metrics/dataset=series/**/*.parquet")
            db.execute(
                "CREATE OR REPLACE TEMP TABLE series AS SELECT * FROM "
                "read_parquet(?, union_by_name=true, filename=true) QUALIFY "
                "row_number() OVER(PARTITION BY series_id ORDER BY emitted_at DESC, "
                "filename DESC)=1",
                [series],
            )
            path = str(downloaded / "v=1/signal=metrics/dataset=values/**/*.parquet")
            stored = [
                row[0]
                for row in db.execute(
                    "SELECT s.attrs['request.id'] FROM read_parquet(?) v "
                    "JOIN series s USING(series_id)",
                    [path],
                ).fetchall()
            ]
            self.assertEqual(
                set(stored),
                expected_metric_ids,
                "missing precomputed metric IDs",
            )
            # Each stored request id accounts for POINTS_PER_METRIC rows, so
            # only rows beyond that are duplicates.
            duplicates["metrics"] = len(stored) - POINTS_PER_METRIC * len(
                expected_metric_ids
            )
        forced = {}
        if resend:
            after_counts = stored_multiplicity(downloaded)
            for dataset, counter in after_counts.items():
                replayed = set(resent["logs" if dataset == "logs" else "metrics"])
                self.assertEqual(
                    set(counter),
                    set(before_counts[dataset]),
                    f"the replay changed which ids exist in {dataset}",
                )
                for request_id, count in counter.items():
                    if request_id in replayed:
                        # The replayed id is stored again, on top of the row
                        # that was already acknowledged: a duplicate, present
                        # by construction rather than by a race.
                        self.assertGreaterEqual(
                            count,
                            before_counts[dataset][request_id] + 1,
                            f"replayed {request_id} was not stored again in "
                            f"{dataset}",
                        )
                        self.assertGreaterEqual(
                            count,
                            2 if dataset == "logs" else 2 * POINTS_PER_METRIC,
                            f"{dataset} {request_id}",
                        )
                    else:
                        self.assertEqual(
                            count,
                            before_counts[dataset][request_id],
                            f"{request_id} changed multiplicity in {dataset} "
                            "without being replayed",
                        )
                forced[dataset] = sum(
                    counter[request_id] - before_counts[dataset][request_id]
                    for request_id in replayed
                )
        codes = collections.Counter(
            f"{signal}/{code.name}" for signal, code in server_codes
        )
        print(
            f"{kind} outage (client wait {call_wait}s): "
            f"{len(server_codes)} server refusals {dict(codes)}, "
            f"{len(local_waits)} client waits expired, "
            f"flush failures/retries {counters['flush.failures']:.0f}"
            f"/{counters['flush.retries']:.0f}, "
            f"Alloy {len(alloy_refusals)} refused exports and "
            f"{len(alloy_deadlines)} own deadlines, "
            f"incidental duplicate rows {duplicates}"
            + (f", forced by replay {forced}" if resend else "")
        )
        return {
            "duplicates": duplicates,
            "forced": forced,
            "client_waits": len(local_waits),
            "server_codes": codes,
        }


if __name__ == "__main__":
    unittest.main()
