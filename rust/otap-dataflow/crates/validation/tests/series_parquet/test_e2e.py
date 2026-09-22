# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Real OTLP producer, df_engine process and Parquet reader."""
import collections
import concurrent.futures
import contextlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid

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


def free_port():
    """Bind an ephemeral port and return it after closing the socket."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


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


def metric_request(request_id, unsupported=False):
    """Build an OTLP metrics request with one gauge and one histogram point.

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
    if unsupported:
        scope.metrics.add(name="summary").summary.data_points.add(count=1, sum=2.0)
    return req


class Engine:
    """A real `df_engine` process running the series Parquet example config."""

    def __init__(
        self,
        directory,
        storage=None,
        overrides=None,
        interval="1s",
        telemetry_interval=None,
    ):
        self.root = Path(directory)
        self.data = self.root / "data"
        self.data.mkdir(exist_ok=True)
        self.grpc_port = free_port()
        self.admin_port = free_port()
        self.config = yaml.safe_load(
            (WORKSPACE / "configs/series-parquet-local.yaml").read_text()
        )
        nodes = self.config["groups"]["default"]["pipelines"]["main"]["nodes"]
        nodes["receiver"]["config"]["protocols"]["grpc"]["listening_addr"] = (
            f"127.0.0.1:{self.grpc_port}"
        )
        export = nodes["exporter"]["config"]
        export["storage"] = storage or {"file": {"base_uri": str(self.data)}}
        export["window"]["interval"] = interval
        if overrides:
            for key, value in overrides.items():
                export[key] = value
        if telemetry_interval:
            # The exporter's gauges are sampled when the engine collects
            # telemetry, so a test that has to observe a short-lived state
            # needs the collection to be faster than that state.
            self.config["engine"]["telemetry"]["reporting_interval"] = (
                telemetry_interval
            )
        self.path = self.root / "pipeline.yaml"
        self.path.write_text(yaml.safe_dump(self.config))
        binary = Path(os.environ.get("DF_ENGINE", WORKSPACE / "target/debug/df_engine"))
        if not binary.is_file():
            raise AssertionError(f"build the feature-enabled engine first: {binary}")
        self.log = (self.root / "engine.log").open("w+")
        self.process = subprocess.Popen(
            [
                str(binary),
                "--config",
                str(self.path),
                "--http-admin-bind",
                f"127.0.0.1:{self.admin_port}",
            ],
            stdout=self.log,
            stderr=subprocess.STDOUT,
        )
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.grpc_port}")
        try:
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
        """Latest value of each named exporter gauge, from the admin API.

        Reads the Prometheus text the engine already exposes rather than
        adding a second reporting path. A gauge the engine has not published
        yet reads as zero, which is what the callers want: they wait for a
        state to appear.
        """
        url = f"http://127.0.0.1:{self.admin_port}/api/v1/telemetry/metrics"
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                body = response.read().decode()
        except Exception:
            return dict.fromkeys(names, 0.0)
        found = dict.fromkeys(names, 0.0)
        for line in body.splitlines():
            if line.startswith("#"):
                continue
            for name in names:
                if line.startswith(f"{name}{{") and "series_parquet" in line:
                    found[name] = float(line.rsplit(" ", 2)[-2])
        return found

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

    # Scenario: a gauge INT64_MAX and histogram arrive in one real OTLP request.
    # Guarantees: integer precision, histogram shape and wrapped timestamp
    # nullability survive Parquet; and each point row joins on series_id to
    # exactly one descriptor carrying the metric name, unit, kind,
    # temporality, scope and point attributes, so the identity split is
    # readable back with the latest-descriptor join the README documents.
    def test_number_and_histogram(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            try:
                metrics_rpc.MetricsServiceStub(engine.channel).Export(
                    metric_request("metric-0"), timeout=20
                )
                with duckdb.connect() as db:
                    number = str(
                        engine.data / "v=1/signal=metrics/dataset=number/**/*.parquet"
                    )
                    rows = db.execute(
                        "SELECT value_int, time_unix_nano FROM read_parquet(?)",
                        [number],
                    ).fetchall()
                    self.assertEqual(rows, [(2**63 - 1, None)])
                    histogram = str(
                        engine.data
                        / "v=1/signal=metrics/dataset=histogram/**/*.parquet"
                    )
                    rows = db.execute(
                        "SELECT count, bucket_counts, explicit_bounds "
                        "FROM read_parquet(?)",
                        [histogram],
                    ).fetchall()
                    self.assertEqual(rows, [(3, [1, 2], [1.0])])
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
                    joined = db.execute(
                        "SELECT s.metric_name, s.unit, s.metric_type, "
                        "s.temporality, s.scope_name, s.attrs['request.id'], "
                        "v.producer_id "
                        "FROM read_parquet(?) v JOIN latest s USING (series_id) "
                        "UNION ALL "
                        "SELECT s.metric_name, s.unit, s.metric_type, "
                        "s.temporality, s.scope_name, s.attrs['request.id'], "
                        "h.producer_id "
                        "FROM read_parquet(?) h JOIN latest s USING (series_id) "
                        "ORDER BY 1",
                        [number, histogram],
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
                                        "v=1/signal=metrics/dataset=number/"
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
                del metrics[:2]
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
                        "block_flushing_bytes", "block_active_bytes"
                    )
                    if gauges["block_flushing_bytes"] > 0 and (
                        gauges["block_active_bytes"] > 0
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


class AlloyProducer:
    """Grafana Alloy tailing a file into the engine's OTLP gRPC receiver.

    The container runs on the host network so that it can reach a receiver
    bound to loopback, and the reference River config the repository ships is
    the one it runs: the test exercises the documented deployment rather than
    a fixture of its own.
    """

    def __init__(self, directory, engine):
        self.root = Path(directory) / "alloy"
        self.engine = engine
        self.name = "series-alloy-" + uuid.uuid4().hex
        self.container = None

    def __enter__(self):
        if not sys.platform.startswith("linux"):
            unavailable("Alloy host-network fixture requires Linux")
        image = require_docker_image("alloy")
        self.root.mkdir(mode=0o755)
        self.lines = self.root / "events.log"
        self.lines.write_text("")
        config = self.root / "config.alloy"
        config.write_text((WORKSPACE / "configs/series-parquet.alloy").read_text())
        port = free_port()
        args = [
            "docker", "run", "--pull=never", "--detach", "--name", self.name,
            "--network", "host", "--user", f"{os.getuid()}:{os.getgid()}",
            "--mount", f"type=bind,src={self.root.resolve()},dst=/input,readonly",
            "-e", f"OTLP_ENDPOINT=127.0.0.1:{self.engine.grpc_port}",
            image, "run", "--storage.path=/tmp/alloy-state",
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
    the crate README documents. A disagreement between them is a defect in
    what was written, not in one reader.
    """
    root = Path(root).resolve()
    alloy_ids = set(alloy_ids)
    with duckdb.connect() as db, clickhouse_reader(root) as clickhouse:
        for signal, dataset in (
            ("logs", "values"),
            ("metrics", "number"),
            ("metrics", "histogram"),
        ):
            relative = f"v=1/signal={signal}/dataset={dataset}/**/*.parquet"
            paths = sorted(root.glob(relative))
            expected_count = len(log_ids) if signal == "logs" else metric_count
            if not paths:
                test.assertEqual(expected_count, 0, f"missing {signal}/{dataset}")
                continue
            series = f"v=1/signal={signal}/dataset=series/**/*.parquet"
            duck_values = (
                f"read_parquet({sql_string(root / relative)}, union_by_name=true)"
            )
            duck_series = (
                f"read_parquet({sql_string(root / series)}, union_by_name=true, "
                "filename=true)"
            )
            ch_values = f"file({sql_string(relative)}, 'Parquet')"
            ch_series = f"file({sql_string(series)}, 'Parquet')"
            projection = (
                "v.body, coalesce(v.attrs['e2e.source'], '')"
                if signal == "logs"
                else "s.attrs['request.id']"
            )
            duck_sql = f"""
                WITH canonical AS (
                    SELECT * FROM {duck_series}
                    QUALIFY row_number() OVER (
                        PARTITION BY series_id
                        ORDER BY emitted_at DESC, filename DESC) = 1
                )
                SELECT {projection} FROM {duck_values} v
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
                SELECT {projection} FROM {ch_values} AS v
                INNER JOIN canonical AS s ON v.series_id = s.series_id
            """
            duck_rows = sorted(db.execute(duck_sql).fetchall())
            ch_rows = sorted(clickhouse(ch_sql))
            test.assertEqual(
                ch_rows, duck_rows, f"reader disagreement for {signal}/{dataset}"
            )
            before = db.execute(f"SELECT count(*) FROM {duck_values}").fetchone()[0]
            ch_before = int(clickhouse(f"SELECT count(*) FROM {ch_values}")[0][0])
            test.assertEqual(ch_before, before)
            test.assertEqual(
                len(duck_rows),
                before,
                "latest-descriptor join must preserve cardinality",
            )
            if allow_duplicates:
                test.assertGreaterEqual(before, expected_count)
            else:
                test.assertEqual(before, expected_count)
            if signal == "logs":
                expected = sorted(
                    (body, "alloy-file" if body in alloy_ids else "") for body in log_ids
                )
                if allow_duplicates:
                    test.assertEqual(set(duck_rows), set(expected))
                else:
                    test.assertEqual(duck_rows, expected)
            elif metric_ids:
                test.assertEqual({row[0] for row in duck_rows}, set(metric_ids))


class DockerStore:
    """A MinIO or RustFS container serving S3 on a loopback port.

    The store keeps its data in its own container across a stop and start,
    which is what makes an outage recoverable; the exporter itself still holds
    nothing across a restart.
    """

    def __init__(self, kind):
        self.kind = kind
        self.name = "series-e2e-" + uuid.uuid4().hex
        self.container = None
        self.bucket = "series-test"
        self.key = "series-test-access"
        self.secret = "series-test-secret-12345"

    def __enter__(self):
        image = require_docker_image(self.kind)
        # The host port is chosen here rather than by Docker: a container that
        # is stopped and started again keeps an explicit mapping, so the
        # engine's configured endpoint stays valid across an outage.
        self.port = free_port()
        args = [
            "docker", "run", "--pull=never", "--detach", "--name", self.name,
            "--publish", f"127.0.0.1:{self.port}:9000",
        ]
        if self.kind == "minio":
            args += [
                "-e", f"MINIO_ROOT_USER={self.key}",
                "-e", f"MINIO_ROOT_PASSWORD={self.secret}",
                image, "server", "/data", "--address", ":9000",
            ]
        else:
            args += [
                "-e", f"RUSTFS_ACCESS_KEY={self.key}",
                "-e", f"RUSTFS_SECRET_KEY={self.secret}",
                "-e", "RUSTFS_ADDRESS=0.0.0.0:9000",
                "-e", "RUSTFS_VOLUMES=/data",
                image,
            ]
        try:
            self.container = subprocess.check_output(args, text=True).strip()
            mapping = subprocess.check_output(
                ["docker", "port", self.container, "9000/tcp"], text=True
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
            ["docker", "logs", self.container], stderr=subprocess.STDOUT, text=True
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
        )

    def recover(self):
        """Start the same container again and wait for it to serve S3."""
        subprocess.run(
            ["docker", "start", self.container], check=True, capture_output=True
        )
        self.ready()

    def __exit__(self, *exc):
        if self.container:
            subprocess.run(
                ["docker", "rm", "--force", "--volumes", self.container],
                check=False,
                capture_output=True,
            )
            self.container = None


def verify_files(test, root, log_ids, metric_count, allow_duplicates=False):
    """Read every downloaded part file and check the lake's own invariants.

    Each file is checked against the metadata it carries: the recorded row
    count, the sort key it claims and, for a descriptor file, that every
    stored identity really hashes to the series id it is filed under. The
    values rows of a partition must then be covered by the descriptors of the
    same signal, partition and writer, which is what makes the lake readable
    without a catalog.
    """
    files = sorted(Path(root).rglob("*.parquet"))
    test.assertTrue(files)
    coverage = set()
    values = []
    bodies = []
    counts = {"number": 0, "histogram": 0}
    with duckdb.connect() as db:
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
                if dataset == "values":
                    bodies.extend(
                        row[0]
                        for row in db.execute(
                            "SELECT body FROM read_parquet(?)", [str(path)]
                        ).fetchall()
                    )
                else:
                    counts[dataset] += len(keys)
        test.assertTrue(
            set(values).issubset(coverage),
            "descriptor coverage per partition and worker",
        )
        test.assertTrue(set(log_ids).issubset(set(bodies)))
        if not allow_duplicates:
            test.assertEqual(sorted(bodies), sorted(log_ids))
            test.assertEqual(counts, {"number": metric_count, "histogram": metric_count})
        else:
            test.assertGreaterEqual(counts["number"], metric_count)
            test.assertGreaterEqual(counts["histogram"], metric_count)
        for signal, datasets in (
            ("logs", ("values",)),
            ("metrics", ("number", "histogram")),
        ):
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


class DockerSlice(unittest.TestCase):
    """Grafana Alloy and a real S3 object store in containers."""

    def exercise(self, kind):
        """Run the Alloy file-tail topology against one object store."""
        require_clickhouse()
        with DockerStore(kind) as store, tempfile.TemporaryDirectory() as directory:
            with Engine(directory, storage=store.storage) as engine:
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
                    "retry_timeout": "5s",
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
                        time.sleep(8)
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
                            engine_metrics(engine), "block.requests_pending"
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
                second, storage=store.storage, overrides={"window": {"interval": "1s"}}
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
                        "interval": "3153600000s",
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
                self.assertEqual(windows, [("0",)])
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
                    call = engine.logs.Export.future(
                        log_request("disconnected"), timeout=20
                    )
                    deadline = time.monotonic() + 4
                    while time.monotonic() < deadline:
                        metrics = engine_metrics(engine)
                        if metric_max(metrics, "block.requests_pending") >= 1:
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


def metric_max(document, name):
    """The largest reported value of one exporter metric in a snapshot."""
    values = [
        item["value"]
        for group in document["metric_sets"]
        if group["name"] == "exporter.series_parquet"
        for item in group["metrics"]
        if item["name"] == name and isinstance(item["value"], (int, float))
    ]
    return max(values, default=0)


if __name__ == "__main__":
    unittest.main()
