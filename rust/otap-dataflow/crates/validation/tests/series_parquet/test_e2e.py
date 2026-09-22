# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Real OTLP producer, df_engine process and Parquet reader."""
import collections
import concurrent.futures
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
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
        log_level=None,
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
        if log_level:
            # Set explicitly rather than through RUST_LOG, which the engine
            # only consults when the configuration omits a level: a test that
            # reads a DEBUG event must not depend on the caller's environment.
            self.config["engine"]["telemetry"]["logs"] = {"level": log_level}
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


def alloy_producer_id():
    """The `host.id` the reference Alloy config stamps on its resource.

    Read out of the shipped config rather than repeated here, so the test
    cannot drift away from the deployment it documents.
    """
    text = (WORKSPACE / "configs/series-parquet.alloy").read_text()
    found = re.search(r'set\(attributes\["host\.id"\], "([^"]+)"\)', text)
    if not found:
        raise AssertionError("the Alloy config no longer sets a resource host.id")
    return found.group(1)


# Columns every dataset must contribute to the canonical row, checked against
# the columns the files actually carry so the comparison cannot quietly shrink.
REQUIRED_COLUMNS = {
    ("logs", "values"): {
        "series_id", "producer_id", "time", "time_unix_nano", "observed_time",
        "observed_time_unix_nano", "severity_number", "severity_text", "body",
        "event_name", "trace_id", "span_id", "flags", "attrs", "service_name",
    },
    ("metrics", "number"): {
        "series_id", "producer_id", "metric_name", "time", "time_unix_nano",
        "start_time", "start_time_unix_nano", "flags", "value_int",
        "value_double", "service_name",
    },
    ("metrics", "histogram"): {
        "series_id", "producer_id", "metric_name", "time", "time_unix_nano",
        "start_time", "start_time_unix_nano", "flags", "count", "sum", "min",
        "max", "bucket_counts", "explicit_bounds", "service_name",
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
            "x -> coalesce(CAST(x AS VARCHAR), 'null')), ','), 'null')",
            f"ifNull(arrayStringConcat(arrayMap(x -> ifNull(toString(x), 'null'), "
            f"{column}), ','), 'null')",
        )
    if kind == "DOUBLE[]":
        return (
            f"coalesce(array_to_string(list_transform({column}, "
            "x -> coalesce(CAST(CAST(x AS BIT) AS VARCHAR), 'null')), ','), 'null')",
            "ifNull(arrayStringConcat(arrayMap(x -> if(isNull(x), 'null', "
            f"bin(reverse(reinterpretAsFixedString(assumeNotNull(x))))), {column}), "
            "','), 'null')",
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


# The frozen object layout of spec section 5.3. The writer id is matched
# loosely because it is user configured and may itself contain a dash; the
# boot id and sequence have fixed shapes, so the split stays unambiguous.
PART_NAME = re.compile(
    r"^v=1/signal=(?P<signal>logs|metrics)"
    r"/dataset=(?P<dataset>series|values|number|histogram)"
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


def flush_attempts(engine):
    """The object names the exporter announced for each write attempt.

    The exporter emits `series_parquet.flush_attempt` before every attempt.
    Every object of one block carries the same file name and differs only in
    its dataset directory, so the announced name and count are the whole set
    of names that attempt was about to write. Reading them back is what lets a
    test compare the names a failed attempt was going to use with the names
    the retry actually wrote.
    """
    return [
        (int(attempt), file, int(count))
        for attempt, file, count in re.findall(
            r"series_parquet\.flush_attempt.*?"
            r"\[attempt=(\d+), file=(\S+), objects=(\d+)\]",
            engine.engine_log(),
        )
    ]


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
            best[name] = max(best[name], metric_max(document, name))
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

    def exercise_outage(self, kind, store, directory):
        """One store: stop it under load, recover it, and check the lake."""
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
                                call, wait = stub.Export.future(request), CALL_WAIT
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
                        samples.append((document, rss_bytes(engine.process.pid)))
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
                                    metric_max(document, name)
                                    for document, _ in samples
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
                    # Alloy's own client deadline is 6s, so a refusal it was
                    # still waiting for shows up as its deadline rather than
                    # as a status. Those prove the attempt just as well; the
                    # statuses the engine did return must all be UNAVAILABLE.
                    alloy_deadlines = [
                        line
                        for line in alloy_failures
                        if "code = DeadlineExceeded" in line
                    ]
                    alloy_refusals = [
                        line
                        for line in alloy_failures
                        if "code = DeadlineExceeded" not in line
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
                        samples.append(
                            (engine_metrics(engine), rss_bytes(engine.process.pid))
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
                for document, rss in samples:
                    self.assertLessEqual(
                        metric_max(document, "block.active_bytes"), 8 << 20
                    )
                    self.assertLessEqual(
                        metric_max(document, "block.flushing_bytes"), 8 << 20
                    )
                    self.assertLessEqual(
                        metric_max(document, "block.pending_slot_occupied"), 1
                    )
                    self.assertLessEqual(metric_max(document, "notify.queued"), 16)
                    budget = metric_max(document, "memory.budget_bytes")
                    self.assertGreater(
                        budget, 0, "missing budget telemetry is a failure"
                    )
                    self.assertLessEqual(rss, baseline_rss + budget + (128 << 20))
                # The engine recovers in place: no restart, and the backlog
                # drains back to an empty pending set.
                self.assertIsNone(engine.process.poll(), "the engine did not survive")
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    document = engine_metrics(engine)
                    if (
                        metric_max(document, "oldest_unacked_seconds") <= 1
                        and metric_max(document, "block.requests_pending") == 0
                    ):
                        break
                    time.sleep(0.2)
                else:
                    self.fail("oldest unacked age did not return to baseline")
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
            for dataset in ("number", "histogram"):
                path = str(
                    downloaded / f"v=1/signal=metrics/dataset={dataset}/**/*.parquet"
                )
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
                    f"missing precomputed {dataset} IDs",
                )
                duplicates[dataset] = len(stored) - len(expected_metric_ids)
        self.assertTrue(all(count >= 0 for count in duplicates.values()))
        codes = collections.Counter(
            f"{signal}/{code.name}" for signal, code in server_codes
        )
        print(
            f"{kind} outage: {len(server_codes)} server refusals {dict(codes)}, "
            f"{len(local_waits)} client waits expired, "
            f"flush failures/retries {counters['flush.failures']:.0f}"
            f"/{counters['flush.retries']:.0f}, "
            f"Alloy {len(alloy_refusals)} refused exports and "
            f"{len(alloy_deadlines)} own deadlines, "
            f"duplicate rows {duplicates}"
        )


if __name__ == "__main__":
    unittest.main()
