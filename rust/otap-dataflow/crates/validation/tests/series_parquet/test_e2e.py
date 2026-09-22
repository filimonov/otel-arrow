# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Real OTLP producer, df_engine process and Parquet reader."""
import collections
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request

import duckdb
import grpc
import yaml
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


if __name__ == "__main__":
    unittest.main()
