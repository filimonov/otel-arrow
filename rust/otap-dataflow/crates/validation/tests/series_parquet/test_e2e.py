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
import urllib.request

import duckdb
import grpc
import yaml
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2 as logs_pb
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc as logs_rpc

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


class Engine:
    """A real `df_engine` process running the series Parquet example config."""

    def __init__(self, directory, storage=None, overrides=None, interval="1s"):
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


if __name__ == "__main__":
    unittest.main()
