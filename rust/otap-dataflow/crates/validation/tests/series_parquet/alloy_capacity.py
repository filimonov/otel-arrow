# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""The Alloy-as-producer confirmation of the capacity family.

The capacity numbers come from a Python generator that sends 1000-record
requests over hundreds of connections. This trial asks whether a real
producer sees the same engine: Grafana Alloy, running the strict River
config (`configs/series-parquet-strict.alloy`, its batching included), tails
one log file into the strict engine on four local-store workers.

A feeder process owns the file and a recording tap. It appends fixed-width
lines at the offered rate, holding at most `BACKLOG_LINES` lines ahead of
what Alloy has read, so a producer slower than the offered rate is measured
at its own maximum instead of building a backlog it drains for minutes.
Alloy exports to the tap, and the tap forwards every request unchanged to
the engine over one upstream connection per Alloy connection, so it
records each request's records, bytes and engine response time and passes
the engine's status back to Alloy. Alloy's own histograms cannot give the
request sizes: its queue-batch histogram records the one-record requests
the Loki bridge enqueues, before they are merged.

The oracle is the E2E read-back: rows per source file and `e2e.source`,
read by DuckDB and by ClickHouse, the latest-descriptor join, and every
line's sequence number. Every written line must be stored exactly once: a
missing line and a duplicated one both fail the trial.
"""

import collections
import contextlib
import json
import math
import multiprocessing
import os
from pathlib import Path
import queue
import re
import shutil
import threading
import time
import urllib.request

try:
    from . import capacity
    from . import measurement
    from . import performance
    from . import test_e2e
except ImportError:
    import capacity
    import measurement
    import performance
    import test_e2e

WORKLOAD_ID = "alloy-file"
# Every line is this many bytes before its newline, so the file size says
# how many lines were written and a body says which line it is.
BODY_BYTES = 100
LINE_PREFIX = "alloy "
SEQ_DIGITS = 12
# How far the writer may run ahead of Alloy's reads.
BACKLOG_LINES = 1_000_000
WRITE_PERIOD_S = 0.01
POLL_PERIOD_S = 0.5
# The strict engine's producer, and the attempt timeout it falls back to.
STRICT_ALLOY_CONFIG = "configs/series-parquet-strict.alloy"
ATTEMPT_TIMEOUT = "180s"
# After the writer stops, how long Alloy may go without a successful send
# before the trial stops waiting, and the longest it waits in all.
STALL_S = 60
DRAIN_DEADLINE_S = 900
FEEDER_READY_DEADLINE_S = 60
TAP_WORKERS = 32
SERIES_EXPORTER = "otelcol.exporter.otlp.series"
EXPORT_METHOD = "opentelemetry.proto.collector.logs.v1.LogsService/Export"
# The receiver decoding limit a variant sets; None keeps the shipped
# default (tonic's 4 MiB).
VARIANTS = {
    "shipped": None,
    "decoding_16mib": "16MiB",
}


def line(seq) -> str:
    """The body of line `seq`: its sequence number padded to BODY_BYTES."""
    head = f"{LINE_PREFIX}{seq:0{SEQ_DIGITS}d} "
    return head + "x" * (BODY_BYTES - len(head))


def seq_of(body):
    """The sequence number a body carries, or None for a foreign body."""
    if len(body) != BODY_BYTES or not body.startswith(LINE_PREFIX):
        return None
    digits = body[len(LINE_PREFIX):len(LINE_PREFIX) + SEQ_DIGITS]
    return int(digits) if digits.isdigit() else None


def writer_allowance(elapsed_s, rate, read_lines, backlog_lines=BACKLOG_LINES) -> tuple:
    """Lines the writer may have written by `elapsed_s`, and whether the
    backlog bound, not the offered rate, set that number."""
    due = int(elapsed_s * rate)
    bound = int(read_lines) + backlog_lines
    return (min(due, bound), bound < due)


def _labels(text):
    return dict(re.findall(r'(\w+)="([^"]*)"', text))


def parse_metrics(body) -> dict:
    """What the trial reads from Alloy's Prometheus page.

    Only the series exporter's logs queue and the file source count; the
    call-duration buckets are kept per response status.
    """
    found = {
        "read_lines": 0.0, "sent_records": 0.0, "send_failed_records": 0.0,
        "enqueue_failed_records": 0.0, "queue_size_records": None, "in_flight_requests": None,
        "calls": {}, "call_buckets": {},
    }
    for raw in body.splitlines():
        if not raw or raw.startswith("#"):
            continue
        head, _, value = raw.rpartition(" ")
        name, _, rest = head.partition("{")
        labels = _labels(rest)
        try:
            number = float(value)
        except ValueError:
            continue
        if name == "loki_source_file_read_lines_total":
            found["read_lines"] += number
            continue
        if labels.get("component_id") != SERIES_EXPORTER:
            continue
        if name == "otelcol_exporter_sent_log_records_total":
            found["sent_records"] += number
        elif name == "otelcol_exporter_send_failed_log_records_total":
            found["send_failed_records"] += number
        elif name == "otelcol_exporter_enqueue_failed_log_records_total":
            found["enqueue_failed_records"] += number
        elif name == "otelcol_exporter_queue_size" and labels.get("data_type") == "logs":
            found["queue_size_records"] = number
        elif name == "otelcol_exporter_in_flight_requests" and labels.get("data_type") == "logs":
            found["in_flight_requests"] = number
        elif name.startswith("rpc_client_call_duration_seconds") and \
                labels.get("rpc_method") == EXPORT_METHOD:
            status = labels.get("rpc_response_status_code", "")
            if name.endswith("_count"):
                found["calls"][status] = found["calls"].get(status, 0) + number
            elif name.endswith("_bucket"):
                le = float("inf") if labels["le"] == "+Inf" else float(labels["le"])
                buckets = found["call_buckets"].setdefault(status, {})
                buckets[le] = buckets.get(le, 0) + number
    found["call_buckets"] = {
        status: sorted(buckets.items()) for status, buckets in found["call_buckets"].items()
    }
    return found


def bucket_quantile(buckets, q):
    """The upper bound of the cumulative bucket holding quantile `q`."""
    if not buckets or not buckets[-1][1]:
        return None
    total = buckets[-1][1]
    for bound, count in buckets:
        if count >= q * total:
            return bound
    return buckets[-1][0]


def json_bound(value):
    """A bucket bound as JSON can carry it: the open bucket is "+Inf"."""
    return "+Inf" if value == float("inf") else value


def quantile(values, q):
    """Nearest-rank quantile of `values`, or None."""
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))]


def distribution(values) -> dict:
    return {
        "count": len(values),
        "p50": quantile(values, 0.50),
        "p95": quantile(values, 0.95),
        "p99": quantile(values, 0.99),
        "max": max(values) if values else None,
        "mean": sum(values) / len(values) if values else None,
    }


def count_records(raw) -> int:
    """Log records in one serialized logs export request."""
    message = test_e2e.logs_pb.ExportLogsServiceRequest.FromString(raw)
    return sum(len(scope.log_records) for resource in message.resource_logs
               for scope in resource.scope_logs)


class RequestTap:
    """A gRPC logs endpoint that forwards each request unchanged.

    Each distinct client connection (gRPC peer) gets its own upstream
    channel with a local subchannel pool, so the engine sees as many
    connections as the producer opened. The engine's response bytes, or
    its status code and details, go back to the producer as they came.
    Records are counted off the call path.
    """

    def __init__(self, target, *, message_limit=capacity.MESSAGE_LIMIT_BYTES,
                 workers=TAP_WORKERS):
        self.target = target
        self.message_limit = message_limit
        self.workers = workers
        self.requests = []
        self.lock = threading.Lock()
        self.channels = {}
        self.pending = queue.Queue()
        self.server = None
        self.counter = None
        self.port = None

    def _call(self, peer):
        with self.lock:
            entry = self.channels.get(peer)
            if entry is None:
                channel = test_e2e.grpc.insecure_channel(self.target, options=[
                    ("grpc.use_local_subchannel_pool", 1),
                    ("grpc.max_send_message_length", self.message_limit),
                    ("grpc.max_receive_message_length", self.message_limit),
                ])
                entry = (channel, channel.unary_unary(
                    "/" + EXPORT_METHOD, request_serializer=None, response_deserializer=None))
                self.channels[peer] = entry
            return entry[1]

    def _export(self, raw, context):
        grpc = test_e2e.grpc
        received = time.monotonic_ns()
        peer = context.peer()
        call = self._call(peer)
        remaining = context.time_remaining()
        code = "OK"
        details = ""
        response = None
        try:
            response = call(raw, timeout=remaining if remaining else None)
        except grpc.RpcError as error:
            code = error.code().name
            details = str(error.details() or "")[:300]
        answered = time.monotonic_ns()
        with self.lock:
            index = len(self.requests)
            self.requests.append({
                "received_ns": received, "answered_ns": answered, "bytes": len(raw),
                "records": None, "peer": peer, "code": code, "details": details,
            })
        self.pending.put((index, raw))
        if response is None:
            context.abort(getattr(grpc.StatusCode, code), details)
        return response

    def _count(self):
        while True:
            item = self.pending.get()
            if item is None:
                return
            index, raw = item
            try:
                records = count_records(raw)
            except Exception:  # noqa: BLE001 - a malformed request counts -1
                records = -1
            with self.lock:
                self.requests[index]["records"] = records

    def start(self) -> int:
        grpc = test_e2e.grpc
        from concurrent import futures
        handler = grpc.method_handlers_generic_handler(
            EXPORT_METHOD.split("/")[0],
            {"Export": grpc.unary_unary_rpc_method_handler(
                self._export, request_deserializer=None, response_serializer=None)},
        )
        self.server = grpc.server(
            futures.ThreadPoolExecutor(self.workers),
            options=[("grpc.max_receive_message_length", self.message_limit),
                     ("grpc.max_send_message_length", self.message_limit)],
        )
        self.server.add_generic_rpc_handlers((handler,))
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.counter = threading.Thread(target=self._count, name="tap-count", daemon=True)
        self.counter.start()
        return self.port

    def stop(self, grace_s=5):
        if self.server is not None:
            self.server.stop(grace_s).wait(grace_s + 5)
        self.pending.put(None)
        if self.counter is not None:
            self.counter.join(timeout=120)
        for channel, _call in self.channels.values():
            channel.close()


def fetch_metrics(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return parse_metrics(response.read().decode("ascii", "replace"))


def _feeder_process(pipe, config):
    """The tap, the file writer and the Alloy poller, on producer cores."""
    try:
        capacity.confine_threads(config["cpus"])
        tap = RequestTap(config["target"])
        port = tap.start()
        capacity.confine_threads(config["cpus"])
        pipe.send({"ready": True, "pid": os.getpid(), "tap_port": port})
        go = pipe.recv()
        start_ns, end_ns = go["start_ns"], go["end_ns"]
        rate, url = config["rate"], go["metrics_url"]
        polled = []
        latest = {"read_lines": 0.0}
        stop_polling = threading.Event()

        def poll():
            while not stop_polling.is_set():
                with contextlib.suppress(OSError, ValueError):
                    sample = fetch_metrics(url)
                    sample["monotonic_ns"] = time.monotonic_ns()
                    sample.pop("call_buckets", None)
                    latest["read_lines"] = sample["read_lines"]
                    polled.append(sample)
                stop_polling.wait(POLL_PERIOD_S)

        poller = threading.Thread(target=poll, name="alloy-poll", daemon=True)
        poller.start()
        capacity.confine_threads(config["cpus"])
        cpu_before = os.times()
        written = 0
        throttled_ns = 0
        timeline = []
        wake = threading.Event()
        now = time.monotonic_ns()
        if start_ns > now:
            _ = wake.wait((start_ns - now) / 1e9)
        next_mark = start_ns
        with open(config["lines_path"], "a", encoding="ascii") as stream:
            while True:
                now = time.monotonic_ns()
                if now >= end_ns:
                    break
                allowed, bound = writer_allowance(
                    (now - start_ns) / 1e9, rate, latest["read_lines"], config["backlog_lines"])
                if allowed > written:
                    stream.write("".join(line(seq) + "\n" for seq in range(written, allowed)))
                    stream.flush()
                    written = allowed
                if bound:
                    throttled_ns += int(WRITE_PERIOD_S * 1e9)
                if now >= next_mark:
                    timeline.append((now, written))
                    next_mark += int(1e9)
                _ = wake.wait(WRITE_PERIOD_S)
            stream.flush()
            os.fsync(stream.fileno())
        timeline.append((time.monotonic_ns(), written))
        cpu_written = os.times()
        pipe.send({"written": written})
        _ = pipe.recv()  # stop
        stop_polling.set()
        poller.join(timeout=30)
        tap.stop()
        cpu_after = os.times()
        result = {
            "pid": os.getpid(),
            "written": written,
            "throttled_s": throttled_ns / 1e9,
            "writer_timeline": timeline,
            "polled": polled,
            "requests": tap.requests,
            "upstream_connections": len(tap.channels),
            "writer_cpu_s": (cpu_written.user - cpu_before.user)
            + (cpu_written.system - cpu_before.system),
            "cpu_s": (cpu_after.user - cpu_before.user) + (cpu_after.system - cpu_before.system),
            "complete": True,
        }
        Path(config["result_path"]).write_text(json.dumps(result), encoding="ascii")
        pipe.send({"done": True})
    except BaseException as error:  # noqa: BLE001 - reported to the parent
        with contextlib.suppress(Exception):
            pipe.send({"error": f"{type(error).__name__}: {error}"})
        raise


class Feeder:
    """The feeder process, started, released, stopped and reaped."""

    def __init__(self, *, target, rate, cpus, directory, lines_path=None):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.config = {
            "target": target, "rate": rate, "cpus": list(cpus),
            "backlog_lines": BACKLOG_LINES, "lines_path": str(lines_path) if lines_path else None,
            "result_path": str(self.directory / "feeder.json"),
        }
        self.context = multiprocessing.get_context("spawn")
        self.pipe = None
        self.process = None

    def start(self, lines_path) -> dict:
        self.config["lines_path"] = str(lines_path)
        self.pipe, child = self.context.Pipe()
        self.process = self.context.Process(
            target=_feeder_process, args=(child, self.config), name="alloy-feeder", daemon=True)
        self.process.start()
        return self._receive(FEEDER_READY_DEADLINE_S)

    @property
    def pid(self):
        return self.process.pid

    def _receive(self, deadline_s):
        if not self.pipe.poll(deadline_s):
            raise AssertionError("the Alloy feeder did not answer")
        message = self.pipe.recv()
        if "error" in message:
            raise AssertionError(f"Alloy feeder: {message['error']}")
        return message

    def go(self, start_ns, end_ns, metrics_url):
        self.pipe.send({"start_ns": start_ns, "end_ns": end_ns, "metrics_url": metrics_url})

    def written(self, deadline_s) -> int:
        return self._receive(deadline_s)["written"]

    def finish(self, deadline_s=300) -> dict:
        self.pipe.send({"stop": True})
        _ = self._receive(deadline_s)
        self.process.join(timeout=30)
        return json.loads(Path(self.config["result_path"]).read_text(encoding="ascii"))

    def close(self):
        if self.process is not None and self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=10)
        if self.pipe is not None:
            self.pipe.close()


class _Target:
    """What `AlloyProducer` needs of an engine: the port it exports to."""

    def __init__(self, port):
        self.grpc_port = port


def split_producer_cpus(cpu_sets) -> tuple:
    """Alloy's cores and the feeder's: the feeder takes the last two
    physical cores of the producer placement, Alloy the rest."""
    groups = [list(group) for group in cpu_sets]
    if len(groups) < 3:
        raise AssertionError(f"the Alloy trial needs three producer cores, has {groups}")
    feeder = sorted(core for group in groups[-2:] for core in group)
    alloy = sorted(core for group in groups[:-2] for core in group)
    return alloy, feeder


def shipped_alloy_settings() -> dict:
    """The batching and concurrency the strict River config ships."""
    text = (test_e2e.WORKSPACE / STRICT_ALLOY_CONFIG).read_text()

    def number(name):
        found = re.search(rf"\b{name}\s*=\s*(\d+)", text)
        return int(found.group(1)) if found else None

    return {
        "num_consumers": number("num_consumers"),
        "queue_size_records": number("queue_size"),
        "batch_min_size_records": number("send_batch_size"),
        "batch_max_size_records": number("send_batch_max_size"),
        "compression": (re.search(r'compression\s*=\s*"([^"]+)"', text) or [None, None])[1],
    }


def alloy_experiment(plan, trial, spec, result, run_dir, controls):
    """One Alloy trial: engine, tap, Alloy, measured interval, drain, read-back."""
    command = capacity._command()
    run_dir = Path(run_dir)
    controls.coverage_gaps_hard = True
    controls.allocate(plan["allocation"])
    controls.register("harness", os.getpid())
    alloy_cpus, feeder_cpus = split_producer_cpus(plan["producer_cpu_sets"])
    result["environment"]["build"] = plan["provenance"]["build"]
    result["environment"]["git"] = plan["provenance"]["git"]
    result["environment"]["producer"] = {
        "cpus": plan["allocation"]["producer"], "alloy_cpus": alloy_cpus,
        "feeder_cpus": feeder_cpus, "alloy_image": test_e2e.IMAGE_DEFAULTS["alloy"],
    }
    result["ephemeral_values"] = dict(plan["ephemeral"])
    # An earlier trial's objects would read back as this one's lines.
    if plan["store"] is not None:
        result["capacity_store_cleared_objects_count"] = performance.clear_store(plan["store"])
    root = run_dir / "engine"
    root.mkdir(parents=True, exist_ok=True)
    settings = capacity.engine_settings(plan, trial, root)
    with capacity.malloc_conf(capacity.JEMALLOC_STATS_CONF):
        engine = test_e2e.Engine(root, **settings)
    phase = None
    feeder = None
    alloy = None
    observed = {}
    try:
        result["config"]["effective"] = engine.config
        result["config"]["effective_sha256"] = engine.config_sha256
        result["config"]["edges"] = [list(edge) for edge in engine.edges]
        result["config"]["malloc_conf"] = capacity.JEMALLOC_STATS_CONF
        result["config"]["alloy_river"] = (
            test_e2e.WORKSPACE / STRICT_ALLOY_CONFIG).read_text()
        result["ephemeral_values"]["<receiver_listening_addr>"] = f"127.0.0.1:{engine.grpc_port}"
        command.record_graph(result, engine, trial["topology"])
        feeder = Feeder(target=f"127.0.0.1:{engine.grpc_port}", rate=trial["rate"],
                        cpus=feeder_cpus, directory=run_dir / "feeder")
        # The tap needs the engine; Alloy needs the tap, and creates the
        # file the feeder appends to.
        alloy = test_e2e.AlloyProducer(run_dir, _Target(0), timeout=ATTEMPT_TIMEOUT,
                                       config=test_e2e.WORKSPACE / STRICT_ALLOY_CONFIG)
        # A run directory reused after an aborted attempt keeps its old file.
        shutil.rmtree(alloy.root, ignore_errors=True)
        ready = feeder.start(alloy.root / "events.log")
        alloy.engine = _Target(ready["tap_port"])
        controls.register("producer_0", feeder.pid, feeder_cpus)
        _ = alloy.__enter__()
        observed["alloy_pinned"] = performance.pin_container(alloy.container, alloy_cpus)
        alloy_pid = alloy.host_pid()
        controls.register("producer_1", alloy_pid, alloy_cpus)
        metrics_url = f"http://127.0.0.1:{alloy.admin_port}/metrics"
        phase = capacity.CapacityPhase(command, "trial", engine, spec, controls, False)
        snapshot = phase.ready("start")
        worker_tids = set(measurement.worker_tids(snapshot))
        _ = command.await_window_start(spec.interval_s)
        controls.raise_if_invalid()
        start_ns = time.monotonic_ns() + 200_000_000
        window = (start_ns + int(trial["warmup_s"] * 1e9),
                  start_ns + int((trial["warmup_s"] + trial["measure_s"]) * 1e9))
        _ = performance.reset_peak_rss(engine.pid)
        feeder.go(start_ns, window[1], metrics_url)
        readings = {}
        wake = threading.Event()
        for label, at in (("start", window[0]), ("middle", (window[0] + window[1]) // 2),
                          ("end", window[1])):
            now = time.monotonic_ns()
            if at > now:
                _ = wake.wait((at - now) / 1e9)
            readings[label] = {
                "monotonic_ns": time.monotonic_ns(),
                "engine_cpu_ns": performance.procfs_cpu_ns(engine.pid),
                "threads": performance.thread_times(engine.pid),
                "alloy_cpu_ns": performance.procfs_cpu_ns(alloy_pid),
                "alloy_rss_bytes": measurement.process_tree_rss(alloy_pid),
                "feeder_cpu_ns": performance.procfs_cpu_ns(feeder.pid),
                "diskstats": capacity.diskstats(),
                "alloy_to_tap": capacity.established_connections(ready["tap_port"]),
                "tap_to_engine": capacity.established_connections(engine.grpc_port),
            }
            controls.raise_if_invalid()
        written = feeder.written(120)
        observed["written"] = written
        # Alloy owns what it has read and queued; wait for it to send it.
        deadline = time.monotonic() + DRAIN_DEADLINE_S
        last_progress = time.monotonic()
        last_sent = -1.0
        drained = None
        while time.monotonic() < deadline:
            with contextlib.suppress(OSError, ValueError):
                sample = fetch_metrics(metrics_url)
                if sample["sent_records"] >= written:
                    drained = sample
                    break
                if sample["sent_records"] > last_sent:
                    last_sent = sample["sent_records"]
                    last_progress = time.monotonic()
            if time.monotonic() - last_progress > STALL_S:
                break
            time.sleep(POLL_PERIOD_S)
        observed["alloy_drained"] = drained is not None
        observed["alloy_drained_ns"] = time.monotonic_ns()
        observed["alloy_final"] = fetch_metrics(metrics_url)
        controls.raise_if_invalid()
        alloy.__exit__(None, None, None)
        observed["alloy_failures"] = [
            entry for entry in (alloy.root / "alloy.log").read_text().splitlines()
            if "Exporting failed" in entry and f"component_id={SERIES_EXPORTER}" in entry
        ] if (alloy.root / "alloy.log").is_file() else []
        feed = feeder.finish()
        _ = phase.drained()
        observed["peak_rss_bytes"] = performance.procfs_peak_rss(engine.pid)
        _ = controls.snapshot(
            "end", {"engine": (engine.pid, list(spec.cores))},
            workers=phase.workers, requested_cores=list(spec.cores),
        )
        controls.unwatch_workers()
        observed["log_path"] = str(root / "engine.log")
        engine.shutdown(command.SHUTDOWN_DEADLINE_S)
        command.record_event(result, "engine_shut_down", str(engine.pid))
    finally:
        controls.unwatch_workers()
        if phase is not None and phase.sampler is not None:
            phase.sampler.stop()
        if alloy is not None and alloy.container:
            alloy.__exit__(None, None, None)
        if feeder is not None:
            feeder.close()
        engine.close()
    settle_alloy(plan, trial, spec, result, run_dir, phase, feed, readings, window,
                 worker_tids, observed, engine)


def read_back(root, written) -> dict:
    """The E2E read-back of the logs values rows: count per source file.

    DuckDB and ClickHouse each count the rows of every source file; DuckDB
    also checks every body's sequence number against the lines written,
    the resource producer on the latest descriptor, and that the
    latest-descriptor join keeps every row.
    """
    import duckdb
    root = Path(root).resolve()
    relative = "v=1/signal=logs/dataset=values/**/*.parquet"
    series = "v=1/signal=logs/dataset=series/**/*.parquet"
    problems = []
    if not list(root.glob(relative)):
        return {"passed": written == 0, "problems": ["no logs values files"] if written else [],
                "files": {}, "expected_lines": written}
    values = (f"read_parquet({test_e2e.sql_string(root / relative)}, union_by_name=true, "
              "hive_partitioning=false)")
    descriptors = (f"read_parquet({test_e2e.sql_string(root / series)}, union_by_name=true, "
                   "filename=true, hive_partitioning=false)")
    seq = (f"TRY_CAST(substr(body, {len(LINE_PREFIX) + 1}, {SEQ_DIGITS}) AS BIGINT)")
    well_formed = (f"length(body) = {BODY_BYTES} AND starts_with(body, '{LINE_PREFIX}') "
                   f"AND {seq} IS NOT NULL")
    with duckdb.connect() as db:
        rows = db.execute(f"""
            SELECT coalesce(attrs['log.file.path'], '') AS source_file,
                   coalesce(attrs['e2e.source'], '') AS source,
                   count(*), count(DISTINCT CASE WHEN {well_formed} THEN {seq} END),
                   min(CASE WHEN {well_formed} THEN {seq} END),
                   max(CASE WHEN {well_formed} THEN {seq} END),
                   count(*) FILTER (WHERE NOT ({well_formed})),
                   list(DISTINCT producer_id)
            FROM {values} GROUP BY 1, 2 ORDER BY 1, 2
        """).fetchall()
        multiplicity = dict(db.execute(f"""
            SELECT copies, count(*) FROM (
                SELECT {seq} AS seq, count(*) AS copies FROM {values}
                WHERE {well_formed} GROUP BY 1) GROUP BY 1 ORDER BY 1
        """).fetchall())
        total = db.execute(f"SELECT count(*) FROM {values}").fetchone()[0]
        joined = db.execute(f"""
            WITH canonical AS (
                SELECT * FROM {descriptors}
                QUALIFY row_number() OVER (
                    PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1)
            SELECT count(*), list(DISTINCT coalesce(
                list_extract(map_extract(s.resource_attrs, 'host.id'), 1), ''))
            FROM {values} v INNER JOIN canonical s ON v.series_id = s.series_id
        """).fetchone()
    with test_e2e.clickhouse_reader(root) as clickhouse:
        clickhouse_rows = {
            (row[0], row[1]): int(row[2]) for row in clickhouse(
                f"SELECT attrs['log.file.path'], attrs['e2e.source'], count() "
                f"FROM file({test_e2e.sql_string(relative)}, 'Parquet') GROUP BY 1, 2")
        }
    return judge_read_back(
        rows, multiplicity={str(k): v for k, v in multiplicity.items()}, total=total,
        joined=joined, clickhouse_rows=clickhouse_rows, written=written,
        producer=test_e2e.alloy_producer_id(), problems=problems,
    )


def judge_read_back(rows, *, multiplicity, total, joined, clickhouse_rows, written,
                    producer, problems=()) -> dict:
    """The read-back's verdict from what the two readers returned.

    `rows` are DuckDB's per source file and `e2e.source` groups: rows,
    distinct well-formed sequences, first and last sequence, malformed rows
    and producer ids. Every written line must be stored exactly once: a
    missing line and a duplicated one are both problems, and so is any row
    beyond the lines written.
    """
    problems = list(problems)
    files = {}
    for source_file, source, count, distinct, low, high, malformed, producers in rows:
        files[source_file] = {
            "e2e_source": source, "rows": count, "distinct_lines": distinct,
            "duplicate_rows": count - distinct - malformed, "first_seq": low, "last_seq": high,
            "malformed_rows": malformed, "producer_ids": sorted(producers),
            "clickhouse_rows": clickhouse_rows.get((source_file, source)),
        }
    expected_file = "/input/events.log"
    entry = files.get(expected_file)
    if set(files) != {expected_file}:
        problems.append(f"rows from source files {sorted(files)}, expected {[expected_file]}")
    if entry is None:
        problems.append(f"no rows from {expected_file}")
    else:
        if entry["e2e_source"] != "alloy-file":
            problems.append(f"e2e.source {entry['e2e_source']!r}")
        if entry["distinct_lines"] != written:
            problems.append(f"{written - entry['distinct_lines']} of {written} lines missing")
        if entry["duplicate_rows"]:
            problems.append(f"{entry['duplicate_rows']} duplicate rows")
        if written and (entry["first_seq"] != 0 or entry["last_seq"] != written - 1):
            problems.append(f"sequence range {entry['first_seq']}..{entry['last_seq']}, "
                            f"expected 0..{written - 1}")
        if entry["malformed_rows"]:
            problems.append(f"{entry['malformed_rows']} malformed bodies")
        if entry["producer_ids"] != [producer]:
            problems.append(f"producer ids {entry['producer_ids']}")
    repeated = {copies: lines for copies, lines in multiplicity.items() if str(copies) != "1"}
    if repeated:
        problems.append(f"lines stored more than once, by copies: {repeated}")
    if total != written:
        problems.append(f"{total} values rows for {written} lines written")
    for source_file, entry_ in files.items():
        key = (source_file, entry_["e2e_source"])
        if clickhouse_rows.get(key) != entry_["rows"]:
            problems.append(f"ClickHouse counts {clickhouse_rows.get(key)} rows of "
                            f"{source_file}, DuckDB {entry_['rows']}")
    if joined[0] != total:
        problems.append(f"latest-descriptor join kept {joined[0]} of {total} rows")
    if sorted(joined[1]) != [producer]:
        problems.append(f"descriptor host.id {sorted(joined[1])}")
    return {
        "passed": not problems, "problems": problems, "files": files,
        "expected_lines": written, "total_rows": total,
        "multiplicity_histogram": dict(multiplicity),
        "readers": ["duckdb", "clickhouse"],
    }


def rejudge_stored_read_back(oracle) -> dict:
    """`judge_read_back` over what a stored read-back recorded."""
    files = oracle.get("files") or {}
    rows = [(name, entry["e2e_source"], entry["rows"], entry["distinct_lines"],
             entry["first_seq"], entry["last_seq"], entry["malformed_rows"],
             entry["producer_ids"]) for name, entry in files.items()]
    producer = test_e2e.alloy_producer_id()
    return judge_read_back(
        rows, multiplicity=oracle.get("multiplicity_histogram") or {},
        total=oracle.get("total_rows", 0),
        joined=(oracle.get("total_rows", 0), [producer]),
        clickhouse_rows={(name, entry["e2e_source"]): entry["clickhouse_rows"]
                         for name, entry in files.items()},
        written=oracle.get("expected_lines", 0), producer=producer,
        problems=[problem for problem in oracle.get("problems", [])
                  if "latest-descriptor" in problem or "descriptor host.id" in problem],
    )


def rate_between(polled, key, start_ns, end_ns):
    """A polled counter's rate between two instants, by interpolation."""
    points = [(p["monotonic_ns"], p[key]) for p in polled if p.get(key) is not None]
    if len(points) < 2:
        return None

    def at(t):
        before = [p for p in points if p[0] <= t]
        after = [p for p in points if p[0] >= t]
        if not before:
            return points[0][1]
        if not after:
            return points[-1][1]
        (t0, v0), (t1, v1) = before[-1], after[0]
        return v0 if t1 == t0 else v0 + (v1 - v0) * (t - t0) / (t1 - t0)

    return (at(end_ns) - at(start_ns)) / ((end_ns - start_ns) / 1e9)


def settle_alloy(plan, trial, spec, result, run_dir, phase, feed, readings, window,
                 worker_tids, observed, engine):
    """Read-back, metrics, verdict and checks of one Alloy trial."""
    command = capacity._command()
    samples = list(phase.sampler.samples)
    summary = phase.summary()
    residuals, heap = capacity.trial_residuals(
        samples, getattr(phase, "capacity_idle", None), getattr(phase.sampler, "pairs", ()))
    measure_s = (window[1] - window[0]) / 1e9
    tail_start = window[1] - int(min(capacity.STABILITY_TAIL_S, measure_s) * 1e9)
    written = feed["written"]
    requests = feed["requests"]
    in_window = [r for r in requests if window[0] <= r["received_ns"] < window[1]]
    ok = [r for r in in_window if r["code"] == "OK"]
    codes = collections.Counter(r["code"] for r in requests)
    details = sorted({f"{r['code']}: {r['details']}" for r in requests if r["code"] != "OK"})
    records = [r["records"] for r in in_window if r["records"] is not None]
    sizes = [r["bytes"] for r in in_window]
    latency = [(r["answered_ns"] - r["received_ns"]) / 1e9 for r in ok]
    polled = feed["polled"]
    sent_rate = rate_between(polled, "sent_records", *window)
    sent_tail = rate_between(polled, "sent_records", tail_start, window[1])
    read_rate = rate_between(polled, "read_lines", *window)
    written_rate = None
    timeline = feed["writer_timeline"]
    inside = [(t, n) for t, n in timeline if window[0] <= t <= window[1]]
    if len(inside) >= 2:
        written_rate = (inside[-1][1] - inside[0][1]) / ((inside[-1][0] - inside[0][0]) / 1e9)
    a = capacity.value_at(samples, window[0], "rows.written", "values")
    b = capacity.value_at(samples, window[1], "rows.written", "values")
    stored_rate = (b - a) / measure_s if a is not None and b is not None else None
    queue = [p["queue_size_records"] for p in polled
             if window[0] <= p["monotonic_ns"] <= window[1] and p.get("queue_size_records")
             is not None]
    queue_slope = None
    if len(queue) >= 2:
        points = [(p["monotonic_ns"], p["queue_size_records"]) for p in polled
                  if window[0] <= p["monotonic_ns"] <= window[1]
                  and p.get("queue_size_records") is not None]
        queue_slope = capacity.backlog_slope(points)
    storage = (engine.config["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]
               .get("config", {}).get("storage") or {})
    data_dir = Path((storage.get("file") or {}).get("base_uri") or engine.data)
    store = plan["store"]
    objects = capacity.object_inventory(plan, store, data_dir)
    local = data_dir if store is None else Path(run_dir) / "store"
    oracle = None
    oracle_s = None
    try:
        if store is not None:
            store.download(local)
        started = time.monotonic_ns()
        oracle = measurement.run_pinned(plan["oracle_cores"], read_back, local, written)
        oracle_s = (time.monotonic_ns() - started) / 1e9
    finally:
        if store is not None:
            _ = performance.clear_store(store)
            shutil.rmtree(local, ignore_errors=True)
        shutil.rmtree(data_dir, ignore_errors=True)
    jemalloc = capacity.jemalloc_peak(observed["log_path"])
    memory = capacity.memory_at_fill(samples, jemalloc)
    window_ns = readings["end"]["monotonic_ns"] - readings["start"]["monotonic_ns"]
    engine_cpu_ns = readings["end"]["engine_cpu_ns"] - readings["start"]["engine_cpu_ns"]
    schedule = performance.thread_schedule(
        readings["start"]["threads"], readings["end"]["threads"], window_ns, worker_tids)
    workers = len(spec.cores)
    distribution_ = capacity.worker_distribution(samples, window)
    active_workers = sum(
        1 for entry in (distribution_.get("workers") or {}).values()
        if entry["requests_accepted_in_window_count"] > 0)
    # The admission ceiling the slot formula predicts for these requests:
    # slots x records per request / (window + flush), per worker that has a
    # connection; and Alloy's own: consumers x records per request / the
    # engine's response time.
    flush = [
        (entry.get("flush.duration") or {}) for entry in
        (samples[-1].get("extras") or {}).values()] if samples else []
    flush_count = sum(f.get("count", 0) for f in flush)
    flush_mean_s = sum(f.get("sum", 0.0) for f in flush) / flush_count if flush_count else None
    rpr = quantile(records, 0.5) if records else None
    hold_s = trial["interval_s"] + (flush_mean_s or 0.0)
    shipped = shipped_alloy_settings()
    per_worker_ceiling = (capacity.admission_ceiling(trial["receiver_capacity"], rpr, hold_s)
                          if rpr else None)
    latency_p50 = quantile(latency, 0.5)
    alloy_ceiling = (shipped["num_consumers"] * rpr / latency_p50
                     if rpr and latency_p50 else None)
    alloy_final = observed.get("alloy_final") or {}
    offered = float(trial["rate"])
    reasons = []
    if codes.get("OK", 0) != len(requests):
        reasons.append(f"{len(requests) - codes.get('OK', 0)} of {len(requests)} requests "
                       f"refused: {dict(codes)}")
    if sent_tail is not None and read_rate and sent_tail < capacity.DURABLE_RATIO_FLOOR * read_rate:
        reasons.append(f"Alloy sent {sent_tail:.0f}/s over the last 30s < 98% of the "
                       f"{read_rate:.0f} lines/s it read")
    if not observed.get("alloy_drained"):
        reasons.append("Alloy did not send every line it was given before the stall deadline")
    verdict = "unsustainable" if reasons else "sustainable"
    # The writer is held back only by Alloy's reads, so a written rate
    # below the offered one is Alloy's own maximum.
    alloy_limited = written_rate is not None and written_rate < 0.98 * offered
    metrics = {
        "offered_records_per_s": offered,
        "durable_records_per_s": stored_rate,
        "alloy_sent_records_per_s": sent_rate,
        "alloy_sent_tail_records_per_s": sent_tail,
        "alloy_read_records_per_s": read_rate,
        "written_records_per_s": written_rate,
        "request_p50_records": quantile(records, 0.5),
        "request_p95_records": quantile(records, 0.95),
        "request_max_records": max(records) if records else None,
        "request_p50_bytes": quantile(sizes, 0.5),
        "request_p95_bytes": quantile(sizes, 0.95),
        "request_max_bytes": max(sizes) if sizes else None,
        "ack_latency_p50_s": latency_p50,
        "ack_latency_p99_s": quantile(latency, 0.99),
        "engine_cpu_s": engine_cpu_ns / 1e9,
        "engine_cpu_ns_per_record": (
            engine_cpu_ns / (stored_rate * measure_s) if stored_rate else None),
        "engine_core_occupancy_ratio": engine_cpu_ns / (window_ns * workers) if window_ns else None,
        "alloy_cpu_s": (readings["end"]["alloy_cpu_ns"] - readings["start"]["alloy_cpu_ns"]) / 1e9,
        "feeder_cpu_s": (readings["end"]["feeder_cpu_ns"]
                         - readings["start"]["feeder_cpu_ns"]) / 1e9,
        "peak_rss_bytes": float(observed["peak_rss_bytes"]),
        "slot_formula_ceiling_per_worker_records_per_s": per_worker_ceiling,
        "slot_formula_ceiling_records_per_s": (
            per_worker_ceiling * max(1, active_workers) if per_worker_ceiling else None),
        "alloy_consumer_ceiling_records_per_s": alloy_ceiling,
        "accounted_peak_bytes": memory.get("accounted_peak_bytes"),
    }
    result["metrics"] = metrics
    result["metrics_unavailable"] = {
        name: "not measured in this trial" for name, value in metrics.items() if value is None}
    result["metric_directions"] = {
        name: (measurement.HIGHER_IS_BETTER if name.endswith("_per_s")
               else measurement.LOWER_IS_BETTER) for name in metrics}
    result["mandatory_metrics"] = sorted(name for name in metrics if metrics[name] is not None)
    call_buckets = alloy_final.get("call_buckets", {}).get("OK", [])
    result["capacity"] = {
        "verdict": verdict,
        "reasons": reasons,
        "purpose": trial["purpose"],
        "offered_records_per_s": trial["rate"],
        "window_monotonic_ns": list(window),
        "warmup_s": trial["warmup_s"],
        "measure_s": trial["measure_s"],
        "interval_s": trial["interval_s"],
        "receiver_capacity_per_worker": trial["receiver_capacity"],
        "max_decoding_message_size": trial.get("max_decoding_message_size") or "4MiB (default)",
        "upload_concurrency": trial["upload_concurrency"],
        "worker_distribution": distribution_,
        "active_workers_count": active_workers,
        "memory": memory,
        "degradation": capacity.degradation(samples, window, trial["interval_s"], schedule),
        "schedule": {"workers": schedule["workers"],
                     "other_threads_on_cpu_s": schedule["other_threads_on_cpu_s"]},
        "objects_count": len(objects),
        "object_bytes": sum(size for _key, size, _t in objects),
        "oracle": oracle,
        "oracle_s": oracle_s,
        "flush_mean_s": flush_mean_s,
        "hold_s": hold_s,
        "rss_residual_count": len(residuals),
        "rss_heap_term": heap,
        "disk": capacity.disk_view(readings["start"].get("diskstats"),
                                   readings["end"].get("diskstats"), window_ns),
    }
    result["alloy"] = {
        "shipped_settings": shipped,
        "attempt_timeout": ATTEMPT_TIMEOUT,
        "limited_by_alloy": alloy_limited,
        "writer_throttled_s": feed["throttled_s"],
        "backlog_lines_bound": BACKLOG_LINES,
        "body_bytes": BODY_BYTES,
        "written_lines": written,
        "drained": observed.get("alloy_drained"),
        "alloy_rss_bytes": {label: readings[label].get("alloy_rss_bytes")
                            for label in ("start", "middle", "end")},
        "drain_s": (observed["alloy_drained_ns"] - window[1]) / 1e9,
        "final": {k: v for k, v in alloy_final.items() if k != "call_buckets"},
        "call_duration_buckets_ok": [[json_bound(le), count] for le, count in call_buckets],
        "call_duration_p50_le_s": json_bound(bucket_quantile(call_buckets, 0.5)),
        "call_duration_p99_le_s": json_bound(bucket_quantile(call_buckets, 0.99)),
        "requests_total_count": len(requests),
        "requests_in_window_count": len(in_window),
        "response_codes": dict(codes),
        "refusal_details": details[:10],
        "export_failure_log_lines_count": len(observed.get("alloy_failures") or []),
        "export_failure_log_sample": (observed.get("alloy_failures") or [])[:3],
        "request_records": distribution(records),
        "request_bytes": distribution(sizes),
        "ack_latency_s": distribution(latency),
        "queue_size_records_max": max(queue) if queue else None,
        "queue_slope_records_per_s": queue_slope,
        "connections": {
            label: {"alloy_to_tap": readings[label]["alloy_to_tap"]["count"],
                    "tap_to_engine": readings[label]["tap_to_engine"]["count"]}
            for label in ("start", "middle", "end")},
        "alloy_peers_count": len({r["peer"] for r in requests}),
        "upstream_connections_count": feed["upstream_connections"],
        "feeder_cpu_s": feed["cpu_s"],
        "writer_cpu_s": feed["writer_cpu_s"],
        "alloy_pinned": observed.get("alloy_pinned"),
        "polled": polled[::4],
    }
    result["samples"] = [
        dict(measurement.compact_sample(sample), extras=sample.get("extras"))
        for sample in samples[::capacity.PUBLISHED_SAMPLE_STRIDE]
    ]
    result["samples_published_stride"] = capacity.PUBLISHED_SAMPLE_STRIDE
    result["observations"] = {"residual_excursions": measurement.residual_excursions(
        residuals, list(getattr(phase.sampler, "pairs", ())),
        max((s["process_rss_bytes"] for s in samples), default=0))}
    checks = result["checks"]
    checks.append(measurement.check(
        "delivery", measurement.CHECK_HARD,
        measurement.STATUS_PASSED if oracle and oracle["passed"] else measurement.STATUS_FAILED,
        f"read-back problems {oracle and oracle.get('problems')}; responses {dict(codes)}",
    ))
    problems = command.sample_problems([summary])
    checks.append(measurement.check(
        "minimum_samples", measurement.CHECK_HARD,
        measurement.STATUS_FAILED if problems else measurement.STATUS_PASSED,
        "; ".join(problems) or f"{summary['sample_count']} samples",
    ))
    peak = max((s["process_rss_bytes"] for s in samples), default=0)
    checks.append(measurement.residual_check(residuals, peak))
    checks.append(measurement.check(
        "producer_complete", measurement.CHECK_HARD,
        measurement.STATUS_PASSED if feed.get("complete") and observed.get("alloy_pinned")
        else measurement.STATUS_FAILED,
        f"feeder complete {feed.get('complete')}; Alloy pinned {observed.get('alloy_pinned')}",
    ))
    enqueue_failed = alloy_final.get("enqueue_failed_records")
    checks.append(measurement.check(
        "no_enqueue_loss", measurement.CHECK_HARD,
        measurement.STATUS_PASSED if enqueue_failed == 0 else measurement.STATUS_FAILED,
        f"otelcol_exporter_enqueue_failed_log_records_total {enqueue_failed}",
    ))
    result["artifacts"] = [
        dict(measurement.file_entry(path), kind=kind, retention=str(run_dir))
        for path, kind in (
            (run_dir / "engine" / "engine.log", "engine_log"),
            (run_dir / "engine" / "pipeline.yaml", "engine_config"),
            (run_dir / "alloy" / "alloy.log", "alloy_log"),
        ) if path.is_file()
    ]
    result["trial"] = dict(trial)
    result["status"] = measurement.STATUS_PASSED


def step_alloy(plan, state, output_dir, report_dir, cell, options):
    """The Alloy trials: 80 percent of the cell's strict shipped ceiling,
    once with the shipped receiver and once per named variant."""
    base = capacity.winning(state, cell)["sustainable_records_per_s"]
    rate = int(options.get("alloy_rate") or (base or 0) * capacity.CONFIRMATION_FRACTION)
    if not rate:
        raise AssertionError(f"{cell} has no strict shipped ceiling to take 80 percent of")
    for variant in options.get("alloy_variants", tuple(VARIANTS)):
        trial = capacity.make_trial(
            plan, workload_id=WORKLOAD_ID, rate=rate, purpose=f"alloy_{variant}",
            connections=1, receiver_capacity=capacity.SHIPPED_RECEIVER_CAPACITY,
        )
        trial["max_decoding_message_size"] = VARIANTS[variant]
        trial["alloy_variant"] = variant
        _ = capacity.execute(plan, state, trial, output_dir, report_dir, cell,
                             experiment=alloy_experiment)
