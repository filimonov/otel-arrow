# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""The Alloy + durable_buffer + series_parquet reference deployment, validated.

A case runs `configs/series-parquet-buffered.yaml` and
`configs/series-parquet.alloy` as shipped, with only the site values
substituted, behind several Alloy containers that tail files a feeder appends
self-verifying lines to, applies one fault, and reads every stored line back
with DuckDB and clickhouse-local.
"""

import argparse
import bisect
import collections
import contextlib
import json
import multiprocessing
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

import yaml

try:
    from . import alloy_capacity
    from . import faults
    from . import measurement
    from . import performance
    from . import test_e2e
except ImportError:
    import alloy_capacity
    import faults
    import measurement
    import performance
    import test_e2e

ENGINE_CONFIG = "configs/series-parquet-buffered.yaml"
ALLOY_CONFIG = test_e2e.ALLOY_REFERENCE_CONFIG
CASES = ("healthy", "s3_outage", "wal_full", "engine_restart", "engine_kill",
         "alloy_restart", "alloy_kill", "soak")
# The engine process on the campaign's engine cores, its one worker on core 2,
# the store beside it, and Alloy with the feeder on the producer cores.
ENGINE_CPUS = "0-3,16-19"
WORKER_CORE = 2
STORE_CPUS = [4, 5, 6, 7, 20, 21, 22, 23]
PRODUCER_CPUS = "8-15,24-31"
PRODUCER_CPU_LIST = list(range(8, 16)) + list(range(24, 32))
PRODUCERS = 8
RATE_PER_PRODUCER = 5000
# Every line is BODY_BYTES long unless a case sets `body_bytes`:
# `p<NN> <12-digit seq> x...`.
BODY_BYTES = 100
SEQ_DIGITS = 12
TIMELINE_PERIOD_NS = 100_000_000
FRESHNESS_GROUP_LINES = 500
# The size cap the wal_full case sets instead of the shipped one: about three
# times the steady WAL of 40k lines/s (ingest x (window + flush), ~300 MB).
WAL_FULL_CAP = "1GiB"
SIGTERM_GRACE_S = 60
# The shipped window, and where in it the SIGKILLs land (seconds after its start).
WINDOW_S = 15
KILL_PHASES_S = (7.0, 1.0, 0.4)
REPORT_PREFIX = "reference-alloy"
ARCHIVE_DIR = faults.FAULT_ARCHIVE_ROOT / "reference-alloy"
METRIC_PREFIXES = ("exporter.series_parquet", "processor.durable_buffer", "receiver")


def line(producer, seq, body_bytes=BODY_BYTES) -> str:
    """Line `seq` of producer `producer`, padded to `body_bytes`."""
    head = f"p{producer:02d} {seq:0{SEQ_DIGITS}d} "
    return head + "x" * (body_bytes - len(head))


def parse_line(body):
    """(producer, seq) of a body this harness wrote, or None."""
    if len(body) != BODY_BYTES or body[0] != "p" or body[3] != " ":
        return None
    digits = body[4:4 + SEQ_DIGITS]
    if not (body[1:3].isdigit() and digits.isdigit()):
        return None
    return int(body[1:3]), int(digits)


def producer_id(index) -> str:
    """The `host.id` producer `index` stamps through SERIES_PRODUCER_ID."""
    return f"alloy-p{index:02d}"


def shipped_alloy_settings() -> dict:
    """Batch and queue settings of the reference River config."""
    text = (test_e2e.WORKSPACE / ALLOY_CONFIG).read_text()

    def number(name):
        found = re.search(rf"\b{name}\s*=\s*(\d+)", text)
        return int(found.group(1)) if found else None

    timeout = re.search(r'timeout\s*=\s*coalesce\([^,]+,\s*"([^"]+)"\)', text)
    return {
        "num_consumers": number("num_consumers"),
        "queue_size_records": number("queue_size"),
        "batch_max_records": number("send_batch_max_size"),
        "timeout": timeout.group(1) if timeout else None,
        "persistent_queue": "otelcol.storage.file" in text,
    }


def reference_config(*, grpc_port, store, wal_dir, overrides=None):
    """The shipped buffered config with only the site values replaced.

    Returns the config and the dotted paths it changed, so a result records
    exactly how the run differed from the file.
    """
    config = yaml.safe_load((test_e2e.WORKSPACE / ENGINE_CONFIG).read_text())
    nodes = config["groups"]["default"]["pipelines"]["main"]["nodes"]
    changed = {}

    def put(path, value):
        target = config
        keys = path.split(".")
        for key in keys[:-1]:
            target = target[key]
        if keys[-1] not in target and not path.startswith("groups.default.pipelines.main.nodes"):
            raise AssertionError(f"{path} is not in the shipped config")
        changed[path] = {"shipped": target.get(keys[-1]), "run": value}
        target[keys[-1]] = value

    base = "groups.default.pipelines.main.nodes"
    put("policies.resources.core_allocation",
        {"type": "core_set", "set": [{"start": WORKER_CORE, "end": WORKER_CORE}]})
    put(f"{base}.receiver.config.protocols.grpc.listening_addr", f"127.0.0.1:{grpc_port}")
    put(f"{base}.buffer.config.path", str(wal_dir))
    put(f"{base}.exporter.config.storage.s3.endpoint", store.endpoint)
    put(f"{base}.exporter.config.storage.s3.base_uri", f"s3://{store.bucket}/otel")
    for path, value in (overrides or {}).items():
        put(f"{base}.{path}", value)
    assert set(nodes) == {"receiver", "buffer", "exporter"}
    return config, changed


def flatten_metrics(document) -> dict:
    """`set.metric{labels}` -> value, summed over entities, for the sets watched."""
    flat = collections.defaultdict(float)
    for entry in document.get("metric_sets", []):
        name = entry.get("name", "")
        if not name.startswith(METRIC_PREFIXES):
            continue
        for metric in entry.get("metrics", []):
            value = metric.get("value")
            if not isinstance(value, (int, float)):
                continue
            labels = ",".join(
                f"{k}={measurement.attribute_value(v)}"
                for k, v in sorted((metric.get("attributes") or {}).items()))
            flat[f"{name}.{metric['name']}" + (f"{{{labels}}}" if labels else "")] += value
    return dict(flat)


class ReferenceEngine:
    """`df_engine` on the reference config; one instance per boot, same ports and WAL."""

    def __init__(self, root, *, store, wal_dir, grpc_port, admin_port, overrides=None, boot=0):
        self.root = Path(root)
        self.store = store
        self.grpc_port = grpc_port
        self.admin_port = admin_port
        self.boot = boot
        self.config, self.changed = reference_config(
            grpc_port=grpc_port, store=store, wal_dir=wal_dir, overrides=overrides)
        self.binary = Path(os.environ.get(
            "DF_ENGINE", test_e2e.WORKSPACE / "target/release/df_engine"))
        self.process = None
        self.log_path = self.root / f"engine-{boot}.log"
        self.exit = None

    def start(self):
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / "pipeline.yaml"
        path.write_text(yaml.safe_dump(self.config))
        env = dict(os.environ)
        env["SERIES_S3_ACCESS_KEY_ID"] = self.store.key
        env["SERIES_S3_SECRET_ACCESS_KEY"] = self.store.secret
        log = self.log_path.open("w")
        self.started_ns = time.monotonic_ns()
        self.process = subprocess.Popen(
            ["taskset", "-c", ENGINE_CPUS, str(self.binary), "--config", str(path),
             "--http-admin-bind", f"127.0.0.1:{self.admin_port}"],
            stdout=log, stderr=subprocess.STDOUT, env=env)
        log.close()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f"engine exited at start:\n{self.log_path.read_text()[-3000:]}")
            with contextlib.suppress(OSError):
                with urllib.request.urlopen(
                        f"http://127.0.0.1:{self.admin_port}/api/v1/readyz", timeout=2) as answer:
                    if answer.status == 200:
                        self.ready_ns = time.monotonic_ns()
                        return self
            time.sleep(0.1)
        raise AssertionError("engine never became ready")

    @property
    def pid(self):
        return self.process.pid

    def metrics(self) -> dict:
        url = (f"http://127.0.0.1:{self.admin_port}"
               "/api/v1/telemetry/metrics?format=json&keep_all_zeroes=true")
        with urllib.request.urlopen(url, timeout=5) as answer:
            return flatten_metrics(json.load(answer))

    def rss(self) -> dict:
        found = {}
        with contextlib.suppress(OSError):
            for row in Path(f"/proc/{self.pid}/status").read_text().splitlines():
                key, _, value = row.partition(":")
                if key in ("VmRSS", "VmHWM"):
                    found[key] = int(value.split()[0]) * 1024
        return found

    def terminate(self, deadline_s=SIGTERM_GRACE_S + 30) -> dict:
        """SIGTERM, the signal a supervisor sends, and the exit it leads to."""
        sent = time.monotonic_ns()
        self.process.send_signal(signal.SIGTERM)
        try:
            code = self.process.wait(timeout=deadline_s)
        except subprocess.TimeoutExpired:
            self.process.kill()
            code = self.process.wait(timeout=30)
            self.exit = {"signal": "SIGTERM", "code": code, "forced": True,
                         "exit_s": (time.monotonic_ns() - sent) / 1e9}
            return self.exit
        self.exit = {"signal": "SIGTERM", "code": code, "forced": False,
                     "exit_s": (time.monotonic_ns() - sent) / 1e9}
        return self.exit

    def kill(self) -> dict:
        sent = time.monotonic_ns()
        self.process.send_signal(signal.SIGKILL)
        code = self.process.wait(timeout=30)
        self.exit = {"signal": "SIGKILL", "code": code, "exit_s": (time.monotonic_ns() - sent) / 1e9}
        return self.exit

    def boot_id(self):
        text = self.log_path.read_text(errors="replace") if self.log_path.is_file() else ""
        found = faults.BOOT_EVENT.search(text)
        return found.group(1) if found else None


class AlloyInstance:
    """One Grafana Alloy container running the reference River config unchanged."""

    def __init__(self, root, index, grpc_port, config_text=None):
        self.index = index
        self.config_text = config_text
        self.dir = Path(root) / f"alloy-{index:02d}"
        self.input = self.dir / "input"
        self.state = self.dir / "state"
        self.lines = self.input / "events.log"
        self.grpc_port = grpc_port
        self.port = test_e2e.free_port()
        self.name = f"series-ref-alloy-{index:02d}-{os.getpid()}"
        self.container = None
        self.restarts = []

    def start(self):
        image = test_e2e.require_docker_image("alloy")
        self.input.mkdir(parents=True, exist_ok=True)
        self.state.mkdir(parents=True, exist_ok=True)
        (self.input / "config.alloy").write_text(
            self.config_text or (test_e2e.WORKSPACE / ALLOY_CONFIG).read_text())
        self.lines.touch()
        self.container = subprocess.check_output([
            "docker", "run", "--pull=never", "--detach", "--name", self.name,
            "--network", "host", "--user", f"{os.getuid()}:{os.getgid()}",
            "--cpuset-cpus", PRODUCER_CPUS,
            "--mount", f"type=bind,src={self.input.resolve()},dst=/input,readonly",
            "--mount", f"type=bind,src={self.state.resolve()},dst=/state",
            "-e", f"OTLP_ENDPOINT=127.0.0.1:{self.grpc_port}",
            "-e", f"SERIES_PRODUCER_ID={producer_id(self.index)}",
            "-e", f"SERIES_LOG_PATH={test_e2e.ALLOY_LOG_PATH}",
            image, "run", f"--stability.level={test_e2e.ALLOY_STABILITY_LEVEL}",
            "--storage.path=/state", f"--server.http.listen-addr=127.0.0.1:{self.port}",
            "/input/config.alloy",
        ], text=True, timeout=60).strip()
        self.wait_ready()

    def wait_ready(self, deadline_s=60):
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            with contextlib.suppress(OSError):
                with urllib.request.urlopen(
                        f"http://127.0.0.1:{self.port}/-/ready", timeout=1) as answer:
                    if answer.status == 200:
                        return
            time.sleep(0.1)
        raise AssertionError(f"Alloy {self.index} not ready:\n{self.logs()[-2000:]}")

    def restart(self, stop_timeout_s=10) -> dict:
        """`docker stop` then `docker start` of the same container and state."""
        began = time.monotonic_ns()
        subprocess.run(["docker", "stop", "--time", str(stop_timeout_s), self.container],
                       check=True, capture_output=True, timeout=stop_timeout_s + 30)
        stopped = time.monotonic_ns()
        subprocess.run(["docker", "start", self.container], check=True,
                       capture_output=True, timeout=60)
        self.wait_ready()
        entry = {"stop_s": (stopped - began) / 1e9,
                 "down_s": (time.monotonic_ns() - stopped) / 1e9}
        self.restarts.append(entry)
        return entry

    def kill_and_start(self) -> dict:
        """SIGKILL the container, then start it again on the same state."""
        began = time.monotonic_ns()
        subprocess.run(["docker", "kill", "--signal", "KILL", self.container], check=True,
                       capture_output=True, timeout=30)
        subprocess.run(["docker", "start", self.container], check=True,
                       capture_output=True, timeout=60)
        self.wait_ready()
        entry = {"killed": True, "down_s": (time.monotonic_ns() - began) / 1e9}
        self.restarts.append(entry)
        return entry

    def metrics(self) -> dict:
        url = f"http://127.0.0.1:{self.port}/metrics"
        with urllib.request.urlopen(url, timeout=5) as answer:
            body = answer.read().decode("ascii", "replace")
        found = alloy_capacity.parse_metrics(body)
        read_bytes = 0.0
        for raw in body.splitlines():
            if raw.startswith("loki_source_file_read_bytes_total"):
                read_bytes += float(raw.rsplit(" ", 1)[-1])
        found["read_bytes"] = read_bytes
        return found

    def logs(self) -> str:
        if not self.container:
            return ""
        done = subprocess.run(["docker", "logs", self.container], capture_output=True,
                              text=True, timeout=30)
        return done.stdout + done.stderr

    def remove(self, archive=None):
        if not self.container:
            return
        with contextlib.suppress(Exception):
            subprocess.run(["docker", "stop", "--time", "10", self.container],
                           capture_output=True, timeout=40)
        text = self.logs()
        if archive is not None:
            (Path(archive) / f"alloy-{self.index:02d}.log").write_text(text)
        subprocess.run(["docker", "rm", "--force", "--volumes", self.container],
                       capture_output=True, timeout=30)
        self.container = None
        return text


def _feeder(config, stop):
    """Append lines to every producer's file at the rate; record a timeline."""
    os.sched_setaffinity(0, config["cpus"])
    rate = config["rate"]
    body_bytes = config["body_bytes"]
    streams = [open(path, "a", encoding="ascii") for path in config["paths"]]
    written = [0] * len(streams)
    timeline = []
    start = time.monotonic_ns()
    next_mark = start
    while not stop.is_set():
        now = time.monotonic_ns()
        due = int((now - start) / 1e9 * rate)
        for index, stream in enumerate(streams):
            if due > written[index]:
                stream.write("".join(line(index, seq, body_bytes) + "\n"
                                     for seq in range(written[index], due)))
                stream.flush()
                written[index] = due
        if now >= next_mark:
            timeline.append([now, time.time_ns(), list(written)])
            next_mark += TIMELINE_PERIOD_NS
        stop.wait(0.01)
    for stream in streams:
        stream.flush()
        os.fsync(stream.fileno())
        stream.close()
    timeline.append([time.monotonic_ns(), time.time_ns(), list(written)])
    Path(config["result"]).write_text(json.dumps({"written": written, "timeline": timeline,
                                                  "start_ns": start}))


class Feeder:
    def __init__(self, paths, rate, result, body_bytes=BODY_BYTES):
        self.context = multiprocessing.get_context("spawn")
        self.stop_event = self.context.Event()
        self.config = {"paths": [str(p) for p in paths], "rate": rate,
                       "body_bytes": body_bytes,
                       "cpus": PRODUCER_CPU_LIST, "result": str(result)}
        self.process = None

    def start(self):
        self.process = self.context.Process(target=_feeder, args=(self.config, self.stop_event),
                                            daemon=True)
        self.process.start()

    def stop(self) -> dict:
        self.stop_event.set()
        self.process.join(timeout=60)
        return json.loads(Path(self.config["result"]).read_text())


def dir_bytes(path) -> int:
    """Allocated bytes under `path`, as `du` counts them."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            with contextlib.suppress(OSError):
                total += os.lstat(os.path.join(root, name)).st_blocks * 512
    return total


class Sampler:
    """Engine metrics, RSS and WAL disk each second; Alloy and the store every two."""

    def __init__(self, case):
        self.case = case
        self.engine_samples = []
        self.alloy_samples = []
        self.objects = {}
        self.stop_event = threading.Event()
        self.threads = []

    def start(self):
        for target in (self._engine, self._producers):
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=30)

    def _engine(self):
        while not self.stop_event.is_set():
            engine = self.case.engine
            sample = {"t": time.monotonic_ns(), "wall": time.time_ns(),
                      "boot": engine.boot, "wal_disk_bytes": dir_bytes(self.case.wal_dir)}
            if engine.process is not None and engine.process.poll() is None:
                sample.update(engine.rss())
                with contextlib.suppress(Exception):
                    sample["metrics"] = engine.metrics()
            self.engine_samples.append(sample)
            self.stop_event.wait(1.0)

    def _producers(self):
        while not self.stop_event.is_set():
            now = time.monotonic_ns()
            per = {}
            for alloy in self.case.alloys:
                with contextlib.suppress(Exception):
                    found = alloy.metrics()
                    found.pop("call_buckets", None)
                    per[alloy.index] = found
            self.alloy_samples.append({"t": now, "alloys": per})
            with contextlib.suppress(Exception):
                self._list_objects()
            self.stop_event.wait(2.0)

    def _list_objects(self):
        client = self.case.store.client
        seen = time.time_ns()
        for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=self.case.store.bucket, Prefix="otel/"):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith(".parquet") and key not in self.objects:
                    self.objects[key] = {
                        "first_seen_ns": seen,
                        "last_modified_ns": int(item["LastModified"].timestamp() * 1e9),
                        "bytes": item["Size"],
                    }


def metric(sample, key, default=0.0):
    return (sample.get("metrics") or {}).get(key, default)


def metric_sum(sample, prefix):
    return sum(v for k, v in (sample.get("metrics") or {}).items() if k.startswith(prefix))


class Case:
    """One validation cell: store, engine, Alloy fleet, feeder, fault, read-back."""

    def __init__(self, name, store_kind, work_dir, *, producers=PRODUCERS,
                 rate=RATE_PER_PRODUCER, options=None):
        self.name = name
        self.store_kind = store_kind
        self.options = options or {}
        self.work = Path(work_dir) / f"{name}-{store_kind}-{int(time.time())}"
        self.wal_dir = self.work / "wal"
        self.producers = producers
        self.rate = rate
        self.events = []
        self.engines = []
        self.alloys = []
        self.store = None
        self.engine = None
        self.overrides = {"buffer.config.retention_size_cap": WAL_FULL_CAP} \
            if name == "wal_full" else {}

    @property
    def alloy_text(self):
        """The River file the fleet runs: the shipped one, or, for the control
        run `alloy_queue_storage=false`, the same with its queue in memory."""
        text = (test_e2e.WORKSPACE / ALLOY_CONFIG).read_text()
        if self.options.get("alloy_queue_storage", True):
            return text
        return memory_queue_variant(text)

    def event(self, kind, **fields):
        entry = {"kind": kind, "t": time.monotonic_ns(), "wall": time.time_ns(), **fields}
        self.events.append(entry)
        print(f"[{time.strftime('%H:%M:%S')}] {self.name}: {kind} {fields}", flush=True)
        return entry

    def new_engine(self):
        engine = ReferenceEngine(self.work / "engine", store=self.store, wal_dir=self.wal_dir,
                                 grpc_port=self.grpc_port, admin_port=self.admin_port,
                                 overrides=self.overrides, boot=len(self.engines))
        engine.start()
        self.engines.append(engine)
        self.engine = engine
        self.event("engine_ready", boot=engine.boot, pid=engine.pid,
                   start_s=(engine.ready_ns - engine.started_ns) / 1e9)
        return engine

    def latest(self):
        return self.sampler.engine_samples[-1] if self.sampler.engine_samples else {}

    def wait_for(self, predicate, deadline_s, what):
        deadline = time.monotonic() + deadline_s
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(1.0)
        self.event("wait_expired", what=what)
        return False

    def hold(self, seconds):
        time.sleep(seconds)

    # The faults. Each runs after the baseline with input flowing.
    def fault_healthy(self):
        self.hold(self.options.get("measure_s", 180))

    def fault_soak(self):
        self.hold(self.options.get("measure_s", 1800))

    def fault_s3_outage(self):
        self.event("store_stop")
        self.store.stop()
        self.hold(self.options.get("outage_s", 150))
        self.store.recover()
        self.event("store_recovered")
        self.hold(self.options.get("after_s", 120))

    def fault_wal_full(self):
        self.event("store_stop")
        self.store.stop()
        refused = self.wait_for(
            lambda: metric(self.latest(), "processor.durable_buffer.ingest.failures"
                           "{failure=backpressure}") > 0, 300, "WAL backpressure")
        self.event("wal_full_observed", observed=refused)
        self.hold(self.options.get("full_s", 90))
        self.store.recover()
        self.event("store_recovered")
        self.hold(self.options.get("after_s", 240))

    def fault_engine_restart(self):
        self.event("engine_sigterm", boot=self.engine.boot)
        exit_ = self.engine.terminate()
        self.event("engine_exited", **exit_)
        self.new_engine()
        self.hold(self.options.get("after_s", 90))

    def fault_engine_kill(self):
        # Each kill lands at a phase of the 15s window: mid-window with nothing
        # flushing, about when the previous block's acknowledgements reach the
        # buffer, and while its upload is on the wire.
        for phase in self.options.get("kill_phases", KILL_PHASES_S):
            now = time.time()
            wait = (phase - now % WINDOW_S) % WINDOW_S
            time.sleep(wait if wait > 1 else wait + WINDOW_S)
            self.event("engine_sigkill", boot=self.engine.boot,
                       window_phase_s=round(time.time() % WINDOW_S, 3))
            exit_ = self.engine.kill()
            self.event("engine_exited", **exit_)
            self.new_engine()
            self.hold(self.options.get("after_s", 45))

    def fault_alloy_restart(self):
        self.event("alloy_stop")
        threads = [threading.Thread(target=alloy.restart) for alloy in self.alloys]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.event("alloy_restarted", restarts=[a.restarts[-1] for a in self.alloys])
        self.hold(self.options.get("after_s", 90))

    def fault_alloy_kill(self):
        # The engine is down long enough for every Alloy queue to fill and its
        # tailer to park; then every Alloy is SIGKILLed and started again.
        self.event("engine_sigterm", boot=self.engine.boot)
        self.event("engine_exited", **self.engine.terminate())
        full = self.wait_for(lambda: self.queues_full(), 120, "Alloy queues full")
        self.hold(self.options.get("blocked_s", 20))
        self.event("alloy_sigkill", queues_full=full)
        threads = [threading.Thread(target=alloy.kill_and_start) for alloy in self.alloys]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.event("alloy_restarted", restarts=[a.restarts[-1] for a in self.alloys])
        self.hold(10)
        self.new_engine()
        self.hold(self.options.get("after_s", 90))

    def queues_full(self):
        sample = self.sampler.alloy_samples[-1] if self.sampler.alloy_samples else {}
        per = sample.get("alloys") or {}
        capacity = shipped_alloy_settings()["queue_size_records"]
        return len(per) == len(self.alloys) and all(
            (found.get("queue_size_records") or 0) >= capacity * 0.75 for found in per.values())

    def producers_drained(self, stable_polls=6):
        """Every Alloy's queue empty and its read and sent counts unchanged for
        `stable_polls` polls, past its batch timeout; one never restarted has
        also sent every line of its file.

        `loki_source_file_read_bytes_total` is not used: it reports the last
        saved position, which can stay behind the lines already read.
        """
        recent = self.sampler.alloy_samples[-stable_polls:]
        if len(recent) < stable_polls:
            return False
        for alloy in self.alloys:
            seen = [(s.get("alloys") or {}).get(alloy.index) for s in recent]
            if any(found is None for found in seen):
                return False
            if any(found.get("queue_size_records") not in (0, 0.0) for found in seen):
                return False
            if len({(f.get("read_lines"), f.get("sent_records")) for f in seen}) != 1:
                return False
            if not alloy.restarts and seen[-1].get("sent_records", 0) < \
                    self.feed["written"][alloy.index]:
                return False
        return True

    def engine_drained(self):
        sample = self.latest()
        if "metrics" not in sample:
            return False
        return (metric_sum(sample, "processor.durable_buffer.items.queued") == 0
                and metric(sample, "processor.durable_buffer.in.flight") == 0
                and metric(sample, "exporter.series_parquet.block.requests_pending") == 0)

    def run(self):
        self.work.mkdir(parents=True, exist_ok=True)
        self.wal_dir.mkdir()
        self.archive = Path(self.options.get("archive_dir", ARCHIVE_DIR)) / self.work.name
        self.archive.mkdir(parents=True, exist_ok=True)
        lease = measurement.HostLease(run_id=f"{REPORT_PREFIX}-{self.name}-{self.store_kind}")
        lease.acquire(deadline_ns=time.monotonic_ns()
                      + int(self.options.get("lease_wait_s", 4 * 3600) * 1e9))
        self.result = {"case": self.name, "store": self.store_kind,
                       "started_utc": measurement.utc_now(), "lease": lease.as_json(),
                       "options": {k: v for k, v in self.options.items()
                                   if k not in ("archive_dir", "report_dir")}}
        try:
            with test_e2e.DockerStore(self.store_kind) as store:
                self.store = store
                performance.pin_container(store.container, STORE_CPUS)
                self.grpc_port = test_e2e.free_port()
                self.admin_port = test_e2e.free_port()
                self.new_engine()
                self.sampler = Sampler(self)
                self.alloys = [AlloyInstance(self.work, i, self.grpc_port, self.alloy_text)
                               for i in range(self.producers)]
                try:
                    for alloy in self.alloys:
                        alloy.start()
                    self.sampler.start()
                    self.feeder = Feeder([a.lines for a in self.alloys], self.rate,
                                         self.work / "feeder.json",
                                         self.options.get("body_bytes", BODY_BYTES))
                    self.feeder.start()
                    self.event("input_started", producers=self.producers, rate=self.rate)
                    self.hold(self.options.get("baseline_s", 60))
                    self.event("fault_start")
                    getattr(self, f"fault_{self.name}")()
                    self.event("fault_end")
                    self.feed = self.feeder.stop()
                    self.event("input_stopped", written=sum(self.feed["written"]))
                    alloy_done = self.wait_for(self.producers_drained, 900, "Alloy drained")
                    self.event("alloy_drained", ok=alloy_done)
                    time.sleep(20)
                    engine_done = self.wait_for(
                        lambda: self.engine_drained() and self.producers_drained(),
                        600, "engine drained")
                    time.sleep(20)
                    engine_done = engine_done and self.engine_drained()
                    self.event("engine_drained", ok=engine_done)
                    self.drained = alloy_done and engine_done
                    self.final_alloy = {a.index: a.metrics() for a in self.alloys}
                    self.final_buckets = {a.index: self.final_alloy[a.index].pop("call_buckets")
                                          for a in self.alloys}
                    exit_ = self.engine.terminate()
                    self.event("engine_final_exit", **exit_)
                    self.sampler.stop()
                finally:
                    self.sampler.stop_event.set()
                    self.alloy_logs = {a.index: a.remove(self.archive) or "" for a in self.alloys}
                    for engine in self.engines:
                        if engine.process and engine.process.poll() is None:
                            engine.process.kill()
                self.uploads = self.incomplete_uploads()
                local = self.work / "store"
                store.download(local)
                self.result["object_inventory_count"] = len(self.sampler.objects)
                self.oracle = measurement.run_pinned(
                    STORE_CPUS, read_back, local, self.feed["written"],
                    self.options.get("body_bytes", BODY_BYTES))
                self.settle()
        finally:
            lease.release()
            self.archive_raw()
            shutil.rmtree(self.work, ignore_errors=True)
        return self.result

    def incomplete_uploads(self):
        found = []
        pages = self.store.client.get_paginator("list_multipart_uploads").paginate(
            Bucket=self.store.bucket)
        for page in pages:
            for upload in page.get("Uploads", []) or []:
                found.append({"key": upload["Key"], "initiated": str(upload.get("Initiated"))})
        return found

    def archive_raw(self):
        with contextlib.suppress(Exception):
            for engine in self.engines:
                if engine.log_path.is_file():
                    shutil.copy(engine.log_path, self.archive / engine.log_path.name)
            if (self.work / "engine" / "pipeline.yaml").is_file():
                shutil.copy(self.work / "engine" / "pipeline.yaml", self.archive / "pipeline.yaml")
            (self.archive / "samples.json").write_text(json.dumps({
                "engine": self.sampler.engine_samples, "alloy": self.sampler.alloy_samples,
                "objects": self.sampler.objects, "events": self.events}))
            if (self.work / "feeder.json").is_file():
                shutil.copy(self.work / "feeder.json", self.archive / "feeder.json")

    # --------------------------------------------------------------- verdict
    def settle(self):
        feed = self.feed
        samples = [s for s in self.sampler.engine_samples if "metrics" in s]
        events = {e["kind"]: e for e in self.events}
        first = events["input_started"]["t"]
        stopped = events["input_stopped"]["t"]
        input_s = (stopped - first) / 1e9
        written_total = sum(feed["written"])
        boots = [e.boot_id() for e in self.engines]
        oracle = self.oracle
        freshness = freshness_view(oracle.get("groups", []), feed, self.sampler.objects,
                                   first_wall=events["input_started"]["wall"])
        dup = duplicate_view(oracle.get("duplicate_runs", []), boots)
        ack = ack_latency(self.final_buckets)
        alloy_final = self.final_alloy
        statuses = collections.Counter()
        for found in alloy_final.values():
            for status, count in (found.get("calls") or {}).items():
                statuses[status or "OK"] += count
        drop_lines = sum(text.count("Dropping data") for text in self.alloy_logs.values())
        failed_lines = sum(text.count("Exporting failed") for text in self.alloy_logs.values())
        rss_peak = max((s.get("VmHWM", 0) for s in samples), default=0)
        wal_disk_peak = max((s["wal_disk_bytes"] for s in self.sampler.engine_samples), default=0)
        wal_used_peak = max((metric(s, "processor.durable_buffer.storage.bytes.used")
                             for s in samples), default=0)
        budget = max((metric(s, "exporter.series_parquet.memory.budget") for s in samples),
                     default=0)
        accounted_peak = max((metric(s, "exporter.series_parquet.memory.accounted")
                              for s in samples), default=0)
        loss, permanent = buffer_losses(samples)
        abort_failures = sum(metric(s, "exporter.series_parquet.flush.abort_failures")
                             for s in last_per_boot(samples))
        late_commits = sum(metric(s, "exporter.series_parquet.flush.late_commits"
                                  f"{{outcome={outcome}}}")
                           for s in last_per_boot(samples)
                           for outcome in ("stored", "acknowledged"))
        backpressure = sum(metric(s, "processor.durable_buffer.ingest.failures"
                                     "{failure=backpressure}") for s in last_per_boot(samples))
        flush_failures = sum(metric_sum(s, "exporter.series_parquet.flush.failures")
                             for s in last_per_boot(samples))
        retries = sum(metric(s, "processor.durable_buffer.retries.scheduled")
                      for s in last_per_boot(samples))
        settings = shipped_alloy_settings()
        rate = written_total / input_s if input_s else None
        metrics = {
            "offered_lines_per_s": self.producers * self.rate,
            "written_lines_per_s": rate,
            "written_lines": written_total,
            "input_s": input_s,
            "ack_latency_p50_le_s": ack["p50_le_s"],
            "ack_latency_p99_le_s": ack["p99_le_s"],
            "freshness_p50_s": freshness["overall"]["p50_s"],
            "freshness_p99_s": freshness["overall"]["p99_s"],
            "freshness_max_s": freshness["overall"]["max_s"],
            "rss_peak_bytes": rss_peak,
            "exporter_budget_bytes": budget,
            "exporter_accounted_peak_bytes": accounted_peak,
            "wal_disk_peak_bytes": wal_disk_peak,
            "wal_used_peak_bytes": wal_used_peak,
            "duplicate_lines": oracle.get("duplicate_rows", 0),
            "incomplete_uploads": len(self.uploads),
            "flush_abort_failures": abort_failures,
            "flush_late_commits": late_commits,
            "flush_failures": flush_failures,
            "buffer_retries_scheduled": retries,
            "buffer_backpressure_refusals": backpressure,
            "alloy_calls_by_status": dict(statuses),
            "alloy_export_failed_log_lines": failed_lines,
            "alloy_dropping_data_log_lines": drop_lines,
        }
        checks = []

        def check(name, passed, detail):
            checks.append({"name": name, "passed": bool(passed), "detail": detail})

        check("delivery", oracle["passed"], oracle["problems"][:10])
        check("drained", self.drained, "Alloy positions at EOF and queues empty; buffer empty")
        enqueue_failed = sum(f.get("enqueue_failed_records", 0) for f in alloy_final.values())
        send_failed = sum(f.get("send_failed_records", 0) for f in alloy_final.values())
        check("no_producer_loss", enqueue_failed == 0 and send_failed == 0 and drop_lines == 0,
              f"enqueue_failed {enqueue_failed}, send_failed {send_failed}, "
              f"'Dropping data' lines {drop_lines}")
        check("no_buffer_loss", not loss and permanent == 0,
              f"loss {loss}, permanently_rejected {permanent}")
        check("orphans_expected", len(self.uploads) <= abort_failures,
              f"{len(self.uploads)} incomplete uploads, abort_failures {abort_failures}")
        allowed = duplicate_allowance(self.name, self.producers, self.rate, settings)
        passed, detail = duplicate_check(self.name, dup["runs"], dup["runs_count"],
                                         self.events, allowed)
        check("duplicates_within_bound", passed, detail)
        self.fault_checks(check, events, statuses, failed_lines, backpressure, flush_failures,
                          retries, samples)
        engine_exits = [e for e in self.events if e["kind"] in ("engine_exited",
                                                                 "engine_final_exit")]
        self.result.update({
            "finished_utc": measurement.utc_now(),
            "status": "passed" if all(c["passed"] for c in checks) else "failed",
            "checks": checks,
            "metrics": metrics,
            "freshness": freshness,
            "duplicates": dup,
            "duplicate_allowance": allowed,
            "ack_latency": ack,
            "alloy_settings": settings,
            "alloy_final": {str(k): {kk: vv for kk, vv in v.items() if kk != "call_buckets"}
                            for k, v in alloy_final.items()},
            "alloy_restarts": {str(a.index): a.restarts for a in self.alloys},
            "engine_exits": engine_exits,
            "events": [{k: v for k, v in e.items() if k != "t"}
                       | {"at_s": (e["t"] - first) / 1e9} for e in self.events],
            "boots": len(boots),
            "incomplete_uploads": self.uploads,
            "oracle": {k: v for k, v in oracle.items() if k not in ("groups", "duplicate_runs")},
            "config": {
                "engine_file": ENGINE_CONFIG, "alloy_file": ALLOY_CONFIG,
                "site_substitutions": scrub(self.engines[0].changed, self),
                "engine_sha256": sha256(test_e2e.WORKSPACE / ENGINE_CONFIG),
                "alloy_sha256": sha256(test_e2e.WORKSPACE / ALLOY_CONFIG),
            },
            "environment": {
                "git": measurement.git_provenance(),
                "engine_binary": str(self.engines[0].binary),
                "engine_cpus": ENGINE_CPUS, "worker_core": WORKER_CORE,
                "store_cpus": STORE_CPUS, "producer_cpus": PRODUCER_CPUS,
                "producers": self.producers, "rate_per_producer": self.rate,
                "alloy_image": test_e2e.IMAGE_DEFAULTS["alloy"],
                "store_image": store_image(self.store_kind),
            },
            "timeline": timeline_view(self.sampler, first),
            "archive": str(self.archive),
        })

    def fault_checks(self, check, events, statuses, failed_lines, backpressure, flush_failures,
                     retries, samples):
        name = self.name
        if name in ("healthy", "soak"):
            check("alloy_saw_no_errors", failed_lines == 0 and set(statuses) <= {"OK"},
                  f"statuses {dict(statuses)}, 'Exporting failed' lines {failed_lines}")
        if name == "s3_outage":
            check("fault_observed", flush_failures > 0 or retries > 0,
                  f"flush failures {flush_failures}, buffer retries {retries}")
            check("alloy_saw_no_errors", failed_lines == 0 and set(statuses) <= {"OK"},
                  f"statuses {dict(statuses)}, 'Exporting failed' lines {failed_lines}")
        if name == "wal_full":
            check("fault_observed", *wal_full_observed(backpressure, statuses))
        if name == "engine_restart":
            exits = [e for e in self.events if e["kind"] == "engine_exited"]
            ok = all(e.get("code") == 0 and not e.get("forced")
                     and e["exit_s"] <= SIGTERM_GRACE_S + 5 for e in exits)
            check("graceful_exit", ok and exits, f"exits {exits}")
        if name == "engine_kill":
            exits = [e for e in self.events if e["kind"] == "engine_exited"]
            check("fault_observed", exits and all(e.get("code") == -9 for e in exits),
                  f"exits {exits}")
        if name == "alloy_kill":
            killed = [e for e in self.events if e["kind"] == "alloy_sigkill"]
            check("fault_observed", killed and killed[0].get("queues_full")
                  and all(a.restarts for a in self.alloys),
                  f"killed {killed}, restarts {[a.restarts for a in self.alloys]}")
        if name == "alloy_restart":
            restarted = all(a.restarts for a in self.alloys)
            check("fault_observed", restarted, {a.index: a.restarts for a in self.alloys})


def buffer_losses(samples):
    """Buffer loss counters and permanent rejections, summed over every boot."""
    loss = collections.Counter()
    permanent = 0.0
    for sample in last_per_boot(samples):
        for key, value in (sample.get("metrics") or {}).items():
            if key.startswith("processor.durable_buffer.loss") and value:
                loss[key] += value
        permanent += metric(sample, "processor.durable_buffer.bundles.resolved"
                                    "{outcome=permanently_rejected}")
    return dict(loss), permanent


def duplicate_allowance(case, producers, rate, settings):
    """The duplicate lines one producer may store per fault event, and why."""
    batch = settings["batch_max_records"]
    consumers = settings["num_consumers"]
    if case == "engine_kill":
        # Its exports in flight (resent by Alloy), what the WAL acknowledged
        # since its last 100ms tick, and its share of one block whose
        # acknowledgement was not yet persisted (one window of input).
        per = consumers * batch + rate * 0.1 + rate * WINDOW_S
        return {"event": "engine_sigkill", "per_producer_per_event_lines": int(per),
                "rule": "in-flight exports + 100ms of WAL acks + one window of input"}
    if case == "alloy_kill":
        # Re-read from the last saved position (at most 10s old), plus the
        # batches the file-backed queue held, whose exports may have been
        # applied before the kill.
        per = rate * 10 + settings["queue_size_records"] + batch
        return {"event": "alloy_sigkill", "per_producer_per_event_lines": int(per),
                "rule": "10s position sync + the queue + one batch"}
    if case == "alloy_restart":
        per = rate * 10 + batch
        return {"event": "alloy_stop", "per_producer_per_event_lines": int(per),
                "rule": "10s position sync + one batch"}
    return {"event": None, "per_producer_per_event_lines": 0, "rule": "no duplicates"}


def boot_copies(run):
    """{boot ordinal: copies} of a duplicate run, or None when it cannot be known.

    Runs recorded before per-boot counts were kept carry only the distinct
    boots; their counts follow when every copy is in one boot or each copy in
    its own boot.
    """
    if "boot_copies" in run:
        return {int(b): n for b, n in run["boot_copies"].items()}
    boots = run["boots"]
    if len(boots) == 1:
        return {boots[0]: run["copies"]}
    if len(boots) == run["copies"]:
        return {b: 1 for b in boots}
    return None


def duplicate_check(case, runs, runs_count, events, allowed):
    """Duplicates per (fault event, producer) against the per-producer bound.

    For an engine SIGKILL the earliest copy of a line is the original and
    every other copy is charged to the kill that started the boot which
    wrote it (boot `b`, kill `b - 1`); a copy in the first boot or in no known
    boot belongs to no kill and fails. An Alloy fault is one event per
    producer.
    """
    if len(runs) < runs_count:
        return False, f"only {len(runs)} of {runs_count} duplicate runs were kept"
    bound = allowed["per_producer_per_event_lines"]
    kills = sum(1 for e in events if e["kind"] == allowed["event"])
    charged = collections.Counter()
    problems = []
    for run in runs:
        if case != "engine_kill":
            if not kills:
                problems.append(f"run {run} with no fault event")
                continue
            charged[(0, run["producer"])] += run["lines"] * (run["copies"] - 1)
            continue
        per_boot = boot_copies(run)
        if per_boot is None:
            problems.append(f"run {run} has no per-boot copy counts")
            continue
        copies = sorted(b for b, n in per_boot.items() for _ in range(n))
        for boot in copies[1:]:
            event = boot - 1
            if boot < 0 or not 0 <= event < kills:
                problems.append(f"a copy of run {run} in boot {boot} belongs to no kill")
                continue
            charged[(event, run["producer"])] += run["lines"]
    over = {f"event {e} producer {p}": n for (e, p), n in charged.items() if n > bound}
    table = {f"event {e} producer {p}": n for (e, p), n in sorted(charged.items())}
    passed = not problems and not over
    return passed, (f"per (event, producer) {table}; bound {bound} each ({allowed['rule']}); "
                    f"over {over}; {problems[:5]}")


def memory_queue_variant(text):
    """The reference River config with its sending queue kept in memory."""
    lines = [row for row in text.splitlines()
             if "otelcol.storage.file" not in row]
    variant = "\n".join(lines) + "\n"
    if "storage" in variant.split("sending_queue", 1)[1].split("}", 1)[0]:
        raise AssertionError("the queue storage line was not removed")
    return variant


def wal_full_observed(backpressure, statuses):
    """Whether the WAL refused requests and Alloy was told UNAVAILABLE.

    `statuses` are Alloy's `rpc_response_status_code` label values, which
    name the gRPC code in upper case.
    """
    unavailable = statuses.get("UNAVAILABLE", 0)
    return (backpressure > 0 and unavailable > 0,
            f"backpressure refusals {backpressure}, Alloy UNAVAILABLE {unavailable}")


def archived_samples(result):
    """The engine samples a result's raw archive kept, or None."""
    archive = str(result.get("archive", "")).replace(
        "<main_checkout>", str(faults.FAULT_ARCHIVE_ROOT.parent))
    path = Path(archive) / "samples.json"
    if not path.is_file():
        return None
    return [s for s in json.loads(path.read_text())["engine"] if "metrics" in s]


def rejudge(path):
    """Advance a published result to the current checks, from what it stored.

    Re-judges the WAL-full fault check, the duplicate bound per fault event
    and producer, and the buffer-loss check summed over every boot (from the
    raw archive's samples); each changed verdict is recorded.
    """
    path = Path(path)
    result = json.loads(path.read_text())
    metrics = result["metrics"]
    env = result["environment"]
    new = {}
    if result["case"] == "wal_full":
        new["fault_observed"] = (wal_full_observed(metrics["buffer_backpressure_refusals"],
                                                   metrics["alloy_calls_by_status"]),
                                 "the check read the status label 'Unavailable'; Alloy "
                                 "reports 'UNAVAILABLE'")
    allowed = duplicate_allowance(result["case"], env["producers"], env["rate_per_producer"],
                                  result["alloy_settings"])
    runs = result["duplicates"]["runs"]
    new["duplicates_within_bound"] = (
        duplicate_check(result["case"], runs, result["duplicates"]["runs_count"],
                        result["events"], allowed),
        "the bound is enforced per fault event and producer, not as a fleet total")
    samples = archived_samples(result)
    if samples is None:
        new["no_buffer_loss"] = ((False, "the raw archive's samples are missing; rerun"),
                                 "summed over every boot")
    else:
        loss, permanent = buffer_losses(samples)
        new["no_buffer_loss"] = ((not loss and permanent == 0,
                                  f"loss {loss}, permanently_rejected {permanent}, "
                                  f"over {len(last_per_boot(samples))} boots"),
                                 "loss and permanent rejections summed over every boot")
    result["duplicate_allowance"] = allowed
    for entry in result["checks"]:
        if entry["name"] not in new:
            continue
        (passed, detail), reason = new[entry["name"]]
        if entry["passed"] != passed or entry["detail"] != detail:
            result.setdefault("rejudgement", []).append({
                "check": entry["name"], "was": entry["passed"], "now": passed,
                "reason": reason, "at_utc": measurement.utc_now()})
        entry.update(passed=bool(passed), detail=detail)
    result["status"] = "passed" if all(c["passed"] for c in result["checks"]) else "failed"
    path.write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n")
    return result


def last_per_boot(samples):
    """The last metric sample of every engine boot (counters restart per boot)."""
    by_boot = {}
    for sample in samples:
        by_boot[sample["boot"]] = sample
    return list(by_boot.values())


def sha256(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def store_image(kind):
    return os.environ.get("SERIES_" + kind.upper() + "_IMAGE", test_e2e.IMAGE_DEFAULTS[kind])


def scrub(changed, case):
    """Site substitutions with this run's ports and paths as placeholders."""
    text = json.dumps(changed)
    for value, token in ((str(case.wal_dir), "<wal_dir>"), (case.store.endpoint, "<store>"),
                         (f"127.0.0.1:{case.grpc_port}", "<grpc_addr>")):
        text = text.replace(value, token)
    return json.loads(text)


def quantiles(pairs, qs=(0.5, 0.99)):
    """Weighted quantiles of (value, weight) pairs, and the maximum."""
    pairs = sorted(p for p in pairs if p[1] > 0)
    total = sum(w for _, w in pairs)
    found = {}
    for q in qs:
        target = q * total
        run = 0
        for value, weight in pairs:
            run += weight
            if run >= target:
                found[q] = value
                break
    return {"p50_s": found.get(0.5), "p99_s": found.get(0.99),
            "max_s": pairs[-1][0] if pairs else None, "lines": total}


def write_time_ns(feed, producer, seq):
    """Wall time by which line `seq` of `producer` was in its file."""
    timeline = feed["timeline"]
    counts = [entry[2][producer] for entry in timeline]
    index = bisect.bisect_right(counts, seq)
    index = min(index, len(timeline) - 1)
    return timeline[index][1]


def freshness_view(groups, feed, objects, *, first_wall):
    """Line written -> its values object first listed, overall and per 10s of input.

    `groups` are the read-back's (key, producer, first seq, lines) groups of at
    most FRESHNESS_GROUP_LINES consecutive lines of one object; a group's write
    time is its last line's. `last_modified` is the store's own completion time.
    """
    listed, modified = [], []
    per_bucket = collections.defaultdict(list)
    unmatched = 0
    for key, producer, seq, count in groups:
        entry = objects.get("otel/" + key)
        if entry is None:
            unmatched += count
            continue
        written = write_time_ns(feed, producer, seq + count - 1)
        fresh = (entry["first_seen_ns"] - written) / 1e9
        listed.append((fresh, count))
        modified.append(((entry["last_modified_ns"] - written) / 1e9, count))
        per_bucket[int((written - first_wall) / 1e10)].append((fresh, count))
    timeline = [{"input_s": bucket * 10, **quantiles(values)}
                for bucket, values in sorted(per_bucket.items())]
    return {"overall": quantiles(listed), "by_last_modified": quantiles(modified),
            "per_10s_of_input": timeline, "lines_without_listing": unmatched,
            "listing_period_s": 2.0}


def duplicate_view(runs, boots):
    """Duplicated lines grouped into runs, with the boots their copies were written by."""
    index = {boot: n for n, boot in enumerate(boots) if boot}
    summary = collections.Counter()
    shaped = []
    for producer, first, last, copies, run_boots in runs:
        per_boot = collections.Counter(index.get(b, -1) for b in run_boots)
        ordinals = sorted(per_boot)
        lines = last - first + 1
        summary[",".join(map(str, ordinals))] += lines * (copies - 1)
        shaped.append({"producer": producer, "first_seq": first, "last_seq": last,
                       "lines": lines, "copies": copies, "boots": ordinals,
                       "boot_copies": {str(b): n for b, n in sorted(per_boot.items())}})
    return {"runs": shaped[:200], "runs_count": len(shaped),
            "duplicate_lines_by_boots": dict(summary)}


def ack_latency(buckets_by_alloy):
    """Alloy's own OK export durations, summed over the fleet, as bucket bounds."""
    merged = collections.defaultdict(float)
    for buckets in buckets_by_alloy.values():
        for bound, count in (buckets.get("OK") or []):
            merged[bound] += count
    ordered = sorted(merged.items())
    return {
        "p50_le_s": alloy_capacity.json_bound(alloy_capacity.bucket_quantile(ordered, 0.5)),
        "p99_le_s": alloy_capacity.json_bound(alloy_capacity.bucket_quantile(ordered, 0.99)),
        "buckets": [[alloy_capacity.json_bound(b), c] for b, c in ordered],
        "source": "rpc_client_call_duration_seconds of otelcol.exporter.otlp.series",
    }


def timeline_view(sampler, first):
    """Ten-second view of the run: rates, backlog, memory and WAL."""
    rows = []
    last_t = None
    for sample in sampler.engine_samples:
        at = (sample["t"] - first) / 1e9
        if last_t is not None and at - last_t < 10:
            continue
        last_t = at
        rows.append({
            "at_s": round(at, 1), "boot": sample["boot"],
            "rss_bytes": sample.get("VmRSS"), "wal_disk_bytes": sample["wal_disk_bytes"],
            "wal_used_bytes": metric(sample, "processor.durable_buffer.storage.bytes.used", None),
            "buffer_in_flight": metric(sample, "processor.durable_buffer.in.flight", None),
            "rows_written": metric(sample, "exporter.series_parquet.rows.written"
                                           "{dataset=values,signal=logs}", None),
            "oldest_unacked_s": metric(sample, "exporter.series_parquet.oldest_unacked.age", None),
            "backpressure": metric(sample, "processor.durable_buffer.ingest.failures"
                                           "{failure=backpressure}", None),
        })
    return rows


def read_back(root, written, body_bytes=BODY_BYTES) -> dict:
    """Every line of every producer stored, read by DuckDB and clickhouse-local."""
    import duckdb
    root = Path(root).resolve()
    relative = "v=1/signal=logs/dataset=values/**/*.parquet"
    series = "v=1/signal=logs/dataset=series/**/*.parquet"
    problems = []
    if not list(root.glob(relative)):
        return {"passed": False, "problems": ["no logs values files"]}
    values = (f"read_parquet({test_e2e.sql_string(root / relative)}, union_by_name=true, "
              "hive_partitioning=false, filename=true)")
    descriptors = (f"read_parquet({test_e2e.sql_string(root / series)}, union_by_name=true, "
                   "filename=true, hive_partitioning=false)")
    well = (f"length(body) = {body_bytes} AND substr(body, 1, 1) = 'p' AND "
            f"TRY_CAST(substr(body, 2, 2) AS INTEGER) IS NOT NULL AND "
            f"TRY_CAST(substr(body, 5, {SEQ_DIGITS}) AS BIGINT) IS NOT NULL")
    prod = "TRY_CAST(substr(body, 2, 2) AS INTEGER)"
    seq = f"TRY_CAST(substr(body, 5, {SEQ_DIGITS}) AS BIGINT)"
    with duckdb.connect() as db:
        db.execute(f"CREATE TABLE v AS SELECT producer_id, {prod} AS p, {seq} AS s, "
                   f"({well}) AS ok, filename FROM {values}")
        rows = db.execute("""
            SELECT p, producer_id, count(*), count(DISTINCT s), min(s), max(s), sum(s)
            FROM v WHERE ok GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()
        malformed = db.execute("SELECT count(*) FROM v WHERE NOT ok").fetchone()[0]
        total = db.execute("SELECT count(*) FROM v").fetchone()[0]
        dup_lines = db.execute("""
            SELECT p, s, count(*) AS c, list(regexp_extract(filename,
                   '-([0-9a-f]+)-[0-9]+\\.parquet$', 1)) AS boots
            FROM v WHERE ok GROUP BY 1, 2 HAVING count(*) > 1 ORDER BY 1, 2""").fetchall()
        groups = db.execute(f"""
            SELECT regexp_extract(filename, 'v=1/.*$') AS key, p,
                   min(s) AS first, count(*) AS n
            FROM v WHERE ok GROUP BY 1, 2, s // {FRESHNESS_GROUP_LINES}""").fetchall()
        joined = db.execute(f"""
            WITH canonical AS (
                SELECT * FROM {descriptors}
                QUALIFY row_number() OVER (
                    PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1)
            SELECT count(*) FROM read_parquet({test_e2e.sql_string(root / relative)},
                union_by_name=true, hive_partitioning=false) x
            INNER JOIN canonical c ON x.series_id = c.series_id""").fetchone()[0]
    with test_e2e.clickhouse_reader(root) as clickhouse:
        ch = {int(r[0]): (int(r[1]), int(r[2])) for r in clickhouse(
            f"SELECT toInt32(substring(body, 2, 2)), count(), sum(toInt64(substring(body, 5, "
            f"{SEQ_DIGITS}))) FROM file({test_e2e.sql_string(relative)}, 'Parquet') "
            f"WHERE length(body) = {body_bytes} GROUP BY 1")}
    per = {}
    for p, pid, count, distinct, low, high, total_seq in rows:
        entry = per.setdefault(p, {"rows": 0, "distinct": 0, "producer_ids": [], "min": low,
                                   "max": high, "seq_sum": 0})
        entry["rows"] += count
        entry["distinct"] = max(entry["distinct"], distinct)
        entry["producer_ids"].append(pid)
        entry["seq_sum"] += int(total_seq)
    for p, expected in enumerate(written):
        entry = per.get(p)
        if entry is None:
            if expected:
                problems.append(f"producer {p}: no rows of {expected} lines")
            continue
        if entry["producer_ids"] != [producer_id(p)]:
            problems.append(f"producer {p}: producer_id {entry['producer_ids']}")
        if entry["distinct"] != expected or entry["min"] != 0 or entry["max"] != expected - 1:
            problems.append(f"producer {p}: {entry['distinct']} distinct of {expected} lines, "
                            f"range {entry['min']}..{entry['max']}")
        if ch.get(p) != (entry["rows"], entry["seq_sum"]):
            problems.append(f"producer {p}: ClickHouse {ch.get(p)}, DuckDB "
                            f"{(entry['rows'], entry['seq_sum'])}")
    if set(per) - set(range(len(written))):
        problems.append(f"rows of unknown producers {sorted(set(per) - set(range(len(written))))}")
    if malformed:
        problems.append(f"{malformed} malformed bodies")
    if joined != total:
        problems.append(f"latest-descriptor join kept {joined} of {total} rows")
    duplicate_rows = sum(c - 1 for _p, _s, c, _b in dup_lines)
    runs = []
    for p, s, c, b in dup_lines:
        # One boot id per copy, so the copies each boot wrote are kept.
        boots = tuple(sorted(b))
        if runs and runs[-1][0] == p and runs[-1][2] == s - 1 and runs[-1][3] == c \
                and runs[-1][4] == boots:
            runs[-1][2] = s
        else:
            runs.append([p, s, s, c, boots])
    return {
        "passed": not problems, "problems": problems, "total_rows": total,
        "expected_lines": sum(written), "duplicate_rows": duplicate_rows,
        "per_producer": {str(k): v for k, v in sorted(per.items())},
        "readers": ["duckdb", "clickhouse"],
        "duplicate_runs": [tuple(r) for r in runs], "groups": groups,
    }


def scrub_paths(result):
    """The result with this host's checkout paths replaced by placeholders."""
    text = json.dumps(result, default=str)
    for root, token in ((test_e2e.WORKSPACE, "<workspace>"),
                        (faults.FAULT_ARCHIVE_ROOT.parent, "<main_checkout>")):
        text = text.replace(str(root), token)
    return json.loads(text)


def publish(result, report_dir):
    result = scrub_paths(result)
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    label = (result.get("options") or {}).get("label")
    name = f"{REPORT_PREFIX}-{result['case'].replace('_', '-')}-{result['store']}"
    path = report_dir / (f"{name}-{label}.json" if label else f"{name}.json")
    path.write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n")
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="reference_deployment", description=__doc__)
    parser.add_argument("--case", choices=CASES)
    parser.add_argument("--rejudge", help="a published result to re-judge")
    parser.add_argument("--store", choices=("minio", "rustfs"), default="minio")
    parser.add_argument("--work-dir", default="/var/tmp/series-reference")
    parser.add_argument("--report-dir", default=str(measurement.REPORT_DIR))
    parser.add_argument("--option", action="append", default=[],
                        help="name=value, decoded as JSON when it parses")
    args = parser.parse_args(argv)
    if args.rejudge:
        result = rejudge(args.rejudge)
        print(json.dumps({"status": result["status"],
                          "rejudgement": result.get("rejudgement")}, indent=1))
        return 0 if result["status"] == "passed" else 1
    if not args.case:
        parser.error("--case is required")
    # A terminated run still removes its containers, engines and work directory.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    options = {}
    for item in args.option:
        name, _, raw = item.partition("=")
        try:
            options[name] = json.loads(raw)
        except ValueError:
            options[name] = raw
    case = Case(args.case, args.store, args.work_dir,
                producers=options.pop("producers", PRODUCERS),
                rate=options.pop("rate", RATE_PER_PRODUCER), options=options)
    result = case.run()
    path = publish(result, options.get("report_dir", args.report_dir))
    print(json.dumps({"status": result["status"], "path": str(path),
                      "metrics": result["metrics"],
                      "failed": [c for c in result["checks"] if not c["passed"]]},
                     indent=1, default=str))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
