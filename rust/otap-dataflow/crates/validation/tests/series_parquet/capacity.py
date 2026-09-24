# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""The maximum sustainable throughput and durable write speed of the engine.

An open-loop producer offers a fixed record rate to a fresh release engine
for a warm-up and a measured interval; a trial is sustainable when the
durable rate keeps up and the backlog does not grow, and a search brackets
the highest sustainable rate. See the harness README, "Capacity".
"""
import array
import bisect
import collections
import concurrent.futures
import contextlib
import dataclasses
import functools
import hashlib
import json
import math
import mmap
import multiprocessing
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
    from . import performance
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement
    import performance

test_e2e = measurement.test_e2e


def rates(unique_records, wire_bytes, object_bytes, seconds, workers) -> dict:
    """Records, input bytes and output bytes per second, each on its own
    denominator, and the record rate per worker core."""
    if seconds <= 0 or workers <= 0:
        raise ValueError("positive duration and worker count required")
    return {
        "records_per_s": unique_records / seconds,
        "records_per_s_per_core": unique_records / seconds / workers,
        "input_bytes_per_s": wire_bytes / seconds,
        "object_bytes_per_s": object_bytes / seconds,
    }


# --------------------------------------------------------------------------
# The rules of a trial and of a search
# --------------------------------------------------------------------------

SEARCH_START_RECORDS_PER_S = 1000
DOUBLING_TRIALS = 12
BRACKET_WIDTH_RATIO = 0.10
REPETITIONS = 3
WARMUP_S = 15
MEASURE_S = 60
# The plan's capacity stability row: over the last 30 s of the measured
# interval the unique durable rate is at least 98 percent of the offered
# rate, and the backlog grows by at most 2 percent of the offered rate.
STABILITY_TAIL_S = 30
DURABLE_RATIO_FLOOR = 0.98
BACKLOG_SLOPE_LIMIT_RATIO = 0.02
# A send that starts this long after its target while an in-flight slot was
# free is the producer falling behind, not the engine.
PRODUCER_LATE_S = 0.05
PRODUCER_LATE_LIMIT_RATIO = 0.01
SENDER_HEADROOM_RATIO = 1.5
CONFIRMATION_FRACTION = 0.8

# The producer: spawned processes on the producer's physical cores, each
# owning a share of the client connections, sending prebuilt requests.
PRODUCER_PROCESSES = 4
SEARCH_CONNECTIONS = 256
FAN_IN_CONNECTIONS = (1, 8, 64, 256)
PRODUCER_TIMEOUT_S = 180.0
# How far ahead of its schedule a sender asks for its requests' pages.
PREFETCH_AHEAD_S = 3.0
SENDER_READY_DEADLINE_S = 120
MESSAGE_LIMIT_BYTES = 64 << 20

# The engine settings a search runs under. One-second windows keep the
# strict hold time short, so the producer's bounded concurrency does not cap
# a low-cardinality workload; `DEFAULT_INTERVAL_S` is the shipped window.
SEARCH_INTERVAL_S = 1
DEFAULT_INTERVAL_S = 15
SHIPPED_RECEIVER_CAPACITY = 128
RAISED_RECEIVER_CAPACITY = 4096

# jemalloc's statistics print after this many bytes of allocation. Its
# `allocated` total is the heap term of the RSS reconciliation, so every
# exporter trial carries it; the interval is coarse enough that the prints
# cost nothing measurable, which a trial without them checks.
JEMALLOC_STATS_INTERVAL_BYTES = 64 << 20
# How often the allocator prints are looked for, and paired with the RSS.
ALLOCATOR_TAIL_S = 0.005
JEMALLOC_STATS_CONF = (
    f"stats_interval:{JEMALLOC_STATS_INTERVAL_BYTES},stats_interval_opts:Jgmdablxeh"
)

RECORDS_PER_REQUEST = 1000

# A trial samples every 0.25 s; its published file keeps every fourth.
PUBLISHED_SAMPLE_STRIDE = 4

# The measured workloads. `first_index` skips the single metrics request a
# logs-only workload carries at index 0.
CAPACITY_WORKLOADS = {
    "mixed-1k-hot": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=1024,
            series=10000, metrics_every=5, series_scope="record",
        ),
        "first_index": 0,
        "description": "80/20 logs/metric points by record, 1KiB bodies, "
        "10k series slots cycled per record",
    },
    "logs-1k-hot": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=1024,
            series=10000, metrics_every=10**9, series_scope="record",
        ),
        "first_index": 1,
        "description": "logs only, 1KiB bodies, 10k series slots cycled per record",
    },
    "mixed-8k-hot": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=8192,
            series=10000, metrics_every=5, series_scope="record",
        ),
        "first_index": 0,
        "description": "the 8KiB-body variant of mixed-1k-hot",
    },
    "mixed-1k-churn": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=1024,
            series=10**12, metrics_every=5, series_scope="request",
        ),
        "first_index": 0,
        "description": "every request brings new series: one logs series and "
        "one slot per request, never repeated",
    },
    "metrics-1k-hot": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=1024,
            series=10000, metrics_every=1, series_scope="record",
        ),
        "first_index": 0,
        "description": "metric points only, 10k series slots cycled per point",
    },
    "metrics-1k-unique": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=1024,
            series=10**12, metrics_every=1, series_scope="record",
        ),
        "first_index": 0,
        "description": "metric points only, one attribute unique per point, "
        "so every point is a new series",
    },
}
PRIMARY_WORKLOAD = "mixed-1k-hot"


def trial_requests(rate_records_per_s, duration_s, records_per_request) -> int:
    """How many requests an offered rate schedules over a duration."""
    return max(1, int(round(rate_records_per_s * duration_s / records_per_request)))


def next_search_rate(trials, *, start=SEARCH_START_RECORDS_PER_S,
                     doubling=DOUBLING_TRIALS, width=BRACKET_WIDTH_RATIO):
    """The next offered rate of a search, or None when it is decided.

    `trials` are (rate, verdict) pairs in the order they ran. Rates double
    from `start` while every trial was sustainable, up to `doubling` trials;
    then the bracket between the highest sustainable and the lowest
    unsustainable rate is bisected until its width is at most `width` of
    the sustainable rate. A producer-limited trial ends the search: the
    producer, not the engine, bounded it.
    """
    if any(verdict == "producer_limited" for _rate, verdict in trials):
        return None
    sustainable = [rate for rate, verdict in trials if verdict == "sustainable"]
    unsustainable = [rate for rate, verdict in trials if verdict == "unsustainable"]
    if not trials:
        return start
    if not unsustainable:
        if len(trials) >= doubling:
            return None
        return max(sustainable) * 2
    high = min(unsustainable)
    below = [rate for rate in sustainable if rate < high]
    if not below:
        if high <= start:
            return None
        return max(start, high // 2)
    low = max(below)
    if (high - low) / low <= width:
        return None
    return int(round((low + high) / 2))


def search_decision(trials) -> dict:
    """The bracket a finished search establishes."""
    sustainable = [rate for rate, verdict in trials if verdict == "sustainable"]
    unsustainable = [rate for rate, verdict in trials if verdict == "unsustainable"]
    limited = [rate for rate, verdict in trials if verdict == "producer_limited"]
    high = min(unsustainable) if unsustainable else None
    below = [rate for rate in sustainable if high is None or rate < high]
    low = max(below) if below else None
    return {
        "sustainable_records_per_s": low,
        "unsustainable_records_per_s": high,
        "bracketed": low is not None and high is not None,
        "bracket_width_ratio": (high - low) / low if low and high else None,
        "producer_limited_records_per_s": min(limited) if limited else None,
        # Rates measured both ways: the band where verdicts flip.
        "flip_rates_records_per_s": sorted(set(sustainable) & set(unsustainable)),
        "kind": (
            "maximum" if low is not None and high is not None
            else "lower_bound" if low is not None else "none"
        ),
    }


def backlog_slope(points) -> float:
    """The least-squares slope of (seconds, backlog records) points."""
    if len(points) < 2:
        return 0.0
    mean_t = sum(t for t, _ in points) / len(points)
    mean_b = sum(b for _, b in points) / len(points)
    spread = sum((t - mean_t) ** 2 for t, _ in points)
    if spread == 0:
        return 0.0
    return sum((t - mean_t) * (b - mean_b) for t, b in points) / spread


def stability_verdict(*, offered, tail_durable, backlog_slope_records_per_s,
                      late_unblocked_ratio, failed_requests, partial_requests) -> dict:
    """Whether one trial sustained its offered rate, and why not.

    A request the engine refused or failed decides first: the engine did
    not sustain the rate, whatever the producer did. Otherwise a trial whose
    sends fell behind their targets while in-flight slots were free measured
    the producer, and says nothing about the engine.
    """
    reasons = []
    if not failed_requests and late_unblocked_ratio > PRODUCER_LATE_LIMIT_RATIO:
        return {
            "verdict": "producer_limited",
            "reasons": [
                f"{late_unblocked_ratio:.3%} of sends started over "
                f"{PRODUCER_LATE_S}s late with an in-flight slot free"
            ],
        }
    if tail_durable < DURABLE_RATIO_FLOOR * offered:
        reasons.append(
            f"durable rate over the last {STABILITY_TAIL_S}s {tail_durable:.0f} < "
            f"{DURABLE_RATIO_FLOOR:.0%} of offered {offered:.0f}"
        )
    if backlog_slope_records_per_s > BACKLOG_SLOPE_LIMIT_RATIO * offered:
        reasons.append(
            f"backlog grows {backlog_slope_records_per_s:.0f} records/s > "
            f"{BACKLOG_SLOPE_LIMIT_RATIO:.0%} of offered"
        )
    if failed_requests:
        reasons.append(f"{failed_requests} requests failed")
        if late_unblocked_ratio > PRODUCER_LATE_LIMIT_RATIO:
            reasons.append(f"{late_unblocked_ratio:.3%} of sends also started late with a slot free")
    if partial_requests:
        reasons.append(f"{partial_requests} requests partially rejected")
    return {"verdict": "unsustainable" if reasons else "sustainable", "reasons": reasons}


# --------------------------------------------------------------------------
# The prebuilt request pool
# --------------------------------------------------------------------------

POOL_FORMAT = "series-capacity-pool/1"
POOL_SEGMENT_REQUESTS = 1024
POOL_CHUNK_REQUESTS = 64
# One entry per request: index, offset, length, signal, wire SHA-256.
_POOL_ENTRY = struct.Struct("<QQIB32s")
_DIGEST_BYTES = 33
SIGNALS = ("logs", "metrics")


def _build_pool_chunk(payload) -> list:
    """Build one chunk of a pool segment; runs in a worker process."""
    workload_fields, indexes = payload
    built = []
    for index, signal, wire, packed in performance._build_prebuilt_chunk(
        (workload_fields, indexes)
    ):
        built.append((index, signal, wire, packed, hashlib.sha256(wire).digest()))
    return built


class CapacityPool:
    """A workload's requests, built once in segments and read by index.

    A segment holds `POOL_SEGMENT_REQUESTS` consecutive request indexes of
    both signals. A request's bytes depend only on the workload and its
    index, so the pool grows by segments without rebuilding what exists.
    """

    FILES = ("wire.bin", "entries.bin", "digests.bin")

    def __init__(self, directory, workload):
        self.directory = Path(directory)
        self.workload = dataclasses.replace(workload, requests=1)
        self.segments = {}

    def _segment_dir(self, number) -> Path:
        return self.directory / f"segment-{number:05d}"

    def _segment_valid(self, number) -> bool:
        path = self._segment_dir(number) / "sidecar.json"
        if not path.is_file():
            return False
        sidecar = json.loads(path.read_text(encoding="ascii"))
        return (
            sidecar.get("format") == POOL_FORMAT
            and sidecar.get("workload") == self.workload.as_json()
            and sidecar.get("first") == number * POOL_SEGMENT_REQUESTS
            and all(
                (self._segment_dir(number) / name).is_file()
                and (self._segment_dir(number) / name).stat().st_size
                == sidecar["sizes"][name]
                for name in self.FILES
            )
        )

    def ensure(self, end_index, *, processes=8) -> dict:
        """Build every segment below `end_index` that is not built yet."""
        wanted = range(0, (end_index + POOL_SEGMENT_REQUESTS - 1) // POOL_SEGMENT_REQUESTS)
        missing = [number for number in wanted if not self._segment_valid(number)]
        started = time.monotonic_ns()
        for number in missing:
            self._build_segment(number, processes)
        return {
            "segments_built": len(missing),
            "segments": len(wanted),
            "build_s": (time.monotonic_ns() - started) / 1e9,
        }

    def _build_segment(self, number, processes):
        """Build one segment in worker processes and write it atomically."""
        directory = self._segment_dir(number)
        directory.mkdir(parents=True, exist_ok=True)
        first = number * POOL_SEGMENT_REQUESTS
        last = first + POOL_SEGMENT_REQUESTS
        fields = dataclasses.replace(self.workload, requests=last).as_json()
        chunks = [
            (fields, list(range(start, min(last, start + POOL_CHUNK_REQUESTS))))
            for start in range(first, last, POOL_CHUNK_REQUESTS)
        ]
        temporary = {name: directory / f"{name}.partial" for name in self.FILES}
        offset = 0
        records = 0
        with open(temporary["wire.bin"], "wb") as wire, open(
            temporary["entries.bin"], "wb"
        ) as entries, open(temporary["digests.bin"], "wb") as digests, \
                concurrent.futures.ProcessPoolExecutor(
                    max_workers=processes,
                    mp_context=multiprocessing.get_context("spawn"),
                ) as pool:
            for built in pool.map(_build_pool_chunk, chunks):
                for index, signal, body, packed, digest in built:
                    _ = wire.write(body)
                    _ = entries.write(
                        _POOL_ENTRY.pack(index, offset, len(body), SIGNALS.index(signal), digest)
                    )
                    _ = digests.write(packed)
                    offset += len(body)
                    records += len(packed) // _DIGEST_BYTES
        for name, path in temporary.items():
            os.replace(path, directory / name)
        sidecar = {
            "format": POOL_FORMAT,
            "workload": self.workload.as_json(),
            "first": first,
            "count": POOL_SEGMENT_REQUESTS,
            "records": records,
            "wire_bytes": offset,
            "sizes": {name: (directory / name).stat().st_size for name in self.FILES},
        }
        _ = measurement.write_json_atomic(directory / "sidecar.json", sidecar)
        check = measurement.build_request(
            dataclasses.replace(self.workload, requests=last), first + 1
        )
        if self.request(first + 1)[:2] != check[:2]:
            raise AssertionError(f"pool segment {number} differs from build_request")

    def _segment(self, number):
        """The mapped files of one segment."""
        segment = self.segments.get(number)
        if segment is None:
            directory = self._segment_dir(number)
            handles = [open(directory / name, "rb") for name in self.FILES]
            maps = [mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
                    for handle in handles]
            segment = {"handles": handles, "wire": maps[0], "entries": maps[1],
                       "digests": maps[2]}
            self.segments[number] = segment
        return segment

    def entry(self, index):
        """(offset, length, signal, wire sha256) of one request."""
        number, position = divmod(index, POOL_SEGMENT_REQUESTS)
        segment = self._segment(number)
        found, offset, length, signal, digest = _POOL_ENTRY.unpack_from(
            segment["entries"], position * _POOL_ENTRY.size
        )
        if found != index:
            raise AssertionError(f"pool entry {position} of segment {number} is {found}")
        return offset, length, SIGNALS[signal], digest

    def wire(self, index):
        """The signal and exact wire bytes of one request."""
        offset, length, signal, _digest = self.entry(index)
        segment = self._segment(index // POOL_SEGMENT_REQUESTS)
        return signal, segment["wire"][offset:offset + length]

    def prefetch(self, index):
        """Ask the kernel to read one request's bytes ahead, without waiting."""
        offset, length, _signal, _digest = self.entry(index)
        wire = self._segment(index // POOL_SEGMENT_REQUESTS)["wire"]
        start = offset - offset % mmap.PAGESIZE
        wire.madvise(mmap.MADV_WILLNEED, start, offset + length - start)

    def request(self, index):
        """Request `index` as `build_request` returns it."""
        signal, wire = self.wire(index)
        return signal, wire, list(self.records(index))

    def records(self, index):
        """(record id, kind, expected sha256) of every record of a request."""
        number, position = divmod(index, POOL_SEGMENT_REQUESTS)
        segment = self._segment(number)
        width = self.workload.records_per_request
        base = position * width * _DIGEST_BYTES
        for point in range(width):
            start = base + point * _DIGEST_BYTES
            entry = segment["digests"][start:start + _DIGEST_BYTES]
            kind = measurement.ALL_KINDS[entry[0]]
            yield (
                measurement.stable_id(self.workload.seed, index, point, kind),
                kind,
                bytes(entry[1:]).hex(),
            )

    def close(self):
        """Release every map and file."""
        for segment in self.segments.values():
            for name in ("wire", "entries", "digests"):
                segment[name].close()
            for handle in segment["handles"]:
                handle.close()
        self.segments = {}


# --------------------------------------------------------------------------
# The open-loop producer
# --------------------------------------------------------------------------

OUTCOME_CODES = {
    0: measurement.OUTCOME_ACK,
    1: measurement.OUTCOME_PARTIAL,
    2: measurement.OUTCOME_RETRYABLE,
    3: measurement.OUTCOME_PERMANENT,
    4: measurement.OUTCOME_LOCAL,
}


def connection_plan(request_count, connections, processes) -> list:
    """Which process and connection sends each of a trial's requests.

    Request position `j` goes to connection `j % connections`, and
    connection `c` belongs to process `c % processes`, so every connection
    carries the same number of requests to one or two.
    """
    active = max(1, min(processes, connections))
    plans = [{"connections": [], "positions": [], "local": []} for _ in range(active)]
    for connection in range(connections):
        plans[connection % active]["connections"].append(connection)
    for position in range(request_count):
        connection = position % connections
        plan = plans[connection % active]
        plan["positions"].append(position)
        # Connection c is the (c // active)-th of its process's list.
        plan["local"].append(connection // active)
    return plans


def _sender_process(pipe, config):
    """One producer process: connect, wait for the start, send, report."""
    try:
        os.sched_setaffinity(0, set(config["cpus"]))
        grpc = test_e2e.grpc
        pool = CapacityPool(config["pool_dir"], measurement.Workload(**config["workload"]))
        options = [
            ("grpc.use_local_subchannel_pool", 1),
            ("grpc.max_send_message_length", MESSAGE_LIMIT_BYTES),
            ("grpc.max_receive_message_length", MESSAGE_LIMIT_BYTES),
        ]
        channels = [
            grpc.insecure_channel(config["target"], options=options)
            for _ in config["connections"]
        ]
        for channel in channels:
            grpc.channel_ready_future(channel).result(timeout=SENDER_READY_DEADLINE_S)
        calls = [
            {
                signal: channel.unary_unary(
                    method, request_serializer=None,
                    response_deserializer=response.FromString,
                )
                for signal, (method, response, _field) in measurement.EXPORT_METHODS.items()
            }
            for channel in channels
        ]
        rejected_field = {
            signal: field for signal, (_m, _r, field) in measurement.EXPORT_METHODS.items()
        }
        indexes = config["indexes"]
        local = config["local"]
        count = len(indexes)
        target = array.array("q", [0]) * count
        sent = array.array("q", [0]) * count
        finish = array.array("q", [0]) * count
        code = array.array("b", [-1]) * count
        blocked = array.array("b", [0]) * count
        # Whether a send was still behind schedule because an earlier send
        # waited for an in-flight slot: the lateness is the engine's.
        behind = array.array("b", [0]) * count
        outstanding_at_send = array.array("i", [0]) * count
        details = {}
        slots = threading.Semaphore(config["in_flight"])
        state = {"done": 0, "sent": 0}
        lock = threading.Lock()
        finished = threading.Event()
        if count == 0:
            finished.set()

        def done(position, signal, future):
            """Record one response and free its slot."""
            finish[position] = time.monotonic_ns()
            try:
                response = future.result()
                rejected = getattr(response.partial_success, rejected_field[signal], 0)
                code[position] = 1 if rejected else 0
                if rejected:
                    details.setdefault("partial", f"rejected={rejected}")
            except grpc.RpcError as error:
                status = error.code()
                code[position] = 2 if status in test_e2e.RETRYABLE_CODES else 3
                details.setdefault(str(status), str(error.details())[:300])
            except Exception as error:  # noqa: BLE001 - recorded as local
                code[position] = 4
                details.setdefault("local", f"{type(error).__name__}: {error}"[:300])
            slots.release()
            with lock:
                state["done"] += 1
                if state["done"] == count:
                    finished.set()

        # Requests are read ahead of their send, so a send never waits for
        # the pool's pages while the engine's writes keep the disk busy.
        positions = config["positions"]
        gap = (positions[-1] - positions[0]) / (count - 1) if count > 1 else 1
        lookahead = max(4, math.ceil(PREFETCH_AHEAD_S * 1e9 / (gap * config["interval_ns"])))
        for position in range(min(count, lookahead)):
            pool.prefetch(indexes[position])
        pipe.send({"ready": True, "pid": os.getpid(), "connections": len(channels)})
        start_ns = pipe.recv()["start_ns"]
        cpu_before = os.times()
        wake = threading.Event()
        interval_ns = config["interval_ns"]
        late_ns = int(PRODUCER_LATE_S * 1e9)
        catching_up = False
        for position in range(count):
            due = start_ns + config["positions"][position] * interval_ns
            target[position] = due
            now = time.monotonic_ns()
            if due > now:
                _ = wake.wait((due - now) / 1e9)
                catching_up = False
            elif now - due <= late_ns:
                catching_up = False
            behind[position] = 1 if catching_up else 0
            if not slots.acquire(blocking=False):
                blocked[position] = 1
                catching_up = True
                _ = slots.acquire()
            index = indexes[position]
            if position + lookahead < count:
                pool.prefetch(indexes[position + lookahead])
            signal, wire = pool.wire(index)
            body = bytes(wire)
            with lock:
                outstanding_at_send[position] = state["sent"] - state["done"]
                state["sent"] += 1
            sent[position] = time.monotonic_ns()
            future = calls[local[position]][signal].future(body, timeout=config["timeout_s"])
            future.add_done_callback(functools.partial(done, position, signal))
        schedule_end = time.monotonic_ns()
        _ = finished.wait(config["timeout_s"] + 60)
        cpu_after = os.times()
        result = {
            "pid": os.getpid(),
            "count": count,
            "indexes": list(indexes),
            "target": list(target),
            "sent": list(sent),
            "finish": list(finish),
            "code": list(code),
            "blocked": list(blocked),
            "behind": list(behind),
            "outstanding_at_send": list(outstanding_at_send),
            "details": details,
            "schedule_end_ns": schedule_end,
            "complete": finished.is_set(),
            "cpu_user_s": cpu_after.user - cpu_before.user,
            "cpu_system_s": cpu_after.system - cpu_before.system,
            "connections": len(channels),
            "in_flight": config["in_flight"],
        }
        Path(config["result_path"]).write_text(json.dumps(result), encoding="ascii")
        for channel in channels:
            channel.close()
        pool.close()
        pipe.send({"done": True})
    except BaseException as error:  # noqa: BLE001 - reported to the parent
        with contextlib.suppress(Exception):
            pipe.send({"error": f"{type(error).__name__}: {error}"})
        raise


class SenderFleet:
    """The producer processes of one trial, started, released and reaped."""

    def __init__(self, *, target, pool, workload, indexes, rate, connections,
                 processes, in_flight, cpus, directory, timeout_s=PRODUCER_TIMEOUT_S):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.indexes = list(indexes)
        self.rate = rate
        self.connections = connections
        self.interval_ns = int(round(workload.records_per_request / rate * 1e9))
        plans = connection_plan(len(self.indexes), connections, processes)
        context = multiprocessing.get_context("spawn")
        self.processes = []
        for number, plan in enumerate(plans):
            share = max(1, math.ceil(in_flight * len(plan["connections"]) / connections))
            parent, child = context.Pipe()
            config = {
                "target": target,
                "cpus": list(cpus),
                "pool_dir": str(pool.directory),
                "workload": pool.workload.as_json(),
                "connections": plan["connections"],
                "positions": plan["positions"],
                "local": plan["local"],
                "indexes": [self.indexes[position] for position in plan["positions"]],
                "interval_ns": self.interval_ns,
                "in_flight": share,
                "timeout_s": timeout_s,
                "result_path": str(self.directory / f"sender-{number}.json"),
            }
            process = context.Process(
                target=_sender_process, args=(child, config), name=f"sender-{number}",
                daemon=True,
            )
            process.start()
            self.processes.append({"process": process, "pipe": parent, "config": config})

    def pids(self) -> list:
        return [entry["process"].pid for entry in self.processes]

    def _receive(self, entry, deadline_s):
        if not entry["pipe"].poll(deadline_s):
            raise AssertionError(f"sender {entry['process'].name} did not answer")
        message = entry["pipe"].recv()
        if "error" in message:
            raise AssertionError(f"sender {entry['process'].name}: {message['error']}")
        return message

    def ready(self):
        """Wait until every process has connected every channel."""
        return [self._receive(entry, SENDER_READY_DEADLINE_S) for entry in self.processes]

    def go(self, start_ns):
        """Release every process at the same monotonic start."""
        for entry in self.processes:
            entry["pipe"].send({"start_ns": start_ns})

    def finish(self, deadline_s) -> dict:
        """Wait for every process and merge what they sent."""
        for entry in self.processes:
            _ = self._receive(entry, deadline_s)
            entry["process"].join(timeout=30)
        merged = collections.defaultdict(list)
        processes = []
        for entry in self.processes:
            part = json.loads(Path(entry["config"]["result_path"]).read_text(encoding="ascii"))
            for key in ("indexes", "target", "sent", "finish", "code", "blocked",
                        "behind", "outstanding_at_send"):
                merged[key].extend(part[key])
            processes.append(
                {key: part[key] for key in ("pid", "count", "complete", "cpu_user_s",
                                            "cpu_system_s", "connections", "in_flight",
                                            "details", "schedule_end_ns")}
            )
        order = sorted(range(len(merged["indexes"])), key=lambda k: merged["indexes"][k])
        sends = {key: [values[k] for k in order] for key, values in merged.items()}
        sends["processes"] = processes
        return sends

    def close(self):
        for entry in self.processes:
            process = entry["process"]
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
            entry["pipe"].close()


def send_statistics(sends, *, window, rate, records_per_request, wire_bytes_of) -> dict:
    """What the producer offered, sent and was told, per phase."""
    count = len(sends["indexes"])
    lateness = [
        (sends["sent"][k] - sends["target"][k]) / 1e9 for k in range(count)
    ]
    blocked = sum(sends["blocked"])
    behind = sends.get("behind") or [0] * count
    late_behind = sum(
        1 for k in range(count)
        if not sends["blocked"][k] and behind[k] and lateness[k] > PRODUCER_LATE_S
    )
    late_unblocked = sum(
        1 for k in range(count)
        if not sends["blocked"][k] and not behind[k] and lateness[k] > PRODUCER_LATE_S
    )
    outcomes = collections.Counter(OUTCOME_CODES.get(code, "unanswered") for code in sends["code"])
    latencies = sorted(
        (sends["finish"][k] - sends["sent"][k]) / 1e9
        for k in range(count) if sends["code"][k] == 0
    )
    start_ns, end_ns = window
    in_window = [k for k in range(count) if start_ns <= sends["sent"][k] < end_ns]
    return {
        "requests_scheduled_count": count,
        "outcomes": dict(outcomes),
        "blocked_on_in_flight_count": blocked,
        "late_behind_in_flight_wait_count": late_behind,
        "blocked_or_behind_ratio": (blocked + late_behind) / count if count else 0.0,
        "late_unblocked_count": late_unblocked,
        "late_unblocked_ratio": late_unblocked / count if count else 0.0,
        "lateness_p50_s": measurement.percentile(sorted(lateness), 0.50) if count else None,
        "lateness_p99_s": measurement.percentile(sorted(lateness), 0.99) if count else None,
        "lateness_max_s": max(lateness) if count else None,
        "outstanding_at_send_max_count": max(sends["outstanding_at_send"], default=0),
        "outstanding_at_send_p50_count": measurement.percentile(
            sorted(sends["outstanding_at_send"]), 0.50
        ) if count else None,
        "ack_latency_p50_s": measurement.percentile(latencies, 0.50) if latencies else None,
        "ack_latency_p95_s": measurement.percentile(latencies, 0.95) if latencies else None,
        "ack_latency_p99_s": measurement.percentile(latencies, 0.99) if latencies else None,
        "ack_latency_max_s": latencies[-1] if latencies else None,
        "sent_in_window_count": len(in_window),
        "wire_bytes_in_window": sum(wire_bytes_of(sends["indexes"][k]) for k in in_window),
        "wire_bytes_total": sum(wire_bytes_of(index) for index in sends["indexes"]),
        "offered_records_per_s": rate,
    }


def phase_averaged_rate(acked_between, *, start_ns, end_ns, period_s, steps=20) -> float:
    """Acknowledgements per second over an interval, averaged over one window.

    A strict engine acknowledges each window's requests together when its
    block is durable, so a count over a fixed interval holds one window
    more or less depending on where the flushes fell. Shifting the interval
    back through one whole window and averaging removes that phase.
    """
    span_s = (end_ns - start_ns) / 1e9
    total = 0.0
    for step in range(steps):
        shift = int(period_s * 1e9 * step / steps)
        total += acked_between(start_ns - shift, end_ns - shift)
    return total / steps / span_s


def durable_series(sends, records_per_request, *, start_ns, end_ns, step_s=1.0):
    """(seconds since start, backlog records) points and durable counts.

    The backlog at a time is what was due by then minus what was durably
    acknowledged by then, so a producer that fell behind shows up in it as
    much as an engine that did. Points are one window apart, at the same
    phase of each window.
    """
    due = sorted(sends["target"])
    acked = sorted(
        sends["finish"][k] for k in range(len(sends["code"])) if sends["code"][k] == 0
    )
    points = []
    t = start_ns
    step = int(step_s * 1e9)
    while t <= end_ns:
        backlog = (bisect.bisect_right(due, t) - bisect.bisect_right(acked, t)) * records_per_request
        points.append(((t - start_ns) / 1e9, backlog))
        t += step
    acked_between = lambda a, b: (bisect.bisect_left(acked, b) - bisect.bisect_left(acked, a))
    return points, acked_between


# --------------------------------------------------------------------------
# Telemetry beyond the drain gauges
# --------------------------------------------------------------------------

EXPORTER = "exporter.series_parquet"
# Unlabelled exporter metrics kept per worker in every sample.
EXTRA_GAUGES = (
    "acks", "admission.closed", "admission.closed.duration", "admission.closures",
    "block.active", "block.flushing", "flush.workspace", "memory.accounted",
    "memory.budget", "series_cache.entries", "series_cache.hits",
    "series_cache.misses", "series_cache.evictions", "flush.retries",
    "flush.failures",
)
# Labelled metrics kept per worker, summed over the named label's values.
EXTRA_LABELLED = {
    "rows.written": (EXPORTER, "dataset"),
    "files.written": (EXPORTER, "dataset"),
    "series.emitted": (EXPORTER, "reason"),
    "flushes": (EXPORTER, "reason"),
    "nacks": (EXPORTER, "error.type"),
    "accepted": ("receiver.otlp.requests", "protocol"),
    "rejected": ("receiver.otlp.requests", "error.type"),
}


def telemetry_extras(document) -> dict:
    """Per-worker counters and gauges a capacity trial reads, by worker key."""
    parsed = measurement.parse_telemetry(document, expected_workers=None)
    extras = {}
    for key, worker in parsed["workers"].items():
        entry = {}
        for name in EXTRA_GAUGES:
            entities = worker["metrics"].get(f"{EXPORTER}:{name}") or {}
            values = [value for value in entities.values() if isinstance(value, (int, float))]
            if values:
                entry[name] = values[0]
        # The flush wall-time distribution, cumulative over the lifetime.
        flush = next(iter((worker["metrics"].get(performance.FLUSH_METRIC) or {}).values()),
                     None)
        if isinstance(flush, dict):
            entry["flush.duration"] = {
                "sum": float(flush.get("sum") or 0.0), "count": int(flush.get("count") or 0),
                "max": float(flush.get("max") or 0.0),
            }
        for name, (metric_set, label) in EXTRA_LABELLED.items():
            entities = worker["labelled"].get(f"{metric_set}:{name}") or {}
            totals = collections.Counter()
            for parts in entities.values():
                for label_key, value in parts.items():
                    labels = dict(item.split("=", 1) for item in label_key.split(","))
                    if isinstance(value, (int, float)):
                        totals[labels.get(label, "")] += value
            if totals:
                entry[name] = dict(totals)
        extras[key] = entry
    return extras


class CapacitySampler(measurement.Sampler):
    """The harness sampler, keeping the capacity counters of each sample."""

    def once(self):
        monotonic_ns = time.monotonic_ns()
        document = test_e2e.engine_metrics(self.engine)
        parsed = measurement.sample_document(
            document, expected_workers=self.expected_workers, buffered=self.buffered
        )
        pid = self.engine.pid
        sample = {
            "monotonic_ns": monotonic_ns,
            "observed_utc": measurement.utc_now(),
            "scrape_timestamp": document.get("timestamp"),
            "workers": parsed["workers"],
            "process": parsed["process"],
            "process_rss_bytes": test_e2e.rss_bytes(pid),
            "process_pid": pid,
        }
        sample["procfs"] = measurement.procfs_process_sample(pid)
        sample["load_average_1_5_15"] = measurement.load_average()
        sample["phase"] = self.phase
        sample["extras"] = telemetry_extras(document)
        if self.pairs:
            sample["allocator_latest"] = dict(self.pairs[-1])
        return sample

    def watch_allocator(self, log_path):
        """Pair every jemalloc statistics print with the RSS read after it.

        The engine prints its allocator totals into its log after every
        `JEMALLOC_STATS_INTERVAL_BYTES` of allocation. A thread of its own
        reads the log every `ALLOCATOR_TAIL_S` and, on each new print, reads
        the process's smaps rollup at once, so the allocator's resident total
        and the RSS it is compared with are milliseconds apart.
        """
        try:
            from . import memory
        except ImportError:
            import memory
        self.allocator = memory.JemallocStats(log_path)
        self.pairs = []
        self._tail = threading.Thread(target=self._follow, name="allocator-tail",
                                      daemon=True)
        return self

    def _follow(self):
        while not self._stop.is_set():
            try:
                prints = self.allocator.poll()
                if prints:
                    procfs = measurement.procfs_process_sample(self.engine.pid)
                    latest = prints[-1]
                    self.pairs.append({
                        "monotonic_ns": procfs["monotonic_ns"],
                        "procfs": {key: procfs[key] for key in (
                            "smaps_rss_bytes", "smaps_anonymous_bytes") if key in procfs},
                        "jemalloc_resident_bytes": latest["resident_bytes"],
                        "jemalloc_allocated_bytes": latest["allocated_bytes"],
                        "jemalloc_allocated_peak_bytes": max(
                            entry["allocated_bytes"] for entry in prints),
                        "jemalloc_metadata_bytes": latest["metadata_bytes"],
                        "prints_count": len(prints),
                    })
            except Exception as error:  # noqa: BLE001 - recorded, never a zero
                self.errors.append({"monotonic_ns": time.monotonic_ns(),
                                    "error": f"allocator tail: {error}"[:500]})
            _ = self._stop.wait(ALLOCATOR_TAIL_S)

    def start(self):
        super().start()
        if self._tail is not None:
            self._tail.start()
        return self

    def stop(self):
        super().stop()
        if self._tail is not None and self._tail.is_alive():
            self._tail.join(timeout=30)
        return self

    allocator = None
    pairs = ()
    _tail = None


def extras_total(sample, name, label=None) -> float:
    """One extra summed over workers, or over one label value of it."""
    total = 0.0
    for entry in sample.get("extras", {}).values():
        value = entry.get(name)
        if isinstance(value, dict):
            total += value.get(label, 0.0) if label is not None else sum(value.values())
        elif isinstance(value, (int, float)):
            total += value
    return total


def value_at(samples, t_ns, name, label=None):
    """An extra's total interpolated at one monotonic time."""
    before = [s for s in samples if s["monotonic_ns"] <= t_ns and "extras" in s]
    after = [s for s in samples if s["monotonic_ns"] >= t_ns and "extras" in s]
    if not before or not after:
        return None
    a, b = before[-1], after[0]
    va, vb = extras_total(a, name, label), extras_total(b, name, label)
    if b["monotonic_ns"] == a["monotonic_ns"]:
        return va
    share = (t_ns - a["monotonic_ns"]) / (b["monotonic_ns"] - a["monotonic_ns"])
    return va + (vb - va) * share


# --------------------------------------------------------------------------
# The trial ledger
# --------------------------------------------------------------------------


class TrialLedger:
    """The delivery oracle's ledger, reused by every trial of one workload.

    The records of the pool's prefix are inserted once and kept; each trial
    replaces the request and attempt tables with what it sent, so the
    oracle's acknowledged scope is exactly the trial's. Stored rows of
    requests the trial never sent are counted separately.
    """

    def __init__(self, path, pool, first_index):
        self.ledger = measurement.Ledger(path)
        self.pool = pool
        self.first_index = first_index
        with self.ledger.lock:
            self.ledger.connection.execute(
                "CREATE TABLE IF NOT EXISTS prefix (end_index INTEGER NOT NULL)"
            )
            row = self.ledger.connection.execute("SELECT max(end_index) FROM prefix").fetchone()
        self.end_index = row[0] if row and row[0] is not None else first_index

    def extend(self, end_index) -> float:
        """Insert every record of requests below `end_index` not yet present."""
        if end_index <= self.end_index:
            return 0.0
        started = time.monotonic_ns()
        connection = self.ledger.connection
        with self.ledger.lock:
            _ = connection.execute("BEGIN IMMEDIATE")
            try:
                for index in range(self.end_index, end_index):
                    _ = connection.executemany(
                        "INSERT INTO records (record_id, request_id, kind, expected_sha256) "
                        "VALUES (?, ?, ?, ?)",
                        [(record_id, index, kind, digest)
                         for record_id, kind, digest in self.pool.records(index)],
                    )
                _ = connection.execute("INSERT INTO prefix VALUES (?)", (end_index,))
                _ = connection.execute("COMMIT")
            except BaseException:
                _ = connection.execute("ROLLBACK")
                raise
        self.end_index = end_index
        return (time.monotonic_ns() - started) / 1e9

    def load(self, sends):
        """Replace the request and attempt tables with one trial's sends."""
        connection = self.ledger.connection
        rows = []
        attempts = []
        for k, index in enumerate(sends["indexes"]):
            _offset, _length, signal, digest = self.pool.entry(index)
            code = sends["code"][k]
            ack = sends["finish"][k] if code == 0 else None
            rows.append((index, signal, digest.hex(), sends["sent"][k], ack))
            attempts.append(
                (index, 1, sends["sent"][k], sends["finish"][k] or sends["sent"][k],
                 OUTCOME_CODES.get(code, measurement.OUTCOME_LOCAL), "")
            )
        with self.ledger.lock:
            _ = connection.execute("BEGIN IMMEDIATE")
            try:
                _ = connection.execute("DELETE FROM requests")
                _ = connection.execute("DELETE FROM attempts")
                _ = connection.execute("DROP TABLE IF EXISTS actual")
                _ = connection.executemany(
                    "INSERT INTO requests (request_id, signal, wire_sha256, first_send_ns, "
                    "ack_ns) VALUES (?, ?, ?, ?, ?)", rows,
                )
                _ = connection.executemany(
                    "INSERT INTO attempts (request_id, ordinal, start_ns, finish_ns, outcome, "
                    "detail) VALUES (?, ?, ?, ?, ?, ?)", attempts,
                )
                _ = connection.execute("COMMIT")
            except BaseException:
                _ = connection.execute("ROLLBACK")
                raise

    def stored_from_unsent(self) -> int:
        """Stored rows whose request this trial never sent."""
        with self.ledger.lock:
            return self.ledger.connection.execute(
                "SELECT count(*) FROM actual a JOIN records r ON r.record_id = a.record_id "
                "WHERE NOT EXISTS (SELECT 1 FROM requests q WHERE q.request_id = r.request_id)"
            ).fetchone()[0]

    def close(self):
        self.ledger.close()


# --------------------------------------------------------------------------
# One trial
# --------------------------------------------------------------------------


def whole_core_roles(allocation, sibling_groups) -> dict:
    """The allocation with the producer and store owning their SMT siblings.

    Both are roles of whole physical cores; the siblings of their cores are
    given to nobody else, so giving them to the role itself keeps every
    other role's core untouched.
    """
    group_of = {}
    for group in sibling_groups:
        for core in group:
            group_of[int(core)] = [int(item) for item in group]
    available = set(os.sched_getaffinity(0))
    widened = dict(allocation)
    for role in ("producer", "store"):
        if role in allocation:
            cores = set()
            for core in allocation[role]:
                cores |= set(group_of.get(core, [core])) & available
            widened[role] = sorted(cores)
    return widened


def object_inventory(plan, store, data_dir) -> list:
    """(key, bytes, completion ns since epoch) of every completed object."""
    found = []
    if store is None:
        for path in sorted(Path(data_dir).rglob("*.parquet")):
            status = path.stat()
            found.append((str(path.relative_to(data_dir)), status.st_size, status.st_mtime_ns))
        return found
    for page in store.client.get_paginator("list_objects_v2").paginate(
        Bucket=store.bucket, Prefix="otel/"
    ):
        for item in page.get("Contents", []):
            found.append(
                (item["Key"].removeprefix("otel/"), int(item["Size"]),
                 int(item["LastModified"].timestamp() * 1e9))
            )
    return found


def incomplete_uploads(store) -> dict:
    """Multipart uploads the store still holds open, and their part bytes."""
    if store is None:
        return {"count": 0, "part_bytes": 0, "exposed": False}
    uploads = store.client.list_multipart_uploads(Bucket=store.bucket).get("Uploads", [])
    part_bytes = 0
    for upload in uploads:
        parts = store.client.list_parts(
            Bucket=store.bucket, Key=upload["Key"], UploadId=upload["UploadId"]
        ).get("Parts", [])
        part_bytes += sum(int(part["Size"]) for part in parts)
    return {"count": len(uploads), "part_bytes": part_bytes, "exposed": True}


def descriptor_duplication(root) -> dict:
    """Series ids whose descriptor more than one worker wrote, and bytes by dataset."""
    import duckdb

    root = Path(root)
    report = {}
    by_dataset = collections.Counter()
    for path in root.rglob("*.parquet"):
        by_dataset[path.parent.parent.parent.name] += path.stat().st_size
    report["object_bytes_by_dataset"] = dict(by_dataset)
    glob = root / "v=1/signal=*/dataset=series/**/*.parquet"
    if not any(root.glob("v=1/signal=*/dataset=series/**/*.parquet")):
        return dict(report, series_rows_count=0, distinct_series_count=0,
                    series_ids_in_several_workers_count=0)
    with duckdb.connect() as db:
        rows, distinct, several, boots = db.execute(
            "WITH s AS (SELECT series_id, regexp_extract(filename, "
            "'-([0-9a-f]{32})-[0-9]+\\.parquet$', 1) AS boot FROM read_parquet("
            f"{test_e2e.sql_string(glob)}, filename=true, hive_partitioning=false)) "
            "SELECT count(*), count(DISTINCT series_id), "
            "(SELECT count(*) FROM (SELECT series_id FROM s GROUP BY series_id "
            "HAVING count(DISTINCT boot) > 1)), count(DISTINCT boot) FROM s"
        ).fetchone()
    return dict(
        report,
        series_rows_count=int(rows),
        distinct_series_count=int(distinct),
        series_ids_in_several_workers_count=int(several),
        writer_boot_ids_count=int(boots),
    )


def jemalloc_peak(log_path) -> dict:
    """The allocator totals the engine printed, at their allocated peak."""
    try:
        from . import memory
    except ImportError:
        import memory
    stats = memory.JemallocStats(log_path).poll()
    if not stats:
        return {"prints_count": 0}
    peak = max(stats, key=lambda entry: entry["allocated_bytes"])
    return dict(peak, prints_count=len(stats))


def trial_residuals(samples, idle, pairs=()) -> tuple:
    """The RSS residuals of one trial and the heap term they used.

    With allocator prints the ledger is the prints, each paired with the RSS
    read right after it (`CapacitySampler.watch_allocator`), and the first
    pair is the reference: `measurement.allocator_band_residuals`. A trial
    without prints falls back to the pipeline counter from the idle
    reference.
    """
    if idle is None:
        return [], None
    pairs = [pair for pair in pairs if "smaps_rss_bytes" in pair["procfs"]]
    if pairs:
        high = 0
        for pair in pairs:
            high = max(high, pair["jemalloc_allocated_peak_bytes"])
            pair["allocated_high_water_bytes"] = high
        return measurement.allocator_band_residuals(pairs), {
            "source": "jemalloc_band",
            "reference_monotonic_ns": pairs[0]["monotonic_ns"],
            "pairs_count": len(pairs),
            "stats_interval_bytes": JEMALLOC_STATS_INTERVAL_BYTES,
            "tail_period_s": ALLOCATOR_TAIL_S,
            "retention_peak_bytes": max(
                pair["jemalloc_resident_bytes"] - pair["jemalloc_allocated_bytes"]
                for pair in pairs),
            "allocated_peak_bytes": high,
            "resident_peak_bytes": max(pair["jemalloc_resident_bytes"] for pair in pairs),
        }
    return measurement.rss_residuals(samples, idle), {"source": "pipeline_memory_usage"}


def degradation(samples, window, interval_s, schedule) -> dict:
    """What each worker did over the measured interval, from its telemetry.

    Per worker: requests accepted, flushes and their mean and cumulative
    maximum wall time against the window, flushes by rotation reason, the
    time admission was closed (a rotation waits while the previous block
    still flushes) and its closures, the exporter's nacks by refusal class
    and the receiver's refusals by class, with the worker's time on CPU.
    """
    usable = [s for s in samples if s.get("extras")]
    if not usable:
        return {}
    first = max((s for s in usable if s["monotonic_ns"] <= window[0]),
                key=lambda s: s["monotonic_ns"], default=usable[0])
    last = min((s for s in usable if s["monotonic_ns"] >= window[1]),
               key=lambda s: s["monotonic_ns"], default=usable[-1])
    on_cpu = sorted(w["on_cpu_ratio"] for w in (schedule.get("workers") or {}).values())

    def delta(key, name):
        a = (first["extras"].get(key) or {}).get(name)
        b = (last["extras"].get(key) or {}).get(name)
        if isinstance(b, dict):
            a = a or {}
            return {label: b[label] - a.get(label, 0) for label in b if b[label] - a.get(label, 0)}
        return (b or 0) - (a or 0)

    workers = {}
    for key in sorted(last["extras"]):
        before = (first["extras"].get(key) or {}).get("flush.duration") or {}
        after = (last["extras"].get(key) or {}).get("flush.duration") or {}
        count = after.get("count", 0) - before.get("count", 0)
        wall = after.get("sum", 0.0) - before.get("sum", 0.0)
        workers[key] = {
            "requests_accepted_count": sum((delta(key, "accepted") or {}).values()),
            "flush_count": count,
            "flush_mean_s": wall / count if count else None,
            "flush_mean_to_window_ratio": (wall / count / interval_s) if count else None,
            "flush_max_lifetime_s": after.get("max"),
            "flushes_by_reason": delta(key, "flushes"),
            "admission_closed_s": delta(key, "admission.closed.duration"),
            "admission_closures_count": delta(key, "admission.closures"),
            "exporter_nacks_by_class": delta(key, "nacks"),
            "receiver_refusals_by_class": delta(key, "rejected"),
        }
    accepted = [w["requests_accepted_count"] for w in workers.values()]
    mean = sum(accepted) / len(accepted) if accepted else 0
    return {
        "workers": workers,
        "accepted_max_to_mean_ratio": max(accepted) / mean if mean else None,
        "worker_on_cpu_ratios": on_cpu,
    }


def accounted_against_allocated(samples, pairs) -> dict:
    """The exporter's accounted bytes against jemalloc's live heap.

    Each allocator pair is matched with the telemetry sample nearest to it
    in time. The difference `allocated - accounted` is the heap the exporter
    does not account for; its least-squares slope against the values rows
    written says whether it grows with the records a trial wrote, which a
    heap leak inside the exporter would show and the RSS band cannot, since
    such a leak lives inside `allocated`.
    """
    usable = [s for s in samples if s.get("extras")]
    if not usable or not pairs:
        return {}
    times = [s["monotonic_ns"] for s in usable]
    points = []
    for pair in pairs:
        at = bisect.bisect_left(times, pair["monotonic_ns"])
        near = min((i for i in (at - 1, at) if 0 <= i < len(usable)),
                   key=lambda i: abs(times[i] - pair["monotonic_ns"]))
        sample = usable[near]
        accounted = extras_total(sample, "memory.accounted")
        written = extras_total(sample, "rows.written", "values")
        fill = extras_total(sample, "block.active") + extras_total(sample, "block.flushing")
        points.append((written, pair["jemalloc_allocated_bytes"], accounted, fill))
    differences = sorted(allocated - accounted for _w, allocated, accounted, _f in points)
    fill_point = max(points, key=lambda point: point[3])
    slope = backlog_slope([(written, allocated - accounted)
                           for written, allocated, accounted, _f in points])
    written_total = max(point[0] for point in points)
    return {
        "at_highest_fill": {
            "block_fill_bytes": fill_point[3],
            "accounted_bytes": fill_point[2],
            "allocated_bytes": fill_point[1],
            "difference_bytes": fill_point[1] - fill_point[2],
        },
        "difference_p50_bytes": differences[len(differences) // 2],
        "difference_max_bytes": differences[-1],
        "difference_min_bytes": differences[0],
        "difference_slope_bytes_per_record": slope,
        "difference_growth_over_run_bytes": slope * written_total,
        "values_rows_written_count": written_total,
        "points_count": len(points),
    }


def memory_at_fill(samples, jemalloc) -> dict:
    """RSS, accounted and budget at the trial's highest block fill."""
    usable = [s for s in samples if s.get("extras")]
    if not usable:
        return {}
    fill = max(usable, key=lambda s: extras_total(s, "block.active") + extras_total(s, "block.flushing"))
    rss = max(usable, key=lambda s: s["procfs"].get("smaps_rss_bytes") or s["process_rss_bytes"])
    budget = extras_total(fill, "memory.budget")
    accounted_peak = max(extras_total(s, "memory.accounted") for s in usable)
    report = {
        "block_fill_peak_bytes": extras_total(fill, "block.active") + extras_total(fill, "block.flushing"),
        "accounted_at_fill_bytes": extras_total(fill, "memory.accounted"),
        "rss_at_fill_bytes": fill["procfs"].get("smaps_rss_bytes"),
        "anonymous_at_fill_bytes": fill["procfs"].get("smaps_anonymous_bytes"),
        "flush_workspace_at_fill_bytes": extras_total(fill, "flush.workspace"),
        "accounted_peak_bytes": accounted_peak,
        "rss_peak_bytes": rss["procfs"].get("smaps_rss_bytes"),
        "anonymous_at_rss_peak_bytes": rss["procfs"].get("smaps_anonymous_bytes"),
        "budget_bytes": budget,
        "accounted_to_budget_ratio": accounted_peak / budget if budget else None,
        "flush_workspace_peak_bytes": max(extras_total(s, "flush.workspace") for s in usable),
    }
    if jemalloc.get("prints_count"):
        peak_rss = report["rss_peak_bytes"] or 0
        report["jemalloc_at_allocated_peak"] = jemalloc
        report["split_at_peaks"] = {
            "rss_peak_bytes": peak_rss,
            "non_heap_bytes": peak_rss - jemalloc["resident_bytes"],
            "allocator_retention_bytes": jemalloc["resident_bytes"] - jemalloc["allocated_bytes"],
            "jemalloc_allocated_bytes": jemalloc["allocated_bytes"],
            "accounted_peak_bytes": accounted_peak,
            "allocated_beyond_accounted_bytes": jemalloc["allocated_bytes"] - accounted_peak,
            "note": "the RSS and allocator peaks come from different instants; each "
            "term is a peak, so the terms bound rather than add up",
        }
    return report


class CapacityPhase:
    """`measure.EnginePhase` with the capacity sampler."""

    def __new__(cls, command, *args, **kwargs):
        phase = command.EnginePhase(*args, **kwargs)
        original = phase.ready

        def ready(edge):
            snapshot = original(edge)
            phase.sampler.stop()
            phase.sampler = CapacitySampler(
                phase.engine,
                expected_workers=len(phase.spec.cores),
                buffered=phase.buffered,
                period_s=command.SAMPLE_PERIOD_S,
                phase=phase.label,
            ).watch_allocator(Path(phase.engine.log.name))
            phase.sampler.start()
            # The residual is the trial's own (`trial_residuals`); the phase
            # summary computes none, so it never mixes two heap terms.
            phase.capacity_idle, phase.idle = phase.idle, None
            return snapshot

        phase.ready = ready
        return phase


class NoopPhase:
    """The engine phase of a calibration against the noop exporter.

    The noop exporter publishes no exporter metric set, so a worker is
    observed through its own pipeline uptime; there is no block to drain,
    and the phase ends once every worker answered three more collections.
    """

    def __init__(self, engine, spec, controls, period_s):
        self.engine = engine
        self.spec = spec
        self.controls = controls
        self.period_s = period_s
        self.label = "calibration"
        self.workers = None
        self.drain = None
        self.errors = []
        self.sampler = self
        self.samples = []
        self._stop = threading.Event()
        self._thread = None

    def once(self):
        document = test_e2e.engine_metrics(self.engine)
        parsed = measurement.parse_telemetry(
            document, expected_workers=len(self.spec.cores)
        )
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
            "monotonic_ns": time.monotonic_ns(),
            "observed_utc": measurement.utc_now(),
            "workers": workers,
            "process_pid": self.engine.pid,
            "process_rss_bytes": test_e2e.rss_bytes(self.engine.pid),
            "procfs": measurement.procfs_process_sample(self.engine.pid),
            "load_average_1_5_15": measurement.load_average(),
            "extras": telemetry_extras(document),
        }

    def _run(self):
        while not self._stop.is_set():
            try:
                self.samples.append(self.once())
            except Exception as error:  # noqa: BLE001 - recorded, never a zero
                self.errors.append({"monotonic_ns": time.monotonic_ns(),
                                    "error": f"{type(error).__name__}: {error}"[:500]})
            _ = self._stop.wait(self.period_s)

    def start(self):
        self._thread = threading.Thread(target=self._run, name="noop-sampler", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
            self._thread = None

    def ready(self, edge):
        sample = measurement.wait_until(
            self.once,
            lambda s: len(s["workers"]) == len(self.spec.cores)
            and all(w["uptime_s"] > 0 for w in s["workers"].values()),
            deadline_ns=time.monotonic_ns() + 60 * 10**9,
            description="every noop worker answering a collection",
        )
        self.workers = measurement.worker_identities(sample)
        snapshot = self.controls.snapshot(
            edge, {"engine": (self.engine.pid, list(self.spec.cores))},
            workers=self.workers, requested_cores=list(self.spec.cores),
        )
        self.controls.watch_workers(self.engine.pid, snapshot)
        self.start()
        return snapshot

    def drained(self):
        started = time.monotonic_ns()
        baseline = {key: w["uptime_s"] for key, w in self.once()["workers"].items()}
        tracker = {"count": 0}

        def advanced():
            current = self.once()["workers"]
            return min(
                (current[key]["uptime_s"] - baseline[key]) for key in baseline
            )

        _ = measurement.wait_until(
            advanced, lambda value: value >= 3 * 1.0,
            deadline_ns=started + 120 * 10**9,
            description="three more collections of every noop worker",
        )
        self.stop()
        self.drain = {"drained": True, "duration_s": (time.monotonic_ns() - started) / 1e9,
                      "tracker": tracker}
        return self.drain

    def summary(self):
        command = _command()
        return {
            "label": self.label,
            "pid": self.engine.pid,
            "workers": self.workers,
            "drain": self.drain,
            "sample_count": len(self.samples),
            "sampler_errors": self.errors,
            "answered_epochs_by_worker": {
                key: len({s["workers"][key]["uptime_s"] for s in self.samples
                          if key in s["workers"]})
                for key in (self.samples[-1]["workers"] if self.samples else {})
            },
            "stale_gap_s_by_worker": command.stale_gaps(self.samples),
            "residuals": [],
        }


@contextlib.contextmanager
def malloc_conf(value):
    """`MALLOC_CONF` for the engine launched inside, and only for it."""
    previous = os.environ.get("MALLOC_CONF")
    if value:
        os.environ["MALLOC_CONF"] = value
    try:
        yield
    finally:
        if value:
            if previous is None:
                os.environ.pop("MALLOC_CONF", None)
            else:
                os.environ["MALLOC_CONF"] = previous


def _command():
    try:
        from . import measure as command
    except ImportError:
        import measure as command
    return command


def trial_spec(plan, trial) -> measurement.RunSpec:
    """The immutable inputs of one trial."""
    config = CAPACITY_WORKLOADS[trial["workload_id"]]
    count = trial_requests(
        trial["rate"], trial["warmup_s"] + trial["measure_s"],
        config["workload"].records_per_request,
    )
    workload = dataclasses.replace(
        config["workload"], requests=config["first_index"] + count
    )
    cores = tuple(plan["cores"])
    capacity = trial["receiver_capacity"]
    total_in_flight = capacity * len(cores)
    return measurement.RunSpec(
        run_id=measurement.RunSpec.build_run_id(
            f"capacity-{trial['workload_id']}", trial["topology"], plan["store_kind"],
            cores, trial["interval_s"], trial["ordinal"],
        ),
        case=f"capacity-{trial['workload_id']}",
        topology=trial["topology"],
        store=plan["store_kind"],
        cores=cores,
        workload=workload,
        interval_s=trial["interval_s"],
        duration_s=trial["warmup_s"] + trial["measure_s"],
        producer_timeout_s=PRODUCER_TIMEOUT_S,
        max_in_flight=total_in_flight,
        overrides={
            "receiver": {"max_concurrent_requests": total_in_flight},
            "receiver_per_worker_max_concurrent_requests": capacity,
            "pdata_channel_capacity": capacity,
            "rate_requests_per_s": trial["rate"] / workload.records_per_request,
            "offered_records_per_s": trial["rate"],
            "connections": trial["connections"],
            "producer_processes": plan["producer_processes"],
            "warmup_s": trial["warmup_s"],
            "measure_s": trial["measure_s"],
            "upload_concurrency": trial["upload_concurrency"],
            "purpose": trial["purpose"],
            "first_index": config["first_index"],
        },
    )


def engine_settings(plan, trial, root):
    """The engine constructor arguments of one trial."""
    capacity = trial["receiver_capacity"]
    merge = {
        "receiver": {"protocols": {"grpc": {"max_concurrent_requests": capacity}}},
        "pipeline_policies": {"channel_capacity": {"pdata": capacity}},
    }
    overrides = None
    if trial["topology"] != "noop":
        overrides = {
            "upload": {"part_bytes": "8MiB", "concurrency": trial["upload_concurrency"],
                       "abort_timeout": "5s"},
        }
        if plan["store"] is not None:
            overrides["retry"] = test_e2e.S3_RETRY
    return {
        "storage": dict(plan["store"].storage) if plan["store"] is not None else None,
        "overrides": overrides,
        "interval": f"{trial['interval_s']}s",
        "cores": list(plan["cores"]),
        "merge": merge,
        "binary": Path(plan["provenance"]["build"]["binary"]),
        "topology": trial["topology"],
        "buffer_path": (root / "buffer") if trial["topology"] == "buffered" else None,
    }


def trial_experiment(plan, trial, spec, result, run_dir, controls):
    """One trial: engine, producer fleet, measured interval, drain, oracle."""
    command = _command()
    run_dir = Path(run_dir)
    controls.coverage_gaps_hard = True
    controls.allocate(plan["allocation"])
    controls.register("harness", os.getpid())
    if plan.get("store_pid"):
        controls.register("store", plan["store_pid"], plan["allocation"].get("store", []))
    result["environment"]["build"] = plan["provenance"]["build"]
    result["environment"]["git"] = plan["provenance"]["git"]
    result["environment"]["ledger_filesystem"] = plan["ledger_filesystem"]
    result["ephemeral_values"] = dict(plan["ephemeral"])
    pool = plan["pools"][trial["workload_id"]]
    workload = spec.workload
    first = CAPACITY_WORKLOADS[trial["workload_id"]]["first_index"]
    indexes = list(range(first, workload.requests))
    root = run_dir / "engine"
    root.mkdir(parents=True, exist_ok=True)
    settings = engine_settings(plan, trial, root)
    buffered = trial["topology"] == "buffered"
    with malloc_conf(JEMALLOC_STATS_CONF if trial["jemalloc_stats"] else None):
        engine = test_e2e.Engine(root, **settings)
    phase = None
    fleet = None
    observed = {}
    try:
        result["config"]["effective"] = engine.config
        result["config"]["effective_sha256"] = engine.config_sha256
        result["config"]["edges"] = [list(edge) for edge in engine.edges]
        result["config"]["malloc_conf"] = JEMALLOC_STATS_CONF if trial["jemalloc_stats"] else None
        result["ephemeral_values"]["<receiver_listening_addr>"] = f"127.0.0.1:{engine.grpc_port}"
        command.record_graph(result, engine, trial["topology"])
        fleet = SenderFleet(
            target=f"127.0.0.1:{engine.grpc_port}", pool=pool, workload=workload,
            indexes=indexes, rate=trial["rate"], connections=trial["connections"],
            processes=plan["producer_processes"], in_flight=spec.max_in_flight,
            cpus=plan["allocation"]["producer"], directory=run_dir / "senders",
        )
        observed["senders_ready"] = fleet.ready()
        for number, pid in enumerate(fleet.pids()):
            controls.register(f"producer_{number}", pid, plan["allocation"]["producer"])
        if trial["topology"] == "noop":
            phase = NoopPhase(engine, spec, controls, command.SAMPLE_PERIOD_S)
        else:
            phase = CapacityPhase(command, "trial", engine, spec, controls, buffered)
        snapshot = phase.ready("start")
        worker_tids = set(measurement.worker_tids(snapshot))
        observed["connections_established_count"] = established_connections(engine.grpc_port)
        _ = command.await_window_start(spec.interval_s)
        controls.raise_if_invalid()
        start_ns = time.monotonic_ns() + 200_000_000
        window = (start_ns + int(trial["warmup_s"] * 1e9),
                  start_ns + int((trial["warmup_s"] + trial["measure_s"]) * 1e9))
        _ = performance.reset_peak_rss(engine.pid)
        fleet.go(start_ns)
        readings = {}
        wake = threading.Event()
        for label, at in (("start", window[0]), ("end", window[1])):
            now = time.monotonic_ns()
            if at > now:
                _ = wake.wait((at - now) / 1e9)
            readings[label] = {
                "monotonic_ns": time.monotonic_ns(),
                "engine_cpu_ns": performance.procfs_cpu_ns(engine.pid),
                "threads": performance.thread_times(engine.pid),
                "store_cpu_ns": (
                    performance.procfs_cpu_ns(plan["store_pid"]) if plan.get("store_pid") else None
                ),
                "producer_cpu_ns": sum(
                    performance.procfs_cpu_ns(pid) for pid in fleet.pids()
                ),
            }
            controls.raise_if_invalid()
        sends = fleet.finish(PRODUCER_TIMEOUT_S + 120)
        observed["last_response_ns"] = max(sends["finish"], default=0)
        controls.raise_if_invalid()
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
        if fleet is not None:
            fleet.close()
        engine.close()
    settle_trial(plan, trial, spec, result, run_dir, phase, sends, readings, window,
                 worker_tids, observed, start_ns, engine)


def established_connections(port) -> dict:
    """The producer's TCP connections to the receiver, from `ss`."""
    done = subprocess.run(
        ["ss", "-tnH", "state", "established", f"( dport = :{port} )"],
        capture_output=True, text=True, timeout=30,
    )
    ports = sorted(
        int(line.split()[2].rsplit(":", 1)[1]) for line in done.stdout.splitlines()
        if line.split()
    ) if done.returncode == 0 else []
    return {"count": len(ports), "client_ports": ports}


def worker_distribution(samples, window) -> dict:
    """Requests each worker accepted in the window, and their spread."""
    start = [s for s in samples if s["monotonic_ns"] <= window[0] and s.get("extras")]
    end = [s for s in samples if s["monotonic_ns"] >= window[1] and s.get("extras")]
    last = [s for s in samples if s.get("extras")]
    if not last:
        return {}
    first_sample = start[-1] if start else last[0]
    end_sample = end[0] if end else last[-1]
    final = last[-1]
    per_worker = {}
    for key, entry in final["extras"].items():
        total = sum((entry.get("accepted") or {}).values())
        begin = sum(((first_sample["extras"].get(key) or {}).get("accepted") or {}).values())
        stop = sum(((end_sample["extras"].get(key) or {}).get("accepted") or {}).values())
        per_worker[key] = {
            "requests_accepted_total_count": total,
            "requests_accepted_in_window_count": stop - begin,
            "series_cache_entries_count": entry.get("series_cache.entries"),
            "series_rows_written_count": (entry.get("rows.written") or {}).get("series"),
            "values_rows_written_count": (entry.get("rows.written") or {}).get("values"),
        }
    totals = [value["requests_accepted_total_count"] for value in per_worker.values()]
    mean = sum(totals) / len(totals) if totals else 0
    return {
        "workers": per_worker,
        "idle_workers_count": sum(1 for total in totals if total == 0),
        "max_to_mean_ratio": max(totals) / mean if mean else None,
        "min_to_mean_ratio": min(totals) / mean if mean else None,
    }


def settle_trial(plan, trial, spec, result, run_dir, phase, sends, readings, window,
                 worker_tids, observed, start_ns, engine):
    """Oracle, metrics, verdict and checks of one trial."""
    command = _command()
    pool = plan["pools"][trial["workload_id"]]
    workload = spec.workload
    rpr = workload.records_per_request
    store = plan["store"]
    samples = list(phase.sampler.samples)
    summary = phase.summary()
    residuals, heap = trial_residuals(
        samples, getattr(phase, "capacity_idle", None) if trial["topology"] != "noop" else None,
        getattr(phase.sampler, "pairs", ()),
    )
    stats = send_statistics(
        sends, window=window, rate=trial["rate"], records_per_request=rpr,
        wire_bytes_of=lambda index: pool.entry(index)[1],
    )
    points, acked_between = durable_series(
        sends, rpr, start_ns=window[0], end_ns=window[1], step_s=trial["interval_s"]
    )
    slope = backlog_slope(points)
    measure_s = (window[1] - window[0]) / 1e9
    tail_s = min(STABILITY_TAIL_S, measure_s)
    tail_start = window[1] - int(tail_s * 1e9)
    tail_durable = rpr * phase_averaged_rate(
        acked_between, start_ns=tail_start, end_ns=window[1], period_s=trial["interval_s"]
    )
    window_durable_records = acked_between(window[0], window[1]) * rpr
    # The exporter's own count of values rows written over the whole
    # interval corroborates the acknowledgements. Its telemetry lags by up
    # to one reporting interval and moves in whole flushes, so it is allowed
    # one window plus one reporting interval of the offered rate.
    stored_window = None
    stored_floor = None
    if trial["topology"] != "noop":
        a = value_at(samples, window[0], "rows.written", "values")
        b = value_at(samples, window[1], "rows.written", "values")
        stored_window = (b - a) if a is not None and b is not None else None
        stored_floor = trial["rate"] * (
            DURABLE_RATIO_FLOOR * measure_s - trial["interval_s"]
            - _command().REPORTING_INTERVAL_S
        )
    stored_rate = stored_window / measure_s if stored_window is not None else None
    failed = sum(1 for code in sends["code"] if code in (2, 3, 4) or code < 0)
    partial = sum(1 for code in sends["code"] if code == 1)
    permanent = sum(1 for code in sends["code"] if code == 3)
    verdict = stability_verdict(
        offered=trial["rate"], tail_durable=tail_durable,
        backlog_slope_records_per_s=slope,
        late_unblocked_ratio=stats["late_unblocked_ratio"],
        failed_requests=failed, partial_requests=partial,
    )
    if stored_window is not None and verdict["verdict"] == "sustainable" and \
            stored_window < stored_floor:
        verdict = {"verdict": "unsustainable", "reasons": [
            f"values rows written over the interval {stored_window:.0f} < "
            f"{stored_floor:.0f}, the offered rate less one window and one "
            f"reporting interval"
        ]}
    # Warm-up: before the window, the engine completed a flush, and every
    # worker that had accepted a request had acknowledged one. A worker the
    # connection hashing gave no request yet has nothing to warm.
    warm = [s for s in samples if s["monotonic_ns"] <= window[0] and s.get("extras")]
    settled = int((trial["interval_s"] + 1) * 1e9)
    early = [s for s in warm if s["monotonic_ns"] <= window[0] - settled]
    busy = {
        key for key, entry in (early[-1]["extras"] if early else {}).items()
        if sum((entry.get("accepted") or {}).values()) > 0
    }
    warm_ok = trial["topology"] == "noop" or bool(warm) and extras_total(
        warm[-1], "acks"
    ) > 0 and all(
        (warm[-1]["extras"].get(key) or {}).get("acks", 0) > 0 for key in busy
    )
    # Store objects, downloaded or local, then the oracle.
    data_dir = Path(engine.data)
    objects = [] if trial["topology"] == "noop" else object_inventory(plan, store, data_dir)
    uploads = incomplete_uploads(store) if trial["topology"] != "noop" else {}
    local = data_dir if store is None else run_dir / "store"
    oracle = None
    duplication = {}
    downloaded_bytes = None
    stored_unsent = None
    ledger_s = None
    try:
        if trial["topology"] != "noop":
            if store is not None:
                store.download(local)
                downloaded_bytes = sum(path.stat().st_size for path in local.rglob("*.parquet"))
            ledger = plan["ledgers"][trial["workload_id"]]
            ledger_s = ledger.extend(workload.requests)
            ledger.load(sends)
            oracle = measurement.run_pinned(
                plan["oracle_cores"], measurement.read_oracle, local, ledger.ledger,
                require_all=False, healthy=True, workload=workload,
            )
            stored_unsent = ledger.stored_from_unsent()
            duplication = measurement.run_pinned(
                plan["oracle_cores"], descriptor_duplication, local
            )
    finally:
        if store is not None:
            _ = performance.clear_store(store)
            shutil.rmtree(local, ignore_errors=True)
        shutil.rmtree(data_dir, ignore_errors=True)
    jemalloc = jemalloc_peak(observed["log_path"]) if trial["jemalloc_stats"] else {}
    memory = memory_at_fill(samples, jemalloc) if trial["topology"] != "noop" else {}
    # CPU over the measured interval.
    window_ns = readings["end"]["monotonic_ns"] - readings["start"]["monotonic_ns"]
    engine_cpu_ns = readings["end"]["engine_cpu_ns"] - readings["start"]["engine_cpu_ns"]
    schedule = performance.thread_schedule(
        readings["start"]["threads"], readings["end"]["threads"], window_ns, worker_tids
    )
    store_cpu_s = (
        (readings["end"]["store_cpu_ns"] - readings["start"]["store_cpu_ns"]) / 1e9
        if readings["start"]["store_cpu_ns"] is not None else None
    )
    producer_cpu_s = (readings["end"]["producer_cpu_ns"] - readings["start"]["producer_cpu_ns"]) / 1e9
    object_bytes = sum(size for _key, size, _t in objects)
    epoch_offset_ns = time.time_ns() - time.monotonic_ns()
    wall_window = (window[0] + epoch_offset_ns, window[1] + epoch_offset_ns)
    interval_objects = [(size, t) for _key, size, t in objects
                        if wall_window[0] <= t < wall_window[1]]
    last_completion_ns = max(
        [t - epoch_offset_ns for _key, _size, t in objects] + [observed["last_response_ns"]],
        default=0,
    )
    first_send_ns = min(sends["sent"], default=start_ns)
    total_s = (last_completion_ns - first_send_ns) / 1e9 if last_completion_ns > first_send_ns else None
    durable_rate = window_durable_records / measure_s
    workers = len(spec.cores)
    measured = rates(window_durable_records, stats["wire_bytes_in_window"],
                     sum(size for size, _t in interval_objects), measure_s, workers)
    distribution = worker_distribution(samples, window)
    admission = {
        "closed_duration_in_window_s": (
            (value_at(samples, window[1], "admission.closed.duration") or 0)
            - (value_at(samples, window[0], "admission.closed.duration") or 0)
        ),
        "closed_samples_ratio": (
            sum(1 for s in samples if window[0] <= s["monotonic_ns"] <= window[1]
                and extras_total(s, "admission.closed") > 0)
            / max(1, sum(1 for s in samples if window[0] <= s["monotonic_ns"] <= window[1]))
        ),
    }
    flush_reasons = {}
    last = [s for s in samples if s.get("extras")]
    if last:
        for entry in last[-1]["extras"].values():
            for reason, value in (entry.get("flushes") or {}).items():
                flush_reasons[reason] = flush_reasons.get(reason, 0) + value
    metrics = {
        "offered_records_per_s": float(trial["rate"]),
        "durable_records_per_s": durable_rate,
        "durable_tail_records_per_s": tail_durable,
        "records_per_s_per_core": measured["records_per_s_per_core"],
        "input_bytes_per_s": measured["input_bytes_per_s"],
        "object_bytes_per_s": measured["object_bytes_per_s"],
        "total_object_bytes_per_s": object_bytes / total_s if total_s else None,
        "backlog_slope_ratio": slope / trial["rate"],
        "ack_latency_p50_s": stats["ack_latency_p50_s"],
        "ack_latency_p95_s": stats["ack_latency_p95_s"],
        "ack_latency_p99_s": stats["ack_latency_p99_s"],
        "engine_cpu_s": engine_cpu_ns / 1e9,
        "engine_cpu_ns_per_record": (
            engine_cpu_ns / window_durable_records if window_durable_records else None
        ),
        "engine_core_occupancy_ratio": engine_cpu_ns / (window_ns * workers) if window_ns else None,
        "producer_cpu_s": producer_cpu_s,
        "store_cpu_s": store_cpu_s,
        "peak_rss_bytes": float(observed["peak_rss_bytes"]),
        "drain_s": (observed["last_response_ns"] - window[1]) / 1e9,
    }
    if trial["topology"] != "noop":
        metrics.update({
            "object_bytes": float(object_bytes),
            "objects_count": float(len(objects)),
            "average_object_bytes": object_bytes / len(objects) if objects else None,
            "compression_ratio": object_bytes / stats["wire_bytes_total"] if stats["wire_bytes_total"] else None,
            "accounted_peak_bytes": memory.get("accounted_peak_bytes"),
            "accounted_to_budget_ratio": memory.get("accounted_to_budget_ratio"),
            "block_fill_peak_bytes": memory.get("block_fill_peak_bytes"),
        })
    unavailable = {name: "not measured in this trial" for name, value in metrics.items()
                   if value is None}
    result["metrics"] = metrics
    result["metrics_unavailable"] = unavailable
    result["metric_directions"] = {
        name: (measurement.HIGHER_IS_BETTER
               if name.endswith("records_per_s") or name.endswith("bytes_per_s")
               or name.endswith("_per_core") else measurement.LOWER_IS_BETTER)
        for name in metrics
    }
    result["mandatory_metrics"] = sorted(name for name in metrics if metrics[name] is not None)
    result["capacity"] = {
        "verdict": verdict["verdict"],
        "reasons": verdict["reasons"],
        "purpose": trial["purpose"],
        "offered_records_per_s": trial["rate"],
        "window_monotonic_ns": list(window),
        "warmup_s": trial["warmup_s"],
        "measure_s": trial["measure_s"],
        "interval_s": trial["interval_s"],
        "connections": trial["connections"],
        "receiver_capacity_per_worker": trial["receiver_capacity"],
        "upload_concurrency": trial["upload_concurrency"],
        "producer": stats,
        "producer_processes": sends["processes"],
        "senders_ready": observed.get("senders_ready"),
        "connections_established": observed.get("connections_established_count"),
        "backlog_points": points,
        "stored_rows_window_records_per_s": stored_rate,
        "stored_rows_floor_records": stored_floor,
        "window_durable_records": window_durable_records,
        "interval_object_bytes": sum(size for size, _t in interval_objects),
        "interval_objects_count": len(interval_objects),
        "objects_per_s": len(interval_objects) / measure_s,
        "total_object_bytes": object_bytes,
        "total_s_first_send_to_final_completion": total_s,
        "downloaded_bytes": downloaded_bytes,
        "incomplete_uploads": uploads,
        "worker_distribution": distribution,
        "descriptors": duplication,
        "admission": admission,
        "flush_reasons": flush_reasons,
        "memory": memory,
        "degradation": degradation(samples, window, trial["interval_s"], schedule),
        "accounted_against_allocated": accounted_against_allocated(
            samples, list(getattr(phase.sampler, "pairs", ()))),
        "schedule": {
            "workers": schedule["workers"],
            "other_threads_on_cpu_s": schedule["other_threads_on_cpu_s"],
            "worker_on_cpu_s": {
                str(t["tid"]): t["on_cpu_s"] for t in schedule["threads"] if t["role"] == "worker"
            },
        },
        "rates": measured,
        "ledger_extend_s": ledger_s,
        "stored_rows_of_unsent_requests_count": stored_unsent,
        "rss_residual_count": len(residuals),
        "rss_heap_term": heap,
        "rss_residual_range_bytes": [
            min((r["residual_bytes"] for r in residuals), default=None),
            max((r["residual_bytes"] for r in residuals), default=None),
        ],
    }
    result["observations"] = {
        "phase": {k: v for k, v in summary.items() if k != "residuals"},
        "residuals": residuals[::PUBLISHED_SAMPLE_STRIDE],
        "allocator_pairs": list(getattr(phase.sampler, "pairs", ()))[::PUBLISHED_SAMPLE_STRIDE],
    }
    # Every sample decided the checks above; one a second is published.
    result["samples"] = [
        dict(measurement.compact_sample(sample), extras=sample.get("extras"))
        for sample in samples[::PUBLISHED_SAMPLE_STRIDE]
    ]
    result["samples_published_stride"] = PUBLISHED_SAMPLE_STRIDE
    checks = result["checks"]
    if trial["topology"] == "noop":
        passed = stats["outcomes"].get(measurement.OUTCOME_ACK, 0) == len(sends["indexes"])
        checks.append(measurement.check(
            "delivery", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED,
            f"noop: {stats['outcomes']} of {len(sends['indexes'])} requests",
        ))
    else:
        passed = bool(oracle and oracle["passed"]) and stored_unsent == 0 and (
            downloaded_bytes is None or downloaded_bytes == object_bytes
        )
        checks.append(measurement.check(
            "delivery", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED,
            f"acked {stats['outcomes']}; oracle problems {oracle and oracle.get('problems')}; "
            f"stored rows of unsent requests {stored_unsent}; listed {object_bytes} bytes, "
            f"downloaded {downloaded_bytes}",
        ))
        result["capacity"]["oracle"] = {
            key: oracle[key] for key in (
                "passed", "part_file_count", "descriptor_identity_count", "series_cardinality",
                "actual_row_count", "readers", "expected_record_count", "missing_record_count",
                "unexpected_record_count", "corrupt_record_count", "multiplicity_histogram",
                "problems", "stored_rows_by_kind",
            ) if key in oracle
        } if oracle else None
    checks.append(measurement.check(
        "no_permanent_rejection", measurement.CHECK_HARD,
        measurement.STATUS_PASSED if permanent == 0 else measurement.STATUS_FAILED,
        f"{permanent} permanent nacks; details "
        + json.dumps([p["details"] for p in sends["processes"]], sort_keys=True)[:500],
    ))
    problems = command.sample_problems([summary])
    checks.append(measurement.check(
        "minimum_samples", measurement.CHECK_HARD,
        measurement.STATUS_FAILED if problems else measurement.STATUS_PASSED,
        "; ".join(problems) or f"{summary['sample_count']} samples",
    ))
    if trial["topology"] != "noop":
        peak = max((s["process_rss_bytes"] for s in samples), default=0)
        checks.append(measurement.residual_check(residuals, peak))
    checks.append(measurement.check(
        "warmup_complete", measurement.CHECK_HARD,
        measurement.STATUS_PASSED if warm_ok else measurement.STATUS_FAILED,
        "every worker acknowledged a flushed request before the measured interval"
        if warm_ok else "a worker had not completed a flush when the interval began",
    ))
    checks.append(measurement.check(
        "producer_complete", measurement.CHECK_HARD,
        measurement.STATUS_PASSED if all(p["complete"] for p in sends["processes"])
        else measurement.STATUS_FAILED,
        f"{len(sends['processes'])} sender processes",
    ))
    result["artifacts"] = [
        dict(measurement.file_entry(path), kind=kind, retention=str(run_dir))
        for path, kind in (
            (run_dir / "engine" / "engine.log", "engine_log"),
            (run_dir / "engine" / "pipeline.yaml", "engine_config"),
        ) if path.is_file()
    ]
    result["trial"] = dict(trial)
    result["status"] = measurement.STATUS_PASSED


def run_trial(plan, trial, output_dir, report_dir):
    """One trial under the host controls; its failure is recorded, not raised."""
    command = _command()
    spec = trial_spec(plan, trial)
    run_dir = Path(output_dir) / spec.run_id

    def experiment(spec, result, directory, controls):
        trial_experiment(plan, trial, spec, result, directory, controls)

    for attempt in range(1, performance.BUILD_RETRIES + 2):
        if measurement.build_activity():
            _ = performance.wait_for_quiet_host(deadline_s=3600)
        try:
            result = command.run_case(
                spec, run_dir, experiment=experiment, report_dir=report_dir,
                evaluate=False, lease_wait_s=plan.get("lease_wait_s", 0.0),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as error:  # noqa: BLE001 - recorded in the result
            sys.stderr.write(f"{spec.run_id}: {type(error).__name__}: {error}\n")
            result = json.loads((run_dir / f"{spec.run_id}.json").read_text(encoding="ascii"))
        _ = shutil.copyfile(run_dir / f"{spec.run_id}.json",
                            Path(output_dir) / f"{spec.run_id}.json")
        if not performance.invalidated_by_build(result):
            break
        plan["invalidated"].append(spec.run_id)
        trial = dict(trial, ordinal=plan["next_ordinal"]())
        spec = trial_spec(plan, trial)
        run_dir = Path(output_dir) / spec.run_id
    archive_trial(plan, run_dir)
    return result


def archive_trial(plan, run_dir):
    """Keep a trial's logs and configs as a tarball; drop the rest."""
    archive = plan.get("archive_dir")
    if archive:
        Path(archive).mkdir(parents=True, exist_ok=True)
        target = Path(archive) / f"{Path(run_dir).name}.tgz"
        members = [p for p in Path(run_dir).rglob("*")
                   if p.is_file() and p.suffix != ".parquet" and "buffer" not in p.parts]
        if members:
            _ = subprocess.run(
                ["tar", "czf", str(target), "-C", str(Path(run_dir).parent)]
                + [str(p.relative_to(Path(run_dir).parent)) for p in members],
                check=False, capture_output=True, timeout=600,
            )
    for child in ("engine", "store", "senders"):
        shutil.rmtree(Path(run_dir) / child, ignore_errors=True)


def trial_verdict(result) -> str:
    """The search verdict of a published trial; a failed run is `failed`.

    A run whose only failed checks are the memory residual still measured
    delivery, validity and its rate, so its verdict brackets the search;
    the run stays failed and can never be part of a baseline.
    """
    capacity = result.get("capacity") or {}
    if result.get("status") == measurement.STATUS_PASSED:
        return capacity.get("verdict", "failed")
    failed = {entry["name"] for entry in result.get("checks", [])
              if entry["status"] != measurement.STATUS_PASSED}
    if failed and failed <= set(measurement.RESIDUAL_CHECKS) and capacity.get("verdict"):
        return capacity["verdict"]
    return "failed"


def residual_failed(result) -> bool:
    """Whether a trial failed the memory residual gate."""
    return any(entry["name"] in measurement.RESIDUAL_CHECKS
               and entry["status"] != measurement.STATUS_PASSED
               for entry in result.get("checks", []))


# --------------------------------------------------------------------------
# The family: calibration, searches, confirmations and publication
# --------------------------------------------------------------------------

STATE_FILE = "capacity-state.json"
CALIBRATION_WARMUP_S = 5
CALIBRATION_MEASURE_S = 20
CALIBRATION_START_RECORDS_PER_S = 125_000
POOL_DIR_DEFAULT = "/var/tmp/series-capacity-pools"
ARCHIVE_DIR_DEFAULT = measurement.REPO_ROOT / ".measurement-artifacts" / "capacity"


def cell_key(store_kind, core_count) -> str:
    return f"{store_kind}-c{core_count}"


class FamilyState:
    """Every trial the family ran, persisted so a family can be resumed."""

    def __init__(self, output_dir):
        self.path = Path(output_dir) / STATE_FILE
        self.document = (
            json.loads(self.path.read_text(encoding="ascii")) if self.path.is_file()
            else {"trials": [], "family_ordinal": None}
        )

    def add(self, result, trial, cell):
        self.document["trials"].append({
            "run_id": result["run_id"],
            "cell": cell,
            "workload_id": trial["workload_id"],
            "purpose": trial["purpose"],
            "rate": trial["rate"],
            "topology": trial["topology"],
            "interval_s": trial["interval_s"],
            "connections": trial["connections"],
            "receiver_capacity": trial["receiver_capacity"],
            "upload_concurrency": trial["upload_concurrency"],
            "status": result["status"],
            "verdict": trial_verdict(result),
            "residual_failed": residual_failed(result),
            "durable_records_per_s": result["metrics"].get("durable_records_per_s"),
        })
        _ = measurement.write_json_atomic(self.path, self.document)

    def supersede(self, run_ids, reason):
        """Take trials out of every decision, keeping them and the reason."""
        for entry in self.document["trials"]:
            if entry["run_id"] in run_ids:
                entry["superseded_purpose"] = entry["purpose"]
                entry["purpose"] = "superseded"
                entry["superseded_reason"] = reason
        _ = measurement.write_json_atomic(self.path, self.document)

    def trials(self, cell=None, purpose=None, workload_id=None):
        return [
            entry for entry in self.document["trials"]
            if (cell is None or entry["cell"] == cell)
            and (purpose is None or entry["purpose"] in (
                (purpose,) if isinstance(purpose, str) else tuple(purpose)))
            and (workload_id is None or entry["workload_id"] == workload_id)
        ]


def next_ordinal_factory(*directories):
    """A counter past every published or local capacity trial ordinal."""
    highest = 0
    for directory in directories:
        directory = Path(directory)
        if not directory.is_dir():
            continue
        for path in directory.glob("capacity-*-r[0-9][0-9][0-9].json"):
            with contextlib.suppress(IndexError, ValueError):
                highest = max(highest, int(path.stem.rsplit("-r", 1)[1]))
    counter = {"value": highest}

    def take():
        counter["value"] += 1
        return counter["value"]

    return take


def make_trial(plan, *, workload_id=PRIMARY_WORKLOAD, rate, purpose,
               topology="strict", interval_s=SEARCH_INTERVAL_S, connections=SEARCH_CONNECTIONS,
               receiver_capacity=SHIPPED_RECEIVER_CAPACITY, upload_concurrency=2,
               warmup_s=None, measure_s=MEASURE_S, jemalloc_stats=True) -> dict:
    """One trial's settings; the warm-up covers at least two windows."""
    if warmup_s is None:
        warmup_s = max(WARMUP_S, 2 * interval_s + 5)
    if plan.get("rehearsal"):
        # A rehearsal proves the harness in seconds and publishes nowhere
        # the report directory reads.
        warmup_s, measure_s = max(3, 2 * interval_s + 1), 6
    return {
        "workload_id": workload_id,
        "rate": int(rate),
        "purpose": purpose,
        "topology": topology,
        "interval_s": interval_s,
        "connections": connections,
        "receiver_capacity": receiver_capacity,
        "upload_concurrency": upload_concurrency,
        "warmup_s": warmup_s,
        "measure_s": measure_s,
        "jemalloc_stats": jemalloc_stats,
        "ordinal": plan["next_ordinal"](),
    }


def ensure_pool(plan, workload_id, rate, duration_s):
    """The workload's pool and ledger, grown to cover one trial."""
    config = CAPACITY_WORKLOADS[workload_id]
    count = trial_requests(rate, duration_s, config["workload"].records_per_request)
    end = config["first_index"] + count
    pool = plan["pools"].get(workload_id)
    if pool is None:
        pool = CapacityPool(Path(plan["pool_dir"]) / workload_id, config["workload"])
        plan["pools"][workload_id] = pool
    built = pool.ensure(end, processes=plan["build_processes"])
    if built["segments_built"]:
        sys.stderr.write(f"pool {workload_id}: built {built}\n")
    if workload_id not in plan["ledgers"]:
        for other in list(plan["ledgers"]):
            plan["ledgers"].pop(other).close()
            for suffix in ("", "-wal", "-shm"):
                with contextlib.suppress(FileNotFoundError):
                    Path(f"{plan['ledger_dir']}/ledger-{other}.sqlite{suffix}").unlink()
        Path(plan["ledger_dir"]).mkdir(parents=True, exist_ok=True)
        plan["ledgers"][workload_id] = TrialLedger(
            Path(plan["ledger_dir"]) / f"ledger-{workload_id}.sqlite", pool,
            config["first_index"],
        )
    return pool


def execute(plan, state, trial, output_dir, report_dir, cell):
    """Run one trial, record it in the family state and return its result."""
    _ = ensure_pool(plan, trial["workload_id"], trial["rate"],
                    trial["warmup_s"] + trial["measure_s"])
    started = time.monotonic()
    result = run_trial(plan, trial, output_dir, report_dir)
    state.add(result, trial, cell)
    sys.stderr.write(
        f"{result['run_id']}: {result['status']} {trial_verdict(result)} "
        f"offered {trial['rate']} durable "
        f"{result['metrics'].get('durable_records_per_s')} "
        f"cpu/rec {result['metrics'].get('engine_cpu_ns_per_record')} "
        f"({time.monotonic() - started:.0f}s) "
        + json.dumps((result.get("capacity") or {}).get("reasons"))
        + "\n"
    )
    return result


def open_plan(store_kind, core_count, output_dir, options) -> dict:
    """Everything a cell's trials share, gathered before any lease."""
    command = _command()
    topology = measurement.core_topology()
    cores = tuple(options.get("cores") or command.default_engine_cores(core_count))
    allocation = measurement.role_allocation(
        topology["sibling_groups"], sorted(os.sched_getaffinity(0)), cores,
        roles=measurement.CASE_ROLES["capacity"],
    )
    allocation = whole_core_roles(allocation, topology["sibling_groups"])
    ledger_fs = performance.memory_ledger_dir(
        options.get("ledger_dir") or f"/tmp/series-capacity-ledgers-{os.getpid()}"
    )
    plan = {
        "store_kind": store_kind,
        "store": None,
        "store_pid": None,
        "cores": list(cores),
        "allocation": allocation,
        "oracle_cores": performance.oracle_cores(allocation, topology["sibling_groups"]),
        "producer_processes": int(options.get("producer_processes", PRODUCER_PROCESSES)),
        "provenance": command.prepare_build(),
        "ledger_filesystem": ledger_fs,
        "ledger_dir": ledger_fs["directory"],
        "pool_dir": options.get("pool_dir", POOL_DIR_DEFAULT),
        "build_processes": int(options.get("build_processes", 12)),
        "pools": {},
        "ledgers": {},
        "ephemeral": {},
        "invalidated": [],
        "archive_dir": str(options.get("archive_dir", ARCHIVE_DIR_DEFAULT)),
        "lease_wait_s": float(options.get("lease_wait_s", 3600.0)),
        "rehearsal": bool(options.get("rehearsal", False)),
    }
    if plan["rehearsal"] and options.get("report_dir") is None:
        raise AssertionError("a rehearsal names its own --option report_dir=...")
    plan["next_ordinal"] = next_ordinal_factory(
        measurement.resolve_report_dir(options.get("report_dir")), output_dir
    )
    return plan


@contextlib.contextmanager
def cell_store(plan):
    """The cell's object store container, pinned to its cores, or none."""
    if plan["store_kind"] == "local":
        yield None
        return
    store = test_e2e.DockerStore(plan["store_kind"], by_image_id=True)
    try:
        _ = store.__enter__()
        plan["store"] = store
        plan["store_pid"] = performance.container_pid(store.container)
        plan["store_pinned"] = performance.pin_container(
            store.container, plan["allocation"].get("store", [])
        )
        plan["ephemeral"] = {"<store_endpoint>": store.endpoint}
        plan["store_image"] = {"image": store.image, "image_id": store.image_id}
        yield store
    finally:
        store.__exit__(None, None, None)
        plan["store"] = None


def close_plan(plan):
    for ledger in plan["ledgers"].values():
        ledger.close()
    plan["ledgers"] = {}
    shutil.rmtree(plan["ledger_dir"], ignore_errors=True)
    for pool in plan["pools"].values():
        pool.close()


def winning(state, cell, workload_id=PRIMARY_WORKLOAD, variant="shipped") -> dict:
    """The search decision of one cell, from its recorded trials.

    Its search trials and its repetitions count alike, so a rate that one
    repetition could not sustain is unsustainable and the search goes on
    below it. The `raised` variant is seeded with the shipped search's
    sustainable rate, which a larger receiver capacity can only make easier.
    """
    trials = [(entry["rate"], entry["verdict"])
              for entry in state.trials(
                  cell, (SEARCH_PURPOSES[variant], REPETITION_PURPOSES[variant]), workload_id)]
    if variant == "raised":
        shipped = winning(state, cell, workload_id)["sustainable_records_per_s"]
        if shipped is not None:
            trials = [(shipped, "sustainable")] + trials
    decision = search_decision(trials)
    decision["trials"] = trials
    decision["variant"] = variant
    return decision


# The search variants: their trial purposes, receiver slots per worker and
# window. `default_window` is the shipped configuration as it ships.
SEARCH_PURPOSES = {"shipped": "search", "raised": "search_raised",
                   "default_window": "search_default_window"}
REPETITION_PURPOSES = {"shipped": "repetition", "raised": "repetition_raised",
                       "default_window": "repetition_default_window"}
VARIANT_CAPACITY = {"shipped": SHIPPED_RECEIVER_CAPACITY, "raised": RAISED_RECEIVER_CAPACITY,
                    "default_window": SHIPPED_RECEIVER_CAPACITY}
VARIANT_INTERVAL_S = {"shipped": SEARCH_INTERVAL_S, "raised": SEARCH_INTERVAL_S,
                      "default_window": DEFAULT_INTERVAL_S}


def admission_ceiling(slots_per_worker, records_per_request, hold_s) -> float:
    """The strict records/s one worker can sustain at most.

    A strict request holds its receiver slot until its block is durable, so
    at most `slots_per_worker` requests are in flight, each held for about
    one window plus the flush that makes it durable.
    """
    return slots_per_worker * records_per_request / hold_s


def step_calibrate(plan, state, output_dir, report_dir, cell, options):
    """The producer's capacity against the noop exporter, doubling."""
    rate = int(options.get("calibration_start", CALIBRATION_START_RECORDS_PER_S))
    limit = int(options.get("calibration_limit", 2_000_000))
    while rate <= limit:
        trial = make_trial(
            plan, rate=rate, purpose="calibration", topology="noop",
            warmup_s=CALIBRATION_WARMUP_S, measure_s=CALIBRATION_MEASURE_S,
        )
        result = execute(plan, state, trial, output_dir, report_dir, cell)
        if trial_verdict(result) != "sustainable":
            break
        rate *= 2


def sender_capacity(state, core_count) -> dict:
    """The highest offered rate the producer kept up with against noop."""
    trials = [entry for entry in state.trials(purpose="calibration")
              if entry["cell"].endswith(f"-c{core_count}")]
    kept = [entry["rate"] for entry in trials if entry["verdict"] == "sustainable"]
    lost = [entry["rate"] for entry in trials if entry["verdict"] != "sustainable"]
    return {
        "sustained_records_per_s": max(kept) if kept else None,
        "first_failed_records_per_s": min(lost) if lost else None,
        "trials": [(entry["rate"], entry["verdict"], entry["run_id"]) for entry in trials],
    }


def step_search(plan, state, output_dir, report_dir, cell, options, variant="shipped"):
    """Double, then bisect, then repeat the winner until three repetitions.

    The `raised` variant runs with the receiver capacity raised and starts
    from the shipped search's bracket: its first trial is the rate the
    shipped capacity could not sustain.
    """
    workload_id = options.get("workload_id", PRIMARY_WORKLOAD)
    purpose = SEARCH_PURPOSES[variant]
    capacity = VARIANT_CAPACITY[variant]
    if variant == "raised":
        shipped = winning(state, cell, workload_id)
        if shipped["sustainable_records_per_s"] is None:
            return
    while True:
        own = [(entry["rate"], entry["verdict"])
               for entry in state.trials(
                   cell, (purpose, REPETITION_PURPOSES[variant]), workload_id)]
        if any(verdict == "failed" for _rate, verdict in own):
            sys.stderr.write(f"{cell}: a failed trial stops the {variant} search\n")
            return
        if variant == "raised" and not own and shipped["unsustainable_records_per_s"]:
            rate, trial_purpose = shipped["unsustainable_records_per_s"], purpose
        else:
            decision = winning(state, cell, workload_id, variant)
            rate, trial_purpose = next_search_rate(decision["trials"]), purpose
            if rate is None:
                # Decided: repeat the winner until three trials measured it;
                # a repetition that fails moves the search below that rate.
                rate = decision["sustainable_records_per_s"]
                if rate is None:
                    return
                measured = sum(1 for r, verdict in own if r == rate and verdict == "sustainable")
                if measured >= REPETITIONS:
                    return
                trial_purpose = REPETITION_PURPOSES[variant]
        _ = execute(plan, state, make_trial(plan, workload_id=workload_id, rate=rate,
                                            purpose=trial_purpose, receiver_capacity=capacity,
                                            interval_s=VARIANT_INTERVAL_S[variant]),
                    output_dir, report_dir, cell)


def step_search_raised(plan, state, output_dir, report_dir, cell, options):
    """The search with the receiver capacity raised, from the shipped bracket."""
    step_search(plan, state, output_dir, report_dir, cell, options, variant="raised")


def step_search_default_window(plan, state, output_dir, report_dir, cell, options):
    """The search in the configuration as it ships: 15 s windows, 128 slots."""
    step_search(plan, state, output_dir, report_dir, cell, options, variant="default_window")


def step_confirm_default_window(plan, state, output_dir, report_dir, cell, options):
    """The winning rate with the shipped 15 s window, upload concurrency 2 and 1."""
    decision = winning(state, cell)
    rate = decision["sustainable_records_per_s"]
    if rate is None:
        return
    for concurrency in options.get("upload_concurrencies", (2, 1)):
        _ = execute(plan, state, make_trial(
            plan, rate=rate, purpose=f"default_window_upload{concurrency}",
            interval_s=DEFAULT_INTERVAL_S, upload_concurrency=concurrency,
        ), output_dir, report_dir, cell)


def ceiling(state, cell) -> tuple:
    """The exporter's sustainable rate for a cell and the receiver capacity
    it was measured with: the raised search when it ran, else the shipped."""
    raised = winning(state, cell, variant="raised")
    if state.trials(cell, SEARCH_PURPOSES["raised"]) and raised["sustainable_records_per_s"]:
        return raised["sustainable_records_per_s"], RAISED_RECEIVER_CAPACITY
    return winning(state, cell)["sustainable_records_per_s"], SHIPPED_RECEIVER_CAPACITY


def step_buffered(plan, state, output_dir, report_dir, cell, options):
    """The buffered topology at 80 percent of the strict ceiling."""
    rate, capacity = ceiling(state, cell)
    if rate is None:
        return
    _ = execute(plan, state, make_trial(
        plan, rate=int(rate * CONFIRMATION_FRACTION), purpose="buffered",
        topology="buffered", receiver_capacity=capacity,
    ), output_dir, report_dir, cell)


def step_fan_in(plan, state, output_dir, report_dir, cell, options):
    """The ceiling rate from 1, 8, 64 and 256 client connections."""
    rate, capacity = ceiling(state, cell)
    if rate is None:
        return
    for connections in options.get("fan_in", FAN_IN_CONNECTIONS):
        if connections == SEARCH_CONNECTIONS:
            continue
        _ = execute(plan, state, make_trial(
            plan, rate=rate, purpose=f"fan_in_{connections}", connections=connections,
            receiver_capacity=capacity,
        ), output_dir, report_dir, cell)


def step_workloads(plan, state, output_dir, report_dir, cell, options):
    """The other workload rows at 80 percent of the primary capacity.

    A row that fails there is bracketed on its own by halving and then
    bisecting; it is never labelled a maximum of the primary search.
    """
    base, capacity = ceiling(state, cell)
    if base is None:
        return
    for workload_id in options.get("rows", ("logs-1k-hot", "mixed-1k-churn", "mixed-8k-hot")):
        rate = int(base * CONFIRMATION_FRACTION)
        result = execute(plan, state, make_trial(
            plan, workload_id=workload_id, rate=rate, purpose="confirmation",
            receiver_capacity=capacity,
        ), output_dir, report_dir, cell)
        if trial_verdict(result) != "unsustainable":
            continue
        trials = [(rate, "unsustainable")]
        while True:
            sustainable = [r for r, v in trials if v == "sustainable"]
            high = min(r for r, v in trials if v == "unsustainable")
            if not sustainable:
                next_rate = high // 2
                if next_rate < SEARCH_START_RECORDS_PER_S:
                    break
            else:
                low = max(r for r in sustainable if r < high)
                if (high - low) / low <= BRACKET_WIDTH_RATIO:
                    break
                next_rate = (low + high) // 2
            result = execute(plan, state, make_trial(
                plan, workload_id=workload_id, rate=next_rate, purpose="confirmation_bracket",
                receiver_capacity=capacity,
            ), output_dir, report_dir, cell)
            verdict = trial_verdict(result)
            if verdict not in ("sustainable", "unsustainable"):
                break
            trials.append((next_rate, verdict))


def step_high_cardinality(plan, state, output_dir, report_dir, cell, options):
    """A point attribute unique per point against the 10k-slot workload."""
    rate = int(options.get("high_cardinality_rate", 20_000))
    for workload_id in ("metrics-1k-hot", "metrics-1k-unique"):
        _ = execute(plan, state, make_trial(
            plan, workload_id=workload_id, rate=rate, purpose="high_cardinality",
        ), output_dir, report_dir, cell)


def step_stats_off(plan, state, output_dir, report_dir, cell, options):
    """The shipped winning rate without jemalloc's statistics prints, to show
    the prints the residual reads cost nothing measurable."""
    rate = winning(state, cell)["sustainable_records_per_s"]
    if rate is None:
        return
    _ = execute(plan, state, make_trial(
        plan, rate=rate, purpose="stats_off", jemalloc_stats=False,
    ), output_dir, report_dir, cell)


def step_trial(plan, state, output_dir, report_dir, cell, options):
    """One trial with explicit settings, for a rehearsal or a named check."""
    settings = dict(options.get("trial") or {})
    settings.setdefault("purpose", "single")
    _ = execute(plan, state, make_trial(plan, **settings), output_dir, report_dir, cell)


STEPS = {
    "trial": step_trial,
    "calibrate": step_calibrate,
    "search": step_search,
    "default_window": step_confirm_default_window,
    "buffered": step_buffered,
    "fan_in": step_fan_in,
    "search_raised": step_search_raised,
    "stats_off": step_stats_off,
    "search_default_window": step_search_default_window,
    "workloads": step_workloads,
    "high_cardinality": step_high_cardinality,
}


def run_capacity(output_dir, report_dir=None, **options) -> list:
    """`measure capacity`: run the named steps for each store and core count,
    then publish one index per store."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    state = FamilyState(output_dir)
    stores = list(options.get("stores") or ("local", "minio", "rustfs"))
    core_counts = [int(count) for count in (options.get("core_counts") or (1, 4))]
    steps = list(options.get("steps") or ("search",))
    unknown = sorted(set(steps) - set(STEPS) - {"publish"})
    if unknown:
        raise AssertionError(f"unknown capacity steps {unknown}; known {sorted(STEPS)}")
    for store_kind in stores:
        for core_count in core_counts:
            work = [step for step in steps if step != "publish"]
            if not work:
                continue
            plan = open_plan(store_kind, core_count, output_dir,
                             dict(options, report_dir=report_dir))
            try:
                with cell_store(plan):
                    for step in work:
                        STEPS[step](plan, state, output_dir, report_dir,
                                    cell_key(store_kind, core_count), options)
            finally:
                close_plan(plan)
    if "publish" in steps:
        return [publish_store(store_kind, state, output_dir, report_dir, options)
                for store_kind in stores]
    return []


# --------------------------------------------------------------------------
# Aggregates and per-store indexes
# --------------------------------------------------------------------------

AGGREGATE_METRICS = (
    "durable_records_per_s", "records_per_s_per_core", "engine_cpu_ns_per_record",
    "input_bytes_per_s", "object_bytes_per_s", "total_object_bytes_per_s",
    "compression_ratio", "ack_latency_p50_s", "ack_latency_p95_s", "ack_latency_p99_s",
    "peak_rss_bytes", "engine_core_occupancy_ratio",
)


def load_result(output_dir, run_id) -> dict:
    return json.loads((Path(output_dir) / f"{run_id}.json").read_text(encoding="ascii"))


def aggregate_cell(state, cell, output_dir, report_dir, family_ordinal,
                   variant="shipped") -> dict:
    """A cell's winning rate as the median of three independent repetitions."""
    decision = winning(state, cell, variant=variant)
    rate = decision["sustainable_records_per_s"]
    entries = [entry for entry in state.trials(
                   cell, (SEARCH_PURPOSES[variant], REPETITION_PURPOSES[variant]))
               if entry["rate"] == rate and entry["workload_id"] == PRIMARY_WORKLOAD]
    children = [load_result(output_dir, entry["run_id"]) for entry in entries]
    first = children[0]
    store_kind, cores = cell.rsplit("-c", 1)
    case = f"capacity-{PRIMARY_WORKLOAD}-{variant}"
    run_id = (f"{case}-strict-{store_kind}-c{cores}-w{VARIANT_INTERVAL_S[variant]}"
              f"-f{family_ordinal:03d}")
    result = measurement.new_result(
        {"run_id": run_id, "case": case}, artifact_kind="capacity_aggregate",
    )
    environment = first.get("environment") or {}
    result["environment"] = {
        "start": environment.get("start") or {},
        "end": (children[-1].get("environment") or {}).get("end") or {},
        "build": environment.get("build"),
        "git": environment.get("git") or {},
        "core_allocation": environment.get("core_allocation"),
        "machine_identity_sha256": environment.get("machine_identity_sha256"),
        "children": [child["run_id"] for child in children],
    }
    result["config"] = first.get("config") or {}
    result["workload"] = first.get("workload") or {}
    result["workload_schedule"] = first.get("workload_schedule") or {}
    result["ephemeral_values"] = first.get("ephemeral_values") or {}
    result["run_dir"] = first.get("run_dir") or str(output_dir)
    by_metric = {
        name: [child["metrics"][name] for child in children
               if isinstance(child["metrics"].get(name), (int, float))]
        for name in AGGREGATE_METRICS
    }
    result["metrics"] = {
        name: performance.median(values) if values else None
        for name, values in by_metric.items()
    }
    result["metrics"]["sustainable_records_per_s"] = result["metrics"].pop("durable_records_per_s")
    result["metrics_unavailable"] = {
        name: "no repetition measured it" for name, value in result["metrics"].items()
        if value is None
    }
    result["metric_directions"] = {
        name: (measurement.HIGHER_IS_BETTER if name.endswith(("records_per_s", "bytes_per_s",
                                                               "_per_core"))
               else measurement.LOWER_IS_BETTER)
        for name in result["metrics"]
    }
    result["mandatory_metrics"] = sorted(n for n, v in result["metrics"].items() if v is not None)
    verdicts = [trial_verdict(child) for child in children]
    rates_ = by_metric["durable_records_per_s"]
    cv = performance.coefficient_of_variation(rates_) if len(rates_) > 1 else None
    checks = [
        measurement.check(
            "repetitions_complete", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if len(children) >= REPETITIONS
            else measurement.STATUS_FAILED,
            f"{len(children)} repetitions at {rate} records/s",
        ),
        measurement.check(
            "repetitions_sustainable", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if all(v == "sustainable" for v in verdicts)
            else measurement.STATUS_FAILED,
            f"verdicts {verdicts}",
        ),
        measurement.check(
            "repetition_stability", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if cv is not None and cv <= performance.MAXIMUM_CV
            else measurement.STATUS_FAILED,
            f"durable rate coefficient of variation {cv}",
        ),
        measurement.check(
            "bracketed", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if decision["bracketed"] else measurement.STATUS_FAILED,
            json.dumps({k: decision[k] for k in ("sustainable_records_per_s",
                                                  "unsustainable_records_per_s",
                                                  "bracket_width_ratio", "kind")}),
        ),
    ]
    for name in measurement.REQUIRED_HARD_CHECKS + ("no_permanent_rejection", "warmup_complete"):
        failed = [child["run_id"] for child in children
                  if not any(entry["name"] == name and entry["status"] == measurement.STATUS_PASSED
                             for entry in child.get("checks", []))]
        checks.append(measurement.check(
            name, measurement.CHECK_HARD,
            measurement.STATUS_FAILED if failed else measurement.STATUS_PASSED,
            f"not passed in {failed}" if failed else f"passed in all {len(children)}",
        ))
    result["checks"] = checks
    result["capacity"] = {
        "cell": cell,
        "variant": variant,
        "receiver_capacity_per_worker": VARIANT_CAPACITY[variant],
        "decision": decision,
        "repetitions": [
            {"run_id": child["run_id"], "verdict": trial_verdict(child),
             "metrics": child["metrics"]} for child in children
        ],
        "durable_rate_cv_ratio": cv,
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
        baseline = measurement.evaluate_baseline(result)
        if baseline["action"] == "created":
            candidate = result.pop("baseline_candidate")
            path = measurement.write_published_json(
                Path(output_dir) / baseline["baseline_name"], candidate
            )
            result["baseline_files"].append(measurement.file_entry(path))
    except AssertionError as error:
        result["status"] = measurement.STATUS_FAILED
        measurement.record_event(result, "baseline_policy", str(error))
    result.pop("baseline_candidate", None)
    _ = measurement.write_result(Path(output_dir) / f"{run_id}.json", result)
    return result


def trial_row(output_dir, entry) -> dict:
    """What an index shows of one trial."""
    child = load_result(output_dir, entry["run_id"])
    capacity = child.get("capacity") or {}
    metrics = child.get("metrics") or {}
    memory = capacity.get("memory") or {}
    return {
        "run_id": entry["run_id"],
        "cell": entry["cell"],
        "workload_id": entry["workload_id"],
        "purpose": entry["purpose"],
        "topology": entry["topology"],
        "interval_s": entry["interval_s"],
        "connections": entry["connections"],
        "receiver_capacity_per_worker": entry["receiver_capacity"],
        "upload_concurrency": entry["upload_concurrency"],
        "status": child.get("status"),
        "verdict": trial_verdict(child),
        "residual_failed": residual_failed(child),
        "superseded_reason": entry.get("superseded_reason"),
        "reasons": capacity.get("reasons"),
        "failed_checks": [c["name"] for c in child.get("checks", [])
                          if c["status"] != measurement.STATUS_PASSED],
        "metrics": {key: metrics.get(key) for key in sorted(metrics)},
        "producer": {key: (capacity.get("producer") or {}).get(key) for key in (
            "late_unblocked_ratio", "blocked_on_in_flight_count", "lateness_p99_s",
            "outstanding_at_send_max_count", "outcomes")},
        "worker_distribution": capacity.get("worker_distribution"),
        "descriptors": capacity.get("descriptors"),
        "admission": capacity.get("admission"),
        "flush_reasons": capacity.get("flush_reasons"),
        "memory": memory,
        "degradation": capacity.get("degradation"),
        "client_refusals": [p.get("details") for p in capacity.get("producer_processes") or []],
        "accounted_against_allocated": capacity.get("accounted_against_allocated"),
        "rss_heap_term": capacity.get("rss_heap_term"),
        "objects_per_s": capacity.get("objects_per_s"),
        "total_s_first_send_to_final_completion": capacity.get(
            "total_s_first_send_to_final_completion"),
        "incomplete_uploads": capacity.get("incomplete_uploads"),
        "oracle": {key: (capacity.get("oracle") or {}).get(key) for key in (
            "passed", "expected_record_count", "actual_row_count", "missing_record_count",
            "unexpected_record_count", "corrupt_record_count", "multiplicity_histogram",
            "series_cardinality", "readers")},
    }


def publish_store(store_kind, state, output_dir, report_dir, options) -> dict:
    """One store's index: every trial, each cell's aggregate and bracket."""
    output_dir = Path(output_dir)
    ordinal = int(options.get("family_ordinal", 1))
    run_id = f"capacity-{store_kind}"
    index = measurement.new_result({"run_id": run_id, "case": "capacity"},
                                   artifact_kind="capacity_index")
    cells = sorted({entry["cell"] for entry in state.trials()
                    if entry["cell"].startswith(f"{store_kind}-c")})
    entries = [entry for entry in state.trials() if entry["cell"] in cells]
    if store_kind == "local":
        entries += [entry for entry in state.trials(purpose="calibration")
                    if entry not in entries]
    aggregates = []
    for cell in cells:
        for variant in SEARCH_PURPOSES:
            if not state.trials(cell, SEARCH_PURPOSES[variant]):
                continue
            decision = winning(state, cell, variant=variant)
            if decision["sustainable_records_per_s"] is not None and any(
                entry["rate"] == decision["sustainable_records_per_s"]
                for entry in state.trials(
                    cell, (SEARCH_PURPOSES[variant], REPETITION_PURPOSES[variant]))
            ):
                aggregates.append(aggregate_cell(state, cell, output_dir, report_dir,
                                                 ordinal, variant))
    rows = [trial_row(output_dir, entry) for entry in entries]
    index["run_dir"] = str(output_dir)
    index["run_files"] = [
        measurement.file_entry(output_dir / f"{entry['run_id']}.json") for entry in entries
    ] + [measurement.file_entry(output_dir / f"{agg['run_id']}.json") for agg in aggregates]
    index["baseline_files"] = [entry for agg in aggregates for entry in agg["baseline_files"]]
    first_child = load_result(output_dir, entries[0]["run_id"]) if entries else {}
    index["environment"] = {
        "start": (first_child.get("environment") or {}).get("start")
        or measurement.environment_snapshot({"harness": os.getpid()}),
        "end": (first_child.get("environment") or {}).get("end")
        or measurement.environment_snapshot({"harness": os.getpid()}),
        "build": (first_child.get("environment") or {}).get("build"),
        "git": (first_child.get("environment") or {}).get("git"),
        "malloc_conf": JEMALLOC_STATS_CONF,
        "jemalloc_stats_interval_bytes": JEMALLOC_STATS_INTERVAL_BYTES,
    }
    summary = {}
    for cell in cells:
        summary[cell] = {"sender_capacity": sender_capacity(state, int(cell.rsplit("-c", 1)[1]))}
        for variant in SEARCH_PURPOSES:
            aggregate = next((agg for agg in aggregates if agg["capacity"]["cell"] == cell
                              and agg["capacity"]["variant"] == variant), None)
            summary[cell][variant] = {
                "decision": winning(state, cell, variant=variant),
                "aggregate": aggregate["run_id"] if aggregate else None,
                "aggregate_status": aggregate["status"] if aggregate else None,
                "aggregate_metrics": aggregate["metrics"] if aggregate else None,
            }
    for cell in cells:
        if not cell.endswith("-c4"):
            continue
        for variant in SEARCH_PURPOSES:
            one = (summary.get(cell.replace("-c4", "-c1")) or {}).get(variant) or {}
            four = summary[cell][variant]
            if one.get("aggregate_metrics") and four.get("aggregate_metrics"):
                single = one["aggregate_metrics"]["sustainable_records_per_s"]
                quad = four["aggregate_metrics"]["sustainable_records_per_s"]
                four["scaling_efficiency_ratio"] = quad / (4 * single) if single else None
    index["capacity"] = {
        "store": store_kind,
        "cells": summary,
        "trials": rows,
        "invalidated_by_build": state.document.get("invalidated", []),
        "rules": {
            "start_records_per_s": SEARCH_START_RECORDS_PER_S,
            "doubling_trials": DOUBLING_TRIALS,
            "bracket_width_ratio": BRACKET_WIDTH_RATIO,
            "warmup_s": WARMUP_S,
            "measure_s": MEASURE_S,
            "tail_s": STABILITY_TAIL_S,
            "durable_ratio_floor": DURABLE_RATIO_FLOOR,
            "backlog_slope_limit_ratio": BACKLOG_SLOPE_LIMIT_RATIO,
            "producer_late_s": PRODUCER_LATE_S,
            "producer_late_limit_ratio": PRODUCER_LATE_LIMIT_RATIO,
            "sender_headroom_ratio": SENDER_HEADROOM_RATIO,
            "search_interval_s": SEARCH_INTERVAL_S,
            "search_interval_override": "one-second windows instead of the shipped 15 s, "
            "so a bounded producer concurrency does not cap the strict hold time",
            "search_connections": SEARCH_CONNECTIONS,
            "receiver_capacity_per_worker": dict(VARIANT_CAPACITY),
            "admission_ceiling": "strict records/s per worker <= receiver slots x records "
            "per request / hold time, a window plus the flush that makes a block durable",
            "raised_search_seed": "the raised search starts from the shipped search's "
            "bracket: its sustainable rate is taken as sustainable and its first trial "
            "is the shipped unsustainable rate",
            "records_per_request": RECORDS_PER_REQUEST,
            "workloads": {key: dict(value["workload"].as_json(),
                                    description=value["description"],
                                    first_index=value["first_index"])
                          for key, value in CAPACITY_WORKLOADS.items()},
        },
        "stage_join": {
            "joined": False,
            "reason": "no attribution family ran this workload, configuration and "
            "core allocation; Task 4 shares are for logs-1k-stable and metrics-mixed "
            "at 100 records per request with 128-request blocks",
        },
    }
    index["status"] = (
        measurement.STATUS_PASSED
        if aggregates and all(agg["status"] == measurement.STATUS_PASSED for agg in aggregates)
        else measurement.STATUS_FAILED
    )
    index["elapsed_s"] = 0.0
    path = measurement.write_result(output_dir / f"{run_id}.json", index)
    _ = measurement.publish_result_tree(path, report_dir)
    return index
