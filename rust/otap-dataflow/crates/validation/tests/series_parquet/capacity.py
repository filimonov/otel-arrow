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
import contextlib
import dataclasses
import functools
import json
import math
import multiprocessing
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import threading
import time

try:  # Imported as a package module by `python3 -m crates...`.
    from . import generator as requests_generator
    from . import measurement
    from . import performance
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import generator as requests_generator
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
# Where the senders run: the host's CPUs outside the harness's own affinity
# (the campaign pins the harness, engine and store to 0-7,16-23), one
# sender process per physical core.
PRODUCER_CPUS = "8-15,24-31"
SEARCH_CONNECTIONS = 256
FAN_IN_CONNECTIONS = (1, 8, 64, 256)
PRODUCER_TIMEOUT_S = 180.0
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
        "description": "the 8KiB-body variant of mixed-1k-hot; its 8 MB log requests "
        "exceed the receiver's default 4 MiB decoding limit, so it runs with 16 MiB",
        "max_decoding_message_size": "16MiB",
    },
    "mixed-1k-hot-churn1": {
        "workload": measurement.Workload(
            requests=1, records_per_request=RECORDS_PER_REQUEST, body_bytes=1024,
            series=10000, metrics_every=5, series_scope="record", churn_every=100,
        ),
        "first_index": 0,
        "description": "mixed-1k-hot with every hundredth record on a series no "
        "other record uses: 10k hot series and 1 percent deterministic churn",
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
    # The Alloy confirmation (alloy_capacity.py): Alloy, not the generator,
    # builds the requests; the request size is its minimum batch.
    "alloy-file": {
        "workload": measurement.Workload(
            requests=1, records_per_request=20000, body_bytes=100,
            series=1, metrics_every=10**9,
        ),
        "first_index": 0,
        "description": "Grafana Alloy tailing one file of 100-byte lines through the "
        "reference River config, batches of 20000 to 50000 records",
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
    the sustainable rate. A producer-limited rate bounds the bisection from
    above like an unsustainable one, since nothing above it can be offered,
    but the decision then names a lower bound, never a maximum.
    """
    sustainable = [rate for rate, verdict in trials if verdict == "sustainable"]
    unsustainable = [rate for rate, verdict in trials
                     if verdict in ("unsustainable", "producer_limited")]
    if not trials:
        return start
    if not unsustainable:
        if len(trials) >= doubling:
            return None
        return max(sustainable) * 2
    high = min(unsustainable)
    below = [rate for rate in sustainable if rate < high]
    if not below:
        # Halve until a rate passes, from wherever the search started.
        return high // 2 if high // 2 >= SEARCH_START_RECORDS_PER_S else None
    low = max(below)
    if (high - low) / low <= width:
        return None
    return int(round((low + high) / 2))


def search_decision(trials) -> dict:
    """The bracket a finished search establishes."""
    sustainable = [rate for rate, verdict in trials if verdict == "sustainable"]
    unsustainable = [rate for rate, verdict in trials if verdict == "unsustainable"]
    limited = [rate for rate, verdict in trials if verdict == "producer_limited"]
    bounds = unsustainable + limited
    ceiling = min(bounds) if bounds else None
    high = min(unsustainable) if unsustainable else None
    below = [rate for rate in sustainable if ceiling is None or rate < ceiling]
    low = max(below) if below else None
    producer_bound = ceiling is not None and ceiling in limited and ceiling not in unsustainable
    return {
        "sustainable_records_per_s": low,
        "unsustainable_records_per_s": high,
        "bracketed": low is not None and high is not None and not producer_bound,
        "bracket_width_ratio": (high - low) / low if low and high else None,
        "producer_limited_records_per_s": min(limited) if limited else None,
        # Rates measured both ways: the band where verdicts flip.
        "flip_rates_records_per_s": sorted(set(sustainable) & set(unsustainable)),
        "kind": (
            "lower_bound_producer_limited" if low is not None and producer_bound
            else "maximum" if low is not None and high is not None
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
                      late_unblocked_ratio, failed_requests, partial_requests,
                      engine_reasons=()) -> dict:
    """Whether one trial sustained its offered rate, and why not.

    Anything the engine refused, failed or could not keep up with decides
    first: a failed request, a partially rejected one, and the engine-side
    reasons a topology adds (`engine_reasons`: the durable buffer's growing
    write-ahead log, its ingest failures and permanently rejected bundles).
    The engine did not sustain the rate, whatever the producer did. Only
    without any of them does a trial whose sends fell behind their targets
    while in-flight slots were free measure the producer, and say nothing
    about the engine.
    """
    late = late_unblocked_ratio > PRODUCER_LATE_LIMIT_RATIO
    engine = []
    if failed_requests:
        engine.append(f"{failed_requests} requests failed")
    if partial_requests:
        engine.append(f"{partial_requests} requests partially rejected")
    engine.extend(engine_reasons)
    if not engine and late:
        return {
            "verdict": "producer_limited",
            "reasons": [
                f"{late_unblocked_ratio:.3%} of sends started over "
                f"{PRODUCER_LATE_S}s late with an in-flight slot free"
            ],
        }
    reasons = []
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
    reasons.extend(engine)
    if engine and late:
        reasons.append(f"{late_unblocked_ratio:.3%} of sends also started late with a slot free")
    return {"verdict": "unsustainable" if reasons else "sustainable", "reasons": reasons}


def judge_trial(*, offered, tail_durable, backlog_slope_records_per_s, late_unblocked_ratio,
                failed_requests, partial_requests, engine_reasons=(), stored_window=None,
                stored_floor=None) -> dict:
    """A trial's verdict: the stability rule, then the stored-rows floor.

    The values rows the exporter wrote over the interval corroborate the
    acknowledgements; falling short of the floor makes a sustainable trial
    unsustainable. A live trial and a stored one are judged by this one path.
    """
    verdict = stability_verdict(
        offered=offered, tail_durable=tail_durable,
        backlog_slope_records_per_s=backlog_slope_records_per_s,
        late_unblocked_ratio=late_unblocked_ratio, failed_requests=failed_requests,
        partial_requests=partial_requests, engine_reasons=engine_reasons,
    )
    if stored_window is not None and verdict["verdict"] == "sustainable" and \
            stored_window < stored_floor:
        verdict = {"verdict": "unsustainable", "reasons": [
            f"values rows written over the interval {stored_window:.0f} < "
            f"{stored_floor:.0f}, the offered rate less one window and one "
            f"reporting interval"
        ]}
    return verdict


def rejudge_stored(result) -> dict:
    """The verdict `judge_trial` gives a stored trial, from what it recorded.

    The producer's outcome counts give the failed and partial requests, the
    buffer view its engine-side reasons, and the metrics the durable tail,
    the backlog slope and the stored rows.
    """
    capacity = result["capacity"]
    metrics = result["metrics"]
    producer = capacity["producer"]
    outcomes = producer.get("outcomes") or {}
    offered = float(capacity["offered_records_per_s"])
    stored_rate = capacity.get("stored_rows_window_records_per_s")
    measure_s = (capacity["window_monotonic_ns"][1] - capacity["window_monotonic_ns"][0]) / 1e9
    return judge_trial(
        offered=offered, tail_durable=metrics["durable_tail_records_per_s"],
        backlog_slope_records_per_s=metrics["backlog_slope_ratio"] * offered,
        late_unblocked_ratio=producer["late_unblocked_ratio"],
        failed_requests=sum(outcomes.get(name, 0) for name in (
            measurement.OUTCOME_RETRYABLE, measurement.OUTCOME_PERMANENT,
            measurement.OUTCOME_LOCAL, "unanswered")),
        partial_requests=outcomes.get(measurement.OUTCOME_PARTIAL, 0),
        engine_reasons=(capacity.get("buffer") or {}).get("reasons", ()),
        stored_window=stored_rate * measure_s if stored_rate is not None else None,
        stored_floor=capacity.get("stored_rows_floor_records"),
    )


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


def confine_threads(cpus):
    """Confine every thread of this process to `cpus`.

    The affinity call binds one thread; threads the process already started
    (the gRPC runtime's among them) keep the mask they were born with.
    """
    for task in Path("/proc/self/task").iterdir():
        with contextlib.suppress(ProcessLookupError, FileNotFoundError):
            os.sched_setaffinity(int(task.name), set(cpus))


def _sender_process(pipe, config):
    """One producer process: connect, wait for the start, send, report."""
    try:
        confine_threads(config["cpus"])
        grpc = test_e2e.grpc
        source = requests_generator.TemplateRequests(
            measurement.Workload(**config["workload"]))
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
        confine_threads(config["cpus"])
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

        # Every template is built before the start, so none is built on
        # the schedule.
        for index in indexes[:2 * requests_generator.VARIANTS]:
            _ = source.request(index)
        generation_ns = 0
        pipe.send({"ready": True, "pid": os.getpid(), "connections": len(channels)})
        start_ns = pipe.recv()["start_ns"]
        cpu_before = os.times()
        faults_before = resource.getrusage(resource.RUSAGE_SELF).ru_majflt
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
            built = time.monotonic_ns()
            signal, body = source.request(index)
            generation_ns += time.monotonic_ns() - built
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
            "major_faults_count": (
                resource.getrusage(resource.RUSAGE_SELF).ru_majflt - faults_before),
            "generation_s": generation_ns / 1e9,
            "connections": len(channels),
            "in_flight": config["in_flight"],
        }
        Path(config["result_path"]).write_text(json.dumps(result), encoding="ascii")
        for channel in channels:
            channel.close()
        pipe.send({"done": True})
    except BaseException as error:  # noqa: BLE001 - reported to the parent
        with contextlib.suppress(Exception):
            pipe.send({"error": f"{type(error).__name__}: {error}"})
        raise


class SenderFleet:
    """The producer processes of one trial, started, released and reaped."""

    def __init__(self, *, target, workload, indexes, rate, connections,
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
                "cpus": list(cpus[number % len(cpus)]),
                "workload": workload.as_json(),
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
                {key: part.get(key) for key in ("pid", "count", "complete", "cpu_user_s",
                                            "cpu_system_s", "connections", "in_flight", "major_faults_count",
                                            "generation_s",
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
# The durable buffer's unlabelled metrics kept per worker, by extras key.
BUFFER = "processor.durable_buffer"
EXTRA_BUFFER_GAUGES = {
    "buffer.storage.bytes.used": (BUFFER, "storage.bytes.used"),
    "buffer.in.flight": (BUFFER, "in.flight"),
    "buffer.storage.bytes.cap": (BUFFER, "storage.bytes.cap"),
    "buffer.retries.scheduled": (BUFFER, "retries.scheduled"),
}
# Labelled metrics kept per worker by extras key: (metric set, metric,
# label), summed over the label's values.
EXTRA_LABELLED = {
    "rows.written": (EXPORTER, "rows.written", "dataset"),
    "files.written": (EXPORTER, "files.written", "dataset"),
    "series.emitted": (EXPORTER, "series.emitted", "reason"),
    "flushes": (EXPORTER, "flushes", "reason"),
    "nacks": (EXPORTER, "nacks", "error.type"),
    "accepted": ("receiver.otlp.requests", "accepted", "protocol"),
    "rejected": ("receiver.otlp.requests", "rejected", "error.type"),
    "buffer.items.queued": (f"{BUFFER}.items", "queued", "signal"),
    "buffer.items.produced": (f"{BUFFER}.items", "produced", "signal"),
    "buffer.items.consumed": (f"{BUFFER}.items", "consumed", "signal"),
    "buffer.items.rejected": (f"{BUFFER}.items", "rejected", "signal"),
    "buffer.ingest.failures": (f"{BUFFER}.ingest", "failures", "failure"),
    "buffer.bundles.resolved": (f"{BUFFER}.bundles", "resolved", "outcome"),
    "buffer.loss.items": (f"{BUFFER}.loss", "items", "reason"),
    "flush.failures.by_class": (EXPORTER, "flush.failures", "error.type"),
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
        for name, (metric_set, metric) in EXTRA_BUFFER_GAUGES.items():
            entities = worker["metrics"].get(f"{metric_set}:{metric}") or {}
            values = [value for value in entities.values() if isinstance(value, (int, float))]
            if values:
                entry[name] = values[0]
        for name, (metric_set, metric, label) in EXTRA_LABELLED.items():
            entities = worker["labelled"].get(f"{metric_set}:{metric}") or {}
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
                        # Diagnostic only; never an edge of the band.
                        "interval_resident_max_bytes": max(
                            entry["resident_bytes"] for entry in prints),
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
# One trial
# --------------------------------------------------------------------------


def producer_placement(spec, sibling_groups, allocation) -> list:
    """The producer's physical cores, one CPU set of SMT siblings each.

    `spec` is a kernel CPU list, or `allocated` to keep the producer on the
    cores the role allocation gave it. Each sender process is confined to
    one of the returned sets, so a sender owns a whole physical core. The
    CPUs may lie outside the harness's own affinity; none may share a
    physical core with another role.
    """
    if spec in (None, "", "allocated"):
        return []
    try:
        from . import host_monitor
    except ImportError:
        import host_monitor
    wanted = set(host_monitor.parse_core_list(str(spec)))
    taken = {int(core) for role, cores in allocation.items() if role != "producer"
             for core in cores}
    groups = []
    for group in sibling_groups:
        members = sorted(set(int(core) for core in group) & wanted)
        if not members:
            continue
        if set(int(core) for core in group) & taken:
            raise AssertionError(
                f"producer CPUs {members} share physical core {list(group)} with another role"
            )
        groups.append(members)
    missing = wanted - {core for group in groups for core in group}
    if missing:
        raise AssertionError(f"producer CPUs {sorted(missing)} are not on this host")
    return groups


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

    With allocator prints the reconciliation reads the prints, each paired with the RSS
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


# The block devices a trial's disk traffic is read from.
DISK_DEVICES = ("nvme0n1", "nvme1n1", "dm-0", "dm-1")


def diskstats() -> dict:
    """/proc/diskstats counters of the named devices, by device."""
    found = {}
    for line in Path("/proc/diskstats").read_text().splitlines():
        fields = line.split()
        if len(fields) >= 20 and fields[2] in DISK_DEVICES:
            values = [int(value) for value in fields[3:]]
            found[fields[2]] = {
                "writes": values[4], "sectors_written": values[6], "write_ms": values[7],
                "flushes": values[14], "flush_ms": values[15],
            }
    return found


def disk_view(start, end, window_ns) -> dict:
    """Writes, bytes written and the mean write and flush latency per device."""
    if not start or not end:
        return {}
    seconds = window_ns / 1e9
    view = {}
    for device, after in end.items():
        before = start.get(device)
        if not before:
            continue
        delta = {key: after[key] - before[key] for key in after}
        view[device] = {
            "write_bytes_per_s": delta["sectors_written"] * 512 / seconds,
            "writes_per_s": delta["writes"] / seconds,
            "write_await_ms": delta["write_ms"] / delta["writes"] if delta["writes"] else None,
            "flushes_per_s": delta["flushes"] / seconds,
            "flush_await_ms": delta["flush_ms"] / delta["flushes"] if delta["flushes"] else None,
        }
    return view


def buffer_view(samples, window, trial, stats) -> dict:
    """The durable buffer over the measured interval, and why it did not keep up.

    The buffer acknowledges the producer once a request is in its write-
    ahead log, so a buffered trial is sustainable only when the log stays
    bounded -- it grows over the interval by less than one window of the
    offered bytes -- and the buffer refused nothing: no ingest failure
    (backpressure at its size cap) and no bundle the exporter permanently
    rejected.
    """
    usable = [s for s in samples if s.get("extras")]
    inside = [s for s in usable if window[0] <= s["monotonic_ns"] <= window[1]]
    if not inside:
        return {"reasons": ["no buffer sample inside the measured interval"]}
    wal = [(s["monotonic_ns"], extras_total(s, "buffer.storage.bytes.used")) for s in inside]
    start = value_at(samples, window[0], "buffer.storage.bytes.used") or wal[0][1]
    end = value_at(samples, window[1], "buffer.storage.bytes.used") or wal[-1][1]
    slope = backlog_slope([((t - window[0]) / 1e9, b) for t, b in wal])
    measure_s = (window[1] - window[0]) / 1e9
    bytes_per_record = (stats["wire_bytes_total"] / max(1, stats["requests_scheduled_count"])
                        / CAPACITY_WORKLOADS[trial["workload_id"]]["workload"].records_per_request)
    allowance = trial["rate"] * trial["interval_s"] * bytes_per_record

    def delta(name, label=None):
        a = value_at(samples, window[0], name, label) or 0
        b = value_at(samples, window[1], name, label) or 0
        return b - a

    failures = delta("buffer.ingest.failures")
    rejected = delta("buffer.bundles.resolved", "permanently_rejected")
    reasons = []
    if slope * measure_s > allowance:
        reasons.append(f"write-ahead log grows {slope * measure_s:.0f} bytes over the interval, "
                       f"more than one window of offered bytes ({allowance:.0f})")
    if failures:
        reasons.append(f"{failures:.0f} buffer ingest failures")
    if rejected:
        reasons.append(f"{rejected:.0f} bundles permanently rejected downstream")
    return {
        "wal_bytes_start": start,
        "wal_bytes_max": max(b for _t, b in wal),
        "wal_bytes_end": end,
        "wal_slope_bytes_per_s": slope,
        "wal_growth_allowance_bytes": allowance,
        "in_flight_max": max(extras_total(s, "buffer.in.flight") for s in inside),
        "items_queued_max": max(extras_total(s, "buffer.items.queued") for s in inside),
        "items_queued_slope_per_s": backlog_slope([
            ((s["monotonic_ns"] - window[0]) / 1e9, extras_total(s, "buffer.items.queued"))
            for s in inside]),
        "ingest_failures_count": failures,
        "bundles_permanently_rejected_count": rejected,
        "bundles_acked_count": delta("buffer.bundles.resolved", "acked"),
        "bundles_deferred_count": delta("buffer.bundles.resolved", "deferred"),
        "placement": "a node of every worker pipeline, on the worker's own core; its "
        "write-ahead log is in the trial's engine directory on the harness disk",
        "reasons": reasons,
    }


def request_visibility(db_root, rpr, objects) -> dict:
    """When each stored request became visible, in epoch nanoseconds.

    A request is visible when the last object holding any of its records
    has completed.
    """
    import duckdb

    completion = {key: t for key, _size, t in objects}
    visible = {}
    with duckdb.connect() as db:
        for signal in ("logs", "metrics"):
            glob = Path(db_root) / f"v=1/signal={signal}/dataset=values/**/*.parquet"
            if not any(Path(db_root).glob(f"v=1/signal={signal}/dataset=values/**/*.parquet")):
                continue
            rows = db.execute(
                f"SELECT DISTINCT filename, {requests_generator._seq('time_unix_nano')} // {rpr} "
                f"FROM read_parquet({test_e2e.sql_string(glob)}, filename=true, "
                "hive_partitioning=false)"
            ).fetchall()
            for filename, request in rows:
                key = str(Path(filename).relative_to(db_root))
                at = completion.get(key)
                if at is not None:
                    visible[int(request)] = max(visible.get(int(request), 0), at)
    return visible


def freshness(visible, sends, epoch_offset_ns) -> dict:
    """Acknowledgement to object-visible lag per acknowledged request.

    The lag is the request's visibility (`request_visibility`) less the
    producer's acknowledgement. Negative means the acknowledgement came
    after the object, as it does in the strict topology.
    """
    acked = {index: finish for index, finish, code in
             zip(sends["indexes"], sends["finish"], sends["code"]) if code == 0}
    lags = sorted((visible[index] - epoch_offset_ns - finish) / 1e9
                  for index, finish in acked.items() if index in visible)
    if not lags:
        return {}
    return {
        "requests_count": len(lags),
        "lag_p50_s": measurement.percentile(lags, 0.50),
        "lag_p95_s": measurement.percentile(lags, 0.95),
        "lag_p99_s": measurement.percentile(lags, 0.99),
        "lag_max_s": lags[-1],
        "completion_clock": "file modification time (local) or LastModified (S3, 1 s)",
    }


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
    grpc = {"max_concurrent_requests": capacity}
    # A workload whose requests exceed the receiver's default decoding
    # limit names the limit it needs; a trial may name one of its own.
    decoding = trial.get("max_decoding_message_size") or CAPACITY_WORKLOADS.get(
        trial["workload_id"], {}).get("max_decoding_message_size")
    if decoding:
        grpc["max_decoding_message_size"] = decoding
    merge = {
        "receiver": {"protocols": {"grpc": grpc}},
        "pipeline_policies": {"channel_capacity": {"pdata": capacity}},
    }
    if trial.get("buffer_config") and trial["topology"] == "buffered":
        merge[test_e2e.BUFFER_NODE] = dict(trial["buffer_config"])
    overrides = None
    if trial["topology"] != "noop":
        overrides = {
            "upload": {"part_bytes": "8MiB", "concurrency": trial["upload_concurrency"],
                       "abort_timeout": "5s"},
        }
        if plan["store"] is not None:
            overrides["retry"] = test_e2e.S3_RETRY
    storage = dict(plan["store"].storage) if plan["store"] is not None else None
    if trial.get("local_store_dir") and plan["store"] is None:
        # The local store needs its base directory to exist.
        base = Path(trial["local_store_dir"]) / root.parent.name
        base.mkdir(parents=True, exist_ok=True)
        storage = {"file": {"base_uri": str(base)}}
    buffer_path = None
    if trial["topology"] == "buffered":
        buffer_path = (Path(trial["wal_dir"]) / root.parent.name) if trial.get("wal_dir") \
            else root / "buffer"
    return {
        "storage": storage,
        "overrides": overrides,
        "interval": f"{trial['interval_s']}s",
        "cores": list(plan["cores"]),
        "merge": merge,
        "binary": Path(plan["provenance"]["build"]["binary"]),
        "topology": trial["topology"],
        "buffer_path": buffer_path,
    }


def trial_experiment(plan, trial, spec, result, run_dir, controls, extension=None):
    """One trial: engine, producer fleet, measured interval, drain, oracle.

    An `extension` observes a longer run: `start(engine, fleet, window)` once
    the producers are released and `stop()` after the drain, then
    `settle_trial` hands it the read-back.
    """
    command = _command()
    run_dir = Path(run_dir)
    controls.coverage_gaps_hard = True
    controls.allocate(plan["allocation"])
    controls.register("harness", os.getpid())
    if plan.get("store_pid"):
        controls.register("store", plan["store_pid"], plan["allocation"].get("store", []))
    result["environment"]["build"] = plan["provenance"]["build"]
    result["environment"]["git"] = plan["provenance"]["git"]
    result["environment"]["producer"] = {
        "cpus": plan["allocation"]["producer"],
        "processes": plan["producer_processes"],
        "cpu_set_per_process": plan["producer_cpu_sets"],
    }
    result["ephemeral_values"] = dict(plan["ephemeral"])
    workload = spec.workload
    first = CAPACITY_WORKLOADS[trial["workload_id"]]["first_index"]
    indexes = list(range(first, workload.requests))
    # Every trial sends the same request indexes, so objects a trial that
    # was aborted left behind would read as duplicates of this one's.
    if plan["store"] is not None:
        result["capacity_store_cleared_objects_count"] = performance.clear_store(plan["store"])
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
            target=f"127.0.0.1:{engine.grpc_port}", workload=workload,
            indexes=indexes, rate=trial["rate"], connections=trial["connections"],
            processes=plan["producer_processes"], in_flight=spec.max_in_flight,
            cpus=plan["producer_cpu_sets"], directory=run_dir / "senders",
        )
        observed["senders_ready"] = fleet.ready()
        for number, pid in enumerate(fleet.pids()):
            controls.register(f"producer_{number}", pid, plan["producer_cpu_sets"][
                number % len(plan["producer_cpu_sets"])])
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
        if extension is not None:
            extension.start(engine=engine, fleet=fleet, window=window)
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
                "diskstats": diskstats(),
            }
            controls.raise_if_invalid()
        sends = fleet.finish(PRODUCER_TIMEOUT_S + 120)
        observed["last_response_ns"] = max(sends["finish"], default=0)
        controls.raise_if_invalid()
        _ = phase.drained()
        if extension is not None:
            extension.stop()
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
        if extension is not None:
            extension.stop()
        if phase is not None and phase.sampler is not None:
            phase.sampler.stop()
        if fleet is not None:
            fleet.close()
        engine.close()
    settle_trial(plan, trial, spec, result, run_dir, phase, sends, readings, window,
                 worker_tids, observed, start_ns, engine, extension=extension)


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
                 worker_tids, observed, start_ns, engine, extension=None):
    """Oracle, metrics, verdict and checks of one trial.

    An `extension` moves the objects out of the store itself
    (`download(store, directory)`) and completes the result (`finalize`)
    with the stored requests' visibility.
    """
    command = _command()
    workload = spec.workload
    source = requests_generator.TemplateRequests(workload)
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
        wire_bytes_of=source.size,
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
    buffer = buffer_view(samples, window, trial, stats) if trial["topology"] == "buffered" \
        else None
    verdict = judge_trial(
        offered=trial["rate"], tail_durable=tail_durable,
        backlog_slope_records_per_s=slope,
        late_unblocked_ratio=stats["late_unblocked_ratio"],
        failed_requests=failed, partial_requests=partial,
        engine_reasons=(buffer or {}).get("reasons", ()),
        stored_window=stored_window, stored_floor=stored_floor,
    )
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
    # A local store writes where its configuration points; a trial may move it.
    storage = (engine.config["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]
               .get("config", {}).get("storage") or {})
    data_dir = Path((storage.get("file") or {}).get("base_uri") or engine.data)
    objects = [] if trial["topology"] == "noop" else object_inventory(plan, store, data_dir)
    uploads = incomplete_uploads(store) if trial["topology"] != "noop" else {}
    local = data_dir if store is None else run_dir / "store"
    oracle = None
    duplication = {}
    downloaded_bytes = None
    acked = [index for index, code in zip(sends["indexes"], sends["code"]) if code == 0]
    failed = [index for index, code in zip(sends["indexes"], sends["code"]) if code != 0]
    oracle_s = None
    fresh = {}
    visible = {}
    download_s = None
    try:
        if trial["topology"] != "noop":
            if store is not None:
                started = time.monotonic_ns()
                if extension is not None:
                    extension.download(store, local)
                else:
                    store.download(local)
                downloaded_bytes = sum(path.stat().st_size for path in local.rglob("*.parquet"))
                download_s = (time.monotonic_ns() - started) / 1e9
            started = time.monotonic_ns()
            oracle = measurement.run_pinned(
                plan["oracle_cores"], requests_generator.aggregate_oracle, local, source,
                acked, failed,
            )
            oracle_s = (time.monotonic_ns() - started) / 1e9
            visible = measurement.run_pinned(
                plan["oracle_cores"], request_visibility, local, rpr, objects,
            )
            fresh = freshness(visible, sends, time.time_ns() - time.monotonic_ns())
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
        "oracle_s": oracle_s,
        "freshness": fresh,
        "disk": disk_view(readings["start"].get("diskstats"), readings["end"].get("diskstats"),
                          window_ns),
        "wal_dir": trial.get("wal_dir"),
        "local_store_dir": trial.get("local_store_dir"),
        "buffer": buffer,
        "generator": source.as_json(),
        "acknowledged_request_ranges": request_ranges(acked),
        "failed_request_ranges": request_ranges(failed),
        "rss_residual_count": len(residuals),
        "rss_heap_term": heap,
        "rss_residual_range_bytes": [
            min((r["residual_bytes"] for r in residuals), default=None),
            max((r["residual_bytes"] for r in residuals), default=None),
        ],
    }
    result["observations"] = {
        "phase": {k: v for k, v in summary.items() if k != "residuals"},
        "residual_excursions": measurement.residual_excursions(
            residuals, list(getattr(phase.sampler, "pairs", ())),
            max((s["process_rss_bytes"] for s in samples), default=0)),
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
        passed = bool(oracle and oracle["passed"]) and (
            downloaded_bytes is None or downloaded_bytes == object_bytes
        )
        checks.append(measurement.check(
            "delivery", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED,
            f"acked {stats['outcomes']}; oracle problems {oracle and oracle.get('problems')}; "
            f"listed {object_bytes} bytes, "
            f"downloaded {downloaded_bytes}",
        ))
        result["capacity"]["oracle"] = {
            key: oracle[key] for key in (
                "passed", "problems", "signals", "files", "series_cardinality", "readers",
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
    if extension is not None:
        extension.finalize(result, {
            "samples": samples, "sends": sends, "objects": objects, "visible": visible,
            "window": window, "observed": observed, "stats": stats, "oracle": oracle,
            "residuals": residuals, "start_ns": start_ns, "trial": trial, "spec": spec,
            "epoch_offset_ns": epoch_offset_ns, "buffer": buffer,
            "download_s": download_s, "oracle_s": oracle_s, "memory": memory,
            "source": source, "summary": summary,
            "pairs": list(getattr(phase.sampler, "pairs", ())),
        })


def run_trial(plan, trial, output_dir, report_dir, experiment_fn=None, *, spec_fn=None,
              evaluate=False):
    """One trial under the host controls; its failure is recorded, not raised.

    `spec_fn` builds the run's spec in place of `trial_spec`; `evaluate`
    applies the baseline policy to this run alone, for a run that is its own
    family.
    """
    command = _command()
    spec_fn = spec_fn or trial_spec
    spec = spec_fn(plan, trial)
    run_dir = Path(output_dir) / spec.run_id
    experiment_fn = experiment_fn or trial_experiment

    def experiment(spec, result, directory, controls):
        experiment_fn(plan, trial, spec, result, directory, controls)

    for attempt in range(1, performance.BUILD_RETRIES + 2):
        if measurement.build_activity():
            _ = performance.wait_for_quiet_host(deadline_s=3600)
        try:
            result = command.run_case(
                spec, run_dir, experiment=experiment, report_dir=report_dir,
                evaluate=evaluate, lease_wait_s=plan.get("lease_wait_s", 0.0),
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
        spec = spec_fn(plan, trial)
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


def request_ranges(indexes) -> list:
    """Sorted request indexes as [first, last] runs."""
    runs = []
    for index in sorted(indexes):
        if runs and index == runs[-1][1] + 1:
            runs[-1][1] = index
        else:
            runs.append([index, index])
    return runs


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


def next_ordinal_factory(*directories, prefix="capacity-"):
    """A counter past every published or local run ordinal of the runs
    whose file names start with `prefix`."""
    highest = 0
    for directory in directories:
        directory = Path(directory)
        if not directory.is_dir():
            continue
        for path in directory.glob(f"{prefix}*-r[0-9][0-9][0-9].json"):
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
               warmup_s=None, measure_s=MEASURE_S, jemalloc_stats=True,
               wal_dir=None, local_store_dir=None, buffer_config=None) -> dict:
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
        "wal_dir": wal_dir,
        "local_store_dir": local_store_dir,
        "buffer_config": buffer_config,
        "ordinal": plan["next_ordinal"](),
    }


def execute(plan, state, trial, output_dir, report_dir, cell, experiment=None):
    """Run one trial, record it in the family state and return its result."""
    started = time.monotonic()
    result = run_trial(plan, trial, output_dir, report_dir, experiment)
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
    producer_groups = producer_placement(
        options.get("producer_cpus", PRODUCER_CPUS), topology["sibling_groups"], allocation
    )
    if producer_groups:
        allocation["producer"] = sorted(core for group in producer_groups for core in group)
    plan = {
        "store_kind": store_kind,
        "store": None,
        "store_pid": None,
        "cores": list(cores),
        "allocation": allocation,
        "oracle_cores": performance.oracle_cores(allocation, topology["sibling_groups"]),
        "producer_processes": int(options.get(
            "producer_processes", len(producer_groups) or PRODUCER_PROCESSES)),
        "producer_cpu_sets": producer_groups or [allocation["producer"]],
        "provenance": command.prepare_build(),
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
    """Nothing a cell's trials share outlives the cell but its evidence."""
    plan["store"] = None


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
# window. `default_window` is the shipped configuration as it ships;
# `buffered_default_window` is the buffered topology with it.
SEARCH_PURPOSES = {"shipped": "search", "raised": "search_raised",
                   "default_window": "search_default_window",
                   "buffered": "search_buffered",
                   "buffered_default_window": "search_buffered_default_window"}
REPETITION_PURPOSES = {"shipped": "repetition", "raised": "repetition_raised",
                       "default_window": "repetition_default_window",
                       "buffered": "repetition_buffered",
                       "buffered_default_window": "repetition_buffered_default_window"}
VARIANT_CAPACITY = {"shipped": SHIPPED_RECEIVER_CAPACITY, "raised": RAISED_RECEIVER_CAPACITY,
                    "default_window": SHIPPED_RECEIVER_CAPACITY,
                    "buffered": SHIPPED_RECEIVER_CAPACITY,
                    "buffered_default_window": SHIPPED_RECEIVER_CAPACITY}
VARIANT_INTERVAL_S = {"shipped": SEARCH_INTERVAL_S, "raised": SEARCH_INTERVAL_S,
                      "default_window": DEFAULT_INTERVAL_S, "buffered": SEARCH_INTERVAL_S,
                      "buffered_default_window": DEFAULT_INTERVAL_S}
VARIANT_TOPOLOGY = {"shipped": "strict", "raised": "strict", "default_window": "strict",
                    "buffered": "buffered", "buffered_default_window": "buffered"}


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
    floor = ((options.get("search_floor") or {}).get(cell) or {}).get(variant)
    start = int(floor or SEARCH_START_RECORDS_PER_S)
    shipped = {"unsustainable_records_per_s": None}
    if variant == "raised" and state.trials(cell, SEARCH_PURPOSES["shipped"], workload_id):
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
            # A rate the host could not measure validly (the overload it
            # offers starves the monitor) steers the bisection from above;
            # it never becomes the decision's unsustainable bound.
            steering = decision["trials"] + [
                (int(rate), "unsustainable")
                for rate in (options.get("unmeasurable_above") or {}).get(cell, ())
            ]
            rate, trial_purpose = next_search_rate(steering, start=start), purpose
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
                                            interval_s=VARIANT_INTERVAL_S[variant],
                                            topology=VARIANT_TOPOLOGY[variant]),
                    output_dir, report_dir, cell)


def step_search_raised(plan, state, output_dir, report_dir, cell, options):
    """The search with the receiver capacity raised, from the shipped bracket."""
    step_search(plan, state, output_dir, report_dir, cell, options, variant="raised")


def step_search_buffered(plan, state, output_dir, report_dir, cell, options):
    """The search of the buffered topology at the shipped receiver slots."""
    step_search(plan, state, output_dir, report_dir, cell, options, variant="buffered")


def step_search_buffered_default_window(plan, state, output_dir, report_dir, cell, options):
    """The search of the buffered topology with the shipped 15 s window."""
    step_search(plan, state, output_dir, report_dir, cell, options,
                variant="buffered_default_window")


def step_search_default_window(plan, state, output_dir, report_dir, cell, options):
    """The search in the configuration as it ships: 15 s windows, 128 slots."""
    step_search(plan, state, output_dir, report_dir, cell, options, variant="default_window")


def default_window_rate(state, cell):
    """The rate the upload-concurrency comparison runs at with the 15 s
    window: the default-window search's winner.

    The one-second search's winner is no rate for a 15 s window: the strict
    admission ceiling falls with the hold time (about 8k records/s per
    worker at 128 slots), so a trial there only measures an overload and its
    producer never finishes. Without a default-window search there is no
    rate to confirm.
    """
    if not state.trials(cell, SEARCH_PURPOSES["default_window"]):
        return None
    return winning(state, cell, variant="default_window")["sustainable_records_per_s"]


def step_confirm_default_window(plan, state, output_dir, report_dir, cell, options):
    """Upload concurrency 2 and 1 with the shipped 15 s window at its own
    ceiling, and concurrency 1 at the cell's one-second strict ceiling,
    where the search ran concurrency 2."""
    rate = default_window_rate(state, cell)
    if rate is not None:
        for concurrency in options.get("upload_concurrencies", (2, 1)):
            _ = execute(plan, state, make_trial(
                plan, rate=rate, purpose=f"default_window_upload{concurrency}",
                interval_s=DEFAULT_INTERVAL_S, upload_concurrency=concurrency,
            ), output_dir, report_dir, cell)
    base, capacity = ceiling(state, cell)
    if base is not None:
        _ = execute(plan, state, make_trial(
            plan, rate=base, purpose="ceiling_upload1", upload_concurrency=1,
            receiver_capacity=capacity,
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


def step_alloy(plan, state, output_dir, report_dir, cell, options):
    """The Alloy-as-producer confirmation; see `alloy_capacity`."""
    try:
        from . import alloy_capacity
    except ImportError:
        import alloy_capacity
    alloy_capacity.step_alloy(plan, state, output_dir, report_dir, cell, options)


STEPS = {
    "alloy": step_alloy,
    "trial": step_trial,
    "calibrate": step_calibrate,
    "search": step_search,
    "default_window": step_confirm_default_window,
    "buffered": step_buffered,
    "fan_in": step_fan_in,
    "search_raised": step_search_raised,
    "stats_off": step_stats_off,
    "search_default_window": step_search_default_window,
    "search_buffered": step_search_buffered,
    "search_buffered_default_window": step_search_buffered_default_window,
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
    run_id = (f"{case}-{VARIANT_TOPOLOGY[variant]}-{store_kind}-c{cores}"
              f"-w{VARIANT_INTERVAL_S[variant]}"
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


REJUDGEMENT_RULES = (
    "every trial judged again from what it stored: the verdict through `judge_trial` "
    "(engine-side failures -- failed, refused and partially rejected requests, the "
    "durable buffer's growing log, ingest failures and rejected bundles -- outrank "
    "producer lateness), the generator read-back's aggregate equalities (stored rows "
    "less stored failed-request rows equal the acknowledged records, every stored "
    "record once, the sequence sum when no failed request's rows were stored), and "
    "the Alloy read-back through `judge_read_back` (a duplicate fails it)"
)


def stored_oracle_equalities(result) -> tuple:
    """The aggregate equalities a stored generator read-back can still be held to.

    The stored coverage keeps the counts and the global sequence sum; the sum
    is checkable only when no failed request's rows were stored (their
    sequences are not in the record), which is said.
    """
    oracle = (result.get("capacity") or {}).get("oracle") or {}
    problems, unchecked = [], []
    for signal, coverage in (oracle.get("signals") or {}).items():
        expected = coverage["expected_record_count"]
        stored = coverage["stored_record_count"]
        failed_stored = coverage["stored_failed_records_count"]
        if stored - failed_stored != expected:
            problems.append(f"{signal}: rows beyond the acknowledged and failed records "
                            f"{stored - failed_stored}, expected {expected}")
        if coverage["distinct_record_count"] != stored:
            problems.append(f"{signal}: distinct stored records "
                            f"{coverage['distinct_record_count']} of {stored}")
        if failed_stored == 0:
            if coverage["seq_sum"] != coverage["expected_seq_sum_of_acknowledged"]:
                problems.append(f"{signal}: sequence sum {coverage['seq_sum']}, expected "
                                f"{coverage['expected_seq_sum_of_acknowledged']}")
        else:
            unchecked.append(f"{signal}: sequence sum not checkable, {failed_stored} "
                             f"failed-request records stored")
    return problems, unchecked


def rejudgement(state, output_dir, entries) -> dict:
    """Every trial of `entries` judged again by the current rules, and the
    search decisions those verdicts give next to the recorded ones."""
    try:
        from . import alloy_capacity
    except ImportError:
        import alloy_capacity
    output_dir = Path(output_dir)
    verdict_changes, oracle_changes, unchecked = [], [], []
    rejudged = {}
    for entry in entries:
        result = load_result(output_dir, entry["run_id"])
        block = result.get("capacity")
        if not block or "producer" not in block and entry["workload_id"] != \
                alloy_capacity.WORKLOAD_ID:
            unchecked.append({"run_id": entry["run_id"],
                              "reason": f"no stored trial figures ({result.get('status')})"})
            continue
        if entry["workload_id"] == alloy_capacity.WORKLOAD_ID:
            oracle = block.get("oracle") or {}
            if not oracle.get("files"):
                unchecked.append({"run_id": entry["run_id"], "reason": "nothing was stored"})
                continue
            again = alloy_capacity.rejudge_stored_read_back(oracle)
            if again["passed"] != oracle.get("passed"):
                oracle_changes.append({"run_id": entry["run_id"], "recorded": oracle.get("passed"),
                                       "rejudged": again["passed"],
                                       "problems": again["problems"]})
            continue
        verdict = rejudge_stored(result)
        if verdict["verdict"] != block["verdict"]:
            verdict_changes.append({"run_id": entry["run_id"], "purpose": entry["purpose"],
                                    "recorded": block["verdict"], "rejudged": verdict["verdict"],
                                    "reasons": verdict["reasons"]})
        problems, not_checked = stored_oracle_equalities(result)
        unchecked += [{"run_id": entry["run_id"], "reason": reason} for reason in not_checked]
        oracle = block.get("oracle")
        status = result["status"]
        if oracle is not None and problems:
            status = measurement.STATUS_FAILED
            if oracle.get("passed"):
                oracle_changes.append({"run_id": entry["run_id"], "recorded": True,
                                       "rejudged": False, "problems": problems})
        rejudged[entry["run_id"]] = trial_verdict(
            dict(result, status=status, capacity=dict(block, verdict=verdict["verdict"])))
    again = FamilyState.__new__(FamilyState)
    again.path = None
    again.document = {"trials": [
        dict(entry, verdict=rejudged.get(entry["run_id"], entry["verdict"]))
        for entry in state.document["trials"]]}
    keys = ("sustainable_records_per_s", "unsustainable_records_per_s", "kind",
            "flip_rates_records_per_s")
    decisions = []
    for cell in sorted({entry["cell"] for entry in entries}):
        for variant in SEARCH_PURPOSES:
            if not state.trials(cell, SEARCH_PURPOSES[variant]):
                continue
            recorded = winning(state, cell, variant=variant)
            now = winning(again, cell, variant=variant)
            if any(recorded.get(key) != now.get(key) for key in keys):
                decisions.append({"cell": cell, "variant": variant,
                                  "recorded": {key: recorded.get(key) for key in keys},
                                  "rejudged": {key: now.get(key) for key in keys}})
    return {
        "rules": REJUDGEMENT_RULES,
        "trials_count": len(entries),
        "verdict_changes": verdict_changes,
        "oracle_changes": oracle_changes,
        "decision_changes": decisions,
        "unchecked": unchecked,
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
            "search_floors": options.get("search_floor") or {},
            "search_floor_rule": "a cell after the first may start at half of min(the "
            "one-worker ceiling on the same store times the workers, the shipped-slot "
            "formula ceiling), rounded down to the doubling grid; an unsustainable first "
            "trial halves until one passes",
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
    index["capacity"]["rejudgement"] = rejudgement(state, output_dir, entries)
    index["status"] = (
        measurement.STATUS_PASSED
        if aggregates and all(agg["status"] == measurement.STATUS_PASSED for agg in aggregates)
        else measurement.STATUS_FAILED
    )
    # The index this one replaces stays published, as an immutable child.
    previous = measurement.archive_published_index(f"{run_id}.json", output_dir, report_dir)
    index["child_indexes"] = [previous] if previous else []
    index["elapsed_s"] = 0.0
    path = measurement.write_result(output_dir / f"{run_id}.json", index)
    _ = measurement.publish_result_tree(path, report_dir)
    return index
