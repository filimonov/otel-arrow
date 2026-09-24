# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Thirty-minute soaks of both topologies, and the short soaks the PR lane runs.

A soak is one capacity trial held for thirty minutes at a fraction of a
measured ceiling and summarized every second; a PR-tier soak is one minute
of forced rotations through a store outage. See the harness README, "Soak".
"""
import bisect
import concurrent.futures
import dataclasses
import functools
import heapq
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import sys
import threading
import time

try:  # Imported as a package module by `python3 -m crates...`.
    from . import capacity
    from . import measurement
    from . import performance
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import capacity
    import measurement
    import performance

test_e2e = measurement.test_e2e

# --------------------------------------------------------------------------
# The soak's conditions
# --------------------------------------------------------------------------

SOAK_INPUT_S = 1800
# The fraction of a measured ceiling a soak offers.
SOAK_FRACTION = 0.7
SOAK_WORKLOAD = "mixed-1k-hot-churn1"
SOAK_INTERVAL_S = capacity.DEFAULT_INTERVAL_S
# At least two windows plus five seconds, as every capacity trial.
SOAK_WARMUP_S = 2 * SOAK_INTERVAL_S + 5
SAMPLE_COVERAGE_FLOOR = 0.99
# The span of the final-rate and of the first and last medians.
EDGE_WINDOW_S = 300
SIDE_PERIOD_S = 1.0
LISTING_PERIOD_S = 10.0
WAL_PERIOD_S = 5.0
MOVE_THREADS = 8
# Where the buffered bracket starts: the doubling grid below the one-worker
# buffered rate Task 5 could not sustain at 149.6k.
BRACKET_FLOOR_RECORDS_PER_S = 64_000
ARCHIVE_DIR_DEFAULT = measurement.REPO_ROOT / ".measurement-artifacts" / "soak"
STATE_FILE = "soak-state.json"
CELL = "minio-c1"

# The two soaks: topology, store, workers, receiver slots per worker, and
# where the ceiling their rate is a fraction of comes from.
SOAKS = {
    "soak-strict": {
        "topology": "strict", "store": "minio", "core_count": 1,
        "receiver_capacity": capacity.RAISED_RECEIVER_CAPACITY,
        "ceiling": {"index": "capacity-minio.json", "cell": CELL, "variant": "raised"},
    },
    "soak-buffered": {
        "topology": "buffered", "store": "minio", "core_count": 1,
        "receiver_capacity": capacity.SHIPPED_RECEIVER_CAPACITY,
        "ceiling": {"bracket": "buffered_default_window", "cell": CELL},
    },
}

# The correctness counters every soak reports, each required to be zero.
ZERO_METRICS = (
    "missing_acked_records", "missing_intended_records", "descriptor_violations_count",
    "reader_disagreements_count", "permanent_rejections_count", "buffer_loss_records",
)


def published_ceiling(index, cell, variant) -> dict:
    """A searched ceiling from a committed capacity index, and where it is."""
    path = measurement.REPORT_DIR / index
    document = json.loads(path.read_text(encoding="ascii"))
    entry = document["capacity"]["cells"][cell][variant]
    decision = entry["decision"]
    if decision.get("kind") != "maximum" or entry.get("aggregate_status") != "passed":
        raise AssertionError(
            f"{index} {cell} {variant} is not a passed maximum: "
            f"{decision.get('kind')} {entry.get('aggregate_status')}"
        )
    return {
        "records_per_s": int(decision["sustainable_records_per_s"]),
        "index": index, "cell": cell, "variant": variant,
        "aggregate": entry["aggregate"],
        "unsustainable_records_per_s": decision["unsustainable_records_per_s"],
    }


def cohorts(rate, records_per_request, warmup_s, input_s) -> tuple:
    """Requests of the warm-up cohort and of the measured cohort."""
    return (math.ceil(rate * warmup_s / records_per_request),
            math.ceil(rate * input_s / records_per_request))


def soak_trial(plan, case, rate, *, input_s, ceiling) -> dict:
    """A capacity trial that is a soak: fixed rate, one warm-up, `input_s`."""
    config = SOAKS[case]
    rpr = capacity.RECORDS_PER_REQUEST
    warm, measured = cohorts(rate, rpr, SOAK_WARMUP_S, input_s)
    trial = capacity.make_trial(
        plan, workload_id=SOAK_WORKLOAD, rate=rate, purpose=case,
        topology=config["topology"], interval_s=SOAK_INTERVAL_S,
        receiver_capacity=config["receiver_capacity"],
        warmup_s=warm * rpr / int(rate), measure_s=measured * rpr / int(rate),
    )
    trial.update({
        "case": case, "warmup_requests": warm, "measured_requests": measured,
        "input_s": input_s, "ceiling": ceiling, "fraction": SOAK_FRACTION,
    })
    return trial


def soak_spec(plan, trial) -> measurement.RunSpec:
    """The spec of a soak: the capacity trial's, named and sized as a soak."""
    base = capacity.trial_spec(plan, trial)
    first = capacity.CAPACITY_WORKLOADS[trial["workload_id"]]["first_index"]
    workload = dataclasses.replace(
        base.workload, requests=first + trial["warmup_requests"] + trial["measured_requests"])
    overrides = dict(base.overrides, **{
        "warmup_requests": trial["warmup_requests"],
        "measured_requests": trial["measured_requests"],
        "input_s": trial["input_s"],
        "soak_fraction": trial["fraction"],
        "ceiling_records_per_s": trial["ceiling"]["records_per_s"],
    })
    return dataclasses.replace(
        base,
        run_id=measurement.RunSpec.build_run_id(
            trial["case"], trial["topology"], plan["store_kind"], base.cores,
            trial["interval_s"], trial["ordinal"]),
        case=trial["case"], workload=workload,
        duration_s=int(math.ceil(trial["warmup_s"] + trial["measure_s"])),
        overrides=overrides,
    )


# --------------------------------------------------------------------------
# What a soak observes beside the capacity sampler
# --------------------------------------------------------------------------


class SoakObserver:
    """The capacity trial extension of a soak.

    Every second it reads the store's and the producers' resident memory,
    every few seconds the write-ahead log's size on disk and the objects the
    store lists as complete (never a partial upload); after the drain it
    moves the objects to the oracle's disk, and it turns the whole run into
    the soak's per-second, per-minute and drift view.
    """

    def __init__(self, plan, trial):
        self.plan = plan
        self.trial = trial
        self.rows = []
        self.listed = {}
        self.listings = []
        self.errors = []
        self.moved = {}
        self._stop = threading.Event()
        self._thread = None
        self.pids = []
        self.wal_dir = None

    def start(self, *, engine, fleet, window):
        self.pids = fleet.pids()
        self.wal_dir = engine.buffer_path
        self._thread = threading.Thread(target=self._run, name="soak-side", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=60)
            self._thread = None

    def _run(self):
        wake = threading.Event()
        next_listing = next_wal = 0
        store = self.plan.get("store")
        while not self._stop.is_set():
            now = time.monotonic_ns()
            try:
                row = {
                    "monotonic_ns": now,
                    "producer_rss_bytes": sum(measurement.process_tree_rss(pid) for pid in self.pids),
                }
                if self.plan.get("store_pid"):
                    row["store_rss_bytes"] = measurement.process_tree_rss(self.plan["store_pid"])
                if self.wal_dir is not None and now >= next_wal:
                    row["wal_dir_bytes"] = measurement.directory_bytes(self.wal_dir)
                    next_wal = now + int(WAL_PERIOD_S * 1e9)
                if store is not None and now >= next_listing:
                    self._list(store)
                    next_listing = now + int(LISTING_PERIOD_S * 1e9)
                self.rows.append(row)
            except Exception as error:  # noqa: BLE001 - recorded, never a zero
                self.errors.append({"monotonic_ns": now, "error": f"{error}"[:300]})
            _ = wake.wait(SIDE_PERIOD_S)

    def _list(self, store):
        """Record every newly completed object with the time it was seen."""
        started = time.monotonic_ns()
        seen_epoch_ns = time.time_ns()
        new = 0
        for key, size, completed in capacity.object_inventory(self.plan, store, None):
            if key not in self.listed:
                self.listed[key] = (size, completed, seen_epoch_ns)
                new += 1
        self.listings.append({
            "monotonic_ns": started, "duration_s": (time.monotonic_ns() - started) / 1e9,
            "objects_count": len(self.listed), "new_objects_count": new,
            "object_bytes": sum(size for size, _c, _s in self.listed.values()),
        })

    def download(self, store, directory):
        """Move every completed object to `directory`, deleting it from the
        store once copied, so a soak's objects are never held twice."""

        def move(key):
            destination = Path(directory) / key.removeprefix("otel/")
            destination.parent.mkdir(parents=True, exist_ok=True)
            store.client.download_file(store.bucket, key, str(destination))
            store.client.delete_object(Bucket=store.bucket, Key=key)
            return destination.stat().st_size

        def move_all():
            keys = [key for key, _size in performance.store_objects(store)
                    if key.endswith(".parquet")]
            with concurrent.futures.ThreadPoolExecutor(max_workers=MOVE_THREADS) as pool:
                sizes = list(pool.map(move, keys))
            return {"objects_count": len(keys), "bytes": sum(sizes)}

        started = time.monotonic_ns()
        self.moved = measurement.run_pinned(self.plan["oracle_cores"], move_all)
        self.moved["duration_s"] = (time.monotonic_ns() - started) / 1e9

    def finalize(self, result, context):
        settle_soak(result, context, self)


# --------------------------------------------------------------------------
# The per-second view and what it shows
# --------------------------------------------------------------------------


def _gauge_total(sample, name):
    return sum(worker["gauges"][name] for worker in sample["workers"].values())


def _observed(sample) -> bool:
    """Whether a sample is a measurement: every worker answered and the
    process was read."""
    workers = sample.get("workers") or {}
    return bool(workers) and all(measurement.worker_reported(w) for w in workers.values()) \
        and "smaps_rss_bytes" in (sample.get("procfs") or {})


class _Timeline:
    """Sorted event times with prefix sums, for counts and totals up to a time."""

    def __init__(self, events):
        events = sorted(events)
        self.times = [t for t, _v in events]
        self.prefix = [0]
        for _t, value in events:
            self.prefix.append(self.prefix[-1] + value)

    def upto(self, t_ns) -> float:
        """The total of every event at or before `t_ns`."""
        return self.prefix[bisect.bisect_right(self.times, t_ns)]

    def between(self, a_ns, b_ns) -> float:
        """The total of the events in [a_ns, b_ns)."""
        return self.prefix[bisect.bisect_left(self.times, b_ns)] - \
            self.prefix[bisect.bisect_left(self.times, a_ns)]


def oldest_outstanding(sent, finish, instants) -> list:
    """At each instant, the age of the oldest request sent and not answered."""
    order = sorted(range(len(sent)), key=lambda k: sent[k])
    heap = []
    position = 0
    ages = []
    for t in instants:
        while position < len(order) and sent[order[position]] <= t:
            k = order[position]
            heapq.heappush(heap, (sent[k], finish[k] or math.inf))
            position += 1
        while heap and heap[0][1] <= t:
            _ = heapq.heappop(heap)
        ages.append((t - heap[0][0]) / 1e9 if heap else 0.0)
    return ages


def per_second_rows(*, samples, side, sends, objects, visible, window, input_end_ns,
                    epoch_offset_ns, records_per_request, size_of, buffered) -> list:
    """One row per second of the run, second 0 the first of the input phase.

    Producer columns come from every send (attempted, acknowledged, wire
    bytes, requests and bytes in flight, the oldest unanswered request);
    storage columns from completed objects and from the request visibility
    the oracle read back (unique records committed); engine columns from the
    last telemetry sample of the second and side columns from the soak's
    own second-by-second reads. A row with no sample in its second is
    marked unobserved, never filled with zeros.
    """
    rpr = records_per_request
    start = window[0]
    sent = sends["sent"]
    finish = sends["finish"]
    code = sends["code"]
    sizes = [size_of(index) for index in sends["indexes"]]
    attempted = _Timeline((sent[k], rpr) for k in range(len(sent)) if sent[k])
    wire = _Timeline((sent[k], sizes[k]) for k in range(len(sent)) if sent[k])
    acked = _Timeline((finish[k], rpr) for k in range(len(sent)) if code[k] == 0)
    answered = _Timeline((finish[k], 1) for k in range(len(sent)) if finish[k])
    answered_bytes = _Timeline((finish[k], sizes[k]) for k in range(len(sent)) if finish[k])
    started = _Timeline((sent[k], 1) for k in range(len(sent)) if sent[k])
    committed = _Timeline((at - epoch_offset_ns, rpr) for at in visible.values())
    stored = _Timeline((at - epoch_offset_ns, size) for _key, size, at in objects)
    engine = sorted((s for s in samples if s.get("workers")), key=lambda s: s["monotonic_ns"])
    side = sorted(side, key=lambda row: row["monotonic_ns"])
    last = max([engine[-1]["monotonic_ns"] if engine else start] + [input_end_ns])
    first_second = math.floor(((engine[0]["monotonic_ns"] if engine else start) - start) / 1e9)
    seconds = list(range(first_second, math.ceil((last - start) / 1e9)))
    ends = [start + (second + 1) * 10**9 for second in seconds]
    oldest = oldest_outstanding(sent, finish, ends)
    engine_times = [s["monotonic_ns"] for s in engine]
    side_times = [row["monotonic_ns"] for row in side]
    rows = []
    previous_written = None
    wal_dir_bytes = None
    for position, second in enumerate(seconds):
        a = start + second * 10**9
        b = a + 10**9
        lo, hi = bisect.bisect_left(engine_times, a), bisect.bisect_left(engine_times, b)
        inside = engine[lo:hi]
        row = {
            "t_s": second,
            "phase": "warmup" if b <= start else "input" if b <= input_end_ns else "drain",
            "samples_count": len(inside),
            "attempted_records": attempted.between(a, b),
            "acked_records": acked.between(a, b),
            "committed_records": committed.between(a, b),
            "wire_bytes": wire.between(a, b),
            "object_bytes": stored.between(a, b),
            "in_flight_requests": started.upto(b) - answered.upto(b),
            "in_flight_bytes": wire.upto(b) - answered_bytes.upto(b),
            "oldest_unanswered_s": oldest[position],
        }
        observed = [s for s in inside if _observed(s)]
        row["observed"] = bool(observed)
        if observed:
            sample = observed[-1]
            procfs = sample["procfs"]
            written = capacity.extras_total(sample, "rows.written", "values")
            row.update({
                "rows_written_records": (written - previous_written
                                         if previous_written is not None else None),
                "block_active_bytes": _gauge_total(sample, "block.active"),
                "block_flushing_bytes": _gauge_total(sample, "block.flushing"),
                "block_pending_bytes": _gauge_total(sample, "block.pending"),
                "requests_pending_count": _gauge_total(sample, "block.requests_pending"),
                "notify_queued_count": _gauge_total(sample, "notify.queued"),
                "exporter_oldest_unacked_s": max(
                    w["gauges"]["oldest_unacked.age"] for w in sample["workers"].values()),
                "series_cache_entries_count": _gauge_total(sample, "series_cache.entries"),
                "accounted_bytes": _gauge_total(sample, "memory.accounted"),
                "budget_bytes": _gauge_total(sample, "memory.budget"),
                "flush_workspace_bytes": capacity.extras_total(sample, "flush.workspace"),
                "admission_closed_count": capacity.extras_total(sample, "admission.closed"),
                "rss_bytes": procfs.get("smaps_rss_bytes"),
                "anonymous_bytes": procfs.get("smaps_anonymous_bytes"),
                "open_fds_count": procfs.get("open_fd_count"),
                "threads_count": procfs.get("num_threads"),
            })
            previous_written = written
            allocator = sample.get("allocator_latest") or {}
            if allocator:
                row["jemalloc_allocated_bytes"] = allocator["jemalloc_allocated_bytes"]
                row["jemalloc_resident_bytes"] = allocator["jemalloc_resident_bytes"]
                # The allocator totals are the latest print's, read this long
                # before the telemetry sample.
                row["jemalloc_age_s"] = (sample["monotonic_ns"]
                                         - allocator["monotonic_ns"]) / 1e9
            if buffered:
                row.update({
                    "wal_bytes": capacity.extras_total(sample, "buffer.storage.bytes.used"),
                    "buffer_queued_items": capacity.extras_total(sample, "buffer.items.queued"),
                    "buffer_in_flight_count": capacity.extras_total(sample, "buffer.in.flight"),
                    "buffer_retries_count": capacity.extras_total(
                        sample, "buffer.retries.scheduled"),
                })
        at = bisect.bisect_left(side_times, b) - 1
        if at >= 0 and side[at]["monotonic_ns"] >= a:
            entry = side[at]
            row["producer_rss_bytes"] = entry.get("producer_rss_bytes")
            row["store_rss_bytes"] = entry.get("store_rss_bytes")
        for entry in side[bisect.bisect_left(side_times, a):bisect.bisect_left(side_times, b)]:
            if "wal_dir_bytes" in entry:
                wal_dir_bytes = entry["wal_dir_bytes"]
        if buffered:
            row["wal_dir_bytes"] = wal_dir_bytes
        rows.append(row)
    return rows


def _median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def per_minute(rows, input_s) -> list:
    """The input phase minute by minute: rates, and memory at its highest."""
    minutes = []
    for minute in range(math.ceil(input_s / 60)):
        inside = [row for row in rows if row["phase"] == "input"
                  and minute * 60 <= row["t_s"] < (minute + 1) * 60]
        if not inside:
            continue
        span = len(inside)

        def rate(key):
            return sum(row.get(key) or 0 for row in inside) / span

        def peak(key):
            values = [row.get(key) for row in inside if row.get(key) is not None]
            return max(values) if values else None

        minutes.append({
            "minute": minute,
            "seconds_count": span,
            "attempted_records_per_s": rate("attempted_records"),
            "acked_records_per_s": rate("acked_records"),
            "committed_records_per_s": rate("committed_records"),
            "rows_written_records_per_s": rate("rows_written_records"),
            "input_bytes_per_s": rate("wire_bytes"),
            "object_bytes_per_s": rate("object_bytes"),
            "rss_max_bytes": peak("rss_bytes"),
            "rss_median_bytes": _median(row.get("rss_bytes") for row in inside),
            "accounted_max_bytes": peak("accounted_bytes"),
            "jemalloc_allocated_max_bytes": peak("jemalloc_allocated_bytes"),
            "open_fds_max_count": peak("open_fds_count"),
            "series_cache_entries_max_count": peak("series_cache_entries_count"),
            "in_flight_requests_max": peak("in_flight_requests"),
            "in_flight_max_bytes": peak("in_flight_bytes"),
            "wal_max_bytes": peak("wal_bytes"),
        })
    return minutes


def series_drift(rows, key, input_s, edge_s=EDGE_WINDOW_S) -> dict:
    """How one per-second series moved over the input phase: its slope, the
    median of the first and of the last `edge_s`, their ratio and change."""
    points = [(row["t_s"], row[key]) for row in rows
              if row["phase"] == "input" and row.get(key) is not None]
    if not points:
        return {"points_count": 0}
    first = _median(value for t, value in points if t < edge_s)
    last = _median(value for t, value in points if t >= input_s - edge_s)
    return {
        "points_count": len(points),
        "slope_per_s": capacity.backlog_slope(points),
        "first_median": first,
        "last_median": last,
        "median_ratio": last / first if first else None,
        "median_change": last - first if first is not None and last is not None else None,
        "min": min(value for _t, value in points),
        "max": max(value for _t, value in points),
    }


def drift(rows, input_s) -> dict:
    """RSS, descriptors, cache occupancy and unaccounted heap over the input."""
    unaccounted = [
        dict(row, unaccounted_bytes=row["jemalloc_allocated_bytes"] - row["accounted_bytes"])
        for row in rows if row.get("jemalloc_allocated_bytes") is not None
        and row.get("accounted_bytes") is not None
    ]
    report = {
        "rss_bytes": series_drift(rows, "rss_bytes", input_s),
        "anonymous_bytes": series_drift(rows, "anonymous_bytes", input_s),
        "open_fds_count": series_drift(rows, "open_fds_count", input_s),
        "series_cache_entries_count": series_drift(rows, "series_cache_entries_count", input_s),
        "accounted_bytes": series_drift(rows, "accounted_bytes", input_s),
        "jemalloc_allocated_bytes": series_drift(rows, "jemalloc_allocated_bytes", input_s),
        "allocated_minus_accounted_bytes": series_drift(unaccounted, "unaccounted_bytes",
                                                         input_s),
        "store_rss_bytes": series_drift(rows, "store_rss_bytes", input_s),
        "producer_rss_bytes": series_drift(rows, "producer_rss_bytes", input_s),
    }
    if any("wal_bytes" in row for row in rows):
        report["wal_bytes"] = series_drift(rows, "wal_bytes", input_s)
        report["wal_dir_bytes"] = series_drift(rows, "wal_dir_bytes", input_s)
    return report


def sample_coverage(rows, input_phase_s) -> float:
    """The share of input seconds with a sample every worker answered."""
    seconds = [row for row in rows if 0 <= row["t_s"] < math.floor(input_phase_s)]
    if not seconds:
        return 0.0
    return sum(1 for row in seconds if row["observed"]) / len(seconds)


def input_phase(sends, *, first_measured_index, records_per_request, rate) -> dict:
    """How long the measured cohort's producers were active.

    From the first measured send to the last, plus the one request interval
    the last send opens; warm-up, startup and drain are outside it.
    """
    measured = [k for k, index in enumerate(sends["indexes"])
                if index >= first_measured_index and sends["sent"][k]]
    if not measured:
        return {"input_phase_s": 0.0, "measured_requests_sent_count": 0}
    first = min(sends["sent"][k] for k in measured)
    last = max(sends["sent"][k] for k in measured)
    return {
        "input_phase_s": (last - first) / 1e9 + records_per_request / rate,
        "measured_requests_sent_count": len(measured),
        "first_measured_send_ns": first,
        "last_measured_send_ns": last,
    }


def backlog_and_drain(sends, visible, *, stop_ns, epoch_offset_ns, records_per_request) -> dict:
    """What was owed when input stopped, and how long it took to pay.

    The acknowledgement backlog is what was sent and not yet acknowledged;
    the storage backlog is what was sent and not yet in a completed object.
    """
    rpr = records_per_request
    sent_by_stop = [k for k, t in enumerate(sends["sent"]) if t and t <= stop_ns]
    acked = [(sends["finish"][k], sends["indexes"][k]) for k in range(len(sends["sent"]))
             if sends["code"][k] == 0]
    acked_by_stop = sum(1 for finish, _i in acked if finish <= stop_ns)
    last_ack = max((finish for finish, _i in acked), default=stop_ns)
    committed = {index: at - epoch_offset_ns for index, at in visible.items()}
    sent_indexes = [sends["indexes"][k] for k in sent_by_stop]
    storage_owed = [index for index in sent_indexes
                    if committed.get(index, math.inf) > stop_ns]
    last_commit = max(committed.values(), default=stop_ns)
    ack_backlog = (len(sent_by_stop) - acked_by_stop) * rpr
    storage_backlog = len(storage_owed) * rpr
    ack_drain_s = max(0.0, (last_ack - stop_ns) / 1e9)
    storage_drain_s = max(0.0, (last_commit - stop_ns) / 1e9)
    return {
        "ack_backlog_at_stop_records": ack_backlog,
        "ack_drain_s": ack_drain_s,
        "ack_drain_records_per_s": ack_backlog / ack_drain_s if ack_drain_s else None,
        "storage_backlog_at_stop_records": storage_backlog,
        "storage_drain_s": storage_drain_s,
        "storage_drain_records_per_s": (storage_backlog / storage_drain_s
                                        if storage_drain_s else None),
        "acked_not_committed_requests_count": sum(
            1 for _finish, index in acked if index not in committed),
    }


def phase_rates(rows, input_s, edge_s=EDGE_WINDOW_S) -> dict:
    """Full-input and final-`edge_s` averages of the per-second rates."""
    inside = [row for row in rows if row["phase"] == "input"]
    final = [row for row in inside if row["t_s"] >= input_s - edge_s]

    def average(selected, key):
        return (sum(row.get(key) or 0 for row in selected) / len(selected)
                if selected else None)

    return {
        "attempted_records_per_s": average(inside, "attempted_records"),
        "acked_records_per_s": average(inside, "acked_records"),
        "committed_records_per_s": average(inside, "committed_records"),
        "rows_written_records_per_s": average(inside, "rows_written_records"),
        "input_bytes_per_s": average(inside, "wire_bytes"),
        "object_bytes_per_s": average(inside, "object_bytes"),
        "final_attempted_records_per_s": average(final, "attempted_records"),
        "final_acked_records_per_s": average(final, "acked_records"),
        "final_committed_records_per_s": average(final, "committed_records"),
        "final_input_bytes_per_s": average(final, "wire_bytes"),
        "final_object_bytes_per_s": average(final, "object_bytes"),
    }


def memory_at_highest_fill(rows) -> dict:
    """The input second whose ACTIVE plus FLUSHING bytes were highest, and
    every memory domain in it: exporter accounting against its budget,
    jemalloc, the receiver's in-flight bytes and, buffered, the write-ahead
    log."""
    filled = [row for row in rows if row.get("block_active_bytes") is not None
              and row["phase"] == "input"]
    if not filled:
        return {}
    row = max(filled, key=lambda r: r["block_active_bytes"] + r["block_flushing_bytes"])
    keys = ("t_s", "block_active_bytes", "block_flushing_bytes", "block_pending_bytes",
            "accounted_bytes", "budget_bytes", "flush_workspace_bytes", "rss_bytes",
            "anonymous_bytes", "jemalloc_allocated_bytes", "jemalloc_resident_bytes",
            "jemalloc_age_s", "in_flight_bytes", "in_flight_requests", "wal_bytes",
            "wal_dir_bytes")
    report = {key: row[key] for key in keys if key in row}
    report["fill_bytes"] = row["block_active_bytes"] + row["block_flushing_bytes"]
    if row.get("budget_bytes"):
        report["accounted_to_budget_ratio"] = row["accounted_bytes"] / row["budget_bytes"]
    if row.get("jemalloc_allocated_bytes") is not None:
        report["allocated_minus_accounted_bytes"] = (
            row["jemalloc_allocated_bytes"] - row["accounted_bytes"])
    return report


def oracle_counters(oracle, sends, *, records_per_request) -> dict:
    """The correctness counters of a soak, from its oracle and its sends."""
    rpr = records_per_request
    signals = (oracle or {}).get("signals") or {}
    missing_acked = sum(s.get("missing_record_count", 0) for s in signals.values())
    stored_failed = sum(s.get("stored_failed_records_count", 0) for s in signals.values())
    not_acked = sum(1 for code in sends["code"] if code != 0) * rpr
    files = (oracle or {}).get("files") or {}
    readers = (oracle or {}).get("readers") or {}
    return {
        "missing_acked_records": missing_acked,
        "missing_intended_records": missing_acked + not_acked - stored_failed,
        "duplicate_records": sum(s.get("duplicate_record_count", 0) for s in signals.values()),
        "unexpected_records": sum(s.get("unexpected_record_count", 0)
                                  for s in signals.values()),
        "descriptor_violations_count": len(files.get("problems") or []) if oracle else None,
        "reader_disagreements_count": len(readers.get("problems") or []) if oracle else None,
        "permanent_rejections_count": sum(1 for code in sends["code"] if code == 3),
        "failed_requests_count": sum(1 for code in sends["code"] if code != 0),
    }


def rss_metrics(rows, input_s) -> dict:
    """The compared RSS figures: the peak and the last-to-first median ratio.

    The slope is kept beside them as an observation: near zero its relative
    change is noise, while the median ratio sits near one.
    """
    rss = series_drift(rows, "rss_bytes", input_s)
    return {
        "rss_peak_bytes": rss.get("max"),
        "rss_post_warmup_median_ratio": rss.get("median_ratio"),
    }


# Metrics a soak compares under the Controller baseline policy, by direction.
SOAK_DIRECTIONS = {
    "input_phase_s": measurement.HIGHER_IS_BETTER,
    "offered_records_per_s": measurement.HIGHER_IS_BETTER,
    "acked_records_per_s": measurement.HIGHER_IS_BETTER,
    "committed_records_per_s": measurement.HIGHER_IS_BETTER,
    "final_acked_records_per_s": measurement.HIGHER_IS_BETTER,
    "final_committed_records_per_s": measurement.HIGHER_IS_BETTER,
    "input_bytes_per_s": measurement.HIGHER_IS_BETTER,
    "object_bytes_per_s": measurement.HIGHER_IS_BETTER,
    "sample_coverage_ratio": measurement.HIGHER_IS_BETTER,
    "ack_latency_p50_s": measurement.LOWER_IS_BETTER,
    "ack_latency_p99_s": measurement.LOWER_IS_BETTER,
    "engine_cpu_ns_per_record": measurement.LOWER_IS_BETTER,
    "rss_peak_bytes": measurement.LOWER_IS_BETTER,
    "rss_post_warmup_median_ratio": measurement.LOWER_IS_BETTER,
    "fd_peak_count": measurement.LOWER_IS_BETTER,
    "accounted_peak_bytes": measurement.LOWER_IS_BETTER,
    "missing_acked_records": measurement.LOWER_IS_BETTER,
    "missing_intended_records": measurement.LOWER_IS_BETTER,
    "duplicate_records": measurement.LOWER_IS_BETTER,
    "descriptor_violations_count": measurement.LOWER_IS_BETTER,
    "reader_disagreements_count": measurement.LOWER_IS_BETTER,
    "permanent_rejections_count": measurement.LOWER_IS_BETTER,
    "buffer_loss_records": measurement.LOWER_IS_BETTER,
    "wal_peak_bytes": measurement.LOWER_IS_BETTER,
    "freshness_p99_s": measurement.LOWER_IS_BETTER,
}

# Why a metric is an explicit zero rather than a measurement.
NOT_APPLICABLE_NO_BUFFER = "no_buffer"


def settle_soak(result, context, observer):
    """Turn a settled capacity trial into a soak result.

    The capacity trial's metrics are kept under `soak.capacity_metrics`;
    the soak's own metrics, its per-second samples, its per-minute and drift
    view, the backlog and drain after input stopped, memory at the highest
    fill and the soak checks replace them.
    """
    trial = context["trial"]
    spec = context["spec"]
    window = context["window"]
    sends = context["sends"]
    buffered = trial["topology"] == "buffered"
    rpr = spec.workload.records_per_request
    first = capacity.CAPACITY_WORKLOADS[trial["workload_id"]]["first_index"]
    input_s = trial["input_s"]
    phase = input_phase(sends, first_measured_index=first + trial["warmup_requests"],
                        records_per_request=rpr, rate=trial["rate"])
    rows = per_second_rows(
        samples=context["samples"], side=observer.rows, sends=sends,
        objects=context["objects"], visible=context["visible"], window=window,
        input_end_ns=window[1], epoch_offset_ns=context["epoch_offset_ns"],
        records_per_request=rpr, size_of=context["source"].size, buffered=buffered,
    )
    coverage = sample_coverage(rows, phase["input_phase_s"])
    rates = phase_rates(rows, input_s)
    drained = backlog_and_drain(sends, context["visible"], stop_ns=window[1],
                                epoch_offset_ns=context["epoch_offset_ns"],
                                records_per_request=rpr)
    counters = oracle_counters(context["oracle"], sends, records_per_request=rpr)
    capacity_metrics = result["metrics"]
    fill = memory_at_highest_fill(rows)
    final = [s for s in context["samples"] if s.get("extras")]
    not_applicable = {}
    if buffered:
        counters["buffer_loss_records"] = capacity.extras_total(
            final[-1], "buffer.loss.items") if final else None
    else:
        counters["buffer_loss_records"] = 0
        not_applicable["buffer_loss_records"] = NOT_APPLICABLE_NO_BUFFER
    fds = [row["open_fds_count"] for row in rows if row.get("open_fds_count") is not None]
    metrics = {
        "input_phase_s": phase["input_phase_s"],
        "offered_records_per_s": float(trial["rate"]),
        "acked_records_per_s": rates["acked_records_per_s"],
        "committed_records_per_s": rates["committed_records_per_s"],
        "final_acked_records_per_s": rates["final_acked_records_per_s"],
        "final_committed_records_per_s": rates["final_committed_records_per_s"],
        "input_bytes_per_s": rates["input_bytes_per_s"],
        "object_bytes_per_s": rates["object_bytes_per_s"],
        "sample_coverage_ratio": coverage,
        "ack_latency_p50_s": capacity_metrics.get("ack_latency_p50_s"),
        "ack_latency_p99_s": capacity_metrics.get("ack_latency_p99_s"),
        "engine_cpu_ns_per_record": capacity_metrics.get("engine_cpu_ns_per_record"),
        "fd_peak_count": max(fds) if fds else None,
        "accounted_peak_bytes": capacity_metrics.get("accounted_peak_bytes"),
        **rss_metrics(rows, input_s),
        **{name: counters[name] for name in (
            "missing_acked_records", "missing_intended_records", "duplicate_records",
            "descriptor_violations_count", "reader_disagreements_count",
            "permanent_rejections_count", "buffer_loss_records")},
    }
    fresh = result["capacity"].get("freshness") or {}
    if buffered:
        wal = [row["wal_bytes"] for row in rows if row.get("wal_bytes") is not None]
        metrics["wal_peak_bytes"] = max(wal) if wal else None
        metrics["freshness_p99_s"] = fresh.get("lag_p99_s")
    result["metrics"] = metrics
    result["metrics_unavailable"] = {
        name: "not observed in this soak" for name, value in metrics.items() if value is None}
    result["metric_directions"] = {name: SOAK_DIRECTIONS[name] for name in metrics}
    result["mandatory_metrics"] = sorted(name for name, value in metrics.items()
                                         if value is not None)
    result["samples"] = rows
    result["samples_period_s"] = 1.0
    result.pop("samples_published_stride", None)
    observations = result.get("observations") or {}
    for key in ("residuals", "allocator_pairs"):
        entries = observations.get(key) or []
        observations[key] = _thin(entries, 10.0)
    peak_rss = max((row.get("rss_bytes") or 0 for row in rows), default=0)
    observations["residual_excursions"] = residual_excursions(
        context["residuals"], context.get("pairs") or [], peak_rss)
    listing_lags = sorted((seen - completed) / 1e9
                          for _size, completed, seen in observer.listed.values())
    result["soak"] = {
        "conditions": {
            "case": trial["case"], "topology": trial["topology"], "store": spec.store,
            "workers_count": len(spec.cores), "interval_s": trial["interval_s"],
            "receiver_capacity_per_worker": trial["receiver_capacity"],
            "offered_records_per_s": trial["rate"], "fraction_of_ceiling": trial["fraction"],
            "ceiling": trial["ceiling"], "workload_id": trial["workload_id"],
            "workload": spec.workload.as_json(),
            "hot_series_count": spec.workload.series,
            "churn_every_records": spec.workload.churn_every,
            "churned_series_count": (
                (first + trial["warmup_requests"] + trial["measured_requests"]) * rpr
                // spec.workload.churn_every if spec.workload.churn_every else 0),
            "warmup_requests_count": trial["warmup_requests"],
            "measured_requests_count": trial["measured_requests"],
            "input_s_required": input_s,
            "wal_on": "the trial's engine directory on the host disk" if buffered else None,
        },
        "input_phase": phase,
        "rates": rates,
        "backlog_and_drain": drained,
        "drain_proof": (context["summary"] or {}).get("drain"),
        "memory_at_highest_fill": fill,
        "memory_at_fill_capacity_view": context["memory"],
        "drift": drift(rows, input_s),
        "per_minute": per_minute(rows, input_s),
        "counters": counters,
        "not_applicable": not_applicable,
        "healthy": counters["failed_requests_count"] == 0,
        "listing": {
            "listings_count": len(observer.listings),
            "objects_count": len(observer.listed),
            "listing_lag_p50_s": measurement.percentile(listing_lags, 0.5) if listing_lags
            else None,
            "listing_lag_max_s": listing_lags[-1] if listing_lags else None,
            "listing_duration_max_s": max((entry["duration_s"] for entry in observer.listings),
                                          default=None),
        },
        "moved": observer.moved,
        "download_s": context["download_s"],
        "oracle_s": context["oracle_s"],
        "side_errors": observer.errors[:20],
        "capacity_metrics": capacity_metrics,
    }
    checks = result["checks"]

    def hard(name, passed, detail):
        checks.append(measurement.check(
            name, measurement.CHECK_HARD,
            measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED, detail))

    hard("input_duration", phase["input_phase_s"] >= input_s,
         f"producers active {phase['input_phase_s']:.1f}s of the required {input_s}s")
    hard("sample_coverage", coverage >= SAMPLE_COVERAGE_FLOOR,
         f"{coverage:.4f} of input seconds observed; floor {SAMPLE_COVERAGE_FLOOR}")
    hard("rate_sustained", result["capacity"]["verdict"] == "sustainable",
         f"{result['capacity']['verdict']}: {result['capacity']['reasons']}")
    drain_proof = (context["summary"] or {}).get("drain") or {}
    hard("backlog_drained", bool(drain_proof.get("drained"))
         and drained["acked_not_committed_requests_count"] == 0,
         f"drain proof {drain_proof.get('drained')}; acknowledged requests not in a "
         f"completed object {drained['acked_not_committed_requests_count']}; storage drain "
         f"{drained['storage_drain_s']:.1f}s")
    hard("duplicate_free", counters["duplicate_records"] == 0
         or counters["failed_requests_count"] > 0,
         f"{counters['duplicate_records']} duplicate records, "
         f"{counters['failed_requests_count']} failed requests")
    if buffered:
        hard("buffer_retention_lossless", counters["buffer_loss_records"] == 0,
             f"{counters['buffer_loss_records']} items lost to retention")


EXCURSION_NEIGHBOURS = 3


def residual_excursions(residuals, pairs, peak_rss_bytes) -> dict:
    """Every RSS residual beyond half the frozen tolerance, with the
    allocator pairs around it, kept whole where the rest is thinned.

    `allocator_band_residuals` computes residual `k` from pair `k + 1`, so
    each excursion shows the prints before and after it: a residual the
    next pair's allocator totals already cover is a print read before a
    large allocation it did not yet include.
    """
    tolerance = max(measurement.RESIDUAL_FLOOR_BYTES,
                    measurement.RESIDUAL_PEAK_FRACTION * peak_rss_bytes)
    flagged = [k for k, entry in enumerate(residuals)
               if abs(entry["residual_bytes"]) > tolerance / 2]
    events = []
    for k in flagged[:50]:
        low = max(0, k + 1 - EXCURSION_NEIGHBOURS)
        high = min(len(pairs), k + 2 + EXCURSION_NEIGHBOURS)
        events.append({
            "residual": residuals[k],
            "beyond_tolerance": abs(residuals[k]["residual_bytes"]) > tolerance,
            "pairs": [dict(pairs[j], offset=j - (k + 1)) for j in range(low, high)],
        })
    return {"tolerance_bytes": tolerance, "flagged_count": len(flagged),
            "beyond_tolerance_count": sum(1 for k in flagged
                                          if abs(residuals[k]["residual_bytes"]) > tolerance),
            "events": events}


def _thin(entries, period_s) -> list:
    """At most one entry per `period_s` of monotonic time."""
    kept = []
    next_ns = None
    for entry in entries:
        at = entry.get("monotonic_ns")
        if at is None or next_ns is None or at >= next_ns:
            kept.append(entry)
            next_ns = (at or 0) + int(period_s * 1e9)
    return kept


def soak_checks(result, *, baselines=None) -> dict:
    """The soak acceptance of one result: every correctness counter zero,
    enough samples, and the shared Controller baseline policy.

    A strict soak carries an explicit zero for each buffer-only counter,
    tagged `not_applicable: no_buffer`; a counter that is absent is an
    instrumentation error, never a zero.
    """
    metrics = result["metrics"]
    tags = (result.get("soak") or {}).get("not_applicable") or {}
    failures = []
    for name in ZERO_METRICS:
        if name not in metrics or metrics[name] is None:
            failures.append(f"{name} absent: an instrumentation error")
        elif metrics[name] != 0:
            failures.append(f"{name}={metrics[name]}")
    buffered = result.get("config", {}).get("requested", {}).get("topology") == "buffered"
    if not buffered and tags.get("buffer_loss_records") != NOT_APPLICABLE_NO_BUFFER:
        failures.append("buffer_loss_records is not tagged not_applicable: no_buffer")
    if metrics.get("sample_coverage_ratio") is None or \
            metrics["sample_coverage_ratio"] < SAMPLE_COVERAGE_FLOOR:
        failures.append("insufficient RSS/rate samples")
    decision = measurement.evaluate_baseline(result, baselines=baselines)
    if failures:
        raise AssertionError("; ".join(failures))
    return decision


# --------------------------------------------------------------------------
# The soak family
# --------------------------------------------------------------------------


class FamilyState:
    """Which run each soak step produced, so a family resumes by step."""

    def __init__(self, output_dir):
        self.path = Path(output_dir) / STATE_FILE
        self.document = (json.loads(self.path.read_text(encoding="ascii"))
                         if self.path.is_file() else {"runs": {}})

    def record(self, step, value):
        self.document["runs"][step] = value
        _ = measurement.write_json_atomic(self.path, self.document)

    def get(self, step):
        return self.document["runs"].get(step)


def open_soak_plan(output_dir, options) -> dict:
    """The capacity plan of the soak cell, with soak run ordinals."""
    options = dict(options)
    options.setdefault("archive_dir", str(ARCHIVE_DIR_DEFAULT))
    plan = capacity.open_plan("minio", 1, output_dir, options)
    plan["soak_ordinal"] = capacity.next_ordinal_factory(
        measurement.resolve_report_dir(options.get("report_dir")), output_dir, prefix="soak-")
    return plan


def run_one_soak(plan, case, rate, ceiling, output_dir, report_dir, *, input_s) -> dict:
    """One soak under the host controls, evaluated as its own family."""
    soak_plan = dict(plan, next_ordinal=plan["soak_ordinal"])
    trial = soak_trial(soak_plan, case, rate, input_s=input_s, ceiling=ceiling)

    def experiment(plan_, trial_, spec, result, run_dir, controls):
        capacity.trial_experiment(plan_, trial_, spec, result, run_dir, controls,
                                  extension=SoakObserver(plan_, trial_))

    started = time.monotonic()
    result = capacity.run_trial(soak_plan, trial, output_dir, report_dir, experiment,
                                spec_fn=soak_spec, evaluate=True)
    run_dir = Path(output_dir) / result["run_id"]
    for entry in result.get("baseline_files") or []:
        source = run_dir / entry["name"]
        if source.is_file():
            _ = shutil.copyfile(source, Path(output_dir) / entry["name"])
    sys.stderr.write(
        f"{result['run_id']}: {result['status']} ({time.monotonic() - started:.0f}s) "
        + json.dumps({k: result["metrics"].get(k) for k in (
            "input_phase_s", "acked_records_per_s", "committed_records_per_s",
            "rss_peak_bytes", "rss_post_warmup_median_ratio")}) + "\n")
    return result


def buffered_bracket(plan, output_dir, report_dir, options) -> dict:
    """The one-worker buffered sustainable rate at the soak's configuration,
    searched as every capacity cell is (doubling from a floor, bisection to
    10 percent, three repetitions of the winner)."""
    directory = Path(output_dir) / "bracket"
    directory.mkdir(parents=True, exist_ok=True)
    state = capacity.FamilyState(directory)
    variant = "buffered_default_window"
    floor = int(options.get("bracket_floor", BRACKET_FLOOR_RECORDS_PER_S))
    search = {"workload_id": SOAK_WORKLOAD, "search_floor": {CELL: {variant: floor}}}
    capacity.step_search(plan, state, directory, report_dir, CELL, search, variant=variant)
    decision = capacity.winning(state, CELL, SOAK_WORKLOAD, variant)
    for entry in state.trials(CELL):
        _ = shutil.copyfile(directory / f"{entry['run_id']}.json",
                            Path(output_dir) / f"{entry['run_id']}.json")
    return {
        "records_per_s": decision["sustainable_records_per_s"],
        "variant": variant, "cell": CELL, "floor_records_per_s": floor,
        "decision": decision,
        "run_ids": [entry["run_id"] for entry in state.trials(CELL)],
    }


SOAK_STEPS = ("strict", "bracket", "buffered", "pr_strict", "pr_buffered", "alloy", "publish")
ALLOY_RATE_LINES_PER_S = 20_000


def run_soak(output_dir, report_dir=None, **options) -> list:
    """`measure soak`: the named steps, then one index per topology.

    `strict` and `buffered` are the thirty-minute soaks, `bracket` searches
    the buffered rate the buffered soak takes 70 percent of, `pr_strict` and
    `pr_buffered` are the PR-tier soaks, `alloy` is the short Alloy producer
    compatibility trial, and `publish` writes `soak-strict.json` and
    `soak-buffered.json` over what the family state recorded.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    steps = list(options.pop("steps", SOAK_STEPS))
    unknown = sorted(set(steps) - set(SOAK_STEPS))
    if unknown:
        raise AssertionError(f"unknown soak steps {unknown}; known {list(SOAK_STEPS)}")
    input_s = int(options.pop("input_s", SOAK_INPUT_S))
    state = FamilyState(output_dir)
    measured = [step for step in steps if step in ("strict", "bracket", "buffered", "alloy")]
    if measured:
        plan = open_soak_plan(output_dir, dict(options, report_dir=report_dir))
        try:
            with capacity.cell_store(plan):
                if "strict" in steps:
                    ceiling = published_ceiling(**SOAKS["soak-strict"]["ceiling"])
                    rate = int(SOAK_FRACTION * ceiling["records_per_s"])
                    result = run_one_soak(plan, "soak-strict", rate, ceiling, output_dir,
                                          report_dir, input_s=input_s)
                    state.record("strict", result["run_id"])
                if "bracket" in steps:
                    state.record("bracket", buffered_bracket(plan, output_dir, report_dir,
                                                             options))
                if "buffered" in steps:
                    ceiling = state.get("bracket")
                    if not ceiling or not ceiling.get("records_per_s"):
                        raise AssertionError("the buffered soak needs the bracket step first")
                    rate = int(options.get("buffered_rate")
                               or SOAK_FRACTION * ceiling["records_per_s"])
                    ceiling = {key: ceiling[key] for key in (
                        "records_per_s", "variant", "cell", "floor_records_per_s", "run_ids")}
                    result = run_one_soak(plan, "soak-buffered", rate, ceiling, output_dir,
                                          report_dir, input_s=input_s)
                    state.record("buffered", result["run_id"])
                if "alloy" in steps:
                    state.record("alloy", alloy_trial(plan, output_dir, report_dir, options))
        finally:
            capacity.close_plan(plan)
    for step, topology in (("pr_strict", "strict"), ("pr_buffered", "buffered")):
        if step in steps:
            result = pr_soak_case(output_dir, report_dir, topology=topology,
                                  **{k: v for k, v in options.items()
                                     if k in ("lease_wait_s", "publish", "archive_dir")})
            state.record(step, result["run_id"])
    if "publish" in steps:
        return publish(state, output_dir, report_dir)
    return []


def alloy_trial(plan, output_dir, report_dir, options) -> str:
    """The Alloy producer compatibility trial on the soak cell.

    Alloy runs its shipped batching and retry configuration; the receiver's
    decoding limit is raised to 16 MiB, since the shipped 4 MiB refuses every
    one of Alloy's shipped batches (harness README, "Capacity").
    """
    try:
        from . import alloy_capacity
    except ImportError:
        import alloy_capacity
    state = capacity.FamilyState(Path(output_dir) / "alloy")
    alloy_options = {
        "alloy_rate": int(options.get("alloy_rate", ALLOY_RATE_LINES_PER_S)),
        "alloy_variants": list(options.get("alloy_variants", ("decoding_16mib",))),
    }
    alloy_capacity.step_alloy(plan, state, Path(output_dir) / "alloy", report_dir, CELL,
                              alloy_options)
    run_id = state.trials(CELL)[-1]["run_id"]
    _ = shutil.copyfile(Path(output_dir) / "alloy" / f"{run_id}.json",
                        Path(output_dir) / f"{run_id}.json")
    return run_id


def publish(state, output_dir, report_dir) -> list:
    """`soak-strict.json` and `soak-buffered.json` over the recorded runs."""
    command = capacity._command()
    output_dir = Path(output_dir)

    def load(run_id):
        return json.loads((output_dir / f"{run_id}.json").read_text(encoding="ascii"))

    indexes = []
    bracket = state.get("bracket") or {}
    families = (
        ("soak-strict", [state.get("strict"), state.get("pr_strict"), state.get("alloy")]),
        ("soak-buffered", [state.get("buffered"), state.get("pr_buffered")]
         + list(bracket.get("run_ids") or [])),
    )
    for case, run_ids in families:
        children = [load(run_id) for run_id in run_ids if run_id]
        if not children:
            continue
        indexes.append(command.write_index(case, output_dir, report_dir, children,
                                           publishable=True))
    return indexes


def soak_case(case, output_dir, report_dir=None, **options) -> dict:
    """One registered soak case: its run result (the family index is the
    `soak` command's)."""
    steps = {"soak-strict": ["strict"], "soak-buffered": ["bracket", "buffered"]}[case]
    output_dir = Path(output_dir)
    run_soak(output_dir, report_dir, steps=steps, **options)
    run_id = FamilyState(output_dir).get(steps[-1])
    return json.loads((output_dir / f"{run_id}.json").read_text(encoding="ascii"))


soak_strict_case = functools.partial(soak_case, "soak-strict")
soak_buffered_case = functools.partial(soak_case, "soak-buffered")


# --------------------------------------------------------------------------
# The PR-tier soak: one minute through a store outage
# --------------------------------------------------------------------------

PR_RATE_REQUESTS_PER_S = 20
PR_INPUT_S = 65
PR_RECORDS_PER_REQUEST = 100
# Every tenth request is a metrics request, so a block of six requests holds
# six logs requests (rotated on bytes) or five and one metrics request
# (rotated on requests).
PR_WORKLOAD = measurement.Workload(
    requests=PR_RATE_REQUESTS_PER_S * PR_INPUT_S, records_per_request=PR_RECORDS_PER_REQUEST,
    body_bytes=1024, series=100, metrics_every=10,
)
PR_FLUSH_DEADLINE_S = 3
PR_WINDOW = {
    "flush_retry_deadline": f"{PR_FLUSH_DEADLINE_S}s",
    "max_block_bytes": "640KiB",
    "max_requests_per_block": 6,
}
PR_INGRESS = {"max_extracted_bytes": "320KiB"}
# Strictly below the flush deadline, which the exporter requires of a cloud store.
PR_RETRY = {
    "max_retries": 1, "init_backoff": "100ms", "max_backoff": "500ms",
    "backoff_base": 2.0, "retry_timeout": "2s",
}
PR_PRODUCER_ATTEMPTS = 40
PR_PRODUCER_TIMEOUT_S = 30.0
PR_GATE_DEADLINE_S = 60
PR_DIRECTIONS = {
    "records_acked_records": measurement.HIGHER_IS_BETTER,
    "peak_rss_bytes": measurement.LOWER_IS_BETTER,
    "accounted_peak_bytes": measurement.LOWER_IS_BETTER,
    "oldest_unacked_age_max_s": measurement.LOWER_IS_BETTER,
    "missing_records": measurement.LOWER_IS_BETTER,
    "unexpected_records": measurement.LOWER_IS_BETTER,
    "corrupt_records": measurement.LOWER_IS_BETTER,
}


def byte_size(text) -> int:
    """`640KiB` and the like as bytes."""
    units = {"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}
    for suffix, factor in units.items():
        if str(text).endswith(suffix):
            return int(str(text)[:-len(suffix)]) * factor
    return int(text)


def pr_soak_spec(topology, **options) -> measurement.RunSpec:
    command = capacity._command()
    cores = tuple(options.get("cores") or command.default_engine_cores(1))
    case = f"pr-soak-{topology}"
    return measurement.RunSpec(
        run_id=measurement.RunSpec.build_run_id(
            case, topology, "minio", cores, 1, int(options.get("ordinal", 1))),
        case=case, topology=topology, store="minio", cores=cores, workload=PR_WORKLOAD,
        interval_s=1, duration_s=PR_INPUT_S, producer_timeout_s=PR_PRODUCER_TIMEOUT_S,
        max_in_flight=128,
        overrides={
            "rate_requests_per_s": PR_RATE_REQUESTS_PER_S,
            "window": PR_WINDOW, "ingress": PR_INGRESS, "retry": PR_RETRY,
            "producer_attempts": PR_PRODUCER_ATTEMPTS,
            "outage": "DockerStore.stop after a completed object and a nonempty ACTIVE "
            "block; DockerStore.recover after a flush failure and the flush deadline",
        },
    )


def _telemetry(engine) -> dict:
    """One worker's gauges and capacity extras, read now."""
    document = test_e2e.engine_metrics(engine)
    extras = capacity.telemetry_extras(document)
    parsed = measurement.sample_document(document, expected_workers=None)
    return {"extras": extras, "workers": parsed["workers"]}


def drive_outage(store, engine, *, deadline_s=PR_GATE_DEADLINE_S) -> dict:
    """Stop the store once it holds an object and a block is ACTIVE; start
    it again once the exporter reported a failed flush and the flush
    deadline has passed since the stop."""

    def observe_stop():
        objects = len(performance.store_objects(store))
        telemetry = _telemetry(engine)
        active = sum(w["gauges"]["block.active"] for w in telemetry["workers"].values())
        return {"objects_count": objects, "active_bytes": active}

    before = measurement.wait_until(
        observe_stop, lambda v: v["objects_count"] > 0 and v["active_bytes"] > 0,
        deadline_ns=time.monotonic_ns() + deadline_s * 10**9,
        description="a completed object and a nonempty ACTIVE block before the outage",
    )
    stopped_ns = time.monotonic_ns()
    store.stop()

    def observe_failure():
        extras = next(iter(_telemetry(engine)["extras"].values()), {})
        return {
            "elapsed_s": (time.monotonic_ns() - stopped_ns) / 1e9,
            "flush_failures": dict(extras.get("flush.failures.by_class") or {}),
            "nacks": dict(extras.get("nacks") or {}),
        }

    failure = measurement.wait_until(
        observe_failure,
        lambda v: sum(v["flush_failures"].values()) >= 1
        and v["elapsed_s"] >= PR_FLUSH_DEADLINE_S,
        deadline_ns=stopped_ns + deadline_s * 10**9,
        description="a failed flush and the elapsed flush deadline during the outage",
    )
    recovered_ns = time.monotonic_ns()
    store.recover()
    return {"before_stop": before, "stopped_ns": stopped_ns, "failure": failure,
            "recovered_ns": recovered_ns, "ready_ns": time.monotonic_ns(),
            "outage_s": (recovered_ns - stopped_ns) / 1e9}


def pr_soak_experiment(spec, result, output_dir, controls, *, publishable, provenance):
    """A PR-tier soak: forced rotations, one store outage, recovery, drain.

    The producer sends a finite ledgered workload at a fixed rate and, in
    the strict topology, resends each retryably refused request with its
    original bytes. The outage starts on an observed condition and ends on
    one, never after a fixed sleep.
    """
    command = capacity._command()
    output_dir = Path(output_dir)
    controls.coverage_gaps_hard = publishable
    buffered = spec.topology == "buffered"
    topology = measurement.core_topology()
    allocation = measurement.role_allocation(
        topology["sibling_groups"], sorted(os.sched_getaffinity(0)), spec.cores,
        roles=measurement.CASE_ROLES["pr_soak"], strict=publishable,
    )
    controls.allocate(allocation)
    controls.register("harness", os.getpid())
    result["environment"]["build"] = provenance["build"]
    result["environment"]["git"] = provenance["git"]
    ledger = measurement.Ledger(output_dir / "ledger.sqlite")
    phase = None
    engine = None
    outage = None
    extras_samples = []
    with test_e2e.DockerStore("minio", by_image_id=True) as store:
        store_pid = performance.container_pid(store.container)
        store_cores = allocation.get("store", [])
        pinned = performance.pin_container(store.container, store_cores)
        if store_pid and store_cores:
            controls.register("store", store_pid, store_cores)
        result["ephemeral_values"] = {"<store_endpoint>": store.endpoint}
        root = output_dir / "engine-1"
        root.mkdir(parents=True, exist_ok=True)
        with capacity.malloc_conf(capacity.JEMALLOC_STATS_CONF):
            engine = test_e2e.Engine(
                root, storage=store.storage, overrides={"retry": PR_RETRY},
                interval=f"{spec.interval_s}s", topology=spec.topology,
                buffer_path=output_dir / "buffer" if buffered else None,
                cores=list(spec.cores),
                merge={"exporter": {"window": PR_WINDOW, "ingress": PR_INGRESS}},
                binary=Path(provenance["build"]["binary"]),
            )
        try:
            result["config"]["effective"] = engine.config
            result["config"]["effective_sha256"] = engine.config_sha256
            result["config"]["edges"] = [list(edge) for edge in engine.edges]
            result["config"]["malloc_conf"] = capacity.JEMALLOC_STATS_CONF
            result["config"]["store_pinned"] = pinned
            result["ephemeral_values"]["<receiver_listening_addr>"] = \
                f"127.0.0.1:{engine.grpc_port}"
            command.record_graph(result, engine, spec.topology)
            phase = capacity.CapacityPhase(command, "engine-1", engine, spec, controls,
                                           buffered)
            phase.ready("start")
            producer = measurement.Producer(
                engine.channel, ledger, spec.workload, cores=allocation.get("producer", []),
                timeout_s=spec.producer_timeout_s, max_in_flight=spec.max_in_flight,
                retry_attempts=PR_PRODUCER_ATTEMPTS if not buffered else 1,
            )
            _ = command.await_window_start(spec.interval_s)
            controls.raise_if_invalid()
            sent = {}

            def produce():
                sent["outcome"] = producer.send_paced(
                    range(spec.workload.requests), PR_RATE_REQUESTS_PER_S)

            sender = threading.Thread(target=produce, name="pr-producer")
            sender.start()
            try:
                outage = drive_outage(store, engine)
                command.record_event(result, "store_stopped", json.dumps(outage["before_stop"]))
                command.record_event(result, "store_recovered", json.dumps(outage["failure"]))
            finally:
                sender.join()
            if "outcome" not in sent:
                raise AssertionError("the producer did not finish")
            phase.inputs.append(sent["outcome"])
            controls.raise_if_invalid()
            _ = phase.drained()
            outage["drained_ns"] = time.monotonic_ns()
            _ = controls.snapshot(
                "end", {"engine": (engine.pid, list(spec.cores))},
                workers=phase.workers, requested_cores=list(spec.cores),
            )
            controls.unwatch_workers()
            engine.shutdown(command.SHUTDOWN_DEADLINE_S)
            command.record_event(result, "engine_shut_down", str(engine.pid))
        finally:
            controls.unwatch_workers()
            if phase is not None and phase.sampler is not None:
                phase.sampler.stop()
            engine.close()
        local = output_dir / "store"
        store.download(local)
    samples = list(phase.sampler.samples)
    extras_samples = [s for s in samples if s.get("extras")]
    summary = phase.summary()
    residuals, heap = capacity.trial_residuals(samples, phase.capacity_idle,
                                               getattr(phase.sampler, "pairs", ()))
    summary["residuals"] = residuals
    result["observations"] = {"phases": [summary]}
    result["samples"] = [dict(measurement.compact_sample(s), extras=s.get("extras"))
                         for s in samples[::capacity.PUBLISHED_SAMPLE_STRIDE]]
    oracle = measurement.run_pinned(
        allocation.get("reader") or sorted(os.sched_getaffinity(0)),
        measurement.read_oracle, local, ledger, require_all=True, healthy=False,
        workload=spec.workload,
    )
    shutil.rmtree(local, ignore_errors=True)
    counts = ledger.counts()
    latencies = ledger.acknowledgement_latencies_s()
    ledger.close()
    command.settle_local_result(result, spec, [phase], oracle, counts, latencies, output_dir,
                                replayable=True)
    settle_pr_soak(result, spec, extras_samples, outage, counts, heap)


def settle_pr_soak(result, spec, samples, outage, counts, heap):
    """The PR-tier checks and compared figures beside the common ones."""
    buffered = spec.topology == "buffered"
    final = samples[-1]["extras"] if samples else {}
    worker = next(iter(final.values()), {})
    flushes = worker.get("flushes") or {}
    failures = worker.get("flush.failures.by_class") or {}
    nacks = worker.get("nacks") or {}
    max_block = byte_size(PR_WINDOW["max_block_bytes"])
    over = []
    for sample in samples:
        for key, entry in sample["extras"].items():
            if entry.get("memory.accounted", 0) > entry.get("memory.budget", math.inf):
                over.append(f"{key}: accounted over budget")
            if entry.get("block.active", 0) > max_block:
                over.append(f"{key}: ACTIVE {entry['block.active']} over {max_block}")
            cap = entry.get("buffer.storage.bytes.cap")
            if buffered and cap and entry.get("buffer.storage.bytes.used", 0) > cap:
                over.append(f"{key}: buffer over its size cap")
    checks = result["checks"]

    def hard(name, passed, detail):
        checks.append(measurement.check(
            name, measurement.CHECK_HARD,
            measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED, detail))

    hard("rotations_forced", flushes.get("bytes", 0) > 0 and flushes.get("requests", 0) > 0,
         f"flushes by reason {flushes}")
    gated = bool(outage) and outage["before_stop"]["objects_count"] > 0 \
        and outage["before_stop"]["active_bytes"] > 0 \
        and outage["failure"]["elapsed_s"] >= PR_FLUSH_DEADLINE_S
    hard("outage_gated", gated, json.dumps(outage, sort_keys=True)[:400] if outage else "none")
    retryable = counts["attempts_by_outcome"].get(measurement.OUTCOME_RETRYABLE, 0)
    if buffered:
        retries = worker.get("buffer.retries.scheduled", 0)
        hard("buffer_retried", sum(failures.values()) > 0 and nacks.get("storage", 0) > 0
             and retries > 0,
             f"flush failures {failures}; exporter nacks {nacks}; buffer retries {retries}")
    else:
        hard("retryable_refusal_classified",
             failures.get("deadline", 0) > 0 and nacks.get("storage", 0) > 0 and retryable > 0,
             f"flush failures {failures}; exporter nacks {nacks}; producer retryable "
             f"attempts {retryable}")
    hard("retained_state_caps", not over, "; ".join(over[:5]) or
         f"{len(samples)} samples within the budget, the block limit and the buffer cap")
    metrics = result["metrics"]
    observations = result["observations"]
    for name in ("throughput_records_per_s", "ack_latency_p50_s", "ack_latency_p99_s"):
        observations[name] = metrics.pop(name, None)
    oldest = [w["gauges"]["oldest_unacked.age"] for s in samples
              for w in (s.get("workers") or {}).values() if "gauges" in w]
    accounted = [capacity.extras_total(s, "memory.accounted") for s in samples]
    metrics["oldest_unacked_age_max_s"] = max(oldest) if oldest else None
    metrics["accounted_peak_bytes"] = max(accounted) if accounted else None
    for name, value in metrics.items():
        if value is None:
            result["metrics_unavailable"][name] = "not observed in this run"
        else:
            result["metrics_unavailable"].pop(name, None)
    result["metric_directions"] = {name: PR_DIRECTIONS[name] for name in metrics}
    result["mandatory_metrics"] = sorted(name for name, value in metrics.items()
                                         if value is not None)
    observations["pr_soak"] = {
        "outage": outage, "flushes_by_reason": flushes, "flush_failures_by_class": failures,
        "exporter_nacks_by_class": nacks, "producer_attempts_by_outcome":
        counts["attempts_by_outcome"], "rss_heap_term": heap,
        "buffer_retries_scheduled": worker.get("buffer.retries.scheduled"),
    }


def pr_soak_case(output_dir, report_dir=None, *, topology, **options) -> dict:
    """One PR-tier soak.

    `publish=false` is the CI mode, as for `launcher-ci`: nothing reaches the
    report directory, no baseline is evaluated, the host's core floor is not
    enforced and the engine may be the fixture suite's debug build.
    """
    command = capacity._command()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    publishable = bool(options.pop("publish", True))
    if not publishable:
        report_dir = output_dir
    provenance = command.prepare_build(require_release=publishable)
    if "ordinal" not in options:
        options["ordinal"] = capacity.next_ordinal_factory(
            measurement.resolve_report_dir(report_dir), output_dir,
            prefix=f"pr-soak-{topology}-")()
    spec = pr_soak_spec(topology, **options)
    run_dir = output_dir / spec.run_id

    def experiment(spec_, result, directory, controls):
        pr_soak_experiment(spec_, result, directory, controls, publishable=publishable,
                           provenance=provenance)

    try:
        result = command.run_case(
            spec, run_dir, experiment=experiment, report_dir=report_dir,
            evaluate=publishable, lease_wait_s=float(options.get("lease_wait_s", 3600.0)),
        )
    except Exception as error:  # noqa: BLE001 - recorded in the result
        sys.stderr.write(f"{spec.run_id}: {type(error).__name__}: {error}\n")
        result = json.loads((run_dir / f"{spec.run_id}.json").read_text(encoding="ascii"))
    for entry in [{"name": f"{spec.run_id}.json"}] + result["baseline_files"]:
        _ = shutil.copyfile(run_dir / entry["name"], output_dir / entry["name"])
    if publishable:
        capacity.archive_trial({"archive_dir": options.get("archive_dir", ARCHIVE_DIR_DEFAULT)},
                               run_dir)
    return result


pr_soak_strict_case = functools.partial(pr_soak_case, topology="strict")
pr_soak_buffered_case = functools.partial(pr_soak_case, topology="buffered")
