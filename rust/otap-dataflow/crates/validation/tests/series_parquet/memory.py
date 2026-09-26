# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Retained memory, transients and the RSS residual of the real engine.

Run it from the Rust workspace, after building the release engine and the
measurement bench:

    SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure \\
        memory --output-dir /tmp/series-memory

One pair is two fresh release engines on the same cores under the same
offered workload: a control whose exporter is the noop exporter, and the
measured strict or buffered pipeline. Both run the same jemalloc allocator
with its periodic statistics printed to the engine log, and both publish
jemalloc's resident bytes through the engine's own memory limiter
(`source: jemalloc_resident`, observe-only), so every sample splits the
process into terms that are measured rather than assumed:

    RSS
      = non-heap resident      (every mapping but the allocator's anonymous
                                extents: binary and library text and data,
                                thread stacks, kernel pages; from smaps)
      + allocator retention    (resident allocator extents - jemalloc
                                allocated: metadata, dirty pages kept for
                                reuse, fragmentation inside active pages)
      + net untracked heap     (jemalloc allocated - the pipeline threads'
                                tracked heap: other threads' live heap, less
                                bytes a pipeline thread allocated and another
                                thread freed, which its counter keeps)
      + tracked heap           (the engine's per-pipeline-thread counters)

and the exporter's accounted bytes are compared against the heap that is
really live. Nothing here changes the engine; the allocator statistics come
from jemalloc's own `stats_interval` option, printed after every few MiB of
allocation, so they are also a synchronous observation of transients that a
100 ms sampler misses. The engine's own `process.memory.usage.bytes` is
recorded too, but the controller refreshes it on a fixed five-second timer,
so it is never paired with a 100 ms sample.
"""
import collections
import contextlib
import concurrent.futures
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
import urllib.request

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement


# --------------------------------------------------------------------------
# The accounting ledger, transcribed from the worker
# --------------------------------------------------------------------------

# `worker.rs` constants the ledger is written against.
CACHE_ENTRY_BYTES = 128
FIXED_WORKSPACE_BYTES = 64 * 1024 * 1024

BYTE_UNITS = {
    "": 1, "b": 1,
    "kb": 1000, "mb": 1000**2, "gb": 1000**3,
    "kib": 1024, "mib": 1024**2, "gib": 1024**3,
}
BYTE_VALUE = re.compile(r"^\s*(\d+)\s*([A-Za-z]*)\s*$")


def parse_bytes(value) -> int:
    """A configuration byte size, either a number or `<n><unit>`."""
    if isinstance(value, bool):
        raise ValueError(f"not a byte size: {value!r}")
    if isinstance(value, int):
        return value
    match = BYTE_VALUE.match(str(value))
    if not match or match.group(2).lower() not in BYTE_UNITS:
        raise ValueError(f"not a byte size: {value!r}")
    return int(match.group(1)) * BYTE_UNITS[match.group(2).lower()]


def ledger_inputs(exporter_config) -> dict:
    """The configured sizes the worker's reservation is written in terms of.

    `B` maximum block bytes, `E` maximum extracted request bytes, `C` cache
    capacity, `N` requests per block, `R` run target, `M` merge chunk
    target, `W` writer limit, `P` upload part bytes, `U` upload concurrency
    and `I` maximum wire request bytes.
    """
    window = exporter_config["window"]
    ingress = exporter_config["ingress"]
    sorting = exporter_config["sorting"]
    return {
        "B": parse_bytes(window["max_block_bytes"]),
        "E": parse_bytes(ingress["max_extracted_bytes"]),
        "C": int(exporter_config["series_cache"]["max_entries"]),
        "N": int(window["max_requests_per_block"]),
        "R": parse_bytes(sorting["run_target_bytes"]),
        "M": parse_bytes(sorting["merge_chunk_bytes"]),
        "W": parse_bytes(exporter_config["parquet"]["writer_limit_bytes"]),
        "P": parse_bytes(exporter_config["upload"]["part_bytes"]),
        "U": int(exporter_config["upload"]["concurrency"]),
        "I": parse_bytes(ingress["max_request_bytes"]),
    }


def retained_reservation(inputs, token_high_water) -> int:
    """`2*B + E + 128*C + 2*N*T`: what a worker may retain."""
    return (
        2 * inputs["B"] + inputs["E"] + CACHE_ENTRY_BYTES * inputs["C"]
        + 2 * inputs["N"] * int(token_high_water)
    )


def workspace_reservation(inputs) -> int:
    """`2*R + 2*M + 3*W + P*(U+1) + M + 4*I + 64MiB`: transient workspace."""
    return (
        2 * inputs["R"] + 2 * inputs["M"] + 3 * inputs["W"]
        + inputs["P"] * (inputs["U"] + 1) + inputs["M"] + 4 * inputs["I"]
        + FIXED_WORKSPACE_BYTES
    )


def token_high_water_of(inputs, budget_bytes):
    """The retained token high-water `T` a reported budget implies.

    The budget is the two reservations added; everything but `T` is
    configuration, so a reported budget either yields a whole non-negative
    `T` or proves the transcription wrong. Returns None in the second case.
    """
    rest = retained_reservation(inputs, 0) + workspace_reservation(inputs)
    remainder = int(budget_bytes) - rest
    step = 2 * inputs["N"]
    if remainder < 0 or step <= 0 or remainder % step:
        return None
    return remainder // step


def residual(rss, accounted, runtime, buffer, workspace, allocator) -> int:
    """The signed resident bytes no named term explains.

    Every term is subtracted as given and nothing is clamped: a term that
    counts the same allocation twice drives the result negative, and that
    negative is the evidence of the double count.
    """
    return rss - accounted - runtime - buffer - workspace - allocator


# --------------------------------------------------------------------------
# Sources of one sample
# --------------------------------------------------------------------------

# The allocator statistics every engine of a pair prints: the totals only
# (`J` JSON, and every per-arena, bin, extent and mutex section omitted),
# after roughly this many bytes of allocation activity. Read-only: the
# option changes neither allocation nor purging.
JEMALLOC_STATS_INTERVAL_BYTES = 4 * 1024 * 1024
JEMALLOC_STATS_CONF = (
    f"stats_interval:{JEMALLOC_STATS_INTERVAL_BYTES},"
    "stats_interval_opts:Jgmdablxeh"
)

# Diagnostic allocator settings. `decay0` returns every freed page at once,
# which removes allocator retention by construction; `prof` samples live
# allocations with their stacks for the heap-profile endpoint.
DIAGNOSTIC_CONF = {
    "decay0": JEMALLOC_STATS_CONF + ",dirty_decay_ms:0,muzzy_decay_ms:0",
    # jemalloc's own purging thread: without it, dirty pages decay only when
    # the process allocates, so an idle engine keeps a varying amount.
    "bgthread": JEMALLOC_STATS_CONF + ",background_thread:true",
    # The thread explicitly off, as every engine ran before it became the
    # default: what a family measured then is re-run under this.
    "nobgthread": JEMALLOC_STATS_CONF + ",background_thread:false",
    "prof": JEMALLOC_STATS_CONF + ",prof:true,prof_active:true,lg_prof_sample:17",
}

JEMALLOC_TOTALS = re.compile(
    r'\{"jemalloc":\{"stats":\{"allocated":(\d+),"active":(\d+),'
    r'"metadata":(\d+),.*?"resident":(\d+),"mapped":(\d+),"retained":(\d+)'
)


class JemallocStats:
    """The allocator totals the engine prints into its own log.

    Each call returns the totals printed since the previous call. The log is
    read incrementally, so a print split across two reads is completed by
    the next one rather than lost.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.offset = 0
        self.pending = ""
        self.count = 0

    def poll(self) -> list:
        """Every complete totals record appended since the last poll."""
        try:
            with open(self.path, "rb") as handle:
                _ = handle.seek(self.offset)
                data = handle.read()
        except OSError:
            return []
        self.offset += len(data)
        text = self.pending + data.decode("ascii", "replace")
        found = []
        end = 0
        for match in JEMALLOC_TOTALS.finditer(text):
            allocated, active, metadata, resident, mapped, retained = (
                int(value) for value in match.groups()
            )
            found.append(
                {
                    "allocated_bytes": allocated,
                    "active_bytes": active,
                    "metadata_bytes": metadata,
                    "resident_bytes": resident,
                    "mapped_bytes": mapped,
                    "retained_bytes": retained,
                }
            )
            end = match.end()
        # Keep an unfinished record for the next read, but never an
        # unbounded tail of ordinary log text.
        tail = text[end:]
        start = tail.rfind('{"jemalloc"')
        self.pending = tail[start:] if start >= 0 else ""
        if len(self.pending) > 65536:
            self.pending = ""
        self.count += len(found)
        return found


def smaps_categories(pid, *, binary=None) -> dict:
    """Resident bytes of one process by what the mappings are.

    Thread stacks are anonymous writable mappings that directly follow a
    small inaccessible guard mapping, which is how the C library lays out
    every thread it creates. jemalloc places no guard pages, so its extents
    fall under `anonymous_other`.
    """
    categories = collections.Counter()
    stacks = 0
    previous = None
    current = None
    try:
        text = Path(f"/proc/{pid}/smaps").read_text(encoding="ascii", errors="replace")
    except OSError as error:
        return {"reason": str(error)}

    def settle(entry):
        """Add one finished mapping to its category."""
        nonlocal stacks
        if entry is None:
            return
        name = entry["name"]
        rss = entry.get("rss", 0)
        if name.startswith("/"):
            key = "binary" if binary and name == str(binary) else "file"
            categories[f"{key}_file_bytes"] += rss - entry.get("anonymous", 0)
            categories[f"{key}_anonymous_bytes"] += entry.get("anonymous", 0)
        elif name == "[heap]":
            categories["brk_heap_bytes"] += rss
        elif name == "[stack]":
            categories["main_stack_bytes"] += rss
        elif name.startswith("["):
            categories["kernel_bytes"] += rss
        elif entry["guarded"]:
            categories["thread_stack_bytes"] += rss
            stacks += 1
        else:
            categories["anonymous_other_bytes"] += rss

    for line in text.splitlines():
        # A mapping header starts with a lower-case hex address; every field
        # line starts with an upper-case name, and only two of them matter.
        if not line:
            continue
        if line[0] in "0123456789abcdef":
            head = line.split(None, 5)
            settle(current)
            low, high = (int(part, 16) for part in head[0].split("-"))
            perms = head[1]
            name = head[5].strip() if len(head) > 5 else ""
            guarded = (
                previous is not None
                and previous["perms"].startswith("---")
                and previous["high"] == low
                and previous["high"] - previous["low"] <= 64 * 1024
                and perms.startswith("rw")
                and not name
            )
            current = {
                "low": low, "high": high, "perms": perms, "name": name,
                "guarded": guarded,
            }
            previous = current
        elif current is not None and line.startswith(("Rss:", "Anonymous:")):
            key, value = line.split(":", 1)
            current[key.lower()] = int(value.split()[0]) * 1024
    settle(current)
    result = dict(categories)
    result["thread_stack_count"] = stacks
    result["rss_total_bytes"] = sum(
        value for key, value in categories.items() if key.endswith("_bytes")
    )
    return result


def interpolate_smaps(last, procfs):
    """This instant's categories from the rollup and the last classification.

    The rollup gives the total resident and anonymous bytes now. Every
    category but the allocator's anonymous extents is taken from the last
    full read, and the extents are what remains of the anonymous total, so
    the categories still add up to the rollup RSS.
    """
    if not last or "rss_total_bytes" not in last:
        return None
    rss = procfs.get("smaps_rss_bytes")
    anonymous = procfs.get("smaps_anonymous_bytes")
    if rss is None or anonymous is None:
        return None
    result = dict(last, source="rollup")
    other_anonymous = sum(
        last.get(name, 0) for name in (
            "binary_anonymous_bytes", "file_anonymous_bytes", "brk_heap_bytes",
            "main_stack_bytes", "thread_stack_bytes",
        )
    )
    result["anonymous_other_bytes"] = anonymous - other_anonymous
    fixed = sum(
        value for key, value in last.items()
        if key.endswith("_bytes") and key not in ("anonymous_other_bytes", "rss_total_bytes")
    )
    result["rss_total_bytes"] = fixed + result["anonymous_other_bytes"]
    # File-backed pages the rollup saw beyond the last read are attributed
    # to the binary, whose text is what faults in as new paths run.
    result["binary_file_bytes"] = last.get("binary_file_bytes", 0) + (
        rss - result["rss_total_bytes"]
    )
    result["rss_total_bytes"] = rss
    return result


def _metric(worker, slot):
    """The sum of one worker metric over its entities, or None when absent."""
    entities = worker["metrics"].get(slot)
    if not entities:
        return None
    values = [value for value in entities.values() if isinstance(value, (int, float))]
    return sum(values) if values else None


def telemetry_terms(document) -> dict:
    """The memory terms one telemetry response reports, leniently.

    The control engine runs the noop exporter, so the exporter's gauges may
    be absent; everything the response does carry is kept, and an absent
    value is None, never zero.
    """
    parsed = measurement.parse_telemetry(
        document, expected_workers=None, include_system=True
    )
    process = parsed["process"]
    pipelines = {}
    exporter = collections.Counter()
    exporter_seen = False
    for key, worker in parsed["workers"].items():
        usage = _metric(worker, "pipeline:memory.usage")
        pipelines[key] = {
            "group_id": worker["group_id"],
            "pipeline_id": worker["pipeline_id"],
            "core_id": worker["core_id"],
            "generation": worker["generation"],
            "uptime_s": _metric(worker, "pipeline:uptime"),
            "memory_usage_bytes": usage,
        }
        for slot, entities in worker["metrics"].items():
            if slot.startswith(measurement.test_e2e.EXPORTER_METRIC_SET + ":"):
                exporter_seen = True
                exporter[slot.split(":", 1)[1]] += sum(
                    value for value in entities.values()
                    if isinstance(value, (int, float))
                )
    tracked = [
        entry["memory_usage_bytes"] for entry in pipelines.values()
        if entry["memory_usage_bytes"] is not None
    ]
    return {
        "engine_rss_bytes": process.get("engine.memory.rss"),
        "jemalloc_resident_bytes": process.get("engine.process.memory.usage.bytes"),
        "tracked_heap_bytes": sum(tracked) if tracked else None,
        "pipelines": pipelines,
        "exporter": dict(exporter) if exporter_seen else None,
    }


def engine_workers(document) -> list:
    """The measured pipeline's worker identities, for the affinity checks."""
    parsed = measurement.parse_telemetry(document, expected_workers=None)
    return [
        {
            "key": worker["key"],
            "group_id": worker["group_id"],
            "pipeline_id": worker["pipeline_id"],
            "core_id": worker["core_id"],
            "generation": worker["generation"],
        }
        for _key, worker in sorted(parsed["workers"].items())
    ]


# --------------------------------------------------------------------------
# Sampling one engine lifetime
# --------------------------------------------------------------------------

# How often the sampler observes the engine, and how often the engine
# collects its own telemetry and samples jemalloc's resident bytes.
SAMPLE_PERIOD_S = 0.1
TELEMETRY_INTERVAL = "100ms"
LIMITER_CHECK_INTERVAL = "100ms"
# How often, in samples, the full mapping list is classified. The samples
# between read the cheap rollup and take the non-heap anonymous pages
# (stacks, binary data) from the latest classification, which change only
# when a thread starts or exits.
SMAPS_EVERY = 10

# The fewest paired observations, and fresh telemetry epochs, a phase must
# hold to count as measured.
MINIMUM_PHASE_SAMPLES = 30
# The phases every lifetime must cover, and the one the measured exporter
# adds while a block is flushing.
REQUIRED_PHASES = ("idle", "load", "retained")
FLUSH_PHASE = "flush"


class MemorySampler:
    """Every memory term of one engine, on one timeline, every 100 ms."""

    def __init__(self, engine, *, period_s=SAMPLE_PERIOD_S):
        self.engine = engine
        self.period_s = period_s
        self.stats = JemallocStats(Path(engine.root) / "engine.log")
        self.phase = "startup"
        self.samples = []
        self.errors = []
        self.last_stats = None
        self.last_stats_ns = None
        self.last_smaps = None
        self._count = 0
        self._stop = threading.Event()
        self._thread = None

    def once(self) -> dict:
        """One sample of the telemetry, the process and the allocator."""
        started = time.monotonic_ns()
        document = measurement.test_e2e.engine_metrics(self.engine)
        telemetry = telemetry_terms(document)
        procfs = measurement.procfs_process_sample(self.engine.pid)
        now = time.monotonic_ns()
        printed = self.stats.poll()
        if printed:
            self.last_stats = printed[-1]
            self.last_stats_ns = now
        if self._count % SMAPS_EVERY == 0 or self.last_smaps is None:
            self.last_smaps = smaps_categories(self.engine.pid, binary=self.engine.binary)
            smaps = dict(self.last_smaps, source="smaps")
        else:
            smaps = interpolate_smaps(self.last_smaps, procfs)
        self._count += 1
        return {
            "monotonic_ns": now,
            "scrape_started_ns": started,
            "phase": self.phase,
            "telemetry": telemetry,
            "rss_bytes": procfs.get("smaps_rss_bytes"),
            "anonymous_bytes": procfs.get("smaps_anonymous_bytes"),
            "num_threads": procfs.get("num_threads"),
            "jemalloc": dict(self.last_stats) if self.last_stats else None,
            "jemalloc_age_ns": (now - self.last_stats_ns) if self.last_stats_ns else None,
            "jemalloc_prints": len(printed),
            "jemalloc_printed": printed,
            "smaps": smaps,
        }

    def _run(self):
        """Sample until stopped; a failed sample is an error, never a zero."""
        while not self._stop.is_set():
            try:
                self.samples.append(self.once())
            except Exception as error:
                self.errors.append(
                    {
                        "monotonic_ns": time.monotonic_ns(),
                        "phase": self.phase,
                        "error": f"{type(error).__name__}: {error}"[:300],
                    }
                )
            _ = self._stop.wait(self.period_s)

    def start(self):
        """Begin sampling in the background."""
        self._thread = threading.Thread(target=self._run, name="memory-sampler")
        self._thread.daemon = True
        self._thread.start()
        return self

    def stop(self):
        """Stop sampling and wait for the thread."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
            self._thread = None
        return self

    def count(self, phase) -> int:
        """Samples taken so far in one phase."""
        return sum(1 for sample in list(self.samples) if sample["phase"] == phase)

    def wait_for(self, phase, samples, *, deadline_s=60):
        """Stay in `phase` until it holds `samples` samples and fresh epochs."""
        deadline = time.monotonic() + deadline_s
        wake = threading.Event()
        while time.monotonic() < deadline:
            taken = [sample for sample in list(self.samples) if sample["phase"] == phase]
            if len(taken) >= samples and fresh_epochs(taken) >= samples:
                return len(taken)
            _ = wake.wait(self.period_s)
        raise AssertionError(
            f"phase {phase} did not reach {samples} samples and fresh epochs "
            f"within {deadline_s}s"
        )


def measured_uptime(sample):
    """The collection uptime of the measured pipeline in one sample."""
    for entry in sample["telemetry"]["pipelines"].values():
        if entry["group_id"] != measurement.SYSTEM_GROUP:
            return entry["uptime_s"]
    return None


def fresh_epochs(samples) -> int:
    """Samples whose telemetry comes from a collection not seen before."""
    fresh = 0
    last = None
    for sample in samples:
        uptime = measured_uptime(sample)
        if uptime is not None and uptime != last:
            fresh += 1
            last = uptime
    return fresh


def flushing(sample) -> bool:
    """Whether a flush happened during the interval this sample closes.

    Set by `annotate_flushes`. A 100 ms sampler rarely lands inside a flush
    of a block this size, because the flush holds the worker's thread until
    it awaits the store, so the worker answers no collection meanwhile; the
    flush is instead recognized by the active block emptying between two
    samples, and the allocator totals printed in that interval are the
    synchronous observations of it.
    """
    return bool(sample.get("flush_bracket"))


def annotate_flushes(samples) -> int:
    """Mark the samples whose interval contains a flush; return how many.

    A flush is either reported directly (`block.flushing` above zero) or
    seen as the active block shrinking between consecutive samples. The
    worker's gauges are up to one collection stale, so the sample after the
    one that shows the drop is marked too.
    """
    marked = 0
    previous = None
    carry = False
    for sample in samples:
        exporter = sample["telemetry"].get("exporter") or {}
        active = exporter.get("block.active")
        direct = (exporter.get("block.flushing") or 0) > 0
        dropped = (
            previous is not None and active is not None and previous > 0
            and active < previous
        )
        sample["flush_bracket"] = bool(direct or dropped or carry)
        carry = bool(direct or dropped)
        marked += sample["flush_bracket"]
        if active is not None:
            previous = active
    return marked


def sample_terms(sample, control=None) -> dict:
    """The measured split of one sample, every term in bytes.

    `control` is the paired control's heap in the same phase, the live heap
    the receiver, the runtime and the telemetry hold without the exporter.
    """
    telemetry = sample["telemetry"]
    smaps = sample.get("smaps") or {}
    # Every resident category comes from one read of the mapping list, so
    # they add up to its own RSS; the rollup RSS is the fallback.
    rss = smaps.get("rss_total_bytes") or sample["rss_bytes"]
    heap = smaps.get("anonymous_other_bytes") if smaps.get("rss_total_bytes") else None
    printed = sample.get("jemalloc") or {}
    allocated = printed.get("allocated_bytes")
    tracked = telemetry["tracked_heap_bytes"]
    exporter = telemetry.get("exporter") or {}
    accounted = exporter.get("memory.accounted", 0)
    terms = {
        "rss_bytes": rss,
        "heap_resident_bytes": heap,
        "jemalloc_resident_bytes": printed.get("resident_bytes"),
        "jemalloc_allocated_bytes": allocated,
        "jemalloc_metadata_bytes": printed.get("metadata_bytes"),
        "limiter_resident_bytes": telemetry["jemalloc_resident_bytes"],
        "tracked_heap_bytes": tracked,
        "accounted_bytes": accounted,
        # Inside `accounted` already: the write in progress's merge chunk,
        # encoder buffers and unacknowledged upload bytes. None for an
        # engine whose worker does not publish it.
        "flush_workspace_bytes": exporter.get("flush.workspace"),
        "budget_bytes": exporter.get("memory.budget"),
        "file_backed_bytes": (
            smaps.get("binary_file_bytes", 0) + smaps.get("file_file_bytes", 0)
            if smaps.get("rss_total_bytes") else (
                rss - sample["anonymous_bytes"]
                if rss is not None and sample.get("anonymous_bytes") is not None
                else None
            )
        ),
        "thread_stack_bytes": smaps.get("thread_stack_bytes"),
    }
    if heap is not None:
        terms["non_heap_bytes"] = rss - heap
    if heap is not None and allocated is not None:
        terms["allocator_retention_bytes"] = heap - allocated
    if heap is not None and printed.get("resident_bytes") is not None:
        # jemalloc documents `stats.resident` as a maximum: it counts pages
        # that were never touched as resident. This is by how much.
        terms["allocator_resident_overstatement_bytes"] = (
            printed["resident_bytes"] - heap
        )
    if allocated is not None and tracked is not None:
        terms["untracked_heap_bytes"] = allocated - tracked
    if tracked is not None:
        terms["tracked_minus_accounted_bytes"] = tracked - accounted
    if allocated is not None:
        terms["allocated_minus_accounted_bytes"] = allocated - accounted
    if control is not None and heap is not None and allocated is not None:
        # The Task 6 ledger: resident bytes the exporter's accounting, the
        # runtime outside the heap, the allocator and the control's heap do
        # not explain. A buffered pipeline's buffer heap has no separate
        # measurement here and so stays inside this residual.
        terms["unexplained_bytes"] = residual(
            rss, accounted, rss - heap, 0, control, heap - allocated
        )
    return terms


def summarize(values) -> dict:
    """Median and range of a list of numbers, None when there are none."""
    values = sorted(value for value in values if value is not None)
    if not values:
        return {"count": 0, "median": None, "min": None, "max": None}
    return {
        "count": len(values),
        "median": values[len(values) // 2] if len(values) % 2 else
        (values[len(values) // 2 - 1] + values[len(values) // 2]) / 2,
        "min": values[0],
        "max": values[-1],
    }


PHASE_TERMS = (
    "rss_bytes", "heap_resident_bytes", "jemalloc_resident_bytes",
    "jemalloc_allocated_bytes", "jemalloc_metadata_bytes", "limiter_resident_bytes",
    "tracked_heap_bytes", "accounted_bytes", "flush_workspace_bytes", "file_backed_bytes",
    "thread_stack_bytes", "non_heap_bytes", "allocator_retention_bytes",
    "allocator_resident_overstatement_bytes", "untracked_heap_bytes",
    "tracked_minus_accounted_bytes", "allocated_minus_accounted_bytes",
    "unexplained_bytes",
)


def phase_table(samples, controls=None) -> dict:
    """Per phase: sample and epoch counts, and every term's median and range."""
    by_phase = collections.defaultdict(list)
    for sample in samples:
        by_phase[sample["phase"]].append(sample)
        if flushing(sample):
            by_phase[FLUSH_PHASE].append(sample)
    table = {}
    for phase, taken in sorted(by_phase.items()):
        control = (controls or {}).get(phase if phase != FLUSH_PHASE else "load")
        terms = [sample_terms(sample, control) for sample in taken]
        printed = [entry for sample in taken for entry in sample.get("jemalloc_printed", [])]
        table[phase] = {
            "samples": len(taken),
            "fresh_epochs": fresh_epochs(taken),
            "jemalloc_prints": len(printed),
            # Every allocator print in the phase, not only the last one per
            # sample: the synchronous view of what the sampler interpolates.
            "printed_allocated_bytes": summarize(
                entry["allocated_bytes"] for entry in printed
            ),
            "printed_resident_bytes": summarize(
                entry["resident_bytes"] for entry in printed
            ),
            "terms": {
                name: summarize(entry.get(name) for entry in terms)
                for name in PHASE_TERMS
            },
        }
    return table


def control_heap_by_phase(samples) -> dict:
    """The control's median allocated heap in each phase."""
    by_phase = collections.defaultdict(list)
    for sample in samples:
        allocated = (sample.get("jemalloc") or {}).get("allocated_bytes")
        if allocated is not None:
            by_phase[sample["phase"]].append(allocated)
    return {phase: summarize(values)["median"] for phase, values in by_phase.items()}


def block_pair_uncertainty(samples) -> dict:
    """How far one sample's accounted bytes can be from its RSS instant.

    The worker publishes its accounted bytes only when it answers a
    CollectTelemetry, on the engine's reporting timer, while RSS is read by
    this sampler on its own timer. A sample therefore pairs RSS with an
    accounted value up to one collection old, which may describe the block
    before a flush started or after it finished. The bound is the change of
    the accounted (and tracked heap) value between consecutive collections.
    """
    changes = {"accounted_bytes": [], "tracked_heap_bytes": [], "rss_bytes": []}
    last = None
    for sample in samples:
        uptime = measured_uptime(sample)
        if uptime is None or (last is not None and uptime == last[0]):
            continue
        terms = sample_terms(sample)
        if last is not None:
            for name in changes:
                if terms.get(name) is not None and last[1].get(name) is not None:
                    changes[name].append(abs(terms[name] - last[1][name]))
        last = (uptime, terms)
    result = {}
    for name, values in changes.items():
        values = sorted(values)
        result[name] = {
            "epochs": len(values),
            "max_change_bytes": values[-1] if values else None,
            "p95_change_bytes": values[int(0.95 * (len(values) - 1))] if values else None,
        }
    return result


def gate_residuals(samples) -> list:
    """The harness's frozen RSS reconciliation, applied to these samples.

    The same definition `measurement.rss_residuals` applies to every engine
    run: RSS growth since the idle reference, less file-backed growth and
    less the tracked heap's high-water growth. The reference is the last
    idle sample, so the warm-up is excluded as it is in a measured run.
    """
    idle = [
        sample for sample in samples
        if sample["phase"] == "idle" and sample.get("smaps")
    ]
    if not idle:
        return []
    reference = sample_terms(idle[-1])
    heap_high = reference["tracked_heap_bytes"]
    # The same gauge as a one-second reporting interval would publish it:
    # refreshed only when a whole second of collection uptime has passed.
    gauge_high = reference["tracked_heap_bytes"]
    gauge_uptime = measured_uptime(idle[-1])
    entries = []
    for sample in samples:
        if sample["monotonic_ns"] <= idle[-1]["monotonic_ns"]:
            continue
        terms = sample_terms(sample)
        if None in (terms["rss_bytes"], terms["file_backed_bytes"], terms["tracked_heap_bytes"]):
            continue
        heap_high = max(heap_high, terms["tracked_heap_bytes"])
        uptime = measured_uptime(sample)
        if uptime is not None and gauge_uptime is not None and uptime - gauge_uptime >= 1.0:
            gauge_uptime = uptime
            gauge_high = max(gauge_high, terms["tracked_heap_bytes"])
        growth = terms["rss_bytes"] - reference["rss_bytes"]
        file_growth = terms["file_backed_bytes"] - reference["file_backed_bytes"]
        heap_growth = heap_high - reference["tracked_heap_bytes"]
        entry = {
            "monotonic_ns": sample["monotonic_ns"],
            "phase": sample["phase"],
            "rss_bytes": terms["rss_bytes"],
            "rss_growth_bytes": growth,
            "file_growth_bytes": file_growth,
            "heap_high_water_growth_bytes": heap_growth,
            "residual_bytes": growth - file_growth - heap_growth,
            "one_second_gauge_residual_bytes": (
                growth - file_growth - (gauge_high - reference["tracked_heap_bytes"])
            ),
        }
        # The same residual, decomposed by the mapping list and the
        # allocator's own figures: heap growth the tracked counters did not
        # see, and non-heap anonymous growth such as new thread stacks.
        if None not in (
            reference.get("heap_resident_bytes"), terms.get("heap_resident_bytes"),
            reference.get("allocator_retention_bytes"),
            terms.get("allocator_retention_bytes"),
        ):
            entry["heap_resident_growth_bytes"] = (
                terms["heap_resident_bytes"] - reference["heap_resident_bytes"]
            )
            entry["allocator_retention_growth_bytes"] = (
                terms["allocator_retention_bytes"] - reference["allocator_retention_bytes"]
            )
            entry["allocated_growth_bytes"] = (
                terms["jemalloc_allocated_bytes"] - reference["jemalloc_allocated_bytes"]
            )
            entry["non_heap_anonymous_growth_bytes"] = (
                terms["non_heap_bytes"] - reference["non_heap_bytes"] - file_growth
            )
            # residual = non-heap anonymous growth + retention growth
            #          + (allocated growth - tracked high-water growth), exactly;
            # the last term is live heap the pipeline threads' counters did
            # not show at their high-water.
            entry["heap_beyond_tracked_growth_bytes"] = (
                entry["allocated_growth_bytes"] - heap_growth
            )
        entries.append(entry)
    return entries


# --------------------------------------------------------------------------
# Producing a paired workload
# --------------------------------------------------------------------------


def prebuild(workload) -> dict:
    """Every request of a workload, built once before anything is measured.

    Building a request in Python costs about 100 microseconds per record,
    which at a measured rate would make the producer the bottleneck; the
    bytes are identical to what `Producer` would build and send.
    """
    return {
        index: measurement.build_request(workload, index)
        for index in range(workload.requests)
    }


class PrebuiltProducer(measurement.Producer):
    """`Producer`, sending prebuilt requests on a fixed offered schedule."""

    def __init__(self, *args, prebuilt, **kwargs):
        super().__init__(*args, **kwargs)
        self.prebuilt = prebuilt

    def send_one(self, index):
        """Send prebuilt request `index` once and ledger what happened."""
        signal, wire, rows = self.prebuilt[index]
        start = time.monotonic_ns()
        _ = self.ledger.add_request(index, signal, wire, rows, send_ns=start)
        try:
            response = self.calls[signal](wire, timeout=self.timeout_s)
        except measurement.test_e2e.grpc.RpcError as error:
            finish = time.monotonic_ns()
            code = error.code()
            outcome = (
                measurement.OUTCOME_RETRYABLE
                if code in measurement.test_e2e.RETRYABLE_CODES
                else measurement.OUTCOME_PERMANENT
            )
            self.ledger.attempt(index, 1, start, finish, outcome, str(code))
            return outcome
        except Exception as error:
            finish = time.monotonic_ns()
            self.ledger.attempt(
                index, 1, start, finish, measurement.OUTCOME_LOCAL,
                f"{type(error).__name__}",
            )
            return measurement.OUTCOME_LOCAL
        finish = time.monotonic_ns()
        rejected = getattr(
            response.partial_success, measurement.EXPORT_METHODS[signal][2], 0
        )
        if rejected:
            self.ledger.attempt(
                index, 1, start, finish, measurement.OUTCOME_PARTIAL,
                f"rejected={rejected}",
            )
            return measurement.OUTCOME_PARTIAL
        self.ledger.attempt(index, 1, start, finish, measurement.OUTCOME_ACK)
        self.ledger.ack(index, finish)
        return measurement.OUTCOME_ACK

    def send_scheduled(self, indexes, rate_per_s) -> dict:
        """Offer `indexes` at `rate_per_s`, at most `max_in_flight` at once.

        Request `n` is released at `n / rate` seconds after the start. A
        release that finds every in-flight slot busy waits for one, and how
        late the last release was is recorded, so a schedule the engine
        could not absorb is visible rather than silently stretched.
        """
        indexes = list(indexes)
        wake = threading.Event()
        slots = threading.Semaphore(self.max_in_flight)
        started = time.monotonic_ns()
        late_ns = 0

        def run(index):
            """Send one request and free its slot."""
            try:
                return self.send_one(index)
            finally:
                slots.release()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_in_flight, initializer=self._pin,
            thread_name_prefix="producer",
        ) as pool:
            futures = []
            for position, index in enumerate(indexes):
                due = started + int(position * 1e9 / rate_per_s)
                remaining = (due - time.monotonic_ns()) / 1e9
                if remaining > 0:
                    _ = wake.wait(remaining)
                _ = slots.acquire()
                late_ns = max(late_ns, time.monotonic_ns() - due)
                futures.append(pool.submit(run, index))
            outcomes = collections.Counter(future.result() for future in futures)
        return {
            "requests": len(indexes),
            "rate_requests_per_s": rate_per_s,
            "max_in_flight": self.max_in_flight,
            "started_ns": started,
            "finished_ns": time.monotonic_ns(),
            "latest_release_s": late_ns / 1e9,
            "outcomes": dict(outcomes),
        }


# --------------------------------------------------------------------------
# One engine lifetime
# --------------------------------------------------------------------------

LIMITER = {
    "mode": "observe_only",
    "source": "jemalloc_resident",
    "check_interval": LIMITER_CHECK_INTERVAL,
    # Far above anything a run reaches: the limiter only observes.
    "soft_limit": "60GiB",
    "hard_limit": "64GiB",
}
READY_DEADLINE_S = 60
DRAIN_DEADLINE_S = 120
SHUTDOWN_DEADLINE_S = 180


@contextlib.contextmanager
def allocator_environment(malloc_conf):
    """`MALLOC_CONF` for the engine launched inside, and only for it."""
    previous = os.environ.get("MALLOC_CONF")
    os.environ["MALLOC_CONF"] = malloc_conf
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MALLOC_CONF", None)
        else:
            os.environ["MALLOC_CONF"] = previous


def heap_profile(engine, path) -> dict:
    """Dump the live heap through the admin endpoint and summarize it."""
    url = f"http://127.0.0.1:{engine.admin_port}/api/v1/debug/pprof/heap"
    with urllib.request.urlopen(url, timeout=60) as response:
        body = response.read()
    Path(path).write_bytes(body)
    return pprof_summary(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)


class Lifetime:
    """One engine process of a pair: launch, warm, idle, load, drain, keep."""

    def __init__(self, label, topology, spec, directory, *, binary, malloc_conf,
                 buffer_path=None, storage=None):
        self.label = label
        self.topology = topology
        self.spec = spec
        self.directory = Path(directory)
        self.binary = binary
        self.malloc_conf = malloc_conf
        self.buffer_path = buffer_path
        self.storage = storage
        self.engine = None
        self.sampler = None
        self.inputs = []
        self.drain = None
        self.workers = None
        self.profiles = []
        self.events = []
        self.load_rounds = 1

    def merge(self) -> dict:
        """The deep-merged settings every measured engine runs with."""
        merge = {
            "engine": {"telemetry": {"reporting_interval": TELEMETRY_INTERVAL}},
            "policies": {"resources": {"memory_limiter": dict(LIMITER)}},
        }
        if self.topology != "noop" and "exporter" in self.spec.overrides:
            merge["exporter"] = self.spec.overrides["exporter"]
        if "receiver" in self.spec.overrides:
            merge["receiver"] = {"protocols": {"grpc": self.spec.overrides["receiver"]}}
        return merge

    def launch(self):
        """Start the engine and the sampler, and wait for every worker."""
        self.directory.mkdir(parents=True, exist_ok=True)
        with allocator_environment(self.malloc_conf):
            self.engine = measurement.test_e2e.Engine(
                self.directory,
                storage=self.storage,
                interval=f"{self.spec.interval_s}s",
                topology=self.topology,
                buffer_path=self.buffer_path,
                cores=list(self.spec.cores),
                merge=self.merge(),
                binary=self.binary,
            )
        self.sampler = MemorySampler(self.engine).start()
        document = measurement.wait_until(
            lambda: self._document(),
            lambda value: value is not None and len(engine_workers(value)) == len(
                self.spec.cores
            ),
            deadline_ns=time.monotonic_ns() + READY_DEADLINE_S * 10**9,
            description=f"{self.label}: every worker answering a collection",
        )
        self.workers = engine_workers(document)
        return self

    def _document(self):
        """One telemetry response, or None while the engine comes up."""
        try:
            return measurement.test_e2e.engine_metrics(self.engine)
        except (OSError, ValueError):
            return None

    def phase(self, label):
        """Label every following sample with `label`."""
        self.sampler.phase = label
        self.events.append({"monotonic_ns": time.monotonic_ns(), "phase": label})

    def drained(self):
        """Prove a measured exporter drained; the noop control has no state."""
        if self.topology == "noop":
            return None
        buffered = self.topology == "buffered"
        started = time.monotonic_ns()
        report = measurement.observe_drain(
            lambda: measurement.sample_engine(
                self.engine, expected_workers=len(self.spec.cores), buffered=buffered
            ),
            expected_workers=len(self.spec.cores),
            buffered=buffered,
            deadline_ns=started + DRAIN_DEADLINE_S * 10**9,
        )
        self.drain = {
            key: report[key]
            for key in ("drained", "empty_epochs_by_worker", "sample_count")
        }
        self.drain["duration_s"] = (time.monotonic_ns() - started) / 1e9
        return report

    def profile(self, name):
        """A heap profile now, when the engine samples its allocations."""
        if "prof:true" not in self.malloc_conf:
            return None
        path = self.directory / f"heap-{name}.pb.gz"
        summary = heap_profile(self.engine, path)
        summary["name"] = name
        summary["phase"] = self.sampler.phase
        summary["monotonic_ns"] = time.monotonic_ns()
        summary["file"] = path.name
        self.profiles.append(summary)
        return summary

    def shutdown(self):
        """Stop sampling, then the engine, through the admin API."""
        if self.sampler is not None:
            self.sampler.stop()
        if self.engine is not None:
            try:
                self.engine.shutdown(SHUTDOWN_DEADLINE_S)
            finally:
                self.engine.close()

    def summary(self, controls=None) -> dict:
        """What this lifetime observed."""
        samples = self.sampler.samples if self.sampler else []
        flushes = annotate_flushes(samples)
        return {
            "flush_bracketed_samples": flushes,
            "label": self.label,
            "topology": self.topology,
            "pid": self.engine.pid if self.engine else None,
            "config_sha256": self.engine.config_sha256 if self.engine else None,
            "malloc_conf": self.malloc_conf,
            "workers": self.workers,
            "inputs": self.inputs,
            "load_rounds": self.load_rounds,
            "drain": self.drain,
            "events": self.events,
            "sample_count": len(samples),
            "sampler_errors": self.sampler.errors[:20] if self.sampler else [],
            "sampler_error_count": len(self.sampler.errors) if self.sampler else 0,
            "jemalloc_prints": self.sampler.stats.count if self.sampler else 0,
            "phases": phase_table(samples, controls),
            "block_pair_uncertainty": block_pair_uncertainty(samples),
            "profiles": self.profiles,
        }


# --------------------------------------------------------------------------
# One pair: the control and the measured pipeline
# --------------------------------------------------------------------------

# The workload of a pair: the harness-local request shape (100 records of
# 1KiB bodies, one metrics request in five over 100 series), offered at 50
# requests per second on one-second windows -- about the rate the strict
# harness-local run reached in Task 2 -- for 25 seconds.
DEFAULT_WORKLOAD = {
    "requests": 1275,
    "records_per_request": 100,
    "body_bytes": 1024,
    "series": 100,
    "metrics_every": 5,
}
WARMUP_REQUESTS = 25
DEFAULT_RATE = 50
# jemalloc returns dirty pages over its ten-second decay; both quiet phases
# start only after that long, so neither measures a decay in progress.
SETTLE_S = 11


# The workload shapes a family can run. `harness-rate` is the default above.
# `logs-high-rate` is the shape of the Task 4 attribution logs workload whose
# harness reconciliation failed: logs only, 1KiB bodies, 256 requests in
# flight against 128-request blocks, so blocks rotate on their request limit
# several times a second; the offered rate is far above what the engine
# absorbs, so the run is closed-loop at 256 in flight. Like Task 4 it writes
# to a pinned MinIO, whose client uploads on the worker's own runtime.
CONFIGS = {
    "harness-rate": {},
    "logs-high-rate": {
        "store": "minio",
        "requests": 9025,
        "metrics_every": 1_000_000,
        "rate_requests_per_s": 20000,
        "max_in_flight": 256,
        "receiver": {"max_concurrent_requests": 256},
        # The retry section every harness S3 configuration sets: the store
        # default of three minutes would outlast the block deadline.
        "exporter": {
            "window": {"max_requests_per_block": 128},
            "retry": measurement.test_e2e.S3_RETRY,
        },
    },
}


def memory_spec(topology, *, ordinal=1, case=None, config="harness-rate",
                **options) -> measurement.RunSpec:
    """The spec of one pair of the named topology and workload shape."""
    try:
        from . import measure
    except ImportError:
        import measure
    options = dict(CONFIGS[config], **options)
    cores = tuple(options.get("cores") or measure.default_engine_cores(1))
    interval_s = int(options.get("interval_s", 1))
    workload = measurement.Workload(
        **{name: int(options.get(name, value)) for name, value in DEFAULT_WORKLOAD.items()}
    )
    case = case or family_case(topology, config)
    rate = float(options.get("rate_requests_per_s", DEFAULT_RATE))
    load_s = (workload.requests - WARMUP_REQUESTS) / rate
    extra = {}
    if "receiver" in options:
        extra["receiver"] = options["receiver"]
    if "max_in_flight" in options:
        extra["max_in_flight"] = int(options["max_in_flight"])
    store = options.get("store", "local")
    return measurement.RunSpec(
        run_id=measurement.RunSpec.build_run_id(
            case, topology, store, cores, interval_s, ordinal
        ),
        case=case,
        topology=topology,
        store=store,
        cores=cores,
        workload=workload,
        interval_s=interval_s,
        duration_s=max(1, int(round(load_s))),
        max_in_flight=extra.pop("max_in_flight", 128),
        overrides={
            "rate_requests_per_s": rate,
            "warmup_requests": WARMUP_REQUESTS,
            "config": config,
            **({"exporter": options["exporter"]} if "exporter" in options else {}),
            **extra,
        },
    )


def family_case(topology, config) -> str:
    """The case name of one topology and workload shape."""
    return f"memory-{topology}" + ("" if config == "harness-rate" else f"-{config}")


# The most times a control resends its load to cover the load phase.
CONTROL_ROUNDS = 8


def _drive(lifetime, producer, spec, result, controls, again=None):
    """Warm, idle, load, drain and keep one lifetime, phase by phase.

    The noop control acknowledges at once, so its load can end before the
    phase holds enough samples; `again` then yields a producer on a fresh
    ledger and the same requests are offered once more, until it does. The
    measured exporter stores what it takes, so it is never offered twice.
    """
    rate = spec.overrides["rate_requests_per_s"]
    warmup = spec.overrides["warmup_requests"]
    lifetime.phase("warmup")
    lifetime.inputs.append(producer.send_scheduled(range(warmup), rate))
    lifetime.drained()
    lifetime.phase("settle")
    _ = threading.Event().wait(SETTLE_S)
    lifetime.phase("idle")
    _ = lifetime.sampler.wait_for("idle", MINIMUM_PHASE_SAMPLES)
    _ = lifetime.profile("idle")
    controls.raise_if_invalid()
    lifetime.phase("load")
    _ = measurement.run_pinned(
        sorted(producer.cores) or sorted(os.sched_getaffinity(0)),
        lambda: lifetime.inputs.append(
            producer.send_scheduled(range(warmup, spec.workload.requests), rate)
        ),
    )
    rounds = 1
    while again is not None and rounds < CONTROL_ROUNDS and min(
        lifetime.sampler.count("load"), fresh_epochs(
            [sample for sample in list(lifetime.sampler.samples)
             if sample["phase"] == "load"]
        )
    ) < MINIMUM_PHASE_SAMPLES:
        repeat = again(rounds)
        _ = measurement.run_pinned(
            sorted(repeat.cores) or sorted(os.sched_getaffinity(0)),
            lambda: lifetime.inputs.append(
                repeat.send_scheduled(range(warmup, spec.workload.requests), rate)
            ),
        )
        rounds += 1
    lifetime.load_rounds = rounds
    _ = lifetime.profile("load-end")
    controls.raise_if_invalid()
    lifetime.phase("drain")
    lifetime.drained()
    _ = lifetime.profile("drained")
    lifetime.phase("decay")
    _ = threading.Event().wait(SETTLE_S)
    lifetime.phase("retained")
    _ = lifetime.sampler.wait_for("retained", MINIMUM_PHASE_SAMPLES)
    _ = lifetime.profile("retained")
    controls.raise_if_invalid()


BACKGROUND_THREAD_LINE = re.compile(
    r"INFO memory allocator (\S+), background_thread (on|off|not applicable)"
)


def engine_background_thread(log_path):
    """The allocator and background-thread state the engine reported at start."""
    try:
        text = Path(log_path).read_text(encoding="ascii", errors="replace")
    except OSError:
        return None
    match = BACKGROUND_THREAD_LINE.search(text)
    return (match.group(1), match.group(2)) if match else None


def allocator_label(build_allocator, malloc_conf, reported) -> str:
    """The allocator a pair ran, as the fingerprint names it.

    The engine's own startup line decides whether jemalloc's background
    thread ran; a binary too old to print it ran without one, because that
    was the only default before the thread was compiled in. Allocator
    options beyond the statistics print are appended verbatim.
    """
    state = reported[1] if reported else "off"
    label = build_allocator
    if state == "on":
        label += "+background_thread"
    if malloc_conf != JEMALLOC_STATS_CONF:
        label += f":{malloc_conf}"
    return label


def pair_allocation(spec) -> dict:
    """The role placement of one pair: a store role only when it has one."""
    topology_info = measurement.core_topology()
    return measurement.role_allocation(
        topology_info["sibling_groups"], sorted(os.sched_getaffinity(0)), spec.cores,
        roles=measurement.CASE_ROLES["memory_store" if spec.store != "local" else "memory"],
    )


def copy_store(store, prefix, destination) -> int:
    """Download every object under `prefix` so the oracle can read it back."""
    destination = Path(destination)
    count = 0
    paginator = store.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=store.bucket, Prefix=prefix + "/"):
        for entry in page.get("Contents", []):
            target = destination / entry["Key"][len(prefix) + 1:]
            target.parent.mkdir(parents=True, exist_ok=True)
            store.client.download_file(store.bucket, entry["Key"], str(target))
            count += 1
    return count


def pair_experiment(spec, result, output_dir, controls, *, provenance, prebuilt,
                    malloc_conf=JEMALLOC_STATS_CONF, control=True, store=None):
    """The measured body of one pair, run under the host controls."""
    output_dir = Path(output_dir)
    controls.coverage_gaps_hard = True
    allocation = pair_allocation(spec)
    controls.allocate(allocation)
    controls.register("harness", os.getpid())
    storage = None
    if store is not None:
        # One store serves the family; each pair writes under its own prefix.
        storage = {"s3": dict(store.storage["s3"], base_uri=f"s3://{store.bucket}/{spec.run_id}")}
        pid = getattr(store, "host_pid", None)
        if pid:
            controls.register("store", pid, allocation.get("store", []))
    # The allocator options change what is measured, so a non-default set
    # is part of the build the fingerprint covers.
    build = dict(provenance["build"])
    base_allocator = build["allocator"]
    build["allocator"] = allocator_label(base_allocator, malloc_conf, None)
    result["environment"]["build"] = build
    result["environment"]["git"] = provenance["git"]
    result["environment"]["allocator_conf"] = malloc_conf
    binary = Path(provenance["build"]["binary"])
    lifetimes = []
    oracle = None
    counts = {}
    latencies = []
    control_ledger = None
    ledger = measurement.Ledger(output_dir / "ledger.sqlite")
    try:
        if control:
            control_life = Lifetime(
                "control", "noop", spec, output_dir / "control",
                binary=binary, malloc_conf=malloc_conf,
            ).launch()
            lifetimes.append(control_life)
            snapshot = controls.snapshot(
                "start", {"engine": (control_life.engine.pid, list(spec.cores))},
                workers=control_life.workers, requested_cores=list(spec.cores),
            )
            controls.watch_workers(control_life.engine.pid, snapshot)
            control_ledger = measurement.Ledger(output_dir / "control-ledger.sqlite")
            producer = PrebuiltProducer(
                control_life.engine.channel, control_ledger, spec.workload,
                cores=allocation.get("producer", []), timeout_s=spec.producer_timeout_s,
                max_in_flight=spec.max_in_flight, prebuilt=prebuilt,
            )
            repeats = []

            def again(round_number):
                """A producer on a fresh ledger for one more control round."""
                ledger_round = measurement.Ledger(
                    output_dir / f"control-ledger-{round_number + 1}.sqlite"
                )
                repeats.append(ledger_round)
                return PrebuiltProducer(
                    control_life.engine.channel, ledger_round, spec.workload,
                    cores=allocation.get("producer", []),
                    timeout_s=spec.producer_timeout_s,
                    max_in_flight=spec.max_in_flight, prebuilt=prebuilt,
                )

            try:
                _drive(control_life, producer, spec, result, controls, again=again)
            finally:
                for ledger_round in repeats:
                    ledger_round.close()
            controls.unwatch_workers()
            control_life.shutdown()
        buffer_path = output_dir / "buffer" if spec.topology == "buffered" else None
        measured = Lifetime(
            "measured", spec.topology, spec, output_dir / "measured",
            binary=binary, malloc_conf=malloc_conf, buffer_path=buffer_path,
            storage=storage,
        ).launch()
        lifetimes.append(measured)
        reported = engine_background_thread(Path(measured.engine.root) / "engine.log")
        result["environment"]["engine_allocator_report"] = (
            {"allocator": reported[0], "background_thread": reported[1]}
            if reported else None
        )
        build["allocator"] = allocator_label(base_allocator, malloc_conf, reported)
        roles = {"engine": (measured.engine.pid, list(spec.cores))}
        if control:
            snapshot = controls.checkpoint(
                "measured", roles, workers=measured.workers,
                requested_cores=list(spec.cores),
            )
        else:
            snapshot = controls.snapshot(
                "start", roles, workers=measured.workers,
                requested_cores=list(spec.cores),
            )
        controls.watch_workers(measured.engine.pid, snapshot)
        result["config"]["effective"] = measured.engine.config
        result["config"]["effective_sha256"] = measured.engine.config_sha256
        result["ephemeral_values"] = {
            "<receiver_listening_addr>": f"127.0.0.1:{measured.engine.grpc_port}"
        }
        if store is not None:
            result["ephemeral_values"]["<store_endpoint>"] = store.endpoint
            result["ephemeral_values"]["<store_prefix>"] = spec.run_id
        producer = PrebuiltProducer(
            measured.engine.channel, ledger, spec.workload,
            cores=allocation.get("producer", []), timeout_s=spec.producer_timeout_s,
            max_in_flight=spec.max_in_flight, prebuilt=prebuilt,
        )
        _drive(measured, producer, spec, result, controls)
        _ = controls.snapshot(
            "end", roles, workers=measured.workers, requested_cores=list(spec.cores)
        )
        controls.unwatch_workers()
        measured.shutdown()
        measurement.record_event(result, "engine_shut_down", str(measured.engine.pid))
        data_root = measured.engine.data
        if store is not None:
            data_root = output_dir / "store-copy"
            result["observations_store_objects"] = copy_store(store, spec.run_id, data_root)
        oracle = measurement.run_pinned(
            allocation.get("reader") or sorted(os.sched_getaffinity(0)),
            measurement.read_oracle,
            data_root,
            ledger,
            require_all=True,
            healthy=True,
            workload=spec.workload,
        )
        counts = ledger.counts()
        latencies = ledger.acknowledgement_latencies_s()
    finally:
        controls.unwatch_workers()
        for lifetime in lifetimes:
            if lifetime.sampler is not None:
                lifetime.sampler.stop()
            if lifetime.engine is not None:
                lifetime.engine.close()
        ledger.close()
        control_counts = control_ledger.counts() if control_ledger else None
        if control_ledger is not None:
            control_ledger.close()
        settle_pair(
            result, spec, lifetimes, oracle, counts, latencies, control_counts,
            output_dir,
        )


# Primary positive memory metrics of a pair: the family refuses a baseline
# when any of them spreads by more than MAXIMUM_SPREAD across pairs.
PRIMARY_METRICS = (
    "measured_idle_rss_bytes",
    "measured_peak_rss_bytes",
    "measured_retained_rss_bytes",
    "measured_peak_heap_resident_bytes",
    "measured_peak_jemalloc_allocated_bytes",
    "measured_peak_tracked_heap_bytes",
    "measured_peak_accounted_bytes",
    "control_idle_rss_bytes",
    "control_peak_rss_bytes",
)
# Signed paired differences: each must keep one sign across the pairs unless
# its spread is inside the pair's own measurement uncertainty.
SIGNED_METRICS = (
    "exporter_peak_rss_delta_bytes",
    "load_unexplained_median_bytes",
)
MAXIMUM_SPREAD = 0.15
PAIRS = 3


def _peak(samples, name, phases=("load", "drain")):
    """The largest value of one term over the named phases."""
    values = [
        sample_terms(sample).get(name) for sample in samples if sample["phase"] in phases
    ]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _median(table, phase, name):
    """One phase's median of one term, from a phase table."""
    return ((table.get(phase) or {}).get("terms") or {}).get(name, {}).get("median")


# The ledger's workspace term. The ledger may subtract only a workspace
# measured in the same run and present at the sample. An engine whose worker
# publishes no `flush.workspace` gives no such measurement (a stage profile
# of another fixture is not this run), so the term is zero and whatever the
# flush holds stays in the residual, where the frozen tolerance judges it.
NO_WORKSPACE_TERM = {
    "bytes": 0,
    "provenance": (
        "no in-run measurement of the flush workspace exists, so the term is "
        "zero; a flush's transient heap stays inside the residual"
    ),
}

# An engine whose worker publishes `flush.workspace` measures the flush
# workspace in the run itself, at every sample, and charges it inside
# `memory.accounted`, which the ledger subtracts already. The separate term
# is therefore zero: subtracting it again would count it twice.
IN_RUN_WORKSPACE_TERM = {
    "bytes": 0,
    "provenance": (
        "the worker publishes its live flush workspace as flush.workspace and "
        "charges it inside memory.accounted at every sample, so the ledger "
        "subtracts it through memory.accounted and adds no separate term"
    ),
}


def workspace_term(samples) -> dict:
    """The ledger's workspace term for one lifetime's samples."""
    published = any(
        ((sample.get("telemetry") or {}).get("exporter") or {}).get("flush.workspace")
        is not None
        for sample in samples
    )
    return IN_RUN_WORKSPACE_TERM if published else NO_WORKSPACE_TERM


def ledger_check(entries, peak_rss) -> dict:
    """The frozen reconciliation applied to ledger residual entries."""
    return measurement.residual_check(entries, peak_rss)


def settle_pair(result, spec, lifetimes, oracle, counts, latencies, control_counts,
                output_dir):
    """Checks, metrics and observations of one pair."""
    checks = result["checks"]
    by_label = {lifetime.label: lifetime for lifetime in lifetimes}
    measured = by_label.get("measured")
    control = by_label.get("control")
    control_samples = control.sampler.samples if control and control.sampler else []
    samples = measured.sampler.samples if measured and measured.sampler else []
    controls_by_phase = control_heap_by_phase(control_samples)
    summaries = [
        lifetime.summary(controls_by_phase if lifetime.label == "measured" else None)
        for lifetime in lifetimes
    ]
    delivered = bool(
        oracle and oracle["passed"]
        and counts.get("requests_acked_count") == counts.get("requests_attempted_count")
        == spec.workload.requests
        and (control_counts is None or control_counts["requests_acked_count"]
             == spec.workload.requests)
    )
    checks.append(
        measurement.check(
            "delivery", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if delivered else measurement.STATUS_FAILED,
            f"measured acked {counts.get('requests_acked_count')}/"
            f"{counts.get('requests_attempted_count')}; control "
            f"{control_counts}; problems {(oracle or {}).get('problems')}",
        )
    )
    problems = []
    for summary in summaries:
        if summary["sampler_error_count"]:
            problems.append(f"{summary['label']}: {summary['sampler_error_count']} sampler errors")
        for phase in REQUIRED_PHASES:
            entry = summary["phases"].get(phase) or {"samples": 0, "fresh_epochs": 0}
            if min(entry["samples"], entry["fresh_epochs"]) < MINIMUM_PHASE_SAMPLES:
                problems.append(
                    f"{summary['label']}/{phase}: {entry['samples']} samples, "
                    f"{entry['fresh_epochs']} fresh epochs"
                )
        if summary["topology"] != "noop":
            # A flush is shorter than the sampling period and the worker
            # answers no collection while it runs, so its coverage is the
            # allocator's synchronous prints inside flush intervals.
            entry = summary["phases"].get(FLUSH_PHASE) or {"jemalloc_prints": 0}
            if entry["jemalloc_prints"] < MINIMUM_PHASE_SAMPLES:
                problems.append(
                    f"{summary['label']}/{FLUSH_PHASE}: {entry['jemalloc_prints']} "
                    f"allocator prints inside flush intervals"
                )
        if not summary["jemalloc_prints"]:
            problems.append(f"{summary['label']}: no allocator statistics were printed")
    checks.append(
        measurement.check(
            "minimum_samples", measurement.CHECK_HARD,
            measurement.STATUS_FAILED if problems or not summaries
            else measurement.STATUS_PASSED,
            "; ".join(problems) or ", ".join(
                f"{summary['label']}: {summary['sample_count']} samples"
                for summary in summaries
            ),
        )
    )
    # The reconciliation gate, with its frozen tolerance, applied to the
    # Task 6 ledger: after the measured non-heap and allocator terms, the
    # live heap neither the exporter accounts for nor the paired control
    # holds is unexplained. Every sample of the measured traffic counts.
    peak_rss = max((sample["rss_bytes"] or 0 for sample in samples), default=0)
    workspace = workspace_term(samples)
    ledger_residuals = [
        {
            "monotonic_ns": sample["monotonic_ns"],
            "phase": sample["phase"],
            "flush_interval": flushing(sample),
            "flush_workspace_bytes": terms.get("flush_workspace_bytes"),
            "residual_without_workspace_bytes": terms["unexplained_bytes"],
            "residual_bytes": terms["unexplained_bytes"],
        }
        for sample in samples
        if sample["phase"] in ("load", "drain", "decay", "retained")
        for terms in [sample_terms(
            sample, controls_by_phase.get(
                "load" if sample["phase"] in ("load", "drain") else "retained"
            )
        )]
        if terms.get("unexplained_bytes") is not None
    ]
    checks.append(ledger_check(ledger_residuals, peak_rss))
    # The harness's own reconciliation, recorded but not gating here: its
    # heap term is the pipeline threads' allocated-minus-freed counters,
    # which keep every byte a pipeline thread allocated and another thread
    # freed, so over many windows it drifts away from the live heap.
    residuals = gate_residuals(samples)
    harness_check = measurement.residual_check(residuals, peak_rss)
    table = summaries[-1]["phases"] if summaries else {}
    control_table = summaries[0]["phases"] if control else {}
    control_peak = _peak(control_samples, "rss_bytes")
    measured_peak = _peak(samples, "rss_bytes")
    input_s = sum(
        (entry["finished_ns"] - entry["started_ns"]) / 1e9
        for entry in (measured.inputs[1:] if measured else [])
    )
    metrics = {
        "measured_idle_rss_bytes": _median(table, "idle", "rss_bytes"),
        "measured_peak_rss_bytes": measured_peak,
        "measured_retained_rss_bytes": _median(table, "retained", "rss_bytes"),
        "measured_peak_heap_resident_bytes": _peak(samples, "heap_resident_bytes"),
        "measured_peak_jemalloc_allocated_bytes": max(
            (
                entry["allocated_bytes"] for sample in samples
                if sample["phase"] in ("load", "drain")
                for entry in sample.get("jemalloc_printed", [])
            ),
            default=None,
        ),
        "measured_peak_tracked_heap_bytes": _peak(samples, "tracked_heap_bytes"),
        "measured_peak_accounted_bytes": _peak(samples, "accounted_bytes"),
        "measured_peak_flush_workspace_bytes": _peak(samples, "flush_workspace_bytes"),
        "control_idle_rss_bytes": _median(control_table, "idle", "rss_bytes"),
        "control_peak_rss_bytes": control_peak,
        "exporter_peak_rss_delta_bytes": (
            measured_peak - control_peak
            if measured_peak is not None and control_peak is not None else None
        ),
        "load_non_heap_median_bytes": _median(table, "load", "non_heap_bytes"),
        "load_allocator_retention_median_bytes": _median(
            table, "load", "allocator_retention_bytes"
        ),
        "load_untracked_heap_median_bytes": _median(table, "load", "untracked_heap_bytes"),
        "load_unexplained_median_bytes": _median(table, "load", "unexplained_bytes"),
        "retained_tracked_heap_drift_bytes": (
            _median(table, "retained", "tracked_heap_bytes") - _median(
                table, "idle", "tracked_heap_bytes"
            )
            if _median(table, "retained", "tracked_heap_bytes") is not None
            and _median(table, "idle", "tracked_heap_bytes") is not None else None
        ),
        "harness_residual_max_bytes": max(
            (entry["residual_bytes"] for entry in residuals), default=None
        ),
        "harness_residual_min_bytes": min(
            (entry["residual_bytes"] for entry in residuals), default=None
        ),
        "ledger_residual_max_bytes": max(
            (entry["residual_bytes"] for entry in ledger_residuals), default=None
        ),
        "ledger_residual_min_bytes": min(
            (entry["residual_bytes"] for entry in ledger_residuals), default=None
        ),
        "throughput_records_per_s": (
            counts.get("records_acked_count", 0) / input_s if input_s > 0 else None
        ),
        "ack_latency_p99_s": (
            measurement.percentile(latencies, 0.99) if latencies else None
        ),
    }
    result["metrics"] = metrics
    for name, value in metrics.items():
        if value is None:
            result["metrics_unavailable"][name] = "this pair did not observe it"
    result["metric_directions"] = {
        name: (
            measurement.HIGHER_IS_BETTER if name == "throughput_records_per_s"
            else measurement.LOWER_IS_BETTER
        )
        for name in metrics
    }
    result["mandatory_metrics"] = sorted(PRIMARY_METRICS)
    exporter_config = (
        result["config"]["effective"].get("groups", {}).get("default", {})
        .get("pipelines", {}).get("main", {}).get("nodes", {}).get("exporter", {})
        .get("config")
    )
    ledger_view = None
    if exporter_config and "window" in exporter_config:
        inputs = ledger_inputs(exporter_config)
        budget = _peak(samples, "budget_bytes", phases=("idle", "load", "drain", "retained"))
        token = token_high_water_of(inputs, budget) if budget else None
        ledger_view = {
            "inputs": inputs,
            "reported_budget_bytes": budget,
            "token_high_water_bytes": token,
            "retained_reservation_bytes": (
                retained_reservation(inputs, token) if token is not None else None
            ),
            "workspace_reservation_bytes": workspace_reservation(inputs),
            "transcription_matches_budget": token is not None,
        }
    result["observations"] = {
        "lifetimes": summaries,
        "control_heap_by_phase": controls_by_phase,
        "ledger_residuals": ledger_residuals,
        "flush_workspace_term": workspace,
        "harness_residuals": residuals,
        "harness_residual_check": harness_check,
        "harness_residual_one_second_check": measurement.residual_check(
            [
                dict(entry, residual_bytes=entry["one_second_gauge_residual_bytes"])
                for entry in residuals
            ],
            peak_rss,
        ),
        "harness_residual_shares": residual_shares(residuals),
        "ledger": ledger_view,
        "ledger_counts": counts,
        "control_ledger_counts": control_counts,
        "oracle": {
            key: oracle[key] for key in (
                "part_file_count", "expected_record_count", "missing_record_count",
                "unexpected_record_count", "corrupt_record_count", "readers",
                "problems",
            ) if oracle and key in oracle
        },
    }
    result["samples"] = [
        compact(sample, lifetime.label)
        for lifetime in lifetimes
        for sample in (lifetime.sampler.samples if lifetime.sampler else [])
    ]
    result["artifacts"] = [
        dict(measurement.file_entry(path), kind=kind, retention=str(output_dir))
        for path, kind in sorted(
            [(path, "engine_log") for path in Path(output_dir).glob("*/engine.log")]
            + [(path, "engine_config") for path in Path(output_dir).glob("*/pipeline.yaml")]
            + [(path, "heap_profile") for path in Path(output_dir).glob("*/heap-*.pb.gz")]
        )
        if path.is_file()
    ]
    result["status"] = measurement.STATUS_PASSED


def compact(sample, label) -> dict:
    """What a result keeps of one memory sample."""
    telemetry = sample["telemetry"]
    return {
        "lifetime": label,
        "monotonic_ns": sample["monotonic_ns"],
        "phase": sample["phase"],
        "rss_bytes": sample["rss_bytes"],
        "anonymous_bytes": sample["anonymous_bytes"],
        "num_threads": sample["num_threads"],
        "limiter_resident_bytes": telemetry["jemalloc_resident_bytes"],
        "engine_rss_bytes": telemetry["engine_rss_bytes"],
        "flush_bracket": bool(sample.get("flush_bracket")),
        "tracked_heap_bytes": telemetry["tracked_heap_bytes"],
        "pipeline_heap_bytes": {
            entry["pipeline_id"]: entry["memory_usage_bytes"]
            for entry in telemetry["pipelines"].values()
        },
        "exporter": telemetry.get("exporter"),
        "jemalloc": sample.get("jemalloc"),
        "jemalloc_printed": sample.get("jemalloc_printed"),
        "jemalloc_age_ns": sample.get("jemalloc_age_ns"),
        "smaps": sample.get("smaps"),
    }


def memory_experiment(spec, output_dir, *, report_dir=None, provenance=None,
                      prebuilt=None, malloc_conf=JEMALLOC_STATS_CONF,
                      control=True, store=None) -> dict:
    """One pair under the host controls, published, its result returned.

    A pair is one control and one measured release engine on the same cores
    under the same offered workload, each warmed separately. A child of a
    family is not compared on its own: the family applies the stability rule
    and the baseline policy to its pairs together.
    """
    try:
        from . import measure
    except ImportError:
        import measure
    provenance = provenance or measure.prepare_build()
    prebuilt = prebuilt or prebuild(spec.workload)

    def experiment(spec, result, directory, controls):
        """The pair, with this call's options."""
        pair_experiment(
            spec, result, directory, controls, provenance=provenance,
            prebuilt=prebuilt, malloc_conf=malloc_conf, control=control, store=store,
        )

    try:
        return measure.run_case(
            spec, output_dir, experiment=experiment, report_dir=report_dir,
            evaluate=False,
        )
    except Exception as error:
        print(f"{spec.run_id}: {type(error).__name__}: {error}", flush=True)
        return json.loads((Path(output_dir) / f"{spec.run_id}.json").read_text(
            encoding="ascii"
        ))


# --------------------------------------------------------------------------
# Heap profiles
# --------------------------------------------------------------------------


def _varint(data, position):
    """One protobuf varint at `position`, and the position after it."""
    value = 0
    shift = 0
    while True:
        byte = data[position]
        position += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, position
        shift += 7


def _fields(data):
    """Every (field number, wire type, value) of one protobuf message."""
    position = 0
    while position < len(data):
        key, position = _varint(data, position)
        number, kind = key >> 3, key & 7
        if kind == 0:
            value, position = _varint(data, position)
        elif kind == 2:
            length, position = _varint(data, position)
            value = data[position:position + length]
            position += length
        elif kind == 1:
            value = int.from_bytes(data[position:position + 8], "little")
            position += 8
        elif kind == 5:
            value = int.from_bytes(data[position:position + 4], "little")
            position += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {kind}")
        yield number, kind, value


def _packed(kind, value):
    """A repeated integer field, packed or not."""
    if kind == 0:
        return [value]
    out = []
    position = 0
    while position < len(value):
        item, position = _varint(value, position)
        out.append(item)
    return out


# Where a live allocation is attributed, by the first frame that names one
# of these, searched from the allocation site outward.
HEAP_CATEGORIES = (
    ("merge_keys", ("arrow_row",)),
    ("values_builders", ("series_lake::extract",)),
    ("series_lake_block", ("series_lake::buffer", "series_lake::sort")),
    ("parquet_writer", ("parquet::",)),
    ("durable_buffer", ("quiver", "durable_buffer")),
    ("otap_conversion", ("otel_arrow_dfe_pdata", "otap_df_pdata")),
    ("grpc", ("h2::", "hyper::", "tonic::")),
    ("telemetry", ("telemetry", "metrics_registry")),
    ("tokio", ("tokio::",)),
)


def pprof_summary(data) -> dict:
    """The live heap of one profile, in total and by attributed category.

    Parses the pprof protobuf directly: samples with their location stacks,
    locations with their lines, functions and the string table. The value
    kept per sample is the last one, the bytes.
    """
    strings = []
    functions = {}
    locations = {}
    samples = []
    sample_types = []
    for number, kind, value in _fields(data):
        if number == 6:
            strings.append(value.decode("utf-8", "replace"))
        elif number == 1:
            fields = {n: v for n, _k, v in _fields(value)}
            sample_types.append((fields.get(1, 0), fields.get(2, 0)))
        elif number == 2:
            stack = []
            values = []
            for n, k, v in _fields(value):
                if n == 1:
                    stack.extend(_packed(k, v))
                elif n == 2:
                    values.extend(_packed(k, v))
            samples.append((stack, values))
        elif number == 4:
            fields = collections.defaultdict(list)
            for n, _k, v in _fields(value):
                fields[n].append(v)
            lines = []
            for line in fields.get(4, []):
                inner = {n: v for n, _k, v in _fields(line)}
                lines.append(inner.get(1, 0))
            locations[fields[1][0]] = lines
        elif number == 5:
            fields = {n: v for n, _k, v in _fields(value)}
            functions[fields.get(1, 0)] = fields.get(2, 0)

    def name(function_id):
        """A function's name, or an empty string."""
        index = functions.get(function_id, 0)
        return strings[index] if 0 <= index < len(strings) else ""

    categories = collections.Counter()
    top = collections.Counter()
    total = 0
    for stack, values in samples:
        if not values:
            continue
        size = values[-1]
        total += size
        frames = [name(function) for location in stack for function in locations.get(location, [])]
        category = "other"
        for frame in frames:
            for label, needles in HEAP_CATEGORIES:
                if any(needle in frame for needle in needles):
                    category = label
                    break
            if category != "other":
                break
        categories[category] += size
        # The allocator's own C frames carry no `::`; the Rust allocation
        # plumbing is skipped too, so a stack starts at the caller.
        interesting = [
            frame for frame in frames
            if "::" in frame and not frame.startswith(("alloc::", "core::", "std::"))
        ][:3]
        top[" <- ".join(interesting) or "(unsymbolized)"] += size
    return {
        "sample_types": [
            [strings[kind] if kind < len(strings) else "", strings[unit] if unit < len(strings) else ""]
            for kind, unit in sample_types
        ],
        "samples": len(samples),
        "live_bytes": total,
        "by_category_bytes": dict(categories),
        "top_stacks": [
            {"stack": stack, "bytes": size} for stack, size in top.most_common(15)
        ],
    }


# --------------------------------------------------------------------------
# Accounting-gap probes, through the stage bench
# --------------------------------------------------------------------------

# The fixtures the probes measure: the stage family's four workloads, plus
# one-record and ten-record requests, which are what the builder-capacity
# hypothesis is about.
PROBE_SHAPES = {
    "logs-1-record": ("logs", {"requests": 40, "records_per_request": 1, "metrics_every": 1000}),
    "logs-10-records": ("logs", {"requests": 40, "records_per_request": 10, "metrics_every": 1000}),
    # The harness adds one metric per kind to every metrics request, so a
    # request of fewer than six points carries a metric without data, which
    # OTLP does not allow: six points, one per kind, is the smallest shape.
    "metrics-6-points": ("metrics", {"requests": 40, "records_per_request": 6, "metrics_every": 1}),
    "metrics-10-points": ("metrics", {"requests": 40, "records_per_request": 10, "metrics_every": 1}),
}


def probe_inputs(directory) -> dict:
    """Every probe fixture, written as the stage bench reads it."""
    try:
        from . import performance
    except ImportError:
        import performance
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    inputs = {}
    for config_id, config in performance.WORKLOAD_CONFIGS.items():
        path = directory / f"{config_id}.otlp"
        sidecar = performance.write_stage_input(config["workload"], config["signal"], path)
        inputs[config_id] = {"path": path, "lake": config["lake"], "sidecar": sidecar}
    for config_id, (signal, shape) in PROBE_SHAPES.items():
        workload = measurement.Workload(body_bytes=1024, series=100, **shape)
        path = directory / f"{config_id}.otlp"
        sidecar = performance.write_stage_input(workload, signal, path)
        inputs[config_id] = {
            "path": path, "lake": performance.lake_config(), "sidecar": sidecar,
        }
    return inputs


def run_probes(directory, executable) -> dict:
    """Run the merge-key and values-capacity probes on every fixture.

    Each probe is one timing-profile bench process whose fixture reports the
    resident merge keys (merge stage) and the values capacity of every
    request (extract stage); those are byte counts of Arrow buffers, exact
    and independent of the timing, which is recorded only as provenance.
    """
    directory = Path(directory)
    results = {}
    for config_id, entry in probe_inputs(directory / "inputs").items():
        results[config_id] = {"sidecar": {
            key: entry["sidecar"][key] for key in ("signal", "requests", "records", "bytes")
        }}
        for stage, extra in (("merge", "merge_keys"), ("extract", "values_capacity")):
            config = {
                "workload_config_id": config_id,
                "lake": entry["lake"],
                "storage": {"file": {"base_uri": str(directory / "store")}},
                "scratch_dir": str(directory / "scratch"),
                "cache_entries": 200000,
                "committed_fraction": 0.5,
                "extra_committed_series": 0,
                "window_start_secs": measurement.LOG_BASE_TIME_NS // 10**9,
                "seal_at_us": measurement.LOG_BASE_TIME_NS // 1000,
            }
            config_path = measurement.write_json_atomic(
                directory / f"{config_id}-{stage}-config.json", config
            )
            output = directory / f"{config_id}-{stage}-report.json"
            done = subprocess.run(
                [
                    str(executable), "--stage", stage, "--input", str(entry["path"]),
                    "--config", str(config_path), "--output", str(output),
                    "--iterations", "1", "--profile", "timing", "--compression", "zstd",
                ],
                capture_output=True, text=True, timeout=600,
            )
            if done.returncode != 0:
                results[config_id][extra] = {"error": done.stderr[-500:]}
                continue
            report = json.loads(output.read_text(encoding="ascii"))
            observation = report.get("observation") or {}
            results[config_id][extra] = (observation.get("extra") or {}).get(extra)
    return results


# --------------------------------------------------------------------------
# A family of pairs, its stability rule and its baseline
# --------------------------------------------------------------------------


def spread(values):
    """`(max - min) / median` of positive values, None when undefined."""
    values = [value for value in values if value is not None]
    if not values:
        return None
    middle = summarize(values)["median"]
    if not middle or middle <= 0:
        return None
    return (max(values) - min(values)) / middle


def signed_consistent(values, uncertainty) -> bool:
    """Whether paired signed differences keep one sign within uncertainty.

    Values of both signs are accepted only when their whole range fits in
    the recorded measurement uncertainty, in which case the difference is
    indistinguishable from zero rather than of changing sign.
    """
    values = [value for value in values if value is not None]
    if not values:
        return False
    if min(values) >= 0 or max(values) <= 0:
        return True
    return uncertainty is not None and (max(values) - min(values)) <= uncertainty


def next_ordinal(report_dir, case) -> int:
    """The first pair number no published run of `case` has used."""
    return _next(
        report_dir,
        re.escape(case) + r"-[a-z]+-(?:local|minio|rustfs)-c\d+-w\d+-r(\d{3})\.json",
    )


def next_family_ordinal(report_dir, case) -> int:
    """The first family number no published aggregate of `case` has used."""
    return _next(report_dir, re.escape(case) + r"-f(\d{3})\.json")


def _next(report_dir, pattern) -> int:
    """One more than the highest number `pattern` captures in the report dir."""
    directory = measurement.resolve_report_dir(report_dir)
    compiled = re.compile("^" + pattern + "$")
    highest = 0
    if Path(directory).is_dir():
        for path in Path(directory).iterdir():
            match = compiled.match(path.name)
            if match:
                highest = max(highest, int(match.group(1)))
    return highest + 1


def lease_holder(path=None):
    """The PID recorded by the holder of the host lease, or None when free.

    The lock is tried and released at once; the record beside it is only
    read to know whom to wait for.
    """
    import fcntl

    path = Path(
        path or os.environ.get("SERIES_MEASURE_LEASE", measurement.DEFAULT_LEASE_PATH)
    )
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o644)
    except OSError:
        return None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            return None
        except BlockingIOError:
            pass
        try:
            return int(json.loads(path.read_text(encoding="ascii"))["pid"])
        except (OSError, ValueError, KeyError, TypeError):
            return -1
    finally:
        os.close(fd)


def wait_for_lease(path=None, *, log=print):
    """Block until the shared host lease is free, waiting on its holder.

    A busy lease is another measurement, not a failure: this waits for the
    holding process to exit, never polling the process table by name.
    """
    wake = threading.Event()
    while True:
        holder = lease_holder(path)
        if holder is None:
            return
        log(f"host lease held by {holder}; waiting for it")
        if holder > 0 and Path(f"/proc/{holder}").exists():
            _ = subprocess.run(["tail", f"--pid={holder}", "-f", "/dev/null"], check=False)
        else:
            _ = wake.wait(1.0)


def lease_refused(result) -> bool:
    """Whether a run failed only because another measurement held the lease."""
    return any(
        event["kind"] == "failed" and "another measurement holds" in event["detail"]
        for event in result.get("events", [])
    )


def invalidated(result) -> bool:
    """Whether a compiler or image build ran inside the measured window."""
    return any(
        entry["name"] == "no_concurrent_build"
        and entry["status"] != measurement.STATUS_PASSED
        for entry in result.get("checks", [])
    )


# How many attempts one family may spend on invalidated or refused runs.
MAXIMUM_ATTEMPTS = 12


def run_pairs(case, topology, output_dir, report_dir, *, count, provenance, prebuilt,
              malloc_conf=JEMALLOC_STATS_CONF, rejected=None, store=None,
              **options) -> list:
    """`count` independent valid pairs, each on fresh engines, in order.

    A pair refused the lease measured nothing; a pair a build invalidated
    measured a disturbed host. Both are rerun under the next ordinal rather
    than accepted, and an invalidated one is listed in `rejected`. Children
    are published only through the family's index.
    """
    results = []
    ordinal = max(next_ordinal(report_dir, case), next_ordinal(output_dir, case))
    attempts = 0
    while len(results) < count:
        attempts += 1
        if attempts > MAXIMUM_ATTEMPTS:
            raise AssertionError(
                f"{case}: {MAXIMUM_ATTEMPTS} attempts did not yield {count} valid pairs"
            )
        wait_for_lease()
        spec = memory_spec(topology, ordinal=ordinal, case=case, **options)
        ordinal += 1
        started = time.monotonic()
        directory = Path(output_dir) / spec.run_id
        result = memory_experiment(
            spec, directory, report_dir=directory,
            provenance=provenance, prebuilt=prebuilt, malloc_conf=malloc_conf,
            store=store,
        )
        print(
            f"{spec.run_id}: {result['status']} in {time.monotonic() - started:.0f}s",
            flush=True,
        )
        if lease_refused(result):
            # Nothing was measured; the next ordinal is a fresh attempt.
            continue
        if invalidated(result):
            if rejected is not None:
                rejected.append(
                    {
                        "run_id": result["run_id"],
                        "reason": "a build ran inside the measured window",
                        "builds": (result["environment"].get("build_monitor") or {}).get(
                            "observations", []
                        )[:3],
                    }
                )
            continue
        results.append(result)
    return results


def aggregate_family(case, children, output_dir, report_dir, *, ordinal,
                     observations=None) -> dict:
    """The pairs of one topology and configuration, as a comparable result.

    Its metrics are the medians of the pairs. It passes only when every pair
    passed every hard gate, when no primary memory metric spreads by more
    than MAXIMUM_SPREAD, and when every paired signed difference keeps one
    sign outside its measurement uncertainty; only then is the Controller
    baseline policy applied, to the family.
    """
    try:
        from . import performance
    except ImportError:
        import performance
    first, last = children[0], children[-1]
    run_id = f"{case}-f{ordinal:03d}"
    result = measurement.new_result({"run_id": run_id, "case": case},
                                    artifact_kind="memory_aggregate")
    result["environment"] = {
        "start": first["environment"]["start"],
        "end": last["environment"]["end"],
        "build": first["environment"].get("build"),
        "git": first["environment"].get("git"),
        "core_allocation": first["environment"].get("core_allocation"),
        "machine_identity_sha256": first["environment"].get("machine_identity_sha256"),
        "allocator_conf": first["environment"].get("allocator_conf"),
        "children": [child["run_id"] for child in children],
    }
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"]
    )
    result["config"] = first["config"]
    result["workload"] = first["workload"]
    result["workload_schedule"] = first["workload_schedule"]
    result["ephemeral_values"] = first.get("ephemeral_values") or {}
    result["run_dir"] = str(output_dir)
    names = sorted({name for child in children for name in child.get("metrics", {})})
    by_metric = {
        name: [
            child["metrics"].get(name) for child in children
            if isinstance(child.get("metrics", {}).get(name), (int, float))
        ]
        for name in names
    }
    result["metrics"] = {
        name: (summarize(values)["median"] if values else None)
        for name, values in by_metric.items()
    }
    for name, value in result["metrics"].items():
        if value is None:
            result["metrics_unavailable"][name] = "no pair measured it"
    result["metric_directions"] = {
        name: first.get("metric_directions", {}).get(name, measurement.LOWER_IS_BETTER)
        for name in result["metrics"]
    }
    result["mandatory_metrics"] = sorted(PRIMARY_METRICS)
    checks = performance.derived_checks(children)
    reconciled = all(
        any(
            entry["name"] == "rss_reconciliation"
            and entry["status"] == measurement.STATUS_PASSED
            for entry in child["checks"]
        )
        for child in children
    )
    checks.append(
        measurement.check(
            "rss_reconciliation", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if reconciled else measurement.STATUS_FAILED,
            json.dumps(
                {
                    child["run_id"]: next(
                        (entry["detail"] for entry in child["checks"]
                         if entry["name"] == "rss_reconciliation"), "absent"
                    )
                    for child in children
                },
                sort_keys=True,
            ),
        )
    )
    spreads = {name: spread(by_metric.get(name, [])) for name in PRIMARY_METRICS}
    unstable = sorted(
        name for name, value in spreads.items() if value is None or value > MAXIMUM_SPREAD
    )
    checks.append(
        measurement.check(
            "pair_stability", measurement.CHECK_HARD,
            measurement.STATUS_FAILED if unstable or len(children) < PAIRS
            else measurement.STATUS_PASSED,
            f"{len(children)} pairs; (max-min)/median "
            + json.dumps(
                {name: None if value is None else round(value, 4)
                 for name, value in sorted(spreads.items())},
                sort_keys=True,
            )
            + (f"; over {MAXIMUM_SPREAD:.0%} in {unstable}" if unstable else ""),
        )
    )
    per_pair = {child["run_id"]: pair_uncertainty(child) for child in children}
    uncertainty = {
        name: max(
            (entry[name]["bound_bytes"] for entry in per_pair.values()
             if entry[name]["bound_bytes"] is not None),
            default=None,
        )
        for name in SIGNED_METRICS
    }
    inconsistent = sorted(
        name for name in SIGNED_METRICS
        if not signed_consistent(by_metric.get(name, []), uncertainty.get(name))
    )
    checks.append(
        measurement.check(
            "paired_sign_consistent", measurement.CHECK_HARD,
            measurement.STATUS_FAILED if inconsistent else measurement.STATUS_PASSED,
            json.dumps(
                {name: {"values": by_metric.get(name), "uncertainty_bytes": uncertainty[name]}
                 for name in SIGNED_METRICS},
                sort_keys=True,
            ),
        )
    )
    result["checks"] = checks
    result["observations"] = dict(observations or {})
    result["observations"].update(
        {
            "pairs": [
                {"run_id": child["run_id"], "status": child["status"],
                 "metrics": child.get("metrics")}
                for child in children
            ],
            "dispersion": {
                name: dict(summarize(values), spread=spread(values))
                for name, values in sorted(by_metric.items())
            },
            "decomposition_by_phase": {
                child["run_id"]: {
                    life["label"]: {
                        phase: {
                            "samples": entry["samples"],
                            "fresh_epochs": entry["fresh_epochs"],
                            "jemalloc_prints": entry["jemalloc_prints"],
                            "printed_allocated_bytes": entry["printed_allocated_bytes"],
                            "terms": {
                                name: entry["terms"][name]
                                for name in PHASE_TERMS if name in entry["terms"]
                            },
                        }
                        for phase, entry in life["phases"].items()
                    }
                    for life in child.get("observations", {}).get("lifetimes", [])
                }
                for child in children
            },
            "block_pair_uncertainty": {
                child["run_id"]: {
                    life["label"]: life["block_pair_uncertainty"]
                    for life in child.get("observations", {}).get("lifetimes", [])
                }
                for child in children
            },
            "ledger": [child.get("observations", {}).get("ledger") for child in children],
            "harness_residual_checks": {
                child["run_id"]: {
                    "hundred_ms_gauge": child.get("observations", {}).get(
                        "harness_residual_check"
                    ),
                    "one_second_gauge": child.get("observations", {}).get(
                        "harness_residual_one_second_check"
                    ),
                    "shares": child.get("observations", {}).get("harness_residual_shares"),
                }
                for child in children
            },
            "signed_uncertainty_bytes": uncertainty,
            "pair_uncertainty": per_pair,
        }
    )
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
        decision = measurement.evaluate_baseline(result)
        if decision["action"] == "created":
            candidate = result.pop("baseline_candidate")
            path = measurement.write_published_json(
                Path(output_dir) / decision["baseline_name"], candidate
            )
            result["baseline_files"].append(measurement.file_entry(path))
    except AssertionError as error:
        result["status"] = measurement.STATUS_FAILED
        measurement.record_event(result, "baseline_policy_failed", str(error))
        result["checks"].append(
            measurement.check(
                "baseline_policy", measurement.CHECK_HARD, measurement.STATUS_FAILED,
                str(error),
            )
        )
    result.pop("baseline_candidate", None)
    result["config"] = dict(result["config"])
    result["config"]["requested"] = dict(
        first["config"].get("requested") or {}, topology=first["config"].get(
            "requested", {}
        ).get("topology")
    )
    _ = measurement.write_result(Path(output_dir) / f"{run_id}.json", result)
    return result


def gather(children, output_dir):
    """Copy every child's result beside the family's own files.

    Each child ran in `<output_dir>/<run_id>/`; the path is rebuilt from that
    layout because a result read back from disk has its paths scrubbed.
    """
    for child in children:
        source = Path(output_dir) / child["run_id"] / f"{child['run_id']}.json"
        destination = Path(output_dir) / source.name
        if source.resolve() != destination.resolve():
            destination.write_bytes(source.read_bytes())


def diagnostic_summary(child) -> dict:
    """What a diagnostic pair adds to its family's evidence."""
    lifetimes = child.get("observations", {}).get("lifetimes", [])
    return {
        "run_id": child["run_id"],
        "status": child["status"],
        "allocator_conf": child["environment"].get("allocator_conf"),
        "metrics": child.get("metrics"),
        "phases": {
            life["label"]: {
                phase: {name: entry["terms"][name]["median"] for name in (
                    "rss_bytes", "heap_resident_bytes", "jemalloc_allocated_bytes",
                    "allocator_retention_bytes", "tracked_heap_bytes",
                    "accounted_bytes", "unexplained_bytes",
                )}
                for phase, entry in life["phases"].items()
            }
            for life in lifetimes
        },
        "profiles": {
            life["label"]: life.get("profiles") for life in lifetimes if life.get("profiles")
        },
    }


def memory_family(topology, output_dir, report_dir=None, *, pairs=PAIRS,
                  diagnostics=(), probes=False, provenance=None, options_evidence=None,
                  allocator=None, **options) -> dict:
    """Every pair of one topology, optional diagnostics and probes, and the index."""
    try:
        from . import measure, performance
    except ImportError:
        import measure
        import performance
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    case = family_case(topology, options.get("config", "harness-rate")) + (
        f"-{allocator}" if allocator else ""
    )
    provenance = provenance or measure.prepare_build()
    spec = memory_spec(topology, ordinal=1, case=case, **options)
    started = time.monotonic()
    prebuilt = prebuild(spec.workload)
    print(f"{case}: {len(prebuilt)} requests prebuilt in {time.monotonic() - started:.0f}s",
          flush=True)
    rejected = []
    store = None
    if spec.store != "local":
        # One pinned store for the whole family, started before any lease,
        # as the stage family starts its own.
        store = measurement.test_e2e.DockerStore(spec.store).__enter__()
        store.host_pid = performance.container_pid(store.container)
        store.pinned = performance.pin_container(
            store.container, pair_allocation(spec).get("store", [])
        )
    try:
        children = run_pairs(
            case, topology, output_dir, report_dir, count=pairs,
            provenance=provenance, prebuilt=prebuilt, rejected=rejected, store=store,
            malloc_conf=DIAGNOSTIC_CONF[allocator] if allocator else JEMALLOC_STATS_CONF,
            **options,
        )
    finally:
        if store is not None:
            store.__exit__(None, None, None)
    observations = {"invalidated_and_rerun": rejected}
    diagnostic_children = []
    for name in diagnostics:
        diagnostic_children += run_pairs(
            f"{case}-{name}", topology, output_dir, report_dir, count=1,
            provenance=provenance, prebuilt=prebuilt,
            malloc_conf=DIAGNOSTIC_CONF[name], rejected=rejected, **options,
        )
    if diagnostic_children:
        observations["diagnostics"] = [
            diagnostic_summary(child) for child in diagnostic_children
        ]
    if probes:
        executable = performance.locate_benches()["measurement"]["executable"]
        heap_executable = performance.locate_benches(features=("bench-heap",))[
            "measurement"
        ]["executable"]
        wait_for_lease()
        lease = measurement.HostLease(run_id=f"{case}-probes")
        lease.acquire(deadline_ns=time.monotonic_ns() + 3600 * 10**9)
        try:
            observations["accounting_probes"] = {
                "executable": Path(executable).name,
                "fixtures": run_probes(output_dir / "probes", executable),
            }
            observations["series_row_cost"] = {
                "executable": Path(heap_executable).name,
                "points": run_series_cost(output_dir / "series-cost", heap_executable),
            }
        finally:
            lease.release()
    for name, path in (options_evidence or {}).items():
        observations[name] = json.loads(Path(path).read_text(encoding="ascii"))
    gather(children + diagnostic_children, output_dir)
    aggregate = aggregate_family(
        case, children, output_dir, report_dir,
        ordinal=next_family_ordinal(report_dir, case),
        observations=observations,
    )
    # The diagnostics are children of the index, so their evidence is
    # published with it; they are never part of the stability
    # rule or the baseline, which the aggregate applies to the pairs alone.
    return measure.write_index(
        case, output_dir, report_dir, children + diagnostic_children + [aggregate],
        publishable=True,
    )


def run_memory(output_dir, report_dir=None, **options) -> list:
    """The strict family with its diagnostics and probes, then the buffered one."""
    try:
        from . import measure
    except ImportError:
        import measure
    output_dir = Path(output_dir)
    provenance = measure.prepare_build()
    families = options.pop(
        "families",
        [["strict", "harness-rate"], ["strict", "logs-high-rate"], ["buffered", "harness-rate"]],
    )
    if "topologies" in options:
        families = [[topology, "harness-rate"] for topology in options.pop("topologies")]
    diagnostics = options.pop("diagnostics", ["decay0", "prof"])
    probes = bool(options.pop("probes", True))
    pairs = int(options.pop("pairs", PAIRS))
    # Earlier evidence to carry into the strict index, as name=path: the
    # probes measured on the build before a fix, for instance.
    evidence = options.pop("evidence", {})
    results = []
    for family in families:
        topology, config = family[0], family[1]
        allocator = family[2] if len(family) > 2 else None
        first = topology == "strict" and config == "harness-rate" and not allocator
        results.append(
            memory_family(
                topology,
                output_dir / (family_case(topology, config) + (f"-{allocator}" if allocator else "")),
                report_dir,
                allocator=allocator,
                pairs=pairs,
                # The high-rate shape also gets a heap profile, to attribute
                # the live heap its pipeline counters do not show.
                diagnostics=diagnostics if first else (
                    ["prof"] if "prof" in diagnostics and config == "logs-high-rate" else ()
                ),
                probes=probes and first,
                provenance=provenance,
                options_evidence=evidence if first else None,
                config=config,
                **options,
            )
        )
    return results


# The series counts, signals and denormalization the per-series cost is
# measured at: from one series, where fixed costs dominate, to 100k in one
# request. A logs request stops at 50k: OTAP numbers log records with 16-bit
# ids, and an OTLP logs request of more than 65,536 attributed records is
# refused by the conversion as carrying duplicate attribute keys.
SERIES_COST_POINTS = tuple(
    (signal, series, denormalize)
    for signal in ("logs", "metrics")
    for denormalize in (False, True)
    for series in (1, 10, 100, 1000, 10000, 50000 if signal == "logs" else 100000)
)


def run_series_cost(directory, executable, points=SERIES_COST_POINTS) -> list:
    """Measure the resident cost of a series row, one process per point.

    The DHAT build measures the block's and the cache's heap exactly, and a
    fresh process per point keeps one point's peak from masking the next.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    found = []
    for signal, series, denormalize in points:
        output = directory / f"{signal}-{series}-{'denorm' if denormalize else 'plain'}.json"
        argv = [
            str(executable), "--series-cost", "--signal", signal, "--series", str(series),
            "--output", str(output),
        ] + (["--denormalize"] if denormalize else [])
        done = subprocess.run(
            argv, capture_output=True, text=True, timeout=1200, cwd=str(directory)
        )
        if done.returncode != 0:
            found.append({"signal": signal, "series": series, "denormalize": denormalize,
                          "error": done.stderr[-500:]})
            continue
        found.append(json.loads(output.read_text(encoding="ascii")))
    return found


def residual_shares(entries) -> dict:
    """What the harness residual is made of, at its largest sample.

    Taken at the sample where the residual with a one-second heap gauge
    peaks -- the cadence of the harness's own engine runs. The shares add up
    to that residual exactly: sampling skew is what a one-second gauge misses
    against a 100 ms one; allocator retention and non-heap anonymous growth
    come from the mapping list and jemalloc; the rest is live heap the
    pipeline threads' counters did not show.
    """
    usable = [entry for entry in entries if "allocator_retention_growth_bytes" in entry]
    if not usable:
        return {}
    peak = max(usable, key=lambda entry: entry["one_second_gauge_residual_bytes"])
    return {
        "monotonic_ns": peak["monotonic_ns"],
        "phase": peak["phase"],
        "one_second_gauge_residual_bytes": peak["one_second_gauge_residual_bytes"],
        "sampling_skew_bytes": (
            peak["one_second_gauge_residual_bytes"] - peak["residual_bytes"]
        ),
        "allocator_retention_growth_bytes": peak["allocator_retention_growth_bytes"],
        "non_heap_anonymous_growth_bytes": peak["non_heap_anonymous_growth_bytes"],
        "heap_beyond_tracked_growth_bytes": peak["heap_beyond_tracked_growth_bytes"],
        "hundred_ms_gauge_residual_bytes": peak["residual_bytes"],
    }



# --------------------------------------------------------------------------
# The uncertainty of one pair, over both of its lifetimes
# --------------------------------------------------------------------------


def _spread_stats(values) -> dict:
    """Maximum and 95th percentile of non-negative magnitudes."""
    values = sorted(value for value in values if value is not None)
    if not values:
        return {"count": 0, "max_bytes": None, "p95_bytes": None}
    return {
        "count": len(values),
        "max_bytes": values[-1],
        "p95_bytes": values[int(0.95 * (len(values) - 1))],
    }


def ledger_interval_movement(samples, lifetime) -> list:
    """How far allocated can move within each interval one ledger sample spans.

    A ledger sample pairs its RSS with the latest allocator print and with
    accounted bytes up to one collection -- one sampling interval -- old.
    Over the lifetime's complete chronological print stream, the interval a
    sample spans runs from the last print of the previous sample, through
    every print of this sample, to the first print after it; the movement of
    that interval is the range of allocated over those prints. A sample that
    printed nothing spans from the last earlier print to the next later one.
    """
    stream = []
    last_index = []
    for sample in samples:
        if sample.get("lifetime") != lifetime:
            continue
        stream.extend(
            entry["allocated_bytes"] for entry in sample.get("jemalloc_printed") or []
        )
        last_index.append(len(stream) - 1)
    moves = []
    previous = -1
    for last in last_index:
        start = max(previous, 0)
        end = min(last + 1, len(stream) - 1)
        window = stream[start:end + 1]
        if len(window) > 1:
            moves.append(max(window) - min(window))
        previous = last
    return moves


def pair_uncertainty(child) -> dict:
    """How far a pair's paired quantities can be off, from both lifetimes.

    Read from a published pair result, so it can be recomputed for any
    committed family. Four sources, each as its maximum (a bound over the
    samples taken) and its 95th percentile (an estimate):

    * block pairing: the change of the measured engine's accounted bytes,
      and of either lifetime's RSS, between consecutive collections;
    * control heap: how far the control's allocated heap strays from the
      phase median the ledger subtracts;
    * allocator statistics timing: how old the latest allocator print is
      when a sample reads it, and how far allocated moves within the whole
      interval each ledger sample spans, over the lifetime's complete print
      stream (`ledger_interval_movement`).

    `bound_bytes` adds the maxima and is a bound only over what was sampled
    (a transient between two observations can exceed it);
    `estimate_bytes` adds the 95th percentiles and is labelled an estimate.
    """
    lifetimes = {life["label"]: life for life in
                 (child.get("observations") or {}).get("lifetimes", [])}
    samples = child.get("samples") or []

    def pairing(label, name, field):
        """One lifetime's collection-to-collection change of one term."""
        entry = ((lifetimes.get(label) or {}).get("block_pair_uncertainty") or {}).get(name)
        return (entry or {}).get(field)

    control_devs = []
    for phase in ("load", "retained"):
        values = [
            (sample.get("jemalloc") or {}).get("allocated_bytes")
            for sample in samples
            if sample.get("lifetime") == "control" and sample.get("phase") == phase
        ]
        values = [value for value in values if value is not None]
        if values:
            middle = summarize(values)["median"]
            control_devs.extend(abs(value - middle) for value in values)
    control = _spread_stats(control_devs)
    ages = _spread_stats(
        sample.get("jemalloc_age_ns") for sample in samples
        if sample.get("lifetime") == "measured"
    )
    between_prints = _spread_stats(ledger_interval_movement(samples, "measured"))

    def total(*parts):
        """The sum of the available parts, None when all are absent."""
        parts = [part for part in parts if part is not None]
        return sum(parts) if parts else None

    unexplained = {
        "accounted_pairing_max_bytes": pairing("measured", "accounted_bytes", "max_change_bytes"),
        "accounted_pairing_p95_bytes": pairing("measured", "accounted_bytes", "p95_change_bytes"),
        "control_heap_max_bytes": control["max_bytes"],
        "control_heap_p95_bytes": control["p95_bytes"],
        "allocated_interval_movement_max_bytes": between_prints["max_bytes"],
        "allocated_interval_movement_p95_bytes": between_prints["p95_bytes"],
    }
    unexplained["bound_bytes"] = total(
        unexplained["accounted_pairing_max_bytes"], unexplained["control_heap_max_bytes"],
        unexplained["allocated_interval_movement_max_bytes"],
    )
    unexplained["estimate_bytes"] = total(
        unexplained["accounted_pairing_p95_bytes"], unexplained["control_heap_p95_bytes"],
        unexplained["allocated_interval_movement_p95_bytes"],
    )
    delta = {
        "measured_rss_pairing_max_bytes": pairing("measured", "rss_bytes", "max_change_bytes"),
        "control_rss_pairing_max_bytes": pairing("control", "rss_bytes", "max_change_bytes"),
        "measured_rss_pairing_p95_bytes": pairing("measured", "rss_bytes", "p95_change_bytes"),
        "control_rss_pairing_p95_bytes": pairing("control", "rss_bytes", "p95_change_bytes"),
    }
    delta["bound_bytes"] = total(
        delta["measured_rss_pairing_max_bytes"], delta["control_rss_pairing_max_bytes"]
    )
    delta["estimate_bytes"] = total(
        delta["measured_rss_pairing_p95_bytes"], delta["control_rss_pairing_p95_bytes"]
    )
    return {
        "load_unexplained_median_bytes": unexplained,
        "exporter_peak_rss_delta_bytes": delta,
        "allocator_print_age_ns": {
            "max": ages["max_bytes"], "p95": ages["p95_bytes"], "count": ages["count"],
        },
        "labels": {
            "bound_bytes": "sum of maxima over the sampled observations",
            "estimate_bytes": "sum of 95th percentiles; an estimate, not a bound",
        },
    }


# --------------------------------------------------------------------------
# Re-aggregating published families under a corrected ledger
# --------------------------------------------------------------------------


def reaggregate(index_names, output_dir, report_dir=None,
                run_id="memory-ledger-reaggregation-r1") -> dict:
    """Re-judge published families from their committed pair results.

    For each named index the family aggregate and its pairs are read back
    from the report directory. Each pair's ledger gate is recomputed with no
    workspace term, from the per-sample residuals the pair recorded, and its
    uncertainty over both lifetimes; the family's stability verdict is read
    from its aggregate unchanged. The result is a new published document that
    references every file it read by hash; nothing already published is
    rewritten.
    """
    report = measurement.resolve_report_dir(report_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    families = []
    files = []
    first_child = None
    for name in index_names:
        # A family aggregate (`...-fNNN`) is read directly; an index name
        # resolves to the aggregate it currently lists.
        if re.search(r"-f\d{3}$", name):
            aggregate_entry = {"name": f"{name}.json"}
        else:
            files.append(measurement.file_entry(report / f"{name}.json"))
            index = json.loads((report / f"{name}.json").read_text(encoding="ascii"))
            aggregate_entry = next(
                entry for entry in index["run_files"]
                if re.search(r"-f\d{3}\.json$", entry["name"])
            )
        aggregate = json.loads((report / aggregate_entry["name"]).read_text(encoding="ascii"))
        pairs = []
        for entry in aggregate["run_files"]:
            path = report / entry["name"]
            child = json.loads(path.read_text(encoding="ascii"))
            first_child = first_child or child
            files.append(measurement.file_entry(path))
            residuals = [
                dict(item, residual_bytes=item["residual_without_workspace_bytes"])
                for item in (child.get("observations") or {}).get("ledger_residuals", [])
            ]
            peak = max(
                (sample.get("rss_bytes") or 0 for sample in child.get("samples", [])
                 if sample.get("lifetime") == "measured"),
                default=0,
            )
            gate = ledger_check(residuals, peak)
            beyond = sorted(
                (item["residual_bytes"] for item in residuals), reverse=True
            )[:3]
            pairs.append({
                "run_id": child["run_id"],
                "ledger_gate": gate,
                "ledger_residual_max_bytes": beyond[0] if beyond else None,
                "flush_interval_samples": sum(1 for item in residuals if item.get("flush_interval")),
                "uncertainty": pair_uncertainty(child),
            })
        files.append(measurement.file_entry(report / aggregate_entry["name"]))
        stability = next(
            (entry for entry in aggregate["checks"] if entry["name"] == "pair_stability"), None
        )
        gates_pass = all(
            pair["ledger_gate"]["status"] == measurement.STATUS_PASSED for pair in pairs
        )
        children = [
            json.loads((report / entry["name"]).read_text(encoding="ascii"))
            for entry in aggregate["run_files"]
        ]
        signs = {}
        for metric in SIGNED_METRICS:
            values = [child["metrics"].get(metric) for child in children]
            bound = max(
                (pair["uncertainty"][metric]["bound_bytes"] or 0 for pair in pairs), default=None
            )
            signs[metric] = {
                "values": values, "bound_bytes": bound,
                "consistent": signed_consistent(values, bound),
            }
        families.append({
            "index": name,
            "aggregate": aggregate["run_id"],
            "ledger_gate_passed_in_every_pair": gates_pass,
            "pair_stability": stability,
            "paired_sign": signs,
            "family_passes_under_corrected_ledger": gates_pass and bool(stability)
            and stability["status"] == measurement.STATUS_PASSED
            and all(entry["consistent"] for entry in signs.values()),
            "pairs": pairs,
        })
    result = measurement.new_result({"run_id": run_id, "case": "memory-reaggregation"},
                                    artifact_kind="memory_reaggregation")
    result["environment"] = {
        "start": first_child["environment"]["start"],
        "end": first_child["environment"]["end"],
        "note": "re-judged from committed pair results; no engine was run",
    }
    result["observations"] = {
        "workspace_term": NO_WORKSPACE_TERM,
        "families": families,
    }
    result["metrics"] = {
        "families_count": len(families),
        "families_passing_count": sum(
            1 for family in families if family["family_passes_under_corrected_ledger"]
        ),
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    for family in families:
        result["checks"].append(
            measurement.check(
                f"ledger_{family['index']}", measurement.CHECK_MEASURED,
                measurement.STATUS_PASSED if family["ledger_gate_passed_in_every_pair"]
                else measurement.STATUS_FAILED,
                "; ".join(
                    f"{pair['run_id']}: {pair['ledger_gate']['detail']}" for pair in family["pairs"]
                )[:2000],
            )
        )
    result["run_files"] = files
    result["status"] = measurement.STATUS_PASSED
    measurement.settle_status(result)
    for name, entry in ((entry["name"], entry) for entry in files):
        source = report / name
        destination = output_dir / name
        if not destination.exists():
            destination.write_bytes(source.read_bytes())
    path = measurement.write_result(output_dir / f"{run_id}.json", result)
    _ = measurement.publish_result_tree(path, report_dir)
    return result
