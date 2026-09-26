# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""A seeded chaos schedule and multi-hour soaks on the reference deployment.

`ChaosCase` runs `reference_deployment.Case` with lines that carry a
cardinality profile's series slot, the engine reaching MinIO through a
`faults.FaultRig`, and a recorded schedule of store faults, engine restarts
and Alloy restarts drawn from a seed.
"""

import argparse
import collections
import contextlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import sys
import threading
import time

try:
    from . import faults
    from . import generator
    from . import memory
    from . import reference_deployment as ref
    from . import test_e2e
except ImportError:
    import faults
    import generator
    import memory
    import reference_deployment as ref
    import test_e2e

EVENT_KINDS = ("s3_latency", "http503_burst", "store_outage", "engine_sigterm",
               "engine_sigkill", "alloy_restart")
ENGINE_EVENTS = ("engine_sigterm", "engine_sigkill")
# Each store event and the registered rig fault it activates.
STORE_FAULTS = {"s3_latency": "s3_latency", "http503_burst": "http503",
                "store_outage": "store_outage"}
# The range each store event's duration is drawn from, seconds.
DURATION_S = {"s3_latency": (60, 180), "http503_burst": (20, 60), "store_outage": (30, 120)}
# The added response latency of an s3_latency event, milliseconds.
LATENCY_MS = (100, 1000)
LATENCY_JITTER_MS = 100

SAMPLE_PERIOD_S = 10.0
# RSS alone is read every second, so an hour's p99 is not decided by whether a
# few samples land on the peak of a 15 s window's flush.
RSS_PERIOD_S = 1.0
# jemalloc prints its totals into the engine log after each GiB allocated.
JEMALLOC_CONF = "stats_interval:1073741824,stats_interval_opts:Jgmdablxeh"

# A 10 s bucket of input is fresh when its lines' p99 from write to listing is
# within the external freshness alert, 2 x window.interval plus the flush.
FRESHNESS_BUCKET_S = 10
FRESHNESS_HEALTHY_S = 2 * ref.WINDOW_S + 5
# Lines written this long before an event can be held back by it, and a
# recovery must hold this long before that exposure begins.
PRE_EVENT_EXPOSURE_S = 45
RECOVERY_HOLD_S = 30

# A duplicate may come from an event up to this long after its line was
# written, or from a store event's failed block up to this long after it ended.
DUPLICATE_LOOKBACK_S = 120
STORE_EVENT_TAIL_S = 2 * ref.WINDOW_S

# RSS trend: p99 over the quiet samples of the last window against the second,
# a window being an hour of a run of three or more hours. Samples within
# RSS_QUIET_AFTER_S of an event's end are not quiet.
RSS_TREND_WINDOW_S = 3600
RSS_TREND_TOLERANCE = 1.15
RSS_QUIET_AFTER_S = 120
RSS_MIN_QUIET_SAMPLES = 30
# RSS is budgeted at this multiple of the live-memory formula (exporter README, "Sizing").
RSS_FORMULA_MARGIN = 1.25

REPORT_DIR = faults.FAULT_ARCHIVE_ROOT / "reference-alloy" / "canary"


# --------------------------------------------------------------------------
# The schedule
# --------------------------------------------------------------------------

def chaos_schedule(seed, input_s, *, first_s, min_gap_s, max_gap_s, tail_s):
    """The planned events of one run, drawn from `seed`.

    Kinds come from a shuffled bag of every kind, refilled when empty, so each
    six consecutive events hold each kind once. Between one event's planned
    end and the next start lie `min_gap_s` to `max_gap_s`; none starts before
    `first_s` or ends within `tail_s` of the input's end.
    """
    rng = random.Random(seed)
    bag = []
    events = []
    at = first_s
    while True:
        if not bag:
            bag = list(EVENT_KINDS)
            rng.shuffle(bag)
        kind = bag.pop()
        event = {"index": len(events), "kind": kind, "at_s": at}
        if kind in DURATION_S:
            event["duration_s"] = rng.randint(*DURATION_S[kind])
        if kind == "s3_latency":
            event["latency_ms"] = rng.randint(*LATENCY_MS)
            event["jitter_ms"] = LATENCY_JITTER_MS
        if kind == "engine_sigkill":
            event["window_phase_s"] = round(rng.uniform(0, ref.WINDOW_S), 1)
        end = at + event.get("duration_s", 0) + (ref.WINDOW_S if kind == "engine_sigkill" else 0)
        if end + tail_s > input_s:
            return events
        events.append(event)
        at = end + rng.randint(min_gap_s, max_gap_s)


def _activate_latency(rig, parameters):
    """The latency toxic alone on every proxy: slow answers, full bandwidth."""
    toxic = faults.slow_toxics(parameters)[1]
    for proxy in faults.PROXY_PORTS:
        rig.toxiproxy.add_toxic(proxy, toxic)
    return {"toxics": [toxic],
            "api_state": {proxy: rig.toxiproxy.toxics(proxy) for proxy in faults.PROXY_PORTS}}


if "s3_latency" not in faults.FAULTS:
    faults.register_fault("s3_latency", _activate_latency, faults.FAULTS["slow"][1])


# The shipped route from the transform to the batch, which the site stage joins.
BATCH_ROUTE = "logs = [otelcol.processor.batch.series.input]"
# The one stage a site adds: each line's series slot, SEQ_DIGITS digits at
# SLOT_OFFSET, becomes its `logger.name`, a series attribute of the shipped
# engine config.
SITE_STAGE = f"""
otelcol.processor.transform "site" {{
  error_mode = "ignore"
  log_statements {{
    context = "log"
    statements = [
      `set(log.attributes["logger.name"], Substring(log.body, {ref.SLOT_OFFSET}, {ref.SEQ_DIGITS}))`,
    ]
  }}
  output {{
    {BATCH_ROUTE}
  }}
}}
"""


def site_alloy_config(text):
    """The River config with SITE_STAGE between its transform and its batch."""
    if text.count(BATCH_ROUTE) != 1:
        raise AssertionError("the River config no longer routes transform -> batch")
    return text.replace(BATCH_ROUTE, "logs = [otelcol.processor.transform.site.input]") \
        + SITE_STAGE


# --------------------------------------------------------------------------
# Judgement
# --------------------------------------------------------------------------

def percentile(values, q):
    """The nearest-rank `q` quantile of `values`, or None."""
    values = sorted(values)
    if not values:
        return None
    return values[min(len(values) - 1, max(0, int(round(q * len(values) + 0.5)) - 1))]


def executed_events(case_events, first_wall):
    """Each executed chaos event with its start and end in input seconds."""
    starts = {e["index"]: e for e in case_events if e["kind"] == "chaos_start"}
    found = []
    for end in (e for e in case_events if e["kind"] == "chaos_end"):
        start = starts[end["index"]]
        found.append({
            "index": end["index"], "kind": end["event"],
            "start_s": (start["wall"] - first_wall) / 1e9, "end_s": (end["wall"] - first_wall) / 1e9,
            "start_wall": start["wall"], "end_wall": end["wall"],
            "started_boot": end.get("started_boot"),
        })
    return sorted(found, key=lambda e: e["start_s"])


def duplicate_bounds(producers, rate, settings):
    """Duplicate lines one producer may store per event, by kind; None means
    only copies found in the files of a failed block."""
    kill = ref.duplicate_allowance("engine_kill", producers, rate, settings)
    restart = ref.duplicate_allowance("alloy_restart", producers, rate, settings)
    bounds = {kind: None for kind in STORE_FAULTS}
    bounds.update(engine_sigterm=0,
                  engine_sigkill=kill["per_producer_per_event_lines"],
                  alloy_restart=restart["per_producer_per_event_lines"])
    return bounds


def attribute_duplicates(runs, events, write_s, bounds):
    """Every extra copy charged to one event and producer, against `bounds`.

    `runs` hold consecutive duplicated lines with the same copies: `boots`
    (one ordinal per copy) and `in_failed` (copies in a failed block's
    files). The earliest-boot copy is the original. Up to `in_failed` extra
    copies belong to a store event exposed to the line; any other extra copy
    to an Alloy restart exposed to it or to the engine event that started
    the copy's boot. A line is exposed to an event starting at most
    DUPLICATE_LOOKBACK_S after it was written (`write_s`, input seconds) and,
    for a store event, ending at most STORE_EVENT_TAIL_S before.
    """
    charged = collections.Counter()
    problems = []

    def exposed(event, written):
        tail = STORE_EVENT_TAIL_S if event["kind"] in STORE_FAULTS else 0
        return event["start_s"] - DUPLICATE_LOOKBACK_S <= written <= event["end_s"] + tail

    for run in runs:
        written = write_s(run["producer"], run["first_seq"])
        extras = sorted(run["boots"])[1:]
        by_failed = min(len(extras), run.get("in_failed", 0))
        for n, boot in enumerate(extras):
            if n < by_failed:
                candidates = [e for e in events if e["kind"] in STORE_FAULTS
                              and exposed(e, written)]
            else:
                candidates = [e for e in events if exposed(e, written) and (
                    e["kind"] == "alloy_restart"
                    or (e["kind"] in ENGINE_EVENTS and e["started_boot"] == boot))]
            if not candidates:
                problems.append(f"a copy in boot {boot} of producer {run['producer']} lines "
                                f"{run['first_seq']}..{run['last_seq']} belongs to no event")
                continue
            event = min(candidates, key=lambda e: abs(e["start_s"] - written))
            charged[(event["index"], run["producer"])] += run["lines"]
    kinds = {e["index"]: e["kind"] for e in events}
    over = {}
    for (index, producer), lines in charged.items():
        bound = bounds[kinds[index]]
        if bound is not None and lines > bound:
            over[f"event {index} producer {producer}"] = lines
    table = collections.defaultdict(dict)
    for (index, producer), lines in sorted(charged.items()):
        table[index][producer] = lines
    return {"passed": not problems and not over, "per_event": dict(table), "over": over,
            "problems": problems[:20], "problems_count": len(problems)}


def freshness_buckets(groups, feed, objects, first_wall):
    """{producer: {bucket start s: p99 s}} of write-to-listing freshness."""
    per = collections.defaultdict(lambda: collections.defaultdict(list))
    for key, producer, seq, count in groups:
        entry = objects.get("otel/" + key)
        if entry is None:
            continue
        written = ref.write_time_ns(feed, producer, seq + count - 1)
        bucket = int((written - first_wall) / 1e9 // FRESHNESS_BUCKET_S) * FRESHNESS_BUCKET_S
        per[producer][bucket].append(((entry["first_seen_ns"] - written) / 1e9, count))
    return {producer: {bucket: ref.quantiles(values)["p99_s"] for bucket, values in buckets.items()}
            for producer, buckets in per.items()}


def freshness_recovery(events, buckets, input_s):
    """Whether each producer's freshness is back within FRESHNESS_HEALTHY_S
    after each event, holding until the next event's exposure begins, and
    how long after the event's end its stale lines were all listed."""
    verdicts = []
    for n, event in enumerate(events):
        boundary = (events[n + 1]["start_s"] if n + 1 < len(events) else input_s) \
            - PRE_EVENT_EXPOSURE_S
        per_producer = {}
        for producer, series in sorted(buckets.items()):
            window = sorted((b, v) for b, v in series.items()
                            if event["start_s"] - FRESHNESS_BUCKET_S < b < boundary)
            peak = max((v for _b, v in window), default=None)
            recovered_at = None
            for b, _v in window:
                if all(v <= FRESHNESS_HEALTHY_S for bb, v in window if bb >= b):
                    recovered_at = b
                    break
            ok = recovered_at is not None and recovered_at + RECOVERY_HOLD_S <= boundary
            # Caught up when the last stale bucket's lines were listed.
            caught_up = max((b + FRESHNESS_BUCKET_S + v for b, v in window
                             if recovered_at is not None and b < recovered_at), default=None)
            per_producer[producer] = {
                "peak_p99_s": peak, "ok": ok,
                "recovery_s": None if recovered_at is None
                else max(0.0, (caught_up or 0.0) - event["end_s"])}
        recovery = [v["recovery_s"] for v in per_producer.values()]
        verdicts.append({
            "index": event["index"], "kind": event["kind"],
            "passed": bool(per_producer) and all(v["ok"] for v in per_producer.values()),
            "recovery_s": None if None in recovery or not recovery else max(recovery),
            "peak_p99_s": max((v["peak_p99_s"] or 0 for v in per_producer.values()), default=None),
            "per_producer": per_producer})
    return verdicts


def quiet(sample_s, events):
    """Whether a sample at `sample_s` input seconds is outside every event's
    disturbance: from its start to RSS_QUIET_AFTER_S after its end."""
    return not any(e["start_s"] <= sample_s <= e["end_s"] + RSS_QUIET_AFTER_S for e in events)


def rss_trend(points, events, input_s):
    """RSS p99 over the quiet samples of the last window against the second.

    `points` are (input seconds, RSS bytes). The window is RSS_TREND_WINDOW_S
    for a run of at least three windows, which gates; a shorter run is judged
    on sixths of its input and does not gate.
    """
    gating = input_s >= 3 * RSS_TREND_WINDOW_S
    window = RSS_TREND_WINDOW_S if gating else input_s / 6
    second = [v for s, v in points if window <= s < 2 * window and quiet(s, events)]
    last = [v for s, v in points if input_s - window <= s < input_s and quiet(s, events)]
    p_second, p_last = percentile(second, 0.99), percentile(last, 0.99)
    enough = min(len(second), len(last)) >= RSS_MIN_QUIET_SAMPLES
    ratio = p_last / p_second if enough and p_second else None
    return {"gating": gating, "window_s": window, "tolerance": RSS_TREND_TOLERANCE,
            "second_window_p99_bytes": p_second, "last_window_p99_bytes": p_last,
            "second_window_quiet_samples": len(second), "last_window_quiet_samples": len(last),
            "ratio": ratio, "passed": ratio is not None and ratio <= RSS_TREND_TOLERANCE}


def wal_per_event(points, events):
    """Each event's WAL peak beside the "Sizing" rule
    `ingest x (store down + window + 5 s)`, with the ingest bytes per second
    taken as the fastest WAL growth over a store outage."""
    steady = percentile([v for s, v in points if quiet(s, events)], 0.5) or 0
    growth = []
    for event in events:
        inside = [(s, v) for s, v in points if event["start_s"] <= s <= event["end_s"]]
        if event["kind"] == "store_outage" and len(inside) >= 2 and inside[-1][0] > inside[0][0]:
            growth.append((inside[-1][1] - inside[0][1]) / (inside[-1][0] - inside[0][0]))
    ingest = max(growth, default=0.0)
    rows = []
    for event in events:
        peak = max((v for s, v in points
                    if event["start_s"] <= s <= event["end_s"] + RSS_QUIET_AFTER_S), default=None)
        down = event["end_s"] - event["start_s"] if event["kind"] in STORE_FAULTS else 0
        rows.append({"index": event["index"], "kind": event["kind"], "peak_bytes": peak,
                     "sizing_rule_bytes": ingest * (down + ref.WINDOW_S + 5)})
    return {"steady_median_bytes": steady, "ingest_bytes_per_s": ingest, "events": rows}


def memory_bound_bytes(config, alloy_text, budget):
    """The live-memory formula of the exporter README's "Sizing", per worker:
    memory.budget + (max_in_flight + receiver slots) x the largest export."""
    nodes = config["groups"]["default"]["pipelines"]["main"]["nodes"]
    in_flight = int(nodes["buffer"]["config"]["max_in_flight"])
    slots = int(nodes["receiver"]["config"]["protocols"]["grpc"]["max_concurrent_requests"])
    export = int(re.search(r"\bmax_size\s*=\s*(\d+)", alloy_text).group(1))
    return {"formula_bytes": budget + (in_flight + slots) * export,
            "max_in_flight": in_flight, "receiver_slots": slots, "export_max_bytes": export,
            "budget_bytes": budget}


def resource_checks(samples, limits):
    """Accounted memory, process high-water, blocks, cache and WAL within
    their limits in every sample; the first violations of each are kept."""
    problems = collections.defaultdict(list)
    peaks = collections.Counter()
    for sample in samples:
        m = sample.get("metrics") or {}

        def get(name):
            return m.get(name, 0.0)

        values = {
            "accounted_bytes": get("exporter.series_parquet.memory.accounted"),
            "vm_hwm_bytes": sample.get("VmHWM", 0),
            "block_active_bytes": get("exporter.series_parquet.block.active"),
            "block_flushing_bytes": get("exporter.series_parquet.block.flushing"),
            "series_cache_entries": get("exporter.series_parquet.series_cache.entries"),
            "wal_used_bytes": get("processor.durable_buffer.storage.bytes.used"),
        }
        for name, value in values.items():
            peaks[name] = max(peaks[name], value)
        rules = (("accounted_bytes", get("exporter.series_parquet.memory.budget")),
                 ("vm_hwm_bytes", limits["rss_bytes"]),
                 ("block_active_bytes", limits["block_bytes"]),
                 ("block_flushing_bytes", limits["block_bytes"]),
                 ("series_cache_entries", limits["cache_entries"]),
                 ("wal_used_bytes", get("processor.durable_buffer.storage.bytes.cap")))
        for name, limit in rules:
            if limit and values[name] > limit and len(problems[name]) < 5:
                problems[name].append({"at_wall": sample["wall"], "value": values[name],
                                       "limit": limit})
    return {"passed": not problems, "problems": dict(problems), "peaks": dict(peaks),
            "limits": limits}


def event_observed(event, detail, access_log):
    """Whether the fault of one executed event took effect, and the evidence."""
    kind = event["kind"]
    start, end = event["start_wall"] / 1e9, event["end_wall"] / 1e9
    during = [e for e in access_log if "msec" in e and start <= float(e["msec"]) <= end]
    if kind == "s3_latency":
        floor = detail.get("latency_ms", 0) / 1000 * 0.5
        slow = [e for e in during if _seconds(e.get("upstream_response_time")) >= floor]
        return bool(slow), f"{len(slow)} of {len(during)} requests answered after >= {floor} s"
    if kind == "http503_burst":
        refused = [e for e in during if e.get("status") == "503"]
        return bool(refused), f"{len(refused)} of {len(during)} requests answered 503"
    if kind == "store_outage":
        failed = [e for e in during if e.get("status", "").startswith("5")]
        return bool(failed), f"{len(failed)} of {len(during)} requests answered 5xx"
    exit_ = detail.get("exit") or {}
    if kind == "engine_sigterm":
        ok = exit_.get("code") == 0 and not exit_.get("forced") \
            and exit_.get("exit_s", 1e9) <= ref.SIGTERM_GRACE_S + 5
        return ok, f"exit {exit_}"
    if kind == "engine_sigkill":
        return exit_.get("code") == -9, f"exit {exit_}"
    restarts = detail.get("restarts") or []
    return bool(restarts) and all(restarts), f"restarts {restarts}"


def _seconds(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def duplicate_runs(rows, boot_index):
    """Runs of consecutive duplicated lines of one producer with the same copies.

    `rows` are (producer, seq, boot ids per copy, copies in failed blocks).
    """
    runs = []
    for producer, seq, boots, in_failed in rows:
        ordinals = tuple(sorted(boot_index.get(b, -1) for b in boots))
        last = runs[-1] if runs else None
        if last and last["producer"] == producer and last["last_seq"] == seq - 1 \
                and tuple(last["boots"]) == ordinals and last["in_failed"] == in_failed:
            last["last_seq"] = seq
            last["lines"] += 1
        else:
            runs.append({"producer": producer, "first_seq": seq, "last_seq": seq, "lines": 1,
                         "boots": list(ordinals), "copies": len(ordinals),
                         "in_failed": in_failed})
    return runs


def canary_read_back(database, root, profile_json, written, failed_files):
    """The profile's series in the stored lines, and every duplicated line
    with the boot of each copy and its copies in failed blocks' files.

    Reads table `v` that `reference_deployment.read_back` kept in `database`.
    """
    import duckdb
    root = Path(root).resolve()
    profile = generator.CardinalityProfile(**profile_json)
    values = test_e2e.sql_string(root / "v=1/signal=logs/dataset=values/**/*.parquet")
    series = test_e2e.sql_string(root / "v=1/signal=logs/dataset=series/**/*.parquet")
    width = ref.SEQ_DIGITS
    with duckdb.connect(str(database)) as db:
        db.execute(f"SET temp_directory = {test_e2e.sql_string(str(database) + '.tmp')}")
        db.execute(f"SET memory_limit = '{ref.READ_BACK_MEMORY}'")
        db.execute("SET preserve_insertion_order = false")
        db.execute("CREATE OR REPLACE TABLE failed (name VARCHAR)")
        if failed_files:
            db.executemany("INSERT INTO failed VALUES (?)", [(n,) for n in set(failed_files)])
        # `dupkeys` is the read-back's table of duplicated lines.
        rows = db.execute("""
            SELECT v.p, v.s,
                   list(regexp_extract(filename, '-([0-9a-f]+)-[0-9]+\\.parquet$', 1)),
                   count(*) FILTER (WHERE regexp_extract(filename, '[^/]+$')
                                    IN (SELECT name FROM failed))
            FROM v JOIN dupkeys d ON v.p = d.p AND v.s = d.s
            WHERE ok GROUP BY v.p, v.s ORDER BY v.p, v.s""").fetchall()
        seq = f"TRY_CAST(substr(body, 5, {width}) AS BIGINT)"
        slot_text = f"substr(body, {ref.SLOT_OFFSET + 1}, {width})"
        db.execute(f"""
            CREATE OR REPLACE TEMP VIEW x AS
            SELECT series_id, producer_id, {slot_text} AS slot_text, {seq} AS seq
            FROM read_parquet({values}, union_by_name=true, hive_partitioning=false)
            WHERE length(body) = {ref.BODY_BYTES}""")
        db.execute(f"""
            CREATE OR REPLACE TEMP TABLE canonical AS
            SELECT series_id, list_extract(map_extract(attrs, 'logger.name'), 1) AS logger
            FROM read_parquet({series}, union_by_name=true, filename=true,
                              hive_partitioning=false)
            QUALIFY row_number() OVER (
                PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1""")
        expected_slot = f"lpad(CAST({profile.slot_sql('x.seq')} AS VARCHAR), {width}, '0')"
        total, logger_mismatch, slot_mismatch = db.execute(f"""
            SELECT count(*),
                   count(*) FILTER (WHERE c.logger IS DISTINCT FROM x.slot_text),
                   count(*) FILTER (WHERE x.slot_text IS DISTINCT FROM {expected_slot})
            FROM x LEFT JOIN canonical c USING (series_id)""").fetchone()
        stored_series = dict(db.execute(
            "SELECT producer_id, count(DISTINCT series_id) FROM x GROUP BY 1").fetchall())
        expected_series = {}
        for count in sorted(set(written)):
            expected_series[count] = db.execute(
                f"SELECT count(DISTINCT {profile.slot_sql('range')}) FROM range({count})"
            ).fetchone()[0]
    per_producer = {}
    problems = []
    for index, count in enumerate(written):
        pid = ref.producer_id(index)
        per_producer[pid] = {"stored": stored_series.get(pid), "expected": expected_series[count]}
        if stored_series.get(pid) != expected_series[count]:
            problems.append(f"{pid}: {stored_series.get(pid)} series stored, "
                            f"{expected_series[count]} written")
    if logger_mismatch:
        problems.append(f"{logger_mismatch} rows whose descriptor's logger.name is not their slot")
    if slot_mismatch:
        problems.append(f"{slot_mismatch} rows whose slot is not the profile's")
    return {"series": {"rows_checked": total, "logger_mismatch_rows": logger_mismatch,
                       "slot_mismatch_rows": slot_mismatch, "per_producer": per_producer,
                       "stored_total": sum(v or 0 for v in stored_series.values()),
                       "problems": problems, "passed": not problems},
            "duplicate_rows": rows}


# --------------------------------------------------------------------------
# The case
# --------------------------------------------------------------------------

class ChaosSampler(ref.Sampler):
    """Every SAMPLE_PERIOD_S: the reference sample, jemalloc's latest totals
    and the age of the newest values object listed."""

    def __init__(self, case):
        super().__init__(case, engine_period_s=SAMPLE_PERIOD_S, alloy_period_s=SAMPLE_PERIOD_S)
        self.allocator = {}
        self.latest_print = {}
        self.rss = []

    def start(self):
        super().start()
        thread = threading.Thread(target=self._rss, daemon=True)
        thread.start()
        self.threads.append(thread)

    def _rss(self):
        while not self.stop_event.is_set():
            engine = self.case.engine
            if engine.process is not None and engine.process.poll() is None:
                found = engine.rss()
                if "VmRSS" in found:
                    self.rss.append((time.time_ns(), engine.boot, found["VmRSS"]))
            self.stop_event.wait(RSS_PERIOD_S)

    def annotate(self, sample):
        engine = self.case.engine
        stats = self.allocator.setdefault(engine.boot, memory.JemallocStats(engine.log_path))
        prints = stats.poll()
        if prints:
            self.latest_print[engine.boot] = prints[-1]
        if engine.boot in self.latest_print:
            sample["jemalloc"] = self.latest_print[engine.boot]
        newest = max((entry["last_modified_ns"] for key, entry in list(self.objects.items())
                      if "/dataset=values/" in key), default=None)
        if newest is not None:
            sample["newest_values_age_s"] = (time.time_ns() - newest) / 1e9


class ChaosCase(ref.Case):
    """The reference deployment through a seeded chaos schedule."""

    def __init__(self, store_kind, work_dir, *, profile, input_s, seed, options):
        options = dict(options, baseline_s=0)
        super().__init__("chaos", store_kind, work_dir, producers=options.pop("producers",
                                                                             ref.PRODUCERS),
                         rate=options.pop("rate", ref.RATE_PER_PRODUCER), options=options)
        self.input_s = input_s
        self.seed = seed
        self.profile = generator.CardinalityProfile.for_run(profile, self.producers,
                                                            self.rate * input_s)
        self.schedule = chaos_schedule(
            seed, input_s, first_s=options.get("first_s", 600),
            min_gap_s=options.get("min_gap_s", 300), max_gap_s=options.get("max_gap_s", 480),
            tail_s=options.get("tail_s", 600))
        self.rig = None

    @property
    def alloy_text(self):
        return site_alloy_config(super().alloy_text)

    @contextlib.contextmanager
    def store_route(self, store):
        with faults.FaultRig(store, self.work / "rig", cores=ref.STORE_CPUS) as rig:
            self.rig = rig
            try:
                yield rig.route_endpoint
            finally:
                self.access_log = faults.parse_access_log(rig.artifact_dir / faults.ACCESS_LOG)
                with contextlib.suppress(OSError):
                    shutil.copy(rig.artifact_dir / faults.ACCESS_LOG,
                                self.archive / faults.ACCESS_LOG)
                self.rig_activations = [
                    {k: v for k, v in entry.items() if k != "state"}
                    for entry in rig.activations]

    def make_sampler(self):
        return ChaosSampler(self)

    def feeder_profile(self):
        return self.profile.as_json()

    def fault_chaos(self):
        start = next(e for e in self.events if e["kind"] == "input_started")["t"]
        for planned in self.schedule:
            self.sleep_until(start + int(planned["at_s"] * 1e9))
            self.run_event(planned)
        self.sleep_until(start + int(self.input_s * 1e9))

    @staticmethod
    def sleep_until(monotonic_ns):
        delay = (monotonic_ns - time.monotonic_ns()) / 1e9
        if delay > 0:
            time.sleep(delay)

    def run_event(self, planned):
        kind = planned["kind"]
        if kind == "engine_sigkill":
            time.sleep((planned["window_phase_s"] - time.time() % ref.WINDOW_S) % ref.WINDOW_S)
        fields = {k: v for k, v in planned.items() if k != "kind"}
        self.event("chaos_start", event=kind, **fields)
        detail = {}
        if kind in STORE_FAULTS:
            parameters = {k: planned[k] for k in ("latency_ms", "jitter_ms") if k in planned}
            self.rig.activate(STORE_FAULTS[kind], parameters)
            time.sleep(planned["duration_s"])
            self.rig.recover()
            detail.update(parameters)
        elif kind == "engine_sigterm":
            detail["exit"] = self.engine.terminate()
            self.new_engine()
            detail["started_boot"] = self.engine.boot
        elif kind == "engine_sigkill":
            detail["window_phase_actual_s"] = round(time.time() % ref.WINDOW_S, 3)
            detail["exit"] = self.engine.kill()
            self.new_engine()
            detail["started_boot"] = self.engine.boot
        else:
            threads = [threading.Thread(target=alloy.restart) for alloy in self.alloys]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            detail["restarts"] = [a.restarts[-1] if a.restarts else None for a in self.alloys]
        self.event("chaos_end", index=planned["index"], event=kind, **detail)

    def read_back(self, local):
        database = self.work / "read-back.duckdb"
        oracle = ref.read_back(local, self.feed["written"],
                               self.options.get("body_bytes", ref.BODY_BYTES), database=database)
        self.engine_events = [faults.engine_events(e.log_path.read_text(errors="replace"))
                              for e in self.engines if e.log_path.is_file()]
        failed = [name for found in self.engine_events for name in found["failed_files"]]
        oracle["canary"] = canary_read_back(database, local, self.profile.as_json(),
                                            self.feed["written"], failed)
        return oracle

    def duplicate_verdict(self, dup, settings):
        first_wall = next(e for e in self.events if e["kind"] == "input_started")["wall"]
        events = executed_events(self.events, first_wall)
        boots = {e.boot_id(): e.boot for e in self.engines if e.boot_id()}
        runs = duplicate_runs(self.oracle["canary"].pop("duplicate_rows"), boots)
        bounds = duplicate_bounds(self.producers, self.rate, settings)
        feed = self.feed

        def write_s(producer, seq):
            return (ref.write_time_ns(feed, producer, seq) - first_wall) / 1e9

        verdict = attribute_duplicates(runs, events, write_s, bounds)
        self.duplicate_attribution = dict(verdict, runs=runs[:200], runs_count=len(runs))
        return bounds, verdict["passed"], (
            f"per event and producer {verdict['per_event']}; bounds {bounds}; "
            f"over {verdict['over']}; {verdict['problems'][:5]}")

    def fault_checks(self, check, events, statuses, failed_lines, backpressure, flush_failures,
                     retries, samples):
        first_wall = events["input_started"]["wall"]
        executed = executed_events(self.events, first_wall)
        ends = {e["index"]: e for e in self.events if e["kind"] == "chaos_end"}
        observed = []
        for event in executed:
            passed, detail = event_observed(event, ends[event["index"]], self.access_log)
            observed.append({"index": event["index"], "kind": event["kind"], "passed": passed,
                             "detail": detail})
        check("events_observed", len(executed) == len(self.schedule)
              and all(o["passed"] for o in observed),
              [o for o in observed if not o["passed"]] or f"{len(observed)} events")
        check("series_profile", self.oracle["canary"]["series"]["passed"],
              self.oracle["canary"]["series"]["problems"][:5])
        config = self.engines[0].config
        exporter = config["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]["config"]
        budget = max((ref.metric(s, "exporter.series_parquet.memory.budget") for s in samples),
                     default=0)
        bound = memory_bound_bytes(config, self.alloy_text, budget)
        limits = {"rss_bytes": RSS_FORMULA_MARGIN * bound["formula_bytes"],
                  "block_bytes": faults.soak_byte_size(exporter["window"]["max_block_bytes"]),
                  "cache_entries": int(exporter["series_cache"]["max_entries"])}
        resources = resource_checks(samples, limits)
        check("resources_within_bounds", resources["passed"], resources["problems"])
        cache = cache_view(samples, input_s=None)
        if self.options.get("expect_cache_pressure"):
            check("cache_pressure", cache["evictions"] > 0
                  and cache["entries_peak"] == limits["cache_entries"],
                  f"evictions {cache['evictions']}, entries peak {cache['entries_peak']} "
                  f"of {limits['cache_entries']}")
        input_s = (events["input_stopped"]["t"] - events["input_started"]["t"]) / 1e9
        points = [((wall - first_wall) / 1e9, rss) for wall, _boot, rss in self.sampler.rss]
        trend = rss_trend(points, executed, input_s)
        trend["every_10s"] = rss_trend(
            [((s["wall"] - first_wall) / 1e9, s["VmRSS"]) for s in samples if "VmRSS" in s],
            executed, input_s)
        if trend["gating"]:
            check("rss_trend", trend["passed"], trend)
        buckets = freshness_buckets(self.oracle.get("groups", []), self.feed,
                                    self.sampler.objects, first_wall)
        recovery = freshness_recovery(executed, buckets, input_s)
        check("freshness_recovers", recovery and all(r["passed"] for r in recovery),
              [r for r in recovery if not r["passed"]][:5] or f"{len(recovery)} events")
        self.canary = {
            "seed": self.seed, "profile": self.profile.as_json(), "input_s": self.input_s,
            "schedule": self.schedule, "executed": executed, "observed": observed,
            "memory_bound": bound, "resources": resources, "rss_trend": trend,
            "freshness_recovery": recovery, "cache": cache_view(samples, input_s),
            "wal_per_event": wal_per_event(
                [((s["wall"] - first_wall) / 1e9,
                  ref.metric(s, "processor.durable_buffer.storage.bytes.used")) for s in samples],
                executed),
            "duplicates": getattr(self, "duplicate_attribution", None),
            "jemalloc_conf": os.environ.get("MALLOC_CONF"),
            "rig_activations": getattr(self, "rig_activations", []),
            "flush_failures": [f for found in self.engine_events
                               for f in found["flush_failures"]],
        }

    def settle(self):
        super().settle()
        self.result["canary"] = self.canary
        first_wall = next(e for e in self.events if e["kind"] == "input_started")["wall"]
        self.result["canary_timeline"] = canary_timeline(self.sampler.engine_samples, first_wall)
        self.result["config"]["alloy_site_stage"] = SITE_STAGE.strip()
        self.result["series_parquet"] = {"oracle_series": self.oracle["canary"]["series"]}

    def archive_raw(self):
        super().archive_raw()
        with contextlib.suppress(Exception):
            (self.archive / "rss-1s.json").write_text(json.dumps(self.sampler.rss))


def cache_view(samples, input_s):
    """Series cache fill and churn: entries, hits, misses and evictions over every boot."""
    last = ref.last_per_boot(samples)

    def total(name):
        return sum(ref.metric(s, f"exporter.series_parquet.{name}") for s in last)

    evictions, hits, misses = total("series_cache.evictions"), total("series_cache.hits"), \
        total("series_cache.misses")
    new = total("series.emitted{reason=new}")
    return {"entries_peak": max((ref.metric(s, "exporter.series_parquet.series_cache.entries")
                                 for s in samples), default=0),
            "evictions": evictions, "evictions_per_s": evictions / input_s if input_s else None,
            "hits": hits, "misses": misses, "series_emitted_new": new,
            "hit_ratio": hits / (hits + misses) if hits + misses else None}


def canary_timeline(samples, first_wall):
    """Every engine sample as one row of the watched values."""
    rows = []
    for s in samples:
        m = s.get("metrics") or {}
        jem = s.get("jemalloc") or {}
        rows.append({
            "at_s": round((s["wall"] - first_wall) / 1e9, 1), "boot": s["boot"],
            "rss": s.get("VmRSS"), "hwm": s.get("VmHWM"),
            "jemalloc_allocated": jem.get("allocated_bytes"),
            "jemalloc_resident": jem.get("resident_bytes"),
            "accounted": m.get("exporter.series_parquet.memory.accounted"),
            "wal_used": m.get("processor.durable_buffer.storage.bytes.used"),
            "wal_disk": s.get("wal_disk_bytes"),
            "queued": sum(v for k, v in m.items()
                          if k.startswith("processor.durable_buffer.items.queued")) if m else None,
            "in_flight": m.get("processor.durable_buffer.in.flight"),
            "cache_entries": m.get("exporter.series_parquet.series_cache.entries"),
            "newest_values_age_s": s.get("newest_values_age_s"),
        })
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="canary", description=__doc__)
    parser.add_argument("--profile", choices=generator.CARDINALITY_PROFILES, required=True)
    parser.add_argument("--input-s", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--purpose", required=True)
    parser.add_argument("--work-dir", default="/var/tmp/series-canary")
    parser.add_argument("--report-dir", default=str(REPORT_DIR))
    parser.add_argument("--option", action="append", default=[],
                        help="name=value, decoded as JSON when it parses")
    parser.add_argument("--schedule-only", action="store_true",
                        help="print the planned events and exit")
    args = parser.parse_args(argv)
    options = {}
    for item in args.option:
        name, _, raw = item.partition("=")
        try:
            options[name] = json.loads(raw)
        except ValueError:
            options[name] = raw
    options.update(label=f"{args.profile}-{args.label}", purpose=args.purpose,
                   profile=args.profile, seed=args.seed, input_s=args.input_s)
    case = ChaosCase("minio", args.work_dir, profile=args.profile, input_s=args.input_s,
                     seed=args.seed, options=options)
    print(json.dumps({"profile": case.profile.as_json(), "schedule": case.schedule}, indent=1),
          flush=True)
    if args.schedule_only:
        return 0
    os.environ["MALLOC_CONF"] = JEMALLOC_CONF
    # A terminated run still removes its containers, engines and work directory.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    result = case.run()
    path = ref.publish(result, args.report_dir)
    print(json.dumps({"status": result["status"], "path": str(path),
                      "failed": [c for c in result["checks"] if not c["passed"]]},
                     indent=1, default=str))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
