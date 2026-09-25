# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Contracts of the chaos soak: no engine, no container."""

import collections
import unittest

try:
    from . import canary
    from . import generator
    from . import reference_deployment as ref
except ImportError:
    import canary
    import generator
    import reference_deployment as ref


def event(index, kind, start_s, end_s, started_boot=None):
    return {"index": index, "kind": kind, "start_s": start_s, "end_s": end_s,
            "start_wall": int(start_s * 1e9), "end_wall": int(end_s * 1e9),
            "started_boot": started_boot}


def run(producer, first, lines, boots, in_failed=0):
    return {"producer": producer, "first_seq": first, "last_seq": first + lines - 1,
            "lines": lines, "boots": boots, "copies": len(boots), "in_failed": in_failed}


BOUNDS = {"s3_latency": None, "http503_burst": None, "store_outage": None,
          "engine_sigterm": 0, "engine_sigkill": 83_500, "alloy_restart": 54_000}


class ScheduleContracts(unittest.TestCase):
    # Scenario: the same seed drawn twice, and a different seed.
    # Guarantees: a schedule is a function of its seed and inputs.
    def test_schedule_is_seeded(self):
        args = dict(first_s=600, min_gap_s=300, max_gap_s=480, tail_s=600)
        self.assertEqual(canary.chaos_schedule(7, 14400, **args),
                         canary.chaos_schedule(7, 14400, **args))
        self.assertNotEqual(canary.chaos_schedule(7, 14400, **args),
                            canary.chaos_schedule(8, 14400, **args))

    # Scenario: a four-hour schedule.
    # Guarantees: events keep the minimum gap after each planned end, the
    # first starts at first_s, the last ends before the tail, and every six
    # consecutive events hold each kind once.
    def test_schedule_spacing_and_kinds(self):
        events = canary.chaos_schedule(3, 14400, first_s=600, min_gap_s=300, max_gap_s=480,
                                       tail_s=600)
        self.assertEqual(events[0]["at_s"], 600)
        for before, after in zip(events, events[1:]):
            end = before["at_s"] + before.get("duration_s", 0) + (
                ref.WINDOW_S if before["kind"] == "engine_sigkill" else 0)
            self.assertGreaterEqual(after["at_s"] - end, 300)
            self.assertLessEqual(after["at_s"] - end, 480)
        last = events[-1]
        self.assertLessEqual(last["at_s"] + last.get("duration_s", 0) + ref.WINDOW_S, 14400 - 600)
        for start in range(0, len(events) - 5, 6):
            self.assertEqual(sorted(e["kind"] for e in events[start:start + 6]),
                             sorted(canary.EVENT_KINDS))
        for e in events:
            if e["kind"] in canary.DURATION_S:
                low, high = canary.DURATION_S[e["kind"]]
                self.assertTrue(low <= e["duration_s"] <= high)


class ProfileContracts(unittest.TestCase):
    # Scenario: each profile's slots for the first and last records of a run,
    # computed in Python and in DuckDB.
    # Guarantees: the feeder and the read-back compute the same slot.
    def test_python_and_sql_slots_agree(self):
        import duckdb
        for name in generator.CARDINALITY_PROFILES:
            profile = generator.CardinalityProfile.for_run(name, 8, 1_000_000)
            rows = duckdb.sql(
                f"SELECT range, {profile.slot_sql('range')} FROM range(1000000) "
                "WHERE range < 20000 OR range >= 980000 ORDER BY range").fetchall()
            self.assertEqual([profile.slot(seq) for seq, _ in rows], [slot for _, slot in rows],
                             name)

    # Scenario: the three profiles over one producer's planned records.
    # Guarantees: stable keeps its hot set, churn reaches its distinct count,
    # and mixed keeps both with one record in five churning.
    def test_profiles_reach_their_cardinality(self):
        import duckdb
        counts = {}
        for name in generator.CARDINALITY_PROFILES:
            profile = generator.CardinalityProfile.for_run(name, 8, 2_000_000)
            counts[name] = duckdb.sql(
                f"SELECT count(DISTINCT {profile.slot_sql('range')}) FROM range(2000000)"
            ).fetchone()[0]
        self.assertEqual(counts["stable"], generator.HOT_SERIES // 8)
        self.assertGreater(counts["churn"], 0.99 * generator.CHURN_DISTINCT_SERIES // 8)
        self.assertGreater(counts["mixed"], counts["churn"])
        mixed = generator.CardinalityProfile.for_run("mixed", 8, 2_000_000)
        churning = sum(mixed.slot(seq) >= generator.CHURN_SLOT_BASE for seq in range(10_000))
        self.assertEqual(churning, 10_000 // generator.MIXED_CHURN_EVERY)

    # Scenario: a line of producer 3 with a slot.
    # Guarantees: the slot sits at SLOT_OFFSET, the line keeps its length and
    # parses back, and the site stage reads exactly that substring.
    def test_slot_line_and_site_stage_agree(self):
        body = ref.line(3, 42, slot=1_000_000_123)
        self.assertEqual(len(body), ref.BODY_BYTES)
        self.assertEqual(ref.parse_line(body), (3, 42))
        self.assertEqual(body[ref.SLOT_OFFSET:ref.SLOT_OFFSET + ref.SEQ_DIGITS], "001000000123")
        self.assertIn(f"Substring(log.body, {ref.SLOT_OFFSET}, {ref.SEQ_DIGITS})",
                      canary.SITE_STAGE)

    # Scenario: the shipped River config with the site stage.
    # Guarantees: the transform feeds the site stage, which feeds the batch;
    # nothing else of the file changes.
    def test_site_stage_is_the_only_change(self):
        text = (ref.test_e2e.WORKSPACE / ref.ALLOY_CONFIG).read_text()
        site = canary.site_alloy_config(text)
        self.assertEqual(site.count(canary.BATCH_ROUTE), 1)
        self.assertEqual(site.replace(canary.SITE_STAGE, "").replace(
            "logs = [otelcol.processor.transform.site.input]", canary.BATCH_ROUTE), text)


class DuplicateContracts(unittest.TestCase):
    EVENTS = [event(0, "engine_sigkill", 300, 302, started_boot=1),
              event(1, "alloy_restart", 500, 510),
              event(2, "store_outage", 900, 960),
              event(3, "engine_sigterm", 1300, 1301, started_boot=2)]

    def judge(self, runs, written_s):
        return canary.attribute_duplicates(runs, self.EVENTS, lambda _p, _s: written_s, BOUNDS)

    # Scenario: lines written 20 s before a SIGKILL stored by boots 0 and 1.
    # Guarantees: the copy in boot 1 is charged to the kill that started it.
    def test_kill_copies_are_charged_to_the_kill(self):
        verdict = self.judge([run(4, 0, 4000, [0, 1])], 280)
        self.assertTrue(verdict["passed"], verdict)
        self.assertEqual(verdict["per_event"], {0: {4: 4000}})

    # Scenario: a copy in boot 1 of a line written long before any event.
    # Guarantees: a copy no event is exposed to fails the check.
    def test_unexposed_copy_fails(self):
        verdict = self.judge([run(4, 0, 10, [0, 1])], 10)
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["problems_count"], 1)

    # Scenario: same-boot copies written just before an Alloy restart, below
    # and above its per-producer bound.
    # Guarantees: they are charged to the restart and the bound is per producer.
    def test_alloy_restart_bound_is_per_producer(self):
        self.assertTrue(self.judge([run(1, 0, 50_000, [1, 1]), run(2, 0, 50_000, [1, 1])],
                                   495)["passed"])
        verdict = self.judge([run(1, 0, 50_000, [1, 1]), run(1, 60_000, 5_000, [1, 1])], 495)
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["over"], {"event 1 producer 1": 55_000})

    # Scenario: copies around a store outage, one found in a failed block's
    # file and one not.
    # Guarantees: only the failed block's copy is explained by the outage.
    def test_store_event_explains_only_failed_block_copies(self):
        self.assertTrue(self.judge([run(0, 0, 9000, [1, 1], in_failed=1)], 930)["passed"])
        self.assertFalse(self.judge([run(0, 0, 9000, [1, 1])], 930)["passed"])

    # Scenario: a copy in the boot a SIGTERM restart started.
    # Guarantees: a graceful restart may cause no duplicate.
    def test_sigterm_allows_none(self):
        verdict = self.judge([run(0, 0, 1, [1, 2])], 1290)
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["over"], {"event 3 producer 0": 1})

    # Scenario: duplicated lines 5..7 of producer 0 with the same boots, then 9.
    # Guarantees: consecutive lines with equal copies form one run.
    def test_duplicate_rows_form_runs(self):
        rows = [(0, 5, ["a", "b"], 0), (0, 6, ["b", "a"], 0), (0, 7, ["a", "b"], 0),
                (0, 9, ["a", "b"], 0), (1, 10, ["a", "a"], 1)]
        runs = canary.duplicate_runs(rows, {"a": 0, "b": 1})
        self.assertEqual([(r["producer"], r["first_seq"], r["lines"], r["boots"], r["in_failed"])
                          for r in runs],
                         [(0, 5, 3, [0, 1], 0), (0, 9, 1, [0, 1], 0), (1, 10, 1, [0, 0], 1)])


class JudgementContracts(unittest.TestCase):
    # Scenario: RSS with a flat baseline and a 1 s peak in every 15 s window,
    # sampled every second for four hours.
    # Guarantees: every hour's p99 sees the peaks, so the trend passes.
    def test_rss_trend_of_a_flush_sawtooth_sampled_every_second(self):
        points = [(s, 700e6 if s % 15 == 0 else 480e6) for s in range(0, 4 * 3600)]
        verdict = canary.rss_trend(points, [], 4 * 3600)
        self.assertEqual(verdict["second_window_p99_bytes"], 700e6)
        self.assertEqual(verdict["last_window_p99_bytes"], 700e6)
        self.assertTrue(verdict["passed"])

    # Scenario: the WAL grows 16 MB/s through a 100 s outage.
    # Guarantees: the ingest estimate is that growth and the rule adds 20 s.
    def test_wal_rule_uses_the_outage_growth(self):
        events = [event(0, "store_outage", 1000, 1100)]
        points = [(s, 3e8 + (16e6 * (s - 1000) if 1000 <= s <= 1100 else 0))
                  for s in range(0, 2000, 10)]
        found = canary.wal_per_event(points, events)
        self.assertAlmostEqual(found["ingest_bytes_per_s"], 16e6)
        self.assertAlmostEqual(found["events"][0]["sizing_rule_bytes"], 16e6 * 120)

    # Scenario: flat RSS with a spike during an event, and RSS that grows 30
    # percent between the second and the last hour.
    # Guarantees: event samples are not quiet; a rising baseline fails.
    def test_rss_trend(self):
        events = [event(0, "store_outage", 5000, 5100)]
        flat = [(s, 800e6 if 5000 <= s <= 5200 else 500e6) for s in range(0, 4 * 3600, 10)]
        verdict = canary.rss_trend(flat, events, 4 * 3600)
        self.assertTrue(verdict["gating"])
        self.assertTrue(verdict["passed"], verdict)
        rising = [(s, 500e6 * (1 + 0.1 * s / 3600)) for s in range(0, 4 * 3600, 10)]
        self.assertFalse(canary.rss_trend(rising, [], 4 * 3600)["passed"])
        self.assertFalse(canary.rss_trend(flat, [], 1800)["gating"])

    # Scenario: a producer whose freshness peaks after an outage and returns
    # 40 s after it, and one that never returns before the next event.
    # Guarantees: recovery is measured from the event's end and must hold
    # until the next event's exposure.
    def test_freshness_recovery(self):
        events = [event(0, "store_outage", 100, 200), event(1, "alloy_restart", 500, 510)]
        good = {b: (150.0 if 100 <= b < 240 else 12.0) for b in range(0, 900, 10)}
        bad = {b: (150.0 if 100 <= b < 480 else 12.0) for b in range(0, 900, 10)}
        verdicts = canary.freshness_recovery(events, {0: good}, 900)
        self.assertTrue(verdicts[0]["passed"])
        self.assertEqual(verdicts[0]["recovery_s"], 190.0)
        self.assertFalse(canary.freshness_recovery(events, {0: good, 1: bad}, 900)[0]["passed"])

    # Scenario: samples with accounted memory over the budget and a cache
    # over its limit.
    # Guarantees: each limit is checked in every sample.
    def test_resource_limits(self):
        def sample(accounted, entries):
            return {"wall": 1, "VmHWM": 1e9, "metrics": {
                "exporter.series_parquet.memory.accounted": accounted,
                "exporter.series_parquet.memory.budget": 100.0,
                "exporter.series_parquet.series_cache.entries": entries}}
        limits = {"rss_bytes": 4e9, "block_bytes": 500, "cache_entries": 10}
        self.assertTrue(canary.resource_checks([sample(50, 10)], limits)["passed"])
        found = canary.resource_checks([sample(150, 11)], limits)
        self.assertEqual(sorted(found["problems"]), ["accounted_bytes", "series_cache_entries"])

    # Scenario: the shipped engine and River configs with a 1.64 GB budget.
    # Guarantees: the memory formula is budget + (max_in_flight + slots) x 2MiB.
    def test_memory_formula_reads_the_shipped_files(self):
        import yaml
        config = yaml.safe_load((ref.test_e2e.WORKSPACE / ref.ENGINE_CONFIG).read_text())
        alloy = (ref.test_e2e.WORKSPACE / ref.ALLOY_CONFIG).read_text()
        bound = canary.memory_bound_bytes(config, alloy, 1_640_013_824)
        self.assertEqual(bound["formula_bytes"], 1_640_013_824 + (640 + 128) * 2 * 1024 * 1024)

    # Scenario: access-log entries during a 503 burst and a latency event.
    # Guarantees: a store event counts as observed only from requests inside it.
    def test_event_observed_from_the_access_log(self):
        burst = event(0, "http503_burst", 10, 20)
        log = [{"msec": "15.0", "status": "503"}, {"msec": "30.0", "status": "503"}]
        self.assertTrue(canary.event_observed(burst, {}, log)[0])
        self.assertFalse(canary.event_observed(burst, {}, log[1:])[0])
        slow = event(1, "s3_latency", 10, 20)
        log = [{"msec": "12.0", "status": "200", "upstream_response_time": "0.6"}]
        self.assertTrue(canary.event_observed(slow, {"latency_ms": 1000}, log)[0])
        self.assertFalse(canary.event_observed(slow, {"latency_ms": 2000}, log)[0])

    # Scenario: executed chaos events from the case's event log.
    # Guarantees: each is placed on the input's time axis with its started boot.
    def test_executed_events(self):
        log = [{"kind": "chaos_start", "index": 0, "wall": 5_000_000_000},
               {"kind": "chaos_end", "index": 0, "event": "engine_sigkill",
                "wall": 7_000_000_000, "started_boot": 1}]
        found = canary.executed_events(log, 1_000_000_000)
        self.assertEqual((found[0]["start_s"], found[0]["end_s"], found[0]["started_boot"]),
                         (4.0, 6.0, 1))
        self.assertEqual(collections.Counter(e["kind"] for e in found), {"engine_sigkill": 1})


if __name__ == "__main__":
    unittest.main()
