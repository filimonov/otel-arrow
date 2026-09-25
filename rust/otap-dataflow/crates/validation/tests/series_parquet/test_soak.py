# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""The soak: its analysis contracts, the PR-tier soaks and the opt-in
thirty-minute soaks.

The analysis contracts start nothing. The PR-tier soaks run a real engine
against a MinIO container for about a minute each. The thirty-minute soaks
need `SERIES_MEASURE_LONG=1`.
"""
import math
import os
import unittest

try:  # Imported as a package module by `python3 -m crates...`.
    from . import capacity
    from . import measure
    from . import measurement
    from . import soak
    from . import test_measurement
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import capacity
    import measure
    import measurement
    import soak
    import test_measurement

INPUT_S = 1800


def rss_rows(start_bytes, slope_bytes_per_s, *, input_s=INPUT_S, observed=True):
    """Synthetic per-second rows: RSS rising linearly over the input phase."""
    return [
        {"t_s": t, "phase": "input", "observed": observed,
         "rss_bytes": start_bytes + slope_bytes_per_s * t}
        for t in range(input_s)
    ]


def soak_result(rows, *, topology="strict", **metrics) -> dict:
    """A passed soak result over `rows`, every counter zero."""
    values = {
        "input_phase_s": float(INPUT_S),
        "sample_coverage_ratio": soak.sample_coverage(rows, INPUT_S),
        **soak.rss_metrics(rows, INPUT_S),
        **{name: 0 for name in soak.ZERO_METRICS},
    }
    values.update(metrics)
    result = test_measurement.measured_result(
        case="soak-strict",
        metrics=values,
        metric_directions={name: soak.SOAK_DIRECTIONS[name] for name in values},
        mandatory_metrics=sorted(values),
    )
    result["config"]["requested"] = {"topology": topology}
    result["soak"] = {"not_applicable": (
        {"buffer_loss_records": soak.NOT_APPLICABLE_NO_BUFFER} if topology == "strict" else {})}
    return result


class SoakAnalysis(unittest.TestCase):
    # Scenario: the strict soak reads its searched ceiling while the report
    # directory is somewhere else entirely.
    # Guarantees: the committed capacity index is found by its own explicit
    # path, so the soak rate does not depend on where runs publish.
    def test_the_published_ceiling_is_read_from_its_explicit_path(self):
        ceiling = dict(soak.SOAKS["soak-strict"]["ceiling"])
        original = measurement.REPORT_DIR
        measurement.REPORT_DIR = measurement.REPO_ROOT / "no-such-report-dir"
        try:
            found = soak.published_ceiling(**ceiling)
        finally:
            measurement.REPORT_DIR = original
        self.assertGreater(found["records_per_s"], 0)
        self.assertTrue((measurement.REPO_ROOT / ceiling["index"]).is_file())

    # Scenario: a soak's RSS rises by half over thirty minutes where its
    # matching baseline stayed flat.
    # Guarantees: the shared regression check fails it; no engine is started.
    def test_rising_rss_fails_against_matching_baseline(self):
        flat = soak_result(rss_rows(1 << 30, 0))
        baseline = measurement.new_baseline(flat, measurement.baseline_fingerprint(flat))
        rising = soak_result(rss_rows(1 << 30, (1 << 30) / 2 / INPUT_S))
        self.assertGreater(rising["metrics"]["rss_post_warmup_median_ratio"], 1.25)
        with self.assertRaisesRegex(AssertionError, "rss_post_warmup_median_ratio"):
            soak.soak_checks(rising, baselines={baseline["fingerprint"]: baseline})

    # Scenario: the same rising soak meets no baseline of its fingerprint.
    # Guarantees: it becomes a baseline candidate, and only when every
    # validity gate passed.
    def test_rising_rss_without_baseline_is_a_candidate_after_validity(self):
        rising = soak_result(rss_rows(1 << 30, (1 << 30) / 2 / INPUT_S))
        decision = soak.soak_checks(rising, baselines={})
        self.assertEqual(decision["action"], "created")
        invalid = soak_result(rss_rows(1 << 30, (1 << 30) / 2 / INPUT_S))
        invalid["checks"] = test_measurement.passed_hard_checks(
            except_names=("no_concurrent_build",)) + [measurement.check(
                "no_concurrent_build", measurement.CHECK_HARD, measurement.STATUS_FAILED)]
        with self.assertRaisesRegex(AssertionError, "no_concurrent_build"):
            soak.soak_checks(invalid, baselines={})
        self.assertNotIn("baseline_candidate", invalid)

    # Scenario: a soak lost one acknowledged record and one metric is absent.
    # Guarantees: the loss fails and the absent counter is an
    # instrumentation error, never a zero.
    def test_counters_fail_and_absence_is_an_error(self):
        lost = soak_result(rss_rows(1 << 30, 0), missing_acked_records=1)
        with self.assertRaisesRegex(AssertionError, "missing_acked_records=1"):
            soak.soak_checks(lost, baselines={})
        absent = soak_result(rss_rows(1 << 30, 0))
        del absent["metrics"]["reader_disagreements_count"]
        del absent["metric_directions"]["reader_disagreements_count"]
        with self.assertRaisesRegex(AssertionError, "reader_disagreements_count absent"):
            soak.soak_checks(absent, baselines={})

    # Scenario: a strict soak reports the buffer-loss counter without saying
    # it does not apply.
    # Guarantees: an untagged strict zero fails; the tagged one passes.
    def test_strict_buffer_counter_needs_not_applicable_tag(self):
        untagged = soak_result(rss_rows(1 << 30, 0))
        untagged["soak"]["not_applicable"] = {}
        with self.assertRaisesRegex(AssertionError, "not_applicable: no_buffer"):
            soak.soak_checks(untagged, baselines={})
        _ = soak.soak_checks(soak_result(rss_rows(1 << 30, 0)), baselines={})

    # Scenario: two percent of the input seconds carry no sample.
    # Guarantees: the coverage floor of 99 percent fails the soak.
    def test_sample_coverage_floor(self):
        rows = rss_rows(1 << 30, 0)
        for row in rows[::50]:
            row["observed"] = False
        self.assertLess(soak.sample_coverage(rows, INPUT_S), soak.SAMPLE_COVERAGE_FLOOR)
        with self.assertRaisesRegex(AssertionError, "insufficient RSS/rate samples"):
            soak.soak_checks(soak_result(rows), baselines={})

    # Scenario: the strict soak's cohorts at the Task 5 MinIO raised ceiling.
    # Guarantees: the measured cohort covers the whole input phase and the
    # warm-up cohort is labelled apart; the 100-request smoke default is gone.
    def test_cohorts_cover_the_input_phase(self):
        rate = int(soak.SOAK_FRACTION * 187_000)
        warm, measured = soak.cohorts(rate, capacity.RECORDS_PER_REQUEST,
                                      soak.SOAK_WARMUP_S, INPUT_S)
        self.assertEqual(measured, math.ceil(rate * INPUT_S / capacity.RECORDS_PER_REQUEST))
        self.assertGreaterEqual(measured * capacity.RECORDS_PER_REQUEST / rate, INPUT_S)
        self.assertEqual(warm, math.ceil(rate * soak.SOAK_WARMUP_S
                                         / capacity.RECORDS_PER_REQUEST))

    # Scenario: the measured cohort's sends span 1800 s, or stop early.
    # Guarantees: the input phase is the producers' active time, so an early
    # end fails the duration gate instead of counting idle time.
    def test_input_phase_is_producer_active_time(self):
        interval = 10**9
        indexes = list(range(0, 1810))
        sends = {"indexes": indexes, "sent": [10 * interval + k * interval for k in indexes]}
        full = soak.input_phase(sends, first_measured_index=10, records_per_request=1000,
                                rate=1000)
        self.assertEqual(full["input_phase_s"], 1800.0)
        sends["sent"] = sends["sent"][:1000] + [0] * 810
        short = soak.input_phase(sends, first_measured_index=10, records_per_request=1000,
                                 rate=1000)
        self.assertLess(short["input_phase_s"], 1800)

    # Scenario: a workload with one percent churn over ten thousand hot slots.
    # Guarantees: every hundredth record takes a slot no other record uses
    # and the rest cycle the hot slots; churn needs per-record identity.
    def test_churn_slots(self):
        workload = measurement.Workload(requests=10, records_per_request=1000, series=10000,
                                        series_scope="record", churn_every=100)
        slots = [workload.slot(request, point) for request in range(10)
                 for point in range(1000)]
        churned = [slot for slot in slots if slot >= 10000]
        self.assertEqual(len(churned), 100)
        self.assertEqual(len(set(churned)), 100)
        self.assertTrue(all(slot < 10000 for k, slot in enumerate(slots) if k % 100 != 99))
        self.assertEqual(workload.as_json()["churn_every"], 100)
        self.assertNotIn("churn_every", measurement.Workload().as_json())
        with self.assertRaises(ValueError):
            measurement.Workload(churn_every=100)

    # Scenario: two requests sent and answered at known instants.
    # Guarantees: the per-second rows count attempts, acknowledgements,
    # commits and bytes in the second they happened, the in-flight bytes
    # and the oldest unanswered age at each second's end.
    def test_per_second_rows(self):
        second = 10**9
        start = 100 * second
        sends = {"indexes": [0, 1], "sent": [start + second // 2, start + second + 1],
                 "finish": [start + 3 * second, start + second + 2], "code": [0, 0]}
        objects = [("a.parquet", 700, start + 3 * second + 5)]
        visible = {0: start + 3 * second + 5, 1: start + 3 * second + 5}
        rows = soak.per_second_rows(
            samples=[], side=[], sends=sends, objects=objects, visible=visible,
            window=(start, start + 4 * second), input_end_ns=start + 4 * second,
            epoch_offset_ns=0, records_per_request=10, size_of=lambda index: 100,
            buffered=False)
        by_second = {row["t_s"]: row for row in rows}
        self.assertEqual(by_second[0]["attempted_records"], 10)
        self.assertEqual(by_second[0]["in_flight_bytes"], 100)
        self.assertEqual(by_second[0]["oldest_unanswered_s"], 0.5)
        self.assertEqual(by_second[1]["acked_records"], 10)
        self.assertEqual(by_second[3]["committed_records"], 20)
        self.assertEqual(by_second[3]["object_bytes"], 700)
        self.assertEqual(by_second[3]["acked_records"], 10)
        self.assertFalse(by_second[0]["observed"])

    # Scenario: input stopped with one request unacknowledged and two not
    # yet in a completed object.
    # Guarantees: both backlogs and the time each took to drain are counted.
    def test_backlog_and_drain(self):
        second = 10**9
        sends = {"indexes": [0, 1, 2], "sent": [1, 2, 3],
                 "finish": [5, 3 * second, 6], "code": [0, 0, 0]}
        visible = {0: 7, 1: 4 * second, 2: 2 * second}
        drained = soak.backlog_and_drain(sends, visible, stop_ns=second, epoch_offset_ns=0,
                                         records_per_request=10)
        self.assertEqual(drained["ack_backlog_at_stop_records"], 10)
        self.assertEqual(drained["ack_drain_s"], 2.0)
        self.assertEqual(drained["storage_backlog_at_stop_records"], 20)
        self.assertEqual(drained["storage_drain_s"], 3.0)
        self.assertEqual(drained["acked_not_committed_requests_count"], 0)

    # Scenario: one residual of ten thousand lies beyond the tolerance.
    # Guarantees: it is kept whole with the allocator pairs around it, while
    # an in-band residual is not.
    def test_residual_excursions_keep_their_pairs(self):
        pairs = [{"monotonic_ns": k, "jemalloc_allocated_bytes": k} for k in range(10001)]
        residuals = [{"monotonic_ns": k + 1, "residual_bytes": 0} for k in range(10000)]
        residuals[5000]["residual_bytes"] = 200 << 20
        kept = measurement.residual_excursions(residuals, pairs, 1 << 30)
        self.assertEqual(kept["beyond_tolerance_count"], 1)
        self.assertEqual(len(kept["events"]), 1)
        offsets = [pair["offset"] for pair in kept["events"][0]["pairs"]]
        self.assertEqual(offsets, list(range(-measurement.EXCURSION_NEIGHBOURS,
                                             measurement.EXCURSION_NEIGHBOURS + 1)))
        self.assertEqual(kept["events"][0]["pairs"][measurement.EXCURSION_NEIGHBOURS]["monotonic_ns"],
                         5001)


def engine_or_skip():
    """Skip when no engine is built, unless Docker is required."""
    if not measure.engine_binary().is_file() and os.environ.get("SERIES_REQUIRE_DOCKER") != "1":
        raise unittest.SkipTest(f"no engine at {measure.engine_binary()}")


class PrSoakTests(measurement.MeasurementTestCase):
    """One minute each against MinIO, publishing nothing (the CI mode)."""

    def run_pr(self, topology):
        engine_or_skip()
        measurement.test_e2e.require_docker_image("minio")
        result = soak.pr_soak_case(self.output_dir, topology=topology, publish=False)
        failed = measure.ci_failures(result)
        self.assertEqual(failed, [], [entry for entry in result["checks"]
                                      if entry["name"] in failed])
        return result

    # Scenario: a strict engine with forced byte and request rotations loses
    # its store past a 3 s flush deadline while a ledgered producer sends.
    # Guarantees: the refusal is a retryable storage NACK after a deadline
    # flush failure, the producer's resends deliver every record, and the
    # retained-state caps and the RSS reconciliation hold.
    def test_strict_store_outage(self):
        result = self.run_pr("strict")
        self.assertGreater(result["observations"]["pr_soak"]["exporter_nacks_by_class"]
                           .get("storage", 0), 0)

    # Scenario: the same outage behind the durable buffer.
    # Guarantees: the buffer retries the refused bundles, every acknowledged
    # record is stored after recovery and both memory domains drain.
    def test_buffered_store_outage(self):
        result = self.run_pr("buffered")
        self.assertGreater(result["observations"]["pr_soak"]["buffer_retries_scheduled"], 0)


class LongSoakTests(measurement.MeasurementTestCase):
    # Scenario: mixed supported telemetry flows for thirty minutes in strict mode.
    # Guarantees: all ACKed IDs survive drain and RSS observations satisfy validity and the Controller baseline policy.
    def test_strict_thirty_minutes(self):
        measurement.require_long()
        result = measure.run_named("soak-strict", self.output_dir)
        self.assertGreaterEqual(result["metrics"]["input_phase_s"], 1800)
        soak.soak_checks(result)

    # Scenario: mixed telemetry flows for thirty minutes through a persistent buffer.
    # Guarantees: WAL ACKs reconcile with stored IDs and both memory domains drain.
    def test_buffered_thirty_minutes(self):
        measurement.require_long()
        result = measure.run_named("soak-buffered", self.output_dir)
        self.assertGreaterEqual(result["metrics"]["input_phase_s"], 1800)
        soak.soak_checks(result)


if __name__ == "__main__":
    unittest.main()
