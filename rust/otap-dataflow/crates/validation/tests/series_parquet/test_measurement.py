# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Fast contracts for the measurement harness.

Nothing here starts an engine, a container or a build. These are the rules
every later task's real measurement is read against: the ledger, the loss
oracle, the result schema, the collection-epoch rule, the baseline policy and
the publication and staging rules.
"""
import ctypes
import json
import os
from pathlib import Path
import select
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
    from . import measure
    from . import performance
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement
    import measure
    import performance

Ledger = measurement.Ledger
RunSpec = measurement.RunSpec
Workload = measurement.Workload
assert_records = measurement.assert_records
build_request = measurement.build_request


def temporary_directory(case) -> Path:
    """A directory that lives for exactly one test."""
    holder = tempfile.TemporaryDirectory()
    case.addCleanup(holder.cleanup)
    return Path(holder.name)


class RecordContracts(unittest.TestCase):
    """The producer's own statement of what it sent."""

    # Scenario: an acknowledged histogram is absent although its sibling
    # gauge exists.
    # Guarantees: per-kind stable IDs catch loss that aggregate metric counts
    # conceal.
    def test_missing_metric_kind_fails(self):
        expected = {"r:gauge": "a", "r:histogram": "b"}
        actual = {"r:gauge": ["a", "a"]}
        with self.assertRaisesRegex(AssertionError, "r:histogram"):
            _ = assert_records(expected, actual, healthy=False)

    # Scenario: replay preserves payloads but adds two copies of one
    # supported record.
    # Guarantees: duplicates are counted independently from missing or
    # corrupt records.
    def test_replay_multiplicity(self):
        actual = {"r:log": ["a", "a", "a"], "s:log": ["b"]}
        self.assertEqual(
            assert_records({"r:log": "a", "s:log": "b"}, actual, healthy=False),
            {1: 1, 3: 1},
        )

    # Scenario: rebuilding a workload request for retry uses the original
    # index and seed.
    # Guarantees: retry bytes and all expected record IDs are identical.
    def test_retry_is_deterministic(self):
        workload = Workload(seed=73, records_per_request=9)
        self.assertEqual(build_request(workload, 7), build_request(workload, 7))

    # Scenario: a stored record keeps its payload but under a changed value.
    # Guarantees: a corrupt record is named and never counted as delivered.
    def test_corrupt_record_is_named(self):
        with self.assertRaisesRegex(AssertionError, "corrupt=r:log"):
            _ = assert_records({"r:log": "a"}, {"r:log": ["b"]}, healthy=False)

    # Scenario: a healthy no-retry run stores one supported record twice.
    # Guarantees: multiplicity other than exactly one fails a healthy run.
    def test_healthy_run_requires_single_multiplicity(self):
        with self.assertRaisesRegex(AssertionError, "multiplicity=r:log:2"):
            _ = assert_records({"r:log": "a"}, {"r:log": ["a", "a"]}, healthy=True)

    # Scenario: one request of each signal is built from the same seed.
    # Guarantees: every supported point kind is present, each record id is
    # unique and each carries its own canonical payload hash.
    def test_every_supported_kind_is_produced(self):
        workload = Workload(requests=10, records_per_request=12, body_bytes=64)
        kinds = set()
        identities = set()
        for index in range(workload.requests):
            signal, wire, rows = build_request(workload, index)
            self.assertIn(signal, ("logs", "metrics"))
            self.assertTrue(wire)
            for record_id, kind, digest in rows:
                self.assertNotIn(record_id, identities)
                identities.add(record_id)
                self.assertEqual(len(digest), 64)
                kinds.add(kind)
        self.assertEqual(
            kinds, set(measurement.ALL_KINDS), "a supported point kind is unsent"
        )

    # Scenario: a logs body is too short to carry its own stable id.
    # Guarantees: a workload that cannot be read back is refused at
    # construction rather than measured.
    def test_body_must_hold_the_stable_id(self):
        with self.assertRaisesRegex(ValueError, "body_bytes"):
            _ = Workload(body_bytes=4)

    # Scenario: a metrics point's record id is derived from its stored
    # timestamp and metric name alone.
    # Guarantees: the reader can reconstruct identity without a per-point
    # attribute that would make every point its own series.
    def test_metric_identity_is_recoverable_from_time_and_name(self):
        workload = Workload(requests=4, records_per_request=7, body_bytes=64)
        fields = measurement.metric_point(workload, 0, 3)
        ordinal = (
            fields["time_unix_nano"] - measurement.METRIC_BASE_TIME_NS
        ) // measurement.METRIC_TIME_STEP_NS
        self.assertEqual(ordinal // workload.records_per_request, 0)
        self.assertEqual(ordinal % workload.records_per_request, 3)
        self.assertEqual(
            fields["record_id"],
            measurement.stable_id(workload.seed, 0, 3, fields["kind"]),
        )

    # Scenario: a double travels from the producer through Parquet to a
    # reader.
    # Guarantees: the canonical rendering is the IEEE-754 bit pattern, so a
    # signed zero and a rounding difference are both visible.
    def test_double_rendering_is_exact(self):
        self.assertNotEqual(
            measurement.double_bits(0.0), measurement.double_bits(-0.0)
        )
        self.assertEqual(len(measurement.double_bits(1.5)), 64)


class SpecContracts(unittest.TestCase):
    """What a run may not be asked to do."""

    def spec(self, **overrides):
        """A valid spec with the named fields replaced."""
        fields = {
            "run_id": "t-strict-local-c1-w15-r001",
            "case": "t",
            "topology": "strict",
            "store": "local",
            "cores": (0,),
            "workload": Workload(requests=2, records_per_request=2, body_bytes=64),
        }
        fields.update(overrides)
        return RunSpec(**fields)

    # Scenario: a run names a topology, a store or a core set the harness
    # cannot honour.
    # Guarantees: an impossible run is refused before it produces numbers.
    def test_invalid_inputs_are_refused(self):
        for overrides, pattern in (
            ({"topology": "hybrid"}, "topology"),
            ({"store": "gcs"}, "store"),
            ({"cores": ()}, "cores"),
            ({"cores": (1, 1)}, "distinct"),
            ({"interval_s": 0}, "interval_s"),
            ({"max_in_flight": 4096}, "receiver capacity"),
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, pattern):
                    _ = self.spec(**overrides)

    # Scenario: a family assigns each trial its own ordinal.
    # Guarantees: every trial is one file, named by its own settings.
    def test_run_id_names_one_trial(self):
        self.assertEqual(
            RunSpec.build_run_id("http503", "buffered", "rustfs", (3,), 15, 1),
            "http503-buffered-rustfs-c1-w15-r001",
        )


class LedgerContracts(unittest.TestCase):
    """The acknowledgement ledger is the delivery oracle."""

    def ledger(self) -> Ledger:
        """A ledger in a directory that lives for one test."""
        ledger = Ledger(temporary_directory(self) / "ledger.sqlite")
        self.addCleanup(ledger.close)
        return ledger

    # Scenario: a retry rebuilds a request whose bytes differ from the first
    # attempt.
    # Guarantees: every later comparison is against the payload that was
    # actually sent, so a rebuilt payload fails here rather than silently.
    def test_rebuilt_request_must_be_byte_identical(self):
        ledger = self.ledger()
        rows = [("a:log", "log", "0" * 64)]
        _ = ledger.add_request(1, "logs", b"first", rows, send_ns=1)
        _ = ledger.add_request(1, "logs", b"first", rows, send_ns=2)
        with self.assertRaisesRegex(AssertionError, "different bytes"):
            _ = ledger.add_request(1, "logs", b"second", rows, send_ns=3)

    # Scenario: a request is acknowledged, retried after a duplicate
    # delivery and acknowledged again.
    # Guarantees: the acknowledgement time is the first successful response
    # and never moves.
    def test_acknowledgement_time_is_the_first_success(self):
        ledger = self.ledger()
        _ = ledger.add_request(1, "logs", b"x", [("a:log", "log", "0")], send_ns=10)
        ledger.ack(1, 100)
        ledger.ack(1, 200)
        self.assertEqual(
            [row for row in ledger.acked_ids()], [("a:log", "log", "0")]
        )
        self.assertEqual(ledger.acknowledgement_latencies_s(), [(100 - 10) / 1e9])

    # Scenario: attempts end as acknowledgements, retryable NACKs and
    # producer-local failures.
    # Guarantees: the three are counted separately, so a broken client cannot
    # be read as a healthy server.
    def test_outcomes_are_counted_separately(self):
        ledger = self.ledger()
        for index in range(3):
            _ = ledger.add_request(
                index, "logs", f"{index}".encode(), [(f"{index}:log", "log", "0")],
                send_ns=index,
            )
        ledger.attempt(0, 0, 0, 1, measurement.OUTCOME_RETRYABLE, "UNAVAILABLE")
        ledger.attempt(0, 1, 2, 3, measurement.OUTCOME_ACK)
        ledger.ack(0, 3)
        ledger.attempt(1, 0, 0, 1, measurement.OUTCOME_LOCAL, "local deadline")
        ledger.attempt(2, 0, 0, 1, measurement.OUTCOME_PARTIAL, "1 rejected")
        counts = ledger.counts()
        self.assertEqual(counts["requests_attempted_count"], 3)
        self.assertEqual(counts["requests_acked_count"], 1)
        self.assertEqual(counts["requests_outstanding_count"], 2)
        self.assertEqual(
            counts["attempts_by_outcome"],
            {
                measurement.OUTCOME_ACK: 1,
                measurement.OUTCOME_RETRYABLE: 1,
                measurement.OUTCOME_PERMANENT: 0,
                measurement.OUTCOME_PARTIAL: 1,
                measurement.OUTCOME_LOCAL: 1,
            },
        )

    # Scenario: an outcome the classifier does not know is recorded.
    # Guarantees: an unclassified status is an error, not a silent bucket.
    def test_unknown_outcome_is_refused(self):
        ledger = self.ledger()
        with self.assertRaisesRegex(ValueError, "unknown outcome"):
            ledger.attempt(1, 0, 0, 1, "mystery")

    # Scenario: the codes a producer must retry are read from the existing
    # end-to-end harness.
    # Guarantees: the measurement lane and the standing suite agree on what a
    # retryable NACK is, including CANCELLED and excluding ABORTED.
    def test_retryable_codes_come_from_the_existing_harness(self):
        import grpc

        codes = measurement.test_e2e.RETRYABLE_CODES
        self.assertIn(grpc.StatusCode.CANCELLED, codes)
        self.assertNotIn(grpc.StatusCode.ABORTED, codes)


class OracleContracts(unittest.TestCase):
    """Loss, duplication and corruption are counted in SQL, not in Python."""

    def loaded(self, expected, acked, actual):
        """A ledger holding `expected` records and `actual` reader rows."""
        ledger = Ledger(temporary_directory(self) / "ledger.sqlite")
        self.addCleanup(ledger.close)
        for request_id, rows in expected.items():
            _ = ledger.add_request(
                request_id, "logs", f"{request_id}".encode(), rows, send_ns=request_id
            )
            if request_id in acked:
                ledger.ack(request_id, request_id + 1)
        _ = measurement._load_actual(ledger, actual)
        return ledger

    # Scenario: one acknowledged record never reaches the lake while another
    # is stored twice.
    # Guarantees: loss and duplication are reported as separate counts, so a
    # duplicate cannot mask a loss in the row total.
    def test_loss_and_duplication_are_counted_apart(self):
        ledger = self.loaded(
            {0: [("a:log", "log", "h1"), ("b:log", "log", "h2")]},
            {0},
            [("a:log", "logs", "h1"), ("a:log", "logs", "h1")],
        )
        report = measurement._compare(ledger, require_all=True, healthy=True)
        self.assertEqual(report["missing_record_count"], 1)
        self.assertEqual(report["missing_sample"], ["b:log"])
        self.assertEqual(report["multiplicity_histogram"], {"0": 1, "2": 1})
        self.assertFalse(report["passed"])

    # Scenario: a record is stored under a payload the producer never sent.
    # Guarantees: a changed payload fails even when every id is present
    # exactly once.
    def test_changed_payload_fails(self):
        ledger = self.loaded(
            {0: [("a:log", "log", "h1")]}, {0}, [("a:log", "logs", "other")]
        )
        report = measurement._compare(ledger, require_all=True, healthy=True)
        self.assertEqual(report["corrupt_record_count"], 1)
        self.assertEqual(report["corrupt_sample"], ["a:log"])
        self.assertFalse(report["passed"])

    # Scenario: a run ends while one request is still outstanding.
    # Guarantees: only acknowledged records are required, and the outstanding
    # request's records are neither demanded nor counted as unexpected.
    def test_outstanding_requests_are_not_required(self):
        ledger = self.loaded(
            {0: [("a:log", "log", "h1")], 1: [("b:log", "log", "h2")]},
            {0},
            [("a:log", "logs", "h1")],
        )
        acked_only = measurement._compare(ledger, require_all=False, healthy=True)
        self.assertTrue(acked_only["passed"], acked_only["problems"])
        self.assertEqual(acked_only["expected_record_count"], 1)
        everything = measurement._compare(ledger, require_all=True, healthy=True)
        self.assertEqual(everything["missing_record_count"], 1)

    # Scenario: the lake holds a record the producer never sent.
    # Guarantees: an unexpected id fails and is named, rather than being
    # absorbed into a row count.
    def test_unexpected_record_fails(self):
        ledger = self.loaded(
            {0: [("a:log", "log", "h1")]},
            {0},
            [("a:log", "logs", "h1"), ("z:log", "logs", "h9")],
        )
        report = measurement._compare(ledger, require_all=True, healthy=True)
        self.assertEqual(report["unexpected_record_count"], 1)
        self.assertEqual(report["unexpected_sample"], ["z:log"])

    # Scenario: a fault run replays one record and loses none.
    # Guarantees: duplicates are allowed and counted rather than tolerated
    # silently, and the run still passes.
    def test_fault_run_counts_duplicates_without_failing(self):
        ledger = self.loaded(
            {0: [("a:log", "log", "h1")]},
            {0},
            [("a:log", "logs", "h1"), ("a:log", "logs", "h1")],
        )
        report = measurement._compare(ledger, require_all=True, healthy=False)
        self.assertTrue(report["passed"], report["problems"])
        self.assertEqual(report["multiplicity_histogram"], {"2": 1})


def telemetry(uptime, *, gauges=None, drop=(), workers=1, generation=0):
    """A JSON telemetry response for `workers` workers at one uptime.

    Built to the wire shape the engine actually publishes: a metric set per
    entity, identifying attributes as tagged values, and zeroes retained.
    """
    values = {name: 0 for name in measurement.REQUIRED_EXPORTER_GAUGES}
    # A worker that answered the collection reports its configuration-derived
    # budget, which is never zero; tests that model a worker which did not
    # answer override it back to zero.
    values[measurement.LIVENESS_GAUGE] = 1637851136
    values.update(gauges or {})
    sets = []
    for core in range(workers):
        identity = {
            "pipeline.group.id": {"String": "default"},
            "pipeline.id": {"String": "main"},
            "core.id": {"UInt": core},
            "deployment.generation": {"UInt": generation},
            "numa.node.id": {"UInt": 0},
        }
        sets.append(
            {
                "name": "pipeline",
                "attributes": identity,
                "metrics": [
                    {"name": "uptime", "value": uptime},
                    {"name": "memory.usage", "value": 5 * 1024 * 1024},
                ],
            }
        )
        sets.append(
            {
                "name": "exporter.series_parquet",
                "attributes": dict(
                    identity,
                    **{
                        "node.id": {"String": "exporter"},
                        "node.urn": {"String": "urn:otel:exporter:series_parquet"},
                    },
                ),
                "metrics": [
                    {"name": name, "value": value}
                    for name, value in values.items()
                    if name not in drop
                ],
            }
        )
    return {"timestamp": "2026-09-22T00:00:00Z", "metric_sets": sets}


def sample_of(document, *, workers=1):
    """The strict sample `sample_engine` would build from one response."""
    parsed = measurement.parse_telemetry(document, expected_workers=workers)
    built = {}
    for key, worker in parsed["workers"].items():
        built[key] = {
            "key": key,
            "core_id": worker["core_id"],
            "generation": worker["generation"],
            "uptime_s": measurement.require_gauge(worker, "pipeline", "uptime"),
            "gauges": {
                name: measurement.require_gauge(
                    worker, "exporter.series_parquet", name
                )
                for name in measurement.REQUIRED_EXPORTER_GAUGES
            },
        }
    return {"monotonic_ns": 0, "workers": built, "process": parsed["process"]}


class TelemetryContracts(unittest.TestCase):
    """A sample is an attributed observation or it is an error."""

    # Scenario: the exporter publishes no pending-requests gauge at all.
    # Guarantees: an absent required gauge is an error and is never read as
    # zero, which would look exactly like a drained worker.
    def test_absent_gauge_is_not_zero(self):
        document = telemetry(1.0, drop=("block.requests_pending",))
        with self.assertRaisesRegex(AssertionError, "block.requests_pending"):
            _ = sample_of(document)

    # Scenario: every required gauge is present and genuinely zero.
    # Guarantees: a real zero is an observation of an empty worker, which is
    # what the drain proof needs to be able to see.
    def test_zero_valued_required_gauge_is_an_observation(self):
        sample = sample_of(telemetry(1.0))
        worker = next(iter(sample["workers"].values()))
        self.assertEqual(worker["gauges"]["block.requests_pending"], 0)
        self.assertTrue(measurement.worker_is_empty(worker, buffered=False))

    # Scenario: two workers run on two cores and the caller expects one.
    # Guarantees: wrong worker cardinality invalidates the sample instead of
    # silently measuring one of them.
    def test_worker_cardinality_is_checked(self):
        with self.assertRaisesRegex(AssertionError, "expected 1 workers"):
            _ = sample_of(telemetry(1.0, workers=2), workers=1)

    # Scenario: the engine's own observability pipeline publishes a
    # `pipeline` metric set of its own.
    # Guarantees: the system group is not counted as a measured worker.
    def test_system_pipeline_is_not_a_worker(self):
        document = telemetry(1.0)
        document["metric_sets"].append(
            {
                "name": "pipeline",
                "attributes": {
                    "pipeline.group.id": {"String": "system"},
                    "pipeline.id": {"String": "observability"},
                    "core.id": {"UInt": 0},
                    "deployment.generation": {"UInt": 0},
                },
                "metrics": [{"name": "uptime", "value": 9.0}],
            }
        )
        self.assertEqual(len(sample_of(document)["workers"]), 1)

    # Scenario: two nodes of one worker publish a metric of the same name.
    # Guarantees: an ambiguously attributed value fails rather than being
    # picked arbitrarily or summed.
    def test_ambiguous_entity_attribution_fails(self):
        document = telemetry(1.0)
        duplicate = dict(document["metric_sets"][1])
        duplicate["attributes"] = dict(
            duplicate["attributes"], **{"node.id": {"String": "other"}}
        )
        document["metric_sets"].append(duplicate)
        with self.assertRaisesRegex(AssertionError, "cannot attribute"):
            _ = sample_of(document)

    # Scenario: a process-wide residual is published without a pipeline
    # identity.
    # Guarantees: process values stay out of the per-worker map and are never
    # summed across workers.
    def test_process_metrics_are_kept_apart(self):
        document = telemetry(1.0)
        document["metric_sets"].append(
            {
                "name": "exporter.series_parquet",
                "attributes": {},
                "metrics": [
                    {"name": "memory.unaccounted_rss_bytes", "value": 1234}
                ],
            }
        )
        sample = sample_of(document)
        self.assertEqual(
            sample["process"][
                "exporter.series_parquet.memory.unaccounted_rss_bytes"
            ],
            1234,
        )
        worker = next(iter(sample["workers"].values()))
        self.assertNotIn("memory.unaccounted_rss_bytes", worker["gauges"])


class BufferGaugeContracts(unittest.TestCase):
    """The buffer's per-signal gauges are one instance's value in parts."""

    def worker(self, labelled):
        """A worker carrying only labelled buffer metrics."""
        return {"key": "default/main/core1", "metrics": {}, "labelled": labelled}

    # Scenario: the buffer publishes `items.queued` once per signal for its
    # one instance on a worker.
    # Guarantees: the parts are added within that instance, so a queued
    # metrics item is not hidden by an empty logs queue.
    def test_signal_parts_of_one_instance_are_added(self):
        worker = self.worker(
            {
                "processor.durable_buffer.items:queued": {
                    "node.id=buffer": {
                        "signal=logs": 0, "signal=metrics": 2, "signal=traces": 0,
                    }
                }
            }
        )
        self.assertEqual(
            measurement.require_signal_gauge(
                worker, "processor.durable_buffer.items", "queued"
            ),
            2,
        )

    # Scenario: two buffer instances on one worker publish the gauge, or no
    # instance publishes it at all.
    # Guarantees: an ambiguous gauge and an absent gauge are errors, never a
    # sum across instances and never zero.
    def test_ambiguous_or_absent_signal_gauges_fail(self):
        worker = self.worker(
            {
                "processor.durable_buffer.items:queued": {
                    "node.id=one": {"signal=logs": 0},
                    "node.id=two": {"signal=logs": 0},
                }
            }
        )
        with self.assertRaisesRegex(AssertionError, "cannot attribute"):
            _ = measurement.require_signal_gauge(
                worker, "processor.durable_buffer.items", "queued"
            )
        with self.assertRaisesRegex(AssertionError, "publishes no"):
            _ = measurement.require_signal_gauge(
                self.worker({}), "processor.durable_buffer", "in.flight"
            )


class DrainContracts(unittest.TestCase):
    """Three collections, not three HTTP responses."""

    def responses(self, documents):
        """A `sample_once` callable that replays prepared responses."""
        queue = list(documents)

        def once():
            document = queue.pop(0) if queue else documents[-1]
            return sample_of(document)

        return once

    # Scenario: the admin API answers three times while the worker's uptime
    # gauge never advances.
    # Guarantees: repeated responses of one collection epoch add no
    # observation, so a fast poller cannot manufacture a drain proof.
    def test_three_unchanged_responses_do_not_drain(self):
        once = self.responses([telemetry(5.0)] * 3)
        with self.assertRaisesRegex(AssertionError, "drain deadline"):
            _ = measurement.observe_drain(
                once,
                expected_workers=1,
                buffered=False,
                deadline_ns=measurement.time.monotonic_ns() + int(0.4 * 10**9),
            )

    # Scenario: the worker's uptime advances three times with every required
    # gauge empty.
    # Guarantees: three empty collection epochs of every worker are what
    # proves drainage.
    def test_three_increasing_empty_epochs_drain(self):
        documents = [telemetry(float(index)) for index in range(1, 6)]
        report = measurement.observe_drain(
            self.responses(documents),
            expected_workers=1,
            buffered=False,
            deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
        )
        self.assertTrue(report["drained"])
        self.assertEqual(
            list(report["empty_epochs_by_worker"].values()),
            [measurement.DRAIN_EMPTY_EPOCHS],
        )

    # Scenario: a worker still holds a pending request at the second advance.
    # Guarantees: a nonempty observation resets that worker's streak, so the
    # three epochs must be consecutive.
    def test_a_nonempty_epoch_resets_the_streak(self):
        documents = [
            telemetry(1.0),
            telemetry(2.0),
            telemetry(3.0, gauges={"block.requests_pending": 1}),
            telemetry(4.0),
            telemetry(5.0),
        ]
        with self.assertRaisesRegex(AssertionError, "drain deadline"):
            _ = measurement.observe_drain(
                self.responses(documents),
                expected_workers=1,
                buffered=False,
                deadline_ns=measurement.time.monotonic_ns() + int(0.4 * 10**9),
            )

    # Scenario: the uptime advances three times, but the exporter did not
    # answer any of those collections and reports every gauge as zero.
    # Guarantees: a worker that did not report cannot prove its own
    # drainage, even though every emptiness gauge reads zero.
    def test_a_worker_that_did_not_report_cannot_drain(self):
        quiet = [
            telemetry(float(index), gauges={measurement.LIVENESS_GAUGE: 0})
            for index in range(1, 6)
        ]
        with self.assertRaisesRegex(AssertionError, "unanswered"):
            _ = measurement.observe_drain(
                self.responses(quiet),
                expected_workers=1,
                buffered=False,
                deadline_ns=measurement.time.monotonic_ns() + int(0.4 * 10**9),
            )

    # Scenario: a worker is empty, then does not answer one collection, then
    # is empty for two more.
    # Guarantees: an unanswered epoch breaks the run of empty epochs, so the
    # sequence empty, unanswered, empty, empty is two consecutive empty
    # epochs and not three, and busy state hidden behind an unanswered epoch
    # cannot prove drainage.
    def test_an_unanswered_epoch_breaks_the_empty_streak(self):
        quiet = {measurement.LIVENESS_GAUGE: 0}
        documents = [
            telemetry(1.0),
            telemetry(2.0),
            telemetry(3.0, gauges=quiet),
            telemetry(4.0),
            telemetry(5.0),
        ]
        with self.assertRaisesRegex(AssertionError, "drain deadline"):
            _ = measurement.observe_drain(
                self.responses(documents),
                expected_workers=1,
                buffered=False,
                deadline_ns=measurement.time.monotonic_ns() + int(0.4 * 10**9),
            )
        # One more answered empty epoch completes a genuine run of three.
        report = measurement.observe_drain(
            self.responses(documents + [telemetry(6.0)]),
            expected_workers=1,
            buffered=False,
            deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
        )
        self.assertEqual(
            list(report["unanswered_epochs_by_worker"].values()), [1]
        )

    # Scenario: a worker reports a lower uptime than it did a moment ago
    # within one deployment generation.
    # Guarantees: an uptime that goes backwards is an error rather than a new
    # epoch.
    def test_uptime_may_not_decrease_within_a_generation(self):
        tracker = measurement.EpochTracker()
        _ = tracker.observe(sample_of(telemetry(5.0)))
        with self.assertRaisesRegex(AssertionError, "uptime"):
            _ = tracker.observe(sample_of(telemetry(4.0)))

    # Scenario: the engine is restarted and the worker comes back in a new
    # deployment generation.
    # Guarantees: a restart is counted as a restart and not as an uptime
    # regression.
    def test_a_new_generation_is_a_restart(self):
        tracker = measurement.EpochTracker()
        _ = tracker.observe(sample_of(telemetry(5.0)))
        _ = tracker.observe(sample_of(telemetry(0.5, generation=1)))
        self.assertEqual(sum(tracker.restarts.values()), 1)


# prctl(PR_SET_NAME) names the calling thread, which is how the controller's
# worker threads get the `comm` the mapping reads.
PR_SET_NAME = 15


def name_this_thread(name):
    """Set the calling thread's kernel `comm`, as the engine's workers do."""
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_NAME, name.encode("ascii"), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_NAME) failed")


def run_on(core):
    """Spin on the calling thread until it has actually run on `core`."""
    libc = ctypes.CDLL(None, use_errno=True)
    deadline = measurement.time.monotonic_ns() + 5 * 10**9
    while libc.sched_getcpu() != core:
        if measurement.time.monotonic_ns() > deadline:
            raise AssertionError(f"the thread never ran on core {core}")


class WorkerThread:
    """A real thread of this process standing in for one engine worker.

    It names itself with the truncated worker `comm`, runs on `run_core` so
    that its last-run CPU is that core, then optionally widens its allowed
    set to `allowed` before blocking. The kernel therefore reports exactly
    the evidence an engine worker would: a name shared with every other
    worker, a last-run CPU, and an allowed-core list.
    """

    def __init__(self, case, *, run_core, allowed=None, core_name=None):
        self.ready = threading.Event()
        self.release = threading.Event()
        self.error = None
        self.tid = None
        name = measurement.worker_thread_name(
            "default", "main", core_name if core_name is not None else run_core, 0
        )[: measurement.COMM_WIDTH]

        def body():
            """Name, place and park the thread until the test releases it."""
            try:
                name_this_thread(name)
                os.sched_setaffinity(0, {run_core})
                run_on(run_core)
                if allowed is not None:
                    os.sched_setaffinity(0, set(allowed))
                self.tid = threading.get_native_id()
            except BaseException as error:
                self.error = error
            finally:
                self.ready.set()
            _ = self.release.wait()

        self.thread = threading.Thread(target=body, name="worker-stand-in")
        self.thread.start()
        case.addCleanup(self.stop)
        if not self.ready.wait(10) or self.error is not None:
            raise AssertionError(f"the stand-in worker did not start: {self.error}")

    def stop(self):
        """Release and join the thread."""
        self.release.set()
        self.thread.join(10)


def two_cores(case):
    """Two distinct cores this process may run on, or skip."""
    cores = sorted(os.sched_getaffinity(0))
    if len(cores) < 2:
        raise unittest.SkipTest("the worker mapping tests need two allowed cores")
    return cores[-2], cores[-1]


def worker_identity(core):
    """One expected worker identity as the telemetry reports it."""
    return {
        "key": f"default/main/core{core}",
        "group_id": "default",
        "pipeline_id": "main",
        "core_id": core,
        "generation": 0,
    }


class AffinityContracts(unittest.TestCase):
    """A worker whose affinity cannot be attributed is not a measurement."""

    def snapshot(self, workers, *, requested=None, role_cores=()):
        """Observe this process as the engine role, against `workers`."""
        return measurement.environment_snapshot(
            {"engine": (os.getpid(), list(role_cores))},
            workers=[worker_identity(core) for core in workers],
            requested_cores=requested,
        )

    # Scenario: two workers of the default group run on two cores, so the
    # kernel reports both threads as `pipeline-defaul`.
    # Guarantees: each worker is mapped to its own thread by the core it
    # runs on, not reported ambiguous because the truncated names collide.
    def test_workers_with_one_truncated_name_map_by_core(self):
        first, second = two_cores(self)
        one = WorkerThread(self, run_core=first)
        other = WorkerThread(self, run_core=second)
        snapshot = self.snapshot([first, second], requested=[first, second])
        self.assertEqual(snapshot["ambiguous_workers"], [])
        mapped = {thread["key"]: thread["tid"] for thread in snapshot["worker_threads"]}
        self.assertEqual(
            mapped,
            {f"default/main/core{first}": one.tid, f"default/main/core{second}": other.tid},
        )
        measurement.assert_affinity(snapshot)

    # Scenario: the engine role is allowed both requested cores, and one
    # worker thread is allowed both as well although it should own one.
    # Guarantees: each worker is compared with its own core, never with the
    # process-wide list, so the widened worker aborts the run.
    def test_each_worker_is_compared_with_its_own_core(self):
        first, second = two_cores(self)
        _ = WorkerThread(self, run_core=first, allowed=(first, second))
        _ = WorkerThread(self, run_core=second)
        snapshot = self.snapshot(
            [first, second], requested=[first, second], role_cores=[first, second]
        )
        self.assertEqual(snapshot["ambiguous_workers"], [])
        with self.assertRaisesRegex(AssertionError, "affinity mismatch"):
            measurement.assert_affinity(snapshot)

    # Scenario: the telemetry names a worker on a core where no thread with
    # the worker name ever ran.
    # Guarantees: a worker that cannot be attributed to exactly one thread
    # aborts the run instead of borrowing another worker's thread.
    def test_an_unattributable_worker_aborts(self):
        first, second = two_cores(self)
        _ = WorkerThread(self, run_core=second, core_name=first)
        snapshot = self.snapshot([first])
        self.assertTrue(snapshot["ambiguous_workers"])
        with self.assertRaisesRegex(AssertionError, "ambiguous"):
            measurement.assert_affinity(snapshot)

    # Scenario: the workers run on a core set other than the one requested.
    # Guarantees: a correctly pinned worker on the wrong core still aborts.
    def test_workers_on_unrequested_cores_abort(self):
        first, second = two_cores(self)
        _ = WorkerThread(self, run_core=first)
        snapshot = self.snapshot([first], requested=[second])
        with self.assertRaisesRegex(AssertionError, "requested"):
            measurement.assert_affinity(snapshot)

    # Scenario: a worker expected on core 4 can actually run on cores 4 and 5.
    # Guarantees: runtime pinning warnings cannot silently validate a measurement.
    def test_affinity_mismatch_aborts(self):
        with self.assertRaisesRegex(AssertionError, "affinity"):
            measurement.check_worker_affinity({123: {4, 5}}, {123: {4}})

    # Scenario: a mapped worker thread is gone, or a new one appeared, since
    # the mapping was made.
    # Guarantees: a changed TID set is a mapping mismatch, never a pass, and
    # an unchanged exact mapping passes.
    def test_a_changed_worker_mapping_aborts(self):
        with self.assertRaisesRegex(AssertionError, "TID mapping mismatch"):
            measurement.check_worker_affinity({124: {4}}, {123: {4}})
        with self.assertRaisesRegex(AssertionError, "tid=123 actual=None"):
            measurement.check_worker_affinity({123: None}, {123: {4}})
        measurement.check_worker_affinity({123: {4}, 124: {6}}, {123: {4}, 124: {6}})

    # Scenario: a run may use both SMT siblings of one core, one sibling of
    # another, and nothing of a third.
    # Guarantees: siblings of one core count once, and a core outside the
    # run's affinity does not count at all.
    def test_available_physical_cores_count_sibling_groups(self):
        groups = [[0, 16], [1, 17], [2, 18]]
        self.assertEqual(measurement.available_physical_cores(groups, [0, 16]), 1)
        self.assertEqual(
            measurement.available_physical_cores(groups, [0, 16, 17]), 2
        )
        self.assertEqual(measurement.available_physical_cores(groups, []), 0)
        self.assertEqual(
            measurement.available_physical_cores(groups, range(19)), 3
        )

    # Scenario: a kernel core list names a range and a single core.
    # Guarantees: an observed allowed set is compared as core ids rather than
    # as text.
    def test_core_lists_are_parsed(self):
        self.assertEqual(measurement.parse_core_list("0-3,8"), [0, 1, 2, 3, 8])

    # Scenario: this machine is inspected before a measurement.
    # Guarantees: the snapshot names every field the environment policy
    # requires, on every run.
    def test_snapshot_carries_every_required_field(self):
        snapshot = measurement.environment_snapshot({"harness": os.getpid()})
        for field in (
            "cpu_model",
            "logical_core_count",
            "physical_core_count",
            "ram_bytes",
            "kernel",
            "load_average_1_5_15",
            "thread_affinity",
        ):
            self.assertIn(field, snapshot)
        self.assertTrue(snapshot["thread_affinity"])

    # Scenario: a role has no process in this run.
    # Guarantees: an absent observation carries an explicit reason and is
    # never a zero.
    def test_absent_role_carries_a_reason(self):
        snapshot = measurement.environment_snapshot({"store": None})
        self.assertIn("store", snapshot["absent_roles"])

    # Scenario: the machine the run ended on is not the one it started on.
    # Guarantees: an unmatched start and end environment is visible in the
    # result rather than assumed away.
    def test_environment_mismatch_is_reported(self):
        start = measurement.environment_snapshot({})
        end = dict(start, ram_bytes=start["ram_bytes"] + 1)
        match = measurement.environment_match(start, end)
        self.assertFalse(match["matched"])
        self.assertIn("ram_bytes", match["differences"])


def blocked_child(case, code):
    """A child Python process that runs `code` and then blocks on stdin.

    Closing its stdin is what ends it, so no test waits on a fixed sleep and
    no child outlives the test that started it.
    """
    child = subprocess.Popen(
        [sys.executable, "-c", code + "\nimport sys\nsys.stdin.read()\n"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )

    def finish():
        """Close the child's stdin, then make sure it has exited."""
        if child.stdin and not child.stdin.closed:
            child.stdin.close()
        try:
            _ = child.wait(10)
        except subprocess.TimeoutExpired:
            child.kill()
            _ = child.wait(10)
        if child.stdout:
            child.stdout.close()

    case.addCleanup(finish)
    return child


def read_line(child, *, seconds=10):
    """One line of a child's stdout, under a monotonic deadline."""
    ready, _, _ = select.select([child.stdout], [], [], seconds)
    if not ready:
        raise AssertionError(f"child {child.pid} wrote nothing within {seconds}s")
    return child.stdout.readline().strip()


class BuildAndLeaseContracts(unittest.TestCase):
    """A measurement owns the machine and says so."""

    # Scenario: a compiler is running beside a measurement.
    # Guarantees: the run is invalidated and the evidence preserved, without
    # stopping anybody else's process.
    def test_a_concurrent_build_is_detected(self):
        monitor = measurement.BuildMonitor(interval_s=0.05)
        child = blocked_child(self, "print('ready', flush=True)")
        self.assertEqual(read_line(child), "ready")
        with mock.patch.object(
            measurement, "BUILD_COMMANDS", (Path(sys.executable).name,)
        ):
            found = monitor.scan()
        self.assertTrue(
            any(entry["pid"] == child.pid for entry in found),
            f"the monitor did not see pid {child.pid}",
        )
        self.assertEqual(child.poll(), None, "the monitor must not stop it")

    # Scenario: nothing is compiling.
    # Guarantees: a clean scan reports no detection, so the check is not one
    # that always fires.
    def test_a_quiet_machine_reports_no_build(self):
        monitor = measurement.BuildMonitor(interval_s=0.05)
        with mock.patch.object(measurement, "BUILD_COMMANDS", ("no-such-command",)):
            report = monitor.start().stop()
        self.assertFalse(report["detected"])
        self.assertGreater(report["scans"], 0)

    # Scenario: a second measurement starts while one holds the host lease.
    # Guarantees: the lease is exclusive for the whole run and the second
    # attempt is told who holds it.
    def test_the_host_lease_is_exclusive(self):
        path = temporary_directory(self) / "host.lease"
        first = measurement.HostLease(path).acquire()
        self.addCleanup(first.release)
        second = measurement.HostLease(path)
        with self.assertRaisesRegex(AssertionError, "another measurement holds"):
            _ = second.acquire(deadline_ns=measurement.time.monotonic_ns())
        first.assert_held()
        first.release()
        third = measurement.HostLease(path).acquire()
        self.addCleanup(third.release)
        third.assert_held()

    # Scenario: another process holds the lease and is then killed with
    # SIGKILL, so it runs no cleanup at all.
    # Guarantees: the kernel releases the lock with the process, so the
    # lease is available again without deleting anything, and it was
    # genuinely exclusive while the holder lived.
    def test_a_killed_holder_releases_the_lease(self):
        path = temporary_directory(self) / "host.lease"
        holder = blocked_child(
            self,
            "import fcntl, os\n"
            f"fd = os.open({str(path)!r}, os.O_CREAT | os.O_RDWR, 0o644)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\n"
            "print('locked', flush=True)",
        )
        self.assertEqual(read_line(holder), "locked")
        with self.assertRaisesRegex(AssertionError, "another measurement holds"):
            _ = measurement.HostLease(path).acquire(
                deadline_ns=measurement.time.monotonic_ns()
            )
        holder.kill()
        _ = holder.wait(10)
        lease = measurement.HostLease(path).acquire(
            deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9
        )
        self.addCleanup(lease.release)
        lease.assert_held()
        self.assertTrue(path.exists(), "the lease file is never removed")

    # Scenario: a crashed run left a lease file with a holder record behind
    # but holds no lock on it.
    # Guarantees: a leftover file is not a lease; only a held lock excludes.
    def test_a_leftover_lease_file_is_not_a_lease(self):
        path = temporary_directory(self) / "host.lease"
        _ = path.write_text(
            json.dumps({"owner": "gone", "pid": 2**22, "acquired_utc": "x"}),
            encoding="ascii",
        )
        lease = measurement.HostLease(path).acquire(
            deadline_ns=measurement.time.monotonic_ns()
        )
        self.addCleanup(lease.release)
        lease.assert_held()
        self.assertEqual(lease._read_holder()["owner"], lease.owner)

    # Scenario: the lease file is removed and recreated while a run holds
    # the lock on the original.
    # Guarantees: the run notices it no longer excludes anyone, because a
    # lock on a replaced file protects nothing.
    def test_a_replaced_lease_file_is_detected(self):
        path = temporary_directory(self) / "host.lease"
        lease = measurement.HostLease(path).acquire()
        self.addCleanup(lease.release)
        path.unlink()
        _ = path.write_text("{}\n", encoding="ascii")
        with self.assertRaisesRegex(AssertionError, "replaced"):
            lease.assert_held()


def passed_hard_checks(*, except_names=()):
    """Every required hard gate, passed, apart from the named ones."""
    return [
        measurement.check(name, measurement.CHECK_HARD, measurement.STATUS_PASSED)
        for name in measurement.REQUIRED_HARD_CHECKS
        if name not in except_names
    ]


MEASURED_METRICS = {
    "throughput_records_per_s": 1000.0,
    "ack_p99_s": 0.5,
    "peak_rss_bytes": 100.0,
}
MEASURED_DIRECTIONS = {
    "throughput_records_per_s": measurement.HIGHER_IS_BETTER,
    "ack_p99_s": measurement.LOWER_IS_BETTER,
    "peak_rss_bytes": measurement.LOWER_IS_BETTER,
}


def measured_result(**overrides) -> dict:
    """A minimal valid passed run carrying every fingerprint input."""
    snapshot = {
        "cpu_model": "Test CPU",
        "logical_core_count": 32,
        "physical_core_count": 16,
        "sibling_groups": [[core, core + 16] for core in range(16)],
        "available_cores": list(range(32)),
        "ram_bytes": 64 * 1024**3,
        "kernel": "Linux-test",
        "load_average_1_5_15": [0.1, 0.2, 0.3],
        "thread_affinity": [],
    }
    result = measurement.new_result(
        {"run_id": "t-strict-local-c1-w15-r001", "case": "t"}
    )
    result.update(
        {
            "status": measurement.STATUS_PASSED,
            "elapsed_s": 1.0,
            "run_dir": "/runs/t-001",
            "environment": {
                "start": dict(snapshot),
                "end": dict(snapshot),
                "machine_identity_sha256": "m",
                "core_allocation": {"engine": [0], "producer": [2, 3]},
                "build": {
                    "profile": "release",
                    "features": "series_parquet",
                    "allocator": "system",
                    "toolchain": "1.88",
                },
            },
            "config": {
                "requested": {},
                "effective": {
                    "window": {"interval": "15s"},
                    "storage": {"file": {"base_uri": "/runs/t-001/data"}},
                },
            },
            "workload": Workload().as_json(),
            "workload_schedule": {
                "duration_s": 30,
                "rate_requests_per_s": "closed_loop",
                "max_in_flight": 128,
            },
            "metrics": dict(MEASURED_METRICS),
            "metric_directions": dict(MEASURED_DIRECTIONS),
            "mandatory_metrics": ["throughput_records_per_s"],
            "checks": passed_hard_checks(),
        }
    )
    if "metrics" in overrides and "metric_directions" not in overrides:
        # A test that replaces the metrics keeps the declared direction of
        # each one it kept, so it tests what it names rather than tripping
        # over a stale direction for a metric it removed.
        overrides = dict(
            overrides,
            metric_directions={
                name: MEASURED_DIRECTIONS[name]
                for name in overrides["metrics"]
                if name in MEASURED_DIRECTIONS
            },
        )
    result.update(overrides)
    return result


class SchemaContracts(unittest.TestCase):
    """A result file is evidence or it is not written."""

    # Scenario: a result omits the end environment snapshot.
    # Guarantees: both snapshots are required on every run, whatever its
    # status.
    def test_both_environment_snapshots_are_required(self):
        result = measured_result()
        del result["environment"]["end"]
        with self.assertRaisesRegex(AssertionError, "environment.end"):
            measurement.validate_result(result)

    # Scenario: a metric is reported without a unit in its name.
    # Guarantees: a number whose meaning depends on the reader's memory is
    # refused.
    def test_metrics_must_state_their_unit(self):
        result = measured_result(metrics={"throughput": 1.0})
        with self.assertRaisesRegex(AssertionError, "does not state its unit"):
            measurement.validate_result(result)

    # Scenario: a measured value could not be obtained.
    # Guarantees: it is null with a reason rather than zero, and a passed run
    # may not be missing a mandatory metric.
    def test_unavailable_metrics_are_null_with_a_reason(self):
        result = measured_result(
            metrics={"throughput_records_per_s": None},
            metrics_unavailable={"throughput_records_per_s": "the producer failed"},
        )
        with self.assertRaisesRegex(AssertionError, "mandatory metrics"):
            measurement.validate_result(result)
        result["status"] = measurement.STATUS_FAILED
        measurement.validate_result(result)
        del result["metrics_unavailable"]["throughput_records_per_s"]
        with self.assertRaisesRegex(AssertionError, "null without a reason"):
            measurement.validate_result(result)

    # Scenario: a run passes while one of its hard checks failed.
    # Guarantees: a passed status and a failed check cannot coexist in one
    # file.
    def test_a_passed_run_carries_no_failed_check(self):
        result = measured_result()
        result["checks"][0]["status"] = measurement.STATUS_FAILED
        with self.assertRaisesRegex(AssertionError, "failed checks"):
            measurement.validate_result(result)

    # Scenario: a result is written while another reader could open the file.
    # Guarantees: the file is replaced atomically, is ASCII, sorts its keys
    # and leaves no temporary behind.
    def test_results_are_written_atomically_and_in_ascii(self):
        directory = temporary_directory(self)
        path = measurement.write_result(directory / "r.json", measured_result())
        text = path.read_text(encoding="ascii")
        self.assertEqual(json.loads(text)["case"], "t")
        self.assertTrue(text.endswith("\n"))
        self.assertEqual(sorted(directory.iterdir()), [path])

    # Scenario: a measured value is a non-finite float.
    # Guarantees: the file never carries a token no strict JSON reader
    # accepts.
    def test_non_finite_metrics_are_refused(self):
        result = measured_result(metrics={"throughput_records_per_s": float("inf")})
        with self.assertRaises(ValueError):
            _ = measurement.write_result(
                temporary_directory(self) / "r.json", result
            )


class BaselineContracts(unittest.TestCase):
    """The Controller baseline policy, and only it, decides."""

    # Scenario: a run's fingerprint matches no committed baseline.
    # Guarantees: a new fingerprint writes a candidate baseline instead of
    # comparing against one that does not describe it.
    def test_a_new_fingerprint_creates_a_baseline(self):
        result = measured_result()
        decision = measurement.evaluate_baseline(result, baselines={})
        self.assertEqual(decision["action"], "created")
        self.assertIn("baseline_candidate", result)
        self.assertEqual(
            result["baseline_candidate"]["fingerprint"], decision["fingerprint"]
        )

    # Scenario: the machine, its core topology, the cores available to the
    # run, the role placement, the configuration, the workload, its rate or
    # duration, or the build profile changed.
    # Guarantees: the fingerprint changes with each of them, so a run is
    # never compared against a different environment.
    def test_the_fingerprint_covers_every_declared_input(self):
        base = measurement.baseline_fingerprint(measured_result())
        for label, mutate in (
            ("ram", lambda r: r["environment"]["start"].update(ram_bytes=1)),
            (
                "siblings",
                lambda r: r["environment"]["start"].update(
                    sibling_groups=[[core] for core in range(32)]
                ),
            ),
            (
                "available cores",
                lambda r: r["environment"]["start"].update(
                    available_cores=list(range(8))
                ),
            ),
            (
                "placement",
                lambda r: r["environment"].update(
                    core_allocation={"engine": [7], "producer": [2, 3]}
                ),
            ),
            (
                "config",
                lambda r: r["config"]["effective"].update(
                    window={"interval": "1s"}
                ),
            ),
            (
                "path outside the run directory",
                lambda r: r["config"]["effective"].update(
                    storage={"file": {"base_uri": "/elsewhere/data"}}
                ),
            ),
            ("workload", lambda r: r.update(workload=Workload(seed=1).as_json())),
            (
                "rate",
                lambda r: r["workload_schedule"].update(rate_requests_per_s=500),
            ),
            ("duration", lambda r: r["workload_schedule"].update(duration_s=60)),
            (
                "allocator",
                lambda r: r["environment"]["build"].update(allocator="jemalloc"),
            ),
        ):
            with self.subTest(changed=label):
                result = measured_result()
                mutate(result)
                self.assertNotEqual(measurement.baseline_fingerprint(result), base)

    # Scenario: two runs differ only in their core sibling topology, one of
    # them on a machine without SMT.
    # Guarantees: they hash differently, so a measurement taken with SMT
    # siblings is never compared against one taken without.
    def test_sibling_topology_changes_the_fingerprint(self):
        smt = measured_result()
        flat = measured_result()
        flat["environment"]["start"]["sibling_groups"] = [
            [core] for core in range(32)
        ]
        self.assertNotEqual(
            measurement.baseline_fingerprint(smt),
            measurement.baseline_fingerprint(flat),
        )

    # Scenario: a fingerprint input was never recorded, or recorded as null.
    # Guarantees: the fingerprint refuses rather than hashing the absence,
    # which would let runs with unknown and different environments match.
    def test_a_missing_fingerprint_input_raises(self):
        for label, mutate in (
            (
                "siblings",
                lambda r: r["environment"]["start"].pop("sibling_groups"),
            ),
            (
                "machine identity",
                lambda r: r["environment"].update(machine_identity_sha256=None),
            ),
            ("placement", lambda r: r["environment"].pop("core_allocation")),
            ("toolchain", lambda r: r["environment"]["build"].pop("toolchain")),
            ("schedule rate", lambda r: r["workload_schedule"].pop("rate_requests_per_s")),
            ("run directory", lambda r: r.pop("run_dir")),
            ("effective config", lambda r: r["config"].update(effective={})),
            ("workload seed", lambda r: r["workload"].pop("seed")),
            (
                "every workload member but one",
                lambda r: r.update(workload={"requests": 100}),
            ),
        ):
            with self.subTest(missing=label):
                result = measured_result()
                mutate(result)
                with self.assertRaisesRegex(AssertionError, "fingerprint input"):
                    _ = measurement.baseline_fingerprint(result)

    # Scenario: a harness-supplied fingerprint input is present but null,
    # in the workload, the machine topology or the schedule.
    # Guarantees: a null at any depth of those inputs is refused rather than
    # hashed as JSON null, which would let two runs that each failed to
    # record the same input share a fingerprint.
    def test_a_nested_null_fingerprint_input_raises(self):
        for label, mutate in (
            ("workload seed", lambda r: r["workload"].update(seed=None)),
            (
                "environment build member",
                lambda r: r["environment"]["build"].update(features=None),
            ),
            (
                "nested sibling group member",
                lambda r: r["environment"]["start"]["sibling_groups"].append(
                    [None]
                ),
            ),
            (
                "schedule concurrency",
                lambda r: r["workload_schedule"].update(max_in_flight=None),
            ),
        ):
            with self.subTest(null=label):
                result = measured_result()
                mutate(result)
                with self.assertRaisesRegex(AssertionError, "fingerprint input"):
                    _ = measurement.baseline_fingerprint(result)

    # Scenario: the effective engine configuration sets the durable
    # buffer's `max_age` to an explicit null, as the plan requires.
    # Guarantees: an explicit null in the engine's own configuration is a
    # setting, so the run fingerprints successfully instead of being refused
    # like an unrecorded harness input.
    def test_an_explicit_null_engine_setting_fingerprints(self):
        result = measured_result()
        result["config"]["effective"]["durable_buffer"] = {"max_age": None}
        self.assertEqual(len(measurement.baseline_fingerprint(result)), 64)

    # Scenario: two runs differ only in whether `max_age` is an explicit null
    # or one hour.
    # Guarantees: the null is hashed as a real value, so the two settings
    # have different fingerprints and are never compared with each other.
    def test_an_explicit_null_setting_differs_from_a_value(self):
        unset = measured_result()
        unset["config"]["effective"]["durable_buffer"] = {"max_age": None}
        hourly = measured_result()
        hourly["config"]["effective"]["durable_buffer"] = {"max_age": "1h"}
        self.assertNotEqual(
            measurement.baseline_fingerprint(unset),
            measurement.baseline_fingerprint(hourly),
        )

    # Scenario: the same configuration runs twice, each in its own run
    # directory.
    # Guarantees: only paths under the run directory are canonicalized, so
    # the two runs share a fingerprint while a path elsewhere still counts.
    def test_only_run_directory_paths_are_canonicalized(self):
        first = measured_result()
        second = measured_result(run_dir="/runs/t-002")
        second["config"]["effective"]["storage"]["file"]["base_uri"] = (
            "/runs/t-002/data"
        )
        self.assertEqual(
            measurement.baseline_fingerprint(first),
            measurement.baseline_fingerprint(second),
        )
        self.assertEqual(
            measurement.canonicalize_run_paths(
                {"a": "/runs/t-001/x", "b": "/runs/t-0011/x", "c": ["/runs/t-001"]},
                "/runs/t-001",
            ),
            {"a": "<run_dir>/x", "b": "/runs/t-0011/x", "c": ["<run_dir>"]},
        )

    # Scenario: the source revision and binary changed but nothing else did.
    # Guarantees: provenance is recorded outside the fingerprint, so a new
    # implementation is compared rather than excused.
    def test_provenance_is_not_part_of_the_fingerprint(self):
        base = measurement.baseline_fingerprint(measured_result())
        result = measured_result()
        result["environment"]["git"] = {"revision": "deadbeef"}
        result["environment"]["build"]["binary_sha256"] = "f" * 64
        self.assertEqual(measurement.baseline_fingerprint(result), base)

    # Scenario: a later run on the same fingerprint is faster and smaller.
    # Guarantees: an improvement passes and the original baseline is left
    # untouched, so small regressions cannot ratchet it upward.
    def test_an_improvement_passes_and_does_not_ratchet(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        improved = measured_result(
            metrics={
                "throughput_records_per_s": 1200.0,
                "ack_p99_s": 0.4,
                "peak_rss_bytes": 90.0,
            }
        )
        decision = measurement.evaluate_baseline(
            improved, baselines={fingerprint: baseline}
        )
        self.assertEqual(decision["action"], "compared")
        self.assertEqual(decision["worse_than_limit"], [])
        self.assertEqual(baseline["metrics"]["throughput_records_per_s"], 1000.0)
        self.assertNotIn("baseline_candidate", improved)

    # Scenario: throughput falls by more than a quarter against a matching
    # baseline.
    # Guarantees: the configured boundary lives in the shared evaluator alone
    # and a regression worse than it fails.
    def test_a_regression_beyond_the_limit_fails(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        slower = measured_result(
            metrics={
                "throughput_records_per_s": 740.0,
                "ack_p99_s": 0.5,
                "peak_rss_bytes": 100.0,
            }
        )
        with self.assertRaisesRegex(AssertionError, "throughput_records_per_s"):
            _ = measurement.evaluate_baseline(
                slower, baselines={fingerprint: baseline}
            )
        decision = slower["baseline_decision"]
        self.assertAlmostEqual(
            decision["regressions"]["throughput_records_per_s"][
                "signed_regression"
            ],
            0.26,
        )

    # Scenario: a run regresses by less than the limit.
    # Guarantees: the boundary is a quarter, not any regression at all.
    def test_a_regression_within_the_limit_passes(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        slower = measured_result(
            metrics={
                "throughput_records_per_s": 800.0,
                "ack_p99_s": 0.6,
                "peak_rss_bytes": 120.0,
            }
        )
        decision = measurement.evaluate_baseline(
            slower, baselines={fingerprint: baseline}
        )
        self.assertEqual(decision["worse_than_limit"], [])

    # Scenario: a run lost an acknowledged record, or its accounted memory
    # cannot be reconciled with its resident set.
    # Guarantees: a hard gate fails on every run whatever the baseline says,
    # and such a run can never establish one.
    def test_a_hard_failure_cannot_establish_a_baseline(self):
        for name in ("delivery", "rss_reconciliation"):
            with self.subTest(check=name):
                result = measured_result()
                for entry in result["checks"]:
                    if entry["name"] == name:
                        entry["status"] = measurement.STATUS_FAILED
                with self.assertRaisesRegex(AssertionError, "hard gates failed"):
                    _ = measurement.evaluate_baseline(result, baselines={})
                self.assertNotIn("baseline_candidate", result)
                self.assertEqual(
                    result["baseline_decision"]["hard_gate_failures"], [name]
                )

    # Scenario: a failed run is evaluated against the policy.
    # Guarantees: a run that did not pass is neither compared nor turned into
    # a baseline.
    def test_a_failed_run_is_not_a_baseline_candidate(self):
        result = measured_result(status=measurement.STATUS_FAILED)
        with self.assertRaisesRegex(AssertionError, "cannot be"):
            _ = measurement.evaluate_baseline(result, baselines={})
        self.assertNotIn("baseline_candidate", result)

    # Scenario: a baseline records a metric this run did not report.
    # Guarantees: a run missing part of the baseline's schema is not
    # compared.
    def test_a_missing_metric_fails_the_schema(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        other = measured_result(
            metrics={"throughput_records_per_s": 1000.0},
            metric_directions={
                "throughput_records_per_s": measurement.HIGHER_IS_BETTER
            },
        )
        with self.assertRaisesRegex(AssertionError, "missing .*ack_p99_s"):
            _ = measurement.evaluate_baseline(
                other, baselines={fingerprint: baseline}
            )

    # Scenario: a run reports every baseline metric and one the baseline
    # never had.
    # Guarantees: the metric-name sets must be exactly equal, so an extra
    # metric fails rather than escaping comparison.
    def test_an_extra_metric_fails_the_schema(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        other = measured_result(
            metrics=dict(MEASURED_METRICS, backlog_ratio=0.1),
            metric_directions=dict(
                MEASURED_DIRECTIONS, backlog_ratio=measurement.LOWER_IS_BETTER
            ),
        )
        with self.assertRaisesRegex(AssertionError, "extra .*backlog_ratio"):
            _ = measurement.evaluate_baseline(
                other, baselines={fingerprint: baseline}
            )

    # Scenario: a run carries no hard checks at all, or only some of them.
    # Guarantees: an empty or incomplete hard-gate set is rejected, naming
    # every absent gate, and no baseline is built from it.
    def test_an_incomplete_hard_gate_set_is_rejected(self):
        for label, checks in (
            ("empty", []),
            ("no residual", passed_hard_checks(except_names=("rss_reconciliation",))),
            ("no correctness", passed_hard_checks(except_names=("delivery",))),
            (
                "no build monitor",
                passed_hard_checks(except_names=("no_concurrent_build",)),
            ),
        ):
            with self.subTest(gates=label):
                result = measured_result(checks=checks)
                with self.assertRaisesRegex(AssertionError, "absent"):
                    _ = measurement.evaluate_baseline(result, baselines={})
                self.assertNotIn("baseline_candidate", result)

    # Scenario: a baseline reference value is zero and the new run is not.
    # Guarantees: a zero reference permits no worsening unless a predeclared
    # resolution floor covers it.
    def test_a_zero_reference_permits_no_increase(self):
        lower = measurement.LOWER_IS_BETTER
        self.assertEqual(measurement.signed_regression(0, 0, lower), 0.0)
        self.assertEqual(measurement.signed_regression(3, 0, lower), float("inf"))
        self.assertEqual(measurement.signed_regression(3, 0, lower, floor=5), 0.0)

    # Scenario: a metric where a larger value is better falls, and one where
    # a smaller value is better rises.
    # Guarantees: the declared direction decides the sign of a regression.
    def test_regression_sign_follows_the_declared_direction(self):
        self.assertAlmostEqual(
            measurement.signed_regression(50.0, 100.0, measurement.HIGHER_IS_BETTER),
            0.5,
        )
        self.assertAlmostEqual(
            measurement.signed_regression(150.0, 100.0, measurement.LOWER_IS_BETTER),
            0.5,
        )

    # Scenario: a backlog ratio doubles against a matching baseline.
    # Guarantees: a ratio is not assumed to improve upwards; its declared
    # lower-is-better direction makes the rise a failing regression.
    def test_a_rising_backlog_ratio_is_a_regression(self):
        metrics = dict(MEASURED_METRICS, backlog_ratio=0.1)
        directions = dict(
            MEASURED_DIRECTIONS, backlog_ratio=measurement.LOWER_IS_BETTER
        )
        first = measured_result(metrics=metrics, metric_directions=directions)
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        worse = measured_result(
            metrics=dict(metrics, backlog_ratio=0.2), metric_directions=directions
        )
        with self.assertRaisesRegex(AssertionError, "backlog_ratio"):
            _ = measurement.evaluate_baseline(
                worse, baselines={fingerprint: baseline}
            )

    # Scenario: a run reports a metric without declaring its direction, or
    # declares a direction that is not one of the two.
    # Guarantees: a metric with no valid direction is an error, never a
    # guess from its name.
    def test_a_metric_without_a_direction_is_an_error(self):
        directions = dict(MEASURED_DIRECTIONS)
        del directions["ack_p99_s"]
        with self.assertRaisesRegex(AssertionError, "declare no direction"):
            _ = measurement.evaluate_baseline(
                measured_result(metric_directions=directions), baselines={}
            )
        with self.assertRaisesRegex(AssertionError, "unknown"):
            measurement.validate_result(
                measured_result(
                    metric_directions=dict(MEASURED_DIRECTIONS, ack_p99_s="sideways")
                )
            )

    # Scenario: a baseline and a later run declare opposite directions for
    # the same metric.
    # Guarantees: the comparison refuses rather than signing a regression by
    # whichever run happens to be newer.
    def test_a_changed_direction_is_refused(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        flipped = measured_result(
            metric_directions=dict(
                MEASURED_DIRECTIONS, ack_p99_s=measurement.HIGHER_IS_BETTER
            )
        )
        with self.assertRaisesRegex(AssertionError, "directions differ"):
            _ = measurement.evaluate_baseline(
                flipped, baselines={fingerprint: baseline}
            )


class PublicationContracts(unittest.TestCase):
    """Evidence is published and staged by exact name or not at all."""

    def tree(self):
        """An index naming one run file, one baseline and one child index."""
        directory = temporary_directory(self)
        run = directory / "t-strict-local-c1-w15-r001.json"
        _ = measurement.write_result(run, measured_result())
        baseline = directory / "baseline-t-0123456789abcdef.json"
        _ = baseline.write_text(
            json.dumps({"fingerprint": "0123456789abcdef", "metrics": {}}) + "\n",
            encoding="ascii",
        )
        child = directory / "child-index.json"
        child_document = measured_result(run_id="child-index", case="t")
        child_document["run_files"] = [measurement.file_entry(run)]
        _ = measurement.write_result(child, child_document)
        index = directory / "family.json"
        document = measured_result(run_id="family", case="t")
        document["run_files"] = [measurement.file_entry(run)]
        document["baseline_files"] = [measurement.file_entry(baseline)]
        document["child_indexes"] = [measurement.file_entry(child)]
        _ = measurement.write_result(index, document)
        return directory, index, {run.name, baseline.name, child.name, index.name}

    # Scenario: a complete family index is staged for a commit.
    # Guarantees: every enumerated index, child, run and baseline file is
    # handed to `git add` by its exact path, never by a glob or a directory.
    def test_every_enumerated_path_is_staged(self):
        directory, index, names = self.tree()
        with mock.patch.object(measurement.subprocess, "run") as run:
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            measurement.stage_run_files(index)
        staged = set()
        for call in run.call_args_list:
            arguments = call.args[0]
            self.assertEqual(arguments[:3], ["git", "add", "--"])
            staged.update(Path(item).name for item in arguments[3:])
            for item in arguments[3:]:
                self.assertTrue(Path(item).is_absolute(), item)
                self.assertEqual(Path(item).parent, directory.resolve())
        self.assertEqual(staged, names)

    # Scenario: an index records a hash that no longer matches its run file.
    # Guarantees: a tree whose evidence changed after it was summarised is
    # refused rather than staged.
    def test_changed_evidence_is_refused(self):
        directory, index, _ = self.tree()
        run = directory / "t-strict-local-c1-w15-r001.json"
        _ = run.write_text(run.read_text(encoding="ascii") + " \n", encoding="ascii")
        with self.assertRaisesRegex(AssertionError, "hashes to"):
            measurement.stage_run_files(index)

    # Scenario: an index names a path outside the report directory.
    # Guarantees: a path escape, a separator and a non-JSON name are all
    # rejected before anything is read or staged.
    def test_path_escape_is_rejected(self):
        for name in (
            "../secret.json",
            "nested/run.json",
            "run.txt",
            "..",
            "/etc/passwd",
        ):
            with self.subTest(name=name):
                with self.assertRaises(AssertionError):
                    _ = measurement.safe_json_name(name)

    # Scenario: an index names itself as its own child.
    # Guarantees: a cycle is an error rather than an endless walk or a
    # quietly truncated tree.
    def test_index_cycles_are_rejected(self):
        directory = temporary_directory(self)
        index = directory / "family.json"
        document = measured_result(run_id="family")
        document["child_indexes"] = [{"name": "family.json"}]
        _ = measurement.write_result(index, document)
        with self.assertRaisesRegex(AssertionError, "cycle"):
            _ = measurement.enumerate_tree(index)

    # Scenario: a re-execution publishes a run file name that already exists
    # with different content.
    # Guarantees: run and baseline file names are immutable, so evidence is
    # never overwritten.
    def test_publishing_over_different_evidence_fails(self):
        directory, index, _ = self.tree()
        report = temporary_directory(self) / "report"
        published = measurement.publish_result_tree(index, report)
        self.assertTrue(published.is_file())
        run = report / "t-strict-local-c1-w15-r001.json"
        _ = run.write_text("{}\n", encoding="ascii")
        with self.assertRaisesRegex(AssertionError, "immutable"):
            _ = measurement.publish_result_tree(index, report)

    # Scenario: the same complete tree is published twice unchanged.
    # Guarantees: republishing identical evidence is not a collision.
    def test_republishing_identical_evidence_is_allowed(self):
        _, index, names = self.tree()
        report = temporary_directory(self) / "report"
        _ = measurement.publish_result_tree(index, report)
        _ = measurement.publish_result_tree(index, report)
        self.assertEqual({path.name for path in report.iterdir()}, names)


    # Scenario: a family index is re-executed and its content changes.
    # Guarantees: it may advance only by enumerating the index it replaces as
    # an immutable child, so superseded evidence never vanishes.
    def test_an_index_advances_only_by_preserving_the_previous_one(self):
        directory, index, _ = self.tree()
        report = temporary_directory(self) / "report"
        _ = measurement.publish_result_tree(index, report)
        published_hash = measurement.file_digest(report / index.name)

        advanced = temporary_directory(self)
        document = measured_result(run_id="family", case="t")
        document["metrics"]["peak_rss_bytes"] = 101.0
        _ = measurement.write_result(advanced / index.name, document)
        with self.assertRaisesRegex(AssertionError, "immutable run-id-named child"):
            _ = measurement.publish_result_tree(advanced / index.name, report)

        child = measurement.archive_published_index(index.name, advanced, report)
        self.assertEqual(child["sha256"], published_hash)
        document["child_indexes"] = [child]
        _ = measurement.write_result(advanced / index.name, document)
        _ = measurement.publish_result_tree(advanced / index.name, report)
        self.assertIn(child["name"], {path.name for path in report.iterdir()})
        self.assertEqual(
            measurement.file_digest(report / child["name"]), published_hash
        )


class CommandContracts(unittest.TestCase):
    """The command line every later task's commands are written against."""

    # Scenario: a long measurement is asked for without opting in.
    # Guarantees: it refuses and explains how to opt in, rather than running
    # for half an hour by accident.
    def test_long_commands_require_an_opt_in(self):
        environment = dict(os.environ)
        environment.pop("SERIES_MEASURE_LONG", None)
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(measure.main(["soak"]), 2)

    # Scenario: the harness-local case is constructed.
    # Guarantees: its registered settings are a hundred requests of mixed
    # supported signals with one-second windows, and its spec validates.
    def test_harness_local_is_registered(self):
        spec = measure.harness_local_spec()
        self.assertEqual(spec.case, "harness-local")
        self.assertEqual(spec.workload.requests, 100)
        self.assertEqual(spec.interval_s, 1)
        self.assertEqual(spec.topology, "strict")
        signals = {spec.workload.signal_of(index) for index in range(100)}
        self.assertEqual(signals, {"logs", "metrics"})
        self.assertEqual(spec.run_id, "harness-local-strict-local-c1-w1-r001")

    # Scenario: a case name nobody registered is asked for.
    # Guarantees: the command names the cases it does have rather than
    # failing obscurely.
    def test_an_unknown_case_is_named(self):
        with self.assertRaisesRegex(SystemExit, "harness-contracts"):
            _ = measure.run_named("no-such-case", temporary_directory(self))

    def isolated(self):
        """Host controls confined to this test: a private lease, no builds."""
        lease = temporary_directory(self) / "host.lease"
        for patcher in (
            mock.patch.object(measurement, "BUILD_COMMANDS", ("no-such-command",)),
            mock.patch.object(measurement, "MINIMUM_PHYSICAL_CORES", 1),
        ):
            _ = patcher.start()
            self.addCleanup(patcher.stop)
        return lease

    def experiment(self, *, core, throughput=1000.0, end=True, requested=None):
        """A measured body that does everything a real one must.

        It places a stand-in worker thread on `core`, takes both snapshots
        through the controls, records a build, an effective configuration
        with a path under the run directory, metrics with declared
        directions, and the correctness, sample and residual checks.
        """
        case = self

        def body(_spec, result, output_dir, controls):
            """Run the lifecycle a real experiment runs."""
            worker = WorkerThread(case, run_core=core)
            try:
                measured(result, output_dir, controls)
            finally:
                # Like an engine that is shut down, the worker is gone once
                # the run ends, so the next run maps its own worker alone.
                worker.stop()

        def measured(result, output_dir, controls):
            """Everything a measured body records between its snapshots."""
            roles = {"engine": (os.getpid(), [core])}
            workers = [worker_identity(core)]
            wanted = requested if requested is not None else [core]
            _ = controls.snapshot(
                "start", roles, workers=workers, requested_cores=wanted
            )
            result["environment"]["build"] = {
                "profile": "debug",
                "features": "series_parquet",
                "allocator": "jemalloc",
                "toolchain": "rustc-test",
                "binary_sha256": "0" * 64,
            }
            result["config"]["effective"] = {
                "storage": {"file": {"base_uri": str(Path(output_dir) / "data")}},
                "window": {"interval": "1s"},
            }
            result["metrics"] = {
                "throughput_records_per_s": throughput,
                "peak_rss_bytes": 1000.0,
            }
            result["metric_directions"] = {
                "throughput_records_per_s": measurement.HIGHER_IS_BETTER,
                "peak_rss_bytes": measurement.LOWER_IS_BETTER,
            }
            result["mandatory_metrics"] = sorted(result["metrics"])
            for name in ("delivery", "minimum_samples", "rss_reconciliation"):
                result["checks"].append(
                    measurement.check(
                        name, measurement.CHECK_HARD, measurement.STATUS_PASSED
                    )
                )
            if end:
                _ = controls.snapshot(
                    "end", roles, workers=workers, requested_cores=wanted
                )
            result["status"] = measurement.STATUS_PASSED

        return body

    def run_measured(self, directory, report, body, lease, *, ordinal=1, **options):
        """One run_case call with this test's isolated controls."""
        return measure.run_case(
            measure.harness_local_spec(ordinal=ordinal),
            directory,
            experiment=body,
            report_dir=report,
            lease_path=lease,
            **options,
        )

    def published(self, report, *, ordinal=1):
        """The published result document of one harness-local run."""
        spec = measure.harness_local_spec(ordinal=ordinal)
        return json.loads((report / f"{spec.run_id}.json").read_text(encoding="ascii"))

    def checks_of(self, document):
        """The status of each named check in a result document."""
        return {entry["name"]: entry["status"] for entry in document["checks"]}

    # Scenario: a measured case is run with no measured body at all.
    # Guarantees: the run's JSON is still written and published, with both
    # environment snapshots and a failed status.
    def test_a_failed_run_still_writes_its_file(self):
        lease = self.isolated()
        directory = temporary_directory(self)
        report = temporary_directory(self) / "report"
        spec = measure.harness_local_spec()
        with self.assertRaises(NotImplementedError):
            _ = measure.run_case(spec, directory, report_dir=report, lease_path=lease)
        for location in (directory, report):
            document = json.loads(
                (location / f"{spec.run_id}.json").read_text(encoding="ascii")
            )
            self.assertEqual(document["status"], measurement.STATUS_FAILED)
            self.assertIn("start", document["environment"])
            self.assertIn("end", document["environment"])
            self.assertTrue(
                any(event["kind"] == "failed" for event in document["events"])
            )

    # Scenario: the first valid run of a fingerprint, then a second run of
    # the same fingerprint in its own run directory.
    # Guarantees: the first run writes its candidate baseline atomically,
    # references it by hash in `baseline_files` and publishes it; the second
    # run is compared against it and writes no new one.
    def test_the_first_valid_run_writes_a_baseline_and_the_next_compares(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        report = temporary_directory(self) / "report"
        first_dir = temporary_directory(self)
        first = self.run_measured(first_dir, report, self.experiment(core=core), lease)
        self.assertEqual(first["status"], measurement.STATUS_PASSED)
        self.assertEqual(first["baseline_decision"]["action"], "created")
        self.assertEqual(len(first["baseline_files"]), 1)
        entry = first["baseline_files"][0]
        self.assertEqual(entry["name"], first["baseline_decision"]["baseline_name"])
        for location in (first_dir, report):
            path = location / entry["name"]
            self.assertEqual(measurement.file_digest(path), entry["sha256"])
        self.assertFalse(list(first_dir.glob("*.tmp")), "no temporary file remains")
        baseline = json.loads((report / entry["name"]).read_text(encoding="ascii"))
        self.assertEqual(baseline["fingerprint"], first["baseline_decision"]["fingerprint"])
        self.assertEqual(
            self.checks_of(self.published(report)),
            dict.fromkeys(
                sorted(self.checks_of(self.published(report))), measurement.STATUS_PASSED
            ),
        )

        second = self.run_measured(
            temporary_directory(self), report, self.experiment(core=core), lease,
            ordinal=2,
        )
        self.assertEqual(second["baseline_decision"]["action"], "compared")
        self.assertEqual(second["baseline_files"], [])
        self.assertEqual(
            second["baseline_decision"]["fingerprint"],
            first["baseline_decision"]["fingerprint"],
            "only the run directory differs, and it is canonicalized",
        )

    # Scenario: a later run of the same fingerprint loses more than a
    # quarter of its throughput.
    # Guarantees: the regression fails the run, and the failed result is
    # still published with the decision that failed it.
    def test_a_regressed_run_fails_and_is_published(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        report = temporary_directory(self) / "report"
        _ = self.run_measured(temporary_directory(self), report, self.experiment(core=core), lease)
        with self.assertRaisesRegex(AssertionError, "throughput_records_per_s"):
            _ = self.run_measured(
                temporary_directory(self),
                report,
                self.experiment(core=core, throughput=700.0),
                lease,
                ordinal=2,
            )
        document = self.published(report, ordinal=2)
        self.assertEqual(document["status"], measurement.STATUS_FAILED)
        self.assertEqual(
            document["baseline_decision"]["worse_than_limit"],
            ["throughput_records_per_s"],
        )

    # Scenario: the experiment never takes its end snapshot.
    # Guarantees: the snapshot check fails, the run is failed, and no
    # baseline is written from it.
    def test_a_missing_end_snapshot_fails_the_run(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        report = temporary_directory(self) / "report"
        with self.assertRaisesRegex(AssertionError, "environment_snapshots_complete"):
            _ = self.run_measured(
                temporary_directory(self),
                report,
                self.experiment(core=core, end=False),
                lease,
            )
        document = self.published(report)
        self.assertEqual(document["status"], measurement.STATUS_FAILED)
        self.assertEqual(
            self.checks_of(document)["environment_snapshots_complete"],
            measurement.STATUS_FAILED,
        )
        self.assertEqual(document["baseline_files"], [])
        self.assertIn("fallback", document["environment"]["end"])

    # Scenario: the workers run on a core the run did not request.
    # Guarantees: the start snapshot aborts the run, and the published result
    # records the failed affinity check.
    def test_an_affinity_mismatch_aborts_the_run(self):
        lease = self.isolated()
        first, second = two_cores(self)
        report = temporary_directory(self) / "report"
        with self.assertRaisesRegex(AssertionError, "requested"):
            _ = self.run_measured(
                temporary_directory(self),
                report,
                self.experiment(core=first, requested=[second]),
                lease,
            )
        document = self.published(report)
        self.assertEqual(
            self.checks_of(document)["affinity_matched"], measurement.STATUS_FAILED
        )
        self.assertEqual(document["baseline_files"], [])

    # Scenario: a compiler starts while the run is measuring.
    # Guarantees: the build monitor invalidates the run and records what it
    # saw, and no baseline is written from it.
    def test_a_concurrent_build_invalidates_the_run(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        report = temporary_directory(self) / "report"
        builder = blocked_child(self, "print('ready', flush=True)")
        self.assertEqual(read_line(builder), "ready")
        with mock.patch.object(
            measurement, "BUILD_COMMANDS", (Path(sys.executable).name,)
        ):
            with self.assertRaisesRegex(AssertionError, "no_concurrent_build"):
                _ = self.run_measured(
                    temporary_directory(self),
                    report,
                    self.experiment(core=core),
                    lease,
                )
        document = self.published(report)
        self.assertEqual(
            self.checks_of(document)["no_concurrent_build"], measurement.STATUS_FAILED
        )
        self.assertTrue(document["environment"]["build_monitor"]["detected"])
        self.assertEqual(builder.poll(), None, "the monitor must not stop it")

    def restrict_affinity(self, cores):
        """Confine the test's main thread to `cores` for this test only."""
        before = os.sched_getaffinity(0)
        os.sched_setaffinity(0, set(cores))
        self.addCleanup(os.sched_setaffinity, 0, before)

    # Scenario: the machine has many physical cores, but the run's affinity
    # allows only one of them and two are required.
    # Guarantees: the gate counts the physical cores available to the run,
    # not the machine's, so the restricted run fails and writes no baseline.
    def test_a_run_restricted_below_the_core_floor_fails(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        self.restrict_affinity([core])
        report = temporary_directory(self) / "report"
        with mock.patch.object(measurement, "MINIMUM_PHYSICAL_CORES", 2):
            with self.assertRaisesRegex(AssertionError, "physical_cores_sufficient"):
                _ = self.run_measured(
                    temporary_directory(self), report, self.experiment(core=core), lease
                )
        document = self.published(report)
        self.assertEqual(
            self.checks_of(document)["physical_cores_sufficient"],
            measurement.STATUS_FAILED,
        )
        self.assertEqual(document["environment"]["start"]["available_physical_core_count"], 1)
        self.assertGreaterEqual(document["environment"]["start"]["physical_core_count"], 2)
        self.assertEqual(document["baseline_files"], [])

    # Scenario: the run may use both SMT siblings of one physical core, and
    # two physical cores are required.
    # Guarantees: two siblings of one core count as one, so the run fails.
    def test_two_smt_siblings_count_as_one_physical_core(self):
        allowed = set(os.sched_getaffinity(0))
        pair = next(
            (
                group
                for group in measurement.core_topology()["sibling_groups"]
                if len(group) >= 2 and set(group) <= allowed
            ),
            None,
        )
        if pair is None:
            raise unittest.SkipTest("no SMT sibling pair is available to this run")
        lease = self.isolated()
        self.restrict_affinity(pair)
        report = temporary_directory(self) / "report"
        with mock.patch.object(measurement, "MINIMUM_PHYSICAL_CORES", 2):
            with self.assertRaisesRegex(AssertionError, "physical_cores_sufficient"):
                _ = self.run_measured(
                    temporary_directory(self),
                    report,
                    self.experiment(core=pair[0]),
                    lease,
                )
        start = self.published(report)["environment"]["start"]
        self.assertEqual(start["available_cores"], sorted(pair))
        self.assertEqual(start["available_physical_core_count"], 1)

    # Scenario: the build monitor fails to start after the lease was taken.
    # Guarantees: the lease is released before the error propagates, the
    # failed run is still published, and the next run can take the lease.
    def test_a_monitor_that_fails_to_start_releases_the_lease(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        report = temporary_directory(self) / "report"
        with mock.patch.object(
            measurement.BuildMonitor, "start", side_effect=RuntimeError("no procfs")
        ):
            with self.assertRaisesRegex(RuntimeError, "no procfs"):
                _ = self.run_measured(
                    temporary_directory(self), report, self.experiment(core=core), lease
                )
        document = self.published(report)
        self.assertEqual(document["status"], measurement.STATUS_FAILED)
        again = measurement.HostLease(lease).acquire(
            deadline_ns=measurement.time.monotonic_ns()
        )
        self.addCleanup(again.release)
        again.assert_held()

    # Scenario: the build monitor fails to stop at the end of a run.
    # Guarantees: the lease is still released, the unobservable build
    # activity is a failed hard check rather than a skipped one, closing
    # again is a no-op, and the next run can take the lease.
    def test_a_monitor_that_fails_to_stop_still_releases_the_lease(self):
        lease = self.isolated()
        result = measurement.new_result({"run_id": "r", "case": "t"})
        controls = measurement.RunControls(result, lease_path=lease, lease_wait_s=0)
        _ = controls.open()

        def stop_monitor():
            """End the real monitor process the patched stop left running."""
            controls.monitor._abandon()

        self.addCleanup(stop_monitor)
        with mock.patch.object(
            controls.monitor, "stop", side_effect=AssertionError("monitor stuck")
        ):
            controls.close()
            controls.close()
        checks = {entry["name"]: entry for entry in result["checks"]}
        self.assertEqual(
            checks["no_concurrent_build"]["status"], measurement.STATUS_FAILED
        )
        self.assertIn("monitor stuck", checks["no_concurrent_build"]["detail"])
        self.assertEqual(
            [entry["name"] for entry in result["checks"]].count("no_concurrent_build"),
            1,
            "closing twice records the outcomes once",
        )
        self.assertFalse(controls.lease.held)
        again = measurement.HostLease(lease).acquire(
            deadline_ns=measurement.time.monotonic_ns()
        )
        self.addCleanup(again.release)
        again.assert_held()

    # Scenario: another measurement already holds the host lease.
    # Guarantees: the run does not start measuring, fails with the lease
    # check recorded, and still publishes its result.
    def test_a_held_lease_stops_the_run(self):
        lease = self.isolated()
        core = sorted(os.sched_getaffinity(0))[-1]
        holder = measurement.HostLease(lease).acquire()
        self.addCleanup(holder.release)
        report = temporary_directory(self) / "report"
        with self.assertRaisesRegex(AssertionError, "another measurement holds"):
            _ = self.run_measured(
                temporary_directory(self),
                report,
                self.experiment(core=core),
                lease,
                lease_wait_s=0,
            )
        document = self.published(report)
        self.assertEqual(
            self.checks_of(document)["host_lease_held"], measurement.STATUS_FAILED
        )


def fake_process(root, pid, comm, *, ppid=1, cmdline=""):
    """One injected procfs process: comm, stat with its parent, cmdline.

    The entry is assembled beside the tree and renamed into it, so the
    monitor process never reads half of it.
    """
    staging = Path(tempfile.mkdtemp(dir=root.parent))
    _ = (staging / "comm").write_text(comm + "\n", encoding="ascii")
    fields = ["S", str(ppid)] + ["0"] * 17 + ["4242"] + ["0"] * 20
    _ = (staging / "stat").write_text(
        f"{pid} ({comm}) " + " ".join(fields) + "\n", encoding="ascii"
    )
    _ = (staging / "cmdline").write_bytes(cmdline.replace(" ", "\0").encode("ascii"))
    staging.rename(root / str(pid))


def fake_proc(case):
    """An injected procfs tree holding only an init process."""
    root = temporary_directory(case) / "proc"
    root.mkdir()
    fake_process(root, 1, "init", ppid=0)
    return root


class InjectedProcContracts(unittest.TestCase):
    """Monitor decisions, driven by injected procfs observations."""

    rules = {
        "build_commands": list(measurement.BUILD_COMMANDS),
        "build_daemons": list(measurement.BUILD_DAEMONS),
        "container_clients": list(measurement.DOCKER_CLIENTS),
    }

    # Scenario: a builder container's buildkitd runs with no build step.
    # Guarantees: an idle build daemon does not invalidate a run, so a
    # machine that merely hosts a builder can still measure.
    def test_an_idle_build_daemon_is_not_a_build(self):
        root = fake_proc(self)
        fake_process(root, 50, "buildkitd")
        self.assertEqual(measurement.host_monitor.scan(root, self.rules, set()), [])

    # Scenario: the same daemon starts a build step.
    # Guarantees: a process descending from a build daemon is a build, with
    # its ancestry recorded as evidence.
    def test_a_build_step_under_a_daemon_is_a_build(self):
        root = fake_proc(self)
        fake_process(root, 50, "buildkitd")
        fake_process(root, 51, "runc", ppid=50)
        fake_process(root, 52, "sh", ppid=51)
        found = measurement.host_monitor.scan(root, self.rules, set())
        self.assertEqual(sorted(entry["pid"] for entry in found), [51, 52])
        self.assertEqual(found[1]["ancestry"][0], {"pid": 51, "comm": "runc"})

    # Scenario: a docker client runs `docker buildx build` with a secret
    # build argument, beside a docker client that only lists containers.
    # Guarantees: the build client is a build, the listing client is not,
    # and the secret's value never reaches the evidence.
    def test_container_build_clients_are_builds_and_secrets_are_redacted(self):
        root = fake_proc(self)
        fake_process(
            root, 60, "docker",
            cmdline="docker buildx build --build-arg API_TOKEN=hunter2 .",
        )
        fake_process(root, 61, "docker", cmdline="docker ps")
        found = measurement.host_monitor.scan(root, self.rules, set())
        self.assertEqual([entry["pid"] for entry in found], [60])
        self.assertNotIn("hunter2", found[0]["cmdline"])
        self.assertIn("API_TOKEN=<redacted>", found[0]["cmdline"])

    # Scenario: the monitor's ticks were once further apart than the
    # coverage limit.
    # Guarantees: a window the monitor could not see invalidates the run.
    def test_a_coverage_gap_fails_the_coverage_check(self):
        report = {
            "coverage": {"gaps_over_limit_count": 1, "limit_s": 0.1,
                         "max_gap_s": 0.9, "ticks": 10},
            "visibility": {"complete": True},
            "docker": {part: {"observable": True} for part in ("start", "end", "events")},
        }
        self.assertEqual(
            measurement.coverage_check(report)["status"], measurement.STATUS_FAILED
        )
        report["coverage"]["gaps_over_limit_count"] = 0
        self.assertEqual(
            measurement.coverage_check(report)["status"], measurement.STATUS_PASSED
        )

    # Scenario: Docker is installed but its daemon cannot be asked about
    # builder containers, or procfs hides other users' processes.
    # Guarantees: an inaccessible build namespace invalidates the run.
    def test_an_unobservable_build_namespace_fails_the_coverage_check(self):
        report = {
            "coverage": {"gaps_over_limit_count": 0, "limit_s": 0.1,
                         "max_gap_s": 0.05, "ticks": 10},
            "visibility": {"complete": True},
            "docker": {
                "start": {"observable": True},
                "end": {"observable": False, "reason": "permission denied"},
                "events": {"observable": True},
            },
        }
        self.assertIn(
            "docker end unobservable", measurement.coverage_check(report)["detail"]
        )
        report["docker"]["end"] = {"observable": True}
        report["visibility"] = {"complete": False, "hidepid": "2"}
        self.assertEqual(
            measurement.coverage_check(report)["status"], measurement.STATUS_FAILED
        )

    # Scenario: a compiler appears while the monitor process runs and is
    # gone again before the monitor stops.
    # Guarantees: the separate monitor process reports the build as soon as
    # it ticks, keeps it after the build has gone, and reports its own tick
    # coverage when it stops.
    def test_the_monitor_process_reports_a_transient_build(self):
        root = fake_proc(self)
        monitor = measurement.BuildMonitor(proc_root=root, docker=False).start()
        self.addCleanup(monitor._abandon)
        fake_process(root, 7100, "rustc")
        _ = measurement.wait_until(
            monitor.invalid.is_set,
            bool,
            deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
            description="the monitor process reporting the build",
        )
        shutil.rmtree(root / "7100")
        report = monitor.stop()
        self.assertTrue(report["detected"])
        self.assertEqual(report["observations"][0]["pid"], 7100)
        self.assertGreaterEqual(report["coverage"]["ticks"], 2)
        self.assertIsNotNone(report["coverage"]["max_gap_s"])


class ControlledRunContracts(unittest.TestCase):
    """Each host-control failure invalidates a run and publishes no baseline."""

    # The measured-body helpers the command contracts already define.
    isolated = CommandContracts.isolated
    experiment = CommandContracts.experiment
    published = CommandContracts.published
    checks_of = CommandContracts.checks_of

    def setUp(self):
        self.lease = self.isolated()
        self.core = sorted(os.sched_getaffinity(0))[-1]
        self.report = temporary_directory(self) / "report"

    def details_of(self, document):
        """The detail of each named check in a result document."""
        return {entry["name"]: entry["detail"] for entry in document["checks"]}

    def run_body(self, body, **options):
        """One run_case with this test's lease and report directory."""
        return measure.run_case(
            measure.harness_local_spec(),
            temporary_directory(self),
            experiment=body,
            report_dir=self.report,
            lease_path=self.lease,
            **options,
        )

    def document(self):
        """The published result of this test's run."""
        return self.published(self.report)

    # Scenario: a cargo build appears in procfs after the start snapshot,
    # while traffic would be flowing, and exits before the run ends.
    # Guarantees: the monitor sees it, the run stops itself cleanly, the
    # detection is kept although the build is gone, and no baseline is
    # written.
    def test_a_build_appearing_midway_invalidates_the_run(self):
        root = fake_proc(self)
        measured = self.experiment(core=self.core)

        def body(spec, result, output_dir, controls):
            """Measure, then meet a build partway through."""
            fake_process(root, 7000, "cargo", cmdline="cargo build --release")
            _ = measurement.wait_until(
                controls.monitor.invalid.is_set,
                bool,
                deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
                description="the monitor seeing the build",
            )
            shutil.rmtree(root / "7000")
            controls.raise_if_invalid()
            measured(spec, result, output_dir, controls)

        # The isolated controls ignore real compilers on this machine; the
        # injected tree is the only place this monitor may find one.
        with mock.patch.object(measurement, "BUILD_COMMANDS", ("cargo",)):
            with self.assertRaisesRegex(AssertionError, "invalidated while measuring"):
                _ = self.run_body(body, proc_root=root)
        document = self.document()
        checks = self.checks_of(document)
        self.assertEqual(checks["no_concurrent_build"], measurement.STATUS_FAILED)
        self.assertIn(
            7000,
            [entry["pid"] for entry in document["environment"]["build_monitor"]["observations"]],
        )
        self.assertEqual(document["baseline_files"], [])

    # Scenario: another run on this host already holds the measurement lease.
    # Guarantees: the second run aborts before its body sends any traffic,
    # names the holder's run, records the failed lease check and publishes
    # no baseline.
    def test_a_second_lease_holder_aborts_before_traffic(self):
        holder = measurement.MeasurementLease(self.lease, run_id="other-run").acquire()
        self.addCleanup(holder.release)
        started = []

        def body(*_arguments):
            """A body that must never run."""
            started.append(True)

        with self.assertRaisesRegex(AssertionError, "other-run"):
            _ = self.run_body(body)
        self.assertEqual(started, [])
        document = self.document()
        self.assertEqual(
            self.checks_of(document)["host_lease_held"],
            measurement.STATUS_FAILED,
        )
        self.assertEqual(document["baseline_files"], [])

    # Scenario: the core topology the run ends on differs from the one it
    # started on.
    # Guarantees: a changed CPU or core configuration fails the environment
    # match and cannot become a baseline.
    def test_a_changed_core_configuration_invalidates_the_run(self):
        measured = self.experiment(core=self.core)
        real = measurement.core_topology

        def changed():
            """The same machine reporting one sibling group fewer."""
            topology = real()
            topology["sibling_groups"] = topology["sibling_groups"][:-1]
            topology["physical_core_count"] -= 1
            return topology

        def body(spec, result, output_dir, controls):
            """Measure, with the end snapshot seeing another topology."""
            original = controls.snapshot

            def snapshot(edge, *arguments, **keywords):
                """Take the end snapshot under the changed topology."""
                if edge != "end":
                    return original(edge, *arguments, **keywords)
                with mock.patch.object(measurement, "core_topology", changed):
                    return original(edge, *arguments, **keywords)

            controls.snapshot = snapshot
            measured(spec, result, output_dir, controls)

        with self.assertRaisesRegex(AssertionError, "environment_matched"):
            _ = self.run_body(body)
        document = self.document()
        self.assertEqual(
            self.checks_of(document)["environment_matched"],
            measurement.STATUS_FAILED,
        )
        self.assertIn("sibling_groups", document["environment"]["match"]["differences"])
        self.assertEqual(document["baseline_files"], [])

    # Scenario: a pinned worker is widened to a second core after the start
    # snapshot, while the monitor watches it.
    # Guarantees: the monitor's own tick catches the widened worker, so a
    # pinning that holds only at the edges cannot validate a measurement.
    def test_a_worker_widened_midway_fails_affinity(self):
        first, second = two_cores(self)
        widen = threading.Event()
        widened = threading.Event()

        def worker_body():
            """A stand-in worker that widens itself when told."""
            name_this_thread(
                measurement.worker_thread_name("default", "main", first, 0)[
                    : measurement.COMM_WIDTH
                ]
            )
            os.sched_setaffinity(0, {first})
            run_on(first)
            state["tid"] = threading.get_native_id()
            ready.set()
            _ = widen.wait(10)
            os.sched_setaffinity(0, {first, second})
            widened.set()
            _ = release.wait(10)

        state = {}
        ready = threading.Event()
        release = threading.Event()
        thread = threading.Thread(target=worker_body)
        thread.start()
        self.addCleanup(thread.join, 10)
        self.addCleanup(release.set)
        self.assertTrue(ready.wait(10))

        def body(_spec, result, _output_dir, controls):
            """Map the worker, watch it, then widen it."""
            roles = {"engine": (os.getpid(), [first])}
            snapshot = controls.snapshot(
                "start", roles, workers=[worker_identity(first)],
                requested_cores=[first],
            )
            controls.watch_workers(os.getpid(), snapshot)
            widen.set()
            self.assertTrue(widened.wait(10))
            _ = measurement.wait_until(
                controls.monitor.invalid.is_set,
                bool,
                deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
                description="the monitor seeing the widened worker",
            )
            controls.raise_if_invalid()

        with self.assertRaisesRegex(AssertionError, "invalidated while measuring"):
            _ = self.run_body(body)
        document = self.document()
        self.assertEqual(
            self.checks_of(document)["affinity_matched"],
            measurement.STATUS_FAILED,
        )
        self.assertIn(
            f"tid={state['tid']}",
            self.details_of(document)["affinity_matched"],
        )
        self.assertEqual(document["baseline_files"], [])


class LauncherContracts(unittest.TestCase):
    """The topology and launcher options, checked before anything launches."""

    def config(self, **options):
        """One engine configuration with fixed ports and paths."""
        return measurement.test_e2e.engine_config(
            grpc_port=4317, data=Path("/runs/r/data"), **options
        )

    # Scenario: the buffered topology on one explicitly chosen core.
    # Guarantees: the buffer sits between receiver and exporter with the
    # measurement settings verbatim, including the explicit null max age,
    # and the worker is pinned by a core_set naming exactly that core.
    def test_the_buffered_graph_inserts_one_buffer(self):
        config = self.config(
            topology="buffered", buffer_path=Path("/runs/r/buffer"), cores=[3]
        )
        self.assertEqual(
            measurement.test_e2e.graph_edges(config),
            [("receiver", "buffer"), ("buffer", "exporter")],
        )
        nodes = config["groups"]["default"]["pipelines"]["main"]["nodes"]
        self.assertEqual(
            nodes["buffer"],
            {
                "type": "processor:durable_buffer",
                "config": {
                    "path": "/runs/r/buffer",
                    "retention_size_cap": "1GiB",
                    "size_cap_policy": "backpressure",
                    "max_age": None,
                    "otlp_handling": "pass_through",
                },
            },
        )
        self.assertEqual(
            config["policies"]["resources"]["core_allocation"],
            {"type": "core_set", "set": [{"start": 3, "end": 3}]},
        )

    # Scenario: a buffered engine is configured again for a restart, on a
    # new receiver port, with the same cores and buffer directory, and the
    # directory already holds retained data.
    # Guarantees: the restart keeps the graph, the core ids and the buffer
    # path, and building the configuration never touches retained data.
    def test_a_restart_keeps_cores_graph_and_buffer_path(self):
        retained = temporary_directory(self) / "buffer"
        (retained / "core_3").mkdir(parents=True)
        _ = (retained / "core_3" / "segment").write_bytes(b"retained")
        before = measurement.test_e2e.engine_config(
            grpc_port=4317, data=Path("/runs/r/data"), topology="buffered",
            buffer_path=retained, cores=[3],
        )
        after = measurement.test_e2e.engine_config(
            grpc_port=4318, data=Path("/runs/r/data"), topology="buffered",
            buffer_path=retained, cores=[3],
        )
        self.assertEqual(
            measurement.test_e2e.graph_edges(before),
            measurement.test_e2e.graph_edges(after),
        )
        for key in ("policies",):
            self.assertEqual(before[key], after[key])
        nodes = lambda config: config["groups"]["default"]["pipelines"]["main"]["nodes"]
        self.assertEqual(nodes(before)["buffer"], nodes(after)["buffer"])
        self.assertEqual((retained / "core_3" / "segment").read_bytes(), b"retained")

    # Scenario: a connection names two recipients, or asks to broadcast.
    # Guarantees: a request that could reach more than one instance is
    # refused before launch.
    def test_multiple_recipient_routing_is_refused(self):
        config = self.config()
        pipeline = config["groups"]["default"]["pipelines"]["main"]
        pipeline["connections"] = [{"from": "receiver", "to": ["exporter", "other"]}]
        with self.assertRaisesRegex(AssertionError, "2 recipients"):
            _ = measurement.test_e2e.graph_edges(config)
        pipeline["connections"] = [
            {"from": "receiver", "to": "exporter",
             "policies": {"dispatch": "broadcast"}}
        ]
        with self.assertRaisesRegex(AssertionError, "dispatch policy"):
            _ = measurement.test_e2e.graph_edges(config)

    # Scenario: the fixture tests build their configuration without any of
    # the new options.
    # Guarantees: legacy calls produce exactly the configuration they always
    # did.
    def test_legacy_configuration_is_unchanged(self):
        expected = measurement.test_e2e.yaml.safe_load(
            (measurement.test_e2e.WORKSPACE / "configs/series-parquet-local.yaml").read_text()
        )
        nodes = expected["groups"]["default"]["pipelines"]["main"]["nodes"]
        nodes["receiver"]["config"]["protocols"]["grpc"]["listening_addr"] = (
            "127.0.0.1:4317"
        )
        nodes["exporter"]["config"]["storage"] = {"file": {"base_uri": "/runs/r/data"}}
        nodes["exporter"]["config"]["window"]["interval"] = "1s"
        nodes["exporter"]["config"]["unsupported"] = "drop"
        self.assertEqual(self.config(overrides={"unsupported": "drop"}), expected)

    # Scenario: a measurement overrides one nested exporter setting.
    # Guarantees: the override is merged map by map, so the sibling settings
    # of the example configuration are kept.
    def test_measurement_overrides_are_deep_merged(self):
        config = self.config(merge={"exporter": {"window": {"interval": "2s"}}})
        window = config["groups"]["default"]["pipelines"]["main"]["nodes"][
            "exporter"
        ]["config"]["window"]
        self.assertEqual(window["interval"], "2s")
        self.assertEqual(window["max_block_bytes"], "500MiB")
        with self.assertRaisesRegex(ValueError, "names no node"):
            _ = self.config(merge={"nowhere": {}})

    # Scenario: the local launcher starts a process.
    # Guarantees: the PID it reports is the host PID of the process it
    # started, which every RSS, affinity and kill operation uses.
    def test_the_local_launcher_reports_the_host_pid(self):
        launcher = measurement.test_e2e.LocalLauncher()
        with open(os.devnull, "w", encoding="ascii") as log:
            process = launcher.start(
                [sys.executable, "-c", "pass"], log, dict(os.environ)
            )
        self.assertEqual(launcher.pid(process), process.pid)
        self.assertEqual(process.wait(10), 0)


class PlacementContracts(unittest.TestCase):
    """Roles on physical cores of their own."""

    groups = [[core, core + 16] for core in range(16)]

    # Scenario: a one-worker run on a sixteen-core SMT machine.
    # Guarantees: the observability core, the worker, the engine's reserved
    # cores and every other role each own a whole physical core, and no
    # role is given an SMT sibling of another role's core.
    def test_roles_own_whole_physical_cores(self):
        allocation = measurement.role_allocation(self.groups, range(32), [1])
        self.assertEqual(
            allocation,
            {
                "engine_observability": [0],
                "engine": [1],
                "engine_reserved": [2, 3, 4],
                "producer": [5, 6],
                "store": [7],
                "reader": [8],
            },
        )
        used = [core for cores in allocation.values() for core in cores]
        self.assertFalse(set(used) & {core + 16 for core in used})

    # Scenario: a worker is asked for on the observability core's sibling,
    # or the machine has too few physical cores.
    # Guarantees: a shared physical core is refused, and a strict placement
    # that cannot fit raises instead of doubling roles up.
    def test_shared_or_insufficient_cores_are_refused(self):
        with self.assertRaisesRegex(AssertionError, "shares physical core"):
            _ = measurement.role_allocation(self.groups, range(32), [16])
        with self.assertRaisesRegex(AssertionError, "no physical core is left"):
            _ = measurement.role_allocation(self.groups[:4], range(4), [1])
        loose = measurement.role_allocation(
            self.groups[:4], range(4), [1], strict=False
        )
        self.assertEqual(loose["engine"], [1])
        self.assertEqual(loose["reader"], [])


class ReconciliationContracts(unittest.TestCase):
    """The RSS residual is signed, measured term by term, and bounded."""

    def sample(self, *, rss, anonymous, heap, accounted, reported=True):
        """One sample carrying the memory terms the residual reads."""
        return {
            "monotonic_ns": 0,
            "procfs": {"smaps_rss_bytes": rss, "smaps_anonymous_bytes": anonymous},
            "workers": {
                "w": {
                    "pipeline_memory_usage_bytes": heap,
                    "gauges": {
                        "memory.accounted_bytes": accounted,
                        measurement.LIVENESS_GAUGE: 1 if reported else 0,
                    },
                }
            },
        }

    # Scenario: RSS grows by code pages faulted in and by heap that the
    # allocator keeps resident after it was freed.
    # Guarantees: both are measured terms, so neither is a residual; only
    # anonymous growth beyond the heap high-water mark remains.
    def test_file_pages_and_retained_heap_are_explained(self):
        MiB = 1024 * 1024
        idle = self.sample(rss=100 * MiB, anonymous=40 * MiB, heap=5 * MiB, accounted=MiB)
        peak = self.sample(rss=150 * MiB, anonymous=70 * MiB, heap=35 * MiB, accounted=20 * MiB)
        after = self.sample(rss=150 * MiB, anonymous=70 * MiB, heap=6 * MiB, accounted=MiB)
        residuals = measurement.rss_residuals([peak, after], idle)
        self.assertEqual([entry["residual_bytes"] for entry in residuals], [0, 0])
        self.assertEqual(residuals[0]["file_growth_bytes"], 20 * MiB)

    # Scenario: resident memory grows well beyond every measured term.
    # Guarantees: an unexplained positive residual above the frozen
    # tolerance fails, and a sample the workers did not answer is skipped.
    def test_an_unexplained_growth_fails(self):
        MiB = 1024 * 1024
        idle = self.sample(rss=100 * MiB, anonymous=40 * MiB, heap=5 * MiB, accounted=MiB)
        grown = self.sample(rss=160 * MiB, anonymous=100 * MiB, heap=5 * MiB, accounted=MiB)
        silent = self.sample(
            rss=100 * MiB, anonymous=40 * MiB, heap=0, accounted=0, reported=False
        )
        residuals = measurement.rss_residuals([grown, silent], idle)
        self.assertEqual(len(residuals), 1)
        self.assertEqual(
            measurement.residual_check(residuals, 160 * MiB)["status"],
            measurement.STATUS_FAILED,
        )

    # Scenario: accounted heap is reported that is never resident, in most
    # samples.
    # Guarantees: a persistent negative residual is kept and fails; it is
    # never clamped to zero.
    def test_a_persistent_negative_residual_fails(self):
        MiB = 1024 * 1024
        idle = self.sample(rss=100 * MiB, anonymous=40 * MiB, heap=5 * MiB, accounted=MiB)
        short = self.sample(rss=100 * MiB, anonymous=40 * MiB, heap=60 * MiB, accounted=MiB)
        residuals = measurement.rss_residuals([short, short, short], idle)
        self.assertLess(residuals[0]["residual_bytes"], 0)
        self.assertEqual(
            measurement.residual_check(residuals, 100 * MiB)["status"],
            measurement.STATUS_FAILED,
        )


class FingerprintEphemeralContracts(unittest.TestCase):
    """A declared per-run value is the one difference a fingerprint ignores."""

    # Scenario: two runs differ only in the receiver's ephemeral port, which
    # each declares; a third differs in an undeclared setting.
    # Guarantees: the declared port does not separate the fingerprints, and
    # an undeclared difference still does.
    def test_declared_ephemeral_values_are_canonicalized(self):
        def result_with(port, interval="15s"):
            """A result whose effective config names one listening port."""
            document = measured_result()
            document["config"]["effective"] = {
                "receiver": {"listening_addr": f"127.0.0.1:{port}"},
                "window": {"interval": interval},
            }
            document["ephemeral_values"] = {"<receiver_listening_addr>": f"127.0.0.1:{port}"}
            return document

        first = measurement.baseline_fingerprint(result_with(40001))
        self.assertEqual(first, measurement.baseline_fingerprint(result_with(40002)))
        self.assertNotEqual(
            first, measurement.baseline_fingerprint(result_with(40002, interval="1s"))
        )


def fake_task(root, pid, tid, name, cores):
    """One injected thread of an injected process, renamed into place."""
    task = root / str(pid) / "task"
    task.mkdir(exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=root.parent))
    _ = (staging / "status").write_text(
        f"Name:\t{name}\nPid:\t{tid}\nCpus_allowed_list:\t{cores}\n", encoding="ascii"
    )
    staging.rename(task / str(tid))


class WorkerEnumerationContracts(unittest.TestCase):
    """Every tick sees every worker thread, not only the mapped ones."""

    # Scenario: the engine's one mapped worker keeps its core, but a second
    # thread with the worker name exists for about one tick and is gone.
    # Guarantees: the monitor enumerates all of the engine's threads on
    # every tick, so the transient extra worker is a mapping failure that
    # invalidates the run even though it vanished again.
    def test_a_transient_extra_worker_fails_the_mapping(self):
        root = fake_proc(self)
        fake_process(root, 900, "df_engine")
        fake_task(root, 900, 900, "df_engine", "0-31")
        fake_task(root, 900, 901, "pipeline-defaul", "1")
        fake_task(root, 900, 902, "tokio-rt-worker", "1")
        monitor = measurement.BuildMonitor(proc_root=root, docker=False).start()
        self.addCleanup(monitor._abandon)
        monitor.watch(900, {901: {1}}, names=["pipeline-defaul"])
        fake_task(root, 900, 903, "pipeline-defaul", "2")
        _ = measurement.wait_until(
            monitor.invalid.is_set,
            bool,
            deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
            description="the monitor seeing the extra worker",
        )
        shutil.rmtree(root / "900" / "task" / "903")
        report = monitor.stop()
        self.assertGreaterEqual(report["affinity_failure_count"], 1)
        self.assertIn("TID mapping mismatch", report["affinity_failures"][0]["detail"])
        self.assertIn("903", report["affinity_failures"][0]["detail"])

    # Scenario: the mapped worker thread is replaced by a new thread with
    # the same name on the same core.
    # Guarantees: a replacing worker is a changed mapping, never a pass.
    def test_a_replaced_worker_fails_the_mapping(self):
        root = fake_proc(self)
        fake_process(root, 910, "df_engine")
        fake_task(root, 910, 911, "pipeline-defaul", "1")
        found = measurement.host_monitor.worker_threads(
            910, {"pipeline-defaul"}, proc_root=root
        )
        measurement.check_worker_affinity(found, {911: {1}})
        shutil.rmtree(root / "910" / "task" / "911")
        fake_task(root, 910, 912, "pipeline-defaul", "1")
        found = measurement.host_monitor.worker_threads(
            910, {"pipeline-defaul"}, proc_root=root
        )
        with self.assertRaisesRegex(AssertionError, "TID mapping mismatch"):
            measurement.check_worker_affinity(found, {911: {1}})


class ReleaseProfileContracts(unittest.TestCase):
    """A measured case runs the release engine or does not run."""

    def binary(self, profile):
        """A stand-in engine binary inside a target directory of `profile`."""
        directory = temporary_directory(self) / profile
        directory.mkdir()
        path = directory / "df_engine"
        _ = path.write_bytes(b"not really an engine")
        return path

    # Scenario: a measured case is pointed at a debug engine build.
    # Guarantees: it is refused before any lease or run, with an error
    # that says a release engine is required and how to build one.
    def test_a_debug_engine_is_refused(self):
        with mock.patch.dict(os.environ, {"DF_ENGINE": str(self.binary("debug"))}):
            with self.assertRaisesRegex(AssertionError, "require a release df_engine"):
                _ = measure.prepare_build()

    # Scenario: the same case is pointed at a release engine build.
    # Guarantees: the release profile is accepted and recorded.
    def test_a_release_engine_is_accepted(self):
        with mock.patch.dict(os.environ, {"DF_ENGINE": str(self.binary("release"))}):
            provenance = measure.prepare_build()
        self.assertEqual(provenance["build"]["profile"], "release")

    # Scenario: no DF_ENGINE is set.
    # Guarantees: the measured default is the release binary, never debug.
    def test_the_default_engine_is_the_release_build(self):
        environment = dict(os.environ)
        environment.pop("DF_ENGINE", None)
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(measure.engine_binary().parent.name, "release")


class CiIndexContracts(unittest.TestCase):
    """The non-publishable CI index still fails on every hard gate."""

    def child(self, directory, run_id, failed=()):
        """A child with every required gate recorded, `failed` ones failed."""
        names = measurement.REQUIRED_HARD_CHECKS + ("graph_edges",)
        child = {
            "run_id": run_id,
            "status": measurement.STATUS_FAILED,
            "checks": [
                measurement.check(
                    name,
                    measurement.CHECK_HARD,
                    measurement.STATUS_FAILED if name in failed else measurement.STATUS_PASSED,
                )
                for name in names
            ],
            "config": {"requested": {"topology": "strict"}},
            "metrics": {},
            "baseline_files": [],
        }
        _ = (directory / f"{run_id}.json").write_text("{}\n", encoding="ascii")
        return child

    # Scenario: a CI child passes every gate but the RSS reconciliation, and
    # another fails only the core floor of a small runner.
    # Guarantees: the RSS failure fails the CI index; the core floor alone,
    # which such a runner cannot meet, does not.
    def test_a_failed_hard_gate_fails_the_ci_index(self):
        directory = temporary_directory(self)
        small = self.child(directory, "small-host", failed=("physical_cores_sufficient",))
        self.assertEqual(measure.ci_failures(small), [])
        residual = self.child(
            directory, "residual", failed=("rss_reconciliation", "physical_cores_sufficient")
        )
        self.assertEqual(measure.ci_failures(residual), ["rss_reconciliation"])
        index = measure.write_index(
            "ci-test", directory, directory, [small, residual], publishable=False
        )
        self.assertEqual(index["status"], measurement.STATUS_FAILED)
        passing = measure.write_index(
            "ci-test-small", directory, directory, [small], publishable=False
        )
        self.assertEqual(passing["status"], measurement.STATUS_PASSED)

    # Scenario: a CI child never recorded its RSS reconciliation at all.
    # Guarantees: an absent required gate is a failure, never a pass.
    def test_an_absent_required_gate_fails(self):
        directory = temporary_directory(self)
        child = self.child(directory, "absent")
        child["checks"] = [
            entry for entry in child["checks"] if entry["name"] != "rss_reconciliation"
        ]
        self.assertEqual(measure.ci_failures(child), ["rss_reconciliation"])

    def without(self, name):
        """A child that never recorded the gate `name`."""
        child = self.child(temporary_directory(self), f"without-{name}")
        child["checks"] = [entry for entry in child["checks"] if entry["name"] != name]
        return child

    # Scenario: a CI child never recorded the host lease check.
    # Guarantees: the required set is the evaluator's own, so a missing
    # validity gate fails CI just as a missing correctness gate does.
    def test_a_missing_host_lease_check_fails(self):
        self.assertEqual(
            measure.ci_failures(self.without("host_lease_held")), ["host_lease_held"]
        )

    # Scenario: a CI child never recorded the core floor, the one gate a
    # non-publishable host is exempt from, and recorded every other gate.
    # Guarantees: the exemption covers exactly that gate and nothing else.
    def test_a_missing_core_floor_alone_passes(self):
        self.assertEqual(
            measure.ci_failures(self.without("physical_cores_sufficient")), []
        )
        self.assertEqual(
            set(measurement.REQUIRED_HARD_CHECKS) - set(measure.CI_REQUIRED_CHECKS),
            {"physical_cores_sufficient"},
        )


class AnsweredEpochContracts(unittest.TestCase):
    """A lifetime is sampled until it spans enough answered collections."""

    class GrowingSampler:
        """A sampler whose every read finds one more answered collection."""

        def __init__(self, limit=None):
            self.taken = []
            self.limit = limit

        @property
        def samples(self):
            """The samples so far, one more answered epoch per read."""
            if self.limit is None or len(self.taken) < self.limit:
                uptime = float(len(self.taken) + 1)
                self.taken.append(
                    {
                        "monotonic_ns": len(self.taken),
                        "workers": {
                            "w": {
                                "uptime_s": uptime,
                                "generation": 0,
                                "gauges": {measurement.LIVENESS_GAUGE: 1},
                            }
                        },
                    }
                )
            return list(self.taken)

    # Scenario: a lifetime has answered fewer than three collections when
    # its drain is proven, and more arrive one per observation.
    # Guarantees: sampling continues until exactly three answered epochs of
    # every worker are present, and stops there rather than later.
    def test_sampling_continues_until_three_answered_epochs(self):
        sampler = self.GrowingSampler()
        fewest = measure.await_answered_epochs(
            sampler, ["w"],
            deadline_ns=measurement.time.monotonic_ns() + 10 * 10**9,
        )
        self.assertEqual(fewest, measure.MINIMUM_EPOCHS)
        # One sample establishes the uptime, each later one is an epoch.
        self.assertEqual(len(sampler.taken), measure.MINIMUM_EPOCHS + 1)

    # Scenario: the collections stop after two answered epochs.
    # Guarantees: fewer than three is never accepted; the deadline fails.
    def test_fewer_than_three_answered_epochs_never_stop_sampling(self):
        sampler = self.GrowingSampler(limit=3)
        with self.assertRaisesRegex(AssertionError, "answered epochs"):
            _ = measure.await_answered_epochs(
                sampler, ["w"],
                deadline_ns=measurement.time.monotonic_ns() + int(0.3 * 10**9),
            )


def exporter_snapshot(metrics):
    """A JSON snapshot carrying one exporter metric set with `metrics`."""
    return {
        "timestamp": "2026-09-22T00:00:00Z",
        "metric_sets": [
            {
                "name": "exporter.series_parquet",
                "attributes": {"node.id": {"String": "exporter"}},
                "metrics": [
                    {"name": name, "value": value}
                    for name, value in metrics.items()
                ],
            }
        ],
    }


class SnapshotReadingContracts(unittest.TestCase):
    """An absent metric is not a zero-valued one.

    These guard `test_e2e.metric_max`, which the end-to-end suite asserts
    through. The old helper returned `max(values, default=0)`, so a snapshot
    that did not carry a metric was indistinguishable from one reporting
    zero: an upper-bound assertion passed vacuously and a lower-bound
    assertion failed with a message that named the wrong cause.
    """

    # Scenario: one snapshot omits the exporter metric set entirely and
    # another carries the same metric present at zero.
    # Guarantees: the two are distinguishable, which the old helper's
    # `default=0` made impossible.
    def test_absent_and_zero_are_distinguishable(self):
        absent = exporter_snapshot({"acks": 3})
        zero = exporter_snapshot({"memory.budget_bytes": 0})
        self.assertFalse(
            measurement.test_e2e.metric_present(absent, "memory.budget_bytes")
        )
        self.assertTrue(
            measurement.test_e2e.metric_present(zero, "memory.budget_bytes")
        )
        self.assertEqual(
            measurement.test_e2e.metric_values(absent, "memory.budget_bytes"), []
        )
        self.assertEqual(
            measurement.test_e2e.metric_values(zero, "memory.budget_bytes"), [0]
        )

    # Scenario: a caller that asserts on a value reads a metric the snapshot
    # does not carry.
    # Guarantees: it fails with the metric set and metric named, rather than
    # silently receiving a zero.
    def test_a_required_metric_must_be_present(self):
        absent = exporter_snapshot({"acks": 3})
        with self.assertRaisesRegex(AssertionError, "memory.budget_bytes"):
            _ = measurement.test_e2e.metric_max(absent, "memory.budget_bytes")
        self.assertEqual(
            measurement.test_e2e.metric_max(
                exporter_snapshot({"memory.budget_bytes": 0}),
                "memory.budget_bytes",
            ),
            0,
        )

    # Scenario: a poll is waiting for a metric to appear.
    # Guarantees: tolerating absence is explicit and yields the caller's own
    # default, so only callers that mean it get one.
    def test_tolerating_absence_is_explicit(self):
        absent = exporter_snapshot({"acks": 3})
        self.assertEqual(
            measurement.test_e2e.metric_max(
                absent, "block.requests_pending", default=0
            ),
            0,
        )

    # Scenario: an upper-bound assertion runs against a snapshot that does
    # not carry the metric.
    # Guarantees: it fails instead of passing vacuously, which is how the old
    # helper let a bound check succeed while measuring nothing.
    def test_an_upper_bound_cannot_pass_vacuously(self):
        absent = exporter_snapshot({"acks": 3})
        with self.assertRaises(AssertionError):
            self.assertLessEqual(
                measurement.test_e2e.metric_max(absent, "block.active_bytes"),
                8 << 20,
            )

    # Scenario: the exporter did not report in this collection, so its
    # configuration-derived budget reads zero.
    # Guarantees: a zero budget marks a snapshot the exporter did not answer,
    # because the budget is computed from configuration constants and is
    # never zero while the worker is alive.
    def test_zero_budget_marks_a_snapshot_the_exporter_did_not_answer(self):
        quiet = exporter_snapshot(
            {"memory.budget_bytes": 0, "memory.accounted_bytes": 0}
        )
        live = exporter_snapshot(
            {"memory.budget_bytes": 1637851136, "memory.accounted_bytes": 983168}
        )
        self.assertFalse(measurement.test_e2e.exporter_reported(quiet))
        self.assertTrue(measurement.test_e2e.exporter_reported(live))
        self.assertFalse(
            measurement.test_e2e.exporter_reported(exporter_snapshot({"acks": 3}))
        )


class HelperContracts(unittest.TestCase):
    """The shared helpers every later task's tests are written against."""

    # Scenario: a state the harness is waiting for never arrives.
    # Guarantees: the wait ends at a monotonic deadline and reports the last
    # observation, so elapsed time alone never proves readiness.
    def test_wait_until_reports_its_last_observation(self):
        observations = []

        def observe():
            observations.append(len(observations))
            return observations[-1]

        with self.assertRaisesRegex(AssertionError, "last=") as caught:
            _ = measurement.wait_until(
                observe,
                lambda value: False,
                deadline_ns=measurement.time.monotonic_ns() + int(0.2 * 10**9),
                description="a state that never arrives",
            )
        self.assertIn("a state that never arrives", str(caught.exception))
        self.assertTrue(observations)

    # Scenario: the awaited state is already true.
    # Guarantees: the observation that satisfied the predicate is returned,
    # not a later one.
    def test_wait_until_returns_the_accepted_observation(self):
        self.assertEqual(
            measurement.wait_until(
                lambda: 7,
                lambda value: value == 7,
                deadline_ns=measurement.time.monotonic_ns() + 10**9,
                description="seven",
            ),
            7,
        )

    # Scenario: a long measurement is reached without the opt-in variable.
    # Guarantees: the test skips with a reason that says how to run it.
    def test_require_long_skips_with_a_reason(self):
        environment = dict(os.environ)
        environment.pop("SERIES_MEASURE_LONG", None)
        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaisesRegex(unittest.SkipTest, "SERIES_MEASURE_LONG"):
                measurement.require_long()
        with mock.patch.dict(os.environ, {"SERIES_MEASURE_LONG": "1"}):
            measurement.require_long()

    # Scenario: a measurement test wants somewhere to keep its artifacts.
    # Guarantees: each test gets its own retained directory named after it.
    def test_measurement_test_case_retains_a_directory(self):
        root = temporary_directory(self)

        class Sample(measurement.MeasurementTestCase):
            """A test case that only records where it would write."""

            # Scenario: a measurement test case is set up by the runner.
            # Guarantees: its base class has prepared a retained output
            # directory before the test body runs.
            def test_records_its_output_directory(self):
                """Record the directory the base class prepared."""
                seen.append(self.output_dir)

        seen = []
        with mock.patch.dict(os.environ, {"SERIES_ARTIFACT_DIR": str(root)}):
            with open(os.devnull, "w", encoding="ascii") as sink:
                outcome = unittest.TextTestRunner(stream=sink).run(
                    unittest.TestLoader().loadTestsFromTestCase(Sample)
                )
        self.assertTrue(outcome.wasSuccessful())
        self.assertEqual(len(seen), 1)
        self.assertTrue(seen[0].is_dir())
        self.assertEqual(seen[0].parent, root)

    # Scenario: this process is sampled from procfs.
    # Guarantees: resident memory, CPU ticks and descriptor counts are
    # recorded on one monotonic timeline, and anything unreadable carries a
    # reason instead of a zero.
    def test_process_sample_is_attributed_or_explained(self):
        sample = measurement.procfs_process_sample(os.getpid())
        self.assertGreater(sample["rss_bytes"], 0)
        self.assertGreater(sample["monotonic_ns"], 0)
        for field, reason in (
            ("utime_ticks", "stat_reason"),
            ("open_fd_count", "fd_reason"),
        ):
            self.assertTrue(
                field in sample or reason in sample,
                f"{field} is neither observed nor explained",
            )

    # Scenario: a buffer directory holds a file smaller than one block.
    # Guarantees: allocated disk bytes are counted in blocks, so a sparse or
    # tiny file is not reported as its logical length.
    def test_directory_bytes_counts_allocated_blocks(self):
        directory = temporary_directory(self)
        _ = (directory / "segment").write_bytes(b"x")
        self.assertGreaterEqual(measurement.directory_bytes(directory), 512)
        self.assertEqual(measurement.directory_bytes(directory / "absent"), 0)

    # Scenario: a stable id is rendered for a record.
    # Guarantees: it is fixed width, so a reader can slice it out of a body
    # without parsing.
    def test_stable_ids_are_fixed_width(self):
        one = measurement.stable_id(1, 2, 3, "log")
        other = measurement.stable_id(2**31, 999999999999, 999999, "log")
        self.assertEqual(len(one), len(other))
        self.assertEqual(
            len(one), measurement.ID_FIXED_WIDTH + len(measurement.LOG_KIND)
        )


# The fields every environment snapshot must carry, with placeholder
# values: a synthetic child is read by the same validator as a real one.
SNAPSHOT_FIELDS = {
    "cpu_model": "cpu",
    "logical_core_count": 32,
    "physical_core_count": 16,
    "sibling_groups": [[0, 16]],
    "available_cores": [0, 1],
    "available_physical_core_count": 16,
    "ram_bytes": 1,
    "kernel": "linux",
    "load_average_1_5_15": [0.0, 0.0, 0.0],
    "thread_affinity": [],
}


def stage_child(stage="extract", profile="timing", repetition=1, **overrides):
    """One synthetic child measurement of the stage family."""
    metrics = {
        "timing": {
            "records_per_s_per_core": 1000000.0,
            "cpu_ns_per_record": 1000.0,
            "wall_ns_per_record": 1100.0,
            "peak_rss_bytes": 200 * 1024 * 1024,
            "output_bytes_per_input_record": 512.0,
        },
        "heap": {
            "allocated_bytes_per_record": 2048.0,
            "peak_live_heap_bytes": 90 * 1024 * 1024,
            "peak_workspace_bytes": 40 * 1024 * 1024,
        },
        "criterion": {"wall_ns_per_record": 1050.0},
        "pipeline": {
            "records_per_s_per_core": 500000.0,
            "cpu_ns_per_record": 2000.0,
            "wall_ns_per_record": 5000.0,
            "peak_rss_bytes": 300 * 1024 * 1024,
            "output_bytes_per_input_record": 0.0,
        },
        "pipeline_heap": {
            "allocated_bytes_per_record": 4096.0,
            "peak_live_heap_bytes": 120 * 1024 * 1024,
            "peak_workspace_bytes": 50 * 1024 * 1024,
        },
    }[profile]
    mode = "pipeline" if profile.startswith("pipeline") else performance.stage_mode(stage)
    child = {
        "run_id": f"stages-{stage}-{mode}-w-zstd-{profile}-r{repetition:03d}",
        "case": f"stages-{stage}-{profile}",
        "status": measurement.STATUS_PASSED,
        "stage": stage,
        "mode": mode,
        "profile": profile,
        "compression": "zstd",
        "workload_config_id": "w",
        "repetition": repetition,
        "family_ordinal": 1,
        "metrics": dict(metrics),
        "metric_directions": {
            name: performance.METRIC_DIRECTIONS[name] for name in metrics
        },
        "checks": [
            measurement.check(name, measurement.CHECK_HARD, measurement.STATUS_PASSED)
            for name in measurement.REQUIRED_HARD_CHECKS
        ],
        "samples": [{"index": 0}],
        "observations": {
            "bench_report": {
                "sample_count": 30,
                "resident": {
                    "start_bytes": 100 * 1024 * 1024,
                    "peak_bytes": 200 * 1024 * 1024,
                    "growth_bytes": 40 * 1024 * 1024,
                },
                "resident_steady": {
                    "start_bytes": 100 * 1024 * 1024,
                    "peak_bytes": 141 * 1024 * 1024,
                    "growth_bytes": 41 * 1024 * 1024,
                },
                "observation": {"representation": "extracted_rows"},
            },
            "engine_rss_growth_bytes": 50 * 1024 * 1024,
            "engine_peak_rss_bytes": 300 * 1024 * 1024,
        },
        "config": {"requested": {"cores": [1]}, "effective": {"lake": {}}},
        "workload": measurement.Workload().as_json(),
        "workload_schedule": {
            "duration_s": 1, "rate_requests_per_s": "closed_loop", "max_in_flight": 1,
        },
        "environment": {
            "start": dict(SNAPSHOT_FIELDS),
            "end": dict(SNAPSHOT_FIELDS),
            "build": {"profile": "bench"},
            "git": {"revision": "abc"},
            "core_allocation": {"bench": [1]},
        },
        "baseline_files": [],
    }
    child.update(overrides)
    return child


class StageContracts(unittest.TestCase):
    """The registered stage results every later attribution reads."""

    # Scenario: the registered OTLP-to-noop stage omits allocation data.
    # Guarantees: every stage, including the pipeline baseline, has the full
    # metric schema.
    def test_otlp_noop_requires_all_metrics(self):
        with self.assertRaisesRegex(AssertionError, "allocated_bytes_per_record"):
            performance.validate_stage_result({"stage": "otlp_noop", "metrics": {}})

    # Scenario: a stage result carries every metric but not the fields that
    # say which stage, mode, repetition and build it describes.
    # Guarantees: an unidentifiable measurement is refused, so a result can
    # never be compared against one of another profile or repetition.
    def test_identifying_fields_are_required(self):
        complete = {
            "metrics": {name: 1.0 for name in performance.STAGE_METRICS},
            "stage": "extract",
            "mode": "isolated",
            "sample_count": 30,
            "repetition": 1,
            "fingerprint": {"timing": "a"},
            "metric_sources": {name: "child" for name in performance.STAGE_METRICS},
        }
        performance.validate_stage_result(dict(complete))
        for field in performance.STAGE_FIELDS:
            partial = dict(complete)
            del partial[field]
            with self.subTest(field=field):
                with self.assertRaisesRegex(AssertionError, field):
                    performance.validate_stage_result(partial)

    # Scenario: a stage result reports a metric as a string, as infinity or
    # with a sample count of zero.
    # Guarantees: only finite numbers from a positive sample count are
    # accepted as measurements.
    def test_only_finite_measurements_from_samples_are_accepted(self):
        base = {
            "metrics": {name: 1.0 for name in performance.STAGE_METRICS},
            "stage": "encode",
            "mode": "isolated",
            "sample_count": 30,
            "repetition": 2,
            "fingerprint": {"timing": "a"},
            "metric_sources": {name: "child" for name in performance.STAGE_METRICS},
        }
        for value in ("1.0", float("inf"), float("nan"), True, None):
            broken = dict(base, metrics=dict(base["metrics"]))
            broken["metrics"]["cpu_ns_per_record"] = value
            with self.subTest(value=value):
                with self.assertRaisesRegex(AssertionError, "cpu_ns_per_record"):
                    performance.validate_stage_result(broken)
        with self.assertRaisesRegex(AssertionError, "samples"):
            performance.validate_stage_result(dict(base, sample_count=0))

    # Scenario: a stage result does not name the child that measured each of
    # its fields.
    # Guarantees: a composite identifies exactly which profile supplied
    # every metric, so a heap number can never be read as a timing one.
    def test_composite_names_the_child_of_every_metric(self):
        base = {
            "metrics": {name: 1.0 for name in performance.STAGE_METRICS},
            "stage": "merge",
            "mode": "isolated",
            "sample_count": 30,
            "repetition": 1,
            "fingerprint": {"timing": "a"},
            "metric_sources": {name: "child" for name in performance.STAGE_METRICS},
        }
        del base["metric_sources"]["peak_workspace_bytes"]
        with self.assertRaisesRegex(AssertionError, "peak_workspace_bytes"):
            performance.validate_stage_result(base)

    # Scenario: a heap child reports a throughput, and a timing child
    # reports an allocation.
    # Guarantees: each profile measures its own fields only; a heap child
    # never supplies throughput, and a missing profile field is incomplete
    # acceptance.
    def test_profiles_measure_their_own_fields(self):
        heap = stage_child(profile="heap")
        heap["metrics"]["records_per_s_per_core"] = 10.0
        with self.assertRaisesRegex(AssertionError, "records_per_s_per_core"):
            performance.validate_child_result(heap, "heap")
        timing = stage_child(profile="timing")
        del timing["metrics"]["cpu_ns_per_record"]
        with self.assertRaisesRegex(AssertionError, "cpu_ns_per_record"):
            performance.validate_child_result(timing, "timing")

    # Scenario: the exported stage names and Criterion layers are read back.
    # Guarantees: the names later tasks join on are exactly the registered
    # ones, in order, and the layers are the first six.
    def test_registered_stage_names(self):
        self.assertEqual(
            performance.STAGES,
            (
                "otlp_noop", "otlp_convert", "otlp_extract_hash", "otlp_sort",
                "otlp_parquet_local", "otlp_parquet_zstd", "otlp_minio",
                "convert", "extract", "sort_seal", "merge", "encode",
                "local_write", "upload", "sink",
            ),
        )
        self.assertEqual(performance.CRITERION_LAYERS, performance.STAGES[:6])
        self.assertEqual(performance.stage_mode("otlp_noop"), "criterion")
        self.assertEqual(performance.stage_mode("upload"), "async")
        self.assertEqual(performance.stage_mode("extract"), "isolated")

    # Scenario: every stage metric name is written into a result file.
    # Guarantees: the schema's names state their unit, so a later reader
    # never has to remember what a number meant.
    def test_stage_metrics_state_their_units(self):
        for name in performance.STAGE_METRICS:
            with self.subTest(name=name):
                self.assertTrue(name.endswith(measurement.UNIT_SUFFIXES), name)

    # Scenario: three repetitions of one profile are aggregated, one of them
    # three times slower than the others.
    # Guarantees: the dispersion of the repetitions is a hard stability
    # gate, and the aggregate publishes medians and ranges.
    def test_unstable_repetitions_fail_the_stage(self):
        directory = temporary_directory(self)
        children = [stage_child(repetition=index) for index in (1, 2, 3)]
        children[2]["metrics"]["cpu_ns_per_record"] = 3000.0
        for child in children:
            _ = measurement.write_json_atomic(
                directory / f"{child['run_id']}.json", child
            )
        reconciliation = performance.rss_reconciliation(
            children, [stage_child(profile="heap", repetition=index) for index in (1, 2, 3)]
        )
        aggregate = performance.aggregate_profile(
            children, plan={}, output_dir=directory, reconciliation=reconciliation
        )
        self.assertEqual(aggregate["status"], measurement.STATUS_FAILED)
        stability = [
            entry for entry in aggregate["checks"]
            if entry["name"] == "repetition_stability"
        ][0]
        self.assertEqual(stability["status"], measurement.STATUS_FAILED)
        self.assertIn("cpu_ns_per_record", stability["detail"])
        self.assertEqual(
            aggregate["observations"]["dispersion"]["cpu_ns_per_record"]["max"], 3000.0
        )

    # Scenario: the resident growth one steady-state iteration of a timing
    # child saw is far larger than the heap workspace its paired allocation
    # profile measured, and in another pair it is smaller.
    # Guarantees: unexplained resident growth is a hard failure of both
    # profiles, resident memory the fixtures already made resident is not,
    # and the pairing is by repetition.
    def test_unexplained_residual_fails_the_pair(self):
        timing = stage_child()
        timing["observations"]["bench_report"]["resident_steady"]["growth_bytes"] = (
            700 * 1024 * 1024
        )
        outcome = performance.rss_reconciliation([timing], [stage_child(profile="heap")])
        self.assertEqual(outcome["check"]["status"], measurement.STATUS_FAILED)
        self.assertFalse(outcome["residuals"][0]["within_tolerance"])
        within = performance.rss_reconciliation(
            [stage_child()], [stage_child(profile="heap")]
        )
        self.assertEqual(within["check"]["status"], measurement.STATUS_PASSED)
        reused = stage_child()
        reused["observations"]["bench_report"]["resident_steady"]["growth_bytes"] = 0
        explained = performance.rss_reconciliation(
            [reused], [stage_child(profile="heap")]
        )
        self.assertEqual(explained["check"]["status"], measurement.STATUS_PASSED)
        self.assertTrue(explained["residuals"][0]["explained_negative"])
        unpaired = performance.rss_reconciliation([stage_child(repetition=2)], [])
        self.assertEqual(unpaired["check"]["status"], measurement.STATUS_FAILED)

    # Scenario: a child of one profile never recorded a hard gate the
    # policy requires.
    # Guarantees: an absent gate is not a passed gate; the family's own gate
    # fails and names the child.
    def test_an_absent_child_gate_fails_the_family(self):
        child = stage_child()
        child["checks"] = [
            entry for entry in child["checks"] if entry["name"] != "affinity_matched"
        ]
        derived = {
            entry["name"]: entry for entry in performance.derived_checks([child])
        }
        self.assertEqual(
            derived["affinity_matched"]["status"], measurement.STATUS_FAILED
        )
        self.assertIn(child["run_id"], derived["affinity_matched"]["detail"])

    # Scenario: the timing and the heap children of one stage are built
    # differently, one with the system allocator and one with DHAT.
    # Guarantees: the two carry different baseline fingerprints, so a heap
    # profile is never compared against a timing baseline.
    def test_profiles_do_not_share_a_fingerprint(self):
        directory = temporary_directory(self)
        base = {
            "case": "stages-extract",
            "run_dir": str(directory),
            "environment": {
                "machine_identity_sha256": "m",
                "start": {
                    "cpu_model": "cpu", "logical_core_count": 32,
                    "physical_core_count": 16, "sibling_groups": [[0, 16]],
                    "ram_bytes": 1, "kernel": "linux",
                    "available_cores": [0, 1],
                },
                "core_allocation": {"bench": [1]},
                "build": {
                    "profile": "bench", "features": "default",
                    "allocator": "system", "toolchain": "rustc 1.88",
                },
            },
            "config": {"effective": {"lake": {}}},
            "workload": measurement.Workload().as_json(),
            "workload_schedule": {
                "duration_s": 1, "rate_requests_per_s": "closed_loop",
                "max_in_flight": 1,
            },
        }
        timing = measurement.baseline_fingerprint(base)
        heap = json.loads(json.dumps(base))
        heap["environment"]["build"].update(
            {"features": "bench-heap", "allocator": "dhat"}
        )
        self.assertNotEqual(timing, measurement.baseline_fingerprint(heap))

    # Scenario: a workload's requests are written as the bench input file.
    # Guarantees: the file is length-prefixed, its sidecar states the
    # records and series a stage must produce, and its hash covers it.
    def test_stage_input_is_length_prefixed_with_a_sidecar(self):
        directory = temporary_directory(self)
        workload = Workload(
            requests=6, records_per_request=3, body_bytes=64, series=2,
            metrics_every=3,
        )
        path = directory / "logs.otlp"
        sidecar = performance.write_stage_input(workload, "logs", path)
        self.assertEqual(sidecar["format"], performance.INPUT_FORMAT)
        self.assertEqual(sidecar["requests"], 4)
        self.assertEqual(sidecar["records"], 12)
        self.assertEqual(sidecar["expected_series"], 2)
        self.assertEqual(sidecar["sha256"], measurement.file_digest(path))
        data = path.read_bytes()
        at = 0
        seen = 0
        while at < len(data):
            (length,) = struct.unpack("<I", data[at:at + 4])
            at += 4
            _signal, wire, _rows = measurement.build_request(
                workload, sidecar["request_indexes"][seen]
            )
            self.assertEqual(data[at:at + length], wire)
            at += length
            seen += 1
        self.assertEqual(seen, sidecar["requests"])

    # Scenario: a fixture would build a row larger than the configured row
    # limit.
    # Guarantees: an illegal fixture is refused before it is measured, so it
    # can never claim supported throughput.
    def test_illegal_fixtures_are_refused(self):
        for config_id in performance.WORKLOAD_CONFIGS:
            with self.subTest(config_id=config_id):
                performance.check_fixture(config_id)
        with mock.patch.dict(
            performance.WORKLOAD_CONFIGS,
            {
                "too-wide": {
                    "signal": "logs",
                    "workload": Workload(body_bytes=4 * 1024 * 1024),
                    "lake": performance.lake_config(),
                    "committed_fraction": 0.0,
                    "stages": ("extract",),
                    "description": "an illegal fixture",
                }
            },
        ):
            with self.assertRaisesRegex(AssertionError, "row limit"):
                performance.check_fixture("too-wide")

    # Scenario: the only prebuilt executable that answers is a debug build
    # of the bench.
    # Guarantees: a debug bench is refused before it measures anything, and
    # the refusal names the command that builds a measured one.
    def test_a_debug_bench_is_refused(self):
        directory = temporary_directory(self)
        executable = directory / "measurement-deadbeef"
        _ = executable.write_text("")
        executable.chmod(0o755)
        description = {
            "bench": "measurement",
            "bench_heap": False,
            "allocator": "system",
            "debug_assertions": True,
        }
        with mock.patch.dict(
            os.environ, {"SERIES_STAGE_BENCH": str(executable)}, clear=False
        ):
            with mock.patch.object(
                performance, "describe_bench", return_value=description
            ):
                with self.assertRaisesRegex(AssertionError, "debug assertions"):
                    _ = performance.locate_benches()

    # Scenario: the bench executables have not been built.
    # Guarantees: the family refuses to start, names the cargo command that
    # builds what is missing, and runs no compiler of its own -- a build
    # beside a measurement is what invalidates it.
    def test_missing_benches_are_refused_without_building(self):
        directory = temporary_directory(self)
        calls = []

        def refuse(argv, *args, **kwargs):
            """Record any subprocess the discovery would have started."""
            calls.append(list(argv))
            raise AssertionError(f"discovery started {argv!r}")

        with mock.patch.object(performance.test_e2e, "WORKSPACE", directory):
            with mock.patch.object(performance.subprocess, "run", refuse):
                with self.assertRaisesRegex(
                    AssertionError, "cargo bench -p otel-arrow-dfe-series-lake"
                ):
                    _ = performance.locate_benches()
        self.assertEqual(calls, [])

    # Scenario: two prebuilt executables of the same bench exist, one with
    # DHAT's allocator and one without.
    # Guarantees: each profile is given the executable that identifies
    # itself as its build, never the one that merely sorts first.
    def test_each_profile_gets_its_own_build(self):
        directory = temporary_directory(self)
        deps = directory / "target/release/deps"
        deps.mkdir(parents=True)
        described = {}
        for name, heap in (("measurement-aaa", False), ("measurement-bbb", True),
                           ("layered-ccc", False)):
            path = deps / name
            _ = path.write_text("")
            path.chmod(0o755)
            described[str(path)] = {
                "bench": name.split("-")[0],
                "bench_heap": heap,
                "allocator": "dhat" if heap else "system",
                "debug_assertions": False,
            }
        with mock.patch.object(performance.test_e2e, "WORKSPACE", directory):
            with mock.patch.object(
                performance, "describe_bench", lambda path, **_: described[str(path)]
            ):
                timing = performance.locate_benches()
                heap = performance.locate_benches(features=("bench-heap",))
        self.assertTrue(timing["measurement"]["executable"].endswith("measurement-aaa"))
        self.assertTrue(timing["layered"]["executable"].endswith("layered-ccc"))
        self.assertTrue(heap["measurement"]["executable"].endswith("measurement-bbb"))

    # Scenario: the noop pipeline stores nothing, and its stage result says
    # so with a measured zero.
    # Guarantees: the zero is declared as a representation and an oracle
    # that does not apply, never as an unmeasured field.
    def test_noop_output_is_a_measured_zero(self):
        pipeline = [stage_child(stage="otlp_noop", profile="pipeline", repetition=r)
                    for r in (1, 2, 3)]
        heap = [stage_child(stage="otlp_noop", profile="pipeline_heap", repetition=r)
                for r in (1, 2, 3)]
        composites = performance.composite_stage_results([], pipeline + heap)
        self.assertEqual(len(composites), 3)
        for composite in composites:
            performance.validate_stage_result(composite)
            self.assertEqual(composite["metrics"]["output_bytes_per_input_record"], 0.0)
            self.assertEqual(composite["output_representation"], "none")
            self.assertIn("not_applicable", composite["descriptor_oracle"])
            self.assertEqual(
                composite["metric_sources"]["allocated_bytes_per_record"],
                heap[0]["run_id"].replace("r001", f"r{composite['repetition']:03d}"),
            )

    # Scenario: a composite is built from a timing child whose paired heap
    # child never ran.
    # Guarantees: completeness rejects it rather than filling the missing
    # allocation with a zero, and the rejection is carried into the index.
    def test_a_composite_without_its_heap_child_is_rejected(self):
        composites = performance.composite_stage_results([], [stage_child()])
        self.assertEqual(len(composites), 1)
        self.assertIn("allocated_bytes_per_record", composites[0]["incomplete"])
        self.assertNotIn("allocated_bytes_per_record", composites[0]["metrics"])
        with self.assertRaisesRegex(AssertionError, "allocated_bytes_per_record"):
            performance.validate_stage_result(composites[0])



if __name__ == "__main__":
    unittest.main()
