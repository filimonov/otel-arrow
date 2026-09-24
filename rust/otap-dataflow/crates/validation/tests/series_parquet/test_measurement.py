# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Fast contracts for the measurement harness.

Nothing here starts an engine, a container or a build. These are the rules
every later task's real measurement is read against: the ledger, the loss
oracle, the result schema, the collection-epoch rule, the baseline policy and
the publication and staging rules.
"""
import ctypes
import inspect
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
    from . import alloy_capacity
    from . import capacity
    from . import generator
    from . import measurement
    from . import measure
    from . import memory
    from . import performance
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import alloy_capacity
    import capacity
    import generator
    import measurement
    import measure
    import memory
    import performance

Ledger = measurement.Ledger
rates = capacity.rates
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
                    "features": "series-parquet",
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
            self.assertEqual(measure.main(["memory"]), 2)

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
                "features": "series-parquet",
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
        allocation = measurement.role_allocation(
            self.groups, range(32), [1], roles=measurement.CASE_ROLES["engine"]
        )
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
            _ = measurement.role_allocation(
                self.groups, range(32), [16], roles=measurement.CASE_ROLES["engine"]
            )
        with self.assertRaisesRegex(AssertionError, "no physical core is left"):
            _ = measurement.role_allocation(
                self.groups[:4], range(4), [1], roles=measurement.CASE_ROLES["engine"]
            )
        loose = measurement.role_allocation(
            self.groups[:4], range(4), [1], roles=measurement.CASE_ROLES["engine"],
            strict=False,
        )
        self.assertEqual(loose["engine"], [1])
        self.assertEqual(loose["reader"], [])


    # Scenario: the campaign's affinity, cores 0-7 with their SMT siblings
    # 16-23, is offered to a stages family and to a case that declares the
    # engine case's roles, reader included, with the full four-core engine
    # reservation.
    # Guarantees: a case claims cores only for the roles it declares, so
    # the stages set -- observability, the engine reservation, the producer
    # pair and the store -- fits in the eight physical cores exactly and
    # claims no reader; the reader-declaring case still does not fit and is
    # refused, never squeezed onto a shared core.
    def test_each_case_claims_only_its_own_roles(self):
        pinned = list(range(8)) + list(range(16, 24))
        stages = measurement.role_allocation(
            self.groups, pinned, [1], roles=measurement.CASE_ROLES["stages"]
        )
        self.assertEqual(
            stages,
            {
                "engine_observability": [0],
                "engine": [1],
                "engine_reserved": [2, 3, 4],
                "producer": [5, 6],
                "store": [7],
            },
        )
        self.assertNotIn("reader", stages)
        physical = {core % 16 for cores in stages.values() for core in cores}
        self.assertLessEqual(len(physical), measurement.MINIMUM_PHYSICAL_CORES)
        self.assertEqual(measurement.MINIMUM_PHYSICAL_CORES, 8)
        with self.assertRaisesRegex(AssertionError, "no physical core is left for reader"):
            _ = measurement.role_allocation(
                self.groups, pinned, [1], roles=measurement.CASE_ROLES["engine"]
            )


class HostNeighbourContracts(unittest.TestCase):
    """What else the host ran, and on which cores."""

    # Scenario: a fake process table holds a heavy neighbour pinned to the
    # upper cores, a light one, the family's own process and an entry with
    # nothing readable in it.
    # Guarantees: the record keeps the load average and the family's own
    # affinity, lists neighbours heaviest first with the cores each may
    # run on, and leaves out the family itself and unreadable entries.
    def test_neighbours_are_recorded_with_their_cores(self):
        proc = temporary_directory(self)
        _ = (proc / "loadavg").write_text("9.98 9.96 9.57 9/3854 3930739\n")
        def process(pid, comm, ticks, cores):
            """One fake /proc entry."""
            directory = proc / str(pid)
            directory.mkdir()
            fields = ["S"] + ["0"] * 10 + [str(ticks), "0"] + ["0"] * 30
            _ = (directory / "stat").write_text(f"{pid} ({comm}) " + " ".join(fields))
            _ = (directory / "status").write_text(
                f"Name:\t{comm}\nCpus_allowed_list:\t{cores}\n"
            )
        process(10, "java", 800_000, "8-15,24-31")
        process(11, "htop", 100, "0-31")
        process(12, "python3", 5_000_000, "0-7,16-23")
        (proc / "13").mkdir()
        record = performance.host_neighbours(proc, exclude=(12,), limit=5)
        self.assertEqual(record["load_average_1_5_15"], [9.98, 9.96, 9.57])
        self.assertEqual(record["family_affinity"], sorted(os.sched_getaffinity(0)))
        self.assertEqual(
            [(item["comm"], item["cpus_allowed"])
             for item in record["heaviest_other_processes"]],
            [("java", "8-15,24-31"), ("htop", "0-31")],
        )


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
                        "memory.accounted": accounted,
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

    # Scenario: beside the mapped worker, a second thread with the worker's
    # name is confined to the worker's own core, as a worker runtime's
    # blocking-pool thread is; then one is confined to another core.
    # Guarantees: the helper on the worker's core passes, a worker-named
    # thread anywhere else is a mapping mismatch naming it.
    def test_a_blocking_pool_thread_on_the_worker_core_passes(self):
        measurement.check_worker_affinity({911: {1}, 915: {1}}, {911: {1}})
        with self.assertRaisesRegex(AssertionError, r"\[915\] not confined"):
            measurement.check_worker_affinity({911: {1}, 915: {2}}, {911: {1}})

    # Scenario: an end snapshot finds two threads with the worker's name
    # that last ran on its core, with and without the start mapping.
    # Guarantees: the start snapshot's TID is the worker and the other is
    # recorded as a same-named helper; without it the mapping is ambiguous.
    def test_a_later_snapshot_keeps_the_start_worker(self):
        worker = {"key": "default/main/core1", "group_id": "default",
                  "pipeline_id": "main", "core_id": 1, "generation": 0}
        comm = measurement.worker_thread_name("default", "main", 1, 0)[
            :measurement.COMM_WIDTH]
        threads = [
            {"tid": 911, "name": comm, "last_cpu": 1, "cpus_allowed_list": "1"},
            {"tid": 915, "name": comm, "last_cpu": 1, "cpus_allowed_list": "1"},
        ]
        selected, ambiguous = measurement.select_worker_threads(
            threads, [worker], previous={worker["key"]: 911}
        )
        self.assertEqual(ambiguous, [])
        self.assertEqual(selected[worker["key"]]["tid"], 911)
        self.assertEqual(selected[worker["key"]]["same_named_on_core_tids"], [915])
        _selected, ambiguous = measurement.select_worker_threads(threads, [worker])
        self.assertEqual(len(ambiguous), 1)


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
        zero = exporter_snapshot({"memory.budget": 0})
        self.assertFalse(
            measurement.test_e2e.metric_present(absent, "memory.budget")
        )
        self.assertTrue(
            measurement.test_e2e.metric_present(zero, "memory.budget")
        )
        self.assertEqual(
            measurement.test_e2e.metric_values(absent, "memory.budget"), []
        )
        self.assertEqual(
            measurement.test_e2e.metric_values(zero, "memory.budget"), [0]
        )

    # Scenario: a caller that asserts on a value reads a metric the snapshot
    # does not carry.
    # Guarantees: it fails with the metric set and metric named, rather than
    # silently receiving a zero.
    def test_a_required_metric_must_be_present(self):
        absent = exporter_snapshot({"acks": 3})
        with self.assertRaisesRegex(AssertionError, "memory.budget"):
            _ = measurement.test_e2e.metric_max(absent, "memory.budget")
        self.assertEqual(
            measurement.test_e2e.metric_max(
                exporter_snapshot({"memory.budget": 0}),
                "memory.budget",
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
                measurement.test_e2e.metric_max(absent, "block.active"),
                8 << 20,
            )

    # Scenario: the exporter did not report in this collection, so its
    # configuration-derived budget reads zero.
    # Guarantees: a zero budget marks a snapshot the exporter did not answer,
    # because the budget is computed from configuration constants and is
    # never zero while the worker is alive.
    def test_zero_budget_marks_a_snapshot_the_exporter_did_not_answer(self):
        quiet = exporter_snapshot(
            {"memory.budget": 0, "memory.accounted": 0}
        )
        live = exporter_snapshot(
            {"memory.budget": 1637851136, "memory.accounted": 983168}
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

    def complete_stage_result(self, stage="extract", **overrides):
        """A stage result that satisfies the whole contract."""
        result = {
            "metrics": {name: 1.0 for name in performance.STAGE_METRICS},
            "stage": stage,
            "mode": performance.stage_mode(stage),
            "sample_count": 30,
            "repetition": 1,
            "fingerprint": {"timing": "a"},
            "metric_sources": {name: "child" for name in performance.STAGE_METRICS},
            "input_representation": performance.INPUT_REPRESENTATIONS[stage],
            "output_representation": "extracted_rows",
            "denominator": performance.DENOMINATOR,
            "rates": {name: 1.0 for name in performance.RATE_FIELDS},
            "completion_semantics": performance.COMPLETION_SEMANTICS,
        }
        result.update(overrides)
        return result

    # Scenario: a stage result does not say what it was handed, or what its
    # per-record numbers divide by, and a stage that times already encoded
    # input reports only a per-record rate.
    # Guarantees: the denominator of every registered number is explicit,
    # and a stage fed encoded bytes also reports the object and byte rates
    # its input actually has, so an attribution never has to guess.
    def test_denominators_are_explicit(self):
        performance.validate_stage_result(self.complete_stage_result())
        for field in ("input_representation", "denominator"):
            partial = self.complete_stage_result()
            del partial[field]
            with self.subTest(field=field):
                with self.assertRaisesRegex(AssertionError, field):
                    performance.validate_stage_result(partial)
        wrong = self.complete_stage_result(input_representation="otlp_wire_bytes")
        with self.assertRaisesRegex(AssertionError, "handed"):
            performance.validate_stage_result(wrong)
        empty = self.complete_stage_result(denominator="")
        with self.assertRaisesRegex(AssertionError, "divide by"):
            performance.validate_stage_result(empty)
        for stage in performance.PRE_ENCODED_INPUT_STAGES:
            encoded = self.complete_stage_result(
                stage=stage,
                input_representation=performance.INPUT_REPRESENTATIONS[stage],
                output_representation="stored_objects",
                rates={"records_per_s": 1.0},
            )
            with self.subTest(stage=stage):
                with self.assertRaisesRegex(AssertionError, "objects_per_s"):
                    performance.validate_stage_result(encoded)
            performance.validate_stage_result(
                dict(encoded, rates={name: 1.0 for name in performance.RATE_FIELDS})
            )

    # Scenario: a stage result carries every metric but not the fields that
    # say which stage, mode, repetition and build it describes.
    # Guarantees: an unidentifiable measurement is refused, so a result can
    # never be compared against one of another profile or repetition.
    def test_identifying_fields_are_required(self):
        complete = self.complete_stage_result()
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
        base = self.complete_stage_result(
            stage="encode", repetition=2, output_representation="parquet_bytes"
        )
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
        base = self.complete_stage_result(
            stage="merge", output_representation="merged_chunks"
        )
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

    # Scenario: a layer's Criterion group was repeated, so each attempt
    # wrote its artifacts under its own id, and the harness is asked for an
    # attempt whose artifacts are not there.
    # Guarantees: each attempt is read under the id it ran with, through
    # Criterion's own sanitization of that id, and a missing artifact is
    # refused by name instead of being answered from a neighbouring id.
    def test_criterion_artifacts_are_read_per_attempt(self):
        home = temporary_directory(self)
        def write(function, times):
            """One attempt's artifacts, as Criterion writes them."""
            directory = (
                home
                / performance.criterion_directory_name("otlp_sort")
                / performance.criterion_directory_name(function)
                / "new"
            )
            directory.mkdir(parents=True)
            _ = (directory / "sample.json").write_text(
                json.dumps({"times": times, "iters": [1.0] * len(times)})
            )
            _ = (directory / "estimates.json").write_text(
                json.dumps(
                    {
                        "median": {"point_estimate": sum(times) / len(times)},
                        "mean": {"point_estimate": sum(times) / len(times)},
                        "std_dev": {"point_estimate": 0.0},
                    }
                )
            )
        write("logs-1k", [1e9, 5e8])
        write("logs-1k-attempt2", [2e9])
        write("logs/slash", [4e9])
        first = performance.criterion_estimates(home, "otlp_sort", "logs-1k")
        second = performance.criterion_estimates(home, "otlp_sort", "logs-1k-attempt2")
        self.assertAlmostEqual(first["measured_wall_s"], 1.5)
        self.assertAlmostEqual(second["measured_wall_s"], 2.0)
        sanitized = performance.criterion_estimates(home, "otlp_sort", "logs/slash")
        self.assertAlmostEqual(sanitized["measured_wall_s"], 4.0)
        with self.assertRaisesRegex(AssertionError, "logs-1k-attempt3"):
            _ = performance.criterion_estimates(home, "otlp_sort", "logs-1k-attempt3")

    # Scenario: a stages family records the subprocesses its setup phase
    # starts, and the functions that start them are read for the command
    # each one runs.
    # Guarantees: every setup command the harness actually runs -- the git
    # provenance, the rustc version probe, the --describe probe and the
    # Docker store setup -- is named in the recorded list; the number of
    # rustc --version probes it states matches the builds the family
    # actually probes; and the compiler-free claim says it covers measured
    # windows only.
    def test_setup_subprocesses_are_recorded(self):
        recorded = " ".join(performance.SETUP_SUBPROCESSES["before_any_lease"])
        for function, token in (
            (measurement.git_provenance, "rev-parse"),
            (measurement.engine_build, "rustc"),
            (performance.describe_bench, "--describe"),
            (performance.test_e2e.require_docker_image, "info"),
            (performance.test_e2e.require_docker_image, "inspect"),
            (performance.pin_container, "update"),
        ):
            with self.subTest(token=token):
                self.assertIn(token, inspect.getsource(function))
                self.assertIn(token, recorded)
        claim = performance.SETUP_SUBPROCESSES["compiler_free_claim"]
        self.assertIn("measured windows only", claim)
        # One rustc --version per engine_build call: the three bench builds
        # run_stages probes and the two engines engine_binaries probes.
        self.assertEqual(inspect.getsource(performance.bench_build).count(
            "measurement.engine_build("), 1)
        benches = inspect.getsource(performance.run_stages).count("bench_build(")
        engines = inspect.getsource(performance.engine_binaries).count(
            "measurement.engine_build(")
        self.assertEqual((benches, engines), (3, 2))
        self.assertIn("rustc five times, each only for --version", claim)
        self.assertIn("five times in a stages family", recorded)
        self.assertNotIn("once,", claim)

    # Scenario: a layer's group ran three attempts, and its record is read
    # against the artifacts each attempt wrote -- once as the fixed bench
    # records it, once as the stale bench did, repeating the first
    # attempt's 0.78 s for every attempt while the published third attempt
    # measured 1.88 s, and once with an attempt's artifact missing.
    # Guarantees: only a record whose every attempt carries its own
    # artifact's seconds is accepted; the stale readback and a missing
    # artifact are refused by name.
    def test_criterion_attempts_are_their_own_artifacts(self):
        home = temporary_directory(self)
        measured = {
            "logs": [0.5e9, 0.280718548e9],
            "logs-attempt2": [0.9e9],
            "logs-attempt3": [1.0e9, 0.8843071e9],
        }
        for function, times in measured.items():
            directory = (
                home / "otlp_noop" / performance.criterion_directory_name(function)
                / "new"
            )
            directory.mkdir(parents=True)
            _ = (directory / "sample.json").write_text(
                json.dumps({"times": times, "iters": [1.0] * len(times)})
            )
            _ = (directory / "estimates.json").write_text(
                json.dumps({name: {"point_estimate": 1.0}
                            for name in ("median", "mean", "std_dev")})
            )
        def summary(seconds):
            """A layer summary whose attempts recorded `seconds`."""
            return {
                "criterion_function_id": "logs-attempt3",
                "group_attempts": [
                    {"function_id": function, "measured_wall_s": value}
                    for function, value in zip(measured, seconds)
                ],
            }
        performance.criterion_attempts_agree(
            home, "otlp_noop", summary([0.780718548, 0.9, 1.8843071])
        )
        with self.assertRaisesRegex(AssertionError, "attempt 2 .* 0.780718548 s"):
            performance.criterion_attempts_agree(
                home, "otlp_noop", summary([0.780718548] * 3)
            )
        shutil.rmtree(home / "otlp_noop" / "logs-attempt2")
        with self.assertRaisesRegex(AssertionError, "logs-attempt2"):
            performance.criterion_attempts_agree(
                home, "otlp_noop", summary([0.780718548, 0.9, 1.8843071])
            )
        with self.assertRaisesRegex(AssertionError, "does not name its id"):
            performance.criterion_attempts_agree(
                home, "otlp_noop",
                {"criterion_function_id": "logs",
                 "group_attempts": [{"measured_wall_s": 0.780718548}]},
            )

    # Scenario: a stage that stores an object reports its metrics without
    # saying what a completed write means, with a vague claim, with the
    # reversed claim that it provides host power-loss durability, and with
    # a paraphrase of the correct meaning.
    # Guarantees: only the bench's fixed completion semantics are accepted,
    # character for character, so no wording -- least of all the reversed
    # claim -- can pass for a durability statement it is not.
    def test_store_stages_record_completion_semantics(self):
        refused = (
            None,
            "",
            "the object was written",
            "this provides host power-loss durability",
            "the store reported the object written; this is host power-loss "
            "durability",
            "the store reported the object written and the bytes were read back "
            "and verified; this is object-store visibility, not host power-loss "
            "durability",
        )
        for stage in performance.STORE_STAGES:
            result = self.complete_stage_result(
                stage=stage,
                input_representation=performance.INPUT_REPRESENTATIONS[stage],
                output_representation="stored_objects",
            )
            for semantics in refused:
                with self.subTest(stage=stage, semantics=semantics):
                    with self.assertRaisesRegex(AssertionError, "completed write"):
                        performance.validate_stage_result(
                            dict(result, completion_semantics=semantics)
                        )
            with self.subTest(stage=stage, semantics="the bench constant"):
                performance.validate_stage_result(
                    dict(result, completion_semantics=performance.COMPLETION_SEMANTICS)
                )

    # Scenario: the harness's accepted completion semantics are compared
    # with the constant the bench writes into every storing stage result.
    # Guarantees: the two cannot drift apart; a change to the bench's text
    # fails here instead of failing every measured result.
    def test_completion_semantics_match_the_bench(self):
        source = (
            Path(performance.__file__).resolve().parents[3]
            / "series-lake/benches/measurement/stages.rs"
        ).read_text(encoding="ascii")
        start = source.index("pub const COMPLETION_SEMANTICS: &str = ")
        literal = source[start:source.index(";\n", start)]
        literal = literal[literal.index('"') + 1:literal.rindex('"')]
        # A Rust string continuation drops the newline and the next line's
        # leading whitespace.
        text = "".join(
            part.lstrip() if index else part
            for index, part in enumerate(literal.split("\\\n"))
        )
        self.assertEqual(text, performance.COMPLETION_SEMANTICS)
        self.assertEqual(performance.ALLOWED_COMPLETION_SEMANTICS,
                         (performance.COMPLETION_SEMANTICS,))

    # Scenario: a Criterion group takes its thirty samples but accumulates
    # only 0.57 s of measured work, because its batched preparation costs
    # more than the operation it times.
    # Guarantees: every timing process, Criterion's included, must
    # accumulate one second of measured work; a shorter group fails its
    # sample gate instead of being excused by another child.
    def test_a_short_criterion_group_fails(self):
        short = {
            "sample_count": 30,
            "iterations_per_sample": [82.0] * 30,
            "measured_wall_s": 0.572362956,
        }
        passed, detail = performance.criterion_samples_ok(short)
        self.assertFalse(passed)
        self.assertIn("0.572s measured", detail)
        self.assertIn(f"{performance.MINIMUM_MEASURED_S}s required", detail)
        long_enough = dict(short, measured_wall_s=1.0)
        self.assertTrue(performance.criterion_samples_ok(long_enough)[0])
        too_few = dict(long_enough, sample_count=29)
        self.assertFalse(performance.criterion_samples_ok(too_few)[0])

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
    # differently, one on jemalloc and one with DHAT.
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
                    "allocator": performance.TIMING_ALLOCATOR,
                    "toolchain": "rustc 1.88",
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
            "allocator": performance.TIMING_ALLOCATOR,
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
                "allocator": "dhat" if heap else performance.TIMING_ALLOCATOR,
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

    # Scenario: the newest prebuilt timing bench still runs on the system
    # allocator, an older one on the engine's jemalloc; then only the system
    # one is left.
    # Guarantees: a timing child always runs on the engine's allocator: the
    # jemalloc build is chosen over the newer one, and without it the family
    # refuses to start, naming the allocator it found and the build command.
    def test_a_timing_bench_must_run_on_the_engine_allocator(self):
        directory = temporary_directory(self)
        deps = directory / "target/release/deps"
        deps.mkdir(parents=True)
        described = {}
        for age, (name, allocator) in enumerate((
            ("measurement-new", "system"),
            ("measurement-old", performance.TIMING_ALLOCATOR),
            ("layered-ccc", performance.TIMING_ALLOCATOR),
        )):
            path = deps / name
            _ = path.write_text("")
            path.chmod(0o755)
            os.utime(path, (1000 - age, 1000 - age))
            described[str(path)] = {
                "bench": name.split("-")[0], "bench_heap": False,
                "allocator": allocator, "debug_assertions": False,
            }
        with mock.patch.object(performance.test_e2e, "WORKSPACE", directory):
            with mock.patch.object(
                performance, "describe_bench", lambda path, **_: described[str(path)]
            ):
                timing = performance.locate_benches()
                self.assertTrue(
                    timing["measurement"]["executable"].endswith("measurement-old")
                )
                (deps / "measurement-old").unlink()
                with self.assertRaisesRegex(
                    AssertionError, r"'system' allocator.*jemalloc\+background_thread"
                ):
                    _ = performance.locate_benches()

    # Scenario: the build provenance of a timing bench is taken from what
    # the executable described, once for a system-allocator build and once
    # for the engine's jemalloc build, on otherwise equal stage results.
    # Guarantees: the recorded allocator is the described one and the two
    # builds fingerprint differently, so a baseline written on one allocator
    # is never compared with a run on the other; an executable that does not
    # name its allocator is refused.
    def test_the_described_allocator_enters_the_fingerprint(self):
        directory = temporary_directory(self)
        engine = {"profile": "release", "features": "x", "allocator": "jemalloc",
                  "toolchain": "rustc 1.88", "binary": "b", "binary_sha256": "h",
                  "binary_size_bytes": 1}
        builds = {}
        with mock.patch.object(
            performance.measurement, "engine_build", lambda _path: dict(engine)
        ):
            for allocator in ("system", performance.TIMING_ALLOCATOR):
                builds[allocator] = performance.bench_build({
                    "executable": str(directory / "measurement-aaa"),
                    "description": {"bench": "measurement", "allocator": allocator},
                    "features": [],
                })
            with self.assertRaisesRegex(AssertionError, "does not name its allocator"):
                _ = performance.bench_build({
                    "executable": str(directory / "measurement-aaa"),
                    "description": {"bench": "measurement"}, "features": [],
                })
        self.assertEqual(
            builds[performance.TIMING_ALLOCATOR]["allocator"], "jemalloc+background_thread"
        )
        self.assertEqual(builds["system"]["features"], "default")

        def result(build):
            """A stage result that differs only in its build."""
            return {
                "case": "stages-extract",
                "run_dir": str(directory),
                "environment": {
                    "machine_identity_sha256": "m",
                    "start": {
                        "cpu_model": "cpu", "logical_core_count": 32,
                        "physical_core_count": 16, "sibling_groups": [[0, 16]],
                        "ram_bytes": 1, "kernel": "linux", "available_cores": [0, 1],
                    },
                    "core_allocation": {"bench": [1]},
                    "build": build,
                },
                "config": {"effective": {"lake": {}}},
                "workload": measurement.Workload().as_json(),
                "workload_schedule": {
                    "duration_s": 1, "rate_requests_per_s": "closed_loop",
                    "max_in_flight": 1,
                },
            }

        self.assertNotEqual(
            measurement.baseline_fingerprint(result(builds["system"])),
            measurement.baseline_fingerprint(result(builds[performance.TIMING_ALLOCATOR])),
        )

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



class HarnessHygieneContracts(unittest.TestCase):
    """Task 3c harness minors and publication scrubbing."""

    # Scenario: a document carries the repository root, the home directory,
    # paths under /srv, /tmp and /var, S3 static credentials, container and
    # AWS credential assignments, the same secret quoted inside a log line,
    # and ordinary identifiers under a bare `key` and a URL path.
    # Guarantees: the written copy names the repository and home as tokens,
    # reduces every other host path to its final component, redacts every
    # credential value -- user names included -- wherever it appears while
    # keeping its key and variable name, leaves identifiers and URLs intact,
    # and never touches the caller's own document.
    def test_published_documents_carry_no_host_paths_or_credentials(self):
        document = {
            "binary": "/home/alice/src/otel-arrow/rust/otap-dataflow/target/x",
            "log": "/home/alice/notes.txt",
            "other": "/srv/data/file",
            "run_dir": "/tmp/series-launcher/run-1/engine.log",
            "spool": "cwd=/var/lib/docker/overlay2/abc/merged",
            "endpoint": "http://127.0.0.1:9000/api/v1/readyz",
            "base_uri": "s3://series-test/otel",
            "key": "default/main/0/0",
            "auth": {
                "type": "static_credentials",
                "access_key_id": "series-test-access",
                "secret_access_key": "series-test-secret-12345",
            },
            "argv": ["docker", "run", "-e", "MINIO_ROOT_PASSWORD=hunter2",
                     "-e", "RUSTFS_SECRET_KEY=abcdef", "-e", "MINIO_ROOT_USER=minioadmin",
                     "-e", "AWS_ACCESS_KEY_ID=AKIAEXAMPLE",
                     "-e", "AWS_SECRET_ACCESS_KEY=wJalrEXAMPLE"],
            "tail": "signing with series-test-secret-12345 for minioadmin failed",
            "count": 3,
        }
        original = json.loads(json.dumps(document))
        scrubbed = measurement.scrub_published(
            document, repo_root="/home/alice/src/otel-arrow", home="/home/alice"
        )
        self.assertEqual(document, original, "the input is not modified")
        self.assertEqual(scrubbed["binary"], "<repo>/rust/otap-dataflow/target/x")
        self.assertEqual(scrubbed["log"], "<home>/notes.txt")
        self.assertEqual(scrubbed["other"], "<host-path>/file")
        self.assertEqual(scrubbed["run_dir"], "<host-path>/engine.log")
        self.assertEqual(scrubbed["spool"], "cwd=<host-path>/merged")
        self.assertEqual(scrubbed["endpoint"], document["endpoint"])
        self.assertEqual(scrubbed["base_uri"], document["base_uri"])
        self.assertEqual(scrubbed["key"], "default/main/0/0")
        self.assertEqual(scrubbed["auth"]["type"], "static_credentials")
        self.assertEqual(scrubbed["auth"]["access_key_id"], "<redacted>")
        self.assertEqual(scrubbed["auth"]["secret_access_key"], "<redacted>")
        for name in ("MINIO_ROOT_PASSWORD", "RUSTFS_SECRET_KEY", "MINIO_ROOT_USER",
                     "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            self.assertIn(f"{name}=<redacted>", scrubbed["argv"])
        self.assertEqual(
            scrubbed["tail"], "signing with <redacted> for <redacted> failed"
        )
        self.assertEqual(scrubbed["count"], 3)
        text = json.dumps(scrubbed)
        for leaked in ("/home/", "/srv/", "/tmp/", "/var/", "hunter2", "abcdef",
                       "minioadmin", "AKIAEXAMPLE", "wJalrEXAMPLE",
                       "series-test-access", "series-test-secret-12345"):
            self.assertNotIn(leaked, text)

    # Scenario: host paths written as `file://` URLs, a remote URL whose path
    # merely looks like a host path, and a one-letter MinIO user name that a
    # log line also quotes beside words containing the same letter.
    # Guarantees: the `scheme://` form loses its host path like a bare path,
    # a remote authority's path and a name that only starts like a root are
    # kept, a second scrub changes nothing, and a short credential is removed
    # wherever it stands as a whole token while every other word is intact.
    def test_url_paths_and_short_credentials_are_scrubbed(self):
        document = {
            "store": "file:///tmp/secret/objects",
            "home": "file:///home/alice/lake",
            "remote": "http://example.com/tmp/page",
            "tmpdir": "/tmp/tmpabc123",
            "nested": "/tmp/run/data",
            "lookalike": "/tmpfs-is-not-a-root",
            "argv": ["-e", "MINIO_ROOT_USER=u", "-e", "MINIO_ROOT_PASSWORD=p"],
            "tail": "user u signed in, ubuntu saw u's p key; up",
        }
        scrubbed = measurement.scrub_published(
            document, repo_root="/nowhere/repo", home="/nowhere/home"
        )
        self.assertEqual(scrubbed["store"], "file://<host-path>/objects")
        self.assertEqual(scrubbed["home"], "file://<host-path>/lake")
        self.assertEqual(scrubbed["remote"], document["remote"])
        self.assertEqual(scrubbed["tmpdir"], "<host-path>/tmpabc123")
        self.assertEqual(scrubbed["nested"], "<host-path>/data")
        self.assertEqual(scrubbed["lookalike"], "/tmpfs-is-not-a-root")
        self.assertEqual(
            measurement.scrub_published(
                scrubbed, repo_root="/nowhere/repo", home="/nowhere/home"
            ),
            scrubbed,
            "the scrub is idempotent, so publication can verify it",
        )
        self.assertIn("MINIO_ROOT_USER=<redacted>", scrubbed["argv"])
        self.assertEqual(
            scrubbed["tail"],
            "user <redacted> signed in, ubuntu saw <redacted>'s <redacted> key; up",
        )

    # Scenario: a published tree -- an index and one child result -- was
    # written before the scrub and carries a /tmp path and a credential; it
    # is offered for publication, then rescrubbed in place, then offered
    # again.
    # Guarantees: publication refuses the unscrubbed tree; `rescrub_tree`
    # removes the path and the credential from both files, recomputes the
    # child's hash and size in the index so the tree verifies, reports both
    # files as changed, and the rescrubbed tree then publishes.
    def test_a_published_tree_is_rescrubbed_hash_consistently(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "artifacts"
            report = Path(directory) / "report"
            child = measurement.write_json_atomic(
                root / "child.json",
                {"log": "/tmp/series-launcher/legacy-e2e.log",
                 "auth": {"secret_access_key": "series-test-secret-12345"}},
            )
            index = measurement.write_json_atomic(
                root / "index.json",
                {"legacy": {"log": "/tmp/series-launcher/legacy-e2e.log"},
                 "run_files": [measurement.file_entry(child)]},
            )
            with self.assertRaisesRegex(AssertionError, "host path or a credential"):
                measurement.publish_result_tree(index, report)
            changed = measurement.rescrub_tree(index)
            self.assertEqual(sorted(changed), ["child.json", "index.json"])
            for path in (child, index):
                text = path.read_text(encoding="ascii")
                self.assertNotIn("/tmp/", text)
                self.assertNotIn("series-test-secret-12345", text)
            entry = json.loads(index.read_text(encoding="ascii"))["run_files"][0]
            self.assertEqual(entry["sha256"], measurement.file_digest(child))
            self.assertEqual(entry["size_bytes"], child.stat().st_size)
            published = measurement.publish_result_tree(index, report)
            self.assertTrue(published.is_file())
            self.assertEqual(measurement.rescrub_tree(published), [])

    # Scenario: a result is written through `write_result` from a document
    # holding this host's real repository path and a static secret.
    # Guarantees: the file on disk holds neither, so no published result
    # can carry them whatever produced it.
    def test_write_result_scrubs_the_file_it_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            result = measured_result()
            result["config"]["effective"] = {
                "binary": str(measurement.REPO_ROOT / "target/release/df_engine"),
                "secret_access_key": "series-test-secret-12345",
            }
            path = measurement.write_result(Path(directory) / "r.json", result)
            text = path.read_text(encoding="ascii")
            self.assertNotIn(str(measurement.REPO_ROOT), text)
            self.assertNotIn("series-test-secret-12345", text)

    # Scenario: the measured body of a child raises KeyboardInterrupt.
    # Guarantees: `run_child` re-raises it rather than recording one failed
    # child and moving on to the next, so an operator's interrupt stops the
    # whole family.
    def test_run_child_does_not_swallow_an_interrupt(self):
        spec = mock.Mock(run_id="interrupted")
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(performance, "child_spec", return_value=spec), \
                mock.patch.object(measure, "run_case", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                performance.run_child({}, {"profile": "timing"}, directory, directory)

    # Scenario: a build monitor report with a tick gap over the coverage
    # limit is judged for a publishable host and for a non-publishable one,
    # and one whose Docker events were unobservable is judged for the
    # non-publishable host.
    # Guarantees: the gap fails the gate only on a publishable host; on the
    # other it is recorded in the detail as an observation; an unobservable
    # build namespace still fails either way.
    def test_monitor_tick_gaps_are_an_observation_off_a_publishable_host(self):
        report = {
            "coverage": {"gaps_over_limit_count": 2, "limit_s": 0.1,
                         "max_gap_s": 0.4, "ticks": 100},
            "visibility": {"complete": True},
            "docker": {part: {"observable": True} for part in ("start", "end", "events")},
        }
        hard = measurement.coverage_check(report)
        self.assertEqual(hard["status"], measurement.STATUS_FAILED)
        soft = measurement.coverage_check(report, gaps_hard=False)
        self.assertEqual(soft["status"], measurement.STATUS_PASSED)
        self.assertIn("observed, not gated", soft["detail"])
        self.assertIn("2 tick gaps", soft["detail"])
        report["docker"]["events"] = {"observable": False}
        self.assertEqual(
            measurement.coverage_check(report, gaps_hard=False)["status"],
            measurement.STATUS_FAILED,
        )

    # Scenario: the kernel hands the same ephemeral port to two probes in a
    # row.
    # Guarantees: `free_port` never returns a port it already issued; it
    # probes again instead.
    def test_free_port_never_issues_a_port_twice(self):
        e2e = measurement.test_e2e
        issued = set(e2e._ISSUED_PORTS)
        first = e2e.free_port()
        self.assertNotIn(first, issued)

        class Fixed:
            """A socket whose kernel keeps returning `first`, then another."""
            answers = [first, first, 1]

            def __init__(self, *args):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def bind(self, address):
                pass

            def getsockname(self):
                return ("127.0.0.1", Fixed.answers.pop(0))

        e2e._ISSUED_PORTS.discard(1)
        with mock.patch.object(e2e.socket, "socket", Fixed):
            self.assertEqual(e2e.free_port(), 1)
        e2e._ISSUED_PORTS.discard(1)

    # Scenario: the admin API's snapshot does not carry a requested exporter
    # gauge, and then cannot be read at all.
    # Guarantees: `exporter_gauges` reports the gauge as None, never as zero,
    # and reads a present gauge through the same `metric_values` every
    # other assertion uses.
    def test_exporter_gauges_never_read_absence_as_zero(self):
        e2e = measurement.test_e2e
        engine = object.__new__(e2e.Engine)
        snapshot = {"metric_sets": [{"name": e2e.EXPORTER_METRIC_SET, "metrics": [
            {"name": "block.active", "value": 7},
        ]}]}
        with mock.patch.object(e2e, "engine_metrics", return_value=snapshot):
            self.assertEqual(
                engine.exporter_gauges("block.active", "block.flushing"),
                {"block.active": 7, "block.flushing": None},
            )
        with mock.patch.object(e2e, "engine_metrics", side_effect=OSError("down")):
            self.assertEqual(
                engine.exporter_gauges("block.active"),
                {"block.active": None},
            )

    # Scenario: the wall clock reads 3.7 s into a 5 s window.
    # Guarantees: `sleep_to_window_offset` sleeps to 0.2 s into the next
    # window, 1.5 s, so a test acts at a known point of an aligned window.
    def test_sleep_to_window_offset_lands_after_the_boundary(self):
        e2e = measurement.test_e2e
        with mock.patch.object(e2e.time, "time", return_value=1003.7), \
                mock.patch.object(e2e.time, "sleep") as sleep:
            e2e.sleep_to_window_offset(5.0, 0.2)
        self.assertAlmostEqual(sleep.call_args.args[0], 1.5)

    # Scenario: `docker run` of a store container fails with a port another
    # process took, then succeeds on a new port.
    # Guarantees: the failed attempt's container is removed by name before
    # the retry, every docker call carries a timeout, and the store ends up
    # running on the second port.
    def test_docker_store_removes_a_failed_start_and_retries(self):
        e2e = measurement.test_e2e
        store = e2e.DockerStore("minio")
        calls = []

        def run(args, **kwargs):
            calls.append((args, kwargs))
            if args[:2] == ["docker", "run"] and len(
                [c for c in calls if c[0][:2] == ["docker", "run"]]
            ) == 1:
                return subprocess.CompletedProcess(
                    args, 125, "", "Bind for 127.0.0.1:1 failed: port is already allocated"
                )
            return subprocess.CompletedProcess(args, 0, "abc123\n", "")

        with mock.patch.object(e2e.subprocess, "run", side_effect=run):
            store.start_container("image:tag")
        self.assertEqual(store.container, "abc123")
        removals = [c for c in calls if c[0][:3] == ["docker", "rm", "--force"]]
        self.assertEqual(len(removals), 1)
        self.assertEqual(removals[0][0][-1], store.name)
        self.assertTrue(all(kwargs.get("timeout") for _, kwargs in calls))


class MemoryContracts(unittest.TestCase):
    """The Task 6 memory ledger, its sources and its family rules."""

    # Scenario: a double-counted workspace estimate exceeds observed process RSS.
    # Guarantees: reconciliation retains a negative residual instead of clamping it away.
    def test_residual_preserves_negative_discrepancy(self):
        self.assertEqual(memory.residual(100, 60, 15, 10, 20, 5), -10)

    # Scenario: the example configuration's exporter settings and the budget
    # a release worker reported for them in a trial run (1,637,851,136 bytes).
    # Guarantees: the transcribed reservations add up to the worker's own
    # budget with a whole token high-water, and a budget that is not such a
    # sum is refused instead of yielding a fractional token size.
    def test_ledger_transcription_reproduces_the_worker_budget(self):
        exporter = measurement.test_e2e.yaml.safe_load(
            (measurement.test_e2e.WORKSPACE / "configs/series-parquet-local.yaml").read_text()
        )["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]["config"]
        inputs = memory.ledger_inputs(exporter)
        self.assertEqual(inputs["B"], 500 * 1024 * 1024)
        self.assertEqual(inputs["C"], 200000)
        token = memory.token_high_water_of(inputs, 1637851136)
        self.assertEqual(token, 200)
        self.assertEqual(
            memory.retained_reservation(inputs, token) + memory.workspace_reservation(inputs),
            1637851136,
        )
        self.assertIsNone(memory.token_high_water_of(inputs, 1637851137))
        self.assertEqual(memory.parse_bytes("96MiB"), 96 * 1024 * 1024)
        with self.assertRaises(ValueError):
            _ = memory.parse_bytes("12 parsecs")

    # Scenario: an engine log carries tracing lines and jemalloc totals, one
    # record split across two reads.
    # Guarantees: every totals record is returned exactly once, the split one
    # on the read that completes it, and ordinary log text is never kept as
    # a pending fragment.
    def test_allocator_totals_are_read_across_split_writes(self):
        directory = temporary_directory(self)
        log = directory / "engine.log"
        record = (
            '{"jemalloc":{"stats":{"allocated":%d,"active":2,"metadata":3,'
            '"metadata_edata":0,"metadata_rtree":0,"metadata_thp":0,"resident":4,'
            '"mapped":5,"retained":6,"zero_reallocs":0,"background_thread":'
            '{"num_threads":0,"num_runs":0,"run_interval":0}}}}'
        )
        whole = "INFO start\n" + record % 1 + (record % 7)[:40]
        log.write_text(whole)
        stats = memory.JemallocStats(log)
        first = stats.poll()
        self.assertEqual([entry["allocated_bytes"] for entry in first], [1])
        with open(log, "a", encoding="ascii") as handle:
            _ = handle.write((record % 7)[40:] + "\nINFO more\n")
        second = stats.poll()
        self.assertEqual([entry["allocated_bytes"] for entry in second], [7])
        self.assertEqual(second[0]["resident_bytes"], 4)
        self.assertEqual(stats.pending, "")
        self.assertEqual(stats.poll(), [])

    # Scenario: one sample of a measured engine with its mapping categories,
    # allocator totals, tracked heap and accounted bytes.
    # Guarantees: the split adds up -- non-heap plus allocator retention plus
    # allocated equals RSS -- the tracked counters' excess over the live
    # heap is a negative untracked term rather than hidden, and the ledger
    # residual is exactly the live heap neither accounted nor held by the
    # paired control.
    def test_sample_split_adds_up_to_rss(self):
        sample = {
            "rss_bytes": 1000, "anonymous_bytes": 700,
            "smaps": {"rss_total_bytes": 1000, "anonymous_other_bytes": 600,
                      "binary_file_bytes": 250, "file_file_bytes": 50,
                      "thread_stack_bytes": 100},
            "jemalloc": {"allocated_bytes": 250, "resident_bytes": 650,
                         "metadata_bytes": 40},
            "telemetry": {"jemalloc_resident_bytes": 640, "tracked_heap_bytes": 300,
                          "exporter": {"memory.accounted": 120}},
        }
        terms = memory.sample_terms(sample, control=100)
        self.assertEqual(
            terms["non_heap_bytes"] + terms["allocator_retention_bytes"]
            + terms["jemalloc_allocated_bytes"],
            terms["rss_bytes"],
        )
        self.assertEqual(terms["untracked_heap_bytes"], -50)
        self.assertEqual(terms["allocator_resident_overstatement_bytes"], 50)
        self.assertEqual(terms["unexplained_bytes"], 250 - 120 - 100)
        self.assertEqual(terms["file_backed_bytes"], 300)

    # Scenario: a mapping list with a guarded thread stack, the binary, a
    # library, the heap and an anonymous allocator extent.
    # Guarantees: each mapping lands in one category, a guard page's
    # neighbour is a stack and an unguarded extent is not, and the
    # categories add up to the list's own resident total.
    def test_smaps_categories_separate_stacks_from_the_heap(self):
        text = "\n".join([
            "55d000000000-55d000100000 r-xp 00000000 fd:00 1 /opt/df_engine",
            "Rss:                 400 kB",
            "Anonymous:             0 kB",
            "7f0000000000-7f0000001000 ---p 00000000 00:00 0 ",
            "Rss:                   0 kB",
            "7f0000001000-7f0000201000 rw-p 00000000 00:00 0 ",
            "Rss:                  16 kB",
            "Anonymous:            16 kB",
            "7f1000000000-7f1000400000 rw-p 00000000 00:00 0 ",
            "Rss:                1024 kB",
            "Anonymous:          1024 kB",
            "7f2000000000-7f2000010000 r--p 00000000 fd:00 2 /usr/lib/libc.so.6",
            "Rss:                  64 kB",
            "7ffd00000000-7ffd00021000 rw-p 00000000 00:00 0 [stack]",
            "Rss:                  12 kB",
            "",
        ])
        with mock.patch.object(memory.Path, "read_text", return_value=text):
            found = memory.smaps_categories(1, binary="/opt/df_engine")
        self.assertEqual(found["binary_file_bytes"], 400 * 1024)
        self.assertEqual(found["thread_stack_bytes"], 16 * 1024)
        self.assertEqual(found["thread_stack_count"], 1)
        self.assertEqual(found["anonymous_other_bytes"], 1024 * 1024)
        self.assertEqual(found["file_file_bytes"], 64 * 1024)
        self.assertEqual(found["main_stack_bytes"], 12 * 1024)
        self.assertEqual(found["rss_total_bytes"], (400 + 16 + 1024 + 64 + 12) * 1024)

    # Scenario: an active block grows, empties across one pair of samples
    # and grows again; another flush is reported directly.
    # Guarantees: the interval that contains each flush and the one after it
    # (the gauges are one collection stale) are marked, and growth alone is
    # never mistaken for a flush.
    def test_flush_intervals_are_recognized_by_the_block_emptying(self):
        def sample(active, flushing=0):
            """One sample carrying only the two block gauges."""
            return {"telemetry": {"exporter": {
                "block.active": active, "block.flushing": flushing,
            }}}

        samples = [sample(1), sample(5), sample(9), sample(2), sample(4),
                   sample(6), sample(6, flushing=3), sample(7), sample(8)]
        self.assertEqual(memory.annotate_flushes(samples), 4)
        self.assertEqual(
            [memory.flushing(entry) for entry in samples],
            [False, False, False, True, True, False, True, True, False],
        )

    # Scenario: accounted and tracked values of consecutive collections, with
    # repeated responses of one collection in between.
    # Guarantees: the block-pair uncertainty is the largest change between
    # distinct collections; a repeated response adds no pair.
    def test_block_pair_uncertainty_is_the_change_between_collections(self):
        def sample(uptime, accounted):
            """One sample of the measured pipeline at one collection."""
            return {
                "rss_bytes": 10, "anonymous_bytes": 5, "smaps": None, "jemalloc": None,
                "telemetry": {
                    "jemalloc_resident_bytes": None, "tracked_heap_bytes": accounted,
                    "exporter": {"memory.accounted": accounted},
                    "pipelines": {"w": {"group_id": "default", "uptime_s": uptime}},
                },
            }

        samples = [sample(1.0, 0), sample(1.0, 0), sample(1.1, 400), sample(1.2, 100)]
        bound = memory.block_pair_uncertainty(samples)
        self.assertEqual(bound["accounted_bytes"]["epochs"], 2)
        self.assertEqual(bound["accounted_bytes"]["max_change_bytes"], 400)

    # Scenario: three pairs whose primary metric spreads 10 and 20 percent,
    # and signed differences of one sign, of mixed sign within the
    # measurement uncertainty, and of mixed sign beyond it.
    # Guarantees: the spread is (max - min) / median, and a paired difference
    # may change sign only when its whole range is inside the uncertainty.
    def test_family_stability_and_sign_rules(self):
        self.assertAlmostEqual(memory.spread([95, 100, 105]), 0.10)
        self.assertGreater(memory.spread([90, 100, 110]), memory.MAXIMUM_SPREAD)
        self.assertIsNone(memory.spread([]))
        self.assertTrue(memory.signed_consistent([3, 5, 9], uncertainty=0))
        self.assertTrue(memory.signed_consistent([-2, 1, 3], uncertainty=6))
        self.assertFalse(memory.signed_consistent([-20, 1, 30], uncertainty=6))
        self.assertFalse(memory.signed_consistent([-2, 3], uncertainty=None))

    # Scenario: the lease file is held by a live process of this test, then
    # released.
    # Guarantees: a busy lease names its holder so the family waits for that
    # process instead of failing, and a free lease is reported free without
    # being kept.
    def test_a_busy_lease_is_waited_for_not_failed(self):
        directory = temporary_directory(self)
        path = directory / "lease.lock"
        lease = measurement.HostLease(path, run_id="holder")
        _ = lease.acquire(deadline_ns=measurement.time.monotonic_ns() + 10**9)
        try:
            self.assertEqual(memory.lease_holder(path), os.getpid())
        finally:
            lease.release()
        self.assertIsNone(memory.lease_holder(path))
        again = measurement.HostLease(path, run_id="after")
        _ = again.acquire(deadline_ns=measurement.time.monotonic_ns() + 10**9)
        again.release()

    # Scenario: a pair invalidated by a compiler, a pair refused the lease and
    # a clean pair.
    # Guarantees: the first two are recognized so the family reruns them
    # rather than accepting them; the clean one is neither.
    def test_invalidated_and_refused_pairs_are_rerun(self):
        built = {"checks": [measurement.check(
            "no_concurrent_build", measurement.CHECK_HARD, measurement.STATUS_FAILED
        )], "events": []}
        refused = {"checks": [], "events": [
            {"kind": "failed", "detail": "AssertionError: another measurement holds x"}
        ]}
        clean = {"checks": [measurement.check(
            "no_concurrent_build", measurement.CHECK_HARD, measurement.STATUS_PASSED
        )], "events": []}
        self.assertTrue(memory.invalidated(built))
        self.assertTrue(memory.lease_refused(refused))
        self.assertFalse(memory.invalidated(clean) or memory.lease_refused(clean))

    # Scenario: a minimal pprof profile with two samples, one allocated under
    # an arrow_row frame and one under a tokio frame.
    # Guarantees: the parser attributes each sample's bytes to its category
    # and the live total is their sum.
    def test_heap_profile_attributes_live_bytes(self):
        def varint(value):
            """One protobuf varint."""
            out = bytearray()
            while True:
                byte = value & 0x7F
                value >>= 7
                if value:
                    out.append(byte | 0x80)
                else:
                    out.append(byte)
                    return bytes(out)

        def field(number, payload):
            """One length-delimited protobuf field."""
            return varint(number << 3 | 2) + varint(len(payload)) + payload

        def packed(number, values):
            """One packed repeated integer field."""
            return field(number, b"".join(varint(value) for value in values))

        strings = [b"", b"space", b"bytes", b"arrow_row::RowConverter::convert_columns",
                   b"tokio::runtime::task"]
        profile = b"".join(field(6, text) for text in strings)
        profile += field(1, varint(1 << 3) + varint(1) + varint(2 << 3) + varint(2))
        for identifier, name in ((1, 3), (2, 4)):
            profile += field(5, varint(1 << 3) + varint(identifier) + varint(2 << 3) + varint(name))
            line = varint(1 << 3) + varint(identifier)
            profile += field(4, varint(1 << 3) + varint(identifier) + field(4, line))
        profile += field(2, packed(1, [1]) + packed(2, [1, 4096]))
        profile += field(2, packed(1, [2]) + packed(2, [1, 1000]))
        summary = memory.pprof_summary(profile)
        self.assertEqual(summary["live_bytes"], 5096)
        self.assertEqual(summary["by_category_bytes"]["merge_keys"], 4096)
        self.assertEqual(summary["by_category_bytes"]["tokio"], 1000)

    # Scenario: the command line's memory subcommand and the memory case's
    # role placement.
    # Guarantees: `memory` is a long command that must be opted into, and a
    # memory pair claims a producer and a reader but no object store.
    def test_memory_command_and_roles(self):
        self.assertIn("memory", measure.LONG_COMMANDS)
        self.assertEqual(
            dict(measurement.CASE_ROLES["memory"]), {"producer": 2, "reader": 1}
        )
        with mock.patch.dict(os.environ, {"SERIES_MEASURE_LONG": ""}):
            self.assertEqual(measure.main(["memory", "--output-dir", "/nonexistent"]), 2)


    # Scenario: a full mapping classification, then a cheaper rollup read
    # whose anonymous total grew by 3 MiB and whose RSS grew by 4 MiB.
    # Guarantees: the interpolated categories keep the stacks and binary
    # data of the last full read, give the anonymous growth to the heap and
    # the file-backed rest to the binary text, and add up to the rollup RSS.
    def test_rollup_samples_interpolate_the_last_classification(self):
        mib = 1024 * 1024
        last = {"rss_total_bytes": 100 * mib, "anonymous_other_bytes": 40 * mib,
                "thread_stack_bytes": 2 * mib, "binary_anonymous_bytes": 5 * mib,
                "binary_file_bytes": 50 * mib, "file_file_bytes": 3 * mib}
        now = memory.interpolate_smaps(
            last, {"smaps_rss_bytes": 104 * mib, "smaps_anonymous_bytes": 50 * mib}
        )
        self.assertEqual(now["anonymous_other_bytes"], 43 * mib)
        self.assertEqual(now["thread_stack_bytes"], 2 * mib)
        self.assertEqual(now["binary_file_bytes"], 51 * mib)
        self.assertEqual(now["rss_total_bytes"], 104 * mib)
        self.assertEqual(
            sum(value for key, value in now.items()
                if key.endswith("_bytes") and key != "rss_total_bytes"),
            now["rss_total_bytes"],
        )

    # Scenario: harness residual entries with a one-second gauge residual
    # that peaks where the 100 ms gauge residual is much smaller.
    # Guarantees: the shares are taken at the one-second peak and the
    # sampling skew is exactly the difference between the two cadences.
    def test_residual_shares_split_the_one_second_peak(self):
        entries = [
            {"monotonic_ns": 1, "phase": "load", "residual_bytes": 5,
             "one_second_gauge_residual_bytes": 10, "allocator_retention_growth_bytes": 3,
             "non_heap_anonymous_growth_bytes": 0, "heap_beyond_tracked_growth_bytes": 2},
            {"monotonic_ns": 2, "phase": "load", "residual_bytes": 15,
             "one_second_gauge_residual_bytes": 47, "allocator_retention_growth_bytes": 12,
             "non_heap_anonymous_growth_bytes": 0, "heap_beyond_tracked_growth_bytes": 3},
        ]
        shares = memory.residual_shares(entries)
        self.assertEqual(shares["monotonic_ns"], 2)
        self.assertEqual(shares["sampling_skew_bytes"], 32)
        self.assertEqual(
            shares["sampling_skew_bytes"] + shares["hundred_ms_gauge_residual_bytes"],
            shares["one_second_gauge_residual_bytes"],
        )

    # Scenario: the high-rate logs shape of the Task 4 attribution workload.
    # Guarantees: it is logs only, 256 in flight against 128-request blocks
    # on a MinIO store with the harness's S3 retry section, and its case and
    # run id name the shape and the store.
    def test_high_rate_shape_matches_the_attribution_workload(self):
        with mock.patch.object(measure, "default_engine_cores", return_value=(1,)):
            spec = memory.memory_spec("strict", config="logs-high-rate")
        self.assertEqual(spec.case, "memory-strict-logs-high-rate")
        self.assertEqual(spec.store, "minio")
        self.assertIn("-strict-minio-", spec.run_id)
        self.assertEqual(spec.max_in_flight, 256)
        self.assertEqual(spec.workload.signal_of(5), "logs")
        exporter = spec.overrides["exporter"]
        self.assertEqual(exporter["window"]["max_requests_per_block"], 128)
        self.assertEqual(exporter["retry"], measurement.test_e2e.S3_RETRY)
        self.assertEqual(
            dict(measurement.CASE_ROLES["memory_store"]),
            {"producer": 1, "store": 1, "reader": 1},
        )


    # Scenario: engine logs from a binary with the compiled-in background
    # thread, from one with it off, and from one too old to say.
    # Guarantees: the fingerprint's allocator names the background thread
    # only when the engine itself reported it on, appends non-default
    # allocator options, and reads an engine that reported nothing as off.
    def test_allocator_label_follows_the_engine_report(self):
        directory = temporary_directory(self)
        log = directory / "engine.log"
        log.write_text("banner\nINFO memory allocator jemalloc, background_thread on\n")
        reported = memory.engine_background_thread(log)
        self.assertEqual(reported, ("jemalloc", "on"))
        self.assertEqual(
            memory.allocator_label("jemalloc", memory.JEMALLOC_STATS_CONF, reported),
            "jemalloc+background_thread",
        )
        decay = memory.DIAGNOSTIC_CONF["decay0"]
        self.assertEqual(
            memory.allocator_label("jemalloc", decay, ("jemalloc", "off")),
            f"jemalloc:{decay}",
        )
        log.write_text("an older engine prints no allocator line\n")
        self.assertIsNone(memory.engine_background_thread(log))
        self.assertEqual(
            memory.allocator_label("jemalloc", memory.JEMALLOC_STATS_CONF, None), "jemalloc"
        )


    # Scenario: the ledger's workspace term when the run itself measured no
    # flush workspace.
    # Guarantees: the term is zero with a provenance saying why, so a flush's
    # transient heap stays in the residual and a tolerance failure is
    # reported rather than explained away by another run's measurement.
    def test_ledger_workspace_term_is_zero_without_an_in_run_measurement(self):
        self.assertEqual(memory.NO_WORKSPACE_TERM["bytes"], 0)
        self.assertIn("no in-run measurement", memory.NO_WORKSPACE_TERM["provenance"])
        self.assertFalse(hasattr(memory, "flush_workspace"))
        mib = 1024 * 1024
        entries = [{"residual_bytes": 46 * mib, "monotonic_ns": 1, "phase": "load"}]
        self.assertEqual(
            memory.ledger_check(entries, 100 * mib)["status"], measurement.STATUS_FAILED
        )

    # Scenario: a family asks for jemalloc's background thread explicitly
    # off, as the engine ran before the thread became its default.
    # Guarantees: the mode keeps the statistics print, turns the thread off
    # and names that setting in the pair's allocator label, so a family run
    # under it is never compared with one that ran the thread.
    def test_the_background_thread_can_be_turned_off_for_a_family(self):
        conf = memory.DIAGNOSTIC_CONF["nobgthread"]
        self.assertTrue(conf.startswith(memory.JEMALLOC_STATS_CONF))
        self.assertIn("background_thread:false", conf)
        self.assertEqual(
            memory.allocator_label("jemalloc", conf, ("jemalloc", "off")),
            f"jemalloc:{conf}",
        )

    # Scenario: samples of an engine whose worker publishes its live flush
    # workspace as `flush.workspace`, one of them taken during a flush, and
    # samples of an older engine that publishes none.
    # Guarantees: each sample records the workspace it saw; the ledger names
    # the in-run term when the run published one -- already inside
    # `memory.accounted`, so it is not subtracted twice and the residual is
    # unchanged -- and keeps the zero term with its provenance otherwise.
    def test_the_ledger_uses_the_in_run_flush_workspace_when_published(self):
        def sample(exporter):
            return {
                "rss_bytes": 1000, "anonymous_bytes": 700,
                "smaps": {"rss_total_bytes": 1000, "anonymous_other_bytes": 600,
                          "binary_file_bytes": 250, "file_file_bytes": 50,
                          "thread_stack_bytes": 100},
                "jemalloc": {"allocated_bytes": 250, "resident_bytes": 650,
                             "metadata_bytes": 40},
                "telemetry": {"jemalloc_resident_bytes": 640, "tracked_heap_bytes": 300,
                              "exporter": exporter},
            }
        flushing = sample({"memory.accounted": 160, "flush.workspace": 40})
        quiet = sample({"memory.accounted": 120, "flush.workspace": 0})
        older = sample({"memory.accounted": 120})
        terms = memory.sample_terms(flushing, control=100)
        self.assertEqual(terms["flush_workspace_bytes"], 40)
        self.assertEqual(terms["unexplained_bytes"], 250 - 160 - 100)
        self.assertIsNone(memory.sample_terms(older, control=100)["flush_workspace_bytes"])
        term = memory.workspace_term([quiet, flushing])
        self.assertEqual(term, memory.IN_RUN_WORKSPACE_TERM)
        self.assertEqual(term["bytes"], 0)
        self.assertIn("flush.workspace", term["provenance"])
        self.assertIn("memory.accounted", term["provenance"])
        self.assertEqual(memory.workspace_term([older]), memory.NO_WORKSPACE_TERM)

    # Scenario: a published pair whose measured lifetime pairs accounted
    # bytes 3 MiB apart at most, whose control heap strays 2 MiB from its
    # load median, and whose allocator prints move 1 MiB apart at most.
    # Guarantees: the pair's uncertainty covers both lifetimes and the
    # allocator timing; the maxima add up to the bound and the 95th
    # percentiles to a separately labelled estimate.
    def test_pair_uncertainty_covers_both_lifetimes(self):
        mib = 1024 * 1024

        def bpu(accounted, rss):
            """One lifetime's block-pair record."""
            return {
                "accounted_bytes": {"max_change_bytes": accounted, "p95_change_bytes": accounted // 2},
                "rss_bytes": {"max_change_bytes": rss, "p95_change_bytes": rss // 2},
            }

        samples = [
            {"lifetime": "control", "phase": "load", "jemalloc": {"allocated_bytes": value}}
            for value in (6 * mib, 8 * mib, 10 * mib)
        ] + [
            {"lifetime": "measured", "phase": "load", "jemalloc_age_ns": 5,
             "jemalloc_printed": [{"allocated_bytes": 20 * mib}, {"allocated_bytes": 21 * mib}]},
        ]
        child = {
            "observations": {"lifetimes": [
                {"label": "control", "block_pair_uncertainty": bpu(0, 4 * mib)},
                {"label": "measured", "block_pair_uncertainty": bpu(3 * mib, 6 * mib)},
            ]},
            "samples": samples,
        }
        found = memory.pair_uncertainty(child)
        unexplained = found["load_unexplained_median_bytes"]
        self.assertEqual(unexplained["control_heap_max_bytes"], 2 * mib)
        self.assertEqual(unexplained["allocated_interval_movement_max_bytes"], 1 * mib)
        self.assertEqual(unexplained["bound_bytes"], (3 + 2 + 1) * mib)
        self.assertLessEqual(unexplained["estimate_bytes"], unexplained["bound_bytes"])
        self.assertEqual(found["exporter_peak_rss_delta_bytes"]["bound_bytes"], 10 * mib)
        self.assertIn("estimate", found["labels"]["estimate_bytes"])


    # Scenario: a measured lifetime whose samples each carry one allocator
    # print, rising 1 MiB, then 5 MiB, then back to 2 MiB, followed by a
    # sample whose three prints climb 0, 2 and 4 MiB above the previous one,
    # a sample that printed nothing, and a control print in between that
    # belongs to the other lifetime.
    # Guarantees: movement is measured over the lifetime's whole print
    # stream, so a change between the last print of one sample and the first
    # print of the next counts; a multi-print interval counts its whole range,
    # not only adjacent steps; a silent sample spans to the next print; and
    # the other lifetime's prints are ignored.
    def test_allocator_movement_spans_the_whole_print_stream(self):
        mib = 1024 * 1024

        def sample(lifetime, *values):
            """One compact sample with its printed allocated values."""
            return {"lifetime": lifetime,
                    "jemalloc_printed": [{"allocated_bytes": v * mib} for v in values]}

        samples = [
            sample("measured", 1), sample("measured", 5), sample("control", 50),
            sample("measured", 2), sample("measured", 2, 4, 6), sample("measured"),
            sample("measured", 3),
        ]
        moves = memory.ledger_interval_movement(samples, "measured")
        # Stream 1, 5, 2, 2, 4, 6, 3 MiB: every interval runs from the previous
        # sample's last print to the next sample's first one.
        self.assertEqual(moves, [4 * mib, 4 * mib, 3 * mib, 4 * mib, 3 * mib, 3 * mib])
        # Adjacent steps inside the three-print sample are 2 MiB; its interval
        # moved 4 MiB (2 -> 6), and the silent sample spans 6 -> 3.
        self.assertNotIn(48 * mib, moves)

    # Scenario: a report directory holding one family index, its aggregate
    # and two pairs, one of whose recorded ledger residuals reaches 40 MiB.
    # Guarantees: reaggregate re-judges the pairs with no workspace term,
    # names every file it read -- index, aggregate and both pairs -- in its
    # manifest with its hash, publishes a new document under the given run id
    # and leaves every source file unchanged.
    def test_reaggregate_names_every_file_it_reads(self):
        report = temporary_directory(self)
        out = temporary_directory(self)
        mib = 1024 * 1024
        snapshot = {
            "cpu_model": "cpu", "logical_core_count": 2, "physical_core_count": 1,
            "ram_bytes": 1, "kernel": "k", "load_average_1_5_15": [0, 0, 0],
            "thread_affinity": {},
        }

        def pair(run_id, peak):
            """One published pair with its per-sample ledger residuals."""
            document = {
                "run_id": run_id,
                "environment": {"start": snapshot, "end": snapshot},
                "metrics": {"exporter_peak_rss_delta_bytes": 30 * mib,
                            "load_unexplained_median_bytes": 2 * mib},
                "observations": {
                    "ledger_residuals": [
                        {"monotonic_ns": i, "phase": "load", "flush_interval": False,
                         "residual_without_workspace_bytes": value,
                         "residual_bytes": value - 30 * mib}
                        for i, value in enumerate((1 * mib, peak))
                    ],
                    "lifetimes": [],
                },
                "samples": [{"lifetime": "measured", "rss_bytes": 100 * mib}],
            }
            path = report / f"{run_id}.json"
            path.write_text(json.dumps(document))
            return path

        children = [pair("fam-strict-local-c1-w1-r001", 40 * mib),
                    pair("fam-strict-local-c1-w1-r002", 3 * mib)]
        aggregate = report / "fam-f001.json"
        aggregate.write_text(json.dumps({
            "run_id": "fam-f001",
            "run_files": [measurement.file_entry(path) for path in children],
            "checks": [measurement.check(
                "pair_stability", measurement.CHECK_HARD, measurement.STATUS_PASSED)],
        }))
        index = report / "fam.json"
        index.write_text(json.dumps({"run_files": [measurement.file_entry(aggregate)]}))
        before = {path.name: measurement.file_digest(path) for path in report.iterdir()}
        result = memory.reaggregate(["fam"], out, report_dir=report, run_id="fam-reagg-t1")
        manifest = {entry["name"]: entry["sha256"] for entry in result["run_files"]}
        self.assertEqual(set(manifest), set(before))
        for name, digest in before.items():
            self.assertEqual(manifest[name], digest)
            self.assertEqual(measurement.file_digest(report / name), digest)
        family = result["observations"]["families"][0]
        gates = [entry["ledger_gate"]["status"] for entry in family["pairs"]]
        self.assertEqual(gates, [measurement.STATUS_FAILED, measurement.STATUS_PASSED])
        self.assertFalse(family["family_passes_under_corrected_ledger"])
        self.assertTrue((report / "fam-reagg-t1.json").is_file())

# A stand-in for perf that speaks the control-FIFO protocol a real one does:
# `record` acknowledges enable and disable, writes its data file when it is
# interrupted, and `script` prints a fixed two-sample profile. With
# FAKE_PERF_MODE=refuse, `record` fails the way a host with a restrictive
# perf_event_paranoid makes it fail.
FAKE_PERF = r"""
import os, select, signal, sys
mode = os.environ.get("FAKE_PERF_MODE", "record")
arguments = sys.argv[1:]
if arguments[:1] == ["--version"]:
    print("perf version fake")
    sys.exit(0)
if arguments[:1] == ["script"]:
    print("df_engine  101 [003]  10.000001:    5000000 cpu-clock:u: ")
    print("\t    55d0a1 otel_arrow_dfe_series_lake::extract::logs::extract_logs+0x12 (/bin/df_engine)")
    print("\t    55d0a0 main (/bin/df_engine)")
    print("")
    print("df_engine  101 [003]  10.005001:    5000000 cpu-clock:u: ")
    print("\t    55d0b1 parquet::arrow::arrow_writer::ArrowWriter::write+0x9 (/bin/df_engine)")
    print("\t    55d0b0 main (/bin/df_engine)")
    print("")
    sys.exit(0)
if mode == "refuse":
    sys.stderr.write("Error:\nAccess to performance monitoring and observability "
                     "operations is limited.\nperf_event_paranoid setting is 4:\n")
    sys.exit(255)
output = arguments[arguments.index("-o") + 1]
ctl, ack = arguments[arguments.index("--control") + 1].split(":", 1)[1].split(",")
stop = []
signal.signal(signal.SIGINT, lambda *_: stop.append(True))
ctl_fd = os.open(ctl, os.O_RDONLY | os.O_NONBLOCK)
ack_fd = os.open(ack, os.O_WRONLY)
pending = b""
while not stop:
    ready, _, _ = select.select([ctl_fd], [], [], 0.05)
    if not ready:
        continue
    try:
        pending += os.read(ctl_fd, 64)
    except BlockingIOError:
        continue
    while b"\n" in pending:
        line, pending = pending.split(b"\n", 1)
        if line in (b"enable", b"disable"):
            os.write(ack_fd, b"ack\n")
with open(output, "w") as handle:
    handle.write("fake")
sys.exit(0)
"""


def fake_perf(case) -> str:
    """An executable stand-in for perf in a directory of this test's own."""
    path = temporary_directory(case) / "perf"
    path.write_text(f"#!{sys.executable}\n{FAKE_PERF}", encoding="ascii")
    path.chmod(0o755)
    return str(path)


# The shape of a `perf script -F comm,tid,cpu,time,period,event,ip,sym,dso`
# profile: two samples, innermost frame first, one of them through the
# kernel, and one line that is neither a header nor a frame.
PERF_SCRIPT_TEXT = """\
pipeline-defaul  4242 [001] 12345.000100:    5025125 cpu-clock:
\tffffffff9a2b1c00 copy_user_enhanced_fast_string+0x10 ([kernel.kallsyms])
\t    55d0c3a1b2c4 <object_store::aws::client::S3Client>::put_part::{{closure}}+0x44 (/tmp/df_engine)
\t    55d0c3a1b000 <otel_arrow_dfe_series_lake::sink::Sink>::write_block::<T>::{{closure}}+0x1f (/tmp/df_engine)
\t    55d0c3a10000 tokio::runtime::task::raw::poll+0x3 (/tmp/df_engine)

pipeline-defaul  4242 [001] 12345.005100:    4999875 cpu-clock:
\t    55d0c3a2a000 _rjem_malloc+0x5 (/tmp/df_engine)
\t    55d0c3a29000 otel_arrow_dfe_series_lake::extract::logs::extract_logs+0x800 (/tmp/df_engine)
\t    55d0c3a28000 [unknown] ([unknown])

this line is neither
"""


class AttributionContracts(unittest.TestCase):
    """Direct CPU attribution of the real engine and its reconciliation."""

    # Scenario: extraction, encoding and upload have distinct CPU stacks and durations.
    # Guarantees: overlapping async wall intervals cannot inflate CPU percentages.
    def test_cpu_attribution_is_exclusive(self):
        samples = [
            {"frames": ["Worker::extract", "extract::logs::extract"], "weight": 7},
            {"frames": ["Sink::write_block", "parquet::arrow::arrow_writer"], "weight": 5},
            {"frames": ["Sink::write_block", "object_store::client::http"], "weight": 2},
        ]
        cpu = performance.classify_cpu(samples)
        self.assertEqual(cpu["extraction"], 7)
        self.assertEqual(cpu["encoding"], 5)
        self.assertEqual(cpu["upload"], 2)
        self.assertEqual(sum(cpu.values()), 14)

    # Scenario: stacks as the release engine produces them -- the allocator
    # under extraction, Tokio and hyper frames under an object store call,
    # an Arrow kernel under the merge, a bare scheduler stack, and a stack
    # of frames no rule knows.
    # Guarantees: the innermost production frame decides, a runtime frame
    # never takes a production sample, the runtime fallback still claims
    # the scheduler's own work, unmatched samples stay unknown, and the
    # allocator's callers are named.
    def test_the_innermost_production_frame_decides(self):
        lake = "otel_arrow_dfe_series_lake"
        stacks = {
            "allocator": [
                "tokio::runtime::task::raw::poll",
                f"{lake}::extract::logs::extract_logs",
                "alloc::vec::Vec<T>::push",
                "<tikv_jemallocator::Jemalloc as core::alloc::global::GlobalAlloc>::alloc",
                "_rjem_je_malloc_default",
            ],
            "upload": [
                f"<{lake}::sink::Sink>::write_block::{{{{closure}}}}",
                "<object_store::buffered::BufWriter as tokio::io::AsyncWrite>::poll_write",
                "hyper::proto::h1::dispatch::Dispatcher::poll",
                "tokio::net::tcp::stream::TcpStream::poll_write",
                "__libc_send",
            ],
            "sort_seal_merge": [
                f"<{lake}::sink::Sink>::write_table::{{{{closure}}}}",
                f"<{lake}::sort::MergeIter as core::iter::traits::iterator::Iterator>::next",
                "arrow_select::interleave::interleave",
                "__memmove_avx_unaligned_erms",
            ],
            "encoding": [
                f"<{lake}::sink::Sink>::write_table::{{{{closure}}}}",
                "parquet::arrow::arrow_writer::ArrowWriter<W>::write",
                "ZSTD_compressBlock_doubleFast",
            ],
            "conversion": [
                "otel_arrow_dfe_core_nodes::exporters::series_parquet_exporter::worker::Worker::extract",
                "<otel_arrow_dfe_pdata::otap::OtapArrowRecords as otel_arrow_dfe_pdata::payload::TryFromWithOptions<otel_arrow_dfe_pdata::OtlpProtoBytes>>::try_from_with_options",
                "otel_arrow_dfe_pdata::encode::encode_logs_otap_batch",
                "arrow_array::builder::GenericByteBuilder<T>::append_value",
            ],
            "buffer": [
                "otel_arrow_dfe_core_nodes::exporters::series_parquet_exporter::worker::Worker::offer",
                f"<{lake}::buffer::Block<T>>::admit",
                f"{lake}::cache::SeriesCache::is_committed",
            ],
            "engine_runtime": [
                "std::sys::pal::unix::thread::Thread::new::thread_start",
                "tokio::runtime::scheduler::current_thread::CurrentThread::block_on",
                "mio::poll::Poll::poll",
                "epoll_wait",
            ],
            "unknown": ["[unknown]", "__memmove_avx_unaligned_erms"],
        }
        samples = [
            {"frames": frames, "weight": 3} for frames in stacks.values()
        ]
        for category, frames in stacks.items():
            with self.subTest(category=category):
                self.assertEqual(performance.classify_frames(frames)[0], category)
        cpu = performance.classify_cpu(samples)
        self.assertEqual(sum(cpu.values()), 3 * len(stacks))
        self.assertEqual(set(cpu), set(performance.CPU_CATEGORIES))
        callers = performance.allocator_callers(samples)
        self.assertEqual(callers["extraction"], 3)
        self.assertEqual(sum(callers.values()), cpu["allocator"])

    # Scenario: a sample with a zero, negative, fractional or boolean weight,
    # or without a list of frames.
    # Guarantees: it is refused rather than added, and an empty stack is an
    # unknown sample, not an error.
    def test_invalid_samples_are_refused(self):
        for weight in (0, -1, 1.5, True, None):
            with self.subTest(weight=weight):
                with self.assertRaises(ValueError):
                    _ = performance.classify_cpu([{"frames": ["main"], "weight": weight}])
        with self.assertRaises(ValueError):
            _ = performance.classify_cpu([{"frames": "main", "weight": 1}])
        self.assertEqual(
            performance.classify_cpu([{"frames": [], "weight": 4}])["unknown"], 4
        )

    # Scenario: demangled Rust symbols with offsets, legacy hashes, generic
    # arguments, turbofish, trait impls and nested qualified paths.
    # Guarantees: each reduces to the implementing type's module path, which
    # is what a rule prefix matches.
    def test_frame_paths_drop_rust_symbol_syntax(self):
        cases = {
            "<otel_arrow_dfe_series_lake::sink::Sink>::write_block::<u8>::{{closure}}+0x1f":
                "otel_arrow_dfe_series_lake::sink::Sink::write_block::{{closure}}",
            "otel_arrow_dfe_series_lake::extract::metrics::extract_metrics::h0123456789abcdef":
                "otel_arrow_dfe_series_lake::extract::metrics::extract_metrics",
            "<alloc::vec::Vec<T,A> as core::ops::drop::Drop>::drop":
                "alloc::vec::Vec::drop",
            "<&mut F as core::ops::function::FnOnce<A>>::call_once": "F::call_once",
            "<<tokio::runtime::task::Task<S> as X>::Output as Y>::poll":
                "tokio::runtime::task::Task::Output::poll",
            "<[otel_arrow_dfe_series_lake::value::Value] as Z>::to_vec":
                "otel_arrow_dfe_series_lake::value::Value]::to_vec",
            # Rust's v0 mangling, as c++filt demangles it, with crate
            # disambiguators and const generics.
            "<otel_arrow_dfe_core_nodes[ac52c27a24eadf66]::exporters::series_parquet_exporter"
            "::worker::Worker>::admit":
                "otel_arrow_dfe_core_nodes::exporters::series_parquet_exporter::worker::Worker::admit",
            "arrow_ord[4b22431ba0740f66]::ord::compare_impl::<false: bool, false: bool>"
            "::{closure#0}": "arrow_ord::ord::compare_impl::{closure#0}",
        }
        for symbol, path in cases.items():
            with self.subTest(symbol=symbol):
                self.assertEqual(performance.frame_path(symbol), path)
        self.assertEqual(
            performance.classify_frame(
                "<[otel_arrow_dfe_series_lake::value::Value] as Z>::to_vec", 1
            ),
            "extraction",
        )
        self.assertEqual(
            performance.classify_frame("__rustc[100742bb89c490cb]::__rust_alloc", 1),
            "allocator",
        )

    # Scenario: three ELF files: one linked the way lld links the engine by
    # default, with the executable segment 4 KiB above its file offset, one
    # relinked with page-aligned segments, and one where every segment shares
    # one base.
    # Guarantees: only a binary whose executable segments share the first
    # segment's base is accepted, since perf's unwinder places the module
    # there; the refusal names the relink command, and the preflight
    # refuses such an engine before recording anything.
    def test_an_engine_perf_cannot_unwind_is_refused(self):
        def elf(path, segments):
            header = bytearray(64)
            header[:6] = b"\x7fELF\x02\x01"
            struct.pack_into("<Q", header, 32, 64)
            struct.pack_into("<HH", header, 54, 56, len(segments))
            table = b"".join(
                struct.pack("<IIQQQQQQ", 1, flags, offset, vaddr, vaddr, 0, 0, 0x1000)
                for offset, vaddr, flags in segments
            )
            path.write_bytes(bytes(header) + table)
            return path

        root = temporary_directory(self)
        lld = elf(root / "lld", [(0, 0, 4), (0x1ca7d40, 0x1ca8d40, 5)])
        relinked = elf(root / "relinked", [(0, 0, 4), (0x1ca8000, 0x1ca8000, 5)])
        fixed = elf(root / "fixed", [(0, 0x400000, 4), (0x20000, 0x420000, 5)])
        refused = performance.unwind_layout(lld)
        self.assertFalse(refused["compatible"])
        self.assertIn("separate-loadable-segments", refused["build"])
        self.assertTrue(performance.unwind_layout(relinked)["compatible"])
        self.assertTrue(performance.unwind_layout(fixed)["compatible"])
        facts = performance.perf_preflight(
            temporary_directory(self), perf=fake_perf(self), engine=lld
        )
        self.assertFalse(facts["attached"])
        self.assertIn("relink", facts["reason"])

    # Scenario: the text `perf script` prints for two samples, one through
    # the kernel, followed by a stray line.
    # Guarantees: frames come out outermost first with their period as the
    # weight, the kernel sample is marked, and the stray line is counted,
    # never silently dropped.
    def test_perf_script_is_parsed_outermost_first(self):
        parsed = performance.parse_perf_script(PERF_SCRIPT_TEXT)
        samples = parsed["samples"]
        self.assertEqual(len(samples), 2)
        self.assertEqual(parsed["unparsed_lines_count"], 1)
        first, second = samples
        self.assertEqual(first["weight"], 5025125)
        self.assertEqual((first["comm"], first["tid"], first["cpu"]), ("pipeline-defaul", 4242, 1))
        self.assertEqual(first["frames"][0], "tokio::runtime::task::raw::poll+0x3")
        self.assertEqual(first["frames"][-1], "copy_user_enhanced_fast_string+0x10")
        self.assertTrue(first["kernel"])
        self.assertFalse(second["kernel"])
        self.assertEqual(performance.classify_frames(first["frames"])[0], "upload")
        self.assertEqual(performance.classify_frames(second["frames"])[0], "allocator")
        cpu = performance.classify_cpu(samples)
        self.assertEqual(cpu["upload"] + cpu["allocator"], 5025125 + 4999875)

    # Scenario: a profile of ten samples, three of them unknown, one inside
    # the flush task.
    # Guarantees: shares are by weight and add up to one, each has the
    # binomial interval of its sample count, CPU seconds are split by
    # logical CPU and thread, and the unknown share is named by its leaf.
    def test_profile_summary_states_shares_intervals_and_the_residual(self):
        flush = "otel_arrow_dfe_core_nodes::exporters::series_parquet_exporter::flush::write_until::{{closure}}"
        samples = (
            [{"frames": ["otel_arrow_dfe_series_lake::extract::extract"], "weight": 10,
              "cpu": 1, "comm": "worker"}] * 6
            + [{"frames": [flush, "parquet::file::writer::write"], "weight": 10,
                "cpu": 2, "comm": "worker"}]
            + [{"frames": ["__memcpy_evex"], "weight": 10, "cpu": 1, "comm": "other"}] * 3
        )
        summary = performance.profile_summary(samples)
        categories = summary["categories"]
        self.assertAlmostEqual(sum(entry["share_ratio"] for entry in categories.values()), 1.0)
        self.assertAlmostEqual(categories["extraction"]["share_ratio"], 0.6)
        self.assertAlmostEqual(
            categories["extraction"]["share_ci95_ratio"], 1.96 * (0.6 * 0.4 / 10) ** 0.5
        )
        self.assertEqual(summary["classified_samples_count"], 7)
        self.assertEqual(summary["flush_task_weight"], 10)
        self.assertEqual(summary["cpu_s_by_logical_cpu"], {"1": 90e-9, "2": 10e-9})
        self.assertEqual(summary["named_residual"][0]["symbol"], "__memcpy_evex")

    # Scenario: the rules are recorded with a result.
    # Guarantees: every category is named, every rule states its pass, its
    # pattern and why, and the decision procedure is written down.
    def test_the_mapping_rules_are_retained(self):
        rules = performance.classification_rules()
        self.assertEqual(rules["categories"], list(performance.CPU_CATEGORIES))
        self.assertIn("innermost", rules["decision"])
        self.assertTrue(all({"pass", "category", "pattern", "why"} <= set(rule)
                            for rule in rules["rules"]))
        named = {rule["category"] for rule in rules["rules"]}
        self.assertEqual(named, set(performance.CPU_CATEGORIES) - {"unknown"})

    # Scenario: a logs and a metrics workload are prebuilt, and the logs one
    # is asked for again.
    # Guarantees: every request read back is byte for byte what
    # `build_request` returns, records included, and an identical input is
    # reused rather than rebuilt.
    def test_prebuilt_requests_are_the_built_requests(self):
        root = temporary_directory(self)
        for signal_name, workload in (
            ("logs", Workload(requests=7, records_per_request=5, body_bytes=64,
                              series=3, metrics_every=4)),
            ("metrics", Workload(requests=4, records_per_request=9, series=5,
                                 metrics_every=1)),
        ):
            prebuilt = performance.PrebuiltRequests.build(
                workload, signal_name, root / signal_name, processes=0
            )
            self.addCleanup(prebuilt.close)
            self.assertEqual(
                prebuilt.indexes, performance.request_indexes(workload, signal_name)
            )
            for index in prebuilt.indexes:
                self.assertEqual(prebuilt.request(index), build_request(workload, index))
            self.assertEqual(
                prebuilt.sidecar["records"],
                len(prebuilt.indexes) * workload.records_per_request,
            )
        logs = Workload(requests=7, records_per_request=5, body_bytes=64, series=3,
                        metrics_every=4)
        before = (root / "logs" / "sidecar.json").read_text(encoding="ascii")
        again = performance.PrebuiltRequests.build(logs, "logs", root / "logs", processes=0)
        self.addCleanup(again.close)
        self.assertEqual((root / "logs" / "sidecar.json").read_text(encoding="ascii"), before)

    # Scenario: the logs workload is sized for a profile at 5 us of engine
    # CPU per record over three repetitions.
    # Guarantees: the stage family's shape is kept, the logs requests are a
    # whole number of blocks carrying enough records for the sample target
    # with its margin, and the total is the smallest that carries them.
    def test_the_profile_is_sized_for_the_sample_target(self):
        workload = performance.attribution_workload(
            "logs-1k-stable", 5000.0, repetitions=3
        )
        base = performance.WORKLOAD_CONFIGS["logs-1k-stable"]["workload"]
        for field in ("records_per_request", "body_bytes", "series", "metrics_every", "seed"):
            self.assertEqual(getattr(workload, field), getattr(base, field))
        logs = performance.signal_request_count(workload, "logs")
        self.assertEqual(logs % performance.ATTRIBUTION_BLOCK_REQUESTS, 0)
        self.assertEqual(logs, len(performance.request_indexes(workload, "logs")))
        cpu_s = logs * workload.records_per_request * 5000.0 / 1e9
        expected = cpu_s * performance.PERF_FREQUENCY_HZ * 3
        self.assertGreaterEqual(
            expected,
            performance.ATTRIBUTION_MINIMUM_SAMPLES * performance.ATTRIBUTION_SAMPLE_MARGIN,
        )
        shorter = measurement.dataclasses.replace(workload, requests=workload.requests - 1)
        self.assertLess(performance.signal_request_count(shorter, "logs"), logs)
        with self.assertRaisesRegex(AssertionError, "positive reference"):
            _ = performance.attribution_workload("metrics-mixed", 0, repetitions=3)

    # Scenario: a profiled and a control lifetime take their requests from
    # one prebuilt input of 1,000 blocks, and from one of a single block.
    # Guarantees: the profiled lifetime sends everything, the control sends
    # the first third in whole blocks, and never less than one block.
    def test_the_control_lifetime_sends_a_whole_block_prefix(self):
        block = performance.ATTRIBUTION_BLOCK_REQUESTS
        indexes = list(range(1000 * block))
        self.assertEqual(performance.lifetime_indexes(indexes, profiled=True), indexes)
        control = performance.lifetime_indexes(indexes, profiled=False)
        self.assertEqual(control, indexes[: 333 * block])
        self.assertEqual(len(control) % block, 0)
        single = list(range(block))
        self.assertEqual(performance.lifetime_indexes(single, profiled=False), single)

    # Scenario: the campaign's pin, physical cores 0-7 with their SMT
    # siblings, is offered to an attribution family.
    # Guarantees: the observability core, the engine's four-core
    # reservation, the producer, the store and the profiler each own a
    # physical core, eight in all, and no reader is claimed; the read-back
    # after an engine stops uses only that engine's cores and siblings.
    def test_the_attribution_roles_fit_the_campaign_pin(self):
        groups = [[core, core + 16] for core in range(16)]
        pinned = list(range(8)) + list(range(16, 24))
        allocation = measurement.role_allocation(
            groups, pinned, [1], roles=measurement.CASE_ROLES["attribution"]
        )
        self.assertEqual(
            allocation,
            {
                "engine_observability": [0],
                "engine": [1],
                "engine_reserved": [2, 3, 4],
                "producer": [5],
                "store": [6],
                "profiler": [7],
            },
        )
        with mock.patch.object(os, "sched_getaffinity", return_value=set(pinned)):
            self.assertEqual(
                performance.oracle_cores(allocation, groups),
                [1, 2, 3, 4, 17, 18, 19, 20],
            )

    # Scenario: a lifetime's ledger is written under the plan's ledger
    # directory, with its journal, and then retired.
    # Guarantees: each repetition's ledgers are kept apart by run id, and
    # retiring one records its name, hash and size before the file and its
    # journal are deleted.
    def test_a_retired_ledger_keeps_its_identity(self):
        root = temporary_directory(self)
        plan = {"ledger_dir": str(root / "ledgers")}
        path = performance.ledger_path(plan, root / "attribution-x-r001", "control")
        self.assertEqual(path.parent, root / "ledgers" / "attribution-x-r001")
        ledger = Ledger(path)
        _ = ledger.add_request(1, "logs", b"wire", [("id", "log", "h")], send_ns=1)
        ledger.close()
        digest = measurement.file_digest(path)
        entry = performance.retire_ledger(path)
        self.assertEqual((entry["name"], entry["sha256"]), ("ledger-control.sqlite", digest))
        self.assertEqual(entry["retention"], "deleted")
        self.assertFalse(list(path.parent.iterdir()))

    # Scenario: two telemetry documents of one worker, before and after a
    # window with two flushes, the admin API serving the flush instrument
    # cumulatively.
    # Guarantees: the window's flushes are the difference of the readings,
    # a worker with no flush yet reads as none, and a reading that goes
    # backwards is an error, never a negative wall time.
    def test_flush_wall_time_is_a_difference_of_cumulative_readings(self):
        def document(sum_s, count, max_s):
            built = telemetry(5)
            if count is not None:
                built["metric_sets"][1]["metrics"].append(
                    {"name": "flush.duration",
                     "value": {"min": 0.1, "max": max_s, "sum": sum_s, "count": count}}
                )
            return built

        before = performance.flush_reading(document(0.0, None, 0.0))
        self.assertEqual(before["count"], 0)
        middle = performance.flush_reading(document(0.5, 1, 0.5))
        after = performance.flush_reading(document(1.25, 3, 0.5))
        window = performance.flush_window(middle, after)
        self.assertEqual(window["flush_count"], 2)
        self.assertAlmostEqual(window["flush_wall_s"], 0.75)
        with self.assertRaisesRegex(AssertionError, "backwards"):
            _ = performance.flush_window(after, middle)

    # Scenario: a worker thread and a helper thread over a two-second window,
    # and a thread that started inside it.
    # Guarantees: the worker's window splits into on-CPU, run-queue and
    # off-CPU fractions that add up to one, and every thread is listed with
    # its role.
    def test_thread_schedule_splits_the_window(self):
        before = {
            10: {"comm": "pipeline-defaul", "on_cpu_ns": 1_000, "runqueue_wait_ns": 0,
                 "timeslices_count": 1},
            11: {"comm": "tokio-rt", "on_cpu_ns": 0, "runqueue_wait_ns": 0,
                 "timeslices_count": 0},
        }
        after = {
            10: {"comm": "pipeline-defaul", "on_cpu_ns": 1_000 + 1_200_000_000,
                 "runqueue_wait_ns": 200_000_000, "timeslices_count": 9},
            11: {"comm": "tokio-rt", "on_cpu_ns": 100_000_000, "runqueue_wait_ns": 0,
                 "timeslices_count": 2},
            12: {"comm": "new", "on_cpu_ns": 5, "runqueue_wait_ns": 0,
                 "timeslices_count": 1},
        }
        schedule = performance.thread_schedule(before, after, 2_000_000_000, {10})
        worker = schedule["workers"]["10"]
        self.assertAlmostEqual(worker["on_cpu_ratio"], 0.6)
        self.assertAlmostEqual(worker["runqueue_wait_ratio"], 0.1)
        self.assertAlmostEqual(worker["off_cpu_ratio"], 0.3)
        roles = {thread["tid"]: thread["role"] for thread in schedule["threads"]}
        self.assertEqual(roles, {10: "worker", 11: "other", 12: "other"})
        self.assertTrue(schedule["threads"][2]["started_in_window"])

    # Scenario: the preflight profiles a busy process through a stand-in
    # perf that speaks the control-FIFO protocol, and again through one
    # that refuses the way perf_event_paranoid 4 makes perf refuse.
    # Guarantees: the preflight passes only when perf attached,
    # acknowledged enable and disable and produced an unwound sample; a
    # refusal is reported with perf's own words, and nothing is left
    # running either way.
    def test_the_preflight_proves_the_recording_or_names_the_refusal(self):
        perf = fake_perf(self)
        attached = performance.perf_preflight(temporary_directory(self), perf=perf)
        self.assertTrue(attached["attached"], attached)
        recording = temporary_directory(self)
        recorder = performance.PerfRecorder(recording, perf=perf)
        recorder.argv = performance.perf_record_argv(
            perf, 1, recording / "perf.data", recording / "perf.ctl", recording / "perf.ack"
        )
        self.assertNotIn(str(recording), json.dumps(recorder.as_json()))
        self.assertIn(
            f"fifo:{performance.PERF_DIR_TOKEN}/perf.ctl,{performance.PERF_DIR_TOKEN}/perf.ack",
            recorder.as_json()["argv"],
        )
        self.assertEqual(attached["unwound_samples_count"], 2)
        self.assertEqual(attached["record_returncode"], 0)
        with mock.patch.dict(os.environ, {"FAKE_PERF_MODE": "refuse"}):
            refused = performance.perf_preflight(temporary_directory(self), perf=perf)
        self.assertFalse(refused["attached"])
        self.assertIn("perf exited with 255", refused["reason"])
        self.assertIn("perf_event_paranoid", refused["perf_log_tail"])
        missing = performance.perf_preflight(
            temporary_directory(self), perf="/no/such/perf"
        )
        self.assertFalse(missing["attached"])
        self.assertIn("no perf executable", missing["reason"])

    # Scenario: the attribution command runs on a host where perf cannot
    # attach.
    # Guarantees: nothing is built, no store is started and no repetition
    # runs; the published index is skipped with a failed perf_attached
    # check, the preflight's evidence and acceptance marked incomplete, and
    # the command exits with the skipped status, not success.
    def test_a_host_that_cannot_profile_publishes_an_incomplete_index(self):
        report = temporary_directory(self)
        output = temporary_directory(self)
        refusal = {"attached": False, "perf_event_paranoid": 4,
                   "reason": "perf exited with 255 before acknowledging enable"}
        allocation = {"engine_observability": [0], "engine": [1],
                      "engine_reserved": [2, 3, 4], "producer": [5], "store": [6],
                      "profiler": [7]}
        engines = {"valid": True, "problems": []}
        with mock.patch.object(performance, "perf_preflight", return_value=refusal), \
                mock.patch.object(performance, "prepare_profiled_engine",
                                  return_value=engines), \
                mock.patch.object(measurement, "role_allocation", return_value=allocation), \
                mock.patch.object(measure, "prepare_build",
                                  side_effect=AssertionError("must not build")), \
                mock.patch.object(measurement.test_e2e, "DockerStore",
                                  side_effect=AssertionError("must not start a store")), \
                mock.patch.dict(os.environ, {"SERIES_MEASURE_LONG": "1"}):
            code = measure.main([
                "attribution", "--output-dir", str(output),
                "--option", f"report_dir={report}", "--option", "cores=[1]",
            ])
        self.assertEqual(code, measure.ATTRIBUTION_SKIPPED_EXIT)
        index = json.loads((report / "attribution.json").read_text(encoding="ascii"))
        self.assertEqual(index["status"], measurement.STATUS_SKIPPED)
        self.assertEqual(index["acceptance"]["mandatory"], "incomplete")
        self.assertEqual(index["preflight"]["perf_event_paranoid"], 4)
        self.assertEqual(
            index["environment"]["perf_event_paranoid"], performance.perf_event_paranoid()
        )
        checks = {entry["name"]: entry for entry in index["checks"]}
        self.assertEqual(checks["perf_attached"]["status"], measurement.STATUS_FAILED)
        self.assertEqual(index["run_files"], [])
        self.assertEqual(index["metrics"]["children_count"], 0)
        self.assertEqual(index["classification"]["categories"],
                         list(performance.CPU_CATEGORIES))

    def evidence(self, *, verified=True, spot_extract=800.0, pinned_extract=1400.0):
        """Two stage families as the reconciliation reads them."""
        def summary(cost):
            return {
                "metrics": dict({name: 1.0 for name in performance.STAGE_METRICS},
                                cpu_ns_per_record=cost,
                                output_bytes_per_input_record=100.0),
                "input_representation": "otlp_wire_bytes",
                "output_representation": "otap_arrow_records",
                "denominator": performance.DENOMINATOR,
                "repetitions": 3,
            }

        config = "metrics-mixed"
        costs = {"convert": 500.0, "extract": pinned_extract, "sort_seal": 350.0,
                 "merge": 100.0, "encode": 220.0, "upload": 15.0,
                 "otlp_noop": 300.0, "otlp_minio": 3000.0}
        modes = {"upload": "async", "otlp_minio": "async", "otlp_noop": "pipeline"}
        pinned = {
            f"{stage}/{modes.get(stage, 'isolated')}/{config}": summary(cost)
            for stage, cost in costs.items()
        }
        spot = {f"extract/isolated/{config}": summary(spot_extract)}
        return {
            "pinned": {"index": "stages.json", "present": True, "verified": verified,
                       "expected_sha256": "a" * 64, "file": {"sha256": "b" * 64},
                       "summaries": pinned},
            "spot": {"index": "stages-spot.json", "present": True, "verified": True,
                     "git": {"revision": "r1"}, "summaries": spot},
        }

    def aggregate(self):
        """One workload's aggregate as reconciliation reads it."""
        categories = {name: {"share_ratio": 0.0} for name in performance.CPU_CATEGORIES}
        categories["extraction"]["share_ratio"] = 0.25
        return {
            "workload_config_id": "metrics-mixed",
            "environment": {"git": {"revision": "r1"}},
            "metrics": dict(
                {f"{name}_cpu_ns_per_record": 0.0 for name in performance.CPU_CATEGORIES},
                engine_cpu_ns_per_record=3200.0,
                extraction_cpu_ns_per_record=800.0,
                upload_wait_s=4.0,
                flush_wall_s=6.0,
            ),
            "checks": [
                measurement.check(name, measurement.CHECK_HARD, measurement.STATUS_PASSED)
                for name in performance.BINDING_RECONCILIATION_CHECKS
            ],
            "observations": {
                "pooled_profile": {
                    "categories": categories,
                    "allocator_callers_share_ratio": {"extraction": 0.025},
                },
                "stage_agreement": {"repetitions": [
                    {"error_ratio": 0.003, "unexplained_ratio": 0.005}
                ]},
            },
        }

    # Scenario: a metrics workload whose binding checks all passed is joined
    # with the pinned family, whose extraction cost is far from the engine's,
    # and with a later spot family that re-measured extraction.
    # Guarantees: the reconciliation is valid on the binding rule alone; the
    # stage comparison is labelled descriptive and uses the pinned family as
    # its reference, never the spot family in its place; the allocator
    # samples a stage caused are added back before comparing; an
    # out-of-band row is explained only by the published spot measurement
    # bringing it into the band, and otherwise says it is unexplained; and
    # no byte rate is added across stages.
    def test_reconciliation_joins_stage_evidence_by_workload(self):
        reconciliation = performance.reconcile_attribution(
            [self.aggregate()], self.evidence(pinned_extract=2000.0)
        )
        self.assertTrue(reconciliation["valid"], reconciliation["problems"])
        self.assertIn("10%", reconciliation["binding_rule"])
        self.assertFalse(reconciliation["descriptive_stage_comparison"]["gating"])
        self.assertTrue(reconciliation["spot_revision_matches"])
        workload = reconciliation["workloads"]["metrics-mixed"]
        self.assertTrue(all(workload["binding"]["checks"].values()))
        rows = {row["row"]: row for row in workload["descriptive_stage_comparison"]}
        extraction = rows["extraction"]
        self.assertEqual(extraction["stages"][0]["reference"], "pinned")
        self.assertEqual(extraction["reference_cpu_ns_per_record"], 2000.0)
        self.assertEqual(extraction["supplementary_spot_cpu_ns_per_record"], 800.0)
        self.assertAlmostEqual(
            extraction["attributed_with_allocator_cpu_ns_per_record"], 800.0 + 0.025 * 3200.0
        )
        self.assertEqual(extraction["verdict"], "outside_band")
        self.assertEqual(extraction["explanation"]["status"], "explained")
        self.assertEqual(extraction["explanation"]["evidence"]["index"], "stages-spot.json")
        conversion = rows["conversion"]
        self.assertEqual(conversion["verdict"], "outside_band")
        self.assertEqual(conversion["explanation"]["status"], "unexplained")
        total = rows["total"]
        self.assertEqual(total["reference_cpu_ns_per_record"], 3300.0)
        self.assertEqual(total["verdict"], "within_band")
        self.assertIsNone(total["explanation"])
        for row in rows.values():
            self.assertFalse([key for key in row if "bytes" in key])
            for stage in row["stages"]:
                self.assertIn("output_representation", stage)

    # Scenario: the pinned family on disk no longer has the recorded hash,
    # a joined stage is missing from it, and the aggregate passed none of
    # the binding checks.
    # Guarantees: each makes the reconciliation invalid and names why.
    def test_an_unverified_or_incomplete_reconciliation_is_invalid(self):
        evidence = self.evidence(verified=False)
        del evidence["pinned"]["summaries"]["merge/isolated/metrics-mixed"]
        aggregate = self.aggregate()
        aggregate["checks"] = []
        reconciliation = performance.reconcile_attribution([aggregate], evidence)
        self.assertFalse(reconciliation["valid"])
        problems = " ".join(reconciliation["problems"])
        self.assertIn("hashes to", problems)
        self.assertIn("merge/isolated/metrics-mixed", problems)
        self.assertIn("binding checks not passed", problems)
        for name in performance.BINDING_RECONCILIATION_CHECKS:
            self.assertIn(name, problems)

    def child(self, repetition, classified, cpu=3000.0):
        """One profiled repetition that passed every gate."""
        metrics = dict(
            {f"{name}_cpu_ns_per_record": cpu / 10 for name in performance.CPU_CATEGORIES},
            engine_cpu_ns_per_record=cpu,
            control_engine_cpu_ns_per_record=cpu * 0.98,
            throughput_records_per_s=1e5,
            control_throughput_records_per_s=1.02e5,
            classified_samples_count=classified,
            flush_wall_s=5.0,
            upload_wait_s=2.0,
            peak_rss_bytes=1e8,
        )
        categories = {name: {"weight": 10, "samples_count": classified // 8,
                             "share_ratio": 0.1, "share_ci95_ratio": 0.01}
                      for name in performance.CPU_CATEGORIES}
        categories["unknown"]["samples_count"] = 0
        checks = passed_hard_checks() + [
            measurement.check(name, measurement.CHECK_HARD, measurement.STATUS_PASSED)
            for name in ("perf_recorded", "stage_agreement_error",
                         "attribution_exclusive", "stage_agreement_unexplained")
        ]
        return measured_result(
            run_id=f"attribution-metrics-mixed-strict-minio-c1-w15-r{repetition:03d}",
            case="attribution",
            metrics=metrics,
            metric_directions={
                name: performance.ATTRIBUTION_METRIC_DIRECTIONS[name] for name in metrics
            },
            checks=checks,
            repetition=repetition,
            workload_config_id="metrics-mixed",
            observations={
                "attribution": {"categories": categories,
                                "allocator_callers": {"extraction": 5},
                                "named_residual": [{"symbol": "x", "weight": 1}]},
                "profile_overhead": {"cpu_per_record_ratio": 0.02},
            },
        )

    # Scenario: three profiled repetitions of one workload carry 12,000
    # classified samples in all, and three others carry 6,000.
    # Guarantees: the aggregate takes the medians, pools the profile, and
    # passes the sample gate only with 10,000 or more; a first valid
    # aggregate creates its baseline, and a short one never does.
    def test_the_family_needs_ten_thousand_classified_samples(self):
        plan = {"profile": True, "repetitions": 3, "minimum_samples": 10_000,
                "cores": [1], "family_ordinal": 1,
                "evidence": {"pinned": {"index": "stages.json", "present": True,
                                        "verified": True, "file": {"sha256": "a"},
                                        "expected_sha256": "a"}}}

        def written(children, directory):
            """The children, each also written where the aggregate hashes it."""
            for child in children:
                _ = measurement.write_result(directory / f"{child['run_id']}.json", child)
            return children

        output = temporary_directory(self)
        enough = performance.aggregate_attribution(
            written([self.child(index, 4000) for index in (1, 2, 3)], output),
            plan=plan, output_dir=output,
        )
        checks = {entry["name"]: entry for entry in enough["checks"]}
        self.assertEqual(checks["classified_samples_sufficient"]["status"],
                         measurement.STATUS_PASSED)
        self.assertEqual(enough["status"], measurement.STATUS_PASSED, enough["checks"])
        self.assertEqual(enough["metrics"]["engine_cpu_ns_per_record"], 3000.0)
        self.assertEqual(len(enough["baseline_files"]), 1)
        self.assertEqual(
            enough["observations"]["pooled_profile"]["classified_samples_count"], 12000
        )
        other = temporary_directory(self)
        short = performance.aggregate_attribution(
            written([self.child(index, 2000) for index in (4, 5, 6)], other),
            plan=plan, output_dir=other,
        )
        checks = {entry["name"]: entry for entry in short["checks"]}
        self.assertEqual(checks["classified_samples_sufficient"]["status"],
                         measurement.STATUS_FAILED)
        self.assertEqual(short["baseline_files"], [])

    # Scenario: profiles whose categories and named residual hold all of the
    # measured engine CPU, 85 percent of it, and all of it with a quarter
    # named residual.
    # Guarantees: the reconciliation error is the relative difference from
    # the measured CPU and unexplained CPU is the residual plus what no
    # sample covered, so the plan's 10 and 20 percent limits catch both.
    def test_stage_agreement_is_the_plan_rule(self):
        full = performance.stage_agreement(1000, 10, 1000)
        self.assertEqual(full, {"error_ratio": 0.0, "unexplained_ratio": 0.01})
        short = performance.stage_agreement(850, 10, 1000)
        self.assertAlmostEqual(short["error_ratio"], 0.15)
        self.assertAlmostEqual(short["unexplained_ratio"], 0.16)
        self.assertGreater(short["error_ratio"], performance.STAGE_AGREEMENT_ERROR_LIMIT)
        residual = performance.stage_agreement(1000, 250, 1000)
        self.assertGreater(residual["unexplained_ratio"], performance.UNEXPLAINED_CPU_LIMIT)
        self.assertEqual(
            performance.stage_agreement(10, 0, 0),
            {"error_ratio": None, "unexplained_ratio": None},
        )

    # Scenario: three repetitions pass every gate, but the pinned reference
    # the family is reconciled against does not verify.
    # Guarantees: the reference is a binding gate of the aggregate itself, so
    # it fails there and no baseline is written.
    def test_no_baseline_without_the_verified_reference(self):
        output = temporary_directory(self)
        plan = {"profile": True, "repetitions": 3, "minimum_samples": 10_000,
                "cores": [1], "family_ordinal": 1,
                "evidence": {"pinned": {"index": "stages.json", "present": True,
                                        "verified": False}}}
        children = [self.child(index, 4000) for index in (1, 2, 3)]
        for child in children:
            _ = measurement.write_result(output / f"{child['run_id']}.json", child)
        aggregate = performance.aggregate_attribution(children, plan=plan, output_dir=output)
        checks = {entry["name"]: entry for entry in aggregate["checks"]}
        self.assertEqual(checks["reference_family_verified"]["status"],
                         measurement.STATUS_FAILED)
        self.assertEqual(aggregate["baseline_files"], [])
        self.assertEqual(aggregate["status"], measurement.STATUS_FAILED)

    # Scenario: the ledger directory is asked for on an ext4 disk while
    # /dev/shm is tmpfs, on tmpfs itself, and on a host with no memory file
    # system at all.
    # Guarantees: the file system type is read from mountinfo and recorded;
    # a disk directory falls back to /dev/shm, a tmpfs one is kept, and with
    # neither the family is refused rather than throttled.
    def test_ledgers_live_on_a_memory_file_system(self):
        root = temporary_directory(self)
        mountinfo = root / "mountinfo"
        mountinfo.write_text(
            "22 1 8:1 / / rw - ext4 /dev/sda1 rw\n"
            "23 22 0:5 / /dev/shm rw - tmpfs tmpfs rw\n"
            "24 22 0:6 / /tmp rw - tmpfs tmpfs rw\n",
            encoding="ascii",
        )
        moved = performance.memory_ledger_dir("/var/tmp/ledgers", mountinfo=mountinfo)
        self.assertEqual((moved["fstype"], moved["fallback"]), ("tmpfs", True))
        self.assertEqual(moved["directory"], "/dev/shm/ledgers")
        kept = performance.memory_ledger_dir("/tmp/ledgers", mountinfo=mountinfo)
        self.assertEqual((kept["mount_point"], kept["fallback"]), ("/tmp", False))
        disk_only = root / "disk-only"
        disk_only.write_text("22 1 8:1 / / rw - ext4 /dev/sda1 rw\n", encoding="ascii")
        with self.assertRaisesRegex(AssertionError, "memory file system"):
            _ = performance.memory_ledger_dir("/var/tmp/ledgers", mountinfo=disk_only)

    def engine_proof(self, *, clean=True, same_symbols=True):
        """prepare_profiled_engine with cargo, nm and git replaced."""
        root = temporary_directory(self)
        (root / "target" / "release").mkdir(parents=True)
        canonical = root / "target" / "release" / "df_engine"
        profiled = root / "target" / "release" / "df_engine-perf"
        commands = []

        def run(argv, **kwargs):
            commands.append(argv)
            canonical.write_bytes(b"relinked" if argv[1] == "rustc" else b"canonical")
            return subprocess.CompletedProcess(argv, 0)

        def symbols(path, nm="nm"):
            digest = "same" if same_symbols or Path(path) == canonical else "other"
            return {"count": 3, "sha256": digest, "tool": "nm"}

        def build(path):
            return {"profile": "release", "features": "f", "allocator": "jemalloc",
                    "toolchain": "rustc 1", "binary": str(path),
                    "binary_sha256": measurement.file_digest(path)}

        with mock.patch.object(measurement.test_e2e, "WORKSPACE", root), \
                mock.patch.dict(os.environ, {performance.ATTRIBUTION_ENGINE_ENV: str(profiled)}), \
                mock.patch.object(performance, "rust_tree_status",
                                  return_value={"clean": clean, "changes": [] if clean else [" M a.rs"]}), \
                mock.patch.object(performance.subprocess, "run", side_effect=run), \
                mock.patch.object(performance, "function_symbols", side_effect=symbols), \
                mock.patch.object(performance, "rustc_version", return_value="rustc 1 -vV"), \
                mock.patch.object(performance, "unwind_layout",
                                  return_value={"compatible": True, "skewed_executable_segments": []}), \
                mock.patch.object(measurement, "engine_build", side_effect=build), \
                mock.patch.object(measurement, "git_provenance",
                                  return_value={"revision": "r", "dirty": not clean}):
            facts = performance.prepare_profiled_engine(
                root / "log", lease_path=root / "lease",
            )
        return facts, commands

    # Scenario: the attribution builds its two engines from a clean tree with
    # identical function symbols, from a dirty tree, and into binaries whose
    # function symbols differ.
    # Guarantees: the canonical build, the one-flag relink and the restore
    # run in order under the lease; only a clean tree whose two binaries list
    # the same functions is valid; the exact flag difference is recorded;
    # and each refusal says why.
    def test_the_profiled_engine_is_proven_the_canonical_one(self):
        facts, commands = self.engine_proof()
        self.assertTrue(facts["valid"], facts["problems"])
        self.assertEqual([argv[1] for argv in commands], ["build", "rustc", "build"])
        self.assertEqual(commands[1][-2:], ["-C", performance.PROFILED_LINK_ARG])
        self.assertEqual(facts["rustflags_difference"]["profiled"],
                         ["-C", performance.PROFILED_LINK_ARG])
        self.assertEqual(facts["canonical"]["function_symbols"],
                         facts["profiled"]["function_symbols"])
        dirty, commands = self.engine_proof(clean=False)
        self.assertFalse(dirty["valid"])
        self.assertIn("uncommitted", " ".join(dirty["problems"]))
        self.assertEqual(commands, [])
        different, _ = self.engine_proof(same_symbols=False)
        self.assertFalse(different["valid"])
        self.assertIn("function symbols differ", " ".join(different["problems"]))

    # Scenario: the reader's rows fail part-way through a second load, after
    # a first load committed.
    # Guarantees: the failed load is rolled back whole -- the first load's
    # rows are what the ledger still holds -- and the connection is left
    # outside any transaction, usable by the comparison that follows.
    def test_a_failed_read_back_load_is_rolled_back(self):
        ledger = Ledger(temporary_directory(self) / "ledger.sqlite")
        self.addCleanup(ledger.close)
        rows = [(f"id{index}", "logs", "h") for index in range(5)]
        self.assertEqual(measurement._load_actual(ledger, iter(rows)), 5)

        def failing():
            for index in range(7):
                yield (f"new{index}", "logs", "h")
            raise OSError("reader failed")

        with mock.patch.object(measurement, "ORACLE_BATCH", 3):
            with self.assertRaisesRegex(OSError, "reader failed"):
                _ = measurement._load_actual(ledger, failing())
        self.assertFalse(ledger.connection.in_transaction)
        kept = ledger.connection.execute("SELECT record_id FROM actual ORDER BY 1").fetchall()
        self.assertEqual([row[0] for row in kept], [row[0] for row in rows])

    # Scenario: a lifetime fails immediately after its engine started, while
    # constructing its phase.
    # Guarantees: the engine is closed anyway, so no failure can leave an
    # engine behind on a measured core, and the failure propagates.
    def test_an_early_lifetime_failure_closes_its_engine(self):
        closed = []

        class FakeEngine:
            pid = 4242
            config_sha256 = "c"

            def __init__(self, *args, **kwargs):
                pass

            def close(self):
                closed.append(True)

        class FailingCommand:
            @staticmethod
            def EnginePhase(*args, **kwargs):
                raise RuntimeError("phase failed")

        prebuilt = mock.Mock(workload=Workload())
        plan = {"inputs": {"logs-1k-stable": {"prebuilt": prebuilt}}, "storage": {},
                "merge": {}, "provenance": {"build": {"binary": "/bin/true"}},
                "ledger_dir": str(temporary_directory(self))}
        spec = measure.harness_local_spec()
        controls = mock.Mock()
        with mock.patch.object(measurement.test_e2e, "Engine", FakeEngine), \
                mock.patch.object(performance, "_command", return_value=FailingCommand):
            with self.assertRaisesRegex(RuntimeError, "phase failed"):
                _ = performance.attribution_lifetime(
                    "control", plan, {"config_id": "logs-1k-stable"}, spec,
                    {"config": {}, "ephemeral_values": {}}, temporary_directory(self),
                    controls, edge="start", last=True, profile=False,
                )
        self.assertEqual(closed, [True])
        controls.unwatch_workers.assert_called()

    # Scenario: another agent's compile invalidates a repetition: the host
    # shows a build for two scans and then none, and earlier children with
    # ordinals up to 6 are published in two directories.
    # Guarantees: only a failed no_concurrent_build marks a child as
    # invalidated by a build; the family waits until the host has been
    # build-free for a whole quiet period before running it again; and the
    # rerun takes an ordinal no published child has used.
    def test_a_build_invalidated_repetition_is_rerun_on_a_quiet_host(self):
        failed = {"checks": [measurement.check(
            "no_concurrent_build", measurement.CHECK_HARD, measurement.STATUS_FAILED)]}
        other = {"checks": [measurement.check(
            "rss_reconciliation", measurement.CHECK_HARD, measurement.STATUS_FAILED)]}
        self.assertTrue(performance.invalidated_by_build(failed))
        self.assertFalse(performance.invalidated_by_build(other))
        scans = iter([[{"pid": 7, "comm": "cargo"}]] * 2 + [[]] * 1000)
        waited = performance.wait_for_quiet_host(
            quiet_s=0.2, deadline_s=30, scan=lambda: next(scans)
        )
        self.assertGreaterEqual(waited["waited_s"], 0.2)
        self.assertEqual(waited["last_builds_seen"], [{"pid": 7, "comm": "cargo"}])
        with self.assertRaisesRegex(AssertionError, "without a build"):
            _ = performance.wait_for_quiet_host(
                quiet_s=5, deadline_s=0.3, scan=lambda: [{"pid": 8, "comm": "rustc"}]
            )
        report = temporary_directory(self)
        local = temporary_directory(self)
        (report / "attribution-logs-1k-stable-strict-minio-c1-w15-r003.json").write_text("{}")
        (local / "attribution-metrics-mixed-strict-minio-c1-w15-r006.json").write_text("{}")
        self.assertEqual(performance.attribution_child_ordinal(report, local), 7)

    # Scenario: the family runs one workload of two repetitions; the first
    # repetition's run is valid, and every attempt at the second -- the first
    # run and all three reruns -- is invalidated by a concurrent build.
    # Guarantees: all four invalidated attempts are published as invalidated
    # and none reaches the aggregate, which sees the valid repetition alone;
    # the exhausted repetition is simply missing.
    def test_exhausted_build_retries_aggregate_nothing_invalidated(self):
        invalid = {"checks": [measurement.check(
            "no_concurrent_build", measurement.CHECK_HARD, measurement.STATUS_FAILED)]}
        runs = []

        def run_child(plan, job, output_dir, report_dir):
            runs.append(job)
            child = dict(
                {"checks": []} if job["repetition"] == 1 else invalid,
                run_id=f"attribution-metrics-mixed-r{job['ordinal']:03d}",
                repetition=job["repetition"],
                workload_config_id=job["config_id"],
            )
            return child

        aggregated = []

        def aggregate(members, *, plan, output_dir):
            aggregated.extend(members)
            return {"workload_config_id": members[0]["workload_config_id"],
                    "run_id": "agg", "status": measurement.STATUS_FAILED,
                    "checks": [], "baseline_files": []}

        published = {}

        def publish(spec, plan, children, aggregates, output_dir, report_dir,
                    started, *, preflight, invalidated=()):
            published.update(children=children, aggregates=aggregates,
                             invalidated=list(invalidated))
            return {"status": "failed"}

        store = mock.MagicMock(storage={"s3": {}}, endpoint="http://x", container="c")
        store.__enter__.return_value = store
        prebuilt = mock.Mock(workload=Workload(), close=mock.Mock())
        engines = {"valid": True, "problems": [], "profiled": {"binary": "b"},
                   "canonical": {}, "rustflags_difference": {}, "commands": {},
                   "git": {"revision": "r"}}
        allocation = {"engine_observability": [0], "engine": [1],
                      "engine_reserved": [2, 3, 4], "producer": [5], "store": [6],
                      "profiler": [7]}
        spec = performance.attribution_spec(cores=[1])
        with mock.patch.object(measurement, "role_allocation", return_value=allocation), \
                mock.patch.object(performance, "prepare_profiled_engine", return_value=engines), \
                mock.patch.object(performance, "perf_preflight", return_value={"attached": True}), \
                mock.patch.object(performance.PrebuiltRequests, "build", return_value=prebuilt), \
                mock.patch.object(measurement.test_e2e, "DockerStore", return_value=store), \
                mock.patch.object(performance, "container_pid", return_value=1), \
                mock.patch.object(performance, "pin_container", return_value=True), \
                mock.patch.object(measurement, "build_activity", return_value=[]), \
                mock.patch.object(performance, "wait_for_quiet_host", return_value={}), \
                mock.patch.object(performance, "run_attribution_child", side_effect=run_child), \
                mock.patch.object(performance, "aggregate_attribution", side_effect=aggregate), \
                mock.patch.object(performance, "publish_attribution", side_effect=publish):
            _ = performance.run_attribution(
                spec, temporary_directory(self), report_dir=temporary_directory(self),
                configs=["metrics-mixed"], repetitions=2, cpu_ns_per_record=1000.0,
            )
        self.assertEqual([job["repetition"] for job in runs], [1, 2, 2, 2, 2])
        self.assertEqual([job["attempt"] for job in runs if job["repetition"] == 2],
                         [1, 2, 3, 4])
        self.assertEqual(len({job["ordinal"] for job in runs}), 5)
        self.assertEqual(len(published["invalidated"]), 4)
        self.assertEqual([child["repetition"] for child in aggregated], [1])
        invalid_ids = {child["run_id"] for child in published["invalidated"]}
        self.assertFalse(invalid_ids & {child["run_id"] for child in aggregated})
        self.assertFalse(invalid_ids & {child["run_id"] for child in published["children"]})

    # Scenario: a workload's aggregate is built from repetitions 1 and 3 of
    # three, repetition 2 having no valid run.
    # Guarantees: completeness is a hard gate that names the missing
    # repetition, and no baseline is written.
    def test_a_missing_repetition_fails_completeness(self):
        output = temporary_directory(self)
        plan = {"profile": True, "repetitions": 3, "minimum_samples": 1,
                "cores": [1], "family_ordinal": 1,
                "evidence": {"pinned": {"index": "stages.json", "present": True,
                                        "verified": True}}}
        children = [self.child(index, 6000) for index in (1, 3)]
        for child in children:
            _ = measurement.write_result(output / f"{child['run_id']}.json", child)
        aggregate = performance.aggregate_attribution(children, plan=plan, output_dir=output)
        checks = {entry["name"]: entry for entry in aggregate["checks"]}
        self.assertEqual(checks["repetitions_complete"]["status"], measurement.STATUS_FAILED)
        self.assertIn("[2]", checks["repetitions_complete"]["detail"])
        self.assertEqual(aggregate["baseline_files"], [])

    # Scenario: the Rust tree is clean when the engine build starts waiting
    # for the lease, but has an edit once the lease is held; and in another
    # run the revision moves while the builds run.
    # Guarantees: the source is re-checked after the lease and after the
    # builds; a change in either window refuses the engines, and a tree that
    # changed while waiting is never built.
    def test_a_tree_that_moves_around_the_build_is_refused(self):
        clean = {"clean": True, "changes": [], "revision": "r"}
        with mock.patch.object(performance, "source_state",
                               return_value={"clean": False, "changes": [" M a.rs"],
                                             "revision": "r"}):
            waited, commands = self.engine_proof()
        self.assertFalse(waited["valid"])
        self.assertIn("while waiting for the lease", " ".join(waited["problems"]))
        self.assertEqual(commands, [])
        with mock.patch.object(performance, "source_state",
                               side_effect=[clean, dict(clean, revision="s")]):
            moved, commands = self.engine_proof()
        self.assertFalse(moved["valid"])
        self.assertIn("during the builds, the revision moved", " ".join(moved["problems"]))
        self.assertEqual(len(commands), 3)

    # Scenario: a git repository whose rust/ directory has one committed file
    # and one untracked .rs file.
    # Guarantees: the untracked source makes the tree unclean, since it can
    # be compiled in, and it is named.
    def test_an_untracked_rust_file_makes_the_tree_unclean(self):
        root = temporary_directory(self)
        (root / "rust").mkdir()
        (root / "rust" / "lib.rs").write_text("pub fn a() {}\n")

        def git(*arguments):
            return subprocess.run(
                ["git", "-c", "user.email=t@t", "-c", "user.name=t", *arguments],
                cwd=root, capture_output=True, text=True, check=True,
            )

        _ = git("init", "-q")
        _ = git("add", "rust/lib.rs")
        _ = git("commit", "-q", "-m", "init")
        with mock.patch.object(measurement, "REPO_ROOT", root):
            self.assertTrue(performance.rust_tree_status()["clean"])
            (root / "rust" / "new.rs").write_text("pub fn b() {}\n")
            status = performance.rust_tree_status()
        self.assertFalse(status["clean"])
        self.assertIn("?? rust/new.rs", status["changes"])

    # Scenario: a stages family is asked for a filtered set of stages or
    # workloads, or the complete set under another name.
    # Guarantees: a filtered family must publish under its own index and is
    # checked against what it asked for; the complete family is only ever
    # `stages.json`; unknown names are refused.
    def test_a_spot_family_never_replaces_the_family_of_record(self):
        with self.assertRaisesRegex(AssertionError, "spot family"):
            _ = performance.family_scope(["metrics-mixed"], ["extract"])
        spot = performance.family_scope(
            ["metrics-mixed"], ["extract", "upload"], "stages-spot"
        )
        self.assertEqual(spot["kind"], "spot")
        self.assertEqual(spot["stages"], ["extract", "upload"])
        full = performance.family_scope(list(performance.WORKLOAD_CONFIGS), None)
        self.assertEqual((full["kind"], full["index"]), ("full", "stages"))
        with self.assertRaisesRegex(AssertionError, "family of record"):
            _ = performance.family_scope(
                list(performance.WORKLOAD_CONFIGS), None, "stages-spot"
            )
        with self.assertRaisesRegex(AssertionError, "unknown stages"):
            _ = performance.family_scope(["metrics-mixed"], ["nope"], "stages-spot")

    # Scenario: the attribution subcommand is asked for without the long
    # opt-in, and the command line is inspected.
    # Guarantees: it is registered as a long subcommand and gated the same
    # way as any other.
    def test_attribution_is_a_long_subcommand(self):
        self.assertIn("attribution", measure.LONG_COMMANDS)
        environment = dict(os.environ)
        environment.pop("SERIES_MEASURE_LONG", None)
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(
                measure.main(["attribution", "--output-dir", str(temporary_directory(self))]),
                2,
            )


class GeneratorContracts(unittest.TestCase):
    """The template requests and the aggregate oracle that reads them."""

    WORKLOAD = Workload(requests=1, records_per_request=4, body_bytes=64, series=10,
                        metrics_every=3, series_scope="record")

    # Scenario: two requests of each signal are built from their templates.
    # Guarantees: each record carries its sequence number in its timestamp
    # (and, for logs, its record id), its series slot as the workload
    # defines it, and the bytes parse as OTLP.
    def test_template_requests_carry_their_sequence(self):
        source = generator.TemplateRequests(self.WORKLOAD)
        for index in (1, 2, 3, 6):
            records = generator.expected_records(source, index)
            self.assertEqual(sorted(records), [index * 4 + point for point in range(4)])
            for seq, record in records.items():
                self.assertEqual(record["time_unix_nano"], generator.time_of(seq))
                slot = self.WORKLOAD.slot(index, seq - index * 4)
                if "body" in record:
                    self.assertTrue(record["body"].startswith(
                        measurement.stable_id(self.WORKLOAD.seed, index, seq - index * 4, "log")))
                    self.assertEqual(record["logger"], f"series.logger.{slot:012d}")
                else:
                    self.assertEqual(record["slot"], f"{slot:012d}")
        self.assertNotEqual(source.request(1)[1][-200:], source.request(2)[1][-200:])

    def coverage(self, seqs, acked, failed=()):
        """`request_coverage` over stored sequence numbers `seqs`."""
        import duckdb

        with duckdb.connect() as db:
            db.execute("CREATE TABLE s (seq HUGEINT)")
            db.executemany("INSERT INTO s VALUES (?)", [(seq,) for seq in seqs])
            return generator.request_coverage(db, "s", 4, list(acked), list(failed))

    # Scenario: three acknowledged requests of four records are stored whole,
    # then with one request lost, one record duplicated, and a request that
    # was never sent stored.
    # Guarantees: each defect is counted on its own and a whole store has
    # none, with the sequence sum of the acknowledged requests.
    def test_request_coverage_finds_lost_duplicated_and_foreign_records(self):
        whole = [r * 4 + p for r in (1, 2, 3) for p in range(4)]
        report = self.coverage(whole, [1, 2, 3])
        self.assertEqual((report["missing_record_count"], report["duplicate_record_count"],
                          report["unexpected_record_count"]), (0, 0, 0))
        self.assertEqual(report["seq_sum"], report["expected_seq_sum_of_acknowledged"])
        lost = self.coverage([seq for seq in whole if seq // 4 != 2], [1, 2, 3])
        self.assertEqual((lost["lost_requests_count"], lost["missing_record_count"]), (1, 4))
        duplicated = self.coverage(whole + [5], [1, 2, 3])
        self.assertEqual(duplicated["duplicate_record_count"], 1)
        foreign = self.coverage(whole + [36, 37], [1, 2, 3])
        self.assertEqual(foreign["unexpected_record_count"], 2)
        failed = self.coverage(whole + [36], [1, 2, 3], failed=[9])
        self.assertEqual((failed["unexpected_record_count"],
                          failed["stored_failed_records_count"]), (0, 1))

    # Scenario: the stored logs rows of one acknowledged request, whole, and
    # with one body corrupted.
    # Guarantees: the field-by-field sample passes the whole rows and names
    # the corrupted record.
    def test_a_corrupted_record_fails_the_sample(self):
        import duckdb

        source = generator.TemplateRequests(self.WORKLOAD)
        records = generator.expected_records(source, 1)
        for corrupt in (False, True):
            root = temporary_directory(self)
            values = root / "v=1/signal=logs/dataset=values"
            series = root / "v=1/signal=logs/dataset=series"
            values.mkdir(parents=True)
            series.mkdir(parents=True)
            with duckdb.connect() as db:
                db.execute("CREATE TABLE v (series_id BLOB, time_unix_nano BIGINT, body VARCHAR)")
                db.execute("CREATE TABLE s (series_id BLOB, emitted_at BIGINT, "
                           "attrs MAP(VARCHAR, VARCHAR))")
                for seq, record in sorted(records.items()):
                    body = record["body"][:-1] + "!" if corrupt and seq == 5 else record["body"]
                    key = f"s{seq}".encode("ascii")
                    db.execute("INSERT INTO v VALUES (?, ?, ?)",
                               [key, record["time_unix_nano"], body])
                    db.execute("INSERT INTO s VALUES (?, 1, MAP(['logger.name'], [?]))",
                               [key, record["logger"]])
                db.execute(f"COPY v TO '{values / 'part.parquet'}' (FORMAT PARQUET)")
                db.execute(f"COPY s TO '{series / 'part.parquet'}' (FORMAT PARQUET)")
                report = generator.compare_sample(db, root, source, [1], "logs", count=40)
            self.assertEqual(report["sampled_count"], 4)
            if corrupt:
                self.assertEqual([m["seq"] for m in report["mismatches"]], [5])
                self.assertEqual(report["mismatches"][0]["fields"], ["body"])
            else:
                self.assertEqual(report["mismatch_count"], 0)


class CapacityContracts(unittest.TestCase):
    """The arithmetic and the decisions of the capacity family."""

    # Scenario: a cell's one-second search won 128k and its 15 s search 8.5k.
    # Guarantees: the default-window confirmation runs at 8.5k, never at the
    # one-second winner, and without a 15 s search it has no rate.
    def test_default_window_confirmation_runs_at_the_default_window_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            state = capacity.FamilyState(directory)

            def entry(purpose, rate, verdict):
                return {"run_id": f"r{rate}", "cell": "minio-c1",
                        "workload_id": capacity.PRIMARY_WORKLOAD, "purpose": purpose,
                        "rate": rate, "verdict": verdict}

            state.document["trials"] = [
                entry("search", 128000, "sustainable"), entry("search", 136000, "unsustainable")]
            self.assertIsNone(capacity.default_window_rate(state, "minio-c1"))
            state.document["trials"] += [
                entry("search_default_window", 8000, "sustainable"),
                entry("search_default_window", 9000, "unsustainable"),
                entry("search_default_window", 8500, "sustainable"),
            ]
            self.assertEqual(capacity.default_window_rate(state, "minio-c1"), 8500)

    # Scenario: two workers complete 12,000 unique records and 3MB of objects in 3s.
    # Guarantees: records, input bytes and output bytes retain distinct denominators.
    def test_capacity_units(self):
        measured = rates(12000, 12000000, 3000000, 3.0, 2)
        self.assertEqual(measured["records_per_s"], 4000)
        self.assertEqual(measured["records_per_s_per_core"], 2000)
        self.assertEqual(measured["input_bytes_per_s"], 4000000)
        self.assertEqual(measured["object_bytes_per_s"], 1000000)

    # Scenario: a rate is computed over a zero duration or zero workers.
    # Guarantees: the arithmetic refuses rather than dividing by zero.
    def test_capacity_units_refuse_empty_denominators(self):
        with self.assertRaises(ValueError):
            _ = rates(1, 1, 1, 0.0, 1)
        with self.assertRaises(ValueError):
            _ = rates(1, 1, 1, 1.0, 0)

    # Scenario: a search doubles from 1,000 records/s, fails at 64,000 and
    # bisects the bracket.
    # Guarantees: it doubles while sustainable, bisects between the highest
    # sustainable and lowest unsustainable rate, and stops at a 10% bracket.
    def test_search_doubles_then_bisects_to_ten_percent(self):
        trials = []
        ceiling = 45_000
        while True:
            rate = capacity.next_search_rate(trials)
            if rate is None:
                break
            trials.append((rate, "sustainable" if rate <= ceiling else "unsustainable"))
        rates_run = [rate for rate, _verdict in trials]
        self.assertEqual(rates_run[:7], [1000, 2000, 4000, 8000, 16000, 32000, 64000])
        decision = capacity.search_decision(trials)
        self.assertTrue(decision["bracketed"])
        self.assertEqual(decision["kind"], "maximum")
        self.assertLessEqual(decision["bracket_width_ratio"], 0.10)
        self.assertLessEqual(decision["sustainable_records_per_s"], ceiling)
        self.assertGreater(decision["unsustainable_records_per_s"], ceiling)

    # Scenario: a search settled on 320,000 records/s, then a repetition
    # there was unsustainable.
    # Guarantees: the rate counts as unsustainable, its flip is recorded,
    # and the search bisects below it rather than keeping it as the winner.
    def test_a_failed_repetition_moves_the_search_below(self):
        trials = [(256_000, "sustainable"), (320_000, "sustainable"),
                  (352_000, "unsustainable"), (320_000, "unsustainable")]
        self.assertEqual(capacity.next_search_rate(trials), 288_000)
        decision = capacity.search_decision(trials)
        self.assertEqual(decision["sustainable_records_per_s"], 256_000)
        self.assertEqual(decision["flip_rates_records_per_s"], [320_000])

    # Scenario: a search starts at a floor of 256,000 records/s derived from
    # its neighbours, and the floor itself is unsustainable.
    # Guarantees: the first trial is the floor, and the search halves until
    # a rate passes, then bisects as before.
    def test_a_search_from_a_floor_halves_until_one_passes(self):
        self.assertEqual(capacity.next_search_rate([], start=256_000), 256_000)
        trials = [(256_000, "unsustainable")]
        self.assertEqual(capacity.next_search_rate(trials, start=256_000), 128_000)
        trials.append((128_000, "sustainable"))
        self.assertEqual(capacity.next_search_rate(trials, start=256_000), 192_000)

    # Scenario: every one of the twelve doubling trials is sustainable.
    # Guarantees: the search stops and reports a lower bound, never a maximum.
    def test_an_unbracketed_search_is_a_lower_bound(self):
        trials = []
        while True:
            rate = capacity.next_search_rate(trials)
            if rate is None:
                break
            trials.append((rate, "sustainable"))
        self.assertEqual(len(trials), capacity.DOUBLING_TRIALS)
        decision = capacity.search_decision(trials)
        self.assertEqual(decision["kind"], "lower_bound")
        self.assertFalse(decision["bracketed"])

    # Scenario: a trial's producer fell behind with in-flight slots free.
    # Guarantees: the trial is producer-limited, the search bisects below
    # it, and a search bounded there names a lower bound, never a maximum.
    def test_a_producer_limited_trial_bounds_the_search(self):
        verdict = capacity.stability_verdict(
            offered=100_000, tail_durable=60_000, backlog_slope_records_per_s=10_000,
            late_unblocked_ratio=0.2, failed_requests=0, partial_requests=0,
        )
        self.assertEqual(verdict["verdict"], "producer_limited")
        trials = [(1000, "sustainable"), (2000, "producer_limited")]
        self.assertEqual(capacity.next_search_rate(trials), 1500)
        trials += [(1500, "sustainable"), (1750, "sustainable"), (1875, "sustainable")]
        self.assertIsNone(capacity.next_search_rate(trials))
        decision = capacity.search_decision(trials)
        self.assertEqual(decision["kind"], "lower_bound_producer_limited")
        self.assertFalse(decision["bracketed"])
        self.assertEqual(decision["sustainable_records_per_s"], 1875)

    # Scenario: the receiver shed 6,219 requests as RESOURCE_EXHAUSTED while
    # 3% of the sends also started late with a slot free.
    # Guarantees: the engine's refusal decides: the trial is unsustainable,
    # a bracket, and the producer's lateness is kept as a reason.
    def test_an_engine_refusal_outranks_producer_lateness(self):
        verdict = capacity.stability_verdict(
            offered=512_000, tail_durable=420_000, backlog_slope_records_per_s=0,
            late_unblocked_ratio=0.03, failed_requests=6219, partial_requests=0,
        )
        self.assertEqual(verdict["verdict"], "unsustainable")
        self.assertTrue(any("late" in reason for reason in verdict["reasons"]))

    # Scenario: the durable tail rate is 97% of offered, or the backlog grows
    # by 3% of the offered rate, or a request failed.
    # Guarantees: each alone makes the trial unsustainable, with its reason.
    def test_the_stability_rule_is_the_plan_row(self):
        base = dict(offered=100_000, tail_durable=100_000,
                    backlog_slope_records_per_s=0, late_unblocked_ratio=0.0,
                    failed_requests=0, partial_requests=0)
        self.assertEqual(capacity.stability_verdict(**base)["verdict"], "sustainable")
        for change in ({"tail_durable": 97_000},
                       {"backlog_slope_records_per_s": 3_000},
                       {"failed_requests": 1}):
            verdict = capacity.stability_verdict(**dict(base, **change))
            self.assertEqual(verdict["verdict"], "unsustainable", change)
            self.assertTrue(verdict["reasons"])

    # Scenario: a backlog that grows by 500 records every second.
    # Guarantees: the least-squares slope reads 500 records/s.
    def test_backlog_slope_is_least_squares(self):
        points = [(t, 1000 + 500 * t) for t in range(10)]
        self.assertAlmostEqual(capacity.backlog_slope(points), 500.0)

    # Scenario: 10 requests over 4 connections and 3 processes, and over one
    # connection.
    # Guarantees: every request is sent once, each connection belongs to one
    # process, and one connection uses one process only.
    def test_connections_are_spread_over_processes(self):
        plans = capacity.connection_plan(10, 4, 3)
        self.assertEqual(len(plans), 3)
        positions = sorted(p for plan in plans for p in plan["positions"])
        self.assertEqual(positions, list(range(10)))
        for plan in plans:
            for position, local in zip(plan["positions"], plan["local"]):
                self.assertEqual(plan["connections"][local], position % 4)
        self.assertEqual(len(capacity.connection_plan(10, 1, 4)), 1)

    # Scenario: a record-scope workload builds one logs request.
    # Guarantees: each record carries its own series slot, the default scope
    # is unchanged, and a default workload records no new field.
    def test_record_scope_varies_series_per_record(self):
        workload = Workload(requests=3, records_per_request=4, series=10,
                            metrics_every=100, series_scope="record")
        loggers = {measurement.logger_name(workload, 1, point) for point in range(4)}
        self.assertEqual(len(loggers), 4)
        self.assertEqual(measurement.logger_name(Workload(), 7, 3),
                         measurement.logger_name(Workload(), 7, 0))
        self.assertNotIn("series_scope", Workload().as_json())
        self.assertEqual(workload.as_json()["series_scope"], "record")
        self.assertEqual(Workload(**workload.as_json()), workload)
        with self.assertRaises(ValueError):
            _ = Workload(series_scope="window")

    @staticmethod
    def memory_sample(t, pipeline_heap, rss=500 << 20):
        """One telemetry sample with the given RSS and pipeline counter."""
        return {
            "monotonic_ns": t,
            "procfs": {"smaps_rss_bytes": rss, "smaps_anonymous_bytes": rss - (50 << 20)},
            "workers": {"w": {"gauges": {"memory.budget": 1, "memory.accounted": 70 << 20},
                              "pipeline_memory_usage_bytes": pipeline_heap}},
        }

    @staticmethod
    def allocator_pair(t, rss, resident, allocated):
        """One allocator print paired with the RSS read after it."""
        return {
            "monotonic_ns": t,
            "procfs": {"smaps_rss_bytes": rss, "smaps_anonymous_bytes": rss - (50 << 20)},
            "jemalloc_resident_bytes": resident,
            "jemalloc_allocated_bytes": allocated,
            "jemalloc_allocated_peak_bytes": allocated,
        }

    # Scenario: the workers' `memory.usage` grows by 10 GB over a trial, as
    # it does when the local store's blocking pool frees what a worker
    # allocated, while the RSS the allocator's prints are paired with swings
    # between its live heap and its resident total.
    # Guarantees: the capacity residual is the allocator band and passes;
    # the pipeline counter alone reads a 10 GB negative residual and fails;
    # RSS beyond the allocator's resident total is a positive residual.
    def test_the_residual_heap_term_is_the_allocator_band(self):
        samples = [self.memory_sample(k * 10**9, (10 << 20) + k * (1 << 30))
                   for k in range(11)]
        pairs = [
            self.allocator_pair(k * 10**9, (300 + 150 * (k % 2)) << 20,
                                (230 + 200 * (k % 2)) << 20, (30 + 100 * (k % 2)) << 20)
            for k in range(11)
        ]
        residuals, heap = capacity.trial_residuals(samples, samples[0], pairs)
        self.assertEqual(heap["source"], "jemalloc_band")
        self.assertEqual(heap["retention_peak_bytes"], 300 << 20)
        self.assertTrue(all(r["residual_bytes"] == 0 for r in residuals))
        self.assertEqual(
            measurement.residual_check(residuals, 500 << 20)["status"],
            measurement.STATUS_PASSED,
        )
        residuals, heap = capacity.trial_residuals(samples, samples[0])
        self.assertEqual(heap["source"], "pipeline_memory_usage")
        self.assertLess(min(r["residual_bytes"] for r in residuals), -(9 << 30))
        self.assertEqual(
            measurement.residual_check(residuals, 500 << 20)["status"],
            measurement.STATUS_FAILED,
        )
        leaking = [self.allocator_pair(0, 300 << 20, 230 << 20, 30 << 20),
                   self.allocator_pair(10**9, 900 << 20, 430 << 20, 230 << 20)]
        residuals, _heap = capacity.trial_residuals(samples, samples[0], leaking)
        self.assertEqual(residuals[0]["residual_bytes"], 400 << 20)

    # Scenario: jemalloc's live heap exceeds the exporter's accounted bytes
    # by 100 MB plus 1 KB for every values row written.
    # Guarantees: the ledger view reports the difference at the highest fill
    # and its growth per record, so a heap leak the RSS band cannot see is
    # visible.
    def test_unaccounted_heap_growth_per_record_is_reported(self):
        samples, pairs = [], []
        for k in range(10):
            written = k * 1000
            accounted = (50 + 10 * (k % 3)) << 20
            samples.append({"monotonic_ns": k * 10**9, "extras": {"w": {
                "memory.accounted": accounted, "block.active": accounted,
                "block.flushing": 0, "rows.written": {"values": written}}}})
            pairs.append({"monotonic_ns": k * 10**9 + 1000,
                          "jemalloc_allocated_bytes": accounted + (100 << 20) + 1024 * written})
        view = capacity.accounted_against_allocated(samples, pairs)
        self.assertAlmostEqual(view["difference_slope_bytes_per_record"], 1024.0)
        self.assertAlmostEqual(view["difference_growth_over_run_bytes"], 1024.0 * 9000)
        self.assertEqual(view["at_highest_fill"]["accounted_bytes"], 70 << 20)
        self.assertEqual(view["difference_min_bytes"], 100 << 20)

    # Scenario: over a measured interval one worker flushed 60 times for 90 s
    # of wall time at one-second windows, with admission closed 30 s and 12
    # requests nacked as storage failures.
    # Guarantees: the per-worker view reports the flush time against the
    # window, the admission closure and the nacks by class as deltas.
    def test_degradation_is_the_interval_delta_per_worker(self):
        def sample(t, flushes, wall, closed, nacks, accepted):
            return {"monotonic_ns": t, "extras": {"w": {
                "flush.duration": {"sum": wall, "count": flushes, "max": 2.5},
                "admission.closed.duration": closed, "admission.closures": flushes,
                "nacks": {"storage": nacks}, "accepted": {"grpc": accepted},
                "flushes": {"time": flushes}, "rejected": {"concurrency_limit": 0}}}}
        samples = [sample(0, 5, 5.0, 1.0, 0, 50), sample(100, 65, 95.0, 31.0, 12, 650)]
        view = capacity.degradation(samples, (10, 90), 1, {"workers": {
            "1": {"on_cpu_ratio": 0.5}}})
        worker = view["workers"]["w"]
        self.assertEqual(worker["flush_count"], 60)
        self.assertAlmostEqual(worker["flush_mean_to_window_ratio"], 1.5)
        self.assertAlmostEqual(worker["admission_closed_s"], 30.0)
        self.assertEqual(worker["exporter_nacks_by_class"], {"storage": 12})
        self.assertEqual(worker["requests_accepted_count"], 600)

    # Scenario: the producer is placed on CPUs 8-9 and 24-25 of a host whose
    # SMT siblings are i and i+16, beside an engine on core 1 and a store on
    # core 7, and then on the store's core.
    # Guarantees: each sender gets one whole physical core, the placement
    # passes the run's role check, and a producer CPU on another role's
    # physical core is refused.
    def test_the_producer_owns_whole_physical_cores_of_its_own(self):
        groups = [[core, core + 16] for core in range(16)]
        allocation = {"engine_observability": [0], "engine": [1], "store": [7, 23]}
        placed = capacity.producer_placement("8-9,24-25", groups, allocation)
        self.assertEqual(placed, [[8, 24], [9, 25]])
        result = {"environment": {}, "events": []}
        controls = measurement.RunControls(result)
        with mock.patch.object(measurement, "core_topology",
                               return_value={"sibling_groups": groups}):
            controls.allocate(dict(allocation, producer=[8, 9, 24, 25]))
        self.assertEqual(result["environment"]["core_allocation"]["producer"], [8, 9, 24, 25])
        with self.assertRaisesRegex(AssertionError, "shares? physical core"):
            _ = capacity.producer_placement("7", groups, allocation)
        self.assertEqual(capacity.producer_placement("allocated", groups, allocation), [])

    # Scenario: 128 receiver slots, 1000-record requests, held one second or
    # sixteen seconds (a 15 s window plus the flush).
    # Guarantees: the strict admission ceiling is slots times records over
    # the hold: 128,000 and 8,000 records/s per worker.
    def test_the_strict_admission_ceiling(self):
        self.assertEqual(capacity.admission_ceiling(128, 1000, 1.0), 128000)
        self.assertEqual(capacity.admission_ceiling(128, 1000, 16.0), 8000)

    # Scenario: the capacity subcommand is asked for without the long opt-in.
    # Guarantees: it is gated as a long measurement.
    def test_capacity_is_a_long_subcommand(self):
        self.assertIn("capacity", measure.LONG_COMMANDS)
        environment = dict(os.environ)
        environment.pop("SERIES_MEASURE_LONG", None)
        with mock.patch.dict(os.environ, environment, clear=True):
            self.assertEqual(
                measure.main(["capacity", "--output-dir", str(temporary_directory(self))]),
                2,
            )


class AlloyContracts(unittest.TestCase):
    """The Alloy-as-producer confirmation's own arithmetic and its tap."""

    def test_line_is_fixed_width_and_carries_its_sequence(self):
        for seq in (0, 7, 123456789):
            body = alloy_capacity.line(seq)
            self.assertEqual(len(body), alloy_capacity.BODY_BYTES)
            self.assertEqual(alloy_capacity.seq_of(body), seq)
        self.assertIsNone(alloy_capacity.seq_of("warmup"))
        self.assertIsNone(alloy_capacity.seq_of(alloy_capacity.line(3)[:-1]))

    def test_writer_holds_a_bounded_backlog_ahead_of_alloy(self):
        # On schedule while Alloy keeps up; held at reads + bound when not.
        self.assertEqual(alloy_capacity.writer_allowance(1.0, 1000, 900, 500), (1000, False))
        self.assertEqual(alloy_capacity.writer_allowance(2.0, 1000, 100, 500), (600, True))

    def test_metrics_read_only_the_series_logs_exporter(self):
        exporter = 'component_id="otelcol.exporter.otlp.series"'
        other = 'component_id="otelcol.exporter.otlp.other"'
        method = 'rpc_method="opentelemetry.proto.collector.logs.v1.LogsService/Export"'
        body = "\n".join([
            "# HELP ignored",
            'loki_source_file_read_lines_total{component_id="loki.source.file.series",'
            'path="/input/events.log"} 5000',
            f"otelcol_exporter_sent_log_records_total{{{exporter},server_port=\"1\"}} 4000",
            f"otelcol_exporter_sent_log_records_total{{{other}}} 99",
            f'otelcol_exporter_queue_size{{{exporter},data_type="logs"}} 1000',
            f'otelcol_exporter_queue_size{{{exporter},data_type="metrics"}} 7',
            f'rpc_client_call_duration_seconds_bucket{{{exporter},{method},'
            'rpc_response_status_code="OK",le="0.5"} 1',
            f'rpc_client_call_duration_seconds_bucket{{{exporter},{method},'
            'rpc_response_status_code="OK",le="1"} 3',
            f'rpc_client_call_duration_seconds_bucket{{{exporter},{method},'
            'rpc_response_status_code="OK",le="+Inf"} 4',
            f'rpc_client_call_duration_seconds_count{{{exporter},{method},'
            'rpc_response_status_code="OK"} 4',
        ])
        found = alloy_capacity.parse_metrics(body)
        self.assertEqual(found["read_lines"], 5000)
        self.assertEqual(found["sent_records"], 4000)
        self.assertEqual(found["queue_size_records"], 1000)
        self.assertEqual(found["calls"], {"OK": 4})
        buckets = found["call_buckets"]["OK"]
        self.assertEqual(alloy_capacity.bucket_quantile(buckets, 0.25), 0.5)
        self.assertEqual(alloy_capacity.bucket_quantile(buckets, 0.5), 1.0)
        self.assertEqual(alloy_capacity.bucket_quantile(buckets, 0.99), float("inf"))

    def test_nearest_rank_quantiles(self):
        values = list(range(1, 11))
        self.assertEqual(alloy_capacity.quantile(values, 0.5), 5)
        self.assertEqual(alloy_capacity.quantile(values, 0.95), 10)
        self.assertEqual(alloy_capacity.distribution([4, 4])["max"], 4)
        self.assertIsNone(alloy_capacity.quantile([], 0.5))

    def test_counter_rate_interpolates_between_polls(self):
        polled = [{"monotonic_ns": 0, "sent": 0}, {"monotonic_ns": 2_000_000_000, "sent": 200}]
        self.assertAlmostEqual(
            alloy_capacity.rate_between(polled, "sent", 500_000_000, 1_500_000_000), 100.0)

    def test_tap_forwards_requests_and_refusals_unchanged(self):
        grpc = alloy_capacity.test_e2e.grpc
        logs_pb = alloy_capacity.test_e2e.logs_pb
        from concurrent import futures
        seen = []

        def export(raw, context):
            request = logs_pb.ExportLogsServiceRequest.FromString(raw)
            seen.append(len(raw))
            if len(request.resource_logs) > 2:
                context.abort(grpc.StatusCode.OUT_OF_RANGE, "too large")
            return logs_pb.ExportLogsServiceResponse().SerializeToString()

        upstream = grpc.server(futures.ThreadPoolExecutor(4))
        upstream.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            "opentelemetry.proto.collector.logs.v1.LogsService",
            {"Export": grpc.unary_unary_rpc_method_handler(
                export, request_deserializer=None, response_serializer=None)}),))
        port = upstream.add_insecure_port("127.0.0.1:0")
        upstream.start()
        tap = alloy_capacity.RequestTap(f"127.0.0.1:{port}")
        tap_port = tap.start()
        try:
            def request(resources):
                message = logs_pb.ExportLogsServiceRequest()
                for number in range(resources):
                    record = message.resource_logs.add().scope_logs.add().log_records.add()
                    record.body.string_value = alloy_capacity.line(number)
                return message.SerializeToString()

            with grpc.insecure_channel(f"127.0.0.1:{tap_port}") as channel:
                call = channel.unary_unary(
                    "/" + alloy_capacity.EXPORT_METHOD, request_serializer=None,
                    response_deserializer=None)
                small = request(2)
                _ = call(small, timeout=10)
                with self.assertRaises(grpc.RpcError) as refused:
                    _ = call(request(3), timeout=10)
                self.assertEqual(refused.exception.code(), grpc.StatusCode.OUT_OF_RANGE)
                self.assertEqual(refused.exception.details(), "too large")
        finally:
            tap.stop()
            upstream.stop(0)
        self.assertEqual(seen[0], len(small))
        self.assertEqual([r["code"] for r in tap.requests], ["OK", "OUT_OF_RANGE"])
        self.assertEqual([r["records"] for r in tap.requests], [2, 3])
        self.assertEqual(tap.requests[0]["bytes"], len(small))
        # One client connection, one upstream connection.
        self.assertEqual(len(tap.channels), 1)

    def test_feeder_and_alloy_never_share_a_core(self):
        groups = [[8, 24], [9, 25], [10, 26], [11, 27]]
        alloy, feeder = alloy_capacity.split_producer_cpus(groups)
        self.assertEqual(alloy, [8, 9, 24, 25])
        self.assertEqual(feeder, [10, 11, 26, 27])
        with self.assertRaises(AssertionError):
            alloy_capacity.split_producer_cpus(groups[:2])


if __name__ == "__main__":
    unittest.main()
