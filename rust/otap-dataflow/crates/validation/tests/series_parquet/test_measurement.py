# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Fast contracts for the measurement harness.

Nothing here starts an engine, a container or a build. These are the rules
every later task's real measurement is read against: the ledger, the loss
oracle, the result schema, the collection-epoch rule, the baseline policy and
the publication and staging rules.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
    from . import measure
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement
    import measure

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
                "metrics": [{"name": "uptime", "value": uptime}],
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


class AffinityContracts(unittest.TestCase):
    """A worker whose affinity cannot be attributed is not a measurement."""

    def worker(self, core=3, group="default"):
        """One expected worker identity as the telemetry reports it."""
        return {
            "key": f"{group}/main/core{core}/gen0",
            "group_id": group,
            "pipeline_id": "main",
            "core_id": core,
            "generation": 0,
        }

    def thread(self, name, cpus, tid=101):
        """One observed thread of the engine process."""
        return {
            "pid": 1,
            "tid": tid,
            "role": "engine",
            "name": name,
            "cpus_allowed_list": cpus,
            "expected_cores": [3],
        }

    # Scenario: the kernel truncates two workers' thread names to the same
    # fifteen-character `comm`.
    # Guarantees: an ambiguous worker mapping aborts the run rather than
    # attributing an affinity to a guess.
    def test_ambiguous_worker_mapping_aborts(self):
        workers = [self.worker(core=1), self.worker(core=1, group="defaultX")]
        _, ambiguous = measurement.select_worker_threads([], workers)
        self.assertTrue(ambiguous)
        with self.assertRaisesRegex(AssertionError, "ambiguous"):
            measurement.assert_affinity(
                {"worker_threads": [], "ambiguous_workers": ambiguous}
            )

    # Scenario: the worker thread is allowed to run on every core although
    # one was requested.
    # Guarantees: a requested-versus-observed affinity mismatch aborts.
    def test_affinity_mismatch_aborts(self):
        name = measurement.worker_thread_name("default", "main", 3, 0)
        threads = [self.thread(name[: measurement.COMM_WIDTH], "0-31")]
        snapshot = measurement.environment_snapshot({}, workers=[self.worker()])
        snapshot["thread_affinity"] = threads
        selected, ambiguous = measurement.select_worker_threads(
            threads, [self.worker()]
        )
        snapshot["worker_threads"] = [
            dict(thread, key=key) for key, thread in selected.items()
        ]
        snapshot["ambiguous_workers"] = ambiguous
        with self.assertRaisesRegex(AssertionError, "affinity mismatch"):
            measurement.assert_affinity(snapshot)

    # Scenario: the worker thread is pinned to exactly the requested core.
    # Guarantees: a matching affinity passes, so the abort is a real check
    # rather than one that always fires.
    def test_matching_affinity_passes(self):
        name = measurement.worker_thread_name("default", "main", 3, 0)
        threads = [self.thread(name[: measurement.COMM_WIDTH], "3")]
        selected, ambiguous = measurement.select_worker_threads(
            threads, [self.worker()]
        )
        measurement.assert_affinity(
            {
                "worker_threads": [
                    dict(thread, key=key) for key, thread in selected.items()
                ],
                "ambiguous_workers": ambiguous,
            }
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


class BuildAndLeaseContracts(unittest.TestCase):
    """A measurement owns the machine and says so."""

    # Scenario: a compiler is running beside a measurement.
    # Guarantees: the run is invalidated and the evidence preserved, without
    # stopping anybody else's process.
    def test_a_concurrent_build_is_detected(self):
        monitor = measurement.BuildMonitor(interval_s=0.05)
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"]
        )
        self.addCleanup(child.wait)
        self.addCleanup(child.kill)
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

    # Scenario: a measurement is killed and leaves its lease file behind.
    # Guarantees: a lease whose holder no longer exists is reclaimed rather
    # than blocking the machine forever.
    def test_a_stale_lease_is_reclaimed(self):
        path = temporary_directory(self) / "host.lease"
        _ = path.write_text(
            json.dumps({"owner": "gone", "pid": 2**22, "acquired_utc": "x"}),
            encoding="ascii",
        )
        lease = measurement.HostLease(path).acquire(
            deadline_ns=measurement.time.monotonic_ns() + 10**9
        )
        self.addCleanup(lease.release)
        lease.assert_held()


def measured_result(**overrides) -> dict:
    """A minimal valid passed run, for the schema and baseline contracts."""
    snapshot = {
        "cpu_model": "Test CPU",
        "logical_core_count": 32,
        "physical_core_count": 16,
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
            "environment": {
                "start": dict(snapshot),
                "end": dict(snapshot),
                "machine_identity_sha256": "m",
                "core_allocation": {"engine": [0]},
                "build": {
                    "profile": "release",
                    "features": "series_parquet",
                    "allocator": "system",
                    "toolchain": "1.88",
                },
            },
            "config": {"requested": {}, "effective": {"window": {"interval": "15s"}}},
            "workload": Workload().as_json(),
            "metrics": {
                "throughput_records_per_s": 1000.0,
                "ack_p99_s": 0.5,
                "peak_rss_bytes": 100.0,
            },
            "mandatory_metrics": ["throughput_records_per_s"],
            "checks": [
                measurement.check(
                    "delivery", measurement.CHECK_HARD, measurement.STATUS_PASSED
                ),
                measurement.check(
                    "rss_reconciliation",
                    measurement.CHECK_HARD,
                    measurement.STATUS_PASSED,
                ),
            ],
        }
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

    # Scenario: the machine, configuration, workload or build profile
    # changed.
    # Guarantees: the fingerprint changes with each of them, so a run is
    # never compared against a different environment.
    def test_the_fingerprint_covers_every_declared_input(self):
        base = measurement.baseline_fingerprint(measured_result())
        for mutate in (
            lambda r: r["environment"]["start"].update(ram_bytes=1),
            lambda r: r["environment"].update(core_allocation={"engine": [7]}),
            lambda r: r["config"].update(effective={"window": {"interval": "1s"}}),
            lambda r: r.update(workload=Workload(seed=1).as_json()),
            lambda r: r["environment"]["build"].update(allocator="jemalloc"),
        ):
            with self.subTest(mutate=mutate):
                result = measured_result()
                mutate(result)
                self.assertNotEqual(measurement.baseline_fingerprint(result), base)

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
    # Guarantees: only identical metric schemas are compared.
    def test_schemas_must_match_to_compare(self):
        first = measured_result()
        fingerprint = measurement.baseline_fingerprint(first)
        baseline = measurement.new_baseline(first, fingerprint)
        other = measured_result(
            metrics={"throughput_records_per_s": 1000.0},
            mandatory_metrics=["throughput_records_per_s"],
        )
        with self.assertRaisesRegex(AssertionError, "ack_p99_s"):
            _ = measurement.evaluate_baseline(
                other, baselines={fingerprint: baseline}
            )

    # Scenario: a baseline reference value is zero and the new run is not.
    # Guarantees: a zero reference permits no positive increase unless a
    # predeclared resolution floor covers it.
    def test_a_zero_reference_permits_no_increase(self):
        self.assertEqual(
            measurement.signed_regression("flush_failures_count", 0, 0), 0.0
        )
        self.assertEqual(
            measurement.signed_regression("flush_failures_count", 3, 0),
            float("inf"),
        )
        self.assertEqual(
            measurement.signed_regression("flush_failures_count", 3, 0, floor=5),
            0.0,
        )

    # Scenario: a metric where a larger value is better falls, and one where
    # a smaller value is better rises.
    # Guarantees: the direction decides the sign of a regression, not its
    # threshold.
    def test_regression_direction_follows_the_metric(self):
        self.assertAlmostEqual(
            measurement.signed_regression("x_records_per_s", 50.0, 100.0), 0.5
        )
        self.assertAlmostEqual(
            measurement.signed_regression("x_bytes", 150.0, 100.0), 0.5
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

    # Scenario: a measured experiment fails part way through.
    # Guarantees: the run's JSON is still written and published, with both
    # environment snapshots and a failed status.
    def test_a_failed_run_still_writes_its_file(self):
        directory = temporary_directory(self)
        report = temporary_directory(self) / "report"
        spec = measure.harness_local_spec()
        with self.assertRaises(NotImplementedError):
            _ = measure.run_case(spec, directory, report_dir=report)
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

    # Scenario: a measured body reports its metrics and checks.
    # Guarantees: the contract around the body -- one file per run, both
    # snapshots, an environment match and a published tree -- holds for a run
    # that succeeded as well as for one that failed.
    def test_a_successful_run_publishes_its_evidence(self):
        directory = temporary_directory(self)
        report = temporary_directory(self) / "report"
        spec = measure.harness_local_spec()

        def experiment(_spec, result, _output_dir):
            result["metrics"] = {"throughput_records_per_s": 10.0}
            result["mandatory_metrics"] = ["throughput_records_per_s"]
            result["checks"] = [
                measurement.check(
                    "delivery", measurement.CHECK_HARD, measurement.STATUS_PASSED
                )
            ]
            result["status"] = measurement.STATUS_PASSED

        outcome = measure.run_case(
            spec, directory, experiment=experiment, report_dir=report
        )
        self.assertEqual(outcome["status"], measurement.STATUS_PASSED)
        document = json.loads(
            (report / f"{spec.run_id}.json").read_text(encoding="ascii")
        )
        self.assertTrue(document["environment"]["match"]["matched"])
        self.assertEqual(document["metrics"]["throughput_records_per_s"], 10.0)


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

            def test_nothing(self):
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


if __name__ == "__main__":
    unittest.main()
