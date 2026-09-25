# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Contracts of the reference-deployment validation: no engine, no container."""

import types
import unittest

try:
    from . import reference_deployment as ref
except ImportError:
    import reference_deployment as ref


class ReferenceDeploymentContracts(unittest.TestCase):
    # Scenario: a line written for producer 7 at sequence 123456 is parsed back.
    # Guarantees: every line is BODY_BYTES long and names its producer and sequence.
    def test_line_round_trips(self):
        body = ref.line(7, 123456)
        self.assertEqual(len(body), ref.BODY_BYTES)
        self.assertEqual(ref.parse_line(body), (7, 123456))
        self.assertIsNone(ref.parse_line("alloy 000000000001 x"))

    # Scenario: the run config is built from the shipped buffered file.
    # Guarantees: only the site values change (core, listen address, WAL path,
    # store endpoint and bucket, plus named overrides), each recorded with its
    # shipped value.
    def test_only_site_values_differ_from_the_shipped_config(self):
        store = types.SimpleNamespace(endpoint="http://127.0.0.1:9", bucket="b")
        config, changed = ref.reference_config(
            grpc_port=1, store=store, wal_dir="/w",
            overrides={"buffer.config.retention_size_cap": "1MiB"})
        base = "groups.default.pipelines.main.nodes"
        self.assertEqual(set(changed), {
            "policies.resources.core_allocation",
            f"{base}.receiver.config.protocols.grpc.listening_addr",
            f"{base}.buffer.config.path",
            f"{base}.exporter.config.storage.s3.endpoint",
            f"{base}.exporter.config.storage.s3.base_uri",
            f"{base}.buffer.config.retention_size_cap",
        })
        exporter = config["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]
        self.assertEqual(exporter["config"]["window"]["flush_retry_deadline"], "15s")
        self.assertEqual(changed[f"{base}.buffer.config.retention_size_cap"]["run"], "1MiB")

    # Scenario: a feeder timeline where producer 0 had written 10, then 20 lines.
    # Guarantees: a line's write time is the first mark by which it was in the file.
    def test_write_time_is_the_first_mark_covering_the_line(self):
        feed = {"timeline": [[0, 100, [10]], [1, 200, [20]]]}
        self.assertEqual(ref.write_time_ns(feed, 0, 9), 100)
        self.assertEqual(ref.write_time_ns(feed, 0, 10), 200)

    # Scenario: freshness values of weight 1, 1 and 98.
    # Guarantees: the quantiles are weighted by the lines each value stands for.
    def test_quantiles_are_weighted_by_lines(self):
        found = ref.quantiles([(1.0, 1), (2.0, 1), (9.0, 98)])
        self.assertEqual(found["p50_s"], 9.0)
        self.assertEqual(found["max_s"], 9.0)
        self.assertEqual(found["lines"], 100)

    # Scenario: two duplicated runs, one across boots 0 and 1, one inside boot 1.
    # Guarantees: duplicate lines are attributed to the boots that wrote their copies.
    def test_duplicates_are_attributed_to_boots(self):
        view = ref.duplicate_view(
            [(0, 10, 19, 2, ("a", "b")), (1, 0, 4, 2, ("b",))], ["a", "b"])
        self.assertEqual(view["duplicate_lines_by_boots"], {"0,1": 10, "1": 5})
        self.assertEqual(view["runs_count"], 2)

    # Scenario: a WAL-full run whose Alloy fleet reports 384 UNAVAILABLE calls.
    # Guarantees: the fault check reads Alloy's upper-case status label and
    # needs both the buffer's refusals and the producers' UNAVAILABLE.
    def test_wal_full_is_observed_from_alloys_status_label(self):
        self.assertTrue(ref.wal_full_observed(384, {"OK": 10, "UNAVAILABLE": 384})[0])
        self.assertFalse(ref.wal_full_observed(384, {"OK": 10})[0])
        self.assertFalse(ref.wal_full_observed(0, {"UNAVAILABLE": 3})[0])

    # Scenario: the control variant of the reference River config.
    # Guarantees: only the file storage component and the queue's storage
    # reference are removed.
    def test_memory_queue_variant_drops_only_the_queue_storage(self):
        text = (ref.test_e2e.WORKSPACE / ref.ALLOY_CONFIG).read_text()
        variant = ref.memory_queue_variant(text)
        self.assertNotIn("otelcol.storage.file", variant)
        removed = set(text.splitlines()) - set(variant.splitlines())
        self.assertEqual(len(removed), 2, removed)
        self.assertIn("block_on_overflow = true", variant)

    # Scenario: after two SIGKILLs one producer stores 9000 duplicates at the
    # first kill while the fleet total stays under eight producers' bounds.
    # Guarantees: the bound holds per kill and producer, so unused allowance
    # of other producers or kills does not mask the excess.
    def test_duplicates_are_bounded_per_kill_and_producer(self):
        allowed = {"event": "engine_sigkill", "per_producer_per_event_lines": 8000,
                   "rule": "test"}
        events = [{"kind": "engine_sigkill"}, {"kind": "engine_sigkill"}]
        within = [{"producer": 0, "lines": 4000, "copies": 2, "boots": [1]},
                  {"producer": 0, "lines": 4000, "copies": 2, "boots": [1, 2]}]
        self.assertTrue(ref.duplicate_check("engine_kill", within, 2, events, allowed)[0])
        over = [{"producer": 3, "lines": 9000, "copies": 2, "boots": [0, 1]}]
        self.assertFalse(ref.duplicate_check("engine_kill", over, 1, events, allowed)[0])
        orphan = [{"producer": 0, "lines": 10, "copies": 2, "boots": [0]}]
        self.assertFalse(ref.duplicate_check("engine_kill", orphan, 1, events, allowed)[0])
        self.assertFalse(ref.duplicate_check("engine_kill", within, 3, events, allowed)[0])

    # Scenario: 8000 lines duplicated in boot 1 only and 1000 lines present in
    # boots 0, 1 and 2, after two SIGKILLs, against an 8000-line bound.
    # Guarantees: each later-boot copy is charged to the kill that started its
    # boot, so kill 1 (starting boot 1) carries 9000 and fails, where charging
    # a run to its latest boot alone would pass.
    def test_each_copy_is_charged_to_the_kill_that_started_its_boot(self):
        allowed = {"event": "engine_sigkill", "per_producer_per_event_lines": 8000,
                   "rule": "test"}
        events = [{"kind": "engine_sigkill"}, {"kind": "engine_sigkill"}]
        runs = [{"producer": 0, "lines": 8000, "copies": 2, "boots": [0, 1],
                 "boot_copies": {"0": 1, "1": 1}},
                {"producer": 0, "lines": 1000, "copies": 3, "boots": [0, 1, 2],
                 "boot_copies": {"0": 1, "1": 1, "2": 1}}]
        passed, detail = ref.duplicate_check("engine_kill", runs, 2, events, allowed)
        self.assertFalse(passed)
        self.assertIn("'event 0 producer 0': 9000", detail)
        self.assertIn("'event 1 producer 0': 1000", detail)
        # Old results without per-boot counts: two copies in one boot are known.
        self.assertEqual(ref.boot_copies({"boots": [1], "copies": 2}), {1: 2})
        self.assertIsNone(ref.boot_copies({"boots": [0, 1], "copies": 3}))

    # Scenario: a permanent rejection counted by boot 0 and a clean boot 1.
    # Guarantees: buffer losses are summed over every boot, so a restart
    # resetting the counter does not hide them.
    def test_buffer_losses_are_summed_over_boots(self):
        key = "processor.durable_buffer.bundles.resolved{outcome=permanently_rejected}"
        samples = [{"boot": 0, "metrics": {key: 2.0}},
                   {"boot": 1, "metrics": {key: 0.0,
                                           "processor.durable_buffer.loss.bundles": 1.0}}]
        loss, permanent = ref.buffer_losses(samples)
        self.assertEqual(permanent, 2.0)
        self.assertEqual(loss, {"processor.durable_buffer.loss.bundles": 1.0})

    # Scenario: the shipped River config is read for the duplicate bound.
    # Guarantees: the reference producer keeps its file-backed queue, two
    # consumers and 4000-record batches.
    def test_shipped_alloy_settings(self):
        settings = ref.shipped_alloy_settings()
        self.assertEqual(settings["num_consumers"], 2)
        self.assertEqual(settings["batch_max_records"], 4000)
        self.assertEqual(settings["timeout"], "10s")
        self.assertTrue(settings["persistent_queue"])


if __name__ == "__main__":
    unittest.main()
