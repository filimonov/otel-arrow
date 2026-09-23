# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Fault-tool prerequisites, privileges and the fault rig's contracts.

The contract classes start no container, engine or build: they pin how a
probe result becomes a pass, a skip or a failure, which container may hold
NET_ADMIN, the exact toxic bodies and NGINX configuration, the ACK-only
bytecode conversion and the rig's activation rules. `LiveRigSlice` builds
one real rig around a real store when Docker and the provisioned images are
present, and skips otherwise unless the fault tools are required.
"""
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest import mock

try:  # Imported as a package module by `python3 -m crates...`.
    from . import faults
    from . import measure
    from . import measurement
    from . import test_e2e
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import faults
    import measure
    import measurement
    import test_e2e

require_probe = faults.require_probe


def temporary_directory(case) -> Path:
    """A directory that lives for exactly one test."""
    holder = tempfile.TemporaryDirectory()
    case.addCleanup(holder.cleanup)
    return Path(holder.name)


def environment(**values):
    """Patch the fault-tool and Docker requirement flags for one test."""
    cleared = {"SERIES_REQUIRE_FAULT_TOOLS": "", "SERIES_REQUIRE_DOCKER": ""}
    cleared.update(values)
    return mock.patch.dict(os.environ, cleared)


class ProbeVerdicts(unittest.TestCase):
    """How one probe record decides a pass, a skip or a failure."""

    # Scenario: a required lane cannot install the disposable namespace's DNS rule.
    # Guarantees: missing NET_ADMIN is fatal instead of silently dropping required coverage.
    def test_required_probe_failure_is_fatal(self):
        with self.assertRaisesRegex(AssertionError, "UDP DNS"):
            require_probe({"name": "UDP DNS", "passed": False}, required=True)

    # Scenario: TCP DNS blocking fails in a required lane while UDP passed.
    # Guarantees: each DNS transport is its own required probe; one passing
    # does not cover the other.
    def test_required_tcp_dns_failure_is_fatal(self):
        require_probe({"name": "UDP DNS", "passed": True}, required=True)
        with self.assertRaisesRegex(AssertionError, "TCP DNS"):
            require_probe({"name": "TCP DNS", "passed": False, "detail": "no timeout"},
                          required=True)

    # Scenario: the kernel refuses the bpf match inside the namespace.
    # Guarantees: a required lane fails, and the message names the exact
    # host command that would fix it rather than trying it.
    def test_xt_bpf_failure_names_the_host_fix(self):
        probe = {"name": "xt_bpf", "passed": False, "host_fix": faults.BPF_HOST_FIX,
                 "detail": "the bpf match rule could not be inserted"}
        with self.assertRaisesRegex(AssertionError, "xt_bpf.*sudo modprobe xt_bpf"):
            require_probe(probe, required=True)

    # Scenario: the same failed probes in a lane that does not require fault tools.
    # Guarantees: an optional lane skips cleanly instead of failing or passing.
    def test_optional_probe_failure_skips(self):
        for name in ("UDP DNS", "TCP DNS", "xt_bpf", "capabilities"):
            with self.subTest(name=name):
                with self.assertRaisesRegex(unittest.SkipTest, name):
                    require_probe({"name": name, "passed": False}, required=False)

    # Scenario: a probe record says something truthy that is not True, or nothing.
    # Guarantees: only an explicit `passed: True` counts; a malformed probe
    # can never be relabelled a pass.
    def test_only_explicit_true_passes(self):
        require_probe({"name": "capabilities", "passed": True}, required=True)
        for record in ({"name": "x", "passed": 1}, {"name": "x", "passed": "yes"},
                       {"name": "x"}, {"name": "x", "passed": None}):
            with self.subTest(record=record):
                with self.assertRaises(AssertionError):
                    require_probe(record, required=True)
                with self.assertRaises(unittest.SkipTest):
                    require_probe(record, required=False)

    # Scenario: the tools image is missing, with and without the require flags.
    # Guarantees: a missing tool is a skip only where nothing requires it.
    def test_missing_tools_skip_unless_required(self):
        with environment():
            with self.assertRaises(unittest.SkipTest):
                faults.tools_unavailable("image absent")
        for flag in ("SERIES_REQUIRE_FAULT_TOOLS", "SERIES_REQUIRE_DOCKER"):
            with self.subTest(flag=flag), environment(**{flag: "1"}):
                with self.assertRaisesRegex(AssertionError, "image absent"):
                    faults.tools_unavailable("image absent")

    # Scenario: the xt_bpf probe failed on one store and everything else passed.
    # Guarantees: only the classes that depend on it become unavailable, and
    # the consequence follows the lane's requirement.
    def test_coverage_marks_only_dependent_classes(self):
        probes = [{"name": name, "store": "minio", "passed": True}
                  for name in faults.PREFLIGHT_PROBES]
        for probe in probes:
            if probe["name"] == "xt_bpf":
                probe["passed"] = False
        optional = faults.coverage(probes, required=False)
        self.assertEqual(optional["tcp_ack_loss"]["status"], "unavailable")
        self.assertEqual(optional["tcp_ack_loss"]["failed_probes"], ["minio: xt_bpf"])
        self.assertEqual(optional["tcp_ack_loss"]["consequence"], "skipped before traffic")
        for name in ("slow", "http503", "store_outage", "dns_nxdomain_timeout"):
            self.assertEqual(optional[name]["status"], "available", name)
        required = faults.coverage(probes, required=True)
        self.assertEqual(required["tcp_ack_loss"]["consequence"], "fails the lane")

    # Scenario: a preflight never ran the DNS probes at all.
    # Guarantees: an absent probe makes its classes unavailable; absence is
    # never read as success.
    def test_absent_probe_is_not_coverage(self):
        probes = [{"name": name, "store": "minio", "passed": True}
                  for name in faults.BASE_PROBES]
        classes = faults.coverage(probes, required=False)
        self.assertEqual(classes["dns_nxdomain_timeout"]["status"], "unavailable")
        self.assertEqual(sorted(classes["dns_nxdomain_timeout"]["missing_probes"]),
                         ["TCP DNS", "UDP DNS"])


class Privileges(unittest.TestCase):
    """Which container may hold an added capability, by its command line."""

    OWNER = "0123456789ab"

    def argvs(self):
        """The command line of every container a rig or a store starts."""
        owner = faults.owner_argv(
            name="series-fault-x-tools", network="net", image="tools", run_id="x",
            control_dir="/c", artifact_dir="/a", engine_ports=(40001, 40002), cores=(1, 2),
        )
        toxiproxy = faults.toxiproxy_argv(name="series-fault-x-toxiproxy", owner=self.OWNER,
                                          image="toxiproxy", run_id="x")
        engine = faults.engine_argv(
            name="series-fault-x-engine-1", cidfile="/s/engine-1.cid", owner=self.OWNER,
            image="tools", run_id="x", argv=["/bin/df_engine", "--config", "/r/p.yaml"],
            mounts=[("/bin/df_engine", "ro"), ("/repo", "ro"), ("/r", "rw")],
            user="1000:1000", env={"RUST_LOG": "info", "AWS_SECRET_ACCESS_KEY": "s"},
        )
        store = test_e2e.DockerStore("minio")
        store.port = 40003
        return {"owner": owner, "toxiproxy": toxiproxy, "engine": engine,
                "store": store.run_args("minio-image")}

    # Scenario: the rig starts its owner, Toxiproxy, the engine and the store.
    # Guarantees: NET_ADMIN is added to the namespace owner only, never to the
    # engine, the store or Toxiproxy.
    def test_net_admin_only_for_the_namespace_owner(self):
        for role, argv in self.argvs().items():
            added = [argv[index + 1] for index, word in enumerate(argv) if word == "--cap-add"]
            with self.subTest(role=role):
                self.assertEqual(added, ["NET_ADMIN"] if role == "owner" else [])

    # Scenario: any rig container asks for broad privileges.
    # Guarantees: no --privileged, host networking, host PID namespace or
    # device access is ever requested.
    def test_no_container_is_privileged_or_on_the_host_network(self):
        for role, argv in self.argvs().items():
            text = " ".join(argv)
            with self.subTest(role=role):
                self.assertNotIn("--privileged", argv)
                self.assertNotIn("--network host", text)
                self.assertNotIn("--pid", argv)
                self.assertNotIn("--device", argv)

    # Scenario: Toxiproxy and the engine must see the proxy's loopback listeners.
    # Guarantees: both join the owner's namespace instead of a network of
    # their own, and only the owner publishes ports, on host loopback only.
    def test_sidecars_join_the_owner_namespace(self):
        argvs = self.argvs()
        for role in ("toxiproxy", "engine"):
            self.assertIn(f"container:{self.OWNER}", argvs[role])
            self.assertNotIn("--publish", argvs[role])
        published = [argvs["owner"][index + 1] for index, word in enumerate(argvs["owner"])
                     if word == "--publish"]
        self.assertTrue(published)
        self.assertTrue(all(value.startswith("127.0.0.1:") for value in published), published)
        self.assertIn("127.0.0.1:40001:40001", published)

    # Scenario: the harness environment carries credentials.
    # Guarantees: the engine container inherits logging settings only, and
    # its binary and repository are read-only while its run directory is not.
    def test_engine_container_mounts_and_environment(self):
        engine = self.argvs()["engine"]
        self.assertIn("RUST_LOG=info", engine)
        self.assertFalse(any("SECRET" in word for word in engine), engine)
        self.assertIn("/bin/df_engine:/bin/df_engine:ro", engine)
        self.assertIn("/repo:/repo:ro", engine)
        self.assertIn("/r:/r:rw", engine)
        self.assertEqual(engine[engine.index("--user") + 1], "1000:1000")

    # Scenario: a fault case is placed on the eight physical cores the
    # measurement lanes are confined to.
    # Guarantees: the declared fault roles fit beside the engine's reservation.
    def test_fault_roles_fit_eight_physical_cores(self):
        groups = [[core, core + 8] for core in range(8)]
        allocation = measurement.role_allocation(
            groups, range(16), [1], roles=measurement.CASE_ROLES["faults"]
        )
        self.assertEqual(len(allocation["fault_tools"]), 1)
        owned = [core for cores in allocation.values() for core in cores]
        self.assertEqual(len(owned), len(set(owned)))


class ToolContracts(unittest.TestCase):
    """The exact bodies, configuration and parsing the fault tools rely on."""

    # Scenario: slow storage is activated with its defaults.
    # Guarantees: the toxics are exactly the plan's bodies, in Toxiproxy's
    # own units (rate in KB/s, latency in ms).
    def test_slow_toxics_are_the_plan_bodies(self):
        self.assertEqual(faults.slow_toxics(), [
            json.loads('{"name":"slow_upload","type":"bandwidth","stream":"upstream",'
                       '"toxicity":1.0,"attributes":{"rate":256}}'),
            json.loads('{"name":"slow_response","type":"latency","stream":"downstream",'
                       '"toxicity":1.0,"attributes":{"latency":1500,"jitter":0}}'),
        ])
        self.assertEqual(faults.TOXIC_UNITS["bandwidth.rate"], "KB/s")

    # Scenario: the NGINX fault front is configured.
    # Guarantees: the checked-in configuration carries the plan's body
    # verbatim -- signed Host preserved, no buffering, no retries, the 503
    # control file and both backends.
    def test_nginx_configuration_is_the_plan_body(self):
        body = faults.NGINX_CONF.read_text()
        body = "\n".join(line for line in body.splitlines() if not line.startswith("#"))
        for line in (
            "log_format faults '$msec $request_method $uri $status $upstream_status '",
            "default http://127.0.0.1:19001;",
            "~dataset=values/ http://127.0.0.1:19002;",
            "listen 19000;",
            "if (-f /control/fail503) { return 503; }",
            "proxy_set_header Host $http_host;",
            "proxy_request_buffering off;",
            "proxy_buffering off;",
            "proxy_next_upstream off;",
            "proxy_pass $backend;",
        ):
            self.assertIn(line, body)
        self.assertEqual(faults.NGINX_PORT, 19000)
        self.assertEqual(faults.PROXY_PORTS, {"general": 19001, "values": 19002})

    # Scenario: tcpdump prints the ACK-only program.
    # Guarantees: the count and every instruction reach iptables in its
    # comma-separated form; a malformed listing is refused.
    def test_ddd_output_becomes_iptables_bytecode(self):
        self.assertEqual(faults.bytecode_from_ddd("2\n40 0 0 12\n6 0 0 0\n"),
                         "2,40 0 0 12,6 0 0 0")
        for text in ("", "x\n1 2 3 4", "3\n40 0 0 12\n6 0 0 0", "1\n40 0 0"):
            with self.subTest(text=text):
                with self.assertRaises(AssertionError):
                    faults.bytecode_from_ddd(text)

    # Scenario: the store address comes from `docker inspect`.
    # Guarantees: only a dotted IPv4 address and an integer port reach the
    # filter expression, so nothing else can be compiled into the program.
    def test_ack_filter_substitutes_only_an_address_and_port(self):
        expression = faults.ack_only_expression("172.18.0.2", 9000)
        self.assertTrue(expression.startswith("src host 172.18.0.2 and src port 9000 and "))
        self.assertIn("tcp[13] = 16", expression)
        for address in ("172.18.0.2 or tcp", "store", "300.1.1.1", None):
            with self.subTest(address=address):
                with self.assertRaises(ValueError):
                    faults.ack_only_expression(address, 9000)
        with self.assertRaises(ValueError):
            faults.ack_only_expression("172.18.0.2", 70000)

    # Scenario: the helper compiles through a command prefix, such as the
    # owner's docker exec.
    # Guarantees: the expression is one argv value after `-ddd -y RAW`.
    def test_ack_drop_bytecode_passes_the_expression_as_argv(self):
        listing = {"exit_status": 0, "stdout": "1\n6 0 0 0\n", "stderr": ""}
        with mock.patch.object(faults, "run_command", return_value=listing) as run:
            self.assertEqual(faults.ack_drop_bytecode("10.0.0.2", 9000, tcpdump=("x", "tcpdump")),
                             "1,6 0 0 0")
        argv = run.call_args.args[0]
        self.assertEqual(argv[:5], ["x", "tcpdump", "-ddd", "-y", "RAW"])
        self.assertEqual(argv[5], faults.ack_only_expression("10.0.0.2", 9000))

    # Scenario: this host has tcpdump, which compiles without an interface.
    # Guarantees: the real compiler's output converts, and the program
    # matches on the store address it was given.
    @unittest.skipUnless(shutil.which("tcpdump"), "tcpdump is not installed on this host")
    def test_real_tcpdump_compiles_the_ack_filter(self):
        bytecode = faults.ack_drop_bytecode("172.18.0.2", 9000)
        count, *instructions = bytecode.split(",")
        self.assertEqual(int(count), len(instructions))
        # 172.18.0.2 as the 32-bit constant the program compares against.
        self.assertIn(str((172 << 24) | (18 << 16) | 2), bytecode)

    # Scenario: iptables lists a probe rule with its counters.
    # Guarantees: exactly one matching rule yields counters; zero or several
    # matches yield none, so a probe cannot read another rule's packets.
    def test_rule_counters_need_exactly_one_match(self):
        listing = ("-P INPUT ACCEPT -c 0 0\n"
                   "-A INPUT -d 127.0.0.1/32 -p udp -m udp --dport 53 -c 3 180 -j DROP\n")
        found = faults.rule_counters(listing, ["-p udp", "--dport 53", "-j DROP"])
        self.assertEqual((found["packets"], found["bytes"]), (3, 180))
        doubled = listing + listing.splitlines()[1] + "\n"
        self.assertIsNone(faults.rule_counters(doubled, ["-p udp", "-j DROP"])["packets"])
        self.assertIsNone(faults.rule_counters(listing, ["-p tcp"])["packets"])

    # Scenario: NGINX logs the engine's requests in the fault format.
    # Guarantees: every field is read by name, and the values map sends
    # exactly the values dataset to its own proxy.
    def test_access_log_and_backend_map(self):
        path = temporary_directory(self) / "access.log"
        path.write_text(
            "1.5 PUT /b/otel/v=1/signal=logs/dataset=values/x.parquet 200 200 0.1 0.1 0\n"
            "1.6 PUT /b/otel/v=1/signal=logs/dataset=series/y.parquet 503 - 0.0 - 0\n"
            "truncated line\n"
        )
        entries = faults.parse_access_log(path)
        self.assertEqual(entries[0]["status"], "200")
        self.assertEqual(entries[1]["upstream_status"], "-")
        self.assertEqual(entries[2], {"raw": "truncated line"})
        self.assertEqual(faults.backend_of(entries[0]["uri"]), "values")
        self.assertEqual(faults.backend_of(entries[1]["uri"]), "general")

    # Scenario: a process's capability mask is read from /proc.
    # Guarantees: NET_ADMIN is decoded from its own bit, not guessed.
    def test_capability_mask_decoding(self):
        self.assertEqual(faults.capability_names(1 << 12), ["NET_ADMIN"])
        self.assertIn("NET_RAW", faults.capability_names(0xa80425fb))
        self.assertNotIn("NET_ADMIN", faults.capability_names(0xa80425fb))
        self.assertEqual(faults.capability_names(1 << 45), ["CAP_45"])

    # Scenario: an engine runs in a namespace a launcher owns.
    # Guarantees: the receiver binds the launcher's address; the default
    # configuration is unchanged.
    def test_engine_config_binds_the_launcher_address(self):
        data = temporary_directory(self)
        default = test_e2e.engine_config(grpc_port=4317, data=data)
        bound = test_e2e.engine_config(grpc_port=4317, data=data, grpc_host="0.0.0.0")
        address = ["groups", "default", "pipelines", "main", "nodes", "receiver", "config",
                   "protocols", "grpc", "listening_addr"]

        def read(config):
            for key in address:
                config = config[key]
            return config

        self.assertEqual(read(default), "127.0.0.1:4317")
        self.assertEqual(read(bound), "0.0.0.0:4317")


class FakeRig(faults.FaultRig):
    """A rig whose Docker side is replaced, to exercise its own rules."""

    def __init__(self, root):
        super().__init__(store=mock.Mock(kind="minio"), root=root, probes=("fake",))
        self.exits = 0

    def _start(self):
        """Nothing to start."""

    def __exit__(self, *exc):
        self.exits += 1
        return False


class RigRules(unittest.TestCase):
    """What a rig does with probe failures and with faults after preflight."""

    def rig(self):
        """A fake rig that believes its images are present."""
        patcher = mock.patch.object(faults, "require_fault_images", return_value={})
        patcher.start()
        self.addCleanup(patcher.stop)
        return FakeRig(temporary_directory(self))

    # Scenario: an optional lane's entry probe fails.
    # Guarantees: the rig cleans up first and then skips; no case traffic
    # can follow.
    def test_optional_entry_probe_skips_after_cleanup(self):
        rig = self.rig()
        failing = {"fake": lambda _rig: {"name": "UDP DNS", "passed": False}}
        with environment(), mock.patch.dict(faults.PROBES, failing):
            with self.assertRaisesRegex(unittest.SkipTest, "UDP DNS"):
                rig.__enter__()
        self.assertEqual(rig.exits, 1)

    # Scenario: the same entry probe fails where fault tools are required.
    # Guarantees: the rig cleans up and fails.
    def test_required_entry_probe_fails_after_cleanup(self):
        rig = self.rig()
        failing = {"fake": lambda _rig: {"name": "xt_bpf", "passed": False}}
        with environment(SERIES_REQUIRE_FAULT_TOOLS="1"), mock.patch.dict(faults.PROBES, failing):
            with self.assertRaisesRegex(AssertionError, "xt_bpf"):
                rig.__enter__()
        self.assertEqual(rig.exits, 1)

    # Scenario: after a successful preflight a fault's activation raises a
    # skip or an ordinary error, in a lane that does not require tools.
    # Guarantees: activation errors after preflight are failures, never skips.
    def test_activation_errors_after_preflight_are_failures(self):
        rig = self.rig()

        def skip(_rig, _parameters):
            raise unittest.SkipTest("tool vanished")

        def broken(_rig, _parameters):
            raise RuntimeError("proxy API refused")

        faults_table = {"skipper": (skip, None), "broken": (broken, None)}
        with environment(), mock.patch.dict(faults.FAULTS, faults_table):
            for name in faults_table:
                with self.subTest(name=name):
                    with self.assertRaisesRegex(AssertionError, f"activating {name}"):
                        rig.activate(name, {})
            with self.assertRaisesRegex(AssertionError, "no fault named"):
                rig.activate("unregistered", {})
        self.assertEqual(rig.active, [])

    # Scenario: two faults are active and one recovery fails.
    # Guarantees: every fault is still recovered, newest first, and the
    # failure is raised rather than swallowed.
    def test_recovery_is_complete_and_loud(self):
        rig = self.rig()
        order = []

        def activate(_rig, parameters):
            return dict(parameters)

        def recover(_rig, state):
            order.append(state["id"])
            if state["id"] == "b":
                raise RuntimeError("recovery broke")
            return {}

        table = {"a": (activate, recover), "b": (activate, recover)}
        with mock.patch.dict(faults.FAULTS, table):
            rig.activate("a", {"id": "a"})
            rig.activate("b", {"id": "b"})
            with self.assertRaisesRegex(AssertionError, "b: recovery broke"):
                rig.recover()
        self.assertEqual(order, ["b", "a"])
        self.assertEqual(rig.active, [])
        self.assertEqual(len(rig.activations), 2)

    # Scenario: the plan's first fault names are looked up.
    # Guarantees: slow, http503 and store_outage are registered with both
    # an activation and a recovery.
    def test_plan_faults_are_registered(self):
        for name in ("slow", "http503", "store_outage"):
            activate, recover = faults.FAULTS[name]
            self.assertTrue(callable(activate) and callable(recover), name)

    # Scenario: the engine container is signalled.
    # Guarantees: signals reach the container through docker kill, not the
    # CLI process that merely waits for it.
    def test_container_process_signals_the_container(self):
        cli = mock.Mock()
        cli.wait.return_value = 0
        process = faults.ContainerProcess(cli, temporary_directory(self) / "absent.cid",
                                          "series-fault-x-engine-1")
        with mock.patch.object(faults, "run_command") as run:
            process.terminate()
            process.kill()
        signals = [call.args[0][3] for call in run.call_args_list]
        self.assertEqual(signals, ["SIGTERM", "SIGKILL"])
        self.assertTrue(all(call.args[0][:2] == ["docker", "kill"] for call in run.call_args_list))
        cli.kill.assert_not_called()


class PreflightOutcome(unittest.TestCase):
    """What `measure fault-preflight` writes and returns when tools are missing."""

    def run_preflight(self, required):
        """A preflight whose images are absent, unpublished, in a temporary directory."""
        output = temporary_directory(self)
        with mock.patch.object(faults, "require_fault_images",
                               side_effect=unittest.SkipTest("the fault_tools image is absent")):
            try:
                faults.preflight_fault_tools(required, output_dir=output, publish=False)
            finally:
                self.document = json.loads((output / "fault-preflight.json").read_text())

    # Scenario: an optional preflight finds no tools image.
    # Guarantees: it skips, and still leaves a valid evidence file saying so.
    def test_optional_preflight_without_tools_skips_with_evidence(self):
        with self.assertRaisesRegex(unittest.SkipTest, "image is absent"):
            self.run_preflight(False)
        self.assertEqual(self.document["status"], measurement.STATUS_SKIPPED)
        self.assertEqual(self.document["probes"][0]["name"], "images")
        self.assertTrue(all(entry["status"] == "unavailable"
                            for entry in self.document["coverage"].values()))

    # Scenario: a required preflight finds no tools image.
    # Guarantees: it fails after writing the same evidence.
    def test_required_preflight_without_tools_fails_with_evidence(self):
        with self.assertRaisesRegex(AssertionError, "image is absent"):
            self.run_preflight(True)
        self.assertEqual(self.document["status"], measurement.STATUS_FAILED)
        self.assertEqual(self.document["coverage"]["slow"]["consequence"], "fails the lane")

    # Scenario: the command line runs the preflight.
    # Guarantees: the subcommand is implemented, a skip exits 3 and a
    # failure exits 1, so a caller can tell them apart.
    def test_command_line_exit_statuses(self):
        self.assertNotIn("fault-preflight", measure.PLANNED_COMMANDS)
        output = str(temporary_directory(self))
        for raised, status in ((unittest.SkipTest("x"), measure.FAULT_PREFLIGHT_SKIPPED_EXIT),
                               (AssertionError("x"), 1)):
            with self.subTest(raised=type(raised).__name__):
                with mock.patch.object(faults, "preflight_fault_tools", side_effect=raised):
                    self.assertEqual(
                        measure.main(["fault-preflight", "--output-dir", output]), status
                    )


def lease_wait_s():
    """How long the live slice waits for another measurement's host lease."""
    return float(os.environ.get("SERIES_FAULT_LEASE_WAIT_S", "3600"))


class LiveRigSlice(unittest.TestCase):
    """One real rig around one real store, under the host lease."""

    # Scenario: a rig is built around a running MinIO and then removed.
    # Guarantees: its entry probes pass on real tools (signed S3 through
    # both backends, NET_ADMIN only in the owner, nothing left over), the
    # engine storage section points at NGINX in the namespace, and cleanup
    # leaves no container or network of the run behind.
    def test_rig_round_trip_and_cleanup(self):
        faults.require_fault_images()
        root = temporary_directory(self)
        lease =measurement.HostLease(run_id="fault-rig-live")
        lease.acquire(deadline_ns=time.monotonic_ns() + int(lease_wait_s() * 10**9))
        self.addCleanup(lease.release)
        with test_e2e.DockerStore("minio") as store:
            rig = faults.FaultRig(store, root)
            with rig:
                self.assertEqual(rig.storage["s3"]["endpoint"], "http://127.0.0.1:19000")
                self.assertTrue(all(probe["passed"] for probe in rig.probes))
                rig.assert_clean()
            self.assertTrue(rig.cleanup_report["clean"], rig.cleanup_report)
            self.assertIsNone(store.network_address(rig.network_id))


if __name__ == "__main__":
    unittest.main()
