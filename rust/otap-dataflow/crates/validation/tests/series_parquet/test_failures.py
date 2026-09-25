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

    # Scenario: every probe the preflight runs passed.
    # Guarantees: disconnect/reset and the dropped completion response stay
    # unavailable, because no direct probe with a negative control exists
    # for them yet; the evidence names the task that owns each one.
    def test_unprobed_classes_are_not_claimed(self):
        probes = [{"name": name, "store": "minio", "passed": True}
                  for name in faults.PREFLIGHT_PROBES]
        for required in (False, True):
            classes = faults.coverage(probes, required=required)
            for name in ("disconnect_reset", "dropped_completion_response"):
                with self.subTest(name=name, required=required):
                    self.assertEqual(classes[name]["status"], "unavailable")
                    self.assertIn("Task 11", classes[name]["consequence"])
                    self.assertTrue(classes[name]["deferred_probes"])
            self.assertEqual(classes["tcp_ack_loss"]["status"], "available")

    # Scenario: the RustFS rig fails before any probe ran, while every MinIO
    # probe passed.
    # Guarantees: availability is per store -- every RustFS class is
    # unavailable, every MinIO class that its probes decide stays available,
    # and no class is available overall on MinIO's evidence alone.
    def test_failed_store_rig_is_not_covered_by_the_other_store(self):
        probes = [{"name": name, "store": "minio", "passed": True}
                  for name in faults.PREFLIGHT_PROBES]
        probes.append({"name": "rustfs rig", "store": "rustfs", "passed": False})
        classes = faults.coverage(probes, required=False, stores=["minio", "rustfs"])
        for name, entry in classes.items():
            with self.subTest(name=name):
                self.assertEqual(entry["status"], "unavailable")
                self.assertEqual(entry["stores"]["rustfs"]["status"], "unavailable")
                self.assertEqual(entry["consequence"], "skipped before traffic")
        for name in ("slow", "http503", "store_outage", "tcp_ack_loss",
                     "dns_nxdomain_timeout", "containerized_engine"):
            self.assertEqual(classes[name]["stores"]["minio"]["status"], "available", name)
        self.assertIn("xt_bpf", classes["tcp_ack_loss"]["stores"]["rustfs"]["missing_probes"])

    # Scenario: a store named by the preflight produced no probe at all.
    # Guarantees: an explicitly listed store without evidence is unavailable.
    def test_listed_store_without_probes_is_unavailable(self):
        probes = [{"name": name, "store": "minio", "passed": True}
                  for name in faults.PREFLIGHT_PROBES]
        classes = faults.coverage(probes, required=True, stores=["minio", "rustfs"])
        self.assertEqual(classes["slow"]["stores"]["rustfs"]["status"], "unavailable")
        self.assertEqual(classes["slow"]["consequence"], "fails the lane")
        only_minio = faults.coverage(probes, required=True, stores=["minio"])
        self.assertEqual(only_minio["slow"]["status"], "available")

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
            name="series-fault-x-tools", cidfile="/s/owner.cid", network="net",
            image="tools", run_id="x",
            control_dir="/c", artifact_dir="/a", engine_ports=(40001, 40002), cores=(1, 2),
        )
        toxiproxy = faults.toxiproxy_argv(name="series-fault-x-toxiproxy",
                                          cidfile="/s/toxiproxy.cid", owner=self.OWNER,
                                          image="toxiproxy", run_id="x")
        engine = faults.engine_argv(
            name="series-fault-x-engine-1", cidfile="/s/engine-1.cid", owner=self.OWNER,
            image="tools", run_id="x", argv=["/bin/df_engine", "--config", "/r/p.yaml"],
            mounts=[("/bin/df_engine", "ro"), ("/repo", "ro"), ("/r", "rw")],
            user="1000:1000", env={"RUST_LOG": "info", "AWS_SECRET_ACCESS_KEY": "s",
                                   "MALLOC_CONF": "stats_interval:1"},
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
    # Guarantees: the engine container inherits logging and allocator settings
    # only, and its binary and repository are read-only while its run
    # directory is not.
    def test_engine_container_mounts_and_environment(self):
        engine = self.argvs()["engine"]
        self.assertIn("RUST_LOG=info", engine)
        self.assertIn("MALLOC_CONF=stats_interval:1", engine)
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
            "1.7 PUT /b/otel/v=1/signal=logs/dataset=values/x.parquet 200 200 0.1 0.1 0 "
            "5243392 /b/otel/v=1/signal=logs/dataset=values/x.parquet?partNumber=1&uploadId=u\n"
        )
        entries = faults.parse_access_log(path)
        self.assertEqual(entries[0]["status"], "200")
        self.assertEqual(entries[1]["upstream_status"], "-")
        self.assertEqual(entries[2], {"raw": "truncated line"})
        self.assertEqual(entries[3]["request_length"], "5243392")
        self.assertTrue(entries[3]["request_uri"].endswith("?partNumber=1&uploadId=u"))
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
    # Guarantees: every other fault is still recovered, newest first, the
    # failure is raised rather than swallowed, and the failed one stays
    # active so it can be retried.
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
        self.assertEqual([entry["name"] for entry in rig.active], ["b"])
        self.assertEqual(len(rig.activations), 2)

    # Scenario: the plan's first fault names are looked up.
    # Guarantees: slow, http503 and store_outage are registered with both
    # an activation and a recovery.
    def test_plan_faults_are_registered(self):
        for name in ("slow", "http503", "store_outage"):
            activate, recover = faults.FAULTS[name]
            self.assertTrue(callable(activate) and callable(recover), name)

    # Scenario: the engine container is signalled.
    # Guarantees: signals reach the container through docker kill, by the id
    # Docker returned, not the CLI process that merely waits for it; with no
    # id there is nothing of this rig's to signal.
    def test_container_process_signals_the_container(self):
        cli = mock.Mock()
        cli.wait.return_value = 0
        cidfile = temporary_directory(self) / "engine-1.cid"
        process = faults.ContainerProcess(cli, cidfile, "series-fault-x-engine-1")
        with self.assertRaisesRegex(AssertionError, "no id"):
            process.terminate()
        cidfile.write_text("abc123\n")
        with mock.patch.object(faults, "run_command") as run:
            process.terminate()
            process.kill()
        signals = [call.args[0][3] for call in run.call_args_list]
        self.assertEqual(signals, ["SIGTERM", "SIGKILL"])
        self.assertTrue(all(call.args[0][:2] == ["docker", "kill"] for call in run.call_args_list))
        self.assertTrue(all(call.args[0][4] == "abc123" for call in run.call_args_list))
        cli.kill.assert_not_called()


def ok_command(stdout="", stderr="", status=0):
    """One run_command result."""
    return {"argv": [], "exit_status": status, "elapsed_s": 0.0,
            "stdout": stdout, "stderr": stderr}


class TeardownRig(faults.FaultRig):
    """A rig with its real teardown and Docker replaced by a recorder."""

    def __init__(self, root):
        super().__init__(store=mock.Mock(kind="minio", name="store"), root=root, probes=())
        self.state_dir = Path(root)
        self.network_id = "net-id"
        self.attached = True
        self.owner_id = "owner-id"
        self.baseline_rules = "-P INPUT ACCEPT\n"
        self.toxiproxy = mock.Mock()
        self.toxiproxy.toxics.return_value = []
        for role, ident in (("owner", "owner-id"), ("toxiproxy", "toxi-id")):
            cidfile = self.state_dir / f"{role}.cid"
            cidfile.write_text(ident)
            self.adopt_container(role, f"series-fault-x-{role}", cidfile)

    def exec(self, argv, *, timeout=60):
        if argv[:2] == ["iptables", "-w"] and argv[2:] == ["-S"]:
            return ok_command(self.baseline_rules)
        return ok_command()


class Teardown(unittest.TestCase):
    """The rig's cleanup: ownership by id, retained recovery, interrupts."""

    def setUp(self):
        self.calls = []

        def run(argv, *, timeout=60):
            self.calls.append([str(part) for part in argv])
            return ok_command()

        for patcher in (mock.patch.object(faults, "run_command", side_effect=run),
                        mock.patch.object(faults, "leftovers", return_value={
                            "containers": [], "networks": [], "commands_ok": True})):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.rig = TeardownRig(temporary_directory(self))

    def removed(self):
        """The ids `docker rm` was given, in order."""
        return [argv[-1] for argv in self.calls if argv[:2] == ["docker", "rm"]]

    # Scenario: a container name is reused but Docker wrote no id for it.
    # Guarantees: only containers whose id Docker returned are recorded, and
    # they are removed by that id, never by name.
    def test_containers_are_owned_by_returned_id(self):
        missing = self.rig.adopt_container("engine", "series-fault-x-engine-1",
                                           self.rig.state_dir / "never-written.cid")
        self.assertIsNone(missing)
        self.rig.__exit__(None, None, None)
        self.assertEqual(self.removed(), ["toxi-id", "owner-id"])
        self.assertTrue(self.rig.cleanup_report["clean"], self.rig.cleanup_report)

    # Scenario: a store outage's recovery fails once, inside a case.
    # Guarantees: the fault stays active instead of being forgotten, and the
    # rig's teardown retries it; every attempt is recorded.
    def test_failed_recovery_stays_active_and_is_retried(self):
        attempts = []

        def recover(_rig, state):
            attempts.append(state)
            if len(attempts) == 1:
                raise RuntimeError("store did not come back")
            return {"recovered": True}

        with mock.patch.dict(faults.FAULTS, {"flaky": (lambda _rig, p: dict(p), recover)}):
            self.rig.activate("flaky", {"id": 1})
            with self.assertRaisesRegex(AssertionError, "store did not come back"):
                self.rig.recover()
            self.assertEqual([entry["name"] for entry in self.rig.active], ["flaky"])
            self.rig.__exit__(None, None, None)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.rig.active, [])
        entry = self.rig.activations[0]
        self.assertEqual(len(entry["recovery_attempts"]), 2)
        self.assertIn("error", entry["recovery_attempts"][0])
        self.assertTrue(self.rig.cleanup_report["stages"]["faults"]["ok"])

    # Scenario: Ctrl-C arrives while the rig removes its first container.
    # Guarantees: the remaining container, the store attachment and the
    # network are still removed, and the interrupt is raised afterwards.
    def test_interrupt_mid_teardown_still_removes_everything(self):
        real = self.rig._remove_entry

        def interrupted(entry):
            if entry["id"] == "toxi-id":
                raise KeyboardInterrupt
            return real(entry)

        with mock.patch.object(self.rig, "_remove_entry", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                self.rig.__exit__(None, None, None)
        self.assertEqual(self.removed(), ["owner-id"])
        self.rig.store.detach.assert_called_once_with("net-id")
        self.assertIn(["docker", "network", "rm", "net-id"], self.calls)
        report = self.rig.cleanup_report
        self.assertFalse(report["clean"])
        self.assertFalse(report["stages"]["containers"]["ok"])
        for stage in ("store_attachment", "network", "leftovers"):
            self.assertTrue(report["stages"][stage]["ok"], stage)

    # Scenario: one teardown stage raises an ordinary error.
    # Guarantees: every later stage still runs and the report is not clean.
    def test_failing_stage_does_not_stop_later_stages(self):
        self.rig.toxiproxy.toxics.side_effect = RuntimeError("api gone")
        self.rig.__exit__(None, None, None)
        report = self.rig.cleanup_report
        self.assertFalse(report["stages"]["proxies"]["ok"])
        self.assertEqual(self.removed(), ["toxi-id", "owner-id"])
        self.assertFalse(report["clean"])


class Provenance(unittest.TestCase):
    """What a rig runs: images by id, and only a release engine."""

    def rig(self):
        """A rig with inspected images and no Docker behind it."""
        rig = faults.FaultRig(store=mock.Mock(kind="minio"), root=temporary_directory(self),
                              probes=())
        rig.images = {"fault_tools": {"tag": "series-measure-fault-tools:local",
                                      "id": "sha256:" + "a" * 64},
                      "toxiproxy": {"tag": faults.TOXIPROXY_IMAGE, "id": "sha256:" + "b" * 64}}
        rig.state_dir = rig.root
        return rig

    # Scenario: the tools tag is moved to another image after inspection.
    # Guarantees: containers are started from the inspected id, never the tag.
    def test_containers_run_the_inspected_image_id(self):
        rig = self.rig()
        self.assertEqual(rig.image("fault_tools"), "sha256:" + "a" * 64)
        with self.assertRaisesRegex(AssertionError, "never inspected"):
            rig.image("absent")

    # Scenario: DF_ENGINE names a debug or custom build.
    # Guarantees: the rig refuses it before running anything, by the
    # harness's own profile rule.
    def test_non_release_engine_is_refused(self):
        rig = self.rig()
        root = temporary_directory(self)
        for profile in ("debug", "profiling"):
            binary = root / "target" / profile / "df_engine"
            binary.parent.mkdir(parents=True)
            binary.write_bytes(b"not an engine")
            with self.subTest(profile=profile), mock.patch.object(faults, "run_command") as run:
                with self.assertRaisesRegex(AssertionError, "release df_engine only"):
                    rig.launcher.check_binary(binary)
                run.assert_not_called()
        self.assertEqual(measurement.build_profile(root / "target/release/df_engine"), "release")

    # Scenario: a release engine passes ldd.
    # Guarantees: ldd runs from the image id, its container is removed by
    # the id Docker returned, and the binary's hash is recorded.
    def test_release_engine_is_hashed_and_checked_by_image_id(self):
        rig = self.rig()
        binary = temporary_directory(self) / "target" / "release" / "df_engine"
        binary.parent.mkdir(parents=True)
        binary.write_bytes(b"engine bytes")
        calls = []

        def run(argv, *, timeout=60):
            calls.append([str(part) for part in argv])
            if argv[:2] == ["docker", "run"]:
                Path(argv[argv.index("--cidfile") + 1]).write_text("ldd-id")
                return ok_command("\tlibc.so.6 => /lib/libc.so.6\n")
            return ok_command()

        with mock.patch.object(faults, "run_command", side_effect=run):
            report = rig.launcher.check_binary(binary)
        self.assertTrue(report["compatible"])
        self.assertEqual(report["binary_sha256"], measurement.file_digest(binary))
        self.assertIn("sha256:" + "a" * 64, calls[0])
        self.assertEqual(calls[1], ["docker", "rm", "--force", "--volumes", "ldd-id"])


class StoreImage(unittest.TestCase):
    """Which image the store container is started from."""

    def started_image(self, **options):
        """The image argument `docker run` got for a store with `options`."""
        store = test_e2e.DockerStore("minio", **options)
        started = []

        def inspect(argv, **_kwargs):
            return mock.Mock(returncode=0, stdout="sha256:" + "c" * 64 + "\n")

        with mock.patch.object(test_e2e, "require_docker_image",
                               return_value="minio/minio:tag"), \
                mock.patch.object(test_e2e.subprocess, "run", side_effect=inspect), \
                mock.patch.object(test_e2e.DockerStore, "start_container",
                                  side_effect=lambda image: started.append(image)
                                  or (_ for _ in ()).throw(RuntimeError("stop"))), \
                mock.patch.object(test_e2e.DockerStore, "remove"):
            with self.assertRaisesRegex(RuntimeError, "stop"):
                store.__enter__()
        return store, started

    # Scenario: the fault preflight starts its store while the tag could move.
    # Guarantees: an opted-in store runs the inspected image id and records
    # both the tag and the id.
    def test_opted_in_store_starts_from_the_image_id(self):
        store, started = self.started_image(by_image_id=True)
        self.assertEqual(started, ["sha256:" + "c" * 64])
        self.assertEqual((store.image, store.image_id), ("minio/minio:tag", "sha256:" + "c" * 64))

    # Scenario: the legacy end-to-end suite starts its stores.
    # Guarantees: without the option nothing changes -- the tag is used.
    def test_legacy_store_keeps_the_tag(self):
        store, started = self.started_image()
        self.assertEqual(started, ["minio/minio:tag"])
        self.assertIsNone(store.image_id)


class AckLossVerdict(unittest.TestCase):
    """When the xt_bpf ACK-loss probe may pass."""

    RESTORED = {"status": 200, "bytes_verified": True}

    # Scenario: every part of the ACK-loss probe held.
    # Guarantees: the verdict has no problem to report.
    def test_complete_evidence_passes(self):
        self.assertEqual(faults.ack_loss_problems(
            dropped=4, captured=19, retransmissions=1, restored=self.RESTORED), [])

    # Scenario: the rule counted drops but the capture was empty (the
    # archived first preflight child).
    # Guarantees: an empty capture or no retransmission fails the probe.
    def test_empty_capture_or_no_retransmission_fails(self):
        for captured, retransmissions in ((0, 0), (19, 0), (None, 1)):
            with self.subTest(captured=captured, retransmissions=retransmissions):
                self.assertTrue(faults.ack_loss_problems(
                    dropped=4, captured=captured, retransmissions=retransmissions,
                    restored=self.RESTORED))

    # Scenario: after the rule is deleted, the store answers 403 (an
    # unsigned request) or the bytes cannot be read back.
    # Guarantees: only a 2xx signed transfer with verified bytes counts as a
    # restored route.
    def test_403_or_unverified_restoration_fails(self):
        for restored in ({"status": 403, "bytes_verified": False},
                         {"status": 200, "bytes_verified": False},
                         {"status": None, "bytes_verified": False}, None):
            with self.subTest(restored=restored):
                problems = faults.ack_loss_problems(
                    dropped=4, captured=19, retransmissions=1, restored=restored)
                self.assertTrue(any("did not succeed" in problem for problem in problems))

    # Scenario: the rule never matched.
    # Guarantees: zero dropped ACKs fails even with packets and a restored route.
    def test_no_dropped_ack_fails(self):
        self.assertTrue(faults.ack_loss_problems(
            dropped=0, captured=19, retransmissions=1, restored=self.RESTORED))


class EvidenceScrub(unittest.TestCase):
    """Host paths in container command lines, as the evidence records them."""

    # Scenario: the engine's run directory is bind-mounted at its own path.
    # Guarantees: both sides of `-v SRC:DST[:MODE]` and both `--mount`
    # paths lose their host prefix; container-only paths are kept.
    def test_bind_specifications_are_scrubbed_on_both_sides(self):
        scrub = measurement.scrub_published
        self.assertEqual(scrub("/tmp/series-fault-preflight/rigs/minio/engine-probe:"
                               "/tmp/series-fault-preflight/rigs/minio/engine-probe:rw"),
                         "<host-path>/engine-probe:<host-path>/engine-probe:rw")
        self.assertEqual(scrub("/tmp/r/fault-control:/control:ro"),
                         "<host-path>/fault-control:/control:ro")
        self.assertEqual(scrub("type=bind,source=/tmp/q/r,target=/tmp/q/r"),
                         "type=bind,source=<host-path>/r,target=<host-path>/r")
        self.assertEqual(scrub("/etc/a.conf:/etc/nginx/nginx.conf:ro"),
                         "/etc/a.conf:/etc/nginx/nginx.conf:ro")
        self.assertEqual(scrub("http://127.0.0.1:9000/tmp/x"), "http://127.0.0.1:9000/tmp/x")
        # Evidence scrubbed by the earlier, source-only rule is completed.
        self.assertEqual(scrub("<host-path>/engine-probe:/tmp/r/engine-probe:rw"),
                         "<host-path>/engine-probe:<host-path>/engine-probe:rw")
        once = scrub("/tmp/a/b:/tmp/a/b:rw")
        self.assertEqual(scrub(once), once)


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
        with test_e2e.DockerStore("minio", by_image_id=True) as store:
            rig = faults.FaultRig(store, root)
            with rig:
                self.assertEqual(rig.storage["s3"]["endpoint"], "http://127.0.0.1:19000")
                self.assertTrue(all(probe["passed"] for probe in rig.probes))
                rig.assert_clean()
                self.assertEqual(rig.evidence()["store"]["running_image_id"], store.image_id)
            self.assertTrue(rig.cleanup_report["clean"], rig.cleanup_report)
            self.assertIsNone(store.network_address(rig.network_id))


def passing_fault_result(**metrics):
    """A fault case result whose every required check passed."""
    checks = [measurement.check(name, measurement.CHECK_HARD, measurement.STATUS_PASSED)
              for name in faults.FAULT_CHECKS]
    return {"checks": checks, "metrics": dict({"missing_acked_records": 0}, **metrics),
            "observations": {"fault": {"multiplicity_histogram": {"1": 2000},
                                       "numbers": {"duplicate_records": 0}}}}


class FaultCaseContracts(unittest.TestCase):
    """How a fault case reads its evidence and decides its verdicts, without Docker."""

    # Scenario: the committed http503 strict MinIO run, which left one
    # multipart upload no abort failure accounts for, is judged.
    # Guarantees: fault_check fails every result its published status fails,
    # here on the orphaned upload.
    def test_fault_check_fails_the_committed_http503_run(self):
        path = (measurement.resolve_report_dir(None)
                / "failure-s3-http503-strict-minio-c1-w5-r001.json")
        result = json.loads(path.read_text(encoding="ascii"))
        self.assertEqual(result["status"], measurement.STATUS_FAILED)
        with self.assertRaisesRegex(AssertionError, "orphaned_uploads_expected"):
            faults.fault_check(result)

    # Scenario: a fault case result is judged.
    # Guarantees: every required check must be present and every hard check
    # passed, an acknowledged record may not be missing, and the duplicates
    # must have been measured.
    def test_fault_check_requires_every_check_and_no_missing_record(self):
        faults.fault_check(passing_fault_result())
        result = passing_fault_result()
        result["checks"].append(measurement.check("affinity_matched", measurement.CHECK_HARD,
                                                  measurement.STATUS_FAILED, "moved"))
        with self.assertRaisesRegex(AssertionError, "affinity_matched"):
            faults.fault_check(result)
        result = passing_fault_result()
        result["checks"] = [entry for entry in result["checks"]
                            if entry["name"] != "partition_lateness_bound"]
        with self.assertRaisesRegex(AssertionError, "partition_lateness_bound: never checked"):
            faults.fault_check(result)
        for name in faults.FAULT_CHECKS:
            result = passing_fault_result()
            next(entry for entry in result["checks"] if entry["name"] == name)["status"] = \
                measurement.STATUS_FAILED
            with self.subTest(check=name), self.assertRaisesRegex(AssertionError, name):
                faults.fault_check(result)
        with self.assertRaisesRegex(AssertionError, "acknowledged supported record missing"):
            faults.fault_check(passing_fault_result(missing_acked_records=3))
        result = passing_fault_result()
        result["observations"]["fault"]["multiplicity_histogram"] = None
        with self.assertRaisesRegex(AssertionError, "histogram"):
            faults.fault_check(result)
        result = passing_fault_result()
        result["observations"]["fault"]["numbers"]["duplicate_records"] = None
        with self.assertRaisesRegex(AssertionError, "duplicates"):
            faults.fault_check(result)

    # Scenario: a straddling cell is scheduled at several instants of an hour.
    # Guarantees: it arms ten seconds before an hour end that leaves it the
    # full start lead, never waits more than one hour plus that lead, and
    # holds the lease before arming no longer than a baseline plus a window.
    def test_straddle_arms_before_the_first_reachable_hour_end(self):
        self.assertLessEqual(faults.STRADDLE_LEAD_S,
                             2 * faults.FAULT_BASELINE_S + faults.FAULT_INTERVAL_S)
        hour = 1790290800
        for now in (hour - 3600, hour - 101, hour - 99, hour - 1):
            with self.subTest(now=now):
                arm = faults.straddle_arm_instant(now)
                self.assertEqual((arm + faults.STRADDLE_ARM_BEFORE_END_S) % 3600, 0)
                self.assertGreaterEqual(arm - now, faults.STRADDLE_LEAD_S)
                self.assertLessEqual(arm - now, faults.STRADDLE_MAX_WAIT_S)

    # Scenario: the lateness bound is computed from the exporter settings.
    # Guarantees: it is FORMAT.md's L = interval + 2 * (flush_retry_deadline +
    # abort_timeout): 145 s at the defaults.
    def test_lateness_bound_follows_the_format_rule(self):
        settings = {"window": {"interval": "15s", "flush_retry_deadline": "60s"},
                    "upload": {"abort_timeout": "5s"}}
        self.assertEqual(faults.lateness_bound_s(settings), 145.0)
        settings["window"]["interval"] = "5s"
        self.assertEqual(faults.lateness_bound_s(settings), 135.0)
        self.assertEqual(faults.duration_s("200ms"), 0.2)

    # Scenario: objects of one partition hour become visible before, just
    # after and long after the hour ended.
    # Guarantees: only an object visible more than L after its hour's end is a
    # violation, by the store's LastModified or by the first listing that saw it.
    def test_partition_lateness_flags_only_objects_beyond_the_bound(self):
        end = faults.partition_hour("otel/v=1/signal=logs/dataset=values/date=2026-09-25/"
                                    "hour=03/a.parquet")[1]
        key = "otel/v=1/signal=logs/dataset=values/date=2026-09-25/hour=03/{}.parquet"
        objects = [
            {"key": key.format("early"), "last_modified_unix_s": end - 30,
             "first_listed_unix_s": end - 29},
            {"key": key.format("late"), "last_modified_unix_s": end + 40,
             "first_listed_unix_s": None},
        ]
        report = faults.partition_lateness(objects, 135.0)
        self.assertEqual(report["violations"], [])
        self.assertEqual(report["objects_visible_after_hour_end_count"], 1)
        self.assertEqual(report["hours"]["2026-09-25T03"]["latest_last_modified_after_end_s"], 40)
        objects[0]["first_listed_unix_s"] = end + 136
        self.assertEqual(faults.partition_lateness(objects, 135.0)["violations"],
                         ["2026-09-25T03"])

    # Scenario: NGINX logs the exporter's multipart upload and plain requests.
    # Guarantees: each S3 operation is named from the method and the query of
    # the full request URI, and only the exporter's prefix counts.
    def test_s3_operations_are_named_from_the_query(self):
        base = "/b/otel/v=1/signal=logs/dataset=values/x.parquet"
        cases = {
            ("POST", base + "?uploads"): "create_multipart_upload",
            ("PUT", base + "?partNumber=2&uploadId=u"): "upload_part",
            ("POST", base + "?uploadId=u"): "complete_multipart_upload",
            ("DELETE", base + "?uploadId=u"): "abort_multipart_upload",
            ("PUT", base): "put_object",
            ("HEAD", base): "head_object",
            ("GET", "/b?list-type=2&prefix=otel%2F"): "list_objects",
        }
        for (method, uri), expected in cases.items():
            with self.subTest(uri=uri):
                entry = {"method": method, "uri": uri.partition("?")[0], "request_uri": uri}
                self.assertEqual(faults.s3_operation(entry), expected)
        entries = [{"method": "PUT", "uri": "/b/fault-preflight/k", "request_uri": "/b/fault-preflight/k"},
                   {"method": "PUT", "uri": base, "request_uri": base}]
        self.assertEqual([entry["uri"] for entry in faults.engine_requests(entries, "b")], [base])

    # Scenario: the route saw a multipart upload but the store holds only
    # single-part objects, and then both sides agree.
    # Guarantees: multipart counts as exercised only with every step answered
    # 2xx and a stored object above one part carrying a multipart ETag.
    def test_multipart_evidence_needs_both_sides(self):
        requests = [{"operation": op, "status": "200"} for op in (
            "create_multipart_upload", "upload_part", "upload_part",
            "complete_multipart_upload")]
        single = [{"key": "k", "size_bytes": 7 << 20, "etag": '"' + "a" * 32 + '"'}]
        multi = [{"key": "k", "size_bytes": 7 << 20, "etag": '"' + "a" * 32 + '-2"'}]
        self.assertFalse(faults.multipart_evidence(requests, single, 5 << 20)["exercised"])
        evidence = faults.multipart_evidence(requests, multi, 5 << 20)
        self.assertTrue(evidence["exercised"])
        self.assertEqual(evidence["parts_per_object"], [2])
        self.assertFalse(faults.multipart_evidence(requests[:2], multi, 5 << 20)["exercised"])

    # Scenario: the producer ledgered storage nacks, a receiver refusal, a
    # client deadline and acknowledgements.
    # Guarantees: the exporter's storage sentence is counted by class, apart
    # from producer-local timeouts and from other retryable refusals.
    def test_ledger_attempts_classify_storage_nacks_and_local_timeouts(self):
        ledger = measurement.Ledger(temporary_directory(self) / "ledger.sqlite")
        self.addCleanup(ledger.close)
        retryable = measurement.OUTCOME_RETRYABLE
        ledger.attempt(0, 1, 10, 11, retryable, "StatusCode.UNAVAILABLE: could not write to "
                       "object storage (unavailable); retry the request")
        ledger.attempt(0, 2, 12, 13, measurement.OUTCOME_ACK)
        ledger.attempt(1, 1, 10, 11, retryable, "StatusCode.DEADLINE_EXCEEDED: Deadline Exceeded")
        ledger.attempt(2, 1, 10, 11, retryable, "StatusCode.RESOURCE_EXHAUSTED: too many")
        report = faults.ledger_attempts(ledger)
        self.assertEqual(report["storage_nacks_count"], 1)
        self.assertEqual(report["storage_nack_classes"], {"unavailable": 1})
        self.assertEqual(report["local_timeouts_count"], 1)
        self.assertEqual(report["by_code"]["RESOURCE_EXHAUSTED"], 1)
        self.assertEqual(faults.ledger_attempts(ledger, since_ns=12)["by_outcome"],
                         {measurement.OUTCOME_ACK: 1})

    # Scenario: records are stored twice, some from a request the producer
    # resent and some from a request it sent once.
    # Guarantees: duplicates outside the producer's resent requests are
    # counted apart, so they cannot hide among expected replays.
    def test_duplicate_attribution_splits_resent_requests(self):
        ledger = measurement.Ledger(temporary_directory(self) / "ledger.sqlite")
        self.addCleanup(ledger.close)
        for request, rows in ((0, ["a", "b"]), (1, ["c"])):
            ledger.add_request(request, "logs", b"wire%d" % request,
                               [(row, "log", "h") for row in rows], send_ns=1)
        ledger.attempt(0, 1, 1, 2, measurement.OUTCOME_RETRYABLE, "x")
        ledger.attempt(0, 2, 3, 4, measurement.OUTCOME_ACK)
        ledger.attempt(1, 1, 1, 2, measurement.OUTCOME_ACK)
        measurement._load_actual(ledger, iter([(row, "logs", "h")
                                               for row in ("a", "a", "b", "c", "c", "c")]))
        report = faults.duplicate_attribution(ledger)
        self.assertEqual(report["duplicated_records"], 2)
        self.assertEqual(report["extra_copies_records"], 3)
        self.assertEqual(report["duplicated_in_resent_requests_records"], 1)
        self.assertEqual(report["duplicated_outside_resent_requests_records"], 1)
        self.assertEqual(report["resent_requests_count"], 1)

    # Scenario: buffered duplicates, few enough to fit in the nacked bundles'
    # records, where one duplicated record has no copy in a failed block's file.
    # Guarantees: a buffered duplicate is explained only by a copy in a file
    # of a block whose flush failed, never by a count budget.
    def test_buffered_duplicates_need_a_copy_in_a_failed_block(self):
        root = temporary_directory(self)
        workload = measurement.Workload(requests=2, records_per_request=2)
        width = measurement.ID_FIXED_WIDTH + len(measurement.LOG_KIND)
        ids = [measurement.stable_id(workload.seed, request, point, measurement.LOG_KIND)
               for request in range(2) for point in range(2)]
        self.assertEqual({len(record) for record in ids}, {width})
        partition = root / "v=1/signal=logs/dataset=values/date=d/hour=h"
        partition.mkdir(parents=True)
        with measurement.duckdb.connect() as db:
            for name, bodies in (("part-a-00000003.parquet", [ids[0] + "x"]),
                                 ("part-a-00000004.parquet", [ids[1] + "x", ids[2] + "x"])):
                rows = ", ".join(f"('{body}')" for body in bodies)
                db.execute(f"COPY (SELECT * FROM (VALUES {rows}) t(body)) TO "
                           f"'{partition / name}' (FORMAT parquet)")
        failed = faults.failed_block_record_ids(root, workload, ["part-a-00000003.parquet"])
        self.assertEqual(failed, {ids[0]})
        ledger = measurement.Ledger(root / "ledger.sqlite")
        self.addCleanup(ledger.close)
        for request in range(2):
            ledger.add_request(request, "logs", b"w%d" % request,
                               [(ids[2 * request + point], "log", "h") for point in range(2)],
                               send_ns=1)
            ledger.attempt(request, 1, 1, 2, measurement.OUTCOME_ACK)
        stored = [ids[0], ids[0], ids[1], ids[2], ids[3]]
        measurement._load_actual(ledger, iter([(record, "logs", "h") for record in stored]))
        explained, _why = faults.duplicates_explained(
            True, faults.duplicate_attribution(ledger, failed))
        self.assertTrue(explained)
        stored.append(ids[3])
        measurement._load_actual(ledger, iter([(record, "logs", "h") for record in stored]))
        report = faults.duplicate_attribution(ledger, failed)
        self.assertEqual(report["duplicated_outside_failed_blocks_records"], 1)
        explained, why = faults.duplicates_explained(True, report)
        self.assertFalse(explained, why)

    # Scenario: the engine log carries cleanup, failure and commit events with
    # colour codes.
    # Guarantees: flush events are counted by name and outcome, and other
    # events are ignored.
    def test_engine_events_count_cleanup_outcomes(self):
        log = (
            "\x1b[2m2026\x1b[0m  \x1b[33mWARN \x1b[0m \x1b[1motel.exporter.series_parquet::"
            "series_parquet.flush.cleanup\x1b[0m: [outcome=abort_failed, seq=4, attempt=3]\n"
            "2026  INFO otel.exporter.series_parquet::series_parquet.flush.cleanup: "
            "a failed block's objects exist [outcome=late_commit, seq=5, file=p-5.parquet]\n"
            "2026  ERROR otel.exporter.series_parquet::series_parquet.flush.failed: "
            "[window_start=1, seq=5, file=p-5.parquet, requests=2]\n"
            "2026  WARN otel.exporter.series_parquet::series_parquet.flush.attempt_failed: "
            "[seq=5, attempt=1]\n"
            "2026  INFO otel.exporter.series_parquet::series_parquet.block_committed: [files=2]\n"
        )
        events = faults.engine_events(log)
        self.assertEqual(events["counts"], {
            "series_parquet.flush.attempt_failed": 1,
            "series_parquet.flush.cleanup{abort_failed}": 1,
            "series_parquet.flush.cleanup{late_commit}": 1,
            "series_parquet.flush.failed": 1,
        })
        self.assertEqual(events["failed_files"], ["p-5.parquet"])
        self.assertEqual(events["cleanup_files"], {"late_commit": ["p-5.parquet"]})
        objects = [{"key": "otel/v=1/signal=logs/dataset=values/date=d/hour=h/p-5.parquet"},
                   {"key": "otel/v=1/signal=logs/dataset=values/date=d/hour=h/p-6.parquet"}]
        self.assertEqual(faults.failed_block_objects(events, objects),
                         {"p-5.parquet": [objects[0]["key"]]})

    # Scenario: an outage's qualifying deadline failure came from a block
    # sealed 4.6 s after arming, and the fault has been held 70 s.
    # Guarantees: the outage is held until the flush deadline plus a 15 s
    # margin after that block was sealed, not removed at its failure.
    def test_outage_is_held_past_the_failed_block_deadline(self):
        case = mock.Mock(flush_deadline_s=60.0)
        seen = {"elapsed_s": 70.0, "flush_failures_count": 1, "storage_nacks_count": 100,
                "retried": True, "deadline_failures_after_deadline": [64.6],
                "hold_until_s": 4.6 + 60.0 + faults.OUTAGE_HOLD_MARGIN_S}
        self.assertFalse(faults.FAULT_CONDITIONS["store_outage"](case, seen))
        self.assertTrue(faults.FAULT_CONDITIONS["store_outage"](case, dict(seen, elapsed_s=80.0)))

    # Scenario: the harness runs from a git worktree of the repository.
    # Guarantees: raw fault-case archives default to the main checkout's
    # artifact directory, never to the worktree's.
    def test_archives_default_to_the_main_checkout(self):
        self.assertEqual(faults.FAULT_ARCHIVE_DIR,
                         faults.main_checkout() / ".measurement-artifacts" / "failure-s3")
        self.assertNotIn(".claude", faults.FAULT_ARCHIVE_DIR.parts)

    # Scenario: during an outage a flush failed 59.5 s after the stop and the
    # timer has since passed the 60 s flush deadline.
    # Guarantees: the outage condition needs a deadline-class failure whose
    # own logged time is at least the flush deadline after the stop; the
    # timer and a failure count are not enough.
    def test_outage_condition_needs_a_failure_after_the_deadline(self):
        case = mock.Mock(flush_deadline_s=60.0)
        seen = {"elapsed_s": 61.0, "flush_failures_count": 1, "storage_nacks_count": 100,
                "retried": True, "deadline_failures_after_deadline": []}
        self.assertFalse(faults.FAULT_CONDITIONS["store_outage"](case, seen))
        log = temporary_directory(self) / "engine.log"
        log.write_text(
            '{"jemalloc":{}}2026-09-24T23:09:10.018Z  ERROR otel.exporter.series_parquet::'
            "series_parquet.flush.failed: [window_start=1790291285, seq=4, file=p-4.parquet, "
            "requests=100, error_type=deadline, error=x]\n"
            "2026-09-24T23:09:15.000Z  ERROR otel.exporter.series_parquet::"
            "series_parquet.flush.failed: [window_start=1790291290, seq=5, file=p-5.par")
        tail = faults.LogTail(log)
        failures = faults.flush_failures(tail.lines())
        self.assertEqual(failures, [{"unix_s": 1790291350.018, "error_type": "deadline",
                                     "window_start_unix_s": 1790291285, "file": "p-4.parquet"}])
        with log.open("a") as handle:
            handle.write("quet, error_type=deadline]\n")
        self.assertEqual([entry["file"] for entry in faults.flush_failures(tail.lines())],
                         ["p-5.parquet"])

    # Scenario: stored outage runs are re-judged; one's only deadline failure
    # was logged 59.5 s after arming, another's 64.6 s, and a buffered run
    # stored duplicates without their failed-block copies.
    # Guarantees: re-judging fails the first fault_observed, keeps the second,
    # and reports the undecidable duplicates as not re-judged, never passed.
    def test_rejudge_outage_runs_from_stored_evidence(self):
        def run(failure_after_s, topology="strict", duplicated=0):
            armed = 1790291290.0
            return {
                "run_id": "r", "config": {"requested": {"topology": topology}, "effective": {
                    "groups": {"default": {"pipelines": {"main": {"nodes": {"exporter": {
                        "config": {"window": {"flush_retry_deadline": "60s"}}}}}}}}}},
                "checks": [measurement.check(name, measurement.CHECK_HARD,
                                             measurement.STATUS_PASSED)
                           for name in faults.FAULT_CHECKS],
                "observations": {"fault": {
                    "fault": "store_outage",
                    "states": [{"state": "armed", "unix_s": armed},
                               {"state": "fault_removed", "unix_s": armed + 66}],
                    "duplicates": {"duplicated_records": duplicated,
                                   "duplicated_outside_resent_requests_records": 0},
                    "engine_events": {"flush_failures": [{
                        "unix_s": armed + failure_after_s, "error_type": "deadline",
                        "window_start_unix_s": int(armed) - 5, "file": "p.parquet"}]}}}}

        def verdict(result, name):
            return next(entry for entry in faults.rejudge_fault_checks(result, "/nonexistent")
                        if entry["check"] == name)

        self.assertEqual(verdict(run(59.5), "fault_observed")["rejudged"],
                         measurement.STATUS_FAILED)
        self.assertEqual(verdict(run(64.6), "fault_observed")["rejudged"],
                         measurement.STATUS_PASSED)
        self.assertEqual(verdict(run(64.6, "buffered"), "duplicates_explained")["rejudged"],
                         measurement.STATUS_PASSED)
        self.assertIsNone(verdict(run(64.6, "buffered", 10), "duplicates_explained")["rejudged"])

    # Scenario: each fault's intended condition is judged from partial evidence.
    # Guarantees: a 503 or an outage counts only with the exporter's nack and
    # its retry (an outage also past the flush deadline), and slow storage
    # only with a measured delay, a flush with work behind it and backpressure.
    def test_fault_conditions_need_every_part(self):
        case = mock.Mock(flush_deadline_s=60.0)
        seen = {"http_503_writes_count": 2, "flush_retries_count": 1,
                "storage_nacks_count": 1, "retried": True}
        self.assertTrue(faults.FAULT_CONDITIONS["http503"](case, seen))
        self.assertFalse(faults.FAULT_CONDITIONS["http503"](case, dict(seen, retried=False)))
        outage = {"elapsed_s": 80.0, "flush_failures_count": 1, "storage_nacks_count": 1,
                  "retried": True, "deadline_failures_after_deadline": [64.6],
                  "hold_until_s": 79.6}
        self.assertTrue(faults.FAULT_CONDITIONS["store_outage"](case, outage))
        slow = {"delayed_requests_count": 1, "throttled_uploads_count": 1,
                "flushing_with_work_samples_count": 3, "admission_closed_s": 5.5,
                "receiver_rejections_count": 0, "buffer_in_flight_plateau": False}
        self.assertTrue(faults.FAULT_CONDITIONS["slow"](case, slow))
        self.assertFalse(faults.FAULT_CONDITIONS["slow"](case, dict(slow, admission_closed_s=1.0)))
        self.assertFalse(faults.FAULT_CONDITIONS["slow"](case, dict(slow,
                                                                   throttled_uploads_count=0)))


class FailurePrerequisites(unittest.TestCase):
    """How a fault case treats missing tools before traffic and errors after it."""

    def run_case(self, **env):
        """failure_case with the fault-tools image absent, the case body refused."""
        missing = mock.patch.object(faults, "image_provenance", return_value=None)
        docker = mock.patch.object(faults, "run_command", return_value=ok_command("27.0"))
        body = mock.patch.object(measure, "run_case",
                                 side_effect=AssertionError("the case body ran"))
        with environment(**env), missing, docker, body as run_case:
            try:
                faults.failure_case("s3", "http503", "strict", "minio",
                                    temporary_directory(self))
            finally:
                self.assertFalse(run_case.called)

    # Scenario: a fault case starts where the fault-tools image is absent and
    # the tools are optional.
    # Guarantees: the case skips cleanly before any lease or traffic.
    def test_missing_prerequisite_skips_an_optional_lane(self):
        with self.assertRaisesRegex(unittest.SkipTest, "absent"):
            self.run_case()

    # Scenario: the same case where the fault tools are required.
    # Guarantees: the missing image fails the case instead of skipping it.
    def test_missing_prerequisite_fails_a_required_lane(self):
        with self.assertRaisesRegex(AssertionError, "absent"):
            self.run_case(SERIES_REQUIRE_FAULT_TOOLS="1")

    # Scenario: after a successful preflight, arming the case's fault raises a
    # skip in a lane that does not require the tools.
    # Guarantees: the case fails; a post-preflight activation error is never a skip.
    def test_activation_error_after_preflight_is_a_failure(self):
        with mock.patch.object(faults, "require_fault_images", return_value={}):
            rig = FakeRig(temporary_directory(self))
        spec = faults.failure_spec("s3", "http503", "strict", "minio", cores=(1,))
        settings = {"window": {"flush_retry_deadline": "60s"}}
        phase = mock.Mock()
        phase.sampler.samples = []
        case = faults.FaultCase(spec, rig, mock.Mock(), None, phase, None, None, settings)

        def vanished(_rig, _parameters):
            raise unittest.SkipTest("control directory vanished")

        with environment(), mock.patch.dict(faults.FAULTS, {"http503": (vanished, None)}):
            with self.assertRaisesRegex(AssertionError, "activating http503 after preflight"):
                case.arm()
        self.assertIsNone(case.at("armed"))


class S3FailureTests(measurement.MeasurementTestCase):
    """A real S3 fault reaches each topology on each store, and it recovers."""

    # Scenario: a real S3 HTTP 503 reaches each topology before service recovers.
    # Guarantees: recovery preserves every ACKed ID and reports replay multiplicities.
    def test_http503_recovery(self):
        measurement.require_long()
        for topology in ("strict", "buffered"):
            for store in ("minio", "rustfs"):
                with self.subTest(topology=topology, store=store):
                    result = faults.failure_case("s3", "http503", topology, store,
                                                 self.output_dir, report_dir=self.output_dir,
                                                 archive_dir=self.output_dir / "archive")
                    self.assertGreater(
                        result["observations"]["fault"]["numbers"]["http_503_responses_count"], 0)
                    faults.fault_check(result)


if __name__ == "__main__":
    unittest.main()
