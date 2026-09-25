# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Disposable fault tools for the series Parquet failure measurements.

A `FaultRig` builds one private Docker bridge network per run, attaches the
caller's `DockerStore` to it as `store`, and starts a fault-tools container
that owns a network namespace: NGINX in front of the store, plus dnsmasq,
iptables, tcpdump and tshark. That owner is the only container given an
added capability, `NET_ADMIN`, and only inside its own namespace: no host
networking, no `--privileged`, no host firewall change and no module
loading. A Toxiproxy sidecar and the engine itself join the owner's
namespace with `--network container:OWNER`, so the proxy's loopback
listeners are real and every DNS, firewall and capture rule applies to the
engine's own traffic.

    engine --127.0.0.1:19000--> nginx --19001 general--> toxiproxy --> store
                                      \\--19002 values--/

Faults are real behaviour of real tools, driven through their own control
surfaces: Toxiproxy's HTTP API, NGINX's control file, the store container's
lifecycle and iptables rules installed in the owner with `docker exec`.
Nothing in this module serves storage traffic itself.

`preflight_fault_tools` is `measure fault-preflight`: before any workload it
proves the signed S3 route through both backends, probes UDP and TCP DNS
blocking, the `xt_bpf` match, packet capture and the container capability
split, smoke-tests the registered activations, and checks that nothing a
probe installed is left behind. With `SERIES_REQUIRE_FAULT_TOOLS=1` any
failed probe is fatal; otherwise a failed probe skips cleanly, after
cleanup, before any case traffic. A failure after a successful preflight is
always a failure.

Nothing here pulls or builds an image: provisioning is a separate step
(README, "Fault tools"), finished before the host lease is taken.
"""
import collections
import dataclasses
import datetime
import functools
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
import yaml

try:  # Imported as a package module by `python3 -m crates...`.
    from . import capacity
    from . import measurement
    from . import performance
    from . import test_e2e
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import capacity
    import measurement
    import performance
    import test_e2e

HERE = Path(__file__).resolve().parent
NGINX_CONF = HERE / "fault-nginx.conf"
DOCKERFILE = HERE / "fault-tools.Dockerfile"

# The images, by the tags provisioning gives them. The evidence records
# their immutable ids and digests, never only these mutable names.
FAULT_TOOLS_IMAGE = "series-measure-fault-tools:local"
TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.12.0"
TOXIPROXY_VERSION = "2.12.0"
# The packages whose versions the evidence records for the tools image.
TOOL_PACKAGES = (
    "nginx", "dnsmasq", "dnsutils", "iproute2", "iptables", "tcpdump",
    "tshark", "ca-certificates", "libssl3t64", "libstdc++6", "procps", "curl",
)

# Namespace-local ports. They are fixed because every rig has a namespace of
# its own; only ports the harness chooses are published on host loopback.
NGINX_PORT = 19000
PROXY_PORTS = {"general": 19001, "values": 19002}
TOXIPROXY_API_PORT = 8474
STORE_ALIAS = "store"
STORE_PORT = 9000

# Where the owner container sees the run's directories.
CONTROL_MOUNT = "/control"
ARTIFACT_MOUNT = "/artifacts"
FAIL503_FILE = "fail503"
ACCESS_LOG = "nginx-access.log"

# Every container and network a rig creates carries this label with the
# rig's run id, so cleanup can prove nothing of this run is left.
RUN_LABEL = "series-fault-run"

NET_ADMIN = "NET_ADMIN"
# Bit numbers of the Linux capabilities, for decoding /proc CapEff.
CAPABILITY_NAMES = (
    "CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID", "KILL",
    "SETGID", "SETUID", "SETPCAP", "LINUX_IMMUTABLE", "NET_BIND_SERVICE",
    "NET_BROADCAST", "NET_ADMIN", "NET_RAW", "IPC_LOCK", "IPC_OWNER",
    "SYS_MODULE", "SYS_RAWIO", "SYS_CHROOT", "SYS_PTRACE", "SYS_PACCT",
    "SYS_ADMIN", "SYS_BOOT", "SYS_NICE", "SYS_RESOURCE", "SYS_TIME",
    "SYS_TTY_CONFIG", "MKNOD", "LEASE", "AUDIT_WRITE", "AUDIT_CONTROL",
    "SETFCAP", "MAC_OVERRIDE", "MAC_ADMIN", "SYSLOG", "WAKE_ALARM",
    "BLOCK_SUSPEND", "AUDIT_READ", "PERFMON", "BPF", "CHECKPOINT_RESTORE",
)

# The DNS probe's private zone: a name only the rig's dnsmasq answers, with
# a documentation address (RFC 5737) nothing routes to.
PROBE_DNS_NAME = "probe.series-faults.test"
PROBE_DNS_ADDRESS = "192.0.2.53"
DNS_LISTEN = "127.0.0.1"
# How long one blocked lookup may wait, and the slack a bounded timeout is
# allowed on top of it.
DNS_TIMEOUT_S = 2
DNS_TIMEOUT_SLACK_S = 3.0

# The ACK-only filter of spec'd pure TCP ACK loss: segments from the store
# with only the ACK flag and no payload. Task 11 reuses the compiled form.
ACK_ONLY_FILTER = (
    "src host {store_ip} and src port {store_port} and tcp[13] = 16 and "
    "(ip[2:2] - ((ip[0] & 15) << 2) - ((tcp[12] & 240) >> 2)) = 0"
)

# The slow-storage toxics, in Toxiproxy's own units: `rate` in KB/s and
# `latency`/`jitter` in milliseconds.
SLOW_RATE_KBPS = 256
SLOW_LATENCY_MS = 1500
TOXIC_UNITS = {"bandwidth.rate": "KB/s", "latency.latency": "ms", "latency.jitter": "ms"}

# The largest command output kept in the evidence, per stream.
OUTPUT_LIMIT = 4000

# How long the whole preflight waits for another measurement's lease.
DEFAULT_LEASE_WAIT_S = 4 * 3600

DOCKER_TIMEOUT_S = test_e2e.DOCKER_TIMEOUT_S

# The only engine build a rig launches, as for every measured case.
RELEASE_PROFILE = "release"


# --------------------------------------------------------------------------
# Required versus optional coverage
# --------------------------------------------------------------------------

def fault_tools_required() -> bool:
    """Whether this run must have every fault tool: `SERIES_REQUIRE_FAULT_TOOLS=1`."""
    return os.environ.get("SERIES_REQUIRE_FAULT_TOOLS") == "1"


def require_probe(probe: dict, *, required: bool) -> None:
    """Return only for a probe that passed; otherwise fail or skip.

    `passed` must be exactly `True`: a probe that recorded nothing, or
    recorded anything else, did not pass. A required lane raises
    AssertionError, so missing coverage is fatal rather than silent; an
    optional lane raises `unittest.SkipTest`, which must happen only before
    case traffic and after cleanup.
    """
    if probe.get("passed") is True:
        return
    name = probe.get("name", "<unnamed probe>")
    detail = probe.get("detail") or "no detail recorded"
    fix = probe.get("host_fix")
    message = f"fault-tool probe {name} failed: {detail}"
    if fix:
        message += f"; a host change would fix it: {fix}"
    if required:
        raise AssertionError(message)
    raise unittest.SkipTest(message)


def tools_unavailable(reason):
    """A missing tool, image or daemon: fatal when required, else a skip."""
    if fault_tools_required() or os.environ.get("SERIES_REQUIRE_DOCKER") == "1":
        raise AssertionError(reason)
    raise unittest.SkipTest(reason)


# Which probes each fault class depends on. A class is available only when
# every one of its probes passed; the S3 route and the capability split
# underlie every fault the rig injects.
BASE_PROBES = ("capabilities", "S3 route general", "S3 route values",
               "route isolation", "restored state")
FAULT_CLASS_PROBES = {
    "store_outage": BASE_PROBES + ("store_outage activation",),
    "slow": BASE_PROBES + ("slow activation",),
    "http503": BASE_PROBES + ("http503 activation",),
    # No direct probe with a negative control exists for these two yet, so
    # they stay unavailable until the task that owns them adds one.
    "disconnect_reset": BASE_PROBES + ("disconnect_reset direct",),
    "dropped_completion_response": BASE_PROBES + ("dropped_completion_response direct",),
    "dns_nxdomain_timeout": BASE_PROBES + ("UDP DNS", "TCP DNS"),
    "tcp_ack_loss": BASE_PROBES + ("xt_bpf", "capture"),
    "containerized_engine": BASE_PROBES + ("engine launch",),
}

# Classes whose direct probe a later task owns, and what it must show.
DEFERRED_PROBES = {
    "disconnect_reset direct": (
        "Task 11: a client-observed connection reset and refused connection "
        "through the route while a bypass request succeeds (negative control)"
    ),
    "dropped_completion_response direct": (
        "Task 11 (reused by Task 13): a values PUT whose completion response is "
        "withheld while the bypass HEAD/GET shows the complete object, against "
        "an untoxicated control PUT"
    ),
}


def _consequence(failed, missing, deferred, *, required) -> str:
    """What a lane does with one class, from why it is or is not available."""
    if not failed and not missing:
        return "runs"
    if deferred and not failed and sorted(missing) == sorted(deferred):
        return "not probed yet: " + "; ".join(
            DEFERRED_PROBES[name] for name in sorted(set(deferred))
        )
    return "fails the lane" if required else "skipped before traffic"


def coverage(probes, *, required: bool, stores=None) -> dict:
    """Each fault class's availability, per store and over all of them.

    A class is available for a store only when every probe it needs ran
    against that store's own rig and passed; a probe of no store (the image
    check) counts for every store. A store whose rig failed before its
    probes ran therefore has every class unavailable, whatever another
    store showed. The class as a whole is `available` only when it is
    available for every store in `stores` (by default every store a probe
    names); `consequence` says what a lane does with it: a required lane
    fails, an optional one skips the class before any traffic, and a class
    whose only gap is a direct probe a later task owns says so.
    """
    if stores is None:
        stores = sorted({probe.get("store") for probe in probes
                         if probe.get("store") is not None}) or [None]
    classes = {}
    for fault, needed in sorted(FAULT_CLASS_PROBES.items()):
        per_store = {}
        for store in stores:
            own = [probe for probe in probes
                   if probe.get("store") in (store, None)]
            failed = sorted({probe["name"] for probe in own
                             if probe["name"] in needed and probe.get("passed") is not True})
            missing = sorted(set(needed) - {probe["name"] for probe in own})
            deferred = sorted(name for name in missing if name in DEFERRED_PROBES)
            per_store[str(store)] = {
                "status": "available" if not failed and not missing else "unavailable",
                "failed_probes": failed,
                "missing_probes": missing,
                "deferred_probes": deferred,
                "consequence": _consequence(failed, missing, deferred, required=required),
            }
        failed = sorted({f"{store}: {name}" for store, entry in per_store.items()
                         for name in entry["failed_probes"]})
        missing = sorted({name for entry in per_store.values()
                          for name in entry["missing_probes"]})
        deferred = sorted({name for entry in per_store.values()
                           for name in entry["deferred_probes"]})
        available = all(entry["status"] == "available" for entry in per_store.values())
        if available:
            consequence = "runs"
        elif all(entry["status"] == "available" or entry["consequence"].startswith(
                "not probed yet") for entry in per_store.values()):
            consequence = _consequence([], deferred, deferred, required=required)
        else:
            consequence = "fails the lane" if required else "skipped before traffic"
        classes[fault] = {
            "probes": list(needed),
            "status": "available" if available else "unavailable",
            "failed_probes": failed,
            "missing_probes": missing,
            "deferred_probes": deferred,
            "consequence": consequence,
            "stores": per_store,
        }
    return classes


# --------------------------------------------------------------------------
# Commands and their evidence
# --------------------------------------------------------------------------

def _clip(text):
    """At most OUTPUT_LIMIT characters of one output stream, as text."""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    text = text or ""
    if len(text) <= OUTPUT_LIMIT:
        return text
    return text[:OUTPUT_LIMIT] + f"... <{len(text) - OUTPUT_LIMIT} more characters>"


def run_command(argv, *, timeout=60) -> dict:
    """Run one command and return its argv, exit status, output and duration.

    A command that outlives `timeout` is recorded with exit status None and
    the reason, never raised: a probe decides what a timeout means.
    """
    started = time.monotonic()
    try:
        done = subprocess.run(
            [str(part) for part in argv], capture_output=True, timeout=timeout
        )
        status, stdout, stderr = done.returncode, done.stdout, done.stderr
    except subprocess.TimeoutExpired as error:
        status, stdout, stderr = None, error.stdout, (error.stderr or b"") + (
            f"\n<timed out after {timeout} s>".encode()
        )
    except OSError as error:
        status, stdout, stderr = None, b"", str(error).encode()
    return {
        "argv": [str(part) for part in argv],
        "exit_status": status,
        "elapsed_s": round(time.monotonic() - started, 6),
        "stdout": _clip(stdout),
        "stderr": _clip(stderr),
    }


def _docker_json(argv, *, timeout=DOCKER_TIMEOUT_S):
    """One docker command whose output is JSON, decoded, or None."""
    done = run_command(["docker", *argv], timeout=timeout)
    if done["exit_status"] != 0:
        return None
    try:
        return json.loads(done["stdout"])
    except ValueError:
        return None


def image_provenance(image) -> dict:
    """The immutable identity of one local image, or None if it is absent."""
    found = _docker_json(["image", "inspect", image])
    if not found:
        return None
    entry = found[0]
    return {
        "tag": image,
        "id": entry.get("Id"),
        "repo_digests": entry.get("RepoDigests") or [],
        "created": entry.get("Created"),
        "labels": (entry.get("Config") or {}).get("Labels") or {},
    }


def require_fault_images() -> dict:
    """The provenance of both fault images, once Docker and both are present.

    Nothing is pulled or built here; an absent image is a preflight skip,
    or a failure where the fault tools are required.
    """
    probe = run_command(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=15)
    if probe["exit_status"] != 0:
        tools_unavailable(f"Docker daemon unavailable: {probe['stderr'].strip()}")
    images = {}
    for role, image in (
        ("fault_tools", os.environ.get("SERIES_FAULT_TOOLS_IMAGE", FAULT_TOOLS_IMAGE)),
        ("toxiproxy", os.environ.get("SERIES_TOXIPROXY_IMAGE", TOXIPROXY_IMAGE)),
    ):
        provenance = image_provenance(image)
        if provenance is None:
            tools_unavailable(
                f"the {role} image {image} is absent; provision it first (README, "
                f"Fault tools)"
            )
        images[role] = provenance
    images["docker_server_version"] = probe["stdout"].strip()
    return images


def package_versions(image) -> dict:
    """The installed version of each recorded package in the tools image."""
    done = run_command(
        ["docker", "run", "--rm", "--pull=never", "--network", "none",
         "--label", f"{RUN_LABEL}=provenance", image,
         "dpkg-query", "-W", "-f", "${Package} ${Version}\\n", *TOOL_PACKAGES],
        timeout=DOCKER_TIMEOUT_S,
    )
    versions = {}
    for line in done["stdout"].splitlines():
        parts = line.split()
        if len(parts) == 2:
            versions[parts[0]] = parts[1]
    return {"versions": versions, "command": done}


def capability_names(mask: int) -> list:
    """The capability names set in one CapEff bit mask."""
    return [
        CAPABILITY_NAMES[bit] if bit < len(CAPABILITY_NAMES) else f"CAP_{bit}"
        for bit in range(mask.bit_length())
        if mask >> bit & 1
    ]


def effective_capabilities(pid) -> dict:
    """The effective capability set of one host process, from its status."""
    try:
        text = Path(f"/proc/{int(pid)}/status").read_text()
    except (OSError, TypeError, ValueError) as error:
        return {"pid": pid, "error": str(error)}
    for line in text.splitlines():
        if line.startswith("CapEff:"):
            mask = int(line.split(":", 1)[1].strip(), 16)
            names = capability_names(mask)
            return {
                "pid": int(pid),
                "cap_eff": f"{mask:016x}",
                "names": names,
                "net_admin": NET_ADMIN in names,
            }
    return {"pid": pid, "error": "no CapEff line"}


def loaded_modules(names=("xt_bpf", "nft_compat")) -> dict:
    """Whether each named kernel module is loaded, from /proc/modules."""
    try:
        loaded = {line.split()[0] for line in Path("/proc/modules").read_text().splitlines()}
    except OSError as error:
        return {"error": str(error)}
    return {name: name in loaded for name in names}


# --------------------------------------------------------------------------
# The ACK-only BPF program
# --------------------------------------------------------------------------

_IPV4 = re.compile(r"^(25[0-5]|2[0-4]\d|1?\d?\d)(\.(25[0-5]|2[0-4]\d|1?\d?\d)){3}$")


def ack_only_expression(store_ip, store_port) -> str:
    """The pcap expression for the store's pure ACKs, argv-safe.

    The address must be a dotted IPv4 address and the port an integer, so
    nothing but those two values can reach the compiler.
    """
    if not isinstance(store_ip, str) or not _IPV4.match(store_ip):
        raise ValueError(f"the store address must be dotted IPv4: {store_ip!r}")
    port = int(store_port)
    if not 0 < port < 65536:
        raise ValueError(f"the store port is out of range: {store_port!r}")
    return ACK_ONLY_FILTER.format(store_ip=store_ip, store_port=port)


def bytecode_from_ddd(text) -> str:
    """Convert `tcpdump -ddd` output to the `iptables -m bpf --bytecode` form.

    `-ddd` prints the instruction count on the first line and then one
    `code jt jf k` line per instruction; iptables takes the count and the
    instructions joined by commas. Anything else is refused rather than
    passed to the kernel.
    """
    lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
    if not lines or not lines[0].isdigit():
        raise AssertionError(f"tcpdump -ddd printed no instruction count: {text!r}")
    count = int(lines[0])
    instructions = lines[1:]
    if count != len(instructions) or count == 0:
        raise AssertionError(
            f"tcpdump -ddd announced {count} instructions and printed "
            f"{len(instructions)}"
        )
    for line in instructions:
        fields = line.split()
        if len(fields) != 4 or not all(field.isdigit() for field in fields):
            raise AssertionError(f"not a numeric BPF instruction: {line!r}")
    return ",".join([str(count)] + [" ".join(line.split()) for line in instructions])


def ack_drop_bytecode(store_ip: str, store_port: int, *, tcpdump=("tcpdump",)) -> str:
    """Compile the store's ACK-only filter to iptables BPF bytecode.

    The address and port are substituted as argv values and the program is
    compiled for raw IPv4 (`-y RAW`), the view `xt_bpf` gives a rule in the
    INPUT chain. `tcpdump` is the command prefix to compile with; a rig
    passes its owner's `docker exec`, so the compiler is the recorded one.
    """
    argv = [*tcpdump, "-ddd", "-y", "RAW", ack_only_expression(store_ip, store_port)]
    done = run_command(argv, timeout=30)
    if done["exit_status"] != 0:
        raise AssertionError(
            f"tcpdump could not compile the ACK-only filter: {done['stderr'].strip()}"
        )
    return bytecode_from_ddd(done["stdout"])


# --------------------------------------------------------------------------
# Container command lines
# --------------------------------------------------------------------------

def _cpuset(cores):
    """The `--cpuset-cpus` argument for a set of cores, or nothing."""
    if not cores:
        return []
    return ["--cpuset-cpus", ",".join(str(core) for core in sorted(cores))]


def owner_argv(*, name, cidfile, network, image, run_id, control_dir, artifact_dir,
               engine_ports, cores=()) -> list:
    """`docker run` for the namespace owner: the only container with NET_ADMIN.

    It runs NGINX with the checked-in configuration and publishes, on host
    loopback only, NGINX and the Toxiproxy API at Docker-chosen ports and
    the engine's gRPC and admin ports at the same number on both sides.
    """
    argv = [
        "docker", "run", "--pull=never", "--detach", "--name", name,
        "--cidfile", str(cidfile),
        "--label", f"{RUN_LABEL}={run_id}",
        "--network", network,
        "--cap-add", NET_ADMIN,
        "--volume", f"{NGINX_CONF}:/etc/nginx/nginx.conf:ro",
        "--volume", f"{control_dir}:{CONTROL_MOUNT}:ro",
        "--volume", f"{artifact_dir}:{ARTIFACT_MOUNT}:rw",
        "--publish", f"127.0.0.1::{NGINX_PORT}",
        "--publish", f"127.0.0.1::{TOXIPROXY_API_PORT}",
    ]
    for port in engine_ports:
        argv += ["--publish", f"127.0.0.1:{int(port)}:{int(port)}"]
    return argv + _cpuset(cores) + [image]


def toxiproxy_argv(*, name, cidfile, owner, image, run_id, cores=()) -> list:
    """`docker run` for Toxiproxy inside the owner's namespace, no added capability."""
    return [
        "docker", "run", "--pull=never", "--detach", "--name", name,
        "--cidfile", str(cidfile),
        "--label", f"{RUN_LABEL}={run_id}",
        "--network", f"container:{owner}",
    ] + _cpuset(cores) + [image, "-host=0.0.0.0", f"-port={TOXIPROXY_API_PORT}"]


# The environment an engine container inherits from the harness: logging,
# backtraces and the allocator's statistics setting, never the harness's
# whole environment.
ENGINE_ENVIRONMENT = ("RUST_LOG", "RUST_BACKTRACE", "MALLOC_CONF")


def engine_argv(*, name, cidfile, owner, image, run_id, argv, mounts, user,
                env=None, cores=()) -> list:
    """`docker run` for the engine inside the owner's namespace.

    Attached, so the CLI's output is the engine log and its exit is the
    engine's. The binary and the repository are mounted read-only, the run
    and buffer directories read-write, each at its own host path, so the
    engine's command line and configuration stay exactly as for a local
    launch. It gets no added capability.
    """
    command = [
        "docker", "run", "--pull=never", "--name", name, "--cidfile", str(cidfile),
        "--label", f"{RUN_LABEL}={run_id}",
        "--network", f"container:{owner}",
        "--user", user,
    ]
    for path, mode in mounts:
        command += ["--volume", f"{path}:{path}:{mode}"]
    for key in ENGINE_ENVIRONMENT:
        if env and key in env:
            command += ["--env", f"{key}={env[key]}"]
    return command + _cpuset(cores) + [image] + [str(part) for part in argv]


def _signal_name(sig) -> str:
    """The name docker kill takes for a signal number."""
    return signal.Signals(sig).name


class ContainerProcess:
    """The engine as a container, with the process interface Engine uses.

    `poll`, `wait` and `returncode` come from the attached `docker run`
    CLI, which exits with the engine's own status; signals go to the
    container itself through `docker kill`, because signalling the CLI
    would not reach the engine.
    """

    def __init__(self, cli, cidfile, name):
        self.cli = cli
        self.cidfile = Path(cidfile)
        self.name = name

    @property
    def container_id(self):
        """The container id Docker wrote, once it has."""
        try:
            return self.cidfile.read_text().strip() or None
        except OSError:
            return None

    @property
    def returncode(self):
        """The engine's exit status, once it has exited."""
        return self.cli.returncode

    def poll(self):
        """None while the engine runs, its exit status afterwards."""
        return self.cli.poll()

    def wait(self, timeout=None):
        """Wait for the engine to exit; raises TimeoutExpired like Popen."""
        return self.cli.wait(timeout)

    def send_signal(self, sig):
        """Deliver one signal to the engine in its container, by its id."""
        ident = self.container_id
        if ident is None:
            raise AssertionError(f"the engine container {self.name} has no id to signal")
        _ = run_command(
            ["docker", "kill", "--signal", _signal_name(sig), ident],
            timeout=DOCKER_TIMEOUT_S,
        )

    def terminate(self):
        """SIGTERM to the engine."""
        self.send_signal(signal.SIGTERM)

    def kill(self):
        """SIGKILL to the engine; the CLI is killed too if it does not follow."""
        self.send_signal(signal.SIGKILL)
        try:
            _ = self.cli.wait(5)
        except subprocess.TimeoutExpired:
            self.cli.kill()


class ContainerLauncher:
    """Run the engine in the rig's namespace, as Engine's launcher.

    It owns the engine's ports, which the owner published before any engine
    existed, and makes the engine bind every address inside the namespace.
    One engine runs at a time; a restart is a new Engine on the same
    launcher and reuses the ports. Before the first launch of a binary its
    shared libraries are checked with `ldd` inside the tools image, and an
    incompatible binary is a setup failure with the diagnostic output.
    """

    kind = "container"
    bind_host = "0.0.0.0"

    def __init__(self, rig):
        self.rig = rig
        self.launches = []
        self.ldd = {}
        self.extra_mounts = []

    def reserve_ports(self):
        """The (gRPC, admin) ports the owner publishes on host loopback."""
        return self.rig.engine_ports

    def mount(self, path):
        """Also mount `path` read-write, for a directory outside the run root."""
        self.extra_mounts.append(Path(path).resolve())

    def check_binary(self, binary):
        """Fail unless `binary` is a release engine whose libraries resolve.

        The profile is read by the harness's own rule (the target directory
        the binary was built into) and anything but release is refused, as
        every measured case refuses it; the binary's hash is recorded. Then
        `ldd` runs inside the tools image, by its inspected id.
        """
        requested = Path(binary)
        binary = requested.resolve()
        if binary in self.ldd:
            return self.ldd[binary]
        profile = measurement.build_profile(requested)
        if profile != RELEASE_PROFILE:
            raise AssertionError(
                f"the fault rig runs a release df_engine only; {requested} is a "
                f"{profile} build. Point DF_ENGINE at target/release/df_engine"
            )
        ordinal = len(self.ldd) + 1
        name = f"series-fault-{self.rig.run_id}-ldd-{ordinal}"
        cidfile = self.rig.state_dir / f"ldd-{ordinal}.cid"
        try:
            done = run_command(
                ["docker", "run", "--pull=never", "--name", name, "--cidfile", str(cidfile),
                 "--label", f"{RUN_LABEL}={self.rig.run_id}", "--network", "none",
                 "--volume", f"{binary}:{binary}:ro", self.rig.image("fault_tools"),
                 "ldd", str(binary)],
                timeout=DOCKER_TIMEOUT_S,
            )
        finally:
            self.rig.adopt_container("ldd", name, cidfile)
            self.rig.remove_container(name)
        missing = [line.strip() for line in done["stdout"].splitlines() if "not found" in line]
        report = {"binary": str(binary), "profile": profile,
                  "binary_sha256": measurement.file_digest(binary),
                  "binary_size_bytes": binary.stat().st_size,
                  "command": done, "missing": missing,
                  "compatible": done["exit_status"] == 0 and not missing}
        self.ldd[binary] = report
        if not report["compatible"]:
            raise AssertionError(
                f"the engine binary {binary} cannot run in the fault-tools image: "
                f"missing {missing}, ldd exit {done['exit_status']}:\n"
                f"{done['stdout']}{done['stderr']}"
            )
        return report

    def _mounts(self, argv):
        """Every host path the engine needs, each with its mode."""
        binary = Path(argv[0]).resolve()
        config = Path(argv[argv.index("--config") + 1]).resolve()
        writable = [config.parent, self.rig.root.resolve(), *self.extra_mounts]
        try:
            document = yaml.safe_load(config.read_text()) or {}
            for group in (document.get("groups") or {}).values():
                for pipeline in (group.get("pipelines") or {}).values():
                    for node in (pipeline.get("nodes") or {}).values():
                        settings = node.get("config") or {}
                        if node.get("type") == "processor:durable_buffer" and settings.get("path"):
                            writable.append(Path(settings["path"]).resolve())
                        file_store = (settings.get("storage") or {}).get("file") or {}
                        if file_store.get("base_uri"):
                            writable.append(Path(file_store["base_uri"]).resolve())
        except (OSError, ValueError, AttributeError):
            pass
        mounts = [(binary, "ro"), (test_e2e.WORKSPACE.resolve(), "ro")]
        seen = set()
        for path in writable:
            if path in seen:
                continue
            seen.add(path)
            path.mkdir(parents=True, exist_ok=True)
            mounts.append((path, "rw"))
        return mounts

    def start(self, argv, log, env):
        """Start the engine container attached, with its output in `log`."""
        self.check_binary(argv[0])
        ordinal = len(self.launches) + 1
        name = f"series-fault-{self.rig.run_id}-engine-{ordinal}"
        cidfile = self.rig.state_dir / f"engine-{ordinal}.cid"
        command = engine_argv(
            name=name, cidfile=cidfile, owner=self.rig.owner_id,
            image=self.rig.image("fault_tools"), run_id=self.rig.run_id,
            argv=argv, mounts=self._mounts(argv),
            user=f"{os.getuid()}:{os.getgid()}", env=env, cores=self.rig.engine_cores,
        )
        cli = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        process = ContainerProcess(cli, cidfile, name)
        self.launches.append({"name": name, "argv": command})
        # Docker writes the id when it creates the container; only then is
        # the container this rig's to remove.
        deadline = time.monotonic() + 30
        while process.container_id is None and cli.poll() is None:
            if time.monotonic() > deadline:
                break
            time.sleep(0.02)
        self.rig.adopt_container("engine", name, cidfile)
        self.launches[-1]["container_id"] = process.container_id
        return process

    def pid(self, process):
        """The engine's host PID, from `docker inspect` once it is running."""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssertionError(
                    f"the engine container {process.name} exited with "
                    f"{process.returncode} before it had a PID"
                )
            ident = process.container_id
            state = ident and _docker_json(["inspect", "--format", "{{json .State}}", ident])
            if state and state.get("Running") and state.get("Pid"):
                pid = int(state["Pid"])
                self.launches[-1]["host_pid"] = pid
                self.launches[-1]["container_id"] = ident
                self.rig.adopt_container("engine", process.name, process.cidfile, pid=pid)
                return pid
            time.sleep(0.05)
        raise AssertionError(f"the engine container {process.name} never started")


# --------------------------------------------------------------------------
# Toxiproxy's HTTP API
# --------------------------------------------------------------------------

class Toxiproxy:
    """A client of one Toxiproxy server's HTTP API, reached on host loopback."""

    def __init__(self, base_url):
        self.base_url = base_url.rstrip("/")

    def request(self, method, path, body=None, *, timeout=10):
        """One API call; returns (status, decoded JSON or text)."""
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            self.base_url + path, data=data, method=method,
            headers={"Content-Type": "application/json"} if data else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, raw = error.code, error.read()
        text = raw.decode("utf-8", errors="replace")
        try:
            return status, json.loads(text) if text.strip() else None
        except ValueError:
            return status, text

    def expect(self, method, path, body=None, *, statuses=(200, 201, 204)):
        """One API call that must succeed; returns its decoded body."""
        status, payload = self.request(method, path, body)
        if status not in statuses:
            raise AssertionError(f"toxiproxy {method} {path} answered {status}: {payload}")
        return payload

    def version(self):
        """The server's own version string."""
        _status, payload = self.request("GET", "/version")
        if isinstance(payload, dict):
            return payload.get("version")
        return payload.strip() if isinstance(payload, str) else payload

    def proxies(self):
        """Every proxy, by name, with its toxics."""
        return self.expect("GET", "/proxies")

    def toxics(self, proxy):
        """The toxics currently on one proxy."""
        return self.expect("GET", f"/proxies/{proxy}/toxics")

    def add_toxic(self, proxy, toxic):
        """Add one toxic to one proxy."""
        return self.expect("POST", f"/proxies/{proxy}/toxics", toxic)

    def remove_toxic(self, proxy, name):
        """Remove one named toxic from one proxy."""
        return self.expect("DELETE", f"/proxies/{proxy}/toxics/{name}")

    def update(self, proxy, fields):
        """Change one proxy's fields, such as `enabled` or `upstream`."""
        return self.expect("POST", f"/proxies/{proxy}", fields)


def slow_toxics(parameters=None) -> list:
    """The two slow-storage toxics, in the plan's exact bodies by default."""
    parameters = parameters or {}
    return [
        {"name": "slow_upload", "type": "bandwidth", "stream": "upstream",
         "toxicity": 1.0,
         "attributes": {"rate": int(parameters.get("rate_kbps", SLOW_RATE_KBPS))}},
        {"name": "slow_response", "type": "latency", "stream": "downstream",
         "toxicity": 1.0,
         "attributes": {"latency": int(parameters.get("latency_ms", SLOW_LATENCY_MS)),
                        "jitter": int(parameters.get("jitter_ms", 0))}},
    ]


# --------------------------------------------------------------------------
# Registered activations
# --------------------------------------------------------------------------

# name -> (activate(rig, parameters) -> state, recover(rig, state) -> evidence).
# Tasks 9-11 register their additional fault names here.
FAULTS = {}


def register_fault(name, activate, recover):
    """Make one fault activatable by name on every rig."""
    if name in FAULTS:
        raise AssertionError(f"fault {name} is already registered")
    FAULTS[name] = (activate, recover)


def _activate_slow(rig, parameters):
    """Add the bandwidth and latency toxics to both proxies."""
    toxics = slow_toxics(parameters)
    for proxy in PROXY_PORTS:
        for toxic in toxics:
            rig.toxiproxy.add_toxic(proxy, toxic)
    state = {proxy: rig.toxiproxy.toxics(proxy) for proxy in PROXY_PORTS}
    for proxy, present in state.items():
        names = sorted(toxic["name"] for toxic in present)
        if names != sorted(toxic["name"] for toxic in toxics):
            raise AssertionError(f"proxy {proxy} carries toxics {names} after activation")
    return {"toxics": toxics, "api_state": state, "units": TOXIC_UNITS}


def _recover_slow(rig, state):
    """Remove exactly the named toxics from both proxies."""
    for proxy in PROXY_PORTS:
        for toxic in state["toxics"]:
            rig.toxiproxy.remove_toxic(proxy, toxic["name"])
    remaining = {proxy: rig.toxiproxy.toxics(proxy) for proxy in PROXY_PORTS}
    if any(remaining.values()):
        raise AssertionError(f"toxics remain after recovery: {remaining}")
    return {"api_state": remaining}


def _activate_http503(rig, parameters):
    """Create the control file NGINX answers 503 for."""
    rig.fail503.write_text("series fault http503\n")
    return {"control_file": str(rig.fail503)}


def _recover_http503(rig, state):
    """Remove that exact control file."""
    Path(state["control_file"]).unlink()
    if rig.fail503.exists():
        raise AssertionError(f"{rig.fail503} still exists after recovery")
    return {"control_file_present": False}


def _activate_store_outage(rig, parameters):
    """Stop the store container without losing what it holds."""
    rig.store.stop()
    return {"store": rig.store.name, "address_before": rig.store_ip}


def _recover_store_outage(rig, state):
    """Start the same store again and repoint both proxies if it moved."""
    rig.store.recover()
    address = rig.store.network_address(rig.network_id)
    if not address:
        raise AssertionError("the recovered store has no address on the rig network")
    moved = address != rig.store_ip
    if moved:
        rig.store_ip = address
        for proxy in PROXY_PORTS:
            rig.toxiproxy.update(proxy, {"upstream": f"{address}:{STORE_PORT}"})
    return {"address_after": address, "proxies_repointed": moved}


register_fault("slow", _activate_slow, _recover_slow)
register_fault("http503", _activate_http503, _recover_http503)
register_fault("store_outage", _activate_store_outage, _recover_store_outage)


# --------------------------------------------------------------------------
# The rig
# --------------------------------------------------------------------------

# The fields of `fault-nginx.conf`'s log format, in order.
ACCESS_LOG_FIELDS = ("msec", "method", "uri", "status", "upstream_status",
                     "request_time", "upstream_response_time", "body_bytes_sent",
                     "request_length", "request_uri")


def parse_access_log(path) -> list:
    """NGINX's fault log, one dict per request, in the configured format."""
    entries = []
    try:
        lines = Path(path).read_text(errors="replace").splitlines()
    except OSError:
        return entries
    for line in lines:
        parts = line.split()
        # Eight fields is the format before the request length and URI were logged.
        if len(parts) in (len(ACCESS_LOG_FIELDS), len(ACCESS_LOG_FIELDS) - 2):
            entries.append(dict(zip(ACCESS_LOG_FIELDS, parts)))
        else:
            entries.append({"raw": line})
    return entries


def backend_of(uri) -> str:
    """Which proxy NGINX's map sends a URI to."""
    return "values" if re.search(r"dataset=values/", uri or "") else "general"


class FaultRig:
    """The disposable fault environment around one DockerStore.

    `storage` is the exporter storage section that reaches the store
    through NGINX, `launcher` runs the engine inside the namespace, and
    `activate`/`recover` drive registered faults. `probes` names the
    probes run on entry (the S3 route and capability split by default); a
    failed one is fatal where fault tools are required and a skip
    otherwise, in both cases after everything created was removed.
    Cleanup removes only the containers and the network this rig recorded
    and detaches the store; the run's artifacts stay under `root`.
    """

    DEFAULT_PROBES = ("capabilities", "S3 route general", "S3 route values",
                      "restored state")

    def __init__(self, store, root, *, probes=DEFAULT_PROBES, cores=None,
                 engine_cores=None):
        self.store = store
        self.root = Path(root)
        self.run_id = uuid.uuid4().hex[:12]
        self.entry_probes = tuple(probes)
        # NGINX, Toxiproxy and the probes run on `cores`; the engine
        # container may use `engine_cores`, inside which the engine pins its
        # own workers. Both default to this harness's own affinity, so a
        # harness started under `taskset` confines its containers too.
        self.cores = sorted(cores if cores is not None else os.sched_getaffinity(0))
        self.engine_cores = sorted(
            engine_cores if engine_cores is not None else os.sched_getaffinity(0)
        )
        self.containers = []
        self.network_id = None
        self.network_name = f"series-fault-{self.run_id}-net"
        self.attached = False
        self.owner_id = None
        self.store_ip = None
        self.active = []
        self.activations = []
        self.probes = []
        self.cleanup_report = None
        self.launcher = ContainerLauncher(self)
        self.images = {}

    # -- lifecycle --------------------------------------------------------

    def image(self, role):
        """The immutable id of one inspected image; containers run by id.

        A tag can be moved between inspection and launch; the id cannot.
        """
        ident = (self.images.get(role) or {}).get("id")
        if not ident:
            raise AssertionError(f"the {role} image was never inspected")
        return ident

    def adopt_container(self, role, name, cidfile, *, pid=None):
        """Record a container once Docker has written its id to `cidfile`.

        Only a container whose id Docker returned to this rig is recorded,
        and cleanup removes it by that id, so a name collision can never make
        the rig remove a container it did not create. Returns the id, or
        None when Docker created nothing.
        """
        try:
            ident = Path(cidfile).read_text().strip() or None
        except OSError:
            ident = None
        if ident is None:
            return None
        for entry in self.containers:
            if entry["id"] == ident:
                if pid is not None:
                    entry["host_pid"] = pid
                return ident
        self.containers.append({"role": role, "name": name, "id": ident, "host_pid": pid,
                                "removed": False})
        return ident

    def remove_container(self, name):
        """Remove the recorded container of this name, by its id."""
        for entry in self.containers:
            if entry["name"] == name and not entry["removed"]:
                return self._remove_entry(entry)
        return None

    @staticmethod
    def _remove_entry(entry):
        """Remove one recorded container by the id Docker returned for it."""
        done = run_command(["docker", "rm", "--force", "--volumes", entry["id"]])
        entry["removed"] = (done["exit_status"] == 0
                            or "No such container" in done["stderr"])
        return done

    def _run_container(self, role, name, build_argv):
        """`docker run --detach` one container and adopt it by its cidfile."""
        cidfile = self.state_dir / f"{name}.cid"
        try:
            done = run_command(build_argv(cidfile))
        finally:
            ident = self.adopt_container(role, name, cidfile)
        if done["exit_status"] != 0:
            return None, done
        return ident, done

    def __enter__(self):
        self.images = require_fault_images()
        try:
            self._start()
            for name in self.entry_probes:
                probe = PROBES[name](self)
                self.probes.append(probe)
                require_probe(probe, required=fault_tools_required())
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def _start(self):
        """Create the network, the owner, the proxy and the route, in order."""
        self.root.mkdir(parents=True, exist_ok=True)
        self.artifact_dir = self.root / "fault-artifacts"
        self.control_dir = self.root / "fault-control"
        self.state_dir = self.root / "fault-state"
        for directory in (self.artifact_dir, self.control_dir, self.state_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self.fail503 = self.control_dir / FAIL503_FILE
        created = run_command(
            ["docker", "network", "create", "--driver", "bridge",
             "--label", f"{RUN_LABEL}={self.run_id}", self.network_name],
        )
        if created["exit_status"] != 0:
            raise AssertionError(f"docker network create failed: {created['stderr']}")
        self.network_id = created["stdout"].strip()
        self.store.attach(self.network_id, STORE_ALIAS)
        self.attached = True
        self.store_ip = self.store.network_address(self.network_id)
        if not self.store_ip:
            raise AssertionError("the store has no address on the rig network")
        # The image the store container really runs, whatever it was
        # started from.
        self.store_running_image = (_docker_json(
            ["inspect", "--format", "{{json .Image}}", self.store.container]) or None)
        self._start_owner()
        name = f"series-fault-{self.run_id}-toxiproxy"
        ident, done = self._run_container("toxiproxy", name, lambda cidfile: toxiproxy_argv(
            name=name, cidfile=cidfile, owner=self.owner_id, image=self.image("toxiproxy"),
            run_id=self.run_id, cores=self.cores,
        ))
        if ident is None:
            raise AssertionError(f"toxiproxy did not start: {done['stderr']}")
        self.toxiproxy_port = self._published(TOXIPROXY_API_PORT)
        self.nginx_port = self._published(NGINX_PORT)
        self.toxiproxy = Toxiproxy(f"http://127.0.0.1:{self.toxiproxy_port}")
        self._wait(lambda: self.toxiproxy.version(), "the toxiproxy API")
        self.toxiproxy_version = self.toxiproxy.version()
        for proxy, port in PROXY_PORTS.items():
            self.toxiproxy.expect("POST", "/proxies", {
                "name": proxy, "listen": f"127.0.0.1:{port}",
                "upstream": f"{self.store_ip}:{STORE_PORT}", "enabled": True,
            })
        self.endpoint = f"http://127.0.0.1:{NGINX_PORT}"
        self.route_endpoint = f"http://127.0.0.1:{self.nginx_port}"
        self._wait(self._route_answers, "the NGINX route to the store")
        for entry in self.containers:
            if entry["host_pid"] is None and not entry["removed"]:
                entry["host_pid"] = container_pid(entry["id"])
        self.baseline_rules = self.iptables_rules()
        self.storage = {
            "s3": {
                "base_uri": f"s3://{self.store.bucket}/otel",
                "region": "us-east-1",
                "endpoint": self.endpoint,
                "allow_http": True,
                "virtual_hosted_style_request": False,
                "auth": {
                    "type": "static_credentials",
                    "access_key_id": self.store.key,
                    "secret_access_key": self.store.secret,
                },
            }
        }

    def _start_owner(self, attempts=3):
        """Start the namespace owner, retrying when a chosen port was taken."""
        for attempt in range(attempts):
            name = f"series-fault-{self.run_id}-tools-{attempt + 1}"
            self.engine_ports = (test_e2e.free_port(), test_e2e.free_port())
            ident, done = self._run_container("owner", name, lambda cidfile: owner_argv(
                name=name, cidfile=cidfile, network=self.network_id,
                image=self.image("fault_tools"), run_id=self.run_id,
                control_dir=self.control_dir.resolve(),
                artifact_dir=self.artifact_dir.resolve(),
                engine_ports=self.engine_ports, cores=self.cores,
            ))
            if ident is not None:
                self.owner_id = ident
                return
            # A container Docker created but could not start is this rig's,
            # by its cidfile, and is removed by that id.
            self.remove_container(name)
            taken = "already allocated" in done["stderr"] or "in use" in done["stderr"]
            if not taken or attempt + 1 == attempts:
                raise AssertionError(f"the fault-tools owner did not start: {done['stderr']}")

    def _published(self, port):
        """The host loopback port the owner publishes `port` on."""
        done = run_command(["docker", "port", self.owner_id, f"{port}/tcp"])
        for line in done["stdout"].splitlines():
            if line.startswith("127.0.0.1:"):
                return int(line.rsplit(":", 1)[1])
        raise AssertionError(f"the owner does not publish {port}: {done}")

    @staticmethod
    def _wait(observe, what, seconds=30):
        """Poll until `observe` returns a truthy value without raising."""
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                if observe():
                    return
            except Exception as error:  # a starting service refuses or resets
                last = error
            time.sleep(0.1)
        raise AssertionError(f"{what} never became ready: {last}")

    def _route_answers(self):
        """Whether NGINX forwards a request and the store answers it."""
        try:
            with urllib.request.urlopen(self.route_endpoint + "/", timeout=5) as response:
                return response.status < 500
        except urllib.error.HTTPError as error:
            return error.code < 500

    # The teardown stages, in order. Each one runs whatever an earlier one
    # raised, an interrupt included, so a failed recovery or a Ctrl-C in
    # the middle can never leave a container, a network or an attachment
    # behind.
    TEARDOWN_STAGES = ("faults", "namespace_rules", "proxies", "control_file",
                       "artifacts", "containers", "store_attachment", "network",
                       "leftovers")

    def __exit__(self, *exc):
        """Recover what is still active, then remove what this rig created.

        Every stage in `TEARDOWN_STAGES` runs, each catching everything it
        raises into the report. An interrupt (KeyboardInterrupt, SystemExit)
        caught in a stage is raised again once every stage has run.
        """
        report = {"stages": {}, "errors": [], "removed": []}
        interrupted = None
        for stage in self.TEARDOWN_STAGES:
            try:
                outcome = getattr(self, f"_teardown_{stage}")(report)
                report["stages"][stage] = {"ok": True, "detail": outcome}
            except BaseException as error:  # every stage runs, whatever this was
                report["stages"][stage] = {"ok": False,
                                           "detail": f"{type(error).__name__}: {error}"}
                report["errors"].append(f"{stage}: {type(error).__name__}: {error}")
                if not isinstance(error, Exception) and interrupted is None:
                    interrupted = error
        leftover = report.get("leftovers") or {}
        report["clean"] = (not report["errors"] and not leftover.get("containers")
                           and not leftover.get("networks"))
        self.cleanup_report = report
        if interrupted is not None:
            raise interrupted
        return False

    def _teardown_faults(self, report):
        """Retry the recovery of every fault still active."""
        if not self.active:
            return "no active fault"
        self.recover()
        return "recovered"

    def _teardown_namespace_rules(self, report):
        """Put the owner's filter table back to its baseline."""
        if not self.owner_id or getattr(self, "baseline_rules", None) is None:
            return "no namespace"
        if self.iptables_rules() == self.baseline_rules:
            return "baseline"
        if any(line.startswith("-A") for line in self.baseline_rules.splitlines()):
            raise AssertionError("probe rules remain and the baseline is not empty")
        flushed = self.exec(["iptables", "-w", "-F"])
        if flushed["exit_status"] != 0 or self.iptables_rules() != self.baseline_rules:
            raise AssertionError(f"the filter table could not be restored: {flushed}")
        return "flushed back to the empty baseline"

    def _teardown_proxies(self, report):
        """Remove every toxic and re-enable both proxies."""
        if getattr(self, "toxiproxy", None) is None:
            return "no proxy"
        for proxy in PROXY_PORTS:
            for toxic in self.toxiproxy.toxics(proxy) or []:
                self.toxiproxy.remove_toxic(proxy, toxic["name"])
            self.toxiproxy.update(proxy, {"enabled": True})
        return "no toxics, both enabled"

    def _teardown_control_file(self, report):
        """Remove the 503 control file if it is still there."""
        fail503 = getattr(self, "fail503", None)
        if fail503 is not None and fail503.exists():
            fail503.unlink()
            return "removed"
        return "absent"

    def _teardown_artifacts(self, report):
        """Make the root-written artifacts readable before the owner goes away."""
        if not self.owner_id:
            return "no owner"
        done = self.exec(["chmod", "-R", "a+rX", ARTIFACT_MOUNT])
        if done["exit_status"] != 0:
            raise AssertionError(f"chmod failed: {done['stderr']}")
        return "readable"

    def _teardown_containers(self, report):
        """Remove every recorded container, newest first, by its id.

        Each removal is attempted even if an earlier one raised; an
        interrupt is raised again after the last one.
        """
        failures = []
        interrupted = None
        for entry in reversed(self.containers):
            if entry["removed"]:
                continue
            try:
                done = self._remove_entry(entry)
            except BaseException as error:  # the next container is still removed
                failures.append(f"{entry['name']}: {type(error).__name__}: {error}")
                if not isinstance(error, Exception) and interrupted is None:
                    interrupted = error
                continue
            report["removed"].append({"name": entry["name"], "id": entry["id"],
                                      "exit_status": done and done["exit_status"]})
            if not entry["removed"]:
                failures.append(f"{entry['name']}: {done and done['stderr'].strip()}")
        if interrupted is not None:
            raise interrupted
        if failures:
            raise AssertionError(f"containers not removed: {failures}")
        return f"{len(report['removed'])} removed"

    def _teardown_store_attachment(self, report):
        """Detach the store from the rig network."""
        if not self.attached:
            return "not attached"
        self.store.detach(self.network_id)
        self.attached = False
        return "detached"

    def _teardown_network(self, report):
        """Remove the rig network, by the id Docker returned."""
        if not self.network_id:
            return "no network"
        done = run_command(["docker", "network", "rm", self.network_id])
        if done["exit_status"] != 0 and "not found" not in done["stderr"]:
            raise AssertionError(f"network rm: {done['stderr'].strip()}")
        return "removed"

    def _teardown_leftovers(self, report):
        """Prove by label that nothing of this run is left."""
        report["leftovers"] = leftovers(self.run_id)
        if not report["leftovers"]["commands_ok"]:
            raise AssertionError("the leftover check could not list containers or networks")
        return "checked"

    # -- control ----------------------------------------------------------

    def exec(self, argv, *, timeout=60) -> dict:
        """Run one command in the owner's namespace and record it."""
        return run_command(["docker", "exec", self.owner_id, *argv], timeout=timeout)

    def iptables_rules(self) -> str:
        """The owner namespace's filter table, as `iptables -S` prints it."""
        return self.exec(["iptables", "-w", "-S"])["stdout"]

    def route_client(self, *, read_timeout=30):
        """A signed S3 client that reaches the store through NGINX."""
        return boto3.client(
            "s3",
            endpoint_url=self.route_endpoint,
            region_name="us-east-1",
            aws_access_key_id=self.store.key,
            aws_secret_access_key=self.store.secret,
            config=BotoConfig(
                connect_timeout=5, read_timeout=read_timeout,
                retries={"max_attempts": 0}, s3={"addressing_style": "path"},
            ),
        )

    def activate(self, name: str, parameters: dict) -> None:
        """Activate one registered fault; any error here is a failure.

        Activation happens after a successful preflight, so it can never be
        turned into a skip.
        """
        if name not in FAULTS:
            raise AssertionError(f"no fault named {name} is registered: {sorted(FAULTS)}")
        if any(entry["name"] == name for entry in self.active):
            raise AssertionError(f"fault {name} is already active")
        activate, _recover = FAULTS[name]
        entry = {"name": name, "parameters": dict(parameters or {}),
                 "activated_utc": measurement.utc_now(),
                 "activated_monotonic_ns": time.monotonic_ns()}
        try:
            entry["state"] = activate(self, entry["parameters"])
        except unittest.SkipTest as error:
            raise AssertionError(f"activating {name} after preflight failed: {error}")
        except Exception as error:
            raise AssertionError(f"activating {name} after preflight failed: {error}") from error
        self.active.append(entry)
        self.activations.append(entry)

    def recover(self) -> None:
        """Recover every active fault, newest first; any error is a failure.

        A fault leaves `active` only once its recovery succeeded, so a
        failed recovery stays active and a later `recover` -- the rig's own
        teardown included -- retries it. Every attempt is recorded.
        """
        errors = []
        for entry in list(reversed(self.active)):
            _activate, recover = FAULTS[entry["name"]]
            attempt = {"started_utc": measurement.utc_now()}
            entry.setdefault("recovery_attempts", []).append(attempt)
            try:
                entry["recovery"] = recover(self, entry["state"])
            except Exception as error:
                attempt["error"] = f"{type(error).__name__}: {error}"
                errors.append(f"{entry['name']}: {error}")
                continue
            entry["recovered_utc"] = measurement.utc_now()
            entry["recovered_monotonic_ns"] = time.monotonic_ns()
            self.active.remove(entry)
        if errors:
            raise AssertionError(f"fault recovery failed: {errors}")

    def residual_state(self) -> dict:
        """Everything a probe or fault could leave behind, and whether it did."""
        proxies = self.toxiproxy.proxies()
        rules = self.iptables_rules()
        state = {
            "iptables_filter": rules,
            "iptables_matches_baseline": rules == self.baseline_rules,
            "proxies": {
                name: {"enabled": entry.get("enabled"), "upstream": entry.get("upstream"),
                       "toxics": entry.get("toxics") or []}
                for name, entry in sorted((proxies or {}).items())
            },
            "fail503_present": self.fail503.exists(),
            "store_address": self.store_ip,
        }
        state["clean"] = (
            state["iptables_matches_baseline"]
            and not state["fail503_present"]
            and sorted(state["proxies"]) == sorted(PROXY_PORTS)
            and all(
                entry["enabled"] is True and not entry["toxics"]
                and entry["upstream"] == f"{self.store_ip}:{STORE_PORT}"
                for entry in state["proxies"].values()
            )
            and not self.active
        )
        return state

    def assert_clean(self):
        """Fail unless no rule, toxic, control file or fault is left over."""
        state = self.residual_state()
        if not state["clean"]:
            raise AssertionError(f"the fault rig is not clean: {state}")
        return state

    def completed_values(self) -> list:
        """The values objects NGINX saw uploaded that the store really holds.

        Read from the store directly (a HEAD bypassing NGINX and Toxiproxy),
        so an object whose completion response was dropped on the way back
        still counts as complete; each entry keeps the statuses NGINX logged
        for that key.
        """
        seen = {}
        for entry in parse_access_log(self.artifact_dir / ACCESS_LOG):
            uri = entry.get("uri", "")
            if entry.get("method") not in ("PUT", "POST") or backend_of(uri) != "values":
                continue
            prefix = f"/{self.store.bucket}/"
            if not uri.startswith(prefix):
                continue
            key = uri[len(prefix):]
            seen.setdefault(key, []).append(
                {field: entry.get(field) for field in ("msec", "method", "status",
                                                       "upstream_status")}
            )
        completed = []
        for key, requests in sorted(seen.items()):
            try:
                head = self.store.client.head_object(Bucket=self.store.bucket, Key=key)
            except ClientError:
                continue
            completed.append({
                "key": key, "size_bytes": head.get("ContentLength"),
                "etag": head.get("ETag"), "requests": requests,
            })
        return completed

    def evidence(self) -> dict:
        """The rig's identity, containers, route, faults and cleanup, as JSON."""
        return {
            "run_id": self.run_id,
            "network": {"name": self.network_name, "id": self.network_id},
            "store": {"kind": self.store.kind, "container": self.store.name,
                      "address": self.store_ip, "port": STORE_PORT, "alias": STORE_ALIAS,
                      "image": getattr(self.store, "image", None),
                      "image_id": getattr(self.store, "image_id", None),
                      "running_image_id": getattr(self, "store_running_image", None)},
            "images": self.images,
            "toxiproxy_version": getattr(self, "toxiproxy_version", None),
            "toxic_units": TOXIC_UNITS,
            "containers": [
                dict(entry, capabilities=effective_capabilities(entry["host_pid"])
                     if entry.get("host_pid") else None)
                for entry in self.containers
            ],
            "cores": self.cores,
            "route": {
                "engine_endpoint": getattr(self, "endpoint", None),
                "nginx_port": NGINX_PORT, "proxy_ports": PROXY_PORTS,
                "host_nginx_port": getattr(self, "nginx_port", None),
                "host_toxiproxy_port": getattr(self, "toxiproxy_port", None),
                "engine_ports": list(getattr(self, "engine_ports", ())),
            },
            "launches": self.launcher.launches,
            "ldd": [dict(report, binary=str(binary)) for binary, report in self.launcher.ldd.items()],
            "activations": self.activations,
            "access_log_requests_count": len(parse_access_log(self.artifact_dir / ACCESS_LOG))
            if hasattr(self, "artifact_dir") else 0,
            "cleanup": self.cleanup_report,
        }


def container_pid(name):
    """A container's host PID, or None when it is not running."""
    state = _docker_json(["inspect", "--format", "{{json .State}}", name])
    if state and state.get("Pid"):
        return int(state["Pid"])
    return None


def leftovers(run_id) -> dict:
    """Containers and networks still labelled with one rig's run id."""
    label = f"label={RUN_LABEL}={run_id}"
    containers = run_command(["docker", "ps", "--all", "--quiet", "--filter", label])
    networks = run_command(["docker", "network", "ls", "--quiet", "--filter", label])
    return {
        "containers": containers["stdout"].split(),
        "networks": networks["stdout"].split(),
        "commands_ok": containers["exit_status"] == 0 and networks["exit_status"] == 0,
    }


# --------------------------------------------------------------------------
# Probes
# --------------------------------------------------------------------------

def new_probe(name, rig=None) -> dict:
    """An empty probe record: failed until a probe proves otherwise."""
    return {
        "name": name,
        "store": rig.store.kind if rig is not None else None,
        "passed": False,
        "detail": "",
        "commands": [],
        "operations": [],
        "evidence": {},
        "restored": {},
        "started_utc": measurement.utc_now(),
        "elapsed_s": 0.0,
    }


def finish(probe, started, passed, detail) -> dict:
    """Close one probe record with its verdict."""
    probe["passed"] = bool(passed)
    probe["detail"] = detail
    probe["elapsed_s"] = round(time.monotonic() - started, 6)
    return probe


def _record(probe, command):
    """Append one command's evidence to a probe and return the command."""
    probe["commands"].append(command)
    return command


def _s3_op(probe, label, call, **fields):
    """One S3 call, timed and recorded; returns (ok, result or error)."""
    started = time.monotonic()
    entry = {"op": label, **fields}
    try:
        value = call()
        entry.update(ok=True, status=_status_of(value))
        outcome = (True, value)
    except ClientError as error:
        entry.update(ok=False, status=error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"), error=error.response.get("Error", {}).get("Code"))
        outcome = (False, error)
    except (BotoCoreError, OSError) as error:
        entry.update(ok=False, status=None, error=f"{type(error).__name__}: {error}")
        outcome = (False, error)
    entry["elapsed_s"] = round(time.monotonic() - started, 6)
    probe["operations"].append(entry)
    return outcome


def _status_of(value):
    """The HTTP status of one boto3 response, when it carries one."""
    if isinstance(value, dict):
        return value.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return None


MULTIPART_PART_BYTES = 5 * 1024 * 1024


def _payload(size, seed):
    """Deterministic bytes of one size, different for every seed."""
    block = hashlib.sha256(seed.encode()).digest()
    return (block * (size // len(block) + 1))[:size]


def probe_route(rig, backend) -> dict:
    """Signed PUT, HEAD, GET, DELETE and a multipart upload through one backend.

    The key names the backend NGINX's map selects, and the access log must
    show every request with the status the client saw, so the route, the
    signing Host and the body framing are all proven on real requests.
    """
    probe = new_probe(f"S3 route {backend}", rig)
    started = time.monotonic()
    client = rig.route_client()
    dataset = "values" if backend == "values" else "series"
    base = f"fault-preflight/{rig.run_id}/dataset={dataset}/probe"
    bucket = rig.store.bucket
    small = _payload(64 * 1024, base)
    problems = []
    ok, _ = _s3_op(probe, "put_object", lambda: client.put_object(
        Bucket=bucket, Key=f"{base}-small", Body=small), key=f"{base}-small")
    ok &= _s3_op(probe, "head_object", lambda: client.head_object(
        Bucket=bucket, Key=f"{base}-small"))[0]
    got, response = _s3_op(probe, "get_object", lambda: client.get_object(
        Bucket=bucket, Key=f"{base}-small"))
    if got and response["Body"].read() != small:
        problems.append("GET returned different bytes than PUT stored")
    ok &= got
    parts = [_payload(MULTIPART_PART_BYTES, base + "-1"), _payload(4096, base + "-2")]
    key = f"{base}-multipart"
    made, upload = _s3_op(probe, "create_multipart_upload", lambda: client.create_multipart_upload(
        Bucket=bucket, Key=key), key=key)
    etags = []
    if made:
        for number, body in enumerate(parts, 1):
            done, part = _s3_op(probe, f"upload_part_{number}", lambda: client.upload_part(
                Bucket=bucket, Key=key, UploadId=upload["UploadId"], PartNumber=number,
                Body=body), bytes=len(body))
            made &= done
            if done:
                etags.append({"ETag": part["ETag"], "PartNumber": number})
    if made:
        made &= _s3_op(probe, "complete_multipart_upload", lambda: client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload["UploadId"],
            MultipartUpload={"Parts": etags}))[0]
        got, response = _s3_op(probe, "get_multipart", lambda: client.get_object(
            Bucket=bucket, Key=key))
        if got and response["Body"].read() != b"".join(parts):
            problems.append("the completed multipart object differs from its parts")
        made &= got
    ok &= made
    for name in (f"{base}-small", key):
        ok &= _s3_op(probe, "delete_object", lambda: client.delete_object(
            Bucket=bucket, Key=name), key=name)[0]
    gone, _ = _s3_op(probe, "head_deleted", lambda: client.head_object(
        Bucket=bucket, Key=f"{base}-small"))
    if gone:
        problems.append("a deleted object still answers HEAD")
    # The access log is written when each response completes; allow NGINX
    # a moment to flush the last line.
    time.sleep(0.2)
    logged = [entry for entry in parse_access_log(rig.artifact_dir / ACCESS_LOG)
              if base in entry.get("uri", "")]
    methods = sorted({entry["method"] for entry in logged})
    probe["evidence"] = {
        "key_prefix": base, "backend": backend,
        "access_log": logged,
        "logged_backends": sorted({backend_of(entry["uri"]) for entry in logged}),
        "multipart_part_bytes": [len(part) for part in parts],
    }
    if methods != ["DELETE", "GET", "HEAD", "POST", "PUT"]:
        problems.append(f"the access log shows methods {methods}")
    if probe["evidence"]["logged_backends"] != [backend]:
        problems.append(f"NGINX mapped the keys to {probe['evidence']['logged_backends']}")
    failed_ops = [entry["op"] for entry in probe["operations"]
                  if not entry["ok"] and entry["op"] != "head_deleted"]
    if failed_ops:
        problems.append(f"failed operations {failed_ops}")
    return finish(probe, started, ok and not problems,
                  "; ".join(problems) or f"signed S3 round trips through the {backend} proxy")


def probe_route_isolation(rig) -> dict:
    """Disabling one proxy breaks exactly the keys NGINX maps to it."""
    probe = new_probe("route isolation", rig)
    started = time.monotonic()
    client = rig.route_client(read_timeout=10)
    bucket = rig.store.bucket
    keys = {
        "general": f"fault-preflight/{rig.run_id}/dataset=series/isolation",
        "values": f"fault-preflight/{rig.run_id}/dataset=values/isolation",
    }
    problems = []
    for disabled in PROXY_PORTS:
        rig.toxiproxy.update(disabled, {"enabled": False})
        try:
            for backend, key in keys.items():
                ok, _ = _s3_op(probe, f"put_with_{disabled}_disabled", lambda: client.put_object(
                    Bucket=bucket, Key=key, Body=b"isolation"), key=key, backend=backend)
                if ok == (backend == disabled):
                    problems.append(
                        f"with {disabled} disabled the {backend} key "
                        f"{'succeeded' if ok else 'failed'}"
                    )
        finally:
            rig.toxiproxy.update(disabled, {"enabled": True})
    for key in keys.values():
        _s3_op(probe, "delete_object", lambda: client.delete_object(Bucket=bucket, Key=key), key=key)
    probe["restored"] = {name: entry.get("enabled")
                         for name, entry in rig.toxiproxy.proxies().items()}
    if probe["restored"] != {name: True for name in PROXY_PORTS}:
        problems.append(f"proxies not re-enabled: {probe['restored']}")
    return finish(probe, started, not problems,
                  "; ".join(problems) or "each proxy carries exactly its own keys")


def probe_capabilities(rig) -> dict:
    """Only the namespace owner has NET_ADMIN; nothing is privileged.

    Read from each container's configuration and from the kernel's view of
    its main process, for the owner, Toxiproxy, the store and any engine
    that has run. The owner must also be able to use it: `iptables -S` in
    its namespace must succeed.
    """
    probe = new_probe("capabilities", rig)
    started = time.monotonic()
    problems = []
    observed = {}
    subjects = [(entry["role"], entry["name"], entry["id"]) for entry in rig.containers
                if not entry["removed"]
                and (entry["role"] != "engine" or entry.get("host_pid"))]
    subjects.append(("store", rig.store.name, rig.store.container))
    for role, name, ident in subjects:
        host = _docker_json(["inspect", "--format", "{{json .HostConfig}}", ident]) or {}
        pid = container_pid(ident)
        caps = effective_capabilities(pid) if pid else {"error": "not running"}
        observed[name] = {
            "role": role, "cap_add": host.get("CapAdd"), "privileged": host.get("Privileged"),
            "network_mode": host.get("NetworkMode"), "effective": caps,
        }
        wants = role == "owner"
        # Docker reports an added capability with or without its CAP_ prefix.
        added = [cap.removeprefix("CAP_") for cap in host.get("CapAdd") or []]
        if host.get("Privileged"):
            problems.append(f"{role} {name} is privileged")
        if added != ([NET_ADMIN] if wants else []):
            problems.append(f"{role} {name} adds capabilities {host.get('CapAdd')}")
        if role != "engine" and caps.get("net_admin") is not wants:
            problems.append(f"{role} {name} effective NET_ADMIN is {caps.get('net_admin')}")
        if role in ("toxiproxy", "engine") and host.get("NetworkMode") != f"container:{rig.owner_id}":
            problems.append(f"{role} {name} is not in the owner's namespace")
    for launch in rig.launcher.launches:
        caps = effective_capabilities(launch["host_pid"]) if launch.get("host_pid") else {}
        if caps.get("net_admin"):
            problems.append(f"engine {launch['name']} has NET_ADMIN")
    usable = _record(probe, rig.exec(["iptables", "-w", "-S"]))
    if usable["exit_status"] != 0:
        problems.append(f"iptables in the owner failed: {usable['stderr'].strip()}")
    probe["evidence"] = {"containers": observed}
    return finish(probe, started, not problems,
                  "; ".join(problems) or "NET_ADMIN in the owner only, no container privileged")


def start_dnsmasq(rig) -> dict:
    """Start the rig's dnsmasq with its private probe zone, once."""
    if getattr(rig, "dnsmasq", None):
        return rig.dnsmasq
    rig.dnsmasq = rig.exec([
        "dnsmasq", "--conf-file=/dev/null", "--user=root", "--no-resolv", "--no-hosts",
        "--bind-interfaces", f"--listen-address={DNS_LISTEN}", "--port=53",
        f"--address=/{PROBE_DNS_NAME}/{PROBE_DNS_ADDRESS}", "--log-queries",
        f"--log-facility={ARTIFACT_MOUNT}/dnsmasq.log",
        "--pid-file=/run/series-dnsmasq.pid",
    ])
    return rig.dnsmasq


def dns_rule(protocol) -> list:
    """The exact namespace-local rule dropping DNS to the rig's dnsmasq."""
    return ["INPUT", "-p", protocol, "-d", f"{DNS_LISTEN}/32", "--dport", "53", "-j", "DROP"]


def rule_counters(rules_text, required_words) -> dict:
    """Packet and byte counters of the one `-A` line containing every word."""
    matches = []
    for line in rules_text.splitlines():
        words = line.split()
        if words[:1] != ["-A"] or not all(word in line for word in required_words):
            continue
        found = re.search(r"-c (\d+) (\d+)", line)
        if found:
            matches.append({"line": line, "packets": int(found.group(1)),
                            "bytes": int(found.group(2))})
    if len(matches) != 1:
        return {"matches": matches, "packets": None, "bytes": None}
    return matches[0]


def probe_dns(rig, protocol) -> dict:
    """Blocking DNS to the rig's resolver times out, and unblocking restores it.

    Resolve the probe name, add the exact port-53 DROP rule for this
    protocol, require a bounded timeout and a positive rule counter, delete
    that exact rule and require the name to resolve again.
    """
    label = "UDP DNS" if protocol == "udp" else "TCP DNS"
    probe = new_probe(label, rig)
    started = time.monotonic()
    problems = []
    daemon = _record(probe, start_dnsmasq(rig))
    if daemon["exit_status"] != 0:
        return finish(probe, started, False, f"dnsmasq did not start: {daemon['stderr'].strip()}")
    dig = ["dig", f"@{DNS_LISTEN}", "-p", "53", f"+time={DNS_TIMEOUT_S}", "+tries=1",
           "+short", PROBE_DNS_NAME, "A"]
    if protocol == "tcp":
        dig.insert(1, "+tcp")
    before = _record(probe, rig.exec(dig, timeout=30))
    if before["exit_status"] != 0 or before["stdout"].strip() != PROBE_DNS_ADDRESS:
        problems.append(f"the probe name did not resolve before the rule: {before['stdout']!r}")
    rule = dns_rule(protocol)
    inserted = _record(probe, rig.exec(["iptables", "-w", "-I", rule[0], "1", *rule[1:]]))
    blocked = counters = None
    if inserted["exit_status"] != 0:
        problems.append(f"the DROP rule could not be inserted: {inserted['stderr'].strip()}")
    else:
        blocked = _record(probe, rig.exec(dig, timeout=30))
        listing = _record(probe, rig.exec(["iptables", "-w", "-v", "-S", "INPUT"]))
        counters = rule_counters(listing["stdout"], ["-p " + protocol, "--dport 53", "-j DROP"])
        deleted = _record(probe, rig.exec(["iptables", "-w", "-D", *rule]))
        if deleted["exit_status"] != 0:
            problems.append(f"the exact rule could not be deleted: {deleted['stderr'].strip()}")
        if blocked["exit_status"] == 0 and blocked["stdout"].strip():
            problems.append("the name still resolved with the DROP rule in place")
        low, high = DNS_TIMEOUT_S * 0.8, DNS_TIMEOUT_S + DNS_TIMEOUT_SLACK_S
        if not low <= blocked["elapsed_s"] <= high:
            problems.append(
                f"the blocked lookup took {blocked['elapsed_s']} s, outside [{low}, {high}]"
            )
        if not (counters.get("packets") or 0) > 0:
            problems.append(f"the DROP rule counted no packets: {counters}")
    after = _record(probe, rig.exec(dig, timeout=30))
    if after["exit_status"] != 0 or after["stdout"].strip() != PROBE_DNS_ADDRESS:
        problems.append(f"the name did not resolve after the rule was deleted: {after['stdout']!r}")
    rules = rig.iptables_rules()
    probe["restored"] = {"iptables_matches_baseline": rules == rig.baseline_rules,
                         "iptables_filter": rules}
    if not probe["restored"]["iptables_matches_baseline"]:
        problems.append("the filter table differs from its baseline after the probe")
    probe["evidence"] = {
        "rule": rule, "counters": counters,
        "blocked_elapsed_s": blocked["elapsed_s"] if blocked else None,
        "blocked_exit_status": blocked["exit_status"] if blocked else None,
        "timeout_s": DNS_TIMEOUT_S, "name": PROBE_DNS_NAME, "address": PROBE_DNS_ADDRESS,
    }
    return finish(probe, started, not problems, "; ".join(problems) or (
        f"{label} blocked in {blocked['elapsed_s']} s with {counters['packets']} dropped "
        f"packets, restored after the exact rule was deleted"))


class Capture:
    """One tcpdump capture in the owner's namespace, written to the artifacts."""

    def __init__(self, rig, name, expression):
        self.rig = rig
        self.file = f"{ARTIFACT_MOUNT}/{name}.pcap"
        self.host_file = rig.artifact_dir / f"{name}.pcap"
        self.argv = ["docker", "exec", rig.owner_id, "timeout", "60", "tcpdump",
                     "-Z", "root", "-i", "any", "--immediate-mode", "-U", "-n",
                     "-w", self.file, expression]
        self.process = None
        self.stderr = ""

    def __enter__(self):
        self.process = subprocess.Popen(self.argv, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.PIPE)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and "listening on" not in self.stderr:
            ready, _, _ = select.select([self.process.stderr], [], [], 0.2)
            if ready:
                line = self.process.stderr.readline().decode(errors="replace")
                if not line:
                    break
                self.stderr += line
        if "listening on" not in self.stderr:
            self.stop()
            raise AssertionError(f"tcpdump did not start: {self.stderr}")
        return self

    # How long packets already received may take to reach the file before
    # the capture is stopped.
    SETTLE_S = 0.5

    def stop(self):
        """Stop the capture with SIGINT so tcpdump flushes the file."""
        if self.process is None:
            return
        time.sleep(self.SETTLE_S)
        _ = self.rig.exec(["pkill", "-INT", "-x", "tcpdump"])
        try:
            _, rest = self.process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            _, rest = self.process.communicate()
        self.stderr += rest.decode(errors="replace")
        self.returncode = self.process.returncode
        self.process = None

    def __exit__(self, *exc):
        self.stop()

    def as_json(self):
        """The capture command, its exit and its tcpdump summary."""
        return {"argv": self.argv, "exit_status": getattr(self, "returncode", None),
                "stderr": _clip(self.stderr), "file": self.host_file.name}


def tshark_counts(rig, capture) -> dict:
    """Packets and TCP retransmissions tshark reads from one capture."""
    total = rig.exec(["tshark", "-n", "-r", capture.file, "-T", "fields", "-e", "frame.number"])
    retrans = rig.exec(["tshark", "-n", "-r", capture.file, "-Y", "tcp.analysis.retransmission",
                        "-T", "fields", "-e", "frame.number"])
    return {
        "commands": [total, retrans],
        "packets": len(total["stdout"].split()) if total["exit_status"] == 0 else None,
        "retransmissions": len(retrans["stdout"].split()) if retrans["exit_status"] == 0 else None,
    }


PROBE_TRANSFER_BYTES = 256 * 1024
# How long the client waits for the stalled upload under the ACK-only rule.
ACK_LOSS_READ_TIMEOUT_S = 5


def signed_transfer(rig, probe, label, *, read_timeout):
    """One signed PUT through the route, then a GET that verifies its bytes.

    Toxiproxy's connection to the store runs inside the owner's namespace,
    so this is the path every namespace rule acts on. Returns the PUT's
    status (None when it did not complete), its duration and whether the
    stored bytes were read back unchanged.
    """
    client = rig.route_client(read_timeout=read_timeout)
    key = f"fault-preflight/{rig.run_id}/dataset=series/{label}"
    body = _payload(PROBE_TRANSFER_BYTES, label)
    put, _ = _s3_op(probe, f"{label}_put", lambda: client.put_object(
        Bucket=rig.store.bucket, Key=key, Body=body), key=key, bytes=len(body))
    outcome = {"key": key, "status": probe["operations"][-1].get("status"),
               "elapsed_s": probe["operations"][-1]["elapsed_s"], "bytes_verified": False}
    if put:
        got, response = _s3_op(probe, f"{label}_get", lambda: client.get_object(
            Bucket=rig.store.bucket, Key=key), key=key)
        outcome["bytes_verified"] = bool(got) and response["Body"].read() == body
        _s3_op(probe, f"{label}_delete", lambda: client.delete_object(
            Bucket=rig.store.bucket, Key=key), key=key)
    return outcome


def succeeded(transfer) -> bool:
    """Whether a signed transfer got a 2xx status and read its bytes back."""
    status = (transfer or {}).get("status")
    return isinstance(status, int) and 200 <= status < 300 and bool(
        transfer.get("bytes_verified"))


BPF_HOST_FIX = "sudo modprobe xt_bpf"


def ack_loss_problems(*, dropped, captured, retransmissions, restored) -> list:
    """Why an ACK-loss probe must fail; empty only when every part held.

    The rule must have dropped at least one pure ACK, the capture must hold
    packets, the sender must have retransmitted at least once because of the
    drops, and after the rule is gone a signed transfer must succeed with a
    2xx status and its bytes read back. Any other HTTP status, a 403
    included, is not a restored route.
    """
    problems = []
    if not isinstance(dropped, int) or dropped <= 0:
        problems.append(f"the ACK-only rule dropped no packets: {dropped}")
    if not isinstance(captured, int) or captured <= 0:
        problems.append(f"the capture holds no packets: {captured}")
    if not isinstance(retransmissions, int) or retransmissions < 1:
        problems.append(f"no retransmission was observed: {retransmissions}")
    if not succeeded(restored):
        problems.append(
            f"the signed transfer after the rule was deleted did not succeed: "
            f"status {(restored or {}).get('status')}, bytes verified "
            f"{(restored or {}).get('bytes_verified')}"
        )
    return problems


def probe_xt_bpf(rig) -> dict:
    """The store's pure ACKs can be dropped by an xt_bpf rule in the namespace.

    Compile the ACK-only filter for the store's address and insert it at the
    head of INPUT. Under a capture, send a signed PUT through the route:
    Toxiproxy's upload to the store loses every pure ACK, so it stalls and
    retransmits until the client gives up. Require dropped ACKs, captured
    packets and at least one retransmission; delete that exact rule and
    require a signed PUT to succeed (2xx, bytes read back) afterwards.
    """
    probe = new_probe("xt_bpf", rig)
    started = time.monotonic()
    problems = []
    probe["evidence"]["modules_before"] = loaded_modules()
    compile_argv = ["docker", "exec", rig.owner_id, "tcpdump", "-ddd", "-y", "RAW",
                    ack_only_expression(rig.store_ip, STORE_PORT)]
    _record(probe, run_command(compile_argv))
    try:
        bytecode = ack_drop_bytecode(rig.store_ip, STORE_PORT,
                                     tcpdump=("docker", "exec", rig.owner_id, "tcpdump"))
    except AssertionError as error:
        return finish(probe, started, False, str(error))
    probe["evidence"]["bytecode"] = bytecode
    rule = ["INPUT", "-p", "tcp", "-m", "bpf", "--bytecode", bytecode, "-j", "DROP"]
    counters, transfer = {}, None
    capture = Capture(rig, f"xt-bpf-{rig.store.kind}",
                      f"host {rig.store_ip} and tcp port {STORE_PORT}")
    try:
        with capture:
            inserted = _record(probe, rig.exec(["iptables", "-w", "-I", "INPUT", "1", *rule[1:]]))
            if inserted["exit_status"] != 0:
                probe["host_fix"] = BPF_HOST_FIX
                problems.append(
                    f"the bpf match rule could not be inserted (the xt_bpf match is "
                    f"unavailable to this namespace): {inserted['stderr'].strip()}"
                )
            else:
                try:
                    transfer = signed_transfer(rig, probe, "ack-loss",
                                               read_timeout=ACK_LOSS_READ_TIMEOUT_S)
                    listing = _record(probe, rig.exec(["iptables", "-w", "-v", "-S", "INPUT"]))
                    counters = rule_counters(listing["stdout"], ["-m bpf", "-j DROP"])
                finally:
                    deleted = _record(probe, rig.exec(["iptables", "-w", "-D", *rule]))
                if deleted["exit_status"] != 0:
                    problems.append(f"the exact rule could not be deleted: {deleted['stderr']}")
    except AssertionError as error:
        problems.append(str(error))
    counts = tshark_counts(rig, capture) if capture.host_file.exists() else {}
    for command in counts.get("commands", []):
        _record(probe, command)
    restored = signed_transfer(rig, probe, "ack-restored", read_timeout=30)
    if not problems:
        problems.extend(ack_loss_problems(
            dropped=counters.get("packets"), captured=counts.get("packets"),
            retransmissions=counts.get("retransmissions"), restored=restored))
    rules = rig.iptables_rules()
    probe["restored"] = {"iptables_matches_baseline": rules == rig.baseline_rules,
                         "iptables_filter": rules, "signed_transfer": restored}
    if not probe["restored"]["iptables_matches_baseline"]:
        problems.append("the filter table differs from its baseline after the probe")
    probe["evidence"].update({
        "rule": rule, "counters": counters,
        "transfer_under_rule": transfer,
        "capture": capture.as_json(),
        "captured_packets": counts.get("packets"),
        "captured_retransmissions": counts.get("retransmissions"),
        "modules_after": loaded_modules(),
    })
    return finish(probe, started, not problems, "; ".join(problems) or (
        f"xt_bpf dropped {counters['packets']} pure ACKs from the store; "
        f"{counts['retransmissions']} retransmissions in {counts['packets']} captured "
        f"packets; a signed PUT succeeded after the rule was deleted"))


def probe_capture(rig) -> dict:
    """tcpdump captures a signed transfer's store traffic and tshark reads it."""
    probe = new_probe("capture", rig)
    started = time.monotonic()
    problems = []
    transfer = None
    capture = Capture(rig, f"capture-{rig.store.kind}",
                      f"host {rig.store_ip} and tcp port {STORE_PORT}")
    try:
        with capture:
            transfer = signed_transfer(rig, probe, "capture", read_timeout=30)
    except AssertionError as error:
        problems.append(str(error))
    if not succeeded(transfer):
        problems.append(f"the captured signed transfer did not succeed: {transfer}")
    counts = tshark_counts(rig, capture) if capture.host_file.exists() else {}
    for command in counts.get("commands", []):
        _record(probe, command)
    if not (counts.get("packets") or 0) > 0:
        problems.append(f"tshark read no packets: {counts.get('packets')}")
    if "0 packets captured" in capture.stderr:
        problems.append("tcpdump wrote no packet it received")
    if counts.get("retransmissions") is None:
        problems.append("tshark could not evaluate tcp.analysis.retransmission")
    probe["evidence"] = {"capture": capture.as_json(), "packets": counts.get("packets"),
                         "retransmissions": counts.get("retransmissions"),
                         "transfer": transfer}
    return finish(probe, started, not problems, "; ".join(problems) or (
        f"{counts['packets']} packets of a signed transfer captured and read back"))


def _timed_get(rig, probe, label, key):
    """One timed GET of `key` through the route; returns its duration or None."""
    client = rig.route_client()
    ok, response = _s3_op(probe, label, lambda: client.get_object(
        Bucket=rig.store.bucket, Key=key), key=key)
    if ok:
        response["Body"].read()
        return probe["operations"][-1]["elapsed_s"]
    return None


def probe_slow(rig) -> dict:
    """`slow` adds both toxics to both proxies and slows real requests."""
    probe = new_probe("slow activation", rig)
    started = time.monotonic()
    problems = []
    client = rig.route_client(read_timeout=60)
    bucket = rig.store.bucket
    keys = {backend: f"fault-preflight/{rig.run_id}/dataset={dataset}/slow"
            for backend, dataset in (("general", "series"), ("values", "values"))}
    body = _payload(PROBE_TRANSFER_BYTES, "slow")
    for key in keys.values():
        _s3_op(probe, "seed_put", lambda: client.put_object(Bucket=bucket, Key=key, Body=body), key=key)
    healthy = {backend: _timed_get(rig, probe, "healthy_get", key) for backend, key in keys.items()}
    try:
        rig.activate("slow", {})
    except AssertionError as error:
        return finish(probe, started, False, str(error))
    try:
        slowed = {backend: _timed_get(rig, probe, "slowed_get", key)
                  for backend, key in keys.items()}
        upload = {}
        for backend, key in keys.items():
            ok, _ = _s3_op(probe, "slowed_put", lambda: client.put_object(
                Bucket=bucket, Key=key, Body=body), key=key)
            upload[backend] = probe["operations"][-1]["elapsed_s"] if ok else None
    finally:
        rig.recover()
    recovered = {backend: _timed_get(rig, probe, "recovered_get", key)
                 for backend, key in keys.items()}
    floor_get = SLOW_LATENCY_MS / 1000.0 * 0.9
    floor_put = PROBE_TRANSFER_BYTES / 1000.0 / SLOW_RATE_KBPS * 0.8
    for backend in keys:
        if not (slowed.get(backend) or 0) >= floor_get:
            problems.append(f"{backend} GET took {slowed.get(backend)} s under latency, "
                            f"expected >= {floor_get}")
        if not (upload.get(backend) or 0) >= floor_put:
            problems.append(f"{backend} PUT took {upload.get(backend)} s under bandwidth, "
                            f"expected >= {floor_put:.3f}")
        if recovered.get(backend) is None or recovered[backend] >= floor_get:
            problems.append(f"{backend} GET still slow after recovery: {recovered.get(backend)}")
    for key in keys.values():
        _s3_op(probe, "delete_object", lambda: client.delete_object(Bucket=bucket, Key=key), key=key)
    probe["evidence"] = {"healthy_get_s": healthy, "slowed_get_s": slowed,
                         "slowed_put_s": upload, "recovered_get_s": recovered,
                         "toxics": slow_toxics(), "units": TOXIC_UNITS,
                         "activation": rig.activations[-1]}
    probe["restored"] = {proxy: rig.toxiproxy.toxics(proxy) for proxy in PROXY_PORTS}
    if any(probe["restored"].values()):
        problems.append(f"toxics remain: {probe['restored']}")
    return finish(probe, started, not problems, "; ".join(problems) or
                  "both proxies slowed real GETs and PUTs and recovered")


def probe_http503(rig) -> dict:
    """`http503` answers 503 on the S3 path until its control file is removed."""
    probe = new_probe("http503 activation", rig)
    started = time.monotonic()
    problems = []
    client = rig.route_client()
    key = f"fault-preflight/{rig.run_id}/dataset=series/http503"
    try:
        rig.activate("http503", {})
    except AssertionError as error:
        return finish(probe, started, False, str(error))
    try:
        ok, _ = _s3_op(probe, "put_under_503", lambda: client.put_object(
            Bucket=rig.store.bucket, Key=key, Body=b"503"), key=key)
    finally:
        rig.recover()
    status = probe["operations"][-1].get("status")
    if ok or status != 503:
        problems.append(f"PUT under the fault answered {status}, expected 503")
    ok_after, _ = _s3_op(probe, "put_after_recovery", lambda: client.put_object(
        Bucket=rig.store.bucket, Key=key, Body=b"503"), key=key)
    if not ok_after:
        problems.append("PUT failed after the control file was removed")
    _s3_op(probe, "delete_object", lambda: client.delete_object(Bucket=rig.store.bucket, Key=key))
    time.sleep(0.2)
    logged = [entry for entry in parse_access_log(rig.artifact_dir / ACCESS_LOG)
              if entry.get("uri", "").endswith("/http503")]
    statuses = [entry.get("status") for entry in logged if entry.get("method") == "PUT"]
    if statuses[:1] != ["503"] or "200" not in statuses:
        problems.append(f"the access log shows PUT statuses {statuses}")
    probe["evidence"] = {"access_log": logged, "activation": rig.activations[-1]}
    probe["restored"] = {"fail503_present": rig.fail503.exists()}
    if rig.fail503.exists():
        problems.append("the control file remains")
    return finish(probe, started, not problems, "; ".join(problems) or
                  "NGINX answered 503 on a real PUT and recovered")


def probe_store_outage(rig) -> dict:
    """`store_outage` stops the store behind the route and recovers it."""
    probe = new_probe("store_outage activation", rig)
    started = time.monotonic()
    problems = []
    client = rig.route_client(read_timeout=10)
    key = f"fault-preflight/{rig.run_id}/dataset=series/outage"
    try:
        rig.activate("store_outage", {})
    except AssertionError as error:
        return finish(probe, started, False, str(error))
    try:
        ok, _ = _s3_op(probe, "put_during_outage", lambda: client.put_object(
            Bucket=rig.store.bucket, Key=key, Body=b"outage"), key=key)
        if ok:
            problems.append("a PUT succeeded while the store was stopped")
    finally:
        rig.recover()
    ok, _ = _s3_op(probe, "put_after_recovery", lambda: client.put_object(
        Bucket=rig.store.bucket, Key=key, Body=b"outage"), key=key)
    if not ok:
        problems.append("a PUT failed after the store recovered")
    _s3_op(probe, "delete_object", lambda: client.delete_object(Bucket=rig.store.bucket, Key=key))
    probe["evidence"] = {"activation": rig.activations[-1]}
    probe["restored"] = {"store_address": rig.store_ip,
                         "proxies": {name: entry.get("upstream") for name, entry in
                                     rig.toxiproxy.proxies().items()}}
    return finish(probe, started, not problems, "; ".join(problems) or
                  "PUTs fail while the store is stopped and succeed after recovery")


def probe_engine(rig, binary=None) -> dict:
    """The release engine runs in the namespace and exports through the route.

    One log request is acknowledged, its series and values objects are
    listed directly in the store, NGINX logged the engine's PUTs on both
    backends, and the engine process has no NET_ADMIN.
    """
    probe = new_probe("engine launch", rig)
    started = time.monotonic()
    problems = []
    binary = Path(binary or os.environ.get(
        "DF_ENGINE", test_e2e.WORKSPACE / "target/release/df_engine"))
    if not binary.is_file():
        return finish(probe, started, False, f"no engine binary at {binary}")
    directory = rig.root / "engine-probe"
    directory.mkdir(parents=True, exist_ok=True)
    engine = None
    try:
        engine = test_e2e.Engine(directory, storage=rig.storage, launcher=rig.launcher,
                                 binary=binary, overrides={"retry": test_e2e.S3_RETRY})
        probe["evidence"]["host_pid"] = engine.pid
        probe["evidence"]["capabilities"] = effective_capabilities(engine.pid)
        if probe["evidence"]["capabilities"].get("net_admin") is not False:
            problems.append(f"the engine's capabilities: {probe['evidence']['capabilities']}")
        engine.logs.Export(test_e2e.log_request("fault-preflight"), timeout=60)
        deadline = time.monotonic() + 30
        keys = []
        while time.monotonic() < deadline:
            listed = rig.store.client.list_objects_v2(Bucket=rig.store.bucket, Prefix="otel/")
            keys = [item["Key"] for item in listed.get("Contents", [])
                    if item["Key"].endswith(".parquet")]
            if any("dataset=values/" in key for key in keys) and any(
                    "dataset=series/" in key for key in keys):
                break
            time.sleep(0.2)
        probe["evidence"]["objects"] = keys
        engine.shutdown(60)
    except Exception as error:
        problems.append(f"{type(error).__name__}: {error}")
        if engine is not None:
            probe["evidence"]["engine_log_tail"] = _clip(engine.engine_log()[-OUTPUT_LIMIT:])
    finally:
        if engine is not None:
            engine.close()
    objects = probe["evidence"].get("objects") or []
    if not any("dataset=values/" in key for key in objects) or not any(
            "dataset=series/" in key for key in objects):
        problems.append(f"the engine stored {objects}")
    engine_puts = [entry for entry in parse_access_log(rig.artifact_dir / ACCESS_LOG)
                   if "/otel/" in entry.get("uri", "") and entry.get("method") == "PUT"]
    probe["evidence"]["engine_puts"] = engine_puts
    backends = sorted({backend_of(entry["uri"]) for entry in engine_puts
                       if entry.get("status") == "200"})
    if backends != ["general", "values"]:
        problems.append(f"the engine's successful PUTs used backends {backends}")
    probe["evidence"]["ldd"] = [dict(report, binary=str(path))
                                for path, report in rig.launcher.ldd.items()]
    probe["evidence"]["launch"] = rig.launcher.launches[-1] if rig.launcher.launches else None
    return finish(probe, started, not problems, "; ".join(problems) or
                  "the containerized release engine exported through both backends")


def probe_restored(rig) -> dict:
    """Nothing a probe installed remains: rules, toxics, control file, faults."""
    probe = new_probe("restored state", rig)
    started = time.monotonic()
    state = rig.residual_state()
    probe["restored"] = state
    return finish(probe, started, state["clean"],
                  "no rule, toxic, control file or active fault remains" if state["clean"]
                  else f"left over: {state}")


PROBES = {
    "capabilities": probe_capabilities,
    "S3 route general": lambda rig: probe_route(rig, "general"),
    "S3 route values": lambda rig: probe_route(rig, "values"),
    "route isolation": probe_route_isolation,
    "UDP DNS": lambda rig: probe_dns(rig, "udp"),
    "TCP DNS": lambda rig: probe_dns(rig, "tcp"),
    "xt_bpf": probe_xt_bpf,
    "capture": probe_capture,
    "slow activation": probe_slow,
    "http503 activation": probe_http503,
    "store_outage activation": probe_store_outage,
    "engine launch": probe_engine,
    "restored state": probe_restored,
}

# The preflight's order: the route before anything relies on it, the engine
# before the capability probe so that its process is covered too, and the
# residual check last.
PREFLIGHT_PROBES = (
    "S3 route general", "S3 route values", "route isolation", "engine launch",
    "capabilities", "UDP DNS", "TCP DNS", "xt_bpf", "capture",
    "slow activation", "http503 activation", "store_outage activation",
    "restored state",
)


# --------------------------------------------------------------------------
# measure fault-preflight
# --------------------------------------------------------------------------

PREFLIGHT_STORES = ("minio", "rustfs")


def _store_preflight(kind, root, probes_out, rigs_out):
    """Every preflight probe against one store kind, in its own rig."""
    try:
        with test_e2e.DockerStore(kind, by_image_id=True) as store:
            rig = FaultRig(store, root / kind, probes=())
            try:
                with rig:
                    for name in PREFLIGHT_PROBES:
                        try:
                            probe = PROBES[name](rig)
                        except Exception as error:  # a probe's own defect
                            probe = finish(new_probe(name, rig), time.monotonic(), False,
                                           f"the probe raised {type(error).__name__}: {error}")
                        probes_out.append(probe)
            finally:
                rigs_out.append(rig.evidence())
                cleanup = new_probe("cleanup", rig)
                report = rig.cleanup_report or {}
                cleanup["restored"] = report
                probes_out.append(finish(
                    cleanup, time.monotonic(), bool(report.get("clean")),
                    "every recorded container and the network were removed"
                    if report.get("clean") else f"cleanup incomplete: {report}"))
    except unittest.SkipTest as skipped:
        probe = new_probe(f"{kind} rig")
        probe["store"] = kind
        probes_out.append(finish(probe, time.monotonic(), False, f"skipped: {skipped}"))
    except AssertionError as error:
        probe = new_probe(f"{kind} rig")
        probe["store"] = kind
        probes_out.append(finish(probe, time.monotonic(), False, str(error)))


def preflight_metrics(probes) -> dict:
    """The recorded numbers: counts plus each probe's own measured values."""
    metrics = {
        "probes_count": len(probes),
        "probes_failed_count": sum(1 for probe in probes if probe.get("passed") is not True),
    }
    for probe in probes:
        slug = re.sub(r"[^a-z0-9]+", "_", f"{probe.get('store')}_{probe['name']}".lower()).strip("_")
        metrics[f"{slug}_elapsed_s"] = probe.get("elapsed_s", 0.0)
        evidence = probe.get("evidence") or {}
        if isinstance(evidence.get("blocked_elapsed_s"), (int, float)):
            metrics[f"{slug}_blocked_elapsed_s"] = evidence["blocked_elapsed_s"]
        counters = evidence.get("counters") or {}
        if isinstance(counters.get("packets"), int):
            metrics[f"{slug}_rule_packets_count"] = counters["packets"]
        if isinstance(evidence.get("captured_retransmissions"), int):
            metrics[f"{slug}_retransmissions_count"] = evidence["captured_retransmissions"]
    return metrics


def preflight_fault_tools(required: bool, *, output_dir=None, report_dir=None,
                          stores=PREFLIGHT_STORES, lease_wait_s=DEFAULT_LEASE_WAIT_S,
                          publish=True) -> dict:
    """Implement `measure fault-preflight` and return its result.

    Image provenance is read before the host lease; the probes run under
    it, one rig per store kind. The evidence -- every probe's commands, exit
    statuses, counters, latencies and restored-state checks, the rigs'
    cleanup and the fault-class coverage -- is written and published before
    any verdict is raised. Then a failed probe raises AssertionError when
    `required`, or `unittest.SkipTest` otherwise.
    """
    output_dir = Path(output_dir or "/tmp/series-fault-preflight")
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    result = measurement.new_result({"run_id": "fault-preflight", "case": "fault-preflight"})
    result["environment"]["start"] = measurement.environment_snapshot({"harness": os.getpid()})
    result["environment"]["git"] = measurement.git_provenance()
    result["required"] = bool(required)
    probes, rigs = [], []
    try:
        images = require_fault_images()
    except (unittest.SkipTest, AssertionError) as missing:
        images = None
        probe = new_probe("images")
        probes.append(finish(probe, time.monotonic(), False, str(missing)))
    if images is not None:
        images["fault_tools"]["packages"] = package_versions(images["fault_tools"]["id"])
        images["fault_tools"]["dockerfile_sha256"] = measurement.file_digest(DOCKERFILE)
        images["fault_tools"]["nginx_conf_sha256"] = measurement.file_digest(NGINX_CONF)
        result["environment"]["images"] = images
        lease = measurement.HostLease(run_id="fault-preflight")
        lease.acquire(deadline_ns=time.monotonic_ns() + int(float(lease_wait_s) * 10**9))
        result["environment"]["lease"] = lease.as_json()
        try:
            for kind in stores:
                _store_preflight(kind, output_dir / "rigs", probes, rigs)
        finally:
            lease.release()
            result["environment"]["lease"] = lease.as_json()
        result["environment"]["kernel_modules"] = loaded_modules()
    result["probes"] = probes
    result["rigs"] = rigs
    result["coverage"] = coverage(probes, required=required, stores=list(stores))
    result["toxiproxy"] = {"image": TOXIPROXY_IMAGE, "version": TOXIPROXY_VERSION,
                           "units": TOXIC_UNITS}
    for probe in probes:
        result["checks"].append(measurement.check(
            f"{probe.get('store')}: {probe['name']}", measurement.CHECK_HARD,
            measurement.STATUS_PASSED if probe.get("passed") is True else measurement.STATUS_FAILED,
            probe.get("detail", ""),
        ))
    result["metrics"] = preflight_metrics(probes)
    passed = bool(probes) and all(probe.get("passed") is True for probe in probes)
    result["status"] = (
        measurement.STATUS_PASSED if passed
        else measurement.STATUS_FAILED if required else measurement.STATUS_SKIPPED
    )
    result["policy"] = (
        "Probe latencies and counters are diagnostic bounds, not performance "
        "quantities: no baseline is written or compared for this index."
    )
    result["environment"]["end"] = measurement.environment_snapshot({"harness": os.getpid()})
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"])
    result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
    if publish:
        previous = measurement.archive_published_index("fault-preflight.json", output_dir,
                                                       report_dir)
        result["child_indexes"] = [previous] if previous else []
    written = measurement.write_result(output_dir / "fault-preflight.json", result)
    result["written"] = str(written)
    if publish:
        result["published"] = str(measurement.publish_result_tree(written, report_dir))
    for probe in probes:
        require_probe(probe, required=required)
    return result


# --------------------------------------------------------------------------
# Fault cases: `measure failures`
# --------------------------------------------------------------------------

# Each family's faults; a matrix cell is one fault, topology and store.
FAILURE_FAMILIES = {"s3": ("slow", "http503", "store_outage"),
                    "process": ("graceful_restart", "kill_active", "kill_upload")}
FAILURE_TOPOLOGIES = ("strict", "buffered")

# A finite mixed-signal producer: 20 requests of 100 one-KiB records a
# second, every tenth a metrics request. It stops on the case's schedule;
# the request cap only bounds a case that never gets there.
FAULT_RATE_REQUESTS_PER_S = 20
FAULT_MAX_REQUESTS = FAULT_RATE_REQUESTS_PER_S * 900
FAULT_WORKLOAD = measurement.Workload(
    requests=FAULT_MAX_REQUESTS, records_per_request=100, body_bytes=1024, series=100,
    metrics_every=10,
)
# Five-second windows of that input hold about 6.8 MB of compressed logs
# values, above the S3 minimum part size of 5 MiB, so every case uploads
# its logs values files as multipart uploads.
FAULT_INTERVAL_S = 5
FAULT_EXPORTER_MERGE = {"upload": {"part_bytes": "5MiB"}}
# Input before the fault is armed: two windows plus five seconds.
FAULT_BASELINE_S = 2 * FAULT_INTERVAL_S + 5
# Input after the exporter resumed, before the producer stops.
FAULT_AFTER_S = 20
# How long each fault may take to show its intended condition; for a
# process case, how long each of its lifecycle gates may take.
FAULT_OBSERVE_DEADLINE_S = {"slow": 180, "http503": 240, "store_outage": 240,
                            "graceful_restart": 60, "kill_active": 60, "kill_upload": 120}
# Everything after the endpoint is healthy again -- resumption, the rest of
# the input and the drain -- must finish within this.
RECOVERY_DEADLINE_S = 300
ENDPOINT_HEALTH_DEADLINE_S = 120
# The producer resends a retryable refusal with the same bytes until it is
# acknowledged; the attempts outlast any fault plus the recovery deadline.
FAULT_PRODUCER_ATTEMPTS = 2000
FAULT_PRODUCER_TIMEOUT_S = 180.0
FAULT_LISTING_PERIOD_S = 1.0
# A cell run with `straddle_hour` arms its fault this long before a
# partition hour ends, so the hour's last blocks are written under the
# fault and the lateness bound is exercised. It takes the lease the lead
# before arming: rig, engine and baseline took 20 s of it on this host, so
# the case waits under the lease, with its input running, about 15 s.
STRADDLE_ARM_BEFORE_END_S = 10
STRADDLE_LEAD_S = 35
STRADDLE_MAX_WAIT_S = 3600 + STRADDLE_LEAD_S
# A request NGINX logged at least this long under the slow fault was delayed by it.
SLOW_DELAY_FLOOR_S = SLOW_LATENCY_MS / 1000.0 * 0.9
SHIPPED_S3_CONFIG = test_e2e.WORKSPACE / "configs/series-parquet-s3.yaml"
# How long an outage is held past the deadline of the block that failed.
OUTAGE_HOLD_MARGIN_S = 15


def main_checkout() -> Path:
    """The main checkout of this repository, also when running from a worktree."""
    done = subprocess.run(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=measurement.REPO_ROOT, capture_output=True, text=True, timeout=30, check=False)
    common = Path(done.stdout.strip()) if done.returncode == 0 and done.stdout.strip() else None
    return common.parent if common is not None and common.name == ".git" \
        else measurement.REPO_ROOT


# Raw fault-case archives live in the main checkout, shared by its worktrees,
# one directory per family.
FAULT_ARCHIVE_ROOT = main_checkout() / ".measurement-artifacts"
FAULT_ARCHIVE_DIR = FAULT_ARCHIVE_ROOT / "failure-s3"
STORAGE_NACK_SENTENCE = "could not write to object storage ("
# The compared metrics: memory and the correctness counts. Durations and
# counts depend on where in a window the fault landed and are recorded only.
FAULT_DIRECTIONS = {
    "peak_rss_bytes": measurement.LOWER_IS_BETTER,
    "accounted_peak_bytes": measurement.LOWER_IS_BETTER,
    "buffer_storage_peak_bytes": measurement.LOWER_IS_BETTER,
    "missing_acked_records": measurement.LOWER_IS_BETTER,
    "missing_records": measurement.LOWER_IS_BETTER,
    "unexpected_records": measurement.LOWER_IS_BETTER,
    "corrupt_records": measurement.LOWER_IS_BETTER,
}
# The fault checks every case of every family records; `fault_check`
# requires them present and every hard check of the result passed.
FAULT_CHECKS = ("fault_observed", "recovered", "drained", "at_least_once",
                "descriptor_coverage", "reader_agreement", "bounded_resources",
                "multipart_exercised", "orphaned_uploads_expected", "partition_lateness_bound",
                "duplicates_explained", "fault_rig_clean")


def shipped_s3_retry() -> dict:
    """The store retry section `configs/series-parquet-s3.yaml` ships."""
    document = yaml.safe_load(SHIPPED_S3_CONFIG.read_text())
    return dict(document["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]
                ["config"]["retry"])


def duration_s(text) -> float:
    """A humantime duration such as `60s` or `200ms`, in seconds."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)\s*", str(text))
    if not match:
        raise ValueError(f"not a duration: {text!r}")
    return float(match[1]) * {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}[match[2]]


def exporter_settings(engine_config) -> dict:
    """The effective exporter settings of one launched configuration."""
    return engine_config["groups"]["default"]["pipelines"]["main"]["nodes"]["exporter"]["config"]


def lateness_bound_s(settings) -> float:
    """FORMAT.md's partition lateness bound for these exporter settings.

    `L = window.interval + 2 * (flush_retry_deadline + upload.abort_timeout)`.
    """
    window = settings["window"]
    return duration_s(window["interval"]) + 2 * (
        duration_s(window["flush_retry_deadline"])
        + duration_s(settings["upload"]["abort_timeout"]))


PARTITION = re.compile(r"/date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/")


def partition_hour(key):
    """The partition hour of an object key and the instant it ends, or None."""
    match = PARTITION.search("/" + key)
    if not match:
        return None
    start = datetime.datetime.strptime(match[1], "%Y-%m-%d").replace(
        hour=int(match[2]), tzinfo=datetime.timezone.utc)
    return f"{match[1]}T{match[2]}", (start + datetime.timedelta(hours=1)).timestamp()


def partition_lateness(objects, bound_s) -> dict:
    """The latest visibility in each partition hour against the hour's end.

    Each object carries the store's `last_modified_unix_s` and the first
    time a direct listing showed it, `first_listed_unix_s` (None when no
    listing saw it before the final one). An hour is violated when either
    instant of any of its objects lies more than `bound_s` after the hour
    ended.
    """
    fields = (("last_modified_unix_s", "latest_last_modified_after_end_s"),
              ("first_listed_unix_s", "latest_first_listed_after_end_s"))
    hours = {}
    after_end = 0
    for item in objects:
        found = partition_hour(item["key"])
        if found is None:
            continue
        hour, end = found
        entry = hours.setdefault(hour, {"hour_end_unix_s": end, "objects_count": 0,
                                        **{name: None for _field, name in fields}})
        entry["objects_count"] += 1
        late_any = False
        for field, name in fields:
            if item.get(field) is None:
                continue
            late = round(item[field] - end, 3)
            late_any |= late > 0
            if entry[name] is None or late > entry[name]:
                entry[name] = late
        after_end += late_any
    violations = sorted(hour for hour, entry in hours.items()
                        if any(entry[name] is not None and entry[name] > bound_s
                               for _field, name in fields))
    return {"bound_s": bound_s, "hours": dict(sorted(hours.items())),
            "violations": violations, "objects_visible_after_hour_end_count": after_end}


def s3_operation(entry) -> str:
    """The S3 operation of one NGINX log entry, named from method and query."""
    method = entry.get("method") or ""
    uri = entry.get("request_uri") or entry.get("uri") or ""
    query = urllib.parse.parse_qs(uri.partition("?")[2], keep_blank_values=True)
    if "uploads" in query and method == "POST":
        return "create_multipart_upload"
    if "uploadId" in query:
        return {"PUT": "upload_part" if "partNumber" in query else "put_object",
                "POST": "complete_multipart_upload", "DELETE": "abort_multipart_upload",
                "GET": "list_parts"}.get(method, "unknown")
    if method == "GET" and ("list-type" in query or "prefix" in query):
        return "list_objects"
    return {"PUT": "put_object", "HEAD": "head_object", "GET": "get_object",
            "DELETE": "delete_object", "POST": "post"}.get(method, "unknown")


def engine_requests(entries, bucket) -> list:
    """The access log entries of the exporter's own requests, each with its operation."""
    found = []
    for entry in entries:
        uri = entry.get("request_uri") or entry.get("uri") or ""
        if f"/{bucket}/otel/" in uri or (uri.startswith(f"/{bucket}") and "prefix=otel" in uri):
            found.append(dict(entry, operation=s3_operation(entry)))
    return found


def _within(entry, start_unix, end_unix) -> bool:
    """Whether a log entry completed inside [start, end); no end is open."""
    try:
        instant = float(entry["msec"])
    except (KeyError, ValueError):
        return False
    return start_unix <= instant and (end_unix is None or instant < end_unix)


def _float(value):
    """A logged number, or None for NGINX's `-`."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def route_summary(requests, start_unix=None, end_unix=None) -> dict:
    """Counts, statuses, latencies and upload bandwidth of the logged requests.

    With `start_unix`, only requests that completed in [start, end) count.
    """
    chosen = [entry for entry in requests
              if start_unix is None or _within(entry, start_unix, end_unix)]
    by_operation = collections.defaultdict(collections.Counter)
    latency = collections.defaultdict(list)
    uploaded = 0.0
    upload_time = 0.0
    for entry in chosen:
        by_operation[entry["operation"]][entry.get("status", "?")] += 1
        seconds = _float(entry.get("request_time"))
        if seconds is not None:
            latency[entry["operation"]].append(seconds)
        size = _float(entry.get("request_length"))
        if entry["operation"] in ("put_object", "upload_part") and size and seconds:
            uploaded += size
            upload_time += seconds
    return {
        "requests_count": len(chosen),
        "status_by_operation": {op: dict(counts) for op, counts in sorted(by_operation.items())},
        "request_time_max_s_by_operation": {op: max(values)
                                            for op, values in sorted(latency.items())},
        "request_time_p50_s_by_operation": {
            op: measurement.percentile(sorted(values), 0.5)
            for op, values in sorted(latency.items())},
        "http_503_writes_count": sum(1 for entry in chosen if entry.get("status") == "503"
                                     and entry.get("method") in ("PUT", "POST")),
        "upload_bytes": int(uploaded),
        "upload_bytes_per_s": uploaded / upload_time if upload_time else None,
    }


MULTIPART_ETAG = re.compile(r'"?[0-9a-f]{32}-(\d+)"?')


def multipart_evidence(requests, objects, part_bytes) -> dict:
    """Whether the multipart path ran against the store, seen from both sides.

    The route's trace must show CreateMultipartUpload, at least two
    UploadPart and a CompleteMultipartUpload answered 2xx, and the store
    must hold an object above `part_bytes` with a multipart ETag
    (`"<md5>-<parts>"`). `applicable` says whether the store holds any
    object above one part at all.
    """
    ok = collections.Counter(entry["operation"] for entry in requests
                             if str(entry.get("status", "")).startswith("2"))
    large = [item for item in objects if (item.get("size_bytes") or 0) > part_bytes]
    multipart = [item for item in large if MULTIPART_ETAG.fullmatch(item.get("etag") or "")]
    return {
        "part_bytes": part_bytes,
        "applicable": bool(large),
        "exercised": (ok["create_multipart_upload"] >= 1 and ok["upload_part"] >= 2
                      and ok["complete_multipart_upload"] >= 1 and bool(multipart)),
        "create_multipart_upload_ok_count": ok["create_multipart_upload"],
        "upload_part_ok_count": ok["upload_part"],
        "complete_multipart_upload_ok_count": ok["complete_multipart_upload"],
        "abort_multipart_upload_ok_count": ok["abort_multipart_upload"],
        "objects_above_part_bytes_count": len(large),
        "multipart_etag_objects_count": len(multipart),
        "parts_per_object": sorted({int(MULTIPART_ETAG.fullmatch(item["etag"])[1])
                                    for item in multipart}),
        "example": ({key: multipart[0][key] for key in ("key", "size_bytes", "etag")}
                    if multipart else None),
    }


ANSI = re.compile(r"\x1b\[[0-9;]*m")
EVENT_LINE = re.compile(
    r"\b(TRACE|DEBUG|INFO|WARN|ERROR)\s+\S*?(series_parquet\.[a-z_.]+): ([^\[]*)\[(.*)\]")


LOG_TIMESTAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)Z")


def flush_failures(lines) -> list:
    """Every `flush.failed` event in engine log lines: its time, class, window and file."""
    found = []
    for line in lines:
        line = ANSI.sub("", line)
        match = EVENT_LINE.search(line)
        stamp = LOG_TIMESTAMP.search(line)
        if not match or not stamp or match[2] != "series_parquet.flush.failed":
            continue
        fields = match[4]
        error_type = re.search(r"\berror_type=(\w+)", fields)
        window = re.search(r"\bwindow_start=(\d+)", fields)
        file = re.search(r"\bfile=([^,\s\]]+)", fields)
        found.append({
            "unix_s": datetime.datetime.fromisoformat(stamp[1]).replace(
                tzinfo=datetime.timezone.utc).timestamp(),
            "error_type": error_type[1] if error_type else None,
            "window_start_unix_s": int(window[1]) if window else None,
            "file": file[1] if file else None,
        })
    return found


class LogTail:
    """The complete lines appended to a log since the last read."""

    def __init__(self, path):
        self.path = Path(path)
        self.offset = 0
        self.partial = b""

    def lines(self) -> list:
        """The complete lines written since the previous call."""
        try:
            with self.path.open("rb") as handle:
                handle.seek(self.offset)
                data = handle.read()
        except OSError:
            return []
        self.offset += len(data)
        data = self.partial + data
        complete, _, self.partial = data.rpartition(b"\n")
        return complete.decode(errors="replace").splitlines() if complete else []


def engine_events(log_text) -> dict:
    """The exporter's flush, request and upload events, counted by name and outcome.

    `failed_files` names the block file of every `flush.failed` event,
    `flush_failures` gives each one's time, class and window, and
    `cleanup_files` names the block file of every `flush.cleanup` event, by
    outcome.
    """
    counts = collections.Counter()
    samples = collections.defaultdict(list)
    failed = []
    cleanup = collections.defaultdict(list)
    for line in log_text.splitlines():
        match = EVENT_LINE.search(ANSI.sub("", line))
        if not match:
            continue
        level, name, message, fields = match.groups()
        if not name.startswith(("series_parquet.flush", "series_parquet.request",
                                "series_parquet.upload")):
            continue
        outcome = re.search(r"\boutcome=([A-Za-z_]+)", fields)
        key = f"{name}{{{outcome[1]}}}" if outcome else name
        counts[key] += 1
        if len(samples[key]) < 3:
            samples[key].append(f"{level} {message.strip()} [{fields}]"[:400])
        file = re.search(r"\bfile=([^,\s\]]+)", fields)
        if file and name == "series_parquet.flush.failed":
            failed.append(file[1])
        elif file and name == "series_parquet.flush.cleanup":
            cleanup[outcome[1] if outcome else "unknown"].append(file[1])
    return {"counts": dict(sorted(counts.items())), "samples": dict(sorted(samples.items())),
            "failed_files": failed, "flush_failures": flush_failures(log_text.splitlines()),
            "cleanup_files": dict(sorted(cleanup.items()))}


def failed_block_objects(events, objects) -> dict:
    """The stored objects of every block whose flush failed, by block file name.

    A failed block can leave some or all of its objects behind; their rows
    are stored again when the nacked requests are resent.
    """
    return {name: sorted(item["key"] for item in objects if item["key"].endswith("/" + name))
            for name in events["failed_files"]}


def flat_totals(sample) -> dict:
    """One sample's exporter gauges and counters, summed over its workers.

    A labelled counter appears as its total and as `name.label` per label.
    """
    flat = collections.Counter()
    for worker in (sample.get("workers") or {}).values():
        for name, value in (worker.get("gauges") or {}).items():
            if isinstance(value, (int, float)):
                flat[name] += value
    scalars = set(flat)
    for entry in (sample.get("extras") or {}).values():
        for name, value in entry.items():
            if isinstance(value, (int, float)) and name not in scalars:
                flat[name] += value
            elif isinstance(value, dict) and name != "flush.duration":
                for label, count in value.items():
                    if isinstance(count, (int, float)):
                        flat[name] += count
                        flat[f"{name}.{label}"] += count
    return dict(flat)


def ledger_attempts(ledger, since_ns=0) -> dict:
    """The producer's attempts started since `since_ns`, by outcome and status code.

    A storage nack is a retryable attempt whose status message is the
    exporter's storage sentence; a producer-local timeout is
    DEADLINE_EXCEEDED, which the client raises itself.
    """
    with ledger.lock:
        rows = ledger.connection.execute(
            "SELECT outcome, detail, count(*) FROM attempts WHERE start_ns >= ? "
            "GROUP BY outcome, detail", (int(since_ns),)).fetchall()
    by_code = collections.Counter()
    by_outcome = collections.Counter()
    classes = collections.Counter()
    for outcome, detail, count in rows:
        detail = detail or ""
        by_outcome[outcome] += count
        by_code[detail.split(":", 1)[0].replace("StatusCode.", "") or "OK"] += count
        if STORAGE_NACK_SENTENCE in detail:
            classes[detail.split(STORAGE_NACK_SENTENCE, 1)[1].split(")", 1)[0]] += count
    return {"by_outcome": dict(sorted(by_outcome.items())),
            "by_code": dict(sorted(by_code.items())),
            "storage_nacks_count": sum(classes.values()),
            "storage_nack_classes": dict(classes),
            "local_timeouts_count": by_code.get("DEADLINE_EXCEEDED", 0)}


def ledger_acks(ledger) -> list:
    """Every acknowledged request's acknowledgement instant, sorted."""
    with ledger.lock:
        return [row[0] for row in ledger.connection.execute(
            "SELECT ack_ns FROM requests WHERE ack_ns IS NOT NULL ORDER BY ack_ns")]


def duplicate_attribution(ledger, failed_block_ids=()) -> dict:
    """Stored duplicates, split by where their copies can come from.

    Runs after `read_oracle` loaded the stored rows into the ledger's
    `actual` table. A duplicated record whose request the producer resent
    was replayed by the producer; one with a copy in a file of a block whose
    flush failed (`failed_block_ids`, from `failed_block_record_ids`) was
    stored by the failed block and again by the retry of its nacked request.
    """
    with ledger.lock:
        connection = ledger.connection
        _ = connection.execute("DROP TABLE IF EXISTS failed_block_rows")
        _ = connection.execute("CREATE TEMP TABLE failed_block_rows (record_id TEXT PRIMARY KEY)")
        _ = connection.executemany("INSERT OR IGNORE INTO failed_block_rows VALUES (?)",
                                   [(record_id,) for record_id in failed_block_ids])
        row = connection.execute(
            "SELECT count(*), coalesce(sum(d.copies - 1), 0), "
            "coalesce(sum(CASE WHEN t.n > 1 THEN 1 ELSE 0 END), 0), "
            "coalesce(sum(CASE WHEN f.record_id IS NOT NULL THEN 1 ELSE 0 END), 0) FROM ("
            "SELECT e.record_id, e.request_id, count(a.record_id) AS copies "
            "FROM records e JOIN actual a ON a.record_id = e.record_id "
            "GROUP BY e.record_id HAVING count(a.record_id) > 1) d "
            "JOIN (SELECT request_id, count(*) AS n FROM attempts GROUP BY request_id) t "
            "ON t.request_id = d.request_id "
            "LEFT JOIN failed_block_rows f ON f.record_id = d.record_id").fetchone()
        resent = connection.execute(
            "SELECT count(*) FROM (SELECT request_id FROM attempts GROUP BY request_id "
            "HAVING count(*) > 1)").fetchone()[0]
        failed_requests = connection.execute(
            "SELECT count(DISTINCT e.request_id) FROM records e "
            "JOIN failed_block_rows f ON f.record_id = e.record_id").fetchone()[0]
    duplicated, extra, replayed, in_failed = (int(value) for value in row)
    return {"duplicated_records": duplicated, "extra_copies_records": extra,
            "duplicated_in_resent_requests_records": replayed,
            "duplicated_outside_resent_requests_records": duplicated - replayed,
            "resent_requests_count": int(resent),
            "duplicated_in_failed_blocks_records": in_failed,
            "duplicated_outside_failed_blocks_records": duplicated - in_failed,
            "failed_block_records_count": len(set(failed_block_ids)),
            "failed_block_requests_count": int(failed_requests)}


def failed_block_record_ids(root, workload, failed_files) -> set:
    """The record ids stored in the values files of blocks whose flush failed.

    `failed_files` are the block file names of the exporter's `flush.failed`
    events; `root` is the downloaded store.
    """
    names = set(failed_files)
    found = set()
    if not names:
        return found
    with measurement.duckdb.connect() as db:
        for signal in ("logs", "metrics"):
            files = [path for path in Path(root).glob(
                f"v=1/signal={signal}/dataset=values/**/*.parquet") if path.name in names]
            if not files:
                continue
            record_id, _payload = measurement._duck_record_expression(workload, signal)
            relation = (f"read_parquet({[str(path) for path in files]!r}, union_by_name=true, "
                        "hive_partitioning=false)")
            found.update(row[0] for row in db.execute(f"SELECT {record_id} FROM {relation} v"
                                                      ).fetchall())
    return found


def duplicates_explained(buffered, duplicates) -> tuple:
    """Whether every stored duplicate is attributed, and why.

    Strict: each duplicated record belongs to a request the producer resent.
    Buffered: the producer never resends after the log acknowledged it, so
    each duplicated record must have a copy in a file of a failed block,
    whose nacked requests the buffer delivered again.
    """
    if not duplicates:
        return False, "the duplicates were not measured"
    if buffered:
        outside = duplicates.get("duplicated_outside_failed_blocks_records")
        return outside == 0, (
            f"{outside} of {duplicates.get('duplicated_records')} duplicated records have no "
            f"copy in the files of failed blocks ({duplicates.get('failed_block_records_count')}"
            f" records of {duplicates.get('failed_block_requests_count')} nacked requests)")
    outside = duplicates.get("duplicated_outside_resent_requests_records")
    return outside == 0, (
        f"{outside} duplicated records outside the {duplicates.get('resent_requests_count')} "
        "resent requests")


class StoreLister:
    """Lists the store directly every second and keeps each object's first sighting.

    The listing bypasses NGINX and Toxiproxy, so it sees what the store
    holds; a listing that fails while the store is stopped is counted.
    """

    def __init__(self, store, period_s=FAULT_LISTING_PERIOD_S):
        self.store = store
        self.period_s = period_s
        self.first_listed = {}
        self.errors = 0
        self.listings = 0
        self._stop = threading.Event()
        self._thread = None

    def list_once(self) -> dict:
        """Every object under the exporter prefix, with size, ETag and LastModified."""
        found = {}
        for page in self.store.client.get_paginator("list_objects_v2").paginate(
                Bucket=self.store.bucket, Prefix="otel/"):
            for item in page.get("Contents", []):
                found[item["Key"]] = {
                    "key": item["Key"], "size_bytes": int(item["Size"]),
                    "etag": item.get("ETag"),
                    "last_modified_unix_s": item["LastModified"].timestamp(),
                }
        return found

    def _run(self):
        while not self._stop.is_set():
            try:
                listed = self.list_once()
                self.listings += 1
                now = time.time()
                for key in listed:
                    self.first_listed.setdefault(key, now)
            except Exception:  # noqa: BLE001 - a stopped store refuses; counted
                self.errors += 1
            _ = self._stop.wait(self.period_s)

    def start(self):
        """Start listing in a thread of its own."""
        self._thread = threading.Thread(target=self._run, name="store-lister", daemon=True)
        self._thread.start()
        return self

    def stop(self):
        """Stop listing."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)

    def objects(self) -> list:
        """The final listing, each object with its first sighting."""
        return [dict(item, first_listed_unix_s=self.first_listed.get(key))
                for key, item in sorted(self.list_once().items())]


# `orphan_verdict`'s cleanup argument for a case that leaves cleanup to the bucket.
ORPHANS_NOT_CLEANED = "not cleaned"


def orphan_verdict(orphans, expected, abort_failures, cleanup=ORPHANS_NOT_CLEANED) -> tuple:
    """Whether the incomplete uploads after a case are exactly the expected ones.

    `expected` maps each upload a killed engine left open to its key; every
    one must still be listed under that key. Any other upload is allowed
    only up to `abort_failures`, the aborts the exporter reported as failed.
    A case that aborts the uploads itself (`abort_uploads`) must leave none.
    """
    if not isinstance(orphans, list):
        return False, f"the uploads could not be listed: {orphans}"
    listed = {entry["upload_id"]: entry["key"] for entry in orphans}
    missing = sorted(upload for upload, key in expected.items() if listed.get(upload) != key)
    unknown = [entry for entry in orphans if entry["upload_id"] not in expected]
    unexpected = max(0, len(unknown) - abort_failures)
    cleaned = cleanup == ORPHANS_NOT_CLEANED or bool((cleanup or {}).get("clean"))
    return (not missing and unexpected == 0 and cleaned,
            f"{len(orphans)} incomplete multipart uploads; {len(expected)} left open by a killed "
            f"engine, of which not listed under their key {missing[:5]}; {abort_failures} "
            f"reported abort failures may each leave one; unexpected {unexpected}: "
            f"{unknown[:5]}; cleanup "
            f"{cleanup if cleanup == ORPHANS_NOT_CLEANED else (cleanup or {}).get('remaining')}")


def orphaned_uploads(store) -> list:
    """The bucket's incomplete multipart uploads, read from the store directly."""
    found = []
    for page in store.client.get_paginator("list_multipart_uploads").paginate(
            Bucket=store.bucket):
        for upload in page.get("Uploads", []) or []:
            found.append({"key": upload["Key"], "upload_id": upload["UploadId"],
                          "initiated_unix_s": upload["Initiated"].timestamp()})
    return found


def failure_spec(family, fault, topology, store, *, ordinal=1, cores=None) -> measurement.RunSpec:
    """The immutable inputs of one matrix cell."""
    if fault not in FAILURE_FAMILIES.get(family, ()):
        raise AssertionError(f"family {family} has no fault {fault}: {FAILURE_FAMILIES}")
    command = capacity._command()
    cores = tuple(cores or command.default_engine_cores(1))
    case = f"failure-{family}-{fault}"
    return measurement.RunSpec(
        run_id=measurement.RunSpec.build_run_id(case, topology, store, cores,
                                                FAULT_INTERVAL_S, int(ordinal)),
        case=case, topology=topology, store=store, cores=cores, workload=FAULT_WORKLOAD,
        interval_s=FAULT_INTERVAL_S,
        duration_s=FAULT_MAX_REQUESTS // FAULT_RATE_REQUESTS_PER_S,
        producer_timeout_s=FAULT_PRODUCER_TIMEOUT_S, max_in_flight=128,
        overrides={
            "rate_requests_per_s": FAULT_RATE_REQUESTS_PER_S,
            "retry": shipped_s3_retry(), "exporter": FAULT_EXPORTER_MERGE,
            "fault": fault, "producer_attempts": FAULT_PRODUCER_ATTEMPTS,
            "baseline_s": FAULT_BASELINE_S, "after_s": FAULT_AFTER_S,
            "observe_deadline_s": FAULT_OBSERVE_DEADLINE_S[fault],
            "recovery_deadline_s": RECOVERY_DEADLINE_S,
        },
    )


class FaultCase:
    """One fault case as an observable state machine.

    Every state is recorded with its monotonic and wall-clock instant and
    the evidence that moved the case into it: `input_started`; `baseline`,
    a completed values object HEADed in the store, a nonempty ACTIVE block
    and live input; `armed`; `observed`, the fault's intended condition
    (`FAULT_CONDITIONS`); `fault_removed`; `endpoint_healthy`, a signed HEAD
    through the route answered; `resumed`, a values file written and a
    request acknowledged after the removal; `input_stopped`; `drained`.
    """

    def __init__(self, spec, rig, store, engine, phase, ledger, lister, settings):
        self.spec = spec
        self.rig = rig
        self.store = store
        self.engine = engine
        self.phase = phase
        self.ledger = ledger
        self.lister = lister
        self.settings = settings
        self.buffered = spec.topology == "buffered"
        self.fault = spec.overrides["fault"]
        self.flush_deadline_s = duration_s(settings["window"]["flush_retry_deadline"])
        self.states = []
        self.problems = []
        self.log = LogTail(engine.log.name) if engine is not None else None
        self.failures = []

    def transition(self, state, evidence=None):
        """Enter `state`, recording when and why."""
        self.states.append({"state": state, "monotonic_ns": time.monotonic_ns(),
                            "unix_s": time.time(), "evidence": evidence or {}})
        sys.stderr.write(f"{self.spec.run_id}: {state}\n")

    def at(self, state):
        """The record of `state`, or None when the case never reached it."""
        return next((entry for entry in self.states if entry["state"] == state), None)

    def samples_since(self, monotonic_ns):
        """The sampler's samples taken at or after `monotonic_ns`."""
        return [sample for sample in list(self.phase.sampler.samples)
                if sample["monotonic_ns"] >= monotonic_ns]

    def totals(self):
        """The latest sample's exporter totals."""
        samples = list(self.phase.sampler.samples)
        return flat_totals(samples[-1]) if samples else {}

    def route_requests(self):
        """The exporter's requests NGINX logged so far."""
        return engine_requests(parse_access_log(self.rig.artifact_dir / ACCESS_LOG),
                               self.store.bucket)

    def delta(self, name, since_state):
        """How much a total grew since `since_state` was entered."""
        base = (self.at(since_state) or {}).get("evidence", {}).get("totals", {})
        return self.totals().get(name, 0) - base.get(name, 0)

    def retry_evidence(self, since_ns):
        """The topology's evidence that a storage nack was retried.

        Strict: the producer received the exporter's storage sentence as a
        retryable refusal, and resends the same bytes. Buffered: the buffer
        scheduled a retry of a nacked bundle.
        """
        if self.buffered:
            scheduled = self.delta("buffer.retries.scheduled", "armed")
            return {"buffer_retries_scheduled_count": scheduled, "retried": scheduled > 0}
        attempts = ledger_attempts(self.ledger, since_ns)
        return {"producer_storage_nacks_count": attempts["storage_nacks_count"],
                "producer_local_timeouts_count": attempts["local_timeouts_count"],
                "retried": attempts["storage_nacks_count"] > 0}

    def observe_condition(self):
        """What the armed fault has shown so far."""
        armed = self.at("armed")
        requests = [entry for entry in self.route_requests()
                    if _within(entry, armed["unix_s"], None)]
        observed = {
            "elapsed_s": (time.monotonic_ns() - armed["monotonic_ns"]) / 1e9,
            "flush_retries_count": self.delta("flush.retries", "armed"),
            "flush_failures_count": self.delta("flush.failures.by_class", "armed"),
            "storage_nacks_count": self.delta("nacks.storage", "armed"),
            "http_503_writes_count": sum(1 for entry in requests if entry.get("status") == "503"
                                         and entry.get("method") in ("PUT", "POST")),
        }
        if self.fault == "store_outage":
            if self.log is not None:
                self.failures.extend(flush_failures(self.log.lines()))
            qualifying = [entry for entry in self.failures
                          if entry["error_type"] == "deadline"
                          and entry["unix_s"] >= armed["unix_s"] + self.flush_deadline_s]
            observed["deadline_failures_after_deadline"] = [
                round(entry["unix_s"] - armed["unix_s"], 3) for entry in qualifying]
            # A deadline failure is logged when its block's deadline expires,
            # which for a block that waited for the flush slot is later than its
            # window's end; the fault is held a margin past it.
            observed["hold_until_s"] = min(
                (round(entry["unix_s"] + OUTAGE_HOLD_MARGIN_S - armed["unix_s"], 3)
                 for entry in qualifying), default=None)
        if self.fault == "slow":
            totals = [flat_totals(sample) for sample in self.samples_since(armed["monotonic_ns"])]
            # Only requests that started under the fault show its delay.
            started = [entry for entry in requests
                       if float(entry["msec"]) - (_float(entry.get("request_time")) or 0)
                       >= armed["unix_s"]]
            observed["delayed_requests_count"] = sum(
                1 for entry in started
                if (_float(entry.get("request_time")) or 0) >= SLOW_DELAY_FLOOR_S)
            observed["throttled_uploads_count"] = sum(
                1 for entry in started
                if entry["operation"] in ("put_object", "upload_part")
                and (_float(entry.get("request_length")) or 0) >= PROBE_TRANSFER_BYTES
                and (_float(entry.get("request_length")) or 0)
                / max(_float(entry.get("request_time")) or 0, 1e-3)
                <= 2 * SLOW_RATE_KBPS * 1000)
            observed["flushing_with_work_samples_count"] = sum(
                1 for entry in totals if entry.get("block.flushing", 0) > 0 and (
                    entry.get("block.active", 0) > 0 or entry.get("block.pending", 0) > 0
                    or entry.get("block.requests_pending", 0) > 0))
            observed["admission_closed_s"] = self.delta("admission.closed.duration", "armed")
            observed["receiver_rejections_count"] = self.delta("rejected", "armed")
            in_flight = [entry.get("buffer.in.flight", 0) for entry in totals[-12:]]
            observed["buffer_in_flight_plateau"] = bool(
                self.buffered and len(in_flight) == 12 and min(in_flight) > 0
                and max(in_flight) == min(in_flight)
                and totals[-1].get("admission.closed", 0) > 0)
        else:
            observed.update(self.retry_evidence(armed["monotonic_ns"]))
        return observed

    def await_baseline(self):
        """A durable baseline and live input, before any fault is armed."""
        started = self.at("input_started")["monotonic_ns"]

        def observe():
            values = sorted(key for key in self.lister.first_listed if "dataset=values/" in key)
            head = None
            if values:
                try:
                    head = self.store.client.head_object(
                        Bucket=self.store.bucket, Key=values[0]).get("ContentLength")
                except ClientError:
                    head = None
            acks = ledger_acks(self.ledger)
            totals = self.totals()
            return {
                "input_s": (time.monotonic_ns() - started) / 1e9,
                "values_objects_count": len(values), "headed_values_bytes": head,
                "block_active_bytes": totals.get("block.active", 0),
                "requests_acked_count": len(acks),
                "last_ack_age_s": (time.monotonic_ns() - acks[-1]) / 1e9 if acks else None,
                "totals": totals,
            }

        evidence = measurement.wait_until(
            observe, lambda seen: seen["input_s"] >= FAULT_BASELINE_S
            and bool(seen["headed_values_bytes"]) and seen["block_active_bytes"] > 0
            and seen["last_ack_age_s"] is not None
            and seen["last_ack_age_s"] < 2 * FAULT_INTERVAL_S,
            deadline_ns=started + 6 * FAULT_BASELINE_S * 10**9,
            description="a HEADed values object, a nonempty ACTIVE block and live input",
        )
        self.transition("baseline", evidence)

    def arm(self, parameters=None):
        """Activate the fault, recording the totals it is measured against."""
        totals = self.totals()
        self.rig.activate(self.fault, parameters or {})
        self.transition("armed", {"totals": totals, "activation": self.rig.activations[-1]})

    def await_condition(self):
        """Hold the fault until its intended condition is observed."""
        armed = self.at("armed")
        evidence = measurement.wait_until(
            self.observe_condition, lambda seen: FAULT_CONDITIONS[self.fault](self, seen),
            deadline_ns=armed["monotonic_ns"] + FAULT_OBSERVE_DEADLINE_S[self.fault] * 10**9,
            description=f"the intended condition of {self.fault}",
        )
        evidence["totals"] = self.totals()
        self.transition("observed", evidence)

    def remove_fault(self, controls, store_cores):
        """Remove the fault, then wait for a signed HEAD through the route."""
        self.rig.recover()
        self.transition("fault_removed", {"totals": self.totals(),
                                          "recovery": self.rig.activations[-1].get("recovery")})
        if self.fault == "store_outage":
            pid = performance.container_pid(self.store.container)
            if pid:
                controls.register("store", pid, store_cores)
        client = self.rig.route_client(read_timeout=10)
        removed = self.at("fault_removed")

        def observe():
            try:
                status = client.head_bucket(Bucket=self.store.bucket)["ResponseMetadata"][
                    "HTTPStatusCode"]
            except (ClientError, BotoCoreError) as error:
                status = type(error).__name__
            return {"head_bucket_status": status,
                    "elapsed_s": (time.monotonic_ns() - removed["monotonic_ns"]) / 1e9}

        evidence = measurement.wait_until(
            observe, lambda seen: seen["head_bucket_status"] == 200,
            deadline_ns=removed["monotonic_ns"] + ENDPOINT_HEALTH_DEADLINE_S * 10**9,
            description="a signed HEAD of the bucket through the route",
        )
        self.transition("endpoint_healthy", evidence)

    def recovery_deadline_ns(self):
        """The fixed total deadline of everything after endpoint health."""
        return self.at("endpoint_healthy")["monotonic_ns"] + RECOVERY_DEADLINE_S * 10**9

    def await_resumed(self):
        """A values file written and a request acknowledged after the removal."""
        removed = self.at("fault_removed")

        def observe():
            acks = [ack for ack in ledger_acks(self.ledger) if ack >= removed["monotonic_ns"]]
            return {"values_files_written_count": self.delta("files.written.values",
                                                               "fault_removed"),
                    "exporter_acks_count": self.delta("acks", "fault_removed"),
                    "producer_acks_count": len(acks)}

        evidence = measurement.wait_until(
            observe, lambda seen: seen["values_files_written_count"] > 0
            and seen["exporter_acks_count"] > 0 and seen["producer_acks_count"] > 0,
            deadline_ns=self.recovery_deadline_ns(),
            description="a values file and an acknowledgement after the fault's removal",
        )
        self.transition("resumed", evidence)

    def await_after_input(self):
        """FAULT_AFTER_S worth of requests acknowledged after the exporter resumed."""
        resumed = self.at("resumed")
        wanted = FAULT_AFTER_S * FAULT_RATE_REQUESTS_PER_S

        def observe():
            acks = [ack for ack in ledger_acks(self.ledger) if ack >= resumed["monotonic_ns"]]
            return {"requests_acked_since_resumed_count": len(acks)}

        return measurement.wait_until(
            observe, lambda seen: seen["requests_acked_since_resumed_count"] >= wanted,
            deadline_ns=self.recovery_deadline_ns(),
            description=f"{wanted} requests acknowledged after the exporter resumed",
        )

    def run(self, controls, store_cores, result, options):
        """The fault sequence after the baseline: arm, observe, remove, resume."""
        arm_at = options.get("arm_at_unix_s")
        if arm_at is not None:
            sys.stderr.write(f"{self.spec.run_id}: arming at {arm_at} (unix s)\n")
            _ = await_wall_clock(arm_at, "the straddling fault's arming instant")
            result["config"]["straddle"]["armed_before_hour_end"] = \
                time.time() < arm_at + STRADDLE_ARM_BEFORE_END_S
        self.arm(options.get("fault_parameters"))
        self.attempt(self.await_condition)
        # The fault is removed whether or not it showed its condition.
        if self.attempt(self.remove_fault, controls, store_cores) \
                and self.attempt(self.await_resumed):
            self.attempt(self.await_after_input)

    def span(self, first, last):
        """Seconds from entering state `first` to entering `last`, or None."""
        a, b = self.at(first), self.at(last)
        return (b["monotonic_ns"] - a["monotonic_ns"]) / 1e9 if a and b else None

    def fault_window(self):
        """The wall-clock interval the fault was in place, or None."""
        armed, removed = self.at("armed"), self.at("fault_removed")
        return (armed["unix_s"], removed and removed["unix_s"]) if armed else None

    def throughput_edges(self):
        """The states that bound the before, during and after throughput phases."""
        return {"before": ("input_started", "armed"), "during": ("armed", "endpoint_healthy"),
                "after": ("endpoint_healthy", "input_stopped")}

    def all_samples(self):
        """Every sample of the case's engine lifetimes."""
        return list(self.phase.sampler.samples)

    def lifetime_samples(self, phase):
        """The samples of one engine lifetime that describe the running engine."""
        return list(phase.sampler.samples)

    def lifetime_summary(self, phase):
        """Replacements for one lifetime's summary fields: none."""
        return {}

    def abort_failures(self, final):
        """The multipart aborts the exporter reported as failed."""
        return int(final.get("flush.abort_failures", 0))

    def observations(self, record):
        """The case's own additions to `observations.fault`: none."""
        return {}

    def expected_orphans(self):
        """Upload id to key of every upload this case is known to orphan: none."""
        return {}

    def explain_duplicates(self, duplicates, record):
        """Whether every stored duplicate is attributed (`duplicates_explained`)."""
        return duplicates_explained(self.buffered, duplicates)

    def verdicts(self, record):
        """The case's own hard checks: its fault's condition and the recovery."""
        observed = self.at("observed")
        recovered_s = self.span("endpoint_healthy", "drained")
        return [
            ("fault_observed", observed is not None,
             json.dumps({key: value for key, value in observed["evidence"].items()
                         if key != "totals"}) if observed
             else "; ".join(self.problems) or "the condition was never observed"),
            ("recovered", self.at("resumed") is not None and recovered_s is not None
             and recovered_s <= RECOVERY_DEADLINE_S,
             f"endpoint healthy to drained {recovered_s} s against {RECOVERY_DEADLINE_S} s; "
             f"resumed {bool(self.at('resumed'))}; problems {self.problems}"),
        ]

    def numbers(self, record, during):
        """The case's own recorded durations and counts."""
        return {
            "http_503_responses_count": (sum(
                count for statuses in during["status_by_operation"].values()
                for status, count in statuses.items() if status == "503") if during else None),
            "fault_duration_s": self.span("armed", "fault_removed"),
            "time_to_condition_s": self.span("armed", "observed"),
            "recovery_s": self.span("endpoint_healthy", "resumed"),
            "endpoint_healthy_to_drained_s": self.span("endpoint_healthy", "drained"),
            "drain_s": self.span("input_stopped", "drained"),
        }

    def attempt(self, step, *args):
        """Run one step; a failure is recorded and ends the fault sequence."""
        try:
            step(*args)
            return True
        except AssertionError as error:
            self.problems.append(f"{step.__name__}: {error}"[:2000])
            sys.stderr.write(f"{self.spec.run_id}: {step.__name__} failed: {error}\n"[:2000])
            return False


def _slow_met(case, seen) -> bool:
    """Slow: a delayed response and a throttled upload that both started under
    the fault, a flush with work behind it, and backpressure: the exporter
    closed admission for at least a window, the receiver refused, or the
    buffer's in-flight bundles held still while admission was closed."""
    backpressure = (seen["admission_closed_s"] >= FAULT_INTERVAL_S
                    or seen["receiver_rejections_count"] > 0
                    or seen["buffer_in_flight_plateau"])
    return (seen["delayed_requests_count"] > 0 and seen["throttled_uploads_count"] > 0
            and seen["flushing_with_work_samples_count"] > 0 and backpressure)


def _http503_met(case, seen) -> bool:
    """503: a write answered 503, the exporter retried and nacked, and the nack was retried."""
    return (seen["http_503_writes_count"] > 0 and seen["flush_retries_count"] > 0
            and seen["storage_nacks_count"] > 0 and seen["retried"])


def _outage_met(case, seen) -> bool:
    """Outage: a deadline-class flush failure logged at least the flush deadline
    after the store stopped, held OUTAGE_HOLD_MARGIN_S past that block's own
    deadline, a storage nack, and its retry."""
    return (bool(seen["deadline_failures_after_deadline"])
            and seen.get("hold_until_s") is not None
            and seen["elapsed_s"] >= seen["hold_until_s"]
            and seen["flush_failures_count"] > 0
            and seen["storage_nacks_count"] > 0 and seen["retried"])


FAULT_CONDITIONS = {"slow": _slow_met, "http503": _http503_met, "store_outage": _outage_met}


def phase_throughput(acks_ns, objects, case) -> dict:
    """Acknowledged records and object bytes per second before, during and after the fault."""
    edges = case.throughput_edges()
    rows = case.spec.workload.records_per_request
    report = {}
    for name, (first_state, last_state) in edges.items():
        first, last = case.at(first_state), case.at(last_state)
        if not first or not last:
            report[name] = None
            continue
        seconds = (last["monotonic_ns"] - first["monotonic_ns"]) / 1e9
        acked = sum(1 for ack in acks_ns if first["monotonic_ns"] <= ack < last["monotonic_ns"])
        written = sum(item["size_bytes"] for item in objects
                      if first["unix_s"] <= (item.get("first_listed_unix_s") or 0) < last["unix_s"])
        report[name] = {"duration_s": seconds,
                        "acked_records_per_s": acked * rows / seconds if seconds > 0 else None,
                        "object_bytes_per_s": written / seconds if seconds > 0 else None}
    return report


def resource_bounds(samples, settings, buffered, baseline_oldest_s) -> dict:
    """The retained-state invariants over every sample of one case.

    ACTIVE and FLUSHING within `window.max_block_bytes`, the series cache
    within its capacity, at most one pending slot, accounted memory within
    the budget, the buffer within its cap with nothing lost, and the oldest
    unacknowledged age back at its baseline once drained.
    """
    block_limit = soak_byte_size(settings["window"]["max_block_bytes"])
    cache_limit = int(settings["series_cache"]["max_entries"])
    problems = set()
    peaks = collections.Counter()
    for sample in samples:
        totals = flat_totals(sample)
        for name in ("block.active", "block.flushing", "block.pending", "memory.accounted",
                     "series_cache.entries", "block.pending_slot_occupied",
                     "buffer.storage.bytes.used", "oldest_unacked.age", "admission.closed"):
            peaks[name] = max(peaks[name], totals.get(name, 0))
        peaks["process_rss_bytes"] = max(peaks["process_rss_bytes"],
                                         sample.get("process_rss_bytes") or 0)
        if totals.get("memory.accounted", 0) > totals.get("memory.budget", float("inf")):
            problems.add("accounted over budget")
        for name in ("block.active", "block.flushing"):
            if totals.get(name, 0) > block_limit:
                problems.add(f"{name} over {block_limit} bytes")
        if totals.get("series_cache.entries", 0) > cache_limit:
            problems.add("series cache over its capacity")
        if totals.get("block.pending_slot_occupied", 0) > 1:
            problems.add("more than one pending slot")
        cap = totals.get("buffer.storage.bytes.cap")
        if buffered and cap and totals.get("buffer.storage.bytes.used", 0) > cap:
            problems.add("buffer over its size cap")
    final = flat_totals(samples[-1]) if samples else {}
    losses = {name: value for name, value in final.items()
              if name.startswith("buffer.loss.items") and value}
    if losses:
        problems.add(f"buffer loss {losses}")
    if final.get("buffer.bundles.resolved.permanently_rejected"):
        problems.add("bundles permanently rejected")
    if final.get("buffer.ingest.failures"):
        problems.add("buffer ingest failures")
    oldest = final.get("oldest_unacked.age")
    if oldest is None or oldest > max(baseline_oldest_s, 0.0) + FAULT_INTERVAL_S:
        problems.add(f"oldest unacknowledged age {oldest} s did not return to its baseline "
                     f"{baseline_oldest_s} s")
    return {"problems": sorted(problems), "peaks": dict(peaks),
            "block_limit_bytes": block_limit, "series_cache_limit_entries": cache_limit,
            "baseline_oldest_unacked_age_s": baseline_oldest_s,
            "final_oldest_unacked_age_s": oldest}


def straddle_arm_instant(now_unix_s) -> float:
    """When a straddling cell arms: STRADDLE_ARM_BEFORE_END_S before the first
    hour end that leaves at least STRADDLE_LEAD_S to start the cell."""
    end = (int(now_unix_s + STRADDLE_LEAD_S + STRADDLE_ARM_BEFORE_END_S) // 3600 + 1) * 3600
    return end - STRADDLE_ARM_BEFORE_END_S


def await_wall_clock(instant_unix_s, description):
    """Wait until the wall clock reaches `instant_unix_s`, observing it."""
    return measurement.wait_until(
        time.time, lambda now: now >= instant_unix_s,
        deadline_ns=time.monotonic_ns() + int((instant_unix_s - time.time() + 60) * 1e9),
        description=description,
    )


def failure_prerequisites(store) -> dict:
    """Docker, both fault images and the store image, before any lease or traffic.

    A missing one skips an optional lane and fails a required one
    (`tools_unavailable`, `test_e2e.require_docker_image`).
    """
    images = require_fault_images()
    images["store"] = test_e2e.require_docker_image(store)
    return images


def launch_engine(root, rig, spec, output_dir, binary):
    """One containerized release engine of a fault case, in the rig's namespace.

    The launch options are kept on the engine, so `restart_engine` starts
    its successor exactly alike.
    """
    options = {
        "storage": rig.storage, "launcher": rig.launcher,
        "overrides": {"retry": spec.overrides["retry"]},
        "interval": f"{spec.interval_s}s", "topology": spec.topology,
        "buffer_path": Path(output_dir) / "buffer" if spec.topology == "buffered" else None,
        "cores": list(spec.cores), "merge": {"exporter": spec.overrides["exporter"]},
        "binary": Path(binary),
    }
    Path(root).mkdir(parents=True, exist_ok=True)
    with capacity.malloc_conf(capacity.JEMALLOC_STATS_CONF):
        engine = test_e2e.Engine(root, **options)
    engine.launch_options = options
    return engine


def failure_experiment(spec, result, output_dir, controls, *, provenance, options):
    """One fault case: baseline, fault, recovery, drain, read-back and verdicts.

    The case class of the family (`FaultCase` or `ProcessCase`) runs
    everything between the baseline and the end of input.
    """
    command = capacity._command()
    output_dir = Path(output_dir)
    buffered = spec.topology == "buffered"
    process = spec.overrides["fault"] in FAILURE_FAMILIES["process"]
    topology = measurement.core_topology()
    allocation = measurement.role_allocation(
        topology["sibling_groups"], sorted(os.sched_getaffinity(0)), spec.cores,
        roles=measurement.CASE_ROLES["faults"],
    )
    groups = capacity.producer_placement(options.get("producer_cpus", capacity.PRODUCER_CPUS),
                                         topology["sibling_groups"], allocation)
    if groups:
        allocation["producer"] = sorted(core for group in groups for core in group)
    controls.allocate(allocation)
    controls.register("harness", os.getpid())
    result["environment"]["build"] = provenance["build"]
    result["environment"]["git"] = provenance["git"]
    arm_at = options.get("arm_at_unix_s")
    if arm_at is not None:
        result["config"]["straddle"] = {"arm_at_unix_s": arm_at,
                                        "arm_before_hour_end_s": STRADDLE_ARM_BEFORE_END_S}
    ledger = measurement.Ledger(output_dir / "ledger.sqlite")
    record = {}
    engine = phase = case = lister = channel = None
    engines = []
    local = output_dir / "store"
    with test_e2e.DockerStore(spec.store, by_image_id=True) as store:
        store_cores = allocation.get("store", [])
        pinned = performance.pin_container(store.container, store_cores)
        store_pid = performance.container_pid(store.container)
        if store_pid and store_cores:
            controls.register("store", store_pid, store_cores)
        result["ephemeral_values"] = {"<store_endpoint>": store.endpoint}
        rig = FaultRig(store, output_dir / "rig", cores=allocation.get("fault_tools") or None)
        with rig:
            engine = launch_engine(output_dir / "engine-1", rig, spec, output_dir,
                                   provenance["build"]["binary"])
            engines.append(engine)
            try:
                settings = exporter_settings(engine.config)
                result["config"]["effective"] = engine.config
                result["config"]["effective_sha256"] = engine.config_sha256
                result["config"]["edges"] = [list(edge) for edge in engine.edges]
                result["config"]["malloc_conf"] = capacity.JEMALLOC_STATS_CONF
                result["config"]["store_pinned"] = pinned
                result["ephemeral_values"]["<receiver_listening_addr>"] = \
                    f"127.0.0.1:{engine.grpc_port}"
                command.record_graph(result, engine, spec.topology)
                phase = capacity.CapacityPhase(command, "engine-1", engine, spec, controls,
                                               buffered)
                phase.ready("start")
                lister = StoreLister(store).start()
                # A process case's producer outlives each engine: its channel
                # reconnects to the next engine on the launcher's same port.
                channel = restart_channel(engine) if process else engine.channel
                producer = measurement.Producer(
                    channel, ledger, spec.workload, cores=allocation.get("producer", []),
                    timeout_s=spec.producer_timeout_s, max_in_flight=spec.max_in_flight,
                    retry_attempts=FAULT_PRODUCER_ATTEMPTS,
                )
                if process:
                    case = ProcessCase(spec, rig, store, engine, phase, ledger, lister, settings,
                                       controls=controls, engines=engines)
                else:
                    case = FaultCase(spec, rig, store, engine, phase, ledger, lister, settings)
                _ = command.await_window_start(spec.interval_s)
                controls.raise_if_invalid()
                stop = threading.Event()
                sent = {}

                def produce():
                    sent["outcome"] = producer.send_paced(
                        range(spec.workload.requests), FAULT_RATE_REQUESTS_PER_S, stop=stop)

                sender = threading.Thread(target=produce, name="fault-producer")
                case.transition("input_started")
                sender.start()
                try:
                    if case.attempt(case.await_baseline):
                        case.run(controls, store_cores, result, options)
                finally:
                    if rig.active:
                        rig.recover()
                    stop.set()
                    sender.join()
                case.transition("input_stopped", {"outcome": sent.get("outcome")})
                if "outcome" not in sent:
                    raise AssertionError("the producer did not finish")
                engine, phase = case.engine, case.phase
                phase.inputs.append(sent["outcome"])
                controls.raise_if_invalid()
                if case.attempt(phase.drained):
                    case.transition("drained", {"drain": phase.drain})
                record["final_totals"] = flat_totals(phase.sampler.samples[-1])
                _ = controls.snapshot(
                    "end", {"engine": (engine.pid, list(spec.cores))},
                    workers=phase.workers, requested_cores=list(spec.cores),
                )
                controls.unwatch_workers()
                if process:
                    record["final_stop"] = stop_engine(engine, case.drain_deadline_s)
                else:
                    engine.shutdown(command.SHUTDOWN_DEADLINE_S)
                command.record_event(result, "engine_shut_down", str(engine.pid))
            finally:
                controls.unwatch_workers()
                for lifetime in (case.phases if process and case is not None else [phase]):
                    if lifetime is not None and lifetime.sampler is not None:
                        lifetime.sampler.stop()
                if lister is not None:
                    lister.stop()
                for launched in engines:
                    launched.close()
                if process and channel is not None:
                    channel.close()
            record["residual_state"] = rig.residual_state()
            record["requests"] = case.route_requests()
            record["events"] = engine_events("\n".join(
                Path(launched.log.name).read_text(errors="replace") for launched in engines))
        record["rig"] = rig.evidence()
        record["rig_cleanup_clean"] = bool((rig.cleanup_report or {}).get("clean"))
        record["objects"] = lister.objects()
        record["listing"] = {"listings_count": lister.listings, "errors_count": lister.errors}
        try:
            record["orphans"] = orphaned_uploads(store)
        except (ClientError, BotoCoreError) as error:
            record["orphans"] = f"{type(error).__name__}: {error}"
        if process:
            # Only once the evidence above is kept does the test abort what was left.
            record["orphan_cleanup"] = abort_uploads(store, record["orphans"])
        store.download(local)
    phases = case.phases if process else [phase]
    summaries, heaps, samples = [], [], []
    for lifetime in phases:
        lifetime_samples = case.lifetime_samples(lifetime)
        summary = lifetime.summary()
        summary.update(case.lifetime_summary(lifetime))
        residuals, heap = capacity.trial_residuals(lifetime_samples, lifetime.capacity_idle,
                                                   getattr(lifetime.sampler, "pairs", ()))
        summary["residuals"] = residuals
        summaries.append(summary)
        heaps.append(heap)
        samples.extend(lifetime_samples)
    result["observations"] = {
        "phases": summaries,
        "residual_excursions": measurement.residual_excursions(
            [residual for summary in summaries for residual in summary["residuals"]],
            [pair for lifetime in phases for pair in getattr(lifetime.sampler, "pairs", ())],
            max((s["process_rss_bytes"] for s in samples), default=0)),
        "rss_heap_term": heaps[0] if len(heaps) == 1 else heaps,
    }
    result["samples"] = [dict(measurement.compact_sample(s), extras=s.get("extras"))
                         for s in samples[::capacity.PUBLISHED_SAMPLE_STRIDE * 4]]
    oracle_error = None
    record["replay"] = None
    try:
        oracle = measurement.run_pinned(
            performance.oracle_cores(allocation, topology["sibling_groups"])
            or sorted(os.sched_getaffinity(0)),
            measurement.read_oracle, local, ledger, require_all=True, healthy=False,
            workload=spec.workload,
        )
        acked_scope = measurement._compare(ledger, require_all=False, healthy=False)
        failed_ids = failed_block_record_ids(local, spec.workload,
                                             record["events"]["failed_files"])
        record["duplicates"] = duplicate_attribution(ledger, failed_ids)
        if process:
            record["replay"] = replay_analysis(
                ledger, values_rows_by_file(local, spec.workload), case.replay_events(),
                {item["key"]: item.get("etag") for item in record["objects"]}, failed_ids,
                buffered=buffered)
    except AssertionError as error:
        oracle_error = str(error)[:2000]
        oracle = {"passed": False, "problems": [oracle_error], "readers": {},
                  "multiplicity_histogram": {}, "missing_record_count": None,
                  "unexpected_record_count": None, "corrupt_record_count": None}
        acked_scope = {"missing_record_count": None}
        record["duplicates"] = None
    shutil.rmtree(local, ignore_errors=True)
    counts = ledger.counts()
    latencies = ledger.acknowledgement_latencies_s()
    record["acks_ns"] = ledger_acks(ledger)
    record["attempts"] = ledger_attempts(ledger)
    record["ledger_outcomes"] = counts["attempts_by_outcome"]
    record["producer_finished"] = True
    if process:
        record["windows"] = case.producer_windows()
    ledger.close()
    sent_spec = dataclasses.replace(spec, workload=dataclasses.replace(
        spec.workload, requests=counts["requests_attempted_count"]))
    # The producer stops on the case's own schedule, so what it sent is what it intended.
    command.settle_local_result(result, sent_spec, phases, oracle, counts, latencies,
                                output_dir)
    settle_fault(result, case, record, oracle, acked_scope, oracle_error, counts)


def settle_fault(result, case, record, oracle, acked_scope, oracle_error, counts):
    """The fault checks, metrics and observations beside the common ones.

    The case contributes its own verdicts and numbers (`FaultCase.verdicts`,
    `FaultCase.numbers`): the first two verdicts are `fault_observed` and
    `recovered`, any further ones follow the common checks.
    """
    checks = result["checks"]
    metrics = result["metrics"]
    observations = result["observations"]
    samples = case.all_samples()
    final = record.get("final_totals") or {}

    def hard(name, passed, detail):
        checks.append(measurement.check(
            name, measurement.CHECK_HARD,
            measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED,
            str(detail)[:1500]))

    verdicts = case.verdicts(record)
    for name, passed, detail in verdicts[:2]:
        hard(name, passed, detail)
    drain = case.phase.drain or {}
    hard("drained", bool(drain.get("drained")), f"drain {drain}")
    missing_acked = acked_scope.get("missing_record_count")
    all_acked = counts["requests_acked_count"] == counts["requests_attempted_count"]
    hard("at_least_once", oracle.get("passed") is True and missing_acked == 0 and all_acked,
         f"missing acked {missing_acked}; missing sent {oracle.get('missing_record_count')}; "
         f"unexpected {oracle.get('unexpected_record_count')}; corrupt "
         f"{oracle.get('corrupt_record_count')}; acked {counts['requests_acked_count']}/"
         f"{counts['requests_attempted_count']} requests; histogram "
         f"{oracle.get('multiplicity_histogram')}")
    readers_disagree = bool(oracle_error and "reader disagreement" in oracle_error)
    hard("descriptor_coverage", oracle_error is None or readers_disagree,
         oracle_error or f"{oracle.get('descriptor_identity_count')} descriptor identities "
         "cover every values row in its partition and writer")
    hard("reader_agreement", oracle_error is None and bool(oracle.get("readers")),
         oracle_error or f"DuckDB and clickhouse-local agree: {oracle.get('readers')}")
    baseline = (case.at("baseline") or {}).get("evidence", {}).get("totals", {})
    bounds = resource_bounds(samples, case.settings, case.buffered,
                             baseline.get("oldest_unacked.age", 0.0))
    rss = next((entry for entry in checks if entry["name"] == "rss_reconciliation"), None)
    hard("bounded_resources", not bounds["problems"],
         "; ".join(bounds["problems"]) or f"peaks {bounds['peaks']}; rss reconciliation "
         f"{rss and rss['status']}")
    part_bytes = soak_byte_size(case.settings["upload"]["part_bytes"])
    multipart = multipart_evidence(record["requests"], record["objects"], part_bytes)
    hard("multipart_exercised", multipart["exercised"], json.dumps(
        {key: value for key, value in multipart.items() if key != "example"}))
    abort_failures = case.abort_failures(final)
    orphans = record["orphans"]
    known = case.expected_orphans()
    hard("orphaned_uploads_expected", *orphan_verdict(
        orphans, known, abort_failures, record.get("orphan_cleanup", ORPHANS_NOT_CLEANED)))
    lateness = partition_lateness(record["objects"], lateness_bound_s(case.settings))
    hard("partition_lateness_bound", not lateness["violations"],
         f"bound {lateness['bound_s']} s; violations {lateness['violations']}; hours "
         f"{lateness['hours']}")
    duplicates = record.get("duplicates") or {}
    explained, why = case.explain_duplicates(duplicates, record)
    hard("duplicates_explained", explained, why)
    hard("fault_rig_clean", record["residual_state"].get("clean") and record["rig_cleanup_clean"],
         f"residual {record['residual_state'].get('clean')}; cleanup "
         f"{record['rig_cleanup_clean']}")
    for name, passed, detail in verdicts[2:]:
        hard(name, passed, detail)
    window = case.fault_window()
    during = route_summary(record["requests"], *window) if window else None
    # Only memory and the correctness counts are compared metrics; every
    # other number depends on where in a window the fault landed.
    for name in list(metrics):
        if name not in FAULT_DIRECTIONS:
            observations[name] = metrics.pop(name)
    accounted = [capacity.extras_total(s, "memory.accounted") for s in samples]
    buffer_used = [flat_totals(s).get("buffer.storage.bytes.used", 0) for s in samples]
    metrics["accounted_peak_bytes"] = max(accounted) if accounted else None
    metrics["missing_acked_records"] = missing_acked
    if case.buffered:
        metrics["buffer_storage_peak_bytes"] = max(buffer_used) if buffer_used else None
    numbers = {
        "records_offered_records": counts["records_attempted_count"],
        "records_acked_records": counts["records_acked_count"],
        "records_stored_records": oracle.get("actual_row_count"),
        "requests_offered_count": counts["requests_attempted_count"],
        "duplicate_records": duplicates.get("duplicated_records"),
        "duplicate_extra_copies_records": duplicates.get("extra_copies_records"),
        **case.numbers(record, during),
        "flush_retries_count": final.get("flush.retries"),
        "flush_failures_count": final.get("flush.failures.by_class", 0),
        "flush_failures_by_class": {key.split(".", 3)[-1]: value for key, value in final.items()
                                    if key.startswith("flush.failures.by_class.")},
        "flush_abort_failures_count": final.get("flush.abort_failures"),
        "flush_late_commits_count": final.get("flush.late_commits"),
        "exporter_nacks_by_class": {key.split(".", 1)[1]: value for key, value in final.items()
                                    if key.startswith("nacks.") and value},
        "producer_storage_nacks_count": record["attempts"]["storage_nacks_count"],
        "producer_local_timeouts_count": record["attempts"]["local_timeouts_count"],
        "producer_resent_requests_count": duplicates.get("resent_requests_count"),
        "buffer_retries_scheduled_count": (final.get("buffer.retries.scheduled", 0)
                                           if case.buffered else None),
        "orphaned_uploads_count": len(orphans) if isinstance(orphans, list) else None,
        "multipart_uploads_count": multipart["complete_multipart_upload_ok_count"],
        "partition_visibility_max_after_hour_end_s": max(
            (value for entry in lateness["hours"].values()
             for value in (entry["latest_last_modified_after_end_s"],
                           entry["latest_first_listed_after_end_s"]) if value is not None),
            default=None),
        "peak_rss_bytes": metrics.get("peak_rss_bytes"),
        "accounted_peak_bytes": metrics["accounted_peak_bytes"],
        "buffer_storage_peak_bytes": metrics.get("buffer_storage_peak_bytes"),
    }
    for name, value in metrics.items():
        if value is None:
            result["metrics_unavailable"][name] = "not observed in this run"
        else:
            result["metrics_unavailable"].pop(name, None)
    for name in list(result["metrics_unavailable"]):
        if name not in metrics:
            del result["metrics_unavailable"][name]
    result["metric_directions"] = {name: FAULT_DIRECTIONS[name] for name in metrics}
    result["mandatory_metrics"] = sorted(metrics)
    observations["fault"] = {
        "fault": case.fault,
        "numbers": numbers,
        "failed_block_objects": failed_block_objects(record["events"], record["objects"]),
        "states": [{key: value for key, value in state.items() if key != "evidence"}
                   | {"evidence": {k: v for k, v in state["evidence"].items() if k != "totals"}}
                   for state in case.states],
        "problems": case.problems,
        "multiplicity_histogram": oracle.get("multiplicity_histogram"),
        "duplicates": duplicates,
        "producer_attempts": record["attempts"],
        "final_totals": {key: value for key, value in final.items()
                         if key.startswith(("flush.", "nacks", "acks", "buffer.", "files.",
                                            "admission.", "oldest", "rejected"))},
        "route": {
            "whole": route_summary(record["requests"]),
            "during_fault": during,
        },
        "multipart": multipart,
        "orphaned_uploads": orphans,
        "orphaned_uploads_expected_max_count": abort_failures + len(known),
        "partition_lateness": lateness,
        "engine_events": record["events"],
        "throughput_by_phase": phase_throughput(record["acks_ns"], record["objects"], case),
        "resources": bounds,
        "listing": record["listing"],
        "objects_count": len(record["objects"]),
        "recovery_deadline_s": RECOVERY_DEADLINE_S,
        "rig": {key: record["rig"][key] for key in ("run_id", "store", "images",
                                                    "toxiproxy_version", "activations",
                                                    "access_log_requests_count", "cleanup")},
    }
    observations["fault"].update(case.observations(record))
    result["artifacts"].append(dict(
        measurement.file_entry(case.rig.artifact_dir / ACCESS_LOG), kind="access_log",
        retention=str(case.rig.root)))


# --------------------------------------------------------------------------
# Process restart and hard kill
# --------------------------------------------------------------------------

# How long a killed engine may take to exit, and a shut-down one once its
# admin shutdown returned.
KILL_EXIT_DEADLINE_S = 10
SHUTDOWN_EXIT_DEADLINE_S = 30
# kill_active selects the requests sent in the current window from this long
# after its boundary on, and kills only with this many of them, this far into
# the window and this far from its end.
ACTIVE_COHORT_MARGIN_S = 0.25
ACTIVE_COHORT_MIN_REQUESTS = 5
ACTIVE_WINDOW_ELAPSED_S = 1.0
ACTIVE_WINDOW_LEFT_S = 1.5
# kill_upload throttles the values route while it catches a multipart upload
# (a 5 MiB part takes about 10 s), then the general route while it catches
# the restarted engine's first series PUT (about 8 KiB, several seconds).
MULTIPART_THROTTLE_KBPS = 512
PUT_THROTTLE_KBPS = 1
# The allowance past the cleanup cutoff for deciding held requests
# synchronously (about a microsecond each) and for the process to exit.
HELD_DECISION_ALLOWANCE_S = 1.0
# How long FLUSHING must have held before that series PUT is on the wire.
PUT_IN_FLIGHT_S = 1.0
# A request NGINX logged ending this close to a kill was open at the kill.
KILL_LOG_TOLERANCE_S = 0.1
# The nack classes that refuse a request's own content; no retry cures them.
PERMANENT_NACK_CLASSES = ("request_too_large", "extracted_too_large", "row_too_large",
                          "block_too_large", "too_deep", "invalid", "unsupported")
# The checks a process case records beside FAULT_CHECKS.
PROCESS_CHECKS = ("new_boot_id", "restart_same_cores_and_buffer", "prior_acks_durable",
                  "replay_only_eligible", "no_permanent_rejection", "retry_bytes_identical")
BOOT_EVENT = re.compile(r"series_parquet\.start\b.*?\bboot_id=([0-9a-f]+)")
FILE_BOOT = re.compile(r"/part-\d{8}T\d{6}Z-[A-Za-z0-9_.]+-([0-9a-f]+)-\d+\.parquet$")


def _activate_upload_throttle(rig, parameters):
    """Limit one proxy's upstream (client to store) bandwidth."""
    proxy = parameters["proxy"]
    toxic = {"name": "throttle_upload", "type": "bandwidth", "stream": "upstream",
             "toxicity": 1.0, "attributes": {"rate": int(parameters["rate_kbps"])}}
    rig.toxiproxy.add_toxic(proxy, toxic)
    state = rig.toxiproxy.toxics(proxy)
    if [entry["name"] for entry in state] != [toxic["name"]]:
        raise AssertionError(f"proxy {proxy} carries toxics {state} after activation")
    return {"proxy": proxy, "toxic": toxic, "api_state": state, "units": TOXIC_UNITS}


def _recover_upload_throttle(rig, state):
    """Remove that exact toxic."""
    rig.toxiproxy.remove_toxic(state["proxy"], state["toxic"]["name"])
    remaining = rig.toxiproxy.toxics(state["proxy"])
    if remaining:
        raise AssertionError(f"toxics remain after recovery: {remaining}")
    return {"api_state": remaining}


register_fault("upload_throttle", _activate_upload_throttle, _recover_upload_throttle)


def restart_channel(engine):
    """A gRPC channel to the engine's port that reconnects within a second.

    It is the producer's, not the engine's, so it survives each engine and
    reaches the next one the launcher starts on the same port.
    """
    return test_e2e.grpc.insecure_channel(
        f"127.0.0.1:{engine.grpc_port}",
        options=[("grpc.initial_reconnect_backoff_ms", 200),
                 ("grpc.min_reconnect_backoff_ms", 200),
                 ("grpc.max_reconnect_backoff_ms", 1000)])


def engine_boot_id(engine, deadline_s=30.0):
    """The exporter boot id an engine logged in its start event."""
    cached = getattr(engine, "boot_id", None)
    if cached:
        return cached
    path = Path(engine.log.name)

    def observe():
        match = BOOT_EVENT.search(ANSI.sub("", path.read_text(errors="replace")))
        return match[1] if match else None

    engine.boot_id = measurement.wait_until(
        observe, lambda boot: boot is not None,
        deadline_ns=time.monotonic_ns() + int(deadline_s * 1e9),
        description=f"the exporter start event of {path.parent.name}")
    return engine.boot_id


def file_boot_id(key):
    """The boot id in a part file's name, or None."""
    match = FILE_BOOT.search("/" + key)
    return match[1] if match else None


def container_state(process):
    """`docker inspect`'s state of a container engine, or None for a local one."""
    ident = getattr(process, "container_id", None)
    return _docker_json(["inspect", "--format", "{{json .State}}", ident]) if ident else None


def kill_engine(engine) -> dict:
    """SIGKILL the engine and observe its exit.

    A container engine is killed by its recorded container id and its exit
    read from `docker inspect`, never by signalling the docker client that
    waits for it; a local one is signalled by its host PID.
    """
    process = engine.process
    ident = getattr(process, "container_id", None)
    signal_ns, signal_unix = time.monotonic_ns(), time.time()
    deadline = signal_ns + KILL_EXIT_DEADLINE_S * 10**9
    state = None
    if ident is not None:
        done = run_command(["docker", "kill", "--signal", "KILL", ident],
                           timeout=DOCKER_TIMEOUT_S)
        if done["exit_status"] != 0:
            raise AssertionError(f"docker kill of {ident} failed: {done}")
        state = measurement.wait_until(
            lambda: _docker_json(["inspect", "--format", "{{json .State}}", ident]),
            lambda seen: bool(seen) and not seen.get("Running"),
            deadline_ns=deadline, description="the killed engine container exits")
        _ = measurement.wait_until(process.poll, lambda code: code is not None,
                                   deadline_ns=deadline,
                                   description="the killed engine's docker client exits")
        exit_code = state.get("ExitCode")
    else:
        os.kill(engine.pid, signal.SIGKILL)
        exit_code = measurement.wait_until(process.poll, lambda code: code is not None,
                                           deadline_ns=deadline,
                                           description="the killed engine process exits")
    exited_ns = time.monotonic_ns()
    return {"kind": "kill", "signal": "SIGKILL", "pid": engine.pid, "container_id": ident,
            "signal_ns": signal_ns, "signal_unix_s": signal_unix, "exited_ns": exited_ns,
            "exit_s": (exited_ns - signal_ns) / 1e9, "exit_code": exit_code,
            "container_state": state}


def stop_engine(engine, deadline_s) -> dict:
    """Admin shutdown with `deadline_s` (whole seconds, as the API takes them),
    then the process's own exit, both observed."""
    deadline_s = int(-(-deadline_s // 1))
    signal_ns, signal_unix = time.monotonic_ns(), time.time()
    admin_error = None
    try:
        engine.shutdown(deadline_s)
    except Exception as error:  # noqa: BLE001 - recorded, then raised below
        admin_error = f"{type(error).__name__}: {error}"[:500]
    admin_ns = time.monotonic_ns()
    if admin_error is not None:
        raise AssertionError(f"the admin shutdown of {engine.pid} failed: {admin_error}")
    _ = measurement.wait_until(engine.process.poll, lambda code: code is not None,
                               deadline_ns=admin_ns + SHUTDOWN_EXIT_DEADLINE_S * 10**9,
                               description="the shut-down engine process exits")
    exited_ns = time.monotonic_ns()
    state = container_state(engine.process)
    return {"kind": "graceful", "deadline_s": deadline_s, "pid": engine.pid,
            "container_id": getattr(engine.process, "container_id", None),
            "signal_ns": signal_ns, "signal_unix_s": signal_unix, "exited_ns": exited_ns,
            "admin_returned_s": (admin_ns - signal_ns) / 1e9, "admin_error": admin_error,
            "exit_s": (exited_ns - signal_ns) / 1e9,
            "exit_code": state.get("ExitCode") if state else engine.process.poll(),
            "container_state": state}


def graceful_exit_problem(stopped, deadline_s, abort_timeout_s):
    """Why a graceful stop broke its bounds, or None.

    The admin call must return within the shutdown deadline and the process
    must exit 0 by the absolute cleanup cutoff: the deadline plus
    `upload.abort_timeout`, plus HELD_DECISION_ALLOWANCE_S for deciding held
    requests and exiting.
    """
    cutoff = deadline_s + abort_timeout_s + HELD_DECISION_ALLOWANCE_S
    admin = stopped.get("admin_returned_s")
    if stopped.get("admin_error") or stopped.get("exit_code") != 0 or admin is None \
            or admin > deadline_s + HELD_DECISION_ALLOWANCE_S or stopped["exit_s"] > cutoff:
        return (f"graceful exit {stopped.get('exit_code')} after {stopped['exit_s']:.3f} s "
                f"(admin returned after {admin} s) against the {deadline_s} s shutdown "
                f"deadline and the {cutoff} s cleanup cutoff; admin {stopped.get('admin_error')}")
    return None


def restart_engine(previous, *, retain_buffer: bool):
    """A fresh engine exactly like `previous`, on its launcher, cores and store.

    `retain_buffer` reuses the previous buffer directory as it is; otherwise
    a buffered engine gets a new, empty one. The old and new PID, cores,
    boot ids and buffer paths are recorded as `restart` on the new engine.
    """
    options = dict(previous.launch_options)
    ordinal = int(previous.root.name.rsplit("-", 1)[1]) + 1
    root = previous.root.parent / f"engine-{ordinal}"
    root.mkdir(parents=True, exist_ok=True)
    if options.get("buffer_path") is not None and not retain_buffer:
        options["buffer_path"] = root / "buffer"
    previous_boot = engine_boot_id(previous)
    started_ns = time.monotonic_ns()
    with capacity.malloc_conf(capacity.JEMALLOC_STATS_CONF):
        engine = test_e2e.Engine(root, **options)
    ready_ns = time.monotonic_ns()
    engine.launch_options = options
    engine.restart = {
        "previous_pid": previous.pid, "pid": engine.pid,
        "previous_container_id": getattr(previous.process, "container_id", None),
        "container_id": getattr(engine.process, "container_id", None),
        "previous_cores": previous.cores, "cores": engine.cores,
        "previous_edges": [list(edge) for edge in previous.edges],
        "edges": [list(edge) for edge in engine.edges],
        "previous_boot_id": previous_boot, "boot_id": engine_boot_id(engine),
        "previous_buffer_path": str(previous.buffer_path) if previous.buffer_path else None,
        "buffer_path": str(engine.buffer_path) if engine.buffer_path else None,
        "launched_ns": started_ns, "ready_ns": ready_ns,
        "launch_to_ready_s": (ready_ns - started_ns) / 1e9,
    }
    return engine


def buffer_bytes(path) -> int:
    """The bytes of every file under a buffer directory."""
    return sum(item.stat().st_size for item in Path(path).rglob("*") if item.is_file()) \
        if path is not None and Path(path).is_dir() else 0


def multipart_uploads(store, *, parts=True) -> list:
    """The bucket's incomplete multipart uploads, each with its stored parts' bytes."""
    found = orphaned_uploads(store)
    for upload in found if parts else ():
        try:
            listed = store.client.list_parts(Bucket=store.bucket, Key=upload["key"],
                                             UploadId=upload["upload_id"])
            upload["part_bytes"] = sum(int(part["Size"]) for part in listed.get("Parts", []))
            upload["parts_count"] = len(listed.get("Parts", []))
        except (ClientError, BotoCoreError) as error:
            upload["part_bytes"] = None
            upload["parts_error"] = f"{type(error).__name__}: {error}"[:300]
    return found


def abort_uploads(store, uploads) -> dict:
    """Abort every listed incomplete upload in the store, then list again."""
    if not isinstance(uploads, list):
        return {"aborted": [], "remaining": None, "error": "the uploads were not listed"}
    aborted = []
    for upload in uploads:
        store.client.abort_multipart_upload(Bucket=store.bucket, Key=upload["key"],
                                            UploadId=upload["upload_id"])
        aborted.append(upload["upload_id"])
    remaining = orphaned_uploads(store)
    return {"aborted": aborted, "remaining": remaining, "clean": not remaining}


def ledger_cohorts(ledger, instant_ns, since_ns=None) -> dict:
    """What the producer knew at `instant_ns`: acknowledged and pending requests.

    A pending request was sent before the instant and not acknowledged by
    it. With `since_ns`, the requests first sent in [since_ns, instant_ns)
    form the selected cohort, split into acknowledged and not.
    """
    since_ns = instant_ns if since_ns is None else since_ns
    with ledger.lock:
        row = ledger.connection.execute(
            "SELECT "
            "coalesce(sum(CASE WHEN ack_ns IS NOT NULL AND ack_ns < :t THEN 1 ELSE 0 END), 0), "
            "coalesce(sum(CASE WHEN first_send_ns < :t AND (ack_ns IS NULL OR ack_ns >= :t) "
            "THEN 1 ELSE 0 END), 0), "
            "coalesce(sum(CASE WHEN first_send_ns >= :s AND first_send_ns < :t "
            "AND ack_ns IS NOT NULL AND ack_ns < :t THEN 1 ELSE 0 END), 0), "
            "coalesce(sum(CASE WHEN first_send_ns >= :s AND first_send_ns < :t "
            "AND (ack_ns IS NULL OR ack_ns >= :t) THEN 1 ELSE 0 END), 0), "
            "max(first_send_ns) FROM requests", {"t": int(instant_ns), "s": int(since_ns)},
        ).fetchone()
    acked, pending, cohort_acked, cohort_unacked, last_send = row
    return {"acked_requests_count": acked, "pending_requests_count": pending,
            "cohort_acked_requests_count": cohort_acked,
            "cohort_unacked_requests_count": cohort_unacked,
            "last_send_age_s": (instant_ns - last_send) / 1e9 if last_send else None}


def values_rows_by_file(root, workload) -> list:
    """(record id, store key) of every values row in a downloaded store."""
    rows = []
    with measurement.duckdb.connect() as db:
        for signal in ("logs", "metrics"):
            files = sorted(Path(root).glob(f"v=1/signal={signal}/dataset=values/**/*.parquet"))
            if not files:
                continue
            record_id, _payload = measurement._duck_record_expression(workload, signal)
            relation = (f"read_parquet({[str(path) for path in files]!r}, union_by_name=true, "
                        "hive_partitioning=false, filename=true)")
            for record, filename in db.execute(
                    f"SELECT {record_id}, v.filename FROM {relation} v").fetchall():
                rows.append((record, "otel/" + str(Path(filename).relative_to(root))))
    return rows


def replay_analysis(ledger, rows, events, final_etags, failed_ids=(), *, buffered) -> dict:
    """Each stored record's multiplicity before and after every restart.

    `rows` are the final store's values rows by file; `events` give each
    signal's exit instant, the keys and ETags listed in the store at that
    exit and the case's selected cohort. A record's pre-multiplicity counts
    its rows in files listed at the exit, its post-multiplicity every row;
    a record whose post exceeds a positive pre was stored again after that
    restart (replayed). The eligible cohorts are the requests not
    acknowledged before the exit, which the producer resends, and after a
    SIGKILL in the buffered topology every request, since the buffer
    redelivers what it had not recorded as delivered. A listed key that
    changed or vanished is recorded. A duplicate is attributed when it was
    replayed, belongs to a request the producer resent, or has a copy in a
    failed block (`failed_ids`).
    """
    with ledger.lock:
        connection = ledger.connection
        for statement in (
                "DROP TABLE IF EXISTS file_rows", "DROP TABLE IF EXISTS snapshot_keys",
                "DROP TABLE IF EXISTS replayed_rows", "DROP TABLE IF EXISTS failed_rows",
                "CREATE TEMP TABLE file_rows (record_id TEXT NOT NULL, key TEXT NOT NULL)",
                "CREATE TEMP TABLE snapshot_keys (key TEXT PRIMARY KEY)",
                "CREATE TEMP TABLE replayed_rows (record_id TEXT PRIMARY KEY)",
                "CREATE TEMP TABLE failed_rows (record_id TEXT PRIMARY KEY)"):
            _ = connection.execute(statement)
        _ = connection.executemany("INSERT INTO file_rows VALUES (?, ?)", rows)
        _ = connection.executemany("INSERT OR IGNORE INTO failed_rows VALUES (?)",
                                   [(record,) for record in failed_ids])
        _ = connection.execute("CREATE INDEX file_rows_key ON file_rows(key)")
        _ = connection.execute("CREATE INDEX file_rows_record ON file_rows(record_id)")
        report = []
        for event in events:
            _ = connection.execute("DELETE FROM snapshot_keys")
            _ = connection.executemany("INSERT INTO snapshot_keys VALUES (?)",
                                       [(key,) for key in event["listing"]])
            parameters = {"t": int(event["exited_ns"]),
                          "s": int(event.get("cohort_since_ns") or event["exited_ns"])}
            transitions = collections.defaultdict(dict)
            cohort_stored = 0
            for cohort, pre, post, count, selected in connection.execute(
                    "WITH pre AS (SELECT record_id, count(*) AS n FROM file_rows "
                    "WHERE key IN (SELECT key FROM snapshot_keys) GROUP BY record_id), "
                    "post AS (SELECT record_id, count(*) AS n FROM file_rows GROUP BY record_id) "
                    "SELECT CASE WHEN q.ack_ns IS NOT NULL AND q.ack_ns < :t THEN 'acked' "
                    "WHEN q.first_send_ns < :t THEN 'pending' ELSE 'later' END, "
                    "coalesce(pre.n, 0), coalesce(post.n, 0), count(*), "
                    "sum(CASE WHEN q.first_send_ns >= :s AND q.first_send_ns < :t "
                    "AND coalesce(pre.n, 0) > 0 THEN 1 ELSE 0 END) "
                    "FROM records r JOIN requests q ON q.request_id = r.request_id "
                    "LEFT JOIN pre ON pre.record_id = r.record_id "
                    "LEFT JOIN post ON post.record_id = r.record_id GROUP BY 1, 2, 3",
                    parameters).fetchall():
                transitions[cohort][f"{pre}->{post}"] = count
                cohort_stored += selected or 0
            _ = connection.execute(
                "INSERT OR IGNORE INTO replayed_rows SELECT pre.record_id FROM "
                "(SELECT record_id, count(*) AS n FROM file_rows WHERE key IN "
                "(SELECT key FROM snapshot_keys) GROUP BY record_id) pre JOIN "
                "(SELECT record_id, count(*) AS n FROM file_rows GROUP BY record_id) post "
                "ON post.record_id = pre.record_id WHERE post.n > pre.n")

            def total(cohort, test):
                return sum(count for pair, count in transitions.get(cohort, {}).items()
                           if test(*(int(value) for value in pair.split("->"))))

            replayed = {cohort: total(cohort, lambda pre, post: 0 < pre < post)
                        for cohort in ("acked", "pending", "later")}
            eligible = ("acked", "pending", "later") if buffered and event["kind"] == "kill" \
                else ("pending", "later")
            report.append({
                "ordinal": event["ordinal"], "kind": event["kind"],
                "multiplicity_by_cohort": {cohort: dict(sorted(pairs.items()))
                                           for cohort, pairs in sorted(transitions.items())},
                "acked_records_count": total("acked", lambda pre, post: True),
                "acked_stored_at_exit_records": total("acked", lambda pre, post: pre > 0),
                "acked_missing_at_exit_records": total("acked", lambda pre, post: pre == 0),
                "acked_missing_at_end_records": total("acked", lambda pre, post: post == 0),
                "pending_records_count": total("pending", lambda pre, post: True),
                "pending_stored_at_exit_records": total("pending", lambda pre, post: pre > 0),
                "replayed_records_by_cohort": replayed,
                "replayed_records_count": sum(replayed.values()),
                "eligible_cohorts": list(eligible),
                "ineligible_replayed_records": sum(value for cohort, value in replayed.items()
                                                   if cohort not in eligible),
                "selected_cohort_stored_at_exit_records": cohort_stored,
                "changed_listed_keys": sorted(
                    key for key, etag in event["listing"].items()
                    if final_etags.get(key) != etag)[:20],
            })
        duplicated, outside = connection.execute(
            "SELECT count(*), coalesce(sum(CASE WHEN p.record_id IS NULL AND f.record_id IS NULL "
            "AND s.record_id IS NULL THEN 1 ELSE 0 END), 0) FROM (SELECT record_id FROM "
            "file_rows GROUP BY record_id HAVING count(*) > 1) d "
            "LEFT JOIN replayed_rows p ON p.record_id = d.record_id "
            "LEFT JOIN failed_rows f ON f.record_id = d.record_id "
            "LEFT JOIN (SELECT r.record_id FROM records r JOIN (SELECT request_id FROM attempts "
            "GROUP BY request_id HAVING count(*) > 1) t ON t.request_id = r.request_id) s "
            "ON s.record_id = d.record_id").fetchone()
        replayed_total = connection.execute("SELECT count(*) FROM replayed_rows").fetchone()[0]
    return {"events": report, "replayed_records_count": int(replayed_total),
            "duplicated_records": int(duplicated),
            "duplicated_unattributed_records": int(outside)}


def interrupted_requests(requests, instant_unix, *, key_part):
    """The logged requests on keys containing `key_part` that were open at a kill.

    Such a request started before the kill and NGINX logged its end at or
    after it, with a status other than 2xx.
    """
    found = []
    for entry in requests:
        if key_part not in (entry.get("uri") or ""):
            continue
        end = _float(entry.get("msec"))
        took = _float(entry.get("request_time")) or 0.0
        if end is None or str(entry.get("status", "")).startswith("2"):
            continue
        if end - took <= instant_unix + KILL_LOG_TOLERANCE_S \
                and end >= instant_unix - KILL_LOG_TOLERANCE_S:
            found.append({key: entry.get(key) for key in (
                "msec", "method", "operation", "status", "upstream_status", "request_time",
                "request_length", "uri")})
    return found


def window_position(unix_s, interval_s):
    """The aligned window around a wall-clock instant: its start, elapsed and left seconds."""
    start = (unix_s // interval_s) * interval_s
    return start, unix_s - start, start + interval_s - unix_s


class ProcessCase(FaultCase):
    """A process case: a graceful restart or SIGKILL of the engine, then a new one.

    Every signal is an event with its gate evidence, the producer's cohorts
    at the signal and at the exit, the exit itself, the store's listing and
    incomplete uploads at the exit and the restart that followed. States
    carry the event's ordinal: `gate_N`, `signalled_N`, `exited_N`,
    `restarted_N`; kill_upload also has `armed_N` and `fault_removed_N`.
    """

    def __init__(self, spec, rig, store, engine, phase, ledger, lister, settings, *,
                 controls, engines):
        super().__init__(spec, rig, store, engine, phase, ledger, lister, settings)
        self.controls = controls
        self.engines = engines
        self.phases = [phase]
        self.events = []
        self.discarded = []
        self.drain_deadline_s = int(lateness_bound_s(settings)) + 15

    # -- the sequences ----------------------------------------------------

    def run(self, controls, store_cores, result, options):
        """This case's lifecycle after the baseline."""
        getattr(self, f"run_{self.fault}")()

    def run_graceful_restart(self):
        """Pending work, admin shutdown and drain, restart on the same buffer and cores."""
        if self.attempt(self.gate_pending) and self.attempt(self.shutdown) \
                and self.attempt(self.restart):
            self.resume()

    def run_kill_active(self):
        """SIGKILL with a cohort only in the ACTIVE block, then restart."""
        if self.attempt(self.gate_active) and self.attempt(self.kill) \
                and self.attempt(self.restart):
            self.resume()

    def run_kill_upload(self):
        """SIGKILL in a multipart upload, restart, SIGKILL in a single PUT, restart."""
        self.throttle("values", MULTIPART_THROTTLE_KBPS)
        if not (self.attempt(self.gate_multipart) and self.attempt(self.kill)):
            return
        self.unthrottle()
        self.throttle("general", PUT_THROTTLE_KBPS)
        if not (self.attempt(self.restart) and self.attempt(self.gate_put)
                and self.attempt(self.kill)):
            return
        self.unthrottle()
        if self.attempt(self.restart):
            self.resume()

    def resume(self):
        """A values file of the new boot and an ack, then the rest of the input."""
        if self.attempt(self.await_resumed):
            self.attempt(self.await_after_input)

    # -- observations -----------------------------------------------------

    def boot_id(self):
        """The current engine's boot id."""
        return engine_boot_id(self.engine)

    def observed_totals(self, sample):
        """A sample's totals, the latest background sample's without one."""
        return flat_totals(sample) if sample is not None else self.totals()

    def observe_pending(self, sample=None):
        """graceful_restart: ACTIVE nonempty, live input and requests awaiting their ack."""
        totals = self.observed_totals(sample)
        cohorts = ledger_cohorts(self.ledger, time.monotonic_ns())
        return {"block_active_bytes": totals.get("block.active", 0),
                "block_flushing_bytes": totals.get("block.flushing", 0),
                "buffer_in_flight_count": totals.get("buffer.in.flight", 0),
                "cohorts": cohorts, "fresh": sample is not None, "totals": totals}

    def pending_met(self, seen):
        """Work the drain must finish: strict, unacknowledged requests; buffered,
        bundles the exporter holds."""
        work = (seen["buffer_in_flight_count"] > 0 if self.buffered
                else seen["cohorts"]["pending_requests_count"] > 0)
        return (seen["block_active_bytes"] > 0 and work
                and (seen["cohorts"]["last_send_age_s"] or 99) < 1.0)

    def observe_active(self, sample=None):
        """kill_active: the window's position, the blocks and the selected cohort."""
        totals = self.observed_totals(sample)
        now_ns, now_unix = time.monotonic_ns(), time.time()
        start, elapsed, left = window_position(now_unix, self.spec.interval_s)
        since_ns = now_ns - int((elapsed - ACTIVE_COHORT_MARGIN_S) * 1e9)
        cohorts = ledger_cohorts(self.ledger, now_ns, since_ns)
        return {"window_start_unix_s": start, "elapsed_s": round(elapsed, 3),
                "left_s": round(left, 3), "cohort_since_ns": since_ns,
                "block_active_bytes": totals.get("block.active", 0),
                "block_flushing_bytes": totals.get("block.flushing", 0),
                "block_pending_bytes": totals.get("block.pending", 0),
                "selected_cohort_requests_count": cohorts[
                    "cohort_acked_requests_count" if self.buffered
                    else "cohort_unacked_requests_count"],
                "cohorts": cohorts, "fresh": sample is not None, "totals": totals}

    @staticmethod
    def active_met(seen):
        """ACTIVE only: nothing flushing or sealed, a cohort of this window, and
        time left before it rotates."""
        return (seen["block_active_bytes"] > 0 and seen["block_flushing_bytes"] == 0
                and seen["block_pending_bytes"] == 0
                and seen["elapsed_s"] >= ACTIVE_WINDOW_ELAPSED_S
                and seen["left_s"] >= ACTIVE_WINDOW_LEFT_S
                and seen["selected_cohort_requests_count"] >= ACTIVE_COHORT_MIN_REQUESTS)

    def observe_multipart(self, sample=None):
        """kill_upload: the current boot's open values uploads, their bytes, and FLUSHING."""
        totals = self.observed_totals(sample)
        boot = self.boot_id()
        requests = self.route_requests()
        completed = {entry["uri"] for entry in requests
                     if entry["operation"] == "complete_multipart_upload"
                     and str(entry.get("status", "")).startswith("2")}
        logged = collections.Counter()
        for entry in requests:
            if entry["operation"] == "upload_part" and str(entry.get("status", "")).startswith("2"):
                query = urllib.parse.parse_qs((entry.get("request_uri") or "").partition("?")[2])
                for upload_id in query.get("uploadId", []):
                    logged[upload_id] += int(_float(entry.get("request_length")) or 0)
        uploads = [dict(upload, logged_part_bytes=logged[upload["upload_id"]])
                   for upload in multipart_uploads(self.store)
                   if boot in upload["key"] and "/dataset=values/" in upload["key"]
                   and f"/{self.store.bucket}/{upload['key']}" not in completed]
        return {"block_flushing_bytes": totals.get("block.flushing", 0), "open_uploads": uploads,
                "fresh": sample is not None, "totals": totals}

    @staticmethod
    def multipart_met(seen):
        """A multipart upload open in the store with bytes transferred, and FLUSHING."""
        return seen["block_flushing_bytes"] > 0 and any(
            (upload.get("part_bytes") or 0) > 0 or upload["logged_part_bytes"] > 0
            for upload in seen["open_uploads"])

    def observe_put(self, sample=None):
        """kill_upload: FLUSHING held with no request of the new boot finished yet."""
        totals = self.observed_totals(sample)
        boot = self.boot_id()
        samples = list(self.phase.sampler.samples)
        since = None
        for entry in reversed(samples):
            if flat_totals(entry).get("block.flushing", 0) <= 0:
                break
            since = entry["monotonic_ns"]
        flushing = totals.get("block.flushing", 0)
        return {"block_flushing_bytes": flushing,
                "flushing_for_s": (time.monotonic_ns() - since) / 1e9
                if since is not None and flushing > 0 else 0.0,
                "boot_requests_logged": [entry["operation"] for entry in self.route_requests()
                                         if boot in (entry.get("uri") or "")],
                "fresh": sample is not None, "totals": totals}

    @staticmethod
    def put_met(seen):
        """The first flush of the boot has held its series PUT open a while."""
        return (seen["block_flushing_bytes"] > 0 and seen["flushing_for_s"] >= PUT_IN_FLIGHT_S
                and not seen["boot_requests_logged"])

    # -- the steps --------------------------------------------------------

    def gate(self, observe, met, description):
        """Wait for a lifecycle gate, then confirm it on a fresh sample.

        A gate the fresh sample no longer shows is a discarded setup attempt,
        recorded and never claimed; the wait resumes until the case's setup
        deadline, and a gate never confirmed by then fails the step. The
        lifetime first answers the collection epochs every measured lifetime
        needs (`measure.MINIMUM_EPOCHS`).
        """
        ordinal = len(self.events) + 1
        deadline = time.monotonic_ns() + FAULT_OBSERVE_DEADLINE_S[self.fault] * 10**9
        _ = capacity._command().await_answered_epochs(
            self.phase.sampler, [worker["key"] for worker in self.phase.workers],
            deadline_ns=deadline)
        while True:
            seen = measurement.wait_until(observe, met, deadline_ns=deadline,
                                          description=description)
            fresh = observe(self.phase.sampler.once())
            if met(fresh):
                fresh["background"] = {key: value for key, value in seen.items()
                                       if key != "totals"}
                self.transition(f"gate_{ordinal}", fresh)
                return fresh
            self.discarded.append({"gate": ordinal, "unix_s": time.time(),
                                   "fresh": {key: value for key, value in fresh.items()
                                             if key != "totals"}})

    def gate_pending(self):
        """graceful_restart's gate."""
        return self.gate(self.observe_pending, self.pending_met, "pending work to drain")

    def gate_active(self):
        """kill_active's gate."""
        return self.gate(self.observe_active, self.active_met,
                         "a cohort only in the ACTIVE block, nothing flushing")

    def gate_multipart(self):
        """kill_upload's first gate."""
        return self.gate(self.observe_multipart, self.multipart_met,
                         "a multipart values upload in flight with bytes stored")

    def gate_put(self):
        """kill_upload's second gate."""
        return self.gate(self.observe_put, self.put_met,
                         "the restarted engine's first series PUT on the wire")

    def throttle(self, proxy, rate_kbps):
        """Throttle one route's uploads."""
        self.rig.activate("upload_throttle", {"proxy": proxy, "rate_kbps": rate_kbps})
        self.transition(f"armed_{len(self.events) + 1}", {
            "proxy": proxy, "rate_kbps": rate_kbps, "activation": self.rig.activations[-1]})

    def unthrottle(self):
        """Remove the throttle."""
        self.rig.recover()
        self.transition(f"fault_removed_{len(self.events)}",
                        {"recovery": self.rig.activations[-1].get("recovery")})

    def kill(self):
        """SIGKILL the current engine (`kill_engine`)."""
        self.signal("kill")

    def shutdown(self):
        """Shut the current engine down through the admin API (`stop_engine`)."""
        self.signal("graceful")

    def signal(self, kind):
        """Stop the current engine and record the event: cohorts, exit, store state."""
        engine = self.engine
        ordinal = len(self.events) + 1
        gate = (self.at(f"gate_{ordinal}") or {}).get("evidence", {})
        since_ns = gate.get("cohort_since_ns")
        self.controls.unwatch_workers()
        at_signal = ledger_cohorts(self.ledger, time.monotonic_ns(), since_ns)
        self.transition(f"signalled_{ordinal}", {"kind": kind, "pid": engine.pid})
        try:
            stopped = kill_engine(engine) if kind == "kill" \
                else stop_engine(engine, self.drain_deadline_s)
        finally:
            self.phase.sampler.stop()
            self.phase.signal_ns = self.at(f"signalled_{ordinal}")["monotonic_ns"]
        listing = self.lister.list_once()
        event = {
            "ordinal": ordinal, "kind": kind, "pid": engine.pid, "boot_id": engine_boot_id(engine),
            "gate": {key: value for key, value in gate.items() if key not in ("totals",)},
            "cohorts_at_signal": at_signal,
            "cohorts_at_exit": ledger_cohorts(self.ledger, stopped["exited_ns"], since_ns),
            "cohort_since_ns": since_ns, "exit": stopped, "exited_ns": stopped["exited_ns"],
            "listing": {key: item.get("etag") for key, item in listing.items()},
            "uploads_at_exit": multipart_uploads(self.store),
        }
        self.events.append(event)
        self.transition(f"exited_{ordinal}", {
            key: stopped[key] for key in ("exit_s", "exit_code", "pid", "container_id")}
            | {"objects_at_exit_count": len(listing),
               "uploads_at_exit_count": len(event["uploads_at_exit"])})

    def restart(self):
        """Start the next engine like the last on the same buffer and cores, and verify it."""
        event = self.events[-1]
        previous = self.engine
        listing = self.lister.list_once()
        event["late_objects"] = sorted(set(listing) - set(event["listing"]))
        inventory = capacity._command().buffer_inventory
        event["buffer_before"] = inventory(previous.buffer_path) if self.buffered else None
        event["buffer_bytes_at_restart"] = buffer_bytes(previous.buffer_path) \
            if self.buffered else None
        engine = restart_engine(previous, retain_buffer=True)
        self.engines.append(engine)
        phase = capacity.CapacityPhase(capacity._command(), engine.root.name, engine, self.spec,
                                       self.controls, self.buffered)
        self.engine, self.phase = engine, phase
        self.phases.append(phase)
        self.log = LogTail(engine.log.name)
        phase.ready(f"restart-{event['ordinal']}")
        workers_ns = time.monotonic_ns()
        event["restart"] = dict(
            engine.restart,
            buffer_after=inventory(engine.buffer_path) if self.buffered else None,
            workers_ready_ns=workers_ns,
            exit_to_ready_s=(workers_ns - event["exited_ns"]) / 1e9,
            signal_to_ready_s=(workers_ns - event["exit"]["signal_ns"]) / 1e9)
        self.transition(f"restarted_{event['ordinal']}", {
            key: event["restart"][key] for key in (
                "previous_pid", "pid", "previous_boot_id", "boot_id", "cores",
                "launch_to_ready_s", "exit_to_ready_s")}
            | {"late_objects_count": len(event["late_objects"])})

    def await_resumed(self):
        """A values file of the new boot and a request acknowledged after the restart."""
        restarted = self.at(f"restarted_{len(self.events)}")
        boot = self.boot_id()

        def observe():
            acks = [ack for ack in ledger_acks(self.ledger) if ack >= restarted["monotonic_ns"]]
            files = sorted(key for key in self.lister.first_listed
                           if boot in key and "dataset=values/" in key)
            return {"values_files_of_new_boot_count": len(files),
                    "first_values_file_s": min(
                        (self.lister.first_listed[key] - restarted["unix_s"] for key in files),
                        default=None),
                    "producer_acks_count": len(acks),
                    "first_ack_after_restart_s": (acks[0] - restarted["monotonic_ns"]) / 1e9
                    if acks else None,
                    "exporter_acks_count": self.totals().get("acks", 0)}

        evidence = measurement.wait_until(
            observe, lambda seen: seen["values_files_of_new_boot_count"] > 0
            and seen["producer_acks_count"] > 0 and seen["exporter_acks_count"] > 0,
            deadline_ns=self.recovery_deadline_ns(),
            description="a values file of the new boot and an acknowledgement after the restart",
        )
        self.transition("resumed", evidence)

    def recovery_deadline_ns(self):
        """Everything after the last restart must finish within RECOVERY_DEADLINE_S."""
        return self.at(f"restarted_{len(self.events)}")["monotonic_ns"] \
            + RECOVERY_DEADLINE_S * 10**9

    # -- what the settlement reads ----------------------------------------

    def lifetime_samples(self, phase):
        """A lifetime's samples up to its signal; later ones describe no engine."""
        cut = getattr(phase, "signal_ns", None)
        return [sample for sample in phase.sampler.samples
                if cut is None or sample["monotonic_ns"] < cut]

    def lifetime_summary(self, phase):
        """A signalled lifetime's sample figures up to its signal; the sampler
        errors after it met an engine that was stopping or gone."""
        cut = getattr(phase, "signal_ns", None)
        if cut is None:
            return {}
        command = capacity._command()
        samples = self.lifetime_samples(phase)
        errors = list(phase.sampler.errors)
        return {"sampler_errors": [entry for entry in errors if entry["monotonic_ns"] < cut],
                "sampler_errors_after_signal_count": sum(
                    1 for entry in errors if entry["monotonic_ns"] >= cut),
                "sample_count": len(samples),
                "answered_epochs_by_worker": dict(command.answered_epochs(samples)),
                "stale_gap_s_by_worker": command.stale_gaps(samples),
                "signal_ns": cut}

    def all_samples(self):
        """Every lifetime's samples, each up to its signal."""
        return [sample for phase in self.phases for sample in self.lifetime_samples(phase)]

    def abort_failures(self, final):
        """The failed multipart aborts every engine reported by its last sample."""
        return sum(int(flat_totals(samples[-1]).get("flush.abort_failures", 0))
                   for samples in (self.lifetime_samples(phase) for phase in self.phases)
                   if samples)

    def expected_orphans(self):
        """Every values upload a killed engine left open at its exit."""
        return {upload["upload_id"]: upload["key"] for event in self.events
                if event["kind"] == "kill" for upload in event["uploads_at_exit"]
                if event["boot_id"] in upload["key"]}

    def replay_events(self):
        """The events `replay_analysis` compares, one per signal that exited."""
        return [{key: event.get(key) for key in (
            "ordinal", "kind", "exited_ns", "listing", "cohort_since_ns")}
            for event in self.events]

    def fault_window(self):
        """From the first signal to the last restart."""
        first, last = self.at("signalled_1"), self.at(f"restarted_{len(self.events)}")
        return (first["unix_s"], last and last["unix_s"]) if first else None

    def throughput_edges(self):
        """Before the first signal, until the last restart, and after it."""
        last = f"restarted_{len(self.events)}"
        return {"before": ("input_started", "signalled_1"), "during": ("signalled_1", last),
                "after": (last, "input_stopped")}

    def explain_duplicates(self, duplicates, record):
        """Strict: resent requests. Buffered: a copy stored before a restart and
        again after it, a request the producer resent because a kill cut off
        its acknowledgement, or a copy in a failed block."""
        if not self.buffered:
            return duplicates_explained(False, duplicates)
        replay = record.get("replay")
        if not replay:
            return False, "the replay was not measured"
        outside = replay["duplicated_unattributed_records"]
        return outside == 0, (
            f"{outside} of {replay['duplicated_records']} duplicated records were neither "
            f"stored again after a restart ({replay['replayed_records_count']} replayed), nor "
            f"in a resent request ({duplicates.get('duplicated_in_resent_requests_records')}), "
            "nor copied in a failed block")

    def verdicts(self, record):
        """fault_observed and recovered, then PROCESS_CHECKS."""
        replay = record.get("replay") or {}
        by_event = {entry["ordinal"]: entry for entry in replay.get("events", [])}
        observed, why = self.lifecycle_observed(record, by_event)
        last = f"restarted_{len(self.events)}"
        recovered_s = self.span(last, "drained")
        boots = [engine_boot_id(engine) for engine in self.engines]
        values_boots = collections.Counter(
            file_boot_id(item["key"]) for item in record["objects"]
            if "/dataset=values/" in item["key"])
        stored_boots = {file_boot_id(item["key"]) for item in record["objects"]}
        restarts = [event.get("restart") for event in self.events]
        same = [bool(entry) and entry["cores"] == entry["previous_cores"] == list(self.spec.cores)
                and entry["edges"] == entry["previous_edges"] for entry in restarts]
        retained = [not self.buffered or (
            bool(entry) and entry["buffer_path"] == entry["previous_buffer_path"]
            and capacity._command().buffer_retained(event.get("buffer_before"),
                                                    (entry or {}).get("buffer_after")))
            for event, entry in zip(self.events, restarts)]
        strict_missing = sum(entry["acked_missing_at_exit_records"] for entry in by_event.values())
        end_missing = sum(entry["acked_missing_at_end_records"] for entry in by_event.values())
        ineligible = sum(entry["ineligible_replayed_records"] for entry in by_event.values())
        changed = [key for entry in by_event.values() for key in entry["changed_listed_keys"]]
        outcomes = record.get("ledger_outcomes") or {}
        permanent = {}
        for index, phase in enumerate(self.phases):
            samples = self.lifetime_samples(phase)
            totals = flat_totals(samples[-1]) if samples else {}
            for name in [f"nacks.{label}" for label in PERMANENT_NACK_CLASSES] + [
                    "buffer.bundles.resolved.permanently_rejected"]:
                if totals.get(name):
                    permanent[f"engine-{index + 1}:{name}"] = totals[name]
        resent = (record.get("duplicates") or {}).get("resent_requests_count")
        return [
            ("fault_observed", observed, why),
            ("recovered", self.at("resumed") is not None and recovered_s is not None
             and recovered_s <= RECOVERY_DEADLINE_S,
             f"last restart to drained {recovered_s} s against {RECOVERY_DEADLINE_S} s; "
             f"resumed {bool(self.at('resumed'))}; problems {self.problems}"),
            ("new_boot_id", len(self.engines) == len(self.events) + 1 and all(boots)
             and len(set(boots)) == len(boots) and values_boots.get(boots[0], 0) > 0
             and values_boots.get(boots[-1], 0) > 0 and stored_boots <= set(boots),
             f"boots {boots}; values files by boot {dict(values_boots)}; stored boots not "
             f"launched here {sorted(str(b) for b in stored_boots - set(boots))}"),
            ("restart_same_cores_and_buffer", bool(restarts) and all(same) and all(retained),
             f"cores and graph unchanged {same}; buffer retained {retained}; restarts "
             f"{[{k: (e or {}).get(k) for k in ('previous_cores', 'cores', 'previous_buffer_path', 'buffer_path')} for e in restarts]}"),
            ("prior_acks_durable", bool(by_event) and end_missing == 0
             and (self.buffered or strict_missing == 0),
             f"records acknowledged before an exit and not stored at that exit "
             f"{strict_missing} (strict must be 0; buffered holds them in its log), not stored "
             f"at the end {end_missing}"),
            ("replay_only_eligible", bool(by_event) and ineligible == 0 and not changed,
             f"records stored again after a restart outside the eligible cohorts {ineligible}; "
             f"per event {[(e['ordinal'], e['kind'], e['replayed_records_by_cohort']) for e in by_event.values()]}; "
             f"listed keys changed or removed {changed[:5]}"),
            ("no_permanent_rejection", not permanent and not outcomes.get(
                measurement.OUTCOME_PERMANENT) and not outcomes.get(measurement.OUTCOME_PARTIAL),
             f"producer outcomes {outcomes}; engine permanent refusals {permanent}"),
            ("retry_bytes_identical", record.get("producer_finished", False),
             f"the ledger refuses a resent request with different bytes; {resent} requests were "
             "resent and the producer finished"),
        ]

    def lifecycle_observed(self, record, by_event):
        """Whether every gate was met and every signal hit what it aimed at."""
        problems = list(self.problems)
        expected = {"graceful_restart": 1, "kill_active": 1, "kill_upload": 2}[self.fault]
        if len(self.events) != expected or not all(event.get("restart") for event in self.events):
            problems.append(f"{len(self.events)} of {expected} signals exited and restarted")
        for event in self.events:
            stopped = event["exit"]
            if event["kind"] == "kill" and stopped["exit_code"] not in (137, -9):
                problems.append(f"event {event['ordinal']}: exit {stopped['exit_code']} is not "
                                "a SIGKILL")
            if event["kind"] == "graceful":
                problem = graceful_exit_problem(stopped, self.drain_deadline_s, duration_s(
                    self.settings["upload"]["abort_timeout"]))
                if problem:
                    problems.append(problem)
        if self.fault == "kill_active" and self.events:
            event = self.events[0]
            gate = event["gate"]
            end = gate.get("window_start_unix_s", 0) + self.spec.interval_s
            stored = (by_event.get(1) or {}).get("selected_cohort_stored_at_exit_records")
            if event["exit"]["signal_unix_s"] >= end or stored != 0:
                problems.append(f"the kill at {event['exit']['signal_unix_s']} was not inside the "
                                f"window ending {end}, or {stored} cohort records were stored")
        if self.fault == "kill_upload" and len(self.events) == 2:
            problems.extend(self.upload_problems(record))
        return not problems, "; ".join(problems)[:1500] or json.dumps(
            [{"ordinal": e["ordinal"], "kind": e["kind"], "exit_s": e["exit"]["exit_s"],
              "exit_code": e["exit"]["exit_code"]} for e in self.events])

    def upload_problems(self, record):
        """kill_upload: the multipart upload and the single PUT were both cut off.

        The caught multipart upload can never complete, since no completion
        was sent. A single PUT whose whole body the route had already taken
        in may still be committed by the store after the kill; that is a
        late object of the killed boot, recorded, not a missed kill.
        """
        problems = []
        final_keys = {item["key"] for item in record["objects"]}
        first, second = self.events
        caught = [upload for upload in first["gate"].get("open_uploads", [])
                  if (upload.get("part_bytes") or 0) > 0 or upload["logged_part_bytes"] > 0]
        open_ids = {upload["upload_id"] for upload in first["uploads_at_exit"]}
        if not any(upload["upload_id"] in open_ids and upload["key"] not in final_keys
                   for upload in caught):
            problems.append("no caught multipart upload stayed incomplete and uncompleted")
        put = interrupted_requests(record["requests"], second["exit"]["signal_unix_s"],
                                   key_part=second["boot_id"])
        cut = [entry for entry in put if entry["operation"] == "put_object"
               and "/dataset=series/" in entry["uri"]]
        if not cut:
            problems.append(f"no series PUT of boot {second['boot_id']} was cut off by the kill: "
                            f"{put}")
        return problems

    def numbers(self, record, during):
        """The lifecycle's durations, PIDs, boots and cohort sizes."""
        first = self.events[0] if self.events else {}
        last = self.events[-1] if self.events else {}
        replay = record.get("replay") or {}
        restart = first.get("restart") or {}
        resumed = (self.at("resumed") or {}).get("evidence", {})
        return {
            "old_pid": first.get("pid"),
            "new_pid": (last.get("restart") or {}).get("pid"),
            "boot_ids": [engine_boot_id(engine) for engine in self.engines],
            "signals_count": len(self.events),
            "time_to_exit_s": (first.get("exit") or {}).get("exit_s"),
            "exit_codes": [event["exit"]["exit_code"] for event in self.events],
            "launch_to_ready_s": restart.get("launch_to_ready_s"),
            "exit_to_ready_s": restart.get("exit_to_ready_s"),
            "signal_to_ready_s": restart.get("signal_to_ready_s"),
            "first_ack_after_restart_s": resumed.get("first_ack_after_restart_s"),
            "first_values_file_after_restart_s": resumed.get("first_values_file_s"),
            "graceful_drain_s": (first.get("exit") or {}).get("exit_s")
            if first.get("kind") == "graceful" else None,
            "partition_lateness_bound_s": lateness_bound_s(self.settings),
            "admin_shutdown_deadline_s": self.drain_deadline_s,
            "cleanup_cutoff_s": self.drain_deadline_s + duration_s(
                self.settings["upload"]["abort_timeout"]) + HELD_DECISION_ALLOWANCE_S,
            "final_shutdown_s": (record.get("final_stop") or {}).get("exit_s"),
            "acked_requests_at_signal_count": (first.get("cohorts_at_signal") or {}).get(
                "acked_requests_count"),
            "pending_requests_at_signal_count": (first.get("cohorts_at_signal") or {}).get(
                "pending_requests_count"),
            "selected_cohort_requests_count": (first.get("gate") or {}).get(
                "selected_cohort_requests_count"),
            "replayed_records_count": replay.get("replayed_records_count"),
            "orphans_expected_count": len(self.expected_orphans()),
            "buffer_bytes_at_restart": first.get("buffer_bytes_at_restart"),
            "drain_s": self.span("input_stopped", "drained"),
            "gate_discards_count": len(self.discarded),
        }

    def producer_windows(self):
        """What the producer was told during each downtime and after each restart."""
        windows = []
        for event in self.events:
            ready = (event.get("restart") or {}).get("workers_ready_ns")
            windows.append({
                "ordinal": event["ordinal"],
                "signal_to_ready": ledger_attempts_between(
                    self.ledger, event["exit"]["signal_ns"], ready),
                "ready_to_next_signal": ledger_attempts_between(
                    self.ledger, ready, next((later["exit"]["signal_ns"] for later in self.events
                                              if later["ordinal"] > event["ordinal"]), None))
                if ready else None,
            })
        return windows

    def observations(self, record):
        """The events, their timelines and the replay analysis."""
        final_keys = {item["key"] for item in record["objects"]}
        return {"process": {
            "events": [{key: value for key, value in event.items() if key != "listing"}
                       | {"objects_at_exit_count": len(event["listing"]),
                          "interrupted_requests": [
                              dict(entry, completed_in_store=entry["uri"].partition(
                                  f"/{self.store.bucket}/")[2] in final_keys)
                              for entry in interrupted_requests(
                                  record["requests"], event["exit"]["signal_unix_s"],
                                  key_part=event["boot_id"])]}
                       for event in self.events],
            "gate_discards": self.discarded,
            "replay": record.get("replay"),
            "producer_windows": record.get("windows"),
            "final_stop": record.get("final_stop"),
            "orphan_cleanup": record.get("orphan_cleanup"),
            "expected_orphans": self.expected_orphans(),
        }}


def ledger_attempts_between(ledger, start_ns, end_ns) -> dict:
    """The producer's attempts started in [start_ns, end_ns), by outcome and code."""
    with ledger.lock:
        rows = ledger.connection.execute(
            "SELECT outcome, detail, count(*) FROM attempts WHERE start_ns >= ? "
            "AND (? IS NULL OR start_ns < ?) GROUP BY outcome, detail",
            (int(start_ns), end_ns, end_ns)).fetchall()
    by_outcome, by_code = collections.Counter(), collections.Counter()
    samples = {}
    for outcome, detail, count in rows:
        by_outcome[outcome] += count
        code = (detail or "").split(":", 1)[0].replace("StatusCode.", "") or "OK"
        by_code[code] += count
        if detail and code not in samples:
            samples[code] = detail[:200]
    return {"by_outcome": dict(sorted(by_outcome.items())), "by_code": dict(sorted(by_code.items())),
            "detail_samples": samples}


def soak_byte_size(text) -> int:
    """A byte size such as `5MiB`, read by the soak's parser."""
    try:
        from . import soak
    except ImportError:
        import soak
    return soak.byte_size(text)


def fault_check(result: dict) -> None:
    """Fail unless a fault case passes exactly what its published status requires.

    Every check in `FAULT_CHECKS` must be present, every hard check of the
    result must have passed, no acknowledged record may be missing, and the
    multiplicity histogram and the duplicates must have been measured.
    """
    names = {entry["name"] for entry in result["checks"]}
    fault = (result.get("observations", {}).get("fault") or {}).get("fault")
    for name in FAULT_CHECKS + (PROCESS_CHECKS if fault in FAILURE_FAMILIES["process"] else ()):
        if name not in names:
            raise AssertionError(f"failed required fault check: {name}: never checked")
    for entry in result["checks"]:
        if entry["kind"] == measurement.CHECK_HARD and entry["status"] != measurement.STATUS_PASSED:
            raise AssertionError(f"failed required fault check: {entry['name']}: "
                                 f"{entry['detail']}")
    if result["metrics"].get("missing_acked_records") != 0:
        raise AssertionError("acknowledged supported record missing")
    fault = result.get("observations", {}).get("fault") or {}
    histogram = fault.get("multiplicity_histogram")
    if not isinstance(histogram, dict) or not histogram:
        raise AssertionError("multiplicity histogram absent")
    if not isinstance((fault.get("numbers") or {}).get("duplicate_records"), int):
        raise AssertionError("duplicates were not measured")


def failure_case(family: str, fault: str, topology: str, store: str, output_dir: Path, *,
                 report_dir=None, ordinal=None, lease_wait_s=3600.0, archive_dir=None,
                 **options) -> dict:
    """Run one matrix cell and return its result.

    Prerequisites are checked before the lease and before any traffic: a
    missing one skips an optional lane and fails a required one. Anything
    after that, a fault activation error included, is a failure.
    """
    command = capacity._command()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    _ = failure_prerequisites(store)
    provenance = command.prepare_build()
    if ordinal is None:
        ordinal = capacity.next_ordinal_factory(
            measurement.resolve_report_dir(report_dir), output_dir,
            prefix=f"failure-{family}-{fault}-{topology}-{store}-")()
    spec = failure_spec(family, fault, topology, store, ordinal=ordinal,
                        cores=options.get("cores"))
    run_dir = output_dir / spec.run_id
    if options.pop("straddle_hour", False):
        # The wait for the hour happens before the lease, so the host stays free.
        arm_at = straddle_arm_instant(time.time())
        if arm_at - time.time() > STRADDLE_MAX_WAIT_S:
            raise AssertionError(f"{spec.run_id}: the straddled hour end is more than "
                                 f"{STRADDLE_MAX_WAIT_S} s away")
        sys.stderr.write(f"{spec.run_id}: waiting without the lease until "
                         f"{arm_at - STRADDLE_LEAD_S} (unix s), {STRADDLE_LEAD_S} s before "
                         f"arming {STRADDLE_ARM_BEFORE_END_S} s before the hour ends\n")
        _ = await_wall_clock(arm_at - STRADDLE_LEAD_S, "the straddling cell's start")
        options["arm_at_unix_s"] = arm_at

    def experiment(spec_, result, directory, controls):
        failure_experiment(spec_, result, directory, controls, provenance=provenance,
                           options=options)

    try:
        result = command.run_case(spec, run_dir, experiment=experiment, report_dir=report_dir,
                                  evaluate=True, lease_wait_s=float(lease_wait_s))
    except Exception as error:  # noqa: BLE001 - recorded in the result
        sys.stderr.write(f"{spec.run_id}: {type(error).__name__}: {error}\n")
        result = json.loads((run_dir / f"{spec.run_id}.json").read_text(encoding="ascii"))
    for entry in [{"name": f"{spec.run_id}.json"}] + result["baseline_files"]:
        _ = shutil.copyfile(run_dir / entry["name"], output_dir / entry["name"])
    capacity.archive_trial({"archive_dir": archive_dir or FAULT_ARCHIVE_ROOT / f"failure-{family}"},
                           run_dir)
    shutil.rmtree(run_dir / "buffer", ignore_errors=True)
    for child in run_dir.glob("engine-*/data"):
        shutil.rmtree(child, ignore_errors=True)
    return result


FAILURE_STATE = "failures-state.json"


def run_failures(output_dir, report_dir=None, *, family="s3", faults=None, topologies=None,
                 stores=None, only_cells=None, purposes=None, straddle_cells=(),
                 **options) -> dict:
    """`measure failures`: every requested cell of one family, then its index.

    The family state in the output directory remembers the latest run of
    every cell, so a later invocation can run some cells again; the index
    `failure-<family>.json` lists the latest run of each cell, and a rerun
    names why it was made through `purposes`.
    """
    command = capacity._command()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / FAILURE_STATE
    state = json.loads(state_path.read_text()) if state_path.is_file() else {}
    cells = state.setdefault(family, {})
    reasons = state.setdefault("purposes", {})
    matrix = list(only_cells or (
        f"{fault}-{topology}-{store}" for fault in faults or FAILURE_FAMILIES[family]
        for topology in topologies or FAILURE_TOPOLOGIES
        for store in stores or PREFLIGHT_STORES))
    # A straddling cell waits for an hour end, so it runs after the others.
    matrix.sort(key=lambda cell: cell in straddle_cells)
    for cell in matrix:
        fault, topology, store = cell.rsplit("-", 2)
        previous = cells.get(cell)
        result = failure_case(family, fault, topology, store, output_dir,
                              report_dir=report_dir, straddle_hour=cell in straddle_cells,
                              **options)
        cells[cell] = result["run_id"]
        if previous and (purposes or {}).get(cell):
            reasons[result["run_id"]] = f"replaces {previous}: {purposes[cell]}"
        failed = [entry["name"] for entry in result["checks"] if entry["status"] != "passed"]
        sys.stderr.write(f"{result['run_id']}: {result['status']} failed checks {failed}\n")
        state_path.write_text(json.dumps(state, indent=2, sort_keys=True))
    children = [json.loads((output_dir / f"{run_id}.json").read_text(encoding="ascii"))
                for _cell, run_id in sorted(cells.items())]
    return command.write_index(f"failure-{family}", output_dir, report_dir, children,
                               publishable=True, purposes=reasons)


# --------------------------------------------------------------------------
# Re-judging published fault cases
# --------------------------------------------------------------------------

FAULT_REJUDGE_RULES = {
    "duplicates_explained": "strict: every duplicated record belongs to a resent request; "
    "buffered: every duplicated record has a copy in a values file of a failed block",
    "fault_observed": "store_outage: a flush.failed event of class deadline logged at least "
    "the flush deadline after arming and before the fault was removed",
}


def archived_engine_log(archive_dir, run_id):
    """The engine log a run's raw archive keeps, as text, or None."""
    path = Path(archive_dir) / f"{run_id}.tgz"
    if not path.is_file():
        return None
    with tarfile.open(path) as archive:
        for member in archive.getmembers():
            if member.name.endswith("engine-1/engine.log"):
                return archive.extractfile(member).read().decode(errors="replace")
    return None


def rejudge_fault_checks(result, archive_dir) -> list:
    """Each fault check whose verdict the current rules change, from stored evidence.

    Returns one entry per re-judged check: its recorded and re-judged status
    (None when the stored evidence cannot decide it) and the reason.
    """
    fault = result["observations"]["fault"]
    if fault["fault"] in FAILURE_FAMILIES["process"]:
        return []
    buffered = result["config"]["requested"]["topology"] == "buffered"
    statuses = {entry["name"]: entry["status"] for entry in result["checks"]}
    verdicts = []
    duplicates = dict(fault.get("duplicates") or {})
    if buffered and "duplicated_outside_failed_blocks_records" not in duplicates \
            and duplicates.get("duplicated_records") == 0:
        duplicates["duplicated_outside_failed_blocks_records"] = 0
    if buffered and "duplicated_outside_failed_blocks_records" not in duplicates:
        verdicts.append({"check": "duplicates_explained", "recorded": statuses.get(
            "duplicates_explained"), "rejudged": None,
            "reason": "buffered duplicates were stored without their failed-block copies"})
    else:
        explained, why = duplicates_explained(buffered, duplicates)
        verdicts.append({"check": "duplicates_explained",
                         "recorded": statuses.get("duplicates_explained"),
                         "rejudged": measurement.STATUS_PASSED if explained
                         else measurement.STATUS_FAILED, "reason": why})
    if fault["fault"] == "store_outage":
        states = {state["state"]: state["unix_s"] for state in fault["states"]}
        failures = (fault.get("engine_events") or {}).get("flush_failures")
        source = "the run file"
        if failures is None:
            text = archived_engine_log(archive_dir, result["run_id"])
            failures = flush_failures(text.splitlines()) if text is not None else None
            source = "the archived engine log"
        deadline = duration_s(exporter_settings(result["config"]["effective"])["window"]
                              ["flush_retry_deadline"])
        if failures is None or "armed" not in states:
            verdicts.append({"check": "fault_observed", "recorded": statuses.get(
                "fault_observed"), "rejudged": None, "reason": "no engine log was kept"})
        else:
            end = states.get("fault_removed", float("inf"))
            timed = [round(entry["unix_s"] - states["armed"], 3) for entry in failures
                     if entry["unix_s"] <= end]
            proven = [round(entry["unix_s"] - states["armed"], 3) for entry in failures
                      if entry["error_type"] == "deadline" and entry["unix_s"] <= end
                      and entry["unix_s"] >= states["armed"] + deadline]
            verdicts.append({
                "check": "fault_observed", "recorded": statuses.get("fault_observed"),
                "rejudged": measurement.STATUS_PASSED if proven and statuses.get(
                    "fault_observed") == measurement.STATUS_PASSED
                else measurement.STATUS_FAILED,
                "reason": f"flush failures {timed} s after arming ({source}); a deadline "
                f"failure at or after {deadline} s: {proven or 'none'} before the fault was "
                f"removed at {round(end - states['armed'], 3)} s"})
    return verdicts


def rejudge_failure_index(index_name, output_dir, archive_dir=FAULT_ARCHIVE_DIR,
                          report_dir=None) -> dict:
    """Advance a published fault-case index to the current fault checks.

    Every run file is re-judged from what it stored and its raw archive
    (`rejudge_fault_checks`); nothing is rerun and no run file changes. The
    advanced index records every verdict that changed, every check the
    evidence could not decide and the rules, and keeps the index it replaces
    as an immutable child.
    """
    report_dir = measurement.resolve_report_dir(report_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    index = json.loads((report_dir / measurement.safe_json_name(index_name)).read_text(
        encoding="ascii"))
    changes, unjudged, rejudged_failed = [], [], {}
    for entry in index.get("run_files", []):
        result = json.loads((report_dir / entry["name"]).read_text(encoding="ascii"))
        failed = {check["name"] for check in result["checks"]
                  if check["kind"] == measurement.CHECK_HARD
                  and check["status"] != measurement.STATUS_PASSED}
        for verdict in rejudge_fault_checks(result, archive_dir):
            if verdict["rejudged"] is None:
                unjudged.append(dict(verdict, run_id=result["run_id"]))
                continue
            if verdict["rejudged"] == measurement.STATUS_PASSED:
                failed.discard(verdict["check"])
            else:
                failed.add(verdict["check"])
            if verdict["rejudged"] != verdict["recorded"]:
                changes.append(dict(verdict, run_id=result["run_id"]))
        status = measurement.STATUS_FAILED if failed else measurement.STATUS_PASSED
        rejudged_failed[result["run_id"]] = sorted(failed)
        for change in changes:
            if change["run_id"] == result["run_id"]:
                change["run_status_recorded"] = result["status"]
                change["run_status_rejudged"] = status
    advanced = json.loads(json.dumps(index))
    for child in advanced.get("children", []):
        child["rejudged_failed_checks"] = rejudged_failed.get(child["run_id"])
    advanced["fault_rejudgement"] = {"rules": FAULT_REJUDGE_RULES,
                                     "verdict_changes": changes, "not_rejudged": unjudged}
    for name in {entry["name"] for entry in advanced.get("baseline_files", [])}:
        if not (output_dir / name).is_file():
            _ = shutil.copyfile(report_dir / name, output_dir / name)
    previous = measurement.archive_published_index(index_name, output_dir, report_dir)
    advanced["child_indexes"] = [previous] if previous else []
    advanced["started_utc"] = measurement.utc_now()
    path = measurement.write_result(output_dir / measurement.safe_json_name(index_name),
                                    advanced)
    _ = measurement.publish_result_tree(path, report_dir)
    return advanced
