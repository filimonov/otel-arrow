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
# `completion` carries only the completion front's CompleteMultipartUpload
# requests of values objects (`fault-nginx.conf`).
PROXY_PORTS = {"general": 19001, "values": 19002, "completion": 19003}
# The proxies the main front on NGINX_PORT routes through.
ROUTE_PROXIES = ("general", "values")
COMPLETION_FRONT_PORT = 19010
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
    "disconnect_reset": BASE_PROBES + ("disconnect_reset direct",),
    "dropped_completion_response": BASE_PROBES + ("dropped_completion_response direct",),
    "dns_nxdomain_timeout": BASE_PROBES + ("UDP DNS", "TCP DNS"),
    "tcp_ack_loss": BASE_PROBES + ("xt_bpf", "capture"),
    "containerized_engine": BASE_PROBES + ("engine launch",),
}

# Probes a class needs that no probe implements yet, and what each must show.
DEFERRED_PROBES = {}


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
        "--publish", f"127.0.0.1::{COMPLETION_FRONT_PORT}",
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
    launch; a `(source, target, mode)` mount, such as a run's resolv.conf,
    names its own target. It gets no added capability.
    """
    command = [
        "docker", "run", "--pull=never", "--name", name, "--cidfile", str(cidfile),
        "--label", f"{RUN_LABEL}={run_id}",
        "--network", f"container:{owner}",
        "--user", user,
    ]
    for mount in mounts:
        source, target, mode = mount if len(mount) == 3 else (mount[0], mount[0], mount[1])
        command += ["--volume", f"{source}:{target}:{mode}"]
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
        self.file_mounts = []

    def reserve_ports(self):
        """The (gRPC, admin) ports the owner publishes on host loopback."""
        return self.rig.engine_ports

    def mount(self, path):
        """Also mount `path` read-write, for a directory outside the run root."""
        self.extra_mounts.append(Path(path).resolve())

    def mount_file(self, source, target):
        """Mount one host file read-only at `target` in every later engine."""
        self.file_mounts.append((Path(source).resolve(), target, "ro"))

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
        return mounts + list(self.file_mounts)

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
        self.completion_port = self._published(COMPLETION_FRONT_PORT)
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

    def exec_output(self, argv, *, timeout=120) -> tuple:
        """One command in the owner's namespace: its exit status and its whole
        standard output, which `exec` would clip for the evidence."""
        done = subprocess.run(["docker", "exec", self.owner_id, *argv], capture_output=True,
                              timeout=timeout, check=False)
        return done.returncode, done.stdout.decode(errors="replace"), \
            done.stderr.decode(errors="replace")

    def nginx_error_log(self) -> str:
        """NGINX's error log, which the tools image sends to the owner's stderr."""
        if not self.owner_id:
            return ""
        done = subprocess.run(["docker", "logs", self.owner_id], capture_output=True,
                              timeout=DOCKER_TIMEOUT_S, check=False)
        return done.stderr.decode(errors="replace")

    def route_health(self, client) -> dict:
        """What the store answered through each route of the main front.

        A signed HEAD of the bucket travels the general proxy and a signed
        HEAD of a values key the values proxy; a status from the store,
        404 for the absent key included, is an answer.
        """
        answers = {}
        for route, call in (
                ("general", lambda: client.head_bucket(Bucket=self.store.bucket)),
                ("values", lambda: client.head_object(
                    Bucket=self.store.bucket,
                    Key=f"fault-health/{self.run_id}/dataset=values/health"))):
            try:
                answers[route] = call()["ResponseMetadata"]["HTTPStatusCode"]
            except ClientError as error:
                answers[route] = error.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode")
            except BotoCoreError as error:
                answers[route] = type(error).__name__
        answers["answered"] = (answers["general"] == 200 and answers["values"] in (200, 404))
        return answers

    def route_client(self, *, read_timeout=30, completion_front=False):
        """A signed S3 client that reaches the store through NGINX's main
        front, or through its completion front."""
        return boto3.client(
            "s3",
            endpoint_url=(f"http://127.0.0.1:{self.completion_port}" if completion_front
                          else self.route_endpoint),
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

    def __init__(self, rig, name, expression, *, seconds=60, snaplen=None, interface="any"):
        self.rig = rig
        self.file = f"{ARTIFACT_MOUNT}/{name}.pcap"
        self.host_file = rig.artifact_dir / f"{name}.pcap"
        self.argv = ["docker", "exec", rig.owner_id, "timeout", str(int(seconds)), "tcpdump",
                     "-Z", "root", "-i", interface, "--immediate-mode", "-U", "-n"]
        if snaplen:
            self.argv += ["-s", str(int(snaplen))]
        self.argv += ["-w", self.file, expression]
        self.process = None
        self.stderr = ""

    def __enter__(self):
        self.process = subprocess.Popen(self.argv, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.PIPE)
        deadline = time.monotonic() + 10
        # Read the raw descriptor: a buffered readline can hold the second
        # line where select no longer sees it.
        descriptor = self.process.stderr.fileno()
        while time.monotonic() < deadline and "listening on" not in self.stderr:
            ready, _, _ = select.select([descriptor], [], [], 0.2)
            if ready:
                chunk = os.read(descriptor, 4096).decode(errors="replace")
                if not chunk:
                    break
                self.stderr += chunk
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


def captured_nothing(stderr) -> bool:
    """Whether tcpdump's summary says it wrote no packet (a count of exactly 0)."""
    return re.search(r"(?m)^0 packets captured$", stderr or "") is not None


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
    if captured_nothing(capture.stderr):
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
                    "process": ("graceful_restart", "kill_active", "kill_upload"),
                    "network": ("disconnect", "reset", "dns_nxdomain", "dns_timeout",
                                "tcp_ack_loss", "dropped_completion_response",
                                "dropped_multipart_completion", "held_multipart_completion")}
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
# The same input with only request 0 a metrics request: a block's frozen
# objects are then its logs files alone, so a late commit of its multipart
# values object is the whole block.
LOGS_WORKLOAD = dataclasses.replace(FAULT_WORKLOAD, metrics_every=FAULT_MAX_REQUESTS + 1)
FAULT_WORKLOADS = {"dropped_multipart_completion": LOGS_WORKLOAD,
                   "held_multipart_completion": LOGS_WORKLOAD}
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
                            "graceful_restart": 60, "kill_active": 60, "kill_upload": 120,
                            "disconnect": 240, "reset": 240, "dns_nxdomain": 240,
                            "dns_timeout": 240, "tcp_ack_loss": 60,
                            "dropped_completion_response": 25,
                            "dropped_multipart_completion": 180,
                            "held_multipart_completion": 300}
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


# The retries the exporter's object store client makes by default
# (`retry.max_retries`, aligned with object_store's RetryConfig).
DEFAULT_MAX_RETRIES = 10


def uploads_per_abort_failure(settings) -> int:
    """How many incomplete uploads one counted `flush.abort_failures` may stand for.

    The exporter counts one failure per failed write attempt, but inside an
    attempt the object store client retries a CreateMultipartUpload up to
    `retry.max_retries` times, and the store may create an upload on every try
    whose response was lost (Task 16: a reset CreateMultipartUpload left 54
    uploads behind 9 counted failures with `max_retries: 5`). Each counted
    failure therefore allows `retry.max_retries + 1` uploads.
    """
    retry = (settings or {}).get("retry") or {}
    return int(retry.get("max_retries", DEFAULT_MAX_RETRIES)) + 1


def orphan_verdict(orphans, expected, abort_failures, cleanup=ORPHANS_NOT_CLEANED,
                   uploads_per_failure=1) -> tuple:
    """Whether the incomplete uploads after a case are exactly the expected ones.

    `expected` maps each upload a killed engine left open to its key; every
    one must still be listed under that key. Any other upload is allowed
    only up to `abort_failures`, the aborts the exporter reported as failed,
    times `uploads_per_failure` (`uploads_per_abort_failure`). A case that
    aborts the uploads itself (`abort_uploads`) must leave none.
    """
    if not isinstance(orphans, list):
        return False, f"the uploads could not be listed: {orphans}"
    listed = {entry["upload_id"]: entry["key"] for entry in orphans}
    missing = sorted(upload for upload, key in expected.items() if listed.get(upload) != key)
    unknown = [entry for entry in orphans if entry["upload_id"] not in expected]
    allowed = abort_failures * uploads_per_failure
    unexpected = max(0, len(unknown) - allowed)
    cleaned = cleanup == ORPHANS_NOT_CLEANED or bool((cleanup or {}).get("clean"))
    return (not missing and unexpected == 0 and cleaned,
            f"{len(orphans)} incomplete multipart uploads; {len(expected)} left open by a killed "
            f"engine, of which not listed under their key {missing[:5]}; {abort_failures} "
            f"reported abort failures may each leave up to {uploads_per_failure} "
            f"({allowed}); unexpected {unexpected}: "
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
        case=case, topology=topology, store=store, cores=cores,
        workload=FAULT_WORKLOADS.get(fault, FAULT_WORKLOAD),
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
    (`FAULT_CONDITIONS`); `fault_removed`; `endpoint_healthy`, signed HEADs
    through the general and the values route answered (`FaultRig.route_health`);
    `resumed`, a values file written and a
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
        """Remove the fault, then wait for both routes to answer."""
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
            return {"routes": self.rig.route_health(client),
                    "elapsed_s": (time.monotonic_ns() - removed["monotonic_ns"]) / 1e9}

        evidence = measurement.wait_until(
            observe, lambda seen: seen["routes"]["answered"],
            deadline_ns=removed["monotonic_ns"] + ENDPOINT_HEALTH_DEADLINE_S * 10**9,
            description="signed HEADs through the general and the values route",
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

    def produce(self, producer, stop):
        """The case's input: paced requests until `stop`."""
        return producer.send_paced(range(self.spec.workload.requests),
                                   FAULT_RATE_REQUESTS_PER_S, stop=stop)

    def collect(self, record):
        """What the case reads from the rig before it is removed: nothing."""
        return {}

    def oracle_extras(self, ledger):
        """The case's own read of the loaded oracle rows: none."""
        return None

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


def launch_engine(root, rig, spec, output_dir, binary, storage=None):
    """One containerized release engine of a fault case, in the rig's namespace.

    The launch options are kept on the engine, so `restart_engine` starts
    its successor exactly alike. `storage` defaults to the rig's route.
    """
    options = {
        "storage": storage or rig.storage, "launcher": rig.launcher,
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
        fault = spec.overrides["fault"]
        rig = FaultRig(store, output_dir / "rig", cores=allocation.get("fault_tools") or None,
                       probes=entry_probes(fault))
        with rig:
            setup = NETWORK_SETUP.get(fault)
            storage = setup(rig) if setup else None
            result["config"]["network"] = getattr(rig, "network_setup", None)
            engine = launch_engine(output_dir / "engine-1", rig, spec, output_dir,
                                   provenance["build"]["binary"], storage=storage)
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
                elif fault in FAILURE_FAMILIES["network"]:
                    case = NETWORK_CASES.get(fault, NetworkCase)(
                        spec, rig, store, engine, phase, ledger, lister, settings)
                else:
                    case = FaultCase(spec, rig, store, engine, phase, ledger, lister, settings)
                _ = command.await_window_start(spec.interval_s)
                controls.raise_if_invalid()
                stop = threading.Event()
                sent = {}

                def produce():
                    sent["outcome"] = case.produce(producer, stop)

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
            record["network"] = case.collect(record)
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
        record["case_oracle"] = case.oracle_extras(ledger)
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
    per_failure = uploads_per_abort_failure(case.settings)
    hard("orphaned_uploads_expected", *orphan_verdict(
        orphans, known, abort_failures, record.get("orphan_cleanup", ORPHANS_NOT_CLEANED),
        uploads_per_failure=per_failure))
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
        "flush_late_commits_count": final.get("flush.late_commits.stored", 0)
        + final.get("flush.late_commits.acknowledged", 0),
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
        "orphaned_uploads_expected_max_count": abort_failures * per_failure + len(known),
        "uploads_per_abort_failure": per_failure,
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
                          "too_many_series", "token_too_large", "too_deep", "invalid",
                          "unsupported")
# The checks a process case records beside FAULT_CHECKS.
PROCESS_CHECKS = ("new_boot_id", "restart_same_cores_and_buffer", "prior_acks_durable",
                  "replay_only_eligible", "no_permanent_rejection", "retry_bytes_identical")
BOOT_EVENT = re.compile(r"series_parquet\.start\b.*?\bboot_id=([0-9a-f]+)")
FILE_BOOT = re.compile(r"/part-\d{8}T\d{6}Z-[A-Za-z0-9_.-]+-([0-9a-f]+)-\d+\.parquet$")


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
        """A multipart upload open in the store with part bytes the store itself
        lists (list_parts), and FLUSHING; NGINX's logged bytes are evidence only."""
        return seen["block_flushing_bytes"] > 0 and any(
            (upload.get("part_bytes") or 0) > 0 for upload in seen["open_uploads"])

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
                  if (upload.get("part_bytes") or 0) > 0]
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


# --------------------------------------------------------------------------
# Network, DNS, TCP acknowledgement and completion-response faults
# --------------------------------------------------------------------------

# Toxiproxy bodies of the network faults, in its own units.
RESET_TOXIC = {"name": "reset", "type": "reset_peer", "stream": "downstream",
               "toxicity": 1.0, "attributes": {"timeout": 0}}
DROP_COMPLETION_TOXIC = {"name": "drop_completion", "type": "timeout", "stream": "downstream",
                         "toxicity": 1.0, "attributes": {"timeout": 0}}
# A request held in the proxy for up to ten minutes; removing the toxic
# delivers what it holds.
HOLD_COMPLETION_TOXIC = {"name": "hold_completion", "type": "latency", "stream": "upstream",
                         "toxicity": 1.0, "attributes": {"latency": 600000, "jitter": 0}}
# The DNS timeout's namespace-local rules: every query to any resolver dropped.
DNS_TIMEOUT_RULES = (["OUTPUT", "-p", "udp", "--dport", "53", "-j", "DROP"],
                     ["OUTPUT", "-p", "tcp", "--dport", "53", "-j", "DROP"])


def engine_dns_rules(uid):
    """The same drops for the engine's own queries, matched by its user id ahead
    of the general rules: the engine container runs as the invoking user, every
    tool in the namespace as root, so these counters count only the engine."""
    return tuple(rule[:5] + ["-m", "owner", "--uid-owner", str(int(uid))] + rule[5:]
                 for rule in DNS_TIMEOUT_RULES)
# The DNS cases' resolver: dnsmasq on the namespace's loopback, answering
# only from the run's hosts file, authoritative for `.test`, never cached.
DNS_RESOLV_CONF = "nameserver 127.0.0.1\noptions attempts:1 timeout:1\n"
DNS_HOSTS = "hosts"
DNSMASQ_CASE_PID = "/run/series-dnsmasq-case.pid"
DNSMASQ_CASE_LOG = "dnsmasq-case.log"
# Requests per single-signal cohort: about a megabyte of logs, one PUT. Its
# completed object must show within FAULT_OBSERVE_DEADLINE_S, inside
# object_store's 30 s request timeout.
COHORT_REQUESTS = 10
# The withheld object must answer this many direct HEADs, this far apart,
# unchanged.
COHORT_HEADS = 3
COHORT_HEAD_PERIOD_S = 1.0
# A held completion is released this long past the later of the writer's
# cleanup cutoff and its block's window end plus L.
HELD_RELEASE_MARGIN_S = 10
# How long a released completion may take to publish its object.
RELEASE_VISIBLE_S = 30
# A capture of a network case outlives its fault by this much.
CAPTURE_MARGIN_S = 120
# Headers are enough for the TCP evidence.
CAPTURE_SNAPLEN = 128
# The ACK loss is held past object_store's 30 s request timeout, inside the
# case's 60 s activation deadline.
ACK_LOSS_HOLD_S = 35
# How often a costly observation (tshark, docker logs, iptables) is refreshed.
COSTLY_PERIOD_S = 2.0


def _curl(rig, url, *, max_time=5):
    """One unsigned request from the owner's namespace, as curl saw it.

    curl's exit status names the transport outcome: 0 answered, 7 refused,
    28 timed out, 52 empty reply, 56 reset by peer.
    """
    done = rig.exec(["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", "--max-time",
                     str(max_time), url], timeout=max_time + 15)
    return {"url": url, "exit_status": done["exit_status"], "http_code": done["stdout"].strip(),
            "stderr": done["stderr"].strip()[:200], "elapsed_s": done["elapsed_s"]}


CURL_OUTCOMES = {0: "answered", 7: "refused", 28: "timed_out", 52: "empty_reply", 56: "reset"}


def curl_outcome(entry) -> str:
    """The transport outcome of one `_curl` record."""
    return CURL_OUTCOMES.get(entry.get("exit_status"), f"exit_{entry.get('exit_status')}")


def _add_toxics(rig, placements):
    """Add each (proxy, toxic) and require every proxy to carry exactly its own."""
    for proxy, toxic in placements:
        rig.toxiproxy.add_toxic(proxy, toxic)
    proxies = sorted({proxy for proxy, _toxic in placements})
    state = {proxy: rig.toxiproxy.toxics(proxy) for proxy in proxies}
    for proxy, present in state.items():
        wanted = sorted(toxic["name"] for name, toxic in placements if name == proxy)
        if sorted(entry["name"] for entry in present) != wanted:
            raise AssertionError(f"proxy {proxy} carries toxics {present} after activation")
    return {"placements": [[proxy, toxic] for proxy, toxic in placements], "api_state": state,
            "units": TOXIC_UNITS}


def _remove_toxics(rig, state):
    """Remove exactly the toxics `_add_toxics` placed."""
    for proxy, toxic in state["placements"]:
        rig.toxiproxy.remove_toxic(proxy, toxic["name"])
    remaining = {proxy: rig.toxiproxy.toxics(proxy)
                 for proxy in sorted({proxy for proxy, _toxic in state["placements"]})}
    if any(remaining.values()):
        raise AssertionError(f"toxics remain after recovery: {remaining}")
    return {"api_state": remaining, "removed_unix_s": time.time()}


def _toxic_fault(placements):
    """A registered fault that adds `placements` and removes them again."""
    return (lambda rig, parameters: _add_toxics(rig, placements), _remove_toxics)


def _activate_disconnect(rig, parameters):
    """Disable both routes' proxies: their listeners close and refuse."""
    for proxy in ROUTE_PROXIES:
        rig.toxiproxy.update(proxy, {"enabled": False})
    enabled = {name: entry.get("enabled") for name, entry in rig.toxiproxy.proxies().items()
               if name in ROUTE_PROXIES}
    if any(enabled.values()):
        raise AssertionError(f"proxies still enabled after the disconnect: {enabled}")
    return {"proxies": list(ROUTE_PROXIES), "enabled_after": enabled}


def _recover_disconnect(rig, state):
    """Enable both proxies again."""
    for proxy in state["proxies"]:
        rig.toxiproxy.update(proxy, {"enabled": True})
    enabled = {name: entry.get("enabled") for name, entry in rig.toxiproxy.proxies().items()
               if name in state["proxies"]}
    if not all(enabled.values()):
        raise AssertionError(f"proxies not enabled after recovery: {enabled}")
    return {"enabled_after": enabled, "removed_unix_s": time.time()}


def dig(rig, name, *, timeout_s=1):
    """One diagnostic lookup of `name` from the owner's namespace: its status and answer."""
    done = rig.exec(["dig", "@127.0.0.1", "-p", "53", f"+time={timeout_s}", "+tries=1",
                     name, "A"], timeout=30)
    status = re.search(r"status: ([A-Z]+)", done["stdout"])
    answer = re.findall(rf"^{re.escape(name)}\.\s+\d+\s+IN\s+A\s+(\S+)", done["stdout"], re.M)
    return {"exit_status": done["exit_status"], "status": status[1] if status else None,
            "answers": answer, "elapsed_s": done["elapsed_s"],
            "timed_out": done["exit_status"] == 9}


def write_dns_hosts(rig, present):
    """The run's hosts file: the endpoint name mapped to NGINX, or nothing."""
    (rig.control_dir / DNS_HOSTS).write_text(f"127.0.0.1 {rig.dns_name}\n" if present else "")


def reload_dnsmasq(rig):
    """Make the case's dnsmasq read its hosts file again (SIGHUP)."""
    done = rig.exec(["sh", "-c", f"kill -HUP \"$(cat {DNSMASQ_CASE_PID})\""])
    if done["exit_status"] != 0:
        raise AssertionError(f"dnsmasq could not be reloaded: {done}")
    return done


def reset_front_connections(rig):
    """Reset every established connection to NGINX's fronts in the namespace.

    NGINX closes each client connection after one response
    (`keepalive_timeout 0`), so none should exist; any that does is killed
    with `ss -K`, which forces the engine to open, and resolve, a new one.
    """
    selector = ["state", "established", f"( dport = :{NGINX_PORT} or dport = "
                f":{COMPLETION_FRONT_PORT} )"]
    before = rig.exec(["ss", "-tnH", *selector])
    killed = rig.exec(["ss", "-K", "-tnH", *selector]) if before["stdout"].strip() else None
    return {"established_before": before["stdout"].splitlines(),
            "killed": killed and {key: killed[key] for key in ("exit_status", "stdout", "stderr")}}


def _activate_dns_nxdomain(rig, parameters):
    """Remove the endpoint's name and reload: the resolver answers NXDOMAIN."""
    write_dns_hosts(rig, False)
    reload_dnsmasq(rig)
    lookup = dig(rig, rig.dns_name)
    if lookup["status"] != "NXDOMAIN":
        raise AssertionError(f"the endpoint name still resolves: {lookup}")
    return {"name": rig.dns_name, "dig": lookup, "connections": reset_front_connections(rig)}


def _recover_dns_nxdomain(rig, state):
    """Restore the name and reload; it must resolve to NGINX again."""
    write_dns_hosts(rig, True)
    reload_dnsmasq(rig)
    lookup = dig(rig, rig.dns_name)
    if lookup["answers"] != ["127.0.0.1"]:
        raise AssertionError(f"the endpoint name does not resolve after recovery: {lookup}")
    return {"dig": lookup, "removed_unix_s": time.time()}


def dns_rule_counts(listing) -> dict:
    """Packets the DNS timeout rules dropped, from `iptables -v -S OUTPUT`: the
    general rules as `udp`/`tcp`, the engine's user-matched ones as
    `engine_udp`/`engine_tcp` (None when a rule is absent or ambiguous)."""
    found = {}
    for line in listing.splitlines():
        protocol = re.search(r"-p (udp|tcp)\b", line)
        packets = re.search(r"-c (\d+) \d+", line)
        if not (line.startswith("-A OUTPUT") and protocol and packets and "--dport 53" in line
                and "-j DROP" in line):
            continue
        name = ("engine_" if "--uid-owner" in line else "") + protocol[1]
        found[name] = None if name in found else int(packets[1])
    return {name: found.get(name) for name in ("udp", "tcp", "engine_udp", "engine_tcp")}


def dns_rule_counters(rig):
    """Packets each DNS timeout rule dropped so far (`dns_rule_counts`)."""
    return dns_rule_counts(rig.exec(["iptables", "-w", "-v", "-S", "OUTPUT"])["stdout"])


def _activate_dns_timeout(rig, parameters):
    """Drop every DNS query leaving any process in the namespace, the engine's
    through rules of its own user id so they are counted apart."""
    rules = [list(rule) for rule in DNS_TIMEOUT_RULES] + [
        list(rule) for rule in engine_dns_rules(os.getuid())]
    # Each insertion goes first, so the engine's rules end up ahead of the general ones.
    for rule in rules:
        done = rig.exec(["iptables", "-w", "-I", rule[0], "1", *rule[1:]])
        if done["exit_status"] != 0:
            raise AssertionError(f"the DNS rule {rule} could not be inserted: {done['stderr']}")
    return {"rules": rules, "engine_uid": os.getuid(),
            "connections": reset_front_connections(rig)}


def _recover_dns_timeout(rig, state):
    """Delete exactly those rules; the name must resolve again."""
    counters = dns_rule_counters(rig)
    for rule in state["rules"]:
        done = rig.exec(["iptables", "-w", "-D", *rule])
        if done["exit_status"] != 0:
            raise AssertionError(f"the DNS rule {rule} could not be deleted: {done['stderr']}")
    lookup = dig(rig, rig.dns_name)
    if lookup["answers"] != ["127.0.0.1"]:
        raise AssertionError(f"the endpoint name does not resolve after recovery: {lookup}")
    return {"counters_at_removal": counters, "dig": lookup, "removed_unix_s": time.time()}


def ack_rule(bytecode):
    """The INPUT rule dropping what the ACK-only program matches."""
    return ["INPUT", "-p", "tcp", "-m", "bpf", "--bytecode", bytecode, "-j", "DROP"]


def ack_rule_packets(rig):
    """Packets the ACK-only rule dropped so far, or None when it is absent."""
    listing = rig.exec(["iptables", "-w", "-v", "-S", "INPUT"])["stdout"]
    return rule_counters(listing, ["-m bpf", "-j DROP"]).get("packets")


def _activate_tcp_ack_loss(rig, parameters):
    """Drop the store's pure ACKs on this run's store connection with xt_bpf."""
    bytecode = ack_drop_bytecode(rig.store_ip, STORE_PORT,
                                 tcpdump=("docker", "exec", rig.owner_id, "tcpdump"))
    rule = ack_rule(bytecode)
    done = rig.exec(["iptables", "-w", "-I", "INPUT", "1", *rule[1:]])
    if done["exit_status"] != 0:
        raise AssertionError(f"the ACK-only rule could not be inserted: {done['stderr']}")
    return {"rule": rule, "bytecode": bytecode, "store_ip": rig.store_ip,
            "store_port": STORE_PORT, "expression": ack_only_expression(rig.store_ip, STORE_PORT)}


def _recover_tcp_ack_loss(rig, state):
    """Delete exactly that rule, reading its counter first."""
    packets = ack_rule_packets(rig)
    done = rig.exec(["iptables", "-w", "-D", *state["rule"]])
    if done["exit_status"] != 0:
        raise AssertionError(f"the ACK-only rule could not be deleted: {done['stderr']}")
    return {"dropped_packets_at_removal": packets, "removed_unix_s": time.time()}


register_fault("disconnect", _activate_disconnect, _recover_disconnect)
register_fault("reset", *_toxic_fault([(proxy, RESET_TOXIC) for proxy in ROUTE_PROXIES]))
register_fault("dns_nxdomain", _activate_dns_nxdomain, _recover_dns_nxdomain)
register_fault("dns_timeout", _activate_dns_timeout, _recover_dns_timeout)
register_fault("tcp_ack_loss", _activate_tcp_ack_loss, _recover_tcp_ack_loss)
register_fault("dropped_completion_response", *_toxic_fault([("values", DROP_COMPLETION_TOXIC)]))
register_fault("dropped_multipart_completion",
               *_toxic_fault([("completion", DROP_COMPLETION_TOXIC)]))
register_fault("held_multipart_completion", *_toxic_fault([("completion", HOLD_COMPLETION_TOXIC)]))


# NGINX error log lines by how the upstream connection failed.
NGINX_ERROR_CLASSES = (
    ("refused", re.compile(r"\(111: Connection refused\)")),
    ("reset", re.compile(r"\(104: Connection reset by peer\)")),
    ("prematurely_closed", re.compile(r"upstream prematurely closed")),
    ("timed_out", re.compile(r"\(110: Connection timed out\)|upstream timed out")),
)
NGINX_ERROR_TIME = re.compile(r"^(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d) \[(\w+)\]")


def nginx_error_classes(text, since_unix_s=None, until_unix_s=None) -> dict:
    """NGINX error log lines counted by upstream failure class, in a window.

    The log's timestamps are UTC at second resolution, so the window is
    widened to whole seconds.
    """
    counts = collections.Counter()
    examples = {}
    for line in text.splitlines():
        stamp = NGINX_ERROR_TIME.match(line)
        if not stamp:
            continue
        instant = datetime.datetime.strptime(stamp[1], "%Y/%m/%d %H:%M:%S").replace(
            tzinfo=datetime.timezone.utc).timestamp()
        if since_unix_s is not None and instant < int(since_unix_s):
            continue
        if until_unix_s is not None and instant > until_unix_s + 1:
            continue
        label = next((name for name, pattern in NGINX_ERROR_CLASSES if pattern.search(line)),
                     "other")
        counts[label] += 1
        examples.setdefault(label, line[:300])
    return {"counts": dict(sorted(counts.items())), "examples": examples}


DNSMASQ_LINE = re.compile(
    r"^(\w{3} [ \d]\d \d\d:\d\d:\d\d) dnsmasq\[\d+\]: (query\[(\w+)\] (\S+) from \S+|"
    r"(?:config|\S+) (\S+) is (\S+))")


def dnsmasq_queries(text, name, year) -> list:
    """Every query for `name` and every answer dnsmasq logged, with its time."""
    found = []
    for line in text.splitlines():
        match = DNSMASQ_LINE.match(line)
        if not match:
            continue
        instant = datetime.datetime.strptime(f"{year} {match[1]}", "%Y %b %d %H:%M:%S").replace(
            tzinfo=datetime.timezone.utc).timestamp()
        if match[4] == name:
            found.append({"unix_s": instant, "kind": "query", "type": match[3]})
        elif match[5] == name:
            found.append({"unix_s": instant, "kind": "answer", "answer": match[6]})
    return found


def dns_evidence(entries, since_unix_s, until_unix_s=None) -> dict:
    """Queries and answers for the endpoint within a window, whole seconds wide."""
    chosen = [entry for entry in entries if entry["unix_s"] >= int(since_unix_s)
              and (until_unix_s is None or entry["unix_s"] <= until_unix_s + 1)]
    return {"queries_count": sum(1 for entry in chosen if entry["kind"] == "query"),
            "nxdomain_answers_count": sum(1 for entry in chosen if entry["kind"] == "answer"
                                          and entry["answer"] == "NXDOMAIN"),
            "address_answers_count": sum(1 for entry in chosen if entry["kind"] == "answer"
                                         and entry["answer"] != "NXDOMAIN"
                                         and not entry["answer"].startswith("NODATA"))}


def fault_dns_evidence(entries, armed_unix_s) -> dict:
    """The resolver's queries and answers for the endpoint after a DNS fault was armed.

    The resolver logs whole seconds, so only seconds wholly after the arming
    instant count; `engine_queries_count` counts AAAA queries, which glibc
    sends beside A and the diagnostic `dig` never does.
    """
    since = int(armed_unix_s) + 1
    evidence = dns_evidence(entries, since)
    evidence["engine_queries_count"] = sum(
        1 for entry in entries if entry["kind"] == "query" and entry["type"] == "AAAA"
        and entry["unix_s"] >= since)
    return evidence


def pcap_count(rig, capture_file, display_filter=None) -> int:
    """Frames of one capture tshark reads, optionally only those a filter keeps.

    The capture may still be written; tshark's complaint about a cut-off
    last frame does not discard the frames it did read.
    """
    argv = ["tshark", "-n", "-r", capture_file, "-T", "fields", "-e", "frame.number"]
    if display_filter:
        argv[4:4] = ["-Y", display_filter]
    status, stdout, stderr = rig.exec_output(argv)
    if status not in (0, 2) and not stdout.strip():
        raise AssertionError(f"tshark could not read {capture_file}: {stderr[:300]}")
    return len(stdout.split())


def ack_capture_evidence(rig, capture, expression, store_ip) -> dict:
    """What a capture of the store connection shows about the ACK-only rule.

    The capture is filtered again with the rule's exact expression; every
    frame that expression keeps must be a pure ACK (TCP payload length 0,
    flags exactly ACK), so no data-bearing segment counts as one.
    Retransmissions are tshark's `tcp.analysis.retransmission`.
    """
    matched = f"{capture.file}.matched"
    filtered = rig.exec(["tcpdump", "-n", "-r", capture.file, "-w", matched, expression],
                        timeout=120)
    evidence = {"filter_exit_status": filtered["exit_status"]}
    if filtered["exit_status"] != 0:
        evidence["error"] = filtered["stderr"][:300]
        return evidence
    pure = "tcp.len == 0 && tcp.flags == 0x010"
    evidence.update({
        "captured_packets_count": pcap_count(rig, capture.file),
        "matched_packets_count": pcap_count(rig, matched),
        "matched_pure_ack_packets_count": pcap_count(rig, matched, pure),
        "store_pure_ack_packets_count": pcap_count(
            rig, capture.file, f"ip.src == {store_ip} && tcp.srcport == {STORE_PORT} && {pure}"),
        "store_data_packets_count": pcap_count(
            rig, capture.file, f"ip.src == {store_ip} && tcp.srcport == {STORE_PORT} "
            "&& tcp.len > 0"),
        "tcp_retransmissions_count": pcap_count(rig, capture.file,
                                                "tcp.analysis.retransmission"),
    })
    return evidence


def ack_loss_evidence_problems(dropped, evidence) -> list:
    """Why a TCP ACK loss case did not prove its fault; empty when it did.

    The rule must have dropped packets, the capture must hold retransmissions,
    and every captured frame the rule's expression matches must be a pure ACK,
    so no data-bearing segment is taken for one. A writer that drained
    afterwards proves nothing about the fault.
    """
    problems = []
    if not isinstance(dropped, int) or dropped <= 0:
        problems.append(f"the ACK-only rule dropped no packets: {dropped}")
    matched = evidence.get("matched_packets_count")
    if not isinstance(matched, int) or matched <= 0:
        problems.append(f"the capture holds no frame the rule's expression matches: {matched}")
    elif evidence.get("matched_pure_ack_packets_count") != matched:
        problems.append(f"{matched - (evidence.get('matched_pure_ack_packets_count') or 0)} of "
                        f"{matched} matched frames are not pure ACKs")
    retransmissions = evidence.get("tcp_retransmissions_count")
    if not isinstance(retransmissions, int) or retransmissions <= 0:
        problems.append(f"no TCP retransmission was captured: {retransmissions}")
    return problems


def block_window_end(key, interval_s):
    """The end of the window a part file's name stamps, in Unix seconds, or None."""
    match = re.search(r"/part-(\d{8}T\d{6}Z)-", "/" + key)
    if not match:
        return None
    start = datetime.datetime.strptime(match[1], "%Y%m%dT%H%M%SZ").replace(
        tzinfo=datetime.timezone.utc).timestamp()
    return start + interval_s


def block_lateness(objects, interval_s, bound_s) -> dict:
    """Each object's visibility after its own window ended, against the bound.

    L bounds how long after a block's window closed its writer may still
    publish it; the partition hour check is this rule at the hour's last
    window. Visibility is the later of the store's LastModified and the
    first direct listing that showed the object.
    """
    late = []
    worst = None
    for item in objects:
        end = block_window_end(item["key"], interval_s)
        if end is None:
            continue
        seen = [value for value in (item.get("last_modified_unix_s"),
                                    item.get("first_listed_unix_s")) if value is not None]
        if not seen:
            continue
        after = round(max(seen) - end, 3)
        worst = after if worst is None or after > worst else worst
        if after > bound_s:
            late.append({"key": item["key"], "after_window_end_s": after})
    return {"bound_s": bound_s, "max_after_window_end_s": worst,
            "beyond_bound": sorted(late, key=lambda entry: -entry["after_window_end_s"])[:20],
            "beyond_bound_count": len(late)}


def outage_stall_verdict(samples) -> dict:
    """Whether a stall after a store's return lies in the rig or in the store.

    `samples[0]` holds the requests sent together right after the store came
    back: straight to the store from the namespace and from the host,
    through each proxy and through NGINX's two routes. A stalled proxy path
    while the store itself answers is the rig; a store that does not answer
    either is the store; nothing stalled is no stall.
    """
    if not samples:
        return {"decided": False, "stall": None, "stalled_paths": [], "direct_answered": None}
    first = samples[0]
    direct = (curl_outcome(first.get("bypass_namespace") or {}) == "answered"
              and first.get("bypass_host") == 200)
    stalled = [name for name in ("general_proxy", "values_proxy")
               if curl_outcome(first.get(name) or {}) != "answered"]
    stalled += [name for name in ("route_general", "route_values")
                if first.get(name) not in (200, 404)]
    stall = None if not stalled else "rig" if direct else "store"
    return {"decided": True, "stall": stall, "stalled_paths": stalled, "direct_answered": direct}


# Stores that came back about 80 s after the stop stalled the route's writes,
# those back after 140 s did not (`failure-s3.json`).
OUTAGE_STALL_RETURN_S = 80
OUTAGE_STALL_WATCH_S = 200
OUTAGE_STALL_PERIOD_S = 5


def proxy_sockets(rig) -> dict:
    """The namespace's view of the proxies' sockets: each listener's accept
    queue (connections the kernel accepted that the proxy has not taken yet)
    and every half-open (SYN-SENT) connection to the store."""
    listening = rig.exec(["ss", "-ltnH"])["stdout"].splitlines()
    queues = {}
    for proxy, port in PROXY_PORTS.items():
        for line in listening:
            fields = line.split()
            if len(fields) >= 4 and fields[3].endswith(f":{port}"):
                queues[proxy] = int(fields[1])
    return {"accept_queue": queues,
            "syn_sent": rig.exec(["ss", "-tnH", "state", "syn-sent"])["stdout"].splitlines()}


def probe_outage_stall(rig) -> dict:
    """Is the stall after a store returned early the rig's or the store's?

    Stop the store and, as the exporter does, keep opening connections
    through the values proxy (one every OUTAGE_STALL_PERIOD_S, each given
    up after 3 s), recording the proxies' accept queues and half-open
    connections. Bring the store back after OUTAGE_STALL_RETURN_S, then,
    every OUTAGE_STALL_PERIOD_S until both routes answer, send one request
    straight to the store from the namespace and from the host, one through
    each proxy and one through each NGINX route (`outage_stall_verdict`).
    """
    probe = new_probe("outage stall", rig)
    started = time.monotonic()
    problems = []
    samples = []
    during = []
    client = rig.route_client(read_timeout=4)
    try:
        rig.activate("store_outage", {})
    except AssertionError as error:
        return finish(probe, started, False, str(error))
    stopped = time.monotonic()
    try:
        while time.monotonic() - stopped < OUTAGE_STALL_RETURN_S:
            began = time.monotonic()
            during.append(dict(proxy_sockets(rig), since_stop_s=round(began - stopped, 3),
                               values_proxy=_curl(rig, f"http://127.0.0.1:{PROXY_PORTS['values']}/",
                                                  max_time=3)))
            time.sleep(max(0.0, min(OUTAGE_STALL_PERIOD_S - (time.monotonic() - began),
                                    OUTAGE_STALL_RETURN_S - (time.monotonic() - stopped))))
    finally:
        rig.recover()
    returned_s = round(time.monotonic() - stopped, 3)
    while time.monotonic() - stopped < OUTAGE_STALL_WATCH_S:
        began = time.monotonic()
        sample = dict(proxy_sockets(rig), since_stop_s=round(began - stopped, 3),
                      bypass_namespace=_curl(rig, f"http://{rig.store_ip}:{STORE_PORT}/",
                                             max_time=4))
        try:
            sample["bypass_host"] = rig.store.client.head_bucket(Bucket=rig.store.bucket)[
                "ResponseMetadata"]["HTTPStatusCode"]
        except (ClientError, BotoCoreError) as error:
            sample["bypass_host"] = type(error).__name__
        for name, proxy in (("general_proxy", "general"), ("values_proxy", "values")):
            sample[name] = _curl(rig, f"http://127.0.0.1:{PROXY_PORTS[proxy]}/", max_time=4)
        routes = rig.route_health(client)
        sample["route_general"], sample["route_values"] = routes["general"], routes["values"]
        samples.append(sample)
        if curl_outcome(sample["values_proxy"]) == "answered" and routes["answered"]:
            break
        time.sleep(max(0.0, OUTAGE_STALL_PERIOD_S - (time.monotonic() - began)))
    verdict = outage_stall_verdict(samples)
    answered = next((sample["since_stop_s"] for sample in samples
                     if curl_outcome(sample["values_proxy"]) == "answered"), None)
    probe["evidence"] = {
        "during_outage": during, "returned_after_stop_s": returned_s, "samples": samples,
        "verdict": verdict, "values_proxy_answered_after_stop_s": answered,
        "activation": rig.activations[-1],
    }
    if not verdict["decided"]:
        problems.append("no request was sent after the store returned")
    if answered is None:
        problems.append(f"the values proxy never answered within {OUTAGE_STALL_WATCH_S} s")
    return finish(probe, started, not problems, "; ".join(problems) or (
        f"stall: {verdict['stall']}; stalled paths {verdict['stalled_paths']} while the store "
        f"answered directly: {verdict['direct_answered']}; the values proxy answered "
        f"{answered} s after the stop (store back at {returned_s} s)"))


def _stored(store, key):
    """The bytes the store holds under `key`, read directly, or None."""
    try:
        return store.client.get_object(Bucket=store.bucket, Key=key)["Body"].read()
    except (ClientError, BotoCoreError):
        return None


def probe_disconnect_reset(rig) -> dict:
    """A disabled proxy refuses and a reset toxic resets, while the store answers.

    Through each route's proxy a direct connection is refused (disconnect)
    or reset by peer (reset), NGINX answers the signed PUT through the route
    502 and logs the upstream failure by class, and the same request
    straight to the store (the negative control) is answered throughout.
    """
    probe = new_probe("disconnect_reset direct", rig)
    started = time.monotonic()
    problems = []
    client = rig.route_client(read_timeout=10)
    bucket = rig.store.bucket
    key = f"fault-preflight/{rig.run_id}/dataset=values/disconnect-reset"
    seen = {}
    for fault, wanted in (("disconnect", "refused"), ("reset", "reset")):
        since = time.time()
        try:
            rig.activate(fault, {})
        except AssertionError as error:
            return finish(probe, started, False, str(error))
        try:
            direct = {proxy: _curl(rig, f"http://127.0.0.1:{PROXY_PORTS[proxy]}/")
                      for proxy in ROUTE_PROXIES}
            _s3_op(probe, f"put_under_{fault}", lambda: client.put_object(
                Bucket=bucket, Key=key, Body=fault.encode() * 512), key=key)
            route_status = probe["operations"][-1].get("status")
            bypass = _curl(rig, f"http://{rig.store_ip}:{STORE_PORT}/")
        finally:
            rig.recover()
        time.sleep(0.3)
        errors = nginx_error_classes(rig.nginx_error_log(), since)
        seen[fault] = {"direct": direct, "route_status": route_status, "bypass": bypass,
                       "stored_under_fault": _stored(rig.store, key) == fault.encode() * 512,
                       "nginx_errors": errors,
                       "after": {proxy: _curl(rig, f"http://127.0.0.1:{PROXY_PORTS[proxy]}/")
                                 for proxy in ROUTE_PROXIES}}
        for proxy, entry in direct.items():
            if curl_outcome(entry) != wanted:
                problems.append(f"{fault}: the {proxy} proxy was {curl_outcome(entry)}, "
                                f"expected {wanted}")
        if route_status != 502:
            problems.append(f"{fault}: NGINX answered {route_status}, expected 502")
        if curl_outcome(bypass) != "answered":
            problems.append(f"{fault}: the store itself was {curl_outcome(bypass)}")
        if not errors["counts"].get(wanted):
            problems.append(f"{fault}: NGINX logged no {wanted} upstream: {errors['counts']}")
        for proxy, entry in seen[fault]["after"].items():
            if curl_outcome(entry) != "answered":
                problems.append(f"{fault}: the {proxy} proxy did not answer after recovery")
    ok, _ = _s3_op(probe, "put_restored", lambda: client.put_object(
        Bucket=bucket, Key=key, Body=b"restored"), key=key)
    if not ok:
        problems.append("the signed PUT failed after recovery")
    _s3_op(probe, "delete_object", lambda: rig.store.client.delete_object(Bucket=bucket, Key=key))
    probe["evidence"] = seen
    probe["restored"] = rig.residual_state()
    if not probe["restored"]["clean"]:
        problems.append("the rig is not clean after the probe")
    return finish(probe, started, not problems, "; ".join(problems) or (
        "disabled proxies refused and reset toxics reset both routes while the store "
        "answered directly; NGINX answered 502 and logged refused and reset upstreams"))


def probe_dropped_completion(rig) -> dict:
    """A completion response withheld while the object is complete in the store.

    An untoxicated control PUT is answered; under `dropped_completion_response`
    a values PUT gets no response while the store directly holds its exact
    bytes, and removing the toxic closes the waiting connection (NGINX 502).
    Through the completion front, a CompleteMultipartUpload under
    `dropped_multipart_completion` gets no response while the store holds the
    completed object, and one held by `held_multipart_completion` publishes
    nothing until its client has given up and the toxic is removed.
    """
    probe = new_probe("dropped_completion_response direct", rig)
    started = time.monotonic()
    problems = []
    bucket = rig.store.bucket
    base = f"fault-preflight/{rig.run_id}/dataset=values/completion"
    body = _payload(PROBE_TRANSFER_BYTES, base)
    evidence = {}
    control = rig.route_client(read_timeout=10)
    ok, _ = _s3_op(probe, "control_put", lambda: control.put_object(
        Bucket=bucket, Key=base + "-control", Body=body), key=base + "-control")
    evidence["control_put"] = probe["operations"][-1]
    if not ok:
        problems.append("the untoxicated control PUT failed")
    rig.activate("dropped_completion_response", {})
    waiting = {}

    def withheld_put():
        slow = rig.route_client(read_timeout=60)
        _s3_op(probe, "withheld_put", lambda: slow.put_object(
            Bucket=bucket, Key=base + "-withheld", Body=body), key=base + "-withheld")
        waiting["operation"] = probe["operations"][-1]

    thread = threading.Thread(target=withheld_put, name="withheld-put")
    try:
        thread.start()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and _stored(rig.store, base + "-withheld") != body:
            time.sleep(0.1)
        evidence["stored_while_withheld"] = _stored(rig.store, base + "-withheld") == body
        evidence["waiting_while_stored"] = thread.is_alive()
    finally:
        rig.recover()
    thread.join(30)
    evidence["withheld_put"] = waiting.get("operation")
    if not evidence["stored_while_withheld"] or not evidence["waiting_while_stored"]:
        problems.append(f"the PUT was not stored while its response was withheld: {evidence}")
    if (waiting.get("operation") or {}).get("status") != 502:
        problems.append(f"removing the toxic did not close the waiting PUT with 502: "
                        f"{waiting.get('operation')}")
    parts = [_payload(MULTIPART_PART_BYTES, base + "-1"), _payload(4096, base + "-2")]
    for fault, key in (("dropped_multipart_completion", base + "-multipart-dropped"),
                       ("held_multipart_completion", base + "-multipart-held")):
        front = rig.route_client(read_timeout=5, completion_front=True)
        made, upload = _s3_op(probe, f"{fault}_create", lambda: front.create_multipart_upload(
            Bucket=bucket, Key=key), key=key)
        etags = []
        for number, part in enumerate(parts, 1):
            if made:
                done, answer = _s3_op(probe, f"{fault}_part_{number}", lambda: front.upload_part(
                    Bucket=bucket, Key=key, UploadId=upload["UploadId"], PartNumber=number,
                    Body=part), bytes=len(part))
                made &= done
                if done:
                    etags.append({"ETag": answer["ETag"], "PartNumber": number})
        if not made:
            problems.append(f"{fault}: the upload could not be prepared")
            continue
        rig.activate(fault, {})
        try:
            answered, _ = _s3_op(probe, f"{fault}_complete", lambda: front.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload["UploadId"],
                MultipartUpload={"Parts": etags}), key=key)
            gave_up = time.time()
            time.sleep(2)
            present = _stored(rig.store, key)
        finally:
            rig.recover()
        released = time.time()
        visible = None
        while time.time() - released < 10:
            if _stored(rig.store, key) == b"".join(parts):
                visible = round(time.time() - released, 3)
                break
            time.sleep(0.1)
        evidence[fault] = {"answered": answered, "present_after_client_gave_up":
                           present == b"".join(parts), "client_gave_up_unix_s": gave_up,
                           "visible_after_removal_s": visible}
        if answered:
            problems.append(f"{fault}: the completion was answered")
        if fault == "dropped_multipart_completion" and present != b"".join(parts):
            problems.append(f"{fault}: the completed object was not in the store")
        if fault == "held_multipart_completion" and (present is not None or visible is None):
            problems.append(f"{fault}: the held completion published before its release "
                            f"({present is not None}) or never ({visible})")
    ok, _ = _s3_op(probe, "control_put_after", lambda: control.put_object(
        Bucket=bucket, Key=base + "-control", Body=body), key=base + "-control")
    if not ok:
        problems.append("the control PUT failed after recovery")
    time.sleep(0.3)
    evidence["route_log"] = [entry for entry in parse_access_log(rig.artifact_dir / ACCESS_LOG)
                             if base in entry.get("uri", "")]
    for suffix in ("-control", "-withheld", "-multipart-dropped", "-multipart-held"):
        _s3_op(probe, "delete_object", lambda: rig.store.client.delete_object(
            Bucket=bucket, Key=base + suffix))
    probe["evidence"] = evidence
    probe["restored"] = rig.residual_state()
    if not probe["restored"]["clean"]:
        problems.append("the rig is not clean after the probe")
    return finish(probe, started, not problems, "; ".join(problems) or (
        "withheld completions left complete objects in the store, removal closed the waiting "
        "PUT with 502, and a held completion published only after its release"))


PROBES.update({"disconnect_reset direct": probe_disconnect_reset,
               "dropped_completion_response direct": probe_dropped_completion,
               "outage stall": probe_outage_stall})
PREFLIGHT_PROBES = PREFLIGHT_PROBES[:-1] + (
    "disconnect_reset direct", "dropped_completion_response direct", "outage stall",
) + PREFLIGHT_PROBES[-1:]

# The probes a network case's rig runs on entry, beside the route and
# capability probes, before any traffic.
NETWORK_ENTRY_PROBES = {
    "disconnect": ("disconnect_reset direct",),
    "reset": ("disconnect_reset direct",),
    "dns_nxdomain": ("UDP DNS", "TCP DNS"),
    "dns_timeout": ("UDP DNS", "TCP DNS"),
    "tcp_ack_loss": ("xt_bpf", "capture"),
    "dropped_completion_response": ("dropped_completion_response direct",),
    "dropped_multipart_completion": ("dropped_completion_response direct",),
    "held_multipart_completion": ("dropped_completion_response direct",),
}


def entry_probes(fault) -> tuple:
    """The rig's entry probes for one fault: the defaults, the fault's own, the residual check."""
    defaults = FaultRig.DEFAULT_PROBES
    return defaults[:-1] + NETWORK_ENTRY_PROBES.get(fault, ()) + defaults[-1:]


def dns_setup(rig) -> dict:
    """The DNS cases' resolver and the engine's endpoint name; returns its storage.

    The probe resolver is stopped, the case's dnsmasq started with the run's
    hosts file mapping `lake-<run>.test` to NGINX, the engine given a
    read-only resolv.conf naming only that resolver, and the name must
    resolve before any traffic.
    """
    rig.exec(["sh", "-c", "pkill -x dnsmasq; true"])
    rig.dnsmasq = None
    rig.dns_name = f"lake-{rig.run_id}.test"
    write_dns_hosts(rig, True)
    daemon = rig.exec([
        "dnsmasq", "--conf-file=/dev/null", "--user=root", "--no-resolv", "--no-hosts",
        "--bind-interfaces", f"--listen-address={DNS_LISTEN}", "--port=53", "--log-queries",
        "--local=/test/", f"--addn-hosts={CONTROL_MOUNT}/{DNS_HOSTS}", "--local-ttl=0",
        f"--log-facility={ARTIFACT_MOUNT}/{DNSMASQ_CASE_LOG}", f"--pid-file={DNSMASQ_CASE_PID}",
    ])
    if daemon["exit_status"] != 0:
        raise AssertionError(f"the case's dnsmasq did not start: {daemon['stderr']}")
    resolv = rig.root / "resolv.conf"
    resolv.write_text(DNS_RESOLV_CONF)
    rig.launcher.mount_file(resolv, "/etc/resolv.conf")
    lookup = dig(rig, rig.dns_name)
    if lookup["answers"] != ["127.0.0.1"]:
        raise AssertionError(f"the endpoint name does not resolve: {lookup}")
    storage = json.loads(json.dumps(rig.storage))
    storage["s3"]["endpoint"] = f"http://{rig.dns_name}:{NGINX_PORT}"
    rig.network_setup = {"dns_name": rig.dns_name, "endpoint": storage["s3"]["endpoint"],
                         "resolv_conf": DNS_RESOLV_CONF, "dnsmasq": daemon["argv"],
                         "dig": lookup}
    return storage


def completion_front_setup(rig) -> dict:
    """The multipart completion cases' storage: the engine uses the completion front."""
    storage = json.loads(json.dumps(rig.storage))
    storage["s3"]["endpoint"] = f"http://127.0.0.1:{COMPLETION_FRONT_PORT}"
    rig.network_setup = {"endpoint": storage["s3"]["endpoint"]}
    return storage


NETWORK_SETUP = {"dns_nxdomain": dns_setup, "dns_timeout": dns_setup,
                 "dropped_multipart_completion": completion_front_setup,
                 "held_multipart_completion": completion_front_setup}

# The capture each network fault keeps: its tcpdump expression.
NETWORK_CAPTURES = {
    "reset": lambda rig: (f"(tcp port {PROXY_PORTS['general']} or tcp port "
                          f"{PROXY_PORTS['values']}) and tcp[tcpflags] & tcp-rst != 0"),
    "dns_nxdomain": lambda rig: "port 53",
    "dns_timeout": lambda rig: "port 53",
    "tcp_ack_loss": lambda rig: f"host {rig.store_ip} and tcp port {STORE_PORT}",
}


DURATION_FIELD = re.compile(r"([0-9.]+)(ms|us|\u00b5s|ns|s)\b")
DURATION_SCALE = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "\u00b5s": 1e-6, "ns": 1e-9}


def flush_attempt_failures(lines) -> list:
    """Every `flush.attempt_failed` event: its time, file and the deadline it names.

    The event carries `deadline_remaining` as Rust prints a Duration, so the
    flush's own deadline is the event's time plus that.
    """
    found = []
    for line in lines:
        line = ANSI.sub("", line)
        match = EVENT_LINE.search(line)
        stamp = LOG_TIMESTAMP.search(line)
        if not match or not stamp or match[2] != "series_parquet.flush.attempt_failed":
            continue
        file = re.search(r"\bfile=([^,\s\]]+)", match[4])
        remaining = re.search(r"\bdeadline_remaining=(\S+?),", match[4])
        amount = DURATION_FIELD.fullmatch(remaining[1]) if remaining else None
        instant = datetime.datetime.fromisoformat(stamp[1]).replace(
            tzinfo=datetime.timezone.utc).timestamp()
        found.append({"unix_s": instant, "file": file[1] if file else None,
                      "deadline_unix_s": instant + float(amount[1]) * DURATION_SCALE[amount[2]]
                      if amount else None})
    return found


def flush_cleanups(lines) -> list:
    """Every `flush.cleanup` event in engine log lines: its time, outcome and file."""
    found = []
    for line in lines:
        line = ANSI.sub("", line)
        match = EVENT_LINE.search(line)
        stamp = LOG_TIMESTAMP.search(line)
        if not match or not stamp or match[2] != "series_parquet.flush.cleanup":
            continue
        outcome = re.search(r"\boutcome=(\w+)", match[4])
        file = re.search(r"\bfile=([^,\s\]]+)", match[4])
        found.append({"unix_s": datetime.datetime.fromisoformat(stamp[1]).replace(
            tzinfo=datetime.timezone.utc).timestamp(),
            "outcome": outcome[1] if outcome else None, "file": file[1] if file else None,
            "level": match[1]})
    return found


def block_commits(lines) -> list:
    """Every `block.committed` event in engine log lines: its time, block sequence
    number and attempts. The event's path is truncated in the log, so a block is
    named by its sequence number, the last field of its file name."""
    found = []
    for line in lines:
        line = ANSI.sub("", line)
        match = EVENT_LINE.search(line)
        stamp = LOG_TIMESTAMP.search(line)
        if not match or not stamp or match[2] != "series_parquet.block.committed":
            continue
        seq = re.search(r"\bseq=(\d+)", match[4])
        attempts = re.search(r"\battempts=(\d+)", match[4])
        found.append({"unix_s": datetime.datetime.fromisoformat(stamp[1]).replace(
            tzinfo=datetime.timezone.utc).timestamp(),
            "seq": int(seq[1]) if seq else None,
            "attempts": int(attempts[1]) if attempts else None})
    return found


def block_seq(file):
    """The block sequence number a frozen file name ends with (`...-00000004.parquet`)."""
    match = re.search(r"-(\d+)\.parquet$", file or "")
    return int(match[1]) if match else None


def any_event(*events):
    """An event set as soon as any of `events` is."""
    combined = threading.Event()
    for event in events:
        threading.Thread(target=lambda e=event: (e.wait(), combined.set()), daemon=True).start()
    return combined


def merge_outcomes(outcomes) -> dict:
    """One `send_paced` outcome for several consecutive schedules."""
    counts = collections.Counter()
    for outcome in outcomes:
        counts.update(outcome.get("outcomes") or {})
    return {"requests": sum(outcome["requests"] for outcome in outcomes),
            "concurrency": max((outcome["concurrency"] for outcome in outcomes), default=0),
            "rate_requests_per_s": FAULT_RATE_REQUESTS_PER_S,
            "started_ns": min(outcome["started_ns"] for outcome in outcomes),
            "finished_ns": max(outcome["finished_ns"] for outcome in outcomes),
            "lateness_max_s": max((outcome["lateness_max_s"] for outcome in outcomes), default=0),
            "outcomes": dict(counts), "schedules_count": len(outcomes)}


class NetworkCase(FaultCase):
    """A network fault case: the S3 family's state machine with network evidence.

    The fault's own condition (`FAULT_CONDITIONS`) reads what the rig saw:
    NGINX's upstream failures by class, captured RSTs, the resolver's
    queries and answers, rule counters, retransmissions and the pure-ACK
    check. A capture runs from just before arming until the routes are
    healthy again.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.capture = None
        self.costly = {}
        self.flush_events = {"failures": [], "cleanups": [], "attempts": [], "commits": []}
        self.network = {"setup": getattr(self.rig, "network_setup", None)}

    def cached(self, name, compute):
        """A costly observation, refreshed at most every COSTLY_PERIOD_S."""
        now = time.monotonic()
        entry = self.costly.get(name)
        if entry is None or now - entry[0] >= COSTLY_PERIOD_S:
            entry = (now, compute())
            self.costly[name] = entry
        return entry[1]

    def engine_flush_events(self):
        """Every flush failure and cleanup the current engine logged so far."""
        if self.log is not None:
            lines = self.log.lines()
            self.flush_events["failures"].extend(flush_failures(lines))
            self.flush_events["cleanups"].extend(flush_cleanups(lines))
            self.flush_events["attempts"].extend(flush_attempt_failures(lines))
            self.flush_events["commits"].extend(block_commits(lines))
        return self.flush_events

    def dns_entries(self):
        """The case resolver's log, parsed."""
        _status, text, _stderr = self.rig.exec_output(
            ["cat", f"{ARTIFACT_MOUNT}/{DNSMASQ_CASE_LOG}"])
        return dnsmasq_queries(text, self.rig.dns_name, time.gmtime().tm_year)

    def arm(self, parameters=None):
        """Start the fault's capture, then activate it."""
        expression = NETWORK_CAPTURES.get(self.fault)
        if expression is not None:
            self.capture = Capture(self.rig, f"{self.fault}-{self.store.kind}",
                                   expression(self.rig),
                                   seconds=FAULT_OBSERVE_DEADLINE_S[self.fault] + CAPTURE_MARGIN_S,
                                   snaplen=CAPTURE_SNAPLEN)
            self.capture.__enter__()
        super().arm(parameters)
        if self.fault == "dns_timeout":
            self.network["diagnostic_dig"] = dig(self.rig, self.rig.dns_name)
            self.network["dns_rules_after_dig"] = dns_rule_counters(self.rig)

    def observe_condition(self):
        """The S3 family's observations plus the fault's network evidence."""
        observed = super().observe_condition()
        armed = self.at("armed")
        requests = [entry for entry in self.route_requests()
                    if _within(entry, armed["unix_s"], None)]
        observed["engine_requests_logged_count"] = len(requests)
        observed["http_502_writes_count"] = sum(
            1 for entry in requests if entry.get("status") == "502"
            and entry.get("method") in ("PUT", "POST"))
        if self.fault in ("disconnect", "reset"):
            observed["nginx_errors"] = self.cached("nginx_errors", lambda: nginx_error_classes(
                self.rig.nginx_error_log(), armed["unix_s"]))["counts"]
        if self.fault == "reset":
            observed["rst_packets_count"] = self.cached(
                "rst", lambda: pcap_count(self.rig, self.capture.file))
        if self.fault in ("dns_nxdomain", "dns_timeout"):
            observed["dns"] = fault_dns_evidence(self.cached("dns", self.dns_entries),
                                                 armed["unix_s"])
        if self.fault == "dns_timeout":
            observed["dns_rule_packets"] = self.cached("dns_rules",
                                                       lambda: dns_rule_counters(self.rig))
            observed["diagnostic_dig"] = self.network.get("diagnostic_dig")
            observed["dns_rule_packets_after_dig"] = self.network.get("dns_rules_after_dig")
        if self.fault == "tcp_ack_loss":
            observed["dropped_pure_ack_packets_count"] = self.cached(
                "ack_rule", lambda: ack_rule_packets(self.rig))
            observed["ack_capture"] = self.cached("ack_capture", lambda: ack_capture_evidence(
                self.rig, self.capture, armed["evidence"]["activation"]["state"]["expression"],
                self.rig.store_ip))
        return observed

    def remove_fault(self, controls, store_cores):
        """Remove the fault, wait for both routes, then close the capture."""
        try:
            super().remove_fault(controls, store_cores)
        finally:
            self.close_capture()

    def close_capture(self):
        """Stop the capture and read its final evidence."""
        if self.capture is None or self.capture.process is None:
            return
        self.capture.stop()
        final = {"capture": self.capture.as_json()}
        if self.fault == "tcp_ack_loss":
            state = self.at("armed")["evidence"]["activation"]["state"]
            final["ack_capture"] = ack_capture_evidence(self.rig, self.capture,
                                                        state["expression"], self.rig.store_ip)
        elif self.fault == "reset":
            final["rst_packets_count"] = pcap_count(self.rig, self.capture.file)
        else:
            windows = self.fault_window()
            final["dns_packets_count"] = pcap_count(self.rig, self.capture.file)
            final["dns_packets_during_fault_count"] = pcap_count(
                self.rig, self.capture.file,
                f"frame.time_epoch >= {windows[0]} && frame.time_epoch < {windows[1]}")
        self.network["capture"] = final

    def collect(self, record):
        """What the rig alone can still tell before it is removed."""
        self.close_capture()
        window = self.fault_window()
        text = self.rig.nginx_error_log()
        (self.rig.artifact_dir / "nginx-error.log").write_text(text)
        self.network["nginx_errors_during_fault"] = nginx_error_classes(
            text, *(window if window else (None, None)))
        if self.fault in ("dns_nxdomain", "dns_timeout") and window and window[1]:
            entries = self.dns_entries()
            self.network["dns_during_fault"] = dns_evidence(entries, int(window[0]) + 1,
                                                            window[1])
            self.network["dns_after_fault"] = dns_evidence(entries, window[1])
        self.network["activations"] = self.rig.activations
        return self.network

    def verdicts(self, record):
        """The S3 family's two verdicts, then the fault's own checks."""
        return super().verdicts(record) + self.network_verdicts(record)

    def network_verdicts(self, record):
        """Checks beside the common ones: none by default."""
        return []

    def numbers(self, record, during):
        """The S3 family's numbers plus the network evidence's."""
        numbers = super().numbers(record, during)
        network = record.get("network") or {}
        observed = (self.at("observed") or {}).get("evidence", {})
        errors = (network.get("nginx_errors_during_fault") or {}).get("counts", {})
        capture = network.get("capture") or {}
        ack = capture.get("ack_capture") or {}
        numbers.update({
            "http_502_responses_count": (sum(
                count for statuses in during["status_by_operation"].values()
                for status, count in statuses.items() if status == "502") if during else None),
            "nginx_upstream_refused_count": errors.get("refused", 0),
            "nginx_upstream_reset_count": errors.get("reset", 0),
            "nginx_upstream_prematurely_closed_count": errors.get("prematurely_closed", 0),
            "nginx_upstream_timed_out_count": errors.get("timed_out", 0),
            "rst_packets_count": capture.get("rst_packets_count"),
            "engine_requests_during_fault_count": during["requests_count"] if during else None,
            "dns_engine_queries_count": (observed.get("dns") or {}).get("engine_queries_count"),
            "dns_nxdomain_answers_count": (network.get("dns_during_fault") or {}).get(
                "nxdomain_answers_count"),
            "dns_queries_during_fault_count": (network.get("dns_during_fault") or {}).get(
                "queries_count"),
            "dns_packets_during_fault_count": capture.get("dns_packets_during_fault_count"),
            "dns_rule_dropped_udp_count": (observed.get("dns_rule_packets") or {}).get("udp"),
            "dns_rule_dropped_tcp_count": (observed.get("dns_rule_packets") or {}).get("tcp"),
            "dns_engine_dropped_after_dig_count": engine_dns_drops_after_dig(observed),
            "dropped_pure_ack_packets_count": self.removal_state().get(
                "dropped_packets_at_removal", observed.get("dropped_pure_ack_packets_count")),
            "tcp_retransmissions_count": ack.get("tcp_retransmissions_count"),
            "ack_matched_packets_count": ack.get("matched_packets_count"),
            "ack_matched_pure_ack_packets_count": ack.get("matched_pure_ack_packets_count"),
            "block_visibility_max_after_window_end_s": block_lateness(
                record["objects"], self.spec.interval_s,
                lateness_bound_s(self.settings))["max_after_window_end_s"],
        })
        return numbers

    def removal_state(self):
        """What the fault's recovery recorded, such as counters read before removal."""
        activation = (self.at("armed") or {}).get("evidence", {}).get("activation") or {}
        return activation.get("recovery") or {}

    def observations(self, record):
        """The network evidence and each object's lateness after its window."""
        return {"network": dict(record.get("network") or {}, block_lateness=block_lateness(
            record["objects"], self.spec.interval_s, lateness_bound_s(self.settings)))}


def _upstream_failure_met(case, seen, label) -> bool:
    """NGINX answered a write 502 and logged the upstream failure as `label`, the
    exporter retried and nacked, and the nack was retried."""
    return (seen["http_502_writes_count"] > 0 and seen.get("nginx_errors", {}).get(label, 0) > 0
            and seen["flush_retries_count"] > 0 and seen["storage_nacks_count"] > 0
            and seen["retried"])


def _disconnect_met(case, seen) -> bool:
    """Disconnect: writes answered 502 over refused upstream connections, retried and nacked."""
    return _upstream_failure_met(case, seen, "refused")


def _reset_met(case, seen) -> bool:
    """Reset: RSTs captured and writes answered 502 over reset upstreams, retried and nacked."""
    return (seen.get("rst_packets_count") or 0) > 0 and _upstream_failure_met(case, seen, "reset")


def _dns_nxdomain_met(case, seen) -> bool:
    """NXDOMAIN: the engine's own fresh queries were answered NXDOMAIN, the exporter
    retried and nacked, and the nack was retried."""
    dns = seen.get("dns") or {}
    return (dns.get("engine_queries_count", 0) > 0 and dns.get("nxdomain_answers_count", 0) > 0
            and seen["flush_retries_count"] > 0 and seen["storage_nacks_count"] > 0
            and seen["retried"])


def engine_dns_drops_after_dig(seen):
    """Queries of the engine's own user the rules dropped after the diagnostic
    lookup's snapshot, or None without both readings."""
    now = seen.get("dns_rule_packets") or {}
    then = seen.get("dns_rule_packets_after_dig") or {}
    if any(not isinstance(reading.get(name), int) for reading in (now, then)
           for name in ("engine_udp", "engine_tcp")):
        return None
    return sum(now[name] - then[name] for name in ("engine_udp", "engine_tcp"))


def _dns_timeout_met(case, seen) -> bool:
    """DNS timeout: the engine's own queries were dropped after the diagnostic
    lookup, which timed out, the resolver received no query for the name, and the
    exporter retried and nacked."""
    dns = seen.get("dns") or {}
    return ((engine_dns_drops_after_dig(seen) or 0) > 0
            and bool((seen.get("diagnostic_dig") or {}).get("timed_out"))
            and dns.get("queries_count") == 0 and seen["flush_retries_count"] > 0
            and seen["storage_nacks_count"] > 0 and seen["retried"])


def _tcp_ack_loss_met(case, seen) -> bool:
    """ACK loss: dropped pure ACKs, retransmissions and only pure ACKs matched,
    held ACK_LOSS_HOLD_S so a stalled request meets the client's own timeout."""
    return seen.get("elapsed_s", 0) >= ACK_LOSS_HOLD_S and not ack_loss_evidence_problems(
        seen.get("dropped_pure_ack_packets_count"), seen.get("ack_capture") or {})


FAULT_CONDITIONS.update({"disconnect": _disconnect_met, "reset": _reset_met,
                         "dns_nxdomain": _dns_nxdomain_met, "dns_timeout": _dns_timeout_met,
                         "tcp_ack_loss": _tcp_ack_loss_met})


def cohort_object_check(store, key, ledger, request_ids, workload, signal, root) -> dict:
    """Read one completed values object back as the oracle does, against its cohort.

    The object and every series file of its signal are copied from the store
    directly; the scanner checks the files' own invariants and descriptor
    coverage, and each values row's record id and payload hash, joined to
    its descriptor, must be exactly the cohort's records.
    """
    root = Path(root)
    shutil.rmtree(root, ignore_errors=True)
    head = store.client.head_object(Bucket=store.bucket, Key=key)
    body = store.client.get_object(Bucket=store.bucket, Key=key)["Body"].read()
    target = root / key.removeprefix("otel/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    for page in store.client.get_paginator("list_objects_v2").paginate(
            Bucket=store.bucket, Prefix=f"otel/v=1/signal={signal}/dataset=series/"):
        for item in page.get("Contents", []):
            destination = root / item["Key"].removeprefix("otel/")
            destination.parent.mkdir(parents=True, exist_ok=True)
            store.client.download_file(store.bucket, item["Key"], str(destination))
    report = {"key": key, "size_bytes": head["ContentLength"], "etag": head.get("ETag"),
              "bytes_complete": len(body) == head["ContentLength"]}
    placeholders = ",".join("?" for _ in request_ids)
    with ledger.lock:
        expected = dict(ledger.connection.execute(
            f"SELECT record_id, expected_sha256 FROM records WHERE request_id IN ({placeholders})",
            list(request_ids)).fetchall())
    try:
        with measurement.duckdb.connect() as db:
            test_e2e.scan_objects(measurement._SilentAsserts(), root, db, collect_bodies=False)
            values, canonical = measurement._duck_latest_descriptor(root, signal)
            record_id, payload = measurement._duck_record_expression(workload, signal)
            rows = db.execute(f"SELECT {record_id}, lower(sha256({payload})) FROM {values} v "
                              f"INNER JOIN {canonical} s ON v.series_id = s.series_id").fetchall()
            total = db.execute(f"SELECT count(*) FROM {values}").fetchone()[0]
        report["scan"] = "passed"
    except (AssertionError, measurement.duckdb.Error) as error:
        report["scan"] = f"{type(error).__name__}: {error}"[:500]
        rows, total = [], None
    found = dict(rows)
    report.update({
        "values_rows_count": total, "joined_rows_count": len(rows),
        "cohort_records_count": len(expected),
        "missing_records_count": len(set(expected) - set(found)),
        "unexpected_records_count": len(set(found) - set(expected)),
        "corrupt_records_count": sum(1 for record, digest in found.items()
                                     if record in expected and expected[record] != digest),
    })
    report["valid"] = (report["bytes_complete"] and report["scan"] == "passed"
                       and total == len(rows) == len(expected) and not report["missing_records_count"]
                       and not report["unexpected_records_count"]
                       and not report["corrupt_records_count"])
    shutil.rmtree(root, ignore_errors=True)
    return report


class CompletionCohortCase(NetworkCase):
    """The dropped completion response: one single-signal cohort per signal.

    With input paused and the exporter idle, the values proxy's downstream
    toxic withholds every response, and a cohort of COHORT_REQUESTS requests
    of one signal is sent inside one window, small enough for one values
    PUT. The case then requires, while the response is withheld, the values
    object complete in the store and read back as exactly the cohort, the
    exporter still FLUSHING, and no acknowledgement (strict) or no buffer
    resolution (buffered). Removing the toxic must close the waiting
    connection and the client must retry to an acknowledgement. The logs
    cohort's states carry the plain names, the metrics cohort's `_metrics`.
    """

    SIGNALS = ("logs", "metrics")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pause = threading.Event()
        self.segments = []
        self.segment_ready = threading.Condition()
        self.baseline_requests = None
        self.next_index = None
        self.cohorts = {}

    @staticmethod
    def suffix(signal):
        """The state suffix of one cohort."""
        return "" if signal == "logs" else f"_{signal}"

    def produce(self, producer, stop):
        """Paced input until paused, then each schedule the case hands over."""
        outcomes = [producer.send_paced(range(self.spec.workload.requests),
                                        FAULT_RATE_REQUESTS_PER_S, stop=any_event(stop, self.pause))]
        with self.segment_ready:
            self.baseline_requests = outcomes[0]["requests"]
            self.segment_ready.notify_all()
        while not stop.is_set():
            with self.segment_ready:
                if not self.segments:
                    self.segment_ready.wait(0.2)
                    continue
                indexes, done = self.segments.pop(0)
            outcomes.append(producer.send_paced(indexes, FAULT_RATE_REQUESTS_PER_S, stop=stop))
            done.set()
        return merge_outcomes(outcomes)

    def schedule(self, indexes):
        """Hand one schedule to the producer; returns the event set when it finished."""
        done = threading.Event()
        with self.segment_ready:
            self.segments.append((list(indexes), done))
            self.segment_ready.notify_all()
        return done

    def run(self, controls, store_cores, result, options):
        """Pause, then a cohort per signal, then resume the paced input."""
        self.pause.set()
        if not self.attempt(self.await_paused):
            return
        for kind in self.SIGNALS:
            if not (self.attempt(self.quiesce, kind) and self.attempt(self.cohort, kind)):
                return
        self.schedule(range(self.next_index, self.spec.workload.requests))
        self.attempt(self.await_after_input)

    def await_paused(self):
        """The paced producer stopped and every request it started finished."""
        with self.segment_ready:
            deadline = time.monotonic() + FAULT_PRODUCER_TIMEOUT_S
            while self.baseline_requests is None and time.monotonic() < deadline:
                self.segment_ready.wait(0.5)
        if self.baseline_requests is None:
            raise AssertionError("the paced producer did not stop")
        self.next_index = self.baseline_requests
        self.transition("paused", {"requests_sent_count": self.baseline_requests})

    def quiesce(self, signal):
        """Nothing pending anywhere: the producer, the exporter and the buffer."""
        def observe():
            # A fresh sample every quarter second, not every poll.
            time.sleep(0.25)
            totals = flat_totals(self.phase.sampler.once())
            return {"pending_requests_count": ledger_cohorts(
                        self.ledger, time.monotonic_ns())["pending_requests_count"],
                    "block_active_bytes": totals.get("block.active", 0),
                    "block_flushing_bytes": totals.get("block.flushing", 0),
                    "block_pending_bytes": totals.get("block.pending", 0),
                    "buffer_in_flight_count": totals.get("buffer.in.flight", 0),
                    "buffer_queued_items": totals.get("buffer.items.queued", 0)}

        evidence = measurement.wait_until(
            observe, lambda seen: not any(seen.values()),
            deadline_ns=time.monotonic_ns() + 90 * 10**9,
            description="no request pending and the exporter and buffer idle")
        self.transition(f"quiet{self.suffix(signal)}", evidence)

    def cohort(self, signal):
        """One cohort: arm, send, observe the withheld completion, remove, resume."""
        suffix = self.suffix(signal)
        indexes = []
        index = self.next_index
        while len(indexes) < COHORT_REQUESTS:
            if self.spec.workload.signal_of(index) == signal:
                indexes.append(index)
            index += 1
        self.next_index = index
        before = set(self.lister.list_once())
        capacity._command().await_window_start(self.spec.interval_s)
        totals = self.totals()
        self.rig.activate(self.fault, {})
        self.transition(f"armed{suffix}", {"totals": totals,
                                           "activation": self.rig.activations[-1],
                                           "signal": signal, "request_ids": indexes})
        cohort = {"signal": signal, "request_ids": indexes, "keys_before": before}
        self.cohorts[signal] = cohort
        cohort["done"] = self.schedule(indexes)
        try:
            evidence = measurement.wait_until(
                lambda: self.observe_cohort(cohort), self.cohort_met,
                deadline_ns=self.at(f"armed{suffix}")["monotonic_ns"]
                + FAULT_OBSERVE_DEADLINE_S[self.fault] * 10**9,
                description=f"the {signal} cohort's completed values object with its "
                            "response withheld")
            confirmed = flat_totals(self.phase.sampler.once())
            evidence["confirmed_block_flushing_bytes"] = confirmed.get("block.flushing", 0)
            if not evidence["confirmed_block_flushing_bytes"]:
                raise AssertionError(f"the exporter no longer holds FLUSHING: {evidence}")
            evidence["totals"] = self.totals()
            self.transition(f"observed{suffix}", evidence)
        finally:
            removed_unix = time.time()
            self.rig.recover()
        self.transition(f"fault_removed{suffix}", {"totals": self.totals(),
                                                   "removed_unix_s": removed_unix})
        client = self.rig.route_client(read_timeout=10)
        health = measurement.wait_until(
            lambda: self.rig.route_health(client), lambda seen: seen["answered"],
            deadline_ns=time.monotonic_ns() + ENDPOINT_HEALTH_DEADLINE_S * 10**9,
            description="signed HEADs through the general and the values route")
        self.transition(f"endpoint_healthy{suffix}", {"routes": health})
        resumed = measurement.wait_until(
            lambda: self.observe_retry(cohort, removed_unix), self.retry_met,
            deadline_ns=self.at(f"endpoint_healthy{suffix}")["monotonic_ns"]
            + RECOVERY_DEADLINE_S * 10**9,
            description=f"the {signal} cohort's closed connection, its retry and its "
                        "acknowledgement")
        self.transition(f"resumed{suffix}", resumed)
        if not cohort["done"].wait(FAULT_PRODUCER_TIMEOUT_S):
            raise AssertionError(f"the {signal} cohort's requests did not finish")

    def observe_cohort(self, cohort):
        """What the store, the route, the producer and the exporter show for a cohort."""
        suffix = self.suffix(cohort["signal"])
        armed = self.at(f"armed{suffix}")
        marker = f"signal={cohort['signal']}/dataset=values/"
        keys = sorted(key for key in self.lister.first_listed
                      if marker in key and key not in cohort["keys_before"])
        totals = self.totals()
        base = armed["evidence"]["totals"]
        with self.ledger.lock:
            acked = self.ledger.connection.execute(
                "SELECT count(*) FROM requests WHERE ack_ns IS NOT NULL AND request_id IN "
                f"({','.join(str(index) for index in cohort['request_ids'])})").fetchone()[0]
        seen = {"elapsed_s": (time.monotonic_ns() - armed["monotonic_ns"]) / 1e9,
                "values_keys": keys, "cohort_acked_requests_count": acked,
                "block_flushing_bytes": totals.get("block.flushing", 0),
                "buffer_resolved_delta": totals.get("buffer.bundles.resolved", 0)
                - base.get("buffer.bundles.resolved", 0),
                "buffer_in_flight_count": totals.get("buffer.in.flight", 0),
                "buffered": self.buffered}
        if len(keys) == 1:
            key = keys[0]
            logged = [entry for entry in self.route_requests()
                      if entry.get("uri", "").endswith("/" + key)]
            seen["route_log"] = [{field: entry.get(field) for field in (
                "msec", "method", "operation", "status", "upstream_status")} for entry in logged]
            seen["values_operations"] = sorted({entry["operation"] for entry in logged})
            if "check" not in cohort:
                cohort["check"] = cohort_object_check(
                    self.store, key, self.ledger, cohort["request_ids"], self.spec.workload,
                    cohort["signal"], self.rig.root / f"cohort-{cohort['signal']}")
                cohort["key"] = key
            seen["object"] = cohort["check"]
            heads = cohort.setdefault("heads", [])
            if not heads or time.time() - heads[-1]["unix_s"] >= COHORT_HEAD_PERIOD_S:
                head = self.store.client.head_object(Bucket=self.store.bucket, Key=key)
                heads.append({"unix_s": time.time(), "etag": head.get("ETag"),
                              "size_bytes": head["ContentLength"]})
            seen["stable_heads_count"] = sum(
                1 for entry in heads if (entry["etag"], entry["size_bytes"])
                == (cohort["check"]["etag"], cohort["check"]["size_bytes"]))
            name = key.rsplit("/", 1)[1]
            seen["block_objects"] = sorted(item for item in self.lister.first_listed
                                           if item.endswith("/" + name))
        return seen

    @staticmethod
    def cohort_met(seen):
        """One completed values object that reads back as exactly the cohort and
        that repeated direct HEADs show unchanged, whose response is withheld,
        while the exporter still owns the flush: strict, no acknowledgement;
        buffered, no buffer resolution."""
        withheld = not any(str(entry.get("status", "")).startswith("2")
                           for entry in seen.get("route_log", []))
        owner = (seen["buffer_resolved_delta"] == 0 and seen["buffer_in_flight_count"] > 0
                 if seen["buffered"] else seen["cohort_acked_requests_count"] == 0)
        return (len(seen["values_keys"]) == 1 and bool((seen.get("object") or {}).get("valid"))
                and seen.get("stable_heads_count", 0) >= COHORT_HEADS
                and withheld and seen["block_flushing_bytes"] > 0 and owner)

    def observe_retry(self, cohort, removed_unix):
        """After removal: the withheld request's close, the retry and the acknowledgement."""
        key = cohort.get("key", "")
        logged = [entry for entry in self.route_requests()
                  if key and entry.get("uri", "").endswith("/" + key)]
        closed = [entry for entry in logged if entry.get("status") == "502"
                  and float(entry["msec"]) >= removed_unix - 0.5]
        retried = [entry for entry in logged if str(entry.get("status", "")).startswith("2")
                   and float(entry["msec"]) >= removed_unix]
        suffix = self.suffix(cohort["signal"])
        totals = self.totals()
        base = self.at(f"armed{suffix}")["evidence"]["totals"]
        with self.ledger.lock:
            acked = self.ledger.connection.execute(
                "SELECT count(*) FROM requests WHERE ack_ns IS NOT NULL AND request_id IN "
                f"({','.join(str(index) for index in cohort['request_ids'])})").fetchone()[0]
        return {"closed_requests": [{f: entry.get(f) for f in ("msec", "status", "upstream_status",
                                                               "request_time")} for entry in closed],
                "retried_requests": [{f: entry.get(f) for f in ("msec", "status", "operation")}
                                     for entry in retried],
                "flush_retries_delta": totals.get("flush.retries", 0) - base.get("flush.retries", 0),
                "exporter_acks_delta": totals.get("acks", 0) - base.get("acks", 0),
                "buffer_resolved_delta": totals.get("buffer.bundles.resolved", 0)
                - base.get("buffer.bundles.resolved", 0),
                "cohort_acked_requests_count": acked,
                "cohort_requests_count": len(cohort["request_ids"]), "buffered": self.buffered}

    @staticmethod
    def retry_met(seen):
        """The withheld connection closed (NGINX 502), the client retried to a 2xx,
        and the cohort was acknowledged (buffered: resolved by the buffer)."""
        acked = (seen["buffer_resolved_delta"] > 0 if seen["buffered"]
                 else seen["cohort_acked_requests_count"] == seen["cohort_requests_count"])
        return (bool(seen["closed_requests"]) and bool(seen["retried_requests"])
                and seen["exporter_acks_delta"] > 0 and acked)

    def await_after_input(self):
        """The paced input after the last cohort, measured from its acknowledgement."""
        resumed = self.at(f"resumed{self.suffix(self.SIGNALS[-1])}")
        wanted = FAULT_AFTER_S * FAULT_RATE_REQUESTS_PER_S

        def observe():
            acks = [ack for ack in ledger_acks(self.ledger) if ack >= resumed["monotonic_ns"]]
            return {"requests_acked_since_resumed_count": len(acks)}

        return measurement.wait_until(
            observe, lambda seen: seen["requests_acked_since_resumed_count"] >= wanted,
            deadline_ns=self.recovery_deadline_ns(),
            description=f"{wanted} requests acknowledged after the last cohort")

    def recovery_deadline_ns(self):
        """Everything after the last cohort's routes were healthy again."""
        return self.at(f"endpoint_healthy{self.suffix(self.SIGNALS[-1])}")["monotonic_ns"] \
            + RECOVERY_DEADLINE_S * 10**9

    def fault_window(self):
        """From the first cohort's arming to the last cohort's removal."""
        armed = self.at("armed")
        removed = self.at(f"fault_removed{self.suffix(self.SIGNALS[-1])}")
        return (armed["unix_s"], removed and removed["unix_s"]) if armed else None

    def throughput_edges(self):
        """Before the first cohort, across both, and after the last."""
        last = f"endpoint_healthy{self.suffix(self.SIGNALS[-1])}"
        return {"before": ("input_started", "paused"), "during": ("armed", last),
                "after": (last, "input_stopped")}

    def verdicts(self, record):
        """Both cohorts observed; both recovered, and the rest drained in time."""
        observed = [self.at(f"observed{self.suffix(signal)}") for signal in self.SIGNALS]
        last = self.suffix(self.SIGNALS[-1])
        recovered_s = self.span(f"endpoint_healthy{last}", "drained")
        return [
            ("fault_observed", all(observed), json.dumps(
                {signal: {key: value for key, value in (entry or {}).get("evidence", {}).items()
                          if key in ("values_keys", "object", "values_operations",
                                     "cohort_acked_requests_count", "block_flushing_bytes",
                                     "buffer_resolved_delta", "block_objects",
                                     "stable_heads_count")}
                 for signal, entry in zip(self.SIGNALS, observed)})
             if all(observed) else "; ".join(self.problems) or "a cohort was never observed"),
            ("recovered", all(self.at(f"resumed{self.suffix(signal)}") for signal in self.SIGNALS)
             and recovered_s is not None and recovered_s <= RECOVERY_DEADLINE_S,
             f"last cohort's healthy routes to drained {recovered_s} s against "
             f"{RECOVERY_DEADLINE_S} s; problems {self.problems}"),
        ]

    def numbers(self, record, during):
        """Per cohort: held, observed and recovery times; the completed-but-unacknowledged
        objects and bytes; and the cohorts' multiplicities."""
        numbers = super().numbers(record, during)
        checks = [cohort.get("check") or {} for cohort in self.cohorts.values()]
        numbers.update({
            "completed_unacknowledged_objects_count": sum(1 for check in checks
                                                          if check.get("valid")),
            "completed_unacknowledged_bytes": sum(check.get("size_bytes") or 0 for check in checks
                                                  if check.get("valid")),
            "fault_duration_s": sum(self.span(f"armed{self.suffix(s)}",
                                              f"fault_removed{self.suffix(s)}") or 0
                                    for s in self.SIGNALS),
            "time_to_condition_s": max((self.span(f"armed{self.suffix(s)}",
                                                  f"observed{self.suffix(s)}") or 0
                                        for s in self.SIGNALS), default=None),
            "recovery_s": max((self.span(f"endpoint_healthy{self.suffix(s)}",
                                         f"resumed{self.suffix(s)}") or 0
                               for s in self.SIGNALS), default=None),
            "endpoint_healthy_to_drained_s": self.span(
                f"endpoint_healthy{self.suffix(self.SIGNALS[-1])}", "drained"),
        })
        numbers.update((record.get("case_oracle") or {}).get("numbers", {}))
        return numbers

    def oracle_extras(self, ledger):
        """Each cohort's multiplicity histogram and everything else's."""
        cohort_ids = sorted(index for cohort in self.cohorts.values()
                            for index in cohort["request_ids"])
        marks = ",".join(str(index) for index in cohort_ids) or "-1"
        histograms = {}
        with ledger.lock:
            for name, clause in (("cohorts", f"IN ({marks})"), ("outside", f"NOT IN ({marks})")):
                rows = ledger.connection.execute(
                    "SELECT copies, count(*) FROM (SELECT e.record_id, count(a.record_id) AS copies "
                    "FROM records e LEFT JOIN actual a ON a.record_id = e.record_id "
                    f"WHERE e.request_id {clause} GROUP BY e.record_id) GROUP BY copies"
                ).fetchall()
                histograms[name] = {str(copies): count for copies, count in rows}
        return {"multiplicity": histograms, "numbers": {
            "cohort_records_count": sum(histograms["cohorts"].values()),
            "cohort_duplicated_records": sum(count for copies, count in histograms["cohorts"].items()
                                             if int(copies) > 1),
            "outside_cohort_duplicated_records": sum(
                count for copies, count in histograms["outside"].items() if int(copies) > 1)}}

    def observations(self, record):
        """The network evidence plus every cohort's evidence."""
        observations = super().observations(record)
        observations["network"]["cohorts"] = {
            signal: {key: value for key, value in cohort.items()
                     if key not in ("done", "keys_before")}
            for signal, cohort in self.cohorts.items()}
        observations["network"]["cohort_multiplicity"] = (record.get("case_oracle") or {}).get(
            "multiplicity")
        return observations


class MultipartCompletionCase(NetworkCase):
    """A CompleteMultipartUpload whose response is lost, or which is held, past
    the writer's deadline, through the completion front.

    `dropped_multipart_completion`: the completion lands in the store and its
    response is dropped; the case holds the fault until the writer's flush of
    that block failed at its deadline and its cleanup cutoff passed, so the
    late-commit probe can find the object. `held_multipart_completion`: the
    completion is held in the proxy, the writer's flush fails without it, and
    the case releases it past the later of the writer's cleanup cutoff and the
    block's window end plus L, then measures whether and when the object
    appears.
    The target is the first block whose write attempt failed after arming; its
    logs values object is the multipart one.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.completion = {}
        self.abort_timeout_s = duration_s(self.settings["upload"]["abort_timeout"])
        self.bound_s = lateness_bound_s(self.settings)

    def target(self):
        """The first block whose write attempt failed after arming: its file, the
        deadline its attempt named, its flush failure once logged (of any class)
        with the cleanup cutoff that follows it, and its values key once the store
        shows it or its upload."""
        armed = self.at("armed")
        events = self.engine_flush_events()
        attempts = [entry for entry in events["attempts"] if entry["unix_s"] >= armed["unix_s"]
                    and entry["file"] and entry["deadline_unix_s"] is not None]
        if attempts:
            file = attempts[0]["file"]
            deadline = attempts[0]["deadline_unix_s"]
        elif self.fault == "dropped_multipart_completion":
            # Since the lost-completion probe (Task 12a) a completion that lost
            # its response is settled inside its attempt: the writer HEADs the
            # frozen name, finds the object and acknowledges the block, so no
            # attempt fails. The target is then the first values block whose
            # completion NGINX answered without a 2xx after arming.
            file = dropped_completion_file(self.route_requests(), armed["unix_s"])
            if file is None:
                return None
            deadline = None
        else:
            return None
        failure = next((entry for entry in events["failures"] if entry["file"] == file), None)
        commit = next((entry for entry in events["commits"]
                       if entry["seq"] == block_seq(file) and entry["unix_s"] >= armed["unix_s"]),
                      None)
        suffix = "/" + file
        listed = [key for key in self.lister.first_listed if key.endswith(suffix)
                  and "signal=logs/dataset=values/" in key]
        uploads = self.cached("uploads", lambda: orphaned_uploads(self.store))
        pending = [upload["key"] for upload in uploads if upload["key"].endswith(suffix)
                   and "signal=logs/dataset=values/" in upload["key"]]
        key = (listed or pending or [None])[0]
        # A block settled by the probe has no cleanup cutoff: nothing of it is
        # left to unwind once it is committed.
        settled = failure["unix_s"] if failure else deadline
        return {"file": file, "deadline_unix_s": deadline, "failure": failure, "key": key,
                "commit": commit,
                "window_end_unix_s": block_window_end(key, self.spec.interval_s) if key else None,
                "cutoff_unix_s": settled + self.abort_timeout_s if settled is not None
                else (commit or {}).get("unix_s"),
                "incomplete_uploads": [upload for upload in uploads
                                       if upload["key"].endswith(suffix)]}

    def release_at(self, target):
        """When a held completion is released: HELD_RELEASE_MARGIN_S past the later
        of the writer's cleanup cutoff and L after the block's window end, or
        after its partition hour's end when its window is within
        HELD_RELEASE_MARGIN_S of it (a straddling cell). A store drops a
        connection whose request has not arrived within its own request timeout,
        if it has one; a completion it still holds then lands."""
        end = target["window_end_unix_s"]
        hour = partition_hour(target["key"])
        if hour and hour[1] - end <= HELD_RELEASE_MARGIN_S:
            end = hour[1]
        return max(target["cutoff_unix_s"], end + self.bound_s) + HELD_RELEASE_MARGIN_S

    def await_condition(self):
        """Held: release the completion at its instant, then observe; the dropped
        case waits for its condition as every fault does."""
        if self.fault != "held_multipart_completion":
            return super().await_condition()
        armed = self.at("armed")
        target = measurement.wait_until(
            self.target, lambda seen: bool(seen and seen.get("key") and seen.get("failure")),
            deadline_ns=armed["monotonic_ns"] + FAULT_OBSERVE_DEADLINE_S[self.fault] * 10**9,
            description="the target block's failed attempt and its values upload")
        release = self.release_at(target)
        time.sleep(max(0.0, release - time.time()))
        head_state, before = self.head_state(target["key"])
        uploads = orphaned_uploads(self.store)
        aborts = writer_aborts(self.route_requests(), target["key"])
        self.rig.recover()
        self.released_unix_s = time.time()
        # NGINX logs a held completion when it ends, just after the release.
        try:
            held = measurement.wait_until(
                lambda: held_completions(self.route_requests(), target["key"],
                                         armed["unix_s"], self.released_unix_s),
                bool, deadline_ns=time.monotonic_ns() + RELEASE_VISIBLE_S * 10**9,
                description="the held completion's end after the release")
        except AssertionError:
            held = []
        evidence = self.observe_condition()
        # The target as it stood before the release; afterwards its upload may be
        # complete and not yet listed.
        evidence["target_after_release"] = evidence.get("target")
        evidence.update({"target": target, "object_before_release": before,
                         "head_before_release": head_state,
                         "held_completions": held or [],
                         "release_at_unix_s": release,
                         "released_unix_s": self.released_unix_s,
                         "uploads_before_release": [upload for upload in uploads
                                                    if upload["key"] == target["key"]],
                         "aborts_before_release": aborts})
        if not _held_multipart_met(self, evidence):
            raise AssertionError(f"the held completion's condition failed: "
                                 f"{json.dumps(evidence, default=str)[:1500]}")
        evidence["totals"] = self.totals()
        self.transition("observed", evidence)

    def observe_condition(self):
        """The S3 family's observations plus the target block's completion."""
        observed = super().observe_condition()
        target = self.target()
        observed["target"] = target
        if target and target["key"]:
            head = self.cached("target_head", lambda: self.head(target["key"]))
            observed["target_object"] = head
            cleanups = [entry for entry in self.flush_events["cleanups"]
                        if entry["file"] == target["file"]]
            observed["target_cleanups"] = cleanups
        observed["now_unix_s"] = time.time()
        return observed

    def head(self, key):
        """The store's own HEAD of `key`, read directly, or None."""
        return self.head_state(key)[1]

    def head_state(self, key):
        """The store's own HEAD of `key`, read directly: ("found", object),
        ("absent", None) only for a NotFound answer, or ("unknown: <code>", None)
        for any other failure, which proves nothing about the object."""
        try:
            answer = self.store.client.head_object(Bucket=self.store.bucket, Key=key)
        except ClientError as error:
            code = str((error.response.get("Error") or {}).get("Code")
                       or error.response.get("ResponseMetadata", {}).get("HTTPStatusCode"))
            if code in ("404", "NoSuchKey", "NotFound"):
                return "absent", None
            return f"unknown: {code}", None
        return "found", {"size_bytes": answer["ContentLength"], "etag": answer.get("ETag"),
                         "last_modified_unix_s": answer["LastModified"].timestamp()}

    def remove_fault(self, controls, store_cores):
        """Remove the fault; a held completion is watched until its object appears."""
        super().remove_fault(controls, store_cores)
        observed = (self.at("observed") or {}).get("evidence", {})
        target = observed.get("target") or {}
        key = target.get("key")
        if not key:
            return
        released = getattr(self, "released_unix_s", None) or self.at("fault_removed")["unix_s"]
        visible = self.head(key)
        seen_unix = time.time() if visible else None
        while visible is None and time.time() - released < RELEASE_VISIBLE_S:
            time.sleep(0.1)
            visible = self.head(key)
            seen_unix = time.time() if visible else None
        self.completion = {"key": key, "released_unix_s": released,
                           "visible_after_removal": visible, "seen_after_removal_unix_s": seen_unix}

    def collect(self, record):
        """The network evidence plus the target's completion timeline."""
        network = super().collect(record)
        observed = (self.at("observed") or {}).get("evidence", {})
        target = observed.get("target") or {}
        cleanups = [entry for entry in self.engine_flush_events()["cleanups"]
                    if target and entry["file"] == target["file"]]
        network["completion"] = dict(self.completion, target=target, target_cleanups=cleanups,
                                     object_at_observation=observed.get("target_object"))
        return network

    def completion_timeline(self, record):
        """When the target object became visible, against its window end, the
        writer's cutoff and L."""
        completion = (record.get("network") or {}).get("completion") or {}
        target = completion.get("target") or {}
        key = target.get("key")
        final = next((item for item in record["objects"] if item["key"] == key), None)
        steps = [{field: entry.get(field) for field in (
            "msec", "operation", "status", "upstream_status", "request_time")}
            for entry in record["requests"] if key and key in (entry.get("uri") or "")
            and entry["operation"] in ("create_multipart_upload", "complete_multipart_upload",
                                       "abort_multipart_upload")]
        if not key or final is None:
            return {"key": key, "visible": False, "multipart_steps": steps,
                    "released_unix_s": completion.get("released_unix_s"),
                    "writer_deadline_unix_s": target.get("deadline_unix_s"),
                    "cleanup_outcomes": [entry["outcome"] for entry in completion.get(
                        "target_cleanups", [])]}
        first = min(value for value in (final.get("first_listed_unix_s"),
                                        completion.get("seen_after_removal_unix_s"),
                                        (completion.get("object_at_observation") or {}).get(
                                            "last_modified_unix_s")) if value is not None)
        end = target["window_end_unix_s"]
        return {"key": key, "visible": True, "first_visible_unix_s": first,
                "last_modified_unix_s": final.get("last_modified_unix_s"),
                "window_end_unix_s": end, "writer_deadline_unix_s": target["deadline_unix_s"],
                "writer_cutoff_unix_s": target["cutoff_unix_s"],
                "released_unix_s": completion.get("released_unix_s"),
                "first_visible_after_window_end_s": round(first - end, 3),
                "last_modified_after_window_end_s": round(final["last_modified_unix_s"] - end, 3),
                "first_visible_after_cutoff_s": round(first - target["cutoff_unix_s"], 3)
                if target.get("cutoff_unix_s") is not None else None,
                "first_visible_after_deadline_s": round(first - target["deadline_unix_s"], 3)
                if target.get("deadline_unix_s") is not None else None,
                "commit": target.get("commit"),
                "multipart_steps": steps, "bound_s": self.bound_s,
                "beyond_bound": max(first, final["last_modified_unix_s"]) - end > self.bound_s,
                "cleanup_outcomes": [entry["outcome"] for entry in completion.get(
                    "target_cleanups", [])]}

    def network_verdicts(self, record):
        """dropped_multipart_completion: the late-commit detector named the target."""
        if self.fault != "dropped_multipart_completion":
            return []
        timeline = self.completion_timeline(record)
        final = record.get("final_totals") or {}
        late = int(final.get("flush.late_commits.stored", 0)
                   + final.get("flush.late_commits.acknowledged", 0))
        detected = "late_commit" in timeline.get("cleanup_outcomes", []) and late >= 1
        return [("late_commit_detected", detected,
                 f"cleanup outcomes for the target {timeline.get('cleanup_outcomes')}; "
                 f"flush.late_commits {late}; target {timeline.get('key')}")]

    def numbers(self, record, during):
        """The network numbers plus the target's completion timeline."""
        numbers = super().numbers(record, during)
        timeline = self.completion_timeline(record)
        numbers.update({
            "target_first_visible_after_window_end_s": timeline.get(
                "first_visible_after_window_end_s"),
            "target_last_modified_after_window_end_s": timeline.get(
                "last_modified_after_window_end_s"),
            "target_first_visible_after_cutoff_s": timeline.get("first_visible_after_cutoff_s"),
            "late_commit_events_count": timeline.get("cleanup_outcomes", []).count("late_commit"),
        })
        return numbers

    def observations(self, record):
        """The network evidence plus the target's completion timeline."""
        observations = super().observations(record)
        observations["network"]["completion_timeline"] = self.completion_timeline(record)
        return observations


def dropped_completion_file(requests, armed_unix_s):
    """The file of the first values CompleteMultipartUpload the exporter sent
    through the completion front that NGINX logged without a 2xx after arming,
    or None. NGINX logs a request when it ends, so this is known once the
    store or the client closed the held response."""
    for entry in sorted(requests, key=lambda item: float(item.get("msec") or 0)):
        uri = entry.get("uri") or ""
        if (entry.get("operation") == "complete_multipart_upload"
                and "dataset=values/" in uri and _within(entry, armed_unix_s, None)
                and not str(entry.get("status") or "").startswith("2")):
            return uri.partition("?")[0].rsplit("/", 1)[-1]
    return None


def _dropped_multipart_met(case, seen) -> bool:
    """The target's completion landed in the store with its response dropped,
    and the writer settled the block one of two ways: its flush failed, the
    cleanup cutoff passed and the nack was retried (before the lost-completion
    probe), or it committed the block without a failed attempt (the probe found
    the object; nothing is nacked, so nothing is retried)."""
    target = seen.get("target") or {}
    if not (target.get("key") and seen.get("target_object")):
        return False
    if target.get("failure") is not None:
        return (seen["now_unix_s"] >= target["cutoff_unix_s"] + 1
                and seen["storage_nacks_count"] > 0 and seen["retried"])
    return target.get("deadline_unix_s") is None and target.get("commit") is not None


def upload_id_of(entry):
    """The `uploadId` an NGINX log entry's request URI names, or None."""
    query = (entry.get("request_uri") or "").partition("?")[2]
    ids = urllib.parse.parse_qs(query, keep_blank_values=True).get("uploadId")
    return ids[0] if ids else None


def _of_key(entry, key) -> bool:
    return (entry.get("uri") or "").partition("?")[0].endswith("/" + key)


def writer_aborts(requests, key) -> list:
    """The AbortMultipartUpload requests of `key` the store answered 2xx, each
    with the upload id it aborted."""
    return [{"msec": entry.get("msec"), "status": entry.get("status"),
             "upload_id": upload_id_of(entry)}
            for entry in requests
            if entry.get("operation") == "abort_multipart_upload" and _of_key(entry, key)
            and str(entry.get("status") or "").startswith("2")]


def held_completions(requests, key, armed_unix_s, release_unix_s) -> list:
    """The CompleteMultipartUpload requests of `key` the proxy held across the
    release: begun (their end less their request time, as NGINX logs them)
    after arming and before the release, and ended at or after it."""
    found = []
    for entry in requests:
        if entry.get("operation") != "complete_multipart_upload" or not _of_key(entry, key):
            continue
        try:
            end = float(entry["msec"])
            start = end - float(entry.get("request_time") or 0.0)
        except (KeyError, ValueError):
            continue
        if armed_unix_s <= start < release_unix_s <= end:
            found.append({"upload_id": upload_id_of(entry), "start_unix_s": round(start, 3),
                          "end_unix_s": end, "status": entry.get("status")})
    return found


def _held_multipart_met(case, seen) -> bool:
    """The target's completion was held and released at its instant.

    It needs: a confirmed absence of the object just before the release (a HEAD
    answered NotFound, `head_before_release == "absent"`; any other answer
    decides nothing); the release at its instant (`release_at`); and a held
    CompleteMultipartUpload of the target key (`held_completions`: begun while
    armed and before the release, ended at or after it) whose upload id is one
    the store still listed as open before the release or one the writer
    aborted. Since the lost-completion probe (Task 12a) the writer HEADs the
    name after its completion fails and, finding nothing, aborts that upload,
    so the released completion finds nothing to complete; before it, the
    writer left the upload open."""
    target = seen.get("target") or {}
    uploads = {upload.get("upload_id") for upload in seen.get("uploads_before_release") or []}
    aborted = {abort.get("upload_id") for abort in seen.get("aborts_before_release") or []}
    known = (uploads | aborted) - {None}
    held = [entry for entry in seen.get("held_completions") or []
            if entry.get("upload_id") in known]
    return (bool(target.get("key")) and seen.get("head_before_release") == "absent"
            and seen.get("object_before_release") is None and bool(held)
            and target.get("window_end_unix_s") is not None
            and seen.get("released_unix_s", 0) >= seen.get("release_at_unix_s", float("inf")))


FAULT_CONDITIONS.update({"dropped_multipart_completion": _dropped_multipart_met,
                         "held_multipart_completion": _held_multipart_met})

NETWORK_CASES = {"dropped_completion_response": CompletionCohortCase,
                 "dropped_multipart_completion": MultipartCompletionCase,
                 "held_multipart_completion": MultipartCompletionCase}


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
    "the flush deadline after arming and before the fault was removed; process: a graceful "
    "stop within the shutdown deadline and the cleanup cutoff (graceful_exit_problem), and "
    "kill_upload's multipart gate on part bytes the store lists (ProcessCase.multipart_met)",
    "orphaned_uploads_expected": "incomplete uploads other than those a killed engine left open "
    "(each listed under its key) number at most flush.abort_failures x (retry.max_retries + "
    "1): one counted failure is one failed write attempt, inside which the object store client "
    "retries a CreateMultipartUpload up to max_retries times, each try possibly creating an "
    "upload whose id the writer never receives (Task 16 fix round 1); process: the test's "
    "cleanup leaving none (orphan_verdict)",
}


def rejudge_process_checks(result) -> list:
    """The process-case verdicts `rejudge_fault_checks` re-judges from a run file.

    fault_observed passes only when it passed as recorded and the stored
    events meet the graceful-stop and multipart-gate rules; the orphans are
    judged again from the stored listing, expectations and cleanup.
    """
    fault = result["observations"]["fault"]
    process = fault["process"]
    numbers = fault["numbers"]
    statuses = {entry["name"]: entry["status"] for entry in result["checks"]}
    abort_timeout = duration_s(exporter_settings(result["config"]["effective"])["upload"]
                               ["abort_timeout"])
    deadline = numbers["admin_shutdown_deadline_s"]
    problems = []
    for event in process["events"]:
        if event["kind"] == "graceful":
            problem = graceful_exit_problem(event["exit"], deadline, abort_timeout)
            if problem:
                problems.append(f"event {event['ordinal']}: {problem}")
    if fault["fault"] == "kill_upload" and process["events"]:
        gate = process["events"][0]["gate"]
        if not ProcessCase.multipart_met(gate):
            problems.append("the multipart gate lists no part bytes in the store: "
                            f"{[upload.get('part_bytes') for upload in gate.get('open_uploads', [])]}")
    recorded = statuses.get("fault_observed")
    observed = recorded == measurement.STATUS_PASSED and not problems
    expected = process.get("expected_orphans") or {}
    # Runs before the per-failure allowance recorded the raw count.
    abort_failures = ((fault["orphaned_uploads_expected_max_count"] - len(expected))
                      // int(fault.get("uploads_per_abort_failure", 1)))
    passed, why = orphan_verdict(
        fault["orphaned_uploads"], expected, abort_failures, process.get("orphan_cleanup"),
        uploads_per_failure=uploads_per_abort_failure(
            exporter_settings(result["config"]["effective"])))
    status = {True: measurement.STATUS_PASSED, False: measurement.STATUS_FAILED}
    return [
        {"check": "fault_observed", "recorded": recorded, "rejudged": status[observed],
         "reason": "; ".join(problems) or (
             "graceful stops within the shutdown deadline and cleanup cutoff; multipart gate "
             f"on stored part bytes; recorded {recorded}")},
        {"check": "orphaned_uploads_expected",
         "recorded": statuses.get("orphaned_uploads_expected"), "rejudged": status[passed],
         "reason": why},
    ]


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
        return rejudge_process_checks(result)
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
    if isinstance((fault.get("numbers") or {}).get("flush_abort_failures_count"), int):
        passed, why = orphan_verdict(
            fault["orphaned_uploads"], {}, fault["numbers"]["flush_abort_failures_count"],
            uploads_per_failure=uploads_per_abort_failure(
                exporter_settings(result["config"]["effective"])))
        verdicts.append({"check": "orphaned_uploads_expected",
                         "recorded": statuses.get("orphaned_uploads_expected"),
                         "rejudged": measurement.STATUS_PASSED if passed
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
