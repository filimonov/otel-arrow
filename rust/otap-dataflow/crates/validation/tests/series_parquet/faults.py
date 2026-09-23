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
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import time
import unittest
import urllib.error
import urllib.request
import uuid

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
    from . import test_e2e
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement
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
    "disconnect_reset": BASE_PROBES,
    "dropped_completion_response": BASE_PROBES,
    "dns_nxdomain_timeout": BASE_PROBES + ("UDP DNS", "TCP DNS"),
    "tcp_ack_loss": BASE_PROBES + ("xt_bpf", "capture"),
    "containerized_engine": BASE_PROBES + ("engine launch",),
}


def coverage(probes, *, required: bool) -> dict:
    """Each fault class's availability from the probes that decide it.

    A class is `available` when all its probes passed on every store the
    preflight covered. Otherwise it is `unavailable` with the failed probes
    named, and `consequence` says what a lane does with it: a required lane
    fails, an optional one skips the class before any traffic.
    """
    classes = {}
    for fault, needed in sorted(FAULT_CLASS_PROBES.items()):
        failed = sorted(
            {
                f"{probe.get('store')}: {probe['name']}"
                for probe in probes
                if probe["name"] in needed and probe.get("passed") is not True
            }
        )
        missing = sorted(set(needed) - {probe["name"] for probe in probes})
        available = not failed and not missing
        classes[fault] = {
            "probes": list(needed),
            "status": "available" if available else "unavailable",
            "failed_probes": failed,
            "missing_probes": missing,
            "consequence": (
                "runs" if available else "fails the lane" if required
                else "skipped before traffic"
            ),
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


def owner_argv(*, name, network, image, run_id, control_dir, artifact_dir,
               engine_ports, cores=()) -> list:
    """`docker run` for the namespace owner: the only container with NET_ADMIN.

    It runs NGINX with the checked-in configuration and publishes, on host
    loopback only, NGINX and the Toxiproxy API at Docker-chosen ports and
    the engine's gRPC and admin ports at the same number on both sides.
    """
    argv = [
        "docker", "run", "--pull=never", "--detach", "--name", name,
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


def toxiproxy_argv(*, name, owner, image, run_id, cores=()) -> list:
    """`docker run` for Toxiproxy inside the owner's namespace, no added capability."""
    return [
        "docker", "run", "--pull=never", "--detach", "--name", name,
        "--label", f"{RUN_LABEL}={run_id}",
        "--network", f"container:{owner}",
    ] + _cpuset(cores) + [image, "-host=0.0.0.0", f"-port={TOXIPROXY_API_PORT}"]


# The environment an engine container inherits from the harness: logging
# and backtraces only, never the harness's whole environment.
ENGINE_ENVIRONMENT = ("RUST_LOG", "RUST_BACKTRACE")


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
        """Deliver one signal to the engine in its container."""
        _ = run_command(
            ["docker", "kill", "--signal", _signal_name(sig),
             self.container_id or self.name],
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
        """Fail unless every shared library of `binary` resolves in the image."""
        binary = Path(binary).resolve()
        if binary in self.ldd:
            return self.ldd[binary]
        name = f"series-fault-{self.rig.run_id}-ldd-{len(self.ldd) + 1}"
        done = run_command(
            ["docker", "run", "--rm", "--pull=never", "--name", name,
             "--label", f"{RUN_LABEL}={self.rig.run_id}", "--network", "none",
             "--volume", f"{binary}:{binary}:ro", self.rig.images["fault_tools"]["tag"],
             "ldd", str(binary)],
            timeout=DOCKER_TIMEOUT_S,
        )
        missing = [line.strip() for line in done["stdout"].splitlines() if "not found" in line]
        report = {"binary": str(binary), "command": done, "missing": missing,
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
            import yaml
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
            image=self.rig.images["fault_tools"]["tag"], run_id=self.rig.run_id,
            argv=argv, mounts=self._mounts(argv),
            user=f"{os.getuid()}:{os.getgid()}", env=env, cores=self.rig.engine_cores,
        )
        self.rig.record_container("engine", name)
        cli = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        process = ContainerProcess(cli, cidfile, name)
        self.launches.append({"name": name, "argv": command})
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
            state = _docker_json(["inspect", "--format", "{{json .State}}", process.name])
            if state and state.get("Running") and state.get("Pid"):
                pid = int(state["Pid"])
                self.launches[-1]["host_pid"] = pid
                self.launches[-1]["container_id"] = process.container_id
                self.rig.record_container("engine", process.name, pid=pid)
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

def parse_access_log(path) -> list:
    """NGINX's fault log, one dict per request, in the configured format."""
    entries = []
    try:
        lines = Path(path).read_text(errors="replace").splitlines()
    except OSError:
        return entries
    fields = ("msec", "method", "uri", "status", "upstream_status",
              "request_time", "upstream_response_time", "body_bytes_sent")
    for line in lines:
        parts = line.split()
        if len(parts) != len(fields):
            entries.append({"raw": line})
            continue
        entries.append(dict(zip(fields, parts)))
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

    def record_container(self, role, name, *, pid=None):
        """Remember one container this rig created, for evidence and cleanup."""
        for entry in self.containers:
            if entry["name"] == name:
                if pid is not None:
                    entry["host_pid"] = pid
                return
        self.containers.append({"role": role, "name": name, "host_pid": pid})

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
        self._start_owner()
        name = f"series-fault-{self.run_id}-toxiproxy"
        self.record_container("toxiproxy", name)
        done = run_command(toxiproxy_argv(
            name=name, owner=self.owner_id, image=self.images["toxiproxy"]["tag"],
            run_id=self.run_id, cores=self.cores,
        ))
        if done["exit_status"] != 0:
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
            if entry["host_pid"] is None:
                entry["host_pid"] = container_pid(entry["name"])
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
        name = f"series-fault-{self.run_id}-tools"
        for attempt in range(attempts):
            self.engine_ports = (test_e2e.free_port(), test_e2e.free_port())
            self.record_container("owner", name)
            done = run_command(owner_argv(
                name=name, network=self.network_id,
                image=self.images["fault_tools"]["tag"], run_id=self.run_id,
                control_dir=self.control_dir.resolve(),
                artifact_dir=self.artifact_dir.resolve(),
                engine_ports=self.engine_ports, cores=self.cores,
            ))
            if done["exit_status"] == 0:
                self.owner_id = done["stdout"].strip()
                return
            _ = run_command(["docker", "rm", "--force", name])
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

    def __exit__(self, *exc):
        """Recover what is still active, then remove what this rig created."""
        report = {"recovered": None, "removed": [], "errors": []}
        if self.active:
            try:
                self.recover()
                report["recovered"] = True
            except Exception as error:
                report["recovered"] = False
                report["errors"].append(f"recover: {error}")
        if self.owner_id:
            # The owner's tools write the artifacts as root; make them
            # readable to the harness user before the owner goes away.
            report["artifacts_readable"] = self.exec(
                ["chmod", "-R", "a+rX", ARTIFACT_MOUNT])["exit_status"] == 0
        for entry in reversed(self.containers):
            done = run_command(["docker", "rm", "--force", "--volumes", entry["name"]])
            report["removed"].append({"name": entry["name"], "exit_status": done["exit_status"]})
        if self.attached:
            self.store.detach(self.network_id)
            self.attached = False
        if self.network_id:
            done = run_command(["docker", "network", "rm", self.network_id])
            report["network_removed"] = done["exit_status"] == 0
            if done["exit_status"] != 0:
                report["errors"].append(f"network rm: {done['stderr'].strip()}")
        report["leftovers"] = leftovers(self.run_id)
        report["clean"] = not report["leftovers"]["containers"] and not report[
            "leftovers"]["networks"] and not report["errors"]
        self.cleanup_report = report
        return False

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
        """Recover every active fault, newest first; any error is a failure."""
        errors = []
        while self.active:
            entry = self.active.pop()
            _activate, recover = FAULTS[entry["name"]]
            try:
                entry["recovery"] = recover(self, entry["state"])
            except Exception as error:
                entry["recovery"] = {"error": str(error)}
                errors.append(f"{entry['name']}: {error}")
            entry["recovered_utc"] = measurement.utc_now()
            entry["recovered_monotonic_ns"] = time.monotonic_ns()
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
                      "address": self.store_ip, "port": STORE_PORT, "alias": STORE_ALIAS},
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
    subjects = [(entry["role"], entry["name"]) for entry in rig.containers
                if entry["role"] != "engine" or entry.get("host_pid")]
    subjects.append(("store", rig.store.name))
    for role, name in subjects:
        host = _docker_json(["inspect", "--format", "{{json .HostConfig}}", name]) or {}
        pid = container_pid(name)
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


def _store_transfer(rig, *, max_time):
    """One disposable HTTP PUT from the namespace straight to the store."""
    body = rig.artifact_dir / "probe-transfer.bin"
    if not body.exists():
        body.write_bytes(_payload(PROBE_TRANSFER_BYTES, "transfer"))
    return rig.exec([
        "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", str(max_time),
        "-X", "PUT", "--data-binary", f"@{ARTIFACT_MOUNT}/{body.name}",
        f"http://{rig.store_ip}:{STORE_PORT}/{rig.store.bucket}/fault-preflight-transfer",
    ], timeout=max_time + 30)


BPF_HOST_FIX = "sudo modprobe xt_bpf"


def probe_xt_bpf(rig) -> dict:
    """The store's pure ACKs can be dropped by an xt_bpf rule in the namespace.

    Compile the ACK-only filter for the store's address, insert it at the
    head of INPUT, run a disposable transfer from the namespace to the
    store under a capture, require a positive rule counter, delete that
    exact rule and require an unhindered transfer afterwards.
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
    counters = transfer = None
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
                    transfer = _record(probe, _store_transfer(rig, max_time=6))
                    listing = _record(probe, rig.exec(["iptables", "-w", "-v", "-S", "INPUT"]))
                    counters = rule_counters(listing["stdout"], ["-m bpf", "-j DROP"])
                finally:
                    deleted = _record(probe, rig.exec(["iptables", "-w", "-D", *rule]))
                if deleted["exit_status"] != 0:
                    problems.append(f"the exact rule could not be deleted: {deleted['stderr']}")
                if not (counters.get("packets") or 0) > 0:
                    problems.append(f"the ACK-only rule counted no packets: {counters}")
    except AssertionError as error:
        problems.append(str(error))
    counts = tshark_counts(rig, capture) if capture.host_file.exists() else {}
    for command in counts.get("commands", []):
        _record(probe, command)
    after = _record(probe, _store_transfer(rig, max_time=15))
    if after["exit_status"] != 0 or not after["stdout"].strip().isdigit():
        problems.append(f"a transfer after the rule was deleted failed: {after['stderr']}")
    rules = rig.iptables_rules()
    probe["restored"] = {"iptables_matches_baseline": rules == rig.baseline_rules,
                         "iptables_filter": rules}
    if not probe["restored"]["iptables_matches_baseline"]:
        problems.append("the filter table differs from its baseline after the probe")
    probe["evidence"].update({
        "rule": rule, "counters": counters,
        "transfer_under_rule": transfer and {
            "exit_status": transfer["exit_status"], "http_code": transfer["stdout"],
            "elapsed_s": transfer["elapsed_s"]},
        "capture": capture.as_json(),
        "captured_packets": counts.get("packets"),
        "captured_retransmissions": counts.get("retransmissions"),
        "modules_after": loaded_modules(),
    })
    return finish(probe, started, not problems, "; ".join(problems) or (
        f"xt_bpf dropped {counters['packets']} pure ACKs from the store; "
        f"{counts.get('retransmissions')} retransmissions captured"))


def probe_capture(rig) -> dict:
    """tcpdump captures the namespace's store traffic and tshark reads it."""
    probe = new_probe("capture", rig)
    started = time.monotonic()
    problems = []
    capture = Capture(rig, f"capture-{rig.store.kind}",
                      f"host {rig.store_ip} and tcp port {STORE_PORT}")
    try:
        with capture:
            transfer = _record(probe, _store_transfer(rig, max_time=15))
            if transfer["exit_status"] != 0:
                problems.append(f"the captured transfer failed: {transfer['stderr']}")
    except AssertionError as error:
        problems.append(str(error))
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
                         "retransmissions": counts.get("retransmissions")}
    return finish(probe, started, not problems, "; ".join(problems) or (
        f"{counts['packets']} packets captured and read back"))


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
        with test_e2e.DockerStore(kind) as store:
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
        images["fault_tools"]["packages"] = package_versions(images["fault_tools"]["tag"])
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
    result["coverage"] = coverage(probes, required=required)
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
