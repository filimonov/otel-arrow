# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""The host monitor a measured run keeps beside itself.

It runs as its own small process, so that a harness busy producing load --
many Python threads competing for one interpreter lock -- can never stretch
the time between two observations. It depends on the standard library only
and starts in milliseconds.

The parent writes one JSON configuration line to its stdin, then commands
(`watch`, `unwatch`, `stop`), one JSON object per line. The monitor ticks on
schedule: each tick scans procfs for builds, enumerates the watched
engine's threads and requires every mapped worker thread, each on
exactly its allowed cores. A detection is written to stdout at once as
one JSON line; at `stop` it performs a last tick and writes its report. It
never stops or signals any other process.

The same scanning and affinity functions are imported by `measurement.py`,
so an in-process preflight scan applies exactly these rules.
"""
import json
import os
from pathlib import Path
import re
import select
import sys
import time


def parse_core_list(text: str):
    """Expand a kernel core list such as `0-3,8` into explicit ids."""
    cores = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, high = part.split("-", 1)
            cores.extend(range(int(low), int(high) + 1))
        else:
            cores.append(int(part))
    return sorted(cores)


def check_worker_affinity(observed, expected) -> None:
    """Abort unless every mapped worker thread may run on exactly its cores.

    Both arguments map a worker TID to a set of core ids. Every mapped TID
    must still be there -- a worker that vanished or was replaced is a
    changed mapping, not a pinned one -- and each observed allowed set must
    equal the expected one exactly, so a worker that may also run on an SMT
    sibling or any other core fails. A further thread with a worker's name
    passes only when it is confined to exactly one mapped worker's cores:
    a worker runtime's blocking-pool threads take its name and its affinity
    (the local file store runs its file writes there); any other
    worker-named thread is a changed mapping.
    """
    wanted_sets = [set(cores) for cores in expected.values()]
    extra = [
        tid for tid in sorted(set(observed) - set(expected))
        if observed[tid] is not None and observed[tid] not in wanted_sets
    ]
    if set(expected) - set(observed) or extra:
        raise AssertionError(
            f"affinity: worker TID mapping mismatch: observed "
            f"{sorted(observed)} expected {sorted(expected)}"
            + (f"; {extra} not confined to a mapped worker's cores" if extra else "")
        )
    for tid, wanted in sorted(expected.items()):
        if observed[tid] != set(wanted):
            actual = None if observed[tid] is None else sorted(observed[tid])
            raise AssertionError(
                f"affinity: tid={tid} actual={actual} expected={sorted(wanted)}"
            )


def observed_affinity(pid, tids, *, proc_root="/proc") -> dict:
    """The allowed core set of each named thread now, None when it is gone."""
    observed = {}
    for tid in tids:
        try:
            text = Path(f"{proc_root}/{pid}/task/{tid}/status").read_text()
        except OSError:
            observed[tid] = None
            continue
        allowed = None
        for line in text.splitlines():
            if line.startswith("Cpus_allowed_list:"):
                allowed = set(parse_core_list(line.split(":", 1)[1].strip()))
        observed[tid] = allowed
    return observed


def worker_threads(pid, names, *, proc_root="/proc") -> dict:
    """Every thread of `pid` whose name is a worker name, with its cores.

    The whole task list is enumerated, so a worker thread that appeared or
    replaced a mapped one is seen, not only the threads already mapped.
    Returns `{tid: allowed core set}`; a thread that exits while being read
    is left out, exactly as if it had exited a moment earlier.
    """
    found = {}
    for entry in Path(f"{proc_root}/{pid}/task").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            text = (entry / "status").read_text()
        except OSError:
            continue
        name, allowed = None, None
        for line in text.splitlines():
            if line.startswith("Name:"):
                name = line.split(":", 1)[1].strip()
            elif line.startswith("Cpus_allowed_list:"):
                allowed = set(parse_core_list(line.split(":", 1)[1].strip()))
        if name in names:
            found[int(entry.name)] = allowed
    return found


_SECRET_ARGUMENT = re.compile(
    r"(?i)((?:token|secret|password|passwd|key|credential)[A-Za-z0-9_-]*=)\S+"
)


def redact(cmdline: str) -> str:
    """A command line with the values of secret-looking arguments removed."""
    return _SECRET_ARGUMENT.sub(r"\1<redacted>", cmdline)


def is_container_build(cmdline: str) -> bool:
    """Whether a container client's command line builds an image."""
    words = cmdline.split()
    return any(word in ("build", "bake") for word in words[1:])


def processes(proc_root):
    """Every readable process: comm, parent and start ticks."""
    table = {}
    for entry in Path(proc_root).iterdir():
        if not entry.name.isdigit():
            continue
        try:
            comm = (entry / "comm").read_text().strip()
            stat = (entry / "stat").read_text()
        except OSError:
            continue
        try:
            fields = stat.rsplit(") ", 1)[1].split()
            parent = int(fields[1])
            start_ticks = int(fields[19])
        except (IndexError, ValueError):
            parent, start_ticks = 0, None
        table[int(entry.name)] = {
            "comm": comm, "ppid": parent, "start_ticks": start_ticks,
        }
    return table


def cmdline(proc_root, pid):
    """One process's command line, secrets redacted, or empty."""
    try:
        raw = (Path(proc_root) / str(pid) / "cmdline").read_bytes()
    except OSError:
        return ""
    text = raw.decode("ascii", "replace").replace("\x00", " ").strip()
    return redact(text)[:200]


def _ancestor_in(table, pid, daemons):
    """The daemon `pid` descends from, if any; never `pid` itself."""
    seen = set()
    current = table.get(pid, {}).get("ppid")
    while current and current not in seen:
        if current in daemons:
            return current
        seen.add(current)
        current = table.get(current, {}).get("ppid")
    return None


def _ancestry(table, pid, depth=8):
    """The parent chain of one process, as recorded evidence."""
    chain = []
    current = table.get(pid, {}).get("ppid")
    while current and len(chain) < depth:
        info = table.get(current)
        if info is None:
            break
        chain.append({"pid": current, "comm": info["comm"]})
        current = info["ppid"]
    return chain


def scan(proc_root, rules, own_pids):
    """The build processes one pass over procfs finds.

    A build command or a container build client is a build. A build daemon
    is not a build while it is idle, but any process descending from it is
    a build step.
    """
    table = processes(proc_root)
    daemons = {
        pid for pid, info in table.items() if info["comm"] in rules["build_daemons"]
    }
    found = []
    for pid, info in sorted(table.items()):
        if pid in own_pids:
            continue
        comm = info["comm"]
        reason = None
        if comm in rules["build_commands"]:
            reason = "build command"
        elif comm in rules["container_clients"]:
            if is_container_build(cmdline(proc_root, pid)):
                reason = "container build client"
        if reason is None and daemons:
            daemon = _ancestor_in(table, pid, daemons)
            if daemon is not None:
                reason = f"build step under daemon pid {daemon}"
        if reason is None:
            continue
        found.append(
            {
                "pid": pid,
                "comm": comm,
                "reason": reason,
                "cmdline": cmdline(proc_root, pid),
                "start_ticks": info["start_ticks"],
                "ancestry": _ancestry(table, pid),
                "observed_unix": time.time(),
            }
        )
    return found


class Monitor:
    """The tick loop and its bookkeeping."""

    def __init__(self, config):
        self.interval_s = float(config["interval_s"])
        self.limit_s = float(config["limit_s"])
        self.proc_root = config["proc_root"]
        self.rules = config["rules"]
        self.own_pids = set(config["own_pids"]) | {os.getpid()}
        self.watched = None
        self.ticks = 0
        self.last_tick_ns = None
        self.max_gap_s = None
        self.late = []
        self.late_count = 0
        self.observations = []
        self.observation_count = 0
        self.affinity_failures = []
        self.affinity_failure_count = 0

    def emit(self, message):
        """Write one JSON line to the parent at once."""
        sys.stdout.write(json.dumps(message, sort_keys=True) + "\n")
        sys.stdout.flush()

    def tick(self):
        """One scan and one affinity check, with the gap since the last."""
        now = time.monotonic_ns()
        if self.last_tick_ns is not None:
            gap = (now - self.last_tick_ns) / 1e9
            self.max_gap_s = gap if self.max_gap_s is None else max(self.max_gap_s, gap)
            if gap > self.limit_s:
                self.late_count += 1
                if len(self.late) < 50:
                    self.late.append({"gap_s": gap, "observed_unix": time.time()})
        self.last_tick_ns = now
        self.ticks += 1
        for entry in scan(self.proc_root, self.rules, self.own_pids):
            self.observation_count += 1
            if len(self.observations) < 50:
                self.observations.append(entry)
            self.emit({"type": "build", "entry": entry})
        if self.watched is not None:
            expected = self.watched["expected"]
            try:
                if self.watched["names"]:
                    # Every thread carrying a worker name is enumerated, so a
                    # new, extra or replacing worker thread is a changed
                    # mapping even for a single tick.
                    observed = worker_threads(
                        self.watched["pid"], self.watched["names"],
                        proc_root=self.proc_root,
                    )
                else:
                    observed = observed_affinity(
                        self.watched["pid"], expected, proc_root=self.proc_root
                    )
                check_worker_affinity(observed, expected)
            except (AssertionError, OSError) as error:
                detail = (
                    str(error)
                    if isinstance(error, AssertionError)
                    else f"affinity: engine tasks unreadable: {error}"
                )
                failure = {"detail": detail, "observed_unix": time.time()}
                self.affinity_failure_count += 1
                if len(self.affinity_failures) < 50:
                    self.affinity_failures.append(failure)
                self.emit({"type": "affinity", "entry": failure})
            except AssertionError as error:
                failure = {"detail": str(error), "observed_unix": time.time()}
                self.affinity_failure_count += 1
                if len(self.affinity_failures) < 50:
                    self.affinity_failures.append(failure)
                self.emit({"type": "affinity", "entry": failure})

    def command(self, message):
        """Apply one parent command; return False on `stop`."""
        if message["cmd"] == "watch":
            self.watched = {
                "pid": int(message["pid"]),
                "expected": {
                    int(tid): set(cores) for tid, cores in message["expected"].items()
                },
                "names": set(message.get("names") or ()),
            }
            self.emit({"type": "watching", "tids": sorted(self.watched["expected"])})
        elif message["cmd"] == "unwatch":
            self.watched = None
            self.emit({"type": "unwatched"})
        elif message["cmd"] == "stop":
            return False
        return True

    def report(self):
        """Everything this monitor saw, for the parent's result."""
        return {
            "type": "report",
            "ticks": self.ticks,
            "interval_s": self.interval_s,
            "limit_s": self.limit_s,
            "max_gap_s": self.max_gap_s,
            "gaps_over_limit_count": self.late_count,
            "gaps_over_limit": self.late,
            "observations": self.observations,
            "observation_count": self.observation_count,
            "affinity_failures": self.affinity_failures,
            "affinity_failure_count": self.affinity_failure_count,
        }


def main():
    """Read the configuration, then tick until told to stop."""
    buffered = b""

    def lines():
        """Complete lines read so far from stdin, without blocking."""
        nonlocal buffered
        complete = []
        while b"\n" in buffered:
            line, buffered = buffered.split(b"\n", 1)
            if line.strip():
                complete.append(json.loads(line))
        return complete

    while b"\n" not in buffered:
        chunk = os.read(0, 65536)
        if not chunk:
            return 1
        buffered += chunk
    monitor = Monitor(lines()[0])
    monitor.tick()
    monitor.emit({"type": "started", "pid": os.getpid()})
    step = int(monitor.interval_s * 10**9)
    next_tick = time.monotonic_ns() + step
    running = True
    while running:
        remaining = max(0.0, (next_tick - time.monotonic_ns()) / 1e9)
        readable, _, _ = select.select([0], [], [], remaining)
        if readable:
            chunk = os.read(0, 65536)
            if not chunk:
                # The parent is gone: nobody will read a report.
                return 1
            buffered += chunk
            for message in lines():
                if not monitor.command(message):
                    running = False
            continue
        monitor.tick()
        next_tick += step
        if next_tick < time.monotonic_ns():
            # A late tick is recorded as a gap, not caught up in a burst.
            next_tick = time.monotonic_ns() + step
    monitor.tick()
    monitor.emit(monitor.report())
    return 0


if __name__ == "__main__":
    sys.exit(main())
