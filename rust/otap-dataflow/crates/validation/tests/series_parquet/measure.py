# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""The measurement command line: named cases, runs and result publication.

Run it from the Rust workspace:

    python3 -m crates.validation.tests.series_parquet.measure run \\
        --case harness-contracts --output-dir /tmp/series-contracts

Every producing subcommand writes one JSON document per run, publishes the
complete evidence tree into the report directory and exits nonzero when the
run failed. `stage-results` hands an already published tree to `git add` by
exact file name and is the step each commit block invokes first.
"""
import argparse
import collections
import json
import os
from pathlib import Path
import shutil
import sys
import time
import unittest

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement


# Subcommands whose measurement is long enough that it must be asked for.
LONG_COMMANDS = (
    "stages", "attribution", "capacity", "memory", "soak", "failures", "buffered",
)


def registered_cases() -> dict:
    """Every named case this command line can run today."""
    try:
        from . import soak
    except ImportError:
        import soak
    return {
        "harness-contracts": harness_contracts_case,
        "harness-local": harness_local_case,
        "launcher-ci": launcher_ci_case,
        "soak-strict": soak.soak_strict_case,
        "soak-buffered": soak.soak_buffered_case,
        "pr-soak-strict": soak.pr_soak_strict_case,
        "pr-soak-buffered": soak.pr_soak_buffered_case,
    }


def default_engine_cores(count=1):
    """The first `count` physical cores after the observability core.

    The controller runs its own observability pipeline on the first core
    the engine may use, so a worker never goes there. Each worker gets the
    lowest logical core of its own physical core. On a host with too few
    cores for that, the last available cores are used and the run fails its
    core floor rather than failing to start.
    """
    available = sorted(os.sched_getaffinity(0))
    group_of = {}
    for group in measurement.core_topology()["sibling_groups"]:
        for core in group:
            group_of[core] = tuple(group)
    groups = []
    for core in available:
        group = group_of.get(core, (core,))
        if group not in groups:
            groups.append(group)
    chosen = [
        min(core for core in group if core in available) for group in groups[1:]
    ][:count]
    if len(chosen) < count:
        # A host this small cannot publish a measurement, and the core floor
        # fails it; the workers still get cores of their own where they can.
        chosen = available[-count:]
    return tuple(chosen)


def harness_local_spec(**options) -> measurement.RunSpec:
    """The smallest publishable real-engine slice.

    One hundred requests of mixed supported signals through one worker on one
    explicitly chosen core, with one-second windows and exact no-retry
    multiplicity. The core defaults to the first physical core after the
    engine's observability core.
    """
    case = options.get("case", "harness-local")
    topology = options.get("topology", "strict")
    cores = tuple(options.get("cores") or default_engine_cores(1))
    ordinal = int(options.get("ordinal", 1))
    interval_s = int(options.get("interval_s", 1))
    workload = measurement.Workload(
        requests=int(options.get("requests", 100)),
        records_per_request=int(options.get("records_per_request", 100)),
        body_bytes=int(options.get("body_bytes", 1024)),
        series=int(options.get("series", 100)),
        metrics_every=int(options.get("metrics_every", 5)),
    )
    return measurement.RunSpec(
        run_id=measurement.RunSpec.build_run_id(
            case, topology, "local", cores, interval_s, ordinal
        ),
        case=case,
        topology=topology,
        store="local",
        cores=cores,
        workload=workload,
        interval_s=interval_s,
        duration_s=int(options.get("duration_s", 30)),
        overrides=dict(options.get("overrides", {})),
    )




# How long a measured phase may take to become ready, and to drain. Both
# are deadlines on observable conditions, never waits that prove anything.
READY_DEADLINE_S = 60
DRAIN_DEADLINE_S = 120
SHUTDOWN_DEADLINE_S = 180

# How often the background sampler observes the engine, and the engine's own
# telemetry collection interval in the example configuration.
SAMPLE_PERIOD_S = 0.25
REPORTING_INTERVAL_S = 1.0

# The fewest collection epochs each worker must answer in each engine
# lifetime, and the longest a worker's uptime may go without advancing.
MINIMUM_EPOCHS = 3
STALE_COLLECTIONS = 3

# Metrics a measured local run compares against its baseline, with the
# direction in which each one gets worse. Everything else a run observes is
# recorded under `observations` for review but is not a compared quantity.
LOCAL_METRIC_DIRECTIONS = {
    "records_acked_records": measurement.HIGHER_IS_BETTER,
    "throughput_records_per_s": measurement.HIGHER_IS_BETTER,
    "ack_latency_p50_s": measurement.LOWER_IS_BETTER,
    "ack_latency_p99_s": measurement.LOWER_IS_BETTER,
    "peak_rss_bytes": measurement.LOWER_IS_BETTER,
    "missing_records": measurement.LOWER_IS_BETTER,
    "unexpected_records": measurement.LOWER_IS_BETTER,
    "corrupt_records": measurement.LOWER_IS_BETTER,
    "duplicate_records": measurement.LOWER_IS_BETTER,
}


def engine_merge(spec):
    """The deep-merged measurement overrides one spec asks for."""
    merge = {}
    if "exporter" in spec.overrides:
        merge["exporter"] = spec.overrides["exporter"]
    if "receiver" in spec.overrides:
        merge["receiver"] = {"protocols": {"grpc": spec.overrides["receiver"]}}
    if "engine" in spec.overrides:
        merge["engine"] = spec.overrides["engine"]
    return merge


def buffer_inventory(path) -> dict:
    """The identity of a buffer directory and of each per-core directory."""
    path = Path(path)
    if not path.is_dir():
        return {"exists": False}
    status = path.stat()
    cores = {}
    for entry in sorted(path.iterdir()):
        if entry.is_dir():
            inner = entry.stat()
            cores[entry.name] = {
                "inode": inner.st_ino,
                "files": sorted(
                    str(item.relative_to(entry)) for item in entry.rglob("*")
                    if item.is_file()
                ),
            }
    return {
        "exists": True,
        "path": str(path),
        "device": status.st_dev,
        "inode": status.st_ino,
        "cores": cores,
    }


def await_ready(engine, spec, buffered):
    """The first sample in which every requested worker answered."""
    expected = len(spec.cores)

    def observe():
        """One sample, or None while the engine is still coming up."""
        try:
            return measurement.sample_engine(
                engine, expected_workers=expected, buffered=buffered
            )
        except (AssertionError, OSError, ValueError) as error:
            return {"error": str(error)}

    return measurement.wait_until(
        observe,
        lambda sample: "workers" in sample
        and len(sample["workers"]) == expected
        and all(worker["reported"] for worker in sample["workers"].values()),
        deadline_ns=time.monotonic_ns() + READY_DEADLINE_S * 10**9,
        description=f"every one of {expected} workers answering a collection",
    )


def await_window_start(interval_s):
    """Wait until just after an aligned window boundary.

    Blocks are sealed on wall-clock boundaries, so an input that starts at a
    random phase of the window has an acknowledgement latency that varies
    by up to a whole window between otherwise identical runs. Starting just
    after a boundary removes that phase from the comparison.
    """
    def phase():
        """Where in its window the wall clock is now."""
        return time.time() % interval_s

    return measurement.wait_until(
        phase,
        lambda value: 0.02 <= value <= 0.2,
        deadline_ns=time.monotonic_ns() + int((2 * interval_s + 1) * 10**9),
        description="the start of an aligned window",
    )


def stale_gaps(samples):
    """The longest time each worker's uptime went without advancing."""
    last = {}
    longest = {}
    for sample in samples:
        for key, worker in sample["workers"].items():
            now = sample["monotonic_ns"]
            previous = last.get(key)
            if previous is None or worker["uptime_s"] != previous[1]:
                if previous is not None:
                    longest[key] = max(longest.get(key, 0), (now - previous[0]) / 1e9)
                last[key] = (now, worker["uptime_s"])
            else:
                longest[key] = max(longest.get(key, 0), (now - previous[0]) / 1e9)
    return longest


def answered_epochs(samples):
    """How many collection epochs each worker answered in `samples`."""
    tracker = measurement.EpochTracker()
    answered = collections.Counter()
    for sample in samples:
        for key in tracker.observe(sample):
            if measurement.worker_reported(sample["workers"][key]):
                answered[key] += 1
    return answered


def await_answered_epochs(sampler, workers, *, deadline_ns):
    """Keep sampling until every worker has answered MINIMUM_EPOCHS epochs.

    A release engine can take its input and drain inside one or two
    collections. The lifetime's own samples must still span enough answered
    epochs to be a measurement, so the sampler keeps running until they do:
    an observable condition under a deadline, not a wait. Returns the
    fewest answered epochs of any worker once it is enough.
    """
    def fewest():
        """The fewest answered epochs any worker has in the samples."""
        answered = answered_epochs(list(sampler.samples))
        return min(answered.get(key, 0) for key in workers)

    return measurement.wait_until(
        fewest,
        lambda count: count >= MINIMUM_EPOCHS,
        deadline_ns=deadline_ns,
        description=f"{MINIMUM_EPOCHS} answered epochs of every worker",
    )


class EnginePhase:
    """One engine lifetime of a measured run: ready, input, drain."""

    def __init__(self, label, engine, spec, controls, buffered):
        self.label = label
        self.engine = engine
        self.spec = spec
        self.controls = controls
        self.buffered = buffered
        self.sampler = None
        self.idle = None
        self.inputs = []
        self.drain = None
        self.workers = None

    def ready(self, edge):
        """Wait for every worker, verify its affinity and start sampling."""
        sample = await_ready(self.engine, self.spec, self.buffered)
        self.workers = measurement.worker_identities(sample)
        roles = {"engine": (self.engine.pid, list(self.spec.cores))}
        if edge == "start":
            snapshot = self.controls.snapshot(
                "start", roles, workers=self.workers,
                requested_cores=list(self.spec.cores),
            )
        else:
            snapshot = self.controls.checkpoint(
                edge, roles, workers=self.workers,
                requested_cores=list(self.spec.cores),
            )
        self.controls.watch_workers(self.engine.pid, snapshot)
        self.sampler = measurement.Sampler(
            self.engine,
            expected_workers=len(self.spec.cores),
            buffered=self.buffered,
            period_s=SAMPLE_PERIOD_S,
            phase=self.label,
        )
        self.idle = self.sampler.once()
        self.sampler.start()
        return snapshot

    def send(self, producer, indexes):
        """Send one batch of requests starting at a window boundary."""
        _ = await_window_start(self.spec.interval_s)
        self.controls.raise_if_invalid()
        outcome = producer.send(indexes)
        self.inputs.append(outcome)
        self.controls.raise_if_invalid()
        return outcome

    def drained(self):
        """Prove the engine drained, then stop sampling."""
        started = time.monotonic_ns()
        report = measurement.observe_drain(
            lambda: measurement.sample_engine(
                self.engine,
                expected_workers=len(self.spec.cores),
                buffered=self.buffered,
            ),
            expected_workers=len(self.spec.cores),
            buffered=self.buffered,
            deadline_ns=started + DRAIN_DEADLINE_S * 10**9,
        )
        self.drain = {
            key: report[key]
            for key in (
                "drained", "empty_epochs_by_worker", "unanswered_epochs_by_worker",
                "epochs", "sample_count",
            )
        }
        self.drain["duration_s"] = (time.monotonic_ns() - started) / 1e9
        _ = await_answered_epochs(
            self.sampler,
            [worker["key"] for worker in self.workers],
            deadline_ns=time.monotonic_ns() + READY_DEADLINE_S * 10**9,
        )
        self.sampler.stop()
        self.controls.raise_if_invalid()
        return report

    def summary(self):
        """What this lifetime observed, for the result."""
        samples = self.sampler.samples if self.sampler else []
        residuals = (
            measurement.rss_residuals(samples, self.idle) if self.idle else []
        )
        return {
            "label": self.label,
            "pid": self.engine.pid,
            "config_sha256": self.engine.config_sha256,
            "edges": [list(edge) for edge in self.engine.edges],
            "cores": self.engine.cores,
            "buffer_path": str(self.engine.buffer_path)
            if self.engine.buffer_path else None,
            "workers": self.workers,
            "idle_rss_bytes": self.idle["process_rss_bytes"] if self.idle else None,
            "inputs": self.inputs,
            "drain": self.drain,
            "sample_count": len(samples),
            "sampler_errors": self.sampler.errors if self.sampler else [],
            "answered_epochs_by_worker": dict(answered_epochs(samples)),
            "stale_gap_s_by_worker": stale_gaps(samples),
            "residuals": residuals,
        }


def engine_binary() -> Path:
    """The engine binary a measured run launches: the release build.

    `DF_ENGINE` may name another binary, but `prepare_build` refuses any
    profile other than release, so a debug engine can never be measured.
    """
    return Path(
        os.environ.get(
            "DF_ENGINE", measurement.test_e2e.WORKSPACE / "target/release/df_engine"
        )
    )


# The only build profile a measured or publishable case may run. A debug
# engine's memory and speed describe the debug build, not the exporter.
MEASURED_PROFILE = "release"


def prepare_build(*, require_release=True) -> dict:
    """The engine build and source provenance, gathered before any lease.

    Hashing the binary and asking `rustc` for its version are work this
    harness does, and a `rustc` running while the monitor watches is a
    concurrent build; both happen before the host controls open. Any
    profile other than release is refused here, before anything runs,
    unless `require_release` is false: a run that publishes nothing and
    evaluates no baseline (a CI functional check) may use the fixture
    suite's debug engine, and records its profile.
    """
    binary = engine_binary()
    if not binary.is_file():
        raise AssertionError(
            f"no engine binary at {binary}; build the release engine with "
            f"cargo build --release --locked -p otel-arrow-dfe --bin df_engine "
            f"--features series-parquet,aws,durable-buffer"
        )
    build = measurement.engine_build(binary)
    if require_release and build["profile"] != MEASURED_PROFILE:
        raise AssertionError(
            f"measured cases require a release df_engine; {binary} is a "
            f"{build['profile']} build. Build with --release, or point "
            f"DF_ENGINE at target/release/df_engine"
        )
    return {"build": build, "git": measurement.git_provenance()}


def local_experiment(spec, result, output_dir, controls, *, restart=False,
                     publishable=True, provenance=None):
    """The measured body of a local-filesystem run, strict or buffered.

    Roles are placed on physical cores first; the engine then starts with
    its workers pinned through a `core_set`, the start snapshot maps and
    verifies every worker thread before any input, the monitor re-checks
    those threads on every tick, and the producer sends the deterministic
    workload exactly once per request from a window boundary. After input
    the drain proof must see three empty answered collections per worker.
    A buffered run with `restart` sends half its requests, drains, stops the
    engine, starts a fresh one on the same cores and the same retained
    buffer directory, verifies both, and sends the rest. The end snapshot is
    taken after the final drain while the workers still run; the engine is
    then shut down through the admin API and the oracle reads every file
    back through both readers.
    """
    output_dir = Path(output_dir)
    # A host that cannot publish records monitor tick gaps as an observation
    # rather than failing the coverage gate on them.
    controls.coverage_gaps_hard = publishable
    buffered = spec.topology == "buffered"
    topology = measurement.core_topology()
    allocation = measurement.role_allocation(
        topology["sibling_groups"], sorted(os.sched_getaffinity(0)), spec.cores,
        roles=measurement.CASE_ROLES["engine"], strict=publishable,
    )
    controls.allocate(allocation)
    # The harness is the producer and the reader; both snapshots record its
    # threads, whose sender and reader threads are pinned to their roles.
    controls.register("harness", os.getpid())
    if provenance is None:
        raise AssertionError(
            "the build provenance is gathered before the host controls open"
        )
    if provenance["build"]["profile"] != MEASURED_PROFILE:
        raise AssertionError(
            f"measured cases require a release df_engine, not "
            f"{provenance['build']['profile']}"
        )
    result["environment"]["build"] = provenance["build"]
    result["environment"]["git"] = provenance["git"]
    buffer_path = output_dir / "buffer" if buffered else None
    merge = engine_merge(spec)
    phases = []
    engine = None
    binary = Path(provenance["build"]["binary"])
    ledger = measurement.Ledger(output_dir / "ledger.sqlite")
    data_root = None
    try:
        first_root = output_dir / "engine-1"
        first_root.mkdir(parents=True, exist_ok=True)
        engine = measurement.test_e2e.Engine(
            first_root,
            interval=f"{spec.interval_s}s",
            topology=spec.topology,
            buffer_path=buffer_path,
            cores=list(spec.cores),
            merge=merge,
            binary=binary,
        )
        data_root = engine.data
        # The receiver's port is chosen fresh for every launch.
        result["ephemeral_values"] = {
            "<receiver_listening_addr>": f"127.0.0.1:{engine.grpc_port}"
        }
        result["config"]["effective"] = engine.config
        result["config"]["effective_sha256"] = engine.config_sha256
        result["config"]["edges"] = [list(edge) for edge in engine.edges]
        record_graph(result, engine, spec.topology)
        phase = EnginePhase("engine-1", engine, spec, controls, buffered)
        phases.append(phase)
        phase.ready("start")
        producer = measurement.Producer(
            engine.channel,
            ledger,
            spec.workload,
            cores=allocation.get("producer", []),
            timeout_s=spec.producer_timeout_s,
            max_in_flight=spec.max_in_flight,
        )
        indexes = list(range(spec.workload.requests))
        if buffered and restart:
            half = len(indexes) // 2
            _ = phase.send(producer, indexes[:half])
            _ = phase.drained()
            inventory = buffer_inventory(buffer_path)
            controls.unwatch_workers()
            engine.shutdown(SHUTDOWN_DEADLINE_S)
            engine.close()
            record_event(result, "engine_stopped_for_restart", str(engine.pid))
            second_root = output_dir / "engine-2"
            second_root.mkdir(parents=True, exist_ok=True)
            previous = engine
            engine = measurement.test_e2e.Engine(
                second_root,
                storage={"file": {"base_uri": str(previous.data)}},
                interval=f"{spec.interval_s}s",
                topology=spec.topology,
                buffer_path=buffer_path,
                cores=list(spec.cores),
                merge=merge,
                binary=binary,
            )
            result["config"]["effective_restart"] = engine.config
            record_restart(result, previous, engine, inventory)
            phase = EnginePhase("engine-2", engine, spec, controls, buffered)
            phases.append(phase)
            phase.ready("restart")
            producer = measurement.Producer(
                engine.channel,
                ledger,
                spec.workload,
                cores=allocation.get("producer", []),
                timeout_s=spec.producer_timeout_s,
                max_in_flight=spec.max_in_flight,
            )
            _ = phase.send(producer, indexes[half:])
            _ = phase.drained()
        else:
            _ = phase.send(producer, indexes)
            _ = phase.drained()
        _ = controls.snapshot(
            "end",
            {"engine": (engine.pid, list(spec.cores))},
            workers=phase.workers,
            requested_cores=list(spec.cores),
        )
        controls.unwatch_workers()
        engine.shutdown(SHUTDOWN_DEADLINE_S)
        record_event(result, "engine_shut_down", str(engine.pid))
    finally:
        controls.unwatch_workers()
        for phase in phases:
            if phase.sampler is not None:
                phase.sampler.stop()
        if engine is not None:
            engine.close()
        result["observations"] = {"phases": [phase.summary() for phase in phases]}
        result["samples"] = [
            measurement.compact_sample(sample)
            for phase in phases
            for sample in (phase.sampler.samples if phase.sampler else [])
        ]
    # A restart is a durability event: the buffer may replay bundles whose
    # acknowledgement it had not yet persisted, and at-least-once delivery
    # allows that. Such a run counts every copy instead of requiring one.
    replayable = buffered and restart
    oracle = measurement.run_pinned(
        allocation.get("reader") or sorted(os.sched_getaffinity(0)),
        measurement.read_oracle,
        data_root,
        ledger,
        require_all=True,
        healthy=not replayable,
        workload=spec.workload,
    )
    counts = ledger.counts()
    latencies = ledger.acknowledgement_latencies_s()
    ledger.close()
    settle_local_result(
        result, spec, phases, oracle, counts, latencies, output_dir,
        replayable=replayable,
    )


def record_event(result, kind, detail=""):
    """Append one event to a result."""
    measurement.record_event(result, kind, detail)


def record_graph(result, engine, topology):
    """The graph check: the edges that were launched are the expected ones."""
    expected = [list(edge) for edge in measurement.test_e2e.EXPECTED_EDGES[topology]]
    actual = [list(edge) for edge in engine.edges]
    result["checks"].append(
        measurement.check(
            "graph_edges",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED if actual == expected else measurement.STATUS_FAILED,
            f"{topology}: {actual}",
        )
    )


def record_restart(result, previous, engine, inventory):
    """Checks that a restart kept the graph, the cores and the buffer."""
    after = buffer_inventory(engine.buffer_path)
    retained = (
        inventory.get("exists")
        and after.get("exists")
        and engine.buffer_path == previous.buffer_path
        and (inventory["device"], inventory["inode"]) == (after["device"], after["inode"])
        and all(
            name in after["cores"] and after["cores"][name]["inode"] == entry["inode"]
            for name, entry in inventory["cores"].items()
        )
        and bool(inventory["cores"])
    )
    result["observations_restart"] = {
        "buffer_before": inventory,
        "buffer_after_launch": after,
        "previous_pid": previous.pid,
        "pid": engine.pid,
    }
    checks = result["checks"]
    checks.append(
        measurement.check(
            "buffer_path_retained",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED if retained else measurement.STATUS_FAILED,
            f"{previous.buffer_path} -> {engine.buffer_path}; per-core "
            f"directories {sorted(inventory.get('cores', {}))}",
        )
    )
    same = engine.cores == previous.cores and engine.edges == previous.edges
    checks.append(
        measurement.check(
            "restart_graph_and_cores_unchanged",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED if same else measurement.STATUS_FAILED,
            f"cores {previous.cores} -> {engine.cores}; edges {previous.edges} "
            f"-> {engine.edges}",
        )
    )


def settle_local_result(result, spec, phases, oracle, counts, latencies, output_dir,
                        *, replayable=False):
    """Metrics, observations and correctness checks of a local run.

    In a `replayable` run the number of duplicated records depends on how
    far the buffer had persisted its progress when it stopped, so it is not
    a compared metric: it is recorded, with the multiplicity histogram, as
    an observation and an event.
    """
    checks = result["checks"]
    duplicates = sum(
        count for copies, count in oracle["multiplicity_histogram"].items()
        if int(copies) > 1
    )
    delivered = (
        oracle["passed"]
        and counts["requests_acked_count"] == counts["requests_attempted_count"]
        and counts["requests_attempted_count"] == spec.workload.requests
    )
    checks.append(
        measurement.check(
            "delivery",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED if delivered else measurement.STATUS_FAILED,
            f"acked {counts['requests_acked_count']}/"
            f"{counts['requests_attempted_count']} requests; problems "
            f"{oracle['problems']}; readers {oracle['readers']}",
        )
    )
    summaries = result["observations"]["phases"]
    problems = sample_problems(summaries)
    checks.append(
        measurement.check(
            "minimum_samples",
            measurement.CHECK_HARD,
            measurement.STATUS_FAILED if problems or not summaries else measurement.STATUS_PASSED,
            "; ".join(problems)
            or f"{sum(summary['sample_count'] for summary in summaries)} samples",
        )
    )
    peak_rss = max(
        (sample["process_rss_bytes"] for sample in result["samples"]), default=0
    )
    residual_checks = [
        measurement.residual_check(summary["residuals"], peak_rss)
        for summary in summaries
    ]
    failed = [entry for entry in residual_checks if entry["status"] != measurement.STATUS_PASSED]
    checks.append(
        failed[0] if failed else (
            residual_checks[0] if residual_checks else measurement.residual_check([], 0)
        )
    )
    input_s = sum(
        (entry["finished_ns"] - entry["started_ns"]) / 1e9
        for summary in summaries
        for entry in summary["inputs"]
    )
    result["metrics"] = {
        "records_acked_records": counts["records_acked_count"],
        "throughput_records_per_s": (
            counts["records_acked_count"] / input_s if input_s > 0 else None
        ),
        "ack_latency_p50_s": measurement.percentile(latencies, 0.50) if latencies else None,
        "ack_latency_p99_s": measurement.percentile(latencies, 0.99) if latencies else None,
        "peak_rss_bytes": peak_rss or None,
        "missing_records": oracle["missing_record_count"],
        "unexpected_records": oracle["unexpected_record_count"],
        "corrupt_records": oracle["corrupt_record_count"],
        "duplicate_records": duplicates,
    }
    if replayable:
        del result["metrics"]["duplicate_records"]
        result["observations"]["restart_replay_duplicate_records"] = duplicates
        if duplicates:
            record_event(
                result,
                "restart_replayed_acknowledged_records",
                f"{duplicates} records stored more than once: "
                f"{oracle['multiplicity_histogram']}",
            )
    for name, value in result["metrics"].items():
        if value is None:
            result["metrics_unavailable"][name] = "no acknowledged input to measure"
    result["metric_directions"] = {
        name: LOCAL_METRIC_DIRECTIONS[name] for name in result["metrics"]
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    result["observations"].update(
        {
            "ledger": counts,
            "ack_latency_max_s": max(latencies) if latencies else None,
            "input_duration_s": input_s,
            "oracle": {
                key: oracle[key]
                for key in (
                    "part_file_count", "descriptor_identity_count", "values_key_count",
                    "series_cardinality", "actual_row_count", "readers",
                    "expected_record_count", "missing_record_count",
                    "unexpected_record_count", "corrupt_record_count",
                    "multiplicity_histogram", "stored_rows_by_kind", "problems",
                )
                if key in oracle
            },
            "residual_checks": residual_checks,
        }
    )
    result["artifacts"] = [
        dict(measurement.file_entry(path), kind=kind, retention=str(output_dir))
        for path, kind in sorted(
            [(path, "engine_log") for path in output_dir.glob("engine-*/engine.log")]
            + [(path, "engine_config") for path in output_dir.glob("engine-*/pipeline.yaml")]
            + [(output_dir / "ledger.sqlite", "ledger")]
        )
        if path.is_file()
    ]
    result["status"] = measurement.STATUS_PASSED


def sample_problems(summaries) -> list:
    """Why the engine lifetimes in `summaries` are not enough of a sample.

    Every lifetime must have sampled without errors, every worker must have
    answered `MINIMUM_EPOCHS` collection epochs, and no worker's uptime may
    have gone stale for more than `STALE_COLLECTIONS` collections.
    """
    problems = []
    for summary in summaries:
        if summary["sampler_errors"]:
            problems.append(f"{summary['label']}: {len(summary['sampler_errors'])} sampler errors")
        for worker in summary["workers"] or []:
            answered = summary["answered_epochs_by_worker"].get(worker["key"], 0)
            if answered < MINIMUM_EPOCHS:
                problems.append(
                    f"{summary['label']}: {worker['key']} answered {answered} epochs"
                )
        for key, gap in summary["stale_gap_s_by_worker"].items():
            if gap > STALE_COLLECTIONS * REPORTING_INTERVAL_S + SAMPLE_PERIOD_S:
                problems.append(f"{summary['label']}: {key} stale for {gap:.2f}s")
    return problems


def harness_local_case(output_dir, report_dir=None, **options) -> dict:
    """The smallest publishable real-engine run, and its index.

    One strict local run under the full host controls, published with its
    baseline decision, then summarized by `harness-local.json`.
    """
    output_dir = Path(output_dir)
    spec = harness_local_spec(**options)
    child = run_child(spec, output_dir, report_dir=report_dir, provenance=prepare_build())
    return write_index(
        "harness-local", output_dir, report_dir, [child], publishable=True
    )


def launcher_ci_case(output_dir, report_dir=None, **options) -> dict:
    """The fast launcher lane: legacy suite, strict smoke, buffered smoke.

    The original end-to-end suite runs first with Docker required, then one
    strict and one buffered local smoke run under the host controls. The
    buffered smoke restarts its engine on the same cores and the same
    retained buffer directory halfway through. Each smoke is an independent
    result file; `launcher-ci.json` indexes them with the legacy outcome.

    `publish=false` is the CI mode for a runner that is not a publishable
    measurement host: nothing reaches the report directory, no baseline is
    evaluated, and the index fails whenever a child fails any hard gate
    except the core floor such a host necessarily fails; the RSS
    reconciliation is one of the gates it enforces.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    publishable = bool(options.pop("publish", True))
    legacy = bool(options.pop("legacy_tests", True))
    if not publishable:
        report_dir = output_dir
    legacy_outcome = run_legacy_suite(output_dir) if legacy else None
    provenance = prepare_build()
    children = []
    for topology in ("strict", "buffered"):
        spec = harness_local_spec(case="launcher-ci", topology=topology, **options)
        children.append(
            run_child(
                spec, output_dir / spec.run_id, report_dir=report_dir,
                restart=topology == "buffered", publishable=publishable,
                copy_to=output_dir, provenance=provenance,
            )
        )
    return write_index(
        "launcher-ci", output_dir, report_dir, children, publishable=publishable,
        legacy=legacy_outcome,
    )


def run_child(spec, output_dir, *, report_dir=None, restart=False,
              publishable=True, copy_to=None, provenance=None):
    """One measured run; its failure is recorded, never raised past the index."""
    output_dir = Path(output_dir)

    def experiment(spec, result, directory, controls):
        """The local experiment with this child's options."""
        local_experiment(
            spec, result, directory, controls, restart=restart,
            publishable=publishable, provenance=provenance,
        )

    try:
        result = run_case(
            spec, output_dir, experiment=experiment, report_dir=report_dir,
            evaluate=publishable,
        )
    except Exception as error:
        sys.stderr.write(f"{spec.run_id}: {type(error).__name__}: {error}\n")
        result = json.loads(
            (output_dir / f"{spec.run_id}.json").read_text(encoding="ascii")
        )
    if copy_to is not None and Path(copy_to) != output_dir:
        # The index enumerates its children from its own directory.
        for entry in [{"name": f"{spec.run_id}.json"}] + result["baseline_files"]:
            _ = shutil.copyfile(output_dir / entry["name"], Path(copy_to) / entry["name"])
    return result


# The one hard gate a non-publishable host is expected to fail: it is too
# small to publish, which is why it runs in this mode. Every other hard gate
# a child fails also fails the CI index.
HOST_CAPACITY_CHECKS = ("physical_cores_sufficient",)

# The gates a CI child must record: the evaluator's own required set, less
# the host-capacity gate. Derived, so the two can never drift apart.
CI_REQUIRED_CHECKS = tuple(
    name for name in measurement.REQUIRED_HARD_CHECKS
    if name not in HOST_CAPACITY_CHECKS
)


def ci_failures(child) -> list:
    """The hard gates that fail one child in the non-publishable CI mode.

    Every gate the baseline evaluator requires, except the host-capacity
    gate a runner too small to publish on necessarily fails, must be present
    and passed; an absent gate fails. Every other hard check the child
    recorded must have passed too.
    """
    statuses = {}
    for entry in child["checks"]:
        if entry["kind"] == measurement.CHECK_HARD:
            statuses.setdefault(entry["name"], []).append(entry["status"])
    failed = [name for name in CI_REQUIRED_CHECKS if name not in statuses]
    for name, recorded in sorted(statuses.items()):
        if name in HOST_CAPACITY_CHECKS:
            continue
        if any(status != measurement.STATUS_PASSED for status in recorded):
            failed.append(name)
    return sorted(set(failed))


def run_legacy_suite(output_dir) -> dict:
    """The original end-to-end suite, with Docker required, in this process."""
    try:
        from . import test_e2e
    except ImportError:
        import test_e2e
    started = time.monotonic_ns()
    suite = unittest.TestLoader().loadTestsFromModule(test_e2e)
    log = Path(output_dir) / "legacy-e2e.log"
    previous = os.environ.get("SERIES_REQUIRE_DOCKER")
    os.environ["SERIES_REQUIRE_DOCKER"] = "1"
    try:
        with open(log, "w", encoding="ascii", errors="replace") as stream:
            outcome = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    finally:
        if previous is None:
            os.environ.pop("SERIES_REQUIRE_DOCKER", None)
        else:
            os.environ["SERIES_REQUIRE_DOCKER"] = previous
    return {
        "tests_count": outcome.testsRun,
        "failures_count": len(outcome.failures),
        "errors_count": len(outcome.errors),
        "skips_count": len(outcome.skipped),
        "successful": outcome.wasSuccessful() and not outcome.skipped,
        "summary": _describe(outcome),
        "elapsed_s": (time.monotonic_ns() - started) / 1e9,
        "log": str(log),
    }


# The original suite's size. A different count is changed legacy behavior.
LEGACY_TEST_COUNT = 19


def write_index(case, output_dir, report_dir, children, *, publishable, legacy=None,
                purposes=None):
    """Summarize child runs in one index file and publish the tree.

    `purposes` names, by run id, why a child was run, for a run that is not
    the first of its kind.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    result = measurement.new_result(
        {"run_id": case, "case": case}, artifact_kind="index"
    )
    result["publishable"] = publishable
    result["environment"]["start"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    checks = result["checks"]
    for child in children:
        if publishable:
            passed = child["status"] == measurement.STATUS_PASSED
            detail = f"{child['run_id']}: {child['status']}"
        else:
            failed = ci_failures(child)
            passed = not failed
            detail = f"{child['run_id']}: failed CI checks {failed}"
        checks.append(
            measurement.check(
                f"child_{child['run_id']}",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED if passed else measurement.STATUS_FAILED,
                detail,
            )
        )
    metrics = {
        "children_count": len(children),
        "children_passed_count": sum(
            1 for child in children if child["status"] == measurement.STATUS_PASSED
        ),
    }
    if legacy is not None:
        metrics.update(
            {
                "legacy_tests_count": legacy["tests_count"],
                "legacy_failures_count": legacy["failures_count"],
                "legacy_errors_count": legacy["errors_count"],
                "legacy_skips_count": legacy["skips_count"],
                "legacy_elapsed_s": legacy["elapsed_s"],
            }
        )
        result["legacy"] = legacy
        checks.append(
            measurement.check(
                "legacy_tests_passed",
                measurement.CHECK_HARD,
                measurement.STATUS_PASSED
                if legacy["successful"] and legacy["tests_count"] == LEGACY_TEST_COUNT
                else measurement.STATUS_FAILED,
                legacy["summary"],
            )
        )
    result["metrics"] = metrics
    result["mandatory_metrics"] = sorted(metrics)
    result["children"] = [
        {
            "run_id": child["run_id"],
            "status": child["status"],
            "topology": child["config"]["requested"].get("topology"),
            "failed_checks": sorted(
                entry["name"] for entry in child["checks"]
                if entry["status"] != measurement.STATUS_PASSED
            ),
            "baseline_decision": child.get("baseline_decision"),
            "metrics": child["metrics"],
            **({"purpose": purposes[child["run_id"]]}
               if child["run_id"] in (purposes or {}) else {}),
        }
        for child in children
    ]
    result["run_files"] = [
        measurement.file_entry(output_dir / f"{child['run_id']}.json")
        for child in children
    ]
    result["baseline_files"] = [
        measurement.file_entry(output_dir / entry["name"])
        for child in children
        for entry in child["baseline_files"]
    ]
    previous = (
        measurement.archive_published_index(f"{case}.json", output_dir, report_dir)
        if publishable else None
    )
    result["child_indexes"] = [previous] if previous else []
    result["environment"]["end"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"]
    )
    result["status"] = (
        measurement.STATUS_PASSED
        if all(entry["status"] == measurement.STATUS_PASSED for entry in checks)
        else measurement.STATUS_FAILED
    )
    result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
    index = measurement.write_result(output_dir / f"{case}.json", result)
    _ = measurement.publish_result_tree(index, report_dir)
    return result


def harness_contracts_case(output_dir, report_dir=None, **options) -> dict:
    """Run the harness contract tests and publish a verification index.

    This launches no engine, measures nothing and can never create a
    measured baseline. It exists so that the contracts every later task
    depends on -- the ledger, the oracle, the schema, the epoch rule, the
    baseline policy and the staging rules -- are checked and the check itself
    is committed evidence.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    result = measurement.new_result(
        {"run_id": "harness-contracts", "case": "harness-contracts"},
        artifact_kind="contract_checks",
    )
    result["environment"]["start"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    measurement.record_event(result, "contract_tests_started")
    try:
        from . import test_measurement
    except ImportError:
        import test_measurement
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(test_measurement)
    stream = open(output_dir / "harness-contracts.log", "w", encoding="ascii")
    try:
        runner = unittest.TextTestRunner(stream=stream, verbosity=2)
        outcome = runner.run(suite)
    finally:
        stream.close()
    measurement.record_event(result, "contract_tests_finished")
    result["environment"]["end"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    result["environment"]["match"] = measurement.environment_match(
        result["environment"]["start"], result["environment"]["end"]
    )
    result["metrics"] = {
        "contract_tests_count": outcome.testsRun,
        "contract_failures_count": len(outcome.failures),
        "contract_errors_count": len(outcome.errors),
        "contract_skips_count": len(outcome.skipped),
        "contract_expected_failures_count": len(outcome.expectedFailures),
    }
    result["mandatory_metrics"] = sorted(result["metrics"])
    result["checks"] = [
        measurement.check(
            "contract_tests_ran",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED
            if outcome.testsRun > 0
            else measurement.STATUS_FAILED,
            f"{outcome.testsRun} tests",
        ),
        measurement.check(
            "contract_tests_successful",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED
            if outcome.wasSuccessful()
            else measurement.STATUS_FAILED,
            _describe(outcome),
        ),
        measurement.check(
            "environment_matched",
            measurement.CHECK_HARD,
            measurement.STATUS_PASSED
            if result["environment"]["match"]["matched"]
            else measurement.STATUS_FAILED,
            json.dumps(result["environment"]["match"]["differences"], sort_keys=True),
        ),
    ]
    result["status"] = (
        measurement.STATUS_PASSED
        if all(
            entry["status"] == measurement.STATUS_PASSED for entry in result["checks"]
        )
        else measurement.STATUS_FAILED
    )
    result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
    result["artifacts"] = [
        dict(
            measurement.file_entry(output_dir / "harness-contracts.log"),
            kind="unittest_output",
            retention=str(output_dir),
        )
    ]
    # A verification index enumerates no measured evidence and claims no
    # baseline. Both lists stay empty by construction, not by omission.
    result["run_files"] = []
    result["baseline_files"] = []
    # An index may advance, but what it replaces may not vanish: the
    # previously published index becomes an immutable child of this one.
    previous = measurement.archive_published_index(
        "harness-contracts.json", output_dir, report_dir
    )
    result["child_indexes"] = [previous] if previous else []
    index = measurement.write_result(
        output_dir / "harness-contracts.json", result
    )
    _ = measurement.publish_result_tree(index, report_dir)
    return result


def _describe(outcome) -> str:
    """A one-line summary of a unittest result, failures named."""
    names = [str(test) for test, _ in outcome.failures + outcome.errors]
    return (
        f"run={outcome.testsRun} failures={len(outcome.failures)} "
        f"errors={len(outcome.errors)} skipped={len(outcome.skipped)} "
        f"{names[:5]}"
    )


def run_named(case: str, output_dir, **options) -> dict:
    """Construct a registered case's spec and run it."""
    cases = registered_cases()
    if case not in cases:
        raise SystemExit(
            f"unknown case {case}; registered cases are {sorted(cases)}"
        )
    return cases[case](output_dir, **options)


def run_case(spec: measurement.RunSpec, output_dir, *, experiment=None,
             report_dir=None, evaluate=True, lease_path=None,
             lease_wait_s=0.0, proc_root="/proc") -> dict:
    """Perform one experiment under the host controls and publish its result.

    The lifecycle is fixed and every step is mandatory:

    1. open the host controls -- the exclusive lease, then the build monitor;
    2. run `experiment(spec, result, output_dir, controls)`, which must call
       `controls.snapshot("start", ...)` before measured traffic and
       `controls.snapshot("end", ...)` after its final drain, and must add
       the correctness, sample-count and residual checks;
    3. close the controls, recording the lease, build, snapshot, environment,
       affinity and core-count checks;
    4. settle the status, so a run with any failed check is failed;
    5. apply the Controller baseline policy through `evaluate_baseline`,
       unless `evaluate` is false because the run is one child of a family
       whose policy is applied to the family after its stability checks;
    6. on a new fingerprint, write the candidate baseline atomically beside
       the result and reference it, by hash, in `baseline_files`.

    Whatever happened, the result is written and published from `finally`,
    so a run that failed at any step leaves its evidence. Calling this
    without an experiment is an explicit refusal.

    A busy lease aborts at once, before any traffic, unless `lease_wait_s`
    asks to wait for it. `proc_root` is where the monitor looks for builds;
    only a test points it anywhere but `/proc`.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    result = measurement.new_result(spec)
    result["config"]["requested"] = spec.as_json()
    result["run_dir"] = str(output_dir)
    result["workload_schedule"] = {
        "duration_s": spec.duration_s,
        "rate_requests_per_s": spec.overrides.get(
            "rate_requests_per_s", "closed_loop"
        ),
        "max_in_flight": spec.max_in_flight,
    }
    if report_dir is not None:
        result["report_dir"] = str(report_dir)
    controls = measurement.RunControls(
        result, lease_path=lease_path, lease_wait_s=lease_wait_s,
        proc_root=proc_root,
    )
    try:
        controls.open()
        if experiment is None:
            raise NotImplementedError(
                f"case {spec.case} was run without a measured body"
            )
        experiment(spec, result, output_dir, controls)
        controls.close()
        measurement.settle_status(result)
        if evaluate:
            decision = measurement.evaluate_baseline(result)
            if decision["action"] == "created":
                candidate = result.pop("baseline_candidate")
                path = measurement.write_published_json(
                    output_dir / decision["baseline_name"], candidate
                )
                result["baseline_files"].append(measurement.file_entry(path))
    except BaseException as error:
        result["status"] = measurement.STATUS_FAILED
        measurement.record_event(result, "failed", f"{type(error).__name__}: {error}")
        raise
    finally:
        controls.close()
        measurement.settle_status(result)
        # Both snapshots are required in every file. An edge the experiment
        # never reached is filled from the harness alone and marked as such;
        # it cannot satisfy the snapshot check, which already failed.
        for edge in measurement.RunControls.EDGES:
            if edge not in result["environment"]:
                snapshot = measurement.environment_snapshot({"harness": os.getpid()})
                snapshot["fallback"] = (
                    "the experiment did not reach this edge; this is the "
                    "harness process only"
                )
                result["environment"][edge] = snapshot
        result.pop("baseline_candidate", None)
        result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
        index = measurement.write_result(output_dir / f"{spec.run_id}.json", result)
        _ = measurement.publish_result_tree(index, result["report_dir"])
    return result


def parse_options(pairs) -> dict:
    """Case options given as `name=value`, decoded as JSON when they parse."""
    options = {}
    for pair in pairs or []:
        if "=" not in pair:
            raise SystemExit(f"case options are name=value, not {pair!r}")
        name, value = pair.split("=", 1)
        try:
            options[name] = json.loads(value)
        except ValueError:
            options[name] = value
    return options


def build_parser() -> argparse.ArgumentParser:
    """The command line every task's commands are written against."""
    parser = argparse.ArgumentParser(prog="measure", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="perform one named case")
    _ = run.add_argument("--case", required=True)
    _ = run.add_argument("--output-dir", required=True, type=Path)
    _ = run.add_argument("--option", action="append", default=[])
    stages = sub.add_parser(
        "stages", help="measure every registered stage and layer"
    )
    _ = stages.add_argument("--output-dir", required=True, type=Path)
    _ = stages.add_argument("--option", action="append", default=[])
    stage = sub.add_parser(
        "stage-results", help="git add one published evidence tree"
    )
    _ = stage.add_argument("--index", required=True, type=Path)
    attribution = sub.add_parser(
        "attribution",
        help="profile the real engine with perf and reconcile its CPU shares "
        "with the stage family",
    )
    _ = attribution.add_argument("--output-dir", required=True, type=Path)
    _ = attribution.add_argument("--option", action="append", default=[])
    memory = sub.add_parser(
        "memory",
        help="measure the real engine's memory in paired control and measured runs",
    )
    _ = memory.add_argument("--output-dir", type=Path)
    _ = memory.add_argument("--option", action="append", default=[])
    capacity = sub.add_parser(
        "capacity",
        help="search the maximum sustainable throughput and durable write speed",
    )
    _ = capacity.add_argument("--output-dir", required=True, type=Path)
    _ = capacity.add_argument("--option", action="append", default=[])
    soak = sub.add_parser(
        "soak",
        help="run the thirty-minute strict and buffered soaks and the PR-tier soaks",
    )
    _ = soak.add_argument("--output-dir", required=True, type=Path)
    _ = soak.add_argument("--option", action="append", default=[])
    rejudge = sub.add_parser(
        "rejudge-band",
        help="advance published indexes to the current RSS band rule, from stored runs",
    )
    _ = rejudge.add_argument("--index", action="append", required=True)
    _ = rejudge.add_argument("--output-dir", required=True, type=Path)
    rescrub = sub.add_parser(
        "rescrub",
        help="scrub one published evidence tree in place and re-hash it",
    )
    _ = rescrub.add_argument("--index", required=True, type=Path)
    failures = sub.add_parser(
        "failures",
        help="run one family's fault cases against real stores and record its index",
    )
    _ = failures.add_argument("--family", required=True, choices=["s3"])
    _ = failures.add_argument("--output-dir", required=True, type=Path)
    _ = failures.add_argument("--option", action="append", default=[])
    preflight = sub.add_parser(
        "fault-preflight",
        help="probe the disposable fault tools and record fault-preflight.json",
    )
    _ = preflight.add_argument("--output-dir", required=True, type=Path)
    _ = preflight.add_argument("--option", action="append", default=[])
    return parser


# The exit status of an attribution the host could not profile: neither a
# pass nor a measured failure, so a caller can tell the two apart.
ATTRIBUTION_SKIPPED_EXIT = 3

# The exit status of a fault preflight whose tools are optional and missing
# or failed a probe: a clean skip, neither a pass nor a failure.
FAULT_PREFLIGHT_SKIPPED_EXIT = 3


def fault_preflight(arguments) -> int:
    """`measure fault-preflight`: required when SERIES_REQUIRE_FAULT_TOOLS=1."""
    try:
        from . import faults
    except ImportError:
        import faults
    options = parse_options(arguments.option)
    keywords = {
        key: options[key] for key in ("stores", "lease_wait_s") if key in options
    }
    try:
        result = faults.preflight_fault_tools(
            faults.fault_tools_required(), output_dir=arguments.output_dir, **keywords
        )
    except unittest.SkipTest as skipped:
        sys.stderr.write(f"fault-preflight: skipped: {skipped}\n")
        return FAULT_PREFLIGHT_SKIPPED_EXIT
    except AssertionError as failure:
        sys.stderr.write(f"fault-preflight: failed: {failure}\n")
        return 1
    sys.stderr.write(
        f"fault-preflight: {result['status']} "
        f"{json.dumps(result['metrics'], sort_keys=True)}\n"
    )
    return 0 if result["status"] == measurement.STATUS_PASSED else 1


def main(argv=None) -> int:
    """Run one subcommand and report its exit status."""
    arguments = build_parser().parse_args(argv)
    if arguments.command in LONG_COMMANDS and os.environ.get(
        "SERIES_MEASURE_LONG"
    ) != "1":
        sys.stderr.write(
            f"{arguments.command} is a long measurement: set "
            "SERIES_MEASURE_LONG=1 to opt in\n"
        )
        return 2
    if arguments.command == "rejudge-band":
        for name in arguments.index:
            advanced = measurement.rejudge_band_index(name, arguments.output_dir)
            block = advanced["rss_band_rejudgement"]
            sys.stderr.write(
                f"{name}: {block['runs_applying_the_band_count']} runs on the band; "
                f"changes {json.dumps([(c['run_id'], c['recorded'], c['rejudged']) for c in block['verdict_changes']])}; "
                f"not re-judged {json.dumps([u['run_id'] for u in block['not_rejudged']])}\n")
        return 0
    if arguments.command == "rescrub":
        for name in measurement.rescrub_tree(arguments.index):
            sys.stderr.write(f"rescrubbed {name}\n")
        return 0
    if arguments.command == "fault-preflight":
        return fault_preflight(arguments)
    if arguments.command == "failures":
        try:
            from . import faults
        except ImportError:
            import faults
        options = parse_options(arguments.option)
        index = faults.run_failures(arguments.output_dir, options.pop("report_dir", None),
                                    family=arguments.family, **options)
        sys.stderr.write(f"{index['run_id']}: {index['status']} "
                         f"{json.dumps(index['metrics'], sort_keys=True)}\n")
        return 0 if index["status"] == measurement.STATUS_PASSED else 1
    if arguments.command == "stage-results":
        measurement.stage_run_files(arguments.index)
        return 0
    if arguments.command == "stages":
        try:
            from . import performance
        except ImportError:
            import performance
        options = parse_options(arguments.option)
        output_dir = arguments.output_dir or Path("/tmp/series-stages")
        result = performance.run_stages(
            performance.stages_spec(**options), output_dir, **options
        )
        sys.stderr.write(
            f"{result['run_id']}: {result['status']} "
            f"{json.dumps(result['metrics'], sort_keys=True)}\n"
        )
        return 0 if result["status"] == measurement.STATUS_PASSED else 1
    if arguments.command == "memory":
        try:
            from . import memory
        except ImportError:
            import memory
        options = parse_options(arguments.option)
        indexes = memory.run_memory(
            arguments.output_dir or Path("/tmp/series-memory"),
            options.pop("report_dir", None),
            **options,
        )
        for index in indexes:
            sys.stderr.write(
                f"{index['run_id']}: {index['status']} "
                f"{json.dumps(index['metrics'], sort_keys=True)}\n"
            )
        return 0 if all(
            index["status"] == measurement.STATUS_PASSED for index in indexes
        ) else 1
    if arguments.command == "capacity":
        try:
            from . import capacity
        except ImportError:
            import capacity
        options = parse_options(arguments.option)
        indexes = capacity.run_capacity(
            arguments.output_dir, options.pop("report_dir", None), **options
        )
        for index in indexes:
            sys.stderr.write(f"{index['run_id']}: {index['status']}\n")
        return 0 if all(
            index["status"] == measurement.STATUS_PASSED for index in indexes
        ) else 1
    if arguments.command == "soak":
        try:
            from . import soak
        except ImportError:
            import soak
        options = parse_options(arguments.option)
        indexes = soak.run_soak(arguments.output_dir, options.pop("report_dir", None), **options)
        for index in indexes:
            sys.stderr.write(f"{index['run_id']}: {index['status']}\n")
        return 0 if all(
            index["status"] == measurement.STATUS_PASSED for index in indexes
        ) else 1
    if arguments.command == "attribution":
        try:
            from . import performance
        except ImportError:
            import performance
        options = parse_options(arguments.option)
        result = performance.run_attribution(
            performance.attribution_spec(**options), arguments.output_dir, **options
        )
        sys.stderr.write(
            f"{result['run_id']}: {result['status']} "
            f"{result.get('acceptance', {}).get('mandatory')} "
            f"{json.dumps(result['metrics'], sort_keys=True)}\n"
        )
        if result["status"] == measurement.STATUS_SKIPPED:
            sys.stderr.write(f"{result['acceptance']['reason']}\n")
            return ATTRIBUTION_SKIPPED_EXIT
        return 0 if result["status"] == measurement.STATUS_PASSED else 1
    if arguments.command == "run":
        result = run_named(
            arguments.case,
            arguments.output_dir,
            **parse_options(arguments.option),
        )
        sys.stderr.write(
            f"{result['run_id']}: {result['status']} "
            f"{json.dumps(result['metrics'], sort_keys=True)}\n"
        )
        return 0 if result["status"] == measurement.STATUS_PASSED else 1
    raise AssertionError(f"unhandled command: {arguments.command}")


if __name__ == "__main__":
    sys.exit(main())
