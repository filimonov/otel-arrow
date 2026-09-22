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
import json
import os
from pathlib import Path
import sys
import time
import unittest

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement


# Subcommands whose measurement is long enough that it must be asked for.
LONG_COMMANDS = ("capacity", "memory", "soak", "failures", "buffered")

# Subcommands later tasks own. They are named here so that the command line
# is one contract rather than a set that grows behind the plan.
PLANNED_COMMANDS = (
    "stages",
    "attribution",
    "capacity",
    "memory",
    "soak",
    "fault-preflight",
    "failures",
    "buffered",
    "remediate",
    "report",
)


def registered_cases() -> dict:
    """Every named case this command line can run today."""
    return {
        "harness-contracts": harness_contracts_case,
        "harness-local": harness_local_case,
    }


def harness_local_spec(**options) -> measurement.RunSpec:
    """The smallest publishable real-engine slice.

    One hundred requests of mixed supported signals through one worker on one
    explicitly chosen core, with one-second windows and exact no-retry
    multiplicity. Task 2 adds the topology, launcher and enforced host
    controls this case needs before its numbers may be published.
    """
    cores = tuple(options.get("cores", (0,)))
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
            "harness-local", "strict", "local", cores, interval_s, ordinal
        ),
        case="harness-local",
        topology="strict",
        store="local",
        cores=cores,
        workload=workload,
        interval_s=interval_s,
        duration_s=int(options.get("duration_s", 30)),
        overrides=dict(options.get("overrides", {})),
    )


def harness_local_case(output_dir, **options):
    """Refuse to publish a measurement before its host controls exist."""
    spec = harness_local_spec(**options)
    raise NotImplementedError(
        f"case {spec.case} is registered and its spec validates, but its "
        "measured run needs the topology, launcher and enforced host controls "
        "of task 2; run it from there"
    )


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
             report_dir=None) -> dict:
    """Perform one experiment and always write and publish its result.

    `experiment(spec, result, output_dir)` is the measured body. Task 1 owns
    the contract around it and validates that contract; the body itself
    arrives with the topology, launcher and enforced host controls of task 2,
    which is why calling this without one is an explicit refusal rather than
    an empty passed result.

    Whatever the body did, the result is written in `finally` and published,
    including a failure: a run that ended badly leaves evidence rather than
    nothing, and the caller's exit status reports it.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    result = measurement.new_result(spec)
    result["config"]["requested"] = spec.as_json()
    if report_dir is not None:
        result["report_dir"] = str(report_dir)

    result["environment"]["start"] = measurement.environment_snapshot(
        {"harness": os.getpid()}
    )
    try:
        if experiment is None:
            raise NotImplementedError(
                f"case {spec.case} has no measured body yet: the topology, "
                "launcher and enforced host controls arrive in task 2"
            )
        experiment(spec, result, output_dir)
    except BaseException as error:
        result["status"] = measurement.STATUS_FAILED
        measurement.record_event(result, "failed", f"{type(error).__name__}: {error}")
        raise
    finally:
        result["elapsed_s"] = (time.monotonic_ns() - started) / 1e9
        result["environment"]["end"] = measurement.environment_snapshot(
            {"harness": os.getpid()}
        )
        result["environment"]["match"] = measurement.environment_match(
            result["environment"]["start"], result["environment"]["end"]
        )
        index = measurement.write_result(
            output_dir / f"{spec.run_id}.json", result
        )
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
    stage = sub.add_parser(
        "stage-results", help="git add one published evidence tree"
    )
    _ = stage.add_argument("--index", required=True, type=Path)
    for name in PLANNED_COMMANDS:
        planned = sub.add_parser(
            name, help=f"{name}: implemented by a later task of this plan"
        )
        _ = planned.add_argument("--output-dir", type=Path)
        _ = planned.add_argument("--finding", type=Path)
        _ = planned.add_argument("--option", action="append", default=[])
    return parser


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
    if arguments.command == "stage-results":
        measurement.stage_run_files(arguments.index)
        return 0
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
    sys.stderr.write(
        f"{arguments.command} is implemented by a later task of this plan\n"
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
