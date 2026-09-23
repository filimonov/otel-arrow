# Campaign working record (snapshot)

This directory is a committed copy of the plan-3 campaign's working files,
which otherwise live in the git-ignored `.superpowers/sdd/2026-09-22-series-parquet-measurement/`.

- `ledger.md`: the controller ledger, with every dispatch, ruling and its rationale, in time order.
- `reports/`: the implementer report of each task: method, conditions, numbers, findings, concerns.
- `briefs/`: the task text each implementer received.
- `HANDOFF.md`: the resume pointer for a new controller session.

The copy is refreshed when a task closes. The readable summary of the
measurements is `../FINDINGS.md`. The machine-readable evidence is the JSON
beside it. These files are working notes, not documentation: they are
trimmed or removed when the branch history is rewritten before the upstream PR.
