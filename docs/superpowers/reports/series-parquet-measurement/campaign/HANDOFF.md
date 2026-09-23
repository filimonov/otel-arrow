# Handoff -- series_parquet measurement campaign (written 2026-09-22 21:5x)

Read this first, then `progress.md` in this directory (the ledger, append-only), then the plan.

## Where the work is

- Repo <repo>, branch `series-parquet-exporter`. Fork remote `filimonov` has the branch up to 69d5b8eea. No PRs, ever, unless the user asks.
- Spec (authority): docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md, revision 7.
- Plan 2 (exporter): docs/superpowers/plans/2026-09-21-series-parquet-exporter-node.md -- COMPLETE, all tasks reviewed.
- Plan 3 (measurement and failure models): docs/superpowers/plans/2026-09-22-series-parquet-measurement.md, 14 tasks.
- Ledgers: .superpowers/sdd/2026-09-21-series-parquet-exporter-node/progress.md (plan 2), .superpowers/sdd/2026-09-22-series-parquet-measurement/progress.md (plan 3). Task briefs and reports live beside them.

## Plan 3 status

- Task 1 (harness, baseline policy, host controls): COMPLETE. 52e964fc1, 24eceba60, fcb71d492, 57467a583, 114605eb3.
- Task 2 (launchers, enforced controls, fast CI): COMPLETE. 8a41e36f3, 93d3a1ccf, 69d5b8eea.
- Task 3 (layered Criterion plus stage benches): IN FIX ROUND 1. 5c7c49e98, df08c9e04. At handoff time the implementer (agent impl3-task-3, stopped when the session ends) was re-measuring the full stage matrix after four review fixes. The working tree has deleted/regenerated baseline JSON under docs/superpowers/reports/series-parquet-measurement/ -- that is the re-measurement in progress, not damage. If the run did not finish, re-dispatch Task 3 fix round 1 from the task-3 brief plus the review at scratchpad/codex-review-p3t3.out; the four items are: setup inside timers, one second of measured work per timing process, explicit input representation and denominator per stage result, and documented completion semantics for uploads.
- Tasks 4-14: NOT STARTED. Briefs exist for the early ones; generate the rest with the sdd scripts.

## Process that must continue

- Subagent-driven: one implementer per task, fresh agent, opus for judgment-heavy tasks. Review every task, then fix rounds until clean. Stop the agent when its task closes.
- At every task close: add or update its section in docs/superpowers/reports/series-parquet-measurement/FINDINGS.md, then refresh the committed snapshot in docs/superpowers/reports/series-parquet-measurement/campaign/ (ledger.md, reports/, briefs/, HANDOFF.md) and commit both.
- Reviews run through codex, not Claude subagents, to save tokens: `codex exec -m gpt-5.6-sol --sandbox read-only -C <repo> - < prompt.txt > out.txt` in the background, prompt on stdin.
- Re-verify every review finding against current HEAD before dispatching a fix round. Reviews go stale fast here.
- Commit trailers: `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` and a `Claude-Session:` line with the NEW session's URL.
- The 20-minute cron status check is session-only and dies with the session. Recreate it: every 20 minutes, list agents and processes, treat 25 minutes of no progress as hung, ledger "cron check OK <time>", never ask the user.

## Rulings in force (do not relitigate)

- Baseline policy: the first valid run writes a fingerprinted baseline; later matching runs fail beyond 25 percent. Correctness, measurement validity and unexplained residual are hard every run.
- Release binaries only for measured cases; a debug profile is refused. The profile is fingerprint material.
- Fingerprint rejects null at any depth in harness-supplied inputs. The effective engine configuration is hashed verbatim, and its explicit nulls (durable buffer max_age) are settings.
- rss_reconciliation stays hard with its tolerance unchanged. Strict runs sit at about 90-95 percent of it on release, so it will trip. Task 6's residual decomposition is pulled forward, BEFORE Task 5: split RSS minus jemalloc resident (non-heap) from jemalloc resident minus tracked heap (allocator retention plus untracked heap). jemalloc is the default allocator and memory_limiter source jemalloc_resident already publishes process_memory_usage_bytes, so no engine change is needed.
- A failed measured run is preserved and investigated. Never rerun until green, never loosen a rule.
- CI required gates = REQUIRED_HARD_CHECKS minus physical_cores_sufficient.
- Headline acceptance number: 100k-1M records/s from dozens to hundreds of producers. Task 5 must report cores needed for 1M/s and run the fan-in dimension at 1, 8, 64 and 256 producers.

## Open findings routed to Task 12

- Durable buffer replays acknowledged, drained bundles after a GRACEFUL admin shutdown and restart (5000 records stored twice). Reproduced on the full host with a release build. Suspect progress persistence is not flushed on shutdown.
- Stability refusals with no baseline: sink timing peak RSS, encode timing peak RSS, otlp_noop pipeline CPU measured over a 0.1 s window (the window length may itself be the defect).
- Upstream (not ours, no PR opened): exporter:parquet `should_flush` else-if makes flush_when_older_than dead when target_rows_per_file is set.

## Environment facts that cost time to rediscover

- The Python harness imports duckdb. Run it as `cd rust/otap-dataflow && /tmp/series-parquet-venv/bin/python3 -m unittest crates.validation.tests.series_parquet.test_measurement`. System python3 fails to import.
- Measurements are serialized by a flock lease at /tmp/series-parquet-host-measurement.lock. Never build while a measured run is in flight.
- Never wait with `pgrep -f <pattern>` loops: they match their own command line and hang forever. Wait on a PID or an output-file marker.
- E2E needs SERIES_REQUIRE_DOCKER=1; MinIO, RustFS, ClickHouse and Alloy images are already local.
- The GitHub workflow triggers only on pull_request and push to main, so it never runs on this branch. Verify CI by running its commands locally.

## Raw measurement artifacts (added 2026-09-22 22:06)

Raw stage artifacts lived only in /tmp/series-stages (11 GB, of which 10 GB is reproducible Parquet output from the Criterion parquet stages). Ruling: the committed run JSON is the evidence; bulk Parquet output is NOT retained, since its sizes and hashes are already recorded there. Everything else (DHAT profiles, Criterion sample files, logs, manifests, inputs) is archived durably at `.measurement-artifacts/stages-raw-20260922-2206.tgz` (110 MB, git-ignored). Reconstitute with `tar -C /tmp -xzf .measurement-artifacts/stages-raw-*.tgz` before verifying a hash reference. The Task 14 "inaccessible raw artifacts" failure applies to this archive, not to the discarded Parquet bytes; state the exclusion in the report.
