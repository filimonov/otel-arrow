# Review report: 5588c3e0d..HEAD (branch `series-parquet-exporter`, 121 commits)

## Summary
The range adds a new `series-lake` crate (canonical series identity, extraction, sorted blocks, Parquet sink), a feature-gated `series_parquet` exporter with durable acknowledgements, engine-side memory accounting, a Python E2E and measurement harness, a CI workflow, and about 17k lines of design docs. Sixteen review seats ran in parallel; I verified the load-bearing claims against the source.

The core Rust design is sound. The durable-ack state machine, cache commit ordering, two-block bound and seal atomicity all held under the deep audit and the concurrency seat. The blockers are repository and rollout hygiene, not correctness. Below them sit a handful of real defects: a fingerprint tied to Arrow's Display output, internal errors surfaced as permanent client refusals, a retry layering that turns S3 outages into silent "cancelled" flushes, and a metrics hot path that defeats its own memo.

Verdict: **request changes**. Not upstreamable as-is; the code itself is close.

## Reviewed scope
- Diff: `5588c3e0d590d4df0303697a6862c8324235a310..d6454dbb8`
- Base: upstream `main` ancestor
- Files changed: 602 (534 are measurement JSON under `docs/superpowers/reports/`, 93.6% of added lines)
- Main moving parts:
  - `crates/series-lake`: 12 src modules, 6 test files, 2 benches, FORMAT.md
  - `crates/core-nodes/src/exporters/series_parquet`: mod, config, worker, flush, token, window, metrics, 3.9k-line tests, 1k-line README
  - `crates/engine/src/engine_metrics.rs` (+643/-40), `message.rs`, `crates/otap/src/pdata.rs`
  - `crates/validation/tests/series_parquet`: 16k lines Python
  - `.github/workflows/series-parquet-e2e.yml`, configs, chloggen, Cargo.lock

## Blockers

**1. 21 MB / 880k lines of host-specific measurement JSON committed into branch history, rewritten inside code commits**
Risk score: 88. Sources: seats 3, 12, 14.
Files: `docs/superpowers/reports/series-parquet-measurement/` (534 files); `measurement.py:52` (REPORT_DIR points into the repo), `measurement.py:4395-4420` (harness runs `git add`); commits b0a397f8d and 5c7c49e98 (+253k/-253k JSON lines beside ~800 code lines).
Evidence: 1,584 distinct report blobs across the range, 14.6 MB packed. 420 files embed `/home/mfilimonov/...`, the username, RAM size, CPU model and Docker builder names from an unrelated project, contradicting the harness's own "no absolute host paths" docstring at `measurement.py:59-61`. 35 files embed the MinIO test credentials.
Impact: Unreviewable PR, permanent clone bloat even after deletion at the tip, developer environment leaked into a public repo. The uncommitted working-tree deletions do not fix history.
Fix: Rewrite the branch so these blobs never enter history. Default REPORT_DIR to the already-ignored `.measurement-artifacts/`, drop the `stage-results` subcommand, publish evidence as CI artifacts or an orphan branch on the fork.

**2. New CI workflow serializes every otap-dataflow PR repo-wide and departs from repo conventions**
Risk score: 82. Sources: seats 3, 4, 7c, 11, 12, 14.
Files: `.github/workflows/series-parquet-e2e.yml:5-13, 20, 24-26, 39, 58-63, 84`.
Evidence: Trigger is `rust/otap-dataflow/**`. Concurrency group is the constant `series-parquet-host-measurement`, so GitHub keeps one running and one pending job for the whole repo and cancels older pending runs. Every other workflow keys on `${{ github.workflow }}-${{ pr number || ref }}`. The job does two cold df_engine builds with no rust-cache inside 60 minutes, and uses tag-pinned actions while the rest of the repo pins by SHA and runs OSSF Scorecard. Hosted runners are separate VMs, so the "one measurement per host" rationale does not apply.
Impact: Most PRs get this check cancelled or wait an hour behind unrelated PRs. It is also the only place S3, Alloy, ClickHouse/DuckDB, restart and outage paths run.
Fix: Narrow `paths` to the series crates and validation dir. Key the group per ref with cancel-in-progress. Reuse toolchain, cache and disk-free steps from rust-ci.yml. SHA-pin actions. Move the measurement smoke to workflow_dispatch or a label lane.

## Major issues

**3. `schema_fingerprint` hashes Arrow's `Display` text; FORMAT.md documents a different rendering**
Risk score: 72. Sources: seats 7a, 12.
Files: `crates/series-lake/src/schema.rs:230-238`; `docs/FORMAT.md:325-337`.
Evidence: arrow-schema 58.4 renders `Timestamp(µs, "UTC")` and `Map("entries": non-null Struct(...), unsorted)`; FORMAT.md says `Timestamp(Microsecond, Some("UTC"))`. Seat 7a recomputed the logs_values fingerprint in Python: the Display form matches the golden value, the documented form does not. Top-level nullability is not hashed.
Impact: The fingerprint is a persisted compaction-scope key. No third-party writer can reproduce it, and a routine arrow bump silently splits every lake's scope. Cheap to fix before first release, needs a migration rule after.
Fix: Render a crate-owned versioned type vocabulary; pin golden fingerprints for all four datasets plus one denormalized schema; have gen_golden.py compute them independently.

**4. Storage outage with default config surfaces as "cancelled" with zero retries and no cause**
Risk score: 72. Sources: seats 8, 2, 7b.
Files: `flush.rs:138-147, 156-185, 193-217`; `crates/otap/src/object_store.rs:106` (default retry_timeout 3 min); `config.rs:43` (flush_retry_deadline 60 s).
Evidence: Verified. When `retry` is omitted the object_store default retry_timeout is 180 s, longer than the 60 s block deadline. One attempt spins inside object_store until the deadline cancels it, `last` is None, and the flush is logged as `error="cancelled"` with `flush.retries += 0`. Failed attempts are never logged, only the final error. Nothing ties the two timeouts together at validation.
Impact: The most common production incident is invisible. `flush.cancelled` conflates shutdown and storage hangs. The shipped S3 YAML avoids it only because it sets retry_timeout 30 s.
Fix: Reject or warn on `retry.retry_timeout >= flush_retry_deadline` (including the default). Report deadline expiry as a distinct outcome carrying the last error. Log each failed attempt at WARN with the error.

**5. Internal invariant errors are reported to producers as permanent INVALID_ARGUMENT**
Risk score: 62. Sources: seats 1, 7a, 15.
Files: `worker.rs:417-421` (`map_err(Failure::Permanent)` on every extract error); `series-lake/src/error.rs:51-52`; `Error::invalid` used for "unsupported builder type", "column/builder mismatch", "row width does not match", "block already sealed", sort-column-missing, Arrow errors.
Impact: A writer bug becomes a permanent nack. OTLP producers drop permanently rejected data. The README states the opposite policy for the flush path ("producer holds the only copy"). Operators see `nacks{reason=invalid}` and blame clients.
Fix: Add `Error::Internal`; map only `Error::Refused` to `Failure::Permanent`. Also carry a bounded, sanitized detail in the nack reason instead of the bare token "invalid" (seat 1 finding 2, seat 6 finding 4).

**6. Metrics series memo is keyed by the point's own id, so it never hits**
Risk score: 65. Sources: seats 5, 7a.
Files: `extract/metrics.rs:57-72, 193-251`; `extract/mod.rs:832-885`; pdata `encode/mod.rs:653,670` assigns a fresh id per point.
Evidence: Verified. Every point deep-clones resource, scope and point attributes, renders them to JSON for sizing, canonical-encodes and hashes, and charges the budget before `seen` dedups it. Resource attributes are charged three times per descriptor. Extract is 1342 ns/record in the committed baselines, the largest metrics stage.
Impact: Steady-state CPU against the 1M records/s target, inflated `denorm_type_mismatch`, and a request with about 5k series under a typical k8s resource is permanently refused as `too_large` at the default 32 MiB regardless of what the cache already holds. Logs extraction has a similar allocation-heavy per-row path (seat 5 finding 4).
Fix: Key the memo on content (metric_id plus hash of sorted point attrs); check `seen` before building and charging a DescriptorRow; share resource/scope lists per request.

**7. Dictionary-encoded OTAP input is expanded before any ingress budget applies**
Risk score: 65 (medium confidence). Source: seat 4.
Files: `attrs.rs:39, 64, 156-168`; `worker.rs:345-352`.
Evidence: The pre-extract check uses logical Arrow bytes, which counts a dictionary's values once. `plain()` then casts whole columns to Utf8 and `AttrTable::from_batch` copies every value into owned Strings before `Budget::charge_row`. One 8 MiB dictionary value referenced by 256 rows passes 16 MiB and expands to gigabytes.
Impact: Memory exhaustion from one request on any pipeline where OTAP Arrow ingress reaches the exporter. OTLP input is unaffected.
Fix: Bound expanded size before casting, or read through the dictionary; charge str cells against `max_row_bytes` as they are read.

**8. Engine crate hard-codes one exporter's accounting, a process global and a metric set named `exporter.series_parquet`**
Risk score: 65. Sources: seats 2, 3, 8, 10, 12, 7b, 11.
Files: `engine_metrics.rs:73-172, 329-340`; `worker.rs:282, 866`.
Evidence: The residual is process RSS minus an accounted value the worker publishes only on CollectTelemetry, sampled on a different timer and thread. It includes every other node's memory. The metric set sits on the engine entity with no node attributes and collides by name with the node-scoped sets. Existing engine tests were rewritten and a retry added for a flaky RSS test. No chloggen covers the new engine/otap public API.
Impact: Dependency inversion in a generic crate; alerting on the residual fires on unrelated growth; the measurement plan uses this metric as acceptance evidence while it can be off by a block pair in either direction.
Fix: Postpone. Publish only `memory.accounted_bytes` per worker and compute the residual offline. If an in-process residual is wanted, propose a component-agnostic registry with a neutral metric name in its own engine PR, and update accounting at state transitions.

**9. Shutdown drains use `now_or_never` on a tokio mpsc send; most nacks are dropped and the "reserved slot" is never used**
Risk score: 50. Sources: seats 10, 7b, 13.
Files: `token.rs:340-380`; `worker.rs:1027-1035`; `mod.rs:346-352, 188-193`.
Evidence: Verified. `deliver_now` polls once; tokio's bounded send consults the coop budget (~128) and the dispatcher shares the thread, so a synchronous pass over up to 8192 tokens delivers roughly min(budget, free capacity of 512) and counts the rest as failures. `force_shutdown` never goes through `push`, so the reserved last slot documented at three sites protects nothing. With `max_requests_per_block: 1` the 2N-1 cap serializes the worker to one in-flight request.
Impact: On restart, producers wait out their own 180 s timeout instead of receiving a retryable NodeShutdown. No data loss.
Fix: Wrap drains in `tokio::task::coop::unconstrained` or a try_send path; queue force-drained refusals through the notifier until the deadline; fix or remove the reserved-slot comments.

**10. Ack-after-flush plus shipped receiver concurrency caps throughput far below target**
Risk score: 65 (medium confidence). Sources: seats 5, 8.
Files: `configs/series-parquet-s3.yaml:49` (max_concurrent_requests 128); `worker.rs:262`.
Evidence: Hold time is about half a 15 s window plus flush. 128 slots at ~8 s gives ~16 requests/s. Committed baselines show 5-10k records/s while stage CPU allows 250-380k/s per core. Nothing exposes `accept == false` as a metric.
Impact: The stated acceptance number (100k-1M records/s from dozens to hundreds of producers) cannot be reached with the shipped configs, and the bottleneck shows up as receiver RESOURCE_EXHAUSTED, not exporter metrics.
Fix: Document and gate on `in_flight = rate × (interval/2 + flush) / records_per_request`; size the configs from it; add an admission-closed gauge; run one launcher case with concurrency raised to separate producer from receiver limits.

**11. Backward wall-clock step stalls time-based rotation, and therefore all acks, for the length of the step**
Risk score: 45. Source: seat 15.
Files: `clock.rs:189-194`; `window.rs:71-80, 95-103`; `worker.rs:464-470`.
Evidence: `on_wake` returns TooEarly until wall time reaches `last_boundary + interval`; admission keeps joining the same ACTIVE block. Test `busy_rotation_rearms_boundary_sleep` shows the shape.
Impact: After an hour step back, nothing is acked for an hour unless a byte/request budget seals the block; producers resend into the same open block.
Fix: Rotate once one interval of monotonic time has passed, keeping the floored window start with reemit; bound TooEarly sleeps to one interval.

**12. `RequestTooLarge` permanence depends on cache and reemit state**
Risk score: 40. Sources: seats 15, 7b, 1.
Files: `buffer.rs:378-389`; `worker.rs:319-324`; `config.rs:407` (only `max_block_bytes >= max_extracted_bytes`).
Impact: Identical bytes can be refused permanently in a fresh partition or reemit block and accepted once the cache is warm, breaking the stated "identical bytes are refused again" rule. Reachable only when `max_block_bytes` is tuned near `max_extracted_bytes`, which the README's sizing section encourages. A related concern: `max_request_bytes` is measured per representation (protobuf vs Arrow), so a batch processor upstream can turn accepted OTLP into permanent refusals.
Fix: Decide permanence against the worst case (all descriptors new, reemit on), otherwise park/retry; tighten validation to include series-row inflation.

**13. `logs.series_attributes` and other identity-affecting config change series ids silently, with no format signal**
Risk score: 55. Sources: seats 12, 1.
Files: `extract/logs.rs:83, 97-100`; `sink.rs:152-175`; FORMAT.md:321-325.
Impact: Tuning the allow-list on a live lake re-keys every log stream and moves attributes between `series.attrs` and `values.attrs`; readers cannot detect the mix. Nothing enforces FORMAT.md's "needs a new base_uri" rule at startup.
Fix: Record an `identity_config` hash in Parquet metadata and the compaction scope; write a small marker object per base_uri and refuse incompatible fingerprints or identity config at startup.

**14. series-lake exposes primitives; the exporter reimplements the format invariants**
Risk score: 60. Source: seat 2.
Files: `worker.rs:472-520, 602-625, 726-730`; `buffer.rs:278-293, 342-425`.
Impact: Reemit-after-rotation, commit-after-flush and cache classification live in the exporter, so a second writer must copy the protocol exactly. Related: memory-budget multipliers for lake internals live in the exporter (`worker.rs:842-872`), retry classification reaches into `parquet::ParquetError::External` (`flush.rs:94-115`), and config is deserialized twice with suffix-based `*_bytes` rewriting (`config.rs:123-135`).
Fix: A small lake-level writer facade owning cache, sequencing and the reemit decision; `lake::Error::is_transient()`; byte-unit deserializers on LakeConfig.

**15. Docs contradict the code on the component's main behavior**
Risk score: 62. Sources: seats 6, 12, 3, 1.
Files: `configs/README.md:258` ("Only logs are accepted; metrics and traces are refused"); `series-lake/README.md:12-14` and `.chloggen/series-lake-core.yaml:24` ("does not exist yet" / "No pipeline node uses it yet"); `.chloggen/*:` placeholder issue 4128 with two marker styles; `core-nodes/Cargo.toml:97-104` puts `series_parquet` in `core-exporters` (default build) while changelog and README say `--features series_parquet`; exporter README:984-985 says benchmarks are "not part of this version".
Fix: One changelog entry, correct signal statement, decide default-vs-opt-in and make manifest and docs agree.

**16. Design spec links to an ignored `.superpowers/` path, so repo-lint's offline lychee check fails**
Risk score: 65 (medium confidence). Source: seat 14.
Files: `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md:1356`.
Fix: Inline or remove the link. More broadly (seats 3, 6, 14): the 15k lines of plans, the nine placeholder `measure.py` subcommands, and ~30 "spec section N" citations in code should not go upstream; keep FORMAT.md and the READMEs as the normative docs.

**17. Plain `cargo bench` fails on the two harness-driven benches**
Risk score: 52 (medium confidence). Source: seat 3.
Files: `benches/measurement.rs:130-175`, `benches/layered.rs:87-114`; `.github/workflows/rust-bench.yml:32-34` runs workspace `cargo bench`.
Fix: Exit 0 with a skip message when inputs are absent, or keep the benches out of the upstream PR.

## Minor issues / improvements
- Cancel during multipart creation leaves an orphaned upload and reports `abort_error: None` (`sink.rs:277-307`, object_store `buffered.rs:334-372`). Seats 10, 13. Let a started `put` finish bounded by `abort_timeout`, then abort.
- Retry-deadline and write completion in the same poll nacks a durable block (`flush.rs:190-218`); flush.rs:31-33 claim "never leaves a completed file" is false. Seats 15, 13. Poll the write before the deadline branch; count late successes.
- `VecDeque::with_capacity(2×max_requests_per_block)` and `LruCache::new(max_entries)` pre-allocate from unbounded config; a "disable the limit" value aborts at start (`token.rs:167`, `cache.rs:35`, `config.rs:166-171`). Seats 7b, 13. Cap or allocate lazily; use saturating math in budget.
- Rotate/resume in the flush-done arm is dead code and its comment describes an ordering enforced elsewhere (`mod.rs:282-298`, `worker.rs:641, 796`). Seats 7b, 10, 15. Remove and add `debug_assert!(cleaning.is_none())` in `complete()`.
- Every `object_store::Error` variant is retried until the deadline, including PermissionDenied and NotFound (`flush.rs:94-116`). Seat 7b.
- Per-request WARN on refusals is unthrottled and carries no producer context (`worker.rs:577-586`). Seats 1, 4, 8. Downgrade or rate-limit; include signal, size vs limit.
- `too_large` conflates four budgets; nesting depth counts as `invalid` (`worker.rs:114-126`). Seat 8.
- No start event with writer_id/boot_id/storage; `block_committed` lacks window/seq/path; shutdown outcome not summarized; retries counted only at completion and lost on abandon. Seat 8.
- `max_nesting_depth` has no upper bound and feeds ciborium's recursion limit (`config.rs:217-229`). Seat 4. Cap at 128-256.
- Pre-existing CBOR encoder in pdata recurses without a depth limit, reachable through `try_into_with_default` before any lake limit (`pdata/src/encode/cbor.rs:45-96`). Seat 4, follow-up upstream.
- Denormalized column names may collide with hive keys `v`, `signal`, `dataset`, `date`, `hour` (`config.rs:432-443`). Seat 12. Reserve them.
- `metrics.series_attributes` is accepted and ignored (`config.rs:183-194`). Seat 1.
- `render_v1` float text is serde_json's formatter with no golden vectors (`value.rs:156-166`). Seat 12.
- Merge keys for the whole block are resident but never charged, ~20-30% of block bytes at peak (`sort.rs:225-237`). Seat 7a.
- Values builders keep 1024-row capacity per request, ~110 KB accounted per small request (`extract/mod.rs:355-380`). Seat 5.
- Series rows form one sorted run per request; churn yields up to 4096 runs for the merge (`buffer.rs:165-199`). Seat 5.
- Empty ACTIVE block still waits for the flush slot at a window boundary (`worker.rs:640-651`). Seat 15.
- Sink abort uses `tokio::time::timeout` while everything else uses the engine clock; not simulable (`sink.rs:188`). Seats 2, 10.
- `writer_id` only rejects `/`; `-` breaks the documented file-name pattern (`config.rs:385-390`). Seat 7a.
- Validation errors lump many keys into one message; section-less serde errors (`config.rs:166-171, 217-231`). Seat 1.
- Default per-worker budget ~1.5 GiB multiplies by the engine's AllCores default with no warning. Seats 1, 8.
- Default flush/abort budgets (145 s bound) do not fit the 60 s signal grace the README itself documents. Seat 1.
- Python harness: `run_child` swallows KeyboardInterrupt (`performance.py:2471-2499`); DockerStore leaks containers on failed start and has no docker timeouts (`test_e2e.py:1695-1813`); `test_disconnect_does_not_remove_data` sends at an arbitrary window offset (~1-3% flake); `free_port()` check-then-use with no retry; build fingerprint inferred from path and env, not the binary; three telemetry parsers, one of which reads absence as zero. Seat 7c.
- CI launcher smoke keeps timing gates (100 ms monitor coverage) hard in `publish=false` mode; likely flaky on 4-vCPU runners. Seat 7c.
- Cargo.lock re-resolves nine unrelated packages to windows-sys 0.61.2. Seats 3, 12.
- Static test credentials in `configs/series-parquet-s3.yaml:62-63`; use env substitution. Seat 14.
- `gen_golden.py` lacks the SPDX header; golden JSON lacks a trailing newline. Seat 14.

## Needs verification
- Whether OTAP Arrow ingress from untrusted senders reaches this exporter in any shipped topology (decides severity of finding 7).
- Whether the OTLP receiver decodes with a prost recursion limit before handing raw bytes on (decides the stack-overflow half of the CBOR encoder concern).
- Whether object_store's `LocalFileSystem` fsyncs; if not, "ack means durable" for `storage: file` holds only against process crash and the README should say so.
- Whether the OTLP receiver can outlive the exporter during pipeline teardown; if so, undecided tokens on worker drop become client hangs rather than connection errors.
- Whether `try_into_with_default` of a truncated OTLP body yields empty records without error in `parquet_exporter` and `otap_exporter` (the framing check exists only in this exporter).
- Recorded CI run duration and disk use for the two cold builds on ubuntu-latest, and the launcher-ci flake rate in `publish=false` mode.
- Whether `components-baseline.json` is generated from default features (affects finding 15 if the exporter becomes opt-in).
- How DuckDB, ClickHouse and Spark resolve a file column named like a hive partition key.
- The descriptor cache is per worker; with one pipeline per core each descriptor may be written once per core per partition. Whether spec rev 7 intends this.

## Suggested commit / diff split
Squash the ~40 fix-on-fix commits into their feature commits first, then:
1. **PR 1, engine/otap prep (small):** `ExporterInbox::shutdown_deadline`, `Context::take_authorized_identity`, `Context::retained_frame_bytes`, with tests and an `enhancement` chloggen. No engine accounting.
2. **PR 2, series-lake crate:** src, golden vectors plus `gen_golden.py`, fuzz/oracle tests, FORMAT.md, README. Lockfile adds only new packages. Fix finding 3 before this lands.
3. **PR 3, series_parquet exporter:** node code, tests.rs, configs, trimmed README (~500 lines), one chloggen entry, components-baseline. Decide default vs opt-in.
4. **PR 4, functional E2E:** `test_e2e.py`, requirements, a narrowly path-filtered workflow with one debug build and no measurement steps.
- Separate follow-ups or out of tree: measurement harness and benches, engine RSS residual as a generic facility, design spec (condensed) if upstream wants it, all measurement results, windows-sys lock bump, flaky-RSS test retry.

## Tests to add or strengthen
- Factory `create` with an S3 config requiring a token provider and no capability (expect InvalidUserConfig), and with the capability present.
- A flush whose single put outlives `flush_retry_deadline` under the object_store default retry; assert a retryable nack at the deadline with the underlying error surfaced.
- `tests.rs:657` and `:1018` "not acked early" assertions: assert `worker.notify` is empty after `rotate()`; today they cannot fail.
- Drain more than 128 tokens into a completion channel with room for all; assert all are delivered.
- Same request admitted once cold and once warm; assert identical outcome.
- Backward wall-clock step larger than the interval; assert rotation within one monotonic interval.
- Metrics request with ~8k points under a 25-attribute resource at default limits; assert acceptance and memo hit rate.
- Dictionary batch with one large value referenced by many rows; assert refusal before expansion.
- `AckToken::split` strips headers and claims (deleting `token.rs:49-50` fails no test today).
- Config-reject table: assert `base` is valid first, check error substrings, run through `validate_config`.
- Exact-boundary admission at `1_000_000_000` ns; CBOR depth at limit-1 and with nested maps.
- Golden fingerprints for all four datasets plus one denormalized schema; CI step regenerating `gen_golden.py` output and diffing.
- Run `measurement --self-test` somewhere in CI or move its pure checks into `#[cfg(test)]`.
- Engine residual: `finish_reporting_until` with a registered worker; deferred-send retry path; a worker wired to an isolated accounting.

## Coverage summary
- Entry points reviewed: pdata (normal and force-drained), window wake, flush result, cleanup join, requested rotation, notifier delivery, Shutdown with deadline (future, past, synthetic), CollectTelemetry, inbox error, deadline branch and abandon phases 0-2.
- Transitions reviewed: prepare/offer/park/resume, reserve/admit and admit failure, rotate (empty, seal failure, sealed to FlushJob), complete (Ok, Err, RecvError) into cleaning, abandon.
- Fault categories checked: store failure per phase, retry deadline mid-upload, non-retryable encode error, task panic, full or closed completion channel, closed inbox, timer firing while busy, missed windows, clock steps both ways, deadline in the past, parked request, FLUSHING block, empty ACTIVE at shutdown, oversize request, N = 1, full cache, coop budget exhaustion.
- Invariants that held: cache marked only on success against the block's own partition, in block order; each admitted token reaches exactly one push or one counted failure; ≤2 blocks + ≤1 parked; parked-first ordering via `accept()`; seal is all-or-nothing; notifier assert cannot fire; flush-task panics are contained.
- Deferred / not covered: sort/merge internals beyond allocation review, metrics.rs schema tests, engine controller routing of completions to the OTLP receiver, running any build, test, bench or the harness (static review only).
- Main assumptions: single-threaded local runtime per pipeline (verified in runtime_pipeline.rs); tokio 1.53.1 and object_store 0.13.2 as locked.

## Final verdict
Status: **request changes**

Minimum required actions before an upstream PR:
- Rewrite history to drop `docs/superpowers/reports/` and point the harness at an ignored directory.
- Fix the workflow: narrow paths, per-ref concurrency, SHA-pinned actions, cache, single build, no measurement smoke on hosted runners.
- Replace the Display-based `schema_fingerprint` with a format-owned type vocabulary and pin goldens for every dataset.
- Introduce `Error::Internal` and stop mapping non-content errors to permanent nacks.
- Tie `retry.retry_timeout` (including its default) to `flush_retry_deadline`, report deadline expiry distinctly, and log failed attempts.
- Fix the metrics memo key and share resource/scope lists per request.
- Move engine `SeriesAccounting` out of the upstream PR or generalize it.
- Fix the docs/manifest contradictions (metrics accepted, opt-in vs default, one changelog entry, real issue number) and the dangling `.superpowers` link.

Full per-seat reports are in the scratchpad under `review/out-*.md` if you want the unmerged detail for any seat.