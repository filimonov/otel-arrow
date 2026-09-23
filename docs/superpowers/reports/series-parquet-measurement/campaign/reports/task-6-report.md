# Task 6 report: retained memory, transients and the RSS residual

Worktree: `<repo>/.claude/worktrees/agent-a3dc6950c02333ef6`, branch
`worktree-agent-a3dc6950c02333ef6`. The worktree was created from `main`, so I reset it to `936ace03a` first.
This report sits in the session scratchpad. Worktree isolation refused writes to the main-checkout path
`.superpowers/sdd/2026-09-22-series-parquet-measurement/task-6-report.md`; copy it there.

## Commits (on 936ace03a)

| Commit | What |
| --- | --- |
| 73b8aeb59 | fix: charge the merge keys a flushing table holds resident. Adds `MergeIter::resident_key_bytes` and `Sink::merge_key_bytes` / `merge_key_high_water_bytes`, and adds the keys to `memory.accounted`. Tests in sort.rs and sink.rs. |
| bad399722 | bench: merge-key and values-capacity fixture probes, `measurement --series-cost` (`benches/measurement/series_cost.rs`), and the self-test `memory_terms_are_reported`. |
| 23e43ae52 | harness: `memory.py`, `measure memory`, `CASE_ROLES` `memory` / `memory_store`, a `policies` merge in `engine_config`, README, and contract tests. |
| f64cd11af | engine (user decision): `malloc_conf = background_thread:true` on Linux jemalloc builds, `memory_limiter::jemalloc_background_thread()`, a startup line, a test, a chloggen entry and an exporter README note. |
| b9c083b43 | harness: the allocator fingerprint now follows the engine's own startup report (`jemalloc+background_thread`). |
| 4352f1ef9 | docs: the background_thread effect stated as measured. |
| d32e543b3 | evidence: 9 memory families, 39 files, one baseline. |

## Validation

- Contract tests: 200 OK.
- series-lake lib: 150 passed. core-nodes `series_parquet`: 91 passed.
- df_engine bin: 15 passed, including `jemalloc_starts_with_its_background_thread`.
- engine `memory_limiter`: 19 passed.
- Bench `--self-test`: exit 0.
- `cargo fmt`: clean. clippy `-D warnings`: clean for series-lake (all targets, bench-harness and bench-heap), core-nodes, engine and df_engine.
- `cargo xtask check`: "All tests passed". The first attempt failed only because the `proto/opentelemetry-proto` submodule is not initialized in a fresh worktree (query-engine-playground `include_str!`). I ran `git submodule update --init` for it in the worktree; no tracked file changed.
- The startup line was checked on a live engine. It printed `INFO memory allocator jemalloc, background_thread on`, and `MALLOC_CONF=background_thread:false` turned it off.
- Every measured run ran under `taskset -c 0-7,16-23` with the host lease held, on release binaries. Every build and test ran under the lease too.

## Method

A pair is two fresh release engines on the same cores under the same offered workload, each warmed separately:

- a control, whose exporter is the noop exporter;
- the measured strict or buffered engine.

Workloads:

- `harness-rate`: 1,275 prebuilt harness-local requests (100 records of 1 KiB, every fifth request metrics) at 50 requests/s.
- `logs-high-rate`: the Task 4 shape. 9,025 logs requests, 256 in flight, 128-request blocks, pinned MinIO, about 108k records/s.

Phases:

1. warmup (25 requests), then drain;
2. settle for 11 s;
3. idle, at least 30 samples and 30 fresh epochs;
4. load;
5. drain;
6. decay for 11 s;
7. retained, at least 30 samples and 30 fresh epochs.

A 100 ms sampler records, on one timeline:

- telemetry, at a 100 ms reporting interval;
- `/proc/PID/smaps`, classified every tenth sample, with `smaps_rollup` interpolated in between;
- jemalloc's own totals (allocated, active, metadata, resident, mapped, retained).

The jemalloc totals come from `MALLOC_CONF=stats_interval:4MiB,stats_interval_opts:Jgmdablxeh`, which prints them to the engine log after every 4 MiB of allocation. That makes them a synchronous observation of flush transients: 160 to 6,600 prints fall in the flush intervals.

The engine's `process.memory.usage.bytes` (limiter source `jemalloc_resident`, observe-only) is recorded, but it only refreshes on the controller's fixed 5 s timer.

**RSS split.** Measured, and the parts add up:

- non-heap: binary and library pages, stacks, kernel;
- allocator retention: resident allocator extents minus jemalloc allocated;
- jemalloc allocated: the live heap of all threads.

The tracked heap is the sum of the pipeline threads' `memory.usage` counters.

**Ledger gate.** `residual(rss, accounted, runtime, buffer, workspace, allocator)`, where:

- runtime = non-heap;
- allocator = retention;
- workspace = the control's live heap, plus a measured flush workspace in flush intervals only. The flush workspace is 36.9 MB: Task 3 `stages-sink-async-logs-1k-stable-zstd-heap-f001` `peak_workspace_bytes`, with its hash recorded;
- buffer = 0; it is not measured separately, so it stays inside the residual.

The gate applies the frozen `measurement.residual_check` unchanged.

The harness's own reconciliation (`rss_residuals`) is recorded beside it at two cadences, 100 ms and as a 1 s gauge would see it, and split into shares at its peak.

**Family rules.** At least 3 valid pairs. A family fails if any primary metric has `(max - min) / median > 0.15`, if a paired signed difference changes sign outside its uncertainty, or if any hard gate fails.

Pairs invalidated by a compiler were rerun under a new ordinal, listed in `invalidated_and_rerun`, and not aggregated. There were 7 such pairs over the task, always another agent's cargo or rustc running without the lease.

## Memory model

The ledger was transcribed and checked. The reported `memory.budget` of 1,637,851,136 B decomposes exactly into two terms, with token size T = 200 B:

- retained: `2B + E + 128C + 2NT`;
- workspace: `2R + 2M + 3W + P(U+1) + M + 4I + 64MiB`.

A contract test pins this.

Medians over 3 pairs, [min, max]. Strict is memory-strict f001, with background_thread off. High-rate is memory-strict-logs-high-rate f001.

| Term | strict, load | high-rate, load |
| --- | --- | --- |
| RSS peak | 100.3 MB [97.6, 103.6] | 146.1 MB [136.6, 147.2] |
| non-heap | 48.4 MB [48.3, 48.4] | 49.8 MB [49.0, 50.0] |
| of which file-backed | 43.2 MB | 44.8 MB |
| of which thread stacks | 0.4 MB | 0.4 MB |
| allocator retention | 30.4 MB [29.9, 32.0] | 41.3 MB [40.9, 45.6] |
| jemalloc allocated, peak | 25.6 MB [25.1, 26.6] | 58.3 MB [57.8, 58.8] |
| exporter memory.accounted, peak | 11.5 MB [10.8, 11.7] | 29.5 MB |
| control live heap | 6.1 MB | 5.8 MB |
| unexplained live heap, load median | 2.0 MB [1.7, 2.1] | 14.1 MB [9.8, 14.5] |
| ledger residual max, flush workspace applied | 6.6 MB [4.5, 8.6] | 4.7 MB [4.7, 8.6] |
| paired RSS delta (measured minus control peak) | 36.4 MB [33.9, 43.0] | 69.8 MB [62.9, 69.8] |

Reading the table:

- In the high-rate shape the 14 MB of live heap beyond accounted is 256 requests in flight in the receiver (converted, not yet admitted) plus the flush that is always running there.
- The ledger gate passes in every family.
- After drain and decay the live heap returns to 6.5 MB, the same as idle.

**Buffered** (memory-buffered f001, harness-rate): RSS peak 104.7 MB [104.6, 106.2], allocated peak 28.2 MB, accounted 11.7 MB, unexplained 3.2 MB. Against strict, the buffer adds about 1.2 MB of live heap and about 2.4 MB of file-backed WAL mapping.

**Transients.** The committed Task 3 DHAT stage workspaces are listed below. All are below the 528 MB workspace reservation. In-process, flush intervals add 3 to 5 MB of retention. The synchronous allocator prints show a flush peak equal to the load maximum: 24.9 MB in strict, 58.8 MB in high-rate.

| Stage | logs-1k | metrics-mixed | logs-512k | logs-8k-churn-wide |
| --- | --- | --- | --- | --- |
| convert | 42.1 MB | 29.6 MB | | |
| extract | 52.1 MB | 23.1 MB | 56.5 MB | 78.6 MB |
| sort_seal | 27.2 MB | 4.8 MB | 58.8 MB | 54.6 MB |
| merge | 17.9 MB | 4.7 MB | 16.3 MB | 67.8 MB |
| encode zstd | 36.2 MB | 4.3 MB | 95.6 MB | 109.7 MB |
| sink | 36.9 MB | 6.9 MB | 111.8 MB | 160.6 MB |

## Hypothesis 1: merge keys resident and never charged -- CONFIRMED

| Fixture | block | keys, peak table | ratio |
| --- | --- | --- | --- |
| logs-1k-stable | 23.2 MB | 0.68 MB | 0.029 |
| logs-512k-near-limit | 50.3 MB | 0.004 MB | 0.0001 |
| metrics-mixed | 2.9 MB | 0.51 MB | 0.173 |
| metrics 6 / 10 points | 0.12 / 0.15 MB | 0.010 / 0.014 MB | 0.086 / 0.094 |
| logs-8k-churn-wide, body sort key | 50.0 MB | 51.0 MB | 1.020 |

The static estimate of 20 to 30 percent holds for narrow metrics rows (17 to 18 percent). For logs with the default keys it is about 3 percent. With a wide custom key the keys equal the whole block.

Fix 73b8aeb59: the keys are now charged in `memory.accounted` while their table is written, and the charge is zero between tables.

Task 12: the admission reservation has no term that covers keys of about one block, since they are not bounded by M. Either reserve for them or validate wide keys at startup.

## Hypothesis 2: values builders keep 1024-row capacity -- CONFIRMED, as over-charging

The capacity a request's values batch pins, beyond what its rows use:

| Request | pinned beyond rows | multiple of rows |
| --- | --- | --- |
| logs, 1 record | 117,156 B | 102x |
| logs, 10 records | 122,156 B | 11.6x |
| logs-1k | 134,232 B | 2.2x |
| logs-8k-churn-wide | 225,592 B | 1.5x |
| metrics, 6 points | 134,276 B | 124x |
| metrics-mixed | 122,232 B | 7.7x |

The block is charged that capacity until the building run seals, and the seal then produces a compact run. So the capacity is charged and real, but short-lived:

- It is not a residual.
- It makes blocks of small requests rotate early. 4,096 one-record requests would be charged about 480 MB for about 5 MB of rows.

The bounded fix, `shrink_to_fit` in `RowSink::seal`, works, but I reverted it. It breaks `extract::logs::budget_is_enforced_on_the_measured_output` (observed 1318 > limit 1265): once the measurement falls below the row estimate, the in-progress estimate decides the extraction budget. The fix and the test's premise have to change together. Routed to Task 12.

## Series row cost against F

Measured with `measurement --series-cost` in the DHAT build, one process per point. Each point is one request of N new minimal series admitted into an empty block through the production path. Series Arrow pinned is 311 to 381 B per series; each cache entry adds 70 to 83 B.

| Signal | F | full charge per series | real block heap per series, N >= 1000 |
| --- | --- | --- | --- |
| logs | 1352 | 1964 | 342-365 |
| logs + 1 denormalized column | 1480 | 2150 | 343-375 |
| metrics | 2120 | 2868 | 405-428 |
| metrics + 1 denormalized column | 2248 | 3054 | 406-438 |

- The marginal cost, taken as the slope from N = 10 to N = 100, is about 330 B per logs series and about 400 B per metrics series. A denormalized column adds 1 to 10 B.
- F alone is about 4.0x (logs) and 5.2x (metrics) conservative. The full charge is about 5.7x and 7.0x.
- The first series of a block costs about 19 to 22 KB of fixed table and batch structures. That is per block, not per series.
- Extraction peaks at about 510 B per series (50.9 MB for 100k metrics series).
- At the real cost, 500 MiB holds about 1.3M metric series against the reservation's per-request ceiling of about 215k. F was not changed.
- Logs points stop at 50k: OTLP logs requests with more than 65,536 attributed records are refused by the conversion (16-bit ids).

## Task 4 logs residual and the residual shares

The shares add up exactly to the 1 s gauge residual:

```text
1 s gauge residual = sampling skew + retention growth + non-heap anonymous growth + live heap the tracked counters missed
```

Non-heap anonymous growth is 0 in every family below.

| Family | 1 s gauge peak | (a) sampling skew | (b) retention | (c) heap not tracked | 100 ms gauge peak |
| --- | --- | --- | --- | --- | --- |
| strict, bgthread off | 22.2-24.1 MB | 5.2-5.6 | 24.9-25.5 | -6.4 to -8.5 | 16.9-18.5 |
| **strict high-rate (Task 4 shape)** | **44.0 / 55.1 / 62.2 MB** | **31.6-32.0** | 14.3-27.3 | -3.8 to +8.3 | 12.4-30.2 |
| buffered | 17.5-23.1 MB | 5.5-10.9 | 25.4-31.5 | -17.1 to -18.7 | 6.7-13.2 |
| strict, new default binary | 16.3-21.0 MB | 4.3-6.3 | 15.6-22.9 | -3.6 to -8.1 | 12.0-14.8 |

The Task 4 failure (48 to 65 MB) is reproduced: 44 to 62 MB, with 4 to 13 samples over tolerance per pair. At the peak it splits as follows:

- **(a) Sampling skew is about 32 MB, 51 to 72 percent of the residual.** Blocks turn over every 0.14 s, faster than a 1 s gauge.
- **(b) Allocator retention growth is 14 to 27 MB.**
- **(c) The uncharged term is small**, -4 to +8 MB. My ledger, including the flush workspace, stays at 4.7 to 8.6 MB.

At a 100 ms gauge the residual peaks at 12 to 30 MB; one pair out of three still has 3 samples over 33.5 MB. The tolerance was not changed.

## Block-pair uncertainty (umbrella finding 8)

`memory.accounted` is published only when the worker answers a CollectTelemetry, and a flush holds the worker thread, so no collection is answered during a flush. A sample therefore pairs RSS with an accounted value up to one collection old.

The bound is the change between consecutive distinct collections at the 100 ms interval, given as p95 [max]:

| Family | accounted | tracked heap | RSS |
| --- | --- | --- | --- |
| strict | 2.6-3.0 [10.3-10.5] MB | 2.6 [4.2-4.6] MB | 3.4-5.7 [19-23] MB |
| high-rate | 6.2 [17.9] MB | 11.1 [36.8] MB | 13.0 [39.2] MB |

At a 1 s interval the skew is about 5 MB in strict and about 32 MB in high-rate, as in the previous table.

## Findings

1. **The default-allocator families fail `pair_stability` only on quiet-phase RSS.** Preserved refusals, labelled "background_thread off": memory-strict f001, memory-strict-logs-high-rate f001 and memory-buffered f001.
   - Control idle spread: 23.4 / 9.3 / 23.0 percent. Measured idle: 15.3 / 16.1 percent. Retained: up to 33.7 percent.
   - Non-heap and allocated are flat (at most 2 percent). The whole spread is allocator retention, 8.8 to 30.5 MB.
   - Mechanism: without the background thread, decay only progresses while the process allocates. No baselines were written.
2. **background_thread on reduces the spread, but the family still fails on the control.**
   - Via `MALLOC_CONF`: memory-strict-bgthread f001 passed with every primary metric's spread at 6.6 percent or less, and wrote `baseline-memory-strict-bgthread-30f46f91e7de52d4.json`. The high-rate and buffered bgthread families still failed, on the noop control only (control idle 29.9 / 17.0 percent, control peak 16.1 / 31.8 percent). The measured engine's spread stayed at 10.6 percent or less.
   - On the new default binary (f64cd11af), memory-strict f002 has a stable measured engine: idle 5.0, retained 1.4, peak RSS 5.6 percent. It **fails** `pair_stability` on `control_peak_rss_bytes` at 20.2 percent; control idle is 14.7 percent.
   - The noop control's retention right after its short bursts (8 to 19 MB over 6 MB allocated) is not removed by the thread. I did not drop control metrics from the primary set after seeing results. Task 12 should decide between a longer control settle and a different role for the control metric.
   - There is no strict baseline at the new default fingerprint yet.
3. **The harness's `rss_reconciliation` heap term is unreliable.** The pipeline `memory.usage` counter keeps bytes freed on other threads. With local storage it drifts to +137 MB (strict) and +273 MB (buffered), against 6.5 MB of real live heap. That drives the harness residual to between -140 and -280 MB, which can mask a positive residual. With MinIO the drift is only +0.7 MB. Task 12: base the heap term on jemalloc allocated instead.
4. **`process.memory.usage.bytes` is coarse and high.** It refreshes every 5 s, whatever `check_interval` says, and jemalloc `stats.resident` overstates the resident heap by 11 to 13 MB.
5. **The heap-profile diagnostic is unusable for attribution.** The first `/debug/pprof/heap` dump makes the symbolizer hold about 266 MB of heap, which dominates every later profile.
6. **decay0** (`dirty_decay_ms:0, muzzy_decay_ms:0`) confirms the split: retention falls to 2.6 to 4.0 MB while allocated stays 13.4 MB (13.1 MB with default decay).

## Evidence

Published and committed in `docs/superpowers/reports/series-parquet-measurement/`:

- `memory-strict.json` (f002, with f001 as a child index that also carries the probes and the series cost);
- `memory-strict-logs-high-rate.json` and `memory-buffered.json`;
- the three `*-bgthread.json` indexes;
- every pair and aggregate, and the one baseline.

The raw artifacts, without Parquet, are in `<repo>/.measurement-artifacts/`: `memory-raw-20260923-default.tgz`, `memory-raw-20260923-bgthread-env.tgz` and `memory-raw-20260923-default-bgthread.tgz`. I deleted the `/tmp` output.

## Concerns

- Other agents compiled without the lease 7 times during my measured windows. Each was detected, rerun and excluded.
- The measured workloads are modest: 1 to 13 MB blocks. Large-block transients rely on the Task 3 DHAT profiles.
- The flush-workspace term comes from Task 3's 23 MB block and errs toward explaining too much in the smaller in-process blocks.
- The buffer's heap and WAL mapping are not separated from the exporter's. The buffer term stays inside the residual.

## Fix round 1

Commits (on d32e543b3):

| Commit | Item |
| --- | --- |
| a9835728e | 1. The merge-key charge is now a bound for the merge's whole life. Each heap entry is charged the longest encoded key of any row (`longest_key`, computed when the merge is built), so the value the sink takes at construction never falls below what is resident later. Test `resident_key_bytes_bound_keys_that_grow_during_the_merge`: string keys grow from 4 to about 600 bytes during the merge, the resident size grows, and the bound holds at every chunk. |
| 4928e04f6 | 5. The background_thread default requires `target_os = "linux"` and `target_env = "gnu"`. musl keeps jemalloc's own default, and the startup line reports `off` (documented in main.rs, the README and the chloggen entry). The default test skips, saying so, when `MALLOC_CONF` is set; I checked this with `MALLOC_CONF=background_thread:false`. The changelog keeps the `[4128]` placeholder and now states only the measured quiet-RSS spread of a series_parquet engine, not "RSS is predictable". |
| 7c5bea578 | 2 and 4. The ledger workspace term is zero (`NO_WORKSPACE_TERM`, with provenance) because no in-run flush-workspace measurement exists. `pair_uncertainty` covers both lifetimes of a pair. `reaggregate` re-judges published families from their committed pairs. There are 2 new contract tests. |
| 9c8151e5f | Evidence: `memory-ledger-reaggregation-r1.json`. It references every aggregate and pair it read by hash; no published file was rewritten. |

Tests:
- 202 contract tests OK.
- series-lake sort and sink tests: 34 passed.
- df_engine bin tests: 15 passed.
- clippy clean.
- The final `cargo xtask check` result is at the end of this section.

**Item 2: families re-judged under the corrected ledger** (workspace term 0; ledger residual max per pair against the 33.5 MB tolerance):

| Family | Ledger max per pair | Ledger gate | pair_stability | Passes every rule |
| --- | --- | --- | --- | --- |
| memory-strict f001 (bgthread off) | 8.6 / 4.5 / 6.6 MB | pass | fail | no (preserved refusal) |
| memory-strict f002 (new default binary) | 7.6 / 8.1 / 7.7 MB | pass | fail (control peak 20 percent) | no |
| memory-strict-logs-high-rate f001 | **38.2 / 33.4 / 36.2 MB** | **FAIL** (3 and 1 samples beyond) | fail | no |
| memory-buffered f001 | 6.2 / 10.8 / 6.9 MB | pass | fail | no |
| memory-strict-bgthread f001 | 8.4 / 9.1 / 4.3 MB | pass | pass | **yes** (its baseline stands) |
| memory-strict-logs-high-rate-bgthread f001 | **37.6 / 46.7 / 45.6 MB** | **FAIL** (4 / 2 / 3 samples beyond) | fail | no |
| memory-buffered-bgthread f001 | 8.7 / 8.7 / 6.6 MB | pass | fail | no |

- **Finding (Task 12):** in the logs high-rate shape, the live heap beyond `memory.accounted` and the control heap reaches 33.4 to 46.7 MB during flushes. The largest residual this ledger can see is the flush's transient workspace: merge chunk, Parquet encoder and upload parts. The worker never accounts for it and does not publish it. The earlier report's "ledger passes in every family" held only because another fixture's 36.9 MB stage peak was subtracted; that claim is withdrawn.
- **Remedy (Task 12):** have the sink publish its live flush workspace so the ledger can subtract a term measured in the same run: `AsyncArrowWriter::memory_size`, the current chunk's pinned bytes, and the parts in flight.
- The share decomposition of the harness's 1 s-gauge residual (Task 4 question) is unchanged. It never used the workspace term: sampling skew about 32 MB, allocator retention 14 to 27 MB, untracked heap -4 to +8 MB.

**Item 4: pair uncertainty over both lifetimes.** Each pair now reports the following, all read back from the committed pairs:
- block pairing of the measured engine's accounted bytes and of both lifetimes' RSS;
- the control heap's deviation from the median the ledger subtracts;
- the allocator-print timing: the age of the latest print when a sample reads it, and how far allocated moves between consecutive prints.

Maxima add up to `bound_bytes`, a bound only over the sampled observations. 95th percentiles add up to `estimate_bytes`, labelled an estimate. The unexplained-heap figures are per-pair ranges:

| Family | Unexplained-heap bound | Unexplained-heap estimate | RSS-delta bound (both lifetimes) |
| --- | --- | --- | --- |
| strict (f001, f002, bgthread) | 27.9-33.2 MB | 10.1-12.6 MB | 15.5-42.0 MB |
| high-rate | 59.2-69.8 MB | 18.5-24.4 MB | 36.4-69.3 MB |
| buffered | 30.0-33.1 MB | 8.6-10.7 MB | 14.8-39.2 MB |

Breakdown of the unexplained-heap figures:
- Allocated moves between consecutive allocator prints by up to 15-20 MB (strict and buffered) and 37 MB (high-rate). That is the largest term, because one print can follow a single large allocation such as a merge chunk.
- Accounted pairing: 9-11 MB (strict and buffered), 20-30 MB (high-rate).
- Control heap deviation: 2.0-3.1 MB.
- Print age when read: at most one sampling interval. In these runs every sample saw a fresh print, so the recorded age is 0 ms.

`paired_sign_consistent` holds in every family under the new bounds; every paired difference kept one sign.

**Item 3: moved, no work here.** The buffered heap/WAL mapping split, physical versus logical WAL disk, and the backlog/replay experiment go to Task 13. The upload concurrency 1-versus-2 burst goes to Task 5.

**Note for Task 12, from the review:** the series-cost numbers exclude variable-width keys, run fragmentation, tokens and merge keys. They cannot on their own justify lowering F; keep F.

**Not re-measured:** the committed families ran on the build before item 1. Item 1 only raises the merge-key charge inside `memory.accounted`, so it can only lower the ledger residual. The re-judged verdicts above are therefore conservative for item 1.

`cargo xtask check` after fix round 1: "All tests passed successfully", exit 0 (run under the lease).

## Fix round 2

| Commit | Item |
| --- | --- |
| c0eb74bc6 | Items 1 and 2 (they touch the same functions, so one commit). `ledger_interval_movement` measures allocator movement over each lifetime's complete chronological print stream. It takes the range of allocated across the whole interval a ledger sample spans: from the previous sample's last print, through this sample's prints, to the first print after it. A sample that printed nothing spans from the last earlier print to the next later one. The reaggregation manifest now lists every file it reads, including the source indexes, each with its hash. `reaggregate` takes its run id as an argument. New tests: `test_allocator_movement_spans_the_whole_print_stream` (the old per-sample `zip` would return `[2, 2]` MiB against the expected six-interval list, so it catches the cross-sample transition) and `test_reaggregate_names_every_file_it_reads` (a direct run over a synthetic report directory; checks the manifest, the hashes, the verdicts and that the sources are unchanged). |
| b2e9f1763 | `memory-ledger-reaggregation-r2.json`: a new file; r1 stays as published. It lists all 33 files it read: 21 pairs, 7 aggregates and 5 indexes. |
| 2de5a8c7c | Item 3. The main.rs comment now matches the README and the changelog: the measured engine's quiet-RSS spread was 15-34 percent without the thread and 1.4-6.6 percent with it. |

Tests: the contract module passes 204 tests; the df_engine bin tests pass 15. Both ran under the lease with `taskset -c 0-7,16-23`.

Recomputed uncertainty (r2). The verdicts are unchanged from r1:

| Family | Unexplained-heap bound (sum of maxima) | Estimate (sum of p95) | Allocator movement max |
| --- | --- | --- | --- |
| strict (f001, f002, bgthread) | 30.8-33.6 MB | 16.1-19.3 MB | 17.9-20.7 MB |
| buffered (both allocator settings) | 32.4-35.0 MB | 16.3-19.0 MB | 19.8-22.7 MB |
| logs high-rate (both allocator settings) | 72.3-83.4 MB | 56.0-61.6 MB | 50.2-51.7 MB |

Your example: high-rate r001 now records 51.7 MB. That is at least the 40.7 MB maximum over adjacent prints in the full stream, because the interval range includes cumulative movement across several prints. The RSS-delta bounds are unchanged from r1, and `paired_sign_consistent` still holds in every family.
