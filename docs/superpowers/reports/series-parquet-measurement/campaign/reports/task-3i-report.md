# Task 3i report: bounded flush work on the ingest core, the flush workspace, and the loss contract

Main checkout, branch `series-parquet-exporter`, started at `0dd988914`.

## Commits

In history order:

| Commit | What |
| --- | --- |
| a223a4d25 | bench: `measurement --flush-stall` probe (item 1, measure first). |
| fcceef306 | bench: the probe writes to a local store and records a cancel observation (superseded by a61f3c48d). |
| bbc888b3d | fix(series-lake): bounded slices, `MergeBuild` and `MergeIter::step`; the sink yields and checks cancellation between steps. |
| da4df0228 | feat: the live flush workspace, `flush.workspace`, charged in `memory.accounted`; harness in-run ledger term. |
| 1acdb648e | harness: `nobgthread` allocator mode. |
| fd9deb7b7 | evidence: the two logs high-rate memory families re-judged. |
| 712f5d0bf | feat: `metrics.exemplars`, `dropped.exemplars{signal}`, README loss section, FORMAT.md, chloggen. |
| 75732cafd | perf(series-lake): one merge step carries work across runs and columns. |
| f811ce961 | evidence: first flush-stall results and the sink/encode stage spot family. |
| d79cbfa1f | fix: exemplars dropped by default whatever `unsupported` says; only an explicit `metrics.exemplars: reject` refuses. |
| 1e1314f82 | fix round 1, items 1 and 2: every merge step bounded by rows processed (row-range interleave); the build keeps its totals and seeds the heap incrementally. |
| 5d2727fee | fix round 1, item 4: an unsorted chunk is the block's own run and charges no workspace. |
| f590dfa30 | fix round 1, item 5: the upload ledger releases each buffer when its bytes have landed, in any order. |
| a61f3c48d | fix round 1, item 3: a fired token stops a writer step at once; the probe times the sink's own observation. |
| ff67591cf | fix round 1, minor: values rows are not deduplicated, series descriptors are. |
| 4980383c1 | fix round 1, item 3 follow-up: the cleanup deadline is taken where the cancellation is observed. |
| 545c9f038 | bench: the probe removes a write's store directory best effort. |
| 2f78bf190 | evidence: flush-stall re-measured with the fixed probe; README figures. |
| 07b3a5206 | fix round 2, item 2: the build's live key figure includes the heap and every run's owned key. |
| 8d5be48eb | fix round 2, item 1: chunk columns built in place by a `ChunkBuilder` over `MutableArrayData`, at most a step's budget copied per step. |
| f8867b32b | evidence: flush-stall on the changed build; README step description and figures. |
| 92c4a35f3 | evidence: the flush-stall bench configurations scrubbed of host paths. |
| 8f5dddc4a | fix round 3: chunk steps budgeted in copied elements, every buffer presized and charged as allocated, dictionaries and offset overflow fail cleanly. |
| 5b717a528 | evidence: flush-stall on the round-3 build; README figures. |

## 1. Measure first: the flush-stall probe

There was no Task 5 worst-stall probe, so this task adds one:
`measurement --flush-stall` in the series-lake measurement bench
(`crates/series-lake/benches/measurement/flush_stall.rs`).

What it does:

1. It builds the largest block the default configuration admits. Every
   request of a harness input goes through conversion, extraction and
   `Block::reserve`/`admit` until the block refuses one as full, and then
   the block is sealed. With the default budgets the block fills on its
   worst-case byte reservation before it reaches 4096 requests.
2. It runs `Sink::write_block` on a current-thread runtime, the exporter's
   shape, into a local file store in the scratch directory. That store does
   its I/O on the blocking pool, off the measured thread. An in-memory store
   was tried first and dropped: completing a multipart upload there copies
   the whole object into one buffer on the calling thread, a stretch no real
   store has.
3. A ticker task on the same runtime stands in for the node loop. It records
   every time it is scheduled. The longest gap between two ticks is the
   longest the node loop could not run. Each poll of the write future is
   timed too.
4. It runs 20 cancelled writes. Each has a signal instant at an evenly spaced
   fraction of the uncancelled write. The ticker cancels at its first tick
   after the instant, as the node loop would after a shutdown deadline.
   - "Observed" runs from the signal to the first poll of the write after
     the cancel, where the sink checks its token.
   - "Returned" runs from the signal to `write_block` returning, abort
     included.
5. It reports a per-phase breakdown of the synchronous work of each table.
6. `--dump` writes the files of the first write with a fixed file identity,
   for byte comparison.

Inputs (harness `write_stage_input`, the Task 3 workload shapes lengthened
to fill a block):

| Fixture | Requests offered | Admitted | Block charged | Values rows |
| --- | --- | --- | --- | --- |
| logs-1k-stable | 4091 | 2096 | 242.7 MB | 209,600 |
| metrics-mixed | 4096 | 3716 | 69.2 MB | 371,600 |
| logs-8k-churn-wide (body sort key) | 1022 | 813 | 338.9 MB | 40,650 |
| logs-512k-near-limit | 255 | 236 | 495.1 MB | 944 |

Before the change, where the stretch came from (values table):

| Fixture | Key build | Longest chunk production | Longest chunk encode | Longest row-group close | Close file |
| --- | --- | --- | --- | --- | --- |
| logs-1k-stable | 1.3 ms | 2.7 ms | 23.8 ms | 21.3 ms | 6.1 ms |
| metrics-mixed | 2.2 ms | 11.6 ms | 20.3 ms | none | 6.6 ms |
| logs-8k-churn-wide | 71.7 ms | 4.1 ms | 18.0 ms | 24.8 ms | 16.1 ms |
| logs-512k-near-limit | 0.3 ms | 1.3 ms | 15.5 ms | 20.7 ms | 15.6 ms |

The worst stretch was two of these run back to back in one poll:

- logs: a chunk encode followed by its row-group close;
- metrics: chunk production followed by its encode;
- a wide sort key: the whole key build.

With the default keys, key building was not the cost; with a wide key it
was the largest single step.

## 2. Bounded slices

`crates/series-lake/src/sort.rs`:

- `MergeBuild::step` encodes sort keys one slice at a time. A slice has at
  most `MERGE_STEP_ROWS` (8,192) rows and is sized to about
  `MERGE_STEP_KEY_BYTES` (1 MiB) of encoded key from the width seen so far;
  the first slice is 256 rows. Every slice is encoded by the merge's one
  `RowConverter`, and each run keeps its keys as segments, so every
  comparison, and therefore the output, is what one call would give.
- `MergeIter::step` pops the heap in slices of the same budget (rows, and
  bytes of key copied), then interleaves one output column per step.
- `merge_runs` and `Iterator::next` run the same steps back to back.

`crates/series-lake/src/sink.rs`: the write loop is a sequence of those
steps with `tokio::task::yield_now()` and a cancellation check between every
two, and producing, encoding and flushing a chunk each get a poll of their
own. The shutdown deadline is observed through the same yields: the node
loop and `write_until`'s deadline branch run at each one, and the worker
cancels the token when the deadline passes. Chunk boundaries, writer calls
and the flush predicate are unchanged.

What cannot be sliced without changing bytes: one `AsyncArrowWriter::write`
of a chunk (bounded by `sorting.merge_chunk_bytes`) and one row-group close
(bounded by `parquet.row_group_bytes`). Splitting a chunk write would move
Parquet's mini-batch boundaries, and so the page boundaries, of the nested
map and list columns. Those two steps are now the longest stretch.

## 3. Before and after

Both builds ran the same probe source, 3 uncancelled and 20 cancelled
writes per fixture, under `taskset -c 0-7,16-23` with the host lease held,
back to back on release bench binaries.

Longest stretch of the thread (ticker gap), worst of 3 writes:

| Fixture | Before (fcceef306) | After slicing (bbc888b3d) | Final (75732cafd) |
| --- | --- | --- | --- |
| logs-1k-stable | 47.6 ms | 25.1 ms | 25.0 ms |
| metrics-mixed | 33.9 ms | 22.3 ms | 21.4 ms |
| logs-8k-churn-wide | 64.3 ms | 22.1 ms | 21.8 ms |
| logs-512k-near-limit | 35.7 ms | 22.1 ms | 19.9 ms |

Cancellation, 20 signals per fixture, worst and median:

| Fixture | Observed before | Observed final | Returned before | Returned final |
| --- | --- | --- | --- | --- |
| logs-1k-stable | 36.0 / 13.5 ms | 19.8 / 9.3 ms | 36.3 ms | 25.1 ms |
| metrics-mixed | 28.2 / 12.2 ms | 14.2 / 0.5 ms | 28.9 ms | 14.8 ms |
| logs-8k-churn-wide | 56.2 / 14.4 ms | 14.8 / 1.7 ms | 67.6 ms | 29.1 ms |
| logs-512k-near-limit | 31.2 / 8.1 ms | 17.7 / 3.5 ms | 32.5 ms | 28.5 ms |

"Returned" adds the multipart abort and dropping the merge, about 14 ms with
the wide key. The node loop itself is not
kept waiting by those: the observed column bounds how long it waits.

Whole-flush CPU, median of 3 (process CPU, which includes the blocking
pool's file writes):

| Fixture | Before | Final |
| --- | --- | --- |
| logs-1k-stable | 451 ms | 463 ms |
| metrics-mixed | 132 ms | 132 ms |
| logs-8k-churn-wide | 600 ms | 565 ms |
| logs-512k-near-limit | 628 ms | 595 ms |

The differences, -6 to +3 percent, are inside the run-to-run spread of
either build.

Lowering `sorting.merge_chunk_bytes` or `parquet.row_group_bytes` shortens
the two remaining steps but changes the files; that is a configuration
choice, not measured here.

## 4. Byte identity

The probe dumps both files of each fixture with a fixed identity (writer
`local_1`, boot id `flushstall`, seq 1, fixed seal stamp).

- All 8 files (series and values for the four fixtures, 8 KB to 369 MB)
  have identical SHA-256 before the change, after the slicing commit, and
  on the final build. For example, logs-1k-stable values is
  `15aa86b579c1...`, 158,355,956 bytes, in all three builds.
- `sort::tests::sliced_merge_matches_the_unsliced_merge_and_a_stable_sort`
  checks chunk boundaries and chunk contents for step budgets from one row
  and one key byte upwards, against the unsliced merge and a stable sort.
- The goldens (`tests/golden.rs`, `tests/golden_roundtrip.rs`), the oracle
  and the existing sink and sort tests pass unchanged.
- E2E files cannot be compared across runs byte for byte, because the boot
  id and the seal stamp differ per run; the E2E suite's oracle checks
  their content (section Tests).

## 5. Flush workspace accounting

`Sink::flush_workspace_bytes()` is read live and has three terms:

- the merge chunk being produced: the popped row indices and the columns
  interleaved so far, then the finished chunk until it is encoded;
- `AsyncArrowWriter::memory_size()`, the encoder's in-progress row group;
- an upload ledger. A counting `AsyncFileWriter` enters every buffer the
  encoder hands over; a counting multipart upload and single put enter
  every part and when it lands or is dropped. A part is a slice of the
  row-group buffer and keeps all of it alive, so a buffer counts whole
  until every byte before its end has landed. An out-of-order landing
  overstates by at most the parts in flight.

The worker adds it to `memory.accounted` and publishes it as
`flush.workspace` (`By`). `memory.py` records it per sample
(`flush_workspace_bytes`), reports `measured_peak_flush_workspace_bytes`,
and names the ledger term `IN_RUN_WORKSPACE_TERM`: zero, with provenance,
because the ledger already subtracts it through `memory.accounted`.

Measured peaks:

| Where | Peak flush workspace |
| --- | --- |
| probe, logs-1k-stable 243 MB block | 166.0 MB |
| probe, logs-8k-churn-wide | 164.9 MB |
| probe, logs-512k-near-limit | 161.7 MB |
| probe, metrics-mixed | 33.1 MB |
| engine, logs high-rate (13 MB blocks) | 20.6 MB |

Task 3's DHAT sink peaks were 112 to 161 MB on the large fixtures, the same
order. Finding for Task 12: the tail of each row group, shorter than one
part, waits in the buffered writer for the next row group and pins the
whole previous row-group buffer, so a large table holds roughly encoder
plus one row group plus one chunk: about 64 + 64 + 16 MB at the defaults.
Copying the tail would release it.

## 6. Re-judged logs high-rate ledger

`measure memory` with
`families=[["strict","logs-high-rate","nobgthread"],["strict","logs-high-rate","bgthread"]]`,
no diagnostics, no probes, release engine at da4df0228 + 1acdb648e, under
`taskset -c 0-7,16-23`; the harness took the lease itself. No pair was
invalidated by a compiler.

Ledger residual maximum per pair (tolerance 33.5 MB), against Task 6's
re-judged r1 values:

| Family | Task 6 | Task 3i | Ledger gate per pair |
| --- | --- | --- | --- |
| thread off (Task 6 f001, now nobgthread f001) | 38.2 / 33.4 / 36.2 MB | 31.0 / 46.5 / 28.9 MB | pass / fail / pass |
| thread on (Task 6 bgthread f001, now bgthread f002) | 37.6 / 46.7 / 45.6 MB | 43.6 / 37.0 / 33.7 MB | fail / fail / fail |

**The ledger does not pass**: 2 of 6 pairs pass, and neither family does.
Where the remaining excursions are:

- Every positive sample beyond tolerance, except the first load sample of
  one pair, is a sample whose `flush.workspace` reads 0 while the next
  sample reads 20.5 MB: the first instant of a flush, where the allocator
  print already shows the flush's allocation and the telemetry answer
  predates it.
- Every negative sample beyond tolerance (-32 to -33 MB, one per pair in
  five pairs) is the mirror: `flush.workspace` still 20.5 MB in the answer,
  while the allocator print shows the memory freed.
- The residual here is allocated minus accounted minus control, so both are
  one-sample pairing skew between the telemetry answer and the allocator
  print. Task 6 measured that skew at up to 50 MB in this shape.
- The in-flush samples themselves now sit well inside tolerance. The load
  median of the residual is -4.7 MB (thread off) and -2.1 MB (thread on).
- The first load sample (+46.5 MB, no flush running) is the burst of 256
  in-flight requests, converted in the receiver and not yet admitted.

Both families also still fail `pair_stability`, on the noop control's RSS
only (control idle spread 30 to 33 percent), as in Task 6. The measured
engine's primary metrics spread by 14 percent or less.

The Task 6 verdict "the flush workspace takes the ledger past tolerance"
is superseded. The workspace is now measured and charged, and at the
samples where it is known the ledger closes. What still exceeds 33.5 MB is
the pairing of a 100 ms telemetry sample with an allocator print at a
flush's edge. Closing that needs a synchronous pairing or a
workspace-change-aware tolerance (Task 12), not another term.

Evidence: `docs/superpowers/reports/series-parquet-measurement/memory-strict-logs-high-rate-{nobgthread,bgthread}*.json`;
raw artifacts without Parquet in `.measurement-artifacts/memory-raw-20260923-task3i.tgz`.

## 7. Loss contract

Policy (series-lake `LakeConfig::exemplar_policy`), as corrected in
d79cbfa1f after the lead relayed the user's decision:

- `metrics.exemplars` alone decides, whatever `unsupported` says.
- Unset or `drop` keeps the points and counts the exemplars in
  `dropped.exemplars`.
- Only an explicit `reject` refuses a request whose stored points carry
  exemplars, as a permanent `unsupported` nack. Its reason reads
  "exemplars are not stored by series_parquet, and metrics.exemplars:
  reject refuses a request that carries them; set metrics.exemplars: drop
  (the default) to store the points without their exemplars, or route the
  request to another exporter".
- `logs.exemplars` is refused at startup.
- An exemplar of a point that `unsupported: drop` discards goes with its
  point and is counted.
- `dropped.exemplars` (`{exemplar}`, label `signal`, only `metrics` occurs)
  replaces `dropped.unsupported{kind=exemplar}`.

712f5d0bf first coupled the policy to `unsupported`, so the default
refused exemplar-carrying requests; d79cbfa1f removes that coupling.

The README section "What this exporter does not keep" is a table of every
intentional loss, what is kept instead, and how it shows:

- traces;
- exponential histogram and summary points;
- exemplars, with their filtered attributes and trace and span ids;
- attribute value types in the three maps, with where types survive
  (`identity_bytes`, `series_id`, typed denormalized columns);
- the type of a non-string log body;
- an optional metrics value that is zero in every point of a request (the
  histogram `sum` case);
- zero and out-of-range timestamps (`timestamp.out_of_range`);
- mistyped denormalized values (`denormalize.type_mismatch`);
- metric metadata attributes;
- `dropped_attributes_count`;
- arrival order.

A closing note says nothing is deduplicated. The table is followed by the
existing Limits section, whose unsupported paragraph now describes the
exemplar policy. FORMAT.md's two exemplar statements follow the policy.
The chloggen entry is `.chloggen/series-parquet-exemplar-policy.yaml`,
with the placeholder issue 4128 like the other branch entries.

## 8. CPU per record

`measure stages` spot family, `stages=["encode","sink"]`, all four
workloads, 3 repetitions, `index_name=stages-spot-task3i`. It ran twice,
under `taskset -c 0-7,16-23` with the lease per child and Docker.

- The first run (f003, at 712f5d0bf) found sink on metrics-mixed at 400.6
  ns per record against 353.0 in `stages.json`, +13.5 percent.
- A test (`a_small_merge_takes_one_step_per_phase`) and 75732cafd followed.
  A step now carries work across runs and columns up to its budget, so a
  small table, such as the series table's 100 one-row runs, takes one step
  per phase rather than one per run and per column.
- The second run (f004, at 75732cafd) gave the table below.

CPU per record, median of 3 repetitions [min, max], against the family of
record `stages.json`:

| Stage | Workload | f004 | stages.json | Ratio |
| --- | --- | --- | --- | --- |
| sink | logs-1k-stable | 2274.8 [2268.7, 2276.2] | 2250.9 [2242.2, 2532.1] | 1.011 |
| sink | metrics-mixed | 396.0 [391.9, 396.5] | 353.0 [351.2, 353.3] | 1.122 |
| sink | logs-8k-churn-wide | 13042 [12994, 13069] | 14484 [14408, 16775] | 0.900 |
| sink | logs-512k-near-limit | 675138 [671841, 733201] | 718203 [706145, 720622] | 0.940 |
| encode zstd | logs-1k-stable | 1612.4 | 1913.5 | 0.843 |
| encode zstd | metrics-mixed | 222.3 | 225.5 | 0.986 |
| encode zstd | logs-8k-churn-wide | 8258.7 | 8474.6 | 0.975 |
| encode zstd | logs-512k-near-limit | 533957 | 563987 | 0.947 |
| encode none | all four | | | 0.86 to 1.00 |

Like for like on sink metrics-mixed:

- Setup: the same input and configuration, the pre-task bench (0dd988914,
  built in a temporary worktree) and HEAD, 5 alternating rounds of 30+
  iterations each.
- Pre-task: 378.9 ns per record [369.4, 382.4].
- HEAD: 390.2 ns per record [389.1, 392.6].
- So of the 12 percent against `stages.json`, about 9 points predate this
  task, and this task adds 3.0 percent.

A `perf record` of both on the same stage does not attribute it:

- samples per iteration are 6.37 on HEAD against 6.50 before;
- the merge itself costs 0.43 against 0.42 samples per iteration;
- the visible differences are in parquet's dictionary interner and hashing,
  whose hash seed is random per process.

I report it as a 3 percent timing difference on the smallest block, not
explained by the changed code. Every larger block is at or below the family
of record. The spot family's own failed aggregates (repetition_stability
for encode logs-8k zstd timing; rss_reconciliation for the sink 512k heap
and timing children) are its own gates, and I did not investigate them
further.

## Tests

New tests (each written first and seen failing):

- series-lake sort:
  - `sliced_merge_matches_the_unsliced_merge_and_a_stable_sort`;
  - `one_merge_step_does_bounded_work` (rows processed per step);
  - `the_build_keeps_its_totals_as_it_goes`;
  - `a_small_merge_takes_one_step_per_phase`, red before 75732cafd.
- series-lake sink:
  - `the_write_returns_to_the_runtime_between_bounded_slices`, red: the
    ticker ran 4 times;
  - `a_cancellation_during_merge_key_building_stops_the_build`, red: 3.4
    of 3.4 MB of keys built before the cancel was seen;
  - `the_upload_ledger_releases_each_buffer_when_its_bytes_land`;
  - `only_a_chunk_the_merge_allocated_is_charged`;
  - `a_step_whose_token_has_fired_is_not_driven_again`;
  - `the_sink_publishes_the_upload_bytes_a_part_in_flight_holds`.
  The last two were red by compile only.
- series-lake config:
  - `metrics_exemplars_alone_decides_and_defaults_to_drop`;
  - `logs_exemplars_is_refused`;
  - `metrics_exemplars_parses_from_the_document`.
- series-lake extract:
  - `exemplars_are_dropped_by_default_and_refused_only_when_asked`;
  - `an_exemplar_of_a_dropped_point_is_dropped_with_it`;
  - `extracts_number_and_histogram` now runs under `unsupported: drop`.
- core-nodes exporter:
  - `the_flush_workspace_is_charged_while_a_write_is_in_flight`;
  - `an_exemplar_is_dropped_and_counted_by_default_and_refused_when_asked`;
  - `the_exemplar_policy_is_validated_at_startup`.
  The metric schema and extraction-counter tests were updated for
  `flush.workspace` and `dropped.exemplars`.
- harness contracts:
  - `test_the_ledger_uses_the_in_run_flush_workspace_when_published`;
  - `test_the_background_thread_can_be_turned_off_for_a_family`.

Results:

| Suite | Result |
| --- | --- |
| series-lake lib | 163 passed |
| series-lake integration: fuzz, golden, golden_roundtrip, oracle | 3 + 5 + 6 + 2 + 3 passed |
| core-nodes `series_parquet` | 99 passed |
| harness contracts (`test_measurement`) | 239 passed |

clippy `-D warnings` is clean for series-lake (all targets,
bench-harness) and core-nodes (series-parquet, all targets). `cargo fmt`
is clean.

Final gates, at f811ce961:

- `cargo xtask check`: "All tests passed successfully", exit 0, under the
  lease.
- The full E2E suite: 19 tests OK in 152 s, none skipped. It ran with
  `SERIES_REQUIRE_DOCKER=1` under `taskset -c 0-7,16-23`, on the debug
  engine built at HEAD, with the lease held.

Raw artifacts without Parquet are in `.measurement-artifacts/`:

- `memory-raw-20260923-task3i.tgz`;
- `stages-spot-task3i-raw-20260923-f003.tgz` and `-f004.tgz`;
- `flush-stall-ab-20260923.tgz`: the probe JSONs and the sink A/B reports.

The `/tmp` outputs, the probe inputs and dumps, and the temporary
pre-task worktree are deleted.

## Concerns

- **The ledger still fails** in the high-rate shape (2 of 6 pairs pass). The
  remaining excursions are one-sample pairing skew at a flush's first and
  last sample, both signs, about 33 to 46 MB. Task 12 should pair telemetry
  and allocator prints synchronously, or widen the tolerance by the
  workspace change between the two.
- **The flush workspace of a large table is about 160 MB** at the defaults.
  A row group's tail part pins the whole previous row-group buffer until
  the next row group is handed over. Task 12 could copy the tail and keep
  about 64 MB less.
- **Two steps remain unsliced**: one chunk encode and one row-group close,
  each about 20 to 25 ms at the defaults. Going below that needs smaller
  `merge_chunk_bytes`/`row_group_bytes`, which changes files, or the plan-4
  off-core writer.
- **Sink CPU on the smallest block is +3.0 percent** against the pre-task
  build, like for like, and not attributed by the profile. The spot family
  shows it at +12 percent against `stages.json`, most of which predates
  this task.
- The probe measures the sink directly, not an engine: the node loop is a
  ticker. The engine-level effect shows only indirectly, as in-flush ledger
  samples that now see the workspace.

## Fix round 1

Review verdict "Needs fixes"; every item fixed.

1. **Bounded rows per step** (1e1314f82). A step interleaves one row range
   of at most its remaining budget. A column of a chunk larger than
   `MERGE_STEP_ROWS` is built across steps, and its ranges are
   concatenated when the last one is done. The test asserts rows processed
   per step.
2. **Incremental totals** (1e1314f82). The build keeps key bytes, the
   longest key and the pinned totals as it goes, and seeds the heap run by
   run. `resident_key_bytes()` and `finish()` are constant work.
3. **Cancellation observation** (a61f3c48d, 4980383c1).
   - The writer step's select polls the token first.
   - The probe times the sink's first clock reading, which is where the
     sink takes the cleanup deadline.
   - The first run of the fixed probe showed the deadline was taken only
     after the merge was dropped, 4 ms late with a wide key. It is now
     taken where the token is seen.
4. **Unsorted double count** (5d2727fee). An unsorted chunk charges no
   workspace.
5. **Per-buffer upload release** (f590dfa30). Each buffer is released when
   its bytes have landed, in any order.
6. **Final gates and fixed-probe numbers.**

Re-measured with the fixed probe source (545c9f038) on both builds: before
is the library at fcceef306, after is 545c9f038. Each fixture ran 7
uncancelled and 20 cancelled writes. The files are byte-identical to each
other and to the committed pre-task manifest.

| Fixture | Longest stretch | Worst observation | Median observation | Whole-flush CPU, median of 7 [range] |
| --- | --- | --- | --- | --- |
| logs-1k-stable | 49.1 to 23.9 ms | 44.7 to 20.4 ms | 19.6 to 8.4 ms | 458.8 [443.6, 482.0] to 468.9 [464.8, 487.1] ms |
| metrics-mixed | 34.2 to 21.1 ms | 28.5 to 21.6 ms | 9.7 to 5.6 ms | 133.9 [127.8, 139.8] to 137.1 [136.3, 140.8] ms |
| logs-8k-churn-wide | 63.4 to 21.4 ms | 56.3 to 17.2 ms | 6.1 to 5.7 ms | 579.3 [570.3, 592.7] to 592.0 [545.5, 609.6] ms |
| logs-512k-near-limit | 33.0 to 20.4 ms | 30.6 to 19.9 ms | 6.4 to 6.7 ms | 604.0 [599.4, 610.6] to 599.9 [594.0, 613.2] ms |

- Observations are the sink's clock reading in 98 percent of the cancels.
  The rest were seen during finalization and timed at the return.
- The worst observation is at most one stretch plus the step boundary.
- CPU medians move by -0.7 to +2.4 percent with overlapping ranges. The
  row-range concatenation adds one copy of a column for chunks larger than
  8,192 rows (logs-1k-stable and metrics-mixed).

Final gates at 2f78bf190 (HEAD after fix round 1), under the lease:

- `cargo xtask check`: "All tests passed successfully", exit 0.
  - The first attempt hung in
    `otel-arrow-dfe-telemetry::log_tap::tests::retention_evicts_oldest_by_entry_limit`,
    18 minutes at 0 CPU. That crate is untouched by this task. I stopped
    the process, the test passed 5 of 5 in isolation, and the re-run of
    xtask passed.
- E2E: 19 tests OK in 156 s, none skipped. It ran with
  `SERIES_REQUIRE_DOCKER=1` under `taskset -c 0-7,16-23`, on the debug
  engine rebuilt at HEAD.
- Targeted tests: series-lake 166 lib tests plus fuzz, golden, round trip
  and oracle; core-nodes `series_parquet` 99; clippy `-D warnings` clean.

## Fix round 2

The scoped re-review confirmed items 2 to 5 and both minors, and found two
points open.

1. **A sliced column was concatenated in one step** (8d5be48eb).
   - `MergeIter::step` now only pops rows. It returns `Ready` once a
     chunk's rows are popped, recorded as runs of consecutive rows of one
     input run.
   - A `ChunkBuilder` borrows the merge and builds each column in a
     `MutableArrayData` over the runs' column data. The build collects
     that data run by run, so `finish()` stays constant.
   - Each column is sized first: a string or binary column's value bytes
     and a list's items are counted a bounded number of ranges per step.
     Then at most `MERGE_STEP_ROWS` rows are copied per step.
   - Completing a column freezes its buffer as it is.
   - A map's entry buffers grow as they fill; arrow's `Capacities` cannot
     presize a map's children. That amortizes to one extra copy of the
     map's entries per chunk.
   - The sink yields, checks the token and publishes the builder's
     workspace between builder steps.
   - `one_merge_step_does_bounded_work` counts rows copied per builder
     step, the completing step included, and requires every row of every
     column copied exactly once (214 rows by 3 columns).
2. **The live key figure undercounted during the build** (07b3a5206).
   `MergeBuild::resident_key_bytes()` now adds every seeded run's owned
   key and the heap's allocation to the segments, still in constant time.
   The test asserts that full figure against a recount at every slice.

The changed build, probe source 8d5be48eb, 7 uncancelled and 20 cancelled
writes per fixture. The before side stays round 1's.

| Fixture | Longest stretch | Worst observation | Whole-flush CPU median, before to after |
| --- | --- | --- | --- |
| logs-1k-stable | 27.7 ms on the cold first write, 23.3 to 24.5 ms on the other six | 21.4 ms | 458.8 to 446.1 ms |
| metrics-mixed | 21.0 ms | 19.8 ms | 133.9 to 135.4 ms |
| logs-8k-churn-wide | 19.1 ms | 13.3 ms | 579.3 to 483.1 ms |
| logs-512k-near-limit | 19.1 ms | 13.4 ms | 604.0 to 569.9 ms |

- All 8 files are byte-identical to the committed pre-task manifest
  (`flush-stall/files-head-8d5be48eb.sha256`).
- Copying runs of consecutive rows instead of gathering row by row lowered
  the CPU on the logs fixtures. metrics-mixed is 1.1 percent above, within
  its range.
- Tests: series-lake 166 lib plus fuzz, golden, round trip and oracle;
  core-nodes `series_parquet` 99; clippy `-D warnings` clean.

Final gates at f8867b32b, under the lease:

- `cargo xtask check`: "All tests passed successfully", exit 0, first
  attempt.
- E2E: 19 tests OK in 151 s, none skipped. It ran with
  `SERIES_REQUIRE_DOCKER=1` under `taskset -c 0-7,16-23`, on the debug
  engine rebuilt at HEAD.

## Fix round 3

The re-review confirmed both round-2 items and found three points in the
chunk builder (8f5dddc4a, one commit because all three live in one
rewritten builder).

1. **Budget in copied elements.**
   - A step's budget is `MERGE_STEP_ROWS` elements: a row counts one, each
     list item or map entry one more.
   - A range is split where its elements would pass the remaining budget.
     A row larger than the whole budget is copied alone, in a step that
     has copied nothing else; a row is bounded by `ingress.max_row_bytes`.
   - Lists and maps are assembled from their own offsets, validity and
     presized children. arrow 58.4's `MutableArrayData` panics on struct
     capacities, so it cannot presize a map's entries.
   - The charge test found one hidden reallocation: extending a string
     array reserves one offset more than it writes, so an offsets buffer
     presized exactly, at a multiple of 64 bytes, was reallocated by its
     last extend. String and binary builders are now created one row
     larger.
   - No buffer of an exporter column type grows inside a step.
2. **Workspace charged as allocated.**
   - Each column is charged its buffers' capacities, children and
     validity included, from the moment they are allocated.
   - `the_builder_charges_what_its_buffers_allocate` compares the charge
     with the completed column's real buffer capacities. They match
     exactly for the Int64, map, list and nullable-string columns.
3. **No panics on dictionaries or offset overflow.**
   - Presized totals are checked against the i32 offset limit before any
     copy and return an `ArrowError`. The test lowers the limit to 16
     bytes.
   - A dictionary column, or any type no lake dataset uses, is
     interleaved whole in one step as before. Differing dictionaries
     merge, and 200 values under Int8 keys are an error.
     `every_lake_column_is_built_in_bounded_steps` pins that no lake
     column takes that path.

The per-step test derives the elements copied from the builder's own
buffer lengths before and after each step, over runs with map, list and
string columns. It checks that every row, item and entry is copied
exactly once.

Changed build (probe 8f5dddc4a, 7 uncancelled and 20 cancelled writes per
fixture):

| Fixture | Longest stretch per write | Worst observation | CPU median, before to after |
| --- | --- | --- | --- |
| logs-1k-stable | 22.7 to 25.4 ms | 21.7 ms | 458.8 to 441.1 ms |
| metrics-mixed | 20.0 to 20.8 ms | 13.8 ms | 133.9 to 139.1 ms (+3.9 percent, ranges overlap) |
| logs-8k-churn-wide | 17.8 to 19.4 ms | 13.8 ms | 579.3 to 492.9 ms |
| logs-512k-near-limit | 18.7 to 22.2 ms | 11.6 ms | 604.0 to 589.6 ms |

- All 8 files are byte-identical to the committed pre-task manifest
  (`flush-stall/files-head-8f5dddc4a.sha256`).
- Tests: series-lake 170 lib plus fuzz, golden, round trip and oracle;
  core-nodes `series_parquet` 99; clippy `-D warnings` clean.

Gates at 5b717a528 (code at 8f5dddc4a), under the lease:

- E2E: 19 tests OK in 150 s, none skipped, `SERIES_REQUIRE_DOCKER=1`
  under `taskset -c 0-7,16-23`.
- `cargo xtask check` failed on a test this task does not touch:
  `otel-arrow-dfe-core-nodes exporters::otlp_grpc_exporter::tests::test_otlp_exporter`
  panicked with `AddrInUse`, because its test server's port was taken on
  the host. There is no diff under `otlp_grpc_exporter` in
  0dd988914..HEAD, and the test passed 3 of 3 alone. Per the lead's
  ruling, xtask was not rerun whole; the series-lake suite (170 lib plus
  integration) and core-nodes `series_parquet` (99) were rerun and pass.
- The lead ruled that the final gates (probe, xtask check, E2E) run once
  more after the re-review approves; the figures above come from the probe
  run made before that ruling.
