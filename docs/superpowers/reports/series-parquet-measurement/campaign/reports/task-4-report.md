# Task 4 report: direct CPU attribution by perf, reconciled with the stage families

Status: done, with one hard gate failing for a reason outside this task.
perf could attach: `kernel.perf_event_paranoid` was 4 at the start and
1 when the run happened. The full attribution family ran at 5e87fff73
under `taskset -c 0-7,16-23` and is committed. The metrics workload passes
every gate and wrote its baseline. The logs workload fails only
`rss_reconciliation`, in every repetition. That is the Task 6 accounting
finding, not a CPU-attribution defect. So `attribution.json` is `failed`
and no logs baseline exists.

## Commits (series-parquet-exporter, not pushed)

| Commit | Subject |
| --- | --- |
| 2a6f9c15a | feat(series_parquet): attribute engine CPU with perf and reconcile it with the stage families |
| 85f7b8b8f | chore: spot-measure the series parquet stages at HEAD for attribution |
| 32b24276b | fix(series_parquet): make perf attribution unwind and classify the real engine |
| f920b9cee | fix(series_parquet): load the oracle's read-back rows in one transaction |
| 479d46785 | feat(series_parquet): let attribution repetitions wait for a shared host lease |
| 5e87fff73 | fix(series_parquet): write attribution ledgers on the memory file system |
| 524405d92 | chore: attribute series parquet pipeline CPU costs |
| 318d94c36 | fix(series_parquet): publish perf's control FIFO argument without host paths |
| 46c0aab57 | chore(series_parquet): scrub the perf FIFO paths from the attribution evidence |

No Rust changed, so fmt, clippy and `cargo xtask check` were not needed.

## What was built

All of it is in `performance.py`, with small changes in `measure.py` and
`measurement.py`. `measure attribution` replaces the planned subcommand.
It is a long command and exits 3 when the host cannot profile.

- **`classify_cpu(samples)`** assigns each weighted sample exactly once. The
  frames are read outermost first, and the innermost frame a rule matches
  decides the sample.
  - Pass 1 matches production namespaces: the lake, the exporter, the
    engine's own crates, Parquet, `object_store`/`reqwest` and the allocator.
  - Pass 2 is a fallback for runtime and server libraries such as Tokio,
    hyper, h2 and tonic. They decide a sample only when no production frame
    is on its stack.
  - Encoder frames therefore win over their Sink and upload ancestors, and a
    Tokio frame inside an upload never takes the upload's sample.
  - Weights must be positive integers, and unmatched samples stay `unknown`.
    The classifier asserts that classified plus unknown equals the input.
    The rules are recorded in every index, via `classification_rules()`.
  - `allocator_callers` names the stage each allocator sample came from.
    That lets bench costs, which include allocation, be compared fairly.
- **Profiling** uses `perf record -e cpu-clock -F 199 -g --call-graph dwarf
  --sample-cpu -p ENGINE_PID`. The events start disabled (`-D -1`) and are
  enabled through control FIFOs across exactly the input phase, from the
  first request to the last durable acknowledgement. perf runs pinned to a
  profiler core of its own. `perf script` output is passed through binutils
  `c++filt`, because perf leaves Rust v0 symbols mangled.
- **The preflight** profiles a busy process with the same command line, and
  checks the engine's ELF segment layout (see Findings). When perf cannot
  attach, no repetition runs, the index is published `skipped` with the
  preflight evidence, acceptance is `incomplete`, and the command exits 3.
- **Each repetition** runs two lifetimes of the relinked release engine.
  Both use the same cores, a pinned MinIO, blocks of 128 requests and 256
  requests in flight.
  - The first is an unprofiled control that sends the first third of the
    input.
  - The second is the profiled lifetime.
  - Both get the full delivery oracle, drain proof, RSS reconciliation,
    flush-count cross-check and per-thread schedstat.
  - Inputs are prebuilt once, before any lease, in parallel processes, and
    read back by index byte-identical to `build_request`.
  - Sizing comes from the stage family's cost, targeting 10,000 classified
    samples per workload with margin 2.0.
- **Results.** Each repetition and each per-workload aggregate is a result
  file. The aggregates carry medians, pooled shares with binomial 95%
  intervals, and repetition stability, and apply the Controller baseline
  policy. They also record:
  - per-CPU and per-thread CPU;
  - worker on-CPU, run-queue and off-CPU fractions;
  - `upload_wait_s`, which is flush wall time from the cumulative
    `flush.duration` minus flush-task CPU;
  - the named residual and profile overhead.
- **Reconciliation** is against the pinned family, `stages.json`, measured
  at 42be4c5d3. It is identified by hash: sha256 ede76546... as measured,
  and e4d4b812... after the bc75f2f6c rescrub, which is verified. It also
  uses the spot family, `stages-spot.json`, sha256 bd38d603..., measured at
  936ace03a.
  - Validity is structural and is a hard gate: hashes, presence, the same
    logical-record denominator, and exclusivity.
  - Numeric agreement within 0.5 to 2x is a reported finding.
  - Each stage row keeps its own output representation. Byte rates are never
    added across stages.
- **Spot family.** A filtered `stages` run is now a spot family. It must
  publish under its own index (`--option index_name=stages-spot`) and is
  checked only against the stages it asked for. The spot family at HEAD
  measured convert, extract, sort_seal, merge, encode, upload, sink and
  otlp_minio for both primary workloads. It passed 108 of 108 children and
  36 of 36 aggregates, and wrote 36 new baselines, because the fingerprint
  changed. Its git record says dirty: the tree held only the uncommitted
  spot-scope edit to performance.py, committed in 2a6f9c15a.
- **Cases.** `CASE_ROLES["attribution"]` is producer 1, store 1 and
  profiler 1. With the engine's four-core reservation and the observability
  core, that is exactly 8 physical cores. The read-back runs on the stopped
  engine's cores and their siblings.

## Tests

The contract suite has 206 tests, all passing with 1 skip. That is the 184
existing tests plus 22 new ones in `AttributionContracts`. The new tests
cover:

- the brief's exclusivity test, verbatim;
- innermost-frame precedence and the runtime fallback;
- rejection of invalid weights;
- v0 and legacy symbol paths;
- parsing of `perf script` output;
- shares and binomial intervals;
- retention of the mapping rules;
- prebuilt requests matching `build_request` byte for byte;
- sizing for the sample target;
- the whole-block prefix of the control lifetime;
- roles under the pin, and the oracle's cores;
- retiring a ledger;
- cumulative flush readings;
- thread schedule fractions;
- the preflight through a stand-in perf that speaks the FIFO protocol, and
  one that refuses the way paranoid 4 does;
- refusal of an unwindable ELF layout;
- an incomplete index on a host that cannot profile, through the CLI with
  exit 3;
- reconciliation joins, and invalid reconciliations;
- the 10,000-sample aggregate gate and baseline creation;
- the spot-family scope;
- the attribution subcommand being long.

Every test has Scenario and Guarantees comments.

## Results (attribution.json, family f001, 3 repetitions per workload)

Samples are cpu-clock, sampling user and kernel, with coverage 0.997 to
0.998 of the engine's scheduler CPU in every repetition.

### Exclusive CPU shares, pooled over 3 repetitions (95% interval)

| Category | logs-1k-stable share | ns/record | metrics-mixed share | ns/record |
| --- | --- | --- | --- | --- |
| encoding | 32.8% +-0.7 | 1,706 | 11.2% +-0.4 | 274 |
| extraction | 16.2% +-0.6 | 844 | 24.1% +-0.6 | 580 |
| allocator | 12.2% +-0.5 | 641 | 14.7% +-0.5 | 356 |
| engine_runtime | 12.1% +-0.5 | 633 | 13.8% +-0.5 | 337 |
| sort_seal_merge | 10.9% +-0.5 | 566 | 13.2% +-0.5 | 316 |
| conversion | 7.8% +-0.4 | 414 | 20.2% +-0.6 | 482 |
| upload | 6.4% +-0.4 | 335 | 1.2% +-0.2 | 27 |
| buffer (admission) | 0.4% +-0.1 | 21 | 1.6% +-0.2 | 38 |
| unknown (named residual) | 1.4% +-0.2 | 71 | 0.2% +-0.1 | 4 |

- **Samples.** logs: 15,911 samples, 15,695 classified (5,138 / 5,240 /
  5,317 per repetition). metrics: 19,034 samples, 19,002 classified (6,420
  / 6,363 / 6,219). Total classified: 34,697.
- **Named residual.** logs: 1.1% of the profile ends in an `[unknown]` leaf
  and 0.2% in `__memmove_avx512`. metrics: the largest leaf is
  `arrow_data::MutableArrayData::with_capacities`, at 0.06%.
- **Allocator callers.** Extraction causes the largest part: 5.1% of logs
  and 6.0% of metrics CPU is allocation called from extraction.

### Engine totals, medians of 3

| | logs-1k-stable | metrics-mixed |
| --- | --- | --- |
| engine CPU ns/record, profiled | 5,271 (5,161 / 5,271 / 5,343) | 2,423 (2,445 / 2,423 / 2,369) |
| engine CPU ns/record, control | 5,285 | 2,384 |
| CV of CPU per record | 0.017 | 0.016 |
| throughput, records/s | 84,676 | 220,853 |
| worker on-CPU / run-queue / off-CPU | 0.44 / 0.002 / 0.56 | 0.52 / 0.001 / 0.47 |
| CPU s per profiled lifetime | about 26.5 on logical CPU 1, 0.4 to 0.6 on http-admin, under 0.3 elsewhere | about 31.5 on CPU 1 |
| user / system CPU s | 23.4 to 24.2 / 2.9 to 3.0 | 31.1 to 32.1 / 0.3 |
| flush wall s | 34.1 | 23.9 |
| upload wait s, flush wall not spent on flush-task CPU | 23.9 | 18.5 |
| admission closed s | 0.41 to 0.44 | 0.59 to 0.61 |
| peak RSS | 150 MB | 109 MB |

- **Worker load.** The worker is about half busy. The engine is not
  saturated, and the Python sender and the durable-ack pipeline bound the
  rate.
- **Profile overhead.** Measured as profiled CPU per record against control
  CPU per record, it is -0.3% for logs (per repetition -3.2 / -0.3 / +3.4%)
  and -0.1% for metrics (+2.6 / -1.0 / -0.1%). perf's cost is below the
  repetition noise. The throughput ratio says nothing about perf, because
  the producer bounds throughput. The first two logs controls ran at 56k
  records/s, not 110k, while the prebuilt input's pages were cold.

### Reconciliation (valid; spot family preferred where it measured the stage)

Attributed cost includes the allocator samples that the row's categories
called.

| Row | logs attributed | logs bench | ratio | metrics attributed | metrics bench | ratio |
| --- | --- | --- | --- | --- | --- | --- |
| conversion vs convert | 481 | 383 | 1.26 | 561 | 506 | 1.11 |
| extraction vs extract | 1,114 | 1,203 | 0.93 | 726 | 791 (pinned 1,368) | 0.92 (pinned 0.53) |
| admission+sort_seal_merge vs sort_seal+merge | 622 | 602 | 1.03 | 406 | 486 | 0.83 |
| encoding vs encode (zstd) | 1,744 | 1,633 | 1.07 | 309 | 226 | 1.37 |
| upload vs upload | 387 | 405 | 0.96 | 31 | 14 | 2.17, differs |
| engine_runtime vs otlp_noop pipeline | 728 | 1,738 | 0.42, differs | 376 | 312 | 1.20 |
| total vs noop pipeline + otlp_minio | 5,271 | 6,605 | 0.80 | 2,423 | 2,531 (pinned 3,305) | 0.96 (pinned 0.73) |

- **Task 3c memo.** The memo that made metrics extraction cheaper is
  visible exactly where the lead expected. Against the pinned family,
  metrics extraction is 0.53 times its cost and the metrics total 0.73
  times. The spot family at HEAD brings both to 0.92 and 0.96.
- **Logs engine_runtime differs.** The noop pipeline's reference cost of
  1,738 ns/record was measured with a producer building 1 KiB records on
  the fly, at a wall time of 121 us per record, which is an idling engine.
  The first attribution run on disk was throttled the same way. It measured
  6.7 us per record against 5.3 at load, with a runtime share of 18%
  against 12%. So the reference, not the attribution, is inflated.
- **Metrics upload differs.** It is 27 against 14 ns per record, on a
  stage that is 1.2% of the profile. The row's known-deviation note says
  the bench sends pre-encoded bytes in one object, while the engine streams
  multipart parts during encoding.
- **Joined stage metrics.** For each stage, the rows in `attribution.json`
  carry records/s/core, CPU ns/record, allocated bytes/record, peak RSS,
  peak live heap and workspace, and output bytes per input record, each with
  its own output representation. For example, logs encode allocates
  8,728 B/record, peaks at 160 MB RSS and outputs 755 B/record of
  Parquet bytes.

## Findings and concerns

1. **The RSS reconciliation fails for logs in all 3 repetitions.** The
   residual peaks at 48 to 65 MB against 33.5 MB, with 10 to 14 samples
   over it per repetition. Metrics stays under, at 24 to 27 MB. Its steady
   state is 20 to 35 MB. RSS rises about 70 MB over idle as 12.8 MB blocks
   turn over every 0.14 s, while the one-second pipeline heap gauge stays
   near 32 MB and misses the transient peaks. This is the Task 6 accounting
   question (merge and encode transients between collections). It is not
   caused by attribution. It blocks the logs baseline and makes
   `attribution.json` failed.
2. **Unwinding needed a relinked engine.** Rust 1.98 links with lld by
   default, and lld puts the executable segment 4 KiB above its file offset
   (0x1ca7d40 against 0x1ca8d40). perf 6.14's libdw unwinder derives the
   module base from the offset, so every Rust stack ended after one frame.
   The attribution profiles `target/release/df_engine-perf`, which is the
   same objects relinked with `-z separate-loadable-segments`, with no
   codegen change. Its provenance records the canonical binary's hash, and
   the preflight refuses a skewed engine. `target/release/df_engine` is
   unchanged, c479bc28.... Any later perf-based task needs the same relink.
3. **Harness defects found and fixed along the way.**
   - The oracle read-back paid an fsync per row on a real disk: eight
     minutes for 1.7M rows, now 0.26 s per 300k.
   - A disk-backed ledger throttled the sender to about 15k records/s.
     Ledgers now live on /tmp during a lifetime and are deleted after the
     read-back, with their hash kept.
   - The admin API serves the delta `flush.duration` cumulatively, so
     flush time is the difference of two readings.
   - perf's `fifo:` argument escaped the scrubber. It was fixed at the
     source and the committed files were corrected, with no measured value
     changed.
   - A stray engine from my first rehearsal was left on core 1 by an early
     exception. The lifetime code now closes it on every path, and I
     stopped the stray process by PID.
4. **Shared host.**
   - Another agent's `measure memory` runs held the lease twice. The first
     launch was refused in full and cleaned up. The committed run used
     `--option lease_wait_s=10800`.
   - A cargo LTO bench build from worktree agent-a3dc6950 invalidated one
     rehearsal. The committed run saw no build and passed every
     `no_concurrent_build` and `build_monitor_coverage` check.
   - I sent team-lead one note about the contention.
5. **Wall-clock cost.** The committed family took 20 minutes of lease time
   (10:21 to 10:41, including a lease wait of about 9 minutes before the
   first child). The brief estimated 10 to 20 minutes. Prebuilt inputs
   (6.5 GB) are reused only within one output directory.
6. **The spot family does not share the attribution's source revision.**
   The spot family was measured at 936ace03a, which was dirty only in the
   harness. The attribution ran at 5e87fff73. Only harness Python differs
   between them, not Rust sources, and the index records
   `spot_revision_matches: false` rather than claiming a match.

## Artifacts

`.measurement-artifacts/` holds the raw artifacts of every run. Each archive
excludes Parquet files and ledgers, and the attribution archives also
exclude prebuilt inputs.
- attribution-raw-20260923-1041.tgz: the committed run, including perf.data
  and perf-script.txt.
- stages-spot-raw-20260923-0925.tgz: the spot family.
- attribution-rehearsal-raw-20260923-0934.tgz and
  attribution-smoke-raw-20260923-0951.tgz: validation runs, never
  published.

The /tmp and /var/tmp outputs were deleted.

## Addendum after the lead's two messages

- **perf_event_paranoid.** The committed family ran with the value at 1. It
  is recorded as `preflight.perf_event_paranoid: 1` in `attribution.json`.
  From 3be4a776e on, the index and every repetition also record the value
  they ran under in `environment.perf_event_paranoid`. The committed family
  predates that field, and I did not re-measure it only to add the field.
  The preflight stays: after a reboot restores 4, it skips the family and
  marks acceptance incomplete.
- **Lease etiquette.** The default lease policy is unchanged. The committed
  run used the additive `--option lease_wait_s=...`, which waits inside
  `HostLease.acquire` before any traffic, rather than
  `tail --pid=<holder>`. Its build monitor saw no compiler in any measured
  window. The one rehearsal a compiler invalidated was discarded, never
  accepted. Harness edits are additive, apart from two bug fixes in shared
  code: the oracle's transactional load in `measurement._load_actual`, and
  `Producer(source=...)` with an unchanged default.

## Fix round 1

Review verdict was "Needs fixes". All five rulings are applied, and the
family was re-run once as f002 at 865b7dfc7, under `taskset -c 0-7,16-23`
with lease waiting. It supersedes f001 as `attribution.json`; f001 stays
archived as the child index `attribution-e137b294fdec.json`.

### Commits

| Commit | Subject |
| --- | --- |
| 3be4a776e | feat(series_parquet): record perf_event_paranoid in the attribution environment |
| 8ecfa8d9a | fix(series_parquet): bind attribution to the plan's stage agreement rule and a proven engine |
| 865b7dfc7 | feat(series_parquet): rerun attribution repetitions a concurrent build invalidated |
| 43fe81106 | fix(series_parquet): remove the attribution's default ledger directory with its run |
| 4a5d74bad | chore: re-attribute series parquet pipeline CPU costs under the binding rule |

### What changed

1. **Engine provenance (critical).** The family builds its engines itself,
   holding the host lease and before any measured window, from a Rust tree
   that must be clean. It runs the canonical `cargo build --release`, then
   `cargo rustc ... -- -C link-arg=-Wl,-z,separate-loadable-segments`, then
   the restore. For both binaries it records the revision, profile,
   features, allocator, `rustc -vV`, hash, segment layout and the exact
   flag difference. They are one engine only when their sorted
   `(size, demangled name)` function symbols from
   `nm --defined-only --size-sort -C` are identical. Any mismatch, a dirty
   tree, or an unwindable layout refuses the family with the reason. In
   f002, both binaries list 191,989 functions with digest 3e0cd91a....
   The profiled binary is bb9e4388..., the same file f001 profiled, so the
   proof covers the earlier evidence too.
2. **Binding reconciliation (critical).** The plan's Stage agreement row is
   now the hard check in every repetition and aggregate:
   `stage_agreement_error` (exclusive CPU plus the named residual against
   measured engine CPU, at most 10%) and `stage_agreement_unexplained`
   (named residual plus uncovered CPU, at most 20%). With exclusivity and
   `reference_family_verified` (the pinned family's hash), these are
   decided inside the aggregate, before the baseline policy runs, so a
   baseline is written only after every binding gate. The per-stage
   comparison is labelled `descriptive_stage_comparison` and
   `gating: false`, referenced to the pinned family, with the spot family
   beside it as supplementary. The unconditional spot preference is gone,
   including in sizing.
3. **tmpfs ledger (important).** The ledger directory's file system is read
   from `/proc/self/mountinfo` and recorded in the index and every
   repetition. A directory that is not tmpfs falls back to `/dev/shm`, or
   the family is refused. The ledgers always go in a run-owned
   subdirectory. f002 records tmpfs.
4. **Out-of-band rows (important).** A row outside 0.5 to 2 is `explained`
   only when the published spot family brings the same row into the band,
   with its index and hash cited. Otherwise it is marked `unexplained`.
5. **Minor.** The `/usr/bin` tool paths are kept, as ruled. New failure-path
   tests: the oracle load's rollback, and engine cleanup when a lifetime
   fails early.

A full re-run was needed, because fix 1 changes what a valid profiled binary
is. The first relaunch lost five of six repetitions to another agent's
cargo build. So a repetition whose `no_concurrent_build` failed is now kept
as `invalidated_children`, never aggregated. It is rerun under a new
ordinal once the host has been build-free for 60 s, at most three times, and
a repetition also waits for a quiet host before starting. In f002, logs r004
was invalidated by a rustc and rerun as r005 after a 60 s wait.

### Tests

The contract suite has 213 tests, all passing with 1 skip; that is 7 more
than before this round.

### f002 results

| | logs-1k-stable | metrics-mixed |
| --- | --- | --- |
| aggregate status | failed, `rss_reconciliation` only | passed, baseline `baseline-attribution-10b18890c3147099.json` |
| classified samples, 3 repetitions | 16,518 (6,048 / 5,329 / 5,141) | 14,818 (5,025 / 4,928 / 4,865) |
| engine CPU ns/record, median | 5,345 | 2,453 |
| stage agreement error, per repetition | 0.19 / 0.26 / 0.26% | 0.30 / 0.30 / 0.23% |
| unexplained CPU, per repetition | 1.6 / 1.7 / 1.5% | 0.4 / 0.4 / 0.4% |
| profile overhead, CPU per record | -1.9% | +0.1% |
| upload wait s / flush wall s | 24.7 / 35.2 | 14.5 / 18.7 |

Shares, pooled with 95% intervals:

| Category | logs-1k-stable | metrics-mixed |
| --- | --- | --- |
| encoding | 33.0% +-0.7 | 11.3% +-0.5 |
| extraction | 16.8% +-0.6 | 24.7% +-0.7 |
| allocator | 12.6% +-0.5 | 15.0% +-0.6 |
| engine_runtime | 11.9% +-0.5 | 13.4% +-0.5 |
| sort_seal_merge | 10.7% +-0.5 | 12.9% +-0.5 |
| conversion | 7.5% +-0.4 | 19.8% +-0.6 |
| upload | 5.9% +-0.4 | 1.2% +-0.2 |
| buffer | 0.3% +-0.1 | 1.5% +-0.2 |
| unknown | 1.4% +-0.2 | 0.1% +-0.1 |

Descriptive comparison against the pinned family, 14 rows:
- 12 rows are within the 0.5 to 2 band. That includes metrics extraction at
  0.57, and 0.98 against the supplementary spot family.
- Two rows are marked `unexplained`, with no published measurement bringing
  them into the band:
  - Logs engine_runtime is 0.42 against the noop pipeline's 1,738 ns per
    record. My earlier explanation, a starved engine in that reference,
    rested on a discarded run, so it is withdrawn rather than asserted.
  - Metrics upload is 2.27 against the pinned family and 2.54 against the
    spot family: 36 against 16 ns per record, on 1.2% of the profile.

### Status

`attribution.json` is `failed`, and mandatory acceptance is `failed`. The
binding reconciliation holds in every repetition, and every other hard gate
passes. The only failing gate is the logs `rss_reconciliation`. That is the
Task 6 accounting finding: RSS follows the transient block turnover that
the one-second heap gauge misses. It blocks the logs baseline, as before.

## Fix round 2

Two harness defects are fixed in one harness-only commit. No re-measurement
was done, because the committed f002 evidence is valid on the path it
exercised: r004 is listed as invalidated.

1. **Exhausted retries.** A fourth invalidated attempt used to be appended
   to the aggregated children. Now every invalidated attempt stays in
   `invalidated_children` and the repetition stays missing. The aggregate
   fails a new hard `repetitions_complete` gate that names the missing
   repetition, so no baseline is written. A workload with no valid
   repetition fails `workload_measured_<config>` in the index.
2. **Provenance time-of-check to time-of-use.** The tree check now covers
   tracked and untracked files under `rust/`. The source state, meaning
   cleanliness and revision, is re-checked after the lease is acquired and
   again after the builds. A change in either window refuses the engines,
   and a tree that changed while waiting is never built. The checks are
   recorded under `source_checks`.

Tests: the contract suite has 217 tests, all passing with 1 skip. The new
ones are:
- an orchestration test that drives `run_attribution` with a stubbed
  repetition runner through four invalidated attempts, and asserts that
  nothing invalidated is aggregated;
- a completeness test with a missing repetition;
- a tree that changes while waiting for the lease, and a revision that moves
  during the builds;
- an untracked `.rs` file in a scratch git repository.
