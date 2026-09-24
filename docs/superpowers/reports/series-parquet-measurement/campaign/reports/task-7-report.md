# Task 7 report: thirty-minute soaks and PR-tier soaks

Worktree `<repo>/.claude/worktrees/agent-a0a31511a3634cee8`,
branch `worktree-agent-a0a31511a3634cee8`, reset to 4437634dd at the start.

## Status (updated after the controller's band ruling)

- **All four soak cases pass** under the ruled band rule (d4cf05179):
  - `soak-buffered` r003 passed as measured;
  - both PR-tier soaks passed as measured;
  - `soak-strict` passes on re-judgement of **r002 as recorded**, with no rerun: its 5
    excursions fall inside the new upper edge (the larger jemalloc `resident` of this paired
    print and the previous paired print). The strict baseline
    `baseline-soak-strict-5cb03bef4ffc17fe.json` is written from r002 as recorded.
- r001 cannot be re-judged: the fix that keeps excursions came after it, so its two failing
  pairs were thinned out of the published file. It stays failed and is listed in the index
  under `rss_band_rejudgement.not_rejudged`.
- r002 is recorded in `soak-strict.json` as the diagnostic rerun that kept the excursions;
  r001 and r002 together are the result.
- Alloy compatibility trial: r294 failed on a harness fault (its read-back ignored the S3
  store); after the fix (8e9c0f18c) r295 passed.
- The CI step's PR-tier path (debug engine, `publish=false`) was run once as CI runs it:
  pass, wall time 163 s.

### The band ruling, carried out

1. **Rule.**
   - `measurement.RSS_BAND_RULE` and `band_upper_resident` (d4cf05179, corrected in b98165b25)
     apply to every family, through `allocator_band_residuals`.
   - The upper edge is the larger `resident` of this paired print and the previous paired
     print.
   - The live sampler also records `interval_resident_max_bytes`, the largest `resident` of the
     prints read in one poll, as a diagnostic only. d4cf05179 had wrongly taken it into the
     edge; the codex review found this, and b98165b25 removes it.
   - The lower edge, the tolerance and the persistent-negative rule are unchanged.
   - The rule and its reason are in the harness README ("Capacity", the RSS band paragraph)
     and in the rule text of each advanced index (`rss_band_rule`; capacity indexes also carry
     `capacity.rules.rss_band`).
   - Every family (capacity, Alloy, soak, PR-tier) now keeps each residual beyond half the
     tolerance with 3 pairs on each side (`observations.residual_excursions`).
2. **Contract tests** (in `test_measurement.CapacityContracts`):
   - `test_band_ignores_an_unpaired_interval_maximum` (codex regression): paired residents
     100 MiB, allocated flat, RSS 100 to 800 MiB and an unpaired 1 GiB print give residual
     700 MiB and fail.
   - `test_band_upper_edge_covers_the_release_after_a_purge`: after a 400 MB purge, an RSS
     read that still holds 300 MB of it lies inside max(previous, this) resident and passes;
     RSS 150 MB above both residents (tolerance 115 MB) fails.
   - `test_rejudge_band_check_from_kept_excursions`: a kept excursion is re-judged; a failed
     run without kept pairs is reported not re-judgeable, never passed.
   - `test_rejudge_band_index_writes_the_earned_baseline`: an advanced index records the
     change and the rule, keeps the old index as a child, leaves the run file unchanged and
     writes the baseline the run now earns.
   - The whole `test_measurement` suite passes (294 tests).
3. **Re-judgement from stored runs, no reruns.** `measure rejudge-band` advanced
   capacity-local, -minio, -rustfs, soak-strict and soak-buffered; each keeps its predecessor
   as a child. Runs on the band: 82 local, 101 MinIO, 38 RustFS, 5 soak-strict and 9
   soak-buffered.
   - After b98165b25 the same five indexes were re-judged again (eb17383d0). No verdict
     differs from the first re-judgement: no stored run recorded an
     interval maximum, and r002 passes through its adjacent pair. On the second pass r002 is
     compared with the baseline the first pass wrote.
   - **Verdict changes: one.** soak-strict r002, rss_reconciliation failed to passed, run
     status failed to passed. Its five residuals re-judge as follows:

     | recorded (MB) | re-judged (MB) |
     | --- | --- |
     | +130 | 0 |
     | +143 | 0 |
     | +146 | 0 |
     | +138 | 0 |
     | +234 | 0 |

   - **Not re-judgeable, still failed:**
     - soak-strict r001;
     - capacity r048 (local c1, +77 MB);
     - capacity r283 (8 KiB bodies, MinIO c1, +234 MB).
     - In all three the failing pairs were thinned out of the published samples, one pair in
       four for capacity trials and one per 10 s for r001.
     - Their search verdicts are unchanged: a residual-only failure already counted for the
       bracket.
   - Every trial that passed stays passed. A wider upper edge cannot add a positive residual,
     and the lower edge is unchanged. I also recomputed all 229 band runs over their stored
     pairs: none turns negative beyond the tolerance.
   - Runs on the pipeline-counter heap term (the superseded Task 5 r013-r033 and the
     stats-off control r088) are outside the band rule and unchanged.
4. **CI step, run as CI runs it** (`DF_ENGINE=$PWD/target/debug/df_engine`, `publish=false`,
   `SERIES_REQUIRE_DOCKER=1`, no taskset): 15 tests in 162.9 s, 13 passed and 2 long tests
   skipped, exit 0.

   | | strict | buffered |
   | --- | --- | --- |
   | engine profile | debug | debug |
   | run time | 77.7 s | 83.4 s |
   | outage | 3.94 s | 3.93 s |
   | records stored once | 130,000 | 130,000 |
   | residual range | -13.7 to +2.4 MB | -7.7 to +0.7 MB |
   | ack p99 | 4.54 s | 0.05 s |

   Every CI-required hard gate passed on both.

## Commits (on 4437634dd)

| commit | what |
| --- | --- |
| 4d27a9626 | soak harness: `soak.py`, `test_soak.py`, `measure soak` and the four registered cases, capacity extension hooks, the `buffered_default_window` search variant, the `mixed-1k-hot-churn1` workload (`Workload.churn_every`), `Producer` retry attempts and `send_paced`, Alloy RSS and a `no_enqueue_loss` check, README "Soak", the CI step |
| a5bc8b083 | the soak keeps every RSS residual beyond half the tolerance whole, with 3 allocator pairs on each side (r001 had thinned them away); PR-tier raw artifacts are archived |
| 4cc9edf77 | a soak index lists every run of each step |
| 71b5a612d | a soak index records why a step was run again (`--option purposes=...`) |
| 8e9c0f18c | fix: the Alloy trial reads back an S3 store (a harness fault that blocked the Alloy step on MinIO) |
| 9dee38948 | evidence: `soak-strict.json`, `soak-buffered.json`, the run files, the baselines and the bracket trials (21 files, about 21 MB) |
| d4cf05179 | band rule (controller ruling): upper edge is max `resident` over this print, prints read with it and the previous paired print; excursions kept in every family; `measure rejudge-band`; contract tests; README |
| abfbd01a6 | a re-judged run whose only failure was the band check is passed, so it writes its earned baseline; contract test over a published index |
| d7a21067f | evidence: the five advanced indexes (each keeping its predecessor as a child) and `baseline-soak-strict-5cb03bef4ffc17fe.json` |
| b98165b25 | codex High: the band's upper edge takes paired prints only; the unpaired interval maximum is a diagnostic; regression test; rule text and README |
| eb17383d0 | evidence: the five indexes re-judged by the corrected rule (no verdict differs) |

No product code was changed.

## Conditions

Both soaks share:
- one worker on core 1, MinIO on its own core, the engine under `taskset 0-7,16-23`;
- 8 sender processes on CPUs 8-15,24-31, 256 connections, 1000-record requests;
- workload `mixed-1k-hot-churn1`:
  - 80/20 logs/metric points, 1 KiB bodies;
  - 10k hot series;
  - every 100th record on a new series (deterministic 1 percent churn);
- the template generator and the aggregate oracle;
- jemalloc statistics printed every 64 MiB of allocation, and the allocator-band residual;
- the shipped 15 s window;
- a warm-up cohort (35 s), then a measured cohort of `ceil(rate * 1800 / 1000)` requests;
- `input_phase_s` is the measured cohort's active time; 1800.006 s strict, 1800.000 s buffered.

| soak | rate | slots | topology | series | churned series |
| --- | --- | --- | --- | --- | --- |
| strict r001/r002 | 130,899/s = 0.7 x 187,000 (committed `capacity-minio.json`, minio-c1 raised) | 4096 | strict | 10k hot | 2,402,010 (1 per 100 of 240.2M records) |
| buffered r003 | 72,800/s = 0.7 x 104,000 (bracket below) | 128 (shipped) | durable buffer, WAL on the host disk in the run dir | 10k hot | 1,335,880 |

**Buffered bracket** (one worker, MinIO, 128 slots, 15 s window, same workload):
- search rules as in "Capacity", starting from a 64k floor;
- trials: 64k sustainable (r287), 128k unsustainable (r288, backlog +8.1k/s), 96k sustainable (r289), 112k unsustainable (r290, last-30 s durable rate 108.9k), 104k sustainable 3 of 3 (r291-r293);
- **sustainable 104,000/s, unsustainable 112,000/s**, bracket width 7.7 percent;
- CPU 5.6-5.9 us/record.

Numbering note: `soak-buffered` is r003 because soak ordinals are one counter shared by both soak cases.

## soak-strict (r001 and r002)

**Rates** (identical across the two runs to within 0.1 percent):
- offered 130,899/s;
- acknowledged 130,901/s (r001) and 130,899/s (r002) over the full run;
- committed (unique, read back) the same;
- final five minutes 130,917/s and 130,903/s;
- input 115.5 MB/s; objects 79.7 MB/s (compression 0.69);
- 2,448 objects of 59.7 MB average, 146.2 GB in total;
- ack latency p50 3.55 s, p99 5.58 s;
- engine CPU 4,820-4,838 ns/record, occupancy 0.63 of the core.

Every block rotated on `max_block_bytes`:
- flushes by reason: 480 on bytes and 120 on time over the interval;
- mean flush 1.51 s, max 2.40 s;
- admission closed 113-115 s of 1800 s, 600 closures: the next rotation waits for the previous flush, about 0.19 s each.

**Per-minute acknowledged rate and RSS (r002).** Minutes 13 and 14 read 138.7k and 123.1k because one flush's acknowledgement fell on the minute boundary; their average is 130.9k. RSS max ranged 1,127-1,248 MB and RSS median 961-1,073 MB, with no trend.

```text
min acked/s  rssMax  rssMed  accMax allocMax fds cache   inFlightMax(MB)
 0  130883   1146    1002    851    983     301 132990  577
 5  130867   1180    1029    881   1022     301 200000  578
10  130900   1127     987    870    942     301 200000  578
15  130933   1248    1039    861    993     301 200000  579
20  130917   1176     982    863    982     301 200000  579
25  130917   1230    1048    858    991     301 200000  590
29  130917   1179    1051    869   1008     301 200000  578
```

The full 30-row tables for both soaks are in each run file under `soak.per_minute`. The per-second rows are `samples`.

**Memory at the highest fill** (r002, input second 79; r001 is similar):
- ACTIVE 304 MB and FLUSHING 461 MB (fill 765 MB);
- accounted 989 MB against a budget of 1,638 MB (ratio 0.60), with the flush workspace at 191 MB;
- RSS 1,101 MB;
- jemalloc allocated 402 MB and resident 1,123 MB, from the print 12 ms earlier;
- receiver in-flight 340 MB (385 requests, sent and not yet answered), which sits outside the exporter's accounting;
- accounted peak over the run 0.989 GB, RSS peak 1.31 GB (r002) and 1.36 GB (r001).

**Drift over 30 minutes** (r002; r001 in brackets):

| series | first-5-min median | last-5-min median | ratio | slope |
| --- | --- | --- | --- | --- |
| RSS | 1,073 MB | 1,087 MB | 1.013 (1.013) | +1.9 kB/s |
| FDs | 301 | 301 | 1.0 | 0; max 301 (302) |
| series cache | 200,000 | 200,000 | 1.0 | at its cap from minute 1 |
| allocated - accounted | +103 MB | +106 MB | - | -0.019 (r001) / +0.020 (r002) bytes per values row over 240.2M rows |

- Accounted minus allocated: the capacity view has p50 +102 MB and a per-instant range of -648 MB to +503 MB. That swing is the flush allocating and freeing about 500 MB within tens of ms, not growth. **No leak signal.**
- MinIO RSS grew from 551 to 687 MB (+25 percent) on the store side. Producer RSS stayed flat at 3.7 GB.

**Backlog at stop and drain:**
- 687,000-689,000 records owed; that is one window's tail block;
- drained in 10.3-10.6 s, at about 65-67k records/s;
- no acknowledged request is missing from a completed object;
- the drain proof passed.

**Oracle:** 240,201 acknowledged requests (240.2M records), 0 missing, 0 duplicated, 0 foreign, 0 descriptor or file problems, and ClickHouse agrees with DuckDB. Moving the objects out of the store (download plus delete) took 278-312 s; the oracle took 119-125 s.

**Listing during input:** every 10 s, at most 0.27 s per listing, listing lag p50 5.1 s and max 10.1 s.

## The strict RSS residual excursions (Task 12 input)

- Tolerance: `max(32 MiB, 0.10 x peak RSS)`, which comes to 136 MB (r001) and 134 MB (r002).
- r001: excursions up to +205 MB. It published only one residual per 10 s, so its two excursions are not in its file; a5bc8b083 keeps them from r002 on.
- r002: 29 residuals beyond half the tolerance were kept, with the pairs around them (`observations.residual_excursions`).

**The 13 positive ones, 5 beyond the tolerance** (+130, +143, +146, +138, +234 MB), all show the same pattern:
- **When:** each falls in the input second in which a FLUSHING block completes. In that second the flushing bytes go from 439/457 MB to 0 or about 67 MB, about 468-492k records are acknowledged together, and 270-284 MB of objects complete.
- **Relative to the jemalloc prints:**
  - each is the first print after the release;
  - jemalloc resident has dropped 358-460 MB from the previous print, 10-31 ms earlier;
  - the smaps rollup, read at most 5 ms after the harness saw the print, still holds 122-234 MB of those pages as anonymous memory above resident.
- **How long:** less than the gap to the next print. The next pair, 11-49 ms later, reads anonymous memory 8-118 MB *below* resident and well above allocated, which is inside the band. No excursion survives into a second pair.

**The 16 negative ones** (-65 to -111 MB, none beyond the tolerance) come in bursts when a flush starts. Allocated grows faster than RSS there, because freshly reserved memory is not yet touched.

**Would synchronous pairing close them?** No. The RSS read already follows the print by at most 5 ms, and reading at the print instant would still catch the kernel mid-release:
- jemalloc has stopped counting the extent;
- the page-table teardown of about 400 MB (madvise or munmap of about 100k pages) has not finished.

What would close all 5, using data that was already recorded: bounding each pair's anonymous reading by the reading at the next print, or reading RSS after the release settles.

That is a change to a frozen rule, so it is the controller's call; I made no change. Task 5 finding 8 (+77 MB in r048, +234 MB in r283) is the same event.

The buffered soak had 3 flagged and 0 beyond the tolerance. Its tolerance is 175 MB (peak RSS 1.74 GB) for releases of the same size, so the strict soak's lower RSS makes the same event fail there.

## soak-buffered (r003, passed)

**Rates:**
- offered 72,800/s; WAL-acknowledged 72,799/s; committed 72,762/s full run;
- final five minutes 72,800 and 72,757/s;
- input 64.2 MB/s, objects 44.3 MB/s;
- 1,468 objects of 55.4 MB average, 81.3 GB in total;
- ack latency p50 0.023 s, p99 0.87 s;
- CPU 5,849 ns/record, occupancy 0.43;
- flushes 240 on bytes and 120 on time; admission closed 3.6 s.

**Per minute:**
- acknowledged 72,783-72,817/s in every minute;
- committed 71,900-73,633/s, moving with the flush phase;
- RSS max 1,532-1,663 MB, median 1,119-1,275 MB;
- accounted max 728-881 MB;
- WAL max 519-590 MB;
- FDs 303-304;
- series cache reached its 200k cap at minute 3.

**Memory at the highest fill** (input second 197):
- ACTIVE 152 MB and FLUSHING 479 MB;
- accounted 812 MB against 1,640 MB (ratio 0.50);
- RSS 1,217 MB;
- jemalloc allocated 198 MB and resident 753 MB;
- WAL 558 MB (buffer gauge), 494 MB on disk;
- receiver in-flight about 1 MB, because the buffer acknowledges at once;
- peaks: WAL 619 MB against its 1 GiB cap, accounted 0.92 GB, RSS 1.74 GB.

**Drift:**
- RSS medians 1,237 to 1,280 MB (ratio 1.035);
- FDs flat;
- WAL gauge 342 to 347 MB (ratio 1.014, slope +0.5 kB/s);
- WAL on disk 343 to 389 MB (ratio 1.13, max 646 MB);
- allocated minus accounted +34 to +36 MB, and -0.066 bytes per values row over 133.6M rows;
- MinIO RSS 732 to 885 MB.

**Buffer:** 130,971 bundles acknowledged, 0 deferred, 0 permanently rejected, 0 ingest failures, 0 retention loss. In-flight peaked at 637 bundles; queued items grew +6.5/s, which is flat.

**Backlog at stop:**
- acknowledgement backlog: 1 request, drained in 8 ms;
- storage backlog: 454,000 records, drained in 11.0 s at 41k/s.

**Freshness** (WAL acknowledgement to object visible): p50 4.55 s, p95 7.77 s, p99 8.29 s, max 16.5 s. That fits the 15 s window.

**Oracle:** every one of 133,588 acknowledged requests is stored exactly once, with no descriptor or reader problems. Reconciliation range -105 to +95 MB against a 175 MB tolerance.

## PR-tier soaks

Both use 65 s at 20 requests/s of 100 records, every tenth request a metrics request, on MinIO with 1 s windows. The overrides are `max_block_bytes 640KiB`, `max_requests_per_block 6` and `flush_retry_deadline 3s`, with a store `retry_timeout` of 2 s. The outage is `DockerStore.stop()` once an object exists and ACTIVE is nonempty, then `DockerStore.recover()` once a flush has failed and 3 s have passed.

| | strict r001 | buffered r001 |
| --- | --- | --- |
| elapsed (run, controls included) | 78.3 s | 76.2 s |
| outage (stop to recover) | 3.89 s | 3.98 s |
| flushes by reason | bytes 141, requests 59, time 63 | bytes 141, requests 58, time 63 |
| failures | 1 deadline flush failure, 5 storage NACKs, 5 retryable producer attempts resent | 1 deadline flush failure, 5 storage NACKs to the buffer, 5 buffer retries scheduled |
| delivery | 130,000 records, each stored once | 130,000 records, each stored once |
| oldest unacked age max | 4.64 s | 4.88 s |
| RSS peak | 90 MB | 104 MB |
| accounted peak | 1.6 MB | 1.4 MB |
| caps, residual | pass | pass |

**CI budget:** `python3 -m unittest crates.validation.tests.series_parquet.test_soak -v` ran in **154 s** with the release engine: 13 passed, the 2 long tests skipped. That is inside the five-minute added-suite budget. With the debug engine, as the workflow runs it, the same module took 163 s (see Status).

## Alloy compatibility (r295)

- Setup: MinIO c1, strict, shipped 128 slots, 1 s window, receiver decoding limit 16 MiB; Alloy on its shipped batching and retry config, fed 20,000 lines/s.
- Results:
  - durable 20,000/s; Alloy sent 19,904/s;
  - 75 exports, all OK; every request was 20,000 records;
  - ack p50 0.50 s, p99 0.52 s;
  - enqueue failures 0 (the new `no_enqueue_loss` check passed);
  - Alloy RSS 331, 350 and 346 MB at start, middle and end;
  - read-back passed.
- r294, the same trial before the fix, stays in the index as failed, with the reason.

## Findings for Task 12

1. **RSS band pairing race at block release.** Ruled and resolved by the band rule
   (d4cf05179): kernel release lag after a large jemalloc purge, not unexplained memory.
   - Remaining to note: r001, r048 and r283 cannot be re-judged from what they published.
   - From now on every family keeps its excursions whole, so this cannot recur.
2. **Receiver in-flight memory outside the budget, at real fill.**
   - Strict at 131k/s holds 340 MB of requests (385 of them) beside 0.99 GB accounted at the highest fill, and up to 590-609 MB per minute.
   - RSS peak is 1.31-1.36 GB, while the budget is 1.64 GB for the exporter alone.
   - This quantifies Task 5 finding 2 at the 15 s window.
3. **Admission stalls at block rotation** (sizing note, not a defect). With 15 s windows at 131k/s, blocks rotate on bytes every 3 s. Admission is closed 6.3 percent of the time (600 closures of about 0.19 s each), and ack p99 is 5.6 s.
4. **The accounted-versus-allocated instant difference swings about 1.1 GB around each flush** (-648 to +503 MB), while its median (+0.1 GB) does not grow over 240M rows. A per-second "accounted minus allocated" is not a leak detector; the slope per values row is (0.02 bytes per row).
5. **MinIO RSS grows 25 percent over 30 minutes** in both soaks (store side). This is an observation for the deployment sizing notes, not an engine defect.
6. **Harness fault, fixed:** the Alloy trial read back only a local store (8e9c0f18c).

## Evidence

- Indexes:
  - `docs/superpowers/reports/series-parquet-measurement/soak-strict.json` (status failed);
  - `soak-buffered.json` (passed);
  - each one's earlier index is kept as an immutable child (`soak-strict-d319525e9c60.json`, `soak-buffered-ffa43e5ea2e8.json`).
- Run files:
  - `soak-strict-strict-minio-c1-w15-r001.json` and `-r002.json`;
  - `soak-buffered-buffered-minio-c1-w15-r003.json`;
  - `pr-soak-strict-strict-minio-c1-w1-r001.json`, `pr-soak-buffered-buffered-minio-c1-w1-r001.json`;
  - `capacity-alloy-file-strict-minio-c1-w1-r294.json` and `-r295.json`;
  - `capacity-mixed-1k-hot-churn1-buffered-minio-c1-w15-r287..r293.json`.
- Baselines: `baseline-soak-strict-5cb03bef4ffc17fe.json` (r002 re-judged), `baseline-soak-buffered-0cdc375094eee478.json`, `baseline-pr-soak-strict-f7af59b2dd635790.json`, `baseline-pr-soak-buffered-9c59b3479f127bd2.json`.
- Raw logs, configs, sender files and PR ledgers (no Parquet): `<repo>/.measurement-artifacts/soak/<run_id>.tgz` (14 archives, about 59 MB).
- Family state: `/var/tmp/series-soak/soak-state.json`. Parquet is deleted; the directory is about 0.5 MB of JSON plus leftover sender and alloy directories.
- Logs: `<scratchpad>/t7/` (`strict.log`, `family.log`, `alloy2.log`, `publish.log`, `unittest.log`, `perminute.txt`).

## Deviations from the brief, and concerns

- **Metric names.** The brief's `missing_acked_ids` and similar fail the harness's enforced unit-suffix rule, so they are `missing_acked_records`, `missing_intended_records`, `descriptor_violations_count`, `reader_disagreements_count`, `permanent_rejections_count` and `buffer_loss_records`.
- **Compared metrics.** The RSS slope is recorded but not compared: near zero its relative change is noise. The last-to-first median ratio (compared) and the RSS peak cover RSS growth.
- **New module.** The soak lives in a new `soak.py`, like `capacity.py`. `capacity.py`, `measurement.py` and `alloy_capacity.py` got additive hooks.
- **Heap dumps.** Deferred to Task 12, as the amendment says.
- **Receiver in-flight bytes.** Derived from the producer's sent-and-unanswered requests, because the receiver publishes no such gauge.
- **Red-first step skipped.** The analysis tests were written in the same step as `soak_checks`, not run red first.
- **CI mode.** `prepare_build(require_release=False)` lets a `publish=false` PR-tier run use
  the debug engine. It has now been run as CI runs it (163 s, pass). It still has to pass
  every hard gate except the core floor.
- **Disk.** A strict soak needs about 150 GB in the store at peak. Objects are moved out one at a time for the oracle, never held twice. 359 GB was free at the end.
