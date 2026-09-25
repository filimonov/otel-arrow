# Task 10 report: graceful restart and hard kill of the series_parquet engine

## Status

DONE_WITH_CONCERNS.

- The full matrix ran: 3 process cases x 2 topologies x 2 stores = 12 cells, on real MinIO and RustFS behind the Task 8/9 fault rig, with the containerized release engine built at 598cdcde6.
- 9 cells pass. 3 fail one hard check each; all three are product findings for Task 12, not harness expectations, so they were not rerun:
  - `graceful_restart-buffered-minio` and `graceful_restart-buffered-rustfs`: `replay_only_eligible`. The durable buffer redelivered bundles the exporter had already written during the graceful shutdown (10,200 and 2,600 records stored twice). This is the campaign's known replay-after-graceful-restart finding, reproduced on both stores, with its mechanism (T10-F1).
  - `kill_upload-buffered-rustfs` (r002): `duplicates_explained`. Two requests acknowledged by the log 10 and 60 ms before the second SIGKILL were each stored twice by the next engine, after the buffer's WAL replay (T10-F2, new).
- Two cells (both `kill_upload` on RustFS) were rerun once for recorded harness bugs; see "Reruns".
- Held in all 12 cells:
  - no acknowledged record missing: 0 missing, 0 unexpected, 0 corrupt;
  - every sent request acknowledged;
  - descriptor coverage and agreement between DuckDB and clickhouse-local;
  - bounded resources and the RSS band;
  - a new boot id per engine, with the same cores and (buffered) the same buffer directory;
  - no permanent rejection and no producer-local timeout;
  - orphaned uploads exactly the ones the kills left open.
- F1 (a phase-2 multipart failure leaks an upload with no signal) did not appear. `flush.abort_failures` was 0 everywhere and there was no unexpected incomplete upload.

Worktree `<repo>/.claude/worktrees/agent-ae33a2d38c949eff5`, branch `worktree-agent-ae33a2d38c949eff5`. It was reset to 598cdcde6 at the start. Not pushed.

## Commits

| commit | what |
| --- | --- |
| 1f8650941 | `chore(series-parquet): process restart and hard-kill fault cases`. It adds the process family to `faults.py`:<br>- `ProcessCase`, `kill_engine`, `stop_engine`, `restart_engine`, `replay_analysis`, `ledger_cohorts`;<br>- the `upload_throttle` fault;<br>- `PROCESS_CHECKS`.<br>`failure_experiment`/`settle_fault` now take the case's own verdicts and numbers, so S3 cases are unchanged. `measure.buffer_retained` is factored out of `record_restart`; `--family process`; contract tests; README "Process restart and hard kill". |
| e7bbd87fc | `fix(series-parquet): a store may commit a cut-off PUT; a resent buffered request explains its duplicate`. Two harness bugs the first matrix exposed; see "Reruns". |
| e511becc0 | `chore: measure series restart and hard-kill recovery`. `failure-process.json` and its tree: 14 run files (12 current plus 2 superseded RustFS r001), 9 baselines and 1 child index, 4.8 MB, scrubbed. |

No product code was changed.

## How the cases run

- Rig, engine and producer are the S3 family's (Task 9):
  - one worker on core 1, the harness under `taskset -c 0-7,16-23`, producers on 8-15,24-31;
  - 5 s windows, `upload.part_bytes` 5 MiB, so each logs values file (about 6.8 MB) is a two-part multipart upload;
  - flush deadline 60 s and abort timeout 5 s, so the partition lateness bound L = 5 + 2 x (60 + 5) = 135 s, the admin shutdown deadline L + 15 = 150 s and the cleanup cutoff 150 + 5 + 1 = 156 s;
  - 20 requests/s of 100 one-KiB records, every tenth request a metrics request.
- The producer's gRPC channel and its SQLite ledger outlive every engine:
  - the channel reconnects to the next engine on the launcher's fixed port within 1 s;
  - every refused request is resent with its original bytes, and the ledger refuses different bytes;
  - the ledger is never rebuilt from the output being checked.
- Restart (`restart_engine`): a new `Engine` from the previous one's launch options, on the same launcher, cores, store and (buffered) retained buffer directory. It records:
  - the old and new PID, container id, cores and graph;
  - the boot id, from each engine's `series_parquet.start` event;
  - the buffer inventory before and after.
- SIGKILL (`kill_engine`):
  - a container engine gets `docker kill --signal KILL <container id>`, its exit code comes from `docker inspect` (137), and the docker client is never signalled;
  - a local engine gets `os.kill(pid, SIGKILL)`.
  - Exits are observed with `poll`/`inspect` under deadlines (10 s for a kill, 30 s after the admin call for a graceful stop).
- Graceful stop (`stop_engine`): `POST /api/v1/groups/shutdown?wait=true&timeout_secs=150`, i.e. L + 15 s, then the process's own exit.
- Each signal is an event (`observations.fault.process.events`) recording:
  - the gate evidence;
  - the ledger's acknowledged and pending requests at the signal and at the exit;
  - the exit;
  - the store's direct listing (keys and ETags) and its incomplete uploads (with stored part bytes) at the exit;
  - objects that appeared between the exit and the restart (`late_objects`);
  - buffer bytes on disk;
  - the restart timeline.
- States: `input_started`, `baseline`, `gate_N`, `signalled_N`, `exited_N`, `restarted_N`, `resumed` (a values file of the new boot plus a producer ack after the restart), 20 s of acknowledged input, `input_stopped`, `drained`. `kill_upload` adds `armed_N` and `fault_removed_N`.
- Gates are confirmed on a fresh synchronous sample just before the signal. Before gating, the lifetime must answer 3 collection epochs.
  - A gate the fresh sample no longer shows is recorded in `gate_discards` and never claimed.
  - A gate not confirmed within 60 s (120 s for `kill_upload`) fails `fault_observed`.
  - Discards seen: 16 in `graceful_restart-buffered-rustfs`, 1 in `kill_upload-buffered-rustfs`, 0 elsewhere.
- The cases:
  - `graceful_restart`:
    - The gate: ACTIVE nonempty, a request sent in the last second, and work to drain (strict: unacknowledged requests; buffered: bundles in flight to the exporter).
    - Then the admin shutdown and a restart on the same buffer and cores, and new request ids continue.
  - `kill_active`:
    - The gate: ACTIVE nonempty, FLUSHING and the sealed slot empty, at least 1 s into the aligned window and 1.5 s before its end.
    - The selected cohort is at least 5 requests first sent from 0.25 s after the window's boundary (strict: not yet acknowledged; buffered: acknowledged by the log).
    - After the kill, the kill instant must lie inside that window and no cohort record may be in the store at the exit.
  - `kill_upload`:
    1. The values route is throttled to 512 KB/s (Toxiproxy upstream bandwidth).
    2. Kill 1 fires when the store lists a values multipart upload of the current boot with part bytes the store itself lists (list_parts; NGINX's logged bytes are evidence only), no completion is logged and FLUSHING is nonzero.
    3. The toxic is removed, the general route is throttled to 1 KB/s, and engine 2 starts.
    4. Kill 2 fires when engine 2's first flush has held FLUSHING for 1 s with no request of its boot finished. That flush's logs series PUT, a single PUT of about 8 KiB, is on the wire.
    5. The toxic is removed and engine 3 runs.
    - The caught upload must stay incomplete and its key never completed.
    - NGINX must log the series PUT cut off by kill 2 (non-2xx, started before the kill, ended at or after it).
    - Incomplete upload ids are recorded apart from completed objects. After the evidence is kept, the test aborts every incomplete upload itself (`orphan_cleanup`, clean in all cells).
- Checks: all of `FAULT_CHECKS` (Task 9) plus:
  - `new_boot_id`;
  - `restart_same_cores_and_buffer`;
  - `prior_acks_durable`: strict, every record acknowledged before an exit was in the store at that exit; both topologies, in the store at the end;
  - `replay_only_eligible`: a record stored at an exit and again after the restart is eligible only when its request was unacknowledged at the exit, or after a buffered SIGKILL;
  - `no_permanent_rejection`;
  - `retry_bytes_identical`.
- Also:
  - `orphaned_uploads_expected` requires every upload a killed engine left open to be still listed under its key, allows other uploads only up to the reported abort failures, and requires the test's cleanup to leave none (`orphan_verdict`);
  - buffered `duplicates_explained` accepts a restart replay, a request the producer resent after a kill cut off its acknowledgement, or a failed-block copy.
- `replay_analysis` computes exact pre/post multiplicity per record id. Pre counts rows in files listed at the exit; post counts rows in the final store. It reports them per cohort (acked, pending, later) as `pre->post` histograms.

## Per-case results

Times are in seconds. In the tables:

- "exit->ready" runs from the observed exit to every worker answering a collection, and includes the harness's listing and inventory. The engine itself went from launch to `readyz` in 0.16-0.20 s in every restart.
- "first ack" and "first values file" are measured from the restart.

| cell | run | status | failed check | signal->exit | exit code | exit->ready | first ack | first values file | final drain | acked/pending at signal | offered = acked records | stored | multiplicity | replayed | orphans (expected) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| graceful strict minio | r001 | passed | - | 5.00 | 0 | 1.31 | 3.73 | 4.12 | 2.21 | 199/102 | 91300 | 91300 | 1: all | 0 | 0 (0) |
| graceful strict rustfs | r001 | passed | - | 5.01 | 0 | 1.30 | 3.73 | 4.51 | 2.33 | 199/103 | 91200 | 91200 | 1: all | 0 | 0 (0) |
| graceful buffered minio | r001 | failed | replay_only_eligible | 0.15 | 0 | 1.24 | 0.05 | 4.09 | 4.94 | 301/0 | 81400 | 91600 | 1: 71200, 2: 10200 | 10200 | 0 (0) |
| graceful buffered rustfs | r001 | failed | replay_only_eligible | 0.15 | 0 | 1.25 | 0.03 | 2.44 | 5.10 | 325/0 | 81600 | 84200 | 1: 79000, 2: 2600 | 2600 | 0 (0) |
| kill_active strict minio | r001 | passed | - | 0.06 | 137 | 1.29 | 2.68 | 3.50 | 2.32 | 300/21 | 81200 | 81200 | 1: all | 0 | 0 (0) |
| kill_active strict rustfs | r001 | passed | - | 0.07 | 137 | 1.27 | 2.73 | 3.11 | 2.27 | 299/21 | 81200 | 81200 | 1: all | 0 | 0 (0) |
| kill_active buffered minio | r001 | passed | - | 0.06 | 137 | 1.24 | 0.04 | 2.64 | 5.03 | 326/0 | 81500 | 81500 | 1: all | 0 | 0 (0) |
| kill_active buffered rustfs | r001 | passed | - | 0.07 | 137 | 1.24 | 0.01 | 3.58 | 4.55 | 320/0 | 81800 | 81800 | 1: all | 0 | 0 (0) |
| kill_upload strict minio | r001 | passed | - | 0.07 / 0.08 | 137 / 137 | 1.47 / 1.46 | 0.83 | 1.36 | 2.85 | 299/128 | 102900 | 102900 | 1: all | 0 | 1 (1) |
| kill_upload strict rustfs | r002 | passed | - | 0.07 / 0.07 | 137 / 137 | 1.48 / 1.47 | 0.87 | 0.97 | 2.15 | 297/128 | 96100 | 96100 | 1: all | 0 | 1 (1) |
| kill_upload buffered minio | r001 | passed | - | 0.07 / 0.06 | 137 / 137 | 1.22 / 1.22 | 0.01 | 3.28 | 4.83 | 462/0 | 102100 | 102100 | 1: all | 0 | 1 (1) |
| kill_upload buffered rustfs | r002 | failed | duplicates_explained | 0.07 / 0.06 | 137 / 137 | 1.23 / 1.23 | 0.05 | 3.44 | 4.73 | 462/0 | 102500 | 102800 | 1: 102200, 2: 300 | 0 | 1 (1) |

What the producer saw, and resources:

| cell | producer attempts started between signal and restart, by code | resent requests | local timeouts | peak RSS MB | accounted MB | buffer peak MB | buffer on disk at restart MB |
|---|---|---|---|---|---|---|---|
| graceful strict minio / rustfs | OK 126, UNAVAILABLE 600 / 620 | 105 / 107 | 0 | 116 / 117 | 15 | - | - |
| graceful buffered minio / rustfs | OK 28, UNAVAILABLE 14 / 12 | 12 / 11 | 0 | 144 / 121 | 22 / 27 | 73 | 40 / 35 |
| kill_active strict minio / rustfs | OK 48, UNAVAILABLE 22 / 6 | 32 / 27 | 0 | 111 / 113 | 11 / 10 | - | - |
| kill_active buffered minio / rustfs | OK 26-27, UNAVAILABLE 10 / 5 | 10 / 5 | 0 | 121 | 27 / 11 | 73 / 72 | 35 / 34 |
| kill_upload strict minio / rustfs | kill 1: UNAVAILABLE 128; kill 2: OK 128 | 128 | 0 | 118 / 117 | 30 | - | - |
| kill_upload buffered minio / rustfs | kill 1: OK 26, UNAVAILABLE 12; kill 2: OK 26, UNAVAILABLE 5-6 | 16 / 17 | 0 | 170 / 176 | 34-35 | 88-89 | 62 then 75-76 |

The refusals were all UNAVAILABLE, never a storage nack and never a permanent refusal:

- "failed to connect to all addresses ... Socket closed" or "Connection reset by peer" while no engine listened;
- "Stream removed (Socket closed)" for requests in flight at a kill.

Exporter nacks: none in any engine's last sample.

### graceful_restart

- Strict (both stores). Timeline:
  1. Shutdown requested with 102-103 requests pending.
  2. The receiver immediately stopped taking connections (`otlp.receiver.drain_ingress`), and new sends got UNAVAILABLE "Socket closed".
  3. The exporter kept its normal cycle: the ACTIVE block sealed at the next window boundary and flushed, and every pending request was acknowledged by the old engine (acked 199 at the signal, 301-302 at the exit).
  4. Only then did the exporter receive Shutdown: `shutdown.complete accepted=312 acked=312 nacked=0`, duration 2 us.
  - Admin returned after 4.95 s and the process exited 0 at 5.00-5.01 s.
- Drain bound: exit after 5.0 s (admin returned after 4.95 s) against the 150 s admin shutdown deadline and the 156 s cleanup cutoff (deadline + abort_timeout + 1 s for held-request decisions and exit). The buffered stops exited after 0.15 s. The drain waits for the window's natural end: up to one window plus a flush, so about 15 s at the shipped 15 s window.
- Delivery and producer:
  - The 100 requests refused during the drain and the restart were resent to the new engine and acknowledged.
  - Every record was stored once. Every record acknowledged before the exit was in the store at the exit (`1->1`: 30,100 and 30,200).
- Restart: exit to workers ready about 1.3 s; the first ack came after the first window of the new boot (3.7 s).
- The receiver outliving the exporter (the umbrella's "Task 10 observes" item): in strict mode the receiver stops accepting at the shutdown request and holds its in-flight requests until the exporter acknowledges them. No request was accepted and then dropped.
- Buffered (both stores). Timeline:
  1. The buffer drained 1 bundle, shut its engine down and completed within 23 ms.
  2. The exporter then received Shutdown, flushed its ACTIVE block (about 10 MB, tens of bundles in flight from the buffer; 90 in the MinIO dev run) in 11-20 ms, and acknowledged it: `accepted=324 acked=324 nacked=1`.
  3. The process exited 0 after 0.15 s.
  - After the restart the buffer replayed those bundles, so 10,200 (MinIO) and 2,600 (RustFS) records that were in the store at the exit were stored a second time (`acked 1->2`).
  - One bundle nacked at shutdown (100 records, acknowledged by the log, not in the store at the exit) was delivered once after the restart (`acked 0->1`: 100).
  - Finding T10-F1 below.

### kill_active

- Strict (both stores):
  - The kill landed 1.3 s into the window, with 16 selected cohort requests (21 pending in all), ACTIVE 2.4 MB, FLUSHING 0.
  - The engine exited 137 at 0.06-0.07 s. No cohort record was in the store at the exit, and none was stored by the killed engine.
  - The producer got UNAVAILABLE for the requests cut off and resent 27-32 requests. The new engine stored each once (`pending 0->1`: 2,200 and 2,300 records).
  - Acknowledged history: 29,900 and 30,000 records, `1->1`.
- Buffered (both stores):
  - The cohort (21 and 15 requests) was acknowledged by the log before the kill.
  - At the exit, 2,700 and 2,200 log-acknowledged records were not in the store; the retained buffer replayed them after the restart and each was stored once (`acked 0->1`).
  - Nothing already stored was stored again. First ack after the restart 0.01-0.04 s: the log acknowledges at once.

### kill_upload

- Kill 1 (all four cells):
  - The throttled logs values upload of block 4 or 5 had part 2 (1,538,968-1,540,930 bytes) stored and part 1 (5,243,644 bytes) on the wire.
  - FLUSHING was 10.7 MB, and the kill came 3.1 s into the upload.
  - NGINX logged the cut-off UploadPart as 499 after 3.08-3.12 s.
  - The upload stayed incomplete through both restarts, and its key was never completed: false complete-file accounting was not seen.
  - It was the only incomplete upload at the end; the test aborted it and the listing came back empty.
- Kill 2 (all four cells):
  - Engine 2's first flush held its logs series PUT for 1.0-2.1 s, and the kill cut it off: NGINX 499, request_length 8,200 (the whole request), after 1.48-2.78 s.
  - MinIO never stored that object.
  - RustFS stored it within about 1.4 s after the kill, before the restart (`late_objects` 1, `completed_in_store` true). That makes it a late object of the dead boot: a series file of 100 descriptors with no values, harmless to the readers (observation T10-O1).
- Strict:
  - 128 requests (the in-flight cap) were pending at kill 1; the throttle had stalled acknowledgements.
  - All 128 were refused, resent to engine 2, cut off again, and acknowledged by engine 3.
  - Each record was stored once (`pending 0->1`: 12,800).
- Buffered:
  - At the kills, 16,300-16,400 and 22,900-23,200 log-acknowledged records were not in the store; engines 2 and 3 replayed them.
  - MinIO: every record once.
  - RustFS r002: 300 records stored twice (finding T10-F2 below):
    - 100 belong to request 462, sent 13 ms after the kill-1 signal. The receiver took it into the log, the kill cut off its acknowledgement, and the producer resent it. This is an attributed at-least-once duplicate.
    - 200 belong to requests 528 and 529, acknowledged by engine 2's log 60 and 10 ms before kill 2 and never resent. Engine 3 stored each twice (pre-multiplicity 0 at the exit, 2 at the end).

### Orphaned uploads, lateness, multipart

- Incomplete uploads after each case:
  - `kill_upload`: exactly the one upload kill 1 left open;
  - every other case: 0.
- `flush.abort_failures`: 0 in every engine.
- Multipart ran on both real stores in every cell: CreateMultipartUpload, UploadPart and CompleteMultipartUpload answered 2xx, with multipart ETags `-2`.
- Partition lateness: 0 violations. No cell straddled an hour end.

## Findings for Task 12

**T10-F1 (defect, reproduced and located): a graceful shutdown of the buffered topology makes the durable buffer redeliver bundles the exporter already wrote.**

- Seen in both buffered `graceful_restart` cells:
  - MinIO: 102 bundles, 10,200 records stored twice;
  - RustFS: 26 bundles, 2,600 records stored twice.
  - A dev run showed the same (10,200).
- Log sequence (both runs):
  1. `durable_buffer.shutdown.start`, then `shutdown.drained bundles_drained=1`, then `shutdown.complete`, all within 23 ms.
  2. Then `series_parquet.shutdown` and `shutdown.complete accepted=324 acked=324 nacked=1` 11-20 ms later.
  - So the exporter flushes and acknowledges its ACTIVE block after the buffer's engine has shut down, and those acknowledgements are never persisted.
- Source: `crates/core-nodes/src/processors/durable_buffer_processor/mod.rs:1643-1737` (`handle_shutdown`). It forwards what it can, then calls `engine.shutdown()` at once, without waiting for downstream acknowledgements of the bundles in flight.
- Remedy direction: in `handle_shutdown`, wait (until the shutdown deadline) for the in-flight bundles' ack/nack before `engine.shutdown()`, or persist their acknowledgements after it. The exporter's own drain takes about 20 ms here.
- Reproducer:

  ```bash
  SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 \
    taskset -c 0-7,16-23 python3 -m crates.validation.tests.series_parquet.measure \
    failures --family process --output-dir DIR --option 'only_cells=["graceful_restart-buffered-minio"]'
  ```

  About 90 s per cell. The failing check is `replay_only_eligible`.

**T10-F2 (defect, new, timing-dependent): after a SIGKILL, the buffer can deliver a log-acknowledged bundle twice.**

- Seen once, in `kill_upload-buffered-rustfs` r002.
- Requests 528 and 529 were acknowledged by engine 2's log 60 and 10 ms before kill 2.
- Engine 3 logged `quiver.segment.scan recovered 110 segments` and `quiver.wal.replay replayed_count=2`. Exactly those two requests were then stored twice, both copies by engine 3, and neither was ever resent.
- Hypothesis: entries that had already reached a segment were also left unconsumed in the WAL, so recovery replays them into a new segment and both are delivered. The WAL cursor or segment boundary is not made durable together with the segment.
- Not seen in the other 5 buffered kill cells or in r001 of the same cell. r001's single duplicate is fully explained by a producer resend. So it needs a kill within tens of ms of a log acknowledgement.
- At-least-once holds and nothing was lost. But it is duplication with no resend and no earlier delivery, which the buffer's contract does not explain.
- Task 13 (no-resend proof and backlog replay) is the place to reproduce it at scale. Evidence: the run file (`observations.fault.process.replay`) and the archive's engine-3 log.

**T10-O1 (observation): a single PUT cut off by a kill can still be committed after the process died.**

- RustFS committed engine 2's cut-off 8 KiB series PUT in both of its kill_upload cells. MinIO did not, in both of its cells.
- NGINX had already taken in the whole request (request_length 8,200) and the throttled proxy kept delivering it. The store then completed an object whose client had been killed about 1.4 s earlier.
- For the exporter this is the documented "an issued storage operation may still complete" case. Here it happened after the writer process died, not only after its deadline.
- It left a series file of a dead boot with no values. That is harmless: descriptor coverage and both readers passed.
- Suggested README line: an object of a killed writer can appear after it died. `boot_id` separates it, and readers are unaffected.

**T10-O2 (observation): graceful drain time follows the window.**

- Strict: the exporter received Shutdown only after the receiver's in-flight requests were acknowledged at the next window boundary. Exit took 5.0 s at a 5 s window, which extrapolates to about 15 s plus a flush at the shipped window. This is well inside L = 135 s and a 150 s admin deadline.
- Buffered: the exit took 0.15 s, the direct cause of T10-F1.

**F1 (the phase-2 multipart leak, open from Task 9):**

- Not triggered. The only incomplete uploads were the ones killed engines left open, and `abort_failures` was 0.
- The graceful drains finished all uploads.

## Reruns (recorded as `purpose` in the index)

The first full matrix had two failures that were harness bugs. Both were fixed in e7bbd87fc, with contract tests that fail on the old code, and each cell was rerun once. The r001 runs stay published in child index `failure-process-ac6577648c50.json`.

- `kill_upload-strict-rustfs`:
  - r001 failed `fault_observed`: the harness required the cut-off series PUT's key to stay absent, but RustFS committed it (T10-O1). The kill had cut the request off, so the rule was wrong, not the product.
  - r002 passed.
- `kill_upload-buffered-rustfs`:
  - r001 failed `fault_observed` for the same reason, and `duplicates_explained` because the buffered rule ignored a request the producer resent after a kill cut off its log acknowledgement (100 records, all in resent requests).
  - r002 no longer fails on those. It failed `duplicates_explained` on T10-F2, a different, genuine duplicate, so it was not rerun again.

Development runs, not published, before the final code:

- graceful strict MinIO r001: a float `timeout_secs=150.0` was refused by the admin API. Fixed before any published run.
- kill_upload strict MinIO: engine 2 was killed after 2 answered epochs, which fails `minimum_samples`. The gate now waits for 3 epochs first.
- Their archives are in `.measurement-artifacts/failure-process-dev/`.

## Deviations for the record

- `result["checks"]` is a list, as in Task 9. The long test reads a check with `check_status(result, "new_boot_id")`, and reads `old_pid`/`new_pid` from `observations.fault.numbers` rather than `result["metrics"]`, because metrics are only the compared memory and correctness figures.
- The fast contract tests (`ProcessCaseContracts`, 13 tests) were written alongside the implementation. The long `ProcessFailureTests.test_kill_during_upload` was run red first: it failed on the unregistered family after a successful preflight. It was not rerun green through unittest; the same four cells ran through `measure failures`.
- `upload_throttle` is registered as a fault so the rig's recovery and teardown own the toxic.
- The drain deadline passed to the admin API is `int(L) + 15` = 150 s. The plan's formula gives 150 s here. The held-request allowance past the cleanup cutoff (`HELD_DECISION_ALLOWANCE_S`) is 1 s.

## Tests

- `test_failures`, with Docker and fault tools required, under taskset: 84 tests OK, 2 skipped (the two long tests).
- `test_measurement`: 281 OK.
- markdownlint on the README: clean.
- `sanitycheck`: only the pre-existing `docs/superpowers` non-ASCII files.

## Evidence

- Index: `docs/superpowers/reports/series-parquet-measurement/failure-process.json` (status failed, 9 of 12). It contains:
  - 12 current run files `failure-process-<case>-<topology>-<store>-c1-w5-r00N.json`;
  - 9 baselines;
  - child index `failure-process-e0a711aa44eb.json` (the index before the re-judgement), which keeps `failure-process-ac6577648c50.json`, holding the superseded RustFS r001 runs.
- Raw archives (engine logs of every engine, the NGINX access log, configs, the ledger; no Parquet): `<repo>/.measurement-artifacts/failure-process/*.tgz` (14 archives, 145 MB).
- Development archives: `<repo>/.measurement-artifacts/failure-process-dev/`.
- Family state: `/var/tmp/series-failure-process/failures-state.json`. Run directories were deleted after archiving; no Parquet and no container remain.
- Logs: `<scratchpad>/t10/full.log` and `rerun.log`.

## Concerns

1. The index is failed: 3 of 12 cells fail on T10-F1 (twice) and T10-F2. Both findings are durable-buffer defects, not exporter defects. The checks were not loosened.
2. T10-F2 was seen once. It is timing-dependent, and its mechanism is a hypothesis drawn from the WAL replay count.
3. The two rule fixes in e7bbd87fc changed harness verdict rules after seeing a failure. Each fix makes the check match what the brief asks (an interrupted request; an attributable duplicate), and each has a contract test. A reviewer should confirm that reading.
4. `exit->ready` (about 1.2-1.5 s) includes the harness's listing, inventory and container start. The engine's own launch to `readyz` was 0.16-0.20 s.
5. No cell straddled an hour end, so the lateness bound was exercised only trivially. No graceful drain came near its bound.

## Codex review fix round

The review confirmed that both post-failure rule changes are scoped correctly and that T10-F1 and T10-F2 are supported. Three Important fixes followed, one commit each, each with a contract test that fails on the old code. A fourth commit extends the re-judgement to process runs.

| commit | fix |
| --- | --- |
| b7f71f5c1 | The graceful exit was judged against the 135 s partition lateness bound. It is now judged against the 150 s shutdown deadline (the admin call's return) and the 156 s cleanup cutoff (`graceful_exit_problem`). The README is updated, and 135 s stays the lateness bound only. |
| 60be0269a | `orphaned_uploads_expected` only rejected unknown uploads (`orphan_verdict`). It now also requires: every expected upload listed under its key; extras only up to the reported abort failures; and, where the case cleans up, no upload left after the cleanup. |
| 439528036 | The multipart gate, and the caught upload in `upload_problems`, require part bytes that `list_parts` shows in the store. NGINX's logged bytes alone no longer count. |
| 7a3a7e7bd | `rejudge-failures` re-judges process runs from the run file alone (`rejudge_process_checks`), with the same rules. |
| 6f6f9193e | `failure-process.json` advanced by `rejudge-failures`; the previous index is kept as child `failure-process-e0a711aa44eb.json`. |

Re-judgement of the 12 published runs: no verdict changes, and nothing undecidable.
- Graceful exits: 5.00-5.01 s (strict) and 0.15 s (buffered), all with exit code 0 and admin return within 150 s.
- Orphans: 0 after graceful_restart and kill_active; exactly the 1 expected upload after kill_upload, under its key. Cleanup was clean in every run.
- Multipart gate: every kill_upload run had stored part bytes of 1,538,968-1,540,930.
- The index status stays failed, 9 of 12, on T10-F1 (x2) and T10-F2.

Checks: `test_failures` 87 run, OK, 2 long skipped. markdownlint on the README is clean.
