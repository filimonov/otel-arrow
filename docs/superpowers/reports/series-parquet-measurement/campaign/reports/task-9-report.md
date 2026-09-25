# Task 9 report: S3 fault state machines and recovery evidence

## Status

DONE_WITH_CONCERNS.

- The full matrix ran: 3 faults x 2 topologies x 2 stores = 12 cells, on real MinIO and RustFS behind the Task 8 fault rig, with the containerized release engine.
- 7 cells pass. 5 fail one hard check, `orphaned_uploads_expected`:
  - all four http503 cells;
  - store_outage buffered on RustFS.
- In each of those 5 cells, one incomplete multipart upload is left in the bucket while the exporter's `flush.abort_failures` reads 0. This is a product defect (Task 12 finding F1, below). It is not a harness expectation to adjust, so the cells stay failed and were not rerun.
- These held in all 12 cells:
  - delivery;
  - descriptor coverage;
  - reader agreement;
  - bounded resources;
  - the RSS band reconciliation;
  - multipart on the real store;
  - the partition lateness bound.

Worktree `<repo>/.claude/worktrees/agent-a88e8655466ef5fd9`, branch `worktree-agent-a88e8655466ef5fd9`. It was reset to c28864563 at the start. Not pushed.

## Commits

| commit | what |
| --- | --- |
| 342a094ea | `chore(series-parquet): S3 fault cases as observable state machines`: `failure_case`, `fault_check` and `measure failures`, plus contract tests. Changes to shared files: NGINX logs the request length and the full request URI (`$request_uri`, needed to name multipart steps); `MALLOC_CONF` reaches the engine container (needed for the jemalloc band); `Producer.send_paced(stop=)`; the ledger keeps the gRPC status message; capacity extras gain `flush.cancelled`, `flush.abort_failures` and `flush.late_commits`. |
| c4dbb422b | `fix(series-parquet): a straddling fault cell waits for its hour without the lease`: the controller's hang intervention (below). Also adds `only_cells`. |
| de1d906ee | `chore: measure series recovery from real S3 faults`: `failure-s3.json` and its tree (12 runs, 7 baselines, 1 child index, 3.9 MB), and the README "Failures" section. |

No product code was changed.

## How the cases run

- One cell = a fresh store (by image id), a fresh FaultRig and a fresh release engine in the rig namespace.
- Engine:
  - one worker on core 1;
  - the harness under `taskset -c 0-7,16-23`, producer threads on 8-15,24-31;
  - the shipped S3 store retry section (max_retries 5, retry_timeout 30s);
  - default `flush_retry_deadline` 60s and `upload.abort_timeout` 5s;
  - `window.interval` 5s and `upload.part_bytes` 5MiB, so every logs values file (about 6.8 MB) is a multipart upload.
- Producer: the ledgered producer of the PR-tier soak.
  - 20 req/s of 100 one-KiB records, every tenth a metrics request (a finite mixed-signal input).
  - It resends a retryable refusal with the same bytes until acknowledged (up to 2000 attempts); the ledger refuses different bytes.
  - It stops 20 s of acknowledged input after the exporter resumed.
- State machine per cell, recorded in `observations.fault.states` with instants and evidence:
  1. `input_started`
  2. `baseline`: at least 15 s of input, a values object HEADed directly in the store, ACTIVE nonempty, and an ack within 2 windows.
  3. `armed`
  4. `observed`: the fault's intended condition.
  5. `fault_removed`
  6. `endpoint_healthy`: a signed HEAD of the bucket through the route.
  7. `resumed`: a values file written and an ack after the removal.
  8. `input_stopped`
  9. `drained`: the drain proof.
  - Everything after `endpoint_healthy` must finish within 300 s.
- Intended conditions:
  - slow: a delayed response (at least 1.35 s) and a throttled upload (at most 512 KB/s), both started under the fault; FLUSHING nonempty with ACTIVE or pending work; and backpressure. Backpressure means one of: admission closed for at least one window, a receiver refusal, or the buffer's in-flight bundles holding still while admission is closed.
  - http503: a PUT or POST NGINX answered 503, `flush.retries` grew, a storage nack, and its retry. Strict: the producer received "could not write to object storage (unavailable)". Buffered: `buffer.retries.scheduled` grew.
  - store_outage: a flush failure at least 60 s (the flush deadline) after the stop, a storage nack and its retry.
- Oracle and checks:
  - The ledger oracle: every sent record, the multiplicity histogram, DuckDB plus clickhouse-local, descriptor coverage.
  - `fault_check`: fault_observed, recovered, drained, at_least_once, descriptor_coverage, reader_agreement, bounded_resources, no missing acknowledged record, and the histogram present.
  - Also checked: multipart_exercised, orphaned_uploads_expected, partition_lateness_bound, duplicates_explained, fault_rig_clean, rss_reconciliation (the allocator band rule) and the host controls.

## Per-case results

Times are in seconds. "Recovery" is from endpoint healthy to the first values file plus acknowledgement after the removal.

| fault | topology | store | status | failed checks | time to condition | fault held | recovery | healthy to drained | drain |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| slow | buffered | minio | passed | - | 15.8 | 15.8 | 1.01 | 25.1 | 4.10 |
| slow | buffered | rustfs | passed (straddled) | - | 16.0 | 16.0 | 1.03 | 25.1 | 4.08 |
| slow | strict | minio | passed (straddled) | - | 10.6 | 10.6 | 1.71 | 26.9 | 2.44 |
| slow | strict | rustfs | passed | - | 15.1 | 15.1 | 1.86 | 28.0 | 2.65 |
| http503 | buffered | minio | failed | orphaned_uploads_expected | 60.8 | 60.8 | 0.76 | 25.0 | 4.16 |
| http503 | buffered | rustfs | failed | orphaned_uploads_expected | 60.7 | 60.7 | 1.02 | 25.1 | 4.03 |
| http503 | strict | minio | failed | orphaned_uploads_expected | 121.8 | 121.8 | 1.68 | 31.1 | 2.86 |
| http503 | strict | rustfs | failed | orphaned_uploads_expected | 123.0 | 123.0 | 0.81 | 29.7 | 2.72 |
| store_outage | buffered | minio | passed | - | 65.7 | 66.1 | 0.67 | 24.7 | 4.02 |
| store_outage | buffered | rustfs | failed | orphaned_uploads_expected | 60.0 | 60.4 | 2.91 | 24.0 | 1.01 |
| store_outage | strict | minio | passed | - | 64.9 | 65.3 | 7.69 | 31.4 | 2.27 |
| store_outage | strict | rustfs | passed | - | 124.7 | 125.0 | 1.50 | 27.0 | 2.33 |

### What the exporter did, and the delivery verdict

- Every cell: offered = acknowledged, and 0 missing, 0 unexpected, 0 corrupt.
- Strict cells: every storage nack was a server nack ("could not write to object storage (unavailable)"), never a producer-local timeout: `producer_local_timeouts_count` = 0 in all cells. Each nacked request was resent with its original bytes.

| cell | records offered / acked / stored | multiplicity | dup outside resent | 503s | flush retries | flush failures | exporter nacks | producer storage nacks / resent | buffer retries | abort failures | late commits | cleanup events |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| slow buffered minio | 103700 / 103700 / 103700 | 1: all | 0 | 0 | 0 | 0 | 0 | 0 / 0 | 0 | 0 | 0 | - |
| slow buffered rustfs | 244200 / 244200 / 244200 | 1: all | 0 | 0 | 0 | 0 | 0 | 0 / 0 | 0 | 0 | 0 | - |
| slow strict minio | 226300 / 226300 / 226300 | 1: all | 0 | 0 | 0 | 0 | 0 | 0 / 0 | - | 0 | 0 | - |
| slow strict rustfs | 103100 / 103100 / 103100 | 1: all | 0 | 0 | 0 | 0 | 0 | 0 / 0 | - | 0 | 0 | - |
| http503 buffered minio | 193300 x3 | 1: all | 0 | 63 | 8 | deadline 1 | storage 100 | 0 / 0 | 100 | 0 | 0 | - |
| http503 buffered rustfs | 193700 x3 | 1: all | 0 | 64 | 8 | deadline 1 | storage 100 | 0 / 0 | 100 | 0 | 0 | partial 1 |
| http503 strict minio | 94100 x3 | 1: all | 0 | 121 | 16 | deadline 2 | storage 128 | 128 / 128 | - | 0 | 0 | unknown 1 |
| http503 strict rustfs | 91800 x3 | 1: all | 0 | 126 | 17 | deadline 2 | storage 128 | 128 / 128 | - | 0 | 0 | unknown 1 |
| outage buffered minio | 204200 x3 | 1: all | 0 | 0 | 2 | deadline 1 | storage 100 | 0 / 0 | 100 | 0 | 0 | - |
| outage buffered rustfs | 208800 x3 | 1: all | 0 | 0 | 1 | deadline 1 | storage 100 | 0 / 0 | 100 | 0 | 0 | unknown 1 |
| outage strict minio | 86200 x3 | 1: all | 0 | 0 | 1 | deadline 1 | storage 100 | 100 / 100 | - | 0 | 0 | unknown 1 |
| outage strict rustfs | 83100 / 83100 / 92100 | 1: 74100, 2: 9000 | 0 | 0 | 3 | deadline 2 | storage 128 | 128 / 128 | - | 0 | 0 | unknown 1 |

State machine observed, by fault:

- slow:
  - Held 10-16 s. One throttled 5 MiB part took 7.6 s, and responses were delayed 1.5 s.
  - Admission closed 5.1-5.9 s (the backpressure). FLUSHING with ACTIVE behind it in 40-58 samples.
  - No flush failed. No retry, no nack. Upload retries 0.
  - Strict acknowledged records per second: 0 during the fault on MinIO, 763 on RustFS.
  - Buffered producers kept being acknowledged at 2000 records/s, while the log grew to 92-97 MB.
- http503:
  - object_store retried each 503 up to 6 times, then the exporter retried the flush (8 or 16-17 attempts) until the 60 s deadline.
  - `flush.failures{deadline}` 1-2, then 100 or 128 storage nacks.
  - Strict: the producer got UNAVAILABLE with the storage sentence and resent. Buffered: the buffer scheduled 100 retries.
  - The cleanup probe ran after the ambiguous end:
    - `unknown` when the HEAD itself got 503 (NGINX answers every request 503);
    - `partial` on RustFS buffered (the series object present, values absent).
  - Recovery took 0.8-1.7 s after the control file was removed.
- store_outage:
  - With the store stopped, route requests hang until the client gives up (NGINX 499/502), as Task 8 noted.
  - The flush failed at the 60 s deadline (1-3 attempts). Nacks: 100 or 128.
  - Strict producers were refused and resent. Buffered retries: 100.
  - Recovery took 0.7-7.7 s after the store answered. RustFS buffered was 2.9 s because the route needed 5.5 s to answer again.
- Duplicates:
  - Only store_outage strict RustFS stored any: 9000 records stored twice.
  - All belong to the 128 resent requests: `duplicated_outside_resent_requests_records` = 0 everywhere.
  - Cause: failed block 00000003 left both its logs series and logs values objects (`failed_block_objects`), and its nacked requests were resent into a new block. This is the documented no-block-atomicity case (FORMAT.md section 4: "rows of a nacked request may ... appear again after the producer retries"). No exactly-once claim is made.
  - The cleanup probe could not tell (`unknown`: the cutoff passed before the probe finished while the store was down), so `flush.late_commits` stayed 0 although the objects exist. Finding F3.
  - A development run of http503 strict MinIO (before the final code, not published) stored 9000 duplicates of the same shape.

### Resources (every sample of every cell)

- ACTIVE and FLUSHING stayed within 500 MiB.
  - Buffered ACTIVE reached 115 MB during 60 s faults.
  - Buffered RustFS outage FLUSHING reached 107 MB (13-part upload) after recovery.
- Series cache: 150 of 200000 entries.
- Pending slot: 0.
- Accounted memory within the budget.
- Buffer: within its cap, no loss, no rejection, no ingest failure.
- Oldest unacknowledged age: baseline 3.9-4.8 s; peak 15 s (slow), 65 s (60 s faults), 121-124 s (125 s faults); 0 after the drain.
- rss_reconciliation (the allocator band) passed everywhere, with the largest residual 9.1 MB against a 33-50 MB tolerance.

| cell | peak RSS MB | accounted peak MB | buffer peak MB | acked records/s before / during / after |
| --- | --- | --- | --- | --- |
| slow buffered minio / rustfs | 150 / 146 | 38.5 / 38.5 | 92 / 97 | about 2000 / 2000 / 2000 |
| slow strict minio / rustfs | 112 / 121 | 30.2 / 33.9 | - | 1882 / 0 / 2713; 1321 / 763 / 2833 |
| http503 buffered minio / rustfs | 437 / 448 | 115.2 / 115.2 | 182 / 183 | 2000 / 2000 / 2000 |
| http503 strict minio / rustfs | 124 / 116 | 30.2 / 23.4 | - | 1332 / 0 / 2625; 1325 / 0 / 2669 |
| outage buffered minio / rustfs | 432 / 499 | 115.2 / 252.8 | 174 / 186 | 2000 / 2000 / 2000 |
| outage strict minio / rustfs | 116 / 130 | 30.1 / 19.3 | - | 1917 / 0 / 1933; 1295 / 0 / 2565 |

The strict "before" rate is below 2000 because the first acknowledgements wait a window plus a flush.

### Multipart evidence per store

- Every cell exercised the multipart path on the real store. The check requires both:
  - the route's trace, with CreateMultipartUpload, UploadPart (2 or more) and CompleteMultipartUpload answered 2xx;
  - the store, holding objects above 5 MiB with multipart ETags `"<md5>-N"` (from a direct listing).
- MinIO: 7-21 completed multipart uploads per cell, parts per object 2 (strict) or 2 to 13 (buffered). Example: slow strict minio completed 21 uploads, 42 parts, 21 multipart-ETag objects.
- RustFS: 8-23 per cell, parts per object 2 (strict) or 2 to 13 (buffered). Example: slow buffered rustfs completed 23 uploads, 47 parts, 23 multipart-ETag objects.

### Orphaned uploads per case

- Listing: `list_multipart_uploads` on the bucket, directly in the store, after the engine stopped. Expected maximum: the exporter's `flush.abort_failures`, which was 0 in every cell.
- Cells with 0 orphans: all four slow cells, store_outage strict MinIO, store_outage buffered MinIO, store_outage strict RustFS.
- Cells with 1 unexpected orphan:

| cell | orphaned key |
| --- | --- |
| http503 strict minio | logs values `...00000003.parquet` |
| http503 strict rustfs | logs values `...00000004.parquet` |
| http503 buffered minio | logs values `...00000004.parquet` |
| http503 buffered rustfs | logs values `...00000004.parquet` |
| store_outage buffered rustfs | logs values `...00000004.parquet` |

- Each upload was created just before arming, by the flush of the window that had just sealed. By the NGINX-logged completion of CreateMultipartUpload: http503 12, 33, 36 and 53 ms before arming; store_outage buffered RustFS 451 ms before (the store's second-resolution `Initiated` reads 0.46 s before). Its later part and complete requests failed under the fault.
- A development run of store_outage strict RustFS also left 1 orphan with 0 abort failures (archive below).

Analysis, as the controller asked: were aborts issued, and what did the store answer?

- http503, all 4 cells, from the NGINX trace. Example http503 strict minio, upload `YzQzZjE...`:
  1. POST ?uploads 200.
  2. PUT part 1 and part 2, both 200.
  3. POST ?uploadId (CompleteMultipartUpload) answered 503 six times (object_store's retries).
  4. DELETE ?uploadId (AbortMultipartUpload) answered 503 six times.
  5. The next attempt starts with POST ?uploads, answered 503.
  - So an abort was issued, by object_store's `WriteMultipart::finish`: on a `complete()` error it aborts and returns the abort's error (object_store-0.13.2 src/upload.rs:226-241). The store (NGINX, standing in for it) refused it.
  - `flush.abort_failures` stayed 0, and no `flush.cleanup{abort_failed}` was logged.
- store_outage buffered rustfs, upload `ODVjZTJk...`:
  1. POST ?uploads 200.
  2. Parts 1 and 2 got 502 when the store stopped.
  3. The part-2 retry got 499 after 30 s.
  4. The part-1 retry got 400 after 68 s, once the store was back.
  - No DELETE ?uploadId was issued at all: `WriteMultipart::finish` returns from `wait_for_capacity(0)?` without aborting.
  - The flush ended at the deadline with `flush.cleanup{unknown}` ("the cleanup cutoff passed before the probe"), and abort_failures stayed 0.
- The development store_outage strict RustFS run showed the same shape: a part got 502, no DELETE, a 132 s part retry, and abort_failures 0.
- Verdict: a product defect, not an expectation to adjust.
  - The metric's own contract (metrics.rs:67-71) is: "Cancelled writes of decided flushes whose multipart abort failed or that did not unwind by the cleanup cutoff, each of which may leave an upload to the bucket's lifecycle rule". Here uploads were left and it did not move.
  - FORMAT.md section 4 promises only a best-effort abort, and leaves leftovers "after a crash" to a lifecycle rule. These cells had no crash. A plain storage failure in the finalizing phase leaks one upload per failed attempt, and no signal says so.

### Partition lateness bound

- The bound is L = 5 + 2 x (60 + 5) = 135 s.
- Each run recorded, per partition hour, the latest store LastModified and the latest first sighting in a direct listing (every 1 s), relative to the hour's end.
- 0 violations in 12 cells.
- Two slow cells ran with `straddle_hour`: the fault was armed 10 s before the hour ended, and `armed_before_hour_end` is true in both.

| cell | hour | latest LastModified after its end | latest first sighting after its end |
| --- | --- | --- | --- |
| slow strict minio | 22:00 | +0.705 s | +1.018 s |
| slow buffered rustfs | 23:00 | +6.301 s | +6.660 s |

- The other 10 cells ran 20-57 minutes before their hour ended. Their latest visibility was -3434 s to -1225 s: no object of those hours was written after the hour.
- The worst case (a block still retrying across the hour end) was not produced. Every 60 s fault ran mid-hour, and the two straddles used the slow fault, whose blocks completed within seconds of recovery. See concern 3.

## Findings for Task 12

**F1 (defect): a multipart upload that fails while finalizing is leaked and not counted.**
- What was seen: in 5 of 12 cells, 1 incomplete multipart upload remains while `flush.abort_failures` = 0.
- Source: `crates/series-lake/src/sink/write.rs:704-714`, phase 2.
  - `writer.finish()` runs the last part uploads and CompleteMultipartUpload.
  - Its error is returned with `finish?`. No abort is attempted, and the error is not wrapped as `AbortFailed`.
  - object_store sometimes aborts internally (on a `complete()` failure), but its abort failure is returned as a plain storage error. On a part failure it does not abort at all.
- The flush then retries into a new upload (`flush.rs:231` keeps only `last = Some(error)`), so every such failed attempt leaks an upload.
- Reproducer (under 3 minutes per cell):

  ```bash
  SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 \
    taskset -c 0-7,16-23 python3 -m crates.validation.tests.series_parquet.measure \
    failures --family s3 --output-dir DIR --option 'only_cells=["http503-strict-minio"]'
  ```

  Failing check: `orphaned_uploads_expected`. The upload id and the full trace are in the run file (`observations.fault.orphaned_uploads`, `route`) and in the archive (NGINX log).
- Remedies:
  - After a phase-2 failure, abort through the store's multipart API using the upload id, and classify the result.
  - At the least, count and log it as `abort_failed` (outcome "upload left").
  - Say in FORMAT.md section 4 and the README that a finalizing failure can leave an upload without a crash.

**F2 (observation): a failed attempt's part-upload retry keeps running after its attempt ended.**
- In the RustFS outage runs a part-1 retry of attempt 1 stayed in flight 68 s (132 s in the development run). It ended 400/502 after the store returned, well after its attempt had failed and the next one had started.
- It cannot complete an object, since no CompleteMultipartUpload follows. It is a request outliving the flush deadline plus abort_timeout, which the lateness argument assumes cannot happen.
- Source: object_store `WriteMultipart` in-flight part tasks, not cancelled when `finish` errors (the same write.rs path).

**F3 (observation): a late commit is missed when the store is down through the cleanup cutoff.**
- In store_outage strict RustFS, failed block 00000003 left both of its objects, which caused 9000 duplicates after the resend.
- The cleanup outcome was `unknown` (the HEAD could not run before the cutoff), so `flush.late_commits` = 0.
- The metric therefore undercounts late or partial commits exactly when the store is unreachable. This is by design (a probe bounded by the cutoff), but it is worth a README line: `late_commits` is a lower bound, and `cleanup{unknown}` means duplicates are possible.

**F4 (observation): a cleanup probe during a 503 fault can only report `unknown`.**
- The probe's HEAD is answered 503 as well.
- Seen in http503 strict MinIO and RustFS.

**Deviations for the record:**
- Metric names follow the harness's unit-suffix rule (`missing_acked_records`, `http_503_responses_count`), the Task 7 precedent.
- The compared metrics are only memory (peak RSS, accounted peak, buffer peak) and the correctness counts, because the harness compares every metric of a run against its baseline.
- The per-case numbers (durations, counts, the 503 total, duplicates) are under `observations.fault.numbers`. So the long test reads `observations.fault.numbers.http_503_responses_count`, and `fault_check` reads the multiplicity histogram and duplicates from `observations.fault`.
- `fault_check` reads the result's check list by name rather than a dict.

## Controller intervention (the "hang")

- `measure failures` was not blocked on a join or a pipe.
- slow-buffered-rustfs ran with `straddle_hour` and was waiting in `await_wall_clock`, a `threading.Event.wait` poll with a deadline, for 23:58:20 UTC, so that it could arm at 23:59:50.
- The waiting was deliberate. Two things were wrong with it:
  - it held the host lease;
  - it printed nothing.
- Fix c4dbb422b:
  - the start wait now happens in `failure_case` before `run_case`, so without the lease;
  - it announces the instant it waits for, and is capped at one hour plus the start lead;
  - as first fixed, the case took the lease 90 s before arming and then waited under it, with input running, about 70 s after its baseline, not only the final 10 s as first reported (codex review). Fix 5dc4031e8 shrinks the lead to 35 s: rig, engine and baseline took 20 s on this host, so about 15 s of running input remain under the lease before arming. The case records `armed_before_hour_end`;
  - a contract test covers the schedule.
- The SIGTERM left only an empty run directory, and no result or container. The cell was then run again from the state file and passed.

## Tests

- `test_failures -v` (Docker and fault tools required, under taskset): 64 tests OK, 1 skipped (the long `S3FailureTests.test_http503_recovery`, which needs `SERIES_MEASURE_LONG=1`). The run includes the live rig.
- New contract tests:
  - fault_check;
  - the lateness bound and partition lateness;
  - S3 operation naming;
  - multipart evidence;
  - ledger attempt classes;
  - duplicate attribution;
  - engine events and failed-block objects;
  - fault conditions;
  - the straddle schedule;
  - prerequisites: a missing image skips an optional lane, fails a required one, and an activation error after preflight fails.
- `test_measurement`: 281 OK (1 skipped). `test_soak`: 15 OK (2 skipped).
- markdownlint on the README is clean. Sanitycheck reports only pre-existing `docs/superpowers/*` non-ASCII files.
- Red-first: the contract tests were written alongside the implementation, not run red first.
- The long both-topology test was not run through unittest. The same 12 cells ran through `measure failures`.

## Evidence

- Index: `docs/superpowers/reports/series-parquet-measurement/failure-s3.json` (sha256 prefix 53fc49c02f695716).
  - 12 run files `failure-s3-<fault>-<topology>-<store>-c1-w5-r001.json`.
  - 7 baselines.
  - child index `failure-s3-8a70aaa839ea.json` (the first invocation's index).
- Raw artifacts (engine logs, NGINX access logs, configs, ledgers, no Parquet): `<repo>/.measurement-artifacts/failure-s3/*.tgz` (12 archives, 206 MB).
- Development runs, not published: `<repo>/.measurement-artifacts/failure-s3-dev/`. Notably `failure-s3-store_outage-strict-rustfs-c1-w5-r001.tgz` (the no-abort orphan) and `failure-s3-http503-strict-minio-c1-w5-r001.tgz` (9000 partial-block duplicates).
- Family state: `/var/tmp/series-failure-s3/failures-state.json`. Run directories were deleted after archiving, and no Parquet remains.
- Logs: `<scratchpad>/t9/matrixA.log`, `matrixB.log`, `matrixC.log`. Generated tables: `scratchpad/task-9-tables.md`.

## Concerns

1. The index status is failed (7 of 12 cells pass) because of F1. By the brief's rules, an unexpected orphan is a Task 12 finding, and the checks were not loosened.
2. The first invocation's index is kept as a child of the final one.
3. Lateness was exercised only lightly: +1.0 s and +6.7 s after the hour's end, against a 135 s bound. A 60 s fault that straddles an hour end would push it toward the bound; it costs up to an hour's wait per cell.
4. The run files are 230-400 KB each, dominated by samples (one kept per 4 s).
5. Wall time was longer than the brief's 15-25 min: about 35 min of cell work, plus hour-end waits for the two straddled cells.

## Codex fix round

Review: `scratchpad/codex-review-t9.out`. It confirmed four things:
- loss cannot pass;
- the strict RustFS duplicate attribution is consistent;
- the lateness formula and the multipart evidence are genuine;
- the F1 diagnosis.

Commits, one per fix. Each fix has a contract test that fails on the old code, and the harness ran only focused checks.

| commit | fix | test that fails on the old code |
| --- | --- | --- |
| d7d57ad9a | Buffered duplicates had been accepted while the extra copies fit in nacks x 100 records. Now each duplicated record must have a copy in a values file of a block whose flush failed (file names from `flush.failed` events, record ids read from the downloaded files before deletion); unattributed duplicates fail. | `test_buffered_duplicates_need_a_copy_in_a_failed_block`: a duplicate with no failed-block copy is not explained |
| 5d660904e | `fault_check` now requires every fault check present and every hard check passed, which is exactly the published status rule. | `test_fault_check_fails_the_committed_http503_run` (the old helper accepted it) |
| 82561b29b | The outage condition now needs a `flush.failed` event of class `deadline` whose own logged time is at least the flush deadline after arming. The case tails the engine log (`LogTail`), and every run stores each flush failure's time, class and window (`engine_events.flush_failures`). | `test_outage_condition_needs_a_failure_after_the_deadline`: a failure at 59.5 s with the timer past 60 s is not enough |
| 5dc4031e8 | The straddle lead dropped from 90 s to 35 s, so about 15 s of running input remain under the lease before arming, instead of about 70 s. README updated. | the straddle test bounds the lead by a baseline plus a window |
| ad0076a56 | New `measure rejudge-failures`: re-judges a published index from stored runs and their archived engine logs; no rerun, run files unchanged. | `test_rejudge_outage_runs_from_stored_evidence` |
| 3e1242a98 | `failure-s3.json` advanced; the previous index is kept as child `failure-s3-53fc49c02f69.json`, which keeps `failure-s3-8a70aaa839ea.json`. | - |

Re-judgement of the 12 committed runs: one verdict changes.
- `store_outage-buffered-rustfs` fault_observed goes from passed to failed.
  - Its only flush failure was logged 59.518 s after arming, and the fault was removed at 60.382 s.
  - That block was sealed 0.48 s before arming, so its own deadline fell before arming + 60 s. The run no longer proves a failure beyond the deadline measured from the stop.
  - The run was already failed on its orphan, so its status is unchanged: failed checks are now fault_observed and orphaned_uploads_expected.
- The other outage runs keep a qualifying failure logged before removal: buffered MinIO at 64.6 s, strict MinIO at 64.4 s, strict RustFS at 124.6 s.
- The http503 runs failed at 59.9-59.96 s and at 121.7 / 122.9 s. The http503 condition is unchanged.
- duplicates_explained: unchanged everywhere. No buffered run stored a duplicate, and strict attribution by resent request is unchanged.
- Index status stays failed. Passing runs: 7 of 12.

Report corrections:
- The orphan creation timing, in the orphan section: 0.451 s by NGINX, 0.46 s by the store, before arming.
- The under-lease wait, in the intervention section.

Focused checks: `test_failures` 68 OK (1 long skip). `test_measurement` was not rerun because `measurement.py` did not change in this round. markdownlint on the README is clean.

## Follow-up: outage hold and the store_outage buffered RustFS rerun

The controller ruled that the store_outage buffered RustFS verdict was a flaw in how the case was designed, not a property of the product. Its outage ended 0.4 s after the deadline, so a rerun was justified.

**Commits**
- `b6b09a59a` An outage now stays in place until 15 s past the deadline of the failed block. Raw archives now default to the main checkout's `.measurement-artifacts/failure-s3`, also when run from a git worktree; the README documents this and `archive_dir`.
- `317eaee90` Harness bug in that first hold rule. It measured the hold from the failed block's window end. A block that waits for the flush slot starts its deadline only when its flush starts, so the window end understates it. The hold now ends 15 s after the qualifying deadline failure itself, which is logged when the block's deadline expires. Tested with a block that waited for the slot.
- `06bc32673` Evidence. The reason for each rerun is recorded in the index chain (`purpose`).

**Reruns of store_outage buffered RustFS**
- **r002** (first hold rule): the only qualifying failure was logged at 124.6 s, from block 4, which had waited behind block 3's failed flush. The fault was removed at 125.0 s, 0.4 s after that failure, so the margin was not really held. The run failed `orphaned_uploads_expected` (1 orphan, F1). It is superseded but still published in the index chain.
- **r003** (fixed hold): **passed every check.**
  - Deadline failure logged at 63.6 s. The store stayed stopped until 78.6 s, then the fault was removed at 79.0 s.
  - 352,400 records offered, acknowledged and stored once.
  - 200 storage nacks, 200 buffer retries.
  - Multipart path exercised: 9 uploads, up to 13 parts.
  - 0 orphans. The stop hit while uploads were being created (CreateMultipartUpload 499 ×4), so no upload was open. F1 needs the fault to land after an upload is created, so it is timing-dependent; F1 stays evidenced by the four http503 cells and by r001/r002.
  - Recovery took 60.8 s, from endpoint healthy to the first new file and ack. The second flush attempt was issued while the store was stopped and kept waiting until its own 60 s deadline (failure at 128.6 s), even though the store was back at 79 s. Only after that did the next block write. This fits F2: a request issued to a stopped store is not retried when the store recovers. It is within the 300 s recovery deadline.
  - This run wrote the store_outage baseline for its fingerprint.
- **Index `failure-s3.json`:** 8 of 12 cells pass. The four http503 cells still fail `orphaned_uploads_expected` (F1).
  - `rejudge-failures` was run again with the default archive directory (the main checkout): no changes, and nothing undecidable.
  - Index chain: current → `failure-s3-551650863b58` (r003) → `failure-s3-f20a3a7431e0` (r002) → `failure-s3-b0eb464c9ccd` (first re-judgement) → `failure-s3-53fc49c02f69` → `failure-s3-8a70aaa839ea`.
- **The other three outage runs (r001)** still pass the deadline-failure proof. They were run under the old condition, though, and were held only 0.4-1.5 s past their qualifying failure, not 15 s. That fits the new rule's intent for the check, but not its hold margin. I did not rerun them; the controller asked for this one cell only.
- **Archives:** r002 and r003 are in `<repo>/.measurement-artifacts/failure-s3/`.

Checks: `test_failures` contract classes, 19 OK. markdownlint on the README is clean.

## Final follow-up: all four outage cells under the new hold

The controller asked for the other three store_outage cells to be rerun, each with its reason recorded as `purpose`: the old hold could not show the post-recovery behaviour. Commit: `47934f8a0`.

| cell | run | status | store stopped for | recovery (healthy to file + ack) | healthy to drained | F2-type stall after restart | orphans | duplicates |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| buffered RustFS | r003 | passed | 79.0 s | **60.8 s** | 84.9 s | yes | 0 | 0 |
| strict MinIO | r002 | passed | 80.1 s | **62.4 s** | 87.5 s | yes | 0 | 0 |
| strict RustFS | r002 | failed (F1) | 140.0 s | 1.9 s | 27.6 s | no | 1 | 0 |
| buffered MinIO | r002 | passed | 140.1 s | 1.3 s | 25.5 s | no | 0 | 9000, all in failed block 00000004's files |

Every cell: 0 missing, unexpected or corrupt records, and both readers agree.
- Strict cells: 128 storage nacks, resent with the same bytes.
- Buffered cells: 200 nacks and 200 buffer retries.
- In buffered MinIO r002, failed block 00000004 left both its series and values objects, and the retry stored those 9000 records again. The new duplicate attribution (d7d57ad9a) matched every duplicate to that block's file.

**What the stall is.** I checked the NGINX logs of the two slow cells.

Strict MinIO r002 (times in seconds after arming; the store is back at 80.1):

| request | start | end | outcome |
| --- | --- | --- | --- |
| PUT | 69.6 | 99.6 | 499: the 30 s client timeout, no upstream answer |
| PUT (next attempt) | 99.8 | 129.6 | 499: hung until the flush deadline |
| HEAD (cleanup probe) | 129.6 | 134.6 | hung 5 s |
| POST ?uploads | 134.6 | 141.0 | 200, after 6.3 s |
| everything after | from 141 | | normal, a few ms each |

Buffered RustFS r003 shows the same shape: store back at 79.0, POST 68.6→98.6 (499), POST 98.9→128.6 (499), HEAD 128.6→133.6 hung, first success started 133.8.

So F2 is more than "an attempt sent while the store was down is not retried when it returns":
- Requests the exporter sent 20 s after the store was back also got no upstream answer.
- NGINX logs them with upstream status `-`, so the hang lies between NGINX and the store, not in the exporter's client.
- Meanwhile the harness's signed HEAD of the bucket answered at once, which is why `endpoint_healthy` was declared.
- In both slow cells, route writes resumed about 134 s after the stop, whenever the store was restarted.
- The two fast cells were restarted after 140 s, past that point, and recovered in 1-2 s.
- A fixed timer of about 130 s from the stop fits the Linux TCP connect timeout (tcp_syn_retries 6, about 127 s) of a connection dialled to the stopped container, stuck somewhere on the Toxiproxy/NGINX path, likely on the values proxy. I have not verified this: the harness's HEAD went through the general proxy.

**Revised F2 for Task 12 and Task 11.**
- After a store restart, writes through the fault route got no answer until about 130 s after the stop. It happened on both stores and both topologies:
  - strict MinIO and buffered RustFS were restarted at about 80 s and recovered in 60.8-62.4 s;
  - strict RustFS and buffered MinIO were restarted at 140 s and recovered in 1-2 s.
- The exporter behaved as specified throughout:
  - each attempt ended at the 30 s client timeout or at the 60 s flush deadline;
  - the requests were nacked and retried;
  - nothing was lost.
- The measured recovery time in those two cells is therefore most likely a property of the rig, not of the exporter.
- To tell them apart, Task 11 can send a direct request that bypasses Toxiproxy (and one through each proxy) in that window. If a direct request also hangs, it is store or kernel behaviour a deployment would see too.
- One thing is on the exporter's side: `endpoint_healthy` (HEAD bucket through the general proxy) does not prove the values route works.
- F1 (the leaked multipart upload) appeared again in strict RustFS r002, bringing the total to 7 runs: the four http503 runs, buffered RustFS r001 and r002, strict RustFS r002.

**Index `failure-s3.json`:** 7 of 12 cells pass.
- Failing: the four http503 cells and store_outage strict RustFS r002, all on `orphaned_uploads_expected` (F1). The dev-only store_outage strict RustFS orphan has now been reproduced in a published run.
- Re-judged under the current rules: no change.
- Raw archives: `<repo>/.measurement-artifacts/failure-s3/*-r002.tgz`. The archive directory defaulted correctly to the main checkout.
- Run directories were deleted, and no containers are left.

Task 9 is complete.
