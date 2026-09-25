# Task 11 report: network, DNS, TCP ACK and dropped completion response faults

## Status

DONE_WITH_CONCERNS.

- Full matrix: 8 network cases x strict/buffered x MinIO/RustFS = 32 cells, on real stores behind the fault rig, containerized release engine built from this worktree at c310b9f22 (product code unchanged by this task).
- 14 of 32 cells pass (after fix round 1). The 18 failures are: product findings for Task 12 (F1, T11-F2); evidence of inherent S3/store properties that need documentation, not an exporter fix (T11-F1, T11-F3); and four single-sample RSS band excursions. No check was loosened.
- In every one of the 32 cells: offered = acknowledged, 0 missing, 0 unexpected, 0 corrupt; DuckDB and clickhouse-local agree; descriptor coverage holds; every duplicate is attributed (resent request, strict; failed-block copy, buffered).
- Rig versus store: the stall after an early store return is the rig (Toxiproxy), not the store. Health check now also asks the values route.
- Dropped completion / lateness: a CompleteMultipartUpload held past the writer's cleanup cutoff published its object 145.0 s after the partition hour ended (bound 135 s), on RustFS, in a straddled cell. The violation expected by the amendment is recorded. On MinIO the store itself drops such a held request after 30.25 s, so no late object appears (an orphaned upload does instead).
- Late-commit detector (S8) on a real store: works on MinIO (INFO `late_commit`, `flush.late_commits` 1-2); never runs on RustFS (see T11-F2).

Worktree `<repo>/.claude/worktrees/agent-a100d6968899c7521`, branch `worktree-agent-a100d6968899c7521`, reset to c310b9f22 at the start. Not pushed.

## Commits

| commit | what |
| --- | --- |
| 523cb59fd | `chore(series-parquet): network, DNS, TCP ACK and completion-response fault cases` - the network family in `faults.py` (8 registered faults, `NetworkCase`, `CompletionCohortCase`, `MultipartCompletionCase`), the completion front in `fault-nginx.conf`, the three new rig probes, the values-route health check, 2 harness bug fixes, contract tests, README, `measure failures --family network`. |
| 820c8569b | `fix(series-parquet): a capture of 100 packets is not an empty capture` (Task 8 harness bug that failed the required preflight on MinIO). |
| 1b2b18291 | `chore: re-measure the series fault preflight with the network probes` - `fault-preflight.json` (required mode, 32/32 probes pass, every class available on both stores). |
| 71de92d90 | `fix(series-parquet): DNS fault evidence starts after the arming second; held release counts from the hour end` (rule change after a failure, flagged below). |
| 888311cf1 | `chore: measure series network DNS and acknowledgement faults` - `failure-network.json` and its tree (37 run files incl. 5 superseded, 16 baselines, 1 child index, 14 MB, 0 host paths). |
| 527b0ff1c | `fix(series-parquet): a DNS timeout counts only the engine's own dropped queries` (fix round 1, review finding 1). |
| 42b44ef68 | `chore: re-measure the series dns_timeout cells under the engine-attributed judge` (r003 x4; index 14/32). |

No product code was changed.

## How the cases run

- Rig, engine, producer and settings are the S3 family's: one worker on core 1, harness `taskset -c 0-7,16-23`, producers 8-15,24-31, 5 s windows, `part_bytes` 5 MiB, flush deadline 60 s, abort timeout 5 s, L = 5 + 2 x (60 + 5) = 135 s, the shipped retry section (max_retries 5, retry_timeout 30 s; object_store request timeout 30 s), 20 req/s of 100 x 1 KiB records.
- Each cell's rig first runs its own entry probes (`entry_probes`): disconnect/reset direct, UDP+TCP DNS, xt_bpf+capture, or dropped-completion direct.
- State machine as in Task 9 (`input_started`, `baseline`, `armed`, `observed`, `fault_removed`, `endpoint_healthy`, `resumed`, 20 s input, `input_stopped`, `drained`); the dropped-completion cohort case adds `paused`, `quiet`, and a `_metrics` copy of each fault state.
- `endpoint_healthy` now requires a signed HEAD through the general route (bucket) AND through the values route (a values key; 404 counts as an answer) - carried item 1.
- Captures (tcpdump in the owner namespace, snaplen 128) run from just before arming until both routes answer again.
- New recorded numbers per cell (under `observations.fault.numbers`, Task 9 precedent): NGINX upstream failures by class (refused/reset/prematurely closed/timed out, from the NGINX error log, which the tools image sends to the owner's stderr, now kept as `nginx-error.log`), RST packets, resolver queries/answers, DNS rule drops, pure-ACK drops, retransmissions, matched/pure-ACK frames, completed-unacknowledged objects/bytes, late-commit events, the target completion's timeline, and each object's visibility after its own window end (`block_lateness`).

## Per-case results

Times in s. "held" = fault in place; "recovery" = endpoint healthy to first values file + ack; "drained" = endpoint healthy to drained.

| cell | status | failed checks | held | recovery | drained | offered=acked | stored | dup | flush failures | nacks | late commits | orphans |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| disconnect strict minio | passed | - | 121.4 | 1.67 | 30.7 | 94000 | 103000 | 9000 | deadline 2 | 128 | 0 | 0 |
| disconnect strict rustfs | failed | orphaned_uploads_expected | 124.0 | 0.76 | 28.6 | 83500 | 83500 | 0 | deadline 2 | 128 | 0 | 1 |
| disconnect buffered minio | passed | - | 65.3 | 1.07 | 25.1 | 203000 | 203000 | 0 | deadline 1 | 100 | 0 | 0 |
| disconnect buffered rustfs | failed | orphaned_uploads_expected | 61.0 | 1.08 | 25.0 | 194300 | 194300 | 0 | deadline 1 | 100 | 0 | 1 |
| reset strict minio | failed | orphaned_uploads_expected | 65.7 | 1.08 | 31.9 | 96800 | 96800 | 0 | deadline 1 | 100 | 0 | 54 |
| reset strict rustfs | failed | orphaned_uploads_expected | 65.3 | 1.06 | 31.9 | 96800 | 96800 | 0 | deadline 1 | 100 | 0 | 54 |
| reset buffered minio | failed | orphaned_uploads_expected | 65.3 | 1.06 | 25.0 | 203000 | 203000 | 0 | deadline 1 | 100 | 0 | 54 |
| reset buffered rustfs | failed | orphaned_uploads_expected | 61.0 | 1.09 | 25.0 | 194400 | 194400 | 0 | deadline 1 | 100 | 0 | 1 |
| dns_nxdomain strict minio | passed | - | 65.5 | 1.03 | 31.8 | 96900 | 96900 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_nxdomain strict rustfs | passed | - | 65.5 | 0.99 | 31.7 | 96800 | 96800 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_nxdomain buffered minio | passed | - | 65.3 | 0.80 | 24.9 | 202700 | 202700 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_nxdomain buffered rustfs | passed | - | 65.2 | 0.97 | 25.1 | 202900 | 202900 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_timeout strict minio (r003) | passed | - | 65.5 | 4.27 | - | 109700 | 109700 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_timeout strict rustfs (r003) | passed | - | 65.5 | 0.81 | - | 86200 | 86200 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_timeout buffered minio (r003) | failed | rss_reconciliation | 65.2 | 1.48 | - | 204500 | 204500 | 0 | deadline 1 | 100 | 0 | 0 |
| dns_timeout buffered rustfs (r003) | failed | rss_reconciliation | 65.4 | 0.80 | - | 203600 | 203600 | 0 | deadline 1 | 100 | 0 | 0 |
| tcp_ack_loss strict minio | failed | orphaned_uploads_expected | 35.6 | 2.08 | 31.6 | 106800 | 106800 | 0 | - (1 retry) | - | 0 | 1 |
| tcp_ack_loss strict rustfs | failed | orphaned_uploads_expected | 35.5 | 2.16 | 27.1 | 96600 | 96600 | 0 | - (1 retry) | - | 0 | 1 |
| tcp_ack_loss buffered minio | failed | orphaned_uploads_expected | 35.4 | 1.32 | 25.4 | 143800 | 143800 | 0 | - (1 retry) | - | 0 | 1 |
| tcp_ack_loss buffered rustfs | failed | rss_reconciliation, orphaned_uploads_expected | 35.3 | 1.35 | 24.9 | 143600 | 143600 | 0 | - (1 retry) | - | 0 | 1 |
| dropped_completion_response strict minio | passed | - | 7.1+7.1 | 1.08 | 30.8 | 77800 | 77800 | 0 | - | - | 0 | 0 |
| dropped_completion_response strict rustfs | passed | - | 7.7+7.7 | 0.83 | 29.7 | 76300 | 76300 | 0 | - | - | 0 | 0 |
| dropped_completion_response buffered minio | passed | - | 7.8+7.8 | 0.96 | 22.9 | 72100 | 72100 | 0 | - | - | 0 | 0 |
| dropped_completion_response buffered rustfs | passed | - | 7.4+7.4 | 0.91 | 22.9 | 72200 | 72200 | 0 | - | - | 0 | 0 |
| dropped_multipart_completion strict minio | passed | - | 125.2 | 6.78 | 37.2 | 99600 | 122400 | 12800 | deadline 2 | 228 | 2 | 0 |
| dropped_multipart_completion strict rustfs | failed | late_commit_detected | 65.1 | 6.91 | 37.6 | 99700 | 122500 | 12800 | permanent_storage 2 | 228 | 0 | 0 |
| dropped_multipart_completion buffered minio | failed | rss_reconciliation | 66.0 | 0.97 | 25.0 | 204000 | 224000 | 20000 | deadline 1, permanent_storage 1 | 200 | 1 | 0 |
| dropped_multipart_completion buffered rustfs | failed | late_commit_detected | 36.1 | 1.34 | 24.5 | 145000 | 165000 | 20000 | permanent_storage 2 | 200 | 0 | 0 |
| held_multipart_completion strict minio | failed | orphaned_uploads_expected | 149.7 | 30.06 | 52.6 | 162800 | 162800 | 0 | deadline 2 | 228 | 0 | 2 |
| held_multipart_completion strict rustfs (r002, straddled) | failed | partition_lateness_bound | 155.0 | 1.12 | 27.5 | 88500 | 111300 | 12800 | deadline 2 | 228 | 0 | 0 |
| held_multipart_completion buffered minio | failed | orphaned_uploads_expected | 144.9 | 30.05 | 55.6 | 420000 | 420000 | 0 | deadline 2 | 200 | 0 | 2 |
| held_multipart_completion buffered rustfs | passed | - | 148.9 | 0.05 | 25.3 | 370000 | 390000 | 20000 | deadline 2 | 200 | 0 | 0 |

Producer: strict cells got the storage sentence "could not write to object storage (unavailable)" for every nacked request (100 or 128, the in-flight cap) and resent it with the same bytes; `producer_local_timeouts_count` 0 everywhere. Buffered cells: the buffer scheduled 100-200 retries (1100 in the old dns_timeout r001). Duplicates occur only where a failed block's objects exist (below); strict duplicates all lie in resent requests, buffered duplicates all have a copy in a failed block's files.

dns_timeout r003 buffered cells fail `rss_reconciliation` on one sample each (MinIO 70.8 MB residual vs 44.6 MB tolerance, 1 of 168 samples; RustFS 53.9 MB vs 45.0 MB, 1 of 171); `fault_observed` and every correctness check pass. Not rerun.

Resources: ACTIVE/FLUSHING within 500 MiB, series cache 150/200000, pending slot <=1, buffer within cap, no loss; peak RSS 111-617 MB, accounted peak 10-253 MB, buffer peak up to 521 MB (dns_timeout r001, 240 s fault). `rss_reconciliation` failed in two cells on a single sample each: tcp_ack_loss buffered rustfs (residual 44.7 MB vs 33.6 MB tolerance, 1 of 110 samples) and dropped_multipart_completion buffered minio (57.0 MB vs 56.4 MB, 1 of 164 samples). Not rerun: no host-independent reason.

### State machines, what happened, recovery

- **disconnect** (both route proxies `{"enabled":false}`): direct connections to 19001/19002 are refused (curl exit 7), NGINX answers every write 502 and logs `connect() failed (111: Connection refused)` (59-121 per cell, 0 reset, 0-2 prematurely closed = in-flight connections cut at the disable). object_store retried each 502, the flush retried 8-17 times until the 60 s deadline (`flush.failures{deadline}` 1-2), then nacked; strict producers resent, the buffer rescheduled. Recovery 0.8-1.7 s after re-enable.
- **reset** (`reset_peer`, downstream, timeout 0, both route proxies): the proxy resets the client connection when the store starts answering, i.e. after the request reached the store. Captured 56-62 RSTs; NGINX 502 with `104: Connection reset by peer` (56-62). Refusal, reset and timeout stay separate labels (NGINX error classes; curl exits 7/56/28 in the probe). Recovery ~1.1 s.
- **dns_nxdomain** (endpoint `lake-<run>.test:19000`, read-only resolv.conf `nameserver 127.0.0.1` / `options attempts:1 timeout:1`, dnsmasq `--no-resolv --no-hosts --log-queries --local=/test/ --addn-hosts=/control/hosts --local-ttl=0`): the name is removed and dnsmasq SIGHUPed; no established connection to the front existed to reset (NGINX `keepalive_timeout 0`). The engine's own fresh lookups (61-62 AAAA queries, which the diagnostic dig never sends) got NXDOMAIN (113-117 answers); NGINX logged no engine request during the fault. Deadline failure, nack, retry; recovery 0.8-1.0 s after the name returned. No resolver caching beyond the deadline was seen, so no reconnect-DNS restart was needed.
- **dns_timeout** (OUTPUT DROP udp/tcp dport 53; since fix round 1 the engine's queries go through rules matching its user id, `-m owner --uid-owner 1000`, ahead of the general rules): r003, all four cells: the diagnostic `dig` from the namespace timed out (exit 9) and counted exactly 1 packet on the general UDP rule; read right after it, the engine's rules were at 0, and they then dropped 54-56 of the engine's own UDP queries (TCP 0); the resolver received no query for the name after the arming second; NGINX logged no engine request. Loopback capture: 2-18 DNS packets during the fault window (the OUTPUT-dropped queries never reach the capture point; the few packets are the removal second's recovery lookups). Recovery requires a resolving dig, then a new engine request: 0.8-0.9 s.
- **tcp_ack_loss** (xt_bpf ACK-only rule for the inspected store address:9000, 33-instruction program compiled in the namespace): held 35 s (past the 30 s request timeout, inside the 60 s activation deadline). Rule drops 29-38 pure ACKs; 16-18 retransmissions; 121-201 captured frames match the rule's exact expression and every one is a pure ACK (tcp.len 0, flags 0x010). What happened: each upload in flight stalled (the store's ACKs lost, the sender retransmitted); at 30 s the client gave up on the stalled UploadPart (NGINX 499 x1-2), the attempt failed and the flush retried once into a new upload, which completed after the rule was removed. No flush failed and nothing was nacked. The abandoned upload leaked (F1).
- **dropped_completion_response** (values proxy `{"name":"drop_completion","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}`): input paused, nothing pending anywhere, then a 10-request cohort of one signal in one window. Logs cohort values object 755,595 B, metrics 25,404 B, each a single `put_object` (no multipart initiation); the block had no series object (descriptors already committed). While the response was withheld (7.1-7.8 s per cohort): the object answered 3 direct HEADs 1 s apart unchanged, read back as exactly the cohort (1000 records, record ids and payload hashes, descriptor coverage via the scanner), NGINX logged nothing for it, FLUSHING held 1.17 MB / 0.18 MB, strict producers had 0 of 10 acks, buffered producers had 10 WAL acks but `buffer.bundles.resolved` had not moved. Removing the toxic closed the waiting request (NGINX 502, "upstream prematurely closed", after 2.1-2.8 s), object_store retried it to a 200 PUT of the same key within 0.3 s, and the cohort was acknowledged (resolved). Multiplicity 1 for the 2000 cohort records and for everything outside the cohort.
- **dropped_multipart_completion** (completion front, `drop_completion` on the completion proxy; logs-only input so a block's frozen objects are its logs files): see "Dropped completion and lateness".
- **held_multipart_completion** (completion front, upstream `latency` 600 s on the completion proxy; released 10 s past the later of the writer's cleanup cutoff and L after the block's window end, or after the hour end for an hour's last blocks): see below.

## Rig versus store (carried item 1)

Answer: **the rig.** The `outage stall` probe (in `fault-preflight.json`, and three development runs) stops the store, opens a connection through the values proxy every 5 s as the exporter does, returns the store after 80 s, then every 5 s sends one request straight to the store from the namespace and from the host, one through each proxy and one through each NGINX route.

- MinIO (published run): during the outage the values proxy listener's accept queue grew by one every 5 s (0 -> 14) while exactly one SYN-SENT connection from Toxiproxy to the store existed. After the store returned at 80.4 s, the store answered at once from the namespace (403) and the host (HEAD bucket 200), the general proxy answered, while the values proxy and the NGINX values route timed out, until 130.0 s after the stop (accept queue 15 -> 27). The same shape in the first preflight run: MinIO 135.2 s, RustFS 135.6 s.
- RustFS (published run): no stall; its pending connect had restarted at ~75 s, and the next SYN retransmission reached the returned store.
- Mechanism: Toxiproxy dials the upstream inside its accept loop, so one kernel connect to the stopped store (SYN retries up to ~127 s) blocks every later connection of that proxy; how long it stalls after the store returns depends on where that connect is in its SYN backoff. A client connecting directly would see only its own connect hang. The ~60 s recovery in two Task 9 outage cells is therefore a rig artifact.
- Rig change: `endpoint_healthy` now requires the values route to answer too, so a stalled values proxy can no longer be declared healthy.

## Dropped completion and lateness (carried item 2, amendment)

Target = the first block whose write attempt failed after arming; its logs values file is a 2-part multipart upload (~7.5 MB); `deadline_remaining` of its `flush.attempt_failed` event gives its deadline.

**Dropped response, MinIO** (strict and buffered): the Complete lands; its object is visible 0.59-0.93 s after the block's window end; object_store's request timeout at 30 s triggers its own abort (DELETE 204 on an already completed upload), the attempt fails retryable, attempt 2 re-uploads and completes the same key (LastModified +30.4 s), its response is dropped too, and the flush fails at the 60 s deadline. The cleanup probe HEADs every frozen object, finds them, and logs INFO `series_parquet.flush.cleanup outcome=late_commit` for the target; `flush.late_commits` 2 (strict) and 1 (buffered). **Detector verified on MinIO.** No orphans. Rows of the late-committed blocks are stored again after the nack: 12,800 (strict, all in resent requests, multiplicities 2 and 3) and 20,000 (buffered, all in failed-block files).

**Dropped response, RustFS** (strict and buffered): the Complete lands (object visible at the window end +0.0-0.1 s), then object_store's abort after the 30 s timeout gets **404** from RustFS (the upload is already complete), object_store returns the abort's error, the exporter classifies it `permanent_storage` and ends the flush at once without the late-commit probe: no cleanup event, `flush.late_commits` 0, although every object exists. 12,800 / 20,000 duplicates (attributed). `late_commit_detected` fails - finding T11-F2.

**Held completion, after the writer's deadline, in hour H - how late against L:**

- RustFS holds the connection (no request-header timeout seen, up to 154.9 s): on release, attempt 1's Complete gets 404 (that upload was aborted) and attempt 2's Complete gets 200, and the object publishes.
  - Straddled r002 (armed 05:59:50, block window ending 05:59:50): object of hour 05 first visible **145.0 s after the hour end** (LastModified +145.007 s, first listing +145.236 s) against L = 135 s -> `partition_lateness_bound` violated. That is 155.0 s after its window end, 95.0 s after the writer's deadline and 90.0 s after its cleanup cutoff. The writer's probe had reported `partial` (the series file present, the values file absent), `flush.late_commits` 0. The rows were also resent: 12,800 records stored 2-3 times (attributed).
  - Straddled r001 (superseded, kept): released 10 s past L after the block's window end, which lay 10 s before the hour end: visible 135.006 s after the hour end, a 0.006 s violation - the release rule then counted from the hour end (commit 71de92d90) and the cell was rerun with that purpose.
  - Buffered, mid-hour: visible 145.0 s after the window end (block lateness beyond L), 20,000 duplicates in failed-block files; the partition check passes because the hour was far from its end.
- MinIO drops the held connection 30.25 s after it opened (NGINX logs the Complete 502 after 30.25 s, "upstream prematurely closed"). So a completion held longer than that never lands: no late object, `visible: false` after release; the abandoned second upload stays incomplete (2 orphans per cell, F1).
- Answer to the amendment: with a lost response alone, the object is already visible when the request lands (before the deadline, <1 s after the window end); nothing appears later. A completion the store receives or finishes after the writer gave up publishes whenever the store completes it: on RustFS that was 90 s after the writer's cleanup cutoff and 10 s beyond L for the hour, and nothing in the writer bounds it; on MinIO the store's own ~30 s request timeout bounds it. The late-commit probe cannot see such an object (it looks at the deadline). Evidence for the plan-4 per-writer seal marker.

## Orphaned uploads after every case

Listing `list_multipart_uploads` in the store directly after the engine stopped; expected at most `flush.abort_failures`, which was 0 in all 32 cells.

| case | minio strict / buffered | rustfs strict / buffered | attribution |
|---|---|---|---|
| disconnect | 0 / 0 | 1 / 1 | parts answered 502 when the proxy was disabled mid-upload, no abort (F1) |
| reset | 54 / 54 | 54 / 1 | 54 = exactly the 54 CreateMultipartUpload requests answered 502: the store created the upload, its response was reset, the writer never learned the upload id (T11-F1); the RustFS buffered 1 is a part 502 (F1) |
| dns_nxdomain, dns_timeout | 0 | 0 | requests never left the engine |
| tcp_ack_loss | 1 / 1 | 1 / 1 | the upload of the attempt whose UploadPart stalled and timed out (499), no abort (F1) |
| dropped_completion_response | 0 | 0 | single PUT |
| dropped_multipart_completion | 0 / 0 | 0 / 0 | the completed upload |
| held_multipart_completion | 2 / 2 | 0 / 0 | MinIO: the abandoned uploads whose held Complete the store dropped (F1); RustFS: released Completes completed them |

## Multipart evidence per store

`multipart_exercised` passed in all 32 cells: CreateMultipartUpload, UploadPart (2+) and CompleteMultipartUpload answered 2xx in the route trace, and objects above 5 MiB with multipart ETags in a direct listing. MinIO: 6-14 completed uploads per cell, parts per object 2-15. RustFS: 6-11 per cell, 2-15 parts.

## Findings for Task 12

**T11-F1 (S3 protocol ambiguity, not an exporter correctness defect): a lost CreateMultipartUpload response hides the upload it created.** In the reset cells the store creates the upload and the response is reset; the client never learns the upload id, so no handle-based abort is possible (the sink already documents this for a creation dropped in flight: `crates/series-lake/src/sink/write.rs` ~348, `Creations`). object_store retries Create, so each lost Create response leaves one upload. Seen: 54 orphans in 3 of 4 reset cells, equal to the 54 Create 502s. Remedy (operational): document it next to F1 (FORMAT.md section 4, README), require a bucket `AbortIncompleteMultipartUpload` lifecycle rule, and count uncertain Create failures; Task 12 folds that counter into the F1 item. An exact-key `ListMultipartUploads` sweep (frozen names carry writer and boot ids) is optional S3-specific hardening for the backlog: it needs an API and permission outside the generic object-store path and does not replace the lifecycle rule. Reproducer: `measure failures --family network --option 'only_cells=["reset-strict-minio"]'`.

**T11-F2 (product defect, Task 12): after a failed Complete, RustFS's 404 on the abort replaces the ambiguous completion error, and the committed block is not probed.** In finalization (phase 2, `crates/series-lake/src/sink/write.rs` `finish?`) object_store aborts the upload after CompleteMultipartUpload failed (here: timed out with its response lost). RustFS answers the abort of the already completed upload 404, and object_store returns that abort error instead of the original ambiguous completion error. `NotFound` is non-retryable (`crates/series-lake/src/error.rs` ~210, `transient_store_error`), so `Settle::after` (`crates/core-nodes/src/exporters/series_parquet_exporter/flush.rs` ~471) selects `Nothing` instead of `Probe`: no HEAD of the frozen names, no late-commit event, `flush.late_commits` 0, and the flush ends at once as `permanent_storage`. The block that is in fact committed is nacked, its requests are resubmitted into a later block under a new file name, and its rows are stored twice (12,800 strict, 20,000 buffered; attributed). MinIO answers the same abort 204 and the probe runs. Seen in both RustFS dropped_multipart cells, and once in dropped_multipart buffered MinIO (`permanent_storage 1`). Remedy: a phase-2 abort answered NotFound (and any phase-2 error that carries an abort failure) means "possibly committed": HEAD-probe the frozen paths.

**T11-F3 (inherent end-to-end store property; documentation item, not a Task 12 fix): an abandoned completion can publish beyond L.** Once an intermediary (a proxy, a load balancer, the store's own front end) keeps a request after the client cancelled it, the exporter cannot impose a remote completion deadline. Measured: 145.0 s after the hour end on RustFS (L = 135 s), 90 s after the writer's cleanup cutoff, invisible to the probe (`partial`). MinIO bounds it by dropping the held request after ~30.25 s; RustFS does not bound it. FORMAT.md (`crates/series-lake/docs/FORMAT.md` ~699) already states the exception; the documentation must also say that compactors cannot treat L as a completeness bound without a store-enforced request limit or the planned per-writer seal marker (plan 4).

**F1 (open): reproduced in 12 cells:** disconnect RustFS x2, reset RustFS buffered, tcp_ack_loss x4, held MinIO x2 (2 each), plus the Task 9/10 shapes. New trigger: a stalled UploadPart that times out under TCP ACK loss.

**T11-O1 (observation): partial cleanup outcome.** When a block's series file exists and its values file does not, the probe logs `partial` and counts nothing; in the held cases the values file then appeared later.

**T11-O2 (observation): dns_timeout's 2x lateness in action.** In the old dns_timeout r001 runs (240 s faults) a block that waited for the flush slot was visible 111-120 s after its window end, inside L = 135 s.

**T10-F1, T10-F2:** not triggered (no restarts in this family).

## Rule changes after seeing a failure (flagged)

0. **dns_timeout judge** (fix round 1, 527b0ff1c, from the codex review): the condition accepted any positive DROP counter, which the harness's own diagnostic dig also increments, so it did not prove that the engine's lookup hit the rule. The engine's queries are now dropped by rules matching its user id (the engine container runs as the invoking user, every tool in the namespace as root; `xt_owner` present), the counters are read right after the diagnostic lookup, and the condition needs a later drop on the engine's rules. Contract test `test_dns_timeout_needs_the_engines_own_dropped_query`: only the dig hitting the rules fails, an engine drop after the snapshot passes (the old judge passed the former). The r002 run files hold no post-dig reading, so the four cells were rerun (r003, purpose "judge fix from review" in the index); r002 stay in the tree. Verdicts after the fix: `fault_observed` passes in all four (engine drops after the dig 54, 54, 54, 56); status strict MinIO passed, strict RustFS passed, buffered MinIO and buffered RustFS failed on `rss_reconciliation` only.
1. **DNS window** (71de92d90): the dns_timeout condition "the resolver received no query for the name" counted from the arming second itself; dnsmasq logs whole seconds, and all four r001 cells failed on lookups answered with an address in the arming second, before the DROP rules existed (an answered query cannot have passed the rules). Only seconds wholly after arming now count, for both DNS faults. Contract test `test_fault_dns_evidence_starts_after_the_arming_second`. The 4 cells were rerun once with this purpose recorded in the index (r002, all pass); r001 stay in the tree.
2. **Held release instant** (71de92d90): not a verdict rule but the case's schedule: release 10 s past L after the partition hour end for an hour's last blocks (r001 released from the window end, 10 s earlier). Contract test `test_held_release_counts_from_the_hour_end_for_its_last_blocks`. The straddled cell was rerun once with the purpose recorded.
3. **TCP ACK hold 35 s** (in 523cb59fd, before the published matrix): decided after a development run that proved drops in 6.8 s and saw no effect on the writer; the hold lets a stalled request meet the client's own 30 s timeout. It strengthens the observation and exposed F1.

Harness bugs fixed (each with a contract test failing on the old code): tcpdump's two start lines in one burst were missed by a buffered readline (`test_capture_start_sees_both_lines_of_one_burst`); capture counts and the resolver log were read from output clipped to 4000 characters (`test_capture_counts_read_the_whole_output`); a capture of 100 packets matched the substring "0 packets captured" (`test_only_a_zero_count_is_an_empty_capture`, 820c8569b).

## Deviations for the record

- `fault-nginx.conf` changed (not in the brief's file list): a second server, the completion front on 19010, routes only values CompleteMultipartUpload requests to a third proxy `completion` (19003) and sets `proxy_ignore_client_abort on`. The main front on 19000 is byte-identical in behaviour. Needed to produce a CompleteMultipartUpload with a lost response (the values proxy's toxic would drop CreateMultipartUpload's response first) and to hold one past the writer's give-up (Toxiproxy discards held data once NGINX closes, verified).
- Two cases beyond the brief's six: `dropped_multipart_completion`, `held_multipart_completion` (item 2 / amendment). They use a logs-only workload (only request 0 is metrics) so a block's frozen objects are its logs files.
- `dropped_completion_response` runs logs and metrics cohorts in one cell (states `_metrics`); the paced producer is paused for them (`CompletionCohortCase.produce`).
- The brief's long test reads `result["metrics"]`; per Task 9 precedent the numbers live in `observations.fault.numbers` (`dropped_pure_ack_packets_count`, `tcp_retransmissions_count`).
- DNS timeout: tcpdump cannot see queries dropped in OUTPUT on loopback (capture point after netfilter), and NFLOG would need a host module; unanswered queries are proven by the rule counters, the timed-out diagnostic dig, the resolver's empty log and NGINX's silence.
- The engine's own error text is truncated in its log (`error=parquet: External: Gene[...]`), so failure classes are read from NGINX and the resolver, not from the engine.
- Preflight chain: the first two preflight attempts failed on harness problems (the capture substring bug; then a stale cidfile from reusing the output directory). I restored the committed index before the passing run so the chain is current -> previous committed index; the two failed indexes are kept in `<repo>/.measurement-artifacts/fault-preflight-t11-failed/`.
- The engine binary was built in this worktree (release, `series-parquet,aws,durable-buffer`, sha256 prefix 1b4285a6); the main checkout's binary was older than Task 10's.

## Tests

- `test_failures`: 108 tests, all OK (the live rig and two long tests skip without their flags). New class `NetworkCaseContracts` (20 tests) plus `NetworkFailureTests.test_tcp_ack_loss` (long, brief's test adapted). Updated: NGINX body test, direct-probe coverage test.
- `measure fault-preflight` required mode: 32/32 probes pass.
- Red-first: the long test was not run red-first through unittest; the same cells ran through `measure failures`. Contract tests were written alongside.
- markdownlint clean; sanitycheck reports only pre-existing `docs/superpowers` non-ASCII files.

## Evidence

- Index: `docs/superpowers/reports/series-parquet-measurement/failure-network.json` (status failed, 14/32), child `failure-network-0f065e3e0cd9.json` (the previous index, which keeps `failure-network-2f1252fc0f6e.json` and `failure-network-1635a4f7180a.json`), 37 run files `failure-network-<case>-<topology>-<store>-c1-w5-r00N.json` (r001 of the four dns_timeout cells and of the straddled held cell superseded, purposes in the index), 16 baselines.
- Preflight: `docs/superpowers/reports/series-parquet-measurement/fault-preflight.json` (outage stall probe under `probes`).
- Raw archives (engine logs, NGINX access and error logs, dnsmasq logs, pcaps, ledgers; no Parquet): `<repo>/.measurement-artifacts/failure-network/*.tgz` (37, 691 MB). Development archives: `.measurement-artifacts/failure-network-dev/`.
- Family state: `/var/tmp/series-failure-network/failures-state.json`; run directories deleted.
- Logs and tables: `<scratchpad>/t11/` (`matrix.log`, `straddle1.log`, `reruns.log`, `preflight3.log`, `tables-final.md`).

## Concerns

1. The index is failed (14/32): product findings F1 and T11-F2, inherent-property evidence T11-F1 and T11-F3, and four single-sample RSS band excursions (two of them the dns_timeout buffered r003 cells); no rerun without a host-independent reason.
2. Three rule/schedule changes after seeing results (above); reviewers should confirm the DNS-second reading and the held release schedule.
3. The completion front changes `fault-nginx.conf`'s hash recorded by the preflight; Task 13 should use it for the completion boundary.
4. T11-F2's classification path is inferred from the NGINX trace (abort 404) and the engine's `permanent_storage` class; the engine log truncates the error text.
5. The rig-vs-store stall is timing-dependent (reproduced 3 of 4 store runs); the mechanism is Toxiproxy's accept loop, inferred from accept-queue growth with one SYN-SENT socket, not from Toxiproxy's source.
