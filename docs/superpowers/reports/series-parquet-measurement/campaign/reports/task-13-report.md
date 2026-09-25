# Task 13 report: buffered topology, answered from existing evidence and source

## Status

STOPPED by user decision: the budget was cut to 20 minutes, and nothing new is measured.

- No measured run was published.
- No commit was made. The worktree was reset to 54a603fd0, and all harness edits were discarded (`git status` is clean).
- The unfinished harness work is saved outside the repository, for the plan-4 backlog:
  - `scratchpad/t13/harness-wip.patch`: the buffered family in `faults.py`, `measure buffered`, the README section and the `capacity.py` extra;
  - `scratchpad/t13/test_buffered.py.wip`.
- A background development batch was stopped mid-run. Its rig and store containers were removed and its run directories deleted.

Every figure below comes from committed run files in `docs/superpowers/reports/series-parquet-measurement/` or from the task reports, unless it is marked as a development run.

## 1. Buffered producer ACK latency (existing run files)

Each run file records ACK latency from the ledger: first send to the successful response, resends included. A file keeps only p50, p99 and max. The per-request samples are not published.

| Source | File(s) | Samples (acked requests) | p50 | p99 | max |
|---|---|---|---|---|---|
| Task 7 buffered soak (MinIO, 1 worker, 15 s window, 72.8k rec/s, 30 min) | `soak-buffered-buffered-minio-c1-w15-r003.json` (metrics) | 133,588 (task-7 report) | 0.023 s | 0.869 s | not recorded |
| PR-tier buffered soak (1 s window) | `pr-soak-buffered-buffered-minio-c1-w1-r001.json` | 1,300 | 0.009 s | 0.021 s | 0.038 s |
| Task 5 buffered MinIO, 4 workers, 1 s window (winner trials) | `capacity-mixed-1k-hot-buffered-minio-c4-w1-r194.json` … `r202.json` | not recorded in the file | 0.07-0.18 s (sustainable trials); 0.57-2.08 s near the ceiling (r195-r197) | 0.73-2.02 s; 3.7-4.4 s near the ceiling | not recorded |
| Task 9 store_outage buffered (5 s window, 20 req/s, a 79-140 s outage) | `failure-s3-store_outage-buffered-minio-c1-w5-r002.json`, `…-rustfs-c1-w5-r003.json` | 3,536 / 3,524 | 0.008 / 0.009 s | 0.021 / 0.035 s | 0.170 / 0.211 s |
| Task 9 slow / http503 buffered | `failure-s3-{slow,http503}-buffered-{minio,rustfs}-c1-w5-r001.json` | 1,037-2,442 | 0.008-0.010 s | 0.020-0.044 s | 0.070-0.123 s |
| Task 11 network buffered (16 cells) | `failure-network-*-buffered-*-c1-w5-r00N.json` | 721-4,200 | 0.008-0.019 s | 0.018-0.050 s | 0.035-0.235 s |
| Task 10 process buffered (kill and restart) | `failure-process-*-buffered-*-c1-w5-r00N.json` | 814-1,025 | 0.008 s | 0.03-0.51 s | 0.53-1.03 s |

In the Task 10 cells, p99 and max include the resend across the engine restart.

What the evidence shows:

- The buffered ACK stays at about 8-10 ms (p50) and 20-50 ms (p99) at 20 requests/s.
- It does not move during a store outage, a store slowdown, HTTP 503 errors or network faults.
- It reaches seconds only when the WAL device saturates (Task 5, near the 152k ceiling).
- No 15 s against 120 s window comparison exists.
- No committed matched strict/buffered cohort exists at the brief's 2 requests/s of 10 records.

A development run, not published, at the brief's 15 s strict shape with a 5 s timeout and resends (`scratchpad/t13/dev/runs/proof-compat15-strict-minio-c1-w15-r001.json`):

- 60 requests;
- 66 producer timeouts; 40 first attempts censored;
- ACK latency p50 7.99 s, p99 26.0 s;
- 400 of 600 records stored 2-5 times.

This is the evidence that strict mode does not work for an Alloy-like producer with a 5 s timeout.

## 2. Freshness (producer send to values object visible)

**Healthy state:**

- Task 7 buffered soak, 15 s window: p50 4.55 s, p95 7.77 s, p99 8.29 s, max 16.5 s.
  - Measured from the WAL acknowledgement to the object being visible.
  - The run file keeps only `freshness_p99_s` 8.29; the other figures come from `task-7-report.md`.
- Task 5 buffered, 1 s window: p50/p99 0.74/1.54 s (local) and 0.93/1.91 s (MinIO), from `task-5-report.md`.

**During and after an outage:** not derivable.

- The Task 9 buffered store_outage run files do not record per-request send times against the commit times of the stored objects.
- They keep only `throughput_by_phase`, the state instants and the object counts, so no per-request freshness can be computed.
- Coarse bound from the state instants: requests acknowledged when the outage started become visible no earlier than the outage duration plus recovery.
  - MinIO r002: outage 140.1 s, recovery 1.3 s, 333 MB of buffer backlog.
  - RustFS r003: outage 79.0 s, recovery 60.8 s (the Toxiproxy stall of the rig, Task 11), 320 MB of backlog.
- The backlog drained within `endpoint_healthy_to_drained` of 25.5 s and 84.9 s.

**What is missing:** a per-request (send, object LastModified) table in the outage cells.

## 3. Loss paths after the WAL acknowledgement (source only)

**Exporter permanent refusal after the WAL ack:**

- Location: `crates/core-nodes/src/processors/durable_buffer_processor/mod.rs:1506-1525` (`handle_nack`).
- On `nack.permanent`, the buffer increments `bundles.resolved{outcome="permanently_rejected"}` (`bundles_for(BundleOutcome::PermanentlyRejected)`, `:1512`).
- It logs WARN `durable_buffer.bundle.rejected_permanent` with the reason (`:1517`).
- It then calls `handle.reject()` (`:1524`): no retry, and the data is dropped.
- The producer already received OK, so this loss is invisible to it.
- A bundle that fails conversion is rejected the same way (`:1426-1430`, `durable_buffer.bundle.conversion_failed` and `read_errors`), but with no resolved{permanently_rejected} increment on that path.
- The exporter side counts its refusal in `nacks{error.type=request_too_large|...|unsupported|invalid}`.
- Not measured: a development run with a 64 KiB request limit, `unsupported: reject`, a traces request and a non-UTF-8 body did not finish cleanly before the stop, and is not evidence.

**drop_oldest:**

- Location: `crates/quiver/src/engine.rs:749-763` (`ingest`, while over the soft cap), which calls `force_drop_oldest_pending_segments` (`engine.rs:1599-1690`).
- That function marks the segments `ReclamationReason::DropOldest` (`:1684`) and logs `quiver.segment.drop`.
- On deletion, `maintain` counts `force_dropped_segments` and `force_reclaimed_bytes` (`engine.rs:1924-1929`).
- The buffer reports:
  - `processor.durable_buffer.loss.bundles` and `.bytes{reason=drop_oldest}`;
  - `loss.items{signal,reason=drop_oldest}`;
  - `reclaimed.segments` and `.bytes{reason=drop_oldest}`.
- These come from `durable_buffer_processor/mod.rs:808-835` (`retention_loss_delta`, `:261`).

**max_age:**

- Location: `crates/quiver/src/engine.rs:1734-1850` (`cleanup_expired_segments`), run from `maintain` (`:1917`) on every timer tick. It marks the segments `ReclamationReason::Expired` (`:1848`) and logs `quiver.segment.drop`.
- `maintain` counts `expired_segments` and `expired_reclaimed_bytes` (`:1930-1935`).
- WAL replay also skips expired entries (`engine.rs:1025`, `skipped_expired`), adds them to `expired_bundles` (`:1170-1178`) and logs INFO `quiver.wal.replay` with `skipped_expired`.
- The buffer reports `loss.*{reason=expired}` and `reclaimed.*{reason=expired}` (`mod.rs:819-842`).
- The cutoff is measured from segment finalization (`config.rs` `max_age` doc), not from the telemetry timestamps.

## 4. T10-F2 cause (source; not verified by measurement)

**Step that leaves acknowledged entries replayable:** the segment is made durable before the WAL cursor, as two separate persistence steps with a window between them.

- Every timer tick calls `engine.flush()`, which finalizes the open segment whatever its age.
  - Timer tick: `durable_buffer_processor/mod.rs:1168`, every `poll_interval`, default 100 ms (`config.rs:34-36`).
  - `engine.flush()` calls `finalize_segment_impl()` (`quiver/src/engine.rs:1254-1255`).
- `finalize_segment_impl` then runs, in order:
  1. It writes the segment directly at its final path (`segment/writer.rs:187`, `File::create`, no temporary name).
  2. It calls fsync (`:218`), then fsyncs the parent directory (`:224`). Call site: `engine.rs:1336`.
  3. Only then does it persist the WAL cursor ("Step 5", `engine.rs:1398-1411`). That step goes through `write_cursor_sidecar`/`record_wal_position` (`wal/writer.rs:1471-1503`) and `CursorSidecar::write_to`: a temporary file, `sync_data`, rename and a directory fsync (`wal/cursor_sidecar.rs:205-226`).
- A SIGKILL after step 2 and before the rename in step 3 leaves a valid finalized segment and a stale cursor.
- On restart the segment scan recovers that segment, and `replay_wal` replays from the old cursor (`engine.rs:863-946`, `iter_from(cursor_position)`). The same entries are then re-ingested into a new segment, and both copies are delivered.
- This matches the Task 10 log (`quiver.segment.scan recovered 110 segments`, then `quiver.wal.replay replayed_count=2`, then both copies stored by engine 3).

**Interval that bounds the window:**

- Per kill, the entries exposed are those appended since the previous finalization: at most one `poll_interval` (100 ms) of input. That was about 2 requests at 20 requests/s, matching the 2 replayed entries.
- The vulnerable interval itself runs from the segment's write to the cursor's rename: two fsyncs plus the directory fsync and the sidecar's `sync_data`.
- So an acknowledged entry can be delivered twice only if the kill lands within roughly (the time from its acknowledgement to the next tick, up to 100 ms) plus (that finalization's fsync time, a few ms to tens of ms on this NVMe).

**The other duplicate window after a kill** (records already stored, replayed again) is ordinary at-least-once:

- The exporter's ACK only reaches Quiver's subscriber progress in memory.
- It becomes durable on the next `maintain` → `flush_progress` (`mod.rs:1285`, `engine.rs:1899-1915`). That write goes through a temporary file, fsync and rename (`subscriber/progress.rs:539-611`), also once per 100 ms tick.

**Ingest/ACK ordering** (for the "OK means on the local WAL" contract):

- The producer ACK is sent after `engine.ingest` returns (`mod.rs:1106-1109`).
- `ingest` appends the WAL entry (`wal/writer.rs:567-635`) through a tokio `write_all`.
- The WAL is fsynced only when at least `flush_interval` (default 25 ms, `quiver/src/config.rs:227`) has passed since the last fsync (`wal/writer.rs:858-876`).
- So an OK means the entry was written to the WAL file (it survives a process kill), not that it was fsynced (it may not survive power loss within about 25 ms).

**Status: unverified by a published measurement.** One unpublished development run matches it: `scratchpad/t13/dev/runs/proof-midwindow15-buffered-minio-c1-w15-r001.json`, MinIO, 15 s window, SIGKILL 28 ms after the log acknowledged the cohort's last request.

- Engine 2 logged `recovered 10 segments` and `wal_replayed 1`.
- Exactly that request (10 records) was stored twice by engine 2.
- It had no copy before the kill and was never resent.
- Everything else was stored once.
- The same run passed every other check: no resend, no missing acknowledged record, and the replay written about 8.9 s after the restart.

A second unpublished development run (`proof-graceful15-buffered-minio-c1-w15-r001.json`) reproduced T10-F1 at the latency workload's scale:

- It failed `replay_only_eligible`.
- 10 records acknowledged and stored before the graceful stop were stored again by the next boot.

## Left for the plan-4 backlog (not done)

- Strict and buffered latency cohorts at 15 s and 120 s (600 requests each, both stores).
- The mid-window kill and restart with no sender on both stores and both windows, as published runs.
- The kill-series measurement of the T10-F2 window as numbers.
- The NACK/backoff envelope proof through an outage.
- The per-request freshness during and after an outage.
- The buffer's heap, mapped and disk decomposition.
- The ambiguous-completion kill (logs and metrics, both stores).
- The permanent-refusal and drop_oldest/max_age measurements.
- The Alloy 5 s compatibility control.
- The harness for all of these exists as the WIP patch above, and its fast contract tests passed. It was not committed, per the user decision.

## Evidence paths

- Committed:
  - `docs/superpowers/reports/series-parquet-measurement/`: `soak-buffered-buffered-minio-c1-w15-r003.json`, `pr-soak-buffered-buffered-minio-c1-w1-r001.json`, `capacity-mixed-1k-hot-buffered-minio-c4-w1-r194..r202.json`, `failure-s3-*-buffered-*`, `failure-process-*-buffered-*`, `failure-network-*-buffered-*`;
  - `.superpowers/sdd/2026-09-22-series-parquet-measurement/task-5-report.md`, `task-7-report.md`, `task-10-report.md`.
- Unpublished development runs: `<scratchpad>/t13/dev/runs/`.
- WIP harness: `<scratchpad>/t13/harness-wip.patch` and `test_buffered.py.wip`.
