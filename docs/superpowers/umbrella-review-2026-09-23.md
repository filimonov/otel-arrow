# Review report: 5588c3e0d590d4df0303697a6862c8324235a310..HEAD (series-parquet-exporter)

## Summary

The range is the whole series_parquet campaign: a new engine-independent `series-lake`
crate, a new opt-in `series_parquet` exporter, a schema-aware OTLP framing validator
adopted by four exporters, an engine-wide jemalloc default, a 30k-line Python
measurement and fault harness, and a 150 MB tree of raw measurement JSON.
About 300 commits, 1173 files, 5.29M inserted lines, of which 86k are source.

The exporter's core delivery state machine is in good shape: thirteen seats checked the
ack-only-after-durable rule, cancellation points, the upload ledger, block seal
atomicity, cache commit ordering and the byte-view/validator parity, and found them
sound. Three seats independently found one real defect in the shutdown drain (a
release-mode `assert!` that panics and drops every held completion), one seat found
the latched shutdown deadline collapsing to one second when the upstream channel
closes, and one seat found an unbounded memory amplification in the attribute-table
decoder. The stricter OTLP validation is a behaviour change for three existing
exporters that the changelog does not disclose and that reverses an earlier upstream
decision on invalid UTF-8. The branch cannot go upstream in its current shape because
of the evidence tree, the process docs and the unrelated engine-wide changes bundled in.

Verdict: request changes. Three code defects must be fixed, the UTF-8 policy for the
existing exporters must be decided and disclosed, and the history must be split and
rewritten before any upstream PR.

## Reviewed scope

- Diff: `5588c3e0d590d4df0303697a6862c8324235a310..HEAD` (HEAD = c82d0520a)
- Base: 5588c3e0d, which is also the merge-base with `main`
- Files changed: 1173 (1062 of them evidence JSON; ~111 source files)
- Main moving parts:
  - `crates/series-lake/`: extraction, canonical series id, buffer/sort/merge, Parquet sink, config, FORMAT.md, goldens, oracle proptest, benches
  - `crates/core-nodes/src/exporters/series_parquet_exporter/`: factory + select loop (mod.rs), two-block worker, flush tasks, completion tokens, windows, metrics, README, 6.1k-line test file
  - `crates/pdata/src/views/otlp/bytes/validate.rs` + byte-view fixes: `OtlpProtoBytes::validate_framing`, adopted by file, parquet, otap and series_parquet exporters
  - `crates/otap/src/object_store.rs`: shared object-store wiring, `required_token_provider`, event rename
  - `crates/engine/`: series RSS residual in engine_metrics.rs, `ProcessorInbox::shutdown_deadline`, jemalloc probe
  - `src/main.rs`: `malloc_conf = background_thread:true` for every glibc jemalloc build
  - `crates/validation/tests/series_parquet/`: E2E (Alloy, MinIO, RustFS, ClickHouse, DuckDB), fault rig, measurement/memory/performance harness, 6k-line harness self-tests
  - `configs/`, `.chloggen/` (7 entries), `.github/workflows/series-parquet-e2e.yml`
  - `docs/superpowers/`: spec, plans, review notes, and `reports/series-parquet-measurement/` (150 MB)

Seats run: 1 UX, 2 architecture, 3 YAGNI, 4 security, 5 performance, 6 docs, 7 quality,
8 operability, 10 concurrency, 11 tests, 12 compatibility, 13 lifetime/panic, 14 repo,
15 deep audit. Seat 9 (C++ headers) skipped as not applicable. No seat built or ran
code; findings below marked "verified" were re-checked by the aggregator against the
worktree.

## Blockers

1. Force-drained shutdown refusals can exhaust notifier credit; a later block-token push hits a release-mode `assert!` and the exporter panics mid-drain
   Risk score: 85
   Sources: seats 10, 13, 15 (independent)
   Files/lines: `series_parquet_exporter/token.rs:394-402` (assert in `push_with`), `token.rs:511-533` (`force_shutdown`), `worker.rs:1096-1098` (`complete`), `worker.rs:1414-1450` (`abandon`), `mod.rs:473-479`
   Evidence (verified): `force_shutdown` queues a refusal whenever `self.len() < self.capacity`, counting only the notifier's queue plus the sending slot. It ignores tokens still held by the ACTIVE block, the FLUSHING job and the parked request, which `has_credit(live_tokens())` admitted up to `capacity - 1`. `push_with` then asserts `self.len() + 1 + reserved <= self.capacity` ("worker must reserve completion credit"), with `reserved = 1` for Ack/Storage and 0 for Shutdown. The doc comment says the one spare slot covers both "the shutdown decision of a completion the worker already holds, or a force-drained refusal", which a single slot cannot.
   Repro sketch: `max_requests_per_block = 1` (capacity 2); admit one request that rotates into a slow write; completion channel has room for one message; buffer three pdata, then Shutdown. Refusal 1 is sent, refusal 2 stalls in the sending slot (len 1), refusal 3 is queued (len 2). Flush succeeds: `complete()` asserts 2+1+1 <= 2 and panics. At the deadline `abandon()` asserts 2+1 <= 2 and panics. With default N=4096 and both blocks near full after an outage, two stuck forced refusals suffice.
   Impact: `AckToken` has no Drop fallback, so every held token is dropped undecided. Producers under `wait_for_result` hang until their own timeout; the engine sees a node failure instead of a bounded drain. This breaks the "every token resolved exactly once" invariant the design rests on.
   Proposed fix: pass the count of tokens held outside the notifier (active + flushing + parked) into `force_shutdown`; queue only while `len() + held + 1 < capacity`, else `deliver_now`. Make a Shutdown push over the bound fall back to `deliver_now` and count a failure rather than assert. Add a regression test: one request admitted and flushing, a stalled completion channel, at least two force-drained pdata, then release the write and separately let the deadline fire; assert no panic and exactly one decision per request. No existing test combines held block tokens, force-drain and a full completion channel (`saturated_inbox_shutdown_stays_bounded` holds no block tokens; `blocked_completion_keeps_boundary_and_control_live` force-drains nothing).

2. A drain that ends on a closed pdata channel replaces the latched shutdown deadline with `now + 1s`, so the exporter nacks blocks it was granted 60s to commit
   Risk score: 80
   Sources: seat 10
   Files/lines: `crates/engine/src/message.rs:342-348, 405-426` (pre-existing engine behaviour); `series_parquet_exporter/mod.rs:376, 470-487, 515-517`; `worker.rs:539-544, 1349-1358`
   Evidence (verified): `recv_with_policy` probes a closed pdata channel eagerly when `!accept_pdata` and returns `closed_pdata_shutdown()`, which discards `pending_shutdown` and synthesizes `Shutdown { deadline: now + 1s, reason: "pdata channel closed" }`. The new engine test `exporter_deadline_is_visible_during_forced_drain` (message.rs:1395) pins exactly this. Once the exporter has seen the latched deadline D, `worker.accept()` is false, so the loop calls `recv_when(false)`, hits the probe as soon as upstream drops its sender, and `worker.shutdown(now+1s)` keeps `old.min(deadline)`.
   Interleaving: FLUSHING block writing to slow S3 and non-empty ACTIVE; admin shutdown latches D = now+60s; a force-drained pdata sets `worker.deadline = D`; upstream exits and drops the sender; next `recv_when(false)` returns the synthesized Shutdown; one second later `abandon()` nacks both blocks `NodeShutdown`. The same happens with no backlog if admission was already closed by backpressure when Shutdown latched.
   Impact: the documented drain bound (window + 2x(flush_retry_deadline + abort_timeout), 145s at defaults) is not honoured under load or slow storage; the exporter nacks data it could have committed, producers must resend, and the admin-API shutdown timeout the README recommends has no effect.
   Proposed fix: in the engine, make `closed_pdata_shutdown` return the latched `pending_shutdown` (with its deadline) whenever `shutting_down_deadline` is `Some`. Add an exporter test where the flush is still in flight more than 1s after `drop(pdata_tx)` and is still acknowledged (`shutdown_commits_both_blocks_before_deadline` heals storage immediately after the drop and never exercises this).

3. The attribute-table budget does not bound decoded CBOR trees; dictionary-encoded `ser` cells are re-decoded per row and scalars charge zero, so one 1 MiB cell can expand to gigabytes
   Risk score: 78
   Sources: seat 7
   Files/lines: `crates/series-lake/src/attrs.rs:224-229, 277-278, 317-327`; `value.rs:87-112` (`decode_cbor`); `crates/pdata/src/schema/payloads.rs:196-202` (`ser` is `Dictionary<U16, Binary>` in the OTAP schema)
   Evidence (verified): `from_batch` does `let value = any.value_at(row, limits)?; total += payload_bytes(&value)` per row. `payload_bytes` counts only string and byte content: `Value::Null | Int | Double | Bool => 0`, and no per-node overhead. Map/slice cells go through `bytes_cell(a, row, |b| decode_cbor(b, limits))`, decoding the shared dictionary value again for every row that references it. `decode_cbor` bounds only the encoded length (`max_cell_bytes`) and depth; not node count.
   Impact: a 1 MiB `ser` value holding ~1M small ints decodes to ~32 MB of `Value` nodes per row and charges 0 to the table budget. With N rows referencing it, N=100 is ~3 GB before any `Budget`, row or block limit applies, plus N MiB of CBOR parsing CPU. Without dictionary encoding the amplification is still ~30x (16 MiB request to ~512 MB). Reachable from any OTAP-Arrow-producing upstream (OTAP receiver, converting processor); the OTLP-only path still has the ~30x case.
   Proposed fix: charge the decoded footprint to the table budget (`size_of::<Value>()` per node plus content), or at minimum `max(encoded len, payload)` per row; decode each distinct dictionary key once and share via `Arc` or memoize by dictionary index. Add a test mirroring `a_large_dictionary_value_is_refused_before_it_is_expanded` with a `ser` array of ints.

## Major issues

4. `validate_framing` refuses invalid UTF-8 and repeated singular fields in three existing exporters, reversing the #2181 lossy-UTF-8 policy, undisclosed in changelog and contradicted by the README
   Risk score: 72
   Sources: seats 1, 12, 6, 7, 2
   Files/lines: `crates/pdata/src/views/otlp/bytes/validate.rs:21, 26-30, 631-637`; `otap_exporter/mod.rs:745-760`; `parquet_exporter/mod.rs:314-331`; `file_exporter/mod.rs:376-389`; `series_parquet_exporter/README.md:1254-1256`; `worker.rs:28`; `.chloggen/pdata-validate-nested-otlp-framing.yaml`, `engine-inbox-deadline-and-otlp-framing.yaml`
   Evidence (verified): `Field::Str => if std::str::from_utf8(&buf[start..end]).is_err() { return Err(fail("invalid UTF-8 in a string field", at)); }`. Commit 719328d88 (#2181) introduced `binary_to_utf8_lossy` specifically because dropping a whole batch over one bad string was "unacceptable". The otap exporter now nacks such a body permanently, parquet drops it with a WARN, file refuses it. The README still says "string fields are not checked for UTF-8 there". The changelog entries list only wire types, ragged packed fields and nesting, and file the change as `bug_fix`/`enhancement` under `engine`.
   Impact: silent data loss for existing deployments whose producers emit non-UTF-8 strings (Go pdata marshalling does not validate UTF-8); repeated singular fields are spec-legal and were previously exported first-wins. UTF-8 is content, not framing.
   Proposed fix: decide the policy explicitly. Recommended: keep the UTF-8 check out of the validation the file/parquet/otap exporters use (a `validate_framing` variant or option), preserving #2181; let series_parquet opt into strictness. Whatever is chosen, list every refusal class and every affected exporter in the changelog, fix README:1255 and worker.rs:28, and consider `breaking` for the otap permanent nack.

5. Corrupt-body handling is patched into four exporters with four different policies, while the OTLP-to-Arrow conversion that silently truncates stays unguarded for processors
   Risk score: 60
   Sources: seats 2, 1
   Files/lines: the four call sites above; `pdata/src/otlp/mod.rs:144`; unguarded: `durable_buffer_processor/mod.rs:1051` (ConvertToArrow), `batch_processor/mod.rs:799`; `crates/otap/src/nack_status.rs:33-44`
   Evidence: file and otap send permanent nacks with different wording; otap uses `NackMsg::new_permanent` (cause Unspecified, so the receiver answers gRPC INTERNAL / HTTP 500), series uses `NackCause::Refused` (INVALID_ARGUMENT); parquet neither acks nor nacks and emits an unbounded per-request WARN. Any upstream processor that converts OTLP bytes to Arrow turns a truncated body into a partial batch no exporter can detect, which undermines "ack means rows stored" in the documented buffered topology.
   Proposed fix: one shared helper (`OtapPayload::check_otlp_framing() -> Result<(), NackReason>`) with one cause (`Refused`) and one event name, or validate inside the OTLP-bytes-to-Arrow conversion. Rate-limit the parquet WARN. Document the processor gap in the series README topology section.

6. The 150 MB raw evidence tree and ~17.5k lines of agent process documents are in the branch history and cannot go upstream
   Risk score: 85 (upstream), 40 (fork)
   Sources: seats 14, 3, 6, 4
   Files/lines: `docs/superpowers/reports/series-parquet-measurement/` (1062 files, 150.6 MB at HEAD, 262 MB across all blob versions, 18.7 MB packed; `samples` arrays are 60% of bytes; 30 commits touch it, 7 are rescrub/re-measure rewrites); `docs/superpowers/{plans,specs}/*.md`, `umbrella-review-2026-09-22.md`, `concistency-review-2026-09-22.md` (sic)
   Evidence (verified): `docs/` on main holds 14 hand-written .md files and `img/`; no data dumps. The harness is load-bearing on the tree: `measurement.py:52 REPORT_DIR_RELATIVE`, `load_baselines` globs `baseline-*.json` (188 files, 0.2 MB). `series-lake/README.md:9`, `FORMAT.md:9`, and rustdoc "spec section N" point at `docs/superpowers/...`, which is dead on crates.io for a `publish = true` crate. Pre-scrub history contains no `/home/mfilimonov` in the reports tree (checked with `git log -S`). Four `flush-stall/*-bench-config.json` files at HEAD still embed a `/tmp/claude-1000/...` scratch path with the user name (never passed through the scrubber).
   Proposed fix: keep baselines and contract indexes in-repo at a stable non-superpowers path (~1-2 MB), strip `samples`/`thread_affinity`/residual arrays from any committed run document, archive raw trees as the workflow's existing upload-artifact step or an orphan `evidence` branch on `filimonov` (whole HEAD tree is 5 MB as tar.zst). Rewrite history (`git filter-repo --path docs/superpowers/reports --invert-paths` or a fresh squash) before any upstream PR. Move a trimmed design spec to `rust/otap-dataflow/docs/series-parquet-exporter.md`; drop plans and review notes upstream. Rescrub `flush-stall/` and add a CI check that fails on `HOST_PATH`/credential patterns in committed evidence. Also move `chatgpt-discussion.md`, `otap-dataflow.patch` and `docs/superpowers/compaction-and-format-chat.md` out of the worktree or into `.git/info/exclude` before they get swept into a commit.

7. Engine-wide jemalloc `background_thread:true` default rides in a feature branch, justified only by this exporter's measurement, and its test is compiled out of every required CI job
   Risk score: 72
   Sources: seats 3, 12, 2, 11, 8, 6
   Files/lines: `src/main.rs:112-137, 314-319`; `crates/engine/src/memory_limiter.rs:25-40`; `.chloggen/jemalloc-background-thread.yaml`; `.github/workflows/rust-ci.yml:70-82` (`--all-features` enables `mimalloc` and `dhat-heap`, cfg-ing out both the static and the test)
   Evidence (verified): the static applies to every glibc jemalloc build regardless of the `series-parquet` feature. The only opt-out is `MALLOC_CONF`. The startup line is a `println!("INFO memory allocator ...")` that bypasses tracing and duplicates `startup::system_info`. The startup line reports `opt.background_thread`, the option, not whether threads started. Benchmark binaries use tikv-jemallocator without the symbol, so their numbers no longer match production.
   Proposed fix: split into its own engine PR with measurements on the default OTAP/OTLP paths, or gate behind a feature and have the harness set `MALLOC_CONF` itself (its `bgthread` mode already can). Fold the state into `startup::system_info` as a structured event. Add a default-feature assertion in test_e2e.py's engine start (`background_thread on` unless `MALLOC_CONF` set) so a required lane covers it.

8. Generic engine crate now owns one optional exporter's memory accounting and metric-set name
   Risk score: 62
   Sources: seats 2, 3, 8
   Files/lines: `crates/engine/src/engine_metrics.rs` (+643/-40: `SeriesAccounting`, `SeriesMemoryAccounting`, `#[metric_set(name = "exporter.series_parquet")] SeriesProcessMetrics`, `PROCESS_ACCOUNTING` LazyLock, `with_accounting`); `worker.rs:516`; `metrics.rs:32`
   Evidence: the same metric-set name is defined in two crates on two entities; the engine code compiles and runs with the feature off; the residual `max(0, RSS - sum  accounted)` is derivable in a backend from existing `memory_rss` and per-worker `memory.accounted` gauges. The metric name `memory.unaccounted_rss_bytes` carries a unit word, which the exporter's own `assert_no_unit_in_name` test forbids (it passes only because the set is registered in the engine crate). A fresh RSS is subtracted from a stale accounted sum (published only on CollectTelemetry), and `get_rss_bytes` returns 0 on a `/proc` read failure, so 0 is ambiguous.
   Proposed fix: remove from the feature PR and compute in the backend, or propose a generic "component accounted memory" registry in a separate engine PR. If kept: skip the residual on RSS read failure, export the accounted total beside it, drop the unit word.

9. FORMAT.md's normative partition lateness bound understates the real bound (80s stated, >=130s actual, README says 145s)
   Risk score: 70
   Sources: seats 6, 1
   Files/lines: `crates/series-lake/docs/FORMAT.md:685-690`; `worker.rs:917-962` (`rotate` returns early while `flushing`/`cleaning` is `Some`; `FlushJob::new` takes the deadline at seal); `series_parquet_exporter/README.md:237-243`
   Evidence (verified): FORMAT.md gives `L = window.interval + flush_retry_deadline + upload.abort_timeout` "(80 s with the defaults)". The README states "The two retry windows are therefore sequential" and gives `interval + 2 * (flush_retry_deadline + abort_timeout)` = 145s.
   Impact: a compactor following FORMAT.md can close hour H while a writer is still legitimately writing into it; compacted output misses rows.
   Proposed fix: restate L as `interval + 2 x (flush_retry_deadline + abort_timeout)` in FORMAT.md, or explain the sequential-slot term, and keep both documents aligned.

10. Logs extraction deep-clones and canonically encodes every record's attributes even on a memo hit; values rows are materialized row-wise with several per-cell heap copies
    Risk score: 55
    Sources: seat 5
    Files/lines: `crates/series-lake/src/extract/logs.rs:96-215`; `extract/mod.rs:345-405, 499-513, 572-587`; `extract/metrics.rs:398-443, 546-590`; `attrs.rs:250-283`
    Evidence: the logs `MemoKey` is fully owned (four `String`s plus a canonical byte vector, SipHash); every record pays a full attribute clone, a second identity clone, two empty `Arc<[_]>`s, a 256-byte canonical buffer and four string copies before the lookup. Metrics already moved to a borrowed content-keyed `MemoKey<'t>`; the previous umbrella review flagged the logs path and it is unchanged. The 1 KiB body is copied three times per record (`Value::Str(s.to_owned())`, `map_string` clone, builder append). Committed attribution (`attribution-logs-1k-stable-strict-minio-c1-w15-r007.json`) shows ~105k records/s per core with extraction the largest allocator caller.
    Proposed fix: mirror the metrics memo (borrowed key, owned descriptor only on miss, fast hasher); append directly into typed builders from borrowed cells; move the body out of `Value::Str`; cache `producer_id` per resource id; hoist `denorm_columns` out of the metrics point loop. The bound on the 1M/s target depends on these.

11. Flush deadline/cancel cleanup outcome is discarded, so orphaned multipart uploads and late-committed duplicate files leave no log or metric; failure events lack correlation keys; metrics cannot separate a bad credential from an outage
    Risk score: 55
    Sources: seat 8
    Files/lines: `flush.rs:107-160, 227-240, 262-278, 327-333`; `sink.rs:826-830, 1043-1048`; `worker.rs:1068-1073`; `metrics.rs:66-74, 160-163`
    Evidence: the sink builds `abort_error: Some("...a multipart upload it was creating may be left to the bucket lifecycle rule")`, but the decided branch in `write_until` runs `select! { _ = &mut write => {} _ = sleep_until(cleanup_deadline) => {} }` and drops the result unlogged. `series_parquet.flush.failed` carries no `window_start`/`seq`/`file`/`requests`; `flush.attempt_failed` has no `seq` or deadline remaining. `retryable()` classifies PermissionDenied/Unauthenticated/NotFound as permanent, but `flush.failures` is unlabelled and the nack label is just `storage`.
    Proposed fix: match the write result in the decided branch and log `series_parquet.flush.cleanup` (WARN with file/attempt/abort_error, INFO for a late commit); add `flush.abort_failures` and `flush.late_commits`; put `window_start, seq, file, requests, bytes` on `flush.failed` and `seq, deadline_remaining` on the attempt events; label `flush.failures` with a closed `error.type` (`deadline`, `permanent_storage`, `cancelled`, `encode`, `internal`).

12. Exporter test suite has no model-based test of the ack/nack state machine and no test asserts "exactly once" (only "at least once"); the validator has no differential test against prost
    Risk score: 60
    Sources: seat 11
    Files/lines: `series_parquet_exporter/tests.rs` (87 example tests; no `try_recv().is_err()` on any completion receiver; `values_retry_reuses_paths_and_bytes` docstring says "exactly once" and checks one `recv()`); `validate.rs:1004-1559` (12 tests, one prost-encoded positive fixture per signal)
    Proposed fix: a proptest state machine on `Worker` with `SimClock`/`TestWallClock`/`FaultStore` (ops: admit, boundary, store-mode flip, release, shutdown, advance, drain notify; invariant: each id has <=1 completion at every step and exactly 1 at the end, ack only if both files exist); an `assert_no_more_completions` helper used at the end of every completion test; a `#[cfg(test)]` drop bomb on `AckToken`; a proptest that mutates the fixture bodies and asserts `validate_request(b).is_ok() => prost decode ok` plus a table-completeness test over every `Message::field` entry.

13. Shutdown-grace WARN fires on every default deployment, once per core, against a hard-coded copy of a controller constant; no Kubernetes guidance
    Risk score: 50
    Sources: seat 8
    Files/lines: `mod.rs:200-205, 247-265` (`SIGNAL_SHUTDOWN_GRACE = 60s`); `crates/controller/src/lib.rs:2141-2148` (`SHUTDOWN_TIMEOUT_SECS = 60 // TODO: make this configurable`); README.md:248-260, 1102-1123
    Evidence: defaults give 145s vs 60s grace; the README says "a default worker warns at start"; `announce` runs per worker. Kubelet's default 30s SIGKILL is the binding constraint and the README never mentions `terminationGracePeriodSeconds` or a preStop hook.
    Proposed fix: export the grace from the controller instead of copying it; warn once per pipeline; pick defaults that fit the signal grace (flush_retry_deadline ~ 15-20s) or document the Kubernetes settings.

## Minor issues / improvements

- Producer-visible nack reason quotes the object-store error, which starts with the internal endpoint, bucket and key layout and up to ~200 bytes of the store's error XML (`worker.rs:326-330`, `token.rs:134-155`). Credentials are not exposed. Send a fixed sentence plus a classification; keep the full error in the WARN. (seat 4)
- Seal and admission failures nack co-tenant requests as `Outcome::Storage` with the "could not write ... to object storage" sentence, although both are in-memory Arrow/invariant errors (`worker.rs:793, 939`, verified). Use `Outcome::Internal`. Neither path is tested with co-tenants; the `rotate()` seal-failure path has no exporter test. (seats 7, 15)
- Minimal cloud configs are refused at startup: with no `retry` section the effective `retry_timeout` is the object_store default 180s, which fails `check_retry_deadline` against the 60s default deadline (`config.rs:198-221`, `object_store.rs:112-125`, verified). The shipped s3 example sets 30s so it passes. Derive a default below the deadline when unset; refuse only explicit conflicts. (seat 1)
- Default `unsupported: reject` refuses a whole metrics request over one summary or exponential-histogram point, and behind `durable_buffer` a permanent refusal is a silent drop; inconsistent with the exemplar default `drop` and its rationale (`series-lake/src/config.rs:357-366`, README:801-812). Default to `drop`, or ship examples with `drop` and call out the interaction. (seat 1)
- Each attribute table gets its own full `max_extracted_bytes` outside the shared `Budget` (3 tables for logs, up to 4 for metrics), and `value_bytes` charges 8 bytes per scalar where a `Value` is 32; `memory.budget` understates the real peak. (seat 7)
- `metric_rows` reads `metric_type` without an `is_valid` check and silently overwrites duplicate `metric_id`s (`extract/metrics.rs:191-193, 270-272`, verified); points can be stored under the wrong series and acked. Refuse both as invalid. (seat 7)
- Run seal copies every values row twice (`concat_batches` then `take`) before the merge's `interleave`; the k-way merge allocates an `OwnedRow` per popped row (`buffer.rs:92-113`, `sort.rs:205-237`). Lexsort key columns to a permutation and interleave once; keep the heap over `(run, seg, off)`. (seat 5)
- The run sort on the admission path (`append` -> `seal`, up to `run_target_bytes` = 8 MiB) is one synchronous stretch on the ingest core, and `rotate()` does a full `recount()` synchronously; bbc888b3d sliced only the flush side. Measure with the flush-stall probe or document the bound. (seat 10)
- `validate_framing` adds an unbenchmarked second full-body walk to three existing exporters' hot paths and re-checks UTF-8 that `binary_to_utf8_lossy` checks again; no bench calls it. Add a Criterion bench next to the OTLP-to-OTAP conversion and report the ratio. (seat 5)
- The select loop in `mod.rs` drives worker state through `pub(super)` fields and restates `rotate()`'s precondition in a select guard; the two-block invariant is enforced across two files. Expose intent-level methods. (seat 2)
- The lake crate hard-codes the exporter's dotted YAML keys in its validation messages (two of them name keys that live under `window.*` in the exporter schema), forcing duplicated cross-field checks; config errors are typed as request refusals (`Error::Refused`), and `Refused` displays Debug output. Return structured config errors; give `RefuseReason` a Display. (seats 2, 1, 6)
- Retry classification in `flush.rs:107-160` walks the lake's `ParquetError::External` source chain with downcasts; a change in sink error wrapping silently flips retry behaviour. Add `lake::Error::is_transient()` or classify at the sink boundary. (seat 2)
- `Block<T>`'s generic request slot is unused in production (`Block<()>` with tokens kept beside it), so request count is tracked in two places. Replace with a counter. (seat 2)
- Three clock-injection styles in one subsystem; `SinkClock` defaults to tokio time unless `with_clock` is remembered. Require one monotonic clock trait in `Sink::new`. (seat 2)
- `storage_kind` parses the backend name from `Debug` output; add `StorageType::kind()`. (seat 2)
- `ProcessorInbox::shutdown_deadline` has no caller outside its own test. Drop it or name the processor that needs it. (seats 2, 3)
- Published `series-lake` crate: every module is `pub`, `Sink::new`/`Block::new` accept an unvalidated `LakeConfig`, config structs are all-pub without `#[non_exhaustive]`, `TestWallClock` is public and ungated, one-word rustdoc on `LakeConfig` fields, README/rustdoc link to repo-internal paths. (seats 1, 3, 6)
- `ingress.max_request_bytes` measures protobuf length for OTLP input and estimated Arrow bytes for OTAP input; undocumented. (seat 1)
- README contradictions: "shipped buffered topology" does not exist under `configs/` (only in Python-generated YAML); "A write that has finished by the moment the deadline expires is reported as the success it is. Its requests are still nacked as retryable" contradicts `write_until`, which acks it; the local config and `configs/README.md` promise durability that the README's fsync caveat denies; the Alloy example hard-codes `host.id = "alloy-producer"` against a uniqueness contract the series-lake README never states; series-lake README's exemplar limitation is stale; the exporter README embeds a ~150-line Python test and contributor-only harness text. (seats 6, 1, 8)
- Changelog: all seven entries carry placeholder `issues: [4128]`; a `breaking` entry and an `enhancement` entry describe deltas against a component new in the same release; the engine entry bundles an exporter behaviour change and the event rename `parquet.exporter.retry_ignored_for_file_storage` -> `object_store.retry_ignored_for_file_storage` with no migration line. Fold revision 2 and the exemplar policy into the `new_component` entries; split the framing refusal into a `pipeline` entry that lists exporters and refusal classes. (seats 6, 12, 3, 8)
- Nothing in written files distinguishes format revision 1 from 2 (`format_version = "1"`, path `v=1`) although render_v1 spelling changed; add `format_revision=2` now while it is free. (seat 12)
- `OUTCOMES = 11` and the nack list in `sample_metrics` are maintained by hand next to the `Outcome` enum; the validator's singular-field bitmask assumes field numbers < 32 (`1u32 << slot`). Add const asserts / a `u64` mask. (seat 7)
- `flush.retries` is credited only when a flush resolves, so an ongoing outage shows zero retries for up to `flush_retry_deadline`; CollectTelemetry is not served during the post-Shutdown drain, so gauges freeze. (seat 8)
- Workflow container images are pinned by mutable tag (one an `rc`); GitHub Actions are SHA-pinned, permissions are read-only, no secrets. Add `@sha256:` digests. (seat 4)
- The `series-parquet-e2e` path filters omit `configs/series-parquet*`, `crates/pdata/src/views/otlp/**`, `crates/otap/src/object_store.rs`, `crates/engine/src/message.rs`, `engine_metrics.rs`, `src/main.rs`, `Cargo.lock`; changes there skip the writer/reader matrix. (seats 11, 12, 3)
- `test_failures.py`'s pure unit classes are not run by any workflow; `measure.py` registers `capacity`, `soak`, `failures`, `buffered`, `remediate`, `report` as "implemented by a later task". Against real stores only a network outage and a SIGKILL are exercised. (seat 11)
- Python harness layering is inverted: `faults.py`, `memory.py`, `performance.py`, `measure.py` import `test_e2e` as a shared library and `memory.py` imports the CLI module; cycles are avoided only by function-local imports. `test_failures.py` imports `measurement`, so the harness cannot be split from the E2E suite without extracting a `harness/` package first. (seats 2, 3)
- `series-lake` dev-dependencies (criterion, dhat, cpu-time, sha2, prost, otap with `aws`/`crypto-ring`, object_store `aws`) are used only under `benches/` but are built on every `cargo test -p otel-arrow-dfe-series-lake`. (seat 3)
- Seven exporter tests re-pin pdata validator semantics end to end (`tests.rs:1070-1600`); keep one, move the rest with the validator. (seat 3)
- No test samples accounting after ack, nack or abandon to assert return to baseline; sink cancellation/failure tests do not check `merge_key_bytes`/`flush_workspace_bytes` read 0. (seat 11)
- The oracle proptest never mixes metric kinds in one request. (seat 11)

## Needs verification

- Whether the pipeline completion channel can stay full long enough for blocker 1 in production: depends on controller scheduling relative to the exporter task and the default completion channel capacity. The test suite already treats that state as in scope. (seats 10, 13, 15)
- Whether the engine nacks outstanding contexts when a node task dies or its `start` future is dropped from outside; otherwise every `AckToken` (and a detached `spawn_local` flush task holding `Rc<Sink>`) is dropped undecided. Not visible in the reviewed files. (seats 10, 13)
- Whether common Go-based producers (OTel Collector, Alloy) actually emit non-UTF-8 OTLP strings or repeated singular fields; decides the real-world impact of major 4. (seats 1, 12)
- Whether any df_engine processor builds `OtlpProtoBytes` by splicing sub-messages (repeated singular fields the otap exporter would now nack). (seat 12)
- Whether `try_into_with_default` can return non-content errors after `validate_framing` passed; `worker.rs:664-669` classifies every conversion error as `Failure::Permanent` ("fix the producer") while the README calls Arrow failures retryable. (seat 1)
- `sync_locked` in engine_metrics.rs calls `register_metric_set_for_entity`/`unregister_metric_set` while holding the `SeriesAccounting` std `Mutex`, which every worker's `SeriesMemoryAccounting::set` takes on its ingest thread. No lock-order inversion found; hold time under a long registry collection is unknown. (seat 10)
- `rotate()`'s empty-block branch replaces ACTIVE without deciding its tokens; safe only because a zero-row request is acked before touching a block. Add a `debug_assert!(tokens.is_empty())`. (seats 13, 15)
- `Worker::shutdown` nacks the parked request the moment shutdown latches although the drain still opens a block afterwards; avoidable retries, not loss. (seat 15)
- `PROFILING.md` and `crates/admin/README.md` tell users to set `_RJEM_MALLOC_CONF`; on glibc Linux the build is unprefixed and reads `MALLOC_CONF`. Pre-existing; matters now that main.rs documents the override. One run with `_RJEM_MALLOC_CONF=prof:true` settles it. (seat 4)
- Whether a hung store attempt (connection accepted, never answered) surfaces any log before the flush deadline through object_store's internal retries. (seat 8)
- The tracing capture in tests.rs uses a process-global subscriber plus a thread-local buffer; safe under nextest process-per-test, but WARNs emitted from `spawn_blocking` threads (LocalFileSystem) may be missed under plain `cargo test`. (seat 11)
- The harness README says "18 tests"; test_e2e.py defines 19 `def test_`. (seat 6)
- Whether `aggregate_family`/the evaluator re-read `samples` from committed children; decides whether summary-only evidence can re-evaluate from the repo alone. (seat 14)

## Suggested commit / diff split

Upstream, in merge order (11 commits mix shared code with the feature and need `git checkout -p`, not cherry-pick; 23 more mix docs or harness with Rust):

1. **pdata framing PR**: `validate.rs`, the byte-view fixes (`common.rs`, `decode.rs`, logs/metrics/traces), the batching rename, adoption by file/parquet/otap with one shared helper and one `NackCause`, per-exporter positive and negative tests, a differential proptest against prost, a Criterion bench, one `pipeline` changelog entry listing every refusal class and exporter. UTF-8 policy decided here.
2. **Prep refactors PR**: shared object-store wiring (`required_token_provider`, `exporter_store`, `effective_retry_timeout`; keep the old parquet event name or ship a `Migration:` line), `byte_units` required helpers plus the journald cleanup, the pdata accessor visibility changes, `ExporterInbox::shutdown_deadline` only, and the `closed_pdata_shutdown` fix for major 2.
3. **jemalloc background_thread PR** (or drop): `malloc_conf`, `jemalloc_background_thread()`, a structured startup event, a default-feature CI assertion, its changelog entry, benchmarks on the default OTAP/OTLP paths.
4. **series-lake crate PR**: `src/`, `tests/`, goldens, `gen_golden.py`, FORMAT.md (with the corrected lateness bound and a `format_revision` marker), README pointing at in-crate docs; without the `bench-harness` benches and their dev-deps; blocker 3 fixed.
5. **series_parquet exporter PR**: the module, configs (plus a buffered example), README, `components-baseline.json`, the Cargo feature, one `new_component` changelog entry, `test_e2e.py` and `test_failures.py` with the minimal fault shim, the functional CI lane with widened path filters, a trimmed design doc under `rust/otap-dataflow/docs/`; blocker 1 and majors 5, 9, 11, 13 fixed.
- **Follow-up or fork-only**: the measurement/memory/performance harness, `test_measurement.py`, `host_monitor.py`, the benches, the measurement CI lane, the engine RSS residual (or a generic accounted-memory registry PR).
- **Never upstream**: the raw evidence tree and `docs/superpowers` plans and review notes. History must be rewritten so no upstream commit contains them; keep the current branch on `filimonov` as the archive, plus an orphan `evidence` branch or artifact tarball for the 5 MB compressed tree.

## Tests to add or strengthen

- Blocker 1 regression: one request admitted and flushing, stalled completion channel, >=2 force-drained pdata, then (a) release the write, (b) let the deadline fire; assert no panic and exactly one decision per request.
- Blocker 2 regression: Shutdown latched with D = 60s, flush in flight, `drop(pdata_tx)`, advance 1s; assert the flush is still awaited and acknowledged, not nacked `NodeShutdown`.
- Blocker 3 regression: dictionary-encoded `ser` column with one 1 MiB CBOR array of small ints referenced by N rows; assert refusal against the table budget before the N-th decode, and that a distinct dictionary key is decoded once.
- Proptest state machine on `Worker` (ops and invariants in major 12); `assert_no_more_completions` at the end of every completion-asserting test; `#[cfg(test)]` drop bomb on `AckToken`.
- Differential proptest of `validate_request` against `prost::Message::decode` on mutated fixture bodies; table-completeness test over every `Message::field` entry; positive OTLP-bytes tests in the otap and parquet exporters that assert an ack / written rows.
- Co-tenant labelling: an admit or seal failure with other requests in the block asserts `Outcome::Internal` for all of them; drive the `rotate()` seal-failure path.
- Accounting returns to baseline after ack, after nack and after abandon; sink cancellation and failure tests assert both workspace gauges read 0.
- Oracle case mixing gauge, sum and histogram points in one request.
- Config: minimal S3 config with no `retry` section is accepted (after the default fix) and an explicit `retry_timeout >= flush_retry_deadline` is refused.
- Operability: the cleanup branch logs `series_parquet.flush.cleanup` with `abort_error` on deadline; `flush.failures{error.type=permanent_storage}` increments on PermissionDenied.
- CI: default-feature assertion that the engine log contains `background_thread on` (unless `MALLOC_CONF` set); run `test_failures.py` unit classes in the writer-reader-matrix job; a check that fails on `HOST_PATH`/credential patterns in committed evidence.

## Coverage summary

- Entry points reviewed: PData (normal and force-drained), Shutdown{deadline} including the latched deadline read after every message, window boundary wake, flush result (finish/try_finish/cleanup join), CollectTelemetry, inbox error; config load and factory; the four `validate_framing` call sites; jemalloc startup.
- Transitions reviewed: admit, park, resume, rotate (empty, busy, seal fail), complete (ok, err, task died), the cleaning slot, abandon phases 0-2; sink phases (series then values, multipart create/settle/abort, upload ledger release); cache reserve and commit.
- Fault categories checked: transient then permanent store errors; permission/auth errors; hung write and deadline expiry; wedged multipart abort; finish exactly at the deadline; both blocks plus parked at the deadline; busy boundary; backward, drift and over-one-interval clock steps; oversized request; parked before newer; failed descriptor not poisoning the cache; values retry reusing paths; past deadlines; closed upstream during drain; saturated completion channel during force-drain; malformed/truncated/oversized/deep OTLP and CBOR input; concurrent admission across cores (per-core `boot_id`).
- Invariants confirmed: one ACTIVE plus one FLUSHING-or-cleaning slot; at most one parked request; ack only after `write_block` returns Ok; a failed write nacks the whole block retryably and a retry rewrites identical names and bytes (`emitted_at` frozen); a failed seal leaves the block intact; cache commit only on success against the block's own partition; a flush resolved in the deadline poll is acked via `try_finish`; upload charges released exactly once in any order; byte views step over everything the validator accepts; object paths are fixed-segment with a `[A-Za-z0-9_.]` writer id; `malloc_conf` export is ABI-sound.
- Invariant that does not hold: "every token resolved exactly once" (blocker 1); "the latched shutdown deadline bounds the drain" (blocker 2); "table budget bounds decoded attribute memory" (blocker 3).
- Deferred / not covered: extraction correctness beyond the oracle's single-kind cases; sort/merge correctness beyond the oracle; the Python harness internals beyond structure, import layering and scrubbing; Azure path (one Azurite E2E is the stated gate); no seat built or ran code.
- Main assumptions: engine force-drain semantics taken from `message.rs` docs; `Value` and `ciborium::Value` sizes (~32-48 bytes) estimated, not measured; attribution numbers taken from the committed r007 evidence.

## Final verdict

Status: request changes

Minimum required actions before this branch can be proposed upstream:
- Fix blocker 1 (force-drain credit accounting; no release-mode assert on the shutdown path) with a regression test.
- Fix blocker 2 (`closed_pdata_shutdown` must honour the latched deadline) in the engine, with an exporter test.
- Fix blocker 3 (charge decoded CBOR footprint; decode each dictionary value once) with a regression test.
- Decide and disclose the UTF-8 and repeated-singular-field policy for the file, parquet and otap exporters (major 4); align README:1255 and worker.rs:28; rewrite the changelog entries and replace issue 4128.
- Correct FORMAT.md's partition lateness bound to the sequential-slot value (major 9).
- Rewrite history to exclude the evidence tree and process documents, rescrub `flush-stall/`, relocate baselines and the design doc, and split the branch into the PR series above (major 6, 7, 8).
- Add the `assert_no_more_completions` helper and the co-tenant `Outcome::Internal` fix; widen the E2E path filters.

For the fork, the exporter is usable behind `durable_buffer` once blockers 1-3 are fixed; the remaining majors are reviewability, disclosure and operability items rather than data-safety defects.
