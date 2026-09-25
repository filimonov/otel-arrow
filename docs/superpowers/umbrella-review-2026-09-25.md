# Review report: 5588c3e0d590d4df0303697a6862c8324235a310..HEAD (series-parquet-exporter, 2026-09-25)

## Summary

The range is the whole series_parquet campaign since the fork point: 491 commits, a new `series-lake` crate, the `exporter:series_parquet` node, a Python validation harness, CI, 18 changelog entries, and about 3.9k lines of changes to pre-existing engine, pdata, otap, durable_buffer and exporter code. Fourteen review seats ran (Opus 5.5); every finding below was re-checked against HEAD by the aggregator.

The exporter's own delivery state machine held up: the deep audit traced every admitted request to exactly one ack or nack, found acks only after files exist and descriptors are marked committed, and confirmed the block-budget and memory-bound invariants. The problems are elsewhere:

- The measurement evidence tree is still in history and has grown to 624 MB (45.7 MB packed). Two earlier reviews required its removal. This alone blocks any upstream PR.
- The durable_buffer shutdown fix (bfec1b506) introduced a regression for every durable_buffer user: the final segment finalize is now cut at an already-expired deadline, which the base code deliberately never did.
- A queued real Shutdown can be discarded by the inbox's closed-pdata probe and replaced with a made-up 1-second deadline. The branch's new output close in the buffer's completion wait makes this reachable in the reference Alloy + durable_buffer deployment.
- The S3 `unsigned_payload` TLS default reads only the YAML, so an environment-supplied plain-HTTP endpoint goes unsigned, and an operator's `AWS_UNSIGNED_PAYLOAD=false` is overridden.
- The shipped "reference" Alloy config hard-codes one `host.id` for every producer, which collapses series across hosts.
- The branch is still one unsplit PR with 18.8k lines of process documents, a 43.5k-line harness, exporter-specific code in the engine crate, and 18 changelog entries for a component that never shipped.

Verdict: request changes for the fork; block for upstream until the history rewrite and split.

## Reviewed scope

- Diff: 5588c3e0d590d4df0303697a6862c8324235a310..HEAD (committed HEAD only; the uncommitted durable_buffer working-tree change was excluded)
- Base: fork point from upstream main
- Files changed: 2693 (2421 measurement JSON; 161 code files under rust/; 13 docs)
- Main moving parts:
  - series-lake crate: extract, canonical identity, block buffer, sort, Parquet sink, FORMAT.md
  - series_parquet_exporter node: worker state machine, flush/retry/probe, token, outcome, metrics
  - engine: latched shutdown deadline, processor completion phase, engine_metrics series accounting
  - durable_buffer: shutdown acks, bounded shutdown steps
  - pdata: OTLP framing validator adopted by file/otap/parquet exporters
  - otap object_store: shared wiring, S3 unsigned_payload
  - main.rs: jemalloc background_thread default
  - validation harness (Python), CI workflow, shipped configs, 18 chloggen entries

## Blockers

1. Measurement evidence tree still in branch history, now 624 MB
   Risk score: 93
   Sources: repo-impact, ockham, docs
   Files/lines: docs/superpowers/reports/series-parquet-measurement/ (2529 files); measurement.py:52-65 (REPORT_DIR default), measure.py `stage-results` (runs `git add`); series_parquet_exporter/README.md:132 and series-lake/src/extract/mod.rs:106 cite a file in the tree; soak.py:82-85 reads a committed capacity index
   Evidence: the range packs to 48.7 MB, of which 45.7 MB is the reports tree; the base repo packs to 68.2 MB. 72 commits touch the tree and 26 of them also change code, so dropping commits is not enough. 2026-09-22 (534 files, 21 MB) and 2026-09-23 (1062 files, 150 MB) both required a history rewrite; none happened.
   Impact: every clone pays permanently; upstream cannot take it; the harness keeps growing it by design.
   Proposed fix: archive the current branch on the fork; rewrite upstream-bound history with `git filter-repo --path docs/superpowers --invert-paths` (or fresh squashed commits); default REPORT_DIR to the ignored `.measurement-artifacts/`; remove `stage-results`; keep only baseline-*.json (0.47 MB) at a stable non-superpowers path if the baseline gate needs them; replace the two citations with the measured numbers inline.

## Major issues

2. durable_buffer now skips the final segment finalize when the deadline is already past
   Risk score: 70
   Sources: compat, code-quality, concurrency, ownership, deep-audit (independently)
   Files/lines: durable_buffer_processor/mod.rs (HEAD) 157-166 `until_deadline`, 1697 and 1783 (call sites), 1717 (`until_deadline(deadline, engine.flush())`), 1792-1817 `shutdown_engine`; introduced by bfec1b506
   Evidence: base code ran `engine.shutdown()` unconditionally with the comment "Even if past deadline, this is fast ... and critical for data durability". HEAD wraps `flush_progress` + `shutdown` in a biased select against `sleep_until(deadline)`. Both call sites reach it with an expired deadline (the `deadline_exceeded` branch, and a drain loop that runs until `now >= deadline`), so the first pending poll drops the finalize. `SHUTDOWN_PERSIST_RESERVE` (1s) bounds only the completion wait. Quiver's `finalize_segment_impl` takes the open segment and cursor before its first await, and its partial-file cleanup runs only on error, not on drop; the `spawn_blocking` write detaches.
   Impact: every durable_buffer user, without opting in. Recorded acks are not persisted and the open segment is not finalized, so bundles replay on restart (duplicates). A torn or unregistered `.qseg` can be left behind. Whether the WAL guarantees no loss when the engine is dropped without shutdown is unverified. Not disclosed in the changelog.
   Proposed fix: end the flush and drain at `deadline - SHUTDOWN_PERSIST_RESERVE`, and give `shutdown_engine` a floor of the reserve from the moment it starts (or run the finalize in a spawned local task and stop awaiting it rather than dropping it). Add a test with a stalled `flush()` that asserts the recorded acks are persisted and `shutdown.complete` is emitted. Disclose in the changelog.

3. A queued Shutdown is discarded by the closed-pdata probe and replaced with a made-up 1-second deadline
   Risk score: 65
   Sources: concurrency, tests (as needs-verification)
   Files/lines: engine/src/message.rs:426-443 (probe runs before control_rx is read), 355-365 `closed_pdata_shutdown` (falls back to now+1s and drops control_rx); engine/src/processor.rs:237 (`router.close()` in `await_completions!`)
   Evidence: verified: the probe fires when admission is closed and the pdata channel is closed, before line 627 checks the control channel; `pending_shutdown` is set only after a Shutdown was dequeued. Interleaving: the controller try_sends Shutdown to buffer and exporter in one pass; the buffer runs first, its flush and drain never yield when the open segment is empty, and `await_completions!` closes its router; the exporter, with admission closed (rotation waiting, request parked, or credit spent), polls its inbox and gets the 1-second Shutdown while the real one sits unread.
   Impact: the exporter cuts FLUSHING and ACTIVE to 1 second and nacks everything with NodeShutdown, which defeats the "records acks during graceful shutdown" feature exactly in the reference deployment; every in-flight bundle replays (duplicates, not loss). The probe predates the branch, but the branch makes it reachable while the buffer is alive.
   Proposed fix: in `closed_pdata_shutdown` (or before the probe) drain `control_rx` with `try_recv` for a queued Shutdown and release it with its own deadline. Add an inbox test (queue Shutdown, drop the pdata sender, `recv_when(false)` with nothing latched) and an engine-level receiver-first drain test.

4. S3 `unsigned_payload` TLS default ignores an environment endpoint and overrides `AWS_UNSIGNED_PAYLOAD`
   Risk score: 60
   Sources: ux-contract, security, code-quality, tests, compat, docs, deep-audit
   Files/lines: otap/src/object_store.rs:236-240 `s3_uses_tls`, 248 `with_unsigned_payload_over_tls`, 519 (`AmazonS3Builder::from_env()`), 533; series_parquet_exporter/config.rs:358; README.md:292-298
   Evidence: verified: the check reads `endpoint.unwrap_or(base_uri)` from YAML only; `from_env` maps `AWS_ENDPOINT_URL`/`AWS_ENDPOINT` to the endpoint and reads `AWS_UNSIGNED_PAYLOAD`; the resolved value is always `Some` and applied after `from_env`. `base_uri: s3://bucket` + `AWS_ENDPOINT_URL=http://minio:9000` + `AWS_ALLOW_HTTP=true` resolves to unsigned over plaintext, contradicting the changelog ("off for plain HTTP").
   Impact: loss of the only body-integrity check on plaintext; silent override of the operator's env setting; some stores and bucket policies deny UNSIGNED-PAYLOAD (403 on every upload). The unit test also reads the ambient environment.
   Proposed fix: decide after the builder is configured (`get_config_value(&AmazonS3ConfigKey::Endpoint)`, honour `AllowHttp`); apply the default only when neither YAML nor `AWS_UNSIGNED_PAYLOAD` is set; hermetic tests for an env http endpoint and `AWS_UNSIGNED_PAYLOAD=false`; document the 403 symptom and the `unsigned_payload: false` opt-out.

5. The shipped reference Alloy config hard-codes one `host.id` and test-only attributes
   Risk score: 55
   Sources: ux-contract, docs
   Files/lines: configs/series-parquet.alloy:25-26, 36 (`host.id = "alloy-producer"`, `service.name = "series-e2e-service"`, `e2e.source`, `job = "series-e2e"`, `/input/events.log`)
   Evidence: verified. The series-lake README requires the producer id to be "stable for the producer and unique to it"; the exporter README calls this "the reference Grafana Alloy producer". Only the harness README notes the constant is fixture-only.
   Impact: operators copying the reference file get one `producer_id` for all hosts; streams differing only by host collapse into one `series_id`; every stored row carries `e2e.source`. This is the stated deliverable.
   Proposed fix: set `host.id` from `constants.hostname` or `sys.env`; make `service.name` a placeholder; move `e2e.source`, the job label and the input path into an e2e overlay; or rename the file as a fixture and ship a separate reference.

6. Branch is one unsplit PR carrying process documents, the harness, and 18 changelog entries
   Risk score: 72
   Sources: ockham, repo-impact, docs, compat, ux-contract
   Files/lines: docs/superpowers/{plans,specs,*.md} (18.8k lines; misspelled `concistency-review`, `s3-compatitibility`); validation/tests/series_parquet/ (43.5k lines Python, ~27k never run by CI); rust/otap-dataflow/.chloggen/*.yaml (18 entries, all `issues: [4128]`, which is a real unrelated upstream issue about a Windows Event Log receiver)
   Evidence: at least five independent upstream changes share the branch: pdata framing validator + adoption by three exporters; pdata byte-view fixes (traces.rs out-of-bounds field arm, ResourceXIter loops); engine completion phase + durable_buffer fix; jemalloc background_thread for every df_engine; object_store refactor + unsigned_payload. Ten changelog entries narrate intra-branch history of an unreleased component, three marked `breaking`; the parquet exporter's `retry_ignored_for_file_storage` event rename hides in the subtext of an engine entry without the old name. The 2026-09-23 review required all of this.
   Impact: unreviewable upstream; a revert of the exporter reverts unrelated fixes; release notes would announce breaking changes to a component users never had; a real event rename goes undisclosed.
   Proposed fix: PR series in dependency order: (1) pdata byte-view fixes, (2) framing validator + adoption with the UTF-8 policy decided, (3) engine completions + durable_buffer, (4) object_store + unsigned_payload, (5) jemalloc (or drop it and let the harness set MALLOC_CONF), (6) series-lake, (7) exporter. Collapse changelog to one `new_component` entry each for series-lake and series_parquet plus one entry per pre-existing-component change; separate `breaking` entry for the event rename with the old name; replace 4128 with the PR number and add a CI grep for "PLACEHOLDER". Upstream only test_e2e.py, generator.py, a minimal fault shim and requirements; keep measurement/performance/capacity/memory/faults/soak on the fork.

7. Engine crate hard-codes a series_parquet metric set and process-global accounting
   Risk score: 58
   Sources: ockham, architecture
   Files/lines: engine/src/engine_metrics.rs:73 `SeriesAccounting`, 79 `static PROCESS_ACCOUNTING`, 166 `#[metric_set(name = "exporter.series_parquet")]`, monitor branches `observe_series_residual` etc.; worker.rs:27, 309
   Evidence: verified. The generic engine registers a metric set named after one optional, feature-gated exporter and ships the lock and branches in every build. The residual it reports (`memory.unaccounted_rss_bytes`) is computable from the existing `memory_rss` and per-worker `memory.accounted`; its only non-Rust reader is test_measurement.py. The README's guidance for it is also misleading in the shipped deployments, where receiver-held requests and durable_buffer dominate the residual.
   Proposed fix: remove from the engine and compute in the harness/dashboard, or generalize to a component-keyed accounting registry in its own engine PR.

8. durable_buffer completion phase and engine completion phase are tested only with Acks
   Risk score: 55
   Sources: tests
   Files/lines: durable_buffer_processor/mod.rs (HEAD) 1586-1590 (`if self.awaiting_completions { return Ok(()) }` in the nack path), tests at 2643/2694; engine/src/processor.rs 2150-2480 (local loop only, `AwaitingProcessor` handles only Ack)
   Evidence: the series_parquet exporter answers every held request at its deadline with a retryable NodeShutdown nack, so behind the buffer the phase mostly sees Nacks; neither the transient nor the permanent Nack branch during shutdown is tested; no test covers `process` returning Err inside the phase (which skips the final Shutdown and `shutdown_engine`), a second Shutdown, or the shared loop wiring.
   Proposed fix: reuse `start_buffer`/`deliver_one`/`shut_down`/`replays_after_restart` for a transient-Nack test (ends before deadline minus reserve, no retry scheduled, replays) and a permanent-Nack test (does not replay); parametrize the engine tests over local and shared wrappers with Nack, TimerTick-before-Ack and processor-error cases.

9. The no-dictionary setting for `attrs.entries.values` never takes effect, so files contradict FORMAT.md
   Risk score: 50
   Sources: performance
   Files/lines: series-lake/src/sink/properties.rs:55 (`ColumnPath::from(*column)`); sink/tests.rs:1872-1885; docs/FORMAT.md:567-569
   Evidence: verified: parquet 58.4.0's `From<&str>` builds a single-part path (`parts: vec![single_path]`), while the encoder looks up the nested leaf by `["attrs","entries","values"]`; the override matches only top-level names such as `body`. The test looks the property up the same way and passes by construction. On disk the column carries a dictionary page. The S6 encode measurements were taken with this column still dictionary-encoded.
   Proposed fix: `ColumnPath::new(column.split('.').map(str::to_owned).collect())`; assert on a written file's column-chunk encodings; re-run the encode and size benches; if the dictionary helps, change FORMAT.md instead.

10. The E2E path filter omits durable_buffer, the processor run loop and the OTLP receiver
    Risk score: 45
    Sources: tests, ockham
    Files/lines: .github/workflows/series-parquet-e2e.yml:5-31
    Evidence: `test_buffered_store_outage` is the only CI end-to-end run of the target deployment, and a PR touching only `durable_buffer_processor/**`, `engine/src/processor.rs`, `quiver/**` or the receiver does not trigger it, while `Cargo.lock` does (every dependency bump runs the 60-minute Docker lane). The 2026-09-23 review asked to widen the filters.
    Proposed fix: add the buffer, processor, quiver and receiver paths; drop Cargo.lock and shared engine files once finding 7 is fixed.

## Minor issues / improvements

- A partial `retry:` section fails startup because `retry_timeout` then defaults to 180s instead of the derived `flush_retry_deadline / 2`; the new public `effective_retry_timeout` has no callers and reports the wrong budget (config.rs:203-233; object_store.rs). Merge user fields over `derived_retry()`; remove the helper. [ux-contract, architecture]
- `writer_id` refuses `-`, so hostnames and pod names fail at startup (series-lake/src/config.rs:611). The file name is unambiguous with hyphens; allow them or suggest the replacement in the error. [ux-contract]
- OTLP framing validation is copied into four exporters, while the batch processor (batch_processor/mod.rs:799) and durable_buffer `convert_to_arrow` (mod.rs:1076) convert without it, so a converting processor upstream re-opens the truncation bug for every exporter below. Validate once in the conversion (`try_into_otap_validated`) or at the receiver. [architecture]
- Nested invalid UTF-8 is refused permanently while top-level is repaired to U+FFFD; the file, otap, parquet and series exporters now disagree about the same body; series_parquet also refuses repeated singular fields, which is valid protobuf. Decide one policy and document it. [ux-contract, compat]
- `flush.late_commits` conflates the nacked-but-stored case (duplicates) with the acked probe case; `partial` and `unknown` cleanups have no counter (flush.rs:441-460). Split by a closed `outcome` label. [operability, architecture]
- Two `abort_failed` paths log only the file name, not the object key the README promises (flush.rs:399, 711-716); the start event omits base_uri, endpoint, resolved `unsigned_payload` and retry timeout; `series_parquet.notify.failed` is not rate limited; one refusal log gate covers all outcomes and carries no producer identity; `receiver_limit.unverified` is an unconditional INFO per worker. [operability]
- Shipped S3 config and configs README omit the `AbortIncompleteMultipartUpload` lifecycle rule and the metrics scrape URL; local-backend staging files (`<file>#N`) left by a crash are undocumented and never reclaimed; the local config caps a worker at about 7k records/s (128 slots, 15s window) without saying so; it writes to a predictable world-writable `/tmp` path. [operability, performance, security]
- "Format revision 2" is recorded nowhere a reader can see: files still carry `format_version=1`, `v=1` and `render_v1` while the rendering changed; FORMAT.md never mentions a revision. Either drop the framing (nothing released) or add a `format_revision` key. [compat, ux-contract, docs]
- The parquet, file and otap exporter READMEs do not document the new framing refusal or the `otlp.malformed_body` WARN; the parquet README still says "no events"; durable_buffer telemetry.md and the changelog say acks are recorded "until the deadline" while the code stops 1s earlier. [docs]
- `ProcessorRuntimeRequirements.shutdown_completions` and `StorageType::S3.unsigned_payload` are new public fields on published crates without `#[non_exhaustive]` or a changelog note. [compat]
- `ExporterInbox::shutdown_deadline()` returns `None` again after the latched Shutdown is released (message.rs:1524), opposite to its doc. [ux-contract]
- A very large `window.interval` passes validation then panics in `Instant + Duration` (window.rs:43, 72; clock.rs:184, 189); flush.rs already guards this with `deadline_at`. Cap the interval. [code-quality]
- A panicked flush task is nacked as `Outcome::Storage` ("could not write ... to object storage; retry") instead of `Internal` (worker.rs `complete()`); operators would debug the bucket. [ownership]
- A second, tighter Shutdown during the completion phase is swallowed and the final Shutdown carries the first deadline (`recv_completion_until`); an Err from `process(completion)` skips the final Shutdown and `shutdown_engine`. [code-quality, ownership]
- Cancelling a table write during `writer.finish()` (tables between part_bytes and row_group_bytes) can drop a multipart creation in flight, leaving an upload with no id and no `abort_failures` count, reported as a clean abort (sink/write.rs phase 2 vs `step()`). [ownership]
- `RefuseReason` mixes permanent refusals with block-scoped `BlockFull`/`TooManyRequests` and uses `Unsupported(String)` matched by literal; ingress limits and validation are split across node and lake with lake error messages naming keys users never write; the worker's memory-budget formula encodes sort/sink/cache internals; the node loop's rotation guard duplicates `rotate()`'s precondition. [architecture]
- series-lake exposes test scaffolding as public API (`hook_store`, `TestWallClock`, high-water accessors, `Deserialize` on internal config types) and ships 5.5k lines of harness-driven benches plus two bench-only features in a `publish = true` crate. [ockham]
- The delivery-model proptest never asserts `!nack.permanent` and covers three of ten faults; several new tests assert wall-clock windows under llvm-cov; the live fault matrix (5xx, kill, partition) never runs in CI. [tests]
- Per-request extraction rebuilds identity and descriptor rows for series already committed, and re-encodes the resource/scope prefix per series; ingress materializes every attribute three times; the k-way merge pops then pushes instead of replacing the top. Follow-ups for the 1M/s target, which also needs encoding off the ingest core (measured 684k/s on four cores local, 448k/s MinIO). [performance]
- Harness README bakes in one host's CPU layout (`taskset -c 0-7,16-23`) and a stale test count. [docs]
- Drive-by changes: journald byte-units refactor, core-nodes dev-deps forcing `aws`, a second allocator println, `.gitignore` entry for a fork-only directory. [ockham]

## Needs verification

- Whether Quiver's WAL guarantees no loss when the engine is dropped without `shutdown()` (finding 2 is duplicates-only if yes). Missing: the WAL writer's Drop/sync path and whether runtime teardown can abandon in-flight blocking writes.
- The receiver-first drain race (finding 3 variant): a drained receiver drops its pdata senders before the controller sends Shutdown to processors, so the buffer itself may take the synthesized 1s deadline and never await completions. Missing: a buffered SIGTERM-restart E2E that counts replayed bundles.
- Whether an Ack routed to `retry_processor` or `fanout_processor` after their control receiver closed is dropped, making series_parquet behind those processors a duplicate path.
- Conversion memory before any budget: `try_into_with_default()` runs before `max_extracted_bytes`; the README reserves 4x `max_request_bytes`. Missing: peak allocation on a 16 MiB body of empty log records or key-only attributes.
- Hourly partition rollover under 100k+ hot series (cache keyed by partition re-emits every series): no throughput run crosses an hour boundary.
- jemalloc `background_thread:true` for every glibc df_engine build: no A/B on a standard pipeline; background threads are unpinned.
- Whether any common producer or proxy concatenates serialized OTLP messages (repeated singular fields), which series_parquet would refuse permanently.
- Whether the engine routes object_store's own tracing events, without which S3 throttling inside one attempt is invisible except as rising `flush.duration`.
- Whether the parquet exporter's silent `continue` on a malformed body (no ack, no nack) can hang an upstream that waits for a completion.

## Suggested commit / diff split

- Core refactoring / behavior changes (upstream, in dependency order):
  1. pdata byte-view fixes (traces.rs field arm, ResourceXIter loops).
  2. OTLP framing validator + adoption in file/otap/parquet, with one UTF-8 and repeated-singular policy, READMEs updated, one `pipeline` changelog entry.
  3. Engine completion phase + `closed_pdata_shutdown` fix (findings 2, 3) + durable_buffer shutdown acks, with Nack tests; changelog discloses the bounded finalize and the longer wait.
  4. object_store shared wiring + `unsigned_payload` (finding 4), own `breaking` entry for the event rename.
  5. jemalloc background_thread, or drop it.
  6. series-lake crate with the ColumnPath fix (finding 9), public surface trimmed.
  7. series_parquet exporter + configs + minimal E2E; reference Alloy config fixed (finding 5).
- Separate follow-ups:
  - Engine accounting registry (finding 7) if wanted upstream.
  - Measurement, performance, capacity, memory, faults, soak tooling: fork-only or a later tools/ PR aligned with pipeline_perf_test.
  - Process documents and evidence: fork archive branch only.

## Tests to add or strengthen

- durable_buffer: transient and permanent Nack during the completion phase; a stalled `flush()` past the deadline asserting recorded acks are persisted and the finalize runs; a receiver-first drain through `RuntimeCtrlMsgManager`.
- Engine inbox: queued Shutdown + closed pdata sender + `recv_when(false)` releases the real deadline; completion-phase tests over local and shared wrappers with Nack, TimerTick and processor error.
- object_store: hermetic tests for env `AWS_ENDPOINT_URL=http://...` and `AWS_UNSIGNED_PAYLOAD=false`.
- series-lake sink: assert column-chunk encodings of a written file (no dictionary on the five high-entropy leaves); cancel during `finish()` with a stalled creation hook and assert the orphan is counted.
- Exporter model proptest: `prop_assert!(!nack.permanent)`, all `Fault` variants, small `part_bytes`.
- Config: `retry: {max_retries: N}` alone starts; `writer_id` with `-`; oversized `window.interval` refused.
- CI: widen the E2E path filter; a nightly job for one S3 5xx and one process-restart case against MinIO.

## Coverage summary

- Entry points reviewed: OTLP request admission and prepare; window timer; flush completion, cleanup and rotation; Shutdown with latched deadline; forced drain; closed pdata channel; engine `await_completions!`; durable_buffer `handle_shutdown`/`shutdown_engine`; object_store wiring; framing validation in four exporters.
- Transitions reviewed: ACTIVE to FLUSHING to cleaning to freed slot; park and resume; BlockFull/TooManyRequests; seal failure; abandon phases 0-2; notifier credit and force_shutdown; buffer flush, drain, completion wait, finalize.
- Fault categories checked: object_store 5xx/timeout/NotFound-on-abort/HEAD failure/part and creation failure; lost completion; retry deadline mid-attempt and mid-backoff; cancellation mid-finish; task panic; clock steps; oversized token; requests at each limit; empty request; cache full; deadline-vs-flush select tie; expired deadline at each shutdown step.
- Deferred / not covered: sort.rs merge internals beyond the heap; extract byte-estimate proofs; validate.rs beyond its differential proptest; Azure backend; Quiver internals beyond `finalize_segment_impl`; the Python harness's runtime behaviour; the proptest model's fidelity to the real loop.
- Main assumptions: one thread per pipeline with LocalSet; `recv_when` cancel-safe; S3/MinIO strongly consistent for HEAD; durable_buffer uses Quiver's default WAL durability; no builds or harness runs were performed (host lease).

## Final verdict

Status: request changes (fork); block (upstream, until 1 and 6 are done)

Minimum required actions:
- Rewrite upstream-bound history to exclude docs/superpowers (evidence and process documents); point the harness at an ignored directory; drop `stage-results`; replace the two in-code citations. Create the fork archive branch first.
- Fix the durable_buffer finalize cut (2) with a reserve and a stalled-flush test; disclose the behavior change.
- Fix the closed-pdata probe so a queued Shutdown is never replaced by a synthesized deadline (3), with inbox and engine-level tests.
- Fix the `unsigned_payload` default to use the resolved endpoint and to yield to `AWS_UNSIGNED_PAYLOAD` (4), with hermetic tests.
- Fix the reference Alloy config's producer id and strip the e2e attributes (5).
- Fix the nested `ColumnPath` (9) and re-measure encode cost.
- Split the branch into the PR series above; collapse the changelog and replace issue 4128; move the series accounting out of the engine crate or into its own PR; widen the E2E path filter.
