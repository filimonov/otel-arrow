# Plan 4 backlog (series_parquet exporter)

> Review documents cited here (umbrella, consistency, complexity and deslop reviews, the S3 compatibility note, the compaction and format chat) were removed from the tree on 2026-09-25 after every open item moved to the plan-4 backlog; they remain in git history.

Status: groomed backlog (2026-09-25), not a plan. Plan 4 is written from it
after the plan-3 final report (Task 14).

How items enter and leave: an item enters with its source (task report,
review, spec section or user decision) and leaves either into a plan with a
task number or into "Done / removed" with the task that delivered it. An open
item is never dropped silently; duplicates are merged and the merge is listed
at the end.

Not here: Task 12 (fixes), Task 12g (Azurite smoke) and Task 15 (canary-ready
chaos and multi-hour soak on the reference deployment) are in plan 3,
docs/superpowers/plans/2026-09-22-series-parquet-measurement.md.

Each item has a why, a done-when criterion and its source.

## P0: before production, right after plan 3

Items that affect correctness or operability of the shipped Alloy +
durable_buffer deployment and are not in plan 3.

- **P0-1 Acks to retry_processor and fanout_processor after they exit.**
  Why: in durable_buffer -> retry/fanout -> series_parquet the dispatcher drops
  completions to a closed control channel silently (retry_processor/mod.rs:668,
  705; fanout_processor 512, 988, 1107-1109; pipeline_ctrl.rs:1033, 1058), so
  the buffer replays bundles the exporter already wrote.
  Done when: both processors declare shutdown_completions and track in-flight
  frames (or a dispatcher rule covers closed nodes), and a dropped completion is
  counted and logged; a restart test through each processor stores no duplicate.
  Source: Task 12e item 5; umbrella review 2026-09-25, needs verification.

- **P0-2 End-to-end freshness gauge.**
  Why: operators alert today from external object checks and indirect signals;
  no metric gives the age of the oldest record accepted by the receiver but not
  yet in a values file.
  Done when: durable_buffer exports an oldest-pending-age gauge, the operator
  guide alerts on it combined with the exporter's `oldest_unacked.age` and on
  WAL fill.
  Source: backlog freshness contract (1), user review 2026-09-23; Task 12c F5.

- **P0-3 Refuse permanently invalid requests before the WAL acknowledges them.**
  Why: a request the exporter will refuse permanently (framing, row size, too
  many series, unsupported) is acknowledged by the WAL and later dropped, and
  one oversize line loses its whole export; the reference Alloy config works
  around it by truncating lines at 512 KiB, which changes data.
  Done when: the exporter's permanent checks (or a cheap subset) run at or
  before the buffer, the producer gets INVALID_ARGUMENT, and the Alloy line
  truncation is removed or replaced by drop-and-count.
  Source: backlog freshness contract (2); Task 12c fix round 1 F1.

- **P0-4 Validate OTLP framing once, in conversion or at the receiver.**
  Why: the framing check is copied into four exporters while batch_processor
  (mod.rs:799) and durable_buffer `convert_to_arrow` (mod.rs:1076) convert
  without it, so a converting processor upstream reopens the truncated-body
  acknowledgement for every exporter below.
  Done when: one validated conversion (`try_into_otap_validated` or the
  receiver) is used by every converting node and the exporter copies are
  removed; a damaged body through durable_buffer is refused in a test.
  Source: umbrella review 2026-09-25, minor issues.

- **P0-5 Conversion failure in durable_buffer has no resolved outcome.**
  Why: a bundle that fails conversion is rejected without
  `resolved{outcome=...}` (durable_buffer_processor/mod.rs:1426-1430), so the
  buffer's outcome totals do not add up.
  Done when: the rejection increments `resolved` with its own outcome and a test
  asserts it.
  Source: Task 12 triage amendment.

- **P0-6 OTAP Arrow receiver's own RESOURCE_EXHAUSTED.**
  Why: Task 12d made the OTLP receiver's retryable refusals UNAVAILABLE, but the
  OTAP Arrow receiver still answers RESOURCE_EXHAUSTED, which Alloy and other
  OTLP clients without RetryInfo drop.
  Done when: its concurrency refusals answer UNAVAILABLE, counted, as in the
  OTLP receiver.
  Source: Task 12d residual 7.

- **P0-7 durable_buffer WAL per-write sync option.**
  Why: an acknowledgement means written to the WAL and synced within about
  100 ms, so a host crash or power loss can lose the last 100 ms; quiver
  supports per-write sync (flush_interval 0) but durable_buffer does not expose
  it, and its cost is unmeasured.
  Done when: the option is exposed, its throughput cost is measured, and the
  README "Durability of the acknowledgement" offers it next to strict mode.
  Source: Task 12c fix round 1 F2.

## P1: performance and scale toward 1M records/s

Acceptance number: 100k-1M records/s from dozens to hundreds of producers
(Task 5: 684k on four worker cores, about six cores estimated for 1M;
buffered 144-152k bound by the WAL device).

### Milestones

- **P1-1 Shared writer with parallel preparation (flush off the ingest core
  folded in).**
  Why: one writer per worker duplicates descriptors N-fold, writes N file sets
  per window, scales at 0.67 from 1 to 4 workers, and runs merge, encode and
  zstd on the admission core, so a large flush stalls intake (up to 32.8 ms per
  stretch) and a flush longer than the window closes admission.
  Done when: spec 10.4 is revised (sections 3, 6, 7) and built with its own
  plan: per-worker caches deduplicated in a w-way merge at the window boundary,
  encoding on a bounded pool, one file set per window, parallel multipart parts,
  a process-wide ACTIVE + FLUSHING reservation; a Task 5 rerun shows the new
  scaling and no ingest-core flush stall. Bounded yielding in merge-key building
  and per-chunk encoding is needed only if this milestone slips.
  Source: spec 10.4; user decisions 2026-09-23 (together) and 2026-09-25
  (major milestone); Task 5 scaling; Task 3i deferred; fifth review 2026-09-23.

### Other P1 items


- **P1-2 durable_buffer WAL throughput.**
  Why: the buffered topology is bound by the WAL device: about 2x the wire bytes
  written, sync every 25 ms, segment finalization synchronous on the worker
  runtime (144k/s local, 152k/s MinIO, four workers on one NVMe).
  Done when: finalization runs off the worker runtime, the sync interval and
  segment size are configurable, and the buffered ceiling is re-measured.
  Source: Task 5; Task 12 triage amendment.

- **P1-3 CPU candidates.**
  Why: sort/seal/merge is 11-13 percent of engine CPU on row-format keys and
  conversion 20 percent of metrics CPU; extraction rebuilds identity rows for
  committed series and re-encodes the resource/scope prefix per series.
  Done when: each candidate is tried with a stage-bench number and kept or
  rejected: fixed-width sort keys (series_id + time); extraction straight from
  OTLP bytes via pdata views; streaming series_id from a resource+scope+metric
  prefix hash; skipping committed-series rows; one attribute materialization
  instead of three; k-way merge replace-top; typed per-dataset builders in
  extraction instead of the Col -> ValuesRow -> AnyBuilder layer with its
  runtime width check (extract/mod.rs ~643-699).
  Source: Task 4 attribution; backlog streaming series_id; umbrella review
  2026-09-25, performance; complexity review 2026-09-24 item 5.

- **P1-4 jemalloc heap-dump attribution of the memory residual.**
  Why: the ledger excess (33-47 MB per pair), the ~160 MB large-table flush
  workspace, the row-group tail pinning ~64 MB per large table, the per-run
  bookkeeping of chunk builders, the values builder over-charge and the RSS
  high-water 1.25x above the live formula (Task 12c) are not attributed.
  Done when: raw dumps symbolized offline name the stacks for each term, and
  each gets a fix, a charged term or a documented margin (row-group tail copy
  included if it matters).
  Source: Task 12 heap-dump amendment (runs after the reference deployment,
  backlog if it yields no fix); Tasks 3i, 6, 12c.

- **P1-5 Receiver in-flight byte bound.**
  Why: in-flight requests are bounded by receiver slots, not bytes (16.2 GB at
  4096 slots in strict mode), and the exporter cannot see the receiver's limits
  to state the bound.
  Done when: a receiver-level byte limit (engine, upstream) or an exporter-level
  retryable refusal exists, and the exporter logs the computed whole-process
  bound at startup from limits it can see.
  Source: Task 5 amendment; Task 12 triage amendment; Task 12b residual.

- **P1-6 Re-measure S6 encode after the ColumnPath fix.**
  Why: Task 12f fixed per-column writer properties (leaf_path split on '.'),
  which changes attribute-map encoding, and the S6 encode numbers predate it.
  Done when: the S6 stage family is rerun and FINDINGS updated.
  Source: Task 12f item 3.

- **P1-7 Conversion memory before the request budget.**
  Why: `try_into_with_default()` runs before `max_extracted_bytes` is charged,
  and the README reserves 4x `max_request_bytes` without a measurement.
  Done when: peak allocation on a 16 MiB body of empty log records and of
  key-only attributes is measured and the reservation matches it.
  Source: umbrella review 2026-09-25, needs verification.

- **P1-8 Hourly partition rollover under 100k+ hot series.**
  Why: the cache is keyed by partition, so every series is re-emitted at the
  hour boundary, and no throughput run crosses one.
  Done when: a capacity run crosses an hour boundary at 100k+ hot series and
  the stall and memory spike are within the documented bounds (or fixed).
  Source: umbrella review 2026-09-25, needs verification.

- **P1-9 Oldest-pending-age rotation trigger.**
  Why: with the buffer, longer windows cut file count, but a slow trickle then
  waits a full window before it is visible.
  Done when: an optional max-freshness trigger rotates a block by the age of its
  oldest request, with a test and a README line.
  Source: backlog freshness contract (3); plan 3 out of scope.

- **P1-10 Block sizing options B and C.**
  Why: `max_block_bytes` both rotates (file size) and caps memory, and an
  oversize request can only be refused.
  Done when: decided and, if taken, built: B separates `target_block_bytes`
  from `max_block_bytes`; C splits an oversize request across blocks with a
  multi-block token.
  Source: Task 12 option A amendment.

- **P1-11 Resumable extraction.**
  Why: admission work is one step per request; finer interleaving with control
  handling may be needed if the section 6.7 bound proves too loose at scale.
  Done when: measured against the 6.7 bound at the P1-1 load and built only if
  it fails.
  Source: spec 10.2.

## P2: format and data

- **P2-1 Compactor with seal markers.**
  Why: one file set per window per worker gives thousands of small files per
  hour, and after T11-F3 a compactor cannot treat the lateness bound as a
  completeness bound on stores that apply abandoned requests (RustFS).
  Done when: a stateless `series-lake-compactor` ships with all of the
  following, which closes T11-F3:
  - Sealing: hour H closes only when every live writer has written
    `_sealed/hour=H/writer=<writer_id>/<boot_id>`, after every PUT for H
    resolved.
  - Policy and sizes: triggered by size, file count or age, not cron (for
    example file_count >= 32, total bytes >= target, or oldest file >= 5 min);
    optional L1 minor compaction (~128-256 MiB) inside the open hour, skipped at
    low volume; final L2 at hour close of ~256-512 MiB per file (up to ~1 GB); a
    large hour becomes several `part-NNN.parquet` committed together by the
    watermark.
  - Sort order, declared in `sort_key` and native sorting_columns:
    metrics/values (metric_name, series_id, time); logs/values
    (time, service_name, series_id) benchmarked against
    (service_name, time, series_id); metrics/series (metric_name, series_id);
    logs/series series_id. Page index everywhere, Bloom filters on series_id
    (and trace/span ids for logs) in compacted files only.
  - Watermark and reader recipe: raw and compacted under separate prefixes (the
    writer never writes compacted, the compactor never raw); one
    `_compact_watermark` (compacted_through) for all four datasets, one atomic
    PUT after all outputs validate; a missing compacted object for a committed
    hour means an empty dataset; a reader reads the watermark once per query and
    reads hours at or below it only from compacted, later hours only from raw,
    never both; FORMAT.md and README recipes updated (today's `**/*.parquet`
    globs would double-read).
  - Recovery and GC: the compactor is idempotent (after a crash before the
    watermark moves it validates and reuses or rebuilds outputs); one output key
    is never published concurrently (conditional Complete with If-None-Match, or
    one compactor per scope); GC is a separate job deleting raw hours older than
    the watermark minus a grace of at least 2x the longest query (preferably
    hours).
  Source: spec 10.2; docs/superpowers/compaction-and-format-chat.md; Task 11
  T11-F3.

- **P2-2 Format batch 2.**
  Why: the format lacks an identity-config guard, a dedup key, a threat model
  and a v2 path, and its "revision 2" rendering change is recorded nowhere a
  reader sees (files still say `format_version=1`, `v=1`).
  Done when: all of these ship with FORMAT.md: `identity_config` hash in the
  footer and compaction scope plus `_lake.json` with startup refusal on
  mismatch; `ingestion_id` values column with the reader dedup recipe; threat
  model (unseeded xxh3, producer_id from telemetry, one trust domain); v2 path
  and version from the footer; `parquet.compression` removed until a second
  codec; `metric_type`, `temporality`, `is_monotonic` in metrics/values; a
  visible format revision (or the framing dropped); `body_bytes` for non-string
  log bodies.
  Source: user decision 2026-09-23; umbrella review 2026-09-25, minor issues.

- **P2-3 Data-model coverage.**
  Why: the exporter README "What this exporter does not keep" lists gaps with no
  decision: multivariate metrics, metric-level attributes (never read or
  validated), `dropped_attributes_count`, histogram sum/min/max all zero stored
  as null (OTAP omits all-default columns), zero or out-of-range timestamps as
  null, number and histogram points sharing one values dataset, log `flags` and
  out-of-range severity, arrival order among equal sort keys (by design, not yet
  stated in FORMAT.md).
  Done when: each gap has a decision (format change, validation, or documented
  limit); MetricAttrs are validated now; the zero-sum presence is fixed in the
  pdata transport first.
  Source: user note 2026-09-24; fifth review 2026-09-23.

- **P2-4 Typed attribute maps.**
  Why: attribute values are rendered to Map<string,string>, so 42 and "42"
  collide and bytes become base64; types survive only in identity bytes.
  Done when: a typed layout is specified and shipped as a format revision.
  Source: spec 10.2.

- **P2-5 Partition granularity, day or hour.**
  Why: at low volume a day partition gives fewer directories and files.
  Done when: configurable, included in the compaction scope and identity config.
  Source: spec 10.2.

- **P2-6 Idempotent replay by producer batch id.**
  Why: at-least-once duplicates come from WAL replay, retries after lost
  responses (single PUT, abort-after-complete stores) and producer restarts.
  Done when: a writer-side or compactor-side dedup by batch id removes replayed
  rows, with the source of the id decided (P2-2 `ingestion_id`, producer
  header).
  Source: spec 10.2; Task 12a residuals.

- **P2-7 Producer id from transport headers.**
  Why: producer_id comes from telemetry attributes, which a producer can set to
  anything.
  Done when: an option takes it from an authenticated transport header.
  Source: spec 10.2.

- **P2-8 Lossless high-cardinality mode.**
  Why: a unique attribute per point makes one series per point (series dataset
  at 66 percent of values, 1.9x CPU).
  Done when: a separate mode groups by stable fields, keeps varying attributes
  per point and stores the full identity separately, or is rejected with a
  reason.
  Source: fifth review 2026-09-23; Task 5.

- **P2-9 One refusal policy for invalid UTF-8 and repeated singular fields.**
  Why: nested invalid UTF-8 is refused while top-level is repaired to U+FFFD,
  series_parquet refuses repeated singular fields (valid protobuf) that other
  exporters accept, and it is unknown whether any producer or proxy
  concatenates OTLP messages.
  Done when: one policy across file, otap, parquet and series_parquet is
  documented, informed by a check of common producers.
  Source: umbrella review 2026-09-25, minor issues and needs verification.

- **P2-10 Commit manifests.**
  Why: readers cannot see block-atomic commits; may return as an audit record.
  Done when: decided against the P2-1 watermark and built or closed.
  Source: spec 10.2.

- **P2-11 Discovery index.**
  Why: partitions are by receive time, so a time-range query cannot prune by
  path and must prune files by per-file event-time bounds.
  Done when: footers (and the index) carry observed_time (logs) and start_time
  (metrics) bounds, and for compacted files min/max of metric_name and
  series_id and the compaction level; the per-hour index itself is built
  asynchronously if a measurement shows listing dominates, never a correctness
  dependency.
  Source: spec 10.2; compaction chat.

## P3: hygiene, upstream, harness

- **P3-1 Complexity-review refactors.**
  Why: the worker state machine, config mirrors and sink stack carry duplicated
  logic that the shared writer (P1-1) will touch anyway.
  Done when: done with P1-1: worker behind a narrow event API with intent-level
  methods and `drive` near 60 lines; notifier as a VecDeque with one live-token
  counter; LakeWriter facade { offer, seal, commit, abort } with reemit,
  commit-after-flush and cache classification in series-lake; one config form
  (lake sections equal user sections, exporter mirrors deleted);
  `PutLanded` merged into `PartLanded`, one gauge type with a drop guard;
  `Outcome` via `derive(AttributeEnum)`; `RefuseReason` split into permanent and
  block-scoped refusals; the `ParquetObjectWriter` adapter removed
  (`LedgeredWriter` implements `AsyncFileWriter` over `BufWriter`).
  Source: complexity review 2026-09-24 (item 4 for the adapter); deslop B6;
  fourth review 2026-09-23; umbrella 14; umbrella review 2026-09-25,
  architecture.

- **P3-2 Engine upstream issues.**
  Why: engine defects outside the exporter distort memory telemetry or fail CI.
  Done when: each has an upstream issue or PR: `pipeline.memory.usage` credits
  frees only to the allocating thread (10.3 GB against 550 MB RSS); memory
  accounting rebuilt on `retained_work` with a handle through PipelineContext
  (umbrella 8, consistency C2); pdata CBOR encoder recursion limit; flaky
  otel-arrow-dfe-telemetry log_tap hang, otlp_grpc_exporter test_otlp_exporter
  and opamp AddrInUse; parquet exporter's silent `continue` on a malformed body;
  whether object_store tracing events are routed (S3 throttling is otherwise
  invisible); jemalloc `background_thread:true` A/B on a standard pipeline,
  then its default hygiene (on for every glibc jemalloc build at main.rs ~136;
  a println! bypasses tracing and reports the option, not the threads; no CI
  assertion; benches differ): if kept, a structured `startup::system_info`
  event, an E2E default-feature assertion and benches on the same malloc_conf;
  if dropped, removed.
  Source: Tasks 5, 6; Task 12 triage amendment; umbrella review 2026-09-25;
  umbrella review 2026-09-23 major 7.

- **P3-3 Receiver refusal hygiene.**
  Why: after Task 12d one message still names two limits, oversize is not in
  `receiver.received{outcome=refused}`, and other decode failures (bad
  compression flag, unsupported encoding) are uncounted.
  Done when: each is counted under its own reason with a test.
  Source: Task 12d residuals 3, 5, 6.

- **P3-4 durable_buffer: persist the WAL cursor with the segment.**
  Why: a SIGKILL between segment fsync and cursor persist replays up to about
  100 ms of acknowledged entries (T10-F2, documented as at-least-once).
  Done when: the cursor is made durable together with the segment, or the
  window is closed another way, with a kill test.
  Source: Tasks 10, 13; Task 12a residual.

- **P3-5 Exporter operability leftovers.**
  Why: some signals an operator needs are missing.
  Done when: the start event logs base_uri, endpoint, resolved
  `unsigned_payload` and retry timeout; the refusal log gate carries producer
  identity per outcome; local-backend staging files (`<file>#N`) are documented
  and reclaimed; the README notes versioned buckets and object lock; the
  exporter README limits table and the lake config doc state what
  `ingress.max_request_bytes` measures (protobuf length for OTLP input,
  estimated Arrow bytes for OTAP input, so a batch or converting processor
  upstream can turn an accepted OTLP request into a permanent refusal).
  Source: umbrella review 2026-09-25, minor issues; Task 12f; S3 compatibility
  note 2026-09-23; umbrella 2026-09-22 finding 12, 2026-09-23 minor,
  consistency C12.

- **P3-6 S3 store matrix.**
  Why: only MinIO and RustFS are exercised (Azurite in Task 12g).
  Done when: a `workflow_dispatch` lane runs on real AWS S3 with secrets; Ceph
  RGW, Garage and SeaweedFS run in Docker; R2, B2, GCS XML, `checksum_algorithm`
  and S3 Express only on request; optional exact-key ListMultipartUploads sweep
  (T11-F1) and a startup check for the abort-incomplete lifecycle rule.
  Source: S3 compatibility note 2026-09-23; Task 11 T11-F1.

- **P3-7 Tests.**
  Why: some guarantees have no test or only a narrow one.
  Done when: added: factory with a bound bearer-token capability (engine test
  helper); DuckDB/ClickHouse reading native SortingColumn; delivery-model
  proptest asserting `!nack.permanent` over all ten faults; deterministic
  phase-2 cancellation between the last part and completion; `merge_key_bound`
  checked at compile time against arrow-row; wall-clock windows off llvm-cov;
  the live fault matrix as a CI lane; an oracle proptest mixing gauge, sum and
  histogram points in one request; `memory.accounted` back to baseline after
  ack, nack and abandon (check tests/metrics.rs and sink/tests.rs first); an
  exact-boundary admission test at 1,000,000,000 ns if absent.
  Source: backlog tests; umbrella reviews 2026-09-23 and 2026-09-25, tests;
  Tasks 12a, 12b residuals.

- **P3-8 Qualification soak and failpoints.**
  Why: Task 15 runs nightly-length chaos (at least 4 h); spec 10.3 also asks for
  24-72 h qualification, a nightly lane and failpoint builds.
  Done when: a 24-72 h run passes the Task 15 acceptance and a nightly lane
  exists; failpoints only if Task 15 exposes a gap.
  Source: spec 10.3.

- **P3-9 Task 13 proofs not run.**
  Why: strict vs buffered acknowledgement latency at 15 s and 120 s windows,
  mid-window kill replay without resend at both windows, NACK/backoff timing
  against the jittered envelope, lost completion followed by SIGKILL, the
  buffer heap versus mapped split, and a kill within 100 ms after a block commit
  are unmeasured.
  Done when: run with the WIP harness kept in the campaign workspace
  `task-13-wip/`, or closed as covered by Task 15.
  Source: Task 13 cut 2026-09-25; Task 12c F-G.

- **P3-10 Harness polish.**
  Why: the memory ledger and stage baselines carry known skews and open
  questions.
  Done when: synchronous telemetry/allocator pairing; measurement conditions in
  every result; heap counters in stage benches; noop-control RSS decision with
  written rationale; strict memory baseline at the default fingerprint; logs
  attribution baseline; pipeline baseline CPU dose of several seconds; the encode
  first-repetition RSS and upload wall CV explained; engine_runtime 0.42 and
  metrics upload 2.27 explained; quiet-host re-measurement; byte-calibrated
  families (faults, ledger, capacity) rerun after the key reservation; harness
  README without one host's CPU layout.
  Source: FINDINGS open questions and Deferred tables; Task 12b residual;
  umbrella review 2026-09-25, docs.

- **P3-11 Metric sets grouping.**
  Why: at most three attribute sets were proposed, which would drop labels of
  several counters, and worker `acks` duplicates ExporterExportMetrics.
  Done when: decided with the shared-writer telemetry or at upstream PR time.
  Source: Task 3f item 10.

- **P3-12 Upstream PR logistics.**
  Why: the upstream branch must carry only the needed minimum.
  Done when: plan 3 "After Task 14" is executed (de-slop, clean branch from
  origin/main, one commit per PR) and these are settled with it: core-nodes vs
  contrib-nodes placement with the maintainers; Python lane under tools/;
  `#[non_exhaustive]` or changelog for `shutdown_completions` and
  `StorageType::S3.unsigned_payload`; series-lake test scaffolding out of the
  public API and benches out of the published crate; drive-by changes dropped;
  parquet, file and otap READMEs document the framing refusal and the
  `otlp.malformed_body` WARN; the parquet exporter event rename
  `parquet.exporter.retry_ignored_for_file_storage` ->
  `object_store.retry_ignored_for_file_storage` (5f0135e73) gets its own
  `breaking` changelog entry naming both in the object_store commit. Opening
  PRs remains the user's call.
  Source: plan 3 "After Task 14"; umbrella review 2026-09-25, minor issues and
  finding 6.

- **P3-13 Engine settles contexts when a node task dies.**
  Why: an AckToken has no Drop fallback, so if the exporter task panics or its
  start future is dropped in a live process, held requests may stay undecided
  and producers hang until their timeout.
  Done when: a test or a recorded code reading states what the engine does and,
  if contexts are dropped, the node failure nacks them retryably, with a README
  line.
  Source: umbrella review 2026-09-23, needs verification; plan 3 Task 3j.

- **P3-14 durable_buffer directory ownership and dispatch docs.**
  Why: quiver takes no exclusive lock on `path/core_<id>` although the engine
  README says old and new runtimes overlap during live reconfiguration; the
  buffer module doc (mod.rs ~27-38) and config.rs ~6-13 recommend
  RoundRobin/Random/LeastLoaded, which no longer exist (one_of, broadcast); the
  buffer README lacks the per-process directory rule, the no-migration drain
  procedure on a core change and the per-core split of retention_size_cap.
  Done when: quiver locks the WAL directory and refuses a second opener (or an
  upstream issue is filed), the dispatch table is rewritten, and the buffer
  README states the three rules.
  Source: docs/superpowers/parallel.md (user note 2026-09-25).

## Deferred features

- **Live access to buffered data.** Why: `tail -f` and buffer inspection for
  accepted but uncommitted data. Done when: its own spec is written. Source:
  spec 10.1; user 2026-09-25: not now.
- **Traces, exponential histograms, summaries, exemplars.** Why: refused or
  dropped and counted today. Done when: each has a spec choice and a format
  revision. Source: spec 10.2; data-model note 2026-09-24.
- **Spatial aggregation processor.** Why: nothing drops attributes and merges
  colliding streams with temporality-correct aggregation (Task 3h: a collapsed
  cumulative pair is two rows under one series). Done when: raised upstream as a
  separate node building on temporal_reaggregation. Source: user question
  2026-09-23.
- **Query-side indexes (postings, Tantivy).** Why: Parquet cannot prune
  arbitrary `attrs` MAP predicates or full-text body search. Done when:
  profiling shows one is the bottleneck, then a postings index (metric_name or
  attribute key=value -> series_id, Roaring/FST) for metrics/series or a
  Tantivy index for the logs body and selected attrs, built only on compacted
  outputs and published before the watermark advances; trace_id stays on Bloom
  filters and the page index. Source: compaction chat.

## Done / removed

- Receiver OUT_OF_RANGE for an oversize message, load-shed uncounted and its
  misleading message: Task 12d.
- Multipart E2E, NoSuchUpload after a retried Complete, orphaned uploads after
  kill, 5xx during UploadPart and Complete: Tasks 9-11 (F1 fixed in Task 12a).
- `unsigned_payload` exposure, ColumnPath: Task 12f.
- Umbrella 2026-09-25 minors fixed in Tasks 12e/12f: partial `retry:` section,
  `writer_id` with `-`, `window.interval` cap, panicked flush as Internal,
  creation dropped in `finish()`, `late_commits{outcome}`, abort events with
  keys, notify rate limit, receiver_limit INFO once, lifecycle rule and metrics
  URL in configs, local config ceiling and path, `shutdown_deadline()` doc, a
  tighter Shutdown during completion.
- Buffered vs strict acknowledgement semantics documented: Task 12c operator
  guide.
- Quiver dropped without `shutdown()`: answered in Task 12e (no loss,
  duplicates possible). Receiver-first drain race: fixed in Task 12e; SIGTERM
  restart under load covered by Task 15.
- Nightly soak with storage faults and restarts: Task 15 (qualification stays
  as P3-8).
- "Tested on S3-compatible stores" wording: adopted for the Task 14 report.
- `part_bytes` validated against 5 GiB and 10,000 parts: series-lake
  src/config.rs ~478-481, 652.

Merged duplicates: worker scaling 0.67 and flush off the ingest core into P1-1;
bounded yielding into P1-1; streaming series_id and umbrella extraction costs
into P1-3; row-group tail into P1-4; receiver startup log line into P1-5;
fourth-review refactoring, lake-level writer facade and RefuseReason into
P3-1; histogram zero sum (fifth review) and data-model typing bullet into
P2-3; format revision record and `body_bytes` into P2-2; engine memory
accounting and `pipeline.memory.usage` into P3-2; Alloy line truncation into
P0-3.
