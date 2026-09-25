# Plan 4 backlog (series_parquet exporter)

Status: backlog, not a plan yet. Plan 4 is written from the plan-3 Task 12
classification and the Task 14 report; this file only collects what has been
deferred to it, with its origin, so nothing is lost. Ordered by the user's
priority where stated.

## Priority 1 (user, 2026-09-23: "changes the picture most")

1. **Shared writer per process with parallel preparation** (spec 10.4). One
   writer instead of one per worker: shared series cache (removes N-fold
   descriptor duplication per partition with N workers), w-way merge of all
   workers' sorted runs at the window boundary, one file set per window,
   multipart upload with parallel parts. Task 5 supplies the measured cost
   of per-worker duplication and scaling efficiency.
2. **Flush off the ingest core.** Merge, encode and zstd run through
   spawn_local on the same core as admission, so a large flush stalls intake
   for tens of milliseconds per chunk and a flush longer than the window
   closes intake. Move flush to a bounded pool (or into the shared writer of
   item 1), keeping the ACTIVE + FLUSHING memory bound. Task 5 measures the
   admission-closed duration first. User decision 2026-09-23: do it together
   with item 1.
3. **Compactor** (spec 10.2), shaped by docs/superpowers/compaction-and-format-chat.md: one hourly high-watermark object instead of per-file manifests (readers use compacted files at or below the watermark and raw files above it, never both for one hour); per-writer seal markers `_sealed/hour=H/writer=<writer_id>/<boot_id>` written only after every PUT of that writer for H has resolved (success or confirmed abort; an unknown outcome is resolved by HEAD first), and the compactor closes H only when all live writers have sealed it; re-sort on compaction (metrics/values by metric_name, series_id, time; logs/values time-first, to be decided from queries); page index everywhere and Bloom filters on series_id (and trace/span ids for logs) in compacted files only; several compacted files per hour above a target size; GC of raw files after a grace period, decoupled from the compactor. Originally: One file set per window per worker means
   thousands of small files per hour at 15 s and 32 cores. A stateless
   `series-lake-compactor` with a Quickwit-style policy over the existing
   compaction scope.
4. **Format batch 2** (user decision 2026-09-23: plan 4, not before plan 3
   ends):
   - `identity_config` hash (series_attributes, producer_id_attribute and any
     other identity-affecting setting) in the Parquet footer and in the
     compaction scope, plus a `_lake.json` marker in base_uri; refuse at
     startup when the configured identity differs from the lake's.
   - `ingestion_id` (request UUID) as a values column, the dedup key for
     at-least-once replays, with the reader recipe in FORMAT.md.
   - Explicit threat model in FORMAT.md (unseeded xxh3, producer_id taken
     from telemetry, single trust domain).
   - Path to v2: what may change without a new `v=`, and how a reader learns
     the version from the footer, not only from the path.
   - Remove `parquet.compression` until a second codec exists.
   - `metric_type`, `temporality` and `is_monotonic` duplicated into metrics/values (dictionary/RLE makes them nearly free), so a values file is self-describing without the join to series.

## Other deferred items

- Buffered-topology freshness and loss contract (user review, 2026-09-23): (1) an end-to-end freshness SLO metric -- age of the oldest record accepted by the receiver but not yet visible in a values file -- combining the buffer's oldest undelivered segment age and the exporter's `oldest_unacked.age`, with alerting guidance on it and on WAL fill; (2) refuse permanently-invalid requests BEFORE the WAL acknowledges them (run the exporter's framing/size/support checks, or a cheap subset, at or before the buffer), so a producer is told instead of the buffer silently dropping an acknowledged bundle; (3) an optional "max freshness" rotation trigger (oldest-pending age) so a slow trickle does not wait a full window; (4) documentation of the semantics change: with the buffer, success means "durable on the local WAL", without it "durable in the object store".

- Larger CPU candidates from the Task 4 attribution (2026-09-23): sort on fixed-width keys (series_id + time) instead of row-format keys materialised for every row (sort/seal/merge 11-13 percent of engine CPU); extract directly from OTLP bytes via pdata views, skipping OTAP Arrow construction (conversion is 20 percent of metrics CPU, 8 percent of logs).

- Spatial aggregation processor (user question 2026-09-23): otap-dataflow has `processor:attribute` (delete/hash) and `processor:temporal_reaggregation` (temporal only) but nothing that drops attributes AND merges the colliding streams with temporality-correct aggregation (sum cumulative totals per stream with reset handling, sum deltas, chosen function for gauges), the equivalent of SDK Views or the Go collector's aggregate_labels. A separate node, worth raising upstream; temporal_reaggregation already tracks streams and cumulative state.

- Fifth review (2026-09-23): histogram `sum` of exactly zero becomes null after an OTAP round trip because the transport omits an all-default column; fix in the pdata transport layer by preserving presence, then in the exporter (pdata PR, upstream-relevant). A lossless high-cardinality mode (physical grouping by stable fields with varying attributes kept per point and the full stream identity stored separately) is a format change and a separate mode, not a hash tweak. Bounded yielding inside merge-key building and per-chunk encoding, if the flush does not move off the ingest core first.

- Refactoring folded into the shared writer (fourth review, 2026-09-23): the worker state machine behind a narrow event API (on_pdata, on_window, on_flush_done, on_cleanup_done, on_shutdown, one private after_slot_freed, futures out instead of fields, metrics and accounting injected at construction); the notifier as a plain VecDeque of (token, outcome) with one live-token counter and a real await-until-deadline drain; and the LakeWriter facade { offer, seal, commit(FlushReport), abort } with LakeConfig::workspace_bytes() next to the allocating code and private Block fields.

- Oversize requests: option B, separate `target_block_bytes` (rotation, file
  size) from `max_block_bytes` (memory cap); option C, split an oversize
  request across blocks with a multi-block token.
- Streaming series_id: compute it from a resource+scope+metric prefix hash
  plus point attributes, without materialising canonical bytes per point.
- Engine memory accounting rebuilt on `retained_work` with a handle through
  PipelineContext, engine-namespaced metric, its own engine PR (umbrella 8,
  consistency C2).
- Lake-level writer facade: reemit rule, commit-after-flush and cache
  classification move into series-lake (umbrella 14).
- Live access to buffered data (spec 10.1).
- Traces, exponential histograms, summaries, exemplars; day/hour partition
  granularity; typed attribute maps; idempotent replay by producer batch id;
  producer id from transport headers; commit manifests; discovery index;
  resumable extraction (spec 10.2).
- Long-run program: nightly hours-long runs, 24-72 h qualification, random
  chaos, failpoints (spec 10.3).
- Tests: factory with a bound bearer-token capability (needs an engine test
  helper); DuckDB/ClickHouse actually using native SortingColumn.
- Upstream: decide core-nodes vs contrib-nodes placement with the maintainers; pdata CBOR encoder recursion limit; PR split; Python lane under
  tools/; history rewrite before the upstream PR (user decision).
- Metric sets grouping (3f item 10, deferred): reviewer proposed at most three sets (none; {signal,dataset}; {error.type}); folding would drop labels of flushes{reason}, series.emitted{reason}, dropped.unsupported{kind}, dropped.exemplars{signal}, denormalize.type_mismatch{column,registration}; also worker 'acks' duplicates ExporterExportMetrics outcome=success. Decide with the shared-writer telemetry redesign or at upstream PR time.

## S3 compatibility review (user note, 2026-09-23)

Source: docs/superpowers/s3-compatitibility.md (in Russian). The user asked to look at it, not to follow it blindly. Controller assessment per point:

- Multipart path in E2E: valid. Only `test_short_client_waits_duplicate_rather_than_lose` sets `part_bytes: 5MiB` with 8 MiB blocks, and nothing checks that a file actually crossed a part boundary; the rest of E2E likely writes single PUTs. Cheapest fix: one E2E case per store with a file above `part_bytes`, asserting the multipart calls in the server trace (MinIO `mc admin trace`). Candidate for plan 3 Task 9 rather than plan 4.
- Complete retried after it succeeded (`NoSuchUpload`): already covered by the planned dropped-completion probe (Task 9) and the late-commit log and counter (Task 3j, major 11). Nothing new beyond checking the answer per store.
- Orphaned multipart uploads after SIGKILL or an abort timeout: valid and cheap. Tasks 9 and 10 should list incomplete uploads after each scenario and expect zero or a known number. Also consider a startup check or README warning when the bucket has no abort-incomplete-multipart lifecycle rule.
- Store-specific error codes (SlowDown, 503, RequestTimeout, TLS reset): partly covered by the Task 8 fault rig on real MinIO/RustFS; add 5xx during UploadPart and on Complete if the rig can inject per-operation.
- Upper bound on `part_bytes` (5 GiB per part, 10,000 parts): trivial validation, take it with the next config change.
- Versioned buckets and object lock: frozen names plus retries create versions; one README line.
- Not exposed: `unsigned_payload` (sometimes needed behind proxies; Task 5a already measures unsigned payload on TLS), `checksum_algorithm`, S3 Express. Expose `unsigned_payload` only if Task 5a shows a gain or a user needs it.
- Store matrix: a `workflow_dispatch` lane on real AWS S3 with secrets is the useful one; Ceph RGW, Garage and SeaweedFS in Docker are cheap additions; R2, B2 and GCS XML interop only on request. Azurite remains the plan 3 gate.
- The note's conclusion, that "tested on S3-compatible stores" should not be claimed before the multipart and completion points are checked, is adopted for the final report wording.

## Complexity review of 2026-09-24 (deferred by user decision)

Source and triage: docs/superpowers/complexity-review-2026-09-24.md.
- Worker owns its state: intent-level methods (`rotation_ready`, `rotate_and_resume`, `refuse_forced`, `cleaned`) and futures for the select, private fields, one notifier-credit calculation; `drive` down to about 60 lines. Do it with the worker state machine and the shared writer.
- One config form: lake sections equal the user sections, `Error::Config` in lake, delete the exporter mirror structs and the duplicated validation rules (deslop B9).
- Sink write stack: merge `PutLanded` into `PartLanded`; one gauge type with a drop guard for merge keys and flush workspace; drop the `ParquetObjectWriter` layer if the shared writer keeps this stack.
- `Outcome` with `derive(AttributeEnum)` and `Outcome::ALL` instead of the hand table.

## Data-model coverage gaps (user note, 2026-09-24)

What the OTAP and OTLP models carry and series_parquet does not keep. The exporter README section "What this exporter does not keep" is the user-facing list; this is the work list. Each item is a format change unless marked otherwise, so it belongs with format batch 2 or a later format revision.

- Signals: traces (OTAP Spans, SpanAttrs, SpanEvents, SpanLinks and their attributes; the plain parquet exporter writes them; today a trace request is refused as a whole). Profiles are not in the OTAP model at all, so they wait for the engine.
- Metric point kinds: exponential histograms and summaries (the engine carries ExpHistogramDataPoints and SummaryDataPoints with attributes; today dropped and counted, `unsupported: drop` becomes the default in slice S8); exemplars for number and histogram points with their filtered attributes, trace_id and span_id (six OTAP payload types; today dropped and counted by default); multivariate metrics (payload type 25, never read); metric-level attributes (MetricAttrs, never read or validated: at least validate them now, not a format change).
- Fields not stored: `dropped_attributes_count` on resource, scope, log record and point; arrival order among equal sort keys (by design; say so in FORMAT.md).
- Typing the format flattens: attribute value types in `attrs`, `resource_attrs`, `scope_attrs` (all rendered to Map<string,string>: 42 and "42" collide, bytes become base64, nested values JSON text; types survive only in identity bytes, series_id and typed denormalize columns); non-string log bodies rendered as JSON text (the `body_bytes` column is already deferred to the next format version); histogram sum/min/max that are zero in every point of a request arrive as absent OTAP columns and are stored as null (a transport limit the format could compensate for with a presence flag); zero or out-of-range timestamps stored as null.
- Layout: number and histogram points share one values dataset, so half the rows of a mixed stream hold null in value_* and the other half in count/sum/min/max, which weakens Parquet statistics; consider per-kind datasets or row groups with the compactor.
- To check before deciding: span-like `flags` handling and out-of-range severity numbers in logs (the standard OTAP log columns are all present).

## Plan-3 findings triaged to the backlog (user, 2026-09-25)

Scope rule: plan 3 fixes the exporter and correctness defects of the shipped
topology; throughput and limits of other components wait here.

- durable_buffer WAL throughput (Task 5): the buffered topology is bound by the
  WAL device (about 2x the wire bytes written, sync_data every 25 ms, segment
  finalization synchronous on the worker runtime); 144k/s local and 152k/s
  MinIO with 4 workers on one NVMe. Move finalization off the worker runtime
  and expose the sync interval and the segment size.
- Receiver in-flight byte bound (Task 5): the receiver holds every in-flight
  request, bounded by slots, not bytes (16.2 GB at 4096 slots in strict mode).
  A receiver-level byte limit (engine, upstream) or an exporter-level retryable
  refusal, plus a startup log line with the computed bound. The README formula
  itself is written in plan 3.
- Engine `pipeline.memory.usage` credits frees only to the allocating thread,
  so it grows without bound when blocking-pool threads free buffers (Task 5,
  local store: 9 MB to 10.3 GB at RSS ~550 MB). Upstream issue.
- Receiver load-shed (tower GlobalConcurrencyLimitLayer, RESOURCE_EXHAUSTED)
  is not counted, and its message "Too many active requests for the
  connection" names a connection limit where the limit is per worker.
  Upstream.
- The OTLP gRPC receiver answers an oversize message with OUT_OF_RANGE, which
  clients retry forever; answer with a non-retryable status and count it in
  `receiver.otlp.requests.rejected`. Upstream PR.
- Row-group tail pins the previous row-group buffer (~64 MB per large table,
  Task 3i); back to plan 3 only if the Task 12 heap dumps show it matters.
- Flaky tests in crates the campaign does not touch: otel-arrow-dfe-telemetry
  log_tap hang (18 min at 0 CPU), otlp_grpc_exporter test_otlp_exporter and
  opamp test_client_configured_with_client_tls_from_files AddrInUse.
- Harness polish: synchronous telemetry/allocator pairing for the ledger
  (one-sample skew), measurement conditions recorded in every result, heap
  counters in the stage benches.
- Worker scaling 0.67 from 1 to 4 workers (0.81 x 0.82) and ~6 cores
  estimated for 1M records/s: addressed by the shared writer (priority 1).
- Task 13 proofs not run (user cut Task 13 to 20 minutes, 2026-09-25; the
  questions that matter for the shipped deployment are answered by the Task 12
  Alloy + buffered reference validation): strict vs buffered acknowledgement
  latency at 15 s and 120 s windows; mid-window kill replay without resend at
  both windows; NACK/backoff timing against the jittered envelope; lost
  completion followed by SIGKILL; the Task 6 buffer heap versus mapped split.
  A work-in-progress harness for them (test_buffered.py, faults/capacity
  changes, fast contract tests passing) is kept outside the repository in the
  campaign workspace `task-13-wip/`.
- A durable_buffer bundle that fails conversion is rejected without
  `resolved{outcome=...}` (durable_buffer_processor/mod.rs:1426-1430); only
  `conversion_failed` records it.
