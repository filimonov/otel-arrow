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
3. **Compactor** (spec 10.2). One file set per window per worker means
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

## Other deferred items

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
- Upstream: pdata CBOR encoder recursion limit; PR split; Python lane under
  tools/; history rewrite before the upstream PR (user decision).
