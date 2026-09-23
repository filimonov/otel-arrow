# Task 3f report (interim: items 7 and 13 pending the final Task 3i commit)

Worktree: <repo>/.claude/worktrees/agent-a34d9a5c685620f7b
Branch: worktree-agent-a34d9a5c685620f7b, rebased onto series-parquet-exporter 92c4a35f3 (lead ruling), HEAD 466e9d34b.

## Commits (history order)

1. refactor(series_parquet): two refusal vocabularies, lake::Error classes and one Outcome (item 2)
2. refactor(series-lake): Block without a token type, sharing one Arc<LakeConfig> (item 5)
3. refactor(series_parquet): drop the config rule the lake already checks (item 4)
4. refactor(series_parquet): reason sentences as small Display structs (item 9, plus golden sentence test)
5. refactor(otap): StorageType::kind names the backend for the start event (item 2 addendum)
6. refactor(series-lake): the retry classification lives beside the error wrapping (item 2 addendum, Error::is_retryable)
7. refactor(series-lake): a refusal displays as text, not as Debug output (item 2 addendum)
8. refactor(series-lake): one point path for number and histogram metrics (1/2) (item 8)
9. refactor(series-lake): column builders from arrow's make_builder (2/2) (item 8)
10. docs(series_parquet): code comments cite FORMAT.md and the READMEs, not the spec (item 14, except sink.rs/sort.rs)
11. test(series_parquet): split tests.rs by topic with a shared support module (item 11) -- 466e9d34b

## Per item

- Item 2: lake::Error = Refused(RefuseReason) | Transient(TransientError: ObjectStore, Cancelled, DeadlineExceeded, AbortFailed) | Internal(InternalError: Arrow, Parquet, Invariant). Display strings unchanged; unused Pdata variant removed. Exporter Outcome moved to outcome.rs, derives AttributeEnum (replaces NackErrorType; nack variants in the old label order, Ack bucket never touched so never reported). Outcome::of is the single classification; nack_cause/refused/label/sentence/explain derive from it. worker::Failure, classify, reservation_failure and the Outcome->NackErrorType table deleted. Block-scoped refusals map to Internal (what the reachable reservation path reported). Table test covers every RefuseReason/SizeBudget -> Outcome -> (NackCause, permanent, error.type).
  Addenda (umbrella review): StorageType::kind() replaces Debug parsing; lake::Error::is_retryable() moved from exporter flush.rs, table test per class and wrapping; RefuseReason/SizeBudget Display ("refused: {reason}"), BlockFull/TooManyRequests keep their names so every producer-visible sentence stays byte-identical (golden test).
- Item 5: Block has no type parameter; counts requests (usize); worker rotation uses block.request_count() (one counter); Block holds Arc<LakeConfig>; reserve/reserve_with_reemit read it. Unused into_parts removed. full_block_refuses_with_block_full rewritten to build the block under the tighter config (same refusal checked).
- Item 4: no JSON rewriting remains (typed serde annotations only). Removed the duplicate max_row_bytes rule (identical lake message). Kept window.max_block_bytes, window.interval, window.max_requests_per_block pre-checks because the lake names lake keys. validate() unwrapped with invalid_detail.
- Item 8: Common::append_points is the one number/histogram path with a per-kind closure; column read order preserved so refusal precedence and byte charges are unchanged. AnyBuilder replaced by arrow make_builder (same 1024-row capacities; typed downcast per cell).
- Item 9: Outcome::explain is a dispatch to Display structs (TooLarge, NotStored, TooDeep, InvalidContent, StorageFailed, InternalFailure, BlockWriteFailed).
- Item 10: no-op per lead ruling. The 8 sets and keys: worker (none); flushes {reason: FlushReason}; nacks {error.type}; rows/files.written {signal, dataset}; series.emitted {reason: EmitReason}; dropped.unsupported {kind}; dropped.exemplars {signal}; denormalize.type_mismatch {column, registration}. No two share an attribute schema (the two `reason` keys have different value sets). The three-set grouping and the acks vs ExporterExportMetrics duplication are a plan 4 / upstream-time question.
- Item 11: tests/ = mod.rs, support.rs (all shared helpers: builders, simulated clocks, gated/fault stores for the store override, capture), admission, rotation, flush, shutdown, config, metrics. 87 tests moved verbatim with Scenario/Guarantees, names identical.
- Item 14: code references to spec sections replaced by FORMAT.md/README sections or dropped where self-explanatory; exporter README embedded Python driver and unittest replaced by pointer to DockerSlice.test_minio in test_e2e.py with its command. Remaining: sink.rs (9 refs) and sort.rs (1 ref), deferred to the post-3i step with item 7.
- Items 7 and 13: NOT DONE, held by the lead until the final Task 3i commit.

## Test counts

| Suite | Before (c82d0520a) | After (466e9d34b) |
| --- | --- | --- |
| series-lake lib | 166 | 168 |
| series-lake integration fuzz_canonical/fuzz_extract/golden/golden_roundtrip/oracle | 3/5/6/2/3 | 3/5/6/2/3 |
| core-nodes series_parquet | 99 | 101 |
| measurement bench --self-test checks | 11 | 11 (item 7 will remove diagnostic_encoding_matches_sink by design) |
| otap object_store tests | - | 44 pass (new kind test) |

Clippy -D warnings clean (series-lake all targets + bench-harness, core-nodes series-parquet all targets, otap all features). fmt clean. sanitycheck OK. Golden regeneration (tools/gen_golden.py) diff: identical.

Per the lead's gate policy change, xtask check, E2E and stage spot-measurement are deferred until review approval.

## Concerns

- Dropping the duplicate max_row_bytes check changes which error a multiply-invalid config reports first; single-error messages unchanged.
- make_builder adds a per-cell dynamic downcast in extraction; isolated in its own commit for revert if the extract spot-measurement regresses.
- Item 2 addendum touched the otap crate (StorageType::kind).
- Item 8 is two commits rather than one (for the revert isolation above).

## S4 (items 7, 13, 14 rest, B8, 3i finding 3, C2) and review fixes

Branch worktree-agent-a34d9a5c685620f7b, rebased onto series-parquet-exporter bdd44828c (3i closed at 4cbc696d9, then S1). Tip 540157cf6. The earlier commits above were rebased and have new ids (see the list below).

### Commits since bdd44828c, history order

1. 854bf37f3 item 2, two refusal vocabularies and one Outcome
2. b2c18ea40 item 5, Block without T, Arc<LakeConfig>
3. c13bb5aaf item 4, duplicate config rule dropped
4. c2a4f4f46 item 9, reason sentences as Display structs
5. 61f8199ba item 2 addendum, StorageType::kind
6. 790c3eda1 item 2 addendum, Error::is_retryable
7. 5a10ff68d item 2 addendum, RefuseReason Display
8. ec9c25d41 item 8, one number/histogram point path
9. 6ec7696c9 item 8, make_builder (reverted by 11)
10. dee896bde item 14, spec references outside sink/sort, README pointer to test_e2e.py
11. f71bfee6a item 11, tests.rs split (S1's new test and changed saturated test carried into tests/shutdown.rs unchanged; message updated to 88 tests / 102)
12. 08f80c3bf review fix: revert of make_builder
13. 65f24dfcd review fix: buffer.rs reemit comment reference
14. ea03623db B8: MergeIter::step infallible, last_step_rows cfg(test), drain() test helper
15. ae7d7b4df 3i finding 3: ChunkBuilder bound documented for the lake's column types only; fallback is one unbounded step and may panic on i32 offset overflow for some types
16. 5cdf42802 item 7 (1/3): sink.rs -> sink/{mod,naming,properties,write,tests}.rs, pure move (line-multiset check: only rustfmt reflow differences)
17. b89254766 item 7 (2/3): sink::writer_properties(compression) builder, sink::compression(), sink::row_group_full(); sink and benches call them; bench's copied properties, sink_equivalence, Equivalence, the diagnostic_encoding_matches_sink self-test and the stage fixture check removed
18. 7bfe47510 item 7 (3/3), B6 scope: Sink::checkpoint (yield + cancel check) and write_chunks with ?; yields, cancellation points, accounting updates and release order unchanged; UploadLedger and CreationWatch unchanged
19. d57d811a7 item 13: Sink::new takes StartAbortTimer (fn(Duration) -> AbortTimer future), started where the failure/cancellation is first seen and awaited in the unwinding step and the abort; SinkClock and its tokio default removed; exporter passes the engine clock with deadline_at; tests/oracle/benches pass tokio. No tokio::time in series-lake production code.
20. 24146fdea item 14 rest: sink/ and sort.rs comments cite FORMAT.md sections 4/5; also removed "plan 3" (sort.rs, worker.rs) and "Task 6" (bench test comment)
21. 85ff988e9 C2 (1/2): series-lake hook_store::{HookStore, StoreHooks, HookGuard} with default hooks before_put / before_multipart (may fail or return a guard held across the call) / wrap_upload; CreationWatch = HookStore<Creations> (same counting, settle wake-up, guard lifetimes, Display); sink tests' four wrappers become hooks
22. 540157cf6 C2 (2/2): exporter GatedStore and FaultStore become HookStore hooks; FAULT_* u8 modes -> enum Fault, dead mode 4 removed, ValuesOnce heals under the mode lock; SeqCst/AtomicUsize one import

Not done, with reason:
- parquet_exporter's FailPutForPrefixStore not moved to HookStore: the parquet exporter does not depend on series-lake, so moving it would add a dependency beyond test code; other agents also work on exporters.
- B6 extras (dropping ParquetObjectWriter, merging PutLanded/PartLanded, one Gauge for MergeKeys/FlushWorkspace) are outside the S4 brief ("the parts listed here") and left alone.

### Checks on the tip 540157cf6

| Suite | Result |
| --- | --- |
| series-lake lib | 172 passed (base 4cbc696d9: 172) |
| series-lake integration fuzz_canonical/fuzz_extract/golden/golden_roundtrip/oracle | 3/5/6/2/3 |
| core-nodes series_parquet | 102 passed (101 + S1's new test) |
| measurement bench --self-test | passes, 10 checks (11 -> 10 by design) |
| clippy -D warnings | clean: series-lake (all targets, bench-harness), core-nodes (series-parquet, all targets), otap |
| bench-heap benches | compile |
| golden regeneration diff | identical |

Per-commit checks (clippy both crates + lake lib + core series_parquet) ran green for the first 9 commits after the rebase before the lead's speed-up ruling stopped them; the tip is checked in full.

sanitycheck: fails only on three pre-existing campaign docs that are not mine (docs/superpowers/deslop-plan-2026-09-23.md, docs/superpowers/s3-compatitibility.md, docs/superpowers/reports/series-parquet-measurement/campaign/ledger.md: non-ASCII). No file of this task is flagged.

Not run (gate policy): cargo xtask check, E2E, stage spot-measurement.

### S4 concerns

- Sink::new gained a required fourth argument (the abort timer); every caller changed (exporter, oracle, benches, tests).
- The sink/encode stage results no longer carry the diagnostic_encoding_matches_sink fixture check or the "equivalence" extra; no Python reads them. The encoder now shares writer_properties and row_group_full with the sink instead of being cross-checked.
- HookStore is a new public series-lake type (used by production CreationWatch and by the exporter tests).
- Test the_abort_is_bounded_on_the_injected_clock renamed to the_abort_is_bounded_by_the_callers_timer (count unchanged).

### Part-2 review fix

- d1c9a7d7b: Fault::ValuesOnce is again taken only by an operation whose mode captured at entry was ValuesOnce (the guard the u8 compare-exchange gave). core-nodes clippy -D warnings clean; series_parquet 102 passed.
