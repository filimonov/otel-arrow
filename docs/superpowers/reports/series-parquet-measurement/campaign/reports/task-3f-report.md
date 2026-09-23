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
