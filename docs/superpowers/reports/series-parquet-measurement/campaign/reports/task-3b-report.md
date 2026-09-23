# Task 3b report: exporter correctness under faults

Branch `series-parquet-exporter`, starting at `42be4c5d3`. Fifteen commits,
not pushed. Every item was reproduced against HEAD at the time it was worked
on, got a regression test that was run red first, then a bounded fix, then a
green run. "Red at HEAD" means the test was run against the previous
commit's version of the fixed files with only the new test added, unless
stated otherwise.

## Commits

| Commit | Scope |
| --- | --- |
| `8ce4a3fab` | Item B and C12: `Error::Internal`, only `Refused` is permanent, reason sentences |
| `2bf56495a` | Item A: retry budget validation, `DeadlineExceeded`, WARN per failed attempt |
| `621f9398f` | Item C: shutdown drains outside the coop budget, reserved-slot comments |
| `8bb0f691d` | Item D: monotonic bound on a window's life after a backward step |
| `a0c877fe7` | Item E: `RequestTooLarge` decided on the worst case, 2x validation |
| `cb595cba9` | Item F and C3 (partial): dictionary read-through, per-cell and per-table charge, pdata visibility |
| `d87ebd8e1` | Item G: settle a started multipart creation, engine clock in the sink, write polled before the deadline, no retry of permission/not-found |
| `a1959e0d6` | Item G and C21: `writer_id` class, depth cap 256, hive keys reserved, `metrics.series_attributes` refused, config-reject table |
| `b0b79f9a9` | Item G: refusal WARN rate limit, empty ACTIVE block rotation, lazy preallocation, saturating budget, dead arm removed |
| `2a168ae24` | C14: temporality through `AggregationTemporality::try_from` |
| `6bab9010f` | Review tests-to-add list |
| `5ead1b785` | chloggen `series-parquet-review-fixes.yaml` (bug_fix, pipeline, [4128]) |
| `c5c918195`, `296eb2053`, `400c8403a` | README line wrapping for markdownlint |

## Items

### Item A (finding 4): storage outage surfaced as "cancelled"

- **Defect at HEAD.** `flush.rs` sent `last.unwrap_or(Cancelled)` at the
  deadline, so a single attempt still retrying inside object_store (default
  `retry_timeout` 180 s against the 60 s `flush_retry_deadline`) ended as
  `Cancelled { abort_error: None }`, counted in `flush.cancelled`, with no
  underlying error. Failed attempts were never logged. Nothing tied the two
  timeouts together.
- **Tests.** `a_store_retry_budget_must_be_shorter_than_the_flush_deadline`,
  `a_hung_write_expires_the_flush_deadline_as_its_own_outcome`,
  `a_slowly_failing_store_surfaces_its_last_error_at_the_deadline` (tests.rs),
  `effective_retry_timeout_reports_the_default_when_unset` (object_store.rs).
- **Red.** With `flush.rs` reverted: the hung write returned
  `Cancelled { abort_error: None }`; the slowly failing store returned the raw
  `Parquet(External(... injected store failure))` instead of a deadline
  outcome, and the nack reason was the bare token.
- **Fix.** `RetryOptions::effective_retry_timeout` and `DEFAULT_RETRY_TIMEOUT`
  in `otap/src/object_store.rs` (validation hook only). `config::check_retry_deadline`
  refuses a cloud `retry.retry_timeout`, including the 180 s default, that is
  not strictly less than `window.flush_retry_deadline`; the message names both
  values. Local file storage applies no store retry and is exempt, which keeps
  the shipped local config valid. New `lake::Error::DeadlineExceeded
  { attempts, last }`, classified as storage, not counted as cancelled. Every
  retried failure logs `series_parquet.flush_attempt_failed` at WARN with the
  error. The block's nack reason carries the flush error.
- **Commit.** `2bf56495a`.

### Item B (finding 5) and C12: internal errors reported as permanent

- **Defect at HEAD.** `worker.rs` mapped every extraction error through
  `map_err(Failure::Permanent)`, and writer invariants used `Error::invalid`,
  so a builder mismatch became a permanent `INVALID_ARGUMENT` with the reason
  `invalid`.
- **Tests.** `an_internal_extraction_error_is_a_retryable_nack_with_detail`,
  `a_reason_detail_is_bounded_and_single_line`,
  `each_failure_class_maps_to_its_outcome` (extended); existing tests now
  assert the sentences.
- **Red.** The test does not compile at HEAD, because `Failure::classify`,
  `lake::Error::internal` and `Outcome::Internal` do not exist. At HEAD the
  same error is `Error::invalid`, which the inline `map_err` makes permanent.
- **Fix.** `lake::Error::Internal` for the writer invariants: builder
  mismatch, unsupported builder type, row width, sealed block, unsealed
  block, reservation index, emitted_at type, missing sort column, merge
  schema mismatch, metrics descriptor without metric, values without
  descriptors. `Failure::classify` makes only `Refused` permanent. A retryable
  failure is `storage` when storage caused it and `internal` otherwise; a new
  `internal` label was added to the `nacks` metric. Every nack reason is a
  sentence naming the rule or limit and the remedy, with a detail sanitized
  to one line and cut at 256 bytes. The token stays the metric label only.
  The size sentence names the limit by stage: request, extraction or block.
- **Commit.** `8ce4a3fab`.

### Item C (finding 9): shutdown drains dropped most tokens

- **Defect at HEAD.** `deliver_now` and `drain_now` polled each send once with
  `now_or_never`, and tokio's coop budget returns `Pending` after 128
  operations per task poll. `force_shutdown` never used the queue, so the
  reserved slot protected nothing.
- **Tests.** `a_deadline_drain_delivers_more_than_the_coop_budget`,
  `force_drained_refusals_beyond_the_coop_budget_are_all_delivered`,
  `forced_shutdown_refusals_wait_in_the_bound_then_fail` (replaces the old
  bypass test).
- **Red.** Both new tests: 172 failures out of 300, so exactly 128 delivered.
- **Fix.** Every once-polled send is wrapped in
  `tokio::task::coop::unconstrained`. A force-drained refusal starts a send in
  the free slot, keeps a send that would block in that slot, and otherwise
  queues up to `capacity`. Only past `capacity` is it attempted once and
  counted. The three "reserved slot" comments now describe that.
- **Commit.** `621f9398f`.

### Item D (finding 11): backward step stalled rotation

- **Defect at HEAD.** `WindowClock::on_wake` returned `TooEarly` until wall
  time reached `last_boundary + interval`, and the sleep was armed for the
  whole wall distance.
- **Tests.** `a_backward_clock_step_rotates_within_one_monotonic_interval`,
  `wall_clock_drift_is_not_mistaken_for_a_backward_step`.
- **Red.** No rotation in 3600 s of simulated monotonic time. The first red
  run used a wall base of 100 s; the test was rebased to 100000 s to avoid
  negative wall time, with the logic unchanged.
- **Fix.** `Window` tracks the monotonic instant of the last rotation. A wake
  that finds the wall clock short of the boundary rotates once one interval
  of monotonic time has passed, unless the shortfall is within a drift
  tolerance of 1 s plus interval/1000. Every sleep is capped at that
  monotonic deadline. The window start stays floored, and the new
  `Window::floored` flag makes the replacement block re-emit.
- **Commit.** `8bb0f691d`.

### Item E (finding 12): permanence depended on the cache

- **Defect at HEAD.** `reserve_with_reemit` refused `RequestTooLarge` on the
  bytes this block and cache would charge, so a cold cache refused what a
  warm one admitted.
- **Tests.** `a_request_is_decided_alike_cold_and_warm` (buffer.rs),
  `max_block_bytes_below_twice_extracted_is_rejected` (config.rs).
- **Red.** `limit 114999, reemit false: Ok(Reservation { bytes: 113352, ...
  new_series: [] })`: the warm cache admitted while the cold one refused.
- **Fix.** Permanence is decided on the worst case: every descriptor written
  by this block, as with reemit on. A request that fits the worst case but
  not the remaining space parks. Validation requires `max_block_bytes >= 2 *
  max_extracted_bytes`, because `series_row_bytes` charges up to twice the
  extracted estimate. The fixed per-series overhead cannot be bounded
  proportionally; see "Not bounded".
- **Commit.** `a0c877fe7`.

### Item F (finding 7) and C3 (partial): dictionary expansion

- **Defect at HEAD.** `attrs::plain`, `AnyValueColumns` and `struct_child`
  cast dictionary columns to plain arrays, and `AttrTable::from_batch` copied
  every value, before any budget.
- **Tests.** `a_large_dictionary_value_is_refused_before_it_is_expanded`,
  `many_references_to_one_dictionary_value_are_bounded`,
  `a_dictionary_batch_reads_through_without_expansion`.
- **Red.** With HEAD `attrs.rs` and `extract/mod.rs`, both refusal tests
  returned `Ok`: HEAD expanded 512 MiB and 128 MiB tables.
- **Fix.** `readable()` keeps u8/u16-keyed dictionaries of Utf8, Binary,
  FixedSizeBinary and Int64 as they are. `str_cell`, `bytes_cell` and
  `int_cell` read through them with `StringArrayAccessor`,
  `ByteArrayAccessor` and `MaybeDictArrayAccessor::<Int64Array>`, with a
  native fast path. Parent ids are read through
  `MaybeDictArrayAccessor::<UInt16Array>` and `<UInt32Array>`. Each key and
  value is checked against `max_row_bytes` before it is copied. A table's
  decoded string and byte content is bounded by `max_extracted_bytes` through
  the new `DecodeLimits::with_table_bytes`. pdata makes `StringArrayAccessor`,
  `FixedSizeBinaryArrayAccessor`, `AnyValueArrays` and `AttributeArrays`
  public; see Deviations.
- **Prost question.** The OTLP receiver does not decode with prost at all.
  `OtlpBytesDecoder::decode` in `otap/src/otap_grpc/otlp/server_new.rs:303-311`
  and the HTTP path at `otap/src/otlp_http.rs:845` wrap the raw bytes.
  Conversion then runs `pdata/src/payload.rs:632-650`, which calls
  `RawLogsData::new` and then `encode_logs_otap_batch`, and nested AnyValues
  are encoded by `pdata/src/encode/cbor.rs:45` `serialize_any_value`, which
  recurses with no depth limit. The stack-overflow half of the CBOR concern
  therefore stands. It is recorded in the README Limits list and left as the
  upstream pdata follow-up; it is not fixed here.
- **Commit.** `cb595cba9`.

### Item G (minor items)

| Minor item | Test, red | Commit |
| --- | --- | --- |
| Started multipart creation finishes before the abort | `a_cancellation_during_multipart_creation_still_aborts_the_upload`; red at HEAD sink.rs, the upload was not aborted | `d87ebd8e1` |
| Sink abort on the engine clock | `the_abort_is_bounded_on_the_injected_clock`; new API, so it does not compile at HEAD, where `tokio::time::timeout` was used | `d87ebd8e1` |
| Write polled before the deadline; flush.rs:31-33 claim corrected | `a_write_finishing_as_the_deadline_expires_is_a_success`; red, `DeadlineExceeded { attempts: 1, last: None }` | `d87ebd8e1` |
| PermissionDenied, Unauthenticated and NotFound not retried | `a_permission_error_is_not_retried_until_the_deadline`, `retry_classifier_distinguishes_encoding_from_storage`; red, retried to the deadline | `d87ebd8e1` |
| Refusal WARN rate limited, with signal and the limit sentence | `refusal_warnings_are_rate_limited`; new type | `b0b79f9a9` |
| `max_nesting_depth` capped at 256 | `max_nesting_depth_is_capped`; red | `a1959e0d6` |
| Hive keys reserved as denormalized names | `a_denormalized_column_named_like_a_partition_key_is_rejected`; red | `a1959e0d6` |
| `metrics.series_attributes` refused | `metrics_series_attributes_is_rejected`; red | `a1959e0d6` |
| `writer_id` limited to `[A-Za-z0-9_.]` (C21) | `writer_id_outside_its_character_class_is_rejected`; red | `a1959e0d6` |
| Dead rotate/resume arm removed; `debug_assert!(cleaning.is_none())` | covered by the existing suite in debug builds | `b0b79f9a9` |
| Lazy preallocation; saturating budget math | `an_unbounded_capacity_is_not_preallocated` and `a_huge_capacity_is_not_preallocated`; red, capacity overflow panics | `b0b79f9a9` |
| Empty ACTIVE block does not wait for the flush slot | `an_empty_block_rotates_without_waiting_for_the_flush_slot`; red | `b0b79f9a9` |

- **Creation tracking.** The sink tracks multipart creations through a thin
  `CreationWatch` store wrapper. A cancelled step is driven further only
  while a creation is in flight, within the one `abort_timeout` deadline
  shared with the abort, so a step blocked on a wedged part still aborts at
  once.
- **Wedged-abort test.** `a_wedged_multipart_abort_is_bounded_and_leaves_no_object`
  now sets `sorting.merge_chunk_bytes = 4096`. Polling the write before the
  deadline gives it one more poll, which carried that test's single-chunk
  values file into finalization, where no abort is attempted by design. More
  chunks keep it in the writable phase the test is about.

### C14

Temporality is decoded with `AggregationTemporality::try_from`, and unknown
values stay unspecified. This is a pure refactor covered by the existing
metrics tests. Commit `2a168ae24`.

### Tests from the review

- **Factory `create`.** `the_factory_creates_file_storage_without_a_capability`
  runs by default. `the_factory_refuses_azure_storage_without_a_token_provider`
  is gated on the `azure` feature and was run once with
  `--features azure`; it passes. The "with the capability" half is not
  testable from core-nodes, see "Not bounded".
- **Notifier empty after `rotate()`.** `complete_files_before_ack` and
  `a_mixed_metrics_request_drops_only_the_unsupported_points` now assert it.
- **`AckToken::split`.** `split_drops_transport_headers_and_claims`. A
  mutation check that deleted `take_transport_headers` failed it.
- **Config-reject table.** `startup_rejects_invalid_configuration` now asserts
  its base is valid, runs through `SERIES_PARQUET.validate_config` and checks
  an error substring per case. Five cases were added.
- **Exact boundary.** `admission_at_the_exact_boundary_belongs_to_the_next_window`.
- **CBOR depth.** `decode_cbor_depth_is_exact_for_nested_maps`.

## Verification

```text
cargo fmt --all -- --check                                  clean
cargo clippy -p otel-arrow-dfe-pdata -p otel-arrow-dfe-series-lake \
  -p otel-arrow-dfe-otap -p otel-arrow-dfe-core-nodes --all-targets \
  --features otel-arrow-dfe-core-nodes/series_parquet -- -D warnings   clean
cargo test -p otel-arrow-dfe-series-lake -p otel-arrow-dfe-core-nodes \
  --features otel-arrow-dfe-core-nodes/series_parquet
  core-nodes lib 1179 passed; series-lake lib 138 passed; lake integration
  suites (fuzz_canonical 3, fuzz_extract 5, golden 3, golden_roundtrip 2,
  oracle 3) passed; 0 failed
cargo test -p otel-arrow-dfe-core-nodes --features azure --lib -- the_factory_   2 passed
cargo test -p otel-arrow-dfe-otap --lib object_store         14 passed
python3 tools/sanitycheck.py      FAIL only in docs/superpowers/umbrella-review-2026-09-22.md
                                  and concistency-review-2026-09-22.md (pre-existing non-ASCII,
                                  not touched by this task)
npx markdownlint-cli2 (exporter README, FORMAT.md)          0 issues
make chlog-validate               not run: chloggen is not installed here;
                                  note 173 chars, subtext 274 chars
cargo xtask check (workspace, at 400c8403a)     EXIT 0, "All tests passed successfully."
```

The feature is still named `series_parquet` and was not renamed. No golden
hash moved.

## Not bounded, left for the controller

- **Python harness S3 configs are now refused at load (Item A ruling).**
  `test_e2e.py` S3 engines run with no `retry` section at about line 2117
  (`test_minio`, `test_rustfs`), 2249 (restart) and 2293. The outage test at
  2178 sets `retry_timeout: 5s` equal to `flush_retry_deadline: 5s`. Each
  needs `retry.retry_timeout` strictly below its deadline. Those files are
  outside my scope.
- **Harness `writer_id` (C21 ruling).** `performance.py:444` builds a lake
  config with `writer_id: "local-1"`, and the bench validates it at
  `benches/measurement/stages.rs:396`. Stage runs from that config are now
  refused until it becomes `local_1`. The shipped YAML configs were changed
  to `local_1`, and `test_measurement.py` reads the YAML, so it follows.
- **Factory with a bound token provider.** A `Capabilities` holding a
  resolved shared entry can only be built inside the engine crate, because
  `Capabilities::new` and `resolve_bindings` are `pub(crate)`. The positive
  half needs an engine test helper.
- **`AckToken::split` claims.** `capture_authorized_identity` is `pub(crate)`
  in otap, so the test asserts the claims are absent but cannot plant them.
- **Series-row inflation in validation.** The fixed per-series overhead
  (`2 * 64 * columns + 72` bytes) is not proportional to the extracted
  estimate, so no config-time factor bounds it without rejecting the
  defaults. Validation covers the proportional part (2x), and the worst-case
  reservation keeps the outcome consistent either way.
- **Pdata CBOR encoder recursion.** This is the upstream follow-up described
  under Item F.
- **Serialized small blocks.** With `max_requests_per_block: 1` the 2N-1
  credit cap still serializes a worker to one in-flight request, part of
  finding 9's evidence. The brief did not list it and it is unchanged.

## Deviations from the brief

- **Files outside the brief's list.**
  - `core-nodes/.../metrics.rs`: the `internal` nack label.
  - `series-lake/src/cache.rs`: lazy LRU, named by the finding.
  - `series-lake/Cargo.toml`: `async-trait` and `futures` moved from
    dev-dependencies to dependencies for `CreationWatch`; `Cargo.lock` is
    unchanged.
  - `series-lake/docs/FORMAT.md`: the `writer_id` rule line.
  - `configs/series-parquet-{local,s3}.yaml`: `writer_id: local_1`.
  - `series-lake/src/value.rs`: the `DecodeLimits` table bound.
  - `series-lake/src/{extract/mod.rs,extract/logs.rs,sort.rs}`: internal
    errors and dictionary read-through.
- **Pdata widening beyond the four types.** The methods a caller needs on
  the widened aliases became `pub` too: `try_new` and `str_at` on the string
  accessor, `try_new` and `slice_at` on the fixed-size binary accessor.
  Without them the aliases are unusable. series-lake does not use
  `AnyValueArrays` or `AttributeArrays` yet; it reads through the
  accessors.
- **Decode errors from transport-optimized ids.** These now map to
  `Error::invalid`, a permanent content refusal, instead of `Error::Pdata`,
  which would have become retryable under the new rule. They judge the
  request's own id columns.
- **Changelog type.** The entry is `bug_fix` as briefed, but two changes
  can refuse configs that loaded before: cloud storage without a short
  `retry.retry_timeout`, and `writer_id` values containing `-`. The
  controller may prefer `breaking` with a Migration section.

## Fix round 1

Review: "Needs fixes", no Critical. Six commits on top of `400c8403a`.

| Commit | Scope |
| --- | --- |
| `456d29875` | Item D: strict monotonic bound, no drift tolerance |
| `9df21501b` | Item A: WARN for every failed attempt, retryable or not |
| `aa51c8285` | Item E: worst-case formula documented and pinned |
| `8606055fa` | Tests that can fail: S3 load path, S3 factory, real writer invariant, split claims |
| `ea1ea351c` | Item G: size refusals carry the observed size and the limit |
| `03bbc0407` | Harness: S3 retry sections, `writer_id: local_1` |

### Harness

- **S3 engines.** Every S3 engine in `test_e2e.py` now gets an explicit
  `retry` section, `S3_RETRY`, with `retry_timeout: 20s` against the 60 s
  default deadline. That covers the MinIO and RustFS exercise and both
  restart launches.
- **Outage test.** Its budget dropped from 5 s to 2 s, under its 5 s
  deadline.
- **Bench config.** `performance.py` uses `writer_id: local_1`.
- **E2E.** `SERIES_REQUIRE_DOCKER=1 taskset -c 0-7,16-23` with the venv
  interpreter: 18 ran, 18 OK, 0 skipped. The `df_engine` binary was rebuilt
  with `series_parquet,aws` first.
- **Contract tests.** `test_measurement.py`: 174 ran, all OK.

### Item D: strict monotonic bound

- **Defect.** A one-second window opened 600 ms into its interval and
  stepped back 1.5 s stayed open 1.9 s. At the monotonic deadline the wall
  clock was 0.9 s short, inside the old 1 s + interval/1000 tolerance.
- **Test.** `a_step_just_over_one_interval_rotates_within_one_monotonic_interval`.
  With the tolerance still in place it failed. The first draft opened the
  window on a boundary; it passed at HEAD because the shortfall there was
  1.5 s, so the test opens the window mid-interval.
- **Fix.** The tolerance is gone. A window rotates, floored, once one
  interval of monotonic time has passed and the wall clock is short of the
  boundary. The drift test became
  `wall_clock_drift_rotates_at_the_monotonic_bound_then_at_the_boundary`:
  a slow wall clock now costs one extra file set for that window.

### Item A: per-attempt WARN for non-retryable failures

- **Fix.** One helper, `log_failed_attempt`, logs every failed attempt as
  `series_parquet.flush_attempt_failed` with `attempt`, `retryable` and
  `error`. The block-level ERROR stays in addition.
- **Test.** `a_non_retryable_failed_attempt_is_logged_at_warn` failed first:
  nothing was logged. A mutation that logged retryable failures only, as HEAD
  did, fails it.
- **How the test observes the WARN.** A per-test tracing subscriber was
  tried first and was flaky, about one run in five. The capture came back
  empty whenever a concurrently running test registered the flush callsites
  before this thread's subscriber was visible. Test builds therefore record
  what the helper logged in a thread-local, and the test reads that. The
  tracing dev-dependency attempt was reverted.

### Item E: validation completeness

- **Choice: document and pin the formula; do not validate it.** The fixed
  per-series term cannot pass the defaults. The worst case of a request is
  `2 * E + T + S * F`. F is `LakeConfig::series_row_fixed_bytes`, which is
  `2 * 64 * columns + 8 + pending_series_entry_bytes`: 1352 bytes for logs
  and 2120 for metrics, plus 128 per denormalized series column.
- **Why not validate.** S is bounded only by `E / min_estimate`. With a
  minimal descriptor estimate of roughly 60 bytes, the default E of 32 MiB
  allows about 559k series, and 559k x 2120 bytes is about 1.2 GB, far above
  the 500 MiB default block.
- **Where it is documented.** At the validation rule in `config.rs` and in
  the README Delivery section. Such a request is refused consistently, and
  the refusal names `window.max_block_bytes`.
- **Pinning test.** `the_documented_worst_case_block_cost_bounds_every_request`
  checks the bound. It also pins each row's exact charge,
  `2 * (estimate - decoded trees) + F`, because a mutation of the row factor
  from 2 to 3 passed the bound check alone and fails the exact check.

### Tests that could not fail

- **S3 load path.** `an_s3_config_without_a_retry_section_is_refused_at_load`
  runs through `SERIES_PARQUET.validate_config`. Removing the
  `check_retry_deadline` call from `Config::try_from` fails it. core-nodes'
  otap dev-dependency now enables `aws`, so the S3 variant compiles in every
  core-nodes test build; `Cargo.lock` is unchanged.
- **Factory.** The brief's premise was wrong: S3 needs no bearer-token
  capability, and only Azure requires one. The new
  `the_factory_creates_s3_storage_without_a_token_provider` asserts that. The
  Azure negative case stays behind the `azure` feature. A positive capability
  case needs an engine-crate helper, because `Capabilities::new` and
  `resolve_bindings` are `pub(crate)`.
- **Injected internal error.** No request content can make extraction break
  a writer invariant: builders and rows come from the same configuration, and
  OTAP schema validation refuses mistyped columns before extraction runs.
  `a_real_writer_invariant_failure_is_a_retryable_nack_with_detail`
  therefore breaks a real lake invariant at admission. It sets a sort key
  missing from the schema after validation, with a one-byte run target, and
  asserts a non-permanent `internal` nack carrying the lake's own detail.
  This covers the admission call site, not the extraction one. The earlier
  classification test stays as a unit test.
- **Split claims.** The test is renamed `split_drops_transport_headers`. Its
  Scenario comment states that claims cannot be planted from core-nodes,
  because `capture_authorized_identity` is private to otap, and it no longer
  asserts them.

### Item G: refusal WARN fields

- **Fix.** `RefuseReason::RequestTooLarge` now carries an `Excess`: the
  `SizeBudget` (Request, Extracted, Row, Cell, Table, Block), the observed
  size and the limit. Every construction site in the lake fills it in. The
  nack sentence reads "`<what>` of N bytes exceeds `<setting>` (L bytes)". The
  WARN carries `limit_setting`, `observed_bytes` and `limit_bytes`.
- **Simplification.** The worker's `Stage` plumbing is removed.
- **Test.** `a_size_refusal_reports_the_observed_size_and_the_limit` covers
  the extract and block stages. It reads the thread-local record of the WARN
  fields and also checks the sentence. It does not compile at HEAD, which
  had no fields to record.

### Verification, fix round 1

```text
cargo fmt --all -- --check                                  clean
cargo clippy (pdata, otap, series-lake, core-nodes) --all-targets \
  --features otel-arrow-dfe-core-nodes/series_parquet -- -D warnings   clean
cargo test -p otel-arrow-dfe-series-lake -p otel-arrow-dfe-core-nodes \
  --features otel-arrow-dfe-core-nodes/series_parquet
  core-nodes lib 1185 passed; series-lake lib 139 passed; lake integration
  suites 3 + 5 + 3 + 2 + 3 passed; 0 failed
E2E test_e2e.py (SERIES_REQUIRE_DOCKER=1, taskset -c 0-7,16-23)   18 ran, OK
contract test_measurement.py                                174 ran, OK
markdownlint (from the repo root)                           0 issues
cargo xtask check (workspace, at 03bbc0407)                 EXIT 0, "All tests passed successfully."
```

### Concerns after fix round 1

- **Strict bound cost.** A wall clock that runs slower than the monotonic
  clock now makes each window end a moment before its boundary. That writes
  one extra floored file set for the window, with re-emitted descriptors.
- **Worst case not validated.** The S x F term is documented, not validated,
  for the reason given under Item E.
- **Changelog type.** It is still `bug_fix`; `breaking` may fit better,
  because configs that loaded before can now be refused.

## Fix round 2

Re-review: harness, Item D and the four previously ineffective tests are
confirmed fixed. Round 2 is about proof quality; no production defect was
found. Two commits on top of `03bbc0407`.

| Commit | Scope |
| --- | --- |
| `39d9d2fe3` | Items A and G: WARNs asserted as emitted; numeric size fields |
| `912cc9357` | Item E: exact worst-case charge documented and pinned for logs and metrics |

### Items A and G: tracing proof

- **What changed.** The thread-local records in `flush.rs` and `worker.rs`
  are gone. The tests now capture real events.
- **How the capture works.** One process-wide test subscriber,
  `GlobalCapture`, is installed once through a `OnceLock` with
  `set_global_default`. It registers every callsite as `sometimes`, so
  `enabled` is asked per event. It records only on a thread that holds a
  `Capture` guard, which keeps parallel tests out of each other's records.
  This avoids the callsite-cache race of per-test scoped subscribers. Each
  capture also calls `rebuild_interest_cache`. Events keep their field types:
  `U64`, `I64`, `Bool`, `Str`, and `Debug` for `?x` or `%x`.
- **What the tests assert.** Level WARN, the event name, and the field
  values:
  - `a_non_retryable_failed_attempt_is_logged_at_warn`: attempt 1,
    retryable false, the error text.
  - `a_slowly_failing_store_surfaces_its_last_error_at_the_deadline`: two
    WARNs, attempts 1 and 2, retryable true, the error text.
  - `a_size_refusal_reports_the_observed_size_and_the_limit`: at the extract
    stage, outcome, signal, `limit_setting`, and the numbers
    `observed_bytes` > 100 and `limit_bytes` = 100; at the block stage,
    `window.max_block_bytes` with `limit_bytes` = 1.
- **Mutation checks.** Each of these fails the named tests:
  - `limit_bytes = ?limit` (debug formatting): the size-refusal test.
  - Renaming `observed_bytes`: the size-refusal test.
  - Deleting the `otel_warn!` in `log_failed_attempt`: both flush tests.
- **Stability.** The series_parquet filter passed 8 of 8 runs, and the full
  core-nodes lib passed in every run.
- **Dependency.** core-nodes gains a `tracing` dev-dependency with `std`, and
  `Cargo.lock` gains that one line.

### Item G: numeric attributes

`observed_bytes` and `limit_bytes` are recorded as `u64`, and
`limit_setting` as a string. Each is omitted when absent, because
`Option<u64>` records nothing for `None`. They are never debug strings like
`Some(123)`.

### Item E: exact formula

- **Documented charge.** The README and the doc on
  `LakeConfig::series_row_fixed_bytes` now state the exact charge
  `Block::reserve` computes:
  `P + T + sum over series of (2 * (A - D) + 128 * C + 8 + Q)`. P is the
  values bytes, T the token, A the series' extracted estimate, D its decoded
  attribute trees, C the series columns (10 for logs, 16 for metrics, plus
  one per denormalized series column) and Q the 64-byte pending entry. The
  simple `2 * E + T + S * F` is kept and labelled as an upper bound.
- **Test.** `the_documented_worst_case_block_cost_is_what_reserve_charges`
  asserts that `reserve`'s bytes equal the exact charge on an empty block
  with a cold cache, where every descriptor is new. It covers logs with 1
  and 64 series, logs with a denormalized series column, and metrics with 1
  and 64 series. It pins F at 1352 bytes for logs, 2120 for metrics and 1480
  for logs with one denormalized column.
- **Mutation.** Changing the metrics column count from 6 to 7 fails it:
  132876 against 132748.

### Verification, fix round 2

```text
cargo fmt --all -- --check                                  clean
cargo clippy -p otel-arrow-dfe-series-lake -p otel-arrow-dfe-core-nodes \
  --all-targets --features otel-arrow-dfe-core-nodes/series_parquet \
  -- -D warnings                                            clean
cargo test -p otel-arrow-dfe-series-lake -p otel-arrow-dfe-core-nodes \
  --features otel-arrow-dfe-core-nodes/series_parquet
  core-nodes lib 1185 passed; series-lake lib 139 passed; lake integration
  suites 16 passed; 0 failed
markdownlint (from the repo root)                           0 issues
cargo xtask check (workspace, at 912cc9357)                 EXIT 0, "All tests passed successfully."
```

- **Flaky test outside this task.**
  `otlp_grpc_exporter::tests::the_last_bearer_token_is_reused_after_the_provider_closes_its_stream`
  failed once in one combined run. It uses a real gRPC server on a picked
  port. It then passed alone and in four further full runs.
- **Why xtask was run.** Non-test files changed: the `flush.rs` and
  `worker.rs` hook removal, `config.rs` docs, the README, `Cargo.toml` and
  `Cargo.lock`.
- **Not rerun.** E2E and the contract tests were not rerun in round 2. The
  only production change is the refusal WARN's field types, and no Python
  or engine configuration changed.
