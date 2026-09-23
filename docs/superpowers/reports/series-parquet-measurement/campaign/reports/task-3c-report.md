# Task 3c report: throughput path, CI and documentation hygiene

Branch `series-parquet-exporter`, base 912cc9357, 15 commits, nothing pushed.

## Commits

| Commit | Items |
| --- | --- |
| 5659c7ed7 | Cargo.lock windows-sys revert |
| b07c3a8c3 | C1 publish policy |
| a6a54cd8a | C4 feature rename + opt-in (L), C5 module/constant rename |
| 1172a3d8a | C6 metric renames + harness, I admission gauges, C8 shared export set, C7 events, introspection minors |
| 5f0135e73 | C9 shared object-store wiring |
| c6eaccac2 | C10 typed byte-unit sections, C11 one error per field |
| 9a8e7f32a | C13 `OtlpProtoBytes::validate_framing` in four exporters |
| 084e0f7cc | C20 `ProcessorInbox::shutdown_deadline` + engine/otap changelog entry |
| b4eb69bf4 | H metrics memo on content, shared resource/scope lists |
| 19db70f9c | J/C15 benches behind `bench-harness`, skip without inputs |
| ed140b8c1 | K workflow |
| 6235f77ae | L docs/config/changelog hygiene, I sizing docs and configs, fsync answer |
| b45695e11 | Harness minors, published-JSON scrubbing |
| 433a7d398 | E2E log reader follows the `flush.attempt` event rename |
| f4f55f9f9 | S3 example credential variables default to empty (bundled-config parse test) |

## Per item

### Cargo.lock windows-sys (5659c7ed7)
- At HEAD: nine unrelated packages were re-resolved to windows-sys 0.61.2.
- Fix: restored the base (5588c3e0d) windows-sys dependency of the nine packages whose diff was only that line (curl-sys, dirs-sys, errno, quinn-udp, rustix, rustls-platform-verifier, tempfile, terminal_size, winapi-util); core-nodes and parquet kept their real changes. `cargo metadata --locked` accepts it. (Corrected in fix round 1.)
- Lockfile delta over the whole Task 3c range (912cc9357..HEAD): nine windows-sys lines (the revert) plus one new edge, series-lake -> byte-unit (c6eaccac2). There is no tracing delta; tracing was already resolved.

### C1 publish policy (b07c3a8c3)
- At HEAD: `cargo xtask crates-publish check` failed: "publishable package otel-arrow-dfe-core-nodes depends on unpublished path package otel-arrow-dfe-series-lake".
- Fix: series-lake `publish = true`, `keywords`, `categories = ["database-implementations"]`; `otel-arrow-dfe-series-lake` in `PUBLISH_PACKAGES` (alphabetical position). Check passes; structure check passes.

### C4 + L opt-in, C5 rename (a6a54cd8a)
- At HEAD: `series_parquet` was in `core-exporters`; feature was the only underscore feature; module `series_parquet/`, constant `SERIES_PARQUET_URN`.
- Fix: removed from `core-exporters`; feature `series-parquet` in both manifests, workflow, configs, READMEs, changelog, harness build strings and test fixtures; `git mv` to `series_parquet_exporter/`, `SERIES_PARQUET_EXPORTER_URN`; URN unchanged. component-inventory's dev-dependency enables `series-parquet` so the compiled oracle keeps linking the component.
- components-baseline.json: not feature-derived (see verification); unchanged.

### C6, I, C8, C7, introspection minors (1172a3d8a)
- At HEAD: flat names `rows_written`, `files_written`, `series_emitted`, `dropped_unsupported`, `oldest_unacked_seconds`, `flush.count`; acks/nacks `{request}`; label `reason`; fused `DatasetLabel`; no admission metric; no shared export set; flat event names; no start event; `too_large` conflated four budgets; nesting depth was `invalid`; retries lost on abandon.
- Fix:
  - Names `rows.written`, `files.written`, `series.emitted`, `dropped.unsupported`, `oldest_unacked.age` (s), `flushes`; `{message}`; `error.type`; `signal` + `dataset` (`series`/`values`).
  - `error.type` values: `storage`, `request_too_large`, `extracted_too_large`, `row_too_large`, `block_too_large`, `too_deep`, `invalid`, `unsupported`, `shutdown`, `internal`. New lake refusal `RefuseReason::TooDeep(limit)`, raised by the conversion and by ciborium's own recursion limit.
  - Item I gauge: `admission.closed` (1 while not taking pdata; shutdown is not counted as backpressure), `admission.closures`, `admission.closed.duration`.
  - C8: `ExporterExportMetrics` (`exporter.exports`) registered; the notifier records every decision by signal and success/refused/failure with receipt-to-decision duration.
  - C7: `shutdown.deadline_exceeded`, `flush.failed`, `seal.failed`, plus `request.failed`, `block.committed`, `flush.attempt`, `flush.attempt_failed`, `flush.task_failed`, `flush.cleanup_failed`, `notify.failed`, `inbox.failed`; new `series_parquet.start` (writer_id, boot_id, storage, num_cores, memory_budget_bytes).
  - `block.committed` carries `window_start`, `seq`, `path`, `attempts`; `shutdown.complete` carries accepted, acked, nacked, abandoned, deadline_exceeded, duration; `flush.retries` counts attempts of an abandoned flush (shared `Rc<Cell>` with the task).
  - Start-up WARNs: `memory_budget.oversubscribed` (budget x `num_cores` > MemTotal from /proc/meminfo) and `shutdown.grace_exceeded` (interval + 2 x (flush deadline + abort) > 60s).
  - Harness: only `oldest_unacked_seconds` was read (measurement.py, test_e2e.py); both updated in the same commit. README metrics table and new Events table updated; FORMAT.md metric references updated.
- Tests: `series_metric_schema_is_exact` (every name, unit, label), `each_failure_class_maps_to_its_outcome` (all budgets, TooDeep), `decode_cbor_reports_excess_depth_as_too_deep`, `admission_closure_is_visible_in_metrics`, `decisions_are_recorded_in_the_shared_export_metrics`, `start_up_announces_the_worker_and_warns_on_budgets_that_cannot_hold`, `a_committed_block_names_its_window_sequence_and_path`, retries in `an_abandoned_flush_is_counted_as_cancelled`, summary in `shutdown_decides_every_force_drained_request`.

### C9 (5f0135e73)
- At HEAD: token requirement, file-storage retry warning and store construction were byte-identical in both exporters; series_parquet ran a third `retry.validate()`.
- Fix: `otap::object_store::required_token_provider` and `exporter_store`, used by parquet and series_parquet; one warning event `object_store.retry_ignored_for_file_storage` (the parquet exporter's event name changes; noted in the engine changelog entry). Redundant validate removed. Test `exporter_store_wiring_is_shared`.

### C10 + C11 (c6eaccac2)
- At HEAD: `normalized()` rewrote `*_bytes` keys in `serde_json::Value` sections; a hand-kept ingress allow-list; grouped error messages; lake errors naming lake keys.
- Fix: `byte_units::deserialize_required_u64`/`_usize` in the config crate (journald uses the former, its copy removed); series-lake annotates its ten byte fields with a local byte-unit `deserialize_with` (no config-crate dependency); the exporter reads typed sections, with its own four-field `Ingress` and `Parquet` (compression) structs; every section error is prefixed with its key via a `section!` wrapper; one message per rule with the dotted user key, including the two cross-field rules pre-checked with `window.`/`sorting.` keys; lake messages use dotted lake keys and one wording.
- Tests: `byte_valued_settings_accept_units`, `required_byte_sizes_parse_and_refuse_null`, `startup_rejects_invalid_configuration` extended (29 cases, each asserting the dotted key).

### C13 (9a8e7f32a) -- upstream-relevant, belongs in the engine/otap prep PR
- At HEAD: parquet and otap converted a truncated OTLP body through `try_into_with_default` without error.
- Fix: `OtlpProtoBytes::validate_framing()` (pdata; `validate_message_wire_format` widened to `pub(crate)`). series_parquet and file call it (file then uses the unchecked view constructors); parquet drops the request as a failed export with WARN `parquet.exporter.malformed_otlp_body`; otap nacks permanently and continues.
- Tests: `validate_framing_refuses_a_damaged_body` (pdata); `a_malformed_otlp_body_is_not_written` (parquet) and `a_malformed_otlp_body_is_nacked_permanently` (otap), each shown to fail with the check disabled.

### C20 (084e0f7cc)
- `ProcessorInbox::shutdown_deadline()`; test `processor_deadline_is_visible_during_drain`; enhancement entry `engine-inbox-deadline-and-otlp-framing.yaml` (note 169, subtext 294 chars).

### H (b4eb69bf4)
- At HEAD: memo keyed on the point's own OTAP id, so it never hit; every point copied resource/scope/attrs, encoded and hashed, and charged a row before `seen` dropped it.
- Fix: memo key is metric id + the point's attribute list by content (borrowed from the point table; equality decides a hit, so no hash collision can merge series); identity computed and checked against `seen` before a `DescriptorRow` is built or charged; resource and scope lists are `Arc<[..]>` in `Descriptor`, decoded, charged and held once per request (`SharedLists`), logs too. `DescriptorRow.decoded_bytes` keeps the block charge per request unchanged; `Extracted.shared_bytes` reports the shared lists (the worker's pending gauge includes them). New `ExtractStats.series_memo_hits/misses`.
- Test: `many_points_under_a_wide_resource_hit_the_memo_and_fit_the_default_budget`: 8192 points, 4096 series under a 25-attribute resource, default limits: accepted, 4096 hits, 4096 misses, one shared resource copy. The same request on pre-H code (probe in a worktree at 084e0f7cc) was refused: `RequestTooLarge(Extracted, observed 33556580, limit 33554432)`.
- No golden vector moved: golden, golden_roundtrip, oracle and fuzz tests pass unchanged.

### J + C15 (19db70f9c)
- At HEAD: plain `cargo bench` failed on both harness benches.
- Fix: `required-features = ["bench-harness"]` on both, `bench-heap` implies it; with no inputs both print a skip line and exit 0 (`measurement` also for a bare `--bench`); partial arguments still fail. Self-test `a_bare_cargo_bench_run_skips`. Harness build commands and README updated. Verified: `cargo bench -p otel-arrow-dfe-series-lake` exits 0; with `--features bench-harness` both run and skip.

### K (ed140b8c1)
- Paths narrowed to series-lake, the exporter module, the harness directory and the workflow (pull_request and push to main, same trigger set as before); concurrency `${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}`, cancel-in-progress; SHA-pinned checkout, dtolnay toolchain, rust-cache (restore only, rust-ci's settings) and upload-artifact; rust-ci's disk-free step. The pull_request job builds the debug engine, runs the 18-test E2E, the contracts and the bench self-test (`cargo test --features bench-harness --bench measurement -- --self-test`, test profile). The release build and launcher-ci smoke are a separate `workflow_dispatch`-only job. Python dependencies install from the hashed lock.

### L (6235f77ae, f4f55f9f9)
- configs/README.md: metrics are stored.
- "does not exist yet" removed from the series-lake README and changelog; exporter README no longer says benchmarks are absent.
- Changelog: one entry per PR-shaped change (engine prep enhancement, series-lake new_component, exporter new_component); `series-parquet-review-fixes.yaml` folded into the exporter entry; one placeholder comment style, issue 4128.
- Spec line 1356 `.superpowers/` link removed, the two references are plain text; no `](...superpowers` link remains in docs.
- `gen_golden.py`: SPDX header, writes a trailing newline; golden JSON gained only the newline; regeneration is byte-identical; golden tests pass.
- S3 example: credentials are `"${env:SERIES_S3_ACCESS_KEY_ID:-}"` / `"${env:SERIES_S3_SECRET_ACCESS_KEY:-}"`.
- Item I docs: README sizing section gives `in_flight = rate * (interval / 2 + flush) / records_per_request`; S3 example sized for 100k records/s in 512-record requests (`max_concurrent_requests: 2048`, about 2930 requests per window); Alloy config sized for 10000 records/s per producer at the 20000-record batch (`num_consumers = 8`); arithmetic in both files and the README.

### Harness minors and published JSON (b45695e11, 433a7d398)
- `run_child` re-raises KeyboardInterrupt/SystemExit.
- DockerStore: every docker call has a 120 s timeout; removal by name after a failed start; retry on a taken port.
- `free_port` never issues a port twice and re-probes.
- `test_disconnect_does_not_remove_data` sends 0.2 s into an aligned window.
- Readiness polls `/api/v1/readyz` before the gRPC wait.
- The Prometheus text parser that read absence as zero is removed; `exporter_gauges` uses the JSON snapshot through `metric_values` and returns None for absence.
- `publish=false`: monitor tick gaps are recorded in the coverage check's detail as an observation; visibility and Docker conditions still fail it.
- `requirements.txt` pins direct dependencies; `requirements.lock.txt` from `pip-compile --generate-hashes`, verified by a fresh `pip install --require-hashes` and an import check.
- Published JSON: `write_result` and both baseline-candidate writes go through `scrub_published` (repo root to `<repo>`, home to `<home>`, credential keys and `NAME=value` credential arguments to `<redacted>`), after fingerprints are computed.
- Tests: `HarnessHygieneContracts` (8 tests).

## Verification

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | clean |
| clippy `-D warnings`, all targets, core-nodes/series-lake/pdata/otap/engine/config/otel-arrow-dfe with series-parquet, journald, bench-heap | clean |
| `cargo xtask check` | passed (after f4f55f9f9; the first run failed `bundled_configs_parse_as_engine_configs` on the unset credential variables, fixed there) |
| core-nodes lib tests, `--features series-parquet` | 1191 passed |
| series-lake tests | 158 passed |
| `cargo xtask crates-publish check`, `cargo metadata --locked` | pass |
| Contract tests (`test_measurement`, venv) | 182 passed |
| E2E, 18 tests, `SERIES_REQUIRE_DOCKER=1`, `taskset -c 0-7,16-23` | 18 passed (at 433a7d398; the first run at b45695e11 failed `test_retry_keeps_frozen_object_names` on the old event name, fixed in 433a7d398) |
| Bench self-test | passed |
| launcher-ci smoke, release engine, `publish=false legacy_tests=false`, under the taskset | passed, 2/2 children, 14 s; its JSON carries no `/home/` path and no test secret |
| markdownlint on changed Markdown | clean |
| `tools/sanitycheck.py` | fails only on the two committed review documents (see concerns) |

## Extract spot measurement (item H)

Stage `extract`, fixture `metrics-mixed` (150 requests, 15000 records), release bench build, timing profile, 30 minimum iterations, `taskset -c 0-7,16-23`, median CPU ns per record:

| Build | ns/record |
| --- | --- |
| Before (084e0f7cc) | 1357.5 |
| Before, rerun after the change was built | 1316.9 |
| After (b4eb69bf4) | 919.7 |

One run each, not a family measurement; Task 4 consumes the new number.

## Needs-verification answers

- **LocalFileSystem fsync:** object_store 0.13.2 `src/local.rs` has no `sync_all`, `sync_data` or fsync; it writes a staging file and renames it. The README now says that for `storage: file` an ack survives a process crash only.
- **components-baseline.json:** built by the syn source scanner, which sees every component regardless of `cfg`/features; not feature-derived, unchanged. The compiled oracle only checks linked components, and its dev-dependency now enables `series-parquet`.
- **pull_request lane duration:** run locally on this host with a warm target directory, so not a cold hosted-runner figure. First run: 53 s incremental debug build, 207 s E2E, 5 s contracts, 5 s self-test, 270 s total. Second run: 1 s build, 128 s E2E, 4 s contracts, 5 s self-test, 138 s total. Disk use of a cold build was not measurable here (the shared target directory is 419 GB).

## Deviations

- C18 "one telemetry parser": the absence-as-zero Prometheus parser is gone, and `prometheus_client` is not used because no Prometheus text is parsed any more. Two JSON readers remain: `metric_values`/`metric_max` for the E2E and `parse_telemetry` for per-worker measurement samples. Both refuse to read absence as zero.
- Start-up warnings are emitted per worker, not once per process. The grace warning fires with default settings, since 15 s + 2 x (60 s + 5 s) = 145 s.
- The start event's `storage` is taken from the variant name in the `Debug` output. The cloud variants' `cfg` belongs to the otap crate and cannot be named here; the fields after the name are never read.
- The shared export set is recorded only when metrics are installed through `Worker::set_metrics`, which is the production path. Tests that assign `worker.metrics` directly do not record it.
- The parquet exporter has no ack/nack path, so it drops a malformed body with a WARN and a failure metric instead of nacking it.
- The S3 example uses empty env defaults, not a hard startup failure, because bundled configs must parse with no environment set.
- The measurement harness's recorded build features string is now `series-parquet`. New build fingerprints differ from earlier committed results for that reason alone.
- An extra consistency rename, not requested: every exporter event uses the dotted form, not only the three the brief names.

## Not bounded / concerns

- The two committed review documents under `docs/superpowers/` fail `tools/sanitycheck.py` (non-ASCII, missing trailing newline). They came in with fc698ff71 and I left them untouched.
- A request of 8192 *distinct* series under the same 25-attribute resource is still refused at the default 32 MiB. Each series row renders its own copy of the resource map and its identity bytes, which comes to about 4 KB per series. Only the shared decoded tree was removed.
- The workflow is untested on GitHub. I ran its commands locally only, and have no hosted-runner timing, cold-cache disk figure or launcher-ci flake rate.
- The Alloy `num_consumers` change (4 to 8) is exercised by the E2E. The 100k/s receiver sizing in the S3 example is arithmetic, not a measurement.

## Fix round 1

Review verdict "Needs fixes", four Important findings and one report correction.

| Commit | Finding |
| --- | --- |
| 008627819 | 1. Memo `Hash`/`Eq` for doubles |
| 659ad646e | 2. Published-JSON scrub |
| 7809f9c96 | 3. Unit words in metric names |
| 41fc82f2d | 4. Dotted keys in lake validation |

1. **Memo keys (008627819).** `MemoKey` took equality from `Value::PartialEq` but hashed doubles by `to_bits`: `0.0 == -0.0` hashed apart, and a NaN key did not equal itself. `canonical_double_bits` is extracted from the canonical encoder (NaN to the quiet NaN, `-0.0` to `0.0`), and the memo's `kv_eq`/`value_eq` and `hash_value` both use it, recursively. Tests: `memo_keys_follow_the_canonical_double_rules` (signed zero, two NaN patterns, nested NaN: equal, reflexive, same hash, and the canonical bytes agree) and `signed_zero_and_nan_attributes_hit_the_memo` (six points with 0.0, -0.0, two NaNs and 1.5 twice: 3 descriptors, 3 misses, 3 hits).
2. **Scrub (659ad646e).** Every absolute path under /home, /Users, /root, /srv, /tmp, /var, /opt, /mnt, /data, /run and /media is reduced to `<host-path>/<final component>`, after the `<repo>`/`<home>` substitutions.
   - Credential keys matching secret, password, passwd, token, credential, api/access/private/secret key, key id or root user are redacted.
   - `NAME=value` assignments with PASSWORD, SECRET, TOKEN, ACCESS_KEY, SECRET_KEY, KEY_ID or ROOT_USER in the name are redacted. That covers MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
   - Every credential value found either way is removed wherever else it appears, for example a log tail.
   - Deviation: a bare key named `key` is not treated as a credential. The harness publishes worker and baseline identifiers under `key`, so `(?i)key` would redact data. The key regex matches the credential compounds listed above instead.
   - The contract test now asserts that all of these are scrubbed, and that URLs, object URIs and identifiers are kept.
   - A launcher-ci smoke writes no `/home/`, `/tmp/` or `/var/` path into its JSON.
3. **Unit words (7809f9c96).** Renamed `block.active`, `block.flushing`, `block.pending`, `notify.token_size`, `memory.budget` and `memory.accounted`; the unit stays By. The harness (measurement.py, test_e2e.py, test_measurement.py, README) and the docs change in the same commit.
   - `assert_schema` now fails any exporter metric whose dot- or underscore-separated segment is a unit word (bytes, seconds, ms, ns, us, count, total, ...).
   - `a_unit_in_a_metric_name_is_rejected` proves that the old forms fail.
   - Not changed: the engine's process-scoped `memory.unaccounted_rss_bytes` (engine_metrics.rs, finding 8, deferred to plan 4) and the `memory_budget_bytes` log-event field.
4. **Dotted keys (41fc82f2d).** `LakeConfig::validate` names each entry by position: `logs.denormalize[i].path`, `logs.denormalize[i].column` (separators, partition key, collision), and `metrics.values_sort[i].column` (unknown column, unsortable type). A collision names the later of the colliding entries. The denormalize path is now checked before the schema is built. `Error::invalid_detail` returns the rule sentence without the refusal wrapper. The lake tests and every case of the exporter's startup table assert the full dotted key.
5. **Report.** Corrected the lockfile paragraph above.

### Verification

| Check | Result |
| --- | --- |
| clippy `-D warnings`, core-nodes and series-lake, all targets, series-parquet and bench-heap | clean |
| core-nodes lib tests with series-parquet | 1192 passed |
| series-lake tests | 160 passed |
| Contract tests (venv) | 182 passed |
| E2E, 18 tests, `SERIES_REQUIRE_DOCKER=1`, `taskset -c 0-7,16-23`, at 41fc82f2d | 18 passed, 146 s |
| Bench self-test | passed |
| launcher-ci smoke, release, publish=false | passed, 2/2 |
| `cargo xtask check` | passed |

## Fix round 2

The fix is one commit, 0bb39d807, covering all three scrub gaps.

1. **Committed evidence.** I rescrubbed the committed launcher-ci tree in place instead of re-running and republishing it. The tool is `measure rescrub --index` (`measurement.rescrub_tree`).
   - It verifies the tree's hashes first. It then rewrites every file through `scrub_published`, children before the documents that reference them, and recomputes each reference's `sha256` and `size_bytes` from the rewritten child. It verifies the tree again at the end.
   - Three files changed and lost every `/tmp` path: `launcher-ci.json` (including the legacy E2E log path), `launcher-ci-strict-local-c1-w1-r001.json` and `launcher-ci-buffered-local-c1-w1-r001.json`. The two baseline files were already clean and are byte-identical.
   - Afterwards `enumerate_tree` verifies all five files, both runs and the index pass `validate_result`, and a second rescrub changes nothing.
   - `publish_result_tree` now refuses any file of a tree that the scrub would still change, archived older indexes included. That makes the scrub hold for every published file at publish time.
   - Not done: 424 other committed evidence files under `docs/superpowers/reports/` still contain `/home/` or `/tmp/` paths. They belong to other trees and to deferred finding 1 (history rewrite). Rescrubbing all of them would add another large JSON diff to the branch history, so that is left for the user's decision on finding 1. `measure rescrub --index <tree index>` handles any of them.
2. **URL forms.** `HOST_PATH` now takes a host root right after `scheme://`, so `file:///tmp/x` becomes `file://<host-path>/x`. A remote authority's path, such as `http://host/tmp/x`, is kept.
   - A root must end at `/`, at the end of the string or at a delimiter, and a path is never taken right after a placeholder's `>`.
   - That fixed a non-idempotent scrub: `/tmp/tmpXYZ` and `/tmp/run/data` turned into doubled placeholders on a second pass. The new publish-time check exposed it through seven existing contract tests.
3. **Short credentials.** A discovered credential value shorter than four characters is now removed wherever it stands as a whole token, bounded by non-word characters. `MINIO_ROOT_USER=u` now scrubs `u` from log lines while `ubuntu` and `up` stay intact.

Tests:
- `test_url_paths_and_short_credentials_are_scrubbed` covers `file://` paths, a remote URL, `/tmpXYZ`, a `/tmpfs...` lookalike, idempotency, and short user and password values in a log line.
- `test_a_published_tree_is_rescrubbed_hash_consistently` covers the publish refusal, the rescrub, the recomputed hash and size, and the successful publish afterwards.

Verification:

| Check | Result |
| --- | --- |
| Contract tests | 184 passed |
| launcher-ci smoke, release engine, publish=false, `taskset -c 0-7,16-23` | passed, 2/2 |
| grep of the smoke's JSON for `/tmp/`, `/home/`, `/var/`, `file://`, `series-test-access`, `series-test-secret-12345` | 0 matches in each of the three files |
| `tools/sanitycheck.py` | clean |

No Rust changed in this round.
