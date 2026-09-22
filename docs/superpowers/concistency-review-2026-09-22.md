All ten lookups verified against source. The publish-policy failure, the dormant `retained_work.rs` primitive, and the pdata accessor visibility gap all check out. Writing the report.

# Codebase Consistency Review

## Scope

- **Input**: commit range `5588c3e0d..HEAD` (d6454dbb8) on branch `series-parquet-exporter`, 121 commits.
- **Changed files**: 602 (534 are measurement JSON). Reviewed the concept-bearing set: `crates/series-lake/**`, `crates/core-nodes/src/exporters/series_parquet/**`, `crates/engine/src/{engine_metrics,message}.rs`, `crates/otap/src/pdata.rs`, `crates/validation/tests/series_parquet/*.py`, `.github/workflows/series-parquet-e2e.yml`, Cargo manifests, `.chloggen`, configs, READMEs.
- **Repository search available**: yes (local git, rg, file reads).
- **Subagents**: 10 lookup workers, parallel, one per target cluster. Every finding below was re-verified by reading the cited lines.
- **Detected languages / frameworks**: Rust (tokio, arrow-rs 58.4, parquet, object_store 0.13), Python 3 (unittest, docker CLI, DuckDB, ClickHouse), GitHub Actions.
- **Limitations**: no PR description exists; commit messages and in-code comments were used for stated intent. Nothing was built or run. Correctness was covered by the umbrella review earlier in this session and is not repeated here.

## Executive Summary

The new code follows most local conventions where a shared mechanism already exists: object store construction, `RetryOptions`, `validate_typed_config`, `NackCause::Refused`, pdata's `record_batch_pinned_bytes`, `AttributeValueType`, `MetricType`, the `consts` column names, and the `<component>.` metric-set naming. The problems are concentrated in four places. First, the branch would fail the repo's own publish-policy check, because a published crate now depends on an unpublished path crate. Second, the engine gains a bespoke, globally scoped memory-accounting facility while an unused retained-work primitive and an RFC prescribing the opposite design already sit in the same crate. Third, series-lake becomes the third consumer to work around pdata's crate-private attribute and CBOR accessors, each in a different way. Fourth, the Python harness re-implements what `crates/otap-test-net`, `crates/validation` and `tools/pipeline_perf_test` already provide, and its packaging and CI trigger depart from every existing Python lane.

- Exact reuse opportunities: 4
- Likely duplication: 5
- Abstraction / generalization opportunities: 3
- Naming / API consistency issues: 6
- Settings / config consistency issues: 3
- Schema / data-model consistency issues: 2
- Standard-mechanism consistency issues: 4
- Checked but not flagged: 12

## Findings

### Finding 1: Published `core-nodes` depends on the unpublished `series-lake` crate

- **Severity**: Blocker
- **Category**: Non-standard codebase mechanism
- **Changed code**: `crates/series-lake/Cargo.toml:9` (`publish.workspace = true`, workspace default is `publish = false` at `Cargo.toml:20`); `crates/core-nodes/Cargo.toml:27` (optional path dependency).
- **Existing precedent**: `crates/quiver/Cargo.toml:10-13` (`publish = true`, `keywords`, `categories`); `xtask/src/publish_policy.rs:8-35` (`PUBLISH_PACKAGES` lists quiver, not series-lake); `xtask/src/crates_publish.rs:283-296` bails with "publishable package … depends on unpublished path package" for any non-dev path dependency, optional included; enforced in `rust-ci.yml:465` and `prepare-release.yml:163`.
- **Analysis**: Quiver is the exact precedent: an engine-independent library pulled in by an optional core-nodes feature (`durable-buffer`). It is published and listed. Series-lake copies the dependency shape but not the publish setup. The check runs in required CI, so this is a mechanical failure, not a style point.
- **Recommendation**: Set `publish = true`, add `keywords` and `categories`, and append `otel-arrow-dfe-series-lake` to `PUBLISH_PACKAGES`, mirroring quiver.
- **Confidence**: High (inferred from code; not executed).

### Finding 2: Engine memory accounting parallels the existing retained-work primitive and contradicts the engine's own RFC

- **Severity**: Blocker
- **Category**: Existing abstraction should be extended
- **Changed code**: `crates/engine/src/engine_metrics.rs:73-172` (`SeriesAccounting`, `PROCESS_ACCOUNTING` LazyLock, `SeriesMemoryAccounting::register()` reading the ambient singleton, `SeriesProcessMetrics` named `exporter.series_parquet`); consumer `series_parquet/worker.rs:282, 866`.
- **Existing precedent**: `crates/engine/src/retained_work.rs:1-7, 62-131` (`LocalRetainedAccount` / `LocalRetainedTicket`, an RAII byte-charge handle whose module doc says export and charge sites "are layered on separately"; no users yet); `rfcs/0000-observe-only-retained-work-accounting.md:419-423` ("a component should receive its accounting handle explicitly when it is constructed … if the handle is instead picked up from ambient runtime state, a missing handle silently disables accounting"), `:495-509` (attribution per pipeline and runtime), `:644-651` ("the first level should not try to reconcile" logical bytes with RSS); `docs/memory-resource-management.md:70-83` (names retained-work accounting as the home for this, tracked by #3272); `memory_limiter.rs:100` plus `context.rs:301-305, 411-415` (`MemoryPressureState` threaded explicitly through ControllerContext and PipelineContext, the local pattern for process-shared state).
- **Analysis**: Same concept, different mechanism, in the same crate. The new handle is process-global and ambient; the existing primitive and RFC are runtime-local and explicit. The new metric does the RSS reconciliation the RFC defers. `PROCESS_ACCOUNTING` is the first mutable static in `engine/src` (the others are identity constants, descriptors or thread-locals). The set name `exporter.series_parquet` is the only component name in the engine crate outside tests, and it collides with seven node-side sets of the same name. The node already publishes `memory.accounted_bytes`, so the residual is derivable.
- **Recommendation**: Remove the engine-side facility from this change. If in-process attribution is wanted, build it on `retained_work.rs` with a handle passed through `PipelineContext` like `MemoryPressureState`, and name the metric under an engine namespace. Land that as its own engine PR referencing #3272.
- **Confidence**: High.

### Finding 3: Attribute and CBOR reading is a third divergent workaround for pdata's crate-private accessors

- **Severity**: Strong recommendation
- **Category**: Consider generalizing existing code
- **Changed code**: `crates/series-lake/src/attrs.rs:32-40` (`plain()` casts whole columns), `:43-138` (`AnyValueColumns`), `:150-167` (`AttrTable::from_batch`); `value.rs:70-126` (`decode_cbor`, `convert`); `extract/mod.rs:596-760` (`str_at` and friends).
- **Existing precedent**: `crates/pdata/src/arrays.rs:420-653` (`MaybeDictArrayAccessor`, zero-copy over native or dictionary columns; primitive `try_new` is `pub`, string/binary/fixed-size aliases are `pub(crate)` at `:646-650`); `pdata/src/otlp/common.rs:278-331` (`AnyValueArrays`, the same seven value columns) and `otlp/attributes.rs:43-80` (`AttributeArrays`), both `pub(crate)`; `pdata/src/otlp/attributes/cbor.rs:61` (`proto_encode_cbor_bytes`, `pub`, near-identical conversion rules); `contrib-nodes/.../clickhouse_exporter/arrays.rs:4-9` (already copied the accessors and says "If those accessors are ever stabilized as public pdata API, we can replace this file with re-exports").
- **Analysis**: `AnyValueColumns` is a near duplicate of `AnyValueArrays`. The visibility gap is documented by the ClickHouse exporter, which solved it by copying; series-lake solves it by casting entire columns to plain types, which also has the memory-amplification consequence noted in the umbrella review. The CBOR decoder differs deliberately (depth limit, byte cap, sorted keys, duplicate refusal), so the decode logic is a related design precedent rather than direct reuse. Semantic differences on missing `ser`, missing bool, unknown tags and null parent ids are intentional per series-lake comments, so blind reuse would change behaviour.
- **Recommendation**: Widen pdata's visibility for `StringArrayAccessor`, `FixedSizeBinaryArrayAccessor`, `AnyValueArrays` and `AttributeArrays` (a small pdata PR), then have series-lake read through them instead of `cast`. Replace the ClickHouse copy at the same time. Use `MaybeDictArrayAccessor::<UInt16Array>` and `<Int64Array>` now, since those are already public. Keep the bounded CBOR decoder but consider hosting a "decode `ser` to an owned tree with limits" helper in `pdata/otlp/attributes/cbor.rs` for the three consumers.
- **Confidence**: High on the precedent; medium on reuse cost.

### Finding 4: Cargo feature `series_parquet` is the only underscore feature in the workspace

- **Severity**: Strong recommendation
- **Category**: Inconsistent naming
- **Changed code**: `crates/core-nodes/Cargo.toml:97-104, 135`; top-level `Cargo.toml:333`; repeated in `series-parquet-e2e.yml:36,59,63`, both READMEs, and the changelog subtext.
- **Existing precedent**: every multi-word feature is kebab-case: `host-metrics`, `syslog-cef`, `durable-buffer` (core-nodes `Cargo.toml:112-117`), `user-events`, `azure-monitor`, `resource-validator` (top level). The established pairing is snake_case URN with kebab feature: `receiver:syslog_cef` / `syslog-cef`, `processor:durable_buffer` / `durable-buffer`.
- **Analysis**: The URN `urn:otel:exporter:series_parquet` and scope target are correct. Only the feature diverges. Features are user-facing build flags and appear in docs, so renaming later costs more than now.
- **Recommendation**: Rename the feature to `series-parquet` everywhere; keep the URN.
- **Confidence**: High.

### Finding 5: Module directory and URN constant drop the `_exporter` suffix

- **Severity**: Suggestion
- **Category**: Inconsistent naming
- **Changed code**: `crates/core-nodes/src/exporters/series_parquet/`, `mod.rs:53` (`SERIES_PARQUET_URN`).
- **Existing precedent**: every sibling is `<name>_exporter` (`parquet_exporter`, `otlp_grpc_exporter`, `file_exporter`, `topic_exporter`) with `PARQUET_EXPORTER_URN`, `FILE_EXPORTER_URN`; receivers use `_receiver`. Only processors have unsuffixed outliers.
- **Recommendation**: Rename to `series_parquet_exporter` and `SERIES_PARQUET_EXPORTER_URN` before upstreaming; it is a pure move now and a churny one later.
- **Confidence**: High.

### Finding 6: Metric names mix dotted and flat forms and depart from the semantic-conventions guide

- **Severity**: Strong recommendation
- **Category**: Inconsistent user-facing setting/API
- **Changed code**: `series_parquet/metrics.rs:34-282`. Dotted names (`flush.count`, `block.active_bytes`, `series_cache.hits`) coexist with flat ones (`rows_written` :199, `files_written` :202, `series_emitted` :232, `dropped_unsupported` :260, `oldest_unacked_seconds` :87). `acks`/`nacks` use unit `{request}`; nack label key is `reason`; `DatasetLabel` fuses signal and dataset into one key.
- **Existing precedent**: the macro derives names by replacing `_` with `.` (`telemetry-macros/src/lib.rs:239`), so parquet's `rows_written` publishes as `rows.written` (`parquet_exporter/metrics.rs:24`); rule at `docs/telemetry/semantic-conventions-guide.md:46-47`, no units in names and no `_count` suffix at `:76-79, 247-248`; `{message}` for per-pdata counts in `engine/src/channel_metrics.rs:167`, `otlp_grpc_exporter/metrics.rs:92`, `file_exporter/metrics.rs:22`; failure classes keyed `error.type` in `otap_exporter/metrics.rs:94`, `common_attributes.rs:100-104`; `SignalType` implements `AttributeEnum` (`telemetry/src/common_attributes.rs:17-28`) and siblings use a separate `signal` key (`durable_buffer/metrics.rs:162`).
- **Analysis**: The set name `exporter.series_parquet` matches the dominant code form, so no change there. Within the set, the flat names read as sibling `rows.written` would after macro derivation, so dashboards see two spellings of the same concept across exporters. `oldest_unacked_seconds` and `flush.count` break explicit guide rules; `_bytes` has local tolerance (`process_memory_usage_bytes`).
- **Recommendation**: Rename `rows_written` to `rows.written`, `files_written` to `files.written`, `series_emitted` to `series.emitted`, `dropped_unsupported` to `dropped.unsupported`, `oldest_unacked_seconds` to `oldest_unacked.age` (unit `s`), `flush.count` to `flushes`. Use `{message}` for acks/nacks. Key the nack classification `error.type`. Split `DatasetLabel` into `signal: SignalType` plus a two-value `dataset` enum.
- **Confidence**: High on precedent; medium on the exact replacement names.

### Finding 7: Log event names use a synonym for an established event and a flat verb form

- **Severity**: Suggestion
- **Category**: Inconsistent naming
- **Changed code**: `series_parquet/mod.rs:258` (`series_parquet.shutdown_deadline_elapsed`), `mod.rs:153` (`series_parquet.retry_ignored_for_file_storage`), `worker.rs`/`flush.rs` (`flush_failed`, `seal_failed`); no `series_parquet.start` event.
- **Existing precedent**: `durable_buffer.shutdown.deadline_exceeded` (`durable_buffer_processor/mod.rs:1663`), `clickhouse.exporter.shutdown.deadline_exceeded`, `kafka.exporter.shutdown.deadline_exceeded`; `durable_buffer.flush.failed` (`:1178`); `parquet.exporter.retry_ignored_for_file_storage` (`parquet_exporter/mod.rs:192`); `otap_exporter.start`, `topic_exporter.start`, `otlp.exporter.grpc.start`; guide bans synonym drift at `docs/telemetry/events-guide.md:211`.
- **Recommendation**: Use `series_parquet.shutdown.deadline_exceeded`, `series_parquet.flush.failed`, `series_parquet.seal.failed`; add a `series_parquet.start` event carrying writer_id, boot_id and storage. Move the file-storage retry warning next to `from_storage_type_with_retry_and_token_provider` so both exporters emit one event name (see Finding 9).
- **Confidence**: High.

### Finding 8: Exporter does not use the shared exporter metric sets or the engine's per-node input accounting

- **Severity**: Suggestion
- **Category**: Related precedent exists, but no direct reuse recommended
- **Changed code**: `series_parquet/metrics.rs:74` (`acks`), `:154-159` (`nacks{reason}`).
- **Existing precedent**: `engine/src/channel_metrics.rs:161-169` (`node.input.messages{signal,outcome}` registered for every node with input when `NODE_INPUT_METRICS` is on, `runtime_pipeline.rs:161-176`); `otap/src/metrics.rs:393-455` (`ExporterMetrics`), `:539-552` (`ExporterExportMetrics`, used by parquet at `parquet_exporter/mod.rs:61,80`, file, console, otap, otlp, topic).
- **Analysis**: The series nack reason is richer than the engine's `Outcome`, so the exporter's own counters are defensible. But the total ack/nack counts duplicate `node.input.messages`, and every other exporter also registers one of the shared sets, which dashboards rely on for cross-exporter views.
- **Recommendation**: Register `ExporterExportMetrics` (or `ExporterMetrics`) alongside the local sets so the exporter appears in the shared views; keep `nacks{error.type}` for the reason breakdown.
- **Confidence**: Medium.

### Finding 9: Object-store wiring is copied verbatim from `parquet_exporter`

- **Severity**: Suggestion
- **Category**: Duplicate or near-duplicate implementation
- **Changed code**: `series_parquet/mod.rs:92-100` (bearer-token requirement), `:151-156` (file-storage retry warning), `:157-168` (store construction and error mapping); `config.rs:240-242` (third `retry.validate()`).
- **Existing precedent**: `parquet_exporter/mod.rs:99-107` (byte-identical token block), `:185-195` (same warning), `:196-209` (same helper call and identical error string); `RetryOptions` already validates on deserialize (`otap/src/object_store.rs:29, 72-86`) and again in the helper (`:319-321`).
- **Recommendation**: Move the three blocks into `crates/otap/src/object_store.rs` (or a small `exporters/object_store_support.rs`) as one function taking `&StorageType, Option<&RetryOptions>, &Capabilities` and returning the store, and call it from both exporters. Drop the redundant `retry.validate()`.
- **Confidence**: High.

### Finding 10: Byte-unit parsing rewrites JSON by key suffix instead of annotating fields, and duplicates a wrapper

- **Severity**: Strong recommendation
- **Category**: Non-standard codebase mechanism
- **Changed code**: `series_parquet/config.rs:49-53` (`byte_size`), `:82-97` (sections held as `serde_json::Value`), `:123-135` (`normalized()` rewriting every `*_bytes` key), `:181-186` (hand-copied allow-list of `IngressLimits` fields); `series-lake/src/config.rs:206-308` (plain `usize` byte fields).
- **Existing precedent**: every other byte-valued setting annotates the typed field directly with `otel_arrow_dfe_config::byte_units::deserialize`/`deserialize_u64` (`otap_grpc/server_settings.rs:106-143`, `client_settings.rs:96-103`, `otlp_http.rs:181`, `policy.rs:937-951`, `journald_receiver/config.rs:229-238`); `journald_receiver/config.rs:486-491` already has the same non-optional wrapper (`deserialize_byte_size`) with a slightly different error string.
- **Analysis**: The rewrite exists because series-lake avoids depending on the config crate (which pulls reqwest, miette, schemars). That is a legitimate layering concern, but `byte-unit` itself is a workspace dependency, so series-lake can annotate its own fields with a thin `deserialize_with` and the exporter can drop the JSON layer and the allow-list. Two identical non-optional wrappers should become one in `byte_units`.
- **Recommendation**: Add `byte_units::deserialize_required_u64` (or usize) to the config crate and use it in journald; give series-lake a `deserialize_with` on its byte fields backed by `byte-unit`; delete `normalized()`, the `Value` sections and the allow-list.
- **Confidence**: High.

### Finding 11: Config validation error style and key names diverge from siblings

- **Severity**: Suggestion
- **Category**: Inconsistent user-facing setting/API
- **Changed code**: `series_parquet/config.rs:166-171, 217-231` (grouped messages "all byte, depth and abort budgets must be positive"); `series-lake/src/config.rs:416-420` (errors name `window_interval`, `max_requests_per_block`, which users set under `window.`); `:73, 76` ("must be >= 1" and "must be at least 1" in one function).
- **Existing precedent**: one field per message with the user path: "zip.interval must be greater than 0" (`log_sampling/zip.rs:50`), "{path_prefix}.balanced.queue_capacity must be greater than 0" (`config/topic.rs:249`), "file.max_frame_bytes must be in the range 1..=N" (`file_exporter/config.rs:216-218`); `otap_exporter/config.rs:104-115` `deserialize_positive_usize(field_name)`.
- **Recommendation**: One check per field, naming the user-facing dotted key; wrap `decode()` errors with the section prefix.
- **Confidence**: High.

### Finding 12: `max_request_bytes` and the nack reason tokens use spellings no sibling uses

- **Severity**: Suggestion
- **Category**: Inconsistent naming
- **Changed code**: `series-lake/src/config.rs` (`ingress.max_request_bytes`); `token.rs:113-127` (`reason()` returns `"invalid"`, `"too_large"`, `"unsupported"`, `"storage"`).
- **Existing precedent**: receivers: `max_request_body_size` (`otap/src/otlp_http.rs:110`), `max_decoding_message_size` (`otap_grpc/server_settings.rs:145`); exporters: `max_frame_bytes` (`file_exporter/config.rs:112`). Every other node's nack reason is a human sentence, and machine classification lives in `NackCause` (`file_exporter/mod.rs:170-209`, `otlp_grpc_exporter/mod.rs:955`, `content_router/mod.rs:727`).
- **Analysis**: `NackCause::Refused` usage is consistent with `partition_processor:173` and `transform_processor:525`, so the mechanism is right. The reason string is the OTLP status message the client sees; siblings put the actionable sentence there.
- **Recommendation**: Keep `max_request_bytes` only if documented as the exporter-side twin of `max_frame_bytes`; otherwise name it `max_frame_bytes` like the file exporter. Make `reason` a sentence ("request exceeds ingress.max_request_bytes (16MiB); split the batch upstream") and keep the token only as the metric label.
- **Confidence**: Medium.

### Finding 13: OTLP framing check duplicates `file_exporter` and belongs in the shared conversion

- **Severity**: Suggestion
- **Category**: Consider generalizing existing code
- **Changed code**: `worker.rs:393-413` (`check_wire_format` via `RawLogsData::try_new` / `RawMetricsData::try_new`).
- **Existing precedent**: `file_exporter/mod.rs:374-386` (same calls, same permanent nack); `pdata/src/payload.rs:627-650` (`TryFromWithOptions<OtlpProtoBytes> for OtapArrowRecords` uses the unchecked `new` constructors, so `try_into_with_default` never reports framing errors; `parquet_exporter:335` and `otap_exporter:745` inherit that).
- **Recommendation**: Add a validating option to the pdata conversion (or a `validate_framing()` on `OtlpProtoBytes`) and call it from all four exporters; file a follow-up for parquet and otap exporters.
- **Confidence**: High on precedent; the premise that unchecked conversion silently yields empty records was not independently verified.

### Finding 14: Temporality is matched with integer literals although the proto enum exists

- **Severity**: Suggestion
- **Category**: Existing function/class/helper should be reused
- **Changed code**: `series-lake/src/extract/metrics.rs:112-116` (`Some(1) => Delta, Some(2) => Cumulative`).
- **Existing precedent**: `AggregationTemporality` in `pdata/src/proto/opentelemetry.proto.metrics.v1.rs:262-266`; the tests in the same file already use `AggregationTemporality::Delta as i32` (`metrics.rs:1025, 1048`). `MetricType` and `AttributeValueType` are already reused correctly at `:99-107` and `attrs.rs:13-17`.
- **Recommendation**: `AggregationTemporality::try_from(v)` then map to the local `Temporality`.
- **Confidence**: High.

### Finding 15: Benches require CLI args and env vars, breaking the workspace `cargo bench` lane

- **Severity**: Strong recommendation
- **Category**: Non-standard codebase mechanism
- **Changed code**: `crates/series-lake/benches/measurement.rs:130-160` ("--stage is required"), `benches/layered.rs:87-91, 113` (`SERIES_STAGE_CONFIG must name a file`); `Cargo.toml` `[[bench]] harness = false`.
- **Existing precedent**: `.github/workflows/rust-bench.yml:33` runs plain `cargo bench` workspace-wide on the `cargobench` label; no other bench needs arguments or env; pdata gates optional benches with `required-features = ["testing"]` (`pdata/Cargo.toml:62-70`); `benchmarks/README.md` says per-crate benches belong under `benchmarks/benches/<crate>/`.
- **Recommendation**: Gate both benches behind `required-features = ["bench-harness"]` (or exit 0 with a skip message when inputs are absent), and decide whether they belong in the upstream PR at all.
- **Confidence**: High (not executed).

### Finding 16: Parquet sort metadata uses a private key and bypasses the native sorting-columns field

- **Severity**: Suggestion
- **Category**: Inconsistent schema/data model
- **Changed code**: `series-lake/src/sink.rs:162` (file key `sort_key`, format `col:asc:nulls_last,...` from `sort.rs:57-79`); `sink.rs:232-244` (WriterProperties without `set_sorting_columns`).
- **Existing precedent**: `pdata/src/schema/consts.rs:76-78` defines `metadata::SORT_COLUMNS = "sort_columns"` (dormant, no writers); parquet `WriterProperties::set_sorting_columns` is unused anywhere, yet DuckDB, DataFusion and ClickHouse read that standard row-group field.
- **Recommendation**: Emit Parquet's native `SortingColumn` list in addition to the custom key, and either reuse the `sort_columns` name or note in FORMAT.md why it differs.
- **Confidence**: Medium.

### Finding 17: `schema_fingerprint` parallels pdata's `SchemaIdBuilder`

- **Severity**: Suggestion
- **Category**: Borrow existing design ideas
- **Changed code**: `series-lake/src/schema.rs:230-238` (hashes arrow `DataType` Display text, order-sensitive).
- **Existing precedent**: `pdata/src/otap/schema.rs:13-60` (`SchemaIdBuilder`: canonical schema id string with its own type codes, sorted by field name, independent of arrow's Display).
- **Analysis**: Direct reuse is wrong (different scope and ordering semantics), but the design point matters: pdata deliberately owns its type vocabulary so arrow upgrades cannot change ids. The umbrella review flagged the Display dependency as a correctness risk; this is the in-repo precedent for the fix.
- **Recommendation**: Model the fingerprint renderer on `SchemaIdBuilder`'s owned type codes.
- **Confidence**: Medium.

### Finding 18: The Python harness re-implements existing test infrastructure and departs from every Python lane's conventions

- **Severity**: Strong recommendation
- **Category**: Duplicate or near-duplicate implementation
- **Changed code**: `test_e2e.py:42` (`free_port`, called back-to-back at `:336-337` with no dedupe), `:396` (readiness by gRPC channel only), `:406-427` (hand-parsed Prometheus text), `:1679` (`DockerStore` over the `docker` CLI), `measurement.py:2162` and `performance.py:823-872` (/proc sampling), `requirements.txt` (open ranges, no lock), location under `crates/validation/tests/`, runner `python3 -m unittest` via implicit namespace packages.
- **Existing precedent**: `crates/otap-test-net/src/lib.rs:28` `try_pick_unused_loopback_tcp_port`, used with explicit dedupe at `crates/validation/src/scenario.rs:218-250`; `crates/validation/src/simulate.rs:68-91` polls `/api/v1/readyz`; `crates/validation/src/container.rs` (testcontainers with wait strategies); `tools/pipeline_perf_test/orchestrator/lib/impl/strategies/monitoring/prometheus.py:233` (`prometheus_client.parser`), `deployment/process.py:107-158` (`ProcessDeployment`), `deployment/docker.py:153` (`DockerDeployment` with `cpuset_cpus`), `monitoring/process_component.py:216-240` (psutil sampling); every existing Python project lives under `tools/`, ships `requirements.txt` with `==` pins plus `requirements.lock.txt`, installs with `--require-hashes` (`pipeline-perf-on-label.yaml:100-101`), and uses pytest/tox; `rust-validation-tests.yml:16-27` and `pipeline-perf-on-label.yaml:24` keep Docker lanes opt-in by dispatch or label; `docs/testing-guide.md:35` describes `crates/validation` as the Rust scenario framework only.
- **Analysis**: The closest shape match is the orchestrator (subprocess engine plus Docker plus sampling). The new harness overlaps it on eight responsibilities and adds two with no precedent (flock host lease, build monitor). Keeping `test_e2e.py` as the functional suite is reasonable; the measurement lane is a second perf framework.
- **Recommendation**: Keep `test_e2e.py`, but use `prometheus_client.parser`, poll `readyz`, dedupe ports, and move it under `tools/` (or document the new Python lane in `crates/validation/README.md`). Pin dependencies with a lock file and hashes. Trigger the workflow by label or dispatch like its siblings, with SHA-pinned actions and `harden-runner`. Express the measurement lane as orchestrator suites and plugins, or keep it out of the upstream PR.
- **Confidence**: High.

### Finding 19: `WallClock` mirrors the engine's `admission::MonotonicClock` pattern rather than extending it

- **Severity**: Observation
- **Category**: Related precedent exists, but no direct reuse recommended
- **Changed code**: `series-lake/src/clock.rs:62-100` (`WallClock`, `SystemWallClock`, `TestWallClock(Arc<AtomicI64>)`).
- **Existing precedent**: `engine/src/admission/clock.rs:35-120` (`MonotonicClock`, `SystemClock`, `ManualClock(AtomicU64)`, same shape, crate-private, monotonic only); `engine/src/clock.rs` has no wall time; 88 direct `SystemTime::now()` sites elsewhere.
- **Analysis**: series-lake cannot depend on the engine, and no injectable wall clock exists, so this is the first one. Worth knowing when the engine eventually grows one.
- **Confidence**: Medium.

### Finding 20: `ExporterInbox::shutdown_deadline()` has no counterpart on `ProcessorInbox`

- **Severity**: Observation
- **Category**: Inconsistent naming
- **Changed code**: `engine/src/message.rs:875-878`.
- **Existing precedent**: all sibling exporters read the deadline only from `NodeControlMsg::Shutdown { deadline }` (`parquet_exporter/mod.rs:276-293`, `otap_exporter:698`, `file_exporter:135`); `ProcessorInbox` (`message.rs:760-816`) exposes only `new` and `recv_when`.
- **Analysis**: The getter is needed because the latched deadline is only visible during the forced drain (`message.rs:314-315`). It is a small, justified API, but it is asymmetric.
- **Recommendation**: Add the same accessor to `ProcessorInbox` or document why exporters alone need it; add an `enhancement` chloggen entry for the engine API.
- **Confidence**: High.

### Finding 21: `writer_id` validation is looser than the closest precedent

- **Severity**: Suggestion
- **Category**: Inconsistent user-facing setting/API
- **Changed code**: `series-lake/src/config.rs:385-390` (non-empty, no `/`), default `"writer"` at `series_parquet/config.rs:106`.
- **Existing precedent**: `journald_receiver/config.rs:44, 277-278, 500-515` (`source_id`: user string with default and an ASCII `[A-Za-z0-9_.-]` validator); `file_exporter/config.rs:46` derives uniqueness from `core_id()` and deployment generation.
- **Recommendation**: Adopt journald's character class (excluding `-`, which is the file-name separator) and consider defaulting from `pipeline.core_id()` rather than the constant.
- **Confidence**: Medium.

### Finding 22: `render_v1` spells non-finite doubles and bytes differently from the workspace's OTLP JSON

- **Severity**: Observation
- **Category**: Related precedent exists, but no direct reuse recommended
- **Changed code**: `series-lake/src/value.rs:155-167` (`"inf"`, `"-inf"`, hex bytes).
- **Existing precedent**: `pdata/src/otlp/json/common.rs:40-47` (`"Infinity"`, `"-Infinity"`, base64 at `:258`); `azure_monitor_exporter/transformer.rs:591-622` (hex bytes, null for non-finite).
- **Analysis**: render_v1 is a pinned storage canonical form, so divergence is deliberate. FORMAT.md should say it is not OTLP/JSON, so readers do not assume the OTLP spelling.
- **Confidence**: Medium.

## Checked But Not Flagged

- **URN, type string, scope target**: `urn:otel:exporter:series_parquet` and `otel.exporter.series_parquet` follow the dominant snake_case URN pattern.
- **`storage` / `retry` config keys and `humantime_serde` durations**: identical types and spelling to `parquet_exporter`.
- **`validate_typed_config` plus `TryFrom<Raw>`**: established pattern (kafka receiver, geneva exporter, `RetryOptions`).
- **Byte accounting**: series-lake already calls pdata's `record_batch_pinned_bytes`; no reimplementation. Quiver and the batch processor use logical sizes, a pre-existing split.
- **`AttributeValueType`, `MetricType`, `consts` column names, `decode_transport_optimized_ids`**: reused correctly.
- **`NackCause::Refused` and `NodeShutdown` usage**: consistent with processors and `content_router`.
- **k-way merge, `interleave`, aligned wall-clock windows, Parquet file-level key/value metadata, multipart abort on cancel**: no precedent anywhere in the workspace; nothing to align with.
- **File naming vs `parquet_exporter::generate_filename`**: deliberately deterministic per block for retry idempotence; the random-UUID precedent does not fit.
- **xxh3 series identity vs `temporal_reaggregation_processor/identity.rs:282-358`**: same hash family, different byte order and tags; series-lake's is a pinned cross-language contract with a Python-generated vector, so divergence is justified. Worth a one-line note in FORMAT.md.
- **`lru` dependency**: no crate-based cache exists elsewhere (hand-rolled bounds only); adding one is not inconsistent.
- **`component: pipeline` in chloggen**: matches prior new-node entries.
- **Terminal metrics reporting in `engine_metrics.rs`**: reuses `report_snapshot_reliably_until` and `flush_until` correctly; only the accounting facility (Finding 2) is at issue.

## Search Appendix

Ten parallel Opus lookup workers, one per cluster: config schema and store wiring; telemetry naming; engine memory accounting; ack/nack and framing; shutdown deadline and timers; series-lake infrastructure helpers; attribute and CBOR reading; sort, merge and Arrow utilities; Python harness; naming and packaging. High-yield anchors: `byte_units::`, `validate_config:`, `#[metric_set(name`, the telemetry-macros default-name derivation, `docs/telemetry/*.md`, `retained_work.rs`, `rfcs/0000-observe-only-retained-work-accounting.md`, `MemoryPressureState`, `NackCause::`, `RawLogsData::try_new`, `nack_status.rs`, `Shutdown { deadline`, `recv_when`, `tick_timers.cancel_all`, `MaybeDictArrayAccessor`, `AnyValueArrays`, `proto_encode_cbor_bytes`, `record_batch_pinned_bytes`, `SORT_COLUMNS`, `SchemaIdBuilder`, `try_pick_unused_loopback_tcp_port`, `prometheus_client`, `cpuset_cpus`, `PUBLISH_PACKAGES`, `crates_publish.rs`. Files inspected include every sibling exporter's `mod.rs`/`config.rs`/`metrics.rs`, `crates/otap/src/object_store.rs`, `crates/engine/src/{engine_metrics,message,clock,admission/clock,memory_limiter,context,channel_metrics,runtime_pipeline,pipeline_ctrl,retained_work}.rs`, `crates/pdata/src/{arrays,payload,otap/memory,otap/schema,otap/transform,otlp/common,otlp/attributes,otlp/attributes/cbor,otlp/json/common}.rs`, `crates/validation/src/*`, `tools/pipeline_perf_test/orchestrator/lib/**`, `xtask/src/*`, `.github/workflows/*.yml`. Noise: `_bytes` (matches `as_bytes`), `evict`, `BinaryHeap` (scheduler heaps), `sort_by` (Vec sorts). Uncertainties: publish-check and bench failures were inferred from code, not executed; whether receivers already decode OTLP bodies (affects Finding 13's value); upstream issue #3272 and PR #3756 were not read, so retained-work export wiring may already be in flight; the telemetry registry's behaviour when one set name spans two entity types was not checked.