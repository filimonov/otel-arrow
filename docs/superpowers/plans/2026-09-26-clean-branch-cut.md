# Clean upstream branch: cut plan (2026-09-26)

Plan only; nothing here has been executed. Source: `series-parquet-exporter` at
152e89376. Fork point 5588c3e0d. Target base: `origin/main` at 5db83589a (23
upstream commits since the fork, including the v0.57.0 release). Paths below
are relative to `rust/otap-dataflow/` unless they start with `.github/` or `/`.

Campaign diff (`git diff --stat 5588c3e0d..HEAD`): 2736 files, of which 2551 are
under `docs/superpowers/` and are dropped whole. The 185 remaining files are
split into seven commits (one of them optional), plus a list of dropped files.

## How to cut

```
git switch -c series-parquet-upstream origin/main
# whole files:   git checkout series-parquet-exporter -- <paths>
# partial files: git diff 5588c3e0d series-parquet-exporter -- <file> > /tmp/p
#                then keep only the listed hunks and run git apply --3way /tmp/p
# Cargo.lock:    regenerate per commit (cargo update -w), never take ours
```

Each commit is the net campaign diff of its files, not a replay of the history
(556 campaign commits). Rename every `.chloggen` entry to a neutral name and
set `issues:` to the PR number when the PR is opened. Drop the PLACEHOLDER
comment lines.

## Commits

"W" = whole file taken from the campaign branch; "P" = partial (only the listed
hunks); "E" = must be edited at the cut. Line counts are +/- from the campaign
numstat.

### C1. engine: completions after Shutdown, closed-pdata Shutdown deadline, durable_buffer shutdown acks

Depends on: none.

| File | Kind | Notes |
|---|---|---|
| crates/engine/README.md | W | +18/-1, shutdown_completions contract |
| crates/engine/src/exporter.rs | W | +5/-2, `follow_pipeline_deadline` |
| crates/engine/src/lib.rs | W | re-export `ShutdownCompletionRequirements` |
| crates/engine/src/local/processor.rs | W | `awaits_completions()` |
| crates/engine/src/shared/processor.rs | W | same, shared |
| crates/engine/src/shared/exporter.rs | W | `follow_pipeline_deadline` |
| crates/engine/src/message.rs | W | +432/-42; includes `ExporterInbox::shutdown_deadline` (the inbox tests use it; series_parquet reads it in C7) |
| crates/engine/src/processor.rs | W | +723, completion phase |
| crates/engine/src/pipeline_ctrl.rs | W | +232/-1, shutdown latch |
| crates/engine/src/output_router.rs | W | `close()` |
| crates/engine/src/runtime_pipeline.rs | W | latch wiring |
| crates/engine/src/runtime_services.rs | W | `PipelineShutdownDeadline` |
| crates/quiver/Cargo.toml | W | `test-hooks` feature |
| crates/quiver/src/lib.rs | W | `test_hooks` module |
| crates/quiver/src/test_hooks.rs | W | new, 71 |
| crates/quiver/src/wal/writer.rs | W | drop hook under `test-hooks` |
| crates/core-nodes/src/processors/durable_buffer_processor/mod.rs | W | +825/-28 |
| crates/core-nodes/src/processors/durable_buffer_processor/README.md | W | |
| crates/core-nodes/src/processors/durable_buffer_processor/telemetry.md | W | |
| crates/core-nodes/Cargo.toml | P | dev-dep `otel-arrow-dfe-quiver = { features = ["test-hooks"] }` only |

Changelog (two components):
- `engine`, bug_fix: from `engine-closed-pdata-keeps-shutdown-deadline.yaml`; add one sentence for `shutdown_completions` / `awaits_completions()` and the read-only `ExporterInbox::shutdown_deadline()`.
- `pipeline`, bug_fix: from `series-parquet-t10-f1-buffer-shutdown-acks.yaml`, renamed `durable-buffer-shutdown-acks.yaml`; add the bounded finalize and the off-thread storage release (d1973f5b8, 3d3c5602d, 14409117a).

### C2. pdata: OTLP framing check, adopted by the file, otap and parquet exporters

Depends on: none. Must precede C3 (both touch `parquet_exporter/mod.rs`).

| File | Kind | Notes |
|---|---|---|
| crates/pdata/src/views/otlp/bytes/validate.rs | W | new, 1953 (proptest against prost) |
| crates/pdata/src/views/otlp/bytes.rs | W | `mod validate` |
| crates/pdata/src/views/otlp/bytes/decode.rs | W | +167/-70, `field_range` |
| crates/pdata/src/views/otlp/bytes/common.rs | W | +267/-33, byte-view fixes (0ec7e938b, 1b7d3aaf6, 354fa2956) |
| crates/pdata/src/views/otlp/bytes/{logs,metrics,traces}.rs | W | step over accepted fields |
| crates/pdata/src/otlp/batching.rs | W | `field_range` rename |
| crates/pdata/src/proto/consts.rs | W | entity-ref field numbers |
| crates/pdata/src/error.rs | W | `InvalidOtlpWireFormat`, `OtlpNestingTooDeep`, `DuplicateOtlpField` |
| crates/pdata/src/payload.rs | P | `validate_otlp_framing` + import only; the `invalid_utf8_is_replaced_and_counted` test goes to C7 |
| crates/pdata/Cargo.toml | P | dev-dep `proptest` only (the `otlp_framing` bench is dropped unless Q4 says keep) |
| Cargo.toml (workspace) | P | `proptest = "1.5"` |
| crates/core-nodes/src/exporters/otlp_framing.rs | W | new, 51 |
| crates/core-nodes/src/exporters/log_gate.rs | W | new, 70 |
| crates/core-nodes/src/exporters/mod.rs | P/E | `mod otlp_framing` and `mod log_gate`; remove `feature = "series-parquet"` from the `log_gate` cfg (the feature does not exist yet, and `unexpected_cfgs` fails clippy) |
| crates/core-nodes/src/exporters/file_exporter/mod.rs | W | +136/-23 |
| crates/core-nodes/src/exporters/otap_exporter/mod.rs | W | +276/-1 |
| crates/core-nodes/src/exporters/parquet_exporter/mod.rs | P | framing hunks only: the refusal at @@ -332 and its tests in @@ -987 (the store-wiring hunks go to C3) |

Changelog: `otlp-framing-validation.yaml`, component `pipeline`, bug_fix; drop
every mention of series_parquet ("The file, parquet and otap exporters
refuse ..."). Name the pdata API `OtapPayload::validate_otlp_framing` in the subtext.

### C3. otap object_store: shared S3/Azure store wiring, `unsigned_payload`, Azure `endpoint`

Depends on: C2 (textual only, `parquet_exporter/mod.rs`).

| File | Kind | Notes |
|---|---|---|
| crates/otap/src/object_store.rs | W | +458/-31: `exporter_store`, `required_token_provider`, `StorageType::kind`, `UnsignedPayloadDefault`, `unsigned_payload`, `AWS_UNSIGNED_PAYLOAD` |
| crates/otap/src/object_store/azure.rs | W | `endpoint` |
| crates/core-nodes/src/exporters/parquet_exporter/mod.rs | P | store-wiring hunks @@ -41, -96, -182 |
| crates/core-nodes/src/exporters/parquet_exporter/config.rs | W | test: parquet keeps `unsigned_payload` as written |
| crates/core-nodes/src/exporters/parquet_exporter/README.md | W | Azure `endpoint` |

`UnsignedPayloadDefault::OverTls` has no production caller until C7; it is
covered by the object_store tests. Say so in the PR text.

Changelog: one `otap` enhancement merging `object-store-s3-unsigned-payload.yaml`
(rewritten: "S3 storage gains `unsigned_payload`; exporters may default it on
over TLS; the parquet exporter keeps signed payloads"),
`object-store-azure-endpoint.yaml`, and the event rename
`object_store.retry_ignored_for_file_storage` (currently in
`engine-inbox-deadline-and-otlp-framing.yaml`).

### C4. OTLP receiver: retryable refusal statuses and `max_decoding_message_size`

Depends on: none.

| File | Kind | Notes |
|---|---|---|
| crates/otap/src/concurrency_shed_layer.rs | W | new, 103 |
| crates/otap/src/lib.rs | W | `pub mod concurrency_shed_layer` |
| crates/otap/src/memory_pressure_layer.rs | W | UNAVAILABLE |
| crates/otap/src/rate_limit_layer.rs | W | UNAVAILABLE / INVALID_ARGUMENT |
| crates/otap/src/otap_grpc.rs | W | test expectation |
| crates/otap/src/otap_grpc/common.rs | W | `load_shed(false)` in tonic |
| crates/otap/src/otap_grpc/server_settings.rs | W | `DEFAULT_MAX_DECODING_MESSAGE_SIZE` |
| crates/otap/src/otap_grpc/otlp/server_new.rs | W | |
| crates/otap/src/otlp_http.rs | W | Retry-After: 1, timeout while waiting for a permit |
| crates/core-nodes/src/receivers/otlp_receiver/mod.rs | W | +353/-3 |
| crates/core-nodes/src/receivers/otlp_receiver/README.md | W | |
| crates/core-nodes/src/receivers/otap_receiver/mod.rs | W | test: UNAVAILABLE, `check_then_shutdown` |
| crates/engine/src/testing/receiver.rs | W | `check_then_shutdown` |
| docs/otlp-receiver.md | W | |
| docs/memory-limiter-phase1.md | W | |

Changelog: `otlp-receiver-retryable-refusals.yaml`, `pipeline`, bug_fix (as is,
minus the placeholder). It is a wire-visible status change, so ask whether
upstream wants `breaking` (Q9).

### C5. config: required byte-size deserializers

Depends on: none.

| File | Kind |
|---|---|
| crates/config/src/byte_units.rs | W (+56/-1, `deserialize_required_u64/_usize` with a test) |
| crates/core-nodes/src/receivers/journald_receiver/config.rs | W (uses the shared helper; removes its private copy) |

Changelog: none (an internal refactor) or one `engine` enhancement line if
upstream asks. No other small backlog fix exists on the branch: the pdata CBOR
encoder recursion limit named in the plan was never implemented.

### C5b (optional, Q2). engine: jemalloc background purging thread on glibc Linux

| File | Kind |
|---|---|
| src/main.rs | W (+64: `malloc_conf` symbol, startup line, test) |
| crates/engine/src/memory_limiter.rs | W (`jemalloc_background_thread()`) |

Changelog: `jemalloc-background-thread.yaml`, `engine`, enhancement. The
measurements it cites are in the dropped evidence tree; the PR text must
restate them.

### C6. series-lake crate

Depends on: none (it uses only pdata APIs that already exist on origin/main: `MaybeDictArrayAccessor<PrimitiveArray<_>>::try_new`, `NullableArrayAccessor`, `otap::memory`, `schema::consts`, `testing::round_trip`).

| File | Kind | Notes |
|---|---|---|
| crates/series-lake/src/** (22 files) | W | attrs, buffer, cache, canonical, clock, config, error, extract/{mod,logs,metrics}, hook_store, lib, schema, sink/{mod,naming,properties,tests,write}, sort, value |
| crates/series-lake/tests/** | W | cbor_allocation, fuzz_canonical, fuzz_extract, golden, golden_roundtrip, oracle, oracle.proptest-regressions, golden/*.json |
| crates/series-lake/docs/FORMAT.md | W | |
| crates/series-lake/README.md | W/E | remove any bench-harness mention |
| crates/series-lake/tools/gen_golden.py | W | Q5; FORMAT.md cites it as the independent oracle |
| crates/series-lake/Cargo.toml | E | drop features `bench-harness`/`bench-heap`, both `[[bench]]`, and dev-deps `criterion`, `cpu-time`, `sha2`, `prost`, `otel-arrow-dfe-otap`, the `object_store` aws dev variant and the jemalloc target dev-deps (no test uses them); keep `dhat` (cbor_allocation), `proptest`, `tempfile`, `tokio`, pdata `testing` |
| crates/series-lake/metadata.yaml | new | upstream now keeps codeowner metadata per crate (#4103); Q8 |
| Cargo.toml (workspace) | P | `otel-arrow-dfe-series-lake = { version = "0.57.0", ... }` (not 0.56.0), `ciborium-io`, `ciborium-ll`, `lru` |
| xtask/src/publish_policy.rs | W | publish the crate |

Changelog: `series-lake-core.yaml`, `pipeline` (or `pdata`, Q10),
new_component. Drop `series-lake-format-revision-2.yaml`: the format has never
been released, so the first PR is revision 2 and FORMAT.md says so.

### C7. series_parquet exporter

Depends on: C1 (inbox deadline accessor; the buffered config relies on the
durable_buffer fix), C2 (`validate_otlp_framing`, `log_gate`), C3
(`exporter_store`, `UnsignedPayloadDefault::OverTls`, Azure `endpoint`), C4
(`DEFAULT_MAX_DECODING_MESSAGE_SIZE`), C5 (`deserialize_required_usize`), C6.

| File | Kind | Notes |
|---|---|---|
| crates/core-nodes/src/exporters/series_parquet_exporter/{mod,config,flush,metrics,outcome,token,window,worker}.rs | W/E | worker.rs: remove `SeriesMemoryAccounting` (import line 28, field line 228, `register()` line 310, `set()` line 1032), since engine_metrics.rs is dropped |
| .../series_parquet_exporter/tests/{mod,admission,config,flush,model,rotation,shutdown,support}.rs | W | |
| .../series_parquet_exporter/tests/metrics.rs | E | delete `worker_publishes_accounted_bytes_to_process_accounting` |
| .../series_parquet_exporter/README.md | E | line 395 links the dropped harness README: point to the E2E README or remove it; check for measured numbers that cite the evidence tree |
| .../series_parquet_exporter/metadata.yaml | new | Q8 |
| crates/core-nodes/src/exporters/mod.rs | P | `pub mod series_parquet_exporter`; add `feature = "series-parquet"` to the `log_gate` cfg |
| crates/core-nodes/Cargo.toml | P | optional dep `otel-arrow-dfe-series-lake`; feature `series-parquet`; dev-deps `tracing`, `tracing-subscriber`, `proptest`, otap `aws`. Do not add `serde_yaml` (upstream already added it) |
| crates/core-nodes/README.md | P | exporter table row |
| Cargo.toml (workspace) | P | `series-parquet = ["otel-arrow-dfe-core-nodes/series-parquet"]` |
| crates/component-inventory/Cargo.toml | W | dev feature `series-parquet` |
| components-baseline.json | P | `urn:otel:exporter:series_parquet` entry |
| crates/otap/src/pdata.rs | P/E | `Context::take_authorized_identity`, `Context::retained_frame_bytes` and their tests, **re-implemented** on upstream's packed Context (#4089; see conflicts) |
| crates/pdata/src/encode/mod.rs, encode/record/array.rs | W | `count_utf8_repairs` (sole caller: worker.rs) |
| crates/pdata/src/payload.rs | P | test `invalid_utf8_is_replaced_and_counted` |
| configs/series-parquet-{local,s3,buffered}.yaml, series-parquet.alloy, series-parquet-strict.alloy | W | Q7 for the .alloy files |
| configs/README.md | W | +75 |
| crates/validation/tests/series_parquet/ (new minimal E2E) | new | Q3: `README.md` (short), `test_e2e_minio.py` carved from test_e2e.py (`LocalLauncher`, `Engine`, `DockerStore`, one `DockerSlice`-style case: logs and metrics through OTLP gRPC to MinIO, read back with DuckDB), `requirements.txt` + regenerated `requirements.lock.txt` (boto3, duckdb, grpcio, opentelemetry-proto, PyYAML, xxhash if gen_golden stays) |
| .github/workflows/series-parquet-e2e.yml | E | reduce to: build df_engine `--features series-parquet,aws,durable-buffer`; pull MinIO only; run the minimal E2E; regenerate goldens and diff them (if Q5 keeps gen_golden.py). Remove RustFS, ClickHouse, Alloy, Azurite, the measurement, soak and fault steps, and the `launcher-smoke` job unless it only needs MinIO. Trim `paths:` (drop engine_metrics.rs, contrib-extensions, capability/auth, main.rs). Must pass upstream's zizmor lint (#4107): pinned actions, `persist-credentials: false`, least `permissions` |

Changelog: one `pipeline` new_component from `series-parquet-exporter.yaml`,
folding in what a first reader needs from the pre-release entries (at-least-once
contract, `wait_for_result`, `ingress.max_series_per_request`,
`metrics.exemplars`, token allowance, multipart abort, buffered reference
config, receiver message limit). Drop the other ten `series-parquet-*.yaml`
entries: they describe changes to an unreleased component.

## Conflicts with origin/main

Files changed on both sides since 5588c3e0d (`comm` of the two name lists):

| File | Upstream change | Our commit | Expected resolution |
|---|---|---|---|
| Cargo.toml | v0.57.0 bump, deps (#4125, #4120, #4037, #4110) | C2, C6, C7 | textual, adjacent lines; series-lake at 0.57.0 |
| Cargo.lock | many | all | regenerate |
| components-baseline.json | log_parser entry (#4037) | C7 | textual, trivial |
| crates/core-nodes/Cargo.toml | csv-core, regex-automata, serde_yaml dev-dep, dhat, benches (#4037, #4011) | C1, C7 | textual; drop our serde_yaml line |
| crates/core-nodes/src/receivers/otlp_receiver/mod.rs | 1/-1 (#4089) | C4 | textual, small |
| crates/otap/src/otlp_http.rs | 1/-4 (#4089) | C4 | textual, small |
| crates/otap/src/pdata.rs | +438/-48 packed Context (#4089) | C7 | **port**: `authorized_identity` is no longer an `Option`, identity entries are packed in an `Arc`; rewrite both methods and re-measure the 4KiB token allowance tests |
| crates/pdata/Cargo.toml | `sanitize` bench (#4037) | C2 | textual, trivial |
| crates/pdata/src/arrays.rs | StringArray accessor made pub (#4037) | none | our widening is dropped (unused), so no conflict |

Upstream changes that touch none of our files but change assumptions:
- #4103 codeowner `metadata.yaml` per node and crate: C6 and C7 add one each.
- #4108 context policy and authorized-identity retention: check whether policy already keeps claims out of retained contexts, which would shrink `take_authorized_identity` in C7.
- #4111, #4117, #4134, #4143 OTAP view decoding and parent_id column sizing: series-lake reads OTAP columns directly; `attrs.rs` handles u16 and u32 parent ids, but C6 must run its oracle and fuzz tests against origin/main batches.
- C1 and C3 have no overlap with upstream files.

## Dropped

| Path | Reason |
|---|---|
| docs/superpowers/** (2551 files) | process documents, plans, reports, evidence |
| /.gitignore (`.measurement-artifacts/`) | measurement harness output directory |
| crates/series-lake/benches/** (7 files) | measurement benches |
| crates/validation/tests/series_parquet/ alloy_capacity.py, capacity.py, faults.py, fault-nginx.conf, fault-tools.Dockerfile, generator.py, host_monitor.py, measure.py, measurement.py, memory.py, performance.py, reference_deployment.py, soak.py, test_failures.py, test_measurement.py, test_reference_deployment.py, test_soak.py | measurement, soak and fault harness |
| crates/validation/tests/series_parquet/test_e2e.py, README.md, requirements*.txt | replaced by the minimal E2E in C7 |
| crates/engine/src/engine_metrics.rs | series RSS residual (umbrella finding 7); not required, since the exporter keeps its own memory metrics (C7 removes the 4 call-site lines) |
| crates/pdata/src/arrays.rs, otlp/attributes.rs, otlp/common.rs | visibility widenings with no caller outside pdata at HEAD |
| crates/pdata/benches/otlp_framing.rs, benches/README.md, the `[[bench]]` entry | measurement bench (unless Q4) |
| .chloggen: series-lake-format-revision-2, series-parquet-{alloy-batch, buffered-reference, decoded-attribute-budget, exemplar-policy, f1-multipart-orphans, max-series-per-request, merge-key-reservation, receiver-message-limit, t11-f2-lost-completion-probe, token-too-large}, engine-inbox-deadline-and-otlp-framing | pre-release changes to unreleased components, folded into the C6/C7 entries; the last is split into C1 and C3 |
| src/main.rs, crates/engine/src/memory_limiter.rs | only if Q2 drops C5b |

## Gates

Per commit, on origin/main plus the earlier commits, with no later commit present:

| Commit | Compiles alone | Tests | Also |
|---|---|---|---|
| C1 | yes | `cargo test -p otel-arrow-dfe-engine`; `-p otel-arrow-dfe-quiver --features test-hooks`; `-p otel-arrow-dfe-core-nodes --features durable-buffer durable_buffer` | |
| C2 | yes | `-p otel-arrow-dfe-pdata` (validate, views, payload); `-p otel-arrow-dfe-core-nodes --features file,otap,parquet` (file, otap and parquet exporters) | |
| C3 | yes after C2 | `-p otel-arrow-dfe-otap --features aws,azure object_store`; `-p otel-arrow-dfe-core-nodes --features parquet,azure parquet_exporter` | |
| C4 | yes | `-p otel-arrow-dfe-otap` (layers, otap_grpc, otlp_http); `-p otel-arrow-dfe-core-nodes --features otlp,otap` receivers | |
| C5 | yes | `-p otel-arrow-dfe-config byte_units`; `-p otel-arrow-dfe-core-nodes --features journald` | |
| C5b | yes | `-p otel-arrow-dfe --bin df_engine jemalloc_starts_with_its_background_thread` without `MALLOC_CONF` | musl and mimalloc builds compile |
| C6 | yes | `-p otel-arrow-dfe-series-lake` (unit, golden, golden_roundtrip, oracle, fuzz_*, cbor_allocation); gen_golden.py output equals tests/golden | xtask publish-policy check |
| C7 | needs C1-C6 | `-p otel-arrow-dfe-core-nodes --features series-parquet` (+ otap aws); `-p otel-arrow-dfe-component-inventory`; `cargo build --bin df_engine --features series-parquet,aws,durable-buffer`; minimal E2E against MinIO | zizmor on the workflow |

Every commit also passes upstream CI's own lines: `cargo fmt --check`,
`cargo clippy --locked --all-targets --all-features --workspace -- -D warnings`,
`cargo check --locked --no-default-features --workspace`, and
`cargo nextest run --locked --all-features --workspace` (CI enables every
feature, so series-lake's proptests and the series_parquet tests run in rust-ci;
check their wall time). Run the gates on the host only when no measured run
holds the lease.

## Open questions for the user

1. **core-nodes or contrib-nodes.** Upstream keeps its core umbrella feature lists exhaustive ("Keep these umbrella lists exhaustive"), while series-parquet is opt-in outside `core-exporters`, and the ClickHouse exporter already lives in contrib-nodes. Moving it to contrib-nodes fits both. The cost is that `log_gate` is a private core-nodes module: it would have to be copied or made public. Recommendation: ask upstream in the C7 PR description, and cut it in core-nodes as it stands.
2. **jemalloc `background_thread` default (C5b).** It changes allocator behaviour for every glibc build, and its evidence is in the dropped tree. Recommendation: its own PR after C7, or drop it.
3. **Small E2E lane.** Options: (a) a trimmed Python MinIO + DuckDB suite as an optional lane (the plan's choice); (b) a Rust `#[ignore]` test using the `testcontainers` crate, which upstream already uses in crates/validation; (c) none, relying on the Rust tests over local and in-memory stores. Recommendation: (a) for this cut, with (b) offered if reviewers object to Python.
4. **The `otlp_framing` pdata bench.** It shows reviewers what the framing walk costs next to the conversion. Recommendation: carry it in C2 even though it is a bench.
5. **gen_golden.py.** It is the independent oracle FORMAT.md cites, and it needs Python with xxhash. Recommendation: keep it, and run it in the E2E lane.
6. **`count_utf8_repairs` placement.** It sits in pdata and its only caller is series_parquet. Recommendation: C7, or a one-hunk pdata commit before C7 if reviewers want pdata PRs separate.
7. **Alloy configs in `configs/`.** They are the reference deployment, but they are not engine configs. Keep them in C7, or move them into the exporter README as snippets?
8. **Codeowners** for the new `metadata.yaml` files (series-lake, series_parquet_exporter).
9. **C4 change type.** Refusing with UNAVAILABLE instead of RESOURCE_EXHAUSTED changes what clients see on the wire: bug_fix or breaking?
10. **series-lake changelog component**: `pipeline` or `pdata`.

## User decisions (2026-09-26)

1. Placement: keep the exporter where it is (core-nodes, behind its feature).
2. jemalloc background_thread default: its own commit (C5b), not dropped.
3. Tests on the clean branch: only tests CI runs in minutes -- the Rust crate tests and a trimmed E2E (local storage and MinIO, a few minutes in total); no measurement, soak, chaos or fault-matrix harness.
4. C4 (receiver statuses): changelog `bug_fix` (OTLP-spec-conformant statuses; the old ones lost data or retried forever), with every status change and the per-connection wait listed in the subtext; revisit if maintainers ask.
5. Codeowners and the changelog component for series-lake: the common-sense choice (mirror the neighbouring nodes/crates).
Other open questions (otlp_framing bench, gen_golden.py, count_utf8_repairs placement, Alloy configs in configs/): the implementer decides by the same rule -- keep what the kept tests or docs need, drop the rest -- and lists each choice.
