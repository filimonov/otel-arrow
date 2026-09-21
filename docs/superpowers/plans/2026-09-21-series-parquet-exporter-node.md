# Series Parquet Exporter Node Implementation Plan

<!-- markdownlint-disable MD013 MD032 MD031 MD040 MD024 MD033 MD046 MD029 MD004 -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `exporter:series_parquet` for OTLP logs and metrics, with durable acknowledgements, bounded buffering, aligned windows, telemetry, and real-process local/S3 outage tests.

**Architecture:** A local exporter in `otel-arrow-dfe-core-nodes` owns admission, ACTIVE/FLUSHING, one pending request, completion delivery and shutdown. `otel-arrow-dfe-series-lake` owns extraction, schemas, cache, block accounting and Parquet writes; it stays engine-independent. Tests first run a real OTLP producer and `df_engine` against a local directory, then Grafana Alloy file tailing through OTLP gRPC into Docker-hosted MinIO and RustFS. DuckDB and ClickHouse independently verify the stored rows; synthetic Python requests retain fine-grained metrics and acknowledgement coverage.

**Tech Stack:** Workspace Rust 2024/MSRV 1.88, Arrow/Parquet 58.3, object_store 0.13.2, Tokio LocalSet, engine simulated clock, serde, Python unittest/grpcio/opentelemetry-proto/DuckDB/xxhash/boto3, ClickHouse, Grafana Alloy (River), Docker CLI.

**Spec:** `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md`, revision 4; section 11 step 2 plus metrics, sections 3.2, 6.2-6.7, 7, 8, 9.2, 9.4 and the v1 test in 9.5. Section 3.2 is the node boundary. Plan 1 supplies the core; plan 3 owns benchmarks and gates.

## Global Constraints

- No persistent local state. LocalFileSystem is a destination backend in examples/tests, never a WAL or spill directory.
- Never more than ACTIVE plus FLUSHING. There is no third block and no spill.
- The series cache is an optimization, never correctness state.
- No producer ACK before its block is durable in object storage. Delivery is at-least-once, with no block-atomic read snapshot or replay deduplication.
- One pipeline instance equals one worker. Budgets are per worker. Set `policies.resources.core_allocation` explicitly.
- Component/module/feature name: `series_parquet`; URN: `urn:otel:exporter:series_parquet`; primary metric set: `exporter.series_parquet`.
- Default interval 15s, block 500MiB, requests/block 4096, flush deadline 60s, input 16MiB, extracted 32MiB, row 1MiB, nesting 32, cache 200000, run 8MiB, merge 16MiB, upload part 8MiB/concurrency 2/abort 5s, row group 64MiB, writer 96MiB, notify batch 64. Unsupported policy defaults to reject.
- Call `Block::seal(nanos_to_micros(wall.now_unix_nanos()))` before `Sink::write_block`. Never update `SeriesCache::mark_committed` on admission, partial success, failure or cancellation.
- Window arithmetic uses Unix **seconds**; seal timestamps use Unix **microseconds**; the injected wall clock returns Unix **nanoseconds**. Sleeps and absolute deadlines use `engine::clock`, not wall time.
- Strip headers and authorization claims before retaining a token. Retain no original payload or converted input after extraction.
- Rust, YAML and Python source and changelog entries are ASCII-only. Every test has immediately preceding `Scenario:` and `Guarantees:` comments.
- All future cargo commands run from `rust/otap-dataflow`. Run affected-crate `cargo check` after Rust edits, focused tests for each task, and `cargo xtask check` before finalizing. Workspace lints include `missing_docs`, `unused_results`, `unwrap_used` and stdout/stderr restrictions; use `otel_*` events in production.
- New source files start with the repository copyright/SPDX header. New public items have documentation. Existing crates retain their README and `[lints] workspace = true`; no new crate is necessary.
- Plan 1 is finished and merged into this branch at `7fd7c3eab`; additive series-lake edits are authorized. Engine/otap changes are restricted to the necessary additive support in task 2 and task 7 step 6, each with its own tests and File Structure justification. No shutdown-policy or engine-timeout change is included. This planning session changes only this Markdown file and runs no cargo build or Git mutation. Commands in tasks are instructions for the future implementer.
- Reviews for this plan run via codex, as required by plan 1.
- Block-sized synchronous series sealing is an accepted v1 allowance (plan 1 F-park; `crates/series-lake/docs/FORMAT.md`, Limitations of version 1). Budget one additional B for sealing and include its CPU cost in responsiveness statements; do not claim a fixed-workspace bound. Plan 3 validates memory allowances; plan 2 reports the RSS residual.
- Every commit below includes both required trailers. Stage explicit task files, never `git add .`, because other work can share this tree.
- Docker tests skip when the daemon/CLI or selected local image is absent unless `SERIES_REQUIRE_DOCKER=1`, which makes absence fail. MinIO, RustFS and ClickHouse use env-overridable local tags with defaults `minio/minio:RELEASE.2025-04-22T22-12-26Z`, `rustfs/rustfs:1.0.0-rc.3`, and `clickhouse/clickhouse-server:26.7.4`. Only the Alloy image may be pulled. Startup, protocol, assertion and recovery failures after preflight fail. CI provisioning and digest pinning belong to plan 3.

## Ground truth and integration decisions

Paths in this section are repository-relative. All series-lake anchors were re-read against current HEAD `7fd7c3eab` for ruling C3, including unchanged anchors; task-text anchors use that same source snapshot. Re-read before future implementation if the branch advances.

| Evidence | Consequence |
| --- | --- |
| `rust/otap-dataflow/crates/series-lake/src/lib.rs:9` | Public modules are `attrs`, `buffer`, `cache`, `canonical`, `clock`, `config`, `error`, `extract`, `schema`, `sink`, `sort`, `value`; use crate alias `lake` in adapter modules. |
| `rust/otap-dataflow/crates/series-lake/src/config.rs:209`, `:324`, `:384` | `LakeConfig` has flat `window_interval`, integer byte fields and no storage/cache/notify settings. Adapt the user YAML; call `validate()`. It checks a positive whole-second interval, writer ID, row/run ratio, block/extracted ratio, request count, upload minimum 5MiB/concurrency, denormalization collisions/paths, and every values dataset's sort columns. Add positive/whole-second interval checks at the adapter boundary. |
| `rust/otap-dataflow/crates/series-lake/src/extract/mod.rs:68`, `:83`, `:108` | `extract(&mut OtapArrowRecords, &LakeConfig) -> Result<Extracted>` decodes transport IDs itself. `Extracted` owns descriptors, `(Dataset, Vec<RecordBatch>)`, pinned bytes and statistics. Current statistics aggregate unsupported kinds and mismatched columns: task 7 adds bounded detail. |
| `rust/otap-dataflow/crates/series-lake/src/buffer.rs:231`, `:280`, `:350`, `:459` | `reserve(&Extracted, &mut SeriesCache, usize, &LakeConfig) -> Result<Reservation>`, `admit(Extracted, Reservation, T)`, `seal(i64)`. `admit` consumes its token even on error and can partially mutate; keep tokens in an adapter-owned `OwnedBlock` beside `Block<()>`, charge their full size through `reserve`, and fail that whole ACTIVE block on an admission error. Do not call `into_parts` on an unsealed failure. |
| `rust/otap-dataflow/crates/series-lake/src/sink.rs:44`, `:95`, `:134`, `:330` | `FileNaming::new(&str)` creates a boot UUID; `FlushReport.files` is `Vec<(Dataset, Path, usize)>`; `Sink::new(Arc<dyn ObjectStore>, LakeConfig, FileNaming)` and `write_block<T>(&Block<T>, &CancellationToken)`. The runtime guard at `sink.rs:339` returns Invalid for an unsealed block. The adapter always seals explicitly. |
| `rust/otap-dataflow/crates/series-lake/src/cache.rs:44`, `:62` | `is_committed` changes LRU/stats; `mark_committed(id, partition)` may reinsert an evicted ID. Use the returned FLUSHING block's partition, never ACTIVE's. |
| `rust/otap-dataflow/crates/series-lake/src/clock.rs:62`, `:122`, `:137`, `:157`, `:189` | `WallClock`, `WindowClock`, `WakeOutcome::{RotationRequested { effective_boundary }, TooEarly { sleep_until }}` exist. Re-arm immediately, even when rotation is blocked. |
| `rust/otap-dataflow/crates/series-lake/src/error.rs:8`, `:23` | Reasons are `RequestTooLarge`, `BlockFull`, `TooManyRequests`, `Invalid(String)`, `Unsupported(String)`. Errors also include Arrow, Parquet, ObjectStore, Pdata, `Cancelled { abort_error }`, and `AbortFailed { source, abort_error }`. BlockFull/TooManyRequests park work; they are not content nacks. |
| `rust/otap-dataflow/crates/engine/src/local/exporter.rs:54`, `:91`; `rust/otap-dataflow/crates/engine/src/runtime_pipeline.rs:484` | Implement `Exporter<OtapPdata>` using `#[async_trait(?Send)]`, returning `TerminalState`; `spawn_local` is valid. |
| `rust/otap-dataflow/crates/engine/src/message.rs:283`, `:428`, `:810`, `:874` | `ExporterInbox::recv_when(false)` still force-drains pdata after Shutdown. There is no force marker. Task 2 exposes the already-latched shutdown deadline, needed when completion delivery is full. |
| `rust/otap-dataflow/crates/engine/src/control.rs:183`, `:225`, `:235`, `:338`, `:349`; `rust/otap-dataflow/crates/engine/src/effect_handler.rs:367` | Permanent content rejection needs `new_permanent_with_cause(reason, data, NackCause::Refused)`, not merely the cause. Storage uses retryable Unspecified; shutdown uses retryable NodeShutdown. Completion sends await bounded channels. |
| `rust/otap-dataflow/crates/otap/src/pdata.rs:143`, `:512`, `:523`, `:578`, `:831`, `:1068`; `rust/otap-dataflow/crates/pdata/src/payload.rs:299`, `:328` | Context stack/claims are private; `frames()` is test-only. Task 2 adds claims removal and retained frame accounting. `OtapPdata::into_parts` and `OtapPayload::empty(SignalType)` support small notifications; import `ConsumerEffectHandlerExtension` for notify methods. `num_bytes` needs mutable payload and can return None. |
| `rust/otap-dataflow/crates/engine/src/control.rs:77`, `:138`; `rust/otap-dataflow/crates/telemetry/src/reporter.rs:222`, `:259` | CallData is a SmallVec that may spill: include spilled capacity in token accounting. Use report for plain sets and report_measurement for measurement sets; the latter preserves pending data on a full reporter channel. |
| `rust/otap-dataflow/crates/engine/src/clock.rs:54`, `:84`; `rust/otap-dataflow/crates/engine/src/pipeline_ctrl.rs:542` | Local sleeps survive cancellation of engine periodic timers. Receivers receive DrainIngress first; exporters receive Shutdown only after receiver draining. |
| `rust/otap-dataflow/crates/core-nodes/src/exporters/parquet_exporter/mod.rs:84`, `:135`, `:177`, `:1015`; `rust/otap-dataflow/crates/core-nodes/Cargo.toml:1` | Mirror factory/inventory/local wrapper and test wiring in core-nodes. Do not modify the existing parquet exporter. |
| `rust/otap-dataflow/crates/otap/src/object_store.rs:30`, `:165`, `:303`, `:311` | Reuse StorageType and RetryOptions, including S3 endpoint/auth/prefix behavior and bearer-token capability for Azure. Local directories must already exist. |
| `rust/otap-dataflow/crates/otap/src/otap_grpc/server_settings.rs:47`, `:171`; `rust/otap-dataflow/crates/otap/src/otap_grpc/otlp/server_new.rs:202`, `:521` | `protocols.grpc.wait_for_result: true`, `timeout: 180s`; channel send precedes ack wait. Receiver concurrency is clamped to channel capacity. |
| `rust/otap-dataflow/crates/validation/Cargo.toml:25`, `:40`, `:64`; `rust/otap-dataflow/crates/validation/src/container.rs:342` | Existing integration tests use a feature and Docker tests may be ignored. Add a real-process Python unittest runner under validation, with explicit Docker preflight and CI invocation; the existing in-process validator alone is not section 9.4 E2E. |
| `rust/otap-dataflow/crates/engine/src/memory_limiter.rs:955`; `rust/otap-dataflow/crates/engine/src/engine_metrics.rs:85` | RSS comes from `memory_stats::memory_stats().physical_mem`. Reuse the once-per-process engine monitor for residual reporting, not one RSS sample per exporter. |
| `rust/otap-dataflow/crates/controller/src/lib.rs:2148`; `rust/otap-dataflow/crates/admin/src/pipeline_group.rs:165` | Signal shutdown is hard-coded to 60s. Use existing `/api/v1/groups/shutdown?wait=true&timeout_secs=180` in examples/tests; do not invent a pipeline YAML deadline field. |
| `rust/otap-dataflow/AGENTS.md:1`; `rust/otap-dataflow/.chloggen/TEMPLATE.yaml:1`; `rust/otap-dataflow/.chloggen/config.yaml:28`; `Makefile:84` | Follow CONTRIBUTING, ASCII/test comments/lints/README/xtask rules. Copy the changelog template, use component pipeline, and validate at repository root with `make chlog-validate`. |

## File Structure

```text
rust/otap-dataflow/
  Cargo.toml, Cargo.lock                 feature forwarding/dependency lock
  components-baseline.json              additive inventory entry
  configs/series-parquet-local.yaml      runnable local slice
  configs/series-parquet-s3.yaml         S3-compatible example
  configs/series-parquet.alloy           exact file-tail producer for tests/docs
  .chloggen/series-parquet-exporter.yaml  end-user release note
  crates/core-nodes/Cargo.toml           optional core dependency and feature
  crates/core-nodes/README.md            component link
  crates/core-nodes/src/exporters/mod.rs feature-gated module
  crates/core-nodes/src/exporters/series_parquet/
    mod.rs                              factory and LocalSet loop
    config.rs                           YAML adapter and startup validation
    token.rs                            stripped contexts and bounded notifier
    worker.rs                           ACTIVE, pending, cache, rotation
    flush.rs                            owned sealed block, retry/cancel task
    window.rs                           wall/monotonic clock bridge
    metrics.rs                          worker instruments and accounting
    tests.rs                            engine inbox/completion harness
    README.md                           runnable config and public contracts
  crates/otap/src/pdata.rs               task 2: release claims and account retained routing frames
  crates/engine/src/message.rs           task 2: expose existing deadline during forced pdata drain
  crates/engine/src/engine_metrics.rs    task 7 step 6: aggregate worker bytes and report process RSS residual
  crates/series-lake/src/buffer.rs       optional same-window descriptor re-emission
  crates/series-lake/src/cache.rs        non-mutating committed-partition lookup
  crates/series-lake/src/extract/mod.rs  bounded per-column extraction counters
  crates/series-lake/src/extract/metrics.rs bounded unsupported-kind counters
  crates/validation/tests/series_parquet/
    requirements.txt                    real producer/reader dependencies
    test_e2e.py                         local, Docker, shutdown, outage cases
```

No new runtime endpoint is introduced. The existing admin telemetry and shutdown endpoints are used only as test/operational clients. Each task repeats the consumed/produced signatures and gives all new code; apply replacement functions literally and preserve other functions in that module.

---

### Task 1: Runnable logs -> LocalFileSystem slice

**Files:**
- Create: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{mod.rs,config.rs,tests.rs,README.md}`
- Create: `rust/otap-dataflow/configs/series-parquet-local.yaml`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/{requirements.txt,test_e2e.py}`
- Modify: `rust/otap-dataflow/{Cargo.toml,Cargo.lock,components-baseline.json}`
- Modify: `rust/otap-dataflow/crates/core-nodes/{Cargo.toml,src/exporters/mod.rs}`

**Interfaces:**
- Consumes: `lake::extract::extract(&mut OtapArrowRecords, &LakeConfig) -> lake::Result<Extracted>`; `Block::new(i64,u64,&LakeConfig)`, `reserve`, `admit`, `seal(i64)`; `Sink::write_block(&Block<T>, &CancellationToken) -> lake::Result<FlushReport>`.
- Produces: `Config { storage: StorageType, retry: Option<RetryOptions>, lake: LakeConfig, window: Window, cache_entries: usize, notify_batch: usize }`, `Config: Deserialize`; `SeriesParquet::new(Config) -> Self`; factory `SERIES_PARQUET`; a runnable engine config and a real gRPC/DuckDB test. This first implementation flushes one request at a time, preserving durable ack ordering; later tasks replace its loop with bounded window batching.

- [ ] **Step 1: Add the failing real-process test and its dependencies**

`requirements.txt`:

```text
boto3>=1.35,<2
duckdb>=1.1,<2
grpcio>=1.66,<2
opentelemetry-proto>=1.27,<2
PyYAML>=6,<7
xxhash>=3.5,<4
```

Initial `test_e2e.py` (all later Python additions are in this file; keep the `unittest.main()` guard last):

```python
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Real OTLP producer, df_engine process and Parquet reader."""
import concurrent.futures
import contextlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.request

import duckdb
import grpc
import yaml
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2 as logs_pb
from opentelemetry.proto.collector.logs.v1 import logs_service_pb2_grpc as logs_rpc

WORKSPACE = Path(__file__).resolve().parents[4]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def log_request(request_id):
    req = logs_pb.ExportLogsServiceRequest()
    resource = req.resource_logs.add()
    attr = resource.resource.attributes.add(key="host.id")
    attr.value.string_value = "producer-1"
    scope = resource.scope_logs.add()
    scope.scope.name = "series-e2e"
    record = scope.log_records.add(time_unix_nano=1789960500000000000)
    record.body.string_value = request_id
    return req


class Engine:
    def __init__(self, directory, storage=None, overrides=None):
        self.root = Path(directory)
        self.data = self.root / "data"
        self.data.mkdir(exist_ok=True)
        self.grpc_port = free_port()
        self.admin_port = free_port()
        self.config = yaml.safe_load(
            (WORKSPACE / "configs/series-parquet-local.yaml").read_text()
        )
        nodes = self.config["groups"]["default"]["pipelines"]["main"]["nodes"]
        nodes["receiver"]["config"]["protocols"]["grpc"]["listening_addr"] = (
            f"127.0.0.1:{self.grpc_port}"
        )
        export = nodes["exporter"]["config"]
        export["storage"] = storage or {"file": {"base_uri": str(self.data)}}
        export["window"]["interval"] = "1s"
        if overrides:
            for key, value in overrides.items():
                export[key] = value
        self.path = self.root / "pipeline.yaml"
        self.path.write_text(yaml.safe_dump(self.config))
        binary = Path(os.environ.get("DF_ENGINE", WORKSPACE / "target/debug/df_engine"))
        if not binary.is_file():
            raise AssertionError(f"build the feature-enabled engine first: {binary}")
        self.log = (self.root / "engine.log").open("w+")
        self.process = subprocess.Popen(
            [str(binary), "--config", str(self.path), "--http-admin-bind",
             f"127.0.0.1:{self.admin_port}"],
            stdout=self.log, stderr=subprocess.STDOUT,
        )
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.grpc_port}")
        try:
            grpc.channel_ready_future(self.channel).result(timeout=30)
        except Exception:
            self.close()
            raise
        self.logs = logs_rpc.LogsServiceStub(self.channel)

    def shutdown(self, seconds=180):
        url = (f"http://127.0.0.1:{self.admin_port}/api/v1/groups/shutdown"
               f"?wait=true&timeout_secs={seconds}")
        with urllib.request.urlopen(
            urllib.request.Request(url, method="POST"), timeout=seconds + 5
        ) as response:
            if response.status != 200:
                raise AssertionError(response.read().decode())

    def close(self):
        self.channel.close()
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.log.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class LocalSlice(unittest.TestCase):
    # Scenario: a real gRPC logs request reaches a local series exporter.
    # Guarantees: a successful OTLP response has both descriptor and values files.
    def test_local_logs_are_durable_at_ack(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            engine.logs.Export(log_request("request-0"), timeout=20)
            values = list(engine.data.glob("v=1/signal=logs/dataset=values/**/*.parquet"))
            series = list(engine.data.glob("v=1/signal=logs/dataset=series/**/*.parquet"))
            self.assertTrue(values)
            self.assertTrue(series)
            with duckdb.connect() as db:
                rows = db.execute("SELECT body FROM read_parquet(?)",
                                  [[str(path) for path in values]]).fetchall()
            self.assertEqual(rows, [("request-0",)])
            engine.shutdown()


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test before wiring the component**

```bash
cd rust/otap-dataflow
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install -r crates/validation/tests/series_parquet/requirements.txt
cargo build --bin df_engine
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice -v
```

Expected: FAIL because the config/component does not exist. Preserve the engine log on an unexpected failure; do not accept an unrelated Python import error as the red test.

- [ ] **Step 3: Add validated configuration and feature wiring**

Add `otel-arrow-dfe-series-lake = { workspace = true, optional = true }` to core-nodes dependencies. Add `series_parquet = ["dep:otel-arrow-dfe-series-lake", "dep:object_store"]` to its features and `"series_parquet"` to `core-exporters`. Add root feature `series_parquet = ["otel-arrow-dfe-core-nodes/series_parquet"]`. The workspace dependency on series-lake already comes from plan 1; retain it. In `exporters/mod.rs` add:

```rust
/// Series/values Parquet exporter with durable acknowledgements.
#[cfg(feature = "series_parquet")]
pub mod series_parquet;
```

Insert this entry in the sorted inventory baseline (the factory annotation is the scanner input):

```json
{
  "id": "urn:otel:exporter:series_parquet",
  "category": "Exporter",
  "description": null,
  "attributes": {}
}
```

`config.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! User configuration mapped onto the engine-independent lake configuration.
use otel_arrow_dfe_otap::object_store::{RetryOptions, StorageType};
use otel_arrow_dfe_series_lake::config::{LakeConfig, SignalConfig, UnsupportedPolicy};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Window {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(deserialize_with = "byte_size")]
    pub max_block_bytes: usize,
    pub max_requests_per_block: usize,
    #[serde(with = "humantime_serde")]
    pub flush_retry_deadline: Duration,
}
impl Default for Window {
    fn default() -> Self {
        Self { interval: Duration::from_secs(15), max_block_bytes: 500 << 20,
            max_requests_per_block: 4096, flush_retry_deadline: Duration::from_secs(60) }
    }
}
fn byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    let n = otel_arrow_dfe_config::byte_units::deserialize_u64(d)?
        .ok_or_else(|| serde::de::Error::custom("byte size cannot be null"))?;
    usize::try_from(n).map_err(serde::de::Error::custom)
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Cache { max_entries: usize }
impl Default for Cache {
    fn default() -> Self { Self { max_entries: 200_000 } }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    storage: StorageType,
    #[serde(default)] retry: Option<RetryOptions>,
    #[serde(default = "writer")] writer_id: String,
    #[serde(default = "producer")] producer_id_attribute: String,
    #[serde(default)] window: Window,
    #[serde(default = "object")] ingress: Value,
    #[serde(default)] series_cache: Cache,
    #[serde(default = "object")] sorting: Value,
    #[serde(default = "object")] upload: Value,
    #[serde(default = "object")] parquet: Value,
    #[serde(default = "batch")] notify_batch: usize,
    #[serde(default)] unsupported: UnsupportedPolicy,
    #[serde(default)] logs: SignalConfig,
    #[serde(default)] metrics: SignalConfig,
}
fn writer() -> String { "writer".into() }
fn producer() -> String { "host.id".into() }
fn batch() -> usize { 64 }
fn object() -> Value { serde_json::json!({}) }
fn normalized(mut value: Value) -> Result<Value, String> {
    let fields = value.as_object_mut().ok_or("budget section must be an object")?;
    for (name, value) in fields {
        if name.ends_with("_bytes") {
            let n = byte_size(value.clone()).map_err(|e| e.to_string())?;
            *value = Value::from(n);
        }
    }
    Ok(value)
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(normalized(value)?).map_err(|e| e.to_string())
}
/// Validated exporter configuration. Storage and scheduling stay outside series-lake.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawConfig")]
pub struct Config {
    pub(super) storage: StorageType,
    pub(super) retry: Option<RetryOptions>,
    pub(super) lake: LakeConfig,
    pub(super) window: Window,
    pub(super) cache_entries: usize,
    pub(super) notify_batch: usize,
}
impl TryFrom<RawConfig> for Config {
    type Error = String;
    fn try_from(mut raw: RawConfig) -> Result<Self, String> {
        let interval = raw.window.interval;
        if interval.is_zero() || interval.subsec_nanos() != 0
            || interval.as_secs() > i64::MAX as u64 {
            return Err("window.interval must be positive whole seconds fitting i64".into());
        }
        if raw.notify_batch == 0 || raw.series_cache.max_entries == 0
            || raw.window.flush_retry_deadline.is_zero() {
            return Err("notify_batch, cache entries and flush deadline must be positive".into());
        }
        for key in raw.ingress.as_object().ok_or("ingress must be an object")?.keys() {
            if !["max_request_bytes", "max_extracted_bytes", "max_row_bytes",
                 "max_nesting_depth"].contains(&key.as_str()) {
                return Err(format!("unknown ingress setting {key}"));
            }
        }
        let parquet = raw.parquet.as_object_mut().ok_or("parquet must be an object")?;
        if let Some(compression) = parquet.remove("compression") {
            if compression != Value::String("zstd".into()) {
                return Err("parquet.compression must be zstd".into());
            }
        }
        let mut lake = LakeConfig {
            writer_id: raw.writer_id, producer_id_attribute: raw.producer_id_attribute,
            window_interval: interval, ingress: decode(raw.ingress)?,
            sorting: decode(raw.sorting)?, upload: decode(raw.upload)?,
            parquet: decode(raw.parquet)?, unsupported: raw.unsupported,
            logs: raw.logs, metrics: raw.metrics,
        };
        lake.ingress.max_block_bytes = raw.window.max_block_bytes;
        lake.ingress.max_requests_per_block = raw.window.max_requests_per_block;
        if [lake.ingress.max_request_bytes, lake.ingress.max_extracted_bytes,
            lake.ingress.max_row_bytes, lake.ingress.max_nesting_depth,
            lake.sorting.run_target_bytes, lake.sorting.merge_chunk_bytes,
            lake.parquet.row_group_bytes, lake.parquet.writer_limit_bytes]
            .contains(&0) || lake.upload.abort_timeout.is_zero() {
            return Err("all byte, depth and abort budgets must be positive".into());
        }
        let _ = lake.ingress.max_requests_per_block.checked_mul(2)
            .ok_or("request count overflows notification capacity")?;
        lake.validate().map_err(|e| e.to_string())?;
        if let Some(retry) = &raw.retry { retry.validate().map_err(|e| e.to_string())?; }
        Ok(Self { storage: raw.storage, retry: raw.retry, lake, window: raw.window,
            cache_entries: raw.series_cache.max_entries, notify_batch: raw.notify_batch })
    }
}
```

`mod.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! Local series Parquet exporter.
pub mod config;
#[cfg(test)] mod tests;
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::{OTAP_EXPORTER_FACTORIES, pdata::OtapPdata};
use otel_arrow_dfe_pdata::{TryIntoWithOptions, otap::OtapArrowRecords, payload::OtapPayload};
use otel_arrow_dfe_series_lake as lake;
use lake::clock::WallClock;
use tokio_util::sync::CancellationToken;

/// Registered component identifier.
pub const SERIES_PARQUET_URN: &str = "urn:otel:exporter:series_parquet";
otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = SERIES_PARQUET_URN, target = "otel.exporter.series_parquet",
);
/// Series exporter factory.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static SERIES_PARQUET: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SERIES_PARQUET_URN,
    create: |_pipeline, node, node_config, exporter_config, capabilities| {
        let config: config::Config = serde_json::from_value(node_config.config.clone())
            .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            })?;
        let mut exporter = SeriesParquet::new(config);
        if exporter.config.storage.requires_bearer_token_provider() {
            exporter.token_provider = Some(capabilities.require_shared::<BearerTokenProvider>()
                .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                    error: e.to_string(),
                })?);
        }
        Ok(ExporterWrapper::local(exporter, node, node_config, exporter_config))
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<config::Config>,
};
/// One independently budgeted pipeline worker.
pub struct SeriesParquet {
    config: config::Config,
    token_provider: Option<Box<dyn
        otel_arrow_dfe_engine::shared::capability::auth::bearer_token_provider::BearerTokenProvider>>,
}
impl SeriesParquet {
    /// Construct an exporter from validated configuration.
    #[must_use]
    pub fn new(config: config::Config) -> Self { Self { config, token_provider: None } }
}
#[async_trait(?Send)]
impl Exporter<OtapPdata> for SeriesParquet {
    async fn start(mut self: Box<Self>, mut inbox: ExporterInbox<OtapPdata>,
        effects: EffectHandler<OtapPdata>) -> Result<TerminalState, Error> {
        let store = otel_arrow_dfe_otap::object_store::from_storage_type_with_retry_and_token_provider(
            &self.config.storage, self.config.retry.as_ref(), self.token_provider.take(),
        ).map_err(|e| Error::ExporterError {
            exporter: effects.exporter_id(), kind: ExporterErrorKind::Configuration,
            error: e.to_string(), source_detail: e.to_string(),
        })?;
        let sink = lake::sink::Sink::new(store, self.config.lake.clone(),
            lake::sink::FileNaming::new(&self.config.lake.writer_id));
        let mut cache = lake::cache::SeriesCache::new(self.config.cache_entries);
        let mut seq = 0_u64;
        loop {
            match inbox.recv().await? {
                Message::PData(data) => {
                    let (context, mut payload) = data.into_parts();
                    let signal = payload.signal_type();
                    let input_ok = payload.num_bytes().is_some_and(|n|
                        n <= self.config.lake.ingress.max_request_bytes);
                    let result: lake::Result<()> = async {
                        if !input_ok { return Err(lake::Error::Refused(lake::RefuseReason::RequestTooLarge)); }
                        if signal != otel_arrow_dfe_config::SignalType::Logs {
                            return Err(lake::Error::Refused(lake::RefuseReason::Unsupported("signal".into())));
                        }
                        let mut records: OtapArrowRecords = payload.try_into_with_default()
                            .map_err(|e| lake::Error::Pdata(format!("{e}")))?;
                        let extracted = lake::extract::extract(&mut records, &self.config.lake)?;
                        drop(records);
                        if extracted.stats.rows == 0 { return Ok(()); }
                        let wall = lake::clock::SystemWallClock;
                        let secs = lake::clock::nanos_to_secs(wall.now_unix_nanos());
                        let clock = lake::clock::WindowClock::new(self.config.window.interval, secs);
                        let mut block = lake::buffer::Block::new(clock.last_boundary(), seq, &self.config.lake);
                        seq = seq.checked_add(1).ok_or_else(|| lake::Error::invalid("sequence exhausted"))?;
                        let reservation = block.reserve(&extracted, &mut cache, 0, &self.config.lake)?;
                        block.admit(extracted, reservation, ())?;
                        block.seal(lake::clock::nanos_to_micros(wall.now_unix_nanos()))?;
                        let _ = sink.write_block(&block, &CancellationToken::new()).await?;
                        for id in &block.pending_series { cache.mark_committed(*id, block.partition); }
                        Ok(())
                    }.await;
                    let data = OtapPdata::new(context, OtapPayload::empty(signal));
                    let delivered = match result {
                        Ok(()) => effects.notify_ack(AckMsg::new(data)).await,
                        Err(e @ (lake::Error::Refused(_) | lake::Error::Pdata(_) | lake::Error::Arrow(_))) =>
                            effects.notify_nack(NackMsg::new_permanent_with_cause(e.to_string(), data, NackCause::Refused)).await,
                        Err(e) => effects.notify_nack(NackMsg::new(e.to_string(), data)).await,
                    };
                    if let Err(e) = delivered { otel_warn!("series_parquet.notify_failed", error = %e); }
                }
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) =>
                    return Ok(TerminalState::new(deadline, [])),
                Message::Control(_) => {}
            }
        }
    }
}
```

`tests.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
use super::config::Config;
/// Scenario: the full adapter maps byte strings and rejects an impossible lake budget.
/// Guarantees: startup validates the same constraints as the public core API.
#[test]
fn configuration_maps_and_validates() {
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "15s", "max_block_bytes": "64MiB"},
        "parquet": {"compression": "zstd"}
    })).expect("valid config");
    assert_eq!(cfg.lake.ingress.max_block_bytes, 64 << 20);
    assert!(serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "upload": {"part_bytes": "1MiB"}
    })).is_err());
}
```

- [ ] **Step 4: Add the exact runnable YAML and try the slice**

`configs/series-parquet-local.yaml`:

```yaml
version: otel_dataflow/v1
policies:
  resources:
    core_allocation: {type: core_count, count: 1}
engine:
  telemetry: {reporting_interval: 1s}
groups:
  default:
    pipelines:
      main:
        policies:
          channel_capacity:
            control: {node: 100, pipeline: 100, completion: 100}
            pdata: 128
        nodes:
          receiver:
            type: receiver:otlp
            config:
              protocols:
                grpc:
                  listening_addr: "127.0.0.1:4317"
                  wait_for_result: true
                  timeout: 180s
                  max_concurrent_requests: 128
          exporter:
            type: exporter:series_parquet
            config:
              storage: {file: {base_uri: /tmp/series-parquet}}
              writer_id: local-1
              producer_id_attribute: host.id
              window:
                interval: 15s
                max_block_bytes: 500MiB
                max_requests_per_block: 4096
                flush_retry_deadline: 60s
              ingress:
                max_request_bytes: 16MiB
                max_extracted_bytes: 32MiB
                max_row_bytes: 1MiB
                max_nesting_depth: 32
              series_cache: {max_entries: 200000}
              sorting: {enabled: true, run_target_bytes: 8MiB, merge_chunk_bytes: 16MiB}
              upload: {part_bytes: 8MiB, concurrency: 2, abort_timeout: 5s}
              parquet: {compression: zstd, row_group_bytes: 64MiB, writer_limit_bytes: 96MiB}
              notify_batch: 64
              unsupported: reject
              logs:
                series_attributes: [logger.name]
                denormalize: [resource.service.name]
                values_sort:
                  - {column: series_id, order: asc}
                  - {column: time_unix_nano, order: asc, nulls: last}
              metrics:
                denormalize: [resource.service.name]
                values_sort:
                  - {column: series_id, order: asc}
                  - {column: time_unix_nano, order: asc}
        connections:
          - {from: receiver, to: exporter}
```

Initial module README content:

```markdown
# Series Parquet exporter

Build with `--features series_parquet`; add `aws` for S3. The local example
is `configs/series-parquet-local.yaml`. Connect an OTLP receiver directly
with `wait_for_result: true`. An OK response means the request is durable;
retry timeouts and transient failures, allowing duplicates.

Create `/tmp/series-parquet` before starting. Use the existing admin shutdown
endpoint with `timeout_secs=180`; signal shutdown currently grants only 60s.
```

Commands, from `rust/otap-dataflow`:

```bash
cargo check -p otel-arrow-dfe-core-nodes --features series_parquet
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet series_parquet
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
mkdir -p /tmp/series-parquet
./target/debug/df_engine --config configs/series-parquet-local.yaml --validate-and-exit
./target/debug/df_engine --config configs/series-parquet-local.yaml --http-admin-bind 127.0.0.1:8080
```

In another terminal, from that same directory:

```bash
PYTHONPATH=crates/validation/tests/series_parquet /tmp/series-parquet-venv/bin/python -c 'from test_e2e import grpc, logs_rpc, log_request; channel = grpc.insecure_channel("127.0.0.1:4317"); logs_rpc.LogsServiceStub(channel).Export(log_request("hello-series"), timeout=180); channel.close()'
/tmp/series-parquet-venv/bin/python -c "import duckdb; print(duckdb.sql(\"SELECT body FROM read_parquet('/tmp/series-parquet/v=1/signal=logs/dataset=values/**/*.parquet', hive_partitioning=true, union_by_name=true)\").fetchall())"
curl -X POST 'http://127.0.0.1:8080/api/v1/groups/shutdown?wait=true&timeout_secs=180'
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice -v
```

Expected: config valid, `hello-series` readable, E2E PASS. The build and run commands are execution instructions, not commands to run while authoring this plan.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/Cargo.toml rust/otap-dataflow/Cargo.lock rust/otap-dataflow/components-baseline.json rust/otap-dataflow/crates/core-nodes/Cargo.toml rust/otap-dataflow/crates/core-nodes/src/exporters/mod.rs rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet rust/otap-dataflow/configs/series-parquet-local.yaml rust/otap-dataflow/crates/validation/tests/series_parquet
git commit -m "feat(series_parquet): runnable durable local logs slice

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 2: Small ack tokens, persistent notification sends and force-drain visibility

**Files:**
- Create: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/token.rs`
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{mod.rs,tests.rs}`
- Modify: `rust/otap-dataflow/crates/otap/src/pdata.rs:512`
- Modify: `rust/otap-dataflow/crates/engine/src/message.rs:853` (read-only accessor plus its own forced-drain regression)

**Interfaces:**
- Consumes: `OtapPdata::into_parts() -> (Context, OtapPayload)`, `Context::take_transport_headers`, `EffectHandler::notify_ack/notify_nack` through `ConsumerEffectHandlerExtension`, `NackCause::{Refused,NodeShutdown,Unspecified}`.
- Produces: `Context::take_authorized_identity_entries() -> Option<AuthorizedIdentityEntries>`, `Context::retained_frame_bytes() -> usize`; `ExporterInbox::shutdown_deadline() -> Option<Instant>`; `AckToken::split(OtapPdata) -> (AckToken, OtapPayload)`, `AckToken::bytes() -> usize`; `Outcome`; `Notifier::{new,push,len,bytes,oldest,next}`. A notification send future remains stored across select iterations; dropping a select branch must never drop its token.
- Notification capacity is `2*N`, where `N=max_requests_per_block`. Normal total live tokens across blocks/pending/notifier are capped at `2*N-1`, leaving one credit to observe a force-drained shutdown message. Once shutdown is latched, stop reading if the notification capacity is full and drive its known deadline. This deliberately stricter admission cap resolves the otherwise unbounded forced-drain/completion-channel combination.

- [ ] **Step 1: Add failing tests for sensitive metadata and completion backpressure**

Append inside the existing `pdata.rs` tests module, where private fields are accessible:

```rust
/// Scenario: a retained completion context has excess frame capacity and captured metadata.
/// Guarantees: claims and headers can be released without losing routing, and capacity is charged.
#[test]
fn completion_context_can_release_metadata_and_measure_frames() {
    let mut context = Context::with_capacity(17);
    context.set_source_node(42);
    context.authorized_identity = Some(AuthorizedIdentityEntries::default());
    context.set_transport_headers(TransportHeaders::with_capacity(256));
    assert!(context.take_authorized_identity_entries().is_some());
    assert!(context.take_transport_headers().is_some());
    assert!(context.transport_headers().is_none());
    assert!(context.authorized_identity_entries().is_none());
    assert_eq!(context.source_node(), Some(42));
    let frame = context.stack.first_mut().expect("source frame");
    for n in 0_u64..32 { frame.route.calldata.push(n.into()); }
    assert!(context.retained_frame_bytes() >= 17 * std::mem::size_of::<Frame>() + 32 * 8);
}
```

Append to the existing `message.rs` tests module, reusing its `mpsc`, `LocalReceiver` and `TestMsg` fixtures:

```rust
/// Scenario: Shutdown is latched while an exporter with admission closed still has buffered pdata.
/// Guarantees: the read-only accessor exposes the existing deadline without changing forced-drain order.
#[tokio::test]
async fn exporter_deadline_is_visible_during_forced_drain() {
    let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<TestMsg>>::new(2);
    let (pdata_tx, pdata_rx) = mpsc::Channel::<TestMsg>::new(2);
    let mut inbox = ExporterInbox::new(
        Receiver::Local(LocalReceiver::mpsc(control_rx)),
        Receiver::Local(LocalReceiver::mpsc(pdata_rx)), 9, Interests::empty());
    assert_eq!(inbox.shutdown_deadline(), None);
    let deadline = crate::clock::now() + Duration::from_secs(1);
    pdata_tx.send_async(TestMsg::new("buffered")).await.expect("pdata");
    control_tx.send_async(NodeControlMsg::Shutdown {
        deadline, reason: "test".to_owned(),
    }).await.expect("shutdown");
    let message = inbox.recv_when(false).await.expect("forced data");
    assert!(matches!(message, Message::PData(TestMsg(ref body)) if body == "buffered"));
    assert_eq!(inbox.shutdown_deadline(), Some(deadline));
    drop(pdata_tx);
    assert!(matches!(inbox.recv_when(false).await.expect("shutdown control"),
        Message::Control(NodeControlMsg::Shutdown { deadline: observed, .. }) if observed == deadline));
}
```

Add the following to `series_parquet/tests.rs`; it also supplies a reusable real completion-channel harness:

```rust
use super::token::{AckToken, Notifier, Outcome};
use otel_arrow_dfe_engine::control::{PipelineCompletionMsg, pipeline_completion_msg_channel};
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_engine::testing::{test_node, test_pipeline_runtime_services};
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::payload::OtapPayload;
use otel_arrow_dfe_config::SignalType;
use std::time::Duration;

fn effects(capacity: usize) -> (EffectHandler<OtapPdata>,
    otel_arrow_dfe_engine::control::PipelineCompletionMsgReceiver<OtapPdata>) {
    let (_rx, reporter) = otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(16);
    let mut effects = EffectHandler::new(test_node("series"), reporter, test_pipeline_runtime_services());
    let (tx, rx) = pipeline_completion_msg_channel(capacity);
    effects.set_pipeline_completion_msg_sender(tx);
    (effects, rx)
}
fn empty_pdata() -> OtapPdata {
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, OtapPayload::empty(SignalType::Logs))
}
/// Scenario: the bounded engine completion channel fills while a second notification waits.
/// Guarantees: cancelling a poll preserves the second context and sends it exactly once later.
#[tokio::test(flavor = "current_thread")]
async fn notification_survives_cancelled_poll() {
    let (effects, mut rx) = effects(1);
    let mut notify = Notifier::new(effects, 2);
    for _ in 0..2 {
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Ack);
    }
    assert!(notify.next().await.is_ok());
    assert!(tokio::time::timeout(Duration::from_millis(5), notify.next()).await.is_err());
    assert_eq!(notify.len(), 1);
    assert!(matches!(rx.recv().await.expect("first completion"), PipelineCompletionMsg::DeliverAck { .. }));
    assert!(notify.next().await.is_ok());
    assert!(matches!(rx.recv().await.expect("second completion"), PipelineCompletionMsg::DeliverAck { .. }));
    assert_eq!(notify.len(), 0);
}
```

- [ ] **Step 2: Run red tests**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-otap completion_context_can_release_metadata_and_measure_frames
cargo test -p otel-arrow-dfe-engine exporter_deadline_is_visible_during_forced_drain
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet notification_survives_cancelled_poll
```

Expected: missing Context methods/token module. `AuthorizedIdentityEntries` derives Default at `rust/otap-dataflow/crates/otap/src/pdata.rs:70`; the test uses that existing constructor.

- [ ] **Step 3: Add only the required Context/inbox accessors, with their own regressions**

These additive accessors are strictly needed to strip retained metadata, charge frame capacity, and observe an already-latched shutdown deadline. Their own tests are in step 1 and must fail before these methods and pass afterward; no routing, drain or shutdown-timeout behavior changes.

In `impl Context`:

```rust
/// Remove captured authorization entries before retaining a completion token.
#[must_use]
pub fn take_authorized_identity_entries(&mut self) -> Option<AuthorizedIdentityEntries> {
    self.authorized_identity.take()
}
/// Retained frame allocation, including unused vector capacity.
#[must_use]
pub fn retained_frame_bytes(&self) -> usize {
    let spilled = self.stack.iter().filter(|frame| frame.route.calldata.spilled())
        .map(|frame| frame.route.calldata.capacity()
            * std::mem::size_of::<otel_arrow_dfe_engine::control::Context8u8>())
        .sum::<usize>();
    self.stack.capacity() * std::mem::size_of::<Frame>() + spilled
}
```

In `impl<PData> ExporterInbox<PData>`:

```rust
/// Deadline latched by the inbox while it force-drains buffered pdata.
///
/// Stateful exporters can keep shutdown bounded when a completion send is full
/// before the final Shutdown control message is released.
#[must_use]
pub fn shutdown_deadline(&self) -> Option<Instant> {
    self.core.shutting_down_deadline
}
```

- [ ] **Step 3b: Implement exporter-owned tokens and notifier**

`token.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! Payload-free completion ownership and bounded asynchronous delivery.
use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::time::Instant;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, clock};
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::payload::OtapPayload;

pub(super) struct AckToken {
    context: Context,
    signal: SignalType,
    pub received: Instant,
}
impl AckToken {
    pub fn split(data: OtapPdata) -> (Self, OtapPayload) {
        let (mut context, payload) = data.into_parts();
        let _ = context.take_transport_headers();
        let _ = context.take_authorized_identity_entries();
        let signal = payload.signal_type();
        (Self { context, signal, received: clock::now() }, payload)
    }
    pub fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.context.retained_frame_bytes()
    }
    fn pdata(self) -> OtapPdata { OtapPdata::new(self.context, OtapPayload::empty(self.signal)) }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Outcome { Ack, TooLarge, Invalid, Unsupported, Storage, Shutdown }
impl Outcome {
    pub fn reason(self) -> &'static str {
        match self { Self::Ack => "ack", Self::TooLarge => "too_large", Self::Invalid => "invalid",
            Self::Unsupported => "unsupported", Self::Storage => "storage", Self::Shutdown => "shutdown" }
    }
    pub fn refused(self) -> bool { matches!(self, Self::TooLarge | Self::Invalid | Self::Unsupported) }
}
type SendFuture = Pin<Box<dyn Future<Output = Result<(), Error>>>>;
struct Sending { future: SendFuture, bytes: usize, received: Instant }
pub(super) struct Notifier {
    effects: EffectHandler<OtapPdata>,
    queue: VecDeque<(AckToken, Outcome)>,
    sending: Option<Sending>,
    capacity: usize,
}
impl Notifier {
    pub fn new(effects: EffectHandler<OtapPdata>, capacity: usize) -> Self {
        Self { effects, queue: VecDeque::new(), sending: None, capacity }
    }
    pub fn len(&self) -> usize { self.queue.len() + usize::from(self.sending.is_some()) }
    pub fn bytes(&self) -> usize {
        self.queue.iter().map(|(t, _)| t.bytes()).sum::<usize>()
            + self.sending.as_ref().map_or(0, |s| s.bytes)
            + self.queue.capacity() * std::mem::size_of::<(AckToken, Outcome)>()
    }
    pub fn oldest(&self) -> Option<Instant> {
        self.queue.iter().map(|(t, _)| t.received)
            .chain(self.sending.iter().map(|s| s.received)).min()
    }
    pub fn push(&mut self, token: AckToken, outcome: Outcome) {
        assert!(self.len() < self.capacity, "worker must reserve completion credit");
        self.queue.push_back((token, outcome));
    }
    pub async fn next(&mut self) -> Result<(), Error> {
        if self.sending.is_none() {
            let Some((token, outcome)) = self.queue.pop_front() else { return pending().await; };
            let bytes = token.bytes();
            let received = token.received;
            let effects = self.effects.clone();
            self.sending = Some(Sending { bytes, received, future: Box::pin(async move {
                let data = token.pdata();
                match outcome {
                    Outcome::Ack => effects.notify_ack(AckMsg::new(data)).await,
                    outcome if outcome.refused() => effects.notify_nack(
                        NackMsg::new_permanent_with_cause(outcome.reason(), data, NackCause::Refused)).await,
                    Outcome::Shutdown => effects.notify_nack(NackMsg::new_with_cause(
                        "shutdown", data, NackCause::NodeShutdown)).await,
                    _ => effects.notify_nack(NackMsg::new("storage", data)).await,
                }
            }) });
        }
        let result = self.sending.as_mut().expect("send was installed").future.as_mut().await;
        self.sending = None;
        result
    }
}
```

Add `mod token;` to `mod.rs`. In the temporary one-request loop replace the split line by `let (token, mut payload) = token::AckToken::split(data);`; retain the existing signal local. Use the notifier to deliver the outcome: construct one `Notifier::new(effects.clone(), 2 * self.config.window.max_requests_per_block)` before the loop, replace `let data = OtapPdata::new(context, OtapPayload::empty(signal));` and the following delivery match with the following code, and remove now-unused direct notify imports:

```rust
let outcome = match result {
    Ok(()) => token::Outcome::Ack,
    Err(lake::Error::Refused(lake::RefuseReason::RequestTooLarge)) => token::Outcome::TooLarge,
    Err(lake::Error::Refused(lake::RefuseReason::Unsupported(_))) => token::Outcome::Unsupported,
    Err(lake::Error::Refused(_) | lake::Error::Pdata(_) | lake::Error::Arrow(_)) => token::Outcome::Invalid,
    Err(_) => token::Outcome::Storage,
};
notify.push(token, outcome);
if let Err(e) = notify.next().await { otel_warn!("series_parquet.notify_failed", error = %e); }
```

The complete nonblocking select, including `recv_when(false)` and the latched deadline, is task 3's independently tested deliverable. This task proves token release and send-future ownership without introducing another buffered payload.

- [ ] **Step 4: Verify metadata release, one completion, and the original slice**

Run:

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-otap completion_context_can_release_metadata_and_measure_frames
cargo test -p otel-arrow-dfe-engine exporter_deadline_is_visible_during_forced_drain
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet notification_survives_cancelled_poll
cargo check -p otel-arrow-dfe-otap -p otel-arrow-dfe-core-nodes --features otel-arrow-dfe-core-nodes/series_parquet
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice -v
```

Expected: PASS, two distinct channel deliveries, no headers/claims in retained contexts. Read `Context::clone_detached` at `pdata.rs:595`: it copies metadata and loses routing, so it is not an alternative to stripping.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/otap/src/pdata.rs rust/otap-dataflow/crates/engine/src/message.rs rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet
git commit -m "feat(series_parquet): retain small completion tokens and preserve blocked sends

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 3: Own ACTIVE/FLUSHING and commit only complete blocks

**Files:**
- Create: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{worker.rs,flush.rs}`
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{mod.rs,tests.rs}`

**Interfaces:**
- Consumes: `Config`, `AckToken`, `Notifier`, `Outcome`; `SeriesCache::mark_committed(SeriesId, PartitionId)`; `Sink::write_block(&Block<()>, &CancellationToken) -> Result<FlushReport>`.
- Produces: `OwnedBlock { data: Block<()>, tokens: Vec<AckToken> }`; `Worker::{new,live_tokens,accept,admit,rotate,complete,fail_active,shutdown}`; `FlushJob::{new,finish}`; `FlushDone { data: Block<()>, result: lake::Result<FlushReport> }`; `run(Config, Arc<dyn ObjectStore>, Arc<dyn WallClock>, ExporterInbox<OtapPdata>, EffectHandler<OtapPdata>) -> Result<TerminalState, engine::Error>`.
- The task owns the sealed data block; `FlushJob` owns its tokens until the result. Both halves remain one logical FLUSHING block, charged by its `data.bytes`. Keeping tokens outside the task permits deadline nacks without waiting on multipart cleanup and avoids losing contexts on task panic. No third block is introduced.

- [ ] **Step 1: Add a failure test that observes durable completion and cache state**

Append to `tests.rs`:

```rust
use super::worker::Worker;
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogsData, ResourceLogs, ScopeLogs, LogRecord};
use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use std::sync::Arc;
fn config() -> Config {
    serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/unused"}},
        "window": {"interval": "1s", "max_requests_per_block": 4}
    })).expect("config")
}
fn logs_pdata() -> OtapPdata {
    let logs = LogsData { resource_logs: vec![ResourceLogs {
        scope_logs: vec![ScopeLogs { log_records: vec![LogRecord {
            time_unix_nano: 1000, ..Default::default()
        }], ..Default::default() }], ..Default::default()
    }] };
    let mut context = Context::default();
    context.set_source_node(7);
    OtapPdata::new(context, encode_logs(&logs).into())
}
/// Scenario: a request is rotated into a real in-memory object-store flush.
/// Guarantees: no completion precedes both files, and only completion marks the cache committed.
#[tokio::test(flavor = "current_thread")]
async fn complete_files_before_ack() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(InMemory::new());
        let cfg = config();
        let (effects, mut rx) = effects(4);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut worker = Worker::new(cfg, store.clone(), wall, effects);
        worker.admit(logs_pdata());
        let id = *worker.active.data.pending_series.iter().next().expect("descriptor");
        assert!(!worker.cache.is_committed(&id, worker.active.data.partition));
        worker.rotate();
        assert!(tokio::time::timeout(Duration::from_millis(5), rx.recv()).await.is_err());
        let done = worker.flushing.as_mut().expect("flush").finish().await;
        let report = done.as_ref().expect("join").result.as_ref().expect("write");
        assert_eq!(report.files.len(), 2);
        for (_, path, _) in &report.files { assert!(store.head(path).await.is_ok()); }
        worker.complete(done);
        assert!(worker.cache.is_committed(&id, lake::clock::PartitionId::from_unix_secs(0)));
        assert!(worker.notify.next().await.is_ok());
        assert!(matches!(rx.recv().await.expect("ack"), PipelineCompletionMsg::DeliverAck { .. }));
    }).await;
}
```

- [ ] **Step 2: Run the failing lifecycle test**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet complete_files_before_ack
```

Expected: missing Worker/FlushJob. The test asserts object existence before accepting an acknowledgement, rather than equating task creation with durability.

- [ ] **Step 3: Implement block ownership and the local flush task**

`flush.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! One local flush task; dropping its owner requests bounded cancellation.
use super::token::AckToken;
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake as lake;
use std::{rc::Rc, time::Instant};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

pub(super) struct FlushDone {
    pub data: lake::buffer::Block<()>,
    pub result: lake::Result<lake::sink::FlushReport>,
}
pub(super) struct FlushJob {
    handle: JoinHandle<FlushDone>,
    pub cancel: CancellationToken,
    pub tokens: Vec<AckToken>,
    pub bytes: usize,
    pub started: Instant,
}
impl FlushJob {
    pub fn new(data: lake::buffer::Block<()>, tokens: Vec<AckToken>, sink: Rc<lake::sink::Sink>) -> Self {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let bytes = data.bytes;
        let handle = tokio::task::spawn_local(async move {
            let result = sink.write_block(&data, &task_cancel).await;
            FlushDone { data, result }
        });
        Self { handle, cancel, tokens, bytes, started: clock::now() }
    }
    pub async fn finish(&mut self) -> Result<FlushDone, JoinError> { (&mut self.handle).await }
}
impl Drop for FlushJob {
    fn drop(&mut self) { self.cancel.cancel(); }
}
```

`worker.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! Worker state with one active block and at most one owned flush.
use super::{config::Config, flush::{FlushDone, FlushJob}, token::{AckToken, Notifier, Outcome}};
use otel_arrow_dfe_engine::{clock, local::exporter::EffectHandler};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::{TryIntoWithOptions, otap::OtapArrowRecords};
use otel_arrow_dfe_series_lake as lake;
use lake::{buffer::Block, cache::SeriesCache, clock::{WallClock, nanos_to_micros, nanos_to_secs}};
use std::{rc::Rc, sync::Arc, time::Instant};

pub(super) struct OwnedBlock {
    pub data: Block<()>,
    pub tokens: Vec<AckToken>,
}
pub(super) struct Worker {
    pub cfg: Config,
    pub active: OwnedBlock,
    pub flushing: Option<FlushJob>,
    pub cache: SeriesCache,
    pub notify: Notifier,
    pub rotation_requested: bool,
    pub deadline: Option<Instant>,
    pub wall: Arc<dyn WallClock>,
    pub sink: Rc<lake::sink::Sink>,
    pub seq: u64,
}
impl Worker {
    pub fn new(cfg: Config, store: Arc<dyn object_store::ObjectStore>, wall: Arc<dyn WallClock>,
        effects: EffectHandler<OtapPdata>) -> Self {
        let secs = nanos_to_secs(wall.now_unix_nanos());
        let windows = lake::clock::WindowClock::new(cfg.window.interval, secs);
        let active = OwnedBlock { data: Block::new(windows.last_boundary(), 0, &cfg.lake), tokens: vec![] };
        let sink = Rc::new(lake::sink::Sink::new(store, cfg.lake.clone(),
            lake::sink::FileNaming::new(&cfg.lake.writer_id)));
        Self { active, sink, wall, seq: 1, flushing: None,
            cache: SeriesCache::new(cfg.cache_entries),
            notify: Notifier::new(effects, 2 * cfg.window.max_requests_per_block),
            cfg, rotation_requested: false, deadline: None }
    }
    pub fn live_tokens(&self) -> usize {
        self.active.tokens.len() + self.flushing.as_ref().map_or(0, |f| f.tokens.len()) + self.notify.len()
    }
    pub fn accept(&self) -> bool {
        self.deadline.is_none() && !self.rotation_requested
            && self.live_tokens() < 2 * self.cfg.window.max_requests_per_block - 1
    }
    pub fn refusal(error: &lake::Error) -> Outcome {
        match error {
            lake::Error::Refused(lake::RefuseReason::RequestTooLarge) => Outcome::TooLarge,
            lake::Error::Refused(lake::RefuseReason::Unsupported(_)) => Outcome::Unsupported,
            _ => Outcome::Invalid,
        }
    }
    pub fn admit(&mut self, data: OtapPdata) {
        let (token, mut payload) = AckToken::split(data);
        if !payload.num_bytes().is_some_and(|n| n <= self.cfg.lake.ingress.max_request_bytes) {
            self.notify.push(token, Outcome::TooLarge);
            return;
        }
        if payload.signal_type() != otel_arrow_dfe_config::SignalType::Logs {
            self.notify.push(token, Outcome::Unsupported);
            return;
        }
        let extracted = (|| {
            let mut records: OtapArrowRecords = payload.try_into_with_default()
                .map_err(|e| lake::Error::Pdata(e.to_string()))?;
            lake::extract::extract(&mut records, &self.cfg.lake)
        })();
        let extracted = match extracted {
            Ok(e) => e,
            Err(e) => { self.notify.push(token, Self::refusal(&e)); return; }
        };
        if extracted.stats.rows == 0 { self.notify.push(token, Outcome::Ack); return; }
        let reservation = match self.active.data.reserve(&extracted, &mut self.cache, token.bytes(), &self.cfg.lake) {
            Ok(r) => r,
            Err(e) => { self.notify.push(token, Self::refusal(&e)); return; }
        };
        match self.active.data.admit(extracted, reservation, ()) {
            Ok(()) => self.active.tokens.push(token),
            Err(_) => { self.notify.push(token, Outcome::Storage); self.fail_active(Outcome::Storage); }
        }
        self.rotation_requested = true;
    }
    fn new_active(&mut self) -> OwnedBlock {
        let secs = nanos_to_secs(self.wall.now_unix_nanos());
        let windows = lake::clock::WindowClock::new(self.cfg.window.interval, secs);
        let start = windows.last_boundary().max(self.active.data.window_start_secs);
        let seq = self.seq;
        self.seq = self.seq.checked_add(1).expect("u64 blocks cannot be exhausted in one process lifetime");
        OwnedBlock { data: Block::new(start, seq, &self.cfg.lake), tokens: vec![] }
    }
    pub fn fail_active(&mut self, outcome: Outcome) {
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        for token in old.tokens { self.notify.push(token, outcome); }
    }
    pub fn rotate(&mut self) {
        if self.flushing.is_some() { return; }
        self.rotation_requested = false;
        if self.active.data.is_empty() { return; }
        if self.active.data.seal(nanos_to_micros(self.wall.now_unix_nanos())).is_err() {
            self.fail_active(Outcome::Storage);
            return;
        }
        let next = self.new_active();
        let old = std::mem::replace(&mut self.active, next);
        self.flushing = Some(FlushJob::new(old.data, old.tokens, self.sink.clone()));
    }
    pub fn complete(&mut self, done: Result<FlushDone, tokio::task::JoinError>) {
        let mut job = self.flushing.take().expect("a selected completion has a job");
        let outcome = match done {
            Ok(done) if done.result.is_ok() => {
                for id in &done.data.pending_series { self.cache.mark_committed(*id, done.data.partition); }
                Outcome::Ack
            }
            _ if self.deadline.is_some_and(|d| clock::now() >= d) => Outcome::Shutdown,
            _ => Outcome::Storage,
        };
        for token in std::mem::take(&mut job.tokens) { self.notify.push(token, outcome); }
    }
    pub fn shutdown(&mut self, deadline: Instant) {
        self.deadline = Some(self.deadline.map_or(deadline, |old| old.min(deadline)));
        self.rotation_requested = true;
    }
}
```

Replace the temporary `Exporter::start` body in `mod.rs` after store construction with this call, preserving the same storage error mapping:

```rust
run(self.config.clone(), store, Arc::new(lake::clock::SystemWallClock), inbox, effects).await
```

Add `mod worker; mod flush;`. Replace the imports in `mod.rs` with this complete list:

```rust
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::{OTAP_EXPORTER_FACTORIES, pdata::OtapPdata};
use otel_arrow_dfe_series_lake as lake;
use std::sync::Arc;
```

Remove `mut` from the `inbox` parameter of `Exporter::start`; only `run` mutates it now. The complete new loop is:

```rust
async fn run(cfg: config::Config, store: Arc<dyn object_store::ObjectStore>,
    wall: Arc<dyn lake::clock::WallClock>, mut inbox: ExporterInbox<OtapPdata>,
    effects: EffectHandler<OtapPdata>) -> Result<TerminalState, Error> {
    let mut worker = worker::Worker::new(cfg, store, wall, effects);
    let mut notify_turns = 0;
    loop {
        if worker.deadline.is_some() && worker.active.data.is_empty()
            && worker.flushing.is_none() && worker.notify.len() == 0 {
            return Ok(TerminalState::new(worker.deadline.expect("shutdown"), []));
        }
        let accept = worker.accept();
        let can_read = worker.live_tokens() < 2 * worker.cfg.window.max_requests_per_block;
        let deadline = worker.deadline;
        tokio::select! {
            biased;
            () = async { match deadline {
                Some(d) => otel_arrow_dfe_engine::clock::sleep_until(d).await,
                None => std::future::pending().await,
            } } => {
                if let Some(job) = &worker.flushing { job.cancel.cancel(); }
                worker.fail_active(token::Outcome::Shutdown);
                return Ok(TerminalState::new(deadline.expect("deadline elapsed"), []));
            }
            done = async { match worker.flushing.as_mut() {
                Some(job) => job.finish().await,
                None => std::future::pending().await,
            } } => { worker.complete(done); notify_turns = 0; }
            result = worker.notify.next(), if worker.notify.len() > 0 && notify_turns < worker.cfg.notify_batch => {
                if let Err(e) = result { otel_warn!("series_parquet.notify_failed", error = %e); }
                notify_turns += 1;
            }
            () = std::future::ready(()), if worker.rotation_requested && worker.flushing.is_none() => {
                worker.rotate(); notify_turns = 0;
            }
            message = inbox.recv_when(accept), if can_read => {
                notify_turns = 0;
                match message? {
                    Message::PData(data) => {
                        if let Some(d) = inbox.shutdown_deadline() {
                            worker.shutdown(d);
                            let (token, payload) = token::AckToken::split(data);
                            drop(payload);
                            worker.notify.push(token, token::Outcome::Shutdown);
                        } else if accept {
                            worker.admit(data);
                        } else {
                            unreachable!("recv_when(false) only returns forced shutdown pdata");
                        }
                    }
                    Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => worker.shutdown(deadline),
                    Message::Control(_) => {}
                }
            }
            () = tokio::task::yield_now(), if notify_turns >= worker.cfg.notify_batch => { notify_turns = 0; }
        }
    }
}
```

Task 9 replaces the deadline branch with final nacks and bounded cleanup. This task already cancels on owner drop and never blocks inside a notification send. `DrainIngress` is deliberately not a trigger: the engine sends it to receivers only.

- [ ] **Step 4: Verify ownership, durable ordering and the slice**

```bash
cd rust/otap-dataflow
cargo check -p otel-arrow-dfe-core-nodes --features series_parquet
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet series_parquet
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice -v
```

Expected: PASS. While a write awaits I/O, the LocalSet keeps polling the inbox and notifier. The cache assertion is against the flushed partition, including reinsertion after eviction.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet
git commit -m "feat(series_parquet): own two blocks and acknowledge durable flush results

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 4: Reserve before mutation, park one request and propagate backpressure

**Files:**
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{worker.rs,mod.rs,tests.rs}`

**Interfaces:**
- Consumes: `OwnedBlock`, `Worker`, `AckToken::bytes`, `Block::reserve` with `RefuseReason::{RequestTooLarge,BlockFull,TooManyRequests}`.
- Produces: `Pending { extracted: Extracted, token: AckToken, admission_secs: i64 }`, `Worker.pending: Option<Pending>`, `Worker::{prepare,offer,resume_pending}`. `prepare` releases payload and conversion records before returning; `offer` recomputes reservations after rotation, never reuses a reservation from another partition/cache state. `Block<()>` is charged with the measured token size even though actual token ownership is beside it.

- [ ] **Step 1: Add red tests for atomic refusal and pending-before-new ordering**

Append to `tests.rs`:

```rust
/// Scenario: two requests fill a two-token block and a third request needs rotation.
/// Guarantees: exactly one extracted request is parked, input closes, and it enters the next block first.
#[tokio::test(flavor = "current_thread")]
async fn one_pending_request_resumes_before_new_input() {
    tokio::task::LocalSet::new().run_until(async {
        let mut cfg = config();
        cfg.window.max_requests_per_block = 2;
        cfg.lake.ingress.max_requests_per_block = 2;
        let (effects, _rx) = effects(8);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(cfg, Arc::new(InMemory::new()), wall, effects);
        w.admit(logs_pdata());
        w.admit(logs_pdata());
        w.admit(logs_pdata());
        assert_eq!(w.active.tokens.len(), 2);
        assert!(w.pending.is_some());
        assert!(!w.accept());
        w.rotate();
        w.resume_pending();
        assert!(w.pending.is_none());
        assert_eq!(w.active.tokens.len(), 1);
        assert_eq!(w.live_tokens(), 3);
        assert!(w.active.data.bytes <= w.cfg.window.max_block_bytes);
    }).await;
}
/// Scenario: a request's logical bytes exceed the configured input budget.
/// Guarantees: no conversion/admission changes the block, and the producer gets permanent Refused.
#[tokio::test(flavor = "current_thread")]
async fn oversized_input_is_refused_atomically() {
    let mut cfg = config();
    cfg.lake.ingress.max_request_bytes = 1;
    let (effects, mut rx) = effects(2);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut w = Worker::new(cfg, Arc::new(InMemory::new()), wall, effects);
    w.admit(logs_pdata());
    assert_eq!(w.active.data.bytes, 0);
    assert!(w.pending.is_none());
    w.notify.next().await.expect("delivery");
    match rx.recv().await.expect("nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(nack.permanent);
            assert_eq!(nack.cause, otel_arrow_dfe_engine::control::NackCause::Refused);
        }
        other => panic!("expected refusal, got {other:?}"),
    }
}
```

Also append this conversion-ownership regression. `OtapArrowRecords::get` returns a borrowed RecordBatch (`crates/pdata/src/otap/raw_batch_store.rs:212`); a Weak reference to an original array must expire when `prepare` returns.

```rust
/// Scenario: preparation consumes Arrow input whose original array is externally weakly observed.
/// Guarantees: the pending extraction retains no original input array or conversion record batch.
#[tokio::test(flavor = "current_thread")]
async fn prepare_releases_original_arrow_payload() {
    use otel_arrow_dfe_pdata::TryIntoWithOptions;
    use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    let (context, payload) = logs_pdata().into_parts();
    let records: OtapArrowRecords = payload.try_into_with_default().expect("records");
    let weak = Arc::downgrade(records.get(ArrowPayloadType::Logs)
        .expect("logs batch").column(0));
    let (effects, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(config(), Arc::new(InMemory::new()), wall, effects);
    let pending = worker.prepare(OtapPdata::new(context, records.into()));
    assert!(pending.is_ok());
    assert!(weak.upgrade().is_none(), "prepared output cannot pin input arrays");
}
/// Scenario: an invalid protobuf body reaches preparation after its byte-size check.
/// Guarantees: conversion failure is permanent Refused and does not mutate ACTIVE.
#[tokio::test(flavor = "current_thread")]
async fn malformed_conversion_is_refused_atomically() {
    let (context, _) = logs_pdata().into_parts();
    let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(
        bytes::Bytes::from_static(&[255]));
    let (effects, mut rx) = effects(2);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut w = Worker::new(config(), Arc::new(InMemory::new()), wall, effects);
    w.admit(OtapPdata::new(context, payload.into()));
    assert!(w.active.data.is_empty());
    w.notify.next().await.expect("delivery");
    match rx.recv().await.expect("nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(nack.permanent);
            assert_eq!(nack.cause, otel_arrow_dfe_engine::control::NackCause::Refused);
        }
        other => panic!("expected invalid-content refusal, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run the red budget tests**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet one_pending_request_resumes_before_new_input
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet oversized_input_is_refused_atomically
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet prepare_releases_original_arrow_payload
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet malformed_conversion_is_refused_atomically
```

Expected: pending fields/methods missing; the input rejection test should already pass and acts as regression protection.

- [ ] **Step 3: Implement bounded preparation, parking and re-reservation**

Add `pub pending: Option<Pending>` initialized to `None` to Worker, and this type:

```rust
pub(super) struct Pending {
    pub extracted: lake::extract::Extracted,
    pub token: AckToken,
    pub admission_secs: i64,
}
```

Replace `Worker::admit` and add the following functions:

```rust
pub fn prepare(&self, data: OtapPdata) -> Result<Pending, (AckToken, Outcome)> {
    let (token, mut payload) = AckToken::split(data);
    if !payload.num_bytes().is_some_and(|n| n <= self.cfg.lake.ingress.max_request_bytes) {
        return Err((token, Outcome::TooLarge));
    }
    if payload.signal_type() != otel_arrow_dfe_config::SignalType::Logs {
        return Err((token, Outcome::Unsupported));
    }
    let result = (|| {
        let mut records: OtapArrowRecords = payload.try_into_with_default()
            .map_err(|e| lake::Error::Pdata(e.to_string()))?;
        lake::extract::extract(&mut records, &self.cfg.lake)
    })();
    match result {
        Ok(extracted) => Ok(Pending { extracted, token,
            admission_secs: nanos_to_secs(self.wall.now_unix_nanos()) }),
        Err(e) => Err((token, Self::refusal(&e))),
    }
}
pub fn admit(&mut self, data: OtapPdata) {
    match self.prepare(data) {
        Ok(pending) => self.offer(pending),
        Err((token, outcome)) => self.notify.push(token, outcome),
    }
}
pub fn offer(&mut self, pending: Pending) {
    if pending.extracted.stats.rows == 0 {
        self.notify.push(pending.token, Outcome::Ack);
        return;
    }
    let clock = lake::clock::WindowClock::new(self.cfg.window.interval, self.active.data.window_start_secs);
    if clock.effective_boundary(pending.admission_secs) > self.active.data.window_start_secs {
        assert!(self.pending.is_none(), "only one extraction slot");
        self.pending = Some(pending);
        self.rotation_requested = true;
        return;
    }
    let reservation = self.active.data.reserve(&pending.extracted, &mut self.cache,
        pending.token.bytes(), &self.cfg.lake);
    match reservation {
        Err(lake::Error::Refused(lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests)) => {
            assert!(self.pending.is_none(), "only one extraction slot");
            self.pending = Some(pending);
            self.rotation_requested = true;
        }
        Err(e) => self.notify.push(pending.token, Self::refusal(&e)),
        Ok(reservation) => {
            match self.active.data.admit(pending.extracted, reservation, ()) {
                Ok(()) => self.active.tokens.push(pending.token),
                Err(_) => {
                    self.notify.push(pending.token, Outcome::Storage);
                    self.fail_active(Outcome::Storage);
                }
            }
            // Keep the runnable per-request slice until task 5 installs the window sleep.
            self.rotation_requested = true;
        }
    }
}
pub fn resume_pending(&mut self) {
    if !self.rotation_requested && self.deadline.is_none() {
        if let Some(pending) = self.pending.take() { self.offer(pending); }
    }
}
```

Add `usize::from(self.pending.is_some())` to `live_tokens`; add `self.pending.is_none()` to `accept`. In `rotate`, replace the empty-block return with:

```rust
if self.active.data.is_empty() {
    self.active = self.new_active();
    return;
}
```

After every `worker.rotate()` in `run`, call `worker.resume_pending()`. After `worker.complete(done)`, synchronously run the following before the next receive:

```rust
if worker.rotation_requested { worker.rotate(); }
worker.resume_pending();
```

In `shutdown` add this before requesting rotation:

```rust
if let Some(pending) = self.pending.take() {
    self.notify.push(pending.token, Outcome::Shutdown);
}
```

A resume uses the current block's partition and current committed cache contents. A failed descriptor upload cannot suppress the next block's descriptors. Reject `RequestTooLarge` even when ACTIVE is empty; never turn it into an endless rotation. Conversion/schema failures map to Invalid/Refused and the loop continues. Arrow/Parquet errors after mutation fail the affected block with Storage, not a content refusal.

- [ ] **Step 4: Verify budget and notification bounds**

Run the two focused tests and `cargo check -p otel-arrow-dfe-core-nodes --features series_parquet` from `rust/otap-dataflow`. Expected: PASS. Inspect that `Pending` contains no `OtapPdata`, `OtapPayload` or `OtapArrowRecords`, and `prepare` has no payload clone. Block bytes include token capacity through `AckToken::bytes`; spare token-vector capacity is explicitly included in retained accounting in task 7.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet
git commit -m "feat(series_parquet): bound admission and park one extracted request

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 5: Aligned wall-clock windows independent of engine timers

**Files:**
- Create: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/window.rs`
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{worker.rs,mod.rs,tests.rs}`

**Interfaces:**
- Consumes: `WindowClock::{new,on_wake,effective_boundary,next_boundary}`, `WakeOutcome`, `WallClock::now_unix_nanos`, engine `clock::{now,sleep_until,Sleep,SimClock}`.
- Produces: `Window { wall: Arc<dyn WallClock>, clock: WindowClock, sleep: engine::clock::Sleep }`, `Window::{new,wake,admission_boundary}`; Worker gains `window: Window`. ACTIVE's timestamp is never advanced until rotation. A late request stays in `Pending`; an already admitted request remains in the old window.

- [ ] **Step 1: Write clock and busy-flush tests**

Append to `tests.rs`:

```rust
/// Scenario: a forward clock jump occurs while rotation is blocked by a flushing block.
/// Guarantees: missed windows coalesce and the sleep is re-armed without admitting pdata.
#[tokio::test(flavor = "current_thread")]
async fn busy_rotation_rearms_boundary_sleep() {
    let sim = otel_arrow_dfe_engine::clock::SimClock::new();
    let _clock_guard = sim.install();
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut window = super::window::Window::new(Duration::from_secs(15), wall.clone());
    wall.set(65_000_000_000);
    sim.advance(Duration::from_secs(15));
    window.sleep.as_mut().await;
    assert!(window.wake());
    assert_eq!(window.clock.last_boundary(), 60);
    assert_eq!(window.clock.next_boundary(65), 75);
    assert!(futures::poll!(window.sleep.as_mut()).is_pending());
    wall.set(50_000_000_000);
    sim.advance(Duration::from_secs(10));
    window.sleep.as_mut().await;
    assert!(!window.wake());
    assert_eq!(window.clock.last_boundary(), 60);
}
/// Scenario: extraction finishes immediately before versus after a one-second boundary.
/// Guarantees: the earlier request stays in ACTIVE and the later one parks for the next window.
#[tokio::test(flavor = "current_thread")]
async fn admission_time_assigns_exactly_one_window() {
    let (effects, _rx) = effects(8);
    let wall = Arc::new(lake::clock::TestWallClock::new(999_999_999));
    let mut w = Worker::new(config(), Arc::new(InMemory::new()), wall.clone(), effects);
    w.admit(logs_pdata());
    assert_eq!(w.active.data.window_start_secs, 0);
    assert!(!w.rotation_requested);
    wall.set(1_000_000_001);
    w.admit(logs_pdata());
    assert_eq!(w.active.tokens.len(), 1);
    assert!(w.pending.is_some());
    assert!(w.rotation_requested);
    assert!(!w.accept());
}
```

- [ ] **Step 2: Run the red window tests**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet busy_rotation_rearms_boundary_sleep
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet admission_time_assigns_exactly_one_window
```

Expected: missing module and the previous per-request rotation violates the second test.

- [ ] **Step 3: Bridge wall time to one persistent monotonic sleep**

`window.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! Wall-clock windows with engine-clock sleeps.
use std::{sync::Arc, time::Duration};
use otel_arrow_dfe_engine::clock;
use otel_arrow_dfe_series_lake::clock::{WallClock, WindowClock, WakeOutcome, nanos_to_secs};
pub(super) struct Window {
    pub wall: Arc<dyn WallClock>,
    pub clock: WindowClock,
    pub sleep: clock::Sleep,
}
impl Window {
    pub fn new(interval: Duration, wall: Arc<dyn WallClock>) -> Self {
        let now = nanos_to_secs(wall.now_unix_nanos());
        let clock = WindowClock::new(interval, now);
        let target = clock.next_boundary(now);
        let sleep = Self::arm(target, wall.now_unix_nanos());
        Self { wall, clock, sleep }
    }
    fn arm(target_secs: i64, now_nanos: i64) -> clock::Sleep {
        let delay = (i128::from(target_secs) * 1_000_000_000 - i128::from(now_nanos)).max(0);
        let nanos = u64::try_from(delay).unwrap_or(u64::MAX);
        clock::sleep_until(clock::now() + Duration::from_nanos(nanos))
    }
    pub fn wake(&mut self) -> bool {
        let nanos = self.wall.now_unix_nanos();
        let now = nanos_to_secs(nanos);
        let (rotate, target) = match self.clock.on_wake(now) {
            WakeOutcome::RotationRequested { .. } => (true, self.clock.next_boundary(now)),
            WakeOutcome::TooEarly { sleep_until } => (false, sleep_until),
        };
        self.sleep = Self::arm(target, nanos);
        rotate
    }
    pub fn admission_boundary(&mut self, admission_secs: i64) -> i64 {
        let boundary = self.clock.effective_boundary(admission_secs);
        if boundary > self.clock.last_boundary() {
            let _ = self.clock.on_wake(admission_secs);
            self.sleep = Self::arm(self.clock.next_boundary(admission_secs), self.wall.now_unix_nanos());
        }
        boundary
    }
}
```

Add `mod window;` and Worker field `pub window: super::window::Window`. Construct it before moving `wall` into Worker and add `window,` to the Self initializer:

```rust
let window = super::window::Window::new(cfg.window.interval, wall.clone());
```
 Replace `new_active`'s temporary WindowClock/start calculation with:

```rust
let secs = nanos_to_secs(self.wall.now_unix_nanos());
let start = self.window.admission_boundary(secs).max(self.active.data.window_start_secs);
```

In `offer`, replace the temporary clock/comparison with:

```rust
if self.window.admission_boundary(pending.admission_secs) > self.active.data.window_start_secs {
    assert!(self.pending.is_none(), "only one extraction slot");
    self.pending = Some(pending);
    self.rotation_requested = true;
    return;
}
```

Replace the successful admission's unconditional rotation with:

```rust
self.rotation_requested = self.active.data.bytes >= self.cfg.window.max_block_bytes
    || self.active.tokens.len() >= self.cfg.window.max_requests_per_block;
```

Insert this select branch after the shutdown deadline and before the flush result:

```rust
() = worker.window.sleep.as_mut(), if worker.deadline.is_none() => {
    if worker.window.wake() { worker.rotation_requested = true; }
    notify_turns = 0;
}
```

The shutdown deadline has highest priority to guarantee cancellation even under a ready boundary. Normal-operation priority then matches spec 7.1: boundary, flush completion, bounded notifications, rotation/resume, inbox. Do not use `start_periodic_timer`; it is cancelled before receiver draining. Do not reconstruct an elapsed sleep without re-arming it.

- [ ] **Step 4: Verify clock jumps and real window-driven output**

```bash
cd rust/otap-dataflow
cargo check -p otel-arrow-dfe-core-nodes --features series_parquet
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet series_parquet
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice -v
```

Expected: PASS. The E2E test now waits for a one-second test window; the documented manual config still uses 15 seconds. Empty boundary rotations update ACTIVE's window but create no files.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet
git commit -m "feat(series_parquet): rotate on aligned wall-clock windows

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 6: Metrics signals, atomic unsupported policy and content errors

**Files:**
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{worker.rs,tests.rs}`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`

**Interfaces:**
- Consumes: `Worker::prepare(OtapPdata) -> Result<Pending,(AckToken,Outcome)>`, core extraction of `MetricsNumber`/`MetricsHistogram`, `UnsupportedPolicy::{Reject,Drop}`, OTLP timestamps as converted `i64`.
- Produces: logs and metrics admission through the same state machine; traces always Refused; validation of every supplied attribute table, including dropped-point/exemplar attributes. This adapter preflight closes the current core's omission at `rust/otap-dataflow/crates/series-lake/src/extract/metrics.rs:368` without changing canonical identity or schemas.

- [ ] **Step 1: Add failing network tests for numbers, histograms, mixed drops and traces**

Add imports and functions to `test_e2e.py`, before the final guard:

```python
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2 as metrics_pb
from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2_grpc as metrics_rpc
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2 as trace_pb
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc as trace_rpc


def metric_request(request_id, unsupported=False):
    req = metrics_pb.ExportMetricsServiceRequest()
    resource = req.resource_metrics.add()
    resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
    scope = resource.scope_metrics.add()
    scope.scope.name = "series-e2e"
    number = scope.metrics.add(name="integer", unit="1")
    point = number.gauge.data_points.add(time_unix_nano=2**64 - 1, as_int=2**63 - 1)
    point.attributes.add(key="request.id").value.string_value = request_id
    hist = scope.metrics.add(name="histogram", unit="s")
    hist.histogram.aggregation_temporality = 2
    point = hist.histogram.data_points.add(time_unix_nano=1789960500000000000,
                                         count=3, sum=4.0)
    point.bucket_counts.extend([1, 2])
    point.explicit_bounds.append(1.0)
    point.attributes.add(key="request.id").value.string_value = request_id
    if unsupported:
        scope.metrics.add(name="summary").summary.data_points.add(count=1, sum=2.0)
    return req


class MetricsSlice(unittest.TestCase):
    # Scenario: a gauge INT64_MAX and histogram arrive in one real OTLP request.
    # Guarantees: integer precision, histogram shape and wrapped timestamp nullability survive Parquet.
    def test_number_and_histogram(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            metrics_rpc.MetricsServiceStub(engine.channel).Export(metric_request("metric-0"), timeout=20)
            with duckdb.connect() as db:
                pattern = str(engine.data / "v=1/signal=metrics/dataset=number/**/*.parquet")
                rows = db.execute("SELECT value_int, time_unix_nano FROM read_parquet(?)", [pattern]).fetchall()
                self.assertEqual(rows, [(2**63 - 1, None)])
                pattern = str(engine.data / "v=1/signal=metrics/dataset=histogram/**/*.parquet")
                rows = db.execute("SELECT count, bucket_counts, explicit_bounds FROM read_parquet(?)", [pattern]).fetchall()
                self.assertEqual(rows, [(3, [1, 2], [1.0])])
            engine.shutdown()

    # Scenario: supported points share a request with an unsupported summary.
    # Guarantees: reject is atomic, while drop returns OK only after supported rows are durable.
    def test_mixed_policy(self):
        for policy in ("reject", "drop"):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as directory:
                with Engine(directory, overrides={"unsupported": policy}) as engine:
                    call = metrics_rpc.MetricsServiceStub(engine.channel)
                    if policy == "reject":
                        with self.assertRaises(grpc.RpcError) as caught:
                            call.Export(metric_request("mixed", True), timeout=20)
                        self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
                        self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                    else:
                        call.Export(metric_request("mixed", True), timeout=20)
                        self.assertTrue(list(engine.data.glob("v=1/signal=metrics/dataset=number/**/*.parquet")))
                    engine.shutdown()

    # Scenario: a traces request arrives even with unsupported: drop.
    # Guarantees: traces are permanently refused and never produce lake files.
    def test_traces_are_refused(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory, overrides={"unsupported": "drop"}) as engine:
            req = trace_pb.ExportTraceServiceRequest()
            req.resource_spans.add().scope_spans.add().spans.add(name="unsupported")
            with self.assertRaises(grpc.RpcError) as caught:
                trace_rpc.TraceServiceStub(engine.channel).Export(req, timeout=20)
            self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
            self.assertEqual(list(engine.data.rglob("*.parquet")), [])
            engine.shutdown()

    # Scenario: invalid temporality, histogram shape/count or duplicate attributes arrive.
    # Guarantees: each request is refused atomically and subsequent valid requests still commit.
    def test_content_failures_do_not_stop_worker(self):
        cases = []
        temporal = metric_request("bad-temporality")
        temporal.resource_metrics[0].scope_metrics[0].metrics[1].histogram.aggregation_temporality = 0
        cases.append(temporal)
        shape = metric_request("bad-shape")
        shape.resource_metrics[0].scope_metrics[0].metrics[1].histogram.data_points[0].bucket_counts.append(1)
        cases.append(shape)
        count = metric_request("bad-count")
        count.resource_metrics[0].scope_metrics[0].metrics[1].histogram.data_points[0].count = 2**63
        cases.append(count)
        duplicate = metric_request("bad-attrs")
        attrs = duplicate.resource_metrics[0].scope_metrics[0].metrics[0].gauge.data_points[0].attributes
        attrs.add(key="request.id").value.string_value = "duplicate"
        cases.append(duplicate)
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            call = metrics_rpc.MetricsServiceStub(engine.channel)
            for req in cases:
                with self.assertRaises(grpc.RpcError) as caught:
                    call.Export(req, timeout=20)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
            self.assertEqual(list(engine.data.rglob("*.parquet")), [])
            call.Export(metric_request("valid-after-errors"), timeout=20)
            engine.shutdown()

    # Scenario: drop policy receives only an unsupported summary point.
    # Guarantees: zero-output requests receive OK without opening any Parquet file.
    def test_zero_output_drop_acks_without_files(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory, overrides={"unsupported": "drop"}) as engine:
            request = metric_request("unsupported-only", unsupported=True)
            metrics = request.resource_metrics[0].scope_metrics[0].metrics
            del metrics[:2]
            metrics_rpc.MetricsServiceStub(engine.channel).Export(request, timeout=5)
            self.assertEqual(list(engine.data.rglob("*.parquet")), [])
            engine.shutdown()

    # Scenario: logs exceed a row, extracted-output or input budget, or contain excessive nesting.
    # Guarantees: each limit rejects the whole request permanently and writes no partial data.
    def test_each_ingress_budget_refuses_atomically(self):
        cases = []
        large = log_request("x" * 4096)
        cases.append(({"max_request_bytes": "1KiB"}, large))
        cases.append(({"max_row_bytes": "1KiB"}, large))
        cases.append(({"max_extracted_bytes": "1KiB"}, large))
        deep = log_request("deep")
        value = deep.resource_logs[0].resource.attributes.add(key="nested").value
        for _ in range(40):
            value = value.array_value.values.add()
        value.string_value = "leaf"
        cases.append(({"max_nesting_depth": 8}, deep))
        for limits, request in cases:
            with self.subTest(limits=limits), tempfile.TemporaryDirectory() as directory:
                with Engine(directory, overrides={"ingress": limits}) as engine:
                    with self.assertRaises(grpc.RpcError) as caught:
                        engine.logs.Export(request, timeout=20)
                    self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
                    self.assertEqual(list(engine.data.rglob("*.parquet")), [])
                    engine.shutdown()

```

- [ ] **Step 2: Run red metrics tests**

```bash
cd rust/otap-dataflow
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py MetricsSlice -v
```

Expected: supported metric requests currently receive INVALID_ARGUMENT.

- [ ] **Step 3: Admit metrics and validate all attribute content before extraction**

In `prepare`, replace the logs-only check with:

```rust
if payload.signal_type() == otel_arrow_dfe_config::SignalType::Traces {
    return Err((token, Outcome::Unsupported));
}
```

Add this function to `worker.rs` and call it on `&mut records` immediately before `lake::extract::extract`. It releases each validation table before constructing the next, bounding this temporary work by the converted request and depth limits.

```rust
fn validate_attributes(records: &mut OtapArrowRecords, cfg: &lake::config::LakeConfig) -> lake::Result<()> {
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType as P;
    records.decode_transport_optimized_ids().map_err(|e| lake::Error::Pdata(e.to_string()))?;
    for kind in [P::ResourceAttrs, P::ScopeAttrs, P::MetricAttrs, P::LogAttrs,
        P::NumberDpAttrs, P::HistogramDpAttrs, P::ExpHistogramDpAttrs, P::SummaryDpAttrs,
        P::NumberDpExemplarAttrs, P::HistogramDpExemplarAttrs, P::ExpHistogramDpExemplarAttrs] {
        if let Some(batch) = records.get(kind) {
            let _ = lake::attrs::AttrTable::from_batch(batch, cfg.ingress.max_nesting_depth)?;
        }
    }
    Ok(())
}
```

Retain `extract`'s own decode call; it is idempotent (`rust/otap-dataflow/crates/series-lake/src/extract/mod.rs:100`). The validation table reads duplicate keys and nested CBOR even for non-identity or dropped fields. Malformed parent IDs and schema/conversion failures become Invalid/Refused. Zero-output drops use the existing immediate Ack path; a mixed request retains its token until all supported values files commit.

- [ ] **Step 4: Verify logs, metrics and content rejection together**

```bash
cd rust/otap-dataflow
cargo check -p otel-arrow-dfe-core-nodes --features series_parquet
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet series_parquet
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice MetricsSlice -v
```

Expected: PASS with exact INT64_MAX, null wrapped timestamp and histogram lists. Extraction counters are preserved for task 7, rather than re-inferred from written Parquet.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py
git commit -m "feat(series_parquet): export metrics and refuse invalid requests atomically

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 7: Worker telemetry, labeled extraction outcomes and process memory residual

**Files:**
- Create: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/metrics.rs`
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{mod.rs,worker.rs,flush.rs,token.rs,tests.rs}`
- Modify: `rust/otap-dataflow/crates/series-lake/src/{buffer.rs,cache.rs,extract/mod.rs,extract/metrics.rs}`
- Modify: `rust/otap-dataflow/crates/engine/src/engine_metrics.rs`

**Interfaces:**
- Consumes: `FlushReport.files: Vec<(Dataset,Path,usize)>`, `ExtractStats`, `CacheStats`, engine `PipelineContext`, `MetricSet`/`MeasurementMetricSet`, generated registration and `MetricsReporter::report`.
- Produces: `Metrics::{register,report,snapshots}`, `Worker::sample_metrics`, `Worker.metrics: Option<Metrics>`, `Worker.accounting: SeriesMemoryAccounting`; bounded extraction maps; `SeriesCache::last_committed`; `Block::reserve_with_reemit`; `FlushDone.attempts: u64`.
- Metrics never use raw error messages, paths, request IDs, series IDs or producer IDs as labels. `column` is registration-time and bounded by the configured denormalized columns. `dataset` is a five-value signal-qualified enum, avoiding ambiguity between logs/series and metrics/series.
- `series_emitted{reason=new}` means absent/uncommitted in the bounded cache, `partition` means a cached different partition, and `rotation` means an explicit same-window byte/request rotation. Eviction can turn a historical series into `new`; the label is not a global cardinality oracle.

- [ ] **Step 1: Add red tests for required metric names and two-worker accounting**

In `engine_metrics.rs` tests:

```rust
/// Scenario: two exporter workers account memory and one exits.
/// Guarantees: the process total removes the exited worker and never subtracts another worker twice.
#[test]
fn series_accounting_releases_each_worker_once() {
    let baseline = SERIES_ACCOUNTED_BYTES.load(std::sync::atomic::Ordering::Relaxed);
    let mut a = SeriesMemoryAccounting::default();
    let mut b = SeriesMemoryAccounting::default();
    a.set(128);
    b.set(256);
    a.set(192);
    assert_eq!(SERIES_ACCOUNTED_BYTES.load(std::sync::atomic::Ordering::Relaxed), baseline + 448);
    drop(a);
    assert_eq!(SERIES_ACCOUNTED_BYTES.load(std::sync::atomic::Ordering::Relaxed), baseline + 256);
    b.set(0);
    assert_eq!(SERIES_ACCOUNTED_BYTES.load(std::sync::atomic::Ordering::Relaxed), baseline);
    drop(b);
    assert_eq!(SERIES_ACCOUNTED_BYTES.load(std::sync::atomic::Ordering::Relaxed), baseline);
}
```

In `series_parquet/tests.rs`:

```rust
/// Scenario: telemetry is collected after admitting a request and while a notification waits.
/// Guarantees: gauges include live requests and all required worker instruments have stable names.
#[tokio::test(flavor = "current_thread")]
async fn worker_metrics_cover_live_memory_and_requests() {
    use otel_arrow_dfe_telemetry::metrics::MetricSetHandler;
    let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
    let (effects, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut w = Worker::new(config(), Arc::new(InMemory::new()), wall, effects);
    w.metrics = Some(super::metrics::Metrics::register(&context, &w.cfg.lake));
    w.admit(logs_pdata());
    w.sample_metrics();
    let m = w.metrics.as_ref().expect("registered");
    assert_eq!(m.worker.requests_pending.get(), 1);
    assert!(m.worker.memory_accounted_bytes.get() >= w.active.data.bytes as u64);
    assert!(m.worker.memory_budget_bytes.get() >= m.worker.memory_accounted_bytes.get());
    let snapshots = m.worker.snapshot_values();
    assert!(!snapshots.is_empty());
}
```

Also add this core regression test inside `buffer.rs`'s existing tests module, using its local `extracted` fixture:

```rust
/// Scenario: a byte rotation begins another block in a partition with a committed descriptor.
/// Guarantees: explicit re-emission reserves the descriptor again without disabling the cache globally.
#[test]
fn byte_rotation_can_force_descriptor_reemission() {
    let cfg = LakeConfig::default();
    let mut cache = SeriesCache::new(10);
    let e = extracted(&cfg, "h", 1);
    let id = e.descriptors[0].series_id;
    let block: Block<()> = Block::new(0, 1, &cfg);
    cache.mark_committed(id, block.partition);
    let normal = block.reserve(&e, &mut cache, 16, &cfg).expect("reserve");
    assert!(normal.new_series.is_empty());
    let forced = block.reserve_with_reemit(&e, &mut cache, 16, &cfg, true).expect("reserve");
    assert_eq!(forced.new_series, vec![0]);
    assert!(forced.bytes > normal.bytes);
}
```

- [ ] **Step 2: Run red telemetry tests**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-engine series_accounting_releases_each_worker_once -- --test-threads=1
cargo test -p otel-arrow-dfe-series-lake byte_rotation_can_force_descriptor_reemission
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet worker_metrics_cover_live_memory_and_requests
```

Expected: missing support methods and instruments. Use the exact `Gauge::get`/`MetricSetHandler::snapshot_values` interfaces from the telemetry crate; this test checks recorded values, not just a text list.

- [ ] **Step 3: Add bounded details and rotation-aware reservation to the core**

Add these fields to `ExtractStats`, preserving existing aggregate fields/tests:

```rust
/// Dropped exponential histogram points.
pub dropped_exp_histogram: u64,
/// Dropped summary points.
pub dropped_summary: u64,
/// Mismatches keyed only by configured physical column name.
pub denorm_type_mismatch_by_column: std::collections::BTreeMap<String, u64>,
```

In `denorm_lookup`'s mismatch arm, immediately after incrementing the aggregate:

```rust
*stats.denorm_type_mismatch_by_column.entry(d.column.clone()).or_default() += 1;
```

Replace the Drop arm at `rust/otap-dataflow/crates/series-lake/src/extract/metrics.rs:363`:

```rust
UnsupportedPolicy::Drop => {
    let rows = b.num_rows() as u64;
    c.stats.dropped_unsupported += rows;
    match pt {
        ArrowPayloadType::ExpHistogramDataPoints => c.stats.dropped_exp_histogram += rows,
        ArrowPayloadType::SummaryDataPoints => c.stats.dropped_summary += rows,
        _ => unreachable!("loop contains only the two unsupported point tables"),
    }
}
```

Add this cache method (read-only, no LRU/stat changes):

```rust
/// Last durable descriptor partition, without changing recency or hit counters.
#[must_use]
pub fn last_committed(&self, id: &SeriesId) -> Option<PartitionId> {
    self.inner.peek(id).copied().flatten()
}
```

Replace `Block::reserve` with a delegating wrapper and the complete generalized body. Existing callers keep identical behavior:

```rust
/// Reserve a request using normal partition-cache suppression.
pub fn reserve(&self, extracted: &Extracted, cache: &mut SeriesCache,
    token_bytes: usize, cfg: &LakeConfig) -> Result<Reservation> {
    self.reserve_with_reemit(extracted, cache, token_bytes, cfg, false)
}
/// Reserve before mutation, optionally repeating descriptors for a same-window rotation.
pub fn reserve_with_reemit(&self, extracted: &Extracted, cache: &mut SeriesCache,
    token_bytes: usize, cfg: &LakeConfig, reemit: bool) -> Result<Reservation> {
    let limits = &cfg.ingress;
    let mut bytes = extracted.pinned_bytes + token_bytes;
    let mut new_series = Vec::new();
    for (i, d) in extracted.descriptors.iter().enumerate() {
        let committed_here = cache.is_committed(&d.series_id, self.partition);
        if (reemit || !committed_here) && !self.pending_series.contains(&d.series_id) {
            new_series.push(i);
            bytes += d.approx_bytes + limits.pending_series_entry_bytes;
        }
    }
    if bytes > limits.max_block_bytes { return Err(Error::Refused(RefuseReason::RequestTooLarge)); }
    if self.requests.len() >= limits.max_requests_per_block {
        return Err(Error::Refused(RefuseReason::TooManyRequests));
    }
    if self.bytes + bytes > limits.max_block_bytes { return Err(Error::Refused(RefuseReason::BlockFull)); }
    for d in &extracted.descriptors { cache.touch(d.series_id); }
    Ok(Reservation { bytes, token_bytes, new_series })
}
```

This small additive core API is necessary to implement the spec 7.4 byte-rotation statement literally; normal `reserve` suppresses committed same-partition descriptors. Coordinate with plan 1's owner rather than forking or duplicating its reservation arithmetic.

- [ ] **Step 4: Declare every worker instrument and its closed labels**

`metrics.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
//! Bounded operational metrics for one series exporter worker.
use std::collections::BTreeMap;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_telemetry::instrument::{Counter, Gauge, Mmsc, ObserveCounter};
use otel_arrow_dfe_telemetry::metrics::{MetricSet, MeasurementMetricSet, MetricSetSnapshot};
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::{AttributeEnum, attribute_set, metric_set};
use otel_arrow_dfe_series_lake::{config::LakeConfig, extract::ExtractStats, schema::Dataset};

#[metric_set(name = "exporter.series_parquet")]
#[derive(Debug, Default, Clone)]
pub(super) struct WorkerMetrics {
    #[metric(name = "series_cache.entries", unit = "{entry}")] pub cache_entries: Gauge<u64>,
    #[metric(name = "series_cache.hits", unit = "{lookup}")] pub cache_hits: ObserveCounter<u64>,
    #[metric(name = "series_cache.misses", unit = "{lookup}")] pub cache_misses: ObserveCounter<u64>,
    #[metric(name = "series_cache.evictions", unit = "{entry}")] pub cache_evictions: ObserveCounter<u64>,
    #[metric(name = "block.active_bytes", unit = "By")] pub active_bytes: Gauge<u64>,
    #[metric(name = "block.flushing_bytes", unit = "By")] pub flushing_bytes: Gauge<u64>,
    #[metric(name = "block.requests_pending", unit = "{request}")] pub requests_pending: Gauge<u64>,
    #[metric(name = "block.pending_slot_occupied", unit = "{slot}")] pub pending_slot: Gauge<u64>,
    #[metric(name = "flush.duration", unit = "s")] pub flush_duration: Mmsc,
    #[metric(name = "flush.failures", unit = "{flush}")] pub flush_failures: Counter<u64>,
    #[metric(name = "flush.retries", unit = "{attempt}")] pub flush_retries: Counter<u64>,
    #[metric(name = "flush.cancelled", unit = "{flush}")] pub flush_cancelled: Counter<u64>,
    #[metric(unit = "{request}")] pub acks: ObserveCounter<u64>,
    #[metric(name = "notify.queued", unit = "{request}")] pub notify_queued: Gauge<u64>,
    #[metric(name = "notify.failures", unit = "{request}")] pub notify_failures: ObserveCounter<u64>,
    #[metric(name = "oldest_unacked_seconds", unit = "s")] pub oldest: Gauge<f64>,
    #[metric(name = "timestamp.out_of_range", unit = "{timestamp}")] pub timestamp_out_of_range: Counter<u64>,
    #[metric(name = "memory.budget_bytes", unit = "By")] pub memory_budget_bytes: Gauge<u64>,
    #[metric(name = "memory.accounted_bytes", unit = "By")] pub memory_accounted_bytes: Gauge<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum FlushReason { Time, Bytes, Requests, Shutdown }
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct FlushAttrs { pub reason: FlushReason }
#[metric_set(name = "exporter.series_parquet", measurement_attributes = FlushAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct FlushMetrics {
    #[metric(name = "flush.count", unit = "{flush}")] pub count: Counter<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum NackReason { Storage, TooLarge, Invalid, Unsupported, Shutdown }
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct NackAttrs { pub reason: NackReason }
#[metric_set(name = "exporter.series_parquet", measurement_attributes = NackAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct NackMetrics {
    #[metric(unit = "{request}")] pub nacks: ObserveCounter<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum DatasetLabel { LogsSeries, LogsValues, MetricsSeries, MetricsNumber, MetricsHistogram }
impl From<Dataset> for DatasetLabel {
    fn from(ds: Dataset) -> Self {
        match ds { Dataset::LogsSeries => Self::LogsSeries, Dataset::LogsValues => Self::LogsValues,
            Dataset::MetricsSeries => Self::MetricsSeries, Dataset::MetricsNumber => Self::MetricsNumber,
            Dataset::MetricsHistogram => Self::MetricsHistogram }
    }
}
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct DatasetAttrs { pub dataset: DatasetLabel }
#[metric_set(name = "exporter.series_parquet", measurement_attributes = DatasetAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct WrittenMetrics {
    #[metric(unit = "{row}")] pub rows_written: Counter<u64>,
    #[metric(unit = "{file}")] pub files_written: Counter<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum EmitReason { New, Partition, Rotation }
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct EmitAttrs { pub reason: EmitReason }
#[metric_set(name = "exporter.series_parquet", measurement_attributes = EmitAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct EmittedMetrics {
    #[metric(unit = "{row}")] pub series_emitted: Counter<u64>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, AttributeEnum)]
pub(super) enum DroppedKind { ExpHistogram, Summary, Exemplar }
#[attribute_set(item, measurement)]
#[derive(Debug, Clone, Copy)]
pub(super) struct DroppedAttrs { pub kind: DroppedKind }
#[metric_set(name = "exporter.series_parquet", measurement_attributes = DroppedAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct DroppedMetrics {
    #[metric(unit = "{row}")] pub dropped_unsupported: Counter<u64>,
}
#[attribute_set(item, registration)]
#[derive(Debug, Clone)]
pub(super) struct ColumnAttrs { pub column: String }
#[metric_set(name = "exporter.series_parquet", registration_attributes = ColumnAttrs)]
#[derive(Debug, Default, Clone)]
pub(super) struct ColumnMetrics {
    #[metric(name = "denormalize.type_mismatch", unit = "{value}")] pub mismatch: Counter<u64>,
}
pub(super) struct Metrics {
    pub worker: MetricSet<WorkerMetrics>,
    pub flush: MeasurementMetricSet<FlushMetrics>,
    pub nacks: MeasurementMetricSet<NackMetrics>,
    pub written: MeasurementMetricSet<WrittenMetrics>,
    pub emitted: MeasurementMetricSet<EmittedMetrics>,
    pub dropped: MeasurementMetricSet<DroppedMetrics>,
    columns: BTreeMap<String, MetricSet<ColumnMetrics>>,
}
impl Metrics {
    pub fn register(ctx: &PipelineContext, cfg: &LakeConfig) -> Self {
        let mut columns = BTreeMap::new();
        for d in cfg.logs.denormalize.iter().chain(cfg.metrics.denormalize.iter()) {
            if !columns.contains_key(&d.column) {
                let _ = columns.insert(d.column.clone(), ColumnMetrics::register(ctx, &ColumnAttrs { column: d.column.clone() }));
            }
        }
        Self { worker: WorkerMetrics::register(ctx), flush: FlushMetrics::register(ctx),
            nacks: NackMetrics::register(ctx), written: WrittenMetrics::register(ctx),
            emitted: EmittedMetrics::register(ctx), dropped: DroppedMetrics::register(ctx), columns }
    }
    pub fn extracted(&mut self, stats: &ExtractStats) {
        self.worker.timestamp_out_of_range.add(stats.timestamp_out_of_range);
        for (kind, count) in [(DroppedKind::ExpHistogram, stats.dropped_exp_histogram),
            (DroppedKind::Summary, stats.dropped_summary), (DroppedKind::Exemplar, stats.dropped_exemplars)] {
            if count != 0 { self.dropped.with(DroppedAttrs { kind }).dropped_unsupported.add(count); }
        }
        for (column, count) in &stats.denorm_type_mismatch_by_column {
            if let Some(metric) = self.columns.get_mut(column) { metric.mismatch.add(*count); }
        }
    }
    pub fn report(&mut self, reporter: &mut MetricsReporter) {
        let _ = reporter.report(&mut self.worker);
        let _ = reporter.report_measurement(&mut self.flush);
        let _ = reporter.report_measurement(&mut self.nacks);
        let _ = reporter.report_measurement(&mut self.written);
        let _ = reporter.report_measurement(&mut self.emitted);
        let _ = reporter.report_measurement(&mut self.dropped);
        for metric in self.columns.values_mut() { let _ = reporter.report(metric); }
    }
    pub fn snapshots(&mut self) -> Vec<MetricSetSnapshot> {
        let mut out = self.worker.terminal_snapshots();
        out.extend(self.flush.terminal_snapshots());
        out.extend(self.nacks.terminal_snapshots());
        out.extend(self.written.terminal_snapshots());
        out.extend(self.emitted.terminal_snapshots());
        out.extend(self.dropped.terminal_snapshots());
        for metric in self.columns.values_mut() { out.extend(metric.terminal_snapshots()); }
        out
    }
}
```

- [ ] **Step 5: Wire accounting and lifecycle instrumentation**

Add these fields to Notifier: `pub outcomes: [u64; 6]`, `pub failures: u64`, and `pub token_high_water: usize`, initialized to zero. Give Outcome `#[repr(usize)]` (the existing order is Ack, TooLarge, Invalid, Unsupported, Storage, Shutdown). In `push`, before moving the token, execute `self.token_high_water = self.token_high_water.max(token.bytes());` and `self.outcomes[outcome as usize] += 1;`. In `next`, before returning its result, execute `if result.is_err() { self.failures += 1; }`. Thus acks/nacks count decisions, notification failures count delivery errors, and committed-but-unnotified remains observable.

Add `pub metrics: Option<super::metrics::Metrics>`, initialized to None, and `pub accounting: otel_arrow_dfe_engine::engine_metrics::SeriesMemoryAccounting`, initialized with Default, to Worker. Add `pub reason: super::metrics::FlushReason` initialized `super::metrics::FlushReason::Time` and `pub token_high_water: usize` initialized `std::mem::size_of::<AckToken>()`. Add `pub reemit: bool` initialized false to OwnedBlock; `new_active` sets it when the new start equals the previous start and `reason` is Bytes or Requests. In `offer`, replace the reservation call with `self.active.data.reserve_with_reemit(&pending.extracted, &mut self.cache, pending.token.bytes(), &self.cfg.lake, self.active.reemit)`.

Replace `Worker::admit` with this implementation. It records extraction outcomes once; resuming the pending slot never increments them again.

```rust
pub fn admit(&mut self, data: OtapPdata) {
    match self.prepare(data) {
        Ok(pending) => {
            self.token_high_water = self.token_high_water.max(pending.token.bytes());
            if let Some(metrics) = &mut self.metrics { metrics.extracted(&pending.extracted.stats); }
            self.offer(pending);
        }
        Err((token, outcome)) => {
            self.token_high_water = self.token_high_water.max(token.bytes());
            self.notify.push(token, outcome);
        }
    }
}
```

In `new_active`, replace its final OwnedBlock expression with:

```rust
let reemit = start == self.active.data.window_start_secs
    && matches!(self.reason, super::metrics::FlushReason::Bytes | super::metrics::FlushReason::Requests);
OwnedBlock { data: Block::new(start, seq, &self.cfg.lake), tokens: vec![], reemit }
```

For each successful reservation's `new_series`, determine the bounded reason before `admit` and record it only after successful admission:

```rust
let emission_reasons: Vec<_> = reservation.new_series.iter().map(|&index| {
    let id = pending.extracted.descriptors[index].series_id;
    if self.active.reemit { super::metrics::EmitReason::Rotation }
    else if self.cache.last_committed(&id).is_some() { super::metrics::EmitReason::Partition }
    else { super::metrics::EmitReason::New }
}).collect();
```

In the successful `admit` arm after pushing the token:

```rust
if let Some(metrics) = &mut self.metrics {
    for reason in emission_reasons {
        metrics.emitted.with(super::metrics::EmitAttrs { reason }).series_emitted.add(1);
    }
}
```

Replace the combined BlockFull/TooManyRequests reservation arm in `offer` with:

```rust
Err(lake::Error::Refused(reason @ (lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests))) => {
    self.reason = match reason {
        lake::RefuseReason::BlockFull => super::metrics::FlushReason::Bytes,
        _ => super::metrics::FlushReason::Requests,
    };
    assert!(self.pending.is_none(), "only one extraction slot");
    self.pending = Some(pending);
    self.rotation_requested = true;
}
```

Before requesting rotation in `offer`'s advanced-boundary branch insert `self.reason = super::metrics::FlushReason::Time;`. In `shutdown`, insert `self.reason = super::metrics::FlushReason::Shutdown;`. Replace the successful window-wake branch with:

```rust
if worker.window.wake() {
    worker.reason = metrics::FlushReason::Time;
    worker.rotation_requested = true;
}
```

Set the reason for threshold rotation after successful admission as follows:

```rust
if self.active.data.bytes >= self.cfg.window.max_block_bytes {
    self.reason = super::metrics::FlushReason::Bytes;
} else if self.active.tokens.len() >= self.cfg.window.max_requests_per_block {
    self.reason = super::metrics::FlushReason::Requests;
}
```

Immediately before installing a real non-empty FlushJob, insert:

```rust
if let Some(metrics) = &mut self.metrics {
    metrics.flush.with(super::metrics::FlushAttrs { reason: self.reason }).count.add(1);
}
```

Empty rotations do not count as flushes. Replace `rotate`'s seal-error branch with:

```rust
if let Err(error) = self.active.data.seal(nanos_to_micros(self.wall.now_unix_nanos())) {
    if let Some(metrics) = &mut self.metrics { metrics.worker.flush_failures.add(1); }
    otel_error!("series_parquet.seal_failed", error = %error);
    self.fail_active(Outcome::Storage);
    return;
}
```

Add the component scope to `worker.rs` in this task:

```rust
otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = super::SERIES_PARQUET_URN, target = "otel.exporter.series_parquet",
);
```

 Add `attempts: 1` to the FlushDone initializer and `pub attempts: u64` to its type. Replace Worker::complete with:

```rust
pub fn complete(&mut self, done: Result<FlushDone, tokio::task::JoinError>) {
    use super::metrics::DatasetAttrs;
    let mut job = self.flushing.take().expect("selected completion has a job");
    if let Some(m) = &mut self.metrics { m.worker.flush_duration.record(clock::now().duration_since(job.started).as_secs_f64()); }
    let outcome = match done {
        Ok(done) => {
            if let Some(m) = &mut self.metrics { m.worker.flush_retries.add(done.attempts.saturating_sub(1)); }
            match done.result {
                Ok(report) => {
                    for id in &done.data.pending_series { self.cache.mark_committed(*id, done.data.partition); }
                    if let Some(m) = &mut self.metrics {
                        for (dataset, _, rows) in report.files {
                            let bucket = m.written.with(DatasetAttrs { dataset: dataset.into() });
                            bucket.rows_written.add(rows as u64);
                            bucket.files_written.add(1);
                        }
                    }
                    Outcome::Ack
                }
                Err(error) => {
                    if let Some(m) = &mut self.metrics {
                        m.worker.flush_failures.add(1);
                        if matches!(error, lake::Error::Cancelled { .. }) { m.worker.flush_cancelled.add(1); }
                    }
                    if self.deadline.is_some_and(|d| clock::now() >= d) { Outcome::Shutdown } else { Outcome::Storage }
                }
            }
        }
        Err(_) => {
            if let Some(m) = &mut self.metrics { m.worker.flush_failures.add(1); }
            Outcome::Storage
        }
    };
    for token in std::mem::take(&mut job.tokens) { self.notify.push(token, outcome); }
}
```

Add this sampling function. Workspace allowances are engineering reservations, not byte-exact measurements. Q6 accepts block-sized series sealing for v1 (plan 1 F-park; FORMAT.md limitations): `descriptor_seal = B` is included in `memory.budget_bytes`, and synchronous seal work also grows with descriptor volume. This is not a fixed-workspace bound. Q5 leaves allowance validation to plan 3; plan 2 reports the once-process RSS residual.

```rust
pub fn sample_metrics(&mut self) {
    use super::metrics::{NackAttrs, NackReason};
    let flushing = self.flushing.as_ref().map_or(0, |f| f.bytes);
    let pending = self.pending.as_ref().map_or(0, |p| p.extracted.pinned_bytes
        + p.extracted.descriptors.iter().map(|d| d.approx_bytes).sum::<usize>() + p.token.bytes());
    let token = self.token_high_water.max(self.notify.token_high_water) as u64;
    let cfg = &self.cfg.lake;
    let cache = self.cache.len() as u64 * 128;
    let sort = 2 * cfg.sorting.run_target_bytes as u64;
    let merge = 2 * cfg.sorting.merge_chunk_bytes as u64;
    let writer = 3 * cfg.parquet.writer_limit_bytes as u64;
    let upload = cfg.upload.part_bytes as u64 * (cfg.upload.concurrency as u64 + 1)
        + cfg.sorting.merge_chunk_bytes as u64;
    let conversion = 4 * cfg.ingress.max_request_bytes as u64;
    let fixed = 64 * 1024 * 1024_u64;
    // Current core sealing retains all descriptors beside their materialized batches.
    let descriptor_seal = cfg.ingress.max_block_bytes as u64;
    let workspace = sort + merge + writer + upload + conversion + fixed + descriptor_seal;
    let spare_tokens = self.active.tokens.capacity().saturating_sub(self.active.tokens.len())
        + self.flushing.as_ref().map_or(0, |f| f.tokens.capacity().saturating_sub(f.tokens.len()));
    let accounted = self.active.data.bytes as u64 + flushing as u64 + pending as u64
        + cache + self.notify.bytes() as u64
        + (spare_tokens * std::mem::size_of::<AckToken>()) as u64;
    let budget = 2 * cfg.ingress.max_block_bytes as u64 + cfg.ingress.max_extracted_bytes as u64
        + self.cfg.cache_entries as u64 * 128 + 2 * cfg.ingress.max_requests_per_block as u64 * token
        + workspace;
    self.accounting.set(accounted);
    let oldest = self.active.tokens.iter().map(|t| t.received)
        .chain(self.flushing.iter().flat_map(|f| f.tokens.iter().map(|t| t.received)))
        .chain(self.pending.iter().map(|p| p.token.received))
        .chain(self.notify.oldest()).min();
    let requests = self.live_tokens() as u64;
    if let Some(m) = &mut self.metrics {
        let stats = self.cache.stats();
        m.worker.cache_entries.set(self.cache.len() as u64);
        m.worker.cache_hits.observe(stats.hits);
        m.worker.cache_misses.observe(stats.misses);
        m.worker.cache_evictions.observe(stats.evictions);
        m.worker.active_bytes.set(self.active.data.bytes as u64);
        m.worker.flushing_bytes.set(flushing as u64);
        m.worker.requests_pending.set(requests);
        m.worker.pending_slot.set(u64::from(self.pending.is_some()));
        m.worker.notify_queued.set(self.notify.len() as u64);
        m.worker.notify_failures.observe(self.notify.failures);
        m.worker.acks.observe(self.notify.outcomes[Outcome::Ack as usize]);
        for (outcome, reason) in [(Outcome::Storage, NackReason::Storage),
            (Outcome::TooLarge, NackReason::TooLarge), (Outcome::Invalid, NackReason::Invalid),
            (Outcome::Unsupported, NackReason::Unsupported), (Outcome::Shutdown, NackReason::Shutdown)] {
            m.nacks.with(NackAttrs { reason }).nacks.observe(self.notify.outcomes[outcome as usize]);
        }
        m.worker.oldest.set(oldest.map_or(0.0, |t| clock::now().duration_since(t).as_secs_f64()));
        m.worker.memory_accounted_bytes.set(accounted);
        m.worker.memory_budget_bytes.set(budget);
    }
}
```

In `mod.rs`, add `mod metrics;` and `metrics: Option<metrics::Metrics>` to SeriesParquet. Replace its constructor with:

```rust
pub fn new(config: config::Config) -> Self {
    Self { config, token_provider: None, metrics: None }
}
```

Rename the factory closure's `_pipeline` parameter to `pipeline`, and insert immediately after constructing the exporter:

```rust
exporter.metrics = Some(metrics::Metrics::register(&pipeline, &exporter.config.lake));
```

Replace the start delegation with:

```rust
run(self.config.clone(), store, Arc::new(lake::clock::SystemWallClock),
    inbox, effects, self.metrics.take()).await
```

Add `metrics: Option<metrics::Metrics>` as `run`'s last parameter. Immediately after its Worker construction insert `worker.metrics = metrics;`. Tests invoking Worker directly keep None. At the top of each loop call `worker.sample_metrics()`. Handle CollectTelemetry explicitly:

```rust
Message::Control(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
    worker.sample_metrics();
    if let Some(m) = &mut worker.metrics { m.report(&mut metrics_reporter); }
}
```

At each TerminalState return, replace `[]` with `worker.metrics.as_mut().map_or_else(Vec::new, metrics::Metrics::snapshots)` after sampling. Do not return a bare state that loses the last counters.

- [ ] **Step 6: Report process RSS residual once from the existing engine monitor**

This is the only engine change in task 7: additive worker accounting and reporting through the existing monitor. Keep existing monitor scheduling, routing and shutdown behavior. Its own `series_accounting_releases_each_worker_once` regression is introduced in step 1 and run in steps 2/7; no unrelated engine refactor is authorized.

Add to `engine_metrics.rs`:

```rust
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
static SERIES_ACCOUNTED_BYTES: AtomicU64 = AtomicU64::new(0);
static SERIES_REPORTER_OWNED: AtomicBool = AtomicBool::new(false);
/// One worker's contribution to process-wide series exporter memory accounting.
#[derive(Default)]
pub struct SeriesMemoryAccounting { bytes: u64 }
impl SeriesMemoryAccounting {
    /// Replace this worker's accounted bytes without disturbing other workers.
    pub fn set(&mut self, bytes: u64) {
        if bytes >= self.bytes { let _ = SERIES_ACCOUNTED_BYTES.fetch_add(bytes - self.bytes, Ordering::Relaxed); }
        else { let _ = SERIES_ACCOUNTED_BYTES.fetch_sub(self.bytes - bytes, Ordering::Relaxed); }
        self.bytes = bytes;
    }
}
impl Drop for SeriesMemoryAccounting { fn drop(&mut self) { self.set(0); } }
/// Process-scoped exporter residual; the engine monitor is its single reporter.
#[metric_set(name = "exporter.series_parquet")]
#[derive(Debug, Default, Clone)]
pub struct SeriesProcessMetrics {
    /// RSS minus the accounted sum, clamped to zero.
    #[metric(name = "memory.unaccounted_rss_bytes", unit = "By")]
    pub residual: Gauge<u64>,
}
```

Add monitor fields `series: Option<MetricSet<SeriesProcessMetrics>>` and `series_entity: EntityKey`, initialized None and the constructor's entity_key. At the start of `update`:

```rust
if self.series.is_none() && SERIES_REPORTER_OWNED.compare_exchange(false, true,
    Ordering::AcqRel, Ordering::Acquire).is_ok() {
    self.series = Some(self.registry.register_metric_set_for_entity::<SeriesProcessMetrics>(self.series_entity));
}
if let Some(series) = &mut self.series {
    series.residual.set(get_rss_bytes().saturating_sub(SERIES_ACCOUNTED_BYTES.load(Ordering::Relaxed)));
}
```

The engine monitor uses its existing internal registry registration mechanism; worker code uses generated `register(&PipelineContext)` as required by the item-attributes guide. In `report` report the optional series metric first; in `finish_reporting_until` call `report_reliably_until` for it before the existing flush barrier; in Drop unregister its key and release the process reporter flag:

```rust
// At the beginning of report:
if let Some(series) = &mut self.series { self.reporter.report(series)?; }
// Before flush_until in finish_reporting_until:
if let Some(series) = &mut self.series {
    let _ = self.reporter.report_reliably_until(series, deadline).await?;
}
// At the beginning of Drop::drop:
if let Some(series) = self.series.take() {
    let _ = self.registry.unregister_metric_set(series.metric_set_key());
    SERIES_REPORTER_OWNED.store(false, Ordering::Release);
}
```

This reports on the engine entity, not a worker entity, and reuses the memory-limiter sampling source. A surviving second monitor can acquire the reporter on its next update. No exporter-to-engine dependency cycle is introduced.

- [ ] **Step 7: Verify all instruments and commit**

```bash
cd rust/otap-dataflow
cargo check -p otel-arrow-dfe-series-lake -p otel-arrow-dfe-engine -p otel-arrow-dfe-core-nodes --features otel-arrow-dfe-core-nodes/series_parquet
cargo test -p otel-arrow-dfe-series-lake byte_rotation_can_force_descriptor_reemission
cargo test -p otel-arrow-dfe-engine series_accounting_releases_each_worker_once -- --test-threads=1
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet worker_metrics_cover_live_memory_and_requests
cd ../..
git add rust/otap-dataflow/crates/series-lake/src/buffer.rs rust/otap-dataflow/crates/series-lake/src/cache.rs rust/otap-dataflow/crates/series-lake/src/extract/mod.rs rust/otap-dataflow/crates/series-lake/src/extract/metrics.rs rust/otap-dataflow/crates/engine/src/engine_metrics.rs rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet
git commit -m "feat(series_parquet): expose bounded lifecycle and memory telemetry

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 8: Absolute flush retry deadlines and cache correctness across failures

**Files:**
- Modify: `rust/otap-dataflow/crates/core-nodes/Cargo.toml`
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{flush.rs,worker.rs,tests.rs}`

**Interfaces:**
- Consumes: sealed `Block<()>`, `Rc<Sink>`, `CancellationToken`, `FlushDone`, `FlushJob`, `Config.window.flush_retry_deadline`; `Error::{ObjectStore,Parquet,AbortFailed,Cancelled}`.
- Produces: `FlushJob::new(Block<()>, Vec<AckToken>, Rc<Sink>, Duration) -> Self`, `retryable(&lake::Error) -> bool`, `write_until(&Sink, &Block<()>, &CancellationToken, Instant) -> (lake::Result<FlushReport>, u64)`. One absolute deadline starts at job creation and cancels an in-flight attempt. Per-operation retry settings remain inside the shared object-store backend.

- [ ] **Step 1: Add a real ObjectStore fault wrapper and red lifecycle tests**

Append this complete wrapper to `tests.rs`. It intercepts small PUTs and multipart initiation; task 9 additionally covers cancellation during an initiated upload via the core sink's existing tests. No exporter test claims this wrapper is network E2E.

```rust
use std::sync::{Mutex, atomic::{AtomicU8, Ordering}};
use futures::stream::BoxStream;
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path};
#[derive(Debug, Default)]
struct FaultStore {
    inner: InMemory,
    mode: AtomicU8,
    writes: Mutex<Vec<(String, Vec<u8>)>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("series fault store") }
}
impl FaultStore {
    async fn before(&self, path: &Path) -> object_store::Result<()> {
        self.entered.notify_one();
        let mode = self.mode.load(Ordering::SeqCst);
        if mode == 3 { self.release.notified().await; }
        let path = path.as_ref();
        let fail = mode == 4 || (mode == 1 && path.contains("dataset=series/"))
            || (mode == 2 && path.contains("dataset=values/")
                && self.mode.compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst).is_ok());
        if fail {
            return Err(object_store::Error::Generic { store: "series-test",
                source: Box::new(std::io::Error::other("injected store failure")) });
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(&self, path: &Path, payload: PutPayload, options: PutOptions) -> object_store::Result<PutResult> {
        let bytes = payload.iter().flat_map(|b| b.iter().copied()).collect();
        self.writes.lock().expect("writes lock").push((path.to_string(), bytes));
        self.before(path).await?;
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(&self, path: &Path, options: PutMultipartOptions)
        -> object_store::Result<Box<dyn MultipartUpload>> {
        self.before(path).await?;
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(&self, paths: BoxStream<'static, object_store::Result<Path>>)
        -> BoxStream<'static, object_store::Result<Path>> { self.inner.delete_stream(paths) }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
/// Scenario: series succeeds, the first values PUT fails, then the store recovers.
/// Guarantees: retries use byte-identical objects at frozen names and produce exactly one ack.
#[tokio::test(flavor = "current_thread")]
async fn values_retry_reuses_paths_and_bytes() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(2, Ordering::SeqCst);
        let (effects, mut rx) = effects(4);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(config(), store.clone(), wall, effects);
        w.admit(logs_pdata());
        w.rotate();
        let done = w.flushing.as_mut().expect("flush").finish().await;
        assert_eq!(done.as_ref().expect("join").attempts, 2);
        w.complete(done);
        w.notify.next().await.expect("ack send");
        assert!(matches!(rx.recv().await.expect("ack"), PipelineCompletionMsg::DeliverAck { .. }));
        let writes = store.writes.lock().expect("writes lock");
        assert_eq!(writes.len(), 4);
        assert_eq!(writes[0], writes[2]);
        assert_eq!(writes[1], writes[3]);
    }).await;
}
/// Scenario: descriptor upload fails until the absolute deadline, then another request arrives.
/// Guarantees: failure nacks the block and the following request re-emits its descriptor.
#[tokio::test(flavor = "current_thread")]
async fn failed_descriptor_does_not_poison_cache() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(1, Ordering::SeqCst);
        let mut cfg = config();
        cfg.window.flush_retry_deadline = Duration::from_millis(30);
        let (effects, mut rx) = effects(4);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(cfg, store.clone(), wall, effects);
        w.admit(logs_pdata());
        let id = *w.active.data.pending_series.iter().next().expect("series");
        w.rotate();
        let done = w.flushing.as_mut().expect("flush").finish().await;
        w.complete(done);
        w.notify.next().await.expect("nack send");
        match rx.recv().await.expect("nack") {
            PipelineCompletionMsg::DeliverNack { nack } => assert!(!nack.permanent),
            other => panic!("expected retryable nack, got {other:?}"),
        }
        assert!(!w.cache.is_committed(&id, lake::clock::PartitionId::from_unix_secs(0)));
        store.mode.store(0, Ordering::SeqCst);
        w.admit(logs_pdata());
        assert!(w.active.data.pending_series.contains(&id));
        w.rotate();
        let done = w.flushing.as_mut().expect("flush").finish().await;
        w.complete(done);
        assert_eq!(w.notify.len(), 1);
    }).await;
}
/// Scenario: a flush spans an hour, the same series enters ACTIVE, and cache entries are evicted.
/// Guarantees: commit uses the old partition and cannot remove the new partition's pending descriptor.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_series_and_eviction_preserve_partition_coverage() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        let (effects, _rx) = effects(8);
        let wall = Arc::new(lake::clock::TestWallClock::new(3599_000_000_000));
        let mut cfg = config();
        cfg.cache_entries = 1;
        let mut w = Worker::new(cfg, store.clone(), wall.clone(), effects);
        w.admit(logs_pdata());
        let id = *w.active.data.pending_series.iter().next().expect("series");
        w.rotate();
        store.entered.notified().await;
        wall.set(3600_000_000_000);
        w.admit(logs_pdata());
        assert!(w.pending.is_some());
        w.cache.touch([99; 16]);
        store.mode.store(0, Ordering::SeqCst);
        store.release.notify_one();
        let done = w.flushing.as_mut().expect("flush").finish().await;
        w.complete(done);
        assert_eq!(w.cache.last_committed(&id), Some(lake::clock::PartitionId::from_unix_secs(3599)));
        w.rotate();
        w.resume_pending();
        assert_eq!(w.active.data.partition, lake::clock::PartitionId::from_unix_secs(3600));
        assert!(w.active.data.pending_series.contains(&id));
    }).await;
}
/// Scenario: the same series enters ACTIVE while FLUSHING is blocked in the same window.
/// Guarantees: both blocks retain their own descriptor and a late commit cannot retract ACTIVE's copy.
#[tokio::test(flavor = "current_thread")]
async fn same_window_overlap_keeps_both_descriptors() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        let (effects, _rx) = effects(8);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(config(), store.clone(), wall, effects);
        w.admit(logs_pdata());
        let id = *w.active.data.pending_series.iter().next().expect("series");
        w.rotate();
        store.entered.notified().await;
        w.admit(logs_pdata());
        assert!(w.active.data.pending_series.contains(&id));
        assert_eq!(w.flushing.as_ref().expect("flush").tokens.len(), 1);
        assert!(!w.cache.is_committed(&id, w.active.data.partition));
        store.mode.store(0, Ordering::SeqCst);
        store.release.notify_one();
        let done = w.flushing.as_mut().expect("flush").finish().await;
        w.complete(done);
        assert!(w.cache.is_committed(&id, w.active.data.partition));
        assert!(w.active.data.pending_series.contains(&id));
        w.rotate();
        let done = w.flushing.as_mut().expect("second flush").finish().await;
        let report = done.as_ref().expect("join").result.as_ref().expect("write");
        assert_eq!(report.files.len(), 2);
        w.complete(done);
        assert_eq!(w.notify.len(), 2);
    }).await;
}

```

- [ ] **Step 2: Verify the retry test fails before adding retries**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet values_retry_reuses_paths_and_bytes
```

Expected: attempts is 1 and the block is nacked.

- [ ] **Step 3: Retry only storage failures, with one cancellation-aware absolute deadline**

Add `"dep:parquet"` to the `series_parquet` feature in core-nodes for inspecting the Parquet error wrapper. Add to `flush.rs`:

```rust
use std::time::Duration;
fn contains_storage_error(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error.is::<object_store::Error>() || error.is::<std::io::Error>() { return true; }
        match error.source() { Some(source) => error = source, None => return false }
    }
}
pub(super) fn retryable(error: &lake::Error) -> bool {
    match error {
        lake::Error::ObjectStore(_) => true,
        lake::Error::Parquet(parquet::errors::ParquetError::External(source)) => contains_storage_error(source.as_ref()),
        lake::Error::AbortFailed { source, .. } => retryable(source),
        _ => false,
    }
}
async fn write_until(sink: &lake::sink::Sink, data: &lake::buffer::Block<()>,
    cancel: &CancellationToken, deadline: Instant) -> (lake::Result<lake::sink::FlushReport>, u64) {
    let mut attempts = 0_u64;
    let mut delay = Duration::from_millis(200);
    loop {
        if cancel.is_cancelled() || clock::now() >= deadline {
            return (Err(lake::Error::Cancelled { abort_error: None }), attempts);
        }
        attempts += 1;
        let attempt_cancel = cancel.child_token();
        let write = sink.write_block(data, &attempt_cancel);
        tokio::pin!(write);
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                attempt_cancel.cancel();
                // Keep polling so the core performs its bounded writable-phase abort.
                let cleanup = write.await;
                match cleanup { Err(e) => Err(e), Ok(_) => Err(lake::Error::Cancelled { abort_error: None }) }
            }
            () = clock::sleep_until(deadline) => {
                attempt_cancel.cancel();
                let cleanup = write.await;
                match cleanup { Err(e) => Err(e), Ok(_) => Err(lake::Error::Cancelled { abort_error: None }) }
            }
            result = &mut write => result,
        };
        match result {
            Ok(report) => return (Ok(report), attempts),
            Err(error) if retryable(&error) && clock::now() < deadline && !cancel.is_cancelled() => {
                let wake = (clock::now() + delay).min(deadline);
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => return (Err(lake::Error::Cancelled { abort_error: None }), attempts),
                    () = clock::sleep_until(wake) => {}
                }
                delay = (delay * 2).min(Duration::from_secs(10));
            }
            Err(error) => return (Err(error), attempts),
        }
    }
}
```

Replace `FlushJob::new` with:

```rust
pub fn new(data: lake::buffer::Block<()>, tokens: Vec<AckToken>, sink: Rc<lake::sink::Sink>,
    retry_deadline: Duration) -> Self {
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let bytes = data.bytes;
    let started = clock::now();
    let deadline = started + retry_deadline;
    let handle = tokio::task::spawn_local(async move {
        let (result, attempts) = write_until(&sink, &data, &task_cancel, deadline).await;
        FlushDone { data, result, attempts }
    });
    Self { handle, cancel, tokens, bytes, started }
}
```

Pass `self.cfg.window.flush_retry_deadline` at Worker's call site. Do not call `seal` again inside the retry loop. FileNaming and seq stay frozen. A Parquet encoding error without a storage source returns immediately, nacks the entire block as Storage and leaves the worker alive. Log the actual error using the component `otel_error!` in `Worker::complete`'s failure branch:

```rust
otel_error!("series_parquet.flush_failed", error = %error,
    message = "Block failed before durable completion");
```

The `worker.rs` component scope already exists; reuse it for this event.

- [ ] **Step 4: Verify cache, frozen bytes, classifier and in-flight cancellation**

Add the classifier test:

```rust
/// Scenario: an encoding bug and a storage I/O error share the lake Error type.
/// Guarantees: only storage-origin failures receive whole-block retries.
#[test]
fn retry_classifier_distinguishes_encoding_from_storage() {
    assert!(!super::flush::retryable(&lake::Error::Parquet(
        parquet::errors::ParquetError::General("encoding bug".into()))));
    assert!(super::flush::retryable(&lake::Error::ObjectStore(object_store::Error::Generic {
        store: "test", source: Box::new(std::io::Error::other("offline")),
    })));
    assert!(!super::flush::retryable(&lake::Error::Cancelled { abort_error: None }));
}
```

```bash
cd rust/otap-dataflow
cargo check -p otel-arrow-dfe-core-nodes --features series_parquet
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet series_parquet
```

Expected: PASS. Test reads must import `ObjectStoreExt` for `head` under object_store 0.13.2. This task's tests also verify same-series ACTIVE/FLUSHING overlap, late partition commit and eviction before commit; no test marks the cache durable early.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/Cargo.toml rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet
git commit -m "feat(series_parquet): retry sealed blocks until an absolute deadline

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 9: Shutdown with two blocks, saturated completions and cancellation-safe drop

**Files:**
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{mod.rs,worker.rs,flush.rs,tests.rs}`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`

**Interfaces:**
- Consumes: `ExporterInbox::{recv_when,shutdown_deadline}`, `NodeControlMsg::{Shutdown,CollectTelemetry}`, `FlushJob.cancel`, `Notifier.next`, engine clock sleeps and real admin shutdown timeout.
- Produces: `Worker::expire()`, `Worker::finished() -> bool`, `FlushJob::abort_task()`, `finish_expired(&mut Worker) -> ()`. Shutdown first nacks the pending slot, finishes FLUSHING, then rotates ACTIVE. At the deadline all uncommitted tokens become Shutdown nacks; ready notification sends are attempted without extending that deadline. Cleanup may continue for at most abort_timeout, without admitting any new data.
- A blocked completion channel can prevent notification delivery by the deadline. Count and discard undelivered notifications at that point; never re-export committed data. Producers retry their timeout. This is the explicit best-effort delivery limit, not an early durable ACK.

- [ ] **Step 1: Add failing engine-inbox and real receiver-drain tests**

Add harness and tests to `tests.rs`:

```rust
use otel_arrow_dfe_engine::message::{ExporterInbox, Receiver, Sender, Message};
use otel_arrow_dfe_engine::shared::message::{SharedReceiver, SharedSender};
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::Interests;
fn inbox(capacity: usize) -> (Sender<OtapPdata>, Sender<NodeControlMsg<OtapPdata>>, ExporterInbox<OtapPdata>) {
    let (ptx, prx) = tokio::sync::mpsc::channel(capacity);
    let (ctx, crx) = tokio::sync::mpsc::channel(8);
    (Sender::Shared(SharedSender::mpsc(ptx)), Sender::Shared(SharedSender::mpsc(ctx)),
     ExporterInbox::new(Receiver::Shared(SharedReceiver::mpsc(crx)),
         Receiver::Shared(SharedReceiver::mpsc(prx)), 7, Interests::empty()))
}
/// Scenario: the inbox has buffered pdata when Shutdown arrives while admission is closed.
/// Guarantees: a forced message exposes the latched deadline and is nacked without conversion.
#[tokio::test(flavor = "current_thread")]
async fn forced_pdata_exposes_shutdown_and_is_retryably_nacked() {
    let (pdata, control, mut inbox) = inbox(2);
    let deadline = otel_arrow_dfe_engine::clock::now() + Duration::from_secs(1);
    pdata.send(logs_pdata()).await.expect("pdata");
    control.send(NodeControlMsg::Shutdown { deadline, reason: "test".into() }).await.expect("shutdown");
    let data = match inbox.recv_when(false).await.expect("forced data") {
        Message::PData(data) => data,
        other => panic!("expected forced pdata, got {other:?}"),
    };
    assert_eq!(inbox.shutdown_deadline(), Some(deadline));
    let (effects, mut rx) = effects(1);
    let mut notify = Notifier::new(effects, 2);
    let (token, payload) = AckToken::split(data);
    drop(payload);
    notify.push(token, Outcome::Shutdown);
    notify.next().await.expect("delivery");
    match rx.recv().await.expect("nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_eq!(nack.cause, otel_arrow_dfe_engine::control::NackCause::NodeShutdown);
            assert!(nack.refused.is_empty());
        }
        other => panic!("expected nack, got {other:?}"),
    }
}
/// Scenario: FLUSHING is parked, ACTIVE and the pending slot both contain requests.
/// Guarantees: pending is nacked at shutdown and every uncommitted block token is nacked at expiry.
#[tokio::test(flavor = "current_thread")]
async fn deadline_nacks_both_blocks_and_pending() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        let (effects, mut rx) = effects(16);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(config(), store.clone(), wall.clone(), effects);
        w.admit(logs_pdata());
        w.rotate();
        store.entered.notified().await;
        w.admit(logs_pdata());
        wall.set(1_000_000_000);
        w.admit(logs_pdata());
        assert!(w.pending.is_some());
        w.shutdown(otel_arrow_dfe_engine::clock::now());
        assert!(w.pending.is_none());
        w.expire();
        assert_eq!(w.notify.len(), 3);
        for _ in 0..3 {
            w.notify.next().await.expect("nack");
            assert!(matches!(rx.recv().await.expect("completion"), PipelineCompletionMsg::DeliverNack { .. }));
        }
        super::finish_expired(&mut w).await;
        assert!(w.flushing.is_none());
    }).await;
}
/// Scenario: shutdown begins with FLUSHING and ACTIVE populated and storage recovers before the deadline.
/// Guarantees: each block finishes once and both requests receive ACK after durable files exist.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_commits_both_blocks_before_deadline() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        let (effects, mut rx) = effects(4);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(config(), store.clone(), wall, effects);
        w.admit(logs_pdata());
        w.rotate();
        store.entered.notified().await;
        w.admit(logs_pdata());
        w.shutdown(otel_arrow_dfe_engine::clock::now() + Duration::from_secs(10));
        w.inbox_drained = true;
        store.mode.store(0, Ordering::SeqCst);
        store.release.notify_one();
        let first = w.flushing.as_mut().expect("first block").finish().await;
        w.complete(first);
        assert!(!w.finished());
        w.rotate();
        let second = w.flushing.as_mut().expect("active drained into second block").finish().await;
        let report = second.as_ref().expect("join").result.as_ref().expect("durable second block");
        for (_, path, _) in &report.files { assert!(store.head(path).await.is_ok()); }
        w.complete(second);
        for _ in 0..2 {
            w.notify.next().await.expect("delivery");
            assert!(matches!(rx.recv().await.expect("completion"), PipelineCompletionMsg::DeliverAck { .. }));
        }
        assert!(w.finished());
    }).await;
}
/// Scenario: a flush owner is dropped while storage is parked inside a PUT.
/// Guarantees: its token is cancelled and the detached task releases the block within abort_timeout.
#[tokio::test(flavor = "current_thread")]
async fn dropping_worker_cancels_flush_task() {
    tokio::task::LocalSet::new().run_until(async {
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        let (effects, _rx) = effects(2);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let mut w = Worker::new(config(), store.clone(), wall, effects);
        w.admit(logs_pdata());
        w.rotate();
        store.entered.notified().await;
        let cancel = w.flushing.as_ref().expect("flush").cancel.clone();
        drop(w);
        assert!(cancel.is_cancelled());
        tokio::time::timeout(Duration::from_secs(6), async {
            while Arc::strong_count(&store) != 1 { tokio::task::yield_now().await; }
        }).await.expect("flush releases its sink/store reference");
    }).await;
}
```

Add to the Python file:

```python
class ShutdownSlice(unittest.TestCase):
    # Scenario: ingress draining begins with a producer still waiting for its window.
    # Guarantees: the exporter sleep survives timer cancellation and the producer gets ACK, not drain timeout.
    def test_receiver_drain_waits_for_durable_ack(self):
        with tempfile.TemporaryDirectory() as directory, Engine(directory) as engine:
            call = engine.logs.Export.future(log_request("shutdown-request"), timeout=30)
            time.sleep(0.1)
            engine.shutdown(seconds=30)
            call.result(timeout=2)
            with duckdb.connect() as db:
                path = str(engine.data / "v=1/signal=logs/dataset=values/**/*.parquet")
                rows = db.execute("SELECT body FROM read_parquet(?)", [path]).fetchall()
            self.assertEqual(rows, [("shutdown-request",)])
```

- [ ] **Step 2: Run red deadline tests**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet deadline_nacks_both_blocks_and_pending
```

Expected: missing expire/finish functions. Preserve the pending/force-drain tests even if the earlier helper methods already pass.

- [ ] **Step 3: Complete deadline handling without waiting on a full completion channel**

Add `pub inbox_drained: bool` initialized to false to Worker. Only the returned Shutdown control message sets it true: a latched deadline seen during forced pdata is not proof that ingress is empty. Add to Worker:

```rust
pub fn finished(&self) -> bool {
    self.inbox_drained && self.deadline.is_some() && self.pending.is_none()
        && self.active.data.is_empty() && self.flushing.is_none() && self.notify.len() == 0
}
pub fn expire(&mut self) {
    if let Some(pending) = self.pending.take() { self.notify.push(pending.token, Outcome::Shutdown); }
    if let Some(job) = &mut self.flushing {
        job.cancel.cancel();
        if let Some(metrics) = &mut self.metrics { metrics.worker.flush_cancelled.add(1); }
        for token in std::mem::take(&mut job.tokens) { self.notify.push(token, Outcome::Shutdown); }
    }
    self.fail_active(Outcome::Shutdown);
    self.rotation_requested = false;
}
```

Replace the Shutdown control arm in `run` with:

```rust
Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
    worker.inbox_drained = true;
    worker.shutdown(deadline);
}
```

Add to FlushJob:

```rust
pub fn abort_task(&self) { self.handle.abort(); }
```

Add `Notifier::abandon` for queued or in-flight notifications that cannot be delivered:

```rust
pub fn abandon(&mut self) {
    self.failures += self.len() as u64;
    self.queue.clear();
    self.sending = None;
}
```

Add to `mod.rs`:

```rust
async fn finish_expired(worker: &mut worker::Worker) {
    use futures::FutureExt;
    // Poll each ready send once; a blocked channel may not extend the deadline.
    while worker.notify.len() != 0 {
        match worker.notify.next().now_or_never() {
            Some(Ok(())) => {}
            Some(Err(e)) => { otel_warn!("series_parquet.notify_failed", error = %e); }
            None => break,
        }
    }
    if worker.notify.len() != 0 {
        otel_warn!("series_parquet.notify_deadline", undelivered = worker.notify.len(),
            message = "Completion delivery was still blocked at shutdown deadline");
    }
    worker.notify.abandon();
    if let Some(mut job) = worker.flushing.take() {
        job.cancel.cancel();
        let cleanup_deadline = otel_arrow_dfe_engine::clock::now() + worker.cfg.lake.upload.abort_timeout;
        tokio::select! {
            biased;
            _ = job.finish() => {}
            () = otel_arrow_dfe_engine::clock::sleep_until(cleanup_deadline) => {
                job.abort_task();
                let _ = job.finish().await;
                otel_warn!("series_parquet.abort_timeout", message = "Flush cleanup reached its bound");
            }
        }
    }
    worker.sample_metrics();
}
```

Replace the deadline select branch with:

```rust
() = async { match deadline {
    Some(d) => otel_arrow_dfe_engine::clock::sleep_until(d).await,
    None => std::future::pending().await,
} } => {
    worker.expire();
    finish_expired(&mut worker).await;
    let snapshots = worker.metrics.as_mut().map_or_else(Vec::new, metrics::Metrics::snapshots);
    return Ok(TerminalState::new(deadline.expect("deadline elapsed"), snapshots));
}
```

Use `worker.finished()` in the early completion check. On `complete`, if shutting down and ACTIVE is non-empty, set `rotation_requested=true` before the existing rotate/resume code. Do not resume pending during shutdown. Every returned PData checks `inbox.shutdown_deadline()` before admission, even when the receive began with `accept=true`; a latched shutdown always produces a retryable NodeShutdown nack. Also copy the deadline after any control message, so a full notifier cannot hide it. This read is outside the match:

```rust
if let Some(deadline) = inbox.shutdown_deadline() { worker.shutdown(deadline); }
```

At the deadline do not start another store write. Cancellation of the `start` future drops Worker, whose FlushJob Drop cancels the task token; notification futures may drop because the node is being cancelled. Core writable-phase abort remains bounded by `upload.abort_timeout`. The extra cleanup interval does not extend the producer notification deadline; document this separately from the engine's drain deadline.

- [ ] **Step 4: Assert saturated-inbox responsiveness and verify all shutdown paths**

Add this test, using the same inbox/effects/FaultStore helpers:

```rust
/// Scenario: both the pdata inbox and completion channel remain saturated during shutdown.
/// Guarantees: a 100ms deadline is observed within one bounded request plus abort cleanup, under 2s here.
#[tokio::test(flavor = "current_thread")]
async fn saturated_inbox_shutdown_stays_bounded() {
    tokio::task::LocalSet::new().run_until(async {
        let (pdata, control, inbox) = inbox(32);
        let (effects, _completion_rx) = effects(1);
        let mut cfg = config();
        cfg.lake.upload.abort_timeout = Duration::from_millis(100);
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        for _ in 0..32 { pdata.send(logs_pdata()).await.expect("fill inbox"); }
        let started = std::time::Instant::now();
        control.send(NodeControlMsg::Shutdown {
            deadline: otel_arrow_dfe_engine::clock::now() + Duration::from_millis(100), reason: "saturated".into()
        }).await.expect("shutdown");
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        tokio::time::timeout(Duration::from_secs(2), super::run(cfg, store, wall, inbox, effects, None))
            .await.expect("bounded shutdown").expect("node success");
        assert!(started.elapsed() < Duration::from_secs(2));
    }).await;
}
/// Scenario: storage and completion delivery are blocked while another boundary and telemetry control arrive.
/// Guarantees: the actual node re-arms its timer, handles control and observes shutdown without a busy loop.
#[tokio::test(flavor = "current_thread")]
async fn blocked_completion_keeps_boundary_and_control_live() {
    tokio::task::LocalSet::new().run_until(async {
        let simulated = otel_arrow_dfe_engine::clock::SimClock::new();
        let _installed = simulated.install();
        let (pdata, control, inbox) = inbox(4);
        let (effects, _completion_rx) = effects(1);
        let mut prime = Notifier::new(effects.clone(), 1);
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        prime.push(token, Outcome::Ack);
        prime.next().await.expect("fill completion channel");
        let store = Arc::new(FaultStore::default());
        store.mode.store(3, Ordering::SeqCst);
        let wall = Arc::new(lake::clock::TestWallClock::new(0));
        let cfg = config();
        let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
        let metrics = super::metrics::Metrics::register(&context, &cfg.lake);
        let node = tokio::task::spawn_local(super::run(cfg, store.clone(), wall.clone(),
            inbox, effects, Some(metrics)));
        pdata.send(logs_pdata()).await.expect("admit");
        for _ in 0..8 { tokio::task::yield_now().await; }
        wall.set(1_000_000_000);
        simulated.advance(Duration::from_secs(1));
        store.entered.notified().await;
        wall.set(2_000_000_000);
        simulated.advance(Duration::from_secs(1));
        let (samples, reporter) = otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(64);
        control.send(NodeControlMsg::CollectTelemetry { metrics_reporter: reporter })
            .await.expect("collect control");
        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if samples.try_recv().is_ok() { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("telemetry remains responsive after busy boundary");
        control.send(NodeControlMsg::Shutdown {
            deadline: otel_arrow_dfe_engine::clock::now() + Duration::from_secs(1),
            reason: "blocked completions".into(),
        }).await.expect("shutdown");
        for _ in 0..8 { tokio::task::yield_now().await; }
        simulated.advance(Duration::from_secs(2));
        tokio::time::timeout(Duration::from_secs(1), node).await
            .expect("node deadline").expect("join").expect("shutdown result");
    }).await;
}

```

The 2s assertion is a short-test tolerance for tiny requests and a 100ms abort budget, not a claim that arbitrary 16MiB admissions finish in 2s. The design target is one bounded admission plus notify_batch plus one merge chunk, followed by deadline/abort cleanup. Q6 accepts synchronous whole-block descriptor sealing as a v1 allowance; large-block responsiveness includes that work and does not meet a fixed-workspace/control-latency claim.

```bash
cd rust/otap-dataflow
cargo check -p otel-arrow-dfe-core-nodes --features series_parquet
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet series_parquet
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py LocalSlice MetricsSlice ShutdownSlice -v
```

Expected: PASS for successful drains, deadline nacks, force-drained pdata, pending-slot rejection, cancellation and owner drop. No test treats a dropped notification as a storage retry.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py
git commit -m "feat(series_parquet): drain both blocks and bound shutdown cancellation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 10: Alloy -> MinIO/RustFS E2E, two readers and restart/disconnect behavior

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Create: `rust/otap-dataflow/configs/series-parquet-s3.yaml`
- Create: `rust/otap-dataflow/configs/series-parquet.alloy`

**Interfaces:**
- Consumes: `Engine(directory, storage=None, overrides=None)`, `log_request`, `metric_request`, real feature-enabled `df_engine`, Docker CLI, Grafana Alloy, boto3, DuckDB and clickhouse-local or the local ClickHouse image.
- Produces: `DockerStore(kind)` with `storage`, `download(Path)`, `stop()`, `recover()`; `AlloyProducer(directory, engine)` with `write(ids)`; `wait_for_alloy(store, directory, ids)`; `verify_files` and `verify_readers`; `DockerSlice` and `RestartSlice`. Every container has a unique name and cleanup path. No shared resource cleanup occurs. Only Alloy may be pulled; the storage/reader images must already be local.
- Container invocation is grounded in the official [MinIO container guide](https://min.io/docs/minio/container/index.html) and [RustFS container guide](https://docs.rustfs.com/en/installation/container). Both use supplied local images; endpoint/credential fields mirror `configs/trafficgen-parquet-local-s3.yaml:24` and `otap/src/object_store.rs:185`.

- [ ] **Step 1: Add real S3 assertions and Docker test cases before adding the launcher**

Add before `unittest.main()`:

```python
import boto3
from botocore.config import Config as BotoConfig
import uuid
import xxhash


def verify_files(test, root, log_ids, metric_count, allow_duplicates=False):
    files = sorted(Path(root).rglob("*.parquet"))
    test.assertTrue(files)
    coverage = set()
    values = []
    bodies = []
    counts = {"number": 0, "histogram": 0}
    with duckdb.connect() as db:
        for path in files:
            partitions = dict(part.split("=", 1) for part in path.parts if "=" in part)
            metadata = dict(db.execute(
                "SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)", [str(path)]
            ).fetchall())
            signal = partitions["signal"]
            dataset = partitions["dataset"]
            worker = (metadata["writer_id"], metadata["boot_id"])
            partition = (partitions["date"], partitions["hour"])
            rows = db.execute("SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]).fetchall()
            test.assertEqual(int(metadata["row_count"]), len(rows))
            if dataset == "series":
                ids = db.execute("SELECT series_id, identity_bytes FROM read_parquet(?)", [str(path)]).fetchall()
                test.assertEqual([row[0] for row in ids], sorted(row[0] for row in ids))
                for series_id, identity in ids:
                    test.assertEqual(xxhash.xxh3_128_digest(identity), series_id)
                    coverage.add((signal, partition, worker, series_id))
            else:
                keys = db.execute("SELECT series_id, time_unix_nano FROM read_parquet(?)", [str(path)]).fetchall()
                if metadata["sort_key"] != "none":
                    test.assertEqual(keys, sorted(keys, key=lambda row: (row[0], row[1] is None, row[1] or 0)))
                values.extend((signal, partition, worker, row[0]) for row in keys)
                if dataset == "values":
                    bodies.extend(row[0] for row in db.execute("SELECT body FROM read_parquet(?)", [str(path)]).fetchall())
                else:
                    counts[dataset] += len(keys)
        test.assertTrue(set(values).issubset(coverage), "descriptor coverage per partition and worker")
        test.assertTrue(set(log_ids).issubset(set(bodies)))
        if not allow_duplicates:
            test.assertEqual(sorted(bodies), sorted(log_ids))
            test.assertEqual(counts, {"number": metric_count, "histogram": metric_count})
        else:
            test.assertGreaterEqual(counts["number"], metric_count)
            test.assertGreaterEqual(counts["histogram"], metric_count)
        for signal, datasets in (("logs", ("values",)), ("metrics", ("number", "histogram"))):
            series = [str(p) for p in files if f"signal={signal}" in p.parts and "dataset=series" in p.parts]
            if not series:
                continue
            db.execute("CREATE OR REPLACE TEMP TABLE canonical AS SELECT * FROM read_parquet(?, union_by_name=true, filename=true) QUALIFY row_number() OVER (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC)=1", [series])
            for dataset in datasets:
                selected = [str(p) for p in files if f"signal={signal}" in p.parts and f"dataset={dataset}" in p.parts]
                if selected:
                    before = db.execute("SELECT count(*) FROM read_parquet(?, union_by_name=true)", [selected]).fetchone()[0]
                    after = db.execute("SELECT count(*) FROM read_parquet(?, union_by_name=true) v JOIN canonical s USING(series_id)", [selected]).fetchone()[0]
                    test.assertEqual(before, after, "canonical join must preserve values cardinality")


class DockerSlice(unittest.TestCase):
    def exercise(self, kind):
        require_clickhouse()
        with DockerStore(kind) as store, tempfile.TemporaryDirectory() as directory:
            with Engine(directory, storage=store.storage) as engine:
                ids = [f"{kind}-alloy-{i}" for i in range(12)]
                metric_ids = [f"{kind}-metric-{i}" for i in range(6)]
                with AlloyProducer(directory, engine) as alloy:
                    alloy.write(ids)
                    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
                        metric = metrics_rpc.MetricsServiceStub(engine.channel)
                        results = [pool.submit(metric.Export, metric_request(item), timeout=30)
                                   for item in metric_ids]
                        for result in results:
                            result.result(timeout=35)
                    wait_for_alloy(store, directory, ids)
                engine.shutdown()
                downloaded = Path(directory) / "downloaded"
                store.download(downloaded)
                verify_files(self, downloaded, ids, 6)
                verify_readers(self, downloaded, ids, 6, alloy_ids=ids, metric_ids=metric_ids)

    # Scenario: Docker Alloy tails 12 known lines into a real engine using MinIO, alongside synthetic metrics.
    # Guarantees: DuckDB and ClickHouse agree on bodies, attributes, counts and latest-descriptor joins.
    def test_minio(self):
        self.exercise("minio")

    # Scenario: the same Alloy and synthetic-metrics topology writes to the local RustFS image.
    # Guarantees: both readers recover all expected rows with identical bodies, attributes and descriptor coverage.
    def test_rustfs(self):
        self.exercise("rustfs")
```

- [ ] **Step 2: Run red Docker tests**

```bash
cd rust/otap-dataflow
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet,aws
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py DockerSlice -v
```

Expected: missing AlloyProducer/reader/prerequisite helpers and DockerStore. After the launcher exists, absent Docker/image is a reported unittest skip; a reachable Docker daemon with an existing but failing image is a failure.

- [ ] **Step 3: Add a launcher that uses local images and cleans up on failure**

Insert these helpers before `DockerStore` and `DockerSlice`. Defaults implement Q2 exactly. The Alloy default is the [latest stable v1.19.2 release](https://github.com/grafana/alloy/releases/tag/v1.19.2) verified during this amendment; `SERIES_ALLOY_IMAGE` permits a later stable release. Only its missing image may be pulled. The [Loki-to-OTLP bridge](https://grafana.com/docs/alloy/latest/reference/components/otelcol/otelcol.receiver.loki/) and [OTLP exporter](https://grafana.com/docs/alloy/latest/reference/components/otelcol/otelcol.exporter.otlp/) provide the file-to-gRPC path.

```python
IMAGE_DEFAULTS = {
    "minio": "minio/minio:RELEASE.2025-04-22T22-12-26Z",
    "rustfs": "rustfs/rustfs:1.0.0-rc.3",
    "clickhouse": "clickhouse/clickhouse-server:26.7.4",
    "alloy": "grafana/alloy:v1.19.2",
}


def unavailable(reason):
    if os.environ.get("SERIES_REQUIRE_DOCKER") == "1":
        raise AssertionError(reason)
    raise unittest.SkipTest(reason)


def require_docker_image(kind):
    if not shutil.which("docker"):
        unavailable("Docker CLI absent")
    try:
        probe = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
    except subprocess.TimeoutExpired:
        unavailable("Docker daemon did not respond within 10 seconds")
    if probe.returncode:
        unavailable("Docker daemon unavailable")
    image = os.environ.get("SERIES_" + kind.upper() + "_IMAGE", IMAGE_DEFAULTS[kind])
    present = subprocess.run(["docker", "image", "inspect", image],
                             capture_output=True, timeout=10).returncode == 0
    if not present and kind == "alloy":
        # Alloy is the only image this plan authorizes the test runner to pull.
        try:
            pull = subprocess.run(["docker", "pull", image], capture_output=True,
                                  text=True, timeout=180)
            present = pull.returncode == 0
        except subprocess.TimeoutExpired:
            present = False
    if not present:
        unavailable(f"Selected local {kind} image is absent: {image}")
    return image


def require_clickhouse():
    binary = os.environ.get("SERIES_CLICKHOUSE_LOCAL", "/usr/bin/clickhouse-local")
    if Path(binary).is_file() and os.access(binary, os.X_OK):
        return binary
    binary = shutil.which("clickhouse-local") if "SERIES_CLICKHOUSE_LOCAL" not in os.environ else None
    if binary:
        return binary
    require_docker_image("clickhouse")
    return None
```

Create the complete `configs/series-parquet.alloy` below. Task 11 uses this same file; task 12 reproduces it byte-for-byte in the README. Linux host networking lets Alloy reach the engine's loopback-only gRPC listener without exposing it on all interfaces. Alloy's file positions and sending queue belong to the producer; the exporter still has no persistent state.

```river
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
loki.source.file "series" {
  targets = [{ __path__ = "/input/events.log", job = "series-e2e" }]
  forward_to = [otelcol.receiver.loki.series.receiver]
  tail_from_end = false
}

otelcol.receiver.loki "series" {
  output {
    logs = [otelcol.processor.attributes.series.input]
  }
}

otelcol.processor.attributes "series" {
  action {
    key = "e2e.source"
    value = "alloy-file"
    action = "insert"
  }
  output {
    logs = [otelcol.exporter.otlp.series.input]
  }
}

otelcol.exporter.otlp "series" {
  timeout = "6s"
  client {
    endpoint = sys.env("OTLP_ENDPOINT")
    compression = "none"
    tls {
      insecure = true
    }
  }
  sending_queue {
    enabled = true
    num_consumers = 1
    queue_size = 128
  }
  retry_on_failure {
    enabled = true
    initial_interval = "200ms"
    max_interval = "1s"
    max_elapsed_time = "0s"
  }
}
```

The finite file fixture fits Alloy's queue. Infinite retry elapsed time retains failed requests until this bounded test's recovery; the test still times out on a missing row. Alloy's queue is outside `df_engine` RSS, so it cannot mask exporter-owned memory growth. Add this complete launcher and eventual-durability wait:

```python
class AlloyProducer:
    def __init__(self, directory, engine):
        self.root = Path(directory) / "alloy"
        self.engine = engine
        self.name = "series-alloy-" + uuid.uuid4().hex
        self.container = None

    def __enter__(self):
        if not os.sys.platform.startswith("linux"):
            unavailable("Alloy host-network fixture requires Linux")
        image = require_docker_image("alloy")
        self.root.mkdir(mode=0o755)
        self.lines = self.root / "events.log"
        self.lines.write_text("")
        config = self.root / "config.alloy"
        config.write_text((WORKSPACE / "configs/series-parquet.alloy").read_text())
        port = free_port()
        args = ["docker", "run", "--pull=never", "--detach", "--name", self.name,
                "--network", "host", "--user", f"{os.getuid()}:{os.getgid()}",
                "--mount", f"type=bind,src={self.root.resolve()},dst=/input,readonly",
                "-e", f"OTLP_ENDPOINT=127.0.0.1:{self.engine.grpc_port}",
                image, "run", "--storage.path=/tmp/alloy-state",
                f"--server.http.listen-addr=127.0.0.1:{port}", "/input/config.alloy"]
        try:
            self.container = subprocess.check_output(args, text=True, timeout=30).strip()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                state = subprocess.check_output(
                    ["docker", "inspect", "--format", "{{.State.Running}}", self.container],
                    text=True, timeout=10).strip()
                if state != "true":
                    raise AssertionError("Alloy exited before becoming ready")
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/-/ready", timeout=1) as response:
                        if response.status == 200:
                            return self
                except (OSError, urllib.error.URLError):
                    pass
                time.sleep(0.1)
            raise AssertionError("Alloy readiness deadline exceeded")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def write(self, ids):
        with self.lines.open("a") as stream:
            for item in ids:
                if "\n" in item:
                    raise ValueError("each expected body must be one log line")
                stream.write(item + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    def __exit__(self, *exc):
        if self.container:
            try:
                subprocess.run(["docker", "stop", "--time", "10", self.container],
                               check=True, capture_output=True, timeout=20)
                logs = subprocess.run(["docker", "logs", self.container],
                                      capture_output=True, text=True, timeout=10)
                (self.root / "alloy.log").write_text(logs.stdout + logs.stderr)
            finally:
                subprocess.run(["docker", "rm", "--force", "--volumes", self.container],
                               check=False, capture_output=True, timeout=20)
                self.container = None


def wait_for_alloy(store, directory, ids, timeout=90):
    target = Path(directory) / "downloaded"
    deadline = time.monotonic() + timeout
    actual = set()
    while time.monotonic() < deadline:
        store.download(target)
        paths = sorted((target / "v=1/signal=logs/dataset=values").rglob("*.parquet"))
        if paths:
            with duckdb.connect() as db:
                actual = {row[0] for row in db.execute(
                    "SELECT body FROM read_parquet(?, union_by_name=true)",
                    [[str(path) for path in paths]]).fetchall()}
            if set(ids).issubset(actual):
                return
        time.sleep(0.2)
    raise AssertionError(f"Alloy rows did not become durable: {sorted(set(ids) - actual)}")
```

Add the full dual-reader implementation below. The [ClickHouse file table function](https://clickhouse.com/docs/reference/functions/table-functions/file) reads the same downloaded objects as DuckDB. It uses the README's latest-descriptor window join, with the full filename as a deterministic tie breaker. A present but failing binary/container/query is a failure, never a skip. Reader availability is checked before starting the expensive E2E workload.

```python
@contextlib.contextmanager
def clickhouse_reader(root):
    root = Path(root).resolve()
    binary = require_clickhouse()
    container = None
    try:
        if binary:
            prefix = [binary]
        else:
            image = require_docker_image("clickhouse")
            name = "series-reader-" + uuid.uuid4().hex
            container = subprocess.check_output(
                ["docker", "run", "--pull=never", "--detach", "--name", name,
                 "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
                 "--mount", f"type=bind,src={root},dst=/data,readonly",
                 "--entrypoint", "/bin/sleep", image, "infinity"],
                text=True, timeout=30).strip()
            prefix = ["docker", "exec", "--workdir", "/data", container, "clickhouse", "local"]

        def query(sql):
            result = subprocess.run(
                prefix + ["--query", sql + " FORMAT JSONCompactEachRow",
                          "--output_format_json_quote_64bit_integers=0"],
                cwd=root, check=True, capture_output=True, text=True, timeout=30)
            return [tuple(json.loads(line)) for line in result.stdout.splitlines() if line.strip()]

        yield query
    finally:
        if container:
            subprocess.run(["docker", "rm", "--force", "--volumes", container],
                           check=False, capture_output=True, timeout=20)


def sql_string(value):
    return "'" + str(value).replace(chr(92), chr(92) * 2).replace("'", "''") + "'"


def verify_readers(test, root, log_ids, metric_count, allow_duplicates=False,
                   alloy_ids=(), metric_ids=()):
    root = Path(root).resolve()
    alloy_ids = set(alloy_ids)
    with duckdb.connect() as db, clickhouse_reader(root) as clickhouse:
        for signal, dataset in (("logs", "values"), ("metrics", "number"), ("metrics", "histogram")):
            relative = f"v=1/signal={signal}/dataset={dataset}/**/*.parquet"
            paths = sorted(root.glob(relative))
            expected_count = len(log_ids) if signal == "logs" else metric_count
            if not paths:
                test.assertEqual(expected_count, 0, f"missing {signal}/{dataset}")
                continue
            series = f"v=1/signal={signal}/dataset=series/**/*.parquet"
            duck_values = f"read_parquet({sql_string(root / relative)}, union_by_name=true)"
            duck_series = f"read_parquet({sql_string(root / series)}, union_by_name=true, filename=true)"
            ch_values = f"file({sql_string(relative)}, 'Parquet')"
            ch_series = f"file({sql_string(series)}, 'Parquet')"
            projection = ("v.body, coalesce(v.attrs['e2e.source'], '')" if signal == "logs"
                          else "s.attrs['request.id']")
            duck_sql = f"""
                WITH canonical AS (
                    SELECT * FROM {duck_series}
                    QUALIFY row_number() OVER (
                        PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1
                )
                SELECT {projection} FROM {duck_values} v
                INNER JOIN canonical s ON v.series_id = s.series_id
            """
            ch_sql = f"""
                WITH canonical AS (
                    SELECT * FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
                        FROM {ch_series}
                    ) WHERE rank = 1
                )
                SELECT {projection} FROM {ch_values} AS v
                INNER JOIN canonical AS s ON v.series_id = s.series_id
            """
            duck_rows = sorted(db.execute(duck_sql).fetchall())
            ch_rows = sorted(clickhouse(ch_sql))
            test.assertEqual(ch_rows, duck_rows, f"reader disagreement for {signal}/{dataset}")
            before = db.execute(f"SELECT count(*) FROM {duck_values}").fetchone()[0]
            ch_before = int(clickhouse(f"SELECT count(*) FROM {ch_values}")[0][0])
            test.assertEqual(ch_before, before)
            test.assertEqual(len(duck_rows), before, "latest-descriptor join must preserve cardinality")
            if allow_duplicates:
                test.assertGreaterEqual(before, expected_count)
            else:
                test.assertEqual(before, expected_count)
            if signal == "logs":
                expected = sorted((body, "alloy-file" if body in alloy_ids else "") for body in log_ids)
                if allow_duplicates:
                    test.assertEqual(set(duck_rows), set(expected))
                else:
                    test.assertEqual(duck_rows, expected)
            elif metric_ids:
                test.assertEqual({row[0] for row in duck_rows}, set(metric_ids))
```

Insert `DockerStore` before DockerSlice:

```python
class DockerStore:
    def __init__(self, kind):
        self.kind = kind
        self.name = "series-e2e-" + uuid.uuid4().hex
        self.container = None
        self.bucket = "series-test"
        self.key = "series-test-access"
        self.secret = "series-test-secret-12345"

    def __enter__(self):
        image = require_docker_image(self.kind)
        args = ["docker", "run", "--pull=never", "--detach", "--name", self.name,
                "--publish", "127.0.0.1::9000"]
        if self.kind == "minio":
            args += ["-e", f"MINIO_ROOT_USER={self.key}", "-e", f"MINIO_ROOT_PASSWORD={self.secret}",
                     image, "server", "/data", "--address", ":9000"]
        else:
            args += ["-e", f"RUSTFS_ACCESS_KEY={self.key}", "-e", f"RUSTFS_SECRET_KEY={self.secret}",
                     "-e", "RUSTFS_ADDRESS=0.0.0.0:9000", "-e", "RUSTFS_VOLUMES=/data",
                     image]
        try:
            self.container = subprocess.check_output(args, text=True).strip()
            mapping = subprocess.check_output(["docker", "port", self.container, "9000/tcp"], text=True).strip()
            self.endpoint = "http://" + mapping.splitlines()[0]
            self.client = boto3.client("s3", endpoint_url=self.endpoint, region_name="us-east-1",
                aws_access_key_id=self.key, aws_secret_access_key=self.secret,
                config=BotoConfig(connect_timeout=1, read_timeout=1, retries={"max_attempts": 0},
                                  s3={"addressing_style": "path"}))
            self.ready()
            self.client.create_bucket(Bucket=self.bucket)
            self.storage = {"s3": {"base_uri": f"s3://{self.bucket}/otel", "region": "us-east-1",
                "endpoint": self.endpoint, "allow_http": True, "virtual_hosted_style_request": False,
                "auth": {"type": "static_credentials", "access_key_id": self.key,
                         "secret_access_key": self.secret}}}
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def ready(self):
        deadline = time.monotonic() + 60
        last_error = None
        while time.monotonic() < deadline:
            try:
                self.client.list_buckets()
                return
            except Exception as error:
                last_error = error
                time.sleep(0.2)
        logs = subprocess.check_output(["docker", "logs", self.container], stderr=subprocess.STDOUT, text=True)
        raise AssertionError(f"{self.kind} never became S3-ready: {last_error}\n{logs}")

    def download(self, directory):
        directory = Path(directory)
        for page in self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix="otel/"):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith(".parquet"):
                    destination = directory / key.removeprefix("otel/")
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    self.client.download_file(self.bucket, key, str(destination))

    def stop(self):
        subprocess.run(["docker", "stop", "--time", "0", self.container], check=True, capture_output=True)

    def recover(self):
        subprocess.run(["docker", "start", self.container], check=True, capture_output=True)
        self.ready()

    def __exit__(self, *exc):
        if self.container:
            subprocess.run(["docker", "rm", "--force", "--volumes", self.container],
                           check=False, capture_output=True)
            self.container = None
```

MinIO and RustFS retain data in their own container/anonymous volume across stop/start. This is object-store persistence, not exporter persistence. The test downloads actual completed S3 objects once for both DuckDB and ClickHouse, avoiding external reader extensions. Readers run after producer delivery and engine shutdown so both inspect the same final object set.

- [ ] **Step 4: Add restart, producer-disconnect and additive schema tests**

```python
class RestartSlice(unittest.TestCase):
    # Scenario: two engine processes write into the same aligned window and base directory.
    # Guarantees: boot IDs prevent object-name collisions and additive columns read with union-by-name.
    def test_restart_names_and_additive_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "destination"
            target.mkdir()
            storage = {"file": {"base_uri": str(target)}}
            names = []
            for index in range(2):
                work = root / f"engine-{index}"
                work.mkdir()
                overrides = {"window": {"interval": "3153600000s", "max_requests_per_block": 1,
                                        "flush_retry_deadline": "10s"}}
                if index:
                    overrides["logs"] = {"denormalize": [{"path": "attrs.new.field", "column": "new_field", "type": "string"}]}
                with Engine(work, storage=storage, overrides=overrides) as engine:
                    request = log_request(f"restart-{index}")
                    if index:
                        request.resource_logs[0].scope_logs[0].log_records[0].attributes.add(key="new.field").value.string_value = "added"
                    engine.logs.Export(request, timeout=20)
                    engine.shutdown()
                names.append(set(path.name for path in target.rglob("*.parquet")))
            self.assertTrue(names[0] < names[1])
            with duckdb.connect() as db:
                path = str(target / "v=1/signal=logs/dataset=values/**/*.parquet")
                rows = db.execute("SELECT body, new_field FROM read_parquet(?, union_by_name=true) ORDER BY body", [path]).fetchall()
                self.assertEqual(rows, [("restart-0", None), ("restart-1", "added")])
                windows = db.execute("SELECT DISTINCT decode(value) FROM parquet_kv_metadata(?) WHERE decode(key)='window_start'", [path]).fetchall()
                self.assertEqual(windows, [("0",)])
            verify_files(self, target, ["restart-0", "restart-1"], 0)

    # Scenario: a producer disconnects after admission but before a long window commits.
    # Guarantees: abandoning the client result cannot retract accepted data.
    def test_disconnect_does_not_remove_data(self):
        with tempfile.TemporaryDirectory() as directory:
            overrides = {"window": {"interval": "5s", "flush_retry_deadline": "10s"}}
            with Engine(directory, overrides=overrides) as engine:
                call = engine.logs.Export.future(log_request("disconnected"), timeout=20)
                deadline = time.monotonic() + 4
                while time.monotonic() < deadline:
                    metrics = engine_metrics(engine)
                    if metric_max(metrics, "block.requests_pending") >= 1:
                        break
                    time.sleep(0.05)
                else:
                    self.fail("request was not observed as admitted")
                call.cancel()
                engine.shutdown(seconds=30)
                verify_files(self, engine.data, ["disconnected"], 0)


def engine_metrics(engine):
    url = f"http://127.0.0.1:{engine.admin_port}/api/v1/telemetry/metrics?format=json&keep_all_zeroes=true"
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def metric_max(document, name):
    values = [item["value"] for group in document["metric_sets"]
              if group["name"] == "exporter.series_parquet"
              for item in group["metrics"] if item["name"] == name
              and isinstance(item["value"], (int, float))]
    return max(values, default=0)
```

Use the actual admin response names from `admin/src/telemetry.rs:58` and the explicit `format=json` selector. Do not create an exporter introspection endpoint.

Create `configs/series-parquet-s3.yaml` by copying the entire local YAML from task 1 and replacing only the storage mapping with the following mapping and inserting the retry mapping beside it. This is a mechanical copy, not a new schema:

```yaml
storage:
  s3:
    base_uri: s3://series-test/otel
    region: us-east-1
    endpoint: http://127.0.0.1:9000
    allow_http: true
    virtual_hosted_style_request: false
    auth:
      type: static_credentials
      access_key_id: series-test-access
      secret_access_key: series-test-secret-12345
retry:
  max_retries: 5
  init_backoff: 200ms
  max_backoff: 10s
  backoff_base: 2.0
  retry_timeout: 30s
```

The example credentials are local test credentials. Production storage uses the existing shared AWS auth provider configuration. The executable copy command is:

```bash
cp rust/otap-dataflow/configs/series-parquet-local.yaml rust/otap-dataflow/configs/series-parquet-s3.yaml
python3 - <<'PY'
from pathlib import Path
p = Path('rust/otap-dataflow/configs/series-parquet-s3.yaml')
s = p.read_text()
s = s.replace('              storage: {file: {base_uri: /tmp/series-parquet}}', '''              storage:
                s3:
                  base_uri: s3://series-test/otel
                  region: us-east-1
                  endpoint: http://127.0.0.1:9000
                  allow_http: true
                  virtual_hosted_style_request: false
                  auth:
                    type: static_credentials
                    access_key_id: series-test-access
                    secret_access_key: series-test-secret-12345
              retry:
                max_retries: 5
                init_backoff: 200ms
                max_backoff: 10s
                backoff_base: 2.0
                retry_timeout: 30s''')
p.write_text(s)
PY
```

- [ ] **Step 5: Run the required-prerequisite lane with the ruled image defaults**

From `rust/otap-dataflow`, using the already-present storage and reader images:

```bash
SERIES_REQUIRE_DOCKER=1 SERIES_MINIO_IMAGE=minio/minio:RELEASE.2025-04-22T22-12-26Z SERIES_RUSTFS_IMAGE=rustfs/rustfs:1.0.0-rc.3 SERIES_CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:26.7.4 SERIES_ALLOY_IMAGE=grafana/alloy:v1.19.2 /tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py DockerSlice -v
```

The same defaults apply without environment overrides. Missing Docker/storage/reader images skip locally; `SERIES_REQUIRE_DOCKER=1` makes these failures. The runner may pull only Alloy. A missing native clickhouse-local falls back to the selected local ClickHouse image; missing both routes skips cleanly under the local policy. No digest pin or workflow is added here; plan 3 owns CI provisioning and pinning.

- [ ] **Step 6: Verify both Docker backends, skip behavior and commit**

```bash
cd rust/otap-dataflow
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet,aws
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py DockerSlice RestartSlice -v
DOCKER_HOST=unix:///tmp/series-parquet-no-daemon.sock /tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py DockerSlice -v
cd ../..
npx markdownlint-cli2 docs/superpowers/plans/2026-09-21-series-parquet-exporter-node.md
python3 tools/sanitycheck.py
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/configs/series-parquet-s3.yaml rust/otap-dataflow/configs/series-parquet.alloy
git commit -m "test(series_parquet): verify real MinIO and RustFS pipelines

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

Expected: both installed-image tests PASS with 12 Alloy bodies/attributes and six points per metric dataset agreeing in DuckDB and ClickHouse. Docker-absent run reports two clean skips. All stored identities and every values file's partition/worker descriptor coverage are checked; synthetic request/ACK coverage remains in tasks 1/6/11.

### Task 11: V1 storage outage, producer retries and bounded recovery

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`

**Interfaces:**
- Consumes: `DockerStore::{stop,recover,download}`, Engine, both synthetic OTLP clients, `AlloyProducer`, the complete task 10 River file, `wait_for_alloy`, `engine_metrics`, `metric_max`, `verify_files`, and `verify_readers` (including its full ClickHouse subprocess/docker-exec code).
- Produces: `OutageSlice.test_storage_outage_recovers_without_losing_acked_data`. A stopped real endpoint lasts 8s, exceeding a 3s whole-block deadline. Docker Alloy tails 12 known log lines during the outage using task 10's exact River config. Eight synthetic producers continue submitting logs and metrics and retain identical requests across retries for explicit ACK/status assertions. The run is bounded to 120s of producer activity; it is the single v1 failure case, not the deferred chaos/soak program.

- [ ] **Step 1: Add the failing outage regression with exact assertions**

Add before the unittest guard:

```python
import threading


def rss_bytes(pid):
    status = Path(f"/proc/{pid}/status")
    if not status.exists():
        raise unittest.SkipTest("Outage RSS assertion requires Linux /proc")
    for line in status.read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    raise AssertionError("process RSS unavailable while engine is running")


class OutageSlice(unittest.TestCase):
    # Scenario: Alloy file tailing and synthetic OTLP requests continue during an eight-second real S3 outage.
    # Guarantees: both readers recover every Alloy body/attribute and ACKed synthetic ID within the memory envelope.
    def test_storage_outage_recovers_without_losing_acked_data(self):
        require_clickhouse()
        for kind in ("minio", "rustfs"):
            with self.subTest(store=kind), DockerStore(kind) as store:
                with tempfile.TemporaryDirectory() as directory:
                    overrides = {
                        "window": {"interval": "1s", "max_block_bytes": "8MiB",
                                   "max_requests_per_block": 8, "flush_retry_deadline": "3s"},
                        "ingress": {"max_request_bytes": "1MiB", "max_extracted_bytes": "2MiB",
                                    "max_row_bytes": "16KiB", "max_nesting_depth": 32},
                        "series_cache": {"max_entries": 128},
                        "sorting": {"enabled": True, "run_target_bytes": "64KiB", "merge_chunk_bytes": "128KiB"},
                        "parquet": {"compression": "zstd", "row_group_bytes": "512KiB", "writer_limit_bytes": "1MiB"},
                        "upload": {"part_bytes": "5MiB", "concurrency": 1, "abort_timeout": "1s"},
                        "retry": {"max_retries": 0, "init_backoff": "50ms", "max_backoff": "100ms",
                                  "backoff_base": 2.0, "retry_timeout": "200ms"},
                    }
                    with Engine(directory, storage=store.storage, overrides=overrides) as engine, AlloyProducer(directory, engine) as alloy:
                        alloy_ids = [f"{kind}-outage-alloy-{i}" for i in range(12)]
                        engine.logs.Export(log_request("warmup"), timeout=10)
                        baseline_rss = rss_bytes(engine.process.pid)
                        acknowledged = []
                        retry_codes = []
                        durations = []
                        samples = []
                        lock = threading.Lock()
                        end = time.monotonic() + 120

                        def producer(index):
                            logs = logs_rpc.LogsServiceStub(engine.channel)
                            metrics = metrics_rpc.MetricsServiceStub(engine.channel)
                            for item in range(12):
                                request_id = f"p{index}-{item}"
                                for signal, request, send in (
                                    ("logs", log_request(request_id), logs.Export),
                                    ("metrics", metric_request(request_id), metrics.Export),
                                ):
                                    while time.monotonic() < end:
                                        started = time.monotonic()
                                        try:
                                            send(request, timeout=6)
                                            with lock:
                                                acknowledged.append((signal, request_id))
                                                durations.append(time.monotonic() - started)
                                            break
                                        except grpc.RpcError as error:
                                            with lock:
                                                retry_codes.append(error.code())
                                                durations.append(time.monotonic() - started)
                                            if error.code() not in (
                                                grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED,
                                                grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.CANCELLED,
                                            ):
                                                raise
                                            time.sleep(0.1)
                                    else:
                                        raise AssertionError("producer could not drain after recovery")

                        store.stop()
                        alloy.write(alloy_ids)
                        outage_end = time.monotonic() + 8
                        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                            jobs = [pool.submit(producer, index) for index in range(8)]
                            while time.monotonic() < outage_end:
                                document = engine_metrics(engine)
                                samples.append((document, rss_bytes(engine.process.pid)))
                                time.sleep(0.2)
                            self.assertTrue(any(not job.done() for job in jobs), "outage must block producers")
                            store.recover()
                            while not all(job.done() for job in jobs):
                                self.assertLess(time.monotonic(), end, "recovery deadline")
                                samples.append((engine_metrics(engine), rss_bytes(engine.process.pid)))
                                time.sleep(0.2)
                            for job in jobs:
                                job.result()
                        wait_for_alloy(store, directory, alloy_ids, timeout=max(1, end - time.monotonic()))
                        (Path(directory) / "outage-samples.json").write_text(json.dumps(samples))
                        self.assertTrue(retry_codes, "outage must cause retryable failures/timeouts")
                        self.assertTrue(any(duration >= 0.5 for duration in durations), "producer requests must actually wait")
                        self.assertEqual(len(acknowledged), 8 * 12 * 2)
                        for document, rss in samples:
                            self.assertLessEqual(metric_max(document, "block.active_bytes"), 8 << 20)
                            self.assertLessEqual(metric_max(document, "block.flushing_bytes"), 8 << 20)
                            self.assertLessEqual(metric_max(document, "block.pending_slot_occupied"), 1)
                            self.assertLessEqual(metric_max(document, "notify.queued"), 16)
                            budget = metric_max(document, "memory.budget_bytes")
                            self.assertGreater(budget, 0, "missing budget telemetry is a failure")
                            self.assertLessEqual(rss, baseline_rss + budget + (128 << 20))
                        deadline = time.monotonic() + 10
                        while time.monotonic() < deadline:
                            document = engine_metrics(engine)
                            if metric_max(document, "oldest_unacked_seconds") <= 1 and metric_max(document, "block.requests_pending") == 0:
                                break
                            time.sleep(0.2)
                        else:
                            self.fail("oldest unacked age did not return to baseline")
                        engine.shutdown(seconds=30)
                        downloaded = Path(directory) / "downloaded"
                        store.download(downloaded)
                        logs = [rid for signal, rid in acknowledged if signal == "logs"]
                        expected_logs = ["warmup"] + logs + alloy_ids
                        metric_ids = [rid for signal, rid in acknowledged if signal == "metrics"]
                        verify_files(self, downloaded, expected_logs, 96, allow_duplicates=True)
                        verify_readers(self, downloaded, expected_logs, 96, allow_duplicates=True,
                                       alloy_ids=alloy_ids, metric_ids=metric_ids)
                        with duckdb.connect() as db:
                            series = str(downloaded / "v=1/signal=metrics/dataset=series/**/*.parquet")
                            db.execute("CREATE TEMP TABLE series AS SELECT * FROM read_parquet(?, union_by_name=true, filename=true) QUALIFY row_number() OVER(PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC)=1", [series])
                            for dataset in ("number", "histogram"):
                                path = str(downloaded / f"v=1/signal=metrics/dataset={dataset}/**/*.parquet")
                                actual = {row[0] for row in db.execute("SELECT DISTINCT s.attrs['request.id'] FROM read_parquet(?) v JOIN series s USING(series_id)", [path]).fetchall()}
                                expected = {rid for signal, rid in acknowledged if signal == "metrics"}
                                self.assertTrue(expected.issubset(actual), f"missing acknowledged {dataset} points")
```

The RSS assertion includes the measured warm baseline, per-worker formula (including Q6's accepted extra B for block-sized series sealing), and a declared 128MiB envelope for receiver/channel allocations and allocator retention. It is a finite regression bound; Q5 leaves allowance validation to plan 3. The code writes samples before post-recovery assertions. Alloy's producer queue is outside exporter RSS. No fixed-workspace bound is claimed.

Task 11 intentionally uses `configs/series-parquet.alloy` unchanged: `loki.source.file` reads the 12 lines written after `store.stop()`, the attributes processor inserts `e2e.source=alloy-file`, and the OTLP exporter retries until storage returns. The complete task 10 `verify_readers` code runs here too: ClickHouse's latest-descriptor join and DuckDB must agree on every body/attribute pair, raw and joined row counts, and metric request ID. Duplicates are permitted after retries. Alloy does not expose per-line OTLP acknowledgements to the Python test; eventual file coverage proves Alloy delivery, while synthetic clients prove the ACK contract separately.

For review, these are the full shared producer and reader inputs used by the outage above. They are identical to task 10; retain one `configs/series-parquet.alloy` and one helper definition in `test_e2e.py`, rather than appending duplicate definitions. The prerequisite helpers/imports are already provided in task 10.

```river
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
loki.source.file "series" {
  targets = [{ __path__ = "/input/events.log", job = "series-e2e" }]
  forward_to = [otelcol.receiver.loki.series.receiver]
  tail_from_end = false
}

otelcol.receiver.loki "series" {
  output {
    logs = [otelcol.processor.attributes.series.input]
  }
}

otelcol.processor.attributes "series" {
  action {
    key = "e2e.source"
    value = "alloy-file"
    action = "insert"
  }
  output {
    logs = [otelcol.exporter.otlp.series.input]
  }
}

otelcol.exporter.otlp "series" {
  timeout = "6s"
  client {
    endpoint = sys.env("OTLP_ENDPOINT")
    compression = "none"
    tls {
      insecure = true
    }
  }
  sending_queue {
    enabled = true
    num_consumers = 1
    queue_size = 128
  }
  retry_on_failure {
    enabled = true
    initial_interval = "200ms"
    max_interval = "1s"
    max_elapsed_time = "0s"
  }
}
```

```python
@contextlib.contextmanager
def clickhouse_reader(root):
    root = Path(root).resolve()
    binary = require_clickhouse()
    container = None
    try:
        if binary:
            prefix = [binary]
        else:
            image = require_docker_image("clickhouse")
            name = "series-reader-" + uuid.uuid4().hex
            container = subprocess.check_output(
                ["docker", "run", "--pull=never", "--detach", "--name", name,
                 "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
                 "--mount", f"type=bind,src={root},dst=/data,readonly",
                 "--entrypoint", "/bin/sleep", image, "infinity"],
                text=True, timeout=30).strip()
            prefix = ["docker", "exec", "--workdir", "/data", container, "clickhouse", "local"]

        def query(sql):
            result = subprocess.run(
                prefix + ["--query", sql + " FORMAT JSONCompactEachRow",
                          "--output_format_json_quote_64bit_integers=0"],
                cwd=root, check=True, capture_output=True, text=True, timeout=30)
            return [tuple(json.loads(line)) for line in result.stdout.splitlines() if line.strip()]

        yield query
    finally:
        if container:
            subprocess.run(["docker", "rm", "--force", "--volumes", container],
                           check=False, capture_output=True, timeout=20)


def sql_string(value):
    return "'" + str(value).replace(chr(92), chr(92) * 2).replace("'", "''") + "'"


def verify_readers(test, root, log_ids, metric_count, allow_duplicates=False,
                   alloy_ids=(), metric_ids=()):
    root = Path(root).resolve()
    alloy_ids = set(alloy_ids)
    with duckdb.connect() as db, clickhouse_reader(root) as clickhouse:
        for signal, dataset in (("logs", "values"), ("metrics", "number"), ("metrics", "histogram")):
            relative = f"v=1/signal={signal}/dataset={dataset}/**/*.parquet"
            paths = sorted(root.glob(relative))
            expected_count = len(log_ids) if signal == "logs" else metric_count
            if not paths:
                test.assertEqual(expected_count, 0, f"missing {signal}/{dataset}")
                continue
            series = f"v=1/signal={signal}/dataset=series/**/*.parquet"
            duck_values = f"read_parquet({sql_string(root / relative)}, union_by_name=true)"
            duck_series = f"read_parquet({sql_string(root / series)}, union_by_name=true, filename=true)"
            ch_values = f"file({sql_string(relative)}, 'Parquet')"
            ch_series = f"file({sql_string(series)}, 'Parquet')"
            projection = ("v.body, coalesce(v.attrs['e2e.source'], '')" if signal == "logs"
                          else "s.attrs['request.id']")
            duck_sql = f"""
                WITH canonical AS (
                    SELECT * FROM {duck_series}
                    QUALIFY row_number() OVER (
                        PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1
                )
                SELECT {projection} FROM {duck_values} v
                INNER JOIN canonical s ON v.series_id = s.series_id
            """
            ch_sql = f"""
                WITH canonical AS (
                    SELECT * FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
                        FROM {ch_series}
                    ) WHERE rank = 1
                )
                SELECT {projection} FROM {ch_values} AS v
                INNER JOIN canonical AS s ON v.series_id = s.series_id
            """
            duck_rows = sorted(db.execute(duck_sql).fetchall())
            ch_rows = sorted(clickhouse(ch_sql))
            test.assertEqual(ch_rows, duck_rows, f"reader disagreement for {signal}/{dataset}")
            before = db.execute(f"SELECT count(*) FROM {duck_values}").fetchone()[0]
            ch_before = int(clickhouse(f"SELECT count(*) FROM {ch_values}")[0][0])
            test.assertEqual(ch_before, before)
            test.assertEqual(len(duck_rows), before, "latest-descriptor join must preserve cardinality")
            if allow_duplicates:
                test.assertGreaterEqual(before, expected_count)
            else:
                test.assertEqual(before, expected_count)
            if signal == "logs":
                expected = sorted((body, "alloy-file" if body in alloy_ids else "") for body in log_ids)
                if allow_duplicates:
                    test.assertEqual(set(duck_rows), set(expected))
                else:
                    test.assertEqual(duck_rows, expected)
            elif metric_ids:
                test.assertEqual({row[0] for row in duck_rows}, set(metric_ids))
```

- [ ] **Step 2: Establish the red test against an intentionally disabled retry implementation**

```bash
cd rust/otap-dataflow
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet,aws
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py OutageSlice -v
```

If it passes immediately, prove the test's sensitivity by temporarily making the flush failure arm queue Ack, run the test and observe missing acknowledged data, then restore the arm before continuing. The exact one-line mutation is `Outcome::Storage` to `Outcome::Ack` in Worker::complete's store-failure arm only. Never commit the mutation. Alternatively run the new test before task 8 on a separate execution checkout; do not mutate a shared tree occupied by another implementer.

- [ ] **Step 3: Exercise actual backpressure and correct any failures at their owners**

The test supplies the feature here; no additional retry queue, WAL or producer deduplication is introduced. Its expected behavior follows from these exact implemented paths:

```text
store stopped -> FlushJob retries/cancels at its absolute deadline
ACTIVE full or pending occupied -> recv_when(false)
bounded pdata channel full -> receiver channel send waits
receiver timeout/admission cap -> retryable timeout/RESOURCE_EXHAUSTED
store restarted -> fresh blocks commit -> producer retries return OK
```

For a test failure, use the following concrete diagnostics before changing behavior:

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet failed_descriptor_does_not_poison_cache
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet notification_survives_cancelled_poll
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet one_pending_request_resumes_before_new_input
```

Do not relax row coverage, permanent/retryable classification or budget assertions to turn a failure green. Fix the responsible function from tasks 3-9 and rerun its focused test plus OutageSlice. Rows from nacked/timeout requests may exist and be duplicated; the assertion intentionally compares acknowledged IDs as a subset of stored IDs.

- [ ] **Step 4: Verify recovery and commit**

```bash
cd rust/otap-dataflow
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py OutageSlice -v
cd ../..
python3 tools/sanitycheck.py
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py
git commit -m "test(series_parquet): retain acknowledged data across a real storage outage

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 12: Configuration contract, user documentation, changelog and final validation

**Files:**
- Modify: `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/{README.md,tests.rs}`
- Modify: `rust/otap-dataflow/crates/core-nodes/README.md`
- Create: `rust/otap-dataflow/.chloggen/series-parquet-exporter.yaml`, copied from TEMPLATE.yaml

**Interfaces:**
- Consumes: `Config: Deserialize`, runnable `configs/series-parquet-{local,s3}.yaml`, `df_engine --validate-and-exit`, unittest E2E suite, all worker/process metrics from task 7.
- Produces: complete public operating contract, the exact tested Alloy + df_engine + MinIO reference deployment, both reader commands, and a validated release note. Q1 retains `SERIES_TRACKING_ISSUE=4128` as the explicitly identified temporary tracking reference used in plan 1; replace it with the real PR number when the PR is opened.

- [ ] **Step 1: Write failing contract tests before replacing the initial README**

Append to `tests.rs`:

```rust
/// Scenario: configuration contains misspelled settings or cross-field violations.
/// Guarantees: the factory's typed validation rejects them before any network listener starts.
#[test]
fn startup_rejects_invalid_configuration() {
    let base = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-config"}}});
    let cases = [
        ("window", serde_json::json!({"interval": "0s"})),
        ("window", serde_json::json!({"interval": "500ms"})),
        ("window", serde_json::json!({"max_requests_per_block": 0})),
        ("window", serde_json::json!({"max_block_bytes": "1MiB"})),
        ("ingress", serde_json::json!({"max_row_bytes": "3MiB"})),
        ("ingress", serde_json::json!({"max_requset_bytes": "1MiB"})),
        ("upload", serde_json::json!({"concurrency": 0})),
        ("upload", serde_json::json!({"part_bytes": "1MiB"})),
        ("parquet", serde_json::json!({"compression": "snappy"})),
        ("metrics", serde_json::json!({"values_sort": ["value_int"]})),
        ("logs", serde_json::json!({"denormalize": [{"path": "resource.x", "column": "SERIES_ID"}]})),
        ("logs", serde_json::json!({"denormalize": ["unknown.x"]})),
        ("series_cache", serde_json::json!({"max_entries": 0})),
        ("notify_batch", serde_json::json!(0)),
    ];
    for (section, value) in cases {
        let mut candidate = base.clone();
        candidate[section] = value;
        assert!(serde_json::from_value::<Config>(candidate).is_err(), "section={section}");
    }
}
/// Scenario: operators read the component README before configuring durable ingest.
/// Guarantees: documentation states retry, shutdown, memory and unsupported-signal contracts.
#[test]
fn readme_states_operating_contract() {
    let readme = include_str!("README.md");
    for phrase in ["at-least-once", "wait_for_result", "timeout_secs=180", "core_allocation",
        "memory.unaccounted_rss_bytes", "exponential histograms", "union_by_name", "mergeSchema",
        "schema_fingerprint", "notify_batch", "No block-atomic snapshot", "Alloy", "MinIO",
        "ClickHouse", "loki.source.file", "e2e.source", "row_number()", "SERIES_REQUIRE_DOCKER"] {
        assert!(readme.contains(phrase), "missing documented contract: {phrase}");
    }
    let alloy = include_str!("../../../../../configs/series-parquet.alloy");
    assert!(readme.contains(alloy), "README must reproduce the exact tested River config");
}
```

- [ ] **Step 2: Verify the documentation contract is red**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet readme_states_operating_contract
```

Expected: the initial small README lacks the full contract. Startup validation cases should pass against the validated adapter and guard against future schema drift.

- [ ] **Step 3: Replace the component README with the complete operating contract**

````markdown
# Series Parquet exporter

`exporter:series_parquet` writes logs and metric number/histogram points as
series descriptors plus narrow values datasets. Enable Cargo feature
`series_parquet`; add `aws` for S3. The component URN is
`urn:otel:exporter:series_parquet` and metric scope is
`exporter.series_parquet`.

Use `configs/series-parquet-local.yaml` for a local destination and
`configs/series-parquet-s3.yaml` for S3-compatible storage. From the
`rust/otap-dataflow` workspace:

```bash
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet,aws
mkdir -p /tmp/series-parquet
./target/debug/df_engine --config configs/series-parquet-local.yaml --validate-and-exit
./target/debug/df_engine --config configs/series-parquet-local.yaml --http-admin-bind 127.0.0.1:8080
```

Both examples explicitly set `core_allocation` to one core. Each pipeline
instance is a separate worker with a separate budget and cache. Increase
cores only after multiplying the memory estimate below by worker count.

## Reference deployment: Alloy + df_engine + MinIO

The reference topology is a file producer in Docker Alloy, the host's
`df_engine` with its OTLP gRPC receiver, and a Docker MinIO destination:

```text
/input/events.log -> Alloy -> OTLP gRPC -> df_engine -> MinIO Parquet
                                                       |
                                             downloaded object snapshot
                                                       |
                                               DuckDB + ClickHouse
```

Run this example on Linux. Alloy uses host networking to reach the engine's
loopback listener; MinIO publishes an ephemeral loopback-only S3 port. The
engine YAML is generated from `configs/series-parquet-local.yaml` with S3
storage settings equivalent to `configs/series-parquet-s3.yaml`, an explicit
one-core allocation and `wait_for_result: true`. Test helpers write the exact
launched YAML into `SERIES_REFERENCE_DIR/pipeline.yaml` and remove only their
own containers afterward. This example keeps the downloaded Parquet files
for the two-reader check below.

The complete River configuration in `configs/series-parquet.alloy` is shared
with the normal and outage tests. `/input` is the mounted producer directory;
`OTLP_ENDPOINT` is the engine's `127.0.0.1:<grpc_port>` passed by the launcher.
The inserted `e2e.source` attribute is verified alongside each log body:

```river
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
loki.source.file "series" {
  targets = [{ __path__ = "/input/events.log", job = "series-e2e" }]
  forward_to = [otelcol.receiver.loki.series.receiver]
  tail_from_end = false
}

otelcol.receiver.loki "series" {
  output {
    logs = [otelcol.processor.attributes.series.input]
  }
}

otelcol.processor.attributes "series" {
  action {
    key = "e2e.source"
    value = "alloy-file"
    action = "insert"
  }
  output {
    logs = [otelcol.exporter.otlp.series.input]
  }
}

otelcol.exporter.otlp "series" {
  timeout = "6s"
  client {
    endpoint = sys.env("OTLP_ENDPOINT")
    compression = "none"
    tls {
      insecure = true
    }
  }
  sending_queue {
    enabled = true
    num_consumers = 1
    queue_size = 128
  }
  retry_on_failure {
    enabled = true
    initial_interval = "200ms"
    max_interval = "1s"
    max_elapsed_time = "0s"
  }
}
```

The six-second Alloy attempt timeout intentionally exercises producer retry
in the outage fixture. For this example the engine window is one second.
With production defaults of a 15s window and 60s flush deadline, give producer
attempts the 180s operating timeout described below to avoid routine retries.
The sending queue and positions are producer state; the exporter retains no
WAL or spill state. The fixture's 12 lines fit the 128-request Alloy queue.

MinIO defaults to `minio/minio:RELEASE.2025-04-22T22-12-26Z`, RustFS to
`rustfs/rustfs:1.0.0-rc.3`, and the ClickHouse fallback to
`clickhouse/clickhouse-server:26.7.4`. Set `SERIES_MINIO_IMAGE`,
`SERIES_RUSTFS_IMAGE`, or `SERIES_CLICKHOUSE_IMAGE` to another locally present
tag. Alloy defaults to `grafana/alloy:v1.19.2` (stable); `SERIES_ALLOY_IMAGE`
can override it, and only this image may be pulled. Missing Docker/images
skip locally unless `SERIES_REQUIRE_DOCKER=1`; startup or reader failures
always fail. CI provisioning and digest pins are separate plan 3 work.

From `rust/otap-dataflow`, after the feature-enabled build above:

```bash
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install -r crates/validation/tests/series_parquet/requirements.txt
export SERIES_REFERENCE_DIR="$(mktemp -d /tmp/series-reference.XXXXXX)"
PYTHONPATH=crates/validation/tests/series_parquet /tmp/series-parquet-venv/bin/python - <<'PY'
import os
from pathlib import Path
from test_e2e import AlloyProducer, DockerStore, Engine, require_clickhouse, wait_for_alloy

require_clickhouse()
root = Path(os.environ["SERIES_REFERENCE_DIR"])
ids = [f"reference-alloy-{i}" for i in range(12)]
with DockerStore("minio") as store, Engine(root, storage=store.storage) as engine:
    with AlloyProducer(root, engine) as alloy:
        alloy.write(ids)
        wait_for_alloy(store, root, ids)
    engine.shutdown(seconds=180)
    store.download(root / "downloaded")
print(root / "pipeline.yaml")
print(root / "downloaded")
PY
```

Save the complete Python verification below as `/tmp/series-reference-readers.py`
and run it with `/tmp/series-parquet-venv/bin/python /tmp/series-reference-readers.py -v`
in the same terminal. It independently runs DuckDB and native clickhouse-local
(or `docker exec` in the selected local ClickHouse image), checks all 12 bodies
and their `e2e.source` attributes, and proves the latest-descriptor join preserves
row counts. Native ClickHouse is preferred at `/usr/bin/clickhouse-local`;
`SERIES_CLICKHOUSE_LOCAL` can select another executable. Missing both reader
routes skips locally; a present reader that cannot execute the query fails.

```python
# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
import contextlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest
import uuid
import duckdb


def reader_unavailable(reason):
    if os.environ.get("SERIES_REQUIRE_DOCKER") == "1":
        raise AssertionError(reason)
    raise unittest.SkipTest(reason)


@contextlib.contextmanager
def reference_clickhouse(root):
    binary = os.environ.get("SERIES_CLICKHOUSE_LOCAL", "/usr/bin/clickhouse-local")
    if not (Path(binary).is_file() and os.access(binary, os.X_OK)):
        binary = shutil.which("clickhouse-local") if "SERIES_CLICKHOUSE_LOCAL" not in os.environ else None
    container = None
    try:
        if binary:
            command = [binary]
        else:
            if not shutil.which("docker"):
                reader_unavailable("Neither clickhouse-local nor Docker is available")
            try:
                probe = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
            except subprocess.TimeoutExpired:
                reader_unavailable("ClickHouse fallback Docker daemon did not respond")
            if probe.returncode:
                reader_unavailable("ClickHouse fallback Docker daemon is unavailable")
            image = os.environ.get("SERIES_CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server:26.7.4")
            if subprocess.run(["docker", "image", "inspect", image], capture_output=True, timeout=10).returncode:
                reader_unavailable(f"Local ClickHouse image is absent: {image}")
            container = subprocess.check_output(
                ["docker", "run", "--pull=never", "--detach", "--network", "none",
                 "--name", "series-reference-reader-" + uuid.uuid4().hex,
                 "--user", f"{os.getuid()}:{os.getgid()}",
                 "--mount", f"type=bind,src={root},dst=/data,readonly",
                 "--entrypoint", "/bin/sleep", image, "infinity"], text=True, timeout=30).strip()
            command = ["docker", "exec", "--workdir", "/data", container, "clickhouse", "local"]

        def query(sql):
            result = subprocess.run(
                command + ["--query", sql + " FORMAT JSONCompactEachRow",
                           "--output_format_json_quote_64bit_integers=0"],
                cwd=root, text=True, capture_output=True, check=True, timeout=30)
            return [tuple(json.loads(line)) for line in result.stdout.splitlines() if line.strip()]
        yield query
    finally:
        if container:
            subprocess.run(["docker", "rm", "--force", "--volumes", container],
                           capture_output=True, check=False, timeout=20)


class ReferenceReaders(unittest.TestCase):
    # Scenario: Docker Alloy delivers 12 known file lines through df_engine into MinIO Parquet.
    # Guarantees: both readers preserve latest-descriptor join counts and return every expected body and attribute.
    def test_latest_descriptor_join_and_bodies(self):
        root = (Path(os.environ["SERIES_REFERENCE_DIR"]) / "downloaded").resolve()
        values = "v=1/signal=logs/dataset=values/**/*.parquet"
        series = "v=1/signal=logs/dataset=series/**/*.parquet"
        expected = sorted((f"reference-alloy-{i}", "alloy-file") for i in range(12))
        with duckdb.connect() as db, reference_clickhouse(root) as clickhouse:
            duck_rows = sorted(db.execute("""
                WITH canonical AS (
                    SELECT * FROM read_parquet(?, union_by_name=true, filename=true)
                    QUALIFY row_number() OVER (
                        PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) = 1
                )
                SELECT v.body, coalesce(v.attrs['e2e.source'], '')
                FROM read_parquet(?, union_by_name=true) v
                INNER JOIN canonical s ON v.series_id = s.series_id
            """, [str(root / series), str(root / values)]).fetchall())
            ch_rows = sorted(clickhouse(f"""
                WITH canonical AS (
                    SELECT * FROM (
                        SELECT *, row_number() OVER (
                            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
                        FROM file('{series}', 'Parquet')
                    ) WHERE rank = 1
                )
                SELECT v.body, coalesce(v.attrs['e2e.source'], '')
                FROM file('{values}', 'Parquet') AS v
                INNER JOIN canonical AS s ON v.series_id = s.series_id
            """))
            duck_count = db.execute("SELECT count(*) FROM read_parquet(?)", [str(root / values)]).fetchone()[0]
            ch_count = int(clickhouse(f"SELECT count(*) FROM file('{values}', 'Parquet')")[0][0])
            self.assertEqual(duck_count, 12)
            self.assertEqual(ch_count, duck_count)
            self.assertEqual(len(ch_rows), ch_count)
            self.assertEqual(len(duck_rows), duck_count)
            self.assertEqual(ch_rows, duck_rows)
            self.assertEqual(duck_rows, expected)


if __name__ == "__main__":
    unittest.main()
```

## Delivery and shutdown

Connect `receiver:otlp` directly to this exporter with
`protocols.grpc.wait_for_result: true` and `timeout: 180s`. An OK response
means every file for that request's block completed in the object store.
Content/schema/budget rejections are permanent INVALID_ARGUMENT responses.
Storage and shutdown failures are retryable. Producers retain and retry
requests on transient failure or timeout. Delivery is at-least-once:
receiver timeouts and retries can produce duplicates even if an earlier
copy committed. Producer disconnect does not remove accepted rows.

The receiver timeout must cover channel residence, one preceding flush,
one window interval, its own flush and completion delivery. With 15s
windows and a 60s flush deadline, 180s allows margin but cannot eliminate
all timeouts under sustained backpressure. A full bounded channel makes
producers wait; exhausted receiver admission slots return
RESOURCE_EXHAUSTED.

No block-atomic snapshot is provided. Series files complete before their
values files, but readers can observe a subset of a block, including rows
from a request that was later nacked. Read values first, then descriptors.
The exporter has no WAL or spill. A local filesystem path is a final
storage backend, not recovery state. Restart creates a new boot UUID.

Engine signal shutdown currently grants 60s. Use the existing admin
shutdown operation to grant the example's explicit 180s drain deadline:

```bash
curl -X POST 'http://127.0.0.1:8080/api/v1/groups/shutdown?wait=true&timeout_secs=180'
```

There is no pipeline YAML shutdown-deadline key. Receivers drain before
exporters receive Shutdown, so allow more than window.interval plus
flush_retry_deadline plus upload/notification margin. Supervisors must
allow the drain plus upload.abort_timeout cleanup. At a deadline,
uncommitted requests are nacked; a closed/full completion channel can
prevent delivery, in which case the producer retries its timeout.
Committed blocks are never re-exported merely because notification failed.

## Configuration

Byte sizes accept integers or IEC strings such as `16MiB`. Unknown fields
are errors. Durations use humantime strings. Window intervals are positive
whole seconds. ZSTD is the supported compression. Defaults:

| Setting | Default |
| --- | --- |
| window.interval | 15s |
| window.max_block_bytes | 500MiB |
| window.max_requests_per_block | 4096 |
| window.flush_retry_deadline | 60s |
| ingress.max_request_bytes | 16MiB |
| ingress.max_extracted_bytes | 32MiB |
| ingress.max_row_bytes | 1MiB |
| ingress.max_nesting_depth | 32 |
| series_cache.max_entries | 200000 |
| sorting.enabled | true |
| sorting.run_target_bytes | 8MiB |
| sorting.merge_chunk_bytes | 16MiB |
| upload.part_bytes / concurrency / abort_timeout | 8MiB / 2 / 5s |
| parquet.row_group_bytes / writer_limit_bytes | 64MiB / 96MiB |
| notify_batch | 64 |
| unsupported | reject |

Row bytes must be at most one quarter of run bytes. Block bytes must be
at least extracted-request bytes. S3 part bytes must be at least 5MiB.
Request counts, byte/depth budgets, cache capacity, upload concurrency,
notify_batch and abort/retry durations must be positive. A logical input
size that cannot be measured is refused before conversion. Retry settings
apply to individual storage operations; flush_retry_deadline is the
absolute authority for retrying a whole sealed block.

`writer_id` must be nonempty and contain no slash. `producer_id_attribute`
defaults to `host.id`, projects a resource attribute, and does not alter
identity membership: all resource attributes remain in the series hash.
Transport-header producer IDs are unsupported.

Logs use `logs.series_attributes` for record attributes in identity;
metric identity includes point attributes and metric type/temporality.
`denormalize` accepts a path shorthand or an object with path, column and
type (`string`, `int64`, `double`, `bool`). Prefixes are `resource.`,
`scope.` and `attrs.`. Physical names must not collide case-insensitively
with intrinsic or other configured columns. Wrong non-string types become
null and increment `denormalize.type_mismatch{column}`.

`series_id, time_unix_nano` is the default values sort, providing locality
and compression. For queries filtering service or environment, place that
denormalized physical column first, for example
`service_name, series_id, time_unix_nano`; this improves row-group pruning
at some cost in per-series locality. Sort keys must exist in every values
dataset of that signal. Null placement defaults to last. Series files
always sort by series_id. Disabling values sorting preserves schemas and
delivery guarantees. Byte/request rotations inside one window repeat that
block's descriptors, increasing series volume under sustained throughput.

V1 limitations: exponential histograms and summaries are rejected by
default; `unsupported: drop` drops those points and counts them. Exemplars
are always dropped. Traces are always refused. A request with only dropped
rows is acknowledged immediately; mixed supported/dropped requests wait
for commit. Unspecified sum/histogram temporality, duplicate attribute
keys, excessive nesting, invalid histogram list lengths, and counts above
INT64_MAX are refused atomically. Integer values remain INT64. Zero or
absent timestamps become null; negative converted timestamps become null
and increment timestamp.out_of_range.

## Memory and telemetry

Per worker, let B be max_block_bytes, E max_extracted_bytes, C cache entries,
N max_requests_per_block, and T the measured retained completion-token size.
The retained-data bound is 2B + E + 128C + 2NT. Only ACTIVE and FLUSHING
exist; one extracted request may wait in the pending slot. Normal admission
also reserves notification credit, capped at 2N-1 total live tokens, with
one extra credit for observing force-drained shutdown input.

Construction workspace allowance is 4 * max_request_bytes for conversion,
2 * run_target_bytes for sorting, 2 * merge_chunk_bytes for merge/encoding,
3 * writer_limit_bytes for writer plus encoder transient, and
part_bytes * (concurrency + 1) + merge_chunk_bytes for upload. A 64MiB
fixed implementation/configuration allowance covers container and allocator
overhead. The current core also needs a block-sized descriptor-sealing
allowance: it retains pending descriptors while constructing their Arrow
batches. Budget one extra B for this accepted v1 allowance (plan 1 F-park;
FORMAT.md limitations). No fixed-workspace bound is claimed. These
are finite engineering allowances, not validated allocation multipliers;
expansion-factor measurement belongs to the separate benchmark plan.

Process planning bound is the sum of worker bounds plus receiver/channel
memory, the engine baseline and allocator retention. Input bytes before
admission belong to receiver/channel limits and are outside exporter-owned
accounting. The short outage test permits measured warm RSS plus the worker
bound plus 128MiB for those receiver/allocator terms; this is a regression
envelope, not a portable allocator guarantee.

memory.accounted_bytes reports retained exporter data and measured token
allocations, with the cache-entry estimate. memory.budget_bytes reports the
configured retained/workspace allowance. The once-per-process metric
memory.unaccounted_rss_bytes is max(0, RSS - sum(worker accounted bytes));
normal conversion/encoding scratch and allocator overhead appear in this
residual. Watch trends as well as absolute values.

The exporter reports cache entries/hits/misses/evictions; active/flushing
bytes, pending requests and pending-slot occupancy; flush reason, duration,
failures, retries and cancellations; written rows/files per dataset;
descriptor emission reasons; ack/nack decisions and notification queue/
failures; oldest_unacked_seconds; unsupported-kind, timestamp and
per-column mismatch counters. Datasets have signal-qualified labels such
as logs_values and metrics_histogram. Nack reasons are storage, too_large,
invalid, unsupported and shutdown. Metric labels never include request IDs.

The target control responsiveness bound is one admitted request, one
notify_batch and one merge chunk of CPU work. The current synchronous
whole-block descriptor seal adds block-sized work under the accepted v1
allowance; the target bound does not apply independently of block size.
Deadline cancellation adds at most
upload.abort_timeout for best-effort multipart cleanup. Configure bucket
lifecycle removal of incomplete uploads left by a crash or cancellation
during Parquet finalization.

## Reading and schema changes

Use DuckDB 1.1 or newer. Values reference repeated series descriptors, so
join through a canonical view:

```sql
CREATE VIEW series AS
SELECT * FROM read_parquet(
  '/tmp/series-parquet/v=1/signal=logs/dataset=series/**/*.parquet',
  hive_partitioning=true, union_by_name=true, filename=true)
QUALIFY row_number() OVER
  (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC)=1;
SELECT v.*, s.resource_attrs FROM read_parquet(
  '/tmp/series-parquet/v=1/signal=logs/dataset=values/**/*.parquet',
  hive_partitioning=true, union_by_name=true) v
JOIN series s USING(series_id);
```

For Spark 3.5 or newer, use mergeSchema and a canonical row_number view:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
series_path = "/tmp/series-parquet/v=1/signal=logs/dataset=series"
values_path = "/tmp/series-parquet/v=1/signal=logs/dataset=values"
series = spark.read.option("mergeSchema", "true").parquet(series_path)
series = series.withColumn("filename", F.input_file_name())
order = Window.partitionBy("series_id").orderBy(F.desc("emitted_at"), F.desc("filename"))
series = series.withColumn("rank", F.row_number().over(order)).where("rank = 1").drop("rank")
values = spark.read.option("mergeSchema", "true").parquet(values_path)
joined = values.join(series, "series_id")
```

Here series_path and values_path are the complete Hive dataset paths in
one base URI. Adding nullable/denormalized columns is supported with
union_by_name or mergeSchema. Changing an existing column's type, path or
meaning requires a new base URI. Inspect schema_fingerprint alongside the
physical column schema to find incompatible mixtures:

```sql
SELECT file_name, decode(value) AS schema_fingerprint
FROM parquet_kv_metadata('/tmp/series-parquet/**/*.parquet')
WHERE decode(key) = 'schema_fingerprint';
SELECT file_name, name, type, logical_type
FROM parquet_schema('/tmp/series-parquet/**/*.parquet');
```

Different fingerprints can mean a supported additive change; compare
physical columns before concluding incompatibility. The normative format
is in `crates/series-lake/docs/FORMAT.md`.
````

The outer Markdown fence in this plan is documentation content; when copying the README, preserve its inner bash/sql/python fences, and remove only the plan's outer fence. Add this exact link to the core-nodes README's exporter list:

```markdown
- [Series Parquet](src/exporters/series_parquet/README.md): bounded logs and
  metrics storage with durable OTLP acknowledgements.
```

- [ ] **Step 4: Copy and populate the release-note template**

Run from repository root. Q1 explicitly authorizes the temporary `SERIES_TRACKING_ISSUE` value 4128, matching plan 1's `.chloggen/series-lake-core.yaml` comment. Replace it with the real PR number when the PR is opened and rerun `make chlog-validate`:

```bash
export SERIES_TRACKING_ISSUE="${SERIES_TRACKING_ISSUE:-4128}"
cp rust/otap-dataflow/.chloggen/TEMPLATE.yaml rust/otap-dataflow/.chloggen/series-parquet-exporter.yaml
python3 - <<'PY'
import os
from pathlib import Path
issue = int(os.environ['SERIES_TRACKING_ISSUE'])
if issue <= 0:
    raise SystemExit('SERIES_TRACKING_ISSUE must be a positive tracking number')
p = Path('rust/otap-dataflow/.chloggen/series-parquet-exporter.yaml')
s = p.read_text()
s = s.replace('change_type:\n', 'change_type: new_component\n')
s = s.replace('component:\n', 'component: pipeline\n')
s = s.replace('note:\n', 'note: Add a series Parquet exporter for logs and metrics with bounded buffering and durable OTLP acknowledgements.\n')
s = s.replace('issues: []', f'# Temporary SERIES_TRACKING_ISSUE=4128; replace with the real PR number when opened.\nissues: [{issue}]')
s = s.replace('subtext:\n', 'subtext: |\n  Use exporter:series_parquet with a directly connected OTLP receiver and wait_for_result enabled. Local files and S3-compatible destinations are supported. Producers must retry timeouts and transient failures; duplicates are possible.\n')
p.write_text(s)
PY
make chlog-validate
```

Both note and subtext fit the 200/300-character limits and describe end-user behavior. The only authorized temporary reference is Q1's `SERIES_TRACKING_ISSUE=4128`; do not invent another reference or omit the user-facing entry.

- [ ] **Step 5: Run final checks and the real-process suite**

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet startup_rejects_invalid_configuration
cargo test -p otel-arrow-dfe-core-nodes --features series_parquet readme_states_operating_contract
cargo check --no-default-features --features otlp,crypto-ring
cargo check --no-default-features --features otlp,series_parquet,crypto-ring
cargo build -p otel-arrow-dfe --bin df_engine --features series_parquet,aws
./target/debug/df_engine --config configs/series-parquet-local.yaml --validate-and-exit
./target/debug/df_engine --config configs/series-parquet-s3.yaml --validate-and-exit
/tmp/series-parquet-venv/bin/python crates/validation/tests/series_parquet/test_e2e.py -v
cargo xtask check
cd ../..
npx markdownlint-cli2 rust/otap-dataflow/crates/core-nodes/README.md rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/README.md docs/superpowers/plans/2026-09-21-series-parquet-exporter-node.md
python3 tools/sanitycheck.py
make chlog-validate
```

Expected: full xtask check (structure, fmt, all-target clippy, workspace tests) passes, both sample configs validate, Local/metrics/shutdown/restart/Docker/outage tests pass. Docker/reader absence is visible as a skip locally and fails when SERIES_REQUIRE_DOCKER=1; plan 3 supplies the CI lane. The feature-disabled build proves the optional series-lake dependency is not linked accidentally. Do not claim these checks were executed during planning.

- [ ] **Step 6: Commit**

```bash
git add rust/otap-dataflow/crates/core-nodes/README.md rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/README.md rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/tests.rs rust/otap-dataflow/.chloggen/series-parquet-exporter.yaml
git commit -m "docs(series_parquet): document durable delivery, budgets and operation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

---

## Out of scope for this plan

- Benchmarks, expansion-factor measurements, performance gates and broad qualification: plan 3.
- Live buffer/tail/series introspection HTTP endpoints: spec 10.1. Existing engine admin metrics/shutdown clients do not add such an API.
- Compaction, discovery indexes, manifests, producer replay IDs and trace storage: spec 10.2.
- Toxiproxy, failpoint/SIGKILL matrices, nightly or multi-day chaos/soak and the canary gate: spec 10.3. The one bounded v1 outage case remains in task 11.
- Changes to the existing parquet exporter, including its age/row-trigger bug.
- Engine shutdown-timeout changes (Q3), CI image provisioning/digest pins (Q2), and incremental series sealing (Q6).

## Amendments (2026-09-21)

- A01: The requested section 3.3 is absent in the binding design revision. The architecture boundary is section 3.2, with the core in 3.1.
- A02: Source, not the earlier core-plan prose, controls names: RequestTooLarge/BlockFull/TooManyRequests, structured Cancelled/AbortFailed, microsecond sealing and FlushReport.files.
- A03: Task 2 adds minimal Context and inbox accessors because the current engine API cannot strip claims, measure frame capacity, or expose a latched shutdown deadline. It does not change routing or force-drain behavior.
- A04: The adapter retains tokens beside Block<()> so a consuming/partially failing core admission or a cancelled flush cannot silently lose a completion context. Its reservation still charges real token bytes.
- A05: Normal live-token admission reserves one shutdown observation credit; the final notification bound remains 2 * max_requests_per_block even with a full completion channel and forced pdata drain.
- A06: The core statistics need bounded per-column/per-kind details, and byte/request rotation needs explicit descriptor re-emission. Task 7 adds these APIs without changing existing reserve callers.
- A07: Signal shutdown is fixed at 60s in the engine. Examples use the existing admin API's explicit 180s deadline; no unsupported YAML key is invented.
- A08: Metrics number and histogram support is in this plan, per the task request, even though the design lists metrics as the next implementation-order step.
- A09: MinIO/RustFS/ClickHouse use the ruled local image tags with --pull=never and environment overrides. Only Alloy may be pulled. SERIES_REQUIRE_DOCKER=1 turns prerequisite skips into failures; CI provisioning/pinning is plan 3.
- A10: RSS accounting separates measured retained allocations from reserved construction workspace. The short outage envelope is documented; plan 3 still owns empirical expansion-factor validation.
- A11: Q6 accepts whole-descriptor materialization at `rust/otap-dataflow/crates/series-lake/src/buffer.rs:340` as the v1 F-park allowance documented in FORMAT.md. Budget one extra B and include synchronous block-sized sealing in control latency. No fixed-workspace bound is claimed; plan 3 validates the allowances.

## Self-Review

### Spec coverage

Accepted v1 allowance: Q6 retains synchronous block-sized descriptor sealing (plan 1 F-park; FORMAT.md limitations). Tasks 7/12 budget an additional B and include its control-latency cost; no fixed-workspace bound is claimed. Q5 assigns allowance measurement to plan 3. All section 7/8/9.2 integration requirements map below.

| Requirement | Task and check |
| --- | --- |
| 7.1 local exporter, factory, inventory, feature and typed configuration | 1; real df_engine slice and validate-and-exit |
| 7.1 ACTIVE/FLUSHING, task owns sealed data, no third block | 3; complete_files_before_ack, ownership split |
| 7.1 biased select, independent boundary sleep, re-arm while busy | 5; busy_rotation_rearms_boundary_sleep |
| 7.1 completion -> commit cache -> notifications -> release block | 3, 8; durable files and failure/hour/eviction tests |
| 7.1 pending resumes before new input; no pdata while pending/rotation | 4; one_pending_request_resumes_before_new_input |
| 7.1 PData from recv_when(false) is force-drained and retryably nacked | 2, 9; forced_pdata_exposes_shutdown_and_is_retryably_nacked |
| 7.1 Shutdown pending nack, two-block drain, deadline cancellation | 9; deadline_nacks_both_blocks_and_pending and saturated-inbox test |
| 7.1 dropping start cancels task; abort cleanup bounded | 3, 8, 9; dropping_worker_cancels_flush_task |
| 7.1 CollectTelemetry and terminal snapshots; other controls ignored | 7, 9; explicit collection branch; no exporter DrainIngress assumption |
| 7.1 receiver-first draining requires sufficient deadline | 1, 9, 12; real outstanding-request shutdown test and admin command |
| 7.2 no ACK before durable block, zero-output immediate ACK | 1, 3, 6; object existence and mixed/drop tests |
| 7.2 Refused permanent; store/shutdown retryable; timeout duplicates | 2, 6, 8, 11, 12; cause/permanent assertions and outage retry loop |
| 7.3 bounded channel/receiver backpressure | 4, 11; closed admission, waiting producers, retryable status checks |
| 7.4 complete YAML/storage/retry/writer/producer/window/ingress/cache/sort/upload/Parquet/notify/signals | 1, 10, 12; both runnable configs, startup negative cases |
| 7.4 all physical sort schemas, no configurable series sort | 1, 12; delegates LakeConfig::validate and rejects value_int for histogram |
| 7.4 sort guidance, same-window descriptor volume, explicit cores | 7, 12; reserve_with_reemit regression and README |
| 7.5 cache/block/pending metrics | 7; real gauge test and outage sampling |
| 7.5 reason-tagged flush count, duration, failures/retries/cancelled | 7, 8, 9; closed enums and lifecycle call sites |
| 7.5 rows/files per dataset and series emission reasons | 7; FlushReport-only committed writes and bounded reservation labels |
| 7.5 acks/nacks, queued/failed notifications, oldest age | 7, 9, 11; notifier decisions and recovery baseline |
| 7.5 unsupported kinds, mismatch columns, invalid timestamp counters | 6, 7; detailed extraction statistics |
| 7.5 memory budget/accounted and once-process RSS residual | 7, 11, 12; engine monitor aggregation/RAII and public formula |
| 8 input/extracted/row/whole-block oversize -> Refused | 1, 4, 6; pre-conversion size and core reservation/extraction tests |
| 8 conversion/schema/duplicate/depth/temporality/histogram/count errors | 4, 6; attribute preflight and atomic content-error tests |
| 8 unsupported reject, traces always reject | 6; real producer status assertions |
| 8 object failure retries until absolute deadline | 8; retry classifier, frozen names/bytes and descriptor failure tests |
| 8 encoding bugs do not retry; block nack and error event; keep running | 3, 8; no retry classifier and complete error branch |
| 8 shutdown nacks, force drain, cancellation | 9; engine inbox and both-block deadline tests |
| 8 notification delivery failure logs/counts, never re-exports | 2, 7, 9; persistent send, failure counter and abandonment |
| 9.2 ACK only after all files | 1, 3, 10; real backend readback plus engine completion harness |
| 9.2 failed descriptors re-emitted next block | 8; failed_descriptor_does_not_poison_cache |
| 9.2 hour crossing, same-series overlap, eviction before commit | 8; overlapping_series_and_eviction_preserve_partition_coverage and same_window_overlap_keeps_both_descriptors |
| 9.2 busy-boundary re-arm, controls, one rotation and pending precedence | 4, 5, 9; clock, pending and saturated-inbox tests |
| 9.2 admission immediately before/after boundary | 5; admission_time_assigns_exactly_one_window |
| 9.2 series succeeds/values fails, stable retry names/bytes | 8; values_retry_reuses_paths_and_bytes |
| 9.2 restart same window distinct names | 10; real process restart under one known long window |
| 9.2 producer disconnect cannot retract admitted data | 10; telemetry-confirmed admission before cancellation |
| 9.2 release original payload/conversion workspace, strip metadata | 2, 4; Context token tests and payload ownership regression |
| 9.2 full completion channel cannot stall boundary/shutdown | 2, 5, 9; blocked_completion_keeps_boundary_and_control_live and saturated deadline |
| 9.2 shutdown with both blocks, pending, deadline and drop safety | 9; shutdown_commits_both_blocks_before_deadline, deadline_nacks_both_blocks_and_pending, drop and real receiver-drain tests |
| 9.2 supported/dropped mixed requests, zero output, reject and traces | 6; MetricsSlice cases |
| 9.2 INT64_MAX and wrapped timestamps | 6; number_and_histogram DuckDB assertions |
| 9.2 saturated-inbox latency bound | 9; 2s short-fixture bound and general documented formula |
| 9.4 real producer/process/local then S3, logs and metrics | 1, 6, 10; Docker Alloy tails known lines; synthetic clients cover metrics/ACK details |
| E2E-1/2 Alloy realism and independent readers | 10, 11; full River config, native ClickHouse/docker exec code, latest-descriptor joins, counts/body/attribute agreement with DuckDB |
| E2E-3 reference deployment | 12; Alloy + df_engine + MinIO topology, exact shared River config and full two-reader verification test |
| 9.4 counts/partition coverage/sorting/identity/canonical join/additive schema | 10; verify_files and restart schema test |
| 9.5 outage longer than deadline, memory allowance, wait/retry/recovery/no missing ACKed IDs | 11; Alloy plus synthetic clients on both real Docker backends, two readers and the Q6 sealing allowance |

### Completeness and planning validation

Planning checks: 12 tasks retain Files, Interfaces, checkbox TDD steps, full code and commit commands with both required trailers. Current series-lake anchors were checked against the sources. The amended plan is ASCII; all Python fences and shell snippets are syntax-checked in memory. The required repository sanitycheck and Markdown lint pass after editing. These are planning checks, not compilation or E2E execution; no cargo builds, implementation tests, image pulls or Git mutations are performed in this session.

No unfinished implementation markers, omitted function bodies or undefined conceptual helpers are permitted. Every source addition, replacement body, fixture, command and commit trailer is written explicitly. Q1 explicitly retains the temporary tracking reference 4128, to be replaced by the real PR number when opened. No other unfinished implementation markers or omitted helpers remain. Tests/builds described here are future execution checks, not claimed planning results.

### Cross-task type consistency

- One `Config` maps the nested user YAML to one `LakeConfig`; `Window` scheduling config and `window::Window` runtime state have distinct module-qualified types.
- `AckToken` owns stripped Context + SignalType + monotonic received time; no token owns payload data. `OwnedBlock` owns `Block<()>` plus tokens, and FLUSHING splits those ownership responsibilities without adding a data block.
- `Pending` owns Extracted + AckToken + admission seconds. Re-reservation occurs against the eventual ACTIVE block. The same types appear in prepare, offer, resume, shutdown and accounting.
- `FlushDone` carries Block<()> + lake::Result<FlushReport> + attempts; FlushJob retains tokens, cancel handle, bytes and monotonic start. The task 8 constructor adds exactly one Duration argument, updated at its sole worker call site.
- `WindowClock` inputs/outputs are seconds; seal uses microseconds; wall source uses nanoseconds; `engine::clock` governs sleep/retry/shutdown tests.
- `Outcome` order is stable for the six notifier counters. Permanent Refused is distinct from retryable NodeShutdown/Unspecified. Storage-origin Parquet wrappers are inspected recursively through AbortFailed.
- `Metrics` uses plain, measurement and registration sets through their respective APIs; process accounting is in the engine, avoiding a dependency on core-nodes from the engine.
- Python helpers use one Engine, one DockerStore, one AlloyProducer and one shared River file. Synthetic request constructors remain for metrics/ACK cases; both readers see the same final downloaded snapshot with a latest-descriptor join and filename tie breaker. All unittest classes precede the final unittest.main guard.
- Local image defaults and environment overrides are consistent in tests/docs. Only Alloy may be pulled. Missing ClickHouse falls back to the local Docker image; absence follows SERIES_REQUIRE_DOCKER policy.
- The README contract test checks that its River block contains the exact config file used by task 10 and task 11. Q1 is the sole intentional temporary tracking reference.
- Q4 limits engine/otap edits to task 2 accessors and task 7 process accounting; each has a focused regression. Q3 leaves engine shutdown policy unchanged. Q5/Q6 explicitly separate reported allowances from plan 3 validation.

## Rulings applied

- Q1: Keep `SERIES_TRACKING_ISSUE=4128` with a temporary-reference comment; replace it with the real PR number when opened.
- Q2: Use env-overridable local MinIO/RustFS/ClickHouse tags with the ruled defaults; skip missing Docker/images unless `SERIES_REQUIRE_DOCKER=1`; defer CI digest pinning to plan 3.
- Q3: Use the existing admin shutdown endpoint with an explicit timeout; no engine shutdown-policy change or engine PR.
- Q4: Plan 1 is merged at `7fd7c3eab`; authorize additive series-lake edits and only necessary, separately tested Context/inbox/process-accounting additions with File Structure justifications.
- Q5: Report the RSS residual in plan 2; plan 3 validates memory allowances.
- Q6: Accept block-sized series sealing as v1 F-park, budget an extra B, and state its memory/control-latency cost without claiming fixed workspace.
- E2E-1: Docker Alloy tails N known log lines in task 10 and during the task 11 outage; verify bodies and attributes, retaining synthetic clients for metrics and acknowledgement assertions.
- E2E-2: DuckDB and ClickHouse read the same Parquet files, choose the latest descriptor per series, preserve join cardinality and agree with producer expectations; skip unavailable ClickHouse prerequisites cleanly.
- E2E-3: Task 12 documents Alloy + df_engine + MinIO as the reference deployment using the exact test River configuration and complete reader code.
- C1: The Spec header cites section 3.2 as the node boundary.
- C2: Task 1 gives the exact package/feature build, YAML path, df_engine invocation, send-log one-liner and DuckDB one-liner.
- C3: Re-read current series-lake sources and verify/correct every Ground truth and task-text file:line anchor.
- C4: Global Constraints require codex reviews; every planned commit retains both trailer lines.
