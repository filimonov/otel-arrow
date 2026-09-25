# Series Parquet Measurement Implementation Plan

<!-- markdownlint-disable MD013 MD032 MD031 MD040 MD024 MD033 MD046 MD029 MD004 -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce reproducible measurements and passing acceptance checks for every requirement in design revision 7 sections 9.5 and 9.9, including the recommended durable-buffer topology.

**Architecture:** Extend the existing real-process Python E2E harness with deterministic workloads, a disk-backed acknowledgement ledger, structured results, and externally injected faults. Layered Criterion benchmarks and complementary Rust timing/heap targets isolate library stages; real engine runs establish throughput, resident memory, recovery, and topology semantics. Preserve the existing 18 tests, Alloy lane, MinIO/RustFS adapters, and independent DuckDB/ClickHouse readers.

**Tech Stack:** Workspace Rust 2024/MSRV 1.88, Arrow/Parquet 58.3, object_store 0.13.2, Tokio current-thread runtime, cpu-time, DHAT, Linux perf/procfs, Python unittest/SQLite/grpcio/DuckDB/boto3, Docker 29, Grafana Alloy, Toxiproxy 2.12.0, NGINX, dnsmasq, iptables, tcpdump.

**Spec:** `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md`, revision 7; sections 5.7, 6.6, 9.5-9.9 and 10.3. The format template is `docs/superpowers/plans/2026-09-21-series-parquet-exporter-node.md`; its older memory claims do not override revision 7 or current source. Producer evidence is `.superpowers/sdd/2026-09-21-series-parquet-exporter-node/alloy-research.md`.

## Global Constraints

- This planning session changes only `docs/superpowers/plans/2026-09-22-series-parquet-measurement.md`. All implementation, build, container, commit, and spec-edit commands below are instructions for subsequent execution. Do not run cargo or mutate Git state while writing this plan.
- The exporter has no persistent local state, third block, or spill. The upstream durable buffer owns its own persistent WAL and segments. LocalFileSystem remains a destination backend.
- The series cache is an optimization, never correctness state. Delivery is at-least-once; object completion is not a block-atomic read snapshot, and replay is not deduplicated.
- The exporter acknowledges its upstream only after the entire block is durable. In the buffered topology the receiver can acknowledge the producer after the buffer's durable local write, before object storage completion. Keep these two acknowledgement events separate in results.
- One pipeline instance equals one worker; limits and exporter metrics are per worker. Explicitly select physical core IDs with `policies.resources.core_allocation` using `type: core_set` and `{start: id, end: id}` entries, and keep identical IDs across buffered restarts. Record worker count, SMT siblings, producer/store placement, and actual affinity.
- Component/module/feature: `series_parquet`; URN: `urn:otel:exporter:series_parquet`; metric set: `exporter.series_parquet`. The buffer feature is `durable-buffer`, component `processor:durable_buffer`.
- Default interval 15s, block 500MiB, requests/block 4096, flush deadline 60s, input 16MiB, extracted 32MiB, row 1MiB, nesting 32, cache 200000, run 8MiB, merge 16MiB, upload part 8MiB/concurrency 2/abort 5s, row group 64MiB, writer 96MiB, notify batch 64. Unsupported policy defaults to reject. Store the complete effective configuration with each result, including deliberate overrides.
- Supported workloads contain logs, integer/double gauge and sum points, and explicit histograms. Metrics use the merged `metrics/values` dataset. Include mixed histogram shapes in the same request to retain empty-list versus null-list coverage. Unsupported rows are a separate negative control, never silently subtracted from a supported-record loss assertion.
- Production compression remains ZSTD. Uncompressed encoding is a diagnostic bench setting, not a new exporter configuration or format option.
- No fixed sleeps in new tests. Use observable conditions, `time.monotonic_ns()`, bounded RPC/subprocess timeouts, and a monotonic deadline. Polling with `Event.wait(min(poll_interval, remaining))` only schedules another observation; elapsed time alone never proves readiness, drainage, or fault activation.
- Use the existing JSON telemetry API `/api/v1/telemetry/metrics?format=json&keep_all_zeroes=true`, as `test_e2e.py:2240` does. Only advances of each worker's collection-updated `pipeline.uptime` count as new epochs; three HTTP responses or scrape timestamps do not prove three collections. Missing, stale, malformed, or ambiguously attributed samples are errors, never zero. Distinguish process metrics from per-worker metrics and do not sum a process RSS value across workers.
- Use admin shutdown with a deadline of at least `interval + 2 * (flush_retry_deadline + abort_timeout) + 15s`; use 180s for 15s windows and 300s for 120s windows. The controller's 60s signal shutdown is insufficient for the latter. Hard-kill cases intentionally bypass shutdown.
- Faults act on real processes, store HTTP traffic, DNS, or kernel networking. No mock ObjectStore, fake errors in exporter code, or production failpoints count as failure-model evidence.
- Full acceptance requires both strict producer retry and buffered persistent replay, both supported signals, both existing store backends, and both readers. A skipped required case means incomplete acceptance, even when optional local test discovery exits successfully.
- Default test discovery adds at most five minutes after engine build and image provisioning. `SERIES_MEASURE_LONG=1` enables throughput sweeps, profiled memory runs, the 30-minute soak, and long failure/proof runs. CI runs the fast subset with this variable unset; a manually dispatched long lane may set it. Missing binaries/images/tools skip before starting a case unless `SERIES_REQUIRE_DOCKER=1` or `SERIES_REQUIRE_FAULT_TOOLS=1` makes them mandatory. A fault or startup failure after successful preflight is a failure, not a skip.
- All new source, configuration, result metadata authored by this work, and Markdown are ASCII-only. JSON uses `ensure_ascii=True`. Every test declaration has immediately preceding `Scenario:` and `Guarantees:` comments. Add copyright/SPDX headers and public Rust documentation.
- Workspace lints deny `missing_docs`, `unwrap_used`, `unused_results`, and `print_stdout`. Use fallible Rust entry points and write benchmark JSON to a file. Do not add lint suppressions to get a measurement through.
- All future cargo commands run from `rust/otap-dataflow`. After Rust edits run affected-crate `cargo check`; run focused tests, `cargo xtask check-benches`, and final `cargo xtask check`. Do not share another agent's cargo target directory during implementation; use an agreed dedicated `CARGO_TARGET_DIR`.
- Measurement code and reporting are development-only: use `chore` in the implementation PR title; those changes need no changelog. Discovered defects enter the contingency task after the failure-model tasks: implement bounded fixes, regression tests, copied repository changelog entries and failed-measurement reruns within this plan. Only architectural findings are deferred, with evidence, to a plan 4 decision.
- **Controller baseline policy:** The first VALID run (including its required repetitions) writes and commits a baseline fingerprinted by machine, core allocation, effective configuration, workload and build profile. A later run with the same fingerprint fails for a regression greater than 25 percent; a different fingerprint writes and commits a new baseline instead of comparing. Keep the original matching baseline immutable so small regressions cannot ratchet it upward. Correctness (no acknowledged supported record missing after drain, valid descriptors, counted duplicate multiplicities), measurement validity (matched environment, no concurrent build, minimum samples), and unexplained accounted-versus-RSS residuals remain hard checks on EVERY run. Worker workspace reservations are engineering estimates to evaluate and explain, never measured ceilings or absolute first-run performance gates.
- **Controller environment policy:** Every run JSON has start AND end snapshots containing CPU model, logical and physical core count, total RAM, kernel, load averages and observed per-thread affinity. Require at least eight available physical cores for publishable measurements: up to four engine, two producer, one store, one reader/proxy core; reserve unused engine cores in one-worker runs and exclude SMT siblings from competing roles. Acquire an exclusive host measurement lease for the whole run, from preflight through drain and the end snapshot. Verify worker TIDs through `/proc/PID/task/*/status`; ABORT immediately on affinity mismatch or ambiguous worker mapping. Continuously monitor for concurrent cargo or docker builds, invalidate on any detection, and preserve evidence without stopping another user's processes. Unobservable build activity or a changed machine/core/config/build environment makes a run invalid.
- All commit blocks below run from repository root; module commands in parentheses enter the Rust workspace explicitly. Every implementation commit first invokes `measure stage-results` for its listed indexes (staging each index, child and baseline by validated exact filename), then stages explicit source filenames, never `git add -A`, `git add .`, or a directory. Its final two trailer lines are exactly `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd`.

## Ground truth and integration decisions

Source names below are authoritative integration anchors, not new APIs to invent. Re-read them if the branch advances before execution.

| Evidence | Consequence |
| --- | --- |
| `crates/validation/tests/series_parquet/test_e2e.py`: `Engine`, `DockerStore`, `AlloyProducer`, `verify_layout`, `verify_files`, `verify_readers`, `clickhouse_reader`, `canonical_column`, `canonical_row`, `stored_multiplicity`, `rss_bytes` | Extend these helpers additively. Do not replace the 2,893-line harness or copy a second engine/store implementation. Preserve all 18 legacy tests and their default call signatures. Paths in this table are relative to `rust/otap-dataflow`. |
| `Engine.__init__` starts immediately, shallowly replaces exporter sections, and selects one core through the example YAML | Add keyword-only topology, explicit cores, and launcher settings before serializing/launching. Deep-merge measured overrides into a recorded effective config; retain old behavior for old callers. |
| `Engine.exporter_gauges` returns zero on an absent endpoint/series and keeps only a last matching sample | Keep this legacy convenience method; add a strict labelled sampler for measurements. Several workers require distinct samples, not the last line's value. |
| `engine_metrics` and `metric_max` read JSON telemetry; some instruments publish interval deltas | Reuse `engine_metrics` JSON with zeroes retained; distinguish epochs by per-worker `pipeline.uptime`, and only aggregate a delta once per distinct collection epoch. Producer IDs and stored rows remain the delivery oracle; metric totals are corroboration. |
| `verify_files` assumes default values sort and fixture columns; `stored_multiplicity` counts all metric kinds together by request.id | Retain these checks for legacy fixtures. Add a generic stable-ID oracle that checks every metric kind separately and validates sorting against each file's declared keys. Reuse the existing readers and canonical rendering. |
| `extract::extract(&mut OtapArrowRecords, &LakeConfig) -> Result<Extracted>` and `Worker::prepare` | Measure wire-to-OTAP conversion separately from extract/hash. `extract` already decodes transport IDs. Do not charge data generation or input cloning to extraction. |
| `buffer.rs`: `SortedTableBuffer::{building,runs,finalized}`, `Block::{reserve,admit,seal,tables}` | Admission run sealing and final values sealing allocate copies. The old plan's stamp-only seal assertion is not a whole-block memory bound. Measure values overlap separately. |
| `sort.rs`: `MergeIter` owns `Vec<Rows>` for every input run; `rows_per_chunk` derives from average width | Resident merge keys can approach a second payload copy for wide custom keys. Measure their allocation stacks and actual output chunk maxima; nominal `merge_chunk_bytes` is not a hard chunk bound. |
| `sink.rs`: `Sink::write_block`, `AsyncArrowWriter`, `ParquetObjectWriter`, `BufWriter`, `FileNaming` | A diagnostic encoder must match writer properties and row-group flush predicates. An upload-only stage uses pre-encoded bytes and the real object_store client. Verify diagnostic output against the actual sink. |
| `worker.rs::sample_metrics`, `metrics.rs`, `token.rs` | Accounted memory includes retained ACTIVE/FLUSHING, pending extraction/descriptors, cache estimate, token queues, and spare token vector capacity. Budget includes engineering workspace reservations; neither is observed RSS. |
| `crates/engine/src/engine_metrics.rs` | `memory.unaccounted_rss_bytes` is a process residual, sampled once; it is not an exporter allocation measurement. Read signed `RSS - sum(accounted)` independently because the published residual clamps negatives. |
| `durable_buffer_processor/{README.md,config.rs,mod.rs}` and `crates/config/src/pipeline.rs::effective_dispatch_policy` | Persistent directories are per actual core ID. `engine.ingest` precedes producer ACK; downstream ACK resolution and persistence of progress happen later. Default poll/segment durations are 100ms/1s; retry settings are 1s, 30s, multiplier 2 with implementation jitter. Use `backpressure` and `max_age: null`. The buffer README's round-robin advice means one recipient per item; current single-destination YAML resolves to `one_of`, not a `round_robin` YAML enum. |
| `durable_buffer.bundle.nacked`, `durable_buffer.retry.sent`, `retries.scheduled`, `items.requeued` | Correlate bundle IDs and observed retry timestamps to prove retryable exporter NACKs and backoff. One zero `items.queued` sample does not prove drainage during segment finalization. |
| Existing outage/restart/replay tests | Already cover useful cases, but not the full revision-7 matrix or ambiguous buffered completion. Extend coverage instead of counting an old test as new evidence. |
| Alloy research, v1.19.2 | Producer throughput can be limited by `num_consumers * batch_size / response_hold`. Measure queue occupancy, enqueue failures, producer RSS, and catch-up separately. Its 236MB baseline, approximately 2.03KB per held record, and approximately 1.6MiB/s catch-up observations are historical producer results, not exporter measurements. |
| Workspace `Cargo.toml` | Criterion 0.8.0, cpu-time 1.0.0 and DHAT 0.3.3 already exist. Use layered Criterion groups for synchronous cumulative stages as required by spec 9.6. Complement them with purpose-built async, CPU/DHAT and perf runs for allocation lifetimes, upload completion and attribution; all export the common stage contract. |

### Fault tools checked on this machine

Read-only inventory on 2026-09-22 found Docker client/server **29.2.1**, `ip`, `tc`, `nft`, `iptables`, `dnsmasq`, `dig`, `tcpdump`, `nsenter`, `perf`, `valgrind`, Python and npx. The session is UID 1000; presence of host commands does not establish permission to manipulate a namespace. No namespace mutation or container was started during planning.

The existing MinIO `RELEASE.2025-04-22T22-12-26Z`, RustFS `1.0.0-rc.3`, ClickHouse `26.7.4`, and Alloy `v1.19.2` images are local. `toxiproxy-server`, NGINX, and tshark are absent, as are dedicated Toxiproxy/NGINX fault images. Do not claim those cases are currently runnable without provisioning.

| Fault | Exact injector and activation evidence | Provisioning and clean skip |
| --- | --- | --- |
| Store outage/restart | `DockerStore.stop()` uses `docker stop --time 0`; `DockerStore.recover()` recovers the same container and waits for readiness. Require failed real PUT plus exporter storage NACK. | Existing Docker and images available. Existing Docker preflight applies. |
| Slow S3 | Toxiproxy 2.12.0 upstream `bandwidth` and downstream `latency` toxics. Require proxy API state plus delayed completed PUT durations and nonzero forwarded bytes. | Pull `ghcr.io/shopify/toxiproxy:2.12.0` during provisioning, resolve and record digest; skip missing image before test unless fault tools required. |
| HTTP 503 | NGINX `if (-f /control/fail503) { return 503; }` on actual S3 request path. Require access-log PUT/POST status 503 and client storage errors. | Build Task 8's `series-measure-fault-tools:local` image containing distro NGINX. Record base digest/package versions. Skip absent image. |
| Process restart/hard kill | Admin graceful shutdown, then a fresh `Engine`; SIGKILL via `os.kill(pid, signal.SIGKILL)` or `docker kill --signal KILL`. Require old PID exit and a new boot ID. | Host process operations available; container launcher requires Docker. Never kill by process name or kill unrelated engines. |
| Network disconnect/reset | Toxiproxy `enabled: false`, then `reset_peer` on a separate case. Require failed connection and proxy evidence; pcap captures reset. | Toxiproxy image as above; no host routing changes. |
| DNS NXDOMAIN/timeout | dnsmasq in a private container namespace; per-case hostname and authoritative local zone. `iptables` inside that namespace drops port 53 for timeout. Require captured DNS query/response or dropped-query counter. | Task 8 image contains dnsmasq/dnsutils/iptables/tcpdump/tshark; disposable `NET_ADMIN` capability probe must succeed. Skip privilege absence only at preflight. |
| Pure TCP ACK loss | Namespace-local iptables BPF match compiled by `tcpdump -ddd`, dropping store-to-client ACK-only packets. Require counter increase and tshark retransmission evidence. | Task 8 image and `NET_ADMIN`; verify `xt_bpf` support in disposable namespace before case. Skip unavailable capability cleanly. |
| Dropped completion response | NGINX routes values PUTs through a dedicated Toxiproxy downstream `timeout: 0`; series traffic bypasses it. Bypass HEAD/GET proves a complete readable values object while downstream response bytes are dropped. | Both images; test readiness uses real small PUTs before the measured fault. This is an application acknowledgement fault, recorded separately from pure TCP ACK loss. |

Provisioning commands belong to execution, never to test import or this planning session:

```bash
docker pull ghcr.io/shopify/toxiproxy:2.12.0
docker build -t series-measure-fault-tools:local -f crates/validation/tests/series_parquet/fault-tools.Dockerfile crates/validation/tests/series_parquet
```

Run these from `rust/otap-dataflow`. If host utilities are preferred for diagnostics, Ubuntu packages are `iproute2 iptables dnsmasq dnsutils tcpdump tshark linux-tools-common valgrind`; tests still isolate DNS/firewall changes in disposable containers. Do not edit host `/etc/resolv.conf`, add host firewall rules, or require host sudo. Record resolved image digests, not merely mutable local tags. Tool semantics are documented by [Toxiproxy](https://github.com/Shopify/toxiproxy/blob/main/README.md), its [2.12.0 release](https://github.com/Shopify/toxiproxy/releases/tag/v2.12.0), [NGINX return/if directives](https://nginx.org/en/docs/http/ngx_http_rewrite_module.html), and [dnsmasq](https://thekelleys.org.uk/dnsmasq/docs/dnsmasq-man.html).

## File Structure

All paths are repository-relative. These are implementation outputs, not permission to edit them during this planning session.

| File | Responsibility |
| --- | --- |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py` | Additive Engine/topology/launcher and Alloy observability options; preserve legacy tests and helpers. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py` | Immutable run/workload inputs, deterministic protobuf batches, SQLite ledger, telemetry/RSS sampler, generic reader oracle, atomic JSON results. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py` | CLI, named case registry, local/container orchestration, retries, drainage, result exit status. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py` | Fast contract, oracle, sampler, throughput arithmetic, and local smoke tests. |
| `rust/otap-dataflow/crates/series-lake/Cargo.toml` | Criterion and complementary bench targets plus existing-workspace dev dependencies. |
| `rust/otap-dataflow/crates/series-lake/benches/layered.rs` | Cumulative Criterion groups beginning with OTLP-to-noop and stage distributions. |
| `rust/otap-dataflow/Cargo.lock` | Lockfile changes required by bench dev dependencies or a reproduced bounded fix; stage explicitly in Tasks 3 and 12. |
| `rust/otap-dataflow/crates/series-lake/benches/measurement.rs` | Fallible CLI, timing/CPU/DHAT controls and output file; no stdout. |
| `rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs` | Conversion, extraction/hash, sort/seal, merge, Parquet encoding, local persistence and S3 upload stages using production library calls. |
| `rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs` | Stage output equivalence, measured expansion and full metric-schema checks. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py` | Stage invocation, capacity search, perf classification and throughput results. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py` | Repeated paired memory experiments, profile attribution, signed residual and shared baseline policy. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py` | Opt-in 30-minute runs and fast forced-rotation/outage check. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py` | Container namespace launcher, Toxiproxy/NGINX control, fault evidence and cleanup. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/fault-tools.Dockerfile` | Reproducibly recorded NGINX/DNS/kernel-network diagnostic environment. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/fault-nginx.conf` | S3-preserving request routing, access logging, 503 gate and values-response route. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py` | Both-topology S3, process, network, DNS, and TCP matrix. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/test_buffered.py` | 15s/120s latency, retained-disk no-resend recovery, retry/backoff, ambiguous completion. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/report.py` | Completeness validation, numeric Markdown report and exact section-9.8 replacement. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md` | Commands, units, budgets, provisioning, thresholds and reproduction. |
| `.github/workflows/series-parquet-e2e.yml` | Existing 18-test lane plus fast measurement checks; explicit optional long dispatch. |
| `docs/superpowers/reports/2026-09-22-series-parquet-measurement.md` | Final measured results, limitations, failures and shared-writer decision inputs. |
| `docs/superpowers/reports/series-parquet-measurement/*.json` | One compact JSON per run, immutable fingerprinted baselines, family indexes, remediation findings and a manifest listing every index/run/baseline. |
| `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md` | Task 14 only: replace exactly the two obsolete unmeasured sentences in section 9.8. |

Task 12 additionally records the exact source/test paths selected for each bounded defect and copies the Rust changelog template for user-facing fixes. Those paths cannot be known until reproduction; they must be enumerated in the finding before the bounded edit and staged individually afterward.

Raw profiles, parquet files, pcaps, logs and ledgers live below a user-selected run directory outside tracked source, and are retained as CI artifacts. Each committed JSON contains artifact paths, SHA-256 hashes, sizes and retention location. Commit samples and aggregates in JSON; do not commit hundreds of megabytes of raw profiles or claim an inaccessible temporary path is a reproducible artifact.

### Shared contracts, budgets and acceptance policy

Use `python3 -m crates.validation.tests.series_parquet.measure` from `rust/otap-dataflow`. CLI subcommands are `run`, `stages`, `attribution`, `capacity`, `memory`, `soak`, `fault-preflight`, `failures`, `buffered`, `remediate`, `stage-results`, and `report`; measurement/report commands accept `--output-dir PATH`; `stage-results` requires only `--index PATH`, and `remediate` also accepts `--finding PATH`. `run --case harness-contracts --output-dir /tmp/series-contracts` is the initial contract-only slice; `run --case harness-local --output-dir /tmp/series-measure` becomes a publishable real-engine slice after Task 2. Long commands reject execution without `SERIES_MEASURE_LONG=1`, explaining how to opt in; unittest long classes skip with that reason.

The following signatures are owned by Task 1 and consumed unchanged:

```python
@dataclasses.dataclass(frozen=True)
class Workload:
    seed: int = 20260922
    requests: int = 100
    records_per_request: int = 100
    body_bytes: int = 1024
    series: int = 100
    metrics_every: int = 5

@dataclasses.dataclass(frozen=True)
class RunSpec:
    run_id: str
    case: str
    topology: str
    store: str
    cores: tuple[int, ...]
    workload: Workload
    interval_s: int = 15
    duration_s: int = 30
    producer_timeout_s: float = 180.0
    max_in_flight: int = 128
    overrides: dict = dataclasses.field(default_factory=dict)

def stable_id(seed: int, request: int, point: int, kind: str) -> str:
    return f"{seed:08x}:{request:012d}:{point:06d}:{kind}"

def require_long() -> None:
    if os.environ.get("SERIES_MEASURE_LONG") != "1":
        raise unittest.SkipTest("set SERIES_MEASURE_LONG=1 for this measurement")

class MeasurementTestCase(unittest.TestCase):
    """Retain each test's logs, results and oracle ledger for review."""

    def setUp(self):
        root = Path(os.environ.get("SERIES_ARTIFACT_DIR", "/tmp/series-measure-tests"))
        root.mkdir(parents=True, exist_ok=True)
        self.output_dir = Path(tempfile.mkdtemp(prefix=self.id() + "-", dir=root))

def wait_until(observe, accept, *, deadline_ns: int, description: str):
    wake = threading.Event()
    last = None
    while time.monotonic_ns() < deadline_ns:
        last = observe()
        if accept(last):
            return last
        remaining = (deadline_ns - time.monotonic_ns()) / 1e9
        if remaining > 0:
            wake.wait(min(0.05, remaining))
    raise AssertionError(f"deadline: {description}; last={last!r}")
```

`RunSpec` validates topology in `strict|buffered`, store in `local|minio|rustfs`, positive counts, nonempty distinct allowed cores, and body size sufficient for its ID. `max_in_flight` must not exceed receiver capacity. CLI converts case-specific JSON options into `overrides`, then records both requested and effective configurations. `run_case(spec: RunSpec, output_dir: Path) -> dict` performs a single experiment and always writes its result, including failures; `run_named(case: str, output_dir: Path, **options) -> dict` constructs a registered spec and delegates. Exceptions yield `status: failed` and nonzero CLI exit. Preflight skips yield `status: skipped`, reason, and non-acceptance; required-mode skips fail.

`Ledger(path: Path)` owns SQLite tables `requests(request_id, signal, wire_sha256, first_send_ns, ack_ns)`, `attempts(request_id, ordinal, start_ns, finish_ns, outcome)`, and `records(record_id PRIMARY KEY, request_id, kind, expected_sha256)`. `add_request`, `attempt`, `ack`, `acked_ids`, and `all_ids` use transactional updates. A duplicate request must have the identical serialized protobuf hash; ACK time is the first successful response with zero partial rejection. Keep the ledger outside the engine/container namespace and persist each ACK before it can be used as a fault gate. Do not retain an unbounded Python set or all payloads during soak.

`build_request(workload: Workload, request_index: int) -> tuple[str, bytes, list[tuple[str, str, str]]]` returns signal, serialized OTLP, and `(record_id, kind, expected_sha256)` rows. `read_oracle(root: Path, ledger: Ledger, *, require_all: bool, healthy: bool) -> dict` returns numeric coverage, missing/unexpected/corrupt counts and a multiplicity histogram after checking both readers. `sample_engine(engine: Engine, *, expected_workers: int) -> dict` returns timestamped per-worker gauges, process RSS and buffer gauges. `drain(engine: Engine, ledger: Ledger, store, *, deadline_ns: int) -> Path` stops new generation, finishes authorized retries, observes empty exporter/buffer state over three consecutive advances of every worker's `pipeline.uptime`, with empty state at each advance, performs graceful shutdown, downloads completed objects if needed and invokes `read_oracle`. A timeout never invokes the oracle on a partial download and calls it success.

One JSON document per run has `schema_version: 1`, `run_id`, `case`, `status`, `started_utc`, `elapsed_s`, `environment`, `config`, `workload`, `metrics`, `samples`, `events`, `checks`, and `artifacts`. Required `environment.start` and `environment.end` fields: `cpu_model`, `logical_core_count`, `physical_core_count`, `ram_bytes`, `kernel`, `load_average_1_5_15`, and `thread_affinity` (PID/TID, role, observed `Cpus_allowed_list`, expected cores, observation time). Store machine identity hash, Git revision/dirty patch hash, binary SHA-256/profile/features/allocator, core topology/allocation, store/reader/producer versions and digests, filesystem/mount type, tools and clock resolution alongside them. Snapshot immediately before measured traffic and after final drain but before normal process teardown; deliberate kills retain pre-kill observations and replacement PID/TID mappings. Even failed/skipped runs write both snapshots; absent processes have explicit reasons and cannot support a passed measurement. Record lease identity/lifetime, build-monitor observations and environment-match check results. Configuration includes receiver channels/timeouts, all exporter knobs, buffer settings/path/core IDs, producer rate/concurrency/batching/retry policy, and fault parameters. Metrics carry explicit units in names (`_bytes`, `_s`, `_records_per_s`, `_cpu_ns_per_record`); unavailable values are null with a reason, never zero. Run status cannot be passed with unavailable mandatory metrics.

Every capacity trial, repetition, stage subprocess and fault/topology/store cell is a distinct run file. Name it with `run_id = f"{case}-{topology}-{store}-c{len(cores)}-w{interval_s}-r{ordinal:03d}"`, where the family assigns a unique increasing ordinal and stores the full trial settings in the file. For example, `http503-buffered-rustfs-c1-w15-r001.json` has one environment/config/workload and one result. The named artifacts in the tasks are either a single-run file (the simple harness/soaks) or a family summary **index**, containing aggregate metrics plus `run_files` and their hashes. They never replace the one-JSON-per-run files. Each index enumerates all its child filenames; re-execution uses a new artifact directory rather than overwriting evidence.

Task 1 owns `publish_result_tree(index_path: Path, report_dir: Path) -> Path`: validate hashes and copy the compact index, enumerated run files and baseline files by exact filename from the artifact directory into `docs/superpowers/reports/series-parquet-measurement`, preserving referenced raw-artifact locations. Every producing CLI calls it before returning, including failed results; verification-only indexes use the same publication path. Existing run/baseline filenames are immutable and collisions with different hashes fail. Family indexes may advance only after preserving the previous index as an immutable run-ID-named child. Thus commit blocks below operate on already published, complete evidence trees, not indexes that still point only at untracked temporary JSON.

Task 1 also implements `stage_run_files(index_path: Path) -> None` for future commit steps: read the task's completed index, recursively enumerate `run_files`, `baseline_files` and child indexes, include the index itself, validate every child path is a plain `.json` filename in the report directory, reject cycles/path escape, verify hashes, and invoke `subprocess.run(["git", "add", "--", *explicit_paths], check=True)` in bounded argument batches. It stages each enumerated file by name, never a glob or directory. Expose `measure stage-results --index PATH`; invoke it on each task's named result index immediately before that task's explicit `git add`/commit block. For a single-run file it stages that path and its referenced baseline files. Contract-check indexes enumerate verification evidence without claiming a measured baseline. This is an execution-only command and is not run during planning.

All tasks apply the Controller baseline policy in Global Constraints; the table distinguishes hard correctness/validity rules from measured quantities. Store immutable `baseline-CASE-FINGERPRINT.json` files beside each task's index; the index lists their explicit names/hashes in `baseline_files`. A baseline fingerprint hashes canonical JSON of machine identity/model/core topology/RAM/kernel, role-to-core allocation, effective config (normalize run-directory names only), workload including seed/rate/duration, and build profile/features/allocator/toolchain. Record source revision and binary hash as provenance outside that fingerprint so new implementations can be compared. Profiles using different allocators or instrumentation have distinct fingerprints. Compare only identical case/metric schemas; lower throughput and higher latency/CPU/allocation/RSS are regressions. Baseline files contain the fingerprint, metric schema, reference metrics and source run IDs; indexes hash the baseline and source files without cyclic hash references. Record signed changes and directions; zero reference values permit no positive increase unless a predeclared measurement-resolution floor applies. Never replace a failed or unstable baseline candidate with a passing label.

| Check | Hard rule or measured comparison |
| --- | --- |
| Delivery/semantics | Any acknowledged supported ID missing, unexpected ID, changed payload, missing descriptor, invalid sort/metadata, or reader disagreement fails. Healthy no-retry runs require multiplicity exactly one; fault runs allow duplicates but count each ID/kind. Configured retained-state caps and lossless buffer retention remain correctness invariants. |
| Instrumentation/environment | Missing required metric, wrong worker cardinality, stale uptime for over three collection intervals, nonpositive duration, inadequate cores, affinity mismatch, concurrent build, unmatched start/end environment, producer-limited claimed engine maximum, or failed activation invalidates the measurement. |
| Repeatability | Capacity and stage claims require three repetitions, all reported; coefficient of variation over 15% invalidates a capacity/stage baseline. This is a sample-stability test, not a performance regression allowance. Memory has its independent paired-run stability rule in its task. |
| Stage agreement | Staged ZSTD output matches actual Sink semantics. Sum exclusive CPU and named residual; reconciliation error above 10% or unexplained CPU above 20% invalidates attribution. These are instrumentation checks. |
| Expansion | Measure conversion/encoder ratios and compare observations with `4*I`, `3*W` and other engineering reservations descriptively. Apply the Controller baseline policy to measured allocations/peaks; excess over a reservation alone is not a first-run ceiling failure. Explain every discrepancy and classify discovered defects in the contingency task. |
| RSS reconciliation | After independently measured runtime/allocator/buffer/workspace terms, unexplained positive or persistent negative residual above `max(32MiB, 0.10 * peak_RSS)` fails every run. Freeze this diagnostic uncertainty tolerance before the run; a reservation cannot be subtracted as though it were a measurement. |
| Soak | Require 99% sample coverage and full input duration. Record RSS slope, post-warm-up median ratio, peak and drain rate; evaluate measured memory/performance with the Controller baseline policy. No absolute first-run slope or growth SLO. |
| Capacity stability | At an accepted offered rate, last 30s unique durable drain rate is at least 98% of offered rate, backlog slope at most 2%, and no permanent rejection/loss occurs. A higher unsustainable trial brackets the ceiling; these define a valid capacity observation, not a fixed speed promise. |
| Acknowledgement latency | Record p50/p95/p99/max, timeout counts and window deltas for both topologies. Use the Controller baseline policy for numerical performance. Independently prove buffered durable-write ACK before object completion with retained-disk replay; STRICT ACK remains object-durable. |

Task costs below are expected wall-clock **measurement/verification time after dependencies and binaries are available**, not estimates of engineering effort. Compilation, image provisioning and full workspace checks are separately budgeted at 15-60 minutes each depending on caches. Long measurements run sequentially on reserved cores; concurrent builds invalidate performance results.

---

### Task 1: Deterministic measurement harness and durable result contract

**Expected wall-clock cost:** 15-30 seconds for harness/schema contract tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/harness-contracts.json`

**Interfaces:**
- Consumes existing `Engine`, `DockerStore(kind)`, `AlloyProducer`, `rss_bytes`, `verify_layout`, `verify_readers`, `clickhouse_reader`, and canonical helpers without removing their signatures.
- Produces the shared contracts, strict sampler/drain and atomic results. Task 2 adds topology/launcher options and enforced host-run controls before publishing performance evidence.
- `evaluate_baseline(result: dict, *, baselines: dict | None = None) -> dict` is the sole evaluator for the Controller baseline policy. It checks hard gates first, canonicalizes the fingerprint, resolves an immutable matching baseline (or uses test-supplied baselines), and returns the successful decision plus per-metric signed regressions. A hard failure or failed regression attaches the decision to the result and raises AssertionError; `run_case` still publishes the failed JSON in `finally`. For repeated families, evaluate only after all required child runs and stability checks are complete, never after the first child. On valid new fingerprints it atomically writes a candidate baseline and references it in `baseline_files`; the commit step publishes it. A failed hard gate or unstable sample never creates a baseline. Default loading uses the result's recorded report directory; no hidden current-directory lookup.

- [ ] **Step 1: Add failing ledger/oracle tests with exact negative controls**

```python
# Scenario: an acknowledged histogram is absent although its sibling gauge exists.
# Guarantees: per-kind stable IDs catch loss that aggregate metric counts conceal.
def test_missing_metric_kind_fails(self):
    expected = {"r:gauge": "a", "r:histogram": "b"}
    actual = {"r:gauge": ["a", "a"]}
    with self.assertRaisesRegex(AssertionError, "r:histogram"):
        assert_records(expected, actual, healthy=False)

# Scenario: replay preserves payloads but adds two copies of one supported record.
# Guarantees: duplicates are counted independently from missing or corrupt records.
def test_replay_multiplicity(self):
    actual = {"r:log": ["a", "a", "a"], "s:log": ["b"]}
    self.assertEqual(
        assert_records({"r:log": "a", "s:log": "b"}, actual, healthy=False),
        {1: 1, 3: 1},
    )

# Scenario: rebuilding a workload request for retry uses the original index and seed.
# Guarantees: retry bytes and all expected record IDs are identical.
def test_retry_is_deterministic(self):
    workload = Workload(seed=73, records_per_request=9)
    self.assertEqual(build_request(workload, 7), build_request(workload, 7))
```

`assert_records(expected: dict[str, str], actual: dict[str, list[str]], *, healthy: bool) -> dict[int, int]` is the small in-memory contract test adapter. Production `read_oracle` performs the same comparisons using SQLite/DuckDB joins so the soak's cardinality does not inflate producer RSS.

- [ ] **Step 2: Run red from the Rust workspace**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: import/name failure for the new measurement module, not a missing dependency or binary.

- [ ] **Step 3: Implement deterministic IDs, payloads and loss detection**

```python
def assert_records(expected, actual, *, healthy):
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    if missing or unexpected:
        raise AssertionError(f"missing={missing[:10]} unexpected={unexpected[:10]}")
    histogram = collections.Counter()
    for record_id, expected_hash in expected.items():
        copies = actual[record_id]
        if not copies or any(value != expected_hash for value in copies):
            raise AssertionError(f"corrupt={record_id}")
        if healthy and len(copies) != 1:
            raise AssertionError(f"unexpected multiplicity={record_id}:{len(copies)}")
        histogram[len(copies)] += 1
    return dict(sorted(histogram.items()))
```

For logs, derive from existing `log_request`; put the stable ID as a fixed-width body prefix followed by seeded printable padding. Keep `host.id`, service name and scope compatible with existing fixture readers. Vary `logger.name` by `request_index % series`; do not add the ID to configured series identity. For metrics, derive from `metric_request`, remove `request.id`, and construct the requested number of points with fixed series attributes. Encode uniqueness in `(metric name, time_unix_nano)` with `time_unix_nano = 1789960500000000000 + ordinal * 1000`; give gauge, sum, populated histogram and empty-distribution histogram distinct stable kind names. Integer/double values and bucket arrays derive from the seed/index. Keep metric names/attributes from a finite series pool. Exporter rounding and supported nullable representations define the expected canonical payload, with an independent explicit fixture for each kind; do not hash producer protobuf bytes and compare them to unrelated Parquet bytes.

Use deterministic protobuf serialization. Count actual supported points, not requests or `POINTS_PER_METRIC * request_count` for general workloads. Preserve small legacy request.id fixtures for existing tests. Record exact series cardinality separately from record cardinality.

Create SQLite with WAL mode and `synchronous=FULL`; retain bounded request metadata in memory and regenerate retry bytes by index. Enforce unique ID primary keys and immutable request hashes. Record attempted IDs, successful RPC ACKs, retryable NACKs, local deadlines, partial rejections, and outstanding IDs separately. Import and reuse `RETRYABLE_CODES` from `test_e2e.py:2327` without copying its set. It includes CANCELLED and excludes ABORTED. Add ABORTED only after repository/real-run evidence and a regression test that actually produces it. Classify producer-local deadline/connection failures separately; unexpected permanent statuses fail. Retries have a bounded deadline and capped exponential scheduling, driven by next-attempt monotonic time, not a fixed test sleep.

- [ ] **Step 4: Implement the strict JSON telemetry/drain path**

Reuse `engine_metrics(engine)` from `test_e2e.py:2240` and parse its `metric_sets` with original names/attributes. Require zeroes to be present and group/pipeline/node/core identities to select every worker exactly once. Require ACTIVE/FLUSHING/pending/token/cache/accounted/budget/oldest gauges. For each worker record `pipeline.uptime` from the `pipeline` metric set: `pipeline_metrics.rs:545` updates it during collection. The API's top-level timestamp is generated per scrape (`telemetry.rs:433`) and Prometheus suppresses zeroes (`telemetry.rs:1515`), so neither is an epoch discriminator. Reject uptime decreases within one process generation; track restarts separately. Only accumulate interval deltas on an uptime advance. Persist raw JSON and labels; sample procfs RSS/stat/smaps/FDs and buffer allocated disk bytes on the same monotonic timeline, keeping process roles separate.

Drain records each worker's initial uptime, then requires three subsequent consecutive increases with empty exporter requests/notifications and, when buffered, no queued/in-flight/retry work. Repeated responses with identical uptime add zero observations; a nonempty observation resets the empty streak. Missing gauges or no advances fail under the monotonic deadline. After those observations, admin-shutdown and inspect the store. The oracle checks all acknowledged and eventually accepted intended IDs, descriptor coverage per `(signal,date,hour,writer_id,boot_id,series_id)`, hashes, sort metadata/order, joins, payloads, multiplicities and both readers using SQL rather than unbounded Python collections.

- [ ] **Step 5: Write atomic results and baseline decisions**

```python
def write_result(path, result):
    encoded = json.dumps(result, sort_keys=True, indent=2, ensure_ascii=True,
                         allow_nan=False) + "\n"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(encoded, encoding="ascii")
    temporary.replace(path)
```

Validate mandatory fields, units, checks and status before writing; a failed result may omit unavailable measured fields only with explicit reasons. Register `harness-local` with 100 requests, mixed supported signals, 1s windows and exact no-retry multiplicity; its real measurement runs in Task 2 after host controls exist. Add synthetic contract tests for three unchanged uptime responses (no drain), three increasing empty epochs (drain), a zero-valued required gauge, a mismatched fingerprint (new baseline), and matching-fingerprint improvement/regression. Test that a correctness or residual failure cannot establish a baseline. Keep the configured numerical boundary in the shared evaluator alone. Test `stage_run_files` with a temporary index/child/baseline tree and a mocked subprocess invocation; assert every exact path is staged, hashes are checked and path escape is rejected. Each test carries specific Scenario/Guarantees comments.

Register `harness-contracts` to run `test_measurement` through `unittest.TextTestRunner`, collect `testsRun`, failures, errors and skips, write the verification-only index below and publish it with `publish_result_tree`. Exit nonzero if the runner was unsuccessful. It launches no measured engine and writes no measured baseline.

- [ ] **Step 6: Verify, record numbers and commit**

```bash
python3 -m crates.validation.tests.series_parquet.measure run --case harness-contracts --output-dir /tmp/series-contracts
```

**Recorded numbers:** contract-test counts and ledger/oracle/schema/epoch/baseline/staging decisions in `harness-contracts.json`, a verification index with `artifact_kind: contract_checks`, `run_files: []` and `baseline_files: []`. This is no performance measurement and cannot create a measured baseline. **Failure:** broken contract or changed legacy assertions. Task 2 produces the real `harness-local.json` under the Controller baseline/environment policy; discovered defects enter Task 12.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/harness-contracts.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/harness-contracts.json
git commit -m "chore: add deterministic series measurement harness" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 2: Engine launchers, enforced host controls and fast CI

**Expected wall-clock cost:** 30-60 seconds for launcher/control checks; 5-15 minutes for the existing 18-test lane.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Record: `docs/superpowers/reports/series-parquet-measurement/harness-local.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/launcher-ci.json`

**Interfaces:**
- Consumes Task 1's `RunSpec`, JSON schema, baseline evaluator, oracle and sampler.
- Adds keyword-only `topology="strict"`, `buffer_path=None`, `cores=None`, `launcher=None` to `Engine`, preserving legacy calls. A launcher implements `start(argv: list[str], log, env: dict) -> subprocess.Popen` and `pid(process) -> int`; the default is local, and Task 8 supplies the container launcher. All RSS/affinity/kill operations use the engine host PID, never a Docker CLI PID.
- `MeasurementLease(path: Path)` holds an exclusive `fcntl.flock` through run finalization. `environment_snapshot(engine: Engine | None) -> dict`, `check_worker_affinity(observed: dict[int, set[int]], expected: dict[int, set[int]]) -> None` and `build_activity() -> list[dict]` provide the shared environment policy. `run_case` starts a monitor before launching any measured process, captures both snapshots in `finally`, records invalidation events and releases the lease last. Register prebuilt benchmark/producer/store PIDs with the same monitor so engine-less stage runs also record observed thread affinity and both snapshots.

- [ ] **Step 1: Add failing launcher and control tests**

```python
# Scenario: a worker expected on core 4 can actually run on cores 4 and 5.
# Guarantees: runtime pinning warnings cannot silently validate a measurement.
def test_affinity_mismatch_aborts(self):
    with self.assertRaisesRegex(AssertionError, "affinity"):
        check_worker_affinity({123: {4, 5}}, {123: {4}})
```

Add tests with injected procfs observations for a cargo build appearing midway through a run, a second lease holder, absent end snapshot and a changed CPU/core configuration. Each must invalidate the result and prevent baseline publication. These test monitor decisions; real-run verification still reads the actual host. Add a buffered smoke asserting graph edges, unchanged core IDs and retained buffer path across restart.

- [ ] **Step 2: Run red**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing affinity/lease checks or launcher options; existing Task 1 contracts stay green.

- [ ] **Step 3: Add topology and launcher options before configuration serialization**

Insert the buffer into the actual config graph before launch:

```python
nodes["buffer"] = {
    "type": "processor:durable_buffer",
    "config": {
        "path": str(buffer_path),
        "retention_size_cap": "1GiB",
        "size_cap_policy": "backpressure",
        "max_age": None,
        "otlp_handling": "pass_through",
    },
}
pipeline["connections"] = [
    {"from": "receiver", "to": "buffer"},
    {"from": "buffer", "to": "exporter"},
]
```

Single-destination connections resolve to `one_of` in the current config API; do not invent a `round_robin` YAML setting from older README terminology. Verify each input reaches one buffer instance and fail preflight on any broadcast/multiple-recipient routing. Set `config["policies"]["resources"]["core_allocation"] = {"type": "core_set", "set": [{"start": core, "end": core} for core in cores]}`. Do not force a new buffer path on restart or replace a pre-existing retained directory. For measurement overrides deep-merge nested maps into the example config, then serialize the final file and hash it. Keep legacy Engine calls unchanged.

- [ ] **Step 4: Enforce the environment policy for every run**

```python
def check_worker_affinity(observed, expected):
    if observed.keys() != expected.keys():
        raise AssertionError("affinity: worker TID mapping mismatch")
    for tid, wanted in expected.items():
        if observed[tid] != wanted:
            raise AssertionError(f"affinity: tid={tid} actual={observed[tid]} expected={wanted}")
```

Use the shared host-visible path `/tmp/series-parquet-host-measurement.lock` for all checkout/container launchers; acquire `LOCK_EX | LOCK_NB` and record owner PID/start time/run ID. A busy lease aborts before traffic. CI build/provision steps run before acquisition, with a runner-wide concurrency group so jobs sharing a physical host cannot measure concurrently.

Enumerate `/proc/PID/task/*/status` at readiness and on every monitor tick; parse `Name`, `Pid`, `NSpid` and `Cpus_allowed_list`. Map pipeline TIDs using existing pipeline-thread identity/log context plus the unique requested singleton CPU per worker. Linux truncates thread names, so never infer the core ID from a truncated `Name`; fail an ambiguous mapping. Require the expected count and exact per-worker singleton set before input, after restart, throughout measurement and at end. Capture all observed thread affinities, with process role, in both snapshots. `controller/src/lib.rs:2790` only warns if pinning fails, so configuration alone is insufficient. Resolve physical cores/SMT siblings through sysfs and enforce the minimum/role allocation in Global Constraints.

Poll host `/proc/*/cmdline` and process ancestry at most 100ms apart for cargo/rustc compilation and docker build/buildx/buildctl clients; include running build containers via Docker events/inspect for the run interval. Distinguish an idle build daemon from an active build; capture command/PID/start time or build ID without environment secrets. A coverage gap or inaccessible host/build namespace invalidates publishable evidence. Any concurrent build detected after preflight invalidates the whole run even if it later disappears. Stop this run cleanly, retain observations and never terminate unrelated work. Record load at start/end and during sampling; invalid environment, affinity or build checks cannot be overridden by a new fingerprint.

- [ ] **Step 5: Wire fast CI and verify the real launcher**

Add buffered smoke with the same oracle. CI builds with `series_parquet,aws,durable-buffer`, runs all original E2E tests, then `test_measurement`; set no long flag. Retain results/logs with `actions/upload-artifact` on both success and failure. Extend AlloyProducer with optional admin metrics port and host PID discovery, reuse its existing River config, and sample queue size/capacity and enqueue failures without treating infinite-retry `send_failed` as a reliable counter.

Run `harness-local` under the enforced host controls. Register `launcher-ci` to execute strict and buffered local smokes and persist their independent result files plus an index. Apply the Controller baseline policy and direct discovered defects to Task 12. CI retains JSON/logs on success or failure and serializes publishable measurement jobs by physical host.

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
python3 -m crates.validation.tests.series_parquet.measure run --case harness-local --output-dir /tmp/series-measure
python3 -m crates.validation.tests.series_parquet.measure run --case launcher-ci --output-dir /tmp/series-launcher
SERIES_REQUIRE_DOCKER=1 python3 -m unittest crates.validation.tests.series_parquet.test_e2e -v
```

**Recorded numbers:** observed PID/TID affinity, role/core counts, start/end environment, monitor coverage/build detections, strict/buffered oracle counts and legacy test outcomes in `launcher-ci.json`. **Failure:** wrong graph, lost retained path, affinity mismatch, unavailable cores/lease, concurrent build, missing snapshot, changed legacy behavior or absent required Docker coverage.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/harness-local.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/launcher-ci.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/harness-local.json docs/superpowers/reports/series-parquet-measurement/launcher-ci.json
git commit -m "chore: enforce series measurement launch and host controls" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 3: Layered Criterion and complementary benchmark mechanics

**Expected wall-clock cost:** 15-30 minutes for the full stage matrix; 15-30 seconds for reduced contract tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/series-lake/Cargo.toml`
- Modify if resolution changes: `rust/otap-dataflow/Cargo.lock`
- Create: `rust/otap-dataflow/crates/series-lake/benches/layered.rs`
- Create: `rust/otap-dataflow/crates/series-lake/benches/measurement.rs`
- Create: `rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs`
- Create/Test: `rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/stages.json`

**Interfaces:**
- Consumes `build_request`, `RunSpec`, `write_result`, `DockerStore`, and actual `extract`, `Block`, `SeriesCache`, `sort_batch`, `merge_runs`, `Sink` APIs in the ground-truth table.
- Produces bench CLI `measurement --stage NAME --input PATH --config PATH --output PATH --iterations N --profile timing|heap --compression zstd|none`; input is deterministic length-prefixed OTLP requests (u32 little-endian length, one signal per file), with sidecar workload JSON.
- `run_stages(spec: RunSpec, output_dir: Path) -> dict` invokes one new process per stage/repetition/profile, aggregates into `stages.json`, and exports exact stage names `otlp_noop`, `otlp_convert`, `otlp_extract_hash`, `otlp_sort`, `otlp_parquet_local`, `otlp_parquet_zstd`, `otlp_minio`, `convert`, `extract`, `sort_seal`, `merge`, `encode`, `local_write`, `upload`, and `sink`.

- [ ] **Step 1: Add failing stage-schema and equivalence tests**

```python
# Scenario: the registered OTLP-to-noop stage omits allocation data.
# Guarantees: every stage, including the pipeline baseline, has the full metric schema.
def test_otlp_noop_requires_all_metrics(self):
    with self.assertRaisesRegex(AssertionError, "allocated_bytes_per_record"):
        validate_stage_result({"stage": "otlp_noop", "metrics": {}})
```

`validate_stage_result(result: dict) -> None` checks the mandatory stage fields listed below in fixed order beginning with `allocated_bytes_per_record`, requiring numeric finite values and positive sample counts. It is owned by `performance.py`; unmeasured mandatory fields invalidate the run.

Place this test in the existing `test_measurement.py`. Since the bench has `harness = false`, its Rust checks are explicitly invoked by `--self-test`; do not put unreachable `#[test]` functions behind a custom harness and assume cargo runs them. Declare `mod stages; mod tests;` with explicit `#[path = "measurement/stages.rs"]` and `#[path = "measurement/tests.rs"]` in the bench root. The `--self-test` branch invokes `tests::run() -> Result<(), Box<dyn std::error::Error>>`, then exits without a measurement. Include self-test calls for diagnostic encoding versus actual Sink, exact stage row counts and input generation excluded from timing, with Scenario/Guarantees comments; use the public pdata fixture constructors and independent readers already used by library sink tests. Each is called by `tests::run`; any failed assertion/Result exits nonzero. Conversion/encoder peaks are measured and evaluated through the shared Controller baseline policy; do not add a `check_expansion` helper that rejects engineering reservations as hard ceilings.

- [ ] **Step 2: Run red before adding the bench**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
cargo bench -p otel-arrow-dfe-series-lake --bench measurement --no-run
```

Expected: missing stage validator and bench targets. Cargo build/self-test commands here are setup verification outside all measurement leases; only the prebuilt executable runs launched by `measure stages` produce publishable timing evidence. Cargo is executed only by the future implementer.

- [ ] **Step 3: Add the purpose-built target and timing primitive**

Add workspace dev dependencies `criterion`, `cpu-time`, `dhat`, and `bytes` only where used; enable object_store's `aws` feature for the bench through its dev dependency. Declare:

```toml
[[bench]]
name = "measurement"
harness = false

[[bench]]
name = "layered"
harness = false
```

The entry point is `fn main() -> Result<(), Box<dyn std::error::Error>>`; it parses the documented flags, constructs a current-thread Tokio runtime and writes JSON through `serde_json::to_writer_pretty`. Do not run the whole asynchronous upload future inside an extraction timer. Use this synchronous primitive for CPU stages:

```rust
#[derive(serde::Serialize)]
struct Timing {
    wall_ns: u128,
    cpu_ns: u128,
}

fn timed<T, E>(run: impl FnOnce() -> Result<T, E>) -> Result<(T, Timing), E> {
    let wall = std::time::Instant::now();
    let cpu = cpu_time::ThreadTime::now();
    let value = run()?;
    let timing = Timing {
        wall_ns: wall.elapsed().as_nanos(),
        cpu_ns: cpu.elapsed().as_nanos(),
    };
    Ok((value, timing))
}
```

For the single-thread async stage measure process CPU before/after `runtime.block_on` with `cpu_time::ProcessTime`, and wall time separately; there are no concurrent benchmark stages. Hold the host lease around directly invoked Criterion/diagnostic subprocesses as well as engine runs; `run_stages` launches prebuilt bench executables, never cargo during measured intervals. Run fixture construction, input cloning, filesystem setup, warm-up and output verification outside the timed region. Use black_box on retained inputs/outputs, not a discarded future. Each timing process executes at least 30 samples and one second of accumulated measured work; cap iterations by a 60s deadline and mark incomplete if the minimum is not met.

For allocation mode use the safe `dhat::Alloc` global allocator in this bench executable only and one DHAT profiler per process. Start it after fixtures are created; record `HeapStats` before/after and maximum live allocation during the stage, then drop the stage output under the profiler. Profile fixture-retained bytes separately to distinguish input from workspace. DHAT instrumentation results never supply throughput numbers. No unsafe allocator wrapper or global profiler is added to production library code.

- [ ] **Step 4: Implement and validate every stage against the production path**

| Stage | Timed operation | Untimed input and correctness check |
| --- | --- | --- |
| `otlp_noop` | Same Python sender into real engine OTLP receiver plus existing noop exporter, with acknowledgements enabled | Calibrate producer/network capacity and process CPU; this is a pipeline baseline, not a library stage or durable-storage claim. |
| `convert` | Construct `OtapPayload` from serialized `OtlpProtoBytes`, then `try_into_with_default::<OtapArrowRecords>` using the production trait call | Prepared wire bytes; count resulting rows and pinned Arrow buffers with `CountedAllocations`. Include conversion lifetime overlap with input bytes. |
| `extract` | `extract(&mut records, &cfg)` including identity canonicalization/hash | Fresh converted input; verify descriptors, IDs, row types and semantic hashes. |
| `sort_seal` | `reserve`, `admit` through bounded runs, then `seal(fixed_microseconds)` | Pre-extracted batches, cache with specified hit/miss distribution; record admission and final seal timings separately as nested non-overlapping sub-stages. |
| `merge` | Construct and fully consume `merge_runs` | Prepared sorted runs; verify merged order and exact row multiset, retain at most one output chunk. |
| `encode` | ArrowWriter encoding of prepared merged chunks, first uncompressed then ZSTD | Identical chunk stream. Match production dictionary/statistics/row-group settings and memory-triggered flush predicate. Record encoded bytes and writer memory maxima; record output sink capacity separately and exclude it only from a contemporaneous workspace measurement. |
| `local_write` | Write pre-encoded bytes through actual LocalFileSystem object_store | New object path; verify byte hash after completion. Also report actual Sink-to-local end-to-end cost; state object_store completion semantics, without claiming host power-loss durability. |
| `upload` | Pre-encoded bytes through actual S3 object_store `BufWriter`, production part size/concurrency, complete multipart or PUT | MinIO/RustFS container from DockerStore. Verify HEAD size and downloaded hash. No extraction or compression is inside this stage. |
| `sink` | Actual `Sink::write_block(&sealed_block, &CancellationToken)` | Cross-check descriptor coverage, values, compression metadata, row-group policy and output lengths against diagnostic pipeline; byte-for-byte equality is unnecessary when file metadata differs. |

Add `encode_chunks(chunks: &[RecordBatch], cfg: &LakeConfig, compression: Compression) -> Result<Vec<u8>>` in stages.rs. Use `parquet::arrow::ArrowWriter`, copy the actual writer-property construction from `sink.rs`, write each chunk, flush when `memory_size() >= writer_limit_bytes` or `in_progress_size() >= row_group_bytes`, then close/return bytes. The equivalence test checks properties against actual Sink output so a later sink change cannot silently stale the diagnostic bench. No alternative encoding implementation or exporter compression knob is introduced.

The encoder's concrete core is below; its `Result` is `std::result::Result<T, Box<dyn std::error::Error>>`. The caller collects the allocation/timing measurements around this operation and checks row-group/compression semantics against Sink outside the timed region:

```rust
fn encode_chunks(
    chunks: &[RecordBatch],
    cfg: &LakeConfig,
    compression: Compression,
) -> Result<Vec<u8>> {
    let first = chunks.first().ok_or_else(|| std::io::Error::other("empty input"))?;
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_dictionary_enabled(true)
        .set_max_row_group_row_count(None)
        .build();
    let mut encoded = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut encoded, first.schema(), Some(properties))?;
    for chunk in chunks {
        writer.write(chunk)?;
        if writer.memory_size() >= cfg.parquet.writer_limit_bytes
            || writer.in_progress_size() >= cfg.parquet.row_group_bytes
        {
            writer.flush()?;
        }
    }
    let _metadata = writer.close()?;
    Ok(encoded)
}
```

Diagnostic bytes deliberately omit file-identity metadata; actual Sink output supplies the authoritative format checks. For timed runs use an output-capacity warm-up estimate and record Vec capacity/allocation separately; allocation-mode runs must include all growth and finalization. Do not subtract the entire output length from an unrelated heap peak or remove encoder allocations from allocated-bytes/record.

Run default keys and wide custom log body keys, small/large bodies, stable/churning series and mixed histogram widths. Include 1KiB and 8KiB bodies, plus a legal near-row-limit fixture at 512KiB. Reject any fixture that violates the current config constraints; it cannot be used to claim supported throughput.

- [ ] **Step 5: Register the cumulative Criterion layers and full metric contract**

`benches/layered.rs` uses workspace Criterion 0.8 with `harness = false`, a fallible `main`, the stage functions above and no production allocator changes. Register these cumulative groups in order: `otlp_noop` (prepared OTLP bytes consumed by the synchronous noop path), `otlp_convert` (plus conversion), `otlp_extract_hash` (plus extraction/hash), `otlp_sort` (plus admission/seal/merge), `otlp_parquet_local` (plus uncompressed Parquet/local persistence), `otlp_parquet_zstd` (same with ZSTD). The real OTLP network-to-noop pipeline is also registered as `otlp_noop` with `mode: pipeline`; Criterion's synchronous noop floor has `mode: criterion`. Neither mode substitutes for the other. Actual sink/MinIO cumulative `otlp_minio` and isolated upload are complementary async runs in `measurement`; spec 9.6's full layer ladder therefore reaches the real store.

Use `criterion.benchmark_group(stage)`, `sample_size(30)`, `warm_up_time(Duration::from_secs(1))`, `measurement_time(Duration::from_secs(5))` and `Throughput::Elements(record_count)`. `iter_batched` prepares/clones inputs outside timing; the timed closure invokes all cumulative operations for that layer and `black_box`s retained outputs. Keep each Result error in an outer error slot and return it from fallible `main` after the group finishes; never time silently failed operations. Local persistence uses the prepared per-iteration path and synchronous file writes/completion; the complementary actual LocalFileSystem/Sink run validates its output and states completion semantics. Criterion is used for cumulative wall-time distributions, not async wait or allocation attribution. A process per layer/repetition prevents previous stages from contaminating RSS peaks.

All registered stage results, including `otlp_noop`, require `stage`, `mode`, `sample_count`, `repetition`, `fingerprint`, and metrics `allocated_bytes_per_record`, `records_per_s_per_core`, `cpu_ns_per_record`, `peak_rss_bytes`, `output_bytes_per_input_record`, `wall_ns_per_record`, `peak_live_heap_bytes`, `peak_workspace_bytes`; record `output_representation`, CPU/core allocation and config. Noop emits no stored output: report output bytes as measured zero with `output_representation: none`; count successful zero-partial-rejection RPCs and reconcile sent supported counts. Noop has no descriptor/storage oracle, recorded as not applicable; all storage stages require the common oracle. Collect its real engine CPU/RSS and paired heap allocation profile under the same workload/config; profiling has a distinct build/profile fingerprint linked by `workload_config_id`. Missing profiling data is incomplete acceptance, never a fabricated zero.

The full metric schema belongs to each registered composite stage result; its timing, heap and pipeline child runs have explicitly declared profile-specific mandatory fields. A heap child never supplies throughput. Each child still has the common environment/config/workload/provenance and hard validity checks, and references in the composite identify exactly which child measured each field. Completeness rejects a composite with any missing required measurement; differing profile fingerprints are not regression-comparison matches.

`run_stages` combines Criterion's estimates/raw sample artifact hashes with complementary CPU/DHAT/RSS runs using `(stage, mode, workload_config_id, repetition)`. Keep separate complete fingerprints per profile and compare each only within its profile. Require three independent repetitions of each group and 30 samples/at least one second of measured work per timing process; publish medians/ranges. Baseline decisions use the Controller policy in Global Constraints. Route equivalence, validity or measured regression defects to Task 12.

- [ ] **Step 6: Run green, apply failure gates, record and commit**

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
cargo bench -p otel-arrow-dfe-series-lake --bench measurement -- --self-test
cargo bench -p otel-arrow-dfe-series-lake --bench layered --no-run
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure stages --output-dir /tmp/series-stages
```

**Recorded numbers:** Criterion distributions and all registered stage metrics above, conversion/encoder expansion, equivalence counts and three repetition dispersions in `stages.json`. **Failure:** incorrect output, hidden timed work, missing Criterion group/registered stage/mandatory metric, insufficient samples or unstable repetitions, hard residual/validity checks, or regression under the Controller baseline policy. Preserve engineering-reservation comparisons as measurements and route discovered defects to Task 12. Reduced tests run in fast CI; full/profiled measurements are opt-in.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/stages.json)
git add rust/otap-dataflow/crates/series-lake/Cargo.toml rust/otap-dataflow/crates/series-lake/benches/layered.rs rust/otap-dataflow/crates/series-lake/benches/measurement.rs rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs rust/otap-dataflow/Cargo.lock rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py docs/superpowers/reports/series-parquet-measurement/stages.json
git commit -m "chore: measure layered series parquet benchmarks" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 3b: Umbrella review remediation, part 1 -- exporter correctness under faults

**Origin:** the whole-branch umbrella review `docs/superpowers/umbrella-review-2026-09-22.md` (16 seats, verdict "request changes"). Findings numbered below refer to that document. Controller ruling: the bounded exporter defects it found change the very behaviour Tasks 9-11 and 13 are about to measure (outage handling, restart, nack semantics, at-least-once), so they are fixed BEFORE the fault matrix, not after it. Findings that are product or upstream decisions are listed at the end as deferred with the decision owner named. Follow the Task 12 procedure for every item: reproduce, regression test that fails first, bounded fix, chloggen entry where user-facing, ASCII only, Scenario/Guarantees comments.

**Expected wall-clock cost:** 2-4 hours of implementation; unit tests only, no measured runs.

**Files:** `crates/core-nodes/src/exporters/series_parquet/{worker.rs,flush.rs,token.rs,window.rs,config.rs,mod.rs,tests.rs,README.md}`, `crates/series-lake/src/{error.rs,buffer.rs,attrs.rs,sink.rs}`, `crates/otap/src/object_store.rs` (validation hook only), `.chloggen/series-parquet-review-fixes.yaml`.

- [ ] **Item A (finding 4): storage outage must not surface as "cancelled" with zero retries.** Validate at config load that the effective object_store `retry.retry_timeout` (including its 180 s default) is strictly less than `flush_retry_deadline`; reject otherwise with a message naming both values. Report deadline expiry as a distinct flush outcome that carries the last underlying error; log every failed attempt at WARN with the error. Test: a flush whose single put outlives `flush_retry_deadline` under the object_store default retry yields a retryable nack at the deadline with the underlying error surfaced, and `flush.retries` counts the attempts.
- [ ] **Item B (finding 5): internal errors are not permanent client refusals.** Add `Error::Internal` to series-lake; map only `Error::Refused` to `Failure::Permanent` in `worker.rs`; internal and Arrow errors become retryable nacks with a bounded, sanitized detail string instead of the bare token `invalid`. Test: an injected builder mismatch produces a non-permanent nack with detail.
- [ ] **Item C (finding 9): shutdown drains deliver every token.** Drain through `tokio::task::coop::unconstrained` or a try_send path; queue force-drained refusals through the notifier until the shutdown deadline; fix or delete the three "reserved slot" comments. Test: drain more than 128 tokens into a completion channel with room for all and assert all are delivered.
- [ ] **Item D (finding 11): a backward wall-clock step must not stall rotation.** Rotate once one interval of monotonic time has passed, keeping the floored window start with reemit; bound TooEarly sleeps to one interval. Test: a step back larger than the interval rotates within one monotonic interval.
- [ ] **Item E (finding 12): `RequestTooLarge` permanence is decided against the worst case** (all descriptors new, reemit on); otherwise park and retry. Validation includes series-row inflation. Test: the same request admitted once cold and once warm has an identical outcome.
- [ ] **Item F (finding 7): dictionary-encoded OTAP input is bounded before expansion.** Bound the expanded size before casting to Utf8, or read through the dictionary; charge cells against `max_row_bytes` as they are read. Test: one large dictionary value referenced by many rows is refused before expansion.
- [ ] **Item G (minor items, cheap):** let a started multipart `put` finish bounded by `abort_timeout` before aborting; poll the write before the deadline branch so a late success is not nacked and correct the flush.rs:31-33 claim; do not retry `PermissionDenied`/`NotFound` until the deadline; rate-limit the per-request refusal WARN and include signal and size-vs-limit; cap `max_nesting_depth` at 256; reserve the hive keys `v`, `signal`, `dataset`, `date`, `hour` as denormalized column names; reject or honour `metrics.series_attributes` instead of ignoring it; reject `-` in `writer_id`; remove the dead rotate/resume arm with a `debug_assert!(cleaning.is_none())`; lazy or capped preallocation from unbounded config values.
- [ ] **Consistency review items folded into 3b** (`docs/superpowers/concistency-review-2026-09-22.md`, findings C12, C14, C21, C3-partial): the nack `reason` becomes a sentence naming the limit and the remedy, the token stays only as the metric label (C12, joins Item B); temporality is decoded through `AggregationTemporality::try_from`, not integer literals (C14); `writer_id` adopts journald's `[A-Za-z0-9_.]` class, `-` excluded (C21, joins Item G); the dictionary path of Item F reads through the already-public `MaybeDictArrayAccessor::<UInt16Array>` / `<Int64Array>` where it applies (C3, the rest of C3 needs a pdata visibility PR and is deferred).
- [ ] **Remaining umbrella minors placed here:** sink abort uses the engine clock, not `tokio::time::timeout`, so it is simulable; an empty ACTIVE block does not wait for the flush slot at a window boundary; while touching Item F, verify whether the OTLP receiver decodes with a prost recursion limit before handing raw bytes on, and record the answer (it decides the stack-overflow half of the CBOR encoder concern; the pdata CBOR encoder's own unbounded recursion is a pre-existing upstream follow-up, note it, do not fix it here).
- [ ] **Tests from the review that belong here:** factory `create` with an S3 config needing a token provider (with and without the capability); `tests.rs:657` and `:1018` assert `worker.notify` is empty after `rotate()`; `AckToken::split` strips headers and claims; config-reject table checks error substrings through `validate_config`; exact-boundary admission at `1_000_000_000` ns; CBOR depth at limit-1 and with nested maps.
- [ ] **Commit** per item or per coherent group, staged by file name, one chloggen bug_fix entry for the user-facing changes (`component: pipeline`), then run `cargo xtask check` for the workspace.

### Task 3c: Umbrella review remediation, part 2 -- throughput path, CI and documentation hygiene

**Expected wall-clock cost:** 1-3 hours; no measured runs except one launcher-ci smoke to confirm the workflow commands still pass locally.

- [ ] **Item H (finding 6): the metrics series memo must hit.** Key it on content (metric id plus hash of sorted point attributes), check `seen` before building and charging a `DescriptorRow`, share resource and scope attribute lists per request. Test: a metrics request with about 8k points under a 25-attribute resource at default limits is accepted, and the memo hit rate is asserted. Record before/after `extract` ns/record from the stage bench in the report; a Task 3 family re-measurement is NOT required here, Task 4 consumes the new number.
- [ ] **Item I (finding 10): make the ack-hold concurrency limit visible and sized.** Add an admission-closed gauge (`accept == false` count) to the exporter metrics; document `in_flight = rate x (interval/2 + flush) / records_per_request` in the README sizing section and size `configs/series-parquet-s3.yaml` and the Alloy config from it. Task 5's matrix already gains a raised-concurrency launcher case (see Task 5 amendment below).
- [ ] **Item J (finding 17): plain `cargo bench` must not fail.** The two harness-driven benches exit 0 with a skip message when their inputs are absent.
- [ ] **Item K (finding 2): fix the workflow.** Narrow `paths` to `rust/otap-dataflow/crates/series-lake/**`, `crates/core-nodes/src/exporters/series_parquet/**`, `crates/validation/tests/series_parquet/**` and the workflow file; key concurrency on `${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}` with cancel-in-progress; reuse toolchain, rust-cache and disk-free steps from `rust-ci.yml`; SHA-pin actions; keep the debug legacy E2E lane on pull_request; move the release build and the launcher-ci measurement smoke to `workflow_dispatch`. Verify by running the pull_request lane's commands locally.
- [ ] **Item L (findings 15, 16, minors): docs and manifest agree with the code.** Controller ruling: the exporter is OPT-IN (`--features series_parquet`), so remove `series_parquet` from `core-exporters` in `core-nodes/Cargo.toml` and regenerate `components-baseline.json` if it is feature-derived; fix `configs/README.md:258` (metrics are accepted); remove "does not exist yet" from the series-lake README and chloggen; one changelog entry per PR-shaped change with a single placeholder marker style; correct exporter README:984-985; inline or remove the `.superpowers/` link at spec line 1356 so lychee passes; SPDX header on `gen_golden.py`; trailing newline on golden JSON; move the static MinIO credentials in `configs/series-parquet-s3.yaml` to env substitution.
- [ ] **Consistency review items folded into 3c** (same document): C1 publish policy: `crates/series-lake/Cargo.toml` gets `publish = true`, `keywords`, `categories`, and `otel-arrow-dfe-series-lake` joins `PUBLISH_PACKAGES` in `xtask/src/publish_policy.rs`, mirroring quiver, so `crates_publish` stops failing. C4 rename the Cargo feature to `series-parquet` everywhere (manifests, workflow, READMEs, changelog), URN unchanged. C5 rename the module directory to `series_parquet_exporter` and the constant to `SERIES_PARQUET_EXPORTER_URN`, a pure move. C6 metric names follow the semantic-conventions guide: `rows.written`, `files.written`, `series.emitted`, `dropped.unsupported`, `oldest_unacked.age` (unit s), `flushes`; acks/nacks use `{message}`; nack label key `error.type`; split `DatasetLabel` into `signal` plus `dataset`. Every harness reference to a renamed metric (test_e2e.py, measurement.py, performance.py, the README) is updated in the same commit and the contract tests plus one E2E run prove it. C7 log events: `series_parquet.shutdown.deadline_exceeded`, `series_parquet.flush.failed`, `series_parquet.seal.failed`, plus a `series_parquet.start` event carrying writer_id, boot_id and storage. C8 register `ExporterExportMetrics` beside the local sets. C9 move the token-requirement, file-storage-retry warning and store-construction blocks into one shared function in `crates/otap/src/object_store.rs` used by both exporters; drop the redundant `retry.validate()`. C10 replace `normalized()` key-suffix rewriting and the `Value` sections with `byte_units` field annotations (add `deserialize_required_u64` to the config crate and use it in journald too). C11 one validation error per field naming the user-facing dotted key; wrap `decode()` errors with the section prefix. C15 gate both benches behind `required-features = ["bench-harness"]` AND exit 0 with a skip message when inputs are absent (joins Item J). C20 add `shutdown_deadline()` to `ProcessorInbox` with an `enhancement` chloggen entry for the engine API.
- [ ] **Introspection minors (umbrella seat 8; the user's goal names logs and metrics as first-class):** split `too_large` into its four budgets as distinct `error.type` values and stop counting nesting depth as `invalid`; `block_committed` carries window, sequence and object path; a shutdown outcome summary event (accepted, acked, nacked, abandoned, duration); retries are counted on abandon, not only on completion; a start-up warning when the default per-worker budget multiplied by the engine's core allocation exceeds available RAM, and one when `window.interval + 2 x (flush_retry_deadline + abort_timeout)` exceeds the documented 60 s signal grace. Also revert the unrelated windows-sys re-resolution in Cargo.lock, and run `measurement --self-test` in the pull_request CI lane.
- [ ] **Needs-verification items from the umbrella review, resolved here by reading or by a one-line test:** whether object_store's `LocalFileSystem` fsyncs (if not, the README states that for `storage: file` an ack is durable only against process crash); whether `components-baseline.json` is feature-derived; the recorded CI run duration and disk use of the pull_request lane.
- [ ] **Harness minors (7c):** `run_child` must not swallow KeyboardInterrupt; DockerStore removes its container on failed start and applies docker timeouts; `test_disconnect_does_not_remove_data` aligns to a window offset; `free_port()` retries; in `publish=false` mode the 100 ms monitor-coverage gate becomes a recorded observation, not a hard failure. From C18: parse Prometheus text with `prometheus_client.parser` and collapse the three telemetry parsers into one that never reads absence as zero; poll `/api/v1/readyz` for readiness; dedupe `free_port()`; pin `requirements.txt` with a lock file. The published JSON strips absolute host paths and credentials (umbrella finding 1, forward-looking half).
- [ ] **Commit** per item group, staged by file name; `cargo xtask check`; contract tests via the venv; one local launcher-ci smoke.

**Deferred by ruling, with owner:**
- Finding 1 (21 MB of measurement JSON in branch history, host paths and MinIO test credentials embedded): rewriting history is a force-push on the shared fork branch and is the USER's decision. Going forward, plan 3 keeps committing evidence as the plan requires, but from Task 3c on the harness must strip absolute host paths and credentials from published JSON (extend the existing canonicalisation, test it). Recommendation to the user: before any upstream PR, rewrite the branch so `docs/superpowers/reports/` never entered history and publish evidence from an orphan branch on the fork.
- Finding 3 (`schema_fingerprint` hashes Arrow Display text; not reproducible by third parties; splits scope on an arrow bump): a format change to a persisted key. Recommended before first release; USER decides. If accepted it becomes Task 3d: crate-owned versioned type vocabulary, golden fingerprints for all four datasets plus one denormalized schema, `gen_golden.py` computing them independently, FORMAT.md updated.
- Finding 8 (engine crate hard-codes one exporter's accounting): kept for the measurement campaign because Task 6 needs the residual; the offline-computation alternative and the generic registry go to plan 4. Task 6 must state the block-pair uncertainty of the in-process residual.
- Findings 13 and 14 (identity-config marker per base_uri; lake-level writer facade): plan 4.
- Format batch APPROVED by the user 2026-09-22 as Task 3d, to run after 3c and before Task 4: umbrella finding 3 plus C16 emit Parquet's native `SortingColumn` row-group metadata beside the private `sort_key`, and reuse or justify the `sort_columns` name; C17 model the fingerprint renderer on pdata's `SchemaIdBuilder` type codes; C22 spell non-finite doubles and bytes the way the workspace's OTLP JSON does (`Infinity`, base64) or document why not. All three touch persisted bytes or golden vectors, so they land together, once, before first release, or not at all.
- C2 (engine accounting parallels `retained_work.rs` and contradicts the engine RFC): same ruling as umbrella finding 8; plan 4 rebuilds it on `LocalRetainedAccount` with a handle passed through `PipelineContext`, engine-namespaced metric, its own engine PR.
- C3 and C13 are NOT deferred after all (user 2026-09-22 23:5x): the pdata visibility widening (`StringArrayAccessor`, `FixedSizeBinaryArrayAccessor`, `AnyValueArrays`, `AttributeArrays`) joins Task 3b Item F, and `OtlpProtoBytes::validate_framing()` called from all four exporters joins Task 3c. Upstream they travel in the small engine/otap prep PR.
- C18 (move the Python lane under `tools/`, orchestrator suites, harden-runner): upstream packaging; only when the user asks for an upstream PR. C19: observation, no action.
- PR split and squash: only when the user asks for an upstream PR.

### Task 3d: Format batch -- reproducible fingerprint, native sort metadata, OTLP-JSON spellings

**Origin:** umbrella finding 3, consistency findings C16, C17, C22; approved by the user on 2026-09-22 to land once, before first release. All three touch persisted bytes or golden vectors, so they ship in one commit series with one FORMAT.md revision and regenerated golden files.

**Expected wall-clock cost:** 2-4 hours; unit and golden tests only; one E2E run to prove readers still agree.

- [ ] **Fingerprint:** render a crate-owned, versioned type vocabulary modelled on pdata's `SchemaIdBuilder` type codes (not Arrow `Display`), include top-level nullability, pin golden fingerprints for all four datasets plus one denormalized schema, and have `gen_golden.py` compute them independently in Python. FORMAT.md documents the exact rendering. A test asserts the documented rendering equals the code's.
- [ ] **Native sort metadata:** emit Parquet's `SortingColumn` row-group metadata via `WriterProperties::set_sorting_columns` beside the private `sort_key`; reuse the `sort_columns` name from pdata `consts` or record in FORMAT.md why it differs. Verify DuckDB and ClickHouse read the files unchanged.
- [ ] **Spellings:** `render_v1` spells non-finite doubles `Infinity`/`-Infinity`/`NaN` and bytes as base64, matching the workspace OTLP JSON, with golden vectors for each; FORMAT.md updated.
- [ ] **Golden hashes move deliberately:** regenerate, review the diff of every golden file, and record the old and new fingerprints in FORMAT.md's revision note. This is the one sanctioned golden move; the plan-2 rule "no golden hash moved" resumes afterwards.
- [ ] **Commit** per item, staged by name, one `breaking`-style chloggen note (pre-release format change), `cargo xtask check`, one E2E run.

### Task 3g: Never acknowledge a damaged OTLP body (fifth review, P1, 2026-09-23)

**Defect, verified at HEAD:** `OtlpProtoBytes::validate_framing` (crates/pdata/src/otlp/mod.rs, calling `validate_message_wire_format` in crates/pdata/src/views/otlp/bytes/decode.rs) checks only the outer message ("without decoding nested messages"). The lazy view parser treats a damaged nested field as absent, so a body such as ExportLogsServiceRequest `[0x0a, 0x01, 0x0a]` passes validation, converts to zero rows, and worker.rs acknowledges it with nothing written. A damaged body must be refused (permanent, `Refused`, reason sentence names the damage) before any ack, and a partially damaged body must never be acknowledged with only its readable part stored.

- [ ] Reproduce first: a failing test in pdata with `[0x0a, 0x01, 0x0a]` for logs and an equivalent damaged metrics body, plus a failing exporter test that asserts the nack instead of the ack.
- [ ] Make validation schema-aware and recursive: descend into every LEN field that the OTLP schema defines as a sub-message (resource_logs -> resource, scope_logs -> scope, log_records -> attributes/body AnyValue, and the metrics tree down to data points, exemplars and AnyValue/KeyValue lists), validating each nested message's wire framing; unknown fields keep proto3 skip semantics; bound recursion depth by the existing nesting limit. Keep it allocation-free and linear in the body size.
- [ ] Every exporter that calls validate_framing gets the deeper check automatically; add one regression per exporter call site that was covered before.
- [ ] Measure the extra cost on the stage bench (otlp_convert) and record it; the check must stay a small fraction of conversion.
- [ ] chloggen bug_fix for pdata (upstream-relevant); ASCII; Scenario/Guarantees.

### Task 3h: Smoke test with processor:attribute collapsing a high-cardinality attribute (user request 2026-09-23)

**Goal:** prove the exporter coexists correctly with `processor:attribute` deleting a per-point-unique metric attribute upstream. Expected and accepted outcome: two data points that were distinct streams before the delete arrive with the same identity, get the same series_id, and nothing breaks.

- [ ] Add one E2E test to test_e2e.py (functional lane, no measurement): pipeline OTLP receiver -> `processor:attribute` with `apply_to: ["signal"]` and `actions: [{action: delete, key: request.id}]` -> series_parquet exporter (local storage is enough; one S3 variant optional). Producer sends, in one request and in two separate requests, delta sum points and delta histogram points from two streams that differ only in `request.id`, with equal timestamps, plus one cumulative sum pair.
- [ ] Assert: every request acknowledged (no nack); exactly one metrics/series row for the collapsed identity (descriptor written once per partition and worker); two metrics/values rows with the same series_id and timestamp for each collapsed pair; no `request.id` attribute survives anywhere in the written files; both DuckDB and ClickHouse read the files, and `sum(value)` over the collapsed delta series equals the sum of the originals; the series/points ratio signal from Task 5 (if already present) reports the collapse.
- [ ] Document the result in the README next to the high-cardinality strategy: delete is correct for delta sums and histograms (readers sum), wrong for cumulative (interleaved running totals) and ambiguous for gauges; spatial aggregation is not available in otap-dataflow today (plan-4 backlog item).
- [ ] Standard rules: Scenario/Guarantees comments, ASCII, SERIES_REQUIRE_DOCKER=1, measured launches unaffected.

### Task 3i: Bounded runtime work on the ingest core and an explicit loss contract (sixth review, user decisions 2026-09-23)

**Why now:** the project review criterion (rust/otap-dataflow/docs/ai/ai-assisted-pr-review.md, "single-threaded async runtime responsiveness") flags runtime-path work that monopolizes the core. Merge-key building runs synchronously for every row of every run before the first yield (series-lake sink.rs calling merge_runs, sort.rs key construction), and each chunk is encoded synchronously, so a large block delays admission, backpressure, ack/nack delivery, cancel and shutdown on the same core. Moving flush off the core stays in plan 4 with the shared writer; this task bounds the work instead.

- [ ] Measure first (reuse the Task 5 worst-stall probe if it exists, otherwise add it here): longest uninterrupted stretch of the worker thread during a flush of the largest default block for logs and metrics, and the latency from a shutdown/cancel signal to the flush observing it.
- [ ] Build merge keys in bounded slices (a configured or constant row budget per slice) with `tokio::task::yield_now()` between slices; produce and encode output chunks one at a time with a yield between chunks; check cancellation and the shutdown deadline between slices and chunks. No change to output bytes, ordering, row groups or goldens: a golden and E2E proof that files are byte-identical for the same input.
- [ ] Re-measure the worst stretch and the reaction latency; report before/after; total CPU per record must not regress beyond noise (stage bench spot run).
- [ ] **Account the flush workspace (from Task 6):** the sink publishes its live flush workspace (merge chunk, Parquet encoder buffers, in-flight upload parts) as bytes, and the worker's memory.accounted includes them while a flush runs. Task 6 showed this term is what takes the logs high-rate ledger past tolerance (33.4-46.7 MB per pair). Re-judge Task 6's two logs high-rate families once afterwards with the in-run term.
- [ ] Loss contract: add `metrics.exemplars: drop | reject` (default drop); under `unsupported: reject` exemplars are rejected too (the request is refused with a sentence naming exemplars). Count dropped exemplars (`dropped.exemplars`, per signal). Add one README section "What this exporter does not keep" listing every intentional loss with its metric: exemplars, attribute map value types (the v1 format is lossy by decision), a histogram sum of exactly zero after an OTAP round trip, and anything else a reader might expect; the section is the contract a producer can rely on.
- [ ] Standard rules: tests first where behaviour changes, Scenario/Guarantees, ASCII, chloggen for the exemplar policy, `cargo xtask check`, E2E under taskset -c 0-7,16-23.

Placement in core-nodes versus contrib-nodes is decided when the upstream PR is prepared (user decision 2026-09-23).

### Task 3f: Behaviour-preserving simplification (fourth review, user decision 2026-09-23)

**Order:** after Task 6 has merged (it edits series-lake buffer/sort and bench stages) and before the fault matrix (Tasks 9-11, 13), so the fault tasks exercise the simplified code. No behaviour, format, golden vector, metric name or config key changes; the existing unit, golden, contract and E2E suites are the safety net, and every commit keeps them green. Items 1 (worker state machine behind an event API), 3 (notifier rewrite) and 6 (LakeWriter facade) are NOT in this task: they rewrite exactly what plan 4's shared writer rewrites, and go there (see docs/superpowers/plans/2026-09-23-plan-4-backlog.md).

- [ ] **Refusal vocabulary (item 2):** two vocabularies instead of four: `lake::Error { Refused(RefuseReason), Transient, Internal }` in series-lake and one `Outcome` in the exporter from which the nack cause, the `error.type` label and the reason sentence are all derived; delete `worker::Failure` and the hand mappings; a table test covers every RefuseReason/SizeBudget -> Outcome -> (NackCause, error.type) pair.
- [ ] **Block (item 5):** drop the vestigial type parameter of `Block<T>` (the only production instance is `Block<()>`); tokens live in one place with one request counter; `Block` holds `Arc<LakeConfig>` and `reserve()` reads it from self.
- [ ] **Config leftovers (item 4):** confirm no JSON rewriting remains; remove any residual duplicate validation between `TryFrom` and `LakeConfig::validate`.
- [ ] **sink.rs split (item 7):** three modules: naming and paths; writer properties and footer metadata with a `pub fn writer_properties(compression)` the benches reuse (delete the bench's copied WriterProperties and its equivalence test in favour of calling it); the write loop with cancel and abort.
- [ ] **Metrics extraction template (item 8):** one `series_for(parent_ids, attrs_table)` path with a per-kind closure for number and histogram points (resource/scope/point attrs, denorm lookup, budget charge, memo in one place); use `arrow::array::make_builder` for type dispatch in extract/mod.rs where it replaces RowSink/AnyBuilder.
- [ ] **Log sentence (item 9):** replace the long state-to-text function in worker.rs with small Display state structs.
- [ ] **Metric sets (item 10):** at most three sets (no attributes; {signal, dataset} for rows and files; {error.type} for refusals) plus the shared ExporterExportMetrics; acks/nacks come from the shared/engine sets where they already exist. Metric NAMES stay as they are today (the harness depends on them); only the grouping changes, proven by the exact-schema contract test.
- [ ] **Tests (item 11):** split tests.rs by topic (admission/parking, rotation/windows, flush/retry, shutdown, config, metrics) with a shared test_support for simulated clocks and the store override; every test keeps its Scenario/Guarantees; the count of tests does not drop.
- [ ] **Time sources (item 13):** series-lake takes the abort deadline from outside as a future; no tokio::time remains in series-lake production code.
- [ ] **Docs as code (item 14):** replace the ~26 "spec section N" references in code comments with FORMAT.md or README section references; remove the README's embedded Python test in favour of a pointer to test_e2e.py.
- [ ] **Verification:** unit tests (count not lower than before), golden regeneration diff, contract tests, one E2E run under taskset -c 0-7,16-23, `cargo xtask check`; one stage spot-measurement of extract and sink to show no performance regression beyond noise.

Python harness unification (item 12) is decided at upstream time, with the measurement lane's fate.

### Execution slices for Tasks 3f (rest), 3k, 3j and 5a (user decision 2026-09-23)

Tasks 3k, 3j and 5a are grouped by their source (a review, the deslop plan, the CPU attribution). They are executed instead as slices by code area, so each slice owns its files, gets a small review and runs only the tests of the crates it touches. The task sections stay the requirements; each slice names the items it takes. Full gates (`cargo xtask check`, one E2E under taskset -c 0-7,16-23) run once per wave, after the wave's slice reviews approve. Every slice follows the campaign writing rules.

| Slice | Items | Files | Focused tests |
| --- | --- | --- | --- |
| S1 Engine shutdown deadline | 3j blocker 2; drop the unused `ProcessorInbox::shutdown_deadline` (3k B14 part) | engine `message.rs`, one exporter shutdown test | engine, exporter shutdown tests |
| S2 OTLP validation policy | 3j majors 4 and 5 except the series_parquet counter wiring; 3k B16 and C4; 5a double UTF-8 check | pdata `views/otlp/bytes/validate.rs`, `decode.rs`, `otlp/` conversion, file/parquet/otap exporters | pdata, the three exporters |
| S3 Harness and CI | 3j workflow path filters and `test_failures.py` unit classes in CI; 3k D6 stub subcommands | `.github/workflows/series-parquet-e2e.yml`, validation/tests/series_parquet | Python contract tests |
| S4 Sink structure | 3f items 7, 13 and the sink.rs/sort.rs part of 14; 3k B8 and C2 | series-lake `sink.rs`, `sort.rs`, benches, sink tests | series-lake |
| S5 Attribute and extraction budget | 3j blocker 3; `metric_rows` null `metric_type` and duplicate `metric_id`; 3k B1 and B2 | series-lake `attrs.rs`, `value.rs`, `extract/`, `config.rs`, `buffer.rs` sizing | series-lake |
| S6 Extraction and write speed | all of Task 5a except the double UTF-8 check | series-lake `extract/`, `buffer.rs`, `sort.rs` heap, writer properties, otap object store options | series-lake, stage spot-measurement per change |
| S7 Exporter delivery | 3j blocker 1, major 13 (shutdown under a terminate), major 12, co-tenant `Outcome::Internal`, `rotate()` debug assertion; 3k B15 and C3 | exporter `token.rs`, `worker.rs`, `mod.rs`, `flush.rs`, tests | exporter |
| S8 Exporter config, metrics and logs | 3j major 11, `unsupported: drop`, derived `retry_timeout`, fixed nack sentence, `OUTCOMES` const asserts and u64 mask, the `repaired.invalid_utf8` counter, `part_bytes` upper bound | exporter `config.rs`, `metrics.rs`, `worker.rs` telemetry, series-lake `config.rs` defaults | exporter, series-lake config |
| S9 Prose and README | 3k A1 (amended), A2-A7, remaining README contradictions of 3j | all touched files, READMEs | build and all focused suites |

Waves: (1) S1, S2, S3 in parallel, each in its own worktree; (2) S4 after Tasks 3i and 3f close; (3) S5 and S7 in parallel (disjoint files); (4) S6 after S5, S8 after S7; (5) S9 last, then the wave's full gates and the fault tasks.

### Task 3k: De-slop of prose, duplicated formulas and test scaffolding (deslop plan, user decision 2026-09-23)

**Executed through the slices above** (see "Execution slices"); this section is the requirement text the slices cite.

**Source:** docs/superpowers/deslop-plan-2026-09-23.md (item ids below are its ids). **Order:** after Tasks 3i and 3f have both closed, before Task 3j, so 3j, 5a and the fault tasks work on the compact code and their reviews read less. No behaviour, format, golden vector, metric name or config key changes; the unit, golden, oracle, fuzz and contract suites are the safety net and stay green on every commit. One commit per item. Gate policy: focused checks per item; `cargo xtask check` and one E2E only after the task review approves.

- [ ] **Prose (A2, A3, A5, A6):** one explanation per concept at the home the plan names, other copies become a one-line reference; the four module docs that retell the exporter state machine shrink to 1-3 sentences each (window.rs keeps the timer-cancellation constraint); sweep the "rather than <obvious alternative>" tails, "is what keeps" clefts and " -- " asides; fix prose that contradicts the code (the README items listed under A6 that Task 3j does not already own).
- [ ] **Test preambles (A1, amended):** repository AGENTS.md requires `Scenario:` and `Guarantees:` above every test, so they are NOT deleted. Shorten each to one specific line apiece where it restates the test name; keep longer text only where the setup is not obvious.
- [ ] **Process references (A4):** remove the remaining "spec section N", "plan 3", "Task N", "the previous review", fix-history phrasing from code comments, including sink.rs and sort.rs if Task 3f item 14 left them; the invariant stays, with a FORMAT.md or README reference where one helps.
- [ ] **Exporter README (A7):** delete `readme_states_operating_contract` first (it greps the README and blocks any edit); move the Examples section (inline Alloy file, Python script, image variables) to validation/tests/series_parquet/README.md with a short pointer; cut the durable_buffer section to a summary plus the operating contract that Task 3j keeps; deduplicate limits with the series-lake README. Target about 430 lines. The operating facts (ack meaning, loss list, drain bound, sizing) stay.
- [ ] **One size formula (B2):** delete `AttrTable::approx_bytes`; one `entry_bytes` for `map_cell` and `rendered_kv_bytes`; the series column count from `dataset_schema(...).fields().len()`; named constants for 24/64/8; delete the test that only checks two copies agree.
- [ ] **Merge step API (B8):** after 3i's final shape: `step()` infallible if it cannot fail, `last_step_rows` behind `#[cfg(test)]` or replaced by observing output, one `drain()` helper for the Ready -> build -> finish loop in tests.
- [ ] **Test seams (B15):** `Worker::samples`, `store_override`, `task_finished`, high-water getters with no production caller go behind `#[cfg(test)]` with a one-line doc, or the test asserts emitted metrics instead.
- [ ] **Validator schema once (B16):** one match per message with `Field { kind, singular }` instead of `Message::singular` plus `Message::field`; decode.rs range helpers merged; problem strings become an enum with Display. Producer-visible messages stay byte-identical.
- [ ] **Test scaffolding (C2-C7):** one `HookStore` with default-delegating hooks replaces the eight ObjectStore wrappers, `FaultStore` modes become an enum and the dead mode goes; the hand-written tracing Subscriber becomes a `tracing_subscriber::Layer`; the framing-error property is tested once in pdata and once, as a table, in the exporter; tautological and fixture-testing tests (C5) are deleted or bound to the code under test; near-duplicate tests become table rows; shared fixtures move to one test module per crate. The Rust test count may drop only by deleted tautologies and merged duplicates, each listed in the report.
- [ ] **Harness stubs (D6 part):** delete the six stub CLI subcommands that return "implemented by a later task" and register each when its task implements it.

**Not in this task:** B1 drift, B11, B13 grace copy (Task 3j); B3 (Task 5a); B12 (plan 4, with the shared writer); B14, D1-D5 (upstream preparation); items already done by Task 3f (B4, B5, B7, B10 part, B13 part, C1, A4 part).

### Task 3j: Umbrella review of 2026-09-23: defects, validation policy, shutdown under a terminate (user decisions 2026-09-23)

**Executed through the slices above** (see "Execution slices"); this section is the requirement text the slices cite.

**Source:** docs/superpowers/umbrella-review-2026-09-23.md (numbers below are its item numbers). **Order:** after Tasks 3f and 3k (it edits token.rs, worker.rs and mod.rs, which 3f restructures) and before Task 5a. Every fix starts with a regression test that fails before it.

- [ ] **Blocker 1, force-drain credit:** pass the count of tokens held outside the notifier (active, flushing, parked) into `force_shutdown`; queue a refusal only while it fits, else deliver it now; a Shutdown push over the bound delivers now and counts a failure instead of asserting. Regression: one request admitted and flushing, a stalled completion channel, at least two force-drained pdata, then (a) release the write and (b) separately let the deadline fire; no panic and exactly one decision per request.
- [ ] **Blocker 2, latched deadline:** in the engine, `closed_pdata_shutdown` returns the latched pending shutdown with its deadline whenever one is latched. Exporter regression: Shutdown latched with a 60 s deadline, flush in flight, `drop(pdata_tx)`, advance past 1 s; the flush is still awaited and acknowledged.
- [ ] **Blocker 3, decoded CBOR footprint:** charge the decoded size of attribute values (`size_of::<Value>()` per node plus content) to the table budget; decode each distinct dictionary key once and share it; attribute tables draw from the shared request `Budget` instead of a full `max_extracted_bytes` each; `value_bytes` charges the real `Value` size. Regression: a dictionary-encoded `ser` column with one 1 MiB CBOR array of small ints referenced by N rows is refused against the budget before the N-th decode, and one key is decoded once.
- [ ] **Major 4 and 5, validation policy (user decision):** `validate_framing` gets a framing-only mode (tags, wire types, lengths, nesting, packed fields; no UTF-8 check, repeated singular fields accepted) used by the file, parquet and otap exporters, which restores their pre-campaign UTF-8 behaviour from #2181. series_parquet keeps strict structure and repeated-singular-field refusal, but replaces invalid UTF-8 with U+FFFD (as #2181 does) instead of refusing, counts it in a new `repaired.invalid_utf8{signal}` counter, and the README states that two strings differing only in invalid bytes can share a series id. One shared helper with one nack cause (`Refused`) and one event name for the corrupt-body path of file and otap; the parquet exporter's per-request WARN is rate-limited. README:1255 and worker.rs:28 are aligned; the README topology section documents that an upstream processor converting OTLP to Arrow is not guarded. The changelog lists every refusal class and every affected exporter. Add a differential proptest `validate_request(b).is_ok() => prost decode ok` on mutated fixture bodies, a table-completeness test over every `Message::field` entry, and a Criterion bench of the framing walk beside the OTLP-to-OTAP conversion with the ratio in the report.
- [ ] **Major 13, shutdown under a terminate (user decision: no new configuration):** until Shutdown arrives the exporter keeps its conservative retry deadlines. Once Shutdown latches, it finishes only what has started, inside the latched deadline: the flushing block's retries and the ACTIVE block's flush are both cut to the latched deadline (no retry starts that cannot finish by it, backoff drops to its minimum), the ACTIVE block is sealed and flushed at once without waiting for its window, and whatever cannot finish is nacked retryable. The startup grace WARN is removed, since the drain now fits any grace; the README drain section is rewritten around this, and gains a Kubernetes paragraph (`terminationGracePeriodSeconds` at least the controller grace, preStop hook). Tests: with a 60 s grace and storage failing then healing, both blocks are acknowledged when the store heals inside the grace, and nacked retryable at the deadline when it does not; no attempt starts after the deadline.
- [ ] **Major 11, operability:** the deadline/cancel cleanup outcome is matched and logged as `series_parquet.flush.cleanup` (WARN with file, attempt and abort error; INFO for a late commit); add `flush.abort_failures` and `flush.late_commits`; `flush.failed` carries `window_start, seq, file, requests, bytes`, attempt events carry `seq, deadline_remaining`; `flush.failures` gets a closed `error.type` label (`deadline`, `permanent_storage`, `cancelled`, `encode`, `internal`); `flush.retries` is credited per attempt.
- [ ] **Major 12, delivery tests:** a proptest state machine on `Worker` with the simulated clocks and fault store (ops: admit, boundary, store-mode flip, release, shutdown, advance, drain notify; invariants: every request id has at most one completion at every step and exactly one at the end, ack only if both files exist); an `assert_no_more_completions` helper at the end of every completion test; a `#[cfg(test)]` drop bomb on `AckToken`.
- [ ] **Minor correctness and UX items:** `unsupported` defaults to `drop` with `dropped.unsupported{kind}` (user decision; explicit `reject` still refuses); `metric_rows` refuses a null `metric_type` and a duplicate `metric_id`; seal and admission failures nack co-tenants as `Outcome::Internal`, with a test of the `rotate()` seal-failure path; the producer-visible nack reason is a fixed sentence plus a classification, the store's error text stays in the WARN; with no `retry` section the effective `retry_timeout` is derived below the flush deadline, and only an explicit conflict is refused; `debug_assert!(tokens.is_empty())` in `rotate()`'s empty branch; `OUTCOMES` and the sample nack list checked by const asserts, the singular-field mask widened to u64; accounting returns to baseline after ack, nack and abandon, and sink cancellation and failure leave both workspace gauges at 0; the oracle mixes gauge, sum and histogram points in one request; the README contradictions listed in the review are fixed; E2E workflow path filters widened as listed; `test_failures.py` unit classes run in the writer-reader-matrix job; the harness README test count corrected.
- [ ] **Verify and record:** whether the engine nacks outstanding contexts when a node task dies (review "needs verification"); whether `_RJEM_MALLOC_CONF` or `MALLOC_CONF` is read on glibc builds, fixing PROFILING.md if needed.
- [ ] **Verification:** unit, golden, contract suites; one E2E under taskset -c 0-7,16-23; `cargo xtask check`.

**Placed elsewhere:** major 10 (logs memo, row-wise copies) and the run-seal double copy go to Task 5a; the admission-path synchronous sort stall is measured in Task 5 with the flush-stall probe; retry classification by downcast, `storage_kind` from Debug and `RefuseReason` Display join Task 3f item 2; majors 6, 7 and 8, the changelog rewrite, crate publish hygiene, image digests, harness layering, bench dev-dependencies and the pinned validator tests in the exporter go to the history rewrite before the upstream PR; the `format_revision` marker stays with format batch 2 in plan 4 (user decision of 2026-09-23). Major 9 was fixed at 103dab522.

### Task 4: Direct performance attribution and stage reconciliation

**Expected wall-clock cost:** 10-20 minutes for profiled pipeline repetitions; under one second for classifier tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/attribution.json`

**Interfaces:**
- Consumes Task 3's registered stages and full metric schema, Task 2's lease/launcher and Task 1's result/ledger/oracle contracts.
- `classify_cpu(samples: list[dict]) -> dict[str, int]` assigns each weighted perf sample exactly once to conversion, extraction, sort/seal/merge, encoding, upload, buffer, engine/runtime, allocator or unknown, using the innermost matching production frame. Upload wait is separate wall duration.
- `run_attribution(spec: RunSpec, output_dir: Path) -> dict` implements `measure attribution`, writing independent repeated run files and `attribution.json`; link stage evidence by case/workload/config and retain full per-profile fingerprints.

- [ ] **Step 1: Add a failing stage-reconciliation test**

```python
# Scenario: extraction, encoding and upload have distinct CPU stacks and durations.
# Guarantees: overlapping async wall intervals cannot inflate CPU percentages.
def test_cpu_attribution_is_exclusive(self):
    samples = [
        {"frames": ["Worker::extract", "extract::logs::extract"], "weight": 7},
        {"frames": ["Sink::write_block", "parquet::arrow::arrow_writer"], "weight": 5},
        {"frames": ["Sink::write_block", "object_store::client::http"], "weight": 2},
    ]
    cpu = classify_cpu(samples)
    self.assertEqual(cpu["extraction"], 7)
    self.assertEqual(cpu["encoding"], 5)
    self.assertEqual(cpu["upload"], 2)
    self.assertEqual(sum(cpu.values()), 14)
```

- [ ] **Step 2: Run red, then implement exclusive classification**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: absent classifier. Walk frames from innermost to outermost and use the first matching production namespace; encoder frames take precedence over ancestor Sink frames. Add sample weights once, leave unmatched samples in `unknown`, and assert classified plus unknown equals the input weight. Reject zero/negative weights and retain the mapping rules with the result.

- [ ] **Step 3: Measure pipeline CPU shares directly**

Run `perf record -F 199 -g --call-graph dwarf -p ENGINE_PID -o perf.data` while Task 2 drives the actual engine. Resolve the PID from Engine, place perf output in that run's artifacts, and stop perf after the observable input phase ends. Classify `perf script` samples by the explicit stages above, with encoding taking precedence over ancestor sink/upload frames. Report per-core CPU seconds, CPU ns/record, sample count/confidence, extraction/encoding/upload CPU percentages, scheduler/off-CPU wall fraction and named residual. Require at least 10,000 classified samples across repeated runs; lengthen the opt-in profile if needed. If perf permissions prevent profiling, preflight skips the attribution run; mandatory acceptance remains incomplete until it is run on a permitted host. Do not infer CPU shares by subtracting two end-to-end throughput numbers or normalize overlapping upload waits into a CPU pie chart.

Report per stage records/s/core, CPU ns/record, allocated bytes/record, peak RSS, peak live heap/workspace and output bytes/input-record. Task 3 supplies full per-stage timing/allocation/RSS/output metrics; attribution supplies exclusive CPU shares and wait time. Join the evidence without pretending different output representations share a byte-rate denominator.

- [ ] **Step 4: Verify, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure attribution --output-dir /tmp/series-attribution
```

**Recorded numbers:** exclusive sample counts/shares, CPU/core seconds, CPU ns/record, upload wait, unknown/reconciliation residual, profile overhead and three repetitions in `attribution.json`. **Failure:** overlap/double counting, fewer than 10,000 classified samples, unavailable mandatory perf data or invalid reconciliation. Apply the Controller baseline policy to numerical performance; route defects to Task 12.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/attribution.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py docs/superpowers/reports/series-parquet-measurement/attribution.json
git commit -m "chore: attribute series parquet pipeline CPU costs" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 5a: Cheap CPU wins found by the Task 4 attribution (user decision 2026-09-23)

**Executed through the slices above** (see "Execution slices"); this section is the requirement text the slices cite.

**Evidence (Task 4, family f001/f002, one core, taskset 0-7,16-23):** logs-1k-stable 5,271-5,345 engine CPU ns/record: encoding 33%, extraction 16%, allocator 12%, engine runtime 12%, sort/seal/merge 11%, conversion 8%, upload 6%. metrics-mixed 2,423-2,453 ns/record: extraction 24%, conversion 20%, allocator 15%, engine runtime 14%, sort/seal/merge 13%, encoding 11%. Writer properties today (series-lake sink.rs around 529-536): ZSTD at the parquet crate's default level, page statistics and dictionary encoding on EVERY column; S3 uploads use object_store's default signed payload (a SHA-256 over every uploaded byte).

Each experiment is measured on the stage bench (encode, sink, upload stages) and confirmed by one attribution repetition; a change enters the defaults only if it lowers CPU per record beyond noise WITHOUT raising stored bytes beyond a stated bound (report the compressed size ratio) and without changing schema, data, sort order or golden fingerprints. File bytes may change; readers (DuckDB, ClickHouse) must read the result, proven by the E2E suite.

- [ ] **Per-column writer properties:** disable dictionary encoding (and page statistics, keeping chunk statistics) for high-entropy columns (log `body`, attribute blobs, trace/span ids if present); keep dictionaries for low-cardinality columns (severity, series_id-adjacent keys, metric names). Try ZSTD levels 1 and 3 against the current default on the same data. Report CPU/record, bytes/record and read time for a representative DuckDB and ClickHouse query.
  Constraint (compaction chat, 2026-09-23): whatever is disabled on high-entropy columns, page statistics and the page index stay enabled on `series_id` and `time_unix_nano` (and on `metric_name` for metrics), because a compactor and readers prune on them.
- [ ] **S3 unsigned payload:** measure `with_unsigned_payload(true)` (SigV4 UNSIGNED-PAYLOAD) on the MinIO lane; if adopted, make it a documented S3 option that defaults on only for TLS endpoints (payload integrity then comes from TLS; for plain HTTP keep signed payloads), and state the trade-off in the README.
- [ ] **Allocation churn:** using Task 6's measurement of builder capacity and allocator share, reuse values/series builders across requests (reset instead of reallocate) and size them from the actual row count; measure the allocator share before/after.
- [ ] **Double UTF-8 validation:** after Task 3g, validate_framing checks every protobuf string once, and OTLP-to-OTAP conversion checks the same strings again when building string arrays (pdata encode/record/array.rs). Let conversion trust bytes validate_framing accepted on the exporter path, or switch the validator to simdutf8 (already in Cargo.lock); measure the logs otlp_convert stage before and after.
- [ ] Re-run one attribution repetition per workload at the end and report the new shares; update the Task 5 expectations with the new ns/record.

Larger candidates go to plan 4 (see the backlog): sorting on fixed-width keys (series_id + time) instead of materialised row-format keys, and extracting directly from OTLP bytes through pdata views without building OTAP Arrow first (conversion is 20 percent of metrics CPU).


**Amendment (umbrella review 2026-09-23, major 10 and a minor):** mirror the metrics memo on the logs path: a borrowed content key and a fast hasher, an owned descriptor only on a miss; append values directly into typed builders from borrowed cells and move the log body out of `Value::Str` instead of copying it three times; cache `producer_id` per resource id; hoist `denorm_columns` out of the metrics point loop. At run seal, sort the key columns to a permutation and interleave once instead of `concat_batches` then `take`; keep the merge heap over `(run, seg, off)` without an `OwnedRow` per popped row. Each change keeps output byte-identical and is measured with the Task 4 attribution.

### Task 5: Maximum sustainable throughput and durable write speed

**Expected wall-clock cost:** 90-180 minutes for capacity search and three repetitions across stores/core counts; under one second for arithmetic tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/capacity-local.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/capacity-minio.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `run_case`, `RunSpec`, `Ledger`, `read_oracle`, strict telemetry and `run_stages` output.
- Produces `capacity_search(spec: RunSpec, output_dir: Path) -> dict` and `rates(unique_records: int, wire_bytes: int, object_bytes: int, seconds: float, workers: int) -> dict`.
- Each capacity result index references individual trial/repetition JSON files; its winning rate is the median sustainable unique-record rate from three independently drained repetitions, with a failed higher offered-rate bracket.

- [ ] **Step 1: Add a failing rate-units test**

```python
# Scenario: two workers complete 12,000 unique records and 3MB of objects in 3s.
# Guarantees: records, input bytes and output bytes retain distinct denominators.
def test_capacity_units(self):
    measured = rates(12000, 12000000, 3000000, 3.0, 2)
    self.assertEqual(measured["records_per_s"], 4000)
    self.assertEqual(measured["records_per_s_per_core"], 2000)
    self.assertEqual(measured["input_bytes_per_s"], 4000000)
    self.assertEqual(measured["object_bytes_per_s"], 1000000)
```

- [ ] **Step 2: Run red**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing `rates`, with earlier contracts still green.

- [ ] **Step 3: Implement rate arithmetic and a bounded search**

```python
def rates(unique_records, wire_bytes, object_bytes, seconds, workers):
    if seconds <= 0 or workers <= 0:
        raise ValueError("positive duration and worker count required")
    return {
        "records_per_s": unique_records / seconds,
        "records_per_s_per_core": unique_records / seconds / workers,
        "input_bytes_per_s": wire_bytes / seconds,
        "object_bytes_per_s": object_bytes / seconds,
    }
```

Calibrate sender capacity against OTLP-to-noop first, on separate cores, and record generation CPU, RPC concurrency saturation and queue depth. Require sender/noop headroom at least 1.5 times the claimed exporter rate. Maintain open-loop offered-rate scheduling against monotonic target timestamps with bounded in-flight requests; record delayed sends rather than silently redefining offered rate as achieved rate. If Python is the bottleneck, split deterministic request-index ranges across spawned Python processes on reserved producer cores; maintain separate ledgers and merge them by disjoint record IDs. Do not increase exporter worker count as a producer workaround.

Start at 1,000 records/s. Double the offered rate until the stability rule fails; cap at 12 trials and mark an unbracketed result as a lower bound, not a maximum. Bisect sustainable/unsustainable rates until the bracket width is at most 10%. Each trial has 15s warm-up, 60s measurement and bounded drain; three winning-rate repetitions repeat the full trial. The duration is a measurement interval, not a readiness sleep: start only after first complete flush and required telemetry are visible. Stop generation on its monotonic phase deadline, then prove drain.

Use 1s measurement windows initially so producer concurrency cannot cap the low-cardinality case. Record this override prominently and add a default-15s confirmation at the winning rate. Let early byte/request rotations occur and record their reasons. A default-window run with input concurrency as the limiting factor is reported as a topology limit, not as the encoding ceiling. Check unique durably stored rate by completed-object interval counts plus final ledger reconciliation. In buffered mode producer ACK rate is not object drain rate; use strict topology for the ceiling and add a buffered run at 80% of that rate to expose WAL cost.

Matrix: local, MinIO and RustFS; one and four explicitly allocated physical cores; logs-only and 80/20 logs/metric-point mixed workloads; 1KiB bodies and a documented 8KiB variant; 10k hot series and cardinality churn; production ZSTD. Use the same seed and record counts for comparable trials. Limit the primary exhaustive search to mixed/1KiB/hot-series; run the other workload rows as fixed-rate confirmations at 80% of its capacity and bracket separately if they fail. These confirmations cannot be labelled their own maxima. Diagnostic uncompressed numbers come only from Task 3.

Producer fan-in is a required dimension of this task, because the deployment target is dozens to hundreds of senders, not a handful. Repeat the winning rate with 1, 8, 64 and 256 concurrent OTLP client connections at the same offered rate and the same record count, spread across the reserved producer cores. Record, per producer count: accepted records/s, the number of connections each worker terminates, the spread of that distribution across workers, engine CPU per worker, exporter RSS, series-cache occupancy and descriptor duplication across workers, plus producer p50/p95/p99 ACK latency. A connection distribution that leaves any worker idle while another saturates is reported as a fan-in limit with its SO_REUSEPORT hashing evidence, not as an encoding ceiling.

Fifth review (2026-09-23): (a) high-cardinality metrics: measure the ratio of new series to points for a workload where one point attribute is unique per point (for example request.id) and for the healthy workload, report descriptor bytes versus values bytes, and add an exporter signal for excessive growth (a series/points ratio gauge per window and a rate-limited WARN event above a configurable threshold); document the strategy in the README: filter or aggregate such attributes upstream (OTel Views), because series identity is the full attribute set by OTel semantics and cannot drop a varying attribute without merging distinct streams. (b) Worst-case runtime stall: measure the longest single uninterrupted stretch of the worker thread (merge key building, chunk encoding) on the largest block, and the reaction time to cancel and shutdown during a flush, not only throughput.

Memory at scale (user note 2026-09-23): Task 6 validated the memory model at small blocks (tens of MB) against a 2 x 500 MiB budget and the user accepted it without a separate large-block family. Task 5 and Task 7 therefore record, at no extra run cost, peak RSS, memory.accounted and the Task 6 residual split at their highest block fill, and report the ratio of accounted to budget reached; a residual outside tolerance at high fill is a finding for Task 12, not a reason to rerun Task 6.

From Task 6 (2026-09-23): measure the upload burst with `upload.concurrency` 1 versus 2 at the winning rate (peak RSS and completed-object latency), which Task 6 did not cover.

Umbrella finding 10: run one launcher case with `max_concurrent_requests` raised well above the shipped 128 (for example 4096) and report it beside the shipped-config run, so a receiver admission ceiling is separated from the exporter ceiling; report the admission-closed gauge from Task 3c item I in both.

The campaign's headline acceptance number is 100,000 to 1,000,000 records/s sustained, where a record is one metric point or one compact log line. Report the measured sustainable rate, the number of physical cores required to reach 1,000,000 records/s, and, when that rate is not reachable on this host, the measured ceiling with the limiting stage named from the Task 3 and Task 4 attribution. Neither a shortfall nor an extrapolation is a failure of this task; an unreported one is.

- [ ] **Step 4: Record completed bytes and per-core costs without double counting**

Maintain separate denominators for offered wire bytes, accepted supported records, unique stored values, physical stored rows including duplicates, and completed Parquet object bytes. Local byte totals use completed files; S3 uses HEAD content lengths, cross-checked against downloads. Exclude incomplete multipart parts from successful write speed and report their bytes separately where the store exposes them. Report both steady-state interval write speed and `total completed bytes / time from first send to final completion`, so tail drain cannot disappear from the throughput claim.

Record CPU seconds for engine, producer and store; total engine CPU/core occupancy; output/input compression ratio; average file size; objects/s; number of descriptor duplicates across workers; and producer p50/p95/p99 ACK latency. Join Task 4 stage shares by identical workload/config/core allocation and binary provenance, retaining the full environment fingerprint and profile mode, never by a convenient nearby run. The shared-writer discussion consumes measured per-worker descriptor overhead, scaling efficiency and stage saturation; this task implements no shared writer.

- [ ] **Step 5: Verify, record numbers and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure capacity --output-dir /tmp/series-capacity
```

**Failure:** common correctness/validity checks; claimed maximum without an unsustainable bracket; producer bottleneck; increasing backlog at the accepted rate; permanent rejection; missing byte/compression/core metadata; unstable repetitions; nonpositive throughput. Apply the Controller baseline policy in Global Constraints to performance and memory; preserve failed measurements and route defects to the contingency task.

**Recorded numbers:** three per-store JSONs contain every trial, sustainable/unsustainable bounds, records/s/core, total records/s, wire and Parquet bytes/s, CPU shares, scaling efficiency, compression, ACK percentiles, drain duration and oracle counts. A failed trial used to bracket capacity is marked `unsustainable` inside a valid search; semantic loss is always a failed run, never a useful bracket.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/capacity-local.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/capacity-minio.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/capacity-local.json docs/superpowers/reports/series-parquet-measurement/capacity-minio.json docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json
git commit -m "chore: establish series parquet throughput and write rates" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```


**Amendment (umbrella review 2026-09-23):** measure the synchronous stretch on the admission path (run sort in `append` -> `seal` up to `run_target_bytes`, and `rotate()`'s `recount()`) with the flush-stall probe method, at the largest run size of each shape, and record it beside the flush-side figures of Task 3i. A stretch above the flush side's worst figure is a Task 12 finding.

### Task 6: Validate retained memory, transients and the RSS residual

**Amendment (umbrella review, 2026-09-22):** two accounting gaps found by static review are the first hypotheses for the unexplained residual (38.8 MB against a 33.5 MB tolerance in the Task 3 sink family, 90-95 percent of tolerance in Task 2): merge keys for the whole block are resident but never charged (`sort.rs:225-237`, about 20-30 percent of block bytes at peak), and values builders keep 1024-row capacity per request (`extract/mod.rs:355-380`, about 110 KB accounted per small request). Task 6 measures both terms explicitly before decomposing the rest, and a confirmed gap is a bounded Task 12 fix (charge the term), not a tolerance change. Task 6 also states the block-pair uncertainty of the in-process residual (umbrella finding 8). Related perf hypothesis for Tasks 4/5: series rows form one sorted run per request, so churn yields up to 4096 runs for the merge (`buffer.rs:165-199`).

**Expected wall-clock cost:** 75-135 minutes for at least three independent paired release runs per topology/configuration plus separate profiles; under one second for accounting tests.

**Files:**
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs`
- Modify: `rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/memory-strict.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/memory-buffered.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `sample_engine`, staged heap profiles, `RunSpec`, `read_oracle` and the existing engine `dhat-heap` profiling feature; no new runtime metric or introspection endpoint.
- Produces `memory_experiment(spec: RunSpec, output_dir: Path) -> dict` and `residual(rss: int, accounted: int, runtime: int, buffer: int, workspace: int, allocator: int) -> int`.
- Memory output includes timestamped `rss_bytes`, `accounted_bytes`, `budget_bytes`, all named component estimates with measurement/bound provenance, and signed unexplained bytes. Disjoint categories and overlap flags prevent subtracting the same allocation twice.

- [ ] **Step 1: Add a failing signed-residual test**

```python
# Scenario: a double-counted workspace estimate exceeds observed process RSS.
# Guarantees: reconciliation retains a negative residual instead of clamping it away.
def test_residual_preserves_negative_discrepancy(self):
    self.assertEqual(residual(100, 60, 15, 10, 20, 5), -10)
```

- [ ] **Step 2: Run red**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing memory module or residual function.

- [ ] **Step 3: Implement the source-derived ledger and capture paired observations**

```python
def residual(rss, accounted, runtime, buffer, workspace, allocator):
    return rss - accounted - runtime - buffer - workspace - allocator
```

Transcribe the current worker accounting into the report, without claiming it measures every allocation:

```text
retained reservation = 2*B + E + 128*C + 2*N*T
workspace reservation = 2*R + 2*M + 3*W + P*(U+1) + M + 4*I + 64MiB
current accounted = active + flushing_or_cleaning + pending_extracted_and_descriptors
                    + 128*cache_entries + notify_token_bytes + spare_token_capacity
```

Here `B` is maximum block bytes, `E` maximum extracted request bytes, `C` cache capacity, `N` requests/block, `T` observed retained token high-water, `R` run target, `M` merge target, `W` writer limit, `P` upload part bytes, `U` upload concurrency, and `I` maximum wire request bytes. Flushing and cleanup are one slot, not two blocks. ACTIVE/FLUSHING already charge their retained tokens; do not add those again. Pending descriptors, queued/in-flight notification tokens and spare vector capacity must not vanish from the accounted comparison.

Sample normal release engine RSS/smaps and gauges at 100ms and telemetry at 100ms during targeted short memory runs; use 1s for soak. Sampling misses shorter allocation peaks, so use synchronous heap measurements and conservative overlap bounds below as well. Start with receiver/noop idle baseline, strict idle baseline, buffered idle baseline, and equal-workload strict/buffered runs on the same cores. Measure data pages, mappings/stacks, allocator retained pages, engine receiver queues, channels and gRPC buffers. A paired RSS difference is an estimate with an interval, not an exact allocation label. `memory_experiment` owns at least three independent pairs per topology and exact configuration: each pair launches fresh noop/control and measured release processes with matching cores/workload, warmed separately. Profiled runs supplement these pairs and never substitute for release repetitions. For every idle/load/seal/merge/encode/upload/recovery phase require at least 30 fresh telemetry epochs and 30 paired RSS observations; repeat a short transient until sample count is met and capture allocation peaks synchronously. A missed transient or phase is invalid, not zero.

Report each pair, per-phase median and min/max range (or confidence interval), and aggregate median/range across independent pairs. Reject a baseline if any primary positive memory metric has `(max - min) / median > 0.15`, if a paired signed difference crosses zero outside its recorded measurement uncertainty, or if samples/phase coverage/environment matching are incomplete. Investigate and rerun unstable candidates; retain all candidates and never commit an unstable one as the baseline. The Controller baseline policy applies only after this validity test.

- [ ] **Step 4: Exercise and account for each transient explicitly**

| Transient | Experiment and observation | Attribution and comparison |
| --- | --- | --- |
| Conversion | Requests near 16MiB with wide strings, nested attributes and histogram arrays; measure wire overlap, converted pinned Arrow bytes and DHAT peak under the conversion stage | Compare measured workspace to `4*I`; report actual peak/wire expansion separately. Record the reservation discrepancy; apply the Controller baseline policy to the measured peak. |
| Admission/final values sealing | Build several values runs plus unfinished building batches; retain pre-seal snapshots, seal, record old/new uniquely pinned buffers and allocation peak | Compare measured extra retained/copy space with revision-7 `(V+1)*R`, including run overshoot and series timestamp buffers. Record `V`, actual largest run and the resulting conservative bound; explain any discrepancy without turning this engineering estimate into a first-run ceiling. A stamp-only series test is insufficient. |
| Resident merge keys | Narrow default keys, then wide custom body/attribute keys across the whole block; allocate/consume actual MergeIter in the isolated profile | Attribute Arrow RowConverter/key_rows allocations and heap OwnedRows; compare narrow/wide results to a second-payload-scale bound derived from actual row lengths plus row offsets/heap capacity. Keys remain live until iterator drop, not just one output chunk. |
| Merge output | Skew widths so a chunk groups wide rows after many narrow ones; record pinned bytes of every yielded chunk | Report maximum/target ratio, `rows_per_chunk * max_row_bytes` conservative payload bound and Arrow buffer overhead. Unexplained allocation/RSS residual remains a hard failure; do not assert `actual <= M` when source uses average width. |
| Encoding | High-cardinality strings/dictionaries, null-heavy histograms and near-row-group limits; record ArrowWriter memory before/after write/flush/close and peak stage heap | Separate input chunk, output Vec capacity and Parquet workspace; compare workspace to `3*W`. Check final flush/close peak, not only steady writes. |
| Upload | Pre-encoded payload above three part sizes through real S3 at concurrency 1 and 2, plus actual Sink row-group flush; record allocation profile and in-flight part count | Compare live part buffers to `P*(U+1)` and separately measure current encoded chunk and HTTP/TLS client buffers. A whole supplied row group can launch a burst above nominal concurrency; measure and bound that burst by actual encoded row-group bytes, then report how the reservation compares with observations; never assume it is a measured hard concurrency cap. |
| Buffer | Same rate/record set with pass-through WAL, then outstanding backlog and replay; sample disk allocation plus process profile | Attribute Quiver/WAL/bundle stacks, file mappings and caches; report buffer heap/mapped RSS interval and physical/logical disk bytes separately from exporter. No expiry/drop-oldest loss or capacity overrun. |

For final values sealing, `V` is the number of buffered values runs in the spec bound; record both run count and dataset count to prevent interpreting it as signal count. Deduplicate shared Arrow allocations across snapshots with `CountedAllocations`. Distinguish the maximum of individually isolated transients from their possible overlap in the real sink. Include concurrently admitted ACTIVE work while FLUSHING encodes/uploads; two isolated peak measurements cannot prove that their sum never overlaps.

Use the existing engine `dhat-heap` feature in a separate profiling binary with default allocator features disabled. Reproduce the release pipeline's functional features explicitly (`series_parquet,aws,durable-buffer`, one crypto provider); run profiling with its working directory set to the run artifact directory so `dhat-heap.json` is isolated. Record DHAT's allocator change and slowdown. For normal-release allocator retention use `/proc/PID/smaps_rollup` plus the selected allocator's available statistics or a Valgrind Massif confirmation (`--pages-as-heap=yes` for page residency attribution, a separate ordinary heap run for stacks). Tool output must substantiate any large allocator/runtime term; do not assign the unexplained residual to "allocator" by definition.

If observed wide-key/sealing behavior exceeds an engineering reservation, record the counterexample and explain the measured allocations. Apply the Controller baseline policy and the hard residual check, then route any defect to the contingency task for bounded remediation or an evidenced architectural plan 4 decision. Do not relabel an engineering reservation as a measured ceiling.

- [ ] **Step 5: Verify, record numbers and commit**

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure memory --output-dir /tmp/series-memory
```

**Recorded numbers:** RSS/accounted/budget curves, conversion/seal/keys/chunk/encoder/upload peaks and ratios, concurrent peak envelope, cache/token terms, exporter attribution, buffer heap/mapped RSS interval and disk usage, runtime/allocator categories, residual magnitude/uncertainty and profile overhead. **Failure:** any common hard gate, missing named transient/sample count, unstable paired baseline, unexplained residual above the declared tolerance, per-worker retained-block overrun, buffer loss, or regression under the Controller baseline policy. An engineering `budget + idle RSS + 128MiB` comparison alone cannot pass this task.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/memory-strict.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/memory-buffered.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/memory-strict.json docs/superpowers/reports/series-parquet-measurement/memory-buffered.json
git commit -m "chore: validate series parquet memory accounting against RSS" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 7: Thirty-minute soak and bounded PR-tier coverage

**Expected wall-clock cost:** 70-85 minutes for two 30-minute input phases plus verification; 2-3 minutes for the fast PR-tier pair, inside the five-minute added-suite budget.

**Files:**
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Record: `docs/superpowers/reports/series-parquet-measurement/soak-strict.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/soak-buffered.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `capacity_search` results, `run_named`, `Ledger`, strict sampler, `drain`, oracle and Task 6 measured memory baselines and hard residual checks.
- Produces cases `soak-strict`, `soak-buffered`, `pr-soak-strict`, `pr-soak-buffered`, and `soak_checks(result: dict) -> None`.
- Soak output uses the common schema with full 1s samples and one-minute aggregate rate/RSS series. `metrics.input_phase_s` measures actual producer-active monotonic duration, excluding startup/warm-up/drain.

- [ ] **Step 1: Write the opt-in acceptance tests before implementing soak checks**

```python
class LongSoakTests(MeasurementTestCase):
    # Scenario: mixed supported telemetry flows for thirty minutes in strict mode.
    # Guarantees: all ACKed IDs survive drain and RSS observations satisfy validity and the Controller baseline policy.
    def test_strict_thirty_minutes(self):
        require_long()
        result = run_named("soak-strict", self.output_dir)
        self.assertGreaterEqual(result["metrics"]["input_phase_s"], 1800)
        soak_checks(result)

    # Scenario: mixed telemetry flows for thirty minutes through a persistent buffer.
    # Guarantees: WAL ACKs reconcile with stored IDs and both memory domains drain.
    def test_buffered_thirty_minutes(self):
        require_long()
        result = run_named("soak-buffered", self.output_dir)
        self.assertGreaterEqual(result["metrics"]["input_phase_s"], 1800)
        soak_checks(result)
```

Use Task 1's `MeasurementTestCase` to retain `self.output_dir`, rather than a TemporaryDirectory deleted on failure. Add a fast arithmetic regression with a synthetic rising RSS series; paired with a matching synthetic baseline it must fail the common regression check without launching an engine; without a baseline it becomes a candidate only after validity checks. Synthetic samples test analysis only, never stand in for a failure injection or a measured soak.

- [ ] **Step 2: Run red on analysis, then implement checks**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_soak -v
```

Expected: short analysis test fails due to absent `soak_checks`; long tests skip without the opt-in variable.

```python
def soak_checks(result):
    metrics = result["metrics"]
    failures = []
    for name in ("missing_acked_ids", "missing_intended_ids", "descriptor_violations",
                 "reader_disagreements", "permanent_rejections", "buffer_loss_items"):
        if metrics[name] != 0:
            failures.append(f"{name}={metrics[name]}")
    if metrics["sample_coverage_ratio"] < 0.99:
        failures.append("insufficient RSS/rate samples")
    evaluate_baseline(result)  # Shared Controller policy; never an absolute slope gate.
    if failures:
        raise AssertionError("; ".join(failures))
```

The same function requires explicit zero buffer-loss metrics in strict mode tagged `not_applicable: no_buffer`; absence of a required buffered metric is an instrumentation error, not a zero default.

- [ ] **Step 3: Run sustained load with a bounded producer and explicit drain phase**

Choose 70% of the lower repeatable local/S3 sustainable mixed-workload rate from Task 5 for one worker, 15s windows, 10k hot series with deterministic 1% churn, 1KiB bodies and 20% metric points. Set `Workload.requests = ceil(offered_records_per_s * 1800 / records_per_request)` plus the separately labelled warm-up cohort before starting; do not accidentally retain the 100-request smoke default. Use MinIO for strict and RustFS for buffered; failure tasks cover both stores in both topologies. Record the actual offered rate, series count and churn count. Input lasts at least 1,800 monotonic seconds after readiness and warm-up; no reduction to 30 minutes including drain is accepted. If generation exhausts its planned cohort early, the duration gate fails instead of counting idle time as load.

Every 1s sample records attempted, accepted and unique committed record deltas; wire and object bytes; in-flight requests; ACTIVE/FLUSHING/pending/notify gauges; oldest unacked; RSS and FD count; per-worker cache occupancy; store/proxy/producer RSS; buffered disk/queued/in-flight/retry gauges. Unique committed IDs can be enumerated asynchronously from completed files into the disk-backed oracle; keep reader work on separate cores and record lag. Do not read partial uploads or make the writer wait for a full-table query every second. List new completed objects incrementally and verify them after input stops; report provisional physical-row rate separately if unique-ID lag prevents an instantaneous unique rate.

After generation stops, continue authorized strict retries and buffer replay. Measure backlog at stop, time-to-drain and unique drain records/s, plus full-run average and final five-minute input/storage rates. Verify every acknowledged and every eventually accepted intended ID, exact payload semantics, descriptors and multiplicity histogram after all files are complete. Healthy soak has no injected failures and should be duplicate-free; any actual timeout/retry changes the run to a retry-bearing result with counted duplicates and an explained cause, not an unqualified healthy pass.

Retain Alloy as an additional short producer compatibility run using the shipped batching/retry config and existing `AlloyProducer`. Collect producer `/metrics` and RSS; reject enqueue loss. Do not use its historical queue calculations as a substitute for exact synthetic-producer ID accounting or as this exporter's peak throughput number.

- [ ] **Step 4: Keep the PR-tier test short and real**

Add two 60-75s cases with 1s windows, forced byte/request rotations, and one `DockerStore.stop()` outage past an overridden 3s flush deadline. Gate stop on a successful baseline object and nonempty ACTIVE; keep producing a finite ledger-backed workload. Gate recovery on a storage failure/NACK and elapsed deadline, not a fixed sleep. Use the existing `DockerStore.stop()` and `DockerStore.recover()` helpers here, so this task is independently runnable before proxy tooling exists. Require retryable NACK/local deadline classification in strict mode and buffer retries in buffered mode; recover, drain, assert the common oracle, retained-state caps and hard residual check; record memory/oldest age under the Controller baseline policy. Add them to the fast CI lane after `test_measurement`.

- [ ] **Step 5: Verify, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_soak -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure soak --output-dir /tmp/series-soak
```

**Recorded numbers:** at least 1,800 samples/input seconds per topology, input/ACK/storage/drain records and byte rates, duration, RSS curve/slope/median change/peak, FD peak, descriptor count/violations, duplicate histogram, missing IDs, buffer memory/disk and retry counts. **Failure:** any common gate, insufficient duration or samples, non-draining backlog, a regression under the Controller baseline policy or unexplained residual, missing ACKed record, descriptor/reader disagreement, unrecorded duplicate, producer enqueue loss or buffer retention loss.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/soak-strict.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/soak-buffered.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/soak-strict.json docs/superpowers/reports/series-parquet-measurement/soak-buffered.json
git commit -m "chore: record thirty-minute series parquet soak acceptance" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

**Amendment (user decision 2026-09-23): heap dumps across the soak.** Using the raw-dump profiling mode from the first Task 12 item, each 30-minute soak also runs once with allocation sampling and takes a raw jemalloc dump after warm-up, at the midpoint and at the end of the input phase. `jeprof --base` between the first and last dump names every stack that grew. Growth that the ledger does not explain is a Task 12 finding. The profiled soak is diagnostic and never sets the soak baseline.


**Amendment (controller ruling after Task 5, 2026-09-24): soak rates and method.** Task 5 showed the one-worker strict rate at the shipped 15 s window is slot-capped at about 8.5k records/s, where a block fills to a small fraction of `max_block_bytes`; a soak there would not exercise memory at real fill. So:
- `soak-strict`: one worker, MinIO, the shipped 15 s window, receiver slots raised to 4096, offered at 70 percent of Task 5's MinIO one-worker raised ceiling (187k), about 131k records/s, so blocks rotate on `max_block_bytes` and retained memory reaches the configured budget. Record the receiver's in-flight bytes beside the exporter's accounting.
- `soak-buffered`: one worker, MinIO, shipped slots (128), the durable buffer with its WAL on the host's disk, offered at 70 percent of the one-worker buffered sustainable rate; measure that rate first with a short bracket if Task 5 has none for one worker, and record it.
- Both use Task 5's producer placement (senders on CPUs 8-15,24-31), the self-verifying template generator and the aggregate oracle, and the allocator band for the RSS residual (jemalloc stats prints every 64 MiB).
- The heap dumps of the earlier amendment need Task 12's raw-dump mode, which does not exist yet; this soak records the allocator band and accounted-versus-allocated every second instead, and Task 12 runs one profiled soak with dumps after it builds the mode.

### Task 8: Disposable fault-tool provisioning and capability probes

**Expected wall-clock cost:** 1-3 minutes for preflight and proxy smoke after provisioning; image builds/pulls separately budgeted at 15-60 minutes.

**Files:**
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/fault-tools.Dockerfile`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/fault-nginx.conf`
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/fault-preflight.json`

**Interfaces:**
- Consumes Task 2's launcher contract, Engine/DockerStore and host lease, plus Task 1's run schema.
- Produces `FaultRig(store: DockerStore, root: Path)` context manager with `storage`, `launcher`, `evidence() -> dict`, `activate(name: str, parameters: dict) -> None`, `recover() -> None`, and `completed_values() -> list[dict]`. Register `slow`, `http503`, `store_outage` activation/recovery here; Tasks 9-11 own acceptance state machines and additional names.
- `ack_drop_bytecode(store_ip: str, store_port: int) -> str` compiles the IPv4 ACK-only expression below with `tcpdump -ddd -y RAW`, converts numeric lines to iptables bytecode and is consumed by Task 11.
- `preflight_fault_tools(required: bool) -> dict` implements `measure fault-preflight`, returning per-probe commands, exit status, evidence and restored-state checks. Required mode raises on any failed probe; optional mode skips cleanly before workload generation, after cleanup.

- [ ] **Step 1: Write failing prerequisite and privilege tests**

```python
# Scenario: a required lane cannot install the disposable namespace's DNS rule.
# Guarantees: missing NET_ADMIN is fatal instead of silently dropping required coverage.
def test_required_probe_failure_is_fatal(self):
    with self.assertRaisesRegex(AssertionError, "UDP DNS"):
        require_probe({"name": "UDP DNS", "passed": False}, required=True)
```

Define `require_probe(probe: dict, *, required: bool) -> None`: return only for `passed is True`; otherwise raise AssertionError in required mode or unittest.SkipTest in optional mode. Add tests for TCP DNS and xt_bpf failure, optional cleanup/skip, and post-preflight activation errors remaining failures. Test launcher argv grants NET_ADMIN only to the namespace owner, never to engine/store/Toxiproxy.

- [ ] **Step 2: Run red and provision outside the measurement lease**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
```

Expected: missing probe/FaultRig implementation. Provision images with the earlier commands, record resolved image digests/package versions, then finish all builds before acquiring the host lease. Never pull/build from test import or during a measurement.

- [ ] **Step 3: Build the isolated tool environment and preserving S3 proxy**

```dockerfile
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx dnsmasq dnsutils iproute2 iptables tcpdump tshark ca-certificates \
    libssl3t64 libstdc++6 procps curl && rm -rf /var/lib/apt/lists/*
CMD ["/usr/sbin/nginx", "-g", "daemon off;"]
```

Resolve the Ubuntu base digest in provisioning and record package versions and final image digest. The checked-in Dockerfile supplies a reproducible recipe plus result provenance; release evidence identifies the actual immutable image used. Validate the mounted engine binary's shared libraries with `ldd` before launch. Missing tool image/capability is a preflight skip; an incompatible engine binary is a setup failure with diagnostic output.

Use a unique Docker bridge network per run, attach the existing DockerStore container to it with alias `store`, and start a disposable fault-tools namespace owner with `--cap-add=NET_ADMIN`. Only this owner gets that added capability; engine/store/Toxiproxy do not. Both Toxiproxy sidecar and containerized engine join it with `--network container:FAULT_CONTAINER_ID`, so the proxy's loopback addresses below are real. The engine mounts binary/repository read-only and run/buffer directories read-write. Publish gRPC/admin ports from the namespace container on host loopback and bind services to `0.0.0.0` inside the namespace. `docker inspect` supplies the actual engine PID. Extend DockerStore with an optional network attachment method and retain all existing storage/reader/download methods. Cleanup removes only recorded container/network IDs in `finally`; retain raw artifacts and the buffer path until oracle verification finishes.

NGINX listens on port 19000. The ordinary path forwards through the general Toxiproxy port 19001 to the real store. The values path can use a separate proxy port 19002. Materialize the following body in `fault-nginx.conf`; supply `$backend` with a map defined from the per-run proxy addresses, preserve URI and signed Host, and disable response/request buffering and retries:

```nginx
events {}
http {
    log_format faults '$msec $request_method $uri $status $upstream_status '
                      '$request_time $upstream_response_time $body_bytes_sent';
    access_log /artifacts/nginx-access.log faults;
    map $uri $backend {
        default http://127.0.0.1:19001;
        ~dataset=values/ http://127.0.0.1:19002;
    }
    server {
        listen 19000;
        client_max_body_size 0;
        keepalive_timeout 0;
        location / {
            if (-f /control/fail503) { return 503; }
            proxy_set_header Host $http_host;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_request_buffering off;
            proxy_buffering off;
            proxy_next_upstream off;
            proxy_read_timeout 600s;
            proxy_send_timeout 600s;
            proxy_pass $backend;
        }
    }
}
```

The fixed ports are namespace-local; publish only chosen host ports, so parallel test namespaces cannot collide. Both proxy backends normally forward to the same store. Preserve path-style bucket addressing, S3 signing Host, content length/chunked behavior, query string and multipart request method; preflight signed PUT/GET/DELETE and a multipart completion through this route on each backend. Failure here is a harness failure, not exporter recovery evidence.

Configure Toxiproxy through its real HTTP API. Ordinary slowdown/reset/disconnect applies to **both** `general` and `values` proxies, otherwise cached-series workloads could bypass the fault. Fault activation sends these JSON bodies to `/proxies/general/toxics` and `/proxies/values/toxics`:

```json
{"name":"slow_upload","type":"bandwidth","stream":"upstream","toxicity":1.0,"attributes":{"rate":256}}
```

```json
{"name":"slow_response","type":"latency","stream":"downstream","toxicity":1.0,"attributes":{"latency":1500,"jitter":0}}
```

`rate` is KB/s as the [official toxic definitions](https://github.com/Shopify/toxiproxy#toxics) specify; save the tool version and units. Remove named toxics through DELETE for recovery. NGINX 503 activation creates `/control/fail503` in the run's mounted directory and removes that exact file to recover. These are real proxy behaviors; no Python HTTP server fabricates storage behavior inside the exporter process.

- [ ] **Step 4: Probe privileges and rules before workload traffic**

Create only the disposable namespace owner with `--cap-add=NET_ADMIN`; engine, store and Toxiproxy receive no added NET_ADMIN. Run DNS/firewall/capture commands with `docker exec` in that owner. No `--privileged`, host networking, host firewall changes or module loading is required. Its lifetime and recorded container ID scope every rule and cleanup operation.

Before any measured traffic, perform a healthy signed PUT/GET/DELETE and multipart round-trip, then probe UDP DNS and TCP DNS independently: add the exact namespace-local port-53 DROP rule, issue respectively `dig` and `dig +tcp` to local dnsmasq, require positive rule counters and bounded timeout, delete the exact rule, and require successful resolution. Probe xt_bpf by compiling the ACK-only filter below, installing its INPUT match rule, exercising a disposable transfer, reading counters, and deleting the rule; unavailable module/match support or rule insertion is a failed probe. Confirm no probe rules/toxics remain before starting the case. Diagnostic probe packets are preflight traffic, excluded from workload metrics.

The bytecode helper substitutes the inspected store IP/port as argv values and compiles:

```text
src host STORE_IP and src port STORE_PORT and tcp[13] = 16 and
(ip[2:2] - ((ip[0] & 15) << 2) - ((tcp[12] & 240) >> 2)) = 0
```

Use `iptables -I INPUT 1 -p tcp -m bpf --bytecode BYTECODE -j DROP` in the namespace owner, with the compiled program restricting it to this store. Delete that exact rule after the probe. Task 11 reuses this helper and adds the fault's packet/retransmission evidence.

Save all probe output and cleanup status in `fault-preflight.json`. `SERIES_REQUIRE_FAULT_TOOLS=1` makes any failed UDP/TCP/xt_bpf/capability probe fatal. Optional discovery cleanly skips before case traffic after cleanup; no probe failure can be relabelled a passed acceptance case. Test failure after successful preflight is always a failure. Apply the Controller baseline policy to measured quantities; route harness defects to Task 12.

- [ ] **Step 5: Verify and commit provisioning evidence**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure fault-preflight --output-dir /tmp/series-fault-preflight
```

**Recorded numbers:** probe exit codes/counters/latencies, exact capabilities, digests, signed S3 checks, cleanup checks and run environment. **Failure:** missing required capability/tool/rule behavior, stale probe rules, invalid S3 route or any environment hard gate.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/fault-preflight.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/fault-tools.Dockerfile rust/otap-dataflow/crates/validation/tests/series_parquet/fault-nginx.conf rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/fault-preflight.json
git commit -m "chore: provision isolated series fault tools" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 9: S3 fault state machines and recovery evidence

**Expected wall-clock cost:** 15-25 minutes for both stores/topologies; 20-40 seconds for the optional fast proxy activation smoke.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-s3.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes Task 2 launcher/lease, Task 8 FaultRig and successful probes, Engine/DockerStore, run schema, ledger, sampler and oracle.
- `failure_case(family: str, fault: str, topology: str, store: str, output_dir: Path) -> dict` runs one matrix cell; family `s3` supports `slow`, `http503`, `store_outage`. Every cell's result includes immutable before/during/after evidence, records, duplicates, timings and peak memory.
- `fault_check(result: dict) -> None` requires `fault_observed`, `recovered`, `drained`, zero missing/coverage/semantic violations, measured duplicates and bounded resources. Families in Tasks 10-11 reuse this exact check.

- [ ] **Step 1: Add a failing both-topology recovery test**

```python
class S3FailureTests(MeasurementTestCase):
    # Scenario: a real S3 HTTP 503 reaches each topology before service recovers.
    # Guarantees: recovery preserves every ACKed ID and reports replay multiplicities.
    def test_http503_recovery(self):
        require_long()
        for topology in ("strict", "buffered"):
            for store in ("minio", "rustfs"):
                with self.subTest(topology=topology, store=store):
                    result = failure_case("s3", "http503", topology, store,
                                          self.output_dir)
                    self.assertGreater(result["metrics"]["http_503_responses"], 0)
                    fault_check(result)
```

Also add short non-Docker checks that missing prerequisites yield a clean SkipTest only in optional mode, required mode fails, and a post-preflight fault activation error is never converted into a skip. Give each its Scenario/Guarantees comments.

- [ ] **Step 2: Run red with already provisioned tools**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
```

Expected: missing S3 family registration after Task 8 preflight succeeds; prerequisite tests remain green. Image provisioning belongs to Task 8.

- [ ] **Step 3: Implement the three S3 cases as observable state machines**

```python
def fault_check(result):
    checks = result["checks"]
    for name in ("fault_observed", "recovered", "drained", "at_least_once",
                 "descriptor_coverage", "reader_agreement", "bounded_resources"):
        if checks.get(name) is not True:
            raise AssertionError(f"failed required fault check: {name}")
    if result["metrics"]["missing_acked_ids"] != 0:
        raise AssertionError("acknowledged supported record missing")
    if not isinstance(result["metrics"]["multiplicity_histogram"], dict):
        raise AssertionError("multiplicity histogram absent")
```

For each fault, start with a known durable baseline, keep a finite mixed-signal producer active, then arm the external fault. Gate injection on baseline reader/HEAD evidence and nonzero active input. For slowdown require measured upload/response delay, nonempty FLUSHING plus ACTIVE or pending work, and backpressure/in-flight plateau before recovery. For 503 require an actual PUT/POST 503 log and exporter retry. For store outage stop the container and require failure beyond the configured flush deadline. Full cases use the default 60s deadline and observe at least one exporter storage NACK; a timer alone does not establish this. While faulted, sample resource limits and producer/buffer behavior. Recover only once the intended condition has been observed, then restart/retry/drain under a fixed total deadline.

Strict mode retries the original serialized request bytes and distinguishes server retryable NACK from producer-local timeout. Buffered mode stops resending any request after its durable producer ACK, retains disk across all actions, and waits for exporter NACK -> buffer retry evidence. Require ACTIVE and FLUSHING each within B, cache within C, pending slot at most one, oldest unacked age returning to baseline after recovery, RSS reconciled with Task 6's measured terms and compared under the Controller baseline policy and no buffer expiry/eviction. Report upload retries separately from replay duplicates. Recovery deadline is 300s after endpoint health, raised only by a recorded bound from backlog bytes/minimum measured drain rate before the case begins.

- [ ] **Step 4: Run all cells, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family s3 --output-dir /tmp/series-failure-s3
```

**Recorded numbers:** per cell actual fault duration/status count/bandwidth/latency, requests and supported IDs offered/ACKed/stored, retries/NACKs/timeouts, duplicate histogram, recovery/drain seconds, throughput before/during/after, RSS/accounted/buffer disk peaks and coverage violations. **Failure:** activation not proven; a supported record lost; descriptor/reader mismatch; no retry/backpressure when required; violated retained-state/capacity invariant or Controller baseline policy; recovery deadline; or unexplained duplicates outside the replayed ID set. No exactly-once assertion is made for ambiguous requests.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/failure-s3.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/failure-s3.json
git commit -m "chore: measure series recovery from real S3 faults" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

**Amendment (compaction contract, 2026-09-23):** every S3 fault run in this task also checks the partition lateness bound: no object may become visible in partition hour H later than L = window.interval + 2 * (flush_retry_deadline + upload.abort_timeout) after the end of H. Record, per run, the latest visibility time of any object in each hour relative to that hour's end (HEAD/LIST timestamps from the store, not the writer's clock), and report every violation as a finding for Task 12.


**Amendment (S3 compatibility note, 2026-09-23):** (a) one case per real store (MinIO, RustFS) writes a file above `upload.part_bytes` and asserts from the server trace that CreateMultipartUpload, UploadPart and CompleteMultipartUpload ran, so the multipart path is exercised on a real store and not only on the fault store; (b) after every S3 fault case and every hard-kill case of Task 10, list the bucket's incomplete multipart uploads and compare with the expected count (zero, or the uploads the scenario is known to orphan); an unexpected orphan is a Task 12 finding.

### Task 10: Graceful process restart and ungraceful hard kill

**Expected wall-clock cost:** 8-15 minutes for both stores/topologies and kill phases; fast controller tests under one second.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-process.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `failure_case`, `fault_check`, Engine restart options, ledger and retained destination/buffer paths.
- Adds family `process` cases `graceful_restart`, `kill_active`, `kill_upload`; `restart_engine(previous: Engine, *, retain_buffer: bool) -> Engine` records old/new PID, core IDs and boot IDs.
- Process liveness is checked with `poll`/`wait` and the admin endpoint under deadlines; no sleeping to guess that the engine has stopped.

- [ ] **Step 1: Write failing kill/restart tests**

```python
class ProcessFailureTests(MeasurementTestCase):
    # Scenario: SIGKILL interrupts a real upload with acknowledged history on disk.
    # Guarantees: restart and topology-appropriate replay preserve every supported ID.
    def test_kill_during_upload(self):
        require_long()
        for topology in ("strict", "buffered"):
            for store in ("minio", "rustfs"):
                with self.subTest(topology=topology, store=store):
                    result = failure_case("process", "kill_upload", topology,
                                          store, self.output_dir)
                    self.assertNotEqual(result["metrics"]["old_pid"],
                                        result["metrics"]["new_pid"])
                    self.assertTrue(result["checks"]["new_boot_id"])
                    fault_check(result)
```

- [ ] **Step 2: Run the named case red**

```bash
SERIES_MEASURE_LONG=1 python3 -m unittest crates.validation.tests.series_parquet.test_failures.ProcessFailureTests -v
```

Expected: unregistered process family, after successful preflight; do not accept a skipped test as red/green proof.

- [ ] **Step 3: Implement exact lifecycle boundaries**

```python
def kill_engine(engine):
    pid = engine.launcher.pid(engine.process) if engine.launcher else engine.process.pid
    os.kill(pid, signal.SIGKILL)
    wait_until(engine.process.poll, lambda code: code is not None,
               deadline_ns=time.monotonic_ns() + 10_000_000_000,
               description="killed engine process exits")
```

For a container Engine use `docker kill --signal KILL` against its recorded container ID and `docker inspect` exit state; the launcher owns the process handle and real PID mapping. Do not send SIGKILL to the docker client wrapper. This is the only launcher-specific branch.

`graceful_restart`: start pending work, request admin shutdown with the documented drain deadline, assert successful exit and prior ACK coverage, then launch against the same destination. Buffered restart reuses the same buffer path and core IDs. Continue with new IDs and verify both generations and distinct boot metadata.

`kill_active`: gate on nonzero ACTIVE bytes, zero FLUSHING for the selected new cohort, and no completed values containing that cohort. In strict mode these records may not be ACKed yet; retain and retry them after reconnect. In buffered mode gate on durable producer ACKs first, then kill. Task 13 provides the stricter no-resend proof. If a window rotated before the gate, discard the setup attempt without claiming a fault hit and retry within a bounded setup deadline; repeated inability to hit the boundary fails the test.

`kill_upload`: set real upstream bandwidth limit, use enough incompressible seeded payload to force multipart upload, and observe a store multipart upload plus positive transferred bytes and nonzero FLUSHING. Kill the engine before completion; remove the toxic and restart. Also include a small single-PUT interrupted request so success is not specific to multipart. Record incomplete upload IDs separately from completed object files; incomplete multipart leftovers are not acknowledged-record loss or successful output. Cleanup is test-owned after evidence collection.

On every restart verify identical source bytes on retry, new writer boot UUID, valid descriptor coverage within each boot/partition, no loss in already ACKed historical IDs, and no permanent rejection. Compute exact pre/post multiplicity by ID, listing changes only for cohorts eligible for replay. The stable ledger survives the killed engine and is not recreated from the output being checked.

- [ ] **Step 4: Run green, record and commit**

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family process --output-dir /tmp/series-failure-process
```

**Recorded numbers:** time to exit/restart/readiness/drain, old/new PIDs and boot IDs, known ACKed/pending cohort sizes, replayed IDs, exact duplicate histogram, missing/coverage counts, incomplete multipart count and peak memory/disk. **Failure:** lifecycle gate missed, wrong retained path/core allocation, unacknowledged strict requests abandoned, buffered ACKed IDs absent, false complete-file accounting, descriptor/reader disagreement or deadline overrun.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/failure-process.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py docs/superpowers/reports/series-parquet-measurement/failure-process.json
git commit -m "chore: measure series restart and hard-kill recovery" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 11: Network, DNS, TCP ACK and dropped completion response faults

**Expected wall-clock cost:** 20-40 minutes for the complete two-store/two-topology matrix; 20-40 seconds for optional namespace preflight smoke.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-network.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `FaultRig`, container launcher, external-tool inventory, `failure_case` and `fault_check`.
- Adds family `network` cases `disconnect`, `reset`, `dns_nxdomain`, `dns_timeout`, `tcp_ack_loss`, `dropped_completion_response`.
- Adds `FaultRig.activate` names matching those cases and consumes Task 8's `ack_drop_bytecode(store_ip: str, store_port: int) -> str`. No production transport fault flags are added.

- [ ] **Step 1: Add a failing TCP-ACK evidence test**

```python
class NetworkFailureTests(MeasurementTestCase):
    # Scenario: kernel filtering drops ACK-only packets on the real store connection.
    # Guarantees: packet loss is proven and recovery preserves IDs in both topologies.
    def test_tcp_ack_loss(self):
        require_long()
        for topology in ("strict", "buffered"):
            for store in ("minio", "rustfs"):
                with self.subTest(topology=topology, store=store):
                    result = failure_case("network", "tcp_ack_loss", topology,
                                          store, self.output_dir)
                    self.assertGreater(result["metrics"]["dropped_pure_ack_packets"], 0)
                    self.assertGreater(result["metrics"]["tcp_retransmissions"], 0)
                    fault_check(result)
```

Add a negative evidence test: an installed rule with zero matching packets must fail `fault_observed`, even if the writer subsequently drains. That tests the evidence checker only; it is not a simulated networking acceptance case.

- [ ] **Step 2: Run red on the unimplemented family**

```bash
SERIES_MEASURE_LONG=1 python3 -m unittest crates.validation.tests.series_parquet.test_failures.NetworkFailureTests -v
```

Expected: unregistered network cases on a fully provisioned host.

- [ ] **Step 3: Implement disconnect/reset and real DNS failures**

Disconnect both Toxiproxy routes with `POST /proxies/general` and `/proxies/values` body `{"enabled":false}`, observe broken connections, then enable them. For reset add `{"name":"reset","type":"reset_peer","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}` to both and capture RST evidence; remove them for recovery. Avoid collapsing reset, refusal and timeout into one error label.

DNS acts on the **engine's** S3 endpoint resolution, not solely on NGINX's backend lookup. Launch the engine in the private namespace with a per-run read-only mounted resolv.conf containing `nameserver 127.0.0.1`, `options attempts:1 timeout:1`, and a unique endpoint such as `lake-<run-id>.test:19000`. Run dnsmasq on loopback with `--no-resolv --no-hosts --log-queries --local=/test/ --addn-hosts=/control/hosts --local-ttl=0`; the hosts file initially maps this name to the namespace's NGINX address. Warm baseline traffic, remove the entry and send dnsmasq SIGHUP for authoritative NXDOMAIN. Clear the HTTP connection pool by externally resetting the existing proxy connection; require a fresh captured engine DNS query for the case to count. If the client's resolver caches beyond the bounded test deadline, restart the engine against the same unresolved name, preserve the selected topology's retry ownership, and label this as a reconnect-DNS case. Never pretend an unaffected pooled connection exercised DNS.

For DNS timeout keep the name valid, but insert namespace-local `OUTPUT` rules dropping UDP and TCP destination port 53. Confirm rule counters and actual unanswered queries with tcpdump and a diagnostic `dig` from the same namespace. Remove only those rules for recovery; a successful `dig` and then a new engine S3 request are both required. Namespace cleanup removes all its private state; host DNS is untouched.

- [ ] **Step 4: Drop actual TCP ACK-only packets with the kernel**

Use IPv4 for this cell so the exact filter is unambiguous. Compile this expression with `tcpdump -ddd -y RAW` inside the fault namespace; substitute the inspected store IP and actual backend port as argv values, not shell interpolation:

```text
src host STORE_IP and src port STORE_PORT and tcp[13] = 16 and
(ip[2:2] - ((ip[0] & 15) << 2) - ((tcp[12] & 240) >> 2)) = 0
```

The expression compares IP total length against IP plus TCP header lengths, including TCP options, so data-bearing ACKs do not match. Convert the numeric output to `count,instruction,instruction` form and use `iptables -I INPUT 1 -p tcp -m bpf --bytecode BYTECODE -j DROP` inside the client/proxy namespace. Restrict the BPF program to this run's inspected store endpoint. Install before a large throttled multipart transfer, capture both directions with tcpdump, and gate recovery on positive rule counters plus retransmissions in tshark (`tcp.analysis.retransmission`). Require matching captured packets to have `tcp.len == 0` and ACK set. The BPF capability probe is disposable and runs before any test traffic; missing kernel support is a documented preflight skip in optional mode.

TCP may still make progress because response data carries cumulative ACKs. This case's required observations are real dropped pure ACKs and retransmission/recovery, not an invented guaranteed application timeout. If the workload does not produce these observations, increase its bounded multipart cohort or lower bandwidth within the predeclared 60s activation deadline; otherwise fail activation. Record exactly what happened rather than claiming every ACK loss causes a retry.

- [ ] **Step 5: Exercise a dropped completion response separately**

Configure the values-only proxy's downstream toxic as:

```json
{"name":"drop_completion","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}
```

Send a single-signal cohort small enough that values use one PUT, not multipart initiation. Series objects bypass the values route and complete normally. Through the independent DockerStore client, repeatedly list/HEAD/GET the expected values object and run the reader oracle on its IDs. Require complete object bytes and valid descriptors while the proxy discards downstream response bytes, the strict producer has no ACK for the cohort, and the exporter still owns its FLUSHING completion. In buffered mode the producer may already have WAL ACKed, but the buffer's downstream resolution must not have advanced for this cohort.

With `timeout: 0`, bytes are dropped until toxic removal, whose cleanup closes the connection, as the [timeout implementation](https://github.com/Shopify/toxiproxy/blob/main/toxics/timeout.go) shows. Remove the toxic, observe that close and the client transport failure/retry; then recover/drain. Repeat for logs and metrics independently to avoid the first dropped values response blocking completion of a second signal in the same block. Record duplicates and unchanged multiplicities outside the uncertain cohort. This case proves an application acknowledgement can be lost over TCP; it is not labelled pure TCP ACK loss. Task 13 reuses the same proven completion boundary and kills before recovery to force durable-buffer replay.

- [ ] **Step 6: Verify, record and commit**

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family network --output-dir /tmp/series-failure-network
```

**Recorded numbers:** per-cell DNS result/query/timeout counts, RSTs, matched ACK drops and retransmissions, completed-but-unacknowledged object counts/bytes, producer/server retry classes, recovery/drain latency, memory/disk peaks, unique IDs and multiplicities. **Failure:** no independent activation evidence, DNS only affecting the proxy rather than engine, data-bearing packets mislabelled as pure ACKs, missing acknowledged records, invalid descriptors, uncounted duplicates, leaked namespace rules/resources, or recovery timeout. Both topologies and both stores are mandatory in the long result; absence of tooling is listed as incomplete acceptance.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/failure-network.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/failure-network.json
git commit -m "chore: measure series network DNS and acknowledgement faults" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

**Amendment (compaction contract, 2026-09-23):** the dropped-completion-response case is the one where a client that has given up cannot prevent a late object: the store may still complete a CompleteMultipartUpload the writer abandoned. Measure it explicitly: after the writer's deadline, does the object appear in hour H, and how late relative to L? A violation is expected and is recorded as the evidence for the plan-4 per-writer seal marker; it is not fixed here.

### Task 12: Contingency - classify defects, fix bounded causes and rerun

**Expected wall-clock cost:** 1-3 minutes when no defects were found; per bounded defect 5-30 minutes for focused verification plus the full cost of its failed measurement and any affected dependent runs. Build/full-workspace checks retain their separate 15-60 minute budget.

**Files:**
- Modify: only source files named by a reproduced bounded defect, recorded individually in the finding before editing (exporter `worker.rs`, series-lake `buffer.rs`/`sort.rs`/`sink.rs`, or the affected harness module are likely sites, not authorization for unrelated edits).
- Add/Modify: the affected crate's regression test or the concrete Python test module owning the failed behavior; record its exact path in the finding.
- Copy: `rust/otap-dataflow/.chloggen/TEMPLATE.yaml` to `rust/otap-dataflow/.chloggen/series-parquet-measurement-fix.yaml` for the first bounded user-facing fix; use a distinct defect-ID suffix for subsequent entries.
- Modify if dependency resolution changes: `rust/otap-dataflow/Cargo.lock`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/remediation.json`, finding JSONs and failed/rerun child results.

**Interfaces:**
- Consumes findings from Tasks 1-11, 13 and 14. This is an interruptible contingency: invoke it as soon as a defect blocks a task, then return to that task; its document position does not defer early fixes until the fault matrix finishes.
- A finding JSON has `defect_id`, `classification`, `source_evidence` (repository file/line/revision), `failed_run_files`, `reproducer_argv`, `source_paths`, `test_paths`, `test_argv`, `affected_crates`, `rerun_argv`, `changelog_path`, `tracking_issue_or_pr`, `reason`, and `resolution`. Paths/commands are exact values captured from the failed case, not guessed scaffolding.
- `measure remediate --finding PATH --output-dir PATH` validates the finding, dispatches the recorded test/rerun argv through existing runners and writes `remediation.json` with original and rerun child hashes. It does not automatically edit source or fabricate results. Without `--finding`, inventory all recorded failures and emit `no_defects` only when none remain unclassified; an empty contingency has no measured baseline.

- [ ] **Step 1: Reproduce and classify with source evidence**

Retain the failed measurement and its fingerprint. Re-run its smallest reproducer using the recorded argv, with real external faults when relevant, and inspect the repository file/lines responsible. Classify `bounded` when a localized fix preserves worker ownership, delivery/topology contracts and file format (for example a wrong accounting term, missing allocation release, status handling or harness sampler bug). Classify `architectural` only when evidence shows a necessary ownership/scheduling/format redesign, such as the process-wide shared writer in spec 10.4. A large number or a difficult bug alone is not an architectural classification. Invalid host/sample runs are rerun after correcting setup; a harness defect is still bounded remediation.

- [ ] **Step 2: Add a regression test that fails before the fix**

Read root and affected-directory AGENTS.md. Put the regression beside the affected behavior, with immediate specific Scenario/Guarantees comments. Use the observed minimal input/state transition, assert the violated observable invariant (record coverage, release/accounting, retry/ACK order or schema/epoch behavior), run the exact `test_argv` and retain the failure. A performance regression needs the repeatable measurement reproducer plus a deterministic mechanism test where possible; never add a flaky wall-time unit test or a test that only asserts the changed constant. Keep all Rust source and changelog text ASCII.

- [ ] **Step 3: Implement the bounded fix and changelog in this plan**

Edit only the finding's reviewed `source_paths`/`test_paths`, resolve the root cause, and run the focused test again. No controller approval is required for this bounded repair. For a user-facing Rust fix copy the template, rather than constructing an unrelated YAML shape:

```bash
cp rust/otap-dataflow/.chloggen/TEMPLATE.yaml rust/otap-dataflow/.chloggen/series-parquet-measurement-fix.yaml
```

Fill `change_type: bug_fix`, the actual `component` from `.chloggen/config.yaml` (`pipeline` for exporter behavior, `engine` for engine behavior), a concrete user-visible `note` of at most 200 ASCII characters, and the existing tracking issue or PR in `issues`. Optional `subtext` is at most 300 characters. Use the defect-ID path recorded in `changelog_path` for later entries and never overwrite a prior fix. A measurement-only/harness correction is a dev-only chore and records that AGENTS.md exemption explicitly; user-facing fixes include the copied entry and a `fix` commit. Run `make chlog-validate` from the repository root when an entry is required.

- [ ] **Step 4: Rerun the failed measurement and affected dependents**

After relevant cargo check/focused tests and benchmark checks, finish all builds before reacquiring the host lease. Rerun the exact failed measurement, including repetitions, hard oracle/validity/residual checks and the Controller baseline policy. Preserve its original fingerprint comparison when inputs/profile are unchanged; source/binary hashes change only as provenance. A needed config/workload change creates a separate baseline but does not erase the original failure or satisfy its rerun. Re-run dependent attribution/capacity/memory/soak/fault results if their inputs or behavior were affected, and link superseded evidence explicitly.

An architectural finding records the failed run, quantitative counterexample, source evidence, why a bounded fix is insufficient, and the specific plan 4 decision needed. Leave that acceptance item unresolved; defer only its architectural implementation. No shared writer or oldest-pending-age rotation trigger is implemented here. Required correctness/validity failures continue blocking a passing final amendment.

- [ ] **Step 5: Record resolution and commit exact files/evidence**

`remediation.json` records `fixed`, `architectural_deferred`, or `no_defects` per finding, every red/green/rerun outcome, and `changed_files` as a validated list of exact repository-relative source/test/changelog paths. Before committing, enumerate that list, reject directories/globs/path escape, and verify it equals the intended diff paths. An unresolved bounded defect cannot be closed as controller-deferred.

```bash
python3 -m crates.validation.tests.series_parquet.measure remediate --output-dir ../../docs/superpowers/reports/series-parquet-measurement
```

**Recorded numbers:** defect classifications, red/green test counts, rerun metrics and baseline decisions, fixed/architectural/unresolved counts. **Failure:** unreproduced classification, missing regression/changelog, failed rerun, unauthorized architectural scope, or any remaining bounded defect. Preserve architectural blockers in Task 14's report for the plan 4 decision.

The following commit block runs from repository root. Its Python staging command passes every reviewed filename as a separate argument; it never stages a directory or glob. Cargo.lock is explicitly included because a bounded fix may change dependency resolution.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/remediation.json)
python3 - <<'STAGE_FIX'
import json
import subprocess
from pathlib import Path
index = Path("docs/superpowers/reports/series-parquet-measurement/remediation.json")
paths = json.loads(index.read_text())["changed_files"]
for name in paths:
    path = Path(name)
    if path.is_absolute() or ".." in path.parts or not path.is_file():
        raise ValueError(f"invalid exact staging path: {name}")
if paths:
    subprocess.run(["git", "add", "--", *paths], check=True)
STAGE_FIX
git add rust/otap-dataflow/Cargo.lock docs/superpowers/reports/series-parquet-measurement/remediation.json
git commit -m "fix: resolve bounded series measurement findings" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

Use a `chore` commit subject when this pass contains only harness fixes, a no-defects inventory or architectural evidence; preserve the exact trailers. Repeat the contingency for later discoveries and commit every new evidence index/child using the same staging contract.

**Amendment (user decision 2026-09-23): coherent size limits, option A.** A request that passes ingress must always fit an EMPTY block, so `window.max_block_bytes` only rotates and bounds memory and never refuses. Today only the `2 * max_extracted_bytes` term is validated, while block admission also reserves a fixed per-series term F = 128 * C + 8 + Q per new series (1352 B logs, 2120 B metrics, +128 B per denormalized series column), so with the defaults a single request above ~215k new metric series or ~338k new log series is permanently refused naming `window.max_block_bytes`. Bounded fix, after Task 6 has measured the real per-series builder row cost:
- [ ] Replace the unmeasured 128 B-per-column reservation in `LakeConfig::series_row_fixed_bytes` with the measured cost plus a stated margin (never below measured), recording the measurement file and margin in the code doc and FORMAT/README.
- [ ] Add `ingress.max_series_per_request` (distinct series per request), checked during extraction as soon as the count exceeds it; the nack reason names the limit, the observed count and the remedy ("split the batch upstream or raise the limit"); `error.type` gets its own value. Default derived from the defaults so that `2 * max_extracted_bytes + max_series_per_request * F_max + token_bytes <= max_block_bytes` holds, F_max being the largest F for the configured signals and denormalized columns.
- [ ] Startup validation checks that full inequality for the configured values and refuses with the numbers; the README sizing section states it.
- [ ] Block admission keeps its worst-case check as a debug assertion plus a counted `internal` nack if it ever fires, since validation now makes it unreachable; a test proves an ingress-accepted request with max_series_per_request new series fits an empty block for logs, metrics and a denormalized schema.
- [ ] Task 5's high-cardinality shape is rerun afterwards: at the limit it is accepted, one above it is refused at ingress with the new reason.
Options B (separate `target_block_bytes` for rotation vs `max_block_bytes` as memory cap) and C (split an oversize request across blocks with a multi-block token) are recorded for plan 4 and not implemented here.

**Amendment (user decision 2026-09-23): heap attribution by jemalloc profile, first Task 12 item.** Task 6 found the in-process `/debug/pprof/heap` endpoint unusable: its first dump makes the symbolizer hold about 266 MB, which dominates every later profile. Task 3i then attributed the high-rate ledger excess indirectly, by adding a term and re-running whole families. Replace that with direct heap attribution:
- [ ] Add a harness allocator mode that samples allocations (`prof:true,prof_active:true`, `lg_prof_sample:17` unless measured otherwise) and dumps raw jemalloc heap files with no in-process symbolization: on demand through the `prof.dump` mallctl reached from the admin API or a test hook, and optionally on each new high-water mark with `prof_gdump:true`. The dump path lives under the run's output directory.
- [ ] Symbolize offline on the host with `jeprof` against the exact engine binary. Record the top retaining stacks as text, and the difference between two dumps with `jeprof --base`. Prove the dump itself adds no more than 1 MB to the heap by comparing jemalloc `allocated` just before and just after a dump.
- [ ] Take dumps at a flush peak and just before the flush in the logs high-rate shape, and on the 512k near-limit block. Attribute the ledger excess of Task 3i (33 to 47 MB per pair) and the about 160 MB large-table flush workspace, including the row-group tail that pins the previous row-group buffer, to named stacks. Only then decide the ledger fix: synchronous telemetry/allocator pairing, a missing term, or both.
- [ ] Take dumps at the start and the end of a Task 6 local-storage run to attribute the engine `memory.usage` drift of +137/+273 MB.
- [ ] The profiling mode is a diagnostic: its runs never write or update a baseline, and their timings are not compared with normal runs.


**Amendment (Task 5 finding, 2026-09-24): receiver in-flight memory.** In strict mode the receiver keeps every in-flight request in memory until its block is durable, bounded only by the number of receiver slots, not by bytes. At 1.216M records/s with 4096 slots per worker, jemalloc held 16.2 GB (RSS 17 GB) while the exporter accounted 3.4 GB, the rest being requests held in the receiver (4096 slots x 4 workers x about 1 MB). With the shipped 128 slots and `ingress.max_request_bytes` 16 MiB, the unbudgeted worst case is 2 GB per worker.
- [ ] README sizing states the whole-process bound: workers x (exporter budget + receiver slots x maximum request size), with the measured figures, and warns that raising `max_concurrent_requests` to lift the strict admission ceiling multiplies this term.
- [ ] At startup the exporter logs that bound, computed from its own configuration and the receiver slots it can see (or states that it cannot see them).
- [ ] Decide and record a byte bound for in-flight requests: either a receiver-level byte limit (an engine change, upstream) or an exporter-level retryable refusal when bytes held for unacknowledged requests pass a threshold; implement the exporter-level one here if the engine change is deferred.


**Amendment (Task 5 finding, 2026-09-24): the reference Alloy config and the receiver's message limit.** `configs/series-parquet.alloy` has no batch processor, so a request is whatever the Loki bridge reads at once (20,000 lines, 7.96 MB in Task 5); the engine's OTLP gRPC receiver refuses messages above tonic's 4 MiB default (`max_decoding_message_size`, crates/otap/src/otap_grpc/server_settings.rs) with OUT_OF_RANGE, which OTLP clients retry forever, so nothing is delivered. The exporter meanwhile accepts requests up to `ingress.max_request_bytes` 16 MiB.
- [ ] The reference Alloy config gains `otelcol.processor.batch` with a size cap that keeps requests well below the receiver limit (for example 4,000 records), with a comment explaining the limit and the slot formula (larger requests raise the strict ceiling).
- [ ] The shipped series_parquet engine configs set the receiver's `max_decoding_message_size` equal to the exporter's `ingress.max_request_bytes`; at startup the exporter warns when the receiver limit it can see is below `ingress.max_request_bytes` (or states it cannot see it).
- [ ] An E2E case sends one Alloy batch above 4 MiB and asserts it is stored, so the suite covers large batches, not only 12 lines.
- [ ] Engine finding for an upstream PR (not the exporter): the OTLP gRPC receiver answers an oversize message with OUT_OF_RANGE (tonic), which OTLP clients retry forever although a retry can never succeed; answer with a non-retryable status and count the refusal in `receiver.otlp.requests.rejected`. Record the evidence and the proposed change; implement it here only if it stays inside the receiver and has its own test.

**Amendment (user triage 2026-09-25): what Task 12 fixes, what goes to the plan-4 backlog, what is only documented.** The shipped default becomes the buffered topology because producers need fast acknowledgements: Alloy will neither wait for a 15 s window nor hold data itself. Defects on the buffered path that break correctness are therefore fixed here; throughput work on components other than the exporter is not. This amendment overrides the earlier Task 12 amendments where they differ.

Fix in Task 12:
- [ ] T10-F1: `durable_buffer` handle_shutdown shuts Quiver before queued downstream ACK controls reach handle_ack, so a graceful restart replays acknowledged data (Task 10: 10,200 / 2,600 duplicate records). Fixed here although it is buffer code, because every deploy of the default topology hits it.
- [ ] F1: a failed phase 2 of a multipart upload (sink write.rs:704-714) issues no abort and increments no counter, so incomplete uploads accumulate unseen (Tasks 9 and 11: http503, reset, TCP ACK loss, held completion). Abort with its own bounded deadline, count the orphan when the abort fails, and recommend an `AbortIncompleteMultipartUpload` lifecycle rule in the README.
- [ ] Alloy: the first three items of the Alloy amendment above (batch processor, receiver `max_decoding_message_size` equal to `ingress.max_request_bytes` with the startup warning, E2E case above 4 MiB). The upstream OUT_OF_RANGE item moves to the backlog.
- [ ] Option A size limits, as amended above.
- [ ] Wide sort key reservation (Task 6): the block reservation must charge the sort key bytes of wide keys so the budget cannot be exceeded.
- [ ] jemalloc raw heap dumps and offline jeprof, as amended above; they also explain the occasional `rss_reconciliation` excursions of Tasks 7 and 11.
- [ ] Task 11 findings on the lateness bound and on the late-commit detector, if the Task 11 review confirms them as product defects (not rig or judge errors).
- [ ] The flaky `OutageSlice.test_storage_outage_recovers_without_losing_acked_data` (Alloy's per-attempt deadline races the server's refusal under load).
- [ ] Shipped defaults (buffered topology, receiver slots, window) proposed with options for the user after Task 13.
- [ ] Receiver in-flight memory: only the first item of that amendment (README states the whole-process bound workers x (exporter budget + receiver slots x maximum request size) with the measured figures). The startup log line and the byte bound move to the backlog.

Moved to the plan-4 backlog (recorded in docs/superpowers/plans/2026-09-23-plan-4-backlog.md): durable_buffer WAL throughput (segment finalization off the worker runtime, exposed sync interval and segment size); receiver in-flight byte bound and its startup log line; engine `pipeline.memory.usage` crediting frees only to the allocating thread; receiver load-shed not counted and its misleading message; the receiver's retryable OUT_OF_RANGE for an oversize message; row-group tail pinning the previous row-group buffer (promoted back here only if the heap dumps show it matters); flaky tests in crates the campaign does not touch (log_tap hang, otlp_grpc_exporter and opamp AddrInUse); harness polish (synchronous telemetry/allocator pairing, recorded measurement conditions, bench heap counters); worker scaling 0.67 and the ~6 cores estimated for 1M records/s (shared writer).

Documented only (README/FORMAT limitations section, and Task 14):
- T10-F2: after SIGKILL the WAL replays entries acknowledged by the exporter but whose progress was not yet persisted; duplicates within that window are part of at-least-once. Task 13 measures the bound and the documentation states it as a number.
- Strict mode is bounded by the window: ceiling per worker = slots x records per request / hold time (128k/s at a 1 s window, 8.5k/s at 15 s); fast acknowledgements need the buffered topology.
- The WAL writes about 2x the wire bytes and syncs every 25 ms; put it on its own fast device. State the measured buffered ceiling (144k/s local, 152k/s MinIO with 4 workers on one NVMe).
- `LocalFileSystem` never fsyncs (already in the README).
- Merge chunks may exceed `merge_chunk_bytes` by about 1 percent.
- The ~60 s write stall after a store restart (Task 9): documented as a rig property if Task 11 attributes it to the rig; if it is the engine's connect timeout it becomes a Task 12 fix.


### Task 13: Full durable-buffer acknowledgement, restart and replay proof

**Expected wall-clock cost:** 55-80 minutes for both stores, both topologies' latency cohorts and window comparisons; under one second for latency/backoff analysis tests.

**Files:**
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_buffered.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-latency.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/strict-latency.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-midwindow.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-outage.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-ambiguous.json`

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes `run_named`, `Ledger`, actual buffered Engine config, `FaultRig.completed_values`, Task 11's values-only dropped completion response and Task 10's kill/restart.
- Produces named cases `buffered-latency`, `strict-latency`, `buffered-midwindow`, `buffered-outage`, `buffered-ambiguous`; `buffered_checks(result: dict) -> None` extends `fault_check` where relevant and requires lossless retention/no-resend proof.
- `latency_summary(latencies_s: list[float]) -> dict` uses nearest-rank p50/p95/p99 with sample count; `retry_delay_bounds(retry_count: int, initial_s: float, max_s: float, multiplier: float) -> tuple[float, float]` returns the source-derived jitter envelope `[0.5*base, base]`, where `base=min(initial_s*multiplier**retry_count,max_s)`.

- [ ] **Step 1: Add failing no-resend and ambiguous-replay tests**

```python
class BufferedProofTests(MeasurementTestCase):
    # Scenario: the process dies after WAL ACK and before window completion.
    # Guarantees: retained disk replays every ACKed record without a producer resend.
    def test_midwindow_replay_without_resend(self):
        require_long()
        result = run_named("buffered-midwindow", self.output_dir)
        self.assertEqual(result["metrics"]["post_kill_producer_attempts"], 0)
        self.assertEqual(result["metrics"]["missing_acked_ids"], 0)
        buffered_checks(result)

    # Scenario: values are complete in S3 but their response bytes are dropped before SIGKILL.
    # Guarantees: restart replays the unresolved buffer cohort with valid duplicates.
    def test_ambiguous_completion_replays(self):
        require_long()
        result = run_named("buffered-ambiguous", self.output_dir)
        self.assertGreater(result["metrics"]["pre_kill_complete_ids"], 0)
        self.assertGreater(result["metrics"]["ids_with_multiplicity_at_least_two"], 0)
        self.assertEqual(result["metrics"]["post_kill_producer_attempts"], 0)
        buffered_checks(result)
```

Each named case contains both store backends; each backend's independent checks must pass before the aggregate status passes. Register separate subtests/JSON cell identities for logs and metrics, not just one mixed count.

- [ ] **Step 2: Run red and add a fast backoff-envelope test**

```python
# Scenario: a second retry uses the default multiplier with bounded implementation jitter.
# Guarantees: the checker permits jitter but rejects immediate busy-loop retries.
def test_backoff_bounds(self):
    self.assertEqual(retry_delay_bounds(2, 1.0, 30.0, 2.0), (2.0, 4.0))
```

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_buffered -v
```

Expected: missing backoff analysis; long proofs skip without explicit opt-in.

- [ ] **Step 3: Measure producer ACKs at 15s and 120s windows**

Use the actual `receiver -> durable_buffer -> series_parquet` graph, identical physical core IDs and persistent root, pass-through OTLP, backpressure, no expiry, 1GiB available capacity and default retry settings. Ensure configured cap is above Quiver's startup minimum for the selected core count. Use one core for this proof so bundles can be correlated without cross-core aggregation ambiguity.

At each window, send 600 requests at two requests/s, ten supported records/request, identical workload and request sizes, alternating logs and mixed supported metrics. A 120s window receives at most 240 requests and approximately 2.4MiB payload; verify measured block/request thresholds are not reached and byte/request early-rotation counters remain zero. Use 5s producer RPC timeout, the Alloy OTLP exporter's documented default, instead of the shipped example's deliberately longer override. Record the exact producer configuration and no queue saturation.

Measure end-to-end first-send-to-success latency p50/p95/p99/max and timeout count; also report per-attempt latency when a request retries. Compare windows at the same offered rate, hardware and buffer capacity. Apply the Controller baseline policy in Global Constraints to the measured latency distribution, timeout rate and window delta; no first-run absolute latency SLO is imposed. For a cohort with sufficient remaining window time, observe producer ACK while no corresponding values object exists and exporter ACTIVE holds it. Then prove durability with the kill experiment, rather than interpreting low latency alone as a successful fsync. Report buffer disk growth, per-core path and known ingest/ACK ordering from source.

Run matching STRICT (`receiver -> series_parquet`, `wait_for_result: true`) cohorts on both stores at the same 15s and 120s windows, same input sizes/rate/cores and at least 600 completed-request samples per cell. For both topology comparisons set receiver request capacity and bounded producer in-flight capacity to 512: two requests/s across a 120s hold requires more than the shared default of 128. Verify no producer concurrency saturation and no byte/request early rotation. Keep buffered timeout at the default 5s; use a recorded 300s STRICT timeout to observe complete durable ACK latency without 5s censoring. Report the proportion of STRICT samples exceeding 5s separately; a supplemental 5s compatibility control reports censored/time-out observations, never substitutes those for successful-ACK percentiles.

`strict-latency.json` and `buffered-latency.json` contain raw per-request first-send/admission-proxy time, ACK time, aligned window phase, p50/p95/p99/max, timeout/censor counts, sample counts, offered rate and all queue limits. Record oldest-pending-request age from the ledger as an external request-age estimate and label any unknown admission offset; correlate it with ACK latency/window position. These distributions are evidence for or against a future rotation trigger based on the age of the oldest pending request. Record the decision inputs in Task 14; do not implement that trigger in this plan. Apply the Controller baseline policy separately by topology/window/fingerprint, and keep ACK-durability semantics hard on every run.

Run the existing Alloy producer in a separate short compatibility control with timeout explicitly set to 5s; collect queue occupancy, exporter failures, enqueue losses, delivered IDs and RSS. The exact synthetic-producer latency distribution remains the authoritative per-request measure, because file tailing does not expose per-line OTLP ACK timestamps. Do not confuse Alloy file discovery latency with buffer acknowledgement latency.

- [ ] **Step 4: Kill mid-window after ACK, then restart with no sender**

Run the experiment at both 15s and 120s windows. Choose a fresh cohort and wait for all its successful producer RPCs, recorded ledger ACKs, nonzero ACTIVE bytes and absence of its values objects. Ensure at least five seconds remain before the aligned window boundary; if the cohort cannot reach the gate in that interval, retry setup with new IDs after an observed new window, under a bounded setup deadline. Kill immediately once the gate holds, and retain evidence timestamps proving the order. Do not infer the boundary from a sleep.

Before restart close all producer channels, stop all producer workers and snapshot attempt-table row count. Restart with the same persistent buffer path, exact core IDs and destination, in a new engine attempt directory. The buffer initializes from telemetry/timer activity; do not send a new telemetry payload just to trigger replay. Require every pre-kill ACKed ID in the final reader oracle, valid descriptors, changed boot ID, lossless retention and drained queue/retry state. Assert the attempt-table row count did not change after kill. Report buffered replay duration and multiplicities, including any duplicate caused by a race that still satisfied the independently proved object gate.

- [ ] **Step 5: Prove exporter NACK -> buffered backoff -> recovery**

Stop the real store after WAL ACKs and nonempty exporter state, keep disk below capacity, and maintain the outage beyond the default 60s flush deadline. Require `series_parquet.flush_failed` plus `nacks{reason="storage"}`, correlated `durable_buffer.bundle.nacked` events with retryable cause, increasing `retries.scheduled`/requeued evidence, and at least two attempts of the same bundle before restoring storage. Whole-block failures do not emit the individual-admission `series_parquet.request_failed` event. The buffer event supplies retry_count and backoff_ms; compare to source's jittered exponential formula, allowing measured scheduler/reporting delay but rejecting an immediate retry loop. Permanent rejection and retention-loss counters must remain zero.

```python
def retry_delay_bounds(retry_count, initial_s, max_s, multiplier):
    base = min(initial_s * multiplier ** retry_count, max_s)
    return (0.5 * base, base)
```

Use event monotonic receipt times plus the logged delay to bound actual retry timing; wall log timestamps alone cannot establish precise scheduling. Recover the store, require oldest-unacked to return to baseline, buffer queue/retry/in-flight work to drain, and every ACKed ID to appear. Producer resends remain disabled for already ACKed records. This proof is additional to the general S3 family, because it checks retry ownership and backoff explicitly.

- [ ] **Step 6: Kill after object completion but before downstream ACK delivery**

Reuse Task 11's values-only downstream dropped completion response, with one supported signal per cohort and a small single-PUT values object. Before sending, capture buffer resolved/acked counters and logs. Require (1) durable producer ACKs for the cohort, (2) all its series and values objects complete and readable through the independent store client, (3) valid descriptor coverage and exact pre-kill IDs, (4) active response-drop evidence, (5) exporter FLUSHING/completion still pending, and (6) no buffer downstream-ACK resolution for that cohort. NGINX must preserve the healthy series route; dropping every S3 response would block before values completion and would not exercise this boundary.

Kill the entire engine while response bytes continue to be dropped. A completed object whose success response never reached the exporter cannot yet have caused that exporter to notify the buffer, which establishes a stronger observable boundary than racing a guessed `before_ack` instruction. Record this reasoning and the evidence; no production failpoint is required.

After confirmed process exit, restore proxy traffic, restart with retained buffer/core IDs, and leave producers stopped. Require a second stored copy of every selected fully completed cohort ID under the new boot ID, valid descriptor coverage for both generations, and no missing ACKed IDs. Count exact multiplicities rather than asserting exactly two globally: frozen-name retries in the first boot and replay in a new boot have different duplication behavior. Historical IDs outside the replay cohort must retain their prior counts. Repeat for logs and metrics on MinIO and RustFS. If the buffer had already resolved the cohort, the injection missed its intended boundary and must fail, not pass as a duplicate-free recovery.

- [ ] **Step 7: Run green, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_buffered -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure buffered --output-dir /tmp/series-buffered
```

**Recorded numbers:** STRICT and buffered ACK p50/p95/p99/max, oldest-pending-age estimates, censored samples and sample counts for both windows, p99 delta, timeout/early-rotation counts, ACKed cohort sizes, post-kill producer attempts, replay/drain times, exact duplicate histograms, NACK/backoff measurements, buffer heap/mapped RSS and disk separately from exporter, and all oracle violations. **Failure:** buffered ACK waiting for object completion instead of the proven durable local write, a numerical regression under the Controller baseline policy, early rotation invalidating the comparison, missing acknowledged record, any producer resend in a no-resend proof, changed core/path, missed ambiguous boundary, no replay duplicate for the completed cohort, busy-loop/permanent retry handling, retention loss or inability to drain.

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/buffered-latency.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/strict-latency.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/buffered-midwindow.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/buffered-outage.json)
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/buffered-ambiguous.json)
git add docs/superpowers/reports/series-parquet-measurement/strict-latency.json rust/otap-dataflow/crates/validation/tests/series_parquet/test_buffered.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/buffered-latency.json docs/superpowers/reports/series-parquet-measurement/buffered-midwindow.json docs/superpowers/reports/series-parquet-measurement/buffered-outage.json docs/superpowers/reports/series-parquet-measurement/buffered-ambiguous.json
git commit -m "chore: prove buffered series acknowledgement and replay semantics" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

**Amendment (user decision 2026-09-23): buffered topology becomes the shipped default.** After Task 13 has proved the durable-buffer topology (including the fix for the acknowledged-data replay after a graceful restart found in Task 2), switch the shipped example configs to durable_buffer in front of the exporter, and set the exporter defaults for that topology so the documented shutdown bound `window.interval + 2 x (flush_retry_deadline + upload.abort_timeout)` fits the documented 60 s termination grace (the buffer owns long retries; the exporter deadline can be short). Strict ack-after-flush stays a documented option with the in-flight formula and its own example config. Re-run the launcher smoke and the E2E suite on the new defaults; Task 14 reports both topologies.

**Amendment (third review, 2026-09-23): bounded items for Task 12.** Increment `flush.retries` at attempt time, not at completion or abandon; `flush.failed` events carry window, sequence and object path like `block_committed`; `producer_id_attribute` accepts an ordered fallback list (default host.id, service.instance.id) and a counter reports requests with an empty producer id; validation covers `max_requests_per_block: 1` explicitly and gives every size limit an upper bound; Azure quality gate (user decision 2026-09-23): one E2E run against Azurite is sufficient. Azure storage authenticates only through a bearer-token capability (crates/otap/src/object_store.rs requires_bearer_token_provider), so start Azurite with `--oauth basic` over HTTPS with a self-signed certificate trusted by the engine, and bind a token capability that serves a static well-formed token, for example the existing `urn:otel:extension:k8s_service_account_token_auth` pointed at a token file. The run writes logs and metrics, reads them back with DuckDB and ClickHouse through the oracle, and proves at-least-once like the S3 lane; the Azurite image is pulled once and pinned by digest in the workflow's dispatch lane. If Azurite cannot be made to accept the token path, report it and refuse Azure at startup with the reason instead; accumulate series rows into runs up to `run_target_bytes` instead of one run per request, and size values builders to the actual row count (both after Task 6's measurement).

**Amendment (from Task 6, 2026-09-23):** Task 13 also owns the buffered transient decomposition Task 6 left open: the durable buffer's heap versus mapped segment split, physical versus logical WAL disk bytes, and a backlog/replay experiment (build a backlog during an outage, then measure RSS and disk while it replays), all under taskset -c 0-7,16-23 with the Task 6 ledger.

**Amendment (user review of the buffered topology, 2026-09-23):** with durable_buffer in front, the producer is acknowledged at the WAL write, backpressure becomes a retryable NACK (UNAVAILABLE) only once the WAL cap is reached, and there is no strict producer-to-Parquet latency bound. Task 13 therefore also: (a) measures the producer-to-values-file latency distribution in the healthy state (buffer segment finalisation, poll interval, the exporter window and the flush) and during and after an outage, reporting the nominal figure (expected roughly window + ~1.1 s + write) and how it grows with backlog; (b) proves that an exporter PERMANENT refusal after the WAL acknowledgement (damaged body, too_large, unsupported) is dropped by the buffer and counted in `resolved{outcome="permanently_rejected"}`, and states that this is a loss the producer never sees; (c) records the drop_oldest and max_age loss paths as configuration-owned losses with their counters. Task 14 states the buffered contract explicitly: an OK to the producer means "on the local WAL", not "in S3", and names the freshness signal an operator must alert on.

### Task 14: Consolidated measured report and exact section-9.8 amendment

**Expected wall-clock cost:** 1-3 minutes for report/lint validation; allow 15-60 additional minutes for required full workspace checks after implementation.

**Files:**
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/report.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Create: `docs/superpowers/reports/series-parquet-measurement/manifest.json`
- Create: `docs/superpowers/reports/2026-09-22-series-parquet-measurement.md`
- Modify: `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md`, section 9.8 only

**Interfaces:**
- Applies the Controller baseline/environment policy in Global Constraints; every discovered defect enters Task 12, including discoveries during Task 13 or final reporting. Hard correctness, validity and residual failures cannot become a baseline.
- Consumes schema-version-1 JSONs from Tasks 1-13; exact required filenames are listed below. Does not accept invented values or silently replace failed runs with averages.
- Produces `load_runs(directory: Path) -> dict[str, dict]`, `render_report(runs: dict[str, dict]) -> str`, and `amend_measured_section(spec_text: str, runs: dict[str, dict]) -> str`.
- `manifest.json` recursively enumerates every required index, run and baseline file with its SHA-256, artifact references, configuration fingerprint, coverage cells, accepted/failed/skipped status and validation counts. A final measurement status is `complete_pass`, `complete_fail` or `incomplete`, never simply "measured" when mandatory data is absent.

- [ ] **Step 1: Add failing report completeness and precise-edit tests**

```python
# Scenario: an otherwise populated report lacks the DNS buffered RustFS cell.
# Guarantees: a partial failure matrix cannot be promoted to measured acceptance.
def test_report_rejects_missing_matrix_cell(self):
    with self.assertRaisesRegex(AssertionError, "dns_nxdomain:buffered:rustfs"):
        validate_matrix({("dns_nxdomain", "strict", "minio"): "passed"})

# Scenario: the spec's unmeasured paragraph has already changed since planning.
# Guarantees: report generation refuses a broad or guessed replacement.
def test_spec_edit_requires_exact_old_paragraph(self):
    with self.assertRaisesRegex(AssertionError, "section 9.8"):
        amend_measured_section("### 9.8 What has been measured\nDifferent text.\n", {})
```

`validate_matrix(cells: dict[tuple[str, str, str], str]) -> None` enumerates the Tasks 9-11 fault names, both topologies and both stores, emits all absent cell keys in sorted order and raises if any is missing/non-passed. Report completeness also checks all four buffered proof names plus `strict-latency`, both topologies/windows for latency and both windows for mid-window, both signals where specified, two full soaks, all Criterion/async stage names and full metric schemas, direct attribution, stable repeated memory/capacity evidence, host/probe evidence and resolved bounded defects. Keep failed-but-complete evidence reportable, but block a passing acceptance amendment.

- [ ] **Step 2: Run red, then implement completeness and numeric tables**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing report module/checks. Implement the required artifact inventory literally:

```python
REQUIRED_RESULTS = (
    "harness-contracts.json", "harness-local.json", "launcher-ci.json",
    "stages.json", "attribution.json", "capacity-local.json",
    "capacity-minio.json", "capacity-rustfs.json", "memory-strict.json",
    "memory-buffered.json", "soak-strict.json", "soak-buffered.json",
    "fault-preflight.json", "failure-s3.json", "failure-process.json",
    "failure-network.json", "remediation.json", "buffered-latency.json",
    "strict-latency.json", "buffered-midwindow.json", "buffered-outage.json",
    "buffered-ambiguous.json",
)
```

The report includes environment/provenance and reproduction commands; every throughput trial and measured rate; workload/core/compression denominators; extraction/encoding/upload CPU shares and residual; stage allocations/expansion factors; accounted/RSS curves and every named transient; separate buffer memory/disk; soak input/drain/RSS/FD/coverage/multiplicities; every failure cell's injector/evidence/recovery/missing/duplicate numbers; STRICT and buffered ACK distributions at both windows, plus oldest-pending-age rotation decision evidence; no-resend and ambiguous-replay proofs; fingerprinted baselines, hard checks and regression outcomes; tool skips and artifact retention. Add CSV-friendly Markdown tables and link raw JSONs/profiles. Large sample series remain in JSON; report summary numbers must be calculated from them, not hand-copied.

Include a section labelled "Inputs to the shared-writer decision": one versus four-core efficiency, descriptors duplicated per worker, file sizes/object counts, CPU-stage shares, storage utilization, backlog and per-worker memory cost. State what these observations do and do not establish. Also include the STRICT and buffered latency distributions at both window sizes as inputs to the oldest-pending-request-age rotation decision. Do not select or implement that trigger or spec 10.4's design in this plan.

- [ ] **Step 3: Replace exactly the two obsolete sentences in spec 9.8**

Leave the section heading, introductory sentence, and four historical facts from plans 1/2 unchanged. They remain historically scoped; do not replace "18 tests" with the new suite total while retaining its old provenance. Delete exactly these two sentences, including their existing line wrapping:

```text
The exporter's per-core throughput ceiling, memory envelope and the
failure-model behaviour required below have NOT yet been measured. The
existing outage and replay cases do not establish the full failure matrix;
these measurements are the subject of the next plan.
```

Insert these exact new sentences only after all mandatory runs exist and their acceptance checks pass:

```text
Plan 3 has measured the exporter's per-core throughput ceiling, local and S3
write speed, stage CPU shares, memory envelope and failure-model behaviour.
The [plan-3 measurement report][series-measurement-report] records the measured
numbers, environment, workloads, configurations, uncertainty and acceptance
results, including the thirty-minute soaks and every required failure case
in both topologies.
The same report records the section 9.5 durable-buffer proof: 15 s and 120 s
acknowledgement latency, mid-window restart without producer resends,
storage-outage retries with backoff, and replay after ambiguous completion.
These results establish only the measured configurations and durations;
the longer nightly and qualification programme in section 10.3 remains deferred.

[series-measurement-report]: ../reports/2026-09-22-series-parquet-measurement.md
```

The linked report contains every numeric fact, avoiding a second hand-maintained set of numbers in the spec. `amend_measured_section` locates the boundaries from `### 9.8` to `### 9.9`, requires the old paragraph exactly once inside them, validates complete passing evidence, replaces only that paragraph, and compares the prefix/suffix bytes for equality. No section-9.9 requirement is weakened. If any gate fails, write the measured report with failed numbers and do **not** perform this passing amendment; list the exact blocker and keep integration-ready false. Task 14 is not complete until the required failures are resolved and the amendment is justified.

- [ ] **Step 4: Verify default runtime and required checks**

The fast CI lane runs the legacy 18 tests, the new contract/local checks, the two short PR soaks and analysis checks within the added five-minute budget. Fault-tool smoke is a separate explicitly dispatched provisioned lane. Long tests skip with explicit messages. Add a workflow-dispatch boolean `measure_long` with default false. Its long jobs are separate from the existing 60-minute PR job: (1) stages/attribution/capacity/memory with an eight-hour limit, (2) soak, and (3) failure matrix/buffered proof with six-hour limits. Invoke Task 12 contingency checks after failures and budget full reruns explicitly. Run `measure remediate` once before reporting even with no defects, to publish the required inventory. Pass measured capacity/memory artifacts into downstream jobs and retain the same designated hardware fingerprint for comparisons. Provision fault tools only where needed, set both require flags and `SERIES_MEASURE_LONG=1`, reserve runner cores, execute each job's measurements sequentially and upload all artifacts. The full programme is several hours; do not squeeze it into the PR budget or run it merely because a pull request triggered the workflow.

All following cargo commands are future implementation validation, from `rust/otap-dataflow`:

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
cargo xtask check
python3 -m unittest crates.validation.tests.series_parquet.test_measurement crates.validation.tests.series_parquet.test_soak crates.validation.tests.series_parquet.test_failures crates.validation.tests.series_parquet.test_buffered -v
python3 -m crates.validation.tests.series_parquet.measure report --output-dir ../../docs/superpowers/reports/series-parquet-measurement
```

Then from repository root:

```bash
python3 tools/sanitycheck.py
markdownlint-cli2 docs/superpowers/reports/2026-09-22-series-parquet-measurement.md docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md rust/otap-dataflow/crates/validation/tests/series_parquet/README.md
```

**Recorded numbers:** total required/passed/failed/skipped cells, number and duration of runs, zero-loss/coverage counts, and all tables above. **Failure:** missing required result/cell/metric, inaccessible raw artifacts, nonmatching hashes/fingerprints, undocumented threshold changes, overstated memory ceiling, any failed acceptance gate, stale spec text, non-ASCII content, broken tests/lints or fast-runtime budget violation. Passing numbers cannot be fabricated by editing result JSON.

- [ ] **Step 5: Commit the final report and precise spec amendment**

```bash
(cd rust/otap-dataflow && python3 -m crates.validation.tests.series_parquet.measure stage-results --index ../../docs/superpowers/reports/series-parquet-measurement/manifest.json)
git add rust/otap-dataflow/crates/validation/tests/series_parquet/report.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/manifest.json docs/superpowers/reports/2026-09-22-series-parquet-measurement.md docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md
git commit -m "chore: publish series parquet measurement and failure evidence" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

## Out of scope for this plan

- A rotation trigger based on the age of the oldest pending request; Task 13 records the STRICT/buffered evidence only.
- Spec 10.4's shared-writer design, ownership changes or implementation. This plan only produces numbers that inform that decision.
- Spec 10.1 live introspection endpoints, tail/buffer APIs, custom flush endpoint or accepted/committed/acked runtime state API. Existing admin telemetry/shutdown and external observation suffice.
- Spec 10.2 compactor and any compaction scheduling, manifests, discovery metadata or deduplication facility.
- Any change to the on-disk format, partitioning, dataset layout, schema semantics, series identity or compression contract. Diagnostic uncompressed benchmark output is not shipped as a new format option.
- Spec 10.3 hours-long nightly runs, 24-72-hour qualification, random chaos, production failpoint hooks and canary/production rollout. This exclusion does not defer the mandatory 30-minute soak or deterministic failure matrix.
- Extrapolating the measured rate to host classes that were never measured. Task 5 reports cores required for 1,000,000 records/s on this host only.
- Claiming power-loss durability from a process-kill test, exactly-once delivery, production-wide capacity from one host, or a universal memory bound from sampled RSS.

## Amendments (2026-09-22)

- Revision 7, current merged metrics layout, current memory-accounting terms and current durable-buffer code are the implementation authority. The older format template's deferred descriptor-materialization and stamp-only whole-seal statements are not imported.
- The existing E2E suite is extended additively. Reuse Engine, DockerStore, AlloyProducer and the readers; add strict telemetry and generic ID oracles where the existing fixture helpers are intentionally narrower.
- Use layered Criterion groups beginning with `otlp_noop`, registered pipeline stages and complementary async/cpu-time/DHAT/perf runs. Section 9.6 and 9.9 bullet 2 require the complete layer ladder and full metrics for every stage.
- Keep external fault tooling optional for local discovery and mandatory for the acceptance run. Presence/absence was checked read-only on this machine; no tooling was installed while planning.
- Default test execution remains short. Long measurements require `SERIES_MEASURE_LONG=1`; each task ends with a runnable command, a committed artifact and an explicit number/failure gate. A failing empirical model is reported rather than disguised as a validated reservation.

## Self-Review

### Spec 9.9 bullet-to-task mapping

| Spec 9.9 bullet | Tasks and concrete acceptance artifact |
| --- | --- |
| 1. Thirty-minute soak, input/drain rates, RSS, descriptors, multiplicities and no acknowledged supported loss | Tasks 1-2 supply the ledger/oracle/JSON epoch proof and enforced host controls. Task 7 produces `soak-strict.json` and `soak-buffered.json` with at least 1,800s input, complete samples and drain; Task 14 reports them. |
| 2. Maximum throughput/core and extraction/encoding/upload shares using 9.6 | Task 3 registers `otlp_noop` and the cumulative Criterion groups, full stage metrics and complementary async/DHAT runs in `stages.json`; Task 4 supplies exclusive perf attribution in `attribution.json`; Task 5 brackets and repeats capacity. |
| 3. Local/S3 write speed in records/bytes per second with workload, cores and compression | Task 5 produces local, MinIO and RustFS capacity indexes with unique-record/input-byte/object-byte denominators, fingerprints and production ZSTD configuration. |
| 4. Memory model versus RSS, conversion/seal/merge/encoding/upload, separate buffer usage and residual | Task 6 requires three independent paired release runs per topology/config, phase samples and stable median/range summaries in `memory-strict.json`/`memory-buffered.json`. Task 3 profiles transients; Task 7 records trends; Task 14 reports uncertainty and every reservation discrepancy. |
| 5. S3 slowdown/errors, restarts/kills, network/DNS/TCP faults, at-least-once in both topologies | Task 8 provisions/probes disposable fault tools; Tasks 9-11 produce the complete two-store/two-topology failure indexes, activation evidence, ID coverage and duplicate multiplicities. Task 12 fixes bounded defects and reruns failed measurements. |
| 6. Full 9.5 durable-buffer proof | Task 13 produces buffered latency, mid-window no-resend recovery, outage/backoff and ambiguous-replay indexes, plus matching `strict-latency.json` at 15s/120s as requested by the controller. Tasks 2/8/10/11 supply buffered launch, store tooling, restart and dropped-response evidence. |

### Additional spec and repository checks

- Sections 5.7/6.6: worker reservations remain explicitly distinct from measurements. Every named transient is measured; retained-state invariants and unexplained RSS residual stay hard. Controller baseline policy governs numerical performance/memory acceptance.
- Section 9.5 strict outage and short PR soak: Tasks 7 and 9 keep default-deadline long evidence separate from the labelled short override. Task 13 records ACK order and replay, with default buffered timeout and fully observed STRICT latency.
- Section 9.6: Task 3's registered layers begin with OTLP-to-noop and reach real MinIO. Criterion is mandatory for synchronous layers; async, DHAT and perf complement it. The controller ruling applies to expansion measurements as well as other numerical regressions.
- Sections 9.7/9.8: Task 14 refuses a passing amendment with missing, invalid or failed required evidence. It replaces exactly the quoted two old sentences, preserving historical facts and every other section. Architectural blockers remain visible for plan 4.
- Sections 10.3/10.4: extended nightly/qualification and the shared-writer architecture remain deferred; the mandatory 30-minute soak/failure programme and bounded remediation are assigned here. Oldest-pending-age rotation is evidence-only.
- Repository instructions: ASCII source/results/changelogs, immediate Scenario/Guarantees test comments, Rust checks from the workspace, explicit file staging and exact trailers remain mandatory. Task 12 copies the changelog template for user-facing bounded fixes. This revision itself edits only this plan.

### Placeholder scan

- Scanned for unfinished-body markers, unspecified validation/error handling, deferred implementation language and stale references to the former controller-rulings section. No unresolved placeholders remain.
- Runtime substitutions `STORE_IP`, `STORE_PORT`, `BYTECODE`, `ENGINE_PID`, `FAULT_CONTAINER_ID` and CLI `PATH`/`NAME` come from inspected containers/processes or parsed arguments. The exact discovered defect and regression input are deliberately selected from measured evidence by Task 12's classification procedure, not pre-invented.
- All 14 tasks have an independently testable deliverable, explicit wall-clock cost, verification commands, failure rules and an evidence commit. Synthetic contract checks are labelled as such and cannot establish a measured baseline.
- Every new result index, CLI family, stage name and cross-task helper has an owner. Required result inventory contains 22 top-level artifacts; run/baseline/finding children are enumerated with hashes. No result or benchmark number is claimed to already exist.

### Cross-task interface consistency

- Task 1 owns `Workload`, `RunSpec`, ledger/ID oracle, JSON sampler/drain, result publication, baseline evaluator and exact-file evidence staging. Task 2 owns Engine options, PID/TID mapping, lease and build/environment monitor. Tasks 3-14 consume these contracts.
- Epoch progression is per-worker `pipeline.uptime` from JSON with `keep_all_zeroes=true`; unchanged HTTP snapshots cannot prove drain. Retry code classification imports `RETRYABLE_CODES`, including CANCELLED, and store recovery consistently uses `DockerStore.recover()`.
- Task 3 owns cumulative Criterion and complementary stage names/full metrics. Task 4 owns exclusive CPU categories and `run_attribution`; Task 5 consumes both without mixing profile fingerprints. Task 6 pairs normal release observations independently of profiling builds.
- Task 8 owns `FaultRig`, namespace launcher, probe and ACK-bytecode helpers; Task 9 owns `failure_case`/`fault_check`; Tasks 10-11 extend families. Task 13 reuses these for buffered no-resend proof and records a separate STRICT latency index. No production ACK hook is introduced.
- Task 12 can interrupt any task, including Tasks 13-14, to fix a bounded defect and rerun it. Only source-evidenced architectural implementation is deferred; neither a failed bounded fix nor an invalid measurement is a new passing baseline.
- Every future commit block invokes `stage-results` on its explicit indexes before committing; recursive staging includes all indexes, run/baseline/finding children and hashes. Tasks 3 and 12 explicitly stage Cargo.lock. Source paths in the contingency are individually enumerated from the reproduced finding.
- Baseline fingerprint covers machine/core allocation/effective config/workload/build profile. Source/binary hashes remain provenance so code regressions remain comparable. Start/end snapshots, observed affinities, lease/build-monitor coverage, samples and oracle/residual checks decide validity before baseline publication.
- Task 14 requires all 22 artifacts, their children, both latency topology/window matrices, repeated memory evidence and remediation outcomes. Its final manifest cannot hide a skipped required case, unstable baseline or uncommitted child.

### Rulings applied

1. **Baseline (C1):** Applied the controller's single Global Constraints policy throughout: valid fingerprinted baseline establishment/commit and matching-run regression checks, with hard correctness, measurement validity and unexplained-residual checks on every run. Removed absolute first-run memory/performance SLOs and the separate capacity regression rule. Engineering reservations remain diagnostic comparisons.
2. **Environment (I2):** Enforced start/end CPU/core/RAM/kernel/load/affinity snapshots, a physical-core minimum, worker-TID checks and abort on mismatch, host lease and continuous build exclusion. Verified that runtime affinity failure only warns in `rust/otap-dataflow/crates/controller/src/lib.rs:2790`.
3. **Remediation (C2):** Added Task 12 after the failure-model tasks for bounded fixes, regression tests, repository changelog and failed-measurement reruns. Architectural findings alone go to a plan 4 decision with evidence, including spec 10.4 examples. Verified the template-copy policy in root `AGENTS.md:34` and `.chloggen/TEMPLATE.yaml`/`config.yaml`.
4. **Benchmarks (C3):** Added Task 3 cumulative Criterion layers, registered `otlp_noop` and full metric schema; Task 4 retains direct attribution. Verified spec 9.6 at line 1556 and 9.9 bullet 2. Verified the engineering-reservation description in `rust/otap-dataflow/crates/core-nodes/src/exporters/series_parquet/worker.rs:810`.
5. **Epochs (I1):** Applied JSON/zero retention and three collection-uptime advances in Task 1. Verified `rust/otap-dataflow/crates/admin/src/telemetry.rs:433` and `:1515`, existing `test_e2e.py:2240`, and `rust/otap-dataflow/crates/engine/src/pipeline_metrics.rs:545`.
6. **Memory stability (I3):** Task 6 now requires independent paired release repetitions, minimum phase samples, median/range or confidence intervals and rejection of unstable baseline candidates.
7. **Harness compatibility (I4):** Task 1 imports the existing retry set; ABORTED requires evidence and a producing regression test. All store recovery uses `recover()`. Verified `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py:2327` and `:1519`.
8. **Committed evidence (I5):** Every commit block invokes the shared exact-file staging command, including baseline children; dependency-changing tasks name Cargo.lock explicitly. Publication of compact evidence precedes staging.
9. **Network privileges (I6):** Task 8 grants NET_ADMIN only to the disposable namespace owner, probes UDP/TCP DNS rules and xt_bpf before case traffic, and makes required failures fatal while optional discovery skips cleanly. Task 11 consumes those verified capabilities.
10. **Toxiproxy (M1):** Tasks 8/11 use official KB/s units and the dropped completion response name/behavior. The linked official toxic definitions and timeout cleanup confirm discarded bytes followed by connection closure on removal; bypass HEAD/GET proof remains.
11. **Task size (M2):** Split harness/result-schema from launcher/CI (1/2), bench mechanics from attribution (3/4), and fault provisioning from state machines (8/9); renumbered all tasks and references.
12. **Controller addition:** Task 13 records STRICT latency distributions at the same 15s/120s windows and offered rates alongside buffered results. Task 14 reports evidence for the oldest-pending-request-age rotation decision; no such trigger is implemented here.
