# Series Parquet Measurement Implementation Plan

<!-- markdownlint-disable MD013 MD032 MD031 MD040 MD024 MD033 MD046 MD029 MD004 -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce reproducible measurements and passing acceptance checks for every requirement in design revision 7 sections 9.5 and 9.9, including the recommended durable-buffer topology.

**Architecture:** Extend the existing real-process Python E2E harness with deterministic workloads, a disk-backed acknowledgement ledger, structured results, and externally injected faults. A purpose-built Rust timing target isolates library stages; real engine runs establish throughput, resident memory, recovery, and topology semantics. Preserve the existing 18 tests, Alloy lane, MinIO/RustFS adapters, and independent DuckDB/ClickHouse readers.

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
- Use the existing admin Prometheus endpoint `/api/v1/telemetry/metrics` for gauges. Missing, stale, malformed, or ambiguously attributed samples are errors, never zero. Distinguish process metrics from per-worker metrics and do not sum a process RSS value across workers.
- Use admin shutdown with a deadline of at least `interval + 2 * (flush_retry_deadline + abort_timeout) + 15s`; use 180s for 15s windows and 300s for 120s windows. The controller's 60s signal shutdown is insufficient for the latter. Hard-kill cases intentionally bypass shutdown.
- Faults act on real processes, store HTTP traffic, DNS, or kernel networking. No mock ObjectStore, fake errors in exporter code, or production failpoints count as failure-model evidence.
- Full acceptance requires both strict producer retry and buffered persistent replay, both supported signals, both existing store backends, and both readers. A skipped required case means incomplete acceptance, even when optional local test discovery exits successfully.
- Default test discovery adds at most five minutes after engine build and image provisioning. `SERIES_MEASURE_LONG=1` enables throughput sweeps, profiled memory runs, the 30-minute soak, and long failure/proof runs. CI runs the fast subset with this variable unset; a manually dispatched long lane may set it. Missing binaries/images/tools skip before starting a case unless `SERIES_REQUIRE_DOCKER=1` or `SERIES_REQUIRE_FAULT_TOOLS=1` makes them mandatory. A fault or startup failure after successful preflight is a failure, not a skip.
- All new source, configuration, result metadata authored by this work, and Markdown are ASCII-only. JSON uses `ensure_ascii=True`. Every test declaration has immediately preceding `Scenario:` and `Guarantees:` comments. Add copyright/SPDX headers and public Rust documentation.
- Workspace lints deny `missing_docs`, `unwrap_used`, `unused_results`, and `print_stdout`. Use fallible Rust entry points and write benchmark JSON to a file. Do not add lint suppressions to get a measurement through.
- All future cargo commands run from `rust/otap-dataflow`. After Rust edits run affected-crate `cargo check`; run focused tests, `cargo xtask check-benches`, and final `cargo xtask check`. Do not share another agent's cargo target directory during implementation; use an agreed dedicated `CARGO_TARGET_DIR`.
- Measurement code and reporting are development-only: use `chore` in the implementation PR title; no changelog is needed. A user-visible bug discovered by a gate requires a separately scoped fix and copied changelog template, not an unreviewed change hidden in this programme.
- Every implementation commit stages explicit filenames, never `git add -A`, `git add .`, or a directory. Its final two trailer lines are exactly `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` and `Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd`.

## Ground truth and integration decisions

Source names below are authoritative integration anchors, not new APIs to invent. Re-read them if the branch advances before execution.

| Evidence | Consequence |
| --- | --- |
| `crates/validation/tests/series_parquet/test_e2e.py`: `Engine`, `DockerStore`, `AlloyProducer`, `verify_layout`, `verify_files`, `verify_readers`, `clickhouse_reader`, `canonical_column`, `canonical_row`, `stored_multiplicity`, `rss_bytes` | Extend these helpers additively. Do not replace the 2,893-line harness or copy a second engine/store implementation. Preserve all 18 legacy tests and their default call signatures. Paths in this table are relative to `rust/otap-dataflow`. |
| `Engine.__init__` starts immediately, shallowly replaces exporter sections, and selects one core through the example YAML | Add keyword-only topology, explicit cores, and launcher settings before serializing/launching. Deep-merge measured overrides into a recorded effective config; retain old behavior for old callers. |
| `Engine.exporter_gauges` returns zero on an absent endpoint/series and keeps only a last matching sample | Keep this legacy convenience method; add a strict labelled sampler for measurements. Several workers require distinct samples, not the last line's value. |
| `engine_metrics` and `metric_max` read JSON telemetry; some instruments publish interval deltas | Use Prometheus for gauges, retain sample timestamps, and only aggregate a delta once per distinct collection epoch. Producer IDs and stored rows remain the delivery oracle; metric totals are corroboration. |
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
| Workspace `Cargo.toml` | Criterion 0.8.0, cpu-time 1.0.0 and DHAT 0.3.3 already exist. Choose a purpose-built `harness = false` bench because isolated allocation lifetimes, async upload completion, and a common JSON contract matter here; Criterion's statistical wall timing alone does not provide those measurements. |

### Fault tools checked on this machine

Read-only inventory on 2026-09-22 found Docker client/server **29.2.1**, `ip`, `tc`, `nft`, `iptables`, `dnsmasq`, `dig`, `tcpdump`, `nsenter`, `perf`, `valgrind`, Python and npx. The session is UID 1000; presence of host commands does not establish permission to manipulate a namespace. No namespace mutation or container was started during planning.

The existing MinIO `RELEASE.2025-04-22T22-12-26Z`, RustFS `1.0.0-rc.3`, ClickHouse `26.7.4`, and Alloy `v1.19.2` images are local. `toxiproxy-server`, NGINX, and tshark are absent, as are dedicated Toxiproxy/NGINX fault images. Do not claim those cases are currently runnable without provisioning.

| Fault | Exact injector and activation evidence | Provisioning and clean skip |
| --- | --- | --- |
| Store outage/restart | `DockerStore.stop()` uses `docker stop --time 0`; `docker start` recovers the same container. Require failed real PUT plus exporter storage NACK. | Existing Docker and images available. Existing Docker preflight applies. |
| Slow S3 | Toxiproxy 2.12.0 upstream `bandwidth` and downstream `latency` toxics. Require proxy API state plus delayed completed PUT durations and nonzero forwarded bytes. | Pull `ghcr.io/shopify/toxiproxy:2.12.0` during provisioning, resolve and record digest; skip missing image before test unless fault tools required. |
| HTTP 503 | NGINX `if (-f /control/fail503) { return 503; }` on actual S3 request path. Require access-log PUT/POST status 503 and client storage errors. | Build Task 6's `series-measure-fault-tools:local` image containing distro NGINX. Record base digest/package versions. Skip absent image. |
| Process restart/hard kill | Admin graceful shutdown, then a fresh `Engine`; SIGKILL via `os.kill(pid, signal.SIGKILL)` or `docker kill --signal KILL`. Require old PID exit and a new boot ID. | Host process operations available; container launcher requires Docker. Never kill by process name or kill unrelated engines. |
| Network disconnect/reset | Toxiproxy `enabled: false`, then `reset_peer` on a separate case. Require failed connection and proxy evidence; pcap captures reset. | Toxiproxy image as above; no host routing changes. |
| DNS NXDOMAIN/timeout | dnsmasq in a private container namespace; per-case hostname and authoritative local zone. `iptables` inside that namespace drops port 53 for timeout. Require captured DNS query/response or dropped-query counter. | Task 6 image contains dnsmasq/dnsutils/iptables/tcpdump/tshark; disposable `NET_ADMIN` capability probe must succeed. Skip privilege absence only at preflight. |
| Pure TCP ACK loss | Namespace-local iptables BPF match compiled by `tcpdump -ddd`, dropping store-to-client ACK-only packets. Require counter increase and tshark retransmission evidence. | Task 6 image and `NET_ADMIN`; verify `xt_bpf` support in disposable namespace before case. Skip unavailable capability cleanly. |
| Lost S3 completion response | NGINX routes values PUTs through a dedicated Toxiproxy downstream `timeout: 0`; series traffic bypasses it. Bypass HEAD/GET proves a complete readable values object while upstream response is withheld. | Both images; test readiness uses real small PUTs before the measured fault. This is an application acknowledgement fault, recorded separately from pure TCP ACK loss. |

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
| `rust/otap-dataflow/crates/validation/tests/series_parquet/requirements.txt` | Add Prometheus text parser, preserving current dependencies. |
| `rust/otap-dataflow/crates/series-lake/Cargo.toml` | Purpose-built bench target and existing-workspace dev dependencies. |
| `rust/otap-dataflow/Cargo.lock` | Only lockfile changes actually required by bench dev dependencies. |
| `rust/otap-dataflow/crates/series-lake/benches/measurement.rs` | Fallible CLI, timing/CPU/DHAT controls and output file; no stdout. |
| `rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs` | Conversion, extraction/hash, sort/seal, merge, Parquet encoding, local persistence and S3 upload stages using production library calls. |
| `rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs` | Stage output equivalence and expansion-factor acceptance tests. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py` | Stage invocation, capacity search, perf classification and throughput results. |
| `rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py` | Memory experiments, profile attribution, signed residual and regression limits. |
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
| `docs/superpowers/reports/series-parquet-measurement/*.json` | One compact result JSON per concrete run, family summary indexes named by each task, and a manifest listing every run. |
| `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md` | Task 10 only: replace exactly the two obsolete unmeasured sentences in section 9.8. |

Raw profiles, parquet files, pcaps, logs and ledgers live below a user-selected run directory outside tracked source, and are retained as CI artifacts. Each committed JSON contains artifact paths, SHA-256 hashes, sizes and retention location. Commit samples and aggregates in JSON; do not commit hundreds of megabytes of raw profiles or claim an inaccessible temporary path is a reproducible artifact.

### Shared contracts, budgets and acceptance policy

Use `python3 -m crates.validation.tests.series_parquet.measure` from `rust/otap-dataflow`. CLI subcommands are `run`, `stages`, `capacity`, `memory`, `soak`, `failures`, `buffered`, and `report`; all accept `--output-dir PATH`. `run --case harness-local --output-dir /tmp/series-measure` is the initial runnable slice. Long commands reject execution without `SERIES_MEASURE_LONG=1`, explaining how to opt in; unittest long classes skip with that reason.

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

`build_request(workload: Workload, request_index: int) -> tuple[str, bytes, list[tuple[str, str, str]]]` returns signal, serialized OTLP, and `(record_id, kind, expected_sha256)` rows. `read_oracle(root: Path, ledger: Ledger, *, require_all: bool, healthy: bool) -> dict` returns numeric coverage, missing/unexpected/corrupt counts and a multiplicity histogram after checking both readers. `sample_engine(engine: Engine, *, expected_workers: int) -> dict` returns timestamped per-worker gauges, process RSS and buffer gauges. `drain(engine: Engine, ledger: Ledger, store, *, deadline_ns: int) -> Path` stops new generation, finishes authorized retries, observes empty exporter/buffer state over three distinct telemetry collection epochs, performs graceful shutdown, downloads completed objects if needed and invokes `read_oracle`. A timeout never invokes the oracle on a partial download and calls it success.

One JSON document per run has `schema_version: 1`, `run_id`, `case`, `status`, `started_utc`, `elapsed_s`, `environment`, `config`, `workload`, `metrics`, `samples`, `events`, `checks`, and `artifacts`. Required environment fields: Git revision/dirty patch hash, binary SHA-256/profile/features/allocator, OS/kernel/CPU/RAM, affinity/core IDs, store/reader/producer versions and digests, filesystem/mount type, tools, clock resolution and load average. Configuration includes receiver channels/timeouts, all exporter knobs, buffer settings/path/core IDs, producer rate/concurrency/batching/retry policy, and fault parameters. Metrics carry explicit units in names (`_bytes`, `_s`, `_records_per_s`, `_cpu_ns_per_record`); unavailable values are null with a reason, never zero. Run status cannot be passed with unavailable mandatory metrics.

Every capacity trial, repetition, stage subprocess and fault/topology/store cell is a distinct run file. Name it with `run_id = f"{case}-{topology}-{store}-c{len(cores)}-w{interval_s}-r{ordinal:03d}"`, where the family assigns a unique increasing ordinal and stores the full trial settings in the file. For example, `http503-buffered-rustfs-c1-w15-r001.json` has one environment/config/workload and one result. The sixteen named artifacts in the tasks are either a single-run file (the simple harness/soaks) or a family summary **index**, containing aggregate metrics plus `run_files` and their hashes. They never replace the one-JSON-per-run files. Each index enumerates all its child filenames; re-execution uses a new artifact directory rather than overwriting evidence.

Task 1 also implements `stage_run_files(index_path: Path) -> None` for future commit steps: read the task's completed index, validate every child path is a plain `.json` filename in the report directory, verify hashes, and invoke `subprocess.run(["git", "add", "--", *explicit_paths], check=True)` in bounded argument batches. It stages each enumerated file by name, never a glob or directory. Expose `measure stage-results --index PATH`; invoke it on each task's named result index immediately before that task's explicit `git add`/commit block. For a single-run file it stages that one path. This is an execution-only command and is not run during planning.

Numerical thresholds below are proposed **acceptance defaults**, not facts already established by the sources. Freeze them in the run config before measuring. Any threshold change requires retaining the failed result and explaining the change in the final report; never derive a permissive bound from the same observation it evaluates.

| Check | Default failure rule |
| --- | --- |
| Delivery/semantics | Any acknowledged supported ID missing, unexpected ID, changed payload, missing descriptor, invalid sort/metadata, or reader disagreement fails. Healthy no-retry runs require multiplicity exactly one; fault runs allow duplicates but count each ID/kind. |
| Instrumentation | Missing required metric, wrong worker cardinality, stale sample for over three collection intervals, nonpositive duration, producer-limited claimed engine maximum, or failed fault activation fails the measurement. |
| Repeatability | Three measured repetitions; coefficient of variation over 15% fails a capacity claim pending investigation. Report all repetitions, not the best alone. |
| Stage agreement | Staged ZSTD output must match actual Sink semantics. Sum of exclusive stage CPU and named residual must reconcile with measured process CPU within 10%; unexplained CPU over 20% fails attribution. |
| Expansion | Conversion peak live allocation above `4 * ingress.max_request_bytes`, or encoder workspace above `3 * parquet.writer_limit_bytes`, fails the documented reservation check. Also report ratios to actual wire bytes/current writer size; large configured limits must not conceal expansion ratios. |
| RSS reconciliation | After measured runtime/allocator/buffer terms, unexplained positive residual above `max(32MiB, 0.10 * peak_RSS)` fails; persistent negative discrepancy above that tolerance fails too. This is a diagnostic tolerance, not a new advertised memory ceiling. |
| Soak trend | After five-minute warm-up, one-minute RSS medians must have least-squares slope at most 1MiB/min and final five-minute median no more than `max(32MiB, 5% of baseline)` above the first post-warm-up five-minute median. Samples must cover at least 99% of scheduled epochs. |
| Capacity stability | At accepted offered rate, last 30s unique durable drain rate is at least 98% of offered rate, backlog slope is at most 2% of offered rate, and no permanent rejection/loss occurs. A higher unsustainable trial is needed to bracket the ceiling. |
| Buffer latency | With capacity available, zero producer timeouts at 5s, p99 below 5s in both windows, and `p99_120 - p99_15 < 1s`. Prove ACK before object completion separately; latency alone does not prove WAL semantics. |

Task costs below are expected wall-clock **measurement/verification time after dependencies and binaries are available**, not estimates of engineering effort. Compilation, image provisioning and full workspace checks are separately budgeted at 15-60 minutes each depending on caches. Long measurements run sequentially on reserved cores; concurrent builds invalidate performance results.

---

### Task 1: Deterministic measurement harness and durable result contract

**Expected wall-clock cost:** 2-4 minutes for fast contract/local runs; existing 18-test suite remains a separate 5-15 minute lane.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/requirements.txt`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Record: `docs/superpowers/reports/series-parquet-measurement/harness-local.json`

**Interfaces:**
- Consumes existing `Engine`, `DockerStore(kind)`, `AlloyProducer`, `rss_bytes`, `verify_layout`, `verify_readers`, `clickhouse_reader`, and canonical helpers without removing their signatures.
- Produces the shared contracts above. Add keyword-only `topology="strict"`, `buffer_path=None`, `cores=None`, `launcher=None` to `Engine`; restart creates another Engine rooted in a new attempt directory, reusing destination, buffer path, and core IDs.
- A launcher implements `start(argv: list[str], log, env: dict) -> subprocess.Popen` and `pid(process) -> int`. The default launches locally. Task 6 supplies the isolated container launcher. Engine uses the real engine host PID for RSS and kill, not a docker CLI PID.

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

Create SQLite with WAL mode and `synchronous=FULL`; retain bounded request metadata in memory and regenerate retry bytes by index. Enforce unique ID primary keys and immutable request hashes. Record attempted IDs, successful RPC ACKs, retryable NACKs, local deadlines, partial rejections, and outstanding IDs separately. Retry only UNAVAILABLE, RESOURCE_EXHAUSTED, ABORTED, DEADLINE_EXCEEDED, and connection failures; unexpected permanent statuses fail. Retries have a bounded deadline and capped exponential scheduling, driven by next-attempt monotonic time, not a fixed test sleep.

- [ ] **Step 4: Extend Engine and implement the strict telemetry/drain path**

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

Add `prometheus-client>=0.21,<1` for `prometheus_client.parser.text_string_to_metric_families`. Select labels identifying group, pipeline, node and core from actual samples; reject duplicate worker identities. Map dots to the endpoint's underscore names, retaining original metric-set identity. Require ACTIVE/FLUSHING/pending/token/cache/accounted/budget/oldest gauges for every worker. Save raw sample labels/timestamps in artifacts. Do not sum interval counters on repeated scrapes of the same epoch. Read `/proc/PID/status`, `/proc/PID/stat`, `/proc/PID/smaps_rollup`, FD count and buffer directory `stat` allocation bytes on the same monotonic sample timeline. Keep engine, producer, store and proxy process RSS separate.

Drain on three fresh collection epochs with no exporter outstanding requests/notification work and, for buffered mode, no in-flight/retry/queued work; then admin-shutdown and inspect the retained store. Missing gauge publication fails under the deadline. The independent final oracle checks every acknowledged ID, every intended ID once authorized retries finish, descriptor coverage per `(signal,date,hour,writer_id,boot_id,series_id)`, identity hash, sort metadata/order, canonical latest-descriptor joins, unchanged payloads and both readers. Stream SQL comparisons and persist multiplicities; do not materialize all rows as Python objects.

- [ ] **Step 5: Write atomic results and wire fast CI**

```python
def write_result(path, result):
    encoded = json.dumps(result, sort_keys=True, indent=2, ensure_ascii=True,
                         allow_nan=False) + "\n"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(encoded, encoding="ascii")
    temporary.replace(path)
```

Validate mandatory fields, units, checks and status before writing; a failed result may omit unavailable measured fields only with explicit reasons. Add a local real-engine `harness-local` case with 100 requests, mixed supported signals, 1s windows, and exact no-retry multiplicity. Add buffered smoke with the same oracle. CI builds with `series_parquet,aws,durable-buffer`, runs all original E2E tests, then `test_measurement`; set no long flag. Retain results/logs with `actions/upload-artifact` on both success and failure. Extend AlloyProducer with optional admin metrics port and host PID discovery, reuse its existing River config, and sample queue size/capacity and enqueue failures without treating infinite-retry `send_failed` as a reliable counter.

- [ ] **Step 6: Verify, record numbers and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
python3 -m crates.validation.tests.series_parquet.measure run --case harness-local --output-dir /tmp/series-measure
SERIES_REQUIRE_DOCKER=1 python3 -m unittest crates.validation.tests.series_parquet.test_e2e -v
```

**Recorded numbers:** offered/ACKed/stored supported records, each metric kind count, duplicates, missing IDs, descriptor violations, elapsed time, input/output bytes and peak engine/producer RSS in `harness-local.json`. **Failure:** any common oracle/instrumentation failure, nonzero healthy duplicates, a changed legacy assertion, missing Docker coverage in the required lane, or the fast suite exceeding five minutes without a documented cause. Copy only the completed compact result to its named report path.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/requirements.txt rust/otap-dataflow/crates/validation/tests/series_parquet/measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/harness-local.json
git commit -m "chore: add deterministic series measurement harness" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 2: Isolated stage timings, CPU attribution and allocation measurements

**Expected wall-clock cost:** 15-30 minutes for the full stage matrix; 15-30 seconds for reduced contract tests.

**Files:**
- Modify: `rust/otap-dataflow/crates/series-lake/Cargo.toml`
- Modify if resolution changes: `rust/otap-dataflow/Cargo.lock`
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
- `run_stages(spec: RunSpec, output_dir: Path) -> dict` invokes one new process per stage/repetition/profile, aggregates into `stages.json`, and exports exact stage names `convert`, `extract`, `sort_seal`, `merge`, `encode`, `local_write`, `upload`, and `sink`.
- `classify_cpu(samples: list[dict]) -> dict[str, int]` assigns each perf sample once to conversion, extraction/hash, sort/seal/merge, encoding, upload/client, buffer, engine/runtime, allocator, or unknown, using the innermost matching production frame; upload wait is a separate wall duration.

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

Place this test in the existing `test_measurement.py`. Since the bench has `harness = false`, its Rust checks are explicitly invoked by `--self-test`; do not put unreachable `#[test]` functions behind a custom harness and assume cargo runs them. Declare `mod stages; mod tests;` with explicit `#[path = "measurement/stages.rs"]` and `#[path = "measurement/tests.rs"]` in the bench root. The `--self-test` branch invokes `tests::run() -> Result<(), Box<dyn std::error::Error>>`, then exits without a measurement. Include this concrete reservation regression in that function's calls:

```rust
/// Scenario: a measured workspace is one byte larger than its reservation.
/// Guarantees: the benchmark fails an exceeded reservation instead of only reporting it.
fn expansion_limit_rejects_overshoot() {
    assert!(super::stages::check_expansion("conversion", 65, 64).is_err());
    assert!(super::stages::check_expansion("conversion", 64, 64).is_ok());
}
```

In `stages.rs` define `pub(super) fn check_expansion(stage: &str, peak: u64, limit: u64) -> Result<(), std::io::Error>` with documentation, returning `Error::other(format!("{stage}: peak={peak} exceeds reservation={limit}"))` when `peak > limit` and `Ok(())` otherwise. Add self-test calls for diagnostic encoding versus actual Sink and input generation excluded from timing, with Scenario/Guarantees comments; use the public pdata fixture constructors and the independent row readers already used by the library sink tests. Each self-test is called by `tests::run`, and any failed assertion/Result makes the CLI exit nonzero.

- [ ] **Step 2: Run red before adding the bench**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
cargo bench -p otel-arrow-dfe-series-lake --bench measurement --no-run
```

Expected: missing classifier and missing bench target. Cargo is executed only by the future implementer.

- [ ] **Step 3: Add the purpose-built target and timing primitive**

Add workspace dev dependencies `cpu-time`, `dhat`, and `bytes` only where used; enable object_store's `aws` feature for the bench through its dev dependency. Declare:

```toml
[[bench]]
name = "measurement"
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

For the single-thread async stage measure process CPU before/after `runtime.block_on` with `cpu_time::ProcessTime`, and wall time separately; there are no concurrent benchmark stages. Run fixture construction, input cloning, filesystem setup, warm-up and output verification outside the timed region. Use black_box on retained inputs/outputs, not a discarded future. Each timing process executes at least 30 samples and one second of accumulated measured work; cap iterations by a 60s deadline and mark incomplete if the minimum is not met.

For allocation mode use the safe `dhat::Alloc` global allocator in this bench executable only and one DHAT profiler per process. Start it after fixtures are created; record `HeapStats` before/after and maximum live allocation during the stage, then drop the stage output under the profiler. Profile fixture-retained bytes separately to distinguish input from workspace. DHAT instrumentation results never supply throughput numbers. No unsafe allocator wrapper or global profiler is added to production library code.

- [ ] **Step 4: Implement and validate every stage against the production path**

| Stage | Timed operation | Untimed input and correctness check |
| --- | --- | --- |
| OTLP network to noop | Same Python sender into real engine OTLP receiver plus existing noop exporter, with acknowledgements enabled | Calibrate producer/network capacity and process CPU; this is a pipeline baseline, not a library stage or durable-storage claim. |
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

- [ ] **Step 5: Measure pipeline CPU shares directly**

Run `perf record -F 199 -g --call-graph dwarf -p ENGINE_PID -o perf.data` while Task 1 drives the actual engine. Resolve the PID from Engine, place perf output in that run's artifacts, and stop perf after the observable input phase ends. Classify `perf script` samples by the explicit stages above, with encoding taking precedence over ancestor sink/upload frames. Report per-core CPU seconds, CPU ns/record, sample count/confidence, extraction/encoding/upload CPU percentages, scheduler/off-CPU wall fraction and named residual. Require at least 10,000 classified samples across repeated runs; lengthen the opt-in profile if needed. If perf permissions prevent profiling, preflight skips the attribution run; mandatory acceptance remains incomplete until it is run on a permitted host. Do not infer CPU shares by subtracting two end-to-end throughput numbers or normalize overlapping upload waits into a CPU pie chart.

Report per stage records/s/core, CPU ns/record, allocated bytes/record, peak RSS, peak live heap/workspace and output bytes/input-record. For stages without serialized output, use the measured output representation's bytes and label it `arrow`, `extracted`, or `parquet`; do not equate those byte rates.

- [ ] **Step 6: Run green, apply failure gates, record and commit**

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
cargo bench -p otel-arrow-dfe-series-lake --bench measurement -- --self-test
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure stages --output-dir /tmp/series-stages
```

**Recorded numbers:** all stage metrics above, direct CPU shares, conversion/encoder expansion, equivalence counts and three repetition dispersions in `stages.json`. **Failure:** incorrect stage output, hidden work in the wrong stage, absent allocation/RSS/CPU data, excessive unexplained CPU, violated conversion/encoder reservations, or high run variance. Preserve failing numbers; do not change reservations in this measurement task. Reduced tests run in fast CI; profiled measurements are opt-in.

```bash
git add rust/otap-dataflow/crates/series-lake/Cargo.toml rust/otap-dataflow/crates/series-lake/benches/measurement.rs rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py docs/superpowers/reports/series-parquet-measurement/stages.json
git commit -m "chore: measure isolated series parquet stages" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

If dependency resolution changes Cargo.lock, stage that exact filename in a separate `git add rust/otap-dataflow/Cargo.lock` before this commit; do not force a lockfile rewrite.

### Task 3: Maximum sustainable throughput and durable write speed

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

Matrix: local, MinIO and RustFS; one and four explicitly allocated physical cores; logs-only and 80/20 logs/metric-point mixed workloads; 1KiB bodies and a documented 8KiB variant; 10k hot series and cardinality churn; production ZSTD. Use the same seed and record counts for comparable trials. Limit the primary exhaustive search to mixed/1KiB/hot-series; run the other workload rows as fixed-rate confirmations at 80% of its capacity and bracket separately if they fail. These confirmations cannot be labelled their own maxima. Diagnostic uncompressed numbers come only from Task 2.

- [ ] **Step 4: Record completed bytes and per-core costs without double counting**

Maintain separate denominators for offered wire bytes, accepted supported records, unique stored values, physical stored rows including duplicates, and completed Parquet object bytes. Local byte totals use completed files; S3 uses HEAD content lengths, cross-checked against downloads. Exclude incomplete multipart parts from successful write speed and report their bytes separately where the store exposes them. Report both steady-state interval write speed and `total completed bytes / time from first send to final completion`, so tail drain cannot disappear from the throughput claim.

Record CPU seconds for engine, producer and store; total engine CPU/core occupancy; output/input compression ratio; average file size; objects/s; number of descriptor duplicates across workers; and producer p50/p95/p99 ACK latency. Join Task 2 stage shares by the identical workload/config/binary fingerprint, never by a convenient nearby run. The shared-writer discussion consumes measured per-worker descriptor overhead, scaling efficiency and stage saturation; this task implements no shared writer.

- [ ] **Step 5: Verify, record numbers and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure capacity --output-dir /tmp/series-capacity
```

**Failure:** common loss/semantic checks; claimed maximum without an unsustainable bracket; producer bottleneck; increasing backlog at the accepted rate; permanent rejection; missing byte/compression/core metadata; CV above 15%; nonpositive throughput. No absolute records/s performance promise exists in the spec: this task establishes a reproducible ceiling rather than inventing one. Compare later runs against this committed baseline at identical fingerprints, with a predeclared 15% regression threshold; differing hardware is reported rather than falsely compared.

**Recorded numbers:** three per-store JSONs contain every trial, sustainable/unsustainable bounds, records/s/core, total records/s, wire and Parquet bytes/s, CPU shares, scaling efficiency, compression, ACK percentiles, drain duration and oracle counts. A failed trial used to bracket capacity is marked `unsustainable` inside a valid search; semantic loss is always a failed run, never a useful bracket.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/performance.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/capacity-local.json docs/superpowers/reports/series-parquet-measurement/capacity-minio.json docs/superpowers/reports/series-parquet-measurement/capacity-rustfs.json
git commit -m "chore: establish series parquet throughput and write rates" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 4: Validate retained memory, transients and the RSS residual

**Expected wall-clock cost:** 25-45 minutes for paired/profiled runs; under one second for accounting tests.

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

Sample normal release engine RSS/smaps and gauges at 100ms and telemetry at 100ms during targeted short memory runs; use 1s for soak. Sampling misses shorter allocation peaks, so use synchronous heap measurements and conservative overlap bounds below as well. Start with receiver/noop idle baseline, strict idle baseline, buffered idle baseline, and equal-workload strict/buffered runs on the same cores. Measure data pages, mappings/stacks, allocator retained pages, engine receiver queues, channels and gRPC buffers. A paired RSS difference is an estimate with an interval, not an exact allocation label.

- [ ] **Step 4: Exercise and account for each transient explicitly**

| Transient | Experiment and observation | Bound/check that can fail |
| --- | --- | --- |
| Conversion | Requests near 16MiB with wide strings, nested attributes and histogram arrays; measure wire overlap, converted pinned Arrow bytes and DHAT peak under the conversion stage | Compare measured workspace to `4*I`; report actual peak/wire expansion separately. Violation fails even if total process RSS is under a loose envelope. |
| Admission/final values sealing | Build several values runs plus unfinished building batches; retain pre-seal snapshots, seal, record old/new uniquely pinned buffers and allocation peak | Compare measured extra retained/copy space with revision-7 `(V+1)*R`, including run overshoot and series timestamp buffers. Record `V`, actual largest run and the resulting conservative bound; failure if observed peak exceeds the stated bound. A stamp-only series test is insufficient. |
| Resident merge keys | Narrow default keys, then wide custom body/attribute keys across the whole block; allocate/consume actual MergeIter in the isolated profile | Attribute Arrow RowConverter/key_rows allocations and heap OwnedRows; compare narrow/wide results to a second-payload-scale bound derived from actual row lengths plus row offsets/heap capacity. Keys remain live until iterator drop, not just one output chunk. |
| Merge output | Skew widths so a chunk groups wide rows after many narrow ones; record pinned bytes of every yielded chunk | Report maximum/target ratio, `rows_per_chunk * max_row_bytes` conservative payload bound and Arrow buffer overhead. Any unaccounted overshoot fails the model; do not assert `actual <= M` when source uses average width. |
| Encoding | High-cardinality strings/dictionaries, null-heavy histograms and near-row-group limits; record ArrowWriter memory before/after write/flush/close and peak stage heap | Separate input chunk, output Vec capacity and Parquet workspace; compare workspace to `3*W`. Check final flush/close peak, not only steady writes. |
| Upload | Pre-encoded payload above three part sizes through real S3 at concurrency 1 and 2, plus actual Sink row-group flush; record allocation profile and in-flight part count | Compare live part buffers to `P*(U+1)` and separately measure current encoded chunk and HTTP/TLS client buffers. A whole supplied row group can launch a burst above nominal concurrency; measure and bound that burst by actual encoded row-group bytes, then test the documented reservation rather than silently assuming a hard concurrency cap. |
| Buffer | Same rate/record set with pass-through WAL, then outstanding backlog and replay; sample disk allocation plus process profile | Attribute Quiver/WAL/bundle stacks, file mappings and caches; report buffer heap/mapped RSS interval and physical/logical disk bytes separately from exporter. No expiry/drop-oldest loss or capacity overrun. |

For final values sealing, `V` is the number of buffered values runs in the spec bound; record both run count and dataset count to prevent interpreting it as signal count. Deduplicate shared Arrow allocations across snapshots with `CountedAllocations`. Distinguish the maximum of individually isolated transients from their possible overlap in the real sink. Include concurrently admitted ACTIVE work while FLUSHING encodes/uploads; two isolated peak measurements cannot prove that their sum never overlaps.

Use the existing engine `dhat-heap` feature in a separate profiling binary with default allocator features disabled. Reproduce the release pipeline's functional features explicitly (`series_parquet,aws,durable-buffer`, one crypto provider); run profiling with its working directory set to the run artifact directory so `dhat-heap.json` is isolated. Record DHAT's allocator change and slowdown. For normal-release allocator retention use `/proc/PID/smaps_rollup` plus the selected allocator's available statistics or a Valgrind Massif confirmation (`--pages-as-heap=yes` for page residency attribution, a separate ordinary heap run for stacks). Tool output must substantiate any large allocator/runtime term; do not assign the unexplained residual to "allocator" by definition.

If source claims cannot bound wide-key/sealing behavior within the advertised reservation, the model gate fails and the report describes the counterexample. Keep the measurements runnable and committed; controller decides the separately scoped code fix before integration-ready can be claimed. This task neither silently raises budgets nor changes the on-disk format.

- [ ] **Step 5: Verify, record numbers and commit**

```bash
cargo check -p otel-arrow-dfe-series-lake --benches
cargo xtask check-benches
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure memory --output-dir /tmp/series-memory
```

**Recorded numbers:** RSS/accounted/budget curves, conversion/seal/keys/chunk/encoder/upload peaks and ratios, concurrent peak envelope, cache/token terms, exporter attribution, buffer heap/mapped RSS interval and disk usage, runtime/allocator categories, residual magnitude/uncertainty and profile overhead. **Failure:** any common gate, missing named transient, unexplained residual above the declared tolerance, understated bound, violated 4I/3W reservation, per-worker block overrun, or buffer loss. An engineering `budget + idle RSS + 128MiB` comparison alone cannot pass this task.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/memory.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/series-lake/benches/measurement/stages.rs rust/otap-dataflow/crates/series-lake/benches/measurement/tests.rs rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/memory-strict.json docs/superpowers/reports/series-parquet-measurement/memory-buffered.json
git commit -m "chore: validate series parquet memory accounting against RSS" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 5: Thirty-minute soak and bounded PR-tier coverage

**Expected wall-clock cost:** 70-85 minutes for two 30-minute input phases plus verification; 2-3 minutes for the fast PR-tier pair, inside the five-minute added-suite budget.

**Files:**
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Modify: `.github/workflows/series-parquet-e2e.yml`
- Record: `docs/superpowers/reports/series-parquet-measurement/soak-strict.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/soak-buffered.json`

**Interfaces:**
- Consumes `capacity_search` results, `run_named`, `Ledger`, strict sampler, `drain`, oracle and Task 4 memory limits.
- Produces cases `soak-strict`, `soak-buffered`, `pr-soak-strict`, `pr-soak-buffered`, and `soak_checks(result: dict) -> None`.
- Soak output uses the common schema with full 1s samples and one-minute aggregate rate/RSS series. `metrics.input_phase_s` measures actual producer-active monotonic duration, excluding startup/warm-up/drain.

- [ ] **Step 1: Write the opt-in acceptance tests before implementing soak checks**

```python
class LongSoakTests(MeasurementTestCase):
    # Scenario: mixed supported telemetry flows for thirty minutes in strict mode.
    # Guarantees: all ACKed IDs survive drain and RSS meets the declared trend gates.
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

Use Task 1's `MeasurementTestCase` to retain `self.output_dir`, rather than a TemporaryDirectory deleted on failure. Add a fast arithmetic regression with a synthetic rising RSS series; it must fail the slope gate without launching an engine. Synthetic samples test analysis only, never stand in for a failure injection or a measured soak.

- [ ] **Step 2: Run red on analysis, then implement checks**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_soak -v
```

Expected: short analysis test fails due to absent `soak_checks`; long tests skip without the opt-in variable.

```python
def soak_checks(result):
    metrics = result["metrics"]
    limits = result["config"]["acceptance"]
    failures = []
    for name in ("missing_acked_ids", "missing_intended_ids", "descriptor_violations",
                 "reader_disagreements", "permanent_rejections", "buffer_loss_items"):
        if metrics[name] != 0:
            failures.append(f"{name}={metrics[name]}")
    if metrics["sample_coverage_ratio"] < 0.99:
        failures.append("insufficient RSS/rate samples")
    if metrics["rss_slope_bytes_per_min"] > 1024 * 1024:
        failures.append("RSS slope exceeds 1MiB/min")
    if metrics["rss_median_growth_bytes"] > limits["rss_growth_bytes"]:
        failures.append("RSS post-warm-up median grew beyond limit")
    if failures:
        raise AssertionError("; ".join(failures))
```

The same function requires explicit zero buffer-loss metrics in strict mode tagged `not_applicable: no_buffer`; absence of a required buffered metric is an instrumentation error, not a zero default.

- [ ] **Step 3: Run sustained load with a bounded producer and explicit drain phase**

Choose 70% of the lower repeatable local/S3 sustainable mixed-workload rate from Task 3 for one worker, 15s windows, 10k hot series with deterministic 1% churn, 1KiB bodies and 20% metric points. Set `Workload.requests = ceil(offered_records_per_s * 1800 / records_per_request)` plus the separately labelled warm-up cohort before starting; do not accidentally retain the 100-request smoke default. Use MinIO for strict and RustFS for buffered; failure tasks cover both stores in both topologies. Record the actual offered rate, series count and churn count. Input lasts at least 1,800 monotonic seconds after readiness and warm-up; no reduction to 30 minutes including drain is accepted. If generation exhausts its planned cohort early, the duration gate fails instead of counting idle time as load.

Every 1s sample records attempted, accepted and unique committed record deltas; wire and object bytes; in-flight requests; ACTIVE/FLUSHING/pending/notify gauges; oldest unacked; RSS and FD count; per-worker cache occupancy; store/proxy/producer RSS; buffered disk/queued/in-flight/retry gauges. Unique committed IDs can be enumerated asynchronously from completed files into the disk-backed oracle; keep reader work on separate cores and record lag. Do not read partial uploads or make the writer wait for a full-table query every second. List new completed objects incrementally and verify them after input stops; report provisional physical-row rate separately if unique-ID lag prevents an instantaneous unique rate.

After generation stops, continue authorized strict retries and buffer replay. Measure backlog at stop, time-to-drain and unique drain records/s, plus full-run average and final five-minute input/storage rates. Verify every acknowledged and every eventually accepted intended ID, exact payload semantics, descriptors and multiplicity histogram after all files are complete. Healthy soak has no injected failures and should be duplicate-free; any actual timeout/retry changes the run to a retry-bearing result with counted duplicates and an explained cause, not an unqualified healthy pass.

Retain Alloy as an additional short producer compatibility run using the shipped batching/retry config and existing `AlloyProducer`. Collect producer `/metrics` and RSS; reject enqueue loss. Do not use its historical queue calculations as a substitute for exact synthetic-producer ID accounting or as this exporter's peak throughput number.

- [ ] **Step 4: Keep the PR-tier test short and real**

Add two 60-75s cases with 1s windows, forced byte/request rotations, and one `DockerStore.stop()` outage past an overridden 3s flush deadline. Gate stop on a successful baseline object and nonempty ACTIVE; keep producing a finite ledger-backed workload. Gate recovery on a storage failure/NACK and elapsed deadline, not a fixed sleep. Use the existing real store stop/start helper here, so this task is independently runnable before proxy tooling exists. Require retryable NACK/local deadline classification in strict mode and buffer retries in buffered mode; recover, drain, assert the common oracle and bounded memory/oldest age. Add them to the fast CI lane after `test_measurement`.

- [ ] **Step 5: Verify, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_soak -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 python3 -m crates.validation.tests.series_parquet.measure soak --output-dir /tmp/series-soak
```

**Recorded numbers:** at least 1,800 samples/input seconds per topology, input/ACK/storage/drain records and byte rates, duration, RSS curve/slope/median change/peak, FD peak, descriptor count/violations, duplicate histogram, missing IDs, buffer memory/disk and retry counts. **Failure:** any common gate, insufficient duration or samples, non-draining backlog, unacceptable RSS trend or unexplained residual, missing ACKed record, descriptor/reader disagreement, unrecorded duplicate, producer enqueue loss or buffer retention loss.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_soak.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/soak-strict.json docs/superpowers/reports/series-parquet-measurement/soak-buffered.json
git commit -m "chore: record thirty-minute series parquet soak acceptance" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 6: Real S3 slowdown, HTTP errors and outage recovery

**Expected wall-clock cost:** 15-25 minutes for both stores/topologies; 20-40 seconds for the optional fast proxy activation smoke.

**Files:**
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/fault-tools.Dockerfile`
- Create: `rust/otap-dataflow/crates/validation/tests/series_parquet/fault-nginx.conf`
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-s3.json`

**Interfaces:**
- Consumes Task 1 launcher contract, Engine/DockerStore, run schema, ledger, sampler and oracle.
- Produces `FaultRig(store: DockerStore, root: Path)` context manager with `storage`, `launcher`, `evidence() -> dict`, `activate(name: str, parameters: dict) -> None`, `recover() -> None`, and `completed_values() -> list[dict]`.
- `failure_case(family: str, fault: str, topology: str, store: str, output_dir: Path) -> dict` runs one matrix cell; family `s3` supports `slow`, `http503`, `store_outage`. Every cell's result includes immutable before/during/after evidence, records, duplicates, timings and peak memory.
- `fault_check(result: dict) -> None` requires `fault_observed`, `recovered`, `drained`, zero missing/coverage/semantic violations, measured duplicates and bounded resources. Families in Tasks 7-8 reuse this exact check.

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

- [ ] **Step 2: Run red and provision separately**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
```

Expected: fast prerequisite tests fail on absent FaultRig; long cases skip. During execution provision the images using the earlier commands, then record `docker image inspect` digests. Never pull images silently from a test.

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

Use a unique Docker bridge network per run, attach the existing DockerStore container to it with alias `store`, and start a fault-tools namespace container. Both Toxiproxy sidecar and containerized engine join it with `--network container:FAULT_CONTAINER_ID`, so the proxy's loopback addresses below are real. The engine mounts binary/repository read-only and run/buffer directories read-write. Publish gRPC/admin ports from the namespace container on host loopback and bind services to `0.0.0.0` inside the namespace. `docker inspect` supplies the actual engine PID. Extend DockerStore with an optional network attachment method and retain all existing storage/reader/download methods. Cleanup removes only recorded container/network IDs in `finally`; retain raw artifacts and the buffer path until oracle verification finishes.

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

`rate` is KiB/s as the tool defines it; save the tool version and units. Remove named toxics through DELETE for recovery. NGINX 503 activation creates `/control/fail503` in the run's mounted directory and removes that exact file to recover. These are real proxy behaviors; no Python HTTP server fabricates storage behavior inside the exporter process.

- [ ] **Step 4: Implement the three S3 cases as observable state machines**

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

Strict mode retries the original serialized request bytes and distinguishes server retryable NACK from producer-local timeout. Buffered mode stops resending any request after its durable producer ACK, retains disk across all actions, and waits for exporter NACK -> buffer retry evidence. Require ACTIVE and FLUSHING each within B, cache within C, pending slot at most one, oldest unacked age returning to baseline after recovery, RSS within Task 4's explained envelope and no buffer expiry/eviction. Report upload retries separately from replay duplicates. Recovery deadline is 300s after endpoint health, raised only by a recorded bound from backlog bytes/minimum measured drain rate before the case begins.

- [ ] **Step 5: Run all cells, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family s3 --output-dir /tmp/series-failure-s3
```

**Recorded numbers:** per cell actual fault duration/status count/bandwidth/latency, requests and supported IDs offered/ACKed/stored, retries/NACKs/timeouts, duplicate histogram, recovery/drain seconds, throughput before/during/after, RSS/accounted/buffer disk peaks and coverage violations. **Failure:** activation not proven; a supported record lost; descriptor/reader mismatch; no retry/backpressure when required; exceeded memory/capacity bound; recovery deadline; or unexplained duplicates outside the replayed ID set. No exactly-once assertion is made for ambiguous requests.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/fault-tools.Dockerfile rust/otap-dataflow/crates/validation/tests/series_parquet/fault-nginx.conf rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_e2e.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/failure-s3.json
git commit -m "chore: measure series recovery from real S3 faults" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 7: Graceful process restart and ungraceful hard kill

**Expected wall-clock cost:** 8-15 minutes for both stores/topologies and kill phases; fast controller tests under one second.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-process.json`

**Interfaces:**
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

`kill_active`: gate on nonzero ACTIVE bytes, zero FLUSHING for the selected new cohort, and no completed values containing that cohort. In strict mode these records may not be ACKed yet; retain and retry them after reconnect. In buffered mode gate on durable producer ACKs first, then kill. Task 9 provides the stricter no-resend proof. If a window rotated before the gate, discard the setup attempt without claiming a fault hit and retry within a bounded setup deadline; repeated inability to hit the boundary fails the test.

`kill_upload`: set real upstream bandwidth limit, use enough incompressible seeded payload to force multipart upload, and observe a store multipart upload plus positive transferred bytes and nonzero FLUSHING. Kill the engine before completion; remove the toxic and restart. Also include a small single-PUT interrupted request so success is not specific to multipart. Record incomplete upload IDs separately from completed object files; incomplete multipart leftovers are not acknowledged-record loss or successful output. Cleanup is test-owned after evidence collection.

On every restart verify identical source bytes on retry, new writer boot UUID, valid descriptor coverage within each boot/partition, no loss in already ACKed historical IDs, and no permanent rejection. Compute exact pre/post multiplicity by ID, listing changes only for cohorts eligible for replay. The stable ledger survives the killed engine and is not recreated from the output being checked.

- [ ] **Step 4: Run green, record and commit**

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family process --output-dir /tmp/series-failure-process
```

**Recorded numbers:** time to exit/restart/readiness/drain, old/new PIDs and boot IDs, known ACKed/pending cohort sizes, replayed IDs, exact duplicate histogram, missing/coverage counts, incomplete multipart count and peak memory/disk. **Failure:** lifecycle gate missed, wrong retained path/core allocation, unacknowledged strict requests abandoned, buffered ACKed IDs absent, false complete-file accounting, descriptor/reader disagreement or deadline overrun.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py docs/superpowers/reports/series-parquet-measurement/failure-process.json
git commit -m "chore: measure series restart and hard-kill recovery" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 8: Network, DNS, TCP ACK and lost-response faults

**Expected wall-clock cost:** 20-40 minutes for the complete two-store/two-topology matrix; 20-40 seconds for optional namespace preflight smoke.

**Files:**
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/failure-network.json`

**Interfaces:**
- Consumes `FaultRig`, container launcher, external-tool inventory, `failure_case` and `fault_check`.
- Adds family `network` cases `disconnect`, `reset`, `dns_nxdomain`, `dns_timeout`, `tcp_ack_loss`, `completion_response_loss`.
- Adds `FaultRig.activate` names matching those cases and `ack_drop_bytecode(store_ip: str, store_port: int) -> str`, which compiles a tcpdump filter into iptables' bytecode form. No production transport fault flags are added.

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

- [ ] **Step 5: Exercise lost application completion responses separately**

Configure the values-only proxy's downstream toxic as:

```json
{"name":"hold_completion","type":"timeout","stream":"downstream","toxicity":1.0,"attributes":{"timeout":0}}
```

Send a single-signal cohort small enough that values use one PUT, not multipart initiation. Series objects bypass the values route and complete normally. Through the independent DockerStore client, repeatedly list/HEAD/GET the expected values object and run the reader oracle on its IDs. Require complete object bytes and valid descriptors while the proxy still withholds the response, the strict producer has no ACK for the cohort, and the exporter still owns its FLUSHING completion. In buffered mode the producer may already have WAL ACKed, but the buffer's downstream resolution must not have advanced for this cohort.

Remove the toxic and terminate the held connection so the client receives a transport failure and retries; then recover/drain. Repeat for logs and metrics independently to avoid the first withheld values response blocking completion of a second signal in the same block. Record duplicates and unchanged multiplicities outside the uncertain cohort. This case proves an application acknowledgement can be lost over TCP; it is not labelled pure TCP ACK loss. Task 9 reuses the same proven completion boundary and kills before recovery to force durable-buffer replay.

- [ ] **Step 6: Verify, record and commit**

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure failures --family network --output-dir /tmp/series-failure-network
```

**Recorded numbers:** per-cell DNS result/query/timeout counts, RSTs, matched ACK drops and retransmissions, completed-but-unacknowledged object counts/bytes, producer/server retry classes, recovery/drain latency, memory/disk peaks, unique IDs and multiplicities. **Failure:** no independent activation evidence, DNS only affecting the proxy rather than engine, data-bearing packets mislabelled as pure ACKs, missing acknowledged records, invalid descriptors, uncounted duplicates, leaked namespace rules/resources, or recovery timeout. Both topologies and both stores are mandatory in the long result; absence of tooling is listed as incomplete acceptance.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_failures.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/failure-network.json
git commit -m "chore: measure series network DNS and acknowledgement faults" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 9: Full durable-buffer acknowledgement, restart and replay proof

**Expected wall-clock cost:** 35-55 minutes for both stores and window comparisons; under one second for latency/backoff analysis tests.

**Files:**
- Create/Test: `rust/otap-dataflow/crates/validation/tests/series_parquet/test_buffered.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py`
- Modify: `rust/otap-dataflow/crates/validation/tests/series_parquet/README.md`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-latency.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-midwindow.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-outage.json`
- Record: `docs/superpowers/reports/series-parquet-measurement/buffered-ambiguous.json`

**Interfaces:**
- Consumes `run_named`, `Ledger`, actual buffered Engine config, `FaultRig.completed_values`, Task 8's values-only response hold and Task 7's kill/restart.
- Produces named cases `buffered-latency`, `buffered-midwindow`, `buffered-outage`, `buffered-ambiguous`; `buffered_checks(result: dict) -> None` extends `fault_check` where relevant and requires lossless retention/no-resend proof.
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

    # Scenario: values are complete in S3 but their response is held before SIGKILL.
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

Measure end-to-end first-send-to-success latency p50/p95/p99/max and timeout count; also report per-attempt latency when a request retries. Compare windows at the same offered rate, hardware and buffer capacity. Require p99 under 5s, zero timeouts and less than 1s p99 increase. For a cohort with sufficient remaining window time, observe producer ACK while no corresponding values object exists and exporter ACTIVE holds it. Then prove durability with the kill experiment, rather than interpreting low latency alone as a successful fsync. Report buffer disk growth, per-core path and known ingest/ACK ordering from source.

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

Reuse Task 8's values-only downstream response hold, with one supported signal per cohort and a small single-PUT values object. Before sending, capture buffer resolved/acked counters and logs. Require (1) durable producer ACKs for the cohort, (2) all its series and values objects complete and readable through the independent store client, (3) valid descriptor coverage and exact pre-kill IDs, (4) active response-hold evidence, (5) exporter FLUSHING/completion still pending, and (6) no buffer downstream-ACK resolution for that cohort. NGINX must preserve the healthy series route; holding every S3 response would block before values completion and would not exercise this boundary.

Kill the entire engine while the response remains blocked. A completed object whose success response never reached the exporter cannot yet have caused that exporter to notify the buffer, which establishes a stronger observable boundary than racing a guessed `before_ack` instruction. Record this reasoning and the evidence; no production failpoint is required.

After confirmed process exit, restore proxy traffic, restart with retained buffer/core IDs, and leave producers stopped. Require a second stored copy of every selected fully completed cohort ID under the new boot ID, valid descriptor coverage for both generations, and no missing ACKed IDs. Count exact multiplicities rather than asserting exactly two globally: frozen-name retries in the first boot and replay in a new boot have different duplication behavior. Historical IDs outside the replay cohort must retain their prior counts. Repeat for logs and metrics on MinIO and RustFS. If the buffer had already resolved the cohort, the injection missed its intended boundary and must fail, not pass as a duplicate-free recovery.

- [ ] **Step 7: Run green, record and commit**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_buffered -v
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 python3 -m crates.validation.tests.series_parquet.measure buffered --output-dir /tmp/series-buffered
```

**Recorded numbers:** ACK p50/p95/p99/max and sample counts for both windows, p99 delta, timeout/early-rotation counts, ACKed cohort sizes, post-kill producer attempts, replay/drain times, exact duplicate histograms, NACK/backoff measurements, buffer heap/mapped RSS and disk separately from exporter, and all oracle violations. **Failure:** window-sized producer hold, latency thresholds, early rotation invalidating the comparison, missing acknowledged record, any producer resend in a no-resend proof, changed core/path, missed ambiguous boundary, no replay duplicate for the completed cohort, busy-loop/permanent retry handling, retention loss or inability to drain.

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/test_buffered.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/faults.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md docs/superpowers/reports/series-parquet-measurement/buffered-latency.json docs/superpowers/reports/series-parquet-measurement/buffered-midwindow.json docs/superpowers/reports/series-parquet-measurement/buffered-outage.json docs/superpowers/reports/series-parquet-measurement/buffered-ambiguous.json
git commit -m "chore: prove buffered series acknowledgement and replay semantics" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

### Task 10: Consolidated measured report and exact section-9.8 amendment

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
- Consumes schema-version-1 JSONs from Tasks 1-9; exact required filenames are listed below. Does not accept invented values or silently replace failed runs with averages.
- Produces `load_runs(directory: Path) -> dict[str, dict]`, `render_report(runs: dict[str, dict]) -> str`, and `amend_measured_section(spec_text: str, runs: dict[str, dict]) -> str`.
- `manifest.json` contains every run's SHA-256, artifact references, configuration fingerprint, coverage cells, accepted/failed/skipped status and validation counts. A final measurement status is `complete_pass`, `complete_fail` or `incomplete`, never simply "measured" when mandatory data is absent.

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

`validate_matrix(cells: dict[tuple[str, str, str], str]) -> None` enumerates the Task 6-8 fault names, both topologies and both stores, emits all absent cell keys in sorted order and raises if any is missing/non-passed. Report completeness also checks all four buffered proof names, both windows for latency/mid-window, both signals where specified, two full soaks, stages, memory and capacity artifacts. Keep failed-but-complete evidence reportable, but block a passing acceptance amendment.

- [ ] **Step 2: Run red, then implement completeness and numeric tables**

```bash
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v
```

Expected: missing report module/checks. Implement the required artifact inventory literally:

```python
REQUIRED_RESULTS = (
    "harness-local.json", "stages.json", "capacity-local.json",
    "capacity-minio.json", "capacity-rustfs.json", "memory-strict.json",
    "memory-buffered.json", "soak-strict.json", "soak-buffered.json",
    "failure-s3.json", "failure-process.json", "failure-network.json",
    "buffered-latency.json", "buffered-midwindow.json", "buffered-outage.json",
    "buffered-ambiguous.json",
)
```

The report includes environment/provenance and reproduction commands; every throughput trial and measured rate; workload/core/compression denominators; extraction/encoding/upload CPU shares and residual; stage allocations/expansion factors; accounted/RSS curves and every named transient; separate buffer memory/disk; soak input/drain/RSS/FD/coverage/multiplicities; every failure cell's injector/evidence/recovery/missing/duplicate numbers; both ACK latency distributions; no-resend and ambiguous-replay proofs; thresholds with pass/fail outcomes; tool skips and artifact retention. Add CSV-friendly Markdown tables and link raw JSONs/profiles. Large sample series remain in JSON; report summary numbers must be calculated from them, not hand-copied.

Include a section labelled "Inputs to the shared-writer decision": one versus four-core efficiency, descriptors duplicated per worker, file sizes/object counts, CPU-stage shares, storage utilization, backlog and per-worker memory cost. State what these observations do and do not establish. Do not select or implement spec 10.4's design in this plan.

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

The linked report contains every numeric fact, avoiding a second hand-maintained set of numbers in the spec. `amend_measured_section` locates the boundaries from `### 9.8` to `### 9.9`, requires the old paragraph exactly once inside them, validates complete passing evidence, replaces only that paragraph, and compares the prefix/suffix bytes for equality. No section-9.9 requirement is weakened. If any gate fails, write the measured report with failed numbers and do **not** perform this passing amendment; list the exact blocker and keep integration-ready false. Task 10 is not complete until the required failures are resolved and the amendment is justified.

- [ ] **Step 4: Verify default runtime and required checks**

The fast CI lane runs the legacy 18 tests, the new contract/local checks, the two short PR soaks, analysis checks and optional fault-tool smoke when provisioned. Long tests skip with explicit messages. Add a workflow-dispatch boolean `measure_long` with default false. Its long jobs are separate from the existing 60-minute PR job: (1) stages/capacity/memory, (2) soak, and (3) failure matrix/buffered proof, each with a six-hour limit. Pass measured capacity/memory artifacts into downstream jobs and retain the same designated hardware fingerprint for comparisons. Provision fault tools only where needed, set both require flags and `SERIES_MEASURE_LONG=1`, reserve runner cores, execute each job's measurements sequentially and upload all artifacts. The full programme is several hours; do not squeeze it into the PR budget or run it merely because a pull request triggered the workflow.

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
npx --yes markdownlint-cli2 docs/superpowers/reports/2026-09-22-series-parquet-measurement.md docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md rust/otap-dataflow/crates/validation/tests/series_parquet/README.md
```

**Recorded numbers:** total required/passed/failed/skipped cells, number and duration of runs, zero-loss/coverage counts, and all tables above. **Failure:** missing required result/cell/metric, inaccessible raw artifacts, nonmatching hashes/fingerprints, undocumented threshold changes, overstated memory ceiling, any failed acceptance gate, stale spec text, non-ASCII content, broken tests/lints or fast-runtime budget violation. Passing numbers cannot be fabricated by editing result JSON.

- [ ] **Step 5: Commit the final report and precise spec amendment**

```bash
git add rust/otap-dataflow/crates/validation/tests/series_parquet/report.py rust/otap-dataflow/crates/validation/tests/series_parquet/test_measurement.py rust/otap-dataflow/crates/validation/tests/series_parquet/measure.py rust/otap-dataflow/crates/validation/tests/series_parquet/README.md .github/workflows/series-parquet-e2e.yml docs/superpowers/reports/series-parquet-measurement/manifest.json docs/superpowers/reports/2026-09-22-series-parquet-measurement.md docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md
git commit -m "chore: publish series parquet measurement and failure evidence" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_016eXMWRZMWytNktdv5v3vdd"
```

## Out of scope for this plan

- Spec 10.4's shared-writer design, ownership changes or implementation. This plan only produces numbers that inform that decision.
- Spec 10.1 live introspection endpoints, tail/buffer APIs, custom flush endpoint or accepted/committed/acked runtime state API. Existing admin telemetry/shutdown and external observation suffice.
- Spec 10.2 compactor and any compaction scheduling, manifests, discovery metadata or deduplication facility.
- Any change to the on-disk format, partitioning, dataset layout, schema semantics, series identity or compression contract. Diagnostic uncompressed benchmark output is not shipped as a new format option.
- Spec 10.3 hours-long nightly runs, 24-72-hour qualification, random chaos, production failpoint hooks and canary/production rollout. This exclusion does not defer the mandatory 30-minute soak or deterministic failure matrix.
- Claiming power-loss durability from a process-kill test, exactly-once delivery, production-wide capacity from one host, or a universal memory bound from sampled RSS.

## Amendments (2026-09-22)

- Revision 7, current merged metrics layout, current memory-accounting terms and current durable-buffer code are the implementation authority. The older format template's deferred descriptor-materialization and stamp-only whole-seal statements are not imported.
- The existing E2E suite is extended additively. Reuse Engine, DockerStore, AlloyProducer and the readers; add strict telemetry and generic ID oracles where the existing fixture helpers are intentionally narrower.
- Choose a purpose-built bench, using existing workspace cpu-time/DHAT plus perf, to isolate actual stages and distinguish CPU from asynchronous wait. The spec's benchmark goals are retained without requiring Criterion as the timing front end.
- Keep external fault tooling optional for local discovery and mandatory for the acceptance run. Presence/absence was checked read-only on this machine; no tooling was installed while planning.
- Default test execution remains short. Long measurements require `SERIES_MEASURE_LONG=1`; each task ends with a runnable command, a committed artifact and an explicit number/failure gate. A failing empirical model is reported rather than disguised as a validated reservation.

## Self-Review

### Spec 9.9 coverage mapping

| Spec 9.9 bullet | Tasks and concrete acceptance artifact |
| --- | --- |
| 1. Thirty-minute soak: input/drain rates, RSS, descriptors, multiplicities, no acknowledged supported record missing | Task 1 supplies stable IDs/oracle; Task 5 produces `soak-strict.json` and `soak-buffered.json`, each with at least 1,800s of active input, complete samples and a final drain oracle. Task 10 reports both. |
| 2. Maximum throughput/core and extraction/encoding/upload time shares from 9.6 | Task 2 produces isolated wall/CPU/allocation measurements and direct exclusive CPU attribution in `stages.json`; Task 3 brackets sustainable per-core capacity with repeatability and producer-headroom checks. |
| 3. Local/S3 write speed in records and bytes/s with workload, cores and compression | Task 3 produces local, MinIO and RustFS capacity JSONs with distinct input/object-byte denominators, complete effective config and ZSTD settings. |
| 4. Memory model versus RSS, all transients, separate buffer memory/disk, explained residual | Task 4 produces strict/buffered memory results for conversion, values sealing, resident merge keys, real chunk overshoot, encoding and upload; Task 5 validates trend over time. Task 10 preserves failures/uncertainty. |
| 5. S3 slowdown/errors, restart, hard kill, network/DNS/TCP acknowledgement faults; stable-ID at-least-once and duplicates in both topologies | Tasks 6, 7 and 8 produce the complete two-store/two-topology fault matrix, with real injector activation evidence, per-ID multiplicities, descriptors, resource limits and recovery/drain deadlines. |
| 6. Full section-9.5 durable-buffer proof | Task 9 produces both-window latency, mid-window no-resend restart, outage retry/backoff, and ambiguous-completion replay results; Tasks 1/6/8 supply the actual buffered topology, real store and completion-response boundary. |

### Additional spec and repository checks

- Section 9.5 strict outage beyond flush deadline and short PR soak: Tasks 5-6; default-deadline long proof and fast overridden-deadline PR proof are labelled separately.
- Section 9.6 all layers and stage metrics, including allocated bytes/record and conversion/encoder expansion failure gates: Tasks 2-4. Diagnostic encoder output is verified against actual Sink.
- Section 9.7 integration readiness: Task 10 cannot publish passing acceptance while any mandatory case is failed, skipped or absent. Canary readiness is not claimed.
- Section 9.8: Task 10 lists the exact two sentences replaced, exact replacement sentences and reference link, preserving all historical facts and all other sections.
- Section 10.3: nightly/qualification durations remain deferred; the required 30-minute measurement and failure programme are fully assigned here.
- ASCII, test comments, workspace lints, cargo working directory, named staging and both exact commit trailers are mandatory throughout. No implementation action is authorized by this planning session's file edit.

### Placeholder scan

- Reviewed all steps for deferred bodies, unspecified checks, invented measured values and dangling helper references. Every new cross-task helper has an owner, signature, inputs/outputs and a concrete algorithm or code body in this plan.
- Uppercase `STORE_IP`, `STORE_PORT`, `ENGINE_PID`, and CLI `PATH`/`NAME` are explicit runtime substitutions obtained from Docker/Engine/arguments, not missing design decisions. The plan contains no fabricated performance numbers or result files claimed to exist.
- Code blocks are implementation anchors; prose tables define full workload matrices, activation evidence, resource accounting, acceptance rules and result fields. The future implementer must not treat those tables as optional examples.
- All recorded JSON paths have producing tasks, all named CLI families have registration tasks, all long checks have runtime/opt-in controls, and every measurement task can fail.

### Cross-task interface consistency

- `RunSpec`, `Workload`, stable IDs, ledger ownership, sampler, oracle, drain and result schema originate in Task 1 and remain unchanged through Task 10.
- Task 2 defines stage names and CPU categories used by Task 3/4/10; physical bytes and unique records have explicit separate denominators.
- Task 6 owns `FaultRig`, `failure_case` and `fault_check`; Tasks 7-8 add families without separate harnesses. Task 9 reuses the values-response hold and retained-buffer restart rather than inventing a production ACK hook.
- Buffered ACKs mean WAL durability; strict ACKs mean object durability. Every oracle still checks supported stable IDs after recovery/drain. Buffer replay never depends on producer resend in the no-resend proofs.
- Per-worker budget/accounted gauges are summed once, process RSS is sampled once, and buffer/allocator/runtime/transient estimates have explicit non-overlap and uncertainty rules.
- Task 10's required result list has the sixteen top-level artifacts named by Tasks 1-9. Family indexes reference one JSON per actual run/cell/repetition; `manifest.json` validates all child hashes. Every child is staged by its explicit filename through `stage-results`, never hidden inside an aggregate-only report.

### Rulings needed from the controller

1. Approve or replace the proposed numerical acceptance tolerances before measurements: 15% repeatability/regression, 10% CPU reconciliation, 20% unexplained CPU, RSS residual `max(32MiB,10%)`, soak trend 1MiB/min and median-growth allowance, and buffered p99 delta below 1s. The source specifies outcomes and engineering reservations, but no universal performance/noise SLO. Until ruled otherwise, freeze the defaults above and retain failures.
2. Designate the host/core set and artifact retention location for publishable long runs. This machine has 32 visible logical CPUs and Docker 29.2.1, but active builds would invalidate performance comparisons; do not stop another agent's work. The runner records affinity and skips a claimed maximum if adequate isolated producer/store/engine capacity is unavailable.
3. Decide the remediation scope if empirical conversion/encoder reservations, seal/key bounds or RSS residual gates fail. This plan records the counterexample and leaves integration acceptance failed; it does not authorize a hidden algorithm, budget, format or shared-writer redesign.
Missing proxy/packet tools have a concrete installation and skip policy, so they do not require a design ruling. No unresolved ruling permits omitting a section-9.9 requirement or marking skipped evidence as measured acceptance.
