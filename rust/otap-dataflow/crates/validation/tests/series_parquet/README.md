# Series Parquet measurement harness

Two lanes live in this directory. `test_e2e.py` is the standing end-to-end
suite: a real `df_engine` process, real object stores in containers, Grafana
Alloy, and two independent readers. `measurement.py`, `measure.py` and
`test_measurement.py` are the measurement lane built on top of it. The
measurement lane extends the end-to-end helpers; it never replaces them.

## Commands

Every command runs from `rust/otap-dataflow`.

```bash
# The standing suite: 18 tests, no skips when Docker is required.
SERIES_REQUIRE_DOCKER=1 python3 -m unittest \
  crates.validation.tests.series_parquet.test_e2e -v

# The measurement contracts: fast, no engine, no container, no build.
python3 -m unittest crates.validation.tests.series_parquet.test_measurement -v

# The same contracts as a published, committable evidence file.
python3 -m crates.validation.tests.series_parquet.measure run \
  --case harness-contracts --output-dir /tmp/series-contracts

# The smallest real-engine measurement, under the enforced host controls.
python3 -m crates.validation.tests.series_parquet.measure run \
  --case harness-local --output-dir /tmp/series-measure

# The launcher lane: the original suite with Docker required, then a strict
# and a buffered local smoke; the buffered one restarts its engine on the
# same cores and the same retained buffer directory halfway through.
python3 -m crates.validation.tests.series_parquet.measure run \
  --case launcher-ci --output-dir /tmp/series-launcher

# Every registered stage and cumulative layer, three repetitions each, with
# Criterion wall times, CPU and resident memory, DHAT allocation and the
# real engine's OTLP-to-noop baseline. Build the benches and both engines
# first (below); the run itself starts no build.
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure \
  stages --output-dir /tmp/series-stages

# The fault-tool contracts, then the disposable fault tools' preflight:
# signed S3 through NGINX and both Toxiproxy routes, DNS/firewall/capture
# probes, the capability split and the first activations, on MinIO and
# RustFS. Provision the images first (Fault tools, below).
python3 -m unittest crates.validation.tests.series_parquet.test_failures -v
SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 taskset -c 0-7,16-23 \
  python3 -m crates.validation.tests.series_parquet.measure fault-preflight \
  --output-dir /tmp/series-fault-preflight

# Stage one published evidence tree by exact file name, before a commit.
python3 -m crates.validation.tests.series_parquet.measure stage-results \
  --index ../../docs/superpowers/reports/series-parquet-measurement/harness-contracts.json
```

Case options are passed as `--option name=value`, decoded as JSON when they
parse: `report_dir` publishes somewhere other than the committed report
directory, `ordinal` numbers a repeated trial, `cores` pins the workers to
explicit core ids. `launcher-ci` also takes `legacy_tests=false` to skip the
original suite and `publish=false`, the CI mode for a runner that is not a
publishable measurement host: nothing reaches the report directory, no
baseline is evaluated, and the index fails whenever a child fails any hard
gate -- delivery, graph, restart, affinity, samples, RSS reconciliation,
lease, monitor coverage, environment -- except the eight-core floor that such
a host is too small to meet.

Every measured case runs the release engine, `target/release/df_engine`
unless `DF_ENGINE` names another binary, and refuses any other build profile
before it starts: a debug engine's memory and speed describe the debug
build, not the exporter. The fixture suite `test_e2e.py` keeps using the
debug build.

`stages` measures one process per stage, repetition and profile, each under
the same host controls as an engine run. It needs four prebuilt binaries and
starts no build of its own, because a compiler running beside a measurement
invalidates it:

```bash
cargo bench -p otel-arrow-dfe-series-lake --bench measurement --bench layered --no-run \
  --features bench-harness
cargo bench -p otel-arrow-dfe-series-lake --bench measurement --no-run --features bench-heap
cargo build --release --locked -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer
cargo build --profile profiling --no-default-features -p otel-arrow-dfe \
  --bin df_engine --features core-nodes,crypto-ring,dhat-heap
```

The `bench-heap` feature installs DHAT's global allocator in the
`measurement` bench executable only, so a timed sample is never measured
through an allocation tracker; the two builds carry different fingerprints
and are never compared with each other. The `dhat-heap` engine is the
paired allocation profile of the pipeline baseline, and its run directory
keeps the `dhat-heap.json` it writes. `stages` takes
`--option configs='["logs-1k-stable"]'`, `--option stages='["extract"]'` and
`--option repetitions=3`.

A filtered `stages` run is a spot family. It must publish under an index of
its own, `--option index_name=stages-spot`, so it can never replace
`stages.json`, the family of record, and its coverage checks cover the
stages and workloads it asked for.

`attribution` profiles the real engine with perf and reconciles its CPU
shares with the stage families:

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 taskset -c 0-7,16-23 \
  python3 -m crates.validation.tests.series_parquet.measure attribution \
  --output-dir /tmp/series-attribution
```

It needs Docker with the MinIO image, a `perf` that may attach to this
user's processes (`kernel.perf_event_paranoid` of 2 or lower, or
`CAP_PERFMON`), `c++filt` from binutils for Rust's v0 symbols, and a
profilable release engine, `target/release/df_engine-perf`
(`SERIES_ATTRIBUTION_ENGINE` names another). The workspace's default linker,
lld, places the executable segment 4 KiB above its file offset, and perf's
libdw unwinder, which derives the module base from the file offset, then
ends every stack after its first frame. Relinking only the binary crate
with page-aligned segments changes no generated code:

```bash
cargo rustc --release --locked -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer -- \
  -C link-arg=-Wl,-z,separate-loadable-segments
cp target/release/df_engine target/release/df_engine-perf
cargo build --release --locked -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer   # restores df_engine
```

A preflight checks that engine's segment layout and records a busy process
with the exact command line a measured run uses; when perf cannot attach, no
repetition runs, `attribution.json` is published with status `skipped`, a
failed `perf_attached` check and the preflight's evidence, the mandatory
acceptance stays incomplete, and the command exits 3.

Each of the stage family's two primary workloads is prebuilt once, before
any lease, lengthened until three repetitions give at least 10,000
classified samples at 199 Hz. Each repetition runs an unprofiled control
lifetime and a profiled one on the same cores and store: blocks rotate
every 128 requests with 256 in flight. The profiled lifetime records
`perf record -e cpu-clock -F 199 -g --call-graph dwarf -p ENGINE_PID`, with
the events enabled through control FIFOs across exactly the input phase,
first request to last durable acknowledgement. Every sample is assigned once
by its innermost production frame (`performance.classify_cpu`; the rules
are recorded in the index). The flush wall time that is not flush-task CPU
is reported as `upload_wait_s`, outside the CPU shares. `--option
rehearsal=true` runs the control lifetimes alone into the output directory
and publishes nothing; `--option records=...`, `cpu_ns_per_record=...`,
`repetitions=...` and `configs=[...]` adjust the family, and `--option
lease_wait_s=...` lets each repetition wait that long for the host lease
another measurement holds, instead of being refused.

`fault-preflight` takes `--option stores=["minio"]` to probe one store and
`--option lease_wait_s=...` (default four hours) to wait for the host lease.
It exits 0 when every probe passed, 1 when a required probe failed and 3 when
optional fault tools were missing or failed a probe, after cleanup.

The remaining subcommands (`capacity`, `memory`, `soak`, `failures`,
`buffered`, `remediate`, `report`) are named here so the command line is one
contract; each is implemented by its own task.

## Environment variables

| Variable | Effect |
| --- | --- |
| `DF_ENGINE` | The engine binary to run. The fixture suite defaults to `target/debug/df_engine`; measured cases default to `target/release/df_engine` and refuse any non-release profile. |
| `SERIES_REQUIRE_DOCKER` | `1` makes missing images and Docker a failure rather than a skip. |
| `SERIES_REQUIRE_FAULT_TOOLS` | `1` makes missing fault tooling, or any failed fault-tool probe (UDP/TCP DNS, xt_bpf, capture, capabilities, S3 route), a failure rather than a skip. |
| `SERIES_FAULT_TOOLS_IMAGE`, `SERIES_TOXIPROXY_IMAGE` | The fault-tools and Toxiproxy images. Default `series-measure-fault-tools:local` and `ghcr.io/shopify/toxiproxy:2.12.0`. |
| `SERIES_FAULT_LEASE_WAIT_S` | How long the live rig test waits for the host lease. Default 3600. |
| `SERIES_MEASURE_LONG` | `1` opts in to throughput sweeps, profiled memory runs, the soak and long failure runs. |
| `SERIES_MEASURE_LEASE` | The exclusive host measurement lease file. Defaults to `/tmp/series-parquet-host-measurement.lock`, shared by every checkout and launcher on the host. |
| `SERIES_ENGINE_FEATURES`, `SERIES_ENGINE_ALLOCATOR` | The feature set and allocator the engine was built with, recorded in the build fingerprint. Default `default,series-parquet,aws,durable-buffer` and `jemalloc`. |
| `SERIES_ARTIFACT_DIR` | Where measurement tests retain their logs, results and ledgers. |
| `SERIES_PERF` | The perf executable an attribution records with. Defaults to `perf` on the PATH. |
| `SERIES_ATTRIBUTION_ENGINE` | The engine an attribution profiles. Defaults to `target/release/df_engine-perf`. |
| `SERIES_MINIO_IMAGE`, `SERIES_RUSTFS_IMAGE`, `SERIES_CLICKHOUSE_IMAGE`, `SERIES_ALLOY_IMAGE` | The container images the end-to-end lane uses. |

Python dependencies are pinned in `requirements.txt` and, with hashes, in
`requirements.lock.txt`; install with `pip install --require-hashes -r
requirements.lock.txt`.

## Fault tools

`faults.py` builds one disposable rig per run around an existing
`DockerStore`:

```text
engine --127.0.0.1:19000--> nginx --19001 general--> toxiproxy --> store
                                  \--19002 values--/
```

A private bridge network (unique name, labelled `series-fault-run=<id>`)
carries the store under the alias `store`. The fault-tools container is the
namespace owner: it runs NGINX with `fault-nginx.conf` and is the only
container started with `--cap-add=NET_ADMIN`. Toxiproxy and the engine join
its namespace with `--network container:OWNER`, so the proxies' loopback
listeners are real and every DNS, firewall and capture rule installed with
`docker exec` in the owner applies to the engine's own traffic. Nothing uses
`--privileged`, host networking, host firewall rules or module loading. The
owner publishes NGINX, the Toxiproxy API and the engine's gRPC and admin
ports on host loopback only. Every container starts from the inspected image
id, never the mutable tag. The engine must be a release build (the harness's
own profile rule; anything else is refused before launch) and its hash is
recorded; it runs as the invoking user with the binary and the repository
mounted read-only and its run and buffer directories read-write, after `ldd`
inside the image proved its shared libraries resolve; `docker inspect` gives
its host PID. Tool containers and the engine inherit the harness's CPU
affinity through `--cpuset-cpus`, so a harness under `taskset` confines them
too. A container is recorded only once Docker wrote its id to a cidfile, and
is removed by that id. Teardown runs independent stages -- retry of any
fault whose recovery failed, namespace rules, proxies, control file,
artifacts, containers, store attachment, network, leftover check -- each of
which runs whatever an earlier one raised, an interrupt included (re-raised
at the end). Artifacts (access log, captures, dnsmasq log) stay under the
rig's root.

Registered faults: `slow` adds the upstream `bandwidth` (rate 256 KB/s) and
downstream `latency` (1500 ms) toxics to both proxies; `http503` creates the
control file NGINX answers 503 for; `store_outage` stops the store container
and recovers the same container, repointing both proxies if its address
changed. Any activation or recovery error after preflight is a failure, and
a fault whose recovery failed stays active so the teardown retries it.

Provision once, outside any measurement lease, from `rust/otap-dataflow`:

```bash
docker pull ghcr.io/shopify/toxiproxy:2.12.0
docker pull ubuntu:24.04
docker build -t series-measure-fault-tools:local \
  --build-arg BASE=ubuntu@sha256:<digest docker pull printed> \
  -f crates/validation/tests/series_parquet/fault-tools.Dockerfile \
  crates/validation/tests/series_parquet
```

Nothing in the harness pulls or builds these images. `fault-preflight`
records their ids, repository digests and the tools image's package
versions, then, under the host lease, per store: signed PUT/HEAD/GET/DELETE
and a multipart completion through each backend, route isolation (disabling
one proxy breaks exactly its keys), the containerized release engine
exporting through both backends, the capability split read from each
container's configuration and `/proc/PID/status`, UDP and TCP DNS blocking
(exact port-53 DROP rule, bounded `dig` timeout, positive counter, exact
deletion, resolution restored), the `xt_bpf` ACK-only drop (bytecode from
`tcpdump -ddd -y RAW`; a signed PUT through the route stalls under the rule,
and the probe requires dropped ACKs, a non-empty capture and at least one
retransmission, then after the exact deletion a signed PUT with a 2xx
status and its bytes read back), a signed transfer captured and read back
with tshark, the three activations, and the restored state. Every command's
argv, exit status, output and duration is kept in `fault-preflight.json`,
with the fault-class coverage those probes decide, per store: a class is
available for a store only when that store's own probes passed, and
available overall only when it is available for every store (the stores
themselves start from their inspected image id, `DockerStore(kind,
by_image_id=True)`; the legacy suite keeps the tag). `disconnect_reset` and
`dropped_completion_response` stay unavailable until Task 11 adds their
direct probes with negative controls; the coverage names what each must
show.
A failed `xt_bpf` probe names the host fix (`sudo modprobe xt_bpf`); the
harness never runs it.

## What a measurement is allowed to claim

These rules are enforced in code, not by convention. `measurement.py` holds
each of them once.

- **Delivery.** The producer's ledger is the oracle. Every record carries a
  fixed-width stable id and a canonical payload hash, and the comparison is a
  SQL join rather than a Python set. Missing, unexpected, corrupt and
  duplicated records are counted separately, so a duplicate can never mask a
  loss in a row total. A healthy no-retry run requires multiplicity exactly
  one; a fault run allows duplicates and counts every one.
- **Collection epochs.** Only an advance of a worker's collection-updated
  `pipeline.uptime` counts as a new observation. Three HTTP responses and the
  scrape's own timestamp prove nothing. An uptime that decreases within one
  deployment generation is an error; a new generation is a restart and is
  counted as one.
- **Drain.** Three consecutive advances of every worker's uptime, each with
  the exporter holding no block, no pending request and no queued
  notification, and in the buffered topology with nothing queued or in
  flight. A nonempty observation resets that worker's streak.
- **Gauges.** A required gauge that is absent, non-numeric or published by
  more than one entity of a worker is an error, never a zero.
- **Liveness.** Presence is not enough. A snapshot the exporter did not
  answer carries its metric names with every value zero, which looks exactly
  like a drained worker. `memory.budget` is computed from
  configuration constants and is never zero while the worker is alive, so it
  is the marker: a worker whose budget reads zero is not an observation. Its
  epoch resets the empty streak, because an unobserved epoch may have been
  busy, and the drain report records how many such epochs it saw.
- **Environment.** Every run records a start and an end snapshot with the CPU
  model, logical and physical core counts, SMT sibling groups, the cores the
  run may use, total RAM, kernel, load averages and the observed per-thread
  affinity from `/proc/PID/task/*/status`. Any difference between the two
  snapshots in machine or core configuration invalidates the run. A
  requested-versus-observed affinity mismatch or an ambiguous worker mapping
  aborts it.
- **Host lease.** One exclusive `flock` on
  `/tmp/series-parquet-host-measurement.lock` covers the whole run, from
  before the engine starts until after the end snapshot. A busy lease aborts
  the run before any traffic, and the lock file records the holder's PID,
  process start time and run id.
- **Placement.** Roles own whole physical cores: the engine's observability
  pipeline takes the first core the engine may use, the workers take theirs
  through a `core_set`, the rest of the engine's four-core reservation is
  kept free, and the producer (two cores), store and reader (one each)
  follow. No role is given an SMT sibling of another role's core. A
  publishable run needs eight available physical cores.
- **Monitor.** `host_monitor.py` runs as its own process, so a busy harness
  cannot stretch its schedule. Every 50 ms it scans procfs for compilers,
  linkers and container build clients, and for any process descending from
  a build daemon; an idle `buildkitd` is not a build. On each tick it also
  enumerates every thread of the engine and requires the threads carrying a
  worker name to be exactly the mapped worker TIDs, each allowed exactly its
  own core, so an extra or replacing worker fails even for a single tick. A
  tick gap over 100 ms (recorded as an observation only in `publish=false`
  mode), a procfs that hides other processes, or a Docker that is installed
  but cannot be asked about builder containers makes the coverage incomplete
  and the run invalid. Any build seen after preflight invalidates the run even
  if it has gone by the end; the run stops itself, keeps the evidence and
  never stops anybody else's process.
- **Topologies and launchers.** `Engine` takes keyword-only `topology`
  (`strict` or `buffered`), `buffer_path`, `cores`, `launcher` and `merge`;
  every old call is unchanged. The buffered topology inserts one
  `processor:durable_buffer` with `size_cap_policy: backpressure` and
  `max_age: null`; every connection must have exactly one recipient and no
  dispatch policy. A launcher provides `start(argv, log, env)` and
  `pid(process)`, and every RSS, affinity and kill operation uses the host
  PID it reports. A restart reuses the same cores and the same buffer
  directory, and the run checks both.
- **RSS reconciliation.** Every term is measured: the resident set at
  readiness, the growth of file-backed resident pages, and the high-water
  mark of the workers' heap as the engine's allocation tracking counts it,
  which includes the exporter's accounted bytes. The signed remainder of the
  anonymous growth is the residual; above `max(32MiB, 0.10 * peak RSS)` in
  any sample, or persistently below its negative, it fails the run.
- **Units.** Every metric name ends in its unit (`_bytes`, `_s`, `_ns`,
  `_records_per_s`, `_cpu_ns_per_record`, ...). An unavailable value is null
  with a reason and never zero, and a passed run cannot be missing a
  mandatory metric.
- **No fixed sleeps.** Every wait is a poll under a monotonic deadline that
  returns an observation. Elapsed time alone never proves readiness,
  drainage or fault activation.

## Acceptance policy

The first valid run, with its required repetitions, writes and commits a
baseline fingerprinted by machine, core allocation, effective configuration,
workload and build profile. A later run with the same fingerprint fails on a
regression worse than 25 percent. A different fingerprint writes a new
baseline rather than comparing against one that does not describe it. The
original matching baseline is immutable, so small regressions cannot ratchet
it upward. The source revision and binary hash are recorded as provenance
outside the fingerprint, so a new implementation is compared rather than
excused.

Correctness, measurement validity and an unexplained accounted-versus-RSS
residual are hard checks on every run, whatever the baseline says. A run that
fails one of them can never establish a baseline. The single numerical
boundary lives in `measurement.REGRESSION_LIMIT` and nowhere else.

## Evidence files

One compact JSON document per run, named
`{case}-{topology}-{store}-c{cores}-w{interval}-r{ordinal}.json`. A family
summary index lists its children in `run_files`, `baseline_files` and
`child_indexes` with their exact names, sizes and SHA-256 hashes; it never
replaces the per-run files. Committed evidence lives in
`docs/superpowers/reports/series-parquet-measurement`. Published run and
baseline file names are immutable: a re-execution uses a new artifact
directory rather than overwriting evidence. Raw profiles, Parquet files,
packet captures, logs and ledgers stay outside tracked source and are
referenced by path, hash, size and retention location.

`stage-results` reads one published index, enumerates the tree recursively,
rejects a path that is not a plain JSON file name in the report directory,
rejects cycles, verifies every recorded hash, and hands the files to
`git add` by name in bounded batches. Nothing is ever staged by glob or by
directory.

## Reproducing a measurement

1. Build the release engine with the features the case needs:
   `cargo build --release --locked -p otel-arrow-dfe --bin df_engine
   --features series-parquet,aws,durable-buffer`.
   The fixture suite additionally needs the same command without `--release`.
2. Read the run's `environment` block. Match the machine, the core
   allocation and the build profile, or expect a new baseline rather than a
   comparison.
3. Run the case with its recorded `config.requested` and `workload`.
4. Compare the produced JSON against the committed baseline named in
   `baseline_decision`.
