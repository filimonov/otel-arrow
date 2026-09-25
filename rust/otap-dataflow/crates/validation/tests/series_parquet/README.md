# Series Parquet measurement harness

Two lanes live in this directory. `test_e2e.py` is the standing end-to-end
suite: a real `df_engine` process, real object stores in containers, Grafana
Alloy, and two independent readers. `measurement.py`, `measure.py` and
`test_measurement.py` are the measurement lane built on top of it. The
measurement lane extends the end-to-end helpers; it never replaces them.

## Commands

Every command runs from `rust/otap-dataflow`.

```bash
# The standing suite: 19 tests, no skips when Docker is required.
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

Stage timings run on jemalloc like the engine: the timing builds of both
benches install it with the engine's compiled-in background thread, and
their `--describe` names the allocator. The harness refuses a timing bench
on any other allocator and records the described one in every child's
fingerprint. Stage families published before `stages-spot-jemalloc` timed
on the system allocator (glibc malloc), so their baselines are never
compared with a jemalloc run. The `bench-heap` feature installs DHAT's
global allocator instead, so a timed sample is never measured through an
allocation tracker; the two builds carry different fingerprints and are
never compared with each other. The `dhat-heap` engine is the
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
`CAP_PERFMON`), `c++filt` from binutils for Rust's v0 symbols, and a clean
Rust tree. The workspace's default linker, lld, places the executable
segment 4 KiB above its file offset, and perf's libdw unwinder, which
derives the module base from the file offset, then ends every stack after
its first frame. So the family builds its own profiled engine,
`target/release/df_engine-perf`, before any measured window and holding the
host lease so no one else's measurement overlaps the compile:

```bash
cargo build --release --locked -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer          # the canonical engine
cargo rustc --release --locked -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer -- \
  -C link-arg=-Wl,-z,separate-loadable-segments          # relink only
cp target/release/df_engine target/release/df_engine-perf
cargo build --release --locked -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer          # restores df_engine
```

It then proves the two binaries one engine. Both record their revision,
profile, features, allocator, `rustc -vV`, hash, segment layout and the
exact flag difference. Their sorted `(size, demangled name)` function
symbols from `nm --defined-only --size-sort -C` must be identical, and the
profiled layout must be one perf can unwind. Any mismatch refuses the family
with the reason. The perf preflight then records a busy process with the
exact command line a measured run uses. When the engine is not proven or
perf cannot attach, no repetition runs, `attribution.json` is published with
status `skipped`, a failed `perf_attached` check and the preflight's
evidence, the mandatory acceptance stays incomplete, and the command exits
3. Every index and repetition records the `perf_event_paranoid` it ran
under, since the setting does not survive a reboot.

Each of the stage family's two primary workloads is prebuilt once, before
any lease, and lengthened from the pinned family's costs until three
repetitions give at least 10,000 classified samples at 199 Hz. Each
repetition runs an unprofiled control lifetime and a profiled one on the
same cores and store: blocks rotate every 128 requests with 256 in flight.
The profiled lifetime records `perf record -e cpu-clock -F 199 -g
--call-graph dwarf --sample-cpu -p ENGINE_PID`, with the events enabled
through control FIFOs across exactly the input phase, first request to last
durable acknowledgement. Every sample is assigned once by its innermost
production frame (`performance.classify_cpu`; the rules are recorded in the
index). The flush wall time that is not flush-task CPU is reported as
`upload_wait_s`, outside the CPU shares.

The binding reconciliation is the plan's Stage agreement rule, a hard gate
in every repetition and aggregate. Exclusive category CPU plus the named
residual is compared with the engine CPU the scheduler measured; an error
above 10 percent, or unexplained CPU (residual plus uncovered CPU) above 20
percent, invalidates the attribution. An aggregate also requires the pinned
stage family to verify by hash. A baseline is written only after every one of
these gates has passed. The per-stage comparison of attributed costs with
isolated bench costs is descriptive and gates nothing. Its reference is the
pinned family, with the spot family beside it as supplementary evidence and
never substituted. A row outside the 0.5 to 2 band is explained only when the
published spot family brings it into the band, and is otherwise marked
unexplained.

`--option rehearsal=true` runs the control lifetimes alone into the output
directory and publishes nothing; `--option records=...`,
`cpu_ns_per_record=...`, `repetitions=...` and `configs=[...]` adjust the
family. Each lifetime's ledger is written on a memory file system, checked
in `/proc/self/mountinfo` and recorded: `/tmp` by default, `--option
ledger_dir=...` or `SERIES_ATTRIBUTION_LEDGER_DIR` name another, and a
directory that is not tmpfs falls back to `/dev/shm`, or the family is
refused. On a disk the ledger's per-request fsyncs throttle the sender to the
disk's commit rate. It is deleted after the read-back and its hash is kept.
`--option lease_wait_s=...` lets the engine build and each repetition wait
that long for the host lease another measurement holds, instead of being
refused. A repetition whose measured window saw a compiler -- another
agent's build is not this family's to stop -- is kept in the index as
`invalidated_children`, never aggregated, and run again under a new ordinal
once the host has been build-free for a minute, at most three times.

`fault-preflight` takes `--option stores=["minio"]` to probe one store and
`--option lease_wait_s=...` (default four hours) to wait for the host lease.
It exits 0 when every probe passed, 1 when a required probe failed and 3 when
optional fault tools were missing or failed a probe, after cleanup.

`memory` (`SERIES_MEASURE_LONG=1`) measures the real engine's memory in
pairs: each pair launches a control engine whose exporter is the noop
exporter and then the measured strict or buffered engine, fresh, on the same
cores, under the same offered workload (prebuilt harness-local requests at 50
requests per second on one-second windows), each warmed separately. A 100 ms
sampler reads the telemetry (at a 100 ms reporting interval), the process's
mapping list and the allocator totals that jemalloc's `stats_interval` option
prints into the engine log, and splits RSS into non-heap mappings, allocator
retention, jemalloc allocated and the tracked heap; the exporter's accounted
bytes are reconciled against the live heap and the control's heap with the
frozen residual tolerance. Three pairs per topology form a family whose
primary metrics must spread by at most 15 percent before the baseline policy
applies. The strict family also runs a `decay0` and a `prof` diagnostic pair
and the accounting probes: the merge-key and values-capacity terms through the
measurement bench's fixtures, and the per-series row cost through
`measurement --series-cost` in the DHAT build. It needs the release engine
and both measurement benches, and it waits for a busy host lease instead of
failing. Options: `topologies`, `pairs`, `diagnostics`, `probes`,
`requests`, `rate_requests_per_s`, `report_dir` and `evidence` (a map of
names to earlier JSON evidence carried into the strict index).

```bash
SERIES_MEASURE_LONG=1 python3 -m crates.validation.tests.series_parquet.measure \
  memory --output-dir /tmp/series-memory
```

### Capacity

`capacity` (`SERIES_MEASURE_LONG=1`, `capacity.py`) searches the highest
offered record rate the strict engine sustains, per store and worker count,
and measures its durable write speed:

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 taskset -c 0-7,16-23 \
  python3 -m crates.validation.tests.series_parquet.measure capacity \
  --output-dir /var/tmp/series-capacity \
  --option 'stores=["local","minio","rustfs"]' --option 'core_counts=[1,4]' \
  --option 'steps=["calibrate","search","search_raised","stats_off","search_default_window","default_window","buffered","fan_in","workloads","high_cardinality","publish"]'
```

The output directory holds Parquet and logs of tens of gigabytes per trial,
so it belongs on a disk, not on the tmpfs `/tmp`. Each trial is one fresh
release engine and one JSON file, named like every run file; the family
state (`capacity-state.json`) lets a later invocation resume or add steps.
`publish` writes `capacity-local.json`, `capacity-minio.json` and
`capacity-rustfs.json`, each with its trials, each cell's aggregate and its
baseline.

A trial offers a fixed rate from an open-loop producer: one spawned process
per physical core of `--option producer_cpus` (default `8-15,24-31`, the
host's CPUs outside the campaign pin, so eight processes; `allocated` keeps
the two physical cores of the role allocation instead), every thread of
each confined to its core's two SMT threads (the store owns its core
likewise), each owning a share of the client connections
(`grpc.use_local_subchannel_pool`, so each channel is its own TCP
connection) and sending prebuilt requests at monotonic target times, with
in-flight requests bounded by the receivers' summed capacity. A send is
never retried and never silently re-timed: its target, send and response
instants are all kept, and more than 1 percent of sends starting over 50
ms late with a slot free marks the trial `producer_limited`, unless the
engine failed anywhere: a refused or failed request, a partially rejected
one, or, in the buffered topology, a growing write-ahead log, an ingest
failure or a permanently rejected bundle makes it unsustainable, with the
lateness kept as a reason (`capacity.judge_trial`, which also re-judges a
stored trial through `rejudge_stored`). A producer-limited
rate bounds the bisection from above, and a search bounded there reports
`lower_bound_producer_limited`, never a maximum. Each sender reports the
CPU it spent generating requests and its major page faults.

Requests come from `generator.TemplateRequests`: per signal, 97 template
requests whose per-request fields -- each record's timestamp, the request
digits of each log record id and each series slot -- have fixed widths and
known offsets, so a request is its template's fixed parts joined with its
own fields, about 0.7 ms for a 1 MiB logs request. A record's sequence
number, `request * records_per_request + position`, is its timestamp on the
harness time base; padding and point values come from template `request %
97`. No per-record ledger is kept. `generator.aggregate_oracle` reads the
stored values with DuckDB and, per signal and per acknowledged request,
requires every sequence number exactly once, no stored request that was
never sent, the count and sequence sum of the acknowledged requests, a
random sample of records equal field by field to the generator's (body and
logger, or kind, values and slot through the latest descriptor), each
file's recorded row count, sort order and descriptor hashes, descriptor
coverage per partition and writer, and clickhouse-local's count and
sequence sum equal to DuckDB's. A failed request may be stored (it is
counted apart); a lost, duplicated, foreign or corrupted record fails
delivery.

A trial is a 15 s warm-up (at least two windows plus 5 s) and a 60 s
measured interval, then the drain proof, the read-back by both readers and
deletion of the objects. It is sustainable when, over the interval's last
30 s, the durable acknowledgement rate and the values rows the exporter
reports written are each at least 98 percent of the offered rate, the
backlog (due minus durably acknowledged) grows by at most 2 percent of it,
and no request failed. A search starts at 1,000 records/s, doubles while
sustainable (at most 12 trials; an unbracketed search is a lower bound),
bisects until the bracket is at most 10 percent wide and repeats the
winning rate until three independent trials measured it; its aggregate is
the median durable rate with a 15 percent coefficient-of-variation gate.

Deliberate overrides, recorded in every trial: one-second windows for the
search (the shipped window is 15 s, searched on its own by
`search_default_window`; jemalloc's statistics print);
1000 records per request; 256 connections. A strict request holds its
receiver slot until its block is durable, so with the shipped 128 slots per
worker the search can end at the receiver's admission limit rather than the
exporter's; `search_raised` searches again with 4096 slots, starting from
the shipped bracket (its sustainable rate is taken as sustainable, its first
trial is the shipped unsustainable rate); `buffered`, `fan_in` and
`workloads` then run at the raised ceiling with the raised slots.
`default_window` compares upload concurrency 2 and 1 at the default-window
search's winner (never at the one-second winner, which a 15 s window cannot
admit), and runs concurrency 1 once at the cell's one-second ceiling, where
the search ran 2. The
receiver's `max_concurrent_requests` is clamped by the engine to the
pipeline's pdata channel capacity, so a raised limit raises both. The
harness engine configuration drops unsupported points (`unsupported:
drop`); the object stores are plain HTTP, where series_parquet signs every
payload (unsigned payloads are its default over TLS only).

The strict ceiling per worker at a given configuration is at most
`capacity.admission_ceiling`: receiver slots x records per request / the
time a request is held, a window plus the flush that makes its block
durable. `search_default_window` searches the configuration as it ships (15
s windows, 128 slots), where that bound is far below the exporter's; the
remedies are more slots, larger requests, a shorter window or the buffered
topology, whose acknowledgement does not wait for the object store.

`search_buffered` searches the buffered topology (the durable buffer in
front of the exporter) at the shipped 128 slots. Its acknowledgement comes
once a request is in the buffer's write-ahead log, so beside the rules
above a buffered trial is sustainable only when the log grows over the
measured interval by less than one window of the offered bytes, the buffer
reports no ingest failure and no bundle permanently rejected downstream,
and the values rows written keep up with the offered rate. Each trial
reports the log's size (start, maximum, end, slope), the buffer's in-flight
and queued items, and, like every trial, the lag from a request's
acknowledgement to the completion of the last object holding its records.

A `trial={...}` may also set `wal_dir` (where the buffer's write-ahead log
lives), `local_store_dir` (where the local store writes) and
`buffer_config` (merged into the durable buffer's configuration), for
diagnostic trials that isolate a device; every trial records the writes,
bytes written and mean write and flush latency of the host's block
devices over its measured interval.

`--option search_floor={"cell": {"variant": rate}}` starts a cell's search
at a floor derived from measured neighbours instead of 1,000 records/s: half
of the smaller of the same store's one-worker ceiling times the workers and
the shipped-slot formula ceiling, rounded down to the doubling grid. An
unsustainable floor halves until a rate passes; the index records each
floor.

A capacity trial's RSS reconciliation reads the allocator, not the
pipelines' `memory.usage`, which credits a free only to the allocating
thread and so grows with every byte the local store writes from its
blocking pool. The engine prints jemalloc's statistics every 64 MiB of
allocation (`MALLOC_CONF`, recorded in each trial and index); a thread
reads each print within 5 ms and pairs it with the smaps rollup read at
once, and the anonymous growth must lie between the allocator's live heap
and the most it held resident over the interval the kernel may still be
releasing, within the frozen tolerance (`measurement.allocator_band_residuals`).
The band's upper edge is the larger `resident` of the paired print and of
the previous paired print (a print read in the same poll but not paired is
recorded as `interval_resident_max_bytes` and never widens the band): after
a large purge jemalloc
stops counting an extent as resident before the kernel has released its
pages, so an RSS read milliseconds after the print can still hold them
(soak-strict r002 read 122-234 MB above resident five times, each back inside
the band at the next print 11-49 ms later). The lower edge and the tolerance
are unchanged, and growth above both prints' resident still fails. Every
residual beyond half the tolerance is kept with the pairs around it
(`residual_excursions`), and `measure rejudge-band --index NAME --output-dir
DIR` advances a published index to the current rule from the stored runs,
recording every verdict that changes (`rss_band_rejudgement`) and the rule
(`rss_band_rule`). Each trial also reports the
exporter's `memory.accounted` against jemalloc's `allocated`, at its
highest fill and as a slope per values row written, which a heap leak
inside the exporter would show. `stats_off` repeats the shipped winning
rate without the prints.

A send that waits for an in-flight slot delays the sends behind it; those
are counted as `late_behind_in_flight_wait`, the engine's lateness, and
only a late send with a free slot and no such wait before it counts
against the producer.

Workloads (`capacity.CAPACITY_WORKLOADS`): `mixed-1k-hot` is the searched
one, 80/20 logs/metric points with 1 KiB bodies and series slots cycled per
record (`Workload.series_scope = "record"`) over 10k slots; `logs-1k-hot`,
`mixed-8k-hot` and `mixed-1k-churn` (new series with every request) are
confirmations at 80 percent of the searched capacity, bracketed on their own
when they fail there; `metrics-1k-unique` gives every point a unique
attribute value against `metrics-1k-hot`, for the series-to-point ratio.
A thousand 8 KiB log records make an 8 MB request, twice the receiver's
default 4 MiB `max_decoding_message_size`, which refuses it with
`OUT_OF_RANGE`; `mixed-8k-hot` therefore runs with a 16 MiB limit, the one
setting it changes, named in its workload entry.

Series identity is the full attribute set, by OTel semantics, so an
attribute that is unique per point (a request id) makes every point a new
series and the descriptor dataset grows like the values dataset. The
exporter cannot drop such an attribute without merging distinct streams;
filter or aggregate it upstream (an OTel View, or `processor:attribute`).

The `alloy` step (`alloy_capacity.py`) asks whether a real producer sees the
same engine as the generator. Grafana Alloy runs the reference River config
unchanged (its batching, eight consumers and the 180s attempt timeout the
config falls back to) on six producer cores and tails one file into the
strict shipped engine (128 slots, one-second window) on the cell's workers.
A feeder process on the last two producer cores appends fixed-width
100-byte lines, `alloy <12-digit seq> x...`, at 80 percent of the cell's
strict shipped ceiling (`alloy_rate=` overrides), never more than one
million lines ahead of what Alloy has read, so a producer slower than the
offered rate is measured at its own maximum (`limited_by_alloy`). Alloy
exports to a recording tap in the same process, which forwards each request
unchanged to the engine over one upstream connection per Alloy connection
and returns the engine's response or status as it came; the tap is what
measures the request records and bytes, the connections and the engine's
response time. Alloy's own queue-batch histograms cannot: they record the
one-record requests the Loki bridge enqueues before the batcher merges them.
The oracle is the end-to-end suite's read-back, not the sequence oracle:
rows per source file and `e2e.source` from DuckDB and ClickHouse, the
latest-descriptor join, and every written sequence number present, with
a duplicated line failing it like a missing one. `alloy_variants` names
the trials: `shipped` keeps the receiver's default 4 MiB decoding limit and
`decoding_16mib` raises it, the one setting the second trial changes.

Options: `stores`, `core_counts`, `steps`, `rehearsal=true` with its own
`report_dir` (six-second intervals, nothing published to the report
directory), `trial={...}` for the `trial` step, `fan_in`, `rows`,
`high_cardinality_rate`, `upload_concurrencies`, `alloy_rate`, `alloy_variants`,
`producer_cpus`,
`producer_processes`, `archive_dir`, `lease_wait_s`, `family_ordinal`, and
`unmeasurable_above={"cell": [rate]}`: a rate whose trials the host could
not measure validly (the overload starved the build monitor) steers the
bisection from above but never becomes the unsustainable bound, so a
search bounded only by it reports a lower bound.

Each store's index also carries `capacity.rejudgement`: every trial judged
again from what it stored by the current rules (`capacity.judge_trial` for
the verdict, the generator read-back's aggregate equalities, and the Alloy
read-back's `judge_read_back`), with the verdicts, read-backs and search
decisions that change, and the trials whose figures do not allow it. An index
that replaces a published one lists the old one in `child_indexes`.

### Soak

`soak` (`SERIES_MEASURE_LONG=1`, `soak.py`) holds one worker at 70 percent
of a measured ceiling for thirty minutes of input, in each topology, on
MinIO, and runs the two PR-tier soaks:

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 taskset -c 0-7,16-23 \
  python3 -m crates.validation.tests.series_parquet.measure soak \
  --output-dir /var/tmp/series-soak
```

Its steps (`--option 'steps=[...]'`, all by default, in this order) are
`strict`, `bracket`, `buffered`, `pr_strict`, `pr_buffered`, `alloy` and
`publish`; `soak-state.json` in the output directory records the run each
step produced, so a later invocation can run the remaining steps. `publish`
writes `soak-strict.json` (the strict soak, its PR-tier soak and the Alloy
trial) and `soak-buffered.json` (the buffered soak, its PR-tier soak and the
bracket's trials). Each soak is its own family: its first valid run writes
its baseline. The objects of a soak are tens to hundreds of gigabytes, so
the output directory belongs on a disk.

The conditions:

- `soak-strict`: 70 percent of the MinIO one-worker raised-slot ceiling, read
  from the committed `capacity-minio.json`; 4096 receiver slots and the
  shipped 15 s window, so blocks rotate on `max_block_bytes` and the
  exporter's retained memory reaches its budget.
- `soak-buffered`: the shipped 128 slots and 15 s window, the durable
  buffer's write-ahead log in the run's engine directory on the host disk,
  at 70 percent of the one-worker buffered rate that `bracket` searches with
  the same configuration (the "Capacity" search rules, from 64k records/s;
  `--option buffered_rate=...` overrides the fraction, `bracket_floor=...`
  the start).
- Both: workload `mixed-1k-hot-churn1`, which is `mixed-1k-hot` with every
  hundredth record on a series no other record uses (`Workload.churn_every`),
  1000-record requests, 256 connections, the capacity producer placement,
  the template generator and aggregate oracle, and the allocator-band RSS
  reconciliation.
- The producer schedules a warm-up cohort (two windows plus 5 s) and a
  measured cohort of `ceil(rate * 1800 / 1000)` requests.
  `metrics.input_phase_s` is the measured cohort's active time, first send
  to last send plus one request interval, and must be at least 1800 s; an
  early end fails it rather than counting idle time. `--option input_s=...`
  shortens a rehearsal.

The result's `samples` hold one row per second, second 0 the first of the
input phase:

- from the producer's sends: records attempted and acknowledged, wire bytes,
  requests and bytes in flight (sent and unanswered, which is what the
  receiver holds), and the age of the oldest unanswered request;
- from the read-back: records committed, a request counting in the second
  the last object holding its records completed, and object bytes by
  completion;
- from the last telemetry sample of the second: values rows written (the
  provisional physical-row rate), ACTIVE, FLUSHING and pending bytes,
  pending requests, queued notifications, the exporter's oldest unacked
  age, series cache entries, accounted bytes and budget, flush workspace,
  admission closed, RSS, anonymous bytes, descriptors, threads, jemalloc's
  allocated and resident bytes and, buffered, the write-ahead log's bytes,
  queued items, bundles in flight and scheduled retries;
- from the soak's own reads each second: store and producer RSS and,
  buffered, the log's size on disk.

A second without a sample every worker answered is `observed: false`; the
sample coverage is the share of input seconds observed. While input runs
the store is listed every 10 s, which only shows completed objects, and the
listing lag is recorded; nothing is read back until the engine has drained
and stopped. Then each object is downloaded and deleted from the store, so
the objects are held once, and the oracle reads them.

`soak` in the result holds the per-minute view, the drift of RSS,
descriptors, cache entries, accounted bytes, jemalloc's allocated bytes,
allocated minus accounted, store and producer RSS and the write-ahead log
(slope, first and last five-minute medians and their ratio), the backlog at
stop and the time and rate of its drain (acknowledgement and storage),
memory at the highest block fill beside the budget, the full-run and final
five-minute rates, and the capacity trial's own metrics. The checks add
`input_duration`, `sample_coverage` (at least 99 percent), `rate_sustained`
(the capacity stability rule over the last 30 s), `backlog_drained`,
`duplicate_free` and, buffered, `buffer_retention_lossless`.
`soak.soak_checks` requires every correctness counter to be zero -- a
strict soak's `buffer_loss_records` is an explicit zero tagged
`not_applicable: no_buffer`, and an absent counter is an instrumentation
error -- the sample coverage, and the Controller baseline policy. The RSS
slope is an observation, not a compared metric: near zero its relative
change is noise, while the compared last-to-first median ratio sits near
one.

The PR-tier soaks (`measure run --case pr-soak-strict`, `pr-soak-buffered`)
send 65 s of 100-record requests at 20 requests/s, every tenth a metrics
request, into one worker on MinIO with 1 s windows, `max_block_bytes: 640KiB`
and `max_requests_per_block: 6`, so blocks rotate both on bytes and on
requests, and `flush_retry_deadline: 3s` with the store's own retry below
it. The store is stopped once it holds a completed object and the ACTIVE
block is nonempty, and recovered once the exporter has reported a failed
flush and the deadline has passed since the stop. The strict producer
resends a retryably refused request with its original bytes; the run
requires a deadline flush failure, storage NACKs and retryable producer
attempts. The buffered run requires the buffer's scheduled retries. Both
require every intended record stored (the ledger oracle, duplicates
counted), both rotation reasons, the retained-state caps (accounted within
the budget, ACTIVE within `max_block_bytes`, the log within its cap) and
the RSS reconciliation; memory, the oldest unacked age and the correctness
counts are the compared metrics. `--option publish=false` is the CI mode of
`launcher-ci`, which may run the fixture suite's debug engine; the unit
tests always run it.

The `alloy` step runs the "Capacity" Alloy trial on the soak's store at
20,000 lines/s with the receiver's 16 MiB decoding limit.

### Failures

`failures --family s3` (`SERIES_MEASURE_LONG=1`, `faults.py`) runs every
S3 fault against both real stores in both topologies, one fresh rig,
store and containerized release engine per cell, and publishes
`failure-s3.json`:

```bash
SERIES_MEASURE_LONG=1 SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1 \
  taskset -c 0-7,16-23 python3 -m crates.validation.tests.series_parquet.measure \
  failures --family s3 --output-dir /var/tmp/series-failure-s3
```

A cell is `slow`, `http503` or `store_outage` (the registered faults, "Fault
tools") times `strict` or `buffered` times `minio` or `rustfs`. Docker, both
fault images and the store image are checked before the lease: a missing one
skips an optional lane and fails a required one. Anything after that, a fault
activation error included, is a failure.

The engine runs the shipped S3 store retry section and the default 60 s
`flush_retry_deadline`, with five-second windows and `upload.part_bytes:
5MiB`, so every logs values file (about 6.8 MB) is a multipart upload. The
producer is the ledgered one of the PR-tier soak: 20 requests a second of 100
one-KiB records, every tenth a metrics request, on CPUs `8-15,24-31`,
resending a retryable refusal with its original bytes until it is
acknowledged. Buffered, the write-ahead log acknowledges it and it never
resends after that.

Each cell is a state machine whose every state is recorded with its instant
and the evidence that entered it (`observations.fault.states`):

1. `baseline`: at least 15 s of input, a values object HEADed directly in
   the store, a nonempty ACTIVE block and an acknowledgement within two
   windows.
2. `armed`, then `observed` once the fault's intended condition holds:
   - `slow`: a response delayed at least 1.35 s and an upload throttled to
     at most twice the toxic's rate, both started under the fault, a
     nonempty FLUSHING block with ACTIVE or pending work behind it, and
     backpressure (admission closed for at least a window, a receiver
     refusal, or the buffer's in-flight bundles holding still while
     admission is closed);
   - `http503`: a PUT or POST NGINX answered 503, an exporter retry, a
     storage nack, and its retry (strict: the producer received the storage
     sentence; buffered: the buffer scheduled a retry);
   - `store_outage`: a `series_parquet.flush.failed` event of class
     `deadline` whose logged time is at least the flush deadline after the
     stop, a storage nack and its retry.
3. `fault_removed`, `endpoint_healthy` (a signed HEAD of the bucket through
   the route), `resumed` (a values file written and a request acknowledged
   after the removal), 20 s of acknowledged input, `input_stopped` and
   `drained` (the drain proof). Everything after `endpoint_healthy` must
   finish within 300 s.

The hard checks beside the common ones:

- `fault_observed`, `recovered` and `drained` for the states above;
- `at_least_once`: every sent request acknowledged and no sent or
  acknowledged record missing, unexpected or corrupt (the ledger oracle);
  duplicates are counted, and `duplicates_explained` requires every
  duplicated record to belong to a request the producer resent (strict) or
  to have a copy in a values file of a block whose flush failed (buffered:
  the buffer delivers that block's nacked requests again);
- `descriptor_coverage` and `reader_agreement` from the read-back;
- `bounded_resources`: ACTIVE and FLUSHING within `window.max_block_bytes`,
  the series cache within its capacity, at most one pending slot, accounted
  memory within the budget, the log within its cap with nothing lost or
  rejected, and the oldest unacknowledged age back at its baseline;
- `multipart_exercised`: CreateMultipartUpload, UploadPart and
  CompleteMultipartUpload answered 2xx in the route's trace and a stored
  object above one part with a multipart ETag;
- `orphaned_uploads_expected`: the bucket's incomplete multipart uploads,
  listed in the store directly after the engine stopped, number at most the
  `flush.abort_failures` the exporter reported;
- `partition_lateness_bound`: no object of a partition hour visible more
  than `window.interval + 2 * (flush_retry_deadline + upload.abort_timeout)`
  (135 s here) after the hour ended, by the store's LastModified or by the
  first direct listing (every second) that showed it;
- `fault_rig_clean`: nothing of the fault left and the rig removed.

The compared metrics are memory (peak RSS, accounted peak, buffered log
peak) and the correctness counts; every other number depends on where in a
window the fault landed and is recorded under `observations.fault.numbers`:
offered, acknowledged and stored records, 503 responses, fault, recovery
and drain durations, flush retries and failures by class, abort failures and
late commits, nacks by class, the producer's storage nacks and local
timeouts, the buffer's retries, orphaned and multipart uploads and the
latest visibility after an hour's end. The route's requests, statuses,
latencies and upload bandwidth before and during the fault, the exporter's
flush events, the stored objects of every failed block and throughput before,
during and after the fault are kept beside them.

Options: `only_cells=[...]` (names like `http503-strict-minio`), `faults`,
`topologies`, `stores`, `straddle_cells=[...]` (those cells arm their fault
10 s before an hour ends, so the hour's last blocks are written under it;
they run last, wait for the hour without the lease, announcing the instant
they wait for, and take the lease 35 s before arming, enough for the rig, the
engine and the baseline), `purposes={"cell": "why"}` for a
rerun, `archive_dir`, `lease_wait_s` and `report_dir`. The family state
(`failures-state.json`) keeps each cell's latest run, which the index lists.

The remaining subcommands (`buffered`, `remediate`, `report`) are named
here so the command line is one contract; each is implemented by its own
task.

## Reference deployment

The end-to-end suite runs the exporter's reference topology: a file producer
in Docker Grafana Alloy, the host's `df_engine` with its OTLP gRPC receiver,
and a Docker MinIO (or RustFS) destination, read back by two readers.

```text
/input/events.log -> Alloy -> OTLP gRPC -> df_engine -> MinIO Parquet
                                                       |
                                             downloaded object snapshot
                                                       |
                                               DuckDB + ClickHouse
```

It runs on Linux: Alloy uses host networking to reach the engine's loopback
listener, and MinIO publishes an ephemeral loopback-only S3 port. The engine
YAML is generated from `configs/series-parquet-local.yaml` with S3 storage
settings equivalent to `configs/series-parquet-s3.yaml`, one core and
`wait_for_result: true`; the helpers write the launched YAML as
`pipeline.yaml` in the test's working directory and remove only their own
containers.

`DockerSlice.test_minio` in `test_e2e.py` (`test_rustfs` against RustFS)
writes 12 known lines through Alloy and six metrics requests over OTLP gRPC,
downloads the objects, and runs DuckDB and `clickhouse-local` over the same
files. Both readers must return every body with its `e2e.source` attribute,
agree on the row counts, and keep them through the latest-descriptor join.
ClickHouse's `file()` does not synthesize `date` and `hour`, so the suite
reads DuckDB with `hive_partitioning=false` to compare column by column.
Native ClickHouse is preferred at `/usr/bin/clickhouse-local`
(`SERIES_CLICKHOUSE_LOCAL` selects another executable), with `docker exec`
in the selected ClickHouse image as the fallback. Missing both reader routes
skips locally; a reader that is present but cannot run the query fails.

```bash
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install --require-hashes -r crates/validation/tests/series_parquet/requirements.lock.txt
SERIES_REQUIRE_DOCKER=1 /tmp/series-parquet-venv/bin/python -m unittest -v \
  crates.validation.tests.series_parquet.test_e2e.DockerSlice.test_minio
```

Images default to `minio/minio:RELEASE.2025-04-22T22-12-26Z`,
`rustfs/rustfs:1.0.0-rc.3`, `clickhouse/clickhouse-server:26.7.4` and
`grafana/alloy:v1.19.2` (see "Environment variables" to override them).
Alloy is the only image the runner may pull. A missing Docker daemon or image
skips locally unless `SERIES_REQUIRE_DOCKER` is `1`; a startup or reader
failure always fails. The `series-parquet-e2e` workflow provisions the images
and runs both stores and both readers with `SERIES_REQUIRE_DOCKER=1`.

### The Alloy producer

[`configs/series-parquet.alloy`](../../../../configs/series-parquet.alloy),
shared by the normal and the outage tests, tails `/input/events.log` and
exports to `OTLP_ENDPOINT`, the engine's `127.0.0.1:<grpc_port>`. Its
transform stage sets the resource attributes the Loki bridge does not supply:
`host.id`, named by `producer_id_attribute`, and `service.name`, which feeds
the denormalized service column; an attributes stage inserts `e2e.source`.
The fixture is one producer, so it sets a constant `host.id`; each real
producer needs its own value (series-lake README, "Producer id contract").

The shipped attempt timeout of 180s is the exporter README's attempt-timeout
rule applied to a 15s window and a 60s flush deadline. The tests run a
one-second window and set 6s through `SERIES_ALLOY_TIMEOUT`, which the config
reads, so an expired attempt is visible inside their own waits. The fixture
writes 12 lines, far below `min_size`, so the batcher releases them on its 5s
`flush_timeout`.

The producer settings were measured against `grafana/alloy:v1.19.2` with a
server that holds each export for a fixed time, as the exporter does. A
producer holding one export per window sustains at most

```text
records/second = num_consumers * records_per_export / hold_time
```

where the hold time is at worst a whole window plus the flush: four consumers
sustained about 5000 records per second at a 15s hold and about 1050 at a 60s
hold. The shipped file runs eight consumers with a 20000-record minimum
batch, sized for 10000 records/s from one producer. Three settings decide
whether that ceiling is reachable:

- **Batching must be switched on.** `otelcol.receiver.loki` turns one log line
  into one OTLP request, and the sending queue's batcher is off without a
  `batch {}` block, so `records_per_export` stays 1.
- **The queue must be sized in items.** With the default `sizer = "requests"`
  the accumulating batch keeps its single-record slots until its export
  finishes, so a batch never exceeds `queue_size` and concurrency collapses
  to one.
- **Alloy's default `timeout` of 5s is below any usable window.** A producer
  left on it completes nothing against a 15s window and logs a deadline error
  every five seconds.

Queue overflow is silent loss before the exporter sees the data. By default
the sending queue returns a retryable error that the Loki bridge logs and
discards, and the tailer reads on, so the file source sets
`block_on_overflow = true` and leaves unread data on disk. Watch
`otelcol_exporter_enqueue_failed_log_records_total`, the only loss signal:
`otelcol_exporter_send_failed_log_records_total` stays zero under
`max_elapsed_time = "0s"`. Budget about 4 KB of resident memory per
`queue_size` item; an in-flight export keeps its queue space until it
completes, retries included. The engine's `max_concurrent_requests` must be
at least the sum of `num_consumers` over its producers, or exports queue at
the receiver with no signal from the exporter; the local example sets 128.

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
| `SERIES_MINIO_IMAGE`, `SERIES_RUSTFS_IMAGE`, `SERIES_CLICKHOUSE_IMAGE`, `SERIES_ALLOY_IMAGE` | The container images the end-to-end lane uses, each a locally present tag; defaults in "Reference deployment". |
| `SERIES_CLICKHOUSE_LOCAL` | The `clickhouse-local` executable; default `/usr/bin/clickhouse-local`. |

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

NGINX logs each request's completion time, method, URI, status, upstream
status, request and upstream times, response bytes, request length and the
full request URI, whose query names each multipart step.

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
  enumerates every thread of the engine and requires every mapped worker TID
  to be there, allowed exactly its own core, and every other thread carrying
  a worker name to be confined to one mapped worker's core, so a replacing
  worker or a worker-named thread elsewhere fails even for a single tick. A
  worker runtime's blocking-pool threads take its name and its affinity (the
  local file store runs its file writes there), so they pass, and a
  later snapshot maps the worker to the TID the start snapshot mapped. A
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
