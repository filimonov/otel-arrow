# Task 5 report: series_parquet capacity (throughput, fan-in, write speed)

Worktree `<repo>/.claude/worktrees/agent-a9969184136f0dcc8`,
branch `worktree-agent-a9969184136f0dcc8`, reset to 7e2268935 at the start.

## 1. How it was measured

- `measure capacity` (`crates/validation/tests/series_parquet/capacity.py`, documented
  in the harness README, "Capacity"). Release `df_engine` (jemalloc, like the
  engine ships), fresh engine per trial, pinned with `taskset -c 0-7,16-23`;
  workers on whole physical cores of CCD0; the store container on its own core.
- Producer: open loop with monotonic target times and bounded in-flight, eight
  spawned sender processes, one per physical core on CPUs 8-15,24-31 (each confined
  to its core and SMT sibling, every thread confined through /proc/self/task),
  256 TCP connections (one per gRPC channel, local subchannel pool), 1000 records
  per request. Requests come from a self-verifying template generator
  (`generator.py`): 97 templates per signal, fixed-width per-request fields, and an
  aggregate oracle (per-request sequence coverage, duplicates, foreign rows,
  count/sum of sequences, a 200-record field sample, file invariants, a ClickHouse
  count/sum cross-check). It replaced a prebuilt request pool and a per-record
  sqlite ledger on the coordinator's direction; the one-worker cells and the
  shipped four-worker local cell measured with the pool stay valid (their producer
  was never the limit, and the generator's r143 re-anchor at 608k agreed).
- Workload `mixed-1k-hot`: 80/20 logs/metric points by record, 1 KiB bodies, 10k
  series slots cycled per record. Measurement 15 s warm-up plus 60 s measured
  interval.
- Stability rule (hard): durable acknowledged rate over the last 30 s
  (phase-averaged over one window) >= 98 percent of offered, backlog slope <= 2
  percent of offered, no failed or partial request; values rows written over the
  interval >= offered x (0.98 x measure - one window - one reporting interval).
  Refusals decide before producer lateness; a trial is producer-limited when more
  than 1 percent of sends are over 50 ms late with a free slot and not behind an
  in-flight wait; that bounds the search instead of ending it.
- Search: doubling from 1k (or a recorded floor), bisection to a 10 percent
  bracket, the winner repeated to three independent trials; a failed repetition
  moves the search below it. Flip rates (one pass, one fail) are recorded.
- Variants: `shipped` (128 receiver slots per worker, one-second window),
  `raised` (4096 slots and pdata channel 4096; the engine clamps
  `max_concurrent_requests` to the pdata channel capacity), `default_window`
  (15 s, 128 slots, the configuration as it ships), `buffered` (durable buffer in
  front of the exporter, 128 slots).
- Every trial held the host lease; a trial that overlapped a build was retried
  automatically (r162 and r200 by another user's rustc; r251 by my own rehearsal's
  `rustc --version`, see 13).
- Memory: jemalloc `stats_interval:67108864` prints, tailed every 5 ms and paired
  with `smaps_rollup`; the RSS residual is bounded by the allocator band
  (`measurement.allocator_band_residuals`, coordinator-approved). The stats
  prints cost at most about 6 percent CPU per record (section 8).
- Conditions stated as required: MinIO and RustFS run plain HTTP, where
  series_parquet signs every payload (`unsigned_payload` applies to TLS only);
  the harness engine configuration drops unsupported points (`unsupported: drop`);
  the stage benches run on jemalloc.

## 2. Maximum sustainable rate per store, core count and configuration

Workload mixed-1k-hot, 1000-record requests, 256 connections. "Sustainable" was
sustained by three of three independent trials; "unsustainable" is the lowest rate
that failed; flips are rates where trials disagreed.

| cell | variant | sustainable | unsustainable | flips | reps at winner | kind |
| --- | --- | --- | --- | --- | --- | --- |
| local-c1 | shipped | 128,000 | 136,000 | [] | 3/3 (r042, r048, r049) | maximum |
| local-c1 | raised | 255,000 | 272,000 | [] | 3/3 (r054, r055, r056) | maximum |
| local-c4 | shipped | 288,000 | 304,000 | [320000] | 3/3 (r077, r079, r080) | maximum |
| local-c4 | raised | 684,000 | 722,000 | [722000] | 3/3 (r148, r151, r152) | maximum |
| local-c4 | buffered | 144,000 | 152,000 | [152000] | 3/3 (r189, r192, r193) | maximum |
| minio-c1 | shipped | 128,000 | 136,000 | [] | 3/3 (r116, r122, r123) | maximum |
| minio-c1 | raised | 187,000 | 204,000 | [204000] | 3/3 (r131, r132, r133) | maximum |
| minio-c1 | default_window | 8,500 | 9,000 | [] | 3/3 (r248, r249, r250) | maximum |
| minio-c4 | shipped | 240,000 | 256,000 | [256000] | 3/3 (r174, r175, r176) | maximum |
| minio-c4 | raised | 448,000 | 480,000 | [480000] | 3/3 (r180, r183, r184) | maximum |
| minio-c4 | default_window | 28,000 | 30,000 | [30000] | 3/3 (r262, r265, r266) | maximum |
| minio-c4 | buffered | 152,000 | 160,000 | [] | 3/3 (r199, r201, r202) | maximum |
| rustfs-c1 | shipped | 64,000 | - | [] | 1/1 (r141) | lower_bound |
| rustfs-c1 | raised | 192,000 | 208,000 | [] | 3/3 (r213, r216, r217) | maximum |
| rustfs-c4 | shipped | 224,000 | 240,000 | [240000, 256000] | 3/3 (r228, r231, r232) | maximum |
| rustfs-c4 | raised | 360,000 | 390,000 | [390000] | 3/3 (r235, r240, r241) | maximum |

RustFS c1 shipped was skipped on the coordinator's ruling (user-approved
shortcut); only its 64k lower bound (r141) exists. The shipped c1 ceilings on
local and MinIO sit exactly on the slot formula (section 4).

Search floors (user-approved shortcut): a cell after the first could start at half
of min(the one-worker ceiling on the same store x workers, the shipped-slot
formula), rounded down to the doubling grid, halving if unsustainable; the floors
used are recorded in each index (`rules.search_floors`).

## 3. Winner metrics: CPU, latency, write speed, memory

Median of the winner's trials. "whole-proc rec/CPU-s" is durable records per
engine CPU second over every engine thread (workers, runtime and blocking pools,
observability).

| cell | variant | durable/s | per core | whole-proc rec/CPU-s | CPU ns/rec | occ | ack p50/p99 s | fresh p50/p99 s | obj MB/s | total MB/s | in MB/s | compr | avg obj MB | peak RSS GB | accounted GB | acc/budget | rss band residual MB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| local-c1 | shipped | 128,000 | 128,000 | 233,565 | 4,281 | 0.55 | 0.78/1.06 | -/- | 78.0 | 77.2 | 112.2 | 0.695 | 37.5 | 0.59 | 0.16 | 0.096 | 30.3 |
| local-c1 | raised | 255,017 | 255,017 | 259,195 | 3,858 | 0.98 | 1.57/2.70 | -/- | 155.2 | 153.4 | 223.5 | 0.694 | 74.6 | 1.09 | 0.68 | 0.416 | 64.5 |
| local-c4 | shipped | 288,000 | 72,000 | 223,218 | 4,480 | 0.32 | 0.69/1.20 | -/- | 176.0 | 174.2 | 252.4 | 0.697 | 21.2 | 1.07 | 0.28 | 0.043 | 35.3 |
| local-c4 | raised | 683,950 | 170,988 | 212,895 | 4,697 | 0.80 | 1.21/2.56 | -/- | 416.3 | 411.5 | 603.6 | 0.690 | 50.1 | 2.71 | 1.72 | 0.262 | 14.2 |
| local-c4 | buffered | 144,000 | 36,000 | 169,453 | 5,901 | 0.21 | 0.06/1.43 | 0.74/1.54 | 88.4 | 87.5 | 127.1 | 0.696 | 10.8 | 1.22 | 0.25 | 0.038 | 18.6 |
| minio-c1 | shipped | 128,000 | 128,000 | 233,374 | 4,285 | 0.55 | 0.93/1.07 | -/- | 78.1 | 77.1 | 113.0 | 0.691 | 37.5 | 0.58 | 0.22 | 0.133 | 36.6 |
| minio-c1 | raised | 187,017 | 187,017 | 224,688 | 4,451 | 0.83 | 1.30/1.87 | -/- | 113.9 | 112.5 | 165.0 | 0.690 | 54.8 | 0.74 | 0.49 | 0.302 | 48.3 |
| minio-c1 | default_window | 8,500 | 8,500 | 156,878 | 6,374 | 0.05 | 8.39/15.03 | -0.03/-0.01 | 5.2 | 4.7 | 7.5 | 0.692 | 29.0 | 0.53 | 0.29 | 0.177 | 27.1 |
| minio-c4 | shipped | 240,000 | 60,000 | 216,311 | 4,623 | 0.28 | 0.92/1.54 | -0.03/-0.01 | 146.8 | 145.2 | 211.8 | 0.693 | 17.7 | 1.01 | 0.61 | 0.094 | 14.0 |
| minio-c4 | raised | 448,000 | 112,000 | 211,748 | 4,723 | 0.53 | 1.32/2.84 | -0.04/-0.01 | 273.2 | 269.9 | 395.4 | 0.691 | 33.0 | 1.81 | 1.19 | 0.182 | 11.1 |
| minio-c4 | default_window | 28,000 | 7,000 | 173,291 | 5,771 | 0.04 | 8.55/15.45 | -0.03/-0.01 | 17.1 | 15.5 | 24.7 | 0.692 | 23.2 | 1.40 | 0.58 | 0.088 | 43.7 |
| minio-c4 | buffered | 151,967 | 37,992 | 164,565 | 6,077 | 0.23 | 0.11/1.91 | 0.93/1.91 | 93.3 | 92.3 | 134.1 | 0.696 | 11.3 | 1.34 | 0.48 | 0.073 | 12.6 |
| rustfs-c1 | shipped | 64,000 | 64,000 | 220,081 | 4,544 | 0.29 | 0.84/1.34 | -/- | 39.2 | 38.7 | 56.5 | 0.693 | 18.8 | 0.34 | 0.17 | 0.105 | 12.7 |
| rustfs-c1 | raised | 192,000 | 192,000 | 222,205 | 4,500 | 0.86 | 1.37/1.90 | -0.03/-0.01 | 116.9 | 115.4 | 169.4 | 0.690 | 56.2 | 0.75 | 0.48 | 0.291 | 66.1 |
| rustfs-c4 | shipped | 224,000 | 56,000 | 213,567 | 4,682 | 0.26 | 1.02/1.60 | -0.04/-0.01 | 137.1 | 135.4 | 197.7 | 0.694 | 16.5 | 0.96 | 0.62 | 0.095 | 25.4 |
| rustfs-c4 | raised | 360,000 | 90,000 | 211,467 | 4,729 | 0.43 | 1.24/1.87 | -0.04/-0.01 | 219.7 | 216.9 | 317.7 | 0.692 | 26.4 | 1.35 | 1.10 | 0.168 | 38.5 |

Durable write speed (object bytes per second over the measured interval, Parquet
after zstd, compression about 0.69 of wire bytes):

- local: 78 MB/s at c1 shipped, 155 MB/s at c1 raised, 176 MB/s at c4 shipped,
  **416 MB/s at c4 raised** (684k records/s).
- MinIO: 78 MB/s c1 shipped, 114 MB/s c1 raised, 147 MB/s c4 shipped,
  **273 MB/s at c4 raised** (448k records/s).
- RustFS: 39 MB/s c1 shipped (lower bound), 117 MB/s c1 raised, 137 MB/s c4
  shipped, **220 MB/s at c4 raised** (360k records/s).
- With 8 KiB bodies (section 9) MinIO took 196 MB/s from one worker at 40k
  records/s and **315 MB/s from four at 64k**, the highest MinIO write rate
  measured.

Freshness (acknowledgement to object visible): on S3 the object's timestamp
precedes the acknowledgement by 10-40 ms (strict acks after the upload), so the
lag is at most zero; local stores carry no object timestamp the harness trusts.

## 4. The strict admission ceiling: a formula, measured

A strict request holds its receiver slot until its block is durable, so

```text
strict records/s per worker <= receiver slots x records per request / window
```

(every request of a window waits for that window's flush, so the in-flight
count peaks at the window boundary).

| configuration | formula per worker | measured per worker |
| --- | --- | --- |
| 128 slots, 1 s window, 1000 rec/req | 128,000 | local c1 128k sustainable, 136k not; MinIO c1 the same |
| 128 slots, 15 s window (as shipped) | 8,533 | MinIO c1 8.5k sustainable 3/3 (r248-r250), 9k not (r247) |
| 128 slots, 15 s window, four workers | 34,133 | MinIO c4 28k sustainable 3/3 (r262, r265, r266), 30k flip (r263 passed, r264 failed), 32k not (r260): 7k per worker |
| 4096 slots, 1 s | 4.1M | no longer binding; CPU binds (section 5) |

The one-worker shipped ceiling is the formula exactly, on local disk and on
MinIO: the exporter itself had headroom (raised c1 reached 255k local, 187k
MinIO). At four workers the shipped searches land at 47-56 percent of 4 x 128k
(288k local, 240k MinIO, 224k RustFS). The trials just above them fail on a few
dozen to a few hundred receiver refusals, RESOURCE_EXHAUSTED "Too many active
requests for the connection" (r078, r167): tonic's load-shed answer when the
worker's slots are all taken. The 256 connections spread unevenly (the busiest
worker takes 1.11-1.14 times the mean, r077, r174, r228), requests arrive in
bursts, and the flush adds 0.2-0.45 s to the hold, so the busiest worker runs out
of slots while the mean worker is at 60-72k/s. The formula is the upper bound
per worker; with four workers budget for the busiest one.

Remedies, in the order a user should try them: raise `max_concurrent_requests`
(and with it the pdata channel capacity, which clamps it) to cover
rate x window / records per request per worker; send larger requests; shorten the
window; or run the buffered topology, whose acknowledgement does not wait for the
object store (section 6).

## 5. Why four workers give 0.67 of four times one, and the 1M/s verdict

Local raised: c1 255k, c4 684k, scaling 684 / (4 x 255) = 0.67. Measured from
CPU, the loss has two factors:

- CPU per record grows 22 percent (3,858 ns c1 to 4,697 ns c4 over every engine
  thread): 0.81. The four workers share CCD0's 32 MB L3 and memory bandwidth with
  their SMT-idle siblings; whole-process throughput per CPU second falls from
  259k (c1 raised) to 213k (c4 raised) records per CPU second.
- Worker occupancy 0.80 of four cores against 0.98 of one: 0.82. At 684k the
  busiest worker's flush already takes 0.76 of the window (r151) and admission
  closes for 0.9-1.3 s a minute; just above (722k) its flush reaches the window
  and closes admission while the other workers still have CPU. Request
  distribution max-to-mean 1.05.

0.81 x 0.82 = 0.67.

Per worker core: 255k (c1) and 171k (c4) records/s; per whole-process CPU
second: 259k and 213k.

**1M records/s**: not measured; the highest measured sustainable rate is 684k on
four workers (local, raised slots; 722k flipped, 760k and 912k failed; 1.216M
could not be measured validly because the overload starved the build monitor,
r113/r144). At the four-worker whole-process figure of 213k records per CPU
second, 1M/s needs about 4.7 CPU seconds per second of engine work, which at the
four-worker occupancy of 0.80 is about **six worker cores** (estimate from the
measured c4 figures, not extrapolated beyond the per-core cost measured at four;
a six-worker run was not made). The limiting stage at the measured maximum is the
flush on the ingest core reaching the window length on the hottest worker, which
closes admission. The generator is not the limit: calibration against the noop
exporter sustained 4M/s at c4 and 2M/s at c1 (r101-r112), and at 684k each sender
used about 10 percent of a core.

## 6. Buffered topology

Search at 128 slots: local c4 144k sustainable (152k flip), MinIO c4 152k (160k
not). Compared with strict: local c4 shipped 288k, raised 684k.

Buffered at the winner: ack p50/p99 0.06/1.43 s (local) and 0.11/1.91 s (MinIO)
against strict 0.69/1.20 and 0.92/1.54; freshness (ack to object) p50/p99
0.74/1.54 s and 0.93/1.91 s; WAL bounded (r189: 275 MB start, 436 MB max, 266 MB
end, slope 69 kB/s), no ingest failures, no deferred or rejected bundles, every
acknowledged record stored once. CPU per record 5.9-6.1 us against 4.5-4.7 strict.

Cause (diagnostic trials, no baselines): the WAL and its segment files are written
synchronously on the worker's runtime, about 2.5 times the wire bytes; on this
host's single NVMe (dm write await 50 ms at 416 MB/s, r203, 192k unsustainable)
that stalls the worker. With the WAL on tmpfs the same topology sustains 160k,
192k and 256k and fails at 384k (r204, r205, r208, r209); moving the local store
to tmpfs as well changes nothing (r207, r210). The WAL knobs that would let a user
move or batch this are not exposed by the buffer's configuration (code references
sent: quiver engine.rs:778,1209,1306-1337; wal/writer.rs:858-893; config.rs:227,
299; durable_buffer config.rs:104-181; mod.rs:454-475). Task 13 owns durability.

Buffered at 80 percent of the MinIO c1 raised ceiling (149.6k, raised slots,
r255): unsustainable, durable 124k/s, ack p50 10.2 s, peak RSS 2.8 GB. One
buffered worker is below 149.6k; its own ceiling was not searched (the WAL-bound
four-worker ceilings above are the searched ones).

## 7. Fan-in, upload concurrency, default-window memory

**Fan-in** (MinIO c4, raised slots, 448k offered, the cell's ceiling; one trial
per connection count, 1000-record requests):

| connections | verdict | durable/s | ack p50/p99 s | requests per worker in the window | run |
| --- | --- | --- | --- | --- | --- |
| 1 | unsustainable | 225,833 | 40.7/73.3 | 0 / 0 / 13,760 / 0 | r270 |
| 8 | unsustainable | 395,950 | 1.70/18.9 | 3,360 / 6,720 / 13,305 / 3,360 | r271 |
| 64 | unsustainable (backlog slope 2.5 percent) | 438,800 | 1.31/6.53 | 9,246 / 6,300 / 4,200 / 7,140 | r272 |
| 256 | sustainable | 448,000 | 1.30/3.96 | 6,828 / 8,290 / 6,091 / 5,670 | r273 |

A connection is served by one worker for its lifetime, so one producer
connection uses one of four workers (226k/s from one worker at raised slots),
eight connections hash unevenly (one worker took 40 percent), and the cell's
ceiling needs a few hundred connections. `ss` counted one more established
connection than the producer opened in each trial: the harness's own control
channel. For a deployment: the aggregate ceiling of N workers is reached only
with many more connections than workers, or with a producer that opens several
connections (`grpc.use_local_subchannel_pool` or several channels).

**Upload concurrency 1 against 2** (the searches ran 2):

| cell, configuration | rate | concurrency 2 | concurrency 1 |
| --- | --- | --- | --- |
| MinIO c1, 15 s window | 8.5k | sustainable, ack p50/p99 8.38/15.01 s (r252) | sustainable, 8.41/15.02 s (r253) |
| MinIO c4, 15 s window | 28k | sustainable, 8.58/15.47 s (r267) | sustainable, 8.49/15.39 s (r268) |
| MinIO c1, raised, 1 s | 187k | winner r131-r133, 1.30/1.87 s | sustainable, 1.26/1.82 s (r254) |
| MinIO c4, raised, 1 s | 448k | winner r180-r184, 1.32/2.84 s | sustainable, 1.26/1.99 s (r269) |

One upload at a time sustains every measured ceiling on MinIO: with 8 MiB
parts and 20-55 MB objects the upload is not the limit at these rates.

**Memory at the real (15 s) fill**: MinIO c1 at 8.5k/s (r249): block fill peak
149 MB for 127.5k records, accounted peak 290 MB (fill plus a 159 MB flush
workspace), RSS peak 538 MB, 0.18 of the 1.64 GB budget, objects 29 MB. MinIO
c4 at 28k/s (r265): block fill 530 MB, accounted 535 MB, RSS 1.46 GB, 0.08 of
the 6.55 GB budget. The highest fill measured anywhere is local c4 raised at
684k (r151): accounted 1.46 GB at fill, allocated 1.98 GB, RSS 2.39 GB,
0.23 of budget; accounted against jemalloc allocated differs by 0.52 GB at the
highest fill, growing 1.7 bytes per record over the run (c1 raised: 7.2 bytes
per record); no gate, reported as asked.

## 8. Stats-off control

Local c1 shipped at 128k with the jemalloc statistics prints off (r088): 4,025 ns
per record against 4,073-4,287 with prints (r042, r048, r049), so the prints cost
at most about 6 percent. r088 is sustainable but its status is failed: without the
prints there is no allocator band, so its RSS residual falls back to the pipeline
counter that drifts (a known Task 12 finding); it is a CPU control only.

## 9. Workload confirmations, high cardinality, Alloy

**Workload confirmations** at 80 percent of the MinIO ceilings (raised slots;
MinIO c1 149.6k, MinIO c4 358.4k):

| workload | c1 | c4 |
| --- | --- | --- |
| logs-1k-hot | sustainable, 4,962 ns/rec (r256) | sustainable, 5,500 ns/rec (r274) |
| mixed-1k-churn (new series every request) | sustainable, 3,652 ns/rec (r257) | sustainable, 3,946 ns/rec (r275) |
| mixed-8k-hot | see below | see below |

mixed-8k-hot: a thousand 8 KiB records is an 8.26 MB request, twice the
receiver's default 4 MiB decoding limit, which refused every log request with
OUT_OF_RANGE (r258, r276; harness fault, superseded, fixed in 5a865f543 by a
16 MiB limit for that workload only). With 16 MiB and the raised 4096 slots the
engine vanished 22-60 s into the trial with no log line and no shutdown, and
the host's build monitor stalled for 23.9 s (r277, r278, both invalid). 4096
slots admit up to 32 GB of 8 MB requests per worker before the exporter's budget
sees any of it; the engine's RSS was not captured before it vanished, so memory
is the likely cause, not a measured one (Task 12 finding 2). With the shipped
128 slots (RSS watched each second peaked at 1.9 GB in r283 and 5.9 GB in r284):

| cell | offered | verdict | durable/s | input MB/s | object MB/s | flush mean / window | run |
| --- | --- | --- | --- | --- | --- | --- | --- |
| MinIO c1 | 102.4k | unsustainable (residual +234 MB also failed) | 44,667 | 296 | 219 | 1.17 | r283 |
| MinIO c1 | 40k | sustainable | 40,000 | 265 | 196 | 0.76 | r285 |
| MinIO c4 | 192k | invalid (monitor gaps), unsustainable | 65,000 | - | 318 | 2.5-2.7 | r284 |
| MinIO c4 | 64k | sustainable | 64,000 | 423 | 315 | 0.54-0.80 | r286 |

Single trials, not searched to a 10 percent bracket (the time budget's drop
order allows the 8 KiB row to be cut first after upload concurrency): per
record the 8 KiB row costs 17-21 us, and the limit is the flush on the ingest
core (c1) and the MinIO write (c4, 315 MB/s, the highest object write rate
measured on MinIO).

**High cardinality** (local c1, 20k points/s, 1.5M points in the run):

| workload | series | points | series per point | series dataset bytes | values dataset bytes | CPU ns/point | peak RSS | run |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| metrics-1k-hot (10k slots) | 10,000 | 1.5M | 0.0067 | 0.27 MB | 50.3 MB | 2,872 | 0.11 GB | r281 |
| metrics-1k-unique (unique attribute per point) | 1,500,000 | 1.5M | 1.0 | 42.0 MB | 63.8 MB | 5,396 | 0.15 GB | r282 |

A point attribute unique per point makes every point a series: the descriptor
dataset grows to 66 percent of the values dataset, total bytes 2.1 times, CPU
per point 1.9 times. Both sustained 20k/s; the exporter-side series/points
gauge and WARN the spec asks for do not exist (Task 12 finding 5).

**Alloy as producer** (grafana/alloy v1.19.2, the reference River config
unchanged: batch min 20000 / max 50000 items, 8 consumers, queue 120000 items,
block_on_overflow, compression none, attempt timeout 180 s). Local c4, strict
shipped (128 slots, 1 s window). Offered 230,400 lines/s = 0.8 x 288k.
Lines are 100 bytes. Oracle: the end-to-end read-back (rows per source file by
DuckDB and ClickHouse, latest-descriptor join, every sequence number).

| | r279: receiver as shipped (4 MiB decoding limit) | r280: receiver limit 16 MiB, nothing else changed |
| --- | --- | --- |
| verdict | failed: no line delivered | sustainable, status passed |
| requests | 812 attempts, all OUT_OF_RANGE "decoded message length too large: found 7960000 bytes, the limit is: 4194304" | 166 attempts, all OK |
| Alloy read / sent / stored per s | 0 / 0 / 0 (tailer parked at 120,003 lines, queue full) | 29,584 / 29,250 / 29,667 |
| records per request p50/p95/max | 20,000 / 20,000 / 20,000 | 20,000 / 20,000 / 20,000 |
| bytes per request p50/p95/max | 7.96 MB each | 7.96 MB each (398 B per 100-byte line) |
| connections | 1 carrying requests (3 established to the tap) | 1 carrying requests (3 established to the tap) |
| engine response time p50/p99 | - | 0.58 / 1.10 s (tap); Alloy's own histogram p50 <= 0.75 s, p99 <= 2.5 s |
| read-back | nothing stored | 3,300,003 lines, 3,300,003 rows in /input/events.log, e2e.source alloy-file, 0 duplicates, 0 missing; ClickHouse agrees |
| workers used | - | 1 of 4 |
| engine CPU | - | 4,715 ns/record, 0.035 of four cores |
| Alloy CPU | 6.1 s | 88.8 s over 60 s: 1.5 cores for 29.6k lines/s |

Alloy's own maximum is about 29.6k lines/s, far below the offered 230k, so the
trial ran at Alloy's maximum (the writer was held at a million lines ahead of
Alloy's reads for 68 of 75 s). The limit is inside Alloy, not the engine: its
queue sat full at 120,000 items while the exporter had at most one request in
flight and eight consumers idle; the same pipeline into a Python sink reached
24-27k lines/s. The slot formula for these requests predicts 128 x 20,000 /
(1 s + 0.03 s flush) = 2.49M records/s per worker; the Alloy side, 8 consumers x
20,000 / 0.58 s = 275k/s; neither is reached. Alloy's single connection pins
all of it to one engine worker. The Loki bridge wraps every line in its own
ResourceLogs with six attributes, so a 100-byte line is 398 bytes on the wire.

The shipped Alloy batching and the engine's shipped receiver do not work
together at any rate that fills a 20,000-record batch within the 5 s flush
timeout (4,000 lines/s): every export is 7.96 MB, the engine refuses it, and
Alloy retries forever (`max_elapsed_time = "0s"`), parked, delivering nothing.
The end-to-end suite writes 12 lines and never forms such a batch (Task 12
finding 1).

## 10. Admission-path stall

The flush-stall probe (`crates/series-lake/benches/measurement/flush_stall.rs`)
timed each synchronous call on the admission path: the longest stretch was
10.7 ms (a metrics values-run seal of 39,000 rows), `Block::seal` at most 6.6 ms,
against 21-34.5 ms on the flush side. No finding. Evidence:
`docs/superpowers/reports/series-parquet-measurement/flush-stall/*-t5-admission.json`.

## 11. What degrades above the ceiling

From the degradation view each trial now carries (per worker: flush count and
mean against the window, flushes by reason, admission closed seconds, nacks by
class, receiver refusals). Example, local c4 shipped at 384k (r071): workers
36-43 percent on CPU, 1,059 retryable nacks of about 23k requests, admission
closed 1.06 s of the window, 45 sends blocked on in-flight and 408 late (lateness
p99 0.81 s), durable 368.9k/s. No data is lost: the exporter closes admission when
a flush outlasts the window, the receiver refuses, producers fall behind schedule.
At the raised c4 maximum the hottest worker's flush mean is 0.76 s of a 1 s window
(r151), and above it flush/window reaches 1.0.

## 12. Task 12 findings

1. **Receiver decoding limit against the shipped Alloy config and large
   requests.** `configs/series-parquet-*.yaml` do not set the receiver's
   `max_decoding_message_size` (tonic's 4 MiB default); the reference Alloy
   config's 20,000-record minimum batch is 7.96 MB, so every export is refused
   with OUT_OF_RANGE and retried forever (r279, 812 refusals, zero delivered);
   the same limit refuses the 8 KiB-body workload's 8.26 MB requests (r258,
   r276). Remedies: set `max_decoding_message_size` (16 MiB sufficed, r280) in
   the shipped engine configs; or cap Alloy's batch in bytes or at about 10,000
   items; and cover a full batch in the end-to-end suite.
2. **Receiver in-flight memory is outside the exporter's budget.** With 4096
   slots and 8 MB requests the engine disappeared within 22-60 s without a log
   line and the host stalled (r277, r278; memory the likely cause, RSS not
   captured); earlier, 1.216M offered at c4 raised
   reached 16 GB RSS against 3.4 GB accounted. Slots x request size is admitted
   before any budget sees it.
3. **Receiver load-shed is invisible and misnamed.** At the four-worker shipped
   ceilings the refusals are RESOURCE_EXHAUSTED "Too many active requests for
   the connection" (tonic's load-shed of a not-ready service, i.e. the worker's
   slots, not a per-connection limit; r078, r167, r284), and
   `receiver.otlp.requests rejected{concurrency_limit}` stays zero.
4. **`pipeline.memory.usage` drifts** (+10 GB over a run): a free is counted only
   on the allocating thread. The capacity residual now reads jemalloc.
5. **No series-to-point signal.** The spec's exporter gauge and WARN for a
   series/points ratio are not implemented; a per-point unique attribute makes
   the descriptor dataset 66 percent of the values dataset (r282). Product
   change, deferred.
6. **Buffered WAL and segment writes run synchronously on the worker runtime**
   and their placement and sync knobs are not exposed; on one NVMe they bound
   the buffered topology at about half of strict shipped (section 6).
7. **Shipped slot sizing.** With 128 slots and the shipped 15 s window a strict
   worker admits about 8.5k records/s at 1000-record requests (r248-r250;
   four workers 28k). The local example's `max_concurrent_requests: 128` is a
   ceiling most users will hit first.
8. **Paired-sample RSS peak beyond the allocator band**: +77 MB in r048 and
   +234 MB in r283 (8 KiB bodies), once each.
9. **jemalloc statistics prints cost up to about 6 percent CPU per record**
   (r088 against r042-r049).
10. **An overload starves the host monitor**: 1.216M offered at c4 raised (r113,
    r144) and the 8 KiB row at 192k on c4 (r284) could not be measured validly.
11. **Alloy's own ceiling** (sizing note for the reference deployment, not an
    engine defect): about 30k lines/s from one file at 1.5 cores, limited in
    Alloy's Loki bridge and item-sized queue, one connection, one worker; 1M/s
    needs dozens of such producers, which matches the 100k-1M from dozens to
    hundreds of producers target only at the upper producer counts.

## 13. Superseded and invalidated trials

Every superseded trial is kept in the indexes with its reason. Reasons, with
counts:

- first producer (four processes on two cores reading a prebuilt pool) replaced:
  calibrations r001-r010;
- first harness rules (rows-written 30 s check, warm-up on idle workers):
  r011-r013;
- RSS residual read `pipeline.memory.usage`: r014-r020, r025, r028-r033;
- producer rule counted sends behind an in-flight wait as late: r021-r023;
- monitor counted blocking-pool threads as a second worker: r024, r026;
- first verdict order (refusals judged producer-limited): r067-r069;
- producer page faults on the pool / producer on two cores: r082-r084, r095;
- the build monitor missed its tick at 1.216M (invalid, kept as evidence,
  steers the bisection as unmeasurable): r113, r144;
- the bucket still held r162's objects (aborted by another user's compiler):
  r163;
- a moved local store's base directory missing: r206;
- the 8 KiB workload ran against the 4 MiB decoding limit: r258, r276;
- interrupted by the operator for the fixes above: r027, r034, r076, r094,
  r100, r134, r142, r145, r164.

Invalidated by a concurrent build and retried automatically: r062, r085, r096,
r162, r200 (another session's rustc) and r251 (this task's own rehearsal's
`rustc --version`; its retry r252 was stopped when r251 showed the
default-window step ran the one-second winner, 128k, against a 15 s window: a
harness fault fixed in e7d9fea1e; neither file is published). r277 and r278
(8 KiB, 4096 slots) are failed and invalid, published as evidence. The
one-worker cells and the shipped four-worker local cell measured with the
prebuilt pool stay valid: their producer was never the limit and the
generator's r143 re-anchor at 608k agreed with the pool-era 608k trials.

## 14. Commits

Harness and evidence commits on this branch since 7e2268935 (oldest first):
09ed819fe, d8381c1f2, e16d5d1c8, eb43f5c52, 4b9b84f23, c757e796b, efc945d2e,
e0a0241a7, 5654ccef2, e3881f988, 1ab4f3541, 2cdcb6919, ea3b1d648, 27a4841f9,
985aee225, df3ead940, 357c0b91c, 772b93f28, 0b5eff71f, e436489ca, a5bbceda6,
4593ad713, 65b074f6f, e7d9fea1e (default-window rate fix), 7c2bb02be (Alloy
step), 5a865f543 (8 KiB decoding limit), 35d07364b (evidence), then the review
fixes 31e3061a0, ddc49924e, 1071bb2e3 (docs 4f734bf6b), the re-judgement 489774710 and its
evidence aacc4d44d (section 16).

Harness faults fixed in their own commits while measuring: the oracle held
every body (09ed819fe), window-phase judgement (4b9b84f23), blocking-pool
threads (c757e796b), the residual's heap term (efc945d2e), refusal order
(e3881f988), failed repetitions (1ab4f3541), producer read-ahead and bounds
(2cdcb6919, ea3b1d648, 985aee225), store clearing (a5bbceda6), the moved store
(65b074f6f), the default-window rate (e7d9fea1e), the 8 KiB limit (5a865f543).
No product code was changed.

## 15. Evidence

- Indexes: `docs/superpowers/reports/series-parquet-measurement/capacity-local.json`,
  `capacity-minio.json`, `capacity-rustfs.json` (status: MinIO passed; local and
  RustFS failed because local c1 shipped's aggregate carries r048's residual
  failure and RustFS c1 shipped has only its lower-bound trial), with every
  trial JSON, the per-cell aggregates (`*-f001.json`, and `*-f002.json` after
  the re-judgement) and the baselines they wrote (`baseline-capacity-*`). Committed in 35d07364b (313 files, about
  352 MB of JSON: the published samples carry every worker's extras).
- Admission-path probe: `docs/superpowers/reports/series-parquet-measurement/flush-stall/*-t5-admission.json`.
- Raw logs and configs of every trial (non-Parquet):
  `<repo>/.measurement-artifacts/capacity/<run_id>.tgz`.
- Family log: `<scratchpad>/t5/family.log`;
  8 KiB RSS watch: `.../scratchpad/t5/rss8k.log`.
- Harness: `rust/otap-dataflow/crates/validation/tests/series_parquet/capacity.py`,
  `alloy_capacity.py`, `generator.py`, README "Capacity"; contract tests in
  `test_measurement.py` (all pass).
- Re-judgement of every trial: `capacity.rejudgement` in each index; the
  family 1 indexes as `capacity-{local,minio,rustfs}-<hash>.json` children.

## 16. Review fixes and re-judgement

The Task 5 review asked for three fixes; each is its own commit with contract
tests that fail on the code before it:

1. 31e3061a0: the Alloy read-back (`judge_read_back`) fails on any duplicate
   row, any line stored more than once, and any values row count other than the
   lines written (it counted duplicates but never failed on them). The test
   writes a small lake and reads it with DuckDB and ClickHouse.
2. ddc49924e: engine-side failures outrank producer lateness on every path. A
   partially rejected request and the buffered topology's reasons (a growing
   WAL, ingest failures, rejected bundles) used to lose to a late producer
   (`producer_limited`) or only replace a sustainable verdict; `judge_trial` is
   now the one path for a live trial and for a stored one (`rejudge_stored`).
3. 1071bb2e3: `aggregate_oracle` gates the aggregate equalities itself
   (`aggregate_equalities`: acknowledged rows' count, distinct count, sequence
   sum, lowest and highest exactly the generator's; stored rows less stored
   failed-request rows equal the acknowledged records; every stored record
   once). The lost, duplicated, foreign and corrupted contracts now go through
   `aggregate_oracle` end to end on a small logs lake with both readers (only
   the part-file metadata check, which reads the exporter's own metadata, is
   reported clean), and a further contract blinds the per-request counters to
   show the equalities still fail the store.

Re-judgement (489774710, evidence aacc4d44d; no rerun). Every published trial
was judged again from what it stored: the verdict through `judge_trial` over
its outcomes, buffer view and metrics; the generator read-back's equalities
over its stored coverage; the Alloy read-back through `judge_read_back`. The
result is in each index under `capacity.rejudgement`:

- **No published verdict, read-back or search winner changes.** 280 trials,
  every cell and variant decision identical.
- Two superseded trials would be judged differently, each for the reason it
  was superseded: r011 (unsustainable then, sustainable by the current
  rows-written rule; superseded for the first harness rules) and r067
  (recorded producer-limited, now unsustainable on its 6,219 refusals;
  superseded for the first verdict order).
- 19 trials cannot be fully re-judged, each listed: interrupted or invalid
  before they settled (r023, r024, r026, r027, r034, r069, r076, r083, r094,
  r100, r134, r142, r145, r164, r206, r277, r278), r163's sequence sums (its
  failed-request rows are stored; its counts were checked), and r279 (Alloy,
  nothing stored).
- Only r163 stored any failed-request rows, so the sequence sum was checked on
  every other trial with a read-back.
- The Alloy r280 read-back passes the stricter judgement: 3,300,003 lines,
  multiplicity 1 for every line.

The indexes advanced to family 2 over the same trials: the family 1 indexes
are kept as immutable children (`child_indexes`), and the family 2 aggregates
(`*-f002.json`) have the same statuses as family 1 and are compared with the
family 1 baselines. So no cell was rerun.
