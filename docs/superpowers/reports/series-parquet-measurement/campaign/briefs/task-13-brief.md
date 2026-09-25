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

