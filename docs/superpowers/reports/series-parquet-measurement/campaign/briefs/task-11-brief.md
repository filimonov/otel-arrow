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

