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

