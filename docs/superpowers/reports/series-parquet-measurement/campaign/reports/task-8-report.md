# Task 8 report: disposable fault-tool provisioning and capability probes

(Intended path: .superpowers/sdd/2026-09-22-series-parquet-measurement/task-8-report.md in the main checkout. The worktree isolation refused writes to the shared checkout, so the controller should copy this file there.)

Status: DONE. All 28 probes (14 per store, MinIO and RustFS) passed in required mode
(`SERIES_REQUIRE_DOCKER=1 SERIES_REQUIRE_FAULT_TOOLS=1`). Every fault class is available on this host.

## Worktree and commits

- Worktree: `<repo>/.claude/worktrees/agent-a876ff9764ee0afe4`
- Branch: `series-parquet-task-8-faults`. The worktree was created from `main` (5ee994690), not from the campaign branch. It had no changes, so I hard-reset it to `series-parquet-exporter` @ 479d46785 before doing any work.
- Commits on top of 479d46785:
  - `9218090a5` chore: provision isolated series fault tools
  - `7d598a5f7` fix(series_parquet): make fault-rig captures and artifacts complete (the evidence was re-measured; the first index is kept as child `fault-preflight-1b5f36aabfb3.json`)
- Not pushed. No Rust was built. The engine is the main checkout's release df_engine, set with `DF_ENGINE`.

## What was built

- `faults.py` (new)
  - `FaultRig(store, root)` is a context manager with `storage`, `launcher`, `evidence()`, `activate(name, parameters)`, `recover()`, `completed_values()`, `residual_state()` and `assert_clean()`.
  - On entry it does the following, in order:
    1. Creates a unique bridge network labelled `series-fault-run=<id>`.
    2. Attaches the store with alias `store`.
    3. Starts the fault-tools owner. This is the only container with `--cap-add NET_ADMIN`. It runs NGINX and publishes NGINX, the Toxiproxy API and the engine's gRPC and admin ports on 127.0.0.1 only.
    4. Starts Toxiproxy in `--network container:OWNER`.
    5. Creates the general (19001) and values (19002) proxies. Their upstream is the inspected store IP:9000, not the DNS name, so a DNS fault cannot break the proxy itself.
    6. Runs the entry probes (S3 route on both backends, capabilities, restored state) under the required/optional rule.
  - On exit it recovers any active faults and removes only the recorded containers and the network. It detaches the store and checks by label that nothing of the run is left. It keeps the artifacts under root (access log, pcaps, dnsmasq log) and chmods them readable.
  - `ContainerLauncher` runs the engine attached, as `--user uid:gid`.
    - Mounts: the binary and the repository read-only, the run, rig and buffer directories read-write, each at its own host path.
    - Environment: only RUST_LOG and RUST_BACKTRACE are passed through.
    - Before the first launch of a binary, `ldd` runs inside the image; a missing library is a setup failure with the output attached.
    - The engine's host PID comes from `docker inspect`. Signals go through `docker kill`.
    - All rig containers get the harness affinity as `--cpuset-cpus`.
  - Registered faults: `slow` (both toxics on both proxies), `http503` (control file) and `store_outage` (DockerStore stop/recover, then repoint the proxies if the store IP changed). Faults are registered through `register_fault` in the FAULTS table, which Tasks 9-11 extend. Any activation or recovery error after preflight is an AssertionError, never a skip.
  - `require_probe(probe, *, required)`, `coverage(...)`, `ack_drop_bytecode(store_ip, store_port, *, tcpdump=...)` and `preflight_fault_tools(required, ...)` are also here.
- `fault-tools.Dockerfile`: the brief's recipe, with `ARG BASE` so provisioning passes the resolved digest. The base is recorded as the label `org.opencontainers.image.base.name`.
- `fault-nginx.conf`: the brief's body verbatim, plus a comment header.
- `test_failures.py`: 32 engine-free contract tests plus one live rig test (LiveRigSlice, which takes the host lease and waits for it).
- Small hooks in shared files:
  - `test_e2e.py`: Engine takes its ports from `launcher.reserve_ports()` and its bind host from `launcher.bind_host` when the launcher defines them, which means the `self.launcher` assignment moved up in `__init__`. `engine_config(grpc_host=...)` is new. DockerStore gains `attach`, `detach` and `network_address`.
  - `measure.py`: a real `fault-preflight` subcommand (exit 0 pass, 1 fail, 3 skip), removed from PLANNED_COMMANDS.
  - `measurement.py`: `CASE_ROLES["faults"] = (producer 1, store 1, fault_tools 1)`. With the observability core and the 4-core engine reservation this is exactly 8 physical cores.
  - `README.md`: commands, environment variables and a Fault tools section.

## Images (provisioned outside measured windows)

| Image | Provenance |
| --- | --- |
| ghcr.io/shopify/toxiproxy:2.12.0 | sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e (image id 3edf5d14625b). `/version` reports 2.12.0. |
| ubuntu:24.04 base | ubuntu@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 |
| series-measure-fault-tools:local | image id sha256:691a66f109093e712c539b9cefc2210c434bb86a649af534090609e0b2155964, about 22 s to build |

- Packages in the fault-tools image:
  - nginx 1.24.0-2ubuntu7.18
  - dnsmasq 2.91-0ubuntu0.24.04.1
  - dnsutils 9.18.39
  - iproute2 6.1.0
  - iptables 1.8.10 (nf_tables backend)
  - tcpdump 4.99.4
  - tshark 4.2.2
  - libssl3t64 3.0.13-0ubuntu3.15
  - libstdc++6 14.2.0
  - curl 8.5.0
  - procps 4.0.4
- Both builds ran while my own process held the host lease, so no measured run overlapped them. Task 4's attribution waited for the lease both times, for about 22 s each.
- Only my own superseded image (28dfe23489a9) was removed. Nothing else on the host was touched.

## Probe results on this host (final published run, required mode)

MinIO and RustFS gave the same results:

| Probe | Result |
| --- | --- |
| S3 route general / values | Signed PUT, HEAD, GET, DELETE and a 2-part multipart (5 MiB + 4 KiB) all passed. Bytes were verified, and the access log shows every method on the expected backend. About 0.3 s each. |
| Route isolation | Disabling each proxy breaks only its own keys. |
| Engine launch | The containerized release engine acknowledged one log request. It stored series and values objects, NGINX logged 200 PUTs on both backends, and the engine's CapEff is 0. `ldd` found every library. |
| Capabilities | The owner has CapAdd CAP_NET_ADMIN and effective NET_ADMIN. Toxiproxy, the engine and the store have no CapAdd and no NET_ADMIN. Nothing is privileged. Toxiproxy and the engine run in `container:OWNER`. |
| UDP DNS | Blocked in 2.05-2.06 s (`+time=2`), 1 dropped packet counted. The exact rule was deleted and resolution restored. |
| TCP DNS | Blocked in 2.07 s, 2 dropped packets counted, restored. |
| xt_bpf | The ACK-only program (33 instructions) was inserted in the owner's INPUT chain. It dropped 4 (MinIO) and 6 (RustFS) pure ACKs from the store, and tshark saw 1 and 2 retransmissions. The exact rule was deleted, and a later transfer succeeded. |
| Capture | 36 and 32 packets captured and read back with tshark. |
| slow | Healthy GET takes 0.002 s. With the toxics, GET takes 1.51 s and a 256 KiB PUT takes 2.56 s, on both proxies. After recovery GET is back to 0.002 s and no toxics remain. |
| http503 | A real PUT got 503, and the access log shows 503 then 200. The control file was removed. |
| store_outage | A PUT fails while the store is stopped. After `recover()` a PUT gets 200. The store kept its IP, so no repoint was needed. |
| Restored state / cleanup | Clean. No labelled container or network remains. |

Totals: 60 s of probe work, 285 s including the lease wait. Evidence is in `docs/superpowers/reports/series-parquet-measurement/fault-preflight.json`, with one child. Raw artifacts are in `.measurement-artifacts/fault-preflight-raw-20260923-1105.tgz`.

## Coverage

- `SERIES_REQUIRE_FAULT_TOOLS=1` makes every probe required.
- All 8 fault classes are available: store_outage, slow, http503, disconnect_reset, dropped_completion_response, dns_nxdomain_timeout, tcp_ack_loss and containerized_engine.
- In optional mode, a failed probe marks only the dependent classes as unavailable ("skipped before traffic"). In required mode the lane fails.

## xt_bpf and the kernel

- xt_bpf was already loaded in /proc/modules when I first checked, at about 10:30 CEST. This was before any probe of mine ran, so the "lsmod shows none" observation is stale; someone, probably the user, loaded it.
- I ran no sudo or modprobe. The probe records the module state before and after.
- If the module is missing, the probe fails with `host_fix` "sudo modprobe xt_bpf", and only tcp_ack_loss becomes unavailable.
- The module load does not persist across a reboot.

## Tests

- `test_failures`: 33/33 OK (includes the live rig on MinIO and a real host tcpdump compile).
- `test_measurement`: 205/205 OK.
- `test_e2e.LocalSlice`: 2/2 OK against the release engine (checks the Engine hook).
- The full Docker E2E suite was not rerun.
- TDD note: the tests were written alongside the implementation, not strictly red first.

## Concerns

1. The worktree was created from main, not series-parquet-exporter. I reset it; merge the branch normally.
2. Merge surface with Tasks 4 and 6:
   - `test_e2e.py` Engine `__init__`: the port and bind hook, and the `self.launcher` line moved up.
   - Additive DockerStore methods.
   - One CASE_ROLES entry.
   - `measure.py`: the subcommand, and PLANNED_COMMANDS.
3. The engine container needs `overrides={"retry": test_e2e.S3_RETRY}`. The default retry_timeout of 180 s is refused against flush_retry_deadline of 60 s. Tasks 9-11 must pass it too.
4. For Task 9: while the store is stopped, requests through the route hang until the client read timeout (10 s in the probe) instead of being refused fast.
5. A restarted store may get a new IP. store_outage recovery repoints the proxies, but a Task 11 BPF rule compiled for the old IP must be recompiled.
6. The preflight's numbers are diagnostic bounds. No baseline is written or compared.
7. One short unpinned run: I ran the `test_failures` unittest once without taskset. About 3 s of an idle MinIO, NGINX and Toxiproxy could use all cores, including the TLA cores. Every preflight and engine launch ran under `taskset -c 0-7,16-23`.
8. I could not write this report to the main checkout (worktree isolation). The controller should copy it.
9. No host change is needed now.

## Fix round 1

Commits on branch series-parquet-task-8-faults:
- b45a76863 fix(series_parquet): tighten fault probes, coverage, teardown and provenance
- 8236c08c9 chore: re-measure the series fault preflight after fix round 1

The five review items:
1. The ACK-loss probe could pass on weak evidence.
   - It now sends a signed 256 KiB PUT through the route. Toxiproxy's upload to the store runs inside the namespace, so the ACK-only rule applies to it and the PUT stalls under the rule (5 s client timeout).
   - The probe passes only with all of: at least one dropped ACK, a non-empty capture, at least one retransmission, and, after the exact rule is deleted, a signed PUT with a 2xx status whose bytes read back. An unsigned 403 no longer counts.
   - The decision is a pure function, `ack_loss_problems`, with tests for zero captured packets, no retransmission, a 403, unverified bytes and zero drops.
   - The capture probe uses the same signed transfer.
2. `disconnect_reset` and `dropped_completion_response` are now unavailable.
   - They depend on direct probes (`DEFERRED_PROBES`) that do not exist yet.
   - The coverage says what Task 11 must show for each, including the negative control.
   - Tests check both classes stay unavailable even when every preflight probe passes.
3. Teardown and recovery:
   - `recover()` no longer pops a fault before its recovery succeeds. A failed recovery stays active, its attempts are recorded, and teardown retries it.
   - Teardown is nine independent stages: faults, namespace rules, proxies, control file, artifacts, containers, store attachment, network, leftovers. Each stage catches BaseException, and an interrupt is re-raised after all stages have run. Inside the container stage, each removal is also independent.
   - Containers are recorded only after Docker wrote their id to a cidfile, and are removed by that id. This covers the owner, Toxiproxy, the ldd check and the engine. The ContainerProcess signals the container by id and refuses to signal without one.
   - Tests cover removal by id, a failed recovery retried, Ctrl-C during container removal (the rest is still removed, then the interrupt is raised), and a failing proxy stage.
4. Images and engine provenance:
   - Every container runs from the inspected image id, not the tag.
   - The rig refuses a non-release engine before running anything. It uses the new `measurement.build_profile`, the same rule `engine_build` now uses, which runs no rustc so it cannot trip a build monitor.
   - The binary's sha256 and size are recorded in the ldd report. Tests cover the debug and custom refusal, the id-based ldd launch and the recorded hash.
5. Host paths in published JSON:
   - The scrub now also reduces the target side of `SOURCE:TARGET[:MODE]` bind specs. `--mount source=/target=` was already covered.
   - The rule is idempotent, and it also completes sources that an earlier scrub had already reduced to `<host-path>/x`.
   - The other 714 published files are unchanged by the new scrub (I checked all 716).
   - Both old indexes were rescrubbed with `measure rescrub`, and a new required run was published. The chain is now: new index, then fault-preflight-4b251adb1788.json (the rescrubbed previous index), then fault-preflight-1b5f36aabfb3.json.
   - The published tree contains 0 host paths.

Tests:
- `test_failures`: 46/46 (was 33; includes the live rig).
- `test_measurement`: 205/205.
- `test_e2e.LocalSlice`: 2/2.
- Required `measure fault-preflight` exited 0.
- Raw artifacts are in `.measurement-artifacts/fault-preflight-raw-20260923-fix1.tgz`.

New probe table (required mode; MinIO and RustFS gave the same results unless noted):

| Probe | Result |
| --- | --- |
| S3 route general / values | pass, about 0.3 s each |
| route isolation | pass |
| engine launch | pass: release, hash recorded, image by id; 1.6 s MinIO, 2.1 s RustFS |
| capabilities | pass: NET_ADMIN only in the owner |
| UDP DNS | pass: blocked 2.04 / 2.07 s, 1 dropped packet |
| TCP DNS | pass: blocked 2.05 / 2.07 s, 2 dropped packets |
| xt_bpf | pass: 7 dropped ACKs, 17 captured packets, 4 retransmissions (each store); stalled PUT timed out at 5 s; signed restore PUT got 200 with bytes verified |
| capture | pass: 101 / 103 packets of a signed 200 transfer |
| slow activation | pass, 8.2 s |
| http503 activation | pass |
| store_outage activation | pass, 11.6 s |
| restored state | pass |
| cleanup | pass: all 9 stages ok, no leftovers |

Coverage:
- Available: containerized_engine, dns_nxdomain_timeout, http503, slow, store_outage, tcp_ack_loss.
- Unavailable, pending Task 11: disconnect_reset, dropped_completion_response.

## Fix round 2

Commits: 8784bba8e fix(series_parquet): per-store fault coverage and store image by id; 924fc4658 chore: re-measure the series fault preflight with per-store coverage.

1. Coverage is decided per store.
   - A class is available for a store only when that store's own probes passed. A probe with no store, such as the image check, counts for every store.
   - A class is available overall only when it is available for every store the preflight covered. `preflight_fault_tools` now passes its store list explicitly.
   - Each class carries a `stores` map with status, failed, missing and deferred probes, and the consequence.
   - New tests: a failed RustFS rig next to fully passing MinIO leaves every class unavailable overall and on RustFS, while MinIO's own classes stay available; a listed store with no probes is unavailable.
2. The store starts from its image ID.
   - `DockerStore(kind, by_image_id=True)` is opt-in: it inspects the tag's image ID, starts the container from that ID and records both `image` and `image_id`.
   - Without the option, the store still starts from the tag, so the legacy 18-test suite is untouched and was not rerun.
   - The preflight and the live rig test opt in. Each rig records the image its store container really runs (`docker inspect .Image`), and the live test asserts it equals the inspected ID.
   - New tests: opted-in stores start from the ID; the legacy default keeps the tag.

Tests:
- `test_failures`: 50/50, including the live rig.
- `test_measurement`: 205/205.
- Required `fault-preflight` under `taskset -c 0-7,16-23` exited 0: 28/28 probes passed.
  - The published run's elapsed time is 1845 s because it waited about 30 min for the lease held by task-3g's run; probe work itself was about 75 s.

Stores and chain:
- Store images ran by ID: MinIO `sha256:9d668e47f1fc...`, RustFS `sha256:73ca1f01c7c8...`.
- Coverage per store: 6 classes available on both stores; `disconnect_reset` and `dropped_completion_response` unavailable on both, pending Task 11.
- Republished through the scrubber with 0 host paths. The chain is now: new index, then `fault-preflight-775a14488d52.json`, then `fault-preflight-4b251adb1788.json`, then `fault-preflight-1b5f36aabfb3.json`.
- Raw artifacts are in `.measurement-artifacts/fault-preflight-raw-20260923-fix2.tgz`.
