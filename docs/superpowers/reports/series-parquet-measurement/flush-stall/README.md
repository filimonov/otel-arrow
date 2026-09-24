# Flush-stall probe results (Task 3i)

`measurement --flush-stall` results for the four largest-default-block
fixtures, each with 3 uncancelled and 20 cancelled writes, under
`taskset -c 0-7,16-23` with the host lease held.

- `*-before.json`: library at fcceef306 (the probe commit, before the change).
- `*-sliced.json`: library at bbc888b3d (bounded slices).
- `*-final2.json`: library at 75732cafd (flush workspace accounting and one
  step per budget across runs and columns).
- `*-input-sidecar.json`, `*-bench-config.json`: the harness input and the
  bench configuration each run read. The configurations are written
  through the harness's `measurement.scrub_published`, so the run's scratch
  directory reads `<scratch>/...` instead of a host path; nothing else in
  them differs from what the probe read.
- `files-before.sha256`: SHA-256 of every Parquet file written by the
  library at fcceef306, the pre-task source (probe source of a223a4d25 +
  fcceef306). Reproduced byte for byte by the round-1 before build.
- `files-final.sha256`: the same for the library at 75732cafd.
- `files-head-545c9f038.sha256`: the same for the library at 545c9f038,
  after review fix round 1.
- `files-head-8d5be48eb.sha256`: the same for the library at 8d5be48eb,
  after review fix round 2 (columns built in place).
- `files-head-8f5dddc4a.sha256`: the same for the library at 8f5dddc4a,
  after review fix round 3 (element budget, every buffer presized).
- All five manifests are identical. They show that the output is the same
  before and after the change, on these four inputs. The unit test
  `sliced_merge_matches_the_unsliced_merge_and_a_stable_sort` shows
  something else: that the current merge's output does not depend on its
  step budget.
- `probe-binaries.sha256`: the three bench executables of the first round.

Review fix round 1 re-ran the comparison with the fixed probe. The probe
times the flush's observation of a cancellation at the sink's first clock
reading, where it takes the cleanup deadline, and makes 7 uncancelled and
20 cancelled writes per fixture.

- `*-round1-before.json`: library at fcceef306 with the probe source of
  545c9f038, its workspace reading disabled (the old sink has none).
- `*-round1-head.json`: library and probe at 545c9f038.
- `probe-binaries-round1.sha256`: the two executables.

Review fix round 2 re-ran the changed build only; the before side stays
round 1's. `*-round2-head.json` is the library and probe at 8d5be48eb,
with 7 uncancelled and 20 cancelled writes per fixture.
`probe-binaries-round2.sha256` is its executable.

Review fix round 3 re-ran the changed build only. `*-round3-head.json` is
the library and probe at 8f5dddc4a, with 7 uncancelled and 20 cancelled
writes per fixture. `probe-binaries-round3.sha256` is its executable.

The extraction and write speed changes (S6) re-ran the probe with 7
uncancelled and 20 cancelled writes per fixture on the same inputs.

- `*-s6-base-rerun.json`: library at cb3562b76, the S6 base, with the probe
  source of 9d4bc07c0 (it reports rows and pinned bytes per chunk), so the
  S6 build is compared with a same-day base.
- `*-s6-capped.json`: library and probe at 956adf0ae, on each input cut to
  the requests the base block admitted (2096, 3716, 813, 236), so both
  builds write blocks of the same requests.
- `*-s6-full.json`: library and probe at 956adf0ae on the whole inputs.
  Values batches now pin only the capacity they use, so a block admits
  requests until `window.max_block_bytes` really is reached.
- `files-capped-956adf0ae.sha256` and `files-head-956adf0ae.sha256`: the
  files of the capped and the full runs. The capped files hold the same
  rows in the same order as the base's, read row by row with DuckDB; their
  bytes differ because the merge sizes chunks from the runs' pinned bytes
  per row and mostly distinct columns are written without a dictionary.
- `probe-binaries-s6.sha256`: the two executables.
- 956adf0ae and 9d4bc07c0 are the commits as measured; rebased onto
  54bc1aa52 they are 14eea415c and f580bcdf0.

The admission path (Task 5) was timed with the probe of d8381c1f2 on the
library at 7e2268935, on the same four inputs, with 3 uncancelled writes and
no cancelled ones, under the pin with the host lease.

- `*-t5-admission.json`: the probe's result. `admission` holds the longest
  conversion plus extraction of one request, `Block::reserve`,
  `Block::admit` without and with a values-run seal at
  `sorting.run_target_bytes`, and `Block::seal` with its recount. The
  flush-side `max_gap_ns` of the same run is the comparison.
- `probe-binaries-t5-admission.sha256`: the executable. It was built before
  a formatting-only change to the probe source.
