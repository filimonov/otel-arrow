# Flush-stall probe results (Task 3i)

`measurement --flush-stall` results for the four largest-default-block
fixtures, each with 3 uncancelled and 20 cancelled writes, under
`taskset -c 0-7,16-23` with the host lease held.

- `*-before.json`: library at fcceef306 (the probe commit, before the change).
- `*-sliced.json`: library at bbc888b3d (bounded slices).
- `*-final2.json`: library at 75732cafd (flush workspace accounting and one
  step per budget across runs and columns).
- `*-input-sidecar.json`, `*-bench-config.json`: the harness input and the
  bench configuration each run read.
- `files-before.sha256`, `files-final.sha256`: SHA-256 of every Parquet
  file each build wrote for the same input; they are identical.
- `probe-binaries.sha256`: the three bench executables.
