### Task 3d: Format batch -- reproducible fingerprint, native sort metadata, OTLP-JSON spellings

**Origin:** umbrella finding 3, consistency findings C16, C17, C22; approved by the user on 2026-09-22 to land once, before first release. All three touch persisted bytes or golden vectors, so they ship in one commit series with one FORMAT.md revision and regenerated golden files.

**Expected wall-clock cost:** 2-4 hours; unit and golden tests only; one E2E run to prove readers still agree.

- [ ] **Fingerprint:** render a crate-owned, versioned type vocabulary modelled on pdata's `SchemaIdBuilder` type codes (not Arrow `Display`), include top-level nullability, pin golden fingerprints for all four datasets plus one denormalized schema, and have `gen_golden.py` compute them independently in Python. FORMAT.md documents the exact rendering. A test asserts the documented rendering equals the code's.
- [ ] **Native sort metadata:** emit Parquet's `SortingColumn` row-group metadata via `WriterProperties::set_sorting_columns` beside the private `sort_key`; reuse the `sort_columns` name from pdata `consts` or record in FORMAT.md why it differs. Verify DuckDB and ClickHouse read the files unchanged.
- [ ] **Spellings:** `render_v1` spells non-finite doubles `Infinity`/`-Infinity`/`NaN` and bytes as base64, matching the workspace OTLP JSON, with golden vectors for each; FORMAT.md updated.
- [ ] **Golden hashes move deliberately:** regenerate, review the diff of every golden file, and record the old and new fingerprints in FORMAT.md's revision note. This is the one sanctioned golden move; the plan-2 rule "no golden hash moved" resumes afterwards.
- [ ] **Commit** per item, staged by name, one `breaking`-style chloggen note (pre-release format change), `cargo xtask check`, one E2E run.

