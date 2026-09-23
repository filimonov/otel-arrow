# Task 3d report: format batch (fingerprint, native sort metadata, render_v1 spellings)

Status: done. Four commits on `series-parquet-exporter`, not pushed.

| Commit | Subject |
|---|---|
| 7c9c75f2c | test(series-lake): regenerate golden vectors for format revision 2 |
| b04c50365 | fix(series-lake): schema_fingerprint hashes a crate-owned type vocabulary (A) |
| a07b2c385 | feat(series-lake): write Parquet's native SortingColumn row-group metadata (B) |
| 0979ac0b1 | fix(series-lake): render_v1 spells non-finite doubles and bytes as OTLP JSON does (C) |

Commit order: the golden regeneration commit comes first so that every commit
builds and passes. It adds the generator changes, the two new golden files,
the FORMAT.md revision note and the chloggen entry. Nothing reads the new
golden files until A and C land, so the existing tests still pass at that
commit. A, B and C each carry their own code, tests and FORMAT.md section.

## A. Fingerprint (umbrella finding 3, consistency C17)

Design decisions:

- **Vocabulary.** `series-lake-schema/1`, owned by the crate and modelled on
  pdata's `SchemaIdBuilder` codes: `Bol I32 I64 F64 Str Bin FSB<n>`,
  `Ts<us,UTC>`, `[item]` for lists and `Map<key,value>` for maps. `I8 I16 U8
  U16 U32 U64 F32` and other timestamp units and time zones are reserved.
  Anything else renders `Unk<>`, and a unit test asserts that no dataset
  column hits it. Arrow `Display` is no longer used.
- **Nullability.** Every type carries `!` (required) or `?` (nullable), at
  the top level and on list items and map keys and values.
- **Versioning.** The rendering starts with the line `series-lake-schema/1`.
- **Ordering.** Columns stay in column order, not sorted by name as in
  `SchemaIdBuilder`. Column position is part of the physical Parquet schema,
  and a compaction that trusts equal fingerprints may only concatenate row
  groups whose columns are in the same order. FORMAT.md says so.
- **Serialization.** The header is followed by one line per field,
  `<len>:<name><len>:<type>` plus LF. The length prefixes are kept, so the
  delimiter-collision guarantee still holds. The fingerprint is xxh3_64,
  seed 0, over the UTF-8 bytes.
- **Documentation.** FORMAT.md section 3 has the grammar and the full
  rendering and fingerprint of each golden schema.
- **Public API.** `schema::schema_rendering` and
  `schema::SCHEMA_RENDERING_HEADER` are new.

Golden fingerprints:

| schema | revision 1 (Arrow Display) | revision 2 |
|---|---|---|
| logs/series | 02676af0998114ce | b94def7e47c943f8 |
| logs/values | af2f139c9bdf7b07 | 01512d4eb446f355 |
| metrics/series | 675a271eae033992 | e7c5410e543a8cc4 |
| metrics/values | 9e850a2cc126ce60 | 0898a0f9608155f1 |
| logs/values+denormalized | 52863aaef23bfabc | a4585b65682804bf |

The denormalized schema is logs/values with four denormalized columns, one
per denormalize type:

- `resource.service.name` as string
- `attrs.http.status_code` as int64
- `attrs.latency` as double
- `attrs.cached` as bool

The revision 1 values were computed with the pre-change code through a
throwaway test before any edit. The same table is in FORMAT.md's revision
history.

Tests:

- **gen_golden.py.** Computes each rendering and fingerprint in Python from
  column tables transcribed from FORMAT.md section 2. It writes them to
  `tests/golden/schema_fingerprint_v1.json`.
- **`golden.rs::schema_fingerprints_match_the_golden_vectors`.** The Rust
  rendering and fingerprint equal the Python vectors for all five schemas.
  It also validates each golden configuration.
- **`golden.rs::format_md_documents_every_schema_rendering`.** Parses
  FORMAT.md with `include_str!`. For each schema it finds the label line
  that carries the name and fingerprint, then asserts that the following
  text block equals `schema_rendering` byte-for-byte. This is the test that
  the documented rendering equals the code's.
- **`schema.rs` unit tests.** `golden_fingerprint_of_the_default_logs_values_schema`
  is updated to the new value. `every_dataset_column_type_is_in_the_vocabulary`
  and `nullability_changes_the_fingerprint` are new, the second covering both
  the top level and a list item. `delimiter_bearing_names_do_not_collide` is
  kept, respelled with `Str`.

## B. Native sort metadata (consistency C16)

Design decisions:

- **Where it is set.** `sink::native_sorting_columns` builds the list from
  the table's `SortSpec` and the dataset schema. `write_table` passes it to
  `WriterProperties::set_sorting_columns`, so every row group gets it.
- **Column index.** `column_idx` is the Parquet leaf index, found by
  converting the Arrow schema with `ArrowSchemaConverter` and matching a
  single-part leaf path. A map column has two leaves, so later columns shift.
- **Which keys.** The list covers the longest prefix of sort keys that are
  top-level primitive columns. A list-typed key ends the prefix, because the
  row converter can sort a list but no leaf order describes it. An empty
  prefix or an unsorted file writes no field.
- **Metadata name.** The `sort_key` key/value keeps its name and stays
  authoritative. The native field is a footer structure with no key name.
  FORMAT.md section 5 records why `sort_key` is not pdata's
  `metadata::SORT_COLUMNS`. That constant is Arrow record-batch schema
  metadata, has no writer and no value grammar, while `sort_key` has a
  defined grammar that includes order and null placement.
- **Double caveat.** The writer orders `-0.0` equal to `+0.0` and puts every
  NaN after `+Infinity`. FORMAT.md states it for readers of the native field.

Tests:

- **`sink::tests::native_sorting_columns_use_leaf_indexes_and_a_primitive_prefix`.**
  A key after the `attrs` map is Arrow field 14 but leaf 15. Descending and
  nulls-first carry over. A list key in the middle truncates the list, a
  list key first gives `None`, and no keys give `None`.
- **`sink::tests::every_row_group_carries_the_native_sorting_columns`.**
  Writes a real file with row groups forced to a few rows, so there is more
  than one row group, and reads the row-group metadata back. Every values
  row group has `[(0, asc, nulls last), (3, asc, nulls last)]`, matching
  `sort_key`, and every series row group has `[(0, asc, nulls last)]`. With
  values sorting disabled, `sort_key` is `none` and there is no native list,
  while the series file keeps its list.
- **Readers.** The full E2E run reads these files through DuckDB and
  clickhouse-local and passes, 18 of 18.

## C. render_v1 spellings (consistency C22)

Design decisions:

- **Doubles.** Any NaN renders as `"NaN"`, whatever its sign or payload.
  Positive and negative infinity render as `"Infinity"` and `"-Infinity"`.
- **Bytes.** Bytes render as standard base64 with padding, using pdata's
  engine, `base64::engine::general_purpose::STANDARD`, so the spelling is
  the same as `crates/pdata/src/otlp/json/common.rs`.
- **Not OTLP JSON.** FORMAT.md section 2 says `render_v1` is still not OTLP
  JSON. There is no `AnyValue` wrapper, integers are JSON numbers, and
  kvlists are objects.
- **Row budget.** The row-size budget charges the rendered size. Its comments
  and two tests changed from hex's doubling to base64's four characters per
  three bytes. `a_bytes_attribute_is_charged_its_rendered_base64_size` now
  uses 900 KiB instead of 600 KiB, because base64 of 600 KiB no longer
  exceeds the 1 MiB row limit.
- **Docs.** The series-lake README and the exporter README bytes wording is
  updated.

Tests:

- **gen_golden.py.** Has an independent Python `render_v1` and writes 20
  vectors to `tests/golden/render_v1.json`. They cover:
  - every scalar kind, and doubles `1.5`, `3.0` and `-0.0`
  - quiet NaN and NaN with a payload, and both infinities
  - bytes of each base64 padding length (0, 1, 2 and 3 bytes)
  - the `+` and `/` alphabet characters
  - a nested array holding all the special spellings
  - a kvlist with sorted and non-ASCII keys
- **`golden.rs::render_v1_matches_the_golden_vectors`.** Both `map_string`
  and `body_string` equal every vector.
- **`value.rs` unit tests.** Updated to the new spellings.

Golden protocol for all goldens:

- **Regeneration.** `gen_golden.py` now takes the golden directory as its
  argument.
- **Diff review.**
  - `canonical_v1.json` is byte-identical after regeneration and has no
    diff.
  - `schema_fingerprint_v1.json` and `render_v1.json` are new files. Both
    were reviewed and are ASCII-only.
  - Rerunning the generator at HEAD leaves the tree clean.
- **Revision note.** FORMAT.md gains a "Revision history" section. Revision
  2 is dated 2026-09-23 and marked pre-release. `format_version` stays `1`
  because no released data exists.
- **Chloggen.** `.chloggen/series-lake-format-revision-2.yaml` is
  `change_type: breaking`, `component: pipeline`, `issues: [4128]` with the
  same placeholder comment as the other entries. The subtext has a
  `Migration:` line saying no released data exists.

## Did any series id change

No. Series identity hashes the typed canonical encoding (FORMAT.md section
1), which never calls `render_v1`. `canonical_v1.json`, which holds every
`identity_bytes` and `series_id` vector, regenerated byte-identical, and
`golden_vectors_match` plus the round-trip test pass unchanged. FORMAT.md
section 2 and the revision note say this explicitly. What changes is the
stored strings: attribute-map cells, string-typed denormalized cells and log
bodies holding a non-finite double or a bytes value.

## Verification

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy -p otel-arrow-dfe-series-lake --all-targets -- -D warnings` | clean |
| `cargo clippy -p otel-arrow-dfe-core-nodes --features series-parquet --all-targets -- -D warnings` | clean |
| `cargo test -p otel-arrow-dfe-series-lake` | lib 148 passed; golden 6, fuzz and oracle binaries all passed |
| `cargo test -p otel-arrow-dfe-core-nodes --features series-parquet` | 1192 passed, plus 1 doc test |
| Contract tests, `test_measurement` via the venv | 184 passed |
| E2E `test_e2e`, `SERIES_REQUIRE_DOCKER=1`, `taskset -c 0-7,16-23`, debug `df_engine` rebuilt with `series-parquet,aws,durable-buffer` at 0979ac0b1 | 18 passed, 0 skipped, 152 s |
| `cargo xtask check` | passed ("All tests passed successfully") |
| ASCII check on every touched text file | clean |

No measured run was in flight: `pgrep -af "measure "` was empty before
each build.

## Deviations

- **Commit order.** The golden regeneration commit is first, not last, so
  that every commit in the series builds and passes. The revision note and
  chloggen entry ride in it, and it describes B and C one and two commits
  ahead of their code.
- **Cargo.lock.** It changed by one line, adding the dependency edge from
  series-lake to `base64 0.23.1`, which the workspace already locks for
  pdata. No new package was added. The file was staged explicitly in
  commit C. It changed because series-lake reuses the workspace base64
  crate rather than hand-rolling an encoder.
- **Non-ASCII in canonical_v1.json.** The file still contains 3 non-ASCII
  characters in its Unicode vectors, as it did before this task. The
  generator keeps `ensure_ascii=False` for that one file so regeneration
  stays byte-identical. The new golden files use `ensure_ascii=True`.
- **The `sort_key` name is kept.** This is the brief's second option, with
  the reason recorded in FORMAT.md, rather than a rename to `sort_columns`.
- **tools/__pycache__.** The untracked `tools/__pycache__` directory left by
  running the generator was removed.

## Not bounded

- **Finite-double exponent spelling.** `render_v1` follows serde_json/ryu, so
  1e16 prints as `1e16`, while Python's repr gives `1e+16`. The golden
  vectors therefore pin only plain decimals, and FORMAT.md still says only
  "shortest round-trip".
- **Native sorting field on doubles.** Readers that trust the field for a
  double column get the writer's `-0.0 == +0.0` and NaN-last order. The
  Parquet spec leaves float ordering in `SortingColumn` undefined. This is
  documented but not tested against a reader that uses the field for
  pruning.
- **Reader use of the field.** The E2E suite proves DuckDB and ClickHouse
  still read the files. It does not show that either engine uses the native
  field; no test observes a reader skipping a sort because of it.
- **No CI regeneration check.** No CI step reruns `gen_golden.py` and diffs
  the output, which the umbrella review suggested. Drift is caught only
  through the Rust golden tests.
- **Pre-release lakes.** Lakes written by earlier pre-release builds carry
  revision 1 fingerprints and spellings. The chloggen and FORMAT.md say to
  rewrite them under a new `base_uri`, and nothing enforces this.

## Fix round 1

The review verdict was "Needs fixes", with no Critical item. The three items
it raised are fixed in three commits, not pushed.

| Commit | Item |
|---|---|
| 68df821d8 | 1. fix(series-lake): never declare a floating-point sort key in native SortingColumn |
| eee0f30d7 | 3. test(series-lake): pin the layout and exponent spelling of finite doubles in render_v1 |
| 936ace03a | 2. ci(series_parquet): fail when gen_golden.py does not reproduce the checked-in goldens |

Item 1 was committed before the session-limit pause and carries the Fable
trailer. The two commits after the pause carry the Opus 5.5 trailer, as
instructed.

### 1. Floating-point keys end the native list

- **Change.** `native_sorting_columns` now stops at the first key whose
  Arrow type is floating-point, as it already stopped at a list key.
  Nothing after that key is declared.
- **Why.** The merge sorts doubles on a normalized copy, in which `-0.0`
  equals `+0.0` and every NaN equals every other NaN. Parquet's recommended
  IEEE 754 total order puts `-0.0` before `+0.0` and gives NaN payloads
  distinct positions, so declaring a double column sorted was false.
  `sort_key` stays the complete, authoritative description.
- **FORMAT.md.**
  - Section 5 now says the native list covers only the leading keys that are
    neither lists nor floating-point, and explains why for each.
  - The earlier caveat telling readers to "accept" the double order is
    removed.
  - The revision note says the same.
- **Unit test.** The leaf-index test now covers a double key in the middle
  of the spec, which truncates the list to `series_id`, and a double key
  first, which gives no list.
- **File test.** The row-group test also writes a file sorted by
  `series_id`, a denormalized double column `latency` and `time_unix_nano`.
  `sort_key` names all three keys, and every row group declares only
  `[(0, asc, nulls last)]`.

### 3. Finite-double spelling pinned

Correction to the brief's premise: the writer does not print `1e16`. A
throwaway test, deleted afterwards, measured serde_json 1.0.151, which
formats floats through zmij:

| value | serde_json | Python repr |
|---|---|---|
| 1e16 | `1e+16` | `1e+16` |
| 1e15 | `1000000000000000.0` | same |
| 1e-7 | `1e-7` | `1e-07` |
| 1e-6 | `1e-6` | `1e-06` |
| 1e-5 | `0.00001` | `1e-05` |
| 1.23e-5 | `0.0000123` | `1.23e-05` |
| 5e-324 | `5e-324` | same |

- **Rule.** Fixed notation when the decimal exponent of the first digit is
  in -5..=15. Scientific notation otherwise, with an explicit `+` or `-` on
  the exponent and no zero padding. Fixed notation appends `.0` when there
  is no fractional digit.
- **Generator.** The goldens pin the writer's real spelling, `1e+16`, not
  the `1e16` the brief expected. `gen_golden.py` now lays the digits out
  itself: it takes the shortest digits from repr and applies the rule,
  instead of trusting `json.dumps`.
- **Vectors.** Ten new vectors bring the render set to 30:
  - `1e16` and `-1e16`, and `1e15` at the fixed limit
  - `1.2345678901234568e16` and the largest double
  - `1e-7`, `1.23e-6`, and `1e-5` at the fixed limit
  - `1.23e-5` and the smallest subnormal
- **FORMAT.md.** Section 2 specifies the rule with examples. The revision
  note records that revision 1 already produced this spelling but never
  documented it, so no stored string changes from this item.
- **Effect.** A serde_json or zmij upgrade that changes the spelling now
  fails the goldens instead of silently changing stored strings.

### 2. CI regeneration check

- **Where.** The new step "Golden vectors regenerate unchanged" is in the
  `writer-reader-matrix` job of `.github/workflows/series-parquet-e2e.yml`,
  which runs on pull requests touching series-lake. It sits right after the
  pinned Python dependency install and before the engine build.
- **What it runs.** The same command I ran locally from
  `rust/otap-dataflow`:

  ```bash
  out="$(mktemp -d)"
  /tmp/series-parquet-venv/bin/python3 crates/series-lake/tools/gen_golden.py "$out"
  diff -ru crates/series-lake/tests/golden "$out"
  ```

- **Local result.** Exit 0 with no diff. As a negative check, overwriting
  `render_v1.json` in the temporary directory made `diff` exit 1. The
  workflow parses as YAML.
- **Dependency.** `xxhash==3.8.1` is already in the hashed
  `requirements.lock.txt` that the job installs.

### Verification at 936ace03a

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| clippy `-D warnings`, series-lake all targets | clean |
| clippy `-D warnings`, core-nodes `--features series-parquet` all targets | clean |
| `cargo test -p otel-arrow-dfe-series-lake` | lib 148, golden 6, and all other test binaries passed |
| Contract tests, `test_measurement` | 184 passed |
| E2E, `SERIES_REQUIRE_DOCKER=1`, `taskset -c 0-7,16-23`, debug `df_engine` rebuilt at HEAD | 18 passed, 0 skipped, 146 s |
| `cargo xtask check` | passed ("All tests passed successfully") |
| CI golden regeneration command | no diff |

The E2E run reads files whose native list is now shorter through DuckDB and
clickhouse-local, and both still read them. The default sort has no double
key, so default files are unchanged: values files still declare leaves 0
and 3.

### Not bounded, updated

- **Resolved.** The finite-double exponent spelling, the native field's
  double caveat and the missing CI regeneration check are all fixed.
- **Still open.** No test shows DuckDB or ClickHouse actually using the
  native field; the E2E run only shows they still read the files.
- **Still open.** Lakes written by pre-release builds carry revision 1
  fingerprints and spellings, and nothing enforces rewriting them.
