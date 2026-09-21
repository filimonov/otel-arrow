# Series Lake Core (`series-lake` crate) Implementation Plan

<!-- markdownlint-disable MD013 MD032 MD031 MD040 MD024 MD033 MD046 MD029 MD004 -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the engine-independent `series-lake` crate: canonical series identity, extraction of series/values from OTAP Arrow records, the series cache, sorted block buffers, the Parquet sink to an object store, and the window clock arithmetic, all covered by golden vectors, a reference-oracle property test and fuzz-style tests.

**Architecture:** One library crate under `rust/otap-dataflow/crates/series-lake` that depends on `otel-arrow-dfe-pdata`, `arrow`, `parquet` and `object_store` but never on the Dataflow engine. Input is `OtapArrowRecords`; output is Parquet files in the Hive layout of the spec. The exporter node (plan 2) wraps this crate.

**Tech Stack:** Rust 2024 edition (MSRV 1.88), arrow 58.3, parquet 58.3, object_store 0.13.2, xxhash-rust (xxh3), ciborium 0.2, lru 0.12 (new workspace dependency), proptest 1 (new dev dependency), tokio, tokio-util (CancellationToken), serde, serde_json, uuid.

**Spec:** `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md` (revision 4). This plan implements section 11 step 1. Plan 2 (exporter node) and plan 3 (benchmarks, gates) follow.

## Global Constraints

- Workspace root for all `cargo` commands: `rust/otap-dataflow`. Run `cargo check -p otel-arrow-dfe-series-lake` after every Rust change and `cargo xtask quick-check` before each commit.
- Rust source must be ASCII-only (CI `tools/sanitycheck.py`). Use `--`, `->`, straight quotes in comments.
- Every test carries two doc-comment lines directly above it: `/// Scenario: ...` and `/// Guarantees: ...`.
- Workspace lints are strict: `missing_docs = "deny"`, `unwrap_used = "deny"`, `unused_results = "deny"`, `print_stdout = "deny"`. In non-test code use `expect` only with a message explaining why it cannot fail, prefer `?`. Assign `let _ = ...` for ignored results.
- Crate package name must start with `otel-arrow-dfe-`; the crate needs `README.md` and `[lints] workspace = true` (checked by `cargo xtask structure-check`).
- Changelog: add `rust/otap-dataflow/.chloggen/series-lake-core.yaml` with `change_type: new_component`, `component: pipeline` in the last task.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Work on branch `series-parquet-exporter`.
- Invariants of spec section 1.1 apply to every task: no persistent local state; the cache is never correctness state; sorting is an optimization.

## File Structure

```text
rust/otap-dataflow/crates/series-lake/
  Cargo.toml                 package otel-arrow-dfe-series-lake
  README.md                  overview, links to FORMAT.md
  docs/FORMAT.md             implementation-independent format spec (task 13)
  src/lib.rs                 module list, re-exports
  src/error.rs               Error / RefuseReason
  src/value.rs               owned Value tree, CBOR decode, render_v1, map/body strings
  src/canonical.rs           Descriptor, canonical bytes, SeriesId
  src/attrs.rs               reading OTAP attribute batches into per-parent sorted lists
  src/config.rs              LakeConfig, SignalConfig, Denormalize, SortKey, budgets
  src/schema.rs              Dataset enum, Arrow schemas per dataset (with denormalized columns)
  src/extract/mod.rs         Extracted, DescriptorRow, shared helpers, budgets
  src/extract/logs.rs        logs extraction
  src/extract/metrics.rs     metrics extraction (number, histogram)
  src/cache.rs               SeriesCache (LRU series_id -> partition)
  src/sort.rs                SortSpec, key normalization, sort_batch, k-way merge
  src/buffer.rs              SortedTableBuffer, Block<T>
  src/clock.rs               WallClock, PartitionId, boundary arithmetic
  src/sink.rs                paths, file names, Parquet writing, cancellation
  tests/golden/*.json        canonical vectors (task 3)
  tests/golden.rs            golden vector test
  tests/oracle.rs            reference-oracle property test (task 12)
  tools/gen_golden.py        independent Python generator for vectors
```

Each module has one responsibility; `extract` is the only module that reads OTAP column layouts, `sink` the only one that touches `object_store`/`parquet`.

---

### Task 1: Crate scaffold and workspace wiring

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/Cargo.toml`
- Create: `rust/otap-dataflow/crates/series-lake/README.md`
- Create: `rust/otap-dataflow/crates/series-lake/src/lib.rs`
- Create: `rust/otap-dataflow/crates/series-lake/src/error.rs`
- Modify: `rust/otap-dataflow/Cargo.toml` (workspace dependencies)

**Interfaces:**
- Produces: `series_lake::error::{Error, RefuseReason, Result}` used by every later task.

- [ ] **Step 1: Add workspace dependencies**

In `rust/otap-dataflow/Cargo.toml` under `[workspace.dependencies]`, add (alphabetically among the existing entries):

```toml
otel-arrow-dfe-series-lake = { version = "0.56.0", path = "crates/series-lake" }
lru = "0.12"
proptest = "1.5"
```

`ciborium`, `xxhash-rust`, `arrow`, `parquet`, `object_store`, `serde`, `serde_json`, `tokio`, `tokio-util`, `uuid`, `thiserror` already exist as workspace deps (verify with `grep -n '^thiserror\|^serde_json\|^serde ' Cargo.toml`; if `thiserror` is absent, add `thiserror = "2"`).

- [ ] **Step 2: Create `Cargo.toml`**

```toml
[package]
name = "otel-arrow-dfe-series-lake"
version.workspace = true
authors.workspace = true
edition.workspace = true
repository.workspace = true
license.workspace = true
readme = "README.md"
publish.workspace = true
rust-version.workspace = true
description = "Series/values normalization and Parquet sink for OTAP telemetry"

[features]
default = []

[dependencies]
otel-arrow-dfe-pdata.workspace = true
arrow.workspace = true
parquet.workspace = true
object_store.workspace = true
ciborium.workspace = true
xxhash-rust.workspace = true
lru.workspace = true
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
thiserror.workspace = true
tokio.workspace = true
tokio-util.workspace = true
uuid.workspace = true
humantime-serde.workspace = true
futures.workspace = true

[dev-dependencies]
otel-arrow-dfe-pdata = { workspace = true, features = ["testing"] }
proptest.workspace = true
tokio = { workspace = true, features = ["macros", "rt"] }
tempfile.workspace = true

[lints]
workspace = true
```

If `tempfile` or `futures` is not a workspace dependency, add `tempfile = "3"` / `futures = "0.3"` to `[workspace.dependencies]`.

- [ ] **Step 3: Create `README.md`**

```markdown
# series-lake

Engine-independent core of the series Parquet exporter: canonical series
identity, extraction of `series` and values datasets from OTAP Arrow
records, a bounded series cache, sorted block buffers and a Parquet sink
over `object_store`.

The storage format is specified in [docs/FORMAT.md](docs/FORMAT.md). The
design is in `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md`
at the repository root.

This crate never depends on the Dataflow engine; the `exporter:series_parquet`
node in `core-nodes` is a thin adapter over it.
```

- [ ] **Step 4: Create `src/error.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types shared by the crate.

/// Why a request is permanently refused (spec section 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefuseReason {
    /// Input, extracted output, a row or the block reservation exceeds a budget.
    TooLarge,
    /// Malformed content: duplicate keys, nesting too deep, bad histogram, ...
    Invalid(String),
    /// Unsupported signal or point kind under the reject policy.
    Unsupported(String),
}

/// Crate error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request must be nacked as non-retryable.
    #[error("refused: {0:?}")]
    Refused(RefuseReason),
    /// Arrow failure.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet failure.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Object store failure.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// pdata failure.
    #[error("pdata: {0}")]
    Pdata(String),
    /// Flush cancelled.
    #[error("cancelled")]
    Cancelled,
}

/// Crate result.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Shorthand for an invalid-content refusal.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Refused(RefuseReason::Invalid(msg.into()))
    }
}
```

- [ ] **Step 5: Create `src/lib.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Series/values normalization and Parquet sink for OTAP telemetry.
//!
//! See `docs/FORMAT.md` for the storage format and the design spec at the
//! repository root for the architecture.

pub mod error;

pub use error::{Error, RefuseReason, Result};
```

- [ ] **Step 6: Verify it builds and passes structure checks**

Run: `cd rust/otap-dataflow && cargo check -p otel-arrow-dfe-series-lake && cargo xtask structure-check`
Expected: both succeed (no warnings about missing README or lints).

- [ ] **Step 7: Commit**

```bash
git add rust/otap-dataflow/Cargo.toml rust/otap-dataflow/Cargo.lock rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): scaffold engine-independent series-lake crate

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `Value` tree, CBOR decoding and `render_v1`

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/value.rs`
- Modify: `rust/otap-dataflow/crates/series-lake/src/lib.rs` (add `pub mod value;`)

**Interfaces:**
- Produces:
  - `pub enum Value { Null, Str(String), Bytes(Vec<u8>), Int(i64), Double(f64), Bool(bool), Array(Vec<Value>), KvList(Vec<(String, Value)>) }`
  - `pub fn decode_cbor(bytes: &[u8], max_depth: usize) -> Result<Value>` (kvlist keys sorted by bytes, duplicate key -> `Error::invalid`)
  - `pub fn render_v1(v: &Value) -> serde_json::Value`
  - `pub fn map_string(v: &Value) -> Option<String>` (attribute map entry point; `None` for `Null`)
  - `pub fn body_string(v: &Value) -> Option<String>` (log body entry point)
  - `pub fn value_bytes(v: &Value) -> usize` (approximate retained size, for `max_row_bytes`)

- [ ] **Step 1: Write failing tests** (append to `src/value.rs` as `#[cfg(test)] mod tests`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a CBOR map with unsorted keys and nested array is decoded.
    /// Guarantees: keys come out sorted by raw bytes and nesting is preserved.
    #[test]
    fn decode_cbor_sorts_keys() {
        let mut buf = Vec::new();
        let v = ciborium::Value::Map(vec![
            (ciborium::Value::Text("b".into()), ciborium::Value::Integer(2.into())),
            (
                ciborium::Value::Text("a".into()),
                ciborium::Value::Array(vec![ciborium::Value::Bool(true), ciborium::Value::Null]),
            ),
        ]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        let got = decode_cbor(&buf, 32).expect("decode");
        assert_eq!(
            got,
            Value::KvList(vec![
                ("a".into(), Value::Array(vec![Value::Bool(true), Value::Null])),
                ("b".into(), Value::Int(2)),
            ])
        );
    }

    /// Scenario: a CBOR map repeats a key.
    /// Guarantees: decoding refuses the input as invalid.
    #[test]
    fn decode_cbor_rejects_duplicate_keys() {
        let mut buf = Vec::new();
        let v = ciborium::Value::Map(vec![
            (ciborium::Value::Text("k".into()), ciborium::Value::Integer(1.into())),
            (ciborium::Value::Text("k".into()), ciborium::Value::Integer(2.into())),
        ]);
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(matches!(decode_cbor(&buf, 32), Err(Error::Refused(RefuseReason::Invalid(_)))));
    }

    /// Scenario: arrays nested deeper than the limit.
    /// Guarantees: decoding refuses instead of recursing without bound.
    #[test]
    fn decode_cbor_rejects_deep_nesting() {
        let mut v = ciborium::Value::Null;
        for _ in 0..5 {
            v = ciborium::Value::Array(vec![v]);
        }
        let mut buf = Vec::new();
        ciborium::into_writer(&v, &mut buf).expect("encode test cbor");
        assert!(decode_cbor(&buf, 3).is_err());
        assert!(decode_cbor(&buf, 5).is_ok());
    }

    /// Scenario: render_v1 over every scalar kind and a nested kvlist.
    /// Guarantees: the JSON mapping of spec section 5.1 is produced exactly.
    #[test]
    fn render_v1_mapping() {
        let v = Value::KvList(vec![
            ("b".into(), Value::Bytes(vec![0xab, 0x12])),
            ("d".into(), Value::Double(f64::NAN)),
            ("e".into(), Value::Double(1.5)),
            ("i".into(), Value::Int(42)),
            ("n".into(), Value::Null),
            ("s".into(), Value::Str("x".into())),
            ("t".into(), Value::Bool(true)),
        ]);
        let json = serde_json::to_string(&render_v1(&v)).expect("json");
        assert_eq!(
            json,
            r#"{"b":"ab12","d":"NaN","e":1.5,"i":42,"n":null,"s":"x","t":true}"#
        );
    }

    /// Scenario: map and body entry points on top-level scalars.
    /// Guarantees: strings are raw, null is None, other scalars are compact JSON.
    #[test]
    fn map_and_body_entry_points() {
        assert_eq!(map_string(&Value::Str("raw \"q\"".into())).as_deref(), Some("raw \"q\""));
        assert_eq!(map_string(&Value::Null), None);
        assert_eq!(map_string(&Value::Int(42)).as_deref(), Some("42"));
        assert_eq!(map_string(&Value::Bytes(vec![0xab])).as_deref(), Some("\"ab\""));
        assert_eq!(map_string(&Value::Double(f64::INFINITY)).as_deref(), Some("\"inf\""));
        assert_eq!(body_string(&Value::Str("hello".into())).as_deref(), Some("hello"));
        assert_eq!(body_string(&Value::Null), None);
        assert_eq!(
            body_string(&Value::Array(vec![Value::Int(1)])).as_deref(),
            Some("[1]")
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake value::tests`
Expected: compile error (module missing).

- [ ] **Step 3: Implement `src/value.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Owned attribute value tree, CBOR decoding of the OTAP `ser` column and the
//! `render_v1` storage rendering (spec section 5.1).

use crate::error::{Error, Result};

/// An OTLP AnyValue in owned form.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Unset value.
    Null,
    /// UTF-8 string.
    Str(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// 64-bit integer.
    Int(i64),
    /// 64-bit float.
    Double(f64),
    /// Boolean.
    Bool(bool),
    /// Array of values.
    Array(Vec<Value>),
    /// Key/value list, sorted by raw key bytes, keys unique.
    KvList(Vec<(String, Value)>),
}

/// Decode a CBOR blob from the OTAP `ser` column into a [`Value`].
///
/// Kvlist keys are sorted by raw bytes; duplicate keys and nesting deeper
/// than `max_depth` are refused as invalid content.
pub fn decode_cbor(bytes: &[u8], max_depth: usize) -> Result<Value> {
    let raw: ciborium::Value = ciborium::from_reader(bytes)
        .map_err(|e| Error::invalid(format!("cbor decode: {e}")))?;
    convert(raw, max_depth)
}

fn convert(raw: ciborium::Value, depth_left: usize) -> Result<Value> {
    Ok(match raw {
        ciborium::Value::Null => Value::Null,
        ciborium::Value::Bool(b) => Value::Bool(b),
        ciborium::Value::Integer(i) => {
            let i: i64 = i.try_into().map_err(|_| Error::invalid("cbor int out of i64"))?;
            Value::Int(i)
        }
        ciborium::Value::Float(f) => Value::Double(f),
        ciborium::Value::Text(s) => Value::Str(s),
        ciborium::Value::Bytes(b) => Value::Bytes(b),
        ciborium::Value::Array(items) => {
            if depth_left == 0 {
                return Err(Error::invalid("cbor nesting too deep"));
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(convert(item, depth_left - 1)?);
            }
            Value::Array(out)
        }
        ciborium::Value::Map(entries) => {
            if depth_left == 0 {
                return Err(Error::invalid("cbor nesting too deep"));
            }
            let mut out: Vec<(String, Value)> = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                let key = match k {
                    ciborium::Value::Text(s) => s,
                    ciborium::Value::Null => String::new(),
                    _ => return Err(Error::invalid("cbor map key is not text")),
                };
                out.push((key, convert(v, depth_left - 1)?));
            }
            sort_kvlist(&mut out)?;
            Value::KvList(out)
        }
        _ => return Err(Error::invalid("unsupported cbor value")),
    })
}

/// Sort a key/value list by raw key bytes and refuse duplicate keys.
pub fn sort_kvlist(list: &mut Vec<(String, Value)>) -> Result<()> {
    list.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if list.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err(Error::invalid("duplicate attribute key"));
    }
    Ok(())
}

/// The `render_v1` recursive rendering of spec section 5.1.
pub fn render_v1(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Str(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(hex_lower(b)),
        Value::Int(i) => J::from(*i),
        Value::Double(d) => render_double(*d),
        Value::Bool(b) => J::Bool(*b),
        Value::Array(items) => J::Array(items.iter().map(render_v1).collect()),
        Value::KvList(entries) => J::Object(
            entries.iter().map(|(k, v)| (k.clone(), render_v1(v))).collect(),
        ),
    }
}

fn render_double(d: f64) -> serde_json::Value {
    if d.is_nan() {
        serde_json::Value::String("NaN".into())
    } else if d == f64::INFINITY {
        serde_json::Value::String("inf".into())
    } else if d == f64::NEG_INFINITY {
        serde_json::Value::String("-inf".into())
    } else {
        // serde_json renders finite f64 with the shortest round-trip form.
        serde_json::Number::from_f64(d).map_or(serde_json::Value::Null, serde_json::Value::Number)
    }
}

/// Lowercase hex of a byte slice.
pub fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Attribute-map entry point: raw string for strings, `None` for unset,
/// compact JSON of `render_v1` otherwise.
pub fn map_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Str(s) => Some(s.clone()),
        other => Some(render_v1(other).to_string()),
    }
}

/// Log-body entry point: identical rules to [`map_string`].
pub fn body_string(v: &Value) -> Option<String> {
    map_string(v)
}

/// Approximate retained size of a value, used for row-size budgets.
pub fn value_bytes(v: &Value) -> usize {
    match v {
        Value::Null | Value::Int(_) | Value::Double(_) | Value::Bool(_) => 8,
        Value::Str(s) => s.len() + 24,
        Value::Bytes(b) => b.len() + 24,
        Value::Array(items) => 24 + items.iter().map(value_bytes).sum::<usize>(),
        Value::KvList(entries) => {
            24 + entries.iter().map(|(k, v)| k.len() + 24 + value_bytes(v)).sum::<usize>()
        }
    }
}
```

Note: `serde_json::Value::Object` is a `Map<String, Value>`; with the default (non-`preserve_order`) feature it is a `BTreeMap`, which keeps keys sorted by `String` ordering, identical to byte ordering for UTF-8. Do not enable `preserve_order`.

Add `pub mod value;` to `src/lib.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake value::tests`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): value tree, cbor decoding and render_v1

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Canonical encoding v1, `SeriesId`, golden vectors

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/canonical.rs`
- Create: `rust/otap-dataflow/crates/series-lake/tools/gen_golden.py`
- Create: `rust/otap-dataflow/crates/series-lake/tests/golden/canonical_v1.json` (generated)
- Create: `rust/otap-dataflow/crates/series-lake/tests/golden.rs`
- Modify: `src/lib.rs` (add `pub mod canonical;`)

**Interfaces:**
- Consumes: `value::Value`.
- Produces:
  - `pub type SeriesId = [u8; 16];`
  - `pub enum Signal { Logs, Metrics }` with `fn as_str(&self) -> &'static str` (`"logs"`, `"metrics"`)
  - `pub enum MetricKind { Gauge, Sum, Histogram, ExpHistogram, Summary }` with `as_str`
  - `pub enum Temporality { Unspecified, Delta, Cumulative }` with `as_str` (`""`, `"delta"`, `"cumulative"`)
  - `pub struct MetricDescriptor { pub name: String, pub unit: String, pub kind: MetricKind, pub temporality: Temporality, pub is_monotonic: bool, pub description: String }`
  - `pub struct Descriptor { pub signal: Signal, pub resource_attrs: Vec<(String, Value)>, pub resource_schema_url: String, pub scope_name: String, pub scope_version: String, pub scope_schema_url: String, pub scope_attrs: Vec<(String, Value)>, pub metric: Option<MetricDescriptor>, pub attrs: Vec<(String, Value)> }` (attribute lists already sorted and unique)
  - `pub fn canonical_bytes(d: &Descriptor) -> Vec<u8>`
  - `pub fn series_id(identity_bytes: &[u8]) -> SeriesId`
  - `pub fn hex(id: &SeriesId) -> String`

- [ ] **Step 1: Write the failing unit tests** (in `src/canonical.rs`, `#[cfg(test)] mod tests`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn logs_desc() -> Descriptor {
        Descriptor {
            signal: Signal::Logs,
            resource_attrs: vec![("host.id".into(), Value::Str("a1".into()))],
            resource_schema_url: String::new(),
            scope_name: "lib".into(),
            scope_version: "1".into(),
            scope_schema_url: String::new(),
            scope_attrs: vec![],
            metric: None,
            attrs: vec![],
        }
    }

    /// Scenario: encode a minimal logs descriptor by hand.
    /// Guarantees: the byte layout is tag, u32 big-endian length, payload, in the fixed field order.
    #[test]
    fn layout_of_minimal_logs_descriptor() {
        let bytes = canonical_bytes(&logs_desc());
        let mut expect = Vec::new();
        let s = |e: &mut Vec<u8>, v: &str| {
            e.push(0x01);
            e.extend((v.len() as u32).to_be_bytes());
            e.extend(v.as_bytes());
        };
        s(&mut expect, "OTEL-SERIES/1");
        s(&mut expect, "logs");
        // resource attrs kvlist: tag 0x08, len, count=1, key "host.id", value "a1"
        let mut kv = Vec::new();
        kv.extend(1u32.to_be_bytes());
        s(&mut kv, "host.id");
        s(&mut kv, "a1");
        expect.push(0x08);
        expect.extend((kv.len() as u32).to_be_bytes());
        expect.extend(kv);
        s(&mut expect, ""); // resource schema_url
        s(&mut expect, "lib");
        s(&mut expect, "1");
        s(&mut expect, ""); // scope schema_url
        // empty scope attrs
        expect.push(0x08);
        expect.extend(4u32.to_be_bytes());
        expect.extend(0u32.to_be_bytes());
        // empty identity attrs
        expect.push(0x08);
        expect.extend(4u32.to_be_bytes());
        expect.extend(0u32.to_be_bytes());
        assert_eq!(bytes, expect);
    }

    /// Scenario: the same descriptor with an int and a string attribute value.
    /// Guarantees: 42 (int) and "42" (string) give different series ids.
    #[test]
    fn int_and_string_differ() {
        let mut a = logs_desc();
        a.attrs = vec![("x".into(), Value::Int(42))];
        let mut b = logs_desc();
        b.attrs = vec![("x".into(), Value::Str("42".into()))];
        assert_ne!(series_id(&canonical_bytes(&a)), series_id(&canonical_bytes(&b)));
    }

    /// Scenario: two NaN bit patterns as attribute values.
    /// Guarantees: both hash identically (canonical quiet NaN).
    #[test]
    fn nan_is_canonicalized() {
        let mut a = logs_desc();
        a.attrs = vec![("x".into(), Value::Double(f64::from_bits(0x7FF8_0000_0000_0001)))];
        let mut b = logs_desc();
        b.attrs = vec![("x".into(), Value::Double(f64::from_bits(0xFFF8_0000_0000_0000)))];
        assert_eq!(canonical_bytes(&a), canonical_bytes(&b));
    }

    /// Scenario: xxh3_128 of a known input.
    /// Guarantees: seed 0 and big-endian canonical representation are used.
    #[test]
    fn series_id_is_xxh3_128_big_endian() {
        let id = series_id(b"");
        assert_eq!(hex(&id), "99aa06d3014798d86001c324468d497f");
    }
}
```

The empty-input xxh3_128 digest `99aa06d3014798d86001c324468d497f` is the published xxHash test value (`XXH3_128bits("")` with seed 0, canonical big-endian).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake canonical::tests`
Expected: compile error (module missing).

- [ ] **Step 3: Implement `src/canonical.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical encoding v1 and series identity (spec section 4).

use crate::value::Value;

/// 16-byte series identity: XXH3-128 of the canonical bytes, big-endian.
pub type SeriesId = [u8; 16];

/// Telemetry signal handled in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    /// Logs.
    Logs,
    /// Metrics.
    Metrics,
}

impl Signal {
    /// Canonical string.
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
        }
    }
}

/// Metric point kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Gauge.
    Gauge,
    /// Sum.
    Sum,
    /// Explicit-bounds histogram.
    Histogram,
    /// Exponential histogram.
    ExpHistogram,
    /// Summary.
    Summary,
}

impl MetricKind {
    /// Canonical string.
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Gauge => "gauge",
            MetricKind::Sum => "sum",
            MetricKind::Histogram => "histogram",
            MetricKind::ExpHistogram => "exp_histogram",
            MetricKind::Summary => "summary",
        }
    }
}

/// Aggregation temporality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temporality {
    /// Not specified (only valid for gauges).
    Unspecified,
    /// Delta.
    Delta,
    /// Cumulative.
    Cumulative,
}

impl Temporality {
    /// Canonical string.
    pub fn as_str(self) -> &'static str {
        match self {
            Temporality::Unspecified => "",
            Temporality::Delta => "delta",
            Temporality::Cumulative => "cumulative",
        }
    }
}

/// Metric-level identity fields plus non-identity description.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricDescriptor {
    /// Metric name.
    pub name: String,
    /// Unit.
    pub unit: String,
    /// Point kind.
    pub kind: MetricKind,
    /// Temporality.
    pub temporality: Temporality,
    /// Monotonic flag (false for non-sums).
    pub is_monotonic: bool,
    /// Description (not part of the identity).
    pub description: String,
}

/// Everything that identifies a series, plus `description`.
///
/// Attribute lists must be sorted by raw key bytes with unique keys
/// (see [`crate::value::sort_kvlist`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Descriptor {
    /// Signal.
    pub signal: Signal,
    /// Resource attributes.
    pub resource_attrs: Vec<(String, Value)>,
    /// Resource schema URL.
    pub resource_schema_url: String,
    /// Scope name.
    pub scope_name: String,
    /// Scope version.
    pub scope_version: String,
    /// Scope schema URL.
    pub scope_schema_url: String,
    /// Scope attributes.
    pub scope_attrs: Vec<(String, Value)>,
    /// Metric fields (metrics only).
    pub metric: Option<MetricDescriptor>,
    /// Identity attributes: data point attributes, or allow-listed log attributes.
    pub attrs: Vec<(String, Value)>,
}

const TAG_STR: u8 = 0x01;
const TAG_BYTES: u8 = 0x02;
const TAG_INT: u8 = 0x03;
const TAG_DOUBLE: u8 = 0x04;
const TAG_BOOL: u8 = 0x05;
const TAG_NULL: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_KVLIST: u8 = 0x08;
const CANONICAL_NAN: u64 = 0x7FF8_0000_0000_0000;

fn put(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    out.extend((payload.len() as u32).to_be_bytes());
    out.extend(payload);
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put(out, TAG_STR, s.as_bytes());
}

fn encode_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => put(out, TAG_NULL, &[]),
        Value::Str(s) => put_str(out, s),
        Value::Bytes(b) => put(out, TAG_BYTES, b),
        Value::Int(i) => put(out, TAG_INT, &i.to_be_bytes()),
        Value::Double(d) => {
            let bits = if d.is_nan() { CANONICAL_NAN } else { d.to_bits() };
            put(out, TAG_DOUBLE, &bits.to_be_bytes());
        }
        Value::Bool(b) => put(out, TAG_BOOL, &[u8::from(*b)]),
        Value::Array(items) => {
            let mut payload = Vec::new();
            payload.extend((items.len() as u32).to_be_bytes());
            for item in items {
                encode_value(&mut payload, item);
            }
            put(out, TAG_ARRAY, &payload);
        }
        Value::KvList(entries) => encode_kvlist(out, entries),
    }
}

fn encode_kvlist(out: &mut Vec<u8>, entries: &[(String, Value)]) {
    let mut payload = Vec::new();
    payload.extend((entries.len() as u32).to_be_bytes());
    for (k, v) in entries {
        put_str(&mut payload, k);
        encode_value(&mut payload, v);
    }
    put(out, TAG_KVLIST, &payload);
}

/// Build the canonical identity bytes of a descriptor.
pub fn canonical_bytes(d: &Descriptor) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    put_str(&mut out, "OTEL-SERIES/1");
    put_str(&mut out, d.signal.as_str());
    encode_kvlist(&mut out, &d.resource_attrs);
    put_str(&mut out, &d.resource_schema_url);
    put_str(&mut out, &d.scope_name);
    put_str(&mut out, &d.scope_version);
    put_str(&mut out, &d.scope_schema_url);
    encode_kvlist(&mut out, &d.scope_attrs);
    if let Some(m) = &d.metric {
        put_str(&mut out, &m.name);
        put_str(&mut out, &m.unit);
        put_str(&mut out, m.kind.as_str());
        put_str(&mut out, m.temporality.as_str());
        put(&mut out, TAG_BOOL, &[u8::from(m.is_monotonic)]);
    }
    encode_kvlist(&mut out, &d.attrs);
    out
}

/// XXH3-128 (seed 0) of the identity bytes, in big-endian canonical form.
pub fn series_id(identity_bytes: &[u8]) -> SeriesId {
    xxhash_rust::xxh3::xxh3_128(identity_bytes).to_be_bytes()
}

/// Lowercase hex rendering of a series id.
pub fn hex(id: &SeriesId) -> String {
    crate::value::hex_lower(id)
}
```

Add `pub mod canonical;` to `src/lib.rs`.

- [ ] **Step 4: Run unit tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake canonical::tests`
Expected: 4 passed. If `series_id_is_xxh3_128_big_endian` fails, check the digest against `python3 -c "import xxhash; print(xxhash.xxh3_128_hexdigest(b''))"` (package `xxhash`); the Python value is the source of truth for the vector, fix the test constant, not the code.

- [ ] **Step 5: Write the independent Python generator `tools/gen_golden.py`**

```python
#!/usr/bin/env python3
"""Independent implementation of canonical encoding v1 (spec section 4).

Generates tests/golden/canonical_v1.json. Requires: pip install xxhash
"""
import json, math, struct, sys
import xxhash

TAG = dict(str=1, bytes=2, int=3, double=4, bool=5, null=6, array=7, kvlist=8)
CANONICAL_NAN = 0x7FF8000000000000


def put(tag, payload):
    return bytes([tag]) + struct.pack(">I", len(payload)) + payload


def enc_str(s):
    return put(TAG["str"], s.encode("utf-8"))


def enc_value(v):
    t = v["type"]
    if t == "null":
        return put(TAG["null"], b"")
    if t == "str":
        return enc_str(v["value"])
    if t == "bytes":
        return put(TAG["bytes"], bytes.fromhex(v["value"]))
    if t == "int":
        return put(TAG["int"], struct.pack(">q", v["value"]))
    if t == "double":
        bits = v["bits"] if "bits" in v else struct.unpack(">Q", struct.pack(">d", v["value"]))[0]
        exp = bits >> 52 & 0x7FF
        mant = bits & ((1 << 52) - 1)
        if exp == 0x7FF and mant != 0:
            bits = CANONICAL_NAN
        return put(TAG["double"], struct.pack(">Q", bits))
    if t == "bool":
        return put(TAG["bool"], bytes([1 if v["value"] else 0]))
    if t == "array":
        payload = struct.pack(">I", len(v["items"])) + b"".join(enc_value(i) for i in v["items"])
        return put(TAG["array"], payload)
    if t == "kvlist":
        return enc_kvlist(v["entries"])
    raise ValueError(t)


def enc_kvlist(entries):
    entries = sorted(entries, key=lambda e: e["key"].encode("utf-8"))
    payload = struct.pack(">I", len(entries))
    for e in entries:
        payload += enc_str(e["key"]) + enc_value(e["value"])
    return put(TAG["kvlist"], payload)


def canonical(d):
    out = enc_str("OTEL-SERIES/1") + enc_str(d["signal"])
    out += enc_kvlist(d.get("resource_attrs", []))
    out += enc_str(d.get("resource_schema_url", ""))
    out += enc_str(d.get("scope_name", ""))
    out += enc_str(d.get("scope_version", ""))
    out += enc_str(d.get("scope_schema_url", ""))
    out += enc_kvlist(d.get("scope_attrs", []))
    if d["signal"] == "metrics":
        m = d["metric"]
        out += enc_str(m["name"]) + enc_str(m.get("unit", "")) + enc_str(m["kind"])
        out += enc_str(m.get("temporality", "")) + put(TAG["bool"], bytes([1 if m.get("is_monotonic") else 0]))
    out += enc_kvlist(d.get("attrs", []))
    return out


def kv(key, value):
    return {"key": key, "value": value}


S = lambda s: {"type": "str", "value": s}
I = lambda i: {"type": "int", "value": i}
D = lambda f: {"type": "double", "value": f}
DB = lambda bits: {"type": "double", "bits": bits}
B = lambda hexs: {"type": "bytes", "value": hexs}
BOOL = lambda b: {"type": "bool", "value": b}
NULL = {"type": "null"}
ARR = lambda *items: {"type": "array", "items": list(items)}
KV = lambda *entries: {"type": "kvlist", "entries": list(entries)}

base_logs = dict(signal="logs", resource_attrs=[kv("host.id", S("a1"))], scope_name="lib", scope_version="1")
base_metrics = dict(signal="metrics", resource_attrs=[kv("host.id", S("a1"))],
                    metric=dict(name="cpu.usage", unit="s", kind="gauge", temporality="", is_monotonic=False))

cases = [
    ("logs_minimal", base_logs),
    ("empty_string_attr", {**base_logs, "attrs": [kv("k", S(""))]}),
    ("missing_scope", {**base_logs, "scope_name": "", "scope_version": ""}),
    ("int_zero", {**base_logs, "attrs": [kv("k", I(0))]}),
    ("neg_zero_double", {**base_logs, "attrs": [kv("k", DB(0x8000000000000000))]}),
    ("int_min", {**base_logs, "attrs": [kv("k", I(-2**63))]}),
    ("int_max", {**base_logs, "attrs": [kv("k", I(2**63 - 1))]}),
    ("int_42", {**base_logs, "attrs": [kv("k", I(42))]}),
    ("str_42", {**base_logs, "attrs": [kv("k", S("42"))]}),
    ("nan_quiet", {**base_logs, "attrs": [kv("k", DB(0x7FF8000000000000))]}),
    ("nan_payload", {**base_logs, "attrs": [kv("k", DB(0x7FF8000000000001))]}),
    ("nan_negative", {**base_logs, "attrs": [kv("k", DB(0xFFF8000000000000))]}),
    ("pos_inf", {**base_logs, "attrs": [kv("k", D(math.inf))]}),
    ("neg_inf", {**base_logs, "attrs": [kv("k", D(-math.inf))]}),
    ("unicode_bmp", {**base_logs, "attrs": [kv("k", S("\u00e9\u4e2d"))]}),
    ("unicode_supplementary", {**base_logs, "attrs": [kv("k", S("\U0001F600"))]}),
    ("embedded_nul", {**base_logs, "attrs": [kv("k", S("a\u0000b"))]}),
    ("bytes", {**base_logs, "attrs": [kv("k", B("00ff10"))]}),
    ("null_value", {**base_logs, "attrs": [kv("k", NULL)]}),
    ("bool_true", {**base_logs, "attrs": [kv("k", BOOL(True))]}),
    ("nested_array", {**base_logs, "attrs": [kv("k", ARR(I(1), S("x"), ARR(NULL)))]}),
    ("nested_kvlist", {**base_logs, "attrs": [kv("k", KV(kv("b", I(2)), kv("a", KV(kv("z", BOOL(False))))))]}),
    ("key_order_prefix", {**base_logs, "attrs": [kv("ab", I(1)), kv("a", I(2)), kv("a\u00e9", I(3)), kv("b", I(4))]}),
    ("metrics_gauge", base_metrics),
    ("metrics_sum_delta_monotonic", {**base_metrics, "metric": dict(name="req", unit="1", kind="sum", temporality="delta", is_monotonic=True)}),
    ("metrics_histogram_cumulative", {**base_metrics, "metric": dict(name="lat", unit="ms", kind="histogram", temporality="cumulative", is_monotonic=False)}),
    ("metrics_dp_attrs", {**base_metrics, "attrs": [kv("cpu", I(3)), kv("mode", S("user"))]}),
    ("metrics_scope_attrs", {**base_metrics, "scope_attrs": [kv("s", S("v"))], "scope_name": "m", "scope_version": "2"}),
    ("producer_a", {**base_logs, "resource_attrs": [kv("host.id", S("a"))]}),
    ("producer_b", {**base_logs, "resource_attrs": [kv("host.id", S("b"))]}),
    ("schema_urls", {**base_logs, "resource_schema_url": "https://r", "scope_schema_url": "https://s"}),
]

vectors = []
for name, d in cases:
    b = canonical(d)
    vectors.append({"name": name, "descriptor": d, "canonical_hex": b.hex(),
                    "series_id_hex": xxhash.xxh3_128_hexdigest(b)})
json.dump({"format": "canonical_v1", "vectors": vectors}, open(sys.argv[1], "w"), indent=1, ensure_ascii=False)
print(f"wrote {len(vectors)} vectors")
```

Run: `pip install xxhash && python3 rust/otap-dataflow/crates/series-lake/tools/gen_golden.py rust/otap-dataflow/crates/series-lake/tests/golden/canonical_v1.json`
Expected: `wrote 31 vectors`.

- [ ] **Step 6: Write the golden test `tests/golden.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical encoding golden vectors generated by tools/gen_golden.py.

use otel_arrow_dfe_series_lake::canonical::{
    Descriptor, MetricDescriptor, MetricKind, Signal, Temporality, canonical_bytes, hex,
    series_id,
};
use otel_arrow_dfe_series_lake::value::Value;

fn value_from_json(j: &serde_json::Value) -> Value {
    match j["type"].as_str().expect("type") {
        "null" => Value::Null,
        "str" => Value::Str(j["value"].as_str().expect("str").to_string()),
        "bytes" => Value::Bytes(hex_decode(j["value"].as_str().expect("hex"))),
        "int" => Value::Int(j["value"].as_i64().expect("int")),
        "double" => match j.get("bits") {
            Some(bits) => Value::Double(f64::from_bits(bits.as_u64().expect("bits"))),
            None => Value::Double(j["value"].as_f64().expect("double")),
        },
        "bool" => Value::Bool(j["value"].as_bool().expect("bool")),
        "array" => Value::Array(j["items"].as_array().expect("items").iter().map(value_from_json).collect()),
        "kvlist" => Value::KvList(kv_from_json(&j["entries"])),
        other => panic!("unknown type {other}"),
    }
}

fn kv_from_json(j: &serde_json::Value) -> Vec<(String, Value)> {
    let mut v: Vec<(String, Value)> = j
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| (e["key"].as_str().expect("key").to_string(), value_from_json(&e["value"])))
                .collect()
        })
        .unwrap_or_default();
    v.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    v
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex")).collect()
}

fn str_field(j: &serde_json::Value, k: &str) -> String {
    j.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn descriptor_from_json(j: &serde_json::Value) -> Descriptor {
    let signal = match j["signal"].as_str().expect("signal") {
        "logs" => Signal::Logs,
        _ => Signal::Metrics,
    };
    let metric = j.get("metric").map(|m| MetricDescriptor {
        name: str_field(m, "name"),
        unit: str_field(m, "unit"),
        kind: match m["kind"].as_str().expect("kind") {
            "gauge" => MetricKind::Gauge,
            "sum" => MetricKind::Sum,
            "histogram" => MetricKind::Histogram,
            "exp_histogram" => MetricKind::ExpHistogram,
            _ => MetricKind::Summary,
        },
        temporality: match str_field(m, "temporality").as_str() {
            "delta" => Temporality::Delta,
            "cumulative" => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        },
        is_monotonic: m.get("is_monotonic").and_then(|v| v.as_bool()).unwrap_or(false),
        description: String::new(),
    });
    Descriptor {
        signal,
        resource_attrs: kv_from_json(&j["resource_attrs"]),
        resource_schema_url: str_field(j, "resource_schema_url"),
        scope_name: str_field(j, "scope_name"),
        scope_version: str_field(j, "scope_version"),
        scope_schema_url: str_field(j, "scope_schema_url"),
        scope_attrs: kv_from_json(&j["scope_attrs"]),
        metric,
        attrs: kv_from_json(&j["attrs"]),
    }
}

/// Scenario: every vector produced by the independent Python encoder.
/// Guarantees: the Rust encoder and hash reproduce the canonical bytes and series id byte for byte.
#[test]
fn golden_vectors_match() {
    let raw = include_str!("golden/canonical_v1.json");
    let doc: serde_json::Value = serde_json::from_str(raw).expect("json");
    let vectors = doc["vectors"].as_array().expect("vectors");
    assert!(vectors.len() >= 30);
    for v in vectors {
        let name = v["name"].as_str().expect("name");
        let d = descriptor_from_json(&v["descriptor"]);
        let bytes = canonical_bytes(&d);
        assert_eq!(hex_bytes(&bytes), v["canonical_hex"].as_str().expect("hex"), "bytes of {name}");
        assert_eq!(hex(&series_id(&bytes)), v["series_id_hex"].as_str().expect("id"), "id of {name}");
    }
}

/// Scenario: the vector pair that differs only in the producer attribute.
/// Guarantees: the producer attribute is part of the identity.
#[test]
fn producer_attribute_changes_identity() {
    let raw = include_str!("golden/canonical_v1.json");
    let doc: serde_json::Value = serde_json::from_str(raw).expect("json");
    let find = |n: &str| {
        doc["vectors"].as_array().expect("v").iter().find(|v| v["name"] == n).expect("vector")["series_id_hex"].clone()
    };
    assert_ne!(find("producer_a"), find("producer_b"));
}

fn hex_bytes(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
```

- [ ] **Step 7: Run golden tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake --test golden`
Expected: 2 passed. A mismatch means the Rust and Python encoders disagree; the spec text decides which is wrong.

- [ ] **Step 8: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): canonical encoding v1, series id and golden vectors

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Reading OTAP attribute batches (`attrs.rs`)

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/attrs.rs`
- Modify: `src/lib.rs` (add `pub mod attrs;`)

**Interfaces:**
- Consumes: `value::{Value, decode_cbor, sort_kvlist}`.
- Produces:
  - `pub struct AttrTable { ... }` built by `AttrTable::from_batch(batch: &RecordBatch, max_depth: usize) -> Result<AttrTable>`; works for `attributes_16` (u16 parent ids) and `attributes_32` (u32 parent ids) schemas.
  - `pub fn AttrTable::get(&self, parent_id: u32) -> &[(String, Value)]` (sorted, unique; empty slice when absent)
  - `pub fn AttrTable::approx_bytes(&self, parent_id: u32) -> usize`

OTAP attribute batches (pdata `schema/payloads.rs`, `attributes_16` / `attributes_32`) have columns `parent_id` (UInt16, or Dictionary<UInt8, UInt32>), `key` (Dictionary<UInt8, Utf8>), `type` (UInt8, values of `AttributeValueType`: 0 Empty, 1 Str, 2 Int, 3 Double, 4 Bool, 5 Map, 6 Slice, 7 Bytes), `str` (Dictionary<UInt16, Utf8>), `int` (Dictionary<UInt16, Int64>), `double` (Float64), `bool` (Boolean), `bytes` (Dictionary<UInt16, Binary>), `ser` (Dictionary<UInt16, Binary>). Optional value columns may be absent. Dictionary columns are removed with `arrow::compute::cast` to their plain value type before reading.

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `src/attrs.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray, UInt8Array, UInt16Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let mut ser = Vec::new();
        ciborium::into_writer(
            &ciborium::Value::Array(vec![ciborium::Value::Integer(7.into())]),
            &mut ser,
        )
        .expect("cbor");
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
            Field::new("int", DataType::Int64, true),
            Field::new("double", DataType::Float64, true),
            Field::new("ser", DataType::Binary, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![0u16, 0, 1, 1])),
            Arc::new(StringArray::from(vec!["z", "a", "n", "arr"])),
            Arc::new(UInt8Array::from(vec![1u8, 2, 3, 6])),
            Arc::new(StringArray::from(vec![Some("v"), None, None, None])),
            Arc::new(Int64Array::from(vec![None, Some(5), None, None])),
            Arc::new(Float64Array::from(vec![None, None, Some(1.5), None])),
            Arc::new(BinaryArray::from(vec![None, None, None, Some(ser.as_slice())])),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    /// Scenario: two parents with mixed value types, keys unsorted in the batch.
    /// Guarantees: each parent gets a sorted, typed attribute list; absent parents are empty.
    #[test]
    fn groups_and_sorts_by_parent() {
        let t = AttrTable::from_batch(&batch(), 32).expect("table");
        assert_eq!(
            t.get(0),
            &[("a".to_string(), Value::Int(5)), ("z".to_string(), Value::Str("v".into()))]
        );
        assert_eq!(
            t.get(1),
            &[
                ("arr".to_string(), Value::Array(vec![Value::Int(7)])),
                ("n".to_string(), Value::Double(1.5))
            ]
        );
        assert!(t.get(7).is_empty());
    }

    /// Scenario: the same key appears twice for one parent.
    /// Guarantees: the whole batch is refused as invalid content.
    #[test]
    fn duplicate_key_is_refused() {
        let schema = Schema::new(vec![
            Field::new("parent_id", DataType::UInt16, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("type", DataType::UInt8, false),
            Field::new("str", DataType::Utf8, true),
        ]);
        let cols: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![3u16, 3])),
            Arc::new(StringArray::from(vec!["k", "k"])),
            Arc::new(UInt8Array::from(vec![1u8, 1])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ];
        let b = RecordBatch::try_new(Arc::new(schema), cols).expect("batch");
        assert!(matches!(AttrTable::from_batch(&b, 32), Err(Error::Refused(RefuseReason::Invalid(_)))));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake attrs::tests`
Expected: compile error.

- [ ] **Step 3: Implement `src/attrs.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reads OTAP attribute record batches into per-parent, sorted attribute lists.

use std::collections::HashMap;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt8Type, UInt16Type, UInt32Type};
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::value::{Value, decode_cbor, sort_kvlist, value_bytes};

const TYPE_EMPTY: u8 = 0;
const TYPE_STR: u8 = 1;
const TYPE_INT: u8 = 2;
const TYPE_DOUBLE: u8 = 3;
const TYPE_BOOL: u8 = 4;
const TYPE_MAP: u8 = 5;
const TYPE_SLICE: u8 = 6;
const TYPE_BYTES: u8 = 7;

/// Attributes of one OTAP attribute batch, grouped by parent id.
#[derive(Debug, Default)]
pub struct AttrTable {
    groups: HashMap<u32, Vec<(String, Value)>>,
}

fn plain(batch: &RecordBatch, name: &str, to: &DataType) -> Result<Option<ArrayRef>> {
    let Some(col) = batch.column_by_name(name) else {
        return Ok(None);
    };
    if col.data_type() == to {
        return Ok(Some(col.clone()));
    }
    Ok(Some(cast(col, to)?))
}

fn required(batch: &RecordBatch, name: &str, to: &DataType) -> Result<ArrayRef> {
    plain(batch, name, to)?.ok_or_else(|| Error::invalid(format!("attribute batch lacks {name}")))
}

impl AttrTable {
    /// Build the table from an `attributes_16` or `attributes_32` batch.
    pub fn from_batch(batch: &RecordBatch, max_depth: usize) -> Result<Self> {
        let parent_ids: Vec<u32> = {
            let col = batch
                .column_by_name("parent_id")
                .ok_or_else(|| Error::invalid("attribute batch lacks parent_id"))?;
            let plain_col = match col.data_type() {
                DataType::Dictionary(_, _) => cast(col, &DataType::UInt32)?,
                _ => col.clone(),
            };
            match plain_col.data_type() {
                DataType::UInt16 => plain_col
                    .as_primitive::<UInt16Type>()
                    .iter()
                    .map(|v| u32::from(v.unwrap_or(0)))
                    .collect(),
                DataType::UInt32 => plain_col
                    .as_primitive::<UInt32Type>()
                    .iter()
                    .map(|v| v.unwrap_or(0))
                    .collect(),
                other => return Err(Error::invalid(format!("parent_id type {other}"))),
            }
        };
        let keys = required(batch, "key", &DataType::Utf8)?;
        let keys = keys.as_string::<i32>();
        let types = required(batch, "type", &DataType::UInt8)?;
        let types = types.as_primitive::<UInt8Type>();
        let strs = plain(batch, "str", &DataType::Utf8)?;
        let ints = plain(batch, "int", &DataType::Int64)?;
        let doubles = plain(batch, "double", &DataType::Float64)?;
        let bools = plain(batch, "bool", &DataType::Boolean)?;
        let bytes = plain(batch, "bytes", &DataType::Binary)?;
        let sers = plain(batch, "ser", &DataType::Binary)?;

        let mut groups: HashMap<u32, Vec<(String, Value)>> = HashMap::new();
        for row in 0..batch.num_rows() {
            let key = keys.value(row).to_string();
            let ty = types.value(row);
            let value = match ty {
                TYPE_EMPTY => Value::Null,
                TYPE_STR => strs
                    .as_ref()
                    .and_then(|a| a.as_string::<i32>().is_valid(row).then(|| a.as_string::<i32>().value(row).to_string()))
                    .map_or(Value::Null, Value::Str),
                TYPE_INT => ints
                    .as_ref()
                    .and_then(|a| a.as_primitive::<Int64Type>().is_valid(row).then(|| a.as_primitive::<Int64Type>().value(row)))
                    .map_or(Value::Null, Value::Int),
                TYPE_DOUBLE => doubles
                    .as_ref()
                    .and_then(|a| a.as_primitive::<Float64Type>().is_valid(row).then(|| a.as_primitive::<Float64Type>().value(row)))
                    .map_or(Value::Null, Value::Double),
                TYPE_BOOL => bools
                    .as_ref()
                    .and_then(|a| a.as_boolean().is_valid(row).then(|| a.as_boolean().value(row)))
                    .map_or(Value::Null, Value::Bool),
                TYPE_BYTES => bytes
                    .as_ref()
                    .and_then(|a| a.as_binary::<i32>().is_valid(row).then(|| a.as_binary::<i32>().value(row).to_vec()))
                    .map_or(Value::Null, Value::Bytes),
                TYPE_MAP | TYPE_SLICE => match sers.as_ref() {
                    Some(a) if a.as_binary::<i32>().is_valid(row) => {
                        decode_cbor(a.as_binary::<i32>().value(row), max_depth)?
                    }
                    _ => Value::Null,
                },
                other => return Err(Error::invalid(format!("attribute type {other}"))),
            };
            groups.entry(parent_ids[row]).or_default().push((key, value));
        }
        for list in groups.values_mut() {
            sort_kvlist(list)?;
        }
        Ok(Self { groups })
    }

    /// Attributes of one parent, sorted by key, or an empty slice.
    pub fn get(&self, parent_id: u32) -> &[(String, Value)] {
        self.groups.get(&parent_id).map_or(&[], Vec::as_slice)
    }

    /// Approximate retained bytes of one parent's attributes.
    pub fn approx_bytes(&self, parent_id: u32) -> usize {
        self.get(parent_id).iter().map(|(k, v)| k.len() + 24 + value_bytes(v)).sum()
    }
}
```

Add `pub mod attrs;` to `src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake attrs::tests`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): read OTAP attribute batches into sorted per-parent lists

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Configuration types and dataset schemas

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/config.rs`
- Create: `rust/otap-dataflow/crates/series-lake/src/schema.rs`
- Modify: `src/lib.rs` (add `pub mod config; pub mod schema;`)

**Interfaces:**
- Produces (config):
  - `pub enum DenormType { String, Int64, Double, Bool }`
  - `pub struct Denormalize { pub path: String, pub column: String, pub ty: DenormType }` with `serde` accepting either a bare string (`"resource.service.name"`) or the object form; `pub fn source(&self) -> (DenormSource, &str)` where `pub enum DenormSource { Resource, Scope, Attrs }`
  - `pub enum SortOrder { Asc, Desc }`, `pub enum Nulls { First, Last }`, `pub struct SortKey { pub column: String, pub order: SortOrder, pub nulls: Nulls }` (serde: bare string means asc, nulls last)
  - `pub struct SignalConfig { pub series_attributes: Vec<String>, pub denormalize: Vec<Denormalize>, pub values_sort: Vec<SortKey> }`
  - `pub struct IngressLimits { pub max_request_bytes: usize, pub max_extracted_bytes: usize, pub max_row_bytes: usize, pub max_nesting_depth: usize }`
  - `pub struct SortingConfig { pub enabled: bool, pub run_target_bytes: usize, pub merge_chunk_bytes: usize }`
  - `pub struct UploadConfig { pub part_bytes: usize, pub concurrency: usize }`
  - `pub struct ParquetConfig { pub row_group_bytes: usize, pub writer_limit_bytes: usize }` (compression fixed to ZSTD in v1)
  - `pub enum UnsupportedPolicy { Reject, Drop }`
  - `pub struct LakeConfig { pub writer_id: String, pub producer_id_attribute: String, pub window_interval: Duration, pub ingress: IngressLimits, pub sorting: SortingConfig, pub upload: UploadConfig, pub parquet: ParquetConfig, pub unsupported: UnsupportedPolicy, pub logs: SignalConfig, pub metrics: SignalConfig }` with `Default` matching the spec example and `pub fn validate(&self) -> Result<()>` (column collisions, sort keys exist in the dataset schema, `max_row_bytes <= run_target_bytes / 4`).
- Produces (schema):
  - `pub enum Dataset { LogsSeries, LogsValues, MetricsSeries, MetricsNumber, MetricsHistogram }` with `fn signal(self) -> Signal`, `fn name(self) -> &'static str` (`series`, `values`, `number`, `histogram`), `fn is_series(self) -> bool`, `pub const ALL: [Dataset; 5]`
  - `pub fn dataset_schema(ds: Dataset, cfg: &LakeConfig) -> SchemaRef` (intrinsic columns of spec 5.1 plus `d_*`/aliased denormalized columns; for series datasets only identity-path denormalized columns)
  - `pub fn schema_fingerprint(schema: &Schema) -> u64` (xxh3_64 over `name:type;` for each field in order)
  - `pub fn denorm_columns(ds: Dataset, cfg: &LakeConfig) -> Vec<&Denormalize>` (the ones present in that dataset)

- [ ] **Step 1: Write failing tests** (`#[cfg(test)]` in `src/schema.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Denormalize, DenormType, LakeConfig};

    /// Scenario: default config, every dataset.
    /// Guarantees: the intrinsic column lists of spec section 5.1 are produced in order.
    #[test]
    fn intrinsic_columns_match_spec() {
        let cfg = LakeConfig::default();
        let names = |ds: Dataset| -> Vec<String> {
            dataset_schema(ds, &cfg).fields().iter().map(|f| f.name().clone()).collect()
        };
        assert_eq!(
            names(Dataset::LogsValues),
            [
                "series_id", "producer_id", "time", "time_unix_nano", "observed_time",
                "observed_time_unix_nano", "severity_number", "severity_text", "body",
                "event_name", "trace_id", "span_id", "flags", "attrs"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsNumber),
            [
                "series_id", "producer_id", "metric_name", "time", "time_unix_nano",
                "start_time", "start_time_unix_nano", "flags", "value_int", "value_double"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsHistogram),
            [
                "series_id", "producer_id", "metric_name", "time", "time_unix_nano",
                "start_time", "start_time_unix_nano", "flags", "count", "sum", "min", "max",
                "bucket_counts", "explicit_bounds"
            ]
        );
        assert_eq!(
            names(Dataset::LogsSeries),
            [
                "series_id", "identity_bytes", "emitted_at", "resource_schema_url",
                "resource_attrs", "scope_name", "scope_version", "scope_schema_url",
                "scope_attrs", "attrs"
            ]
        );
        assert_eq!(names(Dataset::MetricsSeries)[10..], ["metric_name", "unit", "metric_type", "temporality", "is_monotonic", "description"]);
    }

    /// Scenario: a resource-path and an attrs-path denormalized column for logs.
    /// Guarantees: values get both, series gets only the identity (resource) one; fingerprints differ.
    #[test]
    fn denormalized_columns_and_fingerprint() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![
            Denormalize { path: "resource.service.name".into(), column: "service_name".into(), ty: DenormType::String },
            Denormalize { path: "attrs.http.status_code".into(), column: "http_status_code".into(), ty: DenormType::Int64 },
        ];
        let values = dataset_schema(Dataset::LogsValues, &cfg);
        assert!(values.column_with_name("service_name").is_some());
        assert!(values.column_with_name("http_status_code").is_some());
        let series = dataset_schema(Dataset::LogsSeries, &cfg);
        assert!(series.column_with_name("service_name").is_some());
        assert!(series.column_with_name("http_status_code").is_none());
        assert_ne!(schema_fingerprint(&values), schema_fingerprint(&series));
        assert_ne!(schema_fingerprint(&values), schema_fingerprint(&dataset_schema(Dataset::LogsValues, &LakeConfig::default())));
    }

    /// Scenario: two denormalized columns whose names differ only by case.
    /// Guarantees: validation refuses the configuration.
    #[test]
    fn collision_is_a_config_error() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![
            Denormalize { path: "resource.a".into(), column: "Col".into(), ty: DenormType::String },
            Denormalize { path: "resource.b".into(), column: "col".into(), ty: DenormType::String },
        ];
        assert!(cfg.validate().is_err());
        let mut cfg = LakeConfig::default();
        cfg.logs.values_sort = vec![crate::config::SortKey { column: "nope".into(), order: crate::config::SortOrder::Asc, nulls: crate::config::Nulls::Last }];
        assert!(cfg.validate().is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake schema::tests`
Expected: compile error.

- [ ] **Step 3: Implement `src/config.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Format configuration types (spec section 7.4, crate-relevant subset).

use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::error::{Error, Result};

/// Storage type of a denormalized column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DenormType {
    /// Rendered with the attribute-map string rules.
    #[default]
    String,
    /// 64-bit integer.
    Int64,
    /// 64-bit float.
    Double,
    /// Boolean.
    Bool,
}

/// Where a denormalized path reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenormSource {
    /// Resource attributes.
    Resource,
    /// Scope attributes.
    Scope,
    /// Record or data point attributes.
    Attrs,
}

/// One denormalized column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denormalize {
    /// `resource.<key>`, `scope.<key>` or `attrs.<key>`.
    pub path: String,
    /// Column name.
    pub column: String,
    /// Column type.
    pub ty: DenormType,
}

impl Denormalize {
    /// Split the path into its source and attribute key.
    pub fn source(&self) -> Result<(DenormSource, &str)> {
        if let Some(k) = self.path.strip_prefix("resource.") {
            Ok((DenormSource::Resource, k))
        } else if let Some(k) = self.path.strip_prefix("scope.") {
            Ok((DenormSource::Scope, k))
        } else if let Some(k) = self.path.strip_prefix("attrs.") {
            Ok((DenormSource::Attrs, k))
        } else {
            Err(Error::invalid(format!("denormalize path {}", self.path)))
        }
    }

    fn default_column(path: &str) -> String {
        let key = path.split_once('.').map_or(path, |(_, k)| k);
        key.replace('.', "_")
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum DenormRepr {
    Short(String),
    Full { path: String, column: Option<String>, #[serde(default, rename = "type")] ty: DenormType },
}

impl<'de> Deserialize<'de> for Denormalize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(match DenormRepr::deserialize(d)? {
            DenormRepr::Short(path) => Denormalize { column: Self::default_column(&path), path, ty: DenormType::String },
            DenormRepr::Full { path, column, ty } => Denormalize {
                column: column.unwrap_or_else(|| Self::default_column(&path)),
                path,
                ty,
            },
        })
    }
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    /// Ascending.
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// Null placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Nulls {
    /// Nulls first.
    First,
    /// Nulls last.
    #[default]
    Last,
}

/// One sort key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    /// Physical column.
    pub column: String,
    /// Direction.
    pub order: SortOrder,
    /// Null placement.
    pub nulls: Nulls,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SortKeyRepr {
    Short(String),
    Full { column: String, #[serde(default)] order: SortOrder, #[serde(default)] nulls: Nulls },
}

impl<'de> Deserialize<'de> for SortKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(match SortKeyRepr::deserialize(d)? {
            SortKeyRepr::Short(column) => SortKey { column, order: SortOrder::Asc, nulls: Nulls::Last },
            SortKeyRepr::Full { column, order, nulls } => SortKey { column, order, nulls },
        })
    }
}

fn default_values_sort() -> Vec<SortKey> {
    vec![
        SortKey { column: "series_id".into(), order: SortOrder::Asc, nulls: Nulls::Last },
        SortKey { column: "time_unix_nano".into(), order: SortOrder::Asc, nulls: Nulls::Last },
    ]
}

/// Per-signal configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    /// Log attributes that are part of the identity (logs only).
    #[serde(default)]
    pub series_attributes: Vec<String>,
    /// Denormalized columns.
    #[serde(default)]
    pub denormalize: Vec<Denormalize>,
    /// Sort keys for the values datasets.
    #[serde(default = "default_values_sort")]
    pub values_sort: Vec<SortKey>,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self { series_attributes: vec![], denormalize: vec![], values_sort: default_values_sort() }
    }
}

/// Request budgets (spec section 6.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngressLimits {
    /// Logical input size limit.
    pub max_request_bytes: usize,
    /// Extracted output limit.
    pub max_extracted_bytes: usize,
    /// Single row limit.
    pub max_row_bytes: usize,
    /// Nested value depth limit.
    pub max_nesting_depth: usize,
}

impl Default for IngressLimits {
    fn default() -> Self {
        Self { max_request_bytes: 16 << 20, max_extracted_bytes: 32 << 20, max_row_bytes: 1 << 20, max_nesting_depth: 32 }
    }
}

/// Sorting configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SortingConfig {
    /// Sort values datasets at all.
    pub enabled: bool,
    /// Run size target.
    pub run_target_bytes: usize,
    /// Merge output chunk size.
    pub merge_chunk_bytes: usize,
}

impl Default for SortingConfig {
    fn default() -> Self {
        Self { enabled: true, run_target_bytes: 8 << 20, merge_chunk_bytes: 16 << 20 }
    }
}

/// Upload buffering.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct UploadConfig {
    /// Multipart part size and BufWriter capacity.
    pub part_bytes: usize,
    /// In-flight parts.
    pub concurrency: usize,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self { part_bytes: 8 << 20, concurrency: 2 }
    }
}

/// Parquet writer limits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ParquetConfig {
    /// Row group target.
    pub row_group_bytes: usize,
    /// Enforced writer memory threshold.
    pub writer_limit_bytes: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self { row_group_bytes: 64 << 20, writer_limit_bytes: 96 << 20 }
    }
}

/// Policy for exponential histograms and summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedPolicy {
    /// Nack the whole request.
    #[default]
    Reject,
    /// Drop the unsupported points, keep the rest.
    Drop,
}

/// Crate configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LakeConfig {
    /// Writer identity in file names and metadata.
    pub writer_id: String,
    /// Resource attribute projected into `producer_id`.
    pub producer_id_attribute: String,
    /// Window length.
    #[serde(with = "humantime_serde")]
    pub window_interval: Duration,
    /// Budgets.
    pub ingress: IngressLimits,
    /// Sorting.
    pub sorting: SortingConfig,
    /// Upload.
    pub upload: UploadConfig,
    /// Parquet.
    pub parquet: ParquetConfig,
    /// Unsupported point policy.
    pub unsupported: UnsupportedPolicy,
    /// Logs.
    pub logs: SignalConfig,
    /// Metrics.
    pub metrics: SignalConfig,
}

impl Default for LakeConfig {
    fn default() -> Self {
        Self {
            writer_id: "writer".into(),
            producer_id_attribute: "host.id".into(),
            window_interval: Duration::from_secs(15),
            ingress: IngressLimits::default(),
            sorting: SortingConfig::default(),
            upload: UploadConfig::default(),
            parquet: ParquetConfig::default(),
            unsupported: UnsupportedPolicy::default(),
            logs: SignalConfig::default(),
            metrics: SignalConfig::default(),
        }
    }
}

impl LakeConfig {
    /// Validate cross-field constraints (spec sections 5.2, 6.2, 7.4).
    pub fn validate(&self) -> Result<()> {
        if self.ingress.max_row_bytes > self.sorting.run_target_bytes / 4 {
            return Err(Error::invalid("max_row_bytes must be at most run_target_bytes / 4"));
        }
        if self.upload.concurrency == 0 || self.upload.part_bytes < 5 << 20 {
            return Err(Error::invalid("upload.part_bytes must be >= 5MiB and concurrency >= 1"));
        }
        for ds in crate::schema::Dataset::ALL {
            let schema = crate::schema::dataset_schema(ds, self);
            let mut seen = HashSet::new();
            for f in schema.fields() {
                if !seen.insert(f.name().to_lowercase()) {
                    return Err(Error::invalid(format!("column name collision: {}", f.name())));
                }
            }
            if !ds.is_series() {
                let sig = if ds.signal() == crate::canonical::Signal::Logs { &self.logs } else { &self.metrics };
                for key in &sig.values_sort {
                    if schema.column_with_name(&key.column).is_none() {
                        return Err(Error::invalid(format!("sort key {} not in {}", key.column, ds.name())));
                    }
                }
            }
        }
        for d in self.logs.denormalize.iter().chain(self.metrics.denormalize.iter()) {
            let _ = d.source()?;
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Implement `src/schema.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Output datasets and their Arrow schemas (spec section 5.1).

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use crate::canonical::Signal;
use crate::config::{DenormSource, DenormType, Denormalize, LakeConfig};

/// One output dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Dataset {
    /// `signal=logs/dataset=series`
    LogsSeries,
    /// `signal=logs/dataset=values`
    LogsValues,
    /// `signal=metrics/dataset=series`
    MetricsSeries,
    /// `signal=metrics/dataset=number`
    MetricsNumber,
    /// `signal=metrics/dataset=histogram`
    MetricsHistogram,
}

impl Dataset {
    /// All datasets, in write order per signal (series first).
    pub const ALL: [Dataset; 5] = [
        Dataset::LogsSeries,
        Dataset::LogsValues,
        Dataset::MetricsSeries,
        Dataset::MetricsNumber,
        Dataset::MetricsHistogram,
    ];

    /// Signal of the dataset.
    pub fn signal(self) -> Signal {
        match self {
            Dataset::LogsSeries | Dataset::LogsValues => Signal::Logs,
            _ => Signal::Metrics,
        }
    }

    /// Hive `dataset=` value.
    pub fn name(self) -> &'static str {
        match self {
            Dataset::LogsSeries | Dataset::MetricsSeries => "series",
            Dataset::LogsValues => "values",
            Dataset::MetricsNumber => "number",
            Dataset::MetricsHistogram => "histogram",
        }
    }

    /// Whether this is a series dataset.
    pub fn is_series(self) -> bool {
        matches!(self, Dataset::LogsSeries | Dataset::MetricsSeries)
    }

    /// Series dataset of the same signal.
    pub fn series_of(signal: Signal) -> Dataset {
        match signal {
            Signal::Logs => Dataset::LogsSeries,
            Signal::Metrics => Dataset::MetricsSeries,
        }
    }
}

fn ts_us() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

/// `MAP<STRING, STRING>` with nullable values, Parquet-compatible field names.
pub fn map_string_string() -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ])),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

fn denorm_type(t: DenormType) -> DataType {
    match t {
        DenormType::String => DataType::Utf8,
        DenormType::Int64 => DataType::Int64,
        DenormType::Double => DataType::Float64,
        DenormType::Bool => DataType::Boolean,
    }
}

/// Denormalized columns that appear in a dataset.
pub fn denorm_columns(ds: Dataset, cfg: &LakeConfig) -> Vec<&Denormalize> {
    let sig = match ds.signal() {
        Signal::Logs => &cfg.logs,
        Signal::Metrics => &cfg.metrics,
    };
    sig.denormalize
        .iter()
        .filter(|d| {
            if !ds.is_series() {
                return true;
            }
            match d.source() {
                Ok((DenormSource::Resource | DenormSource::Scope, _)) => true,
                Ok((DenormSource::Attrs, key)) => match ds.signal() {
                    Signal::Metrics => true,
                    Signal::Logs => sig.series_attributes.iter().any(|a| a == key),
                },
                Err(_) => false,
            }
        })
        .collect()
}

/// Arrow schema of a dataset under a configuration.
pub fn dataset_schema(ds: Dataset, cfg: &LakeConfig) -> SchemaRef {
    let mut fields: Vec<Field> = vec![Field::new("series_id", DataType::FixedSizeBinary(16), false)];
    if !ds.is_series() {
        fields.push(Field::new("producer_id", DataType::Utf8, false));
    }
    match ds {
        Dataset::LogsSeries | Dataset::MetricsSeries => {
            fields.extend([
                Field::new("identity_bytes", DataType::Binary, false),
                Field::new("emitted_at", ts_us(), false),
                Field::new("resource_schema_url", DataType::Utf8, false),
                Field::new("resource_attrs", map_string_string(), false),
                Field::new("scope_name", DataType::Utf8, false),
                Field::new("scope_version", DataType::Utf8, false),
                Field::new("scope_schema_url", DataType::Utf8, false),
                Field::new("scope_attrs", map_string_string(), false),
                Field::new("attrs", map_string_string(), false),
            ]);
            if ds == Dataset::MetricsSeries {
                fields.extend([
                    Field::new("metric_name", DataType::Utf8, false),
                    Field::new("unit", DataType::Utf8, false),
                    Field::new("metric_type", DataType::Utf8, false),
                    Field::new("temporality", DataType::Utf8, false),
                    Field::new("is_monotonic", DataType::Boolean, false),
                    Field::new("description", DataType::Utf8, false),
                ]);
            }
        }
        Dataset::LogsValues => fields.extend([
            Field::new("time", ts_us(), true),
            Field::new("time_unix_nano", DataType::Int64, true),
            Field::new("observed_time", ts_us(), true),
            Field::new("observed_time_unix_nano", DataType::Int64, true),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("severity_text", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
            Field::new("event_name", DataType::Utf8, false),
            Field::new("trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("span_id", DataType::FixedSizeBinary(8), true),
            Field::new("flags", DataType::Int32, false),
            Field::new("attrs", map_string_string(), false),
        ]),
        Dataset::MetricsNumber | Dataset::MetricsHistogram => {
            fields.extend([
                Field::new("metric_name", DataType::Utf8, false),
                Field::new("time", ts_us(), true),
                Field::new("time_unix_nano", DataType::Int64, true),
                Field::new("start_time", ts_us(), true),
                Field::new("start_time_unix_nano", DataType::Int64, true),
                Field::new("flags", DataType::Int32, false),
            ]);
            if ds == Dataset::MetricsNumber {
                fields.extend([
                    Field::new("value_int", DataType::Int64, true),
                    Field::new("value_double", DataType::Float64, true),
                ]);
            } else {
                fields.extend([
                    Field::new("count", DataType::Int64, false),
                    Field::new("sum", DataType::Float64, true),
                    Field::new("min", DataType::Float64, true),
                    Field::new("max", DataType::Float64, true),
                    Field::new("bucket_counts", DataType::List(Arc::new(Field::new("item", DataType::Int64, false))), false),
                    Field::new("explicit_bounds", DataType::List(Arc::new(Field::new("item", DataType::Float64, false))), false),
                ]);
            }
        }
    }
    for d in denorm_columns(ds, cfg) {
        fields.push(Field::new(&d.column, denorm_type(d.ty), true));
    }
    Arc::new(Schema::new(fields))
}

/// xxh3_64 over `name:type;` of every field, in order.
pub fn schema_fingerprint(schema: &Schema) -> u64 {
    let mut s = String::new();
    for f in schema.fields() {
        s.push_str(f.name());
        s.push(':');
        s.push_str(&f.data_type().to_string());
        s.push(';');
    }
    xxhash_rust::xxh3::xxh3_64(s.as_bytes())
}
```

Add `pub mod config; pub mod schema;` to `src/lib.rs`.

- [ ] **Step 5: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake schema::tests`
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): configuration types and dataset schemas

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Extraction core and logs extraction

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/extract/mod.rs`
- Create: `rust/otap-dataflow/crates/series-lake/src/extract/logs.rs`
- Modify: `rust/otap-dataflow/crates/series-lake/src/attrs.rs` (expose `AnyValueColumns`)
- Modify: `src/lib.rs` (add `pub mod extract;`)

**Interfaces:**
- Consumes: `attrs::AttrTable`, `canonical::*`, `schema::{Dataset, dataset_schema, denorm_columns}`, `config::LakeConfig`, `value::*`.
- Produces:
  - `pub enum DenormValue { Str(String), Int(i64), Double(f64), Bool(bool) }`
  - `pub struct DescriptorRow { pub series_id: SeriesId, pub identity_bytes: Vec<u8>, pub descriptor: Descriptor, pub denorm: Vec<Option<DenormValue>>, pub approx_bytes: usize }` (`denorm` follows `denorm_columns(series dataset)` order)
  - `pub struct ExtractStats { pub rows: usize, pub dropped_unsupported: u64, pub timestamp_out_of_range: u64, pub denorm_type_mismatch: u64 }`
  - `pub struct Extracted { pub signal: Signal, pub descriptors: Vec<DescriptorRow>, pub values: Vec<(Dataset, Vec<RecordBatch>)>, pub pinned_bytes: usize, pub stats: ExtractStats }`
  - `pub fn extract(records: &OtapArrowRecords, cfg: &LakeConfig) -> Result<Extracted>` (traces -> `Refused(Unsupported)`)
  - `pub fn series_batch(rows: &[&DescriptorRow], emitted_at_us: i64, ds: Dataset, cfg: &LakeConfig) -> Result<RecordBatch>`
  - `pub(crate) struct AnyValueColumns` in `attrs.rs` with `pub(crate) fn from_struct_or_batch(cols: &dyn ColumnSource) ...`; concretely: `pub(crate) fn any_value_columns(get: impl Fn(&str) -> Option<ArrayRef>) -> Result<AnyValueColumns>` and `pub(crate) fn AnyValueColumns::value_at(&self, row: usize, max_depth: usize) -> Result<Value>`
  - `pub(crate) fn timestamp_pair(ns: i64, stats: &mut ExtractStats) -> (Option<i64>, Option<i64>)` (raw nanos, micros; spec 5.1 rule)
  - `pub(crate) fn denorm_lookup(d: &Denormalize, resource: &[(String, Value)], scope: &[(String, Value)], attrs: &[(String, Value)], stats: &mut ExtractStats) -> Option<DenormValue>`
  - `pub(crate) struct RowSink` (in `extract/mod.rs`): accumulates Arrow builders for one dataset, seals slices at `run_target_bytes`, enforces `max_row_bytes` / `max_extracted_bytes`; `fn new(ds, cfg)`, `fn push(&mut self, row: &ValuesRow) -> Result<()>`, `fn finish(self) -> Result<(Vec<RecordBatch>, usize)>`
  - `pub(crate) enum Col { Str(Option<String>), Int(Option<i64>), Double(Option<f64>), Bool(Option<bool>), TsUs(Option<i64>), Fixed(Option<Vec<u8>>), Map(Vec<(String, Option<String>)>), ListI64(Vec<i64>), ListF64(Vec<f64>), Bytes(Vec<u8>) }` and `pub(crate) struct ValuesRow { pub cols: Vec<Col>, pub approx_bytes: usize }` whose `cols` follow the dataset schema order.

- [ ] **Step 1: Refactor `attrs.rs` to expose `AnyValueColumns`**

Replace the per-row `match ty` block in `AttrTable::from_batch` with a reusable struct (keep behavior identical):

```rust
/// The seven AnyValue columns (type, str, int, double, bool, bytes, ser) of an
/// attribute batch or a log body struct, cast to plain types.
pub(crate) struct AnyValueColumns {
    types: ArrayRef,
    strs: Option<ArrayRef>,
    ints: Option<ArrayRef>,
    doubles: Option<ArrayRef>,
    bools: Option<ArrayRef>,
    bytes: Option<ArrayRef>,
    sers: Option<ArrayRef>,
}

impl AnyValueColumns {
    /// Build from a column lookup (a RecordBatch or a StructArray).
    pub(crate) fn new(get: &dyn Fn(&str) -> Option<ArrayRef>) -> Result<Self> {
        let cast_opt = |name: &str, to: &DataType| -> Result<Option<ArrayRef>> {
            match get(name) {
                None => Ok(None),
                Some(c) if c.data_type() == to => Ok(Some(c)),
                Some(c) => Ok(Some(cast(&c, to)?)),
            }
        };
        Ok(Self {
            types: cast_opt("type", &DataType::UInt8)?.ok_or_else(|| Error::invalid("missing type column"))?,
            strs: cast_opt("str", &DataType::Utf8)?,
            ints: cast_opt("int", &DataType::Int64)?,
            doubles: cast_opt("double", &DataType::Float64)?,
            bools: cast_opt("bool", &DataType::Boolean)?,
            bytes: cast_opt("bytes", &DataType::Binary)?,
            sers: cast_opt("ser", &DataType::Binary)?,
        })
    }

    /// Typed value at a row; nulls in the value column become `Value::Null`.
    pub(crate) fn value_at(&self, row: usize, max_depth: usize) -> Result<Value> {
        if !self.types.is_valid(row) {
            return Ok(Value::Null);
        }
        let ty = self.types.as_primitive::<UInt8Type>().value(row);
        Ok(match ty {
            TYPE_EMPTY => Value::Null,
            TYPE_STR => match &self.strs {
                Some(a) if a.is_valid(row) => Value::Str(a.as_string::<i32>().value(row).to_string()),
                _ => Value::Null,
            },
            TYPE_INT => match &self.ints {
                Some(a) if a.is_valid(row) => Value::Int(a.as_primitive::<Int64Type>().value(row)),
                _ => Value::Null,
            },
            TYPE_DOUBLE => match &self.doubles {
                Some(a) if a.is_valid(row) => Value::Double(a.as_primitive::<Float64Type>().value(row)),
                _ => Value::Null,
            },
            TYPE_BOOL => match &self.bools {
                Some(a) if a.is_valid(row) => Value::Bool(a.as_boolean().value(row)),
                _ => Value::Null,
            },
            TYPE_BYTES => match &self.bytes {
                Some(a) if a.is_valid(row) => Value::Bytes(a.as_binary::<i32>().value(row).to_vec()),
                _ => Value::Null,
            },
            TYPE_MAP | TYPE_SLICE => match &self.sers {
                Some(a) if a.is_valid(row) => decode_cbor(a.as_binary::<i32>().value(row), max_depth)?,
                _ => Value::Null,
            },
            other => return Err(Error::invalid(format!("attribute type {other}"))),
        })
    }
}
```

In `from_batch`, construct `let any = AnyValueColumns::new(&|n| batch.column_by_name(n).cloned())?;` and use `any.value_at(row, max_depth)?` in the loop. Re-run `cargo test -p otel-arrow-dfe-series-lake attrs::tests` (still 2 passed).

- [ ] **Step 2: Write failing tests for logs extraction** (`src/extract/logs.rs`, `#[cfg(test)] mod tests`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Denormalize, DenormType, LakeConfig};
    use crate::schema::Dataset;
    use arrow::array::{Array, AsArray};
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue { key: k.into(), value: Some(AnyValue { value: Some(any_value::Value::StringValue(v.into())) }) }
    }

    fn logs_data() -> LogsData {
        let rec = |t: u64, body: &str, attrs: Vec<KeyValue>| LogRecord {
            time_unix_nano: t,
            observed_time_unix_nano: t + 1,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue { value: Some(any_value::Value::StringValue(body.into())) }),
            attributes: attrs,
            ..Default::default()
        };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource { attributes: vec![kv("host.id", "h1"), kv("service.name", "svc")], ..Default::default() }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        rec(1_000, "a", vec![kv("logger.name", "L1"), kv("request_id", "r1")]),
                        rec(2_000, "b", vec![kv("logger.name", "L1")]),
                        rec(0, "c", vec![kv("logger.name", "L2")]),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn cfg() -> LakeConfig {
        let mut cfg = LakeConfig::default();
        cfg.logs.series_attributes = vec!["logger.name".into()];
        cfg.logs.denormalize = vec![
            Denormalize { path: "resource.service.name".into(), column: "service_name".into(), ty: DenormType::String },
            Denormalize { path: "attrs.request_id".into(), column: "request_id".into(), ty: DenormType::String },
        ];
        cfg
    }

    /// Scenario: three log records, two distinct logger.name values, one zero timestamp.
    /// Guarantees: two descriptors, three value rows with series ids, allow-listed attrs
    /// excluded from the residual map, zero timestamp stored as null, denormalized columns filled.
    #[test]
    fn extracts_logs_descriptors_and_values() {
        let records = encode_logs(&logs_data());
        let out = extract(&records, &cfg()).expect("extract");
        assert_eq!(out.signal, Signal::Logs);
        assert_eq!(out.descriptors.len(), 2);
        let (ds, batches) = &out.values[0];
        assert_eq!(*ds, Dataset::LogsValues);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        let b = &batches[0];
        let ids = b.column_by_name("series_id").expect("series_id").as_fixed_size_binary();
        assert_eq!(ids.value(0), ids.value(1));
        assert_ne!(ids.value(0), ids.value(2));
        let producer = b.column_by_name("producer_id").expect("p").as_string::<i32>();
        assert_eq!(producer.value(0), "h1");
        let time = b.column_by_name("time_unix_nano").expect("t");
        assert!(time.is_null(2));
        let svc = b.column_by_name("service_name").expect("svc").as_string::<i32>();
        assert_eq!(svc.value(0), "svc");
        let rid = b.column_by_name("request_id").expect("rid").as_string::<i32>();
        assert_eq!(rid.value(0), "r1");
        assert!(rid.is_null(1));
        let attrs = b.column_by_name("attrs").expect("attrs").as_map();
        // row 0 residual attrs: only request_id (logger.name is identity)
        assert_eq!(attrs.value_length(0), 1);
        assert_eq!(out.stats.rows, 3);
        assert!(out.pinned_bytes > 0);
        // descriptor denorm: only the identity-path column (service_name)
        assert_eq!(out.descriptors[0].denorm.len(), 1);
    }

    /// Scenario: a descriptor row set is turned into a series batch.
    /// Guarantees: identity_bytes hashes to series_id and maps carry the attributes.
    #[test]
    fn series_batch_round_trip() {
        let records = encode_logs(&logs_data());
        let out = extract(&records, &cfg()).expect("extract");
        let rows: Vec<&DescriptorRow> = out.descriptors.iter().collect();
        let batch = series_batch(&rows, 1_700_000_000_000_000, Dataset::LogsSeries, &cfg()).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch.column_by_name("series_id").expect("id").as_fixed_size_binary();
        let ib = batch.column_by_name("identity_bytes").expect("ib").as_binary::<i32>();
        assert_eq!(ids.value(0), crate::canonical::series_id(ib.value(0)));
        let res = batch.column_by_name("resource_attrs").expect("r").as_map();
        assert_eq!(res.value_length(0), 2);
    }

    /// Scenario: max_extracted_bytes far below the request size.
    /// Guarantees: extraction stops with TooLarge instead of allocating everything.
    #[test]
    fn extracted_budget_is_enforced() {
        let mut cfg = cfg();
        cfg.ingress.max_extracted_bytes = 1;
        let records = encode_logs(&logs_data());
        assert!(matches!(extract(&records, &cfg), Err(Error::Refused(RefuseReason::TooLarge))));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake extract::logs::tests`
Expected: compile error.

- [ ] **Step 4: Implement `src/extract/mod.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extraction of descriptors and values from OTAP records (spec section 6.2 step 4).

pub mod logs;
pub mod metrics;

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder,
    Int64Builder, ListBuilder, MapBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};

use crate::canonical::{Descriptor, Signal, SeriesId, hex};
use crate::config::{DenormSource, DenormType, Denormalize, LakeConfig};
use crate::error::{Error, RefuseReason, Result};
use crate::schema::{Dataset, dataset_schema, denorm_columns};
use crate::value::{Value, map_string};

/// A typed denormalized value.
#[derive(Debug, Clone, PartialEq)]
pub enum DenormValue {
    /// String.
    Str(String),
    /// Int64.
    Int(i64),
    /// Double.
    Double(f64),
    /// Bool.
    Bool(bool),
}

/// One descriptor ready to become a `series` row.
#[derive(Debug, Clone)]
pub struct DescriptorRow {
    /// Series id.
    pub series_id: SeriesId,
    /// Canonical bytes.
    pub identity_bytes: Vec<u8>,
    /// Descriptor.
    pub descriptor: Descriptor,
    /// Denormalized identity columns, in `denorm_columns(series dataset)` order.
    pub denorm: Vec<Option<DenormValue>>,
    /// Approximate retained bytes of the row.
    pub approx_bytes: usize,
}

/// Counters produced by extraction.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExtractStats {
    /// Values rows produced.
    pub rows: usize,
    /// Points dropped under the drop policy.
    pub dropped_unsupported: u64,
    /// Timestamps outside `1..=i64::MAX`.
    pub timestamp_out_of_range: u64,
    /// Denormalized values stored as null because of a type mismatch.
    pub denorm_type_mismatch: u64,
}

/// Result of extracting one request.
#[derive(Debug)]
pub struct Extracted {
    /// Signal.
    pub signal: Signal,
    /// Unique descriptors of the request.
    pub descriptors: Vec<DescriptorRow>,
    /// Values batches per dataset, each batch at most `run_target_bytes`.
    pub values: Vec<(Dataset, Vec<RecordBatch>)>,
    /// Pinned bytes of all values batches.
    pub pinned_bytes: usize,
    /// Counters.
    pub stats: ExtractStats,
}

/// Extract descriptors and values from one OTAP request.
pub fn extract(records: &OtapArrowRecords, cfg: &LakeConfig) -> Result<Extracted> {
    match records {
        OtapArrowRecords::Logs(_) => logs::extract_logs(records, cfg),
        OtapArrowRecords::Metrics(_) => metrics::extract_metrics(records, cfg),
        OtapArrowRecords::Traces(_) => {
            Err(Error::Refused(RefuseReason::Unsupported("traces".into())))
        }
    }
}

/// Spec 5.1 timestamp rule on the converted `i64` nanoseconds.
pub(crate) fn timestamp_pair(ns: i64, stats: &mut ExtractStats) -> (Option<i64>, Option<i64>) {
    if ns == 0 {
        (None, None)
    } else if ns < 0 {
        stats.timestamp_out_of_range += 1;
        (None, None)
    } else {
        (Some(ns), Some(ns / 1000))
    }
}

fn lookup<'a>(list: &'a [(String, Value)], key: &str) -> Option<&'a Value> {
    list.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Resolve one denormalized column for a row.
pub(crate) fn denorm_lookup(
    d: &Denormalize,
    resource: &[(String, Value)],
    scope: &[(String, Value)],
    attrs: &[(String, Value)],
    stats: &mut ExtractStats,
) -> Option<DenormValue> {
    let (src, key) = d.source().ok()?;
    let v = match src {
        DenormSource::Resource => lookup(resource, key),
        DenormSource::Scope => lookup(scope, key),
        DenormSource::Attrs => lookup(attrs, key),
    }?;
    match (d.ty, v) {
        (DenormType::String, v) => map_string(v).map(DenormValue::Str),
        (DenormType::Int64, Value::Int(i)) => Some(DenormValue::Int(*i)),
        (DenormType::Double, Value::Double(f)) => Some(DenormValue::Double(*f)),
        (DenormType::Bool, Value::Bool(b)) => Some(DenormValue::Bool(*b)),
        (_, Value::Null) => None,
        _ => {
            stats.denorm_type_mismatch += 1;
            None
        }
    }
}

/// Producer id projection of a resource attribute list.
pub(crate) fn producer_id(resource: &[(String, Value)], attribute: &str) -> String {
    lookup(resource, attribute).and_then(map_string).unwrap_or_default()
}

/// A typed cell of a values row, in dataset schema order.
#[derive(Debug, Clone)]
pub(crate) enum Col {
    /// Utf8.
    Str(Option<String>),
    /// Int64.
    Int(Option<i64>),
    /// Int32.
    Int32(Option<i32>),
    /// Float64.
    Double(Option<f64>),
    /// Boolean.
    Bool(Option<bool>),
    /// Timestamp(us).
    TsUs(Option<i64>),
    /// FixedSizeBinary.
    Fixed(Option<Vec<u8>>),
    /// Map<Utf8, Utf8>.
    Map(Vec<(String, Option<String>)>),
    /// List<Int64>.
    ListI64(Vec<i64>),
    /// List<Float64>.
    ListF64(Vec<f64>),
    /// Binary.
    Bytes(Vec<u8>),
}

impl From<Option<DenormValue>> for Col {
    fn from(v: Option<DenormValue>) -> Self {
        match v {
            None => Col::Str(None),
            Some(DenormValue::Str(s)) => Col::Str(Some(s)),
            Some(DenormValue::Int(i)) => Col::Int(Some(i)),
            Some(DenormValue::Double(f)) => Col::Double(Some(f)),
            Some(DenormValue::Bool(b)) => Col::Bool(Some(b)),
        }
    }
}

/// One values row.
#[derive(Debug, Clone)]
pub(crate) struct ValuesRow {
    /// Cells in dataset schema order.
    pub cols: Vec<Col>,
    /// Approximate retained bytes.
    pub approx_bytes: usize,
}

enum AnyBuilder {
    Str(StringBuilder),
    Int(Int64Builder),
    Int32(Int32Builder),
    Double(Float64Builder),
    Bool(BooleanBuilder),
    TsUs(TimestampMicrosecondBuilder),
    Fixed(FixedSizeBinaryBuilder),
    Map(MapBuilder<StringBuilder, StringBuilder>),
    ListI64(ListBuilder<Int64Builder>),
    ListF64(ListBuilder<Float64Builder>),
    Bytes(BinaryBuilder),
}

fn builder_for(dt: &DataType) -> Result<AnyBuilder> {
    Ok(match dt {
        DataType::Utf8 => AnyBuilder::Str(StringBuilder::new()),
        DataType::Int64 => AnyBuilder::Int(Int64Builder::new()),
        DataType::Int32 => AnyBuilder::Int32(Int32Builder::new()),
        DataType::Float64 => AnyBuilder::Double(Float64Builder::new()),
        DataType::Boolean => AnyBuilder::Bool(BooleanBuilder::new()),
        DataType::Timestamp(_, tz) => AnyBuilder::TsUs(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone())),
        DataType::FixedSizeBinary(n) => AnyBuilder::Fixed(FixedSizeBinaryBuilder::new(*n)),
        DataType::Map(_, _) => AnyBuilder::Map(MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())),
        DataType::List(f) if f.data_type() == &DataType::Int64 => {
            AnyBuilder::ListI64(ListBuilder::new(Int64Builder::new()).with_field(f.clone()))
        }
        DataType::List(_) => AnyBuilder::ListF64(ListBuilder::new(Float64Builder::new()).with_field(match dt {
            DataType::List(f) => f.clone(),
            _ => unreachable!("matched List above"),
        })),
        DataType::Binary => AnyBuilder::Bytes(BinaryBuilder::new()),
        other => return Err(Error::invalid(format!("unsupported builder type {other}"))),
    })
}

fn append(b: &mut AnyBuilder, c: &Col) -> Result<()> {
    match (b, c) {
        (AnyBuilder::Str(b), Col::Str(v)) => b.append_option(v.as_deref()),
        (AnyBuilder::Int(b), Col::Int(v)) => b.append_option(*v),
        (AnyBuilder::Int32(b), Col::Int32(v)) => b.append_option(*v),
        (AnyBuilder::Double(b), Col::Double(v)) => b.append_option(*v),
        (AnyBuilder::Bool(b), Col::Bool(v)) => b.append_option(*v),
        (AnyBuilder::TsUs(b), Col::TsUs(v)) => b.append_option(*v),
        (AnyBuilder::Fixed(b), Col::Fixed(v)) => match v {
            Some(bytes) => b.append_value(bytes)?,
            None => b.append_null(),
        },
        (AnyBuilder::Map(b), Col::Map(entries)) => {
            for (k, v) in entries {
                b.keys().append_value(k);
                b.values().append_option(v.as_deref());
            }
            b.append(true)?;
        }
        (AnyBuilder::ListI64(b), Col::ListI64(items)) => {
            b.values().append_slice(items);
            b.append(true);
        }
        (AnyBuilder::ListF64(b), Col::ListF64(items)) => {
            b.values().append_slice(items);
            b.append(true);
        }
        (AnyBuilder::Bytes(b), Col::Bytes(v)) => b.append_value(v),
        // Denormalized columns arrive as Col::Str(None) when absent, whatever their type.
        (AnyBuilder::Int(b), Col::Str(None)) => b.append_null(),
        (AnyBuilder::Double(b), Col::Str(None)) => b.append_null(),
        (AnyBuilder::Bool(b), Col::Str(None)) => b.append_null(),
        (_, c) => return Err(Error::invalid(format!("column/builder mismatch for {c:?}"))),
    }
    Ok(())
}

fn finish(b: &mut AnyBuilder) -> ArrayRef {
    match b {
        AnyBuilder::Str(b) => Arc::new(b.finish()),
        AnyBuilder::Int(b) => Arc::new(b.finish()),
        AnyBuilder::Int32(b) => Arc::new(b.finish()),
        AnyBuilder::Double(b) => Arc::new(b.finish()),
        AnyBuilder::Bool(b) => Arc::new(b.finish()),
        AnyBuilder::TsUs(b) => Arc::new(b.finish()),
        AnyBuilder::Fixed(b) => Arc::new(b.finish()),
        AnyBuilder::Map(b) => Arc::new(b.finish()),
        AnyBuilder::ListI64(b) => Arc::new(b.finish()),
        AnyBuilder::ListF64(b) => Arc::new(b.finish()),
        AnyBuilder::Bytes(b) => Arc::new(b.finish()),
    }
}

/// Accumulates rows of one dataset into slices of at most `run_target_bytes`,
/// enforcing `max_row_bytes` and `max_extracted_bytes`.
pub(crate) struct RowSink {
    schema: SchemaRef,
    builders: Vec<AnyBuilder>,
    slice_bytes: usize,
    rows_in_slice: usize,
    batches: Vec<RecordBatch>,
    pinned: usize,
    seen: CountedAllocations,
    run_target: usize,
    max_row: usize,
    max_extracted: usize,
}

impl RowSink {
    pub(crate) fn new(ds: Dataset, cfg: &LakeConfig) -> Result<Self> {
        let schema = dataset_schema(ds, cfg);
        let builders = schema.fields().iter().map(|f| builder_for(f.data_type())).collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            builders,
            slice_bytes: 0,
            rows_in_slice: 0,
            batches: Vec::new(),
            pinned: 0,
            seen: CountedAllocations::default(),
            run_target: cfg.sorting.run_target_bytes,
            max_row: cfg.ingress.max_row_bytes,
            max_extracted: cfg.ingress.max_extracted_bytes,
        })
    }

    pub(crate) fn push(&mut self, row: &ValuesRow) -> Result<()> {
        if row.approx_bytes > self.max_row {
            return Err(Error::Refused(RefuseReason::TooLarge));
        }
        if row.cols.len() != self.builders.len() {
            return Err(Error::invalid("row width does not match dataset schema"));
        }
        for (b, c) in self.builders.iter_mut().zip(&row.cols) {
            append(b, c)?;
        }
        self.slice_bytes += row.approx_bytes;
        self.rows_in_slice += 1;
        if self.slice_bytes >= self.run_target {
            self.seal()?;
        }
        Ok(())
    }

    fn seal(&mut self) -> Result<()> {
        if self.rows_in_slice == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = self.builders.iter_mut().map(finish).collect();
        let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
        self.pinned += record_batch_pinned_bytes(&batch, &mut self.seen);
        if self.pinned > self.max_extracted {
            return Err(Error::Refused(RefuseReason::TooLarge));
        }
        self.batches.push(batch);
        self.slice_bytes = 0;
        self.rows_in_slice = 0;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(Vec<RecordBatch>, usize)> {
        self.seal()?;
        Ok((self.batches, self.pinned))
    }
}

fn map_col(list: &[(String, Value)]) -> Col {
    Col::Map(list.iter().map(|(k, v)| (k.clone(), map_string(v))).collect())
}

/// Build a `series` batch from descriptor rows.
pub fn series_batch(rows: &[&DescriptorRow], emitted_at_us: i64, ds: Dataset, cfg: &LakeConfig) -> Result<RecordBatch> {
    let schema = dataset_schema(ds, cfg);
    let mut builders = schema.fields().iter().map(|f| builder_for(f.data_type())).collect::<Result<Vec<_>>>()?;
    for r in rows {
        let d = &r.descriptor;
        let mut cols: Vec<Col> = vec![
            Col::Fixed(Some(r.series_id.to_vec())),
            Col::Bytes(r.identity_bytes.clone()),
            Col::TsUs(Some(emitted_at_us)),
            Col::Str(Some(d.resource_schema_url.clone())),
            map_col(&d.resource_attrs),
            Col::Str(Some(d.scope_name.clone())),
            Col::Str(Some(d.scope_version.clone())),
            Col::Str(Some(d.scope_schema_url.clone())),
            map_col(&d.scope_attrs),
            map_col(&d.attrs),
        ];
        if ds == Dataset::MetricsSeries {
            let m = d.metric.as_ref().ok_or_else(|| Error::invalid("metrics descriptor without metric"))?;
            cols.extend([
                Col::Str(Some(m.name.clone())),
                Col::Str(Some(m.unit.clone())),
                Col::Str(Some(m.kind.as_str().to_string())),
                Col::Str(Some(m.temporality.as_str().to_string())),
                Col::Bool(Some(m.is_monotonic)),
                Col::Str(Some(m.description.clone())),
            ]);
        }
        cols.extend(r.denorm.iter().cloned().map(Col::from));
        for (b, c) in builders.iter_mut().zip(&cols) {
            append(b, c)?;
        }
    }
    let arrays: Vec<ArrayRef> = builders.iter_mut().map(finish).collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Descriptor row constructor shared by logs and metrics.
pub(crate) fn descriptor_row(descriptor: Descriptor, ds_series: Dataset, cfg: &LakeConfig, stats: &mut ExtractStats) -> DescriptorRow {
    let identity_bytes = crate::canonical::canonical_bytes(&descriptor);
    let series_id = crate::canonical::series_id(&identity_bytes);
    let denorm = denorm_columns(ds_series, cfg)
        .into_iter()
        .map(|d| denorm_lookup(d, &descriptor.resource_attrs, &descriptor.scope_attrs, &descriptor.attrs, stats))
        .collect();
    let approx_bytes = identity_bytes.len() * 2 + 256;
    let _ = hex(&series_id);
    DescriptorRow { series_id, identity_bytes, descriptor, denorm, approx_bytes }
}
```

- [ ] **Step 5: Implement `src/extract/logs.rs`**

The OTAP `Logs` batch (pdata `schema/payloads.rs`, `mod logs`) has columns `time_unix_nano` (Timestamp ns), `observed_time_unix_nano`, `body` (Struct: `type` UInt8, `str`, `int`, `double`, `bool`, `bytes`, `ser`, dictionary-encoded), `id` (UInt16), `severity_number` (Dictionary<UInt8, Int32>), `severity_text` (Dictionary<UInt8, Utf8>), `dropped_attributes_count`, `event_name` (Dictionary), `flags` (UInt32), `trace_id` (Dictionary<UInt8, FixedSizeBinary(16)>), `span_id` (Dictionary<UInt8, FixedSizeBinary(8)>), `schema_url` (Dictionary), `resource` (Struct: `id` UInt16, `dropped_attributes_count`, `schema_url` Dictionary), `scope` (Struct: `id` UInt16, `dropped_attributes_count`, `name`, `version` Dictionaries). Every column is optional. Attribute batches: `ResourceAttrs` (parent = `resource.id`), `ScopeAttrs` (parent = `scope.id`), `LogAttrs` (parent = `id`).

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Logs extraction.

use std::collections::HashMap;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int32Type, TimestampNanosecondType, UInt16Type, UInt32Type};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use super::{Col, DescriptorRow, ExtractStats, Extracted, RowSink, ValuesRow, denorm_lookup, descriptor_row, producer_id, timestamp_pair};
use crate::attrs::{AnyValueColumns, AttrTable};
use crate::canonical::{Descriptor, SeriesId, Signal};
use crate::config::LakeConfig;
use crate::error::{Error, Result};
use crate::schema::{Dataset, denorm_columns};
use crate::value::{Value, body_string, value_bytes};

/// Plain (dictionary-free) column of a batch, or `None` if absent.
pub(crate) fn plain_col(batch: &RecordBatch, name: &str, to: &DataType) -> Result<Option<ArrayRef>> {
    match batch.column_by_name(name) {
        None => Ok(None),
        Some(c) if c.data_type() == to => Ok(Some(c.clone())),
        Some(c) => Ok(Some(cast(c, to)?)),
    }
}

/// Child of a struct column, cast to a plain type, or `None`.
pub(crate) fn struct_child(batch: &RecordBatch, parent: &str, child: &str, to: &DataType) -> Result<Option<ArrayRef>> {
    let Some(col) = batch.column_by_name(parent) else { return Ok(None) };
    let s = col.as_struct();
    match s.column_by_name(child) {
        None => Ok(None),
        Some(c) if c.data_type() == to => Ok(Some(c.clone())),
        Some(c) => Ok(Some(cast(c, to)?)),
    }
}

fn u16_at(a: &Option<ArrayRef>, row: usize) -> u32 {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| u32::from(a.as_primitive::<UInt16Type>().value(row)))).unwrap_or(0)
}

fn str_at(a: &Option<ArrayRef>, row: usize) -> String {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_string::<i32>().value(row).to_string())).unwrap_or_default()
}

fn i64_at(a: &Option<ArrayRef>, row: usize) -> i64 {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<TimestampNanosecondType>().value(row))).unwrap_or(0)
}

fn fixed_at(a: &Option<ArrayRef>, row: usize) -> Option<Vec<u8>> {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_fixed_size_binary().value(row).to_vec()))
}

/// Attribute table for a payload type, empty when the payload is absent.
pub(crate) fn attr_table(records: &OtapArrowRecords, pt: ArrowPayloadType, max_depth: usize) -> Result<AttrTable> {
    match records.get(pt) {
        Some(b) => AttrTable::from_batch(b, max_depth),
        None => Ok(AttrTable::default()),
    }
}

pub(crate) fn extract_logs(records: &OtapArrowRecords, cfg: &LakeConfig) -> Result<Extracted> {
    let depth = cfg.ingress.max_nesting_depth;
    let mut stats = ExtractStats::default();
    let Some(logs) = records.get(ArrowPayloadType::Logs) else {
        return Ok(Extracted { signal: Signal::Logs, descriptors: vec![], values: vec![], pinned_bytes: 0, stats });
    };
    let resource_attrs = attr_table(records, ArrowPayloadType::ResourceAttrs, depth)?;
    let scope_attrs = attr_table(records, ArrowPayloadType::ScopeAttrs, depth)?;
    let log_attrs = attr_table(records, ArrowPayloadType::LogAttrs, depth)?;

    let ts_ns = DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None);
    let time = plain_col(logs, "time_unix_nano", &ts_ns)?;
    let observed = plain_col(logs, "observed_time_unix_nano", &ts_ns)?;
    let id = plain_col(logs, "id", &DataType::UInt16)?;
    let severity_number = plain_col(logs, "severity_number", &DataType::Int32)?;
    let severity_text = plain_col(logs, "severity_text", &DataType::Utf8)?;
    let event_name = plain_col(logs, "event_name", &DataType::Utf8)?;
    let flags = plain_col(logs, "flags", &DataType::UInt32)?;
    let trace_id = plain_col(logs, "trace_id", &DataType::FixedSizeBinary(16))?;
    let span_id = plain_col(logs, "span_id", &DataType::FixedSizeBinary(8))?;
    let res_id = struct_child(logs, "resource", "id", &DataType::UInt16)?;
    let res_schema = struct_child(logs, "resource", "schema_url", &DataType::Utf8)?;
    let scope_id = struct_child(logs, "scope", "id", &DataType::UInt16)?;
    let scope_name = struct_child(logs, "scope", "name", &DataType::Utf8)?;
    let scope_version = struct_child(logs, "scope", "version", &DataType::Utf8)?;
    let scope_schema = plain_col(logs, "schema_url", &DataType::Utf8)?;
    let body = match logs.column_by_name("body") {
        Some(c) => {
            let s = c.as_struct().clone();
            Some(AnyValueColumns::new(&|n| s.column_by_name(n).cloned())?)
        }
        None => None,
    };

    let allow: &[String] = &cfg.logs.series_attributes;
    let values_denorm = denorm_columns(Dataset::LogsValues, cfg);
    let mut sink = RowSink::new(Dataset::LogsValues, cfg)?;
    let mut descriptors: Vec<DescriptorRow> = Vec::new();
    let mut seen: HashMap<SeriesId, usize> = HashMap::new();
    let mut memo: HashMap<(u32, u32, Vec<u8>), SeriesId> = HashMap::new();

    for row in 0..logs.num_rows() {
        let rid = u16_at(&res_id, row);
        let sid = u16_at(&scope_id, row);
        let lid = u16_at(&id, row);
        let resource = resource_attrs.get(rid);
        let scope = scope_attrs.get(sid);
        let all_attrs = log_attrs.get(lid);
        let (identity_attrs, residual): (Vec<(String, Value)>, Vec<(String, Value)>) =
            all_attrs.iter().cloned().partition(|(k, _)| allow.iter().any(|a| a == k));
        let identity_key = crate::canonical::canonical_bytes(&Descriptor {
            signal: Signal::Logs,
            resource_attrs: vec![],
            resource_schema_url: String::new(),
            scope_name: String::new(),
            scope_version: String::new(),
            scope_schema_url: String::new(),
            scope_attrs: vec![],
            metric: None,
            attrs: identity_attrs.clone(),
        });
        let series_id = match memo.get(&(rid, sid, identity_key.clone())) {
            Some(id) => *id,
            None => {
                let descriptor = Descriptor {
                    signal: Signal::Logs,
                    resource_attrs: resource.to_vec(),
                    resource_schema_url: str_at(&res_schema, row),
                    scope_name: str_at(&scope_name, row),
                    scope_version: str_at(&scope_version, row),
                    scope_schema_url: str_at(&scope_schema, row),
                    scope_attrs: scope.to_vec(),
                    metric: None,
                    attrs: identity_attrs.clone(),
                };
                let dr = descriptor_row(descriptor, Dataset::LogsSeries, cfg, &mut stats);
                let id = dr.series_id;
                if !seen.contains_key(&id) {
                    let _ = seen.insert(id, descriptors.len());
                    descriptors.push(dr);
                }
                let _ = memo.insert((rid, sid, identity_key), id);
                id
            }
        };

        let (t_ns, t_us) = timestamp_pair(i64_at(&time, row), &mut stats);
        let (o_ns, o_us) = timestamp_pair(i64_at(&observed, row), &mut stats);
        let body_value = match &body {
            Some(b) => b.value_at(row, depth)?,
            None => Value::Null,
        };
        let body_str = body_string(&body_value);
        let mut approx = 64 + body_str.as_ref().map_or(0, String::len);
        approx += residual.iter().map(|(k, v)| k.len() + value_bytes(v)).sum::<usize>();
        let mut cols = vec![
            Col::Fixed(Some(series_id.to_vec())),
            Col::Str(Some(producer_id(resource, &cfg.producer_id_attribute))),
            Col::TsUs(t_us),
            Col::Int(t_ns),
            Col::TsUs(o_us),
            Col::Int(o_ns),
            Col::Int32(Some(severity_number.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<Int32Type>().value(row))).unwrap_or(0))),
            Col::Str(Some(str_at(&severity_text, row))),
            Col::Str(body_str),
            Col::Str(Some(str_at(&event_name, row))),
            Col::Fixed(fixed_at(&trace_id, row)),
            Col::Fixed(fixed_at(&span_id, row)),
            Col::Int32(Some(flags.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<UInt32Type>().value(row))).unwrap_or(0) as i32)),
            Col::Map(residual.iter().map(|(k, v)| (k.clone(), crate::value::map_string(v))).collect()),
        ];
        for d in &values_denorm {
            cols.push(Col::from(denorm_lookup(d, resource, scope, all_attrs, &mut stats)));
        }
        sink.push(&ValuesRow { cols, approx_bytes: approx })?;
        stats.rows += 1;
    }
    let (batches, pinned_bytes) = sink.finish()?;
    let values = if batches.is_empty() { vec![] } else { vec![(Dataset::LogsValues, batches)] };
    if descriptors.is_empty() && !values.is_empty() {
        return Err(Error::invalid("values without descriptors"));
    }
    Ok(Extracted { signal: Signal::Logs, descriptors, values, pinned_bytes, stats })
}
```

`flags as i32` is a bit reinterpretation of `u32` into the signed storage column; add a comment `// stored as received; readers treat it as a bit set` and allow the lint locally with `#[allow(clippy::cast_possible_wrap)]` on the function if clippy complains (`trivial_numeric_casts` does not fire for `u32 as i32`).

Create `src/extract/metrics.rs` with a stub for now so the crate compiles:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics extraction (task 7).

use otel_arrow_dfe_pdata::otap::OtapArrowRecords;

use super::Extracted;
use crate::config::LakeConfig;
use crate::error::{Error, RefuseReason, Result};

pub(crate) fn extract_metrics(_records: &OtapArrowRecords, _cfg: &LakeConfig) -> Result<Extracted> {
    Err(Error::Refused(RefuseReason::Unsupported("metrics: not implemented".into())))
}
```

Add `pub mod extract;` to `src/lib.rs`.

- [ ] **Step 6: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake extract::logs::tests`
Expected: 3 passed. If `encode_logs` places `logger.name` rows out of `parent_id` order, `AttrTable` handles it (hash map grouping). If the `body` struct's children are dictionary-encoded, `AnyValueColumns::new` casts them.

- [ ] **Step 7: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): extraction core and logs extraction

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Metrics extraction (number and histogram points)

**Files:**
- Modify: `rust/otap-dataflow/crates/series-lake/src/extract/metrics.rs` (replace the stub)

**Interfaces:**
- Consumes: everything from task 6 (`RowSink`, `Col`, `ValuesRow`, `descriptor_row`, `timestamp_pair`, `denorm_lookup`, `producer_id`, `attr_table`, `plain_col`, `struct_child`), `canonical::{MetricDescriptor, MetricKind, Temporality}`.
- Produces: `pub(crate) fn extract_metrics(records: &OtapArrowRecords, cfg: &LakeConfig) -> Result<Extracted>` returning `values` with `Dataset::MetricsNumber` and/or `Dataset::MetricsHistogram`.

OTAP layout (pdata `schema/payloads.rs`): `UnivariateMetrics` columns `id` UInt16, `metric_type` UInt8 (1 gauge, 2 sum, 3 histogram, 4 exp histogram, 5 summary), `name`, `aggregation_temporality` (Dictionary<UInt8, Int32>: 0 unspecified, 1 delta, 2 cumulative), `description`, `is_monotonic` Boolean, `unit`, `schema_url`, `resource` struct, `scope` struct. `NumberDataPoints`: `parent_id` UInt16 (metric id), `start_time_unix_nano`, `time_unix_nano`, `int_value` Int64, `double_value` Float64, `id` UInt32, `flags` UInt32; attributes in `NumberDpAttrs` keyed by the point `id`. `HistogramDataPoints`: `parent_id`, `id`, `count` UInt64, `sum`, `min`, `max`, `bucket_counts` List<UInt64>, `explicit_bounds` List<Float64>, `start_time_unix_nano`, `time_unix_nano`, `flags`; attributes in `HistogramDpAttrs`. `ExpHistogramDataPoints` and `SummaryDataPoints` are unsupported in v1. Exemplar payloads are simply not read.

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `metrics.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{LakeConfig, UnsupportedPolicy};
    use crate::error::{Error, RefuseReason};
    use crate::schema::Dataset;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Float64Type, Int64Type};
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric,
        number_data_point,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_metrics;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue { key: k.into(), value: Some(AnyValue { value: Some(any_value::Value::StringValue(v.into())) }) }
    }

    fn dp(t: u64, v: number_data_point::Value, attrs: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint { time_unix_nano: t, value: Some(v), attributes: attrs, ..Default::default() }
    }

    fn data(metrics: Vec<Metric>) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource { attributes: vec![kv("host.id", "h1")], ..Default::default() }),
                scope_metrics: vec![ScopeMetrics { metrics, ..Default::default() }],
                ..Default::default()
            }],
        }
    }

    fn gauge_and_hist() -> MetricsData {
        data(vec![
            Metric {
                name: "cpu".into(),
                unit: "1".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![
                        dp(10, number_data_point::Value::AsInt(i64::MAX), vec![kv("cpu", "0")]),
                        dp(20, number_data_point::Value::AsDouble(0.5), vec![kv("cpu", "0")]),
                        dp(30, number_data_point::Value::AsDouble(0.7), vec![kv("cpu", "1")]),
                    ],
                })),
                ..Default::default()
            },
            Metric {
                name: "lat".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    data_points: vec![HistogramDataPoint {
                        time_unix_nano: 40,
                        count: 3,
                        sum: Some(6.0),
                        bucket_counts: vec![1, 2],
                        explicit_bounds: vec![5.0],
                        ..Default::default()
                    }],
                })),
                ..Default::default()
            },
        ])
    }

    /// Scenario: a gauge with two series and a cumulative histogram.
    /// Guarantees: three descriptors, number rows keep int and double separately with
    /// INT64_MAX intact, histogram lists are stored as signed integers.
    #[test]
    fn extracts_number_and_histogram() {
        let out = extract_metrics(&encode_metrics(&gauge_and_hist()), &LakeConfig::default()).expect("extract");
        assert_eq!(out.descriptors.len(), 3);
        let number = out.values.iter().find(|(d, _)| *d == Dataset::MetricsNumber).expect("number").1[0].clone();
        assert_eq!(number.num_rows(), 3);
        let vi = number.column_by_name("value_int").expect("vi").as_primitive::<Int64Type>();
        let vd = number.column_by_name("value_double").expect("vd").as_primitive::<Float64Type>();
        assert_eq!(vi.value(0), i64::MAX);
        assert!(vd.is_null(0));
        assert!(vi.is_null(1));
        assert_eq!(vd.value(1), 0.5);
        let ids = number.column_by_name("series_id").expect("id").as_fixed_size_binary();
        assert_eq!(ids.value(0), ids.value(1));
        assert_ne!(ids.value(0), ids.value(2));
        let name = number.column_by_name("metric_name").expect("n").as_string::<i32>();
        assert_eq!(name.value(0), "cpu");
        let hist = out.values.iter().find(|(d, _)| *d == Dataset::MetricsHistogram).expect("hist").1[0].clone();
        assert_eq!(hist.num_rows(), 1);
        let bc = hist.column_by_name("bucket_counts").expect("bc").as_list::<i32>();
        assert_eq!(bc.value(0).as_primitive::<Int64Type>().values(), &[1i64, 2]);
        let kinds: Vec<_> = out.descriptors.iter().map(|d| d.descriptor.metric.as_ref().expect("m").kind).collect();
        assert!(kinds.contains(&MetricKind::Histogram));
    }

    /// Scenario: a sum without temporality.
    /// Guarantees: the request is refused as invalid.
    #[test]
    fn unspecified_temporality_is_refused() {
        let md = data(vec![Metric {
            name: "s".into(),
            data: Some(metric::Data::Sum(Sum {
                aggregation_temporality: AggregationTemporality::Unspecified as i32,
                is_monotonic: true,
                data_points: vec![dp(1, number_data_point::Value::AsInt(1), vec![])],
            })),
            ..Default::default()
        }]);
        assert!(matches!(extract_metrics(&encode_metrics(&md), &LakeConfig::default()), Err(Error::Refused(RefuseReason::Invalid(_)))));
    }

    /// Scenario: an exponential histogram under reject and under drop.
    /// Guarantees: reject refuses the whole request; drop yields zero rows and counts one drop.
    #[test]
    fn exp_histogram_policy() {
        let md = data(vec![Metric {
            name: "e".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: vec![ExponentialHistogramDataPoint { time_unix_nano: 1, count: 1, ..Default::default() }],
            })),
            ..Default::default()
        }]);
        let records = encode_metrics(&md);
        assert!(matches!(extract_metrics(&records, &LakeConfig::default()), Err(Error::Refused(RefuseReason::Unsupported(_)))));
        let mut cfg = LakeConfig::default();
        cfg.unsupported = UnsupportedPolicy::Drop;
        let out = extract_metrics(&records, &cfg).expect("drop");
        assert_eq!(out.stats.rows, 0);
        assert_eq!(out.stats.dropped_unsupported, 1);
        assert!(out.values.is_empty());
    }

    /// Scenario: a histogram whose bucket_counts length is not bounds + 1.
    /// Guarantees: refused as invalid.
    #[test]
    fn inconsistent_histogram_is_refused() {
        let md = data(vec![Metric {
            name: "h".into(),
            data: Some(metric::Data::Histogram(Histogram {
                aggregation_temporality: AggregationTemporality::Delta as i32,
                data_points: vec![HistogramDataPoint { count: 1, bucket_counts: vec![1, 1, 1], explicit_bounds: vec![1.0], ..Default::default() }],
            })),
            ..Default::default()
        }]);
        assert!(matches!(extract_metrics(&encode_metrics(&md), &LakeConfig::default()), Err(Error::Refused(RefuseReason::Invalid(_)))));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake extract::metrics::tests`
Expected: 4 failed (stub returns Unsupported).

- [ ] **Step 3: Implement `extract_metrics`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics extraction: number and histogram points (spec section 5.1).

use std::collections::HashMap;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int32Type, Int64Type, TimeUnit, UInt8Type, UInt16Type, UInt32Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

use super::logs::{attr_table, plain_col, struct_child};
use super::{Col, DescriptorRow, ExtractStats, Extracted, RowSink, ValuesRow, denorm_lookup, descriptor_row, producer_id, timestamp_pair};
use crate::attrs::AttrTable;
use crate::canonical::{Descriptor, MetricDescriptor, MetricKind, SeriesId, Signal, Temporality};
use crate::config::{LakeConfig, UnsupportedPolicy};
use crate::error::{Error, RefuseReason, Result};
use crate::schema::{Dataset, denorm_columns};
use crate::value::Value;

/// Per-metric fields read once from the `UnivariateMetrics` batch.
struct MetricRow {
    resource_id: u32,
    scope_id: u32,
    descriptor_base: Descriptor,
}

fn opt_str(a: &Option<ArrayRef>, row: usize) -> String {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_string::<i32>().value(row).to_string())).unwrap_or_default()
}

fn opt_u16(a: &Option<ArrayRef>, row: usize) -> u32 {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| u32::from(a.as_primitive::<UInt16Type>().value(row)))).unwrap_or(0)
}

fn ts(a: &Option<ArrayRef>, row: usize) -> i64 {
    a.as_ref()
        .and_then(|a| a.is_valid(row).then(|| a.as_primitive::<arrow::datatypes::TimestampNanosecondType>().value(row)))
        .unwrap_or(0)
}

fn flags(a: &Option<ArrayRef>, row: usize) -> i32 {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<UInt32Type>().value(row))).unwrap_or(0) as i32
}

fn metric_rows(records: &OtapArrowRecords, resource_attrs: &AttrTable, scope_attrs: &AttrTable) -> Result<HashMap<u32, MetricRow>> {
    let Some(m) = records.get(ArrowPayloadType::UnivariateMetrics) else { return Ok(HashMap::new()) };
    let id = plain_col(m, "id", &DataType::UInt16)?;
    let kind = plain_col(m, "metric_type", &DataType::UInt8)?;
    let name = plain_col(m, "name", &DataType::Utf8)?;
    let temporality = plain_col(m, "aggregation_temporality", &DataType::Int32)?;
    let description = plain_col(m, "description", &DataType::Utf8)?;
    let is_monotonic = plain_col(m, "is_monotonic", &DataType::Boolean)?;
    let unit = plain_col(m, "unit", &DataType::Utf8)?;
    let scope_schema = plain_col(m, "schema_url", &DataType::Utf8)?;
    let res_id = struct_child(m, "resource", "id", &DataType::UInt16)?;
    let res_schema = struct_child(m, "resource", "schema_url", &DataType::Utf8)?;
    let scope_id = struct_child(m, "scope", "id", &DataType::UInt16)?;
    let scope_name = struct_child(m, "scope", "name", &DataType::Utf8)?;
    let scope_version = struct_child(m, "scope", "version", &DataType::Utf8)?;
    let mut out = HashMap::with_capacity(m.num_rows());
    for row in 0..m.num_rows() {
        let kind_u8 = kind.as_ref().map_or(0, |a| a.as_primitive::<UInt8Type>().value(row));
        let kind = match kind_u8 {
            1 => MetricKind::Gauge,
            2 => MetricKind::Sum,
            3 => MetricKind::Histogram,
            4 => MetricKind::ExpHistogram,
            5 => MetricKind::Summary,
            other => return Err(Error::invalid(format!("metric_type {other}"))),
        };
        let temporality = match temporality.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<Int32Type>().value(row))) {
            Some(1) => Temporality::Delta,
            Some(2) => Temporality::Cumulative,
            _ => Temporality::Unspecified,
        };
        if kind != MetricKind::Gauge && temporality == Temporality::Unspecified {
            return Err(Error::invalid("sum or histogram with unspecified temporality"));
        }
        let rid = opt_u16(&res_id, row);
        let sid = opt_u16(&scope_id, row);
        let descriptor_base = Descriptor {
            signal: Signal::Metrics,
            resource_attrs: resource_attrs.get(rid).to_vec(),
            resource_schema_url: opt_str(&res_schema, row),
            scope_name: opt_str(&scope_name, row),
            scope_version: opt_str(&scope_version, row),
            scope_schema_url: opt_str(&scope_schema, row),
            scope_attrs: scope_attrs.get(sid).to_vec(),
            metric: Some(MetricDescriptor {
                name: opt_str(&name, row),
                unit: opt_str(&unit, row),
                kind,
                temporality: if kind == MetricKind::Gauge { Temporality::Unspecified } else { temporality },
                is_monotonic: kind == MetricKind::Sum && is_monotonic.as_ref().is_some_and(|a| a.is_valid(row) && a.as_boolean().value(row)),
                description: opt_str(&description, row),
            }),
            attrs: vec![],
        };
        let _ = out.insert(opt_u16(&id, row), MetricRow { resource_id: rid, scope_id: sid, descriptor_base });
    }
    Ok(out)
}

struct Common<'a> {
    cfg: &'a LakeConfig,
    metrics: &'a HashMap<u32, MetricRow>,
    descriptors: Vec<DescriptorRow>,
    seen: HashMap<SeriesId, usize>,
    stats: ExtractStats,
}

impl Common<'_> {
    /// Descriptor for (metric, point attrs); returns the series id and the resolved descriptor index.
    fn series_for(&mut self, metric_id: u32, attrs: &[(String, Value)]) -> Result<(SeriesId, usize)> {
        let m = self.metrics.get(&metric_id).ok_or_else(|| Error::invalid("data point references unknown metric"))?;
        let mut d = m.descriptor_base.clone();
        d.attrs = attrs.to_vec();
        let dr = descriptor_row(d, Dataset::MetricsSeries, self.cfg, &mut self.stats);
        let id = dr.series_id;
        let idx = match self.seen.get(&id) {
            Some(i) => *i,
            None => {
                let i = self.descriptors.len();
                let _ = self.seen.insert(id, i);
                self.descriptors.push(dr);
                i
            }
        };
        Ok((id, idx))
    }
}

fn opt_i64(a: &Option<ArrayRef>, row: usize) -> Option<i64> {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<Int64Type>().value(row)))
}

fn opt_f64(a: &Option<ArrayRef>, row: usize) -> Option<f64> {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<Float64Type>().value(row)))
}

fn opt_u32(a: &Option<ArrayRef>, row: usize) -> u32 {
    a.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<UInt32Type>().value(row))).unwrap_or(0)
}

fn common_cols(id: SeriesId, m: &MetricRow, cfg: &LakeConfig, t_ns: i64, s_ns: i64, fl: i32, stats: &mut ExtractStats) -> Vec<Col> {
    let (t_ns, t_us) = timestamp_pair(t_ns, stats);
    let (s_ns, s_us) = timestamp_pair(s_ns, stats);
    vec![
        Col::Fixed(Some(id.to_vec())),
        Col::Str(Some(producer_id(&m.descriptor_base.resource_attrs, &cfg.producer_id_attribute))),
        Col::Str(Some(m.descriptor_base.metric.as_ref().map(|x| x.name.clone()).unwrap_or_default())),
        Col::TsUs(t_us),
        Col::Int(t_ns),
        Col::TsUs(s_us),
        Col::Int(s_ns),
        Col::Int32(Some(fl)),
    ]
}

fn push_denorm(cols: &mut Vec<Col>, ds: Dataset, m: &MetricRow, attrs: &[(String, Value)], cfg: &LakeConfig, stats: &mut ExtractStats) {
    for d in denorm_columns(ds, cfg) {
        cols.push(Col::from(denorm_lookup(d, &m.descriptor_base.resource_attrs, &m.descriptor_base.scope_attrs, attrs, stats)));
    }
}

pub(crate) fn extract_metrics(records: &OtapArrowRecords, cfg: &LakeConfig) -> Result<Extracted> {
    let depth = cfg.ingress.max_nesting_depth;
    let resource_attrs = attr_table(records, ArrowPayloadType::ResourceAttrs, depth)?;
    let scope_attrs = attr_table(records, ArrowPayloadType::ScopeAttrs, depth)?;
    let metrics = metric_rows(records, &resource_attrs, &scope_attrs)?;
    let mut c = Common { cfg, metrics: &metrics, descriptors: Vec::new(), seen: HashMap::new(), stats: ExtractStats::default() };

    // Unsupported point kinds.
    for pt in [ArrowPayloadType::ExpHistogramDataPoints, ArrowPayloadType::SummaryDataPoints] {
        if let Some(b) = records.get(pt) {
            if b.num_rows() > 0 {
                match cfg.unsupported {
                    UnsupportedPolicy::Reject => {
                        return Err(Error::Refused(RefuseReason::Unsupported(format!("{pt:?}"))));
                    }
                    UnsupportedPolicy::Drop => c.stats.dropped_unsupported += b.num_rows() as u64,
                }
            }
        }
    }

    let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let mut values = Vec::new();
    let mut pinned_bytes = 0;

    // Number points.
    if let Some(b) = records.get(ArrowPayloadType::NumberDataPoints) {
        let attrs = attr_table(records, ArrowPayloadType::NumberDpAttrs, depth)?;
        let parent = plain_col(b, "parent_id", &DataType::UInt16)?;
        let pid = plain_col(b, "id", &DataType::UInt32)?;
        let start = plain_col(b, "start_time_unix_nano", &ts_ns)?;
        let time = plain_col(b, "time_unix_nano", &ts_ns)?;
        let iv = plain_col(b, "int_value", &DataType::Int64)?;
        let dv = plain_col(b, "double_value", &DataType::Float64)?;
        let fl = plain_col(b, "flags", &DataType::UInt32)?;
        let mut sink = RowSink::new(Dataset::MetricsNumber, cfg)?;
        for row in 0..b.num_rows() {
            let metric_id = opt_u16(&parent, row);
            let point_attrs = attrs.get(opt_u32(&pid, row));
            let (id, _) = c.series_for(metric_id, point_attrs)?;
            let m = &metrics[&metric_id];
            let mut cols = common_cols(id, m, cfg, ts(&time, row), ts(&start, row), flags(&fl, row), &mut c.stats);
            cols.push(Col::Int(opt_i64(&iv, row)));
            cols.push(Col::Double(opt_f64(&dv, row)));
            push_denorm(&mut cols, Dataset::MetricsNumber, m, point_attrs, cfg, &mut c.stats);
            sink.push(&ValuesRow { cols, approx_bytes: 96 })?;
            c.stats.rows += 1;
        }
        let (batches, pinned) = sink.finish()?;
        pinned_bytes += pinned;
        if !batches.is_empty() {
            values.push((Dataset::MetricsNumber, batches));
        }
    }

    // Histogram points.
    if let Some(b) = records.get(ArrowPayloadType::HistogramDataPoints) {
        let attrs = attr_table(records, ArrowPayloadType::HistogramDpAttrs, depth)?;
        let parent = plain_col(b, "parent_id", &DataType::UInt16)?;
        let pid = plain_col(b, "id", &DataType::UInt32)?;
        let start = plain_col(b, "start_time_unix_nano", &ts_ns)?;
        let time = plain_col(b, "time_unix_nano", &ts_ns)?;
        let count = plain_col(b, "count", &DataType::UInt64)?;
        let sum = plain_col(b, "sum", &DataType::Float64)?;
        let min = plain_col(b, "min", &DataType::Float64)?;
        let max = plain_col(b, "max", &DataType::Float64)?;
        let fl = plain_col(b, "flags", &DataType::UInt32)?;
        let bc = b.column_by_name("bucket_counts");
        let eb = b.column_by_name("explicit_bounds");
        let mut sink = RowSink::new(Dataset::MetricsHistogram, cfg)?;
        for row in 0..b.num_rows() {
            let metric_id = opt_u16(&parent, row);
            let point_attrs = attrs.get(opt_u32(&pid, row));
            let (id, _) = c.series_for(metric_id, point_attrs)?;
            let m = &metrics[&metric_id];
            let counts: Vec<i64> = match bc {
                Some(a) if a.is_valid(row) => {
                    let list = a.as_list::<i32>().value(row);
                    let vals = arrow::compute::cast(&list, &DataType::UInt64)?;
                    let mut out = Vec::with_capacity(vals.len());
                    for v in vals.as_primitive::<UInt64Type>().iter() {
                        let v = v.unwrap_or(0);
                        out.push(i64::try_from(v).map_err(|_| Error::invalid("bucket count above i64::MAX"))?);
                    }
                    out
                }
                _ => vec![],
            };
            let bounds: Vec<f64> = match eb {
                Some(a) if a.is_valid(row) => {
                    let list = a.as_list::<i32>().value(row);
                    let vals = arrow::compute::cast(&list, &DataType::Float64)?;
                    vals.as_primitive::<Float64Type>().iter().map(|v| v.unwrap_or(0.0)).collect()
                }
                _ => vec![],
            };
            let ok = (counts.is_empty() && bounds.is_empty()) || counts.len() == bounds.len() + 1;
            if !ok {
                return Err(Error::invalid("histogram bucket_counts.len != explicit_bounds.len + 1"));
            }
            let cnt = count.as_ref().and_then(|a| a.is_valid(row).then(|| a.as_primitive::<UInt64Type>().value(row))).unwrap_or(0);
            let cnt = i64::try_from(cnt).map_err(|_| Error::invalid("histogram count above i64::MAX"))?;
            let mut cols = common_cols(id, m, cfg, ts(&time, row), ts(&start, row), flags(&fl, row), &mut c.stats);
            cols.push(Col::Int(Some(cnt)));
            cols.push(Col::Double(opt_f64(&sum, row)));
            cols.push(Col::Double(opt_f64(&min, row)));
            cols.push(Col::Double(opt_f64(&max, row)));
            let approx = 128 + counts.len() * 8 + bounds.len() * 8;
            cols.push(Col::ListI64(counts));
            cols.push(Col::ListF64(bounds));
            push_denorm(&mut cols, Dataset::MetricsHistogram, m, point_attrs, cfg, &mut c.stats);
            sink.push(&ValuesRow { cols, approx_bytes: approx })?;
            c.stats.rows += 1;
        }
        let (batches, pinned) = sink.finish()?;
        pinned_bytes += pinned;
        if !batches.is_empty() {
            values.push((Dataset::MetricsHistogram, batches));
        }
    }

    // Descriptors of metrics that only had unsupported points are not emitted.
    let descriptors = c.descriptors;
    Ok(Extracted { signal: Signal::Metrics, descriptors, values, pinned_bytes, stats: c.stats })
}
```

Note on `metrics[&metric_id]`: indexing a `HashMap` panics on a missing key, which `series_for` has already excluded; keep the `series_for` call first, or use `.get(...).ok_or_else(...)` to satisfy reviewers. Note on `flags(...) as i32`: same reinterpretation comment as in logs.

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake extract::`
Expected: 7 passed (3 logs, 4 metrics).

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): metrics extraction for number and histogram points

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Series cache

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/cache.rs`
- Create: `rust/otap-dataflow/crates/series-lake/src/clock.rs` (only the `PartitionId` type now; the rest in task 11)
- Modify: `src/lib.rs` (add `pub mod cache; pub mod clock;`)

**Interfaces:**
- Produces:
  - `pub struct PartitionId { pub date: u32 /* days since epoch */, pub hour: u8 }` in `clock.rs` with `Copy, Eq, Hash, Ord`, `fn from_unix_secs(secs: i64) -> PartitionId`, `fn date_string(&self) -> String` (`YYYY-MM-DD`), `fn hour_string(&self) -> String` (`HH`)
  - `pub struct SeriesCache` with `fn new(max_entries: usize) -> Self`, `fn is_committed(&mut self, id: &SeriesId, partition: PartitionId) -> bool` (touches the entry), `fn touch(&mut self, id: SeriesId)` (insert if absent without a partition, evicts LRU), `fn mark_committed(&mut self, id: SeriesId, partition: PartitionId)`, `fn len(&self) -> usize`, `fn is_empty(&self) -> bool`, `fn stats(&self) -> CacheStats { hits, misses, evictions }` (counters since creation)

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `cache.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::PartitionId;

    fn id(n: u8) -> SeriesId {
        [n; 16]
    }

    /// Scenario: a series is touched, then committed for hour P, then asked about P and Q.
    /// Guarantees: committed only for the partition it was marked with.
    #[test]
    fn committed_partition_tracking() {
        let p = PartitionId { date: 20_000, hour: 3 };
        let q = PartitionId { date: 20_000, hour: 4 };
        let mut c = SeriesCache::new(10);
        assert!(!c.is_committed(&id(1), p));
        c.touch(id(1));
        assert!(!c.is_committed(&id(1), p));
        c.mark_committed(id(1), p);
        assert!(c.is_committed(&id(1), p));
        assert!(!c.is_committed(&id(1), q));
        assert_eq!(c.stats().misses, 1);
        assert_eq!(c.stats().hits, 3);
    }

    /// Scenario: capacity 2, three distinct ids touched in order.
    /// Guarantees: the least recently used id is evicted and counted; a lost entry reads as not committed.
    #[test]
    fn evicts_least_recently_used() {
        let p = PartitionId { date: 1, hour: 0 };
        let mut c = SeriesCache::new(2);
        c.mark_committed(id(1), p);
        c.mark_committed(id(2), p);
        assert!(c.is_committed(&id(1), p)); // 1 becomes most recent
        c.mark_committed(id(3), p); // evicts 2
        assert_eq!(c.len(), 2);
        assert_eq!(c.stats().evictions, 1);
        assert!(!c.is_committed(&id(2), p));
        assert!(c.is_committed(&id(1), p));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake cache::tests`
Expected: compile error.

- [ ] **Step 3: Implement `clock.rs` (partition id only) and `cache.rs`**

`src/clock.rs` (first part; task 11 extends it):

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Wall clock abstraction, partition ids and window boundary arithmetic.

/// A `date/hour` storage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    /// Days since the Unix epoch (UTC).
    pub date: u32,
    /// Hour of day, 0..=23.
    pub hour: u8,
}

impl PartitionId {
    /// Partition of a Unix timestamp in seconds.
    pub fn from_unix_secs(secs: i64) -> Self {
        let days = secs.div_euclid(86_400);
        let hour = secs.rem_euclid(86_400) / 3_600;
        Self { date: u32::try_from(days).unwrap_or(0), hour: hour as u8 }
    }

    /// `YYYY-MM-DD` of the partition (proleptic Gregorian, UTC).
    pub fn date_string(&self) -> String {
        // Civil-from-days algorithm (Howard Hinnant), valid for all u32 day counts.
        let z = i64::from(self.date) + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        format!("{y:04}-{m:02}-{d:02}")
    }

    /// `HH` of the partition.
    pub fn hour_string(&self) -> String {
        format!("{:02}", self.hour)
    }
}
```

`src/cache.rs`:

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bounded LRU of series ids to the partition their descriptor was last
//! committed to. Losing entries only causes descriptor re-emission
//! (spec invariant 3).

use std::num::NonZeroUsize;

use lru::LruCache;

use crate::canonical::SeriesId;
use crate::clock::PartitionId;

/// Cache counters since creation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    /// Lookups that found the id with the requested partition.
    pub hits: u64,
    /// Lookups that did not.
    pub misses: u64,
    /// Entries evicted by capacity.
    pub evictions: u64,
}

/// The series cache.
pub struct SeriesCache {
    inner: LruCache<SeriesId, Option<PartitionId>>,
    stats: CacheStats,
}

impl SeriesCache {
    /// Create a cache holding at most `max_entries` ids (at least 1).
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries.max(1)).expect("max(1) is non-zero");
        Self { inner: LruCache::new(cap), stats: CacheStats::default() }
    }

    /// Whether the descriptor is known to be committed in `partition`. Touches the entry.
    pub fn is_committed(&mut self, id: &SeriesId, partition: PartitionId) -> bool {
        let hit = self.inner.get(id).is_some_and(|p| *p == Some(partition));
        if hit {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        hit
    }

    /// Insert the id if absent (no committed partition) and mark it most recently used.
    pub fn touch(&mut self, id: SeriesId) {
        if self.inner.get(&id).is_none() {
            self.insert(id, None);
        }
    }

    /// Record that the descriptor was committed in `partition`.
    pub fn mark_committed(&mut self, id: SeriesId, partition: PartitionId) {
        self.insert(id, Some(partition));
    }

    fn insert(&mut self, id: SeriesId, p: Option<PartitionId>) {
        if self.inner.len() == self.inner.cap().get() && !self.inner.contains(&id) {
            self.stats.evictions += 1;
        }
        let _ = self.inner.put(id, p);
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Counters.
    pub fn stats(&self) -> CacheStats {
        self.stats
    }
}
```

Add `pub mod cache; pub mod clock;` to `src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake cache::tests`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): bounded series cache keyed by committed partition

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Sort keys, key normalization and k-way merge

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/sort.rs`
- Modify: `src/lib.rs` (add `pub mod sort;`)

**Interfaces:**
- Consumes: `config::{SortKey, SortOrder, Nulls}`.
- Produces:
  - `pub struct SortSpec { keys: Vec<SortKey> }` with `fn new(keys: Vec<SortKey>) -> SortSpec`, `fn series() -> SortSpec` (`series_id` asc), `fn is_empty(&self) -> bool`, `fn metadata_string(&self) -> String` (`column:asc|desc:nulls_first|nulls_last,...` or `none`)
  - `pub fn sort_batch(batch: &RecordBatch, spec: &SortSpec) -> Result<RecordBatch>` (permutation plus `take`; doubles normalized so that `-0.0 == +0.0` and every NaN sorts last ascending)
  - `pub fn merge_runs(runs: &[RecordBatch], spec: &SortSpec, chunk_bytes: usize) -> Result<Vec<RecordBatch>>` (globally sorted output in chunks of at most about `chunk_bytes`, using `arrow::compute::interleave`; with an empty spec it concatenates in order)
  - `pub fn is_sorted(batch: &RecordBatch, spec: &SortSpec) -> Result<bool>` (test helper, public for the oracle test)

Implementation approach: build the sort columns with `arrow::compute::SortColumn { values, options: Some(SortOptions { descending, nulls_first }) }`; for `Float64` columns replace the values by a normalized copy (`NaN -> f64::NAN` positive, `-0.0 -> 0.0`) via `arrow::compute::unary`; `lexsort_to_indices` then orders positive NaN last under ascending (arrow uses `total_cmp`). For the merge, convert the normalized sort columns of every run with `arrow::row::RowConverter` (`SortField::new_with_options`) and run a binary heap over `(Row, run_index, row_index)`; emit `(run_index, row_index)` pairs and materialize each chunk with `interleave` per column.

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `sort.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Nulls, SortKey, SortOrder};
    use arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(keys: Vec<Option<i64>>, f: Vec<f64>, tag: &str) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, false),
            Field::new("tag", DataType::Utf8, false),
        ]);
        let n = keys.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(f)),
            Arc::new(StringArray::from(vec![tag; n])),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
    }

    fn spec() -> SortSpec {
        SortSpec::new(vec![
            SortKey { column: "k".into(), order: SortOrder::Asc, nulls: Nulls::Last },
            SortKey { column: "f".into(), order: SortOrder::Asc, nulls: Nulls::Last },
        ])
    }

    /// Scenario: keys with a null, both NaN signs and both zero signs.
    /// Guarantees: null last, NaNs after all numbers, -0.0 and +0.0 keep input order (stable).
    #[test]
    fn sort_batch_normalizes_doubles() {
        let b = batch(
            vec![Some(2), None, Some(1), Some(1), Some(1), Some(1)],
            vec![0.0, 0.0, f64::from_bits(0xFFF8_0000_0000_0000), 1.0, -0.0, f64::NAN],
            "x",
        );
        let s = sort_batch(&b, &spec()).expect("sort");
        let k = s.column(0).as_primitive::<Int64Type>();
        let f = s.column(1).as_primitive::<Float64Type>();
        assert_eq!(k.iter().collect::<Vec<_>>(), vec![Some(1), Some(1), Some(1), Some(1), Some(2), None]);
        assert_eq!(f.value(0).to_bits(), (-0.0f64).to_bits()); // original values preserved
        assert_eq!(f.value(1), 1.0);
        assert!(f.value(2).is_nan() && f.value(3).is_nan());
    }

    /// Scenario: three sorted runs merged in 2-row chunks.
    /// Guarantees: the concatenation of chunks is globally sorted and contains every row exactly once.
    #[test]
    fn merge_runs_is_globally_sorted() {
        let r1 = sort_batch(&batch(vec![Some(5), Some(1), Some(9)], vec![0.0; 3], "a"), &spec()).expect("s");
        let r2 = sort_batch(&batch(vec![Some(2), Some(8)], vec![0.0; 2], "b"), &spec()).expect("s");
        let r3 = sort_batch(&batch(vec![Some(3), None, Some(4)], vec![0.0; 3], "c"), &spec()).expect("s");
        let out = merge_runs(&[r1, r2, r3], &spec(), 1).expect("merge");
        assert!(out.len() >= 4, "tiny chunk budget must yield several chunks");
        let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
        assert_eq!(all.num_rows(), 8);
        assert!(is_sorted(&all, &spec()).expect("check"));
        let k = all.column(0).as_primitive::<Int64Type>();
        assert_eq!(k.iter().collect::<Vec<_>>(), vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(8), Some(9), None]);
    }

    /// Scenario: an empty spec (sorting disabled).
    /// Guarantees: merge concatenates runs in arrival order.
    #[test]
    fn empty_spec_concatenates() {
        let r1 = batch(vec![Some(5)], vec![0.0], "a");
        let r2 = batch(vec![Some(1)], vec![0.0], "b");
        let out = merge_runs(&[r1, r2], &SortSpec::new(vec![]), 1 << 20).expect("merge");
        let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
        assert_eq!(all.column(0).as_primitive::<Int64Type>().values(), &[5, 1]);
        assert_eq!(SortSpec::new(vec![]).metadata_string(), "none");
        assert_eq!(spec().metadata_string(), "k:asc:nulls_last,f:asc:nulls_last");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake sort::tests`
Expected: compile error.

- [ ] **Step 3: Implement `src/sort.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sort specification, run sorting and k-way merge (spec sections 6.2, 6.5, 7.4).

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::compute::{SortColumn, SortOptions, concat_batches, interleave, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};

use crate::config::{Nulls, SortKey, SortOrder};
use crate::error::{Error, Result};

/// Ordered list of sort keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    keys: Vec<SortKey>,
}

impl SortSpec {
    /// Build from keys; an empty list means "no sorting".
    pub fn new(keys: Vec<SortKey>) -> Self {
        Self { keys }
    }

    /// The fixed series sort: `series_id` ascending.
    pub fn series() -> Self {
        Self::new(vec![SortKey { column: "series_id".into(), order: SortOrder::Asc, nulls: Nulls::Last }])
    }

    /// Whether sorting is disabled.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Keys.
    pub fn keys(&self) -> &[SortKey] {
        &self.keys
    }

    /// `sort_key` file metadata value.
    pub fn metadata_string(&self) -> String {
        if self.keys.is_empty() {
            return "none".into();
        }
        self.keys
            .iter()
            .map(|k| {
                format!(
                    "{}:{}:{}",
                    k.column,
                    match k.order { SortOrder::Asc => "asc", SortOrder::Desc => "desc" },
                    match k.nulls { Nulls::First => "nulls_first", Nulls::Last => "nulls_last" }
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn options(k: &SortKey) -> SortOptions {
        SortOptions { descending: k.order == SortOrder::Desc, nulls_first: k.nulls == Nulls::First }
    }
}

/// Doubles normalized for ordering: every NaN becomes positive quiet NaN and
/// -0.0 becomes +0.0, so `total_cmp` yields the spec order.
fn normalize_key(col: &ArrayRef) -> ArrayRef {
    if col.data_type() != &DataType::Float64 {
        return col.clone();
    }
    let a = col.as_primitive::<Float64Type>();
    let out: Float64Array = arrow::compute::kernels::arity::unary(a, |v: f64| {
        if v.is_nan() {
            f64::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        }
    });
    Arc::new(out)
}

fn sort_columns(batch: &RecordBatch, spec: &SortSpec) -> Result<Vec<SortColumn>> {
    spec.keys
        .iter()
        .map(|k| {
            let col = batch
                .column_by_name(&k.column)
                .ok_or_else(|| Error::invalid(format!("sort column {} missing", k.column)))?;
            Ok(SortColumn { values: normalize_key(col), options: Some(SortSpec::options(k)) })
        })
        .collect()
}

/// Sort one batch by the spec (stable). Returns the input unchanged for an empty spec.
pub fn sort_batch(batch: &RecordBatch, spec: &SortSpec) -> Result<RecordBatch> {
    if spec.is_empty() || batch.num_rows() < 2 {
        return Ok(batch.clone());
    }
    let cols = sort_columns(batch, spec)?;
    let indices = lexsort_to_indices(&cols, None)?;
    let taken: Vec<ArrayRef> = batch.columns().iter().map(|c| take(c, &indices, None)).collect::<std::result::Result<_, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), taken)?)
}

struct HeapItem {
    row: OwnedRow,
    run: usize,
    idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        self.row.as_ref() == o.row.as_ref() && self.run == o.run
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapItem {
    // Reversed so BinaryHeap pops the smallest row; ties broken by run index for stability.
    fn cmp(&self, o: &Self) -> Ordering {
        o.row.as_ref().cmp(&self.row.as_ref()).then(o.run.cmp(&self.run))
    }
}

fn approx_row_bytes(batch: &RecordBatch) -> usize {
    if batch.num_rows() == 0 {
        return 1;
    }
    (batch.get_array_memory_size() / batch.num_rows()).max(1)
}

/// Merge sorted runs into globally sorted chunks of about `chunk_bytes`.
pub fn merge_runs(runs: &[RecordBatch], spec: &SortSpec, chunk_bytes: usize) -> Result<Vec<RecordBatch>> {
    let runs: Vec<&RecordBatch> = runs.iter().filter(|r| r.num_rows() > 0).collect();
    let Some(first) = runs.first() else { return Ok(vec![]) };
    let schema = first.schema();
    if spec.is_empty() {
        // Arrival order; re-chunk by size only.
        let all = concat_batches(&schema, runs.iter().copied())?;
        let rows_per_chunk = (chunk_bytes / approx_row_bytes(&all)).max(1);
        return Ok((0..all.num_rows()).step_by(rows_per_chunk).map(|s| all.slice(s, rows_per_chunk.min(all.num_rows() - s))).collect());
    }
    let fields: Vec<SortField> = spec
        .keys
        .iter()
        .map(|k| {
            let dt = schema.field_with_name(&k.column)?.data_type().clone();
            Ok(SortField::new_with_options(dt, SortSpec::options(k)))
        })
        .collect::<std::result::Result<_, arrow::error::ArrowError>>()?;
    let converter = RowConverter::new(fields)?;
    let mut rows_per_run = Vec::with_capacity(runs.len());
    for r in &runs {
        let cols: Vec<ArrayRef> = sort_columns(r, spec)?.into_iter().map(|c| c.values).collect();
        rows_per_run.push(converter.convert_columns(&cols)?);
    }
    let mut heap = BinaryHeap::new();
    for (run, rows) in rows_per_run.iter().enumerate() {
        heap.push(HeapItem { row: rows.row(0).owned(), run, idx: 0 });
    }
    let row_bytes = approx_row_bytes(first);
    let rows_per_chunk = (chunk_bytes / row_bytes).max(1);
    let arrays_per_col: Vec<Vec<&dyn Array>> = (0..schema.fields().len())
        .map(|c| runs.iter().map(|r| r.column(c).as_ref()).collect())
        .collect();
    let mut out = Vec::new();
    let mut pending: Vec<(usize, usize)> = Vec::with_capacity(rows_per_chunk);
    let flush = |pending: &mut Vec<(usize, usize)>, out: &mut Vec<RecordBatch>| -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = arrays_per_col
            .iter()
            .map(|arrs| interleave(arrs, pending))
            .collect::<std::result::Result<_, _>>()?;
        out.push(RecordBatch::try_new(schema.clone(), cols)?);
        pending.clear();
        Ok(())
    };
    while let Some(item) = heap.pop() {
        pending.push((item.run, item.idx));
        let next = item.idx + 1;
        if next < rows_per_run[item.run].num_rows() {
            heap.push(HeapItem { row: rows_per_run[item.run].row(next).owned(), run: item.run, idx: next });
        }
        if pending.len() >= rows_per_chunk {
            flush(&mut pending, &mut out)?;
        }
    }
    flush(&mut pending, &mut out)?;
    Ok(out)
}

/// Whether a batch is sorted by the spec (test helper).
pub fn is_sorted(batch: &RecordBatch, spec: &SortSpec) -> Result<bool> {
    if spec.is_empty() || batch.num_rows() < 2 {
        return Ok(true);
    }
    let cols = sort_columns(batch, spec)?;
    let idx = lexsort_to_indices(&cols, None)?;
    Ok(idx.values().iter().enumerate().all(|(i, v)| *v as usize == i))
}
```

`OwnedRow` per heap item allocates; acceptable for v1 (rows are short). Add `pub mod sort;` to `src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake sort::tests`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): sort spec, double key normalization and k-way merge

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: `SortedTableBuffer` and `Block`

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/buffer.rs`
- Modify: `src/lib.rs` (add `pub mod buffer;`)

**Interfaces:**
- Consumes: `sort::{SortSpec, sort_batch}`, `extract::{Extracted, DescriptorRow, series_batch}`, `schema::Dataset`, `clock::PartitionId`, `cache::SeriesCache`, `config::LakeConfig`.
- Produces:
  - `pub struct SortedTableBuffer` with `fn new(dataset: Dataset, spec: SortSpec, run_target_bytes: usize)`, `fn append(&mut self, batch: RecordBatch) -> Result<usize>` (returns pinned bytes newly retained; seals a run when the building size reaches the target), `fn seal(&mut self) -> Result<()>`, `fn runs(&self) -> &[RecordBatch]`, `fn building(&self) -> &[RecordBatch]`, `fn rows(&self) -> usize`, `fn is_empty(&self) -> bool`, `fn dataset(&self) -> Dataset`, `fn spec(&self) -> &SortSpec`, `fn iter_snapshots(&self) -> impl Iterator<Item = &RecordBatch>` (building then runs; spec 10.1 consequence)
  - `pub struct Reservation { pub bytes: usize, pub new_series: Vec<usize> }` (indices into `Extracted::descriptors` that must be emitted)
  - `pub struct Block<T> { pub window_start_secs: i64, pub partition: PartitionId, pub seq: u64, tables: BTreeMap<Dataset, SortedTableBuffer>, pub pending_series: HashSet<SeriesId>, pub bytes: usize, pub requests: Vec<T> }`
  - `impl<T> Block<T>`: `fn new(window_start_secs: i64, seq: u64, cfg: &LakeConfig) -> Block<T>`, `fn reserve(&self, extracted: &Extracted, cache: &mut SeriesCache, token_bytes: usize, cfg: &LakeConfig) -> Reservation`, `fn admit(&mut self, extracted: Extracted, reservation: Reservation, token: T, emitted_at_us: i64, cfg: &LakeConfig) -> Result<()>`, `fn seal(&mut self) -> Result<()>`, `fn tables(&self) -> impl Iterator<Item = &SortedTableBuffer>` (series datasets first), `fn is_empty(&self) -> bool`, `fn request_count(&self) -> usize`, `fn into_parts(self) -> (Vec<T>, HashSet<SeriesId>, BTreeMap<Dataset, SortedTableBuffer>)`
  - `pub const PENDING_SERIES_ENTRY_BYTES: usize = 64;`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `buffer.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::SeriesCache;
    use crate::config::LakeConfig;
    use crate::extract::extract;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;

    fn logs(host: &str, n: usize) -> LogsData {
        let kv = |k: &str, v: &str| KeyValue { key: k.into(), value: Some(AnyValue { value: Some(any_value::Value::StringValue(v.into())) }) };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource { attributes: vec![kv("host.id", host)], ..Default::default() }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..n).map(|i| LogRecord { time_unix_nano: 1000 + i as u64, ..Default::default() }).collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// Scenario: two requests for the same series in one block, then the same series after commit.
    /// Guarantees: the descriptor is reserved once per block and not at all once committed in the partition.
    #[test]
    fn reserve_emits_descriptor_once_per_block_and_partition() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e1 = extract(&encode_logs(&logs("h", 2)), &cfg).expect("e1");
        let r1 = block.reserve(&e1, &mut cache, 16, &cfg);
        assert_eq!(r1.new_series, vec![0]);
        assert!(r1.bytes > 16);
        block.admit(e1, r1, 1, 0, &cfg).expect("admit");
        let e2 = extract(&encode_logs(&logs("h", 1)), &cfg).expect("e2");
        let r2 = block.reserve(&e2, &mut cache, 16, &cfg);
        assert!(r2.new_series.is_empty());
        block.admit(e2, r2, 2, 0, &cfg).expect("admit");
        assert_eq!(block.request_count(), 2);
        assert_eq!(block.pending_series.len(), 1);
        let series = block.tables().find(|t| t.dataset().is_series()).expect("series table");
        assert_eq!(series.rows(), 1);
        // commit in the block's partition
        for id in &block.pending_series {
            cache.mark_committed(*id, block.partition);
        }
        let mut next: Block<u32> = Block::new(0, 2, &cfg);
        let e3 = extract(&encode_logs(&logs("h", 1)), &cfg).expect("e3");
        assert!(next.reserve(&e3, &mut cache, 16, &cfg).new_series.is_empty());
        let mut other: Block<u32> = Block::new(3600, 3, &cfg); // next hour
        assert_eq!(other.reserve(&e3, &mut cache, 16, &cfg).new_series, vec![0]);
        let _ = &mut other;
        let _ = &mut next;
    }

    /// Scenario: a buffer with a tiny run target receives several batches.
    /// Guarantees: runs are sealed sorted, byte accounting grows, snapshots iterate everything.
    #[test]
    fn buffer_seals_sorted_runs() {
        let cfg = LakeConfig::default();
        let e = extract(&encode_logs(&logs("h", 50)), &cfg).expect("e");
        let (_, batches) = e.values.into_iter().next().expect("values");
        let mut buf = SortedTableBuffer::new(Dataset::LogsValues, SortSpec::new(cfg.logs.values_sort.clone()), 1);
        let mut total = 0;
        for b in batches {
            total += buf.append(b).expect("append");
        }
        assert!(total > 0);
        assert!(!buf.runs().is_empty());
        for r in buf.runs() {
            assert!(crate::sort::is_sorted(r, buf.spec()).expect("sorted"));
        }
        assert_eq!(buf.iter_snapshots().map(|b| b.num_rows()).sum::<usize>(), 50);
        buf.seal().expect("seal");
        assert!(buf.building().is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake buffer::tests`
Expected: compile error.

- [ ] **Step 3: Implement `src/buffer.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sorted run buffers and the ACTIVE/FLUSHING block (spec sections 6.1 to 6.3).

use std::collections::{BTreeMap, HashSet};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};

use crate::cache::SeriesCache;
use crate::canonical::SeriesId;
use crate::clock::PartitionId;
use crate::config::LakeConfig;
use crate::error::Result;
use crate::extract::{Extracted, series_batch};
use crate::schema::Dataset;
use crate::sort::{SortSpec, sort_batch};

/// Charged bytes per `pending_series` entry (spec section 6.1).
pub const PENDING_SERIES_ENTRY_BYTES: usize = 64;

/// Building batches plus sealed sorted runs for one dataset.
pub struct SortedTableBuffer {
    dataset: Dataset,
    spec: SortSpec,
    run_target: usize,
    building: Vec<RecordBatch>,
    building_bytes: usize,
    runs: Vec<RecordBatch>,
    seen: CountedAllocations,
    rows: usize,
}

impl SortedTableBuffer {
    /// New empty buffer.
    pub fn new(dataset: Dataset, spec: SortSpec, run_target_bytes: usize) -> Self {
        Self {
            dataset,
            spec,
            run_target: run_target_bytes.max(1),
            building: Vec::new(),
            building_bytes: 0,
            runs: Vec::new(),
            seen: CountedAllocations::default(),
            rows: 0,
        }
    }

    /// Append a batch; returns the pinned bytes newly retained by this buffer.
    pub fn append(&mut self, batch: RecordBatch) -> Result<usize> {
        let pinned = record_batch_pinned_bytes(&batch, &mut self.seen);
        self.rows += batch.num_rows();
        self.building_bytes += pinned;
        self.building.push(batch);
        if self.building_bytes >= self.run_target {
            self.seal()?;
        }
        Ok(pinned)
    }

    /// Sort and seal the building batches into one run.
    pub fn seal(&mut self) -> Result<()> {
        if self.building.is_empty() {
            return Ok(());
        }
        let schema = self.building[0].schema();
        let merged = concat_batches(&schema, &self.building)?;
        let sorted = sort_batch(&merged, &self.spec)?;
        // Re-account: the concatenated run replaces the building batches.
        self.seen = CountedAllocations::default();
        for r in &self.runs {
            let _ = record_batch_pinned_bytes(r, &mut self.seen);
        }
        let _ = record_batch_pinned_bytes(&sorted, &mut self.seen);
        self.runs.push(sorted);
        self.building.clear();
        self.building_bytes = 0;
        Ok(())
    }

    /// Sealed runs.
    pub fn runs(&self) -> &[RecordBatch] {
        &self.runs
    }

    /// Unsealed batches.
    pub fn building(&self) -> &[RecordBatch] {
        &self.building
    }

    /// Total rows.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Whether nothing was appended.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Dataset.
    pub fn dataset(&self) -> Dataset {
        self.dataset
    }

    /// Sort spec.
    pub fn spec(&self) -> &SortSpec {
        &self.spec
    }

    /// Building batches then sealed runs, for bounded snapshot copies.
    pub fn iter_snapshots(&self) -> impl Iterator<Item = &RecordBatch> {
        self.building.iter().chain(self.runs.iter())
    }
}

/// Outcome of a reservation: bytes to charge and descriptors to emit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// Bytes the request will add to the block.
    pub bytes: usize,
    /// Indices into `Extracted::descriptors` whose descriptor must be written by this block.
    pub new_series: Vec<usize>,
}

/// One ACTIVE or FLUSHING block.
pub struct Block<T> {
    /// Window start (Unix seconds).
    pub window_start_secs: i64,
    /// Destination partition.
    pub partition: PartitionId,
    /// Per-worker sequence number used in file names.
    pub seq: u64,
    tables: BTreeMap<Dataset, SortedTableBuffer>,
    /// Series whose descriptor this block carries.
    pub pending_series: HashSet<SeriesId>,
    /// Accounted bytes.
    pub bytes: usize,
    /// Request tokens.
    pub requests: Vec<T>,
    cfg_run_target: usize,
    sort_enabled: bool,
    logs_sort: SortSpec,
    metrics_sort: SortSpec,
}

impl<T> Block<T> {
    /// New empty block for a window.
    pub fn new(window_start_secs: i64, seq: u64, cfg: &LakeConfig) -> Self {
        Self {
            window_start_secs,
            partition: PartitionId::from_unix_secs(window_start_secs),
            seq,
            tables: BTreeMap::new(),
            pending_series: HashSet::new(),
            bytes: 0,
            requests: Vec::new(),
            cfg_run_target: cfg.sorting.run_target_bytes,
            sort_enabled: cfg.sorting.enabled,
            logs_sort: SortSpec::new(cfg.logs.values_sort.clone()),
            metrics_sort: SortSpec::new(cfg.metrics.values_sort.clone()),
        }
    }

    fn spec_for(&self, ds: Dataset) -> SortSpec {
        if ds.is_series() {
            SortSpec::series()
        } else if !self.sort_enabled {
            SortSpec::new(vec![])
        } else if ds.signal() == crate::canonical::Signal::Logs {
            self.logs_sort.clone()
        } else {
            self.metrics_sort.clone()
        }
    }

    /// Compute what admitting `extracted` would add (spec section 6.2 step 5). Touches the cache.
    pub fn reserve(&self, extracted: &Extracted, cache: &mut SeriesCache, token_bytes: usize, _cfg: &LakeConfig) -> Reservation {
        let mut bytes = extracted.pinned_bytes + token_bytes;
        let mut new_series = Vec::new();
        for (i, d) in extracted.descriptors.iter().enumerate() {
            let committed_here = cache.is_committed(&d.series_id, self.partition);
            cache.touch(d.series_id);
            if !committed_here && !self.pending_series.contains(&d.series_id) {
                new_series.push(i);
                bytes += d.approx_bytes + PENDING_SERIES_ENTRY_BYTES;
            }
        }
        Reservation { bytes, new_series }
    }

    /// Admit a reserved request (spec section 6.2 step 6).
    pub fn admit(&mut self, extracted: Extracted, reservation: Reservation, token: T, emitted_at_us: i64, cfg: &LakeConfig) -> Result<()> {
        let signal = extracted.signal;
        if !reservation.new_series.is_empty() {
            let rows: Vec<&crate::extract::DescriptorRow> = reservation.new_series.iter().map(|i| &extracted.descriptors[*i]).collect();
            let ds = Dataset::series_of(signal);
            let batch = series_batch(&rows, emitted_at_us, ds, cfg)?;
            for r in &rows {
                let _ = self.pending_series.insert(r.series_id);
            }
            let run_target = self.cfg_run_target;
            let spec = self.spec_for(ds);
            let _ = self.tables.entry(ds).or_insert_with(|| SortedTableBuffer::new(ds, spec, run_target)).append(batch)?;
        }
        for (ds, batches) in extracted.values {
            let run_target = self.cfg_run_target;
            let spec = self.spec_for(ds);
            let table = self.tables.entry(ds).or_insert_with(|| SortedTableBuffer::new(ds, spec, run_target));
            for b in batches {
                let _ = table.append(b)?;
            }
        }
        self.bytes += reservation.bytes;
        self.requests.push(token);
        Ok(())
    }

    /// Seal every table's building batches.
    pub fn seal(&mut self) -> Result<()> {
        for t in self.tables.values_mut() {
            t.seal()?;
        }
        Ok(())
    }

    /// Tables in write order: series datasets first (Dataset's Ord puts LogsSeries and MetricsSeries before their values).
    pub fn tables(&self) -> impl Iterator<Item = &SortedTableBuffer> {
        self.tables.values()
    }

    /// Whether the block holds no rows.
    pub fn is_empty(&self) -> bool {
        self.tables.values().all(SortedTableBuffer::is_empty)
    }

    /// Number of admitted requests.
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Take the block apart after a flush result.
    pub fn into_parts(self) -> (Vec<T>, HashSet<SeriesId>, BTreeMap<Dataset, SortedTableBuffer>) {
        (self.requests, self.pending_series, self.tables)
    }
}
```

`Dataset`'s derived `Ord` (declaration order: LogsSeries, LogsValues, MetricsSeries, MetricsNumber, MetricsHistogram) already puts each signal's series dataset before its values datasets, which is the write order of spec 5.3. Add `pub mod buffer;` to `src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake buffer::tests`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): sorted table buffers and block accounting

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Wall clock and window boundaries

**Files:**
- Modify: `rust/otap-dataflow/crates/series-lake/src/clock.rs` (extend the file created in task 8)

**Interfaces:**
- Produces:
  - `pub trait WallClock { fn now_unix_nanos(&self) -> i64; }` with `pub struct SystemWallClock;` and `pub struct TestWallClock(Arc<AtomicI64>)` with `fn new(nanos: i64) -> Self`, `fn set(&self, nanos: i64)`, `fn advance(&self, nanos: i64)`
  - `pub struct WindowClock { interval_secs: i64, last_boundary_secs: i64 }` with `fn new(interval: Duration, start_unix_secs: i64) -> WindowClock`, `fn boundary(&self, unix_secs: i64) -> i64`, `fn effective_boundary(&self, now_unix_secs: i64) -> i64`, `fn next_boundary(&self, now_unix_secs: i64) -> i64`, `fn on_wake(&mut self, now_unix_secs: i64) -> WakeOutcome`, `fn last_boundary(&self) -> i64`
  - `pub enum WakeOutcome { RotationRequested { effective_boundary: i64 }, TooEarly { sleep_until: i64 } }`
  - `pub fn nanos_to_secs(nanos: i64) -> i64`, `pub fn nanos_to_micros(nanos: i64) -> i64`

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `clock.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Scenario: 15 s windows starting at 12:00:07.
    /// Guarantees: boundary floors to the interval, next boundary is strictly ahead even exactly on a boundary.
    #[test]
    fn boundaries_are_aligned_and_strictly_advancing() {
        let mut c = WindowClock::new(Duration::from_secs(15), 7);
        assert_eq!(c.last_boundary(), 0);
        assert_eq!(c.next_boundary(7), 15);
        assert_eq!(c.next_boundary(15), 30); // exact boundary is not re-fired
        match c.on_wake(15) {
            WakeOutcome::RotationRequested { effective_boundary } => assert_eq!(effective_boundary, 15),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.last_boundary(), 15);
        assert_eq!(c.next_boundary(16), 30);
    }

    /// Scenario: the wall clock steps backwards after a boundary was consumed.
    /// Guarantees: no rotation, boundaries never move backwards, sleep targets the expected boundary.
    #[test]
    fn backward_step_does_not_reopen_window() {
        let mut c = WindowClock::new(Duration::from_secs(15), 20);
        let _ = c.on_wake(30);
        match c.on_wake(25) {
            WakeOutcome::TooEarly { sleep_until } => assert_eq!(sleep_until, 45),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.last_boundary(), 30);
        assert_eq!(c.effective_boundary(25), 30);
    }

    /// Scenario: a long flush; the clock wakes 50 seconds late.
    /// Guarantees: missed boundaries coalesce into one rotation at the latest boundary.
    #[test]
    fn missed_boundaries_coalesce() {
        let mut c = WindowClock::new(Duration::from_secs(15), 0);
        match c.on_wake(65) {
            WakeOutcome::RotationRequested { effective_boundary } => assert_eq!(effective_boundary, 60),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(c.next_boundary(65), 75);
    }

    /// Scenario: partition of Unix seconds and the civil date rendering.
    /// Guarantees: date and hour strings are zero padded and correct for a known timestamp.
    #[test]
    fn partition_strings() {
        // 2026-09-21T03:15:00Z = 1789960500
        let p = PartitionId::from_unix_secs(1_789_960_500);
        assert_eq!(p.date_string(), "2026-09-21");
        assert_eq!(p.hour_string(), "03");
        let t = TestWallClock::new(5_000_000_000);
        t.advance(1);
        assert_eq!(t.now_unix_nanos(), 5_000_000_001);
        assert_eq!(nanos_to_secs(5_000_000_001), 5);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake clock::tests`
Expected: compile error.

- [ ] **Step 3: Extend `src/clock.rs`**

Append below `PartitionId`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Source of wall-clock time (injectable in tests).
pub trait WallClock: Send + Sync {
    /// Unix time in nanoseconds.
    fn now_unix_nanos(&self) -> i64;
}

/// System wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_unix_nanos(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}

/// Manually driven wall clock for tests.
#[derive(Debug, Clone)]
pub struct TestWallClock(Arc<AtomicI64>);

impl TestWallClock {
    /// Start at `nanos`.
    pub fn new(nanos: i64) -> Self {
        Self(Arc::new(AtomicI64::new(nanos)))
    }

    /// Set the time.
    pub fn set(&self, nanos: i64) {
        self.0.store(nanos, Ordering::SeqCst);
    }

    /// Advance the time.
    pub fn advance(&self, nanos: i64) {
        let _ = self.0.fetch_add(nanos, Ordering::SeqCst);
    }
}

impl WallClock for TestWallClock {
    fn now_unix_nanos(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Nanoseconds to whole seconds (floor).
pub fn nanos_to_secs(nanos: i64) -> i64 {
    nanos.div_euclid(1_000_000_000)
}

/// Nanoseconds to microseconds (floor).
pub fn nanos_to_micros(nanos: i64) -> i64 {
    nanos.div_euclid(1_000)
}

/// Result of a timer wake-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    /// A boundary was crossed; rotate with this window start.
    RotationRequested {
        /// Effective boundary (window start of the new block).
        effective_boundary: i64,
    },
    /// The wall clock is still before the expected boundary (backward step); sleep again.
    TooEarly {
        /// Boundary to sleep until.
        sleep_until: i64,
    },
}

/// Aligned window boundary arithmetic (spec section 6.4).
#[derive(Debug, Clone)]
pub struct WindowClock {
    interval_secs: i64,
    last_boundary_secs: i64,
}

impl WindowClock {
    /// Create with `last_boundary = boundary(start)`.
    pub fn new(interval: Duration, start_unix_secs: i64) -> Self {
        let interval_secs = i64::try_from(interval.as_secs()).unwrap_or(15).max(1);
        let mut c = Self { interval_secs, last_boundary_secs: 0 };
        c.last_boundary_secs = c.boundary(start_unix_secs);
        c
    }

    /// `floor(t / interval) * interval`.
    pub fn boundary(&self, unix_secs: i64) -> i64 {
        unix_secs.div_euclid(self.interval_secs) * self.interval_secs
    }

    /// `max(boundary(now), last_boundary)`: never moves backwards.
    pub fn effective_boundary(&self, now_unix_secs: i64) -> i64 {
        self.boundary(now_unix_secs).max(self.last_boundary_secs)
    }

    /// The next boundary to sleep until, strictly after the effective boundary.
    pub fn next_boundary(&self, now_unix_secs: i64) -> i64 {
        self.effective_boundary(now_unix_secs) + self.interval_secs
    }

    /// Handle a wake-up at `now`.
    pub fn on_wake(&mut self, now_unix_secs: i64) -> WakeOutcome {
        let expected = self.last_boundary_secs + self.interval_secs;
        if now_unix_secs < expected {
            return WakeOutcome::TooEarly { sleep_until: expected };
        }
        let effective = self.boundary(now_unix_secs);
        self.last_boundary_secs = effective;
        WakeOutcome::RotationRequested { effective_boundary: effective }
    }

    /// Last consumed boundary.
    pub fn last_boundary(&self) -> i64 {
        self.last_boundary_secs
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake clock::tests`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): wall clock trait and aligned window boundaries

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 12: Parquet sink to an object store

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/src/sink.rs`
- Modify: `src/lib.rs` (add `pub mod sink;`)

**Interfaces:**
- Consumes: `buffer::{Block, SortedTableBuffer}`, `sort::merge_runs`, `schema::{Dataset, schema_fingerprint}`, `clock::PartitionId`, `config::LakeConfig`.
- Produces:
  - `pub struct FileNaming { pub writer_id: String, pub boot_id: String }` with `fn new(writer_id: &str) -> FileNaming` (generates a UUIDv4 `boot_id`)
  - `pub fn object_path(ds: Dataset, partition: PartitionId, window_start_secs: i64, naming: &FileNaming, seq: u64) -> object_store::path::Path` producing `v=1/signal=<s>/dataset=<d>/date=YYYY-MM-DD/hour=HH/part-<YYYYMMDD>T<HHMMSS>Z-<writer_id>-<boot_id>-<seq:08>.parquet`
  - `pub struct FlushReport { pub files: Vec<(Dataset, object_store::path::Path, usize /* rows */)> }`
  - `pub struct Sink { store: Arc<dyn ObjectStore>, cfg: LakeConfig, naming: FileNaming }` with `fn new(store: Arc<dyn ObjectStore>, cfg: LakeConfig, naming: FileNaming) -> Sink`
  - `pub async fn Sink::write_block<T>(&self, block: &Block<T>, cancel: &CancellationToken) -> Result<FlushReport>` (series datasets first, then values; frozen names; `Error::Cancelled` on cancellation; writer memory limit enforced; metadata of spec 5.4)
  - `pub fn file_metadata(...) -> Vec<KeyValue>` (parquet `KeyValue` entries): `format_version`, `series_hash`, `schema_fingerprint`, `sort_key`, `writer_id`, `boot_id`, `seq`, `window_start`, `window_end`, `row_count`, `min_time_unix_nano`, `max_time_unix_nano` (the last two only for values datasets, computed with `arrow::compute::min/max` over `time_unix_nano`)

- [ ] **Step 1: Write failing tests** (`#[cfg(test)] mod tests` in `sink.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::Block;
    use crate::cache::SeriesCache;
    use crate::config::LakeConfig;
    use crate::extract::extract;
    use object_store::local::LocalFileSystem;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tokio_util::sync::CancellationToken;

    fn logs(n: usize) -> LogsData {
        let kv = |k: &str, v: &str| KeyValue { key: k.into(), value: Some(AnyValue { value: Some(any_value::Value::StringValue(v.into())) }) };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource { attributes: vec![kv("host.id", "h")], ..Default::default() }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..n).map(|i| LogRecord { time_unix_nano: 5_000 - i as u64, ..Default::default() }).collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn block(cfg: &LakeConfig, n: usize) -> Block<u8> {
        let mut cache = SeriesCache::new(10);
        let mut b: Block<u8> = Block::new(1_789_960_500, 7, cfg);
        let e = extract(&encode_logs(&logs(n)), cfg).expect("extract");
        let r = b.reserve(&e, &mut cache, 8, cfg);
        b.admit(e, r, 0, 1_789_960_500_000_000, cfg).expect("admit");
        b
    }

    /// Scenario: path components for a known window and sequence.
    /// Guarantees: the Hive layout and file name of spec section 5.3 are produced exactly.
    #[test]
    fn object_path_layout() {
        let naming = FileNaming { writer_id: "w1".into(), boot_id: "b".into() };
        let p = object_path(Dataset::LogsValues, PartitionId::from_unix_secs(1_789_960_500), 1_789_960_500, &naming, 42);
        assert_eq!(
            p.as_ref(),
            "v=1/signal=logs/dataset=values/date=2026-09-21/hour=03/part-20260921T031500Z-w1-b-00000042.parquet"
        );
    }

    /// Scenario: a block with 30 log rows written to a local directory, then read back.
    /// Guarantees: series file exists next to the values file, values are sorted by the spec,
    /// metadata carries the window and fingerprint, the same block rewrites the same names.
    #[tokio::test]
    async fn writes_series_before_values_and_reads_back() {
        let dir = tempfile::tempdir().expect("tmp");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("fs"));
        let cfg = LakeConfig::default();
        let mut b = block(&cfg, 30);
        b.seal().expect("seal");
        let sink = Sink::new(store.clone(), cfg.clone(), FileNaming { writer_id: "w".into(), boot_id: "boot".into() });
        let report = sink.write_block(&b, &CancellationToken::new()).await.expect("write");
        assert_eq!(report.files.len(), 2);
        assert_eq!(report.files[0].0, Dataset::LogsSeries);
        assert_eq!(report.files[1].0, Dataset::LogsValues);
        let values_path = dir.path().join(report.files[1].1.as_ref());
        let file = std::fs::File::open(&values_path).expect("open");
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader");
        let kv = reader.metadata().file_metadata().key_value_metadata().expect("kv").clone();
        let get = |k: &str| kv.iter().find(|e| e.key == k).and_then(|e| e.value.clone()).expect(k);
        assert_eq!(get("format_version"), "1");
        assert_eq!(get("window_start"), "1789960500");
        assert_eq!(get("window_end"), "1789960515");
        assert_eq!(get("row_count"), "30");
        assert_eq!(get("sort_key"), "series_id:asc:nulls_last,time_unix_nano:asc:nulls_last");
        let batches: Vec<_> = reader.build().expect("build").map(|b| b.expect("batch")).collect();
        let all = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
        assert_eq!(all.num_rows(), 30);
        let spec = crate::sort::SortSpec::new(cfg.logs.values_sort.clone());
        assert!(crate::sort::is_sorted(&all, &spec).expect("sorted"));
        // rewrite: same names, still two files on disk
        let report2 = sink.write_block(&b, &CancellationToken::new()).await.expect("rewrite");
        assert_eq!(report.files[1].1, report2.files[1].1);
        let count = walkdir_count(dir.path());
        assert_eq!(count, 2);
    }

    /// Scenario: the token is cancelled before writing.
    /// Guarantees: write_block returns Cancelled and leaves no completed object.
    #[tokio::test]
    async fn cancellation_aborts() {
        let dir = tempfile::tempdir().expect("tmp");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).expect("fs"));
        let cfg = LakeConfig::default();
        let mut b = block(&cfg, 5);
        b.seal().expect("seal");
        let sink = Sink::new(store, cfg, FileNaming::new("w"));
        let token = CancellationToken::new();
        token.cancel();
        assert!(matches!(sink.write_block(&b, &token).await, Err(Error::Cancelled)));
        assert_eq!(walkdir_count(dir.path()), 0);
    }

    fn walkdir_count(root: &std::path::Path) -> usize {
        fn walk(p: &std::path::Path, n: &mut usize) {
            for e in std::fs::read_dir(p).expect("dir") {
                let e = e.expect("entry");
                if e.path().is_dir() {
                    walk(&e.path(), n);
                } else if e.path().extension().is_some_and(|x| x == "parquet") {
                    *n += 1;
                }
            }
        }
        let mut n = 0;
        walk(root, &mut n);
        n
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake sink::tests`
Expected: compile error.

- [ ] **Step 3: Implement `src/sink.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Writes a sealed block to an object store as Parquet files (spec sections 5.3 to 5.4, 6.5).

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use tokio_util::sync::CancellationToken;

use crate::buffer::{Block, SortedTableBuffer};
use crate::clock::PartitionId;
use crate::config::LakeConfig;
use crate::error::{Error, Result};
use crate::schema::{Dataset, dataset_schema, schema_fingerprint};
use crate::sort::merge_runs;

/// Identity components of file names.
#[derive(Debug, Clone)]
pub struct FileNaming {
    /// Configured writer id.
    pub writer_id: String,
    /// Random id of this process incarnation.
    pub boot_id: String,
}

impl FileNaming {
    /// New naming with a fresh UUIDv4 boot id.
    pub fn new(writer_id: &str) -> Self {
        Self { writer_id: writer_id.to_string(), boot_id: uuid::Uuid::new_v4().simple().to_string() }
    }
}

fn utc_stamp(unix_secs: i64) -> String {
    let p = PartitionId::from_unix_secs(unix_secs);
    let date = p.date_string().replace('-', "");
    let secs_of_day = unix_secs.rem_euclid(86_400);
    format!("{date}T{:02}{:02}{:02}Z", secs_of_day / 3600, (secs_of_day % 3600) / 60, secs_of_day % 60)
}

/// Object path of a dataset file (spec section 5.3).
pub fn object_path(ds: Dataset, partition: PartitionId, window_start_secs: i64, naming: &FileNaming, seq: u64) -> Path {
    Path::from(format!(
        "v=1/signal={}/dataset={}/date={}/hour={}/part-{}-{}-{}-{seq:08}.parquet",
        ds.signal().as_str(),
        ds.name(),
        partition.date_string(),
        partition.hour_string(),
        utc_stamp(window_start_secs),
        naming.writer_id,
        naming.boot_id,
    ))
}

/// Files written for one block.
#[derive(Debug, Default)]
pub struct FlushReport {
    /// Dataset, path and row count per file, in write order.
    pub files: Vec<(Dataset, Path, usize)>,
}

/// Parquet sink.
pub struct Sink {
    store: Arc<dyn ObjectStore>,
    cfg: LakeConfig,
    naming: FileNaming,
}

fn time_range(chunks: &[RecordBatch]) -> (Option<i64>, Option<i64>) {
    let mut lo = None;
    let mut hi = None;
    for c in chunks {
        if let Some(col) = c.column_by_name("time_unix_nano") {
            let a = col.as_primitive::<Int64Type>();
            if let Some(mn) = arrow::compute::min(a) {
                lo = Some(lo.map_or(mn, |x: i64| x.min(mn)));
            }
            if let Some(mx) = arrow::compute::max(a) {
                hi = Some(hi.map_or(mx, |x: i64| x.max(mx)));
            }
        }
    }
    (lo, hi)
}

impl Sink {
    /// New sink.
    pub fn new(store: Arc<dyn ObjectStore>, cfg: LakeConfig, naming: FileNaming) -> Self {
        Self { store, cfg, naming }
    }

    /// File metadata of spec section 5.4.
    pub fn file_metadata(&self, table: &SortedTableBuffer, chunks: &[RecordBatch], seq: u64, window_start_secs: i64) -> Vec<KeyValue> {
        let schema = dataset_schema(table.dataset(), &self.cfg);
        let mut kv = vec![
            KeyValue::new("format_version".into(), "1".to_string()),
            KeyValue::new("series_hash".into(), "xxh3_128/canonical_v1".to_string()),
            KeyValue::new("schema_fingerprint".into(), format!("{:016x}", schema_fingerprint(&schema))),
            KeyValue::new("sort_key".into(), table.spec().metadata_string()),
            KeyValue::new("writer_id".into(), self.naming.writer_id.clone()),
            KeyValue::new("boot_id".into(), self.naming.boot_id.clone()),
            KeyValue::new("seq".into(), seq.to_string()),
            KeyValue::new("window_start".into(), window_start_secs.to_string()),
            KeyValue::new("window_end".into(), (window_start_secs + i64::try_from(self.cfg.window_interval.as_secs()).unwrap_or(15)).to_string()),
            KeyValue::new("row_count".into(), chunks.iter().map(RecordBatch::num_rows).sum::<usize>().to_string()),
        ];
        if !table.dataset().is_series() {
            let (lo, hi) = time_range(chunks);
            if let (Some(lo), Some(hi)) = (lo, hi) {
                kv.push(KeyValue::new("min_time_unix_nano".into(), lo.to_string()));
                kv.push(KeyValue::new("max_time_unix_nano".into(), hi.to_string()));
            }
        }
        kv
    }

    async fn write_table(&self, table: &SortedTableBuffer, path: &Path, seq: u64, window_start_secs: i64, cancel: &CancellationToken) -> Result<usize> {
        let runs: Vec<RecordBatch> = table.iter_snapshots().cloned().collect();
        let chunks = merge_runs(&runs, table.spec(), self.cfg.sorting.merge_chunk_bytes)?;
        let schema = dataset_schema(table.dataset(), &self.cfg);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_size(usize::MAX)
            .set_key_value_metadata(Some(self.file_metadata(table, &chunks, seq, window_start_secs)))
            .build();
        let buf = BufWriter::with_capacity(self.store.clone(), path.clone(), self.cfg.upload.part_bytes)
            .with_max_concurrency(self.cfg.upload.concurrency);
        let object_writer = ParquetObjectWriter::from_buf_writer(buf);
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;
        let mut rows = 0;
        for chunk in &chunks {
            if cancel.is_cancelled() {
                let _ = writer.into_inner().into_inner().abort().await;
                return Err(Error::Cancelled);
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    let _ = writer.into_inner().into_inner().abort().await;
                    return Err(Error::Cancelled);
                }
                res = writer.write(chunk) => { res?; }
            }
            rows += chunk.num_rows();
            if writer.memory_size() >= self.cfg.parquet.writer_limit_bytes
                || writer.in_progress_size() >= self.cfg.parquet.row_group_bytes
            {
                writer.flush().await?;
            }
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                let _ = writer.into_inner().into_inner().abort().await;
                Err(Error::Cancelled)
            }
            res = writer.close() => { let _ = res?; Ok(rows) }
        }
    }

    /// Write every non-empty table of a sealed block, series datasets first.
    pub async fn write_block<T>(&self, block: &Block<T>, cancel: &CancellationToken) -> Result<FlushReport> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mut report = FlushReport::default();
        for table in block.tables() {
            if table.is_empty() {
                continue;
            }
            let path = object_path(table.dataset(), block.partition, block.window_start_secs, &self.naming, block.seq);
            let rows = self.write_table(table, &path, block.seq, block.window_start_secs, cancel).await?;
            report.files.push((table.dataset(), path, rows));
        }
        Ok(report)
    }
}
```

Notes for the implementer:
- `AsyncArrowWriter::into_inner` consumes the writer and returns the `ParquetObjectWriter`; `ParquetObjectWriter::into_inner` returns the `object_store::buffered::BufWriter`, whose `abort()` aborts an in-flight multipart upload (verified against parquet 58.3 and object_store 0.13.2 docs). If the abort itself fails, the leftover is covered by the bucket lifecycle rule of spec 5.3.
- `set_max_row_group_size(usize::MAX)` disables the row-count-based row group split so the byte-based `flush()` in the loop controls row groups; `in_progress_size()` is the encoded size estimate.
- Tables are iterated through `Block::tables()`, whose `BTreeMap` order writes each signal's series dataset before its values datasets.

Add `pub mod sink;` to `src/lib.rs`.

- [ ] **Step 4: Run tests**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake sink::tests`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake
git commit -m "feat(series_lake): parquet sink with hive paths, frozen names and cancellation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 13: Reference-oracle property test and fuzz-style tests

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/tests/oracle.rs`
- Create: `rust/otap-dataflow/crates/series-lake/tests/fuzz_canonical.rs`

**Interfaces:**
- Consumes the public API of every module. No new production code.

The oracle: generate random log records (random `host.id` from a small set, random `logger.name` from a small set, random timestamps including 0, random bodies and residual attributes), split them into random requests (1..=5 records each), extract and admit into a `Block`, seal, write with `Sink` to a `LocalFileSystem`, read back every Parquet file, and compare against a naive model: `Vec<Row>` sorted with `sort_by` on `(series_id, time_unix_nano nulls last)`; the descriptor set must equal the set of distinct `(host.id, logger.name)` pairs. Runs with sorting enabled (exact order equality) and disabled (multiset equality).

- [ ] **Step 1: Write `tests/oracle.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reference-oracle property test (spec section 9.1).

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::Int64Type;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use otel_arrow_dfe_series_lake::schema::Dataset;
use otel_arrow_dfe_series_lake::sink::{FileNaming, Sink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use proptest::prelude::*;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
struct Rec {
    host: u8,
    logger: u8,
    time: u64,
    body: String,
}

fn rec_strategy() -> impl Strategy<Value = Rec> {
    (0u8..3, 0u8..3, prop_oneof![Just(0u64), 1u64..1_000_000], "[a-z]{0,8}")
        .prop_map(|(host, logger, time, body)| Rec { host, logger, time, body })
}

fn kv(k: &str, v: &str) -> KeyValue {
    KeyValue { key: k.into(), value: Some(AnyValue { value: Some(any_value::Value::StringValue(v.into())) }) }
}

fn to_logs_data(recs: &[Rec]) -> LogsData {
    // One ResourceLogs per host so resources differ; records grouped by host.
    let mut hosts: Vec<u8> = recs.iter().map(|r| r.host).collect();
    hosts.sort_unstable();
    hosts.dedup();
    LogsData {
        resource_logs: hosts
            .iter()
            .map(|h| ResourceLogs {
                resource: Some(Resource { attributes: vec![kv("host.id", &format!("h{h}"))], ..Default::default() }),
                scope_logs: vec![ScopeLogs {
                    log_records: recs
                        .iter()
                        .filter(|r| r.host == *h)
                        .map(|r| LogRecord {
                            time_unix_nano: r.time,
                            body: Some(AnyValue { value: Some(any_value::Value::StringValue(r.body.clone())) }),
                            attributes: vec![kv("logger.name", &format!("L{}", r.logger))],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .collect(),
    }
}

/// Model row: what the values file must contain, in oracle order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ModelRow {
    series: (u8, u8),
    time: Option<i64>,
    body: String,
}

async fn run_case(recs: Vec<Rec>, splits: Vec<u8>, sorting: bool) -> Result<(), TestCaseError> {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = vec!["logger.name".into()];
    cfg.sorting.enabled = sorting;
    cfg.sorting.run_target_bytes = 1; // seal a run per request: maximal merge pressure
    cfg.sorting.merge_chunk_bytes = 1;
    cfg.ingress.max_row_bytes = 0; // disabled check for tiny targets: validate() is not called here
    let dir = tempfile::tempdir().map_err(|e| TestCaseError::fail(e.to_string()))?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).map_err(|e| TestCaseError::fail(e.to_string()))?);
    let mut cache = SeriesCache::new(1000);
    let mut block: Block<usize> = Block::new(1_789_960_500, 1, &cfg);

    // Split into requests.
    let mut idx = 0;
    let mut req = 0;
    while idx < recs.len() {
        let n = usize::from(splits[req % splits.len()].max(1)).min(recs.len() - idx);
        let chunk = &recs[idx..idx + n];
        idx += n;
        req += 1;
        let records = encode_logs(&to_logs_data(chunk));
        let e = extract(&records, &cfg).map_err(|e| TestCaseError::fail(format!("{e}")))?;
        let r = block.reserve(&e, &mut cache, 8, &cfg);
        block.admit(e, r, req, 0, &cfg).map_err(|e| TestCaseError::fail(format!("{e}")))?;
    }
    block.seal().map_err(|e| TestCaseError::fail(format!("{e}")))?;
    let sink = Sink::new(store, cfg.clone(), FileNaming::new("oracle"));
    let report = sink.write_block(&block, &CancellationToken::new()).await.map_err(|e| TestCaseError::fail(format!("{e}")))?;

    // Oracle.
    let mut model: Vec<ModelRow> = recs
        .iter()
        .map(|r| ModelRow { series: (r.host, r.logger), time: (r.time > 0).then_some(r.time as i64), body: r.body.clone() })
        .collect();
    let expected_series: BTreeSet<(u8, u8)> = recs.iter().map(|r| (r.host, r.logger)).collect();

    // Actual.
    let mut actual: Vec<(Vec<u8>, Option<i64>, String)> = Vec::new();
    let mut series_ids: BTreeSet<Vec<u8>> = BTreeSet::new();
    for (ds, path, _) in &report.files {
        let file = std::fs::File::open(dir.path().join(path.as_ref())).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| TestCaseError::fail(e.to_string()))?.build().map_err(|e| TestCaseError::fail(e.to_string()))?;
        for b in reader {
            let b = b.map_err(|e| TestCaseError::fail(e.to_string()))?;
            let ids = b.column_by_name("series_id").expect("id").as_fixed_size_binary();
            match ds {
                Dataset::LogsSeries => {
                    for i in 0..b.num_rows() {
                        let _ = series_ids.insert(ids.value(i).to_vec());
                    }
                }
                Dataset::LogsValues => {
                    let t = b.column_by_name("time_unix_nano").expect("t").as_primitive::<Int64Type>();
                    let body = b.column_by_name("body").expect("body").as_string::<i32>();
                    for i in 0..b.num_rows() {
                        actual.push((ids.value(i).to_vec(), t.is_valid(i).then(|| t.value(i)), body.value(i).to_string()));
                    }
                }
                _ => return Err(TestCaseError::fail("unexpected dataset")),
            }
        }
    }
    prop_assert_eq!(series_ids.len(), expected_series.len(), "distinct descriptors");
    prop_assert_eq!(actual.len(), model.len(), "row count");

    // Map model series to actual ids through the (host, logger) -> id association observed in the data.
    // Since ids are opaque, compare orderings: group actual by id, model by series, and check
    // that both orders are consistent with the sort spec.
    if sorting {
        // Sorted by (series_id, time nulls last): within the output, ids must be non-decreasing and
        // times non-decreasing within one id with nulls at the end.
        for w in actual.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            prop_assert!(a.0 <= b.0, "series ids non-decreasing");
            if a.0 == b.0 {
                match (a.1, b.1) {
                    (Some(x), Some(y)) => prop_assert!(x <= y, "time non-decreasing"),
                    (None, Some(_)) => return Err(TestCaseError::fail("null before value")),
                    _ => {}
                }
            }
        }
    }
    // Multiset equality of (time, body) per series, independent of ids.
    model.sort();
    let mut actual_tb: Vec<(Option<i64>, String)> = actual.iter().map(|(_, t, b)| (*t, b.clone())).collect();
    let mut model_tb: Vec<(Option<i64>, String)> = model.iter().map(|m| (m.time, m.body.clone())).collect();
    actual_tb.sort();
    model_tb.sort();
    prop_assert_eq!(actual_tb, model_tb, "rows as multiset");
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, .. ProptestConfig::default() })]

    /// Scenario: random records, random request splits, sorting enabled.
    /// Guarantees: output equals the naive model as a multiset and is globally sorted by the spec.
    #[test]
    fn oracle_sorted(recs in prop::collection::vec(rec_strategy(), 1..60), splits in prop::collection::vec(1u8..6, 1..8)) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(run_case(recs, splits, true))?;
    }

    /// Scenario: the same with sorting disabled.
    /// Guarantees: output is still the same multiset; invariants do not depend on sorting.
    #[test]
    fn oracle_unsorted(recs in prop::collection::vec(rec_strategy(), 1..60), splits in prop::collection::vec(1u8..6, 1..8)) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("rt");
        rt.block_on(run_case(recs, splits, false))?;
    }
}
```

`max_row_bytes = 0` would refuse every row through `RowSink::push`; set it to `1 << 20` instead and keep `run_target_bytes = 1` (the `validate()` relation is not enforced by `RowSink`). Correct the line to `cfg.ingress.max_row_bytes = 1 << 20;` when writing the file.

- [ ] **Step 2: Write `tests/fuzz_canonical.rs`**

```rust
// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fuzz-style property tests for the canonical encoder and CBOR decoder (spec section 9.3).

use otel_arrow_dfe_series_lake::canonical::{Descriptor, Signal, canonical_bytes, series_id};
use otel_arrow_dfe_series_lake::value::{Value, decode_cbor, sort_kvlist};
use proptest::prelude::*;

fn value_strategy() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<String>().prop_map(Value::Str),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(|b| Value::Double(f64::from_bits(b))),
        any::<bool>().prop_map(Value::Bool),
    ];
    leaf.prop_recursive(4, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::vec((any::<String>(), inner), 0..4).prop_map(|mut kvs| {
                kvs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                kvs.dedup_by(|a, b| a.0 == b.0);
                Value::KvList(kvs)
            }),
        ]
    })
}

fn to_cbor(v: &Value) -> ciborium::Value {
    match v {
        Value::Null => ciborium::Value::Null,
        Value::Str(s) => ciborium::Value::Text(s.clone()),
        Value::Bytes(b) => ciborium::Value::Bytes(b.clone()),
        Value::Int(i) => ciborium::Value::Integer((*i).into()),
        Value::Double(d) => ciborium::Value::Float(*d),
        Value::Bool(b) => ciborium::Value::Bool(*b),
        Value::Array(items) => ciborium::Value::Array(items.iter().map(to_cbor).collect()),
        Value::KvList(kvs) => ciborium::Value::Map(kvs.iter().map(|(k, v)| (ciborium::Value::Text(k.clone()), to_cbor(v))).collect()),
    }
}

fn canon_nan(v: &Value) -> Value {
    match v {
        Value::Double(d) if d.is_nan() => Value::Double(f64::NAN),
        Value::Array(items) => Value::Array(items.iter().map(canon_nan).collect()),
        Value::KvList(kvs) => Value::KvList(kvs.iter().map(|(k, v)| (k.clone(), canon_nan(v))).collect()),
        other => other.clone(),
    }
}

proptest! {
    /// Scenario: arbitrary value trees round-trip through CBOR.
    /// Guarantees: decode never panics and reproduces the tree (NaN payloads excepted, which CBOR
    /// may collapse), so OTAP-converted and original descriptors hash identically.
    #[test]
    fn cbor_round_trip_never_panics(v in value_strategy()) {
        let mut buf = Vec::new();
        ciborium::into_writer(&to_cbor(&v), &mut buf).expect("encode");
        let decoded = decode_cbor(&buf, 32).expect("decode");
        let a = canonical_bytes(&desc(vec![("k".into(), canon_nan(&v))]));
        let b = canonical_bytes(&desc(vec![("k".into(), canon_nan(&decoded))]));
        prop_assert_eq!(a, b);
    }

    /// Scenario: arbitrary attribute lists in arbitrary order.
    /// Guarantees: encoding is order independent after sort_kvlist and the hash is 16 bytes.
    #[test]
    fn encoding_is_order_independent(mut kvs in prop::collection::vec((any::<String>(), value_strategy()), 0..6)) {
        kvs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        kvs.dedup_by(|a, b| a.0 == b.0);
        let mut shuffled = kvs.clone();
        shuffled.reverse();
        sort_kvlist(&mut shuffled).expect("unique keys");
        let a = canonical_bytes(&desc(kvs));
        let b = canonical_bytes(&desc(shuffled));
        prop_assert_eq!(&a, &b);
        prop_assert_eq!(series_id(&a).len(), 16);
    }
}

fn desc(attrs: Vec<(String, Value)>) -> Descriptor {
    Descriptor {
        signal: Signal::Logs,
        resource_attrs: vec![],
        resource_schema_url: String::new(),
        scope_name: String::new(),
        scope_version: String::new(),
        scope_schema_url: String::new(),
        scope_attrs: vec![],
        metric: None,
        attrs,
    }
}
```

- [ ] **Step 3: Run both test files**

Run: `cd rust/otap-dataflow && cargo test -p otel-arrow-dfe-series-lake --test oracle --test fuzz_canonical`
Expected: 4 passed. If `oracle_sorted` fails on "series ids non-decreasing", the merge or run sort is wrong; if it fails on "rows as multiset", extraction or interleave dropped or duplicated rows. Shrunken cases from proptest are the debugging input.

- [ ] **Step 4: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake/tests
git commit -m "test(series_lake): reference-oracle property test and canonical fuzzing

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 14: FORMAT.md, README, changelog and workspace checks

**Files:**
- Create: `rust/otap-dataflow/crates/series-lake/docs/FORMAT.md`
- Modify: `rust/otap-dataflow/crates/series-lake/README.md`
- Create: `rust/otap-dataflow/.chloggen/series-lake-core.yaml`

- [ ] **Step 1: Write `docs/FORMAT.md`**

An implementation-independent description, no Rust, no Dataflow, no buffer internals. Copy the following sections from the design spec (revision 4) verbatim, adjusting only cross-references: section 4 (canonical encoding v1, without the paragraph about `crates/series-lake/docs/FORMAT.md`), section 5.1 (datasets, columns, timestamp rule, `render_v1`, unsupported inputs), section 5.2 (denormalization and the schema contract), section 5.3 (layout, naming, visibility guarantees), section 5.4 (Parquet options, metadata, compaction scope), section 5.5 (reading, canonical view, coverage guarantee). Add a short preamble:

```markdown
# Series Lake Format, version 1

This document specifies the on-disk format written by the series Parquet
exporter: series identity, datasets, layout, file semantics, delivery
semantics and compatibility rules. It is independent of the writer
implementation; any reader or writer in any language may rely on it.

Golden vectors for the identity encoding live next to this file in
`../tests/golden/canonical_v1.json`, generated by `../tools/gen_golden.py`.
```

and a closing section:

```markdown
## Compatibility rules

- `format_version` is part of every file's metadata and of the `v=1` path
  segment. A change to the identity encoding, to an existing column's type or
  meaning, or to the partition layout requires a new version.
- Within one version, dataset schemas change only additively (new nullable
  columns). Readers must read with union-by-name semantics.
- `series_id` values are comparable across writers, versions of this writer,
  and languages, as long as `format_version` matches.
```

Run `npx markdownlint-cli2 rust/otap-dataflow/crates/series-lake/docs/FORMAT.md` and `python3 tools/sanitycheck.py` from the repository root; fix any findings.

- [ ] **Step 2: Extend `README.md`**

Append after the existing text:

```markdown
## Modules

- `canonical`: identity encoding v1 and `series_id`.
- `extract`: OTAP records to descriptors and values batches.
- `cache`: bounded LRU of series ids to their last committed partition.
- `buffer`: sorted run buffers and the block accounting model.
- `sort`: sort keys, double normalization, k-way merge.
- `sink`: Parquet files on an `object_store`, Hive layout, frozen names.
- `clock`: wall clock trait and aligned window boundaries.

## Testing

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-series-lake
```

The reference-oracle property test in `tests/oracle.rs` is the main
correctness test; `tests/golden.rs` checks the identity encoding against
vectors produced by an independent Python implementation.
```

- [ ] **Step 3: Add the changelog entry `.chloggen/series-lake-core.yaml`**

```yaml
change_type: new_component
component: pipeline
note: "Add the series-lake crate: canonical series identity, series/values extraction from OTAP records, bounded series cache, sorted block buffers and a Parquet sink over object_store."
issues: [0]
subtext: |
  This is the engine-independent core of the upcoming exporter:series_parquet node.
  The storage format is documented in crates/series-lake/docs/FORMAT.md.
```

Replace `[0]` with the tracking issue or PR number once it exists. Run `make chlog-validate` from the repository root (or `cd rust/otap-dataflow && make chlog-validate` if the target lives there; check the Makefile).

- [ ] **Step 4: Run the full workspace checks**

Run:

```bash
cd rust/otap-dataflow
cargo xtask structure-check
cargo xtask quick-check
cargo test -p otel-arrow-dfe-series-lake
cd ../.. && python3 tools/sanitycheck.py
```

Expected: all pass with no warnings from the `series-lake` crate. Fix clippy findings (`unwrap_used`, `unused_results`, `missing_docs`) in place.

- [ ] **Step 5: Commit**

```bash
git add rust/otap-dataflow/crates/series-lake rust/otap-dataflow/.chloggen/series-lake-core.yaml
git commit -m "docs(series_lake): format specification, readme and changelog

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Out of scope for this plan

- The Dataflow node `exporter:series_parquet` (admission loop, window sleep,
  ack tokens, notification queue, flush task, shutdown): plan 2.
- End-to-end tests with a real OTLP producer and MinIO, the v1 outage test:
  plan 2.
- Benchmarks and expansion-factor measurements: plan 3.
- The deferred items of spec section 10.
