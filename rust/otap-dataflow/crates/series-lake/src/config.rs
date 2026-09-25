// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Format configuration types: the lake subset of the series_parquet
//! exporter's configuration.

use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::error::{Error, Result};

/// Deserialize a byte size written either as a number of bytes or as a
/// string with units (`64MiB`, `1 GB`), the form every byte-valued engine
/// setting accepts.
///
/// Local to this crate, over the same `byte-unit` parser the engine's config
/// crate uses, so the format library does not depend on the engine's
/// configuration stack.
fn byte_size<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<usize, D::Error> {
    use serde::de::Error as _;
    let bytes = match serde_json::Value::deserialize(d)? {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| {
            D::Error::custom(format!("byte size {n} must be a non-negative integer"))
        })?,
        serde_json::Value::String(text) => text
            .parse::<byte_unit::Byte>()
            .map_err(|e| D::Error::custom(format!("byte size {text:?}: {e}")))?
            .as_u64(),
        serde_json::Value::Null => return Err(D::Error::custom("byte size must not be null")),
        other => {
            return Err(D::Error::custom(format!(
                "byte size must be a number of bytes or a string such as \"64MiB\", not {other}"
            )));
        }
    };
    usize::try_from(bytes).map_err(|_| {
        serde::de::Error::custom(format!(
            "byte size {bytes} exceeds this platform's usize::MAX ({})",
            usize::MAX
        ))
    })
}

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
    Full {
        path: String,
        column: Option<String>,
        #[serde(default, rename = "type")]
        ty: DenormType,
    },
}

impl<'de> Deserialize<'de> for Denormalize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(match DenormRepr::deserialize(d)? {
            DenormRepr::Short(path) => Denormalize {
                column: Self::default_column(&path),
                path,
                ty: DenormType::String,
            },
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
    Full {
        column: String,
        #[serde(default)]
        order: SortOrder,
        #[serde(default)]
        nulls: Nulls,
    },
}

impl<'de> Deserialize<'de> for SortKey {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(match SortKeyRepr::deserialize(d)? {
            SortKeyRepr::Short(column) => SortKey {
                column,
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
            SortKeyRepr::Full {
                column,
                order,
                nulls,
            } => SortKey {
                column,
                order,
                nulls,
            },
        })
    }
}

fn default_values_sort() -> Vec<SortKey> {
    vec![
        SortKey {
            column: "series_id".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        },
        SortKey {
            column: "time_unix_nano".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        },
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
    /// What happens to exemplars (metrics only); unset means drop. See
    /// [`LakeConfig::exemplar_policy`].
    #[serde(default)]
    pub exemplars: Option<ExemplarPolicy>,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            series_attributes: vec![],
            denormalize: vec![],
            values_sort: default_values_sort(),
            exemplars: None,
        }
    }
}

/// Request and block budgets.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngressLimits {
    /// Logical input size limit.
    #[serde(deserialize_with = "byte_size")]
    pub max_request_bytes: usize,
    /// Extracted output limit, enforced on the *measured* extracted output
    /// plus the decoded attribute tables it is built from.
    ///
    /// Values rows are charged as estimates only while their run is being
    /// built; sealing a run replaces that estimate with the measured pinned
    /// bytes of the Arrow batch, so a row is never counted twice. Descriptor
    /// rows, which are not sealed into Arrow batches during extraction, stay
    /// charged at their estimated size.
    #[serde(deserialize_with = "byte_size")]
    pub max_extracted_bytes: usize,
    /// Single row limit.
    #[serde(deserialize_with = "byte_size")]
    pub max_row_bytes: usize,
    /// Nested value depth limit.
    pub max_nesting_depth: usize,
    /// Retained-bytes limit of one block.
    #[serde(deserialize_with = "byte_size")]
    pub max_block_bytes: usize,
    /// Ack tokens (requests) one block may hold.
    pub max_requests_per_block: usize,
    /// Distinct series one request may carry; see
    /// [`LakeConfig::check_request_bound`] for the bound it keeps.
    pub max_series_per_request: usize,
    /// Fixed bytes charged per `pending_series` entry.
    #[serde(deserialize_with = "byte_size")]
    pub pending_series_entry_bytes: usize,
}

impl Default for IngressLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 16 << 20,
            max_extracted_bytes: 32 << 20,
            max_row_bytes: 1 << 20,
            max_nesting_depth: 32,
            max_block_bytes: 500 << 20,
            max_requests_per_block: 4096,
            max_series_per_request: DEFAULT_MAX_SERIES_PER_REQUEST,
            pending_series_entry_bytes: 64,
        }
    }
}

/// Default `ingress.max_series_per_request`: what
/// [`LakeConfig::derived_max_series_per_request`] gives for the default
/// budgets (1,393,826), rounded down so that the defaults stay valid with up
/// to eight denormalized columns per signal.
pub const DEFAULT_MAX_SERIES_PER_REQUEST: usize = 1_000_000;

/// The completion token bytes one request's block worst case allows for.
///
/// The token is the request's routing context; measured tokens hold 200 to
/// 464 bytes. A larger one can only make a request that passed ingress miss
/// an empty block, which block admission reports as an internal failure.
pub const TOKEN_ALLOWANCE_BYTES: usize = 4 << 10;

/// Sorting configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SortingConfig {
    /// Sort values datasets at all.
    pub enabled: bool,
    /// Run size target.
    #[serde(deserialize_with = "byte_size")]
    pub run_target_bytes: usize,
    /// Merge output chunk size.
    #[serde(deserialize_with = "byte_size")]
    pub merge_chunk_bytes: usize,
}

impl Default for SortingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            run_target_bytes: 8 << 20,
            merge_chunk_bytes: 16 << 20,
        }
    }
}

/// Upload buffering.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct UploadConfig {
    /// Multipart part size and `BufWriter` capacity.
    #[serde(deserialize_with = "byte_size")]
    pub part_bytes: usize,
    /// In-flight parts.
    pub concurrency: usize,
    /// Upper bound on a best-effort multipart abort.
    #[serde(with = "humantime_serde")]
    pub abort_timeout: Duration,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            part_bytes: 8 << 20,
            concurrency: 2,
            abort_timeout: Duration::from_secs(5),
        }
    }
}

/// Parquet writer limits.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ParquetConfig {
    /// Row group target.
    #[serde(deserialize_with = "byte_size")]
    pub row_group_bytes: usize,
    /// Enforced writer memory threshold.
    #[serde(deserialize_with = "byte_size")]
    pub writer_limit_bytes: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            row_group_bytes: 64 << 20,
            writer_limit_bytes: 96 << 20,
        }
    }
}

/// Policy for exponential histograms and summaries.
///
/// `drop` is the default: one such point in a metrics request would
/// otherwise refuse the whole request, and a producer drops permanently
/// refused data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum UnsupportedPolicy {
    /// Nack the whole request.
    Reject,
    /// Drop the unsupported points, keep the rest.
    #[default]
    Drop,
}

/// Policy for the exemplars of the points the lake stores.
///
/// No dataset has a column for exemplars, so a stored point never keeps its
/// exemplars; the policy decides whether a request carrying them is
/// accepted without them or refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExemplarPolicy {
    /// Store the points, drop their exemplars and count them.
    Drop,
    /// Refuse the whole request.
    Reject,
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

/// Largest `ingress.max_nesting_depth` accepted.
///
/// The depth becomes the CBOR parser's recursion limit, and the parser
/// recurses on the stack once per level, so an unbounded value would let one
/// request exhaust the stack. 256 is the parser's own default.
pub const MAX_NESTING_DEPTH: usize = 256;

/// Largest accepted `upload.part_bytes`: the S3 multipart maximum part size.
pub const MAX_PART_BYTES: usize = 5 << 30;

/// Most parts one S3 multipart upload may have.
pub const MAX_PARTS: usize = 10_000;

/// Hive partition keys of the lake layout, which no file column may be named
/// like: a reader that resolves partition keys by name would read the file
/// column and the path key as one.
pub const PARTITION_KEYS: [&str; 5] = ["v", "signal", "dataset", "date", "hour"];

impl LakeConfig {
    /// What happens to the exemplars of a stored point.
    ///
    /// `metrics.exemplars` alone decides, and an unset one means drop,
    /// whatever `unsupported` says: most SDKs attach exemplars by default,
    /// so refusing them unless asked would refuse a large share of real
    /// metrics. Exemplars of a point that `unsupported: drop` discards go
    /// with that point whatever this says.
    #[must_use]
    pub fn exemplar_policy(&self) -> ExemplarPolicy {
        self.metrics.exemplars.unwrap_or(ExemplarPolicy::Drop)
    }

    /// Parts one multipart upload of a file as large as a whole block would
    /// take; above [`MAX_PARTS`] such an upload is refused by S3.
    #[must_use]
    pub fn parts_per_block(&self) -> usize {
        self.ingress
            .max_block_bytes
            .div_ceil(self.upload.part_bytes.max(1))
    }

    /// The fixed bytes F a block charges per series row, pending entry
    /// included: `16 * C + 8 + Q`, with C the series columns and Q
    /// `pending_series_entry_bytes`; `extract::series_row_charge` has the
    /// measurement behind it.
    ///
    /// `Block::reserve` judges a request on exactly
    /// `P + T + sum_i (2 * (A_i - D_i) + F)`: P the request's values bytes
    /// and the merge-key bound of their rows, T its token, A_i a series'
    /// extracted estimate and D_i its decoded attribute trees. That never exceeds `2 * E + T + S * F`, E the
    /// request's extracted charge and S its number of series, which
    /// [`LakeConfig::check_request_bound`] keeps within `max_block_bytes`.
    #[must_use]
    pub fn series_row_fixed_bytes(&self, signal: crate::canonical::Signal) -> usize {
        crate::extract::series_row_charge(0, self.series_columns(signal))
            + self.ingress.pending_series_entry_bytes
    }

    /// The larger [`LakeConfig::series_row_fixed_bytes`] of the two signals,
    /// with the signal it belongs to.
    #[must_use]
    pub fn max_series_row_fixed_bytes(&self) -> (crate::canonical::Signal, usize) {
        use crate::canonical::Signal;
        let logs = self.series_row_fixed_bytes(Signal::Logs);
        let metrics = self.series_row_fixed_bytes(Signal::Metrics);
        if logs > metrics {
            (Signal::Logs, logs)
        } else {
            (Signal::Metrics, metrics)
        }
    }

    /// The most series per request that keep one request's worst case
    /// within an empty block: `(B - 2 * E - T) / F_max`, with B
    /// `max_block_bytes`, E `max_extracted_bytes`, T
    /// [`TOKEN_ALLOWANCE_BYTES`] and F_max from
    /// [`LakeConfig::max_series_row_fixed_bytes`]. Zero when the budgets
    /// leave no room for a series.
    #[must_use]
    pub fn derived_max_series_per_request(&self) -> usize {
        let (_, fixed) = self.max_series_row_fixed_bytes();
        self.ingress
            .max_block_bytes
            .saturating_sub(self.ingress.max_extracted_bytes.saturating_mul(2))
            .saturating_sub(TOKEN_ALLOWANCE_BYTES)
            / fixed.max(1)
    }

    /// Refuse budgets under which a request that passes ingress might not
    /// fit an empty block: `2 * E + S * F_max + T <= B`, with S
    /// `max_series_per_request` and the other terms as in
    /// [`LakeConfig::derived_max_series_per_request`].
    ///
    /// The `2 * E` term covers a request's values rows with their merge keys
    /// and its series rows at twice their extracted estimate, `S * F_max` the
    /// fixed part of every series row, and T its completion token. `block_key` is the name the
    /// caller's users write for `max_block_bytes`.
    ///
    /// # Errors
    /// Returns an invalid-configuration error that names every term.
    pub fn check_request_bound(&self, block_key: &str) -> Result<()> {
        let limits = &self.ingress;
        if limits.max_series_per_request == 0 {
            return Err(Error::invalid(
                "ingress.max_series_per_request must be at least 1",
            ));
        }
        let (signal, fixed) = self.max_series_row_fixed_bytes();
        let extracted = limits.max_extracted_bytes.saturating_mul(2);
        let series = limits.max_series_per_request.saturating_mul(fixed);
        let worst = extracted
            .saturating_add(series)
            .saturating_add(TOKEN_ALLOWANCE_BYTES);
        if worst <= limits.max_block_bytes {
            return Ok(());
        }
        let signal = match signal {
            crate::canonical::Signal::Logs => "logs",
            crate::canonical::Signal::Metrics => "metrics",
        };
        Err(Error::invalid(format!(
            "{block_key} ({}) must hold the worst case of one request, {worst} bytes: \
             2 * ingress.max_extracted_bytes ({extracted}) + ingress.max_series_per_request \
             ({}) * {fixed} bytes per {signal} series ({series}) + {TOKEN_ALLOWANCE_BYTES} \
             bytes of completion token; raise {block_key}, or lower \
             ingress.max_extracted_bytes or ingress.max_series_per_request",
            limits.max_block_bytes, limits.max_series_per_request,
        )))
    }

    /// Columns of the series dataset of `signal` under this configuration.
    #[must_use]
    pub fn series_columns(&self, signal: crate::canonical::Signal) -> usize {
        crate::schema::dataset_schema(crate::schema::Dataset::series_of(signal), self)
            .fields()
            .len()
    }

    /// Check the cross-field rules; each error names the dotted key it
    /// refused (FORMAT.md section 3 for the denormalization rules).
    pub fn validate(&self) -> Result<()> {
        if self.writer_id.is_empty() {
            return Err(Error::invalid("writer_id must not be empty"));
        }
        if !self
            .writer_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
        {
            return Err(Error::invalid(format!(
                "writer_id {:?} must use only [A-Za-z0-9_.]: it sits between '-' separators \
                 in every file name",
                self.writer_id
            )));
        }
        if self.ingress.max_nesting_depth > MAX_NESTING_DEPTH {
            return Err(Error::invalid(format!(
                "ingress.max_nesting_depth must be at most {MAX_NESTING_DEPTH}"
            )));
        }
        if self.logs.exemplars.is_some() {
            return Err(Error::invalid(
                "logs.exemplars is not a setting: log records carry no exemplars; set \
                 metrics.exemplars",
            ));
        }
        if !self.metrics.series_attributes.is_empty() {
            return Err(Error::invalid(
                "metrics.series_attributes is not supported: a metric's identity already \
                 includes every point attribute",
            ));
        }
        if self.ingress.max_row_bytes > self.sorting.run_target_bytes / 4 {
            return Err(Error::invalid(
                "ingress.max_row_bytes must be at most sorting.run_target_bytes / 4",
            ));
        }
        if self.upload.part_bytes < 5 << 20 {
            return Err(Error::invalid(
                "upload.part_bytes must be at least 5MiB (S3 multipart minimum)",
            ));
        }
        if self.upload.part_bytes > MAX_PART_BYTES {
            return Err(Error::invalid(
                "upload.part_bytes must be at most 5GiB (S3 multipart maximum)",
            ));
        }
        if self.upload.concurrency == 0 {
            return Err(Error::invalid("upload.concurrency must be at least 1"));
        }
        if self.ingress.max_requests_per_block == 0 {
            return Err(Error::invalid(
                "ingress.max_requests_per_block must be at least 1",
            ));
        }
        self.check_request_bound("ingress.max_block_bytes")?;
        // The window boundary arithmetic and the `window_secs` file metadata
        // both work in whole seconds.
        if self.window_interval.subsec_nanos() != 0 || self.window_interval.as_secs() == 0 {
            return Err(Error::invalid(
                "window_interval must be a whole number of seconds and at least 1s",
            ));
        }
        // Every rule below names the key a user writes: the signal section,
        // the list, the entry's position and the field.
        for (section, signal) in [("logs", &self.logs), ("metrics", &self.metrics)] {
            for (i, d) in signal.denormalize.iter().enumerate() {
                if d.source().is_err() {
                    return Err(Error::invalid(format!(
                        "{section}.denormalize[{i}].path {:?} must start with resource., \
                         scope. or attrs.",
                        d.path
                    )));
                }
                if d.column.contains(':') || d.column.contains(';') {
                    return Err(Error::invalid(format!(
                        "{section}.denormalize[{i}].column {:?} must not contain ':' or ';'",
                        d.column
                    )));
                }
                if PARTITION_KEYS
                    .iter()
                    .any(|key| d.column.eq_ignore_ascii_case(key))
                {
                    return Err(Error::invalid(format!(
                        "{section}.denormalize[{i}].column {:?} is named like a partition key \
                         of the layout",
                        d.column
                    )));
                }
            }
        }
        for ds in crate::schema::Dataset::ALL {
            let (section, signal) = if ds.signal() == crate::canonical::Signal::Logs {
                ("logs", &self.logs)
            } else {
                ("metrics", &self.metrics)
            };
            let schema = crate::schema::dataset_schema(ds, self);
            let mut seen = HashSet::new();
            for f in schema.fields() {
                if !seen.insert(f.name().to_lowercase()) {
                    // Intrinsic names are unique, so a collision always
                    // involves a denormalized column of this signal.
                    let entry = signal
                        .denormalize
                        .iter()
                        .rposition(|d| d.column.eq_ignore_ascii_case(f.name()))
                        .map_or_else(String::new, |i| format!("[{i}]"));
                    return Err(Error::invalid(format!(
                        "{section}.denormalize{entry}.column {:?} is a column name collision \
                         (case-insensitive) in {}",
                        f.name(),
                        ds.name()
                    )));
                }
            }
            if !ds.is_series() {
                for (i, key) in signal.values_sort.iter().enumerate() {
                    let Some((_, field)) = schema.column_with_name(&key.column) else {
                        return Err(Error::invalid(format!(
                            "{section}.values_sort[{i}].column {:?} is not a column of {}",
                            key.column,
                            ds.name()
                        )));
                    };
                    // Sorting goes through Arrow's row format, which cannot
                    // encode every type (a Map, for instance).
                    let sort_field = arrow::row::SortField::new(field.data_type().clone());
                    if !arrow::row::RowConverter::supports_fields(std::slice::from_ref(&sort_field))
                    {
                        return Err(Error::invalid(format!(
                            "{section}.values_sort[{i}].column {:?} has type {}, which Arrow's \
                             row converter cannot sort",
                            key.column,
                            field.data_type()
                        )));
                    }
                    // Block admission reserves every row's merge key, so a
                    // key's encoded size must be computable from its column.
                    if !crate::sort::merge_key_supported(field.data_type()) {
                        return Err(Error::invalid(format!(
                            "{section}.values_sort[{i}].column {:?} has type {}, whose merge \
                             keys a block cannot reserve; sort by a number, timestamp, boolean, \
                             id or string column",
                            key.column,
                            field.data_type()
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule sentence of a configuration refusal.
    fn rule(err: &Error) -> String {
        err.invalid_detail()
            .map_or_else(|| err.to_string(), str::to_owned)
    }

    /// Scenario: `max_row_bytes` exceeds a quarter of `run_target_bytes`.
    /// Guarantees: `validate` refuses the configuration and names the rule.
    #[test]
    fn max_row_bytes_over_quarter_of_run_target_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_row_bytes = cfg.sorting.run_target_bytes;
        let err = cfg.validate().expect_err("max_row_bytes exceeds the bound");
        assert!(err.to_string().contains("run_target_bytes / 4"));
    }

    /// Scenario: `max_requests_per_block` is zero.
    /// Guarantees: `validate` refuses, since a block needs at least one ack token.
    #[test]
    fn max_requests_per_block_zero_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_requests_per_block = 0;
        let err = cfg.validate().expect_err("max_requests_per_block is 0");
        assert!(err.to_string().contains("max_requests_per_block"));
    }

    /// Scenario: `values_sort` names the metrics `bucket_counts` column, a list Arrow's row
    /// converter can sort.
    /// Guarantees: `validate` refuses it, since block admission cannot bound its merge keys.
    #[test]
    fn a_sort_key_whose_merge_keys_cannot_be_reserved_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.metrics.values_sort = vec![SortKey {
            column: "bucket_counts".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }];
        let err = cfg
            .validate()
            .expect_err("a list sort key has no merge-key bound");
        assert!(
            rule(&err).contains(
                "metrics.values_sort[0].column \"bucket_counts\" has type List(non-null Int64), \
                 whose merge keys a block cannot reserve"
            ),
            "{err}"
        );
    }

    /// Scenario: `values_sort` names the logs `attrs` column, a Map.
    /// Guarantees: `validate` refuses it, since Arrow's row converter cannot encode a Map.
    #[test]
    fn a_sort_key_the_row_converter_cannot_sort_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.logs.values_sort = vec![SortKey {
            column: "attrs".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }];
        let err = cfg.validate().expect_err("a Map sort key is unsortable");
        assert!(
            rule(&err).contains("logs.values_sort[0].column \"attrs\" has type"),
            "{err}"
        );
        assert!(err.to_string().contains("row converter cannot sort"));
    }

    /// Scenario: `window_interval` of 500 ms, and of zero.
    /// Guarantees: both are refused.
    #[test]
    fn a_sub_second_window_interval_is_rejected() {
        for interval in [Duration::from_millis(500), Duration::ZERO] {
            let cfg = LakeConfig {
                window_interval: interval,
                ..Default::default()
            };
            let err = cfg.validate().expect_err("sub-second window interval");
            assert!(err.to_string().contains("window_interval"));
        }
        let cfg = LakeConfig {
            window_interval: Duration::from_secs(1),
            ..Default::default()
        };
        cfg.validate().expect("a one second window is valid");
    }

    /// Scenario: a denormalized column name holding the fingerprint's field separators.
    /// Guarantees: `validate` refuses it.
    #[test]
    fn a_denormalized_column_name_with_fingerprint_separators_is_rejected() {
        for column in ["a:Utf8;b", "a;b", "a:b"] {
            let mut cfg = LakeConfig::default();
            cfg.logs.denormalize = vec![Denormalize {
                path: "resource.service.name".into(),
                column: column.into(),
                ty: DenormType::String,
            }];
            let err = cfg.validate().expect_err("separator in a column name");
            assert!(
                rule(&err).contains(&format!(
                    "logs.denormalize[0].column {column:?} must not contain"
                )),
                "{err}"
            );
        }
    }

    /// Scenario: `max_block_bytes` exactly at, and one byte below, one request's worst case
    /// `2 * E + S * F_max + T`, and a series limit of zero.
    /// Guarantees: the first is accepted; the others are refused, the short block with every
    /// term of the inequality.
    #[test]
    fn max_block_bytes_below_one_requests_worst_case_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_series_per_request = 1000;
        let worst = 2 * cfg.ingress.max_extracted_bytes + 1000 * 328 + TOKEN_ALLOWANCE_BYTES;
        cfg.ingress.max_block_bytes = worst;
        cfg.validate().expect("exactly the worst case is enough");
        cfg.ingress.max_block_bytes = worst - 1;
        let err = cfg
            .validate()
            .expect_err("one byte short of the worst case");
        assert_eq!(
            rule(&err),
            format!(
                "ingress.max_block_bytes ({}) must hold the worst case of one request, {worst} \
                 bytes: 2 * ingress.max_extracted_bytes (67108864) + \
                 ingress.max_series_per_request (1000) * 328 bytes per metrics series (328000) \
                 + 4096 bytes of completion token; raise ingress.max_block_bytes, or lower \
                 ingress.max_extracted_bytes or ingress.max_series_per_request",
                worst - 1
            )
        );
        cfg.ingress.max_series_per_request = 0;
        cfg.ingress.max_block_bytes = worst;
        let err = cfg.validate().expect_err("no series per request");
        assert_eq!(
            rule(&err),
            "ingress.max_series_per_request must be at least 1"
        );
    }

    /// Scenario: the default budgets, eight denormalized columns per signal, seven on logs
    /// alone, and a block of exactly twice the extraction budget.
    /// Guarantees: the default series limit is the derived one rounded down and stays valid with
    /// eight denormalized columns, F_max follows the widest signal, and a block with no room left
    /// for a series derives zero.
    #[test]
    fn the_series_limit_is_derived_from_the_budgets() {
        use crate::canonical::Signal;
        let columns = |n: usize| -> Vec<Denormalize> {
            (0..n)
                .map(|i| Denormalize {
                    path: format!("resource.k{i}"),
                    column: format!("k{i}"),
                    ty: DenormType::String,
                })
                .collect()
        };
        let defaults = LakeConfig::default();
        assert_eq!(defaults.derived_max_series_per_request(), 1_393_826);
        assert_eq!(
            defaults.max_series_row_fixed_bytes(),
            (Signal::Metrics, 328)
        );
        defaults.validate().expect("the defaults hold the bound");
        let mut denormalized = LakeConfig::default();
        denormalized.logs.denormalize = columns(8);
        denormalized.metrics.denormalize = columns(8);
        assert!(denormalized.derived_max_series_per_request() >= DEFAULT_MAX_SERIES_PER_REQUEST);
        denormalized
            .validate()
            .expect("eight denormalized columns keep the defaults valid");
        let mut wide = LakeConfig::default();
        wide.logs.denormalize = columns(7);
        assert_eq!(
            wide.max_series_row_fixed_bytes(),
            (Signal::Logs, 232 + 7 * 16)
        );
        let mut tight = LakeConfig::default();
        tight.ingress.max_block_bytes = 2 * tight.ingress.max_extracted_bytes;
        assert_eq!(tight.derived_max_series_per_request(), 0);
    }

    /// Scenario: `upload.part_bytes` at the S3 maximum part size and one byte above.
    /// Guarantees: the maximum is accepted; above it is refused naming the key and the limit.
    #[test]
    fn upload_part_bytes_above_5gib_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.upload.part_bytes = MAX_PART_BYTES;
        cfg.validate().expect("the maximum part size is accepted");
        cfg.upload.part_bytes = MAX_PART_BYTES + 1;
        let err = cfg.validate().expect_err("part_bytes above 5GiB");
        assert_eq!(
            err.invalid_detail(),
            Some("upload.part_bytes must be at most 5GiB (S3 multipart maximum)")
        );
    }

    /// Scenario: whole-block part counts with the defaults and with a partial last part.
    /// Guarantees: the count rounds up.
    #[test]
    fn parts_per_block_rounds_up() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_block_bytes = 500 << 20;
        cfg.upload.part_bytes = 8 << 20;
        assert_eq!(cfg.parts_per_block(), 63);
        cfg.ingress.max_block_bytes = 16 << 20;
        assert_eq!(cfg.parts_per_block(), 2);
    }

    /// Scenario: `upload.part_bytes` below the S3 minimum part size.
    /// Guarantees: `validate` refuses it.
    #[test]
    fn upload_part_bytes_below_5mib_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.upload.part_bytes = (5 << 20) - 1;
        let err = cfg.validate().expect_err("part_bytes below 5MiB");
        assert!(err.to_string().contains("part_bytes"));
    }

    /// Scenario: `upload.concurrency` is zero.
    /// Guarantees: `validate` refuses, since no part could ever be uploaded.
    #[test]
    fn upload_concurrency_zero_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.upload.concurrency = 0;
        let err = cfg.validate().expect_err("concurrency is 0");
        assert!(err.to_string().contains("concurrency"));
    }

    /// Scenario: every combination of `unsupported` and `metrics.exemplars`, unset or set.
    /// Guarantees: all are valid, and only `metrics.exemplars: reject` rejects exemplars.
    #[test]
    fn metrics_exemplars_alone_decides_and_defaults_to_drop() {
        for unsupported in [UnsupportedPolicy::Reject, UnsupportedPolicy::Drop] {
            for (exemplars, expected) in [
                (None, ExemplarPolicy::Drop),
                (Some(ExemplarPolicy::Drop), ExemplarPolicy::Drop),
                (Some(ExemplarPolicy::Reject), ExemplarPolicy::Reject),
            ] {
                let mut cfg = LakeConfig {
                    unsupported,
                    ..LakeConfig::default()
                };
                cfg.metrics.exemplars = exemplars;
                cfg.validate().expect("a valid combination");
                assert_eq!(
                    cfg.exemplar_policy(),
                    expected,
                    "{unsupported:?} / {exemplars:?}"
                );
            }
        }
    }

    /// Scenario: `unsupported` omitted, `reject` and `drop`.
    /// Guarantees: omitted means `drop`; an explicit value is kept.
    #[test]
    fn unsupported_defaults_to_drop() {
        assert_eq!(LakeConfig::default().unsupported, UnsupportedPolicy::Drop);
        for (doc, expected) in [
            (serde_json::json!({}), UnsupportedPolicy::Drop),
            (
                serde_json::json!({"unsupported": "reject"}),
                UnsupportedPolicy::Reject,
            ),
            (
                serde_json::json!({"unsupported": "drop"}),
                UnsupportedPolicy::Drop,
            ),
        ] {
            let cfg: LakeConfig = serde_json::from_value(doc.clone()).expect("parses");
            assert_eq!(cfg.unsupported, expected, "{doc}");
        }
    }

    /// Scenario: `logs.exemplars` written at all.
    /// Guarantees: it is refused at startup naming the setting.
    #[test]
    fn logs_exemplars_is_refused() {
        let mut cfg = LakeConfig::default();
        cfg.logs.exemplars = Some(ExemplarPolicy::Drop);
        let err = cfg.validate().expect_err("logs carry no exemplars");
        assert!(err.to_string().contains("logs.exemplars"), "{err}");
    }

    /// Scenario: the exemplar policy in a configuration document.
    /// Guarantees: `drop` and `reject` parse, omitted is unset, anything else is refused.
    #[test]
    fn metrics_exemplars_parses_from_the_document() {
        let cfg: LakeConfig = serde_json::from_value(serde_json::json!({
            "unsupported": "drop",
            "metrics": {"exemplars": "reject"}
        }))
        .expect("parses");
        assert_eq!(cfg.metrics.exemplars, Some(ExemplarPolicy::Reject));
        assert_eq!(LakeConfig::default().metrics.exemplars, None);
        assert!(
            serde_json::from_value::<LakeConfig>(serde_json::json!({
                "metrics": {"exemplars": "keep"}
            }))
            .is_err()
        );
    }

    /// Scenario: the default configuration.
    /// Guarantees: `validate` accepts it.
    #[test]
    fn default_config_validates() {
        assert!(LakeConfig::default().validate().is_ok());
    }

    /// Scenario: `writer_id` is the empty string.
    /// Guarantees: `validate` refuses it.
    #[test]
    fn writer_id_empty_is_rejected() {
        let cfg = LakeConfig {
            writer_id: String::new(),
            ..LakeConfig::default()
        };
        let err = cfg.validate().expect_err("writer_id is empty");
        assert!(err.to_string().contains("writer_id"));
    }

    /// Scenario: `writer_id` contains a `/`.
    /// Guarantees: `validate` refuses it.
    #[test]
    fn writer_id_with_slash_is_rejected() {
        let cfg = LakeConfig {
            writer_id: "team/writer".into(),
            ..LakeConfig::default()
        };
        let err = cfg.validate().expect_err("writer_id contains '/'");
        assert!(err.to_string().contains("writer_id"));
    }

    /// Scenario: `writer_id` with a hyphen, a space, a non-ASCII letter or a slash, then an allowed
    /// one.
    /// Guarantees: everything outside `[A-Za-z0-9_.]` is refused and the allowed class is accepted.
    #[test]
    fn writer_id_outside_its_character_class_is_rejected() {
        for bad in ["local-1", "a b", "w\u{e9}", "team/writer"] {
            let cfg = LakeConfig {
                writer_id: bad.into(),
                ..LakeConfig::default()
            };
            let err = cfg.validate().expect_err(bad);
            assert!(err.to_string().contains("[A-Za-z0-9_.]"), "{bad}: {err}");
        }
        let cfg = LakeConfig {
            writer_id: "Local_1.eu".into(),
            ..LakeConfig::default()
        };
        cfg.validate().expect("the allowed class");
    }

    /// Scenario: `max_nesting_depth` just above and exactly at 256.
    /// Guarantees: the first is refused and the second accepted.
    #[test]
    fn max_nesting_depth_is_capped() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_nesting_depth = 257;
        let err = cfg.validate().expect_err("above the cap");
        assert!(err.to_string().contains("max_nesting_depth"), "{err}");
        cfg.ingress.max_nesting_depth = 256;
        cfg.validate().expect("at the cap");
    }

    /// Scenario: a denormalized column named like a Hive partition key, in any case.
    /// Guarantees: it is refused.
    #[test]
    fn a_denormalized_column_named_like_a_partition_key_is_rejected() {
        for column in ["v", "signal", "dataset", "date", "Hour"] {
            let mut cfg = LakeConfig::default();
            cfg.metrics.denormalize = vec![Denormalize {
                path: "resource.x".into(),
                column: column.into(),
                ty: DenormType::String,
            }];
            let err = cfg.validate().expect_err(column);
            assert!(
                rule(&err).contains(&format!(
                    "metrics.denormalize[0].column {column:?} is named like a partition key"
                )),
                "{column}: {err}"
            );
        }
    }

    /// Scenario: `metrics.series_attributes` is set.
    /// Guarantees: it is refused, since metric identity already includes every point attribute.
    #[test]
    fn metrics_series_attributes_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.metrics.series_attributes = vec!["k8s.pod.name".into()];
        let err = cfg.validate().expect_err("not a metrics setting");
        assert!(
            err.to_string().contains("metrics.series_attributes"),
            "{err}"
        );
    }

    /// Scenario: a denormalized column given as a bare string.
    /// Guarantees: the path is kept, the column defaults to its last segment and the type to
    /// string.
    #[test]
    fn denormalize_deserializes_from_bare_string() {
        let d: Denormalize = serde_json::from_str("\"resource.service.name\"").expect("valid json");
        assert_eq!(d.path, "resource.service.name");
        assert_eq!(d.column, "service_name");
        assert_eq!(d.ty, DenormType::String);
    }

    /// Scenario: a denormalized column given as the full object form.
    /// Guarantees: `path`, `column` and `type` are all taken from the object.
    #[test]
    fn denormalize_deserializes_from_object_form() {
        let d: Denormalize = serde_json::from_str(
            r#"{"path": "attrs.http.status_code", "column": "http_status", "type": "int64"}"#,
        )
        .expect("valid json");
        assert_eq!(d.path, "attrs.http.status_code");
        assert_eq!(d.column, "http_status");
        assert_eq!(d.ty, DenormType::Int64);
    }

    /// Scenario: a sort key given as a bare string.
    /// Guarantees: it means ascending order with nulls last.
    #[test]
    fn sort_key_deserializes_from_bare_string() {
        let k: SortKey = serde_json::from_str("\"my_col\"").expect("valid json");
        assert_eq!(k.column, "my_col");
        assert_eq!(k.order, SortOrder::Asc);
        assert_eq!(k.nulls, Nulls::Last);
    }

    /// Scenario: a sort key given as the full object form with `desc`/`first`.
    /// Guarantees: both are read from the object.
    #[test]
    fn sort_key_deserializes_from_object_form() {
        let k: SortKey =
            serde_json::from_str(r#"{"column": "my_col", "order": "desc", "nulls": "first"}"#)
                .expect("valid json");
        assert_eq!(k.column, "my_col");
        assert_eq!(k.order, SortOrder::Desc);
        assert_eq!(k.nulls, Nulls::First);
    }

    /// Scenario: `window_interval` and `upload.abort_timeout` as humantime strings.
    /// Guarantees: both parse to the given durations and unset fields keep their defaults.
    #[test]
    fn lake_config_deserializes_humantime_durations() {
        let cfg: LakeConfig = serde_json::from_str(
            r#"{"window_interval": "30s", "upload": {"abort_timeout": "10s"}}"#,
        )
        .expect("valid json");
        assert_eq!(cfg.window_interval, Duration::from_secs(30));
        assert_eq!(cfg.upload.abort_timeout, Duration::from_secs(10));
        assert_eq!(cfg.upload.part_bytes, UploadConfig::default().part_bytes);
    }

    /// Scenario: byte settings with units, one plain number and one unreadable string.
    /// Guarantees: units and numbers parse exactly, unset fields keep defaults, and the unreadable
    /// size is refused.
    #[test]
    fn byte_valued_settings_accept_units() {
        let cfg: LakeConfig = serde_json::from_str(
            r#"{
                "ingress": {"max_request_bytes": "8MiB", "max_extracted_bytes": 1024,
                            "max_row_bytes": "1 KiB", "max_block_bytes": "1GiB",
                            "pending_series_entry_bytes": "64B"},
                "sorting": {"run_target_bytes": "4MiB", "merge_chunk_bytes": "2MiB"},
                "upload": {"part_bytes": "5MiB"},
                "parquet": {"row_group_bytes": "32MiB", "writer_limit_bytes": "48MiB"}
            }"#,
        )
        .expect("valid json");
        assert_eq!(cfg.ingress.max_request_bytes, 8 << 20);
        assert_eq!(cfg.ingress.max_extracted_bytes, 1024);
        assert_eq!(cfg.ingress.max_row_bytes, 1024);
        assert_eq!(cfg.ingress.max_block_bytes, 1 << 30);
        assert_eq!(cfg.ingress.pending_series_entry_bytes, 64);
        assert_eq!(cfg.sorting.run_target_bytes, 4 << 20);
        assert_eq!(cfg.sorting.merge_chunk_bytes, 2 << 20);
        assert_eq!(cfg.upload.part_bytes, 5 << 20);
        assert_eq!(cfg.parquet.row_group_bytes, 32 << 20);
        assert_eq!(cfg.parquet.writer_limit_bytes, 48 << 20);
        assert_eq!(
            cfg.ingress.max_nesting_depth,
            IngressLimits::default().max_nesting_depth
        );
        assert!(
            serde_json::from_str::<LakeConfig>(r#"{"upload": {"part_bytes": "lots"}}"#).is_err()
        );
    }
}
