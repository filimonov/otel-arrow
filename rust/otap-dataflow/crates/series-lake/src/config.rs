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
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            series_attributes: vec![],
            denormalize: vec![],
            values_sort: default_values_sort(),
        }
    }
}

/// Request budgets (spec section 6.2).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngressLimits {
    /// Logical input size limit.
    pub max_request_bytes: usize,
    /// Extracted output limit, enforced on the *measured* extracted output.
    ///
    /// Values rows are charged as estimates only while their run is being
    /// built; sealing a run replaces that estimate with the measured pinned
    /// bytes of the Arrow batch, so a row is never counted twice. Descriptor
    /// rows, which are not sealed into Arrow batches during extraction, stay
    /// charged at their estimated size.
    pub max_extracted_bytes: usize,
    /// Single row limit.
    pub max_row_bytes: usize,
    /// Nested value depth limit.
    pub max_nesting_depth: usize,
    /// Retained-bytes limit of one block (spec section 6.1).
    pub max_block_bytes: usize,
    /// Ack tokens (requests) one block may hold (spec section 6.1).
    pub max_requests_per_block: usize,
    /// Fixed bytes charged per `pending_series` entry (spec section 6.1).
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
            pending_series_entry_bytes: 64,
        }
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
    pub part_bytes: usize,
    /// In-flight parts.
    pub concurrency: usize,
    /// Upper bound on a best-effort multipart abort (spec section 6.5).
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
    pub row_group_bytes: usize,
    /// Enforced writer memory threshold.
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
    ///
    /// Rules enforced: `writer_id` is non-empty and contains no `/` (it is
    /// interpolated into the file-name segment of [`crate::sink::object_path`],
    /// and a delimiter there must not be read as extra path segments; the
    /// boot id is generated internally from a UUIDv4 and is not
    /// user-configurable, so it needs no such rule); `max_row_bytes <=
    /// run_target_bytes / 4`; `max_requests_per_block >= 1`; `max_block_bytes
    /// >= max_extracted_bytes`; `upload.part_bytes >= 5 MiB` (the S3
    /// multipart minimum part size, below which every upload would fail at
    /// flush time); `upload.concurrency >= 1` (otherwise no part could ever
    /// be sent); denormalized and intrinsic column names do not collide
    /// (case-insensitively) within a dataset; each signal's `values_sort`
    /// columns exist in that signal's values dataset schema; and every
    /// `denormalize` path has a valid `resource.`/`scope.`/`attrs.` prefix.
    pub fn validate(&self) -> Result<()> {
        if self.writer_id.is_empty() {
            return Err(Error::invalid("writer_id must not be empty"));
        }
        if self.writer_id.contains('/') {
            return Err(Error::invalid("writer_id must not contain '/'"));
        }
        if self.ingress.max_row_bytes > self.sorting.run_target_bytes / 4 {
            return Err(Error::invalid(
                "max_row_bytes must be at most run_target_bytes / 4",
            ));
        }
        if self.upload.part_bytes < 5 << 20 {
            return Err(Error::invalid(
                "upload.part_bytes must be at least 5MiB (S3 multipart minimum)",
            ));
        }
        if self.upload.concurrency == 0 {
            return Err(Error::invalid("upload.concurrency must be at least 1"));
        }
        if self.ingress.max_requests_per_block == 0 {
            return Err(Error::invalid("max_requests_per_block must be >= 1"));
        }
        if self.ingress.max_block_bytes < self.ingress.max_extracted_bytes {
            return Err(Error::invalid(
                "max_block_bytes must be at least max_extracted_bytes",
            ));
        }
        for ds in crate::schema::Dataset::ALL {
            let schema = crate::schema::dataset_schema(ds, self);
            let mut seen = HashSet::new();
            for f in schema.fields() {
                if !seen.insert(f.name().to_lowercase()) {
                    return Err(Error::invalid(format!(
                        "column name collision: {}",
                        f.name()
                    )));
                }
            }
            if !ds.is_series() {
                let sig = if ds.signal() == crate::canonical::Signal::Logs {
                    &self.logs
                } else {
                    &self.metrics
                };
                for key in &sig.values_sort {
                    if schema.column_with_name(&key.column).is_none() {
                        return Err(Error::invalid(format!(
                            "sort key {} not in {}",
                            key.column,
                            ds.name()
                        )));
                    }
                }
            }
        }
        for d in self
            .logs
            .denormalize
            .iter()
            .chain(self.metrics.denormalize.iter())
        {
            let _ = d.source()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// Scenario: `max_block_bytes` is smaller than `max_extracted_bytes`.
    /// Guarantees: `validate` refuses, since a block could never hold one request's output.
    #[test]
    fn max_block_bytes_below_extracted_is_rejected() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_block_bytes = cfg.ingress.max_extracted_bytes - 1;
        let err = cfg
            .validate()
            .expect_err("max_block_bytes < max_extracted_bytes");
        assert!(err.to_string().contains("max_block_bytes"));
    }

    /// Scenario: `upload.part_bytes` is below the S3 multipart minimum part size.
    /// Guarantees: `validate` refuses at config time rather than deferring to an
    /// upload-time failure.
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

    /// Scenario: the spec-example default configuration.
    /// Guarantees: `validate` accepts it outright.
    #[test]
    fn default_config_validates() {
        assert!(LakeConfig::default().validate().is_ok());
    }

    /// Scenario: `writer_id` is the empty string.
    /// Guarantees: `validate` refuses it, since it is interpolated into every
    /// written file name.
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
    /// Guarantees: `validate` refuses it, since an unescaped `/` would inject
    /// extra path segments into `object_path`'s file-name segment.
    #[test]
    fn writer_id_with_slash_is_rejected() {
        let cfg = LakeConfig {
            writer_id: "team/writer".into(),
            ..LakeConfig::default()
        };
        let err = cfg.validate().expect_err("writer_id contains '/'");
        assert!(err.to_string().contains("writer_id"));
    }

    /// Scenario: a denormalized column given as a bare string.
    /// Guarantees: the path is kept verbatim, the column name defaults to the
    /// path's last segment, and the type defaults to string.
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
    /// Guarantees: both are read from the object rather than defaulted.
    #[test]
    fn sort_key_deserializes_from_object_form() {
        let k: SortKey =
            serde_json::from_str(r#"{"column": "my_col", "order": "desc", "nulls": "first"}"#)
                .expect("valid json");
        assert_eq!(k.column, "my_col");
        assert_eq!(k.order, SortOrder::Desc);
        assert_eq!(k.nulls, Nulls::First);
    }

    /// Scenario: a `LakeConfig` snippet sets `window_interval` and
    /// `upload.abort_timeout` as humantime strings.
    /// Guarantees: both parse through `humantime_serde` to the given
    /// durations, and fields left unset keep their spec defaults.
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
}
