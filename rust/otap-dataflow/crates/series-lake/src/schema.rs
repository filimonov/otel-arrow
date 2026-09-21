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
    #[must_use]
    pub fn signal(self) -> Signal {
        match self {
            Dataset::LogsSeries | Dataset::LogsValues => Signal::Logs,
            _ => Signal::Metrics,
        }
    }

    /// Hive `dataset=` value.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Dataset::LogsSeries | Dataset::MetricsSeries => "series",
            Dataset::LogsValues => "values",
            Dataset::MetricsNumber => "number",
            Dataset::MetricsHistogram => "histogram",
        }
    }

    /// Whether this is a series dataset.
    #[must_use]
    pub fn is_series(self) -> bool {
        matches!(self, Dataset::LogsSeries | Dataset::MetricsSeries)
    }

    /// Series dataset of the same signal.
    #[must_use]
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
#[must_use]
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
#[must_use]
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
#[must_use]
pub fn dataset_schema(ds: Dataset, cfg: &LakeConfig) -> SchemaRef {
    let mut fields: Vec<Field> = vec![Field::new(
        "series_id",
        DataType::FixedSizeBinary(16),
        false,
    )];
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
                    Field::new(
                        "bucket_counts",
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
                        false,
                    ),
                    Field::new(
                        "explicit_bounds",
                        DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                        false,
                    ),
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
#[must_use]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DenormType, Denormalize, LakeConfig};

    /// Scenario: default config, every dataset.
    /// Guarantees: the intrinsic column lists of spec section 5.1 are produced in order.
    #[test]
    fn intrinsic_columns_match_spec() {
        let cfg = LakeConfig::default();
        let names = |ds: Dataset| -> Vec<String> {
            dataset_schema(ds, &cfg)
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        };
        assert_eq!(
            names(Dataset::LogsValues),
            [
                "series_id",
                "producer_id",
                "time",
                "time_unix_nano",
                "observed_time",
                "observed_time_unix_nano",
                "severity_number",
                "severity_text",
                "body",
                "event_name",
                "trace_id",
                "span_id",
                "flags",
                "attrs"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsNumber),
            [
                "series_id",
                "producer_id",
                "metric_name",
                "time",
                "time_unix_nano",
                "start_time",
                "start_time_unix_nano",
                "flags",
                "value_int",
                "value_double"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsHistogram),
            [
                "series_id",
                "producer_id",
                "metric_name",
                "time",
                "time_unix_nano",
                "start_time",
                "start_time_unix_nano",
                "flags",
                "count",
                "sum",
                "min",
                "max",
                "bucket_counts",
                "explicit_bounds"
            ]
        );
        assert_eq!(
            names(Dataset::LogsSeries),
            [
                "series_id",
                "identity_bytes",
                "emitted_at",
                "resource_schema_url",
                "resource_attrs",
                "scope_name",
                "scope_version",
                "scope_schema_url",
                "scope_attrs",
                "attrs"
            ]
        );
        assert_eq!(
            names(Dataset::MetricsSeries)[10..],
            [
                "metric_name",
                "unit",
                "metric_type",
                "temporality",
                "is_monotonic",
                "description"
            ]
        );
    }

    /// Scenario: a resource-path and an attrs-path denormalized column for logs.
    /// Guarantees: values get both, series gets only the identity (resource) one; fingerprints differ.
    #[test]
    fn denormalized_columns_and_fingerprint() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![
            Denormalize {
                path: "resource.service.name".into(),
                column: "service_name".into(),
                ty: DenormType::String,
            },
            Denormalize {
                path: "attrs.http.status_code".into(),
                column: "http_status_code".into(),
                ty: DenormType::Int64,
            },
        ];
        let values = dataset_schema(Dataset::LogsValues, &cfg);
        assert!(values.column_with_name("service_name").is_some());
        assert!(values.column_with_name("http_status_code").is_some());
        let series = dataset_schema(Dataset::LogsSeries, &cfg);
        assert!(series.column_with_name("service_name").is_some());
        assert!(series.column_with_name("http_status_code").is_none());
        assert_ne!(schema_fingerprint(&values), schema_fingerprint(&series));
        assert_ne!(
            schema_fingerprint(&values),
            schema_fingerprint(&dataset_schema(Dataset::LogsValues, &LakeConfig::default()))
        );
    }

    /// Scenario: two denormalized columns whose names differ only by case.
    /// Guarantees: validation refuses the configuration.
    #[test]
    fn collision_is_a_config_error() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize = vec![
            Denormalize {
                path: "resource.a".into(),
                column: "Col".into(),
                ty: DenormType::String,
            },
            Denormalize {
                path: "resource.b".into(),
                column: "col".into(),
                ty: DenormType::String,
            },
        ];
        assert!(cfg.validate().is_err());
        let mut cfg = LakeConfig::default();
        cfg.logs.values_sort = vec![crate::config::SortKey {
            column: "nope".into(),
            order: crate::config::SortOrder::Asc,
            nulls: crate::config::Nulls::Last,
        }];
        assert!(cfg.validate().is_err());
    }
}
