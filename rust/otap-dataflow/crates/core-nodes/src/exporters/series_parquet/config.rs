// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! User configuration mapped onto the engine-independent lake configuration.
//!
//! The user-facing shape is deliberately flatter than
//! [`otel_arrow_dfe_series_lake::config::LakeConfig`]: block budgets live under
//! `window` next to the interval that governs them, and byte-valued settings
//! accept the human byte units (`64MiB`) the rest of the engine accepts.
//! [`RawConfig`] is the literal user document; [`Config`] is the validated
//! result, and every constraint the core crate enforces is checked here so a
//! bad pipeline is refused at startup rather than at the first request.

use otel_arrow_dfe_otap::object_store::{RetryOptions, StorageType};
use otel_arrow_dfe_series_lake::config::{LakeConfig, SignalConfig, UnsupportedPolicy};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::time::Duration;

/// Window rotation interval and the budgets of one block.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Window {
    /// Aligned rotation interval; whole seconds only.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Retained-bytes limit of one block.
    #[serde(deserialize_with = "byte_size")]
    pub max_block_bytes: usize,
    /// Ack tokens one block may hold.
    pub max_requests_per_block: usize,
    /// Upper bound on retrying a flush before the block is failed.
    #[serde(with = "humantime_serde")]
    pub flush_retry_deadline: Duration,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            max_block_bytes: 500 << 20,
            max_requests_per_block: 4096,
            flush_retry_deadline: Duration::from_secs(60),
        }
    }
}

/// Deserialize a byte size written either as a number or as `64MiB`.
fn byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    let n = otel_arrow_dfe_config::byte_units::deserialize_u64(d)?
        .ok_or_else(|| serde::de::Error::custom("byte size cannot be null"))?;
    usize::try_from(n).map_err(serde::de::Error::custom)
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Cache {
    max_entries: usize,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            max_entries: 200_000,
        }
    }
}

/// The user document exactly as written, before cross-field validation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    storage: StorageType,
    #[serde(default)]
    retry: Option<RetryOptions>,
    #[serde(default = "writer")]
    writer_id: String,
    #[serde(default = "producer")]
    producer_id_attribute: String,
    #[serde(default)]
    window: Window,
    // The budget sections below stay as raw JSON until their byte-unit strings
    // have been rewritten into numbers, because the core crate's own structs
    // deserialize plain integers.
    #[serde(default = "object")]
    ingress: Value,
    #[serde(default)]
    series_cache: Cache,
    #[serde(default = "object")]
    sorting: Value,
    #[serde(default = "object")]
    upload: Value,
    #[serde(default = "object")]
    parquet: Value,
    #[serde(default = "batch")]
    notify_batch: usize,
    #[serde(default)]
    unsupported: UnsupportedPolicy,
    #[serde(default)]
    logs: SignalConfig,
    #[serde(default)]
    metrics: SignalConfig,
}

fn writer() -> String {
    "writer".into()
}

fn producer() -> String {
    "host.id".into()
}

fn batch() -> usize {
    64
}

fn object() -> Value {
    Value::Object(serde_json::Map::new())
}

/// Rewrite every `*_bytes` field of a budget section from a byte-unit string
/// into a plain number, so the core crate's structs can deserialize it.
fn normalized(mut value: Value) -> Result<Value, String> {
    let fields = value
        .as_object_mut()
        .ok_or("budget section must be an object")?;
    for (name, field) in fields {
        if name.ends_with("_bytes") {
            let n = byte_size(field.clone()).map_err(|e| e.to_string())?;
            *field = Value::from(n);
        }
    }
    Ok(value)
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(normalized(value)?).map_err(|e| e.to_string())
}

/// Validated exporter configuration. Storage and scheduling stay outside
/// series-lake, which knows only about the lake format itself.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawConfig")]
pub struct Config {
    pub(super) storage: StorageType,
    pub(super) retry: Option<RetryOptions>,
    pub(super) lake: LakeConfig,
    pub(super) window: Window,
    pub(super) cache_entries: usize,
    /// Completion sends the node loop takes before it yields to its other
    /// branches.
    pub(super) notify_batch: usize,
}

impl TryFrom<RawConfig> for Config {
    type Error = String;

    fn try_from(mut raw: RawConfig) -> Result<Self, String> {
        let interval = raw.window.interval;
        if interval.is_zero()
            || interval.subsec_nanos() != 0
            || interval.as_secs() > i64::MAX as u64
        {
            return Err("window.interval must be positive whole seconds fitting i64".into());
        }
        if raw.notify_batch == 0
            || raw.series_cache.max_entries == 0
            || raw.window.flush_retry_deadline.is_zero()
        {
            return Err("notify_batch, cache entries and flush deadline must be positive".into());
        }
        // `ingress` carries only the four request-level budgets here; the two
        // block-level ones are taken from `window` below, so a user who sets
        // them in both places would otherwise get a silently ignored value.
        for key in raw
            .ingress
            .as_object()
            .ok_or("ingress must be an object")?
            .keys()
        {
            if ![
                "max_request_bytes",
                "max_extracted_bytes",
                "max_row_bytes",
                "max_nesting_depth",
            ]
            .contains(&key.as_str())
            {
                return Err(format!("unknown ingress setting {key}"));
            }
        }
        // The sink always writes zstd. Accept the value that documents it and
        // refuse anything else rather than writing a different codec silently.
        let parquet = raw
            .parquet
            .as_object_mut()
            .ok_or("parquet must be an object")?;
        if let Some(compression) = parquet.remove("compression")
            && compression != Value::String("zstd".into())
        {
            return Err("parquet.compression must be zstd".into());
        }
        let mut lake = LakeConfig {
            writer_id: raw.writer_id,
            producer_id_attribute: raw.producer_id_attribute,
            window_interval: interval,
            ingress: decode(raw.ingress)?,
            sorting: decode(raw.sorting)?,
            upload: decode(raw.upload)?,
            parquet: decode(raw.parquet)?,
            unsupported: raw.unsupported,
            logs: raw.logs,
            metrics: raw.metrics,
        };
        lake.ingress.max_block_bytes = raw.window.max_block_bytes;
        lake.ingress.max_requests_per_block = raw.window.max_requests_per_block;
        if [
            lake.ingress.max_request_bytes,
            lake.ingress.max_extracted_bytes,
            lake.ingress.max_row_bytes,
            lake.ingress.max_nesting_depth,
            lake.sorting.run_target_bytes,
            lake.sorting.merge_chunk_bytes,
            lake.parquet.row_group_bytes,
            lake.parquet.writer_limit_bytes,
        ]
        .contains(&0)
            || lake.upload.abort_timeout.is_zero()
        {
            return Err("all byte, depth and abort budgets must be positive".into());
        }
        // The node sizes its notification buffer from this count; refuse a
        // value that cannot be doubled rather than overflowing there.
        let _ = lake
            .ingress
            .max_requests_per_block
            .checked_mul(2)
            .ok_or("request count overflows notification capacity")?;
        lake.validate().map_err(|e| e.to_string())?;
        if let Some(retry) = &raw.retry {
            retry.validate().map_err(|e| e.to_string())?;
        }
        Ok(Self {
            storage: raw.storage,
            retry: raw.retry,
            lake,
            window: raw.window,
            cache_entries: raw.series_cache.max_entries,
            notify_batch: raw.notify_batch,
        })
    }
}
