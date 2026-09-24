// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! User configuration mapped onto the engine-independent lake configuration.
//!
//! The user shape is flatter than
//! [`otel_arrow_dfe_series_lake::config::LakeConfig`]: block budgets live under
//! `window`, and byte sizes accept units such as `64MiB`. [`Config`] is the
//! validated [`RawConfig`]; every lake constraint is checked here at startup,
//! and each refusal names the dotted key a user writes.

use otel_arrow_dfe_config::byte_units::deserialize_required_usize;
use otel_arrow_dfe_otap::object_store::{RetryOptions, StorageType};
use otel_arrow_dfe_series_lake::config::{
    IngressLimits, LakeConfig, ParquetConfig, SignalConfig, SortingConfig, UnsupportedPolicy,
    UploadConfig,
};
use serde::{Deserialize, Deserializer};
use std::time::Duration;

/// Window rotation interval and the budgets of one block.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Window {
    /// Aligned rotation interval; whole seconds only.
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    /// Retained-bytes limit of one block.
    #[serde(deserialize_with = "deserialize_required_usize")]
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

/// The request-level budgets a user sets under `ingress`.
///
/// Only four of the lake's ingress limits: the two block-level ones are set
/// under `window`, so writing them here is an unknown field.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Ingress {
    #[serde(deserialize_with = "deserialize_required_usize")]
    max_request_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    max_extracted_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    max_row_bytes: usize,
    max_nesting_depth: usize,
}

impl Default for Ingress {
    fn default() -> Self {
        let lake = IngressLimits::default();
        Self {
            max_request_bytes: lake.max_request_bytes,
            max_extracted_bytes: lake.max_extracted_bytes,
            max_row_bytes: lake.max_row_bytes,
            max_nesting_depth: lake.max_nesting_depth,
        }
    }
}

/// The Parquet writer settings a user sets under `parquet`.
///
/// `compression` documents the one codec the sink writes; any other value is
/// refused.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Parquet {
    compression: Option<String>,
    #[serde(deserialize_with = "deserialize_required_usize")]
    row_group_bytes: usize,
    #[serde(deserialize_with = "deserialize_required_usize")]
    writer_limit_bytes: usize,
}

impl Default for Parquet {
    fn default() -> Self {
        let lake = ParquetConfig::default();
        Self {
            compression: None,
            row_group_bytes: lake.row_group_bytes,
            writer_limit_bytes: lake.writer_limit_bytes,
        }
    }
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

/// Define a `deserialize_with` function that reads one section and prefixes
/// its error with the section's key, so a misspelled or malformed setting is
/// reported as `ingress: unknown field ...`.
macro_rules! section {
    ($name:ident, $ty:ty, $key:literal) => {
        fn $name<'de, D: Deserializer<'de>>(d: D) -> Result<$ty, D::Error> {
            <$ty>::deserialize(d)
                .map_err(|e| serde::de::Error::custom(format!(concat!($key, ": {}"), e)))
        }
    };
}

section!(window_section, Window, "window");
section!(ingress_section, Ingress, "ingress");
section!(cache_section, Cache, "series_cache");
section!(sorting_section, SortingConfig, "sorting");
section!(upload_section, UploadConfig, "upload");
section!(parquet_section, Parquet, "parquet");
section!(logs_section, SignalConfig, "logs");
section!(metrics_section, SignalConfig, "metrics");

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
    #[serde(default, deserialize_with = "window_section")]
    window: Window,
    #[serde(default, deserialize_with = "ingress_section")]
    ingress: Ingress,
    #[serde(default, deserialize_with = "cache_section")]
    series_cache: Cache,
    #[serde(default, deserialize_with = "sorting_section")]
    sorting: SortingConfig,
    #[serde(default, deserialize_with = "upload_section")]
    upload: UploadConfig,
    #[serde(default, deserialize_with = "parquet_section")]
    parquet: Parquet,
    #[serde(default = "batch")]
    notify_batch: usize,
    #[serde(default)]
    unsupported: UnsupportedPolicy,
    #[serde(default, deserialize_with = "logs_section")]
    logs: SignalConfig,
    #[serde(default, deserialize_with = "metrics_section")]
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

/// Shortest accepted `upload.abort_timeout`: the shutdown drain decides every
/// held request synchronously and then waits for cleanup only until the
/// latched deadline plus this timeout, so the timeout must leave room for that
/// decision (README.md, "The drain").
const MIN_ABORT_TIMEOUT: Duration = Duration::from_secs(1);

/// Refuse an explicit store retry budget that one write attempt could spend
/// past the block's own flush deadline.
///
/// A cloud store retries each request internally for up to
/// `retry.retry_timeout`. If that is not strictly shorter than
/// `window.flush_retry_deadline`, a single attempt against a destination that
/// keeps failing is still retrying inside the store when the block's deadline
/// expires, and the flush ends with no underlying error to report. Without a
/// `retry` section the budget is derived instead (see [`derived_retry`]).
/// Local file storage applies no store retry, so `retried` is false for it and
/// nothing is checked.
pub(super) fn check_retry_deadline(
    retried: bool,
    retry: Option<&RetryOptions>,
    flush_retry_deadline: Duration,
) -> Result<(), String> {
    let Some(retry) = retry.filter(|_| retried) else {
        return Ok(());
    };
    let timeout = retry.retry_timeout;
    if timeout < flush_retry_deadline {
        return Ok(());
    }
    Err(format!(
        "retry.retry_timeout ({timeout:?}) must be strictly less than \
         window.flush_retry_deadline ({flush_retry_deadline:?}), or one write attempt keeps \
         retrying inside the store past the block deadline; set retry.retry_timeout below \
         the deadline, raise window.flush_retry_deadline, or omit the retry section"
    ))
}

/// The store retry options of a cloud store configured without a `retry`
/// section: object_store's defaults, with `retry_timeout` half of
/// `window.flush_retry_deadline`, which leaves the block time for a retry of
/// its own after the store gives up on one attempt.
fn derived_retry(flush_retry_deadline: Duration) -> Result<RetryOptions, String> {
    let mut retry: RetryOptions =
        serde_json::from_value(serde_json::Value::Object(serde_json::Map::new()))
            .map_err(|e| format!("retry: {e}"))?;
    retry.retry_timeout = flush_retry_deadline / 2;
    Ok(retry)
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

    fn try_from(raw: RawConfig) -> Result<Self, String> {
        let interval = raw.window.interval;
        if interval.is_zero()
            || interval.subsec_nanos() != 0
            || interval.as_secs() > i64::MAX as u64
        {
            return Err("window.interval must be positive whole seconds fitting i64".into());
        }
        if raw.window.flush_retry_deadline.is_zero() {
            return Err("window.flush_retry_deadline must be positive".into());
        }
        if raw.window.max_requests_per_block == 0 {
            return Err("window.max_requests_per_block must be at least 1".into());
        }
        // The node sizes its notification buffer from twice this count.
        if raw.window.max_requests_per_block.checked_mul(2).is_none() {
            return Err("window.max_requests_per_block overflows the notification capacity".into());
        }
        if raw.notify_batch == 0 {
            return Err("notify_batch must be positive".into());
        }
        if raw.series_cache.max_entries == 0 {
            return Err("series_cache.max_entries must be positive".into());
        }
        if let Some(compression) = &raw.parquet.compression
            && compression != "zstd"
        {
            // The sink always writes zstd.
            return Err(format!(
                "parquet.compression must be zstd (the only codec written), not {compression:?}"
            ));
        }
        for (key, value) in [
            ("ingress.max_request_bytes", raw.ingress.max_request_bytes),
            (
                "ingress.max_extracted_bytes",
                raw.ingress.max_extracted_bytes,
            ),
            ("ingress.max_row_bytes", raw.ingress.max_row_bytes),
            ("ingress.max_nesting_depth", raw.ingress.max_nesting_depth),
            ("sorting.run_target_bytes", raw.sorting.run_target_bytes),
            ("sorting.merge_chunk_bytes", raw.sorting.merge_chunk_bytes),
            ("parquet.row_group_bytes", raw.parquet.row_group_bytes),
            ("parquet.writer_limit_bytes", raw.parquet.writer_limit_bytes),
        ] {
            if value == 0 {
                return Err(format!("{key} must be positive"));
            }
        }
        if raw.upload.abort_timeout < MIN_ABORT_TIMEOUT {
            return Err(format!(
                "upload.abort_timeout ({:?}) must be at least {MIN_ABORT_TIMEOUT:?}",
                raw.upload.abort_timeout
            ));
        }
        // The one cross-field rule whose lake form names a lake key
        // (`ingress.max_block_bytes`): checked here first with the key the
        // user writes. Every other cross-field rule is the lake's own, and its
        // message already names the user's key.
        if raw.window.max_block_bytes / 2 < raw.ingress.max_extracted_bytes {
            return Err(
                "window.max_block_bytes must be at least twice ingress.max_extracted_bytes, \
                 because a request's series rows may take up to twice their extracted size \
                 in a block"
                    .into(),
            );
        }
        let lake = LakeConfig {
            writer_id: raw.writer_id,
            producer_id_attribute: raw.producer_id_attribute,
            window_interval: interval,
            ingress: IngressLimits {
                max_request_bytes: raw.ingress.max_request_bytes,
                max_extracted_bytes: raw.ingress.max_extracted_bytes,
                max_row_bytes: raw.ingress.max_row_bytes,
                max_nesting_depth: raw.ingress.max_nesting_depth,
                max_block_bytes: raw.window.max_block_bytes,
                max_requests_per_block: raw.window.max_requests_per_block,
                ..IngressLimits::default()
            },
            sorting: raw.sorting,
            upload: raw.upload,
            parquet: ParquetConfig {
                row_group_bytes: raw.parquet.row_group_bytes,
                writer_limit_bytes: raw.parquet.writer_limit_bytes,
            },
            unsupported: raw.unsupported,
            logs: raw.logs,
            metrics: raw.metrics,
        };
        // A configuration rule's own sentence, without the request refusal
        // wrapper the lake error type carries.
        lake.validate().map_err(|e| {
            e.invalid_detail()
                .map_or_else(|| e.to_string(), str::to_owned)
        })?;
        let retried = !matches!(raw.storage, StorageType::File { .. });
        check_retry_deadline(retried, raw.retry.as_ref(), raw.window.flush_retry_deadline)?;
        let retry = match raw.retry {
            None if retried => Some(derived_retry(raw.window.flush_retry_deadline)?),
            retry => retry,
        };
        Ok(Self {
            // An unset `unsigned_payload` is on over TLS for this exporter only.
            storage: raw.storage.with_unsigned_payload_over_tls(),
            retry,
            lake,
            window: raw.window,
            cache_entries: raw.series_cache.max_entries,
            notify_batch: raw.notify_batch,
        })
    }
}
