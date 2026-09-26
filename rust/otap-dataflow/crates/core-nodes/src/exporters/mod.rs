// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// Noop exporter.
pub mod noop_exporter;

/// Console exporter.
pub mod console_exporter;

/// OTLP JSON file exporter.
#[cfg(feature = "file")]
pub mod file_exporter;

/// Topic exporter.
#[cfg(feature = "topic")]
pub mod topic_exporter;

/// Parquet exporter.
#[cfg(feature = "parquet")]
pub mod parquet_exporter;

/// OTAP exporter.
#[cfg(feature = "otap")]
pub mod otap_exporter;

/// Refusal of OTLP bodies whose framing is broken.
#[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
mod otlp_framing;

/// Rate limit of the per-request refusal WARN lines.
#[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
mod log_gate;

/// OTLP gRPC exporter.
#[cfg(feature = "otlp")]
pub mod otlp_grpc_exporter;

/// OTLP HTTP exporter.
#[cfg(feature = "otlp")]
pub mod otlp_http_exporter;
