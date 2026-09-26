// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics specific to the Parquet exporter IO lifecycle.

use otel_arrow_dfe_telemetry::instrument::Counter;
use otel_arrow_dfe_telemetry_macros::metric_set;

/// Parquet exporter IO metrics.
/// Grouped under `otap.exporter.parquet`.
#[metric_set(name = "otap.exporter.parquet")]
#[derive(Debug, Default, Clone)]
pub struct ParquetExporterMetrics {
    /// Number of Parquet files created (across all payload types and partitions).
    #[metric(unit = "{file}")]
    pub files_created: Counter<u64>,

    /// Number of Parquet files successfully closed (flushed and visible to readers).
    #[metric(unit = "{file}")]
    pub files_closed: Counter<u64>,

    /// Total number of rows written into Parquet writers (appended, not necessarily flushed yet).
    #[metric(unit = "{row}")]
    pub rows_written: Counter<u64>,

    /// Files scheduled for flush due to reaching target rows per file.
    #[metric(unit = "{file}")]
    pub flush_scheduled_max_rows: Counter<u64>,

    /// Files scheduled for flush due to exceeding max age threshold.
    #[metric(unit = "{file}")]
    pub flush_scheduled_max_age: Counter<u64>,

    /// File close/flush attempts initiated by the exporter.
    #[metric(unit = "{file}")]
    pub flush_attempts: Counter<u64>,

    /// File close/flush attempts that succeeded and made the file visible to readers.
    #[metric(unit = "{file}")]
    pub flush_successes: Counter<u64>,

    /// File close/flush attempts that failed after the lower-level retry policy was exhausted.
    #[metric(unit = "{file}")]
    pub flush_failures: Counter<u64>,

    /// OTLP requests dropped because their body's protobuf framing is broken.
    #[metric(unit = "{message}")]
    pub malformed_bodies: Counter<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_engine::context::ControllerContext;
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

    /// Scenario: one OTLP request is dropped for a malformed body and the IO metrics are
    /// snapshotted.
    /// Guarantees: the drop is counted as `otap.exporter.parquet.malformed.bodies`, apart from
    /// the other failures.
    #[test]
    fn malformed_bodies_have_their_own_counter() {
        let controller = ControllerContext::new(TelemetryRegistryHandle::new());
        let pipeline = controller.pipeline_context_with("grp".into(), "pipeline".into(), 0, 1, 0);
        let mut metrics = ParquetExporterMetrics::register(&pipeline);
        metrics.malformed_bodies.inc();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.descriptor().name, "otap.exporter.parquet");
        let at = snapshot
            .descriptor()
            .metrics
            .iter()
            .position(|metric| metric.name == "malformed.bodies")
            .expect("a malformed.bodies metric");
        assert_eq!(snapshot.get_metrics()[at].to_u64_lossy(), 1);
    }
}
