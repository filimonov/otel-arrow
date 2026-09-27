// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The check, refusal and accounting of an OTLP request whose protobuf
//! framing is broken, shared by every exporter that reads OTLP bytes.
//!
//! The check sees only requests that reach the exporter as OTLP bytes; a node
//! upstream that converts to Arrow records converts a damaged body leniently
//! first.

#[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
use super::log_gate::LogGate;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::error::Error;
use otel_arrow_dfe_telemetry::common_attributes::SignalAttributes;
use otel_arrow_dfe_telemetry::instrument::Counter;
use otel_arrow_dfe_telemetry::metrics::{MeasurementMetricSet, MetricSetSnapshot};
#[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
use otel_arrow_dfe_telemetry::otel_warn;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use otel_arrow_dfe_telemetry_macros::metric_set;
#[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
use std::time::Instant;

/// Check an OTLP payload's framing as prost decodes it (see
/// `otel_arrow_dfe_pdata::views::otlp::bytes::validate`); Arrow records pass.
pub(crate) fn check(payload: &OtapPayload) -> Result<(), Error> {
    payload.validate_otlp_framing()
}

/// The permanent nack of a request whose body failed the framing check; the
/// producer must change the bytes, so the cause is `Refused`.
#[cfg(any(feature = "file", feature = "otap"))]
pub(crate) fn refusal(
    error: &Error,
    pdata: otel_arrow_dfe_otap::pdata::OtapPdata,
) -> otel_arrow_dfe_engine::control::NackMsg<otel_arrow_dfe_otap::pdata::OtapPdata> {
    use otel_arrow_dfe_engine::control::{NackCause, NackMsg};
    NackMsg::new_permanent_with_cause(
        format!("malformed OTLP request body: {error}"),
        pdata,
        NackCause::Refused,
    )
}

/// OTLP requests an exporter refused because their body's protobuf framing
/// is broken, per signal; every exporter that checks framing registers it.
#[metric_set(
    name = "exporter.malformed_bodies",
    measurement_attributes = SignalAttributes
)]
#[derive(Debug, Default, Clone)]
pub(crate) struct MalformedBodyMetrics {
    /// OTLP requests refused because their body's protobuf framing is broken.
    #[metric(unit = "{message}")]
    pub(crate) messages: Counter<u64>,
}

/// The accounting of refused bodies: `exporter.malformed_bodies.messages`
/// and the `otlp.malformed_body` WARN, rate limited by a [`LogGate`].
#[derive(Debug)]
pub(crate) struct MalformedBodies {
    metrics: Option<MeasurementMetricSet<MalformedBodyMetrics>>,
    /// The WARN's rate limit; series_parquet logs refusals its own way.
    #[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
    log: LogGate,
}

impl MalformedBodies {
    /// Accounting whose counter is registered in `pipeline`.
    pub(crate) fn register(pipeline: &PipelineContext) -> Self {
        Self {
            metrics: Some(MalformedBodyMetrics::register(pipeline)),
            #[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
            log: LogGate::new(),
        }
    }

    /// Accounting that only logs, for an exporter built without a pipeline.
    #[cfg(feature = "parquet")]
    pub(crate) const fn unregistered() -> Self {
        Self {
            metrics: None,
            log: LogGate::new(),
        }
    }

    /// Count one refused body of `signal`.
    pub(crate) fn count(&mut self, signal: SignalType) {
        if let Some(metrics) = &mut self.metrics {
            metrics.with(SignalAttributes { signal }).messages.inc();
        }
    }

    /// Count one refused body and log it.
    #[cfg(any(feature = "file", feature = "otap", feature = "parquet"))]
    pub(crate) fn record(&mut self, signal: SignalType, error: &Error) {
        self.count(signal);
        if let Some(suppressed) = self.log.admit(Instant::now()) {
            otel_warn!(
                "otlp.malformed_body",
                signal = ?signal,
                error = %error,
                suppressed = suppressed
            );
        }
    }

    /// Send the counts collected since the last report.
    pub(crate) fn report(&mut self, reporter: &mut MetricsReporter) {
        if let Some(metrics) = &mut self.metrics {
            _ = reporter.report_measurement(metrics);
        }
    }

    /// The final snapshots of the counter.
    pub(crate) fn terminal_snapshots(&mut self) -> Vec<MetricSetSnapshot> {
        self.metrics
            .as_mut()
            .map(MeasurementMetricSet::terminal_snapshots)
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_engine::context::ControllerContext;
    use otel_arrow_dfe_pdata::OtlpProtoBytes;
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;

    /// Scenario: two logs bodies and one metrics body refused, the first with
    /// a broken frame, and the counter handed off.
    /// Guarantees: the check refuses the broken frame, and each refusal is
    /// counted as `exporter.malformed_bodies.messages` under its signal.
    #[test]
    fn refused_bodies_are_counted_per_signal() {
        let broken = OtapPayload::from(OtlpProtoBytes::new_from_bytes(
            SignalType::Logs,
            vec![0x0a, 0x01, 0x0a],
        ));
        let error = check(&broken).expect_err("a tag without a length is refused");
        let registry = TelemetryRegistryHandle::new();
        let pipeline = ControllerContext::new(registry).pipeline_context_with(
            "grp".into(),
            "pipeline".into(),
            0,
            1,
            0,
        );
        assert!(error.to_string().contains("ResourceLogs"), "{error}");
        let mut bodies = MalformedBodies::register(&pipeline);
        bodies.count(SignalType::Logs);
        bodies.count(SignalType::Logs);
        bodies.count(SignalType::Metrics);

        let mut counts = Vec::new();
        for snapshot in bodies.terminal_snapshots() {
            assert_eq!(snapshot.descriptor().name, "exporter.malformed_bodies");
            let at = snapshot
                .descriptor()
                .metrics
                .iter()
                .position(|metric| metric.name == "messages")
                .expect("a messages metric");
            counts.push((
                snapshot
                    .measurement_attribute_value("signal")
                    .map(str::to_owned),
                snapshot.get_metrics()[at].to_u64_lossy(),
            ));
        }
        counts.sort();
        assert_eq!(
            counts,
            vec![(Some("logs".into()), 2), (Some("metrics".into()), 1)]
        );
    }
}
