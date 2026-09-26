// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The check and refusal of an OTLP request whose protobuf framing is broken,
//! shared by the file, otap and parquet exporters.
//!
//! The check sees only requests that reach the exporter as OTLP bytes; a node
//! upstream that converts to Arrow records converts a damaged body leniently
//! first.

use super::log_gate::LogGate;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_pdata::error::Error;
use otel_arrow_dfe_pdata::views::otlp::bytes::validate::RepeatedSingular;
use otel_arrow_dfe_telemetry::otel_warn;
use std::time::Instant;

/// Check an OTLP payload's framing, accepting a repeated singular field as
/// prost does; Arrow records pass.
pub(crate) fn check(payload: &OtapPayload) -> Result<(), Error> {
    payload.validate_otlp_framing(RepeatedSingular::Accept)
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

/// The `otlp.malformed_body` WARN, rate limited by a [`LogGate`].
#[derive(Debug)]
pub(crate) struct MalformedBodyLog(LogGate);

impl MalformedBodyLog {
    /// A log that has written no line yet.
    pub(crate) const fn new() -> Self {
        Self(LogGate::new())
    }

    /// Report one refused body.
    pub(crate) fn record(&mut self, signal: SignalType, error: &Error) {
        if let Some(suppressed) = self.0.admit(Instant::now()) {
            otel_warn!(
                "otlp.malformed_body",
                signal = ?signal,
                error = %error,
                suppressed = suppressed
            );
        }
    }
}
