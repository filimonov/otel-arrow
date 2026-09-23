// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The refusal of an OTLP request whose protobuf framing is broken, shared by
//! the file, otap and parquet exporters, which check it with
//! [`OtapPayload::validate_otlp_framing`](otel_arrow_dfe_pdata::OtapPayload::validate_otlp_framing)
//! under [`RepeatedSingular::Accept`](otel_arrow_dfe_pdata::views::otlp::bytes::validate::RepeatedSingular::Accept).

use super::log_gate::LogGate;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::error::Error;
use otel_arrow_dfe_telemetry::otel_warn;
use std::time::Instant;

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
