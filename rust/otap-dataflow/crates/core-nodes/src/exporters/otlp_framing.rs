// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The refusal of an OTLP request whose protobuf framing is broken, shared by
//! the file, otap and parquet exporters, which check it with
//! [`OtapPayload::validate_otlp_framing`](otel_arrow_dfe_pdata::OtapPayload::validate_otlp_framing)
//! under [`RepeatedSingular::Accept`](otel_arrow_dfe_pdata::views::otlp::bytes::validate::RepeatedSingular::Accept).

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_pdata::error::Error;
use otel_arrow_dfe_telemetry::otel_warn;
use std::time::{Duration, Instant};

/// Shortest interval between two `otlp.malformed_body` lines.
const LOG_INTERVAL: Duration = Duration::from_secs(1);

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

/// The `otlp.malformed_body` WARN, at most one line per [`LOG_INTERVAL`],
/// each naming how many refusals it left out since the previous one.
#[derive(Debug)]
pub(crate) struct MalformedBodyLog {
    /// When the last line was written.
    last: Option<Instant>,
    /// Refusals left out since then.
    suppressed: u64,
}

impl MalformedBodyLog {
    /// A log that has written no line yet.
    pub(crate) const fn new() -> Self {
        Self {
            last: None,
            suppressed: 0,
        }
    }

    /// Report one refused body.
    pub(crate) fn record(&mut self, signal: SignalType, error: &Error) {
        if let Some(suppressed) = self.admit(Instant::now()) {
            otel_warn!(
                "otlp.malformed_body",
                signal = ?signal,
                error = %error,
                suppressed = suppressed
            );
        }
    }

    /// Whether a refusal seen at `now` is logged, and if it is, how many were
    /// left out since the previous line.
    fn admit(&mut self, now: Instant) -> Option<u64> {
        if self
            .last
            .is_some_and(|last| now.saturating_duration_since(last) < LOG_INTERVAL)
        {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: refusals at 0 ms, 10 ms, 500 ms, one interval after the first
    /// line, and 10 ms after that.
    /// Guarantees: the first refusal and the first one an interval after the
    /// last line are logged, the latter naming the two left out between them,
    /// and every other one is suppressed and counted.
    #[test]
    fn one_line_per_interval_names_the_suppressed_refusals() {
        let start = Instant::now();
        let mut log = MalformedBodyLog::new();
        assert_eq!(log.admit(start), Some(0));
        assert_eq!(log.admit(start + Duration::from_millis(10)), None);
        assert_eq!(log.admit(start + Duration::from_millis(500)), None);
        assert_eq!(log.admit(start + LOG_INTERVAL), Some(2));
        assert_eq!(
            log.admit(start + LOG_INTERVAL + Duration::from_millis(10)),
            None
        );
    }
}
