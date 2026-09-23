// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! How a request was decided, and everything its sender and the operator are
//! told about it.
//!
//! The lake reports a failure in one of three classes (see [`lake::Error`]);
//! [`Outcome::of`] is the one place a class becomes an outcome. Everything
//! downstream is derived from the outcome: the nack cause and whether the nack
//! is permanent, the `error.type` label of the `nacks` counter and the
//! `outcome` log field, and the reason sentence. Only the sentence also reads
//! the error, for the detail a sender can act on.

use otel_arrow_dfe_engine::control::NackCause;
use otel_arrow_dfe_series_lake as lake;
use otel_arrow_dfe_telemetry::attributes::AttributeEnum;
use otel_arrow_dfe_telemetry_macros::AttributeEnum;

/// How a request was decided, in the form the sender is told about it.
///
/// Each refusal rule is a separate variant rather than one, so the counters
/// keep saying which validation rule rejected a request after the error value
/// itself has been dropped with the payload.
///
/// Also the `error.type` label of the `nacks` counter. `Ack` is never recorded
/// there, and an untouched bucket is never reported. The nack variants are
/// declared in the label order the counter has always reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq, AttributeEnum)]
#[repr(usize)]
pub(super) enum Outcome {
    /// The request is durable.
    Ack,
    /// Writing the request failed; the sender may retry.
    Storage,
    /// The request exceeded `ingress.max_request_bytes`.
    RequestTooLarge,
    /// The extracted request or one decoded attribute table exceeded
    /// `ingress.max_extracted_bytes`.
    ExtractedTooLarge,
    /// One row, attribute value or CBOR cell exceeded `ingress.max_row_bytes`.
    RowTooLarge,
    /// The request's worst case in one block exceeded `window.max_block_bytes`.
    BlockTooLarge,
    /// A nested value exceeded `ingress.max_nesting_depth`.
    TooDeep,
    /// The request's content could not be used.
    Invalid,
    /// The request's signal is not handled by this exporter.
    Unsupported,
    /// The node shut down before the request could be decided.
    Shutdown,
    /// A writer invariant failed while handling the request; the request is
    /// not at fault and the sender may retry.
    Internal,
}

/// Number of [`Outcome`] variants, and so the width of the counter array.
pub(super) const OUTCOMES: usize = <Outcome as AttributeEnum>::CARDINALITY;

/// Longest detail an error may contribute to a nack reason, in bytes.
///
/// The reason is the status message a producer sees and logs, so it carries
/// enough of the underlying error to act on but never an unbounded amount of
/// request-derived text.
const MAX_DETAIL_BYTES: usize = 256;

/// A bounded, printable rendering of an error for a nack reason.
///
/// Control characters, including line breaks, become spaces so the reason
/// stays one line, and the text is cut at a character boundary once it passes
/// [`MAX_DETAIL_BYTES`].
pub(super) fn sanitized(detail: &str) -> String {
    let mut out = String::with_capacity(detail.len().min(MAX_DETAIL_BYTES + 3));
    for c in detail.chars() {
        if out.len() + c.len_utf8() > MAX_DETAIL_BYTES {
            out.push_str("...");
            break;
        }
        out.push(if c.is_control() { ' ' } else { c });
    }
    out
}

/// What a size budget measures, and the setting that configures it.
fn budget_names(budget: lake::SizeBudget) -> (&'static str, &'static str) {
    match budget {
        lake::SizeBudget::Request => ("request", "ingress.max_request_bytes"),
        lake::SizeBudget::Extracted => ("extracted request", "ingress.max_extracted_bytes"),
        lake::SizeBudget::Row => ("extracted row", "ingress.max_row_bytes"),
        lake::SizeBudget::Cell => ("attribute value", "ingress.max_row_bytes"),
        lake::SizeBudget::Table => ("decoded attribute table", "ingress.max_extracted_bytes"),
        lake::SizeBudget::Block => (
            "worst case in one block, every series written with the request,",
            "window.max_block_bytes",
        ),
    }
}

/// The size budget a size refusal exceeded: the setting, the observed size
/// when known, and the limit. `None` for any other error.
pub(super) fn excess(error: &lake::Error) -> Option<(&'static str, Option<usize>, usize)> {
    match error {
        lake::Error::Refused(lake::RefuseReason::RequestTooLarge(excess)) => {
            Some((budget_names(excess.budget).1, excess.observed, excess.limit))
        }
        _ => None,
    }
}

impl Outcome {
    /// The outcome a failed request is reported as.
    ///
    /// Only a lake refusal is permanent, and only one that judges the
    /// request's own content: the block-scoped refusals (`BlockFull`,
    /// `TooManyRequests`) judge whichever block happened to be active, so the
    /// worker parks the request instead, and one that reaches a sender is a
    /// broken invariant. Every other error is storage when the destination or
    /// the write caused it and internal otherwise; neither is a client error,
    /// because an OTLP producer drops permanently rejected data.
    pub(super) fn of(error: &lake::Error) -> Self {
        match error {
            lake::Error::Refused(reason) => match reason {
                lake::RefuseReason::RequestTooLarge(excess) => match excess.budget {
                    lake::SizeBudget::Request => Outcome::RequestTooLarge,
                    lake::SizeBudget::Extracted | lake::SizeBudget::Table => {
                        Outcome::ExtractedTooLarge
                    }
                    lake::SizeBudget::Row | lake::SizeBudget::Cell => Outcome::RowTooLarge,
                    lake::SizeBudget::Block => Outcome::BlockTooLarge,
                },
                lake::RefuseReason::TooDeep(_) => Outcome::TooDeep,
                lake::RefuseReason::Unsupported(_) => Outcome::Unsupported,
                lake::RefuseReason::Invalid(_) => Outcome::Invalid,
                lake::RefuseReason::BlockFull | lake::RefuseReason::TooManyRequests => {
                    Outcome::Internal
                }
            },
            lake::Error::Transient(_) => Outcome::Storage,
            lake::Error::Internal(_) => Outcome::Internal,
        }
    }

    /// Stable machine token of the outcome: the `error.type` label and the
    /// `outcome` log field.
    ///
    /// Never the reason a producer is told; that is a sentence, see
    /// [`Outcome::sentence`].
    pub(super) fn label(self) -> &'static str {
        self.as_str()
    }

    /// Whether the sender must change the request before retrying it.
    pub(super) fn refused(self) -> bool {
        matches!(
            self,
            Self::RequestTooLarge
                | Self::ExtractedTooLarge
                | Self::RowTooLarge
                | Self::BlockTooLarge
                | Self::TooDeep
                | Self::Invalid
                | Self::Unsupported
        )
    }

    /// The cause a nack of this outcome carries; `None` for an ack.
    pub(super) fn nack_cause(self) -> Option<NackCause> {
        match self {
            Self::Ack => None,
            Self::Shutdown => Some(NackCause::NodeShutdown),
            refused if refused.refused() => Some(NackCause::Refused),
            _ => Some(NackCause::Unspecified),
        }
    }

    /// The reason sentence a completion carries when its decision supplied
    /// none of its own: what happened and what the sender should do.
    pub(super) fn sentence(self) -> &'static str {
        match self {
            Self::Ack => "stored",
            Self::RequestTooLarge
            | Self::ExtractedTooLarge
            | Self::RowTooLarge
            | Self::BlockTooLarge => {
                "the request exceeds a series_parquet size budget; split the batch upstream"
            }
            Self::TooDeep => {
                "the request nests values deeper than ingress.max_nesting_depth; flatten them \
                 in the producer"
            }
            Self::Invalid => "the request content is invalid; fix the producer",
            Self::Unsupported => {
                "the request carries data series_parquet does not store; route it elsewhere"
            }
            Self::Storage => {
                "series_parquet could not write the block holding this request to object \
                 storage; retry the request"
            }
            Self::Shutdown => {
                "series_parquet shut down before the request was stored; retry the request"
            }
            Self::Internal => {
                "series_parquet hit an internal error handling the request; retry the request"
            }
        }
    }

    /// The reason sentence a sender is told about one failed request: what
    /// was refused or failed, against which limit, and what to do about it.
    ///
    /// Chosen by the error's outcome, so the sentence always agrees with the
    /// nack it travels on. Any text taken from the error itself is
    /// [`sanitized`], because an error can quote request content and the
    /// reason travels back to the producer.
    pub(super) fn explain(error: &lake::Error) -> String {
        match (Self::of(error), error) {
            (_, lake::Error::Refused(lake::RefuseReason::RequestTooLarge(excess))) => {
                let (what, setting) = budget_names(excess.budget);
                let limit = excess.limit;
                match excess.observed {
                    Some(observed) => format!(
                        "{what} of {observed} bytes exceeds {setting} ({limit} bytes); split \
                         the batch upstream or raise the limit"
                    ),
                    None => format!(
                        "{what} could not be measured against {setting} ({limit} bytes); \
                         split the batch upstream"
                    ),
                }
            }
            (_, lake::Error::Refused(lake::RefuseReason::Unsupported(what)))
                if what == "traces" =>
            {
                "traces are not stored by series_parquet; route traces to another exporter"
                    .to_owned()
            }
            (_, lake::Error::Refused(lake::RefuseReason::Unsupported(what)))
                if what == "exemplars" =>
            {
                "exemplars are not stored by series_parquet, and metrics.exemplars: reject \
                 refuses a request that carries them; set metrics.exemplars: drop (the \
                 default) to store the points without their exemplars, or route the request \
                 to another exporter"
                    .to_owned()
            }
            (_, lake::Error::Refused(lake::RefuseReason::Unsupported(what))) => {
                format!(
                    "{} metric points are not stored by series_parquet under unsupported: \
                     reject; set unsupported: drop to keep the supported points, or route \
                     them to another exporter",
                    sanitized(what)
                )
            }
            (_, lake::Error::Refused(lake::RefuseReason::TooDeep(limit))) => {
                format!(
                    "a nested value exceeds ingress.max_nesting_depth ({limit}); flatten it in \
                     the producer or raise the limit"
                )
            }
            (_, lake::Error::Refused(lake::RefuseReason::Invalid(detail))) => {
                format!(
                    "invalid request content: {}; fix the producer",
                    sanitized(detail)
                )
            }
            (Self::Storage, error) => format!(
                "object storage failed: {}; retry the request",
                sanitized(&error.to_string())
            ),
            (_, error) => format!(
                "series_parquet internal error: {}; the request is not at fault, retry it",
                sanitized(&error.to_string())
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every size budget a refusal can name, in declaration order.
    const BUDGETS: [lake::SizeBudget; 6] = [
        lake::SizeBudget::Request,
        lake::SizeBudget::Extracted,
        lake::SizeBudget::Row,
        lake::SizeBudget::Cell,
        lake::SizeBudget::Table,
        lake::SizeBudget::Block,
    ];

    /// Scenario: one error of every lake refusal reason -- a size refusal per
    /// size budget, excess nesting, unsupported content, invalid content and
    /// the two block-scoped refusals -- and one of each non-refusal class is
    /// mapped to its outcome, and the outcome to its nack.
    /// Guarantees: the whole table of error -> outcome -> (nack cause,
    /// permanent, `error.type`) holds: a size refusal names the budget it
    /// exceeded, only a refusal of the request's own content is a permanent
    /// `Refused` nack, a block-scoped refusal that reaches a sender is an
    /// internal retryable failure, storage failures are retryable `storage`,
    /// and every label is the one the `nacks` counter has always carried.
    #[test]
    fn every_lake_error_maps_to_one_outcome_and_one_nack() {
        let store = || {
            lake::Error::from(object_store::Error::Generic {
                store: "test",
                source: "unreachable".into(),
            })
        };
        let mut table: Vec<(lake::Error, Outcome, NackCause, bool, &str)> = BUDGETS
            .into_iter()
            .map(|budget| {
                let (outcome, label) = match budget {
                    lake::SizeBudget::Request => (Outcome::RequestTooLarge, "request_too_large"),
                    lake::SizeBudget::Extracted | lake::SizeBudget::Table => {
                        (Outcome::ExtractedTooLarge, "extracted_too_large")
                    }
                    lake::SizeBudget::Row | lake::SizeBudget::Cell => {
                        (Outcome::RowTooLarge, "row_too_large")
                    }
                    lake::SizeBudget::Block => (Outcome::BlockTooLarge, "block_too_large"),
                };
                (
                    lake::Error::too_large(budget, 2, 1),
                    outcome,
                    NackCause::Refused,
                    true,
                    label,
                )
            })
            .collect();
        table.extend([
            (
                lake::Error::Refused(lake::RefuseReason::TooDeep(32)),
                Outcome::TooDeep,
                NackCause::Refused,
                true,
                "too_deep",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::Unsupported("summary".into())),
                Outcome::Unsupported,
                NackCause::Refused,
                true,
                "unsupported",
            ),
            (
                lake::Error::invalid("undecodable pdata"),
                Outcome::Invalid,
                NackCause::Refused,
                true,
                "invalid",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::BlockFull),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                lake::Error::Refused(lake::RefuseReason::TooManyRequests),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                store(),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::cancelled(None),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::Transient(lake::TransientError::DeadlineExceeded {
                    attempts: 2,
                    last: Some(Box::new(store())),
                }),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::Transient(lake::TransientError::AbortFailed {
                    source: Box::new(lake::Error::internal("encode")),
                    abort_error: "timed out".into(),
                }),
                Outcome::Storage,
                NackCause::Unspecified,
                false,
                "storage",
            ),
            (
                lake::Error::internal("flush failed"),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                lake::Error::from(arrow::error::ArrowError::ComputeError("x".into())),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
            (
                lake::Error::from(parquet::errors::ParquetError::General("x".into())),
                Outcome::Internal,
                NackCause::Unspecified,
                false,
                "internal",
            ),
        ]);
        for (error, outcome, cause, permanent, label) in &table {
            let got = Outcome::of(error);
            assert_eq!(got, *outcome, "{error:?}");
            assert_eq!(got.nack_cause(), Some(*cause), "{error:?}");
            assert_eq!(got.refused(), *permanent, "{error:?}");
            assert_eq!(got.label(), *label, "{error:?}");
            let sized = matches!(
                outcome,
                Outcome::RequestTooLarge
                    | Outcome::ExtractedTooLarge
                    | Outcome::RowTooLarge
                    | Outcome::BlockTooLarge
            );
            assert_eq!(excess(error).is_some(), sized, "{error:?}");
        }
        assert_eq!(Outcome::Ack.nack_cause(), None);
        assert_eq!(
            Outcome::Shutdown.nack_cause(),
            Some(NackCause::NodeShutdown)
        );
        assert!(!Outcome::Shutdown.refused());
        assert_eq!(Outcome::Shutdown.label(), "shutdown");
        assert_eq!(Outcome::Ack.label(), "ack");
    }

    /// Scenario: the reason sentence is built for a size refusal, a storage
    /// failure and an internal failure whose detail spans two lines.
    /// Guarantees: the sentence follows the outcome -- a size refusal names
    /// the setting and both sizes, a storage failure says to retry, an
    /// internal failure says the request is not at fault -- and a detail taken
    /// from the error stays on one line.
    #[test]
    fn the_sentence_follows_the_outcome() {
        assert_eq!(
            Outcome::explain(&lake::Error::too_large(lake::SizeBudget::Row, 2, 1)),
            "extracted row of 2 bytes exceeds ingress.max_row_bytes (1 bytes); split the \
             batch upstream or raise the limit"
        );
        assert!(
            Outcome::explain(&lake::Error::cancelled(None))
                .starts_with("object storage failed: cancelled; retry")
        );
        assert_eq!(
            Outcome::explain(&lake::Error::internal("a\nb")),
            "series_parquet internal error: internal: a b; the request is not at fault, \
             retry it"
        );
        assert_eq!(
            Outcome::explain(&lake::Error::Refused(lake::RefuseReason::BlockFull)),
            "series_parquet internal error: refused: BlockFull; the request is not at \
             fault, retry it"
        );
    }
}
