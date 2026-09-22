// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types shared by the crate.

/// Why a request is permanently refused (spec section 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefuseReason {
    /// The request itself, its extracted output or one of its rows exceeds a budget.
    RequestTooLarge,
    /// The active block cannot take this request; the caller rotates and retries.
    BlockFull,
    /// The active block already holds `max_requests_per_block` tokens.
    TooManyRequests,
    /// Malformed content: duplicate keys, nesting too deep, bad histogram, ...
    Invalid(String),
    /// Unsupported signal or point kind under the reject policy.
    Unsupported(String),
}

/// Crate error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The request must be nacked as non-retryable.
    #[error("refused: {0:?}")]
    Refused(RefuseReason),
    /// Arrow failure.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Parquet failure.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Object store failure.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// pdata failure.
    #[error("pdata: {0}")]
    Pdata(String),
    /// A writer invariant did not hold: the request's content is not at fault.
    ///
    /// Distinct from [`Error::Refused`] so a caller never reports its own bug
    /// to a producer as a permanent refusal of the producer's data.
    #[error("internal: {0}")]
    Internal(String),
    /// Flush cancelled. `abort_error` is set when the best-effort multipart
    /// abort also failed or timed out.
    #[error("cancelled{}", match abort_error { Some(e) => format!(" (multipart abort failed: {e})"), None => String::new() })]
    Cancelled {
        /// Why the cleanup abort did not succeed, if it did not.
        abort_error: Option<String>,
    },
    /// A write failed and the best-effort multipart abort failed as well.
    #[error("{source}; multipart abort failed: {abort_error}")]
    AbortFailed {
        /// The original failure.
        source: Box<Error>,
        /// Why the cleanup abort did not succeed.
        abort_error: String,
    },
}

/// Crate result.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Shorthand for an invalid-content refusal.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Refused(RefuseReason::Invalid(msg.into()))
    }

    /// Shorthand for a broken writer invariant.
    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}
