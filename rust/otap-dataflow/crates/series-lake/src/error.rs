// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types shared by the crate.

/// The size budget a too-large request exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeBudget {
    /// The logical request size (`ingress.max_request_bytes`).
    Request,
    /// The extracted output of the request (`ingress.max_extracted_bytes`).
    Extracted,
    /// One values or series row (`ingress.max_row_bytes`).
    Row,
    /// One attribute key or value, or one CBOR cell (`ingress.max_row_bytes`).
    Cell,
    /// The decoded content of one attribute table
    /// (`ingress.max_extracted_bytes`).
    Table,
    /// The request's worst case in one block (`max_block_bytes`).
    Block,
}

/// How far a too-large request exceeded which budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Excess {
    /// The budget that refused the request.
    pub budget: SizeBudget,
    /// The size measured against it, when the refusing check knew it.
    pub observed: Option<usize>,
    /// The configured limit.
    pub limit: usize,
}

impl Excess {
    /// A refusal of `observed` bytes against `limit` for `budget`.
    #[must_use]
    pub fn new(budget: SizeBudget, observed: usize, limit: usize) -> Self {
        Self {
            budget,
            observed: Some(observed),
            limit,
        }
    }
}

/// Why a request is permanently refused (spec section 8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefuseReason {
    /// The request itself, its extracted output or one of its rows exceeds a
    /// budget; the excess says which, by how much.
    RequestTooLarge(Excess),
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
    /// A caller that retries the sink ran out of its retry deadline.
    ///
    /// Distinct from [`Error::Cancelled`], which is a decision taken by the
    /// caller, so a destination that keeps failing or never answers is not
    /// reported as a cancellation. `last` is the failure of the last attempt
    /// that returned, and `None` when the attempt in flight at the deadline
    /// was the first and had not returned.
    #[error("flush retry deadline exceeded after {attempts} attempt(s); {}", match last { Some(e) => format!("last error: {e}"), None => "no attempt returned before the deadline".to_owned() })]
    DeadlineExceeded {
        /// Attempts started before the deadline, the unfinished one included.
        attempts: u64,
        /// The failure of the last attempt that returned, if any did.
        last: Option<Box<Error>>,
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
    /// Shorthand for a size refusal of `observed` bytes against `limit`.
    #[must_use]
    pub fn too_large(budget: SizeBudget, observed: usize, limit: usize) -> Self {
        Error::Refused(RefuseReason::RequestTooLarge(Excess::new(
            budget, observed, limit,
        )))
    }

    /// Shorthand for an invalid-content refusal.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Refused(RefuseReason::Invalid(msg.into()))
    }

    /// Shorthand for a broken writer invariant.
    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal(msg.into())
    }
}
