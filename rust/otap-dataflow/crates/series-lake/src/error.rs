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
    /// Flush cancelled.
    #[error("cancelled")]
    Cancelled,
}

/// Crate result.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Shorthand for an invalid-content refusal.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Refused(RefuseReason::Invalid(msg.into()))
    }
}
