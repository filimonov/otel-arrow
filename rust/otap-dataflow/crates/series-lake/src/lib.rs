// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Series/values normalization and Parquet sink for OTAP telemetry.
//!
//! See `docs/FORMAT.md` for the storage format and the design spec at the
//! repository root for the architecture.

pub mod attrs;
pub mod cache;
pub mod canonical;
pub mod clock;
pub mod config;
pub mod error;
pub mod extract;
pub mod schema;
pub mod sort;
pub mod value;

pub use error::{Error, RefuseReason, Result};
