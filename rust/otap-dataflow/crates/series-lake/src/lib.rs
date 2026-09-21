// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Series/values normalization and Parquet sink for OTAP telemetry.
//!
//! See `docs/FORMAT.md` for the storage format and the design spec at the
//! repository root for the architecture.

pub mod error;
pub mod value;

pub use error::{Error, RefuseReason, Result};
