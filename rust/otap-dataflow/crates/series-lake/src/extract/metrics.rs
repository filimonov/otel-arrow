// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Metrics extraction (task 7).

use otel_arrow_dfe_pdata::otap::OtapArrowRecords;

use super::{Budget, Extracted};
use crate::config::LakeConfig;
use crate::error::{Error, RefuseReason, Result};

pub(crate) fn extract_metrics(
    _records: &OtapArrowRecords,
    _cfg: &LakeConfig,
    _budget: &mut Budget,
) -> Result<Extracted> {
    Err(Error::Refused(RefuseReason::Unsupported(
        "metrics: not implemented".into(),
    )))
}
