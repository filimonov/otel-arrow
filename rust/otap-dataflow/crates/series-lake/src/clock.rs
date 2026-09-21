// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Wall clock abstraction, partition ids and window boundary arithmetic.

/// A `date/hour` storage partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId {
    /// Days since the Unix epoch (UTC).
    pub date: u32,
    /// Hour of day, 0..=23.
    pub hour: u8,
}

impl PartitionId {
    /// Partition of a Unix timestamp in seconds.
    #[must_use]
    pub fn from_unix_secs(secs: i64) -> Self {
        let days = secs.div_euclid(86_400);
        let hour = secs.rem_euclid(86_400) / 3_600;
        Self {
            date: u32::try_from(days).unwrap_or(0),
            hour: hour as u8,
        }
    }

    /// `YYYY-MM-DD` of the partition (proleptic Gregorian, UTC).
    #[must_use]
    pub fn date_string(&self) -> String {
        // Civil-from-days algorithm (Howard Hinnant), valid for all u32 day counts.
        let z = i64::from(self.date) + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z.rem_euclid(146_097);
        let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        format!("{y:04}-{m:02}-{d:02}")
    }

    /// `HH` of the partition.
    #[must_use]
    pub fn hour_string(&self) -> String {
        format!("{:02}", self.hour)
    }
}
