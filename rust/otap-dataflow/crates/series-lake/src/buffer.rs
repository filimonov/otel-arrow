// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sorted run buffers and the ACTIVE/FLUSHING block (spec sections 6.1 to 6.3).

use std::collections::{BTreeMap, HashSet};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};

use crate::cache::SeriesCache;
use crate::canonical::SeriesId;
use crate::clock::PartitionId;
use crate::config::LakeConfig;
use crate::error::{Error, RefuseReason, Result};
use crate::extract::{DescriptorRow, Extracted, series_batch};
use crate::schema::Dataset;
use crate::sort::{SortSpec, sort_batch};

/// Building batches plus sealed sorted runs for one dataset.
pub struct SortedTableBuffer {
    dataset: Dataset,
    spec: SortSpec,
    run_target: usize,
    building: Vec<RecordBatch>,
    building_bytes: usize,
    runs: Vec<RecordBatch>,
    seen: CountedAllocations,
    rows: usize,
}

impl SortedTableBuffer {
    /// New empty buffer.
    #[must_use]
    pub fn new(dataset: Dataset, spec: SortSpec, run_target_bytes: usize) -> Self {
        Self {
            dataset,
            spec,
            run_target: run_target_bytes.max(1),
            building: Vec::new(),
            building_bytes: 0,
            runs: Vec::new(),
            seen: CountedAllocations::default(),
            rows: 0,
        }
    }

    /// Append a batch; returns the pinned bytes newly retained by this buffer.
    ///
    /// The returned value is an upper bound used while the block fills: it never
    /// sees the bytes that `seal` releases when the building batches are replaced
    /// by one concatenated run. `Block::seal` recomputes the truth.
    ///
    /// # Errors
    /// Propagates an Arrow failure from the concatenate-and-sort of a sealed run.
    pub fn append(&mut self, batch: RecordBatch) -> Result<usize> {
        let pinned = record_batch_pinned_bytes(&batch, &mut self.seen);
        self.rows += batch.num_rows();
        self.building_bytes += pinned;
        self.building.push(batch);
        if self.building_bytes >= self.run_target {
            self.seal()?;
        }
        Ok(pinned)
    }

    /// Sort and seal the building batches into one run.
    ///
    /// No re-accounting happens here: recomputing the pinned bytes of every run
    /// on every seal would be quadratic in the number of runs. `Block::seal`
    /// performs one deduplicated recount over the whole block instead.
    ///
    /// # Errors
    /// Propagates an Arrow failure from the concatenate or the sort.
    pub fn seal(&mut self) -> Result<()> {
        let Some(first) = self.building.first() else {
            return Ok(());
        };
        let schema = first.schema();
        let merged = concat_batches(&schema, &self.building)?;
        let sorted = sort_batch(&merged, &self.spec)?;
        self.runs.push(sorted);
        self.building.clear();
        self.building_bytes = 0;
        Ok(())
    }

    /// Sealed runs.
    #[must_use]
    pub fn runs(&self) -> &[RecordBatch] {
        &self.runs
    }

    /// Unsealed batches.
    #[must_use]
    pub fn building(&self) -> &[RecordBatch] {
        &self.building
    }

    /// Total rows.
    #[must_use]
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Whether nothing was appended.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Dataset.
    #[must_use]
    pub fn dataset(&self) -> Dataset {
        self.dataset
    }

    /// Sort spec.
    #[must_use]
    pub fn spec(&self) -> &SortSpec {
        &self.spec
    }

    /// Building batches then sealed runs, for bounded snapshot copies.
    pub fn iter_snapshots(&self) -> impl Iterator<Item = &RecordBatch> {
        self.building.iter().chain(self.runs.iter())
    }
}

/// Outcome of a reservation: bytes to charge and descriptors to emit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// Bytes the request will add to the block.
    pub bytes: usize,
    /// The part of `bytes` that is the ack token.
    pub token_bytes: usize,
    /// Indices into `Extracted::descriptors` whose descriptor must be written by this block.
    pub new_series: Vec<usize>,
}

/// One ACTIVE or FLUSHING block.
pub struct Block<T> {
    /// Window start (Unix seconds).
    pub window_start_secs: i64,
    /// Destination partition.
    pub partition: PartitionId,
    /// Per-worker sequence number used in file names.
    pub seq: u64,
    tables: BTreeMap<Dataset, SortedTableBuffer>,
    /// Series whose descriptor this block carries.
    pub pending_series: HashSet<SeriesId>,
    /// Accounted bytes: an upper bound while filling, exact after `seal`.
    pub bytes: usize,
    /// Request tokens.
    pub requests: Vec<T>,
    /// Descriptor rows waiting for the seal timestamp, keyed by series dataset.
    pending_descriptors: BTreeMap<Dataset, Vec<DescriptorRow>>,
    token_bytes: usize,
    emitted_at_us: Option<i64>,
    cfg: LakeConfig,
}

impl<T> Block<T> {
    /// New empty block for a window.
    #[must_use]
    pub fn new(window_start_secs: i64, seq: u64, cfg: &LakeConfig) -> Self {
        Self {
            window_start_secs,
            partition: PartitionId::from_unix_secs(window_start_secs),
            seq,
            tables: BTreeMap::new(),
            pending_series: HashSet::new(),
            bytes: 0,
            requests: Vec::new(),
            pending_descriptors: BTreeMap::new(),
            token_bytes: 0,
            emitted_at_us: None,
            cfg: cfg.clone(),
        }
    }

    fn spec_for(&self, ds: Dataset) -> SortSpec {
        if ds.is_series() {
            SortSpec::series()
        } else if !self.cfg.sorting.enabled {
            SortSpec::new(vec![])
        } else if ds.signal() == crate::canonical::Signal::Logs {
            SortSpec::new(self.cfg.logs.values_sort.clone())
        } else {
            SortSpec::new(self.cfg.metrics.values_sort.clone())
        }
    }

    /// Compute what admitting `extracted` would add (spec section 6.2 step 5).
    ///
    /// Refuses before touching anything:
    /// * `RequestTooLarge` when the reservation alone exceeds `max_block_bytes`,
    ///   even for an empty block -- a permanent nack;
    /// * `TooManyRequests` when the block already holds `max_requests_per_block`;
    /// * `BlockFull` when the request does not fit the remaining budget.
    ///
    /// The last two tell the caller to rotate and offer the request to the next
    /// block. The block is never mutated, and the cache is only written (through
    /// `touch`) once the reservation is accepted; the `is_committed` lookups
    /// above move LRU recency, which is not correctness state (invariant 3).
    ///
    /// # Errors
    /// Returns `Error::Refused` with one of the three reasons above.
    pub fn reserve(
        &self,
        extracted: &Extracted,
        cache: &mut SeriesCache,
        token_bytes: usize,
        cfg: &LakeConfig,
    ) -> Result<Reservation> {
        let limits = &cfg.ingress;
        let mut bytes = extracted.pinned_bytes + token_bytes;
        let mut new_series = Vec::new();
        for (i, d) in extracted.descriptors.iter().enumerate() {
            let committed_here = cache.is_committed(&d.series_id, self.partition);
            if !committed_here && !self.pending_series.contains(&d.series_id) {
                new_series.push(i);
                bytes += d.approx_bytes + limits.pending_series_entry_bytes;
            }
        }
        if bytes > limits.max_block_bytes {
            return Err(Error::Refused(RefuseReason::RequestTooLarge));
        }
        if self.requests.len() >= limits.max_requests_per_block {
            return Err(Error::Refused(RefuseReason::TooManyRequests));
        }
        if self.bytes + bytes > limits.max_block_bytes {
            return Err(Error::Refused(RefuseReason::BlockFull));
        }
        for d in &extracted.descriptors {
            cache.touch(d.series_id);
        }
        Ok(Reservation {
            bytes,
            token_bytes,
            new_series,
        })
    }

    /// Admit a reserved request (spec section 6.2 step 6).
    ///
    /// Descriptor rows are held, not written: `seal` stamps them with the block's
    /// `emitted_at` and builds the `series` batch then.
    ///
    /// # Errors
    /// Propagates an Arrow failure from sealing a run inside a table buffer.
    pub fn admit(
        &mut self,
        extracted: Extracted,
        reservation: Reservation,
        token: T,
    ) -> Result<()> {
        let signal = extracted.signal;
        if !reservation.new_series.is_empty() {
            let ds = Dataset::series_of(signal);
            let slot = self.pending_descriptors.entry(ds).or_default();
            for i in reservation.new_series {
                let row = extracted.descriptors[i].clone();
                let _ = self.pending_series.insert(row.series_id);
                slot.push(row);
            }
        }
        for (ds, batches) in extracted.values {
            let run_target = self.cfg.sorting.run_target_bytes;
            let spec = self.spec_for(ds);
            let table = self
                .tables
                .entry(ds)
                .or_insert_with(|| SortedTableBuffer::new(ds, spec, run_target));
            for b in batches {
                let _ = table.append(b)?;
            }
        }
        self.bytes += reservation.bytes;
        self.token_bytes += reservation.token_bytes;
        self.requests.push(token);
        Ok(())
    }

    /// Stamp `emitted_at`, write the descriptor rows and seal every table.
    ///
    /// Idempotent: a flush retry re-seals the same block and reuses the first
    /// stamp, so the file bytes are identical across attempts (spec 5.3).
    ///
    /// # Errors
    /// Propagates a failure from building the `series` batch or sealing a run.
    pub fn seal(&mut self, emitted_at_us: i64) -> Result<()> {
        let stamp = *self.emitted_at_us.get_or_insert(emitted_at_us);
        let pending = std::mem::take(&mut self.pending_descriptors);
        for (ds, rows) in pending {
            if rows.is_empty() {
                continue;
            }
            let refs: Vec<&DescriptorRow> = rows.iter().collect();
            let batch = series_batch(&refs, stamp, ds, &self.cfg)?;
            let run_target = self.cfg.sorting.run_target_bytes;
            let spec = self.spec_for(ds);
            let table = self
                .tables
                .entry(ds)
                .or_insert_with(|| SortedTableBuffer::new(ds, spec, run_target));
            let _ = table.append(batch)?;
        }
        for t in self.tables.values_mut() {
            t.seal()?;
        }
        self.bytes = self.recount();
        Ok(())
    }

    /// The seal timestamp, once the block has been sealed.
    #[must_use]
    pub fn emitted_at_us(&self) -> Option<i64> {
        self.emitted_at_us
    }

    /// One deduplicated pass over everything the block retains.
    fn recount(&self) -> usize {
        let mut seen = CountedAllocations::default();
        let mut bytes = 0usize;
        for t in self.tables.values() {
            for b in t.iter_snapshots() {
                bytes += record_batch_pinned_bytes(b, &mut seen);
            }
        }
        for rows in self.pending_descriptors.values() {
            bytes += rows.iter().map(|r| r.approx_bytes).sum::<usize>();
        }
        bytes += self.pending_series.len() * self.cfg.ingress.pending_series_entry_bytes;
        bytes + self.token_bytes
    }

    /// Tables in write order: series datasets first (Dataset's Ord puts LogsSeries
    /// and MetricsSeries before their values datasets).
    pub fn tables(&self) -> impl Iterator<Item = &SortedTableBuffer> {
        self.tables.values()
    }

    /// Whether the block holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending_descriptors.values().all(Vec::is_empty)
            && self.tables.values().all(SortedTableBuffer::is_empty)
    }

    /// Number of admitted requests.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Take the block apart after a flush result.
    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, HashSet<SeriesId>, BTreeMap<Dataset, SortedTableBuffer>) {
        (self.requests, self.pending_series, self.tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::SeriesCache;
    use crate::config::LakeConfig;
    use crate::error::RefuseReason;
    use crate::extract::extract;
    use arrow::array::AsArray;
    use arrow::datatypes::TimestampMicrosecondType;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;

    const SEAL_AT_US: i64 = 1_700_000_000_000_000;

    fn logs(host: &str, n: usize) -> LogsData {
        let kv = |k: &str, v: &str| KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", host)],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: (0..n)
                        .map(|i| LogRecord {
                            time_unix_nano: 1000 + i as u64,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn extracted(cfg: &LakeConfig, host: &str, n: usize) -> Extracted {
        let mut records = encode_logs(&logs(host, n));
        extract(&mut records, cfg).expect("extract")
    }

    /// Scenario: two requests for the same series in one block, then the same series after commit.
    /// Guarantees: the descriptor is reserved once per block and not at all once committed in the partition.
    #[test]
    fn reserve_emits_descriptor_once_per_block_and_partition() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e1 = extracted(&cfg, "h", 2);
        let r1 = block.reserve(&e1, &mut cache, 16, &cfg).expect("reserve 1");
        assert_eq!(r1.new_series, vec![0]);
        assert!(r1.bytes > 16);
        block.admit(e1, r1, 1).expect("admit");
        let e2 = extracted(&cfg, "h", 1);
        let r2 = block.reserve(&e2, &mut cache, 16, &cfg).expect("reserve 2");
        assert!(r2.new_series.is_empty());
        block.admit(e2, r2, 2).expect("admit");
        assert_eq!(block.request_count(), 2);
        assert_eq!(block.pending_series.len(), 1);
        block.seal(SEAL_AT_US).expect("seal");
        let series = block
            .tables()
            .find(|t| t.dataset().is_series())
            .expect("series table");
        assert_eq!(series.rows(), 1);
        // commit in the block's partition
        for id in &block.pending_series {
            cache.mark_committed(*id, block.partition);
        }
        let next: Block<u32> = Block::new(0, 2, &cfg);
        let e3 = extracted(&cfg, "h", 1);
        assert!(
            next.reserve(&e3, &mut cache, 16, &cfg)
                .expect("reserve 3")
                .new_series
                .is_empty()
        );
        let other: Block<u32> = Block::new(3600, 3, &cfg); // next hour
        assert_eq!(
            other
                .reserve(&e3, &mut cache, 16, &cfg)
                .expect("reserve 4")
                .new_series,
            vec![0]
        );
    }

    /// Scenario: `emitted_at` on a block sealed once and then sealed again, as a flush retry does.
    /// Guarantees: every descriptor row carries the first seal's timestamp and the second seal does not restamp.
    #[test]
    fn emitted_at_is_stamped_at_seal_and_frozen() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e = extracted(&cfg, "h", 2);
        let r = block.reserve(&e, &mut cache, 16, &cfg).expect("reserve");
        block.admit(e, r, 1).expect("admit");
        block.seal(SEAL_AT_US).expect("seal");
        block.seal(SEAL_AT_US + 5_000_000).expect("re-seal");
        assert_eq!(block.emitted_at_us(), Some(SEAL_AT_US));
        let series = block
            .tables()
            .find(|t| t.dataset().is_series())
            .expect("series table");
        let batch = series.runs().first().expect("run");
        let stamps = batch.column_by_name("emitted_at").expect("emitted_at");
        let stamps = stamps.as_primitive::<TimestampMicrosecondType>();
        assert!((0..stamps.len()).all(|i| stamps.value(i) == SEAL_AT_US));
    }

    /// Scenario: a request whose reservation alone exceeds `max_block_bytes`, offered to an empty block.
    /// Guarantees: refused permanently as `RequestTooLarge`, and the empty block is left untouched.
    #[test]
    fn oversize_request_is_refused_by_an_empty_block() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_block_bytes = 1;
        let mut cache = SeriesCache::new(100);
        let block: Block<u32> = Block::new(0, 1, &cfg);
        let e = extracted(&LakeConfig::default(), "h", 4);
        assert!(matches!(
            block.reserve(&e, &mut cache, 16, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge))
        ));
        assert_eq!(block.bytes, 0);
        assert_eq!(block.request_count(), 0);
        assert!(block.pending_series.is_empty());
        assert_eq!(block.tables().count(), 0);
    }

    /// Scenario: a block already holding `max_requests_per_block` tokens.
    /// Guarantees: the next reservation is refused with `TooManyRequests` and the block is unchanged.
    #[test]
    fn request_count_limit_refuses_before_mutating() {
        let mut cfg = LakeConfig::default();
        cfg.ingress.max_requests_per_block = 2;
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        for token in 0..2u32 {
            let e = extracted(&cfg, "h", 1);
            let r = block.reserve(&e, &mut cache, 16, &cfg).expect("reserve");
            block.admit(e, r, token).expect("admit");
        }
        let before_bytes = block.bytes;
        let before_series = block.pending_series.len();
        let e = extracted(&cfg, "h", 1);
        assert!(matches!(
            block.reserve(&e, &mut cache, 16, &cfg),
            Err(Error::Refused(RefuseReason::TooManyRequests))
        ));
        assert_eq!(block.bytes, before_bytes);
        assert_eq!(block.pending_series.len(), before_series);
        assert_eq!(block.request_count(), 2);
    }

    /// Scenario: a non-empty block that cannot take one more request within `max_block_bytes`.
    /// Guarantees: `BlockFull` is returned, which the node loop turns into a rotation, and the block is unchanged.
    #[test]
    fn full_block_refuses_with_block_full() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let first = extracted(&cfg, "h", 4);
        let r = block.reserve(&first, &mut cache, 16, &cfg).expect("reserve");
        block.admit(first, r, 1).expect("admit");
        let before_bytes = block.bytes;

        // A second, distinct series whose own reservation fits the limit, so the
        // only reason to refuse it is the bytes the block already holds.
        let e = extracted(&cfg, "h2", 4);
        let probe = block
            .reserve(&e, &mut cache, 16, &cfg)
            .expect("fits on its own");
        let mut tight = cfg.clone();
        tight.ingress.max_block_bytes = before_bytes + probe.bytes - 1;
        assert!(matches!(
            block.reserve(&e, &mut cache, 16, &tight),
            Err(Error::Refused(RefuseReason::BlockFull))
        ));
        assert_eq!(block.bytes, before_bytes);
        assert_eq!(block.request_count(), 1);
    }

    /// Scenario: two requests that share the same Arrow buffers are admitted into one block.
    /// Guarantees: the seal-time recount deduplicates shared allocations, so the block's
    /// byte count is at most the sum of the per-request reservations and at least one copy.
    #[test]
    fn seal_recounts_shared_buffers_once() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e1 = extracted(&cfg, "h", 8);
        let one_request_pinned = e1.pinned_bytes;
        let r1 = block.reserve(&e1, &mut cache, 16, &cfg).expect("r1");
        block.admit(e1, r1, 1).expect("admit");
        let reserved = block.bytes;
        block.seal(SEAL_AT_US).expect("seal");
        assert!(
            block.bytes <= reserved,
            "the recount is never above the reservation upper bound"
        );
        assert!(
            block.bytes >= one_request_pinned / 2,
            "one copy of the data is still counted"
        );
    }

    /// Scenario: the same Arrow batch, cloned, is appended to one buffer twice.
    /// Guarantees: the clone shares every buffer, so the second append reports
    /// zero newly retained bytes while both copies of the rows are still counted.
    #[test]
    fn append_counts_shared_buffers_once() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 8);
        let (ds, batches) = e.values.into_iter().next().expect("values");
        let batch = batches.into_iter().next().expect("batch");
        let mut buf = SortedTableBuffer::new(ds, SortSpec::new(vec![]), usize::MAX);
        let first = buf.append(batch.clone()).expect("first append");
        let second = buf.append(batch.clone()).expect("second append");
        assert!(first > 0, "the first append retains the batch's buffers");
        assert_eq!(second, 0, "the clone shares them, so nothing new is retained");
        assert_eq!(buf.rows(), batch.num_rows() * 2);
    }

    /// Scenario: a buffer with a tiny run target receives several batches.
    /// Guarantees: runs are sealed sorted, byte accounting grows, snapshots iterate everything.
    #[test]
    fn buffer_seals_sorted_runs() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 50);
        let (_, batches) = e.values.into_iter().next().expect("values");
        let mut buf = SortedTableBuffer::new(
            Dataset::LogsValues,
            SortSpec::new(cfg.logs.values_sort.clone()),
            1,
        );
        let mut total = 0;
        for b in batches {
            total += buf.append(b).expect("append");
        }
        assert!(total > 0);
        assert!(!buf.runs().is_empty());
        for r in buf.runs() {
            assert!(crate::sort::is_sorted(r, buf.spec()).expect("sorted"));
        }
        assert_eq!(buf.iter_snapshots().map(|b| b.num_rows()).sum::<usize>(), 50);
        buf.seal().expect("seal");
        assert!(buf.building().is_empty());
    }
}
