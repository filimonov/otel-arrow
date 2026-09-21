// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sorted run buffers and the ACTIVE/FLUSHING block (spec sections 6.1 to 6.3).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, TimeUnit};
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
    /// Deduplication is scoped to the run being built, never wider. `seen` holds
    /// raw buffer addresses, which are only meaningful while the buffers they
    /// name are alive; every batch it has measured is still held in `building`,
    /// so no address in it can have been freed and handed back out by the
    /// allocator. A set that outlived a seal could match a freed address against
    /// a fresh batch, report zero bytes for it and stall `building_bytes` below
    /// `run_target` forever.
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
    /// With sorting disabled the spec is empty and there is nothing to order, so
    /// the building batches become runs as they are: concatenating them would
    /// copy every row to no purpose. A sorted seal instead produces exactly one
    /// run, which is what the k-way merge downstream consumes.
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
        if self.spec.is_empty() {
            self.runs.append(&mut self.building);
        } else {
            let schema = first.schema();
            let merged = concat_batches(&schema, &self.building)?;
            let sorted = sort_batch(&merged, &self.spec)?;
            self.runs.push(sorted);
            self.building.clear();
        }
        self.building_bytes = 0;
        // The addresses in `seen` name buffers this buffer no longer measures
        // against. Start the next run with an empty set so a reused address
        // cannot silently zero out a fresh batch.
        self.seen = CountedAllocations::default();
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

    /// Append request-local descriptors as independently bounded sorted series runs.
    ///
    /// A request may produce several runs; each is built, sorted and measured on
    /// its own, so at most one candidate run plus the sort's own scratch is live
    /// at a time. A candidate whose measured size overshoots `run_target` is
    /// halved and rebuilt. No block-sized series batch is ever constructed.
    ///
    /// A single row may exceed `run_target` by its Arrow overhead and becomes
    /// its own oversized run rather than a refusal: `run_target_bytes` is a
    /// packing target, and the admission limits are `max_row_bytes`, enforced
    /// during extraction, and `max_block_bytes`, enforced by `reserve`. A
    /// validated configuration barely reaches this case at all, because
    /// `validate` requires `max_row_bytes <= run_target_bytes / 4`.
    ///
    /// # Errors
    /// Propagates a series-batch or sort failure.
    fn append_series(&mut self, rows: &[&DescriptorRow], cfg: &LakeConfig) -> Result<()> {
        let mut start = 0;
        while start < rows.len() {
            let mut end = start;
            let mut estimated = 0usize;
            while end < rows.len() {
                let next = rows[end].series_row_bytes();
                if end > start && estimated.saturating_add(next) > self.run_target {
                    break;
                }
                estimated = estimated.saturating_add(next);
                end += 1;
                if estimated >= self.run_target {
                    break;
                }
            }
            let sorted = loop {
                let batch = series_batch(&rows[start..end], 0, self.dataset, cfg)?;
                let sorted = sort_batch(&batch, &self.spec)?;
                drop(batch);
                let bytes = record_batch_pinned_bytes(&sorted, &mut CountedAllocations::default());
                if bytes <= self.run_target || end == start + 1 {
                    break sorted;
                }
                drop(sorted);
                end = start + (end - start) / 2;
            };
            self.rows += sorted.num_rows();
            self.runs.push(sorted);
            start = end;
        }
        Ok(())
    }

    /// Prepare the run that `seal` would make of the building batches, without
    /// mutating anything.
    ///
    /// The non-mutating half of [`SortedTableBuffer::seal`], used by
    /// `Block::seal` so that finalizing a values table is part of the same
    /// all-or-nothing transaction as stamping the series tables. An empty
    /// building set produces no run. With sorting disabled the building batches
    /// become runs as they are, sharing every buffer, because concatenating
    /// them would copy every row to no purpose.
    ///
    /// # Errors
    /// Propagates an Arrow failure from the concatenate or the sort.
    fn finalized(&self) -> Result<Vec<RecordBatch>> {
        let Some(first) = self.building.first() else {
            return Ok(Vec::new());
        };
        if self.spec.is_empty() {
            return Ok(self.building.clone());
        }
        let schema = first.schema();
        let merged = concat_batches(&schema, &self.building)?;
        Ok(vec![sort_batch(&merged, &self.spec)?])
    }

    /// Prepare every stamp replacement without mutating the retained batches.
    ///
    /// Returns the replacement runs and building batches. Every fallible step
    /// happens here, so a caller that gets an error has a buffer that is still
    /// exactly what it was.
    ///
    /// # Errors
    /// Rejects a series batch whose `emitted_at` is not a UTC microsecond
    /// timestamp, and propagates an Arrow failure from rebuilding the batch.
    fn stamped(&self, stamp: i64) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
        let replace = |batch: &RecordBatch| -> Result<RecordBatch> {
            let schema = batch.schema();
            let index = schema.index_of("emitted_at")?;
            let expected = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
            if schema.field(index).data_type() != &expected {
                return Err(Error::invalid(
                    "series emitted_at must be a UTC microsecond timestamp",
                ));
            }
            // One i64 per row, wrapped in the timestamp type without a cast or a
            // copy: every other column of the batch is shared with the original.
            let ints = Int64Array::from(vec![stamp; batch.num_rows()]);
            let values =
                TimestampMicrosecondArray::new(ints.values().clone(), None).with_timezone("UTC");
            let mut columns = batch.columns().to_vec();
            columns[index] = Arc::new(values) as ArrayRef;
            Ok(RecordBatch::try_new(schema, columns)?)
        };
        let runs = self.runs.iter().map(replace).collect::<Result<Vec<_>>>()?;
        let building = self
            .building
            .iter()
            .map(replace)
            .collect::<Result<Vec<_>>>()?;
        Ok((runs, building))
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
                bytes += d.series_row_bytes() + limits.pending_series_entry_bytes;
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

    /// Admit a reserved request, materializing series rows with a zero stamp
    /// (spec section 6.2 step 6).
    ///
    /// Descriptor rows become Arrow series rows here, in bounded sorted runs,
    /// with `emitted_at` left at zero until the block seals. Values batches
    /// accumulate into the building run of their table and are sealed into a
    /// sorted run once they cross `run_target_bytes`, so a run packs batches
    /// from as many requests as fit it.
    ///
    /// A sealed block takes nothing more. Its `emitted_at` is already fixed, so
    /// a late descriptor would be stamped with a time before it arrived, and its
    /// `bytes` is already the exact recount, which a reservation's upper-bound
    /// estimate would corrupt. The caller rotates to a new block instead.
    ///
    /// An admission that fails leaves the block partially updated on purpose:
    /// the caller's contract is to discard the whole ACTIVE block, and it owns
    /// the request's ack token separately.
    ///
    /// # Errors
    /// Refuses a sealed block, and propagates a series-construction or run
    /// sorting failure.
    pub fn admit(
        &mut self,
        extracted: Extracted,
        reservation: Reservation,
        token: T,
    ) -> Result<()> {
        if self.is_sealed() {
            return Err(Error::invalid("block already sealed"));
        }
        let Extracted {
            signal,
            descriptors,
            values,
            ..
        } = extracted;
        if !reservation.new_series.is_empty() {
            let ds = Dataset::series_of(signal);
            // The reservation is public, so its indices are caller-supplied
            // data: resolve them all before anything is mutated.
            let rows: Vec<&DescriptorRow> = reservation
                .new_series
                .iter()
                .map(|&i| {
                    descriptors.get(i).ok_or_else(|| {
                        Error::invalid("reservation names a descriptor the request does not carry")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            // Take the table out for the call so that `append_series` can borrow
            // it mutably while `self.cfg` is read, then put it straight back --
            // including on the error path, so the block stays well formed.
            let mut table = self.tables.remove(&ds).unwrap_or_else(|| {
                SortedTableBuffer::new(ds, SortSpec::series(), self.cfg.sorting.run_target_bytes)
            });
            let result = table.append_series(&rows, &self.cfg);
            let _ = self.tables.insert(ds, table);
            result?;
            for row in rows {
                let _ = self.pending_series.insert(row.series_id);
            }
        }
        drop(descriptors);
        for (ds, batches) in values {
            let run_target = self.cfg.sorting.run_target_bytes;
            let spec = self.spec_for(ds);
            let table = self
                .tables
                .entry(ds)
                .or_insert_with(|| SortedTableBuffer::new(ds, spec, run_target));
            for batch in batches {
                // `append` seals a run once the building batches cross
                // `run_target`, so batches from several requests pack into one
                // run. Whatever is still building when the block seals is
                // finalized there, inside the same transaction as the stamps.
                let _ = table.append(batch)?;
            }
        }
        self.bytes += reservation.bytes;
        self.token_bytes += reservation.token_bytes;
        self.requests.push(token);
        Ok(())
    }

    /// Replace only `emitted_at` buffers, committing the sealed state after all
    /// swaps succeed.
    ///
    /// Idempotent: a flush retry re-seals the same block and keeps the first
    /// stamp, so the file bytes are identical across attempts (spec 5.3).
    ///
    /// All or nothing. Every replacement batch is prepared before any retained
    /// batch is touched, so a failure leaves every batch, the accounting and the
    /// seal state exactly as they were, and the block can be sealed again once
    /// whatever caused the failure is gone. `emitted_at` is committed last, so
    /// the sink refuses to write a block that never finished sealing rather than
    /// writing unstamped series rows.
    ///
    /// Sealing the series tables costs one new eight-byte timestamp per series
    /// row and nothing else: every other column is shared between the old batch
    /// and its replacement. A values table pays for finalizing whatever was
    /// still building, which is one run bounded by `run_target_bytes`.
    ///
    /// # Errors
    /// A bad series schema, or an Arrow failure finalizing a values run, leaves
    /// all retained batches, accounting and seal state unchanged.
    pub fn seal(&mut self, emitted_at_us: i64) -> Result<()> {
        if self.is_sealed() {
            return Ok(());
        }
        let mut stamped = Vec::new();
        let mut finalized = Vec::new();
        for (ds, table) in &self.tables {
            if ds.is_series() {
                stamped.push((*ds, table.stamped(emitted_at_us)?));
            } else {
                finalized.push((*ds, table.finalized()?));
            }
        }
        // All fallible Arrow work has finished. The transaction holds the
        // series tables' shared non-stamp columns plus one new eight-byte
        // timestamp per series row, and one finalized run per values table.
        for (ds, (runs, building)) in stamped {
            let table = self.tables.get_mut(&ds).expect("prepared table exists");
            table.runs = runs;
            table.building = building;
        }
        for (ds, runs) in finalized {
            let table = self.tables.get_mut(&ds).expect("prepared table exists");
            table.building.clear();
            table.runs.extend(runs);
        }
        for table in self.tables.values_mut() {
            // Every batch is already sorted: the series replacements by
            // admission, the values runs by `finalized`. Moving them cannot
            // copy a column.
            table.runs.append(&mut table.building);
            table.building_bytes = 0;
            table.seen = CountedAllocations::default();
        }
        self.bytes = self.recount();
        self.emitted_at_us = Some(emitted_at_us);
        Ok(())
    }

    /// The seal timestamp, once the block has been sealed.
    #[must_use]
    pub fn emitted_at_us(&self) -> Option<i64> {
        self.emitted_at_us
    }

    /// Whether all timestamp swaps have committed successfully.
    ///
    /// Series rows exist from admission onwards, but they carry a placeholder
    /// `emitted_at` of zero until the stamp transaction commits. A consumer that
    /// walked `tables()` of an unsealed block would read unstamped data, so the
    /// sink checks this before it writes. `seal` commits `emitted_at` only after
    /// every swap has been prepared, so a block whose seal failed reports false
    /// here.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.emitted_at_us.is_some()
    }

    /// One deduplicated pass over everything the block retains.
    ///
    /// Every row of the block, series rows included, is already an Arrow row by
    /// the time this runs, so the batches below are the whole payload.
    fn recount(&self) -> usize {
        let mut seen = CountedAllocations::default();
        let mut bytes = 0usize;
        for t in self.tables.values() {
            for b in t.iter_snapshots() {
                bytes += record_batch_pinned_bytes(b, &mut seen);
            }
        }
        bytes += self.pending_series.len() * self.cfg.ingress.pending_series_entry_bytes;
        bytes + self.token_bytes
    }

    /// Tables in write order: series datasets first (Dataset's Ord puts LogsSeries
    /// and MetricsSeries before their values datasets).
    pub fn tables(&self) -> impl Iterator<Item = &SortedTableBuffer> {
        self.tables.values()
    }

    /// Whether the block holds no materialized rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.values().all(SortedTableBuffer::is_empty)
    }

    /// Number of admitted requests.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Take apart a successfully sealed block after its flush result.
    ///
    /// The series rows of an unsealed block still carry their placeholder
    /// `emitted_at` of zero. The sink always seals before it flushes and only
    /// takes the block apart once the flush has resolved, so the debug assertion
    /// below catches a caller that has stepped outside that order.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        Vec<T>,
        HashSet<SeriesId>,
        BTreeMap<Dataset, SortedTableBuffer>,
    ) {
        debug_assert!(
            self.is_sealed(),
            "into_parts requires a successfully sealed block"
        );
        (self.requests, self.pending_series, self.tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::SeriesCache;
    use crate::config::{LakeConfig, Nulls, SortKey, SortOrder};
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

    fn snapshot_bytes<'a>(batches: impl Iterator<Item = &'a RecordBatch>) -> usize {
        let mut seen = CountedAllocations::default();
        batches
            .map(|batch| record_batch_pinned_bytes(batch, &mut seen))
            .sum()
    }

    /// Scenario: many distinct requests fill a block before its final timestamp is known.
    /// Guarantees: descriptors are already bounded sorted series runs and carry zero timestamps.
    #[test]
    fn admission_materializes_bounded_series_runs() {
        let mut cfg = LakeConfig::default();
        cfg.sorting.run_target_bytes = 64 * 1024;
        cfg.ingress.max_row_bytes = 16 * 1024;
        let mut cache = SeriesCache::new(100);
        let mut block: Block<()> = Block::new(0, 1, &cfg);
        for i in 0..64 {
            let e = extracted(&cfg, &format!("host-{i:03}"), 1);
            let r = block.reserve(&e, &mut cache, 0, &cfg).expect("reserve");
            block.admit(e, r, ()).expect("admit");
        }
        let table = block
            .tables()
            .find(|t| t.dataset().is_series())
            .expect("series");
        assert_eq!(table.rows(), 64);
        assert!(!block.is_sealed());
        for batch in table.iter_snapshots() {
            assert!(snapshot_bytes(std::iter::once(batch)) <= cfg.sorting.run_target_bytes);
            assert!(crate::sort::is_sorted(batch, &SortSpec::series()).expect("sorted"));
            let stamps = batch
                .column_by_name("emitted_at")
                .expect("stamp")
                .as_primitive::<TimestampMicrosecondType>();
            assert!((0..stamps.len()).all(|i| stamps.value(i) == 0));
        }
    }

    /// Scenario: retained series include both completed runs and a final building batch.
    /// Guarantees: the stamp transaction over the series tables adds at most
    /// eight bytes per series row plus 64 bytes of buffer slack, and shares
    /// every non-stamp column with the batch it replaces. The values tables are
    /// measured separately: finalizing their building run is an ordinary
    /// concatenate-and-sort and is not part of this bound.
    #[test]
    fn seal_peak_retained_bytes_only_adds_timestamp_values() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<()> = Block::new(0, 1, &cfg);
        for i in 0..32 {
            let e = extracted(&cfg, &format!("{}-{i}", "x".repeat(4096)), 1);
            let r = block.reserve(&e, &mut cache, 0, &cfg).expect("reserve");
            block.admit(e, r, ()).expect("admit");
        }
        let table = block.tables.get_mut(&Dataset::LogsSeries).expect("series");
        let last = table.runs.pop().expect("last run");
        table.building.push(last);
        let before: Vec<_> = block
            .tables()
            .filter(|t| t.dataset().is_series())
            .flat_map(|t| t.runs().iter().chain(t.building()))
            .cloned()
            .collect();
        let rows = block
            .tables()
            .filter(|t| t.dataset().is_series())
            .map(SortedTableBuffer::rows)
            .sum::<usize>();
        let retained = snapshot_bytes(before.iter());
        block.seal(SEAL_AT_US).expect("seal");
        // Holding every old batch keeps the complete old/new overlap resident;
        // this bounds the transaction's peak, not just its net memory change.
        let peak = snapshot_bytes(
            before.iter().chain(
                block
                    .tables()
                    .filter(|t| t.dataset().is_series())
                    .flat_map(SortedTableBuffer::iter_snapshots),
            ),
        );
        assert!(
            peak <= retained + rows * 8 + 64,
            "peak={peak}, before={retained}, rows={rows}"
        );
        let after: Vec<_> = block
            .tables()
            .filter(|t| t.dataset().is_series())
            .flat_map(SortedTableBuffer::iter_snapshots)
            .collect();
        for (old, new) in before.iter().zip(after) {
            for (index, field) in old.schema().fields().iter().enumerate() {
                if field.name() != "emitted_at" {
                    assert!(Arc::ptr_eq(old.column(index), new.column(index)));
                }
            }
        }
        assert!(block.is_sealed());
        let (_, ids, tables) = block.into_parts();
        assert_eq!(ids.len(), rows);
        assert_eq!(tables[&Dataset::LogsSeries].rows(), rows);
    }

    /// Scenario: two requests whose values batches each stay under the run
    /// target, and then the same two requests under a run target every one of
    /// them crosses.
    /// Guarantees: values batches pack across requests -- sub-target requests
    /// share one building run and become exactly one sorted run at seal, while
    /// a request that crosses the target seals its own run during admission.
    /// Sealing never leaves a values batch unsorted or unaccounted.
    #[test]
    fn values_batches_pack_across_requests_until_the_run_target() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<()> = Block::new(0, 1, &cfg);
        for host in ["a", "b"] {
            let e = extracted(&cfg, host, 4);
            let r = block.reserve(&e, &mut cache, 0, &cfg).expect("reserve");
            block.admit(e, r, ()).expect("admit");
        }
        let values = &block.tables[&Dataset::LogsValues];
        assert!(
            values.runs().is_empty(),
            "a sub-target batch is not sealed into a run during admission"
        );
        assert_eq!(
            values.building().len(),
            2,
            "both requests share one building run"
        );
        block.seal(SEAL_AT_US).expect("seal");
        let values = &block.tables[&Dataset::LogsValues];
        assert_eq!(
            values.runs().len(),
            1,
            "the seal transaction finalizes both requests into one run"
        );
        assert!(values.building().is_empty());
        assert_eq!(values.rows(), 8);
        assert!(
            crate::sort::is_sorted(&values.runs()[0], values.spec()).expect("is_sorted"),
            "the finalized run is sorted by the table's spec"
        );

        let mut tight = LakeConfig::default();
        tight.sorting.run_target_bytes = 1;
        let mut cache = SeriesCache::new(100);
        let mut block: Block<()> = Block::new(0, 1, &tight);
        for host in ["a", "b"] {
            let e = extracted(&tight, host, 1);
            let r = block.reserve(&e, &mut cache, 0, &tight).expect("reserve");
            block.admit(e, r, ()).expect("admit");
        }
        let values = &block.tables[&Dataset::LogsValues];
        assert_eq!(
            values.runs().len(),
            2,
            "each request crossed the run target on its own"
        );
        assert!(values.building().is_empty());
        block.seal(SEAL_AT_US).expect("seal");
        let values = &block.tables[&Dataset::LogsValues];
        assert_eq!(values.runs().len(), 2, "the seal adds no further run");
        assert_eq!(values.rows(), 2);
    }

    /// Scenario: an extracted descriptor lacks a configured denormalized cell.
    /// Guarantees: admission refuses malformed series content before retaining any descriptor or token.
    #[test]
    fn malformed_descriptor_fails_during_admission() {
        let mut cfg = LakeConfig::default();
        cfg.logs.denormalize.push(crate::config::Denormalize {
            path: "resource.host.id".into(),
            column: "host_col".into(),
            ty: crate::config::DenormType::String,
        });
        let mut e = extracted(&cfg, "host", 1);
        e.descriptors[0].denorm.clear();
        let mut cache = SeriesCache::new(10);
        let mut block: Block<()> = Block::new(0, 1, &cfg);
        let r = block.reserve(&e, &mut cache, 0, &cfg).expect("reserve");
        assert!(block.admit(e, r, ()).is_err());
        assert!(block.is_empty());
        assert!(!block.is_sealed());
        assert_eq!(block.request_count(), 0);
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

    /// Scenario: the second retained series batch has an invalid emitted_at type, then is repaired.
    /// Guarantees: failed sealing changes no batch or stamp; retry commits all rows with one frozen stamp.
    #[test]
    fn a_failed_seal_keeps_the_block_intact_and_a_retry_succeeds() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<()> = Block::new(0, 1, &cfg);
        for host in ["first", "second"] {
            let e = extracted(&cfg, host, 1);
            let r = block.reserve(&e, &mut cache, 0, &cfg).expect("reserve");
            block.admit(e, r, ()).expect("admit");
        }
        let table = block.tables.get_mut(&Dataset::LogsSeries).expect("table");
        let good = table.runs[1].clone();
        let index = good.schema().index_of("emitted_at").expect("field");
        let mut fields = good.schema().fields().to_vec();
        fields[index] = Arc::new(Field::new("emitted_at", DataType::Int64, false));
        let mut columns = good.columns().to_vec();
        columns[index] = Arc::new(Int64Array::from(vec![0; good.num_rows()]));
        table.runs[1] =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("bad batch");
        let before: Vec<_> = block
            .tables()
            .flat_map(SortedTableBuffer::iter_snapshots)
            .cloned()
            .collect();
        let bytes = block.bytes;
        assert!(block.seal(SEAL_AT_US).is_err());
        assert!(!block.is_sealed());
        assert_eq!(block.emitted_at_us(), None);
        assert_eq!(block.bytes, bytes);
        for (old, new) in before
            .iter()
            .zip(block.tables().flat_map(SortedTableBuffer::iter_snapshots))
        {
            assert_eq!(old, new);
        }
        block
            .tables
            .get_mut(&Dataset::LogsSeries)
            .expect("table")
            .runs[1] = good;
        block.seal(SEAL_AT_US + 1).expect("retry");
        block.seal(SEAL_AT_US + 2).expect("idempotent retry");
        assert_eq!(block.emitted_at_us(), Some(SEAL_AT_US + 1));
        assert_eq!(block.tables[&Dataset::LogsSeries].rows(), 2);
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
        let r = block
            .reserve(&first, &mut cache, 16, &cfg)
            .expect("reserve");
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

    /// Scenario: a request whose values batches carry builder slack is admitted
    /// into a block, and the block is sealed.
    /// Guarantees: the seal-time recount deduplicates shared allocations and
    /// replaces the reservation estimate, so the block's byte count is at most
    /// the sum of the per-request reservations, is exactly the measured bytes of
    /// every batch the block retains plus its pending-series and token
    /// overheads, and still counts at least one logical copy of the admitted
    /// values data rather than deduplicating it away.
    #[test]
    fn seal_recounts_shared_buffers_once() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e1 = extracted(&cfg, "h", 8);
        // The logical payload of the request, free of the builder capacity slack
        // that `pinned_bytes` also counts.
        let one_request_logical: usize = e1
            .values
            .iter()
            .flat_map(|(_, batches)| batches.iter())
            .map(|b| {
                otel_arrow_dfe_pdata::otap::memory::record_batch_logical_bytes(b)
                    .expect("logical bytes")
            })
            .sum();
        assert!(one_request_logical > 0);
        let r1 = block.reserve(&e1, &mut cache, 16, &cfg).expect("r1");
        block.admit(e1, r1, 1).expect("admit");
        let reserved = block.bytes;
        block.seal(SEAL_AT_US).expect("seal");
        assert!(
            block.bytes <= reserved,
            "the recount is never above the reservation upper bound"
        );
        let retained = snapshot_bytes(block.tables().flat_map(SortedTableBuffer::iter_snapshots));
        assert_eq!(
            block.bytes,
            retained + block.pending_series.len() * cfg.ingress.pending_series_entry_bytes + 16,
            "the recount is every retained buffer counted once, plus the pending-series and token overheads"
        );
        assert!(
            block.bytes >= one_request_logical,
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
        assert_eq!(
            second, 0,
            "the clone shares them, so nothing new is retained"
        );
        assert_eq!(buf.rows(), batch.num_rows() * 2);
    }

    /// Scenario: a buffer seals a run, then receives a batch built from the very
    /// buffers the previous run's accounting already saw.
    /// Guarantees: the dedup set is reset at each seal, so the batch is counted in
    /// full and `building_bytes` climbs towards `run_target` again. A set that
    /// survived the seal would report zero here and the next run would never seal.
    #[test]
    fn seal_resets_the_dedup_set_so_the_next_run_accounts_again() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 8);
        let (ds, batches) = e.values.into_iter().next().expect("values");
        let batch = batches.into_iter().next().expect("batch");
        let mut buf =
            SortedTableBuffer::new(ds, SortSpec::new(cfg.logs.values_sort.clone()), usize::MAX);
        let first = buf.append(batch.clone()).expect("first append");
        assert!(first > 0);
        assert_eq!(buf.building_bytes, first);
        buf.seal().expect("seal");
        assert_eq!(buf.building_bytes, 0);
        // `batch` is still alive, so its buffers keep the addresses the first
        // append recorded. Only a reset set can count them again.
        let second = buf.append(batch.clone()).expect("append after seal");
        assert_eq!(second, first, "the new run accounts the batch in full");
        assert!(buf.building_bytes > 0, "building_bytes grows again");
    }

    /// Scenario: a buffer whose run target is crossed repeatedly, over batches
    /// that are dropped once their run is sealed.
    /// Guarantees: every seal is reached, so the run count tracks the appends
    /// rather than stalling once freed addresses start being reused.
    #[test]
    fn repeated_seals_keep_firing_on_the_run_target() {
        let cfg = LakeConfig::default();
        let mut buf = SortedTableBuffer::new(
            Dataset::LogsValues,
            SortSpec::new(cfg.logs.values_sort.clone()),
            1,
        );
        for _ in 0..8 {
            let e = extracted(&cfg, "h", 4);
            let (_, batches) = e.values.into_iter().next().expect("values");
            for b in batches {
                let pinned = buf.append(b).expect("append");
                assert!(pinned > 0, "each fresh batch is accounted");
            }
            assert!(buf.building().is_empty(), "the run target sealed the batch");
        }
        assert_eq!(buf.runs().len(), 8);
        assert_eq!(buf.rows(), 32);
    }

    /// Scenario: a buffer with an empty sort spec, as configured when sorting is off.
    /// Guarantees: sealing keeps the appended batches as separate runs instead of
    /// concatenating them, and every row survives.
    #[test]
    fn unsorted_seal_keeps_batches_as_runs() {
        let cfg = LakeConfig::default();
        let e = extracted(&cfg, "h", 6);
        let (ds, batches) = e.values.into_iter().next().expect("values");
        let batch = batches.into_iter().next().expect("batch");
        let rows = batch.num_rows();
        let mut buf = SortedTableBuffer::new(ds, SortSpec::new(vec![]), usize::MAX);
        let _ = buf.append(batch.clone()).expect("append 1");
        let _ = buf.append(batch).expect("append 2");
        buf.seal().expect("seal");
        assert_eq!(
            buf.runs().len(),
            2,
            "no concatenation happens without a sort"
        );
        assert!(buf.building().is_empty());
        assert_eq!(buf.rows(), rows * 2);
        assert_eq!(
            buf.runs().iter().map(RecordBatch::num_rows).sum::<usize>(),
            rows * 2
        );
    }

    /// Scenario: `spec_for` asked for each dataset, with logs and metrics given
    /// different values sorts and with sorting switched off.
    /// Guarantees: series datasets always take the fixed series sort, values
    /// datasets take their own signal's configured keys, and disabling sorting
    /// empties the values specs without touching the series ones.
    #[test]
    fn spec_for_maps_each_dataset_to_its_configured_sort() {
        let mut cfg = LakeConfig::default();
        cfg.metrics.values_sort = vec![SortKey {
            column: "series_id".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }];
        assert_ne!(cfg.logs.values_sort, cfg.metrics.values_sort);
        let block: Block<u32> = Block::new(0, 1, &cfg);

        assert_eq!(
            block.spec_for(Dataset::LogsSeries),
            SortSpec::series(),
            "logs series takes the fixed series sort"
        );
        assert_eq!(
            block.spec_for(Dataset::MetricsSeries),
            SortSpec::series(),
            "metrics series takes the fixed series sort"
        );
        assert_eq!(
            block.spec_for(Dataset::LogsValues).keys(),
            cfg.logs.values_sort.as_slice()
        );
        for ds in [Dataset::MetricsNumber, Dataset::MetricsHistogram] {
            assert_eq!(
                block.spec_for(ds).keys(),
                cfg.metrics.values_sort.as_slice(),
                "{} follows the metrics sort",
                ds.name()
            );
        }

        let mut off = cfg.clone();
        off.sorting.enabled = false;
        let block: Block<u32> = Block::new(0, 1, &off);
        for ds in [
            Dataset::LogsValues,
            Dataset::MetricsNumber,
            Dataset::MetricsHistogram,
        ] {
            assert!(
                block.spec_for(ds).is_empty(),
                "{} is unsorted when sorting is disabled",
                ds.name()
            );
        }
        assert_eq!(block.spec_for(Dataset::LogsSeries), SortSpec::series());
    }

    /// Scenario: the runs a block actually produces, with logs values sorted by
    /// the configured keys and with sorting disabled.
    /// Guarantees: `spec_for` is the spec the runs are really sealed under, not
    /// just a value the block reports.
    #[test]
    fn block_runs_are_sealed_under_the_spec_spec_for_reports() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e = extracted(&cfg, "h", 40);
        let r = block.reserve(&e, &mut cache, 16, &cfg).expect("reserve");
        block.admit(e, r, 1).expect("admit");
        block.seal(SEAL_AT_US).expect("seal");
        for t in block.tables() {
            assert_eq!(t.spec(), &block.spec_for(t.dataset()));
            for run in t.runs() {
                assert!(
                    crate::sort::is_sorted(run, t.spec()).expect("is_sorted"),
                    "{} runs are sorted by its spec",
                    t.dataset().name()
                );
            }
        }
    }

    /// Scenario: a request offered to a block that has already been sealed.
    /// Guarantees: `admit` refuses rather than restamping the descriptor with the
    /// earlier seal time or adding estimated bytes to the exact recount.
    #[test]
    fn admit_after_seal_is_rejected() {
        let cfg = LakeConfig::default();
        let mut cache = SeriesCache::new(100);
        let mut block: Block<u32> = Block::new(0, 1, &cfg);
        let e1 = extracted(&cfg, "h", 2);
        let r1 = block.reserve(&e1, &mut cache, 16, &cfg).expect("reserve 1");
        block.admit(e1, r1, 1).expect("admit");
        block.seal(SEAL_AT_US).expect("seal");
        let sealed_bytes = block.bytes;

        let e2 = extracted(&cfg, "h2", 2);
        let r2 = block.reserve(&e2, &mut cache, 16, &cfg).expect("reserve 2");
        let err = block
            .admit(e2, r2, 2)
            .expect_err("a sealed block admits nothing");
        assert!(matches!(err, Error::Refused(RefuseReason::Invalid(_))));
        assert!(err.to_string().contains("already sealed"));
        assert_eq!(block.bytes, sealed_bytes);
        assert_eq!(block.request_count(), 1);
        assert_eq!(block.emitted_at_us(), Some(SEAL_AT_US));
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
        assert_eq!(
            buf.iter_snapshots().map(|b| b.num_rows()).sum::<usize>(),
            50
        );
        buf.seal().expect("seal");
        assert!(buf.building().is_empty());
    }
}
