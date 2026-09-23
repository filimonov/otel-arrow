// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Sort specification, run sorting and k-way merge (spec sections 6.2, 6.5, 7.4).

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::compute::{SortColumn, SortOptions, interleave, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Float64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};

use crate::config::{Nulls, SortKey, SortOrder};
use crate::error::{Error, Result};

/// Ordered list of sort keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortSpec {
    keys: Vec<SortKey>,
}

impl SortSpec {
    /// Build from keys; an empty list means "no sorting".
    #[must_use]
    pub fn new(keys: Vec<SortKey>) -> Self {
        Self { keys }
    }

    /// The fixed series sort: `series_id` ascending.
    #[must_use]
    pub fn series() -> Self {
        Self::new(vec![SortKey {
            column: "series_id".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }])
    }

    /// Whether sorting is disabled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Keys.
    #[must_use]
    pub fn keys(&self) -> &[SortKey] {
        &self.keys
    }

    /// `sort_key` file metadata value.
    #[must_use]
    pub fn metadata_string(&self) -> String {
        if self.keys.is_empty() {
            return "none".into();
        }
        self.keys
            .iter()
            .map(|k| {
                format!(
                    "{}:{}:{}",
                    k.column,
                    match k.order {
                        SortOrder::Asc => "asc",
                        SortOrder::Desc => "desc",
                    },
                    match k.nulls {
                        Nulls::First => "nulls_first",
                        Nulls::Last => "nulls_last",
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn options(k: &SortKey) -> SortOptions {
        SortOptions {
            descending: k.order == SortOrder::Desc,
            nulls_first: k.nulls == Nulls::First,
        }
    }
}

/// Doubles normalized for ordering: every NaN becomes positive quiet NaN and
/// -0.0 becomes +0.0, so `total_cmp` yields the spec order.
fn normalize_key(col: &ArrayRef) -> ArrayRef {
    if col.data_type() != &DataType::Float64 {
        return col.clone();
    }
    let a = col.as_primitive::<Float64Type>();
    let out: Float64Array = arrow::compute::kernels::arity::unary(a, |v: f64| {
        if v.is_nan() {
            f64::NAN
        } else if v == 0.0 {
            0.0
        } else {
            v
        }
    });
    Arc::new(out)
}

/// Every sort column named by the spec must exist in the batch.
///
/// Checked before the short-batch early returns of [`sort_batch`] and
/// [`is_sorted`], so a misspelled sort key is reported for a one-row batch just
/// as it is for a large one, rather than silently passing.
fn check_columns(batch: &RecordBatch, spec: &SortSpec) -> Result<()> {
    for k in &spec.keys {
        if batch.column_by_name(&k.column).is_none() {
            return Err(Error::internal(format!("sort column {} missing", k.column)));
        }
    }
    Ok(())
}

fn sort_columns(batch: &RecordBatch, spec: &SortSpec) -> Result<Vec<SortColumn>> {
    spec.keys
        .iter()
        .map(|k| {
            let col = batch
                .column_by_name(&k.column)
                .ok_or_else(|| Error::internal(format!("sort column {} missing", k.column)))?;
            Ok(SortColumn {
                values: normalize_key(col),
                options: Some(SortSpec::options(k)),
            })
        })
        .collect()
}

/// Sort one batch by the spec. Returns the input unchanged for an empty spec.
///
/// Not stable: arrow's `lexsort_to_indices` is an unstable sort, so the relative
/// order of rows whose sort keys are equal is unspecified. Nothing in the format
/// depends on it.
pub fn sort_batch(batch: &RecordBatch, spec: &SortSpec) -> Result<RecordBatch> {
    if spec.is_empty() {
        return Ok(batch.clone());
    }
    check_columns(batch, spec)?;
    if batch.num_rows() < 2 {
        return Ok(batch.clone());
    }
    let cols = sort_columns(batch, spec)?;
    let indices = lexsort_to_indices(&cols, None)?;
    let taken: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c, &indices, None))
        .collect::<std::result::Result<_, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), taken)?)
}

/// Most rows one merge step encodes keys for, or pops from the merge heap,
/// before it returns.
///
/// A step is the unit of work the sink runs between two returns to its
/// runtime, so this and [`MERGE_STEP_KEY_BYTES`] bound how long a merge holds
/// the thread it runs on. Neither affects which rows a chunk holds or their
/// order: the merge produces the same chunks however its work is sliced.
pub const MERGE_STEP_ROWS: usize = 8192;

/// Most encoded key bytes one merge step builds or copies before it returns.
///
/// Encoding and copying keys costs time in proportion to their bytes, so a
/// wide key, such as a log body, gets proportionally fewer rows per step.
pub const MERGE_STEP_KEY_BYTES: usize = 1 << 20;

/// Rows the first key slice of a merge encodes, before any key width is
/// known.
const FIRST_KEY_SLICE_ROWS: usize = 256;

/// Fewest rows a key slice encodes, whatever the key width.
const MIN_KEY_SLICE_ROWS: usize = 16;

/// How much work one merge step may do.
#[derive(Debug, Clone, Copy)]
struct StepBudget {
    rows: usize,
    key_bytes: usize,
}

impl StepBudget {
    const DEFAULT: Self = Self {
        rows: MERGE_STEP_ROWS,
        key_bytes: MERGE_STEP_KEY_BYTES,
    };

    /// Rows of the next key slice, given the average encoded key width seen
    /// so far.
    fn key_slice_rows(self, average_key_bytes: Option<usize>) -> usize {
        let rows = match average_key_bytes {
            None => FIRST_KEY_SLICE_ROWS,
            Some(width) => self.key_bytes / width.max(1),
        };
        rows.clamp(MIN_KEY_SLICE_ROWS.min(self.rows), self.rows)
    }
}

struct HeapItem {
    row: OwnedRow,
    run: usize,
    /// Row index within the run.
    idx: usize,
    /// Key segment of the run the row's key sits in, and its offset there.
    seg: usize,
    off: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, o: &Self) -> bool {
        self.row.as_ref() == o.row.as_ref() && self.run == o.run
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for HeapItem {
    // Reversed so BinaryHeap pops the smallest row; ties broken by run index so
    // that equal rows leave the merge in run order.
    fn cmp(&self, o: &Self) -> Ordering {
        o.row
            .as_ref()
            .cmp(self.row.as_ref())
            .then(o.run.cmp(&self.run))
    }
}

/// Row converter for the sort keys of a schema.
fn key_converter(schema: &Schema, spec: &SortSpec) -> Result<RowConverter> {
    let fields: Vec<SortField> = spec
        .keys
        .iter()
        .map(|k| {
            let dt = schema.field_with_name(&k.column)?.data_type().clone();
            Ok(SortField::new_with_options(dt, SortSpec::options(k)))
        })
        .collect::<std::result::Result<_, arrow::error::ArrowError>>()?;
    Ok(RowConverter::new(fields)?)
}

/// Encode a batch's normalized sort-key columns into comparable rows.
fn key_rows(batch: &RecordBatch, spec: &SortSpec, converter: &RowConverter) -> Result<Rows> {
    let cols: Vec<ArrayRef> = sort_columns(batch, spec)?
        .into_iter()
        .map(|c| c.values)
        .collect();
    Ok(converter.convert_columns(&cols)?)
}

/// Average pinned bytes per row over every run, deduplicating shared buffers
/// (tests; the build keeps the same totals as it goes).
#[cfg(test)]
fn avg_row_bytes(runs: &[RecordBatch]) -> usize {
    let mut seen = CountedAllocations::default();
    let mut bytes = 0usize;
    let mut rows = 0usize;
    for r in runs {
        bytes += record_batch_pinned_bytes(r, &mut seen);
        rows += r.num_rows();
    }
    if rows == 0 {
        return 1;
    }
    (bytes / rows).max(1)
}

/// A merge whose sort keys are still being encoded, one bounded slice per
/// [`MergeBuild::step`].
///
/// The keys of every row of every run must exist before the first row can
/// leave the merge, and encoding them is linear in the rows and in the key
/// width. Built in one call, that is one uninterrupted stretch as long as the
/// table is large; built here, it is a sequence of slices of at most
/// [`MERGE_STEP_ROWS`] rows and about [`MERGE_STEP_KEY_BYTES`] encoded bytes,
/// between which a caller on an asynchronous runtime can return to it.
///
/// Each run's keys are kept as the segments the slices produced. Every
/// segment is encoded by the one converter of the merge, so a row's encoded
/// key, and therefore every comparison and the whole output, is the same as
/// if the run had been encoded in one call.
pub struct MergeBuild {
    runs: Vec<RecordBatch>,
    schema: SchemaRef,
    spec: SortSpec,
    converter: Option<RowConverter>,
    keys: Vec<Vec<Rows>>,
    chunk_bytes: usize,
    /// The run being encoded and the first row of it not yet encoded.
    run: usize,
    offset: usize,
    /// Encoded key bytes and rows so far, for the next slice's width.
    encoded_bytes: usize,
    encoded_rows: usize,
    budget: StepBudget,
    sorted: bool,
    /// Totals kept as the slices are encoded, so that neither reporting the
    /// keys nor finishing the build scans them again: the heap every run's
    /// first row enters once encoded, the heap bytes of every segment, the
    /// longest encoded key, and the pinned bytes and rows of the runs begun
    /// so far, which set the chunk's row count.
    heap: BinaryHeap<HeapItem>,
    key_bytes: usize,
    longest_key: usize,
    pinned_bytes: usize,
    pinned_rows: usize,
    seen: CountedAllocations,
}

impl MergeBuild {
    /// Validate the runs and prepare the merge, encoding no key yet.
    ///
    /// Runs without rows are dropped. With an empty spec the merge is the
    /// unsorted pass-through and there is nothing to encode.
    ///
    /// # Errors
    ///
    /// Refuses runs whose fields differ, and a spec naming a column the
    /// runs do not have.
    pub fn new(runs: Vec<RecordBatch>, spec: &SortSpec, chunk_bytes: usize) -> Result<Self> {
        let runs: Vec<RecordBatch> = runs.into_iter().filter(|r| r.num_rows() > 0).collect();
        let schema = match runs.first() {
            Some(first) => first.schema(),
            None => Arc::new(Schema::empty()),
        };
        // Every run must share one set of fields: the merge reads column `c` of every
        // run for each output column, so a narrower run would be an out-of-bounds
        // index and a differently typed one would fail `interleave`. Only the fields
        // matter, so runs that differ solely in schema-level metadata merge fine and
        // the first run's metadata is carried into the output.
        if runs.iter().any(|r| r.schema().fields() != schema.fields()) {
            return Err(Error::internal("merge runs have different schemas"));
        }
        let sorted = !runs.is_empty() && !spec.is_empty();
        let converter = if sorted {
            Some(key_converter(&schema, spec)?)
        } else {
            None
        };
        let keys = if sorted {
            runs.iter().map(|_| Vec::new()).collect()
        } else {
            Vec::new()
        };
        Ok(Self {
            runs,
            schema,
            spec: spec.clone(),
            converter,
            keys,
            chunk_bytes,
            run: 0,
            offset: 0,
            encoded_bytes: 0,
            encoded_rows: 0,
            budget: StepBudget::DEFAULT,
            sorted,
            heap: BinaryHeap::new(),
            key_bytes: 0,
            longest_key: 0,
            pinned_bytes: 0,
            pinned_rows: 0,
            seen: CountedAllocations::default(),
        })
    }

    /// The same build, sliced by a different budget (tests).
    #[cfg(test)]
    fn with_budget(self, rows: usize, key_bytes: usize) -> Self {
        Self {
            budget: StepBudget { rows, key_bytes },
            ..self
        }
    }

    /// Whether every key has been encoded.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        !self.sorted || self.run >= self.runs.len()
    }

    /// Encode the next keys, one slice per run or part of a run, until the
    /// step's budget of rows and encoded bytes is spent. Returns whether
    /// every key is now encoded; a complete build does nothing.
    ///
    /// Many small runs, such as a series table's, share one step, so a small
    /// table is not charged a return to the runtime per run.
    ///
    /// # Errors
    ///
    /// Returns the Arrow failure of encoding a slice.
    pub fn step(&mut self) -> Result<bool> {
        let (mut rows_done, mut bytes_done) = (0usize, 0usize);
        while !self.is_complete()
            && rows_done < self.budget.rows
            && bytes_done < self.budget.key_bytes
        {
            let converter = self
                .converter
                .as_ref()
                .ok_or_else(|| Error::internal("sorted merge without a converter"))?;
            let run = &self.runs[self.run];
            let average = (self.encoded_rows > 0).then(|| self.encoded_bytes / self.encoded_rows);
            let rows = self
                .budget
                .key_slice_rows(average)
                .min(self.budget.rows - rows_done)
                .min(run.num_rows() - self.offset);
            if self.offset == 0 {
                self.pinned_bytes += record_batch_pinned_bytes(run, &mut self.seen);
                self.pinned_rows += run.num_rows();
            }
            let slice = run.slice(self.offset, rows);
            let encoded = key_rows(&slice, &self.spec, converter)?;
            let (mut bytes, mut longest) = (0usize, 0usize);
            for length in encoded.lengths() {
                bytes += length;
                longest = longest.max(length);
            }
            self.encoded_bytes += bytes;
            self.encoded_rows += rows;
            self.key_bytes += encoded.size();
            self.longest_key = self.longest_key.max(longest);
            rows_done += rows;
            bytes_done += bytes;
            if self.offset == 0 {
                self.heap.push(HeapItem {
                    row: encoded.row(0).owned(),
                    run: self.run,
                    idx: 0,
                    seg: 0,
                    off: 0,
                });
            }
            self.keys[self.run].push(encoded);
            self.offset += rows;
            if self.offset >= run.num_rows() {
                self.run += 1;
                self.offset = 0;
            }
        }
        Ok(self.is_complete())
    }

    /// Heap the keys encoded so far hold beside the runs. Kept as a running
    /// total, so asking costs nothing.
    #[must_use]
    pub fn resident_key_bytes(&self) -> usize {
        self.key_bytes
    }

    /// Seed the merge heap and hand over the merge, encoding whatever keys
    /// are still missing first.
    ///
    /// # Errors
    ///
    /// Returns the Arrow failure of encoding a remaining slice.
    pub fn finish(mut self) -> Result<MergeIter> {
        while !self.step()? {}
        // Everything below was kept up to date by the slices, so this is
        // constant work however large the table: the heap was seeded run by
        // run, and the chunk row count comes from the running pinned totals,
        // deduplicated across runs exactly as a single pass would.
        let rows_per_chunk = if self.sorted {
            let average = self
                .pinned_bytes
                .checked_div(self.pinned_rows)
                .map_or(1, |average| average.max(1));
            (self.chunk_bytes / average).max(1)
        } else {
            1
        };
        Ok(MergeIter {
            runs: self.runs,
            schema: self.schema,
            keys: self.keys,
            heap: self.heap,
            key_bytes: self.key_bytes,
            longest_key: self.longest_key,
            rows_per_chunk,
            next_run: 0,
            sorted: self.sorted,
            budget: self.budget,
            pending: Vec::new(),
            columns: Vec::new(),
            parts: Vec::new(),
            part_offset: 0,
            last_step_rows: 0,
        })
    }
}

/// What one [`MergeIter::step`] produced.
#[derive(Debug)]
pub enum MergeStep {
    /// Bounded work was done towards the next chunk; step again.
    Progress,
    /// The next output chunk.
    Chunk(RecordBatch),
    /// The merge is exhausted.
    Done,
}

/// Lazy k-way merge: one output chunk is materialized per `next()`.
///
/// Memory: the iterator holds the input runs, the encoded sort keys of every row
/// of every run, and one output chunk. The encoded keys cover the key columns
/// only, not the whole dataset, so they are a small fraction of the runs
/// themselves; they must stay resident because the heap compares rows from any
/// run at any point of the merge.
///
/// Ledger (approximation): the chunk row count is fixed up front from the
/// average pinned bytes per row over all runs, so a chunk whose rows happen to
/// be wider than average exceeds `chunk_bytes`. The overshoot is bounded by
/// `max_row_bytes` per row, and `max_row_bytes <= run_target_bytes / 4` is
/// validated at startup. Exact byte-driven chunking is a plan 3 follow-up.
///
/// A chunk can be produced in bounded steps through [`MergeIter::step`]: a
/// step pops heap rows and interleaves output columns until it has done
/// about [`MERGE_STEP_ROWS`] rows of work or copied
/// [`MERGE_STEP_KEY_BYTES`] of keys. `next()` runs the same steps back to
/// back, so both produce exactly the same chunks.
pub struct MergeIter {
    runs: Vec<RecordBatch>,
    schema: SchemaRef,
    /// Sorted mode: encoded keys per run, as segments, plus the merge heap.
    keys: Vec<Vec<Rows>>,
    heap: BinaryHeap<HeapItem>,
    /// Heap bytes of every key segment, a total kept by the build.
    key_bytes: usize,
    /// The longest encoded key of any row of any run: the most one heap
    /// entry's owned key row can ever hold.
    longest_key: usize,
    rows_per_chunk: usize,
    /// Unsorted mode: index of the next run to hand out unchanged.
    next_run: usize,
    sorted: bool,
    budget: StepBudget,
    /// Sorted mode: the rows popped for the chunk being produced.
    pending: Vec<(usize, usize)>,
    /// Sorted mode: the chunk's columns interleaved so far.
    columns: Vec<ArrayRef>,
    /// Sorted mode: the column being interleaved, as the row ranges done so
    /// far when the chunk has more rows than one step's budget, and the
    /// first row of `pending` it has not reached yet.
    parts: Vec<ArrayRef>,
    part_offset: usize,
    /// Rows the last step processed: popped plus interleaved.
    last_step_rows: usize,
}

impl MergeIter {
    /// Heap bytes the merge holds beside its input runs, as a bound that
    /// holds for the iterator's whole life.
    ///
    /// Every run's encoded sort keys, allocated when the merge is built and
    /// released only when the iterator is dropped, plus the merge heap and
    /// the key row each heap entry owns. The owned rows change as the merge
    /// advances and variable-width keys differ in length, so each of the at
    /// most one entry per run is charged the longest key of any row: the
    /// value never falls below what is resident at any point of the merge,
    /// however far it has advanced. The runs themselves are not counted:
    /// the block they came from already accounts for them. Zero in unsorted
    /// mode, which encodes no keys.
    #[must_use]
    pub fn resident_key_bytes(&self) -> usize {
        let entries = self.heap.capacity().max(self.keys.len());
        self.key_bytes + entries * size_of::<HeapItem>() + self.keys.len() * self.longest_key
    }

    /// Heap the chunk being produced holds right now: the popped row
    /// indices and the columns interleaved so far.
    #[must_use]
    pub fn chunk_workspace_bytes(&self) -> usize {
        self.pending.capacity() * size_of::<(usize, usize)>()
            + self
                .columns
                .iter()
                .chain(self.parts.iter())
                .map(|column| column.get_array_memory_size())
                .sum::<usize>()
    }

    /// What the owned key rows and the merge heap hold right now (tests).
    #[cfg(test)]
    fn resident_key_bytes_now(&self) -> usize {
        self.keys.iter().flatten().map(Rows::size).sum::<usize>()
            + self.heap.capacity() * size_of::<HeapItem>()
            + self
                .heap
                .iter()
                .map(|item| item.row.as_ref().as_ref().len())
                .sum::<usize>()
    }

    /// The same merge, stepped by a different budget (tests).
    #[cfg(test)]
    fn with_budget(self, rows: usize, key_bytes: usize) -> Self {
        Self {
            budget: StepBudget { rows, key_bytes },
            ..self
        }
    }

    /// Do one bounded step of producing the next chunk.
    ///
    /// # Errors
    ///
    /// Returns the Arrow failure of interleaving a column or assembling the
    /// chunk.
    pub fn step(&mut self) -> Result<MergeStep> {
        if !self.sorted {
            // Unsorted mode: hand out the runs in arrival order, unchanged. No
            // concatenation, so no second copy of the dataset is ever built.
            let Some(run) = self.runs.get(self.next_run) else {
                return Ok(MergeStep::Done);
            };
            self.next_run += 1;
            return Ok(MergeStep::Chunk(run.clone()));
        }
        // One step pops rows and interleaves columns until it has processed
        // its budget of rows: a popped row counts one and so does every row
        // of a column interleaved. A column of a chunk larger than the budget
        // is interleaved one row range per step and its ranges concatenated
        // when the last one is done, which yields the same array as one
        // interleave over every row. A small chunk is produced in a single
        // step.
        let mut work = 0usize;
        let mut copied = 0usize;
        let result = loop {
            if self.columns.is_empty() && self.parts.is_empty() && !self.popped_enough() {
                let (rows, bytes) = self.pop_slice(self.budget.rows - work);
                work += rows;
                copied += bytes;
                if self.pending.is_empty() {
                    break MergeStep::Done;
                }
                if work >= self.budget.rows || copied >= self.budget.key_bytes {
                    break MergeStep::Progress;
                }
                continue;
            }
            if self.pending.is_empty() {
                break MergeStep::Done;
            }
            let c = self.columns.len();
            let arrays: Vec<&dyn Array> = self.runs.iter().map(|r| r.column(c).as_ref()).collect();
            let start = self.part_offset;
            let end = self
                .pending
                .len()
                .min(start + (self.budget.rows - work).max(1));
            if start == 0 && end == self.pending.len() {
                self.columns.push(interleave(&arrays, &self.pending)?);
            } else {
                self.parts
                    .push(interleave(&arrays, &self.pending[start..end])?);
                self.part_offset = end;
                if end == self.pending.len() {
                    let parts: Vec<&dyn Array> = self.parts.iter().map(AsRef::as_ref).collect();
                    let column = arrow::compute::concat(&parts)?;
                    self.parts.clear();
                    self.part_offset = 0;
                    self.columns.push(column);
                }
            }
            work += end - start;
            if self.columns.len() == self.schema.fields().len() {
                let columns = std::mem::take(&mut self.columns);
                self.pending.clear();
                break MergeStep::Chunk(RecordBatch::try_new(self.schema.clone(), columns)?);
            }
            if work >= self.budget.rows {
                break MergeStep::Progress;
            }
        };
        self.last_step_rows = work;
        Ok(result)
    }

    /// Whether the rows popped so far complete a chunk: the chunk's row
    /// count is reached or the heap is exhausted.
    fn popped_enough(&self) -> bool {
        self.pending.len() >= self.rows_per_chunk || self.heap.is_empty()
    }

    /// Pop at most `max_rows` more rows of the chunk being produced, and at
    /// most about the budget's key bytes; returns the rows popped and the
    /// key bytes copied.
    fn pop_slice(&mut self, max_rows: usize) -> (usize, usize) {
        if self.pending.capacity() == 0 {
            self.pending.reserve_exact(self.rows_per_chunk);
        }
        let mut rows = 0usize;
        let mut copied = 0usize;
        while rows < max_rows && copied < self.budget.key_bytes {
            let Some(item) = self.heap.pop() else { break };
            self.pending.push((item.run, item.idx));
            rows += 1;
            let segments = &self.keys[item.run];
            let (seg, off) = if item.off + 1 < segments[item.seg].num_rows() {
                (item.seg, item.off + 1)
            } else {
                (item.seg + 1, 0)
            };
            if let Some(segment) = segments.get(seg) {
                let row = segment.row(off);
                copied += row.as_ref().len();
                self.heap.push(HeapItem {
                    row: row.owned(),
                    run: item.run,
                    idx: item.idx + 1,
                    seg,
                    off,
                });
            }
            if self.pending.len() >= self.rows_per_chunk {
                break;
            }
        }
        (rows, copied)
    }
}

impl Iterator for MergeIter {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.step() {
                Ok(MergeStep::Progress) => {}
                Ok(MergeStep::Chunk(chunk)) => return Some(Ok(chunk)),
                Ok(MergeStep::Done) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Merge sorted runs into globally sorted chunks of about `chunk_bytes`.
///
/// The result is an iterator: only one output chunk exists at a time, so the
/// caller (the sink) can hand each chunk to the Parquet writer and drop it. With
/// an empty spec the runs are yielded unchanged, in arrival order.
///
/// Every key is encoded before this returns; [`MergeBuild`] builds the same
/// merge in bounded slices.
///
/// # Errors
///
/// As [`MergeBuild::new`] and [`MergeBuild::finish`].
pub fn merge_runs(
    runs: Vec<RecordBatch>,
    spec: &SortSpec,
    chunk_bytes: usize,
) -> Result<MergeIter> {
    MergeBuild::new(runs, spec, chunk_bytes)?.finish()
}

/// Whether a batch is sorted by the spec (test helper, also used by the oracle).
///
/// Adjacent rows are compared through their encoded `arrow::row` keys, which
/// carry the spec's ascending/descending and null placement. Comparing the
/// permutation produced by `lexsort_to_indices` would be wrong: that sort is
/// unstable, so tied keys can yield a non-identity permutation for a batch that
/// is correctly ordered.
pub fn is_sorted(batch: &RecordBatch, spec: &SortSpec) -> Result<bool> {
    if spec.is_empty() {
        return Ok(true);
    }
    check_columns(batch, spec)?;
    if batch.num_rows() < 2 {
        return Ok(true);
    }
    let converter = key_converter(&batch.schema(), spec)?;
    let rows = key_rows(batch, spec, &converter)?;
    Ok(rows.iter().is_sorted())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Nulls, SortKey, SortOrder};
    use arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Negative quiet NaN: `total_cmp` orders it below every other double, so a
    /// missing NaN normalization moves it to the front of an ascending sort.
    const NEG_NAN: u64 = 0xFFF8_0000_0000_0000;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, false),
            Field::new("tag", DataType::Utf8, false),
        ])
    }

    fn batch_tagged(keys: Vec<Option<i64>>, f: Vec<f64>, tags: Vec<&str>) -> RecordBatch {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(f)),
            Arc::new(StringArray::from(tags)),
        ];
        RecordBatch::try_new(Arc::new(test_schema()), cols).expect("batch")
    }

    fn batch(keys: Vec<Option<i64>>, f: Vec<f64>, tag: &str) -> RecordBatch {
        let n = keys.len();
        batch_tagged(keys, f, vec![tag; n])
    }

    fn spec() -> SortSpec {
        SortSpec::new(vec![
            SortKey {
                column: "k".into(),
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
            SortKey {
                column: "f".into(),
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
        ])
    }

    /// `k` ascending, `f` ascending, then `tag` ascending as a tie-break.
    fn spec_with_tag_tiebreak() -> SortSpec {
        let mut keys = spec().keys().to_vec();
        keys.push(SortKey {
            column: "tag".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        });
        SortSpec::new(keys)
    }

    fn one_key_spec(order: SortOrder, nulls: Nulls) -> SortSpec {
        SortSpec::new(vec![SortKey {
            column: "k".into(),
            order,
            nulls,
        }])
    }

    fn keys_of(b: &RecordBatch) -> Vec<Option<i64>> {
        b.column(0).as_primitive::<Int64Type>().iter().collect()
    }

    fn bits_of(b: &RecordBatch) -> Vec<u64> {
        let f = b.column(1).as_primitive::<Float64Type>();
        (0..b.num_rows()).map(|i| f.value(i).to_bits()).collect()
    }

    fn tags_of(b: &RecordBatch) -> Vec<String> {
        let t = b.column(2).as_string::<i32>();
        (0..b.num_rows()).map(|i| t.value(i).to_string()).collect()
    }

    fn strings(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    fn merged(runs: Vec<RecordBatch>, spec: &SortSpec, chunk_bytes: usize) -> Vec<RecordBatch> {
        merge_runs(runs, spec, chunk_bytes)
            .expect("merge")
            .collect::<Result<_>>()
            .expect("chunks")
    }

    fn concat(chunks: &[RecordBatch]) -> RecordBatch {
        arrow::compute::concat_batches(&chunks[0].schema(), chunks).expect("concat")
    }

    /// Scenario: one key group holds both zero signs and both NaN signs, with the
    /// tag column as the tie-breaking third sort key; each zero and each NaN is
    /// tagged so that the tag order is the opposite of the raw `total_cmp` bit
    /// order.
    /// Guarantees: -0.0 compares equal to +0.0 and every NaN compares equal to
    /// every other, so the tag alone decides those pairs. Dropping either
    /// normalization flips both pairs and fails the assertion. Nulls sort last and
    /// the payload keeps its original bit patterns, since only the sort columns
    /// are normalized.
    #[test]
    fn sort_batch_normalizes_doubles() {
        let b = batch_tagged(
            vec![Some(2), None, Some(1), Some(1), Some(1), Some(1), Some(1)],
            vec![0.0, 0.0, f64::from_bits(NEG_NAN), 0.0, 1.0, f64::NAN, -0.0],
            vec!["x", "y", "n2", "a", "z", "n1", "b"],
        );
        let s = sort_batch(&b, &spec_with_tag_tiebreak()).expect("sort");
        assert_eq!(
            keys_of(&s),
            vec![Some(1), Some(1), Some(1), Some(1), Some(1), Some(2), None]
        );
        // +0.0 is tagged "a" and -0.0 is tagged "b"; raw bits would put -0.0
        // first. Both NaNs likewise: the negative one is tagged "n2".
        assert_eq!(tags_of(&s), strings(&["a", "b", "z", "n1", "n2", "x", "y"]));
        assert_eq!(
            bits_of(&s),
            vec![
                0.0f64.to_bits(),
                (-0.0f64).to_bits(),
                1.0f64.to_bits(),
                f64::NAN.to_bits(),
                NEG_NAN,
                0.0f64.to_bits(),
                0.0f64.to_bits(),
            ],
            "the payload keeps the bits it came in with"
        );
    }

    /// Scenario: every combination of sort direction and null placement over one
    /// key column that contains a null.
    /// Guarantees: `sort_batch` produces the order the combination names,
    /// `is_sorted` accepts that order, and it rejects the exact reverse.
    #[test]
    fn desc_and_nulls_first_orderings_round_trip() {
        let input = batch(vec![Some(1), None, Some(3), Some(2)], vec![0.0; 4], "x");
        let cases = [
            (
                SortOrder::Desc,
                Nulls::First,
                vec![None, Some(3), Some(2), Some(1)],
            ),
            (
                SortOrder::Desc,
                Nulls::Last,
                vec![Some(3), Some(2), Some(1), None],
            ),
            (
                SortOrder::Asc,
                Nulls::First,
                vec![None, Some(1), Some(2), Some(3)],
            ),
            (
                SortOrder::Asc,
                Nulls::Last,
                vec![Some(1), Some(2), Some(3), None],
            ),
        ];
        for (order, nulls, expected) in cases {
            let s = one_key_spec(order, nulls);
            let sorted = sort_batch(&input, &s).expect("sort");
            assert_eq!(keys_of(&sorted), expected, "{order:?}/{nulls:?}");
            assert!(
                is_sorted(&sorted, &s).expect("sorted"),
                "{order:?}/{nulls:?}"
            );
            let mut reversed = expected;
            reversed.reverse();
            let backwards = batch(reversed, vec![0.0; 4], "x");
            assert!(
                !is_sorted(&backwards, &s).expect("reverse"),
                "{order:?}/{nulls:?} must reject the reverse order"
            );
        }
    }

    /// Scenario: two runs sorted descending with nulls first are merged.
    /// Guarantees: the merge honours the descending direction and the null
    /// placement, not just the ascending default, across run boundaries.
    #[test]
    fn desc_nulls_first_merge_is_globally_sorted() {
        let s = one_key_spec(SortOrder::Desc, Nulls::First);
        let r1 =
            sort_batch(&batch(vec![Some(5), None, Some(1)], vec![0.0; 3], "a"), &s).expect("s");
        let r2 = sort_batch(&batch(vec![Some(4), Some(9)], vec![0.0; 2], "b"), &s).expect("s");
        assert_eq!(keys_of(&r1), vec![None, Some(5), Some(1)]);
        assert_eq!(keys_of(&r2), vec![Some(9), Some(4)]);
        let all = concat(&merged(vec![r1, r2], &s, 1));
        assert_eq!(
            keys_of(&all),
            vec![None, Some(9), Some(5), Some(4), Some(1)]
        );
        assert!(is_sorted(&all, &s).expect("check"));
    }

    /// Scenario: three sorted runs merged in 2-row chunks.
    /// Guarantees: the concatenation of chunks is globally sorted and contains every row exactly once.
    #[test]
    fn merge_runs_is_globally_sorted() {
        let r1 = sort_batch(
            &batch(vec![Some(5), Some(1), Some(9)], vec![0.0; 3], "a"),
            &spec(),
        )
        .expect("s");
        let r2 = sort_batch(&batch(vec![Some(2), Some(8)], vec![0.0; 2], "b"), &spec()).expect("s");
        let r3 = sort_batch(
            &batch(vec![Some(3), None, Some(4)], vec![0.0; 3], "c"),
            &spec(),
        )
        .expect("s");
        let out: Vec<RecordBatch> = merge_runs(vec![r1, r2, r3], &spec(), 1)
            .expect("merge")
            .collect::<Result<_>>()
            .expect("chunks");
        assert!(
            out.len() >= 4,
            "tiny chunk budget must yield several chunks"
        );
        let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
        assert_eq!(all.num_rows(), 8);
        assert!(is_sorted(&all, &spec()).expect("check"));
        let k = all.column(0).as_primitive::<Int64Type>();
        assert_eq!(
            k.iter().collect::<Vec<_>>(),
            vec![
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(8),
                Some(9),
                None
            ]
        );
    }

    /// Scenario: the same key value appears several times within one run and
    /// again in two other runs, with every row carrying a distinct tag.
    /// Guarantees: the merge emits each input row exactly once, keeps tied rows
    /// of one run in that run's own order, and orders whole runs among ties by
    /// run index. Both a one-row chunk budget and a whole-merge budget behave the
    /// same, so ties are handled across chunk boundaries too.
    #[test]
    fn merge_preserves_run_local_order_among_tied_keys() {
        let runs = || {
            vec![
                batch_tagged(
                    vec![Some(1), Some(7), Some(7)],
                    vec![0.0; 3],
                    vec!["r0k1", "r0k7a", "r0k7b"],
                ),
                batch_tagged(vec![Some(7), Some(9)], vec![0.0; 2], vec!["r1k7", "r1k9"]),
                batch_tagged(vec![Some(7)], vec![0.0], vec!["r2k7"]),
            ]
        };
        for r in runs() {
            assert!(is_sorted(&r, &spec()).expect("run"), "runs start sorted");
        }
        for chunk_bytes in [1usize, 1 << 20] {
            let all = concat(&merged(runs(), &spec(), chunk_bytes));
            assert_eq!(all.num_rows(), 6, "{chunk_bytes}");
            assert!(is_sorted(&all, &spec()).expect("check"), "{chunk_bytes}");
            assert_eq!(
                keys_of(&all),
                vec![Some(1), Some(7), Some(7), Some(7), Some(7), Some(9)],
                "{chunk_bytes}"
            );
            assert_eq!(
                tags_of(&all),
                strings(&["r0k1", "r0k7a", "r0k7b", "r1k7", "r2k7", "r1k9"]),
                "{chunk_bytes}"
            );
        }
    }

    /// Scenario: the same three runs merged with a one-row chunk budget and then
    /// with a budget sized for exactly three rows.
    /// Guarantees: chunks are filled to the budget-derived row count and no chunk
    /// exceeds it, so the three-row budget really splits the 8-row merge into
    /// 3/3/2 rather than trivially fitting everything. In both cases every row
    /// keeps its own non-key payload, so the output is a permutation of the input
    /// rows and not merely of the sort keys.
    #[test]
    fn merge_chunks_are_bounded_and_carry_payloads() {
        let runs = || {
            vec![
                sort_batch(
                    &batch_tagged(
                        vec![Some(5), Some(1), Some(9)],
                        vec![0.0; 3],
                        vec!["a5", "a1", "a9"],
                    ),
                    &spec(),
                )
                .expect("s"),
                sort_batch(
                    &batch_tagged(vec![Some(2), Some(8)], vec![0.0; 2], vec!["b2", "b8"]),
                    &spec(),
                )
                .expect("s"),
                sort_batch(
                    &batch_tagged(
                        vec![Some(3), None, Some(4)],
                        vec![0.0; 3],
                        vec!["c3", "cn", "c4"],
                    ),
                    &spec(),
                )
                .expect("s"),
            ]
        };
        let expected_keys = vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(8),
            Some(9),
            None,
        ];
        let expected_tags = strings(&["a1", "b2", "c3", "c4", "a5", "b8", "a9", "cn"]);
        // A budget of three average rows must split the 8-row merge, which is
        // what makes the upper bound below worth asserting.
        let three_rows = avg_row_bytes(&runs()) * 3;
        for (chunk_bytes, expected_chunks) in [(1usize, vec![1; 8]), (three_rows, vec![3, 3, 2])] {
            let out = merged(runs(), &spec(), chunk_bytes);
            assert_eq!(
                out.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                expected_chunks,
                "{chunk_bytes}"
            );
            let all = concat(&out);
            assert_eq!(keys_of(&all), expected_keys, "{chunk_bytes}");
            assert_eq!(tags_of(&all), expected_tags, "{chunk_bytes}");
        }
    }

    /// Scenario: no runs at all, and a single run that carries no rows.
    /// Guarantees: the merge iterator is empty in both cases rather than
    /// producing an empty chunk or failing.
    #[test]
    fn merge_of_no_runs_yields_no_chunks() {
        assert!(merged(vec![], &spec(), 1 << 20).is_empty());
        assert!(
            merged(vec![batch(vec![], vec![], "x")], &spec(), 1 << 20).is_empty(),
            "a run with no rows contributes nothing"
        );
        assert!(
            merged(vec![], &SortSpec::new(vec![]), 1 << 20).is_empty(),
            "the same holds with sorting disabled"
        );
    }

    /// Scenario: a single already sorted run is handed to the merge.
    /// Guarantees: its rows come back in their original order with their
    /// payloads, under both a one-row and a whole-run chunk budget.
    #[test]
    fn merge_of_a_single_run_yields_it_unchanged() {
        let r = batch_tagged(
            vec![Some(1), Some(4), Some(9)],
            vec![0.0; 3],
            vec!["a", "b", "c"],
        );
        for chunk_bytes in [1usize, 1 << 20] {
            let all = concat(&merged(vec![r.clone()], &spec(), chunk_bytes));
            assert_eq!(
                keys_of(&all),
                vec![Some(1), Some(4), Some(9)],
                "{chunk_bytes}"
            );
            assert_eq!(tags_of(&all), strings(&["a", "b", "c"]), "{chunk_bytes}");
        }
    }

    /// Scenario: runs with no rows are interleaved with runs that have rows.
    /// Guarantees: the empty runs drop out and the output holds exactly the rows
    /// of the non-empty ones, globally sorted.
    #[test]
    fn merge_skips_runs_with_no_rows() {
        let runs = vec![
            batch(vec![], vec![], "empty"),
            batch_tagged(vec![Some(2), Some(6)], vec![0.0; 2], vec!["a", "b"]),
            batch(vec![], vec![], "empty"),
            batch_tagged(vec![Some(4)], vec![0.0], vec!["c"]),
        ];
        let all = concat(&merged(runs, &spec(), 1 << 20));
        assert_eq!(keys_of(&all), vec![Some(2), Some(4), Some(6)]);
        assert_eq!(tags_of(&all), strings(&["a", "c", "b"]));
    }

    /// Scenario: runs whose schemas differ are handed to the merge.
    /// Guarantees: the merge refuses up front with an invalid-content error rather
    /// than indexing past the end of a narrower run while materializing a chunk.
    #[test]
    fn mismatched_run_schemas_are_refused() {
        let wide = batch(vec![Some(1)], vec![0.0], "a");
        let narrow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(2)])) as ArrayRef],
        )
        .expect("narrow batch");
        let Err(err) = merge_runs(vec![wide, narrow], &spec(), 1 << 20) else {
            panic!("schema mismatch must be refused");
        };
        assert!(err.to_string().contains("different schemas"));
    }

    /// Scenario: two runs with identical fields but different schema-level
    /// metadata.
    /// Guarantees: the guard looks at the fields only, so the runs merge, and the
    /// first run's metadata is what the output carries.
    #[test]
    fn runs_differing_only_in_schema_metadata_merge() {
        let plain = batch_tagged(vec![Some(2)], vec![0.0], vec!["a"]);
        let with_meta = Arc::new(
            test_schema()
                .with_metadata(HashMap::from([("origin".to_string(), "run2".to_string())])),
        );
        let tagged = RecordBatch::try_new(
            with_meta,
            vec![
                Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
                Arc::new(Float64Array::from(vec![0.0])) as ArrayRef,
                Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
            ],
        )
        .expect("metadata batch");
        let out = merged(vec![plain, tagged], &spec(), 1 << 20);
        let all = concat(&out);
        assert_eq!(keys_of(&all), vec![Some(1), Some(2)]);
        assert_eq!(tags_of(&all), strings(&["b", "a"]));
        assert!(
            all.schema().metadata().is_empty(),
            "the first run's schema metadata is the output's"
        );
    }

    /// Scenario: two sorted runs of five rows in total, merged by an Int64 and
    /// a Float64 key, are consumed chunk by chunk.
    /// Guarantees: the merge reports every row's encoded key -- one null byte
    /// plus eight value bytes per key column -- and the row offsets as
    /// resident from the moment it is built until the last chunk, because the
    /// keys are dropped only with the iterator; unsorted mode encodes no keys
    /// and reports zero.
    #[test]
    fn merge_reports_the_keys_it_keeps_resident() {
        let r1 = sort_batch(
            &batch(vec![Some(5), Some(1), Some(9)], vec![0.0; 3], "a"),
            &spec(),
        )
        .expect("s");
        let r2 = sort_batch(&batch(vec![Some(2), Some(8)], vec![0.0; 2], "b"), &spec()).expect("s");
        let mut merge = merge_runs(vec![r1.clone(), r2.clone()], &spec(), 1).expect("merge");
        let encoded_rows = 5 * 2 * (1 + 8);
        let offsets = (3 + 1 + 2 + 1) * size_of::<usize>();
        let built = merge.resident_key_bytes();
        assert!(
            built >= encoded_rows + offsets,
            "{built} bytes reported for {encoded_rows} key bytes and {offsets} offset bytes"
        );
        let mut rows = 0;
        while let Some(chunk) = merge.next() {
            rows += chunk.expect("chunk").num_rows();
            assert!(
                merge.resident_key_bytes() >= encoded_rows + offsets,
                "the keys of every run stay resident until the iterator is dropped"
            );
        }
        assert_eq!(rows, 5);
        let unsorted = merge_runs(vec![r1, r2], &SortSpec::new(vec![]), 1).expect("merge");
        assert_eq!(unsorted.resident_key_bytes(), 0);
    }

    /// Scenario: two runs keyed by a string whose values grow from one byte
    /// to several hundred bytes as the merge advances, consumed one row at a
    /// time.
    /// Guarantees: the reported resident key bytes, taken once when the merge
    /// is built, are never below what the merge actually holds at any later
    /// point, although every owned heap row is replaced by a longer one.
    #[test]
    fn resident_key_bytes_bound_keys_that_grow_during_the_merge() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let run = |width_step: usize, tag: char| {
            let values: Vec<String> = (0..20)
                .map(|i| format!("{:03}{}", i, tag.to_string().repeat(1 + i * width_step)))
                .collect();
            let array: ArrayRef = Arc::new(StringArray::from(values));
            RecordBatch::try_new(schema.clone(), vec![array]).expect("batch")
        };
        let spec = SortSpec::new(vec![SortKey {
            column: "k".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        let mut merge = merge_runs(vec![run(20, 'a'), run(30, 'b')], &spec, 1).expect("merge");
        let reported = merge.resident_key_bytes();
        let first = merge.resident_key_bytes_now();
        let mut largest = first;
        while let Some(chunk) = merge.next() {
            let _ = chunk.expect("chunk");
            let now = merge.resident_key_bytes_now();
            largest = largest.max(now);
            assert!(
                now <= reported,
                "{now} resident bytes above the reported {reported}"
            );
            assert_eq!(
                merge.resident_key_bytes(),
                reported,
                "the bound does not move"
            );
        }
        assert!(largest > first, "the owned key rows grew during the merge");
    }

    /// Scenario: an empty spec (sorting disabled).
    /// Guarantees: merge concatenates runs in arrival order.
    #[test]
    fn empty_spec_concatenates() {
        let r1 = batch(vec![Some(5)], vec![0.0], "a");
        let r2 = batch(vec![Some(1)], vec![0.0], "b");
        let out: Vec<RecordBatch> = merge_runs(vec![r1, r2], &SortSpec::new(vec![]), 1 << 20)
            .expect("merge")
            .collect::<Result<_>>()
            .expect("chunks");
        assert_eq!(
            out.len(),
            2,
            "unsorted mode yields the runs as they are, without concatenating"
        );
        let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
        assert_eq!(all.column(0).as_primitive::<Int64Type>().values(), &[5, 1]);
        assert_eq!(SortSpec::new(vec![]).metadata_string(), "none");
        assert_eq!(
            spec().metadata_string(),
            "k:asc:nulls_last,f:asc:nulls_last"
        );
    }

    /// Scenario: a correctly ordered batch in which every sort key is tied, and a
    /// batch that is genuinely out of order.
    /// Guarantees: `is_sorted` accepts the tied batch and rejects the unordered one.
    /// A permutation-based check would reject the tied batch, because arrow's
    /// lexicographic sort is unstable and need not return the identity there.
    #[test]
    fn is_sorted_accepts_tied_keys() {
        let tied = batch(vec![Some(7); 6], vec![1.0; 6], "x");
        assert!(is_sorted(&tied, &spec()).expect("tied"));
        let descending = batch(vec![Some(3), Some(2), Some(1)], vec![0.0; 3], "x");
        assert!(!is_sorted(&descending, &spec()).expect("descending"));
        let ordered = sort_batch(&descending, &spec()).expect("sort");
        assert!(is_sorted(&ordered, &spec()).expect("ordered"));
    }

    /// Scenario: a one-row batch sorted by a misspelled sort column, which the
    /// short-batch early return would otherwise skip straight past.
    /// Guarantees: both `sort_batch` and `is_sorted` name the missing column, and
    /// a one-row batch whose keys do exist is accepted unchanged.
    #[test]
    fn missing_sort_column_is_refused_even_on_short_batches() {
        let one = batch(vec![Some(1)], vec![0.0], "x");
        let bad = SortSpec::new(vec![SortKey {
            column: "kk".into(),
            order: SortOrder::Asc,
            nulls: Nulls::Last,
        }]);
        let Err(err) = sort_batch(&one, &bad) else {
            panic!("sort_batch must refuse a missing sort column");
        };
        assert!(err.to_string().contains("sort column kk missing"));
        let Err(err) = is_sorted(&one, &bad) else {
            panic!("is_sorted must refuse a missing sort column");
        };
        assert!(err.to_string().contains("sort column kk missing"));
        assert_eq!(
            keys_of(&sort_batch(&one, &spec()).expect("sort")),
            vec![Some(1)]
        );
        assert!(is_sorted(&one, &spec()).expect("one row"));
    }

    /// Runs keyed by an Int64 `k` with ties and nulls and a Utf8 `s` of
    /// widths from 1 to about 90 bytes, each carrying a distinct tag, each
    /// sorted by `k` ascending, nulls last, then `s` descending.
    fn tie_heavy_runs() -> (Vec<RecordBatch>, SortSpec) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
        ]));
        let spec = SortSpec::new(vec![
            SortKey {
                column: "k".into(),
                order: SortOrder::Asc,
                nulls: Nulls::Last,
            },
            SortKey {
                column: "s".into(),
                order: SortOrder::Desc,
                nulls: Nulls::Last,
            },
        ]);
        let runs = (0..4)
            .map(|run| {
                let n = 37 + run * 11;
                let k: Vec<Option<i64>> = (0..n)
                    .map(|i| (i % 9 != 4).then_some(((i * 7 + run * 3) % 5) as i64))
                    .collect();
                let s: Vec<String> = (0..n)
                    .map(|i| "x".repeat(1 + (i * 13 + run) % 90) + &format!("{}", i % 3))
                    .collect();
                let tag: Vec<String> = (0..n).map(|i| format!("r{run}i{i}")).collect();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(k)) as ArrayRef,
                        Arc::new(StringArray::from(s)) as ArrayRef,
                        Arc::new(StringArray::from(tag)) as ArrayRef,
                    ],
                )
                .expect("batch");
                sort_batch(&batch, &spec).expect("sort")
            })
            .collect();
        (runs, spec)
    }

    /// The tags of every run's rows in the order a stable sort of the runs'
    /// concatenation, in run order, by the keys of `tie_heavy_runs` gives.
    fn stable_sort_tags(runs: &[RecordBatch]) -> Vec<String> {
        let mut rows: Vec<(Option<i64>, String, String)> = Vec::new();
        for run in runs {
            let k = run.column(0).as_primitive::<Int64Type>();
            let s = run.column(1).as_string::<i32>();
            let t = run.column(2).as_string::<i32>();
            for i in 0..run.num_rows() {
                rows.push((
                    k.is_valid(i).then(|| k.value(i)),
                    s.value(i).to_string(),
                    t.value(i).to_string(),
                ));
            }
        }
        rows.sort_by(|a, b| {
            let k = match (a.0, b.0) {
                (Some(x), Some(y)) => x.cmp(&y),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            k.then_with(|| b.1.cmp(&a.1))
        });
        rows.into_iter().map(|(_, _, tag)| tag).collect()
    }

    /// Scenario: tie-heavy runs with variable-width string keys are merged
    /// with the default budget and, through `MergeBuild` and `MergeIter::step`,
    /// with budgets from one row and one key byte per step upwards, and with
    /// a chunk budget small enough for several chunks.
    /// Guarantees: however the key encoding and the heap pops are sliced,
    /// every chunk holds the same rows in the same order as the unsliced
    /// merge, which is exactly the stable sort of the runs in run order, and
    /// a small budget really does slice the work into many steps.
    #[test]
    fn sliced_merge_matches_the_unsliced_merge_and_a_stable_sort() {
        let (runs, spec) = tie_heavy_runs();
        let chunk_bytes = avg_row_bytes(&runs) * 23;
        let unsliced = merged(runs.clone(), &spec, chunk_bytes);
        let expected = stable_sort_tags(&runs);
        assert_eq!(tags_of(&concat(&unsliced)), expected);
        assert!(unsliced.len() > 3, "{} chunks", unsliced.len());
        for (rows, key_bytes) in [(1, 1), (2, 7), (5, 64), (16, 1 << 20), (1 << 20, 1 << 30)] {
            let mut build = MergeBuild::new(runs.clone(), &spec, chunk_bytes)
                .expect("build")
                .with_budget(rows, key_bytes);
            let mut build_steps = 0usize;
            let mut resident = 0usize;
            while !build.step().expect("key slice") {
                build_steps += 1;
                assert!(build.resident_key_bytes() >= resident, "keys only grow");
                resident = build.resident_key_bytes();
            }
            let mut merge = build.finish().expect("finish").with_budget(rows, key_bytes);
            let mut chunks = Vec::new();
            let mut steps = 0usize;
            loop {
                steps += 1;
                match merge.step().expect("step") {
                    MergeStep::Progress => {}
                    MergeStep::Chunk(chunk) => chunks.push(chunk),
                    MergeStep::Done => break,
                }
            }
            assert_eq!(
                chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                unsliced
                    .iter()
                    .map(RecordBatch::num_rows)
                    .collect::<Vec<_>>(),
                "chunk boundaries with {rows} rows / {key_bytes} bytes per step"
            );
            for (got, want) in chunks.iter().zip(&unsliced) {
                assert_eq!(
                    got, want,
                    "chunk content with {rows} rows / {key_bytes} bytes"
                );
            }
            if rows <= 16 {
                assert!(build_steps > runs.len(), "{build_steps} key slices");
                assert!(steps > chunks.len() * 2, "{steps} merge steps");
            }
        }
    }

    /// Scenario: the tie-heavy runs, 214 rows in one chunk, merged with a
    /// budget of three rows per step.
    /// Guarantees: no step does more than its budget of rows, counted as the
    /// rows actually processed: a key slice encodes at most three rows, and
    /// a merge step pops or interleaves at most three rows in all, so one
    /// column of a chunk larger than the budget is interleaved across
    /// several steps rather than in one.
    #[test]
    fn one_merge_step_does_bounded_work() {
        let (runs, spec) = tie_heavy_runs();
        let mut build = MergeBuild::new(runs.clone(), &spec, 1 << 20)
            .expect("build")
            .with_budget(3, 1 << 20);
        let mut encoded = 0usize;
        while !build.step().expect("slice") {
            let now = build
                .keys
                .iter()
                .flatten()
                .map(Rows::num_rows)
                .sum::<usize>();
            assert!(
                now - encoded <= 3,
                "{} rows in one key slice",
                now - encoded
            );
            encoded = now;
        }
        let mut merge = build.finish().expect("finish").with_budget(3, 1 << 20);
        let (mut chunks, mut steps) = (0usize, 0usize);
        loop {
            let step = merge.step().expect("step");
            steps += 1;
            assert!(
                merge.last_step_rows <= 3,
                "{} rows processed in one step",
                merge.last_step_rows
            );
            match step {
                MergeStep::Chunk(chunk) => {
                    assert_eq!(chunk.num_rows(), 214);
                    chunks += 1;
                }
                MergeStep::Done => break,
                MergeStep::Progress => {}
            }
        }
        assert_eq!(chunks, 1);
        assert!(
            steps > 214 * 3 / 3,
            "{steps} steps for 214 rows of 3 columns"
        );
    }

    /// Scenario: the tie-heavy runs' keys encoded two rows per step.
    /// Guarantees: the build keeps its totals as it goes -- the resident key
    /// bytes, the longest key and the heap, seeded with each run's first row
    /// as soon as that row is encoded -- so neither reporting the keys after
    /// a slice nor finishing the build scans every key again; the totals
    /// always equal a full recount.
    #[test]
    fn the_build_keeps_its_totals_as_it_goes() {
        let (runs, spec) = tie_heavy_runs();
        let mut build = MergeBuild::new(runs.clone(), &spec, 1 << 20)
            .expect("build")
            .with_budget(2, 1 << 20);
        loop {
            let done = build.step().expect("slice");
            let segments: Vec<&Rows> = build.keys.iter().flatten().collect();
            assert_eq!(
                build.resident_key_bytes(),
                segments.iter().map(|rows| rows.size()).sum::<usize>()
            );
            assert_eq!(
                build.longest_key,
                segments
                    .iter()
                    .flat_map(|rows| rows.lengths())
                    .max()
                    .unwrap_or(0)
            );
            assert_eq!(
                build.heap.len(),
                build.keys.iter().filter(|run| !run.is_empty()).count(),
                "every run whose first row is encoded is in the heap"
            );
            if done {
                break;
            }
        }
        let expected = merged(runs.clone(), &spec, 1 << 20);
        let merge = build.finish().expect("finish");
        assert_eq!(merge.heap.len(), runs.len());
        let got: Vec<RecordBatch> = merge.collect::<Result<_>>().expect("chunks");
        assert_eq!(got, expected);
    }

    /// Scenario: 100 one-row runs, the shape of a series table, merged with
    /// the default budget.
    /// Guarantees: a step carries work across runs and columns until its
    /// budget is spent, so all 100 runs' keys are encoded in one step and
    /// the whole chunk is popped and interleaved in one more: a small table
    /// costs its caller two returns to the runtime, not one per run and per
    /// column.
    #[test]
    fn a_small_merge_takes_one_step_per_phase() {
        let runs: Vec<RecordBatch> = (0..100)
            .map(|i| batch_tagged(vec![Some(100 - i)], vec![0.0], vec!["t"]))
            .collect();
        let mut build = MergeBuild::new(runs, &spec(), 1 << 20).expect("build");
        assert!(build.step().expect("keys"), "every key in one step");
        let mut merge = build.finish().expect("finish");
        let MergeStep::Chunk(chunk) = merge.step().expect("step") else {
            panic!("the whole chunk in one step");
        };
        assert_eq!(chunk.num_rows(), 100);
        assert_eq!(keys_of(&chunk)[0], Some(1));
        assert!(matches!(merge.step().expect("step"), MergeStep::Done));
    }
}
