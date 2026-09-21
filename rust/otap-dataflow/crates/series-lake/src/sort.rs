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

fn sort_columns(batch: &RecordBatch, spec: &SortSpec) -> Result<Vec<SortColumn>> {
    spec.keys
        .iter()
        .map(|k| {
            let col = batch
                .column_by_name(&k.column)
                .ok_or_else(|| Error::invalid(format!("sort column {} missing", k.column)))?;
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
    if spec.is_empty() || batch.num_rows() < 2 {
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

struct HeapItem {
    row: OwnedRow,
    run: usize,
    idx: usize,
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

/// Average pinned bytes per row over every run, deduplicating shared buffers.
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
pub struct MergeIter {
    runs: Vec<RecordBatch>,
    schema: SchemaRef,
    /// Sorted mode: encoded keys per run, plus the merge heap.
    keys: Vec<Rows>,
    heap: BinaryHeap<HeapItem>,
    rows_per_chunk: usize,
    /// Unsorted mode: index of the next run to hand out unchanged.
    next_run: usize,
    sorted: bool,
}

impl MergeIter {
    fn interleave(&self, pending: &[(usize, usize)]) -> Result<RecordBatch> {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for c in 0..self.schema.fields().len() {
            let arrays: Vec<&dyn Array> = self.runs.iter().map(|r| r.column(c).as_ref()).collect();
            cols.push(interleave(&arrays, pending)?);
        }
        Ok(RecordBatch::try_new(self.schema.clone(), cols)?)
    }
}

impl Iterator for MergeIter {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.sorted {
            // Unsorted mode: hand out the runs in arrival order, unchanged. No
            // concatenation, so no second copy of the dataset is ever built.
            let run = self.runs.get(self.next_run)?;
            self.next_run += 1;
            return Some(Ok(run.clone()));
        }
        let mut pending: Vec<(usize, usize)> = Vec::with_capacity(self.rows_per_chunk);
        while let Some(item) = self.heap.pop() {
            pending.push((item.run, item.idx));
            let next = item.idx + 1;
            if next < self.keys[item.run].num_rows() {
                self.heap.push(HeapItem {
                    row: self.keys[item.run].row(next).owned(),
                    run: item.run,
                    idx: next,
                });
            }
            if pending.len() >= self.rows_per_chunk {
                break;
            }
        }
        if pending.is_empty() {
            return None;
        }
        Some(self.interleave(&pending))
    }
}

/// Merge sorted runs into globally sorted chunks of about `chunk_bytes`.
///
/// The result is an iterator: only one output chunk exists at a time, so the
/// caller (the sink) can hand each chunk to the Parquet writer and drop it. With
/// an empty spec the runs are yielded unchanged, in arrival order.
pub fn merge_runs(
    runs: Vec<RecordBatch>,
    spec: &SortSpec,
    chunk_bytes: usize,
) -> Result<MergeIter> {
    let runs: Vec<RecordBatch> = runs.into_iter().filter(|r| r.num_rows() > 0).collect();
    let Some(first) = runs.first() else {
        return Ok(MergeIter {
            runs: Vec::new(),
            schema: Arc::new(Schema::empty()),
            keys: Vec::new(),
            heap: BinaryHeap::new(),
            rows_per_chunk: 1,
            next_run: 0,
            sorted: false,
        });
    };
    let schema = first.schema();
    // Every run must share one schema: the merge reads column `c` of every run
    // for each output column, so a narrower run would be an out-of-bounds index.
    if runs.iter().any(|r| r.schema() != schema) {
        return Err(Error::invalid("merge runs have different schemas"));
    }
    if spec.is_empty() {
        return Ok(MergeIter {
            runs,
            schema,
            keys: Vec::new(),
            heap: BinaryHeap::new(),
            rows_per_chunk: 1,
            next_run: 0,
            sorted: false,
        });
    }
    let converter = key_converter(&schema, spec)?;
    let mut keys = Vec::with_capacity(runs.len());
    for r in &runs {
        keys.push(key_rows(r, spec, &converter)?);
    }
    let mut heap = BinaryHeap::new();
    for (run, rows) in keys.iter().enumerate() {
        heap.push(HeapItem {
            row: rows.row(0).owned(),
            run,
            idx: 0,
        });
    }
    let rows_per_chunk = (chunk_bytes / avg_row_bytes(&runs)).max(1);
    Ok(MergeIter {
        runs,
        schema,
        keys,
        heap,
        rows_per_chunk,
        next_run: 0,
        sorted: true,
    })
}

/// Whether a batch is sorted by the spec (test helper, also used by the oracle).
///
/// Adjacent rows are compared through their encoded `arrow::row` keys, which
/// carry the spec's ascending/descending and null placement. Comparing the
/// permutation produced by `lexsort_to_indices` would be wrong: that sort is
/// unstable, so tied keys can yield a non-identity permutation for a batch that
/// is correctly ordered.
pub fn is_sorted(batch: &RecordBatch, spec: &SortSpec) -> Result<bool> {
    if spec.is_empty() || batch.num_rows() < 2 {
        return Ok(true);
    }
    let converter = key_converter(&batch.schema(), spec)?;
    let rows = key_rows(batch, spec, &converter)?;
    Ok((1..rows.num_rows()).all(|i| rows.row(i - 1) <= rows.row(i)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Nulls, SortKey, SortOrder};
    use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(keys: Vec<Option<i64>>, f: Vec<f64>, tag: &str) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("f", DataType::Float64, false),
            Field::new("tag", DataType::Utf8, false),
        ]);
        let n = keys.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Float64Array::from(f)),
            Arc::new(StringArray::from(vec![tag; n])),
        ];
        RecordBatch::try_new(Arc::new(schema), cols).expect("batch")
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

    /// Scenario: keys with a null, both NaN signs and both zero signs.
    /// Guarantees: null last, NaNs after all numbers, and -0.0 compares equal to +0.0
    /// so the pair stays adjacent; their relative order is unspecified and unasserted.
    #[test]
    fn sort_batch_normalizes_doubles() {
        let b = batch(
            vec![Some(2), None, Some(1), Some(1), Some(1), Some(1)],
            vec![
                0.0,
                0.0,
                f64::from_bits(0xFFF8_0000_0000_0000),
                1.0,
                -0.0,
                f64::NAN,
            ],
            "x",
        );
        let s = sort_batch(&b, &spec()).expect("sort");
        let k = s.column(0).as_primitive::<Int64Type>();
        let f = s.column(1).as_primitive::<Float64Type>();
        assert_eq!(
            k.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(1), Some(1), Some(2), None]
        );
        assert_eq!(f.value(0).to_bits(), (-0.0f64).to_bits()); // original values preserved
        assert_eq!(f.value(1), 1.0);
        assert!(f.value(2).is_nan() && f.value(3).is_nan());
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

    /// Scenario: the same three runs merged first with a one-row chunk budget and
    /// then with a budget that fits the whole merge.
    /// Guarantees: no chunk exceeds the row budget the budget implies, and in both
    /// cases every row keeps its own non-key payload, so the output is a
    /// permutation of the input rows and not merely of the sort keys.
    #[test]
    fn merge_chunks_are_bounded_and_carry_payloads() {
        let runs = || {
            vec![
                sort_batch(
                    &batch(vec![Some(5), Some(1), Some(9)], vec![0.0; 3], "a"),
                    &spec(),
                )
                .expect("s"),
                sort_batch(&batch(vec![Some(2), Some(8)], vec![0.0; 2], "b"), &spec()).expect("s"),
                sort_batch(
                    &batch(vec![Some(3), None, Some(4)], vec![0.0; 3], "c"),
                    &spec(),
                )
                .expect("s"),
            ]
        };
        let expected: Vec<(Option<i64>, &str)> = vec![
            (Some(1), "a"),
            (Some(2), "b"),
            (Some(3), "c"),
            (Some(4), "c"),
            (Some(5), "a"),
            (Some(8), "b"),
            (Some(9), "a"),
            (None, "c"),
        ];
        for (chunk_bytes, max_rows) in [(1usize, 1usize), (1 << 20, 8)] {
            let out: Vec<RecordBatch> = merge_runs(runs(), &spec(), chunk_bytes)
                .expect("merge")
                .collect::<Result<_>>()
                .expect("chunks");
            assert!(
                out.iter()
                    .all(|c| c.num_rows() >= 1 && c.num_rows() <= max_rows),
                "every chunk stays within the {max_rows}-row budget"
            );
            let all = arrow::compute::concat_batches(&out[0].schema(), &out).expect("concat");
            let k = all.column(0).as_primitive::<Int64Type>();
            let tag = all.column(2).as_string::<i32>();
            let got: Vec<(Option<i64>, &str)> = (0..all.num_rows())
                .map(|i| (k.is_valid(i).then(|| k.value(i)), tag.value(i)))
                .collect();
            assert_eq!(got, expected);
        }
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
}
