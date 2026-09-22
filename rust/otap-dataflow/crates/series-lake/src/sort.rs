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
    // Every run must share one set of fields: the merge reads column `c` of every
    // run for each output column, so a narrower run would be an out-of-bounds
    // index and a differently typed one would fail `interleave`. Only the fields
    // matter, so runs that differ solely in schema-level metadata merge fine and
    // the first run's metadata is carried into the output.
    if runs.iter().any(|r| r.schema().fields() != schema.fields()) {
        return Err(Error::internal("merge runs have different schemas"));
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
}
