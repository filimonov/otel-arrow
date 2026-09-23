# series-lake

Engine-independent core of the series Parquet exporter: canonical series
identity, extraction of `series` and values datasets from OTAP Arrow
records, a bounded series cache, sorted block buffers and a Parquet sink
over `object_store`.

The storage format is specified in [docs/FORMAT.md](docs/FORMAT.md). The
design is in `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md`
at the repository root.

This crate is engine-independent: it never depends on the Dataflow engine.
The `core-nodes` exporter `exporter:series_parquet` adapts it into a Dataflow
pipeline; see its
[README](../core-nodes/src/exporters/series_parquet_exporter/README.md).

## Modules

- `canonical`: identity encoding v1 and `series_id`.
- `extract`: OTAP records to descriptors and values batches.
- `cache`: bounded LRU of series ids to their last committed partition.
- `buffer`: sorted run buffers and the block accounting model.
- `sort`: sort keys, double normalization, k-way merge.
- `sink`: Parquet files on an `object_store`, Hive layout, frozen names.
- `clock`: wall clock trait and aligned window boundaries.

## Producer id contract

Every writer that shares a lake must set `producer_id_attribute` to the same
resource attribute, and that attribute must be present and stable on every
request. It stays part of the series identity and is additionally projected
into the `producer_id` column of every values row, so that a reader can tell
which producer a row came from without joining the `series` dataset. A
request whose resource lacks the attribute gets an empty `producer_id`, which
is a distinct producer as far as readers are concerned.

`writer_id` is a different thing: it identifies the writer process in file
names and file metadata and is never part of the identity.

## Limitations in version 1

Descriptors become bounded series runs during request admission. Final sealing
replaces only the emitted_at column of those runs with the block timestamp,
sharing every other column; old/new timestamp storage is at most eight
additional bytes per series row. A values dataset additionally finalizes
whatever is still buffered into one run bounded by run_target_bytes. The
first successful seal timestamp remains fixed across flush retries.

- Exponential histograms and summaries are not stored (`unsupported` decides
  between rejecting the request and dropping the points; drops are counted
  one per dropped data point row).
- Exemplars are not stored; their rows are counted as dropped, and their own
  attributes are never read.
- Metric metadata attributes and exemplar attribute payloads are never read,
  so duplicate keys inside them are not detected. Duplicate-key rejection
  only covers the lists this writer decodes: resource, scope, the log
  record's own attributes, and a supported data point's attributes.
- `attrs` maps are lossy: values are rendered to strings, so a string `"42"`
  and an integer `42` look the same in the map. They are still different
  series. Bytes values render as padded standard base64 and non-finite
  doubles as `"NaN"`, `"Infinity"` and `"-Infinity"`, as in OTLP JSON; a
  dedicated `body_bytes` binary column for the log body is deferred to a
  later format version.
- A cancellation that lands after a file's Parquet finalization has begun
  (for example, exporter shutdown) can leave an orphaned multipart upload;
  it is reclaimed by a bucket lifecycle rule, not by this crate.
- A merge holds the encoded sort keys of every row of the table it is
  merging, so sorting by a wide column such as `body` can hold close to a
  second copy of the table's payload.
- `merge_chunk_bytes` is an average-based approximation: a chunk of rows
  much wider than the table's average overshoots it, and with sorting
  disabled it is ignored altogether -- runs go to the writer as they are,
  each at most `run_target_bytes`.
- A histogram `sum` of zero is stored as null when no point of the same
  request has a non-zero sum: the OTAP transport omits a column whose every
  entry is the type default, so an absent sum and a zero sum arrive the same
  way. The same holds for any optional metrics column.
- One Parquet row group can start several multipart upload parts at once
  whatever `upload.concurrency` says; the burst is bounded by
  `parquet.row_group_bytes`.
- Number and histogram points share one `metrics/values` dataset, so half the
  rows of a mixed stream leave `value_double` null and the other half leave
  `count`, `sum`, `min` and `max` null. Parquet min/max statistics on those
  columns are correspondingly less selective than they were when each point
  kind had its own file. The merge exists to keep a mixed metrics stream at
  one PUT request per window per signal.
- Traces are refused.

## Reading the data

DuckDB:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE VIEW logs_values AS
  SELECT * FROM read_parquet('s3://bucket/v=1/signal=logs/dataset=values/**/*.parquet',
                             hive_partitioning = true, union_by_name = true);
CREATE VIEW logs_series AS
  SELECT * FROM read_parquet('s3://bucket/v=1/signal=logs/dataset=series/**/*.parquet',
                             hive_partitioning = true, union_by_name = true,
                             filename = true);
-- one descriptor per series: the newest wins, ties broken by filename
CREATE VIEW logs_series_latest AS
  SELECT * EXCLUDE (rn, filename) FROM (
    SELECT *, row_number() OVER
      (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) AS rn
    FROM logs_series) WHERE rn = 1;
SELECT v.time, s.resource_attrs, v.body
FROM logs_values v JOIN logs_series_latest s USING (series_id)
WHERE v.date = '2026-09-21';
```

Spark:

```python
from pyspark.sql import Window
from pyspark.sql.functions import col, input_file_name, row_number

values = (spark.read.option("mergeSchema", "true")
          .parquet("s3a://bucket/v=1/signal=logs/dataset=values/"))
series = (spark.read.option("mergeSchema", "true")
          .parquet("s3a://bucket/v=1/signal=logs/dataset=series/")
          .withColumn("_file", input_file_name()))
# one descriptor per series: the newest wins, ties broken by file name
latest = (series.withColumn("rn", row_number().over(
              Window.partitionBy("series_id")
                    .orderBy(col("emitted_at").desc(), col("_file").desc())))
          .filter("rn = 1").drop("rn", "_file"))
values.join(latest, "series_id").select("time", "resource_attrs", "body")
```

Metrics read the same way, against one values dataset: number and histogram
points share `signal=metrics/dataset=values`, and the point kind comes from
`metric_type` in the descriptor that the join already supplies.

```sql
CREATE VIEW metrics_values AS
  SELECT * FROM read_parquet('s3://bucket/v=1/signal=metrics/dataset=values/**/*.parquet',
                             hive_partitioning = true, union_by_name = true);
CREATE VIEW metrics_series AS
  SELECT * FROM read_parquet('s3://bucket/v=1/signal=metrics/dataset=series/**/*.parquet',
                             hive_partitioning = true, union_by_name = true,
                             filename = true);
CREATE VIEW metrics_series_latest AS
  SELECT * EXCLUDE (rn, filename) FROM (
    SELECT *, row_number() OVER
      (PARTITION BY series_id ORDER BY emitted_at DESC, filename DESC) AS rn
    FROM metrics_series) WHERE rn = 1;
-- gauges and counters
SELECT v.time, s.metric_name, coalesce(v.value_double, v.value_int) AS value
FROM metrics_values v JOIN metrics_series_latest s USING (series_id)
WHERE s.metric_type IN ('gauge', 'sum');
-- histograms
SELECT v.time, s.metric_name, v.count, v.sum, v.bucket_counts, v.explicit_bounds
FROM metrics_values v JOIN metrics_series_latest s USING (series_id)
WHERE s.metric_type = 'histogram';
```

The descriptor's `metric_type` is the only supported way to tell the two point
kinds apart. Do not classify a row by which columns are null: a null list and
an empty list are distinguishable in DuckDB but not in ClickHouse, which has
no nullable `Array` and reads a null Parquet list as `[]`.

Both recipes need `union_by_name` / `mergeSchema` because denormalized columns
may be added over time. To detect an incompatible mix, compare the
`schema_fingerprint` key (16 lowercase hex digits) in each file's Parquet
metadata: files of one dataset with different fingerprints disagree on the
column set, a column's type or nullability, or column order, and a reader
that ignores this silently drops or misreads columns. See `docs/FORMAT.md` for exactly
what the fingerprint covers.

## Testing

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-series-lake
```

The reference-oracle property test in `tests/oracle.rs` is the main
correctness test; `tests/golden.rs` and `tests/golden_roundtrip.rs` check the
identity encoding against vectors produced by an independent Python
implementation, both directly and through the real OTLP-to-OTAP conversion.
