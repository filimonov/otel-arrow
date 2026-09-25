# series-lake

Engine-independent core of the series Parquet exporter: canonical series
identity, extraction of `series` and values datasets from OTAP Arrow
records, a bounded series cache, sorted block buffers and a Parquet sink
over `object_store`.

The storage format is specified in [docs/FORMAT.md](docs/FORMAT.md).

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
resource attribute, and every producer must send that attribute on every
request with a value that is stable for the producer and unique to it. It
stays part of the series identity and is also projected into the
`producer_id` column of every values row, so a reader can tell which producer
a row came from without joining the `series` dataset. Two producers sending
the same value are one producer to readers, and a request whose resource
lacks the attribute gets an empty `producer_id`.

`writer_id` is a different thing: it identifies the writer process in file
names and file metadata and is never part of the identity.

## Limitations in version 1

Format-level limitations (unsupported points, exemplars, lossy attribute maps,
a zero `sum` read as null, traces) are listed in
[FORMAT.md](docs/FORMAT.md#limitations-of-version-1). The writer adds these:

- A failed or cancelled write aborts its multipart upload within
  `upload.abort_timeout`. After a completion was sent, a HEAD first tells
  whether the object exists: one that exists counts as written unless the
  write was cancelled, and the upload is aborted either way, an abort answered
  `NotFound` showing this completion committed it
  (`FlushReport::probed_commits`). A HEAD that fails otherwise leaves the
  upload alone and reports it as a possible orphan. A completion still in
  flight may be applied after an abort. An abort that fails or
  times out, and a CreateMultipartUpload that fails without a definite answer
  (the store may hold an upload whose id the writer never received), are
  reported as `TransientError::AbortFailed` naming the object key; the
  leftovers are reclaimed by a bucket lifecycle rule, not by this crate.
- A merge holds the encoded sort keys of every row of the table it is
  merging, so sorting by a wide column such as `body` can hold close to a
  second copy of the table's payload.
- `merge_chunk_bytes` is an approximation from the table's mean row width, so
  a chunk of much wider rows overshoots it. With sorting disabled it is
  ignored, and runs go to the writer as they are, each at most
  `run_target_bytes`.
- Sealing replaces only the `emitted_at` column of the series runs, at most
  eight additional bytes per series row, and keeps the first seal timestamp
  across flush retries. A values dataset finalizes what is still buffered
  into one more run, so the seal transient can reach
  `(V + 1) * run_target_bytes` for V buffered runs.
- One Parquet row group can start several multipart upload parts at once
  whatever `upload.concurrency` says: the object store receives the whole
  encoded row group. The burst is bounded by `parquet.row_group_bytes`.
- Number and histogram points share one `metrics/values` dataset, so each
  kind leaves the other's columns null and their Parquet min/max statistics
  are less selective.

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

ClickHouse (`clickhouse-local` or the `file()`/`s3()` table functions) has no
`QUALIFY`, breaks ties on the virtual `_path` column, and does not synthesize
`date` and `hour` from the path:

```sql
WITH canonical AS (
    SELECT * FROM (
        SELECT *, row_number() OVER (
            PARTITION BY series_id ORDER BY emitted_at DESC, _path DESC) AS rank
        FROM file('v=1/signal=logs/dataset=series/**/*.parquet', 'Parquet')
    ) WHERE rank = 1
)
SELECT v.*, s.resource_attrs
FROM file('v=1/signal=logs/dataset=values/**/*.parquet', 'Parquet') AS v
INNER JOIN canonical AS s ON v.series_id = s.series_id;
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

The recipes use `union_by_name` / `mergeSchema` because denormalized columns
may be added over time. To detect an incompatible mix, compare the
`schema_fingerprint` key (16 lowercase hex digits) in each file's Parquet
metadata: files of one dataset with different fingerprints disagree on the
column set, a column's type or nullability, or column order, which can still
be an additive change. Compare the physical schemas before concluding that
two files are incompatible; FORMAT.md section 3 defines what the fingerprint
covers.

```sql
SELECT file_name, decode(value) AS schema_fingerprint
FROM parquet_kv_metadata('s3://bucket/**/*.parquet')
WHERE decode(key) = 'schema_fingerprint';
SELECT file_name, name, type, logical_type
FROM parquet_schema('s3://bucket/**/*.parquet');
```

## Testing

```bash
cd rust/otap-dataflow
cargo test -p otel-arrow-dfe-series-lake
```

The reference-oracle property test in `tests/oracle.rs` is the main
correctness test; `tests/golden.rs` and `tests/golden_roundtrip.rs` check the
identity encoding against vectors produced by an independent Python
implementation, both directly and through the real OTLP-to-OTAP conversion.
