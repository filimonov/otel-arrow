# series-lake

Engine-independent core of the series Parquet exporter: canonical series
identity, extraction of `series` and values datasets from OTAP Arrow
records, a bounded series cache, sorted block buffers and a Parquet sink
over `object_store`.

The storage format is specified in [docs/FORMAT.md](docs/FORMAT.md). The
design is in `docs/superpowers/specs/2026-09-21-series-parquet-exporter-design.md`
at the repository root.

This crate is engine-independent: it never depends on the Dataflow engine. A
`core-nodes` exporter node (`exporter:series_parquet`) that adapts it into a
Dataflow pipeline is planned for a later plan and does not exist yet.

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
  series. Bytes values render as lowercase hex; a dedicated `body_bytes`
  binary column for the log body is deferred to a later format version.
- A cancellation that lands after a file's Parquet finalization has begun
  (for example, exporter shutdown) can leave an orphaned multipart upload;
  it is reclaimed by a bucket lifecycle rule, not by this crate.
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

Both recipes need `union_by_name` / `mergeSchema` because denormalized columns
may be added over time. To detect an incompatible mix, compare the
`schema_fingerprint` key (16 lowercase hex digits) in each file's Parquet
metadata: files of one dataset with different fingerprints disagree on the
column set, a column's type, or column order, and a reader that ignores
this silently drops or misreads columns. See `docs/FORMAT.md` for exactly
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
