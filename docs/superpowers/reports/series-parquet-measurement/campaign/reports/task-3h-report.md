# Task 3h report: processor:attribute collapsing a high-cardinality attribute

Commit: a0ec74d82 (branch series-parquet-exporter, on top of c57ce24c3). Not pushed.

## What changed

- `crates/validation/tests/series_parquet/test_e2e.py`: a new `processed`
  topology (receiver -> `processor` node -> exporter), selected with
  `Engine(..., topology="processed", processor=<node definition>)`, with its
  own `EXPECTED_EDGES` entry so the one-recipient graph check still applies.
  Default and measured configurations are unchanged
  (`LauncherContracts.test_legacy_configuration_is_unchanged` passes). New
  builder `collapse_request` and test
  `MetricsSlice.test_attribute_delete_collapses_streams`, with Scenario and
  Guarantees comments.
- Exporter README: new `### High-cardinality point attributes` subsection
  under Limits, before Operational limits. No high-cardinality section
  existed at HEAD, so this subsection also states the strategy briefly.

## What the test sends

Pipeline: OTLP receiver -> `processor:attribute`
(`apply_to: ["signal"]`, `actions: [{action: delete, key: request.id}]`)
-> series_parquet exporter, local storage, 1s window.

Two streams, `req-a-7f3c` and `req-b-91d2`, differ only in `request.id`.
Both carry `route=/checkout` and `case=<mode>`, share `host.id=producer-1`,
one `time_unix_nano` and one `start_time_unix_nano`. Per stream:

| metric | kind | stream a | stream b |
| --- | --- | --- | --- |
| requests | delta monotonic int sum | 3 | 4 |
| latency | delta histogram | count 2, sum 1.5, buckets [1,1] | count 3, sum 2.5, buckets [2,1] |
| total | cumulative monotonic int sum | 10 | 20 |

Mode `one`: both streams in one request. Mode `two`: one request per stream.
Three Export calls in total.

## Observed

- All three requests were acknowledged. No nack.
- The processor counted 12 deleted signal attributes, one per point.
- Series rows: 6, exactly one per (metric, mode), each with its own
  series_id. attrs keys are only `case` and `route`. Mode `one` wrote its
  descriptors in the first block. Mode `two` wrote them in the block of its
  first request; the second request's block wrote a values file and no
  series file, because the descriptor was already committed and cached.
- Values rows: 12. Each collapsed pair is two rows with one series_id and
  one time_unix_nano. In mode `one` both rows sit in one file. In mode `two`
  they sit in two files.
- `request.id`, `req-a-7f3c` and `req-b-91d2` appear nowhere: not in any
  column of any row of any file (identity_bytes included, rendered as text),
  not in Parquet key-value metadata, not in the raw file bytes.
- Mutation check, same requests without the processor (strict topology):
  12 descriptors carrying request.id, so the test's descriptor, key and
  forbidden-word assertions would all fail.

## Reader results (DuckDB and ClickHouse agree)

Per (metric, mode), grouped over the values rows joined to descriptors:

| metric | rows | series_ids | timestamps | sum(value_int) | sum(count) | sum(sum) | values |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| latency | 2 | 1 | 1 | null | 5 | 4.0 | buckets [1,1] and [2,1] |
| requests | 2 | 1 | 1 | 7 | null | null | 3, 4 |
| total | 2 | 1 | 1 | 30 | null | null | 10, 20 |

The same holds for both modes. Delta sum 7 = 3 + 4. Histogram count 5 = 2 + 3,
sum 4.0 = 1.5 + 2.5, and buckets sum element-wise to [3,2]. Both readers
return null for a sum over only nulls. ClickHouse's groupArray skips nulls
where DuckDB's list keeps them; that is the only difference.

## Delta and cumulative pairs

- Delta sum and delta histogram pairs: collapse correctly. Readers summing
  the rows get exactly the originals' totals.
- Cumulative pair: nothing breaks. It is accepted and stored as two values
  rows (10 and 20) under one series_id and one timestamp. The test does not
  assert that the stored value is meaningful. The sum 30 happens to equal the
  combined total only because both points share one timestamp. A reader
  taking the latest value would pick one stream arbitrarily. With more points
  over time the two running totals interleave and a rate sees false resets.
- Series-to-points ratio signal: not present at this revision, so not
  asserted. The README names `series_cache.misses` as the nearest indicator.

## README text added

Added to the exporter README verbatim, shown indented:

    ### High-cardinality point attributes

    A series identity is the complete set of resource, scope, metric and point
    attributes, as OpenTelemetry defines a stream. A point attribute that is unique
    per point, such as a request id, a trace id or a user id, therefore makes one
    series per point: the `series` dataset grows as fast as `values` and the
    descriptor cache stops hitting. The exporter cannot drop a varying attribute
    from the identity without merging streams that the producer reported as
    distinct, so remove or bucket such attributes upstream, in the SDK with Views
    or in the pipeline. The exporter has no series-to-points ratio signal yet;
    `series_cache.misses` rising with the point rate is the nearest indicator.

    Deleting the attribute with `processor:attribute` in front of this exporter
    works and is covered end to end:

    ```yaml
    processor:
      type: processor:attribute
      config:
        apply_to: ["signal"]
        actions:
          - {action: delete, key: request.id}
    ```

    Two streams that differ only in the deleted attribute arrive with the same
    identity. They get one `series_id` and one descriptor, written once per
    partition and worker, and their points are stored as separate `values` rows
    under that `series_id`, even when the timestamps are equal. Nothing is refused,
    deduplicated or merged. Whether the collapsed rows still mean something
    depends on the point kind:

    - Delta sums and delta histograms stay correct. A reader sums the rows:
      `value_int` or `value_double`, `count`, `sum` and the bucket counts element
      by element give exactly the totals of the original streams.
    - Cumulative sums and cumulative histograms become wrong. Each row is one
      stream's running total, and the totals of different streams interleave under
      one `series_id` with nothing to tell them apart. A reader taking the latest
      value picks one stream arbitrarily, and a rate over consecutive rows sees
      false resets. Summing rows that share one timestamp happens to give the
      combined total, but streams rarely report at the same instant and reset
      independently, so that does not generalize.
    - Gauges become ambiguous. The collapsed series holds several samples for one
      instant and no rule for combining them; last, mean and maximum are all
      plausible and the files do not say which was meant.

    The `hash` action keeps the cardinality and only obscures the value. Spatial
    aggregation, dropping attributes and merging the colliding streams with a
    temporality-correct function (per-stream cumulative totals with reset
    handling, summed deltas, a chosen function for gauges), is not available in
    otap-dataflow today: `processor:temporal_reaggregation` aggregates over time
    only. Until such a processor exists, delete only attributes of delta metrics,
    or aggregate cumulative metrics and gauges in the SDK.

## Test counts

Environment: `SERIES_REQUIRE_DOCKER=1`, `taskset -c 0-7,16-23`,
`DF_ENGINE=target/release/df_engine`, Python /tmp/series-parquet-venv.

- New test alone: 1 of 1 OK.
- Full test_e2e suite, first run: 19 tests, 18 OK, 1 FAIL.
  `OutageSlice.test_storage_outage_recovers_without_losing_acked_data`
  (store minio) failed because Alloy logged its own client deadline
  (`code = Canceled desc = Timeout expired`) where the test expects a server
  `UNAVAILABLE`. That run overlapped a task-3g build-and-measure lease on the
  host. The test does not use the new topology.
- Full test_e2e suite, rerun: 19 tests, 19 OK, 139 s.
- Contract tests: `test_measurement.LauncherContracts` (6) and
  `test_failures.ToolContracts.test_engine_config_binds_the_launcher_address`
  (1) OK.

## Build

No rebuild. The release df_engine was built at 08:58. No `.rs`, `Cargo.toml`
or `Cargo.lock` under rust/otap-dataflow is newer than it, and the last
Rust-source commit is eee0f30d7 at 08:42. So the binary matches HEAD's engine
code. It includes series-parquet, and the attribute processor is always
enabled. No cargo build ran, so the lease was not touched.

## Concerns

- The outage test above is timing-sensitive under host contention: its
  Alloy-refusal assertion fails when the client deadline beats the server
  refusal. This is not caused by this change, and it passed on the rerun.
- The hour-boundary guard sleeps up to 20 s when the test starts in the last
  20 s of an hour, because descriptors legitimately repeat per hour
  partition.
- No S3 variant was added. The brief marks it optional.
