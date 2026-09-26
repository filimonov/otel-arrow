# Series Parquet end-to-end tests

`test_e2e.py` runs a real `df_engine` on the shipped
`configs/series-parquet-*.yaml`, sends OTLP/gRPC logs and metrics, and reads
the lake back with DuckDB:

- `LocalFiles`: `series-parquet-local.yaml` on a local directory; every
  acknowledged request is stored before shutdown.
- `Minio.test_strict_s3`: `series-parquet-s3.yaml` on a MinIO container;
  every acknowledged request is stored before shutdown.
- `Minio.test_buffered_s3_shutdown_and_restart`: `series-parquet-buffered.yaml`
  (durable_buffer in front of the exporter) on MinIO, shut down gracefully
  while the exporter holds every request in its open block, which the shutdown
  writes; a restart on the same buffer directory stores one new request and
  replays nothing.

Each test shortens the window, to one second (30 seconds for the buffered
test, so the requests are still in the open block at shutdown); every other
setting is the shipped one. The suite takes about a minute.

## Running

From `rust/otap-dataflow`:

```bash
cargo build -p otel-arrow-dfe --bin df_engine \
  --features series-parquet,aws,durable-buffer
python3 -m venv /tmp/series-parquet-venv
/tmp/series-parquet-venv/bin/pip install --require-hashes \
  -r crates/validation/tests/series_parquet/requirements.lock.txt
docker pull minio/minio:RELEASE.2025-04-22T22-12-26Z
SERIES_REQUIRE_DOCKER=1 /tmp/series-parquet-venv/bin/python3 -m unittest \
  crates.validation.tests.series_parquet.test_e2e -v
```

`DF_ENGINE` selects another engine binary (default `target/debug/df_engine`)
and `SERIES_MINIO_IMAGE` another MinIO image. The tests never pull an image.
Without Docker or the image the MinIO tests skip, unless
`SERIES_REQUIRE_DOCKER=1` turns the skip into a failure.
