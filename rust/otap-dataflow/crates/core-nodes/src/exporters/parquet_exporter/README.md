# Parquet Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:parquet` (`urn:otel:exporter:parquet`)
- Feature gate: `parquet`; cloud backends require crate features
- Stability: Experimental

## Overview

The Parquet exporter writes OTAP batches as Parquet files through the shared
object-store abstraction. It can partition output using schema metadata and can
flush files by approximate row count or age.

## Getting Started

Write Parquet files to a local directory with a file storage backend:

```yaml
type: exporter:parquet
config:
  storage:
    file:
      base_uri: "/tmp/otap-parquet"
  writer_options:
    flush_when_older_than: 300s
    target_rows_per_file: 1000000
```

## Configuration

```yaml
type: exporter:parquet
config:
  # Object-store backend and base URI (required).
  storage:
    file:
      base_uri: "/tmp/otap-parquet"

  # Optional partition strategies.
  partitioning_strategies:
    - schema_metadata: ["_part_id"]

  writer_options:
    # Approximate row-count flush target (default: 100000000).
    target_rows_per_file: 1000000

    # Flush files older than this interval (optional).
    flush_when_older_than: 300s
```

The default build supports local file storage. Enable the top-level `azure`
feature for Azure Blob Storage and the top-level `aws` feature for S3. See
[`configs/trafficgen-parquet-azure.yaml`](../../../../../configs/trafficgen-parquet-azure.yaml)
and
[`configs/trafficgen-parquet-s3.yaml`](../../../../../configs/trafficgen-parquet-s3.yaml)
for backend-specific configuration examples.

Azure storage authentication is supplied by a bound `bearer_token_provider`
capability. The top-level `azure` feature enables the Azure identity extension
for you; you still need to declare it and bind it on the node. Configure it with
the storage scope, and do not place identity credentials in the exporter config:

```yaml
extensions:
  azure_identity:
    type: urn:microsoft:extension:azure_identity_auth
    config:
      method: managed_identity
      scope: https://storage.azure.com/.default

nodes:
  parquet:
    type: exporter:parquet
    capabilities:
      bearer_token_provider: azure_identity
    config:
      storage:
        azure:
          base_uri: https://account.blob.core.windows.net/container/prefix
```

Azure storage also takes an optional `endpoint`, the blob service URL to use
instead of the one the account in `base_uri` implies, for a private endpoint,
a sovereign cloud or the Azurite emulator
(`endpoint: https://127.0.0.1:10000/devstoreaccount1`). `base_uri` still names
the account, container and prefix, and only HTTPS is used.

## Examples

Partition by schema metadata:

```yaml
type: exporter:parquet
config:
  storage:
    file:
      base_uri: "/tmp/otap-parquet"
  partitioning_strategies:
    - schema_metadata: ["_part_id"]
```

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

Input PData message volume is reported by the engine through
`channel.receiver.messages` with its `signal` attribute on the PData input
channel and is not duplicated by the exporter.

#### `exporter.exports`

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.exports.messages` | `{message}` | `signal`, `outcome` | Number of PData messages whose export reached a terminal outcome. |
| `exporter.exports.duration` | `s` | `signal`, `outcome` | Time from dequeuing PData through the terminal Parquet write result, including conversion and partitioning. |

#### `otap.exporter.parquet`

| Metric | Unit | Description |
| --- | --- | --- |
| `otap.exporter.parquet.files_created` | `{file}` | Number of Parquet files created (across all payload types and partitions). |
| `otap.exporter.parquet.files_closed` | `{file}` | Number of Parquet files successfully closed (flushed and visible to readers). |
| `otap.exporter.parquet.rows_written` | `{row}` | Total number of rows written into Parquet writers (appended, not necessarily flushed yet). |
| `otap.exporter.parquet.flush_scheduled_max_rows` | `{file}` | Files scheduled for flush due to reaching target rows per file. |
| `otap.exporter.parquet.flush_scheduled_max_age` | `{file}` | Files scheduled for flush due to exceeding max age threshold. |
| `otap.exporter.parquet.malformed.bodies` | `{message}` | OTLP requests dropped because their body's protobuf framing is broken. |

### Events

| Event | Severity | Description |
| --- | --- | --- |
| `otlp.malformed_body` | `warn` | An OTLP request whose protobuf framing is broken was dropped as a failed export; at most one line per second, `suppressed` counting the lines left out. |

The check sees only a request that reaches this exporter as OTLP bytes: a node upstream that converts it to Arrow records (`batch`, `attributes`, `filter`, `transform`, `partition`, `log_sampling`, or `durable_buffer` with `otlp_handling: convert_to_arrow`) converts a damaged body leniently first, and this exporter then receives the partial or empty records.

## Limits

- Row-count flushing is approximate and does not split a single incoming batch
  across multiple output files.
- Very small `flush_when_older_than` values can produce many small files.
- Cloud backends depend on optional compile-time features.

## Related Docs

- [Configuration model](../../../../../docs/configuration-model.md)
- [Core node catalog](../../../README.md)
