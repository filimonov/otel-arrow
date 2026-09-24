// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration: the user document, its validation, the factory and
//! the shipped examples.

use super::support::*;

/// Scenario: the full adapter maps byte strings and rejects an impossible lake
/// budget.
/// Guarantees: startup validates the same constraints as the public core API,
/// and `window.max_block_bytes` written as `64MiB` reaches
/// `lake.ingress.max_block_bytes` as the exact byte count.
#[test]
fn configuration_maps_and_validates() {
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "15s", "max_block_bytes": "64MiB"},
        "parquet": {"compression": "zstd"}
    }))
    .expect("valid config");
    assert_eq!(cfg.lake.ingress.max_block_bytes, 64 << 20);
    // `upload.part_bytes` below the 5 MiB S3 multipart minimum is refused by
    // `LakeConfig::validate`, which this adapter must call.
    assert!(
        serde_json::from_value::<Config>(serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-test"}},
            "upload": {"part_bytes": "1MiB"}
        }))
        .is_err()
    );
}

/// Scenario: a user names a block-level budget inside `ingress`, where the
/// adapter instead takes it from `window`.
/// Guarantees: the setting is refused rather than silently ignored, so a
/// pipeline never runs with a budget the user believes they set.
#[test]
fn a_block_budget_written_under_ingress_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "ingress": {"max_block_bytes": "64MiB"}
    }))
    .expect_err("max_block_bytes does not belong to ingress");
    assert!(
        err.to_string()
            .contains("ingress: unknown field `max_block_bytes`"),
        "unexpected error: {err}"
    );
}

/// Scenario: an exporter document with `unsupported` omitted, and with
/// `unsupported: reject`.
/// Guarantees: omitted means `drop`, so one unsupported point never refuses
/// a whole metrics request unless the user asks for it; an explicit `reject`
/// is kept.
#[test]
fn unsupported_defaults_to_drop_and_reject_is_kept() {
    for (policy, expected) in [
        (None, lake::config::UnsupportedPolicy::Drop),
        (Some("reject"), lake::config::UnsupportedPolicy::Reject),
    ] {
        let mut doc = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-test"}}});
        if let Some(policy) = policy {
            doc["unsupported"] = serde_json::json!(policy);
        }
        let cfg: Config = serde_json::from_value(doc).expect("valid");
        assert_eq!(cfg.lake.unsupported, expected, "{policy:?}");
    }
}

/// Scenario: the exemplar policy in a user document: `metrics.exemplars:
/// drop` beside the default `unsupported`, `metrics.exemplars: reject`, and
/// `logs.exemplars`.
/// Guarantees: the first two are accepted and mean what they say; the
/// third is refused at startup naming the setting, because log records
/// carry no exemplars.
#[test]
fn the_exemplar_policy_is_validated_at_startup() {
    for (exemplars, expected) in [
        ("drop", lake::config::ExemplarPolicy::Drop),
        ("reject", lake::config::ExemplarPolicy::Reject),
    ] {
        let cfg = serde_json::from_value::<Config>(serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-test"}},
            "metrics": {"exemplars": exemplars}
        }))
        .expect("valid");
        assert_eq!(cfg.lake.exemplar_policy(), expected, "{exemplars}");
    }
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "logs": {"exemplars": "drop"}
    }))
    .expect_err("refused");
    assert!(
        err.to_string().contains("logs.exemplars"),
        "unexpected error: {err}"
    );
}

/// Scenario: `parquet.compression` names a codec the sink does not write.
/// Guarantees: the configuration is refused instead of writing zstd while the
/// document claims another codec.
#[test]
fn a_parquet_compression_other_than_zstd_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "parquet": {"compression": "snappy"}
    }))
    .expect_err("only zstd is written");
    assert!(
        err.to_string().contains("must be zstd"),
        "unexpected error: {err}"
    );
}

/// Scenario: `window.interval` is a sub-second duration.
/// Guarantees: the adapter refuses it, matching the whole-second rule the
/// window arithmetic and the `window_secs` file metadata both rely on.
#[test]
fn a_sub_second_window_interval_is_refused() {
    let err = serde_json::from_value::<Config>(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}},
        "window": {"interval": "500ms"}
    }))
    .expect_err("sub-second interval");
    assert!(
        err.to_string().contains("whole seconds"),
        "unexpected error: {err}"
    );
}

/// Scenario: the example pipeline configuration shipped in `configs/` is fed
/// to the same validator the engine uses at startup.
/// Guarantees: the documented example stays loadable, so the manual
/// try-it-out instructions in the module README cannot silently rot.
#[test]
fn the_shipped_example_configuration_is_valid() {
    let yaml = include_str!("../../../../../../configs/series-parquet-local.yaml");
    let doc: serde_json::Value = serde_yaml::from_str(yaml).expect("example config parses");
    let exporter = doc
        .pointer("/groups/default/pipelines/main/nodes/exporter/config")
        .expect("example config has an exporter node");
    let cfg: Config = serde_json::from_value(exporter.clone()).expect("example config is valid");
    assert_eq!(cfg.lake.writer_id, "local_1");
}

/// Scenario: the factory builds the exporter for local file storage with no
/// capability bound to the node.
/// Guarantees: creation succeeds, because file storage needs no bearer token
/// provider, so a local pipeline never has to declare one.
#[test]
fn the_factory_creates_file_storage_without_a_capability() {
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::config::ExporterConfig;
    use otel_arrow_dfe_engine::context::ControllerContext;

    let dir = tempfile::tempdir().expect("temp dir");
    let metrics_system = otel_arrow_dfe_telemetry::InternalTelemetrySystem::default();
    let controller = ControllerContext::new(metrics_system.registry());
    let pipeline = controller
        .pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0)
        .with_node_context(
            "series".into(),
            super::super::SERIES_PARQUET_EXPORTER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Exporter,
            std::collections::HashMap::new(),
        );
    let mut node_config =
        NodeUserConfig::new_exporter_config(super::super::SERIES_PARQUET_EXPORTER_URN);
    node_config.config = serde_json::json!({
        "storage": {"file": {"base_uri": dir.path().to_str().expect("utf-8 path")}}
    });
    let created = (super::super::SERIES_PARQUET.create)(
        pipeline,
        test_node("series"),
        Arc::new(node_config),
        &ExporterConfig::new("series"),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    assert!(created.is_ok(), "file storage needs no capability");
}

/// Scenario: the factory builds the exporter for S3 storage, with a valid
/// retry budget, and no capability bound to the node.
/// Guarantees: creation succeeds without a bearer token provider, because S3
/// authenticates through its own `auth` section; only Azure storage requires
/// the capability.
#[test]
fn the_factory_creates_s3_storage_without_a_token_provider() {
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::config::ExporterConfig;
    use otel_arrow_dfe_engine::context::ControllerContext;

    let metrics_system = otel_arrow_dfe_telemetry::InternalTelemetrySystem::default();
    let controller = ControllerContext::new(metrics_system.registry());
    let pipeline = controller
        .pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0)
        .with_node_context(
            "series".into(),
            super::super::SERIES_PARQUET_EXPORTER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Exporter,
            std::collections::HashMap::new(),
        );
    let mut node_config =
        NodeUserConfig::new_exporter_config(super::super::SERIES_PARQUET_EXPORTER_URN);
    node_config.config = serde_json::json!({
        "storage": s3_storage(),
        "retry": {"retry_timeout": "30s"}
    });
    let created = (super::super::SERIES_PARQUET.create)(
        pipeline,
        test_node("series"),
        Arc::new(node_config),
        &ExporterConfig::new("series"),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    assert!(created.is_ok(), "S3 storage needs no bearer token provider");
}

/// Scenario: the factory builds the exporter for Azure storage, which
/// authenticates through a bearer token provider, with no capability bound
/// to the node; the store retry budget is valid, so only the missing
/// capability can refuse it.
/// Guarantees: creation fails with an invalid-configuration error naming the
/// capability, before the node starts, instead of starting an exporter that
/// cannot authenticate.
#[test]
#[cfg(feature = "azure")]
fn the_factory_refuses_azure_storage_without_a_token_provider() {
    use otel_arrow_dfe_config::node::NodeUserConfig;
    use otel_arrow_dfe_engine::config::ExporterConfig;
    use otel_arrow_dfe_engine::context::ControllerContext;

    let metrics_system = otel_arrow_dfe_telemetry::InternalTelemetrySystem::default();
    let controller = ControllerContext::new(metrics_system.registry());
    let pipeline = controller
        .pipeline_context_with("grp".into(), "pipe".into(), 0, 1, 0)
        .with_node_context(
            "series".into(),
            super::super::SERIES_PARQUET_EXPORTER_URN.into(),
            otel_arrow_dfe_config::node::NodeKind::Exporter,
            std::collections::HashMap::new(),
        );
    let mut node_config =
        NodeUserConfig::new_exporter_config(super::super::SERIES_PARQUET_EXPORTER_URN);
    node_config.config = serde_json::json!({
        "storage": {"azure": {
            "base_uri": "https://mystorageaccount.blob.core.windows.net/container"
        }},
        "retry": {"retry_timeout": "30s"}
    });
    let created = (super::super::SERIES_PARQUET.create)(
        pipeline,
        test_node("series"),
        Arc::new(node_config),
        &ExporterConfig::new("series"),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    let Err(err) = created else {
        panic!("azure storage must require a bound bearer_token_provider");
    };
    assert!(
        matches!(
            err,
            otel_arrow_dfe_config::error::Error::InvalidUserConfig { .. }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("bearer_token_provider"), "{err}");
}

/// Scenario: an explicit cloud store retry budget equal to, above and below
/// the block's flush deadline, no retry section, and local file storage with
/// the same values.
/// Guarantees: only an explicit `retry.retry_timeout` that is not strictly
/// shorter than `window.flush_retry_deadline` is refused, with both values in
/// the message; an absent section and local file storage, which applies no
/// store retry, are never refused for it.
#[test]
fn an_explicit_store_retry_budget_must_be_shorter_than_the_flush_deadline() {
    let deadline = Duration::from_secs(60);
    let retry = |timeout: &str| -> otel_arrow_dfe_otap::object_store::RetryOptions {
        serde_json::from_value(serde_json::json!({ "retry_timeout": timeout }))
            .expect("retry options")
    };
    for timeout in ["60s", "3m"] {
        let err = super::super::config::check_retry_deadline(true, Some(&retry(timeout)), deadline)
            .expect_err("not strictly less");
        assert!(err.starts_with("retry.retry_timeout ("), "{err}");
        assert!(err.contains("window.flush_retry_deadline (60s)"), "{err}");
    }
    assert!(
        super::super::config::check_retry_deadline(true, Some(&retry("30s")), deadline).is_ok()
    );
    assert!(super::super::config::check_retry_deadline(true, None, deadline).is_ok());
    assert!(
        super::super::config::check_retry_deadline(false, Some(&retry("3m")), deadline).is_ok()
    );
    // The shipped local example keeps loading with no retry section at all,
    // and no retry options are made up for a store that applies none.
    let cfg: Config = serde_json::from_value(serde_json::json!({
        "storage": {"file": {"base_uri": "/tmp/series-test"}}
    }))
    .expect("file storage applies no store retry");
    assert!(cfg.retry.is_none());
}

/// Scenario: a minimal S3 configuration with no `retry` section is loaded
/// through the factory's own `validate_config` and as a `Config`, once with
/// the default flush deadline and once with `window.flush_retry_deadline: 20s`;
/// then with an explicit `retry.retry_timeout` of 60s and of 3m against the
/// default 60s deadline.
/// Guarantees: the minimal configuration loads, and its effective store retry
/// budget is half the flush deadline (30s, then 10s) with object_store's other
/// retry defaults; an explicit budget at or above the deadline is refused at
/// load naming both values.
#[test]
fn a_minimal_s3_config_derives_its_retry_budget_from_the_flush_deadline() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    validate(&serde_json::json!({"storage": s3_storage()})).expect("a minimal S3 config loads");
    for (deadline, derived) in [(None, 30), (Some("20s"), 10)] {
        let mut doc = serde_json::json!({"storage": s3_storage()});
        if let Some(deadline) = deadline {
            doc["window"] = serde_json::json!({"flush_retry_deadline": deadline});
        }
        let cfg: Config = serde_json::from_value(doc).expect("valid");
        let retry = cfg.retry.expect("a cloud store gets derived retry options");
        assert_eq!(retry.retry_timeout, Duration::from_secs(derived));
        let defaults: otel_arrow_dfe_otap::object_store::RetryOptions =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(retry.max_retries, defaults.max_retries);
        assert_eq!(retry.init_backoff, defaults.init_backoff);
        assert_eq!(retry.max_backoff, defaults.max_backoff);
    }
    for timeout in ["60s", "3m"] {
        let err = validate(&serde_json::json!({
            "storage": s3_storage(),
            "retry": {"retry_timeout": timeout}
        }))
        .expect_err("an explicit budget at or above the deadline")
        .to_string();
        assert!(err.contains("retry.retry_timeout ("), "{err}");
        assert!(err.contains("window.flush_retry_deadline (60s)"), "{err}");
    }
}

/// Scenario: `upload.abort_timeout` is set to 999 ms and to exactly 1 s.
/// Guarantees: the floor is 1 s inclusive: below it startup is refused with a
/// sentence naming the key, the value and the floor, and at it the
/// configuration is accepted.
#[test]
fn the_abort_timeout_has_a_one_second_floor() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    let with = |abort: &str| {
        serde_json::json!({
            "storage": {"file": {"base_uri": "/tmp/series-config"}},
            "upload": {"abort_timeout": abort}
        })
    };
    let err = validate(&with("999ms"))
        .expect_err("below the floor")
        .to_string();
    assert!(
        err.contains("upload.abort_timeout (999ms) must be at least 1s"),
        "{err}"
    );
    validate(&with("1s")).expect("the floor itself is accepted");
}

/// Scenario: configuration contains misspelled settings or cross-field
/// violations, each applied alone to a base configuration that is itself
/// valid, and each checked through the factory's own `validate_config`.
/// Guarantees: the factory rejects every one of them before any network
/// listener starts, and each rejection names the rule it broke, so a case can
/// only pass by failing for the reason it was written for, never because the
/// base itself was broken, and a pipeline never runs with a budget the
/// operator believes they set.
#[test]
fn startup_rejects_invalid_configuration() {
    let validate = super::super::SERIES_PARQUET.validate_config;
    let base = serde_json::json!({"storage": {"file": {"base_uri": "/tmp/series-config"}}});
    validate(&base).expect("the base configuration is valid");
    let cases = [
        (
            "window",
            serde_json::json!({"interval": "0s"}),
            "window.interval",
        ),
        (
            "window",
            serde_json::json!({"interval": "500ms"}),
            "window.interval must be positive whole seconds",
        ),
        (
            "window",
            serde_json::json!({"max_requests_per_block": 0}),
            "window.max_requests_per_block must be at least 1",
        ),
        (
            "window",
            serde_json::json!({"max_block_bytes": "1MiB"}),
            "window.max_block_bytes must be at least twice ingress.max_extracted_bytes",
        ),
        (
            "ingress",
            serde_json::json!({"max_row_bytes": "3MiB"}),
            "ingress.max_row_bytes must be at most sorting.run_target_bytes / 4",
        ),
        (
            "ingress",
            serde_json::json!({"max_requset_bytes": "1MiB"}),
            "ingress: unknown field `max_requset_bytes`",
        ),
        (
            "ingress",
            serde_json::json!({"max_nesting_depth": 257}),
            "ingress.max_nesting_depth must be at most 256",
        ),
        (
            "upload",
            serde_json::json!({"concurrency": 0}),
            "upload.concurrency must be at least 1",
        ),
        (
            "upload",
            serde_json::json!({"part_bytes": "1MiB"}),
            "upload.part_bytes must be at least 5MiB",
        ),
        (
            "parquet",
            serde_json::json!({"compression": "snappy"}),
            "parquet.compression must be zstd",
        ),
        (
            "metrics",
            serde_json::json!({"values_sort": ["no_such_column"]}),
            "metrics.values_sort[0].column \"no_such_column\" is not a column of",
        ),
        // `attrs` is a Map column of logs/values: present, but not a type the
        // Arrow row format can sort by.
        (
            "logs",
            serde_json::json!({"values_sort": ["attrs"]}),
            "logs.values_sort[0].column \"attrs\" has type",
        ),
        (
            "logs",
            serde_json::json!({"denormalize": [{"path": "resource.x", "column": "SERIES_ID"}]}),
            "logs.denormalize[0].column \"SERIES_ID\" is a column name collision",
        ),
        (
            "logs",
            serde_json::json!({"denormalize": [{"path": "resource.x", "column": "date"}]}),
            "logs.denormalize[0].column \"date\" is named like a partition key",
        ),
        (
            "logs",
            serde_json::json!({"denormalize": ["unknown.x"]}),
            "logs.denormalize[0].path \"unknown.x\" must start with",
        ),
        (
            "metrics",
            serde_json::json!({"series_attributes": ["k8s.pod.name"]}),
            "metrics.series_attributes",
        ),
        (
            "writer_id",
            serde_json::json!("local-1"),
            "writer_id \"local-1\" must use only [A-Za-z0-9_.]",
        ),
        (
            "series_cache",
            serde_json::json!({"max_entries": 0}),
            "series_cache.max_entries must be positive",
        ),
        (
            "notify_batch",
            serde_json::json!(0),
            "notify_batch must be positive",
        ),
        (
            "window",
            serde_json::json!({"flush_retry_deadline": "0s"}),
            "window.flush_retry_deadline must be positive",
        ),
        (
            "window",
            serde_json::json!({"max_requests_per_block": 0}),
            "window.max_requests_per_block must be at least 1",
        ),
        (
            "window",
            serde_json::json!({"max_block_bytes": "lots"}),
            "window: ",
        ),
        (
            "ingress",
            serde_json::json!({"max_request_bytes": 0}),
            "ingress.max_request_bytes must be positive",
        ),
        (
            "sorting",
            serde_json::json!({"merge_chunk_bytes": 0}),
            "sorting.merge_chunk_bytes must be positive",
        ),
        (
            "parquet",
            serde_json::json!({"writer_limit_bytes": 0}),
            "parquet.writer_limit_bytes must be positive",
        ),
        (
            "upload",
            serde_json::json!({"abort_timeout": "0s"}),
            "upload.abort_timeout (0ns) must be at least 1s",
        ),
        (
            "upload",
            serde_json::json!({"abort_timeout": "999ms"}),
            "upload.abort_timeout (999ms) must be at least 1s",
        ),
        (
            "upload",
            serde_json::json!({"part_bytes": null}),
            "upload: byte size must not be null",
        ),
        (
            "metrics",
            serde_json::json!({"denormalise": []}),
            "metrics: unknown field `denormalise`",
        ),
    ];
    for (section, value, expected) in cases {
        let mut candidate = base.clone();
        candidate[section] = value;
        let err = validate(&candidate).expect_err(section).to_string();
        assert!(
            err.contains(expected),
            "section={section}: expected {expected:?} in {err}"
        );
    }
}

/// Scenario: operators read the component README before configuring durable
/// ingest.
/// Guarantees: the documentation states the retry, shutdown, memory and
/// unsupported-signal contracts, and reproduces the exact River configuration
/// the end-to-end tests run, so the documented deployment cannot drift away
/// from the tested one.
#[test]
fn readme_states_operating_contract() {
    let readme = include_str!("../README.md");
    for phrase in [
        "at-least-once",
        "wait_for_result",
        "timeout_secs=180",
        "core_allocation",
        "memory.unaccounted_rss_bytes",
        "exponential histograms",
        "union_by_name",
        "mergeSchema",
        "schema_fingerprint",
        "notify_batch",
        "No block-atomic snapshot",
        "Alloy",
        "MinIO",
        "ClickHouse",
        "loki.source.file",
        "e2e.source",
        "row_number()",
        "SERIES_REQUIRE_DOCKER",
    ] {
        assert!(
            readme.contains(phrase),
            "missing documented contract: {phrase}"
        );
    }
    let alloy = include_str!("../../../../../../configs/series-parquet.alloy");
    assert!(
        readme.contains(alloy),
        "README must reproduce the exact tested River config"
    );
}
