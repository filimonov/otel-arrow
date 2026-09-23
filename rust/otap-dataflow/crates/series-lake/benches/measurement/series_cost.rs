// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The resident cost of a series row, run by `measurement --series-cost`.
//!
//! One process measures one point: a request of some number of new series,
//! extracted and admitted into an empty block through the production path,
//! with the block's own reservation beside the Arrow buffers it pins and,
//! in the DHAT build, the heap it really holds.

use bytes::Bytes;
use otel_arrow_dfe_pdata::otap::memory::{CountedAllocations, record_batch_pinned_bytes};
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_series_lake::buffer::Block;
use otel_arrow_dfe_series_lake::cache::SeriesCache;
use otel_arrow_dfe_series_lake::config::LakeConfig;
use otel_arrow_dfe_series_lake::extract::extract;
use serde::Serialize;

use super::stages::{Result, Signal};

/// Heap figures of the process at one instant, when a heap profiler runs.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct HeapNow {
    /// Bytes live now.
    pub curr_bytes: u64,
    /// The most bytes ever live at once.
    pub max_bytes: u64,
}

/// One request carrying `series` distinct series with one record each.
///
/// The content is as small as a real series gets: an eight-character
/// identifying attribute per series, a one-byte log body or an integer
/// gauge point, under one resource with `host.id` and `service.name`. That
/// is the shape in which the fixed per-series reservation, not the
/// content-proportional one, dominates a series row.
fn series_request(signal: Signal, series: usize) -> OtlpProtoBytes {
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
        number_data_point,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use prost::Message as _;

    let kv = |key: &str, value: String| KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value)),
        }),
    };
    let resource = Some(Resource {
        attributes: vec![
            kv("host.id", "producer-1".into()),
            kv("service.name", "bench".into()),
        ],
        ..Default::default()
    });
    let time = |i: usize| 1_789_960_500_000_000_000 + i as u64;
    match signal {
        Signal::Logs => {
            let data = LogsData {
                resource_logs: vec![ResourceLogs {
                    resource,
                    scope_logs: vec![ScopeLogs {
                        log_records: (0..series)
                            .map(|i| LogRecord {
                                time_unix_nano: time(i),
                                body: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("x".into())),
                                }),
                                attributes: vec![kv("logger.name", format!("l{i:07}"))],
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            OtlpProtoBytes::ExportLogsRequest(Bytes::from(data.encode_to_vec()))
        }
        Signal::Metrics => {
            let gauge = Metric {
                name: "g".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: (0..series)
                        .map(|i| NumberDataPoint {
                            time_unix_nano: time(i),
                            attributes: vec![kv("series.slot", format!("s{i:07}"))],
                            value: Some(number_data_point::Value::AsInt(i as i64)),
                            ..Default::default()
                        })
                        .collect(),
                })),
                ..Default::default()
            };
            let data = MetricsData {
                resource_metrics: vec![ResourceMetrics {
                    resource,
                    scope_metrics: vec![ScopeMetrics {
                        metrics: vec![gauge],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            OtlpProtoBytes::ExportMetricsRequest(Bytes::from(data.encode_to_vec()))
        }
    }
}

/// Measure what one block really holds per new series, against what it
/// reserves.
///
/// One request of `series` new series is extracted and then reserved,
/// admitted and sealed into an empty block, exactly as the exporter does.
/// The reservation is the block's own charge; the Arrow buffers are counted
/// exactly by capacity; and when `heap` reads a live heap profiler, the
/// block's whole heap is measured by dropping it, and the series cache's by
/// dropping that. The admission transient is reported only when it set a
/// new process peak, because the profiler's peak cannot be reset.
///
/// # Errors
/// Propagates a failure extracting, reserving, admitting or sealing.
pub fn series_cost(
    signal: Signal,
    series: usize,
    denormalize: bool,
    heap: &dyn Fn() -> Option<HeapNow>,
) -> Result<serde_json::Value> {
    let mut cfg = LakeConfig::default();
    cfg.logs.series_attributes = vec!["logger.name".into()];
    if denormalize {
        let column: otel_arrow_dfe_series_lake::config::Denormalize =
            serde_json::from_value(serde_json::json!("resource.service.name"))?;
        cfg.logs.denormalize = vec![column.clone()];
        cfg.metrics.denormalize = vec![column];
    }
    // Room for the largest point: the per-series cost, not these limits, is
    // what is measured, and the defaults would refuse the largest request.
    cfg.ingress.max_extracted_bytes = 1 << 30;
    cfg.ingress.max_block_bytes = 4 << 30;
    let lake_signal = match signal {
        Signal::Logs => otel_arrow_dfe_series_lake::canonical::Signal::Logs,
        Signal::Metrics => otel_arrow_dfe_series_lake::canonical::Signal::Metrics,
    };
    let fixed = cfg.series_row_fixed_bytes(lake_signal);
    let wire = series_request(signal, series);
    let wire_bytes = wire.as_bytes().len();
    let payload: OtapPayload = wire.into();
    let mut records: OtapArrowRecords = payload.try_into_with_default()?;
    let extracted = extract(&mut records, &cfg)?;
    drop(records);
    let q = cfg.ingress.pending_series_entry_bytes;
    let descriptors = extracted.descriptors.len();
    let approx: usize = extracted.descriptors.iter().map(|d| d.approx_bytes).sum();
    let decoded: usize = extracted.descriptors.iter().map(|d| d.decoded_bytes).sum();
    let charged: usize = extracted
        .descriptors
        .iter()
        .map(|d| d.series_row_bytes() + q)
        .sum();
    let values_extracted = extracted.pinned_bytes;
    let mut cache = SeriesCache::new(series.max(1) * 2);
    let mut block = Block::new(1_789_960_500, 1, cfg.clone());
    let before_reserve = heap();
    let reservation = block.reserve(&extracted, &mut cache, 0)?;
    let reserved = reservation.bytes;
    let after_reserve = heap();
    block.admit(extracted, reservation)?;
    let after_admit = heap();
    let pinned_of = |block: &Block, want_series: bool| {
        let mut seen = CountedAllocations::default();
        block
            .tables()
            .filter(|table| table.dataset().is_series() == want_series)
            .flat_map(|table| table.iter_snapshots())
            .map(|batch| record_batch_pinned_bytes(batch, &mut seen))
            .sum::<usize>()
    };
    let series_pinned = pinned_of(&block, true);
    let values_pinned = pinned_of(&block, false);
    let pending_capacity = block.pending_series.capacity();
    let active_bytes = block.bytes;
    block.seal(1_789_960_500_000_000)?;
    let after_seal = heap();
    let sealed_bytes = block.bytes;
    let series_pinned_sealed = pinned_of(&block, true);
    drop(block);
    let after_block = heap();
    let cache_entries = cache.len();
    drop(cache);
    let after_cache = heap();
    let per = |bytes: f64| {
        if series == 0 {
            serde_json::Value::Null
        } else {
            (bytes / series as f64).into()
        }
    };
    let measured = match (
        before_reserve,
        after_reserve,
        after_admit,
        after_seal,
        after_block,
        after_cache,
    ) {
        (
            Some(before),
            Some(reserved_heap),
            Some(admitted),
            Some(sealed),
            Some(emptied),
            Some(uncached),
        ) => {
            let admitted_block = admitted.curr_bytes.saturating_sub(emptied.curr_bytes) as f64;
            let sealed_block = sealed.curr_bytes.saturating_sub(emptied.curr_bytes) as f64;
            let cache_heap = emptied.curr_bytes.saturating_sub(uncached.curr_bytes) as f64;
            let series_heap = admitted_block - values_pinned as f64;
            serde_json::json!({
                "admitted_block_heap_bytes": admitted_block,
                "sealed_block_heap_bytes": sealed_block,
                "series_heap_bytes": series_heap,
                "series_heap_bytes_per_series": per(series_heap),
                "sealed_series_heap_bytes_per_series": per(sealed_block - values_pinned as f64),
                "cache_heap_bytes": cache_heap,
                "cache_heap_bytes_per_entry": if cache_entries == 0 {
                    serde_json::Value::Null
                } else {
                    (cache_heap / cache_entries as f64).into()
                },
                "reserve_heap_growth_bytes": reserved_heap.curr_bytes.saturating_sub(before.curr_bytes),
                "admission_peak_above_start_bytes": if admitted.max_bytes > reserved_heap.max_bytes {
                    serde_json::Value::from(admitted.max_bytes - before.curr_bytes)
                } else {
                    serde_json::Value::Null
                },
            })
        }
        _ => serde_json::Value::Null,
    };
    Ok(serde_json::json!({
        "signal": match signal { Signal::Logs => "logs", Signal::Metrics => "metrics" },
        "series": series,
        "descriptors": descriptors,
        "denormalized_series_columns": usize::from(denormalize),
        "wire_bytes": wire_bytes,
        "fixed_reservation_bytes_per_series": fixed,
        "extracted_estimate_bytes_per_series": per(approx as f64),
        "decoded_tree_bytes_per_series": per(decoded as f64),
        "charged_bytes_per_series": per(charged as f64),
        "reserved_bytes": reserved,
        "values_extracted_pinned_bytes": values_extracted,
        "values_pinned_bytes": values_pinned,
        "series_pinned_bytes": series_pinned,
        "series_pinned_bytes_per_series": per(series_pinned as f64),
        "series_pinned_sealed_bytes_per_series": per(series_pinned_sealed as f64),
        "pending_set_capacity": pending_capacity,
        "block_bytes_active": active_bytes,
        "block_bytes_sealed": sealed_bytes,
        "cache_entries": cache_entries,
        "heap": measured,
    }))
}
