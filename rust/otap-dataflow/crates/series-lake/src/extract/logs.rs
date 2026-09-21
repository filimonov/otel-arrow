// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Logs extraction.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::schema::consts::{
    BODY, EVENT_NAME, FLAGS, ID, NAME, OBSERVED_TIME_UNIX_NANO, RESOURCE, SCHEMA_URL, SCOPE,
    SEVERITY_NUMBER, SEVERITY_TEXT, SPAN_ID, TIME_UNIX_NANO, TRACE_ID, VERSION,
};

use super::{
    Budget, Col, DescriptorRow, ExtractStats, Extracted, RowSink, ValuesRow, any_value_col,
    attr_table, attrs_of, denorm_bytes, denorm_lookup, descriptor_row, fixed_at, flags_at, i64_at,
    map_cell, opt_u16_at, plain, producer_id, str_at, struct_child, timestamp_pair,
};
use crate::canonical::{Descriptor, SeriesId, Signal};
use crate::config::LakeConfig;
use crate::error::{Error, Result};
use crate::schema::{Dataset, denorm_columns};
use crate::value::{DecodeLimits, Value, body_string};

/// Memo key for a logs series.
///
/// `(resource_id, scope_id)` alone is not enough: the row-level scope and schema
/// strings are part of the identity, and two rows can share a resource and scope
/// id while carrying different scope names or schema URLs. `None` ids are part
/// of the key too, so a record without attributes never collapses into parent 0.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MemoKey {
    resource_id: Option<u32>,
    scope_id: Option<u32>,
    resource_schema_url: String,
    scope_name: String,
    scope_version: String,
    scope_schema_url: String,
    identity_attrs: Vec<u8>,
}

pub(crate) fn extract_logs(
    records: &OtapArrowRecords,
    cfg: &LakeConfig,
    budget: &mut Budget,
) -> Result<Extracted> {
    let limits = DecodeLimits::new(cfg.ingress.max_nesting_depth, cfg.ingress.max_row_bytes);
    let mut stats = ExtractStats::default();
    let Some(logs) = records.get(ArrowPayloadType::Logs) else {
        return Ok(Extracted {
            signal: Signal::Logs,
            descriptors: vec![],
            values: vec![],
            pinned_bytes: 0,
            stats,
        });
    };
    let resource_attrs = attr_table(records, ArrowPayloadType::ResourceAttrs, limits)?;
    let scope_attrs = attr_table(records, ArrowPayloadType::ScopeAttrs, limits)?;
    let log_attrs = attr_table(records, ArrowPayloadType::LogAttrs, limits)?;

    let ts_ns = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let time = plain(logs, TIME_UNIX_NANO, &ts_ns)?;
    let observed = plain(logs, OBSERVED_TIME_UNIX_NANO, &ts_ns)?;
    let id = plain(logs, ID, &DataType::UInt16)?;
    let severity_number = plain(logs, SEVERITY_NUMBER, &DataType::Int32)?;
    let severity_text = plain(logs, SEVERITY_TEXT, &DataType::Utf8)?;
    let event_name = plain(logs, EVENT_NAME, &DataType::Utf8)?;
    let flags_col = plain(logs, FLAGS, &DataType::UInt32)?;
    let trace_id = plain(logs, TRACE_ID, &DataType::FixedSizeBinary(16))?;
    let span_id = plain(logs, SPAN_ID, &DataType::FixedSizeBinary(8))?;
    let res_id = struct_child(logs, RESOURCE, ID, &DataType::UInt16)?;
    let res_schema = struct_child(logs, RESOURCE, SCHEMA_URL, &DataType::Utf8)?;
    let scope_id = struct_child(logs, SCOPE, ID, &DataType::UInt16)?;
    let scope_name = struct_child(logs, SCOPE, NAME, &DataType::Utf8)?;
    let scope_version = struct_child(logs, SCOPE, VERSION, &DataType::Utf8)?;
    let scope_schema = plain(logs, SCHEMA_URL, &DataType::Utf8)?;
    let body = any_value_col(logs, BODY)?;

    let allow: &[String] = &cfg.logs.series_attributes;
    let values_denorm = denorm_columns(Dataset::LogsValues, cfg);
    let mut sink = RowSink::new(Dataset::LogsValues, cfg)?;
    let mut descriptors: Vec<DescriptorRow> = Vec::new();
    let mut seen: HashSet<SeriesId> = HashSet::new();
    let mut memo: HashMap<MemoKey, SeriesId> = HashMap::new();

    for row in 0..logs.num_rows() {
        let rid = opt_u16_at(&res_id, row);
        let sid = opt_u16_at(&scope_id, row);
        let lid = opt_u16_at(&id, row);
        let resource = attrs_of(&resource_attrs, rid);
        let scope = attrs_of(&scope_attrs, sid);
        let all_attrs = attrs_of(&log_attrs, lid);
        let (identity_attrs, residual): (Vec<(String, Value)>, Vec<(String, Value)>) = all_attrs
            .iter()
            .cloned()
            .partition(|(k, _)| allow.iter().any(|a| a == k));
        let identity_key = crate::canonical::canonical_bytes(&Descriptor {
            signal: Signal::Logs,
            resource_attrs: vec![],
            resource_schema_url: String::new(),
            scope_name: String::new(),
            scope_version: String::new(),
            scope_schema_url: String::new(),
            scope_attrs: vec![],
            metric: None,
            attrs: identity_attrs.clone(),
        });
        let key = MemoKey {
            resource_id: rid,
            scope_id: sid,
            resource_schema_url: str_at(&res_schema, row),
            scope_name: str_at(&scope_name, row),
            scope_version: str_at(&scope_version, row),
            scope_schema_url: str_at(&scope_schema, row),
            identity_attrs: identity_key,
        };
        let series_id = match memo.get(&key) {
            Some(id) => *id,
            None => {
                let descriptor = Descriptor {
                    signal: Signal::Logs,
                    resource_attrs: resource.to_vec(),
                    resource_schema_url: key.resource_schema_url.clone(),
                    scope_name: key.scope_name.clone(),
                    scope_version: key.scope_version.clone(),
                    scope_schema_url: key.scope_schema_url.clone(),
                    scope_attrs: scope.to_vec(),
                    metric: None,
                    attrs: identity_attrs.clone(),
                };
                let dr = descriptor_row(descriptor, Dataset::LogsSeries, cfg, &mut stats, budget)?;
                let id = dr.series_id;
                if seen.insert(id) {
                    descriptors.push(dr);
                } else {
                    // Two memo keys can hash to the same series: the duplicate
                    // is not retained, so give its charge back.
                    budget.uncharge(dr.approx_bytes);
                }
                let _ = memo.insert(key, id);
                id
            }
        };

        let (t_ns, t_us) = timestamp_pair(i64_at(&time, row), &mut stats);
        let (o_ns, o_us) = timestamp_pair(i64_at(&observed, row), &mut stats);
        let body_value = match &body {
            Some(b) => b.value_at(row, limits)?,
            None => Value::Null,
        };
        let body_str = body_string(&body_value);
        let severity_text_str = str_at(&severity_text, row);
        let event_name_str = str_at(&event_name, row);
        let producer = producer_id(resource, &cfg.producer_id_attribute);
        // Charge everything the row actually retains, in the form it is stored
        // in: fixed cells, the rendered body, the rendered residual attribute
        // map, the denormalized strings and the projected producer id. The map
        // and the body are charged their rendered `String::len()`, because hex
        // encoding and JSON escaping can make the stored cell several times the
        // size of the decoded value tree.
        let (residual_cell, residual_bytes) = map_cell(&residual);
        let mut approx = 16 + 8 * 6 + 24 + 8;
        approx += body_str.as_ref().map_or(0, String::len);
        approx += severity_text_str.len() + event_name_str.len() + producer.len();
        approx += residual_bytes;
        let severity = severity_number
            .as_ref()
            .and_then(|a| {
                a.is_valid(row)
                    .then(|| a.as_primitive::<Int32Type>().value(row))
            })
            .unwrap_or(0);
        let mut cols = vec![
            Col::Fixed(Some(series_id.to_vec())),
            Col::Str(Some(producer)),
            Col::TsUs(t_us),
            Col::Int(t_ns),
            Col::TsUs(o_us),
            Col::Int(o_ns),
            Col::Int32(Some(severity)),
            Col::Str(Some(severity_text_str)),
            Col::Str(body_str),
            Col::Str(Some(event_name_str)),
            Col::Fixed(fixed_at(&trace_id, row)),
            Col::Fixed(fixed_at(&span_id, row)),
            Col::Int32(Some(flags_at(&flags_col, row))),
            residual_cell,
        ];
        for d in &values_denorm {
            let v = denorm_lookup(d, resource, scope, all_attrs, &mut stats);
            approx += denorm_bytes(&v);
            cols.push(Col::from(v));
        }
        sink.push(
            &ValuesRow {
                cols,
                approx_bytes: approx,
            },
            budget,
        )?;
        stats.rows += 1;
    }
    let (batches, pinned_bytes) = sink.finish(budget)?;
    let values = if batches.is_empty() {
        vec![]
    } else {
        vec![(Dataset::LogsValues, batches)]
    };
    if descriptors.is_empty() && !values.is_empty() {
        return Err(Error::invalid("values without descriptors"));
    }
    Ok(Extracted {
        signal: Signal::Logs,
        descriptors,
        values,
        pinned_bytes,
        stats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DenormType, Denormalize, LakeConfig};
    use crate::error::{Error, RefuseReason};
    use crate::extract::{extract, series_batch};
    use crate::schema::Dataset;
    use arrow::array::{Array, ArrayRef, AsArray, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
    use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
    use otel_arrow_dfe_pdata::testing::round_trip::encode_logs;
    use std::sync::Arc;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
        }
    }

    fn logs_data() -> LogsData {
        let rec = |t: u64, body: &str, attrs: Vec<KeyValue>| LogRecord {
            time_unix_nano: t,
            observed_time_unix_nano: t + 1,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue(body.into())),
            }),
            attributes: attrs,
            event_name: "ev".into(),
            flags: 0x0000_0101,
            trace_id: vec![0xAA; 16],
            span_id: vec![0xBB; 8],
            ..Default::default()
        };
        LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1"), kv("service.name", "svc")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        rec(
                            1_000,
                            "a",
                            vec![kv("logger.name", "L1"), kv("request_id", "r1")],
                        ),
                        rec(2_000, "b", vec![kv("logger.name", "L1")]),
                        rec(0, "c", vec![kv("logger.name", "L2")]),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn cfg() -> LakeConfig {
        let mut cfg = LakeConfig::default();
        cfg.logs.series_attributes = vec!["logger.name".into()];
        cfg.logs.denormalize = vec![
            Denormalize {
                path: "resource.service.name".into(),
                column: "service_name".into(),
                ty: DenormType::String,
            },
            Denormalize {
                path: "attrs.request_id".into(),
                column: "request_id".into(),
                ty: DenormType::String,
            },
        ];
        cfg
    }

    /// Scenario: three log records, two distinct logger.name values, one zero timestamp.
    /// Guarantees: two descriptors, three value rows with series ids, allow-listed attrs
    /// excluded from the residual map, zero timestamp stored as null, denormalized columns filled.
    #[test]
    fn extracts_logs_descriptors_and_values() {
        let mut records = encode_logs(&logs_data());
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.signal, Signal::Logs);
        assert_eq!(out.descriptors.len(), 2);
        let (ds, batches) = &out.values[0];
        assert_eq!(*ds, Dataset::LogsValues);
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
        let b = &batches[0];
        let ids = b
            .column_by_name("series_id")
            .expect("series_id")
            .as_fixed_size_binary();
        assert_eq!(ids.value(0), ids.value(1));
        assert_ne!(ids.value(0), ids.value(2));
        let producer = b
            .column_by_name("producer_id")
            .expect("p")
            .as_string::<i32>();
        assert_eq!(producer.value(0), "h1");
        let time = b.column_by_name("time_unix_nano").expect("t");
        assert!(time.is_null(2));
        let svc = b
            .column_by_name("service_name")
            .expect("svc")
            .as_string::<i32>();
        assert_eq!(svc.value(0), "svc");
        let rid = b
            .column_by_name("request_id")
            .expect("rid")
            .as_string::<i32>();
        assert_eq!(rid.value(0), "r1");
        assert!(rid.is_null(1));
        let attrs = b.column_by_name("attrs").expect("attrs").as_map();
        // row 0 residual attrs: only request_id (logger.name is identity)
        assert_eq!(attrs.value_length(0), 1);
        let body = b.column_by_name("body").expect("body").as_string::<i32>();
        assert_eq!(body.value(0), "a");
        assert_eq!(body.value(1), "b");
        let sev_num = b
            .column_by_name("severity_number")
            .expect("sev num")
            .as_primitive::<Int32Type>();
        assert_eq!(sev_num.value(0), 9);
        let sev_text = b
            .column_by_name("severity_text")
            .expect("sev text")
            .as_string::<i32>();
        assert_eq!(sev_text.value(0), "INFO");
        let event = b
            .column_by_name("event_name")
            .expect("event_name")
            .as_string::<i32>();
        assert_eq!(event.value(0), "ev");
        let trace = b
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_fixed_size_binary();
        assert_eq!(trace.value(0), [0xAA; 16]);
        let span = b
            .column_by_name("span_id")
            .expect("span_id")
            .as_fixed_size_binary();
        assert_eq!(span.value(0), [0xBB; 8]);
        let flags = b
            .column_by_name("flags")
            .expect("flags")
            .as_primitive::<Int32Type>();
        assert_eq!(flags.value(0), 0x0000_0101);
        assert_eq!(out.stats.rows, 3);
        assert!(out.pinned_bytes > 0);
        // descriptor denorm: only the identity-path column (service_name)
        assert_eq!(out.descriptors[0].denorm.len(), 1);
    }

    /// Scenario: a descriptor row set is turned into a series batch.
    /// Guarantees: identity_bytes hashes to series_id and maps carry the attributes.
    #[test]
    fn series_batch_round_trip() {
        let mut records = encode_logs(&logs_data());
        let out = extract(&mut records, &cfg()).expect("extract");
        let rows: Vec<&DescriptorRow> = out.descriptors.iter().collect();
        let batch =
            series_batch(&rows, 1_700_000_000_000_000, Dataset::LogsSeries, &cfg()).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column_by_name("series_id")
            .expect("id")
            .as_fixed_size_binary();
        let ib = batch
            .column_by_name("identity_bytes")
            .expect("ib")
            .as_binary::<i32>();
        assert_eq!(ids.value(0), crate::canonical::series_id(ib.value(0)));
        let res = batch.column_by_name("resource_attrs").expect("r").as_map();
        assert_eq!(res.value_length(0), 2);
    }

    /// Scenario: max_extracted_bytes far below the request size.
    /// Guarantees: extraction stops with RequestTooLarge instead of allocating everything.
    #[test]
    fn extracted_budget_is_enforced() {
        let mut cfg = cfg();
        cfg.ingress.max_extracted_bytes = 1;
        let mut records = encode_logs(&logs_data());
        assert!(matches!(
            extract(&mut records, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge))
        ));
    }

    /// Scenario: one log record carries a 600 KiB bytes attribute. The decoded
    /// value tree is well under the default 1 MiB row limit, but `render_v1`
    /// hex-encodes bytes, so the stored map cell is over 1.2 MiB.
    /// Guarantees: the row limit is applied to the rendered cell, so the request
    /// is refused instead of being admitted on a tree-sized estimate that the
    /// stored row then exceeds.
    #[test]
    fn a_bytes_attribute_is_charged_its_rendered_hex_size() {
        const RAW: usize = 600 << 10;
        let cfg = LakeConfig::default();
        // The premise of the test: the tree fits the limit, the rendering does not.
        let attr = vec![("blob".to_string(), Value::Bytes(vec![0xABu8; RAW]))];
        assert!(crate::extract::kv_bytes(&attr) < cfg.ingress.max_row_bytes);
        assert!(crate::extract::rendered_kv_bytes(&attr) > cfg.ingress.max_row_bytes);

        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000,
                        attributes: vec![KeyValue {
                            key: "blob".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::BytesValue(vec![0xAB; RAW])),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        assert!(matches!(
            extract(&mut records, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge))
        ));
    }

    /// Scenario: max_row_bytes below the size of a single descriptor row.
    /// Guarantees: the descriptor row is subject to the row limit, not only values rows.
    #[test]
    fn descriptor_rows_are_subject_to_the_row_limit() {
        let mut cfg = cfg();
        cfg.ingress.max_row_bytes = 8;
        let mut records = encode_logs(&logs_data());
        assert!(matches!(
            extract(&mut records, &cfg),
            Err(Error::Refused(RefuseReason::RequestTooLarge))
        ));
    }

    /// Scenario: the same logs request, once with plain parent ids and once with
    /// pdata's quasi-delta transport-optimized parent ids on every attribute payload.
    /// Guarantees: `extract` decodes transport-optimized ids first, so both inputs
    /// produce the same series ids, the same descriptor count and the same row count.
    #[test]
    fn transport_optimized_ids_give_the_same_result() {
        let mut plain_records = encode_logs(&logs_data());
        let plain = extract(&mut plain_records, &cfg()).expect("extract plain");

        let mut optimized = encode_logs(&logs_data());
        // The whole-record encoder covers the Logs payload's own resource.id and
        // scope.id delta encoding as well as the three attribute payloads, so
        // both halves of the decode are exercised.
        optimized.encode_transport_optimized().expect("encode");
        let got = extract(&mut optimized, &cfg()).expect("extract optimized");

        let ids = |e: &Extracted| {
            let mut v: Vec<String> = e
                .descriptors
                .iter()
                .map(|d| crate::canonical::hex(&d.series_id))
                .collect();
            v.sort();
            v
        };
        assert_eq!(ids(&got), ids(&plain));
        assert_eq!(got.stats.rows, plain.stats.rows);
        let rows = |e: &Extracted| -> usize {
            e.values
                .iter()
                .flat_map(|(_, b)| b.iter())
                .map(|b| b.num_rows())
                .sum()
        };
        assert_eq!(rows(&got), rows(&plain));
    }

    /// Scenario: one log record carries attributes and one carries none, so pdata
    /// writes a null `id` for the second record.
    /// Guarantees: the record without attributes gets an empty identity attribute
    /// list instead of inheriting the attributes of log id 0, so the two records
    /// land in different series.
    #[test]
    fn records_without_attributes_do_not_inherit_log_zero() {
        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: 1_000,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("a".into())),
                            }),
                            attributes: vec![kv("logger.name", "L1")],
                            ..Default::default()
                        },
                        LogRecord {
                            time_unix_nano: 2_000,
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("b".into())),
                            }),
                            attributes: vec![],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.descriptors.len(), 2);
        let with_attrs = out
            .descriptors
            .iter()
            .find(|d| !d.descriptor.attrs.is_empty())
            .expect("attributed descriptor");
        let without = out
            .descriptors
            .iter()
            .find(|d| d.descriptor.attrs.is_empty())
            .expect("unattributed descriptor");
        assert_eq!(with_attrs.descriptor.attrs.len(), 1);
        assert_ne!(with_attrs.series_id, without.series_id);
    }

    /// Scenario: the Logs payload's `body` column is replaced by a plain string
    /// column and handed back to `OtapArrowRecords`.
    /// Guarantees: pdata validates the OTAP schema on the way in and refuses it,
    /// so a non-struct body cannot reach `extract` through the public API. The
    /// checked downcast that backs this up is covered directly by
    /// `extract::tests::any_value_col_refuses_a_non_struct_body`.
    #[test]
    fn pdata_refuses_a_non_struct_body_column() {
        let mut records = encode_logs(&logs_data());
        let logs = records.get(ArrowPayloadType::Logs).expect("logs").clone();
        let mut fields: Vec<Field> = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        for (i, f) in logs.schema().fields().iter().enumerate() {
            if f.name() == "body" {
                fields.push(Field::new("body", DataType::Utf8, true));
                cols.push(Arc::new(StringArray::from(vec!["x"; logs.num_rows()])) as ArrayRef);
            } else {
                fields.push(f.as_ref().clone());
                cols.push(logs.column(i).clone());
            }
        }
        let patched =
            RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).expect("patched batch");
        assert!(records.set(ArrowPayloadType::Logs, patched).is_err());
    }

    /// Scenario: the same request extracted with `max_extracted_bytes` set to
    /// exactly what the run measures, and to one byte less.
    /// Guarantees: the limit is enforced on the measured extracted output, so a
    /// values row is charged its estimate or its measurement but never both.
    #[test]
    fn budget_is_enforced_on_the_measured_output() {
        let mut probe = encode_logs(&logs_data());
        let out = extract(&mut probe, &cfg()).expect("extract");
        // What the budget holds once the run is sealed: the estimated descriptor
        // rows plus the measured pinned bytes of the values batches. The values
        // rows' estimates were uncharged when their run was sealed.
        let measured: usize = out
            .descriptors
            .iter()
            .map(|d| d.approx_bytes)
            .sum::<usize>()
            + out.pinned_bytes;

        let mut exact = cfg();
        exact.ingress.max_extracted_bytes = measured;
        let mut records = encode_logs(&logs_data());
        let _accepted = extract(&mut records, &exact).expect("accepted at the measured size");

        let mut tight = cfg();
        tight.ingress.max_extracted_bytes = measured - 1;
        let mut records = encode_logs(&logs_data());
        assert!(matches!(
            extract(&mut records, &tight),
            Err(Error::Refused(RefuseReason::RequestTooLarge))
        ));
    }

    /// Scenario: a denormalized column declared `int64` over a string resource
    /// attribute.
    /// Guarantees: the cell is stored as null and `denorm_type_mismatch` counts it.
    #[test]
    fn denorm_type_mismatch_is_counted() {
        let mut cfg = cfg();
        cfg.logs.denormalize = vec![Denormalize {
            path: "resource.service.name".into(),
            column: "service_name".into(),
            ty: DenormType::Int64,
        }];
        let mut records = encode_logs(&logs_data());
        let out = extract(&mut records, &cfg).expect("extract");
        assert!(out.stats.denorm_type_mismatch > 0);
        let (_, batches) = &out.values[0];
        let svc = batches[0]
            .column_by_name("service_name")
            .expect("service_name");
        assert!(svc.is_null(0));
    }

    /// Scenario: a log record whose `time_unix_nano` is above `i64::MAX`, so it
    /// arrives as a negative nanosecond count.
    /// Guarantees: both timestamp columns are null and `timestamp_out_of_range`
    /// counts the record once.
    #[test]
    fn timestamp_out_of_range_is_counted() {
        let data = LogsData {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv("host.id", "h1")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: u64::MAX,
                        observed_time_unix_nano: 1_000,
                        attributes: vec![kv("logger.name", "L1")],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut records = encode_logs(&data);
        let out = extract(&mut records, &cfg()).expect("extract");
        assert_eq!(out.stats.timestamp_out_of_range, 1);
        let (_, batches) = &out.values[0];
        let b = &batches[0];
        assert!(b.column_by_name("time_unix_nano").expect("t").is_null(0));
        assert!(b.column_by_name("time").expect("time").is_null(0));
    }
}
