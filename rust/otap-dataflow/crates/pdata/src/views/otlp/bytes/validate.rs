// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Schema-aware validation of the protobuf wire framing of an OTLP request.
//!
//! The byte views read a request lazily and treat a damaged nested message as
//! absent, so a body that is broken anywhere below its top level converts into
//! a request carrying fewer rows than it holds, or none, without an error.
//! [`validate_request`] walks every length-delimited field that the OTLP
//! schema defines as a sub-message and checks the framing of each nested
//! message the same way the top level is checked, so such a body is refused
//! instead.
//!
//! What is checked, in every message of the request, as prost 0.14 checks it:
//! - every field key is a well-formed varint inside the protobuf key range,
//!   with a non-zero field number and a wire type protobuf defines;
//! - every varint terminates within ten bytes and fits a `u64`, and every
//!   fixed-width or length-delimited value lies inside the enclosing message;
//! - a field the schema knows arrives with the wire type the schema gives it
//!   (a repeated scalar may also arrive packed);
//! - a `string` field holds valid UTF-8 (a `bytes` field may hold anything);
//! - a packed `fixed64`/`double` field holds a whole number of elements, and a
//!   packed varint field holds only well-formed varints;
//! - `AnyValue` arrays and key-value lists nest at most
//!   [`MAX_ANY_VALUE_NESTING_DEPTH`] levels.
//!
//! A field the schema does not know keeps protobuf skip semantics: its
//! framing is checked and its content is skipped. An unknown group (wire
//! types 3 and 4) is skipped when it is balanced -- closed by an end key of
//! its own field number, nested groups included, each group counted against
//! the same nesting limit -- and a stray or mismatched end group is refused.
//! No value is range-checked: that is content, not framing.
//!
//! Cost: each byte of the request is read once, by the innermost message that
//! holds it, so the walk is linear in the body size; it allocates nothing, and
//! its recursion depth is bounded by the schema's fixed levels plus three per
//! `AnyValue` nesting level, or one per unknown group level. String fields are
//! read a second time by the UTF-8 check.

use super::decode::{
    END_GROUP, START_GROUP, SkipError, read_key, read_varint, skip_group, value_range,
};
use crate::error::Error;
use crate::proto::consts::field_num::{common, logs, metrics, resource, traces};
use crate::proto::consts::wire_types::{FIXED32, FIXED64, LEN, VARINT};

/// The deepest nesting of `AnyValue` arrays and key-value lists an OTLP
/// request may carry and still pass [`validate_request`].
///
/// A top-level attribute value or log body that is an array or a key-value
/// list is one level; each array or list inside it adds one. Scalars add
/// none. The limit exists to bound the validator's recursion, not to judge
/// content, so it is set no lower than any consumer's own nesting limit.
pub const MAX_ANY_VALUE_NESTING_DEPTH: usize = 256;

/// The OTLP message types the validator descends into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Message {
    ExportLogsServiceRequest,
    ResourceLogs,
    ScopeLogs,
    LogRecord,
    ExportMetricsServiceRequest,
    ResourceMetrics,
    ScopeMetrics,
    Metric,
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
    Summary,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    Buckets,
    SummaryDataPoint,
    ValueAtQuantile,
    Exemplar,
    ExportTraceServiceRequest,
    ResourceSpans,
    ScopeSpans,
    Span,
    Event,
    Link,
    Status,
    Resource,
    EntityRef,
    InstrumentationScope,
    KeyValue,
    AnyValue,
    ArrayValue,
    KeyValueList,
}

/// What the schema says about one field number of one message.
#[derive(Clone, Copy)]
enum Field {
    /// A sub-message, always length-delimited.
    Message(Message),
    /// A singular or repeated non-packable value with this wire type
    /// (`bytes` fields are `LEN`).
    Scalar(u64),
    /// A singular or repeated `string`: `LEN`, holding valid UTF-8.
    Str,
    /// A repeated scalar with this element wire type, packed (`LEN`) or not.
    Packed(u64),
    /// Not in the schema: framing checked, content skipped.
    Unknown,
}

impl Message {
    /// The message's name as the OTLP schema spells it.
    const fn name(self) -> &'static str {
        match self {
            Self::ExportLogsServiceRequest => "ExportLogsServiceRequest",
            Self::ResourceLogs => "ResourceLogs",
            Self::ScopeLogs => "ScopeLogs",
            Self::LogRecord => "LogRecord",
            Self::ExportMetricsServiceRequest => "ExportMetricsServiceRequest",
            Self::ResourceMetrics => "ResourceMetrics",
            Self::ScopeMetrics => "ScopeMetrics",
            Self::Metric => "Metric",
            Self::Gauge => "Gauge",
            Self::Sum => "Sum",
            Self::Histogram => "Histogram",
            Self::ExponentialHistogram => "ExponentialHistogram",
            Self::Summary => "Summary",
            Self::NumberDataPoint => "NumberDataPoint",
            Self::HistogramDataPoint => "HistogramDataPoint",
            Self::ExponentialHistogramDataPoint => "ExponentialHistogramDataPoint",
            Self::Buckets => "ExponentialHistogramDataPoint.Buckets",
            Self::SummaryDataPoint => "SummaryDataPoint",
            Self::ValueAtQuantile => "SummaryDataPoint.ValueAtQuantile",
            Self::Exemplar => "Exemplar",
            Self::ExportTraceServiceRequest => "ExportTraceServiceRequest",
            Self::ResourceSpans => "ResourceSpans",
            Self::ScopeSpans => "ScopeSpans",
            Self::Span => "Span",
            Self::Event => "Span.Event",
            Self::Link => "Span.Link",
            Self::Status => "Status",
            Self::Resource => "Resource",
            Self::EntityRef => "EntityRef",
            Self::InstrumentationScope => "InstrumentationScope",
            Self::KeyValue => "KeyValue",
            Self::AnyValue => "AnyValue",
            Self::ArrayValue => "ArrayValue",
            Self::KeyValueList => "KeyValueList",
        }
    }

    /// Whether entering this message is one more `AnyValue` nesting level.
    const fn is_value_container(self) -> bool {
        matches!(self, Self::ArrayValue | Self::KeyValueList)
    }

    /// The schema of field `num` of this message.
    #[inline]
    fn field(self, num: u64) -> Field {
        use Field::{Message as Sub, Packed, Scalar, Str, Unknown};
        use Message as M;
        match self {
            M::ExportLogsServiceRequest => match num {
                logs::LOGS_DATA_RESOURCE => Sub(M::ResourceLogs),
                _ => Unknown,
            },
            M::ResourceLogs => match num {
                logs::RESOURCE_LOGS_RESOURCE => Sub(M::Resource),
                logs::RESOURCE_LOGS_SCOPE_LOGS => Sub(M::ScopeLogs),
                logs::RESOURCE_LOGS_SCHEMA_URL => Str,
                _ => Unknown,
            },
            M::ScopeLogs => match num {
                logs::SCOPE_LOG_SCOPE => Sub(M::InstrumentationScope),
                logs::SCOPE_LOGS_LOG_RECORDS => Sub(M::LogRecord),
                logs::SCOPE_LOGS_SCHEMA_URL => Str,
                _ => Unknown,
            },
            M::LogRecord => match num {
                logs::LOG_RECORD_TIME_UNIX_NANO | logs::LOG_RECORD_OBSERVED_TIME_UNIX_NANO => {
                    Scalar(FIXED64)
                }
                logs::LOG_RECORD_SEVERITY_NUMBER | logs::LOG_RECORD_DROPPED_ATTRIBUTES_COUNT => {
                    Scalar(VARINT)
                }
                logs::LOG_RECORD_SEVERITY_TEXT | logs::LOG_RECORD_EVENT_NAME => Str,
                logs::LOG_RECORD_TRACE_ID | logs::LOG_RECORD_SPAN_ID => Scalar(LEN),
                logs::LOG_RECORD_BODY => Sub(M::AnyValue),
                logs::LOG_RECORD_ATTRIBUTES => Sub(M::KeyValue),
                logs::LOG_RECORD_FLAGS => Scalar(FIXED32),
                _ => Unknown,
            },
            M::ExportMetricsServiceRequest => match num {
                metrics::METRICS_DATA_RESOURCE_METRICS => Sub(M::ResourceMetrics),
                _ => Unknown,
            },
            M::ResourceMetrics => match num {
                metrics::RESOURCE_METRICS_RESOURCE => Sub(M::Resource),
                metrics::RESOURCE_METRICS_SCOPE_METRICS => Sub(M::ScopeMetrics),
                metrics::RESOURCE_METRICS_SCHEMA_URL => Str,
                _ => Unknown,
            },
            M::ScopeMetrics => match num {
                metrics::SCOPE_METRICS_SCOPE => Sub(M::InstrumentationScope),
                metrics::SCOPE_METRICS_METRICS => Sub(M::Metric),
                metrics::SCOPE_METRICS_SCHEMA_URL => Str,
                _ => Unknown,
            },
            M::Metric => match num {
                metrics::METRIC_NAME | metrics::METRIC_DESCRIPTION | metrics::METRIC_UNIT => Str,
                metrics::METRIC_GAUGE => Sub(M::Gauge),
                metrics::METRIC_SUM => Sub(M::Sum),
                metrics::METRIC_HISTOGRAM => Sub(M::Histogram),
                metrics::METRIC_EXPONENTIAL_HISTOGRAM => Sub(M::ExponentialHistogram),
                metrics::METRIC_SUMMARY => Sub(M::Summary),
                metrics::METRIC_METADATA => Sub(M::KeyValue),
                _ => Unknown,
            },
            M::Gauge => match num {
                metrics::GAUGE_DATA_POINTS => Sub(M::NumberDataPoint),
                _ => Unknown,
            },
            M::Sum => match num {
                metrics::SUM_DATA_POINTS => Sub(M::NumberDataPoint),
                metrics::SUM_AGGREGATION_TEMPORALITY | metrics::SUM_IS_MONOTONIC => Scalar(VARINT),
                _ => Unknown,
            },
            M::Histogram => match num {
                metrics::HISTOGRAM_DATA_POINTS => Sub(M::HistogramDataPoint),
                metrics::HISTOGRAM_AGGREGATION_TEMPORALITY => Scalar(VARINT),
                _ => Unknown,
            },
            M::ExponentialHistogram => match num {
                metrics::EXPONENTIAL_HISTOGRAM_DATA_POINTS => Sub(M::ExponentialHistogramDataPoint),
                metrics::EXPONENTIAL_HISTOGRAM_AGGREGATION_TEMPORALITY => Scalar(VARINT),
                _ => Unknown,
            },
            M::Summary => match num {
                metrics::SUMMARY_DATA_POINTS => Sub(M::SummaryDataPoint),
                _ => Unknown,
            },
            M::NumberDataPoint => match num {
                metrics::NUMBER_DP_ATTRIBUTES => Sub(M::KeyValue),
                metrics::NUMBER_DP_START_TIME_UNIX_NANO
                | metrics::NUMBER_DP_TIME_UNIX_NANO
                | metrics::NUMBER_DP_AS_DOUBLE
                | metrics::NUMBER_DP_AS_INT => Scalar(FIXED64),
                metrics::NUMBER_DP_EXEMPLARS => Sub(M::Exemplar),
                metrics::NUMBER_DP_FLAGS => Scalar(VARINT),
                _ => Unknown,
            },
            M::HistogramDataPoint => match num {
                metrics::HISTOGRAM_DP_ATTRIBUTES => Sub(M::KeyValue),
                metrics::HISTOGRAM_DP_START_TIME_UNIX_NANO
                | metrics::HISTOGRAM_DP_TIME_UNIX_NANO
                | metrics::HISTOGRAM_DP_COUNT
                | metrics::HISTOGRAM_DP_SUM
                | metrics::HISTOGRAM_DP_MIN
                | metrics::HISTOGRAM_DP_MAX => Scalar(FIXED64),
                metrics::HISTOGRAM_DP_BUCKET_COUNTS | metrics::HISTOGRAM_DP_EXPLICIT_BOUNDS => {
                    Packed(FIXED64)
                }
                metrics::HISTOGRAM_DP_EXEMPLARS => Sub(M::Exemplar),
                metrics::HISTOGRAM_DP_FLAGS => Scalar(VARINT),
                _ => Unknown,
            },
            M::ExponentialHistogramDataPoint => match num {
                metrics::EXP_HISTOGRAM_DP_ATTRIBUTES => Sub(M::KeyValue),
                metrics::EXP_HISTOGRAM_DP_START_TIME_UNIX_NANO
                | metrics::EXP_HISTOGRAM_DP_TIME_UNIX_NANO
                | metrics::EXP_HISTOGRAM_DP_COUNT
                | metrics::EXP_HISTOGRAM_DP_SUM
                | metrics::EXP_HISTOGRAM_DP_ZERO_COUNT
                | metrics::EXP_HISTOGRAM_DP_MIN
                | metrics::EXP_HISTOGRAM_DP_MAX
                | metrics::EXP_HISTOGRAM_DP_ZERO_THRESHOLD => Scalar(FIXED64),
                metrics::EXP_HISTOGRAM_DP_SCALE | metrics::EXP_HISTOGRAM_DP_FLAGS => Scalar(VARINT),
                metrics::EXP_HISTOGRAM_DP_POSITIVE | metrics::EXP_HISTOGRAM_DP_NEGATIVE => {
                    Sub(M::Buckets)
                }
                metrics::EXP_HISTOGRAM_DP_EXEMPLARS => Sub(M::Exemplar),
                _ => Unknown,
            },
            M::Buckets => match num {
                metrics::EXP_HISTOGRAM_BUCKET_OFFSET => Scalar(VARINT),
                metrics::EXP_HISTOGRAM_BUCKET_BUCKET_COUNTS => Packed(VARINT),
                _ => Unknown,
            },
            M::SummaryDataPoint => match num {
                metrics::SUMMARY_DP_ATTRIBUTES => Sub(M::KeyValue),
                metrics::SUMMARY_DP_START_TIME_UNIX_NANO
                | metrics::SUMMARY_DP_TIME_UNIX_NANO
                | metrics::SUMMARY_DP_COUNT
                | metrics::SUMMARY_DP_SUM => Scalar(FIXED64),
                metrics::SUMMARY_DP_QUANTILE_VALUES => Sub(M::ValueAtQuantile),
                metrics::SUMMARY_DP_FLAGS => Scalar(VARINT),
                _ => Unknown,
            },
            M::ValueAtQuantile => match num {
                metrics::VALUE_AT_QUANTILE_QUANTILE | metrics::VALUE_AT_QUANTILE_VALUE => {
                    Scalar(FIXED64)
                }
                _ => Unknown,
            },
            M::Exemplar => match num {
                metrics::EXEMPLAR_FILTERED_ATTRIBUTES => Sub(M::KeyValue),
                metrics::EXEMPLAR_TIME_UNIX_NANO
                | metrics::EXEMPLAR_AS_DOUBLE
                | metrics::EXEMPLAR_AS_INT => Scalar(FIXED64),
                metrics::EXEMPLAR_SPAN_ID | metrics::EXEMPLAR_TRACE_ID => Scalar(LEN),
                _ => Unknown,
            },
            M::ExportTraceServiceRequest => match num {
                traces::TRACES_DATA_RESOURCE_SPANS => Sub(M::ResourceSpans),
                _ => Unknown,
            },
            M::ResourceSpans => match num {
                traces::RESOURCE_SPANS_RESOURCE => Sub(M::Resource),
                traces::RESOURCE_SPANS_SCOPE_SPANS => Sub(M::ScopeSpans),
                traces::RESOURCE_SPANS_SCHEMA_URL => Str,
                _ => Unknown,
            },
            M::ScopeSpans => match num {
                traces::SCOPE_SPANS_SCOPE => Sub(M::InstrumentationScope),
                traces::SCOPE_SPANS_SPANS => Sub(M::Span),
                traces::SCOPE_SPANS_SCHEMA_URL => Str,
                _ => Unknown,
            },
            M::Span => match num {
                traces::SPAN_TRACE_ID | traces::SPAN_SPAN_ID | traces::SPAN_PARENT_SPAN_ID => {
                    Scalar(LEN)
                }
                traces::SPAN_TRACE_STATE | traces::SPAN_NAME => Str,
                traces::SPAN_FLAGS => Scalar(FIXED32),
                traces::SPAN_KIND
                | traces::SPAN_DROPPED_ATTRIBUTES_COUNT
                | traces::SPAN_DROPPED_EVENTS_COUNT
                | traces::SPAN_DROPPED_LINKS_COUNT => Scalar(VARINT),
                traces::SPAN_START_TIME_UNIX_NANO | traces::SPAN_END_TIME_UNIX_NANO => {
                    Scalar(FIXED64)
                }
                traces::SPAN_ATTRIBUTES => Sub(M::KeyValue),
                traces::SPAN_EVENTS => Sub(M::Event),
                traces::SPAN_LINKS => Sub(M::Link),
                traces::SPAN_STATUS => Sub(M::Status),
                _ => Unknown,
            },
            M::Event => match num {
                traces::SPAN_EVENT_TIME_UNIX_NANO => Scalar(FIXED64),
                traces::SPAN_EVENT_NAME => Str,
                traces::SPAN_EVENT_ATTRIBUTES => Sub(M::KeyValue),
                traces::SPAN_EVENT_DROPPED_ATTRIBUTES_COUNTS => Scalar(VARINT),
                _ => Unknown,
            },
            M::Link => match num {
                traces::SPAN_LINK_TRACE_ID | traces::SPAN_LINK_SPAN_ID => Scalar(LEN),
                traces::SPAN_LINK_TRACE_STATE => Str,
                traces::SPAN_LINK_ATTRIBUTES => Sub(M::KeyValue),
                traces::SPAN_LINK_DROPPED_ATTRIBUTES_COUNT => Scalar(VARINT),
                traces::SPAN_LINK_FLAGS => Scalar(FIXED32),
                _ => Unknown,
            },
            // Field 1 is the reserved `deprecated_code`, skipped as unknown.
            M::Status => match num {
                traces::SPAN_STATUS_MESSAGE => Str,
                traces::SPAN_STATUS_CODE => Scalar(VARINT),
                _ => Unknown,
            },
            M::Resource => match num {
                resource::RESOURCE_ATTRIBUTES => Sub(M::KeyValue),
                resource::RESOURCE_DROPPED_ATTRIBUTES_COUNT => Scalar(VARINT),
                resource::RESOURCE_ENTITY_REFS => Sub(M::EntityRef),
                _ => Unknown,
            },
            M::EntityRef => match num {
                common::ENTITY_REF_SCHEMA_URL
                | common::ENTITY_REF_TYPE
                | common::ENTITY_REF_ID_KEYS
                | common::ENTITY_REF_DESCRIPTION_KEYS => Str,
                _ => Unknown,
            },
            M::InstrumentationScope => match num {
                common::INSTRUMENTATION_SCOPE_NAME | common::INSTRUMENTATION_SCOPE_VERSION => Str,
                common::INSTRUMENTATION_SCOPE_ATTRIBUTES => Sub(M::KeyValue),
                common::INSTRUMENTATION_DROPPED_ATTRIBUTES_COUNT => Scalar(VARINT),
                _ => Unknown,
            },
            M::KeyValue => match num {
                common::KEY_VALUE_KEY => Str,
                common::KEY_VALUE_VALUE => Sub(M::AnyValue),
                _ => Unknown,
            },
            M::AnyValue => match num {
                common::ANY_VALUE_STRING_VALUE => Str,
                common::ANY_VALUE_BYTES_VALUE => Scalar(LEN),
                common::ANY_VALUE_BOOL_VALUE | common::ANY_VALUE_INT_VALUE => Scalar(VARINT),
                common::ANY_VALUE_DOUBLE_VALUE => Scalar(FIXED64),
                common::ANY_VALUE_ARRAY_VALUE => Sub(M::ArrayValue),
                common::ANY_VALUE_KVLIST_VALUE => Sub(M::KeyValueList),
                _ => Unknown,
            },
            M::ArrayValue => match num {
                common::ARRAY_VALUE_VALUES => Sub(M::AnyValue),
                _ => Unknown,
            },
            M::KeyValueList => match num {
                common::KEY_VALUE_LIST_VALUES => Sub(M::KeyValue),
                _ => Unknown,
            },
        }
    }
}

/// Validate the wire framing of `buf`, an OTLP message of type `root`, and of
/// every sub-message the schema defines inside it. See the module
/// documentation for what is checked.
///
/// # Errors
/// [`Error::InvalidOtlpWireFormat`] naming the problem, the innermost message
/// holding it and its byte offset in `buf`, or [`Error::OtlpNestingTooDeep`].
pub(crate) fn validate_request(buf: &[u8], root: Message) -> Result<(), Error> {
    walk(buf, 0, root, 0).map_err(|damage| match damage {
        Damage::Framing {
            problem,
            message,
            offset,
        } => Error::InvalidOtlpWireFormat {
            problem,
            message: message.name(),
            offset,
        },
        Damage::TooDeep { offset } => Error::OtlpNestingTooDeep {
            limit: MAX_ANY_VALUE_NESTING_DEPTH,
            offset,
        },
    })
}

/// Where and how a request is damaged: a small value, so the recursive walk
/// returns it cheaply and only the outermost call builds the crate error.
enum Damage {
    Framing {
        problem: &'static str,
        message: Message,
        offset: usize,
    },
    TooDeep {
        offset: usize,
    },
}

/// Walk one message of type `message`; `base` is the offset of `buf` within
/// the request and `depth` the `AnyValue` nesting level `buf` is at.
fn walk(buf: &[u8], base: usize, message: Message, depth: usize) -> Result<(), Damage> {
    let fail = |problem: &'static str, at: usize| Damage::Framing {
        problem,
        message,
        offset: base + at,
    };
    let mut pos = 0;
    while pos < buf.len() {
        let at = pos;
        let (field_num, wire_type, next) =
            read_key(buf, pos).map_err(|problem| fail(problem, at))?;
        let field = message.field(field_num);
        if wire_type == START_GROUP || wire_type == END_GROUP {
            if wire_type == END_GROUP {
                return Err(fail("end group without a start group", at));
            }
            if !matches!(field, Field::Unknown) {
                return Err(fail("wrong wire type for a known field", at));
            }
            pos = skip_group(buf, next, field_num, depth + 1, at).map_err(|skip| match skip {
                SkipError::Framing { problem, at } => fail(problem, at),
                SkipError::TooDeep { at } => Damage::TooDeep { offset: base + at },
            })?;
            continue;
        }
        let (start, end) =
            value_range(buf, wire_type, next).map_err(|problem| fail(problem, at))?;
        match field {
            Field::Unknown => {}
            Field::Scalar(expected) => {
                if wire_type != expected {
                    return Err(fail("wrong wire type for a known field", at));
                }
            }
            Field::Str => {
                if wire_type != LEN {
                    return Err(fail("wrong wire type for a known field", at));
                }
                if std::str::from_utf8(&buf[start..end]).is_err() {
                    return Err(fail("invalid UTF-8 in a string field", at));
                }
            }
            Field::Packed(element) => {
                if wire_type == LEN {
                    check_packed(&buf[start..end], element).map_err(|problem| fail(problem, at))?;
                } else if wire_type != element {
                    return Err(fail("wrong wire type for a known field", at));
                }
            }
            Field::Message(child) => {
                if wire_type != LEN {
                    return Err(fail("wrong wire type for a known field", at));
                }
                let depth = depth + usize::from(child.is_value_container());
                if depth > MAX_ANY_VALUE_NESTING_DEPTH {
                    return Err(Damage::TooDeep { offset: base + at });
                }
                walk(&buf[start..end], base + start, child, depth)?;
            }
        }
        pos = end;
    }
    Ok(())
}

/// Check the payload of a packed repeated field of `element` wire type.
fn check_packed(payload: &[u8], element: u64) -> Result<(), &'static str> {
    if element == FIXED64 {
        return if payload.len().is_multiple_of(8) {
            Ok(())
        } else {
            Err("packed fixed64 field is not a whole number of elements")
        };
    }
    let mut pos = 0;
    while pos < payload.len() {
        let (_, next) =
            read_varint(payload, pos).ok_or("truncated or overlong varint in a packed field")?;
        pos = next;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
    use crate::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
    use crate::proto::opentelemetry::collector::trace::v1::ExportTraceServiceRequest;
    use crate::proto::opentelemetry::common::v1::{
        AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList, any_value,
    };
    use crate::proto::opentelemetry::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use crate::proto::opentelemetry::metrics::v1::{
        Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
        SummaryDataPoint, exemplar, exponential_histogram_data_point::Buckets, metric,
        number_data_point, summary_data_point::ValueAtQuantile,
    };
    use crate::proto::opentelemetry::resource::v1::Resource;
    use crate::proto::opentelemetry::trace::v1::{
        ResourceSpans, ScopeSpans, Span, Status,
        span::{Event, Link},
    };
    use prost::Message as _;
    use prost::encoding::{WireType, encode_key, encode_varint};

    fn len_field(field: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode_key(field, WireType::LengthDelimited, &mut out);
        encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    }

    fn string(value: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }
    }

    /// Every kind of attribute value, nested arrays and lists included.
    fn attributes() -> Vec<KeyValue> {
        let scalars = vec![
            string("s"),
            AnyValue {
                value: Some(any_value::Value::BoolValue(true)),
            },
            AnyValue {
                value: Some(any_value::Value::IntValue(-3)),
            },
            AnyValue {
                value: Some(any_value::Value::DoubleValue(1.5)),
            },
            AnyValue {
                value: Some(any_value::Value::BytesValue(vec![1, 2])),
            },
        ];
        vec![
            KeyValue {
                key: "array".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::ArrayValue(ArrayValue {
                        values: scalars.clone(),
                    })),
                }),
            },
            KeyValue {
                key: "map".into(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::KvlistValue(KeyValueList {
                        values: vec![KeyValue {
                            key: "inner".into(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::ArrayValue(ArrayValue {
                                    values: scalars,
                                })),
                            }),
                        }],
                    })),
                }),
            },
            KeyValue {
                key: "k".into(),
                value: Some(string("v")),
            },
        ]
    }

    fn resource() -> Option<Resource> {
        Some(Resource {
            attributes: attributes(),
            dropped_attributes_count: 1,
            entity_refs: vec![EntityRef {
                schema_url: "u".into(),
                r#type: "service".into(),
                id_keys: vec!["service.name".into()],
                description_keys: vec!["d".into()],
            }],
        })
    }

    fn scope() -> Option<InstrumentationScope> {
        Some(InstrumentationScope {
            name: "n".into(),
            version: "v".into(),
            attributes: attributes(),
            dropped_attributes_count: 2,
        })
    }

    fn logs_request() -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: resource(),
                scope_logs: vec![ScopeLogs {
                    scope: scope(),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1,
                        observed_time_unix_nano: 2,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(string("body")),
                        attributes: attributes(),
                        dropped_attributes_count: 3,
                        flags: 1,
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        event_name: "e".into(),
                    }],
                    schema_url: "s".into(),
                }],
                schema_url: "r".into(),
            }],
        }
        .encode_to_vec()
    }

    fn exemplars() -> Vec<Exemplar> {
        vec![
            Exemplar {
                filtered_attributes: attributes(),
                time_unix_nano: 1,
                span_id: vec![1; 8],
                trace_id: vec![2; 16],
                value: Some(exemplar::Value::AsDouble(0.5)),
            },
            Exemplar {
                value: Some(exemplar::Value::AsInt(-1)),
                ..Default::default()
            },
        ]
    }

    fn metrics_request() -> Vec<u8> {
        let number_point = |value| NumberDataPoint {
            attributes: attributes(),
            start_time_unix_nano: 1,
            time_unix_nano: 2,
            exemplars: exemplars(),
            flags: 1,
            value: Some(value),
        };
        let metric = |data, metadata| Metric {
            name: "m".into(),
            description: "d".into(),
            unit: "1".into(),
            metadata,
            data: Some(data),
        };
        let buckets = Some(Buckets {
            offset: -2,
            bucket_counts: vec![1, 300, 70_000],
        });
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: resource(),
                scope_metrics: vec![ScopeMetrics {
                    scope: scope(),
                    metrics: vec![
                        metric(
                            metric::Data::Gauge(Gauge {
                                data_points: vec![number_point(
                                    number_data_point::Value::AsDouble(1.0),
                                )],
                            }),
                            attributes(),
                        ),
                        metric(
                            metric::Data::Sum(Sum {
                                data_points: vec![number_point(number_data_point::Value::AsInt(
                                    -7,
                                ))],
                                aggregation_temporality: 2,
                                is_monotonic: true,
                            }),
                            vec![],
                        ),
                        metric(
                            metric::Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    attributes: attributes(),
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 2,
                                    count: 6,
                                    sum: Some(3.0),
                                    bucket_counts: vec![1, 2, 3],
                                    explicit_bounds: vec![0.5, 1.5],
                                    exemplars: exemplars(),
                                    flags: 1,
                                    min: Some(0.1),
                                    max: Some(2.0),
                                }],
                                aggregation_temporality: 1,
                            }),
                            vec![],
                        ),
                        metric(
                            metric::Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    attributes: attributes(),
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 2,
                                    count: 9,
                                    sum: Some(4.0),
                                    scale: -1,
                                    zero_count: 1,
                                    positive: buckets.clone(),
                                    negative: buckets,
                                    flags: 1,
                                    exemplars: exemplars(),
                                    min: Some(0.0),
                                    max: Some(9.0),
                                    zero_threshold: 0.001,
                                }],
                                aggregation_temporality: 2,
                            }),
                            vec![],
                        ),
                        metric(
                            metric::Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint {
                                    attributes: attributes(),
                                    start_time_unix_nano: 1,
                                    time_unix_nano: 2,
                                    count: 3,
                                    sum: 6.0,
                                    quantile_values: vec![ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 2.0,
                                    }],
                                    flags: 1,
                                }],
                            }),
                            vec![],
                        ),
                    ],
                    schema_url: "s".into(),
                }],
                schema_url: "r".into(),
            }],
        }
        .encode_to_vec()
    }

    fn traces_request() -> Vec<u8> {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: resource(),
                scope_spans: vec![ScopeSpans {
                    scope: scope(),
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: "t".into(),
                        parent_span_id: vec![3; 8],
                        flags: 1,
                        name: "span".into(),
                        kind: 2,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: attributes(),
                        dropped_attributes_count: 1,
                        events: vec![Event {
                            time_unix_nano: 1,
                            name: "e".into(),
                            attributes: attributes(),
                            dropped_attributes_count: 1,
                        }],
                        dropped_events_count: 1,
                        links: vec![Link {
                            trace_id: vec![4; 16],
                            span_id: vec![5; 8],
                            trace_state: "l".into(),
                            attributes: attributes(),
                            dropped_attributes_count: 1,
                            flags: 1,
                        }],
                        dropped_links_count: 1,
                        status: Some(Status {
                            message: "ok".into(),
                            code: 1,
                        }),
                    }],
                    schema_url: "s".into(),
                }],
                schema_url: "r".into(),
            }],
        }
        .encode_to_vec()
    }

    fn requests() -> [(Message, Vec<u8>); 3] {
        [
            (Message::ExportLogsServiceRequest, logs_request()),
            (Message::ExportMetricsServiceRequest, metrics_request()),
            (Message::ExportTraceServiceRequest, traces_request()),
        ]
    }

    /// Scenario: a logs, a metrics and a traces request built by prost that
    /// set every field of every message the validator knows -- all five
    /// metric types, exemplars, packed bucket counts and bounds, entity refs,
    /// span events, links and status, and nested array and key-value list
    /// attribute values.
    /// Guarantees: each passes, so the schema table matches the wire types a
    /// conforming encoder writes and a well-formed request is never refused.
    #[test]
    fn a_fully_populated_request_passes() {
        for (root, body) in requests() {
            assert!(validate_request(&body, root).is_ok(), "{root:?}");
        }
    }

    /// Unknown content a newer or proto2 sender may put in any message: a
    /// balanced group of field 31 holding a varint field and a nested group
    /// of field 32, then field 31 as a length-delimited value whose bytes
    /// look like a `resource_logs` / `values` / `key` field (`0a 00`), then
    /// field 31 as a varint. No OTLP message defines field 31 or 32.
    const UNKNOWN: &[u8] = &[
        0xfb, 0x01, 0x08, 0x05, 0x83, 0x02, 0x84, 0x02, 0xfc, 0x01, // group 31
        0xfa, 0x01, 0x02, 0x0a, 0x00, // field 31, LEN, `0a 00`
        0xf8, 0x01, 0x0a, // field 31, varint
    ];

    /// Re-encode `buf`, a message of type `message`, with `UNKNOWN` placed
    /// before its first field, and every sub-message the schema defines
    /// inside it re-encoded the same way.
    fn decorate(buf: &[u8], message: Message) -> Vec<u8> {
        let mut out = UNKNOWN.to_vec();
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, next) = read_key(buf, pos).expect("key");
            let (start, end) = value_range(buf, wire_type, next).expect("value");
            match message.field(field_num) {
                Field::Message(child) => {
                    let inner = decorate(&buf[start..end], child);
                    out.extend(len_field(field_num as u32, &inner));
                }
                _ => out.extend_from_slice(&buf[pos..end]),
            }
            pos = end;
        }
        out
    }

    /// Scenario: each fully populated logs, metrics and traces request, and
    /// the same request with a balanced unknown group, an unknown
    /// length-delimited field whose bytes look like a known field, and an
    /// unknown varint placed before the first field of every message -- the
    /// request itself, resources, scopes, log records, metrics, every data
    /// point kind, exemplars, spans, events, links, key-values, any-values,
    /// arrays and lists.
    /// Guarantees: the decorated request passes validation and converts to
    /// exactly the OTAP records the plain one does, so every field after
    /// unknown content is still read by the byte views: nothing the
    /// validator accepts is silently dropped, and no unknown bytes are read
    /// as a phantom record.
    #[test]
    fn unknown_content_before_known_fields_is_skipped_by_the_views() {
        use crate::otap::OtapArrowRecords;
        use crate::{OtapPayload, OtlpProtoBytes, TryIntoWithOptions};
        use otel_arrow_dfe_config::SignalType;

        let convert = |signal, body: Vec<u8>| {
            let payload = OtapPayload::from(OtlpProtoBytes::new_from_bytes(signal, body));
            let records: OtapArrowRecords = payload.try_into_with_default().expect("converts");
            format!("{records:?}")
        };
        for ((root, body), signal) in
            requests()
                .into_iter()
                .zip([SignalType::Logs, SignalType::Metrics, SignalType::Traces])
        {
            let decorated = decorate(&body, root);
            assert!(decorated.len() > body.len() + 100, "{root:?}");
            assert!(validate_request(&decorated, root).is_ok(), "{root:?}");
            assert_eq!(
                convert(signal, decorated),
                convert(signal, body),
                "{root:?}: the unknown content changed what the views read"
            );
        }
    }

    /// Scenario: every proper prefix of each fully populated request, and
    /// each request with one byte of a nested length prefix changed.
    /// Guarantees: no prefix that ends inside a field passes: a prefix passes
    /// only if it ends exactly on a field boundary of the top level, where
    /// it is itself a well-formed, shorter request. So a body cut anywhere
    /// inside a nested message is refused, however deep the cut.
    #[test]
    fn a_body_cut_inside_any_nested_message_is_refused() {
        for (root, body) in requests() {
            let mut boundaries = Vec::new();
            let mut pos = 0;
            while pos < body.len() {
                let (_, next) = read_varint(&body, pos).expect("key");
                let (_, end) = value_range(&body, LEN, next).expect("value");
                boundaries.push(end);
                pos = end;
            }
            for cut in 1..body.len() {
                let result = validate_request(&body[..cut], root);
                assert_eq!(
                    result.is_ok(),
                    boundaries.contains(&cut),
                    "{root:?} cut at {cut}: {result:?}"
                );
            }
        }
    }

    /// Scenario: a request whose only damage is several levels deep -- a
    /// string `AnyValue` inside a log record attribute declaring more bytes
    /// than it has.
    /// Guarantees: it is refused as `InvalidOtlpWireFormat` naming `AnyValue`
    /// and the byte offset of the damaged field within the whole request.
    #[test]
    fn deep_damage_names_the_message_and_offset() {
        let any_value = [0x0a, 0x05, b'a'];
        let key_value = [len_field(1, b"k"), len_field(2, &any_value)].concat();
        let log_record = len_field(6, &key_value);
        let scope_logs = len_field(2, &log_record);
        let resource_logs = len_field(2, &scope_logs);
        let body = len_field(1, &resource_logs);
        // Each of the five enclosing fields adds a one-byte key and a
        // one-byte length before the AnyValue's content, and the KeyValue's
        // key field adds three more.
        let expected = 5 * 2 + 3;
        match validate_request(&body, Message::ExportLogsServiceRequest) {
            Err(Error::InvalidOtlpWireFormat {
                problem,
                message,
                offset,
            }) => {
                assert_eq!(problem, "length-delimited field overruns its message");
                assert_eq!(message, "AnyValue");
                assert_eq!(offset, expected);
                assert_eq!(body[offset], 0x0a);
            }
            other => panic!("expected a framing error, got {other:?}"),
        }
    }

    /// Scenario: a log record carrying unknown fields of every wire type,
    /// one of them length-delimited with bytes that are not a valid message,
    /// and an unknown field that overruns its record.
    /// Guarantees: unknown fields keep proto3 skip semantics -- their framing
    /// is checked but their content is never parsed -- so a newer sender's
    /// extension fields pass and only a broken frame is refused.
    #[test]
    fn unknown_fields_are_skipped_but_framed() {
        let mut record = vec![0xf8, 0x01, 0x05]; // field 31, varint
        record.extend([0xf9, 0x01, 1, 2, 3, 4, 5, 6, 7, 8]); // field 31, fixed64
        record.extend([0xfd, 0x01, 1, 2, 3, 4]); // field 31, fixed32
        record.extend(len_field(31, &[0xff, 0xff, 0xff])); // not a message
        let body = len_field(1, &len_field(2, &len_field(2, &record)));
        assert!(validate_request(&body, Message::ExportLogsServiceRequest).is_ok());

        let mut overrun = record.clone();
        overrun.extend([0xfa, 0x01, 0x09, 0x00]); // field 31, 9 bytes declared
        let body = len_field(1, &len_field(2, &len_field(2, &overrun)));
        assert!(matches!(
            validate_request(&body, Message::ExportLogsServiceRequest),
            Err(Error::InvalidOtlpWireFormat {
                message: "LogRecord",
                ..
            })
        ));
    }

    /// Scenario: known fields sent with a wire type the schema does not give
    /// them -- `ResourceLogs` as a varint, a log record's `time_unix_nano`
    /// as a varint, an `AnyValue` string as fixed32 -- and field keys using
    /// the obsolete group wire types or field number zero.
    /// Guarantees: each is refused, as prost would refuse it, rather than
    /// being read by the lazy views as garbage or silently dropped.
    #[test]
    fn a_known_field_with_the_wrong_wire_type_is_refused() {
        let root = Message::ExportLogsServiceRequest;
        assert!(validate_request(&[0x08, 0x01], root).is_err());
        let record = [0x08, 0x01];
        let body = len_field(1, &len_field(2, &len_field(2, &record)));
        assert!(validate_request(&body, root).is_err());
        let any_value = [0x0d, 1, 2, 3, 4];
        let key_value = len_field(2, &any_value);
        let body = len_field(1, &len_field(2, &len_field(2, &len_field(6, &key_value))));
        assert!(validate_request(&body, root).is_err());
        for key in [0x0b, 0x0c, 0x0e, 0x0f, 0x02] {
            assert!(validate_request(&[key, 0x00], root).is_err(), "{key:#x}");
        }
    }

    /// Scenario: histogram bucket counts and bounds sent unpacked, packed
    /// with a length that is not a multiple of eight, and exponential
    /// histogram bucket counts packed with a truncated varint.
    /// Guarantees: both proto3 encodings of a repeated scalar pass, and a
    /// packed field whose payload does not divide into whole elements is
    /// refused rather than read as fewer buckets than were sent.
    #[test]
    fn packed_fields_must_hold_whole_elements() {
        let root = Message::ExportMetricsServiceRequest;
        let wrap_histogram = |point: &[u8]| {
            let histogram = len_field(1, point);
            let metric = len_field(9, &histogram);
            len_field(1, &len_field(2, &len_field(2, &metric)))
        };
        // Field 6, fixed64, twice (unpacked); field 7, fixed64 (unpacked).
        let mut unpacked = vec![0x31, 1, 0, 0, 0, 0, 0, 0, 0, 0x31, 2, 0, 0, 0, 0, 0, 0, 0];
        unpacked.extend([0x39, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f]);
        assert!(validate_request(&wrap_histogram(&unpacked), root).is_ok());
        let ragged = len_field(6, &[1, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert!(validate_request(&wrap_histogram(&ragged), root).is_err());
        let ragged = len_field(7, &[0; 12]);
        assert!(validate_request(&wrap_histogram(&ragged), root).is_err());

        let wrap_buckets = |buckets: &[u8]| {
            let point = len_field(8, buckets);
            let histogram = len_field(1, &point);
            let metric = len_field(10, &histogram);
            len_field(1, &len_field(2, &len_field(2, &metric)))
        };
        assert!(validate_request(&wrap_buckets(&len_field(2, &[0x01, 0xac, 0x02])), root).is_ok());
        assert!(validate_request(&wrap_buckets(&[0x10, 0x01, 0x10, 0x02]), root).is_ok());
        assert!(validate_request(&wrap_buckets(&len_field(2, &[0x01, 0xac])), root).is_err());
    }

    /// A log body nesting `levels` arrays, the innermost holding a string.
    fn nested_body(levels: usize) -> Vec<u8> {
        let mut value = len_field(1, b"leaf");
        for level in 0..levels {
            // Alternate arrays and key-value lists: both count alike.
            let container = if level % 2 == 0 {
                len_field(5, &len_field(1, &value))
            } else {
                let key_value = [len_field(1, b"k"), len_field(2, &value)].concat();
                len_field(6, &len_field(1, &key_value))
            };
            value = container;
        }
        let record = len_field(5, &value);
        len_field(1, &len_field(2, &len_field(2, &record)))
    }

    /// Scenario: log bodies nesting arrays and key-value lists exactly at
    /// and one level beyond `MAX_ANY_VALUE_NESTING_DEPTH`.
    /// Guarantees: the limit is exact -- the deepest accepted nesting is the
    /// limit itself -- the walk at the limit completes on a test thread's
    /// stack, and one level more is refused as `OtlpNestingTooDeep`.
    #[test]
    fn nesting_is_bounded_exactly() {
        let root = Message::ExportLogsServiceRequest;
        assert!(validate_request(&nested_body(MAX_ANY_VALUE_NESTING_DEPTH), root).is_ok());
        assert!(matches!(
            validate_request(&nested_body(MAX_ANY_VALUE_NESTING_DEPTH + 1), root),
            Err(Error::OtlpNestingTooDeep {
                limit: MAX_ANY_VALUE_NESTING_DEPTH,
                ..
            })
        ));
    }

    /// A logs request whose only log record is `record`.
    fn in_log_record(record: &[u8]) -> Vec<u8> {
        len_field(1, &len_field(2, &len_field(2, record)))
    }

    /// The problem an `InvalidOtlpWireFormat` names, or a panic.
    fn problem(result: Result<(), Error>) -> &'static str {
        match result {
            Err(Error::InvalidOtlpWireFormat { problem, .. }) => problem,
            other => panic!("expected a framing error, got {other:?}"),
        }
    }

    /// Scenario: a log record's `severity_number` (a varint) holding
    /// `u64::MAX` in ten bytes, then the ten-byte varint
    /// `80 80 80 80 80 80 80 80 80 02` and an eleven-byte varint in the same
    /// field, one as a nested length prefix, and one inside packed
    /// exponential-histogram bucket counts.
    /// Guarantees: the maximum `u64` passes, and a varint carrying bits past
    /// the 64th is refused wherever it appears -- key, length, scalar or
    /// packed element -- instead of being read as a wrapped value, as prost
    /// refuses it.
    #[test]
    fn a_varint_that_overflows_u64_is_refused() {
        let root = Message::ExportLogsServiceRequest;
        let max = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        let overflow = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        let mut eleven = [0x80; 11];
        eleven[10] = 0x00;
        let severity = |varint: &[u8]| in_log_record(&[&[0x10][..], varint].concat());
        assert!(validate_request(&severity(&max), root).is_ok());
        assert_eq!(
            problem(validate_request(&severity(&overflow), root)),
            "truncated or overlong varint"
        );
        assert_eq!(
            problem(validate_request(&severity(&eleven), root)),
            "truncated or overlong varint"
        );
        // LogRecord.body (field 5, LEN) whose length is the overflowing varint.
        let length = in_log_record(&[&[0x2a][..], &overflow].concat());
        assert_eq!(
            problem(validate_request(&length, root)),
            "truncated or overlong length prefix"
        );
        // An unknown field key that overflows.
        let key = in_log_record(&overflow);
        assert_eq!(
            problem(validate_request(&key, root)),
            "truncated or overlong field key"
        );
        let buckets = |counts: &[u8]| {
            let point = len_field(8, &len_field(2, counts));
            let metric = len_field(10, &len_field(1, &point));
            len_field(1, &len_field(2, &len_field(2, &metric)))
        };
        let metrics = Message::ExportMetricsServiceRequest;
        assert!(validate_request(&buckets(&max), metrics).is_ok());
        assert_eq!(
            problem(validate_request(&buckets(&overflow), metrics)),
            "truncated or overlong varint in a packed field"
        );
    }

    /// Scenario: `AnyValue.string_value` holding the byte `0xff`, the same
    /// value as a valid multibyte UTF-8 string, `AnyValue.bytes_value`
    /// holding `0xff`, and a `KeyValue.key` and a `Metric.name` holding
    /// `0xff`.
    /// Guarantees: a `string` field must hold valid UTF-8, as prost requires,
    /// so the conversion never replaces damaged text with U+FFFD and stores it
    /// as if it had been sent; multibyte text and arbitrary `bytes` pass.
    #[test]
    fn a_string_field_must_hold_valid_utf8() {
        let root = Message::ExportLogsServiceRequest;
        let attribute = |key: &[u8], value: &[u8]| {
            let key_value = [len_field(1, key), len_field(2, value)].concat();
            in_log_record(&len_field(6, &key_value))
        };
        assert_eq!(
            problem(validate_request(
                &attribute(b"k", &len_field(1, &[0xff])),
                root
            )),
            "invalid UTF-8 in a string field"
        );
        let text = "h\u{e9}llo \u{2713} \u{1f600}";
        assert!(validate_request(&attribute(b"k", &len_field(1, text.as_bytes())), root).is_ok());
        assert!(validate_request(&attribute(b"k", &len_field(7, &[0xff])), root).is_ok());
        assert_eq!(
            problem(validate_request(
                &attribute(&[0xff], &len_field(1, b"v")),
                root
            )),
            "invalid UTF-8 in a string field"
        );
        let metric = len_field(1, &[0xc3]);
        let body = len_field(1, &len_field(2, &len_field(2, &metric)));
        assert_eq!(
            problem(validate_request(
                &body,
                Message::ExportMetricsServiceRequest
            )),
            "invalid UTF-8 in a string field"
        );
    }

    /// Scenario: unknown field 31 of a log record encoded as a group --
    /// empty (`fb 01 fc 01`), holding fields of every other wire type, and
    /// holding a nested group of field 32 -- then a stray end group, an end
    /// group of the wrong field, a group never closed, a group on a known
    /// field, and groups nested exactly at and one beyond the nesting limit.
    /// Guarantees: a balanced unknown group is skipped as prost skips it, so a
    /// sender's proto2 extension never refuses a request; every unbalanced or
    /// misplaced group is refused, and group nesting is bounded by the same
    /// limit as `AnyValue` nesting.
    #[test]
    fn unknown_groups_are_skipped_when_balanced() {
        let root = Message::ExportLogsServiceRequest;
        let start31 = [0xfb, 0x01];
        let end31 = [0xfc, 0x01];
        let start32 = [0x83, 0x02];
        let end32 = [0x84, 0x02];
        let record = |parts: &[&[u8]]| in_log_record(&parts.concat());
        assert!(validate_request(&record(&[&start31, &end31]), root).is_ok());
        let fields: &[u8] = &[
            0x08, 0x05, 0x11, 1, 2, 3, 4, 5, 6, 7, 8, 0x1a, 0x01, 0xff, 0x25, 1, 2, 3, 4,
        ];
        assert!(validate_request(&record(&[&start31, fields, &end31]), root).is_ok());
        assert!(
            validate_request(&record(&[&start31, &start32, fields, &end32, &end31]), root).is_ok()
        );

        assert_eq!(
            problem(validate_request(&record(&[&end31]), root)),
            "end group without a start group"
        );
        assert_eq!(
            problem(validate_request(&record(&[&start31, &end32]), root)),
            "end group does not match its start group"
        );
        assert_eq!(
            problem(validate_request(&record(&[&start31, fields]), root)),
            "group without an end group"
        );
        // Field 1 of a log record (time_unix_nano) as a group.
        assert_eq!(
            problem(validate_request(&record(&[&[0x0b, 0x0c]]), root)),
            "wrong wire type for a known field"
        );

        let nested = |levels: usize| {
            let mut parts = vec![&start31[..]; levels];
            parts.extend(vec![&end31[..]; levels]);
            record(&parts)
        };
        assert!(validate_request(&nested(MAX_ANY_VALUE_NESTING_DEPTH), root).is_ok());
        assert!(matches!(
            validate_request(&nested(MAX_ANY_VALUE_NESTING_DEPTH + 1), root),
            Err(Error::OtlpNestingTooDeep { .. })
        ));
    }
}
