// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Admission and parking: validation, extraction, refusals and the one
//! parked request.

use super::support::*;

/// Scenario: a traces request reaches an exporter that has no traces schema,
/// and a logs request larger than `ingress.max_request_bytes` arrives.
/// Guarantees: both are refused as permanent client errors with the rule that
/// rejected them, and neither leaves anything in the ACTIVE block, because
/// validation judges the request's own content and the identical bytes would
/// be refused again.
#[tokio::test(flavor = "current_thread")]
async fn validation_refusals_are_permanent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            let mut context = Context::default();
            context.set_source_node(7);
            worker.admit(OtapPdata::new(context, traces_payload()));
            // The same logs request that is admitted elsewhere, now larger
            // than the budget it is measured against.
            worker.cfg.lake.ingress.max_request_bytes = 1;
            worker.admit(logs_pdata());

            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(!worker.rotation_requested);
            // The reason is the sentence the producer sees: it names the
            // rule, the limit and the remedy, not a bare metric token.
            for reason in [
                "traces are not stored by series_parquet",
                "exceeds ingress.max_request_bytes (1 bytes); split the batch upstream",
            ] {
                assert!(worker.notify.next().await.is_ok());
                match rx.recv().await.expect("a refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(nack.permanent);
                        assert_eq!(nack.cause, NackCause::Refused);
                        assert!(nack.reason.contains(reason), "reason: {}", nack.reason);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
        })
        .await;
}

/// Scenario: a well-formed logs request is admitted and rotated, but the
/// object store cannot be written, because the directory the store was rooted
/// at has been replaced by a regular file.
/// Guarantees: every request of the block is nacked as retryable rather than
/// refused, and the descriptor is left uncommitted so the next block writes it
/// again. An unreachable or full destination must not tell the sender to
/// change a request that is perfectly valid. Replacing the root with a file is
/// used rather than dropping its write permission because no user, including
/// root, can create a path below a regular file, so the failure is
/// deterministic everywhere the tests run.
#[tokio::test(flavor = "current_thread")]
async fn a_storage_failure_after_validation_is_retryable() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().expect("temp dir");
            let root = dir.path().join("lake");
            std::fs::create_dir(&root).expect("create the lake root");
            let store = Arc::new(
                object_store::local::LocalFileSystem::new_with_prefix(&root)
                    .expect("local object store"),
            );
            // The store has already resolved its root, so swapping the
            // directory for a file breaks every write underneath it.
            std::fs::remove_dir_all(&root).expect("remove the lake root");
            std::fs::write(&root, b"not a directory").expect("put a file in its place");

            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            // A broken destination is retried until the block's absolute
            // deadline, so the test gives it one it can reach on a simulated
            // clock rather than waiting out the configured default.
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(1);
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            let partition = worker.active.data.partition;
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(50));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            assert!(
                done.as_ref().expect("the flush task joins").result.is_err(),
                "the destination is not writable"
            );

            worker.complete(done);
            assert!(!worker.cache.is_committed(&id, partition));
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a storage failure") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert!(nack.reason.contains("retry"), "reason: {}", nack.reason);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: both failure classes are turned into the outcome the notifier
/// delivers.
/// Guarantees: a size refusal keeps the budget it exceeded as its outcome,
/// excess nesting is its own outcome rather than invalid content, an
/// unsupported signal keeps its own, any other validation refusal is
/// reported as invalid, and every
/// retryable failure becomes a storage outcome, so the phase a failure came
/// from still decides what the sender is told.
#[test]
fn each_failure_class_maps_to_its_outcome() {
    assert_eq!(
        Outcome::of(&lake::Error::too_large(lake::SizeBudget::Row, 2, 1)),
        Outcome::RowTooLarge
    );
    for (budget, outcome) in [
        (lake::SizeBudget::Request, Outcome::RequestTooLarge),
        (lake::SizeBudget::Extracted, Outcome::ExtractedTooLarge),
        (lake::SizeBudget::Table, Outcome::ExtractedTooLarge),
        (lake::SizeBudget::Cell, Outcome::RowTooLarge),
        (lake::SizeBudget::Block, Outcome::BlockTooLarge),
    ] {
        assert_eq!(
            Outcome::of(&lake::Error::too_large(budget, 2, 1)),
            outcome,
            "{budget:?}"
        );
    }
    assert_eq!(
        Outcome::of(&lake::Error::Refused(lake::RefuseReason::TooDeep(32))),
        Outcome::TooDeep
    );
    assert_eq!(
        Outcome::of(&lake::Error::Refused(lake::RefuseReason::Unsupported(
            "signal".into()
        ))),
        Outcome::Unsupported
    );
    assert_eq!(
        Outcome::of(&lake::Error::invalid("undecodable pdata")),
        Outcome::Invalid
    );
    assert_eq!(
        Outcome::of(&lake::Error::from(object_store::Error::Generic {
            store: "test",
            source: "unreachable".into(),
        })),
        Outcome::Storage
    );
    assert_eq!(
        Outcome::of(&lake::Error::internal("flush failed")),
        Outcome::Internal
    );
}

/// Scenario: extraction fails on a writer invariant -- the column/builder
/// mismatch a writer bug produces -- rather than on the request's content,
/// and the failure is classified by the rule the admission path uses and then
/// delivered.
/// Guarantees: only a lake refusal is permanent. The internal error becomes a
/// retryable nack labelled `internal`, and its reason is a sentence carrying
/// the sanitized detail, so a producer never drops data because of an
/// exporter bug and an operator can still see what went wrong.
#[tokio::test(flavor = "current_thread")]
async fn an_internal_extraction_error_is_a_retryable_nack_with_detail() {
    let failure = lake::Error::internal("column/builder mismatch for Str(None)\nsecond line");
    assert!(matches!(failure, lake::Error::Internal(_)));
    assert_eq!(Outcome::of(&failure), Outcome::Internal);
    assert!(!Outcome::of(&failure).refused());
    let sentence = Outcome::explain(&failure);
    assert!(
        sentence.contains("column/builder mismatch for Str(None) second line"),
        "the detail is kept, on one line: {sentence}"
    );
    assert!(Outcome::of(&lake::Error::invalid("bad")).refused());

    let (handler, mut rx) = effects(1);
    let mut notify = Notifier::new(handler, 4);
    let (token, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push_with(token, Outcome::of(&failure), Some(sentence.clone().into()));
    notify.next().await.expect("the completion is accepted");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_ne!(nack.cause, NackCause::Refused);
            assert_eq!(nack.reason, sentence);
        }
        other => panic!("expected a nack, got {other:?}"),
    }
    assert_eq!(notify.outcomes()[Outcome::Internal as usize], 1);
}

/// Scenario: a writer invariant really breaks inside the lake while a request
/// is admitted: the values sort key is changed after validation to a column
/// the dataset does not have, and the run target is one byte, so the lake's
/// run sort fails with its own internal error on the first request. (No
/// request content can make extraction itself break an invariant: its
/// builders and rows are derived from the same configuration, and OTAP schema
/// validation refuses mistyped columns before extraction runs.)
/// Guarantees: the request is nacked as retryable, not refused, with the
/// `internal` label and a reason carrying the lake's detail, so a bug of the
/// writer never tells a producer to drop its data.
#[tokio::test(flavor = "current_thread")]
async fn a_real_writer_invariant_failure_is_a_retryable_nack_with_detail() {
    let (handler, mut rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut cfg = worker_config();
    cfg.lake.sorting.run_target_bytes = 1;
    cfg.lake.logs.values_sort = vec![lake::config::SortKey {
        column: "no_such_column".into(),
        order: lake::config::SortOrder::Asc,
        nulls: lake::config::Nulls::Last,
    }];
    let mut worker = Worker::new(
        cfg,
        Arc::new(object_store::memory::InMemory::new()),
        wall,
        handler,
    );
    worker.admit(logs_pdata());
    assert!(
        worker.active.tokens.is_empty(),
        "the request is not admitted"
    );
    assert_eq!(worker.notify.outcomes()[Outcome::Internal as usize], 1);
    worker
        .notify
        .next()
        .await
        .expect("the completion is accepted");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => {
            assert!(!nack.permanent);
            assert_ne!(nack.cause, NackCause::Refused);
            assert!(
                nack.reason.contains("sort column no_such_column missing")
                    && nack.reason.contains("retry"),
                "reason: {}",
                nack.reason
            );
        }
        other => panic!("expected a nack, got {other:?}"),
    }
}

/// Scenario: an error detail longer than the reason bound, and one holding
/// control characters and multi-byte characters at the cut.
/// Guarantees: the detail a nack reason carries is bounded, single-line and
/// cut on a character boundary, so request-derived text cannot make a status
/// message unbounded or split a character.
#[test]
fn a_reason_detail_is_bounded_and_single_line() {
    let long = "x".repeat(10_000);
    let cut = super::super::outcome::sanitized(&long);
    assert!(cut.len() <= 256 + 3);
    assert!(cut.ends_with("..."));
    let wide = "\u{e9}".repeat(1_000);
    let cut = super::super::outcome::sanitized(&wide);
    assert!(cut.len() <= 256 + 3);
    assert_eq!(super::super::outcome::sanitized("a\r\nb\tc"), "a  b c");
}

/// Scenario: the bounded engine completion channel fills while a second
/// notification waits.
/// Guarantees: cancelling a poll preserves the second context and sends it
/// exactly once later, so a request never loses its decision because the
/// exporter had to attend to something else.
#[tokio::test(flavor = "current_thread")]
async fn notification_survives_cancelled_poll() {
    let (handler, mut rx) = effects(1);
    let mut notify = Notifier::new(handler, 2);

    // The first completion is handed over before the second is queued, so the
    // notifier never holds more normal completions than its cap allows.
    let (first, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push(first, Outcome::Ack);
    assert!(notify.next().await.is_ok());

    // The completion channel holds one message and nothing has read it, so
    // this second send cannot make progress.
    let (second, payload) = AckToken::split(empty_pdata());
    drop(payload);
    notify.push(second, Outcome::Ack);
    assert!(
        tokio::time::timeout(Duration::from_millis(5), notify.next())
            .await
            .is_err()
    );
    assert_eq!(notify.len(), 1);

    assert!(matches!(
        rx.recv().await.expect("first completion"),
        PipelineCompletionMsg::DeliverAck { .. }
    ));
    assert!(notify.next().await.is_ok());
    assert!(matches!(
        rx.recv().await.expect("second completion"),
        PipelineCompletionMsg::DeliverAck { .. }
    ));
    assert_eq!(notify.len(), 0);
}

/// Scenario: two requests fill a two-request block and a third request needs
/// the next one.
/// Guarantees: exactly one extracted request is parked, admission closes while
/// it waits, and it enters the next block before anything newer, so a request
/// that could not be reserved is neither dropped nor reordered behind later
/// input and the worker still holds no more than two blocks and one request.
#[tokio::test(flavor = "current_thread")]
async fn one_pending_request_resumes_before_new_input() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config_with_requests(2), store, wall, handler);

            worker.admit(logs_pdata());
            worker.admit(logs_pdata());
            worker.admit(logs_pdata());

            assert_eq!(worker.active.tokens.len(), 2);
            assert!(worker.pending.is_some());
            assert!(!worker.accept());

            worker.rotate();
            worker.resume_pending();
            assert!(worker.pending.is_none());
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(worker.live_tokens(), 3);
            assert!(worker.active.data.bytes <= worker.cfg.window.max_block_bytes);
        })
        .await;
}

/// Scenario: a request whose logical size exceeds the input budget is offered
/// to an empty ACTIVE block.
/// Guarantees: nothing is admitted, no request is parked and the sender gets a
/// permanent refusal, because a request that cannot fit an empty block would
/// be refused by every following block as well.
#[tokio::test(flavor = "current_thread")]
async fn oversized_input_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.ingress.max_request_bytes = 1;
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert_eq!(worker.active.data.bytes, 0);
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                }
                other => panic!("expected a refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: preparation consumes Arrow input whose original array is still
/// weakly observed from outside the worker.
/// Guarantees: the parked extraction retains no original input array and no
/// conversion record batch, so parking one request cannot keep a whole
/// request's Arrow buffers resident beside the two blocks.
#[tokio::test(flavor = "current_thread")]
async fn prepare_releases_original_arrow_payload() {
    use otel_arrow_dfe_pdata::TryIntoWithOptions;
    use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    let (context, payload) = logs_pdata().into_parts();
    let records: OtapArrowRecords = payload.try_into_with_default().expect("records");
    let weak = Arc::downgrade(
        records
            .get(ArrowPayloadType::Logs)
            .expect("logs batch")
            .column(0),
    );
    let store = Arc::new(object_store::memory::InMemory::new());
    let (handler, _rx) = effects(4);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(worker_config(), store, wall, handler);

    let prepared = worker.prepare(OtapPdata::new(context, records.into()));
    assert!(matches!(prepared, Prepared::Ready(_)));
    assert!(
        weak.upgrade().is_none(),
        "the prepared output cannot pin the input arrays"
    );
}

/// Scenario: a well-formed request is converted and then fails the extraction
/// budget, which is measured only after the conversion has run.
/// Guarantees: the failure is a permanent refusal, nothing is parked and the
/// ACTIVE block is left untouched, because every validation phase completes
/// before the block is reserved against.
#[tokio::test(flavor = "current_thread")]
async fn extraction_failure_is_refused_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            // Large enough to pass the wire-size check, so the refusal can
            // only come from the measured extracted output.
            cfg.lake.ingress.max_extracted_bytes = 1;
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(worker.pending.is_none());

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                }
                other => panic!("expected an extraction refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: OTLP bodies the framing check refuses reach preparation after
/// their byte-size check: a logs and a metrics body whose first field declares
/// 127 missing bytes, a logs and a metrics body whose one nested `Resource*`
/// message is `0a 01 0a`, a `ResourceLogs` carrying `resource` twice, a
/// metric carrying both a gauge and a sum, and a log body nesting arrays one
/// level beyond the walk's bound of 256.
/// Guarantees: each is nacked permanently as `Refused` with a reason naming
/// what refused it, and the ACTIVE block is untouched: repeated singular
/// fields are refused here although the other exporters accept them, and a
/// body too deep for the walk is refused as `ingress.max_nesting_depth`
/// refuses it.
#[tokio::test(flavor = "current_thread")]
async fn a_body_the_framing_check_refuses_is_nacked_atomically() {
    use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, ArrayValue, any_value};
    use otel_arrow_dfe_pdata::views::otlp::bytes::validate::MAX_ANY_VALUE_NESTING_DEPTH;

    let len_field = |field: u32, payload: &[u8]| {
        let mut out = Vec::new();
        prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    };
    let logs = |body: Vec<u8>| otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(body.into());
    let metrics =
        |body: Vec<u8>| otel_arrow_dfe_pdata::OtlpProtoBytes::ExportMetricsRequest(body.into());

    let attribute = len_field(
        1,
        &[len_field(1, b"k"), len_field(2, &len_field(1, b"v"))].concat(),
    );
    let mut record = vec![0x09];
    record.extend(1_789_960_500_000_000_000_u64.to_le_bytes());
    let two_resources = [
        len_field(1, &attribute),
        len_field(1, &attribute),
        len_field(2, &len_field(2, &record)),
    ]
    .concat();
    let mut point = vec![0x19];
    point.extend(1_789_960_500_000_000_000_u64.to_le_bytes());
    point.extend([0x31, 1, 0, 0, 0, 0, 0, 0, 0]);
    let data = len_field(1, &point);
    let gauge_and_sum = [
        len_field(1, b"requests"),
        len_field(5, &data),
        len_field(7, &data),
    ]
    .concat();
    let mut value = AnyValue {
        value: Some(any_value::Value::StringValue("leaf".to_owned())),
    };
    for _ in 0..=MAX_ANY_VALUE_NESTING_DEPTH {
        value = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![value],
            })),
        };
    }
    let too_deep = encoded(&ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_789_960_500_000_000_000,
                    body: Some(value),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    });
    let malformed = "invalid request content: malformed OTLP";
    let cases = [
        (logs(vec![0x0A, 0x7F]), vec![malformed, "logs body"]),
        (metrics(vec![0x0A, 0x7F]), vec![malformed, "metrics body"]),
        (
            logs(vec![0x0A, 0x01, 0x0A]),
            vec![malformed, "ResourceLogs"],
        ),
        (
            metrics(vec![0x0A, 0x01, 0x0A]),
            vec![malformed, "ResourceMetrics"],
        ),
        (
            logs(len_field(1, &two_resources)),
            vec![malformed, "ResourceLogs.resource", "occurs more than once"],
        ),
        (
            metrics(len_field(1, &len_field(2, &len_field(2, &gauge_and_sum)))),
            vec![malformed, "Metric.data", "occurs more than once"],
        ),
        (logs(too_deep), vec!["exceeds ingress.max_nesting_depth"]),
    ];

    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(cases.len());
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);
            for (payload, named) in cases {
                let mut context = Context::default();
                context.set_source_node(7);
                worker.admit(OtapPdata::new(context, payload.into()));
                assert!(worker.active.data.is_empty(), "{named:?}");
                assert!(worker.active.tokens.is_empty(), "{named:?}");
                assert!(worker.pending.is_none(), "{named:?}");
                assert!(worker.notify.next().await.is_ok());
                match rx.recv().await.expect("a refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(nack.permanent, "{named:?}");
                        assert_eq!(nack.cause, NackCause::Refused, "{named:?}");
                        for part in named {
                            assert!(nack.reason.contains(part), "{part}: {}", nack.reason);
                        }
                    }
                    other => panic!("{named:?}: expected a refusal, got {other:?}"),
                }
            }
        })
        .await;
}

/// Scenario: a well-formed OTLP metrics request reaches the same worker that
/// admits logs.
/// Guarantees: it is admitted through one extraction into the ACTIVE block and
/// holds its completion there, so metrics travel the single admission state
/// machine rather than a signal-specific path.
#[tokio::test(flavor = "current_thread")]
async fn metrics_are_admitted_like_logs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            let mut context = Context::default();
            context.set_source_node(7);
            worker.admit(OtapPdata::new(context, metrics_payload()));

            assert!(!worker.active.data.is_empty());
            assert_eq!(worker.active.tokens.len(), 1);
            assert!(worker.pending.is_none());
            assert_eq!(worker.notify.len(), 0);
        })
        .await;
}

/// Scenario: an OTLP logs request whose record body is a string holding bytes
/// that are not UTF-8 (`caf` then a lone `0xc3`).
/// Guarantees: it is prepared for admission, not refused, and its body is
/// extracted with U+FFFD in place of the invalid byte, as the OTAP conversion
/// stores it.
#[tokio::test(flavor = "current_thread")]
async fn invalid_utf8_in_a_log_body_is_stored_replaced() {
    let len_field = |field: u32, payload: &[u8]| {
        let mut out = Vec::new();
        prost::encoding::encode_key(field, prost::encoding::WireType::LengthDelimited, &mut out);
        prost::encoding::encode_varint(payload.len() as u64, &mut out);
        out.extend_from_slice(payload);
        out
    };
    let mut record = vec![0x09];
    record.extend(1_789_960_500_000_000_000_u64.to_le_bytes());
    record.extend(len_field(5, &len_field(1, b"caf\xc3")));
    let body = len_field(1, &len_field(2, &len_field(2, &record)));
    let payload = otel_arrow_dfe_pdata::OtlpProtoBytes::ExportLogsRequest(body.into());

    let store = Arc::new(object_store::memory::InMemory::new());
    let (handler, _rx) = effects(2);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let worker = Worker::new(worker_config(), store, wall, handler);
    let mut context = Context::default();
    context.set_source_node(7);
    match worker.prepare(OtapPdata::new(context, payload.into())) {
        Prepared::Ready(pending) => {
            assert_eq!(pending.extracted.stats.rows, 1);
            let values = format!("{:?}", pending.extracted.values);
            assert!(values.contains("caf\u{FFFD}"), "{values}");
        }
        Prepared::Failed(_, failure) => panic!("refused: {failure:?}"),
    }
}

/// Scenario: one metrics request carries a supported gauge point next to an
/// unsupported summary point, under the default `unsupported: reject`.
/// Guarantees: the whole request is refused as one permanent `unsupported`
/// nack and the ACTIVE block keeps the bytes and the request count it had, so
/// the policy is applied atomically and the supported half of a rejected
/// request is never stored. Nothing is parked, because the refusal judges the
/// request's own content rather than whichever block happened to be active.
#[tokio::test(flavor = "current_thread")]
async fn a_mixed_metrics_request_is_rejected_atomically() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let cfg = worker_config();
            assert_eq!(
                cfg.lake.unsupported,
                lake::config::UnsupportedPolicy::Reject
            );
            let mut worker = Worker::new(cfg, store, wall, handler);

            // A request admitted first, so the assertion is that the rejected
            // request left a non-empty block exactly as it found it rather
            // than that an empty block stayed empty.
            worker.admit(logs_pdata());
            let bytes = worker.active.data.bytes;
            let requests = worker.active.tokens.len();
            assert_eq!(requests, 1);

            worker.admit(mixed_metrics_pdata());
            assert_eq!(worker.active.data.bytes, bytes);
            assert_eq!(worker.active.tokens.len(), requests);
            assert!(worker.pending.is_none());
            assert_eq!(worker.live_tokens(), requests + 1);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(
                        nack.reason.contains("set unsupported: drop"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected an unsupported refusal, got {other:?}"),
            }
            // Exactly one completion, and it belongs to the rejected request:
            // the admitted one is still owed by the ACTIVE block.
            assert_eq!(worker.live_tokens(), requests);
        })
        .await;
}

/// Scenario: the same mixed metrics request arrives under
/// `unsupported: drop`, and the block it lands in is rotated and written.
/// Guarantees: the gauge point is admitted, the summary point is counted as
/// dropped rather than stored, and the request is acknowledged only once its
/// block has been written, so a dropped point does not make the request ack
/// early or fail.
#[tokio::test(flavor = "current_thread")]
async fn a_mixed_metrics_request_drops_only_the_unsupported_points() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.unsupported = lake::config::UnsupportedPolicy::Drop;
            let mut worker = Worker::new(cfg, store, wall, handler);

            // Prepared rather than admitted in one step, because the drop
            // counters live in the extraction and are consumed by admission.
            let Prepared::Ready(pending) = worker.prepare(mixed_metrics_pdata()) else {
                panic!("the drop policy admits the supported points");
            };
            assert_eq!(pending.extracted.stats.dropped_unsupported, 1);
            assert_eq!(pending.extracted.stats.rows, 1);
            worker.offer(pending);
            assert_eq!(worker.active.tokens.len(), 1);
            assert!(!worker.active.data.is_empty());
            assert!(worker.pending.is_none());

            worker.rotate();
            assert!(
                worker.notify.is_empty(),
                "a dropped point decides nothing before the flush resolves"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(5), rx.recv())
                    .await
                    .is_err(),
                "a dropped point does not acknowledge the request early"
            );
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert!(worker.notify.next().await.is_ok());
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
        })
        .await;
}

/// Scenario: the wall clock steps backwards between parking a request for a
/// later window and opening the block that request is waiting for.
/// Guarantees: one rotation admits the parked request, which is not parked
/// again. The block a parked request is opened for takes its window from that
/// request, so a clock that steps back cannot make the node spin opening empty
/// blocks the parked request is forever too late for.
#[tokio::test(flavor = "current_thread")]
async fn a_backward_clock_step_does_not_repark_the_pending_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, _rx) = effects(8);
            // One-second windows, so the block opened at 100 s ends well
            // before the request prepared at 200 s.
            let wall = Arc::new(lake::clock::TestWallClock::new(100 * 1_000_000_000));
            let mut worker = Worker::new(worker_config(), store, Arc::clone(&wall) as _, handler);
            assert_eq!(worker.active.data.window_start_secs, 100);

            wall.set(200 * 1_000_000_000);
            worker.admit(logs_pdata());
            assert!(
                worker.pending.is_some(),
                "a request past the block's window waits for the next block"
            );
            assert!(worker.active.tokens.is_empty());

            // The clock now steps back behind both the parked request and the
            // block that is about to be replaced.
            wall.set(50 * 1_000_000_000);
            worker.rotate();
            worker.resume_pending();

            assert!(worker.pending.is_none(), "one rotation is enough");
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(worker.active.data.window_start_secs, 200);
        })
        .await;
}

/// Scenario: a request whose reservation alone exceeds the block budget is
/// offered to an empty ACTIVE block.
/// Guarantees: `Block::reserve` refuses it as `RequestTooLarge`, the worker
/// reports that permanently rather than parking it, and the block is
/// untouched. An empty block is the largest one the request will ever be
/// offered, so parking it would rotate for ever without admitting it.
#[tokio::test(flavor = "current_thread")]
async fn a_request_too_large_for_an_empty_block_is_refused_not_parked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            // The per-request and extraction budgets stay wide open, so the
            // refusal can only come from the block budget inside `reserve`.
            cfg.window.max_block_bytes = 1;
            cfg.lake.ingress.max_block_bytes = 1;
            let worker = Worker::new(
                cfg.clone(),
                store.clone(),
                Arc::clone(&wall) as _,
                effects(1).0,
            );
            assert!(
                matches!(worker.prepare(logs_pdata()), Prepared::Ready(_)),
                "preparation must succeed, or the refusal would not be the block budget"
            );
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            assert!(worker.active.data.is_empty());
            assert!(worker.active.tokens.is_empty());
            assert!(
                worker.pending.is_none(),
                "an empty block cannot take it, and no later block will either"
            );

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(
                        nack.reason.contains("window.max_block_bytes (1 bytes)"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected a block-budget refusal, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: three requests reach the running node; the block takes two of
/// them and the third has to wait for the next one. Both clocks are
/// simulated, so the window boundary that seals the second block is reached
/// by advancing them rather than by waiting.
/// Guarantees: the parked request reaches storage before the newer one, and
/// the newer one is not taken off the channel while a request is parked, so
/// backpressure is real rather than a third block. Each request carries its
/// own source node, so the completions say which request was decided first.
#[tokio::test(flavor = "current_thread")]
async fn the_parked_request_is_stored_before_a_newer_one() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(8);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));

            // A block reserves room for two requests but is only rotated by
            // the window or a third one, so the third request is parked
            // rather than left on the channel.
            let mut cfg = worker_config();
            cfg.window.max_requests_per_block = 3;
            cfg.lake.ingress.max_requests_per_block = 2;
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                inbox,
                handler,
                None,
            ));

            // The first two fill the room the block reserves; the third
            // cannot be reserved against it and waits for the next block.
            for source in 1..=3 {
                pdata_tx
                    .send_async(logs_pdata_from(source))
                    .await
                    .expect("a request enqueues");
            }

            // Parking the third request asks for a rotation, so the first
            // block is written without any boundary being reached and the
            // parked request enters the block that replaces it.
            for source in 1..=2 {
                assert_eq!(expect_ack(&mut rx).await, Some(source));
            }

            // Those acks are delivered after the rotation that admitted the
            // third request, so the block it sits in already exists. Its
            // window is reached by moving both clocks explicitly.
            wall.set(1_100_000_000);
            sim.advance(Duration::from_secs(1));
            assert_eq!(expect_ack(&mut rx).await, Some(3));

            drop(pdata_tx);
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins");
            if let Err(error) = terminal {
                panic!("unexpected node failure: {error}");
            }
            drop(control_tx);
        })
        .await;
}

/// Scenario: one request is refused by the extraction budget and, on a second
/// worker, one by the block budget.
/// Guarantees: each refusal is logged at WARN as
/// `series_parquet.request.failed`, with the setting that refused it, the
/// size observed against it and the limit as numbers, and the reason sentence
/// the producer is told states the same size and limit, at both stages, so an
/// operator can see how far over which budget a producer is without
/// reproducing the request.
#[tokio::test(flavor = "current_thread")]
async fn a_size_refusal_reports_the_observed_size_and_the_limit() {
    let events = capture();
    /// The one refusal WARN logged since `before` events were recorded.
    fn refusal(events: &Capture, before: usize) -> CapturedEvent {
        let logged = events.named("series_parquet.request.failed");
        assert_eq!(logged.len(), before + 1, "{logged:?}");
        let event = logged[before].clone();
        assert_eq!(event.level, tracing::Level::WARN);
        event
    }
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let store = Arc::new(object_store::memory::InMemory::new());

    let (handler, mut rx) = effects(4);
    let mut extract = Worker::new(
        worker_config(),
        store.clone(),
        Arc::clone(&wall) as _,
        handler,
    );
    extract.cfg.lake.ingress.max_extracted_bytes = 100;
    extract.admit(logs_pdata());
    let event = refusal(&events, 0);
    let field = |name: &str| event.fields.get(name).cloned();
    assert_eq!(
        field("outcome"),
        Some(FieldValue::Str("extracted_too_large".into()))
    );
    assert_eq!(field("signal"), Some(FieldValue::Debug("Logs".into())));
    assert_eq!(
        field("limit_setting"),
        Some(FieldValue::Str("ingress.max_extracted_bytes".into()))
    );
    assert_eq!(field("limit_bytes"), Some(FieldValue::U64(100)));
    let Some(FieldValue::U64(observed)) = field("observed_bytes") else {
        panic!("observed_bytes is a number: {event:?}");
    };
    assert!(observed > 100, "{observed}");
    extract
        .notify
        .next()
        .await
        .expect("the refusal is delivered");
    match rx.recv().await.expect("a nack") {
        PipelineCompletionMsg::DeliverNack { nack } => assert!(
            nack.reason.contains(&format!(
                "extracted request of {observed} bytes exceeds ingress.max_extracted_bytes \
                 (100 bytes)"
            )),
            "reason: {}",
            nack.reason
        ),
        other => panic!("expected a nack, got {other:?}"),
    }

    let (handler, _rx) = effects(4);
    let mut cfg = worker_config();
    cfg.window.max_block_bytes = 1;
    cfg.lake.ingress.max_block_bytes = 1;
    let mut block = Worker::new(cfg, store, wall, handler);
    block.admit(logs_pdata());
    let event = refusal(&events, 1);
    let field = |name: &str| event.fields.get(name).cloned();
    assert_eq!(
        field("limit_setting"),
        Some(FieldValue::Str("window.max_block_bytes".into()))
    );
    assert_eq!(field("limit_bytes"), Some(FieldValue::U64(1)));
    assert!(
        matches!(field("observed_bytes"), Some(FieldValue::U64(n)) if n > 1),
        "{event:?}"
    );
    drop(events);
}

/// Scenario: refusals arrive in a burst, then after a pause of the log
/// interval.
/// Guarantees: one WARN line is written per interval however many requests
/// are refused within it, and the next line reports how many were left out,
/// so a producer resending a refused request cannot flood the log while the
/// count of refusals is still visible.
#[test]
fn refusal_warnings_are_rate_limited() {
    let mut log = super::super::worker::RefusalLog::default();
    let start = std::time::Instant::now();
    assert_eq!(log.admit(start), Some(0));
    for i in 1..=5 {
        assert_eq!(log.admit(start + Duration::from_millis(i * 100)), None);
    }
    assert_eq!(log.admit(start + Duration::from_secs(1)), Some(5));
    assert_eq!(log.admit(start + Duration::from_millis(1_500)), None);
}

/// Scenario: a metrics request whose gauge point carries an exemplar
/// arrives under the default configuration (`unsupported: reject`,
/// `metrics.exemplars` unset) with telemetry registered, and again under an
/// explicit `metrics.exemplars: reject`.
/// Guarantees: by default the point is admitted and the exemplar is counted
/// in `dropped.exemplars{signal=metrics}`; under the explicit reject the
/// request is refused as a permanent `unsupported` nack whose reason names
/// exemplars and the setting that keeps the points, and nothing enters the
/// block.
#[tokio::test(flavor = "current_thread")]
async fn an_exemplar_is_dropped_and_counted_by_default_and_refused_when_asked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, _rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let cfg = worker_config();
            assert_eq!(
                cfg.lake.unsupported,
                lake::config::UnsupportedPolicy::Reject
            );
            let mut worker = Worker::new(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                Arc::clone(&wall) as _,
                handler,
            );
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));
            worker.admit(exemplar_metrics_pdata());
            assert_eq!(worker.active.tokens.len(), 1, "the point is admitted");
            let metrics = worker.metrics.as_ref().expect("registered");
            assert_eq!(
                metrics
                    .exemplars
                    .get(super::super::metrics::ExemplarAttrs {
                        signal: SignalType::Metrics
                    })
                    .dropped_exemplars
                    .get(),
                1
            );

            let (handler, mut rx) = effects(4);
            let mut cfg = worker_config();
            cfg.lake.metrics.exemplars = Some(lake::config::ExemplarPolicy::Reject);
            let mut worker = Worker::new(
                cfg,
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.admit(exemplar_metrics_pdata());
            assert!(worker.active.data.is_empty(), "nothing was admitted");
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(nack.permanent);
                    assert_eq!(nack.cause, NackCause::Refused);
                    assert!(nack.reason.contains("exemplars"), "reason: {}", nack.reason);
                    assert!(
                        nack.reason.contains("metrics.exemplars: drop"),
                        "reason: {}",
                        nack.reason
                    );
                }
                other => panic!("expected an exemplar refusal, got {other:?}"),
            }
        })
        .await;
}
