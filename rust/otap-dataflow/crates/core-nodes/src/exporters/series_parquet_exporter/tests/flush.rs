// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Flush and retry: durable completion, retries, deadlines and the
//! cache commits that follow a write.

use super::support::*;

/// Scenario: a request is admitted, rotated into a real in-memory object store
/// flush, and completed.
/// Guarantees: no completion is emitted before the flush resolved, both files
/// exist in the store by the time the ack is queued, and only a completed
/// flush marks the descriptor committed in the cache under the flushed
/// block's partition.
#[tokio::test(flavor = "current_thread")]
async fn complete_files_before_ack() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            let partition = worker.active.data.partition;
            assert!(!worker.cache.is_committed(&id, partition));

            worker.rotate();
            assert!(
                worker.notify.is_empty(),
                "nothing is decided before the flush resolves"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(5), rx.recv())
                    .await
                    .is_err(),
                "a started flush is not a durable one"
            );

            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let report = done
                .as_ref()
                .expect("the flush task joins")
                .result
                .as_ref()
                .expect("the write succeeds");
            assert_eq!(report.files.len(), 2);
            for (_, path, _) in &report.files {
                assert!(store.head(path).await.is_ok());
            }

            worker.complete(done);
            assert!(worker.cache.is_committed(&id, partition));
            assert!(worker.notify.next().await.is_ok());
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the series file is written, the first values file write fails,
/// and the store heals before the retry.
/// Guarantees: the retry re-uses the frozen file names and byte-identical
/// objects, the block is acknowledged exactly once, and the retry is counted
/// without counting a flush failure.
#[tokio::test(flavor = "current_thread")]
async fn values_retry_reuses_paths_and_bytes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::ValuesOnce);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            let _ticker = ticking(&sim, Duration::from_millis(250));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert_eq!(
                done.as_ref().expect("the flush resolves").attempts,
                2,
                "the failed write is retried once"
            );

            worker.complete(done);
            worker.notify.next().await.expect("the ack is sent");
            assert!(matches!(
                rx.recv().await.expect("ack"),
                PipelineCompletionMsg::DeliverAck { .. }
            ));

            worker.sample_metrics();
            let metrics = worker.metrics.as_mut().expect("metrics");
            assert_eq!(
                metrics.worker.flush_retries.get(),
                1,
                "the extra attempt is reported as a retry"
            );
            assert!(
                metrics.flush_failures.terminal_snapshots().is_empty(),
                "a retried flush that succeeded is not a failure"
            );
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            let writes = store.hooks().writes.lock().expect("writes lock");
            assert_eq!(writes.len(), 4, "two files, one of them written twice");
            assert_eq!(writes[0], writes[2], "the series file is rewritten as-is");
            assert_eq!(writes[1], writes[3], "the values file is rewritten as-is");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the series file write fails until the absolute retry deadline,
/// and a request carrying the same descriptor arrives afterwards.
/// Guarantees: the failed block nacks retryably without marking the cache, and
/// the next block writes that descriptor again.
#[tokio::test(flavor = "current_thread")]
async fn failed_descriptor_does_not_poison_cache() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(30);
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(50));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            worker.complete(done);
            worker.notify.next().await.expect("the nack is sent");
            match rx.recv().await.expect("nack") {
                PipelineCompletionMsg::DeliverNack { nack } => assert!(
                    !nack.permanent,
                    "a storage failure is the sender's to retry"
                ),
                other => panic!("expected a retryable nack, got {other:?}"),
            }
            assert!(
                !worker
                    .cache
                    .is_committed(&id, lake::clock::PartitionId::from_unix_secs(0)),
                "a block that was never written marks nothing durable"
            );
            drain_cleanup(&mut worker).await;

            store.hooks().set(Fault::None);
            worker.admit(logs_pdata());
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the descriptor is written again rather than assumed durable"
            );
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("the second block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert_eq!(worker.notify.len(), 1, "the second block is acknowledged");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: a flush parked across an hour boundary is joined by the same
/// series in a later partition, and the cache evicts that series before the
/// flush resolves.
/// Guarantees: the commit names the flushed block's own partition, and it
/// cannot retract the descriptor the next partition still owes.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_series_and_eviction_preserve_partition_coverage() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(3_599_000_000_000));
            let mut cfg = worker_config();
            cfg.cache_entries = 1;
            let mut worker = Worker::new(cfg, store.clone(), Arc::clone(&wall) as _, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            store.hooks().entered.notified().await;

            wall.set(3_600_000_000_000);
            worker.admit(logs_pdata());
            assert!(
                worker.pending.is_some(),
                "a request for the next hour waits for its own block"
            );
            worker.cache.touch([99; 16]);

            store.hooks().set(Fault::None);
            store.hooks().release.notify_one();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert_eq!(
                worker.cache.last_committed(&id),
                Some(lake::clock::PartitionId::from_unix_secs(3599)),
                "the commit names the partition that was written"
            );
            drain_cleanup(&mut worker).await;

            worker.rotate();
            worker.resume_pending();
            assert_eq!(
                worker.active.data.partition,
                lake::clock::PartitionId::from_unix_secs(3600)
            );
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the new partition writes its own copy of the descriptor"
            );
        })
        .await;
}

/// Scenario: the same series is admitted to the ACTIVE block while the
/// FLUSHING block carrying it is parked in the same window.
/// Guarantees: both blocks keep their own descriptor copy, and the flushed
/// block's commit cannot retract the copy the ACTIVE block still owes.
#[tokio::test(flavor = "current_thread")]
async fn same_window_overlap_keeps_both_descriptors() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store.clone(), wall, handler);

            worker.admit(logs_pdata());
            let id = *worker
                .active
                .data
                .pending_series
                .iter()
                .next()
                .expect("the request carries a descriptor");
            worker.rotate();
            store.hooks().entered.notified().await;

            worker.admit(logs_pdata());
            assert!(
                worker.active.data.pending_series.contains(&id),
                "the ACTIVE block carries its own copy"
            );
            assert_eq!(
                worker
                    .flushing
                    .as_ref()
                    .expect("a rotated block is flushing")
                    .tokens
                    .len(),
                1,
                "the flushing block still owes its own request"
            );
            let partition = worker.active.data.partition;
            assert!(!worker.cache.is_committed(&id, partition));

            store.hooks().set(Fault::None);
            store.hooks().release.notify_one();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            assert!(worker.cache.is_committed(&id, partition));
            assert!(
                worker.active.data.pending_series.contains(&id),
                "a late commit does not retract a descriptor already reserved"
            );
            drain_cleanup(&mut worker).await;

            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("the second block is flushing")
                .finish()
                .await;
            let report = done
                .as_ref()
                .expect("the flush resolves")
                .result
                .as_ref()
                .expect("the write succeeds");
            assert_eq!(report.files.len(), 2, "the descriptor is written again");
            worker.complete(done);
            assert_eq!(worker.notify.len(), 2);
        })
        .await;
}

/// Scenario: a write is parked in the store when its absolute retry deadline
/// expires, while cleanup is allowed a whole second.
/// Guarantees: the retryable decision is published at the deadline rather than
/// after the cleanup allowance, and the FLUSHING slot stays occupied by the
/// cleanup until it has finished.
#[tokio::test(flavor = "current_thread")]
async fn retry_deadline_publishes_before_cleanup_and_reserves_slot() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            store.hooks().entered.notified().await;

            // Only the retry deadline has elapsed. The write is still parked
            // in the store, so a decision that arrives now cannot have waited
            // for either the write or the cleanup allowance.
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the deadline fails the block"
            );
            worker.complete(done);
            assert_eq!(
                worker.notify.outcomes()[Outcome::Storage as usize],
                1,
                "the block is nacked as retryable storage"
            );

            // The slot the cleanup holds is the same FLUSHING slot, so nothing
            // rotates into it while the abandoned write is still unwinding.
            worker.admit(logs_pdata());
            worker.rotate();
            assert!(worker.flushing.is_none(), "no second write is started");
            assert!(worker.cleaning.is_some(), "the slot is still occupied");

            let mut job = worker.cleaning.take().expect("the occupied slot");
            sim.advance(Duration::from_secs(2));
            job.cleanup().await.expect("the cleanup is bounded");

            store.hooks().set(Fault::None);
            worker.rotate();
            assert!(
                worker.flushing.is_some(),
                "the released slot takes the next block"
            );
        })
        .await;
}

/// Scenario: the one write of a flush never returns, so the block's retry
/// deadline expires while that first attempt is still in flight.
/// Guarantees: the flush ends as a distinct deadline outcome rather than as a
/// cancellation -- the cancellation counter stays at zero -- and every request
/// of the block is nacked retryably with a reason that says the deadline
/// expired with no attempt returned, so a storage hang is never reported as a
/// shutdown or as an unexplained failure.
#[tokio::test(flavor = "current_thread")]
async fn a_hung_write_expires_the_flush_deadline_as_its_own_outcome() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            store.hooks().entered.notified().await;
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 1);
            assert!(
                matches!(
                    finished.result,
                    Err(lake::Error::Transient(
                        lake::TransientError::DeadlineExceeded {
                            attempts: 1,
                            last: None
                        }
                    ))
                ),
                "a hung write is a deadline, not a cancellation: {:?}",
                finished.result.as_ref().err()
            );
            worker.complete(done);
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(flush_failures(metrics, WriteFailure::Deadline), 1);
            assert_eq!(flush_failures(metrics, WriteFailure::Cancelled), 0);
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "could not write to object storage (unavailable); retry the request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            sim.advance(Duration::from_secs(10));
            job.cleanup().await.expect("the task is released");
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: every write fails, but only after twenty seconds -- the shape of
/// a cloud store retrying one request internally until its own
/// `retry_timeout` -- under a sixty-second flush deadline.
/// Guarantees: the flush retries the block until the deadline, counts every
/// attempt in `flush.retries`, and nacks the block retryably with the fixed
/// sentence classifying the store as unavailable, while the
/// `series_parquet.flush.failed` ERROR carries the destination's last error,
/// so an outage is visible with its cause rather than as a bare "cancelled"
/// with zero retries. Each of the two attempts that returned is logged at
/// WARN as retryable with its attempt number and the store's error.
#[tokio::test(flavor = "current_thread")]
async fn a_slowly_failing_store_surfaces_its_last_error_at_the_deadline() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::SlowFail);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(100));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            let finished = done.as_ref().expect("the flush resolves");
            // Attempts start at 0 s, 20.2 s and 40.6 s; the third is still
            // failing when the deadline expires at 60 s.
            assert_eq!(finished.attempts, 3);
            match &finished.result {
                Err(lake::Error::Transient(lake::TransientError::DeadlineExceeded {
                    attempts: 3,
                    last: Some(last),
                })) => assert!(
                    last.to_string().contains("injected store failure"),
                    "{last}"
                ),
                other => panic!("expected the deadline with the last error, got {other:?}"),
            }
            worker.complete(done);
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_retries.get(), 2);
            assert_eq!(flush_failures(metrics, WriteFailure::Deadline), 1);
            assert_eq!(metrics.worker.flush_cancelled.get(), 0);

            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "could not write to object storage (unavailable); retry the request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            let ticker = ticking(&sim, Duration::from_secs(1));
            job.cleanup().await.expect("the task is released");
            drop(ticker);
        })
        .await;
    let failed = events.named("series_parquet.flush.failed");
    assert_eq!(failed.len(), 1, "{failed:?}");
    let field = |name: &str| failed[0].fields.get(name).cloned();
    assert_eq!(field("window_start"), Some(FieldValue::I64(0)));
    assert_eq!(field("seq"), Some(FieldValue::U64(0)));
    assert_eq!(field("requests"), Some(FieldValue::U64(1)));
    assert!(matches!(field("bytes"), Some(FieldValue::U64(n)) if n > 0));
    assert!(
        field("file").is_some_and(|file| file.text().ends_with(".parquet")),
        "{failed:?}"
    );
    assert_eq!(
        field("error_type"),
        Some(FieldValue::Str("deadline".into()))
    );
    assert!(
        failed[0].fields.get("error").is_some_and(|error| {
            let text = error.text();
            text.contains("after 3 attempt(s); last error:")
                && text.contains("injected store failure")
        }),
        "{failed:?}"
    );
    let logged = events.named("series_parquet.flush.attempt_failed");
    assert_eq!(
        logged.len(),
        2,
        "two attempts returned a failure: {logged:?}"
    );
    for (index, event) in logged.iter().enumerate() {
        assert_eq!(event.level, tracing::Level::WARN);
        assert_eq!(
            event.fields.get("attempt"),
            Some(&FieldValue::U64(index as u64 + 1))
        );
        assert_eq!(event.fields.get("retryable"), Some(&FieldValue::Bool(true)));
        assert_eq!(event.fields.get("seq"), Some(&FieldValue::U64(0)));
        assert!(
            matches!(
                event.fields.get("deadline_remaining"),
                Some(FieldValue::Debug(_))
            ),
            "{event:?}"
        );
        assert!(
            event
                .fields
                .get("error")
                .is_some_and(|error| error.text().contains("injected store failure")),
            "{event:?}"
        );
    }
    // Attempts 2 and 3 are retries, announced at INFO with the block's
    // sequence and the time left before its deadline.
    let retries: Vec<_> = events
        .named("series_parquet.flush.attempt")
        .into_iter()
        .filter(|event| event.level == tracing::Level::INFO)
        .collect();
    assert_eq!(retries.len(), 2, "{retries:?}");
    for event in &retries {
        assert_eq!(event.fields.get("seq"), Some(&FieldValue::U64(0)));
        assert!(
            matches!(
                event.fields.get("deadline_remaining"),
                Some(FieldValue::Debug(_))
            ),
            "{event:?}"
        );
    }
}

/// Scenario: every write fails after twenty seconds under a sixty-second
/// flush deadline, and telemetry is sampled while the second attempt is still
/// in flight, before the flush has resolved.
/// Guarantees: `flush.retries` already reads 1, so an outage in progress
/// shows its retries instead of zero until the deadline.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_is_counted_when_it_starts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::SlowFail);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(logs_pdata());
            worker.rotate();
            // The first attempt fails at 20 s and the second starts at 20.2 s.
            step_for(&sim, Duration::from_secs(25)).await;
            assert!(
                store
                    .hooks()
                    .entered_at
                    .lock()
                    .expect("entered_at lock")
                    .len()
                    >= 2,
                "the second attempt has reached the store"
            );
            assert!(worker.flushing.is_some(), "the flush has not resolved");
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_retries.get(), 1);

            let ticker = ticking(&sim, Duration::from_secs(1));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            job.cleanup().await.expect("the task is released");
            drop(ticker);
        })
        .await;
}

/// Scenario: an encoding bug and a storage I/O error arrive as the same lake
/// error type.
/// Guarantees: only a failure with a storage origin earns whole-block retries.
#[test]
fn retry_classifier_distinguishes_encoding_from_storage() {
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::General("encoding bug".into())
    )));
    assert!(lake::Error::is_retryable(&lake::Error::from(
        object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }
    )));
    assert!(lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::External(Box::new(object_store::Error::Generic {
            store: "test",
            source: Box::new(std::io::Error::other("offline")),
        }))
    )));
    assert!(!lake::Error::is_retryable(&lake::Error::cancelled(None)));
    // Refused credentials and a missing bucket or prefix are storage errors
    // that no retry cures, however they are wrapped.
    let denied = || object_store::Error::PermissionDenied {
        path: "p".into(),
        source: "denied".into(),
    };
    assert!(!lake::Error::is_retryable(&lake::Error::from(denied())));
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        object_store::Error::NotFound {
            path: "p".into(),
            source: "no such bucket".into(),
        }
    )));
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::External(Box::new(std::io::Error::other(denied())))
    )));
    assert!(!lake::Error::is_retryable(&lake::Error::from(
        parquet::errors::ParquetError::External(Box::new(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied
        )))
    )));
}

/// Scenario: every write is refused as `PermissionDenied` under a
/// sixty-second flush deadline.
/// Guarantees: the block fails on its first attempt instead of being retried
/// until the deadline, so refused credentials are reported at once rather
/// than a minute later as a deadline expiry; the producer is told the fixed
/// sentence classifying the write as rejected by the store, without the
/// store's error text or the object path.
#[tokio::test(flavor = "current_thread")]
async fn a_permission_error_is_not_retried_until_the_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Denied);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_secs(60);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store, wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            let ticker = ticking(&sim, Duration::from_millis(100));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            drop(ticker);
            let finished = done.as_ref().expect("the flush resolves");
            assert_eq!(finished.attempts, 1, "a refused credential is not retried");
            let error = finished.result.as_ref().expect_err("the write is refused");
            assert!(error.to_string().contains("access denied"), "{error}");
            worker.complete(done);
            assert!(worker.notify.next().await.is_ok());
            match rx.recv().await.expect("a nack") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(
                        nack.reason,
                        "could not write to object storage (rejected by the store); retry the \
                         request"
                    );
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            assert_no_more_completions(&mut rx);
            drain_cleanup(&mut worker).await;
        })
        .await;
}

/// Scenario: a write attempt fails with an error no retry can cure, so the
/// flush ends on that first attempt.
/// Guarantees: the failed attempt still goes through the per-attempt WARN
/// (`series_parquet.flush.attempt_failed`) with its attempt number and the
/// store's error, exactly as a retried failure does, so every failed attempt
/// leaves a per-attempt trace and not only the block-level ERROR.
#[tokio::test(flavor = "current_thread")]
async fn a_non_retryable_failed_attempt_is_logged_at_warn() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            store.hooks().set(Fault::Denied);
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);
            worker.admit(logs_pdata());
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert_eq!(done.as_ref().expect("the flush resolves").attempts, 1);
        })
        .await;
    let logged = events.named("series_parquet.flush.attempt_failed");
    assert_eq!(logged.len(), 1, "one failed attempt, one WARN: {logged:?}");
    let event = &logged[0];
    assert_eq!(event.level, tracing::Level::WARN);
    assert_eq!(event.fields.get("attempt"), Some(&FieldValue::U64(1)));
    assert_eq!(
        event.fields.get("retryable"),
        Some(&FieldValue::Bool(false)),
        "a refused credential is not retryable"
    );
    assert!(
        event
            .fields
            .get("error")
            .is_some_and(|error| error.text().contains("access denied")),
        "{event:?}"
    );
}

/// Scenario: the last object of a block is written in the same engine-clock
/// step in which the block's retry deadline expires, so the write result and
/// the deadline are both ready when the flush task is next polled.
/// Guarantees: the flush reports the success, not a deadline expiry, so a
/// block whose files exist is acknowledged rather than nacked and resent.
#[tokio::test(flavor = "current_thread")]
async fn a_write_finishing_as_the_deadline_expires_is_a_success() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            // The series object parks first and is released well before the
            // deadline; then the values object parks, and is released in the
            // same step as the deadline.
            store.hooks().entered.notified().await;
            store.hooks().release.notify_one();
            store.hooks().entered.notified().await;
            store.hooks().release.notify_one();
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let finished = done.as_ref().expect("the flush resolves");
            assert!(
                finished.result.is_ok(),
                "a finished write is not a deadline expiry: {:?}",
                finished.result.as_ref().err()
            );
        })
        .await;
}

/// Scenario: a values multipart upload is initiated and then wedges, its parts
/// never landing and its abort never returning, while the block's absolute
/// retry deadline expires.
/// Guarantees: the retryable decision is published at the deadline, the
/// FLUSHING slot stays occupied until the cleanup ends, the cleanup does end
/// once the abort allowance elapses, and no completed values object is left in
/// the store. The failed abort is reported: one `series_parquet.flush.cleanup`
/// WARN with the block's sequence, file, attempt and abort error, and
/// `flush.abort_failures` reads 1.
#[tokio::test(flavor = "current_thread")]
async fn a_wedged_multipart_abort_is_bounded_and_leaves_no_object() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::MultipartWedge);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            // Set past validation on purpose. The S3 minimum part size puts a
            // real multipart upload out of reach of any block a unit test can
            // encode in milliseconds; a small buffer capacity reaches the same
            // `put_multipart_opts`, `put_part` and `abort` calls at a block
            // size that costs nothing to build. The small row group is what
            // makes the writer push parts while it is still writing, and the
            // small merge chunk keeps it writing chunk after chunk, so it is
            // still blocked on the wedged part -- in the phase where an abort
            // is attempted -- when the deadline expires, even though the flush
            // polls the write once more before it looks at the deadline.
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            cfg.lake.sorting.merge_chunk_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the wedged upload takes a part", || {
                store.hooks().parts.load(SeqCst) > 0
            })
            .await;

            // Only the retry deadline has elapsed: the abort allowance is
            // untouched, so a decision that arrives now cannot have waited for
            // the cleanup.
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the deadline fails the block"
            );
            worker.complete(done);
            assert_eq!(
                worker.notify.outcomes()[Outcome::Storage as usize],
                1,
                "the block is nacked as retryable storage"
            );

            // The slot the cleanup holds is the same FLUSHING slot.
            worker.rotate();
            assert!(worker.flushing.is_none(), "no second write is started");
            assert!(worker.cleaning.is_some(), "the slot is still occupied");
            until("the wedged upload is aborted", || {
                store.hooks().aborts.load(SeqCst) > 0
            })
            .await;

            let mut job = worker.cleaning.take().expect("the occupied slot");
            sim.advance(Duration::from_secs(2));
            job.cleanup().await.expect("the cleanup is bounded");
            drop(job);

            let path = lake::sink::object_path(
                lake::schema::Dataset::LogsValues,
                lake::clock::PartitionId::from_unix_secs(0),
                0,
                &lake::sink::FileNaming::new(&worker.cfg.lake.writer_id),
                1,
            );
            assert!(
                store.inner().head(&path).await.is_err(),
                "a wedged upload never completes an object"
            );
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_abort_failures.get(), 1);
            assert_eq!(metrics.worker.flush_late_commits.get(), 0);
            assert_eq!(flush_failures(metrics, WriteFailure::Deadline), 1);
        })
        .await;
    let cleanup = events.named("series_parquet.flush.cleanup");
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    let event = &cleanup[0];
    assert_eq!(event.level, tracing::Level::WARN);
    let field = |name: &str| event.fields.get(name).cloned();
    assert_eq!(
        field("outcome"),
        Some(FieldValue::Str("abort_failed".into()))
    );
    assert_eq!(field("seq"), Some(FieldValue::U64(0)));
    assert_eq!(field("attempt"), Some(FieldValue::U64(1)));
    assert!(
        field("file").is_some_and(|file| file.text().ends_with(".parquet")),
        "{event:?}"
    );
    assert!(
        field("abort_error").is_some_and(|error| !error.text().is_empty()),
        "{event:?}"
    );
}

/// A cleanup trace for block 7 over the two objects of `part-x.parquet`, on
/// `store`.
fn cleanup_trace(
    store: Arc<dyn ObjectStore>,
) -> (
    super::super::flush::Trace,
    std::rc::Rc<super::super::flush::FlushShared>,
    Vec<object_store::path::Path>,
) {
    let paths: Vec<object_store::path::Path> = ["dataset=series", "dataset=values"]
        .into_iter()
        .map(|dir| object_store::path::Path::from(format!("{dir}/part-x.parquet")))
        .collect();
    let shared = std::rc::Rc::new(super::super::flush::FlushShared {
        store,
        tally: super::super::flush::FlushTally::default(),
    });
    let trace = super::super::flush::Trace::for_test(paths.clone(), 7, std::rc::Rc::clone(&shared));
    (trace, shared, paths)
}

/// Scenario: the cleanup of a failed block is reported for every way it can
/// end: the write completed anyway; it was cancelled with no object, with
/// one of the two objects, and with both objects in the store; its abort
/// timed out inside the sink; its abort failed after a write error; it did
/// not unwind by the cleanup cutoff; and the probe itself could not finish
/// by the cutoff.
/// Guarantees: a completed write and a cancellation that left every object
/// are INFO `outcome=late_commit` and count in `flush.late_commits`; no
/// object is DEBUG `aborted`; one object is INFO `partial` with the count;
/// the three abort failures are WARN `abort_failed` with their abort error
/// and count in `flush.abort_failures`; an unfinished probe is WARN
/// `unknown` and counts nothing. Every event names the block's sequence,
/// attempt and file.
#[tokio::test(flavor = "current_thread")]
async fn every_cleanup_outcome_is_logged_and_counted() {
    let events = capture();
    let store = Arc::new(object_store::memory::InMemory::new());
    let (trace, shared, paths) = cleanup_trace(store.clone());
    let later = || clock::now() + Duration::from_secs(60);
    let cancelled = || Some(Err(lake::Error::cancelled(None)));
    trace
        .cleaned_up(
            2,
            Some(Ok(lake::sink::FlushReport { files: Vec::new() })),
            later(),
        )
        .await;
    trace.cleaned_up(2, cancelled(), later()).await;
    let _ = store
        .put(&paths[0], PutPayload::from_static(b"series"))
        .await
        .expect("put");
    trace.cleaned_up(2, cancelled(), later()).await;
    let _ = store
        .put(&paths[1], PutPayload::from_static(b"values"))
        .await
        .expect("put");
    trace.cleaned_up(2, cancelled(), later()).await;
    trace
        .cleaned_up(
            2,
            Some(Err(lake::Error::cancelled(Some("abort timed out".into())))),
            later(),
        )
        .await;
    trace
        .cleaned_up(
            2,
            Some(Err(lake::Error::Transient(
                lake::TransientError::AbortFailed {
                    source: Box::new(lake::Error::internal("encode")),
                    abort_error: "abort refused".into(),
                },
            ))),
            later(),
        )
        .await;
    trace.cleaned_up(2, None, later()).await;
    // A store whose HEAD takes an hour, probed with a cutoff that has
    // already passed.
    let slow = object_store::throttle::ThrottledStore::new(
        object_store::memory::InMemory::new(),
        object_store::throttle::ThrottleConfig {
            wait_get_per_call: Duration::from_secs(3600),
            ..Default::default()
        },
    );
    let (slow_trace, slow_shared, _) = cleanup_trace(Arc::new(slow));
    slow_trace.cleaned_up(2, cancelled(), clock::now()).await;

    assert_eq!(shared.tally.late_commits.get(), 2);
    assert_eq!(shared.tally.abort_failures.get(), 3);
    assert_eq!(slow_shared.tally.late_commits.get(), 0);
    assert_eq!(slow_shared.tally.abort_failures.get(), 0);
    let logged = events.named("series_parquet.flush.cleanup");
    let summary: Vec<_> = logged
        .iter()
        .map(|event| {
            let text = |name: &str| event.fields.get(name).map(|v| v.text().to_owned());
            (event.level, text("outcome"), text("abort_error"))
        })
        .collect();
    let event = |level, outcome: &str, abort: Option<&str>| {
        (level, Some(outcome.to_owned()), abort.map(str::to_owned))
    };
    use tracing::Level;
    assert_eq!(
        summary,
        [
            event(Level::INFO, "late_commit", None),
            event(Level::DEBUG, "aborted", None),
            event(Level::INFO, "partial", None),
            event(Level::INFO, "late_commit", None),
            event(Level::WARN, "abort_failed", Some("abort timed out")),
            event(Level::WARN, "abort_failed", Some("abort refused")),
            event(
                Level::WARN,
                "abort_failed",
                Some("the write did not unwind by the cleanup cutoff")
            ),
            event(Level::WARN, "unknown", None),
        ]
    );
    assert_eq!(logged[2].fields.get("present"), Some(&FieldValue::U64(1)));
    assert_eq!(logged[2].fields.get("objects"), Some(&FieldValue::U64(2)));
    assert!(
        logged[7]
            .fields
            .get("probe_error")
            .is_some_and(|error| error.text().contains("cutoff")),
        "{:?}",
        logged[7]
    );
    for event in &logged {
        assert_eq!(event.fields.get("seq"), Some(&FieldValue::U64(7)));
        assert_eq!(event.fields.get("attempt"), Some(&FieldValue::U64(2)));
        assert_eq!(
            event.fields.get("file").map(FieldValue::text),
            Some("part-x.parquet")
        );
    }
}

/// Scenario: a values multipart upload whose CompleteMultipartUpload lands in
/// the store and whose response is then lost (the call never returns), so
/// the flush's retry deadline expires with the upload finalized, through the
/// real sink.
/// Guarantees: the block is nacked as retryable storage, and the cleanup
/// probes the frozen objects, finds both, and reports a late commit: one INFO
/// `series_parquet.flush.cleanup` with `outcome=late_commit` naming the file,
/// and `flush.late_commits` reads 1 while `flush.abort_failures` reads 0.
#[tokio::test(flavor = "current_thread")]
async fn a_completed_upload_whose_response_is_lost_is_a_late_commit() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let store = fault_store();
            store.hooks().set(Fault::LostComplete);
            let (handler, _rx) = effects(8);
            let mut cfg = worker_config();
            cfg.window.flush_retry_deadline = Duration::from_millis(20);
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            // Past validation on purpose, as in the wedged-upload test: a
            // small part size puts the values file on the multipart path at
            // a block size a unit test can build.
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            cfg.lake.sorting.merge_chunk_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(super::super::metrics::Metrics::register(
                &context,
                &worker.cfg.lake,
            ));

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the values upload is completed in the store", || {
                store.hooks().completes.load(SeqCst) > 0
            })
            .await;
            sim.advance(Duration::from_millis(20));
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            assert!(
                done.as_ref().expect("the flush resolves").result.is_err(),
                "the lost response fails the block"
            );
            worker.complete(done);
            assert_eq!(worker.notify.outcomes()[Outcome::Storage as usize], 1);
            let mut job = worker.cleaning.take().expect("the cleanup slot");
            job.cleanup().await.expect("the cleanup is bounded");
            drop(job);
            worker.sample_metrics();
            let metrics = worker.metrics.as_ref().expect("metrics");
            assert_eq!(metrics.worker.flush_late_commits.get(), 1);
            assert_eq!(metrics.worker.flush_abort_failures.get(), 0);
        })
        .await;
    let cleanup = events.named("series_parquet.flush.cleanup");
    assert_eq!(cleanup.len(), 1, "{cleanup:?}");
    assert_eq!(cleanup[0].level, tracing::Level::INFO);
    assert_eq!(
        cleanup[0].fields.get("outcome"),
        Some(&FieldValue::Str("late_commit".into()))
    );
    assert!(
        cleanup[0]
            .fields
            .get("file")
            .is_some_and(|file| file.text().ends_with(".parquet")),
        "{cleanup:?}"
    );
}

/// Scenario: one request is admitted, its block is written to an in-memory
/// store on the first attempt, and the worker completes it.
/// Guarantees: the commit is logged once as `series_parquet.block.committed`
/// carrying the block's window start, its sequence, the path of its first
/// object and the attempt count, so an operator can go from a log line to
/// the files it wrote.
#[tokio::test(flavor = "current_thread")]
async fn a_committed_block_names_its_window_sequence_and_path() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let (handler, _rx) = effects(8);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(
                worker_config(),
                Arc::new(object_store::memory::InMemory::new()),
                wall,
                handler,
            );
            worker.admit(logs_pdata());
            let window = worker.active.data.window_start_secs;
            let seq = worker.active.data.seq;
            worker.rotate();
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            let path = done
                .as_ref()
                .expect("the flush task joins")
                .result
                .as_ref()
                .expect("the write succeeds")
                .files[0]
                .1
                .to_string();
            worker.complete(done);

            let logged = events.named("series_parquet.block.committed");
            assert_eq!(logged.len(), 1, "{logged:?}");
            let field = |name: &str| logged[0].fields.get(name).cloned();
            assert_eq!(field("window_start"), Some(FieldValue::I64(window)));
            assert_eq!(field("seq"), Some(FieldValue::U64(seq)));
            assert_eq!(field("path"), Some(FieldValue::Str(path)));
            assert_eq!(field("attempts"), Some(FieldValue::U64(1)));
        })
        .await;
}
