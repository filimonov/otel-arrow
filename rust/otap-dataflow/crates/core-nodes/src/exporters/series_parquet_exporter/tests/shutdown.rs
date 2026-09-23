// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shutdown: force-drained requests, the deadline and the release of
//! both blocks.

use super::support::*;

/// Scenario: two requests are buffered and a third is sent after the node has
/// already latched shutdown and refused the first two, with the upstream
/// sender still alive throughout.
/// Guarantees: every one of them is decided with a retryable `NodeShutdown`
/// nack and the node returns its terminal state only after the upstream
/// channel closes, so a node that has latched shutdown never returns while
/// requests it could still be handed are outstanding; and the node ends with
/// one `series_parquet.shutdown.complete` event summarizing that none was
/// admitted, all three were nacked, none was left to the deadline, and the
/// deadline was not reached.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_decides_every_force_drained_request() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(8);
            let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(8);
            let inbox = ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            );
            let (handler, mut rx) = effects(8);

            // The engine latches this and releases it only once the upstream
            // pdata channel is both empty and closed, so the two requests
            // below are force-drained before the node ever sees it.
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            for _ in 0..2 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a buffered request enqueues");
            }

            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let config = worker_config();
            let node = tokio::task::spawn_local(super::super::run(
                config.clone(),
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(super::super::metrics::Metrics::register(
                    &context,
                    &config.lake,
                )),
            ));

            for _ in 0..2 {
                expect_shutdown_nack(&mut rx).await;
            }

            // The node has refused everything it was handed and is idle, but
            // the sender is still alive, so this request must still be
            // decided rather than dropped with the inbox.
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("a late request enqueues");
            expect_shutdown_nack(&mut rx).await;

            // Closing the upstream pdata channel is what releases the latched
            // shutdown; the control sender stays alive, as it does in the
            // engine.
            drop(pdata_tx);
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns once the upstream channel closes")
                .expect("the node task joins");
            let terminal = match terminal {
                Ok(terminal) => terminal,
                Err(error) => panic!("unexpected node failure: {error}"),
            };
            // The counters of the interval the node ends must reach the
            // collector with it: nothing else will ever report them.
            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                3,
                "every force-drained request is counted as a shutdown refusal"
            );
            assert_eq!(
                terminal_value(snapshots, "notify.failures", &[]),
                0,
                "the completion channel took all three immediately"
            );
            assert_eq!(terminal_value(snapshots, "acks", &[]), 0);
            let summary = events.named("series_parquet.shutdown.complete");
            assert_eq!(summary.len(), 1, "{summary:?}");
            let field = |name: &str| summary[0].fields.get(name).cloned();
            assert_eq!(field("accepted"), Some(FieldValue::U64(0)));
            assert_eq!(field("acked"), Some(FieldValue::U64(0)));
            assert_eq!(field("nacked"), Some(FieldValue::U64(3)));
            assert_eq!(field("abandoned"), Some(FieldValue::U64(0)));
            assert_eq!(field("deadline_exceeded"), Some(FieldValue::Bool(false)));
            drop(control_tx);
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a flush is still outstanding.
/// Guarantees: the write is cancelled and every completion the worker still
/// owns is delivered as a retryable `NodeShutdown` nack, so a request is never
/// dropped undecided along with the worker.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_decides_every_outstanding_request() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(4);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(worker_config(), store, wall, handler);

            worker.admit(logs_pdata());
            worker.rotate();
            let elapsed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(elapsed);
            worker.abandon().await;

            assert!(worker.is_idle());
            match rx.recv().await.expect("a shutdown refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                    assert_eq!(nack.reason, Outcome::Shutdown.sentence());
                }
                other => panic!("expected a nack, got {other:?}"),
            }
        })
        .await;
}

/// Scenario: the shutdown deadline elapses while a values multipart upload is
/// wedged, so the abort its cancellation triggers never returns.
/// Guarantees: every request is decided and delivered before any waiting, and
/// the terminal return happens only once the supervising task has been
/// released -- within `upload.abort_timeout`, ended by an abort of the task
/// that is itself awaited.
#[tokio::test(flavor = "current_thread")]
async fn the_deadline_returns_only_once_the_flush_task_is_released() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_MULTIPART_WEDGE, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_secs(1);
            cfg.lake.upload.part_bytes = 4096;
            cfg.lake.upload.concurrency = 1;
            cfg.lake.parquet.row_group_bytes = 4096;
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);

            worker.admit(bulk_logs_pdata(20_000));
            worker.rotate();
            until("the wedged upload takes a part", || {
                store.parts.load(std::sync::atomic::Ordering::SeqCst) > 0
            })
            .await;
            worker.shutdown(clock::now());

            let cancel = worker
                .flushing
                .as_ref()
                .expect("a rotated block is flushing")
                .cancel
                .clone();
            assert!(
                !cancel.is_cancelled(),
                "nothing is cancelled before the decision is taken"
            );

            {
                let mut abandoning = std::pin::pin!(worker.abandon());
                assert!(
                    futures::poll!(&mut abandoning).is_pending(),
                    "a wedged upload is not released in one turn"
                );
                // No runtime turn has elapsed since that poll, so both of the
                // next two assertions describe the state at the moment the
                // decision phase suspended. The completion is already in the
                // channel, and the write has not yet been able to observe the
                // cancellation, whose store-visible effect is the abort.
                assert_eq!(
                    store.aborts.load(std::sync::atomic::Ordering::SeqCst),
                    0,
                    "the write is cancelled only after the decision is delivered"
                );
                let delivered = futures::poll!(std::pin::pin!(rx.recv()));
                match delivered {
                    std::task::Poll::Ready(Ok(PipelineCompletionMsg::DeliverNack { nack })) => {
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a delivered nack, got {other:?}"),
                }
                // The cancellation belongs to the same turn as the delivery it
                // follows, so it is never deferred past the bounded wait.
                assert!(
                    cancel.is_cancelled(),
                    "the write is cancelled in the turn that decided it"
                );
                for _ in 0..64 {
                    tokio::task::yield_now().await;
                    assert!(
                        futures::poll!(&mut abandoning).is_pending(),
                        "the abort never returns, so the slot is not released"
                    );
                }
                assert!(
                    store.aborts.load(std::sync::atomic::Ordering::SeqCst) > 0,
                    "the abandoned upload is aborted rather than dropped"
                );

                sim.advance(Duration::from_secs(2));
                (&mut abandoning).await;
            }
            assert!(
                worker.is_idle(),
                "both slot holders are gone once the deadline returns"
            );
        })
        .await;
}

/// Scenario: the inbox already holds buffered pdata when a Shutdown is
/// latched, so the request is force-drained past a closed admission gate.
/// Guarantees: the forced message exposes the latched deadline before the
/// Shutdown control message is released, and the request is refused with a
/// retryable `NodeShutdown` nack that is never counted as a delivery failure.
#[tokio::test(flavor = "current_thread")]
async fn forced_pdata_exposes_shutdown_and_is_retryably_nacked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (pdata_tx, control_tx, mut inbox) = inbox(2);
            let deadline = clock::now() + Duration::from_secs(1);
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline,
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");

            // Admission is closed, yet the buffered request is still handed
            // over: that is the force-drain the exporter has to decide.
            let data = match inbox.recv_when(false).await.expect("a forced request") {
                Message::PData(data) => data,
                other => panic!("expected forced pdata, got {other:?}"),
            };
            assert_eq!(
                inbox.shutdown_deadline(),
                Some(deadline),
                "the latched deadline is visible before the control message is released"
            );

            let (handler, mut rx) = effects(1);
            let mut notify = Notifier::new(handler, 2);
            notify.force_shutdown(data);
            assert_eq!(
                notify.failures(),
                0,
                "a free completion slot takes the refusal immediately"
            );
            match rx.recv().await.expect("a refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: the shutdown deadline elapses with a parked write holding the
/// FLUSHING slot, a populated ACTIVE block and one parked request.
/// Guarantees: the parked request is nacked the moment shutdown is latched,
/// every remaining uncommitted token of both blocks is nacked at the deadline,
/// and the worker is left holding nothing.
#[tokio::test(flavor = "current_thread")]
async fn deadline_nacks_both_blocks_and_pending() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(16);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut worker = Worker::new(cfg, store.clone(), wall.clone(), handler);

            // FLUSHING: one request, rotated into a write that never returns.
            worker.admit(logs_pdata());
            worker.rotate();
            store.entered.notified().await;
            // ACTIVE: one request in the block that is still open.
            worker.admit(logs_pdata());
            // Parked: a request whose admission window is later than the one
            // the ACTIVE block was opened for, and no block can be opened for
            // it while the flush slot is held.
            wall.set(1_000_000_000);
            worker.admit(logs_pdata());
            assert!(worker.pending.is_some());
            assert_eq!(worker.active.tokens.len(), 1);
            assert_eq!(
                worker
                    .flushing
                    .as_ref()
                    .expect("a rotated block is flushing")
                    .tokens
                    .len(),
                1
            );

            worker.shutdown(clock::now());
            assert!(
                worker.pending.is_none(),
                "the parked request is decided when shutdown is latched"
            );
            assert_eq!(worker.notify.len(), 1);

            let _ticker = ticking(&sim, Duration::from_millis(50));
            worker.abandon().await;

            for _ in 0..3 {
                match rx.recv().await.expect("a shutdown refusal") {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(!nack.permanent);
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
            assert!(worker.flushing.is_none());
            assert!(worker.cleaning.is_none());
            assert!(worker.is_idle(), "nothing is left undecided");
        })
        .await;
}

/// Scenario: shutdown is latched with one block already flushing and a second
/// block open, and storage recovers well before the deadline.
/// Guarantees: the node finishes the outstanding FLUSHING block, then rotates
/// and flushes the ACTIVE one, both requests are acknowledged only after their
/// files exist, and nothing is nacked.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_commits_both_blocks_before_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let cfg = worker_config();
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                Some(metrics),
            ));

            // Four requests fill a block, so the node rotates it immediately
            // and its write parks.
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the first block enqueues");
            }
            store.entered.notified().await;
            // The fifth joins the block that replaced it; the flush slot is
            // busy, so that block is still ACTIVE when shutdown arrives.
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request of the second block enqueues");
            // The inbox serves control before pdata, so the shutdown below
            // would force-drain the fifth request rather than let it be
            // admitted. The marker is behind it in the same pdata channel, so
            // its completion proves the fifth request is already in a block.
            marker(&pdata_tx, &mut rx, 99).await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Releasing the latched Shutdown is what closing the upstream
            // pdata channel does, exactly as the engine does it.
            drop(pdata_tx);
            // Storage heals, so both blocks can reach object storage inside
            // the deadline.
            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_waiters();

            for _ in 0..5 {
                assert!(
                    matches!(
                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                            .await
                            .expect("a completion arrives")
                            .expect("a completion arrives"),
                        PipelineCompletionMsg::DeliverAck { .. }
                    ),
                    "a block that reached storage before the deadline is acknowledged"
                );
            }
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            let snapshots = terminal.metrics();
            // Five requests in two blocks, plus the row-less marker.
            assert_eq!(terminal_value(snapshots, "acks", &[]), 6);
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                0
            );
            // One values file per block: the block shutdown found flushing and
            // the block it then rotated and flushed itself.
            let written: Vec<String> = store
                .writes
                .lock()
                .expect("writes lock")
                .iter()
                .map(|(path, _)| path.clone())
                .filter(|path| path.contains("dataset=values/"))
                .collect();
            assert_eq!(written.len(), 2, "both blocks reached storage: {written:?}");
            drop(control_tx);
        })
        .await;
}

/// Scenario: shutdown latches a 60 s deadline while a block's write is
/// parked, the node learns it from a force-drained request, and upstream drops
/// its pdata sender while the node is taking a control message; the clock then
/// moves past one second before the store heals.
/// Guarantees: the closed pdata channel releases the latched Shutdown with its
/// own deadline, so the flush is still awaited after that second and every
/// request of the block is acknowledged, not nacked `NodeShutdown`.
#[tokio::test(flavor = "current_thread")]
async fn a_closed_pdata_channel_keeps_the_latched_shutdown_deadline() {
    let events = capture();
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(8);
            let node = tokio::task::spawn_local(super::super::run(
                worker_config(),
                store.clone(),
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));

            // Four requests fill a block, so the node rotates it and its write
            // parks.
            for _ in 0..4 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("a request of the block enqueues");
            }
            store.entered.notified().await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(60),
                    reason: "test".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // Force-drained behind the latched Shutdown, which is how the node
            // learns the deadline and closes its admission.
            marker(&pdata_tx, &mut rx, 99).await;

            // The control message ends the node's current receive, so its next
            // receive starts on a pdata channel that is already closed.
            control_tx
                .send_async(NodeControlMsg::Config {
                    config: serde_json::json!({}),
                })
                .await
                .expect("the config enqueues");
            drop(pdata_tx);
            until("the node is handed the Shutdown", || {
                !events.named("series_parquet.shutdown").is_empty()
            })
            .await;
            let shutdown = events.named("series_parquet.shutdown");
            assert_eq!(
                shutdown[0].fields.get("reason").map(FieldValue::text),
                Some("test"),
                "the node is handed the latched Shutdown"
            );

            sim.advance(Duration::from_secs(2));
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_waiters();

            for _ in 0..4 {
                assert!(
                    matches!(
                        tokio::time::timeout(Duration::from_secs(5), rx.recv())
                            .await
                            .expect("a completion arrives")
                            .expect("a completion arrives"),
                        PipelineCompletionMsg::DeliverAck { .. }
                    ),
                    "a block flushed inside the latched deadline is acknowledged"
                );
            }
            let _ = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            drop(control_tx);
        })
        .await;
}

/// Scenario: the real `SeriesParquet::start` future is aborted while a storage
/// write is parked and will never return.
/// Guarantees: cancellation drops the parked write and releases the block, the
/// sink and the object store within the configured abort timeout, so a node
/// torn down mid-flush leaves nothing owned by a task nobody joins.
#[tokio::test(flavor = "current_thread")]
async fn dropping_start_cancels_flush_task() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, _rx) = effects(2);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let mut cfg = startable_config(1);
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut exporter = super::super::SeriesParquet::new(cfg);
            exporter.store_override = Some(store.clone());
            let node = tokio::task::spawn_local(Box::new(exporter).start(inbox, handler));

            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            store.entered.notified().await;
            assert_eq!(
                store.parked.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the write is parked inside the store"
            );

            node.abort();
            match node.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("start was not aborted"),
            }
            until("the cancelled node releases the store", || {
                Arc::strong_count(&store) == 1
                    && store.parked.load(std::sync::atomic::Ordering::SeqCst) == 0
            })
            .await;
            assert_eq!(
                store.parked_drops.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "the parked write future was dropped rather than leaked"
            );
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: a backlog of buffered requests is force-drained while the
/// completion channel is already full and nothing will ever read it.
/// Guarantees: every request still gets exactly one shutdown decision, each
/// undeliverable decision is counted as a delivery failure rather than parked,
/// and the node returns at the latched deadline instead of waiting for
/// completion credit.
#[tokio::test(flavor = "current_thread")]
async fn saturated_inbox_shutdown_stays_bounded() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (pdata_tx, control_tx, inbox) = inbox(32);
            let (handler, _completion_rx) = effects(1);
            // One delivered completion is enough to fill the channel, so every
            // decision the node takes from here on cannot be handed over.
            // Two slots, because the last one of a notifier is reserved for a
            // shutdown outcome and this priming completion is a normal one.
            let mut prime = Notifier::new(handler.clone(), 2);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);

            for _ in 0..32 {
                pdata_tx
                    .send_async(logs_pdata())
                    .await
                    .expect("the inbox fills");
            }
            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "saturated".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);

            let _ticker = ticking(&sim, Duration::from_secs(1));
            let terminal = tokio::time::timeout(
                Duration::from_secs(2),
                super::super::run(
                    cfg,
                    store,
                    Arc::new(lake::clock::TestWallClock::new(0)),
                    inbox,
                    handler,
                    Some(metrics),
                ),
            )
            .await
            .expect("shutdown stays bounded with a full completion channel")
            .expect("the node succeeds");

            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                32,
                "every force-drained request is decided exactly once"
            );
            assert_eq!(
                terminal_value(snapshots, "notify.failures", &[]),
                32,
                "every undeliverable decision is counted rather than dropped silently"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: window boundaries keep arriving while the one flush slot is held
/// by a write that never returns and the completion channel is already full,
/// and a telemetry control message and then a shutdown are sent into that.
/// Guarantees: the real node re-arms its boundary timer rather than losing or
/// spinning on it, still serves control messages, and observes the shutdown it
/// is then sent, counting the completion it could never hand over instead of
/// waiting for it.
#[tokio::test(flavor = "current_thread")]
async fn blocked_completion_keeps_boundary_and_control_live() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (pdata_tx, control_tx, inbox) = inbox(4);
            let (handler, _completion_rx) = effects(1);
            // Two slots, because the last one of a notifier is reserved for a
            // shutdown outcome and this priming completion is a normal one.
            let mut prime = Notifier::new(handler.clone(), 2);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store.clone(),
                wall.clone(),
                inbox,
                handler,
                Some(metrics),
            ));

            // Boundaries keep arriving for the rest of the test, so the
            // request reaches a sealed block whichever turn the node admits it
            // on, and every later boundary is one the node meets with its
            // flush slot already held.
            let _ticker = ticking_windows(&sim, &wall);
            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            store.entered.notified().await;

            let (samples, reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(64);
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry {
                    metrics_reporter: reporter,
                })
                .await
                .expect("the telemetry control enqueues");
            until(
                "telemetry is served while everything else is blocked",
                || samples.try_recv().is_ok(),
            )
            .await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(1),
                    reason: "blocked completions".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            drop(pdata_tx);
            let terminal = tokio::time::timeout(Duration::from_secs(5), node)
                .await
                .expect("the node returns")
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_eq!(
                terminal_value(terminal.metrics(), "notify.failures", &[]),
                1,
                "the undeliverable decision is counted, not retried forever"
            );
            drop(control_tx);
        })
        .await;
}

/// Scenario: the inbox latches a Shutdown it cannot release yet, because an
/// upstream sender is still alive, and hands the node a telemetry control
/// message while it drains.
/// Guarantees: the node takes the latched deadline from the inbox rather than
/// waiting for a Shutdown message it has not been given, so the block it holds
/// is sealed and acknowledged instead of waiting for a window boundary ten
/// minutes away.
#[tokio::test(flavor = "current_thread")]
async fn a_latched_deadline_starts_the_drain_before_the_shutdown_message() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = Arc::new(object_store::memory::InMemory::new());
            let (handler, mut rx) = effects(8);
            let (pdata_tx, control_tx, inbox) = inbox(4);
            let mut cfg = worker_config();
            // Far enough away that nothing but the shutdown can seal a block.
            cfg.window.interval = Duration::from_secs(600);
            let node = tokio::task::spawn_local(super::super::run(
                cfg,
                store,
                Arc::new(lake::clock::TestWallClock::new(0)),
                inbox,
                handler,
                None,
            ));

            pdata_tx
                .send_async(logs_pdata())
                .await
                .expect("the request enqueues");
            // Behind it in the same channel, so its completion proves the
            // request before it is already in the ACTIVE block.
            marker(&pdata_tx, &mut rx, 99).await;

            control_tx
                .send_async(NodeControlMsg::Shutdown {
                    deadline: clock::now() + Duration::from_secs(30),
                    reason: "latched".to_owned(),
                })
                .await
                .expect("the shutdown enqueues");
            // The inbox holds that message back while `pdata_tx` is alive, so
            // this is the only thing the node is handed after the latch.
            let (_samples, metrics_reporter) =
                otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(8);
            control_tx
                .send_async(NodeControlMsg::CollectTelemetry { metrics_reporter })
                .await
                .expect("the telemetry control enqueues");

            assert!(
                matches!(
                    tokio::time::timeout(Duration::from_secs(5), rx.recv())
                        .await
                        .expect("the held block is sealed without its window ending")
                        .expect("a completion arrives"),
                    PipelineCompletionMsg::DeliverAck { .. }
                ),
                "the block the node was holding reached storage"
            );
            node.abort();
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: the real `drive` loop is polled for the first time in a state
/// where its shutdown deadline has already elapsed and the flush task has
/// already published a successful result that nothing has taken yet. The
/// deadline branch is biased above the branch that awaits that result, so both
/// are ready in the same poll and the deadline wins it.
/// Guarantees: the block whose files exist is acknowledged and its descriptors
/// are committed, rather than refused as uncommitted work, so shutdown never
/// asks a producer to resend rows that are already in object storage.
#[tokio::test(flavor = "current_thread")]
async fn a_flush_ready_at_the_deadline_is_acknowledged_not_nacked() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = Arc::new(FaultStore::default());
            store
                .mode
                .store(FAULT_PARK, std::sync::atomic::Ordering::SeqCst);
            let (handler, mut rx) = effects(4);
            let (pdata_tx, control_tx, inbox) = inbox(2);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let mut worker = Worker::new(cfg, store.clone(), wall, handler);
            worker.metrics = Some(metrics);

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

            // The write parks, then is released, and the supervising task runs
            // to completion. The loop has not been created yet, so nothing can
            // take the result it publishes: that is the state the deadline
            // branch has to be able to win.
            store.entered.notified().await;
            store
                .mode
                .store(FAULT_NONE, std::sync::atomic::Ordering::SeqCst);
            store.release.notify_waiters();
            until("the flush publishes its result", || {
                worker
                    .flushing
                    .as_ref()
                    .is_some_and(|job| job.task_finished())
            })
            .await;

            // An already-elapsed deadline, so the deadline sleep and the flush
            // result are both ready the first time the loop is polled.
            let elapsed = clock::now()
                .checked_sub(Duration::from_secs(1))
                .expect("an instant one second in the past");
            worker.shutdown(elapsed);

            let terminal = super::super::drive(&mut worker, inbox)
                .await
                .expect("the loop returns at its deadline");
            let snapshots = terminal.metrics();
            assert_eq!(
                terminal_value(snapshots, "nacks", &[("error.type", "shutdown")]),
                0,
                "a block whose files exist is never refused"
            );
            assert_eq!(terminal_value(snapshots, "acks", &[]), 1);

            assert!(
                worker.cache.is_committed(&id, partition),
                "a block decided by its own successful result still commits"
            );
            match rx.recv().await.expect("a completion") {
                PipelineCompletionMsg::DeliverAck { .. } => {}
                other => panic!("expected an ack for a durable block, got {other:?}"),
            }
            assert!(worker.is_idle(), "nothing is left holding a slot");
            drop(pdata_tx);
            drop(control_tx);
        })
        .await;
}
