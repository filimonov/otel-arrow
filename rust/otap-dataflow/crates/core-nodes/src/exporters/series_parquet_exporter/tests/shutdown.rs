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
            assert_no_more_completions(&mut rx);
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
            assert_no_more_completions(&mut rx);
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
            let store = fault_store();
            store.hooks().set(Fault::MultipartWedge);
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
                store.hooks().parts.load(SeqCst) > 0
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
                    store.hooks().aborts.load(SeqCst),
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
                    store.hooks().aborts.load(SeqCst) > 0,
                    "the abandoned upload is aborted rather than dropped"
                );

                sim.advance(Duration::from_secs(2));
                (&mut abandoning).await;
            }
            assert!(
                worker.is_idle(),
                "both slot holders are gone once the deadline returns"
            );
            assert_no_more_completions(&mut rx);
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
            notify.force_shutdown(data, 0);
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
            assert_no_more_completions(&mut rx);
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
            let store = fault_store();
            store.hooks().set(Fault::Park);
            let (handler, mut rx) = effects(16);
            let wall = Arc::new(lake::clock::TestWallClock::new(0));
            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let mut worker = Worker::new(cfg, store.clone(), wall.clone(), handler);

            // FLUSHING: one request, rotated into a write that never returns.
            worker.admit(logs_pdata());
            worker.rotate();
            store.hooks().entered.notified().await;
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
            assert_no_more_completions(&mut rx);
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
            let store = fault_store();
            store.hooks().set(Fault::Park);
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
            store.hooks().entered.notified().await;
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
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();

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
                .hooks()
                .writes
                .lock()
                .expect("writes lock")
                .iter()
                .map(|(path, _)| path.clone())
                .filter(|path| path.contains("dataset=values/"))
                .collect();
            assert_eq!(written.len(), 2, "both blocks reached storage: {written:?}");
            drop(control_tx);
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// A node whose first block (requests 1 to 4) is FLUSHING against `store`'s
/// fault and whose ACTIVE block holds request 5, run for `before` and then
/// sent a Shutdown granting `grace`, with the upstream sender dropped so the
/// engine releases it. Returns the node, its completions and the deadline.
async fn shutdown_while_both_blocks_wait(
    store: &Arc<FaultStore>,
    sim: &clock::SimClock,
    before: Duration,
    grace: Duration,
) -> (
    tokio::task::JoinHandle<
        Result<
            otel_arrow_dfe_engine::terminal_state::TerminalState,
            otel_arrow_dfe_engine::error::Error,
        >,
    >,
    PipelineCompletionMsgReceiver<OtapPdata>,
    mpsc::Sender<NodeControlMsg<OtapPdata>>,
    std::time::Instant,
) {
    let (handler, mut rx) = effects(16);
    let (pdata_tx, control_tx, inbox) = inbox(8);
    let node = tokio::task::spawn_local(super::super::run(
        worker_config(),
        store.clone(),
        Arc::new(lake::clock::TestWallClock::new(0)),
        inbox,
        handler,
        None,
    ));
    // Four requests fill a block, so the node rotates it at once.
    for id in 1..=4 {
        pdata_tx
            .send_async(logs_pdata_from(id))
            .await
            .expect("a request of the first block enqueues");
    }
    until("the first block's write reaches the store", || {
        !store
            .hooks()
            .entered_at
            .lock()
            .expect("entered_at lock")
            .is_empty()
    })
    .await;
    pdata_tx
        .send_async(logs_pdata_from(5))
        .await
        .expect("the request of the second block enqueues");
    marker(&pdata_tx, &mut rx, 99).await;
    step_for(sim, before).await;
    let deadline = clock::now() + grace;
    control_tx
        .send_async(NodeControlMsg::Shutdown {
            deadline,
            reason: "terminate".to_owned(),
        })
        .await
        .expect("the shutdown enqueues");
    drop(pdata_tx);
    (node, rx, control_tx, deadline)
}

/// Step the simulated clock until five completions have arrived and the node
/// has returned, or `until` has passed; each completion with the instant it
/// was received.
async fn completions_until(
    sim: &clock::SimClock,
    rx: &mut PipelineCompletionMsgReceiver<OtapPdata>,
    node: &tokio::task::JoinHandle<
        Result<
            otel_arrow_dfe_engine::terminal_state::TerminalState,
            otel_arrow_dfe_engine::error::Error,
        >,
    >,
    until: std::time::Instant,
) -> Vec<(std::time::Instant, PipelineCompletionMsg<OtapPdata>)> {
    let mut got = Vec::new();
    while clock::now() < until && !(got.len() == 5 && node.is_finished()) {
        step_for(sim, Duration::from_millis(100)).await;
        while let Ok(message) = rx.try_recv() {
            got.push((clock::now(), message));
        }
    }
    got
}

/// Scenario: the first block's store fails for 23 s, long enough for its
/// retry backoff to have grown to ten seconds, while a second block waits
/// ACTIVE; a terminate then grants a 60 s grace, and the store heals 32 s into
/// it, before the first block's own retry deadline.
/// Guarantees: once shutdown latches the backoff drops to its minimum, so the
/// first block is retried and written right after the store heals, the
/// ACTIVE block is then sealed and written without waiting for its window,
/// and all five requests are acknowledged before the grace ends.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_heals_inside_the_grace_commits_both_blocks() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(23),
                Duration::from_secs(60),
            )
            .await;
            // A ten-second backoff would next retry at about 52.6 s and then
            // hit the block's own 60 s deadline; this is between the two.
            step_for(&sim, Duration::from_secs(32)).await;
            store.hooks().set(Fault::None);

            let got = completions_until(&sim, &mut rx, &node, deadline).await;
            assert_eq!(
                acked_ids(got),
                vec![1, 2, 3, 4, 5],
                "both blocks are acknowledged"
            );
            assert_no_more_completions(&mut rx);
            assert!(clock::now() < deadline, "the drain ends inside the grace");
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            drop(control_tx);
        })
        .await;
}

/// Scenario: the same two blocks, but the store never heals and the grace,
/// 30 s, ends before the first block's own retry deadline.
/// Guarantees: every request of both blocks is nacked as a retryable
/// `NodeShutdown` at the latched deadline, not before it, and no write reaches
/// the store at or after the deadline.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_never_heals_is_nacked_retryable_at_the_deadline() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(23),
                Duration::from_secs(30),
            )
            .await;

            let got =
                completions_until(&sim, &mut rx, &node, deadline + Duration::from_secs(10)).await;
            assert_eq!(got.len(), 5, "every request is decided");
            for (at, message) in got {
                assert!(at >= deadline, "nacked at the deadline, not before it");
                match message {
                    PipelineCompletionMsg::DeliverNack { nack } => {
                        assert!(!nack.permanent);
                        assert_eq!(nack.cause, NackCause::NodeShutdown);
                    }
                    other => panic!("expected a nack, got {other:?}"),
                }
            }
            let entered = store
                .hooks()
                .entered_at
                .lock()
                .expect("entered_at lock")
                .clone();
            assert!(
                entered.len() > 10,
                "the block was retried until the deadline"
            );
            assert!(
                entered.iter().all(|at| *at < deadline),
                "no attempt starts at or after the deadline"
            );
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_no_more_completions(&mut rx);
            drop(control_tx);
        })
        .await;
}

/// The sorted ids of `got`, all of which must be acks.
fn acked_ids(got: Vec<(std::time::Instant, PipelineCompletionMsg<OtapPdata>)>) -> Vec<usize> {
    let mut ids: Vec<usize> = got
        .into_iter()
        .map(|(_, message)| match message {
            PipelineCompletionMsg::DeliverAck { ack } => ack
                .accepted
                .into_parts()
                .0
                .source_node()
                .expect("a routed request"),
            other => panic!("expected an ack, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Scenario: the first block's store fails fast on every write, a terminate
/// grants 30 s, and the store heals 300 ms before the deadline.
/// Guarantees: attempts keep starting up to the deadline at the minimum
/// backoff, so the attempt started after the heal commits the first block, the
/// ACTIVE block is then sealed and written, and all five requests are
/// acknowledged before the deadline.
#[tokio::test(flavor = "current_thread")]
async fn a_store_that_heals_just_before_the_deadline_still_commits() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::Series);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await;
            step_for(&sim, Duration::from_millis(29_700)).await;
            assert!(clock::now() < deadline);
            store.hooks().set(Fault::None);

            let got = completions_until(&sim, &mut rx, &node, deadline).await;
            assert!(
                got.iter().all(|(at, _)| *at <= deadline),
                "decided by the deadline"
            );
            assert_eq!(acked_ids(got), vec![1, 2, 3, 4, 5]);
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_no_more_completions(&mut rx);
            drop(control_tx);
        })
        .await;
}

/// Scenario: a terminate granting 30 s latches one second into the first
/// block's first attempt, which takes 20 s to fail; the store is healed while
/// that attempt is still running.
/// Guarantees: a slow earlier failure does not stop later attempts: the first
/// block's retry and then the ACTIVE block's first attempt both start before
/// the deadline and commit, so all five requests are acknowledged.
#[tokio::test(flavor = "current_thread")]
async fn a_slow_failure_does_not_stop_the_attempts_after_it() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            store.hooks().set(Fault::SlowFail);
            let (node, mut rx, control_tx, deadline) = shutdown_while_both_blocks_wait(
                &store,
                &sim,
                Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await;
            // The first attempt read its fault on entry, so it still fails.
            step_for(&sim, Duration::from_secs(9)).await;
            store.hooks().set(Fault::None);

            let got = completions_until(&sim, &mut rx, &node, deadline).await;
            assert_eq!(acked_ids(got), vec![1, 2, 3, 4, 5]);
            let entered = store
                .hooks()
                .entered_at
                .lock()
                .expect("entered_at lock")
                .clone();
            assert!(
                entered.iter().skip(1).all(|at| *at < deadline),
                "the later attempts started before the deadline"
            );
            let _ = node
                .await
                .expect("the node task joins")
                .expect("the node succeeds");
            assert_no_more_completions(&mut rx);
            drop(control_tx);
        })
        .await;
}

/// A worker whose one block is FLUSHING in a values multipart upload that
/// wedges, parts and abort alike, with a 10 s `upload.abort_timeout`, a 10 s
/// shutdown deadline latched, and the engine clock then moved 5 s past that
/// deadline before the flush task has run. Returns the worker, its
/// completions, and the absolute cutoff: the deadline plus the abort timeout.
async fn deadline_observed_late(
    sim: &clock::SimClock,
) -> (
    Worker,
    PipelineCompletionMsgReceiver<OtapPdata>,
    std::time::Instant,
) {
    let store = fault_store();
    store.hooks().set(Fault::MultipartWedge);
    let (handler, rx) = effects(8);
    let mut cfg = worker_config();
    cfg.lake.upload.abort_timeout = Duration::from_secs(10);
    cfg.lake.upload.part_bytes = 4096;
    cfg.lake.upload.concurrency = 1;
    cfg.lake.parquet.row_group_bytes = 4096;
    // Small chunks keep the writer blocked on a wedged part, in the phase
    // where a cancelled upload is aborted, when the deadline expires.
    cfg.lake.sorting.merge_chunk_bytes = 4096;
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(cfg, store.clone(), wall, handler);
    worker.admit(bulk_logs_pdata(20_000));
    worker.rotate();
    until("the wedged upload takes a part", || {
        store.hooks().parts.load(SeqCst) > 0
    })
    .await;
    let deadline = clock::now() + Duration::from_secs(10);
    worker.shutdown(deadline);
    sim.advance(Duration::from_secs(15));
    (worker, rx, deadline + Duration::from_secs(10))
}

/// Whether `future` is still pending after the runtime has had several turns.
async fn still_pending<F: Future + Unpin>(future: &mut F) -> bool {
    for _ in 0..32 {
        if futures::poll!(&mut *future).is_ready() {
            return false;
        }
        tokio::task::yield_now().await;
    }
    true
}

/// Scenario: the flush task observes its expiry 5 s after the latched
/// deadline, while the upload it cancels never finishes aborting.
/// Guarantees: the task still releases the write at the latched deadline plus
/// `upload.abort_timeout`, an absolute cutoff, not that long after the moment
/// the expiry was observed; the block is nacked as a retryable shutdown.
#[tokio::test(flavor = "current_thread")]
async fn a_late_expiry_is_cleaned_up_by_the_absolute_cutoff() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (mut worker, mut rx, cutoff) = deadline_observed_late(&sim).await;
            let done = worker
                .flushing
                .as_mut()
                .expect("a rotated block is flushing")
                .finish()
                .await;
            worker.complete(done);
            worker.notify.next().await.expect("the nack is sent");
            expect_shutdown_nack(&mut rx).await;

            let mut job = worker.cleaning.take().expect("the cleanup slot");
            {
                let mut cleanup = std::pin::pin!(job.cleanup());
                sim.advance_to(cutoff - Duration::from_millis(100));
                assert!(
                    still_pending(&mut cleanup).await,
                    "the wedged abort holds the task until the cutoff"
                );
                sim.advance_to(cutoff);
                assert!(
                    !still_pending(&mut cleanup).await,
                    "the task is released at the absolute cutoff"
                );
            }
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the node's deadline branch runs 5 s after the latched deadline,
/// before the flush task has observed its own expiry, and the upload it
/// cancels never finishes aborting.
/// Guarantees: `abandon` decides the block at once and returns by the latched
/// deadline plus `upload.abort_timeout`, not that long after it started, so a
/// late wake cannot push the node's return past the absolute cutoff.
#[tokio::test(flavor = "current_thread")]
async fn a_late_deadline_branch_returns_by_the_absolute_cutoff() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let (mut worker, mut rx, cutoff) = deadline_observed_late(&sim).await;
            {
                let mut abandoning = std::pin::pin!(worker.abandon());
                assert!(still_pending(&mut abandoning).await);
                expect_shutdown_nack(&mut rx).await;
                sim.advance_to(cutoff - Duration::from_millis(100));
                assert!(
                    still_pending(&mut abandoning).await,
                    "the wedged abort holds the node until the cutoff"
                );
                sim.advance_to(cutoff);
                assert!(
                    !still_pending(&mut abandoning).await,
                    "the node returns at the absolute cutoff"
                );
            }
            assert!(worker.is_idle());
            assert_no_more_completions(&mut rx);
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
            let store = fault_store();
            store.hooks().set(Fault::Park);
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
            store.hooks().entered.notified().await;

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
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();

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
            assert_no_more_completions(&mut rx);
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
            let store = fault_store();
            store.hooks().set(Fault::Park);
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
            store.hooks().entered.notified().await;
            assert_eq!(
                store.hooks().parked.load(SeqCst),
                1,
                "the write is parked inside the store"
            );

            node.abort();
            match node.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("start was not aborted"),
            }
            until("the cancelled node releases the store", || {
                Arc::strong_count(&store) == 1 && store.hooks().parked.load(SeqCst) == 0
            })
            .await;
            assert_eq!(
                store.hooks().parked_drops.load(SeqCst),
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
            let mut prime = Notifier::new(handler.clone(), 1);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let mut cfg = worker_config();
            cfg.lake.upload.abort_timeout = Duration::from_millis(100);
            let (context, _registry) = otel_arrow_dfe_engine::testing::test_pipeline_ctx();
            let metrics = super::super::metrics::Metrics::register(&context, &cfg.lake);
            let store = fault_store();
            store.hooks().set(Fault::Park);

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

/// A worker whose one request (id 1) is FLUSHING in a parked write, with
/// shutdown latched and three requests (ids 2 to 4) force-drained into a
/// completion channel that has room for one message and is never read.
///
/// One request per block makes the notifier's capacity two, so the flushing
/// block's completion and two force-drained refusals are more than it holds.
async fn force_drained_past_a_held_block(
    store: &Arc<FaultStore>,
) -> (Worker, PipelineCompletionMsgReceiver<OtapPdata>) {
    store.hooks().set(Fault::Park);
    let (handler, rx) = effects(1);
    let mut cfg = worker_config_with_requests(1);
    cfg.lake.upload.abort_timeout = Duration::from_millis(100);
    let wall = Arc::new(lake::clock::TestWallClock::new(0));
    let mut worker = Worker::new(cfg, store.clone(), wall, handler);
    worker.admit(logs_pdata_from(1));
    worker.rotate();
    store.hooks().entered.notified().await;
    worker.shutdown(clock::now() + Duration::from_secs(30));
    for id in 2..=4 {
        worker.force_shutdown(logs_pdata_from(id));
    }
    (worker, rx)
}

/// Scenario: one request is FLUSHING in a parked write, three requests are
/// force-drained after shutdown while the completion channel has room for
/// one and nobody reads it, and the write is then released.
/// Guarantees: the refusal that does not fit beside the held block is
/// attempted once and counted instead of queued, so the flushing block's ack
/// still has a slot: no panic, the block is acknowledged, and each of the four
/// requests is decided exactly once.
#[tokio::test(flavor = "current_thread")]
async fn force_drain_leaves_credit_for_a_block_whose_write_is_released() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let store = fault_store();
            let (mut worker, mut rx) = force_drained_past_a_held_block(&store).await;
            assert_eq!(
                worker.notify.failures(),
                1,
                "the refusal that does not fit is attempted once and counted"
            );

            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();
            let done = worker
                .flushing
                .as_mut()
                .expect("the block is flushing")
                .finish()
                .await;
            worker.complete(done);
            drain_cleanup(&mut worker).await;

            // The channel took refusal 2; refusal 3 waits in the send slot and
            // the ack of request 1 behind it.
            expect_shutdown_nack(&mut rx).await;
            worker.notify.next().await.expect("refusal 3 is sent");
            expect_shutdown_nack(&mut rx).await;
            worker.notify.next().await.expect("the ack is sent");
            assert_eq!(expect_ack(&mut rx).await, Some(1));
            let outcomes = worker.notify.outcomes();
            assert_eq!(outcomes[Outcome::Shutdown as usize], 3);
            assert_eq!(outcomes[Outcome::Ack as usize], 1);
            assert_eq!(outcomes.iter().sum::<u64>(), 4);
            assert!(worker.is_idle());
            assert_no_more_completions(&mut rx);
        })
        .await;
}

/// Scenario: the same held block and force-drained requests, but the
/// shutdown deadline fires while the write is still parked.
/// Guarantees: the deadline decides the flushing block without a panic,
/// every completion the channel cannot take is counted as a delivery failure,
/// and each of the four requests is decided exactly once.
#[tokio::test(flavor = "current_thread")]
async fn force_drain_leaves_credit_for_a_block_the_deadline_decides() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let sim = clock::SimClock::new();
            let _clock_guard = sim.install();
            let store = fault_store();
            let (mut worker, mut rx) = force_drained_past_a_held_block(&store).await;

            let _ticker = ticking(&sim, Duration::from_millis(50));
            worker.abandon().await;

            expect_shutdown_nack(&mut rx).await;
            let outcomes = worker.notify.outcomes();
            assert_eq!(outcomes[Outcome::Shutdown as usize], 4);
            assert_eq!(outcomes.iter().sum::<u64>(), 4);
            assert_eq!(
                worker.notify.failures(),
                3,
                "one delivered, three counted: every decision is accounted for"
            );
            assert!(worker.is_idle());
            assert_no_more_completions(&mut rx);
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
            let mut prime = Notifier::new(handler.clone(), 1);
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            prime.push(token, Outcome::Ack);
            prime.next().await.expect("the completion channel fills");

            let store = fault_store();
            store.hooks().set(Fault::Park);
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
            store.hooks().entered.notified().await;

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
            assert_no_more_completions(&mut rx);
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
            let store = fault_store();
            store.hooks().set(Fault::Park);
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
            store.hooks().entered.notified().await;
            store.hooks().set(Fault::None);
            store.hooks().release.notify_waiters();
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
            assert_no_more_completions(&mut rx);
        })
        .await;
}
