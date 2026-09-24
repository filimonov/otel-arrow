// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Payload-free completion ownership and bounded asynchronous delivery.
//!
//! [`AckToken`] is what the exporter keeps for an admitted request: its routing
//! frames and signal type. [`Notifier`] delivers decided completions over the
//! bounded engine channel and keeps its one send future across polls, so a
//! cancelled `select` branch never drops a token.

use super::outcome::Outcome;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::control::{AckMsg, NackMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, clock};
use otel_arrow_dfe_otap::metrics::ExporterExportMetrics;
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::OtapPayload;
use otel_arrow_dfe_telemetry::common_attributes::{
    Outcome as ExportOutcome, SignalOutcomeAttributes,
};
use otel_arrow_dfe_telemetry::metrics::MeasurementMetricSet;
use std::collections::VecDeque;
use std::future::{Future, pending};
use std::pin::Pin;
use std::rc::Rc;
use std::time::Instant;

/// The completion a request is still owed, without its payload.
pub(super) struct AckToken {
    /// Routing frames the ack or nack unwinds along.
    context: Context,
    /// Signal type of the request, restored on the returned empty payload.
    signal: SignalType,
    /// When the exporter took ownership of the completion.
    received: Instant,
    /// Fails a test that drops the token without deciding it.
    #[cfg(test)]
    bomb: DropBomb,
}

/// Panics when dropped, unless the token carrying it is decided or its owner
/// is torn down with it.
#[cfg(test)]
struct DropBomb;

#[cfg(test)]
impl Drop for DropBomb {
    fn drop(&mut self) {
        assert!(
            std::thread::panicking(),
            "an AckToken was dropped without a decision"
        );
    }
}

impl AckToken {
    /// Split a request into the completion it owes and its payload.
    ///
    /// The transport headers and the authorization claims derived from them
    /// are dropped here, not held for as long as the exporter holds the token.
    pub(super) fn split(data: OtapPdata) -> (Self, OtapPayload) {
        let (mut context, payload) = data.into_parts();
        let _ = context.take_transport_headers();
        let _ = context.take_authorized_identity();
        let signal = payload.signal_type();
        (
            Self {
                context,
                signal,
                received: clock::now(),
                #[cfg(test)]
                bomb: DropBomb,
            },
            payload,
        )
    }

    /// Release the token undecided, because the worker or notifier holding
    /// it is being torn down by a test.
    #[cfg(test)]
    pub(super) fn discard(self) {
        std::mem::forget(self.bomb);
    }

    /// The node the request was routed from, which a test identifies it by.
    #[cfg(test)]
    pub(super) fn source(&self) -> Option<usize> {
        self.context.source_node()
    }

    /// Total bytes this token keeps resident, inline storage included.
    pub(super) fn bytes(&self) -> usize {
        size_of::<Self>() + self.external_bytes()
    }

    /// When the exporter took ownership of this completion.
    pub(super) fn received(&self) -> Instant {
        self.received
    }

    /// Signal type of the request.
    pub(super) fn signal(&self) -> SignalType {
        self.signal
    }

    /// Bytes owned outside the token's own inline storage.
    ///
    /// Charged separately because the inline part is already accounted for by
    /// whichever container the token currently sits in.
    pub(super) fn external_bytes(&self) -> usize {
        self.context.retained_frame_bytes()
    }

    /// Rebuild the pdata the completion is delivered on, with an empty payload.
    fn pdata(self) -> OtapPdata {
        let Self {
            context,
            signal,
            #[cfg(test)]
            bomb,
            ..
        } = self;
        #[cfg(test)]
        std::mem::forget(bomb);
        OtapPdata::new(context, OtapPayload::empty(signal))
    }
}

/// A completion send that has been started but has not resolved.
type SendFuture = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

/// One decided completion waiting for the send slot: the token, its outcome
/// and, when the decision has something more specific to say than
/// [`Outcome::sentence`], the reason sentence the sender is told.
///
/// The sentence is shared, because a failed block decides every one of its
/// requests with the same one.
type Queued = (AckToken, Outcome, Option<Rc<str>>);

/// The single in-flight completion send, kept across polls.
struct Sending {
    /// The started send; never recreated while it is pending.
    future: SendFuture,
    /// Bytes charged to this send: the token's external buffers plus the
    /// future's own storage, which now owns the token inline.
    bytes: usize,
    /// When the exporter took ownership of the completion being sent.
    received: Instant,
}

/// Bounded queue of completions still to be delivered.
pub(super) struct Notifier {
    /// Handle the completions are routed through.
    effects: EffectHandler<OtapPdata>,
    /// Completions waiting for the send slot.
    queue: VecDeque<Queued>,
    /// The one completion currently being sent, if any.
    sending: Option<Sending>,
    /// Maximum number of live completions: queued, sending, and held by a
    /// block or the parking slot until they are pushed here.
    capacity: usize,
    /// Count of completions pushed, per [`Outcome`].
    outcomes: [u64; Outcome::ALL.len()],
    /// Completions the engine would not accept.
    failures: u64,
    /// Largest single token observed, for capacity reporting.
    token_high_water: usize,
    /// The shared `exporter.exports` outcome set, when the node registered
    /// one: every decision is recorded here once, at the moment it is taken.
    pub(super) exports: Option<MeasurementMetricSet<ExporterExportMetrics>>,
}

impl Notifier {
    /// Create a notifier that may hold `capacity` live completions.
    ///
    /// The queue grows with the completions it holds and is not reserved at
    /// `capacity`, which is derived from an unbounded configuration value.
    pub(super) fn new(effects: EffectHandler<OtapPdata>, capacity: usize) -> Self {
        Self {
            effects,
            queue: VecDeque::new(),
            sending: None,
            capacity,
            outcomes: [0; Outcome::ALL.len()],
            failures: 0,
            token_high_water: 0,
            exports: None,
        }
    }

    /// Count one decision, once, when it is taken.
    ///
    /// The shared export set records it by signal and by the engine-wide
    /// outcome class (`success` for an ack, `refused` for a rule the request
    /// broke, `failure` for everything the sender may retry), with the time
    /// from receipt to decision.
    fn decided(&mut self, token: &AckToken, outcome: Outcome) {
        self.token_high_water = self.token_high_water.max(token.bytes());
        self.outcomes[outcome as usize] += 1;
        if let Some(exports) = &mut self.exports {
            let class = match outcome {
                Outcome::Ack => ExportOutcome::Success,
                refused if refused.refused() => ExportOutcome::Refused,
                _ => ExportOutcome::Failure,
            };
            exports
                .with(SignalOutcomeAttributes {
                    signal: token.signal,
                    outcome: class,
                })
                .record(clock::now().saturating_duration_since(token.received));
        }
    }

    /// Live completions, queued plus the one being sent.
    pub(super) fn len(&self) -> usize {
        self.queue.len() + usize::from(self.sending.is_some())
    }

    /// Whether no completion is outstanding.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes the notifier keeps resident.
    ///
    /// Each token is charged once: a queued token by the queue cell it sits in
    /// plus its external buffers, and the sending token by the send future's
    /// storage, which owns it inline, plus the same external buffers.
    pub(super) fn bytes(&self) -> usize {
        self.queue
            .iter()
            .map(|(token, _, _)| token.external_bytes())
            .sum::<usize>()
            + self.sending.as_ref().map_or(0, |sending| sending.bytes)
            + self.queue.capacity() * size_of::<Queued>()
    }

    /// When the oldest outstanding completion was taken ownership of.
    pub(super) fn oldest(&self) -> Option<Instant> {
        self.queue
            .iter()
            .map(|(token, _, _)| token.received)
            .chain(self.sending.iter().map(|sending| sending.received))
            .min()
    }

    /// Counters of pushed completions, indexed by [`Outcome`].
    pub(super) fn outcomes(&self) -> &[u64; Outcome::ALL.len()] {
        &self.outcomes
    }

    /// Completions the engine would not accept.
    pub(super) fn failures(&self) -> u64 {
        self.failures
    }

    /// Largest single token the notifier has held.
    pub(super) fn token_high_water(&self) -> usize {
        self.token_high_water
    }

    /// Whether one more request may be admitted, given the number of
    /// completions the caller already owes.
    ///
    /// `live` counts every completion the worker owes wherever it sits: here,
    /// or held by a block or the parking slot until it is pushed. Admission
    /// stops one short of `capacity`, so a saturated worker still has a slot
    /// for the first request [`Notifier::force_shutdown`] refuses.
    pub(super) fn has_credit(&self, live: usize) -> bool {
        live + 1 < self.capacity
    }

    /// Queue one decided request.
    ///
    /// The worker owes at most `capacity` completions, counted while a block
    /// or the parking slot still held this one, so moving it here keeps the
    /// queue within its bound. A push past the bound is attempted once
    /// instead, exactly as [`Notifier::force_shutdown`] treats a refusal that
    /// does not fit.
    pub(super) fn push(&mut self, token: AckToken, outcome: Outcome) {
        self.push_with(token, outcome, None);
    }

    /// Queue one decided request with the reason sentence its sender is told.
    ///
    /// `None` falls back to [`Outcome::sentence`]. The bound is the one
    /// [`Notifier::push`] documents.
    pub(super) fn push_with(&mut self, token: AckToken, outcome: Outcome, reason: Option<Rc<str>>) {
        self.decided(&token, outcome);
        if self.len() < self.capacity {
            self.queue.push_back((token, outcome, reason));
        } else {
            self.deliver_now(token, outcome, reason);
        }
    }

    /// The engine call one decided completion is delivered by.
    ///
    /// Every delivery path goes through this, so a queued completion, a
    /// completion abandoned at the shutdown deadline and a force-drained
    /// refusal are reported identically.
    ///
    /// The nack reason is a sentence, the decision's own or the outcome's
    /// default, because it is the status message the producer sees; the
    /// outcome's machine token stays the metric label.
    async fn delivery(
        effects: EffectHandler<OtapPdata>,
        token: AckToken,
        outcome: Outcome,
        reason: Option<Rc<str>>,
    ) -> Result<(), Error> {
        let data = token.pdata();
        let Some(cause) = outcome.nack_cause() else {
            return effects.notify_ack(AckMsg::new(data)).await;
        };
        let sentence = reason.as_deref().unwrap_or(outcome.sentence()).to_owned();
        let nack = if outcome.refused() {
            NackMsg::new_permanent_with_cause(sentence, data, cause)
        } else {
            NackMsg::new_with_cause(sentence, data, cause)
        };
        effects.notify_nack(nack).await
    }

    /// Drive the single completion send to completion.
    ///
    /// Cancellation safe: the send future is installed before it is polled and
    /// is left installed if the poll is dropped, so a cancelled `select` branch
    /// never loses the token it owns. With nothing outstanding this never
    /// resolves, which lets the caller use it as an idle `select` branch.
    pub(super) async fn next(&mut self) -> Result<(), Error> {
        if self.sending.is_none() {
            let Some((token, outcome, reason)) = self.queue.pop_front() else {
                return pending().await;
            };
            self.install(token, outcome, reason);
        }
        let result = self
            .sending
            .as_mut()
            .expect("send was installed")
            .future
            .as_mut()
            .await;
        self.sending = None;
        if result.is_err() {
            self.failures += 1;
        }
        result
    }

    /// Start the send of one completion in the single send slot.
    ///
    /// The future is outside tokio's cooperative budget, which reports
    /// `Pending` after 128 operations in one task poll whatever room the
    /// channel has: the force-drain refusal and the deadline drain poll a send
    /// once, so the budget would cap how many requests they decide per poll.
    fn install(&mut self, token: AckToken, outcome: Outcome, reason: Option<Rc<str>>) {
        let external = token.external_bytes();
        let received = token.received;
        let future = tokio::task::coop::unconstrained(Self::delivery(
            self.effects.clone(),
            token,
            outcome,
            reason,
        ));
        let bytes = external + size_of_val(&future);
        self.sending = Some(Sending {
            bytes,
            received,
            future: Box::pin(future),
        });
    }

    /// Refuse one force-drained request with a retryable `NodeShutdown` nack.
    ///
    /// Called after shutdown is latched, possibly many times within one poll.
    /// `held` counts the completions the caller still owes outside the
    /// notifier. The refusal takes a slot only while it and every held
    /// completion fit in `capacity`: its send starts at once when the send
    /// slot is free and nothing is queued, and otherwise it is queued. A
    /// refusal that does not fit is attempted once and, if the channel is
    /// full, counted as a delivery failure, so force-drain never takes a held
    /// block's credit, never grows without bound and never stalls.
    pub(super) fn force_shutdown(&mut self, data: OtapPdata, held: usize) {
        use futures::FutureExt;

        let (token, payload) = AckToken::split(data);
        drop(payload);
        self.decided(&token, Outcome::Shutdown);
        if self.len() + held >= self.capacity {
            self.deliver_now(token, Outcome::Shutdown, None);
            return;
        }
        if self.sending.is_none() && self.queue.is_empty() {
            self.install(token, Outcome::Shutdown, None);
            let sending = self.sending.as_mut().expect("send was installed");
            if let Some(result) = sending.future.as_mut().now_or_never() {
                self.sending = None;
                if result.is_err() {
                    self.failures += 1;
                }
            }
            return;
        }
        self.queue.push_back((token, Outcome::Shutdown, None));
    }

    /// Attempt one completion immediately, counting a send that would block.
    ///
    /// Used only on the paths that must not park a token: a completion past
    /// the queue bound and the completions abandoned once the shutdown
    /// deadline has elapsed. The token is released either way, so the
    /// request ends decided or counted as a delivery failure, never silently
    /// dropped. The attempt is outside the cooperative budget, so a full
    /// completion channel is the only reason it can fail to be taken.
    fn deliver_now(&mut self, token: AckToken, outcome: Outcome, reason: Option<Rc<str>>) {
        use futures::FutureExt;

        let delivered = tokio::task::coop::unconstrained(Self::delivery(
            self.effects.clone(),
            token,
            outcome,
            reason,
        ))
        .now_or_never();
        if !matches!(delivered, Some(Ok(()))) {
            self.failures += 1;
        }
    }

    /// Attempt every outstanding completion once, without blocking.
    ///
    /// Called when the shutdown deadline has elapsed and the node is about to
    /// return. Whatever the engine cannot take immediately is counted as a
    /// delivery failure and released, so the node leaves nothing undecided and
    /// still returns within its deadline. Every attempt is outside the
    /// cooperative budget, so a completion channel with room takes them all.
    pub(super) fn drain_now(&mut self) {
        use futures::FutureExt;

        if let Some(mut sending) = self.sending.take()
            && !matches!(
                tokio::task::coop::unconstrained(sending.future.as_mut()).now_or_never(),
                Some(Ok(()))
            )
        {
            self.failures += 1;
        }
        while let Some((token, outcome, reason)) = self.queue.pop_front() {
            self.deliver_now(token, outcome, reason);
        }
    }
}

/// A test that drops a notifier still holding queued completions tears them
/// down with it undecided.
#[cfg(test)]
impl Drop for Notifier {
    fn drop(&mut self) {
        for (token, _, _) in self.queue.drain(..) {
            token.discard();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{assert_no_more_completions, effects, empty_pdata};
    use super::*;
    use otel_arrow_dfe_engine::control::{NackCause, PipelineCompletionMsg};
    use std::time::Duration;

    /// Scenario: a request carrying transport headers is split (claims cannot be planted from
    /// outside the otap crate).
    /// Guarantees: the completion keeps the routing frames but not the transport headers.
    #[test]
    fn split_drops_transport_headers() {
        use otel_arrow_dfe_config::context::ContextEntryName;
        use otel_arrow_dfe_config::transport_headers::{
            TransportHeader, TransportHeaders, ValueKind,
        };

        let mut context = Context::default();
        context.set_source_node(7);
        let mut headers = TransportHeaders::new();
        headers.push(TransportHeader::new(
            ContextEntryName::try_from("authorization").expect("a valid name"),
            ValueKind::Text,
            b"Bearer secret".to_vec(),
        ));
        context.set_transport_headers(headers);
        let data = OtapPdata::new(context, OtapPayload::empty(SignalType::Logs));
        assert!(data.transport_headers().is_some());

        let (token, _payload) = AckToken::split(data);
        assert!(token.context.transport_headers().is_none());
        assert_eq!(token.signal(), SignalType::Logs);
        token.discard();
    }

    /// Scenario: a token moves from a reserved queue cell into a blocked send future.
    /// Guarantees: queue storage, external buffers and future storage are each charged once.
    #[tokio::test(flavor = "current_thread")]
    async fn notifier_bytes_do_not_double_count_inline_tokens() {
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 2);

        let (first, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(first, Outcome::Ack);
        notify.next().await.expect("fill completion channel");

        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let external = token.external_bytes();
        notify.push(token, Outcome::Ack);
        let queue = notify.queue.capacity() * size_of::<Queued>();
        assert_eq!(notify.bytes(), queue + external);

        assert!(
            tokio::time::timeout(Duration::from_millis(1), notify.next())
                .await
                .is_err()
        );
        let send = notify.sending.as_ref().expect("blocked send");
        let future = size_of_val(send.future.as_ref().get_ref());
        assert_eq!(notify.bytes(), queue + external + future);
    }

    /// Scenario: each outcome class is delivered through the notifier.
    /// Guarantees: ack as ack, refusal as permanent `Refused` nack, storage failure as retryable
    /// nack.
    #[tokio::test(flavor = "current_thread")]
    async fn each_outcome_is_delivered_as_its_completion() {
        let (handler, mut rx) = effects(4);
        let mut notify = Notifier::new(handler, 2);

        for outcome in [Outcome::Ack, Outcome::RowTooLarge, Outcome::Storage] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
            notify.next().await.expect("completion is accepted");
        }

        assert!(matches!(
            rx.recv().await.expect("ack"),
            PipelineCompletionMsg::DeliverAck { .. }
        ));
        match rx.recv().await.expect("refusal") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(nack.permanent);
                assert_eq!(nack.cause, NackCause::Refused);
                assert_eq!(nack.reason, Outcome::RowTooLarge.sentence());
            }
            other => panic!("expected a nack, got {other:?}"),
        }
        match rx.recv().await.expect("storage failure") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::Unspecified);
                assert_eq!(nack.reason, Outcome::Storage.sentence());
            }
            other => panic!("expected a nack, got {other:?}"),
        }
        assert_eq!(notify.outcomes()[Outcome::Ack as usize], 1);
        assert_eq!(notify.outcomes()[Outcome::RowTooLarge as usize], 1);
        assert_eq!(notify.outcomes()[Outcome::Storage as usize], 1);
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());
        assert!(notify.token_high_water() > 0);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: three force-drained refusals against a channel with room for one and a notifier
    /// bound of one.
    /// Guarantees: the first is delivered, the second waits in the send slot, only the third is
    /// counted as a delivery failure.
    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_refusals_wait_in_the_bound_then_fail() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 1);

        notify.force_shutdown(empty_pdata(), 0);
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());

        // The channel now holds the first refusal, so the second cannot be
        // handed over without blocking: it keeps the send slot.
        notify.force_shutdown(empty_pdata(), 0);
        assert_eq!(notify.failures(), 0);
        assert_eq!(notify.len(), 1);

        // The notifier is at its bound of one, so the third is attempted
        // once, finds the channel full and is counted.
        notify.force_shutdown(empty_pdata(), 0);
        assert_eq!(notify.failures(), 1);
        assert_eq!(notify.len(), 1);
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 3);

        for _ in 0..2 {
            match rx.recv().await.expect("shutdown refusal") {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert!(!nack.permanent);
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            if !notify.is_empty() {
                notify.next().await.expect("the waiting refusal is sent");
            }
        }
        assert!(notify.is_empty());
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: 300 completions, more than tokio's cooperative budget, drained at the deadline
    /// into a channel with room.
    /// Guarantees: all are delivered and none is counted as a failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_deadline_drain_delivers_more_than_the_coop_budget() {
        const N: usize = 300;
        let (handler, mut rx) = effects(N);
        let mut notify = Notifier::new(handler, N + 1);
        for _ in 0..N {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, Outcome::Shutdown);
        }
        notify.drain_now();
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());
        let mut delivered = 0;
        while let Ok(message) = rx.try_recv() {
            assert!(matches!(message, PipelineCompletionMsg::DeliverNack { .. }));
            delivered += 1;
        }
        assert_eq!(delivered, N);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: 300 force-drained requests within one task poll, with room in the channel.
    /// Guarantees: each gets a delivered retryable `NodeShutdown` nack.
    #[tokio::test(flavor = "current_thread")]
    async fn force_drained_refusals_beyond_the_coop_budget_are_all_delivered() {
        const N: usize = 300;
        let (handler, mut rx) = effects(N);
        let mut notify = Notifier::new(handler, 8);
        for _ in 0..N {
            notify.force_shutdown(empty_pdata(), 0);
        }
        assert_eq!(notify.failures(), 0);
        let mut delivered = 0;
        while let Ok(message) = rx.try_recv() {
            match message {
                PipelineCompletionMsg::DeliverNack { nack } => {
                    assert_eq!(nack.cause, NackCause::NodeShutdown);
                }
                other => panic!("expected a nack, got {other:?}"),
            }
            delivered += 1;
        }
        assert_eq!(delivered + notify.len(), N);
        assert_eq!(delivered, N);
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a notifier with a capacity no queue could hold.
    /// Guarantees: creation allocates nothing and the queue grows only with what it holds.
    #[tokio::test(flavor = "current_thread")]
    async fn a_huge_capacity_is_not_preallocated() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, usize::MAX / 2);
        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Ack);
        notify.next().await.expect("the completion is accepted");
        assert!(matches!(
            rx.recv().await.expect("an ack"),
            PipelineCompletionMsg::DeliverAck { .. }
        ));
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: a blocked send and a later completion queued behind it.
    /// Guarantees: the oldest outstanding completion is the blocked send.
    #[tokio::test(flavor = "current_thread")]
    async fn oldest_is_the_blocked_send_not_the_queued_completion() {
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);
        assert_eq!(notify.oldest(), None);

        let (first, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let delivered_received = first.received;
        notify.push(first, Outcome::Ack);
        notify.next().await.expect("fill completion channel");
        assert_eq!(notify.oldest(), None);

        let (blocked, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let blocked_received = blocked.received;
        notify.push(blocked, Outcome::Ack);

        // The two remaining tokens must carry distinct timestamps for the
        // choice between them to mean anything, and the system clock is what
        // `AckToken::split` reads.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let (queued, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let queued_received = queued.received;
        notify.push(queued, Outcome::Ack);
        assert!(blocked_received < queued_received);
        assert!(delivered_received < blocked_received);

        assert!(
            tokio::time::timeout(Duration::from_millis(1), notify.next())
                .await
                .is_err()
        );
        assert!(notify.sending.is_some());
        assert_eq!(notify.len(), 2);
        assert_eq!(notify.oldest(), Some(blocked_received));
    }

    /// Scenario: normal completions up to one short of capacity, then two force-drained refusals.
    /// Guarantees: the last slot takes the first refusal; the second is attempted once, not queued.
    #[tokio::test(flavor = "current_thread")]
    async fn the_last_completion_slot_is_left_for_a_forced_refusal() {
        // Capacity 4 stands for 2N with N = 2; the cap on normal live
        // completions is therefore 3.
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);

        for _ in 0..3 {
            assert!(notify.has_credit(notify.len()));
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, Outcome::Ack);
        }
        assert_eq!(notify.len(), 3);
        assert!(!notify.has_credit(notify.len()));

        notify.force_shutdown(empty_pdata(), 0);
        assert_eq!(notify.len(), 4, "the last slot takes the refusal");
        notify.force_shutdown(empty_pdata(), 0);
        assert_eq!(notify.len(), 4, "a refusal past the bound is not queued");
        assert_eq!(notify.failures(), 0, "the channel had room for it");
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 2);
        assert!(matches!(
            rx.try_recv(),
            Ok(PipelineCompletionMsg::DeliverNack { .. })
        ));
        assert_no_more_completions(&mut rx);
    }

    /// Scenario: pushes past the notifier's bound, with the channel open and then full.
    /// Guarantees: no panic and no queue growth: delivered at once or counted as a delivery
    /// failure.
    #[tokio::test(flavor = "current_thread")]
    async fn a_push_past_the_bound_is_delivered_at_once_not_asserted() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 1);
        for outcome in [Outcome::Ack, Outcome::Shutdown, Outcome::Storage] {
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, outcome);
        }
        assert_eq!(notify.len(), 1, "only the first push is queued");
        assert_eq!(notify.failures(), 1, "the third found the channel full");
        assert_eq!(notify.outcomes().iter().sum::<u64>(), 3);
        assert!(matches!(
            rx.try_recv(),
            Ok(PipelineCompletionMsg::DeliverNack { .. })
        ));
        assert_no_more_completions(&mut rx);
    }
}
