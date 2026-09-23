// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Payload-free completion ownership and bounded asynchronous delivery.
//!
//! A request that has been admitted to a block is no longer represented by its
//! payload: the rows live in the block and the only thing the exporter still
//! owes the sender is a decision. [`AckToken`] is what the exporter keeps in
//! the meantime. It holds the routing frames and the signal type, and nothing
//! else -- the payload is handed back to the caller of [`AckToken::split`] and
//! the inbound credentials and claims are dropped there.
//!
//! [`Notifier`] owns the delivery side. The engine completion channel is
//! bounded, so a send can block; the future that performs it is stored across
//! polls rather than recreated, which is what makes a cancelled `select`
//! branch safe. Dropping a poll never drops a token, so no request can lose
//! its decision because the exporter was busy elsewhere.

use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::exporter::EffectHandler;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, clock};
use otel_arrow_dfe_otap::pdata::{Context, OtapPdata};
use otel_arrow_dfe_pdata::OtapPayload;
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
}

impl AckToken {
    /// Split a request into the completion it owes and its payload.
    ///
    /// The transport headers and the authorization claims derived from them
    /// are dropped here rather than staying resident for as long as the
    /// exporter holds the token (spec section 7).
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
            },
            payload,
        )
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
        OtapPdata::new(self.context, OtapPayload::empty(self.signal))
    }
}

/// How a request was decided, in the form the sender is told about it.
///
/// The three refusals are separate variants rather than one, so the counters
/// keep saying which validation rule rejected a request after the error value
/// itself has been dropped with the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(super) enum Outcome {
    /// The request is durable.
    Ack,
    /// The request exceeded a size budget.
    TooLarge,
    /// The request's content could not be used.
    Invalid,
    /// The request's signal is not handled by this exporter.
    Unsupported,
    /// Writing the request failed; the sender may retry.
    Storage,
    /// The node shut down before the request could be decided.
    Shutdown,
    /// A writer invariant failed while handling the request; the request is
    /// not at fault and the sender may retry.
    Internal,
}

/// Number of [`Outcome`] variants, and so the width of the counter array.
pub(super) const OUTCOMES: usize = 7;

/// Longest detail an error may contribute to a nack reason, in bytes.
///
/// The reason is the status message a producer sees and logs, so it carries
/// enough of the underlying error to act on but never an unbounded amount of
/// request-derived text.
const MAX_DETAIL_BYTES: usize = 256;

/// A bounded, printable rendering of an error for a nack reason.
///
/// Control characters, including line breaks, become spaces so the reason
/// stays one line, and the text is cut at a character boundary once it passes
/// [`MAX_DETAIL_BYTES`].
pub(super) fn sanitized(detail: &str) -> String {
    let mut out = String::with_capacity(detail.len().min(MAX_DETAIL_BYTES + 3));
    for c in detail.chars() {
        if out.len() + c.len_utf8() > MAX_DETAIL_BYTES {
            out.push_str("...");
            break;
        }
        out.push(if c.is_control() { ' ' } else { c });
    }
    out
}

impl Outcome {
    /// Stable machine token of the outcome: the metric label and log field.
    ///
    /// Never the reason a producer is told; that is a sentence, see
    /// [`Outcome::sentence`].
    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::TooLarge => "too_large",
            Self::Invalid => "invalid",
            Self::Unsupported => "unsupported",
            Self::Storage => "storage",
            Self::Shutdown => "shutdown",
            Self::Internal => "internal",
        }
    }

    /// The reason sentence a completion carries when its decision supplied
    /// none of its own: what happened and what the sender should do.
    pub(super) fn sentence(self) -> &'static str {
        match self {
            Self::Ack => "stored",
            Self::TooLarge => {
                "the request exceeds a series_parquet size budget; split the batch upstream"
            }
            Self::Invalid => "the request content is invalid; fix the producer",
            Self::Unsupported => {
                "the request carries data series_parquet does not store; route it elsewhere"
            }
            Self::Storage => {
                "series_parquet could not write the block holding this request to object \
                 storage; retry the request"
            }
            Self::Shutdown => {
                "series_parquet shut down before the request was stored; retry the request"
            }
            Self::Internal => {
                "series_parquet hit an internal error handling the request; retry the request"
            }
        }
    }

    /// Whether the sender must change the request before retrying it.
    pub(super) fn refused(self) -> bool {
        matches!(self, Self::TooLarge | Self::Invalid | Self::Unsupported)
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
    /// Maximum number of live completions, queued plus sending.
    capacity: usize,
    /// Count of completions pushed, per [`Outcome`].
    outcomes: [u64; OUTCOMES],
    /// Completions the engine would not accept.
    failures: u64,
    /// Largest single token observed, for capacity reporting.
    token_high_water: usize,
}

impl Notifier {
    /// Create a notifier that may hold `capacity` live completions.
    ///
    /// The queue grows with the completions it actually holds rather than
    /// being reserved at `capacity`, which is derived from an unbounded
    /// configuration value.
    pub(super) fn new(effects: EffectHandler<OtapPdata>, capacity: usize) -> Self {
        Self {
            effects,
            queue: VecDeque::new(),
            sending: None,
            capacity,
            outcomes: [0; OUTCOMES],
            failures: 0,
            token_high_water: 0,
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

    // The counters and size reporting below are read by the node's metrics.
    // They are built and tested here because the accounting rules they depend
    // on -- one charge per token, a send future that survives a cancelled poll
    // -- belong to this module.
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
    pub(super) fn outcomes(&self) -> &[u64; OUTCOMES] {
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

    /// Whether one more normal completion may be queued, given the number of
    /// completions the caller already owes.
    ///
    /// `live` counts every token that will eventually reach this queue, not
    /// only the ones already in it: a token held by a block is a completion
    /// the notifier has yet to be handed. False once the only slot left is the
    /// last one, so the worker stops admitting rather than spending the slot
    /// [`Notifier::force_shutdown`] queues a refusal in when the completion
    /// channel is full.
    pub(super) fn has_credit(&self, live: usize) -> bool {
        live + 1 < self.capacity
    }

    /// Queue one decided request.
    ///
    /// The caller reserves the credit before it admits the request, so a push
    /// that would exceed the bound is a worker bug rather than a runtime
    /// condition. The bound is checked after the insertion, not before it: a
    /// normal outcome may take the notifier up to `capacity - 1` live
    /// completions, and the last slot is kept for a shutdown outcome: the
    /// shutdown decision of a completion the worker already holds, or a
    /// force-drained refusal queued by [`Notifier::force_shutdown`].
    pub(super) fn push(&mut self, token: AckToken, outcome: Outcome) {
        self.push_with(token, outcome, None);
    }

    /// Queue one decided request with the reason sentence its sender is told.
    ///
    /// `None` falls back to [`Outcome::sentence`]. The credit rule is the one
    /// [`Notifier::push`] documents.
    pub(super) fn push_with(&mut self, token: AckToken, outcome: Outcome, reason: Option<Rc<str>>) {
        let reserved = usize::from(outcome != Outcome::Shutdown);
        assert!(
            self.len() + 1 + reserved <= self.capacity,
            "worker must reserve completion credit"
        );
        self.token_high_water = self.token_high_water.max(token.bytes());
        self.outcomes[outcome as usize] += 1;
        self.queue.push_back((token, outcome, reason));
    }

    /// The engine call one decided completion is delivered by.
    ///
    /// Every delivery path goes through this, so a queued completion, a
    /// completion abandoned at the shutdown deadline and a force-drained
    /// refusal are reported identically.
    ///
    /// The nack reason is a sentence -- the decision's own when it supplied
    /// one, the outcome's default otherwise -- because it is the status
    /// message the producer sees. The machine token of the outcome stays the
    /// metric label only.
    async fn delivery(
        effects: EffectHandler<OtapPdata>,
        token: AckToken,
        outcome: Outcome,
        reason: Option<Rc<str>>,
    ) -> Result<(), Error> {
        let data = token.pdata();
        let sentence = || reason.as_deref().unwrap_or(outcome.sentence()).to_owned();
        match outcome {
            Outcome::Ack => effects.notify_ack(AckMsg::new(data)).await,
            Outcome::Shutdown => {
                effects
                    .notify_nack(NackMsg::new_with_cause(
                        sentence(),
                        data,
                        NackCause::NodeShutdown,
                    ))
                    .await
            }
            refused if refused.refused() => {
                effects
                    .notify_nack(NackMsg::new_permanent_with_cause(
                        sentence(),
                        data,
                        NackCause::Refused,
                    ))
                    .await
            }
            _ => effects.notify_nack(NackMsg::new(sentence(), data)).await,
        }
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
    /// The future is outside tokio's cooperative budget. A send that is
    /// polled once and dropped when it reports `Pending` loses the token it
    /// owns, and the budget reports `Pending` after 128 operations in one task
    /// poll however much room the completion channel has, so every path that
    /// polls a send once -- the force-drain refusal and the deadline drain --
    /// would otherwise decide at most that many requests per poll.
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
    /// Called after shutdown is latched, possibly many times within one poll
    /// of the node. With the send slot free and nothing queued ahead, the send
    /// is started and polled once, and one that cannot finish yet stays in the
    /// slot rather than being dropped. Otherwise the refusal waits in the queue
    /// behind the others, which the node keeps serving until its deadline. The
    /// queue is bounded: normal completions stop one short of `capacity`
    /// (see [`Notifier::has_credit`]), so a saturated node still has at least
    /// one slot for a refusal here. Only past `capacity` is a refusal attempted
    /// once and, if the channel is full, counted as a delivery failure and
    /// released, so force-drain never grows memory without bound and never
    /// stalls on a full completion channel.
    pub(super) fn force_shutdown(&mut self, data: OtapPdata) {
        use futures::FutureExt;

        let (token, payload) = AckToken::split(data);
        drop(payload);
        self.token_high_water = self.token_high_water.max(token.bytes());
        self.outcomes[Outcome::Shutdown as usize] += 1;
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
        if self.len() < self.capacity {
            self.queue.push_back((token, Outcome::Shutdown, None));
            return;
        }
        self.deliver_now(token, Outcome::Shutdown, None);
    }

    /// Attempt one completion immediately, counting a send that would block.
    ///
    /// Used only on the paths that must not park a token: a force-drained
    /// request past the queue bound and the completions abandoned once the
    /// shutdown deadline has elapsed. The token is released either way, so the
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

#[cfg(test)]
mod tests {
    use super::super::tests::{effects, empty_pdata};
    use super::*;
    use otel_arrow_dfe_engine::control::PipelineCompletionMsg;
    use std::time::Duration;

    /// Scenario: a request carrying transport headers is split into its
    /// completion and its payload. Authorization claims are not planted: the
    /// only way to attach them, `capture_authorized_identity`, is private to
    /// the otap crate, so this test cannot cover the claims half of `split`.
    /// Guarantees: the completion keeps the routing frames but not the
    /// transport headers, so the credentials a producer sent are not held for
    /// as long as its request waits for its block to be written.
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
    }

    /// Scenario: a token moves from a reserved queue cell into a blocked send
    /// future.
    /// Guarantees: queue storage, external routing buffers and future storage
    /// are each charged once, so a saturated notifier reports the memory it
    /// actually holds rather than double counting the token in flight.
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
    /// Guarantees: an ack arrives as an ack, a validation refusal as a
    /// permanent `Refused` nack and a storage failure as a retryable one, so a
    /// retry processor redelivers exactly the requests that can still succeed.
    #[tokio::test(flavor = "current_thread")]
    async fn each_outcome_is_delivered_as_its_completion() {
        let (handler, mut rx) = effects(4);
        let mut notify = Notifier::new(handler, 2);

        for outcome in [Outcome::Ack, Outcome::TooLarge, Outcome::Storage] {
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
                assert_eq!(nack.reason, Outcome::TooLarge.sentence());
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
        assert_eq!(notify.outcomes()[Outcome::TooLarge as usize], 1);
        assert_eq!(notify.outcomes()[Outcome::Storage as usize], 1);
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());
        assert!(notify.token_high_water() > 0);
    }

    /// Scenario: force-drained requests are refused while the completion
    /// channel has room for one, so the second has to wait, a third finds the
    /// notifier at capacity, and the channel is then drained.
    /// Guarantees: the first is delivered at once, the second stays in the
    /// send slot and is delivered once the channel has room rather than being
    /// dropped, and only the third, past the notifier's bound, is counted as a
    /// delivery failure and released, so force-drain is bounded, never
    /// blocks, and loses a refusal only when both the channel and the bound
    /// are exhausted.
    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_refusals_wait_in_the_bound_then_fail() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 1);

        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());

        // The channel now holds the first refusal, so the second cannot be
        // handed over without blocking: it keeps the send slot.
        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 0);
        assert_eq!(notify.len(), 1);

        // The notifier is at its bound of one, so the third is attempted
        // once, finds the channel full and is counted.
        notify.force_shutdown(empty_pdata());
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
    }

    /// Scenario: three hundred decided completions -- more than tokio's
    /// cooperative budget of 128 operations per task poll -- are drained at
    /// the shutdown deadline into a completion channel with room for all of
    /// them.
    /// Guarantees: every one is delivered and none is counted as a failure,
    /// because the drain is not throttled by the runtime's cooperative
    /// budget, so a restart hands each producer a retryable nack instead of
    /// leaving it to its own timeout.
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
    }

    /// Scenario: three hundred force-drained requests arrive after shutdown
    /// has been latched, one after the other within a single task poll, while
    /// the completion channel has room for all of them.
    /// Guarantees: each is refused with a delivered retryable `NodeShutdown`
    /// nack and none is counted as a delivery failure, so the force-drain path
    /// is not silently truncated by the cooperative budget either.
    #[tokio::test(flavor = "current_thread")]
    async fn force_drained_refusals_beyond_the_coop_budget_are_all_delivered() {
        const N: usize = 300;
        let (handler, mut rx) = effects(N);
        let mut notify = Notifier::new(handler, 8);
        for _ in 0..N {
            notify.force_shutdown(empty_pdata());
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
    }

    /// Scenario: a notifier is created with a capacity no queue could ever
    /// hold -- what a very large `window.max_requests_per_block` doubles to --
    /// and then used.
    /// Guarantees: creation allocates nothing up front and the queue grows
    /// only with the completions it actually holds, so a large configured
    /// bound neither aborts the worker at start nor reserves memory it never
    /// uses.
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
    }

    /// Scenario: one completion is parked in a blocked send and a second,
    /// strictly later one waits behind it in the queue.
    /// Guarantees: the oldest outstanding completion is the blocked send, not
    /// the queued one and not a completion that has already been delivered, so
    /// a shutdown deadline is measured against the work the notifier still
    /// owes.
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

    /// Scenario: normal completions are queued until the notifier is one slot
    /// from its capacity, and a force-drained request is then decided.
    /// Guarantees: normal outcomes stop at `capacity - 1` live completions and
    /// the last slot stays usable by a shutdown outcome, so the node always
    /// keeps the credit it needs to decide one force-drained request.
    #[tokio::test(flavor = "current_thread")]
    async fn the_last_completion_slot_is_reserved_for_shutdown() {
        // Capacity 4 stands for 2N with N = 2; the cap on normal live
        // completions is therefore 3.
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 4);

        for _ in 0..3 {
            assert!(notify.has_credit(notify.len()));
            let (token, payload) = AckToken::split(empty_pdata());
            drop(payload);
            notify.push(token, Outcome::Ack);
        }
        assert_eq!(notify.len(), 3);
        assert!(!notify.has_credit(notify.len()));

        let (token, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(token, Outcome::Shutdown);
        assert_eq!(notify.len(), 4);
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 1);
    }

    /// Scenario: a normal completion is pushed while the only free slot is the
    /// one reserved for a forced drain.
    /// Guarantees: the push panics rather than silently spending the reserved
    /// credit, so the bound is a checked worker contract.
    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "worker must reserve completion credit")]
    async fn a_normal_completion_cannot_take_the_reserved_slot() {
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 2);

        let (first, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(first, Outcome::Ack);

        let (second, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(second, Outcome::Ack);
    }
}
