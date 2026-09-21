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
    #[allow(dead_code)]
    Shutdown,
}

/// Number of [`Outcome`] variants, and so the width of the counter array.
pub(super) const OUTCOMES: usize = 6;

impl Outcome {
    /// Stable reason string reported on the completion.
    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::TooLarge => "too_large",
            Self::Invalid => "invalid",
            Self::Unsupported => "unsupported",
            Self::Storage => "storage",
            Self::Shutdown => "shutdown",
        }
    }

    /// Whether the sender must change the request before retrying it.
    pub(super) fn refused(self) -> bool {
        matches!(self, Self::TooLarge | Self::Invalid | Self::Unsupported)
    }
}

/// A completion send that has been started but has not resolved.
type SendFuture = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

/// The single in-flight completion send, kept across polls.
#[allow(dead_code)]
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
    queue: VecDeque<(AckToken, Outcome)>,
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
    pub(super) fn new(effects: EffectHandler<OtapPdata>, capacity: usize) -> Self {
        Self {
            effects,
            queue: VecDeque::with_capacity(capacity),
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

    // The exporter's receive loop still blocks on one request at a time, so
    // the inbox force-drains on its own and the shutdown and reporting surface
    // below has no production caller yet. The nonblocking select that calls it
    // arrives with the window pair; it is built and tested here because the
    // ownership rules it depends on -- one charge per token, a send future
    // that survives a cancelled poll -- belong to this module.
    /// Whether no completion is outstanding.
    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes the notifier keeps resident.
    ///
    /// Each token is charged once: a queued token by the queue cell it sits in
    /// plus its external buffers, and the sending token by the send future's
    /// storage, which owns it inline, plus the same external buffers.
    #[allow(dead_code)]
    pub(super) fn bytes(&self) -> usize {
        self.queue
            .iter()
            .map(|(token, _)| token.external_bytes())
            .sum::<usize>()
            + self.sending.as_ref().map_or(0, |sending| sending.bytes)
            + self.queue.capacity() * size_of::<(AckToken, Outcome)>()
    }

    /// When the oldest outstanding completion was taken ownership of.
    #[allow(dead_code)]
    pub(super) fn oldest(&self) -> Option<Instant> {
        self.queue
            .iter()
            .map(|(token, _)| token.received)
            .chain(self.sending.iter().map(|sending| sending.received))
            .min()
    }

    /// Counters of pushed completions, indexed by [`Outcome`].
    #[allow(dead_code)]
    pub(super) fn outcomes(&self) -> &[u64; OUTCOMES] {
        &self.outcomes
    }

    /// Completions the engine would not accept.
    #[allow(dead_code)]
    pub(super) fn failures(&self) -> u64 {
        self.failures
    }

    /// Largest single token the notifier has held.
    #[allow(dead_code)]
    pub(super) fn token_high_water(&self) -> usize {
        self.token_high_water
    }

    /// Queue one decided request.
    ///
    /// The caller reserves the credit before it admits the request, so a push
    /// that would exceed the capacity is a worker bug rather than a runtime
    /// condition.
    pub(super) fn push(&mut self, token: AckToken, outcome: Outcome) {
        assert!(
            self.len() < self.capacity,
            "worker must reserve completion credit"
        );
        self.token_high_water = self.token_high_water.max(token.bytes());
        self.outcomes[outcome as usize] += 1;
        self.queue.push_back((token, outcome));
    }

    /// Drive the single completion send to completion.
    ///
    /// Cancellation safe: the send future is installed before it is polled and
    /// is left installed if the poll is dropped, so a cancelled `select` branch
    /// never loses the token it owns. With nothing outstanding this never
    /// resolves, which lets the caller use it as an idle `select` branch.
    pub(super) async fn next(&mut self) -> Result<(), Error> {
        if self.sending.is_none() {
            let Some((token, outcome)) = self.queue.pop_front() else {
                return pending().await;
            };
            let external = token.external_bytes();
            let received = token.received;
            let effects = self.effects.clone();
            let future = async move {
                let data = token.pdata();
                match outcome {
                    Outcome::Ack => effects.notify_ack(AckMsg::new(data)).await,
                    Outcome::Shutdown => {
                        effects
                            .notify_nack(NackMsg::new_with_cause(
                                outcome.reason(),
                                data,
                                NackCause::NodeShutdown,
                            ))
                            .await
                    }
                    refused if refused.refused() => {
                        effects
                            .notify_nack(NackMsg::new_permanent_with_cause(
                                refused.reason(),
                                data,
                                NackCause::Refused,
                            ))
                            .await
                    }
                    other => {
                        effects
                            .notify_nack(NackMsg::new(other.reason(), data))
                            .await
                    }
                }
            };
            let bytes = external + size_of_val(&future);
            self.sending = Some(Sending {
                bytes,
                received,
                future: Box::pin(future),
            });
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

    /// Refuse one force-drained request without queueing it.
    ///
    /// Called after shutdown is latched, when the notifier may already be at
    /// capacity. The retryable `NodeShutdown` nack is attempted once,
    /// immediately; a send that would block is counted as a failure and the
    /// token is released rather than parked, so force-drain never stalls on a
    /// full completion channel.
    #[allow(dead_code)]
    pub(super) fn force_shutdown(&mut self, data: OtapPdata) {
        use futures::FutureExt;

        let (token, payload) = AckToken::split(data);
        drop(payload);
        self.token_high_water = self.token_high_water.max(token.bytes());
        self.outcomes[Outcome::Shutdown as usize] += 1;
        let delivered = self
            .effects
            .notify_nack(NackMsg::new_with_cause(
                Outcome::Shutdown.reason(),
                token.pdata(),
                NackCause::NodeShutdown,
            ))
            .now_or_never();
        if !matches!(delivered, Some(Ok(()))) {
            self.failures += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{effects, empty_pdata};
    use super::*;
    use otel_arrow_dfe_engine::control::PipelineCompletionMsg;
    use std::time::Duration;

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
        let queue = notify.queue.capacity() * size_of::<(AckToken, Outcome)>();
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
                assert_eq!(nack.reason, "too_large");
            }
            other => panic!("expected a nack, got {other:?}"),
        }
        match rx.recv().await.expect("storage failure") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::Unspecified);
                assert_eq!(nack.reason, "storage");
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

    /// Scenario: a force-drained request is refused while the completion
    /// channel still has room, and then another while it is full.
    /// Guarantees: the first is delivered immediately as a retryable
    /// `NodeShutdown` nack, the second is counted as a delivery failure and
    /// released, and neither enters the queue, so forced drain never blocks
    /// and never leaves a request undecided in the notifier.
    #[tokio::test(flavor = "current_thread")]
    async fn forced_shutdown_refusals_bypass_the_queue() {
        let (handler, mut rx) = effects(1);
        let mut notify = Notifier::new(handler, 1);

        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 0);
        assert!(notify.is_empty());

        // The channel now holds the first refusal, so the second cannot be
        // handed over without blocking.
        notify.force_shutdown(empty_pdata());
        assert_eq!(notify.failures(), 1);
        assert!(notify.is_empty());
        assert_eq!(notify.outcomes()[Outcome::Shutdown as usize], 2);

        match rx.recv().await.expect("shutdown refusal") {
            PipelineCompletionMsg::DeliverNack { nack } => {
                assert!(!nack.permanent);
                assert_eq!(nack.cause, NackCause::NodeShutdown);
            }
            other => panic!("expected a nack, got {other:?}"),
        }
    }

    /// Scenario: two completions are queued and the oldest is asked for while
    /// one of them is parked in a blocked send.
    /// Guarantees: the age of the oldest outstanding completion is reported
    /// whether it sits in the queue or in the send slot, so a shutdown
    /// deadline can be measured against work the notifier still owes.
    #[tokio::test(flavor = "current_thread")]
    async fn oldest_spans_the_queue_and_the_blocked_send() {
        let (handler, _rx) = effects(1);
        let mut notify = Notifier::new(handler, 3);
        assert_eq!(notify.oldest(), None);

        let (first, payload) = AckToken::split(empty_pdata());
        drop(payload);
        let first_received = first.received;
        notify.push(first, Outcome::Ack);
        notify.next().await.expect("fill completion channel");

        let (second, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(second, Outcome::Ack);
        let (third, payload) = AckToken::split(empty_pdata());
        drop(payload);
        notify.push(third, Outcome::Ack);

        assert!(
            tokio::time::timeout(Duration::from_millis(1), notify.next())
                .await
                .is_err()
        );
        assert_eq!(notify.len(), 2);
        let oldest = notify.oldest().expect("two completions are outstanding");
        assert!(oldest >= first_received);
        assert!(notify.sending.is_some());
    }
}
