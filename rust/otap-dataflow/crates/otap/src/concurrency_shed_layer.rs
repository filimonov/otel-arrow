// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Tower middleware that refuses a gRPC request with a retryable status when the
//! receiver's concurrency limit has no free permit.

use crate::otlp_metrics::{OtlpProtocol, OtlpReceiverMetrics};
use futures::future::Either;
use http::{Request, Response};
use otel_arrow_dfe_telemetry::common_attributes::ReceiverRejectionErrorType;
use parking_lot::Mutex;
use std::future::{Ready, ready};
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::{Status, body::Body};
use tower::{Layer, Service};

/// Message of every refusal at the receiver's concurrency limit, on gRPC and HTTP.
pub const CONCURRENCY_LIMIT_MESSAGE: &str =
    "receiver concurrency limit reached (max_concurrent_requests); retry later";

/// Builds the status for a request refused at the receiver's concurrency limit.
///
/// UNAVAILABLE is retryable for every OTLP client, while RESOURCE_EXHAUSTED is
/// retryable only with a `google.rpc.RetryInfo` detail attached.
#[must_use]
pub fn grpc_concurrency_limit_status() -> Status {
    Status::unavailable(CONCURRENCY_LIMIT_MESSAGE)
}

/// Layer that answers [`grpc_concurrency_limit_status`] when the inner service
/// is not ready, instead of queueing the request.
#[derive(Clone)]
pub struct ConcurrencyShedLayer {
    metrics: Arc<Mutex<OtlpReceiverMetrics>>,
}

impl ConcurrencyShedLayer {
    /// Creates a layer that counts each refusal as a gRPC `concurrency_limit` rejection.
    #[must_use]
    pub const fn new(metrics: Arc<Mutex<OtlpReceiverMetrics>>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for ConcurrencyShedLayer {
    type Service = ConcurrencyShedService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConcurrencyShedService {
            inner,
            metrics: self.metrics.clone(),
            inner_ready: false,
        }
    }
}

/// Service implementation for [`ConcurrencyShedLayer`].
pub struct ConcurrencyShedService<S> {
    inner: S,
    metrics: Arc<Mutex<OtlpReceiverMetrics>>,
    inner_ready: bool,
}

impl<S: Clone> Clone for ConcurrencyShedService<S> {
    fn clone(&self) -> Self {
        // A clone's inner service has not been polled, so it is not ready yet.
        Self {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            inner_ready: false,
        }
    }
}

impl<S, ReqBody> Service<Request<ReqBody>> for ConcurrencyShedService<S>
where
    S: Service<Request<ReqBody>, Response = Response<Body>>,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Either<S::Future, Ready<Result<Self::Response, Self::Error>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner_ready = match self.inner.poll_ready(cx) {
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => false,
        };
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        if std::mem::take(&mut self.inner_ready) {
            return Either::Left(self.inner.call(request));
        }
        self.metrics.lock().record_rejection(
            OtlpProtocol::Grpc,
            ReceiverRejectionErrorType::ConcurrencyLimit,
        );
        Either::Right(ready(Ok(grpc_concurrency_limit_status().into_http())))
    }
}
