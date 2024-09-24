// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use cadence::{Counted, StatsdClient, Timed};
use tokio::time::Instant;
use tonic::body::BoxBody;
use tonic::transport::Body;
use tonic_middleware::{Middleware, ServiceBound};
use tracing::{span, Instrument, Level, Span};

#[derive(Clone)]
pub struct MetricsMiddleware {
    statsd_client: Arc<StatsdClient>,
    /// Uniquely identifies the span associated with a request
    request_id: Arc<AtomicU64>,
}

impl MetricsMiddleware {
    pub fn new(statsd_client: StatsdClient) -> Self {
        Self {
            statsd_client: Arc::new(statsd_client),
            request_id: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[tonic::async_trait]
impl<S> Middleware<S> for MetricsMiddleware
where
    S: ServiceBound,
    S::Future: Send,
{
    async fn call(
        &self,
        req: tonic::codegen::http::Request<BoxBody>,
        mut service: S,
    ) -> Result<tonic::codegen::http::Response<BoxBody>, S::Error> {
        let incoming_request_time = Instant::now();
        let uri = req.uri().clone();
        let request_id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let span = span!(Level::INFO, "request", id = request_id,);

        match service.call(req).instrument(span.clone()).await {
            Ok(resp) => {
                let status = resp.status();
                let (parts, body) = resp.into_parts();
                let body = TimedBody {
                    statsd_client: self.statsd_client.clone(),
                    body,
                    status,
                    uri,
                    incoming_request_time,
                    start_streaming_time: Instant::now(),
                    num_frames: 0,
                    span,
                };
                let resp = tonic::codegen::http::Response::from_parts(parts, BoxBody::new(body));
                Ok(resp)
            }
            Err(e) => {
                log::info!(
                    "ERR - {uri} - response: {:?}",
                    incoming_request_time.elapsed(),
                );
                Err(e)
            }
        }
    }
}

struct TimedBody<B: Body + Unpin> {
    statsd_client: Arc<StatsdClient>,
    body: B,
    status: tonic::codegen::http::StatusCode,
    uri: tonic::codegen::http::Uri,
    incoming_request_time: Instant,
    start_streaming_time: Instant,
    num_frames: u64,
    span: Span,
}

impl<B: Body + Unpin> TimedBody<B> {
    fn publish_metrics(&self) {
        // runs the log statement within the same tracing span as the rest of the request
        let _guard = self.span.enter();

        let end_streaming_time = Instant::now();
        let response_duration = self.start_streaming_time - self.incoming_request_time;
        let streaming_duration = end_streaming_time - self.start_streaming_time;
        log::info!(
            "{} - {} - response: {:?} - streaming: {:?}",
            self.status,
            self.uri,
            response_duration,
            streaming_duration
        );
        macro_rules! send_with_tags {
            ($metric_builder:expr) => {
                $metric_builder
                    .with_tag("path", self.uri.path())
                    .with_tag("status", &self.status.as_u16().to_string())
                    .send()
            };
        }
        send_with_tags!(self.statsd_client.count_with_tags("requests_total", 1));
        send_with_tags!(self
            .statsd_client
            .count_with_tags("frames_total", self.num_frames));
        // In millisecond according to the spec: https://github.com/b/statsd_spec#timers
        send_with_tags!(self
            .statsd_client
            .time_with_tags("response_wall_time_ms", response_duration));
        send_with_tags!(self
            .statsd_client
            .time_with_tags("streaming_wall_time_ms", streaming_duration));
    }
}

impl<B: Body + Unpin> Body for TimedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.body).poll_frame(cx).map(|frame| {
            self.num_frames += 1;
            if self.is_end_stream() {
                self.publish_metrics()
            }
            frame
        })
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
