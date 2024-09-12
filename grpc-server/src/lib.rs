// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use cadence::{Counted, StatsdClient, Timed};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::BoxBody;
use tonic::transport::{Body, Server};
use tonic::{Request, Response};
use tonic_middleware::{Middleware, MiddlewareFor, ServiceBound};

use swh_graph::graph::SwhFullGraph;
use swh_graph::properties::NodeIdFromSwhidError;
use swh_graph::utils::suffix_path;
use swh_graph::views::Subgraph;

pub mod proto {
    tonic::include_proto!("swh.graph");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("swhgraph_descriptor");
}

mod filters;
mod find_path;
mod node_builder;
pub mod statsd;
mod traversal;
pub mod visitor;

/// Runs a long-running function in a separate thread so it does not block.
///
/// This differs from [`tokio::task::spawn_blocking`] in that the closure does not
/// need to be `'static` (so it can borrow from its scope)
pub(crate) fn scoped_spawn_blocking<R: Send + Sync + 'static, F: FnOnce() -> R + Send>(f: F) -> R {
    let ((), mut outputs): ((), Vec<Result<R, tokio::task::JoinError>>) =
        async_scoped::TokioScope::scope_and_block(|scope| scope.spawn_blocking(f));
    assert_eq!(outputs.len(), 1, "Unexpected number of futures spawned");
    outputs.pop().unwrap().expect("could not join task")
}

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;

pub struct TraversalService<G: SwhFullGraph + Clone + Send + Sync + 'static>(G);

impl<G: SwhFullGraph + Clone + Send + Sync + 'static> TraversalService<G> {
    pub fn new(graph: G) -> Self {
        TraversalService(graph)
    }
}

pub trait TraversalServiceTrait {
    type Graph: SwhFullGraph + Clone + Send + Sync + 'static;
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status>;
    fn graph(&self) -> &Self::Graph;
}

impl<G: SwhFullGraph + Clone + Send + Sync + 'static> TraversalServiceTrait
    for TraversalService<G>
{
    type Graph = G;

    #[inline]
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status> {
        let node = self
            .0
            .properties()
            .node_id_from_string_swhid(swhid)
            .map_err(|e| match e {
                NodeIdFromSwhidError::InvalidSwhid(e) => {
                    tonic::Status::invalid_argument(format!("Invalid SWHID: {e}"))
                }
                NodeIdFromSwhidError::UnknownSwhid(e) => {
                    tonic::Status::not_found(format!("Unknown SWHID: {e}"))
                }
                NodeIdFromSwhidError::InternalError(e) => {
                    tonic::Status::internal(format!("Internal error: {e}"))
                }
            })?;

        if self.0.has_node(node) {
            Ok(node)
        } else {
            Err(tonic::Status::not_found(format!(
                "Unavailable node: {swhid}"
            )))
        }
    }

    #[inline(always)]
    fn graph(&self) -> &Self::Graph {
        &self.0
    }
}

#[tonic::async_trait]
impl<G: SwhFullGraph + Send + Sync + Clone + 'static>
    proto::traversal_service_server::TraversalService for TraversalService<G>
{
    async fn get_node(&self, request: Request<proto::GetNodeRequest>) -> TonicResult<proto::Node> {
        let arc_checker = filters::ArcFilterChecker::new(self.0.clone(), None)?;
        let subgraph = Arc::new(Subgraph::with_arc_filter(
            self.0.clone(),
            move |src, dst| arc_checker.matches(src, dst),
        ));
        let proto::GetNodeRequest { swhid, mask } = request.get_ref().clone();
        let node_builder = node_builder::NodeBuilder::new(
            subgraph,
            mask.map(|mask| prost_types::FieldMask {
                paths: mask.paths.iter().map(|field| field.to_owned()).collect(),
            }),
        )?;
        let node_id = self.try_get_node_id(&swhid)?;
        Ok(Response::new(node_builder.build_node(node_id)))
    }

    type TraverseStream = ReceiverStream<Result<proto::Node, tonic::Status>>;
    async fn traverse(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<Self::TraverseStream> {
        traversal::SimpleTraversal { service: self }
            .traverse(request)
            .await
    }

    async fn find_path_to(
        &self,
        request: Request<proto::FindPathToRequest>,
    ) -> TonicResult<proto::Path> {
        find_path::FindPath { service: self }
            .find_path_to(request)
            .await
    }

    async fn find_path_between(
        &self,
        request: Request<proto::FindPathBetweenRequest>,
    ) -> TonicResult<proto::Path> {
        find_path::FindPath { service: self }
            .find_path_between(request)
            .await
    }

    async fn count_nodes(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        traversal::SimpleTraversal { service: self }
            .count_nodes(request)
            .await
    }

    async fn count_edges(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        traversal::SimpleTraversal { service: self }
            .count_edges(request)
            .await
    }

    async fn stats(
        &self,
        _request: Request<proto::StatsRequest>,
    ) -> TonicResult<proto::StatsResponse> {
        // Load properties
        let properties_path = suffix_path(self.0.path(), ".properties");
        let properties_path = properties_path.as_path();
        let properties = load_properties(properties_path, ".stats")?;

        // Load stats
        let stats_path = suffix_path(self.0.path(), ".stats");
        let stats_path = stats_path.as_path();
        let stats = load_properties(stats_path, ".stats")?;

        // Load export metadata
        let export_meta_path = self
            .0
            .path()
            .parent()
            .ok_or_else(|| {
                log::error!(
                    "Could not get path to meta/export.json from {}",
                    self.0.path().display()
                );
                tonic::Status::internal("Could not find meta/export.json file")
            })?
            .join("meta")
            .join("export.json");
        let export_meta_path = export_meta_path.as_path();
        let export_meta = load_export_meta(export_meta_path);
        let export_meta = export_meta.as_ref();

        Ok(Response::new(proto::StatsResponse {
            num_nodes: self.0.num_nodes() as i64,
            num_edges: self.0.num_arcs() as i64,
            compression_ratio: get_property(&properties, properties_path, "compratio").ok(),
            bits_per_node: get_property(&properties, properties_path, "bitspernode").ok(),
            bits_per_edge: get_property(&properties, properties_path, "bitsperlink").ok(),
            avg_locality: get_property(&stats, stats_path, "avglocality")?,
            indegree_min: get_property(&stats, stats_path, "minindegree")?,
            indegree_max: get_property(&stats, stats_path, "maxindegree")?,
            indegree_avg: get_property(&stats, stats_path, "avgindegree")?,
            outdegree_min: get_property(&stats, stats_path, "minoutdegree")?,
            outdegree_max: get_property(&stats, stats_path, "maxoutdegree")?,
            outdegree_avg: get_property(&stats, stats_path, "avgoutdegree")?,
            export_started_at: export_meta.map(|export_meta| export_meta.export_start.timestamp()),
            export_ended_at: export_meta.map(|export_meta| export_meta.export_end.timestamp()),
        }))
    }
}

#[derive(serde_derive::Deserialize)]
struct ExportMeta {
    export_start: chrono::DateTime<chrono::Utc>,
    export_end: chrono::DateTime<chrono::Utc>,
}
fn load_export_meta(path: &Path) -> Option<ExportMeta> {
    let file = std::fs::File::open(path)
        .map_err(|e| {
            log::error!("Could not open {}: {}", path.display(), e);
        })
        .ok()?;
    let mut export_meta = String::new();
    std::io::BufReader::new(file)
        .read_to_string(&mut export_meta)
        .map_err(|e| {
            log::error!("Could not read {}: {}", path.display(), e);
        })
        .ok()?;
    let export_meta = serde_json::from_str(&export_meta)
        .map_err(|e| {
            log::error!("Could not parse {}: {}", path.display(), e);
        })
        .ok()?;
    Some(export_meta)
}

fn load_properties(path: &Path, suffix: &str) -> Result<HashMap<String, String>, tonic::Status> {
    let file = std::fs::File::open(path).map_err(|e| {
        log::error!("Could not open {}: {}", path.display(), e);
        tonic::Status::internal(format!("Could not open {} file", suffix))
    })?;
    let properties = java_properties::read(std::io::BufReader::new(file)).map_err(|e| {
        log::error!("Could not parse {}: {}", path.display(), e);
        tonic::Status::internal(format!("Could not parse {} file", suffix))
    })?;
    Ok(properties)
}

fn get_property<V: FromStr>(
    properties: &HashMap<String, String>,
    properties_path: &Path,
    name: &str,
) -> Result<V, tonic::Status>
where
    <V as FromStr>::Err: std::fmt::Display,
{
    properties
        .get(name)
        .ok_or_else(|| {
            log::error!("Missing {} in {}", name, properties_path.display());
            tonic::Status::internal(format!("Could not read {} from .properties", name))
        })?
        .parse()
        .map_err(|e| {
            log::error!(
                "Could not parse {} from {}",
                name,
                properties_path.display()
            );
            tonic::Status::internal(format!("Could not parse {} from .properties: {}", name, e))
        })
}

pub async fn serve<G: SwhFullGraph + Sync + Send + 'static>(
    graph: G,
    bind_addr: std::net::SocketAddr,
    statsd_client: cadence::StatsdClient,
) -> Result<(), tonic::transport::Error> {
    let graph = Arc::new(graph);
    Server::builder()
        .add_service(MiddlewareFor::new(
            proto::traversal_service_server::TraversalServiceServer::new(TraversalService::new(
                graph,
            )),
            MetricsMiddleware::new(statsd_client),
        ))
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .build_v1()
                .expect("Could not load v1 reflection service"),
        )
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .build_v1alpha()
                .expect("Could not load v1alpha reflection service"),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}

#[derive(Clone)]
pub struct MetricsMiddleware {
    statsd_client: Arc<StatsdClient>,
}

impl MetricsMiddleware {
    pub fn new(statsd_client: StatsdClient) -> Self {
        Self {
            statsd_client: Arc::new(statsd_client),
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

        match service.call(req).await {
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
}

impl<B: Body + Unpin> TimedBody<B> {
    fn publish_metrics(&self) {
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
