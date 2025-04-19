// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use cadence::StatsdClient;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_middleware::MiddlewareFor;
use tracing::{instrument, Level};

use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties::NodeIdFromSwhidError;
use swh_graph::utils::suffix_path;
use swh_graph::views::Subgraph;

pub mod proto {
    tonic::include_proto!("swh.graph");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("swhgraph_descriptor");
}

use proto::traversal_service_server::TraversalServiceServer;

mod filters;
mod find_path;
pub mod graph;
pub mod metrics;
mod node_builder;
#[cfg(feature = "sentry")]
pub mod sentry;
pub mod statsd;
mod traversal;
mod utils;
pub mod visitor;

pub(crate) use graph::FullOptGraph;

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

pub struct TraversalService<G: FullOptGraph + Send + Sync + 'static> {
    graph: Arc<G>,
    pub statsd_client: Option<Arc<StatsdClient>>,
}

impl<G: FullOptGraph + Send + Sync + 'static> TraversalService<G> {
    pub fn new(graph: Arc<G>, statsd_client: Option<Arc<StatsdClient>>) -> Self {
        TraversalService {
            graph,
            statsd_client,
        }
    }
}

pub trait TraversalServiceTrait {
    type Graph: FullOptGraph + Send + Sync + 'static;
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status>;
    fn graph(&self) -> &Arc<Self::Graph>;
    fn statsd_client(&self) -> Option<&Arc<StatsdClient>>;
}

impl<G: FullOptGraph + Send + Sync + 'static> TraversalServiceTrait for TraversalService<G> {
    type Graph = G;

    #[inline]
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status> {
        let node = self
            .graph
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

        if self.graph.has_node(node) {
            Ok(node)
        } else {
            Err(tonic::Status::not_found(format!(
                "Unavailable node: {swhid}"
            )))
        }
    }

    #[inline(always)]
    fn graph(&self) -> &Arc<Self::Graph> {
        &self.graph
    }

    #[inline(always)]
    fn statsd_client(&self) -> Option<&Arc<StatsdClient>> {
        self.statsd_client.as_ref()
    }
}

#[tonic::async_trait]
impl<G: FullOptGraph + Send + Sync + 'static> proto::traversal_service_server::TraversalService
    for TraversalService<G>
{
    async fn get_node(&self, request: Request<proto::GetNodeRequest>) -> TonicResult<proto::Node> {
        let arc_checker = filters::ArcFilterChecker::new(self.graph.clone(), None)?;
        let subgraph = Arc::new(Subgraph::with_arc_filter(
            self.graph.clone(),
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
    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn traverse(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<Self::TraverseStream> {
        tracing::info!("{:?}", request.get_ref());
        traversal::SimpleTraversal { service: self }
            .traverse(request)
            .await
    }

    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn find_path_to(
        &self,
        request: Request<proto::FindPathToRequest>,
    ) -> TonicResult<proto::Path> {
        tracing::info!("{:?}", request.get_ref());
        find_path::FindPath { service: self }
            .find_path_to(request)
            .await
    }

    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn find_path_between(
        &self,
        request: Request<proto::FindPathBetweenRequest>,
    ) -> TonicResult<proto::Path> {
        tracing::info!("{:?}", request.get_ref());
        find_path::FindPath { service: self }
            .find_path_between(request)
            .await
    }

    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn count_nodes(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        tracing::info!("{:?}", request.get_ref());
        traversal::SimpleTraversal { service: self }
            .count_nodes(request)
            .await
    }

    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn count_edges(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        tracing::info!("{:?}", request.get_ref());
        traversal::SimpleTraversal { service: self }
            .count_edges(request)
            .await
    }

    #[instrument(skip(self, request))]
    async fn stats(
        &self,
        request: Request<proto::StatsRequest>,
    ) -> TonicResult<proto::StatsResponse> {
        tracing::info!("{:?}", request.get_ref());
        // Load properties
        let properties_path = suffix_path(self.graph.path(), ".properties");
        let properties_path = properties_path.as_path();
        let properties = load_properties(properties_path, ".stats")?;

        // Load stats
        let stats_path = suffix_path(self.graph.path(), ".stats");
        let stats_path = stats_path.as_path();
        let stats = load_properties(stats_path, ".stats")?;

        // Load export metadata
        let export_meta_path = self
            .graph
            .path()
            .parent()
            .ok_or_else(|| {
                log::error!(
                    "Could not get path to meta/export.json from {}",
                    self.graph.path().display()
                );
                tonic::Status::internal("Could not find meta/export.json file")
            })?
            .join("meta")
            .join("export.json");
        let export_meta_path = export_meta_path.as_path();
        let export_meta = load_export_meta(export_meta_path);
        let export_meta = export_meta.as_ref();

        Ok(Response::new(proto::StatsResponse {
            num_nodes: self.graph.num_nodes() as i64,
            num_edges: self.graph.num_arcs() as i64,
            compression_ratio: get_property(&properties, properties_path, "compratio").ok(),
            bits_per_node: get_property(&properties, properties_path, "bitspernode").ok(),
            bits_per_edge: get_property(&properties, properties_path, "bitsperlink").ok(),
            avg_locality: get_property(&stats, stats_path, "avglocality").ok(),
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

pub async fn serve<G: FullOptGraph + Sync + Send + 'static>(
    graph: G,
    bind_addr: std::net::SocketAddr,
    statsd_client: cadence::StatsdClient,
) -> Result<(), tonic::transport::Error> {
    let statsd_client = Arc::new(statsd_client);
    let graph = Arc::new(graph);

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<TraversalServiceServer<TraversalService<G>>>()
        .await;

    #[cfg(not(feature = "sentry"))]
    let mut builder = Server::builder();
    #[cfg(feature = "sentry")]
    let mut builder =
        Server::builder().layer(::sentry::integrations::tower::NewSentryLayer::new_from_top());
    builder
        .add_service(MiddlewareFor::new(
            TraversalServiceServer::new(TraversalService::new(graph, Some(statsd_client.clone()))),
            metrics::MetricsMiddleware::new(statsd_client),
        ))
        .add_service(health_service)
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .build_v1()
                .expect("Could not load v1 reflection service"),
        )
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .build_v1alpha()
                .expect("Could not load v1alpha reflection service"),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}
