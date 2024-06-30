// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response};

use crate::graph::{SwhGraphWithProperties, SwhLabeledBackwardGraph, SwhLabeledForwardGraph};
use crate::properties::NodeIdFromSwhidError;
use crate::utils::suffix_path;
use crate::views::Subgraph;

pub mod proto {
    tonic::include_proto!("swh.graph");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("swhgraph_descriptor");
}

mod filters;
mod find_path;
mod node_builder;
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

pub struct TraversalService<
    G: SwhGraphWithProperties
        + SwhLabeledBackwardGraph
        + SwhLabeledForwardGraph
        + Clone
        + Send
        + Sync
        + 'static,
>(G);

impl<
        G: SwhGraphWithProperties
            + SwhLabeledBackwardGraph
            + SwhLabeledForwardGraph
            + Clone
            + Send
            + Sync
            + 'static,
    > TraversalService<G>
{
    pub fn new(graph: G) -> Self {
        TraversalService(graph)
    }
}

pub trait TraversalServiceTrait {
    type Graph: SwhGraphWithProperties
        + SwhLabeledBackwardGraph
        + SwhLabeledForwardGraph
        + Clone
        + Send
        + Sync
        + 'static;
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status>
    where
        <Self::Graph as SwhGraphWithProperties>::Maps: crate::properties::Maps;
    fn graph(&self) -> &Self::Graph;
}

impl<
        G: SwhGraphWithProperties
            + SwhLabeledBackwardGraph
            + SwhLabeledForwardGraph
            + Clone
            + Send
            + Sync
            + 'static,
    > TraversalServiceTrait for TraversalService<G>
{
    type Graph = G;

    #[inline]
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status>
    where
        <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    {
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
impl<
        G: SwhGraphWithProperties
            + SwhLabeledBackwardGraph
            + SwhLabeledForwardGraph
            + Send
            + Sync
            + Clone
            + 'static,
    > proto::traversal_service_server::TraversalService for TraversalService<G>
where
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: crate::properties::Timestamps,
    <G as SwhGraphWithProperties>::Persons: crate::properties::Persons,
    <G as SwhGraphWithProperties>::Contents: crate::properties::Contents,
    <G as SwhGraphWithProperties>::Strings: crate::properties::Strings,
    <G as SwhGraphWithProperties>::LabelNames: crate::properties::LabelNames,
{
    async fn get_node(&self, request: Request<proto::GetNodeRequest>) -> TonicResult<proto::Node> {
        let arc_checker = filters::ArcFilterChecker::new(self.0.clone(), None)?;
        let subgraph = Arc::new(Subgraph::new_with_arc_filter(
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

pub async fn serve<
    G: SwhGraphWithProperties
        + SwhLabeledForwardGraph
        + SwhLabeledBackwardGraph
        + Sync
        + Send
        + 'static,
>(
    graph: G,
    bind_addr: std::net::SocketAddr,
) -> Result<(), tonic::transport::Error>
where
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    <G as SwhGraphWithProperties>::Timestamps: crate::properties::Timestamps,
    <G as SwhGraphWithProperties>::Persons: crate::properties::Persons,
    <G as SwhGraphWithProperties>::Contents: crate::properties::Contents,
    <G as SwhGraphWithProperties>::Strings: crate::properties::Strings,
    <G as SwhGraphWithProperties>::LabelNames: crate::properties::LabelNames,
{
    let graph = Arc::new(graph);
    Server::builder()
        .add_service(
            proto::traversal_service_server::TraversalServiceServer::new(TraversalService::new(
                graph,
            )),
        )
        .add_service(
            tonic_reflection::server::Builder::configure()
                //.register_encoded_file_descriptor_set(tonic_reflection::pb::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .build()
                .expect("Could not load reflection service"),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}
