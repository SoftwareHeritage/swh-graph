// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response};

use crate::graph::{
    SwhBidirectionalGraph, SwhForwardGraph, SwhGraph, SwhGraphWithProperties, Transposed,
};
use crate::mph::SwhidMphf;
use crate::properties;
use crate::utils::suffix_path;
use crate::AllSwhGraphProperties;

pub mod proto {
    tonic::include_proto!("swh.graph");

    pub(crate) const FILE_DESCRIPTOR_SET: &'static [u8] =
        tonic::include_file_descriptor_set!("swhgraph_descriptor");
}

pub mod traversal;

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;

pub struct TraversalService<MPHF: SwhidMphf>(
    Arc<SwhBidirectionalGraph<AllSwhGraphProperties<MPHF>>>,
);

impl<MPHF: SwhidMphf> TraversalService<MPHF> {
    fn try_get_node_id(&self, swhid: &str) -> Result<usize, tonic::Status> {
        let swhid: crate::SWHID = swhid
            .try_into()
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid SWHID: {e}")))?;
        self.0
            .properties()
            .node_id(swhid)
            .ok_or_else(|| tonic::Status::not_found(format!("Unknown SWHID: {swhid}")))
    }

    async fn undirected_traverse<G: Deref + Clone + Send + Sync + 'static>(
        &self,
        request: Request<proto::TraversalRequest>,
        graph: G,
    ) -> TonicResult<ReceiverStream<Result<proto::Node, tonic::Status>>>
    where
        G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
        <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
    {
        let request = request.get_ref().clone();
        let allowed_edges = request.edges.unwrap_or("*".to_owned());
        let return_nodes = request.return_nodes.unwrap_or_default();
        if allowed_edges != "*" {
            return Err(tonic::Status::unimplemented("edges filter"));
        }
        if request.max_edges.is_some() {
            return Err(tonic::Status::unimplemented("max_edge filter"));
        }
        if request.min_depth.is_some() {
            return Err(tonic::Status::unimplemented("min_depth filter"));
        }
        if request.max_depth.is_some() {
            return Err(tonic::Status::unimplemented("max_depth filter"));
        }
        if return_nodes.types.unwrap_or("*".to_owned()) != "*" {
            return Err(tonic::Status::unimplemented("return_nodes.types filter"));
        }
        let max_matching_nodes = match request.max_matching_nodes {
            None => usize::MAX,
            Some(0) => usize::MAX, // Quirk-compatibility with the Java implementation
            Some(max_nodes) => max_nodes.try_into().map_err(|_| {
                tonic::Status::invalid_argument("max_matching_nodes must be a positive integer")
            })?,
        };
        let min_traversal_successors = match return_nodes.min_traversal_successors {
            None => 0,
            Some(min_succ) => min_succ.try_into().map_err(|_| {
                tonic::Status::invalid_argument(
                    "min_traversal_successors must be a positive integer",
                )
            })?,
        };
        let max_traversal_successors = match return_nodes.max_traversal_successors {
            None => usize::MAX,
            Some(max_succ) => max_succ.try_into().map_err(|_| {
                tonic::Status::invalid_argument(
                    "max_traversal_successors must be a positive integer",
                )
            })?,
        };
        let mut num_matching_nodes = 0;
        let (tx, rx) = mpsc::channel(1_000);
        let mut visitor = traversal::SimpleBfsVisitor::new(
            graph.clone(),
            move |node| {
                if graph.outdegree(node) < min_traversal_successors {
                    return Ok(true);
                }
                if graph.outdegree(node) > max_traversal_successors {
                    return Ok(true);
                }
                num_matching_nodes += 1;
                if num_matching_nodes > max_matching_nodes {
                    return Err(None);
                }
                tx.blocking_send(Ok(proto::Node {
                    swhid: graph
                        .properties()
                        .swhid(node)
                        .expect("Unknown node id")
                        .to_string(),
                    successor: Vec::new(),
                    num_successors: None,
                    data: None,
                }))
                .map(|()| true)
                .map_err(Some)
            },
            |_src, _dst| Ok(true),
        );
        for src in &request.src {
            visitor.push(self.try_get_node_id(src)?);
        }
        // Spawning a thread because it's too annoying to add async support in
        // SimpleBfsVisitor
        tokio::spawn(async move { std::thread::spawn(|| visitor.visit()).join() });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl<MPHF: SwhidMphf + Sync + Send + 'static> proto::traversal_service_server::TraversalService
    for TraversalService<MPHF>
{
    async fn get_node(&self, _request: Request<proto::GetNodeRequest>) -> TonicResult<proto::Node> {
        Err(tonic::Status::unimplemented(
            "get_node is not implemented yet",
        ))
    }

    type TraverseStream = ReceiverStream<Result<proto::Node, tonic::Status>>;
    async fn traverse(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<Self::TraverseStream> {
        let graph = self.0.clone();

        match request.get_ref().direction.try_into() {
            Ok(proto::GraphDirection::Forward) => self.undirected_traverse(request, graph).await,
            Ok(proto::GraphDirection::Backward) => {
                self.undirected_traverse(request, Arc::new(Transposed(graph)))
                    .await
            }
            Err(_) => Err(tonic::Status::invalid_argument("Invalid direction")),
        }
    }

    async fn find_path_to(
        &self,
        _request: Request<proto::FindPathToRequest>,
    ) -> TonicResult<proto::Path> {
        Err(tonic::Status::unimplemented(
            "find_path_to is not implemented yet",
        ))
    }

    async fn find_path_between(
        &self,
        _request: Request<proto::FindPathBetweenRequest>,
    ) -> TonicResult<proto::Path> {
        Err(tonic::Status::unimplemented(
            "find_path_between is not implemented yet",
        ))
    }

    async fn count_nodes(
        &self,
        _request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        Err(tonic::Status::unimplemented(
            "count_nodes is not implemented yet",
        ))
    }

    async fn count_edges(
        &self,
        _request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        Err(tonic::Status::unimplemented(
            "count_edges is not implemented yet",
        ))
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
            compression_ratio: get_property(&properties, properties_path, "compratio")?,
            bits_per_node: get_property(&properties, properties_path, "bitspernode")?,
            bits_per_edge: get_property(&properties, properties_path, "bitsperlink")?,
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
    let file = std::fs::File::open(&path)
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
    let file = std::fs::File::open(&path).map_err(|e| {
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

pub async fn serve<MPHF: SwhidMphf + Send + Sync + 'static>(
    graph: SwhBidirectionalGraph<AllSwhGraphProperties<MPHF>>,
    bind_addr: std::net::SocketAddr,
) -> Result<(), tonic::transport::Error> {
    let graph = Arc::new(graph);
    Server::builder()
        .add_service(
            proto::traversal_service_server::TraversalServiceServer::new(TraversalService(graph)),
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
