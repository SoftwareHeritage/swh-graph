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

use crate::graph::{SwhBidirectionalGraph, SwhForwardGraph, SwhGraph, SwhGraphWithProperties};
use crate::mph::SwhidMphf;
use crate::properties;
use crate::utils::suffix_path;
use crate::views::Transposed;
use crate::AllSwhGraphProperties;

pub mod proto {
    tonic::include_proto!("swh.graph");

    pub(crate) const FILE_DESCRIPTOR_SET: &'static [u8] =
        tonic::include_file_descriptor_set!("swhgraph_descriptor");
}

pub mod traversal;
use traversal::VisitFlow;

mod node_builder;
use node_builder::NodeBuilder;

mod filters;
use filters::{ArcFilterChecker, NodeFilterChecker};

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

    fn make_visitor<'a, G: Deref + Clone + Send + Sync + 'static, Error: Send + 'a>(
        &'a self,
        request: Request<proto::TraversalRequest>,
        graph: G,
        mut on_node: impl FnMut(usize, u64) -> Result<(), Error> + Send + 'a,
        mut on_arc: impl FnMut(usize, usize) -> Result<(), Error> + Send + 'a,
    ) -> Result<
        traversal::SimpleBfsVisitor<
            G::Target,
            G,
            Error,
            impl FnMut(usize, u64, u64) -> Result<VisitFlow, Error>,
            impl FnMut(usize, usize, u64) -> Result<VisitFlow, Error>,
        >,
        tonic::Status,
    >
    where
        G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
        <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
    {
        let proto::TraversalRequest {
            src,
            direction: _, // Handled by caller
            edges,
            max_edges,
            min_depth,
            max_depth,
            return_nodes,
            mask: _, // Handled by caller
            max_matching_nodes,
        } = request.get_ref().clone();
        let min_depth = match min_depth {
            None => 0,
            Some(i) => i.try_into().map_err(|_| {
                tonic::Status::invalid_argument("min_depth must be a positive integer")
            })?,
        };
        let max_depth = match max_depth {
            None => u64::MAX,
            Some(i) => i.try_into().map_err(|_| {
                tonic::Status::invalid_argument("max_depth must be a positive integer")
            })?,
        };
        let mut max_edges = match max_edges {
            None => u64::MAX,
            Some(i) => i.try_into().map_err(|_| {
                tonic::Status::invalid_argument("max_edges must be a positive integer")
            })?,
        };
        let max_matching_nodes = match max_matching_nodes {
            None => usize::MAX,
            Some(0) => usize::MAX, // Quirk-compatibility with the Java implementation
            Some(max_nodes) => max_nodes.try_into().map_err(|_| {
                tonic::Status::invalid_argument("max_matching_nodes must be a positive integer")
            })?,
        };

        let return_node_checker =
            NodeFilterChecker::new(graph.clone(), return_nodes.unwrap_or_default())?;
        let arc_checker = ArcFilterChecker::new(graph.clone(), edges)?;
        let mut num_matching_nodes = 0;
        let mut visitor = traversal::SimpleBfsVisitor::new(
            graph.clone(),
            max_depth,
            move |node, depth, num_successors| {
                if !return_node_checker.matches(node, num_successors) {
                    return Ok(VisitFlow::Continue);
                }

                if num_successors > max_edges {
                    return Ok(VisitFlow::Stop);
                }
                max_edges -= num_successors;

                if depth >= min_depth {
                    on_node(node, num_successors)?;
                    num_matching_nodes += 1;
                    if num_matching_nodes >= max_matching_nodes {
                        return Ok(VisitFlow::Stop);
                    }
                }
                Ok(VisitFlow::Continue)
            },
            move |src, dst, _depth| {
                if arc_checker.matches(src, dst) {
                    on_arc(src, dst)?;
                    Ok(VisitFlow::Continue)
                } else {
                    Ok(VisitFlow::Ignore)
                }
            },
        );
        for src_item in &src {
            visitor.push(self.try_get_node_id(src_item)?);
        }
        Ok(visitor)
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

        let node_builder = NodeBuilder::new(graph.clone(), request.get_ref().mask.clone())?;
        let (tx, rx) = mpsc::channel(1_000);
        let on_node =
            move |node, _num_successors| tx.blocking_send(Ok(node_builder.build_node(node)));
        let on_arc = |_src, _dst| Ok(());

        // Spawning a thread because Tonic currently only supports Tokio, which requires
        // futures to be sendable between threads, and webgraph's successor iterators cannot
        match request.get_ref().direction.try_into() {
            Ok(proto::GraphDirection::Forward) => {
                let visitor = self.make_visitor(request, graph, on_node, on_arc)?;
                tokio::spawn(async move { std::thread::spawn(|| visitor.visit()).join() });
            }
            Ok(proto::GraphDirection::Backward) => {
                let visitor =
                    self.make_visitor(request, Arc::new(Transposed(graph)), on_node, on_arc)?;
                tokio::spawn(async move { std::thread::spawn(|| visitor.visit()).join() });
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }
        Ok(Response::new(ReceiverStream::new(rx)))
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
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        let graph = self.0.clone();

        let mut count = 0i64;
        let count_ref = &mut count;
        let on_node = move |_node, _num_successors| match count_ref.checked_add(1) {
            Some(new_count) => {
                *count_ref = new_count;
                Ok(())
            }
            None => Err(tonic::Status::resource_exhausted(
                "Node count overflowed i64",
            )),
        };
        let on_arc = |_src, _dst| Ok(());

        match request.get_ref().direction.try_into() {
            Ok(proto::GraphDirection::Forward) => {
                self.make_visitor(request, graph, on_node, on_arc)?
                    .visit()?;
            }
            Ok(proto::GraphDirection::Backward) => {
                self.make_visitor(request, Arc::new(Transposed(graph)), on_node, on_arc)?
                    .visit()?;
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }

        Ok(Response::new(proto::CountResponse { count }))
    }

    async fn count_edges(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        let graph = self.0.clone();

        let mut count = 0i64;
        let count_ref = &mut count;
        let on_node = |_node, _num_successors| Ok(());
        let on_arc = move |_src, _dst| match count_ref.checked_add(1) {
            Some(new_count) => {
                *count_ref = new_count;
                Ok(())
            }
            None => Err(tonic::Status::resource_exhausted(
                "Edge count overflowed i64",
            )),
        };

        match request.get_ref().direction.try_into() {
            Ok(proto::GraphDirection::Forward) => {
                self.make_visitor(request, graph, on_node, on_arc)?
                    .visit()?;
            }
            Ok(proto::GraphDirection::Backward) => {
                self.make_visitor(request, Arc::new(Transposed(graph)), on_node, on_arc)?
                    .visit()?;
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }

        Ok(Response::new(proto::CountResponse { count }))
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
