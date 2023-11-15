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
use crate::SWHType;

pub mod proto {
    tonic::include_proto!("swh.graph");

    pub(crate) const FILE_DESCRIPTOR_SET: &'static [u8] =
        tonic::include_file_descriptor_set!("swhgraph_descriptor");
}

pub mod traversal;
use traversal::VisitFlow;

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;

fn parse_node_type(type_name: &str) -> Result<SWHType, tonic::Status> {
    SWHType::try_from(type_name)
        .map_err(|_| tonic::Status::invalid_argument(format!("Invalid node type: {}", type_name)))
}

// Returns `*` if the node type is `*`
fn parse_arc_type(type_name: &str) -> Result<(Option<SWHType>, Option<SWHType>), tonic::Status> {
    let mut splits = type_name.splitn(2, ':');
    let Some(src_type_name) = splits.next() else {
        return Err(tonic::Status::invalid_argument(format!("Invalid arc type: {} (should not be empty)", type_name)));
    };
    let Some(dst_type_name) = splits.next() else {
        return Err(tonic::Status::invalid_argument(format!("Invalid arc type: {} (should have a colon)", type_name)));
    };
    let src_type = match src_type_name {
        "*" => None,
        _ => Some(SWHType::try_from(src_type_name).map_err(|_| {
            tonic::Status::invalid_argument(format!("Invalid node type: {}", src_type_name))
        })?),
    };
    let dst_type = match dst_type_name {
        "*" => None,
        _ => Some(SWHType::try_from(dst_type_name).map_err(|_| {
            tonic::Status::invalid_argument(format!("Invalid node type: {}", dst_type_name))
        })?),
    };
    Ok((src_type, dst_type))
}

#[derive(Clone)]
struct NodeFilterChecker<G: Deref + Clone + Send + Sync + 'static> {
    graph: G,
    types: u8, // Bit mask
    min_traversal_successors: usize,
    max_traversal_successors: usize,
}

impl<G: Deref + Clone + Send + Sync + 'static> NodeFilterChecker<G>
where
    G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
{
    fn new(graph: G, filter: proto::NodeFilter) -> Result<Self, tonic::Status> {
        let proto::NodeFilter {
            types,
            min_traversal_successors,
            max_traversal_successors,
        } = filter;
        let types = types.unwrap_or("*".to_owned());
        Ok(NodeFilterChecker {
            graph,
            types: if types == "*" {
                u8::MAX // all bits set
            } else {
                types
                    .split(",")
                    .map(parse_node_type)
                    .collect::<Result<Vec<_>, _>>()? // Fold errors
                    .into_iter()
                    .map(Self::bit_mask)
                    .sum()
            },
            min_traversal_successors: match min_traversal_successors {
                None => 0,
                Some(min_succ) => min_succ.try_into().map_err(|_| {
                    tonic::Status::invalid_argument(
                        "min_traversal_successors must be a positive integer",
                    )
                })?,
            },
            max_traversal_successors: match max_traversal_successors {
                None => usize::MAX,
                Some(max_succ) => max_succ.try_into().map_err(|_| {
                    tonic::Status::invalid_argument(
                        "max_traversal_successors must be a positive integer",
                    )
                })?,
            },
        })
    }

    fn matches(&self, node: usize) -> bool {
        let outdegree = self.graph.outdegree(node);
        self.min_traversal_successors <= outdegree
            && outdegree <= self.max_traversal_successors
            && (self.types == u8::MAX
                || (Self::bit_mask(self.graph.properties().node_type(node).unwrap()) & self.types)
                    != 0)
    }

    fn bit_mask(node_type: SWHType) -> u8 {
        let type_id = node_type as u8;
        assert!(type_id < (u8::BITS as u8)); // fits in bit mask
        1u8 << type_id
    }
}

#[derive(Clone)]
struct ArcFilterChecker<G: Deref + Clone + Send + Sync + 'static> {
    graph: G,
    types: u64, // Bit mask on a SWHType::NUMBER_OF_TYPES × SWHType::NUMBER_OF_TYPES matrix
}

impl<G: Deref + Clone + Send + Sync + 'static> ArcFilterChecker<G>
where
    G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
{
    fn new(graph: G, types: Option<String>) -> Result<Self, tonic::Status> {
        let types = types.unwrap_or("*".to_owned());
        Ok(ArcFilterChecker {
            graph,
            types: match types.as_str() {
                "*" => u64::MAX, // all bits set, match any arc
                "" => 0,         // no bits set, match no arc
                _ => {
                    types
                        .split(",")
                        .map(parse_arc_type)
                        .collect::<Result<Vec<_>, _>>()? // Fold errors
                        .into_iter()
                        .map(|(src, dst)| match (src, dst) {
                            (None, None) => u64::MAX, // arc '*:*' -> set all bits
                            (None, Some(dst)) => SWHType::all()
                                .into_iter()
                                .map(|src| Self::bit_mask(src, dst))
                                .sum(),
                            (Some(src), None) => SWHType::all()
                                .into_iter()
                                .map(|dst| Self::bit_mask(src, dst))
                                .sum(),
                            (Some(src), Some(dst)) => Self::bit_mask(src, dst),
                        })
                        .sum()
                }
            },
        })
    }

    fn matches(&self, src_node: usize, dst_node: usize) -> bool {
        self.types == u64::MAX
            || ((Self::bit_mask(
                self.graph.properties().node_type(src_node).unwrap(),
                self.graph.properties().node_type(dst_node).unwrap(),
            ) & self.types)
                != 0)
    }

    fn bit_mask(src_node_type: SWHType, dst_node_type: SWHType) -> u64 {
        let src_type_id = src_node_type as u64;
        let dst_type_id = dst_node_type as u64;
        let arc_type_id = src_type_id * (SWHType::NUMBER_OF_TYPES as u64) + dst_type_id;
        assert!(arc_type_id < (u64::BITS as u64)); // fits in bit mask
        1u64 << arc_type_id
    }
}

/// Bit masks selecting which fields should be included by [`NodeBuilder`], based on
/// [`proto::FieldMask`].
///
/// As nodes are recursive structures, we include a sub-structure (eg. "successor")
/// iff any of that sub-structure's fields are selected (eg. "successor.swhid" and
/// "successor.label").
#[rustfmt::skip]
mod node_builder_bitmasks {
    //                                                                             xx
    pub const SUCCESSOR: u32 =                  0b00000000_00000000_00000000_00000011;
    pub const SUCCESSOR_SWHID: u32 =            0b00000000_00000000_00000000_00000001;
    pub const _SUCCESSOR_LABEL: u32 =           0b00000000_00000000_00000000_00000010;

    //                                                                          x
    pub const NUM_SUCCESSORS: u32 =             0b00000000_00000000_00000000_00010000;

    //                                                               xxxxxxx
    pub const REV: u32 =                        0b00000000_00000000_01111111_00000000;
    pub const REV_AUTHOR: u32 =                 0b00000000_00000000_00000001_00000000;
    pub const REV_AUTHOR_DATE: u32 =            0b00000000_00000000_00000010_00000000;
    pub const REV_AUTHOR_DATE_OFFSET: u32 =     0b00000000_00000000_00000100_00000000;
    pub const REV_COMMITTER: u32 =              0b00000000_00000000_00001000_00000000;
    pub const REV_COMMITTER_DATE: u32 =         0b00000000_00000000_00010000_00000000;
    pub const REV_COMMITTER_DATE_OFFSET: u32 =  0b00000000_00000000_00100000_00000000;
    pub const REV_MESSAGE: u32 =                0b00000000_00000000_01000000_00000000;

    //                                                        xxxxx
    pub const REL: u32 =                        0b00000000_00011111_00000000_00000000;
    pub const REL_AUTHOR: u32 =                 0b00000000_00000001_00000000_00000000;
    pub const REL_AUTHOR_DATE: u32 =            0b00000000_00000010_00000000_00000000;
    pub const REL_AUTHOR_DATE_OFFSET: u32 =     0b00000000_00000100_00000000_00000000;
    pub const REL_NAME: u32 =                   0b00000000_00001000_00000000_00000000;
    pub const REL_MESSAGE: u32 =                0b00000000_00010000_00000000_00000000;

    //                                                  xx
    pub const CNT: u32 =                        0b00000011_00000000_00000000_00000000;
    pub const CNT_LENGTH: u32 =                 0b00000001_00000000_00000000_00000000;
    pub const CNT_IS_SKIPPED: u32 =             0b00000010_00000000_00000000_00000000;

    //                                               x
    pub const ORI: u32 =                        0b00010000_00000000_00000000_00000000;
    pub const ORI_URL: u32 =                    0b00010000_00000000_00000000_00000000;

    pub const DATA: u32 = REV | REL | CNT | ORI;
}

#[derive(Clone)]
struct NodeBuilder<G: Deref + Clone + Send + Sync + 'static> {
    graph: G,
    // Which fields to include, based on the [`FieldMask`](proto::FieldMask)
    bitmask: u32,
}

impl<G: Deref + Clone + Send + Sync + 'static> NodeBuilder<G>
where
    G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
    <G::Target as SwhGraphWithProperties>::Timestamps:
        properties::TimestampsTrait + properties::TimestampsOption,
    <G::Target as SwhGraphWithProperties>::Persons:
        properties::PersonsTrait + properties::PersonsOption,
    <G::Target as SwhGraphWithProperties>::Contents:
        properties::ContentsTrait + properties::ContentsOption,
    <G::Target as SwhGraphWithProperties>::Strings:
        properties::StringsTrait + properties::StringsOption,
{
    fn new(graph: G, mask: Option<prost_types::FieldMask>) -> Result<Self, tonic::Status> {
        use node_builder_bitmasks::*;
        let Some(mask) = mask else {
            return Ok(NodeBuilder { graph, bitmask: u32::MAX }); // All bits set
        };
        let mut node_builder = NodeBuilder {
            graph,
            bitmask: 0u32, // No bits set
        };
        for field in mask.paths {
            node_builder.bitmask |= match field.as_str() {
                "successor" => SUCCESSOR,
                "successor.swhid" => SUCCESSOR_SWHID,
                "successor.label" => {
                    return Err(tonic::Status::unimplemented(
                        "edge labels are not implemented yet",
                    ));
                }
                "num_successors" => NUM_SUCCESSORS,
                "cnt" => CNT,
                "cnt.length" => CNT_LENGTH,
                "cnt.is_skipped" => CNT_IS_SKIPPED,
                "rev" => REV,
                "rev.author" => REV_AUTHOR,
                "rev.author_date" => REV_AUTHOR_DATE,
                "rev.author_date_offset" => REV_AUTHOR_DATE_OFFSET,
                "rev.committer" => REV_COMMITTER,
                "rev.committer_date" => REV_COMMITTER_DATE,
                "rev.committer_date_offset" => REV_COMMITTER_DATE_OFFSET,
                "rev.message" => REV_MESSAGE,
                "rel" => REL,
                "rel.author" => REL_AUTHOR,
                "rel.author_date" => REL_AUTHOR_DATE,
                "rel.author_date_offset" => REL_AUTHOR_DATE_OFFSET,
                "rel.name" => REL_NAME,
                "rel.message" => REL_MESSAGE,
                "ori" => ORI,
                "ori.url" => ORI_URL,
                _ => 0, // Ignore unknown fields
            }
        }

        Ok(node_builder)
    }

    fn build_node(&self, node_id: usize) -> proto::Node {
        use node_builder_bitmasks::*;
        let successors: Vec<_> = self.if_mask(SUCCESSOR, || {
            self.graph
                .successors(node_id)
                .into_iter()
                .map(|succ| proto::Successor {
                    swhid: self.if_mask(SUCCESSOR_SWHID, || {
                        Some(self.graph.properties().swhid(succ)?.to_string())
                    }),
                    label: Vec::new(), // Not implemented yet
                })
                .collect()
        });
        proto::Node {
            swhid: self
                .graph
                .properties()
                .swhid(node_id)
                .expect("Unknown node id")
                .to_string(),
            num_successors: self.if_mask(NUM_SUCCESSORS, || {
                Some(
                    if self.bitmask & SUCCESSOR != 0 {
                        // don't need to call .outdegree() as we already have the list of successors
                        successors.len()
                    } else {
                        self.graph.outdegree(node_id)
                    }
                    .try_into()
                    .expect("outdegree overflowed i64"),
                )
            }),
            successor: successors,
            data: self.if_mask(DATA, || {
                match self
                    .graph
                    .properties()
                    .node_type(node_id)
                    .expect("Unknown node id")
                {
                    SWHType::Content => Some(self.build_content_data(node_id)),
                    SWHType::Directory => None,
                    SWHType::Revision => Some(self.build_revision_data(node_id)),
                    SWHType::Release => Some(self.build_release_data(node_id)),
                    SWHType::Snapshot => None,
                    SWHType::Origin => Some(self.build_origin_data(node_id)),
                }
            }),
        }
    }

    fn build_content_data(&self, node_id: usize) -> proto::node::Data {
        use node_builder_bitmasks::*;
        let properties = self.graph.properties();
        proto::node::Data::Cnt(proto::ContentData {
            length: self.if_mask(CNT_LENGTH, || {
                Some(
                    properties
                        .content_length(node_id)?
                        .try_into()
                        .expect("Content length overflowed i64"),
                )
            }),
            is_skipped: self.if_mask(CNT_IS_SKIPPED, || properties.is_skipped_content(node_id)),
        })
    }

    fn build_revision_data(&self, node_id: usize) -> proto::node::Data {
        use node_builder_bitmasks::*;
        let properties = self.graph.properties();
        proto::node::Data::Rev(proto::RevisionData {
            author: self.if_mask(REV_AUTHOR, || Some(properties.author_id(node_id)? as i64)),
            author_date: self.if_mask(REV_AUTHOR_DATE, || properties.author_timestamp(node_id)),
            author_date_offset: self.if_mask(REV_AUTHOR_DATE_OFFSET, || {
                Some(properties.author_timestamp_offset(node_id)?.into())
            }),
            committer: self.if_mask(
                REV_AUTHOR,
                || Some(properties.committer_id(node_id)? as i64),
            ),
            committer_date: self
                .if_mask(REV_AUTHOR_DATE, || properties.committer_timestamp(node_id)),
            committer_date_offset: self.if_mask(REV_AUTHOR_DATE_OFFSET, || {
                Some(properties.committer_timestamp_offset(node_id)?.into())
            }),
            message: self.if_mask(REV_MESSAGE, || properties.message(node_id)),
        })
    }
    fn build_release_data(&self, node_id: usize) -> proto::node::Data {
        use node_builder_bitmasks::*;
        let properties = self.graph.properties();
        proto::node::Data::Rel(proto::ReleaseData {
            author: self.if_mask(REL_AUTHOR, || Some(properties.author_id(node_id)? as i64)),
            author_date: self.if_mask(REL_AUTHOR_DATE, || properties.author_timestamp(node_id)),
            author_date_offset: self.if_mask(REL_AUTHOR_DATE_OFFSET, || {
                Some(properties.author_timestamp_offset(node_id)?.into())
            }),
            name: self.if_mask(REL_NAME, || properties.tag_name(node_id)),
            message: self.if_mask(REL_MESSAGE, || properties.message(node_id)),
        })
    }
    fn build_origin_data(&self, node_id: usize) -> proto::node::Data {
        use node_builder_bitmasks::*;
        let properties = self.graph.properties();
        proto::node::Data::Ori(proto::OriginData {
            url: self.if_mask(ORI_URL, || {
                Some(String::from_utf8_lossy(&properties.message(node_id)?).into())
            }),
        })
    }

    fn if_mask<T: Default>(&self, mask: u32, f: impl FnOnce() -> T) -> T {
        if self.bitmask & mask == 0 {
            T::default()
        } else {
            f()
        }
    }
}

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
        mut on_node: impl FnMut(usize) -> Result<(), Error> + Send + 'a,
        mut on_arc: impl FnMut(usize, usize) -> Result<(), Error> + Send + 'a,
    ) -> Result<
        traversal::SimpleBfsVisitor<
            G::Target,
            G,
            Error,
            impl FnMut(usize, u64) -> Result<VisitFlow, Error>,
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
            move |node, depth| {
                if !return_node_checker.matches(node) {
                    return Ok(VisitFlow::Continue);
                }

                if (graph.outdegree(node) as u64) > max_edges {
                    return Ok(VisitFlow::Stop);
                }
                max_edges -= graph.outdegree(node) as u64;

                num_matching_nodes += 1;
                if num_matching_nodes > max_matching_nodes {
                    return Ok(VisitFlow::Stop);
                }
                if depth >= min_depth {
                    on_node(node)?;
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
        let on_node = move |node| tx.blocking_send(Ok(node_builder.build_node(node)));
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
        {
            let count_ref = &mut count;
            let on_node = move |_node| match count_ref.checked_add(1) {
                Some(new_count) => {
                    *count_ref = new_count;
                    Ok(())
                }
                None => Err(tonic::Status::resource_exhausted(
                    "Node count overflowed i64",
                )),
            };
            let on_arc = |_src, _dst| Ok(());

            // Spawning a thread because Tonic currently only supports Tokio, which requires
            // futures to be sendable between threads, and webgraph's successor iterators cannot
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
        }

        Ok(Response::new(proto::CountResponse { count }))
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
