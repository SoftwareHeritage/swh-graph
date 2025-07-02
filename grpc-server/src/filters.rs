// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::graph::{SwhForwardGraph, SwhGraphWithProperties};
use swh_graph::properties;
use swh_graph::{ArcType, NodeType};

use super::proto;

#[inline(always)]
#[allow(clippy::result_large_err)] // it's inlined
/// Parses `*`, `cnt`, `dir`, `rev`, `rel`, `snp`, `ori` to [`NodeType`]
fn parse_node_type(type_name: &str) -> Result<NodeType, tonic::Status> {
    type_name
        .parse()
        .map_err(|_| tonic::Status::invalid_argument(format!("Invalid node type: {type_name}")))
}

#[inline(always)]
#[allow(clippy::result_large_err)] // it's inlined
/// Parses comma -separated `src:dst` pairs of node types (see [`parse_node_type`]).
///
/// Returns `*` if the node type is `*`
fn parse_arc_type(type_name: &str) -> Result<ArcType, tonic::Status> {
    let mut splits = type_name.splitn(2, ':');
    let Some(src_type_name) = splits.next() else {
        return Err(tonic::Status::invalid_argument(format!(
            "Invalid arc type: {type_name} (should not be empty)"
        )));
    };
    let Some(dst_type_name) = splits.next() else {
        return Err(tonic::Status::invalid_argument(format!(
            "Invalid arc type: {type_name} (should have a colon)"
        )));
    };
    let src_type = match src_type_name {
        "*" => None,
        _ => Some(src_type_name.parse().map_err(|_| {
            tonic::Status::invalid_argument(format!("Invalid node type: {src_type_name}"))
        })?),
    };
    let dst_type = match dst_type_name {
        "*" => None,
        _ => Some(dst_type_name.parse().map_err(|_| {
            tonic::Status::invalid_argument(format!("Invalid node type: {dst_type_name}"))
        })?),
    };
    Ok(ArcType {
        src: src_type,
        dst: dst_type,
    })
}

#[derive(Clone)]
pub struct NodeFilterChecker<G: Clone + Send + Sync + 'static> {
    graph: G,
    types: u8, // Bit mask
    min_traversal_successors: u64,
    max_traversal_successors: u64,
}

impl<G: Clone + Send + Sync + 'static> NodeFilterChecker<G>
where
    G: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    #[allow(clippy::result_large_err)] // this is called by implementations of Tonic traits, which can't return Result<_, Box<Status>>
    pub fn new(graph: G, filter: proto::NodeFilter) -> Result<Self, tonic::Status> {
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
                    .split(',')
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
                None => u64::MAX,
                Some(max_succ) => max_succ.try_into().map_err(|_| {
                    tonic::Status::invalid_argument(
                        "max_traversal_successors must be a positive integer",
                    )
                })?,
            },
        })
    }

    pub fn matches(&self, node: usize, num_traversal_successors: Option<u64>) -> bool {
        let num_traversal_successors = num_traversal_successors.unwrap_or(0);
        self.min_traversal_successors <= num_traversal_successors
            && num_traversal_successors <= self.max_traversal_successors
            && (self.types == u8::MAX
                || (Self::bit_mask(self.graph.properties().node_type(node)) & self.types) != 0)
    }

    fn bit_mask(node_type: NodeType) -> u8 {
        let type_id = node_type as u8;
        assert!(type_id < (u8::BITS as u8)); // fits in bit mask
        1u8 << type_id
    }
}

#[derive(Clone)]
pub struct ArcFilterChecker<G: Clone + Send + Sync + 'static> {
    graph: G,
    types: u64, // Bit mask on a NodeType::NUMBER_OF_TYPES × NodeType::NUMBER_OF_TYPES matrix
}

impl<G: Clone + Send + Sync + 'static> ArcFilterChecker<G>
where
    G: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    #[allow(clippy::result_large_err)] // this is called by implementations of Tonic traits, which can't return Result<_, Box<Status>>
    pub fn new(graph: G, types: Option<String>) -> Result<Self, tonic::Status> {
        let types = types.unwrap_or("*".to_owned());
        Ok(ArcFilterChecker {
            graph,
            types: match types.as_str() {
                "*" => u64::MAX, // all bits set, match any arc
                "" => 0,         // no bits set, match no arc
                _ => {
                    types
                        .split(',')
                        .map(parse_arc_type)
                        .collect::<Result<Vec<_>, _>>()? // Fold errors
                        .into_iter()
                        .map(|ArcType { src, dst }| match (src, dst) {
                            (None, None) => u64::MAX, // arc '*:*' -> set all bits
                            (None, Some(dst)) => NodeType::all()
                                .into_iter()
                                .map(|src| Self::bit_mask(src, dst))
                                .sum(),
                            (Some(src), None) => NodeType::all()
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

    pub fn matches(&self, src_node: usize, dst_node: usize) -> bool {
        self.types == u64::MAX
            || ((Self::bit_mask(
                self.graph.properties().node_type(src_node),
                self.graph.properties().node_type(dst_node),
            ) & self.types)
                != 0)
    }

    fn bit_mask(src_node_type: NodeType, dst_node_type: NodeType) -> u64 {
        let src_type_id = src_node_type as u64;
        let dst_type_id = dst_node_type as u64;
        let arc_type_id = src_type_id * (NodeType::NUMBER_OF_TYPES as u64) + dst_type_id;
        assert!(arc_type_id < (u64::BITS as u64)); // fits in bit mask
        1u64 << arc_type_id
    }
}
