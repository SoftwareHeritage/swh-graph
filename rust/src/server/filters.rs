// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;

use crate::graph::{SwhForwardGraph, SwhGraphWithProperties};
use crate::properties;
use crate::SWHType;

use super::proto;

/// Parses `*`, `cnt`, `dir`, `rev`, `rel`, `snp`, `ori` to [`SWHType`]
fn parse_node_type(type_name: &str) -> Result<SWHType, tonic::Status> {
    SWHType::try_from(type_name)
        .map_err(|_| tonic::Status::invalid_argument(format!("Invalid node type: {}", type_name)))
}

/// Parses comma -separated `src:dst` pairs of node types (see [`parse_node_type`]).
///
/// Returns `*` if the node type is `*`
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
pub struct NodeFilterChecker<G: Deref + Clone + Send + Sync + 'static> {
    graph: G,
    types: u8, // Bit mask
    min_traversal_successors: u64,
    max_traversal_successors: u64,
}

impl<G: Deref + Clone + Send + Sync + 'static> NodeFilterChecker<G>
where
    G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
{
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
                None => u64::MAX,
                Some(max_succ) => max_succ.try_into().map_err(|_| {
                    tonic::Status::invalid_argument(
                        "max_traversal_successors must be a positive integer",
                    )
                })?,
            },
        })
    }

    pub fn matches(&self, node: usize, num_traversal_successors: u64) -> bool {
        self.min_traversal_successors <= num_traversal_successors
            && num_traversal_successors <= self.max_traversal_successors
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
pub(super) struct ArcFilterChecker<G: Deref + Clone + Send + Sync + 'static> {
    graph: G,
    types: u64, // Bit mask on a SWHType::NUMBER_OF_TYPES × SWHType::NUMBER_OF_TYPES matrix
}

impl<G: Deref + Clone + Send + Sync + 'static> ArcFilterChecker<G>
where
    G::Target: SwhForwardGraph + SwhGraphWithProperties + Sized,
    <G::Target as SwhGraphWithProperties>::Maps: properties::MapsTrait + properties::MapsOption,
{
    pub fn new(graph: G, types: Option<String>) -> Result<Self, tonic::Status> {
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

    pub fn matches(&self, src_node: usize, dst_node: usize) -> bool {
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
