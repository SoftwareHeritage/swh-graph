// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use tonic::{Request, Response};

use crate::graph::{SwhForwardGraph, SwhGraphWithProperties};
use crate::mph::SwhidMphf;
use crate::properties;
use crate::views::Transposed;

use super::filters::{ArcFilterChecker, NodeFilterChecker};
use super::node_builder::NodeBuilder;
use super::proto;
use super::visitor::{SimpleBfsVisitor, VisitFlow};

/// [Never type](https://github.com/rust-lang/rust/issues/35121)
enum NeverError {}

impl From<NeverError> for tonic::Status {
    fn from(_: NeverError) -> Self {
        unreachable!("NeverError happened")
    }
}

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;

struct VisitorConfig {
    src: Vec<String>,
    edges: Option<String>,
    max_edges: Option<i64>,
    max_depth: Option<i64>,
}

/// Implementation of the `FindPathTo` and `FindPathBetween` methods of the
/// [`TraversalService`](super::proto::TraversalService)
pub struct FindPath<'s, MPHF: SwhidMphf> {
    pub service: &'s super::TraversalService<MPHF>,
}

impl<'s, MPHF: SwhidMphf + Sync + Send + 'static> FindPath<'s, MPHF> {
    fn make_visitor<'a, G: Deref + Clone + Send + Sync + 'static, Error: Send + 'a>(
        &'a self,
        config: VisitorConfig,
        graph: G,
        mut on_node: impl FnMut(usize, u64, u64) -> Result<VisitFlow, Error> + Send + 'a,
        mut on_arc: impl FnMut(usize, usize) -> Result<VisitFlow, Error> + Send + 'a,
    ) -> Result<
        SimpleBfsVisitor<
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
        let VisitorConfig {
            src,
            edges,
            max_edges,
            max_depth,
        } = config;
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

        let arc_checker = ArcFilterChecker::new(graph.clone(), edges)?;
        let mut visitor = SimpleBfsVisitor::new(
            graph.clone(),
            max_depth,
            move |node, depth, num_successors| {
                if num_successors > max_edges {
                    return Ok(VisitFlow::Stop);
                }
                max_edges -= num_successors;

                on_node(node, depth, num_successors)
            },
            move |src, dst, _depth| {
                if arc_checker.matches(src, dst) {
                    on_arc(src, dst)
                } else {
                    Ok(VisitFlow::Ignore)
                }
            },
        );
        for src_item in &src {
            visitor.push(self.service.try_get_node_id(src_item)?);
        }
        Ok(visitor)
    }

    pub async fn find_path_to(
        &self,
        request: Request<proto::FindPathToRequest>,
    ) -> TonicResult<proto::Path> {
        let proto::FindPathToRequest {
            src,
            target,
            direction,
            edges,
            max_edges,
            max_depth,
            mask,
        } = request.get_ref().clone();
        let visitor_config = VisitorConfig {
            src: src.clone(),
            edges,
            max_edges,
            max_depth,
        };
        let target = target.ok_or(tonic::Status::invalid_argument("target must be provided"))?;

        let graph = self.service.0.clone();

        let target_checker = NodeFilterChecker::new(graph.clone(), target)?;
        let node_builder = NodeBuilder::new(
            graph.clone(),
            mask.map(|mask| prost_types::FieldMask {
                paths: mask
                    .paths
                    .iter()
                    .flat_map(|field| field.strip_prefix("node."))
                    .map(|field| field.to_owned())
                    .collect(),
            }),
        )?;

        let mut parents = HashMap::new();
        let mut found_target = None;
        let mut target_depth = None;
        let on_node = |node, depth, num_successors| {
            if target_checker.matches(node, num_successors) {
                found_target = Some(node);
                target_depth = Some(depth);
                Ok::<_, NeverError>(VisitFlow::Stop)
            } else {
                Ok(VisitFlow::Continue)
            }
        };
        let on_arc = |src, dst| {
            parents.entry(dst).or_insert(src);
            Ok(VisitFlow::Continue)
        };

        // Spawning a thread because Tonic currently only supports Tokio, which requires
        // futures to be sendable between threads, and webgraph's successor iterators cannot
        match direction.try_into() {
            Ok(proto::GraphDirection::Forward) => {
                let visitor = self.make_visitor(visitor_config, graph, on_node, on_arc)?;
                visitor.visit()?;
            }
            Ok(proto::GraphDirection::Backward) => {
                let visitor = self.make_visitor(
                    visitor_config,
                    Arc::new(Transposed(graph)),
                    on_node,
                    on_arc,
                )?;
                visitor.visit()?;
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }

        match found_target {
            Some(found_target) => {
                let target_depth = target_depth.unwrap(); // was set at the same time as found_target
                let target_depth = (target_depth + 1)
                    .try_into()
                    .map_err(|_| tonic::Status::resource_exhausted("path exhausted usize"))?;
                let mut path = Vec::with_capacity(target_depth);
                path.push(found_target);
                let mut current_node = found_target;
                let mut src_ids = Vec::with_capacity(src.len());
                for src_swhid in &src {
                    src_ids.push(self.service.try_get_node_id(src_swhid)?);
                }
                while let Some(next_node) = parents.get(&current_node).copied() {
                    current_node = next_node;
                    path.push(current_node);
                    if src_ids.contains(&current_node) {
                        break;
                    }
                    if path.len() > target_depth + 1 {
                        return Err(tonic::Status::unknown(
                            "Returned path is unexpectedly longer than the traversal depth",
                        ));
                    }
                }

                // Inflate node ids into full nodes
                let path = path
                    .into_iter()
                    .rev() // Reverse order to be src->target
                    .map(|node_id| node_builder.build_node(node_id))
                    .collect();

                Ok(Response::new(proto::Path {
                    node: path,
                    midpoint_index: None,
                }))
            }
            None => {
                let sources = if src.len() < 5 {
                    src.iter().join(", ")
                } else {
                    src[0..5].iter().chain([&"...".to_owned()]).join(", ")
                };
                Err(tonic::Status::not_found(format!(
                    "Could not find a path from the sources ({}) to any matching target",
                    sources
                )))
            }
        }
    }
}
