// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, OnceLock};

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
    src: Vec<usize>,
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
        for src_item in src {
            visitor.push(src_item);
        }
        Ok(visitor)
    }

    fn path_from_visit(
        &self,
        parents: &HashMap<usize, usize>,
        found_target: usize,
        target_depth: u64,
    ) -> Result<Vec<usize>, tonic::Status> {
        let target_depth = (target_depth + 1)
            .try_into()
            .map_err(|_| tonic::Status::resource_exhausted("path exhausted usize"))?;
        let mut path = Vec::with_capacity(target_depth);
        let mut current_node = found_target;
        loop {
            let Some(&next_node) = parents.get(&current_node) else {
                return Err(tonic::Status::unknown(
                    "Missing parents while building path",
                ));
            };
            path.push(current_node);
            if next_node == usize::MAX {
                // Reached src or dst node, this is the end of the path
                break;
            }
            current_node = next_node;
            if path.len() > target_depth + 1 {
                return Err(tonic::Status::unknown(
                    "Returned path is unexpectedly longer than the traversal depth",
                ));
            }
        }

        Ok(path)
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

        let direction: proto::GraphDirection = direction
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Invalid direction"))?;

        let src_ids: Vec<_> = src
            .iter()
            .map(|n| self.service.try_get_node_id(n))
            .collect::<Result<_, _>>()?;

        let visitor_config = VisitorConfig {
            src: src_ids.clone(),
            edges,
            max_edges,
            max_depth,
        };
        let target = target.ok_or(tonic::Status::invalid_argument("target must be provided"))?;

        let graph = self.service.0.clone();

        let arc_checker = ArcFilterChecker::new(graph.clone(), request.get_ref().edges.clone())?;
        let target_checker = NodeFilterChecker::new(graph.clone(), target)?;
        let node_builder = NodeBuilder::new(
            graph.clone(),
            arc_checker,
            mask.map(|mask| prost_types::FieldMask {
                paths: mask
                    .paths
                    .iter()
                    .flat_map(|field| field.strip_prefix("node."))
                    .map(|field| field.to_owned())
                    .collect(),
            }),
        )?;

        let mut parents: HashMap<usize, usize> =
            src_ids.into_iter().map(|n| (n, usize::MAX)).collect();
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

        match direction {
            proto::GraphDirection::Forward => {
                let visitor = self.make_visitor(visitor_config, graph, on_node, on_arc)?;
                visitor.visit()?;
            }
            proto::GraphDirection::Backward => {
                let visitor = self.make_visitor(
                    visitor_config,
                    Arc::new(Transposed(graph)),
                    on_node,
                    on_arc,
                )?;
                visitor.visit()?;
            }
        }

        match found_target {
            Some(found_target) => {
                let target_depth = target_depth.unwrap(); // was set at the same time as found_target
                let path = self
                    .path_from_visit(&parents, found_target, target_depth)?
                    .into_iter()
                    .map(|node_id| node_builder.build_node(node_id))
                    .rev() // Reverse order to be src->target
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

    /**
     * FindPathBetween searches for a shortest path between a set of source nodes and a set of
     * destination nodes.
     *
     * It does so by performing a *bidirectional breadth-first search*, i.e., two parallel breadth-first
     * searches, one from the source set ("src-BFS") and one from the destination set ("dst-BFS"), until
     * both searches find a common node that joins their visited sets. This node is called the "midpoint
     * node". The path returned is the path src -> ... -> midpoint -> ... -> dst, which is always a
     * shortest path between src and dst.
     *
     * The graph direction of both BFS can be configured separately. By default, the dst-BFS will use
     * the graph in the opposite direction than the src-BFS (if direction = FORWARD, by default
     * direction_reverse = BACKWARD, and vice-versa). The default behavior is thus to search for a
     * shortest path between two nodes in a given direction. However, one can also specify FORWARD or
     * BACKWARD for *both* the src-BFS and the dst-BFS. This will search for a common descendant or a
     * common ancestor between the two sets, respectively. These will be the midpoints of the returned
     * path.
     */
    pub async fn find_path_between(
        &self,
        request: Request<proto::FindPathBetweenRequest>,
    ) -> TonicResult<proto::Path> {
        let proto::FindPathBetweenRequest {
            src,
            dst,
            direction,
            direction_reverse,
            edges,
            edges_reverse,
            max_edges,
            max_depth,
            mask,
        } = request.get_ref().clone();

        let direction: proto::GraphDirection = direction
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Invalid direction"))?;
        let direction_reverse = direction_reverse
            .unwrap_or(
                // defaults to the opposite direction
                match direction {
                    proto::GraphDirection::Forward => proto::GraphDirection::Backward,
                    proto::GraphDirection::Backward => proto::GraphDirection::Forward,
                }
                .into(),
            )
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument("Invalid direction_reverse"))?;

        let src_ids: Vec<_> = src
            .iter()
            .map(|n| self.service.try_get_node_id(n))
            .collect::<Result<_, _>>()?;
        let dst_ids: Vec<_> = dst
            .iter()
            .map(|n| self.service.try_get_node_id(n))
            .collect::<Result<_, _>>()?;

        let visitor_config = VisitorConfig {
            src: src_ids.clone(),
            edges: edges.clone(),
            max_edges,
            max_depth,
        };
        let reverse_visitor_config = VisitorConfig {
            src: dst_ids.clone(),
            edges: edges_reverse.or_else(|| {
                // If edges_reverse is not specified:
                // - If `edges` is not specified either, defaults to "*"
                // - If direction == direction_reverse, defaults to `edges`
                // - If direction != direction_reverse, defaults
                //   to the reverse of `edges` (e.g. "rev:dir" becomes "dir:rev").
                if direction == direction_reverse {
                    edges
                } else {
                    edges.map(|edges| {
                        edges
                            .split(',')
                            .map(|s| s.split(':').rev().join(":"))
                            .join(",")
                    })
                }
            }),
            max_edges,
            max_depth,
        };

        let graph = self.service.0.clone();

        let arc_checker = ArcFilterChecker::new(graph.clone(), request.get_ref().edges.clone())?;
        let node_builder = NodeBuilder::new(
            graph.clone(),
            arc_checker,
            mask.map(|mask| prost_types::FieldMask {
                paths: mask
                    .paths
                    .iter()
                    .flat_map(|field| field.strip_prefix("node."))
                    .map(|field| field.to_owned())
                    .collect(),
            }),
        )?;

        // Technically we don't need locks because the closures are called sequentially,
        // but I don't see a way around it without unsafe{}.
        let parents: HashMap<usize, usize> = src_ids.into_iter().map(|n| (n, usize::MAX)).collect();
        let parents = Mutex::new(parents);
        let parents_reverse: HashMap<usize, usize> =
            dst_ids.into_iter().map(|n| (n, usize::MAX)).collect();
        let parents_reverse = Mutex::new(parents_reverse);
        let mut found_midpoint = OnceLock::new();
        let mut max_midpoint_depth = OnceLock::new();

        let on_node = |node, depth, _num_successors| {
            if parents_reverse.lock().unwrap().contains_key(&node) {
                found_midpoint.set(node).expect("Set found_midpoint twice");
                max_midpoint_depth
                    .set(depth)
                    .expect("Set max_midpoint_depth twice");
                Ok::<_, NeverError>(VisitFlow::Stop)
            } else {
                Ok(VisitFlow::Continue)
            }
        };
        let on_node_reverse = |node, depth, _num_successors| {
            if parents.lock().unwrap().contains_key(&node) {
                found_midpoint.set(node).expect("Set found_midpoint twice");
                max_midpoint_depth
                    .set(depth)
                    .expect("Set max_midpoint_depth twice");
                Ok::<_, NeverError>(VisitFlow::Stop)
            } else {
                Ok(VisitFlow::Continue)
            }
        };

        let on_arc = |src, dst| {
            parents.lock().unwrap().entry(dst).or_insert(src);
            Ok(VisitFlow::Continue)
        };
        let on_arc_reverse = |src, dst| {
            parents_reverse.lock().unwrap().entry(dst).or_insert(src);
            Ok(VisitFlow::Continue)
        };

        match direction {
            proto::GraphDirection::Forward => {
                let mut visitor =
                    self.make_visitor(visitor_config, graph.clone(), on_node, on_arc)?;
                match direction_reverse {
                    proto::GraphDirection::Forward => {
                        let mut visitor_reverse = self.make_visitor(
                            reverse_visitor_config,
                            graph,
                            on_node_reverse,
                            on_arc_reverse,
                        )?;
                        let mut more_layers = true;
                        while more_layers {
                            more_layers = false;
                            more_layers |= visitor.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                            more_layers |= visitor_reverse.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                        }
                    }
                    proto::GraphDirection::Backward => {
                        let mut visitor_reverse = self.make_visitor(
                            reverse_visitor_config,
                            Arc::new(Transposed(graph)),
                            on_node_reverse,
                            on_arc_reverse,
                        )?;
                        let mut more_layers = true;
                        while more_layers {
                            more_layers = false;
                            more_layers |= visitor.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                            more_layers |= visitor_reverse.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                        }
                    }
                }
            }
            proto::GraphDirection::Backward => {
                let mut visitor = self.make_visitor(
                    visitor_config,
                    Arc::new(Transposed(graph.clone())),
                    on_node,
                    on_arc,
                )?;
                match direction_reverse {
                    proto::GraphDirection::Forward => {
                        let mut visitor_reverse = self.make_visitor(
                            reverse_visitor_config,
                            graph,
                            on_node_reverse,
                            on_arc_reverse,
                        )?;
                        let mut more_layers = true;
                        while more_layers {
                            more_layers = false;
                            more_layers |= visitor.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                            more_layers |= visitor_reverse.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                        }
                    }
                    proto::GraphDirection::Backward => {
                        let mut visitor_reverse = self.make_visitor(
                            reverse_visitor_config,
                            Arc::new(Transposed(graph)),
                            on_node_reverse,
                            on_arc_reverse,
                        )?;
                        let mut more_layers = true;
                        while more_layers {
                            more_layers = false;
                            more_layers |= visitor.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                            more_layers |= visitor_reverse.visit_layer()?;
                            if found_midpoint.get().is_some() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        match found_midpoint.take() {
            Some(found_midpoint) => {
                let max_midpoint_depth = max_midpoint_depth.take().unwrap(); // was set at the same time as found_midpoint
                let mut path = Vec::with_capacity((max_midpoint_depth * 2 + 1) as usize);
                path.extend(
                    self.path_from_visit(
                        &parents.into_inner().unwrap(),
                        found_midpoint,
                        max_midpoint_depth,
                    )?
                    .into_iter()
                    .rev() // Reverse order to be src->midpoint
                    .map(|node_id| node_builder.build_node(node_id)),
                );
                let midpoint_index = path.len() - 1;
                path.extend(
                    self.path_from_visit(
                        &parents_reverse.into_inner().unwrap(),
                        found_midpoint,
                        max_midpoint_depth,
                    )?
                    .into_iter()
                    .skip(1)
                    .map(|node_id| node_builder.build_node(node_id)),
                );

                Ok(Response::new(proto::Path {
                    node: path,
                    midpoint_index: Some(midpoint_index as i32),
                }))
            }
            None => {
                let sources = if src.len() < 5 {
                    src.iter().join(", ")
                } else {
                    src[0..5].iter().chain([&"...".to_owned()]).join(", ")
                };
                let destinations = if dst.len() < 5 {
                    dst.iter().join(", ")
                } else {
                    dst[0..5].iter().chain([&"...".to_owned()]).join(", ")
                };
                Err(tonic::Status::not_found(format!(
                    "Could not find a path from the sources ({}) to any destination ({})",
                    sources, destinations
                )))
            }
        }
    }
}