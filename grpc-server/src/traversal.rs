// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use cadence::Counted;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response};

use swh_graph::graph::{SwhForwardGraph, SwhGraphWithProperties};
use swh_graph::properties;
use swh_graph::views::{Subgraph, Transposed};

use super::filters::{ArcFilterChecker, NodeFilterChecker};
use super::node_builder::NodeBuilder;
use super::proto;
use super::visitor::{SimpleBfsVisitor, VisitFlow};
use super::{scoped_spawn_blocking, TraversalServiceTrait};

use crate::utils::OnDrop;

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;

/// Implementation of `Traverse`, `CountNodes`, and `CountEdges` methods of the
/// [`TraversalService`](super::proto::TraversalService)
pub struct SimpleTraversal<'s, S: TraversalServiceTrait + Sync + 'static> {
    pub service: &'s S,
}

impl<S: TraversalServiceTrait + Sync> SimpleTraversal<'_, S> {
    #[allow(clippy::result_large_err)] // this is called by implementations of Tonic traits, which can't return Result<_, Box<Status>>
    #[allow(clippy::type_complexity)]
    fn make_visitor<
        'a,
        G: SwhForwardGraph + SwhGraphWithProperties + Clone + Send + Sync + 'static,
        Error: Send + 'a,
    >(
        &'a self,
        request: Request<proto::TraversalRequest>,
        graph: G,
        mut on_node: impl FnMut(usize, Option<u64>) -> Result<(), Error> + Send + 'a,
        mut on_arc: impl FnMut(usize, usize) -> Result<(), Error> + Send + 'a,
    ) -> Result<
        SimpleBfsVisitor<
            Arc<Subgraph<G, impl Fn(usize) -> bool, impl Fn(usize, usize) -> bool>>,
            Error,
            impl FnMut(usize, u64, Option<u64>) -> Result<VisitFlow, Error>,
            impl FnMut(usize, usize, u64) -> Result<VisitFlow, Error>,
        >,
        tonic::Status,
    >
    where
        <G as SwhGraphWithProperties>::Maps: properties::Maps,
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
        let subgraph =
            Subgraph::with_arc_filter(graph.clone(), move |src, dst| arc_checker.matches(src, dst));
        let mut visitor = SimpleBfsVisitor::new(
            Arc::new(subgraph),
            max_depth,
            move |node, depth, num_successors| {
                if !return_node_checker.matches(node, num_successors) {
                    return Ok(VisitFlow::Continue);
                }

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
                if max_edges == 0 {
                    Ok(VisitFlow::Ignore)
                } else {
                    max_edges -= 1;
                    on_arc(src, dst)?;
                    Ok(VisitFlow::Continue)
                }
            },
        );
        for src_item in &src {
            visitor.push(self.service.try_get_node_id(src_item)?);
        }
        Ok(visitor)
    }

    pub async fn traverse(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<ReceiverStream<Result<proto::Node, tonic::Status>>> {
        let graph = self.service.graph().clone();
        let statsd_client = self.service.statsd_client().cloned();
        let mut num_returned_nodes = OnDrop::new(0, move |num_returned_nodes| {
            // Runs at the end of the traversal, when visitor (hence the 'on_node' closure)
            // is dropped
            if let Some(statsd_client) = &statsd_client {
                statsd_client
                    .count_with_tags("traversal_returned_nodes_total", *num_returned_nodes)
                    .send();
            }
        });

        let (tx, rx) = mpsc::channel(1_000);

        macro_rules! traverse {
            ($graph: expr) => {{
                let graph = $graph;
                let arc_checker =
                    ArcFilterChecker::new(graph.clone(), request.get_ref().edges.clone())?;
                let subgraph = Arc::new(Subgraph::with_arc_filter(graph, move |src, dst| {
                    arc_checker.matches(src, dst)
                }));
                let node_builder =
                    NodeBuilder::new(subgraph.clone(), request.get_ref().mask.clone())?;
                let on_node = move |node, _num_successors| {
                    *num_returned_nodes += 1;
                    tx.blocking_send(Ok(node_builder.build_node(node)))
                };
                let on_arc = |_src, _dst| Ok(());
                let visitor = self.make_visitor(request, subgraph, on_node, on_arc)?;
                // Spawning a thread because Tonic currently only supports Tokio, which
                // requires futures to be sendable between threads, and webgraph's
                // successor iterators are not
                tokio::task::spawn_blocking(|| visitor.visit())
            }};
        }
        match request.get_ref().direction.try_into() {
            Ok(proto::GraphDirection::Forward) => {
                traverse!(graph);
            }
            Ok(proto::GraphDirection::Backward) => {
                let graph = Arc::new(Transposed(graph));
                traverse!(graph);
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    pub async fn count_nodes(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        let graph = self.service.graph().clone();

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
                let visitor = self.make_visitor(request, graph, on_node, on_arc)?;
                scoped_spawn_blocking(|| visitor.visit())?;
            }
            Ok(proto::GraphDirection::Backward) => {
                let graph = Arc::new(Transposed(graph));
                let visitor = self.make_visitor(request, graph, on_node, on_arc)?;
                scoped_spawn_blocking(|| visitor.visit())?;
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }

        Ok(Response::new(proto::CountResponse { count }))
    }

    pub async fn count_edges(
        &self,
        request: Request<proto::TraversalRequest>,
    ) -> TonicResult<proto::CountResponse> {
        let graph = self.service.graph().clone();

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
                let visitor = self.make_visitor(request, graph, on_node, on_arc)?;
                scoped_spawn_blocking(|| visitor.visit())?;
            }
            Ok(proto::GraphDirection::Backward) => {
                let graph = Arc::new(Transposed(graph));
                let visitor = self.make_visitor(request, graph, on_node, on_arc)?;
                scoped_spawn_blocking(|| visitor.visit())?;
            }
            Err(_) => return Err(tonic::Status::invalid_argument("Invalid direction")),
        }

        Ok(Response::new(proto::CountResponse { count }))
    }
}
