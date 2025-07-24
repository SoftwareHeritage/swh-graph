// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::borrow::Borrow;
use std::collections::VecDeque;

use swh_graph::graph::*;

use crate::collections::{AdaptiveNodeSet, NodeSet, ReadNodeSet};

/// Stateful BFS (breadth-first search) visit of (a part of) the Software
/// Heritage graph, returning deduplicated node identifiers.
///
/// This visit uses [NodeId]s ([usize] integers) to keep track of visited and
/// to be visited nodes. As such it is suitable for small- to medium-sized
/// visits.  For large-sized graph visits (e.g., the entire graph or close to
/// that) it is preferable to use bit vectors (incurring the price of a higher
/// startup cost to allocate the bit vector.
pub struct NodeVisit<'a, G>
where
    G: SwhForwardGraph,
{
    graph: &'a G,
    visited: AdaptiveNodeSet,
    queue: VecDeque<NodeId>,
}

impl<'a, G> NodeVisit<'a, G>
where
    G: SwhForwardGraph,
{
    fn new<I: IntoIterator<Item: Borrow<NodeId>>>(graph: &'a G, nodes: I) -> Self {
        NodeVisit {
            graph,
            visited: AdaptiveNodeSet::new(graph.num_nodes()),
            queue: nodes.into_iter().map(|item| *item.borrow()).collect(),
        }
    }
}

impl<G> Iterator for NodeVisit<'_, G>
where
    G: SwhForwardGraph,
{
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current_node) = self.queue.pop_front() {
            for succ in self.graph.successors(current_node) {
                if !self.visited.contains(succ) {
                    self.queue.push_back(succ);
                    self.visited.insert(succ);
                }
            }
            Some(current_node)
        } else {
            None
        }
    }
}

/// Iterate on the nodes of the sub-graph rooted at `start`, in BFS order.
///
/// `start` is usually a single node id, passed as `&[node]`, but can be a
/// non-singleton slice or iterator of nodes, to avoid independently re-visiting shared
/// sub-graphs from multiple starting points.
///
/// See [NodeVisit] documentation for performance considerations.
///
/// # Examples
///
/// ```
/// # use swh_graph::graph::SwhForwardGraph;
/// # use swh_graph_stdlib::iter_nodes;
/// #
/// # fn f<G: SwhForwardGraph>(graph: &G) {
/// let src = &[1, 2, 3];
/// for node in iter_nodes(graph, src) {
///     println!("Visiting: {}", node);
/// }
/// # }
/// ```
///
/// ```
/// # use swh_graph::graph::SwhForwardGraph;
/// # use swh_graph_stdlib::iter_nodes;
/// #
/// # fn f<G: SwhForwardGraph>(graph: &G) {
/// let src = 1..4;
/// for node in iter_nodes(graph, src) {
///     println!("Visiting: {}", node);
/// }
/// # }
/// ```
pub fn iter_nodes<G, I: IntoIterator<Item: Borrow<NodeId>>>(graph: &G, start: I) -> NodeVisit<'_, G>
where
    G: SwhForwardGraph,
{
    NodeVisit::new(graph, start)
}
