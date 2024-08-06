// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::VecDeque;

use crate::collections::{AdaptiveNodeSet, NodeSet};
use crate::graph::*;
use crate::properties;

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
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
{
    graph: &'a G,
    visited: AdaptiveNodeSet,
    queue: VecDeque<NodeId>,
}

impl<'a, G> NodeVisit<'a, G>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
{
    pub fn new(graph: &'a G, nodes: &[NodeId]) -> Self {
        NodeVisit {
            graph,
            visited: AdaptiveNodeSet::new(graph.num_nodes()),
            queue: VecDeque::from_iter(nodes.iter().copied()),
        }
    }
}

impl<'a, G> Iterator for NodeVisit<'a, G>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
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
/// non-singleton slice of nodes, to avoid independently re-visiting shared
/// sub-graphs from multiple starting points.
pub fn iter_nodes<'a, G>(graph: &'a G, start: &[NodeId]) -> NodeVisit<'a, G>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
{
    NodeVisit::new(graph, start)
}
