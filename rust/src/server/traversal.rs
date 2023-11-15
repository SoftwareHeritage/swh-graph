// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;

use crate::graph::SwhForwardGraph;

#[derive(Debug)]
pub enum VisitFlow {
    /// Keep browsing after this node/arc
    Continue,
    /// Ignore this node/arc for traversal (ie. don't visit its successors, at least
    /// not yet)
    Ignore,
    /// End the visit immediately
    Stop,
}

const DEPTH_SENTINEL: usize = usize::MAX;

/// A simple traversal.
///
/// For each node (resp. arc), `on_node` (resp. `on_arc`) is called with each node (resp. arc)
/// the current depth, and it affects the next visited node based on which [`VisitFlow`] it returns.
///
/// For each node, `on_arc` is first called on all outgoing edges, then `on_node` is called
/// for that node, with the number of arcs that were not ignored by `on_arc` as last parameter.
#[derive(Debug)]
pub struct SimpleBfsVisitor<
    IG: SwhForwardGraph,
    G: Deref<Target = IG> + Clone,
    Error,
    OnNode: FnMut(usize, u64, u64) -> Result<VisitFlow, Error>,
    OnArc: FnMut(usize, usize, u64) -> Result<VisitFlow, Error>,
> {
    graph: G,
    queue: std::collections::VecDeque<usize>,
    seen: std::collections::HashSet<usize>,
    depth: u64,
    max_depth: u64,
    on_node: OnNode,
    on_arc: OnArc,
}

impl<
        IG: SwhForwardGraph,
        G: Deref<Target = IG> + Clone,
        Error,
        OnNode: FnMut(usize, u64, u64) -> Result<VisitFlow, Error>,
        OnArc: FnMut(usize, usize, u64) -> Result<VisitFlow, Error>,
    > SimpleBfsVisitor<IG, G, Error, OnNode, OnArc>
{
    /// Initializes a new visit
    ///
    /// Arguments:
    ///
    /// * `g`: the graph to be visited
    /// * `max_depth` how deep inside the graph to recurse
    /// * `on_node`/`on_arc`: function called on each visited node or arc, which returns
    ///   whether to add an item to the channel (and keep going), keep going, or stop the visit.
    pub fn new(graph: G, max_depth: u64, on_node: OnNode, on_arc: OnArc) -> Self {
        SimpleBfsVisitor {
            graph,
            queue: Default::default(),
            seen: Default::default(),
            depth: 0,
            max_depth,
            on_node,
            on_arc,
        }
    }

    /// Add a node to the list of nodes to visit
    pub fn push(&mut self, node: usize) {
        self.queue.push_back(node)
    }
    /// Remove a node from the list of nodes to visit and return it
    pub fn pop(&mut self) -> Option<usize> {
        self.queue.pop_front()
    }

    /// Returns whether the given node was already visited
    pub fn was_seen(&self, node: usize) -> bool {
        self.seen.contains(&node)
    }
    /// Mark the given node as visited
    pub fn mark_seen(&mut self, node: usize) {
        self.seen.insert(node);
    }

    /// Calls [`Self::visit_step`] until the queue/stack is empty.
    ///
    /// Returns `Some` if the traversal exited before visiting all nodes.
    pub fn visit(mut self) -> Result<(), Error>
    where
        Self: Sized,
    {
        self.push(DEPTH_SENTINEL);
        while let Some(node) = self.pop() {
            if node == DEPTH_SENTINEL {
                self.depth += 1;
                if self.depth > self.max_depth {
                    break;
                }
                if !self.queue.is_empty() {
                    self.push(DEPTH_SENTINEL);
                }
                continue;
            }
            match self.visit_step(node)? {
                VisitFlow::Continue => {}
                VisitFlow::Ignore => panic!("visit_step returned Ignore"),
                VisitFlow::Stop => break,
            }
        }
        Ok(())
    }

    /// Calls [`Self::visit_node`] for the given node.
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_step(&mut self, node: usize) -> Result<VisitFlow, Error> {
        self.visit_node(node)
    }

    /// Called on each node and calls [`Self::visit_arc`] for each outgoing arcs
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_node(&mut self, node: usize) -> Result<VisitFlow, Error> {
        let mut num_successors = 0;
        for successor in self.graph.clone().successors(node) {
            match self.visit_arc(node, successor)? {
                VisitFlow::Continue => num_successors += 1,
                VisitFlow::Ignore => {}
                VisitFlow::Stop => return Ok(VisitFlow::Stop),
            }
        }
        match (self.on_node)(node, self.depth, num_successors)? {
            VisitFlow::Continue => Ok(VisitFlow::Continue),
            VisitFlow::Ignore => panic!("on_node returned VisitFlow::Ignore"),
            VisitFlow::Stop => return Ok(VisitFlow::Stop),
        }
    }

    /// Called on each arc, and queues the destination.
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_arc(&mut self, src: usize, dst: usize) -> Result<VisitFlow, Error> {
        match (self.on_arc)(src, dst, self.depth)? {
            VisitFlow::Continue => {}
            VisitFlow::Ignore => return Ok(VisitFlow::Ignore),
            VisitFlow::Stop => return Ok(VisitFlow::Stop),
        }
        if !self.was_seen(dst) {
            self.mark_seen(dst);
            self.push(dst);
        }
        Ok(VisitFlow::Continue)
    }
}
