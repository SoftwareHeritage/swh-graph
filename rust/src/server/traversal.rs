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

/// A simple traversal.
///
/// For each node (resp. arc), `on_node` (resp. `on_arc`) is called, and it affects the
/// next visited node based on which [`VisitFlow`] it returns.
#[derive(Debug)]
pub struct SimpleBfsVisitor<
    IG: SwhForwardGraph,
    G: Deref<Target = IG> + Clone,
    Error,
    OnNode: FnMut(usize) -> Result<VisitFlow, Error>,
    OnArc: FnMut(usize, usize) -> Result<VisitFlow, Error>,
> {
    graph: G,
    queue: std::collections::VecDeque<usize>,
    seen: std::collections::HashSet<usize>,
    on_node: OnNode,
    on_arc: OnArc,
}

impl<
        IG: SwhForwardGraph,
        G: Deref<Target = IG> + Clone,
        Error,
        OnNode: FnMut(usize) -> Result<VisitFlow, Error>,
        OnArc: FnMut(usize, usize) -> Result<VisitFlow, Error>,
    > SimpleBfsVisitor<IG, G, Error, OnNode, OnArc>
{
    /// Initializes a new visit
    ///
    /// Arguments:
    ///
    /// * `g`: the graph to be visited
    /// * `item_tx`: a channel where to send the result of callback functions
    /// * `on_node`/`on_arc`: function called on each visited node or arc, which returns
    ///   whether to add an item to the channel (and keep going), keep going, or stop the visit.
    pub fn new(graph: G, on_node: OnNode, on_arc: OnArc) -> Self {
        SimpleBfsVisitor {
            graph,
            queue: Default::default(),
            seen: Default::default(),
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
        while let Some(node) = self.pop() {
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
        match (self.on_node)(node)? {
            VisitFlow::Continue => {}
            VisitFlow::Ignore => return Ok(VisitFlow::Continue),
            VisitFlow::Stop => return Ok(VisitFlow::Stop),
        }
        for successor in self.graph.clone().successors(node) {
            match self.visit_arc(node, successor)? {
                VisitFlow::Continue => {}
                VisitFlow::Ignore => panic!("visit_arc returned Ignore"),
                VisitFlow::Stop => return Ok(VisitFlow::Stop),
            }
        }
        Ok(VisitFlow::Continue)
    }

    /// Called on each arc, and queues the destination.
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_arc(&mut self, src: usize, dst: usize) -> Result<VisitFlow, Error> {
        match (self.on_arc)(src, dst)? {
            VisitFlow::Continue => {}
            VisitFlow::Ignore => return Ok(VisitFlow::Continue),
            VisitFlow::Stop => return Ok(VisitFlow::Stop),
        }
        if !self.was_seen(dst) {
            self.mark_seen(dst);
            self.push(dst);
        }
        Ok(VisitFlow::Continue)
    }
}
