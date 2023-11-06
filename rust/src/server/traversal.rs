// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;

use crate::graph::SwhForwardGraph;

/// A simple traversal.
///
/// For each node (resp. arc), `on_node` (resp. `on_arc`) is called. If it returns
/// `Err`, the traversal is immediately stopped and this error is returned.
/// If it returns `true`, the traversal recurses.
/// If it returns `false`, this node (resp. arc) is ignored for the traversal
/// (ie. its successors are not traversed, at least not yet).
#[derive(Debug)]
pub struct SimpleBfsVisitor<
    IG: SwhForwardGraph,
    G: Deref<Target = IG> + Clone,
    Error,
    OnNode: FnMut(usize) -> Result<bool, Error>,
    OnArc: FnMut(usize, usize) -> Result<bool, Error>,
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
        OnNode: FnMut(usize) -> Result<bool, Error>,
        OnArc: FnMut(usize, usize) -> Result<bool, Error>,
    > SimpleBfsVisitor<IG, G, Error, OnNode, OnArc>
{
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
            self.visit_step(node)?;
        }
        Ok(())
    }

    /// Calls [`Self::visit_node`] for the given node.
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_step(&mut self, node: usize) -> Result<(), Error> {
        self.visit_node(node)
    }

    /// Called on each node and calls [`Self::visit_arc`] for each outgoing arcs
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_node(&mut self, node: usize) -> Result<(), Error> {
        if (self.on_node)(node)? {
            for successor in self.graph.clone().successors(node) {
                self.visit_arc(node, successor)?;
            }
        }

        Ok(())
    }

    /// Called on each arc, and queues the destination.
    ///
    /// Returns `Err` if the visit should stop after this step
    pub fn visit_arc(&mut self, src: usize, dst: usize) -> Result<(), Error> {
        if (self.on_arc)(src, dst)? {
            if !self.was_seen(dst) {
                self.mark_seen(dst);
                self.push(dst);
            }
        }

        Ok(())
    }
}
