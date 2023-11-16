// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;
use std::path::Path;

use crate::graph::*;
use crate::properties;

/// A view over [`SwhGraph`] and related trait, which changes properties based on
/// runtime configuration
pub struct DynamicView<G: Deref>
where
    G::Target: SwhGraph,
{
    pub graph: G,
    pub transposed: bool,
}

impl<G: Deref> DynamicView<G>
where
    G::Target: SwhGraph,
{
    pub fn new(graph: G) -> Self {
        DynamicView {
            graph,
            transposed: false,
        }
    }

    pub fn transpose(&mut self) -> &mut Self {
        self.transposed = !self.transposed;
        self
    }
}

impl<G: Deref> SwhGraph for DynamicView<G>
where
    G::Target: SwhGraph,
{
    fn path(&self) -> &Path {
        self.graph.path()
    }
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        if self.transposed {
            self.graph.has_arc(dst_node_id, src_node_id)
        } else {
            self.graph.has_arc(src_node_id, dst_node_id)
        }
    }
}
impl<G: Deref> SwhForwardGraph for DynamicView<G>
where
    G::Target: SwhForwardGraph + SwhBackwardGraph,
{
    type Successors<'succ> = <<G as Deref>::Target as SwhBackwardGraph>::Successors<'succ> where Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        if self.transposed {
            self.graph.predecessors(node_id)
        } else {
            self.graph.successors(node_id)
        }
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        if self.transposed {
            self.graph.indegree(node_id)
        } else {
            self.graph.outdegree(node_id)
        }
    }
}
impl<G: Deref> SwhBackwardGraph for DynamicView<G>
where
    G::Target: SwhBackwardGraph + SwhForwardGraph,
{
    type Predecessors<'succ> = <<G as Deref>::Target as SwhForwardGraph>::Predecessors<'succ> where Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        if self.transposed {
            self.graph.successors(node_id)
        } else {
            self.graph.predecessors(node_id)
        }
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        if self.transposed {
            self.graph.outdegree(node_id)
        } else {
            self.graph.indegree(node_id)
        }
    }
}
impl<G: Deref> SwhGraphWithProperties for DynamicView<G>
where
    G::Target: SwhGraphWithProperties,
{
    type Maps = <<G as Deref>::Target as SwhGraphWithProperties>::Maps;
    type Timestamps = <<G as Deref>::Target as SwhGraphWithProperties>::Timestamps;
    type Persons = <<G as Deref>::Target as SwhGraphWithProperties>::Persons;
    type Contents = <<G as Deref>::Target as SwhGraphWithProperties>::Contents;
    type Strings = <<G as Deref>::Target as SwhGraphWithProperties>::Strings;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<
        Self::Maps,
        Self::Timestamps,
        Self::Persons,
        Self::Contents,
        Self::Strings,
    > {
        &self.graph.properties()
    }
}
