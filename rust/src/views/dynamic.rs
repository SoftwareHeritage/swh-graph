// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::Path;

use crate::graph::*;
use crate::properties;

/// A view over [`SwhGraph`] and related trait, which changes properties based on
/// runtime configuration
pub struct DynamicView<G: SwhGraph> {
    pub graph: G,
    pub transposed: bool,
}

impl<G: SwhGraph> DynamicView<G> {
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

impl<G: SwhGraph> SwhGraph for DynamicView<G> {
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
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        if self.transposed {
            Transposed(self.graph).num_arcs_by_type()
        } else {
            self.graph.num_arcs_by_type()
        }
    }
}
impl<G: SwhForwardGraph + SwhBackwardGraph> SwhForwardGraph for DynamicView<G> {
    type Successors<'succ>
        = <G as SwhBackwardGraph>::Successors<'succ>
    where
        Self: 'succ;

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
impl<G: SwhBackwardGraph + SwhForwardGraph> SwhBackwardGraph for DynamicView<G> {
    type Predecessors<'succ>
        = <G as SwhForwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

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
impl<G: SwhGraphWithProperties> SwhGraphWithProperties for DynamicView<G> {
    type Maps = <G as SwhGraphWithProperties>::Maps;
    type Timestamps = <G as SwhGraphWithProperties>::Timestamps;
    type Persons = <G as SwhGraphWithProperties>::Persons;
    type Contents = <G as SwhGraphWithProperties>::Contents;
    type Strings = <G as SwhGraphWithProperties>::Strings;

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
