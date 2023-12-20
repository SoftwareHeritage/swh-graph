// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;
use std::path::Path;

use crate::graph::*;
use crate::properties;

/// A view over [`SwhGraph`] and related trait, that flips the direction of all arcs
pub struct Transposed<G: Deref>(pub G)
where
    G::Target: SwhGraph;

impl<G: Deref> SwhGraph for Transposed<G>
where
    G::Target: SwhGraph,
{
    fn path(&self) -> &Path {
        self.0.path()
    }
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.0.num_arcs()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(dst_node_id, src_node_id)
    }
}
impl<G: Deref> SwhForwardGraph for Transposed<G>
where
    G::Target: SwhBackwardGraph,
{
    type Successors<'succ> = <<G as Deref>::Target as SwhBackwardGraph>::Predecessors<'succ> where Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.0.predecessors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.0.indegree(node_id)
    }
}
impl<G: Deref> SwhBackwardGraph for Transposed<G>
where
    G::Target: SwhForwardGraph,
{
    type Predecessors<'succ> = <<G as Deref>::Target as SwhForwardGraph>::Successors<'succ> where Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.0.successors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.0.outdegree(node_id)
    }
}
impl<G: Deref> SwhGraphWithProperties for Transposed<G>
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
        self.0.properties()
    }
}
