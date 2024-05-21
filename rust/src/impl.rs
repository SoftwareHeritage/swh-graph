// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Boring implementations of `SwhGraph*` traits

use std::ops::Deref;
use std::path::Path;

use crate::graph::*;
use crate::properties;

impl<T: Deref> SwhGraph for T
where
    <T as Deref>::Target: SwhGraph,
{
    fn path(&self) -> &Path {
        self.deref().path()
    }
    fn is_transposed(&self) -> bool {
        self.deref().is_transposed()
    }
    fn num_nodes(&self) -> usize {
        self.deref().num_nodes()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.deref().has_node(node_id)
    }
    fn num_arcs(&self) -> u64 {
        self.deref().num_arcs()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.deref().has_arc(src_node_id, dst_node_id)
    }
}

impl<T: Deref> SwhForwardGraph for T
where
    <T as Deref>::Target: SwhForwardGraph,
{
    type Successors<'succ> = <<T as Deref>::Target as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.deref().successors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.deref().outdegree(node_id)
    }
}

impl<T: Deref> SwhLabelledForwardGraph for T
where
    <T as Deref>::Target: SwhLabelledForwardGraph,
{
    type LabelledArcs<'arc> = <<T as Deref>::Target as SwhLabelledForwardGraph>::LabelledArcs<'arc>
    where
        Self: 'arc;
    type LabelledSuccessors<'succ> = <<T as Deref>::Target as SwhLabelledForwardGraph>::LabelledSuccessors<'succ>
    where
        Self: 'succ;

    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_> {
        self.deref().labelled_successors(node_id)
    }
}

impl<T: Deref> SwhBackwardGraph for T
where
    <T as Deref>::Target: SwhBackwardGraph,
{
    type Predecessors<'succ> = <<T as Deref>::Target as SwhBackwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.deref().predecessors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.deref().indegree(node_id)
    }
}

impl<T: Deref> SwhLabelledBackwardGraph for T
where
    <T as Deref>::Target: SwhLabelledBackwardGraph,
{
    type LabelledArcs<'arc> = <<T as Deref>::Target as SwhLabelledBackwardGraph>::LabelledArcs<'arc>
    where
        Self: 'arc;
    type LabelledPredecessors<'succ> = <<T as Deref>::Target as SwhLabelledBackwardGraph>::LabelledPredecessors<'succ>
    where
        Self: 'succ;

    fn labelled_predecessors(&self, node_id: NodeId) -> Self::LabelledPredecessors<'_> {
        self.deref().labelled_predecessors(node_id)
    }
}
impl<T: Deref> SwhGraphWithProperties for T
where
    <T as Deref>::Target: SwhGraphWithProperties,
{
    type Maps = <<T as Deref>::Target as SwhGraphWithProperties>::Maps;
    type Timestamps = <<T as Deref>::Target as SwhGraphWithProperties>::Timestamps;
    type Persons = <<T as Deref>::Target as SwhGraphWithProperties>::Persons;
    type Contents = <<T as Deref>::Target as SwhGraphWithProperties>::Contents;
    type Strings = <<T as Deref>::Target as SwhGraphWithProperties>::Strings;
    type LabelNames = <<T as Deref>::Target as SwhGraphWithProperties>::LabelNames;
    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<
        Self::Maps,
        Self::Timestamps,
        Self::Persons,
        Self::Contents,
        Self::Strings,
        Self::LabelNames,
    > {
        self.deref().properties()
    }
}
