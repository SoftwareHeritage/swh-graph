// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Boring implementations of `SwhGraph*` traits

use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;

use anyhow::Result;
use dsi_progress_logger::ProgressLog;

use crate::graph::*;
use crate::labels::EdgeLabel;
use crate::properties;
use crate::NodeType;

impl<T: Deref> SwhGraph for T
where
    <T as Deref>::Target: SwhGraph,
{
    #[inline(always)]
    fn path(&self) -> &Path {
        self.deref().path()
    }
    #[inline(always)]
    fn is_transposed(&self) -> bool {
        self.deref().is_transposed()
    }
    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.deref().num_nodes()
    }
    #[inline(always)]
    fn has_node(&self, node_id: NodeId) -> bool {
        self.deref().has_node(node_id)
    }
    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.deref().num_arcs()
    }
    #[inline(always)]
    fn num_nodes_by_type(&self) -> Result<HashMap<NodeType, usize>> {
        self.deref().num_nodes_by_type()
    }
    #[inline(always)]
    fn actual_num_nodes(&self) -> Result<usize> {
        self.deref().actual_num_nodes()
    }
    #[inline(always)]
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        self.deref().num_arcs_by_type()
    }
    #[inline(always)]
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.deref().has_arc(src_node_id, dst_node_id)
    }
    #[inline(always)]
    fn iter_nodes<'a>(&'a self, pl: impl ProgressLog + 'a) -> impl Iterator<Item = NodeId> + 'a {
        self.deref().iter_nodes(pl)
    }

    /* We can't forward par_iter_nodes() to the underlying graph because the type checker
       does not know that it is Sync.
       Requiring `<T as Deref>::Target: Sync` would be inconvenient for users (they would
       need the bound every time) and a breaking change.
       Making `SwhGraph` a subtrait of `Sync` would be a breaking change.
    #[inline(always)]
    fn par_iter_nodes<'a>(
        &'a self,
        pl: impl ConcurrentProgressLog + 'a,
    ) -> impl ParallelIterator<Item = NodeId> + 'a
    where
        Self: Sync,
    {
        self.deref().par_iter_nodes(pl)
    }
    */
}

impl<T: Deref> SwhForwardGraph for T
where
    <T as Deref>::Target: SwhForwardGraph,
{
    type Successors<'succ>
        = <<T as Deref>::Target as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.deref().successors(node_id)
    }
    #[inline(always)]
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.deref().outdegree(node_id)
    }
}

impl<T: Deref> SwhLabeledForwardGraph for T
where
    <T as Deref>::Target: SwhLabeledForwardGraph,
{
    type LabeledArcs<'arc>
        = <<T as Deref>::Target as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = <<T as Deref>::Target as SwhLabeledForwardGraph>::LabeledSuccessors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        self.deref().untyped_labeled_successors(node_id)
    }
    #[inline(always)]
    fn labeled_successors(
        &self,
        node_id: NodeId,
    ) -> impl Iterator<Item = (usize, impl Iterator<Item = EdgeLabel>)>
           + IntoFlattenedLabeledArcsIterator<EdgeLabel>
           + '_ {
        self.deref().labeled_successors(node_id)
    }
}

impl<T: Deref> SwhBackwardGraph for T
where
    <T as Deref>::Target: SwhBackwardGraph,
{
    type Predecessors<'succ>
        = <<T as Deref>::Target as SwhBackwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.deref().predecessors(node_id)
    }
    #[inline(always)]
    fn indegree(&self, node_id: NodeId) -> usize {
        self.deref().indegree(node_id)
    }
}

impl<T: Deref> SwhLabeledBackwardGraph for T
where
    <T as Deref>::Target: SwhLabeledBackwardGraph,
{
    type LabeledArcs<'arc>
        = <<T as Deref>::Target as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledPredecessors<'succ>
        = <<T as Deref>::Target as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        self.deref().untyped_labeled_predecessors(node_id)
    }

    #[inline(always)]
    fn labeled_predecessors(
        &self,
        node_id: NodeId,
    ) -> impl IntoIterator<Item = (usize, impl Iterator<Item = crate::labels::EdgeLabel>)>
           + IntoFlattenedLabeledArcsIterator<EdgeLabel>
           + '_ {
        self.deref().labeled_predecessors(node_id)
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
    #[inline(always)]
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
