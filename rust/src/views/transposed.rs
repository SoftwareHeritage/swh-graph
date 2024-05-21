// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::Path;

use crate::graph::*;
use crate::properties;

/// A view over [`SwhGraph`] and related trait, that flips the direction of all arcs
pub struct Transposed<G: SwhGraph>(pub G);

impl<G: SwhGraph> SwhGraph for Transposed<G> {
    fn path(&self) -> &Path {
        self.0.path()
    }
    fn is_transposed(&self) -> bool {
        !self.0.is_transposed()
    }
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(dst_node_id, src_node_id)
    }
}

impl<G: SwhBackwardGraph> SwhForwardGraph for Transposed<G> {
    type Successors<'succ> = <G as SwhBackwardGraph>::Predecessors<'succ> where Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.0.predecessors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.0.indegree(node_id)
    }
}

impl<G: SwhLabelledBackwardGraph> SwhLabelledForwardGraph for Transposed<G> {
    type LabelledArcs<'arc> =  <G as SwhLabelledBackwardGraph>::LabelledArcs<'arc> where Self: 'arc;
    type LabelledSuccessors<'succ> = <G as SwhLabelledBackwardGraph>::LabelledPredecessors<'succ> where Self: 'succ;

    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_> {
        self.0.labelled_predecessors(node_id)
    }
}

impl<G: SwhForwardGraph> SwhBackwardGraph for Transposed<G> {
    type Predecessors<'succ> = <G as SwhForwardGraph>::Successors<'succ> where Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.0.successors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<G: SwhLabelledForwardGraph> SwhLabelledBackwardGraph for Transposed<G> {
    type LabelledArcs<'arc> =  <G as SwhLabelledForwardGraph>::LabelledArcs<'arc> where Self: 'arc;
    type LabelledPredecessors<'succ> = <G as SwhLabelledForwardGraph>::LabelledSuccessors<'succ> where Self: 'succ;

    fn labelled_predecessors(&self, node_id: NodeId) -> Self::LabelledPredecessors<'_> {
        self.0.labelled_successors(node_id)
    }
}

impl<G: SwhGraphWithProperties> SwhGraphWithProperties for Transposed<G> {
    type Maps = <G as SwhGraphWithProperties>::Maps;
    type Timestamps = <G as SwhGraphWithProperties>::Timestamps;
    type Persons = <G as SwhGraphWithProperties>::Persons;
    type Contents = <G as SwhGraphWithProperties>::Contents;
    type Strings = <G as SwhGraphWithProperties>::Strings;
    type LabelNames = <G as SwhGraphWithProperties>::LabelNames;

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
        self.0.properties()
    }
}
