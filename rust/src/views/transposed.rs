// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use crate::graph::*;
use crate::properties;
use crate::NodeType;

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
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        Ok(self
            .0
            .num_arcs_by_type()?
            .into_iter()
            .map(|((src_type, dst_type), count)| ((dst_type, src_type), count))
            .collect())
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.0.has_node(node_id)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(dst_node_id, src_node_id)
    }
}

impl<G: SwhBackwardGraph> SwhForwardGraph for Transposed<G> {
    type Successors<'succ>
        = <G as SwhBackwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.0.predecessors(node_id)
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.0.indegree(node_id)
    }
}

impl<G: SwhLabeledBackwardGraph> SwhLabeledForwardGraph for Transposed<G> {
    type LabeledArcs<'arc>
        = <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = <G as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ>
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        self.0.untyped_labeled_predecessors(node_id)
    }
}

impl<G: SwhForwardGraph> SwhBackwardGraph for Transposed<G> {
    type Predecessors<'succ>
        = <G as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.0.successors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<G: SwhLabeledForwardGraph> SwhLabeledBackwardGraph for Transposed<G> {
    type LabeledArcs<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledPredecessors<'succ>
        = <G as SwhLabeledForwardGraph>::LabeledSuccessors<'succ>
    where
        Self: 'succ;

    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        self.0.untyped_labeled_successors(node_id)
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
