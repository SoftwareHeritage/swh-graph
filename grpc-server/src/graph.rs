// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::iter::{empty, Empty};
use std::path::Path;

use swh_graph::arc_iterators::{LabeledArcIterator, LabeledSuccessorIterator};
use swh_graph::graph::*;
use swh_graph::properties;

/// Alias for structures representing a graph with all arcs, arc labels, and node properties
/// loaded (conditional on them being actually present on disk)
pub trait SwhFullGraphWithOptProperties:
    SwhLabeledForwardGraph
    + SwhLabeledBackwardGraph
    + SwhGraphWithProperties<
        Maps: properties::Maps,
        Timestamps: properties::OptTimestamps,
        Persons: properties::OptPersons,
        Contents: properties::OptContents,
        Strings: properties::OptStrings,
        LabelNames: properties::LabelNames,
    >
{
}

impl<
        G: SwhLabeledForwardGraph
            + SwhLabeledBackwardGraph
            + SwhGraphWithProperties<
                Maps: properties::Maps,
                Timestamps: properties::OptTimestamps,
                Persons: properties::OptPersons,
                Contents: properties::OptContents,
                Strings: properties::OptStrings,
                LabelNames: properties::LabelNames,
            >,
    > SwhFullGraphWithOptProperties for G
{
}

pub trait SwhOptFullGraph: SwhFullGraphWithOptProperties {}
impl<G: SwhFullGraphWithOptProperties> SwhOptFullGraph for G {}

/// Wrapper for a graph that "implements" labels by returning empty lists
pub struct StubLabels<G: SwhGraph>(G);

impl<G: SwhGraph> StubLabels<G> {
    pub fn new(graph: G) -> Self {
        StubLabels(graph)
    }
}

impl<G: SwhGraph> SwhGraph for StubLabels<G> {
    #[inline(always)]
    fn path(&self) -> &Path {
        self.0.path()
    }
    #[inline(always)]
    fn is_transposed(&self) -> bool {
        self.0.is_transposed()
    }
    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    #[inline(always)]
    fn has_node(&self, node_id: NodeId) -> bool {
        self.0.has_node(node_id)
    }
    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }
    #[inline(always)]
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(src_node_id, dst_node_id)
    }
}

impl<G: SwhForwardGraph> SwhForwardGraph for StubLabels<G> {
    type Successors<'succ>
        = <G as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.0.successors(node_id)
    }
    #[inline(always)]
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.0.outdegree(node_id)
    }
}

impl<G: SwhForwardGraph> SwhLabeledForwardGraph for StubLabels<G> {
    type LabeledArcs<'arc>
        = LabeledArcIterator<Empty<u64>>
    where
        Self: 'arc;
    type LabeledSuccessors<'arc>
        = LabeledSuccessorIterator<
        std::iter::Map<
            <<G as SwhForwardGraph>::Successors<'arc> as IntoIterator>::IntoIter,
            fn(usize) -> (usize, Empty<u64>),
        >,
    >
    where
        Self: 'arc;

    #[inline(always)]
    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        LabeledSuccessorIterator::new(
            self.successors(node_id)
                .into_iter()
                .map(succ_to_labeled_succ),
        )
    }
}

impl<G: SwhBackwardGraph> SwhBackwardGraph for StubLabels<G> {
    type Predecessors<'succ>
        = <G as SwhBackwardGraph>::Predecessors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.0.predecessors(node_id)
    }
    #[inline(always)]
    fn indegree(&self, node_id: NodeId) -> usize {
        self.0.indegree(node_id)
    }
}

impl<G: SwhBackwardGraph> SwhLabeledBackwardGraph for StubLabels<G> {
    type LabeledArcs<'arc>
        = LabeledArcIterator<Empty<u64>>
    where
        Self: 'arc;
    type LabeledPredecessors<'arc>
        = LabeledSuccessorIterator<
        std::iter::Map<
            <<G as SwhBackwardGraph>::Predecessors<'arc> as IntoIterator>::IntoIter,
            fn(usize) -> (usize, Empty<u64>),
        >,
    >
    where
        Self: 'arc;

    #[inline(always)]
    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        LabeledSuccessorIterator::new(
            self.predecessors(node_id)
                .into_iter()
                .map(succ_to_labeled_succ),
        )
    }
}

impl<G: SwhGraphWithProperties> SwhGraphWithProperties for StubLabels<G> {
    type Maps = <G as SwhGraphWithProperties>::Maps;
    type Timestamps = <G as SwhGraphWithProperties>::Timestamps;
    type Persons = <G as SwhGraphWithProperties>::Persons;
    type Contents = <G as SwhGraphWithProperties>::Contents;
    type Strings = <G as SwhGraphWithProperties>::Strings;
    type LabelNames = <G as SwhGraphWithProperties>::LabelNames;

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
        self.0.properties()
    }
}

/// Wrapper for a graph that "implements" backward arcs by returning empty lists
pub struct StubBackwardArcs<G: SwhGraph>(G);

impl<G: SwhGraph> StubBackwardArcs<G> {
    #[inline(always)]
    pub fn new(graph: G) -> Self {
        StubBackwardArcs(graph)
    }
}

impl<G: SwhGraph> SwhGraph for StubBackwardArcs<G> {
    #[inline(always)]
    fn path(&self) -> &Path {
        self.0.path()
    }
    #[inline(always)]
    fn is_transposed(&self) -> bool {
        self.0.is_transposed()
    }
    #[inline(always)]
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    #[inline(always)]
    fn has_node(&self, node_id: NodeId) -> bool {
        self.0.has_node(node_id)
    }
    #[inline(always)]
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs()
    }
    #[inline(always)]
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(src_node_id, dst_node_id)
    }
}

impl<G: SwhForwardGraph> SwhForwardGraph for StubBackwardArcs<G> {
    type Successors<'succ>
        = <G as SwhForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.0.successors(node_id)
    }
    #[inline(always)]
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.0.outdegree(node_id)
    }
}

#[inline(always)]
fn succ_to_labeled_succ(node_id: NodeId) -> (NodeId, Empty<u64>) {
    (node_id, empty())
}

impl<G: SwhLabeledForwardGraph> SwhLabeledForwardGraph for StubBackwardArcs<G> {
    type LabeledArcs<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledSuccessors<'arc>
    where
        Self: 'arc;

    #[inline(always)]
    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        self.0.untyped_labeled_successors(node_id)
    }
}

impl<G: SwhGraph> SwhBackwardGraph for StubBackwardArcs<G> {
    type Predecessors<'succ>
        = Empty<NodeId>
    where
        Self: 'succ;

    #[inline(always)]
    fn predecessors(&self, _node_id: NodeId) -> Self::Predecessors<'_> {
        empty()
    }
    #[inline(always)]
    fn indegree(&self, _node_id: NodeId) -> usize {
        0
    }
}

impl<G: SwhGraph> SwhLabeledBackwardGraph for StubBackwardArcs<G> {
    type LabeledArcs<'arc>
        = LabeledArcIterator<Empty<u64>>
    where
        Self: 'arc;
    type LabeledPredecessors<'arc>
        = LabeledSuccessorIterator<Empty<(NodeId, Empty<u64>)>>
    where
        Self: 'arc;

    #[inline(always)]
    fn untyped_labeled_predecessors(&self, _node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        LabeledSuccessorIterator::new(empty())
    }
}

impl<G: SwhGraphWithProperties> SwhGraphWithProperties for StubBackwardArcs<G> {
    type Maps = <G as SwhGraphWithProperties>::Maps;
    type Timestamps = <G as SwhGraphWithProperties>::Timestamps;
    type Persons = <G as SwhGraphWithProperties>::Persons;
    type Contents = <G as SwhGraphWithProperties>::Contents;
    type Strings = <G as SwhGraphWithProperties>::Strings;
    type LabelNames = <G as SwhGraphWithProperties>::LabelNames;

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
        self.0.properties()
    }
}

/// Empty implementation of [`properties::LabelNames`] that never returns values
pub struct StubLabelNames;

pub struct EmptyGetIndex<Output>(std::marker::PhantomData<Output>);
impl<Output> swh_graph::utils::GetIndex for EmptyGetIndex<Output> {
    type Output = Output;
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }
    #[inline(always)]
    fn get(&self, _: usize) -> std::option::Option<<Self as swh_graph::utils::GetIndex>::Output> {
        None
    }
    unsafe fn get_unchecked(&self, _: usize) -> <Self as swh_graph::utils::GetIndex>::Output {
        panic!("EmptyGetIndex::get_unchecked")
    }
}

impl properties::LabelNames for StubLabelNames {
    type LabelNames<'a>
        = EmptyGetIndex<Vec<u8>>
    where
        Self: 'a;

    #[inline(always)]
    fn label_names(&self) -> Self::LabelNames<'_> {
        EmptyGetIndex(std::marker::PhantomData)
    }
}
