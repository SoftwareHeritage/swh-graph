// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::iter::Peekable;
use std::path::Path;

use anyhow::Result;
use itertools::{Either, Itertools, Merge};

use crate::arc_iterators::{
    FlattenedSuccessorsIterator, IntoFlattenedLabeledArcsIterator, LabelTypingSuccessorIterator,
};
use crate::graph::*;
use crate::labels::EdgeLabel;
use crate::properties;
use crate::NodeType;

/// A view over [`SwhGraph`] that makes all arcs bidirectional.
///
/// Beware of passing this structure to external code, as it breaks the assumption that graphs are
/// acyclic.
///
/// # Panics
///
/// When [`Self::is_transposed`] is called.
pub struct Symmetric<G: SwhGraph>(pub G);

impl<G: SwhGraph> SwhGraph for Symmetric<G> {
    fn path(&self) -> &Path {
        self.0.path()
    }
    fn is_transposed(&self) -> bool {
        panic!("Called Symmetric::is_transposed().");
    }
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    fn num_arcs(&self) -> u64 {
        self.0.num_arcs() * 2
    }
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        let mut counts = self.0.num_arcs_by_type()?;
        for (k, v) in super::Transposed(&self.0).num_arcs_by_type()? {
            *counts.entry(k).or_default() += v
        }
        Ok(counts)
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        self.0.has_node(node_id)
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.0.has_arc(src_node_id, dst_node_id) || self.0.has_arc(dst_node_id, src_node_id)
    }
}

impl<G: SwhForwardGraph + SwhBackwardGraph> SwhForwardGraph for Symmetric<G> {
    type Successors<'succ>
        = Merge<
        <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter,
        <<G as SwhBackwardGraph>::Predecessors<'succ> as IntoIterator>::IntoIter,
    >
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        self.0
            .successors(node_id)
            .into_iter()
            .merge(self.0.predecessors(node_id))
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.0.indegree(node_id) + self.0.outdegree(node_id)
    }
}

/// Merges two sorted iterators of `(NodeId, Labels)` pairs, wrapping labels
/// in [`Either`] to track which side they came from.
///
/// Panics if both iterators yield the same `NodeId` (the underlying graph has a loop).
pub struct MergingSortedPairs<F: Iterator, B: Iterator> {
    forward: Peekable<F>,
    backward: Peekable<B>,
}

impl<F: Iterator, B: Iterator> MergingSortedPairs<F, B> {
    fn new(forward: F, backward: B) -> Self {
        Self {
            forward: forward.peekable(),
            backward: backward.peekable(),
        }
    }
}

impl<F, B, FL, BL> Iterator for MergingSortedPairs<F, B>
where
    F: Iterator<Item = (NodeId, FL)>,
    B: Iterator<Item = (NodeId, BL)>,
{
    type Item = (NodeId, Either<FL, BL>);

    fn next(&mut self) -> Option<Self::Item> {
        let take_forward = match (self.forward.peek(), self.backward.peek()) {
            (Some((f_id, _)), Some((b_id, _))) => {
                assert_ne!(f_id, b_id, "Symmetric's backend has a loop");
                f_id < b_id
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return None,
        };
        if take_forward {
            let (id, labels) = self.forward.next().expect("item disappeared");
            Some((id, Either::Left(labels)))
        } else {
            let (id, labels) = self.backward.next().expect("item disappeared");
            Some((id, Either::Right(labels)))
        }
    }
}

impl<F, B, FL, BL, Label> IntoFlattenedLabeledArcsIterator<Label> for MergingSortedPairs<F, B>
where
    F: Iterator<Item = (NodeId, FL)>,
    B: Iterator<Item = (NodeId, BL)>,
    Either<FL, BL>: IntoIterator<Item = Label>,
{
    type Flattened = FlattenedSuccessorsIterator<Self>;

    fn flatten_labels(self) -> Self::Flattened {
        FlattenedSuccessorsIterator::new(self)
    }
}

impl<G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph> SwhLabeledForwardGraph for Symmetric<G> {
    type LabeledArcs<'arc>
        = Either<
        <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>,
        <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>,
    >
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = MergingSortedPairs<
        <<G as SwhLabeledForwardGraph>::LabeledSuccessors<'succ> as IntoIterator>::IntoIter,
        <<G as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ> as IntoIterator>::IntoIter,
    >
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        MergingSortedPairs::new(
            self.0.untyped_labeled_successors(node_id).into_iter(),
            self.0.untyped_labeled_predecessors(node_id).into_iter(),
        )
    }

    fn labeled_successors(
        &self,
        node_id: NodeId,
    ) -> impl IntoIterator<Item = (usize, impl Iterator<Item = EdgeLabel>)>
           + IntoFlattenedLabeledArcsIterator<EdgeLabel>
    where
        Self: SwhGraphWithProperties + Sized,
        <Self as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    {
        MergingSortedPairs::new(
            LabelTypingSuccessorIterator {
                graph: self,
                is_transposed: self.0.is_transposed(),
                src: node_id,
                successors: self.0.untyped_labeled_successors(node_id).into_iter(),
            },
            LabelTypingSuccessorIterator {
                graph: self,
                is_transposed: !self.0.is_transposed(),
                src: node_id,
                successors: self.0.untyped_labeled_predecessors(node_id).into_iter(),
            },
        )
    }
}

impl<G: SwhForwardGraph + SwhBackwardGraph> SwhBackwardGraph for Symmetric<G>
where
    Self: SwhForwardGraph,
{
    type Predecessors<'pred>
        = <Self as SwhForwardGraph>::Successors<'pred>
    where
        Self: 'pred;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        self.successors(node_id)
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.outdegree(node_id)
    }
}

impl<G: SwhGraphWithProperties> SwhGraphWithProperties for Symmetric<G> {
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
