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
    FlattenedSuccessorsIterator, IntoFlattenedLabeledArcsIterator, LabelTypingArcIterator,
    LabelTypingSuccessorIterator,
};
use crate::graph::*;
use crate::labels::{EdgeLabel, UntypedEdgeLabel};
use crate::properties;
use crate::NodeType;

/// A view over [`SwhGraph`] and related traits, that flips the direction of all arcs
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
        // assume the underlying graph is not itself symmetric
        self.0.indegree(node_id) + self.0.outdegree(node_id)
    }
}

/*
struct Uncomparable<T>(T);

impl<T> PartialEq<Self> for Uncomparable<T> {
    fn eq(&self, other: &Self) -> bool {
        false
    }
}

impl<T> PartialOrd<Self> for Uncomparable<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

type WrapLabel<Label> = fn((NodeId, Label)) -> (NodeId, Uncomparable<Label>);
fn wrap_label<Label>(item: (NodeId, Label)) -> (NodeId, Uncomparable<Label>) {
    let (node, label) = item;
    (node, Uncomparable(label))
}
type UnwrapLabel<Label> = fn((NodeId, Uncomparable<Label>)) -> (NodeId, Label);
fn unwrap_label<Label>(item: (NodeId, Uncomparable<Label>)) -> (NodeId, Label) {
    let (node, Uncomparable(label)) = item;
    (node, label)
}
*/

/*
pub struct LabeledNode<Label> {
    node: NodeId,
    label: Label,
}

impl<Label> PartialEq<Self> for LabeledNode<Label> {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
    }
}

impl<Label> PartialOrd<Self> for LabeledNode<Label> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.node.cmp(other.node) {
            std::cmp::Ordering::Equal => None,
            ordering => Some(ordering),
        }
    }
}
type WrapLabel<Label> = fn((NodeId, Label)) -> LabeledNode<Label>;
fn wrap_label<Label>(item: (NodeId, Label)) -> LabeledNode<Label> {
    let (node, label) = item;
    LabeledNode { node, label }
}
type UnwrapLabel<Label> = fn(LabeledNode<Label>) -> (NodeId, Label);
fn unwrap_label<Label>(item: LabeledNode<Label>) -> (NodeId, Label) {
    let LabeledNode { node, label }= item;
    (node, label)
}
*/

/*
pub struct MergedLabeledSuccessors<Label, F, B> {
    forward: F,
    backward: B,
    label: std::marker::PhantomData<Label>,
}

impl<
        Label,
        F: IntoFlattenedLabeledArcsIterator<Label>,
        B: IntoFlattenedLabeledArcsIterator<Label>,
    > IntoFlattenedLabeledArcsIterator<Label> for MergedLabeledSuccessors<Label, F, B>
{
    type Flattened = Map<Merge<
        Map<
            <<F as IntoFlattenedLabeledArcsIterator<Label>>::Flattened as IntoIterator>::IntoIter,
            WrapLabel<Label>
        >,
        Map<
            <<B as IntoFlattenedLabeledArcsIterator<Label>>::Flattened as IntoIterator>::IntoIter,
            WrapLabel<Label>
        >,
    >, UnwrapLabel<Label>>;

    fn flatten_labels(self) -> Self::Flattened {
        self.forward
            .into_iter()
            .map(wrap_label)
            .merge(self.backward.into_iter().map(wrap_label))
            .map(unwrap_label)
    }
}

impl<Label, F: IntoIterator<Item = (NodeId, Label)>, B: IntoIterator<Item = (NodeId, Label)>>
    IntoIterator for MergedLabeledSuccessors<Label, F, B>
{
    type Item = (NodeId, Label);
    type IntoIter = Map<
        Merge<
            Map<<F as IntoIterator>::IntoIter, WrapLabel<Label>>,
            Map<<B as IntoIterator>::IntoIter, WrapLabel<Label>>,
        >,
        UnwrapLabel<Label>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.forward
            .into_iter()
            .map(Uncomparable)
            .merge(self.backward.into_iter().map(Uncomparable))
    }
}
*/

/*
impl<G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph> SwhLabeledForwardGraph for Symmetric<G> {
    type LabeledArcs<'arc>
        = Either<
        <<G as SwhLabeledBackwardGraph>::LabeledArcs<'arc> as IntoIterator>::IntoIter,
        <<G as SwhLabeledForwardGraph>::LabeledArcs<'arc> as IntoIterator>::IntoIter,
    >
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = MergedLabeledSuccessors<
        UntypedEdgeLabel,
        LabeledSuccessorIterator<
            <<G as SwhLabeledForwardGraph>::LabeledSuccessors<'succ> as IntoIterator>::IntoIter,
        >,
        LabeledSuccessorIterator<
            <<G as SwhLabeledBackwardGraph>::LabeledPredecessors<'succ> as IntoIterator>::IntoIter,
        >,
    >
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        MergedLabeledSuccessors {
            forward: LabeledSuccessorIterator::new(
                self.0.untyped_labeled_successors(node_id).into_iter(),
            ),
            backward: LabeledSuccessorIterator::new(
                self.0.untyped_labeled_predecessors(node_id).into_iter(),
            ),
        }
    }

    fn labeled_successors(
        &self,
        node_id: NodeId,
    ) -> impl Iterator<Item = (usize, impl Iterator<Item = EdgeLabel>)>
           + IntoFlattenedLabeledArcsIterator<EdgeLabel>
    where
        Self: SwhGraphWithProperties + Sized,
        <Self as SwhGraphWithProperties>::Maps: crate::properties::Maps,
    {
        LabeledSuccessorIterator::new(
            self.0
                .labeled_successors(node_id)
                .map(Either::Left)
                .into_iter()
                .merge(
                    self.0
                        .labeled_predecessors(node_id)
                        .into_iter()
                        .map(Either::Right),
                ),
        )
    }
}
*/

/*
pub enum MergedLabeledArcs<'arc, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph> {
    Forward(<G as SwhLabeledForwardGraph>::LabeledArcs<'arc>),
    Backward(<G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>),
}

impl<'arc, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph> IntoIterator for MergedLabeledArcs<'arc, G> {
    type Item = UntypedEdgeLabel;
    type IntoIter = Either<
        <<G as SwhLabeledForwardGraph>::LabeledArcs<'arc> as IntoIterator>>::IntoIter,
        <<G as SwhLabeledBackwardGraph>::LabeledArcs<'arc> as IntoIterator>>::IntoIter,
    >;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Forward(arc_labels) => Either::Left(arc_labels.into_iter()),
            Self::Backward(arc_labels) => Either::Right(arc_labels.into_iter()),
        }
    }
}
*/

pub struct MergedLabeledArcs<'arc, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'arc>(
    Either<
        <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>,
        <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>,
    >,
);
impl<'arc, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'arc> Iterator
    for MergedLabeledArcs<'arc, G>
{
    type Item = UntypedEdgeLabel;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[allow(clippy::type_complexity)]
struct MergedTypedLabeledArcs<'arc, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'arc>(
    Either<
        <LabelTypingArcIterator<
            'arc,
            Symmetric<G>,
            <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>,
        > as IntoIterator>::IntoIter,
        <LabelTypingArcIterator<
            'arc,
            Symmetric<G>,
            <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>,
        > as IntoIterator>::IntoIter,
    >,
)
where
    Symmetric<G>: SwhGraphWithProperties,
    <Symmetric<G> as SwhGraphWithProperties>::Maps: crate::properties::Maps;
impl<'arc, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'arc> Iterator
    for MergedTypedLabeledArcs<'arc, G>
where
    Symmetric<G>: SwhGraphWithProperties,
    <Symmetric<G> as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Item = EdgeLabel;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct MergedLabeledSuccessors<
    'node,
    G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node,
> {
    forward: <G as SwhLabeledForwardGraph>::LabeledSuccessors<'node>,
    backward: <G as SwhLabeledBackwardGraph>::LabeledPredecessors<'node>,
}

impl<'node, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node>
    IntoFlattenedLabeledArcsIterator<UntypedEdgeLabel> for MergedLabeledSuccessors<'node, G>
{
    type Flattened = FlattenedSuccessorsIterator<MergedLabeledSuccessorsIterator<'node, G>>;

    fn flatten_labels(self) -> Self::Flattened {
        FlattenedSuccessorsIterator::new(self.into_iter())
    }
}

impl<'node, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node> IntoIterator
    for MergedLabeledSuccessors<'node, G>
{
    type Item = (NodeId, MergedLabeledArcs<'node, G>);
    type IntoIter = MergedLabeledSuccessorsIterator<'node, G>;

    fn into_iter(self) -> Self::IntoIter {
        let Self { forward, backward } = self;
        MergedLabeledSuccessorsIterator {
            forward: forward.into_iter().peekable(),
            backward: backward.into_iter().peekable(),
        }
    }
}

pub struct MergedLabeledSuccessorsIterator<
    'node,
    G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node,
> {
    forward: Peekable<
        <<G as SwhLabeledForwardGraph>::LabeledSuccessors<'node> as IntoIterator>::IntoIter,
    >,
    backward: Peekable<
        <<G as SwhLabeledBackwardGraph>::LabeledPredecessors<'node> as IntoIterator>::IntoIter,
    >,
}

macro_rules! merge {
    ($forward:expr, $backward:expr, $newtype:ident) => {{
        let forward = $forward;
        let backward = $backward;
        match (forward.peek(), backward.peek()) {
            (Some((succ, _)), Some((pred, _))) => {
                if succ < pred {
                    let (succ, labels) = forward.next().expect("item disappeared");
                    Some((succ, $newtype(Either::Left(labels))))
                } else {
                    assert_ne!(succ, pred, "Symmetric's backend has a loop");
                    let (pred, labels) = backward.next().expect("item disappeared");
                    Some((pred, $newtype(Either::Right(labels))))
                }
            }
            (Some(_), None) => {
                let (succ, labels) = forward.next().expect("item disappeared");
                Some((succ, $newtype(Either::Left(labels))))
            }
            (None, Some(_)) => {
                let (pred, labels) = backward.next().expect("item disappeared");
                Some((pred, $newtype(Either::Right(labels))))
            }
            (None, None) => None,
        }
    }};
}
impl<'node, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node> Iterator
    for MergedLabeledSuccessorsIterator<'node, G>
{
    type Item = (usize, MergedLabeledArcs<'node, G>);

    fn next(&mut self) -> Option<Self::Item> {
        // FIXME: less naive implementation, eg. using Itertools::merge
        merge!(&mut self.forward, &mut self.backward, MergedLabeledArcs)
    }
}

struct MergedTypedLabeledSuccessorsIterator<
    'node,
    G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node,
> where
    Symmetric<G>: SwhGraphWithProperties,
    <Symmetric<G> as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    forward: Peekable<
        LabelTypingSuccessorIterator<
            'node,
            Symmetric<G>,
            <<G as SwhLabeledForwardGraph>::LabeledSuccessors<'node> as IntoIterator>::IntoIter,
        >,
    >,
    backward: Peekable<
        LabelTypingSuccessorIterator<
            'node,
            Symmetric<G>,
            <<G as SwhLabeledBackwardGraph>::LabeledPredecessors<'node> as IntoIterator>::IntoIter,
        >,
    >,
}

impl<'node, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node> Iterator
    for MergedTypedLabeledSuccessorsIterator<'node, G>
where
    Symmetric<G>: SwhGraphWithProperties,
    <Symmetric<G> as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Item = (NodeId, MergedTypedLabeledArcs<'node, G>);

    fn next(&mut self) -> Option<Self::Item> {
        // FIXME: less naive implementation, eg. using Itertools::merge
        merge!(
            &mut self.forward,
            &mut self.backward,
            MergedTypedLabeledArcs
        )
    }
}

impl<'node, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node>
    IntoFlattenedLabeledArcsIterator<EdgeLabel> for MergedTypedLabeledSuccessorsIterator<'node, G>
where
    Symmetric<G>: SwhGraphWithProperties,
    <Symmetric<G> as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Flattened = FlattenedSuccessorsIterator<Self>;

    fn flatten_labels(self) -> Self::Flattened {
        FlattenedSuccessorsIterator::new(self)
    }
}

/*
impl<'node, G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph + 'node> Iterator
    for MergedTypedLabeledSuccessorsIterator<'node, G>
where
    G: SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Item = (usize, MergedLabeledArcs<'node, G>);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}*/

impl<G: SwhLabeledForwardGraph + SwhLabeledBackwardGraph> SwhLabeledForwardGraph for Symmetric<G> {
    type LabeledArcs<'arc>
        = MergedLabeledArcs<'arc, G>
    where
        Self: 'arc;
    type LabeledSuccessors<'succ>
        = MergedLabeledSuccessors<'succ, G>
    where
        Self: 'succ;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        MergedLabeledSuccessors {
            forward: self.0.untyped_labeled_successors(node_id),
            backward: self.0.untyped_labeled_predecessors(node_id),
        }
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
        let forward = LabelTypingSuccessorIterator {
            graph: self,
            is_transposed: self.0.is_transposed(),
            src: node_id,
            successors: self.0.untyped_labeled_successors(node_id).into_iter(),
        }
        .peekable();
        let backward = LabelTypingSuccessorIterator {
            graph: self,
            is_transposed: !self.0.is_transposed(),
            src: node_id,
            successors: self.0.untyped_labeled_predecessors(node_id).into_iter(),
        }
        .peekable();
        MergedTypedLabeledSuccessorsIterator { forward, backward }
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
