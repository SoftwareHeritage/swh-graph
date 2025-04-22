// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Structures to manipulate the Software Heritage graph
//!
//! In order to load only what is necessary, these structures are initially created
//! by calling [`SwhUnidirectionalGraph::new`](crate::graph::SwhUnidirectionalGraph::new) or
//! [`SwhBidirectionalGraph::new`](crate::graph::SwhBidirectionalGraph::new), then calling methods
//! on them to progressively load additional data (`load_properties`, `load_all_properties`,
//! `load_labels`)

#![allow(clippy::type_complexity)]

use std::borrow::Borrow;
use std::iter::Iterator;

use webgraph::prelude::*;

use crate::graph::{NodeId, SwhGraphWithProperties};
use crate::labels::{EdgeLabel, UntypedEdgeLabel};

pub trait IntoFlattenedLabeledArcsIterator<Label> {
    type Flattened: IntoIterator<Item = (NodeId, Label)>;

    /// Turns this `Iterator<Item=(succ, Iterator<Item=labels>)>` into an
    /// `Iterator<ITem=(succ, label)>`.
    fn flatten_labels(self) -> Self::Flattened;
}

pub struct LabeledSuccessorIterator<Successors: Iterator>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator<Item: Borrow<u64>>>,
{
    pub(crate) successors: Successors,
}

impl<Successors: Iterator> LabeledSuccessorIterator<Successors>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator<Item: Borrow<u64>>>,
{
    pub fn new(successors: Successors) -> Self {
        LabeledSuccessorIterator { successors }
    }
}

impl<Successors: Iterator> Iterator for LabeledSuccessorIterator<Successors>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator<Item: Borrow<u64>>>,
{
    type Item = (
        NodeId,
        LabeledArcIterator<
            <<<Successors as Iterator>::Item as Pair>::Right as IntoIterator>::IntoIter,
        >,
    );

    fn next(&mut self) -> Option<Self::Item> {
        self.successors.next().map(|pair| {
            let (successor, arc_labels) = pair.into_pair();
            (
                successor,
                LabeledArcIterator {
                    arc_label_ids: arc_labels.into_iter(),
                },
            )
        })
    }
}

impl<Successors: Iterator> IntoFlattenedLabeledArcsIterator<UntypedEdgeLabel>
    for LabeledSuccessorIterator<Successors>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator<Item: Borrow<u64>>>,
{
    type Flattened = FlattenedSuccessorsIterator<Self>;

    fn flatten_labels(self) -> Self::Flattened {
        FlattenedSuccessorsIterator::new(self)
    }
}

pub struct FlattenedSuccessorsIterator<Successors: Iterator>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator>,
{
    current_node_and_labels: Option<(
        NodeId,
        <<<Successors as Iterator>::Item as Pair>::Right as IntoIterator>::IntoIter,
    )>,
    iter: Successors,
}

impl<Successors: Iterator> FlattenedSuccessorsIterator<Successors>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator>,
{
    pub fn new(successors: Successors) -> Self {
        Self {
            current_node_and_labels: None,
            iter: successors,
        }
    }
}

impl<Successors: Iterator> Iterator for FlattenedSuccessorsIterator<Successors>
where
    <Successors as Iterator>::Item: Pair<Left = usize, Right: IntoIterator>,
{
    type Item = (
        NodeId,
        <<<Successors as Iterator>::Item as Pair>::Right as IntoIterator>::Item,
    );

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_node_and_labels.is_none() {
            // first iterator, or exhausted current label list, move to the next one
            self.current_node_and_labels = self
                .iter
                .next()
                .map(Pair::into_pair)
                .map(|(succ, labels)| (succ, labels.into_iter()))
        }
        let Some((current_node, ref mut current_labels)) = self.current_node_and_labels else {
            // Reached the end of the iterator
            return None;
        };
        match current_labels.next() {
            Some(label) => Some((current_node, label)),
            None => {
                // exhausted this label list, move to the next one
                self.current_node_and_labels = None;
                self.next()
            }
        }
    }
}

pub struct LabeledArcIterator<T: Iterator>
where
    <T as Iterator>::Item: Borrow<u64>,
{
    arc_label_ids: T,
}

impl<T: Iterator> Iterator for LabeledArcIterator<T>
where
    <T as Iterator>::Item: Borrow<u64>,
{
    type Item = UntypedEdgeLabel;

    fn next(&mut self) -> Option<Self::Item> {
        self.arc_label_ids
            .next()
            .map(|label| UntypedEdgeLabel::from(*label.borrow()))
    }
}

pub struct LabelTypingSuccessorIterator<'a, G, Successors: Iterator>
where
    <Successors as Iterator>::Item:
        Pair<Left = usize, Right: IntoIterator<Item = UntypedEdgeLabel>>,
    G: SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    pub(crate) graph: &'a G,
    pub(crate) is_transposed: bool,
    pub(crate) successors: Successors,
    pub(crate) src: NodeId,
}

impl<'a, G, Successors: Iterator> Iterator for LabelTypingSuccessorIterator<'a, G, Successors>
where
    <Successors as Iterator>::Item:
        Pair<Left = usize, Right: IntoIterator<Item = UntypedEdgeLabel>>,
    G: SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Item = (
        NodeId,
        LabelTypingArcIterator<
            'a,
            G,
            <<<Successors as Iterator>::Item as Pair>::Right as IntoIterator>::IntoIter,
        >,
    );

    fn next(&mut self) -> Option<Self::Item> {
        self.successors.next().map(|pair| {
            let mut src = self.src;
            let (mut dst, labels) = pair.into_pair();
            let succ = dst;
            if self.is_transposed {
                (src, dst) = (dst, src)
            }
            (
                succ,
                LabelTypingArcIterator {
                    graph: self.graph,
                    labels: labels.into_iter(),
                    src,
                    dst,
                },
            )
        })
    }
}

impl<G, Successors: Iterator> IntoFlattenedLabeledArcsIterator<EdgeLabel>
    for LabelTypingSuccessorIterator<'_, G, Successors>
where
    <Successors as Iterator>::Item:
        Pair<Left = usize, Right: IntoIterator<Item = UntypedEdgeLabel>>,
    G: SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Flattened = FlattenedSuccessorsIterator<Self>;

    fn flatten_labels(self) -> Self::Flattened {
        FlattenedSuccessorsIterator::new(self)
    }
}

pub struct LabelTypingArcIterator<'a, G, Labels: Iterator<Item = UntypedEdgeLabel>>
where
    G: SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    graph: &'a G,
    labels: Labels,
    src: NodeId,
    dst: NodeId,
}

impl<G, Labels: Iterator<Item = UntypedEdgeLabel>> Iterator
    for LabelTypingArcIterator<'_, G, Labels>
where
    G: SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: crate::properties::Maps,
{
    type Item = EdgeLabel;

    fn next(&mut self) -> Option<Self::Item> {
        let props = self.graph.properties();
        self.labels.next().map(move |label| {
            label
                .for_edge_type(
                    props.node_type(self.src),
                    props.node_type(self.dst),
                    self.graph.is_transposed(),
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "Unexpected edge from {} ({}) to {} ({}): {}",
                        props.swhid(self.src),
                        self.src,
                        props.swhid(self.dst),
                        self.dst,
                        e
                    )
                })
        })
    }
}

/// Wraps an iterator of labeled successors, and yields only the successors
pub struct DelabelingIterator<Successors: Iterator>
where
    <Successors as Iterator>::Item: Pair<Left = usize>,
{
    pub(crate) successors: Successors,
}
impl<Successors: Iterator> Iterator for DelabelingIterator<Successors>
where
    <Successors as Iterator>::Item: Pair<Left = usize>,
{
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        self.successors.next().map(|pair| {
            let (successor, _arc_labels) = pair.into_pair();
            successor
        })
    }
}
