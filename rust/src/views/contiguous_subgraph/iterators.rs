// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`SwhForwardGraph`], [`SwhBackwardGraph`], [`SwhLabeledForwardGraph`],
//! and [`SwhLabeledBackwardGraph`] for [`ContiguousSubgraph`].

use crate::arc_iterators::FlattenedSuccessorsIterator;

use super::*;

macro_rules! make_filtered_arcs_iterator {
    ($name:ident, $inner:ident, $( $next:tt )*) => {
        pub struct $name<
            'a,
            $inner: Iterator<Item = NodeId> + 'a,
            N: ContractionBackend
        > {
            inner: $inner,
            contraction: &'a Contraction<N>,
        }

        impl<
            'a,
            $inner: Iterator<Item = NodeId> + 'a,
            N: ContractionBackend,

        > Iterator for $name<'a, $inner, N> {
            type Item = $inner::Item;

            $( $next )*
        }
    }
}

make_filtered_arcs_iterator! {
    TranslatedSuccessors,
    Successors,
    fn next(&mut self) -> Option<Self::Item> {
        for underlying_successor in self.inner.by_ref() {
            if let Some(self_successor) = self.contraction.node_id_from_underlying(underlying_successor) {
                return Some(self_successor)
            }
        }
        None
    }
}

macro_rules! make_filtered_labeled_arcs_iterator {
    ($name:ident, $inner:ident, $( $next:tt )*) => {
        pub struct $name<
            'a,
            Labels,
            $inner: Iterator<Item = (NodeId, Labels)> + 'a,
            N: ContractionBackend
        > {
            inner: $inner,
            contraction: &'a Contraction<N>,
        }

        impl<
            'a,
            Labels,
            $inner: Iterator<Item = (NodeId, Labels)> + 'a,
            N: ContractionBackend
        > Iterator for $name<'a, Labels, $inner, N> {
            type Item = $inner::Item;

            $( $next )*
        }

        impl<
            'a,
            Labels: IntoIterator,
            $inner: Iterator<Item = (NodeId, Labels)> + 'a,
            N: ContractionBackend
        > IntoFlattenedLabeledArcsIterator<<Labels as IntoIterator>::Item> for $name<'a, Labels, $inner, N> {
            type Flattened = FlattenedSuccessorsIterator<Self>;

            fn flatten_labels(self) -> Self::Flattened {
                FlattenedSuccessorsIterator::new(self)
            }
        }
    }
}

make_filtered_labeled_arcs_iterator! {
    TranslatedLabeledSuccessors,
    LabeledSuccessors,
    fn next(&mut self) -> Option<Self::Item> {
        for (underlying_successor, label) in self.inner.by_ref() {
            if let Some(self_successor) = self.contraction.node_id_from_underlying(underlying_successor) {
                return Some((self_successor, label))
            }
        }
        None
    }
}

// Edge direction doesn't matter, so we can reuse the symmetric iterators
type TranslatedPredecessors<'a, G, N> = TranslatedSuccessors<'a, G, N>;
type TranslatedLabeledPredecessors<'a, Labels, G, N> =
    TranslatedLabeledSuccessors<'a, Labels, G, N>;

impl<
        G: SwhForwardGraph,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhForwardGraph
    for ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    type Successors<'succ>
        = TranslatedSuccessors<
        'succ,
        <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter,
        N,
    >
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        TranslatedSuccessors {
            inner: self
                .inner
                .underlying_graph
                .successors(self.inner.contraction.underlying_node_id(node_id))
                .into_iter(),
            contraction: &self.inner.contraction,
        }
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.successors(node_id).count()
    }
}

impl<
        G: SwhBackwardGraph,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhBackwardGraph
    for ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    type Predecessors<'succ>
        = TranslatedPredecessors<
        'succ,
        <<G as SwhBackwardGraph>::Predecessors<'succ> as IntoIterator>::IntoIter,
        N,
    >
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        TranslatedPredecessors {
            inner: self
                .inner
                .underlying_graph
                .predecessors(self.inner.contraction.underlying_node_id(node_id))
                .into_iter(),
            contraction: &self.inner.contraction,
        }
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.predecessors(node_id).count()
    }
}

impl<
        G: SwhLabeledForwardGraph,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhLabeledForwardGraph
    for ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    type LabeledArcs<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'node>
        = TranslatedLabeledSuccessors<
        'node,
        Self::LabeledArcs<'node>,
        <<G as SwhLabeledForwardGraph>::LabeledSuccessors<'node> as IntoIterator>::IntoIter,
        N,
    >
    where
        Self: 'node;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        TranslatedLabeledSuccessors {
            inner: self
                .inner
                .underlying_graph
                .untyped_labeled_successors(self.inner.contraction.underlying_node_id(node_id))
                .into_iter(),
            contraction: &self.inner.contraction,
        }
    }
}

impl<
        G: SwhLabeledBackwardGraph,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhLabeledBackwardGraph
    for ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    type LabeledArcs<'arc>
        = <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledPredecessors<'node>
        = TranslatedLabeledPredecessors<
        'node,
        Self::LabeledArcs<'node>,
        <<G as SwhLabeledBackwardGraph>::LabeledPredecessors<'node> as IntoIterator>::IntoIter,
        N,
    >
    where
        Self: 'node;

    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        TranslatedLabeledSuccessors {
            inner: self
                .inner
                .underlying_graph
                .untyped_labeled_predecessors(self.inner.contraction.underlying_node_id(node_id))
                .into_iter(),
            contraction: &self.inner.contraction,
        }
    }
}
