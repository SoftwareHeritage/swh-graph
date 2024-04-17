// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::Path;

use crate::graph::*;
use crate::properties;

macro_rules! make_filtered_arcs_iterator {
    ($name:ident, $inner:ident, $( $next:tt )*) => {
        pub struct $name<
            'a,
            $inner: Iterator<Item = NodeId> + 'a,
            NodeFilter: Fn(NodeId) -> bool,
            ArcFilter: Fn(NodeId, NodeId) -> bool,
        > {
            inner: $inner,
            node: NodeId,
            node_filter: &'a NodeFilter,
            arc_filter: &'a ArcFilter,
        }

        impl<
            'a,
            $inner: Iterator<Item = NodeId> + 'a,
            NodeFilter: Fn(NodeId) -> bool,
            ArcFilter: Fn(NodeId, NodeId) -> bool,
        > Iterator for $name<'a, $inner, NodeFilter, ArcFilter> {
            type Item = $inner::Item;

            $( $next )*
        }
    }
}

make_filtered_arcs_iterator! {
    FilteredSuccessors,
    Successors,
    fn next(&mut self) -> Option<Self::Item> {
        if !(self.node_filter)(self.node) {
            return None;
        }
        for dst in self.inner.by_ref() {
            if (self.node_filter)(dst) && (self.arc_filter)(self.node, dst) {
                return Some(dst)
            }
        }
        None
    }
}
make_filtered_arcs_iterator! {
    FilteredPredecessors,
    Predecessors,
    fn next(&mut self) -> Option<Self::Item> {
        if !(self.node_filter)(self.node) {
            return None;
        }
        for src in self.inner.by_ref() {
            if (self.node_filter)(src) && (self.arc_filter)(src, self.node) {
                return Some(src)
            }
        }
        None
    }
}

macro_rules! make_filtered_labelled_arcs_iterator {
    ($name:ident, $inner:ident, $( $next:tt )*) => {
        pub struct $name<
            'a,
            Labels,
            $inner: Iterator<Item = (NodeId, Labels)> + 'a,
            NodeFilter: Fn(NodeId) -> bool,
            ArcFilter: Fn(NodeId, NodeId) -> bool,
        > {
            inner: $inner,
            node: NodeId,
            node_filter: &'a NodeFilter,
            arc_filter: &'a ArcFilter,
        }

        impl<
            'a,
            Labels,
            $inner: Iterator<Item = (NodeId, Labels)> + 'a,
            NodeFilter: Fn(NodeId) -> bool,
            ArcFilter: Fn(NodeId, NodeId) -> bool,
        > Iterator for $name<'a, Labels, $inner, NodeFilter, ArcFilter> {
            type Item = $inner::Item;

            $( $next )*
        }
    }
}

make_filtered_labelled_arcs_iterator! {
    FilteredLabelledSuccessors,
    LabelledSuccessors,
    fn next(&mut self) -> Option<Self::Item> {
        if !(self.node_filter)(self.node) {
            return None;
        }
        for (dst, label) in self.inner.by_ref() {
            if (self.node_filter)(dst) && (self.arc_filter)(self.node, dst) {
                return Some((dst, label))
            }
        }
        None
    }
}
make_filtered_labelled_arcs_iterator! {
    FilteredLabelledPredecessors,
    LabelledPredecessors,
    fn next(&mut self) -> Option<Self::Item> {
        if !(self.node_filter)(self.node) {
            return None;
        }
        for (src, label) in self.inner.by_ref() {
            if (self.node_filter)(src) && (self.arc_filter)(src, self.node) {
                return Some((src, label))
            }
        }
        None
    }
}

/// A view over [`SwhGraph`] and related traits, that filters out some nodes and arcs
/// based on arbitrary closures.
pub struct Subgraph<G: SwhGraph, NodeFilter: Fn(usize) -> bool, ArcFilter: Fn(usize, usize) -> bool>
{
    pub graph: G,
    pub node_filter: NodeFilter,
    pub arc_filter: ArcFilter,
}

impl<G: SwhGraph, NodeFilter: Fn(usize) -> bool> Subgraph<G, NodeFilter, fn(usize, usize) -> bool> {
    /// Shorthand for `Subgraph { graph, node_filter, arc_filter: |_src, _dst| true }`
    pub fn new_with_node_filter(
        graph: G,
        node_filter: NodeFilter,
    ) -> Subgraph<G, NodeFilter, fn(usize, usize) -> bool> {
        Subgraph {
            graph,
            node_filter,
            arc_filter: |_src, _dst| true,
        }
    }
}
impl<G: SwhGraph, ArcFilter: Fn(usize, usize) -> bool> Subgraph<G, fn(usize) -> bool, ArcFilter> {
    /// Shorthand for `Subgraph { graph, node_filter: |_node| true, arc_filter }`
    pub fn new_with_arc_filter(
        graph: G,
        arc_filter: ArcFilter,
    ) -> Subgraph<G, fn(usize) -> bool, ArcFilter> {
        Subgraph {
            graph,
            node_filter: |_node| true,
            arc_filter,
        }
    }
}

impl<G: SwhGraph, NodeFilter: Fn(usize) -> bool, ArcFilter: Fn(usize, usize) -> bool> SwhGraph
    for Subgraph<G, NodeFilter, ArcFilter>
{
    fn path(&self) -> &Path {
        self.graph.path()
    }
    fn is_transposed(&self) -> bool {
        self.graph.is_transposed()
    }
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        (self.node_filter)(node_id)
    }
    fn num_arcs(&self) -> u64 {
        self.graph.num_arcs()
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        (self.node_filter)(src_node_id)
            && (self.node_filter)(dst_node_id)
            && (self.arc_filter)(src_node_id, dst_node_id)
            && self.graph.has_arc(dst_node_id, src_node_id)
    }
}

impl<G: SwhForwardGraph, NodeFilter: Fn(usize) -> bool, ArcFilter: Fn(usize, usize) -> bool>
    SwhForwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type Successors<'succ> = FilteredSuccessors<
        'succ,
        <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter
    >
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Self::Successors<'_> {
        FilteredSuccessors {
            inner: self.graph.successors(node_id).into_iter(),
            node: node_id,
            node_filter: &self.node_filter,
            arc_filter: &self.arc_filter,
        }
    }
    fn outdegree(&self, node_id: NodeId) -> usize {
        self.successors(node_id).count()
    }
}

impl<G: SwhBackwardGraph, NodeFilter: Fn(usize) -> bool, ArcFilter: Fn(usize, usize) -> bool>
    SwhBackwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type Predecessors<'succ> = FilteredPredecessors<
        'succ,
        <<G as SwhBackwardGraph>::Predecessors<'succ> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter
    >
    where
        Self: 'succ;

    fn predecessors(&self, node_id: NodeId) -> Self::Predecessors<'_> {
        FilteredPredecessors {
            inner: self.graph.predecessors(node_id).into_iter(),
            node: node_id,
            node_filter: &self.node_filter,
            arc_filter: &self.arc_filter,
        }
    }
    fn indegree(&self, node_id: NodeId) -> usize {
        self.predecessors(node_id).count()
    }
}

impl<
        G: SwhLabelledForwardGraph,
        NodeFilter: Fn(usize) -> bool,
        ArcFilter: Fn(usize, usize) -> bool,
    > SwhLabelledForwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type LabelledArcs<'arc> = <G as SwhLabelledForwardGraph>::LabelledArcs<'arc>
    where
        Self: 'arc;
    type LabelledSuccessors<'node> = FilteredLabelledSuccessors<
        'node,
        Self::LabelledArcs<'node>,
        <<G as SwhLabelledForwardGraph>::LabelledSuccessors<'node> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
    >
    where
        Self: 'node;

    fn labelled_successors(&self, node_id: NodeId) -> Self::LabelledSuccessors<'_> {
        FilteredLabelledSuccessors {
            inner: self.graph.labelled_successors(node_id).into_iter(),
            node: node_id,
            node_filter: &self.node_filter,
            arc_filter: &self.arc_filter,
        }
    }
}

impl<
        G: SwhLabelledBackwardGraph,
        NodeFilter: Fn(usize) -> bool,
        ArcFilter: Fn(usize, usize) -> bool,
    > SwhLabelledBackwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type LabelledArcs<'arc> = <G as SwhLabelledBackwardGraph>::LabelledArcs<'arc>
    where
        Self: 'arc;
    type LabelledPredecessors<'node> = FilteredLabelledPredecessors<
        'node,
        Self::LabelledArcs<'node>,
        <<G as SwhLabelledBackwardGraph>::LabelledPredecessors<'node> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
    >
    where
        Self: 'node;

    fn labelled_predecessors(&self, node_id: NodeId) -> Self::LabelledPredecessors<'_> {
        FilteredLabelledPredecessors {
            inner: self.graph.labelled_predecessors(node_id).into_iter(),
            node: node_id,
            node_filter: &self.node_filter,
            arc_filter: &self.arc_filter,
        }
    }
}

impl<
        G: SwhGraphWithProperties,
        NodeFilter: Fn(usize) -> bool,
        ArcFilter: Fn(usize, usize) -> bool,
    > SwhGraphWithProperties for Subgraph<G, NodeFilter, ArcFilter>
{
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
        self.graph.properties()
    }
}
