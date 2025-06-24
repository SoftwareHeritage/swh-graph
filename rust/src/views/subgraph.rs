// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, Result};

use crate::arc_iterators::FlattenedSuccessorsIterator;
use crate::graph::*;
use crate::properties;
use crate::{NodeConstraint, NodeType};

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

macro_rules! make_filtered_labeled_arcs_iterator {
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

        impl<
            'a,
            Labels: IntoIterator,
            $inner: Iterator<Item = (NodeId, Labels)> + 'a,
            NodeFilter: Fn(NodeId) -> bool,
            ArcFilter: Fn(NodeId, NodeId) -> bool,
        > IntoFlattenedLabeledArcsIterator<<Labels as IntoIterator>::Item> for $name<'a, Labels, $inner, NodeFilter, ArcFilter> {
            type Flattened = FlattenedSuccessorsIterator<Self>;

            fn flatten_labels(self) -> Self::Flattened {
                FlattenedSuccessorsIterator::new(self)
            }
        }
    }
}

make_filtered_labeled_arcs_iterator! {
    FilteredLabeledSuccessors,
    LabeledSuccessors,
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
make_filtered_labeled_arcs_iterator! {
    FilteredLabeledPredecessors,
    LabeledPredecessors,
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
    pub num_nodes_by_type: Option<HashMap<NodeType, usize>>,
    pub num_arcs_by_type: Option<HashMap<(NodeType, NodeType), usize>>,
}

impl<G: SwhGraph, NodeFilter: Fn(usize) -> bool> Subgraph<G, NodeFilter, fn(usize, usize) -> bool> {
    /// Create a [Subgraph] keeping only nodes matching a given node filter function.
    ///
    /// Shorthand for `Subgraph { graph, node_filter, arc_filter: |_src, _dst| true }`
    pub fn with_node_filter(
        graph: G,
        node_filter: NodeFilter,
    ) -> Subgraph<G, NodeFilter, fn(usize, usize) -> bool> {
        Subgraph {
            graph,
            node_filter,
            arc_filter: |_src, _dst| true,
            num_nodes_by_type: None,
            num_arcs_by_type: None,
        }
    }
}

impl<G: SwhGraph, ArcFilter: Fn(usize, usize) -> bool> Subgraph<G, fn(usize) -> bool, ArcFilter> {
    /// Create a [Subgraph] keeping only arcs matching a arc filter function.
    ///
    /// Shorthand for `Subgraph { graph, node_filter: |_node| true, arc_filter }`
    pub fn with_arc_filter(
        graph: G,
        arc_filter: ArcFilter,
    ) -> Subgraph<G, fn(usize) -> bool, ArcFilter> {
        Subgraph {
            graph,
            node_filter: |_node| true,
            arc_filter,
            num_nodes_by_type: None,
            num_arcs_by_type: None,
        }
    }
}

impl<G> Subgraph<G, fn(usize) -> bool, fn(usize, usize) -> bool>
where
    G: SwhGraphWithProperties + Clone,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    /// Create a [Subgraph] keeping only nodes matching a given node constraint.
    #[allow(clippy::type_complexity)]
    pub fn with_node_constraint(
        graph: G,
        node_constraint: NodeConstraint,
    ) -> Subgraph<G, impl Fn(NodeId) -> bool, fn(usize, usize) -> bool> {
        Subgraph {
            graph: graph.clone(),
            num_nodes_by_type: graph.num_nodes_by_type().ok().map(|counts| {
                counts
                    .into_iter()
                    .filter(|&(type_, _count)| node_constraint.matches(type_))
                    .collect()
            }),
            num_arcs_by_type: graph.num_arcs_by_type().ok().map(|counts| {
                counts
                    .into_iter()
                    .filter(|&((src_type, dst_type), _count)| {
                        node_constraint.matches(src_type) && node_constraint.matches(dst_type)
                    })
                    .collect()
            }),
            node_filter: move |node| node_constraint.matches(graph.properties().node_type(node)),
            arc_filter: |_src, _dst| true,
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
    // Note: this return the number or nodes in the original graph, before
    // subgraph filtering.
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        (self.node_filter)(node_id)
    }
    // Note: this return the number or arcs in the original graph, before
    // subgraph filtering.
    fn num_arcs(&self) -> u64 {
        self.graph.num_arcs()
    }
    fn num_nodes_by_type(&self) -> Result<HashMap<NodeType, usize>> {
        self.num_nodes_by_type.clone().ok_or(anyhow!(
            "num_nodes_by_type is not supported by this Subgraph (if possible, use Subgraph::with_node_constraint to build it)"
        ))
    }
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        self.num_arcs_by_type.clone().ok_or(anyhow!(
            "num_arcs_by_type is not supported by this Subgraph (if possible, use Subgraph::with_node_constraint to build it)"
        ))
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        (self.node_filter)(src_node_id)
            && (self.node_filter)(dst_node_id)
            && (self.arc_filter)(src_node_id, dst_node_id)
            && self.graph.has_arc(src_node_id, dst_node_id)
    }
}

impl<G: SwhForwardGraph, NodeFilter: Fn(usize) -> bool, ArcFilter: Fn(usize, usize) -> bool>
    SwhForwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type Successors<'succ>
        = FilteredSuccessors<
        'succ,
        <<G as SwhForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
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
    type Predecessors<'succ>
        = FilteredPredecessors<
        'succ,
        <<G as SwhBackwardGraph>::Predecessors<'succ> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
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
        G: SwhLabeledForwardGraph,
        NodeFilter: Fn(usize) -> bool,
        ArcFilter: Fn(usize, usize) -> bool,
    > SwhLabeledForwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type LabeledArcs<'arc>
        = <G as SwhLabeledForwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledSuccessors<'node>
        = FilteredLabeledSuccessors<
        'node,
        Self::LabeledArcs<'node>,
        <<G as SwhLabeledForwardGraph>::LabeledSuccessors<'node> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
    >
    where
        Self: 'node;

    fn untyped_labeled_successors(&self, node_id: NodeId) -> Self::LabeledSuccessors<'_> {
        FilteredLabeledSuccessors {
            inner: self.graph.untyped_labeled_successors(node_id).into_iter(),
            node: node_id,
            node_filter: &self.node_filter,
            arc_filter: &self.arc_filter,
        }
    }
}

impl<
        G: SwhLabeledBackwardGraph,
        NodeFilter: Fn(usize) -> bool,
        ArcFilter: Fn(usize, usize) -> bool,
    > SwhLabeledBackwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type LabeledArcs<'arc>
        = <G as SwhLabeledBackwardGraph>::LabeledArcs<'arc>
    where
        Self: 'arc;
    type LabeledPredecessors<'node>
        = FilteredLabeledPredecessors<
        'node,
        Self::LabeledArcs<'node>,
        <<G as SwhLabeledBackwardGraph>::LabeledPredecessors<'node> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
    >
    where
        Self: 'node;

    fn untyped_labeled_predecessors(&self, node_id: NodeId) -> Self::LabeledPredecessors<'_> {
        FilteredLabeledPredecessors {
            inner: self.graph.untyped_labeled_predecessors(node_id).into_iter(),
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
