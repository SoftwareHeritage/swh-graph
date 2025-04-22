// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;
use std::sync::Arc;

use swh_graph::graph::*;
use swh_graph::properties;
use swh_graph::views::{Subgraph, Transposed};

pub trait FullOptGraph: SwhGraph + Clone + Sync + Send {
}

impl<UG: UnderlyingGraph, P> FullOptGraph for

/*
impl<G: FullOptGraph> FullOptGraph for Arc<G> {
    type ForwardArcsAvailability = G::ForwardArcsAvailability;
    type WithForwardArcs = G::WithForwardArcs;
    //const FORWARD_ARCS: bool = G::FORWARD_ARCS;

    fn with_forward_arcs(
        &self,
    ) -> impl ConstOption2<Available = Self::ForwardArcsAvailability, Graph = Self::WithForwardArcs>
    {
        G::with_forward_arcs(self)
    }
}

impl<
        G: FullOptGraph,
        NodeFilter: Sync + Send + Fn(NodeId) -> bool,
        ArcFilter: Sync + Send + Fn(NodeId, NodeId) -> bool,
    > FullOptGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type ForwardArcsAvailability = G::ForwardArcsAvailability;
    type WithForwardArcs = Subgraph<G::WithForwardArcs, NodeFilter, ArcFilter>;
    //const FORWARD_ARCS: bool = G::FORWARD_ARCS;

    fn with_forward_arcs(
        &self,
    ) -> impl ConstOption2<Available = Self::ForwardArcsAvailability, Graph = Self::WithForwardArcs>
    {
        <G as FullOptGraph>::with_forward_arcs(&self.graph).map(|graph| Subgraph {
            graph,
            node_filter: &self.node_filter,
            arc_filter: &self.arc_filter,
        })
    }
}
*/
