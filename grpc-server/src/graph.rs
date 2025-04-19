// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Deref;
use std::sync::Arc;

use swh_graph::graph::*;
use swh_graph::properties;
use swh_graph::views::{Subgraph, Transposed};

pub enum Never {}

impl From<Never> for tonic::Status {
    fn from(value: Never) -> tonic::Status {
        match value {}
    }
}

#[derive(thiserror::Error, Debug)]
#[error("Feature is disabled on this server: {0}")]
pub struct DisabledFeature(&'static str);

impl From<DisabledFeature> for tonic::Status {
    fn from(value: DisabledFeature) -> tonic::Status {
        tonic::Status::failed_precondition(format!(
            "Feature is disabled on this server: {}",
            value.0
        ))
    }
}

pub trait DisabledFeatureTrait: Into<tonic::Status> {}
impl DisabledFeatureTrait for DisabledFeature {}
impl DisabledFeatureTrait for Never {}

pub trait Availability {}

pub trait AvailableTrait {}
pub struct Available {}
impl AvailableTrait for Available {}

pub trait UnavailableTrait {}
pub struct Unavailable {}
impl UnavailableTrait for Unavailable {}

/// Base trait for graphs with some properties conditionally loaded
pub trait GraphWithOptProperties:
    SwhGraphWithProperties<
    Maps: properties::Maps,
    Timestamps: properties::OptTimestamps,
    Persons: properties::OptPersons,
    Contents: properties::OptContents,
    Strings: properties::OptStrings,
>
{
}
impl<
        G: SwhGraphWithProperties<
            Maps: properties::Maps,
            Timestamps: properties::OptTimestamps,
            Persons: properties::OptPersons,
            Contents: properties::OptContents,
            Strings: properties::OptStrings,
        >,
    > GraphWithOptProperties for G
{
}

//type Result<T> = std::result::Result<T, DisabledFeature>;

pub trait ConstOption {
    type Graph: SwhGraph + Clone + Sync + Send;
    type Error: DisabledFeatureTrait
    where
        tonic::Status: From<Self::Error>;

    fn get_graph(self) -> Result<Self::Graph, Self::Error>;
}

pub struct ConstSome<G: SwhGraph>(G);
impl<G: SwhGraph> ConstOption for ConstSome<G> {
    type Graph = G;
    type Error = Never;

    #[inline(always)]
    fn get_graph(self) -> Result<Self::Graph, Self::Error> {
        Ok(self)
    }
}

pub struct ConstNone;
impl ConstOption for ConstNone {
    type Graph = Never;
    type Error = DisabledFeature;

    #[inline(always)]
    fn get_graph(self) -> Result<Self::Graph, Self::Error> {
        Err(DisabledFeature("TODO"))
    }
}

pub trait ConstOption2
where
    tonic::Status: From<Self::Error>,
{
    type Available: Availability;
    type Graph: SwhGraph + Clone + Sync + Send;
    type Error: DisabledFeatureTrait;

    fn get_graph(self) -> Result<Self::Graph, Self::Error>;
    fn map<G: SwhGraph + Clone + Sync + Send>(
        self,
        f: impl FnOnce(Self::Graph) -> G,
    ) -> impl ConstOption2<Available = Self::Available, Graph = Self::Graph, Error = Self::Error>;
}

impl<G: SwhGraph> ConstOption2 for ConstSome<G> {
    type Available = Available;
    type Graph = G;
    type Error = Never;

    #[inline(always)]
    fn get_graph(self) -> Result<Self::Graph, Self::Error> {
        Ok(self)
    }
}
impl ConstOption2 for ConstNone {
    type Available = Unavailable;
    type Graph = Never;
    type Error = DisabledFeature;

    #[inline(always)]
    fn get_graph(self) -> Result<Self::Graph, Self::Error> {
        Err(DisabledFeature("TODO"))
    }
}

pub trait FullOptGraph: GraphWithOptProperties + Clone + Sync + Send {
    //type WithForwardArcs: ConstOption<Graph: SwhForwardGraph + GraphWithOptProperties>;
    type ForwardArcsAvailability: Availability;
    type WithForwardArcs: SwhForwardGraph + FullOptGraph;
    //const FORWARD_ARCS: bool;

    //fn with_arc_filter(&self, ...) -> ...;
    fn with_forward_arcs(
        &self,
    ) -> impl ConstOption2<Available = Self::ForwardArcsAvailability, Graph = Self::WithForwardArcs>;
}

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

/*
pub trait OptForwardGraph: SwhGraph {
    type Successors<'succ>: IntoIterator<Item = usize>
    where
        Self: 'succ;

    fn successors(&self, node_id: NodeId) -> Result<Self::Successors<'_>>;
    fn outdegree(&self, node_id: NodeId) -> Result<usize>;
}

impl<P, G: UnderlyingGraph> OptForwardGraph for SwhUnidirectionalGraph<P, G> {
    type Successors<'succ> = SwhUnidirectionalGraph<P, G>::Successors<'succ>;

    #[inline(always)]
    fn successors(&self, node_id: NodeId) -> Result<Self::Successors<'_>> {
        Ok(SwhUnidirectionalGraph::<P, G>::successors(self, node_id))
    }
    #[inline(always)]
    fn outdegree(&self, node_id: NodeId) -> Result<usize> {
        Ok(SwhUnidirectionalGraph::<P, G>::outdegree(self, node_id))
    }
}
impl<T: Deref> OptForwardGraph for T
where
    <T as Deref>::Target: OptForwardGraph,
{
    type Successors<'succ>
        = <<T as Deref>::Target as OptForwardGraph>::Successors<'succ>
    where
        Self: 'succ;

    #[inline(always)]
    fn successors(&self, node_id: NodeId) -> Result<Self::Successors<'_>> {
        self.deref().successors(node_id)
    }
    #[inline(always)]
    fn outdegree(&self, node_id: NodeId) -> Result<usize> {
        self.deref().outdegree(node_id)
    }
}

impl<G: OptForwardGraph, NodeFilter: Fn(usize) -> bool, ArcFilter: Fn(usize, usize) -> bool>
    OptForwardGraph for Subgraph<G, NodeFilter, ArcFilter>
{
    type Successors<'succ>
        = FilteredSuccessors<
        'succ,
        <<G as OptForwardGraph>::Successors<'succ> as IntoIterator>::IntoIter,
        NodeFilter,
        ArcFilter,
    >
    where
        Self: 'succ;

    #[inline(always)]
    fn successors(&self, node_id: NodeId) -> Result<Self::Successors<'_>> {
        self.graph
            .successors(node_id)
            .map(|successors| FilteredSuccessors {
                inner: successors.into_iter(),
                node: node_id,
                node_filter: &self.node_filter,
                arc_filter: &self.arc_filter,
            })
    }
    #[inline(always)]
    fn outdegree(&self, node_id: NodeId) -> Result<usize> {
        self.graph.outdegree(node_id)
    }
}

pub trait FullOptGraph: GraphWithOptProperties + OptForwardGraph {}
impl<G: GraphWithOptProperties + OptForwardGraph> FullOptGraph for G {}
*/
