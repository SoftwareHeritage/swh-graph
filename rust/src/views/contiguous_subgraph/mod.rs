// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;
use sux::dict::elias_fano::{EfSeqDict, EliasFanoBuilder};
use sux::prelude::{IndexedDict, IndexedSeq};

use crate::graph::*;
use crate::mph::SwhidMphf;
use crate::properties;
use crate::{NodeType, OutOfBoundError, SWHID};

mod contents;
mod iterators;
mod label_names;
mod maps;
mod persons;
mod strings;
mod timestamps;

/// Alias for [`IndexedSeq`] + [`IndexedDict`] mapping from [`NodeId`] to [`NodeId`].
pub trait ContractionBackend:
    IndexedSeq<Input = NodeId, Output = NodeId> + IndexedDict<Input = NodeId, Output = NodeId>
{
}

impl<
        B: IndexedSeq<Input = NodeId, Output = NodeId> + IndexedDict<Input = NodeId, Output = NodeId>,
    > ContractionBackend for B
{
}

/// See [`ContiguousSubgraph`]
pub struct Contraction<N: IndexedSeq<Input = NodeId, Output = NodeId>>(pub N);

impl<N: IndexedSeq<Input = NodeId, Output = NodeId>> Contraction<N> {
    /// Given a node id in a [`ContiguousSubgraph`], returns the corresponding node id
    /// in the [`ContiguousSubgraph`]'s underlying graph
    #[inline(always)]
    pub fn underlying_node_id(&self, self_node: NodeId) -> NodeId {
        self.0.get(self_node)
    }

    /// Returns the number of node ids in the [`ContiguousSubgraph`].
    ///
    /// Note: this can be an overapproximation if the underlying graph is a subgraph
    pub fn num_nodes(&self) -> usize {
        self.0.len()
    }
}

impl<N: ContractionBackend> Contraction<N> {
    /// Given a node id in a [`ContiguousSubgraph`]'s underlying graph, returns the
    /// corresponding node id in the [`ContiguousSubgraph`]
    #[inline(always)]
    pub fn node_id_from_underlying(&self, underlying_node: NodeId) -> Option<NodeId> {
        self.0.index_of(underlying_node)
    }
}

/// A view over [`SwhGraph`] and related traits, that filters out some nodes, and renumbers
/// remaining nodes so that the set of valid nodes becomes a range.
///
/// Due to limitations in the Rust type system, properties available on the underlying
/// [`SwhGraph`] are not automatically available on [`ContiguousSubgraph`].
/// They need to be explicitly enabled in a builder-like pattern on `ContiguousSubgraph`
/// using these methods:
/// * [`with_maps`][`ContiguousSubgraph::with_maps`],
/// * [`with_timestamps`][`ContiguousSubgraph::with_timestamps`],
/// * [`with_persons`][`ContiguousSubgraph::with_persons`],
/// * [`with_contents`][`ContiguousSubgraph::with_contents`],
/// * [`with_strings`][`ContiguousSubgraph::with_strings`],
/// * [`with_label_names`][`ContiguousSubgraph::with_label_names`],
///
/// # Examples
///
/// Build a `ContiguousSubgraph` made of only content and directory nodes:
///
/// ```
/// use dsi_progress_logger::progress_logger;
/// use swh_graph::properties;
/// use swh_graph::NodeConstraint;
/// use swh_graph::graph::SwhGraphWithProperties;
/// use swh_graph::views::{ContiguousSubgraph, Contraction, Subgraph, ContractionBackend};
///
/// fn filesystem_subgraph<G>(graph: &G) -> ContiguousSubgraph<
///         Subgraph<&'_ G, impl Fn(usize) -> bool + use<'_, G>, fn(usize, usize) -> bool>,
///         impl ContractionBackend,
///         properties::NoMaps,
///         properties::NoTimestamps,
///         properties::NoPersons,
///         properties::NoContents,
///         properties::NoStrings,
///         properties::NoLabelNames,
///     >
///     where G: SwhGraphWithProperties<Maps: properties::Maps> + Sync {
///     let sparse_subgraph = Subgraph::with_node_constraint(graph, "cnt,ori".parse().unwrap());
///     ContiguousSubgraph::new_from_noncontiguous_graph(sparse_subgraph)
/// }
/// ```
///
/// The graph created above is slightly suboptimal, as `ContiguousSubgraph` wraps
/// a `Subgraph`, causing `Subgraph` to add needless overhead by checking that nodes
/// exist, even though `ContiguousSubgraph` does it too.
/// This should not be noticeable, but if it is an issue, you can skip the Subgraph
/// by manually building a [`Contraction`]:
///
/// ```
/// use dsi_progress_logger::progress_logger;
/// use sux::dict::elias_fano::{EfSeqDict, EliasFanoBuilder};
/// use swh_graph::properties;
/// use swh_graph::{NodeType};
/// use swh_graph::graph::SwhGraphWithProperties;
/// use swh_graph::views::{ContiguousSubgraph, Contraction, Subgraph, ContractionBackend};
///
/// fn filesystem_subgraph<G>(graph: &G) -> ContiguousSubgraph<
///         &'_ G,
///         EfSeqDict,
///         properties::NoMaps,
///         properties::NoTimestamps,
///         properties::NoPersons,
///         properties::NoContents,
///         properties::NoStrings,
///         properties::NoLabelNames,
///     >
///     where G: SwhGraphWithProperties<Maps: properties::Maps> {
///
///     // compute exact number of nodes in the subgraph, which is required
///     // by EliasFanoBuilder
///     let pl = progress_logger!(
///         item_name = "node",
///         expected_updates = Some(graph.num_nodes()),
///     );
///     let num_nodes = graph
///         .iter_nodes(pl)
///         .filter(|&node| match graph.properties().node_type(node) {
///             NodeType::Content | NodeType::Directory => true,
///             _ => false,
///         })
///         .count();
///
///     // compute set of nodes in the subgraph
///     let mut nodes_efb = EliasFanoBuilder::new(num_nodes, graph.num_nodes());
///     for node in 0..graph.num_nodes() {
///         match graph.properties().node_type(node) {
///             NodeType::Content | NodeType::Directory => nodes_efb.push(node),
///             _ => (),
///         }
///     }
///     let contraction = Contraction(nodes_efb.build_with_seq_and_dict());
///
///     // assemble the subgraph
///     ContiguousSubgraph::new_from_contraction(graph, contraction)
/// }
/// ```
pub struct ContiguousSubgraph<
    G: SwhGraph,
    N: ContractionBackend,
    MAPS: properties::MaybeMaps,
    TIMESTAMPS: properties::MaybeTimestamps,
    PERSONS: properties::MaybePersons,
    CONTENTS: properties::MaybeContents,
    STRINGS: properties::MaybeStrings,
    LABELNAMES: properties::MaybeLabelNames,
> {
    inner: Arc<ContiguousSubgraphInner<G, N>>, // TODO: find a way to replace Arc with ouroboros
    properties:
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>,
}

impl<G: SwhGraphWithProperties, N: ContractionBackend>
    ContiguousSubgraph<
        G,
        N,
        properties::NoMaps,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >
{
    /// Creates a new [`ContiguousSubgraph`] by keeping only nodes in the [`Contraction`]
    pub fn new_from_contraction(graph: G, contraction: Contraction<N>) -> Self {
        let path = graph.properties().path.clone();
        let num_nodes = contraction.num_nodes();
        let inner = Arc::new(ContiguousSubgraphInner {
            underlying_graph: graph,
            contraction,
        });
        Self {
            properties: properties::SwhGraphProperties {
                path,
                num_nodes,
                maps: properties::NoMaps,
                timestamps: properties::NoTimestamps,
                persons: properties::NoPersons,
                contents: properties::NoContents,
                strings: properties::NoStrings,
                label_names: properties::NoLabelNames,
                label_names_are_in_base64_order: Default::default(),
            },
            inner,
        }
    }
}

impl<G: SwhGraph, N: ContractionBackend>
    ContiguousSubgraph<
        G,
        N,
        properties::NoMaps,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >
{
    /// Returns the graph this graph is a subgraph of
    ///
    /// Use [`Self::contraction`] to get the mapping between both sets of node ids
    pub fn underlying_graph(&self) -> &G {
        &self.inner.underlying_graph
    }

    /// The structure used to match the underlying graph's node ids with this graph's node ids
    pub fn contraction(&self) -> &Contraction<N> {
        &self.inner.contraction
    }
}

impl<G: SwhGraphWithProperties>
    ContiguousSubgraph<
        G,
        EfSeqDict,
        properties::NoMaps,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >
{
    /// Creates a new [`ContiguousSubgraph`] from an existing graph with "holes".
    pub fn new_from_noncontiguous_graph(graph: G) -> Self
    where
        G: Sync,
    {
        // compute exact number of nodes in the subgraph, which is required
        // by EliasFanoBuilder
        let actual_num_nodes = graph.actual_num_nodes().unwrap_or_else(|_| {
            let mut pl = concurrent_progress_logger!(
                item_name = "node",
                expected_updates = Some(graph.num_nodes()),
            );
            pl.start("Computing number of nodes");
            let actual_num_nodes = graph
                .par_iter_nodes(pl.clone())
                .filter(|&node| graph.has_node(node))
                .count();
            pl.done();
            actual_num_nodes
        });

        // compute set of nodes in the subgraph
        let mut nodes_efb = EliasFanoBuilder::new(actual_num_nodes, graph.num_nodes());
        for node in 0..graph.num_nodes() {
            if graph.has_node(node) {
                nodes_efb.push(node);
            }
        }
        let contraction = Contraction(nodes_efb.build_with_seq_and_dict());

        Self::new_from_contraction(graph, contraction)
    }
}

// content of ContiguousSubgraph that must be wrapped in an Arc in order to make
// ContiguousSubgraphMaps live as long as G (which is an accidental requirement of
// SwhGraphWithProperties, which doesn't seem to be removable without making its
// API painfully hard to use).
struct ContiguousSubgraphInner<G: SwhGraph, N: ContractionBackend> {
    underlying_graph: G,
    contraction: Contraction<N>,
}

impl<
        G: SwhGraph,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhGraph
    for ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    #[inline(always)]
    fn path(&self) -> &Path {
        self.inner.underlying_graph.path()
    }
    #[inline(always)]
    fn is_transposed(&self) -> bool {
        self.inner.underlying_graph.is_transposed()
    }
    #[inline(always)]
    // Note: this can be an overapproximation if the underlying graph is a subgraph
    fn num_nodes(&self) -> usize {
        self.inner.contraction.num_nodes()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        node_id < self.num_nodes()
            && self
                .inner
                .underlying_graph
                .has_node(self.inner.contraction.underlying_node_id(node_id))
    }
    #[inline(always)]
    // Note: this return the number or arcs in the original graph, before
    // subgraph filtering.
    fn num_arcs(&self) -> u64 {
        self.inner.underlying_graph.num_arcs()
    }
    fn num_nodes_by_type(&self) -> Result<HashMap<NodeType, usize>> {
        bail!("num_nodes_by_type is not supported by ContiguousSubgraph")
    }
    fn num_arcs_by_type(&self) -> Result<HashMap<(NodeType, NodeType), usize>> {
        bail!("num_arcs_by_type is not supported by ContiguousSubgraph")
    }
    fn has_arc(&self, src_node_id: NodeId, dst_node_id: NodeId) -> bool {
        self.inner.underlying_graph.has_arc(
            self.inner.contraction.underlying_node_id(src_node_id),
            self.inner.contraction.underlying_node_id(dst_node_id),
        )
    }
}

impl<
        G: SwhGraphWithProperties,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhGraphWithProperties
    for ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;
    type LabelNames = LABELNAMES;

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
        &self.properties
    }
}
