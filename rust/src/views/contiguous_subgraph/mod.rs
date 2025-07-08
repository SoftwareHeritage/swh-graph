// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
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
pub trait NodeMapBackend:
    IndexedSeq<Input = NodeId, Output = NodeId> + IndexedDict<Input = NodeId, Output = NodeId>
{
}

impl<
        B: IndexedSeq<Input = NodeId, Output = NodeId> + IndexedDict<Input = NodeId, Output = NodeId>,
    > NodeMapBackend for B
{
}

/// See [`ContiguousSubgraph`]
pub struct NodeMap<N: IndexedSeq<Input = NodeId, Output = NodeId>>(pub N);

impl<N: IndexedSeq<Input = NodeId, Output = NodeId>> NodeMap<N> {
    /// Given a node id in a [`ContiguousSubgraph`], returns the corresponding node id
    /// in the [`ContiguousSubgraph`]'s underlying graph
    #[inline(always)]
    pub fn underlying_node_id(&self, self_node: NodeId) -> NodeId {
        self.0.get(self_node)
    }
}

impl<N: NodeMapBackend> NodeMap<N> {
    /// Given a node id in a [`ContiguousSubgraph`]'s underlying graph, returns the
    /// corresponding node id in the [`ContiguousSubgraph`]
    #[inline(always)]
    pub fn node_id_from_underlying(&self, underlying_node: NodeId) -> Option<NodeId> {
        self.0.index_of(underlying_node)
    }
}

/// A view over [`SwhGraph`] and related traits, that filters out some node_map and arcs
/// based on arbitrary closures.
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
/// use sux::dict::elias_fano::{EliasFanoBuilder, EfSeqDict};
/// use swh_graph::properties;
/// use swh_graph::NodeType;
/// use swh_graph::graph::SwhGraphWithProperties;
/// use swh_graph::views::{ContiguousSubgraph, NodeMap};
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
///     let node_map = NodeMap(nodes_efb.build_with_seq_and_dict());
///
///     // assemble the subgraph
///     ContiguousSubgraph::new(graph, node_map)
/// }
/// ```
pub struct ContiguousSubgraph<
    G: SwhGraph,
    N: NodeMapBackend,
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

impl<G: SwhGraphWithProperties, N: NodeMapBackend>
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
    pub fn new(graph: G, node_map: NodeMap<N>) -> Self {
        let path = graph.properties().path.clone();
        let num_nodes = node_map.0.len();
        let inner = Arc::new(ContiguousSubgraphInner {
            underlying_graph: graph,
            node_map,
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

// content of ContiguousSubgraph that must be wrapped in an Arc in order to make
// ContiguousSubgraphMaps live as long as G (which is an accidental requirement of
// SwhGraphWithProperties, which doesn't seem to be removable without making its
// API painfully hard to use).
struct ContiguousSubgraphInner<G: SwhGraph, N: NodeMapBackend> {
    underlying_graph: G,
    node_map: NodeMap<N>,
}

impl<
        G: SwhGraph,
        N: NodeMapBackend,
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
        self.inner.node_map.0.len()
    }
    fn has_node(&self, node_id: NodeId) -> bool {
        node_id < self.num_nodes()
            && self
                .inner
                .underlying_graph
                .has_node(self.inner.node_map.underlying_node_id(node_id))
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
            self.inner.node_map.underlying_node_id(src_node_id),
            self.inner.node_map.underlying_node_id(dst_node_id),
        )
    }
}

impl<
        G: SwhGraphWithProperties,
        N: NodeMapBackend,
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
