// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Maps`] for [`ContiguousSubgraph`]

use super::*;

impl<
        G: SwhGraphWithProperties<Maps: properties::Maps>,
        N: ContractionBackend,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    >
    ContiguousSubgraph<G, N, properties::NoMaps, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Makes [`properties::Maps`] available on this [`ContiguousSubgraph`].
    ///
    /// Requires the underlying graph to implement [`properties::Maps`].
    pub fn with_maps(
        self,
    ) -> ContiguousSubgraph<
        G,
        N,
        ContiguousSubgraphMaps<G, N>,
        TIMESTAMPS,
        PERSONS,
        CONTENTS,
        STRINGS,
        LABELNAMES,
    > {
        let ContiguousSubgraph {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps: properties::NoMaps,
                    timestamps,
                    persons,
                    contents,
                    strings,
                    label_names,
                    label_names_are_in_base64_order,
                },
            inner,
        } = self;

        ContiguousSubgraph {
            properties: properties::SwhGraphProperties {
                path,
                num_nodes,
                maps: ContiguousSubgraphMaps {
                    graph: Arc::clone(&inner),
                    mphf: ContiguousSubgraphMphf {
                        graph: Arc::clone(&inner),
                    },
                },
                timestamps,
                persons,
                contents,
                strings,
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
        }
    }
}

/// View for [`MaybeMaps`] that renumbers nodes, as part of [`ContiguousSubgraph`
pub struct ContiguousSubgraphMaps<
    G: SwhGraphWithProperties<Maps: properties::Maps>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
    mphf: ContiguousSubgraphMphf<G, N>,
}

impl<G: SwhGraphWithProperties<Maps: properties::Maps>, N: ContractionBackend> properties::Maps
    for ContiguousSubgraphMaps<G, N>
{
    type MPHF = ContiguousSubgraphMphf<G, N>;

    #[inline(always)]
    fn mphf(&self) -> &Self::MPHF {
        &self.mphf
    }
    #[inline(always)]
    fn node2swhid(&self, node: NodeId) -> Result<SWHID, OutOfBoundError> {
        self.graph
            .underlying_graph
            .properties()
            .maps
            .node2swhid(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn node2type(&self, node: NodeId) -> Result<NodeType, OutOfBoundError> {
        self.graph
            .underlying_graph
            .properties()
            .maps
            .node2type(self.graph.contraction.underlying_node_id(node))
    }
}

/// View for [`SwhidMphf`] that renumbers nodes, as part of [`ContiguousSubgraph`]
pub struct ContiguousSubgraphMphf<
    G: SwhGraphWithProperties<Maps: properties::Maps>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
}

impl<G: SwhGraphWithProperties<Maps: properties::Maps>, N: ContractionBackend> SwhidMphf
    for ContiguousSubgraphMphf<G, N>
{
    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        use properties::Maps;

        self.graph
            .underlying_graph
            .properties()
            .maps
            .mphf()
            .hash_array(swhid)
            .and_then(|underlying_node| {
                self.graph
                    .contraction
                    .node_id_from_underlying(underlying_node)
            })
    }

    /// Hashes a SWHID's textual representation
    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        use properties::Maps;

        self.graph
            .underlying_graph
            .properties()
            .maps
            .mphf()
            .hash_str(swhid)
            .and_then(|underlying_node| {
                self.graph
                    .contraction
                    .node_id_from_underlying(underlying_node)
            })
    }

    /// Hashes a SWHID's textual representation
    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        use properties::Maps;

        self.graph
            .underlying_graph
            .properties()
            .maps
            .mphf()
            .hash_str_array(swhid)
            .and_then(|underlying_node| {
                self.graph
                    .contraction
                    .node_id_from_underlying(underlying_node)
            })
    }

    /// Hashes a [`SWHID`]
    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        use properties::Maps;

        self.graph
            .underlying_graph
            .properties()
            .maps
            .mphf()
            .hash_swhid(swhid)
            .and_then(|underlying_node| {
                self.graph
                    .contraction
                    .node_id_from_underlying(underlying_node)
            })
    }
}
