// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Timestamps`] for [`ContiguousSubgraph`]

use super::*;
use properties::{
    NoTimestamps, OptTimestamps, PropertiesBackend, PropertiesResult, SwhGraphProperties,
};

impl<
        G: SwhGraphWithProperties<Timestamps: OptTimestamps>,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > ContiguousSubgraph<G, N, MAPS, NoTimestamps, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Makes [`OptTimestamps`] available on this [`ContiguousSubgraph`].
    ///
    /// Requires the underlying graph to implement [`OptTimestamps`].
    pub fn with_timestamps(
        self,
    ) -> ContiguousSubgraph<
        G,
        N,
        MAPS,
        ContiguousSubgraphTimestamps<G, N>,
        PERSONS,
        CONTENTS,
        STRINGS,
        LABELNAMES,
    > {
        let ContiguousSubgraph {
            properties:
                SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps: NoTimestamps,
                    persons,
                    contents,
                    strings,
                    label_names,
                    label_names_are_in_base64_order,
                },
            inner,
        } = self;

        ContiguousSubgraph {
            properties: SwhGraphProperties {
                path,
                num_nodes,
                maps,
                timestamps: ContiguousSubgraphTimestamps {
                    graph: Arc::clone(&inner),
                },
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

/// View for [`MaybeTimestamps`] that renumbers nodes, as part of [`ContiguousSubgraph`]
pub struct ContiguousSubgraphTimestamps<
    G: SwhGraphWithProperties<Timestamps: OptTimestamps>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
}

impl<G: SwhGraphWithProperties<Timestamps: OptTimestamps>, N: ContractionBackend> PropertiesBackend
    for ContiguousSubgraphTimestamps<G, N>
{
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Timestamps as PropertiesBackend>::DataFilesAvailability;
}
impl<G: SwhGraphWithProperties<Timestamps: OptTimestamps>, N: ContractionBackend> OptTimestamps
    for ContiguousSubgraphTimestamps<G, N>
{
    #[inline(always)]
    fn author_timestamp(&self, node: NodeId) -> PropertiesResult<'_, Option<i64>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .timestamps
            .author_timestamp(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn author_timestamp_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<i16>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .timestamps
            .author_timestamp_offset(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn committer_timestamp(&self, node: NodeId) -> PropertiesResult<'_, Option<i64>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .timestamps
            .committer_timestamp(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn committer_timestamp_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<i16>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .timestamps
            .committer_timestamp_offset(self.graph.contraction.underlying_node_id(node))
    }
}
