// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Contents`] for [`ContiguousSubgraph`]

use super::*;
use properties::{
    NoContents, OptContents, PropertiesBackend, PropertiesResult, SwhGraphProperties,
};

impl<
        G: SwhGraphWithProperties<Contents: OptContents>,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, NoContents, STRINGS, LABELNAMES>
{
    /// Makes [`OptContents`] available on this [`ContiguousSubgraph`].
    ///
    /// Requires the underlying graph to implement [`OptContents`]
    pub fn with_contents(
        self,
    ) -> ContiguousSubgraph<
        G,
        N,
        MAPS,
        TIMESTAMPS,
        PERSONS,
        ContiguousSubgraphContents<G, N>,
        STRINGS,
        LABELNAMES,
    > {
        let ContiguousSubgraph {
            properties:
                SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps,
                    persons,
                    contents: NoContents,
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
                timestamps,
                persons,
                contents: ContiguousSubgraphContents {
                    graph: Arc::clone(&inner),
                },
                strings,
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
        }
    }
}

/// View for [`MaybeContents`] that renumbers nodes, as part of [`ContiguousSubgraph`]
pub struct ContiguousSubgraphContents<
    G: SwhGraphWithProperties<Contents: OptContents>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
}

impl<G: SwhGraphWithProperties<Contents: OptContents>, N: ContractionBackend> PropertiesBackend
    for ContiguousSubgraphContents<G, N>
{
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Contents as PropertiesBackend>::DataFilesAvailability;
}
impl<G: SwhGraphWithProperties<Contents: OptContents>, N: ContractionBackend> OptContents
    for ContiguousSubgraphContents<G, N>
{
    #[inline(always)]
    fn is_skipped_content(&self, node: NodeId) -> PropertiesResult<'_, Option<bool>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .contents
            .is_skipped_content(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn content_length(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .contents
            .content_length(self.graph.contraction.underlying_node_id(node))
    }
}
