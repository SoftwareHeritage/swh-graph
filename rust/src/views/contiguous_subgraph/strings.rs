// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Strings`] for [`ContiguousSubgraph`]

use super::*;
use properties::{NoStrings, OptStrings, PropertiesBackend, PropertiesResult, SwhGraphProperties};

impl<
        G: SwhGraphWithProperties<Strings: OptStrings>,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        LABELNAMES: properties::MaybeLabelNames,
    > ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, NoStrings, LABELNAMES>
{
    /// Makes [`OptStrings`] available on this [`ContiguousSubgraph`].
    ///
    /// Requires the underlying graph to implement [`OptStrings`].
    pub fn with_strings(
        self,
    ) -> ContiguousSubgraph<
        G,
        N,
        MAPS,
        TIMESTAMPS,
        PERSONS,
        CONTENTS,
        ContiguousSubgraphStrings<G, N>,
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
                    contents,
                    strings: NoStrings,
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
                contents,
                strings: ContiguousSubgraphStrings {
                    graph: Arc::clone(&inner),
                },
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
        }
    }
}

/// View for [`MaybeStrings`] that renumbers nodes, as part of [`ContiguousSubgraph`]
pub struct ContiguousSubgraphStrings<
    G: SwhGraphWithProperties<Strings: OptStrings>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
}

impl<G: SwhGraphWithProperties<Strings: OptStrings>, N: ContractionBackend> PropertiesBackend
    for ContiguousSubgraphStrings<G, N>
{
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Strings as PropertiesBackend>::DataFilesAvailability;
}
impl<G: SwhGraphWithProperties<Strings: OptStrings>, N: ContractionBackend> OptStrings
    for ContiguousSubgraphStrings<G, N>
{
    #[inline(always)]
    fn message(&self) -> PropertiesResult<'_, &[u8], Self> {
        // returned as-is, we only need to change the offsets
        self.graph.underlying_graph.properties().strings.message()
    }
    #[inline(always)]
    fn message_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .strings
            .message_offset(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn tag_name(&self) -> PropertiesResult<'_, &[u8], Self> {
        // returned as-is, we only need to change the offsets
        self.graph.underlying_graph.properties().strings.tag_name()
    }
    #[inline(always)]
    fn tag_name_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .strings
            .tag_name_offset(self.graph.contraction.underlying_node_id(node))
    }
}
