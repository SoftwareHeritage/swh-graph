// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Persons`] for [`ContiguousSubgraph`]

use super::*;
use properties::{NoPersons, OptPersons, PropertiesBackend, PropertiesResult, SwhGraphProperties};

impl<
        G: SwhGraphWithProperties<Persons: OptPersons>,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, NoPersons, CONTENTS, STRINGS, LABELNAMES>
{
    /// Makes [`OptPersons`] available on this [`ContiguousSubgraph`].
    ///
    /// Requires the underlying graph to implement [`OptPersons`].
    pub fn with_persons(
        self,
    ) -> ContiguousSubgraph<
        G,
        N,
        MAPS,
        TIMESTAMPS,
        ContiguousSubgraphPersons<G, N>,
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
                    timestamps,
                    persons: NoPersons,
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
                timestamps,
                persons: ContiguousSubgraphPersons {
                    graph: Arc::clone(&inner),
                },
                contents,
                strings,
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
        }
    }
}

/// View for [`MaybePersons`] that renumbers nodes, as part of [`ContiguousSubgraph`]
pub struct ContiguousSubgraphPersons<
    G: SwhGraphWithProperties<Persons: OptPersons>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
}

impl<G: SwhGraphWithProperties<Persons: OptPersons>, N: ContractionBackend> PropertiesBackend
    for ContiguousSubgraphPersons<G, N>
{
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Persons as PropertiesBackend>::DataFilesAvailability;
}
impl<G: SwhGraphWithProperties<Persons: OptPersons>, N: ContractionBackend> OptPersons
    for ContiguousSubgraphPersons<G, N>
{
    #[inline(always)]
    fn author_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .persons
            .author_id(self.graph.contraction.underlying_node_id(node))
    }
    #[inline(always)]
    fn committer_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self> {
        self.graph
            .underlying_graph
            .properties()
            .persons
            .committer_id(self.graph.contraction.underlying_node_id(node))
    }
}
