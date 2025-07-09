// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::LabelNames`] for [`LabelNames`]

use super::*;

impl<
        G: SwhGraphWithProperties<LabelNames: properties::LabelNames>,
        N: ContractionBackend,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
    >
    ContiguousSubgraph<G, N, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, properties::NoLabelNames>
{
    /// Makes [`properties::LabelNames`] available on this [`ContiguousSubgraph`].
    ///
    /// Requires the underlying graph to implement [`properties::LabelNames`].
    pub fn with_label_names(
        self,
    ) -> ContiguousSubgraph<
        G,
        N,
        MAPS,
        TIMESTAMPS,
        PERSONS,
        CONTENTS,
        STRINGS,
        ContiguousSubgraphLabelNames<G, N>,
    > {
        let ContiguousSubgraph {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps,
                    persons,
                    contents,
                    strings,
                    label_names: properties::NoLabelNames,
                    label_names_are_in_base64_order,
                },
            inner,
        } = self;

        ContiguousSubgraph {
            properties: properties::SwhGraphProperties {
                path,
                num_nodes,
                maps,
                timestamps,
                persons,
                contents,
                strings,
                label_names: ContiguousSubgraphLabelNames {
                    graph: Arc::clone(&inner),
                },
                label_names_are_in_base64_order,
            },
            inner,
        }
    }
}

/// View for [`MaybeMaps`] that renumbers nodes, as part of [`ContiguousSubgraph`
pub struct ContiguousSubgraphLabelNames<
    G: SwhGraphWithProperties<LabelNames: properties::LabelNames>,
    N: ContractionBackend,
> {
    graph: Arc<ContiguousSubgraphInner<G, N>>,
}

impl<G: SwhGraphWithProperties<LabelNames: properties::LabelNames>, N: ContractionBackend>
    properties::LabelNames for ContiguousSubgraphLabelNames<G, N>
{
    type LabelNames<'a>
        = <<G as SwhGraphWithProperties>::LabelNames as properties::LabelNames>::LabelNames<'a>
    where
        Self: 'a;

    fn label_names(&self) -> Self::LabelNames<'_> {
        self.graph
            .underlying_graph
            .properties()
            .label_names
            .label_names()
    }
}
