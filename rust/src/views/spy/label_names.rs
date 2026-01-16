// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::LabelNames`] spy for [`GraphSpy`]

use super::*;

impl<
        G: SwhGraphWithProperties<LabelNames: properties::LabelNames>,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
    > GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, properties::NoLabelNames>
{
    pub fn with_label_names(
        self,
    ) -> GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LabelNamesSpy<G>> {
        let GraphSpy {
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
            history,
        } = self;

        GraphSpy {
            properties: properties::SwhGraphProperties {
                path,
                num_nodes,
                maps,
                timestamps,
                persons,
                contents,
                strings,
                label_names: LabelNamesSpy {
                    graph: Arc::clone(&inner),
                    history: Arc::clone(&history),
                },
                label_names_are_in_base64_order,
            },
            inner,
            history,
        }
    }
}

pub struct LabelNamesSpy<G: SwhGraphWithProperties<LabelNames: properties::LabelNames>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
}

impl<G: SwhGraphWithProperties<LabelNames: properties::LabelNames>> properties::LabelNames
    for LabelNamesSpy<G>
{
    type LabelNames<'a>
        = <<G as SwhGraphWithProperties>::LabelNames as properties::LabelNames>::LabelNames<'a>
    where
        Self: 'a;

    fn label_names(&self) -> Self::LabelNames<'_> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::LabelNames));
        self.graph.graph.properties().label_names.label_names()
    }
}
