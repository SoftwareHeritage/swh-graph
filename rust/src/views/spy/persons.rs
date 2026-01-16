// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Persons`] spy for [`GraphSpy`]

use super::*;
use properties::{OptPersons, PropertiesBackend, PropertiesResult};

impl<
        G: SwhGraphWithProperties<Persons: OptPersons>,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > GraphSpy<G, MAPS, TIMESTAMPS, properties::NoPersons, CONTENTS, STRINGS, LABELNAMES>
{
    pub fn with_persons(
        self,
    ) -> GraphSpy<G, MAPS, TIMESTAMPS, PersonsSpy<G>, CONTENTS, STRINGS, LABELNAMES> {
        let GraphSpy {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps,
                    persons: properties::NoPersons,
                    contents,
                    strings,
                    label_names,
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
                persons: PersonsSpy {
                    graph: Arc::clone(&inner),
                    history: Arc::clone(&history),
                },
                contents,
                strings,
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
            history,
        }
    }
}

pub struct PersonsSpy<G: SwhGraphWithProperties<Persons: OptPersons>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
}

impl<G: SwhGraphWithProperties<Persons: OptPersons>> PropertiesBackend for PersonsSpy<G> {
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Persons as PropertiesBackend>::DataFilesAvailability;
}

impl<G: SwhGraphWithProperties<Persons: OptPersons>> OptPersons for PersonsSpy<G> {
    fn author_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::AuthorId(node)));
        self.graph.graph.properties().persons.author_id(node)
    }

    fn committer_id(&self, node: NodeId) -> PropertiesResult<'_, Option<u32>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::CommitterId(
                node,
            )));
        self.graph.graph.properties().persons.committer_id(node)
    }
}
