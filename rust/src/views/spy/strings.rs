// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Strings`] spy for [`GraphSpy`]

use super::*;
use properties::{OptStrings, PropertiesBackend, PropertiesResult};

impl<
        G: SwhGraphWithProperties<Strings: OptStrings>,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        LABELNAMES: properties::MaybeLabelNames,
    > GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, properties::NoStrings, LABELNAMES>
{
    pub fn with_strings(
        self,
    ) -> GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, CONTENTS, StringsSpy<G>, LABELNAMES> {
        let GraphSpy {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps,
                    persons,
                    contents,
                    strings: properties::NoStrings,
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
                persons,
                contents,
                strings: StringsSpy {
                    graph: Arc::clone(&inner),
                    history: Arc::clone(&history),
                },
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
            history,
        }
    }
}

pub struct StringsSpy<G: SwhGraphWithProperties<Strings: OptStrings>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
}

impl<G: SwhGraphWithProperties<Strings: OptStrings>> PropertiesBackend for StringsSpy<G> {
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Strings as PropertiesBackend>::DataFilesAvailability;
}

impl<G: SwhGraphWithProperties<Strings: OptStrings>> OptStrings for StringsSpy<G> {
    fn message(&self) -> PropertiesResult<'_, &[u8], Self> {
        // no need to log, every access necessarily calls message_offset too
        self.graph.graph.properties().strings.message()
    }

    fn message_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::Message(node)));
        self.graph.graph.properties().strings.message_offset(node)
    }

    fn tag_name(&self) -> PropertiesResult<'_, &[u8], Self> {
        // no need to log, every access necessarily call tag_name_offset too
        self.graph.graph.properties().strings.tag_name()
    }

    fn tag_name_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::TagName(node)));
        self.graph.graph.properties().strings.tag_name_offset(node)
    }
}
