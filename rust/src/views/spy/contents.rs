// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Contents`] spy for [`GraphSpy`]

use super::*;
use properties::{OptContents, PropertiesBackend, PropertiesResult};

impl<
        G: SwhGraphWithProperties<Contents: OptContents>,
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, properties::NoContents, STRINGS, LABELNAMES>
{
    pub fn with_contents(
        self,
    ) -> GraphSpy<G, MAPS, TIMESTAMPS, PERSONS, ContentsSpy<G>, STRINGS, LABELNAMES> {
        let GraphSpy {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps,
                    persons,
                    contents: properties::NoContents,
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
                persons,
                contents: ContentsSpy {
                    graph: Arc::clone(&inner),
                    history: Arc::clone(&history),
                },
                strings,
                label_names,
                label_names_are_in_base64_order,
            },
            inner,
            history,
        }
    }
}

pub struct ContentsSpy<G: SwhGraphWithProperties<Contents: OptContents>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
}

impl<G: SwhGraphWithProperties<Contents: OptContents>> PropertiesBackend for ContentsSpy<G> {
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Contents as PropertiesBackend>::DataFilesAvailability;
}

impl<G: SwhGraphWithProperties<Contents: OptContents>> OptContents for ContentsSpy<G> {
    fn is_skipped_content(&self, node: NodeId) -> PropertiesResult<'_, Option<bool>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(
                PropertyAccess::IsSkippedContent(node),
            ));
        self.graph
            .graph
            .properties()
            .contents
            .is_skipped_content(node)
    }

    fn content_length(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::ContentLength(
                node,
            )));
        self.graph.graph.properties().contents.content_length(node)
    }
}
