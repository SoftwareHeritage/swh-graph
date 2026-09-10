// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Timestamps`] spy for [`GraphSpy`]

use super::*;
use properties::{OptTimestamps, PropertiesBackend, PropertiesResult};

impl<
        G: SwhGraphWithProperties<Timestamps: OptTimestamps>,
        MAPS: properties::MaybeMaps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > GraphSpy<G, MAPS, properties::NoTimestamps, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    pub fn with_timestamps(
        self,
    ) -> GraphSpy<G, MAPS, TimestampsSpy<G>, PERSONS, CONTENTS, STRINGS, LABELNAMES> {
        let GraphSpy {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps,
                    timestamps: properties::NoTimestamps,
                    persons,
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
                timestamps: TimestampsSpy {
                    graph: Arc::clone(&inner),
                    history: Arc::clone(&history),
                },
                persons,
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

pub struct TimestampsSpy<G: SwhGraphWithProperties<Timestamps: OptTimestamps>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
}

impl<G: SwhGraphWithProperties<Timestamps: OptTimestamps>> PropertiesBackend for TimestampsSpy<G> {
    type DataFilesAvailability =
        <<G as SwhGraphWithProperties>::Timestamps as PropertiesBackend>::DataFilesAvailability;
}

impl<G: SwhGraphWithProperties<Timestamps: OptTimestamps>> OptTimestamps for TimestampsSpy<G> {
    fn author_timestamp(&self, node: NodeId) -> PropertiesResult<'_, Option<i64>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(
                PropertyAccess::AuthorTimestamp(node),
            ));
        self.graph
            .graph
            .properties()
            .timestamps
            .author_timestamp(node)
    }

    fn author_timestamp_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<i16>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(
                PropertyAccess::AuthorTimestampOffset(node),
            ));
        self.graph
            .graph
            .properties()
            .timestamps
            .author_timestamp_offset(node)
    }

    fn committer_timestamp(&self, node: NodeId) -> PropertiesResult<'_, Option<i64>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(
                PropertyAccess::CommitterTimestamp(node),
            ));
        self.graph
            .graph
            .properties()
            .timestamps
            .committer_timestamp(node)
    }

    fn committer_timestamp_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<i16>, Self> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(
                PropertyAccess::CommitterTimestampOffset(node),
            ));
        self.graph
            .graph
            .properties()
            .timestamps
            .committer_timestamp_offset(node)
    }
}
