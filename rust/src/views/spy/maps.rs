// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Implementation of [`properties::Maps`] spy for [`GraphSpy`]

use super::*;
use crate::mph::SwhidMphf;
use crate::properties::Maps;
use crate::SWHID;

impl<
        G: SwhGraphWithProperties<Maps: properties::Maps>,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > GraphSpy<G, properties::NoMaps, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    pub fn with_maps(
        self,
    ) -> GraphSpy<G, MapsSpy<G>, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES> {
        let GraphSpy {
            properties:
                properties::SwhGraphProperties {
                    path,
                    num_nodes,
                    maps: properties::NoMaps,
                    timestamps,
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
                maps: MapsSpy {
                    graph: Arc::clone(&inner),
                    history: Arc::clone(&history),
                    mphf: MphfSpy {
                        graph: Arc::clone(&inner),
                        history: Arc::clone(&history),
                    },
                },
                timestamps,
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

pub struct MapsSpy<G: SwhGraphWithProperties<Maps: properties::Maps>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
    mphf: MphfSpy<G>,
}

impl<G: SwhGraphWithProperties<Maps: properties::Maps>> properties::Maps for MapsSpy<G> {
    type MPHF = MphfSpy<G>;

    fn mphf(&self) -> &Self::MPHF {
        &self.mphf
    }

    fn node2swhid(&self, node: NodeId) -> Result<crate::SWHID, crate::OutOfBoundError> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::Swhid(node)));
        self.graph.graph.properties().maps.node2swhid(node)
    }

    fn node2type(&self, node: NodeId) -> Result<NodeType, crate::OutOfBoundError> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::NodeType(node)));
        self.graph.graph.properties().maps.node2type(node)
    }
}

pub struct MphfSpy<G: SwhGraphWithProperties<Maps: properties::Maps>> {
    graph: Arc<GraphSpyInner<G>>,
    history: Arc<Mutex<Vec<GraphAccessRecord>>>,
}

impl<G: SwhGraphWithProperties<Maps: properties::Maps>> SwhidMphf for MphfSpy<G> {
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        match SWHID::try_from(*swhid) {
            Ok(swhid) => self.hash_swhid(&swhid),
            Err(_) => {
                self.history
                    .lock()
                    .unwrap()
                    .push(GraphAccessRecord::Property(
                        PropertyAccess::NodeIdForInvalidBytes(*swhid),
                    ));
                None
            }
        }
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        let swhid = swhid.as_ref();
        match SWHID::try_from(swhid) {
            Ok(swhid) => self.hash_swhid(&swhid),
            Err(_) => {
                self.history
                    .lock()
                    .unwrap()
                    .push(GraphAccessRecord::Property(
                        PropertyAccess::NodeIdForInvalidString(swhid.to_owned()),
                    ));
                None
            }
        }
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        let Ok(swhid_str) = String::from_utf8(swhid.to_vec()) else {
            self.history
                .lock()
                .unwrap()
                .push(GraphAccessRecord::Property(
                    PropertyAccess::NodeIdForInvalidStrArray(*swhid),
                ));
            return None;
        };
        match SWHID::try_from(swhid_str.as_ref()) {
            Ok(swhid) => self.hash_swhid(&swhid),
            Err(_) => {
                self.history
                    .lock()
                    .unwrap()
                    .push(GraphAccessRecord::Property(
                        PropertyAccess::NodeIdForInvalidString(swhid_str),
                    ));
                None
            }
        }
    }

    /// Hashes a [`SWHID`]
    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        self.history
            .lock()
            .unwrap()
            .push(GraphAccessRecord::Property(PropertyAccess::NodeId(*swhid)));
        self.graph.graph.properties().maps.mphf().hash_swhid(swhid)
    }
}
