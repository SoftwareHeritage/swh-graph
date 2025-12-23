// Copyright (C) 2023-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use crate::label_builder::LabelBuilder;

use super::proto;
use swh_graph::graph::*;
use swh_graph::properties::{
    DataFilesAvailability, LabelNames, Maps, OptContents, OptPersons, OptStrings, OptTimestamps,
    PropertiesBackend,
};
use swh_graph::NodeType;

/// Bit masks selecting which fields should be included by [`NodeBuilder`], based on
/// [`proto::FieldMask`].
///
/// As nodes are recursive structures, we include a sub-structure (eg. "successor")
/// iff any of that sub-structure's fields are selected (eg. "successor.swhid" and
/// "successor.label").
#[rustfmt::skip]
mod node_builder_bitmasks {
    //                                                                          xxxxx
    pub const SUCCESSOR: u32 =                  0b00000000_00000000_00000000_00011111;
    pub const SUCCESSOR_SWHID: u32 =            0b00000000_00000000_00000000_00000001;

    //                                                                       x
    pub const NUM_SUCCESSORS: u32 =             0b00000000_00000000_00000000_10000000;

    //                                                               xxxxxxx
    pub const REV: u32 =                        0b00000000_00000000_01111111_00000000;
    pub const REV_AUTHOR: u32 =                 0b00000000_00000000_00000001_00000000;
    pub const REV_AUTHOR_DATE: u32 =            0b00000000_00000000_00000010_00000000;
    pub const REV_AUTHOR_DATE_OFFSET: u32 =     0b00000000_00000000_00000100_00000000;
    pub const REV_COMMITTER: u32 =              0b00000000_00000000_00001000_00000000;
    pub const REV_COMMITTER_DATE: u32 =         0b00000000_00000000_00010000_00000000;
    pub const REV_COMMITTER_DATE_OFFSET: u32 =  0b00000000_00000000_00100000_00000000;
    pub const REV_MESSAGE: u32 =                0b00000000_00000000_01000000_00000000;

    //                                                        xxxxx
    pub const REL: u32 =                        0b00000000_00011111_00000000_00000000;
    pub const REL_AUTHOR: u32 =                 0b00000000_00000001_00000000_00000000;
    pub const REL_AUTHOR_DATE: u32 =            0b00000000_00000010_00000000_00000000;
    pub const REL_AUTHOR_DATE_OFFSET: u32 =     0b00000000_00000100_00000000_00000000;
    pub const REL_NAME: u32 =                   0b00000000_00001000_00000000_00000000;
    pub const REL_MESSAGE: u32 =                0b00000000_00010000_00000000_00000000;

    //                                                  xx
    pub const CNT: u32 =                        0b00000011_00000000_00000000_00000000;
    pub const CNT_LENGTH: u32 =                 0b00000001_00000000_00000000_00000000;
    pub const CNT_IS_SKIPPED: u32 =             0b00000010_00000000_00000000_00000000;

    //                                               x
    pub const ORI: u32 =                        0b00010000_00000000_00000000_00000000;
    pub const ORI_URL: u32 =                    0b00010000_00000000_00000000_00000000;

    pub const DATA: u32 = REV | REL | CNT | ORI;

    //                                            x
    pub const SWHID: u32 =                      0b10000000_00000000_00000000_00000000;
}

use node_builder_bitmasks::*;

#[derive(Clone)]
pub struct NodeBuilder<G: Clone + Send + Sync + 'static> {
    graph: G,
    // Which fields to include, based on the [`FieldMask`](proto::FieldMask)
    bitmask: u32,
    label_builder: LabelBuilder<G>,
}

impl<
        G: SwhLabeledForwardGraph
            + SwhGraphWithProperties<
                Maps: Maps,
                Strings: OptStrings,
                LabelNames: LabelNames,
                Contents: OptContents,
                Persons: OptPersons,
                Timestamps: OptTimestamps,
            > + Clone
            + Send
            + Sync
            + 'static,
    > NodeBuilder<G>
{
    #[allow(clippy::result_large_err)] // this is called by implementations of Tonic traits, which can't return Result<_, Box<Status>>
    pub fn new(
        graph: G,
        mask: Option<prost_types::FieldMask>,
        prefix: Option<&str>,
    ) -> Result<Self, tonic::Status> {
        let mut label_mask = mask.clone();
        let label_prefix = match prefix {
            Some(prefix) => format!("{prefix}.successor.label"),
            None => String::from("successor.label"),
        };
        let mask = match (prefix, mask) {
            (Some(prefix), Some(mask)) => {
                if mask.paths.contains(&String::from(prefix)) {
                    // Disable filtering in prefix.*
                    label_mask = None;
                    None
                } else {
                    let strip_prefix = format!("{prefix}.");
                    Some(prost_types::FieldMask {
                        paths: mask
                            .paths
                            .iter()
                            .flat_map(|field| field.strip_prefix(&strip_prefix))
                            .map(|field| field.to_owned())
                            .collect(),
                    })
                }
            }
            (_, mask) => mask,
        };

        let label_builder = LabelBuilder::new(graph.clone(), label_mask, &label_prefix)?;

        let Some(mask) = mask else {
            return Ok(NodeBuilder {
                graph: graph.clone(),
                bitmask: u32::MAX,
                label_builder,
            }); // All bits set
        };
        let mut node_builder = NodeBuilder {
            graph,
            bitmask: 0u32, // No bits set
            label_builder,
        };
        for field in mask.paths {
            node_builder.bitmask |= match field.as_str() {
                // Tonic does not allow omitting non-optional fields, so we have to
                // include "swhid" unconditionally
                "swhid" => SWHID,
                "successor" => SUCCESSOR,
                "successor.swhid" => SUCCESSOR_SWHID,
                "num_successors" => NUM_SUCCESSORS,
                "cnt" => CNT,
                "cnt.length" => CNT_LENGTH,
                "cnt.is_skipped" => CNT_IS_SKIPPED,
                "rev" => REV,
                "rev.author" => REV_AUTHOR,
                "rev.author_date" => REV_AUTHOR_DATE,
                "rev.author_date_offset" => REV_AUTHOR_DATE_OFFSET,
                "rev.committer" => REV_COMMITTER,
                "rev.committer_date" => REV_COMMITTER_DATE,
                "rev.committer_date_offset" => REV_COMMITTER_DATE_OFFSET,
                "rev.message" => REV_MESSAGE,
                "rel" => REL,
                "rel.author" => REL_AUTHOR,
                "rel.author_date" => REL_AUTHOR_DATE,
                "rel.author_date_offset" => REL_AUTHOR_DATE_OFFSET,
                "rel.name" => REL_NAME,
                "rel.message" => REL_MESSAGE,
                "ori" => ORI,
                "ori.url" => ORI_URL,
                field => {
                    log::warn!("Unknown field {:?}", field);
                    0 // Ignore unknown fields
                }
            }
        }

        Ok(node_builder)
    }

    pub fn empty_mask(&self) -> bool {
        self.bitmask == 0 && self.label_builder.empty_mask()
    }

    pub fn build_node(&self, node_id: usize) -> proto::Node {
        let successors: Vec<_> = self.if_mask(SUCCESSOR, || {
            if self.label_builder.empty_mask() {
                self.graph
                    .successors(node_id)
                    .into_iter()
                    .map(|succ| proto::Successor {
                        swhid: self.if_mask(SUCCESSOR_SWHID, || {
                            Some(self.graph.properties().swhid(succ).to_string())
                        }),
                        label: Vec::new(), // Not requested
                    })
                    .collect()
            } else {
                self.graph
                    .labeled_successors(node_id)
                    .map(|(succ, labels)| proto::Successor {
                        swhid: self.if_mask(SUCCESSOR_SWHID, || {
                            Some(self.graph.properties().swhid(succ).to_string())
                        }),
                        label: labels
                            .into_iter()
                            // Cannot panic because we made sure the mask is not empty
                            .map(|label| self.label_builder.build_edge_label(label).unwrap())
                            .collect(),
                    })
                    .collect()
            }
        });
        proto::Node {
            // TODO: omit swhid if excluded from field mask
            swhid: self.graph.properties().swhid(node_id).to_string(),
            num_successors: self.if_mask(NUM_SUCCESSORS, || {
                Some(
                    if self.bitmask & SUCCESSOR != 0 {
                        // don't need to call .outdegree() as we already have the list of successors
                        successors.len()
                    } else {
                        self.graph.outdegree(node_id)
                    }
                    .try_into()
                    .expect("outdegree overflowed i64"),
                )
            }),
            successor: successors,
            data: self.if_mask(DATA, || match self.graph.properties().node_type(node_id) {
                NodeType::Content => self.if_mask(CNT, || Some(self.build_content_data(node_id))),
                NodeType::Directory => None,
                NodeType::Revision => self.if_mask(REV, || Some(self.build_revision_data(node_id))),
                NodeType::Release => self.if_mask(REL, || Some(self.build_release_data(node_id))),
                NodeType::Snapshot => None,
                NodeType::Origin => self.if_mask(ORI, || Some(self.build_origin_data(node_id))),
            }),
        }
    }

    fn build_content_data(&self, node_id: usize) -> proto::node::Data {
        let properties = self.graph.properties();
        proto::node::Data::Cnt(proto::ContentData {
            length: self.if_mask_opt::<G::Contents, _>(CNT_LENGTH, || {
                G::Contents::map_if_available(
                    properties.content_length(node_id),
                    |content_length: Option<u64>| {
                        Some(
                            content_length?
                                .try_into()
                                .expect("Content length overflowed i64"),
                        )
                    },
                )
            }),
            is_skipped: self.if_mask_opt::<G::Contents, _>(CNT_IS_SKIPPED, || {
                G::Contents::map_if_available(
                    properties.is_skipped_content(node_id),
                    |is_skipped_content: bool| Some(is_skipped_content),
                )
            }),
        })
    }

    fn build_revision_data(&self, node_id: usize) -> proto::node::Data {
        let properties = self.graph.properties();
        proto::node::Data::Rev(proto::RevisionData {
            author: self.if_mask_opt::<G::Persons, _>(REV_AUTHOR, || {
                G::Persons::map_if_available(properties.author_id(node_id), |author_id| {
                    Some(author_id? as i64)
                })
            }),
            author_date: self.if_mask_opt::<G::Timestamps, _>(REV_AUTHOR_DATE, || {
                properties.author_timestamp(node_id)
            }),
            author_date_offset: self.if_mask_opt::<G::Timestamps, _>(
                REV_AUTHOR_DATE_OFFSET,
                || {
                    G::Timestamps::map_if_available(
                        properties.author_timestamp_offset(node_id),
                        |author_timestamp_offset| Some(author_timestamp_offset?.into()),
                    )
                },
            ),
            committer: self.if_mask_opt::<G::Persons, _>(REV_COMMITTER, || {
                G::Persons::map_if_available(properties.committer_id(node_id), |committer_id| {
                    Some(committer_id? as i64)
                })
            }),
            committer_date: self.if_mask_opt::<G::Timestamps, _>(REV_COMMITTER_DATE, || {
                properties.committer_timestamp(node_id)
            }),
            committer_date_offset: self.if_mask_opt::<G::Timestamps, _>(
                REV_COMMITTER_DATE_OFFSET,
                || {
                    G::Timestamps::map_if_available(
                        properties.committer_timestamp_offset(node_id),
                        |committer_timestamp_offset| Some(committer_timestamp_offset?.into()),
                    )
                },
            ),
            message: self.if_mask_opt::<G::Strings, _>(REV_MESSAGE, || properties.message(node_id)),
        })
    }
    fn build_release_data(&self, node_id: usize) -> proto::node::Data {
        let properties = self.graph.properties();
        proto::node::Data::Rel(proto::ReleaseData {
            author: self.if_mask_opt::<G::Persons, _>(REL_AUTHOR, || {
                G::Persons::map_if_available(properties.author_id(node_id), |author_id| {
                    Some(author_id? as i64)
                })
            }),
            author_date: self.if_mask_opt::<G::Timestamps, _>(REL_AUTHOR_DATE, || {
                properties.author_timestamp(node_id)
            }),
            author_date_offset: self.if_mask_opt::<G::Timestamps, _>(
                REL_AUTHOR_DATE_OFFSET,
                || {
                    G::Timestamps::map_if_available(
                        properties.author_timestamp_offset(node_id),
                        |author_timestamp_offset| Some(author_timestamp_offset?.into()),
                    )
                },
            ),
            name: self.if_mask_opt::<G::Strings, _>(REL_NAME, || properties.tag_name(node_id)),
            message: self.if_mask_opt::<G::Strings, _>(REL_MESSAGE, || properties.message(node_id)),
        })
    }
    fn build_origin_data(&self, node_id: usize) -> proto::node::Data {
        let properties = self.graph.properties();
        proto::node::Data::Ori(proto::OriginData {
            url: self.if_mask_opt::<G::Strings, _>(ORI_URL, || {
                G::Strings::map_if_available(properties.message(node_id), |message: Option<_>| {
                    message.map(|message| String::from_utf8_lossy(&message).into())
                })
            }),
        })
    }

    fn if_mask<T: Default>(&self, mask: u32, f: impl FnOnce() -> T) -> T {
        if self.bitmask & mask == 0 {
            T::default()
        } else {
            f()
        }
    }

    fn if_mask_opt<'err, PB: PropertiesBackend, T: Default>(
        &self,
        mask: u32,
        f: impl FnOnce() -> <PB::DataFilesAvailability as DataFilesAvailability>::Result<'err, T>,
    ) -> T {
        if self.bitmask & mask == 0 {
            T::default()
        } else {
            PB::DataFilesAvailability::make_result(f()).unwrap_or_default()
        }
    }
}
