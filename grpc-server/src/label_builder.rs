// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use super::proto;
use swh_graph::graph::*;
use swh_graph::labels::{EdgeLabel, VisitStatus};
use swh_graph::properties::LabelNames;

/// Bit masks selecting which fields should be included by [`LabelBuilder`], based on
/// [`proto::FieldMask`].
///
/// As nodes are recursive structures, we include a sub-structure (eg. "successor")
/// iff any of that sub-structure's fields are selected (eg. "label.name" and
/// "label.permission").
#[rustfmt::skip]
mod label_builder_bitmasks {
    //                                                                xxxx
    pub const LABEL: u32 =            0b00000000_00000000_00000000_00011110;
    pub const LABEL_NAME: u32 =       0b00000000_00000000_00000000_00000010;
    pub const LABEL_PERMISSION: u32 = 0b00000000_00000000_00000000_00000100;
    pub const LABEL_VISIT_TS: u32 =   0b00000000_00000000_00000000_00001000;
    pub const LABEL_FULL_VISIT: u32 = 0b00000000_00000000_00000000_00010000;
}

use label_builder_bitmasks::*;

#[derive(Clone)]
pub struct LabelBuilder<G: Clone + Send + Sync + 'static> {
    graph: G,
    // Which fields to include, based on the [`FieldMask`](proto::FieldMask)
    bitmask: u32,
}

impl<G: SwhGraphWithProperties<LabelNames: LabelNames> + Clone + Send + Sync + 'static>
    LabelBuilder<G>
{
    #[allow(clippy::result_large_err)] // this is called by implementations of Tonic traits, which can't return Result<_, Box<Status>>
    pub fn new(
        graph: G,
        mask: Option<prost_types::FieldMask>,
        prefix: &str,
    ) -> Result<Self, tonic::Status> {
        let mask = match mask {
            Some(mask) => {
                if mask.paths.contains(&String::from(prefix)) {
                    // Disable filtering in prefix.*
                    None
                } else {
                    Some(prost_types::FieldMask {
                        paths: mask
                            .paths
                            .iter()
                            .flat_map(|field| field.strip_prefix(prefix))
                            .map(|field| field.to_owned())
                            .collect(),
                    })
                }
            }
            None => None,
        };

        let Some(mask) = mask else {
            return Ok(LabelBuilder {
                graph,
                bitmask: u32::MAX,
            }); // All bits set
        };
        let mut label_builder = LabelBuilder {
            graph,
            bitmask: 0u32, // No bits set
        };
        for field in mask.paths {
            label_builder.bitmask |= match field.as_str() {
                "" => LABEL,
                ".name" => LABEL_NAME,
                ".permission" => LABEL_PERMISSION,
                ".visit_timestamp" => LABEL_VISIT_TS,
                ".is_full_visit" => LABEL_FULL_VISIT,
                field => {
                    log::warn!("Unknown field {:?}", format!("{prefix}{field}"));
                    0 // Ignore unknown fields
                }
            }
        }

        Ok(label_builder)
    }

    pub fn empty_mask(&self) -> bool {
        self.bitmask == 0
    }

    pub fn build_edge_label(&self, label: EdgeLabel) -> Option<proto::EdgeLabel> {
        if self.bitmask & LABEL == 0 {
            return None;
        }
        Some(match label {
            EdgeLabel::Branch(label) => proto::EdgeLabel {
                name: self.if_mask(LABEL_NAME, || {
                    Some(self.graph.properties().label_name(label.label_name_id()))
                }),
                permission: None,
                visit_timestamp: None,
                is_full_visit: None,
            },
            EdgeLabel::DirEntry(label) => proto::EdgeLabel {
                name: self.if_mask(LABEL_NAME, || {
                    Some(self.graph.properties().label_name(label.label_name_id()))
                }),
                permission: self.if_mask(LABEL_PERMISSION, || {
                    Some(
                        label
                            .permission()
                            .unwrap_or(swh_graph::labels::Permission::None)
                            .to_git()
                            .into(),
                    )
                }),
                visit_timestamp: None,
                is_full_visit: None,
            },
            EdgeLabel::Visit(label) => proto::EdgeLabel {
                name: None,
                permission: None,
                visit_timestamp: self.if_mask(LABEL_VISIT_TS, || Some(label.timestamp())),
                is_full_visit: self.if_mask(LABEL_FULL_VISIT, || {
                    Some(match label.status() {
                        VisitStatus::Full => true,
                        VisitStatus::Partial => false,
                    })
                }),
            },
        })
    }

    fn if_mask<T: Default>(&self, mask: u32, f: impl FnOnce() -> T) -> T {
        if self.bitmask & mask == 0 {
            T::default()
        } else {
            f()
        }
    }
}
