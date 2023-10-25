// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;
use crate::utils::suffix_path;

pub trait ContentsOption {}

pub struct Contents {
    is_skipped_content: LongArrayBitVector<NumberMmap<LittleEndian, u64, Mmap>>,
    content_length: NumberMmap<BigEndian, u64, Mmap>,
}
impl ContentsOption for Contents {}
impl ContentsOption for () {}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, (), STRINGS>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::is_skipped_content`]
    /// * [`SwhGraphProperties::content_length`]
    pub fn load_contents(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, Contents, STRINGS>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: Contents {
                is_skipped_content: LongArrayBitVector::new_from_path(
                    suffix_path(&self.path, CONTENT_IS_SKIPPED),
                    self.num_nodes,
                )
                .context("Could not load is_skipped_content")?,
                content_length: NumberMmap::new(
                    suffix_path(&self.path, CONTENT_LENGTH),
                    self.num_nodes,
                )
                .context("Could not load content_length")?,
            },
            strings: self.strings,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, Contents, STRINGS>
{
    /// Returns whether the node is a skipped content
    ///
    /// Non-content objects get a `false` value, like non-skipped contents.
    pub fn is_skipped_content(&self, node_id: NodeId) -> Option<bool> {
        self.contents.is_skipped_content.get(node_id)
    }

    /// Returns the length of the given content None.
    ///
    /// May be `None` for skipped contents
    pub fn content_length(&self, node_id: NodeId) -> Option<u64> {
        match self.contents.content_length.get(node_id) {
            Some(u64::MAX) => None,
            length => length,
        }
    }
}
