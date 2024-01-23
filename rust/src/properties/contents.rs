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
    is_skipped_content: NumberMmap<BigEndian, u64, Mmap>,
    content_length: NumberMmap<BigEndian, u64, Mmap>,
}
impl ContentsOption for Contents {}
impl ContentsOption for () {}

/// Workaround for [equality in `where` clauses](https://github.com/rust-lang/rust/issues/20041)
pub trait ContentsTrait {
    fn is_skipped_content(&self) -> &NumberMmap<BigEndian, u64, Mmap>;
    fn content_length(&self) -> &NumberMmap<BigEndian, u64, Mmap>;
}

impl ContentsTrait for Contents {
    #[inline(always)]
    fn is_skipped_content(&self) -> &NumberMmap<BigEndian, u64, Mmap> {
        &self.is_skipped_content
    }
    #[inline(always)]
    fn content_length(&self) -> &NumberMmap<BigEndian, u64, Mmap> {
        &self.content_length
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, (), STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::is_skipped_content`]
    /// * [`SwhGraphProperties::content_length`]
    pub fn load_contents(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, Contents, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: Contents {
                is_skipped_content: NumberMmap::new(
                    suffix_path(&self.path, CONTENT_IS_SKIPPED),
                    self.num_nodes.div_ceil(u64::BITS.try_into().unwrap()),
                )
                .context("Could not load is_skipped_content")?,
                content_length: NumberMmap::new(
                    suffix_path(&self.path, CONTENT_LENGTH),
                    self.num_nodes,
                )
                .context("Could not load content_length")?,
            },
            strings: self.strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption + ContentsTrait,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns whether the node is a skipped content
    ///
    /// Non-content objects get a `false` value, like non-skipped contents.
    #[inline]
    pub fn is_skipped_content(&self, node_id: NodeId) -> Option<bool> {
        if node_id >= self.num_nodes {
            return None;
        }
        let cell_id = node_id / (u64::BITS as usize);
        let mask = 1 << (node_id % (u64::BITS as usize));

        // Safe because we checked node_id is lower than the length, and the length of
        // self.contents.is_skipped_content() is checked when creating the mmap
        let cell = unsafe { self.contents.is_skipped_content().get_unchecked(cell_id) };

        Some((cell & mask) != 0)
    }

    /// Returns the length of the given content None.
    ///
    /// May be `None` for skipped contents
    #[inline]
    pub fn content_length(&self, node_id: NodeId) -> Option<u64> {
        match self.contents.content_length().get(node_id) {
            Some(u64::MAX) => None,
            length => length,
        }
    }
}
