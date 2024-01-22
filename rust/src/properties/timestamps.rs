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

pub trait TimestampsOption {}

pub struct Timestamps {
    author_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    author_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
    committer_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    committer_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
}
impl TimestampsOption for Timestamps {}
impl TimestampsOption for () {}

/// Workaround for [equality in `where` clauses](https://github.com/rust-lang/rust/issues/20041)
pub trait TimestampsTrait {
    fn author_timestamp(&self) -> &NumberMmap<BigEndian, i64, Mmap>;
    fn author_timestamp_offset(&self) -> &NumberMmap<BigEndian, i16, Mmap>;
    fn committer_timestamp(&self) -> &NumberMmap<BigEndian, i64, Mmap>;
    fn committer_timestamp_offset(&self) -> &NumberMmap<BigEndian, i16, Mmap>;
}

impl TimestampsTrait for Timestamps {
    #[inline(always)]
    fn author_timestamp(&self) -> &NumberMmap<BigEndian, i64, Mmap> {
        &self.author_timestamp
    }
    #[inline(always)]
    fn author_timestamp_offset(&self) -> &NumberMmap<BigEndian, i16, Mmap> {
        &self.author_timestamp_offset
    }
    #[inline(always)]
    fn committer_timestamp(&self) -> &NumberMmap<BigEndian, i64, Mmap> {
        &self.committer_timestamp
    }
    #[inline(always)]
    fn committer_timestamp_offset(&self) -> &NumberMmap<BigEndian, i16, Mmap> {
        &self.committer_timestamp_offset
    }
}

impl<
        MAPS: MapsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption,
    > SwhGraphProperties<MAPS, (), PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::author_timestamp`]
    /// * [`SwhGraphProperties::author_timestamp_offset`]
    /// * [`SwhGraphProperties::committer_timestamp`]
    /// * [`SwhGraphProperties::committer_timestamp_offset`]
    pub fn load_timestamps(
        self,
    ) -> Result<SwhGraphProperties<MAPS, Timestamps, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: Timestamps {
                author_timestamp: NumberMmap::new(
                    suffix_path(&self.path, AUTHOR_TIMESTAMP),
                    self.num_nodes,
                )
                .context("Could not load author_timestamp")?,
                author_timestamp_offset: NumberMmap::new(
                    suffix_path(&self.path, AUTHOR_TIMESTAMP_OFFSET),
                    self.num_nodes,
                )
                .context("Could not load author_timestamp_offset")?,
                committer_timestamp: NumberMmap::new(
                    suffix_path(&self.path, COMMITTER_TIMESTAMP),
                    self.num_nodes,
                )
                .context("Could not load committer_timestamp")?,
                committer_timestamp_offset: NumberMmap::new(
                    suffix_path(&self.path, COMMITTER_TIMESTAMP_OFFSET),
                    self.num_nodes,
                )
                .context("Could not load committer_timestamp_offset")?,
            },
            persons: self.persons,
            contents: self.contents,
            strings: self.strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption + TimestampsTrait,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the number of seconds since Epoch that a release or revision was
    /// authored at
    #[inline]
    pub fn author_timestamp(&self, node_id: NodeId) -> Option<i64> {
        match self.timestamps.author_timestamp().get(node_id) {
            Some(i64::MIN) => None,
            ts => ts,
        }
    }

    /// Returns the UTC offset in minutes of a release or revision's authorship date
    #[inline]
    pub fn author_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        match self.timestamps.author_timestamp_offset().get(node_id) {
            Some(i16::MIN) => None,
            offset => offset,
        }
    }

    /// Returns the number of seconds since Epoch that a revision was committed at
    #[inline]
    pub fn committer_timestamp(&self, node_id: NodeId) -> Option<i64> {
        match self.timestamps.committer_timestamp().get(node_id) {
            Some(i64::MIN) => None,
            ts => ts,
        }
    }

    /// Returns the UTC offset in minutes of a revision's committer date
    #[inline]
    pub fn committer_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        match self.timestamps.committer_timestamp_offset().get(node_id) {
            Some(i16::MIN) => None,
            offset => offset,
        }
    }
}
