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

pub trait PersonsOption {}

pub struct Persons {
    author_id: NumberMmap<BigEndian, u32, Mmap>,
    committer_id: NumberMmap<BigEndian, u32, Mmap>,
}
impl PersonsOption for Persons {}
impl PersonsOption for () {}

/// Workaround for [equality in `where` clauses](https://github.com/rust-lang/rust/issues/20041)
pub trait PersonsTrait {
    fn author_id(&self) -> &NumberMmap<BigEndian, u32, Mmap>;
    fn committer_id(&self) -> &NumberMmap<BigEndian, u32, Mmap>;
}

impl PersonsTrait for Persons {
    #[inline(always)]
    fn author_id(&self) -> &NumberMmap<BigEndian, u32, Mmap> {
        &self.author_id
    }
    #[inline(always)]
    fn committer_id(&self) -> &NumberMmap<BigEndian, u32, Mmap> {
        &self.committer_id
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, (), CONTENTS, STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::author_id`]
    /// * [`SwhGraphProperties::committer_id`]
    pub fn load_persons(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, Persons, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: Persons {
                author_id: NumberMmap::new(suffix_path(&self.path, AUTHOR_ID), self.num_nodes)
                    .context("Could not load author_id")?,
                committer_id: NumberMmap::new(
                    suffix_path(&self.path, COMMITTER_ID),
                    self.num_nodes,
                )
                .context("Could not load committer_id")?,
            },
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
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption + PersonsTrait,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the id of the author of a revision or release, if any
    #[inline]
    pub fn author_id(&self, node_id: NodeId) -> Option<u32> {
        match self.persons.author_id().get(node_id) {
            Some(u32::MAX) => None,
            id => id,
        }
    }

    /// Returns the id of the committer of a revision, if any
    #[inline]
    pub fn committer_id(&self, node_id: NodeId) -> Option<u32> {
        match self.persons.committer_id().get(node_id) {
            Some(u32::MAX) => None,
            id => id,
        }
    }
}
