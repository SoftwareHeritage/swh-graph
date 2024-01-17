// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::java_compat::fcl::FrontCodedList;
use crate::labels::FilenameId;
use crate::utils::suffix_path;

pub trait LabelNamesOption {}

pub struct LabelNames {
    label_names: FrontCodedList<Mmap, Mmap>,
}
impl LabelNamesOption for LabelNames {}
impl LabelNamesOption for () {}

/// Workaround for [equality in `where` clauses](https://github.com/rust-lang/rust/issues/20041)
pub trait LabelNamesTrait {
    fn label_names(&self) -> &FrontCodedList<Mmap, Mmap>;
}

impl LabelNamesTrait for LabelNames {
    #[inline(always)]
    fn label_names(&self) -> &FrontCodedList<Mmap, Mmap> {
        &self.label_names
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, ()>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with this methods
    /// available:
    ///
    /// * [`SwhGraphProperties::label_name`]
    /// * [`SwhGraphProperties::label_name_base64`]
    pub fn load_label_names(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LabelNames>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: self.contents,
            strings: self.strings,
            label_names: LabelNames {
                label_names: FrontCodedList::load(suffix_path(&self.path, LABEL_NAME))
                    .context("Could not load label names")?,
            },
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
        LABELNAMES: LabelNamesOption + LabelNamesTrait,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the file name (resp. branch name) of a label on an arc from a directory
    /// (resp. snapshot), base64-encoded.
    #[inline]
    pub fn label_name_base64(&self, filename_id: FilenameId) -> Option<Vec<u8>> {
        self.label_names.label_names().get(
            filename_id
                .0
                .try_into()
                .expect("filename_id^overflowed usize"),
        )
    }

    /// Returns the file name (resp. branch name) of a label on an arc from a directory
    /// (resp. snapshot).
    #[inline]
    pub fn label_name(&self, filename_id: FilenameId) -> Option<Vec<u8>> {
        let base64 = base64_simd::STANDARD;
        self.label_name_base64(filename_id).map(|name| {
            base64.decode_to_vec(name).unwrap_or_else(|name| {
                panic!(
                    "Could not decode filename of id {}: {:?}",
                    filename_id.0, name
                )
            })
        })
    }
}
