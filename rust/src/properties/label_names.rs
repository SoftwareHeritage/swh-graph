// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use mmap_rs::Mmap;
use thiserror::Error;

use super::suffixes::*;
use super::*;
use crate::front_coded_list::FrontCodedList;
use crate::labels::FilenameId;
use crate::utils::suffix_path;

/// Trait implemented by both [`NoLabelNames`] and all implementors of [`LabelNames`],
/// to allow loading label names only if needed.
pub trait MaybeLabelNames {}

pub struct MappedLabelNames {
    label_names: FrontCodedList<Mmap, Mmap>,
}
impl<L: LabelNames> MaybeLabelNames for L {}

/// Placeholder for when label names are not loaded.
pub struct NoLabelNames;
impl MaybeLabelNames for NoLabelNames {}

/// Trait for backend storage of label names (either in-memory or memory-mapped)
#[diagnostic::on_unimplemented(
    label = "does not have label names loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_label_names()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<GOVMPH>().unwrap()` to load all properties"
)]
pub trait LabelNames {
    type LabelNames<'a>: GetIndex<Output = Vec<u8>>
    where
        Self: 'a;

    fn label_names(&self) -> Self::LabelNames<'_>;
}

impl LabelNames for MappedLabelNames {
    type LabelNames<'a> = &'a FrontCodedList<Mmap, Mmap> where Self: 'a;

    #[inline(always)]
    fn label_names(&self) -> Self::LabelNames<'_> {
        &self.label_names
    }
}

pub struct VecLabelNames {
    label_names: Vec<Vec<u8>>,
}
impl VecLabelNames {
    /// Returns a new `VecLabelNames` from a list of label names
    pub fn new<S: AsRef<[u8]>>(label_names: Vec<S>) -> Result<Self> {
        let base64 = base64_simd::STANDARD;
        Ok(VecLabelNames {
            label_names: label_names
                .into_iter()
                .map(|s| base64.encode_to_string(s).into_bytes())
                .collect(),
        })
    }
}

impl LabelNames for VecLabelNames {
    type LabelNames<'a> = &'a [Vec<u8>] where Self: 'a;

    #[inline(always)]
    fn label_names(&self) -> Self::LabelNames<'_> {
        self.label_names.as_slice()
    }
}

impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, NoLabelNames>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with this methods
    /// available:
    ///
    /// * [`SwhGraphProperties::label_name`]
    /// * [`SwhGraphProperties::label_name_base64`]
    pub fn load_label_names(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, MappedLabelNames>>
    {
        let label_names = MappedLabelNames {
            label_names: FrontCodedList::load(suffix_path(&self.path, LABEL_NAME))
                .context("Could not load label names")?,
        };
        self.with_label_names(label_names)
    }

    /// Alternative to [`load_label_names`](Self::load_label_names) that allows using
    /// arbitrary label_names implementations
    pub fn with_label_names<LABELNAMES: LabelNames>(
        self,
        label_names: LABELNAMES,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: self.contents,
            strings: self.strings,
            label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

/// Functions to access names of arc labels.
///
/// Only available after calling [`load_label_names`](SwhGraphProperties::load_label_names)
/// or [`load_all_properties`](crate::graph::SwhBidirectionalGraph::load_all_properties).
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: LabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the base64-encoded name of an arc label
    ///
    /// This is the file name (resp. branch name) of a label on an arc from a directory
    /// (resp. snapshot)
    ///
    /// # Panics
    ///
    /// If `filename_id` does not exist
    #[inline]
    pub fn label_name_base64(&self, filename_id: FilenameId) -> Vec<u8> {
        self.try_label_name_base64(filename_id)
            .unwrap_or_else(|e| panic!("Cannot get label name: {}", e))
    }

    /// Returns the base64-encoded name of an arc label
    ///
    /// This is the file name (resp. branch name) of a label on an arc from a directory
    /// (resp. snapshot)
    #[inline]
    pub fn try_label_name_base64(
        &self,
        filename_id: FilenameId,
    ) -> Result<Vec<u8>, OutOfBoundError> {
        let index = filename_id
            .0
            .try_into()
            .expect("filename_id overflowed usize");
        self.label_names
            .label_names()
            .get(index)
            .ok_or(OutOfBoundError {
                index,
                len: self.label_names.label_names().len(),
            })
    }

    /// Returns the name of an arc label
    ///
    /// This is the file name (resp. branch name) of a label on an arc from a directory
    /// (resp. snapshot)
    ///
    /// # Panics
    ///
    /// If `filename_id` does not exist
    #[inline]
    pub fn label_name(&self, filename_id: FilenameId) -> Vec<u8> {
        self.try_label_name(filename_id)
            .unwrap_or_else(|e| panic!("Cannot get label name: {}", e))
    }

    /// Returns the name of an arc label
    ///
    /// This is the file name (resp. branch name) of a label on an arc from a directory
    /// (resp. snapshot)
    #[inline]
    pub fn try_label_name(&self, filename_id: FilenameId) -> Result<Vec<u8>, OutOfBoundError> {
        let base64 = base64_simd::STANDARD;
        self.try_label_name_base64(filename_id).map(|name| {
            base64.decode_to_vec(name).unwrap_or_else(|name| {
                panic!(
                    "Could not decode filename of id {}: {:?}",
                    filename_id.0, name
                )
            })
        })
    }

    /// Given a branch/file name, returns the filename id used by edges with that name,
    /// or `None` if it does not exist.
    ///
    /// This is the inverse function of [`label_name`](Self::label_name)
    ///
    /// Unlike in Java where this function is `O(1)`, this implementation is `O(log2(num_labels))`
    /// because it uses a binary search, as the MPH function can only be read from Java.
    #[inline]
    pub fn label_name_id(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<FilenameId, LabelIdFromNameError> {
        use std::cmp::Ordering::*;
        let base64 = base64_simd::STANDARD;
        let name = base64.encode_to_string(name.as_ref()).into_bytes();

        // both inclusive
        let mut min = 0;
        let mut max = self
            .label_names
            .label_names()
            .len()
            .saturating_sub(1)
            .try_into()
            .expect("number of labels overflowed u64");

        while min <= max {
            let pivot = (min + max) / 2;
            let pivot_id = FilenameId(pivot);
            let pivot_name = self.label_name_base64(pivot_id);
            if min == max {
                if pivot_name.as_slice() == name {
                    return Ok(pivot_id);
                } else {
                    break;
                }
            } else {
                match pivot_name.as_slice().cmp(&name) {
                    Less => min = pivot.saturating_add(1),
                    Equal => return Ok(pivot_id),
                    Greater => max = pivot.saturating_sub(1),
                }
            }
        }

        Err(LabelIdFromNameError(name.to_vec()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
#[error("Unknown label name: {}", String::from_utf8_lossy(.0))]
pub struct LabelIdFromNameError(pub Vec<u8>);
