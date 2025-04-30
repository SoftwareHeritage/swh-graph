// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{ensure, Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;

/// Trait implemented by both [`NoContents`] and all implementors of [`Contents`],
/// to allow loading content properties only if needed.
pub trait MaybeContents {}
impl<C: OptContents> MaybeContents for C {}

/// Placeholder for when "contents" properties are not loaded.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NoContents;
impl MaybeContents for NoContents {}

#[diagnostic::on_unimplemented(
    label = "does not have Content properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_contents()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait implemented by all implementors of [`MaybeContents`] but [`NoContents`]
pub trait OptContents: MaybeContents + PropertiesBackend {
    type Data<'a>: GetIndex<Output = u64> + 'a
    where
        Self: 'a;

    fn is_skipped_content(&self) -> PropertiesResult<'_, Self::Data<'_>, Self>;
    fn content_length(&self) -> PropertiesResult<'_, Self::Data<'_>, Self>;
}

#[diagnostic::on_unimplemented(
    label = "does not have Content properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_contents()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait for backend storage of content properties (either in-memory or memory-mapped)
pub trait Contents: OptContents<DataFilesAvailability = GuaranteedDataFiles> {}
impl<S: OptContents<DataFilesAvailability = GuaranteedDataFiles>> Contents for S {}

/// Variant of [`MappedStrings`] that checks at runtime that files are present every time
/// it is accessed
pub struct OptMappedContents {
    is_skipped_content: Result<NumberMmap<BigEndian, u64, Mmap>, UnavailableProperty>,
    content_length: Result<NumberMmap<BigEndian, u64, Mmap>, UnavailableProperty>,
}
impl PropertiesBackend for OptMappedContents {
    type DataFilesAvailability = OptionalDataFiles;
}
impl OptContents for OptMappedContents {
    type Data<'a>
        = &'a NumberMmap<BigEndian, u64, Mmap>
    where
        Self: 'a;

    #[inline(always)]
    fn is_skipped_content(&self) -> PropertiesResult<'_, Self::Data<'_>, Self> {
        self.is_skipped_content.as_ref()
    }
    #[inline(always)]
    fn content_length(&self) -> PropertiesResult<'_, Self::Data<'_>, Self> {
        self.content_length.as_ref()
    }
}

pub struct MappedContents {
    is_skipped_content: NumberMmap<BigEndian, u64, Mmap>,
    content_length: NumberMmap<BigEndian, u64, Mmap>,
}
impl PropertiesBackend for MappedContents {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl OptContents for MappedContents {
    type Data<'a>
        = &'a NumberMmap<BigEndian, u64, Mmap>
    where
        Self: 'a;

    #[inline(always)]
    fn is_skipped_content(&self) -> Self::Data<'_> {
        &self.is_skipped_content
    }
    #[inline(always)]
    fn content_length(&self) -> Self::Data<'_> {
        &self.content_length
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VecContents {
    is_skipped_content: Vec<u64>,
    content_length: Vec<u64>,
}

impl VecContents {
    pub fn new(data: Vec<(bool, Option<u64>)>) -> Result<Self> {
        let bit_vec_len = data.len().div_ceil(64);
        let mut is_skipped_content = vec![0; bit_vec_len];
        let mut content_length = Vec::with_capacity(data.len());
        for (node_id, (is_skipped, length)) in data.into_iter().enumerate() {
            ensure!(
                length != Some(u64::MAX),
                "content length may not be {}",
                u64::MAX
            );
            content_length.push(length.unwrap_or(u64::MAX));
            if is_skipped {
                let cell_id = node_id / (u64::BITS as usize);
                let mask = 1 << (node_id % (u64::BITS as usize));
                is_skipped_content[cell_id] |= mask;
            }
        }
        Ok(VecContents {
            is_skipped_content,
            content_length,
        })
    }
}

impl PropertiesBackend for VecContents {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl OptContents for VecContents {
    type Data<'a>
        = &'a [u64]
    where
        Self: 'a;

    #[inline(always)]
    fn is_skipped_content(&self) -> Self::Data<'_> {
        self.is_skipped_content.as_slice()
    }
    #[inline(always)]
    fn content_length(&self) -> Self::Data<'_> {
        self.content_length.as_slice()
    }
}

impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, NoContents, STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::is_skipped_content`]
    /// * [`SwhGraphProperties::content_length`]
    pub fn load_contents(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, MappedContents, STRINGS, LABELNAMES>>
    {
        let OptMappedContents {
            is_skipped_content,
            content_length,
        } = self.get_contents()?;
        let contents = MappedContents {
            is_skipped_content: is_skipped_content?,
            content_length: content_length?,
        };
        self.with_contents(contents)
    }

    /// Equivalent to [`Self::load_contents`] that does not require all files to be present
    pub fn opt_load_contents(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, OptMappedContents, STRINGS, LABELNAMES>>
    {
        let contents = self.get_contents()?;
        self.with_contents(contents)
    }

    fn get_contents(&self) -> Result<OptMappedContents> {
        Ok(OptMappedContents {
            is_skipped_content: load_if_exists(&self.path, CONTENT_IS_SKIPPED, |path| {
                let num_bytes = self.num_nodes.div_ceil(u64::BITS.try_into().unwrap());
                NumberMmap::new(path, num_bytes).context("Could not load is_skipped_content")
            })?,
            content_length: load_if_exists(&self.path, CONTENT_LENGTH, |path| {
                NumberMmap::new(path, self.num_nodes).context("Could not load content_length")
            })?,
        })
    }

    /// Alternative to [`load_contents`](Self::load_contents) that allows using arbitrary
    /// contents implementations
    pub fn with_contents<CONTENTS: MaybeContents>(
        self,
        contents: CONTENTS,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents,
            strings: self.strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
            label_names_are_in_base64_order: self.label_names_are_in_base64_order,
        })
    }
}

/// Functions to access properties of `content` nodes
///
/// Only available after calling [`load_contents`](SwhGraphProperties::load_contents)
/// or [`load_all_properties`](crate::graph::SwhBidirectionalGraph::load_all_properties)
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: OptContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns whether the node is a skipped content
    ///
    /// Non-content objects get a `false` value, like non-skipped contents.
    ///
    /// # Panics
    ///
    /// If the node id does not exist.
    #[inline]
    pub fn is_skipped_content(&self, node_id: NodeId) -> PropertiesResult<bool, CONTENTS> {
        CONTENTS::map_if_available(self.try_is_skipped_content(node_id), |is_skipped_content| {
            is_skipped_content
                .unwrap_or_else(|e| panic!("Cannot get is_skipped_content bit of node: {}", e))
        })
    }

    /// Returns whether the node is a skipped content, or `Err` if the node id does not exist
    ///
    /// Non-content objects get a `false` value, like non-skipped contents.
    #[inline]
    pub fn try_is_skipped_content(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<Result<bool, OutOfBoundError>, CONTENTS> {
        CONTENTS::map_if_available(self.contents.is_skipped_content(), |is_skipped_contents| {
            if node_id >= self.num_nodes {
                return Err(OutOfBoundError {
                    index: node_id,
                    len: self.num_nodes,
                });
            }
            let cell_id = node_id / (u64::BITS as usize);
            let mask = 1 << (node_id % (u64::BITS as usize));

            // Safe because we checked node_id is lower than the length, and the length of
            // self.contents.is_skipped_content() is checked when creating the mmap
            let cell = unsafe { is_skipped_contents.get_unchecked(cell_id) };

            Ok((cell & mask) != 0)
        })
    }

    /// Returns the length of the given content.
    ///
    /// May be `None` for skipped contents
    ///
    /// # Panics
    ///
    /// If the node id does not exist.
    #[inline]
    pub fn content_length(&self, node_id: NodeId) -> PropertiesResult<Option<u64>, CONTENTS> {
        CONTENTS::map_if_available(self.try_content_length(node_id), |content_length| {
            content_length.unwrap_or_else(|e| panic!("Cannot get content length: {}", e))
        })
    }

    /// Returns the length of the given content, or `Err` if the node id does not exist
    ///
    /// May be `Ok(None)` for skipped contents
    #[inline]
    pub fn try_content_length(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<Result<Option<u64>, OutOfBoundError>, CONTENTS> {
        CONTENTS::map_if_available(self.contents.content_length(), |content_lengths| {
            match content_lengths.get(node_id) {
                None => Err(OutOfBoundError {
                    // id does not exist
                    index: node_id,
                    len: content_lengths.len(),
                }),
                Some(u64::MAX) => Ok(None), // Skipped content with no length
                Some(length) => Ok(Some(length)),
            }
        })
    }
}
