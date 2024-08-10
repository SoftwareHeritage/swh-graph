// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{ensure, Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;
use crate::utils::suffix_path;

/// Trait implemented by both [`NoContents`] and all implementors of [`Contents`],
/// to allow loading content properties only if needed.
pub trait MaybeContents {}

pub struct MappedContents {
    is_skipped_content: NumberMmap<BigEndian, u64, Mmap>,
    content_length: NumberMmap<BigEndian, u64, Mmap>,
}
impl<C: Contents> MaybeContents for C {}

/// Placeholder for when "contents" properties are not loaded.
pub struct NoContents;
impl MaybeContents for NoContents {}

#[diagnostic::on_unimplemented(
    label = "does not have Content properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_contents()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<GOVMPH>().unwrap()` to load all properties"
)]
/// Trait for backend storage of content properties (either in-memory or memory-mapped)
pub trait Contents {
    type Data<'a>: GetIndex<Output = u64> + 'a
    where
        Self: 'a;

    fn is_skipped_content(&self) -> Self::Data<'_>;
    fn content_length(&self) -> Self::Data<'_>;
}

impl Contents for MappedContents {
    type Data<'a> = &'a NumberMmap<BigEndian, u64, Mmap> where Self: 'a;

    #[inline(always)]
    fn is_skipped_content(&self) -> Self::Data<'_> {
        &self.is_skipped_content
    }
    #[inline(always)]
    fn content_length(&self) -> Self::Data<'_> {
        &self.content_length
    }
}

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

impl Contents for VecContents {
    type Data<'a> = &'a [u64] where Self: 'a;

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
        let contents = MappedContents {
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
        };
        self.with_contents(contents)
    }

    /// Alternative to [`load_contents`](Self::load_contents) that allows using arbitrary
    /// contents implementations
    pub fn with_contents<CONTENTS: Contents>(
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
        CONTENTS: Contents,
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
    pub fn is_skipped_content(&self, node_id: NodeId) -> bool {
        self.try_is_skipped_content(node_id)
            .unwrap_or_else(|e| panic!("Cannot get is_skipped_content bit of node: {}", e))
    }

    /// Returns whether the node is a skipped content, or `Err` if the node id does not exist
    ///
    /// Non-content objects get a `false` value, like non-skipped contents.
    #[inline]
    pub fn try_is_skipped_content(&self, node_id: NodeId) -> Result<bool, OutOfBoundError> {
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
        let cell = unsafe { self.contents.is_skipped_content().get_unchecked(cell_id) };

        Ok((cell & mask) != 0)
    }

    /// Returns the length of the given content.
    ///
    /// May be `None` for skipped contents
    ///
    /// # Panics
    ///
    /// If the node id does not exist.
    #[inline]
    pub fn content_length(&self, node_id: NodeId) -> Option<u64> {
        self.try_content_length(node_id)
            .unwrap_or_else(|e| panic!("Cannot get content length: {}", e))
    }

    /// Returns the length of the given content, or `Err` if the node id does not exist
    ///
    /// May be `Ok(None)` for skipped contents
    #[inline]
    pub fn try_content_length(&self, node_id: NodeId) -> Result<Option<u64>, OutOfBoundError> {
        match self.contents.content_length().get(node_id) {
            None => Err(OutOfBoundError {
                // id does not exist
                index: node_id,
                len: self.contents.content_length().len(),
            }),
            Some(u64::MAX) => Ok(None), // Skipped content with no length
            Some(length) => Ok(Some(length)),
        }
    }
}
