// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;
use crate::utils::suffix_path;

/// Trait implemented by both [`NoStrings`] and all implementors of [`Strings`],
/// to allow loading string properties only if needed.
pub trait MaybeStrings {}

pub struct MappedStrings {
    message: Mmap,
    message_offset: NumberMmap<BigEndian, u64, Mmap>,
    tag_name: Mmap,
    tag_name_offset: NumberMmap<BigEndian, u64, Mmap>,
}
impl<S: Strings> MaybeStrings for S {}

/// Placeholder for when string properties are not loaded
pub struct NoStrings;
impl MaybeStrings for NoStrings {}

/// Trait for backend storage of string properties (either in-memory or memory-mapped)
pub trait Strings {
    type Offsets<'a>: GetIndex<Output = u64> + 'a
    where
        Self: 'a;

    fn message(&self) -> &[u8];
    fn message_offset(&self) -> Self::Offsets<'_>;
    fn tag_name(&self) -> &[u8];
    fn tag_name_offset(&self) -> Self::Offsets<'_>;
}

impl Strings for MappedStrings {
    type Offsets<'a> = &'a NumberMmap<BigEndian, u64, Mmap> where Self: 'a;

    #[inline(always)]
    fn message(&self) -> &[u8] {
        &self.message
    }
    #[inline(always)]
    fn message_offset(&self) -> Self::Offsets<'_> {
        &self.message_offset
    }
    #[inline(always)]
    fn tag_name(&self) -> &[u8] {
        &self.tag_name
    }
    #[inline(always)]
    fn tag_name_offset(&self) -> Self::Offsets<'_> {
        &self.tag_name_offset
    }
}

pub struct VecStrings {
    message: Vec<u8>,
    message_offset: Vec<u64>,
    tag_name: Vec<u8>,
    tag_name_offset: Vec<u64>,
}

impl VecStrings {
    /// Returns [`VecStrings`] from pairs of `(message, tag_name)`
    pub fn new<Msg: AsRef<[u8]>, TagName: AsRef<[u8]>>(
        data: Vec<(Option<Msg>, Option<TagName>)>,
    ) -> Result<Self> {
        let base64 = base64_simd::STANDARD;

        let mut message = Vec::new();
        let mut message_offset = Vec::new();
        let mut tag_name = Vec::new();
        let mut tag_name_offset = Vec::new();

        for (msg, tag) in data.into_iter() {
            match msg {
                Some(msg) => {
                    let msg = base64.encode_to_string(msg);
                    message_offset.push(
                        message
                            .len()
                            .try_into()
                            .context("total message size overflowed usize")?,
                    );
                    message.extend(msg.as_bytes());
                    message.push(b'\n');
                }
                None => message_offset.push(u64::MAX),
            }
            match tag {
                Some(tag) => {
                    let tag = base64.encode_to_string(tag);
                    tag_name_offset.push(
                        tag_name
                            .len()
                            .try_into()
                            .context("total tag_name size overflowed usize")?,
                    );
                    tag_name.extend(tag.as_bytes());
                    tag_name.push(b'\n');
                }
                None => tag_name_offset.push(u64::MAX),
            }
        }

        Ok(VecStrings {
            message,
            message_offset,
            tag_name,
            tag_name_offset,
        })
    }
}

impl Strings for VecStrings {
    type Offsets<'a> = &'a [u64] where Self: 'a;

    #[inline(always)]
    fn message(&self) -> &[u8] {
        self.message.as_slice()
    }
    #[inline(always)]
    fn message_offset(&self) -> Self::Offsets<'_> {
        self.message_offset.as_slice()
    }
    #[inline(always)]
    fn tag_name(&self) -> &[u8] {
        self.tag_name.as_slice()
    }
    #[inline(always)]
    fn tag_name_offset(&self) -> Self::Offsets<'_> {
        self.tag_name_offset.as_slice()
    }
}

impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, NoStrings, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::message_base64`]
    /// * [`SwhGraphProperties::message`]
    /// * [`SwhGraphProperties::tag_name_base64`]
    /// * [`SwhGraphProperties::tag_name`]
    pub fn load_strings(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, MappedStrings, LABELNAMES>>
    {
        let strings = MappedStrings {
            message: mmap(&suffix_path(&self.path, MESSAGE)).context("Could not load messages")?,
            message_offset: NumberMmap::new(
                suffix_path(&self.path, MESSAGE_OFFSET),
                self.num_nodes,
            )
            .context("Could not load message_offset")?,
            tag_name: mmap(&suffix_path(&self.path, TAG_NAME))
                .context("Could not load tag names")?,
            tag_name_offset: NumberMmap::new(
                suffix_path(&self.path, TAG_NAME_OFFSET),
                self.num_nodes,
            )
            .context("Could not load tag_name_offset")?,
        };
        self.with_strings(strings)
    }

    /// Alternative to [`load_strings`] that allows using arbitrary strings implementations
    pub fn with_strings<STRINGS: Strings>(
        self,
        strings: STRINGS,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: self.contents,
            strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

/// Functions to access message of `revision`/`release` nodes, and names of `release` nodes
///
/// Only available after calling [`load_strings`](SwhGraphProperties::load_strings)
/// or [`load_all_properties`](SwhGraph::load_all_properties)
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: Strings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    #[inline(always)]
    fn message_or_tag_name_base64<'a>(
        what: &'static str,
        data: &'a [u8],
        offsets: impl GetIndex<Output = u64>,
        node_id: NodeId,
    ) -> Option<&'a [u8]> {
        match offsets.get(node_id) {
            Some(u64::MAX) => None,
            None => None,
            Some(offset) => {
                let offset = offset as usize;
                let slice: &[u8] = data.get(offset..).unwrap_or_else(|| {
                    panic!("Missing {} for node {} at offset {}", what, node_id, offset)
                });
                slice
                    .iter()
                    .position(|&c| c == b'\n')
                    .map(|end| &slice[..end])
            }
        }
    }

    /// Returns the message of a revision or release, base64-encoded
    #[inline]
    pub fn message_base64(&self, node_id: NodeId) -> Option<&[u8]> {
        Self::message_or_tag_name_base64(
            "message",
            self.strings.message(),
            self.strings.message_offset(),
            node_id,
        )
    }

    /// Returns the message of a revision or release
    #[inline]
    pub fn message(&self, node_id: NodeId) -> Option<Vec<u8>> {
        let base64 = base64_simd::STANDARD;
        self.message_base64(node_id).map(|message| {
            base64.decode_to_vec(message).unwrap_or_else(|_| {
                panic!(
                    "Could not decode message of node {}: {:?}",
                    node_id, message
                )
            })
        })
    }

    /// Returns the tag name of a release, base64-encoded
    #[inline]
    pub fn tag_name_base64(&self, node_id: NodeId) -> Option<&[u8]> {
        Self::message_or_tag_name_base64(
            "tag_name",
            self.strings.tag_name(),
            self.strings.tag_name_offset(),
            node_id,
        )
    }

    /// Returns the tag name of a release
    #[inline]
    pub fn tag_name(&self, node_id: NodeId) -> Option<Vec<u8>> {
        let base64 = base64_simd::STANDARD;
        self.tag_name_base64(node_id).map(|tag_name| {
            base64.decode_to_vec(tag_name).unwrap_or_else(|_| {
                panic!(
                    "Could not decode tag_name of node {}: {:?}",
                    node_id, tag_name
                )
            })
        })
    }
}
