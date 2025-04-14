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

#[diagnostic::on_unimplemented(
    label = "does not have String properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_string()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait implemented by all implementors of [`MaybeStrings`] but [`NoStrings`]
pub trait LoadedStrings: MaybeStrings {
    type Result<T>: PropertyResult<Value = T>;
    type Offsets<'a>: GetIndex<Output = u64> + 'a
    where
        Self: 'a;

    fn message(&self) -> Self::Result<&[u8]>;
    fn message_offset(&self) -> Self::Result<Self::Offsets<'_>>;
    fn tag_name(&self) -> Self::Result<&[u8]>;
    fn tag_name_offset(&self) -> Self::Result<Self::Offsets<'_>>;
}

#[diagnostic::on_unimplemented(
    label = "does not have String properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_string()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
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

/// [`Strings`] implementation backed by files guaranteed to be present once the graph is loaded
pub struct MappedStrings {
    message: Mmap,
    message_offset: NumberMmap<BigEndian, u64, Mmap>,
    tag_name: Mmap,
    tag_name_offset: NumberMmap<BigEndian, u64, Mmap>,
}
impl<S: Strings> MaybeStrings for S {}

impl<S: Strings> LoadedStrings for S {
    type Result<T> = AlwaysPropertyResult<T>;
    type Offsets<'a>
        = <Self as Strings>::Offsets<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn message(&self) -> Self::Result<&[u8]> {
        AlwaysPropertyResult(<Self as Strings>::message(self))
    }
    #[inline(always)]
    fn message_offset(&self) -> Self::Result<Self::Offsets<'_>> {
        AlwaysPropertyResult(<Self as Strings>::message_offset(self))
    }
    #[inline(always)]
    fn tag_name(&self) -> Self::Result<&[u8]> {
        AlwaysPropertyResult(<Self as Strings>::tag_name(self))
    }
    #[inline(always)]
    fn tag_name_offset(&self) -> Self::Result<Self::Offsets<'_>> {
        AlwaysPropertyResult(<Self as Strings>::tag_name_offset(self))
    }
}

/// Variant of [`MappedStrings`] that checks at runtime that files are present every time
/// it is accessed
pub struct DynMappedStrings {
    message: Option<Mmap>,
    message_offset: Option<NumberMmap<BigEndian, u64, Mmap>>,
    tag_name: Option<Mmap>,
    tag_name_offset: Option<NumberMmap<BigEndian, u64, Mmap>>,
}
impl MaybeStrings for DynMappedStrings {}
impl LoadedStrings for DynMappedStrings {
    type Result<T> = OptionPropertyResult<T>;
    type Offsets<'a>
        = &'a NumberMmap<BigEndian, u64, Mmap>
    where
        Self: 'a;

    #[inline(always)]
    fn message(&self) -> Self::Result<&[u8]> {
        OptionPropertyResult(self.message.as_deref())
    }
    #[inline(always)]
    fn message_offset(&self) -> Self::Result<Self::Offsets<'_>> {
        OptionPropertyResult(self.message_offset.as_ref())
    }
    #[inline(always)]
    fn tag_name(&self) -> Self::Result<&[u8]> {
        OptionPropertyResult(self.tag_name.as_deref())
    }
    #[inline(always)]
    fn tag_name_offset(&self) -> Self::Result<Self::Offsets<'_>> {
        OptionPropertyResult(self.tag_name_offset.as_ref())
    }
}

/// Placeholder for when string properties are not loaded
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NoStrings;
impl MaybeStrings for NoStrings {}

impl Strings for MappedStrings {
    type Offsets<'a>
        = &'a NumberMmap<BigEndian, u64, Mmap>
    where
        Self: 'a;

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

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
    type Offsets<'a>
        = &'a [u64]
    where
        Self: 'a;

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
    /// Equivalent to [`Self::load_strings`] that does not require all files to be present
    pub fn load_strings_dyn(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, DynMappedStrings, LABELNAMES>>
    {
        let if_exists = |suffix| -> Result<_> {
            let path = suffix_path(&self.path, suffix);
            if std::fs::exists(&path)
                .with_context(|| format!("Could not stat {}", path.display()))?
            {
                Ok(Some(path))
            } else {
                Ok(None)
            }
        };
        let strings = DynMappedStrings {
            message: if_exists(MESSAGE)?
                .map(|path| mmap(path).context("Could not load messages"))
                .transpose()?,
            message_offset: if_exists(MESSAGE_OFFSET)?
                .map(|path| {
                    NumberMmap::new(path, self.num_nodes).context("Could not load message_offset")
                })
                .transpose()?,
            tag_name: if_exists(TAG_NAME)?
                .map(|path| mmap(path).context("Could not load tag names"))
                .transpose()?,
            tag_name_offset: if_exists(TAG_NAME_OFFSET)?
                .map(|path| {
                    NumberMmap::new(path, self.num_nodes).context("Could not load tag_name_offset")
                })
                .transpose()?,
        };
        self.with_strings(strings)
    }

    /// Alternative to [`load_strings`](Self::load_strings) that allows using arbitrary
    /// strings implementations
    pub fn with_strings<STRINGS: MaybeStrings>(
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
            label_names_are_in_base64_order: self.label_names_are_in_base64_order,
        })
    }
}

/// Functions to access message of `revision`/`release` nodes, and names of `release` nodes
///
/// Only available after calling [`load_strings`](SwhGraphProperties::load_strings)
/// or [`load_all_properties`](crate::graph::SwhBidirectionalGraph::load_all_properties)
impl<
        MAPS: MaybeMaps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: LoadedStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    #[inline(always)]
    fn message_or_tag_name_base64<'a>(
        what: &'static str,
        data: &'a [u8],
        offsets: impl GetIndex<Output = u64>,
        node_id: NodeId,
    ) -> Result<Option<&'a [u8]>, OutOfBoundError> {
        match offsets.get(node_id) {
            None => Err(OutOfBoundError {
                // Unknown node
                index: node_id,
                len: offsets.len(),
            }),
            Some(u64::MAX) => Ok(None), // No message
            Some(offset) => {
                let offset = offset as usize;
                let slice: &[u8] = data.get(offset..).unwrap_or_else(|| {
                    panic!("Missing {} for node {} at offset {}", what, node_id, offset)
                });
                Ok(slice
                    .iter()
                    .position(|&c| c == b'\n')
                    .map(|end| &slice[..end]))
            }
        }
    }

    /// Returns the base64-encoded message of a revision or release,
    /// or the base64-encoded URL of an origin
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn message_base64(
        &self,
        node_id: NodeId,
    ) -> <STRINGS::Result<Option<&[u8]>> as PropertyResult>::Finalized {
        self._try_message_base64(node_id)
            .unwrap_or_else(|e| panic!("Cannot get node message: {}", e))
            .finalize()
    }

    /// Returns the base64-encoded message of a revision or release
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no message.
    #[inline]
    pub fn try_message_base64(&self, node_id: NodeId) -> Result<<<STRINGS::Result<Option<&[u8]>> as PropertyResult>::MapResult<&[u8]> as PropertyResult>::Finalized, OutOfBoundError>{
        Ok(self._try_message_base64(node_id)?.finalize())
    }
    fn _try_message_base64(
        &self,
        node_id: NodeId,
    ) -> Result<<STRINGS::Result<Option<&[u8]>> as PropertyResult>::MapResult<&[u8]>, OutOfBoundError>
    {
        self.strings.message().and_then(|message| {
            self.strings.message_offset().map(|message_offset| {
                Self::message_or_tag_name_base64("message", message, message_offset, node_id)
            })
        })
    }
    /// Returns the message of a revision or release,
    /// or the URL of an origin
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn message(&self, node_id: NodeId) -> Option<Vec<u8>> {
        self.try_message(node_id)
            .unwrap_or_else(|e| panic!("Cannot get node message: {}", e))
    }

    /// Returns the message of a revision or release,
    /// or the URL of an origin
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no message.
    #[inline]
    pub fn try_message(&self, node_id: NodeId) -> Result<Option<Vec<u8>>, OutOfBoundError> {
        let base64 = base64_simd::STANDARD;
        self.try_message_base64(node_id).map(|message_opt| {
            message_opt.map(|message| {
                base64
                    .decode_to_vec(message)
                    .unwrap_or_else(|e| panic!("Could not decode node message: {}", e))
            })
        })
    }

    /// Returns the tag name of a release, base64-encoded
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn tag_name_base64(&self, node_id: NodeId) -> Option<&[u8]> {
        self.try_tag_name_base64(node_id)
            .unwrap_or_else(|e| panic!("Cannot get node tag: {}", e))
    }

    /// Returns the tag name of a release, base64-encoded
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no tag name.
    #[inline]
    pub fn try_tag_name_base64(&self, node_id: NodeId) -> Result<Option<&[u8]>, OutOfBoundError> {
        Self::message_or_tag_name_base64(
            "tag_name",
            self.strings.tag_name(),
            self.strings.tag_name_offset(),
            node_id,
        )
    }

    /// Returns the tag name of a release
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn tag_name(&self, node_id: NodeId) -> Option<Vec<u8>> {
        self.try_tag_name(node_id)
            .unwrap_or_else(|e| panic!("Cannot get node tag name: {}", e))
    }

    /// Returns the tag name of a release
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no tag name.
    #[inline]
    pub fn try_tag_name(&self, node_id: NodeId) -> Result<Option<Vec<u8>>, OutOfBoundError> {
        let base64 = base64_simd::STANDARD;
        self.try_tag_name_base64(node_id).map(|tag_name_opt| {
            tag_name_opt.map(|tag_name| {
                base64.decode_to_vec(tag_name).unwrap_or_else(|_| {
                    panic!(
                        "Could not decode tag_name of node {}: {:?}",
                        node_id, tag_name
                    )
                })
            })
        })
    }
}
