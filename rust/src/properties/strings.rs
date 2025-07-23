// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;

/// Trait implemented by both [`NoStrings`] and all implementors of [`Strings`],
/// to allow loading string properties only if needed.
pub trait MaybeStrings {}
impl<S: OptStrings> MaybeStrings for S {}

/// Placeholder for when string properties are not loaded
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NoStrings;
impl MaybeStrings for NoStrings {}

#[diagnostic::on_unimplemented(
    label = "does not have String properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_string()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait implemented by all implementors of [`MaybeStrings`] but [`NoStrings`]
pub trait OptStrings: MaybeStrings + PropertiesBackend {
    /// Returns an array with all messages, separated by `b'\n'`
    fn message(&self) -> PropertiesResult<'_, &[u8], Self>;
    /// Returns the position of the first character of `node`'s message in [`Self::message`],
    /// or `None` if it is out of bound, or `Some(u64::MAX)` if the node has no message
    fn message_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self>;
    /// Returns an array with all messages, separated by `b'\n'`
    fn tag_name(&self) -> PropertiesResult<'_, &[u8], Self>;
    /// Returns the position of the first character of `node`'s tag_name in [`Self::tag_name`],
    /// or `None` if it is out of bound, or `Some(u64::MAX)` if the node has no tag_name
    fn tag_name_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self>;
}

#[diagnostic::on_unimplemented(
    label = "does not have String properties loaded",
    note = "Use `let graph = graph.load_properties(|props| props.load_string()).unwrap()` to load them",
    note = "Or replace `graph.init_properties()` with `graph.load_all_properties::<DynMphf>().unwrap()` to load all properties"
)]
/// Trait for backend storage of string properties (either in-memory or memory-mapped)
pub trait Strings: OptStrings<DataFilesAvailability = GuaranteedDataFiles> {}
impl<S: OptStrings<DataFilesAvailability = GuaranteedDataFiles>> Strings for S {}

/// Variant of [`MappedStrings`] that checks at runtime that files are present every time
/// it is accessed
pub struct OptMappedStrings {
    message: Result<Mmap, UnavailableProperty>,
    message_offset: Result<NumberMmap<BigEndian, u64, Mmap>, UnavailableProperty>,
    tag_name: Result<Mmap, UnavailableProperty>,
    tag_name_offset: Result<NumberMmap<BigEndian, u64, Mmap>, UnavailableProperty>,
}
impl PropertiesBackend for OptMappedStrings {
    type DataFilesAvailability = OptionalDataFiles;
}
impl OptStrings for OptMappedStrings {
    #[inline(always)]
    fn message(&self) -> PropertiesResult<'_, &[u8], Self> {
        self.message.as_deref()
    }
    #[inline(always)]
    fn message_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.message_offset
            .as_ref()
            .map(|message_offsets| message_offsets.get(node))
    }
    #[inline(always)]
    fn tag_name(&self) -> PropertiesResult<'_, &[u8], Self> {
        self.tag_name.as_deref()
    }
    #[inline(always)]
    fn tag_name_offset(&self, node: NodeId) -> PropertiesResult<'_, Option<u64>, Self> {
        self.tag_name_offset
            .as_ref()
            .map(|tag_name_offsets| tag_name_offsets.get(node))
    }
}

/// [`Strings`] implementation backed by files guaranteed to be present once the graph is loaded
pub struct MappedStrings {
    message: Mmap,
    message_offset: NumberMmap<BigEndian, u64, Mmap>,
    tag_name: Mmap,
    tag_name_offset: NumberMmap<BigEndian, u64, Mmap>,
}
impl PropertiesBackend for MappedStrings {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl OptStrings for MappedStrings {
    #[inline(always)]
    fn message(&self) -> &[u8] {
        &self.message
    }
    #[inline(always)]
    fn message_offset(&self, node: NodeId) -> Option<u64> {
        (&self.message_offset).get(node)
    }
    #[inline(always)]
    fn tag_name(&self) -> &[u8] {
        &self.tag_name
    }
    #[inline(always)]
    fn tag_name_offset(&self, node: NodeId) -> Option<u64> {
        (&self.tag_name_offset).get(node)
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

impl PropertiesBackend for VecStrings {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl OptStrings for VecStrings {
    #[inline(always)]
    fn message(&self) -> &[u8] {
        self.message.as_slice()
    }
    #[inline(always)]
    fn message_offset(&self, node: NodeId) -> Option<u64> {
        self.message_offset.get(node)
    }
    #[inline(always)]
    fn tag_name(&self) -> &[u8] {
        self.tag_name.as_slice()
    }
    #[inline(always)]
    fn tag_name_offset(&self, node: NodeId) -> Option<u64> {
        self.tag_name_offset.get(node)
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
        let OptMappedStrings {
            message,
            message_offset,
            tag_name,
            tag_name_offset,
        } = self.get_strings()?;
        let strings = MappedStrings {
            message: message?,
            message_offset: message_offset?,
            tag_name: tag_name?,
            tag_name_offset: tag_name_offset?,
        };
        self.with_strings(strings)
    }
    /// Equivalent to [`Self::load_strings`] that does not require all files to be present
    pub fn opt_load_strings(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, OptMappedStrings, LABELNAMES>>
    {
        let strings = self.get_strings()?;
        self.with_strings(strings)
    }

    fn get_strings(&self) -> Result<OptMappedStrings> {
        Ok(OptMappedStrings {
            message: load_if_exists(&self.path, MESSAGE, |path| mmap(path))
                .context("Could not load message")?,
            message_offset: load_if_exists(&self.path, MESSAGE_OFFSET, |path| {
                NumberMmap::new(path, self.num_nodes)
            })
            .context("Could not load message_offset")?,
            tag_name: load_if_exists(&self.path, TAG_NAME, |path| mmap(path))
                .context("Could not load tag_name")?,
            tag_name_offset: load_if_exists(&self.path, TAG_NAME_OFFSET, |path| {
                NumberMmap::new(path, self.num_nodes)
            })
            .context("Could not load tag_name_offset")?,
        })
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
        STRINGS: OptStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    #[inline(always)]
    fn message_or_tag_name_base64<'a>(
        &self,
        what: &'static str,
        data: &'a [u8],
        offset: Option<u64>,
        node_id: NodeId,
    ) -> Result<Option<&'a [u8]>, OutOfBoundError> {
        match offset {
            None => Err(OutOfBoundError {
                // Unknown node
                index: node_id,
                len: self.num_nodes,
            }),
            Some(u64::MAX) => Ok(None), // No message
            Some(offset) => {
                let offset = offset as usize;
                let slice: &[u8] = data.get(offset..).unwrap_or_else(|| {
                    panic!("Missing {what} for node {node_id} at offset {offset}")
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
    pub fn message_base64(&self, node_id: NodeId) -> PropertiesResult<'_, Option<&[u8]>, STRINGS> {
        STRINGS::map_if_available(
            self.try_message_base64(node_id),
            |message: Result<_, OutOfBoundError>| {
                message.unwrap_or_else(|e| panic!("Cannot get node message: {e}"))
            },
        )
    }

    /// Returns the base64-encoded message of a revision or release
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no message.
    #[inline]
    pub fn try_message_base64(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<'_, Result<Option<&[u8]>, OutOfBoundError>, STRINGS> {
        STRINGS::map_if_available(
            STRINGS::zip_if_available(self.strings.message(), self.strings.message_offset(node_id)),
            |(messages, message_offset)| {
                self.message_or_tag_name_base64("message", messages, message_offset, node_id)
            },
        )
    }
    /// Returns the message of a revision or release,
    /// or the URL of an origin
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn message(&self, node_id: NodeId) -> PropertiesResult<'_, Option<Vec<u8>>, STRINGS> {
        STRINGS::map_if_available(self.try_message(node_id), |message| {
            message.unwrap_or_else(|e| panic!("Cannot get node message: {e}"))
        })
    }

    /// Returns the message of a revision or release,
    /// or the URL of an origin
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no message.
    #[inline]
    pub fn try_message(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<'_, Result<Option<Vec<u8>>, OutOfBoundError>, STRINGS> {
        let base64 = base64_simd::STANDARD;
        STRINGS::map_if_available(self.try_message_base64(node_id), |message_opt_res| {
            message_opt_res.map(|message_opt| {
                message_opt.map(|message| {
                    base64
                        .decode_to_vec(message)
                        .unwrap_or_else(|e| panic!("Could not decode node message: {e}"))
                })
            })
        })
    }

    /// Returns the tag name of a release, base64-encoded
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn tag_name_base64(&self, node_id: NodeId) -> PropertiesResult<'_, Option<&[u8]>, STRINGS> {
        STRINGS::map_if_available(self.try_tag_name_base64(node_id), |tag_name| {
            tag_name.unwrap_or_else(|e| panic!("Cannot get node tag: {e}"))
        })
    }

    /// Returns the tag name of a release, base64-encoded
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no tag name.
    #[inline]
    pub fn try_tag_name_base64(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<'_, Result<Option<&[u8]>, OutOfBoundError>, STRINGS> {
        STRINGS::map_if_available(
            STRINGS::zip_if_available(
                self.strings.tag_name(),
                self.strings.tag_name_offset(node_id),
            ),
            |(tag_names, tag_name_offset)| {
                self.message_or_tag_name_base64("tag_name", tag_names, tag_name_offset, node_id)
            },
        )
    }

    /// Returns the tag name of a release
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn tag_name(&self, node_id: NodeId) -> PropertiesResult<'_, Option<Vec<u8>>, STRINGS> {
        STRINGS::map_if_available(self.try_tag_name(node_id), |tag_name| {
            tag_name.unwrap_or_else(|e| panic!("Cannot get node tag name: {e}"))
        })
    }

    /// Returns the tag name of a release
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no tag name.
    #[inline]
    pub fn try_tag_name(
        &self,
        node_id: NodeId,
    ) -> PropertiesResult<'_, Result<Option<Vec<u8>>, OutOfBoundError>, STRINGS> {
        let base64 = base64_simd::STANDARD;
        STRINGS::map_if_available(self.try_tag_name_base64(node_id), |tag_name_opt_res| {
            tag_name_opt_res.map(|tag_name_opt| {
                tag_name_opt.map(|tag_name| {
                    base64.decode_to_vec(tag_name).unwrap_or_else(|_| {
                        panic!("Could not decode tag_name of node {node_id}: {tag_name:?}")
                    })
                })
            })
        })
    }
}
