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
pub trait LoadedStrings: MaybeStrings + PropertiesBackend {
    type Offsets<'a>: GetIndex<Output = u64> + 'a
    where
        Self: 'a;

    fn message_and_offsets(
        &self,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<(&[u8], Self::Offsets<'_>)>;
    fn tag_name_and_offsets(
        &self,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<(&[u8], Self::Offsets<'_>)>;
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

impl<S: Strings> PropertiesBackend for S {
    type DataFilesAvailability = GuaranteedDataFiles;
}
impl<S: Strings> LoadedStrings for S {
    type Offsets<'a>
        = <Self as Strings>::Offsets<'a>
    where
        Self: 'a;

    #[inline(always)]
    fn message_and_offsets(
        &self,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<(&[u8], Self::Offsets<'_>)>
    {
        (self.message(), self.message_offset())
    }
    #[inline(always)]
    fn tag_name_and_offsets(
        &self,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<(&[u8], Self::Offsets<'_>)>
    {
        (self.tag_name(), self.tag_name_offset())
    }
}

/// Variant of [`MappedStrings`] that checks at runtime that files are present every time
/// it is accessed
pub struct DynMappedStrings {
    message: Result<Mmap, UnavailableProperty>,
    message_offset: Result<NumberMmap<BigEndian, u64, Mmap>, UnavailableProperty>,
    tag_name: Result<Mmap, UnavailableProperty>,
    tag_name_offset: Result<NumberMmap<BigEndian, u64, Mmap>, UnavailableProperty>,
}
impl MaybeStrings for DynMappedStrings {}
impl PropertiesBackend for DynMappedStrings {
    type DataFilesAvailability = OptionalDataFiles;
}
impl LoadedStrings for DynMappedStrings {
    type Offsets<'a>
        = &'a NumberMmap<BigEndian, u64, Mmap>
    where
        Self: 'a;

    #[inline(always)]
    fn message_and_offsets(
        &self,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<(&[u8], Self::Offsets<'_>)>
    {
        match (&self.message, &self.message_offset) {
            (Ok(message), Ok(message_offset)) => Ok((message, message_offset)),
            (Err(e), _) => Err(e.clone()),
            (_, Err(e)) => Err(e.clone()),
        }
    }
    #[inline(always)]
    fn tag_name_and_offsets(
        &self,
    ) -> <Self::DataFilesAvailability as DataFilesAvailability>::Result<(&[u8], Self::Offsets<'_>)>
    {
        match (&self.tag_name, &self.tag_name_offset) {
            (Ok(message), Ok(message_offset)) => Ok((message, message_offset)),
            (Err(e), _) => Err(e.clone()),
            (_, Err(e)) => Err(e.clone()),
        }
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

    fn message(&self) -> &[u8] {
        &self.message
    }
    fn message_offset(&self) -> Self::Offsets<'_> {
        &self.message_offset
    }
    fn tag_name(&self) -> &[u8] {
        &self.tag_name
    }
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
        let DynMappedStrings {
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
    pub fn load_strings_dyn(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, DynMappedStrings, LABELNAMES>>
    {
        let strings = self.get_strings()?;
        self.with_strings(strings)
    }

    fn get_strings(&self) -> Result<DynMappedStrings> {
        fn if_exists<T>(
            base_path: &Path,
            suffix: &'static str,
            f: impl FnOnce(&Path) -> Result<T>,
        ) -> Result<Result<T, UnavailableProperty>> {
            let path = suffix_path(base_path, suffix);
            if std::fs::exists(&path).map_err(|source| UnavailableProperty {
                path: path.clone(),
                source: source.into(),
            })? {
                Ok(Ok(f(&path)?))
            } else {
                Ok(Err(UnavailableProperty {
                    path,
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "No such file")
                        .into(),
                }))
            }
        }
        Ok(DynMappedStrings {
            message: if_exists(&self.path, MESSAGE, |path| mmap(path))
                .context("Could not load message")?,
            message_offset: if_exists(&self.path, MESSAGE_OFFSET, |path| {
                NumberMmap::new(path, self.num_nodes)
            })
            .context("Could not load message_offset")?,
            tag_name: if_exists(&self.path, TAG_NAME, |path| mmap(path))
                .context("Could not load tag_name")?,
            tag_name_offset: if_exists(&self.path, TAG_NAME_OFFSET, |path| {
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
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<Option<&[u8]>> {
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.try_message_base64(node_id),
            |message: Result<_, OutOfBoundError>| {
                message.unwrap_or_else(|e| panic!("Cannot get node message: {}", e))
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
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<
        Result<Option<&[u8]>, OutOfBoundError>,
    > {
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.strings.message_and_offsets(),
            |(message, message_offset)| {
                Self::message_or_tag_name_base64("message", message, message_offset, node_id)
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
    pub fn message(
        &self,
        node_id: NodeId,
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<Option<Vec<u8>>> {
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.try_message(node_id),
            |message| message.unwrap_or_else(|e| panic!("Cannot get node message: {}", e)),
        )
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
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<
        Result<Option<Vec<u8>>, OutOfBoundError>,
    > {
        let base64 = base64_simd::STANDARD;
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.try_message_base64(node_id),
            |message_opt_res| {
                message_opt_res.map(|message_opt| {
                    message_opt.map(|message| {
                        base64
                            .decode_to_vec(message)
                            .unwrap_or_else(|e| panic!("Could not decode node message: {}", e))
                    })
                })
            },
        )
    }

    /// Returns the tag name of a release, base64-encoded
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn tag_name_base64(
        &self,
        node_id: NodeId,
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<Option<&[u8]>> {
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.try_tag_name_base64(node_id),
            |tag_name| tag_name.unwrap_or_else(|e| panic!("Cannot get node tag: {}", e)),
        )
    }

    /// Returns the tag name of a release, base64-encoded
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no tag name.
    #[inline]
    pub fn try_tag_name_base64(
        &self,
        node_id: NodeId,
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<
        Result<Option<&[u8]>, OutOfBoundError>,
    > {
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.strings.tag_name_and_offsets(),
            |(tag_name, tag_name_offset)| {
                Self::message_or_tag_name_base64("tag_name", tag_name, tag_name_offset, node_id)
            },
        )
    }

    /// Returns the tag name of a release
    ///
    /// # Panics
    ///
    /// If the node id does not exist
    #[inline]
    pub fn tag_name(
        &self,
        node_id: NodeId,
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<Option<Vec<u8>>> {
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.try_tag_name(node_id),
            |tag_name| tag_name.unwrap_or_else(|e| panic!("Cannot get node tag name: {}", e)),
        )
    }

    /// Returns the tag name of a release
    ///
    /// Returns `Err` if the node id is unknown, and `Ok(None)` if the node has
    /// no tag name.
    #[inline]
    pub fn try_tag_name(
        &self,
        node_id: NodeId,
    ) -> <STRINGS::DataFilesAvailability as DataFilesAvailability>::Result<
        Result<Option<Vec<u8>>, OutOfBoundError>,
    > {
        let base64 = base64_simd::STANDARD;
        <STRINGS::DataFilesAvailability as DataFilesAvailability>::map(
            self.try_tag_name_base64(node_id),
            |tag_name_opt_res| {
                tag_name_opt_res.map(|tag_name_opt| {
                    tag_name_opt.map(|tag_name| {
                        base64.decode_to_vec(tag_name).unwrap_or_else(|_| {
                            panic!(
                                "Could not decode tag_name of node {}: {:?}",
                                node_id, tag_name
                            )
                        })
                    })
                })
            },
        )
    }
}
