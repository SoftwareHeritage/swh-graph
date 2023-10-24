// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Node labels

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use byteorder::{BigEndian, LittleEndian};
use mmap_rs::Mmap;

use crate::graph::NodeId;
use crate::java_compat::bit_vector::LongArrayBitVector;
use crate::map::{MappedPermutation, Permutation};
use crate::map::{Node2SWHID, Node2Type, UsizeMmap};
use crate::mph::SwhidMphf;
use crate::utils::mmap::NumberMmap;
use crate::utils::suffix_path;
use crate::{SWHType, SWHID};

pub(crate) mod suffixes {
    pub const NODE2SWHID: &str = ".node2swhid.bin";
    pub const NODE2TYPE: &str = ".node2type.bin";
    pub const AUTHOR_TIMESTAMP: &str = ".property.author_timestamp.bin";
    pub const AUTHOR_TIMESTAMP_OFFSET: &str = ".property.author_timestamp_offset.bin";
    pub const COMMITTER_TIMESTAMP: &str = ".property.committer_timestamp.bin";
    pub const COMMITTER_TIMESTAMP_OFFSET: &str = ".property.committer_timestamp_offset.bin";
    pub const AUTHOR_ID: &str = ".property.author_id.bin";
    pub const COMMITTER_ID: &str = ".property.committer_id.bin";
    pub const CONTENT_IS_SKIPPED: &str = ".property.content.is_skipped.bin";
    pub const CONTENT_LENGTH: &str = ".property.content.length.bin";
    pub const MESSAGE: &str = ".property.message.bin";
    pub const MESSAGE_OFFSET: &str = ".property.message.offset.bin";
    pub const TAG_NAME: &str = ".property.tag_name.bin";
    pub const TAG_NAME_OFFSET: &str = ".property.tag_name.offset.bin";
}

use suffixes::*;

pub trait MapsOption {}

pub struct Maps<MPHF: SwhidMphf> {
    mphf: MPHF,
    order: MappedPermutation,
    node2swhid: Node2SWHID<Mmap>,
    node2type: Node2Type<UsizeMmap<Mmap>>,
}
impl<MPHF: SwhidMphf> MapsOption for Maps<MPHF> {}
impl MapsOption for () {}

pub trait TimestampsOption {}

pub struct Timestamps {
    author_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    author_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
    committer_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    committer_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
}
impl TimestampsOption for Timestamps {}
impl TimestampsOption for () {}

pub trait PersonsOption {}

pub struct Persons {
    author_id: NumberMmap<BigEndian, u32, Mmap>,
    committer_id: NumberMmap<BigEndian, u32, Mmap>,
}
impl PersonsOption for Persons {}
impl PersonsOption for () {}

pub trait ContentsOption {}

pub struct Contents {
    is_skipped_content: LongArrayBitVector<NumberMmap<LittleEndian, u64, Mmap>>,
    content_length: NumberMmap<BigEndian, u64, Mmap>,
}
impl ContentsOption for Contents {}
impl ContentsOption for () {}

pub trait StringsOption {}

pub struct Strings {
    message: Mmap,
    message_offset: NumberMmap<BigEndian, u64, Mmap>,
    tag_name: Mmap,
    tag_name_offset: NumberMmap<BigEndian, u64, Mmap>,
}
impl StringsOption for Strings {}
impl StringsOption for () {}

/// Properties on graph nodes
///
/// This structures has many type parameters, to allow loading only some properties,
/// and checking at compile time that only loaded properties are accessed.
///
/// Extra properties can be loaded, following the builder pattern.
pub struct SwhGraphProperties<
    MAPS: MapsOption,
    TIMESTAMPS: TimestampsOption,
    PERSONS: PersonsOption,
    CONTENTS: ContentsOption,
    STRINGS: StringsOption,
> {
    path: PathBuf,
    num_nodes: usize,
    maps: MAPS,
    timestamps: TIMESTAMPS,
    persons: PERSONS,
    contents: CONTENTS,
    strings: STRINGS,
}

pub type AllSwhGraphProperties<MPHF> =
    SwhGraphProperties<Maps<MPHF>, Timestamps, Persons, Contents, Strings>;

fn mmap(path: &Path) -> Result<Mmap> {
    let file_len = path
        .metadata()
        .with_context(|| format!("Could not stat {}", path.display()))?
        .len();
    let file =
        std::fs::File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let data = unsafe {
        mmap_rs::MmapOptions::new(file_len as _)
            .with_context(|| format!("Could not initialize mmap of size {}", file_len))?
            .with_flags(mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES)
            .with_file(file, 0)
            .map()
            .with_context(|| format!("Could not mmap {}", path.display()))?
    };
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(data.as_ptr() as *mut _, data.len(), libc::MADV_RANDOM)
    };
    Ok(data)
}

impl SwhGraphProperties<(), (), (), (), ()> {
    pub fn new(path: impl AsRef<Path>, num_nodes: usize) -> Self {
        SwhGraphProperties {
            path: path.as_ref().to_owned(),
            num_nodes,
            maps: (),
            timestamps: (),
            persons: (),
            contents: (),
            strings: (),
        }
    }
    pub fn load_all<MPHF: SwhidMphf>(self) -> Result<AllSwhGraphProperties<MPHF>> {
        Ok(self
            .load_maps()?
            .load_timestamps()?
            .load_persons()?
            .load_contents()?
            .load_strings()?)
    }
}

/**************************************************************************************
 * maps
 **************************************************************************************/
impl<
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<(), TIMESTAMPS, PERSONS, CONTENTS, STRINGS>
{
    pub fn load_maps<MPHF: SwhidMphf>(
        self,
    ) -> Result<SwhGraphProperties<Maps<MPHF>, TIMESTAMPS, PERSONS, CONTENTS, STRINGS>> {
        Ok(SwhGraphProperties {
            maps: Maps {
                mphf: MPHF::load(&self.path)?,
                order: unsafe {
                    MappedPermutation::load_unchecked(&suffix_path(&self.path, ".order"))
                }
                .context("Could not load order")?,
                node2swhid: Node2SWHID::load(suffix_path(&self.path, NODE2SWHID))
                    .context("Could not load node2swhid")?,
                node2type: Node2Type::load(suffix_path(&self.path, NODE2TYPE), self.num_nodes)
                    .context("Could not load node2type")?,
            },
            timestamps: self.timestamps,
            persons: self.persons,
            contents: self.contents,
            strings: self.strings,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MPHF: SwhidMphf,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<Maps<MPHF>, TIMESTAMPS, PERSONS, CONTENTS, STRINGS>
{
    /// Returns the node id of the given SWHID
    ///
    /// May return the id of a random node if the SWHID does not exist in the graph.
    ///
    /// # Panic
    ///
    /// May panic if the SWHID does not exist in the graph.
    #[inline]
    pub unsafe fn node_id_unchecked(&self, swhid: &SWHID) -> NodeId {
        self.maps.order.get_unchecked(
            self.maps
                .mphf
                .hash_swhid(swhid)
                .unwrap_or_else(|| panic!("Unknown SWHID {}", swhid)),
        )
    }

    /// Returns the node id of the given SWHID, or `None` if it does not exist.
    #[inline]
    pub fn node_id<T: TryInto<SWHID>>(&self, swhid: T) -> Option<NodeId> {
        let swhid = swhid.try_into().ok()?;
        let node_id = self.maps.order.get(self.maps.mphf.hash_swhid(&swhid)?)?;
        if self.maps.node2swhid.get(node_id)? == swhid {
            Some(node_id)
        } else {
            None
        }
    }

    /// Returns the SWHID of a given node
    #[inline]
    pub fn swhid(&self, node_id: NodeId) -> Option<SWHID> {
        self.maps.node2swhid.get(node_id)
    }

    /// Returns the type of a given node
    #[inline]
    pub fn node_type(&self, node_id: NodeId) -> Option<SWHType> {
        self.maps.node2type.get(node_id)
    }
}

/**************************************************************************************
 * timestamps
 **************************************************************************************/
impl<
        MAPS: MapsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, (), PERSONS, CONTENTS, STRINGS>
{
    pub fn load_timestamps(
        self,
    ) -> Result<SwhGraphProperties<MAPS, Timestamps, PERSONS, CONTENTS, STRINGS>> {
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
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, Timestamps, PERSONS, CONTENTS, STRINGS>
{
    /// Returns the number of seconds since Epoch that a release or revision was
    /// authored at
    pub fn author_timestamp(&self, node_id: NodeId) -> Option<i64> {
        match self.timestamps.author_timestamp.get(node_id) {
            Some(i64::MIN) => None,
            ts => ts,
        }
    }

    /// Returns the UTC offset in minutes of a release or revision's authorship date
    pub fn author_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        match self.timestamps.author_timestamp_offset.get(node_id) {
            Some(i16::MIN) => None,
            offset => offset,
        }
    }

    /// Returns the number of seconds since Epoch that a revision was committed at
    pub fn committer_timestamp(&self, node_id: NodeId) -> Option<i64> {
        match self.timestamps.committer_timestamp.get(node_id) {
            Some(i64::MIN) => None,
            ts => ts,
        }
    }

    /// Returns the UTC offset in minutes of a revision's committer date
    pub fn committer_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        match self.timestamps.committer_timestamp_offset.get(node_id) {
            Some(i16::MIN) => None,
            offset => offset,
        }
    }
}

/**************************************************************************************
 * persons
 **************************************************************************************/
impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, (), CONTENTS, STRINGS>
{
    pub fn load_persons(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, Persons, CONTENTS, STRINGS>> {
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
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        CONTENTS: ContentsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, Persons, CONTENTS, STRINGS>
{
    /// Returns the id of the author of a revision or release, if any
    pub fn author_id(&self, node_id: NodeId) -> Option<u32> {
        match self.persons.author_id.get(node_id) {
            Some(u32::MAX) => None,
            id => id,
        }
    }

    /// Returns the id of the committer of a revision, if any
    pub fn committer_id(&self, node_id: NodeId) -> Option<u32> {
        match self.persons.committer_id.get(node_id) {
            Some(u32::MAX) => None,
            id => id,
        }
    }
}

/**************************************************************************************
 * contents
 **************************************************************************************/
impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, (), STRINGS>
{
    pub fn load_contents(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, Contents, STRINGS>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: Contents {
                is_skipped_content: LongArrayBitVector::new_from_path(
                    suffix_path(&self.path, CONTENT_IS_SKIPPED),
                    self.num_nodes,
                )
                .context("Could not load is_skipped_content")?,
                content_length: NumberMmap::new(
                    suffix_path(&self.path, CONTENT_LENGTH),
                    self.num_nodes,
                )
                .context("Could not load content_length")?,
            },
            strings: self.strings,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        STRINGS: StringsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, Contents, STRINGS>
{
    /// Returns whether the node is a skipped content
    ///
    /// Non-content objects get a `false` value, like non-skipped contents.
    pub fn is_skipped_content(&self, node_id: NodeId) -> Option<bool> {
        self.contents.is_skipped_content.get(node_id)
    }

    /// Returns the length of the given content None.
    ///
    /// May be `None` for skipped contents
    pub fn content_length(&self, node_id: NodeId) -> Option<u64> {
        match self.contents.content_length.get(node_id) {
            Some(u64::MAX) => None,
            length => length,
        }
    }
}

/**************************************************************************************
 * messages
 **************************************************************************************/
impl<
        MAPS: MapsOption,
        TIMESTAMPS: TimestampsOption,
        PERSONS: PersonsOption,
        CONTENTS: ContentsOption,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, ()>
{
    pub fn load_strings(
        self,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, Strings>> {
        Ok(SwhGraphProperties {
            maps: self.maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: self.contents,
            strings: Strings {
                message: mmap(&suffix_path(&self.path, MESSAGE))
                    .context("Could not load messages")?,
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
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, Strings>
{
    #[inline(always)]
    fn message_or_tag_name_base64<'a>(
        what: &'static str,
        data: &'a Mmap,
        offsets: &NumberMmap<BigEndian, u64, Mmap>,
        node_id: NodeId,
    ) -> Option<&'a [u8]> {
        match offsets.get(node_id) {
            Some(u64::MAX) => None,
            None => None,
            Some(offset) => {
                let offset = offset as usize;
                let slice: &[u8] = data.get(offset..).expect(&format!(
                    "Missing {} for node {} at offset {}",
                    what, node_id, offset
                ));
                slice
                    .iter()
                    .position(|&c| c == b'\n')
                    .map(|end| &slice[..end])
            }
        }
    }

    /// Returns the message of a revision or release, base64-encoded
    pub fn message_base64(&self, node_id: NodeId) -> Option<&[u8]> {
        Self::message_or_tag_name_base64(
            "message",
            &self.strings.message,
            &self.strings.message_offset,
            node_id,
        )
    }

    /// Returns the message of a revision or release
    pub fn message(&self, node_id: NodeId) -> Option<Vec<u8>> {
        let base64 = base64_simd::STANDARD;
        self.message_base64(node_id).map(|message| {
            base64.decode_to_vec(message).expect(&format!(
                "Could not decode message of node {}: {:?}",
                node_id, message
            ))
        })
    }

    /// Returns the tag name of a release, base64-encoded
    pub fn tag_name_base64(&self, node_id: NodeId) -> Option<&[u8]> {
        Self::message_or_tag_name_base64(
            "tag_name",
            &self.strings.tag_name,
            &self.strings.tag_name_offset,
            node_id,
        )
    }

    /// Returns the tag name of a release
    pub fn tag_name(&self, node_id: NodeId) -> Option<Vec<u8>> {
        let base64 = base64_simd::STANDARD;
        self.tag_name_base64(node_id).map(|tag_name| {
            base64.decode_to_vec(tag_name).expect(&format!(
                "Could not decode tag_name of node {}: {:?}",
                node_id, tag_name
            ))
        })
    }
}
