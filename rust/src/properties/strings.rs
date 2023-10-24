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

pub trait StringsOption {}

pub struct Strings {
    message: Mmap,
    message_offset: NumberMmap<BigEndian, u64, Mmap>,
    tag_name: Mmap,
    tag_name_offset: NumberMmap<BigEndian, u64, Mmap>,
}
impl StringsOption for Strings {}
impl StringsOption for () {}

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
