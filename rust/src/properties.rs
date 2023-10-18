// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Node labels

use std::path::Path;

use anyhow::{Context, Result};
use byteorder::BigEndian;
use mmap_rs::Mmap;

use crate::graph::NodeId;
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
}

use suffixes::*;

pub struct SwhGraphProperties<MPHF: SwhidMphf> {
    mphf: MPHF,
    order: MappedPermutation,
    node2swhid: Node2SWHID<Mmap>,
    node2type: Node2Type<UsizeMmap<Mmap>>,
    author_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    author_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
    committer_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    committer_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
}

impl<MPHF: SwhidMphf> SwhGraphProperties<MPHF> {
    pub fn new(path: impl AsRef<Path>, num_nodes: usize) -> Result<Self> {
        let properties = SwhGraphProperties {
            mphf: MPHF::load(&path)?,
            order: unsafe { MappedPermutation::load_unchecked(&suffix_path(&path, ".order")) }
                .context("Could not load order")?,
            node2swhid: Node2SWHID::load(suffix_path(&path, NODE2SWHID))
                .context("Could not load node2swhid")?,
            node2type: Node2Type::load(suffix_path(&path, NODE2TYPE), num_nodes)
                .context("Could not load node2type")?,
            author_timestamp: NumberMmap::new(suffix_path(&path, AUTHOR_TIMESTAMP), num_nodes)
                .context("Could not load author_timestamp")?,
            author_timestamp_offset: NumberMmap::new(
                suffix_path(&path, AUTHOR_TIMESTAMP_OFFSET),
                num_nodes,
            )
            .context("Could not load author_timestamp_offset")?,
            committer_timestamp: NumberMmap::new(
                suffix_path(&path, COMMITTER_TIMESTAMP),
                num_nodes,
            )
            .context("Could not load committer_timestamp")?,
            committer_timestamp_offset: NumberMmap::new(
                suffix_path(&path, COMMITTER_TIMESTAMP_OFFSET),
                num_nodes,
            )
            .context("Could not load committer_timestamp_offset")?,
        };
        Ok(properties)
    }

    /// Returns the node id of the given SWHID
    ///
    /// May return the id of a random node if the SWHID does not exist in the graph.
    ///
    /// # Panic
    ///
    /// May panic if the SWHID does not exist in the graph.
    #[inline]
    pub unsafe fn node_id_unchecked(&self, swhid: &SWHID) -> NodeId {
        self.order.get_unchecked(
            self.mphf
                .hash_swhid(swhid)
                .unwrap_or_else(|| panic!("Unknown SWHID {}", swhid)),
        )
    }

    /// Returns the node id of the given SWHID, or `None` if it does not exist.
    #[inline]
    pub fn node_id<T: TryInto<SWHID>>(&self, swhid: T) -> Option<NodeId> {
        let swhid = swhid.try_into().ok()?;
        let node_id = self.order.get(self.mphf.hash_swhid(&swhid)?)?;
        if self.node2swhid.get(node_id)? == swhid {
            Some(node_id)
        } else {
            None
        }
    }

    /// Returns the SWHID of a given node
    #[inline]
    pub fn swhid(&self, node_id: NodeId) -> Option<SWHID> {
        self.node2swhid.get(node_id)
    }

    /// Returns the type of a given node
    #[inline]
    pub fn node_type(&self, node_id: NodeId) -> Option<SWHType> {
        self.node2type.get(node_id)
    }

    /// Returns the number of seconds since Epoch that a release or revision was
    /// authored at
    pub fn author_timestamp(&self, node_id: NodeId) -> Option<i64> {
        match self.author_timestamp.get(node_id) {
            Some(i64::MIN) => None,
            ts => ts,
        }
    }

    /// Returns the UTC offset in minutes of a release or revision's authorship date
    pub fn author_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        match self.author_timestamp_offset.get(node_id) {
            Some(i16::MIN) => None,
            offset => offset,
        }
    }

    /// Returns the number of seconds since Epoch that a revision was committed at
    pub fn committer_timestamp(&self, node_id: NodeId) -> Option<i64> {
        match self.committer_timestamp.get(node_id) {
            Some(i64::MIN) => None,
            ts => ts,
        }
    }

    /// Returns the UTC offset in minutes of a revision's committer date
    pub fn committer_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        match self.committer_timestamp_offset.get(node_id) {
            Some(i16::MIN) => None,
            offset => offset,
        }
    }
}
