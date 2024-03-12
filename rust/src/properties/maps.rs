// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use mmap_rs::Mmap;
use thiserror::Error;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;
use crate::map::{MappedPermutation, OwnedPermutation, Permutation};
use crate::map::{Node2SWHID, Node2Type, UsizeMmap};
use crate::mph::{SwhidMphf, VecMphf};
use crate::utils::suffix_path;
use crate::{swhid::StrSWHIDDeserializationError, SWHType, SWHID};

/// Trait implemented by both [`NoMaps`] and all implementors of [`Maps`],
/// to allow loading maps only if needed.
pub trait MaybeMaps {}

pub struct MappedMaps<MPHF: SwhidMphf> {
    mphf: MPHF,
    order: MappedPermutation,
    node2swhid: Node2SWHID<Mmap>,
    node2type: Node2Type<UsizeMmap<Mmap>>,
}
impl<M: Maps> MaybeMaps for M {}

/// Placeholder for when maps are not loaded.
pub struct NoMaps;
impl MaybeMaps for NoMaps {}

/// Trait for backend storage of maps (either in-memory, or loaded from disk and memory-mapped)
pub trait Maps {
    type MPHF: SwhidMphf;
    type Perm: Permutation;
    type Memory: AsRef<[u8]>;

    fn mphf(&self) -> &Self::MPHF;
    fn order(&self) -> &Self::Perm;
    fn node2swhid(&self) -> &Node2SWHID<Self::Memory>;
    fn node2type(&self) -> &Node2Type<UsizeMmap<Self::Memory>>;
}

impl<MPHF: SwhidMphf> Maps for MappedMaps<MPHF> {
    type MPHF = MPHF;
    type Perm = MappedPermutation;
    type Memory = Mmap;

    #[inline(always)]
    fn mphf(&self) -> &Self::MPHF {
        &self.mphf
    }
    #[inline(always)]
    fn order(&self) -> &MappedPermutation {
        &self.order
    }
    #[inline(always)]
    fn node2swhid(&self) -> &Node2SWHID<Mmap> {
        &self.node2swhid
    }
    #[inline(always)]
    fn node2type(&self) -> &Node2Type<UsizeMmap<Mmap>> {
        &self.node2type
    }
}

/// Trivial implementation of [`Maps`] that stores everything in a vector,
/// instead of mmapping from disk
pub struct VecMaps {
    mphf: VecMphf,
    order: OwnedPermutation<Vec<usize>>,
    node2swhid: Node2SWHID<Vec<u8>>,
    node2type: Node2Type<UsizeMmap<Vec<u8>>>,
}

impl VecMaps {
    pub fn new(swhids: Vec<SWHID>) -> Self {
        let node2swhid = Node2SWHID::new_from_iter(swhids.iter().cloned());
        let node2type = Node2Type::new_from_iter(swhids.iter().map(|swhid| swhid.node_type));
        VecMaps {
            order: OwnedPermutation::new((0..swhids.len()).collect()).unwrap(),
            node2type,
            node2swhid,
            mphf: VecMphf { swhids: swhids },
        }
    }
}

impl Maps for VecMaps {
    type MPHF = VecMphf;
    type Perm = OwnedPermutation<Vec<usize>>;
    type Memory = Vec<u8>;

    #[inline(always)]
    fn mphf(&self) -> &Self::MPHF {
        &self.mphf
    }
    #[inline(always)]
    fn order(&self) -> &Self::Perm {
        &self.order
    }
    #[inline(always)]
    fn node2swhid(&self) -> &Node2SWHID<Self::Memory> {
        &self.node2swhid
    }
    #[inline(always)]
    fn node2type(&self) -> &Node2Type<UsizeMmap<Self::Memory>> {
        &self.node2type
    }
}

impl<
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<NoMaps, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Consumes a [`SwhGraphProperties`] and returns a new one with these methods
    /// available:
    ///
    /// * [`SwhGraphProperties::node_id_unchecked`]
    /// * [`SwhGraphProperties::node_id`]
    /// * [`SwhGraphProperties::swhid`]
    /// * [`SwhGraphProperties::node_type`]
    pub fn load_maps<MPHF: SwhidMphf>(
        self,
    ) -> Result<
        SwhGraphProperties<MappedMaps<MPHF>, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>,
    > {
        let maps = MappedMaps {
            mphf: MPHF::load(&self.path)?,
            order: unsafe { MappedPermutation::load_unchecked(&suffix_path(&self.path, ".order")) }
                .context("Could not load order")?,
            node2swhid: Node2SWHID::load(suffix_path(&self.path, NODE2SWHID))
                .context("Could not load node2swhid")?,
            node2type: Node2Type::load(suffix_path(&self.path, NODE2TYPE), self.num_nodes)
                .context("Could not load node2type")?,
        };
        self.with_maps(maps)
    }

    /// Alternative to [`load_maps`] that allows using arbitrary maps implementations
    pub fn with_maps<MAPS: Maps>(
        self,
        maps: MAPS,
    ) -> Result<SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>> {
        Ok(SwhGraphProperties {
            maps,
            timestamps: self.timestamps,
            persons: self.persons,
            contents: self.contents,
            strings: self.strings,
            label_names: self.label_names,
            path: self.path,
            num_nodes: self.num_nodes,
        })
    }
}

#[derive(Error, Debug, PartialEq, Eq, Hash)]
pub enum NodeIdFromSwhidError<E> {
    #[error("invalid SWHID")]
    InvalidSwhid(E),
    #[error("unknown SWHID: {0}")]
    UnknownSwhid(SWHID),
    #[error("internal error: {0}")]
    InternalError(&'static str),
}

/// Functions to map between SWHID and node id.
///
/// Only available after calling [`load_contents`](SwhGraphProperties::load_contents)
/// or [`load_all_properties`](SwhGraph::load_all_properties)
impl<
        MAPS: Maps,
        TIMESTAMPS: MaybeTimestamps,
        PERSONS: MaybePersons,
        CONTENTS: MaybeContents,
        STRINGS: MaybeStrings,
        LABELNAMES: MaybeLabelNames,
    > SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
{
    /// Returns the node id of the given SWHID
    ///
    /// May return the id of a random node if the SWHID does not exist in the graph.
    ///
    /// # Safety
    ///
    /// Undefined behavior if the swhid does not exist.
    #[inline]
    pub unsafe fn node_id_unchecked(&self, swhid: &SWHID) -> NodeId {
        self.maps.order().get_unchecked(
            self.maps
                .mphf()
                .hash_swhid(swhid)
                .unwrap_or_else(|| panic!("Unknown SWHID {}", swhid)),
        )
    }

    /// Returns the node id of the given SWHID, or `None` if it does not exist.
    #[inline]
    pub fn node_id<T: TryInto<SWHID>>(
        &self,
        swhid: T,
    ) -> Result<NodeId, NodeIdFromSwhidError<<T as TryInto<SWHID>>::Error>> {
        use NodeIdFromSwhidError::*;

        let swhid = swhid.try_into().map_err(InvalidSwhid)?;
        let hash = self
            .maps
            .mphf()
            .hash_swhid(&swhid)
            .ok_or(UnknownSwhid(swhid))?;
        let node_id = self
            .maps
            .order()
            .get(hash)
            .ok_or(InternalError(".order map is shorter than SWHID hash value"))?;
        let actual_swhid = self
            .maps
            .node2swhid()
            .get(node_id)
            .map_err(|()| InternalError("node2swhid map is shorter than SWHID hash value"))?;
        if actual_swhid == swhid {
            Ok(node_id)
        } else {
            Err(UnknownSwhid(swhid))
        }
    }

    /// Specialized version of `node_id` when the SWHID is a string
    ///
    /// Under the hood, when using [`GOVMPH`](crate::java_compat::mph::gov::GOVMPH),
    /// `node_id` serializes the SWHID to a string, which can be bottleneck.
    /// This function skips the serialization by working directly on the string.
    #[inline]
    pub fn node_id_from_string_swhid<T: AsRef<str>>(
        &self,
        swhid: T,
    ) -> Result<NodeId, NodeIdFromSwhidError<StrSWHIDDeserializationError>> {
        use NodeIdFromSwhidError::*;

        let swhid = swhid.as_ref();
        let hash = self
            .maps
            .mphf()
            .hash_str(swhid)
            .ok_or_else(|| match swhid.try_into() {
                Ok(swhid) => UnknownSwhid(swhid),
                Err(e) => InvalidSwhid(e),
            })?;
        let node_id = self
            .maps
            .order()
            .get(hash)
            .ok_or(InternalError(".order map is shorter than SWHID hash value"))?;
        let actual_swhid = self
            .maps
            .node2swhid()
            .get(node_id)
            .map_err(|()| InternalError("node2swhid map is shorter than SWHID hash value"))?;
        let swhid = SWHID::try_from(swhid).map_err(InvalidSwhid)?.into();
        if actual_swhid == swhid {
            Ok(node_id)
        } else {
            Err(UnknownSwhid(swhid))
        }
    }

    /// Returns the SWHID of a given node
    ///
    /// # Panics
    ///
    /// If the node id does not exist.
    #[inline]
    pub fn swhid(&self, node_id: NodeId) -> SWHID {
        self.try_swhid(node_id)
            .unwrap_or_else(|()| panic!("Cannot get SWHID of unknown node id: {}", node_id))
    }

    /// Returns the SWHID of a given node, or `None` if the node id does not exist
    #[inline]
    pub fn try_swhid(&self, node_id: NodeId) -> Result<SWHID, ()> {
        self.maps.node2swhid().get(node_id)
    }

    /// Returns the type of a given node
    ///
    /// # Panics
    ///
    /// If the node id does not exist.
    #[inline]
    pub fn node_type(&self, node_id: NodeId) -> SWHType {
        self.try_node_type(node_id)
            .unwrap_or_else(|()| panic!("Cannot get type of unknown node id: {}", node_id))
    }

    /// Returns the type of a given node, or `None` if the node id does not exist
    #[inline]
    pub fn try_node_type(&self, node_id: NodeId) -> Result<SWHType, ()> {
        self.maps.node2type().get(node_id)
    }
}
