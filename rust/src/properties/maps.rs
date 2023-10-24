// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use mmap_rs::Mmap;

use super::suffixes::*;
use super::*;
use crate::graph::NodeId;
use crate::map::{MappedPermutation, Permutation};
use crate::map::{Node2SWHID, Node2Type, UsizeMmap};
use crate::mph::SwhidMphf;
use crate::utils::suffix_path;
use crate::{SWHType, SWHID};

pub trait MapsOption {}

pub struct Maps<MPHF: SwhidMphf> {
    mphf: MPHF,
    order: MappedPermutation,
    node2swhid: Node2SWHID<Mmap>,
    node2type: Node2Type<UsizeMmap<Mmap>>,
}
impl<MPHF: SwhidMphf> MapsOption for Maps<MPHF> {}
impl MapsOption for () {}

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
