// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Node labels

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{bail, Context, Result};
use byteorder::BigEndian;
use mmap_rs::Mmap;

use crate::graph::NodeId;
use crate::map::{MappedPermutation, Permutation};
use crate::map::{Node2SWHID, Node2Type, UsizeMmap};
use crate::utils::mmap::NumberMmap;
use crate::utils::suffix_path;
use crate::{SWHType, SWHID};

/// Minimal-perfect hash function over [`SWHID`].
///
/// See [`load_swhid_mphf`] to dynamically choose which implementor struct to use.
pub trait SwhidMphf {
    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;

    /// Hashes a SWHID's binary representation
    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        self.hash_swhid(&swhid.clone().try_into().ok()?)
    }

    /// Hashes a SWHID's textual representation
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId>;

    /// Hashes a SWHID's textual representation
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId>;

    /// Hashes a [`SWHID`]
    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        self.hash_str(swhid.to_string())
    }
}

impl SwhidMphf for ph::fmph::Function {
    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = suffix_path(basepath, ".fmph");
        let file =
            File::open(&path).with_context(|| format!("Could not read {}", path.display()))?;
        ph::fmph::Function::read(&mut BufReader::new(file)).context("Could not parse mph")
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        Some(self.get(swhid.as_ref().as_bytes())? as usize)
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        Some(self.get(swhid)? as usize)
    }
}

impl SwhidMphf for crate::java_compat::mph::gov::GOVMPH {
    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = suffix_path(basepath, ".cmph");
        crate::java_compat::mph::gov::GOVMPH::load(&path)
            .with_context(|| format!("Could not load {}", path.display()))
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        Some(self.get_byte_array(swhid.as_ref().as_bytes()) as usize)
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        Some(self.get_byte_array(swhid) as usize)
    }
}

/// Enum of possible implementations of [`SwhidMphf`].
///
/// Loads either [`ph::fmph::Function`] or [`swh_graph::java_compat::mph::gov::GOVMPH`]
/// depending on which file is available at the given path.
pub enum DynMphf {
    Fmph(ph::fmph::Function),
    GOV(crate::java_compat::mph::gov::GOVMPH),
}

impl std::fmt::Debug for DynMphf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DynMphf::Fmph(_) => write!(f, "DynMphf::Fmph(_)"),
            DynMphf::GOV(_) => write!(f, "DynMphf::GOV(_)"),
        }
    }
}

impl SwhidMphf for DynMphf {
    fn load(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref();

        let fmph_path = suffix_path(basepath, ".fmph");
        if fmph_path.exists() {
            return ph::fmph::Function::load(basepath).map(Self::Fmph);
        }

        let gov_path = suffix_path(basepath, ".cmph");
        if gov_path.exists() {
            return crate::java_compat::mph::gov::GOVMPH::load(basepath).map(Self::GOV);
        }

        bail!(
            "Cannot load MPH function, neither {} nor {} exists.",
            fmph_path.display(),
            gov_path.display()
        );
    }

    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        match self {
            Self::Fmph(mphf) => mphf.hash_array(swhid),
            Self::GOV(mphf) => mphf.hash_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        match self {
            Self::Fmph(mphf) => mphf.hash_str(swhid),
            Self::GOV(mphf) => mphf.hash_str(swhid),
        }
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        match self {
            Self::Fmph(mphf) => mphf.hash_str_array(swhid),
            Self::GOV(mphf) => mphf.hash_str_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        match self {
            Self::Fmph(mphf) => mphf.hash_swhid(swhid),
            Self::GOV(mphf) => mphf.hash_swhid(swhid),
        }
    }
}

pub struct SwhGraphProperties<MPHF: SwhidMphf> {
    mphf: MPHF,
    order: MappedPermutation,
    node2swhid: Node2SWHID<Mmap>,
    node2type: Node2Type<UsizeMmap<Mmap>>,
    author_timestamp: NumberMmap<BigEndian, i64, Mmap>,
    author_timestamp_offset: NumberMmap<BigEndian, i16, Mmap>,
}

impl<MPHF: SwhidMphf> SwhGraphProperties<MPHF> {
    pub fn new(path: impl AsRef<Path>, num_nodes: usize) -> Result<Self> {
        let properties = SwhGraphProperties {
            mphf: MPHF::load(&path)?,
            order: unsafe { MappedPermutation::load_unchecked(&suffix_path(&path, ".order")) }
                .context("Could not load order")?,
            node2swhid: Node2SWHID::load(suffix_path(&path, ".node2swhid.bin"))
                .context("Could not load node2swhid")?,
            node2type: Node2Type::load(suffix_path(&path, ".node2type.bin"), num_nodes)
                .context("Could not load node2type")?,
            author_timestamp: NumberMmap::new(
                suffix_path(&path, ".property.author_timestamp.bin"),
                num_nodes,
            )
            .context("Could not load author_timestamp")?,
            author_timestamp_offset: NumberMmap::new(
                suffix_path(&path, ".property.author_timestamp_offset.bin"),
                num_nodes,
            )
            .context("Could not load author_timestamp_offset")?,
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
    pub fn node_id(&self, swhid: &SWHID) -> Option<NodeId> {
        let node_id = self.order.get(self.mphf.hash_swhid(swhid)?)?;
        if self.node2swhid.get(node_id)? == *swhid {
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
        self.author_timestamp.get(node_id)
    }

    /// Returns the UTC offset in minutes of a release or revision's authorship date
    pub fn author_timestamp_offset(&self, node_id: NodeId) -> Option<i16> {
        self.author_timestamp_offset.get(node_id)
    }
}
