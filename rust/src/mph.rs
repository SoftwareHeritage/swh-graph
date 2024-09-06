// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Abstraction over possible Minimal-perfect hash functions

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{bail, Context, Result};
use pthash::{DictionaryDictionary, Minimal, MurmurHash2_128, PartitionedPhf, Phf};

use crate::graph::NodeId;
use crate::java_compat::mph::gov::GOVMPH;
use crate::utils::suffix_path;
use crate::SWHID;

/// Minimal-perfect hash function over [`SWHID`].
///
/// See [`DynMphf`] which wraps all implementor structs in an enum to dynamically choose
/// which MPH algorithm to use with less overhead than `dyn SwhidMphf`.
pub trait SwhidMphf {
    /// Returns the extension of the file containing a permutation to apply after the MPH.
    fn order_suffix(&self) -> Option<&'static str>;

    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;

    /// Hashes a SWHID's binary representation
    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        self.hash_swhid(&(*swhid).try_into().ok()?)
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

/// Trivial implementation of [`SwhidMphf`] that stores the list of items in a vector
pub struct VecMphf {
    pub swhids: Vec<SWHID>,
}

impl SwhidMphf for VecMphf {
    fn order_suffix(&self) -> Option<&'static str> {
        None
    }

    fn load(_basepath: impl AsRef<Path>) -> Result<Self> {
        unimplemented!("VecMphf cannot be loaded from disk");
    }

    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        swhid
            .as_ref()
            .try_into()
            .ok()
            .and_then(|swhid: SWHID| self.hash_swhid(&swhid))
    }

    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        String::from_utf8(swhid.to_vec())
            .ok()
            .and_then(|swhid| self.hash_str(swhid))
    }

    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        self.swhids.iter().position(|item| item == swhid)
    }
}

impl SwhidMphf for ph::fmph::Function {
    fn order_suffix(&self) -> Option<&'static str> {
        Some(".fmph.order")
    }

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

impl SwhidMphf for GOVMPH {
    fn order_suffix(&self) -> Option<&'static str> {
        Some(".order") // not .cmph because .order predates "modular" MPHs
    }

    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = suffix_path(basepath, ".cmph");
        GOVMPH::load(&path).with_context(|| format!("Could not load {}", path.display()))
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

pub struct SwhidPthash(pub PartitionedPhf<Minimal, MurmurHash2_128, DictionaryDictionary>);

impl SwhidPthash {
    pub fn load(path: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = path.as_ref();
        PartitionedPhf::load(path)
            .with_context(|| format!("Could not load {}", path.display()))
            .map(SwhidPthash)
    }
}

/// Newtype to make SWHID hashable by PTHash
pub(crate) struct HashableSWHID<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> pthash::Hashable for HashableSWHID<T> {
    type Bytes<'a> = &'a T where T: 'a;
    fn as_bytes(&self) -> Self::Bytes<'_> {
        &self.0
    }
}

impl SwhidMphf for SwhidPthash {
    fn order_suffix(&self) -> Option<&'static str> {
        Some(".pthash.order")
    }

    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = suffix_path(basepath, ".pthash");
        SwhidPthash::load(path)
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        Some(self.0.hash(HashableSWHID(swhid.as_ref().as_bytes())) as usize)
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        Some(self.0.hash(HashableSWHID(swhid)) as usize)
    }
}

/// Enum of possible implementations of [`SwhidMphf`].
///
/// Loads either [`ph::fmph::Function`] or [`GOVMPH`]
/// depending on which file is available at the given path.
pub enum DynMphf {
    Pthash(SwhidPthash),
    Fmph(ph::fmph::Function),
    GOV(GOVMPH),
}

impl std::fmt::Debug for DynMphf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DynMphf::Pthash(_) => write!(f, "DynMphf::Pthash(_)"),
            DynMphf::Fmph(_) => write!(f, "DynMphf::Fmph(_)"),
            DynMphf::GOV(_) => write!(f, "DynMphf::GOV(_)"),
        }
    }
}

impl From<GOVMPH> for DynMphf {
    fn from(value: GOVMPH) -> DynMphf {
        DynMphf::GOV(value)
    }
}

impl From<ph::fmph::Function> for DynMphf {
    fn from(value: ph::fmph::Function) -> DynMphf {
        DynMphf::Fmph(value)
    }
}

impl From<SwhidPthash> for DynMphf {
    fn from(value: SwhidPthash) -> DynMphf {
        DynMphf::Pthash(value)
    }
}

impl SwhidMphf for DynMphf {
    fn order_suffix(&self) -> Option<&'static str> {
        match self {
            DynMphf::Pthash(f) => f.order_suffix(),
            DynMphf::Fmph(f) => f.order_suffix(),
            DynMphf::GOV(f) => f.order_suffix(),
        }
    }

    fn load(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref();

        let pthash_path = suffix_path(basepath, ".pthash");
        if pthash_path.exists() {
            return SwhidMphf::load(basepath)
                .map(Self::Pthash)
                .with_context(|| format!("Could not load {}", pthash_path.display()));
        }

        let fmph_path = suffix_path(basepath, ".fmph");
        if fmph_path.exists() {
            return SwhidMphf::load(basepath)
                .map(Self::Fmph)
                .with_context(|| format!("Could not load {}", fmph_path.display()));
        }

        let gov_path = suffix_path(basepath, ".cmph");
        if gov_path.exists() {
            return SwhidMphf::load(basepath)
                .map(Self::GOV)
                .with_context(|| format!("Could not load {}", gov_path.display()));
        }

        bail!(
            "Cannot load MPH function, neither {}, {}, nor {} exists.",
            pthash_path.display(),
            fmph_path.display(),
            gov_path.display()
        );
    }

    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_array(swhid),
            Self::Fmph(mphf) => mphf.hash_array(swhid),
            Self::GOV(mphf) => mphf.hash_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_str(swhid),
            Self::Fmph(mphf) => mphf.hash_str(swhid),
            Self::GOV(mphf) => mphf.hash_str(swhid),
        }
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_str_array(swhid),
            Self::Fmph(mphf) => mphf.hash_str_array(swhid),
            Self::GOV(mphf) => mphf.hash_str_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_swhid(swhid),
            Self::Fmph(mphf) => mphf.hash_swhid(swhid),
            Self::GOV(mphf) => mphf.hash_swhid(swhid),
        }
    }
}
