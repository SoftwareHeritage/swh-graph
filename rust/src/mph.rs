// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Abstraction over possible Minimal-perfect hash functions

use std::path::Path;

use anyhow::{bail, Context, Result};
use pthash::{DictionaryDictionary, Minimal, MurmurHash2_128, PartitionedPhf, Phf};

use crate::graph::NodeId;
use crate::java_compat::mph::gov::GOVMPH;
use crate::map::{MappedPermutation, Permutation};
use crate::utils::suffix_path;
use crate::SWHID;

/// Minimal-perfect hash function over [`SWHID`].
///
/// See [`DynMphf`] which wraps all implementer structs in an enum to dynamically choose
/// which MPH algorithm to use with less overhead than `dyn SwhidMphf`.
pub trait SwhidMphf {
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

#[diagnostic::on_unimplemented(
    label = "requires LoadableSwhidMphf (not just SwhidMphf)",
    note = "swh-graph v7.0.0 split some methods of the SwhidMphf trait into a new LoadableSwhidMphf trait."
)]
/// Sub-trait of [`SwhidMphf`] for "concrete" MPHF implementations
pub trait LoadableSwhidMphf: SwhidMphf {
    type WithMappedPermutation: SwhidMphf;

    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;

    /// Given the base path of the MPH, mmaps the associated .order file and returns it
    fn with_mapped_permutation(
        self,
        basepath: impl AsRef<Path>,
    ) -> Result<Self::WithMappedPermutation>;
}

/// Composition of a MPHF and a permutation
pub struct PermutedMphf<MPHF: SwhidMphf, P: Permutation> {
    mphf: MPHF,
    permutation: P,
}

impl<MPHF: SwhidMphf, P: Permutation> SwhidMphf for PermutedMphf<MPHF, P> {
    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        self.mphf
            .hash_array(swhid)
            .and_then(|id| self.permutation.get(id))
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        self.mphf
            .hash_str(swhid)
            .and_then(|id| self.permutation.get(id))
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        self.mphf
            .hash_str_array(swhid)
            .and_then(|id| self.permutation.get(id))
    }

    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        self.mphf
            .hash_swhid(swhid)
            .and_then(|id| self.permutation.get(id))
    }
}

/// Trivial implementation of [`SwhidMphf`] that stores the list of items in a vector
pub struct VecMphf {
    pub swhids: Vec<SWHID>,
}

impl SwhidMphf for VecMphf {
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

impl LoadableSwhidMphf for GOVMPH {
    type WithMappedPermutation = PermutedMphf<Self, MappedPermutation>;

    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = suffix_path(basepath, ".cmph");
        GOVMPH::load(&path).with_context(|| format!("Could not load {}", path.display()))
    }

    fn with_mapped_permutation(
        self,
        basepath: impl AsRef<Path>,
    ) -> Result<Self::WithMappedPermutation> {
        let suffix = ".order"; // not .cmph.order because .order predates "modular" MPH functions
        let permutation = MappedPermutation::load_unchecked(&suffix_path(&basepath, suffix))?;
        Ok(PermutedMphf {
            mphf: self,
            permutation,
        })
    }
}

impl SwhidMphf for GOVMPH {
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
    type Bytes<'a>
        = &'a T
    where
        T: 'a;
    fn as_bytes(&self) -> Self::Bytes<'_> {
        &self.0
    }
}

impl LoadableSwhidMphf for SwhidPthash {
    type WithMappedPermutation = PermutedMphf<Self, MappedPermutation>;

    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized,
    {
        let path = suffix_path(basepath, ".pthash");
        SwhidPthash::load(path)
    }

    fn with_mapped_permutation(
        self,
        basepath: impl AsRef<Path>,
    ) -> Result<Self::WithMappedPermutation> {
        let suffix = ".pthash.order";
        let permutation = MappedPermutation::load_unchecked(&suffix_path(&basepath, suffix))?;
        Ok(PermutedMphf {
            mphf: self,
            permutation,
        })
    }
}

impl SwhidMphf for SwhidPthash {
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
/// Loads either [`SwhidPthash`] or [`GOVMPH`]
/// depending on which file is available at the given path.
pub enum DynMphf {
    Pthash(SwhidPthash),
    GOV(GOVMPH),
}

impl std::fmt::Debug for DynMphf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DynMphf::Pthash(_) => write!(f, "DynMphf::Pthash(_)"),
            DynMphf::GOV(_) => write!(f, "DynMphf::GOV(_)"),
        }
    }
}

impl From<GOVMPH> for DynMphf {
    fn from(value: GOVMPH) -> DynMphf {
        DynMphf::GOV(value)
    }
}

impl From<SwhidPthash> for DynMphf {
    fn from(value: SwhidPthash) -> DynMphf {
        DynMphf::Pthash(value)
    }
}

impl LoadableSwhidMphf for DynMphf {
    type WithMappedPermutation = PermutedDynMphf;

    fn load(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref();

        let pthash_path = suffix_path(basepath, ".pthash");
        if pthash_path.exists() {
            return LoadableSwhidMphf::load(basepath)
                .map(Self::Pthash)
                .with_context(|| format!("Could not load {}", pthash_path.display()));
        }

        let gov_path = suffix_path(basepath, ".cmph");
        if gov_path.exists() {
            return LoadableSwhidMphf::load(basepath)
                .map(Self::GOV)
                .with_context(|| format!("Could not load {}", gov_path.display()));
        }

        bail!(
            "Cannot load MPH function, neither {} nor {} exists.",
            pthash_path.display(),
            gov_path.display()
        );
    }

    fn with_mapped_permutation(
        self,
        basepath: impl AsRef<Path>,
    ) -> Result<Self::WithMappedPermutation> {
        match self {
            Self::Pthash(mphf) => mphf
                .with_mapped_permutation(basepath)
                .map(PermutedDynMphf::Pthash),
            Self::GOV(mphf) => mphf
                .with_mapped_permutation(basepath)
                .map(PermutedDynMphf::GOV),
        }
    }
}

impl SwhidMphf for DynMphf {
    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_array(swhid),
            Self::GOV(mphf) => mphf.hash_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_str(swhid),
            Self::GOV(mphf) => mphf.hash_str(swhid),
        }
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_str_array(swhid),
            Self::GOV(mphf) => mphf.hash_str_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_swhid(swhid),
            Self::GOV(mphf) => mphf.hash_swhid(swhid),
        }
    }
}

/// [`DynMphf`] composed with a permutation
pub enum PermutedDynMphf {
    Pthash(<SwhidPthash as LoadableSwhidMphf>::WithMappedPermutation),
    GOV(<GOVMPH as LoadableSwhidMphf>::WithMappedPermutation),
}

impl SwhidMphf for PermutedDynMphf {
    #[inline(always)]
    fn hash_array(&self, swhid: &[u8; SWHID::BYTES_SIZE]) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_array(swhid),
            Self::GOV(mphf) => mphf.hash_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_str(&self, swhid: impl AsRef<str>) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_str(swhid),
            Self::GOV(mphf) => mphf.hash_str(swhid),
        }
    }

    #[inline(always)]
    fn hash_str_array(&self, swhid: &[u8; 50]) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_str_array(swhid),
            Self::GOV(mphf) => mphf.hash_str_array(swhid),
        }
    }

    #[inline(always)]
    fn hash_swhid(&self, swhid: &SWHID) -> Option<NodeId> {
        match self {
            Self::Pthash(mphf) => mphf.hash_swhid(swhid),
            Self::GOV(mphf) => mphf.hash_swhid(swhid),
        }
    }
}
