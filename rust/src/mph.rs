// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Abstraction over possible Minimal-perfect hash functions

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::graph::NodeId;
use crate::java_compat::mph::gov::GOVMPH;
use crate::utils::suffix_path;
use crate::SWHID;

/// Minimal-perfect hash function over [`SWHID`].
///
/// See [`DynMphf`] which wraps all implementor structs in an enum to dynamically choose
/// which MPH algorithm to use with less overhead than `dyn SwhidMphf`.
pub trait SwhidMphf {
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

/// Enum of possible implementations of [`SwhidMphf`].
///
/// Loads either [`ph::fmph::Function`] or [`GOVMPH`]
/// depending on which file is available at the given path.
pub enum DynMphf {
    Fmph(ph::fmph::Function),
    GOV(GOVMPH),
}

impl std::fmt::Debug for DynMphf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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

impl SwhidMphf for DynMphf {
    fn load(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref();

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
