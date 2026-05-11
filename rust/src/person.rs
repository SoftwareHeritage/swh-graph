// Copyright (C) 2025-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use epserde::deser::{Deserialize, Flags, MemCase};
use mmap_rs::Mmap;
use sha2::{Digest, Sha256};
use sux::{
    bits::{BitFieldVec, BitVec},
    dict::{elias_fano::EfSeq, EliasFano},
    rank_sel::SelectAdaptConst,
    traits::IndexedSeq,
};

type FullnamesOffsets = MemCase<
    EliasFano<
        SelectAdaptConst<BitVec<Box<[usize]>>, Box<[usize]>>,
        BitFieldVec<usize, Box<[usize]>>,
    >,
>;

pub struct FullnameMap {
    fullnames: Mmap,
    offsets: FullnamesOffsets,
    graph_path: PathBuf,
}

impl FullnameMap {
    /// Constructs a new `FullnameMap`.
    pub fn new(graph_path: PathBuf) -> Result<FullnameMap> {
        let fullnames_path = suffix_path(&graph_path, ".persons");
        let offsets_path = suffix_path(&graph_path, ".persons.ef");
        let fullnames = mmap(&fullnames_path)
            .with_context(|| format!("Could not mmap {}", fullnames_path.display()))?;
        let offsets = unsafe { <EfSeq>::mmap(&offsets_path, Flags::RANDOM_ACCESS) }
            .with_context(|| format!("Could not mmap {}", offsets_path.display()))?;
        Ok(FullnameMap {
            fullnames,
            offsets,
            graph_path,
        })
    }

    /// Maps an author ID to its corresponding full name in the SWH graph
    ///
    /// Returns the full name corresponding to the ID.
    ///
    /// # Example
    ///
    /// ```
    /// use std::path::PathBuf;
    /// use anyhow::Result;
    /// use swh_graph::person::FullnameMap;
    ///
    /// fn get_fullname(id: usize, graph_path: PathBuf) -> Result<Vec<u8>> {
    ///     Ok(FullnameMap::new(graph_path)?.map_id(id)?.to_owned())
    /// }
    /// ```
    pub fn map_id(&self, id: usize) -> Result<&[u8]> {
        match self.try_map_id(id) {
            Ok(Some(fullname)) => Ok(fullname),
            Ok(None) => bail!(
                "Invalid id {id}, there are only {} fullnames",
                self.offsets.uncase().len()
            ),
            Err(e) => Err(e),
        }
    }

    /// Maps an author ID to its corresponding full name in the SWH graph,
    /// or `None` if it does not exist
    ///
    /// Returns the full name corresponding to the ID.
    ///
    /// # Example
    /// ```
    /// use std::path::PathBuf;
    /// use anyhow::Result;
    /// use swh_graph::person::FullnameMap;
    ///
    /// fn get_fullname(id: usize, graph_path: PathBuf) -> Result<Option<Vec<u8>>> {
    ///     Ok(FullnameMap::new(graph_path)?.try_map_id(id)?.map(|fullname| fullname.to_owned()))
    /// }
    /// ```
    pub fn try_map_id(&self, id: usize) -> Result<Option<&[u8]>> {
        let offsets = self.offsets.uncase();
        if id + 1 >= offsets.len() {
            return Ok(None);
        }
        Ok(Some(
            self.fullnames
                .get(offsets.get(id)..offsets.get(id + 1))
                .with_context(|| {
                    format!(
                        "Out-of-bound access to {}.persons, index is probably corrupted",
                        self.graph_path.display()
                    )
                })?,
        ))
    }
}

fn suffix_path<P: AsRef<Path>, S: AsRef<std::ffi::OsStr>>(path: P, suffix: S) -> PathBuf {
    let mut path = path.as_ref().as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

fn mmap(path: &Path) -> Result<Mmap> {
    let file_len = path
        .metadata()
        .with_context(|| format!("Could not stat {}", path.display()))?
        .len();
    let file =
        std::fs::File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let data = unsafe {
        mmap_rs::MmapOptions::new(file_len as _)
            .with_context(|| format!("Could not initialize mmap of size {file_len}"))?
            .with_flags(
                mmap_rs::MmapFlags::TRANSPARENT_HUGE_PAGES | mmap_rs::MmapFlags::RANDOM_ACCESS,
            )
            .with_file(&file, 0)
            .map()
            .with_context(|| format!("Could not mmap {}", path.display()))?
    };
    Ok(data)
}

// visibility hack; remove it once the re-export from rust/src/compress/persons.rs is removed
pub(crate) mod person_struct {
    pub struct PseudonymizedPerson<T: AsRef<[u8]>>(pub T);
}

use person_struct::PseudonymizedPerson;

impl<T: AsRef<[u8]>> Hash for PseudonymizedPerson<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ref().hash(state)
    }
}

pub trait PersonMphf {
    fn num_keys(&self) -> u32;
    fn hash_pseudonymized_person(
        &self,
        pseudonymized_person: PseudonymizedPerson<&[u8]>,
    ) -> Result<u32>;
}

pub trait LoadablePersonMphf: PersonMphf {
    fn load(basepath: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;
}

#[cfg(feature = "pthash")]
mod person_pthash {
    use super::*;
    use pthash::{DictionaryDictionary, Hashable, Minimal, MurmurHash2_64, PartitionedPhf, Phf};

    impl<T: AsRef<[u8]>> Hashable for PseudonymizedPerson<T> {
        type Bytes<'a>
            = &'a [u8]
        where
            T: 'a;
        fn as_bytes(&self) -> Self::Bytes<'_> {
            self.0.as_ref()
        }
    }

    pub struct PersonPthash(PartitionedPhf<Minimal, MurmurHash2_64, DictionaryDictionary>);

    impl PersonMphf for PersonPthash {
        fn num_keys(&self) -> u32 {
            self.0
                .num_keys()
                .try_into()
                .expect("person MPH is too large")
        }

        fn hash_pseudonymized_person(
            &self,
            pseudonymized_person: PseudonymizedPerson<&[u8]>,
        ) -> Result<u32> {
            Ok(self
                .0
                .hash(&pseudonymized_person)
                .try_into()
                .expect("person MPH overflowed"))
        }
    }

    impl LoadablePersonMphf for PersonPthash {
        fn load(path: impl AsRef<Path>) -> Result<Self> {
            let path = path.as_ref();
            Ok(PersonPthash(Phf::load(path).with_context(|| {
                format!("Could not load pthash person MPHF {}", path.display())
            })?))
        }
    }
}
#[cfg(feature = "pthash")]
pub use person_pthash::*;

pub struct PersonPhast(pub ph::phast::Function2<ph::seeds::Bits8>);

impl PersonMphf for PersonPhast {
    fn num_keys(&self) -> u32 {
        self.0
            .output_range()
            .try_into()
            .expect("person MPH is too large")
    }
    fn hash_pseudonymized_person(
        &self,
        pseudonymized_person: PseudonymizedPerson<&[u8]>,
    ) -> Result<u32> {
        Ok(self
            .0
            .get(&pseudonymized_person)
            .try_into()
            .expect("person MPH overflowed"))
    }
}

impl LoadablePersonMphf for PersonPhast {
    fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
        Ok(PersonPhast(
            ph::phast::Function2::read(&mut BufReader::new(file))
                .with_context(|| format!("Could not load phast person MPHF {}", path.display()))?,
        ))
    }
}

#[allow(clippy::large_enum_variant)]
pub enum DynPersonMphf {
    #[cfg(feature = "pthash")]
    Pthash(PersonPthash),
    Phast(PersonPhast),
}

impl std::fmt::Debug for DynPersonMphf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "pthash")]
            DynPersonMphf::Pthash(_) => write!(f, "DynMphf::Pthash(_)"),
            DynPersonMphf::Phast(_) => write!(f, "DynMphf::Phast(_)"),
        }
    }
}

#[cfg(feature = "pthash")]
impl From<PersonPthash> for DynPersonMphf {
    #[inline(always)]
    fn from(value: PersonPthash) -> DynPersonMphf {
        DynPersonMphf::Pthash(value)
    }
}

impl From<PersonPhast> for DynPersonMphf {
    #[inline(always)]
    fn from(value: PersonPhast) -> DynPersonMphf {
        DynPersonMphf::Phast(value)
    }
}

impl PersonMphf for DynPersonMphf {
    fn num_keys(&self) -> u32 {
        match self {
            #[cfg(feature = "pthash")]
            DynPersonMphf::Pthash(mphf) => mphf.num_keys(),
            DynPersonMphf::Phast(mphf) => mphf.num_keys(),
        }
    }
    fn hash_pseudonymized_person(
        &self,
        pseudonymized_person: PseudonymizedPerson<&[u8]>,
    ) -> Result<u32> {
        match self {
            #[cfg(feature = "pthash")]
            DynPersonMphf::Pthash(mphf) => mphf.hash_pseudonymized_person(pseudonymized_person),
            DynPersonMphf::Phast(mphf) => mphf.hash_pseudonymized_person(pseudonymized_person),
        }
    }
}

impl LoadablePersonMphf for DynPersonMphf {
    fn load(basepath: impl AsRef<Path>) -> Result<Self> {
        let basepath = basepath.as_ref();

        let phast_path = suffix_path(basepath, ".phast");
        if phast_path.exists() {
            return PersonPhast::load(phast_path).map(Self::Phast);
        }

        let pthash_path = suffix_path(basepath, ".pthash");
        if pthash_path.exists() {
            #[cfg(not(feature = "pthash"))]
            bail!(
                "Cannot load persons MPHF {} because pthash support is disabled. Recompile swh-graph with --features pthash.",
                pthash_path.display()
            );
            #[cfg(feature = "pthash")]
            return PersonPthash::load(pthash_path).map(Self::Pthash);
        }

        bail!(
            "Cannot load MPH function, neither {} nor {} exists.",
            phast_path.display(),
            pthash_path.display(),
        );
    }
}

#[derive(Clone, Copy)]
pub struct PersonHasher<'a, MPHF: PersonMphf = DynPersonMphf> {
    mphf: &'a MPHF,
}

impl<'a, MPHF: PersonMphf> PersonHasher<'a, MPHF> {
    #[inline(always)]
    pub fn new(mphf: &'a MPHF) -> Self {
        PersonHasher { mphf }
    }

    #[inline(always)]
    pub fn mphf(&self) -> &'a MPHF {
        self.mphf
    }

    #[inline(always)]
    pub fn num_persons(&self) -> u32 {
        self.mphf.num_keys()
    }

    #[cfg(feature = "compression")]
    #[doc(hidden)]
    #[deprecated(since = "11.4.0", note = "Use hash_pseudonymized_person instead")]
    pub fn hash<T: AsRef<[u8]>>(&self, person_name: T) -> Result<u32> {
        self.hash_pseudonymized_person(person_name)
    }

    /// `pseudonymized_person` should be the same format as used in the public export,
    /// ie. `base64(sha256(fullname))`.
    pub fn hash_pseudonymized_person<T: AsRef<[u8]>>(
        &self,
        pseudonymized_person: T,
    ) -> Result<u32> {
        let digest = pseudonymized_person.as_ref();
        ensure!(
            digest.len() == 44,
            "Expected pseudonym to be a 44 bytes (base64-encoded SHA256 digest), got {} bytes: {:?}",
            digest.len(),
            digest
        );
        self.mphf
            .hash_pseudonymized_person(PseudonymizedPerson(pseudonymized_person.as_ref()))
    }

    pub fn hash_person_fullname<T: AsRef<[u8]>>(&self, person_fullname: T) -> Result<u32> {
        let base64 = base64_simd::STANDARD;
        self.hash_pseudonymized_person(
            base64
                .encode_to_string(Sha256::digest(person_fullname))
                .into_bytes()
                .into_boxed_slice(),
        )
    }
}
