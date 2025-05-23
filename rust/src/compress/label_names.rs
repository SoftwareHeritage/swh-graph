// Copyright (C) 2024-2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::BufReader;
use std::ops::Deref;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use dsi_progress_logger::progress_logger;
use epserde::prelude::{Deserialize, Flags, MemCase, Serialize};
use lender::Lender;
use pthash::{DictionaryDictionary, Minimal, MurmurHash2_128, PartitionedPhf, Phf};
use sux::prelude::{BitFieldSlice, VBuilder, VFunc};
use sux::utils::{FromIntoIterator, FromLenderFactory, ZstdLineLender};

use crate::labels::FilenameId;
use crate::map::{MappedPermutation, Permutation};

#[derive(Debug)]
pub struct LabelName<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> pthash::Hashable for LabelName<T> {
    type Bytes<'a>
        = &'a [u8]
    where
        T: 'a;
    fn as_bytes(&self) -> Self::Bytes<'_> {
        self.0.as_ref()
    }
}

fn iter_labels(path: &Path) -> Result<impl Lender<Lend = Box<[u8]>>> {
    let base64 = base64_simd::STANDARD;
    let labels_file =
        File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    Ok(ZstdLineLender::new(BufReader::new(labels_file))
        .with_context(|| format!("Could not open {} as a zstd file", path.display()))?
        .map(move |label_base64: std::io::Result<&str>| {
            let label_base64 = label_base64.expect("Could not read line");
            base64
                .decode_to_vec(label_base64)
                .unwrap_or_else(|_| panic!("Label {}, could not be base64-decoded", label_base64))
                .into_boxed_slice()
        }))
}

#[derive(Debug)]
struct CannotReadLabelsError(anyhow::Error);

impl std::fmt::Display for CannotReadLabelsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not read labels: {}", self.0)
    }
}

impl std::error::Error for CannotReadLabelsError {}

/// Reads base64-encoded labels from the path and return a MPH function for them.
pub fn build_hasher(path: PathBuf, num_labels: usize) -> Result<LabelNameHasher<Box<[usize]>>> {
    let mut pass_counter = 0;
    let iter_labels = || -> Result<_, CannotReadLabelsError> {
        pass_counter += 1;
        iter_labels(&path).map_err(CannotReadLabelsError)
    };

    let builder = VBuilder::<_, Box<[usize]>>::default().expected_num_keys(num_labels);
    let mphf = builder
        .try_build_func(
            FromLenderFactory::new(iter_labels)
                .context("Could not initialize FromLenderFactory")?,
            FromIntoIterator::from(0..num_labels),
            &mut progress_logger!(display_memory = true, local_speed = true,),
        )
        .context("Failed to build VFunc")?
        .into();

    Ok(LabelNameHasher { mphf })
}

type LabelNameMphf<D> = VFunc<[u8], usize, D>;

/// Wrapper of [`VFunc`] that can hash label names
pub struct LabelNameHasher<
    D: BitFieldSlice<usize> = &'static [usize],
    M: Deref<Target = LabelNameMphf<D>> = MemCase<LabelNameMphf<D>>,
> {
    mphf: M,
}

impl LabelNameHasher {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mphf = LabelNameMphf::<Box<[usize]>>::mmap(path, Flags::empty())
            .with_context(|| format!("Could not mmap VFunc from {}", path.display()))?;
        Ok(LabelNameHasher { mphf })
    }
}

impl<D: BitFieldSlice<usize>, M: Deref<Target = LabelNameMphf<D>>> LabelNameHasher<D, M>
where
    <M as Deref>::Target: Serialize,
{
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let mut file = std::fs::File::create(path)
            .with_context(|| format!("Could not create {}", path.display()))?;
        self.mphf
            .serialize(&mut file)
            .with_context(|| format!("Could not serialize VFunc to {}", path.display()))?;
        Ok(())
    }
}

impl<D: BitFieldSlice<usize>> LabelNameHasher<D> {
    #[allow(clippy::len_without_is_empty)] // VFunc can't be empty
    pub fn len(&self) -> usize {
        self.mphf.len()
    }

    pub fn hash(&self, label_name: LabelName<impl AsRef<[u8]>>) -> Result<FilenameId> {
        Ok(FilenameId(
            self.mphf
                .get(label_name.0.as_ref())
                .try_into()
                .expect("label MPH overflowed"),
        ))
    }
}

// pthash requires 128-bits hash when using over 2^30 keys, and the 2024-05-16 production
// graph has just over 2^32 keys
pub type LabelNamePthashMphf = PartitionedPhf<Minimal, MurmurHash2_128, DictionaryDictionary>;

#[derive(Clone, Copy)]
pub struct LabelNamePthasher<'a> {
    mphf: &'a LabelNamePthashMphf,
    order: &'a MappedPermutation,
}

impl<'a> LabelNamePthasher<'a> {
    pub fn new(mphf: &'a LabelNamePthashMphf, order: &'a MappedPermutation) -> Result<Self> {
        ensure!(
            mphf.num_keys() == order.len() as u64,
            "Number of MPHF keys ({}) does not match permutation length ({})",
            mphf.num_keys(),
            order.len()
        );

        Ok(LabelNamePthasher { mphf, order })
    }

    pub fn hash(&self, label_name: Box<[u8]>) -> Result<FilenameId> {
        Ok(FilenameId(
            self.order
                .get(
                    self.mphf
                        .hash(LabelName(label_name))
                        .try_into()
                        .expect("label MPH overflowed"),
                )
                .context("out of bound dir entry name hash")?
                .try_into()
                .context("label permutation overflowed")?,
        ))
    }
}
