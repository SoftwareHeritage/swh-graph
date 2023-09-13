// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::Write;
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use dsi_progress_logger::ProgressLogger;
use mmap_rs::{Mmap, MmapFlags};
use rayon::prelude::*;

/// An array of `n` unique integers in the `0..n` range.
pub trait Permutation {
    fn len(&self) -> usize;
    fn get(&self, old_node: usize) -> usize;
}

/// A [`Permutation`] backed by an `usize` vector
pub struct OwnedPermutation<T: Sync + AsRef<[usize]>>(T);

impl<T: Sync + AsRef<[usize]>> OwnedPermutation<T> {
    /// Creates a permutation
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    #[inline(always)]
    pub unsafe fn new_unchecked(perm: T) -> Self {
        OwnedPermutation(perm)
    }

    /// Creates a permutation, or returns an error in case the permutation is
    /// incorrect
    #[inline]
    pub fn new(perm: T) -> Result<Self> {
        // Check the permutation's image has the same size as the preimage
        if let Some((old, new)) = perm
            .as_ref()
            .par_iter()
            .enumerate()
            .find_any(|&(_old, &new)| new >= perm.as_ref().len())
        {
            bail!(
                "Found node {} has id {} in permutation, graph size is {}",
                old,
                new,
                perm.as_ref().len()
            );
        }

        // Check the permutation is injective
        let mut seen = Vec::with_capacity(perm.as_ref().len() as _);
        seen.extend((0..perm.as_ref().len()).map(|_| AtomicBool::new(false)));
        if let Some((old, _)) = perm
            .as_ref()
            .par_iter()
            .map(|&node| seen[node].fetch_or(true, Ordering::SeqCst))
            .enumerate()
            .find_any(|&(_node, is_duplicated)| is_duplicated)
        {
            let new = perm.as_ref()[old];
            bail!(
                "At least two nodes are mapped to {}; one of them is {}",
                new,
                old,
            );
        }

        // Therefore, the permutation is bijective.

        Ok(OwnedPermutation(perm))
    }

    pub fn dump<W: Write>(&self, file: &mut W) -> std::io::Result<()> {
        let mut pl = ProgressLogger::default().display_memory();
        pl.item_name = "byte";
        pl.local_speed = true;
        pl.expected_updates = Some(self.len() * 8);
        pl.start("Writing permutation");

        let chunk_size = 1_000_000; // 1M of u64 -> 8MB
        let mut buf = vec![0u8; chunk_size * 8];
        for chunk in self.0.as_ref().chunks(chunk_size) {
            let buf_slice = &mut buf[..chunk.len() * 8]; // no-op except for the last chunk
            assert_eq!(
                usize::BITS,
                u64::BITS,
                "Only 64-bits architectures are supported"
            );
            BigEndian::write_u64_into(
                unsafe { std::mem::transmute::<&[usize], &[u64]>(chunk) },
                buf_slice,
            );
            file.write_all(buf_slice)?;
            pl.update_with_count(chunk.len() * 8);
        }
        pl.done();
        Ok(())
    }
}

impl OwnedPermutation<Vec<usize>> {
    /// Loads a permutation from disk and returns IO errors if any.
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    #[inline]
    pub unsafe fn load_unchecked(num_nodes: usize, path: &Path) -> Result<Self> {
        assert_eq!(
            usize::BITS,
            u64::BITS,
            "Only 64-bits architectures are supported"
        );
        let mut perm = Vec::with_capacity(num_nodes);

        std::fs::File::open(&path)
            .context("Could not open permutation")?
            .read_u64_into::<BigEndian>(unsafe {
                std::mem::transmute::<&mut [MaybeUninit<usize>], &mut [u64]>(
                    perm.spare_capacity_mut(),
                )
            })
            .context("Could not read permutation")?;

        // read_u64_into() called read_exact(), which checked the length
        unsafe { perm.set_len(num_nodes) };

        Ok(OwnedPermutation(perm))
    }

    /// Loads a permutation from disk, and returns errors in case of IO errors
    /// or incorrect permutations.
    #[inline]
    pub fn load(num_nodes: usize, path: &Path) -> Result<Self> {
        let perm = unsafe { Self::load_unchecked(num_nodes, path) }?;
        Self::new(perm.0)
    }
}

impl<T: Sync + AsRef<[usize]>> AsRef<[usize]> for OwnedPermutation<T> {
    #[inline(always)]
    fn as_ref(&self) -> &[usize] {
        self.0.as_ref()
    }
}

impl<T: Sync + AsRef<[usize]>> std::ops::Index<usize> for OwnedPermutation<T> {
    type Output = usize;

    #[inline(always)]
    fn index(&self, old_node: usize) -> &Self::Output {
        &self.0.as_ref()[old_node]
    }
}

impl<T: Sync + AsRef<[usize]>> Permutation for OwnedPermutation<T> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn get(&self, old_node: usize) -> usize {
        self[old_node]
    }
}

/// A [`Permutation`] backed by a big-endian mmapped file
pub struct MappedPermutation(Mmap);

impl MappedPermutation {
    /// Creates a permutation
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    #[inline(always)]
    pub unsafe fn new_unchecked(perm: Mmap) -> Self {
        MappedPermutation(perm)
    }

    /// Creates a permutation, or returns an error in case the permutation is
    /// incorrect
    #[inline]
    pub fn new(perm: Mmap) -> Result<Self> {
        assert_eq!(
            perm.size() % 8,
            0,
            "mmap has size {}, which is not a multiple of 8",
            perm.size()
        );

        // Check the permutation's image has the same size as the preimage
        if let Some((old, new)) = perm
            .par_iter()
            .chunks(8)
            .map(|bytes| {
                BigEndian::read_u64(bytes.into_iter().cloned().collect::<Vec<_>>().as_slice())
                    as usize
            })
            .enumerate()
            .find_any(|&(_old, new)| new >= perm.len())
        {
            bail!(
                "Found node {} has id {} in permutation, graph size is {}",
                old,
                new,
                perm.len()
            );
        }

        // Check the permutation is injective
        let mut seen = Vec::with_capacity(perm.len() as _);
        seen.extend((0..perm.len()).map(|_| AtomicBool::new(false)));
        if let Some((old, _)) = perm
            .par_iter()
            .chunks(8)
            .map(|bytes| {
                BigEndian::read_u64(bytes.into_iter().cloned().collect::<Vec<_>>().as_slice())
                    as usize
            })
            .map(|node| seen[node].fetch_or(true, Ordering::SeqCst))
            .enumerate()
            .find_any(|&(_node, is_duplicated)| is_duplicated)
        {
            let new = perm[old];
            bail!(
                "At least two nodes are mapped to {}; one of them is {}",
                new,
                old,
            );
        }

        // Therefore, the permutation is bijective.

        Ok(MappedPermutation(perm))
    }

    /// Loads a permutation from disk and returns IO errors if any.
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    #[inline]
    pub unsafe fn load_unchecked(path: &Path) -> Result<Self> {
        assert_eq!(
            usize::BITS,
            u64::BITS,
            "Only 64-bits architectures are supported"
        );

        let file_len = path.metadata()?.len();
        assert_eq!(
            file_len % 8,
            0,
            "{} has size {}, which is not a multiple of 8",
            path.display(),
            file_len
        );

        let file = std::fs::File::open(&path).context("Could not open permutation")?;
        let perm = mmap_rs::MmapOptions::new(file_len as _)
            .context("Could not initialize permutation mmap")?
            .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES)
            .with_file(file, 0)
            .map()
            .context("Could not mmap permutation")?;

        Ok(MappedPermutation(perm))
    }

    /// Loads a permutation from disk, and returns errors in case of IO errors
    /// or incorrect permutations.
    #[inline]
    pub fn load(num_nodes: usize, path: &Path) -> Result<Self> {
        let perm = unsafe { Self::load_unchecked(path) }?;
        assert_eq!(
            perm.len(),
            num_nodes,
            "Expected permutation to have length {}, got {}",
            num_nodes,
            perm.len()
        );
        Self::new(perm.0)
    }
}

impl Permutation for MappedPermutation {
    fn len(&self) -> usize {
        self.0.size() / 8
    }

    fn get(&self, old_node: usize) -> usize {
        BigEndian::read_u64(&self.0[(old_node * 8)..((old_node + 1) * 8)]) as usize
    }
}
