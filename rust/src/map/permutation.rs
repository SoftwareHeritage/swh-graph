// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::{Read, Seek, Write};
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use dsi_progress_logger::{progress_logger, ProgressLog};
use mmap_rs::{Mmap, MmapFlags};
use rayon::prelude::*;

/// An array of `n` unique integers in the `0..n` range.
#[allow(clippy::len_without_is_empty)]
pub trait Permutation {
    /// Returns the number of items
    fn len(&self) -> usize;
    /// Returns an item
    fn get(&self, old_node: usize) -> Option<usize>;
    /// Returns an item without checking it is within the bounds
    ///
    /// # Safety
    ///
    /// Undefined behavior if `old_node >= len()`
    unsafe fn get_unchecked(&self, old_node: usize) -> usize;
}

/// A [`Permutation`] backed by an `usize` vector
pub struct OwnedPermutation<T: Sync + AsRef<[usize]>>(T);

impl<T: Sync + AsRef<[usize]>> OwnedPermutation<T> {
    /// Creates a permutation
    #[inline(always)]
    pub fn new_unchecked(perm: T) -> Self {
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
        let mut pl = progress_logger!(
            display_memory = true,
            item_name = "byte",
            local_speed = true,
            expected_updates = Some(self.len() * 8),
        );
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
            let chunk: &[u64] =
                bytemuck::try_cast_slice(chunk).expect("Could not cast &[usize] to &[u64]");
            BigEndian::write_u64_into(chunk, buf_slice);
            file.write_all(buf_slice)?;
            pl.update_with_count(chunk.len() * 8);
        }
        pl.done();
        Ok(())
    }
}

impl<T: Sync + AsRef<[usize]> + AsMut<[usize]>> OwnedPermutation<T> {
    /// Applies the other permutation to `self`, so `self` becomes a new permutation.
    pub fn compose_in_place<T2: Permutation + Sync>(&mut self, other: T2) -> Result<()> {
        if self.as_mut().len() != other.len() {
            bail!(
                "Cannot compose permutation of size {} with permutation of size {}",
                self.as_ref().len(),
                other.len()
            );
        }
        self.as_mut()
            .par_iter_mut()
            .for_each(|x| *x = unsafe { other.get_unchecked(*x) });

        Ok(())
    }
}

impl OwnedPermutation<Vec<usize>> {
    /// Loads a permutation from disk and returns IO errors if any.
    #[inline]
    pub fn load_unchecked(num_nodes: usize, path: &Path) -> Result<Self> {
        assert_eq!(
            usize::BITS,
            u64::BITS,
            "Only 64-bits architectures are supported"
        );

        let mut file = std::fs::File::open(path).context("Could not open permutation")?;

        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let epserde = &buf == b"epserde ";
        if epserde {
            use epserde::prelude::*;

            let perm = <Vec<usize>>::load_full(path)?;
            Ok(OwnedPermutation(perm))
        } else {
            let mut perm = Vec::with_capacity(num_nodes);
            file.rewind()?;
            file.read_u64_into::<BigEndian>(unsafe {
                std::mem::transmute::<&mut [MaybeUninit<usize>], &mut [u64]>(
                    perm.spare_capacity_mut(),
                )
            })
            .context("Could not read permutation")?;

            // read_u64_into() called read_exact(), which checked the length
            unsafe { perm.set_len(num_nodes) };

            Ok(OwnedPermutation(perm))
        }
    }

    /// Loads a permutation from disk, and returns errors in case of IO errors
    /// or incorrect permutations.
    #[inline]
    pub fn load(num_nodes: usize, path: &Path) -> Result<Self> {
        let perm = Self::load_unchecked(num_nodes, path)?;
        Self::new(perm.0)
    }
}

impl<T: Sync + AsRef<[usize]>> AsRef<[usize]> for OwnedPermutation<T> {
    #[inline(always)]
    fn as_ref(&self) -> &[usize] {
        self.0.as_ref()
    }
}

impl<T: Sync + AsRef<[usize]> + AsMut<[usize]>> AsMut<[usize]> for OwnedPermutation<T> {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [usize] {
        self.0.as_mut()
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

    fn get(&self, old_node: usize) -> Option<usize> {
        self.0.as_ref().get(old_node).copied()
    }

    unsafe fn get_unchecked(&self, old_node: usize) -> usize {
        *self.0.as_ref().get_unchecked(old_node)
    }
}

/// A [`Permutation`] backed by a big-endian mmapped file
pub struct MappedPermutation(Mmap);

impl MappedPermutation {
    /// Creates a permutation
    #[inline(always)]
    pub fn new_unchecked(perm: Mmap) -> Self {
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
    #[inline]
    pub fn load_unchecked(path: &Path) -> Result<Self> {
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

        let file = std::fs::File::open(path).context("Could not open permutation")?;
        let perm = unsafe {
            mmap_rs::MmapOptions::new(file_len as _)
                .context("Could not initialize permutation mmap")?
                .with_flags(MmapFlags::TRANSPARENT_HUGE_PAGES | MmapFlags::RANDOM_ACCESS)
                .with_file(&file, 0)
        }
        .map()
        .context("Could not mmap permutation")?;

        Ok(MappedPermutation(perm))
    }

    /// Loads a permutation from disk, and returns errors in case of IO errors
    /// or incorrect permutations.
    #[inline]
    pub fn load(num_nodes: usize, path: &Path) -> Result<Self> {
        let perm = Self::load_unchecked(path)?;
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

    fn get(&self, old_node: usize) -> Option<usize> {
        let range = (old_node * 8)..((old_node + 1) * 8);
        Some(BigEndian::read_u64(self.0.get(range)?) as usize)
    }

    unsafe fn get_unchecked(&self, old_node: usize) -> usize {
        let range = (old_node * 8)..((old_node + 1) * 8);
        BigEndian::read_u64(self.0.get_unchecked(range)) as usize
    }
}
