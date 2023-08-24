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
use rayon::prelude::*;

/// An array of `n` unique integers in the `0..n` range.
pub struct Permutation(Vec<usize>);

impl Permutation {
    /// Creates a permutation
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    #[inline(always)]
    pub unsafe fn new_unchecked(perm: Vec<usize>) -> Self {
        Permutation(perm)
    }

    /// Creates a permutation, or returns an error in case the permutation is
    /// incorrect
    #[inline]
    pub fn new(perm: Vec<usize>) -> Result<Self> {
        // Check the permutation's image has the same size as the preimage
        if let Some((old, new)) = perm
            .par_iter()
            .enumerate()
            .find_any(|&(_old, &new)| new >= perm.len())
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
            .map(|&node| seen[node].fetch_or(true, Ordering::SeqCst))
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

        Ok(Permutation(perm))
    }

    /// Loads a permutation from disk and returns IO errors if any.
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    #[inline]
    pub unsafe fn load_unchecked(num_nodes: usize, path: &Path) -> Result<Permutation> {
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

        Ok(Permutation(perm))
    }

    /// Loads a permutation from disk, and returns errors in case of IO errors
    /// or incorrect permutations.
    #[inline]
    pub fn load(num_nodes: usize, path: &Path) -> Result<Permutation> {
        let perm = unsafe { Permutation::load_unchecked(num_nodes, path) }?;
        Permutation::new(perm.0)
    }

    pub fn dump<W: Write>(&self, file: &mut W) -> std::io::Result<()> {
        let mut pl = ProgressLogger::default().display_memory();
        pl.item_name = "byte";
        pl.local_speed = true;
        pl.expected_updates = Some(self.0.len() * 8);
        pl.start("Writing permutation");

        let chunk_size = 1_000_000; // 1M of u64 -> 8MB
        let mut buf = vec![0u8; chunk_size * 8];
        for chunk in self.0.chunks(chunk_size) {
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

impl AsRef<[usize]> for Permutation {
    #[inline(always)]
    fn as_ref(&self) -> &[usize] {
        self.0.as_ref()
    }
}

impl std::ops::Index<usize> for Permutation {
    type Output = usize;

    #[inline(always)]
    fn index(&self, old_node: usize) -> &Self::Output {
        &self.0[old_node]
    }
}
