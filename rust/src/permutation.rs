// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ReadBytesExt};
use rayon::prelude::*;

/// An array of `n` unique integers in the `0..n` range.
pub struct Permutation(Vec<usize>);

impl Permutation {
    /// Loads a permutation from disk and returns IO errors if any.
    ///
    /// # Safety
    ///
    /// This function is not unsafe per-se, but it does not check node ids in the
    /// permutation's image are correct or unique, which will violate assumptions
    /// of other unsafe functions down the line.
    pub unsafe fn new_unchecked(num_nodes: usize, path: &Path) -> Result<Permutation> {
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
    pub fn new(num_nodes: usize, path: &Path) -> Result<Permutation> {
        let perm = unsafe { Permutation::new_unchecked(num_nodes, path) }?;

        // Check the permutation's image has the same size as the preimage
        if let Some((old, new)) = perm
            .as_ref()
            .par_iter()
            .enumerate()
            .find_any(|&(_old, &new)| new >= num_nodes)
        {
            bail!(
                "Found node {} has id {} in permutation, graph size is {}",
                old,
                new,
                num_nodes
            );
        }

        // Check the permutation is injective
        let mut seen = Vec::with_capacity(num_nodes as _);
        seen.extend((0..num_nodes).map(|_| AtomicBool::new(false)));
        if let Some((old, _)) = perm
            .as_ref()
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

        Ok(perm)
    }
}

impl AsRef<[usize]> for Permutation {
    fn as_ref(&self) -> &[usize] {
        self.0.as_ref()
    }
}

impl std::ops::Index<usize> for Permutation {
    type Output = usize;

    fn index(&self, old_node: usize) -> &Self::Output {
        &self.0[old_node]
    }
}
