// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::mem::MaybeUninit;
use std::path::Path;

use anyhow::{bail, Context, Result};
use byteorder::{BigEndian, ReadBytesExt};
use rayon::prelude::*;

pub struct Permutation(Vec<usize>);

impl Permutation {
    pub fn new(num_nodes: usize, path: &Path) -> Result<Permutation> {
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

        if let Some((old, new)) = perm
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

        Ok(Permutation(perm))
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
