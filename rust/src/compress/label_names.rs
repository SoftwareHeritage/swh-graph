// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use dsi_progress_logger::{progress_logger, ProgressLog};
use rayon::prelude::*;

use crate::labels::LabelNameId;
use crate::map::{MappedPermutation, OwnedPermutation, Permutation};

pub struct LabelName<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Hash for LabelName<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ref().hash(state)
    }
}

// pthash requires 128-bits hash when using over 2^30 keys, and the 2024-05-16 production
// graph has just over 2^32 keys
pub type LabelNameMphf = ph::phast::Function2<ph::seeds::Bits8>;

fn iter_labels(path: &Path) -> Result<impl Iterator<Item = LabelName<Box<[u8]>>>> {
    let base64 = base64_simd::STANDARD;
    let labels_file =
        File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    Ok(BufReader::new(labels_file)
        .lines()
        .map(move |label_base64| {
            let label_base64 = label_base64.expect("Could not read line");
            LabelName(
                base64
                    .decode_to_vec(&label_base64)
                    .unwrap_or_else(|_| panic!("Label {label_base64}, could not be base64-decoded"))
                    .into_boxed_slice(),
            )
        }))
}

/// Reads base64-encoded labels from the path and return a MPH function for them.
pub fn build_mphf(path: PathBuf, num_labels: usize) -> Result<LabelNameMphf> {
    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "label",
        local_speed = true,
        expected_updates = Some(num_labels),
    );
    pl.start("Reading labels...");
    let labels: Vec<_> = iter_labels(&path)
        .expect("Could not read labels")
        .inspect(|_| pl.light_update())
        .collect();
    pl.done();
    ensure!(
        labels.len() == num_labels,
        "Expected {num_labels} labels, read {}",
        labels.len()
    );

    let mphf = LabelNameMphf::from_vec_mt(labels);
    let output_range = mphf.output_range();
    ensure!(
        output_range == num_labels,
        "Built MPHF from {num_labels}, but its range is {output_range}"
    );
    Ok(mphf)
}

/// Reads base64-encoded labels from the path and a MPH function for these labels
/// (as returned by [`build_mphf`]) and returns a permutation that maps their hashes
/// to their position in the (sorted) list.
pub fn build_order(
    path: PathBuf,
    mphf_path: PathBuf,
    num_labels: usize,
) -> Result<OwnedPermutation<Vec<usize>>> {
    assert_eq!(
        usize::BITS,
        u64::BITS,
        "Only 64-bits architectures are supported"
    );

    let file = File::open(&mphf_path)
        .with_context(|| format!("Could not open {}", mphf_path.display()))?;
    let mphf = LabelNameMphf::read(&mut BufReader::new(file))
        .with_context(|| format!("Could not load MPH from {}", mphf_path.display()))?;
    let output_range = mphf.output_range();
    ensure!(output_range == num_labels, "mphf.output_range() == {output_range} does not match expected number of keys ({num_labels})");

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "label",
        local_speed = true,
        expected_updates = Some(num_labels),
    );
    pl.start("Reading labels");

    let mut order: Vec<_> = (0..num_labels).map(|_| usize::MAX).collect();
    for (i, label) in iter_labels(&path)?.enumerate() {
        pl.light_update();
        let hash = mphf.get(&label);
        ensure!(hash < num_labels, "{} is not minimal", mphf_path.display());
        ensure!(
            order[hash] == usize::MAX,
            "hash collision involving {}",
            String::from_utf8_lossy(&label.0)
        );
        order[hash] = i;
    }

    log::info!("Checking permutation...");
    order.par_iter().enumerate().try_for_each(|(i, value)| {
        ensure!(*value != usize::MAX, "no label hash equals {}", i);
        Ok(())
    })?;

    Ok(OwnedPermutation::new_unchecked(order))
}

#[derive(Clone, Copy)]
pub struct LabelNameHasher<'a> {
    mphf: &'a LabelNameMphf,
    order: &'a MappedPermutation,
}

impl<'a> LabelNameHasher<'a> {
    pub fn new(mphf: &'a LabelNameMphf, order: &'a MappedPermutation) -> Result<Self> {
        ensure!(
            mphf.output_range() == order.len(),
            "Number of MPHF keys ({}) does not match permutation length ({})",
            mphf.output_range(),
            order.len()
        );

        Ok(LabelNameHasher { mphf, order })
    }

    #[inline(always)]
    pub fn mphf(&self) -> &'a LabelNameMphf {
        self.mphf
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.mphf.output_range()
    }

    pub fn hash<T: AsRef<[u8]>>(&self, label_name: T) -> Result<LabelNameId> {
        Ok(LabelNameId(
            self.order
                .get(self.mphf.get(&LabelName(label_name)))
                .context("out of bound dir entry name hash")?
                .try_into()
                .context("label permutation overflowed")?,
        ))
    }
}
