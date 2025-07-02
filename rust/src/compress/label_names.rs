// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use pthash::{
    BuildConfiguration, DictionaryDictionary, Hashable, Minimal, MurmurHash2_128, PartitionedPhf,
    Phf,
};
use rayon::prelude::*;

use crate::labels::LabelNameId;
use crate::map::{MappedPermutation, OwnedPermutation, Permutation};

pub struct LabelName<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Hashable for LabelName<T> {
    type Bytes<'a>
        = &'a [u8]
    where
        T: 'a;
    fn as_bytes(&self) -> Self::Bytes<'_> {
        self.0.as_ref()
    }
}

// pthash requires 128-bits hash when using over 2^30 keys, and the 2024-05-16 production
// graph has just over 2^32 keys
pub type LabelNameMphf = PartitionedPhf<Minimal, MurmurHash2_128, DictionaryDictionary>;

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
    let mut pass_counter = 0;
    let iter_labels = || {
        pass_counter += 1;
        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            item_name = "label",
            local_speed = true,
            expected_updates = Some(num_labels),
        );
        pl.start(format!("Reading labels (pass #{pass_counter})"));
        iter_labels(&path)
            .expect("Could not read labels")
            .inspect(move |_| pl.light_update())
    };
    let temp_dir = tempfile::tempdir().unwrap();

    // From zack's benchmarks on the 2023-09-06 graph (4 billion label names)
    let mut config = BuildConfiguration::new(temp_dir.path().to_owned());
    config.c = 4.000000;
    config.alpha = 0.940000;
    config.num_partitions = (num_labels as u64).div_ceil(40000000);
    config.num_threads = num_cpus::get() as u64;

    log::info!("Building MPH with parameters: {:?}", config);

    assert_ne!(
        config.num_partitions, 0,
        "Cannot build MPHF for empty label list"
    );

    let mut f = LabelNameMphf::new();
    f.build_in_internal_memory_from_bytes(iter_labels, &config)
        .context("Failed to build MPH")?;
    Ok(f)
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

    let mphf = LabelNameMphf::load(&mphf_path)
        .with_context(|| format!("Could not load MPH from {}", mphf_path.display()))?;
    ensure!(
        mphf.num_keys() == num_labels as u64,
        "mphf.num_keys() == {} does not match expected number of keys ({})",
        mphf.num_keys(),
        num_labels
    );

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
        let hash = mphf.hash(&label) as usize;
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
            mphf.num_keys() == order.len() as u64,
            "Number of MPHF keys ({}) does not match permutation length ({})",
            mphf.num_keys(),
            order.len()
        );

        Ok(LabelNameHasher { mphf, order })
    }

    pub fn mphf(&self) -> &'a LabelNameMphf {
        self.mphf
    }

    pub fn hash<T: AsRef<[u8]>>(&self, label_name: T) -> Result<LabelNameId> {
        Ok(LabelNameId(
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
