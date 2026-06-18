// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use anyhow::{ensure, Context, Result};
use dsi_progress_logger::{progress_logger, ProgressLog};
use epserde::deser::{Deserialize, MemCase};
use sux::bits::bit_field_vec::BitFieldVec;
use sux::func::{VBuilder, VFunc};
use sux::utils::lenders::{FromCloneableIntoIterator, FromIntoFallibleLenderFactory};

use crate::labels::LabelNameId;

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
struct VFuncError(#[from] anyhow::Error);

#[derive(Clone)]
pub struct LabelName<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Hash for LabelName<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_ref().hash(state)
    }
}

pub type LabelNameMphf = VFunc<[u8], usize, BitFieldVec<usize>>;

fn iter_labels(path: &Path) -> Result<impl Iterator<Item = Result<Box<[u8]>, VFuncError>>> {
    let base64 = base64_simd::STANDARD;
    let file = File::open(path).with_context(|| format!("Could not open {}", path.display()))?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .with_context(|| format!("Could not decompress {} as zstd", path.display()))?;
    Ok(BufReader::new(decoder).lines().map(move |label_base64| {
        let label_base64 = label_base64.expect("Could not read line");
        Ok(base64
            .decode_to_vec(&label_base64)
            .with_context(|| format!("Label {label_base64}, could not be base64-decoded"))?
            .into_boxed_slice())
    }))
}

/// Reads base64-encoded labels from the path and return a MPH function for them.
pub fn build_mphf(path: PathBuf, num_labels: usize) -> Result<LabelNameMphf> {
    let pass_counter = AtomicU32::new(1);
    let iter_labels = || -> Result<_, VFuncError> {
        let pass_counter = pass_counter.fetch_add(1, Ordering::Relaxed);
        let mut pl = progress_logger!(
            display_memory = true,
            item_name = "label",
            local_speed = true,
            expected_updates = Some(num_labels),
        );
        pl.start(format!("Reading labels (pass #{pass_counter})"));
        Ok(lender::from_fallible_iter_ref(fallible_iterator::convert(
            iter_labels(&path)
                .context("Could not read labels")?
                .inspect(move |_| pl.light_update()),
        )))
    };

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "label",
        local_speed = true,
        expected_updates = Some(num_labels),
    );
    pl.start("Building VFunc");
    let mphf = VBuilder::<_, BitFieldVec<_>>::default()
        .expected_num_keys(num_labels)
        .try_build_func(
            FromIntoFallibleLenderFactory::new(iter_labels)?,
            FromCloneableIntoIterator::new(0..num_labels),
            &mut pl,
        )
        .context("Could not build VFunc")?;

    let len = mphf.len();
    ensure!(
        len == num_labels,
        "Built MPHF from {num_labels}, but its range is {len}"
    );
    Ok(mphf)
}

pub struct LabelNameHasher {
    mphf: MemCase<LabelNameMphf>,
}

impl LabelNameHasher {
    pub fn mmap<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mphf =
            unsafe { LabelNameMphf::mmap(path, epserde::deser::mem_case::Flags::RANDOM_ACCESS) }
                .with_context(|| format!("Could not mmap {}", path.display()))?;
        Ok(LabelNameHasher { mphf })
    }

    #[inline(always)]
    pub fn mphf(&self) -> &epserde::deser::DeserType<'_, LabelNameMphf> {
        self.mphf.uncase()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.mphf().len()
    }

    pub fn hash<T: AsRef<[u8]>>(&self, label_name: T) -> Result<LabelNameId> {
        let hash = self.mphf().get(label_name.as_ref());
        Ok(LabelNameId(
            u64::try_from(hash).context("label name hash overflows u64")?,
        ))
    }
}
